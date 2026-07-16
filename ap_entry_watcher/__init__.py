"""P0 watcher hardening shim.

This package intentionally shadows the legacy top-level ``ap_entry_watcher.py``
module without rewriting that large file in-place. It loads the legacy module
under a private name, re-exports its public surface, then overrides only the
MVP-hardening paths called out by the watcher audit:

* fail closed when side is missing/invalid instead of defaulting to CALL
* keep triggered watchers owned until on_trigger succeeds or exhausts retries
* do not confirm active entry triggers from last-only quotes

Scope: watcher ownership / trigger confirmation only. No broker submit/cancel,
order-state mutation, position mutation, proof_trades mutation, or queue writes
are introduced here.
"""

from __future__ import annotations

import importlib.util as _importlib_util
import sys as _sys
from pathlib import Path as _Path
from typing import Any as _Any

_BASE_PATH = _Path(__file__).resolve().parent.parent / "ap_entry_watcher.py"
_BASE_MODULE_NAME = "_ap_entry_watcher_base"

_spec = _importlib_util.spec_from_file_location(_BASE_MODULE_NAME, _BASE_PATH)
if _spec is None or _spec.loader is None:  # pragma: no cover - import system guard
    raise ImportError(f"Unable to load legacy watcher module from {_BASE_PATH}")

_base = _importlib_util.module_from_spec(_spec)
_sys.modules[_BASE_MODULE_NAME] = _base
_spec.loader.exec_module(_base)

# Re-export the legacy module surface first. Hardened replacements below override
# APEntryWatcher and WatchedSignal but leave constants/helpers/back-compat intact.
for _name in dir(_base):
    if _name.startswith("__") and _name not in {"__doc__"}:
        continue
    globals()[_name] = getattr(_base, _name)

_BaseWatchedSignal = _base.WatchedSignal
_BaseAPEntryWatcher = _base.APEntryWatcher
WatchState = _base.WatchState

_SIDE_ALIASES = {
    "BUY": "CALL",
    "LONG": "CALL",
    "CALLS": "CALL",
    "BULL": "CALL",
    "BULLISH": "CALL",
    "SELL": "PUT",
    "SHORT": "PUT",
    "PUTS": "PUT",
    "BEAR": "PUT",
    "BEARISH": "PUT",
}


def _normalize_watcher_side(raw: _Any) -> str:
    side = str(raw or "").upper().strip()
    side = _SIDE_ALIASES.get(side, side)
    return side if side in {"CALL", "PUT"} else ""


class _SideNormalizedPlan:
    """Proxy an immutable/third-party plan while overriding only ``side``."""

    def __init__(self, plan: _Any, side: str):
        self._plan = plan
        self.side = side

    def __getattr__(self, name: str) -> _Any:
        return getattr(self._plan, name)


class WatchedSignal(_BaseWatchedSignal):
    """WatchedSignal with fail-closed side normalization.

    The legacy class used ``signal.get("side") or "CALL"``. That silently turns
    malformed payloads into bullish entries. This wrapper requires production
    side metadata before the watcher can own a setup.
    """

    def __init__(self, signal: dict, overnight: bool = False):
        normalized = _normalize_watcher_side((signal or {}).get("side"))
        if not normalized:
            ticker = str((signal or {}).get("ticker") or "").upper().strip()
            raise ValueError(
                f"[{ticker or 'UNKNOWN'}] invalid_or_missing_side; expected CALL or PUT"
            )
        signal["side"] = normalized
        super().__init__(signal, overnight=overnight)


class APEntryWatcher(_BaseAPEntryWatcher):
    """MVP-hardened entry watcher.

    This subclass deliberately avoids broker/order-state writes. It only changes
    watcher-local ownership, side validation, and active quote confirmation.
    """

    @staticmethod
    def _is_last_only_quote(quote: dict | None) -> bool:
        if not quote:
            return False
        try:
            bid = float((quote or {}).get("bid", 0) or 0)
            ask = float((quote or {}).get("ask", 0) or 0)
            last = float((quote or {}).get("last", 0) or 0)
        except (TypeError, ValueError):
            return False
        return bid <= 0 and ask <= 0 and last > 0

    def _last_only_trigger_allowed(self) -> bool:
        return str(_base.os.getenv("WATCHER_ALLOW_LAST_ONLY_TRIGGER", "0")).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }

    def watch(
        self,
        plan,
        local_order_id: str,
        *,
        recovery_rearm: bool = False,
        no_cancel_on_reject: bool = False,
        materialization_resume: bool = False,
    ) -> bool:
        if plan is None:
            _base.log.warning("watch() called with None plan -- skipping")
            return False

        raw_side = getattr(plan, "side", None)
        normalized = _normalize_watcher_side(raw_side)
        if not normalized:
            ticker = str(getattr(plan, "ticker", "") or "").upper().strip()
            _base.log.error(
                "[%s] WATCH_ARM_BLOCKED — invalid_or_missing_side side=%r local_order_id=%s",
                ticker,
                raw_side,
                local_order_id,
            )
            with self._lock:
                self._last_reject_reason = "invalid_or_missing_side"
            try:
                audit = self._build_watcher_audit_payload(
                    None,
                    symbol=ticker,
                    score=float(getattr(plan, "score", 0) or 0),
                    tier=str(getattr(plan, "tier", "") or getattr(plan, "grade", "") or ""),
                    direction="",
                    timeframe=str(getattr(plan, "timeframe", "") or ""),
                    pattern=str(getattr(plan, "pattern", "") or ""),
                    signal_id=str(getattr(plan, "signal_id", "") or ""),
                    plan_id=str(getattr(plan, "plan_id", "") or ""),
                    trigger_type="watch_arm_block",
                    signal_entry_price=getattr(plan, "trigger_price", None),
                    trigger_price=getattr(plan, "trigger_price", None),
                    stop_price=getattr(plan, "stop_underlying", None),
                    reason_code="invalid_or_missing_side",
                    raw_reason=f"side={raw_side!r}_is_not_CALL_or_PUT",
                )
                self._persist_watcher_audit(local_order_id, audit)
            except Exception:
                pass
            return False

        try:
            setattr(plan, "side", normalized)
            normalized_plan = plan
        except Exception:
            normalized_plan = _SideNormalizedPlan(plan, normalized)

        return super().watch(
            normalized_plan,
            local_order_id,
            recovery_rearm=recovery_rearm,
            no_cancel_on_reject=no_cancel_on_reject,
            materialization_resume=materialization_resume,
        )

    def _get_quote(self, ticker: str) -> dict:
        quote = super()._get_quote(ticker)
        if self._is_last_only_quote(quote) and not self._last_only_trigger_allowed():
            filtered = dict(quote)
            filtered["last"] = 0.0
            filtered["watcher_bid_ask_unavailable"] = True
            filtered["last_only_quote_blocked"] = True
            return filtered
        return quote

    def _fetch_quotes(self, tickers: list[str]) -> dict:
        quotes = super()._fetch_quotes(tickers)
        if self._last_only_trigger_allowed() or not isinstance(quotes, dict):
            return quotes
        filtered_quotes = {}
        for ticker, quote in quotes.items():
            if self._is_last_only_quote(quote):
                filtered = dict(quote)
                filtered["last"] = 0.0
                filtered["watcher_bid_ask_unavailable"] = True
                filtered["last_only_quote_blocked"] = True
                filtered_quotes[ticker] = filtered
            else:
                filtered_quotes[ticker] = quote
        return filtered_quotes


# Ensure legacy methods whose globals resolve ``WatchedSignal`` pick up the
# hardened subclass when they run via super().watch()/add_signal().
_base.WatchedSignal = WatchedSignal
_base.APEntryWatcher = APEntryWatcher

globals()["WatchedSignal"] = WatchedSignal
globals()["APEntryWatcher"] = APEntryWatcher

try:
    __all__ = sorted(set(getattr(_base, "__all__", [])) | {"APEntryWatcher", "WatchedSignal"})
except Exception:  # pragma: no cover
    __all__ = ["APEntryWatcher", "WatchedSignal"]
