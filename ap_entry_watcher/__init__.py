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

    def _poll_active_signals(self, open_protect_active: bool = False) -> None:
        with self._lock:
            active = [w for w in self._pending if w.is_active and not w.overnight]

        if not active:
            return

        tickers = list({w.ticker for w in active})
        try:
            quotes = self._fetch_quotes(tickers)
        except Exception as exc:
            _base.log.warning("Quote fetch failed: %s", exc)
            return

        triggered: list = []
        terminal_done: list = []
        quote_missing_audits: list[tuple[object, dict]] = []
        open_protection_audits: list[tuple[object, dict]] = []

        with self._lock:
            for w in active:
                quote = quotes.get(w.ticker)
                if not quote:
                    continue

                try:
                    bid = float(quote.get("bid", 0) or 0)
                    ask = float(quote.get("ask", 0) or 0)
                except (TypeError, ValueError):
                    bid = ask = 0.0

                w.last_quote_age_ms = self._coerce_quote_age_ms(quote.get("quote_age_ms"))

                if bid <= 0 and ask <= 0 and not self._last_only_trigger_allowed():
                    try:
                        last = float(quote.get("last", 0) or 0)
                    except (TypeError, ValueError):
                        last = 0.0
                    audit = self._build_watcher_audit_payload(
                        w,
                        trigger_type="quote_check",
                        current_bid=0.0,
                        current_ask=0.0,
                        current_mid=last if last > 0 else 0.0,
                        quote_age_ms=w.last_quote_age_ms,
                        reason_code="watcher_bid_ask_unavailable",
                        raw_reason="bid_ask_zero_hold_no_trigger",
                        extra={
                            "last_price": last,
                            "trigger_poll_blocked": True,
                            "quote_fetch_status": quote.get("quote_fetch_status"),
                        },
                    )
                    quote_missing_audits.append((w, audit))
                    continue

                if bid <= 0 and ask <= 0:
                    # Explicit opt-in compatibility path only.
                    try:
                        last = float(quote.get("last", 0) or 0)
                    except (TypeError, ValueError):
                        last = 0.0
                    bid = ask = last

                new_state = w.check(bid, ask)
                if new_state == WatchState.TRIGGERED:
                    if open_protect_active and w.ticker in self._open_trigger_tickers:
                        w.state = WatchState.EXPIRED
                        w._release_dedup_key()
                        terminal_done.append(w)
                        audit = self._build_watcher_audit_payload(
                            w,
                            trigger_type="open_protection",
                            current_bid=bid,
                            current_ask=ask,
                            current_mid=(bid + ask) / 2.0 if bid and ask else max(bid, ask),
                            quote_age_ms=w.last_quote_age_ms,
                            reason_code="open_protection_block",
                            raw_reason="ticker_already_triggered_inside_open_protect_window",
                            extra={"open_protect_active": True},
                        )
                        open_protection_audits.append((w, audit))
                        _base.log.info(
                            "[%s] OPEN_PROTECTION_BLOCK — ticker already triggered at open",
                            w.ticker,
                        )
                    else:
                        self._open_trigger_count += 1
                        if open_protect_active:
                            self._open_trigger_tickers.add(w.ticker)
                        triggered.append(w)
                elif new_state in (WatchState.EXPIRED, WatchState.INVALIDATED):
                    terminal_done.append(w)

            terminal_ids = {id(w) for w in terminal_done}
            self._pending = [w for w in self._pending if id(w) not in terminal_ids]

        for w, audit in quote_missing_audits:
            self._persist_watcher_audit((getattr(w, "signal", {}) or {}).get("local_order_id"), audit)

        for w, audit in open_protection_audits:
            self._persist_watcher_audit((getattr(w, "signal", {}) or {}).get("local_order_id"), audit)

        for w in terminal_done:
            self._process_terminal_poll_result(w)

        for w in triggered:
            self._process_triggered_poll_result(w)

    def _process_terminal_poll_result(self, w) -> None:
        sig_id = str((getattr(w, "signal", {}) or {}).get("signal_id", ""))
        ticker = str(getattr(w, "ticker", "") or "")
        if w.state == WatchState.EXPIRED:
            if sig_id and ticker:
                _base._ew_record(
                    sig_id,
                    ticker,
                    "EXPIRED",
                    "signal_expired_in_poll_loop",
                    minutes_watching=str(getattr(w, "minutes_watching", "?")),
                )
            if self.on_expire:
                try:
                    self.on_expire(w)
                except Exception as exc:
                    _base.log.error("[%s] on_expire callback failed: %s", w.ticker, exc, exc_info=True)
        elif w.state == WatchState.INVALIDATED:
            pending_audit = getattr(w, "_pending_audit", None)
            if pending_audit:
                self._persist_watcher_audit((getattr(w, "signal", {}) or {}).get("local_order_id"), pending_audit)
            if sig_id and ticker:
                _base._ew_record(sig_id, ticker, "INVALIDATED", "signal_invalidated_in_poll_loop")
            if self.on_invalidate:
                try:
                    self.on_invalidate(w)
                except Exception as exc:
                    _base.log.error("[%s] on_invalidate callback failed: %s", w.ticker, exc, exc_info=True)

    def _process_triggered_poll_result(self, w) -> None:
        sig = getattr(w, "signal", {}) or {}
        sig_id = str(sig.get("signal_id", ""))
        ticker = str(getattr(w, "ticker", "") or "")
        local_order_id = sig.get("local_order_id")

        trigger_audit = self._build_watcher_audit_payload(
            w,
            trigger_type="trigger",
            current_bid=float(getattr(w, "last_quote_bid", 0) or 0),
            current_ask=float(getattr(w, "last_quote_ask", 0) or 0),
            quote_age_ms=getattr(w, "last_quote_age_ms", None),
            reason_code="trigger_ready",
            raw_reason=(
                f"{str(getattr(w, 'side', '')).lower()}_breach_confirmed"
                f"_after_{int(getattr(w, 'breach_count', 0) or 0)}_polls"
            ),
            extra={
                "breach_count": int(getattr(w, "breach_count", 0) or 0),
                "queue_status": str(sig.get("queue_status") or ""),
            },
        )
        self._persist_watcher_audit(local_order_id, trigger_audit)

        if sig_id and ticker:
            _base._ew_record(
                sig_id,
                ticker,
                "TRIGGER_READY",
                "trigger_breached_entry_submitted",
                contract=str(sig.get("contract_symbol", "")),
                entry_trigger=str(getattr(w, "entry_trigger", None) or ""),
            )

        if not self.on_trigger:
            _base.log.error("[%s] TRIGGERED but no on_trigger callback is wired", w.ticker)
            w.state = WatchState.EXPIRED
            w._release_dedup_key()
            with self._lock:
                self._pending = [x for x in self._pending if id(x) != id(w)]
            if self.on_expire:
                try:
                    self.on_expire(w)
                except Exception as exc:
                    _base.log.error("[%s] on_expire callback failed after missing on_trigger: %s", w.ticker, exc, exc_info=True)
            return

        attempts = int(getattr(w, "_trigger_attempts", 0) or 0)
        try:
            self.on_trigger(w)
            w._trigger_attempts = 0
            try:
                _base.log.info(
                    "WATCHER_ON_TRIGGER_RETURNED client_id=%s local_order_id=%s signal_id=%s "
                    "symbol=%s contract=%s callback_wired=true attempt=%d outcome=returned",
                    sig.get("client_email") or "n/a",
                    local_order_id or "n/a",
                    sig.get("signal_id") or "n/a",
                    w.ticker,
                    ((sig.get("plan") or {}).get("contract_symbol") or sig.get("contract_symbol") or sig.get("contract") or "n/a"),
                    attempts + 1,
                )
            except Exception:
                pass
            with self._lock:
                self._pending = [x for x in self._pending if id(x) != id(w)]
            w._release_dedup_key()
        except Exception as exc:
            attempts += 1
            w._trigger_attempts = attempts
            _base.log.error(
                "[%s] on_trigger callback failed (attempt %d/3): %s",
                w.ticker,
                attempts,
                exc,
                exc_info=True,
            )
            try:
                _base.log.error(
                    "WATCHER_ON_TRIGGER_EXCEPTION client_id=%s local_order_id=%s signal_id=%s "
                    "symbol=%s contract=%s callback_wired=true attempt=%d outcome=exception "
                    "exception_type=%s exception_message=%s",
                    sig.get("client_email") or "n/a",
                    local_order_id or "n/a",
                    sig.get("signal_id") or "n/a",
                    w.ticker,
                    ((sig.get("plan") or {}).get("contract_symbol") or sig.get("contract_symbol") or sig.get("contract") or "n/a"),
                    attempts,
                    type(exc).__name__,
                    str(exc)[:200],
                )
            except Exception:
                pass

            if attempts < 3:
                # Keep watcher owned and retryable on the next poll.
                w.state = WatchState.PENDING
                w.triggered_at = None
                w.trigger_price = None
                try:
                    w.breach_count = max(int(getattr(w, "MOMENTUM_POLLS_REQUIRED", 2)) - 1, 0)
                except Exception:
                    w.breach_count = 1
                _base.log.warning(
                    "[%s] TRIGGER_RETRY — watcher retained for next poll cycle (attempt %d/3)",
                    w.ticker,
                    attempts,
                )
                return

            w.state = WatchState.EXPIRED
            exhausted_audit = self._build_watcher_audit_payload(
                w,
                trigger_type="trigger_callback",
                current_bid=float(getattr(w, "last_quote_bid", 0) or 0),
                current_ask=float(getattr(w, "last_quote_ask", 0) or 0),
                quote_age_ms=getattr(w, "last_quote_age_ms", None),
                reason_code="on_trigger_exhausted_3_attempts",
                raw_reason=f"on_trigger_failed_3_attempts_last_error_{type(exc).__name__}",
                extra={
                    "trigger_attempts": attempts,
                    "last_error_type": type(exc).__name__,
                    "last_error_message": str(exc)[:200],
                },
            )
            self._persist_watcher_audit(local_order_id, exhausted_audit)
            w._release_dedup_key()
            if sig_id and ticker:
                _base._ew_record(sig_id, ticker, "EXPIRED", "on_trigger_exhausted_3_attempts")
            with self._lock:
                self._pending = [x for x in self._pending if id(x) != id(w)]
            if self.on_expire:
                try:
                    self.on_expire(w)
                except Exception as expire_exc:
                    _base.log.error("[%s] on_expire callback failed after trigger exhaustion: %s", w.ticker, expire_exc, exc_info=True)


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
