"""P0 watcher hardening shim.

The package shadows the legacy top-level watcher and overrides only ownership
and quote-confirmation seams. It does not submit/cancel broker orders or mutate
positions, proof trades, queues, selector output, scoring, intelligence, or exits.
"""
from __future__ import annotations

import contextvars as _contextvars
import importlib.util as _importlib_util
import json as _json
import sys as _sys
import threading as _threading
from dataclasses import dataclass as _dataclass, field as _field
from datetime import datetime as _datetime, timezone as _timezone
from pathlib import Path as _Path
from typing import Any as _Any, Optional as _Optional

_BASE_PATH = _Path(__file__).resolve().parent.parent / "ap_entry_watcher.py"
_BASE_MODULE_NAME = "_ap_entry_watcher_base"
_spec = _importlib_util.spec_from_file_location(_BASE_MODULE_NAME, _BASE_PATH)
if _spec is None or _spec.loader is None:  # pragma: no cover
    raise ImportError(f"Unable to load legacy watcher module from {_BASE_PATH}")
_base = _importlib_util.module_from_spec(_spec)
_sys.modules[_BASE_MODULE_NAME] = _base
_spec.loader.exec_module(_base)
for _name in dir(_base):
    if not _name.startswith("__") or _name == "__doc__":
        globals()[_name] = getattr(_base, _name)

_BaseWatchedSignal = _base.WatchedSignal
_BaseAPEntryWatcher = _base.APEntryWatcher
WatchState = _base.WatchState
_SIDE_ALIASES = {
    "BUY": "CALL", "LONG": "CALL", "CALLS": "CALL", "BULL": "CALL",
    "BULLISH": "CALL", "SELL": "PUT", "SHORT": "PUT", "PUTS": "PUT",
    "BEAR": "PUT", "BEARISH": "PUT",
}
_TERMINAL_ENTRY = frozenset({"CANCELED", "EXPIRED", "REJECTED", "ERROR"})


@_dataclass(frozen=True)
class ConflictCancellationProof:
    proven_terminal: bool
    outcome: str
    reason_code: str
    local_order_id: str | None
    durable_status: str | None = None
    cancel_returned: bool | None = None


@_dataclass(frozen=True)
class WatchArmResult:
    accepted: bool
    reason_code: str
    local_order_id: str
    has_order_after: bool
    detail: str = ""
    conflict_local_order_id: str = ""
    conflict_direction: str = ""
    conflict_state: str = ""
    conflict_score: float = 0.0
    conflict_signal_id: str = ""
    watcher_token: str = ""
    dedup_key: str = ""


@_dataclass(frozen=True)
class _Identity:
    local_order_id: str
    client_id: str
    execution_mode: str
    signal_id: str
    ticker: str
    side: str
    dedup_key: str


@_dataclass
class _CallResult:
    watcher_id: int
    reason_code: str = ""
    detail: str = ""
    conflict_meta: dict[str, _Any] = _field(default_factory=dict)


_CALL_RESULT: _contextvars.ContextVar[_CallResult | None] = _contextvars.ContextVar(
    "ap_entry_watcher_call_result", default=None
)


def _normalize_watcher_side(raw: _Any) -> str:
    side = _SIDE_ALIASES.get(str(raw or "").upper().strip(), str(raw or "").upper().strip())
    return side if side in {"CALL", "PUT"} else ""


class _SideNormalizedPlan:
    def __init__(self, plan: _Any, side: str):
        self._plan = plan
        self.side = side

    def __getattr__(self, name: str) -> _Any:
        return getattr(self._plan, name)


class WatchedSignal(_BaseWatchedSignal):
    def __init__(self, signal: dict, overnight: bool = False):
        side = _normalize_watcher_side((signal or {}).get("side"))
        if not side:
            ticker = str((signal or {}).get("ticker") or "").upper().strip()
            raise ValueError(f"[{ticker or 'UNKNOWN'}] invalid_or_missing_side; expected CALL or PUT")
        signal["side"] = side
        super().__init__(signal, overnight=overnight)


class APEntryWatcher(_BaseAPEntryWatcher):
    def __init__(
        self,
        broker,
        order_state_machine=None,
        require_on_trigger: _Optional[bool] = None,
        mode: str = "PAPER",
    ):
        self._shared_last_reject_reason = ""
        self._shared_last_conflict_meta: dict[str, _Any] = {}
        super().__init__(broker, order_state_machine, require_on_trigger, mode)
        # Serializes registry admission without extending self._lock across OSM/DB work.
        self._watch_admission_gate = _threading.RLock()
        self._watch_poll_gate = _threading.RLock()

    @property
    def _last_reject_reason(self) -> str:
        call = _CALL_RESULT.get()
        if call is not None and call.watcher_id == id(self):
            return call.reason_code
        return self._shared_last_reject_reason

    @_last_reject_reason.setter
    def _last_reject_reason(self, value: _Any) -> None:
        reason = str(value or "")
        call = _CALL_RESULT.get()
        if call is not None and call.watcher_id == id(self):
            call.reason_code = reason
        self._shared_last_reject_reason = reason

    @property
    def _last_conflict_meta(self) -> dict[str, _Any]:
        call = _CALL_RESULT.get()
        if call is not None and call.watcher_id == id(self):
            return dict(call.conflict_meta)
        return dict(self._shared_last_conflict_meta)

    @_last_conflict_meta.setter
    def _last_conflict_meta(self, value: _Any) -> None:
        meta = dict(value or {})
        call = _CALL_RESULT.get()
        if call is not None and call.watcher_id == id(self):
            call.conflict_meta = meta
        self._shared_last_conflict_meta = meta

    def _record_call_result(self, reason: str, detail: str = "", **meta) -> None:
        # Shared copies remain observability-only; the ContextVar is authoritative.
        self._last_reject_reason = reason
        self._last_conflict_meta = dict(meta)
        call = _CALL_RESULT.get()
        if call is not None and call.watcher_id == id(self):
            call.reason_code, call.detail, call.conflict_meta = reason, detail, dict(meta)

    @staticmethod
    def _mode(value: _Any) -> str:
        return str(value or "").strip().lower()

    @staticmethod
    def _meta(row: dict) -> dict:
        meta = (row or {}).get("meta") or {}
        if isinstance(meta, str):
            try:
                meta = _json.loads(meta)
            except Exception:
                return {}
        return dict(meta) if isinstance(meta, dict) else {}

    def _identity(self, watched) -> _Identity:
        signal = getattr(watched, "signal", None) or {}
        signal_id = str(getattr(watched, "signal_id", "") or signal.get("signal_id") or "").strip()
        return _Identity(
            str(signal.get("local_order_id") or "").strip(),
            str(signal.get("client_id") or "").strip(),
            self._mode(signal.get("execution_mode")),
            signal_id,
            str(getattr(watched, "ticker", "") or signal.get("ticker") or "").upper().strip(),
            str(getattr(watched, "side", "") or signal.get("side") or "").upper().strip(),
            str(self._dedup_key_for_signal(signal) or signal_id or "").strip(),
        )

    def _read_order(self, local_order_id: str):
        osm = getattr(self, "order_state_machine", None)
        read = getattr(osm, "get_order", None)
        if not callable(read):
            read = getattr(osm, "_get_order", None)
        if not callable(read):
            raise RuntimeError("osm_get_order_unavailable")
        return read(local_order_id)

    def _verify_row(self, row: _Any, expected: _Identity) -> tuple[bool, str | None, str]:
        if not isinstance(row, dict):
            return False, None, "row_unreadable"
        meta = self._meta(row)
        local_id = str(row.get("local_order_id") or row.get("id") or "").strip()
        client_id = str(row.get("client_id") or meta.get("client_id") or "").strip()
        mode = self._mode(row.get("execution_mode") or meta.get("execution_mode"))
        signal_id = str(
            row.get("signal_id") or row.get("canonical_signal_id")
            or meta.get("signal_id") or meta.get("canonical_signal_id") or ""
        ).strip()
        status = str(row.get("status") or "").upper().strip() or None
        if local_id != expected.local_order_id:
            return False, status, "durable_identity_mismatch:local_order_id"
        if not client_id or client_id != expected.client_id:
            return False, status, "durable_identity_mismatch:client_id"
        if not mode or mode != expected.execution_mode:
            return False, status, "durable_identity_mismatch:execution_mode"
        if signal_id and signal_id != expected.signal_id:
            return False, status, "durable_identity_mismatch:signal_id"
        if status in _TERMINAL_ENTRY:
            return True, status, "durable_terminal_exact_identity"
        if status == "PENDING_TRIGGER":
            return False, status, "row_pending"
        if not status:
            return False, status, "row_status_missing"
        return False, status, f"row_nonterminal_{status.lower()}"

    def _cancel_conflicting_watcher_with_proof(
        self, watched, expected: _Identity, *, cancel_reason: str
    ) -> ConflictCancellationProof:
        missing = [
            name for name, value in (
                ("local_order_id", expected.local_order_id),
                ("client_id", expected.client_id),
                ("execution_mode", expected.execution_mode),
                ("signal_id", expected.signal_id),
            ) if not value
        ]
        if missing:
            return ConflictCancellationProof(
                False, "retained_owner",
                f"conflict_cancel_unproven:missing_expected_identity:{','.join(missing)}",
                expected.local_order_id or None,
            )
        cancel = getattr(getattr(self, "order_state_machine", None), "cancel_pending_entry", None)
        if not callable(cancel):
            return ConflictCancellationProof(
                False, "retained_owner", "conflict_cancel_unproven:osm_cancel_unavailable",
                expected.local_order_id,
            )
        raised = False
        returned: bool | None = None
        try:
            returned = bool(cancel(expected.local_order_id, reason=cancel_reason))
        except Exception:
            raised = True
        if returned is True:
            return ConflictCancellationProof(
                True, "cancel_returned_true", "conflict_cancel_proven:cancel_returned_true",
                expected.local_order_id, cancel_returned=True,
            )
        try:
            row = self._read_order(expected.local_order_id)
        except Exception:
            subtype = "cancel_raised_row_unreadable" if raised else "cancel_returned_false_row_unreadable"
            return ConflictCancellationProof(
                False, "retained_owner", f"conflict_cancel_unproven:{subtype}",
                expected.local_order_id, cancel_returned=None if raised else False,
            )
        proven, status, why = self._verify_row(row, expected)
        if proven:
            return ConflictCancellationProof(
                True, "durable_terminal_reread", "conflict_cancel_proven:durable_terminal_reread",
                expected.local_order_id, status, None if raised else False,
            )
        subtype = why if why.startswith("durable_identity_mismatch") else (
            f"{'cancel_raised' if raised else 'cancel_returned_false'}_{why}"
        )
        return ConflictCancellationProof(
            False, "retained_owner", f"conflict_cancel_unproven:{subtype}",
            expected.local_order_id, status, None if raised else False,
        )

    def _remove_after_proof(self, watched, expected: _Identity, proof: ConflictCancellationProof) -> bool:
        if not proof.proven_terminal:
            return False
        with self._lock:
            if not any(item is watched for item in self._pending):
                return True
            if self._identity(watched) != expected:
                return False
            self._pending = [item for item in self._pending if item is not watched]
            watched.state = WatchState.CANCELLED
            if expected.dedup_key:
                self._dedup_set.discard(expected.dedup_key)
        return True

    def _opposites(self, ticker: str, side: str) -> list:
        with self._lock:
            return [
                item for item in self._pending
                if (item.is_active or getattr(item, "rearm_mode", False))
                and str(item.ticker).upper().strip() == ticker
                and str(item.side).upper().strip() != side
            ]

    def _same_side(self, ticker: str, side: str) -> list:
        with self._lock:
            return [
                item for item in self._pending
                if (item.is_active or getattr(item, "rearm_mode", False))
                and str(item.ticker).upper().strip() == ticker
                and str(item.side).upper().strip() == side
            ]

    def _block(self, signal: dict, watched, reason: str, detail: str, proof=None) -> bool:
        old = getattr(watched, "signal", None) or {}
        meta = {
            "conflicting_local_order_id": old.get("local_order_id", ""),
            "conflicting_direction": str(getattr(watched, "side", "") or ""),
            "conflicting_state": str(getattr(watched, "state", "") or ""),
            "conflicting_score": float(getattr(watched, "score", 0) or 0),
            "conflicting_signal_id": str(getattr(watched, "signal_id", "") or old.get("signal_id") or ""),
            "cancel_returned": getattr(proof, "cancel_returned", None),
            "durable_status": getattr(proof, "durable_status", None),
        }
        self._record_call_result(reason, detail, **meta)
        payload = self._build_watcher_audit_payload(
            None,
            symbol=str(signal.get("ticker") or ""), score=float(signal.get("score") or 0),
            tier=str(signal.get("grade") or ""), direction=str(signal.get("side") or ""),
            timeframe=str(signal.get("timeframe") or ""), pattern=str(signal.get("pattern") or ""),
            signal_id=str(signal.get("signal_id") or ""), plan_id=str(signal.get("plan_id") or ""),
            trigger_type="add_signal_block", signal_entry_price=signal.get("entry_price"),
            trigger_price=signal.get("entry_price"), stop_price=signal.get("stop_price"),
            reason_code=reason, raw_reason=detail,
            extra={"block_stage": "watcher", "block_reason": reason, **meta},
        )
        for local_id in {str(signal.get("local_order_id") or ""), str(old.get("local_order_id") or "")}:
            if local_id:
                try:
                    self._persist_watcher_audit(local_id, payload)
                except Exception:
                    pass
        return False

    def _prove_remove_all(self, signal: dict, targets: list, cancel_reason: str) -> bool:
        for watched in list(targets):
            with self._lock:
                if not any(item is watched for item in self._pending):
                    continue
                expected = self._identity(watched)
            proof = self._cancel_conflicting_watcher_with_proof(
                watched, expected, cancel_reason=cancel_reason
            )
            if not proof.proven_terminal:
                return self._block(signal, watched, "conflict_cancel_unproven", proof.reason_code, proof)
            if not self._remove_after_proof(watched, expected, proof):
                return self._block(
                    signal, watched, "conflict_cancel_unproven",
                    "conflict_cancel_unproven:watcher_identity_changed_after_proof", proof,
                )
        return True

    def _prunable(self, signal: dict, watched) -> bool:
        age = max(0.0, (_datetime.now(_timezone.utc) - watched.created_at).total_seconds())
        old_score, new_score = float(watched.score or 0), float(signal.get("score") or 0)
        stale = age > float(getattr(_base, "OPPOSITE_CONFLICT_MAX_AGE_SEC", 600)) and old_score <= new_score
        rearm_only = bool(getattr(watched, "rearm_mode", False)) and not watched.is_active
        return stale or (rearm_only and new_score >= old_score)

    def _candidate_wins(self, signal: dict, old) -> bool:
        new_score, old_score = float(signal.get("score") or 0), float(old.score or 0)
        if new_score > old_score:
            return True
        tied = abs(new_score - old_score) <= float(
            getattr(_base, "OPPOSITE_CONFLICT_SCORE_TIE_PCT", 0.03)
        ) * max(1.0, old_score)
        return tied and _base._opposite_tf_rank(signal.get("timeframe")) > _base._opposite_tf_rank(
            (old.signal or {}).get("timeframe")
        )

    def add_signal(self, signal: dict) -> bool:
        ticker = str((signal or {}).get("ticker") or "").upper().strip()
        side = _normalize_watcher_side((signal or {}).get("side"))
        if not ticker or not side:
            return super().add_signal(signal)
        with self._watch_admission_gate:
            opposites = self._opposites(ticker, side)
            prune = [item for item in opposites if self._prunable(signal, item)]
            if prune and not self._prove_remove_all(
                signal, prune, "opposite_side_replaced_stale_or_weaker"
            ):
                return False
            opposites = self._opposites(ticker, side)
            if opposites:
                best = max(opposites, key=lambda item: float(item.score or 0))
                if not self._candidate_wins(signal, best):
                    return self._block(
                        signal, best, "opposite_side_conflict",
                        "opposite_side_conflict:existing_watcher_wins",
                    )
                if not self._prove_remove_all(signal, opposites, "direction_flip_watcher_cancel"):
                    return False
            remaining = self._opposites(ticker, side)
            if remaining:
                return self._block(
                    signal, remaining[0], "conflict_cancel_unproven",
                    "conflict_cancel_unproven:opposite_reappeared_before_admission",
                )
            dedup_key = str(self._dedup_key_for_signal(signal) or "").strip()
            with self._lock:
                dedup_seen = bool(dedup_key and dedup_key in self._dedup_set)
            if dedup_seen:
                return super().add_signal(signal)
            same_side = self._same_side(ticker, side)
            if same_side:
                best = max(same_side, key=lambda item: float(item.score or 0))
                if float(signal.get("score") or 0) <= float(best.score or 0):
                    return self._block(
                        signal,
                        best,
                        "same_side_block",
                        "same_side_block:existing_watcher_score_wins",
                    )
                if not self._prove_remove_all(signal, same_side, "same_side_replace_watcher_cancel"):
                    return False
            return super().add_signal(signal)

    def watch(
        self, plan, local_order_id: str, *, recovery_rearm: bool = False,
        no_cancel_on_reject: bool = False, materialization_resume: bool = False,
    ) -> bool:
        current_call = _CALL_RESULT.get()
        if current_call is not None and current_call.watcher_id == id(self):
            return self._watch_impl(
                plan,
                local_order_id,
                recovery_rearm=recovery_rearm,
                no_cancel_on_reject=no_cancel_on_reject,
                materialization_resume=materialization_resume,
            )
        token = _CALL_RESULT.set(_CallResult(id(self)))
        try:
            return self._watch_impl(
                plan,
                local_order_id,
                recovery_rearm=recovery_rearm,
                no_cancel_on_reject=no_cancel_on_reject,
                materialization_resume=materialization_resume,
            )
        finally:
            _CALL_RESULT.reset(token)

    def _watch_impl(
        self, plan, local_order_id: str, *, recovery_rearm: bool = False,
        no_cancel_on_reject: bool = False, materialization_resume: bool = False,
    ) -> bool:
        if plan is None:
            _base.log.warning("watch() called with None plan -- skipping")
            return False
        side = _normalize_watcher_side(getattr(plan, "side", None))
        if not side:
            ticker = str(getattr(plan, "ticker", "") or "").upper().strip()
            self._record_call_result("invalid_or_missing_side")
            try:
                self._persist_watcher_audit(local_order_id, self._build_watcher_audit_payload(
                    None, symbol=ticker, score=float(getattr(plan, "score", 0) or 0),
                    tier=str(getattr(plan, "tier", "") or getattr(plan, "grade", "") or ""),
                    direction="", timeframe=str(getattr(plan, "timeframe", "") or ""),
                    pattern=str(getattr(plan, "pattern", "") or ""),
                    signal_id=str(getattr(plan, "signal_id", "") or ""),
                    plan_id=str(getattr(plan, "plan_id", "") or ""),
                    trigger_type="watch_arm_block", signal_entry_price=getattr(plan, "trigger_price", None),
                    trigger_price=getattr(plan, "trigger_price", None), stop_price=getattr(plan, "stop_underlying", None),
                    reason_code="invalid_or_missing_side", raw_reason="side_is_not_CALL_or_PUT",
                ))
            except Exception:
                pass
            return False
        try:
            setattr(plan, "side", side)
            normalized_plan = plan
        except Exception:
            normalized_plan = _SideNormalizedPlan(plan, side)
        return super().watch(
            normalized_plan, local_order_id,
            recovery_rearm=recovery_rearm,
            no_cancel_on_reject=no_cancel_on_reject,
            materialization_resume=materialization_resume,
        )

    def watch_with_result(
        self, plan, local_order_id: str, *, recovery_rearm: bool = False,
        no_cancel_on_reject: bool = False, materialization_resume: bool = False,
    ) -> WatchArmResult:
        call = _CallResult(id(self))
        token = _CALL_RESULT.set(call)
        try:
            accepted = bool(self.watch(
                plan, local_order_id, recovery_rearm=recovery_rearm,
                no_cancel_on_reject=no_cancel_on_reject,
                materialization_resume=materialization_resume,
            ))
            has_after = bool(self.has_order(local_order_id))
            meta = call.conflict_meta
            return WatchArmResult(
                accepted, call.reason_code or ("accepted" if accepted else "blocked_unknown_check_audit"),
                local_order_id, has_after, call.detail,
                str(meta.get("conflicting_local_order_id") or ""),
                str(meta.get("conflicting_direction") or ""),
                str(meta.get("conflicting_state") or ""),
                float(meta.get("conflicting_score") or 0),
                str(meta.get("conflicting_signal_id") or ""),
                str(getattr(self, "owner_token", "") or ""),
                str(getattr(plan, "signal_id", "") or ""),
            )
        finally:
            _CALL_RESULT.reset(token)

    def _poll_active_signals(self, open_protect_active: bool = False) -> None:
        """Preserve the no-argument shim API and filter at the final poll boundary.

        Some runtime subclasses override ``_fetch_quotes``. Filtering only in the
        shim's ``_fetch_quotes`` therefore lets last-only quotes bypass the safety
        rule. The dedicated poll gate serializes this narrow method substitution
        without holding ``self._lock`` across quote/network work.
        """
        with self._watch_poll_gate:
            had_instance_override = "_fetch_quotes" in self.__dict__
            prior_instance_value = self.__dict__.get("_fetch_quotes")
            fetch_quotes = self._fetch_quotes

            def _hardened_fetch(tickers):
                quotes = fetch_quotes(tickers)
                if self._last_only_trigger_allowed() or not isinstance(quotes, dict):
                    return quotes
                hardened = {}
                for ticker, quote in quotes.items():
                    if self._is_last_only_quote(quote):
                        # Empty quote makes the legacy poll loop skip this ticker.
                        # Passing bid=ask=last=0 would run stop invalidation and
                        # could consume a valid watcher during a quote outage.
                        hardened[ticker] = {}
                    else:
                        hardened[ticker] = quote
                return hardened

            self.__dict__["_fetch_quotes"] = _hardened_fetch
            try:
                return super()._poll_active_signals(
                    open_protect_active=open_protect_active
                )
            finally:
                if had_instance_override:
                    self.__dict__["_fetch_quotes"] = prior_instance_value
                else:
                    self.__dict__.pop("_fetch_quotes", None)

    @staticmethod
    def _is_last_only_quote(quote: dict | None) -> bool:
        if not quote:
            return False
        try:
            return float(quote.get("bid") or 0) <= 0 and float(quote.get("ask") or 0) <= 0 and float(quote.get("last") or 0) > 0
        except (TypeError, ValueError):
            return False

    def _last_only_trigger_allowed(self) -> bool:
        return str(_base.os.getenv("WATCHER_ALLOW_LAST_ONLY_TRIGGER", "0")).strip().lower() in {"1", "true", "yes", "on"}

    def _get_quote(self, ticker: str) -> dict:
        quote = super()._get_quote(ticker)
        if self._is_last_only_quote(quote) and not self._last_only_trigger_allowed():
            quote = dict(quote)
            quote.update(last=0.0, watcher_bid_ask_unavailable=True, last_only_quote_blocked=True)
        return quote

    def _fetch_quotes(self, tickers: list[str]) -> dict:
        quotes = super()._fetch_quotes(tickers)
        if self._last_only_trigger_allowed() or not isinstance(quotes, dict):
            return quotes
        result = {}
        for ticker, quote in quotes.items():
            if self._is_last_only_quote(quote):
                quote = dict(quote)
                quote.update(last=0.0, watcher_bid_ask_unavailable=True, last_only_quote_blocked=True)
            result[ticker] = quote
        return result


_base.WatchedSignal = WatchedSignal
_base.APEntryWatcher = APEntryWatcher
globals()["WatchedSignal"] = WatchedSignal
globals()["APEntryWatcher"] = APEntryWatcher
try:
    __all__ = sorted(set(getattr(_base, "__all__", [])) | {
        "APEntryWatcher", "WatchedSignal", "ConflictCancellationProof", "WatchArmResult"
    })
except Exception:  # pragma: no cover
    __all__ = ["APEntryWatcher", "WatchedSignal", "ConflictCancellationProof", "WatchArmResult"]
