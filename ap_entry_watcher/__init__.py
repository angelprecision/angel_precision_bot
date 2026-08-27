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
    canonical_signal_id: str


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
        # Direction claims are made only after confirmed trigger evaluation.
        # The map is process-local coordination; durable order identity and
        # cancellation proof remain authoritative for any destructive action.
        self._direction_claim_gate = _threading.RLock()
        self._direction_claims: dict[tuple[str, str, str], dict] = {}
        self._direction_poll_context: dict | None = None

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
        metadata = signal.get("metadata") or {}
        if not isinstance(metadata, dict):
            metadata = {}
        canonical_signal_id = str(
            signal.get("canonical_signal_id")
            or metadata.get("canonical_signal_id")
            or _base.build_canonical_signal_id(signal_id)
            or ""
        ).strip()
        return _Identity(
            str(signal.get("local_order_id") or "").strip(),
            str(signal.get("client_id") or signal.get("client_email") or "").strip(),
            self._mode(signal.get("execution_mode")),
            signal_id,
            str(getattr(watched, "ticker", "") or signal.get("ticker") or "").upper().strip(),
            str(getattr(watched, "side", "") or signal.get("side") or "").upper().strip(),
            str(self._dedup_key_for_signal(signal) or signal_id or "").strip(),
            canonical_signal_id,
        )

    @staticmethod
    def _ownership_key_for_payload(payload: _Any) -> tuple[str, str, str] | None:
        payload = payload if isinstance(payload, dict) else {}
        client_id = str(
            payload.get("client_id") or payload.get("client_email") or ""
        ).strip().lower()
        execution_mode = str(payload.get("execution_mode") or "").strip().lower()
        ticker = str(payload.get("ticker") or "").strip().upper()
        if not client_id or not execution_mode or not ticker:
            return None
        return client_id, execution_mode, ticker

    def _ownership_key(self, value: _Any) -> tuple[str, str, str] | None:
        payload = getattr(value, "signal", value)
        return self._ownership_key_for_payload(payload)

    @staticmethod
    def _direction_identity_complete(watched) -> bool:
        signal = getattr(watched, "signal", None) or {}
        return bool(
            str(signal.get("local_order_id") or "").strip()
            and str(signal.get("signal_id") or getattr(watched, "signal_id", "") or "").strip()
            and str(getattr(watched, "side", "") or signal.get("side") or "").strip()
        )

    def _is_coarmable_opposite(self, watched) -> bool:
        """Healthy, pre-breach opposite watcher that may remain co-armed."""
        return bool(
            self._ownership_key(watched)
            and self._direction_identity_complete(watched)
            and getattr(watched, "state", None) == WatchState.PENDING
            and bool(getattr(watched, "is_active", False))
            and not bool(getattr(watched, "rearm_mode", False))
            and not bool(getattr(watched, "_ownership_quarantine", False))
            and getattr(watched, "trigger_crossed_at", None) is None
            and getattr(watched, "triggered_at", None) is None
        )

    @staticmethod
    def _has_durable_confirmed_direction_evidence(watched) -> bool:
        """Return whether a watcher carries a confirmed breach across restart."""
        return getattr(watched, "trigger_crossed_at", None) is not None

    @staticmethod
    def _is_ordinary_admission(signal: dict) -> bool:
        signal = signal if isinstance(signal, dict) else {}
        return not bool(
            signal.get("__watcher_rearm_pending")
            or signal.get("__recovery_rearm")
            or signal.get("rearm_mode")
            or signal.get("_ownership_quarantine")
        )

    def _opposite_conflict_applies(self, watched, opposite) -> bool:
        if not self._is_ordinary_admission(getattr(watched, "signal", {}) or {}):
            return True
        incoming_key = self._ownership_key(watched)
        opposite_key = self._ownership_key(opposite)
        if incoming_key is None or opposite_key is None:
            # An incomplete ownership key is never silently co-armed.
            return True
        if incoming_key != opposite_key:
            return False
        return not self._is_coarmable_opposite(opposite)

    def _same_side_conflict_applies(self, watched, same_side_watcher) -> bool:
        incoming_key = self._ownership_key(watched)
        existing_key = self._ownership_key(same_side_watcher)
        if incoming_key is None or existing_key is None:
            return True
        return incoming_key == existing_key

    def _read_order(self, local_order_id: str):
        osm = getattr(self, "order_state_machine", None)
        read = getattr(osm, "get_order", None)
        if not callable(read):
            read = getattr(osm, "_get_order", None)
        if not callable(read):
            raise RuntimeError("osm_get_order_unavailable")
        return read(local_order_id)

    def _verify_row(
        self,
        row: _Any,
        expected: _Identity,
        *,
        pending_only: bool = False,
    ) -> tuple[bool, str | None, str]:
        if not isinstance(row, dict):
            return False, None, "row_unreadable"
        meta = self._meta(row)
        local_id = str(row.get("local_order_id") or row.get("id") or "").strip()
        client_id = str(row.get("client_id") or meta.get("client_id") or "").strip()
        mode = self._mode(row.get("execution_mode") or meta.get("execution_mode"))
        signal_id = str(row.get("signal_id") or meta.get("signal_id") or "").strip()
        canonical_signal_id = str(
            row.get("canonical_signal_id")
            or meta.get("canonical_signal_id")
            or ""
        ).strip()
        ticker = str(
            row.get("symbol")
            or row.get("ticker")
            or row.get("underlying")
            or meta.get("symbol")
            or meta.get("ticker")
            or ""
        ).strip().upper()
        side = _normalize_watcher_side(
            row.get("direction")
            or row.get("side")
            or meta.get("direction")
            or meta.get("side")
        )
        status = str(row.get("status") or "").upper().strip() or None
        if local_id != expected.local_order_id:
            return False, status, "durable_identity_mismatch:local_order_id"
        if not client_id or client_id != expected.client_id:
            return False, status, "durable_identity_mismatch:client_id"
        if not mode or mode != expected.execution_mode:
            return False, status, "durable_identity_mismatch:execution_mode"
        if not signal_id:
            return False, status, "durable_identity_mismatch:signal_id_missing"
        if signal_id != expected.signal_id:
            return False, status, "durable_identity_mismatch:signal_id"
        if canonical_signal_id and canonical_signal_id != expected.canonical_signal_id:
            return False, status, "durable_identity_mismatch:canonical_signal_id"
        if not ticker:
            return False, status, "durable_identity_mismatch:ticker_missing"
        if ticker != expected.ticker:
            return False, status, "durable_identity_mismatch:ticker"
        if not side:
            return False, status, "durable_identity_mismatch:side_missing"
        if side != expected.side:
            return False, status, "durable_identity_mismatch:side"
        if pending_only:
            if status == "PENDING_TRIGGER":
                return True, status, "durable_pending_exact_identity"
            if not status:
                return False, status, "winner_row_status_missing"
            return False, status, f"winner_row_not_pending_{status.lower()}"
        if status in _TERMINAL_ENTRY:
            return True, status, "durable_terminal_exact_identity"
        if status == "PENDING_TRIGGER":
            return False, status, "row_pending"
        if not status:
            return False, status, "row_status_missing"
        return False, status, f"row_nonterminal_{status.lower()}"

    def _verify_pending_direction_winner(self, watched) -> bool:
        """Require exact durable identity before a winner can cancel losers."""
        expected = self._identity(watched)
        if not all(
            (
                expected.local_order_id,
                expected.client_id,
                expected.execution_mode,
                expected.signal_id,
                expected.ticker,
                expected.side,
                expected.canonical_signal_id,
            )
        ):
            return False
        try:
            row = self._read_order(expected.local_order_id)
        except Exception:
            return False
        proven, _status, _reason = self._verify_row(
            row, expected, pending_only=True
        )
        return proven

    def _persist_trigger_confirmation_authority(
        self, watched, *, require_pending_row: bool = False
    ) -> bool:
        expected = self._identity(watched)
        if require_pending_row:
            if not all(
                (
                    expected.local_order_id,
                    expected.client_id,
                    expected.execution_mode,
                    expected.signal_id,
                )
            ):
                return False
            if not self._verify_pending_direction_winner(watched):
                return False
        return super()._persist_trigger_confirmation_authority(
            watched,
            require_pending_row=require_pending_row,
            expected_execution_mode=(
                expected.execution_mode if require_pending_row else None
            ),
            expected_signal_id=(expected.signal_id if require_pending_row else None),
        )

    def _cancel_conflicting_watcher_with_proof(
        self, watched, expected: _Identity, *, cancel_reason: str
    ) -> ConflictCancellationProof:
        missing = [
            name for name, value in (
                ("local_order_id", expected.local_order_id),
                ("client_id", expected.client_id),
                ("execution_mode", expected.execution_mode),
                ("signal_id", expected.signal_id),
                ("canonical_signal_id", expected.canonical_signal_id),
                ("ticker", expected.ticker),
                ("side", expected.side),
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

    def _opposites(self, ticker: str, side: str, signal: dict | None = None) -> list:
        incoming_key = self._ownership_key(signal) if signal is not None else None
        with self._lock:
            return [
                item for item in self._pending
                if (item.is_active or getattr(item, "rearm_mode", False))
                and str(item.ticker).upper().strip() == ticker
                and str(item.side).upper().strip() != side
                and (
                    incoming_key is None
                    or self._ownership_key(item) is None
                    or self._ownership_key(item) == incoming_key
                )
            ]

    def _same_side(self, ticker: str, side: str, signal: dict | None = None) -> list:
        incoming_key = self._ownership_key(signal) if signal is not None else None
        with self._lock:
            return [
                item for item in self._pending
                if (item.is_active or getattr(item, "rearm_mode", False))
                and str(item.ticker).upper().strip() == ticker
                and str(item.side).upper().strip() == side
                and (
                    incoming_key is None
                    or self._ownership_key(item) is None
                    or self._ownership_key(item) == incoming_key
                )
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

    def _direction_event_audit(self, watched, reason_code: str, raw_reason: str, **extra) -> None:
        signal = getattr(watched, "signal", {}) or {}
        extra.setdefault("client_id", signal.get("client_id") or signal.get("client_email"))
        extra.setdefault("execution_mode", signal.get("execution_mode"))
        extra.setdefault("local_order_id", signal.get("local_order_id"))
        extra.setdefault("signal_id", signal.get("signal_id") or getattr(watched, "signal_id", ""))
        payload = self._build_watcher_audit_payload(
            watched,
            trigger_type="direction_claim",
            reason_code=reason_code,
            raw_reason=raw_reason,
            extra=extra,
        )
        try:
            self._persist_watcher_audit(
                (getattr(watched, "signal", {}) or {}).get("local_order_id"),
                payload,
            )
        except Exception:
            _base.log.debug("direction claim audit persistence failed", exc_info=True)

    def add_signal(
        self, signal: dict, *, registration_provenance_out: dict | None = None,
    ) -> bool:
        if registration_provenance_out is not None:
            registration_provenance_out["created_by_this_call"] = False
            registration_provenance_out["registration_token"] = None
        ticker = str((signal or {}).get("ticker") or "").upper().strip()
        side = _normalize_watcher_side((signal or {}).get("side"))
        if not ticker or not side:
            return super().add_signal(
                signal, registration_provenance_out=registration_provenance_out,
            )
        with self._watch_admission_gate:
            incoming_key = self._ownership_key(signal)
            if incoming_key is None:
                # A standalone legacy watcher may still be admitted without a
                # client/mode key.  Once another same-ticker watcher exists,
                # however, ownership is ambiguous and the safe result is a
                # fail-closed block without attempting an unprovable cancel.
                uncertain = self._opposites(ticker, side, signal)
                uncertain.extend(self._same_side(ticker, side, signal))
                if uncertain:
                    return self._block(
                        signal,
                        uncertain[0],
                        "ownership_identity_missing",
                        "ownership_identity_missing:client_id_or_execution_mode_or_ticker",
                    )
                return super().add_signal(
                    signal, registration_provenance_out=registration_provenance_out,
                )
            claimed_winner = self._won_direction_claim_winner(incoming_key)
            if claimed_winner is not None:
                return self._block(
                    signal,
                    claimed_winner,
                    "direction_claim_active",
                    "direction_claim_active:won_winner_still_owned",
                )
            opposites = self._opposites(ticker, side, signal)
            # Durable confirmed-breach evidence is lifecycle authority even
            # after restart.  It must never enter legacy stale/score pruning,
            # which would otherwise cancel the confirmed winner before the
            # durable-direction guard below can retain it.
            prune = [
                item for item in opposites
                if not self._has_durable_confirmed_direction_evidence(item)
                and self._prunable(signal, item)
            ]
            if prune and not self._prove_remove_all(
                signal, prune, "opposite_side_replaced_stale_or_weaker"
            ):
                return False
            opposites = self._opposites(ticker, side, signal)
            incoming_can_coarm = self._is_ordinary_admission(signal)
            protected_opposites = [
                item for item in opposites
                if not incoming_can_coarm or not self._is_coarmable_opposite(item)
            ]
            coarmable_opposites = [
                item for item in opposites
                if incoming_can_coarm and self._is_coarmable_opposite(item)
            ]
            durable_confirmed_opposites = [
                item for item in protected_opposites
                if self._has_durable_confirmed_direction_evidence(item)
            ]
            if durable_confirmed_opposites:
                # The process-local claim map is empty after restart, but a
                # confirmed trigger is durable lifecycle authority. Never let
                # legacy pre-breach score replacement cancel that winner.
                return self._block(
                    signal,
                    durable_confirmed_opposites[0],
                    "direction_claim_active",
                    "direction_claim_active:durable_confirmed_trigger_still_owned",
                )
            if protected_opposites:
                best = max(protected_opposites, key=lambda item: float(item.score or 0))
                if not self._candidate_wins(signal, best):
                    return self._block(
                        signal, best, "opposite_side_conflict",
                        "opposite_side_conflict:existing_watcher_wins",
                    )
                if not self._prove_remove_all(
                    signal, protected_opposites, "direction_flip_watcher_cancel"
                ):
                    return False
            remaining = [
                item for item in self._opposites(ticker, side, signal)
                if not self._is_coarmable_opposite(item)
            ]
            if remaining:
                return self._block(
                    signal, remaining[0], "conflict_cancel_unproven",
                    "conflict_cancel_unproven:opposite_reappeared_before_admission",
                )
            dedup_key = str(self._dedup_key_for_signal(signal) or "").strip()
            with self._lock:
                dedup_seen = bool(dedup_key and dedup_key in self._dedup_set)
            if dedup_seen:
                return super().add_signal(
                    signal, registration_provenance_out=registration_provenance_out,
                )
            same_side = self._same_side(ticker, side, signal)
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
            accepted = super().add_signal(
                signal, registration_provenance_out=registration_provenance_out,
            )
            if accepted and coarmable_opposites:
                with self._lock:
                    new_watched = next(
                        (
                            item for item in self._pending
                            if str((getattr(item, "signal", {}) or {}).get("signal_id") or "")
                            == str(signal.get("signal_id") or "")
                        ),
                        None,
                    )
                if new_watched is not None:
                    for existing in coarmable_opposites:
                        key = self._ownership_key(new_watched)
                        self._direction_event_audit(
                            new_watched,
                            "opposite_side_coarmed",
                            "healthy_pre_breach_opposite_retained_until_confirmed_breach",
                            ownership_key=key,
                            coarmed_local_order_id=(getattr(existing, "signal", {}) or {}).get("local_order_id"),
                            coarmed_signal_id=str(getattr(existing, "signal_id", "") or ""),
                            coarmed_direction=str(getattr(existing, "side", "") or ""),
                        )
                        self._direction_event_audit(
                            existing,
                            "opposite_side_coarmed",
                            "healthy_pre_breach_opposite_retained_until_confirmed_breach",
                            ownership_key=key,
                            coarmed_local_order_id=(getattr(new_watched, "signal", {}) or {}).get("local_order_id"),
                            coarmed_signal_id=str(getattr(new_watched, "signal_id", "") or ""),
                            coarmed_direction=str(getattr(new_watched, "side", "") or ""),
                        )
            return accepted

    def watch(
        self, plan, local_order_id: str, *, recovery_rearm: bool = False,
        no_cancel_on_reject: bool = False, materialization_resume: bool = False,
        registration_provenance_out: dict | None = None,
    ) -> bool:
        current_call = _CALL_RESULT.get()
        if current_call is not None and current_call.watcher_id == id(self):
            return self._watch_impl(
                plan,
                local_order_id,
                recovery_rearm=recovery_rearm,
                no_cancel_on_reject=no_cancel_on_reject,
                materialization_resume=materialization_resume,
                registration_provenance_out=registration_provenance_out,
            )
        token = _CALL_RESULT.set(_CallResult(id(self)))
        try:
            return self._watch_impl(
                plan,
                local_order_id,
                recovery_rearm=recovery_rearm,
                no_cancel_on_reject=no_cancel_on_reject,
                materialization_resume=materialization_resume,
                registration_provenance_out=registration_provenance_out,
            )
        finally:
            _CALL_RESULT.reset(token)

    def _watch_impl(
        self, plan, local_order_id: str, *, recovery_rearm: bool = False,
        no_cancel_on_reject: bool = False, materialization_resume: bool = False,
        registration_provenance_out: dict | None = None,
    ) -> bool:
        if registration_provenance_out is not None:
            registration_provenance_out["created_by_this_call"] = False
            registration_provenance_out["registration_token"] = None
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
            registration_provenance_out=registration_provenance_out,
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

    def _direction_key(self, watched) -> tuple[str, str, str] | None:
        key = self._ownership_key(watched)
        if key is None or not self._direction_identity_complete(watched):
            return None
        return key

    def _open_protection_key(self, watched):
        """Keep market-open protection aligned with directional ownership."""
        key = self._direction_key(watched)
        if key is not None:
            return ("direction", *key)
        return ("legacy", str(getattr(watched, "ticker", "") or "").strip().upper())

    def _won_direction_claim_winner(self, key):
        """Return a still-owned winner, clearing only stale process-local claims."""
        if key is None:
            return None
        with self._direction_claim_gate:
            claim = dict(self._direction_claims.get(key) or {})
        if claim.get("status") != "won":
            return None
        winner_local_id = str(claim.get("winner_local_order_id") or "")
        winner_signal_id = str(claim.get("winner_signal_id") or "")
        with self._lock:
            winner = next(
                (
                    item for item in self._pending
                    if self._direction_key(item) == key
                    and self._local_order_id(item) == winner_local_id
                    and self._signal_id(item) == winner_signal_id
                ),
                None,
            )
        if winner is not None:
            return winner
        # Admission and dispatch share _watch_admission_gate, so a missing
        # winner here means the process-local claim is stale and may be pruned.
        with self._direction_claim_gate:
            current = self._direction_claims.get(key) or {}
            if (
                current.get("status") == "won"
                and str(current.get("winner_local_order_id") or "") == winner_local_id
                and str(current.get("winner_signal_id") or "") == winner_signal_id
            ):
                self._direction_claims.pop(key, None)
        return None

    @staticmethod
    def _local_order_id(watched) -> str:
        return str((getattr(watched, "signal", {}) or {}).get("local_order_id") or "").strip()

    @staticmethod
    def _signal_id(watched) -> str:
        signal = getattr(watched, "signal", {}) or {}
        return str(getattr(watched, "signal_id", "") or signal.get("signal_id") or "").strip()

    def _set_direction_hold(self, watched, key, reason_code: str, raw_reason: str) -> None:
        with self._lock:
            if any(item is watched for item in self._pending):
                watched.state = WatchState.PENDING
                watched.deferred_retry_not_before = (
                    _datetime.now(_timezone.utc) + _base.timedelta(seconds=5)
                )
        self._direction_event_audit(
            watched,
            reason_code,
            raw_reason,
            ownership_key=key,
            held_local_order_id=self._local_order_id(watched),
            held_signal_id=self._signal_id(watched),
        )

    def _preexisting_crossed_at(self, watched):
        context = self._direction_poll_context or {}
        raw = (context.get("preexisting_crossed_at") or {}).get(id(watched))
        if raw is None:
            return None
        if isinstance(raw, _datetime):
            return raw
        try:
            return _base._parse_trigger_crossed_at(raw)
        except Exception:
            return None

    def _select_confirmed_winner(self, triggered: list):
        """Choose only from evidence that pre-dates this poll's callbacks."""
        prior = []
        for watched in triggered:
            crossed_at = self._preexisting_crossed_at(watched)
            if crossed_at is not None:
                prior.append((watched, crossed_at))
        if len(prior) == 1:
            return prior[0][0]
        if len(prior) != len(triggered) or len(prior) < 2:
            return None
        ordered = sorted(prior, key=lambda item: item[1])
        if ordered[0][1] == ordered[1][1]:
            return None
        return ordered[0][0]

    def _pending_direction_opposites(self, key, winner) -> list:
        with self._lock:
            return [
                item for item in self._pending
                if item is not winner
                and str(getattr(item, "ticker", "") or "").upper().strip() == str(key[2]).upper().strip()
                and (
                    self._direction_key(item) == key
                    or self._direction_key(item) is None
                )
                and str(getattr(item, "side", "") or "").upper().strip()
                != str(getattr(winner, "side", "") or "").upper().strip()
                and (
                    item.is_active
                    or getattr(item, "rearm_mode", False)
                    or getattr(item, "state", None) == WatchState.TRIGGERED
                )
            ]

    def _before_trigger_dispatch(self, completed):
        """Claim direction only after the complete confirmed-trigger batch exists.

        A same-poll CALL/PUT tie has no independent ordering authority in the
        legacy watcher: each timestamp is generated locally while iterating the
        list.  Such a tie is therefore retained as a HOLD instead of selecting
        the first callback.  A winner is dispatched only after every exact-key
        opposite is terminalized with cancellation proof.
        """
        completed = list(completed or [])
        trigger_groups: dict[tuple[str, str, str], list] = {}
        invalid_triggers = []
        retained = []
        for action, watched in completed:
            if action != "trigger":
                retained.append((action, watched))
                continue
            key = self._direction_key(watched)
            if key is None:
                invalid_triggers.append(watched)
            else:
                trigger_groups.setdefault(key, []).append(watched)

        with self._watch_admission_gate:
            with self._direction_claim_gate:
                with self._lock:
                    pending = list(self._pending)
                pending_keys = {
                    self._direction_key(item)
                    for item in pending
                    if self._direction_key(item) is not None
                }
                for key, claim in list(self._direction_claims.items()):
                    if key not in pending_keys:
                        self._direction_claims.pop(key, None)
                        continue
                    if claim.get("status") == "won":
                        winner_local_id = str(claim.get("winner_local_order_id") or "")
                        winner_signal_id = str(claim.get("winner_signal_id") or "")
                        if not any(
                            self._direction_key(item) == key
                            and self._local_order_id(item) == winner_local_id
                            and self._signal_id(item) == winner_signal_id
                            for item in pending
                        ):
                            self._direction_claims.pop(key, None)

            for watched in invalid_triggers:
                ticker = str(getattr(watched, "ticker", "") or "").upper().strip()
                side = str(getattr(watched, "side", "") or "").upper().strip()
                with self._lock:
                    unresolved_opposite = any(
                        item is not watched
                        and str(getattr(item, "ticker", "") or "").upper().strip() == ticker
                        and str(getattr(item, "side", "") or "").upper().strip() != side
                        and (
                            item.is_active
                            or getattr(item, "rearm_mode", False)
                            or getattr(item, "state", None) == WatchState.TRIGGERED
                        )
                        for item in self._pending
                    )
                if unresolved_opposite:
                    self._set_direction_hold(
                        watched,
                        None,
                        "direction_claim_ambiguous_hold",
                        "confirmed_trigger_missing_exact_direction_identity",
                    )
                else:
                    # No opposite exists to arbitrate. Preserve the legacy
                    # single-watcher callback path even when older recovery
                    # fixtures lack the newer client/mode identity fields.
                    retained.append(("trigger", watched))

            for key, triggered in trigger_groups.items():
                # Admission never intentionally creates same-side duplicates,
                # but a restart/fixture can.  Do not invent a directional claim
                # when the batch itself is internally ambiguous.
                if len({str(getattr(w, "side", "") or "").upper() for w in triggered}) != len(triggered):
                    with self._direction_claim_gate:
                        self._direction_claims[key] = {
                            "status": "ambiguous_hold",
                            "reason": "duplicate_side_in_confirmed_batch",
                        }
                    for watched in triggered:
                        self._set_direction_hold(
                            watched, key, "direction_claim_ambiguous_hold",
                            "duplicate_side_in_confirmed_batch",
                        )
                    continue

                with self._direction_claim_gate:
                    claim = dict(self._direction_claims.get(key) or {})

                if claim.get("status") == "won":
                    winner = next(
                        (
                            watched for watched in triggered
                            if self._local_order_id(watched)
                            == str(claim.get("winner_local_order_id") or "")
                            and self._signal_id(watched)
                            == str(claim.get("winner_signal_id") or "")
                        ),
                        None,
                    )
                    if winner is None:
                        with self._lock:
                            winner_still_pending = any(
                                self._direction_key(item) == key
                                and self._local_order_id(item)
                                == str(claim.get("winner_local_order_id") or "")
                                and self._signal_id(item)
                                == str(claim.get("winner_signal_id") or "")
                                for item in self._pending
                            )
                        if winner_still_pending:
                            for watched in triggered:
                                self._set_direction_hold(
                                    watched, key,
                                    "direction_claim_ambiguous_hold",
                                    "existing_direction_claim_winner_not_confirmed_this_poll",
                                )
                            continue
                        with self._direction_claim_gate:
                            self._direction_claims.pop(key, None)
                        claim = {}
                    else:
                        # The claimed winner is the only watcher allowed to
                        # reach the callback; any opposite in this batch is a
                        # loser and is handled by the proof path below.
                        pass

                if claim.get("status") == "ambiguous_hold":
                    # A cancellation-proof failure is retryable once the
                    # durable row becomes readable/terminal.  Keep a genuine
                    # same-poll ordering ambiguity fail-closed, but clear the
                    # retryable claim so the normal proof path runs again.
                    if claim.get("reason") in {
                        "opposite_cancellation_unproven",
                        "winner_authority_unproven",
                    }:
                        with self._direction_claim_gate:
                            self._direction_claims.pop(key, None)
                        claim = {}
                    else:
                        pending_opposites = self._pending_direction_opposites(key, triggered[0])
                        if len(triggered) > 1 or pending_opposites:
                            for watched in triggered:
                                self._set_direction_hold(
                                    watched, key, "direction_claim_ambiguous_hold",
                                    "same_poll_confirmed_direction_order_unproven",
                                )
                            continue
                        with self._direction_claim_gate:
                            self._direction_claims.pop(key, None)
                        claim = {}

                if len(triggered) > 1:
                    winner = self._select_confirmed_winner(triggered)
                    if winner is None:
                        with self._direction_claim_gate:
                            self._direction_claims[key] = {
                                "status": "ambiguous_hold",
                                "reason": "same_poll_confirmed_direction_order_unproven",
                            }
                        for watched in triggered:
                            self._set_direction_hold(
                                watched, key, "direction_claim_ambiguous_hold",
                                "same_poll_confirmed_direction_order_unproven",
                            )
                        continue
                else:
                    winner = triggered[0]

                losers = []
                for loser in self._pending_direction_opposites(key, winner):
                    if loser not in losers:
                        losers.append(loser)
                for loser in triggered:
                    if loser is not winner and loser not in losers:
                        losers.append(loser)

                # A loser must never be canceled while the selected winner's
                # trigger authority exists only in process memory.  Persist the
                # winner first; a false return/exception is a fail-closed HOLD.
                if losers and not self._persist_trigger_confirmation_authority(
                    winner, require_pending_row=True
                ):
                    with self._direction_claim_gate:
                        self._direction_claims[key] = {
                            "status": "ambiguous_hold",
                            "reason": "winner_authority_unproven",
                        }
                    self._set_direction_hold(
                        winner,
                        key,
                        "direction_claim_authority_unproven_hold",
                        "winner_confirmed_trigger_authority_not_durable",
                    )
                    for watched in triggered:
                        if watched is not winner:
                            self._set_direction_hold(
                                watched,
                                key,
                                "direction_claim_authority_unproven_hold",
                                "winner_confirmed_trigger_authority_not_durable",
                            )
                    continue

                cancel_failure = None
                for loser in losers:
                    with self._lock:
                        if not any(item is loser for item in self._pending):
                            continue
                        expected = self._identity(loser)
                    proof = self._cancel_conflicting_watcher_with_proof(
                        loser,
                        expected,
                        cancel_reason="confirmed_breach_direction_claim_lost",
                    )
                    if not proof.proven_terminal:
                        cancel_failure = proof
                        break
                    if not self._remove_after_proof(loser, expected, proof):
                        cancel_failure = ConflictCancellationProof(
                            False,
                            "retained_owner",
                            "conflict_cancel_unproven:watcher_identity_changed_after_proof",
                            expected.local_order_id,
                            proof.durable_status,
                            proof.cancel_returned,
                        )
                        break
                    self._direction_event_audit(
                        loser,
                        "direction_claim_lost",
                        "opposite_direction_cancelled_after_confirmed_breach_claim",
                        ownership_key=key,
                        winner_local_order_id=self._local_order_id(winner),
                        winner_signal_id=self._signal_id(winner),
                        winner_direction=str(getattr(winner, "side", "") or ""),
                        cancellation_proof=proof.reason_code,
                    )

                if cancel_failure is not None:
                    with self._direction_claim_gate:
                        self._direction_claims[key] = {
                            "status": "ambiguous_hold",
                            "reason": "opposite_cancellation_unproven",
                        }
                    self._set_direction_hold(
                        winner,
                        key,
                        "direction_claim_cancel_unproven_hold",
                        cancel_failure.reason_code,
                    )
                    for watched in triggered:
                        if watched is not winner:
                            self._set_direction_hold(
                                watched,
                                key,
                                "direction_claim_cancel_unproven_hold",
                                cancel_failure.reason_code,
                            )
                    continue

                with self._direction_claim_gate:
                    self._direction_claims[key] = {
                        "status": "won",
                        "winner_local_order_id": self._local_order_id(winner),
                        "winner_signal_id": self._signal_id(winner),
                        "winner_direction": str(getattr(winner, "side", "") or ""),
                    }
                self._direction_event_audit(
                    winner,
                    "direction_claim_won",
                    "confirmed_breach_selected_direction_before_execution_callback",
                    ownership_key=key,
                    winner_local_order_id=self._local_order_id(winner),
                    winner_signal_id=self._signal_id(winner),
                    winner_direction=str(getattr(winner, "side", "") or ""),
                )
                retained.append(("trigger", winner))

        return retained

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
            prior_direction_context = self._direction_poll_context
            with self._lock:
                self._direction_poll_context = {
                    "preexisting_crossed_at": {
                        id(item): getattr(item, "trigger_crossed_at", None)
                        for item in self._pending
                    }
                }
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
                self._direction_poll_context = prior_direction_context
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
