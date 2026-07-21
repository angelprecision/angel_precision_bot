"""P0 exit truth-coordination shim.

This package shadows the legacy top-level ``ap_exit_engine.py`` module and
changes only the quote/evaluation boundary used by the existing exit engine.
The legacy engine remains the sole broker submit authority and retains all
order ownership, idempotency, fill reconciliation, EOD, kill-switch, and proof
behavior.
"""
from __future__ import annotations

import importlib.util as _importlib_util
import json as _json
import logging as _logging
import os as _os
import sys as _sys
from dataclasses import dataclass as _dataclass, field as _field
from datetime import datetime as _datetime, timezone as _timezone
from pathlib import Path as _Path
from typing import Any as _Any, Optional as _Optional

_BASE_PATH = _Path(__file__).resolve().parent.parent / "ap_exit_engine.py"
_BASE_MODULE_NAME = "_ap_exit_engine_base"
_spec = _importlib_util.spec_from_file_location(_BASE_MODULE_NAME, _BASE_PATH)
if _spec is None or _spec.loader is None:  # pragma: no cover
    raise ImportError(f"Unable to load legacy exit engine from {_BASE_PATH}")
_base = _importlib_util.module_from_spec(_spec)
_sys.modules[_BASE_MODULE_NAME] = _base
_spec.loader.exec_module(_base)
for _name in dir(_base):
    if not _name.startswith("__") or _name == "__doc__":
        globals()[_name] = getattr(_base, _name)

log = _logging.getLogger("ap.exit_engine.soft_truth")
_BaseManagedPosition = _base.ManagedPosition
_BaseAPExitEngine = _base.APExitEngine
_legacy_evaluate_exit = _base.evaluate_exit
_legacy_apply_option_quote = _base._apply_option_quote_for_decision
_legacy_build_exit_decision_stamp = _base.build_exit_decision_stamp
_legacy_classify_exit_decision = _base._classify_exit_decision

TOUCHED_PROFIT_ARM_PCT = float(_os.getenv("TOUCHED_PROFIT_ARM_PCT", "0.04"))
TOUCHED_PROFIT_CONFIRM_SNAPSHOTS = max(
    2, int(_os.getenv("TOUCHED_PROFIT_CONFIRM_SNAPSHOTS", "2"))
)
EXIT_DECISION_QUOTE_MAX_AGE_SEC = float(
    _os.getenv("EXIT_DECISION_QUOTE_MAX_AGE_SEC", "5")
)
EXIT_DECISION_QUOTE_FUTURE_TOLERANCE_SEC = float(
    _os.getenv("EXIT_DECISION_QUOTE_FUTURE_TOLERANCE_SEC", "2")
)
SOFT_EXIT_THESIS_GRACE_MINUTES = float(
    _os.getenv("SOFT_EXIT_THESIS_GRACE_MINUTES", "10")
)

SOFT_EXIT_DEFERRED_UNDERLYING_UNAVAILABLE = (
    "SOFT_EXIT_DEFERRED_UNDERLYING_UNAVAILABLE"
)
SOFT_EXIT_SUPPRESSED_THESIS_CONFIRMING = "SOFT_EXIT_SUPPRESSED_THESIS_CONFIRMING"

_CANONICAL_SOFT_CODES = frozenset({
    "TOUCHED_PROFIT_FLOOR",
    "SOFT_OPTION_STOP",
    "RUNNER_TRAIL",
    "SMALL_WIN_CAPTURE",
})
_ALWAYS_AUTHORITATIVE_CODES = frozenset({
    "UNDERLYING_STOP_CONFIRMED",
    "HARD_OPTION_STOP",
    "EOD_FORCE_CLOSE",
    "SENTINEL_FORCED_EXIT",
    "EMERGENCY_STOP",
    "MAX_LOSS",
    "THETA_STOP",
    "MANUAL_CLOSE",
    "MANUAL_EXIT",
    "BROKER_FORCE_CLOSE",
    "BROKER_POSITION_GONE",
    "RECONCILER_BROKER_GONE",
})


@_dataclass
class ManagedPosition(_BaseManagedPosition):
    """Legacy managed position plus durable executable-profit evidence."""

    profit_lock_armed_at: _Optional[_datetime] = None
    profit_lock_arm_bid: float = 0.0
    profit_lock_arm_pnl_pct: float = 0.0
    peak_executable_bid: float = 0.0
    peak_executable_pnl_pct: float = 0.0
    profit_lock_confirmation_count: int = 0
    profit_lock_last_confirmation_snapshot_id: str = ""
    profit_lock_last_confirmation_quote_ts: _Optional[_datetime] = None
    profit_lock_option_quote_source: str = ""
    profit_lock_underlying_quote_source: str = ""
    exit_decision_quote_snapshot: dict[str, _Any] = _field(default_factory=dict)
    last_soft_exit_deferred_at: _Optional[_datetime] = None
    last_soft_exit_deferred_reason: str = ""
    last_soft_exit_suppressed_at: _Optional[_datetime] = None
    last_soft_exit_suppressed_reason: str = ""

    def __setattr__(self, name: str, value: _Any) -> None:
        # The legacy engine writes touched_profit=True for any positive P&L in
        # its pre-gate peak tracker. For canonical positions that assignment is
        # no longer authoritative: durable protection may arm only after the
        # configured executable-bid confirmation contract is satisfied.
        if name in {"touched_profit", "touchedprofit"} and bool(value):
            if not getattr(self, "profit_lock_armed_at", None):
                value = False
        super().__setattr__(name, value)

    def __post_init__(self):
        super().__post_init__()
        self.profit_lock_confirmation_count = max(
            0, int(self.profit_lock_confirmation_count or 0)
        )
        # Pre-PR touched-profit state without the new arm evidence is unproven.
        if not self.profit_lock_armed_at:
            super().__setattr__("touched_profit", False)


def _coerce_dt(value: _Any) -> _Optional[_datetime]:
    if isinstance(value, _datetime):
        return value if value.tzinfo else value.replace(tzinfo=_timezone.utc)
    if isinstance(value, (int, float)):
        try:
            value_f = float(value)
            if value_f > 10_000_000_000:
                value_f /= 1000.0
            return _datetime.fromtimestamp(value_f, _timezone.utc)
        except Exception:
            return None
    if isinstance(value, str) and value.strip():
        try:
            parsed = _datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=_timezone.utc)
        except Exception:
            return None
    return None


def _is_canonical_managed_position(pos: _Any) -> bool:
    return isinstance(pos, ManagedPosition)


def _canonical_reason_code(decision: "ExitDecision") -> str:
    existing = str(getattr(decision, "reason_code", "") or "").upper().strip()
    text = str(getattr(decision, "reason", "") or "").upper()
    legacy = existing or str(_legacy_classify_exit_decision(decision) or "").upper()

    if legacy in {"STOP_HIT"} or "UNDERLYING STOP" in text:
        return "UNDERLYING_STOP_CONFIRMED"
    if legacy in {"HARD_STOP", "HARD_OPTION_STOP"} or "HARD STOP" in text:
        return "HARD_OPTION_STOP"
    if legacy in {"TOUCHED_PROFIT_STOP", "PROFIT_LOCK"} or any(
        fragment in text for fragment in ("TOUCHED PROFIT STOP", "PROFIT LOCK")
    ):
        return "TOUCHED_PROFIT_FLOOR"
    if legacy in {"RUNNER_TRAIL", "TRAILING_STOP"} or any(
        fragment in text for fragment in ("RUNNER TRAIL", "TRAILING STOP", "PEAK DRAWDOWN")
    ):
        return "RUNNER_TRAIL"
    if legacy == "SMALL_WIN_LOCK" or "SMALL WIN" in text:
        return "SMALL_WIN_CAPTURE"
    if legacy in {
        "THESIS_FAIL_SOFT_STOP",
        "THESIS_STALE_SOFT_STOP",
        "NEVER_GREEN_STOP",
        "DEEP_LOSS_STOP",
        "SOFT_LOSS",
    } or any(
        fragment in text
        for fragment in (
            "SOFT STOP",
            "SOFT_STOP",
            "NEVER GREEN STOP",
            "DEEP_LOSS_STOP",
            "THESIS_FAIL_SOFT_STOP",
        )
    ):
        return "SOFT_OPTION_STOP"
    return legacy or "UNKNOWN_EXIT"


def _snapshot_truth(pos: ManagedPosition) -> tuple[bool, list[str], dict[str, _Any]]:
    snap = dict(getattr(pos, "exit_decision_quote_snapshot", None) or {})
    reasons = list(snap.get("invalid_reasons") or [])
    if not snap:
        reasons.append("missing_canonical_exit_snapshot")
        return False, sorted(set(reasons)), snap

    now = _datetime.now(_timezone.utc)
    snapshot_ts = _coerce_dt(snap.get("snapshot_timestamp"))
    option_ts = _coerce_dt(snap.get("option_quote_timestamp"))
    underlying_ts = _coerce_dt(snap.get("underlying_quote_timestamp"))
    for label, ts in (("snapshot", snapshot_ts), ("option", option_ts), ("underlying", underlying_ts)):
        if ts is None:
            reasons.append(f"missing_{label}_quote_timestamp")
            continue
        age = (now - ts).total_seconds()
        if age > EXIT_DECISION_QUOTE_MAX_AGE_SEC:
            reasons.append(f"stale_{label}_quote")
        if age < -EXIT_DECISION_QUOTE_FUTURE_TOLERANCE_SEC:
            reasons.append(f"future_dated_{label}_quote")

    if float(snap.get("underlying_mid") or 0.0) <= 0:
        reasons.append("missing_underlying_mid")
    if not snap.get("underlying_truth_valid", False):
        reasons.append("underlying_truth_invalid")
    if not snap.get("same_evaluation_cycle", False):
        reasons.append("cross_cycle_quote_pair")
    if not snap.get("same_provider", False):
        reasons.append("cross_provider_quote_pair")
    if not snap.get("same_domain", False):
        reasons.append("cross_domain_quote_pair")
    if str(snap.get("execution_mode") or "").lower() not in {"paper", "live"}:
        reasons.append("execution_mode_unproven")

    reasons = sorted(set(str(r) for r in reasons if r))
    return not reasons, reasons, snap


def _underlying_confirms_from_snapshot(pos: ManagedPosition, snap: dict[str, _Any]) -> tuple[bool, str]:
    entry = float(getattr(pos, "underlying_entry", 0.0) or 0.0)
    current = float(snap.get("underlying_mid") or 0.0)
    side = str(getattr(pos, "side", "") or "").upper()
    if entry <= 0 or current <= 0:
        return False, "underlying_entry_or_mid_missing"
    move = (current - entry) / entry
    if side == "CALL":
        return current >= entry * 0.995, f"call_underlying_move={move:+.4%}"
    if side == "PUT":
        return current <= entry * 1.005, f"put_underlying_move={move:+.4%}"
    return False, "unknown_option_side"


def _deferred_decision(
    pos: ManagedPosition,
    *,
    pnl_pct: float,
    invalid_reasons: list[str],
    original_code: str,
) -> "ExitDecision":
    now = _datetime.now(_timezone.utc)
    pos.last_soft_exit_deferred_at = now
    pos.last_soft_exit_deferred_reason = ",".join(invalid_reasons)
    return ExitDecision(
        action="HOLD",
        quantity=0,
        reason=(
            f"{SOFT_EXIT_DEFERRED_UNDERLYING_UNAVAILABLE} — original={original_code} | "
            f"invalid={','.join(invalid_reasons) or 'unknown'} | refresh requested"
        ),
        urgency="NORMAL",
        pnl_pct=pnl_pct,
        reason_code=SOFT_EXIT_DEFERRED_UNDERLYING_UNAVAILABLE,
    )


def _suppressed_decision(
    pos: ManagedPosition,
    *,
    pnl_pct: float,
    original_code: str,
    detail: str,
) -> "ExitDecision":
    now = _datetime.now(_timezone.utc)
    pos.last_soft_exit_suppressed_at = now
    pos.last_soft_exit_suppressed_reason = detail
    return ExitDecision(
        action="HOLD",
        quantity=0,
        reason=(
            f"{SOFT_EXIT_SUPPRESSED_THESIS_CONFIRMING} — original={original_code} | "
            f"{detail} | bounded_grace={SOFT_EXIT_THESIS_GRACE_MINUTES:.1f}m"
        ),
        urgency="NORMAL",
        pnl_pct=pnl_pct,
        reason_code=SOFT_EXIT_SUPPRESSED_THESIS_CONFIRMING,
    )


def _touched_profit_floor(pos: ManagedPosition) -> float:
    maximum = float(getattr(pos, "max_profit_seen", 0.0) or 0.0)
    if maximum >= 0.25:
        return 0.12
    if maximum >= 0.15:
        return 0.08
    if maximum >= 0.10:
        return 0.05
    if maximum >= 0.05:
        return 0.03
    return 0.0


def _soft_candidate_code(
    pos: ManagedPosition,
    decision: "ExitDecision",
    bid_pnl: float,
) -> str:
    code = _canonical_reason_code(decision)
    if code in _CANONICAL_SOFT_CODES:
        return code

    reason = str(getattr(decision, "reason", "") or "").upper()
    if any(fragment in reason for fragment in (
        "STOP_BREACH_STARTED",
        "STOP_BREACH_CONFIRMING",
        "STOP_BREACH_RESET",
        "SOFT_LOSS_WATCH",
        "SOFT_STOP_SUPPRESSED",
    )):
        return "SOFT_OPTION_STOP"

    if (
        bool(getattr(pos, "touched_profit", False))
        and int(getattr(pos, "scale_outs_done", 0) or 0) == 0
        and bid_pnl <= _touched_profit_floor(pos)
    ):
        return "TOUCHED_PROFIT_FLOOR"

    # This catches a soft/never-green threshold crossing whose legacy branch is
    # still inside its bounded confirmation HOLD state. It does not authorize
    # an exit; it only makes missing current-cycle underlying truth visible.
    if not bool(getattr(pos, "touched_profit", False)):
        soft_loss = float(_os.getenv("SOFT_LOSS_STOP_PCT", "-0.12"))
        if bid_pnl <= soft_loss:
            return "SOFT_OPTION_STOP"
    return ""


def evaluate_exit(pos: ManagedPosition, now_et: _Optional[_datetime] = None) -> "ExitDecision":
    """Evaluate with bid authority and current-cycle underlying truth.

    The legacy rule engine still decides thresholds and precedence. This wrapper
    only (1) makes the existing hard option stop unconditionally authoritative,
    (2) normalizes the actual rule taxonomy, and (3) converts non-emergency soft
    actions into HOLD when current-cycle underlying truth is unproven or still
    confirms the thesis inside the bounded grace window.
    """
    if not _is_canonical_managed_position(pos):
        return _legacy_evaluate_exit(pos, now_et)

    bid = float(getattr(pos, "current_bid", 0.0) or 0.0)
    entry = float(getattr(pos, "entry_price", 0.0) or 0.0)
    bid_pnl = ((bid - entry) / entry) if bid > 0 and entry > 0 else 0.0
    hard_stop, _, _ = _base._effective_thresholds(pos)
    if bool(getattr(pos, "executable_quote_valid", False)) and bid_pnl <= hard_stop:
        return ExitDecision(
            action="STOP",
            quantity=max(0, int(getattr(pos, "quantity_remaining", 0) or 0)),
            reason=(
                f"HARD OPTION STOP — executable bid P&L {bid_pnl*100:.1f}% "
                f"breached unchanged hard limit {hard_stop*100:.1f}%"
            ),
            urgency="IMMEDIATE",
            pnl_pct=bid_pnl,
            reason_code="HARD_OPTION_STOP",
        )

    decision = _legacy_evaluate_exit(pos, now_et)
    code = _canonical_reason_code(decision)
    decision.reason_code = code

    if code in _ALWAYS_AUTHORITATIVE_CODES:
        return decision

    candidate_code = _soft_candidate_code(pos, decision, bid_pnl)
    if not candidate_code:
        return decision

    valid, invalid_reasons, snap = _snapshot_truth(pos)
    if not valid:
        return _deferred_decision(
            pos,
            pnl_pct=float(getattr(decision, "pnl_pct", bid_pnl) or bid_pnl),
            invalid_reasons=invalid_reasons,
            original_code=candidate_code,
        )

    confirms, confirm_detail = _underlying_confirms_from_snapshot(pos, snap)
    underlying_stop_breached = bool(getattr(pos, "is_at_stop", False))
    age_min = float(_base._position_age_minutes(pos))
    if (
        confirms
        and not underlying_stop_breached
        and age_min <= SOFT_EXIT_THESIS_GRACE_MINUTES
    ):
        return _suppressed_decision(
            pos,
            pnl_pct=float(getattr(decision, "pnl_pct", bid_pnl) or bid_pnl),
            original_code=candidate_code,
            detail=f"{confirm_detail}; age={age_min:.1f}m",
        )
    return decision


def _apply_option_quote_for_decision(
    pos: ManagedPosition,
    *,
    bid: float,
    ask: float,
    mark: float,
    quote_ts: _Optional[_datetime] = None,
    source: str = "",
):
    """Use bid for canonical PAPER and LIVE positions; retain legacy duck-type compatibility."""
    if not _is_canonical_managed_position(pos):
        return _legacy_apply_option_quote(
            pos, bid=bid, ask=ask, mark=mark, quote_ts=quote_ts, source=source
        )

    raw_mode = str(getattr(pos, "execution_mode", "") or "").strip()
    norm_mode = raw_mode.lower()
    pricing_mode = norm_mode if norm_mode in {"paper", "live"} else "live_risk_unproven"
    mid = round((bid + ask) / 2.0, 4) if bid > 0 and ask > 0 else float(mark or ask or 0.0)
    analytics_mark = float(mark or mid or 0.0)
    valid = bid > 0
    executable = float(bid if valid else 0.0)
    src = source or ("bid" if valid else "bid_missing")

    pos.analytics_mark_price = analytics_mark
    try:
        pos.analyticsmarkprice = analytics_mark
    except Exception:
        pass
    pos.current_bid = float(bid or 0.0)
    pos.current_ask = float(ask or 0.0)
    try:
        pos.currentbid, pos.currentask = pos.current_bid, pos.current_ask
    except Exception:
        pass
    pos.current_option_price = executable
    try:
        pos.currentoptionprice = executable
    except Exception:
        pass
    pos.executable_exit_price = executable
    pos.executable_quote_valid = valid
    pos.live_executable_price_source = src
    try:
        pos.liveexecutablepricesource = src
    except Exception:
        pass
    pos.pricing_mode = pricing_mode
    pos.raw_execution_mode = raw_mode
    if quote_ts is not None:
        pos.last_option_quote_update_ts = quote_ts
        try:
            pos.lastoptionquoteupdatets = quote_ts
        except Exception:
            pass
    return ExecutableQuoteApplication(
        pricing_mode=pricing_mode,
        executable_price=executable,
        executable_valid=valid,
        analytics_mark=analytics_mark,
        source=src,
    )


def build_exit_decision_stamp(
    pos: ManagedPosition,
    decision: "ExitDecision",
    **kwargs: _Any,
) -> dict[str, _Any]:
    payload = _legacy_build_exit_decision_stamp(pos, decision, **kwargs)
    snapshot = dict(getattr(pos, "exit_decision_quote_snapshot", None) or {})
    payload["reason_code"] = _canonical_reason_code(decision)
    payload["decision_quote_snapshot"] = snapshot
    payload["executable_bid_authority"] = {
        "bid": float(getattr(pos, "current_bid", 0.0) or 0.0),
        "entry_price": float(getattr(pos, "entry_price", 0.0) or 0.0),
        "pnl_pct": float(getattr(pos, "option_pnl_pct", 0.0) or 0.0),
        "source": str(getattr(pos, "live_executable_price_source", "") or ""),
        "valid": bool(getattr(pos, "executable_quote_valid", False)),
    }
    return payload


class APExitEngine(_BaseAPExitEngine):
    """Legacy APExitEngine with canonical snapshot ingestion only."""

    def apply_quote_snapshots(self, snapshots: list[dict[str, _Any]]) -> None:
        if not snapshots:
            return
        with self._lock:
            for raw in snapshots:
                snap = dict(raw or {})
                pid = str(snap.get("position_id") or snap.get("positionid") or "")
                pos = self._positions_by_id.get(pid) if pid else None
                if pos is None:
                    contract = str(snap.get("option_symbol") or snap.get("optionsymbol") or "").upper()
                    client = str(snap.get("client_id") or "")
                    mode = str(snap.get("execution_mode") or "").lower()
                    matches = [
                        p for p in self._positions
                        if not p.closed
                        and str(getattr(p, "option_symbol", "") or "").upper() == contract
                        and (not client or str(getattr(p, "client_id", "") or "") == client)
                        and (not mode or str(getattr(p, "execution_mode", "") or "").lower() == mode)
                    ]
                    pos = matches[0] if len(matches) == 1 else None
                if pos is None or not _is_canonical_managed_position(pos):
                    continue
                pos.exit_decision_quote_snapshot = snap
                if "underlying_mid" in snap:
                    pos.current_underlying = float(snap.get("underlying_mid") or 0.0)
                option_ts = _coerce_dt(snap.get("option_quote_timestamp"))
                underlying_ts = _coerce_dt(snap.get("underlying_quote_timestamp"))
                if option_ts is not None:
                    pos.last_option_quote_update_ts = option_ts
                if underlying_ts is not None:
                    pos.last_underlying_quote_update_ts = underlying_ts

    applyquotesnapshots = apply_quote_snapshots

    def _request_soft_truth_refresh(self, pos: ManagedPosition) -> bool:
        return bool(self._request_immediate_quote_retry(pos))

    def _hydrate_profit_lock_state_from_db(self, pos: ManagedPosition) -> None:
        identity = self._protective_position_identity(pos)
        if identity is None:
            return
        try:
            from ap.db import conn, run_with_retry

            def _read():
                with conn() as c:
                    c.execute(
                        """
                        SELECT COALESCE(meta, '{}'::jsonb) AS meta
                        FROM positions
                        WHERE id = %s
                          AND client_id = %s
                          AND LOWER(COALESCE(execution_mode, '')) = %s
                          AND COALESCE(NULLIF(contract, ''), option_symbol, '') = %s
                        LIMIT 1
                        """,
                        (
                            identity.position_id,
                            identity.client_id,
                            identity.execution_mode,
                            identity.contract,
                        ),
                    )
                    row = c.fetchone()
                    if not row:
                        return {}
                    if hasattr(row, 'get'):
                        return row.get('meta') or {}
                    return row[0]

            meta = run_with_retry(_read) or {}
            if isinstance(meta, str):
                meta = _json.loads(meta)
            state = dict((meta or {}).get("exit_executable_truth") or {})
            if not state:
                return
            pos.profit_lock_armed_at = _coerce_dt(state.get("profit_lock_armed_at"))
            pos.profit_lock_arm_bid = float(state.get("profit_lock_arm_bid") or 0.0)
            pos.profit_lock_arm_pnl_pct = float(state.get("profit_lock_arm_pnl_pct") or 0.0)
            pos.peak_executable_bid = float(state.get("peak_executable_bid") or 0.0)
            pos.peak_executable_pnl_pct = float(state.get("peak_executable_pnl_pct") or 0.0)
            pos.profit_lock_confirmation_count = int(state.get("confirmation_count") or 0)
            pos.touched_profit = bool(pos.profit_lock_armed_at)
            pos.peak_pnl_pct = pos.peak_executable_pnl_pct
            pos.max_profit_seen = pos.peak_executable_pnl_pct
        except Exception as exc:
            log.debug("profit-lock hydration unavailable for %s: %s", getattr(pos, "position_id", "?"), exc)

    def add_position(self, pos: ManagedPosition):
        if _is_canonical_managed_position(pos):
            self._hydrate_profit_lock_state_from_db(pos)
        return super().add_position(pos)

    def _emit_exit_decision_stamp(
        self,
        pos: ManagedPosition,
        decision: "ExitDecision",
        *,
        now_et: _Optional[_datetime] = None,
    ) -> None:
        code = _canonical_reason_code(decision)
        decision.reason_code = code
        if code == SOFT_EXIT_DEFERRED_UNDERLYING_UNAVAILABLE:
            refreshed = self._request_soft_truth_refresh(pos)
            self._emit_exit_event(
                pos,
                decision="HOLD",
                reason_code=SOFT_EXIT_DEFERRED_UNDERLYING_UNAVAILABLE,
                explanation=(
                    "Soft exit deferred because the current-cycle underlying "
                    "observation is missing, stale, future-dated, cross-cycle, "
                    "or from the wrong provider/domain."
                ),
                stage="exit_decision",
                extra_inputs={
                    "refresh_requested": refreshed,
                    "decision_quote_snapshot": dict(
                        getattr(pos, "exit_decision_quote_snapshot", None) or {}
                    ),
                    "proposed_rule": str(getattr(decision, "reason", "") or ""),
                },
            )
            self._persist_peak_state_to_db(pos)
        elif code == SOFT_EXIT_SUPPRESSED_THESIS_CONFIRMING:
            self._emit_exit_event(
                pos,
                decision="HOLD",
                reason_code=SOFT_EXIT_SUPPRESSED_THESIS_CONFIRMING,
                explanation=(
                    "Non-emergency option-only exit suppressed inside the bounded "
                    "grace window because fresh same-cycle underlying truth still "
                    "confirms the position direction."
                ),
                stage="exit_decision",
                extra_inputs={
                    "decision_quote_snapshot": dict(
                        getattr(pos, "exit_decision_quote_snapshot", None) or {}
                    ),
                },
            )
            self._persist_peak_state_to_db(pos)
        return super()._emit_exit_decision_stamp(pos, decision, now_et=now_et)

    def _persist_peak_state_to_db(self, pos) -> bool:
        legacy_ok = bool(super()._persist_peak_state_to_db(pos))
        if not _is_canonical_managed_position(pos):
            return legacy_ok
        identity = self._protective_position_identity(pos)
        if identity is None:
            return legacy_ok
        snapshot = dict(getattr(pos, "exit_decision_quote_snapshot", None) or {})
        patch = {
            "exit_executable_truth": {
                "profit_lock_armed_at": (
                    pos.profit_lock_armed_at.isoformat() if pos.profit_lock_armed_at else None
                ),
                "profit_lock_arm_bid": pos.profit_lock_arm_bid,
                "profit_lock_arm_pnl_pct": pos.profit_lock_arm_pnl_pct,
                "peak_executable_bid": pos.peak_executable_bid,
                "peak_executable_pnl_pct": pos.peak_executable_pnl_pct,
                "confirmation_count": pos.profit_lock_confirmation_count,
                "option_quote_source": pos.profit_lock_option_quote_source,
                "underlying_quote_source": pos.profit_lock_underlying_quote_source,
                "decision_quote_snapshot": snapshot,
                "last_soft_exit_deferred_at": (
                    pos.last_soft_exit_deferred_at.isoformat() if pos.last_soft_exit_deferred_at else None
                ),
                "last_soft_exit_deferred_reason": pos.last_soft_exit_deferred_reason,
                "last_soft_exit_suppressed_at": (
                    pos.last_soft_exit_suppressed_at.isoformat() if pos.last_soft_exit_suppressed_at else None
                ),
                "last_soft_exit_suppressed_reason": pos.last_soft_exit_suppressed_reason,
            }
        }
        try:
            from ap.db import conn, run_with_retry

            def _update():
                with conn() as c:
                    c.execute(
                        """
                        UPDATE positions
                        SET meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb,
                            updated_at = NOW()
                        WHERE id = %s
                          AND client_id = %s
                          AND LOWER(COALESCE(execution_mode, '')) = %s
                          AND COALESCE(NULLIF(contract, ''), option_symbol, '') = %s
                          AND status IN ('OPEN', 'CLOSING')
                        """,
                        (
                            _json.dumps(patch, default=str),
                            identity.position_id,
                            identity.client_id,
                            identity.execution_mode,
                            identity.contract,
                        ),
                    )
                    return int(c.rowcount or 0)

            return int(run_with_retry(_update) or 0) == 1 or legacy_ok
        except Exception as exc:
            log.debug("executable truth persistence unavailable for %s: %s", identity.position_id, exc)
            return legacy_ok


# Patch globals used by inherited legacy methods. The inherited function objects
# resolve these names in the private base module, not this package namespace.
_base.ManagedPosition = ManagedPosition
_base.APExitEngine = APExitEngine
_base.evaluate_exit = evaluate_exit
_base._apply_option_quote_for_decision = _apply_option_quote_for_decision
_base.build_exit_decision_stamp = build_exit_decision_stamp
_base._classify_exit_decision = lambda decision: _canonical_reason_code(decision)
_base.FORCED_RISK_EXIT_CODES = set(_base.FORCED_RISK_EXIT_CODES) | {
    "HARD_OPTION_STOP",
    "UNDERLYING_STOP_CONFIRMED",
}

# Keep public package exports authoritative.
globals().update({
    "ManagedPosition": ManagedPosition,
    "APExitEngine": APExitEngine,
    "evaluate_exit": evaluate_exit,
    "_apply_option_quote_for_decision": _apply_option_quote_for_decision,
    "build_exit_decision_stamp": build_exit_decision_stamp,
})
