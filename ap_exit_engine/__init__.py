"""P0 executable-bid and fresh-underlying exit-truth shim.

This package intentionally shadows the legacy top-level ``ap_exit_engine.py``
module. The legacy implementation remains the sole exit evaluator/submission
owner. This shim tightens only the quote/evaluation contract used by that
owner: production PAPER and LIVE decisions use executable option bid; option
and underlying truth must come from one coherent QPM cycle; touched-profit arms
after two fresh threshold snapshots; and non-emergency soft exits defer when
current underlying truth is unproven.

No scanner, selector, sizing, intelligence, entry, broker identity, order
ownership, proof-trade finalization, hard-stop percentage, or EOD behavior is
implemented here.
"""
from __future__ import annotations

import importlib.util as _importlib_util
import json as _json
import logging as _logging
import os as _os
import sys as _sys
import time as _time
from dataclasses import asdict as _asdict, dataclass as _dataclass, field as _field
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

log = _logging.getLogger("ap.exit_engine.soft_exit_truth")
_BaseManagedPosition = _base.ManagedPosition
_BaseAPExitEngine = _base.APExitEngine
_BaseExitDecision = _base.ExitDecision
_base_evaluate_exit = _base.evaluate_exit
_base_apply_option_quote = _base._apply_option_quote_for_decision
_base_classify_exit_decision = _base._classify_exit_decision
_base_build_exit_decision_stamp = _base.build_exit_decision_stamp
_base_submit_exit_decision = _BaseAPExitEngine._submit_exit_decision
_base_add_position = _BaseAPExitEngine.add_position
_base_managed_position_from_row = _BaseAPExitEngine._managed_position_from_row
_base_persist_peak_state_to_db = _BaseAPExitEngine._persist_peak_state_to_db

TOUCHED_PROFIT_ARM_PCT = float(_os.getenv("TOUCHED_PROFIT_ARM_PCT", "0.04"))
TOUCHED_PROFIT_CONFIRM_SNAPSHOTS = max(2, int(_os.getenv("TOUCHED_PROFIT_CONFIRM_SNAPSHOTS", "2")))
TOUCHED_PROFIT_CONFIRM_MAX_GAP_SEC = float(_os.getenv("TOUCHED_PROFIT_CONFIRM_MAX_GAP_SEC", "15"))
EXIT_DECISION_QUOTE_MAX_AGE_SEC = float(_os.getenv("EXIT_DECISION_QUOTE_MAX_AGE_SEC", "12"))
EXIT_DECISION_MAX_FUTURE_SKEW_SEC = float(_os.getenv("EXIT_DECISION_MAX_FUTURE_SKEW_SEC", "2"))
EXIT_DECISION_MAX_CYCLE_SKEW_SEC = float(_os.getenv("EXIT_DECISION_MAX_CYCLE_SKEW_SEC", "1"))
SOFT_EXIT_THESIS_GRACE_SEC = float(_os.getenv("SOFT_EXIT_THESIS_GRACE_SEC", "300"))
SOFT_EXIT_META_PERSIST_SEC = float(_os.getenv("SOFT_EXIT_META_PERSIST_SEC", "5"))
SOFT_EXIT_DEFERRED_UNDERLYING_UNAVAILABLE = "SOFT_EXIT_DEFERRED_UNDERLYING_UNAVAILABLE"
SOFT_EXIT_SUPPRESSED_CONFIRMING_THESIS = "SOFT_EXIT_SUPPRESSED_CONFIRMING_THESIS"
_SOFT_CANONICAL_CODES = frozenset({"TOUCHED_PROFIT_FLOOR", "SOFT_OPTION_STOP", "RUNNER_TRAIL", "SMALL_WIN_CAPTURE"})


def _utc_now() -> _datetime:
    return _datetime.now(_timezone.utc)


def _coerce_aware_dt(value: _Any) -> _Optional[_datetime]:
    if isinstance(value, _datetime):
        return value if value.tzinfo else value.replace(tzinfo=_timezone.utc)
    if isinstance(value, (int, float)):
        try:
            return _datetime.fromtimestamp(float(value), _timezone.utc)
        except Exception:
            return None
    if isinstance(value, str) and value.strip():
        try:
            parsed = _datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=_timezone.utc)
        except Exception:
            return None
    return None


def _positive(value: _Any) -> float:
    try:
        number = float(value or 0.0)
    except Exception:
        return 0.0
    return number if number > 0.0 else 0.0


def _mode(value: _Any) -> str:
    return str(value or "").strip().lower()


@_dataclass(frozen=True)
class ExitDecisionQuoteSnapshot:
    position_id: str
    option_symbol: str
    ticker: str
    option_bid: float
    option_ask: float
    option_midpoint: float
    option_quote_timestamp: _datetime
    underlying_bid: float
    underlying_ask: float
    underlying_midpoint: float
    underlying_quote_timestamp: _datetime
    option_quote_provider: str
    underlying_quote_provider: str
    option_quote_domain: str
    underlying_quote_domain: str
    execution_mode: str
    snapshot_timestamp: _datetime
    cycle_id: str = ""

    def to_dict(self) -> dict:
        raw = _asdict(self)
        for key, value in tuple(raw.items()):
            if isinstance(value, _datetime):
                raw[key] = value.isoformat()
        return raw


@_dataclass(frozen=True)
class ExitTruthStatus:
    valid: bool
    reason: str
    diagnostics: dict = _field(default_factory=dict)


@_dataclass
class ManagedPosition(_BaseManagedPosition):
    executable_bid_soft_exit_truth_enabled: bool = False
    exit_decision_quote_snapshot: _Optional[dict] = None
    profit_lock_armed_at: _Optional[_datetime] = None
    profit_lock_arm_bid: float = 0.0
    profit_lock_arm_pnl_pct: float = 0.0
    peak_executable_bid: float = 0.0
    peak_executable_pnl_pct: float = 0.0
    profit_lock_confirmation_count: int = 0
    profit_lock_confirmation_first_ts: _Optional[_datetime] = None
    profit_lock_last_confirmation_ts: _Optional[_datetime] = None
    profit_lock_quote_timestamps: list[str] = _field(default_factory=list)
    profit_lock_quote_sources: list[str] = _field(default_factory=list)
    soft_exit_grace_started_at: _Optional[_datetime] = None
    soft_exit_last_suppressed_at: _Optional[_datetime] = None
    soft_exit_last_deferred_at: _Optional[_datetime] = None
    soft_exit_last_diagnostic: dict = _field(default_factory=dict)
    _soft_exit_truth_last_persist_epoch: float = 0.0
    _soft_exit_truth_last_persist_signature: str = ""
    _soft_exit_truth_last_transition_signature: str = ""
    _soft_exit_refresh_callback: _Any = None


@_dataclass
class ExecutableQuoteApplication(_base.ExecutableQuoteApplication):
    """Compatibility result; executable price is bid for production positions."""


def _production_truth_enabled(pos: _Any) -> bool:
    return getattr(pos, "executable_bid_soft_exit_truth_enabled", False) is True


def _apply_option_quote_for_decision(pos: ManagedPosition, *, bid: float, ask: float, mark: float, quote_ts: _Optional[_datetime] = None, source: str = ""):
    if not _production_truth_enabled(pos):
        return _base_apply_option_quote(pos, bid=bid, ask=ask, mark=mark, quote_ts=quote_ts, source=source)
    bid = _positive(bid)
    ask = _positive(ask)
    mark = _positive(mark)
    midpoint = round((bid + ask) / 2.0, 4) if bid > 0 and ask > 0 else (mark or ask)
    analytics = mark or midpoint
    raw_mode = str(getattr(pos, "execution_mode", "") or "").strip()
    norm_mode = _mode(raw_mode)
    pricing_mode = norm_mode if norm_mode in {"paper", "live"} else "live_risk_unproven"
    valid = bid > 0
    executable = bid if valid else 0.0
    src = source or ("executable_bid" if pricing_mode in {"paper", "live"} else "executable_bid_live_risk_unproven")
    if not valid:
        src = "bid_missing" if pricing_mode in {"paper", "live"} else "bid_missing_live_risk_unproven"
    for name, value in (
        ("analytics_mark_price", analytics), ("analyticsmarkprice", analytics),
        ("current_bid", bid), ("currentbid", bid), ("current_ask", ask), ("currentask", ask),
        ("current_option_price", executable), ("currentoptionprice", executable),
        ("executable_exit_price", executable), ("executable_quote_valid", valid),
        ("live_executable_price_source", src), ("liveexecutablepricesource", src),
        ("pricing_mode", pricing_mode), ("raw_execution_mode", raw_mode),
    ):
        try:
            setattr(pos, name, value)
        except Exception:
            pass
    if quote_ts is not None:
        aware = _coerce_aware_dt(quote_ts)
        for name in ("last_option_quote_update_ts", "lastoptionquoteupdatets"):
            try:
                setattr(pos, name, aware)
            except Exception:
                pass
    return ExecutableQuoteApplication(pricing_mode=pricing_mode, executable_price=executable, executable_valid=valid, analytics_mark=analytics, source=src)


def _snapshot_dict(pos: _Any) -> dict:
    raw = getattr(pos, "exit_decision_quote_snapshot", None)
    return dict(raw) if isinstance(raw, dict) else {}


def _validate_exit_truth_snapshot(pos: ManagedPosition, *, now_utc: _Optional[_datetime] = None) -> ExitTruthStatus:
    now_utc = now_utc or _utc_now()
    snap = _snapshot_dict(pos)
    if not snap:
        return ExitTruthStatus(False, "missing_snapshot")
    expected = {
        "position_id": str(getattr(pos, "position_id", "") or ""),
        "option_symbol": str(getattr(pos, "option_symbol", "") or "").upper(),
        "ticker": str(getattr(pos, "ticker", "") or "").upper(),
        "execution_mode": _mode(getattr(pos, "execution_mode", "")),
    }
    actual = {
        "position_id": str(snap.get("position_id") or ""),
        "option_symbol": str(snap.get("option_symbol") or "").upper(),
        "ticker": str(snap.get("ticker") or "").upper(),
        "execution_mode": _mode(snap.get("execution_mode")),
    }
    for key, wanted in expected.items():
        if wanted and actual.get(key) != wanted:
            return ExitTruthStatus(False, f"snapshot_identity_mismatch:{key}", {"expected": expected, "actual": actual})
    option_ts = _coerce_aware_dt(snap.get("option_quote_timestamp"))
    underlying_ts = _coerce_aware_dt(snap.get("underlying_quote_timestamp"))
    snapshot_ts = _coerce_aware_dt(snap.get("snapshot_timestamp"))
    if option_ts is None:
        return ExitTruthStatus(False, "missing_or_invalid_option_timestamp")
    if underlying_ts is None:
        return ExitTruthStatus(False, "missing_or_invalid_underlying_timestamp")
    if snapshot_ts is None:
        return ExitTruthStatus(False, "missing_or_invalid_snapshot_timestamp")
    option_age = (now_utc - option_ts).total_seconds()
    underlying_age = (now_utc - underlying_ts).total_seconds()
    snapshot_age = (now_utc - snapshot_ts).total_seconds()
    diag = {"option_age_sec": option_age, "underlying_age_sec": underlying_age, "snapshot_age_sec": snapshot_age}
    if option_age < -EXIT_DECISION_MAX_FUTURE_SKEW_SEC:
        return ExitTruthStatus(False, "future_dated_option_quote", diag)
    if underlying_age < -EXIT_DECISION_MAX_FUTURE_SKEW_SEC:
        return ExitTruthStatus(False, "future_dated_underlying_quote", diag)
    if snapshot_age < -EXIT_DECISION_MAX_FUTURE_SKEW_SEC:
        return ExitTruthStatus(False, "future_dated_snapshot", diag)
    if option_age > EXIT_DECISION_QUOTE_MAX_AGE_SEC:
        return ExitTruthStatus(False, "stale_option_quote", diag)
    if underlying_age > EXIT_DECISION_QUOTE_MAX_AGE_SEC:
        return ExitTruthStatus(False, "stale_underlying_quote", diag)
    if snapshot_age > EXIT_DECISION_QUOTE_MAX_AGE_SEC:
        return ExitTruthStatus(False, "stale_snapshot", diag)
    cycle_skew = abs((option_ts - underlying_ts).total_seconds())
    option_snapshot_skew = abs((option_ts - snapshot_ts).total_seconds())
    underlying_snapshot_skew = abs((underlying_ts - snapshot_ts).total_seconds())
    diag.update({"cycle_skew_sec": cycle_skew, "option_snapshot_skew_sec": option_snapshot_skew, "underlying_snapshot_skew_sec": underlying_snapshot_skew})
    if max(cycle_skew, option_snapshot_skew, underlying_snapshot_skew) > EXIT_DECISION_MAX_CYCLE_SKEW_SEC:
        return ExitTruthStatus(False, "cross_cycle_quote_pair", diag)
    option_provider = str(snap.get("option_quote_provider") or "").strip().lower()
    underlying_provider = str(snap.get("underlying_quote_provider") or "").strip().lower()
    option_domain = str(snap.get("option_quote_domain") or "").strip().lower()
    underlying_domain = str(snap.get("underlying_quote_domain") or "").strip().lower()
    if not option_provider or not underlying_provider:
        return ExitTruthStatus(False, "missing_quote_provider", diag)
    if option_provider != underlying_provider:
        return ExitTruthStatus(False, "cross_provider_quote_pair", diag)
    if not option_domain or not underlying_domain:
        return ExitTruthStatus(False, "missing_quote_domain", diag)
    if option_domain != underlying_domain:
        return ExitTruthStatus(False, "cross_domain_quote_pair", diag)
    if _positive(snap.get("option_bid")) <= 0:
        return ExitTruthStatus(False, "missing_executable_option_bid", diag)
    if _positive(snap.get("underlying_midpoint")) <= 0:
        return ExitTruthStatus(False, "missing_underlying_quote", diag)
    return ExitTruthStatus(True, "fresh_coherent_snapshot", diag)


def _canonical_reason_code(decision: _Any) -> str:
    explicit = str(getattr(decision, "reason_code", "") or "").upper().strip()
    reason = str(getattr(decision, "reason", "") or "").upper()
    if explicit in {
        SOFT_EXIT_DEFERRED_UNDERLYING_UNAVAILABLE, SOFT_EXIT_SUPPRESSED_CONFIRMING_THESIS,
        "TOUCHED_PROFIT_FLOOR", "SOFT_OPTION_STOP", "UNDERLYING_STOP_CONFIRMED",
        "RUNNER_TRAIL", "SMALL_WIN_CAPTURE", "HARD_OPTION_STOP", "EOD_FORCE_CLOSE",
        "DAILY_KILL_SWITCH", "KILL_SWITCH",
    }:
        return explicit
    if "EOD FORCE CLOSE" in reason:
        return "EOD_FORCE_CLOSE"
    if "UNDERLYING" in reason and "STOP" in reason and ("CONFIRM" in reason or "STOP HIT" in reason):
        return "UNDERLYING_STOP_CONFIRMED"
    if "HARD STOP" in reason or explicit in {"HARD_STOP", "MAX_LOSS", "EMERGENCY_STOP"}:
        return "HARD_OPTION_STOP"
    if "TOUCHED PROFIT" in reason or explicit in {"TOUCHED_PROFIT_STOP", "PROFIT_LOCK"}:
        return "TOUCHED_PROFIT_FLOOR"
    if "SMALL WIN" in reason or explicit == "SMALL_WIN_LOCK":
        return "SMALL_WIN_CAPTURE"
    if "RUNNER TRAIL" in reason or "TRAILING STOP" in reason or explicit in {"RUNNER_TRAIL", "TRAILING_STOP"}:
        return "RUNNER_TRAIL"
    if any(fragment in reason for fragment in ("THESIS_FAIL_SOFT_STOP", "THESIS_STALE_SOFT_STOP", "DEEP_LOSS_STOP", "NEVER GREEN STOP", "SOFT STOP")) or explicit in {"SOFT_LOSS", "SOFT_LOSS_WATCH", "DEEP_LOSS_STOP", "THESIS_FAIL_SOFT_STOP", "THESIS_STALE_SOFT_STOP", "NEVER_GREEN_STOP"}:
        return "SOFT_OPTION_STOP"
    if explicit:
        return explicit
    return str(_base_classify_exit_decision(decision) or "UNKNOWN_EXIT").upper()


def _classify_exit_decision(decision: _Any) -> str:
    return _canonical_reason_code(decision)


def _underlying_confirming(pos: ManagedPosition) -> tuple[bool, str]:
    return _base._underlying_still_confirming(pos)


def _grace_status(pos: ManagedPosition, now_utc: _datetime) -> tuple[bool, float]:
    started = _coerce_aware_dt(getattr(pos, "soft_exit_grace_started_at", None))
    if started is None:
        pos.soft_exit_grace_started_at = now_utc
        started = now_utc
    elapsed = max(0.0, (now_utc - started).total_seconds())
    return elapsed < SOFT_EXIT_THESIS_GRACE_SEC, elapsed


def _request_quote_refresh(pos: ManagedPosition) -> bool:
    callback = getattr(pos, "_soft_exit_refresh_callback", None)
    if callable(callback):
        try:
            return bool(callback())
        except Exception as exc:
            log.debug("soft exit quote refresh callback failed: %s", exc)
    return False


def _deferred_decision(pos: ManagedPosition, status: ExitTruthStatus) -> _BaseExitDecision:
    now = _utc_now()
    pos.soft_exit_last_deferred_at = now
    refreshed = _request_quote_refresh(pos)
    pos.soft_exit_last_diagnostic = {
        "truth_status": status.reason,
        "truth_diagnostics": status.diagnostics,
        "refresh_requested": refreshed,
        "snapshot": _snapshot_dict(pos),
    }
    return _BaseExitDecision(
        action="HOLD", quantity=0,
        reason=f"SOFT EXIT DEFERRED — current same-cycle underlying truth is unavailable or invalid ({status.reason}); refresh requested={refreshed}",
        urgency="NORMAL", pnl_pct=float(getattr(pos, "option_pnl_pct", 0.0) or 0.0),
        reason_code=SOFT_EXIT_DEFERRED_UNDERLYING_UNAVAILABLE,
    )


def _suppressed_decision(pos: ManagedPosition, *, rule_code: str, confirming_reason: str, elapsed_sec: float) -> _BaseExitDecision:
    now = _utc_now()
    pos.soft_exit_last_suppressed_at = now
    pos.soft_exit_last_diagnostic = {
        "suppressed_rule": rule_code,
        "confirming_reason": confirming_reason,
        "grace_elapsed_sec": elapsed_sec,
        "grace_limit_sec": SOFT_EXIT_THESIS_GRACE_SEC,
        "snapshot": _snapshot_dict(pos),
    }
    return _BaseExitDecision(
        action="HOLD", quantity=0,
        reason=f"{rule_code} suppressed — fresh underlying still confirms thesis ({confirming_reason}); grace={elapsed_sec:.1f}/{SOFT_EXIT_THESIS_GRACE_SEC:.1f}s",
        urgency="NORMAL", pnl_pct=float(getattr(pos, "option_pnl_pct", 0.0) or 0.0),
        reason_code=SOFT_EXIT_SUPPRESSED_CONFIRMING_THESIS,
    )


def _touched_profit_floor(pos: ManagedPosition) -> float:
    peak = float(getattr(pos, "peak_executable_pnl_pct", 0.0) or 0.0)
    if peak >= 0.25:
        return 0.12
    if peak >= 0.15:
        return 0.08
    if peak >= 0.10:
        return 0.05
    if peak >= 0.05:
        return 0.03
    return 0.0


def _pre_soft_candidate(pos: ManagedPosition) -> str:
    pnl = float(getattr(pos, "option_pnl_pct", 0.0) or 0.0)
    if getattr(pos, "touched_profit", False) and int(getattr(pos, "scale_outs_done", 0) or 0) == 0 and pnl <= _touched_profit_floor(pos):
        return "TOUCHED_PROFIT_FLOOR"
    peak = float(getattr(pos, "peak_executable_pnl_pct", 0.0) or 0.0)
    if peak >= float(getattr(_base, "IMMEDIATE_TP_PCT", 0.12)) and (peak - pnl >= float(getattr(_base, "TRAIL_DROP_FROM_PEAK", 0.10)) or pnl <= 0):
        return "RUNNER_TRAIL"
    max_profit = float(getattr(pos, "max_profit_seen", 0.0) or 0.0)
    if max_profit >= float(getattr(_base, "SMALL_WIN_PCT", 0.12)):
        floor = max(0.03, max_profit - float(getattr(_base, "SMALL_WIN_TRAIL", 0.08)))
        if 0 < pnl <= floor:
            return "SMALL_WIN_CAPTURE"
    if pnl <= float(_os.getenv("SOFT_LOSS_STOP_PCT", "-0.12")):
        return "SOFT_OPTION_STOP"
    age = float(_base._position_age_minutes(pos))
    if not getattr(pos, "touched_profit", False) and age >= 20 and pnl <= -0.08:
        return "SOFT_OPTION_STOP"
    return ""


def _forced_eod_decision(pos: ManagedPosition, now_et: _datetime) -> _Optional[_BaseExitDecision]:
    hour, minute = now_et.hour, now_et.minute
    past_eod = hour > _base.EOD_HARD_CLOSE_HOUR or (hour == _base.EOD_HARD_CLOSE_HOUR and minute >= _base.EOD_HARD_CLOSE_MIN)
    if (past_eod or hour >= 16) and not getattr(pos, "overnight_hold_approved", False):
        return _BaseExitDecision(
            action="CLOSE_ALL", quantity=int(getattr(pos, "quantity_remaining", 0) or 0),
            reason=f"EOD FORCE CLOSE -- {hour}:{minute:02d} ET", urgency="IMMEDIATE",
            pnl_pct=float(getattr(pos, "option_pnl_pct", 0.0) or 0.0), reason_code="EOD_FORCE_CLOSE",
        )
    return None


def _confirmed_underlying_stop(pos: ManagedPosition, now_utc: _datetime) -> _Optional[_BaseExitDecision]:
    if not getattr(pos, "is_at_stop", False):
        return None
    confirm_sec = float(_os.getenv("UNDERLYING_STOP_CONFIRM_SECONDS", "30"))
    started = _coerce_aware_dt(getattr(pos, "_underlying_stop_breach_ts", None))
    if started is None:
        pos._underlying_stop_breach_ts = now_utc
        return None
    elapsed = max(0.0, (now_utc - started).total_seconds())
    if elapsed < confirm_sec:
        return None
    pos._underlying_stop_breach_ts = None
    return _BaseExitDecision(
        action="STOP", quantity=int(getattr(pos, "quantity_remaining", 0) or 0),
        reason=f"UNDERLYING STOP CONFIRMED — ${float(getattr(pos, 'current_underlying', 0.0) or 0.0):.2f} breached ${float(getattr(pos, 'underlying_stop', 0.0) or 0.0):.2f} for {elapsed:.0f}s",
        urgency="HIGH", pnl_pct=float(getattr(pos, "option_pnl_pct", 0.0) or 0.0),
        reason_code="UNDERLYING_STOP_CONFIRMED",
    )


def evaluate_exit(pos: ManagedPosition, now_et: _Optional[_datetime] = None):
    if not _production_truth_enabled(pos):
        return _base_evaluate_exit(pos, now_et)
    if now_et is None:
        now_et = _datetime.now(_base.ET)
    now_utc = now_et.astimezone(_timezone.utc) if now_et.tzinfo else now_et.replace(tzinfo=_base.ET).astimezone(_timezone.utc)
    eod = _forced_eod_decision(pos, now_et)
    if eod is not None:
        return eod
    underlying_stop = _confirmed_underlying_stop(pos, now_utc)
    if underlying_stop is not None:
        return underlying_stop
    hard_stop, _, _ = _base._effective_thresholds(pos)
    pnl = float(getattr(pos, "option_pnl_pct", 0.0) or 0.0)
    if pnl <= hard_stop:
        return _BaseExitDecision(
            action="STOP", quantity=int(getattr(pos, "quantity_remaining", 0) or 0),
            reason=f"HARD STOP -- executable bid P&L {pnl*100:.1f}% exceeded {hard_stop*100:.1f}% max loss",
            urgency="IMMEDIATE", pnl_pct=pnl, reason_code="HARD_OPTION_STOP",
        )
    truth = _validate_exit_truth_snapshot(pos, now_utc=now_utc)
    candidate = _pre_soft_candidate(pos)
    if candidate and not truth.valid:
        return _deferred_decision(pos, truth)
    if candidate and truth.valid:
        confirming, confirming_reason = _underlying_confirming(pos)
        if confirming and not getattr(pos, "is_at_stop", False):
            in_grace, elapsed = _grace_status(pos, now_utc)
            if in_grace:
                return _suppressed_decision(pos, rule_code=candidate, confirming_reason=confirming_reason, elapsed_sec=elapsed)
    decision = _base_evaluate_exit(pos, now_et)
    code = _canonical_reason_code(decision)
    decision.reason_code = code
    if not getattr(decision, "should_act", False) or code not in _SOFT_CANONICAL_CODES:
        return decision
    if not truth.valid:
        return _deferred_decision(pos, truth)
    confirming, confirming_reason = _underlying_confirming(pos)
    if confirming and not getattr(pos, "is_at_stop", False):
        in_grace, elapsed = _grace_status(pos, now_utc)
        if in_grace:
            return _suppressed_decision(pos, rule_code=code, confirming_reason=confirming_reason, elapsed_sec=elapsed)
    return decision


def build_exit_decision_stamp(pos: ManagedPosition, decision: _Any, **kwargs) -> dict:
    payload = _base_build_exit_decision_stamp(pos, decision, **kwargs)
    snapshot = _snapshot_dict(pos)
    armed_at = getattr(pos, "profit_lock_armed_at", None)
    payload.update({
        "reason_code": _canonical_reason_code(decision),
        "decision_quote_snapshot": snapshot,
        "executable_option_bid": _positive(snapshot.get("option_bid")) or _positive(getattr(pos, "current_bid", 0.0)),
        "analytics_option_midpoint": _positive(snapshot.get("option_midpoint")) or _positive(getattr(pos, "analytics_mark_price", 0.0)),
        "underlying_bid": _positive(snapshot.get("underlying_bid")),
        "underlying_ask": _positive(snapshot.get("underlying_ask")),
        "underlying_midpoint": _positive(snapshot.get("underlying_midpoint")),
        "option_quote_timestamp": snapshot.get("option_quote_timestamp"),
        "underlying_quote_timestamp": snapshot.get("underlying_quote_timestamp"),
        "option_quote_provider": snapshot.get("option_quote_provider"),
        "underlying_quote_provider": snapshot.get("underlying_quote_provider"),
        "option_quote_domain": snapshot.get("option_quote_domain"),
        "underlying_quote_domain": snapshot.get("underlying_quote_domain"),
        "profit_lock_armed_at": armed_at.isoformat() if isinstance(armed_at, _datetime) else armed_at,
        "profit_lock_arm_bid": float(getattr(pos, "profit_lock_arm_bid", 0.0) or 0.0),
        "profit_lock_arm_pnl_pct": float(getattr(pos, "profit_lock_arm_pnl_pct", 0.0) or 0.0),
        "peak_executable_bid": float(getattr(pos, "peak_executable_bid", 0.0) or 0.0),
        "peak_executable_pnl_pct": float(getattr(pos, "peak_executable_pnl_pct", 0.0) or 0.0),
        "profit_lock_confirmation_count": int(getattr(pos, "profit_lock_confirmation_count", 0) or 0),
    })
    return payload


def _hydrate_soft_exit_meta(pos: ManagedPosition, row: dict) -> None:
    meta = row.get("meta") if isinstance(row, dict) else None
    if isinstance(meta, str):
        try:
            meta = _json.loads(meta)
        except Exception:
            meta = {}
    meta = dict(meta) if isinstance(meta, dict) else {}
    truth = meta.get("soft_exit_truth")
    truth = dict(truth) if isinstance(truth, dict) else {}
    for key in ("profit_lock_arm_bid", "profit_lock_arm_pnl_pct", "peak_executable_bid", "peak_executable_pnl_pct"):
        try:
            setattr(pos, key, float(truth.get(key) or 0.0))
        except Exception:
            pass
    try:
        pos.profit_lock_confirmation_count = int(truth.get("profit_lock_confirmation_count") or 0)
    except Exception:
        pass
    for key in ("profit_lock_armed_at", "profit_lock_confirmation_first_ts", "profit_lock_last_confirmation_ts", "soft_exit_grace_started_at"):
        setattr(pos, key, _coerce_aware_dt(truth.get(key)))
    if isinstance(truth.get("profit_lock_quote_timestamps"), list):
        pos.profit_lock_quote_timestamps = list(truth["profit_lock_quote_timestamps"])[-8:]
    if isinstance(truth.get("profit_lock_quote_sources"), list):
        pos.profit_lock_quote_sources = list(truth["profit_lock_quote_sources"])[-8:]
    if isinstance(truth.get("decision_quote_snapshot"), dict):
        pos.exit_decision_quote_snapshot = dict(truth["decision_quote_snapshot"])
    if pos.profit_lock_armed_at is not None:
        pos.touched_profit = True


class APExitEngine(_BaseAPExitEngine):
    supports_executable_bid_soft_exit_truth = True

    def add_position(self, pos: ManagedPosition):
        pos.executable_bid_soft_exit_truth_enabled = True
        pos._soft_exit_refresh_callback = lambda p=pos: self._request_immediate_quote_retry(p)
        return _base_add_position(self, pos)

    def _managed_position_from_row(self, row: dict, qty_override: int = 0, *, prefer_qty_override: bool = False):
        pos = _base_managed_position_from_row(self, row, qty_override, prefer_qty_override=prefer_qty_override)
        pos.executable_bid_soft_exit_truth_enabled = True
        pos._soft_exit_refresh_callback = lambda p=pos: self._request_immediate_quote_retry(p)
        _hydrate_soft_exit_meta(pos, row or {})
        return pos

    def _qpm_enriched_snapshot(self, raw: dict, context: dict) -> ExitDecisionQuoteSnapshot:
        position_id = str(raw.get("position_id") or raw.get("positionid") or "")
        option_symbol = str(raw.get("option_symbol") or raw.get("optionsymbol") or "").upper()
        ticker = str(raw.get("ticker") or "").upper()
        option_raw = dict((context.get("option_quotes") or {}).get(option_symbol) or {})
        underlying_raw = dict((context.get("underlying_quotes") or {}).get(ticker) or {})
        cycle_ts = _coerce_aware_dt(context.get("snapshot_timestamp")) or _utc_now()
        option_bid = _positive(option_raw.get("bid")) or _positive(raw.get("current_bid"))
        option_ask = _positive(option_raw.get("ask")) or _positive(raw.get("current_ask"))
        option_mid = round((option_bid + option_ask) / 2.0, 4) if option_bid > 0 and option_ask > 0 else (_positive(option_raw.get("mark")) or _positive(option_raw.get("last")))
        underlying_bid = _positive(underlying_raw.get("bid"))
        underlying_ask = _positive(underlying_raw.get("ask"))
        underlying_last = _positive(underlying_raw.get("last")) or _positive(underlying_raw.get("close")) or _positive(raw.get("current_underlying"))
        underlying_mid = round((underlying_bid + underlying_ask) / 2.0, 4) if underlying_bid > 0 and underlying_ask > 0 else underlying_last
        original_modes = context.get("original_modes") or {}
        original_modes_by_symbol = context.get("original_modes_by_symbol") or {}
        execution_mode = _mode(original_modes.get(position_id) or original_modes_by_symbol.get(option_symbol) or context.get("default_execution_mode"))
        provider = str(context.get("quote_provider") or "unknown").strip().lower()
        domain = str(context.get("quote_domain") or "unknown").strip().lower()
        cycle_id = str(context.get("cycle_id") or cycle_ts.isoformat())
        return ExitDecisionQuoteSnapshot(
            position_id=position_id, option_symbol=option_symbol, ticker=ticker,
            option_bid=option_bid, option_ask=option_ask, option_midpoint=option_mid,
            option_quote_timestamp=cycle_ts, underlying_bid=underlying_bid,
            underlying_ask=underlying_ask, underlying_midpoint=underlying_mid,
            underlying_quote_timestamp=cycle_ts, option_quote_provider=provider,
            underlying_quote_provider=provider, option_quote_domain=domain,
            underlying_quote_domain=domain, execution_mode=execution_mode,
            snapshot_timestamp=cycle_ts, cycle_id=cycle_id,
        )

    def apply_exit_decision_snapshots(self, snapshots: list[dict]) -> None:
        context = dict(getattr(self, "_qpm_cycle_context", {}) or {})
        active = self.active_positions()
        by_id = {str(getattr(p, "position_id", "") or ""): p for p in active}
        by_symbol = {str(getattr(p, "option_symbol", "") or "").upper(): p for p in active}
        for raw in list(snapshots or []):
            snap = self._qpm_enriched_snapshot(dict(raw or {}), context)
            pos = by_id.get(snap.position_id) or by_symbol.get(snap.option_symbol)
            if pos is None:
                continue
            pos.executable_bid_soft_exit_truth_enabled = True
            if snap.execution_mode in {"paper", "live"}:
                pos.execution_mode = snap.execution_mode
            _apply_option_quote_for_decision(
                pos, bid=snap.option_bid, ask=snap.option_ask, mark=snap.option_midpoint,
                quote_ts=snap.option_quote_timestamp,
                source=f"{snap.option_quote_provider}:{snap.option_quote_domain}:executable_bid",
            )
            if snap.underlying_midpoint > 0:
                pos.current_underlying = snap.underlying_midpoint
                pos.last_underlying_quote_update_ts = snap.underlying_quote_timestamp
                pos.last_underlying_quote_missing_ts = None
            else:
                pos.current_underlying = 0.0
                pos.last_underlying_quote_missing_ts = snap.snapshot_timestamp
            pos.exit_decision_quote_snapshot = snap.to_dict()
            if snap.option_bid > 0 and float(getattr(pos, "entry_price", 0.0) or 0.0) > 0:
                pnl = (snap.option_bid - float(pos.entry_price)) / float(pos.entry_price)
                pos.option_pnl_pct = pnl
                if pnl > float(getattr(pos, "peak_executable_pnl_pct", 0.0) or 0.0):
                    pos.peak_executable_pnl_pct = pnl
                    pos.peak_executable_bid = snap.option_bid
                pos.peak_pnl_pct = max(0.0, float(pos.peak_executable_pnl_pct or 0.0))
                pos.max_profit_seen = max(0.0, float(pos.peak_executable_pnl_pct or 0.0))
                if getattr(pos, "profit_lock_armed_at", None) is None:
                    prior_ts = _coerce_aware_dt(getattr(pos, "profit_lock_last_confirmation_ts", None))
                    if pnl >= TOUCHED_PROFIT_ARM_PCT:
                        consecutive = bool(prior_ts is not None and snap.option_quote_timestamp > prior_ts and (snap.option_quote_timestamp - prior_ts).total_seconds() <= TOUCHED_PROFIT_CONFIRM_MAX_GAP_SEC)
                        if not consecutive:
                            pos.profit_lock_confirmation_count = 1
                            pos.profit_lock_confirmation_first_ts = snap.option_quote_timestamp
                            pos.profit_lock_quote_timestamps = []
                            pos.profit_lock_quote_sources = []
                        else:
                            pos.profit_lock_confirmation_count = int(getattr(pos, "profit_lock_confirmation_count", 0) or 0) + 1
                        pos.profit_lock_last_confirmation_ts = snap.option_quote_timestamp
                        pos.profit_lock_quote_timestamps = (list(getattr(pos, "profit_lock_quote_timestamps", []) or []) + [snap.option_quote_timestamp.isoformat()])[-8:]
                        pos.profit_lock_quote_sources = (list(getattr(pos, "profit_lock_quote_sources", []) or []) + [f"{snap.option_quote_provider}:{snap.option_quote_domain}:bid"])[-8:]
                        if pos.profit_lock_confirmation_count >= TOUCHED_PROFIT_CONFIRM_SNAPSHOTS:
                            pos.profit_lock_armed_at = snap.option_quote_timestamp
                            pos.profit_lock_arm_bid = snap.option_bid
                            pos.profit_lock_arm_pnl_pct = pnl
                            pos.touched_profit = True
                    else:
                        pos.profit_lock_confirmation_count = 0
                        pos.profit_lock_confirmation_first_ts = None
                        pos.profit_lock_last_confirmation_ts = None
                        pos.profit_lock_quote_timestamps = []
                        pos.profit_lock_quote_sources = []
                        pos.touched_profit = False
                else:
                    pos.touched_profit = True
            self._persist_soft_exit_truth_to_db(pos)

    def apply_quote_snapshots(self, snapshots: list[dict]) -> None:
        self.apply_exit_decision_snapshots(snapshots)

    def applyquotesnapshots(self, snapshots: list[dict]) -> None:
        self.apply_exit_decision_snapshots(snapshots)

    def _soft_exit_meta_payload(self, pos: ManagedPosition) -> dict:
        def iso(value):
            return value.isoformat() if isinstance(value, _datetime) else value
        return {
            "profit_lock_armed_at": iso(getattr(pos, "profit_lock_armed_at", None)),
            "profit_lock_arm_bid": float(getattr(pos, "profit_lock_arm_bid", 0.0) or 0.0),
            "profit_lock_arm_pnl_pct": float(getattr(pos, "profit_lock_arm_pnl_pct", 0.0) or 0.0),
            "peak_executable_bid": float(getattr(pos, "peak_executable_bid", 0.0) or 0.0),
            "peak_executable_pnl_pct": float(getattr(pos, "peak_executable_pnl_pct", 0.0) or 0.0),
            "profit_lock_confirmation_count": int(getattr(pos, "profit_lock_confirmation_count", 0) or 0),
            "profit_lock_confirmation_first_ts": iso(getattr(pos, "profit_lock_confirmation_first_ts", None)),
            "profit_lock_last_confirmation_ts": iso(getattr(pos, "profit_lock_last_confirmation_ts", None)),
            "profit_lock_quote_timestamps": list(getattr(pos, "profit_lock_quote_timestamps", []) or [])[-8:],
            "profit_lock_quote_sources": list(getattr(pos, "profit_lock_quote_sources", []) or [])[-8:],
            "soft_exit_grace_started_at": iso(getattr(pos, "soft_exit_grace_started_at", None)),
            "soft_exit_last_suppressed_at": iso(getattr(pos, "soft_exit_last_suppressed_at", None)),
            "soft_exit_last_deferred_at": iso(getattr(pos, "soft_exit_last_deferred_at", None)),
            "soft_exit_last_diagnostic": dict(getattr(pos, "soft_exit_last_diagnostic", {}) or {}),
            "decision_quote_snapshot": _snapshot_dict(pos),
            "decision_price_authority": "executable_bid",
            "analytics_price": "midpoint_only",
        }

    def _persist_soft_exit_truth_to_db(self, pos: ManagedPosition, *, force: bool = False) -> bool:
        identity = self._protective_position_identity(pos)
        if identity is None:
            return False
        payload = self._soft_exit_meta_payload(pos)
        signature = _json.dumps(payload, sort_keys=True, default=str)
        transition_signature = _json.dumps({
            "profit_lock_armed_at": payload.get("profit_lock_armed_at"),
            "profit_lock_confirmation_count": payload.get("profit_lock_confirmation_count"),
            "last_deferred_at": payload.get("soft_exit_last_deferred_at"),
            "last_suppressed_at": payload.get("soft_exit_last_suppressed_at"),
            "last_diagnostic_reason": (payload.get("soft_exit_last_diagnostic") or {}).get("truth_status") or (payload.get("soft_exit_last_diagnostic") or {}).get("suppressed_rule"),
        }, sort_keys=True, default=str)
        now_epoch = _time.time()
        last_epoch = float(getattr(pos, "_soft_exit_truth_last_persist_epoch", 0.0) or 0.0)
        last_transition = str(getattr(pos, "_soft_exit_truth_last_transition_signature", "") or "")
        transition = transition_signature != last_transition
        if not force and not transition and (now_epoch - last_epoch) < SOFT_EXIT_META_PERSIST_SEC:
            return False
        try:
            from ap.db import conn, run_with_retry
            patch = _json.dumps({"soft_exit_truth": payload}, default=str)
            def update():
                with conn() as cursor:
                    cursor.execute(
                        """
                        UPDATE positions
                        SET meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb,
                            peak_pnl_pct = %s,
                            max_profit_seen = %s,
                            touched_profit = %s,
                            current_option_price = COALESCE(NULLIF(%s, 0), current_option_price),
                            option_pnl_pct = %s,
                            updated_at = NOW()
                        WHERE id = %s
                          AND client_id = %s
                          AND LOWER(COALESCE(execution_mode, '')) = %s
                          AND COALESCE(contract, option_symbol, '') = %s
                          AND status IN ('OPEN', 'CLOSING', 'PARTIAL', 'ACTIVE')
                        """,
                        (
                            patch,
                            float(getattr(pos, "peak_executable_pnl_pct", 0.0) or 0.0),
                            float(getattr(pos, "peak_executable_pnl_pct", 0.0) or 0.0),
                            bool(getattr(pos, "touched_profit", False)),
                            float(getattr(pos, "current_bid", 0.0) or 0.0),
                            float(getattr(pos, "option_pnl_pct", 0.0) or 0.0),
                            identity.position_id, identity.client_id, identity.execution_mode, identity.contract,
                        ),
                    )
                    return int(cursor.rowcount or 0)
            rowcount = int(run_with_retry(update) or 0)
            if rowcount == 1:
                pos._soft_exit_truth_last_persist_epoch = now_epoch
                pos._soft_exit_truth_last_persist_signature = signature
                pos._soft_exit_truth_last_transition_signature = transition_signature
                return True
            log.warning("SOFT_EXIT_TRUTH_PERSIST_ROWCOUNT client=%s pos=%s mode=%s contract=%s rowcount=%s", identity.client_id, identity.position_id, identity.execution_mode, identity.contract, rowcount)
        except Exception as exc:
            log.debug("soft exit truth persistence failed non-fatally: %s", exc)
        return False

    def _persist_peak_state_to_db(self, pos) -> bool:
        legacy = _base_persist_peak_state_to_db(self, pos)
        canonical = self._persist_soft_exit_truth_to_db(pos)
        return bool(legacy or canonical)

    def _submit_exit_decision(self, pos: ManagedPosition, decision: _Any, *, kill_active: bool = False, allow_inflight_override: bool = False):
        if _production_truth_enabled(pos):
            decision.reason_code = _canonical_reason_code(decision)
            if decision.reason_code in _SOFT_CANONICAL_CODES:
                truth = _validate_exit_truth_snapshot(pos)
                if not truth.valid:
                    deferred = _deferred_decision(pos, truth)
                    self._emit_exit_event(
                        pos, decision="HOLD", reason_code=deferred.reason_code,
                        explanation=deferred.reason, stage="exit_submission",
                        extra_inputs={"decision_quote_snapshot": _snapshot_dict(pos)},
                    )
                    self._persist_soft_exit_truth_to_db(pos, force=True)
                    return False
                decision.suggested_limit = float(getattr(pos, "current_bid", 0.0) or 0.0)
        return _base_submit_exit_decision(self, pos, decision, kill_active=kill_active, allow_inflight_override=allow_inflight_override)


_base.ManagedPosition = ManagedPosition
_base.APExitEngine = APExitEngine
_base.evaluate_exit = evaluate_exit
_base._apply_option_quote_for_decision = _apply_option_quote_for_decision
_base.ExecutableQuoteApplication = ExecutableQuoteApplication
_base._classify_exit_decision = _classify_exit_decision
_base.build_exit_decision_stamp = build_exit_decision_stamp
_base.FORCED_RISK_EXIT_CODES = {
    "EOD_FORCE_CLOSE", "STOP_HIT", "UNDERLYING_STOP_CONFIRMED", "HARD_STOP",
    "HARD_OPTION_STOP", "SENTINEL_FORCED_EXIT", "EMERGENCY_STOP", "MAX_LOSS",
    "DAILY_KILL_SWITCH", "KILL_SWITCH",
}
globals().update({
    "ManagedPosition": ManagedPosition,
    "APExitEngine": APExitEngine,
    "ExecutableQuoteApplication": ExecutableQuoteApplication,
    "ExitDecisionQuoteSnapshot": ExitDecisionQuoteSnapshot,
    "ExitTruthStatus": ExitTruthStatus,
    "evaluate_exit": evaluate_exit,
    "_apply_option_quote_for_decision": _apply_option_quote_for_decision,
    "_validate_exit_truth_snapshot": _validate_exit_truth_snapshot,
    "_classify_exit_decision": _classify_exit_decision,
    "build_exit_decision_stamp": build_exit_decision_stamp,
})
