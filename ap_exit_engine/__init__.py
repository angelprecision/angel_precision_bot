"""P0 executable-bid and fresh-underlying truth for non-emergency exits.

The legacy top-level ``ap_exit_engine.py`` remains the only evaluator and broker
submission owner. This package shadows that module and tightens its quote input.
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
_BaseExecutableQuoteApplication = _base.ExecutableQuoteApplication
_base_evaluate_exit = _base.evaluate_exit
_base_apply_option_quote = _base._apply_option_quote_for_decision
_base_classify_exit_decision = _base._classify_exit_decision
_base_build_exit_decision_stamp = _base.build_exit_decision_stamp
_base_submit_exit_decision = _BaseAPExitEngine._submit_exit_decision
_base_add_position = _BaseAPExitEngine.add_position
_base_seed_from_db = _BaseAPExitEngine.seed_from_db
_base_managed_position_from_row = _BaseAPExitEngine._managed_position_from_row
_base_persist_peak_state_to_db = _BaseAPExitEngine._persist_peak_state_to_db
_base_adopt_canonical_position_identity = _BaseAPExitEngine.adopt_canonical_position_identity

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
_SOFT_CODES = frozenset({"TOUCHED_PROFIT_FLOOR", "SOFT_OPTION_STOP", "RUNNER_TRAIL", "SMALL_WIN_CAPTURE"})
_CONTEXT_CODES = _SOFT_CODES | frozenset({"TARGET_HIT"})
_FORCED_CODES = frozenset({
    "EOD_FORCE_CLOSE", "UNDERLYING_STOP_CONFIRMED", "HARD_OPTION_STOP",
    "SENTINEL_FORCED_EXIT", "DAILY_KILL_SWITCH", "KILL_SWITCH",
    "MANUAL_CLOSE", "MANUAL_EXIT", "BROKER_FORCE_CLOSE",
    "BROKER_POSITION_GONE", "RECONCILER_BROKER_GONE", "EMERGENCY_FLATTEN",
})


def _utc_now() -> _datetime:
    return _datetime.now(_timezone.utc)


def _coerce_dt(value: _Any) -> _Optional[_datetime]:
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
        value = float(value or 0.0)
    except Exception:
        return 0.0
    return value if value > 0 else 0.0


def _mode(value: _Any) -> str:
    return str(value or "").strip().lower()


@_dataclass(frozen=True)
class ExitDecisionQuoteSnapshot:
    position_id: str
    client_id: str
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
    cycle_id: str

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
    profit_lock_last_confirmation_cycle_id: str = ""
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

    def __setattr__(self, name: str, value: _Any) -> None:
        enabled = bool(self.__dict__.get("executable_bid_soft_exit_truth_enabled", False))
        if name in {"touched_profit", "touchedprofit"} and bool(value) and enabled:
            if self.__dict__.get("profit_lock_armed_at") is None:
                return
        if name in {"peak_pnl_pct", "peakpnlpct", "max_profit_seen", "maxprofitseen"} and enabled:
            canonical = float(self.__dict__.get("peak_executable_pnl_pct", 0.0) or 0.0)
            try:
                value = min(max(float(value or 0.0), 0.0), max(canonical, 0.0))
            except Exception:
                value = max(canonical, 0.0)
        object.__setattr__(self, name, value)


@_dataclass
class ExecutableQuoteApplication(_BaseExecutableQuoteApplication):
    pass


def _enabled(pos: _Any) -> bool:
    return getattr(pos, "executable_bid_soft_exit_truth_enabled", False) is True


def _apply_option_quote_for_decision(pos: ManagedPosition, *, bid: float, ask: float,
                                     mark: float, quote_ts: _Optional[_datetime] = None,
                                     source: str = ""):
    if not _enabled(pos):
        return _base_apply_option_quote(pos, bid=bid, ask=ask, mark=mark,
                                        quote_ts=quote_ts, source=source)
    bid, ask, mark = _positive(bid), _positive(ask), _positive(mark)
    midpoint = round((bid + ask) / 2.0, 4) if bid > 0 and ask > 0 else (mark or ask)
    analytics = mark or midpoint
    raw_mode = str(getattr(pos, "execution_mode", "") or "").strip()
    pricing_mode = _mode(raw_mode)
    if pricing_mode not in {"paper", "live"}:
        pricing_mode = "live_risk_unproven"
    valid, executable = bid > 0, bid if bid > 0 else 0.0
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
        aware = _coerce_dt(quote_ts)
        for name in ("last_option_quote_update_ts", "lastoptionquoteupdatets", "last_quote_update_ts"):
            try:
                setattr(pos, name, aware)
            except Exception:
                pass
    return ExecutableQuoteApplication(pricing_mode, executable, valid, analytics, src)


def _snapshot(pos: _Any) -> dict:
    raw = getattr(pos, "exit_decision_quote_snapshot", None)
    return dict(raw) if isinstance(raw, dict) else {}


def _validate_exit_truth_snapshot(pos: ManagedPosition, *,
                                  now_utc: _Optional[_datetime] = None) -> ExitTruthStatus:
    now_utc = now_utc or _utc_now()
    snap = _snapshot(pos)
    if not snap:
        return ExitTruthStatus(False, "missing_snapshot")
    expected = {
        "position_id": str(getattr(pos, "position_id", "") or ""),
        "client_id": str(getattr(pos, "client_id", "") or ""),
        "option_symbol": str(getattr(pos, "option_symbol", "") or "").upper(),
        "ticker": str(getattr(pos, "ticker", "") or "").upper(),
        "execution_mode": _mode(getattr(pos, "execution_mode", "")),
    }
    actual = {
        "position_id": str(snap.get("position_id") or ""),
        "client_id": str(snap.get("client_id") or ""),
        "option_symbol": str(snap.get("option_symbol") or "").upper(),
        "ticker": str(snap.get("ticker") or "").upper(),
        "execution_mode": _mode(snap.get("execution_mode")),
    }
    for key, wanted in expected.items():
        if not wanted:
            return ExitTruthStatus(False, f"position_identity_unproven:{key}",
                                   {"expected": expected, "actual": actual})
        if actual.get(key) != wanted:
            return ExitTruthStatus(False, f"snapshot_identity_mismatch:{key}",
                                   {"expected": expected, "actual": actual})
    if expected["execution_mode"] not in {"paper", "live"}:
        return ExitTruthStatus(False, "execution_mode_unproven")

    option_ts = _coerce_dt(snap.get("option_quote_timestamp"))
    underlying_ts = _coerce_dt(snap.get("underlying_quote_timestamp"))
    snapshot_ts = _coerce_dt(snap.get("snapshot_timestamp"))
    if option_ts is None:
        return ExitTruthStatus(False, "missing_or_invalid_option_timestamp")
    if underlying_ts is None:
        return ExitTruthStatus(False, "missing_or_invalid_underlying_timestamp")
    if snapshot_ts is None:
        return ExitTruthStatus(False, "missing_or_invalid_snapshot_timestamp")

    option_age = (now_utc - option_ts).total_seconds()
    underlying_age = (now_utc - underlying_ts).total_seconds()
    snapshot_age = (now_utc - snapshot_ts).total_seconds()
    diag = {"option_age_sec": option_age, "underlying_age_sec": underlying_age,
            "snapshot_age_sec": snapshot_age}
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
    diag.update({"cycle_skew_sec": cycle_skew, "option_snapshot_skew_sec": option_snapshot_skew,
                 "underlying_snapshot_skew_sec": underlying_snapshot_skew})
    if max(cycle_skew, option_snapshot_skew, underlying_snapshot_skew) > EXIT_DECISION_MAX_CYCLE_SKEW_SEC:
        return ExitTruthStatus(False, "cross_cycle_quote_pair", diag)
    if not str(snap.get("cycle_id") or "").strip():
        return ExitTruthStatus(False, "missing_cycle_id", diag)

    option_provider = str(snap.get("option_quote_provider") or "").lower().strip()
    underlying_provider = str(snap.get("underlying_quote_provider") or "").lower().strip()
    option_domain = str(snap.get("option_quote_domain") or "").lower().strip()
    underlying_domain = str(snap.get("underlying_quote_domain") or "").lower().strip()
    if not option_provider or not underlying_provider or "unknown" in option_provider or "unknown" in underlying_provider:
        return ExitTruthStatus(False, "missing_quote_provider", diag)
    if option_provider != underlying_provider:
        return ExitTruthStatus(False, "cross_provider_quote_pair", diag)
    if not option_domain or not underlying_domain or "unproven" in option_domain or "unproven" in underlying_domain:
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
    known = set(_FORCED_CODES) | set(_CONTEXT_CODES) | {
        SOFT_EXIT_DEFERRED_UNDERLYING_UNAVAILABLE,
        SOFT_EXIT_SUPPRESSED_CONFIRMING_THESIS,
    }
    if explicit in known:
        return explicit
    if "EOD FORCE CLOSE" in reason:
        return "EOD_FORCE_CLOSE"
    if "TARGET HIT" in reason:
        return "TARGET_HIT"
    if ("UNDERLYING" in reason and "STOP" in reason and ("CONFIRM" in reason or "STOP HIT" in reason)) or explicit == "STOP_HIT":
        return "UNDERLYING_STOP_CONFIRMED"
    if "HARD STOP" in reason or explicit in {"HARD_STOP", "MAX_LOSS", "EMERGENCY_STOP"}:
        return "HARD_OPTION_STOP"
    if "TOUCHED PROFIT" in reason or explicit in {"TOUCHED_PROFIT_STOP", "PROFIT_LOCK"}:
        return "TOUCHED_PROFIT_FLOOR"
    if "SMALL WIN" in reason or explicit == "SMALL_WIN_LOCK":
        return "SMALL_WIN_CAPTURE"
    if "RUNNER TRAIL" in reason or "TRAILING STOP" in reason or explicit in {"RUNNER_TRAIL", "TRAILING_STOP"}:
        return "RUNNER_TRAIL"
    if any(x in reason for x in ("THESIS_FAIL_SOFT_STOP", "THESIS_STALE_SOFT_STOP",
                                 "DEEP_LOSS_STOP", "NEVER GREEN STOP", "SOFT STOP")) or explicit in {
        "SOFT_LOSS", "SOFT_LOSS_WATCH", "DEEP_LOSS_STOP",
        "THESIS_FAIL_SOFT_STOP", "THESIS_STALE_SOFT_STOP", "NEVER_GREEN_STOP",
    }:
        return "SOFT_OPTION_STOP"
    if explicit:
        return explicit
    return str(_base_classify_exit_decision(decision) or "UNKNOWN_EXIT").upper()


def _classify_exit_decision(decision: _Any) -> str:
    return _canonical_reason_code(decision)


def _decision_pnl(pos: ManagedPosition) -> float:
    try:
        return float(pos.option_pnl_pct)
    except Exception:
        return 0.0


def _request_refresh(pos: ManagedPosition) -> bool:
    callback = getattr(pos, "_soft_exit_refresh_callback", None)
    if callable(callback):
        try:
            return bool(callback())
        except Exception as exc:
            log.debug("soft-exit refresh failed: %s", exc)
    return False


def _deferred(pos: ManagedPosition, status: ExitTruthStatus) -> _BaseExitDecision:
    pos.soft_exit_last_deferred_at = _utc_now()
    refreshed = _request_refresh(pos)
    pos.soft_exit_last_diagnostic = {
        "classification": SOFT_EXIT_DEFERRED_UNDERLYING_UNAVAILABLE,
        "truth_status": status.reason, "truth_diagnostics": status.diagnostics,
        "refresh_requested": refreshed, "snapshot": _snapshot(pos),
    }
    return _BaseExitDecision(
        "HOLD", 0,
        f"SOFT EXIT DEFERRED — same-cycle underlying truth invalid ({status.reason}); refresh requested={refreshed}",
        "NORMAL", _decision_pnl(pos), reason_code=SOFT_EXIT_DEFERRED_UNDERLYING_UNAVAILABLE,
    )


def _grace(pos: ManagedPosition, now_utc: _datetime) -> tuple[bool, float]:
    started = _coerce_dt(getattr(pos, "soft_exit_grace_started_at", None))
    if started is None:
        pos.soft_exit_grace_started_at = now_utc
        started = now_utc
    elapsed = max(0.0, (now_utc - started).total_seconds())
    return elapsed < SOFT_EXIT_THESIS_GRACE_SEC, elapsed


def _suppressed(pos: ManagedPosition, code: str, reason: str,
                elapsed: float) -> _BaseExitDecision:
    pos.soft_exit_last_suppressed_at = _utc_now()
    pos.soft_exit_last_diagnostic = {
        "classification": SOFT_EXIT_SUPPRESSED_CONFIRMING_THESIS,
        "suppressed_rule": code, "confirming_reason": reason,
        "grace_elapsed_sec": elapsed, "grace_limit_sec": SOFT_EXIT_THESIS_GRACE_SEC,
        "snapshot": _snapshot(pos),
    }
    return _BaseExitDecision(
        "HOLD", 0,
        f"{code} suppressed — fresh underlying confirms thesis ({reason}); grace={elapsed:.1f}/{SOFT_EXIT_THESIS_GRACE_SEC:.1f}s",
        "NORMAL", _decision_pnl(pos), reason_code=SOFT_EXIT_SUPPRESSED_CONFIRMING_THESIS,
    )


def _profit_floor(pos: ManagedPosition) -> float:
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


def _soft_candidate(pos: ManagedPosition) -> str:
    pnl = _decision_pnl(pos)
    if getattr(pos, "touched_profit", False) and int(getattr(pos, "scale_outs_done", 0) or 0) == 0 and pnl <= _profit_floor(pos):
        return "TOUCHED_PROFIT_FLOOR"
    peak = float(getattr(pos, "peak_executable_pnl_pct", 0.0) or 0.0)
    if peak >= float(getattr(_base, "IMMEDIATE_TP_PCT", 0.12)):
        if peak - pnl >= float(getattr(_base, "TRAIL_DROP_FROM_PEAK", 0.10)) or pnl <= 0:
            return "RUNNER_TRAIL"
    max_profit = float(getattr(pos, "max_profit_seen", 0.0) or 0.0)
    if max_profit >= float(getattr(_base, "SMALL_WIN_PCT", 0.12)):
        floor = max(0.03, max_profit - float(getattr(_base, "SMALL_WIN_TRAIL", 0.08)))
        if 0 < pnl <= floor:
            return "SMALL_WIN_CAPTURE"
    if pnl <= float(_os.getenv("SOFT_LOSS_STOP_PCT", "-0.12")):
        return "SOFT_OPTION_STOP"
    if not getattr(pos, "touched_profit", False) and float(_base._position_age_minutes(pos)) >= 20 and pnl <= -0.08:
        return "SOFT_OPTION_STOP"
    return ""


def _eod(pos: ManagedPosition, now_et: _datetime) -> _Optional[_BaseExitDecision]:
    hour, minute = now_et.hour, now_et.minute
    past = hour > _base.EOD_HARD_CLOSE_HOUR or (
        hour == _base.EOD_HARD_CLOSE_HOUR and minute >= _base.EOD_HARD_CLOSE_MIN
    )
    if (past or hour >= 16) and not getattr(pos, "overnight_hold_approved", False):
        return _BaseExitDecision("CLOSE_ALL", int(getattr(pos, "quantity_remaining", 0) or 0),
                                 f"EOD FORCE CLOSE -- {hour}:{minute:02d} ET", "IMMEDIATE",
                                 _decision_pnl(pos), reason_code="EOD_FORCE_CLOSE")
    return None


def _bind_decision(pos: ManagedPosition, decision: _Any) -> None:
    snap = _snapshot(pos)
    decision._exit_truth_cycle_id = str(snap.get("cycle_id") or "")
    decision._exit_truth_snapshot = dict(snap)
    decision._exit_truth_option_bid = _positive(snap.get("option_bid"))


def evaluate_exit(pos: ManagedPosition, now_et: _Optional[_datetime] = None):
    if not _enabled(pos):
        return _base_evaluate_exit(pos, now_et)
    if now_et is None:
        now_et = _datetime.now(_base.ET)
    now_utc = now_et.astimezone(_timezone.utc) if now_et.tzinfo else now_et.replace(tzinfo=_base.ET).astimezone(_timezone.utc)

    forced_eod = _eod(pos, now_et)
    if forced_eod is not None:
        return forced_eod

    hard_stop, _, _ = _base._effective_thresholds(pos)
    pnl = _decision_pnl(pos)
    if pnl <= hard_stop:
        return _BaseExitDecision(
            "STOP", int(getattr(pos, "quantity_remaining", 0) or 0),
            f"HARD STOP -- executable bid P&L {pnl*100:.1f}% exceeded {hard_stop*100:.1f}% max loss",
            "IMMEDIATE", pnl, reason_code="HARD_OPTION_STOP",
        )

    decision = _base_evaluate_exit(pos, now_et)
    code = _canonical_reason_code(decision)
    decision.reason_code = code
    if code in _FORCED_CODES:
        return decision

    truth = _validate_exit_truth_snapshot(pos, now_utc=now_utc)
    candidate = _soft_candidate(pos)
    if not candidate and decision.should_act and code in _SOFT_CODES:
        candidate = code
    if candidate:
        if not truth.valid:
            return _deferred(pos, truth)
        confirming, confirming_reason = _base._underlying_still_confirming(pos)
        if confirming and not getattr(pos, "is_at_stop", False):
            in_grace, elapsed = _grace(pos, now_utc)
            if in_grace:
                return _suppressed(pos, candidate, confirming_reason, elapsed)

    if decision.should_act and code in _CONTEXT_CODES:
        if not truth.valid:
            return _deferred(pos, truth)
        _bind_decision(pos, decision)
    return decision


def build_exit_decision_stamp(pos: ManagedPosition, decision: _Any, **kwargs) -> dict:
    payload = _base_build_exit_decision_stamp(pos, decision, **kwargs)
    snap = _snapshot(pos)
    armed_at = getattr(pos, "profit_lock_armed_at", None)
    payload.update({
        "reason_code": _canonical_reason_code(decision),
        "decision_quote_snapshot": snap,
        "executable_option_bid": _positive(snap.get("option_bid")) or _positive(getattr(pos, "current_bid", 0.0)),
        "analytics_option_midpoint": _positive(snap.get("option_midpoint")) or _positive(getattr(pos, "analytics_mark_price", 0.0)),
        "underlying_bid": _positive(snap.get("underlying_bid")),
        "underlying_ask": _positive(snap.get("underlying_ask")),
        "underlying_midpoint": _positive(snap.get("underlying_midpoint")),
        "option_quote_timestamp": snap.get("option_quote_timestamp"),
        "underlying_quote_timestamp": snap.get("underlying_quote_timestamp"),
        "option_quote_provider": snap.get("option_quote_provider"),
        "underlying_quote_provider": snap.get("underlying_quote_provider"),
        "option_quote_domain": snap.get("option_quote_domain"),
        "underlying_quote_domain": snap.get("underlying_quote_domain"),
        "profit_lock_armed_at": armed_at.isoformat() if isinstance(armed_at, _datetime) else armed_at,
        "profit_lock_arm_bid": float(getattr(pos, "profit_lock_arm_bid", 0.0) or 0.0),
        "profit_lock_arm_pnl_pct": float(getattr(pos, "profit_lock_arm_pnl_pct", 0.0) or 0.0),
        "peak_executable_bid": float(getattr(pos, "peak_executable_bid", 0.0) or 0.0),
        "peak_executable_pnl_pct": float(getattr(pos, "peak_executable_pnl_pct", 0.0) or 0.0),
        "profit_lock_confirmation_count": int(getattr(pos, "profit_lock_confirmation_count", 0) or 0),
        "decision_bound_cycle_id": getattr(decision, "_exit_truth_cycle_id", ""),
    })
    return payload


def _hydrate(pos: ManagedPosition, row: dict) -> None:
    meta = row.get("meta") if isinstance(row, dict) else None
    if isinstance(meta, str):
        try:
            meta = _json.loads(meta)
        except Exception:
            meta = {}
    truth = dict((meta or {}).get("soft_exit_truth") or {}) if isinstance(meta, dict) else {}
    for key in ("profit_lock_arm_bid", "profit_lock_arm_pnl_pct",
                "peak_executable_bid", "peak_executable_pnl_pct"):
        try:
            setattr(pos, key, float(truth.get(key) or 0.0))
        except Exception:
            pass
    pos.profit_lock_confirmation_count = int(truth.get("profit_lock_confirmation_count") or 0)
    pos.profit_lock_last_confirmation_cycle_id = str(truth.get("profit_lock_last_confirmation_cycle_id") or "")
    for key in ("profit_lock_armed_at", "profit_lock_confirmation_first_ts",
                "profit_lock_last_confirmation_ts", "soft_exit_grace_started_at",
                "soft_exit_last_suppressed_at", "soft_exit_last_deferred_at"):
        setattr(pos, key, _coerce_dt(truth.get(key)))
    if isinstance(truth.get("profit_lock_quote_timestamps"), list):
        pos.profit_lock_quote_timestamps = list(truth["profit_lock_quote_timestamps"])[-8:]
    if isinstance(truth.get("profit_lock_quote_sources"), list):
        pos.profit_lock_quote_sources = list(truth["profit_lock_quote_sources"])[-8:]
    if isinstance(truth.get("decision_quote_snapshot"), dict):
        pos.exit_decision_quote_snapshot = dict(truth["decision_quote_snapshot"])
    if isinstance(truth.get("soft_exit_last_diagnostic"), dict):
        pos.soft_exit_last_diagnostic = dict(truth["soft_exit_last_diagnostic"])
    if pos.peak_executable_pnl_pct > 0:
        pos.peak_pnl_pct = pos.peak_executable_pnl_pct
        pos.max_profit_seen = pos.peak_executable_pnl_pct
    if pos.profit_lock_armed_at is not None:
        pos.touched_profit = True


class APExitEngine(_BaseAPExitEngine):
    supports_executable_bid_soft_exit_truth = True

    def add_position(self, pos: ManagedPosition):
        pos.executable_bid_soft_exit_truth_enabled = True
        pos._soft_exit_refresh_callback = lambda p=pos: self._request_immediate_quote_retry(p)
        return _base_add_position(self, pos)

    def _managed_position_from_row(self, row: dict, qty_override: int = 0, *,
                                   prefer_qty_override: bool = False):
        pos = _base_managed_position_from_row(
            self, row, qty_override, prefer_qty_override=prefer_qty_override
        )
        pos.executable_bid_soft_exit_truth_enabled = True
        pos._soft_exit_refresh_callback = lambda p=pos: self._request_immediate_quote_retry(p)
        _hydrate(pos, row or {})
        return pos

    def seed_from_db(self, position_manager):
        result = _base_seed_from_db(self, position_manager)
        try:
            rows = list(position_manager.get_active_positions() or [])
            by_id = {str(row.get("id") or ""): row for row in rows if isinstance(row, dict)}
            for pos in self.active_positions():
                row = by_id.get(str(getattr(pos, "position_id", "") or ""))
                if row is not None:
                    pos.executable_bid_soft_exit_truth_enabled = True
                    pos._soft_exit_refresh_callback = lambda p=pos: self._request_immediate_quote_retry(p)
                    _hydrate(pos, row)
        except Exception as exc:
            log.warning("soft-exit startup hydration failed: %s", exc)
        return result

    def _qpm_enriched_snapshot(self, raw: dict, context: dict) -> ExitDecisionQuoteSnapshot:
        position_id = str(raw.get("position_id") or raw.get("positionid") or "")
        option_symbol = str(raw.get("option_symbol") or raw.get("optionsymbol") or "").upper()
        ticker = str(raw.get("ticker") or "").upper()
        option_raw = dict((context.get("option_quotes") or {}).get(option_symbol) or {})
        underlying_raw = dict((context.get("underlying_quotes") or {}).get(ticker) or {})
        fallback_ts = _coerce_dt(context.get("snapshot_timestamp")) or _utc_now()
        option_ts = _coerce_dt((context.get("option_quote_timestamps") or {}).get(option_symbol)) or fallback_ts
        underlying_ts = _coerce_dt((context.get("underlying_quote_timestamps") or {}).get(ticker)) or fallback_ts
        snapshot_ts = _coerce_dt(context.get("snapshot_timestamp")) or max(option_ts, underlying_ts)

        option_bid, option_ask = _positive(option_raw.get("bid")), _positive(option_raw.get("ask"))
        option_mid = round((option_bid + option_ask) / 2.0, 4) if option_bid > 0 and option_ask > 0 else (
            _positive(option_raw.get("mark")) or _positive(option_raw.get("last"))
        )
        underlying_bid, underlying_ask = _positive(underlying_raw.get("bid")), _positive(underlying_raw.get("ask"))
        underlying_last = (_positive(underlying_raw.get("last")) or _positive(underlying_raw.get("price"))
                           or _positive(underlying_raw.get("mark")) or _positive(underlying_raw.get("close")))
        underlying_mid = round((underlying_bid + underlying_ask) / 2.0, 4) if underlying_bid > 0 and underlying_ask > 0 else underlying_last

        modes, modes_by_symbol = context.get("original_modes") or {}, context.get("original_modes_by_symbol") or {}
        execution_mode = _mode(modes.get(position_id) or modes_by_symbol.get(option_symbol)
                               or context.get("default_execution_mode"))
        provider = str(context.get("quote_provider") or "unknown").lower().strip()
        domain = str(context.get("quote_domain") or "unknown").lower().strip()
        option_provider = str(option_raw.get("provider") or provider).lower().strip()
        underlying_provider = str(underlying_raw.get("provider") or provider).lower().strip()
        option_domain = str(option_raw.get("data_domain") or option_raw.get("domain") or domain).lower().strip()
        underlying_domain = str(underlying_raw.get("data_domain") or underlying_raw.get("domain") or domain).lower().strip()
        cycle_id = str(context.get("cycle_id") or "").strip()
        pos = getattr(self, "_positions_by_id", {}).get(position_id)
        if pos is None:
            pos = next((p for p in self.active_positions()
                        if str(getattr(p, "option_symbol", "") or "").upper() == option_symbol), None)
        client_id = str(getattr(pos, "client_id", "") or "")
        return ExitDecisionQuoteSnapshot(
            position_id, client_id, option_symbol, ticker,
            option_bid, option_ask, option_mid, option_ts,
            underlying_bid, underlying_ask, underlying_mid, underlying_ts,
            option_provider, underlying_provider, option_domain, underlying_domain,
            execution_mode, snapshot_ts, cycle_id,
        )

    @staticmethod
    def _reset_confirmation(pos: ManagedPosition) -> None:
        pos.profit_lock_confirmation_count = 0
        pos.profit_lock_confirmation_first_ts = None
        pos.profit_lock_last_confirmation_ts = None
        pos.profit_lock_last_confirmation_cycle_id = ""
        pos.profit_lock_quote_timestamps = []
        pos.profit_lock_quote_sources = []
        if pos.profit_lock_armed_at is None:
            pos.touched_profit = False

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
            if snap.execution_mode not in {"paper", "live"} or snap.execution_mode != _mode(pos.execution_mode):
                pos.exit_decision_quote_snapshot = snap.to_dict()
                pos.current_underlying = 0.0
                self._reset_confirmation(pos)
                self._persist_soft_exit_truth_to_db(pos)
                continue

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
            truth = _validate_exit_truth_snapshot(pos, now_utc=snap.snapshot_timestamp)

            if snap.option_bid > 0 and float(getattr(pos, "entry_price", 0.0) or 0.0) > 0:
                pnl = (snap.option_bid - float(pos.entry_price)) / float(pos.entry_price)
                if pnl > float(pos.peak_executable_pnl_pct or 0.0):
                    pos.peak_executable_pnl_pct = pnl
                    pos.peak_executable_bid = snap.option_bid
                pos.peak_pnl_pct = max(0.0, float(pos.peak_executable_pnl_pct or 0.0))
                pos.max_profit_seen = max(0.0, float(pos.peak_executable_pnl_pct or 0.0))

                if pos.profit_lock_armed_at is None:
                    prior_ts = _coerce_dt(pos.profit_lock_last_confirmation_ts)
                    distinct_cycle = bool(snap.cycle_id and snap.cycle_id != pos.profit_lock_last_confirmation_cycle_id)
                    consecutive = bool(
                        truth.valid and prior_ts is not None and distinct_cycle
                        and snap.option_quote_timestamp > prior_ts
                        and (snap.option_quote_timestamp - prior_ts).total_seconds()
                        <= TOUCHED_PROFIT_CONFIRM_MAX_GAP_SEC
                    )
                    if truth.valid and pnl >= TOUCHED_PROFIT_ARM_PCT:
                        if not consecutive:
                            pos.profit_lock_confirmation_count = 1
                            pos.profit_lock_confirmation_first_ts = snap.option_quote_timestamp
                            pos.profit_lock_quote_timestamps = []
                            pos.profit_lock_quote_sources = []
                        else:
                            pos.profit_lock_confirmation_count += 1
                        pos.profit_lock_last_confirmation_ts = snap.option_quote_timestamp
                        pos.profit_lock_last_confirmation_cycle_id = snap.cycle_id
                        pos.profit_lock_quote_timestamps = (
                            list(pos.profit_lock_quote_timestamps or []) + [snap.option_quote_timestamp.isoformat()]
                        )[-8:]
                        pos.profit_lock_quote_sources = (
                            list(pos.profit_lock_quote_sources or [])
                            + [f"{snap.option_quote_provider}:{snap.option_quote_domain}:bid"]
                        )[-8:]
                        if pos.profit_lock_confirmation_count >= TOUCHED_PROFIT_CONFIRM_SNAPSHOTS:
                            pos.profit_lock_armed_at = snap.option_quote_timestamp
                            pos.profit_lock_arm_bid = snap.option_bid
                            pos.profit_lock_arm_pnl_pct = pnl
                            pos.touched_profit = True
                    else:
                        self._reset_confirmation(pos)
                else:
                    pos.touched_profit = True
            else:
                self._reset_confirmation(pos)
            self._persist_soft_exit_truth_to_db(pos)

    def apply_quote_snapshots(self, snapshots: list[dict]) -> None:
        self.apply_exit_decision_snapshots(snapshots)

    def applyquotesnapshots(self, snapshots: list[dict]) -> None:
        self.apply_exit_decision_snapshots(snapshots)

    def _meta_payload(self, pos: ManagedPosition) -> dict:
        iso = lambda value: value.isoformat() if isinstance(value, _datetime) else value
        return {
            "profit_lock_armed_at": iso(pos.profit_lock_armed_at),
            "profit_lock_arm_bid": float(pos.profit_lock_arm_bid or 0.0),
            "profit_lock_arm_pnl_pct": float(pos.profit_lock_arm_pnl_pct or 0.0),
            "peak_executable_bid": float(pos.peak_executable_bid or 0.0),
            "peak_executable_pnl_pct": float(pos.peak_executable_pnl_pct or 0.0),
            "profit_lock_confirmation_count": int(pos.profit_lock_confirmation_count or 0),
            "profit_lock_confirmation_first_ts": iso(pos.profit_lock_confirmation_first_ts),
            "profit_lock_last_confirmation_ts": iso(pos.profit_lock_last_confirmation_ts),
            "profit_lock_last_confirmation_cycle_id": str(pos.profit_lock_last_confirmation_cycle_id or ""),
            "profit_lock_quote_timestamps": list(pos.profit_lock_quote_timestamps or [])[-8:],
            "profit_lock_quote_sources": list(pos.profit_lock_quote_sources or [])[-8:],
            "soft_exit_grace_started_at": iso(pos.soft_exit_grace_started_at),
            "soft_exit_last_suppressed_at": iso(pos.soft_exit_last_suppressed_at),
            "soft_exit_last_deferred_at": iso(pos.soft_exit_last_deferred_at),
            "soft_exit_last_diagnostic": dict(pos.soft_exit_last_diagnostic or {}),
            "decision_quote_snapshot": _snapshot(pos),
            "decision_price_authority": "executable_bid",
            "analytics_price": "midpoint_only",
        }

    def _persist_soft_exit_truth_to_db(self, pos: ManagedPosition, *, force: bool = False) -> bool:
        identity = self._protective_position_identity(pos)
        if identity is None:
            return False
        payload = self._meta_payload(pos)
        signature = _json.dumps(payload, sort_keys=True, default=str)
        diagnostic = payload.get("soft_exit_last_diagnostic") or {}
        transition_signature = _json.dumps({
            "armed": bool(payload.get("profit_lock_armed_at")),
            "confirmation_count": payload.get("profit_lock_confirmation_count"),
            "classification": diagnostic.get("classification"),
            "truth_status": diagnostic.get("truth_status"),
            "suppressed_rule": diagnostic.get("suppressed_rule"),
        }, sort_keys=True, default=str)
        now_epoch = _time.time()
        last_epoch = float(pos._soft_exit_truth_last_persist_epoch or 0.0)
        transition = transition_signature != str(pos._soft_exit_truth_last_transition_signature or "")
        due = now_epoch - last_epoch >= SOFT_EXIT_META_PERSIST_SEC
        if not force and not transition and not due:
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
                          AND COALESCE(NULLIF(contract, ''), NULLIF(option_symbol, ''), '') = %s
                          AND status IN ('OPEN','CLOSING','PARTIAL','ACTIVE')
                        """,
                        (patch, float(pos.peak_executable_pnl_pct or 0.0),
                         float(pos.peak_executable_pnl_pct or 0.0), bool(pos.touched_profit),
                         float(pos.current_bid or 0.0), _decision_pnl(pos),
                         identity.position_id, identity.client_id,
                         identity.execution_mode, identity.contract),
                    )
                    return int(cursor.rowcount or 0)

            rowcount = int(run_with_retry(update) or 0)
            if rowcount == 1:
                pos._soft_exit_truth_last_persist_epoch = now_epoch
                pos._soft_exit_truth_last_persist_signature = signature
                pos._soft_exit_truth_last_transition_signature = transition_signature
                return True
            log.warning("SOFT_EXIT_TRUTH_PERSIST_ROWCOUNT client=%s pos=%s mode=%s contract=%s rowcount=%s",
                        identity.client_id, identity.position_id, identity.execution_mode,
                        identity.contract, rowcount)
        except Exception as exc:
            log.debug("soft-exit truth persistence failed non-fatally: %s", exc)
        return False

    def _persist_peak_state_to_db(self, pos) -> bool:
        if _enabled(pos):
            return self._persist_soft_exit_truth_to_db(pos)
        return _base_persist_peak_state_to_db(self, pos)

    def adopt_canonical_position_identity(self, *args, **kwargs):
        contract = str(kwargs.get("contract") or (args[0] if args else "") or "").upper()
        repairs = [
            p for p in list(getattr(self, "_positions", []) or [])
            if str(getattr(p, "option_symbol", "") or "").upper() == contract
            and str(getattr(p, "position_id", "") or "").startswith("broker-repair-")
            and not getattr(p, "closed", False)
        ]
        state = [{
            "armed_at": getattr(p, "profit_lock_armed_at", None),
            "arm_bid": getattr(p, "profit_lock_arm_bid", 0.0),
            "arm_pnl": getattr(p, "profit_lock_arm_pnl_pct", 0.0),
            "peak_bid": getattr(p, "peak_executable_bid", 0.0),
            "peak_pnl": getattr(p, "peak_executable_pnl_pct", 0.0),
        } for p in repairs]
        result = _base_adopt_canonical_position_identity(self, *args, **kwargs)
        if getattr(result, "adopted", False) and state:
            canonical_id = str(kwargs.get("canonical_position_id")
                               or (args[1] if len(args) > 1 else "") or "")
            canonical = getattr(self, "_positions_by_id", {}).get(canonical_id)
            if canonical is not None:
                best = max(state, key=lambda item: float(item.get("peak_pnl") or 0.0))
                if float(best.get("peak_pnl") or 0.0) > float(canonical.peak_executable_pnl_pct or 0.0):
                    canonical.peak_executable_pnl_pct = float(best["peak_pnl"])
                    canonical.peak_executable_bid = float(best.get("peak_bid") or 0.0)
                    canonical.peak_pnl_pct = canonical.peak_executable_pnl_pct
                    canonical.max_profit_seen = canonical.peak_executable_pnl_pct
                armed_at = _coerce_dt(best.get("armed_at"))
                if armed_at is not None:
                    canonical.profit_lock_armed_at = armed_at
                    canonical.profit_lock_arm_bid = float(best.get("arm_bid") or 0.0)
                    canonical.profit_lock_arm_pnl_pct = float(best.get("arm_pnl") or 0.0)
                    canonical.touched_profit = True
                self._persist_soft_exit_truth_to_db(canonical, force=True)
        return result

    def _submit_exit_decision(self, pos: ManagedPosition, decision: _Any, *,
                              kill_active: bool = False,
                              allow_inflight_override: bool = False):
        if _enabled(pos):
            decision.reason_code = _canonical_reason_code(decision)
            if decision.reason_code in _CONTEXT_CODES:
                truth = _validate_exit_truth_snapshot(pos)
                current_cycle = str(_snapshot(pos).get("cycle_id") or "")
                decision_cycle = str(getattr(decision, "_exit_truth_cycle_id", "") or "")
                if not truth.valid or not decision_cycle or decision_cycle != current_cycle:
                    reason = truth.reason if not truth.valid else "decision_snapshot_cycle_changed"
                    deferred = _deferred(pos, ExitTruthStatus(
                        False, reason,
                        {"decision_cycle_id": decision_cycle, "current_cycle_id": current_cycle},
                    ))
                    self._emit_exit_event(
                        pos, decision="HOLD", reason_code=deferred.reason_code,
                        explanation=deferred.reason, stage="exit_submission",
                        extra_inputs={"decision_quote_snapshot": _snapshot(pos),
                                      "decision_cycle_id": decision_cycle,
                                      "current_cycle_id": current_cycle},
                    )
                    self._persist_soft_exit_truth_to_db(pos, force=True)
                    return False
                decision.suggested_limit = float(getattr(pos, "current_bid", 0.0) or 0.0)
        return _base_submit_exit_decision(
            self, pos, decision, kill_active=kill_active,
            allow_inflight_override=allow_inflight_override,
        )


_base.ManagedPosition = ManagedPosition
_base.APExitEngine = APExitEngine
_base.evaluate_exit = evaluate_exit
_base._apply_option_quote_for_decision = _apply_option_quote_for_decision
_base.ExecutableQuoteApplication = ExecutableQuoteApplication
_base._classify_exit_decision = _classify_exit_decision
_base.build_exit_decision_stamp = build_exit_decision_stamp
_base.FORCED_RISK_EXIT_CODES = set(_FORCED_CODES) | {"STOP_HIT", "HARD_STOP", "MAX_LOSS", "EMERGENCY_STOP"}

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
