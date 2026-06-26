from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

log = logging.getLogger(__name__)


def _ef(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return default


def _truthy_env(key: str, default: bool) -> bool:
    raw = os.getenv(key)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _safe_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out > 0 else None


def _normalize_pct_threshold(value: float) -> float:
    """Normalize percentage-like thresholds to decimal form for option gain.

    Existing submit guards use whole percentages (8.0 == 8%). The new remaining
    opportunity config is specified as 0.08 == 8%. Accept both forms so a Render
    typo of 8 still means 8%, not 800%.
    """
    return value / 100.0 if value > 1.0 else value


def _tier_confirm_seconds(score: Optional[float], tier: Optional[str], timeframe: Optional[str]) -> float:
    base = _ef("ENTRY_CONFIRM_SECONDS", 45.0)
    tf = (timeframe or "1d").lower()
    s = float(score or 0)
    if any(tf.startswith(x) for x in ("15", "30", "60")):
        return max(base, _ef("CLIENT_PROOF_QUOTE_MAX_AGE_SECONDS", 10.0))
    if s >= 78:
        return min(base, 15.0)
    if s >= 70:
        return min(base, 45.0)
    return base


@dataclass
class ConfirmationResult:
    passed: bool
    fail_reason: Optional[str]
    metadata: dict = field(default_factory=dict)

    def to_meta(self, started_at: str, completed_at: str) -> dict:
        return {
            "confirmation_required": self.metadata.get("confirmation_required", True),
            "confirmation_seconds": self.metadata.get("confirmation_seconds"),
            "confirmation_started_at": started_at,
            "confirmation_completed_at": completed_at,
            "confirmation_passed": self.passed,
            "confirmation_fail_reason": self.fail_reason,
            **{k: v for k, v in self.metadata.items() if k not in ("confirmation_seconds", "confirmation_required")},
        }


def _fail(reason: str, meta: dict) -> ConfirmationResult:
    log.info("[ENTRY_CONFIRM] BLOCKED reason=%s", reason)
    return ConfirmationResult(False, reason, meta)


def _pass(meta: dict) -> ConfirmationResult:
    log.info("[ENTRY_CONFIRM] PASSED")
    return ConfirmationResult(True, None, meta)


_DAILY_TIMEFRAMES = {"1d", "d", "daily", "1day", "1 day", "overnight"}


def _plan_metadata(plan: Any) -> dict:
    if hasattr(plan, "metadata"):
        meta = getattr(plan, "metadata", None)
        return meta if isinstance(meta, dict) else {}
    if isinstance(plan, dict):
        meta = plan.get("metadata")
        return meta if isinstance(meta, dict) else {}
    return {}


def _plan_get(plan: Any, *names: str) -> Any:
    meta = _plan_metadata(plan)
    trig = meta.get("trigger") if isinstance(meta.get("trigger"), dict) else {}
    for name in names:
        if hasattr(plan, name):
            value = getattr(plan, name)
            if value is not None and value != "":
                return value
        if isinstance(plan, dict):
            value = plan.get(name)
            if value is not None and value != "":
                return value
        value = meta.get(name)
        if value is not None and value != "":
            return value
        value = trig.get(name)
        if value is not None and value != "":
            return value
    return None


def _quality_mode_gate_status(meta: dict) -> str:
    candidates = []
    if isinstance(meta.get("quality_mode_result"), dict):
        candidates.append(meta.get("quality_mode_result"))
    score_audit = meta.get("score_audit")
    if isinstance(score_audit, dict) and isinstance(score_audit.get("quality_mode_result"), dict):
        candidates.append(score_audit.get("quality_mode_result"))
    for item in candidates:
        status = item.get("gate_status") or item.get("status") or item.get("quality_mode_status")
        if status:
            return str(status).upper().strip()
    return ""


def _is_daily_or_overnight(timeframe: Optional[str], meta: dict) -> bool:
    tf = str(timeframe or meta.get("timeframe") or "").strip().lower()
    return tf in _DAILY_TIMEFRAMES or bool(
        meta.get("overnight") or meta.get("contract_deferred") or meta.get("force_overnight_reeval_only")
    )


def _resolve_expected_option_gain_pct(
    *,
    plan: Any,
    remaining_move: Optional[float],
    submit_ask: Optional[float],
) -> tuple[Optional[float], str]:
    """Resolve expected option gain as a decimal fraction.

    Prefer production-supplied projected option upside when present. If absent,
    use contract delta if available; if delta is also absent, use the conservative
    selector-band default so the guard remains deterministic and auditable.
    """
    explicit = _safe_float(
        _plan_get(
            plan,
            "expected_option_gain_pct",
            "projected_option_gain_pct",
            "option_expected_gain_pct",
            "estimated_option_gain_pct",
        )
    )
    if explicit is not None:
        return _normalize_pct_threshold(explicit), "explicit_expected_gain_pct"

    target_option_price = _safe_float(
        _plan_get(
            plan,
            "target_option_price",
            "option_target_price",
            "target_contract_price",
            "projected_option_target_price",
        )
    )
    if target_option_price is not None and submit_ask is not None:
        return (target_option_price - submit_ask) / submit_ask, "target_option_price"

    if remaining_move is None or submit_ask is None or submit_ask <= 0:
        return None, "unavailable"

    delta = _safe_float(
        _plan_get(
            plan,
            "contract_delta",
            "selected_delta",
            "delta",
            "option_delta",
        )
    )
    if delta is not None:
        delta = abs(delta)
        source = "contract_delta"
    else:
        delta = abs(_ef("REMAINING_OPPORTUNITY_DEFAULT_DELTA", 0.30))
        source = "default_delta_proxy"

    return (delta * remaining_move) / submit_ask, source


def _remaining_opportunity_check(
    *,
    plan: Any,
    dirn: str,
    current: Optional[float],
    target: Optional[float],
    stop: Optional[float],
    submit_ask: Optional[float],
) -> tuple[bool, Optional[str], dict]:
    enabled = _truthy_env("REMAINING_OPPORTUNITY_FILTER_ENABLED", True)
    min_remaining_move_pct = _ef("MIN_REMAINING_MOVE_PCT", 0.25)
    min_remaining_r = _ef("MIN_REMAINING_R", 0.75)
    min_expected_option_gain_pct = _normalize_pct_threshold(
        _ef("MIN_EXPECTED_OPTION_GAIN_PCT", 0.08)
    )

    audit: dict[str, Any] = {
        "remaining_opportunity_filter": {
            "enabled": enabled,
            "min_remaining_move_pct": min_remaining_move_pct,
            "min_remaining_r": min_remaining_r,
            "min_expected_option_gain_pct": min_expected_option_gain_pct,
            "current_underlying": current,
            "target_underlying": target,
            "stop_underlying": stop,
            "submit_ask": submit_ask,
        }
    }

    if not enabled:
        audit["remaining_opportunity_filter"].update(
            {"passed": True, "skipped": True, "skip_reason": "filter_disabled"}
        )
        return True, None, audit

    def block(reason: str, **extra: Any) -> tuple[bool, str, dict]:
        payload = {
            **audit["remaining_opportunity_filter"],
            **extra,
            "passed": False,
            "block_reason": reason,
        }
        return False, reason, {"remaining_opportunity_filter": payload, **payload}

    if dirn not in {"CALL", "PUT"} or current is None or target is None:
        # Existing metadata/shape guards own missing/invalid values. This helper
        # should not introduce a second reason taxonomy for already-invalid shape.
        audit["remaining_opportunity_filter"].update(
            {"passed": True, "skipped": True, "skip_reason": "missing_required_shape"}
        )
        return True, None, audit

    if dirn == "CALL":
        if target <= current:
            return block(
                "remaining_opportunity_failed:target_already_reached",
                remaining_move=round(target - current, 6),
                remaining_move_pct=round(((target - current) / current) * 100.0, 6) if current else None,
            )
        if stop is not None and stop >= current:
            return block("remaining_opportunity_failed:target_wrong_side")
        remaining_move = target - current
        risk_to_stop = (current - stop) if stop is not None else None
    else:
        if target >= current:
            return block(
                "remaining_opportunity_failed:target_already_reached",
                remaining_move=round(current - target, 6),
                remaining_move_pct=round(((current - target) / current) * 100.0, 6) if current else None,
            )
        if stop is not None and stop <= current:
            return block("remaining_opportunity_failed:target_wrong_side")
        remaining_move = current - target
        risk_to_stop = (stop - current) if stop is not None else None

    if remaining_move <= 0:
        return block(
            "remaining_opportunity_failed:target_already_reached",
            remaining_move=round(remaining_move, 6),
        )

    remaining_move_pct = (remaining_move / current) * 100.0 if current else 0.0
    remaining_r = (
        (remaining_move / risk_to_stop)
        if risk_to_stop is not None and risk_to_stop > 0
        else None
    )
    expected_option_gain_pct, expected_source = _resolve_expected_option_gain_pct(
        plan=plan,
        remaining_move=remaining_move,
        submit_ask=submit_ask,
    )

    common = {
        "remaining_move": round(remaining_move, 6),
        "remaining_move_pct": round(remaining_move_pct, 6),
        "remaining_r": round(remaining_r, 6) if remaining_r is not None else None,
        "risk_to_stop": round(risk_to_stop, 6) if risk_to_stop is not None else None,
        "expected_option_gain_pct": (
            round(expected_option_gain_pct, 6)
            if expected_option_gain_pct is not None
            else None
        ),
        "expected_option_gain_source": expected_source,
    }

    if remaining_move_pct < min_remaining_move_pct:
        return block(
            "remaining_opportunity_failed:insufficient_move_remaining",
            failed_metric="remaining_move_pct",
            **common,
        )

    if remaining_r is not None and remaining_r < min_remaining_r:
        return block(
            "remaining_opportunity_failed:insufficient_move_remaining",
            failed_metric="remaining_r",
            **common,
        )

    if (
        expected_option_gain_pct is not None
        and expected_option_gain_pct < min_expected_option_gain_pct
    ):
        return block(
            "remaining_opportunity_failed:insufficient_expected_option_gain",
            failed_metric="expected_option_gain_pct",
            **common,
        )

    payload = {
        **audit["remaining_opportunity_filter"],
        **common,
        "passed": True,
        "block_reason": None,
    }
    return True, None, {"remaining_opportunity_filter": payload, **payload}


def _final_live_entry_guard(
    *,
    plan: Any,
    direction: str,
    trigger_price: Optional[float],
    underlying_last: Optional[float],
    live_ask: Optional[float],
    decision_option_price: Optional[float],
    timeframe: Optional[str],
    sandbox_mode: bool,
) -> tuple[bool, Optional[str], dict]:
    meta = _plan_metadata(plan)
    client_id = str(_plan_get(plan, "client_id", "client_email") or meta.get("client_id") or "")
    execution_mode = str(
        _plan_get(plan, "execution_mode", "mode")
        or meta.get("execution_mode")
        or ("paper" if sandbox_mode else "live")
    ).lower()
    ticker = str(_plan_get(plan, "ticker", "symbol") or meta.get("ticker") or meta.get("symbol") or "")
    signal_id = str(_plan_get(plan, "signal_id") or meta.get("signal_id") or "")
    pattern = str(_plan_get(plan, "pattern", "pattern_id") or meta.get("pattern") or meta.get("pattern_id") or "")
    dirn = str(direction or _plan_get(plan, "side", "direction") or "CALL").upper().strip()
    guard_enabled = _truthy_env("LIVE_FINAL_ENTRY_GUARD_ENABLED", True)

    base = {
        "live_final_entry_guard": {
            "enabled": guard_enabled,
            "applies_to_live_only": True,
            "sandbox_mode": bool(sandbox_mode),
            "client_id": client_id,
            "execution_mode": execution_mode,
            "signal_id": signal_id,
            "ticker": ticker,
            "side": dirn,
            "timeframe": timeframe,
            "pattern": pattern,
        }
    }

    if execution_mode != "live" or sandbox_mode or not guard_enabled:
        skip_reason = "non_live_execution_mode" if execution_mode != "live" else ("sandbox_mode" if sandbox_mode else "guard_disabled")
        base["live_final_entry_guard"].update({"passed": True, "skipped": True, "skip_reason": skip_reason})
        return True, None, base

    trigger = _safe_float(trigger_price) or _safe_float(_plan_get(plan, "trigger_price", "entry_trigger", "entry_price", "entry"))
    stop = _safe_float(_plan_get(plan, "stop_underlying", "stop_price", "stop"))
    target = _safe_float(_plan_get(plan, "target_underlying", "target_price", "pt1", "pt2"))
    current = _safe_float(underlying_last)
    submit_ask = _safe_float(live_ask)
    reference = (
        _safe_float(decision_option_price)
        or _safe_float(_plan_get(plan, "original_selector_ask", "selector_reference_price"))
        or _safe_float(_plan_get(plan, "entry_option_price", "contract_premium", "limit_price"))
    )
    daily_or_overnight = _is_daily_or_overnight(timeframe, meta)
    max_distance_pct = _ef("LIVE_MAX_TRIGGER_DISTANCE_PCT_DAILY", 0.75)
    max_drift_pct = _ef("LIVE_MAX_PREMIUM_DRIFT_PCT", 8.0)
    distance_to_trigger_pct = ((current - trigger) / trigger) * 100.0 if trigger and current else None
    premium_drift_pct = ((submit_ask - reference) / reference) * 100.0 if reference and submit_ask else None

    guard_payload = {
        "passed": False,
        "client_id": client_id,
        "execution_mode": execution_mode,
        "signal_id": signal_id,
        "ticker": ticker,
        "side": dirn,
        "timeframe": timeframe,
        "pattern": pattern,
        "trigger_price": trigger,
        "stop_underlying": stop,
        "target_underlying": target,
        "current_underlying": current,
        "distance_to_trigger_pct": round(distance_to_trigger_pct, 6) if distance_to_trigger_pct is not None else None,
        "selector_reference_price": reference,
        "submit_ask": submit_ask,
        "premium_drift_pct": round(premium_drift_pct, 6) if premium_drift_pct is not None else None,
        "max_trigger_distance_pct_daily": max_distance_pct,
        "max_premium_drift_pct": max_drift_pct,
        "daily_or_overnight": daily_or_overnight,
        "quality_mode_gate_status": _quality_mode_gate_status(meta),
    }

    def block(reason: str, **extra: Any) -> tuple[bool, str, dict]:
        payload = {**guard_payload, **extra, "passed": False, "block_reason": reason}
        return False, reason, {"live_final_entry_guard": payload, **payload}

    if dirn not in {"CALL", "PUT"}:
        return block("LIVE_ENTRY_GUARD_INVALID_DIRECTION")
    if trigger is None or current is None:
        return block("LIVE_ENTRY_GUARD_MISSING_CURRENT_OR_TRIGGER")

    require_shape = _truthy_env("LIVE_REQUIRE_VALID_TARGET_SHAPE", True)
    if require_shape and (target is None or stop is None):
        return block("LIVE_ENTRY_GUARD_MISSING_TARGET_OR_STOP")

    if dirn == "CALL":
        if current <= trigger:
            return block("CALL_TRIGGER_NOT_HELD")
        if require_shape and target is not None and trigger is not None and target <= trigger:
            return block("remaining_opportunity_failed:target_wrong_side")
        if require_shape and stop is not None and trigger is not None and trigger <= stop:
            return block("remaining_opportunity_failed:target_wrong_side")
    else:
        if current >= trigger:
            return block("PUT_TRIGGER_NOT_HELD")
        if require_shape and target is not None and trigger is not None and target >= trigger:
            return block("remaining_opportunity_failed:target_wrong_side")
        if require_shape and stop is not None and trigger is not None and trigger >= stop:
            return block("remaining_opportunity_failed:target_wrong_side")

    remaining_ok, remaining_reason, remaining_meta = _remaining_opportunity_check(
        plan=plan,
        dirn=dirn,
        current=current,
        target=target,
        stop=stop,
        submit_ask=submit_ask,
    )
    guard_payload["remaining_opportunity_filter"] = remaining_meta.get("remaining_opportunity_filter")
    if not remaining_ok:
        return block(remaining_reason or "remaining_opportunity_failed", **remaining_meta)

    if daily_or_overnight and distance_to_trigger_pct is not None and abs(distance_to_trigger_pct) > max_distance_pct:
        return block("LIVE_TRIGGER_DISTANCE_TOO_FAR")
    if submit_ask is None:
        return block("LIVE_PREMIUM_DRIFT_SUBMIT_ASK_MISSING")
    if reference is None:
        if _truthy_env("LIVE_BLOCK_PREMIUM_DRIFT_UNKNOWN", True):
            return block("LIVE_PREMIUM_DRIFT_REFERENCE_MISSING")
    elif premium_drift_pct is not None and premium_drift_pct > max_drift_pct:
        return block("LIVE_PREMIUM_DRIFT_TOO_HIGH")

    guard_payload["passed"] = True
    guard_payload["block_reason"] = None
    if guard_payload.get("quality_mode_gate_status") == "DISABLED":
        guard_payload["quality_mode_disabled_replaced_by_final_guard"] = True
    return True, None, {"live_final_entry_guard": guard_payload, **guard_payload}


def check_entry_confirmation(
    *,
    plan,
    direction: str,
    trigger_price: Optional[float],
    live_bid: Optional[float],
    live_ask: Optional[float],
    live_quote_age_ms: Optional[float],
    underlying_last: Optional[float],
    decision_option_price: Optional[float],
    score: Optional[float] = None,
    tier: Optional[str] = None,
    timeframe: Optional[str] = None,
    sandbox_mode: bool = False,
) -> ConfirmationResult:
    started_at = datetime.now(timezone.utc).isoformat()
    meta_src = _plan_metadata(plan)
    gate_meta = meta_src.get("hybrid_client_quality_gate") or {}
    confirmation_required = gate_meta.get("confirmation_required", False)

    guard_ok, guard_reason, guard_meta = _final_live_entry_guard(
        plan=plan,
        direction=direction,
        trigger_price=trigger_price,
        underlying_last=underlying_last,
        live_ask=live_ask,
        decision_option_price=decision_option_price,
        timeframe=timeframe,
        sandbox_mode=sandbox_mode,
    )
    if not guard_ok:
        return _fail(guard_reason or "live_final_entry_guard_failed", {**guard_meta, "confirmation_required": True})

    if not confirmation_required:
        return ConfirmationResult(True, None, {**guard_meta, "confirmation_required": False})

    max_fade = _ef("MAX_PRE_ENTRY_OPTION_FADE_PCT", 8.0)
    max_reversal = _ef("MAX_PRE_ENTRY_UNDERLYING_REVERSAL_PCT", 0.25)
    max_spread = _ef("CLIENT_PROOF_MAX_SPREAD_PCT", 0.10)
    max_quote_age_s = _ef("CLIENT_PROOF_QUOTE_MAX_AGE_SECONDS", 10.0)
    confirm_s = _tier_confirm_seconds(score, tier, timeframe)
    dirn = direction.upper()

    live_mid = None
    spread_pct = None
    if live_bid is not None and live_ask is not None and live_bid > 0 and live_ask > 0:
        live_mid = (live_bid + live_ask) / 2
        spread_pct = (live_ask - live_bid) / live_mid if live_mid > 0 else None
    quote_age_s = (live_quote_age_ms or 0) / 1000.0

    base = {
        **guard_meta,
        "confirmation_required": True,
        "confirmation_seconds": confirm_s,
        "underlying_start": trigger_price,
        "underlying_end": underlying_last,
        "option_mid_start": decision_option_price,
        "option_mid_end": live_mid,
        "option_move_pct": None,
        "underlying_move_pct": None,
        "quote_age_seconds": round(quote_age_s, 2),
        "spread_pct": round(spread_pct, 4) if spread_pct is not None else None,
        "live_entry_bid": live_bid,
        "live_entry_ask": live_ask,
        "live_entry_mid": live_mid,
        "live_entry_ts": started_at,
        "paper_quote_lag_warning": False,
        "sandbox_mode": sandbox_mode,
    }

    effective_max_age = min(confirm_s, max_quote_age_s)
    if live_bid is None and live_ask is None:
        return _fail("entry_confirm_failed_stale_quote", {**base, "reason": "no live quote available at submit time"})
    if quote_age_s > effective_max_age:
        return _fail("entry_confirm_failed_stale_quote", {**base, "quote_age_seconds": round(quote_age_s, 2), "max_age_seconds": effective_max_age})
    if spread_pct is not None and spread_pct > max_spread:
        return _fail("entry_confirm_failed_spread", {**base, "spread_pct": round(spread_pct, 4), "max_spread_pct": max_spread})

    if decision_option_price and decision_option_price > 0 and live_mid is not None:
        fade_pct = (decision_option_price - live_mid) / decision_option_price * 100
        base["option_move_pct"] = round(fade_pct, 2)
        if fade_pct > max_fade:
            return _fail("entry_confirm_failed_option_fade", {**base, "fade_pct": round(fade_pct, 2), "max_fade_pct": max_fade, "decision_price": decision_option_price, "live_mid": live_mid})
        if sandbox_mode and fade_pct > 2.0:
            base["paper_quote_lag_warning"] = True

    if underlying_last is not None and trigger_price is not None and trigger_price > 0:
        move_pct = (underlying_last - trigger_price) / trigger_price * 100
        base["underlying_move_pct"] = round(move_pct, 2)
        if dirn == "CALL":
            reversal = -move_pct
            if reversal > max_reversal:
                return _fail("entry_confirm_failed_underlying_reversal", {**base, "underlying_last": underlying_last, "trigger_price": trigger_price, "reversal_pct": round(reversal, 3), "max_reversal_pct": max_reversal, "direction": "CALL"})
        elif dirn == "PUT":
            reversal = move_pct
            if reversal > max_reversal:
                return _fail("entry_confirm_failed_underlying_reversal", {**base, "underlying_last": underlying_last, "trigger_price": trigger_price, "reversal_pct": round(reversal, 3), "max_reversal_pct": max_reversal, "direction": "PUT"})

    return _pass(base)
