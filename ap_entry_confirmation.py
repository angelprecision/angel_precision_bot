"""
ap_entry_confirmation.py
========================
P0 Follow-up: Pre-submit entry confirmation preflight.

Called from APExecutionCore._on_entry_trigger() AFTER:
  1. _breach_risk_check() passes
  2. contract selected (deferred resolved)
  3. live ask refreshed via _refresh_ask_at_submit()

And BEFORE submit_existing_entry() — the broker call.

DESIGN PRINCIPLE
----------------
This is NOT a sleep/delay loop.  The watcher already confirmed the
price held for MOMENTUM_POLLS_REQUIRED ticks.  This preflight is a
single synchronous check:  are conditions STILL clean right now?

For A+ setups (score ≥ 78, daily, clean quote) this completes in
milliseconds.  For B-tier or intraday it uses the same logic but with
wider thresholds if configured.

WHEN confirmation_required IS NOT SET
--------------------------------------
The legacy confirmation gate is a no-op, but LIVE still runs the final
pre-submit entry-quality guard. This protects live money from stale daily
plans and option-premium chasing even when older metadata did not mark
entry_confirmation.confirmation_required=true.

ENV FLAGS (hot-read per call)
------------------------------
ENTRY_CONFIRM_SECONDS            = 45   (max quote age treated as fresh)
MAX_PRE_ENTRY_OPTION_FADE_PCT    = 8    (block if option faded > 8%)
MAX_PRE_ENTRY_UNDERLYING_REVERSAL_PCT = 0.25  (block if price moved > 0.25% wrong way)
CLIENT_PROOF_MAX_SPREAD_PCT      = 0.10 (block if spread > 10%)
CLIENT_PROOF_QUOTE_MAX_AGE_SECONDS = 10 (block if quote older than 10s)
LIVE_FINAL_ENTRY_GUARD_ENABLED   = true
LIVE_MAX_TRIGGER_DISTANCE_PCT_DAILY = 0.75
LIVE_MAX_PREMIUM_DRIFT_PCT       = 8.0
LIVE_BLOCK_PREMIUM_DRIFT_UNKNOWN = true
LIVE_REQUIRE_VALID_TARGET_SHAPE  = true
"""

from __future__ import annotations

import os
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

log = logging.getLogger(__name__)


# ── Config ────────────────────────────────────────────────────────────────────

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
    if out <= 0:
        return None
    return out


def _tier_confirm_seconds(score: Optional[float], tier: Optional[str],
                          timeframe: Optional[str]) -> float:
    """
    Tier-based confirmation window (used as max quote age).
    Faster for high-conviction setups — we want those trades to go through.
    """
    base = _ef("ENTRY_CONFIRM_SECONDS", 45.0)
    tf = (timeframe or "1d").lower()
    s = float(score or 0)
    is_intraday = any(tf.startswith(x) for x in ("15", "30", "60"))

    if is_intraday:
        # Intraday: stricter — but don't use a fixed long wait
        return max(base, _ef("CLIENT_PROOF_QUOTE_MAX_AGE_SECONDS", 10.0))

    if s >= 78:           # A+ daily: quote must be fresh (10-15s)
        return min(base, 15.0)
    elif s >= 70:         # B-tier daily: 30-45s
        return min(base, 45.0)
    return base           # fallback


# ── Result ────────────────────────────────────────────────────────────────────

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
            **{k: v for k, v in self.metadata.items()
               if k not in ("confirmation_seconds", "confirmation_required")},
        }


def _fail(reason: str, meta: dict) -> ConfirmationResult:
    log.info("[ENTRY_CONFIRM] BLOCKED reason=%s", reason)
    return ConfirmationResult(passed=False, fail_reason=reason, metadata=meta)


def _pass(meta: dict) -> ConfirmationResult:
    log.info("[ENTRY_CONFIRM] PASSED")
    return ConfirmationResult(passed=True, fail_reason=None, metadata=meta)


# ── Final live entry-quality guard ────────────────────────────────────────────

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
    if tf in _DAILY_TIMEFRAMES:
        return True
    return bool(meta.get("overnight") or meta.get("contract_deferred") or meta.get("force_overnight_reeval_only"))


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
    """
    Live-only final pre-submit guard.

    Runs after watcher trigger and option quote refresh, before broker submit.
    Paper/sandbox mode is intentionally unchanged.
    """
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

    base = {
        "live_final_entry_guard": {
            "enabled": _truthy_env("LIVE_FINAL_ENTRY_GUARD_ENABLED", True),
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

    if sandbox_mode or not _truthy_env("LIVE_FINAL_ENTRY_GUARD_ENABLED", True):
        base["live_final_entry_guard"].update({"passed": True, "skipped": True})
        return True, None, base

    trigger = _safe_float(trigger_price) or _safe_float(
        _plan_get(plan, "trigger_price", "entry_trigger", "entry_price", "entry")
    )
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

    distance_to_trigger_pct = None
    if trigger and current:
        distance_to_trigger_pct = ((current - trigger) / trigger) * 100.0

    premium_drift_pct = None
    if reference and submit_ask:
        premium_drift_pct = ((submit_ask - reference) / reference) * 100.0

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
        if target is not None and current >= target:
            return block("STALE_TARGET_ALREADY_REACHED_CALL")
        if require_shape and target is not None and stop is not None and not (target > trigger > stop):
            return block("INVALID_TARGET_TRIGGER_STOP_SHAPE_CALL")
    else:  # PUT
        if current >= trigger:
            return block("PUT_TRIGGER_NOT_HELD")
        if target is not None and current <= target:
            return block("STALE_TARGET_ALREADY_REACHED_PUT")
        if require_shape and target is not None and stop is not None and not (target < trigger < stop):
            return block("INVALID_TARGET_TRIGGER_STOP_SHAPE_PUT")

    if daily_or_overnight and distance_to_trigger_pct is not None:
        if abs(distance_to_trigger_pct) > max_distance_pct:
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


# ── Main preflight ────────────────────────────────────────────────────────────

def check_entry_confirmation(
    *,
    plan,                            # APTradePlan or dict with .metadata / .get()
    direction: str,                  # "CALL" or "PUT"
    trigger_price: Optional[float],
    live_bid: Optional[float],       # option bid at submit time (from _refresh_ask_at_submit)
    live_ask: Optional[float],       # option ask at submit time
    live_quote_age_ms: Optional[float],  # ms since quote was fetched
    underlying_last: Optional[float],    # current underlying price
    decision_option_price: Optional[float],  # option price at signal decision time
    score: Optional[float] = None,
    tier: Optional[str] = None,
    timeframe: Optional[str] = None,
    sandbox_mode: bool = False,
) -> ConfirmationResult:
    """
    Pre-submit confirmation preflight.

    Checks (in order, fail-fast):
    1. LIVE final entry-quality guard (always active for live)
    2. confirmation_required present in plan metadata — skip legacy checks if absent
    3. Quote freshness
    4. Spread acceptability
    5. Option fade from decision price
    6. Underlying reversal from trigger

    Returns immediately on first failure (don't waste time on subsequent checks).
    """
    started_at = datetime.now(timezone.utc).isoformat()

    # ── Resolve plan metadata ─────────────────────────────────────────────
    meta_src = _plan_metadata(plan)
    gate_meta = meta_src.get('hybrid_client_quality_gate') or {}
    confirmation_required = gate_meta.get('confirmation_required', False)

    # ── LIVE final guard: applies even when old metadata did not require confirmation
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
        return _fail(guard_reason or "live_final_entry_guard_failed", {
            **guard_meta,
            "confirmation_required": True,
        })

    # ── Fast path: legacy confirmation gate not required ──────────────────
    if not confirmation_required:
        return ConfirmationResult(
            passed=True,
            fail_reason=None,
            metadata={
                **guard_meta,
                "confirmation_required": False,
            },
        )

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

    # ── 1. Quote age ──────────────────────────────────────────────────────
    # Use the stricter of confirm_s and CLIENT_PROOF_QUOTE_MAX_AGE_SECONDS
    effective_max_age = min(confirm_s, max_quote_age_s)
    if live_bid is None and live_ask is None:
        return _fail("entry_confirm_failed_stale_quote",
                     {**base, "reason": "no live quote available at submit time"})

    if quote_age_s > effective_max_age:
        return _fail("entry_confirm_failed_stale_quote",
                     {**base,
                      "quote_age_seconds": round(quote_age_s, 2),
                      "max_age_seconds": effective_max_age})

    # ── 2. Spread ─────────────────────────────────────────────────────────
    if spread_pct is not None and spread_pct > max_spread:
        return _fail("entry_confirm_failed_spread",
                     {**base,
                      "spread_pct": round(spread_pct, 4),
                      "max_spread_pct": max_spread})

    # ── 3. Option fade ────────────────────────────────────────────────────
    if decision_option_price and decision_option_price > 0 and live_mid is not None:
        fade_pct = (decision_option_price - live_mid) / decision_option_price * 100
        base["option_move_pct"] = round(fade_pct, 2)
        if fade_pct > max_fade:
            return _fail("entry_confirm_failed_option_fade",
                         {**base,
                          "fade_pct": round(fade_pct, 2),
                          "max_fade_pct": max_fade,
                          "decision_price": decision_option_price,
                          "live_mid": live_mid})

        # Paper quote lag warning: if sandbox and live quotes differ materially
        if sandbox_mode and fade_pct > 2.0:
            base["paper_quote_lag_warning"] = True

    # ── 4. Underlying reversal ────────────────────────────────────────────
    if underlying_last is not None and trigger_price is not None and trigger_price > 0:
        move_pct = (underlying_last - trigger_price) / trigger_price * 100
        base["underlying_move_pct"] = round(move_pct, 2)

        if dirn == "CALL":
            # Block if underlying has pulled back below trigger by > threshold
            reversal = -move_pct  # positive means pulled back
            if reversal > max_reversal:
                return _fail("entry_confirm_failed_underlying_reversal",
                             {**base,
                              "underlying_last": underlying_last,
                              "trigger_price": trigger_price,
                              "reversal_pct": round(reversal, 3),
                              "max_reversal_pct": max_reversal,
                              "direction": "CALL"})
        elif dirn == "PUT":
            # Block if underlying has reclaimed above trigger by > threshold
            reversal = move_pct   # positive means reclaimed above
            if reversal > max_reversal:
                return _fail("entry_confirm_failed_underlying_reversal",
                             {**base,
                              "underlying_last": underlying_last,
                              "trigger_price": trigger_price,
                              "reversal_pct": round(reversal, 3),
                              "max_reversal_pct": max_reversal,
                              "direction": "PUT"})

    # ── All checks passed ─────────────────────────────────────────────────
    return _pass(base)
