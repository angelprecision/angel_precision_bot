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
LIVE remains default-required from runner-owned execution context. PAPER
keeps the legacy no-op unless confirmation is explicitly requested. Daily
continuation still follows ENABLE_DAILY_CONTINUATION_MODE and remains
diagnostic-only in observe mode.

ENV FLAGS (hot-read per call)
------------------------------
ENTRY_CONFIRM_SECONDS            = 45   (max quote age treated as fresh)
MAX_PRE_ENTRY_OPTION_FADE_PCT    = 8    (block if option faded > 8%)
MAX_PRE_ENTRY_UNDERLYING_REVERSAL_PCT = 0.25  (block if price moved > 0.25% wrong way)
CLIENT_PROOF_MAX_SPREAD_PCT      = 0.10 (block if spread > 10%)
CLIENT_PROOF_QUOTE_MAX_AGE_SECONDS = 10 (block if quote older than 10s)
"""

from __future__ import annotations

import os
import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping, Optional, Sequence

log = logging.getLogger(__name__)


# ── Config ────────────────────────────────────────────────────────────────────

def _ef(key: str, default: float) -> float:
    try: return float(os.getenv(key, str(default)))
    except: return default


def _env_enabled(key: str, default: bool = False) -> bool:
    raw = os.getenv(key)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off", "disabled"}


_TRUE_CONFIRMATION_VALUES = frozenset({"1", "true", "yes", "on"})
_FALSE_CONFIRMATION_VALUES = frozenset({"0", "false", "no", "off"})


def parse_confirmation_bool(value: Any) -> bool | None:
    """Strict bool parser shared by confirmation guards.

    ``None`` means malformed or absent. Normal Python string truthiness is
    deliberately forbidden at this broker-safety boundary.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(numeric):
            return None
        if numeric == 1.0:
            return True
        if numeric == 0.0:
            return False
        return None
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in _TRUE_CONFIRMATION_VALUES:
            return True
        if normalized in _FALSE_CONFIRMATION_VALUES:
            return False
    return None


def _daily_continuation_mode() -> str:
    raw_mode = (os.getenv("ENABLE_DAILY_CONTINUATION_MODE") or "").strip().lower()
    if raw_mode in {"off", "observe", "enforce"}:
        return raw_mode
    if os.getenv("ENABLE_DAILY_CONTINUATION_VALIDATION") is not None:
        return "observe" if _env_enabled("ENABLE_DAILY_CONTINUATION_VALIDATION", False) else "off"
    return "off"

def _tier_confirm_seconds(score: Optional[float], tier: Optional[str],
                          timeframe: Optional[str]) -> float:
    """
    Tier-based confirmation window (used as max quote age).
    Faster for high-conviction setups — we want those trades to go through.
    """
    base = _ef("ENTRY_CONFIRM_SECONDS", 45.0)
    tf   = (timeframe or "1d").lower()
    s    = float(score or 0)
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

@dataclass(frozen=True)
class ConfirmationRequirement:
    required: bool
    source: str
    execution_mode: str
    top_level_present: bool
    nested_present: bool
    top_level_value: bool | None
    nested_value: bool | None
    conflict: bool
    malformed: bool
    live_default_required: bool


def resolve_entry_confirmation_requirement(
    plan: Any,
    *,
    execution_mode: str = "",
    live_default_required: bool | None = None,
) -> ConfirmationRequirement:
    """Resolve confirmation policy without mutation or external I/O."""
    meta = _extract_plan_metadata(plan)
    top_present = "confirmation_required" in meta
    top_value = parse_confirmation_bool(meta.get("confirmation_required")) if top_present else None

    nested_raw = meta.get("hybrid_client_quality_gate")
    nested = nested_raw if isinstance(nested_raw, Mapping) else {}
    nested_present = "confirmation_required" in nested
    nested_value = (
        parse_confirmation_bool(nested.get("confirmation_required"))
        if nested_present else None
    )

    malformed = bool(
        (top_present and top_value is None)
        or (nested_present and nested_value is None)
        or (nested_raw is not None and not isinstance(nested_raw, Mapping))
    )
    conflict = bool(
        top_value is not None
        and nested_value is not None
        and top_value != nested_value
    )
    mode = str(execution_mode or "").strip().lower()
    live = mode == "live"
    parsed_default = parse_confirmation_bool(live_default_required)
    live_default = (
        True
        if live_default_required is None or parsed_default is None
        else parsed_default
    )

    explicit_true = top_value is True or nested_value is True
    if conflict:
        required = True
        source = "metadata_conflict_fail_closed"
    elif live:
        required = bool(live_default or explicit_true)
        if live_default and explicit_true:
            source = "live_default_plus_explicit"
        elif live_default:
            source = "live_default_required"
        elif explicit_true:
            source = (
                "top_level_metadata"
                if top_value is True
                else "legacy_nested_metadata"
            )
        elif malformed:
            source = "live_break_glass_default_disabled"
        else:
            source = "live_break_glass_default_disabled"
    elif top_value is not None:
        required = top_value
        source = "top_level_metadata"
    elif nested_value is not None:
        required = nested_value
        source = "legacy_nested_metadata"
    elif malformed:
        required = False
        source = "malformed_metadata_ignored"
    else:
        required = False
        source = "not_requested"

    return ConfirmationRequirement(
        required=bool(required),
        source=source,
        execution_mode=mode,
        top_level_present=top_present,
        nested_present=nested_present,
        top_level_value=top_value,
        nested_value=nested_value,
        conflict=conflict,
        malformed=malformed,
        live_default_required=bool(live_default),
    )

@dataclass
class ConfirmationResult:
    passed: bool
    fail_reason: Optional[str]
    metadata: dict = field(default_factory=dict)

    def to_meta(self, started_at: str, completed_at: str) -> dict:
        return {
            "confirmation_required":    bool(
                self.metadata.get("confirmation_required")
                or self.metadata.get("hybrid_confirmation_required")
            ),
            "confirmation_seconds":     self.metadata.get("confirmation_seconds"),
            "confirmation_started_at":  started_at,
            "confirmation_completed_at":completed_at,
            "confirmation_passed":      self.passed,
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


_DAILY_TIMEFRAMES = {"1d", "d", "day", "daily", "overnight"}


def _is_daily_timeframe(timeframe: Optional[str]) -> bool:
    return str(timeframe or "").strip().lower() in _DAILY_TIMEFRAMES


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _extract_plan_metadata(plan: Any) -> dict:
    if hasattr(plan, "metadata"):
        raw = getattr(plan, "metadata") or {}
    elif isinstance(plan, dict):
        raw = plan.get("metadata") or {}
    else:
        raw = {}
    return dict(raw) if isinstance(raw, Mapping) else {}


def _first_sequence(*values: Any) -> Sequence[Any] | None:
    for value in values:
        if value is None or isinstance(value, (str, bytes, Mapping)):
            continue
        if isinstance(value, Sequence):
            return value
    return None


def _extract_intraday_candles(meta_src: Mapping[str, Any]) -> Sequence[Any] | None:
    intraday_context = _as_mapping(
        meta_src.get("intraday_context")
        or meta_src.get("daily_continuation_context")
        or meta_src.get("continuation_context")
    )
    return _first_sequence(
        meta_src.get("intraday_candles"),
        meta_src.get("continuation_candles"),
        meta_src.get("daily_continuation_candles"),
        intraday_context.get("candles"),
        intraday_context.get("intraday_candles"),
        intraday_context.get("bars"),
    )


def _as_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _finite_float(value: Any) -> float | None:
    parsed = _as_float(value)
    return parsed if parsed is not None and math.isfinite(parsed) else None


def _is_missing(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def _candle_open(candle: Any) -> float | None:
    if isinstance(candle, Mapping):
        return _as_float(candle.get("open", candle.get("o")))
    return _as_float(getattr(candle, "open", getattr(candle, "o", None)))


def _candle_high(candle: Any) -> float | None:
    if isinstance(candle, Mapping):
        return _as_float(candle.get("high", candle.get("h")))
    return _as_float(getattr(candle, "high", getattr(candle, "h", None)))


def _candle_low(candle: Any) -> float | None:
    if isinstance(candle, Mapping):
        return _as_float(candle.get("low", candle.get("l")))
    return _as_float(getattr(candle, "low", getattr(candle, "l", None)))


def _candle_close(candle: Any) -> float | None:
    if isinstance(candle, Mapping):
        return _as_float(candle.get("close", candle.get("c")))
    return _as_float(getattr(candle, "close", getattr(candle, "c", None)))


def _first_breach_index(
    candles: Sequence[Any],
    *,
    direction: str,
    trigger_price: float,
) -> int | None:
    for idx, candle in enumerate(candles):
        high = _candle_high(candle)
        low = _candle_low(candle)
        if direction == "CALL" and high is not None and high >= trigger_price:
            return idx
        if direction == "PUT" and low is not None and low <= trigger_price:
            return idx
    return None


def _fresh_extension_after_breach(
    candles: Sequence[Any],
    *,
    direction: str,
    breach_index: int,
) -> bool:
    if breach_index >= len(candles) - 1:
        return False
    breach_candle = candles[breach_index]
    if direction == "CALL":
        breach_high = _candle_high(breach_candle)
        highs_after = [_candle_high(c) for c in candles[breach_index + 1:]]
        highs_after = [v for v in highs_after if v is not None]
        return bool(breach_high is not None and highs_after and max(highs_after) > breach_high)
    breach_low = _candle_low(breach_candle)
    lows_after = [_candle_low(c) for c in candles[breach_index + 1:]]
    lows_after = [v for v in lows_after if v is not None]
    return bool(breach_low is not None and lows_after and min(lows_after) < breach_low)


def _has_continuation_candle(
    candles: Sequence[Any],
    *,
    direction: str,
    trigger_price: float,
    buffer: float,
) -> bool:
    for candle in list(candles[-3:]):
        open_ = _candle_open(candle)
        close = _candle_close(candle)
        if open_ is None or close is None:
            continue
        if direction == "CALL" and close > trigger_price + buffer and close >= open_:
            return True
        if direction == "PUT" and close < trigger_price - buffer and close <= open_:
            return True
    return False


def _evaluate_daily_continuation(
    *,
    meta_src: Mapping[str, Any],
    direction: str,
    trigger_price: Optional[float],
    underlying_last: Optional[float],
    timeframe: Optional[str],
) -> tuple[bool | None, str | None, bool, dict]:
    mode = _daily_continuation_mode()
    diagnostics = {
        "daily_continuation_mode": mode,
        "daily_continuation_passed": None,
        "daily_continuation_fail_reason": None,
        "daily_continuation_would_block": False,
    }
    if not _is_daily_timeframe(timeframe):
        return True, None, False, diagnostics
    if mode == "off":
        return True, None, False, diagnostics

    candles = _extract_intraday_candles(meta_src)
    trigger = _as_float(trigger_price)
    current = _as_float(underlying_last)
    if not candles or trigger is None or current is None:
        reason = "daily_continuation_failed:missing_intraday_context"
        diagnostics["daily_continuation_fail_reason"] = reason
        diagnostics["daily_continuation_would_block"] = True
        return None, reason, mode == "enforce", diagnostics

    candles = list(candles)
    breach_index = _first_breach_index(candles, direction=direction, trigger_price=trigger)
    if breach_index is None:
        reason = "daily_continuation_failed:missing_intraday_context"
        diagnostics["daily_continuation_fail_reason"] = reason
        diagnostics["daily_continuation_would_block"] = True
        return None, reason, mode == "enforce", diagnostics

    buffer_pct = _as_float(os.getenv("DAILY_CONTINUATION_BUFFER_PCT", "0.001"))
    if buffer_pct is None or buffer_pct < 0:
        buffer_pct = 0.001
    buffer = trigger * buffer_pct
    fresh_extension = _fresh_extension_after_breach(
        candles,
        direction=direction,
        breach_index=breach_index,
    )
    continuation_candle = _has_continuation_candle(
        candles,
        direction=direction,
        trigger_price=trigger,
        buffer=buffer,
    )

    if direction == "CALL":
        price_back_through = current < trigger
    else:
        price_back_through = current > trigger

    diagnostics["daily_continuation_fresh_extension"] = fresh_extension
    diagnostics["daily_continuation_continuation_candle"] = continuation_candle
    diagnostics["daily_continuation_price_back_through_trigger"] = price_back_through

    if price_back_through:
        reason = (
            "daily_continuation_failed:trigger_touch_only"
            if not fresh_extension and not continuation_candle
            else "daily_continuation_failed:price_back_through_trigger"
        )
        diagnostics["daily_continuation_passed"] = False
        diagnostics["daily_continuation_fail_reason"] = reason
        diagnostics["daily_continuation_would_block"] = True
        return False, reason, mode == "enforce", diagnostics

    if fresh_extension or continuation_candle:
        diagnostics["daily_continuation_passed"] = True
        return True, None, False, diagnostics

    reason = "daily_continuation_failed:opening_move_exhausted"
    diagnostics["daily_continuation_passed"] = False
    diagnostics["daily_continuation_fail_reason"] = reason
    diagnostics["daily_continuation_would_block"] = True
    return False, reason, mode == "enforce", diagnostics


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
    underlying_quote_age_ms: Optional[float] = None,  # ms since underlying quote was fetched
    score: Optional[float] = None,
    tier: Optional[str]   = None,
    timeframe: Optional[str] = None,
    sandbox_mode: bool = False,
    execution_mode: str = "",
    live_default_required: bool | None = None,
) -> ConfirmationResult:
    """
    Pre-submit confirmation preflight.

    Checks (in order, fail-fast):
    1. canonical confirmation requirement
    2. Quote freshness
    3. Spread acceptability
    4. Option fade from decision price
    5. Underlying reversal from trigger

    Returns immediately on first failure (don't waste time on subsequent checks).
    """
    # ── Resolve plan metadata ─────────────────────────────────────────────
    meta_src = _extract_plan_metadata(plan)

    if live_default_required is None:
        parsed_live_default = parse_confirmation_bool(
            os.getenv("LIVE_CONFIRMATION_REQUIRED", "1")
        )
        live_default_required = (
            True if parsed_live_default is None else parsed_live_default
        )
    requirement = resolve_entry_confirmation_requirement(
        plan,
        execution_mode=execution_mode,
        live_default_required=live_default_required,
    )
    confirmation_required = requirement.required

    started_at   = datetime.now(timezone.utc).isoformat()
    max_fade     = _ef("MAX_PRE_ENTRY_OPTION_FADE_PCT",       8.0)
    max_reversal = _ef("MAX_PRE_ENTRY_UNDERLYING_REVERSAL_PCT", 0.25)
    max_spread   = _ef("CLIENT_PROOF_MAX_SPREAD_PCT",         0.10)
    max_quote_age_s = _ef("CLIENT_PROOF_QUOTE_MAX_AGE_SECONDS", 10.0)
    confirm_s    = _tier_confirm_seconds(score, tier, timeframe)
    dirn         = str(direction or "").strip().upper()

    bid = _finite_float(live_bid)
    ask = _finite_float(live_ask)
    live_mid = (bid + ask) / 2 if bid is not None and ask is not None else None
    spread_pct = (
        (ask - bid) / live_mid
        if live_mid is not None and live_mid > 0 else None
    )
    quote_age_ms = _finite_float(live_quote_age_ms)
    quote_age_s = quote_age_ms / 1000.0 if quote_age_ms is not None else None
    underlying_quote_age = _finite_float(underlying_quote_age_ms)
    underlying_quote_age_s = (
        underlying_quote_age / 1000.0
        if underlying_quote_age is not None else None
    )
    effective_max_age = min(confirm_s, max_quote_age_s)

    base = {
        "confirmation_required":      bool(confirmation_required),
        "hybrid_confirmation_required": bool(confirmation_required),
        "confirmation_requirement_source": requirement.source,
        "confirmation_requirement_conflict": requirement.conflict,
        "confirmation_requirement_malformed": requirement.malformed,
        "confirmation_execution_mode": requirement.execution_mode,
        "confirmation_top_level_present": requirement.top_level_present,
        "confirmation_nested_present": requirement.nested_present,
        "confirmation_top_level_value": requirement.top_level_value,
        "confirmation_nested_value": requirement.nested_value,
        "live_confirmation_default_required": requirement.live_default_required,
        "direction":                   dirn or None,
        "confirmation_seconds":       confirm_s,
        "underlying_start":           trigger_price,
        "underlying_end":             underlying_last,
        "option_mid_start":           decision_option_price,
        "option_mid_end":             live_mid,
        "option_move_pct":            None,
        "underlying_move_pct":        None,
        "quote_age_ms":               live_quote_age_ms,
        "quote_age_seconds":          round(quote_age_s, 2) if quote_age_s is not None else None,
        "max_age_seconds":            effective_max_age,
        "underlying_quote_age_ms":    underlying_quote_age_ms,
        "underlying_quote_age_seconds": (
            round(underlying_quote_age_s, 2)
            if underlying_quote_age_s is not None else None
        ),
        "underlying_quote_max_age_seconds": effective_max_age,
        "underlying_quote_age_status": None,
        "spread_pct":                 round(spread_pct, 4) if spread_pct is not None else None,
        "live_entry_bid":             live_bid,
        "live_entry_ask":             live_ask,
        "live_entry_mid":             live_mid,
        "live_entry_ts":              started_at,
        "paper_quote_lag_warning":    False,
        "sandbox_mode":               sandbox_mode,
    }

    daily_passed, daily_reason, daily_should_block, daily_meta = _evaluate_daily_continuation(
        meta_src=meta_src,
        direction=dirn,
        trigger_price=trigger_price,
        underlying_last=underlying_last,
        timeframe=timeframe,
    )
    base.update(daily_meta)
    if daily_should_block:
        return _fail(daily_reason or "daily_continuation_failed", base)

    # ── Fast path: gate not required ──────────────────────────────────────
    if not confirmation_required:
        return ConfirmationResult(
            passed=True,
            fail_reason=None,
            metadata=base,
        )

    # ── 1. Required direction and synchronized quote ─────────────────────
    if dirn not in {"CALL", "PUT"}:
        return _fail(
            "entry_confirm_failed_invalid_direction",
            {**base, "direction": dirn or None},
        )

    if _is_missing(live_bid) or _is_missing(live_ask):
        return _fail(
            "entry_confirm_failed_missing_quote",
            {**base, "quote_evidence_status": "missing_or_one_sided"},
        )
    if bid is None or ask is None or bid <= 0 or ask <= 0:
        return _fail(
            "entry_confirm_failed_invalid_quote",
            {**base, "quote_evidence_status": "invalid_nonfinite_or_nonpositive"},
        )
    if ask < bid:
        return _fail(
            "entry_confirm_failed_crossed_quote",
            {**base, "quote_evidence_status": "crossed"},
        )

    # ── 2. Quote age ──────────────────────────────────────────────────────
    # Use the stricter of confirm_s and CLIENT_PROOF_QUOTE_MAX_AGE_SECONDS
    if _is_missing(live_quote_age_ms):
        return _fail(
            "entry_confirm_failed_missing_quote_age",
            {**base, "quote_age_status": "missing", "max_age_seconds": effective_max_age},
        )
    if quote_age_s is None or quote_age_s < 0:
        return _fail(
            "entry_confirm_failed_invalid_quote_age",
            {**base, "quote_age_status": "invalid", "max_age_seconds": effective_max_age},
        )
    if quote_age_s > effective_max_age:
        return _fail("entry_confirm_failed_stale_quote",
                     {**base,
                      "quote_age_seconds": round(quote_age_s, 2),
                      "quote_age_status":  "stale",
                      "max_age_seconds":   effective_max_age})

    # ── 3. Spread ─────────────────────────────────────────────────────────
    if spread_pct is not None and spread_pct > max_spread:
        return _fail("entry_confirm_failed_spread",
                     {**base,
                      "spread_pct":     round(spread_pct, 4),
                      "max_spread_pct": max_spread})

    # ── 4. Option fade ────────────────────────────────────────────────────
    decision_price = _finite_float(decision_option_price)
    require_complete_live_evidence = requirement.execution_mode == "live"
    if require_complete_live_evidence and _is_missing(decision_option_price):
        return _fail(
            "entry_confirm_failed_missing_decision_price",
            {**base, "decision_price_status": "missing"},
        )
    if require_complete_live_evidence and (
        decision_price is None or decision_price <= 0
    ):
        return _fail(
            "entry_confirm_failed_invalid_decision_price",
            {**base, "decision_price_status": "invalid"},
        )
    if decision_price is not None and decision_price > 0 and live_mid is not None:
        fade_pct = (decision_price - live_mid) / decision_price * 100
        base["option_move_pct"] = round(fade_pct, 2)
        if fade_pct > max_fade:
            return _fail("entry_confirm_failed_option_fade",
                         {**base,
                          "fade_pct":     round(fade_pct, 2),
                          "max_fade_pct": max_fade,
                          "decision_price": decision_price,
                          "live_mid":       live_mid})

        # Paper quote lag warning: if sandbox and live quotes differ materially
        if sandbox_mode and fade_pct > 2.0:
            base["paper_quote_lag_warning"] = True

    # ── 5. Underlying reversal ────────────────────────────────────────────
    trigger = _finite_float(trigger_price)
    current_underlying = _finite_float(underlying_last)
    if require_complete_live_evidence and (
        _is_missing(trigger_price) or trigger is None or trigger <= 0
    ):
        return _fail(
            "entry_confirm_failed_missing_trigger"
            if _is_missing(trigger_price)
            else "entry_confirm_failed_invalid_trigger",
            {**base, "trigger_status": "missing" if _is_missing(trigger_price) else "invalid"},
        )
    if require_complete_live_evidence and (
        _is_missing(underlying_last)
        or current_underlying is None
        or current_underlying <= 0
    ):
        return _fail(
            "entry_confirm_failed_missing_underlying"
            if _is_missing(underlying_last)
            else "entry_confirm_failed_invalid_underlying",
            {**base, "underlying_status": "missing" if _is_missing(underlying_last) else "invalid"},
        )
    if require_complete_live_evidence and _is_missing(underlying_quote_age_ms):
        return _fail(
            "entry_confirm_failed_missing_underlying_quote_age",
            {**base, "underlying_quote_age_status": "missing"},
        )
    if require_complete_live_evidence and (
        underlying_quote_age_s is None or underlying_quote_age_s < 0
    ):
        return _fail(
            "entry_confirm_failed_invalid_underlying_quote_age",
            {**base, "underlying_quote_age_status": "invalid"},
        )
    if require_complete_live_evidence and underlying_quote_age_s > effective_max_age:
        return _fail(
            "entry_confirm_failed_stale_underlying_quote",
            {
                **base,
                "underlying_quote_age_seconds": round(underlying_quote_age_s, 2),
                "underlying_quote_age_status": "stale",
            },
        )
    if (
        current_underlying is not None
        and current_underlying > 0
        and trigger is not None
        and trigger > 0
    ):
        move_pct = (current_underlying - trigger) / trigger * 100
        base["underlying_move_pct"] = round(move_pct, 2)

        if dirn == "CALL":
            # Block if underlying has pulled back below trigger by > threshold
            reversal = -move_pct  # positive means pulled back
            if reversal > max_reversal:
                return _fail("entry_confirm_failed_underlying_reversal",
                             {**base,
                              "underlying_last":  current_underlying,
                              "trigger_price":    trigger,
                              "reversal_pct":     round(reversal, 3),
                              "max_reversal_pct": max_reversal,
                              "direction": "CALL"})
        elif dirn == "PUT":
            # Block if underlying has reclaimed above trigger by > threshold
            reversal = move_pct   # positive means reclaimed above
            if reversal > max_reversal:
                return _fail("entry_confirm_failed_underlying_reversal",
                             {**base,
                              "underlying_last":  current_underlying,
                              "trigger_price":    trigger,
                              "reversal_pct":     round(reversal, 3),
                              "max_reversal_pct": max_reversal,
                              "direction": "PUT"})

    # ── All checks passed ─────────────────────────────────────────────────
    return _pass(base)
