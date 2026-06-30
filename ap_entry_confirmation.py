"""
ap_entry_confirmation.py
========================
P0 pre-submit entry confirmation preflight.

Called from APExecutionCore._on_entry_trigger() after breach risk checks,
contract selection, and live ask refresh, but before submit_existing_entry().

Missing hybrid_client_quality_gate.confirmation_required now defaults to a
required final pre-submit confirmation via ENTRY_CONFIRMATION_REQUIRED_DEFAULT
and ENTRY_CONFIRMATION_REQUIRED_LIVE_DEFAULT. Explicit False still preserves the
legacy fast path for intentionally non-confirmed flows.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping, Optional, Sequence

log = logging.getLogger(__name__)


def _ef(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return default


def _env_enabled(key: str, default: bool = False) -> bool:
    raw = os.getenv(key)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off", "disabled"}


def _coerce_optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        raw = value.strip().lower()
        if not raw:
            return None
        if raw in {"1", "true", "yes", "on", "enabled"}:
            return True
        if raw in {"0", "false", "no", "off", "disabled"}:
            return False
    return bool(value)


def _daily_continuation_mode() -> str:
    raw_mode = (os.getenv("ENABLE_DAILY_CONTINUATION_MODE") or "").strip().lower()
    if raw_mode in {"off", "observe", "enforce"}:
        return raw_mode
    if os.getenv("ENABLE_DAILY_CONTINUATION_VALIDATION") is not None:
        return "observe" if _env_enabled("ENABLE_DAILY_CONTINUATION_VALIDATION", False) else "off"
    return "off"


def _tier_confirm_seconds(score: Optional[float], tier: Optional[str], timeframe: Optional[str]) -> float:
    base = _ef("ENTRY_CONFIRM_SECONDS", 45.0)
    tf = (timeframe or "1d").lower()
    s = float(score or 0)
    is_intraday = any(tf.startswith(x) for x in ("15", "30", "60"))

    if is_intraday:
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
            "confirmation_required": bool(
                self.metadata.get("confirmation_required")
                or self.metadata.get("hybrid_confirmation_required")
            ),
            "confirmation_seconds": self.metadata.get("confirmation_seconds"),
            "confirmation_started_at": started_at,
            "confirmation_completed_at": completed_at,
            "confirmation_passed": self.passed,
            "confirmation_fail_reason": self.fail_reason,
            **{
                k: v
                for k, v in self.metadata.items()
                if k not in ("confirmation_seconds", "confirmation_required")
            },
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
    elif isinstance(plan, Mapping):
        raw = plan.get("metadata") or {}
    else:
        raw = {}
    return dict(raw) if isinstance(raw, Mapping) else {}


def _plan_value(plan: Any, key: str) -> Any:
    if hasattr(plan, key):
        return getattr(plan, key)
    if isinstance(plan, Mapping):
        return plan.get(key)
    return None


def _execution_mode_from_plan(plan: Any, meta_src: Mapping[str, Any]) -> str:
    sizing = _as_mapping(meta_src.get("sizing_context"))
    raw = (
        _plan_value(plan, "mode")
        or _plan_value(plan, "execution_mode")
        or meta_src.get("execution_mode")
        or meta_src.get("mode")
        or sizing.get("execution_mode")
        or sizing.get("mode")
        or ""
    )
    return str(raw or "").upper().strip()


def _resolve_confirmation_required(*, gate_meta: Mapping[str, Any], execution_mode: str) -> tuple[bool, str]:
    explicit_required = _coerce_optional_bool(gate_meta.get("confirmation_required"))
    if explicit_required is not None:
        return explicit_required, "hybrid_client_quality_gate"

    if execution_mode == "LIVE":
        if os.getenv("ENTRY_CONFIRMATION_REQUIRED_LIVE_DEFAULT") is not None:
            return _env_enabled("ENTRY_CONFIRMATION_REQUIRED_LIVE_DEFAULT", True), "live_env_default"
        return _env_enabled("ENTRY_CONFIRMATION_REQUIRED_DEFAULT", True), "live_global_default"

    return _env_enabled("ENTRY_CONFIRMATION_REQUIRED_DEFAULT", True), "global_default"


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


def _first_breach_index(candles: Sequence[Any], *, direction: str, trigger_price: float) -> int | None:
    for idx, candle in enumerate(candles):
        high = _candle_high(candle)
        low = _candle_low(candle)
        if direction == "CALL" and high is not None and high >= trigger_price:
            return idx
        if direction == "PUT" and low is not None and low <= trigger_price:
            return idx
    return None


def _fresh_extension_after_breach(candles: Sequence[Any], *, direction: str, breach_index: int) -> bool:
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


def _has_continuation_candle(candles: Sequence[Any], *, direction: str, trigger_price: float, buffer: float) -> bool:
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


def _daily_fail(
    diagnostics: dict,
    reason: str,
    *,
    mode: str,
    passed: bool | None,
    legacy_reason: str | None = None,
) -> tuple[bool | None, str, bool, dict]:
    diagnostics["daily_continuation_passed"] = passed
    diagnostics["daily_continuation_fail_reason"] = reason
    diagnostics["daily_continuation_reason_code"] = reason
    diagnostics["daily_continuation_would_block"] = True
    if legacy_reason:
        diagnostics["daily_continuation_legacy_fail_reason"] = legacy_reason
    return passed, reason, mode == "enforce", diagnostics


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
        "daily_continuation_reason_code": None,
        "daily_continuation_would_block": False,
    }
    if not _is_daily_timeframe(timeframe):
        return True, None, False, diagnostics
    if mode == "off":
        return True, None, False, diagnostics

    candles = _extract_intraday_candles(meta_src)
    trigger = _as_float(trigger_price)
    current = _as_float(underlying_last)
    if not candles:
        return _daily_fail(
            diagnostics,
            "DAILY_CONTINUATION_MISSING_INTRADAY_CANDLES",
            mode=mode,
            passed=None,
            legacy_reason="daily_continuation_failed:missing_intraday_context",
        )
    if trigger is None or current is None:
        return _daily_fail(
            diagnostics,
            "DAILY_CONTINUATION_MISSING_TRIGGER_OR_UNDERLYING",
            mode=mode,
            passed=None,
            legacy_reason="daily_continuation_failed:missing_intraday_context",
        )

    candles = list(candles)
    breach_index = _first_breach_index(candles, direction=direction, trigger_price=trigger)
    if breach_index is None:
        return _daily_fail(
            diagnostics,
            "DAILY_CONTINUATION_NO_BREACH_CANDLE",
            mode=mode,
            passed=False,
            legacy_reason="daily_continuation_failed:missing_intraday_context",
        )

    buffer_pct = _as_float(os.getenv("DAILY_CONTINUATION_BUFFER_PCT", "0.001"))
    if buffer_pct is None or buffer_pct < 0:
        buffer_pct = 0.001
    buffer = trigger * buffer_pct
    fresh_extension = _fresh_extension_after_breach(candles, direction=direction, breach_index=breach_index)
    continuation_candle = _has_continuation_candle(
        candles,
        direction=direction,
        trigger_price=trigger,
        buffer=buffer,
    )
    price_back_through = current < trigger if direction == "CALL" else current > trigger

    diagnostics["daily_continuation_fresh_extension"] = fresh_extension
    diagnostics["daily_continuation_continuation_candle"] = continuation_candle
    diagnostics["daily_continuation_price_back_through_trigger"] = price_back_through

    if price_back_through:
        reason = (
            "DAILY_CONTINUATION_TRIGGER_TOUCH_ONLY"
            if not fresh_extension and not continuation_candle
            else "DAILY_CONTINUATION_PRICE_BACK_THROUGH_TRIGGER"
        )
        legacy_reason = (
            "daily_continuation_failed:trigger_touch_only"
            if reason == "DAILY_CONTINUATION_TRIGGER_TOUCH_ONLY"
            else "daily_continuation_failed:price_back_through_trigger"
        )
        return _daily_fail(diagnostics, reason, mode=mode, passed=False, legacy_reason=legacy_reason)

    if fresh_extension or continuation_candle:
        diagnostics["daily_continuation_passed"] = True
        return True, None, False, diagnostics

    return _daily_fail(
        diagnostics,
        "DAILY_CONTINUATION_OPENING_MOVE_EXHAUSTED",
        mode=mode,
        passed=False,
        legacy_reason="daily_continuation_failed:opening_move_exhausted",
    )


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
    meta_src = _extract_plan_metadata(plan)
    gate_meta = _as_mapping(meta_src.get("hybrid_client_quality_gate"))
    execution_mode = _execution_mode_from_plan(plan, meta_src)
    confirmation_required, confirmation_required_source = _resolve_confirmation_required(
        gate_meta=gate_meta,
        execution_mode=execution_mode,
    )

    started_at = datetime.now(timezone.utc).isoformat()
    max_fade = _ef("MAX_PRE_ENTRY_OPTION_FADE_PCT", 8.0)
    max_reversal = _ef("MAX_PRE_ENTRY_UNDERLYING_REVERSAL_PCT", 0.25)
    max_spread = _ef("CLIENT_PROOF_MAX_SPREAD_PCT", 0.10)
    max_quote_age_s = _ef("CLIENT_PROOF_QUOTE_MAX_AGE_SECONDS", 10.0)
    confirm_s = _tier_confirm_seconds(score, tier, timeframe)
    dirn = str(direction or "").upper().strip()

    bid_f = _as_float(live_bid)
    ask_f = _as_float(live_ask)
    quote_age_ms_f = _as_float(live_quote_age_ms)
    quote_age_s = quote_age_ms_f / 1000.0 if quote_age_ms_f is not None and quote_age_ms_f >= 0 else None

    live_mid = None
    spread_pct = None
    if bid_f is not None and ask_f is not None and bid_f > 0 and ask_f > 0 and ask_f >= bid_f:
        live_mid = (bid_f + ask_f) / 2
        spread_pct = (ask_f - bid_f) / live_mid if live_mid > 0 else None

    base = {
        "confirmation_required": bool(confirmation_required),
        "hybrid_confirmation_required": bool(confirmation_required),
        "confirmation_required_source": confirmation_required_source,
        "confirmation_seconds": confirm_s,
        "execution_mode": execution_mode,
        "direction": dirn,
        "underlying_start": trigger_price,
        "underlying_end": underlying_last,
        "option_mid_start": decision_option_price,
        "option_mid_end": live_mid,
        "option_move_pct": None,
        "underlying_move_pct": None,
        "quote_age_seconds": round(quote_age_s, 2) if quote_age_s is not None else None,
        "spread_pct": round(spread_pct, 4) if spread_pct is not None else None,
        "live_entry_bid": live_bid,
        "live_entry_ask": live_ask,
        "live_entry_mid": live_mid,
        "live_entry_ts": started_at,
        "paper_quote_lag_warning": False,
        "sandbox_mode": sandbox_mode,
    }

    if dirn not in {"CALL", "PUT"}:
        return _fail("entry_confirm_failed_invalid_direction", {**base, "raw_direction": direction})

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

    if not confirmation_required:
        return ConfirmationResult(passed=True, fail_reason=None, metadata=base)

    effective_max_age = min(confirm_s, max_quote_age_s)

    if quote_age_ms_f is None or quote_age_ms_f < 0:
        return _fail(
            "entry_confirm_failed_missing_quote_age",
            {**base, "reason": "missing or invalid live_quote_age_ms at submit time"},
        )

    if live_bid is None or live_ask is None or bid_f is None or ask_f is None:
        return _fail(
            "entry_confirm_failed_quote_missing_bid_ask",
            {**base, "reason": "missing bid or ask at submit time"},
        )

    if bid_f <= 0 or ask_f <= 0:
        return _fail(
            "entry_confirm_failed_zero_bid_ask",
            {**base, "reason": "zero bid or ask at submit time"},
        )

    if ask_f < bid_f:
        return _fail(
            "entry_confirm_failed_inverted_quote",
            {**base, "reason": "ask below bid at submit time"},
        )

    if quote_age_s is not None and quote_age_s > effective_max_age:
        return _fail(
            "entry_confirm_failed_stale_quote",
            {**base, "quote_age_seconds": round(quote_age_s, 2), "max_age_seconds": effective_max_age},
        )

    if spread_pct is not None and spread_pct > max_spread:
        return _fail(
            "entry_confirm_failed_spread",
            {**base, "spread_pct": round(spread_pct, 4), "max_spread_pct": max_spread},
        )

    if decision_option_price and decision_option_price > 0 and live_mid is not None:
        fade_pct = (decision_option_price - live_mid) / decision_option_price * 100
        base["option_move_pct"] = round(fade_pct, 2)
        if fade_pct > max_fade:
            return _fail(
                "entry_confirm_failed_option_fade",
                {
                    **base,
                    "fade_pct": round(fade_pct, 2),
                    "max_fade_pct": max_fade,
                    "decision_price": decision_option_price,
                    "live_mid": live_mid,
                },
            )
        if sandbox_mode and fade_pct > 2.0:
            base["paper_quote_lag_warning"] = True

    if underlying_last is not None and trigger_price is not None and trigger_price > 0:
        move_pct = (underlying_last - trigger_price) / trigger_price * 100
        base["underlying_move_pct"] = round(move_pct, 2)

        if dirn == "CALL":
            reversal = -move_pct
            if reversal > max_reversal:
                return _fail(
                    "entry_confirm_failed_underlying_reversal",
                    {
                        **base,
                        "underlying_last": underlying_last,
                        "trigger_price": trigger_price,
                        "reversal_pct": round(reversal, 3),
                        "max_reversal_pct": max_reversal,
                        "direction": "CALL",
                    },
                )
        elif dirn == "PUT":
            reversal = move_pct
            if reversal > max_reversal:
                return _fail(
                    "entry_confirm_failed_underlying_reversal",
                    {
                        **base,
                        "underlying_last": underlying_last,
                        "trigger_price": trigger_price,
                        "reversal_pct": round(reversal, 3),
                        "max_reversal_pct": max_reversal,
                        "direction": "PUT",
                    },
                )

    return _pass(base)
