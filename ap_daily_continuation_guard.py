"""
Daily timeframe intraday continuation guard.

Purpose
-------
Daily/1d scanner signals can be valid at scan time, but still be bad live entries
if the real move happened near the open and price later faded back through the
trigger. This module is intentionally pure and side-effect free.

It does not submit orders, cancel orders, mutate positions, update proof_trades,
write trade_queue rows, or alter client_id / execution_mode. Callers should wire
this immediately before ENTRY submission and preserve the returned diagnostics
for downstream observability.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional, Sequence


DAILY_TIMEFRAMES = {"1d", "d", "daily", "day"}
CALL_SIDES = {"call", "calls", "c", "bull", "long"}
PUT_SIDES = {"put", "puts", "p", "bear", "short"}


@dataclass(frozen=True)
class ContinuationCandle:
    open: float
    high: float
    low: float
    close: float
    ts: Any = None


@dataclass(frozen=True)
class ContinuationDecision:
    allowed: bool
    reason: str
    diagnostics: Mapping[str, Any]


def _as_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        out = float(value)
    except (TypeError, ValueError):
        return None
    if out != out:
        return None
    return out


def _get(mapping_or_obj: Any, *names: str) -> Any:
    if mapping_or_obj is None:
        return None
    if isinstance(mapping_or_obj, Mapping):
        for name in names:
            if name in mapping_or_obj:
                return mapping_or_obj[name]
        return None
    for name in names:
        if hasattr(mapping_or_obj, name):
            return getattr(mapping_or_obj, name)
    return None


def _normalize_side(side: Any) -> Optional[str]:
    value = str(side or "").strip().lower()
    if value in CALL_SIDES:
        return "CALL"
    if value in PUT_SIDES:
        return "PUT"
    return None


def _is_daily_timeframe(timeframe: Any) -> bool:
    return str(timeframe or "").strip().lower() in DAILY_TIMEFRAMES


def _normalize_candle(raw: Any) -> Optional[ContinuationCandle]:
    if raw is None:
        return None
    open_ = _as_float(_get(raw, "open", "o"))
    high = _as_float(_get(raw, "high", "h"))
    low = _as_float(_get(raw, "low", "l"))
    close = _as_float(_get(raw, "close", "c", "last"))
    if open_ is None or high is None or low is None or close is None:
        return None
    return ContinuationCandle(
        open=open_,
        high=high,
        low=low,
        close=close,
        ts=_get(raw, "ts", "timestamp", "time", "datetime"),
    )


def _normalize_candles(candles: Optional[Sequence[Any]]) -> list[ContinuationCandle]:
    if not candles:
        return []
    normalized: list[ContinuationCandle] = []
    for candle in candles:
        parsed = _normalize_candle(candle)
        if parsed is not None:
            normalized.append(parsed)
    return normalized


def _pct_buffer(trigger: float, buffer_bps: float) -> float:
    return max(abs(trigger) * (buffer_bps / 10_000.0), 0.01)


def daily_signal_has_intraday_continuation(
    *,
    timeframe: Any,
    side: Any,
    trigger_price: Any,
    current_underlying_price: Any,
    intraday_candles: Optional[Sequence[Any]] = None,
    buffer_bps: float = 8.0,
    recent_candle_count: int = 3,
) -> ContinuationDecision:
    if not _is_daily_timeframe(timeframe):
        return ContinuationDecision(
            allowed=True,
            reason="not_daily_timeframe",
            diagnostics={"timeframe": timeframe},
        )

    normalized_side = _normalize_side(side)
    trigger = _as_float(trigger_price)
    current = _as_float(current_underlying_price)
    candles = _normalize_candles(intraday_candles)
    buffer = _pct_buffer(trigger, buffer_bps) if trigger is not None else None

    base_diag: dict[str, Any] = {
        "timeframe": timeframe,
        "side": normalized_side or side,
        "trigger_price": trigger,
        "current_underlying_price": current,
        "buffer_bps": buffer_bps,
        "buffer_abs": buffer,
        "candles_seen": len(candles),
        "recent_candle_count": recent_candle_count,
    }

    if normalized_side is None:
        return ContinuationDecision(False, "daily_continuation_missing_side", base_diag)
    if trigger is None or trigger <= 0:
        return ContinuationDecision(False, "daily_continuation_missing_trigger", base_diag)
    if current is None or current <= 0:
        return ContinuationDecision(False, "daily_continuation_missing_current_price", base_diag)
    if buffer is None:
        return ContinuationDecision(False, "daily_continuation_missing_buffer", base_diag)

    recent = candles[-max(1, int(recent_candle_count)) :]
    all_highs = [c.high for c in candles]
    all_lows = [c.low for c in candles]

    if normalized_side == "CALL":
        back_through_trigger = current < trigger
        still_above_with_buffer = current >= trigger + buffer
        reclaimed_after_retest = any(c.low <= trigger and c.close >= trigger + buffer for c in recent)
        fresh_intraday_high = bool(all_highs and current >= max(all_highs) - buffer and max(all_highs) >= trigger + buffer)
        recent_body_confirms = any(c.close >= trigger + buffer and c.close >= c.open for c in recent)

        diagnostics = {
            **base_diag,
            "back_through_trigger": back_through_trigger,
            "still_above_with_buffer": still_above_with_buffer,
            "reclaimed_after_retest": reclaimed_after_retest,
            "fresh_intraday_high": fresh_intraday_high,
            "recent_body_confirms": recent_body_confirms,
            "intraday_high": max(all_highs) if all_highs else None,
        }

        if back_through_trigger:
            return ContinuationDecision(False, "daily_call_back_below_trigger", diagnostics)
        if still_above_with_buffer or reclaimed_after_retest or fresh_intraday_high or recent_body_confirms:
            return ContinuationDecision(True, "daily_call_continuation_confirmed", diagnostics)
        return ContinuationDecision(False, "daily_call_no_intraday_continuation", diagnostics)

    back_through_trigger = current > trigger
    still_below_with_buffer = current <= trigger - buffer
    reclaimed_after_retest = any(c.high >= trigger and c.close <= trigger - buffer for c in recent)
    fresh_intraday_low = bool(all_lows and current <= min(all_lows) + buffer and min(all_lows) <= trigger - buffer)
    recent_body_confirms = any(c.close <= trigger - buffer and c.close <= c.open for c in recent)

    diagnostics = {
        **base_diag,
        "back_through_trigger": back_through_trigger,
        "still_below_with_buffer": still_below_with_buffer,
        "reclaimed_after_retest": reclaimed_after_retest,
        "fresh_intraday_low": fresh_intraday_low,
        "recent_body_confirms": recent_body_confirms,
        "intraday_low": min(all_lows) if all_lows else None,
    }

    if back_through_trigger:
        return ContinuationDecision(False, "daily_put_back_above_trigger", diagnostics)
    if still_below_with_buffer or reclaimed_after_retest or fresh_intraday_low or recent_body_confirms:
        return ContinuationDecision(True, "daily_put_continuation_confirmed", diagnostics)
    return ContinuationDecision(False, "daily_put_no_intraday_continuation", diagnostics)
