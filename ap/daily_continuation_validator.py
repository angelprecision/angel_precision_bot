from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, time
from typing import Any, Mapping, Sequence


REASON_ALLOWED = "daily_continuation_allowed"
REASON_DISABLED = "daily_continuation_skipped:disabled"
REASON_NOT_DAILY = "daily_continuation_skipped:not_daily"
REASON_MISSING_CONTEXT = "daily_continuation_failed:missing_intraday_context"
REASON_TOUCH_ONLY = "daily_continuation_failed:trigger_touch_only"
REASON_NO_RECLAIM = "daily_continuation_failed:no_reclaim_after_open_move"
REASON_NO_EXTENSION = "daily_continuation_failed:no_fresh_intraday_extension"
REASON_BACK_THROUGH = "daily_continuation_failed:price_back_through_trigger"
REASON_LATE_NO_FOLLOW = "daily_continuation_failed:late_day_no_followthrough"
REASON_PREMIUM_WEAK = "daily_continuation_failed:premium_not_confirming"


@dataclass(frozen=True)
class ContinuationDecision:
    allowed: bool
    reason: str
    diagnostics: dict[str, Any]


def _env_enabled(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off", "disabled"}


def _as_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _get(obj: Any, *keys: str, default: Any = None) -> Any:
    for key in keys:
        if isinstance(obj, Mapping) and key in obj:
            return obj.get(key)
        if hasattr(obj, key):
            return getattr(obj, key)
    return default


def _candle_open(candle: Any) -> float | None:
    return _as_float(_get(candle, "open", "o"))


def _candle_high(candle: Any) -> float | None:
    return _as_float(_get(candle, "high", "h"))


def _candle_low(candle: Any) -> float | None:
    return _as_float(_get(candle, "low", "l"))


def _candle_close(candle: Any) -> float | None:
    return _as_float(_get(candle, "close", "c"))


def _normalize_side(direction: Any) -> str:
    raw = str(direction or "").strip().upper()
    if raw in {"CALL", "BUY_CALL", "LONG_CALL", "C"}:
        return "CALL"
    if raw in {"PUT", "BUY_PUT", "LONG_PUT", "P"}:
        return "PUT"
    return raw


def _is_daily_timeframe(timeframe: Any) -> bool:
    raw = str(timeframe or "").strip().lower()
    return raw in {"1d", "d", "day", "daily"}


def _buffer_pct() -> float:
    pct = _as_float(os.getenv("DAILY_CONTINUATION_BUFFER_PCT", "0.001"))
    if pct is None or pct < 0:
        return 0.001
    return pct


def _late_day(now: datetime | None) -> bool:
    if now is None:
        return False
    return now.time() >= time(14, 30)


def _first_breach_index(candles: Sequence[Any], *, side: str, trigger_price: float) -> int | None:
    for idx, candle in enumerate(candles):
        high = _candle_high(candle)
        low = _candle_low(candle)
        if side == "CALL" and high is not None and high >= trigger_price:
            return idx
        if side == "PUT" and low is not None and low <= trigger_price:
            return idx
    return None


def _made_fresh_extension_after_breach(
    candles: Sequence[Any],
    *,
    side: str,
    breach_index: int,
) -> bool:
    if breach_index >= len(candles) - 1:
        return False

    breach_candle = candles[breach_index]
    after = candles[breach_index + 1 :]

    if side == "CALL":
        breach_high = _candle_high(breach_candle)
        highs_after = [_candle_high(c) for c in after]
        highs_after = [h for h in highs_after if h is not None]
        return bool(breach_high is not None and highs_after and max(highs_after) > breach_high)

    breach_low = _candle_low(breach_candle)
    lows_after = [_candle_low(c) for c in after]
    lows_after = [l for l in lows_after if l is not None]
    return bool(breach_low is not None and lows_after and min(lows_after) < breach_low)


def _has_reclaim_after_retest(
    candles: Sequence[Any],
    *,
    side: str,
    trigger_price: float,
    buffer: float,
    breach_index: int,
) -> bool:
    after = candles[breach_index + 1 :]
    if len(after) < 2:
        return False

    retested = False
    for candle in after:
        high = _candle_high(candle)
        low = _candle_low(candle)
        close = _candle_close(candle)
        if high is None or low is None or close is None:
            continue

        if side == "CALL":
            if low <= trigger_price:
                retested = True
            if retested and close > trigger_price + buffer:
                return True
        else:
            if high >= trigger_price:
                retested = True
            if retested and close < trigger_price - buffer:
                return True

    return False


def _has_continuation_candle(
    candles: Sequence[Any],
    *,
    side: str,
    trigger_price: float,
    buffer: float,
) -> bool:
    for candle in list(candles[-3:]):
        open_ = _candle_open(candle)
        close = _candle_close(candle)
        high = _candle_high(candle)
        low = _candle_low(candle)
        if open_ is None or close is None or high is None or low is None:
            continue

        if side == "CALL" and close > trigger_price + buffer and close >= open_:
            return True
        if side == "PUT" and close < trigger_price - buffer and close <= open_:
            return True

    return False


def _premium_confirmed(
    *,
    side: str,
    current_underlying_price: float,
    trigger_price: float,
    option_premium_now: float | None,
    option_premium_prev: float | None,
) -> bool | None:
    if option_premium_now is None or option_premium_prev is None:
        return None

    premium_stronger = option_premium_now >= option_premium_prev
    if side == "CALL":
        underlying_still_good = current_underlying_price >= trigger_price
    else:
        underlying_still_good = current_underlying_price <= trigger_price

    return bool(underlying_still_good and premium_stronger)


def validate_daily_intraday_continuation(
    *,
    ticker: str | None = None,
    timeframe: str | None,
    direction: str,
    trigger_price: float,
    current_underlying_price: float | None,
    intraday_candles: Sequence[Any] | None,
    client_id: str | None,
    execution_mode: str | None,
    canonical_signal_id: str | None = None,
    now: datetime | None = None,
    option_premium_now: float | None = None,
    option_premium_prev: float | None = None,
    extra_diagnostics: Mapping[str, Any] | None = None,
) -> ContinuationDecision:
    """
    Pure daily-signal continuation validation.

    This function intentionally has no broker, database, queue, order, position,
    reservation, submit, or cancel side effects. It only returns a decision and
    diagnostics for the caller to preserve downstream.
    """

    side = _normalize_side(direction)
    trigger = _as_float(trigger_price)
    current = _as_float(current_underlying_price)
    pct = _buffer_pct()

    diagnostics: dict[str, Any] = {
        "validator": "daily_intraday_continuation",
        "ticker": ticker,
        "timeframe": timeframe,
        "direction": side,
        "client_id": client_id,
        "execution_mode": execution_mode,
        "canonical_signal_id": canonical_signal_id,
        "continuation_allowed": False,
        "trigger_price": trigger,
        "current_price": current,
        "buffer_pct": pct,
        "fresh_intraday_high": False,
        "fresh_intraday_low": False,
        "reclaim_confirmed": False,
        "continuation_candle": False,
        "price_back_through_trigger": False,
        "late_day": _late_day(now),
        "premium_confirmed": None,
        "reason": None,
    }

    if extra_diagnostics:
        diagnostics.update(dict(extra_diagnostics))

    def decide(allowed: bool, reason: str) -> ContinuationDecision:
        diagnostics["continuation_allowed"] = allowed
        diagnostics["reason"] = reason
        return ContinuationDecision(allowed=allowed, reason=reason, diagnostics=diagnostics)

    if not _env_enabled("ENABLE_DAILY_CONTINUATION_VALIDATION", True):
        return decide(True, REASON_DISABLED)

    if not _is_daily_timeframe(timeframe):
        return decide(True, REASON_NOT_DAILY)

    if side not in {"CALL", "PUT"} or trigger is None or current is None or not intraday_candles:
        return decide(False, REASON_MISSING_CONTEXT)

    candles = list(intraday_candles)
    if not candles:
        return decide(False, REASON_MISSING_CONTEXT)

    buffer = trigger * pct
    breach_index = _first_breach_index(candles, side=side, trigger_price=trigger)
    if breach_index is None:
        return decide(False, REASON_MISSING_CONTEXT)

    if side == "CALL":
        price_back_through = current < trigger
        holding_with_buffer = current > trigger + buffer
    else:
        price_back_through = current > trigger
        holding_with_buffer = current < trigger - buffer

    diagnostics["price_back_through_trigger"] = price_back_through

    fresh_extension = _made_fresh_extension_after_breach(
        candles,
        side=side,
        breach_index=breach_index,
    )
    if side == "CALL":
        diagnostics["fresh_intraday_high"] = fresh_extension
    else:
        diagnostics["fresh_intraday_low"] = fresh_extension

    reclaim = _has_reclaim_after_retest(
        candles,
        side=side,
        trigger_price=trigger,
        buffer=buffer,
        breach_index=breach_index,
    )
    continuation_candle = _has_continuation_candle(
        candles,
        side=side,
        trigger_price=trigger,
        buffer=buffer,
    )
    premium_ok = _premium_confirmed(
        side=side,
        current_underlying_price=current,
        trigger_price=trigger,
        option_premium_now=_as_float(option_premium_now),
        option_premium_prev=_as_float(option_premium_prev),
    )

    diagnostics["reclaim_confirmed"] = reclaim
    diagnostics["continuation_candle"] = continuation_candle
    diagnostics["premium_confirmed"] = premium_ok

    if price_back_through:
        if not fresh_extension and not reclaim and not continuation_candle:
            return decide(False, REASON_TOUCH_ONLY)
        return decide(False, REASON_BACK_THROUGH)

    if diagnostics["late_day"] and not fresh_extension:
        return decide(False, REASON_LATE_NO_FOLLOW)

    if premium_ok is False and not fresh_extension:
        return decide(False, REASON_PREMIUM_WEAK)

    allowed_by_structure = fresh_extension or reclaim or continuation_candle
    if holding_with_buffer and allowed_by_structure:
        return decide(True, REASON_ALLOWED)

    if not fresh_extension:
        return decide(False, REASON_NO_EXTENSION)

    if not reclaim and not continuation_candle:
        return decide(False, REASON_NO_RECLAIM)

    return decide(False, REASON_TOUCH_ONLY)
