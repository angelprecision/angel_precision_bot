from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, time, timezone
from typing import Any, Mapping, Sequence


REASON_ALLOWED = "daily_continuation_allowed"
REASON_DISABLED = "daily_continuation_skipped:disabled"
REASON_NOT_DAILY = "daily_continuation_skipped:not_daily"
REASON_MISSING_CONTEXT = "daily_continuation_failed:missing_intraday_context"
REASON_TOUCH_ONLY = "daily_continuation_failed:trigger_touch_only"
REASON_NO_RECLAIM = "daily_continuation_failed:no_reclaim_after_open_move"
REASON_NO_EXTENSION = "daily_continuation_failed:no_fresh_intraday_extension"
REASON_BACK_THROUGH = "daily_continuation_failed:price_back_through_trigger"
REASON_OPENING_EXHAUSTED = "daily_continuation_failed:opening_move_exhausted"
REASON_LATE_NO_FOLLOW = "daily_continuation_failed:late_day_no_followthrough"
REASON_PREMIUM_WEAK = "daily_continuation_failed:premium_not_confirming"
REASON_NEAR_INVALIDATION = "daily_continuation_failed:near_invalidation"


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
    return _as_float(_get(candle, "close", "c", "price", "last"))


def _candle_timestamp(candle: Any) -> Any:
    return _get(candle, "time", "timestamp", "datetime", "date", "ts")


def _normalize_side(direction: Any) -> str:
    raw = str(direction or "").strip().upper()
    if raw in {"CALL", "BUY_CALL", "LONG_CALL", "C"}:
        return "CALL"
    if raw in {"PUT", "BUY_PUT", "LONG_PUT", "P"}:
        return "PUT"
    return raw


def _is_daily_timeframe(timeframe: Any) -> bool:
    raw = str(timeframe or "").strip().lower()
    return raw in {"1d", "d", "day", "daily", "overnight"}


def _buffer_pct() -> float:
    pct = _as_float(os.getenv("DAILY_CONTINUATION_BUFFER_PCT", "0.001"))
    if pct is None or pct < 0:
        return 0.001
    return pct


def _near_invalidation_pct() -> float:
    pct = _as_float(os.getenv("DAILY_CONTINUATION_INVALIDATION_BUFFER_PCT", "0.0025"))
    if pct is None or pct < 0:
        return 0.0025
    return pct


def _opening_bars_window() -> int:
    try:
        value = int(os.getenv("DAILY_CONTINUATION_OPENING_BARS", "5"))
    except (TypeError, ValueError):
        value = 5
    return max(1, min(30, value))


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
    buffer: float = 0.0,
) -> bool:
    if breach_index >= len(candles) - 1:
        return False

    breach_candle = candles[breach_index]
    after = candles[breach_index + 1 :]

    if side == "CALL":
        breach_high = _candle_high(breach_candle)
        highs_after = [_candle_high(c) for c in after]
        highs_after = [h for h in highs_after if h is not None]
        return bool(breach_high is not None and highs_after and max(highs_after) > breach_high + buffer)

    breach_low = _candle_low(breach_candle)
    lows_after = [_candle_low(c) for c in after]
    lows_after = [l for l in lows_after if l is not None]
    return bool(breach_low is not None and lows_after and min(lows_after) < breach_low - buffer)


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
            if low <= trigger_price + buffer:
                retested = True
            if retested and close > trigger_price + buffer:
                return True
        else:
            if high >= trigger_price - buffer:
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
    # Recent candle must close on the correct side of the trigger and close in
    # the directional half of its own range. That rejects wick-only trigger pokes
    # where price pierces the level but closes weak.
    for candle in list(candles[-3:]):
        open_ = _candle_open(candle)
        close = _candle_close(candle)
        high = _candle_high(candle)
        low = _candle_low(candle)
        if open_ is None or close is None or high is None or low is None:
            continue

        rng = high - low
        midpoint = low + (rng / 2.0) if rng > 0 else close

        if side == "CALL":
            closed_above = close > trigger_price + buffer
            did_not_wick_fail = close >= open_ and close >= midpoint
            if closed_above and did_not_wick_fail:
                return True
        else:
            closed_below = close < trigger_price - buffer
            did_not_wick_fail = close <= open_ and close <= midpoint
            if closed_below and did_not_wick_fail:
                return True

    return False


def _closed_on_correct_side_after_breach(
    candles: Sequence[Any],
    *,
    side: str,
    trigger_price: float,
    buffer: float,
    breach_index: int,
) -> bool:
    for candle in candles[breach_index:]:
        close = _candle_close(candle)
        if close is None:
            continue
        if side == "CALL" and close >= trigger_price:
            return True
        if side == "PUT" and close <= trigger_price:
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


def _near_or_through_invalidation(
    *,
    side: str,
    current_price: float,
    invalidation_price: float | None,
    pct: float,
) -> bool:
    if invalidation_price is None or invalidation_price <= 0:
        return False

    band = abs(invalidation_price) * pct
    if side == "CALL":
        return current_price <= invalidation_price + band
    return current_price >= invalidation_price - band


def _fetch_quote_price(broker: Any, ticker: str) -> float | None:
    try:
        if broker is not None and hasattr(broker, "get_quote"):
            q = broker.get_quote(ticker)
            if isinstance(q, Mapping):
                for key in ("last", "mark", "mid", "bid", "ask"):
                    val = _as_float(q.get(key))
                    if val and val > 0:
                        return val
    except Exception:
        return None
    return None


def fetch_intraday_continuation_candles(
    *,
    ticker: str,
    broker: Any,
    now: datetime | None = None,
    interval: str = "1min",
) -> list[dict[str, Any]]:
    """Fetch same-day regular-session bars for continuation validation.

    Uses the same production market-data shape as the overnight validator:
    Tradier /v1/markets/timesales with session_filter=open. The helper is
    read-only and never submits/cancels/mutates broker, orders, positions, queue,
    or proof_trades. It returns [] on unavailable data so callers fail closed.
    """
    if not ticker or broker is None:
        return []

    try:
        from zoneinfo import ZoneInfo

        et = ZoneInfo("America/New_York")
    except Exception:  # pragma: no cover - Python 3.8 fallback only
        et = timezone.utc

    now_et = (now or datetime.now(timezone.utc)).astimezone(et)
    today = now_et.strftime("%Y-%m-%d")
    start_str = f"{today}T09:30:00"
    end_str = now_et.strftime("%Y-%m-%dT%H:%M:%S")

    try:
        try:
            from ap.overnight_daily_validator import _resolve_market_data_base_url

            base_url = _resolve_market_data_base_url(broker)
        except Exception:
            base_url = str(
                os.getenv("TRADIER_MARKET_DATA_BASE_URL")
                or os.getenv("TRADIER_DATA_BASE_URL")
                or getattr(getattr(broker, "cfg", None), "base_url", "")
                or "https://api.tradier.com"
            ).rstrip("/")
            if "sandbox.tradier.com" in base_url.lower():
                base_url = "https://api.tradier.com"

        session = getattr(broker, "session", None)
        if session is None or not hasattr(session, "get"):
            return []

        resp = session.get(
            f"{base_url}/v1/markets/timesales",
            params={
                "symbol": ticker,
                "interval": interval,
                "start": start_str,
                "end": end_str,
                "session_filter": "open",
            },
            headers={"Accept": "application/json"},
            timeout=10,
        )
        if hasattr(resp, "raise_for_status"):
            resp.raise_for_status()
        payload = resp.json() if hasattr(resp, "json") else {}
        series = (payload.get("series") or {}) if isinstance(payload, Mapping) else {}
        data_node = series.get("data") if isinstance(series, Mapping) else None
        items = data_node.get("item") if isinstance(data_node, Mapping) else data_node
        if items is None:
            return []
        if isinstance(items, Mapping):
            items = [items]
        candles: list[dict[str, Any]] = []
        for raw in items:
            if not isinstance(raw, Mapping):
                continue
            candles.append(
                {
                    "open": _as_float(raw.get("open") or raw.get("o")),
                    "high": _as_float(raw.get("high") or raw.get("h")),
                    "low": _as_float(raw.get("low") or raw.get("l")),
                    "close": _as_float(raw.get("close") or raw.get("c") or raw.get("price")),
                    "time": raw.get("time") or raw.get("timestamp") or raw.get("datetime"),
                }
            )
        return [c for c in candles if _candle_high(c) is not None and _candle_low(c) is not None]
    except Exception:
        return []


def validate_daily_intraday_continuation_for_watched_signal(
    watched: Any,
    broker: Any,
    *,
    client_id: str | None = None,
    execution_mode: str | None = None,
    canonical_signal_id: str | None = None,
    now: datetime | None = None,
) -> ContinuationDecision:
    """Production-shaped adapter for breach-time entry gates.

    Intended call site: APExecutionCore._on_entry_trigger before
    order_state_machine.submit_existing_entry(). It fetches real intraday
    candles, preserves client/mode/signal diagnostics, and fails closed when
    the continuation context is unavailable.
    """
    sig = getattr(watched, "signal", {}) or {}
    ticker = str(getattr(watched, "ticker", None) or sig.get("ticker") or sig.get("symbol") or "").upper()
    side = str(getattr(watched, "side", None) or sig.get("side") or sig.get("direction") or "")
    timeframe = str(sig.get("timeframe") or getattr(watched, "timeframe", None) or "1d")
    trigger = _as_float(
        getattr(watched, "entry_trigger", None)
        or getattr(watched, "trigger_price", None)
        or sig.get("entry_price")
        or (sig.get("trigger") or {}).get("entry")
    )
    invalidation = _as_float(
        getattr(watched, "stop_level", None)
        or sig.get("stop_price")
        or (sig.get("trigger") or {}).get("stop")
    )

    candles = fetch_intraday_continuation_candles(ticker=ticker, broker=broker, now=now)
    current = _candle_close(candles[-1]) if candles else None
    if current is None:
        current = _fetch_quote_price(broker, ticker)
    if current is None:
        try:
            bid = _as_float(getattr(watched, "last_quote_bid", None))
            ask = _as_float(getattr(watched, "last_quote_ask", None))
            if bid and ask:
                current = (bid + ask) / 2.0
            else:
                current = bid or ask
        except Exception:
            current = None

    return validate_daily_intraday_continuation(
        ticker=ticker,
        timeframe=timeframe,
        direction=side,
        trigger_price=trigger,
        current_underlying_price=current,
        intraday_candles=candles,
        client_id=client_id or sig.get("client_id") or sig.get("client_email"),
        execution_mode=execution_mode or sig.get("execution_mode"),
        canonical_signal_id=canonical_signal_id or sig.get("canonical_signal_id") or sig.get("signal_id"),
        now=now,
        invalidation_price=invalidation,
        extra_diagnostics={
            "adapter": "watched_signal",
            "signal_id": sig.get("signal_id"),
            "local_order_id": sig.get("local_order_id"),
            "candles_count": len(candles),
            "current_price_source": "intraday_candle" if candles else "quote_or_watched",
        },
    )


def validate_daily_intraday_continuation(
    *,
    ticker: str | None = None,
    timeframe: str | None,
    direction: str,
    trigger_price: float | None,
    current_underlying_price: float | None,
    intraday_candles: Sequence[Any] | None,
    client_id: str | None,
    execution_mode: str | None,
    canonical_signal_id: str | None = None,
    now: datetime | None = None,
    option_premium_now: float | None = None,
    option_premium_prev: float | None = None,
    invalidation_price: float | None = None,
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
    inv = _as_float(invalidation_price)

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
        "invalidation_price": inv,
        "buffer_pct": pct,
        "invalidation_buffer_pct": _near_invalidation_pct(),
        "fresh_intraday_high": False,
        "fresh_intraday_low": False,
        "reclaim_confirmed": False,
        "continuation_candle": False,
        "holding_with_buffer": False,
        "price_back_through_trigger": False,
        "opening_breach": False,
        "opening_bars_window": _opening_bars_window(),
        "late_day": _late_day(now),
        "premium_confirmed": None,
        "first_breach_index": None,
        "first_breach_time": None,
        "last_candle_time": None,
        "reason": None,
    }

    if extra_diagnostics:
        diagnostics.update(dict(extra_diagnostics))

    def decide(allowed: bool, reason: str) -> ContinuationDecision:
        diagnostics["continuation_allowed"] = allowed
        diagnostics["reason"] = reason
        return ContinuationDecision(allowed=allowed, reason=reason, diagnostics=diagnostics)

    if not _env_enabled("ENABLE_DAILY_CONTINUATION_VALIDATION", False):  # safe default: must be explicitly enabled in Render
        return decide(True, REASON_DISABLED)

    if not _is_daily_timeframe(timeframe):
        return decide(True, REASON_NOT_DAILY)

    if side not in {"CALL", "PUT"} or trigger is None or trigger <= 0 or current is None or current <= 0 or not intraday_candles:
        return decide(False, REASON_MISSING_CONTEXT)

    candles = list(intraday_candles)
    if not candles:
        return decide(False, REASON_MISSING_CONTEXT)

    diagnostics["last_candle_time"] = _candle_timestamp(candles[-1])

    buffer = trigger * pct
    breach_index = _first_breach_index(candles, side=side, trigger_price=trigger)
    if breach_index is None:
        return decide(False, REASON_MISSING_CONTEXT)

    diagnostics["first_breach_index"] = breach_index
    diagnostics["first_breach_time"] = _candle_timestamp(candles[breach_index])
    diagnostics["opening_breach"] = breach_index < _opening_bars_window()

    if side == "CALL":
        price_back_through = current < trigger
        holding_with_buffer = current > trigger + buffer
    else:
        price_back_through = current > trigger
        holding_with_buffer = current < trigger - buffer

    diagnostics["price_back_through_trigger"] = price_back_through
    diagnostics["holding_with_buffer"] = holding_with_buffer

    if _near_or_through_invalidation(
        side=side,
        current_price=current,
        invalidation_price=inv,
        pct=_near_invalidation_pct(),
    ):
        return decide(False, REASON_NEAR_INVALIDATION)

    fresh_extension = _made_fresh_extension_after_breach(
        candles,
        side=side,
        breach_index=breach_index,
        buffer=buffer,
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
        closed_correct_side = _closed_on_correct_side_after_breach(
            candles,
            side=side,
            trigger_price=trigger,
            buffer=buffer,
            breach_index=breach_index,
        )
        diagnostics["closed_correct_side_after_breach"] = closed_correct_side
        if not closed_correct_side and not fresh_extension and not reclaim and not continuation_candle:
            return decide(False, REASON_TOUCH_ONLY)
        return decide(False, REASON_BACK_THROUGH)

    if diagnostics["opening_breach"] and not fresh_extension and not reclaim and not continuation_candle:
        return decide(False, REASON_OPENING_EXHAUSTED)

    if diagnostics["late_day"] and not fresh_extension:
        return decide(False, REASON_LATE_NO_FOLLOW)

    if premium_ok is False and not fresh_extension:
        return decide(False, REASON_PREMIUM_WEAK)

    allowed_by_structure = holding_with_buffer or fresh_extension or reclaim or continuation_candle
    if allowed_by_structure:
        return decide(True, REASON_ALLOWED)

    if not fresh_extension:
        return decide(False, REASON_NO_EXTENSION)

    if not reclaim and not continuation_candle:
        return decide(False, REASON_NO_RECLAIM)

    return decide(False, REASON_TOUCH_ONLY)
