from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")


def to_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def extract_underlying_price(signal: dict[str, Any]) -> Optional[float]:
    return (
        to_float(signal.get("underlying_price"))
        or to_float(signal.get("current_price"))
    )


def extract_trade_geometry(signal: dict[str, Any]) -> dict[str, Any]:
    side = str(signal.get("side") or signal.get("direction") or "").upper()
    trigger = to_float(signal.get("trigger_price")) or to_float(signal.get("trigger"))
    stop = to_float(signal.get("stop_price")) or to_float(signal.get("stop"))
    target = (
        to_float(signal.get("target_price"))
        or to_float(signal.get("target"))
        or to_float(signal.get("pt1"))
    )
    underlying = extract_underlying_price(signal)
    missing = [
        name
        for name, value in (
            ("side", side if side in {"CALL", "PUT"} else None),
            ("trigger", trigger),
            ("stop", stop),
            ("target", target),
            ("underlying_price", underlying),
        )
        if value is None or value == ""
    ]
    return {
        "available": not missing,
        "side": side,
        "trigger": trigger,
        "stop": stop,
        "target": target,
        "underlying_price": underlying,
        "missing_data": missing,
    }


def extract_candles(signal: dict[str, Any], timeframe: str) -> list[dict[str, Any]]:
    candidates = (
        f"candles_{timeframe}",
        f"{timeframe}_candles",
        timeframe,
    )
    for key in candidates:
        value = signal.get(key)
        if isinstance(value, list):
            return [dict(row) for row in value if isinstance(row, dict)]
    candles = signal.get("candles")
    if isinstance(candles, dict):
        value = candles.get(timeframe)
        if isinstance(value, list):
            return [dict(row) for row in value if isinstance(row, dict)]
    return []


def summarize_timeframe(signal: dict[str, Any], timeframe: str) -> dict[str, Any]:
    rows = extract_candles(signal, timeframe)
    if not rows:
        return {
            "available": False,
            "status": "UNAVAILABLE",
            "missing_reason": f"{timeframe}_candles_missing",
            "candles": [],
        }
    return {
        "available": True,
        "status": "COMPLETE",
        "count": len(rows),
        "last_candle": rows[-1],
        "candles": rows,
    }


def build_data_quality_warnings(signal: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    if not extract_underlying_price(signal):
        warnings.append("underlying_price_missing")
    if not (signal.get("volume_context") or signal.get("volume")):
        warnings.append("volume_context_missing")
    if not (signal.get("sector") or signal.get("sector_etf")):
        warnings.append("sector_context_missing")
    return warnings


def _quote_source(broker: Any) -> Any:
    return getattr(broker, "data_broker", None) or getattr(broker, "market_data_broker", None) or broker


def _quote(symbol: str, broker: Any) -> dict[str, Any]:
    source = _quote_source(broker)
    if not symbol or source is None or not hasattr(source, "get_quote"):
        return {}
    value = source.get_quote(symbol)
    return dict(value) if isinstance(value, dict) else {}


def _history(symbol: str, broker: Any, *, days: int = 400) -> list[dict[str, Any]]:
    source = _quote_source(broker)
    if not symbol or source is None or not hasattr(source, "_get"):
        return []
    now = datetime.now(timezone.utc)
    payload = source._get(
        "/v1/markets/history",
        params={
            "symbol": symbol,
            "interval": "daily",
            "start": (now - timedelta(days=days)).strftime("%Y-%m-%d"),
            "end": now.strftime("%Y-%m-%d"),
        },
    )
    rows = ((payload.get("history") or {}).get("day") or []) if isinstance(payload, dict) else []
    if isinstance(rows, dict):
        rows = [rows]
    result = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            result.append({
                "time": str(row.get("date") or row.get("time") or ""),
                "open": float(row["open"]), "high": float(row["high"]),
                "low": float(row["low"]), "close": float(row["close"]),
                "volume": float(row.get("volume") or 0),
            })
        except (KeyError, TypeError, ValueError):
            continue
    return result


def _aggregate_calendar(
    rows: list[dict[str, Any]], period: str, *, now: Optional[datetime] = None
) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        try:
            day = datetime.fromisoformat(str(row.get("time"))[:10]).date()
        except ValueError:
            continue
        if period == "weekly":
            iso = day.isocalendar()
            key = f"{iso.year}-W{iso.week:02d}"
        else:
            key = f"{day.year}-{day.month:02d}"
        current = grouped.get(key)
        if current is None:
            grouped[key] = dict(row)
        else:
            current["high"] = max(float(current["high"]), float(row["high"]))
            current["low"] = min(float(current["low"]), float(row["low"]))
            current["close"] = float(row["close"])
            current["volume"] = float(current.get("volume") or 0) + float(row.get("volume") or 0)
    now_day = (now or datetime.now(timezone.utc)).astimezone(ET).date()
    if period == "weekly":
        iso = now_day.isocalendar()
        current_key = f"{iso.year}-W{iso.week:02d}"
    else:
        current_key = f"{now_day.year}-{now_day.month:02d}"
    grouped.pop(current_key, None)
    return list(grouped.values())


def _completed_intraday(
    rows: list[dict[str, Any]], *, bucket_minutes: int, now: datetime
) -> list[dict[str, Any]]:
    from ap.fvg_telemetry import aggregate_bars
    aggregated = aggregate_bars(rows, bucket_minutes=bucket_minutes)
    completed = []
    now_et = now.astimezone(ET)
    for row in aggregated:
        try:
            start = datetime.fromisoformat(str(row.get("time") or "").replace("Z", "+00:00"))
            if start.tzinfo is None:
                start = start.replace(tzinfo=ET)
            start_et = start.astimezone(ET)
            session_close = start_et.replace(hour=16, minute=0, second=0, microsecond=0)
            bucket_end = min(start_et + timedelta(minutes=bucket_minutes), session_close)
            if now_et >= bucket_end:
                completed.append(row)
        except ValueError:
            continue
    return completed


def _quote_price(quote: dict[str, Any]) -> Optional[float]:
    return to_float(quote.get("last")) or to_float(quote.get("close"))


def _quote_change_pct(quote: dict[str, Any]) -> Optional[float]:
    primary = to_float(quote.get("change_percentage"))
    return primary if primary is not None else to_float(quote.get("change_pct"))


def _age_seconds(value: Any, *, now: datetime) -> Optional[int]:
    if value is None or value == "":
        return 0
    try:
        if isinstance(value, (int, float)) or str(value).isdigit():
            raw = float(value)
            if raw > 10_000_000_000:
                raw /= 1000.0
            observed = datetime.fromtimestamp(raw, tz=timezone.utc)
        else:
            observed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if observed.tzinfo is None:
                observed = observed.replace(tzinfo=timezone.utc)
        return max(0, int((now - observed.astimezone(timezone.utc)).total_seconds()))
    except (TypeError, ValueError, OSError):
        return None


def _parse_as_of(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(
            str(value).replace("Z", "+00:00")
        )
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def _frozen_candles_as_of(
    rows: list[dict[str, Any]], *, as_of: Optional[datetime], bar_minutes: int
) -> list[dict[str, Any]]:
    if as_of is None:
        return list(rows)
    completed: list[dict[str, Any]] = []
    for row in rows:
        try:
            timestamp = datetime.fromisoformat(
                str(row.get("time") or row.get("timestamp") or "").replace("Z", "+00:00")
            )
            if timestamp.tzinfo is None:
                continue
            if timestamp.astimezone(timezone.utc) + timedelta(minutes=bar_minutes) <= as_of:
                completed.append(dict(row))
        except (TypeError, ValueError):
            continue
    return completed


def _intraday_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {}
    total_volume = sum(to_float(row.get("volume")) or 0 for row in rows)
    pv = sum(
        (((to_float(row.get("high")) or 0) + (to_float(row.get("low")) or 0)
          + (to_float(row.get("close")) or 0)) / 3.0) * (to_float(row.get("volume")) or 0)
        for row in rows
    )
    vwap = pv / total_volume if total_volume > 0 else None
    by_day: dict[str, float] = {}
    for row in rows:
        key = str(row.get("time") or "")[:10]
        by_day[key] = by_day.get(key, 0.0) + (to_float(row.get("volume")) or 0)
    values = list(by_day.values())
    relative_volume = None
    if len(values) >= 2 and sum(values[:-1]) > 0:
        relative_volume = values[-1] / (sum(values[:-1]) / len(values[:-1]))
    return {"vwap": vwap, "relative_volume": relative_volume, "total_volume": total_volume}


def collect_point_in_time_context(
    signal: dict[str, Any], *, broker: Any = None, phase: str
) -> dict[str, Any]:
    """Fetch and normalize evidence for the existing observe-only evaluators."""
    phase = str(phase or "").upper()
    ticker = str(signal.get("ticker") or signal.get("symbol") or "").strip().upper()
    now_utc = datetime.now(timezone.utc)
    breach_as_of = _parse_as_of(signal.get("trigger_crossed_at")) if phase == "BREACH" else None
    evidence_now = breach_as_of or now_utc
    collected_at = now_utc.isoformat()
    errors: list[str] = []
    quote: dict[str, Any] = {}
    daily: list[dict[str, Any]] = []
    bars_15m: list[dict[str, Any]] = (
        extract_candles(signal, "15m") if phase == "BREACH" else []
    )
    bars_5m: list[dict[str, Any]] = (
        extract_candles(signal, "5m") if phase == "BREACH" else []
    )
    fetched_15m = False
    market_quote: dict[str, Any] = {}
    sector_quote: dict[str, Any] = {}
    breach_timestamp_valid = phase != "BREACH" or breach_as_of is not None
    # BREACH must not turn a later worker run into a future quote/market-state
    # authority. Its exact quote is frozen by the callback; only historical
    # bars are requested at the crossed timestamp. PRETRIGGER/PREOPEN retain
    # their existing current-context behavior.
    if broker is not None and ticker and breach_timestamp_valid:
        if phase != "BREACH":
            try:
                quote = _quote(ticker, broker)
            except Exception as exc:
                errors.append(f"underlying_quote:{type(exc).__name__}")
        try:
            daily = _history(ticker, broker)
        except Exception as exc:
            errors.append(f"daily_history:{type(exc).__name__}")
        try:
            from ap.fvg_telemetry import fetch_15m_bars
            fetched = fetch_15m_bars(
                ticker, broker, now=evidence_now if phase == "BREACH" else None
            )
            if fetched:
                bars_15m = fetched
                fetched_15m = True
        except Exception as exc:
            errors.append(f"intraday_history:{type(exc).__name__}")
        if phase != "BREACH":
            try:
                market_quote = _quote(str(signal.get("market_symbol") or "SPY"), broker)
            except Exception as exc:
                errors.append(f"market_quote:{type(exc).__name__}")
            sector_symbol = str(signal.get("sector_etf") or "").strip().upper()
            if sector_symbol:
                try:
                    sector_quote = _quote(sector_symbol, broker)
                except Exception as exc:
                    errors.append(f"sector_quote:{type(exc).__name__}")

    bars_15m = (
        _frozen_candles_as_of(bars_15m, as_of=breach_as_of, bar_minutes=15)
        if breach_timestamp_valid else []
    )
    bars_5m = (
        _frozen_candles_as_of(bars_5m, as_of=breach_as_of, bar_minutes=5)
        if breach_timestamp_valid else []
    )
    candles_15m = (
        _completed_intraday(bars_15m, bucket_minutes=15, now=evidence_now)
        if bars_15m else []
    )
    candles_1h = _completed_intraday(bars_15m, bucket_minutes=60, now=evidence_now) if bars_15m else []
    candles_4h = _completed_intraday(bars_15m, bucket_minutes=240, now=evidence_now) if bars_15m else []
    today_et = evidence_now.astimezone(ET).date().isoformat()
    completed_daily = [row for row in daily if str(row.get("time") or "")[:10] < today_et]
    metrics = _intraday_metrics(bars_15m)
    observed_price = _quote_price(quote)
    source_timestamp = quote.get("trade_date") or quote.get("trade_timestamp") or collected_at
    observation = {
        "price": observed_price,
        "source": "tradier_quote" if observed_price is not None else None,
        "observed_at": collected_at if observed_price is not None else None,
        "source_timestamp": str(source_timestamp) if observed_price is not None else None,
        "age_seconds": _age_seconds(source_timestamp, now=now_utc) if observed_price is not None else None,
    }
    candles = {
        "monthly": _aggregate_calendar(completed_daily, "monthly", now=evidence_now),
        "weekly": _aggregate_calendar(completed_daily, "weekly", now=evidence_now),
        "daily": completed_daily,
        "4h": candles_4h,
        "1h": candles_1h,
    }
    if phase == "BREACH":
        candles.update({"15m": candles_15m, "5m": bars_5m})
    data_sources = {
        "candles": candles,
        "trend": {"vwap": metrics.get("vwap"), "current_price": observed_price},
        "volume": {"relative_volume": metrics.get("relative_volume")},
        "market": {
            "symbol": str(signal.get("market_symbol") or "SPY"),
            "change_pct": _quote_change_pct(market_quote),
        },
        "sector": {
            "sector": signal.get("sector"), "symbol": signal.get("sector_etf"),
            "change_pct": _quote_change_pct(sector_quote),
        },
    }
    intraday_source = (
        "tradier_timesales_15min" if fetched_15m
        else "frozen_signal_15m" if phase == "BREACH" and bars_15m else None
    )
    result = {
        "phase": phase, "collected_at": collected_at,
        "as_of": breach_as_of.isoformat() if breach_as_of else None,
        "data_sources": data_sources, "underlying_observation": observation,
        "errors": errors,
        "provenance": {
            "quote": "tradier_quote" if quote else None,
            "daily": "tradier_history_daily" if daily else None,
            "intraday": intraday_source,
            "market": "tradier_quote" if market_quote else None,
            "sector": "tradier_quote" if sector_quote else None,
        },
    }
    if phase == "BREACH":
        result["provenance"].update({
            "fifteen_minute": (
                "tradier_timesales_15min" if fetched_15m
                else "frozen_signal_15m" if candles_15m else None
            ),
            "five_minute": "frozen_signal_5min" if bars_5m else None,
        })
    return result
