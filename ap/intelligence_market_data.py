from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional
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


def _history(
    symbol: str,
    broker: Any,
    *,
    days: int = 400,
    now: Optional[datetime] = None,
) -> list[dict[str, Any]]:
    source = _quote_source(broker)
    if not symbol or source is None or not hasattr(source, "_get"):
        return []
    history_now = now or datetime.now(timezone.utc)
    if history_now.tzinfo is None or history_now.utcoffset() is None:
        return []
    history_et = history_now.astimezone(ET)
    payload = source._get(
        "/v1/markets/history",
        params={
            "symbol": symbol,
            "interval": "daily",
            "start": (history_et - timedelta(days=days)).strftime("%Y-%m-%d"),
            "end": history_et.strftime("%Y-%m-%d"),
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


def _parse_breach_as_of(value: Any) -> tuple[Optional[datetime], str]:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None, "malformed"
    else:
        return None, "missing"
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None, "timezone_aware_required"
    return parsed, ""


def _filter_completed_bars(
    rows: Any, *, interval_minutes: int, as_of: datetime
) -> list[dict[str, Any]]:
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        return []
    completed: list[dict[str, Any]] = []
    interval = timedelta(minutes=interval_minutes)
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        raw_time = str(row.get("time") or row.get("timestamp") or "")
        try:
            opened = datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
        except ValueError:
            continue
        if opened.tzinfo is None or opened.utcoffset() is None:
            continue
        if opened + interval <= as_of:
            completed.append(dict(row))
    return completed


def _strict_breach_numeric(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, str) and not value.strip():
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _empty_breach_observation() -> dict[str, Any]:
    return {
        "price": None,
        "source": None,
        "observed_at": None,
        "source_timestamp": None,
        "age_seconds": None,
    }


def resolve_frozen_breach_observation(
    signal: dict[str, Any], as_of: datetime
) -> dict[str, Any]:
    """Resolve only explicit, point-in-time breach price evidence."""
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        return _empty_breach_observation()

    scalar_candidates = (
        ("signal.breach_price", "breach_price", "breach_price_at"),
        (
            "signal.frozen_underlying_price",
            "frozen_underlying_price",
            "frozen_underlying_price_at",
        ),
        ("signal.underlying_at_breach", "underlying_at_breach", "underlying_at_breach_at"),
        ("signal.price_at_breach", "price_at_breach", "price_at_breach_at"),
    )
    for source, field, timestamp_field in scalar_candidates:
        if field not in signal:
            continue
        price = _strict_breach_numeric(signal.get(field))
        if price is None:
            continue

        if timestamp_field in signal:
            source_timestamp = signal.get(timestamp_field)
            parsed_timestamp, _ = _parse_breach_as_of(source_timestamp)
            if parsed_timestamp is None or parsed_timestamp > as_of:
                continue
            source_timestamp_value = str(source_timestamp)
        else:
            source_timestamp_value = as_of.isoformat()
        return {
            "price": price,
            "source": source,
            "observed_at": as_of.isoformat(),
            "source_timestamp": source_timestamp_value,
            "age_seconds": None,
        }

    evidence = signal.get("breach_evidence")
    if isinstance(evidence, Mapping):
        timestamp = None
        for field in ("as_of", "data_as_of", "timestamp", "observed_at", "time"):
            if field in evidence:
                timestamp = evidence.get(field)
                break
        parsed_timestamp, _ = _parse_breach_as_of(timestamp)
        if parsed_timestamp is None or parsed_timestamp > as_of:
            return _empty_breach_observation()

        price = None
        for field in ("price", "underlying_price", "breach_price", "value"):
            if field in evidence:
                price = _strict_breach_numeric(evidence.get(field))
                break
        if price is None:
            return _empty_breach_observation()
        return {
            "price": price,
            "source": "signal.breach_evidence",
            "observed_at": as_of.isoformat(),
            "source_timestamp": str(timestamp),
            "age_seconds": None,
        }

    return _empty_breach_observation()


def _breach_data_sources(
    signal: dict[str, Any],
    *,
    daily: list[dict[str, Any]],
    bars_5m: list[dict[str, Any]],
    bars_15m: list[dict[str, Any]],
    candles_1h: list[dict[str, Any]],
    candles_4h: list[dict[str, Any]],
    now: datetime,
    observed_price: Optional[float],
) -> dict[str, Any]:
    today_et = now.astimezone(ET).date().isoformat()
    completed_daily = [
        row
        for row in daily
        if isinstance(row, dict) and str(row.get("time") or "")[:10] < today_et
    ]
    metrics = _intraday_metrics(bars_15m)
    return {
        "candles": {
            "monthly": _aggregate_calendar(completed_daily, "monthly", now=now),
            "weekly": _aggregate_calendar(completed_daily, "weekly", now=now),
            "daily": completed_daily,
            "5m": bars_5m,
            "15m": bars_15m,
            "1h": candles_1h,
            "4h": candles_4h,
        },
        "trend": {"vwap": metrics.get("vwap"), "current_price": observed_price},
        "volume": {"relative_volume": metrics.get("relative_volume")},
        "market": {
            "symbol": str(signal.get("market_symbol") or "SPY"),
            "change_pct": None,
        },
        "sector": {
            "sector": signal.get("sector"),
            "symbol": signal.get("sector_etf"),
            "change_pct": None,
        },
    }


def _collect_breach_context(
    signal: dict[str, Any],
    *,
    broker: Any,
    ticker: str,
    evidence_now: datetime,
    collected_at: str,
) -> dict[str, Any]:
    errors: list[str] = []
    daily: list[dict[str, Any]] = []
    bars_5m: list[dict[str, Any]] = []
    bars_15m: list[dict[str, Any]] = []
    fetched_5m = False
    fetched_15m = False

    # BREACH deliberately has no current quote path. Only bounded historical
    # reads and already-frozen signal evidence are allowed here.
    if broker is not None and ticker:
        try:
            daily = _history(ticker, broker, now=evidence_now)
        except Exception as exc:
            errors.append(f"daily_history:{type(exc).__name__}")
        try:
            from ap.fvg_telemetry import fetch_15m_bars

            fetched_15m = True
            bars_15m = fetch_15m_bars(ticker, broker, now=evidence_now)
        except Exception as exc:
            errors.append(f"intraday_history:{type(exc).__name__}")
        try:
            from ap.fvg_telemetry import fetch_5m_bars

            fetched_5m = True
            bars_5m = fetch_5m_bars(ticker, broker, now=evidence_now)
        except Exception as exc:
            errors.append(f"intraday_history_5m:{type(exc).__name__}")

    bars_15m = _filter_completed_bars(
        bars_15m, interval_minutes=15, as_of=evidence_now
    )
    frozen_15m = _filter_completed_bars(
        extract_candles(signal, "15m"), interval_minutes=15, as_of=evidence_now
    )
    used_frozen_15m = not bars_15m and bool(frozen_15m)
    if used_frozen_15m:
        bars_15m = frozen_15m

    bars_5m = _filter_completed_bars(
        bars_5m, interval_minutes=5, as_of=evidence_now
    )
    frozen_5m = _filter_completed_bars(
        extract_candles(signal, "5m"), interval_minutes=5, as_of=evidence_now
    )
    used_frozen_5m = not bars_5m and bool(frozen_5m)
    if used_frozen_5m:
        bars_5m = frozen_5m

    try:
        candles_1h = _completed_intraday(
            bars_15m, bucket_minutes=60, now=evidence_now
        )
        candles_4h = _completed_intraday(
            bars_15m, bucket_minutes=240, now=evidence_now
        )
    except Exception as exc:
        errors.append(f"intraday_aggregation:{type(exc).__name__}")
        candles_1h = []
        candles_4h = []

    observation = resolve_frozen_breach_observation(signal, evidence_now)
    data_sources = _breach_data_sources(
        signal,
        daily=daily,
        bars_5m=bars_5m,
        bars_15m=bars_15m,
        candles_1h=candles_1h,
        candles_4h=candles_4h,
        now=evidence_now,
        observed_price=observation.get("price"),
    )
    return {
        "phase": "BREACH",
        "collected_at": collected_at,
        "as_of": evidence_now.isoformat(),
        "data_sources": data_sources,
        "underlying_observation": observation,
        "errors": errors,
        "provenance": {
            "quote": None,
            "daily": "tradier_history_daily" if daily else None,
            "intraday": (
                "signal_frozen_15min"
                if used_frozen_15m
                else "tradier_timesales_15min" if fetched_15m and bars_15m else None
            ),
            "intraday_5m": (
                "signal_frozen_5min"
                if used_frozen_5m
                else "tradier_timesales_5min" if fetched_5m and bars_5m else None
            ),
            "market": None,
            "sector": None,
        },
    }


def collect_point_in_time_context(
    signal: dict[str, Any], *, broker: Any = None, phase: str
) -> dict[str, Any]:
    """Fetch and normalize evidence for the existing observe-only evaluators."""
    ticker = str(signal.get("ticker") or signal.get("symbol") or "").strip().upper()
    now_utc = datetime.now(timezone.utc)
    collected_at = now_utc.isoformat()
    phase_name = str(phase).upper()
    if phase_name == "BREACH":
        evidence_now, reason = _parse_breach_as_of(signal.get("trigger_crossed_at"))
        if evidence_now is None:
            return {
                "phase": "BREACH",
                "collected_at": collected_at,
                "as_of": None,
                "data_sources": {
                    "candles": {
                        "monthly": [], "weekly": [], "daily": [],
                        "5m": [], "15m": [], "1h": [], "4h": [],
                    },
                    "trend": {"vwap": None, "current_price": None},
                    "volume": {"relative_volume": None},
                    "market": {
                        "symbol": str(signal.get("market_symbol") or "SPY"),
                        "change_pct": None,
                    },
                    "sector": {
                        "sector": signal.get("sector"),
                        "symbol": signal.get("sector_etf"),
                        "change_pct": None,
                    },
                },
                "underlying_observation": {
                    "price": None,
                    "source": None,
                    "observed_at": None,
                    "source_timestamp": None,
                    "age_seconds": None,
                },
                "errors": [f"breach_as_of_invalid:{reason}"],
                "provenance": {
                    "quote": None,
                    "daily": None,
                    "intraday": None,
                    "intraday_5m": None,
                    "market": None,
                    "sector": None,
                },
            }
        return _collect_breach_context(
            signal,
            broker=broker,
            ticker=ticker,
            evidence_now=evidence_now,
            collected_at=collected_at,
        )

    errors: list[str] = []
    quote: dict[str, Any] = {}
    daily: list[dict[str, Any]] = []
    bars_15m: list[dict[str, Any]] = []
    market_quote: dict[str, Any] = {}
    sector_quote: dict[str, Any] = {}
    if broker is not None and ticker:
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
            bars_15m = fetch_15m_bars(ticker, broker)
        except Exception as exc:
            errors.append(f"intraday_history:{type(exc).__name__}")
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

    candles_1h = _completed_intraday(bars_15m, bucket_minutes=60, now=now_utc) if bars_15m else []
    candles_4h = _completed_intraday(bars_15m, bucket_minutes=240, now=now_utc) if bars_15m else []
    today_et = now_utc.astimezone(ET).date().isoformat()
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
    data_sources = {
        "candles": {
            "monthly": _aggregate_calendar(completed_daily, "monthly", now=now_utc),
            "weekly": _aggregate_calendar(completed_daily, "weekly", now=now_utc),
            "daily": completed_daily,
            "4h": candles_4h,
            "1h": candles_1h,
        },
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
    return {
        "phase": phase_name, "collected_at": collected_at,
        "data_sources": data_sources, "underlying_observation": observation,
        "errors": errors,
        "provenance": {
            "quote": "tradier_quote" if quote else None,
            "daily": "tradier_history_daily" if daily else None,
            "intraday": "tradier_timesales_15min" if bars_15m else None,
            "market": "tradier_quote" if market_quote else None,
            "sector": "tradier_quote" if sector_quote else None,
        },
    }
