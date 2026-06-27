from __future__ import annotations

import math
from datetime import date, datetime
from decimal import Decimal
from typing import Any

CONTEXT_VERSION = "market_context_v1"

_CORE_TIMEFRAMES: tuple[str, ...] = ("monthly", "weekly", "daily", "4h")
_OPTIONAL_TIMEFRAMES: tuple[str, ...] = ("2d", "3d", "4d", "5d")
_ALL_TIMEFRAMES: tuple[str, ...] = _CORE_TIMEFRAMES + _OPTIONAL_TIMEFRAMES

_TIMEFRAME_ALIASES: dict[str, tuple[str, ...]] = {
    "monthly": ("monthly", "month", "1mo", "1m", "monthly_candles"),
    "weekly": ("weekly", "week", "1w", "weekly_candles"),
    "daily": ("daily", "day", "1d", "daily_candles"),
    "4h": ("4h", "4hr", "4hour", "four_hour", "four_hour_candles", "4h_candles"),
    "2d": ("2d", "2day", "two_day", "two_day_candles", "2d_candles"),
    "3d": ("3d", "3day", "three_day", "three_day_candles", "3d_candles"),
    "4d": ("4d", "4day", "four_day", "four_day_candles", "4d_candles"),
    "5d": ("5d", "5day", "five_day", "five_day_candles", "5d_candles"),
}

_LEVEL_KEYS: tuple[str, ...] = (
    "scanner_entry",
    "scanner_stop",
    "scanner_target",
    "monthly_high",
    "monthly_low",
    "weekly_high",
    "weekly_low",
    "daily_high",
    "daily_low",
    "four_hour_high",
    "four_hour_low",
)

_TREND_KEYS: tuple[str, ...] = ("vwap", "ema_stack", "price_above_vwap")
_VOLUME_KEYS: tuple[str, ...] = ("relative_volume", "volume_ratio")
_SECTOR_KEYS: tuple[str, ...] = ("sector", "sector_direction", "sector_green", "sector_red")


def build_market_context_for_signal(
    signal: dict[str, Any], *, data_sources: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Build an observe-only, JSON-safe market context snapshot for a signal.

    The builder only copies data that is already present on the provided signal or
    optional in-memory data_sources payload. It does not fetch, infer, or fill
    missing candles.
    """

    signal_snapshot = signal if isinstance(signal, dict) else {}
    sources_snapshot = data_sources if isinstance(data_sources, dict) else {}
    missing_data: list[str] = []
    data_sources_used: list[str] = []

    candles: dict[str, list[Any]] = {}
    timeframes_available: list[str] = []
    for timeframe in _ALL_TIMEFRAMES:
        raw_candles, source_name = _find_candles(timeframe, signal_snapshot, sources_snapshot)
        candle_list = _coerce_candle_list(raw_candles)
        candles[timeframe] = _json_safe(candle_list)
        if candle_list:
            timeframes_available.append(timeframe)
            if source_name:
                data_sources_used.append(source_name)
        elif timeframe in _CORE_TIMEFRAMES:
            missing_data.append(f"candles.{timeframe}")

    levels = _build_levels(signal_snapshot, sources_snapshot, missing_data, data_sources_used)
    trend = _build_group("trend", _TREND_KEYS, signal_snapshot, sources_snapshot, data_sources_used)
    volume = _build_group("volume", _VOLUME_KEYS, signal_snapshot, sources_snapshot, data_sources_used)
    sector = _build_group("sector", _SECTOR_KEYS, signal_snapshot, sources_snapshot, data_sources_used)

    context = {
        "context_version": CONTEXT_VERSION,
        "ticker": _json_safe(_first_present(signal_snapshot, ("ticker", "symbol", "underlying", "root_symbol"))),
        "timeframes_available": timeframes_available,
        "candles": candles,
        "levels": levels,
        "trend": trend,
        "volume": volume,
        "sector": sector,
        "missing_data": sorted(set(missing_data)),
        "diagnostics": {
            "observe_only": True,
            "data_sources_used": sorted(set(data_sources_used)),
        },
    }
    return _json_safe(context)


def _find_candles(
    timeframe: str, signal: dict[str, Any], data_sources: dict[str, Any]
) -> tuple[Any, str | None]:
    aliases = _TIMEFRAME_ALIASES[timeframe]

    source_candles = data_sources.get("candles")
    if isinstance(source_candles, dict):
        value = _first_present(source_candles, aliases)
        if value is not None:
            return value, f"data_sources.candles.{timeframe}"

    value = _first_present(data_sources, aliases)
    if value is not None:
        return value, f"data_sources.{timeframe}"

    signal_candles = signal.get("candles")
    if isinstance(signal_candles, dict):
        value = _first_present(signal_candles, aliases)
        if value is not None:
            return value, f"signal.candles.{timeframe}"

    value = _first_present(signal, aliases)
    if value is not None:
        return value, f"signal.{timeframe}"

    return None, None


def _coerce_candle_list(raw: Any) -> list[Any]:
    if raw is None or callable(raw):
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, tuple):
        return list(raw)
    return []


def _build_levels(
    signal: dict[str, Any], data_sources: dict[str, Any], missing_data: list[str], data_sources_used: list[str]
) -> dict[str, Any]:
    source_levels = data_sources.get("levels") if isinstance(data_sources.get("levels"), dict) else {}
    signal_levels = signal.get("levels") if isinstance(signal.get("levels"), dict) else {}

    level_sources = [source_levels, signal_levels, data_sources, signal]
    trigger = signal.get("trigger") if isinstance(signal.get("trigger"), dict) else {}

    candidates: dict[str, tuple[str, ...]] = {
        "scanner_entry": ("scanner_entry", "entry", "entry_price", "entry_trigger", "trigger_price", "underlying_entry"),
        "scanner_stop": ("scanner_stop", "stop", "stop_price", "stop_loss", "stop_underlying", "underlying_stop"),
        "scanner_target": ("scanner_target", "target", "target_price", "target_underlying", "underlying_target", "pt1", "pt2"),
        "monthly_high": ("monthly_high", "month_high"),
        "monthly_low": ("monthly_low", "month_low"),
        "weekly_high": ("weekly_high", "week_high"),
        "weekly_low": ("weekly_low", "week_low"),
        "daily_high": ("daily_high", "day_high"),
        "daily_low": ("daily_low", "day_low"),
        "four_hour_high": ("four_hour_high", "4h_high", "four_hour_h", "four_hour_bar_high"),
        "four_hour_low": ("four_hour_low", "4h_low", "four_hour_l", "four_hour_bar_low"),
    }

    levels: dict[str, Any] = {}
    for key in _LEVEL_KEYS:
        value = None
        for source in level_sources:
            value = _first_present(source, candidates[key])
            if value is not None:
                if source is source_levels:
                    data_sources_used.append("data_sources.levels")
                elif source is data_sources:
                    data_sources_used.append(f"data_sources.{key}")
                elif source is signal_levels:
                    data_sources_used.append("signal.levels")
                elif source is signal:
                    data_sources_used.append(f"signal.{key}")
                break
        if value is None and key == "scanner_entry":
            value = _first_present(trigger, ("entry", "trigger", "price"))
            if value is not None:
                data_sources_used.append("signal.trigger")
        elif value is None and key == "scanner_stop":
            value = _first_present(trigger, ("stop", "stop_price"))
            if value is not None:
                data_sources_used.append("signal.trigger")
        elif value is None and key == "scanner_target":
            value = _first_present(trigger, ("target", "target_price", "pt1", "pt2"))
            if value is not None:
                data_sources_used.append("signal.trigger")

        levels[key] = _json_safe(value)
        if value is None and key in {"scanner_entry", "scanner_stop", "scanner_target"}:
            missing_data.append(f"levels.{key}")
    return levels


def _build_group(
    group_name: str,
    keys: tuple[str, ...],
    signal: dict[str, Any],
    data_sources: dict[str, Any],
    data_sources_used: list[str],
) -> dict[str, Any]:
    grouped = data_sources.get(group_name) if isinstance(data_sources.get(group_name), dict) else {}
    signal_grouped = signal.get(group_name) if isinstance(signal.get(group_name), dict) else {}
    result: dict[str, Any] = {}
    for key in keys:
        value = _first_present(grouped, (key,))
        if value is not None:
            data_sources_used.append(f"data_sources.{group_name}")
        if value is None:
            value = _first_present(data_sources, (key,))
            if value is not None:
                data_sources_used.append(f"data_sources.{key}")
        if value is None:
            value = _first_present(signal_grouped, (key,))
            if value is not None:
                data_sources_used.append(f"signal.{group_name}")
        if value is None:
            value = _first_present(signal, (key,))
            if value is not None:
                data_sources_used.append(f"signal.{key}")
        result[key] = _json_safe(value)
    return result


def _first_present(mapping: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key in mapping and mapping[key] is not None and mapping[key] != "":
            return mapping[key]
        lower_key = key.lower()
        upper_key = key.upper()
        if lower_key in mapping and mapping[lower_key] is not None and mapping[lower_key] != "":
            return mapping[lower_key]
        if upper_key in mapping and mapping[upper_key] is not None and mapping[upper_key] != "":
            return mapping[upper_key]
    return None


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Decimal):
        as_float = float(value)
        return as_float if math.isfinite(as_float) else None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(_json_safe(key)): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    return str(value)
