from __future__ import annotations

import math
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable

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
_NEWS_EARNINGS_KEYS: tuple[str, ...] = ("earnings_date", "earnings_risk", "news_risk", "catalyst")
_SCREENSHOT_CONTEXT_KEYS: tuple[str, ...] = (
    "screenshot_context",
    "screenshot_annotations",
    "chart_screenshot",
    "screenshot",
)

_OHLC_KEYS: tuple[str, ...] = ("open", "high", "low", "close")


try:
    from ap.data_quality_context import evaluate_data_quality_context as _DATA_QUALITY_EVALUATOR
except Exception:  # pragma: no cover - optional alignment with #209A
    try:
        from ap.data_quality_context import score_data_quality_context as _DATA_QUALITY_EVALUATOR
    except Exception:  # pragma: no cover - optional alignment with #209A
        _DATA_QUALITY_EVALUATOR = None


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
    warnings: list[str] = []
    data_sources_used: list[str] = []

    candles: dict[str, list[Any]] = {}
    timeframes_available: list[str] = []
    for timeframe in _ALL_TIMEFRAMES:
        raw_candles, source_name = _find_candles(timeframe, signal_snapshot, sources_snapshot)
        candle_list = _coerce_candle_list(raw_candles, timeframe, warnings)
        candles[timeframe] = _json_safe(candle_list)
        _record_candle_quality(timeframe, candle_list, warnings)

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
    news_earnings = _build_group(
        "news_earnings", _NEWS_EARNINGS_KEYS, signal_snapshot, sources_snapshot, data_sources_used
    )
    screenshot_context = _find_screenshot_context(signal_snapshot, sources_snapshot, data_sources_used)

    _record_optional_context_quality(trend, volume, sector, missing_data)

    context = {
        "context_version": CONTEXT_VERSION,
        "ticker": _json_safe(_first_present(signal_snapshot, ("ticker", "symbol", "underlying", "root_symbol"))),
        "timeframes_available": timeframes_available,
        "candles": candles,
        "levels": levels,
        "trend": trend,
        "volume": volume,
        "sector": sector,
        "news_earnings": news_earnings,
        "screenshot_context": _json_safe(screenshot_context),
        "missing_data": sorted(set(missing_data)),
        "warnings": sorted(set(warnings)),
        "diagnostics": {
            "observe_only": True,
            "data_sources_used": sorted(set(data_sources_used)),
            "fake_data_used": False,
        },
    }

    _merge_external_data_quality(context, _DATA_QUALITY_EVALUATOR)
    context["missing_data"] = sorted(set(context.get("missing_data", [])))
    context["warnings"] = sorted(set(context.get("warnings", [])))
    return _json_safe(context)


def _find_candles(
    timeframe: str, signal: dict[str, Any], data_sources: dict[str, Any]
) -> tuple[Any, str | None]:
    aliases = _TIMEFRAME_ALIASES[timeframe]

    source_contexts = _context_candidates(data_sources, "data_sources")
    for source_name, source in source_contexts:
        source_candles = source.get("candles")
        if isinstance(source_candles, dict):
            value = _first_present(source_candles, aliases)
            if value is not None:
                return value, f"{source_name}.candles.{timeframe}"

    for source_name, source in source_contexts:
        value = _first_present(source, aliases)
        if value is not None:
            return value, f"{source_name}.{timeframe}"

    signal_contexts = _context_candidates(signal, "signal")
    for source_name, source in signal_contexts:
        source_candles = source.get("candles")
        if isinstance(source_candles, dict):
            value = _first_present(source_candles, aliases)
            if value is not None:
                return value, f"{source_name}.candles.{timeframe}"

    for source_name, source in signal_contexts:
        value = _first_present(source, aliases)
        if value is not None:
            return value, f"{source_name}.{timeframe}"

    return None, None


def _coerce_candle_list(raw: Any, timeframe: str, warnings: list[str]) -> list[Any]:
    if raw is None or callable(raw):
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, tuple):
        return list(raw)

    warnings.append(f"bad_candle_container:candles.{timeframe}")
    return []


def _record_candle_quality(timeframe: str, candles: list[Any], warnings: list[str]) -> None:
    for index, candle in enumerate(candles):
        if not isinstance(candle, dict):
            warnings.append(f"bad_ohlc_shape:candles.{timeframe}[{index}]")
            continue

        missing_ohlc = [key for key in _OHLC_KEYS if _first_present(candle, (key,)) is None]
        if missing_ohlc:
            warnings.append(f"bad_ohlc_shape:candles.{timeframe}[{index}]")

        for key in _OHLC_KEYS:
            value = _first_present(candle, (key,))
            if value is not None and not _is_number_like(value):
                warnings.append(f"bad_ohlc_value:candles.{timeframe}[{index}].{key}")


def _build_levels(
    signal: dict[str, Any], data_sources: dict[str, Any], missing_data: list[str], data_sources_used: list[str]
) -> dict[str, Any]:
    source_levels = data_sources.get("levels") if isinstance(data_sources.get("levels"), dict) else {}
    signal_levels = signal.get("levels") if isinstance(signal.get("levels"), dict) else {}
    context_levels = _first_context_group(data_sources, "levels", "data_sources")
    signal_context_levels = _first_context_group(signal, "levels", "signal")

    level_sources = [
        ("data_sources.levels", source_levels),
        ("data_sources.context.levels", context_levels),
        ("signal.levels", signal_levels),
        ("signal.context.levels", signal_context_levels),
        ("data_sources", data_sources),
        ("signal", signal),
    ]
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
        for source_name, source in level_sources:
            value = _first_present(source, candidates[key])
            if value is not None:
                data_sources_used.append(source_name if "." in source_name else f"{source_name}.{key}")
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
    data_source_grouped = data_sources.get(group_name) if isinstance(data_sources.get(group_name), dict) else {}
    signal_grouped = signal.get(group_name) if isinstance(signal.get(group_name), dict) else {}
    context_grouped = _first_context_group(data_sources, group_name, "data_sources")
    signal_context_grouped = _first_context_group(signal, group_name, "signal")

    grouped_sources = [
        (f"data_sources.{group_name}", data_source_grouped),
        (f"data_sources.context.{group_name}", context_grouped),
        (f"signal.{group_name}", signal_grouped),
        (f"signal.context.{group_name}", signal_context_grouped),
        ("data_sources", data_sources),
        ("signal", signal),
    ]

    result: dict[str, Any] = {}
    for key in keys:
        value = None
        for source_name, source in grouped_sources:
            value = _first_present(source, (key,))
            if value is not None:
                data_sources_used.append(source_name if "." in source_name else f"{source_name}.{key}")
                break
        result[key] = _json_safe(value)
    return result


def _find_screenshot_context(
    signal: dict[str, Any], data_sources: dict[str, Any], data_sources_used: list[str]
) -> Any:
    for source_name, source in [
        ("data_sources", data_sources),
        ("data_sources.context", _first_context(data_sources, "data_sources")),
        ("signal", signal),
        ("signal.context", _first_context(signal, "signal")),
    ]:
        value = _first_present(source, _SCREENSHOT_CONTEXT_KEYS)
        if value is not None:
            data_sources_used.append(f"{source_name}.screenshot_context")
            return value
    return None


def _record_optional_context_quality(
    trend: dict[str, Any], volume: dict[str, Any], sector: dict[str, Any], missing_data: list[str]
) -> None:
    if trend.get("vwap") is None:
        missing_data.append("trend.vwap")
    if volume.get("relative_volume") is None and volume.get("volume_ratio") is None:
        missing_data.append("volume.relative_volume")
    if sector.get("sector") is None and sector.get("sector_direction") is None:
        missing_data.append("sector.sector")


def _merge_external_data_quality(
    context: dict[str, Any], evaluator: Callable[[dict[str, Any]], dict[str, Any]] | None
) -> None:
    if evaluator is None:
        return

    try:
        quality = evaluator(context)
    except Exception as exc:  # pragma: no cover - defensive diagnostics only
        context.setdefault("warnings", []).append(f"data_quality_context_failed:{exc}")
        return

    if not isinstance(quality, dict):
        return

    for key in ("missing_data", "warnings"):
        values = quality.get(key)
        if isinstance(values, list):
            context.setdefault(key, []).extend(str(item) for item in values)

    for key in ("bad_shape", "stale_data"):
        values = quality.get(key)
        if isinstance(values, list):
            context.setdefault("warnings", []).extend(f"{key}:{item}" for item in values)

    diagnostics = context.setdefault("diagnostics", {})
    diagnostics["data_quality_context"] = _json_safe(quality)


def _context_candidates(mapping: dict[str, Any], root_name: str) -> list[tuple[str, dict[str, Any]]]:
    candidates: list[tuple[str, dict[str, Any]]] = []
    for key in ("market_context", "context", "provided_context"):
        value = mapping.get(key)
        if isinstance(value, dict):
            candidates.append((f"{root_name}.{key}", value))
    candidates.append((root_name, mapping))
    return candidates


def _first_context_group(mapping: dict[str, Any], group_name: str, root_name: str) -> dict[str, Any]:
    for _, context in _context_candidates(mapping, root_name):
        if context is mapping:
            continue
        grouped = context.get(group_name)
        if isinstance(grouped, dict):
            return grouped
    return {}


def _first_context(mapping: dict[str, Any], root_name: str) -> dict[str, Any]:
    for _, context in _context_candidates(mapping, root_name):
        if context is not mapping:
            return context
    return {}


def _first_present(mapping: dict[str, Any], keys: tuple[str, ...]) -> Any:
    if not isinstance(mapping, dict):
        return None

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


def _is_number_like(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, (int, float, Decimal)):
        return True
    if isinstance(value, str):
        try:
            float(value)
        except ValueError:
            return False
        return True
    return False


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
