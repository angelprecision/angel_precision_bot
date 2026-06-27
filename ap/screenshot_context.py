"""
Screenshot context normalization.

This module is intentionally evidence-only:
- no broker submit
- no broker cancel
- no order mutation
- no position mutation
- no proof_trades mutation
- no trade_queue mutation
- no approval/blocking authority

It only converts raw chart screenshot annotations into a stable diagnostic shape.
"""

from __future__ import annotations

import math
from typing import Any

CONTEXT_VERSION = "screenshot_context_v1"
VALID_VISUAL_BIASES = {"bullish", "bearish", "neutral", "mixed", "unclear"}
TIMEFRAME_ALIASES = {
    "1day": "daily",
    "1d": "daily",
    "daily": "daily",
    "day": "daily",
    "1hour": "1h",
    "hourly": "1h",
    "4hour": "4h",
    "4hr": "4h",
    "60m": "1h",
    "30min": "30m",
    "15min": "15m",
}


def _blank_context(
    *,
    available: bool,
    missing_data: list[str] | None = None,
    warnings: list[str] | None = None,
    diagnostics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    clean_diagnostics = {
        "observe_only": True,
        "trading_authority": False,
        "active_gate": False,
        "can_approve_trade": False,
        "can_block_trade": False,
    }
    if diagnostics:
        clean_diagnostics.update(diagnostics)

    return {
        "context_version": CONTEXT_VERSION,
        "available": bool(available),
        "visual_bias": "unclear",
        "timeframes": {},
        "missing_data": list(missing_data or []),
        "warnings": list(warnings or []),
        "diagnostics": clean_diagnostics,
    }


def _as_mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return []


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_timeframe(value: Any) -> str | None:
    text = _clean_text(value)
    if not text:
        return None

    normalized = text.lower().replace(" ", "").replace("_", "").replace("-", "")
    return TIMEFRAME_ALIASES.get(normalized, text.lower().strip())


def _normalize_visual_bias(value: Any, warnings: list[str]) -> str:
    text = _clean_text(value)
    if not text:
        warnings.append("visual_bias_missing")
        return "unclear"

    normalized = text.lower()
    if normalized in {"bull", "long", "call", "calls"}:
        return "bullish"
    if normalized in {"bear", "short", "put", "puts"}:
        return "bearish"
    if normalized in VALID_VISUAL_BIASES:
        return normalized

    warnings.append("visual_bias_unrecognized")
    return "unclear"


def _safe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None

    if not math.isfinite(result):
        return None
    return result


def _normalize_confidence(value: Any, warnings: list[str]) -> float | None:
    confidence = _safe_float(value)
    if confidence is None:
        return None

    if 0.0 <= confidence <= 1.0:
        return confidence

    if 1.0 < confidence <= 100.0:
        warnings.append("confidence_percent_converted")
        return confidence / 100.0

    warnings.append("confidence_out_of_range")
    return None


def _normalize_levels(value: Any, warnings: list[str], field_name: str) -> list[Any]:
    levels = []
    for item in _as_list(value):
        if isinstance(item, dict):
            clean_item = dict(item)
            numeric_level = _safe_float(
                clean_item.get("level")
                if "level" in clean_item
                else clean_item.get("price")
            )
            if numeric_level is not None:
                clean_item["level"] = numeric_level
            levels.append(clean_item)
            continue

        numeric = _safe_float(item)
        if numeric is not None:
            levels.append(numeric)

    if value not in (None, []) and not levels:
        warnings.append(f"{field_name}_unusable")
    return levels


def _normalize_fvg_zones(value: Any, warnings: list[str]) -> list[dict[str, Any]]:
    zones: list[dict[str, Any]] = []
    for item in _as_list(value):
        if not isinstance(item, dict):
            continue

        zone = dict(item)
        low = _safe_float(zone.get("low"))
        high = _safe_float(zone.get("high"))
        top = _safe_float(zone.get("top"))
        bottom = _safe_float(zone.get("bottom"))

        if low is None and bottom is not None:
            low = bottom
        if high is None and top is not None:
            high = top

        if low is not None:
            zone["low"] = low
        if high is not None:
            zone["high"] = high

        zones.append(zone)

    if value not in (None, []) and not zones:
        warnings.append("fvg_zones_unusable")
    return zones


def _timeframe_context(raw: dict[str, Any], warnings: list[str]) -> dict[str, Any]:
    return {
        "support_levels": _normalize_levels(
            raw.get("support_levels"),
            warnings,
            "support_levels",
        ),
        "resistance_levels": _normalize_levels(
            raw.get("resistance_levels"),
            warnings,
            "resistance_levels",
        ),
        "fvg_zones": _normalize_fvg_zones(
            raw.get("fvg_zones", raw.get("detected_fvg_zones")),
            warnings,
        ),
        "trend_label": _clean_text(raw.get("trend_label")),
        "confidence": _normalize_confidence(raw.get("confidence"), warnings),
    }


def normalize_screenshot_context(raw: dict | None) -> dict:
    """Normalize raw screenshot annotations into observe-only diagnostics.

    Screenshot data is evidence only. This function never returns an approval,
    never returns a rejection, and never mutates broker/order/position/proof/queue
    state.
    """

    if not raw:
        return _blank_context(
            available=False,
            missing_data=["screenshot_context"],
            warnings=["screenshot_context_missing"],
        )

    if not isinstance(raw, dict):
        return _blank_context(
            available=False,
            missing_data=["screenshot_context"],
            warnings=["screenshot_context_invalid_type"],
        )

    warnings: list[str] = []
    missing_data: list[str] = []

    visual_bias = _normalize_visual_bias(raw.get("visual_bias"), warnings)
    image_url = _clean_text(raw.get("image_url") or raw.get("screenshot_url"))
    if not image_url:
        missing_data.append("image_url")
        warnings.append("image_url_missing")

    timeframes: dict[str, dict[str, Any]] = {}

    nested_timeframes = raw.get("timeframes")
    if isinstance(nested_timeframes, dict):
        for timeframe_key, timeframe_raw in nested_timeframes.items():
            timeframe = _normalize_timeframe(timeframe_key)
            if not timeframe:
                warnings.append("timeframe_missing")
                continue
            timeframe_payload = _timeframe_context(_as_mapping(timeframe_raw), warnings)
            timeframes[timeframe] = timeframe_payload
    else:
        timeframe = _normalize_timeframe(raw.get("timeframe"))
        if timeframe:
            timeframes[timeframe] = _timeframe_context(raw, warnings)
        else:
            missing_data.append("timeframe")
            warnings.append("timeframe_missing")

    if not timeframes:
        missing_data.append("timeframes")

    diagnostics = {
        "observe_only": True,
        "trading_authority": False,
        "active_gate": False,
        "can_approve_trade": False,
        "can_block_trade": False,
        "image_url_present": bool(image_url),
        "ticker": _clean_text(raw.get("ticker")),
        "signal_id": _clean_text(raw.get("signal_id") or raw.get("canonical_signal_id")),
        "client_id": _clean_text(raw.get("client_id")),
        "execution_mode": _clean_text(raw.get("execution_mode")),
        "operator_notes_present": bool(_clean_text(raw.get("operator_notes"))),
    }

    return {
        "context_version": CONTEXT_VERSION,
        "available": True,
        "visual_bias": visual_bias,
        "timeframes": timeframes,
        "missing_data": missing_data,
        "warnings": warnings,
        "diagnostics": diagnostics,
    }
