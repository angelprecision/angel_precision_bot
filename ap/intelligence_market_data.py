from __future__ import annotations

from typing import Any, Optional


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
        or to_float(signal.get("entry_price"))
        or to_float(signal.get("trigger_price"))
        or to_float(signal.get("trigger"))
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
