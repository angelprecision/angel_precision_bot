from __future__ import annotations

from typing import Any

from ap.score_profile_config import DEFAULT_SCORE_PROFILE_CONFIG as CONFIG
from ap.score_profile_side import normalize_signal_side

MAX_SCORE = CONFIG.volume_confirmation_max_points


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_float(*values: Any) -> float | None:
    for value in values:
        parsed = _safe_float(value)
        if parsed is not None:
            return parsed
    return None


def _first_value(*values: Any) -> Any:
    for value in values:
        if value is not None and value != "":
            return value
    return None


def _boolish(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None or value == "":
        return None
    raw = str(value).strip().lower()
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _volume_payload(signal: dict[str, Any], market_context: dict[str, Any]) -> dict[str, Any]:
    payload = market_context.get("volume") or market_context.get("volume_context") or {}
    return payload if isinstance(payload, dict) else {}


def _latest_candle(signal: dict[str, Any], market_context: dict[str, Any]) -> dict[str, Any]:
    candle = signal.get("latest_candle") or signal.get("current_candle")
    if isinstance(candle, dict):
        return candle
    candles = market_context.get("candles") or market_context.get("ohlcv") or {}
    if isinstance(candles, dict):
        for key in ("4h", "daily", "1d"):
            rows = candles.get(key)
            if isinstance(rows, list) and rows and isinstance(rows[-1], dict):
                return rows[-1]
    return {}


def _infer_direction(signal: dict[str, Any], market_context: dict[str, Any], volume_ctx: dict[str, Any]) -> str | None:
    raw = _first_value(signal.get("volume_direction"), signal.get("volume_bias"), volume_ctx.get("direction"), volume_ctx.get("volume_direction"), volume_ctx.get("bias"))
    if raw is not None:
        value = str(raw).strip().lower()
        if value in {"bull", "bullish", "up", "green", "buy", "buying", "positive"}: return "bullish"
        if value in {"bear", "bearish", "down", "red", "sell", "selling", "negative"}: return "bearish"
        if value in {"neutral", "mixed", "flat"}: return "neutral"
    candle = _latest_candle(signal, market_context)
    open_px = _safe_float(candle.get("open")); close_px = _safe_float(candle.get("close"))
    if open_px is None or close_px is None: return None
    return "bullish" if close_px > open_px else "bearish" if close_px < open_px else "neutral"


def _infer_relative_volume(signal: dict[str, Any], market_context: dict[str, Any], volume_ctx: dict[str, Any]) -> float | None:
    rel = _first_float(signal.get("relative_volume"), signal.get("rel_volume"), signal.get("volume_ratio"), signal.get("rvol"), volume_ctx.get("relative_volume"), volume_ctx.get("rel_volume"), volume_ctx.get("volume_ratio"), volume_ctx.get("rvol"))
    if rel is not None: return rel
    candle = _latest_candle(signal, market_context)
    current = _first_float(signal.get("volume"), signal.get("current_volume"), volume_ctx.get("current_volume"), candle.get("volume"))
    average = _first_float(signal.get("avg_volume"), signal.get("average_volume"), volume_ctx.get("avg_volume"), volume_ctx.get("average_volume"))
    return current / average if current is not None and average and average > 0 else None


def _infer_breakout(signal: dict[str, Any], volume_ctx: dict[str, Any]) -> bool:
    explicit = _boolish(_first_value(signal.get("breakout"), signal.get("is_breakout"), volume_ctx.get("breakout"), volume_ctx.get("is_breakout")))
    if explicit is not None: return explicit
    pattern = str(signal.get("pattern") or signal.get("setup") or "").lower()
    return "breakout" in pattern or "breach" in pattern


def _infer_rejection(signal: dict[str, Any], volume_ctx: dict[str, Any]) -> bool:
    explicit = _boolish(_first_value(signal.get("high_volume_rejection"), signal.get("volume_rejection"), volume_ctx.get("high_volume_rejection"), volume_ctx.get("rejection"), volume_ctx.get("volume_rejection")))
    if explicit is not None: return explicit
    rejection_ratio = _first_float(signal.get("rejection_volume_ratio"), volume_ctx.get("rejection_volume_ratio"))
    return bool(rejection_ratio is not None and rejection_ratio >= CONFIG.high_volume_rejection_ratio)


def score_volume_confirmation(signal: dict[str, Any], market_context: dict[str, Any] | None = None) -> dict[str, Any]:
    sig = dict(signal or {}); ctx = market_context or {}; vctx = _volume_payload(sig, ctx)
    side = normalize_signal_side(sig.get("side") or sig.get("direction"))
    relative_volume = _infer_relative_volume(sig, ctx, vctx)
    breakout_volume_ratio = _first_float(sig.get("breakout_volume_ratio"), sig.get("breakout_volume"), sig.get("breakout_volume_thrust"), vctx.get("breakout_volume_ratio"), vctx.get("breakout_volume"), vctx.get("breakout_volume_thrust"))
    volume_direction = _infer_direction(sig, ctx, vctx)
    is_breakout = _infer_breakout(sig, vctx)
    high_volume_rejection = _infer_rejection(sig, vctx)
    missing: list[str] = []; warnings: list[str] = []; blocks: list[str] = []; boosts: list[str] = []; penalties: list[str] = []
    score = 0.0
    if side == "UNKNOWN": missing.append("side"); warnings.append("unknown_side"); blocks.append("unknown_side"); penalties.append("unknown_side")
    if relative_volume is None: missing.append("relative_volume")
    elif relative_volume >= CONFIG.relative_volume_strong: score += 4; boosts.append("relative_volume_thrust")
    elif relative_volume >= CONFIG.relative_volume_ok: score += 3; boosts.append("relative_volume_confirmed")
    elif relative_volume >= CONFIG.relative_volume_baseline: score += 2
    else: score += 0.5; penalties.append("weak_relative_volume")
    if is_breakout:
        breakout_volume_ratio = relative_volume if breakout_volume_ratio is None else breakout_volume_ratio
        if breakout_volume_ratio is None: missing.append("breakout_volume_ratio")
        elif breakout_volume_ratio >= CONFIG.breakout_volume_strong: score += 2.5; boosts.append("breakout_volume_confirmed")
        elif breakout_volume_ratio >= CONFIG.breakout_volume_min: score += 1.25
        else: warnings.append("weak_volume_breakout"); blocks.append("weak_volume_breakout"); penalties.append("breakout_without_volume")
    elif breakout_volume_ratio is not None and breakout_volume_ratio >= CONFIG.breakout_volume_strong: score += 1; boosts.append("standalone_volume_thrust")
    if volume_direction is None: missing.append("volume_direction")
    elif volume_direction == "neutral": score += 1
    elif side == "UNKNOWN": warnings.append("volume_direction_unscored_unknown_side")
    elif (side == "CALL" and volume_direction == "bullish") or (side == "PUT" and volume_direction == "bearish"): score += 2.5; boosts.append("volume_direction_matches_signal")
    else: warnings.append("volume_direction_opposes_signal"); blocks.append("volume_direction_opposes_signal"); penalties.append("volume_direction_mismatch")
    if high_volume_rejection:
        warnings.append("high_volume_rejection"); blocks.append("high_volume_rejection"); penalties.append("high_volume_rejection"); score -= 2 if relative_volume is not None and relative_volume >= CONFIG.relative_volume_ok else 1
    score = max(0.0, min(MAX_SCORE, score))
    return {"score": round(score, 2), "max_score": MAX_SCORE, "status": "block_recommended" if blocks else "missing_data" if missing else "ok", "missing_data": sorted(set(missing)), "warnings": sorted(set(warnings)), "block_recommendations": sorted(set(blocks)), "boosts": boosts, "penalties": penalties, "diagnostics": {"observe_only": True, "block_recommendations_are_diagnostic_only": True, "side": side, "relative_volume": round(relative_volume, 4) if relative_volume is not None else None, "breakout_volume_ratio": round(breakout_volume_ratio, 4) if breakout_volume_ratio is not None else None, "is_breakout": bool(is_breakout), "volume_direction": volume_direction, "high_volume_rejection": bool(high_volume_rejection)}}
