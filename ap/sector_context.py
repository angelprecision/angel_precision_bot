from __future__ import annotations

from typing import Any

from ap.score_profile_config import DEFAULT_SCORE_PROFILE_CONFIG as CONFIG
from ap.score_profile_side import normalize_signal_side

MAX_SCORE = CONFIG.sector_context_max_points


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "": return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_float(*values: Any) -> float | None:
    for value in values:
        parsed = _safe_float(value)
        if parsed is not None: return parsed
    return None


def _first_value(*values: Any) -> Any:
    for value in values:
        if value is not None and value != "": return value
    return None


def _context_payload(market_context: dict[str, Any], *keys: str) -> dict[str, Any]:
    for key in keys:
        value = market_context.get(key)
        if isinstance(value, dict): return value
    return {}


def _direction_from_payload(payload: dict[str, Any], change_pct: float | None) -> str | None:
    raw = _first_value(payload.get("direction"), payload.get("bias"), payload.get("trend"))
    if raw is not None:
        value = str(raw).strip().lower()
        if value in {"green", "bull", "bullish", "up", "positive", "risk_on"}: return "green"
        if value in {"red", "bear", "bearish", "down", "negative", "risk_off"}: return "red"
        if value in {"flat", "neutral", "mixed"}: return "neutral"
    if change_pct is None: return None
    return "green" if change_pct > 0 else "red" if change_pct < 0 else "neutral"


def score_sector_context(signal: dict[str, Any], market_context: dict[str, Any] | None = None) -> dict[str, Any]:
    sig = dict(signal or {}); ctx = market_context or {}
    sector_ctx = _context_payload(ctx, "sector", "sector_context"); market_ctx = _context_payload(ctx, "market", "market_context", "index")
    side = normalize_signal_side(sig.get("side") or sig.get("direction"))
    sector_name = _first_value(sig.get("sector"), sector_ctx.get("sector"), sector_ctx.get("name"), sector_ctx.get("symbol"))
    sector_change_pct = _first_float(sig.get("sector_change_pct"), sector_ctx.get("change_pct"), sector_ctx.get("sector_change_pct"), sector_ctx.get("day_change_pct"))
    market_change_pct = _first_float(sig.get("market_change_pct"), sig.get("spy_change_pct"), market_ctx.get("change_pct"), market_ctx.get("spy_change_pct"), market_ctx.get("day_change_pct"))
    sector_direction = _direction_from_payload(sector_ctx, sector_change_pct); market_direction = _direction_from_payload(market_ctx, market_change_pct)
    missing: list[str] = []; warnings: list[str] = []; blocks: list[str] = []; boosts: list[str] = []; penalties: list[str] = []
    score = 0.0
    if side == "UNKNOWN": missing.append("side"); warnings.append("unknown_side"); blocks.append("unknown_side"); penalties.append("unknown_side")
    if sector_direction is None: missing.append("sector_direction")
    elif side == "UNKNOWN": warnings.append("sector_direction_unscored_unknown_side")
    else:
        sector_aligns = (side == "CALL" and sector_direction == "green") or (side == "PUT" and sector_direction == "red")
        if sector_aligns:
            score += 3; boosts.append("call_sector_green" if side == "CALL" else "put_sector_red")
            if sector_change_pct is not None and abs(sector_change_pct) >= 0.75: score += 1; boosts.append("sector_move_confirmed")
        elif sector_direction == "neutral": score += 1
        else: warnings.append("signal_direction_opposes_sector"); blocks.append("signal_direction_opposes_sector"); penalties.append("sector_opposes_signal")
    if market_direction is None: missing.append("market_direction")
    elif side == "UNKNOWN": warnings.append("market_direction_unscored_unknown_side")
    else:
        market_aligns = (side == "CALL" and market_direction == "green") or (side == "PUT" and market_direction == "red")
        if market_aligns: score += 1; boosts.append("market_direction_confirms_signal")
        elif market_direction != "neutral": warnings.append("signal_direction_opposes_market"); penalties.append("market_opposes_signal")
    if sector_name is None: missing.append("sector_name")
    score = max(0.0, min(MAX_SCORE, score))
    return {"score": round(score, 2), "max_score": MAX_SCORE, "status": "block_recommended" if blocks else "missing_data" if missing else "ok", "missing_data": sorted(set(missing)), "warnings": sorted(set(warnings)), "block_recommendations": sorted(set(blocks)), "boosts": boosts, "penalties": penalties, "diagnostics": {"observe_only": True, "block_recommendations_are_diagnostic_only": True, "side": side, "sector_name": sector_name, "sector_change_pct": sector_change_pct, "sector_direction": sector_direction, "market_change_pct": market_change_pct, "market_direction": market_direction}}
