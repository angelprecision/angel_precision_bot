from __future__ import annotations

from typing import Any

from ap.score_profile_config import DEFAULT_SCORE_PROFILE_CONFIG as CONFIG
from ap.score_profile_side import normalize_signal_side

MAX_SCORE = CONFIG.vwap_context_max_points


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


def _trend_payload(market_context: dict[str, Any]) -> dict[str, Any]:
    payload = market_context.get("vwap") or market_context.get("vwap_context") or market_context.get("trend") or {}
    return payload if isinstance(payload, dict) else {}


def score_vwap_context(signal: dict[str, Any], market_context: dict[str, Any] | None = None) -> dict[str, Any]:
    sig = dict(signal or {}); ctx = market_context or {}; trend_ctx = _trend_payload(ctx)
    side = normalize_signal_side(sig.get("side") or sig.get("direction"))
    trigger = sig.get("trigger") if isinstance(sig.get("trigger"), dict) else {}
    price = _first_float(sig.get("current_price"), sig.get("underlying_price"), sig.get("entry_price"), sig.get("trigger_price"), trigger.get("entry"), trend_ctx.get("price"), trend_ctx.get("current_price"))
    entry = _first_float(sig.get("entry_price"), sig.get("trigger_price"), trigger.get("entry"), price)
    vwap = _first_float(sig.get("vwap"), trend_ctx.get("vwap"), trend_ctx.get("session_vwap"))
    chop_zone_pct = _first_float(sig.get("vwap_chop_zone_pct"), trend_ctx.get("chop_zone_pct"), CONFIG.vwap_chop_zone_pct) or CONFIG.vwap_chop_zone_pct
    sr_zone_pct = _first_float(trend_ctx.get("support_resistance_zone_pct"), CONFIG.vwap_support_resistance_zone_pct) or CONFIG.vwap_support_resistance_zone_pct
    missing: list[str] = []; warnings: list[str] = []; blocks: list[str] = []; boosts: list[str] = []; penalties: list[str] = []
    score = 0.0; distance_pct = None; aligned = None; vwap_role = None
    if side == "UNKNOWN": missing.append("side"); warnings.append("unknown_side"); blocks.append("unknown_side"); penalties.append("unknown_side")
    if price is None: missing.append("current_price")
    if vwap is None: missing.append("vwap")
    if price is not None and vwap is not None:
        distance_pct = abs(price - vwap) / abs(price) * 100.0 if price else 0.0
        if side == "UNKNOWN": warnings.append("vwap_alignment_unscored_unknown_side")
        else:
            aligned = (side == "CALL" and price > vwap) or (side == "PUT" and price < vwap)
            if aligned: score += 5; boosts.append("call_above_vwap" if side == "CALL" else "put_below_vwap")
            else: warnings.append("vwap_opposes_signal"); blocks.append("vwap_opposes_signal"); penalties.append("vwap_mismatch")
        if distance_pct <= chop_zone_pct: warnings.append("entry_too_close_to_vwap_chop_zone"); blocks.append("entry_too_close_to_vwap_chop_zone"); penalties.append("vwap_chop_zone")
        else: score += 2; boosts.append("outside_vwap_chop_zone")
        if entry is not None and side != "UNKNOWN":
            entry_distance_pct = abs(entry - vwap) / abs(entry) * 100.0 if entry else 0.0
            if side == "CALL" and entry >= vwap and entry_distance_pct <= sr_zone_pct: score += 3; vwap_role = "support"; boosts.append("vwap_support_for_call")
            elif side == "PUT" and entry <= vwap and entry_distance_pct <= sr_zone_pct: score += 3; vwap_role = "resistance"; boosts.append("vwap_resistance_for_put")
            elif side == "CALL" and entry < vwap: warnings.append("entry_below_vwap_for_call")
            elif side == "PUT" and entry > vwap: warnings.append("entry_above_vwap_for_put")
        elif entry is None: missing.append("entry_price")
    score = max(0.0, min(MAX_SCORE, score))
    return {"score": round(score, 2), "max_score": MAX_SCORE, "status": "block_recommended" if blocks else "missing_data" if missing else "ok", "missing_data": sorted(set(missing)), "warnings": sorted(set(warnings)), "block_recommendations": sorted(set(blocks)), "boosts": boosts, "penalties": penalties, "diagnostics": {"observe_only": True, "block_recommendations_are_diagnostic_only": True, "side": side, "price": price, "entry": entry, "vwap": vwap, "distance_pct": round(distance_pct, 4) if distance_pct is not None else None, "chop_zone_pct": round(chop_zone_pct, 4), "aligned_with_signal": aligned, "vwap_role": vwap_role}}
