from __future__ import annotations

from typing import Any

from ap.fair_value_gap import evaluate_fvg_context
from ap.score_profile_types import PositionScoreProfile, ScoreComponent, clamp, grade_from_score
from ap.the_strat_confluence import evaluate_higher_timeframe_confluence

PROFILE_VERSION = "position_score_profile_v1_observe_only"


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_side(side: Any) -> str:
    raw = str(side or "").upper().strip()
    if raw in {"CALL", "BUY", "LONG", "BULLISH", "CALLS"}:
        return "CALL"
    if raw in {"PUT", "BEARISH", "PUTS"}:
        return "PUT"
    return raw or "CALL"


def _component(name: str, score: float, max_score: float, *, reason: str = "", status: str = "ok", details: dict[str, Any] | None = None) -> ScoreComponent:
    return ScoreComponent(name=name, score=max(0.0, min(float(max_score), float(score or 0))), max_score=float(max_score), status=status, reason=reason, details=details or {})


def build_position_score_profile(signal: dict[str, Any], market_context: dict[str, Any] | None = None) -> dict[str, Any]:
    ctx = market_context or {}
    signal_snapshot = dict(signal or {})
    side = _normalize_side(signal_snapshot.get("side") or signal_snapshot.get("direction"))
    components: dict[str, ScoreComponent] = {}
    missing_data: list[str] = []
    block_recommendations: list[str] = []
    diagnostics: dict[str, Any] = {"observe_only": True, "live_behavior_changed": False, "score_source": "diagnostic_position_profile"}

    components["scanner_quality"] = _score_scanner_quality(signal_snapshot)

    geometry = _score_trigger_geometry(signal_snapshot, side=side)
    components["trigger_geometry"] = geometry
    missing_data.extend(geometry.details.get("missing_data", []))
    block_recommendations.extend(geometry.details.get("block_recommendations", []))

    opportunity = _score_remaining_opportunity(signal_snapshot, side=side)
    components["remaining_opportunity"] = opportunity
    missing_data.extend(opportunity.details.get("missing_data", []))
    block_recommendations.extend(opportunity.details.get("block_recommendations", []))

    htf = evaluate_higher_timeframe_confluence(signal_snapshot, ctx)
    components["higher_timeframe_confluence"] = _component("higher_timeframe_confluence", htf["score"], htf["max_score"], status=htf["status"], reason="monthly/weekly/daily/4h confluence", details=htf)
    missing_data.extend(htf.get("missing_data", []))
    block_recommendations.extend(htf.get("block_recommendations", []))

    fvg = evaluate_fvg_context(signal_snapshot, ctx)
    components["fair_value_gap"] = _component("fair_value_gap", fvg["score"], fvg["max_score"], status=fvg["status"], reason="4h/daily FVG context", details=fvg)
    missing_data.extend(fvg.get("missing_data", []))
    block_recommendations.extend(fvg.get("block_recommendations", []))

    stacking = _score_price_stacking(signal_snapshot, ctx, htf, fvg)
    components["price_stacking"] = stacking
    missing_data.extend(stacking.details.get("missing_data", []))

    volume = _score_volume_confirmation(signal_snapshot, ctx)
    components["volume_confirmation"] = volume
    missing_data.extend(volume.details.get("missing_data", []))

    trend = _score_trend_vwap(signal_snapshot, ctx, side=side)
    components["trend_vwap_alignment"] = trend
    missing_data.extend(trend.details.get("missing_data", []))

    contract = _score_contract_execution_quality(signal_snapshot)
    components["contract_execution_quality"] = contract
    missing_data.extend(contract.details.get("missing_data", []))
    block_recommendations.extend(contract.details.get("block_recommendations", []))

    historical = _score_historical_feedback(signal_snapshot)
    components["historical_feedback"] = historical
    missing_data.extend(historical.details.get("missing_data", []))

    base_score = sum(c.score for c in components.values())
    bonus_score, bonus_reasons = _bonus_points(htf=htf, fvg=fvg, opportunity=opportunity, volume=volume)
    penalty_score, penalty_reasons = _penalties(block_recommendations=block_recommendations, missing_data=missing_data, contract=contract)
    total = clamp(base_score + bonus_score - penalty_score, 0, 120)
    diagnostics.update({"side": side, "bonus_reasons": bonus_reasons, "penalty_reasons": penalty_reasons, "component_total_before_bonus_penalty": round(base_score, 2), "market_context_keys": sorted((ctx or {}).keys())})
    profile = PositionScoreProfile(PROFILE_VERSION, total, base_score, bonus_score, penalty_score, grade_from_score(total), bool(total >= 80 and not set(block_recommendations)), components, sorted(set(missing_data)), sorted(set(block_recommendations)), diagnostics)
    return profile.to_dict()


def attach_position_score_profile(plan: Any, signal: dict[str, Any], market_context: dict[str, Any] | None = None) -> dict[str, Any]:
    profile = build_position_score_profile(signal, market_context)
    meta = getattr(plan, "metadata", None)
    if meta is None:
        meta = {}
        try:
            plan.metadata = meta
        except Exception:
            return profile
    meta["position_score_profile"] = profile
    score_audit = meta.get("score_audit")
    if isinstance(score_audit, dict):
        score_audit["position_score_profile"] = profile
    return profile


def _score_scanner_quality(signal: dict[str, Any]) -> ScoreComponent:
    score = _safe_float(signal.get("score") or signal.get("scanner_score") or signal.get("ev_score") or signal.get("score_total"))
    if score is None:
        return _component("scanner_quality", 0.0, 15.0, status="missing_data", reason="scanner score missing", details={"missing_data": ["scanner_score"]})
    pts = max(0.0, min(15.0, (score - 55.0) / 45.0 * 15.0))
    return _component("scanner_quality", pts, 15.0, reason=f"scanner score {score:.1f}", details={"raw_score": score})


def _score_trigger_geometry(signal: dict[str, Any], *, side: str) -> ScoreComponent:
    trigger = signal.get("trigger") or {}
    entry = _safe_float(signal.get("entry_price") or signal.get("trigger_price") or trigger.get("entry"))
    stop = _safe_float(signal.get("stop_price") or signal.get("stop_underlying") or trigger.get("stop"))
    target = _safe_float(signal.get("target_price") or signal.get("target_underlying") or trigger.get("pt1") or trigger.get("pt2"))
    missing = [name for name, value in (("entry", entry), ("stop", stop), ("target", target)) if value is None]
    if missing:
        return _component("trigger_geometry", 0.0, 10.0, status="missing_data", reason="missing geometry", details={"missing_data": [f"geometry_{m}" for m in missing], "block_recommendations": []})
    blocks: list[str] = []
    if side == "CALL" and stop >= entry:
        blocks.append("call_stop_not_below_entry")
    if side == "CALL" and target <= entry:
        blocks.append("call_target_not_above_entry")
    if side == "PUT" and stop <= entry:
        blocks.append("put_stop_not_above_entry")
    if side == "PUT" and target >= entry:
        blocks.append("put_target_not_below_entry")
    if blocks:
        return _component("trigger_geometry", 0.0, 10.0, status="block_recommended", reason="invalid geometry", details={"entry": entry, "stop": stop, "target": target, "block_recommendations": blocks})
    risk = abs(entry - stop)
    reward = abs(target - entry)
    rr = reward / risk if risk > 0 else 0.0
    pts = 10.0 if rr >= 2.0 else 7.0 if rr >= 1.25 else 4.0 if rr >= 0.75 else 1.0
    if rr < 0.75:
        blocks.append("poor_underlying_reward_to_risk")
    return _component("trigger_geometry", pts, 10.0, reason=f"geometry valid rr={rr:.2f}", details={"entry": entry, "stop": stop, "target": target, "underlying_reward_to_risk": round(rr, 3), "block_recommendations": blocks})


def _score_remaining_opportunity(signal: dict[str, Any], *, side: str) -> ScoreComponent:
    trigger = signal.get("trigger") or {}
    current = _safe_float(signal.get("current_price") or signal.get("underlying_price") or signal.get("entry_price") or trigger.get("entry"))
    target = _safe_float(signal.get("target_price") or signal.get("target_underlying") or trigger.get("pt1") or trigger.get("pt2"))
    stop = _safe_float(signal.get("stop_price") or signal.get("stop_underlying") or trigger.get("stop"))
    missing = [name for name, value in (("current_price", current), ("target", target), ("stop", stop)) if value is None]
    if missing:
        return _component("remaining_opportunity", 0.0, 10.0, status="missing_data", reason="missing current/target/stop", details={"missing_data": missing, "block_recommendations": []})
    if side == "CALL":
        remaining_move = target - current
        risk = current - stop
    else:
        remaining_move = current - target
        risk = stop - current
    blocks: list[str] = []
    remaining_pct = remaining_move / current * 100.0 if current else 0.0
    remaining_r = remaining_move / risk if risk and risk > 0 else 0.0
    if remaining_move <= 0:
        blocks.append("target_already_reached_or_wrong_side")
    if remaining_pct < 0.25:
        blocks.append("remaining_move_pct_too_small")
    if remaining_r < 0.75:
        blocks.append("remaining_r_too_small")
    pts = 10.0 if remaining_r >= 2.0 and remaining_pct >= 0.75 else 7.5 if remaining_r >= 1.25 else 4.0 if remaining_r >= 0.75 else 0.0
    return _component("remaining_opportunity", pts, 10.0, status="block_recommended" if blocks else "ok", reason=f"remaining_move_pct={remaining_pct:.2f} remaining_r={remaining_r:.2f}", details={"current_price": current, "target": target, "stop": stop, "remaining_move": round(remaining_move, 4), "remaining_move_pct": round(remaining_pct, 4), "remaining_r": round(remaining_r, 4), "block_recommendations": blocks})


def _score_price_stacking(signal: dict[str, Any], ctx: dict[str, Any], htf: dict[str, Any], fvg: dict[str, Any]) -> ScoreComponent:
    levels = ctx.get("levels") or signal.get("levels") or {}
    trigger = _safe_float(signal.get("trigger_price") or signal.get("entry_price") or (signal.get("trigger") or {}).get("entry"))
    if trigger is None:
        return _component("price_stacking", 0.0, 10.0, status="missing_data", reason="trigger missing", details={"missing_data": ["trigger_price"]})
    matched: list[dict[str, Any]] = []
    for level_name, raw in (levels or {}).items():
        values = raw if isinstance(raw, list) else [raw]
        for value in values:
            px = _safe_float(value)
            if px is None or trigger == 0:
                continue
            dist_pct = abs(px - trigger) / abs(trigger) * 100.0
            if dist_pct <= 0.35:
                matched.append({"name": str(level_name), "price": px, "distance_pct": round(dist_pct, 4)})
    aligned_tfs = htf.get("aligned_timeframes", []) or []
    fvg_aligned = bool((fvg.get("diagnostics") or {}).get("aligned_support_or_resistance"))
    pts = min(10.0, len(matched) * 2.0 + len(aligned_tfs) * 1.0 + (2.0 if fvg_aligned else 0.0))
    return _component("price_stacking", pts, 10.0, reason=f"{len(matched)} nearby levels, {len(aligned_tfs)} aligned tfs", details={"matched_levels": matched, "aligned_timeframes": aligned_tfs, "fvg_aligned": fvg_aligned})


def _score_volume_confirmation(signal: dict[str, Any], ctx: dict[str, Any]) -> ScoreComponent:
    vol_ratio = _safe_float(signal.get("relative_volume") or signal.get("volume_ratio") or (ctx.get("volume") or {}).get("relative_volume") or (ctx.get("volume") or {}).get("volume_ratio"))
    if vol_ratio is None:
        return _component("volume_confirmation", 0.0, 10.0, status="missing_data", reason="relative volume missing", details={"missing_data": ["relative_volume"]})
    pts = 10.0 if vol_ratio >= 1.8 else 8.0 if vol_ratio >= 1.4 else 5.0 if vol_ratio >= 1.0 else 2.0
    return _component("volume_confirmation", pts, 10.0, reason=f"relative_volume={vol_ratio:.2f}", details={"relative_volume": round(vol_ratio, 4)})


def _score_trend_vwap(signal: dict[str, Any], ctx: dict[str, Any], *, side: str) -> ScoreComponent:
    price = _safe_float(signal.get("current_price") or signal.get("underlying_price") or signal.get("entry_price"))
    vwap = _safe_float(signal.get("vwap") or (ctx.get("trend") or {}).get("vwap"))
    ema_stack = str(signal.get("ema_stack") or (ctx.get("trend") or {}).get("ema_stack") or "").lower()
    missing = []
    pts = 0.0
    reasons = []
    if price is None:
        missing.append("current_price")
    if vwap is None:
        missing.append("vwap")
    elif price is not None:
        if side == "CALL" and price >= vwap:
            pts += 4.0
            reasons.append("above_vwap_for_call")
        elif side == "PUT" and price <= vwap:
            pts += 4.0
            reasons.append("below_vwap_for_put")
        else:
            reasons.append("vwap_opposes_signal")
    if ema_stack:
        if side == "CALL" and ema_stack in {"bull", "bullish", "up", "stacked_up"}:
            pts += 6.0
            reasons.append("ema_stack_bullish")
        elif side == "PUT" and ema_stack in {"bear", "bearish", "down", "stacked_down"}:
            pts += 6.0
            reasons.append("ema_stack_bearish")
        elif ema_stack in {"neutral", "mixed", "flat"}:
            pts += 2.0
            reasons.append("ema_stack_neutral")
        else:
            reasons.append("ema_stack_opposes_signal")
    else:
        missing.append("ema_stack")
    return _component("trend_vwap_alignment", pts, 10.0, status="missing_data" if missing else "ok", reason=", ".join(reasons) if reasons else "trend/vwap missing", details={"missing_data": missing, "price": price, "vwap": vwap, "ema_stack": ema_stack})


def _score_contract_execution_quality(signal: dict[str, Any]) -> ScoreComponent:
    spread = _safe_float(signal.get("spread_pct") or signal.get("bid_ask_spread_pct"))
    delta = _safe_float(signal.get("delta") or signal.get("option_delta"))
    oi = _safe_float(signal.get("open_interest") or signal.get("option_open_interest"))
    opt_vol = _safe_float(signal.get("option_volume") or signal.get("daily_volume_options"))
    dte = _safe_float(signal.get("dte"))
    missing = []
    blocks: list[str] = []
    pts = 0.0
    if spread is None:
        missing.append("spread_pct")
    elif spread <= 0.08:
        pts += 3
    elif spread <= 0.12:
        pts += 2
    else:
        blocks.append("spread_too_wide")
    if delta is None:
        missing.append("delta")
    elif 0.30 <= abs(delta) <= 0.55:
        pts += 3
    elif 0.20 <= abs(delta) <= 0.70:
        pts += 1.5
    else:
        blocks.append("delta_outside_quality_band")
    if oi is None:
        missing.append("open_interest")
    elif oi >= 1000:
        pts += 2
    elif oi >= 500:
        pts += 1
    else:
        blocks.append("open_interest_low")
    if opt_vol is None:
        missing.append("option_volume")
    elif opt_vol >= 250:
        pts += 1
    elif opt_vol >= 100:
        pts += 0.5
    if dte is None:
        missing.append("dte")
    elif dte >= 1:
        pts += 1
    else:
        blocks.append("zero_dte_or_unknown_quality")
    return _component("contract_execution_quality", pts, 10.0, status="block_recommended" if blocks else "missing_data" if missing else "ok", reason="contract quality", details={"missing_data": missing, "block_recommendations": blocks, "spread_pct": spread, "delta": delta, "open_interest": oi, "option_volume": opt_vol, "dte": dte})


def _score_historical_feedback(signal: dict[str, Any]) -> ScoreComponent:
    win_rate = _safe_float(signal.get("win_rate") or signal.get("historical_win_rate"))
    sample_size = _safe_float(signal.get("sample_size") or signal.get("historical_sample_size") or signal.get("n"))
    avg_ret = _safe_float(signal.get("avg_opt_ret") or signal.get("avg_option_return"))
    missing = []
    if win_rate is None:
        missing.append("win_rate")
    if sample_size is None:
        missing.append("sample_size")
    if win_rate is None or sample_size is None or sample_size < 5:
        return _component("historical_feedback", 0.0, 5.0, status="missing_data", reason="history missing or sample small", details={"missing_data": missing, "win_rate": win_rate, "sample_size": sample_size, "avg_option_return": avg_ret})
    pts = 3.0 if win_rate >= 0.75 else 2.0 if win_rate >= 0.60 else 1.0 if win_rate >= 0.50 else 0.0
    if avg_ret is not None:
        pts += 2.0 if avg_ret >= 0.15 else 1.0 if avg_ret >= 0.05 else 0.0
    elif sample_size >= 20:
        pts += 1.0
    return _component("historical_feedback", pts, 5.0, reason="historical feedback", details={"win_rate": win_rate, "sample_size": sample_size, "avg_option_return": avg_ret})


def _bonus_points(*, htf: dict[str, Any], fvg: dict[str, Any], opportunity: ScoreComponent, volume: ScoreComponent) -> tuple[float, list[str]]:
    points = 0.0
    reasons: list[str] = []
    aligned = set(htf.get("aligned_timeframes") or [])
    if {"monthly", "weekly", "daily"}.issubset(aligned):
        points += 5.0
        reasons.append("monthly_weekly_daily_aligned")
    fvg_diag = fvg.get("diagnostics") or {}
    if fvg_diag.get("aligned_support_or_resistance") and not fvg_diag.get("entry_inside_opposing_fvg"):
        points += 5.0
        reasons.append("clean_aligned_fvg")
    rel_vol = volume.details.get("relative_volume")
    if rel_vol is not None and rel_vol >= 1.8:
        points += 5.0
        reasons.append("volume_thrust")
    rem_r = opportunity.details.get("remaining_r")
    if rem_r is not None and rem_r >= 2.0:
        points += 5.0
        reasons.append("two_r_or_better_remaining")
    return min(20.0, points), reasons


def _penalties(*, block_recommendations: list[str], missing_data: list[str], contract: ScoreComponent) -> tuple[float, list[str]]:
    points = 0.0
    reasons: list[str] = []
    if "monthly_weekly_oppose_signal" in block_recommendations:
        points += 15.0
        reasons.append("monthly_weekly_oppose_signal")
    if "entry_inside_opposing_fvg" in block_recommendations:
        points += 12.0
        reasons.append("entry_inside_opposing_fvg")
    if "target_into_opposing_fvg" in block_recommendations:
        points += 8.0
        reasons.append("target_into_opposing_fvg")
    if any("remaining" in r or "target_already" in r for r in block_recommendations):
        points += 10.0
        reasons.append("insufficient_remaining_opportunity")
    if contract.status == "block_recommended":
        points += 10.0
        reasons.append("contract_quality_block_recommended")
    required_missing = [m for m in missing_data if m in {"monthly_candles", "weekly_candles", "daily_candles", "4h_candles", "relative_volume"}]
    if required_missing:
        points += min(15.0, len(set(required_missing)) * 3.0)
        reasons.append("required_context_missing")
    return min(35.0, points), reasons
