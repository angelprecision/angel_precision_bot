from __future__ import annotations

from typing import Any

from ap.fair_value_gap import evaluate_fvg_context
from ap.score_profile_config import DEFAULT_SCORE_PROFILE_CONFIG as CONFIG
from ap.score_profile_side import normalize_signal_side
from ap.score_profile_types import PositionScoreProfile, ScoreComponent, clamp, grade_from_score
from ap.the_strat_confluence import evaluate_higher_timeframe_confluence
from ap.trigger_geometry import score_remaining_opportunity, score_trigger_geometry

PROFILE_VERSION = "position_score_profile_v1_observe_only"


def _f(v: Any) -> float | None:
    try:
        return None if v is None or v == "" else float(v)
    except (TypeError, ValueError):
        return None


def _c(name: str, score: float, max_score: float, status: str = "ok", reason: str = "", details: dict[str, Any] | None = None) -> ScoreComponent:
    return ScoreComponent(name, max(0.0, min(float(max_score), float(score or 0))), float(max_score), status, reason, details or {})


def _from_result(name: str, result: dict[str, Any], reason: str) -> ScoreComponent:
    return _c(name, result.get("score", 0), result.get("max_score", 0), result.get("status", "ok"), result.get("reason", reason), dict(result or {}))


def _collect(component: ScoreComponent, missing: list[str], blocks: list[str]) -> None:
    missing.extend(component.details.get("missing_data", []))
    blocks.extend(component.details.get("block_recommendations", []))


def build_position_score_profile(signal: dict[str, Any], market_context: dict[str, Any] | None = None) -> dict[str, Any]:
    ctx = market_context or {}
    sig = dict(signal or {})
    side = normalize_signal_side(sig.get("side") or sig.get("direction"))

    # UNKNOWN side: fail closed — cannot score a directionless signal
    if side == "UNKNOWN":
        return PositionScoreProfile(
            profile_version=PROFILE_VERSION, total_score=0.0, base_score=0.0,
            bonus_score=0.0, penalty_score=0.0, grade="REJECT",
            client_eligible_recommendation=False, components={},
            missing_data=["side"], block_recommendations=["unknown_signal_side"],
            diagnostics={
                "observe_only": True, "live_behavior_changed": False,
                "score_source": "diagnostic_position_profile",
                "side": "UNKNOWN", "early_return": "unknown_side",
                "market_context_keys": sorted(ctx.keys()),
            },
            observe_only=True,
            warnings=["unknown_signal_side"],
        ).to_dict()

    # Track whether a meaningful context was supplied so we do not
    # double-penalise callers that ran the profile without enriched data.
    context_provided = bool(
        ctx and (ctx.get("candles") or ctx.get("levels") or ctx.get("trend") or ctx.get("volume"))
    )

    components: dict[str, ScoreComponent] = {}
    missing: list[str] = []
    blocks: list[str] = []
    diagnostics = {"observe_only": True, "live_behavior_changed": False, "score_source": "diagnostic_position_profile"}

    components["scanner_quality"] = _score_scanner_quality(sig)
    for name, result, reason in (
        ("trigger_geometry", score_trigger_geometry(sig, side), "trigger geometry"),
        ("remaining_opportunity", score_remaining_opportunity(sig, side), "remaining opportunity"),
    ):
        component = _from_result(name, result, reason)
        components[name] = component
        _collect(component, missing, blocks)
    opportunity = components["remaining_opportunity"]

    htf = evaluate_higher_timeframe_confluence(sig, ctx)
    components["higher_timeframe_confluence"] = _from_result("higher_timeframe_confluence", htf, "monthly/weekly/daily/4h confluence")
    missing.extend(htf.get("missing_data", [])); blocks.extend(htf.get("block_recommendations", []))
    fvg = evaluate_fvg_context(sig, ctx)
    components["fair_value_gap"] = _from_result("fair_value_gap", fvg, "4h/daily FVG context")
    missing.extend(fvg.get("missing_data", [])); blocks.extend(fvg.get("block_recommendations", []))

    for component in (_score_price_stacking(sig, ctx, htf, fvg), _score_volume_confirmation(sig, ctx), _score_trend_vwap(sig, ctx, side)):
        components[component.name] = component
        _collect(component, missing, blocks)
    volume = components["volume_confirmation"]

    for component in (_score_contract_execution_quality(sig), _score_historical_feedback(sig)):
        components[component.name] = component
        _collect(component, missing, blocks)
    contract = components["contract_execution_quality"]

    base = sum(c.score for c in components.values())
    bonus, bonus_reasons = _bonus_points(htf, fvg, opportunity, volume)
    penalty, penalty_reasons = _penalties(blocks, missing, contract, context_provided=context_provided)
    total = clamp(base + bonus - penalty, 0, 120)
    # Aggregate warnings from every scored component into a single top-level list.
    # Surfaces actionable cautions without requiring callers to walk the components tree.
    agg_warnings: list[str] = sorted(set(
        w
        for c in components.values()
        for w in (c.details.get("warnings") or [])
    ))
    diagnostics.update({"side": side, "bonus_reasons": bonus_reasons, "penalty_reasons": penalty_reasons, "component_total_before_bonus_penalty": round(base, 2), "market_context_keys": sorted(ctx.keys()), "context_provided": context_provided})
    return PositionScoreProfile(
        PROFILE_VERSION, total, base, bonus, penalty,
        grade_from_score(total),
        bool(total >= 80 and not blocks),
        components, sorted(set(missing)), sorted(set(blocks)), diagnostics,
        observe_only=True,
        warnings=agg_warnings,
    ).to_dict()


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
    if isinstance(meta.get("score_audit"), dict):
        meta["score_audit"]["position_score_profile"] = profile
    return profile


def _score_scanner_quality(sig: dict[str, Any]) -> ScoreComponent:
    raw = _f(sig.get("score") or sig.get("scanner_score") or sig.get("ev_score") or sig.get("score_total"))
    if raw is None:
        return _c("scanner_quality", 0, 15, "missing_data", "scanner score missing", {"missing_data": ["scanner_score"], "warnings": []})
    return _c("scanner_quality", (raw - 55) / 45 * 15, 15, reason=f"scanner score {raw:.1f}", details={"raw_score": raw, "warnings": []})


def _score_price_stacking(sig, ctx, htf, fvg):
    trigger = _f(sig.get("trigger_price") or sig.get("entry_price") or (sig.get("trigger") or {}).get("entry"))
    if trigger is None:
        return _c("price_stacking", 0, CONFIG.price_stacking_max_points, "missing_data", "trigger missing", {"missing_data": ["trigger_price"], "warnings": []})
    matched = []
    for name, raw in (ctx.get("levels") or sig.get("levels") or {}).items():
        for value in raw if isinstance(raw, list) else [raw]:
            px = _f(value)
            dist = abs(px - trigger) / abs(trigger) * 100 if px is not None and trigger else None
            if dist is not None and dist <= CONFIG.price_stacking_tolerance_pct:
                matched.append({"name": str(name), "price": px, "distance_pct": round(dist, 4)})
    aligned = htf.get("aligned_timeframes", []) or []
    fvg_aligned = bool((fvg.get("diagnostics") or {}).get("aligned_support_or_resistance"))
    return _c("price_stacking", len(matched) * 2 + len(aligned) + (2 if fvg_aligned else 0), CONFIG.price_stacking_max_points, reason=f"{len(matched)} nearby levels, {len(aligned)} aligned tfs", details={"matched_levels": matched, "aligned_timeframes": aligned, "fvg_aligned": fvg_aligned, "warnings": []})


def _score_volume_confirmation(sig, ctx):
    vctx = ctx.get("volume") or {}
    rel = _f(sig.get("relative_volume") or sig.get("volume_ratio") or vctx.get("relative_volume") or vctx.get("volume_ratio"))
    if rel is None:
        return _c("volume_confirmation", 0, CONFIG.volume_confirmation_max_points, "missing_data", "relative volume missing", {"missing_data": ["relative_volume"], "warnings": []})
    pts = 10 if rel >= CONFIG.relative_volume_strong else 8 if rel >= CONFIG.relative_volume_ok else 5 if rel >= CONFIG.relative_volume_baseline else 2
    return _c("volume_confirmation", pts, CONFIG.volume_confirmation_max_points, reason=f"relative_volume={rel:.2f}", details={"relative_volume": round(rel, 4), "warnings": []})


def _score_trend_vwap(sig, ctx, side):
    price = _f(sig.get("current_price") or sig.get("underlying_price") or sig.get("entry_price"))
    trend = ctx.get("trend") or {}
    vwap = _f(sig.get("vwap") or trend.get("vwap"))
    ema_stack = str(sig.get("ema_stack") or trend.get("ema_stack") or "").lower()
    missing, warnings, blocks, reasons, pts = [], [], [], [], 0.0
    if side == "UNKNOWN":
        missing.append("side"); warnings.append("unknown_side"); blocks.append("unknown_side")
    if price is None: missing.append("current_price")
    if vwap is None: missing.append("vwap")
    elif side == "CALL" and price is not None and price >= vwap: pts += 4; reasons.append("above_vwap_for_call")
    elif side == "PUT" and price is not None and price <= vwap: pts += 4; reasons.append("below_vwap_for_put")
    elif side != "UNKNOWN": warnings.append("vwap_opposes_signal"); reasons.append("vwap_opposes_signal")
    if ema_stack in {"bull", "bullish", "up", "stacked_up"} and side == "CALL": pts += 6; reasons.append("ema_stack_bullish")
    elif ema_stack in {"bear", "bearish", "down", "stacked_down"} and side == "PUT": pts += 6; reasons.append("ema_stack_bearish")
    elif ema_stack in {"neutral", "mixed", "flat"}: pts += 2; reasons.append("ema_stack_neutral")
    elif not ema_stack: missing.append("ema_stack")
    elif side != "UNKNOWN": warnings.append("ema_stack_opposes_signal"); reasons.append("ema_stack_opposes_signal")
    return _c("trend_vwap_alignment", pts, CONFIG.vwap_context_max_points, "block_recommended" if blocks else "missing_data" if missing else "ok", ", ".join(reasons) if reasons else "trend/vwap missing", {"missing_data": missing, "warnings": sorted(set(warnings)), "block_recommendations": blocks, "price": price, "vwap": vwap, "ema_stack": ema_stack})


def _score_contract_execution_quality(sig):
    vals = {"spread_pct": _f(sig.get("spread_pct") or sig.get("bid_ask_spread_pct")), "delta": _f(sig.get("delta") or sig.get("option_delta")), "open_interest": _f(sig.get("open_interest") or sig.get("option_open_interest")), "option_volume": _f(sig.get("option_volume") or sig.get("daily_volume_options")), "dte": _f(sig.get("dte"))}
    missing = [k for k, v in vals.items() if v is None]; blocks = []; pts = 0.0
    spread, delta, oi, opt_vol, dte = vals["spread_pct"], vals["delta"], vals["open_interest"], vals["option_volume"], vals["dte"]
    if spread is not None: pts += 3 if spread <= 0.08 else 2 if spread <= 0.12 else 0; blocks += ["spread_too_wide"] if spread > 0.12 else []
    if delta is not None: pts += 3 if 0.30 <= abs(delta) <= 0.55 else 1.5 if 0.20 <= abs(delta) <= 0.70 else 0; blocks += ["delta_outside_quality_band"] if not 0.20 <= abs(delta) <= 0.70 else []
    if oi is not None: pts += 2 if oi >= 1000 else 1 if oi >= 500 else 0; blocks += ["open_interest_low"] if oi < 500 else []
    if opt_vol is not None: pts += 1 if opt_vol >= 250 else 0.5 if opt_vol >= 100 else 0
    if dte is not None:
        if dte >= 1:
            pts += 1
        elif dte < 0:
            missing.append("dte_invalid")
        # dte == 0: primary trading mode (0DTE) — no bonus, no block
    return _c("contract_execution_quality", pts, 10, "block_recommended" if blocks else "missing_data" if missing else "ok", "contract quality", {"missing_data": missing, "warnings": blocks, "block_recommendations": blocks, **vals})


def _score_historical_feedback(sig):
    win, n, avg = _f(sig.get("win_rate") or sig.get("historical_win_rate")), _f(sig.get("sample_size") or sig.get("historical_sample_size") or sig.get("n")), _f(sig.get("avg_opt_ret") or sig.get("avg_option_return"))
    missing = [k for k, v in (("win_rate", win), ("sample_size", n)) if v is None]
    if win is None or n is None or n < 5:
        return _c("historical_feedback", 0, 5, "missing_data", "history missing or sample small", {"missing_data": missing, "warnings": [], "win_rate": win, "sample_size": n, "avg_option_return": avg})
    pts = (3 if win >= 0.75 else 2 if win >= 0.60 else 1 if win >= 0.50 else 0) + (2 if avg is not None and avg >= 0.15 else 1 if avg is not None and avg >= 0.05 else 1 if avg is None and n >= 20 else 0)
    return _c("historical_feedback", pts, 5, reason="historical feedback", details={"warnings": [], "win_rate": win, "sample_size": n, "avg_option_return": avg})


def _bonus_points(htf, fvg, opportunity, volume):
    points, reasons = 0.0, []
    if {"monthly", "weekly", "daily"}.issubset(set(htf.get("aligned_timeframes") or [])): points += 5; reasons.append("monthly_weekly_daily_aligned")
    if (fvg.get("diagnostics") or {}).get("aligned_support_or_resistance") and not (fvg.get("diagnostics") or {}).get("entry_inside_opposing_fvg"): points += 5; reasons.append("clean_aligned_fvg")
    if volume.details.get("relative_volume") is not None and volume.details["relative_volume"] >= CONFIG.relative_volume_strong: points += 5; reasons.append("volume_thrust")
    if opportunity.details.get("remaining_r") is not None and opportunity.details["remaining_r"] >= 2: points += 5; reasons.append("two_r_or_better_remaining")
    return min(20, points), reasons


def _penalties(block_recommendations, missing_data, contract, *, context_provided: bool = True):
    points, reasons = 0.0, []
    for key, value in (("monthly_weekly_oppose_signal", 15), ("entry_inside_opposing_fvg", 12), ("target_into_opposing_fvg", 8)):
        if key in block_recommendations: points += value; reasons.append(key)
    if any("remaining" in r or "target_already" in r for r in block_recommendations): points += 10; reasons.append("insufficient_remaining_opportunity")
    if "unknown_side" in block_recommendations: points += 10; reasons.append("unknown_side")
    if contract.status == "block_recommended": points += 10; reasons.append("contract_quality_block_recommended")
    if context_provided:
        required = [m for m in missing_data if m in {"monthly_candles", "weekly_candles", "daily_candles", "4h_candles", "relative_volume"}]
        if required:
            points += min(15, len(set(required)) * 3)
            reasons.append("required_context_missing")
    return min(35, points), reasons
