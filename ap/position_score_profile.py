from __future__ import annotations

from typing import Any

from ap.fair_value_gap import evaluate_fvg_context
from ap.score_profile_config import DEFAULT_SCORE_PROFILE_CONFIG as CONFIG
from ap.score_profile_side import normalize_signal_side
from ap.score_profile_types import PositionScoreProfile, ScoreComponent, clamp, grade_from_score
from ap.sector_context import score_sector_context
from ap.the_strat_confluence import evaluate_higher_timeframe_confluence
from ap.trigger_geometry import score_remaining_opportunity, score_trigger_geometry
from ap.volume_confirmation import score_volume_confirmation
from ap.vwap_context import score_vwap_context

PROFILE_VERSION = "position_score_profile_v2_observe_only"
RANK_EXCLUDED_COMPONENTS = ("historical_feedback",)

_UPSTREAM_MAX_SCORES = {
    "sector_context": 5.0,
    "volume_confirmation": 10.0,
    "vwap_context": 10.0,
    "fair_value_gap": 15.0,
    "the_strat_confluence": 18.0,
    "trigger_geometry": 10.0,
}


def _f(v: Any) -> float | None:
    try:
        return None if v is None or v == "" else float(v)
    except (TypeError, ValueError):
        return None


def _c(name: str, score: float, max_score: float, status: str = "ok", reason: str = "", details: dict[str, Any] | None = None) -> ScoreComponent:
    return ScoreComponent(name, max(0.0, min(float(max_score), float(score or 0))), float(max_score), status, reason, details or {})


def _fold(components: dict[str, ScoreComponent], missing: list[str], blocks: list[str], name: str, result: dict[str, Any], reason: str) -> ScoreComponent:
    component = _c(name, result["score"], result["max_score"], result.get("status", "ok"), result.get("reason", reason), result)
    components[name] = component
    missing.extend(result.get("missing_data", []))
    blocks.extend(result.get("block_recommendations", []))
    return component


def _upstream_or_run(ctx: dict[str, Any], name: str, fallback) -> dict[str, Any]:
    """Use the already-evaluated module result instead of scoring it twice."""

    upstream = ctx.get("_intelligence_upstream")
    entry = upstream.get(name) if isinstance(upstream, dict) else None
    if not isinstance(entry, dict) or not entry.get("provided"):
        return fallback()
    raw = entry.get("raw")
    if isinstance(raw, dict):
        return dict(raw)
    available = bool(entry.get("available"))
    score = _f(entry.get("score")) if available else None
    reason = entry.get("error") or entry.get("missing_reason") or "upstream_result_unavailable"
    return {
        "score": score or 0.0,
        "max_score": _UPSTREAM_MAX_SCORES[name],
        "status": "ok" if score is not None else "missing_data",
        "missing_data": [] if score is not None else [f"{name}:{reason}"],
        "block_recommendations": [],
        "warnings": [str(reason)] if reason else [],
        "diagnostics": {"source": "intelligence_upstream", "available": available},
    }


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
    _fold(
        components, missing, blocks, "trigger_geometry",
        _upstream_or_run(ctx, "trigger_geometry", lambda: score_trigger_geometry(sig, side)),
        "trigger geometry",
    )
    opportunity = _fold(components, missing, blocks, "remaining_opportunity", score_remaining_opportunity(sig, side), "remaining opportunity")

    htf = _upstream_or_run(
        ctx, "the_strat_confluence",
        lambda: evaluate_higher_timeframe_confluence(sig, ctx),
    )
    _fold(components, missing, blocks, "higher_timeframe_confluence", htf, "monthly/weekly/daily/4h confluence")
    fvg = _upstream_or_run(ctx, "fair_value_gap", lambda: evaluate_fvg_context(sig, ctx))
    _fold(components, missing, blocks, "fair_value_gap", fvg, "4h/daily FVG context")

    stacking = _score_price_stacking(sig, ctx, htf, fvg)
    components["price_stacking"] = stacking
    missing.extend(stacking.details.get("missing_data", []))

    volume = _fold(
        components, missing, blocks, "volume_confirmation",
        _upstream_or_run(ctx, "volume_confirmation", lambda: score_volume_confirmation(sig, ctx)),
        "relative/breakout volume confirmation",
    )
    _fold(
        components, missing, blocks, "vwap_context",
        _upstream_or_run(ctx, "vwap_context", lambda: score_vwap_context(sig, ctx)),
        "VWAP alignment and chop-zone diagnostics",
    )
    _fold(
        components, missing, blocks, "sector_context",
        _upstream_or_run(ctx, "sector_context", lambda: score_sector_context(sig, ctx)),
        "sector and broad-market direction diagnostics",
    )

    contract = _score_contract_execution_quality(sig)
    components["contract_execution_quality"] = contract
    missing.extend(contract.details.get("missing_data", [])); blocks.extend(contract.details.get("block_recommendations", []))
    historical = _score_historical_feedback(sig)
    components["historical_feedback"] = historical
    missing.extend(historical.details.get("missing_data", []))

    # Historical aggregates are useful context, but without immutable
    # point-in-time lineage they can include results learned after this signal.
    # Keep the diagnostic visible while excluding it from every ranking value.
    base = sum(
        c.score for name, c in components.items()
        if name not in RANK_EXCLUDED_COMPONENTS
    )
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
    diagnostics.update({
        "side": side,
        "bonus_reasons": bonus_reasons,
        "penalty_reasons": penalty_reasons,
        "component_total_before_bonus_penalty": round(base, 2),
        "rank_excluded_components": list(RANK_EXCLUDED_COMPONENTS),
        "market_context_keys": sorted(ctx.keys()),
        "context_provided": context_provided,
    })
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


def _score_scanner_quality(sig):
    raw = _f(sig.get("score") or sig.get("scanner_score") or sig.get("ev_score") or sig.get("score_total"))
    if raw is None:
        return _c("scanner_quality", 0, 15, "missing_data", "scanner score missing", {"missing_data": ["scanner_score"], "warnings": []})
    return _c("scanner_quality", (raw - 55) / 45 * 15, 15, reason=f"scanner score {raw:.1f}", details={"raw_score": raw, "warnings": []})


def _score_price_stacking(sig, ctx, htf, fvg):
    raw_trigger = sig.get("trigger")
    scalar_trigger = raw_trigger if not isinstance(raw_trigger, dict) else None
    trigger_payload = raw_trigger if isinstance(raw_trigger, dict) else {}
    trigger = _f(
        sig.get("trigger_price")
        or scalar_trigger
        or trigger_payload.get("entry")
        or sig.get("underlying_entry_price")
        or sig.get("entry_price")
    )
    if trigger is None:
        return _c("price_stacking", 0, CONFIG.price_stacking_max_points, "missing_data", "trigger missing", {"missing_data": ["trigger_price"], "warnings": []})
    matched = []
    seen_prices: set[float] = set()
    ignored_geometry_levels: list[str] = []
    for name, raw in (ctx.get("levels") or sig.get("levels") or {}).items():
        normalized_name = str(name).strip().lower().replace("-", "_").replace(" ", "_")
        if any(token in normalized_name for token in ("entry", "trigger", "stop", "target")):
            ignored_geometry_levels.append(str(name))
            continue
        for value in raw if isinstance(raw, list) else [raw]:
            px = _f(value)
            dist = abs(px - trigger) / abs(trigger) * 100 if px is not None and trigger else None
            price_key = round(px, 6) if px is not None else None
            if (
                dist is not None
                and dist <= CONFIG.price_stacking_tolerance_pct
                and price_key not in seen_prices
            ):
                seen_prices.add(price_key)
                matched.append({"name": str(name), "price": px, "distance_pct": round(dist, 4)})
    aligned = htf.get("aligned_timeframes", []) or []
    fvg_aligned = bool((fvg.get("diagnostics") or {}).get("aligned_support_or_resistance"))
    return _c("price_stacking", len(matched) * 2 + len(aligned) + (2 if fvg_aligned else 0), CONFIG.price_stacking_max_points, reason=f"{len(matched)} nearby levels, {len(aligned)} aligned tfs", details={"matched_levels": matched, "ignored_geometry_levels": sorted(set(ignored_geometry_levels)), "aligned_timeframes": aligned, "fvg_aligned": fvg_aligned, "warnings": []})


def _score_contract_execution_quality(sig):
    vals = {"spread_pct": _f(sig.get("spread_pct") or sig.get("bid_ask_spread_pct")), "delta": _f(sig.get("delta") or sig.get("option_delta")), "open_interest": _f(sig.get("open_interest") or sig.get("option_open_interest")), "option_volume": _f(sig.get("option_volume") or sig.get("daily_volume_options")), "dte": _f(sig.get("dte"))}
    missing = [k for k, v in vals.items() if v is None]
    blocks, pts = [], 0.0
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
    return _c("contract_execution_quality", pts, 10, "block_recommended" if blocks else "missing_data" if missing else "ok", "contract quality", {"missing_data": missing, "block_recommendations": blocks, "warnings": blocks, **vals})


def _score_historical_feedback(sig):
    win, n, avg = _f(sig.get("win_rate") or sig.get("historical_win_rate")), _f(sig.get("sample_size") or sig.get("historical_sample_size") or sig.get("n")), _f(sig.get("avg_opt_ret") or sig.get("avg_option_return"))
    lineage = {
        "data_as_of": sig.get("historical_data_as_of"),
        "policy_version": sig.get("historical_policy_version"),
        "rank_excluded": True,
        "rank_exclusion_reason": "historical_feedback_not_point_in_time_safe",
    }
    missing = [k for k, v in (("win_rate", win), ("sample_size", n)) if v is None]
    if win is None or n is None or n < 5:
        return _c("historical_feedback", 0, 5, "diagnostic_only", "history missing or sample small", {"missing_data": missing, "win_rate": win, "sample_size": n, "avg_option_return": avg, "warnings": [], **lineage})
    pts = (3 if win >= 0.75 else 2 if win >= 0.60 else 1 if win >= 0.50 else 0) + (2 if avg is not None and avg >= 0.15 else 1 if avg is not None and avg >= 0.05 else 1 if avg is None and n >= 20 else 0)
    return _c("historical_feedback", pts, 5, "diagnostic_only", "historical feedback (rank excluded)", {"win_rate": win, "sample_size": n, "avg_option_return": avg, "warnings": [], **lineage})


def _bonus_points(htf, fvg, opportunity, volume):
    points, reasons = 0.0, []
    if {"monthly", "weekly", "daily"}.issubset(set(htf.get("aligned_timeframes") or [])): points += 5; reasons.append("monthly_weekly_daily_aligned")
    if (fvg.get("diagnostics") or {}).get("aligned_support_or_resistance") and not (fvg.get("diagnostics") or {}).get("entry_inside_opposing_fvg"): points += 5; reasons.append("clean_aligned_fvg")
    if (_f((volume.details.get("diagnostics") or {}).get("relative_volume")) or 0.0) >= CONFIG.relative_volume_strong: points += 5; reasons.append("volume_thrust")
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
