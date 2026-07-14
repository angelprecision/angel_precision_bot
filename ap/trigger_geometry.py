from __future__ import annotations

from typing import Any

from ap.score_profile_config import DEFAULT_SCORE_PROFILE_CONFIG as CONFIG
from ap.score_profile_side import normalize_signal_side


def _f(value: Any) -> float | None:
    try:
        return None if value is None or value == "" else float(value)
    except (TypeError, ValueError):
        return None


def _levels(signal: dict[str, Any]) -> tuple[float | None, float | None, float | None]:
    trigger = signal.get("trigger") if isinstance(signal.get("trigger"), dict) else {}
    scalar_trigger = signal.get("trigger") if not isinstance(signal.get("trigger"), dict) else None
    entry = _f(signal.get("entry_price") or signal.get("trigger_price") or scalar_trigger or trigger.get("entry"))
    stop = _f(signal.get("stop_price") or signal.get("stop_underlying") or signal.get("stop") or trigger.get("stop"))
    target = _f(signal.get("target_price") or signal.get("target_underlying") or signal.get("target") or trigger.get("pt1") or trigger.get("pt2"))
    return entry, stop, target


def _result(name: str, score: float, max_score: float, status: str, reason: str, *, missing_data=None, warnings=None, block_recommendations=None, boosts=None, penalties=None, diagnostics=None, **details: Any) -> dict[str, Any]:
    return {
        "score": round(max(0.0, min(float(max_score), float(score or 0))), 2),
        "max_score": float(max_score),
        "status": status,
        "reason": reason,
        "missing_data": sorted(set(missing_data or [])),
        "warnings": sorted(set(warnings or [])),
        "block_recommendations": sorted(set(block_recommendations or [])),
        "boosts": boosts or [],
        "penalties": penalties or [],
        "diagnostics": {"observe_only": True, "block_recommendations_are_diagnostic_only": True, **(diagnostics or {})},
        **details,
    }


def score_trigger_geometry(signal: dict[str, Any], side: Any | None = None) -> dict[str, Any]:
    sig = dict(signal or {})
    normalized_side = normalize_signal_side(side if side is not None else sig.get("side") or sig.get("direction"))
    entry, stop, target = _levels(sig)
    missing = [f"geometry_{n}" for n, v in (("entry", entry), ("stop", stop), ("target", target)) if v is None]
    warnings: list[str] = []
    invalid: list[str] = []
    diagnostic: list[str] = []

    if normalized_side == "UNKNOWN":
        missing.append("side")
        warnings.append("unknown_side")
        invalid.append("unknown_side")

    if entry is None or stop is None or target is None:
        return _result("trigger_geometry", 0, CONFIG.trigger_geometry_max_points, "block_recommended" if invalid else "missing_data", "missing geometry", missing_data=missing, warnings=warnings, block_recommendations=invalid, penalties=invalid, diagnostics={"side": normalized_side}, entry=entry, stop=stop, target=target, invalid_geometry_blocks=invalid)

    if normalized_side == "CALL":
        if stop >= entry:
            invalid.append("call_stop_not_below_entry")
        if target <= entry:
            invalid.append("call_target_not_above_entry")
    elif normalized_side == "PUT":
        if stop <= entry:
            invalid.append("put_stop_not_above_entry")
        if target >= entry:
            invalid.append("put_target_not_below_entry")

    risk = abs(entry - stop)
    reward = abs(target - entry)
    rr = reward / risk if risk > 0 else 0.0
    if rr < CONFIG.min_trigger_reward_risk:
        diagnostic.append("poor_underlying_reward_to_risk")
        warnings.append("poor_underlying_reward_to_risk")

    points = 10 if rr >= 2 else 7 if rr >= 1.25 else 4 if rr >= CONFIG.min_trigger_reward_risk else 1
    blocks = invalid + diagnostic
    reason = "invalid geometry" if invalid else "poor reward/risk diagnostic" if diagnostic else f"geometry valid rr={rr:.2f}"
    return _result("trigger_geometry", 0 if invalid else points, CONFIG.trigger_geometry_max_points, "block_recommended" if blocks else "ok", reason, missing_data=missing, warnings=warnings, block_recommendations=blocks, penalties=blocks, diagnostics={"side": normalized_side}, entry=entry, stop=stop, target=target, underlying_reward_to_risk=round(rr, 3), invalid_geometry_blocks=invalid)


def score_remaining_opportunity(signal: dict[str, Any], side: Any | None = None) -> dict[str, Any]:
    sig = dict(signal or {})
    normalized_side = normalize_signal_side(side if side is not None else sig.get("side") or sig.get("direction"))
    trigger = sig.get("trigger") if isinstance(sig.get("trigger"), dict) else {}
    current = _f(sig.get("current_price") or sig.get("underlying_price") or sig.get("entry_price") or trigger.get("entry"))
    _entry, stop, target = _levels(sig)
    missing = [n for n, v in (("current_price", current), ("target", target), ("stop", stop)) if v is None]
    warnings: list[str] = []
    blocks: list[str] = []
    if normalized_side == "UNKNOWN":
        missing.append("side")
        warnings.append("unknown_side")
        blocks.append("unknown_side")
    if current is None or target is None or stop is None or normalized_side == "UNKNOWN":
        return _result("remaining_opportunity", 0, CONFIG.remaining_opportunity_max_points, "block_recommended" if blocks else "missing_data", "missing current/target/stop/side", missing_data=missing, warnings=warnings, block_recommendations=blocks, penalties=blocks, diagnostics={"side": normalized_side}, current_price=current, target=target, stop=stop)
    move = target - current if normalized_side == "CALL" else current - target
    risk = current - stop if normalized_side == "CALL" else stop - current
    pct = move / current * 100 if current else 0
    rem_r = move / risk if risk and risk > 0 else 0
    if move <= 0:
        blocks.append("target_already_reached_or_wrong_side")
    if pct < CONFIG.min_remaining_move_pct:
        blocks.append("remaining_move_pct_too_small")
    if rem_r < CONFIG.min_remaining_r:
        blocks.append("remaining_r_too_small")
    warnings.extend(blocks)
    points = 10 if rem_r >= 2 and pct >= 0.75 else 7.5 if rem_r >= 1.25 else 4 if rem_r >= CONFIG.min_remaining_r else 0
    return _result("remaining_opportunity", points, CONFIG.remaining_opportunity_max_points, "block_recommended" if blocks else "ok", f"remaining_move_pct={pct:.2f} remaining_r={rem_r:.2f}", missing_data=missing, warnings=warnings, block_recommendations=blocks, penalties=blocks, diagnostics={"side": normalized_side}, current_price=current, target=target, stop=stop, remaining_move=round(move, 4), remaining_move_pct=round(pct, 4), remaining_r=round(rem_r, 4))
