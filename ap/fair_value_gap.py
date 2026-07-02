from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from ap.score_profile_side import normalize_signal_side


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_float(*values: Any) -> float | None:
    for value in values:
        result = _safe_float(value)
        if result is not None:
            return result
    return None


@dataclass(frozen=True)
class FairValueGap:
    direction: str
    low: float
    high: float
    midpoint: float
    start_index: int
    end_index: int
    timeframe: str
    mitigated: bool = False
    fill_pct: float = 0.0
    status: str = "unfilled"

    def contains(self, price: float | None) -> bool:
        if price is None:
            return False
        return self.low <= float(price) <= self.high

    def distance_pct(self, price: float | None) -> float | None:
        if price is None or float(price) == 0:
            return None
        if self.contains(price):
            return 0.0
        nearest = self.low if price < self.low else self.high
        return abs(float(price) - nearest) / abs(float(price)) * 100.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "direction": self.direction,
            "low": round(self.low, 4),
            "high": round(self.high, 4),
            "midpoint": round(self.midpoint, 4),
            "start_index": self.start_index,
            "end_index": self.end_index,
            "timeframe": self.timeframe,
            "mitigated": bool(self.mitigated),
            "fill_pct": round(float(self.fill_pct), 4),
            "status": self.status,
        }


def _candle_value(candle: dict[str, Any], key: str) -> float | None:
    return _safe_float(candle.get(key) or candle.get(key.upper()) or candle.get(key.capitalize()))


def detect_fair_value_gaps(candles: Iterable[dict[str, Any]] | None, *, timeframe: str, lookback: int = 120) -> list[FairValueGap]:
    rows = list(candles or [])
    if len(rows) < 3:
        return []
    rows = rows[-int(lookback):] if lookback and lookback > 0 else rows
    gaps: list[FairValueGap] = []

    for idx in range(2, len(rows)):
        c1 = rows[idx - 2]
        c3 = rows[idx]
        c1_high = _candle_value(c1, "high")
        c1_low = _candle_value(c1, "low")
        c3_high = _candle_value(c3, "high")
        c3_low = _candle_value(c3, "low")
        if None in (c1_high, c1_low, c3_high, c3_low):
            continue

        if c1_high < c3_low:
            low = float(c1_high)
            high = float(c3_low)
            lifecycle = _fvg_lifecycle(rows[idx + 1 :], low=low, high=high, direction="bullish")
            gaps.append(
                FairValueGap(
                    "bullish",
                    low,
                    high,
                    (low + high) / 2.0,
                    idx - 2,
                    idx,
                    timeframe,
                    lifecycle["mitigated"],
                    lifecycle["fill_pct"],
                    lifecycle["status"],
                )
            )

        if c1_low > c3_high:
            low = float(c3_high)
            high = float(c1_low)
            lifecycle = _fvg_lifecycle(rows[idx + 1 :], low=low, high=high, direction="bearish")
            gaps.append(
                FairValueGap(
                    "bearish",
                    low,
                    high,
                    (low + high) / 2.0,
                    idx - 2,
                    idx,
                    timeframe,
                    lifecycle["mitigated"],
                    lifecycle["fill_pct"],
                    lifecycle["status"],
                )
            )

    return gaps


def _fvg_lifecycle(future_candles: Iterable[dict[str, Any]], *, low: float, high: float, direction: str) -> dict[str, Any]:
    """Classify later interaction without deleting a still-useful FVG wall.

    A midpoint touch is mitigation, not automatic invalidation.  The zone remains
    actionable until price closes through the far side: bullish FVG support is
    broken by a close below low; bearish FVG resistance is reclaimed by a close
    above high.
    """
    width = max(abs(float(high) - float(low)), 1e-9)
    fill_pct = 0.0
    invalidated = False

    for candle in future_candles:
        candle_low = _candle_value(candle, "low")
        candle_high = _candle_value(candle, "high")
        candle_close = _candle_value(candle, "close")
        if candle_low is None or candle_high is None:
            continue

        if direction == "bullish":
            if candle_low < high:
                touched = min(max(high - float(candle_low), 0.0), width)
                fill_pct = max(fill_pct, touched / width)
            if candle_close is not None and float(candle_close) < low:
                invalidated = True
        elif direction == "bearish":
            if candle_high > low:
                touched = min(max(float(candle_high) - low, 0.0), width)
                fill_pct = max(fill_pct, touched / width)
            if candle_close is not None and float(candle_close) > high:
                invalidated = True

    if invalidated:
        status = "broken" if direction == "bullish" else "reclaimed"
    elif fill_pct >= 1.0:
        status = "filled"
    elif fill_pct >= 0.5:
        status = "midpoint_touched"
    elif fill_pct > 0.0:
        status = "partial_fill"
    else:
        status = "unfilled"

    return {"fill_pct": round(min(1.0, max(0.0, fill_pct)), 4), "mitigated": fill_pct >= 0.5, "status": status}


def _is_mitigated(future_candles: Iterable[dict[str, Any]], *, low: float, high: float, direction: str) -> bool:
    return bool(_fvg_lifecycle(future_candles, low=low, high=high, direction=direction)["mitigated"])


def _trigger(signal: dict[str, Any]) -> dict[str, Any]:
    raw = signal.get("trigger") if isinstance(signal.get("trigger"), dict) else {}
    return raw or {}


def _quote(signal: dict[str, Any]) -> dict[str, Any]:
    raw = signal.get("quote") if isinstance(signal.get("quote"), dict) else {}
    return raw or {}


def _extract_underlying_prices(signal: dict[str, Any]) -> tuple[float | None, float | None, float | None, list[str]]:
    trigger = _trigger(signal)
    quote = _quote(signal)
    warnings: list[str] = []

    current = _first_float(
        signal.get("current_underlying"),
        signal.get("underlying_price"),
        signal.get("underlying"),
        signal.get("current_price"),
        signal.get("last_price"),
        quote.get("last"),
        quote.get("underlying_price"),
    )
    entry = _first_float(
        signal.get("entry_trigger"),
        signal.get("underlying_trigger"),
        signal.get("trigger_price"),
        trigger.get("entry"),
        trigger.get("trigger_price"),
    )
    if entry is None:
        entry = current

    target = _first_float(
        signal.get("target_underlying"),
        signal.get("target_price_underlying"),
        signal.get("underlying_target"),
        trigger.get("pt1"),
        trigger.get("pt2"),
        signal.get("target_price"),
    )

    # Backward-compatible fallback for old observe-only tests/callers.  Production
    # callers should pass current_underlying / trigger_price / target_underlying
    # because generic entry_price is often the option premium.
    if current is None:
        current = _safe_float(signal.get("entry_price"))
        if current is not None:
            warnings.append("used_entry_price_as_current_underlying_fallback")
    if entry is None:
        entry = _safe_float(signal.get("entry_price"))
        if entry is not None:
            warnings.append("used_entry_price_as_trigger_fallback")

    return current, entry, target, warnings


def _active_wall(gap: FairValueGap) -> bool:
    return gap.status not in {"broken", "reclaimed"}


def _path_intersects_gap(start: float | None, end: float | None, gap: FairValueGap, *, side: str) -> bool:
    if start is None or end is None:
        return False
    start_f = float(start)
    end_f = float(end)
    if side == "CALL" and end_f <= start_f:
        return False
    if side == "PUT" and end_f >= start_f:
        return False
    low = min(start_f, end_f)
    high = max(start_f, end_f)
    return low <= gap.high and high >= gap.low


def _front_of_opposing_gap(gap: FairValueGap, *, side: str) -> float:
    # CALLs approach bearish resistance from below. PUTs approach bullish support
    # from above.  The first boundary touched is the safer exit/magnet level.
    return gap.low if side == "CALL" else gap.high


def _confirmation_for_opposing_gap(
    side: str,
    gap: FairValueGap,
    *,
    tf_rows: list[dict[str, Any]],
    one_hour_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    if side == "CALL" and gap.direction == "bearish":
        boundary = gap.high
        tf_close = _candle_value(tf_rows[-1], "close") if tf_rows else None
        one_h_closes = [_candle_value(c, "close") for c in one_hour_rows[-2:]]
        tf_confirmed = tf_close is not None and tf_close > boundary
        one_h_confirmed = len(one_h_closes) == 2 and all(c is not None and c > boundary for c in one_h_closes)
        return {
            "required": True,
            "present": bool(tf_confirmed or one_h_confirmed),
            "type": "reclaim_bearish_fvg",
            "boundary": round(boundary, 4),
            "reason": "4H close or two 1H closes above bearish FVG high required",
        }
    if side == "PUT" and gap.direction == "bullish":
        boundary = gap.low
        tf_close = _candle_value(tf_rows[-1], "close") if tf_rows else None
        one_h_closes = [_candle_value(c, "close") for c in one_hour_rows[-2:]]
        tf_confirmed = tf_close is not None and tf_close < boundary
        one_h_confirmed = len(one_h_closes) == 2 and all(c is not None and c < boundary for c in one_h_closes)
        return {
            "required": True,
            "present": bool(tf_confirmed or one_h_confirmed),
            "type": "break_bullish_fvg",
            "boundary": round(boundary, 4),
            "reason": "4H close or two 1H closes below bullish FVG low required",
        }
    return {"required": False, "present": True, "type": "not_required", "boundary": None, "reason": "aligned_or_non_opposing_fvg"}


def _empty_target_guidance(target: float | None) -> dict[str, Any]:
    return {
        "original_target": round(target, 4) if target is not None else None,
        "action": "none",
        "suggested_target": None,
        "reason": None,
        "candidate_fvg": None,
    }


def evaluate_fvg_context(signal: dict[str, Any], market_context: dict[str, Any] | None = None) -> dict[str, Any]:
    ctx = market_context or {}
    candles_by_tf = ctx.get("candles") or ctx.get("ohlcv") or {}
    side = normalize_signal_side(signal.get("side") or signal.get("direction"))

    if side == "UNKNOWN":
        return {
            "score": 0.0, "max_score": 15.0, "status": "missing_data",
            "missing_data": ["side"], "block_recommendations": ["unknown_signal_side"],
            "boosts": [], "penalties": ["unknown_signal_side"], "warnings": ["unknown_signal_side"],
            "target_guidance": _empty_target_guidance(None),
            "diagnostics": {
                "timeframes": {}, "entry_inside_aligned_fvg": False,
                "entry_inside_opposing_fvg": False, "target_into_opposing_fvg": False,
                "opposing_fvg_in_path": False, "opposing_4h_fvg_in_path": False,
                "aligned_support_or_resistance": False, "nearest_fvg": None, "side": "UNKNOWN",
                "path_state": "unknown_side", "confirmation_required": False,
                "confirmation_present": False,
            },
        }

    current, entry, target, price_warnings = _extract_underlying_prices(signal)
    path_start = current if current is not None else entry

    missing: list[str] = []
    diagnostics: dict[str, Any] = {
        "timeframes": {},
        "entry_inside_aligned_fvg": False,
        "entry_inside_opposing_fvg": False,
        "target_into_opposing_fvg": False,
        "opposing_fvg_in_path": False,
        "opposing_4h_fvg_in_path": False,
        "aligned_support_or_resistance": False,
        "nearest_fvg": None,
        "current_underlying": round(current, 4) if current is not None else None,
        "entry_trigger": round(entry, 4) if entry is not None else None,
        "target_underlying": round(target, 4) if target is not None else None,
        "path_state": "clear_path",
        "confirmation_required": False,
        "confirmation_present": True,
        "obstacles": [],
    }
    score = 0.0
    max_score = 15.0
    penalties: list[str] = []
    boosts: list[str] = []
    warnings: list[str] = list(price_warnings)
    target_guidance = _empty_target_guidance(target)

    one_hour_rows = (
        candles_by_tf.get("1h")
        or candles_by_tf.get("1H")
        or candles_by_tf.get("60m")
        or candles_by_tf.get("60min")
        or []
    )

    if current is None:
        missing.append("current_underlying")
    if entry is None:
        missing.append("entry_trigger")
    if target is None:
        missing.append("target_underlying")

    for tf, weight in (("4h", 10.0), ("1h", 5.0)):
        tf_rows = candles_by_tf.get(tf) or candles_by_tf.get(tf.upper()) or candles_by_tf.get("60m" if tf == "1h" else tf)
        if not tf_rows:
            missing.append(f"{tf}_candles")
            diagnostics["timeframes"][tf] = {"available": False, "fvg_count": 0, "score": 0.0}
            continue

        gaps = detect_fair_value_gaps(tf_rows, timeframe=tf)
        active = [g for g in gaps if _active_wall(g)]
        relevant = _relevant_gaps(active, side=side)
        opposing = _opposing_gaps(active, side=side)
        tf_score = 0.0
        tf_notes: list[str] = []

        if relevant:
            nearest = min(relevant, key=lambda g: g.distance_pct(entry) if g.distance_pct(entry) is not None else 999999.0)
            diagnostics["nearest_fvg"] = diagnostics["nearest_fvg"] or nearest.to_dict()
            if side == "CALL" and entry is not None and nearest.high <= entry:
                tf_score += weight * 0.70
                boosts.append(f"{tf}_bullish_fvg_support_below_entry")
                tf_notes.append("aligned_support_below_entry")
            elif side == "PUT" and entry is not None and nearest.low >= entry:
                tf_score += weight * 0.70
                boosts.append(f"{tf}_bearish_fvg_resistance_above_entry")
                tf_notes.append("aligned_resistance_above_entry")
            dist = nearest.distance_pct(entry)
            if dist is not None and dist <= 0.35:
                tf_score += weight * 0.30
                boosts.append(f"{tf}_entry_near_aligned_fvg")
                tf_notes.append("entry_near_aligned_fvg")
            diagnostics["aligned_support_or_resistance"] = True

        for gap in relevant:
            if gap.contains(entry):
                diagnostics["entry_inside_aligned_fvg"] = True
                tf_notes.append("entry_inside_aligned_fvg")
                tf_score -= weight * 0.30

        for gap in opposing:
            if gap.contains(entry):
                diagnostics["entry_inside_opposing_fvg"] = True
                penalties.append(f"{tf}_entry_inside_opposing_fvg")
                tf_notes.append("entry_inside_opposing_fvg")
                tf_score -= weight * 0.70
            if gap.contains(target):
                diagnostics["target_into_opposing_fvg"] = True
                penalties.append(f"{tf}_target_into_opposing_fvg")
                tf_notes.append("target_into_opposing_fvg")
                tf_score -= weight * 0.40

            full_path_intersects = _path_intersects_gap(path_start, target, gap, side=side)
            entry_path_intersects = _path_intersects_gap(path_start, entry, gap, side=side)
            if full_path_intersects or entry_path_intersects:
                confirmation = _confirmation_for_opposing_gap(
                    side,
                    gap,
                    tf_rows=list(tf_rows),
                    one_hour_rows=list(one_hour_rows),
                )
                obstacle = {
                    "timeframe": tf,
                    "gap": gap.to_dict(),
                    "entry_path_intersects": bool(entry_path_intersects),
                    "target_path_intersects": bool(full_path_intersects),
                    "confirmation": confirmation,
                    "front_boundary": round(_front_of_opposing_gap(gap, side=side), 4),
                }
                diagnostics["obstacles"].append(obstacle)
                diagnostics["opposing_fvg_in_path"] = True
                tf_notes.append("opposing_fvg_in_path")

                if tf == "4h":
                    diagnostics["opposing_4h_fvg_in_path"] = True
                    diagnostics["confirmation_required"] = True
                    diagnostics["confirmation_present"] = bool(confirmation["present"])
                    if not confirmation["present"]:
                        diagnostics["path_state"] = "pushing_through_opposing_4h_fvg"
                        penalties.append("4h_opposing_fvg_in_path_without_confirmation")
                        warnings.append("4h_opposing_fvg_in_path_without_confirmation")
                        tf_score -= weight * 0.80
                    else:
                        diagnostics["path_state"] = "opposing_4h_fvg_reclaimed_or_broken"
                        boosts.append("4h_opposing_fvg_confirmation_present")
                        tf_score += weight * 0.20
                elif not confirmation["present"]:
                    penalties.append("1h_opposing_fvg_in_path_without_confirmation")
                    warnings.append("1h_opposing_fvg_in_path_without_confirmation")
                    tf_score -= weight * 0.50

            # FVGs can also act as magnets.  If the original target stops before
            # the next opposing wall, expose a non-mutating extension candidate.
            # Downstream exit logic may choose to trail/hold toward that front
            # boundary only after separate continuation strength checks.
            if (
                target is not None
                and path_start is not None
                and not _path_intersects_gap(path_start, target, gap, side=side)
            ):
                front = _front_of_opposing_gap(gap, side=side)
                if side == "CALL" and path_start < target < front:
                    if target_guidance["action"] in {"none", "keep_original"}:
                        target_guidance = {
                            "original_target": round(target, 4),
                            "action": "extension_candidate_to_opposing_fvg_front",
                            "suggested_target": round(front, 4),
                            "reason": "Original CALL target is before the next bearish FVG; strong continuation can use the FVG front as a magnet/exit area.",
                            "candidate_fvg": gap.to_dict(),
                        }
                        tf_notes.append("target_extension_candidate_to_fvg_front")
                elif side == "PUT" and path_start > target > front:
                    if target_guidance["action"] in {"none", "keep_original"}:
                        target_guidance = {
                            "original_target": round(target, 4),
                            "action": "extension_candidate_to_opposing_fvg_front",
                            "suggested_target": round(front, 4),
                            "reason": "Original PUT target is before the next bullish FVG; strong continuation can use the FVG front as a magnet/exit area.",
                            "candidate_fvg": gap.to_dict(),
                        }
                        tf_notes.append("target_extension_candidate_to_fvg_front")

        if diagnostics["opposing_4h_fvg_in_path"] and target is not None and diagnostics["obstacles"]:
            first_4h_obstacle = next((o for o in diagnostics["obstacles"] if o["timeframe"] == "4h"), None)
            if first_4h_obstacle and not first_4h_obstacle["confirmation"]["present"]:
                target_guidance = {
                    "original_target": round(target, 4),
                    "action": "cap_before_opposing_fvg_or_block_entry",
                    "suggested_target": first_4h_obstacle["front_boundary"],
                    "reason": "Target path crosses an unconfirmed opposing 4H FVG wall; exit before the wall or block entry.",
                    "candidate_fvg": first_4h_obstacle["gap"],
                }

        tf_score = max(0.0, min(weight, tf_score))
        score += tf_score
        diagnostics["timeframes"][tf] = {
            "available": True,
            "fvg_count": len(gaps),
            "active_fvg_count": len(active),
            "score": round(tf_score, 2),
            "notes": tf_notes,
            "active_gaps": [g.to_dict() for g in active[-5:]],
        }

    block_recommendations: list[str] = []
    if diagnostics["entry_inside_opposing_fvg"]:
        block_recommendations.append("entry_inside_opposing_fvg")
    if diagnostics["target_into_opposing_fvg"]:
        block_recommendations.append("target_into_opposing_fvg")
    if diagnostics["opposing_4h_fvg_in_path"] and not diagnostics["confirmation_present"]:
        block_recommendations.append("opposing_4h_fvg_in_path_without_confirmation")

    return {
        "score": round(max(0.0, min(max_score, score)), 2),
        "max_score": max_score,
        "status": "missing_data" if missing else "block_recommended" if block_recommendations else "ok",
        "missing_data": sorted(set(missing)),
        "block_recommendations": sorted(set(block_recommendations)),
        "boosts": boosts,
        "penalties": penalties,
        "warnings": sorted(set(warnings)),
        "target_guidance": target_guidance,
        "diagnostics": diagnostics,
    }


def _relevant_gaps(gaps: list[FairValueGap], *, side: str) -> list[FairValueGap]:
    if side not in {"CALL", "PUT"}:
        return []
    desired = "bullish" if side == "CALL" else "bearish"
    return [g for g in gaps if g.direction == desired]


def _opposing_gaps(gaps: list[FairValueGap], *, side: str) -> list[FairValueGap]:
    if side not in {"CALL", "PUT"}:
        return []
    undesired = "bearish" if side == "CALL" else "bullish"
    return [g for g in gaps if g.direction == undesired]
