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
            gaps.append(FairValueGap("bullish", low, high, (low + high) / 2.0, idx - 2, idx, timeframe, _is_mitigated(rows[idx + 1:], low=low, high=high, direction="bullish")))

        if c1_low > c3_high:
            low = float(c3_high)
            high = float(c1_low)
            gaps.append(FairValueGap("bearish", low, high, (low + high) / 2.0, idx - 2, idx, timeframe, _is_mitigated(rows[idx + 1:], low=low, high=high, direction="bearish")))

    return gaps


def _is_mitigated(future_candles: Iterable[dict[str, Any]], *, low: float, high: float, direction: str) -> bool:
    midpoint = (low + high) / 2.0
    for candle in future_candles:
        candle_low = _candle_value(candle, "low")
        candle_high = _candle_value(candle, "high")
        if candle_low is None or candle_high is None:
            continue
        if direction == "bullish" and candle_low <= midpoint:
            return True
        if direction == "bearish" and candle_high >= midpoint:
            return True
    return False


def evaluate_fvg_context(signal: dict[str, Any], market_context: dict[str, Any] | None = None) -> dict[str, Any]:
    ctx = market_context or {}
    candles_by_tf = ctx.get("candles") or ctx.get("ohlcv") or {}
    side = normalize_signal_side(signal.get("side") or signal.get("direction"))

    if side == "UNKNOWN":
        return {
            "score": 0.0, "max_score": 15.0, "status": "missing_data",
            "missing_data": ["side"], "block_recommendations": ["unknown_signal_side"],
            "boosts": [], "penalties": ["unknown_signal_side"],
            "diagnostics": {
                "timeframes": {}, "entry_inside_aligned_fvg": False,
                "entry_inside_opposing_fvg": False, "target_into_opposing_fvg": False,
                "aligned_support_or_resistance": False, "nearest_fvg": None, "side": "UNKNOWN",
            },
        }

    entry = _safe_float(signal.get("entry_price") or signal.get("trigger_price") or (signal.get("trigger") or {}).get("entry") or signal.get("current_price"))
    target = _safe_float(signal.get("target_price") or signal.get("target_underlying") or (signal.get("trigger") or {}).get("pt1") or (signal.get("trigger") or {}).get("pt2"))

    missing: list[str] = []
    diagnostics: dict[str, Any] = {
        "timeframes": {},
        "entry_inside_aligned_fvg": False,
        "entry_inside_opposing_fvg": False,
        "target_into_opposing_fvg": False,
        "aligned_support_or_resistance": False,
        "nearest_fvg": None,
    }
    score = 0.0
    max_score = 15.0
    penalties: list[str] = []
    boosts: list[str] = []

    for tf, weight in (("4h", 10.0), ("daily", 5.0)):
        tf_rows = candles_by_tf.get(tf) or candles_by_tf.get(tf.upper()) or candles_by_tf.get("1d" if tf == "daily" else tf)
        if not tf_rows:
            missing.append(f"{tf}_candles")
            diagnostics["timeframes"][tf] = {"available": False, "fvg_count": 0, "score": 0.0}
            continue

        gaps = detect_fair_value_gaps(tf_rows, timeframe=tf)
        active = [g for g in gaps if not g.mitigated]
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

        # Being inside an aligned FVG is friction — you are in the imbalance
        # zone rather than cleanly positioned above/below support.  Deduct
        # 30% from this timeframe.  Being inside an OPPOSING FVG is handled
        # below with a harder -70% penalty.  The old loop ran over ALL active
        # gaps, penalising aligned-gap entries the same as opposing ones.
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

        tf_score = max(0.0, min(weight, tf_score))
        score += tf_score
        diagnostics["timeframes"][tf] = {"available": True, "fvg_count": len(gaps), "active_fvg_count": len(active), "score": round(tf_score, 2), "notes": tf_notes, "active_gaps": [g.to_dict() for g in active[-5:]]}

    block_recommendations: list[str] = []
    if diagnostics["entry_inside_opposing_fvg"]:
        block_recommendations.append("entry_inside_opposing_fvg")
    if diagnostics["target_into_opposing_fvg"]:
        block_recommendations.append("target_into_opposing_fvg")

    return {"score": round(max(0.0, min(max_score, score)), 2), "max_score": max_score, "status": "missing_data" if missing else "ok", "missing_data": missing, "block_recommendations": block_recommendations, "boosts": boosts, "penalties": penalties, "diagnostics": diagnostics}


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
