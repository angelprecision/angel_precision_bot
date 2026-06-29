from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ap.score_profile_side import normalize_signal_side


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None



@dataclass(frozen=True)
class StratBarState:
    timeframe: str
    bar_type: str
    directional_bias: str
    high: float
    low: float
    previous_high: float
    previous_low: float
    distance_to_two_up_pct: float | None = None
    distance_to_two_down_pct: float | None = None

    def aligns_with(self, side: str) -> bool:
        # UNKNOWN side can never be confirmed as aligned — return False so
        # the timeframe contributes no score rather than silently aligning.
        if side not in {"CALL", "PUT"}:
            return False
        return (side == "CALL" and self.directional_bias == "bullish") or (side == "PUT" and self.directional_bias == "bearish")

    def to_dict(self) -> dict[str, Any]:
        return {
            "timeframe": self.timeframe,
            "bar_type": self.bar_type,
            "directional_bias": self.directional_bias,
            "high": round(self.high, 4),
            "low": round(self.low, 4),
            "previous_high": round(self.previous_high, 4),
            "previous_low": round(self.previous_low, 4),
            "distance_to_two_up_pct": round(self.distance_to_two_up_pct, 4) if self.distance_to_two_up_pct is not None else None,
            "distance_to_two_down_pct": round(self.distance_to_two_down_pct, 4) if self.distance_to_two_down_pct is not None else None,
        }


def classify_strat_bar(previous_candle: dict[str, Any], current_candle: dict[str, Any], *, timeframe: str, current_price: float | None = None) -> StratBarState | None:
    prev_high = _safe_float(previous_candle.get("high"))
    prev_low = _safe_float(previous_candle.get("low"))
    high = _safe_float(current_candle.get("high"))
    low = _safe_float(current_candle.get("low"))
    if None in (prev_high, prev_low, high, low):
        return None
    high = float(high)
    low = float(low)
    prev_high = float(prev_high)
    prev_low = float(prev_low)
    took_high = high > prev_high
    took_low = low < prev_low
    if took_high and took_low:
        bar_type = "3"
        directional_bias = "outside"
    elif took_high:
        bar_type = "2U"
        directional_bias = "bullish"
    elif took_low:
        bar_type = "2D"
        directional_bias = "bearish"
    else:
        bar_type = "1"
        directional_bias = "inside"
    px = _safe_float(current_price)
    dist_up = None
    dist_down = None
    if px and px > 0:
        dist_up = max(0.0, (prev_high - px) / px * 100.0)
        dist_down = max(0.0, (px - prev_low) / px * 100.0)
    return StratBarState(timeframe, bar_type, directional_bias, high, low, prev_high, prev_low, dist_up, dist_down)


def evaluate_higher_timeframe_confluence(signal: dict[str, Any], market_context: dict[str, Any] | None = None) -> dict[str, Any]:
    ctx = market_context or {}
    candles_by_tf = ctx.get("candles") or ctx.get("ohlcv") or {}
    side = normalize_signal_side(signal.get("side") or signal.get("direction"))

    if side == "UNKNOWN":
        return {
            "score": 0.0, "max_score": 18.0, "status": "missing_data",
            "missing_data": ["side"], "block_recommendations": ["unknown_signal_side"],
            "aligned_timeframes": [], "opposing_timeframes": [],
            "inside_timeframes": [], "near_breach_timeframes": [],
            "diagnostics": {"states": {}, "side": "UNKNOWN", "current_price": None},
        }

    current_price = _safe_float(signal.get("current_price") or signal.get("underlying_price") or signal.get("entry_price") or signal.get("trigger_price") or (signal.get("trigger") or {}).get("entry"))
    timeframe_weights = {"monthly": 5.0, "weekly": 4.0, "daily": 3.0, "4h": 2.0, "2d": 0.5, "3d": 0.5, "4d": 0.5, "5d": 0.5}
    score = 0.0
    max_score = 18.0  # weights(16) + two alignment bonuses(2) = 18 achievable
    missing_required: list[str] = []
    states: dict[str, Any] = {}
    aligned_tfs: list[str] = []
    opposing_tfs: list[str] = []
    inside_tfs: list[str] = []
    near_breach_tfs: list[str] = []
    for tf, weight in timeframe_weights.items():
        rows = candles_by_tf.get(tf) or candles_by_tf.get(tf.upper()) or candles_by_tf.get("1mo" if tf == "monthly" else tf) or candles_by_tf.get("1wk" if tf == "weekly" else tf) or candles_by_tf.get("1d" if tf == "daily" else tf)
        if not rows or len(rows) < 2:
            if tf in {"monthly", "weekly", "daily", "4h"}:
                missing_required.append(f"{tf}_candles")
            states[tf] = {"available": False, "score": 0.0}
            continue
        state = classify_strat_bar(rows[-2], rows[-1], timeframe=tf, current_price=current_price)
        if state is None:
            if tf in {"monthly", "weekly", "daily", "4h"}:
                missing_required.append(f"{tf}_candles_invalid")
            states[tf] = {"available": False, "score": 0.0}
            continue
        tf_score = 0.0
        if state.aligns_with(side):
            tf_score = weight
            aligned_tfs.append(tf)
        elif state.directional_bias == "inside":
            inside_tfs.append(tf)
            dist = state.distance_to_two_up_pct if side == "CALL" else state.distance_to_two_down_pct
            if dist is not None and dist <= 0.40:
                tf_score = weight * 0.75
                near_breach_tfs.append(tf)
            else:
                tf_score = weight * 0.25
        elif state.directional_bias == "outside":
            tf_score = weight * 0.50
        else:
            opposing_tfs.append(tf)
            tf_score = 0.0
        score += tf_score
        states[tf] = {"available": True, "score": round(tf_score, 2), **state.to_dict()}
    if "monthly" in aligned_tfs and "weekly" in aligned_tfs:
        score += 1.0
    if "weekly" in aligned_tfs and "daily" in aligned_tfs and "4h" in aligned_tfs:
        score += 1.0
    block_recommendations: list[str] = []
    if "monthly" in opposing_tfs and "weekly" in opposing_tfs:
        block_recommendations.append("monthly_weekly_oppose_signal")
    if len(opposing_tfs) >= 3:
        block_recommendations.append("multiple_higher_timeframes_oppose_signal")
    return {"score": round(max(0.0, min(max_score, score)), 2), "max_score": max_score, "status": "missing_data" if missing_required else "ok", "missing_data": missing_required, "block_recommendations": block_recommendations, "aligned_timeframes": aligned_tfs, "opposing_timeframes": opposing_tfs, "inside_timeframes": inside_tfs, "near_breach_timeframes": near_breach_tfs, "diagnostics": {"states": states, "side": side, "current_price": current_price}}
