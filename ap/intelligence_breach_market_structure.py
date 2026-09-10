"""Observe-only BREACH market-structure freeze for PR #435.

Reuses ``ap.fair_value_gap.detect_fair_value_gaps`` / lifecycle — no second
gap detector. Output is research/telemetry only: observe_only=true and
affected_eligibility=false. Later #436/#442 consumers share this schema.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any, Mapping, Optional

from ap.fair_value_gap import FairValueGap, detect_fair_value_gaps, normalize_signal_side

SCHEMA_VERSION = "breach_market_structure.v1"
SESSION_POLICY = "US_EQUITY_RTH"
# Documented Angel Precision 4H construction (ap/fvg_telemetry.py header):
# session-anchored from 09:30 ET intraday bars: [09:30–13:30), [13:30–16:00].
FOUR_HOUR_BUCKET_POLICY = "session_anchored_0930_et_from_15m"
ONE_HOUR_BUCKET_POLICY = "session_anchored_0930_et_60m_from_15m"
SOURCE_PROVIDER = "angel_precision_fvg_evaluator"


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _candle_time(candle: Mapping[str, Any]) -> Optional[str]:
    raw = candle.get("time") or candle.get("timestamp") or candle.get("datetime")
    if raw in (None, ""):
        return None
    return str(raw)


def deterministic_zone_id(
    *,
    timeframe: str,
    direction: str,
    low: float,
    high: float,
    source_start: Optional[str],
    source_end: Optional[str],
) -> str:
    """Immutable zone id from frozen source geometry — not process-local state."""
    payload = "|".join(
        [
            str(timeframe).lower(),
            str(direction).lower(),
            f"{float(low):.6f}",
            f"{float(high):.6f}",
            str(source_start or ""),
            str(source_end or ""),
            FOUR_HOUR_BUCKET_POLICY if str(timeframe).lower() == "4h" else ONE_HOUR_BUCKET_POLICY,
            SCHEMA_VERSION,
        ]
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]
    return f"fvg_{timeframe}_{digest}"


def _normalize_lifecycle_status(status: str) -> str:
    raw = str(status or "unfilled").strip().lower()
    mapping = {
        "unfilled": "unfilled",
        "partial": "partial_fill",
        "partial_fill": "partial_fill",
        "midpoint": "midpoint_touched",
        "midpoint_touched": "midpoint_touched",
        "mitigated": "midpoint_touched",
        "filled": "filled",
        "broken": "broken_reclaimed",
        "reclaimed": "broken_reclaimed",
        "broken_reclaimed": "broken_reclaimed",
        "invalidated": "broken_reclaimed",
    }
    return mapping.get(raw, raw or "unfilled")


def freeze_fvg_zone(
    gap: FairValueGap,
    *,
    candles: list[dict[str, Any]],
    side: str,
    data_as_of: Optional[str],
    source_provider: str = SOURCE_PROVIDER,
) -> dict[str, Any]:
    start_c = candles[gap.start_index] if 0 <= gap.start_index < len(candles) else {}
    end_c = candles[gap.end_index] if 0 <= gap.end_index < len(candles) else {}
    source_start = _candle_time(start_c)
    source_end = _candle_time(end_c)
    aligned = (
        (side == "CALL" and gap.direction == "bullish")
        or (side == "PUT" and gap.direction == "bearish")
    )
    opposing = (
        (side == "CALL" and gap.direction == "bearish")
        or (side == "PUT" and gap.direction == "bullish")
    )
    bucket_policy = (
        FOUR_HOUR_BUCKET_POLICY if gap.timeframe.lower() == "4h" else ONE_HOUR_BUCKET_POLICY
    )
    return {
        "zone_id": deterministic_zone_id(
            timeframe=gap.timeframe,
            direction=gap.direction,
            low=gap.low,
            high=gap.high,
            source_start=source_start,
            source_end=source_end,
        ),
        "timeframe": gap.timeframe,
        "direction": gap.direction,
        "low": round(float(gap.low), 6),
        "midpoint": round(float(gap.midpoint), 6),
        "high": round(float(gap.high), 6),
        "source_candle_start": source_start,
        "source_candle_end": source_end,
        "source_provider": source_provider,
        "session_policy": SESSION_POLICY,
        "candle_bucket_anchor_policy": bucket_policy,
        "completed_bar_cutoff": data_as_of,
        "data_as_of": data_as_of,
        "fill_pct": round(float(gap.fill_pct or 0.0), 6),
        "lifecycle_status": _normalize_lifecycle_status(gap.status),
        "aligned_for_side": bool(aligned),
        "opposing_for_side": bool(opposing),
        "model_schema_version": SCHEMA_VERSION,
    }


def _active_zone(zone: Mapping[str, Any]) -> bool:
    return str(zone.get("lifecycle_status") or "") not in {
        "filled",
        "broken_reclaimed",
    }


def _front_edge(zone: Mapping[str, Any], *, side: str) -> Optional[float]:
    if side == "CALL":
        return _safe_float(zone.get("low"))
    if side == "PUT":
        return _safe_float(zone.get("high"))
    return None


def _far_edge(zone: Mapping[str, Any], *, side: str) -> Optional[float]:
    if side == "CALL":
        return _safe_float(zone.get("high"))
    if side == "PUT":
        return _safe_float(zone.get("low"))
    return None


def _distance(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None:
        return None
    return abs(float(a) - float(b))


def classify_breach_fvg_relationship(
    *,
    side: str,
    price: Optional[float],
    trigger: Optional[float],
    target: Optional[float],
    stop: Optional[float],
    zones: list[dict[str, Any]],
    fvg_diagnostics: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Stable breach-to-FVG relationship from frozen zones + evaluator diagnostics."""
    diag = dict(fvg_diagnostics or {})
    side_u = str(side or "").upper()
    active = [z for z in zones if _active_zone(z)]
    aligned = [z for z in active if z.get("aligned_for_side")]
    opposing = [z for z in active if z.get("opposing_for_side")]

    inside_aligned = next(
        (z for z in aligned if price is not None and float(z["low"]) <= price <= float(z["high"])),
        None,
    )
    inside_opposing = next(
        (z for z in opposing if price is not None and float(z["low"]) <= price <= float(z["high"])),
        None,
    )

    def _ahead(zone: Mapping[str, Any]) -> bool:
        front = _front_edge(zone, side=side_u)
        if price is None or front is None:
            return False
        if side_u == "CALL":
            return front >= price
        if side_u == "PUT":
            return front <= price
        return False

    opposing_ahead = [z for z in opposing if _ahead(z)]
    aligned_behind = []
    for z in aligned:
        far = _far_edge(z, side=side_u)
        if price is None or far is None:
            continue
        if side_u == "CALL" and far <= price:
            aligned_behind.append(z)
        elif side_u == "PUT" and far >= price:
            aligned_behind.append(z)

    nearest_aligned = None
    nearest_opposing = None
    if aligned:
        nearest_aligned = min(
            aligned,
            key=lambda z: _distance(price, _front_edge(z, side=side_u)) or 1e18,
        )
    if opposing_ahead:
        nearest_opposing = min(
            opposing_ahead,
            key=lambda z: _distance(price, _front_edge(z, side=side_u)) or 1e18,
        )
    elif opposing:
        nearest_opposing = min(
            opposing,
            key=lambda z: _distance(price, _front_edge(z, side=side_u)) or 1e18,
        )

    relationship = "CLEAR_PATH"
    confirmation_required = False
    confirmation_present = bool(diag.get("confirmation_present"))
    confirmation_basis = None

    path_state = str(diag.get("path_state") or "")
    if inside_opposing is not None:
        relationship = "INSIDE_OPPOSING_ZONE"
        confirmation_required = True
        confirmation_basis = "price_inside_opposing_fvg"
    elif inside_aligned is not None:
        relationship = "INSIDE_ALIGNED_FVG"
    elif nearest_opposing is not None and price is not None:
        front = _front_edge(nearest_opposing, side=side_u)
        dist = _distance(price, front)
        if dist is not None and dist == 0:
            relationship = "AT_OPPOSING_FRONT"
            confirmation_required = True
            confirmation_basis = "at_opposing_fvg_front"
        elif dist is not None:
            planned = _distance(trigger, target)
            near = planned is not None and planned > 0 and dist <= planned * 0.25
            if near or bool(diag.get("opposing_fvg_in_path") or diag.get("opposing_4h_fvg_in_path")):
                relationship = "APPROACHING_OPPOSING_FVG"
                confirmation_required = True
                confirmation_basis = "opposing_fvg_in_path"
    if relationship == "CLEAR_PATH" and aligned_behind:
        relationship = "ALIGNED_RETEST_ZONE_BEHIND"

    # Only emit wall accepted/rejected when evaluator already has that evidence.
    if "reclaimed" in path_state or "broken" in path_state:
        relationship = "OPPOSING_WALL_REJECTED"
        confirmation_basis = path_state
    elif "pushing_through_opposing" in path_state:
        relationship = "OPPOSING_WALL_ACCEPTED"
        confirmation_basis = path_state

    opposing_front = _front_edge(nearest_opposing, side=side_u) if nearest_opposing else None
    opposing_mid = _safe_float(nearest_opposing.get("midpoint")) if nearest_opposing else None
    opposing_far = _far_edge(nearest_opposing, side=side_u) if nearest_opposing else None
    aligned_front = _front_edge(nearest_aligned, side=side_u) if nearest_aligned else None

    planned_move = _distance(trigger, target)
    runway = _distance(price, opposing_front) if opposing_front is not None else None
    runway_pct = None
    if runway is not None and planned_move not in (None, 0):
        runway_pct = runway / planned_move

    remaining_r_before_wall = None
    risk = _distance(trigger, stop)
    if runway is not None and risk not in (None, 0):
        remaining_r_before_wall = runway / risk

    target_relation = None
    if target is not None and opposing_front is not None and opposing_far is not None:
        lo = min(float(opposing_front), float(opposing_far))
        hi = max(float(opposing_front), float(opposing_far))
        if side_u == "CALL":
            if target < lo:
                target_relation = "BEFORE_WALL"
            elif target == lo:
                target_relation = "AT_WALL"
            elif lo < target <= hi:
                target_relation = "INSIDE_WALL"
            else:
                target_relation = "BEYOND_WALL"
        elif side_u == "PUT":
            if target > hi:
                target_relation = "BEFORE_WALL"
            elif target == hi:
                target_relation = "AT_WALL"
            elif lo <= target < hi:
                target_relation = "INSIDE_WALL"
            else:
                target_relation = "BEYOND_WALL"

    return {
        "relationship": relationship,
        "distance_to_nearest_aligned_zone": _distance(price, aligned_front),
        "distance_to_nearest_opposing_zone_front": _distance(price, opposing_front),
        "distance_to_midpoint": _distance(price, opposing_mid),
        "distance_to_far_edge": _distance(price, opposing_far),
        "clean_directional_runway_before_opposing_wall": runway,
        "clean_runway_pct_of_trigger_to_target": runway_pct,
        "remaining_r_before_first_opposing_wall": remaining_r_before_wall,
        "target_relation": target_relation,
        "confirmation_required": bool(confirmation_required),
        "confirmation_present": confirmation_present,
        "confirmation_basis": confirmation_basis,
        "nearest_aligned_zone_id": (nearest_aligned or {}).get("zone_id"),
        "nearest_opposing_zone_id": (nearest_opposing or {}).get("zone_id"),
        "aligned_retest_zone_ids": [z["zone_id"] for z in aligned_behind],
    }


def freeze_breach_market_structure(
    signal: Mapping[str, Any],
    *,
    candles_by_tf: Mapping[str, Any],
    fvg_context: Mapping[str, Any] | None = None,
    data_as_of: Optional[str] = None,
) -> dict[str, Any]:
    """Freeze 4H/1H FVG geometry + relationship for a BREACH snapshot."""
    sig = dict(signal or {})
    side = normalize_signal_side(sig.get("side") or sig.get("direction"))
    price = (
        _safe_float(sig.get("underlying_price"))
        or _safe_float(sig.get("current_price"))
        or _safe_float(sig.get("trigger_price"))
        or _safe_float(sig.get("trigger"))
    )
    trigger = _safe_float(sig.get("trigger_price")) or _safe_float(sig.get("trigger"))
    target = (
        _safe_float(sig.get("target_price"))
        or _safe_float(sig.get("target"))
        or _safe_float(sig.get("pt1"))
    )
    stop = _safe_float(sig.get("stop_price")) or _safe_float(sig.get("stop"))

    zones: list[dict[str, Any]] = []
    for tf in ("4h", "1h"):
        rows = list(
            (candles_by_tf or {}).get(tf)
            or (candles_by_tf or {}).get(tf.upper())
            or []
        )
        if not rows:
            continue
        for gap in detect_fair_value_gaps(rows, timeframe=tf):
            zones.append(
                freeze_fvg_zone(
                    gap,
                    candles=rows,
                    side=side,
                    data_as_of=data_as_of,
                )
            )

    diag = dict((fvg_context or {}).get("diagnostics") or {})
    relationship = classify_breach_fvg_relationship(
        side=side,
        price=price,
        trigger=trigger,
        target=target,
        stop=stop,
        zones=zones,
        fvg_diagnostics=diag,
    )

    # VI: inventory only — do not fabricate from OHLCV/FVG.
    vi_raw = sig.get("volatility_imbalance") or sig.get("vi") or diag.get("volatility_imbalance")
    if isinstance(vi_raw, dict) and vi_raw.get("status") not in (None, "", "MISSING"):
        vi = {
            "status": str(vi_raw.get("status") or "AVAILABLE"),
            "value": vi_raw.get("value"),
            "source": vi_raw.get("source"),
            "observed_at": vi_raw.get("observed_at") or data_as_of,
        }
    else:
        vi = {
            "status": "MISSING",
            "value": None,
            "source": None,
            "missing_reason": "canonical_point_in_time_vi_unavailable",
        }

    return {
        "observe_only": True,
        "affected_eligibility": False,
        "schema_version": SCHEMA_VERSION,
        "four_hour_construction_policy": FOUR_HOUR_BUCKET_POLICY,
        "one_hour_construction_policy": ONE_HOUR_BUCKET_POLICY,
        "session_policy": SESSION_POLICY,
        "data_as_of": data_as_of,
        "side": side,
        "zones": zones,
        "relationship": relationship,
        "volatility_imbalance": vi,
        "evaluator_path_state": diag.get("path_state"),
        "frozen_at": datetime.now(timezone.utc).isoformat(),
    }
