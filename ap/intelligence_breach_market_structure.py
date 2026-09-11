"""Observe-only BREACH market-structure freeze for PR #435.

Reuses ``ap.fair_value_gap.detect_fair_value_gaps`` / ``evaluate_fvg_context`` —
no second gap detector. Freezes canonical 4H/1H FVG geometry and the
breach-to-FVG relationship for research snapshots only.

Design invariants
-----------------
1. Observe-only: never gates, delays, allows, or mutates execution.
2. Point-in-time: only completed bars at/before ``data_as_of`` influence zones.
3. Volume imbalance (VI): freeze exact PIT if present; else ``status=MISSING``.
4. 4H session policy is attested from ``ap.fvg_telemetry`` header
   (RTH session-anchored buckets from 09:30 ET).
"""

from __future__ import annotations

import hashlib
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Optional
from zoneinfo import ZoneInfo

from ap.fair_value_gap import (
    FairValueGap,
    detect_fair_value_gaps,
    evaluate_fvg_context,
)
from ap.score_profile_side import normalize_signal_side

ET = ZoneInfo("America/New_York")

SCHEMA_VERSION = "market_structure_v1"
MODEL_VERSION = "fvg_reuse_evaluate_fvg_context_v1"

# Attested verbatim from ap/fvg_telemetry.py module header (4H BAR DEFINITION).
SESSION_POLICY_4H_ATTESTATION = {
    "policy_id": "rth_session_anchored_4h_v1",
    "attested_from": "ap.fvg_telemetry module header (4H BAR DEFINITION)",
    "session": "US_EQUITY_RTH",
    "timezone": "America/New_York",
    "anchor": "09:30",
    "buckets": ["[09:30-13:30)", "[13:30-16:00]"],
    "second_bucket_duration_hours": 2.5,
    "notes": (
        "US-equity RTH is 6.5h, so 4h bars are session-anchored buckets from "
        "09:30 ET: [09:30-13:30) and [13:30-16:00]. The second bar is 2.5h."
    ),
}

CANDLE_BUCKET_ANCHOR_POLICY = {
    "4h": {
        "anchor": "09:30 America/New_York",
        "bucket_minutes": 240,
        "aggregation": "session_anchored_from_15m",
        "session_policy": SESSION_POLICY_4H_ATTESTATION["policy_id"],
    },
    "1h": {
        "anchor": "09:30 America/New_York",
        "bucket_minutes": 60,
        "aggregation": "session_anchored_from_15m",
        "notes": "Last RTH 1h bucket is 30 min (15:30-16:00).",
    },
}

LIFECYCLE_STATUSES = frozenset({
    "unfilled",
    "partial_fill",
    "midpoint_touched",
    "filled",
    "broken_reclaimed",
})

RELATIONSHIP_ENUMS = frozenset({
    "CLEAR_PATH",
    "INSIDE_ALIGNED_FVG",
    "INSIDE_OPPOSING_FVG",
    "APPROACHING_OPPOSING_FVG",
    "ALIGNED_RETEST_ZONE_BEHIND",
    "AT_OPPOSING_FRONT",
    "INSIDE_OPPOSING_ZONE",
    "OPPOSING_WALL_ACCEPTED",
    "OPPOSING_WALL_REJECTED",
})

TARGET_RELATION_ENUMS = frozenset({
    "BEFORE_WALL",
    "AT_WALL",
    "INSIDE_WALL",
    "BEYOND_WALL",
    "NO_WALL",
})

_FRONT_EPS = 1e-4


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        out = float(value)
        if not math.isfinite(out):
            return None
        return out
    except (TypeError, ValueError):
        return None


def _parse_ts(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    if dt.tzinfo is None:
        # Treat naive candle times as ET session local (fvg_telemetry / Tradier shape).
        dt = dt.replace(tzinfo=ET)
    return dt.astimezone(timezone.utc)


def _candle_time(candle: Mapping[str, Any]) -> Optional[datetime]:
    return _parse_ts(candle.get("time") or candle.get("timestamp") or candle.get("start"))


def _bucket_end(start: datetime, *, bucket_minutes: int) -> datetime:
    local = start.astimezone(ET)
    session_close = local.replace(hour=16, minute=0, second=0, microsecond=0)
    end_local = min(local + timedelta(minutes=bucket_minutes), session_close)
    return end_local.astimezone(timezone.utc)


def completed_bars_as_of(
    rows: Iterable[Mapping[str, Any]] | None,
    *,
    as_of: Optional[datetime],
    bucket_minutes: int,
) -> list[dict[str, Any]]:
    """Keep only bars whose bucket end is at/before as_of (no future influence)."""
    out: list[dict[str, Any]] = []
    for row in rows or []:
        if not isinstance(row, Mapping):
            continue
        start = _candle_time(row)
        if start is None:
            # Without a timestamp we cannot prove completion; exclude for PIT safety.
            continue
        if as_of is not None and _bucket_end(start, bucket_minutes=bucket_minutes) > as_of:
            continue
        out.append(dict(row))
    return out


def deterministic_zone_id(
    *,
    timeframe: str,
    direction: str,
    low: float,
    high: float,
    source_candle_start: Optional[str],
    source_candle_end: Optional[str],
) -> str:
    payload = "|".join(
        [
            str(timeframe).lower(),
            str(direction).lower(),
            f"{float(low):.6f}",
            f"{float(high):.6f}",
            str(source_candle_start or ""),
            str(source_candle_end or ""),
        ]
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]
    return f"fvg_{timeframe.lower()}_{digest}"


def _normalize_lifecycle(status: str) -> str:
    raw = str(status or "unfilled").strip().lower()
    if raw in {"broken", "reclaimed", "broken_reclaimed"}:
        return "broken_reclaimed"
    if raw in LIFECYCLE_STATUSES:
        return raw
    return "unfilled"


def _alignment_for_side(direction: str, side: str) -> str:
    if side not in {"CALL", "PUT"}:
        return "unknown"
    aligned_dir = "bullish" if side == "CALL" else "bearish"
    return "aligned" if direction == aligned_dir else "opposing"


def _source_candle_bounds(
    rows: list[dict[str, Any]], gap: FairValueGap, *, bucket_minutes: int
) -> tuple[Optional[str], Optional[str]]:
    start_iso = None
    end_iso = None
    if 0 <= gap.start_index < len(rows):
        start_dt = _candle_time(rows[gap.start_index])
        if start_dt is not None:
            start_iso = start_dt.isoformat()
    if 0 <= gap.end_index < len(rows):
        end_dt = _candle_time(rows[gap.end_index])
        if end_dt is not None:
            end_iso = _bucket_end(end_dt, bucket_minutes=bucket_minutes).isoformat()
            if start_iso is None:
                start_iso = end_dt.isoformat()
    return start_iso, end_iso


def freeze_fvg_zone(
    gap: FairValueGap,
    *,
    rows: list[dict[str, Any]],
    side: str,
    data_as_of: Optional[str],
    source_provider: str,
    session_policy: Mapping[str, Any],
    candle_bucket_anchor_policy: Mapping[str, Any],
) -> dict[str, Any]:
    bucket_minutes = int(candle_bucket_anchor_policy.get("bucket_minutes") or (240 if gap.timeframe == "4h" else 60))
    src_start, src_end = _source_candle_bounds(rows, gap, bucket_minutes=bucket_minutes)
    zone_id = deterministic_zone_id(
        timeframe=gap.timeframe,
        direction=gap.direction,
        low=gap.low,
        high=gap.high,
        source_candle_start=src_start,
        source_candle_end=src_end,
    )
    return {
        "zone_id": zone_id,
        "timeframe": gap.timeframe,
        "direction": gap.direction,
        "low": round(gap.low, 4),
        "midpoint": round(gap.midpoint, 4),
        "high": round(gap.high, 4),
        "source_candle_start": src_start,
        "source_candle_end": src_end,
        "source_provider": source_provider,
        "session_policy": dict(session_policy) if gap.timeframe == "4h" else {
            "policy_id": "rth_session_anchored_1h_v1",
            "timezone": "America/New_York",
            "anchor": "09:30",
        },
        "candle_bucket_anchor_policy": dict(candle_bucket_anchor_policy),
        "completed_bar_cutoff": data_as_of,
        "data_as_of": data_as_of,
        "fill_pct": round(float(gap.fill_pct), 4),
        "lifecycle_status": _normalize_lifecycle(gap.status),
        "alignment": _alignment_for_side(gap.direction, side),
        "mitigated": bool(gap.mitigated),
        "model_version": MODEL_VERSION,
        "schema_version": SCHEMA_VERSION,
    }


def _distance_to_zone(price: float, zone: Mapping[str, Any]) -> dict[str, Any]:
    low = float(zone["low"])
    high = float(zone["high"])
    if low <= price <= high:
        return {"distance": 0.0, "distance_pct": 0.0, "position": "inside"}
    if price < low:
        dist = low - price
        nearest = low
        position = "below"
    else:
        dist = price - high
        nearest = high
        position = "above"
    return {
        "distance": round(dist, 6),
        "distance_pct": round(abs(dist) / abs(price) * 100.0, 6) if price else None,
        "nearest_boundary": round(nearest, 4),
        "position": position,
    }


def _target_relation(side: str, target: Optional[float], wall: Optional[Mapping[str, Any]]) -> str:
    if wall is None or target is None:
        return "NO_WALL"
    low = float(wall["low"])
    high = float(wall["high"])
    front = float(wall["front_boundary"])
    if abs(target - front) <= _FRONT_EPS:
        return "AT_WALL"
    if low <= target <= high:
        return "INSIDE_WALL"
    if side == "CALL":
        if target < front:
            return "BEFORE_WALL"
        return "BEYOND_WALL"
    # PUT
    if target > front:
        return "BEFORE_WALL"
    return "BEYOND_WALL"


def _remaining_r_before_wall(
    *,
    side: str,
    price: Optional[float],
    stop: Optional[float],
    front: Optional[float],
) -> Optional[float]:
    if None in (price, stop, front) or side not in {"CALL", "PUT"}:
        return None
    direction = 1.0 if side == "CALL" else -1.0
    risk = direction * (float(price) - float(stop))  # uses current-to-stop if stop behind
    # Prefer classic trigger risk if stop is on protective side of price.
    protective_risk = direction * (float(price) - float(stop))
    if side == "CALL":
        protective_risk = float(price) - float(stop) if stop < price else float(stop) - float(price)
        runway = float(front) - float(price)
    else:
        protective_risk = float(stop) - float(price) if stop > price else float(price) - float(stop)
        runway = float(price) - float(front)
    if protective_risk <= 0:
        return None
    return round(runway / protective_risk, 4)


def classify_breach_fvg_relationship(
    *,
    side: str,
    price: Optional[float],
    target: Optional[float],
    stop: Optional[float],
    aligned_zones: list[dict[str, Any]],
    opposing_zones: list[dict[str, Any]],
    fvg_context: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Classify breach-to-FVG relationship using frozen zones + evaluator diagnostics."""
    diag = dict((fvg_context or {}).get("diagnostics") or {})
    obstacles = list(diag.get("obstacles") or [])
    confirmation_required = bool(diag.get("confirmation_required"))
    confirmation_present = bool(diag.get("confirmation_present"))

    primary = "CLEAR_PATH"
    primary_zone_id = None
    nearest_opposing = None
    nearest_aligned_behind = None

    if price is None or side not in {"CALL", "PUT"}:
        return {
            "relationship": "CLEAR_PATH",
            "relationship_zone_id": None,
            "distances": {},
            "runway_to_opposing_wall": None,
            "remaining_R_before_opposing_wall": None,
            "target_relation": "NO_WALL",
            "opposing_wall": None,
            "aligned_retest_zone": None,
            "enums_allowed": sorted(RELATIONSHIP_ENUMS),
            "evidence_notes": ["price_or_side_unavailable"],
        }

    # Inside checks
    for zone in opposing_zones:
        if float(zone["low"]) <= price <= float(zone["high"]):
            primary = "INSIDE_OPPOSING_FVG"
            # INSIDE_OPPOSING_ZONE is an alias-grade enum retained for consumers.
            if primary == "INSIDE_OPPOSING_FVG":
                pass
            primary_zone_id = zone["zone_id"]
            nearest_opposing = zone
            break
    if primary == "CLEAR_PATH":
        for zone in aligned_zones:
            if float(zone["low"]) <= price <= float(zone["high"]):
                primary = "INSIDE_ALIGNED_FVG"
                primary_zone_id = zone["zone_id"]
                break

    # Opposing wall ahead / at front
    ahead: list[tuple[float, dict[str, Any]]] = []
    for zone in opposing_zones:
        if zone.get("lifecycle_status") == "broken_reclaimed":
            continue
        front = zone["low"] if side == "CALL" else zone["high"]
        zone = {**zone, "front_boundary": round(float(front), 4)}
        if side == "CALL" and price < float(zone["low"]):
            ahead.append((float(zone["low"]) - price, zone))
        elif side == "PUT" and price > float(zone["high"]):
            ahead.append((price - float(zone["high"]), zone))
        elif float(zone["low"]) <= price <= float(zone["high"]):
            # already inside
            nearest_opposing = zone
        elif abs(price - float(front)) <= _FRONT_EPS or (
            (side == "CALL" and abs(price - float(zone["low"])) <= _FRONT_EPS)
            or (side == "PUT" and abs(price - float(zone["high"])) <= _FRONT_EPS)
        ):
            if primary == "CLEAR_PATH":
                primary = "AT_OPPOSING_FRONT"
                primary_zone_id = zone["zone_id"]
            nearest_opposing = zone

    if ahead:
        ahead.sort(key=lambda item: item[0])
        nearest_opposing = ahead[0][1]
        if primary == "CLEAR_PATH":
            primary = "APPROACHING_OPPOSING_FVG"
            primary_zone_id = nearest_opposing["zone_id"]

    # Aligned retest zone behind
    behind: list[tuple[float, dict[str, Any]]] = []
    for zone in aligned_zones:
        if zone.get("lifecycle_status") == "broken_reclaimed":
            continue
        if side == "CALL" and float(zone["high"]) <= price:
            behind.append((price - float(zone["high"]), zone))
        elif side == "PUT" and float(zone["low"]) >= price:
            behind.append((float(zone["low"]) - price, zone))
    if behind:
        behind.sort(key=lambda item: item[0])
        nearest_aligned_behind = behind[0][1]
        if primary == "CLEAR_PATH":
            primary = "ALIGNED_RETEST_ZONE_BEHIND"
            primary_zone_id = nearest_aligned_behind["zone_id"]

    # Wall accepted/rejected only when BREACH evidence exists via evaluator confirmation.
    if nearest_opposing is not None and confirmation_required:
        matching = [
            o for o in obstacles
            if isinstance(o, Mapping)
            and str((o.get("gap") or {}).get("timeframe") or o.get("timeframe") or "") == str(nearest_opposing.get("timeframe"))
        ]
        if matching or obstacles:
            if confirmation_present:
                # Prefer accepted only when it does not erase a stronger inside/at state.
                if primary in {"APPROACHING_OPPOSING_FVG", "CLEAR_PATH", "AT_OPPOSING_FRONT"}:
                    primary = "OPPOSING_WALL_ACCEPTED"
                    primary_zone_id = nearest_opposing["zone_id"]
            else:
                if primary in {"APPROACHING_OPPOSING_FVG", "CLEAR_PATH", "AT_OPPOSING_FRONT"}:
                    primary = "OPPOSING_WALL_REJECTED"
                    primary_zone_id = nearest_opposing["zone_id"]

    # Dual enum: inside opposing zone synonym when inside opposing FVG
    relationship = primary
    if primary == "INSIDE_OPPOSING_FVG":
        # Keep INSIDE_OPPOSING_FVG as primary; expose synonym flag.
        synonym = "INSIDE_OPPOSING_ZONE"
    else:
        synonym = None

    front = None
    if nearest_opposing is not None:
        front = float(nearest_opposing.get("front_boundary") or (
            nearest_opposing["low"] if side == "CALL" else nearest_opposing["high"]
        ))
        nearest_opposing = {**nearest_opposing, "front_boundary": round(front, 4)}

    runway = None
    if front is not None:
        runway = round((front - price) if side == "CALL" else (price - front), 6)

    distances: dict[str, Any] = {}
    if nearest_opposing is not None:
        distances["opposing_wall"] = _distance_to_zone(price, nearest_opposing)
    if nearest_aligned_behind is not None:
        distances["aligned_retest_behind"] = _distance_to_zone(price, nearest_aligned_behind)

    return {
        "relationship": relationship,
        "relationship_synonym": synonym,
        "relationship_zone_id": primary_zone_id,
        "distances": distances,
        "runway_to_opposing_wall": runway,
        "remaining_R_before_opposing_wall": _remaining_r_before_wall(
            side=side, price=price, stop=stop, front=front
        ),
        "target_relation": _target_relation(side, target, nearest_opposing),
        "opposing_wall": nearest_opposing,
        "aligned_retest_zone": nearest_aligned_behind,
        "enums_allowed": sorted(RELATIONSHIP_ENUMS),
        "evidence_notes": [],
    }


def freeze_volume_imbalance(vi_payload: Any) -> dict[str, Any]:
    """Freeze exact PIT VI when present; never fabricate."""
    if not isinstance(vi_payload, Mapping):
        return {
            "status": "MISSING",
            "source": None,
            "missing_reason": "exact_pit_volume_imbalance_absent",
            "approximation_allowed": False,
            "observe_only": True,
        }
    status = str(vi_payload.get("status") or "").upper()
    # Require an explicit AVAILABLE (or equivalent) with numeric imbalance fields.
    has_exact = any(
        _safe_float(vi_payload.get(key)) is not None
        for key in ("imbalance", "imbalance_ratio", "bid_volume", "ask_volume", "delta")
    )
    if status in {"AVAILABLE", "COMPLETE", "OK"} and has_exact:
        frozen = dict(vi_payload)
        frozen["status"] = "AVAILABLE"
        frozen["observe_only"] = True
        frozen["approximation_allowed"] = False
        return frozen
    if status == "MISSING" or not has_exact:
        return {
            "status": "MISSING",
            "source": vi_payload.get("source"),
            "missing_reason": vi_payload.get("missing_reason")
            or "exact_pit_volume_imbalance_absent",
            "approximation_allowed": False,
            "observe_only": True,
        }
    return {
        "status": "MISSING",
        "source": vi_payload.get("source"),
        "missing_reason": "exact_pit_volume_imbalance_absent",
        "approximation_allowed": False,
        "observe_only": True,
    }


def freeze_breach_market_structure(
    signal: Mapping[str, Any],
    *,
    market_context: Mapping[str, Any] | None = None,
    data_as_of: Any = None,
    source_provider: str = "frozen_breach_candles",
    volume_imbalance: Any = None,
    fvg_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build observe-only ``market_structure`` freeze for a BREACH snapshot.

    Reuses the canonical FVG detector/evaluator. Future candles beyond
    ``data_as_of`` are excluded before detection.
    """
    sig = dict(signal or {})
    ctx = dict(market_context or {})
    candles_in = dict(ctx.get("candles") or ctx.get("ohlcv") or {})
    as_of = _parse_ts(data_as_of or sig.get("trigger_crossed_at") or sig.get("data_as_of"))
    as_of_iso = as_of.isoformat() if as_of else None

    candles_4h = completed_bars_as_of(
        candles_in.get("4h") or candles_in.get("4H") or [],
        as_of=as_of,
        bucket_minutes=240,
    )
    candles_1h = completed_bars_as_of(
        candles_in.get("1h") or candles_in.get("1H") or candles_in.get("60m") or [],
        as_of=as_of,
        bucket_minutes=60,
    )
    pit_candles = {"4h": candles_4h, "1h": candles_1h}
    pit_context = dict(ctx)
    pit_context["candles"] = pit_candles

    side = normalize_signal_side(sig.get("side") or sig.get("direction"))
    price = _safe_float(
        sig.get("underlying_price")
        or sig.get("current_price")
        or sig.get("current_underlying")
        or sig.get("breach_price")
    )
    entry = _safe_float(sig.get("trigger_price") or sig.get("entry_trigger") or price)
    target = _safe_float(
        sig.get("target_price")
        or sig.get("target_underlying")
        or sig.get("pt1")
    )
    stop = _safe_float(sig.get("stop_price") or sig.get("stop"))

    # Reuse evaluator on PIT-filtered candles (no second detector).
    evaluated = fvg_context if isinstance(fvg_context, Mapping) and fvg_context else evaluate_fvg_context(
        {
            **sig,
            "current_underlying": price,
            "underlying_price": price,
            "entry_trigger": entry,
            "trigger_price": entry,
            "target_underlying": target,
            "target_price": target,
        },
        pit_context,
    )

    zones: list[dict[str, Any]] = []
    for tf, rows, policy in (
        ("4h", candles_4h, CANDLE_BUCKET_ANCHOR_POLICY["4h"]),
        ("1h", candles_1h, CANDLE_BUCKET_ANCHOR_POLICY["1h"]),
    ):
        gaps = detect_fair_value_gaps(rows, timeframe=tf)
        session_policy = SESSION_POLICY_4H_ATTESTATION if tf == "4h" else {
            "policy_id": "rth_session_anchored_1h_v1",
            "timezone": "America/New_York",
            "anchor": "09:30",
        }
        for gap in gaps:
            zones.append(
                freeze_fvg_zone(
                    gap,
                    rows=rows,
                    side=side,
                    data_as_of=as_of_iso,
                    source_provider=source_provider,
                    session_policy=session_policy,
                    candle_bucket_anchor_policy=policy,
                )
            )

    aligned = [z for z in zones if z.get("alignment") == "aligned"]
    opposing = [z for z in zones if z.get("alignment") == "opposing"]
    # Prefer active walls for relationship geometry.
    opposing_active = [z for z in opposing if z.get("lifecycle_status") != "broken_reclaimed"]
    aligned_active = [z for z in aligned if z.get("lifecycle_status") != "broken_reclaimed"]

    relationship = classify_breach_fvg_relationship(
        side=side,
        price=price if price is not None else entry,
        target=target,
        stop=stop,
        aligned_zones=aligned_active,
        opposing_zones=opposing_active,
        fvg_context=evaluated,
    )

    vi = freeze_volume_imbalance(
        volume_imbalance
        if volume_imbalance is not None
        else sig.get("volume_imbalance")
        or (ctx.get("volume_imbalance") if isinstance(ctx, Mapping) else None)
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "model_version": MODEL_VERSION,
        "observe_only": True,
        "affected_eligibility": False,
        "data_as_of": as_of_iso,
        "completed_bar_cutoff": as_of_iso,
        "source_provider": source_provider,
        "session_policy_4h": dict(SESSION_POLICY_4H_ATTESTATION),
        "candle_bucket_anchor_policy": dict(CANDLE_BUCKET_ANCHOR_POLICY),
        "side": side,
        "zones": zones,
        "zones_4h": [z for z in zones if z["timeframe"] == "4h"],
        "zones_1h": [z for z in zones if z["timeframe"] == "1h"],
        "relationship": relationship,
        "volume_imbalance": vi,
        "fvg_evaluator_status": evaluated.get("status"),
        "fvg_path_state": ((evaluated.get("diagnostics") or {}).get("path_state")),
        "reuse": {
            "detector": "ap.fair_value_gap.detect_fair_value_gaps",
            "evaluator": "ap.fair_value_gap.evaluate_fvg_context",
            "second_gap_detector": False,
        },
    }


def attach_market_structure_to_breach_evidence(
    evidence: dict[str, Any],
    market_structure: Mapping[str, Any],
) -> dict[str, Any]:
    """Wire freeze under stable ``market_structure`` key (observe_only)."""
    out = dict(evidence or {})
    payload = dict(market_structure or {})
    payload["observe_only"] = True
    payload["affected_eligibility"] = False
    out["market_structure"] = payload
    # Keep VI at evidence root consistent with freeze (MISSING unless exact PIT).
    if "volume_imbalance" in payload:
        out["volume_imbalance"] = payload["volume_imbalance"]
    return out


# ---------------------------------------------------------------------------
# Deferred-gap helpers (observe-only): wick/body, HTF freshness, pullback feats
# ---------------------------------------------------------------------------

BREACH_CONFIRMATION_STYLES = frozenset({
    "BODY_CONFIRMED",
    "WICK_ONLY",
    "UNKNOWN",
    "MISSING",
})

HTF_FRESHNESS_STATUSES = frozenset({"AVAILABLE", "STALE", "MISSING", "ERROR"})

# Default: bar older than 1.5× its bucket length vs data_as_of → STALE.
_DEFAULT_STALE_AGE_MULTIPLE = 1.5


def _ohlc_from_candle(candle: Mapping[str, Any] | None) -> Optional[dict[str, float]]:
    if not isinstance(candle, Mapping):
        return None
    open_p = _safe_float(candle.get("open"))
    high_p = _safe_float(candle.get("high"))
    low_p = _safe_float(candle.get("low"))
    close_p = _safe_float(candle.get("close"))
    if None in (open_p, high_p, low_p, close_p):
        return None
    return {"open": open_p, "high": high_p, "low": low_p, "close": close_p}


def _select_breach_candle(
    candles: Iterable[Mapping[str, Any]] | None,
    *,
    data_as_of: Any = None,
    bucket_minutes: int = 5,
) -> Optional[dict[str, Any]]:
    """Pick the last completed candle at/before breach as_of for OHLC style."""
    as_of = _parse_ts(data_as_of)
    rows = completed_bars_as_of(candles, as_of=as_of, bucket_minutes=bucket_minutes)
    if not rows:
        # Fall back to last row with valid OHLC when timestamps are absent.
        fallback: list[dict[str, Any]] = []
        for row in candles or []:
            if isinstance(row, Mapping) and _ohlc_from_candle(row) is not None:
                fallback.append(dict(row))
        if not fallback:
            return None
        return fallback[-1]
    return rows[-1]


def classify_wick_vs_body_breach(
    *,
    side: str,
    trigger: Any,
    candles: Iterable[Mapping[str, Any]] | None = None,
    breach_candle: Mapping[str, Any] | None = None,
    data_as_of: Any = None,
    bucket_minutes: int = 5,
    source: str | None = None,
) -> dict[str, Any]:
    """Deterministic wick-only vs body-confirmed breach from OHLC at as_of.

    CALL: body-confirmed when close > trigger; wick-only when high > trigger
    but close <= trigger. PUT is symmetric on low/close.
    """
    side_u = str(side or "").strip().upper()
    if side_u in {"BULL", "LONG", "BUY"}:
        side_u = "CALL"
    elif side_u in {"BEAR", "SHORT", "SELL"}:
        side_u = "PUT"
    trigger_f = _safe_float(trigger)
    candle = dict(breach_candle) if isinstance(breach_candle, Mapping) else None
    if candle is None:
        candle = _select_breach_candle(
            candles, data_as_of=data_as_of, bucket_minutes=bucket_minutes
        )
    ohlc = _ohlc_from_candle(candle)
    if trigger_f is None or side_u not in {"CALL", "PUT"}:
        return {
            "status": "MISSING",
            "confirmation_style": "MISSING",
            "side": side_u or None,
            "trigger": trigger_f,
            "source": source,
            "reason": "missing_side_or_trigger",
            "observe_only": True,
            "affected_eligibility": False,
        }
    if ohlc is None:
        return {
            "status": "MISSING",
            "confirmation_style": "MISSING",
            "side": side_u,
            "trigger": trigger_f,
            "ohlc": None,
            "source": source,
            "reason": "breach_candle_ohlc_unavailable",
            "observe_only": True,
            "affected_eligibility": False,
        }

    high_p = ohlc["high"]
    low_p = ohlc["low"]
    close_p = ohlc["close"]
    style = "UNKNOWN"
    reason = "no_trigger_pierce_on_candle"
    if side_u == "CALL":
        if close_p > trigger_f:
            style = "BODY_CONFIRMED"
            reason = "close_beyond_trigger"
        elif high_p > trigger_f and close_p <= trigger_f:
            style = "WICK_ONLY"
            reason = "wick_pierced_trigger_close_did_not_hold"
        elif high_p <= trigger_f:
            style = "UNKNOWN"
            reason = "candle_did_not_pierce_trigger"
    else:  # PUT
        if close_p < trigger_f:
            style = "BODY_CONFIRMED"
            reason = "close_beyond_trigger"
        elif low_p < trigger_f and close_p >= trigger_f:
            style = "WICK_ONLY"
            reason = "wick_pierced_trigger_close_did_not_hold"
        elif low_p >= trigger_f:
            style = "UNKNOWN"
            reason = "candle_did_not_pierce_trigger"

    return {
        "status": "AVAILABLE",
        "confirmation_style": style,
        "side": side_u,
        "trigger": trigger_f,
        "ohlc": {k: round(v, 6) for k, v in ohlc.items()},
        "candle_time": (
            candle.get("time") or candle.get("timestamp") or candle.get("start")
            if isinstance(candle, Mapping)
            else None
        ),
        "source": source or "breach_ohlc",
        "reason": reason,
        "observe_only": True,
        "affected_eligibility": False,
    }


def assess_htf_candle_freshness(
    rows: Iterable[Mapping[str, Any]] | None,
    *,
    data_as_of: Any,
    bucket_minutes: int,
    max_age_seconds: Optional[float] = None,
    label: str = "htf",
) -> dict[str, Any]:
    """Mark HTF candle evidence AVAILABLE / STALE / MISSING vs data_as_of."""
    as_of = _parse_ts(data_as_of)
    max_age = (
        float(max_age_seconds)
        if max_age_seconds is not None
        else float(bucket_minutes) * 60.0 * _DEFAULT_STALE_AGE_MULTIPLE
    )
    pit = completed_bars_as_of(rows, as_of=as_of, bucket_minutes=bucket_minutes)
    if not pit:
        # Candles present but none complete before as_of, or empty.
        raw = [r for r in (rows or []) if isinstance(r, Mapping)]
        if not raw:
            return {
                "status": "MISSING",
                "label": label,
                "bucket_minutes": bucket_minutes,
                "max_age_seconds": max_age,
                "age_seconds": None,
                "last_bar_end": None,
                "data_as_of": as_of.isoformat() if as_of else None,
                "bar_count": 0,
                "observe_only": True,
            }
        # Have rows but none PIT-complete: treat as MISSING for this as_of.
        return {
            "status": "MISSING",
            "label": label,
            "bucket_minutes": bucket_minutes,
            "max_age_seconds": max_age,
            "age_seconds": None,
            "last_bar_end": None,
            "data_as_of": as_of.isoformat() if as_of else None,
            "bar_count": 0,
            "raw_bar_count": len(raw),
            "reason": "no_completed_bars_at_as_of",
            "observe_only": True,
        }

    last = pit[-1]
    start = _candle_time(last)
    if start is None or as_of is None:
        # Cannot prove freshness without timestamps → STALE rather than AVAILABLE.
        return {
            "status": "STALE",
            "label": label,
            "bucket_minutes": bucket_minutes,
            "max_age_seconds": max_age,
            "age_seconds": None,
            "last_bar_end": None,
            "data_as_of": as_of.isoformat() if as_of else None,
            "bar_count": len(pit),
            "reason": "missing_as_of_or_bar_timestamp",
            "observe_only": True,
        }

    bar_end = _bucket_end(start, bucket_minutes=bucket_minutes)
    age = max(0.0, (as_of - bar_end).total_seconds())
    status = "STALE" if age > max_age else "AVAILABLE"
    return {
        "status": status,
        "label": label,
        "bucket_minutes": bucket_minutes,
        "max_age_seconds": max_age,
        "age_seconds": round(age, 3),
        "last_bar_end": bar_end.isoformat(),
        "data_as_of": as_of.isoformat(),
        "bar_count": len(pit),
        "observe_only": True,
    }


def assess_breach_htf_freshness(
    candles: Mapping[str, Any] | None,
    *,
    data_as_of: Any,
) -> dict[str, Any]:
    """Joint 4H/1H freshness block for breach evidence / component statuses."""
    c = candles if isinstance(candles, Mapping) else {}
    four = assess_htf_candle_freshness(
        c.get("4h") or c.get("4H") or [],
        data_as_of=data_as_of,
        bucket_minutes=240,
        label="4h",
    )
    one = assess_htf_candle_freshness(
        c.get("1h") or c.get("1H") or c.get("60m") or [],
        data_as_of=data_as_of,
        bucket_minutes=60,
        label="1h",
    )
    return {
        "4h": four,
        "1h": one,
        "observe_only": True,
        "affected_eligibility": False,
    }


PULLBACK_CANDIDATE_SCHEMA = "pullback_candidate_features_v1"


def freeze_pullback_candidate_features(
    *,
    signal: Mapping[str, Any] | None,
    evidence: Mapping[str, Any] | None = None,
    wick_vs_body: Mapping[str, Any] | None = None,
    market_structure: Mapping[str, Any] | None = None,
    htf_freshness: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Research-only freeze of pullback-candidate features at BREACH.

    Records structure available at breach timestamp only. Never records future
    pullback completion or outcomes. Always ``observe_only=true``.
    """
    sig = signal if isinstance(signal, Mapping) else {}
    ev = evidence if isinstance(evidence, Mapping) else {}
    remaining = ev.get("remaining_opportunity") if isinstance(ev.get("remaining_opportunity"), Mapping) else {}
    fifteen = ev.get("fifteen_minute_confirmation") if isinstance(ev.get("fifteen_minute_confirmation"), Mapping) else {}
    five = ev.get("five_minute_confirmation") if isinstance(ev.get("five_minute_confirmation"), Mapping) else {}
    ms = market_structure if isinstance(market_structure, Mapping) else (
        ev.get("market_structure") if isinstance(ev.get("market_structure"), Mapping) else {}
    )
    relationship = ms.get("relationship") if isinstance(ms.get("relationship"), Mapping) else {}
    wick = wick_vs_body if isinstance(wick_vs_body, Mapping) else (
        ev.get("wick_vs_body_breach") if isinstance(ev.get("wick_vs_body_breach"), Mapping) else {}
    )
    freshness = htf_freshness if isinstance(htf_freshness, Mapping) else (
        ev.get("htf_freshness") if isinstance(ev.get("htf_freshness"), Mapping) else {}
    )
    vi = ev.get("volume_imbalance") if isinstance(ev.get("volume_imbalance"), Mapping) else {}
    if not vi and isinstance(ms.get("volume_imbalance"), Mapping):
        vi = ms["volume_imbalance"]

    side = normalize_signal_side(sig.get("side") or sig.get("direction") or ev.get("side"))
    trigger = _safe_float(sig.get("trigger_price") or sig.get("trigger"))
    price = _safe_float(
        sig.get("underlying_price")
        or sig.get("current_price")
        or sig.get("breach_price")
    )
    target = _safe_float(sig.get("target_price") or sig.get("target") or sig.get("pt1"))
    stop = _safe_float(sig.get("stop_price") or sig.get("stop"))

    # Extension dollars beyond trigger (direction-aware).
    extension = None
    if trigger is not None and price is not None and side in {"CALL", "PUT"}:
        extension = round((price - trigger) if side == "CALL" else (trigger - price), 6)

    move_pct = remaining.get("percent_move_consumed")
    if move_pct is None and remaining.get("move_consumed_pct") is not None:
        try:
            move_pct = float(remaining["move_consumed_pct"]) / 100.0
        except (TypeError, ValueError):
            move_pct = None
    else:
        try:
            move_pct = float(move_pct) if move_pct is not None else None
        except (TypeError, ValueError):
            move_pct = None

    lineage = str(ev.get("breach_lineage") or sig.get("breach_lineage") or "UNKNOWN").upper()
    first_vs_rebreach = "UNKNOWN"
    if lineage in {"INITIAL_BREACH"}:
        first_vs_rebreach = "FIRST_BREACH"
    elif lineage in {
        "REBREACH_AFTER_RESET",
        "RECOVERED_BREACH",
        "DIRECTION_REVERSAL_REBREACH",
    }:
        first_vs_rebreach = "REBREACH"

    # VWAP / volume honesty: never invent; surface MISSING when absent.
    vwap_status = "MISSING"
    vwap_payload = ev.get("vwap_context") if isinstance(ev.get("vwap_context"), Mapping) else None
    if vwap_payload is None and isinstance(sig.get("vwap_context"), Mapping):
        vwap_payload = sig.get("vwap_context")
    if isinstance(vwap_payload, Mapping):
        diag = vwap_payload.get("diagnostics") if isinstance(vwap_payload.get("diagnostics"), Mapping) else {}
        if diag.get("vwap") is not None or vwap_payload.get("vwap") is not None:
            vwap_status = str(vwap_payload.get("status") or "AVAILABLE").upper()
        else:
            vwap_status = "MISSING"

    volume_status = str(vi.get("status") or "MISSING").upper()
    if volume_status not in {"AVAILABLE", "MISSING", "STALE", "ERROR"}:
        volume_status = "MISSING"

    aligned_behind = relationship.get("aligned_retest_zone")
    opposing_ahead = relationship.get("opposing_wall")
    runway = relationship.get("runway_to_opposing_wall")

    return {
        "schema_version": PULLBACK_CANDIDATE_SCHEMA,
        "observe_only": True,
        "affected_eligibility": False,
        "records_future_pullback_outcomes": False,
        "side": side,
        "extension": extension,
        "percent_move_consumed": move_pct,
        "remaining_r": (
            remaining.get("remaining_r")
            if remaining.get("remaining_r") is not None
            else remaining.get("remaining_R")
        ),
        "wick_vs_body": {
            "confirmation_style": wick.get("confirmation_style") or "MISSING",
            "status": wick.get("status") or "MISSING",
            "reason": wick.get("reason"),
        },
        "fifteen_minute_state": {
            "status": fifteen.get("status") or "MISSING",
            "follow_through": fifteen.get("follow_through"),
            "directional_closes": fifteen.get("directional_closes"),
            "bar_count": fifteen.get("bar_count"),
        },
        "five_minute_state": {
            "status": five.get("status") or "MISSING",
            "follow_through": five.get("follow_through"),
            "directional_closes": five.get("directional_closes"),
            "bar_count": five.get("bar_count"),
            "missing_reason": five.get("missing_reason"),
        },
        "nearest_aligned_fvg_behind": aligned_behind,
        "opposing_ahead": opposing_ahead,
        "runway_to_opposing_wall": runway,
        "relationship": relationship.get("relationship"),
        "vwap_status": vwap_status,
        "volume_status": volume_status,
        "first_vs_rebreach": first_vs_rebreach,
        "breach_lineage": lineage,
        "htf_freshness": {
            "4h": (freshness.get("4h") or {}).get("status") if isinstance(freshness.get("4h"), Mapping) else freshness.get("4h"),
            "1h": (freshness.get("1h") or {}).get("status") if isinstance(freshness.get("1h"), Mapping) else freshness.get("1h"),
        },
        "geometry": {
            "trigger": trigger,
            "price": price,
            "target": target,
            "stop": stop,
        },
    }
