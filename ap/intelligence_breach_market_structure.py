"""#435-B: deterministic observe-only BREACH market-structure features.

Reuses ``ap.fair_value_gap.detect_fair_value_gaps``.  No broker/selector/order/
position/proof/queue authority lives here.  The 5m/15m "50% body through with
strength" result is telemetry only; later policy may consume it after replay.
"""

from __future__ import annotations

from copy import deepcopy
import hashlib
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Optional
from zoneinfo import ZoneInfo

from ap.fair_value_gap import FairValueGap, detect_fair_value_gaps
from ap.score_profile_side import normalize_signal_side

ET = ZoneInfo("America/New_York")
SCHEMA_VERSION = "breach_market_structure_v1"
MODEL_VERSION = "canonical_fvg_435b_v1"
SESSION_POLICY = {
    "session": "US_EQUITY_RTH",
    "timezone": "America/New_York",
    "anchor": "09:30",
    "4h_buckets": ["[09:30-13:30)", "[13:30-16:00]"],
    "1h_bucket_minutes": 60,
}
KNOWN_REGIMES = {
    "TREND_CONTINUATION", "TREND_PULLBACK", "RANGE", "BREAKOUT",
    "BREAKOUT_RETEST", "MEAN_REVERSION", "VOLATILITY_EXPANSION",
    "EXHAUSTION", "REVERSAL_RISK",
}


def _num(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    try:
        out = float(value)
        return out if math.isfinite(out) else None
    except (TypeError, ValueError):
        return None


def _ts(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    if dt.tzinfo is None or dt.utcoffset() is None:
        return None
    return dt.astimezone(timezone.utc)


def _row_ts(row: Mapping[str, Any]) -> Optional[datetime]:
    return _ts(row.get("time") or row.get("timestamp") or row.get("start"))


def _bucket_end(start: datetime, minutes: int) -> Optional[datetime]:
    if minutes <= 0:
        return None
    local = start.astimezone(ET)
    session_open = local.replace(hour=9, minute=30, second=0, microsecond=0)
    close = local.replace(hour=16, minute=0, second=0, microsecond=0)
    if local.weekday() >= 5 or local < session_open or local >= close:
        return None
    offset_seconds = (local - session_open).total_seconds()
    if offset_seconds % 60 != 0:
        return None
    offset_minutes = int(offset_seconds // 60)
    if minutes == 240:
        if offset_minutes not in {0, 240}:
            return None
    elif offset_minutes % minutes != 0:
        return None
    return min(local + timedelta(minutes=minutes), close).astimezone(timezone.utc)


def completed_bars_as_of(
    rows: Iterable[Mapping[str, Any]] | None, *, as_of: Any, minutes: int
) -> list[dict[str, Any]]:
    """Only valid completed OHLC bars may influence the frozen snapshot."""
    cutoff = _ts(as_of)
    if cutoff is None:
        return []
    out: list[dict[str, Any]] = []
    for raw in rows or []:
        if not isinstance(raw, Mapping):
            continue
        start = _row_ts(raw)
        vals = [_num(raw.get(k)) for k in ("open", "high", "low", "close")]
        if start is None or any(v is None for v in vals):
            continue
        o, h, l, c = vals
        if h < l or not (l <= o <= h and l <= c <= h):
            continue
        bucket_end = _bucket_end(start, minutes)
        if bucket_end is None or bucket_end > cutoff:
            continue
        out.append(dict(raw))
    out.sort(key=lambda row: _row_ts(row) or datetime.min.replace(tzinfo=timezone.utc))
    return out


def _zone_id(tf: str, gap: FairValueGap, start: Optional[str], end: Optional[str]) -> str:
    raw = "|".join((
        tf, gap.direction, f"{gap.low:.6f}", f"{gap.high:.6f}",
        start or "", end or "",
    ))
    return f"fvg_{tf}_{hashlib.sha256(raw.encode()).hexdigest()[:20]}"


def fvg_position(
    price: Any, low: Any, high: Any, *, tolerance: float = 0.01
) -> dict[str, Any]:
    """Return one of inside / above / below / boundary, never a guessed value."""
    p, lo, hi = _num(price), _num(low), _num(high)
    tol = max(0.0, _num(tolerance) or 0.0)
    if p is None or lo is None or hi is None or hi < lo:
        return {"status": "MISSING", "position": "unknown", "distance": None}
    dl, dh = abs(p - lo), abs(p - hi)
    if min(dl, dh) <= tol:
        boundary = "low" if dl <= dh else "high"
        return {
            "status": "AVAILABLE", "position": "boundary", "boundary": boundary,
            "distance": round(min(dl, dh), 6),
        }
    if lo < p < hi:
        return {
            "status": "AVAILABLE", "position": "inside", "boundary": None,
            "distance": round(min(dl, dh), 6),
        }
    return {
        "status": "AVAILABLE",
        "position": "above" if p > hi else "below",
        "boundary": None,
        "distance": round((p - hi) if p > hi else (lo - p), 6),
    }


def _alignment(direction: str, side: str) -> str:
    aligned = (side == "CALL" and direction == "bullish") or (
        side == "PUT" and direction == "bearish"
    )
    return "aligned" if aligned else "opposing"


def _break_boundary(zone: Mapping[str, Any] | None, side: str) -> Optional[float]:
    if not isinstance(zone, Mapping) or zone.get("alignment") != "opposing":
        return None
    if side == "PUT" and zone.get("direction") == "bullish":
        return _num(zone.get("low"))
    if side == "CALL" and zone.get("direction") == "bearish":
        return _num(zone.get("high"))
    return None


def _frozen_breach_price(
    signal: Mapping[str, Any], *, as_of: Any
) -> tuple[Optional[float], Optional[str]]:
    """Use only explicitly frozen breach-price evidence.

    Generic ``underlying_price`` and ``current_price`` aliases are deliberately
    excluded: their provenance is ambiguous and they may be a later worker-time
    quote.  Mapping-shaped evidence must carry an aware timestamp no later than
    the snapshot cutoff.
    """
    cutoff = _ts(as_of)
    if cutoff is None:
        return None, None
    if "_pit_underlying_observation" in signal:
        raw = signal.get("_pit_underlying_observation")
        if not isinstance(raw, Mapping):
            return None, None
        price = next(
            (
                _num(raw.get(key))
                for key in ("price", "underlying_price", "breach_price", "value")
                if _num(raw.get(key)) is not None
            ),
            None,
        )
        observed = _ts(
            next(
                (
                    raw.get(key)
                    for key in ("as_of", "data_as_of", "timestamp", "observed_at", "time")
                    if raw.get(key) not in (None, "")
                ),
                None,
            )
        )
        if price is None or observed is None or observed > cutoff:
            return None, None
        return price, "point_in_time.underlying_observation"
    candidates = (
        ("signal.breach_price", signal.get("breach_price")),
        ("signal.frozen_underlying_price", signal.get("frozen_underlying_price")),
        ("signal.underlying_at_breach", signal.get("underlying_at_breach")),
        ("signal.price_at_breach", signal.get("price_at_breach")),
        ("signal.breach_evidence", signal.get("breach_evidence")),
    )
    for source, raw in candidates:
        if isinstance(raw, Mapping):
            price = None
            for key in ("price", "underlying_price", "breach_price", "value"):
                price = _num(raw.get(key))
                if price is not None:
                    break
            if price is None:
                continue
            raw_ts = next(
                (
                    raw.get(key)
                    for key in ("as_of", "data_as_of", "timestamp", "observed_at", "time")
                    if raw.get(key) not in (None, "")
                ),
                None,
            )
            observed = _ts(raw_ts)
            if observed is None or (cutoff is not None and observed > cutoff):
                continue
            return price, source
        price = _num(raw)
        if price is not None:
            return price, source
    return None, None


def freeze_fvg_zones(
    *, side: str, price: Optional[float], candles: Mapping[str, Any],
    as_of: Any, tolerance: float = 0.01,
) -> list[dict[str, Any]]:
    """Freeze canonical 4H/1H FVG geometry and current price relationship."""
    zones: list[dict[str, Any]] = []
    for tf, minutes in (("4h", 240), ("1h", 60)):
        rows = completed_bars_as_of(
            candles.get(tf) or candles.get(tf.upper()) or [],
            as_of=as_of, minutes=minutes,
        )[-120:]
        for gap in detect_fair_value_gaps(rows, timeframe=tf):
            start_dt = _row_ts(rows[gap.start_index]) if 0 <= gap.start_index < len(rows) else None
            end_dt = _row_ts(rows[gap.end_index]) if 0 <= gap.end_index < len(rows) else None
            start = start_dt.isoformat() if start_dt else None
            end = _bucket_end(end_dt, minutes).isoformat() if end_dt else None
            zone = {
                "zone_id": _zone_id(tf, gap, start, end),
                "timeframe": tf, "direction": gap.direction,
                "low": round(gap.low, 6), "midpoint": round(gap.midpoint, 6),
                "high": round(gap.high, 6), "fill_pct": round(gap.fill_pct, 6),
                "lifecycle_status": gap.status, "mitigated": bool(gap.mitigated),
                "source_candle_start": start, "source_candle_end": end,
                "alignment": _alignment(gap.direction, side),
            }
            zone["price_position"] = fvg_position(
                price, gap.low, gap.high, tolerance=tolerance
            )
            zone["break_boundary"] = _break_boundary(zone, side)
            zones.append(zone)
    return zones


def _missing_volume_imbalance(
    value: Any, reason: str
) -> dict[str, Any]:
    return {
        "status": "MISSING",
        "source": value.get("source") if isinstance(value, Mapping) else None,
        "missing_reason": reason,
        "approximation_allowed": False,
    }


def freeze_volume_imbalance(
    value: Any, *, as_of: Any = None, require_as_of: bool = False
) -> dict[str, Any]:
    """Accept VI only with explicit, non-approximate PIT provenance."""
    cutoff = _ts(as_of) if as_of not in (None, "") else None
    if require_as_of and cutoff is None:
        return _missing_volume_imbalance(value, "snapshot_as_of_missing_or_invalid")
    if not isinstance(value, Mapping):
        return _missing_volume_imbalance(value, "exact_pit_volume_imbalance_absent")

    raw_status = value.get("status")
    if not isinstance(raw_status, str) or raw_status.upper() not in {
        "AVAILABLE", "COMPLETE", "OK"
    }:
        return _missing_volume_imbalance(value, "volume_imbalance_status_unavailable")

    source = value.get("source")
    if not isinstance(source, str) or not source.strip():
        return _missing_volume_imbalance(value, "volume_imbalance_source_missing")

    raw_ts = next(
        (
            value.get(key)
            for key in ("as_of", "data_as_of", "timestamp", "observed_at", "time")
            if value.get(key) not in (None, "")
        ),
        None,
    )
    observed = _ts(raw_ts)
    if observed is None:
        return _missing_volume_imbalance(
            value, "volume_imbalance_timestamp_missing_or_invalid"
        )
    if cutoff is not None and observed > cutoff:
        return _missing_volume_imbalance(value, "volume_imbalance_after_snapshot")

    if any(
        key in value and value.get(key) is not False
        for key in ("approximation_allowed", "approximate", "is_approximate")
    ) or ("exact" in value and value.get("exact") is not True):
        return _missing_volume_imbalance(value, "volume_imbalance_approximation_disallowed")

    numeric_keys = ("imbalance", "imbalance_ratio", "bid_volume", "ask_volume", "delta")
    numeric = False
    for key in numeric_keys:
        if key not in value or value.get(key) in (None, ""):
            continue
        if _num(value.get(key)) is None:
            return _missing_volume_imbalance(
                value, "volume_imbalance_numeric_value_invalid"
            )
        numeric = True
    if not numeric:
        return _missing_volume_imbalance(value, "volume_imbalance_numeric_value_missing")

    out = dict(value)
    out.update(
        status="AVAILABLE",
        approximation_allowed=False,
        exact=True,
        pit_timestamp=observed.isoformat(),
    )
    return out


def _empty_penetration(tf: str, boundary: Optional[float]) -> dict[str, Any]:
    return {
        "status": "MISSING", "timeframe": tf, "boundary": boundary,
        "style": "MISSING", "wick_beyond": None, "close_beyond": None,
        "directional_body": None, "body_beyond_boundary_ratio": None,
        "body_to_range_ratio": None, "fifty_percent_body_through": False,
        "strong_body": False, "strong_break_observed": False,
    }


def measure_boundary_penetration(
    *, side: str, boundary: Any, candle: Mapping[str, Any] | None, timeframe: str,
) -> dict[str, Any]:
    """Freeze wick/body acceptance through an opposing FVG far boundary.

    A strong break means the close is beyond the boundary in trade direction,
    >=50% of the real body is beyond that boundary, and the real body is >=50%
    of the candle's full high-low range.
    """
    b = _num(boundary)
    if b is None or side not in {"CALL", "PUT"} or not isinstance(candle, Mapping):
        return _empty_penetration(timeframe, b)
    vals = [_num(candle.get(k)) for k in ("open", "high", "low", "close")]
    if any(v is None for v in vals):
        return _empty_penetration(timeframe, b)
    o, h, l, c = (float(v) for v in vals)
    if h < l or not (l <= o <= h and l <= c <= h):
        out = _empty_penetration(timeframe, b)
        out["status"], out["style"] = "INVALID", "INVALID"
        return out

    body_lo, body_hi = min(o, c), max(o, c)
    body, span = body_hi - body_lo, h - l
    body_to_range = body / span if span > 0 else 0.0
    if side == "CALL":
        wick_beyond, close_beyond, directional = h > b, c > b, c > o
        beyond = max(0.0, body_hi - max(body_lo, b))
    else:
        wick_beyond, close_beyond, directional = l < b, c < b, c < o
        beyond = max(0.0, min(body_hi, b) - body_lo)

    beyond_ratio = beyond / body if body > 0 else 0.0
    fifty = bool(directional and close_beyond and beyond_ratio >= 0.5)
    strong_body = bool(directional and body_to_range >= 0.5)
    strong = bool(fifty and strong_body)
    style = (
        "NO_PENETRATION" if not wick_beyond else
        "WICK_ONLY" if not close_beyond else
        "STRONG_BODY_50_PLUS" if strong else
        "BODY_CLOSE_WEAK_OR_SHALLOW"
    )
    return {
        "status": "AVAILABLE", "timeframe": timeframe,
        "candle_time": candle.get("time") or candle.get("timestamp") or candle.get("start"),
        "boundary": round(b, 6), "style": style,
        "wick_beyond": wick_beyond, "close_beyond": close_beyond,
        "directional_body": directional,
        "body_beyond_boundary_ratio": round(beyond_ratio, 6),
        "body_to_range_ratio": round(body_to_range, 6),
        "fifty_percent_body_through": fifty, "strong_body": strong_body,
        "strong_break_observed": strong,
        "thresholds": {"body_beyond_boundary_ratio": 0.5, "body_to_range_ratio": 0.5},
    }


def _last_boundary_interaction(
    rows: Any, *, boundary: Optional[float], side: str, as_of: Any, minutes: int,
    not_before: Optional[datetime] = None,
) -> Optional[dict[str, Any]]:
    if boundary is None:
        return None
    completed = completed_bars_as_of(rows or [], as_of=as_of, minutes=minutes)
    for row in reversed(completed):
        if not_before is not None:
            row_start = _row_ts(row)
            if row_start is None or row_start < not_before:
                continue
        h, l = _num(row.get("high")), _num(row.get("low"))
        if h is None or l is None:
            continue
        if l <= boundary <= h or (side == "CALL" and h > boundary) or (
            side == "PUT" and l < boundary
        ):
            return row
    return None


def _coverage_is_authoritative(value: Any) -> bool:
    if not isinstance(value, Mapping):
        return False
    status = str(value.get("status") or "").upper()
    provider_status = str(value.get("provider_status") or "").lower()
    return (
        value.get("authoritative") is True
        and value.get("coverage_complete") is True
        and status not in {"STALE", "MISSING", "ERROR", "FAILED"}
        and provider_status not in {"error", "failed", "failure"}
    )


def _provenance_for_timeframe(
    provenance: Mapping[str, Any] | None, timeframe: str
) -> Any:
    if not isinstance(provenance, Mapping):
        return None
    key = "intraday_5m" if timeframe == "5m" else "intraday"
    return deepcopy(provenance.get(key, provenance.get(timeframe)))


def _apply_penetration_authority(
    measured: dict[str, Any], *, coverage: Any, provenance: Any, timeframe: str
) -> dict[str, Any]:
    """Keep raw measurements visible while refusing untrusted break authority."""
    if not isinstance(coverage, Mapping):
        return measured

    source_coverage = coverage.get(timeframe)
    authoritative = _coverage_is_authoritative(source_coverage)
    measured["source_coverage"] = deepcopy(source_coverage)
    measured["source_provenance"] = _provenance_for_timeframe(provenance, timeframe)
    measured["coverage_authoritative"] = authoritative
    measured["measured_style"] = measured.get("style")
    measured["measured_fifty_percent_body_through"] = bool(
        measured.get("fifty_percent_body_through")
    )
    measured["measured_strong_body"] = bool(measured.get("strong_body"))
    measured["measured_strong_break_observed"] = bool(
        measured.get("strong_break_observed")
    )
    if not authoritative:
        if measured.get("status") == "AVAILABLE":
            measured["style"] = f"NON_AUTHORITATIVE_{measured.get('style')}"
        measured["fifty_percent_body_through"] = False
        measured["strong_body"] = False
        measured["strong_break_observed"] = False
    measured["strong_break_authoritative"] = bool(
        authoritative and measured.get("strong_break_observed") is True
    )
    return measured


def freeze_penetration(
    *, side: str, zone: Mapping[str, Any] | None,
    candles: Mapping[str, Any], as_of: Any,
    coverage: Mapping[str, Any] | None = None,
    provenance: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    boundary = _break_boundary(zone, side)
    not_before = None
    invalid_zone_timestamp = False
    if isinstance(zone, Mapping) and zone.get("source_candle_end") not in (None, ""):
        not_before = _ts(zone.get("source_candle_end"))
        invalid_zone_timestamp = not_before is None
    results = {}
    for tf, minutes in (("5m", 5), ("15m", 15)):
        rows = candles.get(tf) or candles.get(tf.upper()) or (
            candles.get("15min") if tf == "15m" else []
        )
        candle = None if invalid_zone_timestamp else _last_boundary_interaction(
            rows,
            boundary=boundary,
            side=side,
            as_of=as_of,
            minutes=minutes,
            not_before=not_before,
        )
        results[tf] = _apply_penetration_authority(
            measure_boundary_penetration(
                side=side, boundary=boundary, candle=candle, timeframe=tf
            ),
            coverage=coverage,
            provenance=provenance,
            timeframe=tf,
        )
    return {
        "status": "AVAILABLE" if boundary is not None else "NO_OPPOSING_FVG",
        "zone_id": zone.get("zone_id") if isinstance(zone, Mapping) else None,
        "boundary": boundary, **results,
        "strong_break_on_5m_or_15m": any(
            results[tf]["strong_break_observed"] for tf in ("5m", "15m")
        ),
        "strong_break_authoritative": (
            any(
                results[tf].get("strong_break_authoritative") is True
                for tf in ("5m", "15m")
            )
            if isinstance(coverage, Mapping)
            else None
        ),
        "observe_only": True, "affected_eligibility": False,
    }


def _nearest_opposing(zones: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
    active = [
        z for z in zones
        if z["alignment"] == "opposing"
        and z["lifecycle_status"] not in {"broken", "reclaimed", "broken_reclaimed"}
    ]
    if not active:
        return None

    def distance(zone: Mapping[str, Any]) -> Optional[float]:
        value = _num((zone.get("price_position") or {}).get("distance"))
        return value

    ranked = [(distance(zone), zone) for zone in active]
    ranked = [(value, zone) for value, zone in ranked if value is not None]
    return min(ranked, key=lambda item: item[0])[1] if ranked else None


def freeze_pullback_reclaim_rebreach(
    *, side: str, trigger: Any, candles: Mapping[str, Any],
    as_of: Any, supplied_lineage: Any, breach_at: Any = None,
) -> dict[str, Any]:
    """Completed-close sequence only; supplied lifecycle lineage stays separate."""
    t = _num(trigger)
    breach_dt = _ts(breach_at) if breach_at not in (None, "") else None
    source_tf, source_minutes, rows = None, None, []
    for tf, minutes in (("5m", 5), ("15m", 15)):
        raw = candles.get(tf) or candles.get(tf.upper()) or (
            candles.get("15min") if tf == "15m" else []
        )
        completed = completed_bars_as_of(raw, as_of=as_of, minutes=minutes)
        if breach_at not in (None, "") and breach_dt is None:
            completed = []
        elif breach_dt is not None:
            completed = [
                row for row in completed
                if (
                    _row_ts(row) is not None
                    and _bucket_end(_row_ts(row), minutes) is not None
                    and _bucket_end(_row_ts(row), minutes) > breach_dt
                )
            ]
        if completed:
            source_tf, source_minutes, rows = tf, minutes, completed
            break
    lineage = str(supplied_lineage or "UNKNOWN").upper()
    if (
        side not in {"CALL", "PUT"}
        or t is None
        or not rows
        or breach_at in (None, "")
        or breach_dt is None
    ):
        return {
            "status": "MISSING", "source_timeframe": source_tf,
            "pullback_state": "UNKNOWN", "returned_to_pretrigger_side": None,
            "rebreach_after_pullback": None, "supplied_breach_lineage": lineage,
        }

    closes = [_num(row.get("close")) for row in rows]

    def beyond(value: Optional[float]) -> bool:
        return bool(value is not None and (value > t if side == "CALL" else value < t))

    first = next((i for i, value in enumerate(closes) if beyond(value)), None)
    if first is None:
        return {
            "status": "AVAILABLE", "source_timeframe": source_tf,
            "pullback_state": "NO_CONFIRMED_BREACH",
            "returned_to_pretrigger_side": False, "rebreach_after_pullback": False,
            "supplied_breach_lineage": lineage,
        }
    pull = next((i for i in range(first + 1, len(closes)) if not beyond(closes[i])), None)
    rebreach = (
        next((i for i in range(pull + 1, len(closes)) if beyond(closes[i])), None)
        if pull is not None else None
    )
    state = "NO_PULLBACK" if pull is None else "RECLAIMING" if rebreach is not None else "PULLBACK_ACTIVE"

    def stamp(index: Optional[int]) -> Optional[str]:
        if index is None:
            return None
        start = _row_ts(rows[index])
        dt = _bucket_end(start, source_minutes or 0) if start is not None else None
        return dt.isoformat() if dt else None

    return {
        "status": "AVAILABLE", "source_timeframe": source_tf,
        "pullback_state": state, "returned_to_pretrigger_side": pull is not None,
        "rebreach_after_pullback": rebreach is not None,
        "first_confirmed_breach_ts": stamp(first), "pullback_ts": stamp(pull),
        "rebreach_ts": stamp(rebreach), "supplied_breach_lineage": lineage,
    }


def freeze_regime(signal: Mapping[str, Any], context: Mapping[str, Any]) -> dict[str, Any]:
    """Preserve explicit upstream regime. Do not manufacture one from missing data."""
    for source, raw in (
        ("signal.regime", signal.get("regime")),
        ("signal.regime_context", signal.get("regime_context")),
        ("market_context.regime", context.get("regime")),
    ):
        value = (raw.get("regime") or raw.get("label") or raw.get("state")) if isinstance(raw, Mapping) else raw
        value = str(value or "").upper()
        if value in KNOWN_REGIMES:
            return {"status": "AVAILABLE", "regime": value, "source": source}
    return {"status": "MISSING", "regime": "UNKNOWN", "source": None}


def classify_setup(
    *, regime: Mapping[str, Any], zone: Mapping[str, Any] | None,
    penetration: Mapping[str, Any], pullback: Mapping[str, Any],
) -> dict[str, Any]:
    """Research posture only. ``affected_eligibility`` is permanently false here."""
    state = pullback.get("pullback_state")
    zone_pos = ((zone or {}).get("price_position") or {}).get("position")
    strong = bool(penetration.get("strong_break_on_5m_or_15m"))
    lineage = str(pullback.get("supplied_breach_lineage") or "")
    archetype, posture, reasons = "UNKNOWN", "INSUFFICIENT_DATA", []

    if strong:
        archetype, posture = "MOMENTUM_BREAKOUT", "ENTER_NOW_CANDIDATE"
        reasons.append("opposing_fvg_strong_body_break_observed")
    elif state == "RECLAIMING" and zone is not None:
        archetype, posture = "FVG_RETEST", "REARM_CANDIDATE"
        reasons.append("pullback_then_rebreach_near_fvg")
    elif state == "RECLAIMING":
        archetype, posture = "BREAKOUT_RETEST", "REARM_CANDIDATE"
        reasons.append("pullback_then_rebreach")
    elif "REBREACH" in lineage:
        archetype, posture = "SECOND_TOUCH", "REARM_CANDIDATE"
        reasons.append("supplied_rebreach_lineage")
    elif state == "PULLBACK_ACTIVE":
        archetype = "FVG_RETEST" if zone is not None else "BREAKOUT_RETEST"
        posture = "WAIT_RECLAIM_CANDIDATE"
        reasons.append("returned_to_pretrigger_side")
    elif zone is not None and zone_pos in {"inside", "boundary"}:
        archetype, posture = "FVG_RETEST", "WAIT_RECLAIM_CANDIDATE"
        reasons.append("opposing_fvg_not_strongly_broken")
    elif regime.get("regime") == "TREND_CONTINUATION":
        archetype = "TREND_CONTINUATION"
        reasons.append("explicit_upstream_trend_continuation")

    return {
        "schema_version": "setup_archetype_v1", "setup_archetype": archetype,
        "entry_posture_observe_only": posture, "reason_codes": reasons,
        "observe_only": True, "affected_eligibility": False,
    }


def freeze_breach_market_structure(
    signal: Mapping[str, Any], *, market_context: Mapping[str, Any] | None = None,
    data_as_of: Any = None, volume_imbalance: Any = None,
    boundary_tolerance: float = 0.01,
) -> dict[str, Any]:
    """Freeze #435-B evidence. The input mappings are never mutated."""
    sig, ctx = dict(signal or {}), dict(market_context or {})
    candles = dict(ctx.get("candles") or ctx.get("ohlcv") or {})
    side = normalize_signal_side(sig.get("side") or sig.get("direction"))
    side = side if side in {"CALL", "PUT"} else "UNKNOWN"
    trigger_obj = sig.get("trigger") if isinstance(sig.get("trigger"), Mapping) else {}
    trigger = _num(
        sig.get("trigger_price") or sig.get("entry_trigger") or trigger_obj.get("entry")
    )
    as_of = data_as_of or sig.get("trigger_crossed_at") or sig.get("breach_at") or sig.get("data_as_of")
    parsed = _ts(as_of)
    price, price_source = _frozen_breach_price(sig, as_of=as_of)
    breach_at = sig.get("trigger_crossed_at") or sig.get("breach_at")
    zones = freeze_fvg_zones(
        side=side, price=price, candles=candles, as_of=as_of,
        tolerance=boundary_tolerance,
    )
    zone = _nearest_opposing(zones)
    penetration = freeze_penetration(
        side=side,
        zone=zone,
        candles=candles,
        as_of=as_of,
        coverage=ctx.get("_pit_coverage"),
        provenance=ctx.get("_pit_provenance"),
    )
    pullback = freeze_pullback_reclaim_rebreach(
        side=side, trigger=trigger, candles=candles, as_of=as_of,
        supplied_lineage=sig.get("breach_lineage"), breach_at=breach_at,
    )
    regime = freeze_regime(sig, ctx)
    vi = volume_imbalance if volume_imbalance is not None else (
        sig.get("volume_imbalance") or ctx.get("volume_imbalance")
    )
    return {
        "schema_version": SCHEMA_VERSION, "model_version": MODEL_VERSION,
        "observe_only": True, "affected_eligibility": False,
        "data_as_of": parsed.isoformat() if parsed else None,
        "side": side, "underlying_price": price,
        "underlying_price_source": price_source, "trigger_price": trigger,
        "session_policy": dict(SESSION_POLICY), "fvg_zones": zones,
        "relevant_opposing_fvg": zone, "fvg_penetration": penetration,
        "volume_imbalance": freeze_volume_imbalance(
            vi, as_of=parsed, require_as_of=True
        ),
        "pullback_reclaim_rebreach": pullback, "regime": regime,
        "setup": classify_setup(
            regime=regime, zone=zone, penetration=penetration, pullback=pullback
        ),
        "invariants": {
            "broker_authority": False, "admission_authority": False,
            "order_mutation_authority": False, "position_mutation_authority": False,
            "proof_trade_mutation_authority": False, "queue_mutation_authority": False,
        },
    }


def _pit_candles(raw: Any) -> list[dict[str, Any]]:
    return [dict(row) for row in raw if isinstance(row, Mapping)] if isinstance(raw, list) else []


def _pit_observation(
    raw: Any, *, as_of: Optional[datetime]
) -> Optional[dict[str, Any]]:
    if as_of is None or not isinstance(raw, Mapping):
        return None
    price = next(
        (
            _num(raw.get(key))
            for key in ("price", "underlying_price", "breach_price", "value")
            if _num(raw.get(key)) is not None
        ),
        None,
    )
    source = raw.get("source")
    if price is None or not isinstance(source, str) or not source.strip():
        return None
    for key in ("observed_at", "source_timestamp"):
        value = raw.get(key)
        if value in (None, ""):
            continue
        parsed = _ts(value)
        if parsed is None or parsed > as_of:
            return None
    return {
        "price": price,
        "source": source,
        "observed_at": raw.get("observed_at") or as_of.isoformat(),
        "source_timestamp": raw.get("source_timestamp") or as_of.isoformat(),
        "as_of": as_of.isoformat(),
    }


def freeze_breach_market_structure_from_pit(
    signal: Mapping[str, Any],
    point_in_time: Mapping[str, Any] | None,
    *,
    volume_imbalance: Any = None,
    boundary_tolerance: float = 0.01,
) -> dict[str, Any]:
    """Adapt the canonical #614 BREACH envelope without refetching or rebuilding."""
    pit = dict(point_in_time) if isinstance(point_in_time, Mapping) else {}
    parsed_as_of = _ts(pit.get("as_of"))
    data_sources = pit.get("data_sources")
    data_sources = data_sources if isinstance(data_sources, Mapping) else {}
    raw_candles = data_sources.get("candles")
    raw_candles = raw_candles if isinstance(raw_candles, Mapping) else {}
    intervals = {"5m": 5, "15m": 15, "1h": 60, "4h": 240}
    candles = {
        timeframe: (
            completed_bars_as_of(
                _pit_candles(raw_candles.get(timeframe)),
                as_of=parsed_as_of,
                minutes=minutes,
            )
            if parsed_as_of is not None
            else []
        )
        for timeframe, minutes in intervals.items()
    }
    raw_coverage = data_sources.get("coverage")
    coverage = dict(raw_coverage) if isinstance(raw_coverage, Mapping) else {}
    raw_provenance = pit.get("provenance")
    provenance = dict(raw_provenance) if isinstance(raw_provenance, Mapping) else {}
    observation = _pit_observation(
        pit.get("underlying_observation"), as_of=parsed_as_of
    )

    canonical_signal = dict(signal or {})
    for key in (
        "breach_price",
        "frozen_underlying_price",
        "underlying_at_breach",
        "price_at_breach",
        "breach_evidence",
        "volume_imbalance",
    ):
        canonical_signal.pop(key, None)
    canonical_signal["_pit_underlying_observation"] = observation
    if parsed_as_of is None:
        for key in ("trigger_crossed_at", "breach_at", "data_as_of"):
            canonical_signal.pop(key, None)
        candles = {timeframe: [] for timeframe in ("5m", "15m", "1h", "4h")}

    frozen = freeze_breach_market_structure(
        canonical_signal,
        market_context={
            "candles": candles,
            "_pit_coverage": coverage,
            "_pit_provenance": provenance,
        },
        data_as_of=parsed_as_of.isoformat() if parsed_as_of else None,
        volume_imbalance=volume_imbalance,
        boundary_tolerance=boundary_tolerance,
    )
    frozen["point_in_time"] = {
        "phase": pit.get("phase"),
        "as_of": pit.get("as_of"),
        "collected_at": pit.get("collected_at"),
        "data_sources": {
            "candles": deepcopy(candles),
            "coverage": deepcopy(coverage),
        },
        "underlying_observation": deepcopy(pit.get("underlying_observation")),
        "errors": deepcopy(pit.get("errors") or []),
        "provenance": deepcopy(provenance),
    }
    frozen["data_coverage"] = deepcopy(coverage)
    frozen["data_provenance"] = deepcopy(provenance)
    frozen["underlying_observation"] = deepcopy(pit.get("underlying_observation"))
    frozen["market_data"] = {"candles": deepcopy(candles)}
    return frozen
