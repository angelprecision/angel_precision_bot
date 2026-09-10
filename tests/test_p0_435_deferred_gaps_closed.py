"""Close deferred #435 behavioral gaps — observe-only fixtures (no entry authority)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from ap.intelligence_breach_market_structure import (  # noqa: E402
    assess_breach_htf_freshness,
    assess_htf_candle_freshness,
    classify_wick_vs_body_breach,
    freeze_pullback_candidate_features,
)
from ap.intelligence_context_materializer import (  # noqa: E402
    ENTRY_TIMING_CANDIDATE_ENUMS,
    _build_breach_evidence,
    classify_entry_readiness_observe_only,
    map_entry_timing_candidate,
)


def _candle(ts: str, o: float, h: float, l: float, c: float) -> dict:
    return {"time": ts, "open": o, "high": h, "low": l, "close": c, "volume": 1000}


# ---------------------------------------------------------------------------
# 1) Wick-only vs body-confirmed
# ---------------------------------------------------------------------------


def test_wick_only_call_breach():
    # Pierces trigger on high but closes back below.
    rows = [
        _candle("2026-09-09T10:00:00-04:00", 99.5, 100.4, 99.4, 99.8),
    ]
    out = classify_wick_vs_body_breach(
        side="CALL",
        trigger=100.0,
        candles=rows,
        data_as_of="2026-09-09T14:05:00+00:00",
        bucket_minutes=5,
    )
    assert out["confirmation_style"] == "WICK_ONLY"
    assert out["status"] == "AVAILABLE"
    assert out["observe_only"] is True
    assert out["affected_eligibility"] is False


def test_body_confirmed_put_breach():
    rows = [
        _candle("2026-09-09T10:00:00-04:00", 100.2, 100.3, 99.4, 99.5),
    ]
    out = classify_wick_vs_body_breach(
        side="PUT",
        trigger=100.0,
        candles=rows,
        data_as_of="2026-09-09T14:05:00+00:00",
        bucket_minutes=5,
    )
    assert out["confirmation_style"] == "BODY_CONFIRMED"
    assert out["ohlc"]["close"] < 100.0


def test_wick_missing_without_ohlc():
    out = classify_wick_vs_body_breach(side="CALL", trigger=100.0, candles=[])
    assert out["confirmation_style"] == "MISSING"
    assert out["status"] == "MISSING"


def test_wick_only_forces_wait_on_first_breach_readiness():
    signal = {
        "side": "CALL",
        "trigger_price": 100.0,
        "stop_price": 98.0,
        "target_price": 106.0,
        "underlying_price": 100.2,
    }
    evidence = {
        "breach_lineage": "INITIAL_BREACH",
        "remaining_opportunity": {
            "remaining_r": 2.0,
            "percent_move_consumed": 0.1,
        },
        "fifteen_minute_confirmation": {"status": "AVAILABLE", "follow_through": True},
        "five_minute_confirmation": {"status": "AVAILABLE", "follow_through": True},
        "wick_vs_body_breach": {
            "confirmation_style": "WICK_ONLY",
            "status": "AVAILABLE",
        },
    }
    result = classify_entry_readiness_observe_only(signal, evidence=evidence)
    assert result["classification"] == "WAIT_CONFIRMATION"
    assert result["entry_timing_candidate"] == "WAIT_PULLBACK_CANDIDATE"
    assert result["observe_only"] is True
    assert result["affected_eligibility"] is False
    assert "wick_only_breach_on_first_touch" in result["diagnostics"]["reasons"]


# ---------------------------------------------------------------------------
# 2) Stale HTF / market data → explicit STALE
# ---------------------------------------------------------------------------


def test_stale_4h_evidence_explicit_status():
    # Last 4h bar ended ~09:30+4h on prior day; as_of is much later → STALE.
    rows = [
        _candle("2026-09-01T09:30:00-04:00", 90.0, 92.0, 89.0, 91.0),
        _candle("2026-09-01T13:30:00-04:00", 91.0, 93.0, 90.5, 92.5),
    ]
    as_of = "2026-09-09T14:05:00+00:00"
    fresh = assess_htf_candle_freshness(
        rows, data_as_of=as_of, bucket_minutes=240, label="4h"
    )
    assert fresh["status"] == "STALE"
    assert fresh["age_seconds"] is not None and fresh["age_seconds"] > 240 * 60 * 1.5
    assert fresh["bar_count"] == 2


def test_available_1h_when_recent():
    rows = [
        _candle("2026-09-09T09:30:00-04:00", 100.0, 101.0, 99.5, 100.5),
        _candle("2026-09-09T10:30:00-04:00", 100.5, 101.5, 100.0, 101.0),
    ]
    # as_of shortly after 10:30-11:30 bucket would complete at 11:30; use 11:35 ET.
    as_of = "2026-09-09T15:35:00+00:00"  # 11:35 ET
    fresh = assess_htf_candle_freshness(
        rows, data_as_of=as_of, bucket_minutes=60, label="1h"
    )
    assert fresh["status"] == "AVAILABLE"


def test_htf_freshness_wired_into_breach_evidence():
    as_of = "2026-09-09T14:05:00+00:00"
    signal = {
        "side": "CALL",
        "trigger_price": 100.0,
        "stop_price": 98.0,
        "target_price": 106.0,
        "underlying_price": 100.5,
        "trigger_crossed_at": as_of,
        "data_as_of": as_of,
        "breach_count": 1,
        "breach_candle": _candle("2026-09-09T10:00:00-04:00", 99.8, 100.6, 99.7, 100.4),
    }
    stale_4h = [
        _candle("2026-09-01T09:30:00-04:00", 90.0, 92.0, 89.0, 91.0),
        _candle("2026-09-01T13:30:00-04:00", 91.0, 93.0, 90.5, 92.5),
    ]
    evidence = _build_breach_evidence(
        signal,
        data_sources={"candles": {"4h": stale_4h, "1h": [], "15m": [], "5m": []}},
        provenance={},
        observation={"price": 100.5, "observed_at": as_of},
    )
    assert evidence["htf_freshness"]["4h"]["status"] == "STALE"
    assert evidence["htf_freshness"]["1h"]["status"] == "MISSING"
    assert evidence["wick_vs_body_breach"]["confirmation_style"] == "BODY_CONFIRMED"
    assert evidence["market_structure"]["htf_freshness"]["4h"]["status"] == "STALE"


# ---------------------------------------------------------------------------
# 3) Pullback-candidate features (research-only freeze)
# ---------------------------------------------------------------------------


def test_pullback_candidate_features_freeze_honest_missing():
    signal = {
        "side": "CALL",
        "trigger_price": 100.0,
        "stop_price": 98.0,
        "target_price": 106.0,
        "underlying_price": 101.0,
        "breach_lineage": "INITIAL_BREACH",
    }
    evidence = {
        "breach_lineage": "INITIAL_BREACH",
        "remaining_opportunity": {
            "percent_move_consumed": 0.25,
            "remaining_r": 1.8,
        },
        "fifteen_minute_confirmation": {"status": "AVAILABLE", "follow_through": False, "bar_count": 2},
        "five_minute_confirmation": {
            "status": "MISSING",
            "bar_count": 0,
            "missing_reason": "canonical_5min_source_unavailable",
        },
        "volume_imbalance": {"status": "MISSING"},
        "market_structure": {
            "relationship": {
                "relationship": "ALIGNED_RETEST_ZONE_BEHIND",
                "aligned_retest_zone": {"zone_id": "fvg_4h_abc", "low": 98.0, "high": 99.0},
                "opposing_wall": {"zone_id": "fvg_4h_xyz", "low": 104.0, "high": 106.0},
                "runway_to_opposing_wall": 3.0,
            }
        },
        "wick_vs_body_breach": {"confirmation_style": "WICK_ONLY", "status": "AVAILABLE"},
        "htf_freshness": {"4h": {"status": "AVAILABLE"}, "1h": {"status": "STALE"}},
    }
    feats = freeze_pullback_candidate_features(
        signal=signal,
        evidence=evidence,
        wick_vs_body=evidence["wick_vs_body_breach"],
        market_structure=evidence["market_structure"],
        htf_freshness=evidence["htf_freshness"],
    )
    assert feats["schema_version"] == "pullback_candidate_features_v1"
    assert feats["observe_only"] is True
    assert feats["affected_eligibility"] is False
    assert feats["records_future_pullback_outcomes"] is False
    assert feats["extension"] == pytest.approx(1.0)
    assert feats["percent_move_consumed"] == pytest.approx(0.25)
    assert feats["wick_vs_body"]["confirmation_style"] == "WICK_ONLY"
    assert feats["five_minute_state"]["status"] == "MISSING"
    assert feats["vwap_status"] == "MISSING"
    assert feats["volume_status"] == "MISSING"
    assert feats["first_vs_rebreach"] == "FIRST_BREACH"
    assert feats["nearest_aligned_fvg_behind"]["zone_id"] == "fvg_4h_abc"
    assert feats["opposing_ahead"]["zone_id"] == "fvg_4h_xyz"
    assert feats["runway_to_opposing_wall"] == 3.0
    # Must not invent completion / outcome fields.
    assert "pullback_completed" not in feats
    assert "outcome" not in feats
    assert "mfe" not in feats
    assert "pnl" not in feats


def test_pullback_features_on_breach_evidence_wire():
    as_of = "2026-09-09T14:05:00+00:00"
    signal = {
        "side": "PUT",
        "trigger_price": 100.0,
        "stop_price": 102.0,
        "target_price": 94.0,
        "underlying_price": 99.5,
        "trigger_crossed_at": as_of,
        "breach_count": 1,
        "breach_candle": _candle("2026-09-09T10:00:00-04:00", 100.2, 100.3, 99.2, 99.4),
    }
    evidence = _build_breach_evidence(
        signal,
        data_sources={"candles": {"15m": [], "5m": []}},
        provenance={},
        observation={"price": 99.5, "observed_at": as_of},
    )
    feats = evidence["pullback_candidate_features"]
    assert feats["observe_only"] is True
    assert feats["records_future_pullback_outcomes"] is False
    assert feats["first_vs_rebreach"] == "FIRST_BREACH"
    assert feats["vwap_status"] == "MISSING"
    assert feats["volume_status"] == "MISSING"
    assert feats["wick_vs_body"]["confirmation_style"] == "BODY_CONFIRMED"


# ---------------------------------------------------------------------------
# 4) Entry-timing candidate enum alignment (dual-emit)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "legacy,expected_candidate,extra",
    [
        ("READY_NOW", "READY_NOW_CANDIDATE", {}),
        ("REBREACH_PREFERRED", "REBREACH_PREFERRED", {}),
        ("INVALID", "SETUP_INVALID", {}),
        (
            "WAIT_CONFIRMATION",
            "WAIT_OPPOSING_FVG_ACCEPTANCE_CANDIDATE",
            {
                "market_structure": {
                    "relationship": {"relationship": "APPROACHING_OPPOSING_FVG"}
                }
            },
        ),
        (
            "WAIT_CONFIRMATION",
            "WAIT_FVG_RETEST_CANDIDATE",
            {
                "market_structure": {
                    "relationship": {"relationship": "ALIGNED_RETEST_ZONE_BEHIND"}
                }
            },
        ),
        (
            "WAIT_CONFIRMATION",
            "WAIT_PULLBACK_CANDIDATE",
            {
                "wick_vs_body_breach": {"confirmation_style": "WICK_ONLY"},
            },
        ),
    ],
)
def test_entry_timing_candidate_mapping(legacy, expected_candidate, extra):
    candidate = map_entry_timing_candidate(legacy, evidence=extra)
    assert candidate == expected_candidate
    assert candidate in ENTRY_TIMING_CANDIDATE_ENUMS


def test_dual_emit_does_not_affect_eligibility():
    signal = {
        "side": "CALL",
        "trigger_price": 100.0,
        "stop_price": 98.0,
        "target_price": 106.0,
        "underlying_price": 100.5,
    }
    evidence = {
        "breach_lineage": "INITIAL_BREACH",
        "remaining_opportunity": {"remaining_r": 2.0, "percent_move_consumed": 0.1},
        "fifteen_minute_confirmation": {"status": "AVAILABLE", "follow_through": True},
        "five_minute_confirmation": {"status": "AVAILABLE", "follow_through": True},
        "wick_vs_body_breach": {"confirmation_style": "BODY_CONFIRMED"},
    }
    result = classify_entry_readiness_observe_only(signal, evidence=evidence)
    assert result["classification"] == "READY_NOW"
    assert result["entry_timing_candidate"] == "READY_NOW_CANDIDATE"
    assert result["observe_only"] is True
    assert result["affected_eligibility"] is False
