"""#435 canonical FVG / market-structure freeze regressions."""

from __future__ import annotations

from ap.intelligence_breach_market_structure import (
    classify_breach_fvg_relationship,
    deterministic_zone_id,
    freeze_breach_market_structure,
    freeze_fvg_zone,
)
from ap.fair_value_gap import FairValueGap, detect_fair_value_gaps


def _bars():
    # Three-candle bullish FVG then later candles; fixed fixtures only.
    return [
        {"time": "2026-09-08T13:30:00+00:00", "open": 100, "high": 101, "low": 99, "close": 100.5, "volume": 1},
        {"time": "2026-09-08T14:00:00+00:00", "open": 100.5, "high": 102, "low": 100.4, "close": 101.5, "volume": 1},
        {"time": "2026-09-08T14:30:00+00:00", "open": 103, "high": 104, "low": 102.5, "close": 103.5, "volume": 1},
        {"time": "2026-09-08T15:00:00+00:00", "open": 103.5, "high": 103.8, "low": 103.0, "close": 103.2, "volume": 1},
    ]


def test_identical_frozen_input_identical_zone_ids():
    rows = _bars()
    gaps = detect_fair_value_gaps(rows, timeframe="4h")
    assert gaps, "fixture must produce at least one FVG"
    a = freeze_fvg_zone(gaps[0], candles=rows, side="CALL", data_as_of="2026-09-08T15:10:00+00:00")
    b = freeze_fvg_zone(gaps[0], candles=rows, side="CALL", data_as_of="2026-09-08T15:10:00+00:00")
    assert a["zone_id"] == b["zone_id"]
    assert a["zone_id"] == deterministic_zone_id(
        timeframe="4h",
        direction=gaps[0].direction,
        low=gaps[0].low,
        high=gaps[0].high,
        source_start=a["source_candle_start"],
        source_end=a["source_candle_end"],
    )


def test_opposing_wall_and_aligned_retest_relationship():
    zones = [
        {
            "zone_id": "aligned",
            "low": 95.0,
            "midpoint": 96.0,
            "high": 97.0,
            "aligned_for_side": True,
            "opposing_for_side": False,
            "lifecycle_status": "unfilled",
        },
        {
            "zone_id": "opposing",
            "low": 101.0,
            "midpoint": 102.0,
            "high": 103.0,
            "aligned_for_side": False,
            "opposing_for_side": True,
            "lifecycle_status": "unfilled",
        },
    ]
    rel = classify_breach_fvg_relationship(
        side="CALL",
        price=100.0,
        trigger=99.0,
        target=104.0,
        stop=97.0,
        zones=zones,
        fvg_diagnostics={"opposing_4h_fvg_in_path": True, "confirmation_present": False},
    )
    assert rel["relationship"] in {"APPROACHING_OPPOSING_FVG", "ALIGNED_RETEST_ZONE_BEHIND"}
    assert rel["nearest_opposing_zone_id"] == "opposing"
    assert "aligned" in rel["aligned_retest_zone_ids"]
    assert rel["target_relation"] == "BEYOND_WALL"


def test_absent_vi_stays_missing():
    frozen = freeze_breach_market_structure(
        {
            "side": "PUT",
            "trigger_price": 50,
            "stop_price": 51,
            "target_price": 48,
            "underlying_price": 49.5,
        },
        candles_by_tf={"4h": _bars(), "1h": _bars()},
        fvg_context={"diagnostics": {}},
        data_as_of="2026-09-08T15:10:00+00:00",
    )
    assert frozen["observe_only"] is True
    assert frozen["affected_eligibility"] is False
    assert frozen["volatility_imbalance"]["status"] == "MISSING"
    assert frozen["zones"]
