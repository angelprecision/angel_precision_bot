"""P0 #435 observe-only market_structure freeze regressions."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone

from ap.intelligence_breach_market_structure import (
    SESSION_POLICY_4H_ATTESTATION,
    attach_market_structure_to_breach_evidence,
    deterministic_zone_id,
    freeze_breach_market_structure,
    freeze_volume_imbalance,
)


def _candle(ts: str, o: float, h: float, l: float, c: float) -> dict:
    return {"time": ts, "open": o, "high": h, "low": l, "close": c, "volume": 1000}


def _bullish_fvg_triplet_base(day: str, *, low_gap: float, high_gap: float) -> list[dict]:
    """Three candles forming a bullish FVG (c1.high < c3.low) at fixed geometry."""
    # c1 high = low_gap, c3 low = high_gap  => gap [low_gap, high_gap]
    return [
        _candle(f"{day}T09:30:00-04:00", low_gap - 1.0, low_gap, low_gap - 2.0, low_gap - 0.5),
        _candle(f"{day}T10:30:00-04:00", low_gap - 0.5, high_gap + 0.5, low_gap - 0.5, high_gap),
        _candle(f"{day}T11:30:00-04:00", high_gap, high_gap + 1.0, high_gap, high_gap + 0.5),
    ]


def _bearish_fvg_triplet(day: str, *, low_gap: float, high_gap: float) -> list[dict]:
    """Bearish FVG: c1.low > c3.high => gap [c3.high, c1.low] = [low_gap, high_gap]."""
    return [
        _candle(f"{day}T09:30:00-04:00", high_gap + 0.5, high_gap + 1.5, high_gap, high_gap + 0.2),
        _candle(f"{day}T10:30:00-04:00", high_gap, high_gap, low_gap, low_gap + 0.2),
        _candle(f"{day}T11:30:00-04:00", low_gap, low_gap + 0.3, low_gap - 1.0, low_gap - 0.2),
    ]


AS_OF = "2026-09-09T14:05:00+00:00"  # after morning buckets complete


def _base_signal(**overrides):
    sig = {
        "side": "CALL",
        "ticker": "TEST",
        "trigger_price": 100.0,
        "stop_price": 98.0,
        "target_price": 106.0,
        "underlying_price": 100.5,
        "trigger_crossed_at": AS_OF,
        "data_as_of": AS_OF,
    }
    sig.update(overrides)
    return sig


def test_identical_frozen_input_yields_identical_zone_ids():
    candles_4h = _bullish_fvg_triplet_base("2026-09-08", low_gap=95.0, high_gap=97.0)
    candles_4h += _bearish_fvg_triplet("2026-09-09", low_gap=104.0, high_gap=106.0)
    # Shift second triplet times so they are distinct completed 4h-ish hours on as_of day
    # Re-build clean fixed fixture:
    rows = [
        _candle("2026-09-08T09:30:00-04:00", 94.0, 95.0, 93.0, 94.5),  # c1
        _candle("2026-09-08T13:30:00-04:00", 95.0, 97.5, 95.0, 97.0),  # c2
        _candle("2026-09-09T09:30:00-04:00", 97.0, 98.0, 97.0, 97.5),  # c3 bullish FVG 95-97
        _candle("2026-09-09T09:30:00-04:00", 105.5, 106.5, 105.0, 105.2),  # will rebuild properly
    ]
    # Deterministic 4h series with both aligned bullish below and opposing bearish above.
    rows_4h = [
        _candle("2026-09-05T09:30:00-04:00", 90.0, 92.0, 89.0, 91.0),
        _candle("2026-09-05T13:30:00-04:00", 91.0, 93.0, 90.5, 92.5),
        _candle("2026-09-08T09:30:00-04:00", 94.0, 95.0, 93.5, 94.8),  # c1 bullish
        _candle("2026-09-08T13:30:00-04:00", 95.2, 97.2, 95.0, 97.0),  # c2
        _candle("2026-09-09T09:30:00-04:00", 97.1, 98.0, 97.0, 97.6),  # c3 -> bullish FVG [95,97]
    ]
    # Append bearish opposing wall above price using next three bars still before AS_OF.
    rows_4h += [
        _candle("2026-09-08T09:30:00-04:00", 110.0, 111.0, 109.5, 110.2),  # duplicate day ok for detector indices
    ]
    # Cleaner: separate series built only for geometry — detector scans consecutive indices.
    rows_4h = [
        # bars 0-2: bullish FVG [95, 97]
        _candle("2026-09-04T09:30:00-04:00", 94.0, 95.0, 93.0, 94.5),
        _candle("2026-09-04T13:30:00-04:00", 95.0, 96.5, 94.8, 96.0),
        _candle("2026-09-05T09:30:00-04:00", 97.0, 98.0, 97.0, 97.5),
        # bars 3-5: bearish FVG [104, 106]
        _candle("2026-09-05T13:30:00-04:00", 105.5, 107.0, 105.0, 105.2),
        _candle("2026-09-08T09:30:00-04:00", 105.0, 105.5, 103.5, 104.0),
        _candle("2026-09-08T13:30:00-04:00", 103.8, 104.2, 102.5, 103.0),
        # bar 6: completed before AS_OF, no new gap required
        _candle("2026-09-09T09:30:00-04:00", 100.0, 101.0, 99.5, 100.5),
    ]
    ctx = {"candles": {"4h": rows_4h, "1h": list(rows_4h)}}
    sig = _base_signal()
    a = freeze_breach_market_structure(sig, market_context=ctx, data_as_of=AS_OF)
    b = freeze_breach_market_structure(deepcopy(sig), market_context=deepcopy(ctx), data_as_of=AS_OF)
    assert a["observe_only"] is True
    assert a["session_policy_4h"]["policy_id"] == SESSION_POLICY_4H_ATTESTATION["policy_id"]
    ids_a = [z["zone_id"] for z in a["zones"]]
    ids_b = [z["zone_id"] for z in b["zones"]]
    assert ids_a == ids_b
    assert len(ids_a) >= 1
    # Direct id helper stability
    z0 = a["zones"][0]
    assert z0["zone_id"] == deterministic_zone_id(
        timeframe=z0["timeframe"],
        direction=z0["direction"],
        low=z0["low"],
        high=z0["high"],
        source_candle_start=z0["source_candle_start"],
        source_candle_end=z0["source_candle_end"],
    )


def test_opposing_wall_ahead_for_call():
    rows_4h = [
        _candle("2026-09-04T09:30:00-04:00", 105.5, 107.0, 105.0, 105.2),  # c1 bearish
        _candle("2026-09-04T13:30:00-04:00", 105.0, 105.5, 103.5, 104.0),
        _candle("2026-09-05T09:30:00-04:00", 103.8, 104.2, 102.5, 103.0),  # bearish FVG [102.5? wait]
        # c1.low=105.0, c3.high=104.2 → need c1.low > c3.high: 105 > 103.5 style
    ]
    # Explicit bearish: c1_low=106, c3_high=104 → gap [104, 106]
    rows_4h = [
        _candle("2026-09-04T09:30:00-04:00", 106.5, 107.5, 106.0, 106.2),
        _candle("2026-09-04T13:30:00-04:00", 106.0, 106.2, 104.5, 105.0),
        _candle("2026-09-05T09:30:00-04:00", 104.2, 104.8, 103.0, 103.5),  # gap [104.8? c3_high=104.8, c1_low=106] = [104.8, 106]
        _candle("2026-09-09T09:30:00-04:00", 100.0, 101.0, 99.8, 100.4),
    ]
    ctx = {"candles": {"4h": rows_4h, "1h": []}}
    freeze = freeze_breach_market_structure(
        _base_signal(underlying_price=100.5, target_price=108.0),
        market_context=ctx,
        data_as_of=AS_OF,
    )
    rel = freeze["relationship"]["relationship"]
    assert rel in {
        "APPROACHING_OPPOSING_FVG",
        "OPPOSING_WALL_REJECTED",
        "OPPOSING_WALL_ACCEPTED",
        "AT_OPPOSING_FRONT",
    }
    assert freeze["relationship"]["opposing_wall"] is not None
    assert freeze["relationship"]["runway_to_opposing_wall"] is not None
    assert freeze["relationship"]["runway_to_opposing_wall"] > 0
    assert freeze["relationship"]["target_relation"] in {
        "BEFORE_WALL", "AT_WALL", "INSIDE_WALL", "BEYOND_WALL"
    }


def test_aligned_retest_zone_behind_for_call():
    # Bullish FVG entirely below price → ALIGNED_RETEST_ZONE_BEHIND when path clear.
    rows_4h = [
        _candle("2026-09-04T09:30:00-04:00", 94.0, 95.0, 93.0, 94.5),
        _candle("2026-09-04T13:30:00-04:00", 95.0, 96.5, 94.8, 96.0),
        _candle("2026-09-05T09:30:00-04:00", 97.0, 98.0, 97.0, 97.5),  # bullish [95,97]
        _candle("2026-09-09T09:30:00-04:00", 100.0, 101.0, 99.8, 100.5),
    ]
    ctx = {"candles": {"4h": rows_4h, "1h": []}}
    freeze = freeze_breach_market_structure(
        _base_signal(underlying_price=100.5, target_price=103.0),
        market_context=ctx,
        data_as_of=AS_OF,
    )
    assert freeze["relationship"]["relationship"] == "ALIGNED_RETEST_ZONE_BEHIND"
    assert freeze["relationship"]["aligned_retest_zone"] is not None
    assert freeze["relationship"]["aligned_retest_zone"]["alignment"] == "aligned"


def test_absent_vi_is_missing_not_fabricated():
    freeze = freeze_breach_market_structure(
        _base_signal(),
        market_context={"candles": {"4h": [], "1h": []}},
        data_as_of=AS_OF,
        volume_imbalance=None,
    )
    assert freeze["volume_imbalance"]["status"] == "MISSING"
    assert freeze["volume_imbalance"]["approximation_allowed"] is False
    assert "imbalance" not in freeze["volume_imbalance"] or freeze["volume_imbalance"].get("imbalance") in (None, "MISSING")

    exact = freeze_volume_imbalance(
        {"status": "AVAILABLE", "imbalance": 0.42, "source": "pit_fixture", "as_of": AS_OF}
    )
    assert exact["status"] == "AVAILABLE"
    assert exact["imbalance"] == 0.42


def test_no_future_candle_influence_on_zones():
    # Past completed bars create a bullish FVG; a future bar would invalidate/fill it.
    past = [
        _candle("2026-09-04T09:30:00-04:00", 94.0, 95.0, 93.0, 94.5),
        _candle("2026-09-04T13:30:00-04:00", 95.0, 96.5, 94.8, 96.0),
        _candle("2026-09-05T09:30:00-04:00", 97.0, 98.0, 97.0, 97.5),  # bullish [95,97]
    ]
    future_invalidator = _candle("2026-09-09T13:30:00-04:00", 94.0, 94.5, 90.0, 91.0)  # close below low
    # AS_OF is 14:05 UTC = 10:05 ET on Sep 9 — the 09:30 ET 4h bucket ends 13:30 ET,
    # so 13:30 bar is NOT completed at 10:05 ET and must be excluded.
    as_of_morning = "2026-09-09T14:05:00+00:00"  # 10:05 ET
    ctx_with_future = {"candles": {"4h": past + [future_invalidator], "1h": []}}
    ctx_past_only = {"candles": {"4h": past, "1h": []}}
    a = freeze_breach_market_structure(_base_signal(), market_context=ctx_with_future, data_as_of=as_of_morning)
    b = freeze_breach_market_structure(_base_signal(), market_context=ctx_past_only, data_as_of=as_of_morning)
    assert [z["zone_id"] for z in a["zones"]] == [z["zone_id"] for z in b["zones"]]
    assert [z["lifecycle_status"] for z in a["zones"]] == [z["lifecycle_status"] for z in b["zones"]]
    # Ensure at least one unfilled/partial aligned zone survived (future break ignored)
    assert any(z["lifecycle_status"] != "broken_reclaimed" for z in a["zones"])


def test_wire_into_breach_evidence_stable_key():
    evidence = {"breach_lineage": "INITIAL_BREACH"}
    ms = freeze_breach_market_structure(
        _base_signal(),
        market_context={"candles": {"4h": [], "1h": []}},
        data_as_of=AS_OF,
    )
    out = attach_market_structure_to_breach_evidence(evidence, ms)
    assert out["market_structure"]["observe_only"] is True
    assert out["market_structure"]["schema_version"] == "market_structure_v1"
    assert out["volume_imbalance"]["status"] == "MISSING"
