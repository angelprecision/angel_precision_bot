from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from ap import intelligence_market_data as market_data

from ap.intelligence_breach_market_structure import (
    completed_bars_as_of,
    freeze_breach_market_structure,
    freeze_breach_market_structure_from_pit,
    freeze_penetration,
    freeze_pullback_reclaim_rebreach,
    freeze_volume_imbalance,
    fvg_position,
    measure_boundary_penetration,
)

AS_OF = "2026-09-11T14:20:00+00:00"  # 10:20 ET
PIT_AS_OF = "2026-09-11T17:30:00+00:00"  # 13:30 ET
ET = ZoneInfo("America/New_York")


def _bar(ts: str, o: float, h: float, l: float, c: float) -> dict:
    return {
        "time": ts,
        "open": o,
        "high": h,
        "low": l,
        "close": c,
        "volume": 1000,
    }


def _bullish_fvg_4h() -> list[dict]:
    # c1.high 130.50 < c3.low 130.70 -> bullish FVG [130.50, 130.70].
    return [
        _bar("2026-09-09T09:30:00-04:00", 129.5, 130.50, 129.0, 130.1),
        _bar("2026-09-09T13:30:00-04:00", 130.2, 131.3, 130.0, 131.0),
        _bar("2026-09-10T09:30:00-04:00", 130.8, 132.0, 130.70, 131.5),
    ]


def _signal(**updates) -> dict:
    value = {
        "ticker": "NOW",
        "side": "PUT",
        "underlying_price": 130.50,
        "breach_price": 130.50,
        "trigger_price": 130.50,
        "trigger_crossed_at": AS_OF,
        "breach_lineage": "INITIAL_BREACH",
    }
    value.update(updates)
    return value


def _session_15m(day: str, base: float) -> list[dict]:
    start = datetime.fromisoformat(f"{day}T09:30:00").replace(tzinfo=ET)
    rows = []
    for index in range(26):
        timestamp = start + timedelta(minutes=15 * index)
        open_price, high, low, close = base, base + 0.5, base - 0.5, base
        if day == "2026-09-11" and index == 15:
            open_price, high, low, close = 110.5, 111.0, 100.3, 110.0
        rows.append(_bar(timestamp.isoformat(), open_price, high, low, close))
    return rows


def _pit_five_wick() -> dict:
    return _bar("2026-09-11T17:25:00+00:00", 110.5, 111.0, 100.3, 110.0)


def _pit_five_strong() -> dict:
    return _bar("2026-09-11T17:25:00+00:00", 100.7, 100.8, 100.2, 100.3)


def _pit_signal(*, five: dict | None = None, **updates) -> dict:
    fifteen = (
        _session_15m("2026-09-08", 100.0)
        + _session_15m("2026-09-09", 108.0)
        + _session_15m("2026-09-10", 116.0)
        + _session_15m("2026-09-11", 110.0)
    )
    signal = _signal(
        trigger_crossed_at=PIT_AS_OF,
        trigger_price=100.50,
        breach_price=100.50,
        underlying_price=999.0,
        current_price=998.0,
        candles_5m=[five or _pit_five_wick()],
        candles_15m=fifteen,
    )
    signal.update(updates)
    return signal


def _pit_snapshot(signal: dict | None = None) -> dict:
    return market_data.collect_point_in_time_context(
        signal or _pit_signal(), broker=None, phase="BREACH"
    )


def test_fvg_position_has_explicit_inside_above_below_boundary_states():
    assert fvg_position(130.60, 130.50, 130.70)["position"] == "inside"
    assert fvg_position(131.00, 130.50, 130.70)["position"] == "above"
    assert fvg_position(130.20, 130.50, 130.70)["position"] == "below"
    boundary = fvg_position(130.505, 130.50, 130.70, tolerance=0.01)
    assert boundary["position"] == "boundary"
    assert boundary["boundary"] == "low"


def test_missing_as_of_cannot_admit_future_candles():
    rows = [_bar("2026-09-11T09:30:00-04:00", 1, 2, 1, 2)]
    assert completed_bars_as_of(rows, as_of=None, minutes=15) == []


def test_naive_premarket_postmarket_and_misaligned_bars_are_excluded():
    rows = [
        _bar("2026-09-11T09:30:00", 1, 2, 1, 2),
        _bar("2026-09-11T09:30:00+00:00", 1, 2, 1, 2),
        _bar("2026-09-11T09:25:00-04:00", 1, 2, 1, 2),
        _bar("2026-09-11T09:31:00-04:00", 1, 2, 1, 2),
        _bar("2026-09-11T16:05:00-04:00", 1, 2, 1, 2),
    ]
    assert completed_bars_as_of(
        rows, as_of="2026-09-11T16:30:00-04:00", minutes=15
    ) == []


def test_future_incomplete_4h_candle_cannot_invalidate_frozen_gap():
    future_break = _bar(
        "2026-09-11T09:30:00-04:00",
        130.6,
        130.8,
        128.0,
        128.5,
    )
    # At 10:20 ET, the 09:30 4h bar is not complete until 13:30 ET.
    ctx = {"candles": {"4h": _bullish_fvg_4h() + [future_break], "1h": []}}
    frozen = freeze_breach_market_structure(_signal(), market_context=ctx)
    zones = [z for z in frozen["fvg_zones"] if z["direction"] == "bullish"]
    assert zones
    assert zones[-1]["low"] == 130.50
    assert zones[-1]["high"] == 130.70
    assert zones[-1]["lifecycle_status"] != "broken"


def test_now_shape_put_at_bullish_fvg_bottom_freezes_boundary_and_no_weak_break():
    # A 15m wick under the FVG low that closes back above it is not a body break.
    bars_15m = [
        _bar("2026-09-11T09:30:00-04:00", 130.75, 130.80, 130.48, 130.62),
    ]
    ctx = {
        "candles": {
            "4h": _bullish_fvg_4h(),
            "1h": [],
            "15m": bars_15m,
        }
    }
    frozen = freeze_breach_market_structure(_signal(), market_context=ctx)
    zone = frozen["relevant_opposing_fvg"]
    assert zone is not None
    assert zone["direction"] == "bullish"
    assert zone["alignment"] == "opposing"
    assert zone["price_position"]["position"] == "boundary"
    assert zone["break_boundary"] == 130.50

    penetration = frozen["fvg_penetration"]["15m"]
    assert penetration["style"] == "WICK_ONLY"
    assert penetration["wick_beyond"] is True
    assert penetration["close_beyond"] is False
    assert penetration["fifty_percent_body_through"] is False
    assert frozen["fvg_penetration"]["strong_break_on_5m_or_15m"] is False
    assert frozen["setup"]["entry_posture_observe_only"] == "WAIT_RECLAIM_CANDIDATE"
    assert frozen["affected_eligibility"] is False


def test_breach_price_uses_only_explicit_frozen_evidence():
    ctx = {"candles": {"4h": _bullish_fvg_4h(), "1h": []}}
    frozen = freeze_breach_market_structure(
        _signal(underlying_price=999.0, breach_price=130.50),
        market_context=ctx,
    )
    assert frozen["underlying_price"] == 130.50
    assert frozen["underlying_price_source"] == "signal.breach_price"

    no_frozen_price = freeze_breach_market_structure(
        _signal(underlying_price=130.50, breach_price=None),
        market_context=ctx,
    )
    assert no_frozen_price["underlying_price"] is None
    assert no_frozen_price["relevant_opposing_fvg"] is None


def test_future_or_approximate_breach_evidence_is_missing():
    ctx = {"candles": {"4h": _bullish_fvg_4h(), "1h": []}}
    future = freeze_breach_market_structure(
        _signal(
            breach_price={
                "price": 130.50,
                "as_of": "2026-09-11T14:21:00+00:00",
            }
        ),
        market_context=ctx,
    )
    assert future["underlying_price"] is None


def test_put_strong_15m_body_break_records_50pct_strength_without_gating():
    candle = _bar(
        "2026-09-11T09:45:00-04:00",
        130.60,
        130.65,
        130.25,
        130.30,
    )
    measured = measure_boundary_penetration(
        side="PUT",
        boundary=130.50,
        candle=candle,
        timeframe="15m",
    )
    assert measured["style"] == "STRONG_BODY_50_PLUS"
    assert measured["close_beyond"] is True
    assert measured["body_beyond_boundary_ratio"] >= 0.5
    assert measured["body_to_range_ratio"] >= 0.5
    assert measured["fifty_percent_body_through"] is True
    assert measured["strong_break_observed"] is True


def test_call_side_is_symmetric_for_bearish_fvg_resistance():
    candle = _bar(
        "2026-09-11T09:45:00-04:00",
        134.40,
        134.85,
        134.35,
        134.75,
    )
    measured = measure_boundary_penetration(
        side="CALL",
        boundary=134.50,
        candle=candle,
        timeframe="15m",
    )
    assert measured["close_beyond"] is True
    assert measured["body_beyond_boundary_ratio"] >= 0.5
    assert measured["strong_break_observed"] is True


def test_pullback_reclaim_rebreach_is_completed_close_sequence_not_tick_guess():
    bars_15m = [
        _bar("2026-09-11T09:30:00-04:00", 130.7, 130.8, 130.3, 130.4),
        _bar("2026-09-11T09:45:00-04:00", 130.4, 130.7, 130.3, 130.6),
        _bar("2026-09-11T10:00:00-04:00", 130.6, 130.65, 130.2, 130.3),
    ]
    frozen = freeze_breach_market_structure(
        _signal(trigger_crossed_at="2026-09-11T13:30:00+00:00"),
        market_context={
            "candles": {
                "4h": _bullish_fvg_4h(),
                "1h": [],
                "15m": bars_15m,
            }
        },
        data_as_of=AS_OF,
    )
    state = frozen["pullback_reclaim_rebreach"]
    assert state["source_timeframe"] == "15m"
    assert state["returned_to_pretrigger_side"] is True
    assert state["rebreach_after_pullback"] is True
    assert state["pullback_state"] == "RECLAIMING"


def test_pullback_sequence_ignores_candles_before_supplied_breach_time():
    bars_15m = [
        _bar("2026-09-11T09:30:00-04:00", 130.7, 130.8, 130.3, 130.4),
        _bar("2026-09-11T09:45:00-04:00", 130.4, 130.7, 130.3, 130.6),
        _bar("2026-09-11T10:00:00-04:00", 130.6, 130.65, 130.2, 130.3),
    ]
    frozen = freeze_breach_market_structure(
        _signal(),
        market_context={"candles": {"4h": [], "1h": [], "15m": bars_15m}},
        data_as_of=AS_OF,
    )
    state = frozen["pullback_reclaim_rebreach"]
    assert state["status"] == "MISSING"
    assert state["pullback_state"] == "UNKNOWN"


def test_penetration_ignores_interaction_before_fvg_formation():
    zone = {
        "zone_id": "fvg_4h_test",
        "direction": "bullish",
        "alignment": "opposing",
        "low": 130.50,
        "high": 130.70,
        "source_candle_end": "2026-09-11T14:00:00+00:00",
    }
    candle_before_zone = _bar(
        "2026-09-11T13:30:00+00:00", 130.60, 130.65, 130.25, 130.30
    )
    frozen = freeze_penetration(
        side="PUT",
        zone=zone,
        candles={"15m": [candle_before_zone]},
        as_of="2026-09-11T14:30:00+00:00",
    )
    assert frozen["15m"]["status"] == "MISSING"
    assert frozen["15m"]["strong_break_observed"] is False


def test_vi_is_exact_or_missing_never_approximated():
    missing = freeze_volume_imbalance(None)
    assert missing["status"] == "MISSING"
    assert missing["approximation_allowed"] is False

    exact = freeze_volume_imbalance(
        {
            "status": "AVAILABLE",
            "imbalance": 0.42,
            "source": "pit_fixture",
            "as_of": AS_OF,
        }
    )
    assert exact["status"] == "AVAILABLE"
    assert exact["imbalance"] == 0.42
    assert exact["approximation_allowed"] is False
    assert exact["exact"] is True
    assert exact["pit_timestamp"] == "2026-09-11T14:20:00+00:00"


def test_vi_requires_source_timestamp_and_non_approximate_flags():
    assert freeze_volume_imbalance(
        {"status": "AVAILABLE", "source": "pit", "imbalance": 0.1},
        as_of=AS_OF,
        require_as_of=True,
    )["status"] == "MISSING"
    assert freeze_volume_imbalance(
        {
            "status": "AVAILABLE",
            "source": "pit",
            "imbalance": 0.1,
            "as_of": "2026-09-11T14:21:00+00:00",
        },
        as_of=AS_OF,
        require_as_of=True,
    )["status"] == "MISSING"
    assert freeze_volume_imbalance(
        {
            "status": "AVAILABLE",
            "source": "pit",
            "imbalance": 0.1,
            "as_of": AS_OF,
            "approximation_allowed": "false",
        },
        as_of=AS_OF,
        require_as_of=True,
    )["status"] == "MISSING"


def test_missing_price_does_not_select_an_arbitrary_opposing_zone():
    frozen = freeze_breach_market_structure(
        _signal(underlying_price=None, breach_price=None),
        market_context={"candles": {"4h": _bullish_fvg_4h(), "1h": []}},
    )
    assert frozen["fvg_zones"]
    assert all(
        zone["price_position"]["status"] == "MISSING"
        for zone in frozen["fvg_zones"]
    )
    assert frozen["relevant_opposing_fvg"] is None


def test_regime_is_preserved_only_when_explicit_upstream_truth_exists():
    unknown = freeze_breach_market_structure(
        _signal(),
        market_context={"candles": {"4h": [], "1h": [], "15m": []}},
    )
    assert unknown["regime"]["regime"] == "UNKNOWN"
    assert unknown["regime"]["status"] == "MISSING"

    explicit = freeze_breach_market_structure(
        _signal(regime="RANGE"),
        market_context={"candles": {"4h": [], "1h": [], "15m": []}},
    )
    assert explicit["regime"] == {
        "status": "AVAILABLE",
        "regime": "RANGE",
        "source": "signal.regime",
    }


def test_malformed_nonfinite_ohlc_cannot_improve_break_classification():
    measured = measure_boundary_penetration(
        side="PUT",
        boundary=130.50,
        candle={
            "time": "2026-09-11T09:45:00-04:00",
            "open": 130.6,
            "high": 130.7,
            "low": float("nan"),
            "close": 130.2,
        },
        timeframe="15m",
    )
    assert measured["status"] == "MISSING"
    assert measured["strong_break_observed"] is False


def test_freezer_does_not_mutate_signal_or_market_context_and_has_no_money_authority():
    signal = _signal(regime_context={"regime": "RANGE"})
    context = {
        "candles": {
            "4h": _bullish_fvg_4h(),
            "1h": [],
            "15m": [_bar("2026-09-11T09:30:00-04:00", 130.7, 130.8, 130.48, 130.62)],
        }
    }
    before_signal, before_context = deepcopy(signal), deepcopy(context)
    frozen = freeze_breach_market_structure(signal, market_context=context)
    assert signal == before_signal
    assert context == before_context
    assert frozen["observe_only"] is True
    assert frozen["affected_eligibility"] is False
    assert all(value is False for value in frozen["invariants"].values())


def test_pit_adapter_consumes_the_real_614_breach_envelope_without_reshape():
    signal = _pit_signal()
    snapshot = _pit_snapshot(signal)
    before_signal, before_snapshot = deepcopy(signal), deepcopy(snapshot)

    frozen = freeze_breach_market_structure_from_pit(signal, snapshot)

    assert snapshot["phase"] == "BREACH"
    assert snapshot["as_of"] == PIT_AS_OF
    assert frozen["data_as_of"] == PIT_AS_OF
    assert frozen["underlying_price"] == 100.50
    assert frozen["underlying_price_source"] == (
        "point_in_time.underlying_observation"
    )
    assert frozen["data_coverage"] == snapshot["data_sources"]["coverage"]
    assert frozen["data_provenance"] == snapshot["provenance"]
    assert frozen["market_data"]["candles"]["5m"] == snapshot["data_sources"]["candles"]["5m"]
    assert signal == before_signal
    assert snapshot == before_snapshot


def test_exact_pit_5m_reaches_the_penetration_classifier():
    signal = _pit_signal()
    frozen = freeze_breach_market_structure_from_pit(signal, _pit_snapshot(signal))
    penetration = frozen["fvg_penetration"]["5m"]

    assert penetration["status"] == "AVAILABLE"
    assert penetration["candle_time"] == "2026-09-11T17:25:00+00:00"
    assert penetration["style"] == "WICK_ONLY"
    assert penetration["source_provenance"] == "signal_frozen_5min"
    assert penetration["source_coverage"]["source"] == "signal_frozen"


def test_exact_pit_15m_reaches_the_penetration_classifier():
    signal = _pit_signal()
    frozen = freeze_breach_market_structure_from_pit(signal, _pit_snapshot(signal))
    penetration = frozen["fvg_penetration"]["15m"]

    assert penetration["status"] == "AVAILABLE"
    assert penetration["candle_time"] == "2026-09-11T13:15:00-04:00"
    assert penetration["style"] == "WICK_ONLY"
    assert penetration["source_provenance"] == "signal_frozen_15min"
    assert penetration["source_coverage"]["source"] == "signal_frozen"


def test_completed_614_1h_and_4h_candles_reach_canonical_fvg_detection():
    signal = _pit_signal()
    snapshot = _pit_snapshot(signal)
    frozen = freeze_breach_market_structure_from_pit(signal, snapshot)
    timeframes = {zone["timeframe"] for zone in frozen["fvg_zones"]}

    assert len(snapshot["data_sources"]["candles"]["1h"]) == 25
    assert len(snapshot["data_sources"]["candles"]["4h"]) == 7
    assert {"1h", "4h"} <= timeframes
    assert all(zone["zone_id"].startswith("fvg_") for zone in frozen["fvg_zones"])


def test_pit_as_of_controls_future_exclusion_not_collected_at():
    signal = _pit_signal()
    snapshot = _pit_snapshot(signal)
    future = _bar(PIT_AS_OF, 110.5, 111.0, 99.0, 100.0)
    snapshot["collected_at"] = "2099-01-01T00:00:00+00:00"
    snapshot["data_sources"]["candles"]["5m"].append(future)
    snapshot["data_sources"]["candles"]["15m"].append(future)

    frozen = freeze_breach_market_structure_from_pit(signal, snapshot)

    assert frozen["data_as_of"] == PIT_AS_OF
    assert all(
        row["time"] != PIT_AS_OF
        for rows in frozen["market_data"]["candles"].values()
        for row in rows
    )
    assert frozen["point_in_time"]["collected_at"] == "2099-01-01T00:00:00+00:00"


def test_invalid_pit_as_of_cannot_fall_back_to_signal_or_worker_time():
    signal = _pit_signal()
    snapshot = _pit_snapshot(signal)
    snapshot["as_of"] = "not-a-timestamp"
    snapshot["collected_at"] = "2099-01-01T00:00:00+00:00"

    frozen = freeze_breach_market_structure_from_pit(signal, snapshot)

    assert frozen["data_as_of"] is None
    assert frozen["underlying_price"] is None
    assert frozen["fvg_zones"] == []
    assert all(
        not rows for rows in frozen["market_data"]["candles"].values()
    )


def test_frozen_observation_wins_over_contradictory_signal_aliases():
    snapshot = _pit_snapshot()
    contradictory = _pit_signal(
        breach_price=999.0,
        frozen_underlying_price=998.0,
        underlying_at_breach=997.0,
        price_at_breach=996.0,
        breach_evidence={
            "price": 995.0,
            "timestamp": "2026-09-11T17:29:00+00:00",
        },
        underlying_price=994.0,
        current_price=993.0,
    )

    frozen = freeze_breach_market_structure_from_pit(contradictory, snapshot)

    assert frozen["underlying_price"] == 100.50
    assert frozen["underlying_price_source"] == (
        "point_in_time.underlying_observation"
    )


def test_adapter_accepts_only_explicit_exact_volume_imbalance():
    signal = _pit_signal(
        volume_imbalance={
            "status": "AVAILABLE",
            "source": "worker_guess",
            "imbalance": 0.9,
            "as_of": PIT_AS_OF,
        }
    )
    snapshot = _pit_snapshot(signal)

    without_explicit_vi = freeze_breach_market_structure_from_pit(signal, snapshot)
    assert without_explicit_vi["volume_imbalance"]["status"] == "MISSING"

    with_explicit_vi = freeze_breach_market_structure_from_pit(
        signal,
        snapshot,
        volume_imbalance={
            "status": "AVAILABLE",
            "source": "pit_fixture",
            "imbalance": 0.42,
            "as_of": PIT_AS_OF,
        },
    )
    assert with_explicit_vi["volume_imbalance"]["status"] == "AVAILABLE"
    assert with_explicit_vi["volume_imbalance"]["pit_timestamp"] == PIT_AS_OF


def test_stale_coverage_cannot_make_a_strong_break_trustworthy():
    signal = _pit_signal(five=_pit_five_strong())
    snapshot = _pit_snapshot(signal)
    snapshot["data_sources"]["coverage"]["5m"].update(
        status="STALE", authoritative=False
    )

    penetration = freeze_breach_market_structure_from_pit(signal, snapshot)[
        "fvg_penetration"
    ]["5m"]

    assert penetration["measured_strong_break_observed"] is True
    assert penetration["strong_break_observed"] is False
    assert penetration["strong_break_authoritative"] is False
    assert penetration["coverage_authoritative"] is False
    assert penetration["style"].startswith("NON_AUTHORITATIVE_")


def test_provider_failure_without_trustworthy_frozen_evidence_is_not_a_break():
    signal = _pit_signal(five=_pit_five_strong())
    snapshot = _pit_snapshot(signal)
    snapshot["data_sources"]["coverage"]["5m"] = {
        "status": "MISSING",
        "coverage_complete": False,
        "authoritative": False,
        "source": "provider_response",
        "provider_status": "error",
    }

    penetration = freeze_breach_market_structure_from_pit(signal, snapshot)[
        "fvg_penetration"
    ]["5m"]

    assert penetration["measured_strong_break_observed"] is True
    assert penetration["strong_break_observed"] is False
    assert penetration["strong_break_authoritative"] is False


def test_authoritative_frozen_evidence_can_measure_a_strong_break():
    signal = _pit_signal(five=_pit_five_strong())
    snapshot = _pit_snapshot(signal)
    penetration = freeze_breach_market_structure_from_pit(signal, snapshot)[
        "fvg_penetration"
    ]["5m"]

    assert penetration["source_coverage"]["authoritative"] is True
    assert penetration["coverage_authoritative"] is True
    assert penetration["strong_break_observed"] is True
    assert penetration["strong_break_authoritative"] is True


def test_put_strong_5m_body_break_uses_bullish_fvg_low_boundary():
    measured = measure_boundary_penetration(
        side="PUT",
        boundary=130.50,
        candle=_bar("2026-09-11T09:45:00-04:00", 130.70, 130.80, 130.20, 130.30),
        timeframe="5m",
    )

    assert measured["style"] == "STRONG_BODY_50_PLUS"
    assert measured["close_beyond"] is True
    assert measured["fifty_percent_body_through"] is True
    assert measured["strong_break_observed"] is True


def test_pullback_5m_straddling_breach_uses_completion_boundaries():
    rows = [
        _bar("2026-09-11T14:00:00+00:00", 130.60, 130.70, 130.30, 130.40),
        _bar("2026-09-11T14:05:00+00:00", 130.40, 130.70, 130.30, 130.60),
        _bar("2026-09-11T14:10:00+00:00", 130.60, 130.65, 130.20, 130.30),
    ]
    state = freeze_pullback_reclaim_rebreach(
        side="PUT",
        trigger=130.50,
        candles={"5m": rows},
        as_of="2026-09-11T14:16:00+00:00",
        supplied_lineage="INITIAL_BREACH",
        breach_at="2026-09-11T14:02:00+00:00",
    )

    assert state["source_timeframe"] == "5m"
    assert state["pullback_state"] == "RECLAIMING"
    assert state["first_confirmed_breach_ts"] == "2026-09-11T14:05:00+00:00"
    assert state["pullback_ts"] == "2026-09-11T14:10:00+00:00"
    assert state["rebreach_ts"] == "2026-09-11T14:15:00+00:00"


def test_pullback_15m_straddling_breach_has_symmetric_completion_rule():
    rows = [
        _bar("2026-09-11T13:45:00+00:00", 130.60, 130.70, 130.30, 130.40),
        _bar("2026-09-11T14:00:00+00:00", 130.40, 130.70, 130.30, 130.60),
        _bar("2026-09-11T14:15:00+00:00", 130.60, 130.65, 130.20, 130.30),
    ]
    state = freeze_pullback_reclaim_rebreach(
        side="PUT",
        trigger=130.50,
        candles={"15m": rows},
        as_of="2026-09-11T14:31:00+00:00",
        supplied_lineage="INITIAL_BREACH",
        breach_at="2026-09-11T13:55:00+00:00",
    )

    assert state["source_timeframe"] == "15m"
    assert state["pullback_state"] == "RECLAIMING"
    assert state["first_confirmed_breach_ts"] == "2026-09-11T14:00:00+00:00"
    assert state["pullback_ts"] == "2026-09-11T14:15:00+00:00"
    assert state["rebreach_ts"] == "2026-09-11T14:30:00+00:00"


def test_candle_completed_at_breach_is_excluded_from_pullback_sequence():
    state = freeze_pullback_reclaim_rebreach(
        side="PUT",
        trigger=130.50,
        candles={
            "5m": [
                _bar("2026-09-11T13:55:00+00:00", 130.60, 130.70, 130.20, 130.30)
            ]
        },
        as_of="2026-09-11T14:05:00+00:00",
        supplied_lineage="INITIAL_BREACH",
        breach_at="2026-09-11T14:00:00+00:00",
    )

    assert state["status"] == "MISSING"
    assert state["pullback_state"] == "UNKNOWN"
