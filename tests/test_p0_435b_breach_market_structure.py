from __future__ import annotations

from copy import deepcopy

from ap.intelligence_breach_market_structure import (
    completed_bars_as_of,
    freeze_breach_market_structure,
    freeze_volume_imbalance,
    fvg_position,
    measure_boundary_penetration,
)

AS_OF = "2026-09-11T14:20:00+00:00"  # 10:20 ET


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
        "trigger_price": 130.50,
        "trigger_crossed_at": AS_OF,
        "breach_lineage": "INITIAL_BREACH",
    }
    value.update(updates)
    return value


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
        _signal(),
        market_context={
            "candles": {
                "4h": _bullish_fvg_4h(),
                "1h": [],
                "15m": bars_15m,
            }
        },
    )
    state = frozen["pullback_reclaim_rebreach"]
    assert state["source_timeframe"] == "15m"
    assert state["returned_to_pretrigger_side"] is True
    assert state["rebreach_after_pullback"] is True
    assert state["pullback_state"] == "RECLAIMING"


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
