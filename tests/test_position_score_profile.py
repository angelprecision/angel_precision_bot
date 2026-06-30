from copy import deepcopy

from ap.fair_value_gap import detect_fair_value_gaps, evaluate_fvg_context
from ap.position_score_profile import build_position_score_profile
from ap.the_strat_confluence import classify_strat_bar, evaluate_higher_timeframe_confluence


def test_fvg_detects_bullish_gap_and_flags_entry_inside_zone():
    candles = [
        {"open": 10, "high": 10.50, "low": 9.80, "close": 10.20, "volume": 1000},
        {"open": 10.20, "high": 10.80, "low": 10.00, "close": 10.70, "volume": 1100},
        {"open": 11.10, "high": 11.50, "low": 11.00, "close": 11.40, "volume": 1500},
    ]
    gaps = detect_fair_value_gaps(candles, timeframe="4h")
    assert len(gaps) == 1
    assert gaps[0].direction == "bullish"
    assert gaps[0].low == 10.50
    assert gaps[0].high == 11.00

    result = evaluate_fvg_context(
        {"side": "PUT", "entry_price": 10.75, "target_price": 10.0},
        {"candles": {"4h": candles, "1h": candles}},
    )
    # PUT signal entering inside a BULLISH FVG = opposing gap entry
    assert result["diagnostics"]["entry_inside_opposing_fvg"] is True
    assert result["diagnostics"]["entry_inside_aligned_fvg"] is False
    assert "entry_inside_opposing_fvg" in result["block_recommendations"]


def test_fvg_flags_call_path_through_bearish_4h_wall_without_confirmation():
    candles_4h = [
        {"open": 105.8, "high": 106.2, "low": 104.5, "close": 105.0},
        {"open": 104.8, "high": 105.0, "low": 102.0, "close": 102.5},
        {"open": 102.2, "high": 103.5, "low": 101.4, "close": 101.8},
        {"open": 101.7, "high": 102.3, "low": 100.0, "close": 100.4},
    ]
    result = evaluate_fvg_context(
        {
            "side": "CALL",
            "current_underlying": 100.0,
            "entry_trigger": 101.0,
            "target_underlying": 105.0,
        },
        {"candles": {"4h": candles_4h, "1h": candles_4h}},
    )

    assert result["diagnostics"]["opposing_4h_fvg_in_path"] is True
    assert result["diagnostics"]["path_state"] == "pushing_through_opposing_4h_fvg"
    assert "opposing_4h_fvg_in_path_without_confirmation" in result["block_recommendations"]
    assert result["target_guidance"]["action"] == "cap_before_opposing_fvg_or_block_entry"
    assert result["target_guidance"]["suggested_target"] == 103.5


def test_fvg_exposes_extension_candidate_when_target_stops_before_next_wall():
    candles_4h = [
        {"open": 105.8, "high": 106.2, "low": 104.5, "close": 105.0},
        {"open": 104.8, "high": 105.0, "low": 102.0, "close": 102.5},
        {"open": 102.2, "high": 103.5, "low": 101.4, "close": 101.8},
        {"open": 101.7, "high": 102.3, "low": 100.0, "close": 100.4},
    ]
    result = evaluate_fvg_context(
        {
            "side": "CALL",
            "current_underlying": 100.0,
            "entry_trigger": 101.0,
            "target_underlying": 102.0,
        },
        {"candles": {"4h": candles_4h, "1h": candles_4h}},
    )

    assert result["diagnostics"]["opposing_4h_fvg_in_path"] is False
    assert result["target_guidance"]["action"] == "extension_candidate_to_opposing_fvg_front"
    assert result["target_guidance"]["suggested_target"] == 103.5
    assert result["target_guidance"]["candidate_fvg"]["direction"] == "bearish"


def test_fvg_path_through_bearish_wall_allows_reclaim_confirmation():
    candles_4h = [
        {"open": 105.8, "high": 106.2, "low": 104.5, "close": 105.0},
        {"open": 104.8, "high": 105.0, "low": 102.0, "close": 102.5},
        {"open": 102.2, "high": 103.5, "low": 101.4, "close": 101.8},
        {"open": 103.8, "high": 105.2, "low": 103.7, "close": 104.8},
    ]
    result = evaluate_fvg_context(
        {
            "side": "CALL",
            "current_underlying": 100.0,
            "entry_trigger": 101.0,
            "target_underlying": 105.0,
        },
        {"candles": {"4h": candles_4h, "1h": candles_4h}},
    )

    assert result["diagnostics"]["opposing_4h_fvg_in_path"] is False
    assert "opposing_4h_fvg_in_path_without_confirmation" not in result["block_recommendations"]


def test_strat_classifies_two_up_monthly_confluence_for_call():
    state = classify_strat_bar(
        {"high": 100, "low": 90},
        {"high": 103, "low": 92},
        timeframe="monthly",
        current_price=101,
    )
    assert state.bar_type == "2U"
    assert state.directional_bias == "bullish"
    assert state.aligns_with("CALL") is True

    result = evaluate_higher_timeframe_confluence(
        {"side": "CALL", "current_price": 101},
        {
            "candles": {
                "monthly": [{"high": 100, "low": 90}, {"high": 103, "low": 92}],
                "weekly": [{"high": 99, "low": 91}, {"high": 102, "low": 93}],
                "daily": [{"high": 98, "low": 92}, {"high": 101, "low": 94}],
                "4h": [{"high": 97, "low": 93}, {"high": 100, "low": 95}],
            }
        },
    )
    assert result["score"] >= 14
    assert {"monthly", "weekly", "daily", "4h"}.issubset(set(result["aligned_timeframes"]))


def test_position_profile_observe_only_shape_and_no_signal_mutation():
    signal = {
        "signal_id": "sig-1",
        "ticker": "WFC",
        "side": "CALL",
        "score": 78,
        "entry_price": 50.0,
        "stop_price": 49.0,
        "target_price": 53.0,
        "spread_pct": 0.05,
        "delta": 0.42,
        "open_interest": 1500,
        "option_volume": 400,
        "dte": 3,
        "relative_volume": 1.9,
        "win_rate": 0.72,
        "sample_size": 18,
        "avg_opt_ret": 0.18,
    }
    before = deepcopy(signal)
    context = {
        "trend": {"vwap": 49.8, "ema_stack": "bullish"},
        "levels": {"daily_trigger": 50.05, "weekly_level": 50.10},
        "candles": {
            "monthly": [{"high": 49, "low": 44}, {"high": 51, "low": 46}],
            "weekly": [{"high": 49.5, "low": 47}, {"high": 50.5, "low": 48}],
            "daily": [{"high": 49.7, "low": 48}, {"high": 50.7, "low": 49}],
            "4h": [
                {"open": 48.0, "high": 48.8, "low": 47.7, "close": 48.5, "volume": 1000},
                {"open": 48.6, "high": 49.0, "low": 48.1, "close": 48.9, "volume": 1100},
                {"open": 49.7, "high": 50.3, "low": 49.4, "close": 50.1, "volume": 1900},
            ],
            "1h": [
                {"open": 48.0, "high": 48.8, "low": 47.7, "close": 48.5, "volume": 1000},
                {"open": 48.6, "high": 49.0, "low": 48.1, "close": 48.9, "volume": 1100},
                {"open": 49.7, "high": 50.3, "low": 49.4, "close": 50.1, "volume": 1900},
            ],
        },
    }
    profile = build_position_score_profile(signal, context)
    assert signal == before
    assert profile["profile_version"] == "position_score_profile_v1_observe_only"
    assert profile["diagnostics"]["observe_only"] is True
    assert profile["diagnostics"]["live_behavior_changed"] is False
    assert "fair_value_gap" in profile["components"]
    assert "higher_timeframe_confluence" in profile["components"]
    assert profile["total_score"] > 70
    # Shape assertions: top-level observe_only and warnings must always be present
    assert profile["observe_only"] is True
    assert isinstance(profile["warnings"], list)


def test_position_profile_unknown_side_returns_reject_with_observe_only():
    """UNKNOWN side must fail closed: REJECT grade, block, observe_only=True, warnings list."""
    profile = build_position_score_profile({"side": "UNKNOWN", "entry_price": 50.0})
    assert profile["grade"] == "REJECT"
    assert profile["client_eligible_recommendation"] is False
    assert "unknown_signal_side" in profile["block_recommendations"]
    assert profile["observe_only"] is True
    assert isinstance(profile["warnings"], list)
    assert "unknown_signal_side" in profile["warnings"]
    assert profile["total_score"] == 0.0
