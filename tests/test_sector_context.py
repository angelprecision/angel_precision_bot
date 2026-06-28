from __future__ import annotations

from copy import deepcopy
import json

from ap.position_score_profile import build_position_score_profile
from ap.sector_context import score_sector_context


def test_sector_context_json_safe_no_mutation_and_call_sector_green():
    signal = {"ticker": "WFC", "side": "CALL", "sector": "Financials"}
    before = deepcopy(signal)
    result = score_sector_context(signal, {"sector": {"direction": "green", "change_pct": 1.1}, "market": {"direction": "green", "change_pct": 0.4}})
    assert signal == before
    json.dumps(result)
    assert result["score"] == 5.0
    assert "call_sector_green" in result["boosts"]
    assert result["diagnostics"]["observe_only"] is True


def test_sector_context_put_sector_red_scores():
    result = score_sector_context({"ticker": "WFC", "side": "PUT", "sector": "Financials"}, {"sector": {"direction": "red", "change_pct": -1.1}, "market": {"direction": "red", "change_pct": -0.4}})
    assert result["score"] == 5.0
    assert "put_sector_red" in result["boosts"]
    json.dumps(result)


def test_sector_context_records_market_and_sector_missing_data():
    result = score_sector_context({"side": "PUT"}, {})
    assert result["status"] == "missing_data"
    assert "sector_direction" in result["missing_data"]
    assert "market_direction" in result["missing_data"]
    assert "sector_name" in result["missing_data"]
    json.dumps(result)


def test_sector_context_block_recommendations_are_diagnostics_only():
    result = score_sector_context({"side": "CALL", "sector": "Technology"}, {"sector": {"direction": "red", "change_pct": -1.2}, "market": {"direction": "green", "change_pct": 0.2}})
    assert result["status"] == "block_recommended"
    assert "signal_direction_opposes_sector" in result["block_recommendations"]
    assert result["diagnostics"]["block_recommendations_are_diagnostic_only"] is True


def test_sector_context_unknown_side_does_not_default_to_call():
    result = score_sector_context({"side": "", "sector": "Financials"}, {"sector": {"direction": "green", "change_pct": 1.1}, "market": {"direction": "green", "change_pct": 0.4}})
    assert result["diagnostics"]["side"] == "UNKNOWN"
    assert "side" in result["missing_data"]
    assert "unknown_side" in result["block_recommendations"]
    assert "signal_direction_opposes_sector" not in result["block_recommendations"]
    assert "call_sector_green" not in result["boosts"]
    assert "sector_direction_unscored_unknown_side" in result["warnings"]


def test_position_profile_calls_sector_volume_and_vwap_modules():
    profile = build_position_score_profile(
        {
            "signal_id": "sig-213",
            "ticker": "WFC",
            "side": "CALL",
            "score": 80,
            "entry_price": 50.2,
            "current_price": 50.4,
            "stop_price": 49.0,
            "target_price": 53.0,
            "spread_pct": 0.05,
            "delta": 0.42,
            "open_interest": 1500,
            "option_volume": 400,
            "dte": 3,
            "relative_volume": 1.9,
            "breakout_volume_ratio": 1.7,
            "volume_direction": "bullish",
            "sector": "Financials",
            "win_rate": 0.75,
            "sample_size": 20,
            "avg_opt_ret": 0.18,
        },
        {
            "trend": {"vwap": 50.0},
            "sector": {"direction": "green", "change_pct": 1.0},
            "market": {"direction": "green", "change_pct": 0.3},
            "levels": {"daily_trigger": 50.25},
        },
    )
    assert "volume_confirmation" in profile["components"]
    assert "vwap_context" in profile["components"]
    assert "sector_context" in profile["components"]
    assert "trend_vwap_alignment" not in profile["components"]
    assert profile["diagnostics"]["observe_only"] is True
    json.dumps(profile)
