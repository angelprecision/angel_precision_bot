from __future__ import annotations

from copy import deepcopy
import json

from ap.score_profile_side import normalize_signal_side
from ap.volume_confirmation import score_volume_confirmation


def test_volume_confirmation_json_safe_no_mutation_and_confirms_breakout():
    signal = {"ticker": "WFC", "side": "CALL", "pattern": "daily breakout", "relative_volume": 1.9, "breakout_volume_ratio": 1.7, "volume_direction": "bullish"}
    before = deepcopy(signal)
    result = score_volume_confirmation(signal, {"volume": {}})
    assert signal == before
    json.dumps(result)
    assert result["score"] > 8
    assert result["status"] == "ok"
    assert result["diagnostics"]["observe_only"] is True


def test_volume_confirmation_records_missing_data():
    result = score_volume_confirmation({"side": "CALL"}, {})
    assert result["status"] == "missing_data"
    assert "relative_volume" in result["missing_data"]
    assert "volume_direction" in result["missing_data"]
    json.dumps(result)


def test_volume_confirmation_block_recommendations_are_diagnostics_only():
    result = score_volume_confirmation({"side": "CALL", "pattern": "breakout", "relative_volume": 0.8, "breakout_volume_ratio": 0.8, "volume_direction": "bearish"}, {})
    assert result["status"] == "block_recommended"
    assert "weak_volume_breakout" in result["block_recommendations"]
    assert "volume_direction_opposes_signal" in result["block_recommendations"]
    assert result["diagnostics"]["block_recommendations_are_diagnostic_only"] is True


def test_volume_confirmation_high_volume_rejection_is_diagnostic_only():
    result = score_volume_confirmation({"side": "CALL", "relative_volume": 1.7, "volume_direction": "bullish", "high_volume_rejection": True}, {})
    assert result["status"] == "block_recommended"
    assert "high_volume_rejection" in result["warnings"]
    assert "high_volume_rejection" in result["block_recommendations"]
    assert result["diagnostics"]["block_recommendations_are_diagnostic_only"] is True


def test_volume_confirmation_unknown_side_does_not_default_to_call():
    result = score_volume_confirmation({"side": "", "relative_volume": 1.9, "volume_direction": "bullish"}, {})
    assert result["diagnostics"]["side"] == "UNKNOWN"
    assert "side" in result["missing_data"]
    assert "unknown_side" in result["block_recommendations"]
    assert "volume_direction_opposes_signal" not in result["block_recommendations"]
    assert "volume_direction_unscored_unknown_side" in result["warnings"]


def test_sell_and_short_resolve_to_put():
    assert normalize_signal_side("SELL") == "PUT"
    assert normalize_signal_side("SHORT") == "PUT"
    result = score_volume_confirmation({"side": "SELL", "relative_volume": 1.9, "volume_direction": "bearish"}, {})
    assert result["diagnostics"]["side"] == "PUT"
    assert "volume_direction_matches_signal" in result["boosts"]
