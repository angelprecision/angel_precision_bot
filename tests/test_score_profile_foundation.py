from __future__ import annotations

import json

from ap.score_profile_config import BREAKOUT_VOLUME_MIN, RELATIVE_VOLUME_OK, RELATIVE_VOLUME_STRONG, VWAP_CHOP_ZONE_PCT, VWAP_SUPPORT_RESISTANCE_ZONE_PCT
from ap.score_profile_side import normalize_signal_side
from ap.trigger_geometry import score_remaining_opportunity, score_trigger_geometry


def test_shared_side_normalization_never_defaults_unknown_to_call():
    assert normalize_signal_side("") == "UNKNOWN"
    assert normalize_signal_side(None) == "UNKNOWN"
    assert normalize_signal_side("garbage") == "UNKNOWN"
    assert normalize_signal_side("CALL") == "CALL"
    assert normalize_signal_side("BUY") == "CALL"
    assert normalize_signal_side("SELL") == "PUT"
    assert normalize_signal_side("SHORT") == "PUT"


def test_score_profile_config_exports_expected_threshold_names():
    assert RELATIVE_VOLUME_STRONG > RELATIVE_VOLUME_OK
    assert BREAKOUT_VOLUME_MIN > 1.0
    assert VWAP_CHOP_ZONE_PCT > 0
    assert VWAP_SUPPORT_RESISTANCE_ZONE_PCT > VWAP_CHOP_ZONE_PCT


def test_trigger_geometry_poor_rr_is_diagnostic_not_invalid_geometry():
    result = score_trigger_geometry({"side": "CALL", "entry_price": 50.0, "stop_price": 49.0, "target_price": 50.5})
    assert result["status"] == "block_recommended"
    assert result["reason"] == "poor reward/risk diagnostic"
    assert result["score"] == 1.0
    assert result["invalid_geometry_blocks"] == []
    assert "poor_underlying_reward_to_risk" in result["block_recommendations"]
    json.dumps(result)


def test_trigger_geometry_unknown_side_does_not_score_as_call():
    result = score_trigger_geometry({"side": "", "entry_price": 50.0, "stop_price": 49.0, "target_price": 53.0})
    assert result["diagnostics"]["side"] == "UNKNOWN"
    assert "side" in result["missing_data"]
    assert "unknown_side" in result["invalid_geometry_blocks"]
    assert result["score"] == 0.0
    json.dumps(result)


def test_remaining_opportunity_unknown_side_does_not_score_as_call_or_put():
    result = score_remaining_opportunity({"side": "", "current_price": 50.0, "stop_price": 49.0, "target_price": 53.0})
    assert result["diagnostics"]["side"] == "UNKNOWN"
    assert "side" in result["missing_data"]
    assert "unknown_side" in result["block_recommendations"]
    assert result["score"] == 0.0
    json.dumps(result)
