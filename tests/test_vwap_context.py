from __future__ import annotations

from copy import deepcopy
import json

from ap.vwap_context import score_vwap_context


def test_vwap_context_json_safe_no_mutation_and_call_above_vwap():
    signal = {"ticker": "WFC", "side": "CALL", "entry_price": 50.2, "current_price": 50.4}
    before = deepcopy(signal)
    result = score_vwap_context(signal, {"trend": {"vwap": 50.0}})
    assert signal == before
    json.dumps(result)
    assert result["score"] >= 7
    assert result["diagnostics"]["aligned_with_signal"] is True
    assert "call_above_vwap" in result["boosts"]


def test_vwap_context_put_below_vwap_scores():
    result = score_vwap_context({"ticker": "WFC", "side": "PUT", "entry_price": 49.8, "current_price": 49.6}, {"trend": {"vwap": 50.0}})
    assert result["diagnostics"]["aligned_with_signal"] is True
    assert "put_below_vwap" in result["boosts"]
    assert result["score"] >= 7
    json.dumps(result)


def test_vwap_context_records_missing_data():
    result = score_vwap_context({"side": "PUT"}, {})
    assert result["status"] == "missing_data"
    assert "current_price" in result["missing_data"]
    assert "vwap" in result["missing_data"]
    json.dumps(result)


def test_vwap_context_chop_zone_warning_is_diagnostic_only():
    result = score_vwap_context({"side": "CALL", "current_price": 50.02, "entry_price": 50.02, "vwap": 50.0}, {})
    assert result["status"] == "block_recommended"
    assert "entry_too_close_to_vwap_chop_zone" in result["warnings"]
    assert "entry_too_close_to_vwap_chop_zone" in result["block_recommendations"]
    assert result["diagnostics"]["block_recommendations_are_diagnostic_only"] is True


def test_vwap_context_unknown_side_does_not_default_to_call():
    result = score_vwap_context({"side": "", "current_price": 50.4, "entry_price": 50.2, "vwap": 50.0}, {})
    assert result["diagnostics"]["side"] == "UNKNOWN"
    assert result["diagnostics"]["aligned_with_signal"] is None
    assert "side" in result["missing_data"]
    assert "unknown_side" in result["block_recommendations"]
    assert "call_above_vwap" not in result["boosts"]
    assert "vwap_alignment_unscored_unknown_side" in result["warnings"]
