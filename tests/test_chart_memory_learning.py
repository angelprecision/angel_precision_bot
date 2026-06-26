import base64
import json

from ap_chart_memory_learning import (
    build_chart_learning_payload,
    build_manual_backfill_payload,
    build_vision_prompt,
    image_hash_from_base64,
    normalize_confidence,
    normalize_features,
    normalize_trade_result,
    parse_vision_json_response,
    sha256_bytes,
    summarize_learning_row,
)


def test_normalize_confidence_clamps_values():
    assert normalize_confidence(105) == 100
    assert normalize_confidence(-10) == 0
    assert normalize_confidence("72") == 72
    assert normalize_confidence("bad") is None


def test_normalize_trade_result_rejects_unknown_values():
    assert normalize_trade_result("winner") == "winner"
    assert normalize_trade_result("LOSER") == "loser"
    assert normalize_trade_result("moon") == "unknown"


def test_normalize_features_preserves_stable_shape_and_extras():
    features = normalize_features(
        {
            "trend": "Bullish",
            "above_vwap": "true",
            "higher_high": "yes",
            "distance_from_trigger_pct": "0.42",
            "custom_note": "clean morning push",
        }
    )

    assert features["trend"] == "bullish"
    assert features["above_vwap"] is True
    assert features["higher_high"] is True
    assert features["distance_from_trigger_pct"] == 0.42
    assert features["extra_features"]["custom_note"] == "clean morning push"


def test_image_hash_from_base64_supports_data_url():
    raw = b"fake-image-bytes"
    encoded = base64.b64encode(raw).decode("ascii")
    data_url = "data:image/png;base64," + encoded

    assert image_hash_from_base64(data_url) == sha256_bytes(raw)


def test_build_chart_learning_payload_preserves_taxonomy_and_outcome_linkage():
    payload = build_chart_learning_payload(
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        signal_id="sig-123",
        ticker="wfc",
        side="call",
        timeframe="1d",
        trigger_level=77.62,
        current_price=83.98,
        target_level=78.56,
        stop_level=76.8,
        chart_state="late_chase",
        chart_confidence=91,
        reason="Current price is far above trigger and target is already behind price.",
        features={"trend": "exhausted", "distance_from_trigger_pct": 8.2},
        trade_result="loser",
        proof_trade_id="proof-1",
        position_id="pos-1",
        broker_order_id="ord-1",
        operator_notes="Entered after move was gone.",
        mistake="late chase",
        lesson="Do not buy when target is behind current price.",
        review_source="manual_backfill",
    )

    assert payload["client_id"] == "jasoncosby1@gmail.com"
    assert payload["execution_mode"] == "live"
    assert payload["signal_id"] == "sig-123"
    assert payload["ticker"] == "WFC"
    assert payload["side"] == "CALL"
    assert payload["chart_state"] == "late_chase"
    assert payload["chart_confidence"] == 91
    assert payload["trade_result"] == "loser"
    assert payload["proof_trade_id"] == "proof-1"
    assert payload["metadata"]["features"]["trend"] == "exhausted"
    assert payload["metadata"]["schema_version"] == "chart_learning_v1"


def test_build_manual_backfill_payload_accepts_scanner_aliases():
    raw = b"chart"
    row = {
        "client_id": "jasoncosby1@gmail.com",
        "execution_mode": "live",
        "canonical_signal_id": "sig-456",
        "symbol": "aapl",
        "direction": "put",
        "timeframe": "1d",
        "entry_trigger": 201.5,
        "underlying_entry": 200.9,
        "chart_state": "clean_rejection",
        "chart_confidence": "88",
        "trade_result": "winner",
        "image_base64": base64.b64encode(raw).decode("ascii"),
    }

    payload = build_manual_backfill_payload(row)

    assert payload["signal_id"] == "sig-456"
    assert payload["ticker"] == "AAPL"
    assert payload["side"] == "PUT"
    assert payload["trigger_level"] == 201.5
    assert payload["current_price"] == 200.9
    assert payload["chart_state"] == "clean_rejection"
    assert payload["image_hash"] == sha256_bytes(raw)


def test_parse_vision_json_response_normalizes_output():
    response = json.dumps(
        {
            "chart_state": "Clean_Reclaim",
            "chart_confidence": 93,
            "reason": "Trigger reclaimed and fresh high followed.",
            "features": {"above_vwap": True, "fresh_extension": True},
        }
    )

    parsed = parse_vision_json_response(response)

    assert parsed["chart_state"] == "clean_reclaim"
    assert parsed["chart_confidence"] == 93
    assert parsed["features"]["above_vwap"] is True
    assert parsed["features"]["fresh_extension"] is True


def test_build_vision_prompt_contains_required_json_contract():
    prompt = build_vision_prompt(
        ticker="AAPL",
        side="CALL",
        timeframe="1d",
        trigger_level=201.5,
        target_level=205,
        stop_level=198,
    )

    assert "Return only JSON" in prompt
    assert "AAPL" in prompt
    assert "chart_state" in prompt
    assert "distance_from_trigger_pct" in prompt


def test_summarize_learning_row_is_compact():
    summary = summarize_learning_row(
        {
            "ticker": "AAPL",
            "side": "CALL",
            "timeframe": "1d",
            "chart_state": "clean_reclaim",
            "chart_confidence": 91,
            "trade_result": "winner",
            "signal_id": "sig-1",
        }
    )

    assert "AAPL CALL 1d" in summary
    assert "state=clean_reclaim" in summary
