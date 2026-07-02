from ap.screenshot_context import normalize_screenshot_context


def test_missing_context_returns_safe_observe_only_shape():
    context = normalize_screenshot_context(None)

    assert context["context_version"] == "screenshot_context_v1"
    assert context["available"] is False
    assert context["visual_bias"] == "unclear"
    assert context["timeframes"] == {}
    assert "screenshot_context" in context["missing_data"]
    assert context["diagnostics"]["observe_only"] is True
    assert context["diagnostics"]["trading_authority"] is False
    assert context["diagnostics"]["can_approve_trade"] is False
    assert context["diagnostics"]["can_block_trade"] is False


def test_normalizes_single_timeframe_screenshot_payload():
    context = normalize_screenshot_context(
        {
            "ticker": "WFC",
            "timeframe": "4h",
            "image_url": "https://example.test/wfc.png",
            "visual_bias": "bullish",
            "detected_fvg_zones": [{"low": "38.1", "high": "39.2"}],
            "support_levels": ["38.0"],
            "resistance_levels": [41.5],
            "operator_notes": "clean chart",
            "confidence": 0.72,
        }
    )

    assert context["available"] is True
    assert context["visual_bias"] == "bullish"
    assert context["timeframes"]["4h"]["support_levels"] == [38.0]
    assert context["timeframes"]["4h"]["resistance_levels"] == [41.5]
    assert context["timeframes"]["4h"]["fvg_zones"] == [{"low": 38.1, "high": 39.2}]
    assert context["timeframes"]["4h"]["trend_label"] is None
    assert context["timeframes"]["4h"]["confidence"] == 0.72
    assert context["missing_data"] == []
    assert context["diagnostics"]["image_url_present"] is True
    assert context["diagnostics"]["operator_notes_present"] is True


def test_screenshot_labels_do_not_create_active_trade_authority():
    context = normalize_screenshot_context(
        {
            "ticker": "WFC",
            "timeframe": "daily",
            "image_url": "https://example.test/wfc.png",
            "visual_bias": "call",
            "confidence": 95,
        }
    )

    assert "daily" in context["timeframes"]
    assert context["visual_bias"] == "bullish"
    assert context["timeframes"]["daily"]["confidence"] == 0.95
    assert context["diagnostics"]["observe_only"] is True
    assert context["diagnostics"]["active_gate"] is False
    assert context["diagnostics"]["trading_authority"] is False
    assert context["diagnostics"]["can_approve_trade"] is False
    assert context["diagnostics"]["can_block_trade"] is False
    assert "confidence_percent_converted" in context["warnings"]


def test_nested_timeframes_are_preserved_without_execution_mutation_fields():
    context = normalize_screenshot_context(
        {
            "client_id": "client@example.test",
            "execution_mode": "live",
            "signal_id": "sig-123",
            "ticker": "WFC",
            "image_url": "https://example.test/wfc.png",
            "visual_bias": "mixed",
            "timeframes": {
                "4h": {
                    "support_levels": [{"price": "38.0", "source": "vision"}],
                    "resistance_levels": [],
                    "fvg_zones": [{"bottom": "37.5", "top": "38.5"}],
                    "trend_label": "uptrend",
                    "confidence": "0.64",
                },
                "daily": {
                    "support_levels": [],
                    "resistance_levels": ["42.0"],
                    "confidence": "not-a-number",
                },
            },
        }
    )

    assert context["timeframes"]["4h"]["support_levels"] == [
        {"price": "38.0", "source": "vision", "level": 38.0}
    ]
    assert context["timeframes"]["4h"]["fvg_zones"] == [
        {"bottom": "37.5", "top": "38.5", "low": 37.5, "high": 38.5}
    ]
    assert context["timeframes"]["4h"]["trend_label"] == "uptrend"
    assert context["timeframes"]["daily"]["resistance_levels"] == [42.0]
    assert context["timeframes"]["daily"]["confidence"] is None
    assert context["diagnostics"]["client_id"] == "client@example.test"
    assert context["diagnostics"]["execution_mode"] == "live"
    assert context["diagnostics"]["signal_id"] == "sig-123"


def test_invalid_bias_and_missing_image_warn_but_do_not_crash():
    context = normalize_screenshot_context(
        {
            "ticker": "WFC",
            "timeframe": "4h",
            "visual_bias": "moon",
            "support_levels": ["bad"],
        }
    )

    assert context["available"] is True
    assert context["visual_bias"] == "unclear"
    assert "image_url" in context["missing_data"]
    assert "visual_bias_unrecognized" in context["warnings"]
    assert "image_url_missing" in context["warnings"]
    assert "support_levels_unusable" in context["warnings"]
