from ap.news_earnings_context import score_news_earnings_context


def test_news_earnings_context_missing_data_is_safe():
    result = score_news_earnings_context({})
    assert result["status"] == "missing_data"
    assert result["diagnostics"]["observe_only"] is True
    assert result["diagnostics"]["no_external_fetch"] is True


def test_news_earnings_context_flags_event_risk():
    result = score_news_earnings_context({"earnings_risk": True, "news_risk": True})
    assert result["status"] == "block_recommended"
    assert "earnings_risk_present" in result["block_recommendations"]
