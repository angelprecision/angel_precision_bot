from ap.news_earnings_context import score_news_earnings_context


def test_news_earnings_context_shape_keys():
    result = score_news_earnings_context({"earnings_risk": False, "news_risk": False})
    for key in ("score", "max_score", "status", "missing_data", "warnings", "block_recommendations", "diagnostics"):
        assert key in result
