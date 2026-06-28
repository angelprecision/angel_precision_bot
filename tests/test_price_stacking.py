from ap.price_stacking import score_price_stacking


def test_price_stacking_nearby_levels():
    result = score_price_stacking({"entry_price": 100.0, "levels": {"daily_high": 100.2, "weekly_low": 99.8}})
    assert result["score"] > 0
    assert result["stack_count"] == 2


def test_price_stacking_missing_levels():
    result = score_price_stacking({"entry_price": 100.0})
    assert result["status"] == "missing_data"
