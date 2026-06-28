from ap.price_stacking import score_price_stacking


def test_price_stacking_shape_keys():
    result = score_price_stacking({"entry_price": 100, "levels": {"x": 100}})
    for key in ("score", "max_score", "status", "missing_data", "warnings", "block_recommendations", "diagnostics"):
        assert key in result
