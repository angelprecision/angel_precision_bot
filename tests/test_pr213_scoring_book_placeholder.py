from pathlib import Path


def test_pr213_handoff_document_exists():
    assert Path("docs/pr213_price_stacking_news_earnings_plan.md").exists()
