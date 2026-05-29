"""Tests for the score-65 structured eligibility refinement.

Covers _score_allows_entry() in ap_master_control.py — the structured
replacement for the blunt `effective_score < hard_floor` reject.

Run: DATABASE_URL=postgresql://test:test@localhost/test python3 -m pytest tests/test_score65_eligibility.py -v
"""
import os
import importlib
import pytest


def _load(allow="true", floor="65", max_spread="0.08", min_delta="0.35"):
    os.environ["SCORE65_ALLOW"] = allow
    os.environ["SCORE65_FLOOR"] = floor
    os.environ["SCORE65_MAX_SPREAD_PCT"] = max_spread
    os.environ["SCORE65_MIN_DELTA"] = min_delta
    import ap_master_control as mc
    importlib.reload(mc)
    return mc._score_allows_entry


HF = 70.0


class TestScore65Eligibility:
    def test_normal_path_above_floor(self):
        f = _load()
        ok, reason = f(score=72, hard_floor=HF, is_0dte=False, is_index=False,
                       timeframe="1d", spread_pct=0.05, delta=0.5)
        assert ok and reason == ""

    def test_below_65_rejected(self):
        f = _load()
        ok, reason = f(score=60, hard_floor=HF, is_0dte=False, is_index=False,
                       timeframe="1d", spread_pct=0.05, delta=0.5)
        assert not ok and reason == "REJECTED_LOW_SCORE_UNDER_65"

    def test_0dte_at_65_always_rejected_index(self):
        f = _load()
        ok, reason = f(score=65, hard_floor=HF, is_0dte=True, is_index=True,
                       timeframe="1d", spread_pct=0.05, delta=0.5)
        assert not ok and reason == "REJECTED_SCORE65_0DTE"

    def test_0dte_at_68_always_rejected_single(self):
        f = _load()
        ok, reason = f(score=68, hard_floor=HF, is_0dte=True, is_index=False,
                       timeframe="5m", spread_pct=0.03, delta=0.6)
        assert not ok and reason == "REJECTED_SCORE65_0DTE"

    def test_wide_spread_rejected(self):
        f = _load()
        ok, reason = f(score=66, hard_floor=HF, is_0dte=False, is_index=False,
                       timeframe="1d", spread_pct=0.15, delta=0.5)
        assert not ok and reason == "REJECTED_SCORE65_WIDE_SPREAD"

    def test_weak_delta_rejected(self):
        f = _load()
        ok, reason = f(score=66, hard_floor=HF, is_0dte=False, is_index=False,
                       timeframe="1d", spread_pct=0.05, delta=0.20)
        assert not ok and reason == "REJECTED_SCORE65_WEAK_CONTRACT"

    def test_weak_negative_put_delta_rejected(self):
        f = _load()
        ok, reason = f(score=66, hard_floor=HF, is_0dte=False, is_index=False,
                       timeframe="1d", spread_pct=0.05, delta=-0.20)
        assert not ok and reason == "REJECTED_SCORE65_WEAK_CONTRACT"

    def test_clean_daily_admitted(self):
        f = _load()
        ok, reason = f(score=65, hard_floor=HF, is_0dte=False, is_index=False,
                       timeframe="1d", spread_pct=0.05, delta=0.5)
        assert ok and reason == "ALLOWED_SCORE65_NON_0DTE_DAILY"

    def test_clean_intraday_admitted(self):
        f = _load()
        ok, reason = f(score=67, hard_floor=HF, is_0dte=False, is_index=False,
                       timeframe="15m", spread_pct=0.05, delta=0.5)
        assert ok and reason == "ALLOWED_SCORE65_NON_0DTE_CLEAN"

    def test_missing_quote_data_does_not_block(self):
        f = _load()
        ok, reason = f(score=66, hard_floor=HF, is_0dte=False, is_index=False,
                       timeframe="1d", spread_pct=None, delta=None)
        assert ok and reason == "ALLOWED_SCORE65_NON_0DTE_DAILY"

    def test_index_non_0dte_clean_admitted(self):
        f = _load()
        ok, reason = f(score=65, hard_floor=HF, is_0dte=False, is_index=True,
                       timeframe="1d", spread_pct=0.05, delta=0.5)
        assert ok and reason == "ALLOWED_SCORE65_NON_0DTE_DAILY"

    def test_feature_off_preserves_blunt_behavior(self):
        f = _load(allow="false")
        ok, reason = f(score=66, hard_floor=HF, is_0dte=False, is_index=False,
                       timeframe="1d", spread_pct=0.05, delta=0.5)
        assert not ok and reason == "REJECTED_LOW_SCORE"

    def test_feature_off_still_rejects_under_65_specifically(self):
        f = _load(allow="false")
        ok, reason = f(score=60, hard_floor=HF, is_0dte=False, is_index=False,
                       timeframe="1d", spread_pct=0.05, delta=0.5)
        assert not ok and reason == "REJECTED_LOW_SCORE_UNDER_65"
