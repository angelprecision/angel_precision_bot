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

class TestUnknownDTE:
    """Item 2 — dte_known guard. Added on top of PR #55 post-merge."""

    def test_unknown_dte_rejected_in_65_band(self):
        f = _load()
        ok, reason = f(score=66, hard_floor=HF, is_0dte=False, is_index=False,
                       timeframe="1d", spread_pct=0.05, delta=0.5, dte_known=False)
        assert not ok and reason == "REJECTED_SCORE65_UNKNOWN_DTE"

    def test_unknown_dte_rejected_even_clean_everything(self):
        f = _load()
        ok, reason = f(score=69, hard_floor=HF, is_0dte=False, is_index=False,
                       timeframe="daily", spread_pct=0.02, delta=0.6, dte_known=False)
        assert not ok and reason == "REJECTED_SCORE65_UNKNOWN_DTE"

    def test_known_dte_still_admitted(self):
        f = _load()
        ok, reason = f(score=66, hard_floor=HF, is_0dte=False, is_index=False,
                       timeframe="1d", spread_pct=0.05, delta=0.5, dte_known=True)
        assert ok and reason == "ALLOWED_SCORE65_NON_0DTE_DAILY"

    def test_default_dte_known_true_backcompat(self):
        # Default dte_known=True so all existing callers/tests are unaffected.
        f = _load()
        ok, reason = f(score=66, hard_floor=HF, is_0dte=False, is_index=False,
                       timeframe="1d", spread_pct=0.05, delta=0.5)
        assert ok and reason == "ALLOWED_SCORE65_NON_0DTE_DAILY"

    def test_unknown_dte_does_not_affect_score_70(self):
        f = _load()
        ok, reason = f(score=72, hard_floor=HF, is_0dte=False, is_index=False,
                       timeframe="1d", spread_pct=0.05, delta=0.5, dte_known=False)
        assert ok and reason == ""

    def test_unknown_dte_below_65_still_under65(self):
        f = _load()
        ok, reason = f(score=60, hard_floor=HF, is_0dte=False, is_index=False,
                       timeframe="1d", spread_pct=0.05, delta=0.5, dte_known=False)
        assert not ok and reason == "REJECTED_LOW_SCORE_UNDER_65"


class TestExpirationStrptimeParsing:
    """PR #58 review fix: expiration must be validated with strptime, not dash
    position checks. Bad values like '2026-ab-cd' or '2026-99-99' look like
    YYYY-MM-DD but are not real dates — they must not set _dte_known=True.
    """

    @staticmethod
    def _resolve_dte(dte_val, exp_val):
        """Mirror the caller's DTE-resolution block so we can test it directly
        without running full evaluate(). Returns (is_0dte, dte_known)."""
        import importlib, os, sys
        from datetime import datetime, date
        from zoneinfo import ZoneInfo
        is_0dte = False
        dte_known = False
        try:
            today_str = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
            if dte_val is not None:
                try:
                    is_0dte = int(dte_val) == 0
                    dte_known = True
                except (TypeError, ValueError):
                    pass
            if isinstance(exp_val, str) and len(exp_val) >= 10:
                try:
                    exp_date = datetime.strptime(exp_val[:10], "%Y-%m-%d").date()
                    today_date = datetime.now(ZoneInfo("America/New_York")).date()
                    if exp_date == today_date:
                        is_0dte = True
                    dte_known = True
                except (ValueError, TypeError):
                    pass
        except Exception:
            pass
        return is_0dte, dte_known

    def test_valid_future_expiration_sets_dte_known(self):
        _, dte_known = self._resolve_dte(None, "2027-01-15")
        assert dte_known is True

    def test_invalid_month_letters_does_not_set_dte_known(self):
        # "2026-ab-cd" has dashes in the right positions but is not a real date.
        _, dte_known = self._resolve_dte(None, "2026-ab-cd")
        assert dte_known is False, "alphabetic month must not set dte_known"

    def test_impossible_date_99_does_not_set_dte_known(self):
        # "2026-99-99" passes a dash-position check but strptime rejects it.
        _, dte_known = self._resolve_dte(None, "2026-99-99")
        assert dte_known is False, "month=99 must not set dte_known"

    def test_empty_expiration_does_not_set_dte_known(self):
        _, dte_known = self._resolve_dte(None, "")
        assert dte_known is False

    def test_short_string_does_not_set_dte_known(self):
        _, dte_known = self._resolve_dte(None, "2026-01")
        assert dte_known is False

    def test_none_expiration_and_none_dte_leaves_dte_unknown(self):
        _, dte_known = self._resolve_dte(None, None)
        assert dte_known is False

    def test_valid_dte_int_overrides_missing_expiration(self):
        _, dte_known = self._resolve_dte(7, None)
        assert dte_known is True

    def test_unparseable_dte_string_stays_unknown(self):
        _, dte_known = self._resolve_dte("weekly", None)
        assert dte_known is False

    def test_valid_date_that_is_today_sets_0dte(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo
        today = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
        is_0dte, dte_known = self._resolve_dte(None, today)
        assert is_0dte is True
        assert dte_known is True
