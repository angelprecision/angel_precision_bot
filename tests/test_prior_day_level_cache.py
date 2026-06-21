"""
tests/test_prior_day_level_cache.py — PR4

Verifies the prior-day high/low cache fallback with a trading-session freshness
guard:
  - Flag OFF (default) → no caching, no fallback (behavior unchanged).
  - Cache write stamps the trading session the levels represent.
  - Fallback uses cache ONLY when the stamped session matches the actual prior
    trading session.
  - Stale cache (wrong session: weekend/holiday/halt) is rejected → fail-safe.
  - _prior_trading_session_date walks back over weekends correctly.
"""
from __future__ import annotations

import sys
import os
import importlib.util
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

_REPO = Path(__file__).resolve().parents[1]
_SRC  = (_REPO / "ap_overnight_reeval.py").read_text()


# ---------------------------------------------------------------------------
# Source guards
# ---------------------------------------------------------------------------

class TestSourceGuards:
    def test_flag_defaults_off(self):
        assert 'os.getenv("PRIOR_LEVEL_CACHE_FALLBACK", "0")' in _SRC

    def test_session_helper_present(self):
        assert "def _prior_trading_session_date(" in _SRC

    def test_cache_write_and_read_present(self):
        assert "def _cache_prior_levels(" in _SRC
        assert "def _get_cached_prior_levels(" in _SRC

    def test_freshness_guard_present(self):
        """The read must reject a cache whose session != expected session."""
        idx = _SRC.find("def _get_cached_prior_levels(")
        end = _SRC.find("\ndef ", idx + 10)
        body = _SRC[idx:end]
        assert 'rec.get("session_date") != expected_session.isoformat()' in body
        assert "return None" in body

    def test_fallback_only_when_fresh_null(self):
        """Cache fallback only triggers when fresh levels are null."""
        assert "PRIOR_LEVEL_CACHE_FALLBACK_USED" in _SRC
        assert "fresh fetch null" in _SRC.lower() or "fresh fetch null" in _SRC


# ---------------------------------------------------------------------------
# Behavioral tests — load module with deps stubbed
# ---------------------------------------------------------------------------

def _load(env_overrides: dict | None = None):
    stubs = {
        "ap.brokers": MagicMock(),
        "ap.brokers.tradier": MagicMock(),
        "yfinance": MagicMock(),
        "requests": MagicMock(),
        "ap.db": MagicMock(),
        "psycopg2": MagicMock(),
    }
    env = dict(os.environ)
    if env_overrides:
        env.update(env_overrides)
    name = "ap_overnight_reeval_shim"
    with patch.dict(sys.modules, stubs), patch.dict(os.environ, env, clear=False):
        spec = importlib.util.spec_from_file_location(name, _REPO / "ap_overnight_reeval.py")
        mod = importlib.util.module_from_spec(spec)
        sys.modules[name] = mod
        try:
            spec.loader.exec_module(mod)
        finally:
            sys.modules.pop(name, None)
    return mod


class TestPriorTradingSession:
    def test_monday_returns_friday(self):
        mod = _load()
        # Monday 2026-06-22 → prior trading session Friday 2026-06-19
        monday = datetime(2026, 6, 22, 9, 35, tzinfo=ZoneInfo("America/New_York"))
        assert mod._prior_trading_session_date(monday) == date(2026, 6, 19)

    def test_tuesday_returns_monday(self):
        mod = _load()
        tuesday = datetime(2026, 6, 23, 9, 35, tzinfo=ZoneInfo("America/New_York"))
        assert mod._prior_trading_session_date(tuesday) == date(2026, 6, 22)

    def test_sunday_returns_friday(self):
        mod = _load()
        sunday = datetime(2026, 6, 21, 9, 35, tzinfo=ZoneInfo("America/New_York"))
        assert mod._prior_trading_session_date(sunday) == date(2026, 6, 19)


class TestCacheReadWrite:
    def test_write_then_read_same_session(self):
        mod = _load({"PRIOR_LEVEL_CACHE_FALLBACK": "1"})
        session = date(2026, 6, 19)
        mod._cache_prior_levels("AMAT", session, 640.0, 620.0, 632.0)
        rec = mod._get_cached_prior_levels("AMAT", session)
        assert rec is not None
        assert rec["prior_day_high"] == 640.0
        assert rec["prior_day_low"] == 620.0

    def test_read_rejects_wrong_session(self):
        """Stale cache: stamped session != expected → None (fail-safe)."""
        mod = _load({"PRIOR_LEVEL_CACHE_FALLBACK": "1"})
        mod._cache_prior_levels("AMAT", date(2026, 6, 18), 640.0, 620.0, 632.0)
        # expected prior session is the 19th, cache holds the 18th → stale
        rec = mod._get_cached_prior_levels("AMAT", date(2026, 6, 19))
        assert rec is None

    def test_read_missing_ticker_returns_none(self):
        mod = _load({"PRIOR_LEVEL_CACHE_FALLBACK": "1"})
        assert mod._get_cached_prior_levels("NOPE", date(2026, 6, 19)) is None

    def test_write_skips_when_both_levels_none(self):
        mod = _load({"PRIOR_LEVEL_CACHE_FALLBACK": "1"})
        mod._cache_prior_levels("EMPTY", date(2026, 6, 19), None, None, None)
        assert mod._get_cached_prior_levels("EMPTY", date(2026, 6, 19)) is None


class TestFlagOff:
    def test_flag_off_constant_false(self):
        mod = _load({"PRIOR_LEVEL_CACHE_FALLBACK": "0"})
        assert mod._PRIOR_LEVEL_CACHE_ENABLED is False

    def test_flag_on_constant_true(self):
        mod = _load({"PRIOR_LEVEL_CACHE_FALLBACK": "1"})
        assert mod._PRIOR_LEVEL_CACHE_ENABLED is True


class TestFailSafe:
    def test_cache_helpers_never_raise(self):
        mod = _load({"PRIOR_LEVEL_CACHE_FALLBACK": "1"})
        # Garbage inputs must not raise
        mod._cache_prior_levels(None, date(2026, 6, 19), "x", object(), None)
        assert mod._get_cached_prior_levels(None, date(2026, 6, 19)) in (None, mod._PRIOR_LEVEL_CACHE.get("") )
