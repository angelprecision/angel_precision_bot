"""
tests/test_pr111_overnight_snapshot_bars.py
PR#111 regression: fetch_market_snapshot uses intraday bars for session high/low.
Quote high/low are not the source of truth for overnight validation.
"""
from pathlib import Path
import re, pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone, timedelta

_REPO = Path(__file__).resolve().parents[1]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _import_validator():
    import importlib.util, sys
    spec = importlib.util.spec_from_file_location(
        "ap.overnight_daily_validator",
        _REPO / "ap" / "overnight_daily_validator.py",
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("ap.overnight_daily_validator", mod)
    spec.loader.exec_module(mod)
    return mod


def _broker_stub(quote=None, bars=None):
    """Return a broker stub with configurable quote + timesales responses."""
    b = MagicMock()
    b.base_url = "https://sandbox.tradier.com"
    sess = MagicMock()
    b.session = sess

    def _get(url, params=None, headers=None, timeout=None):
        r = MagicMock()
        if "timesales" in url:
            if bars is None:
                r.status_code = 200
                r.json.return_value = {"series": None}
            else:
                # bars is a list of bar dicts
                items = bars if len(bars) != 1 else bars[0]
                r.status_code = 200
                r.json.return_value = {
                    "series": {"data": {"item": items}}
                }
        else:  # quotes
            if quote is None:
                r.status_code = 200
                r.json.return_value = {"quotes": {"quote": {}}}
            else:
                r.status_code = 200
                r.json.return_value = {"quotes": {"quote": quote}}
        return r

    sess.get.side_effect = _get
    return b


def _et_now(hour=9, minute=35, offset_h=-4):
    """Return a fake ET datetime (UTC with offset for testing)."""
    tz = timezone(timedelta(hours=offset_h))
    return datetime(2026, 6, 10, hour, minute, 0, tzinfo=tz)


def _patch_et(mod, hour=9, minute=35):
    """Context-manager patch to fix 'now' inside fetch_market_snapshot."""
    fake_now = _et_now(hour, minute)
    fake_utc = fake_now.astimezone(timezone.utc)
    return patch.object(mod.datetime, "now", return_value=fake_utc)


# ── Source-level checks ───────────────────────────────────────────────────────

def test_source_no_quote_high_low_as_session_source():
    src = (_REPO / "ap" / "overnight_daily_validator.py").read_text()
    # quote["high"] / quote["low"] must no longer be the primary source (unchanged)
    assert "session_high = float(quote.get" not in src, (
        "quote.get('high') must not be the source for session_high"
    )
    # PR #181: timesales / _fetch_intraday_bars replaced by Polygon snapshot
    assert "_fetch_polygon_daily_snapshot" in src, (
        "_fetch_polygon_daily_snapshot must be in source after PR #181"
    )
    assert "POLYGON_API_KEY" in src
    assert "OVERNIGHT_POLYGON_SNAPSHOT_OK" in src

def test_source_has_all_required_log_strings():
    src = (_REPO / "ap" / "overnight_daily_validator.py").read_text()
    # PR #181: replaced timesales log strings with Polygon equivalents
    for log_str in [
        "OVERNIGHT_POLYGON_SNAPSHOT_OK",
        "OVERNIGHT_POLYGON_SNAPSHOT_NO_HILO",
        "OVERNIGHT_SESSION_BARS_NOT_READY",    # kept — still emitted on RETRY_LATER
    ]:
        assert log_str in src, f"Log string missing after PR #181: {log_str}"


# ── AC1: Quote has no high/low → RETRY_LATER, signal stays WATCHING ───────────

def test_ac1_quote_missing_hilo_returns_none():
    """
    Quote returns last/bid/ask but NOT high/low.
    No intraday bars (pre-open).
    Expected: fetch_market_snapshot returns None → caller does RETRY_LATER.
    No hard invalidation from missing quote high/low alone.
    """
    mod = _import_validator()
    quote_no_hilo = {"last": 470.5, "bid": 470.4, "ask": 470.6,
                     "high": None, "low": None}
    broker = _broker_stub(quote=quote_no_hilo, bars=None)

    # 9:10 ET — before session, no bars
    fake_et = _et_now(9, 10)
    with patch("datetime.datetime") as mock_dt:
        mock_dt.now.return_value = fake_et.astimezone(timezone.utc)
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        result = mod.fetch_market_snapshot("SPY", broker)

    assert result is None, (
        "Missing quote high/low with no bars must return None (RETRY_LATER), not a snapshot"
    )


# ── AC2: Intraday bars exist → high/low from bars, direction-correct ──────────

def test_ac2_bars_compute_session_high_low():
    """
    PR #181: session high/low now come from Polygon day.h / day.l (daily bar),
    not from computing max/min over 1-minute timesales bars.
    This test verifies the same invariant — PUT/CALL invalidation logic —
    using the Polygon snapshot mock.
    """
    mod = _import_validator()

    r = MagicMock()
    r.status_code = 200
    r.json.return_value = {
        "ticker": {
            "day": {"h": 474.0, "l": 468.0, "c": 473.0},
            "lastTrade": {"p": 473.0},
        }
    }
    with patch.object(mod, "POLYGON_API_KEY", "test-key"), \
         patch.object(mod, "requests") as mock_requests:
        mock_requests.get.return_value = r
        snap = mod.fetch_market_snapshot("SPY", broker=None)

    assert snap is not None
    assert snap.session_high_so_far == 474.0
    assert snap.session_low_so_far  == 468.0
    assert snap.last_price          == 473.0

    # PUT: session_high=474 < prior_high=475 → VALID
    v_put = mod.validate_overnight_daily_signal(
        ticker="SPY", side="PUT",
        prior_day_high=475.0, prior_day_low=460.0, snapshot=snap
    )
    assert v_put.valid, "PUT must be valid when session_high < prior_day_high"

    # PUT: session_high=474 > prior_high=470 → INVALIDATED
    v_put_bad = mod.validate_overnight_daily_signal(
        ticker="SPY", side="PUT",
        prior_day_high=470.0, prior_day_low=460.0, snapshot=snap
    )
    assert not v_put_bad.valid
    assert "PRIOR_HIGH_BREACHED" in v_put_bad.reason_code

    # CALL: session_low=468 > prior_low=465 → VALID
    v_call = mod.validate_overnight_daily_signal(
        ticker="SPY", side="CALL",
        prior_day_high=480.0, prior_day_low=465.0, snapshot=snap
    )
    assert v_call.valid, "CALL must be valid when session_low > prior_day_low"

    # CALL: session_low=468 < prior_low=470 → INVALIDATED
    v_call_bad = mod.validate_overnight_daily_signal(
        ticker="SPY", side="CALL",
        prior_day_high=480.0, prior_day_low=470.0, snapshot=snap
    )
    assert not v_call_bad.valid
    assert "PRIOR_LOW_BREACHED" in v_call_bad.reason_code


# ── AC3: Before 09:30 ET, no bars → RETRY_LATER, no rejection ────────────────

def test_ac3_before_open_no_bars_retry_later():
    """Before 09:30 ET with no bars: return None, not a hard invalidation."""
    mod = _import_validator()
    broker = _broker_stub(bars=None)

    fake_et = _et_now(9, 15)  # 9:15 AM ET
    with patch("datetime.datetime") as mock_dt:
        mock_dt.now.return_value = fake_et.astimezone(timezone.utc)
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        snap = mod.fetch_market_snapshot("SPY", broker)

    assert snap is None, "Before open with no bars must return None (RETRY_LATER)"

    # The ValidationResult must be SNAPSHOT_UNAVAILABLE (RETRY_LATER in caller)
    v = mod.validate_overnight_daily_signal(
        ticker="SPY", side="CALL",
        prior_day_high=480.0, prior_day_low=465.0, snapshot=None
    )
    assert "SNAPSHOT_UNAVAILABLE" in v.reason_code, (
        "None snapshot must produce SNAPSHOT_UNAVAILABLE, not a different rejection"
    )


# ── AC4: After 09:30 ET, no bars → RETRY_LATER, no hard rejection ─────────────

def test_ac4_after_open_no_bars_retry_later():
    """After 09:30 ET with unavailable bars: return None, not hard rejection."""
    mod = _import_validator()
    broker = _broker_stub(bars=None)

    fake_et = _et_now(9, 45)  # 9:45 AM ET
    with patch("datetime.datetime") as mock_dt:
        mock_dt.now.return_value = fake_et.astimezone(timezone.utc)
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        snap = mod.fetch_market_snapshot("SPY", broker)

    assert snap is None, "After open with no bars must return None (RETRY_LATER)"

    v = mod.validate_overnight_daily_signal(
        ticker="SPY", side="PUT",
        prior_day_high=480.0, prior_day_low=465.0, snapshot=None
    )
    assert "SNAPSHOT_UNAVAILABLE" in v.reason_code


# ── AC5: Premarket bars — default OFF, RETRY_LATER, no invalidation ───────────

@pytest.mark.skip(
    reason="PR #181: OVERNIGHT_PREMARKET_HILO_ENABLED logic removed — "
           "Polygon day.h/day.l are regular-session values; no separate premarket gate needed."
)
def test_ac5_premarket_bars_default_no_invalidation():
    """
    Premarket bars exist but regular session (09:30) has not started.
    Default: OVERNIGHT_PREMARKET_HILO_ENABLED=false.
    The snapshot computed from premarket bars with session_filter=open
    should either return None (Tradier filters them) or the test confirms
    no hard invalidation occurs.
    Source: OVERNIGHT_PREMARKET_HILO_ENABLED flag is present and defaults false.
    """
    src = (_REPO / "ap" / "overnight_daily_validator.py").read_text()
    assert "OVERNIGHT_PREMARKET_HILO_ENABLED" in src
    # Confirm default is false/off
    assert '"false"' in src or "false" in src.lower()
    # The Tradier call uses session_filter=open which excludes premarket
    assert "session_filter" in src
    assert '"open"' in src


# ── AC6: Prior-day high/low calculation unchanged ────────────────────────────

def test_ac6_prior_day_calc_unchanged():
    """
    validate_overnight_daily_signal logic is unchanged.
    Prior-day levels come from signal/broker, not from fetch_market_snapshot.
    """
    mod = _import_validator()
    # Build a snapshot with known values
    snap = mod.MarketSnapshot(
        ticker="SPY",
        session_high_so_far=472.0,
        session_low_so_far=468.0,
        last_price=471.0,
        fetched_at="2026-06-10T14:00:00+00:00",
    )
    # PUT: not invalidated when session_high < prior_day_high
    v = mod.validate_overnight_daily_signal(
        ticker="SPY", side="PUT",
        prior_day_high=475.0, prior_day_low=460.0, snapshot=snap
    )
    assert v.valid
    assert v.prior_high == 475.0
    assert v.prior_low  == 460.0

    # CALL: not invalidated when session_low > prior_day_low
    v2 = mod.validate_overnight_daily_signal(
        ticker="SPY", side="CALL",
        prior_day_high=480.0, prior_day_low=465.0, snapshot=snap
    )
    assert v2.valid
    assert v2.prior_high == 480.0
    assert v2.prior_low  == 465.0


# =============================================================================
# PR #111 amend — live market-data URL regression tests
#
# Required:
#   1. When broker has no base_url / cfg.base_url, _fetch_intraday_bars()
#      calls https://api.tradier.com/v1/markets/timesales (not sandbox).
#   2. Paper/sandbox runtime broker objects still use live market-data URL
#      for overnight validation calls.
# =============================================================================

def _load_v2():
    """Load the amended overnight_daily_validator from the patched local copy."""
    import importlib.util, sys
    from pathlib import Path
    # Prefer the local patched file written by the patch script.
    patched = Path("/home/claude/overnight_daily_validator_v2.py")
    if not patched.exists():
        # Fall back to the repo path (running in CI on the branch)
        patched = Path(__file__).resolve().parents[1] / "ap" / "overnight_daily_validator.py"
    spec = importlib.util.spec_from_file_location("ap.overnight_daily_validator_v2", patched)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.mark.skip(
    reason="PR #181: _fetch_intraday_bars / Tradier timesales removed from "
           "fetch_market_snapshot. URL routing tests superseded by Polygon. "
           "_resolve_market_data_base_url still exists but is no longer called "
           "in the snapshot path. See test_pr181_overnight_polygon_daily.py."
)
class TestLiveMarketDataURL:
    """Spec: overnight validation must never call sandbox.tradier.com."""

    def _broker_no_base_url(self, captured_urls):
        """Broker stub with NO base_url, market_data_base_url, or cfg.
        Captures URLs called via broker.session.get."""
        import unittest.mock as mock

        broker = mock.MagicMock()
        # Explicitly unset every attribute the resolver checks
        del broker.market_data_base_url
        del broker.quote_base_url
        del broker.cfg
        # base_url intentionally absent / None
        broker.base_url = None

        def _get(url, **kw):
            captured_urls.append(url)
            resp = mock.MagicMock()
            resp.status_code = 200
            # Return empty series / quote so function exits cleanly
            if "timesales" in url:
                resp.json.return_value = {"series": None}
            else:
                resp.json.return_value = {"quotes": {"quote": {}}}
            return resp

        broker.session.get.side_effect = _get
        return broker

    def _broker_paper_sandbox(self, captured_urls):
        """Broker stub that looks like a paper/sandbox execution broker.
        base_url is sandbox — this is intentionally a paper broker.
        The resolver must NOT propagate this URL to market-data calls.
        """
        import unittest.mock as mock

        broker = mock.MagicMock()
        # Paper broker has sandbox base_url for ORDER SUBMISSION
        broker.base_url = "https://sandbox.tradier.com"
        # But has NO market_data_base_url / quote_base_url set
        del broker.market_data_base_url
        del broker.quote_base_url
        del broker.cfg

        def _get(url, **kw):
            captured_urls.append(url)
            resp = mock.MagicMock()
            resp.status_code = 200
            if "timesales" in url:
                resp.json.return_value = {"series": None}
            else:
                resp.json.return_value = {"quotes": {"quote": {}}}
            return resp

        broker.session.get.side_effect = _get
        return broker

    def test_no_base_url_broker_uses_api_tradier_com(self):
        """
        PR #181: _fetch_intraday_bars (Tradier timesales) is removed from
        fetch_market_snapshot.  The URL resolver still exists for historical
        reference but is no longer called in the snapshot path.

        This test now only verifies the resolver itself still returns the
        live Tradier URL — the timesales call assertion is superseded by
        test_pr181_overnight_polygon_daily.py which proves Polygon is used.
        """
        mod = _load_v2()
        import unittest.mock as mock
        broker = mock.MagicMock()
        del broker.market_data_base_url
        del broker.quote_base_url
        del broker.cfg
        broker.base_url = None

        # Resolver still returns live endpoint — kept for audit
        resolved = mod._resolve_market_data_base_url(broker)
        assert resolved == "https://api.tradier.com", (
            f"Expected https://api.tradier.com, got {resolved!r}"
        )
        # _fetch_intraday_bars no longer exists on this module (PR #181)
        assert not hasattr(mod, "_fetch_intraday_bars"), (
            "_fetch_intraday_bars must be removed after PR #181"
        )

    def test_paper_sandbox_broker_still_uses_live_market_data_url(self):
        """
        PR #181: The root cause of HTTP 401 was that paper accounts'
        sandbox token was sent to api.tradier.com timesales.
        Fixed by switching to Polygon (no broker token needed).

        This test still verifies _resolve_market_data_base_url behavior
        but no longer asserts timesales was called (it isn't anymore).
        """
        mod = _load_v2()
        import unittest.mock as mock
        broker = mock.MagicMock()
        broker.base_url = "https://sandbox.tradier.com"
        del broker.market_data_base_url
        del broker.quote_base_url
        del broker.cfg

        resolved = mod._resolve_market_data_base_url(broker)
        assert resolved == "https://api.tradier.com", (
            f"Paper broker should resolve to api.tradier.com, got {resolved!r}"
        )
        assert "sandbox" not in resolved
        # timesales is gone; fetch_market_snapshot now uses Polygon.
        # See test_pr181_overnight_polygon_daily.py for the paper-account proof.

    def test_broker_with_market_data_base_url_uses_it(self):
        """broker.market_data_base_url is respected (priority 1)."""
        mod = _load_v2()
        import unittest.mock as mock
        broker = mock.MagicMock()
        broker.market_data_base_url = "https://api.tradier.com"
        del broker.quote_base_url
        del broker.cfg
        resolved = mod._resolve_market_data_base_url(broker)
        assert resolved == "https://api.tradier.com"

    def test_broker_with_quote_base_url_uses_it(self):
        """broker.quote_base_url is respected (priority 2, PR #118 pattern)."""
        mod = _load_v2()
        import unittest.mock as mock
        broker = mock.MagicMock()
        del broker.market_data_base_url
        broker.quote_base_url = "https://api.tradier.com"
        del broker.cfg
        resolved = mod._resolve_market_data_base_url(broker)
        assert resolved == "https://api.tradier.com"

    def test_env_tradier_data_base_url_respected(self, monkeypatch):
        """env TRADIER_DATA_BASE_URL is used when broker has no explicit URL."""
        mod = _load_v2()
        import unittest.mock as mock
        monkeypatch.setenv("TRADIER_DATA_BASE_URL", "https://api.tradier.com")
        broker = mock.MagicMock()
        del broker.market_data_base_url
        del broker.quote_base_url
        del broker.cfg
        broker.base_url = None
        resolved = mod._resolve_market_data_base_url(broker)
        assert resolved == "https://api.tradier.com"
        assert "sandbox" not in resolved

    def test_source_has_no_sandbox_fallback_string(self):
        """Source-level: https://sandbox.tradier.com must not appear as a URL
        in the production code (only comments allowed)."""
        from pathlib import Path
        patched = Path("/home/claude/overnight_daily_validator_v2.py")
        if not patched.exists():
            patched = Path(__file__).resolve().parents[1] / "ap" / "overnight_daily_validator.py"
        src = patched.read_text()
        # Count actual string literals with sandbox URL
        import ast
        tree = ast.parse(src)
        sandbox_literals = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                if "sandbox.tradier.com" in node.value:
                    sandbox_literals.append(node.value)
        assert len(sandbox_literals) == 0, (
            f"Found sandbox.tradier.com in string literals: {sandbox_literals}"
        )


# =============================================================================
# PR #111 amend P1 — sandbox URL sanitisation inside the resolver
#
# Spec: if broker.market_data_base_url, broker.quote_base_url,
# broker.cfg.market_data_base_url, TRADIER_MARKET_DATA_BASE_URL, or
# TRADIER_DATA_BASE_URL are set to https://sandbox.tradier.com, the resolver
# must skip that value (with a warning) and fall through to the next candidate,
# ultimately returning https://api.tradier.com.
# =============================================================================

class TestResolverSandboxSanitisation:
    """Spec: resolver must never return sandbox.tradier.com regardless of source."""

    def _load(self):
        """Load the patched module from local file or repo path."""
        import importlib.util, sys
        from pathlib import Path
        patched = Path("/home/claude/overnight_daily_validator_v3.py")
        if not patched.exists():
            patched = Path(__file__).resolve().parents[1] / "ap" / "overnight_daily_validator.py"
        key = f"ap.overnight_daily_validator_{patched.stat().st_mtime}"
        if key in sys.modules:
            return sys.modules[key]
        spec = importlib.util.spec_from_file_location(key, patched)
        mod  = importlib.util.module_from_spec(spec)
        sys.modules[key] = mod
        spec.loader.exec_module(mod)
        return mod

    def _bare_broker(self, **attrs):
        """Broker with only the attrs given; all others raise AttributeError."""
        from unittest.mock import MagicMock
        b = MagicMock(spec=list(attrs.keys()))
        for k, v in attrs.items():
            setattr(b, k, v)
        return b

    # Required test 1: broker.market_data_base_url = sandbox → api.tradier.com
    def test_broker_market_data_base_url_sandbox_rejected(self, caplog):
        """Spec req 6a: broker.market_data_base_url=sandbox → resolver falls through."""
        import logging
        mod = self._load()
        b = self._bare_broker(market_data_base_url="https://sandbox.tradier.com")
        with caplog.at_level(logging.WARNING, logger="ap.overnight_daily_validator"):
            result = mod._resolve_market_data_base_url(b)
        assert result == "https://api.tradier.com", (
            f"Expected api.tradier.com, got {result!r}"
        )
        assert "OVERNIGHT_MARKET_DATA_SANDBOX_URL_IGNORED" in caplog.text
        assert "broker.market_data_base_url" in caplog.text

    # Required test 2: broker.quote_base_url = sandbox → api.tradier.com
    def test_broker_quote_base_url_sandbox_rejected(self, caplog):
        """Spec req 6b: broker.quote_base_url=sandbox → resolver falls through."""
        import logging
        mod = self._load()
        b = self._bare_broker(quote_base_url="https://sandbox.tradier.com")
        with caplog.at_level(logging.WARNING, logger="ap.overnight_daily_validator"):
            result = mod._resolve_market_data_base_url(b)
        assert result == "https://api.tradier.com"
        assert "OVERNIGHT_MARKET_DATA_SANDBOX_URL_IGNORED" in caplog.text
        assert "broker.quote_base_url" in caplog.text

    # Required test 3: env TRADIER_MARKET_DATA_BASE_URL = sandbox → api.tradier.com
    def test_env_tradier_market_data_base_url_sandbox_rejected(self, caplog, monkeypatch):
        """Spec req 6c: env var=sandbox → resolver falls through to hard-coded live URL."""
        import logging
        mod = self._load()
        monkeypatch.setenv("TRADIER_MARKET_DATA_BASE_URL", "https://sandbox.tradier.com")
        b = self._bare_broker()   # no broker attrs at all
        with caplog.at_level(logging.WARNING, logger="ap.overnight_daily_validator"):
            result = mod._resolve_market_data_base_url(b)
        assert result == "https://api.tradier.com"
        assert "OVERNIGHT_MARKET_DATA_SANDBOX_URL_IGNORED" in caplog.text
        assert "env.TRADIER_MARKET_DATA_BASE_URL" in caplog.text

    # Bonus: TRADIER_DATA_BASE_URL sandbox also rejected
    def test_env_tradier_data_base_url_sandbox_rejected(self, caplog, monkeypatch):
        import logging
        mod = self._load()
        monkeypatch.setenv("TRADIER_DATA_BASE_URL", "https://sandbox.tradier.com")
        b = self._bare_broker()
        with caplog.at_level(logging.WARNING, logger="ap.overnight_daily_validator"):
            result = mod._resolve_market_data_base_url(b)
        assert result == "https://api.tradier.com"
        assert "OVERNIGHT_MARKET_DATA_SANDBOX_URL_IGNORED" in caplog.text

    # Bonus: live value accepted without warning
    def test_live_url_accepted_without_warning(self, caplog):
        import logging
        mod = self._load()
        b = self._bare_broker(market_data_base_url="https://api.tradier.com")
        with caplog.at_level(logging.WARNING, logger="ap.overnight_daily_validator"):
            result = mod._resolve_market_data_base_url(b)
        assert result == "https://api.tradier.com"
        assert "OVERNIGHT_MARKET_DATA_SANDBOX_URL_IGNORED" not in caplog.text

    # All sources sandbox → still falls back to hard-coded live URL
    def test_all_sources_sandbox_falls_back_to_live(self, caplog, monkeypatch):
        """If every configured source is sandbox, hard-coded live fallback is returned."""
        import logging
        mod = self._load()
        monkeypatch.setenv("TRADIER_MARKET_DATA_BASE_URL", "https://sandbox.tradier.com")
        monkeypatch.setenv("TRADIER_DATA_BASE_URL",        "https://sandbox.tradier.com")
        b = self._bare_broker(
            market_data_base_url="https://sandbox.tradier.com",
            quote_base_url="https://sandbox.tradier.com",
        )
        with caplog.at_level(logging.WARNING, logger="ap.overnight_daily_validator"):
            result = mod._resolve_market_data_base_url(b)
        assert result == "https://api.tradier.com"
        # All four sources should have been warned about
        assert caplog.text.count("OVERNIGHT_MARKET_DATA_SANDBOX_URL_IGNORED") >= 4

    # Source-level: warning key present in file
    def test_source_contains_sandbox_ignored_warning_key(self):
        from pathlib import Path
        patched = Path("/home/claude/overnight_daily_validator_v3.py")
        if not patched.exists():
            patched = Path(__file__).resolve().parents[1] / "ap" / "overnight_daily_validator.py"
        src = patched.read_text()
        assert "OVERNIGHT_MARKET_DATA_SANDBOX_URL_IGNORED" in src
        assert '"sandbox.tradier.com" in clean.lower()' in src
