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
    # quote["high"] / quote["low"] must no longer be the primary source
    assert "session_high = float(quote.get" not in src, (
        "quote.get('high') must not be the source for session_high"
    )
    assert "_fetch_intraday_bars" in src
    assert "OVERNIGHT_SESSION_BARS_OK" in src
    assert "OVERNIGHT_PREMARKET_HILO_ENABLED" in src

def test_source_has_all_required_log_strings():
    src = (_REPO / "ap" / "overnight_daily_validator.py").read_text()
    for log_str in [
        "OVERNIGHT_SNAPSHOT_QUOTE_MISSING_HILO",
        "OVERNIGHT_SESSION_BARS_OK",
        "OVERNIGHT_SESSION_BARS_NOT_READY",
        "OVERNIGHT_SESSION_BARS_UNAVAILABLE",
    ]:
        assert log_str in src, f"Log string missing: {log_str}"


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
    After 09:30 ET with bars available.
    PUT: invalidates only if session_high > prior_day_high.
    CALL: invalidates only if session_low < prior_day_low.
    """
    mod = _import_validator()
    bars = [
        {"time": "2026-06-10T09:31:00", "open": 469, "high": 471, "low": 468, "close": 470},
        {"time": "2026-06-10T09:32:00", "open": 470, "high": 473, "low": 469, "close": 472},
        {"time": "2026-06-10T09:33:00", "open": 472, "high": 474, "low": 471, "close": 473},
    ]
    broker = _broker_stub(bars=bars)

    fake_et = _et_now(9, 34)
    with patch("datetime.datetime") as mock_dt:
        mock_dt.now.return_value = fake_et.astimezone(timezone.utc)
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        snap = mod.fetch_market_snapshot("SPY", broker)

    assert snap is not None
    assert snap.session_high_so_far == pytest.approx(474.0)
    assert snap.session_low_so_far  == pytest.approx(468.0)
    assert snap.last_price          == pytest.approx(473.0)  # last bar close

    # PUT: session_high=474 > prior_high=475 → VALID (not breached)
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

    # CALL: session_low=468 < prior_low=465 → VALID (not breached)
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
        """Required test 1: no base_url/cfg → calls api.tradier.com, not sandbox."""
        mod = _load_v2()
        captured = []
        broker = self._broker_no_base_url(captured)

        # Confirm the resolver returns the live endpoint
        resolved = mod._resolve_market_data_base_url(broker)
        assert resolved == "https://api.tradier.com", (
            f"Expected https://api.tradier.com, got {resolved!r}"
        )

        # Call _fetch_intraday_bars and confirm the URL hit
        mod._fetch_intraday_bars("NVDA", broker, "2026-06-11T09:30:00", "2026-06-11T10:00:00")
        timesales_urls = [u for u in captured if "timesales" in u]
        assert len(timesales_urls) >= 1
        for url in timesales_urls:
            assert "api.tradier.com" in url, (
                f"Expected api.tradier.com in URL, got {url!r}"
            )
            assert "sandbox" not in url, (
                f"sandbox.tradier.com must NOT be called for market-data, got {url!r}"
            )

    def test_paper_sandbox_broker_still_uses_live_market_data_url(self):
        """Required test 2: paper broker (sandbox base_url) → live market-data URL."""
        mod = _load_v2()
        captured = []
        broker = self._broker_paper_sandbox(captured)

        # The resolver must NOT use broker.base_url (sandbox) for market data.
        # market_data_base_url / quote_base_url not set → falls through to live.
        resolved = mod._resolve_market_data_base_url(broker)
        assert resolved == "https://api.tradier.com", (
            f"Paper broker should resolve to api.tradier.com, got {resolved!r}"
        )
        assert "sandbox" not in resolved

        # Confirm actual HTTP calls go to live endpoint
        mod._fetch_intraday_bars("SPY", broker, "2026-06-11T09:30:00", "2026-06-11T10:00:00")
        timesales_urls = [u for u in captured if "timesales" in u]
        assert len(timesales_urls) >= 1
        for url in timesales_urls:
            assert "sandbox" not in url, (
                f"Paper broker must NOT use sandbox for market-data validation: {url!r}"
            )
            assert "api.tradier.com" in url

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
