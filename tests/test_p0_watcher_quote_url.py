"""
tests/test_p0_watcher_quote_url.py
P0: watcher quote URL must be api.tradier.com for both paper and live clients.
"""
import os, re, sys, types, threading, pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from ap.live_submit_gates import (
    MarketDataTransportConfigurationError,
)

_REPO  = Path(__file__).resolve().parents[1]
EW_SRC = (_REPO / "ap_entry_watcher.py").read_text()
APP_SRC = (_REPO / "app.py").read_text()


# ── Source checks ─────────────────────────────────────────────────────────────

def test_resolve_watcher_quote_url_defined():
    assert "def _resolve_watcher_quote_url(" in EW_SRC

def test_fetch_quotes_no_sandbox_fallback():
    idx = EW_SRC.find("def _fetch_quotes(")
    end = EW_SRC.find("\n    def ", idx + 1)
    body = EW_SRC[idx:end]
    assert "sandbox.tradier.com" not in body

def test_fetch_quotes_uses_resolve_helper():
    idx = EW_SRC.find("def _fetch_quotes(")
    end = EW_SRC.find("\n    def ", idx + 1)
    body = EW_SRC[idx:end]
    assert "_resolve_watcher_quote_transport()" in body

def test_watcher_quote_identity_uses_resolve_helper():
    idx = EW_SRC.find("def _watcher_quote_identity(")
    end = EW_SRC.find("\n    def ", idx + 1)
    body = EW_SRC[idx:end]
    assert "_current_watcher_quote_proof()" in body

def test_watcher_uses_shared_strict_transport_validator():
    """PR #391 amendment (Correction 4): the watcher's transport resolver
    must delegate to require_proven_market_data_transport and reference
    CANONICAL_TRADIER_MARKET_DATA_BASE_URL. The prior sandbox-substring
    guard that silently rewrote URLs to live has been removed."""
    idx = EW_SRC.find(
        "def _resolve_watcher_quote_transport("
    )
    end = EW_SRC.find("\n    def ", idx + 1)
    body = EW_SRC[idx:end]

    assert "require_proven_market_data_transport" in body
    assert (
        "CANONICAL_TRADIER_MARKET_DATA_BASE_URL"
        in body
    )
    assert (
        "WATCHER_QUOTE_URL_SANDBOX_GUARD_TRIGGERED"
        not in body
    )
    assert "Forcing https://api.tradier.com" not in body

def test_paper_fail_closed_present():
    idx = EW_SRC.find("def _fetch_quotes(")
    end = EW_SRC.find("\n    def ", idx + 1)
    body = EW_SRC[idx:end]
    assert "PAPER_WATCHER_NO_MARKET_DATA_TOKEN" in body
    assert "raise RuntimeError" in body

def test_preflight_failure_message_present():
    idx = EW_SRC.find("def _validate_market_data_preflight(")
    end = EW_SRC.find("\n    def ", idx + 1)
    body = EW_SRC[idx:end]
    assert "PAPER_WATCHER_NO_MARKET_DATA_TOKEN" in body
    assert "TRADIER_MARKET_DATA_TOKEN or TRADIER_DATA_TOKEN" in body

def test_resolve_helper_delegates_to_strict_validator():
    """PR #391 amendment: proof of shared authority. The resolver must
    import both CANONICAL_TRADIER_MARKET_DATA_BASE_URL and
    require_proven_market_data_transport. No sandbox rewrite is allowed."""
    idx = EW_SRC.find("def _resolve_watcher_quote_transport(")
    end = EW_SRC.find("\n    def ", idx + 1)
    body = EW_SRC[idx:end]
    assert "require_proven_market_data_transport" in body
    assert "CANONICAL_TRADIER_MARKET_DATA_BASE_URL" in body
    assert "Forcing https://api.tradier.com" not in body

def test_audit_fields_still_present():
    for field in [
        "watcher_quote_source",
        "watcher_quote_base_url",
        "watcher_sandbox_mode",
        "watcher_quote_token_source",
        "quote_fetch_status",
    ]:
        assert field in EW_SRC, f"watcher_audit field missing: {field}"


def test_paper_order_route_still_uses_tradier_base_url():
    idx = APP_SRC.find("def build_broker()")
    end = APP_SRC.find("\ndef ", idx + 1)
    body = APP_SRC[idx:end]
    assert 'os.getenv("TRADIER_BASE_URL", "https://sandbox.tradier.com")' in body
    assert "TRADIER_MARKET_DATA_BASE_URL" not in body


# ── Behavioral: stub APEntryWatcher ──────────────────────────────────────────

def _load_watcher_class():
    """
    Load ap_entry_watcher without its heavy runtime dependencies.
    Returns (APEntryWatcher class, module) or (None, None).
    """
    # Stub every import the module needs so it can load
    _stubs = [
        "ap.db", "ap.broker", "ap.broker_factory", "ap.config", "ap.state",
        "ap.order_state_machine", "ap.notify", "ap.models", "ap.utils",
        "ap.reconcile", "ap.queue", "supabase", "psycopg2",
        "ap.overnight_daily_validator", "ap.overnight_daily_validator",
        "ap.position_manager", "ap.risk", "ap.signal_store",
    ]
    for stub in _stubs:
        if stub not in sys.modules:
            m = types.ModuleType(stub)
            # Provide common attributes modules expect
            m.conn = MagicMock()
            m.run_with_retry = lambda f: f()
            sys.modules[stub] = m

    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "ap_entry_watcher_test", _REPO / "ap_entry_watcher.py"
    )
    mod = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)
    except Exception:
        return None, None
    cls = getattr(mod, "APEntryWatcher", None)
    return cls, mod


def _make_watcher(mode="PAPER", broker_base="https://sandbox.tradier.com",
                  live_access_token=None):
    """Build a minimal APEntryWatcher stub for unit testing resolve/fetch."""
    cls, mod = _load_watcher_class()
    if cls is None:
        return None

    broker = MagicMock()
    broker.base_url = broker_base
    broker.access_token = "sandbox_exec_tok"
    broker.live_access_token = live_access_token
    broker.cfg = MagicMock()
    broker.cfg.base_url = broker_base
    broker.cfg.live_access_token = live_access_token
    broker.session = MagicMock()

    try:
        w = cls.__new__(cls)
        w.broker = broker
        w.mode   = mode.upper()
        w._lock  = threading.Lock()
        w._pending = []
        w._running = False
        w._thread = None
        w.on_trigger = None
        w.require_on_trigger = True
        w.order_state_machine = None
        return w
    except Exception:
        return None


# ── Test 1 (rewritten): sandbox / hostile env → rejected before token use ────


@pytest.mark.parametrize(
    "configured_url",
    [
        "https://api.tradier.com",
        "https://api.tradier.com/",
        "https://api.tradier.com/v1",
        "https://api.tradier.com/v1/",
        "https://api.tradier.com:443",
    ],
)
def test_watcher_normalizes_accepted_urls_to_canonical_origin(configured_url):
    """PR #391 amendment: every accepted operator variant normalizes to
    the canonical origin. TradierBroker concatenates base_url + '/v1/...',
    so returning '/v1' or a trailing slash would produce '/v1/v1/...' at
    request time. There is exactly one accepted output form."""
    w = _make_watcher(
        mode="PAPER",
        broker_base="https://sandbox.tradier.com",
    )
    if w is None:
        pytest.skip("APEntryWatcher not importable in test env")

    with patch.dict(
        os.environ,
        {
            "TRADIER_MARKET_DATA_TOKEN": "live_data_tok",
            "TRADIER_MARKET_DATA_BASE_URL": configured_url,
        },
        clear=True,
    ):
        transport = w._resolve_watcher_quote_transport()

    assert transport["watcher_quote_base_url"] == "https://api.tradier.com"
    assert transport["watcher_sandbox_mode"] is False


@pytest.mark.parametrize(
    "bad_url",
    [
        "https://sandbox.tradier.com",
        "https://evil.example",
        "https://api.tradier.com.evil.example",
        "http://api.tradier.com",
        "ftp://api.tradier.com",
        "https://user:pass@api.tradier.com",
        "https://api.tradier.com:8443",
        "https://api.tradier.com?redirect=x",
        "https://api.tradier.com#fragment",
        "https://api.tradier.com/v2",
        "https://api.tradier.com/foo",
        "https://api.tradier.com/v1/markets",
        "https://api.tradier.com//evil",
    ],
)
def test_watcher_rejects_untrusted_url_before_token_bearing_request(bad_url):
    """PR #391 amendment: an invalid explicit env var must raise
    MarketDataTransportConfigurationError BEFORE any credential-bearing
    request. Neither requests.get nor broker.session.get may fire — the
    prior silent-rewrite behavior would have attached the live market-data
    bearer token to arbitrary hosts."""
    w = _make_watcher(
        mode="PAPER",
        broker_base="https://sandbox.tradier.com",
    )
    if w is None:
        pytest.skip("APEntryWatcher not importable in test env")

    with patch.dict(
        os.environ,
        {
            "TRADIER_MARKET_DATA_TOKEN": "live_data_tok",
            "TRADIER_MARKET_DATA_BASE_URL": bad_url,
        },
        clear=True,
    ), patch("requests.get") as requests_get:
        with pytest.raises(MarketDataTransportConfigurationError):
            w._fetch_quotes(["SPY"])

    requests_get.assert_not_called()
    w.broker.session.get.assert_not_called()


# ── Test 2: PAPER + no token → hard failure (fail closed) ────────────────────

def test_paper_no_token_raises_hard_failure():
    """
    PAPER client with no market-data token must fail explicitly,
    not fall back to broker.session with sandbox credentials.
    """
    w = _make_watcher(mode="PAPER", broker_base="https://sandbox.tradier.com")
    if w is None:
        pytest.skip("APEntryWatcher not importable in test env")
    env_clean = {k: v for k, v in os.environ.items()
                 if k not in ("TRADIER_MARKET_DATA_TOKEN","TRADIER_DATA_TOKEN",
                              "TRADIER_MARKET_DATA_BASE_URL","TRADIER_DATA_BASE_URL")}
    with patch.dict(os.environ, env_clean, clear=True):
        with pytest.raises(RuntimeError, match="PAPER_WATCHER_NO_MARKET_DATA_TOKEN"):
            w._fetch_quotes(["SPY"])
    # broker.session.get must NOT have been called
    w.broker.session.get.assert_not_called()
    proof = w._current_watcher_quote_proof()
    assert proof["watcher_quote_base_url"] == "https://api.tradier.com"
    assert proof["watcher_sandbox_mode"] is False
    assert proof["quote_fetch_status"] == "market_data_token_missing"


def test_paper_no_token_start_fails_preflight_clearly():
    w = _make_watcher(mode="PAPER", broker_base="https://sandbox.tradier.com")
    if w is None:
        pytest.skip("APEntryWatcher not importable in test env")
    w.on_trigger = MagicMock()
    env_clean = {
        k: v for k, v in os.environ.items()
        if k not in (
            "TRADIER_MARKET_DATA_TOKEN",
            "TRADIER_DATA_TOKEN",
            "TRADIER_MARKET_DATA_BASE_URL",
            "TRADIER_DATA_BASE_URL",
        )
    }
    with patch.dict(os.environ, env_clean, clear=True), \
         patch("ap_health_registry.HEALTH.ensure_registered") as ensure_registered, \
         patch("ap_health_registry.HEALTH.set_status") as set_status:
        with pytest.raises(RuntimeError, match="PAPER_WATCHER_NO_MARKET_DATA_TOKEN") as exc:
            w.start()
    assert "TRADIER_MARKET_DATA_TOKEN or TRADIER_DATA_TOKEN" in str(exc.value)
    ensure_registered.assert_called_once()
    set_status.assert_called_once()
    assert w._running is False


# ── Test 3: PAPER + market-data token → uses api.tradier.com ─────────────────

def test_paper_with_token_uses_live_url():
    """
    PAPER + TRADIER_MARKET_DATA_TOKEN → quote URL is api.tradier.com,
    watcher_audit shows tradier_live / sandbox_mode=False.
    """
    w = _make_watcher(mode="PAPER", broker_base="https://sandbox.tradier.com")
    if w is None:
        pytest.skip("APEntryWatcher not importable in test env")
    with patch.dict(os.environ, {
        "TRADIER_MARKET_DATA_TOKEN": "live_data_tok",
    }):
        os.environ.pop("TRADIER_MARKET_DATA_BASE_URL", None)
        base_url, token = w._resolve_watcher_quote_url()
        identity = w._watcher_quote_identity()
    assert "api.tradier.com" in base_url
    assert token == "live_data_tok"
    assert identity["watcher_sandbox_mode"] is False
    assert identity["watcher_quote_source"] == "tradier_live"
    assert "api.tradier.com" in identity["watcher_quote_base_url"]


def test_paper_with_token_passes_preflight():
    w = _make_watcher(mode="PAPER", broker_base="https://sandbox.tradier.com")
    if w is None:
        pytest.skip("APEntryWatcher not importable in test env")
    with patch.dict(os.environ, {"TRADIER_MARKET_DATA_TOKEN": "live_data_tok"}, clear=True):
        w._validate_market_data_preflight()


def test_paper_with_allowed_live_credential_passes_preflight():
    w = _make_watcher(
        mode="PAPER",
        broker_base="https://sandbox.tradier.com",
        live_access_token="allowed_live_tok",
    )
    if w is None:
        pytest.skip("APEntryWatcher not importable in test env")
    with patch.dict(os.environ, {}, clear=True):
        w._validate_market_data_preflight()
        proof = w._current_watcher_quote_proof()
    assert proof["watcher_quote_token_source"] == "broker.live_access_token"


# ── Test 4: LIVE + no token → broker.session fallback (acceptable) ───────────

def test_live_no_token_uses_broker_session():
    """
    LIVE client with no market-data token may fall back to broker.session.
    The execution token for live clients typically has market-data access.
    broker.session.get must be called (not empty return).
    """
    w = _make_watcher(mode="LIVE", broker_base="https://api.tradier.com")
    if w is None:
        pytest.skip("APEntryWatcher not importable in test env")

    mock_resp = MagicMock()
    mock_resp.json.return_value = {"quotes": {"quote": [{"symbol": "SPY", "last": 550.0}]}}
    w.broker.session.get.return_value = mock_resp

    env_clean = {k: v for k, v in os.environ.items()
                 if k not in ("TRADIER_MARKET_DATA_TOKEN","TRADIER_DATA_TOKEN",
                              "TRADIER_MARKET_DATA_BASE_URL","TRADIER_DATA_BASE_URL")}
    with patch.dict(os.environ, env_clean, clear=True):
        result = w._fetch_quotes(["SPY"])

    w.broker.session.get.assert_called_once()
    call_url = w.broker.session.get.call_args[0][0]
    assert "api.tradier.com" in call_url, (
        f"LIVE broker.session fallback must use api.tradier.com, got {call_url!r}"
    )
    assert "sandbox" not in call_url.lower()


def test_fetch_quotes_success_populates_quote_age_ms():
    w = _make_watcher(mode="PAPER", broker_base="https://sandbox.tradier.com")
    if w is None:
        pytest.skip("APEntryWatcher not importable in test env")

    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "quotes": {"quote": [{"symbol": "SPY", "bid": 549.9, "ask": 550.1, "last": 550.0}]}
    }

    with patch.dict(os.environ, {"TRADIER_MARKET_DATA_TOKEN": "live_data_tok"}, clear=True), \
         patch("requests.get", return_value=mock_resp) as requests_get:
        result = w._fetch_quotes(["SPY"])

    requests_get.assert_called_once()
    assert result["SPY"]["quote_age_ms"] is not None
    assert isinstance(result["SPY"]["quote_age_ms"], int)
    assert result["SPY"]["quote_age_ms"] >= 0
    assert result["SPY"]["quote_fetch_status"] == "success"
    proof = w._current_watcher_quote_proof()
    assert proof["watcher_quote_token_source"] == "TRADIER_MARKET_DATA_TOKEN"
    assert proof["quote_fetch_status"] == "success"


# ── Test 5: watcher_audit fields match actual quote source ───────────────────

def test_watcher_audit_matches_quote_source():
    """_watcher_quote_identity uses _resolve_watcher_quote_url so audit = actual."""
    w = _make_watcher(mode="PAPER")
    if w is None:
        pytest.skip("APEntryWatcher not importable in test env")
    with patch.dict(os.environ, {"TRADIER_MARKET_DATA_TOKEN": "tok"}):
        os.environ.pop("TRADIER_MARKET_DATA_BASE_URL", None)
        base_url, _ = w._resolve_watcher_quote_url()
        identity    = w._watcher_quote_identity()
    assert identity["watcher_quote_base_url"] == base_url
    assert identity["watcher_sandbox_mode"] is False
    assert identity["watcher_quote_source"] == "tradier_live"


def test_watcher_audit_payload_includes_quote_token_source_and_fetch_status():
    w = _make_watcher(mode="PAPER")
    if w is None:
        pytest.skip("APEntryWatcher not importable in test env")

    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "quotes": {"quote": [{"symbol": "SPY", "bid": 549.9, "ask": 550.1, "last": 550.0}]}
    }
    watched = types.SimpleNamespace(
        ticker="SPY",
        score=91.0,
        grade="A",
        side="CALL",
        signal_id="sig-1",
        entry_trigger=550.0,
        stop_level=547.5,
        last_quote_bid=550.1,
        last_quote_ask=550.3,
        last_quote_age_ms=17,
        signal={"timeframe": "1d", "pattern": "breakout", "plan_id": "plan-1"},
    )

    with patch.dict(os.environ, {"TRADIER_MARKET_DATA_TOKEN": "live_data_tok"}, clear=True), \
         patch("requests.get", return_value=mock_resp):
        w._fetch_quotes(["SPY"])
        payload = w._build_watcher_audit_payload(
            watched,
            trigger_type="trigger",
            reason_code="trigger_ready",
            raw_reason="call_breach_confirmed",
        )

    assert payload["watcher_quote_source"] == "tradier_live"
    assert payload["watcher_sandbox_mode"] is False
    assert payload["watcher_quote_base_url"] == "https://api.tradier.com"
    assert payload["watcher_quote_token_source"] == "TRADIER_MARKET_DATA_TOKEN"
    assert payload["quote_fetch_status"] == "success"
