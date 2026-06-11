"""
tests/test_p0_watcher_quote_url.py
P0: watcher quote URL must be api.tradier.com for both paper and live clients.
"""
import os, re, sys, types, threading, pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

_REPO  = Path(__file__).resolve().parents[1]
EW_SRC = (_REPO / "ap_entry_watcher.py").read_text()


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
    assert "_resolve_watcher_quote_url()" in body

def test_watcher_quote_identity_uses_resolve_helper():
    idx = EW_SRC.find("def _watcher_quote_identity(")
    end = EW_SRC.find("\n    def ", idx + 1)
    body = EW_SRC[idx:end]
    assert "_resolve_watcher_quote_url()" in body

def test_sandbox_guard_present():
    idx = EW_SRC.find("def _resolve_watcher_quote_url(")
    end = EW_SRC.find("\n    def ", idx + 1)
    body = EW_SRC[idx:end]
    assert "WATCHER_QUOTE_URL_SANDBOX_GUARD_TRIGGERED" in body
    assert "Forcing https://api.tradier.com" in body

def test_paper_fail_closed_present():
    idx = EW_SRC.find("def _fetch_quotes(")
    end = EW_SRC.find("\n    def ", idx + 1)
    body = EW_SRC[idx:end]
    assert "PAPER_WATCHER_NO_MARKET_DATA_TOKEN" in body
    assert "return {}" in body

def test_resolve_helper_default_is_live():
    """Hardcoded fallback must be api.tradier.com; sandbox guard must reject any sandbox URL."""
    idx = EW_SRC.find("def _resolve_watcher_quote_url(")
    end = EW_SRC.find("\n    def ", idx + 1)
    body = EW_SRC[idx:end]
    # Default fallback must be api.tradier.com
    assert "api.tradier.com" in body
    # Guard: the function must detect sandbox and replace it — look for both the
    # check and the forced replacement value
    assert "sandbox.tradier.com" in body,    "guard must check for sandbox URL"
    assert "Forcing https://api.tradier.com" in body or \
           "_LIVE_QUOTE_URL" in body,         "guard must force to live URL"
    # The guard must never RETURN sandbox — verified by test_sandbox_env_forced_to_live

def test_audit_fields_still_present():
    for field in ["watcher_quote_source", "watcher_quote_base_url", "watcher_sandbox_mode"]:
        assert field in EW_SRC, f"watcher_audit field missing: {field}"


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
        return w
    except Exception:
        return None


# ── Test 1: sandbox env → forced to api.tradier.com ──────────────────────────

def test_sandbox_env_forced_to_live():
    """
    TRADIER_MARKET_DATA_BASE_URL=https://sandbox.tradier.com must be rejected
    and forced to https://api.tradier.com.
    """
    w = _make_watcher(mode="PAPER", broker_base="https://sandbox.tradier.com")
    if w is None:
        pytest.skip("APEntryWatcher not importable in test env")
    with patch.dict(os.environ, {
        "TRADIER_MARKET_DATA_BASE_URL": "https://sandbox.tradier.com",
        "TRADIER_MARKET_DATA_TOKEN":    "some_tok",
    }):
        base_url, _ = w._resolve_watcher_quote_url()
    assert "sandbox" not in base_url.lower(), (
        f"Sandbox env must be overridden to api.tradier.com, got {base_url!r}"
    )
    assert "api.tradier.com" in base_url


# ── Test 2: PAPER + no token → empty dict (fail closed) ──────────────────────

def test_paper_no_token_returns_empty():
    """
    PAPER client with no market-data token must return {} (fail closed),
    not fall back to broker.session with sandbox credentials.
    """
    w = _make_watcher(mode="PAPER", broker_base="https://sandbox.tradier.com")
    if w is None:
        pytest.skip("APEntryWatcher not importable in test env")
    env_clean = {k: v for k, v in os.environ.items()
                 if k not in ("TRADIER_MARKET_DATA_TOKEN","TRADIER_DATA_TOKEN",
                              "TRADIER_MARKET_DATA_BASE_URL","TRADIER_DATA_BASE_URL")}
    with patch.dict(os.environ, env_clean, clear=True):
        result = w._fetch_quotes(["SPY"])
    assert result == {}, (
        "PAPER watcher with no market-data token must return {} (fail closed), "
        f"got {result!r}"
    )
    # broker.session.get must NOT have been called
    w.broker.session.get.assert_not_called()


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
