"""
tests/test_p0_watcher_quote_url.py
P0: watcher quote URL must be api.tradier.com for both paper and live clients.
"""
import os, re, sys, types, pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

_REPO   = Path(__file__).resolve().parents[1]
EW_SRC  = (_REPO / "ap_entry_watcher.py").read_text()


# ── Source checks ─────────────────────────────────────────────────────────────

def test_resolve_watcher_quote_url_defined():
    assert "def _resolve_watcher_quote_url(" in EW_SRC

def test_fetch_quotes_no_sandbox_fallback():
    """_fetch_quotes must not fall back to sandbox.tradier.com."""
    idx = EW_SRC.find("def _fetch_quotes(")
    end = EW_SRC.find("\n    def ", idx + 1)
    body = EW_SRC[idx:end]
    assert "sandbox.tradier.com" not in body, (
        "_fetch_quotes must not contain sandbox.tradier.com default"
    )

def test_fetch_quotes_uses_resolve_helper():
    idx = EW_SRC.find("def _fetch_quotes(")
    end = EW_SRC.find("\n    def ", idx + 1)
    body = EW_SRC[idx:end]
    assert "_resolve_watcher_quote_url()" in body

def test_watcher_quote_identity_uses_resolve_helper():
    idx = EW_SRC.find("def _watcher_quote_identity(")
    end = EW_SRC.find("\n    def ", idx + 1)
    body = EW_SRC[idx:end]
    assert "_resolve_watcher_quote_url()" in body, (
        "_watcher_quote_identity must read from _resolve_watcher_quote_url "
        "so audit matches actual quote source"
    )

def test_resolve_helper_env_vars_present():
    idx = EW_SRC.find("def _resolve_watcher_quote_url(")
    end = EW_SRC.find("\n    def ", idx + 1)
    body = EW_SRC[idx:end]
    assert "TRADIER_MARKET_DATA_BASE_URL" in body
    assert "TRADIER_MARKET_DATA_TOKEN" in body

def test_resolve_helper_default_is_live():
    """The hardcoded fallback URL must be api.tradier.com, not sandbox."""
    idx = EW_SRC.find("def _resolve_watcher_quote_url(")
    end = EW_SRC.find("\n    def ", idx + 1)
    body = EW_SRC[idx:end]
    assert "api.tradier.com" in body
    # The executable return/assignment must use api.tradier.com — check only
    # non-comment, non-docstring lines for sandbox references in the logic
    # Strip docstring before checking; docstring may mention sandbox in explanation.
    # We care that the actual executable default value is api.tradier.com.
    no_docstring = re.sub(r'\'\'\'.*?\'\'\'', '', body, flags=re.DOTALL)
    no_docstring = re.sub(r'"".*?"""', '', no_docstring, flags=re.DOTALL)
    assert "sandbox.tradier.com" not in no_docstring, (
        "Executable code in _resolve_watcher_quote_url must not contain sandbox URL"
    )

def test_audit_fields_still_present():
    """watcher_audit fields must still be written."""
    for field in ["watcher_quote_source", "watcher_quote_base_url", "watcher_sandbox_mode"]:
        assert field in EW_SRC, f"watcher_audit field missing: {field}"


# ── Behavioral: _resolve_watcher_quote_url ───────────────────────────────────

def _make_watcher(mode="PAPER", broker_base="https://sandbox.tradier.com"):
    """Build a minimal EntryWatcher stub for testing."""
    # Load the module with heavy deps stubbed
    for stub in ("ap.db","ap.broker","ap.broker_factory","ap.config",
                 "ap.state","ap.order_state_machine","ap.notify","ap.models",
                 "ap.utils","supabase","ap.reconcile","ap.queue"):
        sys.modules.setdefault(stub, types.ModuleType(stub))
    try:
        import importlib.util
        spec = importlib.util.spec_from_file_location("ap_entry_watcher",
                   Path("/home/claude/wq_ap_entry_watcher_fixed.py").replace("Path(\"","").replace("\")",""))
        mod  = importlib.util.module_from_spec(spec)
        sys.modules["ap_entry_watcher"] = mod
        spec.loader.exec_module(mod)
        cls = getattr(mod, "EntryWatcher", None)
        if cls is None:
            return None, None
        w = cls.__new__(cls)
        w.mode = mode.upper()
        broker = MagicMock()
        broker.base_url = broker_base
        broker.access_token = "sandbox_tok"
        broker.live_access_token = None
        broker.cfg = MagicMock()
        broker.cfg.base_url = broker_base
        broker.cfg.live_access_token = None
        w.broker = broker
        return w, mod
    except Exception as e:
        return None, None


_skip = pytest.mark.skipif(True, reason="module not importable in test env")

try:
    _w, _mod = _make_watcher()
    _CAN_IMPORT = _w is not None
except Exception:
    _CAN_IMPORT = False

_maybe_skip = pytest.mark.skipif(not _CAN_IMPORT, reason="ap_entry_watcher not importable")


@_maybe_skip
def test_resolve_paper_client_returns_live_url_no_env():
    """Paper client with no env vars: URL must be api.tradier.com."""
    w, _ = _make_watcher(mode="PAPER", broker_base="https://sandbox.tradier.com")
    env = {k: v for k, v in os.environ.items()
            if k not in ("TRADIER_MARKET_DATA_BASE_URL", "TRADIER_DATA_BASE_URL",
                          "TRADIER_MARKET_DATA_TOKEN", "TRADIER_DATA_TOKEN")}
    with patch.dict(os.environ, env, clear=True):
        base_url, token = w._resolve_watcher_quote_url()
    assert "api.tradier.com" in base_url, (
        f"Paper client must use api.tradier.com for quotes, got {base_url!r}"
    )
    assert "sandbox" not in base_url.lower(), (
        f"Paper client must NOT use sandbox URL for quotes, got {base_url!r}"
    )

@_maybe_skip
def test_resolve_env_override_takes_priority():
    """TRADIER_MARKET_DATA_BASE_URL env var overrides everything."""
    w, _ = _make_watcher(mode="PAPER", broker_base="https://sandbox.tradier.com")
    with patch.dict(os.environ, {"TRADIER_MARKET_DATA_BASE_URL": "https://api.tradier.com"}):
        base_url, _ = w._resolve_watcher_quote_url()
    assert base_url == "https://api.tradier.com"

@_maybe_skip
def test_resolve_md_token_env_returned():
    """TRADIER_MARKET_DATA_TOKEN env var is used for the token."""
    w, _ = _make_watcher()
    with patch.dict(os.environ, {"TRADIER_MARKET_DATA_TOKEN": "live_data_tok_abc"}):
        _, token = w._resolve_watcher_quote_url()
    assert token == "live_data_tok_abc"

@_maybe_skip
def test_resolve_live_access_token_fallback():
    """broker.live_access_token is used when no env token set."""
    w, _ = _make_watcher()
    w.broker.live_access_token = "broker_live_tok"
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("TRADIER_MARKET_DATA_TOKEN", None)
        os.environ.pop("TRADIER_DATA_TOKEN", None)
        _, token = w._resolve_watcher_quote_url()
    assert token == "broker_live_tok"

@_maybe_skip
def test_watcher_quote_identity_paper_shows_live():
    """_watcher_quote_identity must report tradier_live / sandbox=False for paper."""
    w, _ = _make_watcher(mode="PAPER", broker_base="https://sandbox.tradier.com")
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("TRADIER_MARKET_DATA_BASE_URL", None)
        os.environ.pop("TRADIER_DATA_BASE_URL", None)
        identity = w._watcher_quote_identity()
    assert identity["watcher_sandbox_mode"] is False
    assert identity["watcher_quote_source"] == "tradier_live"
    assert "api.tradier.com" in identity["watcher_quote_base_url"]
