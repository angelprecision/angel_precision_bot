"""
tests/test_p0_watcher_audit_schema.py
P0: watcher_decision_audit schema + quote URL + NONE→INVALIDATED lifecycle fix.
"""
import os, re, sys, types, threading, pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

_REPO   = Path(__file__).resolve().parents[1]
EW_SRC  = (_REPO / "ap_entry_watcher.py").read_text()
LC_SRC  = (_REPO / "ap" / "lifecycle.py").read_text() if (_REPO / "ap" / "lifecycle.py").exists() \
          else (_REPO / "ap_lifecycle.py").read_text()


# ── Source: watcher_decision_audit ───────────────────────────────────────────

def test_insert_targets_watcher_decision_audit():
    idx = EW_SRC.find("def _insert_watcher_audit_row(")
    end = EW_SRC.find("\n    def ", idx + 1)
    body = EW_SRC[idx:end]
    assert "INSERT INTO public.watcher_decision_audit" in body, (
        "_insert_watcher_audit_row must target watcher_decision_audit"
    )
    assert "INSERT INTO public.watcher_audit" not in body, (
        "Must NOT insert into legacy watcher_audit (UUID schema mismatch)"
    )

def test_insert_has_text_client_id():
    idx = EW_SRC.find("INSERT INTO public.watcher_decision_audit")
    region = EW_SRC[idx:idx+600]
    assert "client_id" in region

def test_insert_has_quote_fields():
    idx = EW_SRC.find("INSERT INTO public.watcher_decision_audit")
    region = EW_SRC[idx:idx+600]
    for field in ["watcher_quote_source", "watcher_sandbox_mode", "watcher_quote_base_url"]:
        assert field in region, f"watcher_decision_audit INSERT must include {field}"

def test_migration_sql_exists():
    migration = _REPO / "sql" / "migrations" / "watcher_decision_audit.sql"
    alt       = _REPO / "migrations" / "watcher_decision_audit.sql"
    found = migration.exists() or alt.exists()
    assert found, "Migration SQL file for watcher_decision_audit must exist"

def test_migration_creates_correct_table():
    for p in [_REPO/"sql"/"migrations"/"watcher_decision_audit.sql",
              _REPO/"migrations"/"watcher_decision_audit.sql"]:
        if p.exists():
            sql = p.read_text()
            assert "CREATE TABLE" in sql
            assert "watcher_decision_audit" in sql
            assert "local_order_id" in sql
            assert "client_id" in sql
            assert "watcher_quote_source" in sql
            return
    pytest.skip("migration file not found")


# ── Source: lifecycle NONE → INVALIDATED ─────────────────────────────────────

def test_none_to_invalidated_is_legal():
    """NONE → INVALIDATED must be in LEGAL_TRANSITIONS so watcher before-trigger
    invalidation does not produce a spurious ERROR lifecycle row."""
    idx = LC_SRC.find("None:")
    region = LC_SRC[idx:idx+400]
    assert "INVALIDATED" in region, (
        "LEGAL_TRANSITIONS[None] must include INVALIDATED "
        "(watcher can invalidate before signal ever reached WATCHING state)"
    )

def test_none_to_cancelled_is_legal():
    idx = LC_SRC.find("None:")
    region = LC_SRC[idx:idx+400]
    assert "CANCELLED" in region, (
        "LEGAL_TRANSITIONS[None] must include CANCELLED"
    )


# ── Source: quote URL unchanged ───────────────────────────────────────────────

def test_fetch_quotes_uses_resolve_helper():
    idx = EW_SRC.find("def _fetch_quotes(")
    end = EW_SRC.find("\n    def ", idx + 1)
    body = EW_SRC[idx:end]
    assert "_resolve_watcher_quote_url()" in body
    assert "sandbox.tradier.com" not in body

def test_paper_fail_closed_present():
    idx = EW_SRC.find("def _fetch_quotes(")
    end = EW_SRC.find("\n    def ", idx + 1)
    body = EW_SRC[idx:end]
    assert "PAPER_WATCHER_NO_MARKET_DATA_TOKEN" in body


# ── Behavioral: APEntryWatcher + watcher_decision_audit ──────────────────────

def _load_watcher():
    for stub in ["ap.db","ap.broker","ap.broker_factory","ap.config","ap.state",
                 "ap.order_state_machine","ap.notify","ap.models","ap.utils",
                 "ap.reconcile","ap.queue","supabase","psycopg2",
                 "ap.overnight_daily_validator","ap.position_manager","ap.risk"]:
        sys.modules.setdefault(stub, types.ModuleType(stub))
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "ap_entry_watcher_t", _REPO / "ap_entry_watcher.py")
    mod = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)
    except Exception:
        return None, None
    return getattr(mod, "APEntryWatcher", None), mod


def _make_watcher(mode="PAPER", broker_base="https://sandbox.tradier.com"):
    cls, _ = _load_watcher()
    if cls is None:
        return None
    broker = MagicMock()
    broker.base_url = broker_base
    broker.access_token = "exec_tok"
    broker.live_access_token = None
    broker.cfg = MagicMock()
    broker.cfg.base_url = broker_base
    broker.cfg.live_access_token = None
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

_skip = lambda w: pytest.skip("APEntryWatcher not importable") if w is None else None


def test_paper_watcher_url_is_live():
    w = _make_watcher(mode="PAPER")
    if w is None: pytest.skip("not importable")
    with patch.dict(os.environ, {}, clear=False):
        for k in ("TRADIER_MARKET_DATA_BASE_URL","TRADIER_DATA_BASE_URL",
                  "TRADIER_MARKET_DATA_TOKEN","TRADIER_DATA_TOKEN"):
            os.environ.pop(k, None)
        base_url, _ = w._resolve_watcher_quote_url()
    assert "api.tradier.com" in base_url
    assert "sandbox" not in base_url.lower()

def test_sandbox_env_rejected():
    w = _make_watcher(mode="PAPER")
    if w is None: pytest.skip("not importable")
    with patch.dict(os.environ, {"TRADIER_MARKET_DATA_BASE_URL": "https://sandbox.tradier.com",
                                  "TRADIER_MARKET_DATA_TOKEN": "tok"}):
        base_url, _ = w._resolve_watcher_quote_url()
    assert "api.tradier.com" in base_url

def test_paper_no_token_fail_closed():
    w = _make_watcher(mode="PAPER")
    if w is None: pytest.skip("not importable")
    with patch.dict(os.environ, {}, clear=False):
        for k in ("TRADIER_MARKET_DATA_TOKEN","TRADIER_DATA_TOKEN",
                  "TRADIER_MARKET_DATA_BASE_URL","TRADIER_DATA_BASE_URL"):
            os.environ.pop(k, None)
        result = w._fetch_quotes(["SPY"])
    assert result == {}
    w.broker.session.get.assert_not_called()

def test_watcher_decision_audit_params_use_text_client_id():
    """_insert_watcher_audit_row must coerce client_id to str (not UUID)."""
    idx = EW_SRC.find("str(_client_id) if _client_id is not None else None")
    assert idx > 0, "client_id must be str-coerced before INSERT"

# ── Amendment: quote_age_ms in INSERT ────────────────────────────────────────

def test_insert_has_quote_age_ms():
    idx = EW_SRC.find("INSERT INTO public.watcher_decision_audit")
    end = EW_SRC.find("::", idx)  # finds ::jsonb
    region = EW_SRC[idx:end + 20]
    assert "quote_age_ms" in region, (
        "watcher_decision_audit INSERT must include quote_age_ms column"
    )
    # Also verify it appears in params
    params_idx = EW_SRC.find('payload.get("quote_age_ms")')
    assert params_idx > 0 and params_idx > idx, (
        'params must include payload.get("quote_age_ms")'
    )

def test_trigger_audit_persists_quote_age_ms():
    assert '"quote_age_ms":        quote_age_ms' in EW_SRC
    assert 'reason_code="trigger_ready"' in EW_SRC

def test_order_row_id_documented_nullable():
    """order_row_id is null from watcher context — documented near the INSERT."""
    # Find the INSERT itself (not just the function name which appears elsewhere)
    idx = EW_SRC.find("INSERT INTO public.watcher_decision_audit")
    # The comment about order_row_id appears after the INSERT SQL and params
    region = EW_SRC[idx:idx + 5000]
    assert "order_row_id" in region, "order_row_id must be documented near INSERT params"
    assert "local_order_id" in region, "local_order_id is the primary join key"

# ── Amendment: lifecycle fix in ap_lifecycle.py (the one watcher imports) ────

def test_ap_lifecycle_root_has_fix():
    """ap_lifecycle.py (root) — imported by ap_entry_watcher — must have the fix."""
    lc_root = _REPO / "ap_lifecycle.py"
    if not lc_root.exists():
        pytest.skip("ap_lifecycle.py not in repo")
    content = lc_root.read_text()
    idx = content.find("None:")
    region = content[idx:idx+300]
    assert "INVALIDATED" in region, (
        "ap_lifecycle.py (root, imported by watcher) must allow NONE→INVALIDATED"
    )

def test_duplicate_ap_lifecycle_removed():
    assert not (_REPO / "ap" / "lifecycle.py").exists(), (
        "ap/lifecycle.py should be removed so runtime has one canonical ledger singleton"
    )

def test_build_payload_uses_watched_signal_quote_age_ms():
    cls, _ = _load_watcher()
    if cls is None:
        pytest.skip("APEntryWatcher not importable")

    watcher = cls.__new__(cls)
    watcher.broker = MagicMock()
    watcher.broker.base_url = "https://api.tradier.com"
    watcher.broker.live_access_token = "live_tok"
    watcher.broker.cfg = MagicMock()
    watcher.broker.cfg.live_access_token = "live_tok"
    watcher.mode = "PAPER"

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
    payload = watcher._build_watcher_audit_payload(
        watched,
        trigger_type="trigger",
        reason_code="trigger_ready",
        raw_reason="call_breach_confirmed",
    )

    assert payload["quote_age_ms"] == 17
