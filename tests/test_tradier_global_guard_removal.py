"""
tests/test_tradier_global_guard_removal.py
PR#92: Remove Global Tradier Startup Fatal Guard + DB alias fallback.

All source assertions read from the actual repository files via __file__-relative
paths — never from /home/claude/ ephemeral build artifacts.
"""
import os, sys, pathlib
import pytest

# Locate repo root relative to this test file
_REPO = pathlib.Path(__file__).resolve().parent.parent

def _read(rel: str) -> str:
    return (_REPO / rel).read_text()


# ── A: build_broker() does not raise when global creds missing ─────────────────

def test_build_broker_returns_none_not_raise():
    src = _read("app.py")
    assert "return None" in src
    assert "Global TRADIER_ACCESS_TOKEN" in src   # warning text present

def test_fatal_guard_scoped_to_single_client_only():
    src = _read("app.py")
    idx = src.find("Missing TRADIER_ACCESS_TOKEN")
    assert idx > 0, "TRADIER error text must still exist (for SINGLE_CLIENT mode)"
    region = src[max(0, idx - 400):idx + 100]
    assert "_single" in region, "Raise must be inside the _single / SINGLE_CLIENT branch"

def test_none_broker_handled_gracefully_at_startup():
    src = _read("app.py")
    assert "broker is None" in src
    assert "per-client Tradier credentials will be used" in src

def test_fatal_guard_not_at_top_level_of_build_broker():
    """The raise must NOT be the first thing that happens when creds are missing."""
    src = _read("app.py")
    idx = src.find("def build_broker()")
    fn  = src[idx:src.find("\ndef ", idx + 1)]
    # Original fatal line must be gone
    assert 'raise RuntimeError("Missing TRADIER_ACCESS_TOKEN or TRADIER_ACCOUNT_ID")' not in fn


# ── B: pod skips only client missing creds ─────────────────────────────────────

def _cred_check(members):
    """Mirror the per-client credential check added to client_runner.py."""
    ok, missing = [], []
    for m in members:
        mode  = (m.get("tradier_active_mode") or "paper").lower()
        if mode == "live":
            has   = bool(m.get("tradier_live_account_id") and m.get("tradier_live_access_token"))
            allow = bool(m.get("allow_live_trading") or m.get("approved"))
        else:
            has   = bool(m.get("tradier_account_id") and m.get("tradier_access_token"))
            allow = True
        if has and (mode != "live" or allow):
            ok.append(m)
        else:
            missing.append(m.get("email"))
    return ok, missing

_GOOD = {"email": "jason@x.com", "tradier_active_mode": "live",
         "tradier_live_account_id": "LA1", "tradier_live_access_token": "tok",
         "allow_live_trading": True, "approved": True}
_BAD  = {"email": "bad@x.com", "tradier_active_mode": "live",
         "tradier_live_account_id": "", "tradier_live_access_token": None,
         "allow_live_trading": True, "approved": True}

def test_skips_only_missing_creds_client():
    ok, missing = _cred_check([_GOOD, _BAD])
    assert len(ok) == 1 and ok[0]["email"] == "jason@x.com"
    assert "bad@x.com" in missing

def test_missing_creds_does_not_prevent_valid_client():
    ok, missing = _cred_check([_GOOD, _BAD, dict(_GOOD, email="other@x.com")])
    assert len(ok) == 2 and len(missing) == 1


# ── C: valid clients still start ───────────────────────────────────────────────

def test_pod_continues_all_valid_clients():
    clients = [_GOOD, _BAD, dict(_GOOD, email="c@x.com"), dict(_GOOD, email="d@x.com")]
    ok, missing = _cred_check(clients)
    assert len(ok) == 3 and len(missing) == 1


# ── D: LIVE without allow_live_trading is skipped ──────────────────────────────

def test_live_client_without_allow_live_trading_skipped():
    client = dict(_GOOD, allow_live_trading=False, approved=False)
    ok, missing = _cred_check([client])
    assert len(ok) == 0 and len(missing) == 1

def test_approved_true_satisfies_allow_live_trading():
    client = dict(_GOOD, allow_live_trading=False, approved=True)
    ok, missing = _cred_check([client])
    assert len(ok) == 1


# ── E: SINGLE_CLIENT guard preserved ───────────────────────────────────────────

def test_single_client_guard_preserved_in_source():
    src = _read("app.py")
    assert "SINGLE_CLIENT_EMAIL" in src
    assert "raise RuntimeError" in src   # still there for SINGLE_CLIENT branch


# ── F: no sys.exit or RuntimeError during normal pod boot ──────────────────────

def test_no_sys_exit_in_build_broker():
    src = _read("app.py")
    idx = src.find("def build_broker()")
    fn  = src[idx:src.find("\ndef ", idx + 1)]
    assert "sys.exit" not in fn


# ── Boot log format ────────────────────────────────────────────────────────────

def test_pod_boot_log_in_client_runner():
    src = _read("client_runner.py")
    assert "[POD_BOOT]" in src
    assert "broker_valid" in src
    assert "broker_missing" in src
    assert "global_tradier_fallback" in src

def test_boot_log_does_not_expose_secrets():
    src = _read("client_runner.py")
    idx = src.find("[POD_BOOT]")
    region = src[idx:idx + 400].lower()
    assert "access_token" not in region
    assert "secret" not in region


# ── Restorations: confirm no regressions ──────────────────────────────────────

def test_opportunity_ledger_create_in_route_signal():
    src = _read("client_runner.py")
    idx = src.find("def route_signal_to_all_clients(")
    fn  = src[idx:src.find("\ndef ", idx + 1)]
    assert "create_opportunities" in fn, \
        "opportunity_ledger.create_opportunities must remain in route_signal_to_all_clients"

def test_entries_enabled_in_required_live_fields():
    src = _read("client_runner.py")
    idx = src.find('"entries_enabled"')
    assert idx > 0, "entries_enabled must be in the _REQUIRED live field list"
    region = src[max(0, idx - 300):idx + 50]
    assert "_REQUIRED" in region or "max_concurrent_positions" in region

def test_daily_max_loss_pct_param_in_manifest_signature():
    src = _read("client_runner.py")
    assert "daily_max_loss_pct: float = 0.06" in src, \
        "daily_max_loss_pct must be an explicit parameter of _build_startup_manifest"

def test_daily_max_loss_pct_used_in_manifest_dict():
    src = _read("client_runner.py")
    assert '"effective_daily_max_loss_pct": daily_max_loss_pct,' in src, \
        "Manifest dict must reference the parameter, not bare loss_pct"

def test_daily_max_loss_pct_passed_at_call_site():
    src = _read("client_runner.py")
    assert "daily_max_loss_pct=loss_pct," in src, \
        "Call site must pass daily_max_loss_pct=loss_pct to _build_startup_manifest"


# ── DB: alias fallback + circuit-breaker ──────────────────────────────────────

def test_db_url_alias_fallback_present():
    src = _read("ap/db.py")
    assert 'os.getenv("DATABASE_URI")' in src
    assert 'os.getenv("SUPABASE_DB_URL")' in src
    assert 'os.getenv("POSTGRES_URL")' in src

def test_db_circuit_breaker_fail_fast():
    src = _read("ap/db.py")
    assert "ecircuitbreaker" in src
    assert "too many authentication failures" in src
    assert "failing fast" in src

def test_db_circuit_breaker_does_not_retry():
    src = _read("ap/db.py")
    idx = src.find("ecircuitbreaker")
    region = src[max(0, idx - 50):idx + 150]
    # After detecting circuit breaker, must raise immediately
    assert "raise" in region
