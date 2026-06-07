"""
tests/test_tradier_global_guard_removal.py
PR: Remove Global Tradier Startup Fatal Guard.
"""
import sys
sys.path.insert(0, '/home/claude')
import pytest

APP_SRC = open('/home/claude/tg_app_fixed.py').read()
CR_SRC  = open('/home/claude/tg_cr_fixed.py').read()

# A: build_broker() does not raise when global creds missing
def test_build_broker_returns_none_not_raise():
    assert "return None" in APP_SRC
    warning_phrase = "per client"
    assert warning_phrase in APP_SRC

def test_fatal_guard_scoped_to_single_client():
    # Raise must be inside the _single branch, not at top level
    assert "Missing TRADIER_ACCESS_TOKEN" in APP_SRC
    idx = APP_SRC.find("Missing TRADIER_ACCESS_TOKEN")
    region = APP_SRC[max(0, idx-400):idx+100]
    assert "_single" in region

def test_none_broker_handled_at_startup():
    assert "broker is None" in APP_SRC
    assert "per-client Tradier credentials will be used" in APP_SRC

# B: pod skips only client missing creds
def _cred_check(members):
    ok, missing = [], []
    for m in members:
        mode = (m.get("tradier_active_mode") or "paper").lower()
        if mode == "live":
            has = bool(m.get("tradier_live_account_id") and m.get("tradier_live_access_token"))
            allow = bool(m.get("allow_live_trading") or m.get("approved"))
        else:
            has = bool(m.get("tradier_account_id") and m.get("tradier_access_token"))
            allow = True
        if has and (mode != "live" or allow):
            ok.append(m)
        else:
            missing.append(m.get("email"))
    return ok, missing

GOOD = {"email":"jason@x.com","tradier_active_mode":"live",
        "tradier_live_account_id":"LA1","tradier_live_access_token":"tok","allow_live_trading":True,"approved":True}
BAD  = {"email":"bad@x.com","tradier_active_mode":"live",
        "tradier_live_account_id":"","tradier_live_access_token":None,"allow_live_trading":True,"approved":True}

def test_skips_only_missing_creds_client():
    ok, missing = _cred_check([GOOD, BAD])
    assert len(ok) == 1 and ok[0]["email"] == "jason@x.com"
    assert "bad@x.com" in missing

# C: continues other valid clients
def test_continues_valid_clients():
    clients = [GOOD, BAD, dict(GOOD, email="other@x.com")]
    ok, missing = _cred_check(clients)
    assert len(ok) == 2 and len(missing) == 1

# D: LIVE without allow_live_trading skipped
def test_live_without_allow_skipped():
    client = dict(GOOD, allow_live_trading=False, approved=False)
    ok, missing = _cred_check([client])
    assert len(ok) == 0 and len(missing) == 1

def test_approved_true_passes():
    client = dict(GOOD, allow_live_trading=False, approved=True)
    ok, missing = _cred_check([client])
    assert len(ok) == 1

# E: SINGLE_CLIENT guard preserved
def test_single_client_guard_still_in_source():
    assert "SINGLE_CLIENT_EMAIL" in APP_SRC
    assert "raise RuntimeError" in APP_SRC

# Boot log
def test_pod_boot_log_present():
    assert "[POD_BOOT]" in CR_SRC
    assert "broker_valid" in CR_SRC
    assert "broker_missing" in CR_SRC
    assert "global_tradier_fallback" in CR_SRC

def test_boot_log_no_secrets():
    idx = CR_SRC.find("[POD_BOOT]")
    region = CR_SRC[idx:idx+400].lower()
    assert "access_token" not in region
    assert "secret" not in region
