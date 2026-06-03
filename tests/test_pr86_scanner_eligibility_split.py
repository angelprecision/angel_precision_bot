"""
tests/test_pr86_scanner_eligibility_split.py
PR86: Scanner intake vs client eligibility split.
"""
import os, pytest
os.environ.update({
    "HYBRID_CLIENT_QUALITY_MODE":         "true",
    "MIN_CLIENT_SCORE":                   "70",
    "ALLOW_CLIENT_TIER_B":                "true",
    "DAILY_CLIENT_PATTERN_WHITELIST":     "2-3,3-2-2,1-2_2D",
    "INTRADAY_CLIENT_PATTERN_WHITELIST":  "",
    "ALLOW_FAILED_DIR_CLIENT":            "false",
    "MAX_CLIENT_TRADES_PER_DAY":          "5",
    "MAX_CLIENT_DAILY_TRADES":            "3",
    "MAX_CLIENT_INTRADAY_TRADES":         "2",
    "MAX_CLIENT_SYMBOL_TRADES_PER_DAY":   "1",
})

import sys
sys.path.insert(0, '/home/claude')
import importlib
for m in list(sys.modules.keys()):
    if 'ap_hybrid' in m: del sys.modules[m]
import shutil
shutil.copy('/home/claude/pr86_gate_new.py', '/home/claude/ap_hybrid_client_quality_gate.py')

from ap_hybrid_client_quality_gate import (
    evaluate_client_quality_gate,
    _classify_eligibility,
    HybridGateDecision,
)

EMPTY = {"trades_today": 0, "daily_trades": 0, "intraday_trades": 0, "symbol_trades": {}}

def _g(signal, snap=None):
    return evaluate_client_quality_gate(signal, "c1", snap or EMPTY)

def _sig(**kw):
    base = dict(symbol="AAPL", direction="CALL", timeframe="1d",
                pattern="2-3", tier="A", score=75,
                trigger_price=180.0, stop_underlying=175.0)
    base.update(kw)
    return base


# ── CLIENT_ELIGIBLE ───────────────────────────────────────────────────────────
def test_passing_signal_is_client_eligible():
    g = _g(_sig())
    assert g.allowed is True
    assert g.client_eligibility_status == "CLIENT_ELIGIBLE"
    assert g.observe_reason is None
    assert g.scanner_intake_status == "EVALUATED"

def test_client_eligible_in_to_meta():
    g = _g(_sig())
    m = g.to_meta()
    assert m["client_eligibility_status"] == "CLIENT_ELIGIBLE"
    assert m["scanner_intake_status"] == "EVALUATED"
    assert m["observe_reason"] is None


# ── OBSERVE_NOT_CLIENT — FAILED_DIR ───────────────────────────────────────────
def test_failed_dir_is_observe_not_client():
    os.environ["INTRADAY_CLIENT_PATTERN_WHITELIST"] = "2-3"
    g = _g(_sig(timeframe="60m", pattern="FAILED_DIR_2U_30min+60min",
                score=78, tier="A",
                stop_underlying=185))  # PUT geometry for reference — use CALL with valid geo
    # CALL with stop below trigger is valid geometry
    g = _g(_sig(timeframe="60m", pattern="FAILED_DIR_2U_30min+60min",
                score=78, tier="A",
                trigger_price=180, stop_underlying=175))
    assert g.allowed is False
    assert g.client_eligibility_status == "OBSERVE_NOT_CLIENT"
    assert g.observe_reason == "observe_failed_dir_quarantine"
    assert g.block_reason == "client_intraday_failed_dir_quarantine"
    os.environ["INTRADAY_CLIENT_PATTERN_WHITELIST"] = ""


# ── OBSERVE_NOT_CLIENT — score 65-69 ─────────────────────────────────────────
def test_score_65_is_observe():
    g = _g(_sig(score=65))
    assert g.allowed is False
    assert g.client_eligibility_status == "OBSERVE_NOT_CLIENT"
    assert g.observe_reason == "observe_score_65_69"

def test_score_69_is_observe():
    g = _g(_sig(score=69.9))
    assert g.allowed is False
    assert g.client_eligibility_status == "OBSERVE_NOT_CLIENT"
    assert g.observe_reason == "observe_score_65_69"

def test_score_64_is_blocked():
    g = _g(_sig(score=64))
    assert g.allowed is False
    assert g.client_eligibility_status == "BLOCKED"
    assert g.observe_reason is None


# ── OBSERVE_NOT_CLIENT — non-whitelisted intraday ─────────────────────────────
def test_intraday_no_whitelist_is_observe():
    os.environ["INTRADAY_CLIENT_PATTERN_WHITELIST"] = ""
    g = _g(_sig(timeframe="60m", pattern="2-3",
                trigger_price=180, stop_underlying=175))
    assert g.allowed is False
    assert g.client_eligibility_status == "OBSERVE_NOT_CLIENT"
    assert g.observe_reason == "observe_intraday_not_whitelisted"

def test_intraday_pattern_not_whitelisted_is_observe():
    os.environ["INTRADAY_CLIENT_PATTERN_WHITELIST"] = "3-2-2"
    g = _g(_sig(timeframe="30m", pattern="2-3",
                trigger_price=180, stop_underlying=175))
    assert g.allowed is False
    assert g.client_eligibility_status == "OBSERVE_NOT_CLIENT"
    assert g.observe_reason == "observe_intraday_not_whitelisted"
    os.environ["INTRADAY_CLIENT_PATTERN_WHITELIST"] = ""

def test_2h_timeframe_is_observe():
    g = _g(_sig(timeframe="2h", score=78, tier="A"))
    assert g.allowed is False
    assert g.client_eligibility_status == "OBSERVE_NOT_CLIENT"
    assert g.observe_reason == "observe_intraday_not_whitelisted"


# ── OBSERVE_NOT_CLIENT — unproven pattern (daily not whitelisted) ─────────────
def test_daily_pattern_not_whitelisted_is_observe():
    g = _g(_sig(pattern="2-1"))  # not in whitelist
    assert g.allowed is False
    assert g.client_eligibility_status == "OBSERVE_NOT_CLIENT"
    assert g.observe_reason == "observe_pattern_unproven"

def test_tier_c_is_observe():
    g = _g(_sig(tier="C"))
    assert g.allowed is False
    assert g.client_eligibility_status == "OBSERVE_NOT_CLIENT"
    assert g.observe_reason == "observe_pattern_unproven"


# ── BLOCKED — hard invalid ────────────────────────────────────────────────────
def test_invalid_geometry_is_blocked():
    g = _g(_sig(direction="CALL", trigger_price=180, stop_underlying=182))
    assert g.allowed is False
    assert g.client_eligibility_status == "BLOCKED"
    assert g.observe_reason is None

def test_missing_timeframe_is_blocked():
    g = _g({"symbol":"AAPL","direction":"CALL","pattern":"2-3",
             "tier":"A","score":75,"trigger_price":180,"stop_underlying":175})
    assert g.allowed is False
    assert g.client_eligibility_status == "BLOCKED"

def test_cap_hit_is_blocked():
    snap = {"total_trades": 5, "daily_trades": 0, "intraday_trades": 0, "symbol_trades": {}}
    g = _g(_sig(), snap)
    assert g.allowed is False
    assert g.client_eligibility_status == "BLOCKED"

def test_symbol_duplicate_is_blocked():
    snap = {**EMPTY, "symbol_trades": {"AAPL": 1}}
    g = _g(_sig())
    # Symbol cap = 1, so first trade is fine, second would block
    snap = {**EMPTY, "symbol_trades": {"AAPL": 1}}
    g = _g(_sig(), snap)
    assert g.allowed is False
    assert g.client_eligibility_status == "BLOCKED"


# ── Scanner metadata ──────────────────────────────────────────────────────────
def test_scanner_metadata_in_pass():
    sig = _sig(scanner_type="3-2-2-scanner", scanner_name="overnight",
               scanner_version="v2", pattern_family="directional",
               signal_id="sig_abc123")
    g = _g(sig)
    m = g.to_meta()
    assert m["scanner_type"]    == "3-2-2-scanner"
    assert m["scanner_name"]    == "overnight"
    assert m["scanner_version"] == "v2"
    assert m["pattern_family"]  == "directional"
    assert m["signal_id"]       == "sig_abc123"

def test_scanner_metadata_in_observe():
    sig = _sig(pattern="2-1", scanner_type="mtf-scanner",
               signal_id="obs_999")  # non-whitelisted → observe
    g = _g(sig)
    m = g.to_meta()
    assert m["scanner_type"] == "mtf-scanner"
    assert m["signal_id"]    == "obs_999"
    assert m["client_eligibility_status"] == "OBSERVE_NOT_CLIENT"


# ── scanner_intake_status ────────────────────────────────────────────────────
def test_all_signals_have_intake_status():
    for sig_kw, expected in [
        (_sig(),                         "EVALUATED"),          # pass
        (_sig(score=68),                 "EVALUATED"),          # observe
        (_sig(trigger_price=180,stop_underlying=182), "EVALUATED"),  # blocked
    ]:
        g = _g(sig_kw)
        assert g.scanner_intake_status == expected, f"Expected EVALUATED for {sig_kw}"

def test_gate_disabled_is_client_eligible():
    os.environ["HYBRID_CLIENT_QUALITY_MODE"] = "false"
    g = _g(_sig(score=50, tier="C"))
    assert g.client_eligibility_status == "CLIENT_ELIGIBLE"
    assert g.scanner_intake_status == "EVALUATED"
    os.environ["HYBRID_CLIENT_QUALITY_MODE"] = "true"


# ── classify_eligibility unit tests ──────────────────────────────────────────
def test_classify_none_reason_is_eligible():
    s, r = _classify_eligibility(None, 75)
    assert s == "CLIENT_ELIGIBLE" and r is None

def test_classify_failed_dir_is_observe():
    s, r = _classify_eligibility("client_intraday_failed_dir_quarantine", 78)
    assert s == "OBSERVE_NOT_CLIENT" and r == "observe_failed_dir_quarantine"

def test_classify_score_69_is_observe():
    s, r = _classify_eligibility("client_score_below_70", 69)
    assert s == "OBSERVE_NOT_CLIENT" and r == "observe_score_65_69"

def test_classify_score_64_is_blocked():
    s, r = _classify_eligibility("client_score_below_70", 64)
    assert s == "BLOCKED" and r is None

def test_classify_geometry_is_blocked():
    s, r = _classify_eligibility("invalid_trigger_stop_geometry", 78)
    assert s == "BLOCKED" and r is None

def test_classify_unknown_reason_is_observe():
    s, r = _classify_eligibility("some_future_reason", 72)
    assert s == "OBSERVE_NOT_CLIENT" and r == "observe_manual_research_only"
