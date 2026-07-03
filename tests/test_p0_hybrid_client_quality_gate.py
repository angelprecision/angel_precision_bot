"""
tests/test_hybrid_client_quality_gate.py
P0: Unit tests for the Hybrid Client Quality Gate.
All tests run with HYBRID_CLIENT_QUALITY_MODE=true.
"""
import os
import pytest

os.environ.update({
    "HYBRID_CLIENT_QUALITY_MODE":          "true",
    "MIN_CLIENT_SCORE":                    "70",
    "ALLOW_CLIENT_TIER_B":                 "true",
    "DAILY_CLIENT_PATTERN_WHITELIST":      "2-3,3-2-2,1-2_2D",
    "INTRADAY_CLIENT_PATTERN_WHITELIST":   "",
    "ALLOW_FAILED_DIR_CLIENT":             "false",
    "ENTRY_CONFIRM_SECONDS":               "45",
    "MAX_CLIENT_TRADES_PER_DAY":           "5",
    "MAX_CLIENT_DAILY_TRADES":             "3",
    "MAX_CLIENT_INTRADAY_TRADES":          "2",
    "MAX_CLIENT_SYMBOL_TRADES_PER_DAY":    "1",
    "MAX_PRE_ENTRY_OPTION_FADE_PCT":       "8",
    "MAX_PRE_ENTRY_UNDERLYING_REVERSAL_PCT":"0.25",
})

from ap_hybrid_client_quality_gate import evaluate_client_quality_gate

EMPTY_SNAP = {"trades_today": 0, "daily_trades": 0, "intraday_trades": 0, "symbol_trades": {}}

def _sig(**kw):
    base = dict(symbol="AAPL", direction="CALL", timeframe="1d",
                pattern="2-3", tier="A", score=75,
                trigger_price=180.0, stop_underlying=175.0, target_underlying=190.0)
    base.update(kw)
    return base


# ── Gate disabled ─────────────────────────────────────────────────────────────
def test_gate_disabled_passes_everything():
    os.environ["HYBRID_CLIENT_QUALITY_MODE"] = "false"
    g = evaluate_client_quality_gate(_sig(score=50, tier="C"), "c1", EMPTY_SNAP)
    assert g.allowed is True
    assert g.quality_lane == "GATE_DISABLED"
    os.environ["HYBRID_CLIENT_QUALITY_MODE"] = "true"


# ── Score gate ────────────────────────────────────────────────────────────────
def test_score_below_70_blocked():
    g = evaluate_client_quality_gate(_sig(score=69), "c1", EMPTY_SNAP)
    assert g.allowed is False
    assert g.block_reason == "client_score_below_70"

def test_score_70_passes():
    g = evaluate_client_quality_gate(_sig(score=70), "c1", EMPTY_SNAP)
    assert g.allowed is True

def test_score_78_a_tier_passes():
    g = evaluate_client_quality_gate(_sig(score=78, tier="A"), "c1", EMPTY_SNAP)
    assert g.allowed is True


# ── Tier gate ─────────────────────────────────────────────────────────────────
def test_tier_c_blocked():
    g = evaluate_client_quality_gate(_sig(tier="C"), "c1", EMPTY_SNAP)
    assert g.allowed is False
    assert g.block_reason == "client_tier_block"

def test_tier_b_allowed_when_flag_true():
    g = evaluate_client_quality_gate(_sig(tier="B"), "c1", EMPTY_SNAP)
    assert g.allowed is True

def test_tier_b_blocked_when_flag_false():
    os.environ["ALLOW_CLIENT_TIER_B"] = "false"
    g = evaluate_client_quality_gate(_sig(tier="B"), "c1", EMPTY_SNAP)
    assert g.allowed is False
    assert g.block_reason == "client_tier_block"
    os.environ["ALLOW_CLIENT_TIER_B"] = "true"


# ── Daily lane ────────────────────────────────────────────────────────────────
def test_daily_whitelisted_pattern_passes():
    for pat in ("2-3", "3-2-2", "1-2_2D"):
        g = evaluate_client_quality_gate(_sig(pattern=pat, timeframe="1d"), "c1", EMPTY_SNAP)
        assert g.allowed is True, f"Expected PASS for pattern={pat}"
        assert g.quality_lane == "DAILY_CLIENT"

def test_daily_non_whitelisted_pattern_blocked():
    g = evaluate_client_quality_gate(_sig(pattern="2-1", timeframe="1d"), "c1", EMPTY_SNAP)
    assert g.allowed is False
    assert g.block_reason == "client_daily_pattern_not_whitelisted"


# ── Intraday / FAILED_DIR quarantine ─────────────────────────────────────────
def test_failed_dir_intraday_blocked():
    for tf in ("60m", "30m", "15m"):
        for pat in ("FAILED_DIR_2U_30min+60min", "FAILED_DIR_2D_60min",
                    "FAILED_DIR_2U_60min", "FAILED_DIR_2D_15min"):
            g = evaluate_client_quality_gate(
                _sig(pattern=pat, timeframe=tf, score=78, tier="A"), "c1", EMPTY_SNAP)
            assert g.allowed is False, f"Expected BLOCK for {pat} tf={tf}"
            assert g.block_reason == "client_intraday_failed_dir_quarantine"

def test_intraday_whitelist_empty_blocks_all():
    os.environ["INTRADAY_CLIENT_PATTERN_WHITELIST"] = ""
    g = evaluate_client_quality_gate(
        _sig(pattern="2-3", timeframe="60m"), "c1", EMPTY_SNAP)
    assert g.allowed is False
    assert g.block_reason == "client_intraday_no_whitelist"

def test_intraday_whitelisted_pattern_passes():
    os.environ["INTRADAY_CLIENT_PATTERN_WHITELIST"] = "2-3,3-1"
    g = evaluate_client_quality_gate(
        _sig(pattern="2-3", timeframe="60m"), "c1", EMPTY_SNAP)
    assert g.allowed is True
    assert g.quality_lane == "INTRADAY_CLIENT"
    os.environ["INTRADAY_CLIENT_PATTERN_WHITELIST"] = ""


# ── Geometry gate ─────────────────────────────────────────────────────────────
def test_call_stop_above_trigger_blocked():
    g = evaluate_client_quality_gate(
        _sig(direction="CALL", trigger_price=180, stop_underlying=182), "c1", EMPTY_SNAP)
    assert g.allowed is False
    assert g.block_reason == "invalid_trigger_stop_geometry"

def test_put_stop_below_trigger_blocked():
    g = evaluate_client_quality_gate(
        _sig(direction="PUT", trigger_price=180, stop_underlying=178), "c1", EMPTY_SNAP)
    assert g.allowed is False
    assert g.block_reason == "invalid_trigger_stop_geometry"

def test_call_valid_geometry_passes():
    g = evaluate_client_quality_gate(
        _sig(direction="CALL", trigger_price=180, stop_underlying=175), "c1", EMPTY_SNAP)
    assert g.allowed is True
    assert g.metadata.get("geometry_valid") is True

def test_put_valid_geometry_passes():
    g = evaluate_client_quality_gate(
        _sig(direction="PUT", trigger_price=180, stop_underlying=185,
             target_underlying=170), "c1", EMPTY_SNAP)
    assert g.allowed is True


# ── Client caps ───────────────────────────────────────────────────────────────
def test_total_daily_cap_blocks():
    snap = {**EMPTY_SNAP, "trades_today": 5, "daily_trades": 3}
    g = evaluate_client_quality_gate(_sig(), "c1", snap)
    assert g.allowed is False
    # RED-ON-MAIN CLEANUP (PR #265): reason split into total vs lane caps.
    assert g.block_reason == "client_total_daily_cap_reached"

def test_daily_lane_cap_blocks():
    snap = {**EMPTY_SNAP, "daily_trades": 3}
    g = evaluate_client_quality_gate(_sig(timeframe="1d"), "c1", snap)
    assert g.allowed is False
    # RED-ON-MAIN CLEANUP (PR #265): reason split into total vs lane caps.
    assert g.block_reason == "client_daily_lane_cap_reached"

def test_intraday_cap_blocks():
    os.environ["INTRADAY_CLIENT_PATTERN_WHITELIST"] = "2-3"
    snap = {**EMPTY_SNAP, "intraday_trades": 2}
    g = evaluate_client_quality_gate(_sig(timeframe="60m", pattern="2-3"), "c1", snap)
    assert g.allowed is False
    assert g.block_reason == "client_intraday_cap_reached"
    os.environ["INTRADAY_CLIENT_PATTERN_WHITELIST"] = ""

def test_symbol_duplicate_blocked():
    snap = {**EMPTY_SNAP, "symbol_trades": {"AAPL": 1}}
    g = evaluate_client_quality_gate(_sig(symbol="AAPL"), "c1", snap)
    assert g.allowed is False
    assert g.block_reason == "client_symbol_duplicate_block"


# ── Metadata audit ────────────────────────────────────────────────────────────
def test_allowed_signal_has_full_metadata():
    g = evaluate_client_quality_gate(_sig(), "c1", EMPTY_SNAP)
    assert g.allowed is True
    meta = g.to_meta()
    assert meta["hybrid_client_quality_mode"] is True
    assert meta["client_eligible"] is True
    assert meta["quality_lane"] == "DAILY_CLIENT"
    assert meta["geometry_valid"] is True
    assert meta["confirmation_required"] is True
    assert meta["confirmation_seconds"] == 45.0
    assert meta["score"] == 75

def test_blocked_signal_has_block_reason():
    g = evaluate_client_quality_gate(_sig(score=65), "c1", EMPTY_SNAP)
    assert g.allowed is False
    meta = g.to_meta()
    assert meta["hybrid_client_quality_mode"] is True
    assert meta["client_eligible"] is False
    assert meta["block_reason"] == "client_score_below_70"


# ── Hard termination invariant ────────────────────────────────────────────────
def test_blocked_decision_is_terminal():
    """Gate returns allowed=False — caller must NOT retry or re-approve."""
    g = evaluate_client_quality_gate(_sig(score=50), "c1", EMPTY_SNAP)
    assert g.allowed is False
    # Calling again with same signal must also return False (stateless gate —
    # no state mutation that could flip it to True on retry)
    g2 = evaluate_client_quality_gate(_sig(score=50), "c1", EMPTY_SNAP)
    assert g2.allowed is False
