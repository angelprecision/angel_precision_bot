"""
tests/test_p0_hybrid_gate_hardening.py
P0 follow-up: field fallback + safe rollout hardening tests.
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

# Import from the new hardened module in current dir
import importlib, sys
sys.path.insert(0, '/home/claude')
# Reload to pick up new version
if 'ap_hybrid_client_quality_gate' in sys.modules:
    del sys.modules['ap_hybrid_client_quality_gate']
import shutil
shutil.copy('/home/claude/harden_gate_new.py',
            '/home/claude/ap_hybrid_client_quality_gate.py')

from ap_hybrid_client_quality_gate import (
    evaluate_client_quality_gate,
    _resolve_score, _resolve_pattern, _resolve_timeframe, _resolve_tier,
)

EMPTY = {"trades_today": 0, "daily_trades": 0, "intraday_trades": 0, "symbol_trades": {}}

def _g(signal, snap=None):
    return evaluate_client_quality_gate(signal, "c1", snap or EMPTY)

# ── Score fallbacks ───────────────────────────────────────────────────────────
def test_score_from_score_field():
    s, src = _resolve_score({"score": 75})
    assert s == 75.0 and src == "score"

def test_score_from_score_total():
    s, src = _resolve_score({"score_total": 72})
    assert s == 72.0 and src == "score_total"

def test_score_from_merge_score():
    s, src = _resolve_score({"merge_score": 71})
    assert s == 71.0 and src == "merge_score"

def test_score_from_scanner_score():
    s, src = _resolve_score({"scanner_score": 78})
    assert s == 78.0 and src == "scanner_score"

def test_score_from_intel_score():
    s, src = _resolve_score({"intel_score": 80})
    assert s == 80.0 and src == "intel_score"

def test_score_fallback_priority():
    # score takes priority when present
    s, src = _resolve_score({"score": 75, "score_total": 60})
    assert s == 75.0 and src == "score"

def test_score_total_used_when_score_missing():
    # score_total kicks in when score key is absent
    g = _g({"symbol":"AAPL","direction":"CALL","timeframe":"1d",
             "pattern":"2-3","tier":"A",
             "score_total": 74,
             "trigger_price":180,"stop_underlying":175})
    assert g.allowed is True
    assert g.metadata["resolved_score_source"] == "score_total"
    assert g.metadata["score"] == 74.0

def test_signal_with_score_total_not_blocked_as_zero():
    # Regression: score key absent, score_total=72 — must NOT block as score 0
    g = _g({"symbol":"BAC","direction":"CALL","timeframe":"1d",
             "pattern":"2-3","tier":"B",
             "score_total": 72,
             "trigger_price":46,"stop_underlying":44})
    assert g.allowed is True, f"Expected PASS, got block_reason={g.block_reason}"

# ── Pattern fallbacks ─────────────────────────────────────────────────────────
def test_pattern_from_pattern_id():
    p, src = _resolve_pattern({"pattern_id": "2-3"})
    assert p == "2-3" and src == "pattern_id"

def test_pattern_from_setup_combo():
    p, src = _resolve_pattern({"setup_combo": "3-2-2"})
    assert p == "3-2-2" and src == "setup_combo"

def test_pattern_from_pattern_family():
    p, src = _resolve_pattern({"pattern_family": "1-2_2D"})
    assert p == "1-2_2D" and src == "pattern_family"

def test_pattern_id_passes_whitelist():
    g = _g({"symbol":"AAPL","direction":"CALL","timeframe":"1d",
             "pattern_id":"2-3","tier":"A","score":75,
             "trigger_price":180,"stop_underlying":175})
    assert g.allowed is True
    assert g.metadata["resolved_pattern_source"] == "pattern_id"

def test_setup_combo_passes_whitelist():
    g = _g({"symbol":"AAPL","direction":"CALL","timeframe":"1d",
             "setup_combo":"3-2-2","tier":"A","score":75,
             "trigger_price":180,"stop_underlying":175})
    assert g.allowed is True
    assert g.metadata["resolved_pattern_source"] == "setup_combo"

# ── Timeframe fallbacks ───────────────────────────────────────────────────────
def test_timeframe_from_time_horizon():
    tf, src = _resolve_timeframe({"time_horizon": "1d"})
    assert tf == "1d" and src == "time_horizon"

def test_missing_timeframe_blocks_not_defaults():
    g = _g({"symbol":"AAPL","direction":"CALL",
             "pattern":"2-3","tier":"A","score":75,
             "trigger_price":180,"stop_underlying":175})
    assert g.allowed is False
    assert g.block_reason == "client_timeframe_missing"
    assert g.metadata["resolved_timeframe_source"] == "missing"

def test_time_horizon_daily_passes():
    g = _g({"symbol":"AAPL","direction":"CALL",
             "time_horizon":"1d","pattern":"2-3","tier":"A","score":75,
             "trigger_price":180,"stop_underlying":175})
    assert g.allowed is True
    assert g.metadata["resolved_timeframe_source"] == "time_horizon"

# ── Tier fallbacks ────────────────────────────────────────────────────────────
def test_tier_from_score_grade():
    t, src, missing = _resolve_tier({"score_grade": "A"})
    assert t == "A" and src == "score_grade" and missing is False

def test_missing_tier_sets_flag_but_passes_score70():
    g = _g({"symbol":"AAPL","direction":"CALL","timeframe":"1d",
             "pattern":"2-3","score":75,    # no tier field at all
             "trigger_price":180,"stop_underlying":175})
    assert g.allowed is True
    assert g.metadata["tier_missing"] is True
    assert g.metadata["resolved_tier_source"] == "missing"

def test_score_grade_b_passes():
    g = _g({"symbol":"IWM","direction":"CALL","timeframe":"1d",
             "pattern":"2-3","score_grade":"B","score":71,
             "trigger_price":200,"stop_underlying":195})
    assert g.allowed is True
    assert g.metadata["tier"] == "B"
    assert g.metadata["resolved_tier_source"] == "score_grade"

def test_score_grade_c_blocks():
    g = _g({"symbol":"AAPL","direction":"CALL","timeframe":"1d",
             "pattern":"2-3","score_grade":"C","score":75,
             "trigger_price":180,"stop_underlying":175})
    assert g.allowed is False
    assert g.block_reason == "client_tier_block"

# ── Snapshot cap fix ──────────────────────────────────────────────────────────
def test_daily_trades_not_aliased_to_total():
    # total_trades=5 but daily_trades=1 — should NOT block on daily cap
    snap = {"total_trades": 5, "daily_trades": 1, "intraday_trades": 0,
            "trades_today": 5, "symbol_trades": {}}
    # total cap is 5 so it WILL block on total — but NOT on daily cap
    g = _g({"symbol":"AAPL","direction":"CALL","timeframe":"1d",
             "pattern":"2-3","tier":"A","score":75,
             "trigger_price":180,"stop_underlying":175}, snap)
    assert g.block_reason == "client_daily_cap_reached"
    assert g.metadata.get("trades_today") == 5  # total cap hit
    # Confirm it's the total cap not the daily lane cap
    assert g.metadata.get("max") == 5

def test_daily_lane_cap_independent():
    # daily_trades=3 (at cap), total=2 — should block on daily lane cap
    snap = {"total_trades": 2, "daily_trades": 3, "intraday_trades": 0,
            "trades_today": 2, "symbol_trades": {}}
    g = _g({"symbol":"AAPL","direction":"CALL","timeframe":"1d",
             "pattern":"2-3","tier":"A","score":75,
             "trigger_price":180,"stop_underlying":175}, snap)
    assert g.allowed is False
    assert g.block_reason == "client_daily_cap_reached"
    assert g.metadata.get("daily_trades") == 3

def test_intraday_cap_independent():
    os.environ["INTRADAY_CLIENT_PATTERN_WHITELIST"] = "2-3"
    snap = {"total_trades": 1, "daily_trades": 0, "intraday_trades": 2,
            "trades_today": 1, "symbol_trades": {}}
    # CALL: stop must be below trigger (175 < 180) — valid geometry
    g = _g({"symbol":"AAPL","direction":"CALL","timeframe":"60m",
             "pattern":"2-3","tier":"A","score":75,
             "trigger_price":180,"stop_underlying":175}, snap)
    assert g.allowed is False
    assert g.block_reason == "client_intraday_cap_reached"
    os.environ["INTRADAY_CLIENT_PATTERN_WHITELIST"] = ""

# ── Resolution metadata on every decision ────────────────────────────────────
def test_resolution_metadata_on_pass():
    g = _g({"symbol":"AAPL","direction":"CALL","timeframe":"1d",
             "pattern":"2-3","tier":"A","score":75,
             "trigger_price":180,"stop_underlying":175})
    m = g.to_meta()
    for field in ("resolved_score_source","resolved_pattern_source",
                  "resolved_timeframe_source","resolved_tier_source",
                  "raw_score_fields","raw_pattern_fields"):
        assert field in m, f"Missing: {field}"

def test_resolution_metadata_on_block():
    g = _g({"symbol":"AAPL","direction":"CALL","timeframe":"1d",
             "pattern":"2-3","tier":"A","score":60})
    m = g.to_meta()
    assert "resolved_score_source" in m
    assert m["resolved_score_source"] == "score"

def test_raw_score_fields_captured():
    g = _g({"symbol":"AAPL","direction":"CALL","timeframe":"1d",
             "pattern":"2-3","tier":"A",
             "score": 75, "score_total": 72,
             "trigger_price":180,"stop_underlying":175})
    assert g.metadata["raw_score_fields"] == {"score": 75, "score_total": 72}

def test_raw_pattern_fields_captured():
    g = _g({"symbol":"AAPL","direction":"CALL","timeframe":"1d",
             "pattern":"2-3","pattern_id":"2-3","tier":"A","score":75,
             "trigger_price":180,"stop_underlying":175})
    assert "pattern" in g.metadata["raw_pattern_fields"]
    assert "pattern_id" in g.metadata["raw_pattern_fields"]

# ── Original 23 acceptance criteria still pass ───────────────────────────────
def test_btier_daily_winners_still_pass():
    for sym, pat in [("BAC","2-3"),("AAPL","2-3"),("IWM","3-2-2")]:
        g = _g({"symbol":sym,"direction":"CALL","timeframe":"1d",
                 "pattern":pat,"tier":"B","score":71,
                 "trigger_price":100,"stop_underlying":97})
        assert g.allowed is True, f"{sym} {pat} should pass"

def test_failed_dir_still_blocked():
    os.environ["INTRADAY_CLIENT_PATTERN_WHITELIST"] = "2-3"
    g = _g({"symbol":"NVDA","direction":"PUT","timeframe":"60m",
             "pattern":"FAILED_DIR_2U_30min+60min","tier":"A","score":78,
             "trigger_price":900,"stop_underlying":910})
    assert g.allowed is False
    assert g.block_reason == "client_intraday_failed_dir_quarantine"
    os.environ["INTRADAY_CLIENT_PATTERN_WHITELIST"] = ""

def test_geometry_still_blocked():
    g = _g({"symbol":"AAPL","direction":"CALL","timeframe":"1d",
             "pattern":"2-3","tier":"A","score":78,
             "trigger_price":180,"stop_underlying":182})
    assert g.allowed is False
    assert g.block_reason == "invalid_trigger_stop_geometry"
