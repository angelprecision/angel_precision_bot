"""
tests/test_pr72_quality_mode.py — PR-72 Quality Mode Gate

Covers all 7 required test scenarios from the PR spec:
1. score 69 blocked
2. score 70 allowed if other gates pass
3. SKIP_OVERRIDE blocked
4. RISK_VETO_OVERRIDE blocked below 78
5. same symbol/direction re-entry blocked
6. duplicate signal_id blocked
7. max B-tier count blocks after limit
"""

from __future__ import annotations

import os
import importlib
import sys
import time
import pytest


# ---------------------------------------------------------------------------
# Helpers — reload the module with fresh env so tests are isolated
# ---------------------------------------------------------------------------

def _reload_qm(**env_overrides):
    """Set env vars, (re)import ap_quality_mode, reset config, return module."""
    defaults = {
        "QUALITY_MODE_ENABLED": "true",
        "QUALITY_MODE_SCORE_MIN": "70",
        "QUALITY_MODE_MAX_DAILY_TRADES": "5",
        "QUALITY_MODE_MAX_POSITIONS": "4",
        "QUALITY_MODE_MAX_B_TIER_TRADES": "2",
        "SAME_SYMBOL_REENTRY_COOLDOWN_MINUTES": "60",
        "PYRAMIDING_ENABLED": "false",
    }
    merged = {**defaults, **env_overrides}
    for k, v in merged.items():
        os.environ[k] = v

    # Force reimport so module-level config is fresh
    if "ap_quality_mode" in sys.modules:
        del sys.modules["ap_quality_mode"]
    import ap_quality_mode
    ap_quality_mode.reset_config()
    # Also clear per-client state between tests
    with ap_quality_mode._state_lock:
        ap_quality_mode._client_states.clear()
    return ap_quality_mode


def _signal(
    score: float = 75.0,
    symbol: str = "AAPL",
    direction: str = "CALL",
    timeframe: str = "1d",
    pattern: str = "3-1-2",
    signal_id: str = "sig-001",
    plan_id: str = "",
) -> dict:
    return {
        "score": score,
        "symbol": symbol,
        "direction": direction,
        "timeframe": timeframe,
        "pattern": pattern,
        "signal_id": signal_id,
        "plan_id": plan_id,
    }


def _call(qm, sig, client_id="test_client@ap.com",
          intel_status=None, daily=0, positions=0, score_override=None):
    return qm.check(
        signal=sig,
        client_id=client_id,
        intel_status=intel_status,
        daily_trades_today=daily,
        open_positions=positions,
        approved_score=score_override if score_override is not None else float(sig.get("score", 75)),
    )


# ---------------------------------------------------------------------------
# 1. Score 69 is blocked
# ---------------------------------------------------------------------------

def test_score_69_blocked():
    qm = _reload_qm()
    sig = _signal(score=69.0, signal_id="sig-69")
    v = _call(qm, sig, score_override=69.0)
    assert not v.allowed, "score 69 must be blocked"
    assert v.log_code == "QUALITY_MODE_BLOCKED_SCORE"
    assert v.meta["quality_mode_blocked"] is True
    assert v.meta["score"] == 69.0
    assert v.meta["gate_status"] in ("BLOCKED_SCORE",)


# ---------------------------------------------------------------------------
# 2. Score 70 allowed (when other gates pass)
# ---------------------------------------------------------------------------

def test_score_70_allowed():
    qm = _reload_qm()
    sig = _signal(score=70.0, signal_id="sig-70-ok")
    v = _call(qm, sig, score_override=70.0)
    assert v.allowed, f"score 70 must be allowed; got: {v.quality_mode_reason}"
    assert v.log_code == "QUALITY_MODE_APPROVED"
    assert v.meta.get("quality_mode_blocked") is False


# ---------------------------------------------------------------------------
# 3. SKIP_OVERRIDE blocked
# ---------------------------------------------------------------------------

def test_skip_override_blocked():
    qm = _reload_qm()
    sig = _signal(score=80.0, signal_id="sig-skip")
    v = _call(qm, sig, intel_status="SKIP_OVERRIDE", score_override=80.0)
    assert not v.allowed
    assert v.log_code == "QUALITY_MODE_BLOCKED_OVERRIDE"
    assert "SKIP_OVERRIDE" in v.quality_mode_reason
    assert v.meta.get("intel_status") == "SKIP_OVERRIDE"


# ---------------------------------------------------------------------------
# 4a. RISK_VETO_OVERRIDE blocked below score 78
# ---------------------------------------------------------------------------

def test_risk_veto_override_blocked_below_78():
    qm = _reload_qm()
    sig = _signal(score=77.0, signal_id="sig-rvo-low")
    v = _call(qm, sig, intel_status="RISK_VETO_OVERRIDE", score_override=77.0)
    assert not v.allowed
    assert v.log_code == "QUALITY_MODE_BLOCKED_OVERRIDE"
    assert "RISK_VETO_OVERRIDE" in v.quality_mode_reason


# ---------------------------------------------------------------------------
# 4b. RISK_VETO_OVERRIDE blocked when tier B (score 72, no A tier)
# ---------------------------------------------------------------------------

def test_risk_veto_override_blocked_tier_b():
    qm = _reload_qm()
    sig = _signal(score=72.0, signal_id="sig-rvo-b")
    # score=72 → tier B (75 threshold for A). Even though score>=70, tier != A/A+
    v = _call(qm, sig, intel_status="RISK_VETO_OVERRIDE", score_override=72.0)
    assert not v.allowed
    assert v.log_code == "QUALITY_MODE_BLOCKED_OVERRIDE"


# ---------------------------------------------------------------------------
# 4c. RISK_VETO_OVERRIDE ALLOWED when score>=78 and tier A
# ---------------------------------------------------------------------------

def test_risk_veto_override_allowed_score_78_tier_a():
    qm = _reload_qm()
    sig = _signal(score=78.0, signal_id="sig-rvo-ok")
    # score 78 → Tier A (75 threshold).  Should pass RVO gate.
    v = _call(qm, sig, intel_status="RISK_VETO_OVERRIDE", score_override=78.0)
    assert v.allowed, f"RVO with score 78 + tier A must be allowed; got {v.quality_mode_reason}"


# ---------------------------------------------------------------------------
# 5. Same symbol/direction re-entry blocked within cooldown
# ---------------------------------------------------------------------------

def test_same_symbol_direction_reentry_blocked():
    qm = _reload_qm(SAME_SYMBOL_REENTRY_COOLDOWN_MINUTES="60")
    # First entry: should pass
    sig1 = _signal(score=75.0, symbol="TSLA", direction="CALL", signal_id="sig-tsla-1")
    v1 = _call(qm, sig1, score_override=75.0)
    assert v1.allowed, "first TSLA CALL must be allowed"

    # Second entry same symbol/direction immediately: should be blocked
    sig2 = _signal(score=80.0, symbol="TSLA", direction="CALL", signal_id="sig-tsla-2")
    v2 = _call(qm, sig2, score_override=80.0)
    assert not v2.allowed, "re-entry within cooldown must be blocked"
    assert v2.log_code == "QUALITY_MODE_BLOCKED_REENTRY"
    assert "cooldown" in v2.quality_mode_reason.lower()


def test_different_direction_not_blocked_by_reentry():
    qm = _reload_qm()
    sig1 = _signal(score=75.0, symbol="NVDA", direction="CALL", signal_id="sig-nvda-call")
    v1 = _call(qm, sig1, score_override=75.0)
    assert v1.allowed

    # PUT on same symbol is a different key — should pass
    sig2 = _signal(score=75.0, symbol="NVDA", direction="PUT", signal_id="sig-nvda-put")
    v2 = _call(qm, sig2, score_override=75.0)
    assert v2.allowed, "different direction on same symbol should not trigger reentry block"


# ---------------------------------------------------------------------------
# 6. Duplicate signal_id blocked
# ---------------------------------------------------------------------------

def test_duplicate_signal_id_blocked():
    qm = _reload_qm()
    sig = _signal(score=75.0, symbol="MSFT", signal_id="dup-sig-999")
    v1 = _call(qm, sig, score_override=75.0)
    assert v1.allowed

    # Second call with identical signal_id — different symbol so reentry won't fire first
    sig2 = _signal(score=75.0, symbol="AMZN", signal_id="dup-sig-999")  # same signal_id, different symbol
    v2 = _call(qm, sig2, score_override=75.0)
    assert not v2.allowed
    assert v2.log_code == "QUALITY_MODE_BLOCKED_REENTRY"
    # reason may be "duplicate signal_id=..." or meta has "duplicate_signal_id"
    assert "duplicate" in v2.quality_mode_reason.lower() and "signal_id" in v2.quality_mode_reason.lower()


# ---------------------------------------------------------------------------
# 7. Max B-tier trades per day blocks after limit
# ---------------------------------------------------------------------------

def test_max_b_tier_blocks_after_limit():
    qm = _reload_qm(QUALITY_MODE_MAX_B_TIER_TRADES="2")
    # B-tier: score 65-74 → tier B per tier engine thresholds
    client = "btier_client@ap.com"

    v1 = _call(qm, _signal(score=70.0, signal_id="bt1"), client_id=client, score_override=70.0)
    assert v1.allowed, "first B-tier must be allowed"

    v2 = _call(qm, _signal(score=70.0, symbol="TSLA", signal_id="bt2"), client_id=client, score_override=70.0)
    assert v2.allowed, "second B-tier must be allowed"

    v3 = _call(qm, _signal(score=70.0, symbol="NVDA", signal_id="bt3"), client_id=client, score_override=70.0)
    assert not v3.allowed, "third B-tier must be blocked after limit of 2"
    assert v3.log_code == "QUALITY_MODE_BLOCKED_B_TIER_LIMIT"
    assert "B-tier daily limit" in v3.quality_mode_reason


def test_a_tier_not_limited_by_b_tier_cap():
    qm = _reload_qm(QUALITY_MODE_MAX_B_TIER_TRADES="0")  # zero B-tier allowed
    # Score 80 → tier A — should still be approved
    v = _call(qm, _signal(score=80.0, signal_id="a-tier-1"), score_override=80.0)
    assert v.allowed, "A-tier must not be blocked by B-tier cap"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

def test_quality_mode_disabled_passes_everything():
    qm = _reload_qm(QUALITY_MODE_ENABLED="false")
    v = _call(qm, _signal(score=0.0, signal_id="disabled"), score_override=0.0)
    assert v.allowed
    assert v.log_code == "QUALITY_MODE_DISABLED"


def test_score_exactly_at_65_blocked():
    qm = _reload_qm()
    v = _call(qm, _signal(score=65.0, signal_id="s65"), score_override=65.0)
    assert not v.allowed
    assert v.log_code == "QUALITY_MODE_BLOCKED_SCORE"
    # score 65 < 70 min — caught by gate 1 or gate 4
    assert "65" in v.quality_mode_reason or "floor" in v.quality_mode_reason or "min" in v.quality_mode_reason


def test_max_daily_trades_blocks():
    qm = _reload_qm(QUALITY_MODE_MAX_DAILY_TRADES="3")
    v = _call(qm, _signal(score=75.0, signal_id="dt-1"), daily=3, score_override=75.0)
    assert not v.allowed
    assert "max daily trades" in v.quality_mode_reason.lower()


def test_max_positions_blocks():
    qm = _reload_qm(QUALITY_MODE_MAX_POSITIONS="4")
    v = _call(qm, _signal(score=75.0, signal_id="pos-1"), positions=4, score_override=75.0)
    assert not v.allowed
    assert "max concurrent positions" in v.quality_mode_reason.lower()


def test_meta_fields_on_block():
    qm = _reload_qm()
    sig = _signal(score=60.0, symbol="SPY", direction="PUT", timeframe="1h",
                  pattern="3-3", signal_id="meta-test")
    v = _call(qm, sig, score_override=60.0)
    assert not v.allowed
    m = v.meta
    assert m["score"] == 60.0
    assert m["symbol"] == "SPY"
    assert m["direction"] == "PUT"
    assert m["timeframe"] == "1h"
    assert m["pattern"] == "3-3"
    assert m["quality_mode_blocked"] is True
    assert "gate_status" in m
    # Real schema fields used (no 'side', 'ticker', 'created_at', 'rejection_reason')
    assert "side" not in m
    assert "ticker" not in m
    assert "created_at" not in m
    assert "rejection_reason" not in m
