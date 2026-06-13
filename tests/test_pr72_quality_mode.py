"""
tests/test_pr72_quality_mode.py — PR-72 Quality Mode Gate

Covers all 7 required test scenarios from the PR spec plus the
quality_mode_result object contract (PR-72 patch):
- approved setup includes quality_mode_result.approved=True
- blocked setup includes quality_mode_result.approved=False + block_code
- disabled mode emits enabled=False consistently
"""

from __future__ import annotations

import os
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

    if "ap_quality_mode" in sys.modules:
        del sys.modules["ap_quality_mode"]
    import ap_quality_mode
    ap_quality_mode.reset_config()
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
# quality_mode_result schema helpers
# ---------------------------------------------------------------------------

_QM_RESULT_KEYS = {
    "enabled", "approved", "reason", "block_code",
    "score", "score_min", "tier", "gate_status",
    "gates_applied", "blocked_by",
}

def _assert_result_shape(result: dict, *, context: str = ""):
    """Assert all required keys are present on a quality_mode_result dict."""
    missing = _QM_RESULT_KEYS - set(result.keys())
    assert not missing, f"{context}: quality_mode_result missing keys: {missing}"


# ---------------------------------------------------------------------------
# 1. Score 69 is blocked
# ---------------------------------------------------------------------------

def test_score_69_blocked():
    qm = _reload_qm()
    v = _call(qm, _signal(score=69.0, signal_id="sig-69"), score_override=69.0)
    assert not v.allowed, "score 69 must be blocked"
    assert v.log_code == "QUALITY_MODE_BLOCKED_SCORE"
    assert v.meta["quality_mode_blocked"] is True
    assert v.meta["score"] == 69.0
    assert "gate_status" in v.meta

    # quality_mode_result contract
    r = v.quality_mode_result
    _assert_result_shape(r, context="score_69_blocked")
    assert r["enabled"] is True
    assert r["approved"] is False
    assert r["block_code"] == "QUALITY_MODE_BLOCKED_SCORE"
    assert r["score"] == 69.0
    assert r["score_min"] == 70.0
    assert r["blocked_by"] is not None
    assert isinstance(r["gates_applied"], list)
    assert len(r["gates_applied"]) >= 1

    # Must be present in meta too
    assert "quality_mode_result" in v.meta
    assert v.meta["quality_mode_result"] is r


# ---------------------------------------------------------------------------
# 2. Score 70 allowed — quality_mode_result.approved=True
# ---------------------------------------------------------------------------

def test_score_70_allowed():
    qm = _reload_qm()
    v = _call(qm, _signal(score=70.0, signal_id="sig-70-ok"), score_override=70.0)
    assert v.allowed, f"score 70 must be allowed; got: {v.quality_mode_reason}"
    assert v.log_code == "QUALITY_MODE_APPROVED"
    assert v.meta.get("quality_mode_blocked") is False

    r = v.quality_mode_result
    _assert_result_shape(r, context="score_70_allowed")
    assert r["enabled"] is True
    assert r["approved"] is True
    assert r["block_code"] is None
    assert r["blocked_by"] is None
    assert r["score"] == 70.0
    assert r["score_min"] == 70.0
    assert r["gate_status"] == "APPROVED"
    assert isinstance(r["gates_applied"], list)
    assert len(r["gates_applied"]) >= 1

    assert "quality_mode_result" in v.meta
    assert v.meta["quality_mode_result"] is r


# ---------------------------------------------------------------------------
# 3. SKIP_OVERRIDE blocked
# ---------------------------------------------------------------------------

def test_skip_override_blocked():
    qm = _reload_qm()
    v = _call(qm, _signal(score=80.0, signal_id="sig-skip"),
              intel_status="SKIP_OVERRIDE", score_override=80.0)
    assert not v.allowed
    assert v.log_code == "QUALITY_MODE_BLOCKED_OVERRIDE"
    assert "SKIP_OVERRIDE" in v.quality_mode_reason

    r = v.quality_mode_result
    _assert_result_shape(r, context="skip_override")
    assert r["approved"] is False
    assert r["block_code"] == "QUALITY_MODE_BLOCKED_OVERRIDE"
    assert r["blocked_by"] == "skip_override"
    assert v.meta["quality_mode_result"] is r


# ---------------------------------------------------------------------------
# 4a. RISK_VETO_OVERRIDE blocked below score 78
# ---------------------------------------------------------------------------

def test_risk_veto_override_blocked_below_78():
    qm = _reload_qm()
    v = _call(qm, _signal(score=77.0, signal_id="sig-rvo-low"),
              intel_status="RISK_VETO_OVERRIDE", score_override=77.0)
    assert not v.allowed
    assert v.log_code == "QUALITY_MODE_BLOCKED_OVERRIDE"

    r = v.quality_mode_result
    _assert_result_shape(r, context="rvo_below_78")
    assert r["approved"] is False
    assert r["blocked_by"] == "risk_veto_override"
    assert r["block_code"] == "QUALITY_MODE_BLOCKED_OVERRIDE"


# ---------------------------------------------------------------------------
# 4b. RISK_VETO_OVERRIDE blocked when tier B
# ---------------------------------------------------------------------------

def test_risk_veto_override_blocked_tier_b():
    qm = _reload_qm()
    v = _call(qm, _signal(score=72.0, signal_id="sig-rvo-b"),
              intel_status="RISK_VETO_OVERRIDE", score_override=72.0)
    assert not v.allowed
    assert v.log_code == "QUALITY_MODE_BLOCKED_OVERRIDE"
    r = v.quality_mode_result
    assert r["tier"] == "B"
    assert r["approved"] is False


# ---------------------------------------------------------------------------
# 4c. RISK_VETO_OVERRIDE ALLOWED when score>=78 and tier A
# ---------------------------------------------------------------------------

def test_risk_veto_override_allowed_score_78_tier_a():
    qm = _reload_qm()
    v = _call(qm, _signal(score=78.0, signal_id="sig-rvo-ok"),
              intel_status="RISK_VETO_OVERRIDE", score_override=78.0)
    assert v.allowed, f"RVO score 78 tier A must be allowed; got {v.quality_mode_reason}"
    r = v.quality_mode_result
    assert r["approved"] is True
    assert r["tier"] == "A"


# ---------------------------------------------------------------------------
# 5. Same symbol/direction re-entry blocked
# ---------------------------------------------------------------------------

def test_same_symbol_direction_reentry_blocked():
    qm = _reload_qm(SAME_SYMBOL_REENTRY_COOLDOWN_MINUTES="60")
    client = "reentry_client@ap.com"

    v1 = _call(qm, _signal(score=75.0, symbol="TSLA", direction="CALL",
                            signal_id="sig-tsla-1"), client_id=client, score_override=75.0)
    assert v1.allowed, "first TSLA CALL must be allowed"
    assert v1.quality_mode_result["approved"] is True

    v2 = _call(qm, _signal(score=80.0, symbol="TSLA", direction="CALL",
                            signal_id="sig-tsla-2"), client_id=client, score_override=80.0)
    assert not v2.allowed, "re-entry within cooldown must be blocked"
    assert v2.log_code == "QUALITY_MODE_BLOCKED_REENTRY"

    r = v2.quality_mode_result
    _assert_result_shape(r, context="reentry")
    assert r["approved"] is False
    assert r["blocked_by"] == "reentry_cooldown"
    assert "cooldown" in v2.quality_mode_reason.lower()


def test_different_direction_not_blocked_by_reentry():
    qm = _reload_qm()
    client = "reentry2@ap.com"
    v1 = _call(qm, _signal(score=75.0, symbol="NVDA", direction="CALL",
                            signal_id="sig-nvda-call"), client_id=client, score_override=75.0)
    assert v1.allowed

    v2 = _call(qm, _signal(score=75.0, symbol="NVDA", direction="PUT",
                            signal_id="sig-nvda-put"), client_id=client, score_override=75.0)
    assert v2.allowed, "different direction on same symbol must not trigger reentry block"
    assert v2.quality_mode_result["approved"] is True


# ---------------------------------------------------------------------------
# 6. Duplicate signal_id blocked
# ---------------------------------------------------------------------------

def test_duplicate_signal_id_gate_removed():
    """P0 Final Duplicate Honesty: quality_mode no longer blocks on
    duplicate signal_id. Master_control._has_durable_duplicate_signal is
    the sole authority. Quality_mode must allow same signal_id through."""
    qm = _reload_qm()
    client = "dup_client@ap.com"

    v1 = _call(qm, _signal(score=75.0, symbol="MSFT", signal_id="dup-sig-999"),
               client_id=client, score_override=75.0)
    assert v1.allowed

    # Same signal_id, different symbol — quality_mode used to block. After
    # PR #134 it must NOT block; master_control will gate via durable check.
    v2 = _call(qm, _signal(score=75.0, symbol="AMZN", signal_id="dup-sig-999"),
               client_id=client, score_override=75.0)
    assert v2.allowed, (
        "quality_mode must NOT block on duplicate signal_id — that gate "
        "was removed. Got blocked_by="
        f"{v2.quality_mode_result.get('blocked_by') if v2.quality_mode_result else None}"
    )
    # And the result must NOT carry blocked_by=duplicate_signal_id
    r = v2.quality_mode_result
    if r is not None:
        assert r.get("blocked_by") != "duplicate_signal_id", (
            "quality_mode_result must not surface blocked_by=duplicate_signal_id"
        )


# ---------------------------------------------------------------------------
# 7. Max B-tier trades blocks after limit
# ---------------------------------------------------------------------------

def test_max_b_tier_blocks_after_limit():
    qm = _reload_qm(QUALITY_MODE_MAX_B_TIER_TRADES="2")
    client = "btier_client@ap.com"

    v1 = _call(qm, _signal(score=70.0, signal_id="bt1"),
               client_id=client, score_override=70.0)
    assert v1.allowed
    assert v1.quality_mode_result["approved"] is True

    v2 = _call(qm, _signal(score=70.0, symbol="TSLA", signal_id="bt2"),
               client_id=client, score_override=70.0)
    assert v2.allowed

    v3 = _call(qm, _signal(score=70.0, symbol="NVDA", signal_id="bt3"),
               client_id=client, score_override=70.0)
    assert not v3.allowed
    assert v3.log_code == "QUALITY_MODE_BLOCKED_B_TIER_LIMIT"

    r = v3.quality_mode_result
    _assert_result_shape(r, context="b_tier_limit")
    assert r["approved"] is False
    assert r["blocked_by"] == "b_tier_daily_cap"
    assert r["block_code"] == "QUALITY_MODE_BLOCKED_B_TIER_LIMIT"
    assert "B-tier daily limit" in v3.quality_mode_reason


def test_a_tier_not_limited_by_b_tier_cap():
    qm = _reload_qm(QUALITY_MODE_MAX_B_TIER_TRADES="0")
    v = _call(qm, _signal(score=80.0, signal_id="a-tier-1"), score_override=80.0)
    assert v.allowed
    assert v.quality_mode_result["tier"] == "A"
    assert v.quality_mode_result["approved"] is True


# ---------------------------------------------------------------------------
# Disabled Quality Mode — enabled=False consistently
# ---------------------------------------------------------------------------

def test_quality_mode_disabled_enabled_false():
    """When QUALITY_MODE_ENABLED=false, quality_mode_result.enabled must be False."""
    qm = _reload_qm(QUALITY_MODE_ENABLED="false")
    v = _call(qm, _signal(score=0.0, signal_id="disabled"), score_override=0.0)
    assert v.allowed
    assert v.log_code == "QUALITY_MODE_DISABLED"

    r = v.quality_mode_result
    _assert_result_shape(r, context="disabled")
    assert r["enabled"] is False
    assert r["approved"] is None      # not evaluated — not True or False
    assert r["block_code"] is None
    assert r["blocked_by"] is None
    assert r["gate_status"] == "DISABLED"
    assert r["gates_applied"] == []

    # meta must expose it too
    assert "quality_mode_result" in v.meta
    assert v.meta["quality_mode_result"]["enabled"] is False


def test_build_disabled_result_standalone():
    """build_disabled_result() produces valid schema for PR-73 null-safe use."""
    qm = _reload_qm(QUALITY_MODE_ENABLED="false")
    r = qm.build_disabled_result()
    _assert_result_shape(r, context="build_disabled_result")
    assert r["enabled"] is False
    assert r["approved"] is None
    assert r["gate_status"] == "DISABLED"
    assert r["gates_applied"] == []


# ---------------------------------------------------------------------------
# gates_applied ordering and content
# ---------------------------------------------------------------------------

def test_gates_applied_ordered_and_nonempty_on_approve():
    qm = _reload_qm()
    v = _call(qm, _signal(score=75.0, signal_id="gates-ok"), score_override=75.0)
    assert v.allowed
    r = v.quality_mode_result
    assert len(r["gates_applied"]) >= 8   # all non-state gates ran
    # First gate must always be score_floor
    assert r["gates_applied"][0] == "score_floor"


def test_gates_applied_stops_at_block():
    """Blocked at gate 1 (score_floor) — only one gate listed."""
    qm = _reload_qm()
    v = _call(qm, _signal(score=50.0, signal_id="gates-block"), score_override=50.0)
    assert not v.allowed
    r = v.quality_mode_result
    assert r["gates_applied"] == ["score_floor"]
    assert r["blocked_by"] == "score_floor"


# ---------------------------------------------------------------------------
# Additional coverage
# ---------------------------------------------------------------------------

def test_score_exactly_at_65_blocked():
    qm = _reload_qm()
    v = _call(qm, _signal(score=65.0, signal_id="s65"), score_override=65.0)
    assert not v.allowed
    assert v.log_code == "QUALITY_MODE_BLOCKED_SCORE"
    r = v.quality_mode_result
    assert r["approved"] is False
    assert r["score"] == 65.0


def test_max_daily_trades_blocks():
    qm = _reload_qm(QUALITY_MODE_MAX_DAILY_TRADES="3")
    v = _call(qm, _signal(score=75.0, signal_id="dt-1"), daily=3, score_override=75.0)
    assert not v.allowed
    r = v.quality_mode_result
    assert r["approved"] is False
    assert r["blocked_by"] == "max_daily_trades"


def test_max_positions_blocks():
    qm = _reload_qm(QUALITY_MODE_MAX_POSITIONS="4")
    v = _call(qm, _signal(score=75.0, signal_id="pos-1"), positions=4, score_override=75.0)
    assert not v.allowed
    r = v.quality_mode_result
    assert r["approved"] is False
    assert r["blocked_by"] == "max_positions"


def test_meta_fields_on_block_no_forbidden_schema():
    """Confirm no legacy schema fields leak into meta."""
    qm = _reload_qm()
    v = _call(qm, _signal(score=60.0, symbol="SPY", direction="PUT", timeframe="1h",
                           pattern="3-3", signal_id="meta-test"), score_override=60.0)
    assert not v.allowed
    m = v.meta
    assert m["score"] == 60.0
    assert m["symbol"] == "SPY"
    assert m["direction"] == "PUT"
    assert m["timeframe"] == "1h"
    assert m["pattern"] == "3-3"
    assert m["quality_mode_blocked"] is True
    assert "gate_status" in m
    assert "quality_mode_result" in m
    # Must not contain forbidden legacy fields
    for bad_key in ("side", "ticker", "created_at", "rejection_reason"):
        assert bad_key not in m, f"Forbidden key '{bad_key}' found in meta"


def test_quality_mode_result_in_both_meta_paths():
    """quality_mode_result must be present directly on meta and nested."""
    qm = _reload_qm()
    v = _call(qm, _signal(score=75.0, signal_id="both-paths"), score_override=75.0)
    assert v.allowed
    # Direct meta path
    assert "quality_mode_result" in v.meta
    # The object on meta must be the same as the top-level result
    assert v.meta["quality_mode_result"] is v.quality_mode_result
