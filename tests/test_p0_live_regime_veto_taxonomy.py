"""
tests/test_p0_live_regime_veto_taxonomy.py
==========================================
PR #379 taxonomy fix — regime mismatch must never become an authoritative RISK_VETO.

Tests 1–6 from the PR spec.
"""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch
from dataclasses import asdict


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _make_risk_result(
    *,
    approved=True,
    reason="APPROVED",
    reason_code="APPROVED",
    veto_category="",
    hard_veto=False,
    spy_trend="BULL",
    vix=18.0,
    max_contracts=2,
):
    """Build a minimal RiskResult-like dict for bridge / pipeline tests."""
    return {
        "approved":       approved,
        "reason":         reason,
        "reason_code":    reason_code,
        "veto_category":  veto_category,
        "hard_veto":      hard_veto,
        "spy_trend":      spy_trend,
        "vix":            vix,
        "max_contracts":  max_contracts,
        "max_position_usd": 500.0,
    }


def _risk_manager_side_effects(*, direction, spy_trend):
    """
    Simulate what APRiskManager.evaluate() returns for regime mismatch vs hard veto.
    Returns a mock that exposes the new structured authority attributes.
    """
    m = MagicMock()
    if direction == "bullish" and spy_trend == "BEAR":
        m.approved       = False
        m.reason         = "CALL blocked — SPY in BEAR trend"
        m.reason_code    = "MARKET_REGIME_MISMATCH"
        m.veto_category  = "directional_context"
        m.hard_veto      = False
        m.spy_trend      = "BEAR"
        m.vix            = 18.0
    elif direction == "bearish" and spy_trend == "BULL":
        m.approved       = False
        m.reason         = "PUT blocked — SPY in BULL trend"
        m.reason_code    = "MARKET_REGIME_MISMATCH"
        m.veto_category  = "directional_context"
        m.hard_veto      = False
        m.spy_trend      = "BULL"
        m.vix            = 18.0
    elif direction == "kill_switch":
        m.approved       = False
        m.reason         = "DAILY KILL SWITCH: P&L = $-1500"
        m.reason_code    = "DAILY_LOSS_KILL_SWITCH"
        m.veto_category  = "account_safety"
        m.hard_veto      = True
        m.spy_trend      = "BULL"
        m.vix            = 18.0
    elif direction == "contract_quality":
        m.approved       = False
        m.reason         = "CONTRACT QUALITY: spread 20% > 14%"
        m.reason_code    = "CONTRACT_QUALITY_FAILED"
        m.veto_category  = "execution_quality"
        m.hard_veto      = True
        m.spy_trend      = "BULL"
        m.vix            = 18.0
    else:
        m.approved       = True
        m.reason         = "APPROVED"
        m.reason_code    = "APPROVED"
        m.veto_category  = ""
        m.hard_veto      = False
        m.spy_trend      = spy_trend
        m.vix            = 18.0
    m.max_contracts      = 0 if not m.approved else 2
    m.max_position_usd   = 0.0 if not m.approved else 500.0
    m.volatility_pct     = 25.0
    m.position_limit_pct = 10.0
    m.correlation_multiplier = 1.0
    m.sector_multiplier  = 1.0
    m.risk_dollars       = 0.0
    m.stop_distance_pct  = 0.0
    m.expected_loss_per_contract = 0.0
    from unittest.mock import MagicMock as _M
    m.contract_quality   = _M(passes=True, rejection_reason="")
    return m


# ─── Test 1: SPY bullish + CALL setup → MARKET_REGIME_MISMATCH, hard_veto=False ──

def test_spy_bear_blocks_call_as_advisory():
    """SPY in BEAR trend + bullish CALL returns MARKET_REGIME_MISMATCH with hard_veto=False."""
    from ap_intelligence.agents.ap_risk_manager import APRiskManager

    rm = APRiskManager(portfolio_value=25000)

    spy_stub = {"trend": "BEAR"}
    vix_stub = {"vix": 18.0, "tradeable": True}

    with (
        patch("ap_intelligence.agents.ap_risk_manager.get_spy_trend", return_value=spy_stub),
        patch("ap_intelligence.agents.ap_risk_manager.get_vix", return_value=vix_stub),
    ):
        result = rm.evaluate(
            ticker="AAPL",
            direction="bullish",
            underlying_price=190.0,
            atr_value=2.5,
            option_delta=0.45,
            option_premium=2.50,
        )

    assert result.approved is False
    assert result.reason_code == "MARKET_REGIME_MISMATCH"
    assert result.veto_category == "directional_context"
    assert result.hard_veto is False, (
        "SPY regime mismatch must be advisory (hard_veto=False) — "
        "it is directional context, not an execution safety gate"
    )


# ─── Test 2: SPY bullish + PUT → advisory MARKET_REGIME_MISMATCH ──────────────

def test_spy_bull_blocks_put_as_advisory():
    """SPY in BULL trend + bearish PUT returns MARKET_REGIME_MISMATCH with hard_veto=False."""
    from ap_intelligence.agents.ap_risk_manager import APRiskManager

    rm = APRiskManager(portfolio_value=25000)

    spy_stub = {"trend": "BULL"}
    vix_stub = {"vix": 18.0, "tradeable": True}

    with (
        patch("ap_intelligence.agents.ap_risk_manager.get_spy_trend", return_value=spy_stub),
        patch("ap_intelligence.agents.ap_risk_manager.get_vix", return_value=vix_stub),
    ):
        result = rm.evaluate(
            ticker="MSFT",
            direction="bearish",
            underlying_price=420.0,
            atr_value=5.0,
            option_delta=0.45,
            option_premium=3.00,
        )

    assert result.approved is False
    assert result.reason_code == "MARKET_REGIME_MISMATCH"
    assert result.veto_category == "directional_context"
    assert result.hard_veto is False


# ─── Test 3: Advisory regime mismatch never becomes RISK_VETO ────────────────

def test_advisory_regime_mismatch_never_becomes_risk_veto():
    """
    When APRiskManager returns MARKET_REGIME_MISMATCH + hard_veto=False,
    intelligence_bridge must return intel_status=REGIME_MISMATCH_ADVISORY
    (approved=True), NEVER intel_status=RISK_VETO.
    """
    from intelligence_bridge import _risk_allows_trade, _map_result

    # Simulate risk_detail from pipeline with advisory regime mismatch
    risk_detail = _make_risk_result(
        approved=False,
        reason="CALL blocked — SPY in BEAR trend",
        reason_code="MARKET_REGIME_MISMATCH",
        veto_category="directional_context",
        hard_veto=False,
    )

    # _risk_allows_trade must return True for hard_veto=False
    risk_ok, reason = _risk_allows_trade(risk_detail)
    assert risk_ok is True, (
        "_risk_allows_trade must return True when hard_veto=False "
        f"(got risk_ok={risk_ok}, reason={reason!r})"
    )

    # Build a minimal pipeline result dict with action="skip" (PM sees approved=False)
    pipeline_result = {
        "action":     "skip",
        "confidence": 75.0,
        "score":      75.0,
        "contracts":  0,
        "reasoning":  "put_blocked_spy_bull_trend",
        "risk_detail": risk_detail,
        "ticker":     "AAPL",
    }

    gate = _map_result(pipeline_result, fallback_score=78.0)

    assert gate["intel_status"] != "RISK_VETO", (
        f"Advisory regime mismatch must NOT produce RISK_VETO. "
        f"Got: {gate['intel_status']}"
    )
    assert gate["intel_status"] == "REGIME_MISMATCH_ADVISORY", (
        f"Expected REGIME_MISMATCH_ADVISORY, got {gate['intel_status']!r}"
    )
    assert gate["approved"] is True, (
        "REGIME_MISMATCH_ADVISORY must set approved=True"
    )


# ─── Test 4: Daily kill switch is still authoritative RISK_VETO ───────────────

def test_daily_kill_switch_is_authoritative_veto():
    """Daily loss kill switch must still produce hard_veto=True → RISK_VETO."""
    from intelligence_bridge import _risk_allows_trade, _map_result

    risk_detail = _make_risk_result(
        approved=False,
        reason="DAILY KILL SWITCH: P&L = $-1500",
        reason_code="DAILY_LOSS_KILL_SWITCH",
        veto_category="account_safety",
        hard_veto=True,
    )

    risk_ok, reason = _risk_allows_trade(risk_detail)
    assert risk_ok is False, (
        "_risk_allows_trade must return False for hard_veto=True kill switch"
    )
    assert "kill" in reason.lower() or "DAILY" in reason

    pipeline_result = {
        "action":     "skip",
        "confidence": 75.0,
        "score":      75.0,
        "contracts":  0,
        "reasoning":  "daily_kill_switch",
        "risk_detail": risk_detail,
        "ticker":     "AAPL",
    }

    import os
    with patch.dict(os.environ, {"INTEL_ENFORCE_RISK_VETO": "1", "AP_MODE": "live"}):
        gate = _map_result(pipeline_result, fallback_score=78.0)

    assert gate["intel_status"] == "RISK_VETO", (
        f"Kill switch must produce RISK_VETO. Got: {gate['intel_status']}"
    )
    assert gate["approved"] is False


# ─── Test 5: Contract quality hard failure is still authoritative ─────────────

def test_contract_quality_failure_is_authoritative_veto():
    """CONTRACT_QUALITY_FAILED with hard_veto=True must produce authoritative block."""
    from intelligence_bridge import _risk_allows_trade

    risk_detail = _make_risk_result(
        approved=False,
        reason="CONTRACT QUALITY: spread 22% > 14%",
        reason_code="CONTRACT_QUALITY_FAILED",
        veto_category="execution_quality",
        hard_veto=True,
    )

    risk_ok, reason = _risk_allows_trade(risk_detail)
    assert risk_ok is False, "CONTRACT_QUALITY_FAILED with hard_veto=True must block"


# ─── Test 6: Malformed / missing hard_veto field falls back to legacy ──────────

def test_missing_hard_veto_falls_back_to_legacy_key_detection():
    """
    When hard_veto is absent, _risk_allows_trade must fall back to
    legacy approved/risk_ok key detection — fail open per canonical policy.
    """
    from intelligence_bridge import _risk_allows_trade

    # No hard_veto field — old-format risk_detail
    legacy_approved = {"approved": True, "reason": "legacy approved"}
    risk_ok, _ = _risk_allows_trade(legacy_approved)
    assert risk_ok is True, "Legacy approved=True should pass"

    legacy_rejected = {"approved": False, "reason": "legacy rejected"}
    risk_ok, _ = _risk_allows_trade(legacy_rejected)
    assert risk_ok is False, "Legacy approved=False should block"

    # Completely empty — fail open
    risk_ok, _ = _risk_allows_trade({})
    assert risk_ok is True, "Empty risk dict must fail open"

    # None input — fail open
    risk_ok, _ = _risk_allows_trade(None)  # type: ignore[arg-type]
    assert risk_ok is True, "None risk must fail open"


# ─── Admission policy tests ────────────────────────────────────────────────────

def test_admission_policy_regime_mismatch_advisory_is_allowed_non_authoritative():
    """
    INTEL_REGIME_MISMATCH_ADVISORY must produce allowed=True, authoritative=False.
    """
    from ap.intelligence_admission_policy import (
        adjudicate_intelligence_result,
        INTEL_REGIME_MISMATCH_ADVISORY,
    )

    # Simulate bridge output for advisory regime mismatch
    intel_result = {
        "approved":     True,
        "intel_status": "REGIME_MISMATCH_ADVISORY",
        "score":        78.0,
        "contracts":    2,
        "reasoning":    "regime_mismatch_advisory: CALL blocked — SPY in BEAR trend",
        "_available":   True,
    }

    verdict = adjudicate_intelligence_result(
        intel_result,
        signal={"signal_id": "test-001", "ticker": "AAPL", "side": "CALL"},
        execution_mode="live",
    )

    assert verdict.allowed is True
    assert verdict.authoritative is False
    assert verdict.reason_code == INTEL_REGIME_MISMATCH_ADVISORY


def test_admission_policy_risk_veto_is_still_authoritative():
    """RISK_VETO must still produce allowed=False, authoritative=True."""
    from ap.intelligence_admission_policy import (
        adjudicate_intelligence_result,
        INTEL_AUTHORITATIVE_VETO_RISK,
    )

    intel_result = {
        "approved":     False,
        "intel_status": "RISK_VETO",
        "score":        75.0,
        "contracts":    0,
        "reasoning":    "risk_veto: DAILY KILL SWITCH",
        "_available":   True,
    }

    verdict = adjudicate_intelligence_result(
        intel_result,
        signal={"signal_id": "test-002", "ticker": "AAPL", "side": "CALL"},
        execution_mode="live",
    )

    assert verdict.allowed is False
    assert verdict.authoritative is True
    assert verdict.reason_code == INTEL_AUTHORITATIVE_VETO_RISK
