"""
tests/test_p0_live_regime_veto_taxonomy.py
==========================================
P0 taxonomy fix — regime mismatch must:
  1. Not return early; all hard gates still run.
  2. Never become RISK_VETO.
  3. Never manufacture contracts from zero.
Tests 1-6 from spec plus gateway invariants.
"""
from __future__ import annotations
import pytest
from unittest.mock import MagicMock, patch


# ─── Fixtures ─────────────────────────────────────────────────────────────────

def _spy(trend):
    return {"trend": trend}

def _vix(tradeable=True, value=18.0):
    return {"vix": value, "tradeable": tradeable}

def _rm(portfolio_value=25000):
    from ap_intelligence.agents.ap_risk_manager import APRiskManager
    rm = APRiskManager(portfolio_value=portfolio_value)
    rm.allow_0dte = True
    return rm

_CALL_KWARGS = dict(
    ticker="AAPL", direction="bullish", underlying_price=190.0,
    atr_value=2.5, option_delta=0.45, option_premium=2.50,
    bid_ask_spread_pct=0.05, open_interest=500, daily_volume_options=200, dte=1,
)
_PUT_KWARGS = dict(
    ticker="MSFT", direction="bearish", underlying_price=420.0,
    atr_value=5.0, option_delta=0.45, option_premium=3.00,
    bid_ask_spread_pct=0.05, open_interest=500, daily_volume_options=200, dte=1,
)


# ─── Test 1: Regime mismatch does NOT return early — VIX still called ─────────

def test_regime_mismatch_does_not_skip_vix():
    """SPY bear + bullish CALL must NOT return before calling the VIX gate."""
    rm = _rm()
    vix_called = []

    def fake_vix():
        vix_called.append(True)
        return _vix(tradeable=False, value=45.0)  # untradeable — should block

    with (
        patch("ap_intelligence.agents.ap_risk_manager.get_spy_trend", return_value=_spy("BEAR")),
        patch("ap_intelligence.agents.ap_risk_manager.get_vix", side_effect=fake_vix),
    ):
        result = rm.evaluate(**_CALL_KWARGS)

    assert vix_called, "VIX gate was never called — regime mismatch returned early (bug)"
    assert result.approved is False
    assert result.reason_code == "VIX_POLICY_HARD_CAP", (
        f"Expected VIX_POLICY_HARD_CAP, got {result.reason_code!r}"
    )
    assert result.hard_veto is True


# ─── Test 2: Regime mismatch does not skip contract-quality validation ─────────

def test_regime_mismatch_does_not_skip_contract_quality():
    """Bad spread must still block even with SPY regime mismatch."""
    rm = _rm()
    with (
        patch("ap_intelligence.agents.ap_risk_manager.get_spy_trend", return_value=_spy("BEAR")),
        patch("ap_intelligence.agents.ap_risk_manager.get_vix", return_value=_vix()),
    ):
        result = rm.evaluate(
            **{**_CALL_KWARGS, "bid_ask_spread_pct": 0.25}  # 25% — fails quality
        )

    assert result.approved is False
    assert result.reason_code == "CONTRACT_QUALITY_FAILED"
    assert result.hard_veto is True


# ─── Test 3: Regime mismatch does not skip sector exposure ────────────────────

def test_regime_mismatch_does_not_skip_sector_cap():
    """Sector cap must still block even with SPY regime mismatch."""
    rm = _rm()
    # Fill sector cap: 2 TECH positions already open (MAX_SAME_SECTOR_POSITIONS=2)
    rm.open_positions = {
        "MSFT": {"direction": "bullish", "cost_usd": 2000, "sector": "tech"},
        "NVDA": {"direction": "bullish", "cost_usd": 2000, "sector": "tech"},
    }
    with (
        patch("ap_intelligence.agents.ap_risk_manager.get_spy_trend", return_value=_spy("BEAR")),
        patch("ap_intelligence.agents.ap_risk_manager.get_vix", return_value=_vix()),
    ):
        result = rm.evaluate(**_CALL_KWARGS)  # AAPL = tech sector

    assert result.approved is False
    assert result.reason_code == "SECTOR_EXPOSURE_CAP"
    assert result.hard_veto is True


# ─── Test 4: Regime mismatch does not skip capital sizing ─────────────────────

def test_regime_mismatch_does_not_skip_capital_sizing():
    """Zero affordable contracts must still block even with SPY regime mismatch."""
    rm = _rm()
    # Deploy all capital so nothing remains
    rm.open_positions = {
        "NOPE": {"direction": "bullish", "cost_usd": rm.portfolio_value * 0.81, "sector": "other"}
    }
    with (
        patch("ap_intelligence.agents.ap_risk_manager.get_spy_trend", return_value=_spy("BEAR")),
        patch("ap_intelligence.agents.ap_risk_manager.get_vix", return_value=_vix()),
        patch("ap_intelligence.agents.ap_risk_manager.get_prices",
              return_value=MagicMock(empty=True)),
    ):
        result = rm.evaluate(**_CALL_KWARGS)

    assert result.approved is False
    assert result.reason_code == "INSUFFICIENT_CAPITAL_OR_ZERO_CONTRACTS"
    assert result.hard_veto is True, (
        "INSUFFICIENT_CAPITAL_OR_ZERO_CONTRACTS must be hard_veto=True"
    )


# ─── Test 5: Regime mismatch with good sizing returns APPROVED_WITH_REGIME_MISMATCH ──

def test_regime_mismatch_with_good_sizing_returns_approved():
    """When all hard gates pass, regime mismatch must return approved=True."""
    import pandas as pd
    import numpy as np

    rm = _rm()
    fake_prices = pd.DataFrame(
        {"close": np.linspace(180, 190, 30)},
        index=pd.date_range("2026-06-01", periods=30)
    )

    with (
        patch("ap_intelligence.agents.ap_risk_manager.get_spy_trend", return_value=_spy("BEAR")),
        patch("ap_intelligence.agents.ap_risk_manager.get_vix", return_value=_vix()),
        patch("ap_intelligence.agents.ap_risk_manager.get_prices", return_value=fake_prices),
    ):
        result = rm.evaluate(**_CALL_KWARGS)

    assert result.approved is True, (
        f"Expected approved=True (regime mismatch advisory), got False: {result.reason}"
    )
    assert result.reason_code == "APPROVED_WITH_REGIME_MISMATCH"
    assert result.hard_veto is False
    assert result.veto_category == "directional_context"
    assert result.max_contracts >= 1, "Must have real contract count, not zero"
    assert result.max_position_usd > 0


# ─── Test 6: PUT version — bearish + SPY BULL → APPROVED_WITH_REGIME_MISMATCH ──

def test_spy_bull_put_with_good_sizing_returns_approved():
    import pandas as pd
    import numpy as np

    rm = _rm()
    fake_prices = pd.DataFrame(
        {"close": np.linspace(410, 420, 30)},
        index=pd.date_range("2026-06-01", periods=30)
    )

    with (
        patch("ap_intelligence.agents.ap_risk_manager.get_spy_trend", return_value=_spy("BULL")),
        patch("ap_intelligence.agents.ap_risk_manager.get_vix", return_value=_vix()),
        patch("ap_intelligence.agents.ap_risk_manager.get_prices", return_value=fake_prices),
    ):
        result = rm.evaluate(**_PUT_KWARGS)

    assert result.approved is True
    assert result.reason_code == "APPROVED_WITH_REGIME_MISMATCH"
    assert result.hard_veto is False
    assert result.max_contracts >= 1


# ─── Test 7: Bridge never manufactures contracts from zero ────────────────────

def test_bridge_never_manufactures_contracts_from_zero():
    """
    Even if risk.reason_code=APPROVED_WITH_REGIME_MISMATCH, if PM calculated
    0 contracts, the bridge must NOT return contracts=1.  It falls through to
    the standard APPROVED path.
    """
    from intelligence_bridge import _map_result

    # PM returned 0 contracts (low score, reduced position)
    pipeline_result = {
        "action":     "execute",
        "confidence": 78.0,
        "score":      78.0,
        "contracts":  0,      # PM produced zero
        "reasoning":  "regime_mismatch_advisory",
        "risk_detail": {
            "approved":          True,
            "reason_code":       "APPROVED_WITH_REGIME_MISMATCH",
            "veto_category":     "directional_context",
            "hard_veto":         False,
            "max_contracts":     2,
            "max_position_usd":  500.0,
        },
        "ticker": "AAPL",
    }

    gate = _map_result(pipeline_result, fallback_score=78.0)

    # Must NOT be REGIME_MISMATCH_ADVISORY when contracts=0
    assert gate["intel_status"] in ("APPROVED",), (
        f"With PM contracts=0, bridge must fall through to APPROVED, "
        f"not manufacture contracts. Got: {gate['intel_status']} contracts={gate['contracts']}"
    )


# ─── Test 8: Bridge advisory only when approved=True with real contracts ───────

def test_bridge_advisory_requires_real_contracts():
    """REGIME_MISMATCH_ADVISORY only fires when PM gave real contract count >= 1."""
    from intelligence_bridge import _map_result

    pipeline_result = {
        "action":     "execute",
        "confidence": 78.0,
        "score":      78.0,
        "contracts":  3,   # PM produced real contracts
        "reasoning":  "all_gates_passed",
        "risk_detail": {
            "approved":          True,
            "reason_code":       "APPROVED_WITH_REGIME_MISMATCH",
            "veto_category":     "directional_context",
            "hard_veto":         False,
            "max_contracts":     3,
            "max_position_usd":  750.0,
        },
        "ticker": "AAPL",
    }

    gate = _map_result(pipeline_result, fallback_score=78.0)

    assert gate["intel_status"] == "REGIME_MISMATCH_ADVISORY"
    assert gate["approved"] is True
    assert gate["contracts"] == 3, (
        f"Must preserve exact PM contract count, got {gate['contracts']}"
    )


# ─── Test 9: Daily kill switch is still authoritative RISK_VETO ───────────────

def test_kill_switch_remains_authoritative():
    from intelligence_bridge import _risk_allows_trade, _map_result
    import os

    risk = {
        "approved":     False,
        "reason":       "DAILY KILL SWITCH: P&L = $-1500",
        "reason_code":  "DAILY_LOSS_KILL_SWITCH",
        "hard_veto":    True,
        "max_contracts": 0, "max_position_usd": 0.0,
    }
    ok, _ = _risk_allows_trade(risk)
    assert ok is False

    pipeline_result = {
        "action": "skip", "confidence": 75.0, "score": 75.0, "contracts": 0,
        "reasoning": "daily_kill_switch", "risk_detail": risk, "ticker": "AAPL",
    }
    with patch.dict(os.environ, {"INTEL_ENFORCE_RISK_VETO": "1", "AP_MODE": "live"}):
        gate = _map_result(pipeline_result, fallback_score=78.0)
    assert gate["intel_status"] == "RISK_VETO"
    assert gate["approved"] is False


# ─── Test 10: Admission policy — APPROVED_WITH_REGIME_MISMATCH → advisory ─────

def test_admission_policy_regime_mismatch_non_authoritative():
    from ap.intelligence_admission_policy import (
        adjudicate_intelligence_result,
        INTEL_REGIME_MISMATCH_ADVISORY,
    )
    intel_result = {
        "approved":     True,
        "intel_status": "REGIME_MISMATCH_ADVISORY",
        "score":        78.0, "contracts": 2,
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


# ─── Test 11: INSUFFICIENT_CAPITAL hard_veto=True confirmed ──────────────────

def test_insufficient_capital_is_hard_veto():
    from ap_intelligence.agents.ap_risk_manager import APRiskManager
    import pandas as pd, numpy as np

    rm = APRiskManager(portfolio_value=25000)
    # Fill capital so nothing remains
    rm.open_positions = {"X": {"direction": "bullish", "cost_usd": 21000.0, "sector": "other"}}
    fake_prices = pd.DataFrame(
        {"close": np.linspace(180, 190, 30)},
        index=pd.date_range("2026-06-01", periods=30)
    )
    with (
        patch("ap_intelligence.agents.ap_risk_manager.get_spy_trend", return_value=_spy("BEAR")),
        patch("ap_intelligence.agents.ap_risk_manager.get_vix", return_value=_vix()),
        patch("ap_intelligence.agents.ap_risk_manager.get_prices", return_value=fake_prices),
    ):
        result = rm.evaluate(**_CALL_KWARGS)

    assert result.approved is False
    assert result.hard_veto is True
    assert result.reason_code == "INSUFFICIENT_CAPITAL_OR_ZERO_CONTRACTS"
