"""
tests/test_p0_live_regime_veto_taxonomy.py
==========================================
P0 taxonomy fix — regime mismatch must:
  1. Not return early; all hard gates (VIX, contract quality, sector,
     sizing) still run in full.
  2. Pass through to APPROVED_WITH_REGIME_MISMATCH only when all gates pass.
  3. Never become RISK_VETO.
  4. Never manufacture contracts from zero — not in risk manager, not in bridge.
"""
from __future__ import annotations
import pytest
from unittest.mock import MagicMock, patch


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


# ─── Test 1: Regime mismatch does NOT return early — VIX gate still runs ──────

def test_regime_mismatch_does_not_skip_vix():
    """SPY BEAR + bullish CALL must NOT return before the VIX gate."""
    rm = _rm()
    vix_called = []

    def fake_vix():
        vix_called.append(True)
        return _vix(tradeable=False, value=45.0)  # hard-blocks

    with (
        patch("ap_intelligence.agents.ap_risk_manager.get_spy_trend", return_value=_spy("BEAR")),
        patch("ap_intelligence.agents.ap_risk_manager.get_vix", side_effect=fake_vix),
    ):
        result = rm.evaluate(**_CALL_KWARGS)

    assert vix_called, "VIX gate never called — regime mismatch returned early (regression)"
    assert result.approved is False
    assert result.reason_code == "VIX_POLICY_HARD_CAP"
    assert result.hard_veto is True


# ─── Test 2: Regime mismatch does not skip contract-quality validation ──────────

def test_regime_mismatch_does_not_skip_contract_quality():
    """Bad spread must block even with regime mismatch."""
    rm = _rm()
    with (
        patch("ap_intelligence.agents.ap_risk_manager.get_spy_trend", return_value=_spy("BEAR")),
        patch("ap_intelligence.agents.ap_risk_manager.get_vix", return_value=_vix()),
    ):
        result = rm.evaluate(**{**_CALL_KWARGS, "bid_ask_spread_pct": 0.25})

    assert result.approved is False
    assert result.reason_code == "CONTRACT_QUALITY_FAILED"
    assert result.hard_veto is True


# ─── Test 3: Regime mismatch does not skip sector exposure ────────────────────

def test_regime_mismatch_does_not_skip_sector_cap():
    """Sector cap must block even with regime mismatch."""
    rm = _rm()
    rm.open_positions = {
        "MSFT": {"direction": "bullish", "cost_usd": 2000, "sector": "tech"},
        "NVDA": {"direction": "bullish", "cost_usd": 2000, "sector": "tech"},
    }
    with (
        patch("ap_intelligence.agents.ap_risk_manager.get_spy_trend", return_value=_spy("BEAR")),
        patch("ap_intelligence.agents.ap_risk_manager.get_vix", return_value=_vix()),
    ):
        result = rm.evaluate(**_CALL_KWARGS)

    assert result.approved is False
    assert result.reason_code == "SECTOR_EXPOSURE_CAP"
    assert result.hard_veto is True


# ─── Test 4: Regime mismatch does not skip capital sizing ─────────────────────

def test_regime_mismatch_does_not_skip_capital_sizing():
    """Zero affordable contracts must block even with regime mismatch."""
    rm = _rm()
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
    assert result.hard_veto is True


# ─── Test 5: All gates pass → APPROVED_WITH_REGIME_MISMATCH ──────────────────

def test_regime_mismatch_with_good_sizing_returns_approved():
    """When all hard gates pass, regime mismatch returns approved=True."""
    import pandas as pd, numpy as np
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

    assert result.approved is True
    assert result.reason_code == "APPROVED_WITH_REGIME_MISMATCH"
    assert result.hard_veto is False
    assert result.veto_category == "directional_context"
    assert result.max_contracts >= 1
    assert result.max_position_usd > 0


# ─── Test 6: PUT version ──────────────────────────────────────────────────────

def test_spy_bull_put_with_good_sizing_returns_approved():
    import pandas as pd, numpy as np
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
    assert result.max_contracts >= 1


# ─── Test 7: Bridge uses exact PM contracts — never manufactures ─────────────

def test_bridge_uses_exact_pm_contracts():
    """APPROVED_WITH_REGIME_MISMATCH with PM contracts=3 → bridge returns 3."""
    from intelligence_bridge import _map_result
    gate = _map_result({
        "action": "execute", "confidence": 78.0, "score": 78.0, "contracts": 3,
        "reasoning": "all_gates_passed",
        "risk_detail": {
            "approved": True, "reason_code": "APPROVED_WITH_REGIME_MISMATCH",
            "hard_veto": False, "max_contracts": 3, "max_position_usd": 750.0,
        },
        "ticker": "AAPL",
    }, fallback_score=78.0)

    assert gate["intel_status"] == "REGIME_MISMATCH_ADVISORY"
    assert gate["contracts"] == 3, f"Expected 3, got {gate['contracts']}"
    assert gate["approved"] is True


# ─── Test 8: Zero PM contracts stay zero — never converted to 1 ─────────────

def test_bridge_zero_pm_contracts_stay_zero_case_a():
    """
    Case A: max_contracts=2 (risk approved real size) but PM chose 0.
    Bridge must return contracts=0, NOT 1.
    """
    from intelligence_bridge import _map_result
    gate = _map_result({
        "action": "execute", "confidence": 60.0, "score": 60.0,
        "contracts": 0,  # PM produced zero despite risk saying 2
        "reasoning": "pm_zero",
        "risk_detail": {
            "approved": True, "reason_code": "APPROVED_WITH_REGIME_MISMATCH",
            "hard_veto": False, "max_contracts": 2, "max_position_usd": 500.0,
        },
        "ticker": "AAPL",
    }, fallback_score=60.0)

    assert gate["contracts"] == 0, (
        f"Zero PM contracts must stay 0 — got {gate['contracts']}. "
        "Bridge manufactured a contract (regression)."
    )
    assert gate["intel_status"] == "REGIME_MISMATCH_ADVISORY"
    assert gate["approved"] is True   # allowed but zero-contract = no execution


def test_bridge_zero_pm_contracts_stay_zero_case_b():
    """
    Case B: risk also says max_contracts=0, max_position_usd=0.
    Bridge must STILL return contracts=0, not fall through to max(1, 0)=1.
    """
    from intelligence_bridge import _map_result
    gate = _map_result({
        "action": "execute", "confidence": 50.0, "score": 50.0,
        "contracts": 0,
        "reasoning": "zero",
        "risk_detail": {
            "approved": True, "reason_code": "APPROVED_WITH_REGIME_MISMATCH",
            "hard_veto": False, "max_contracts": 0, "max_position_usd": 0.0,
        },
        "ticker": "AAPL",
    }, fallback_score=50.0)

    assert gate["contracts"] == 0, (
        f"Must not manufacture from max_contracts=0. Got contracts={gate['contracts']}."
    )
    assert gate["intel_status"] == "REGIME_MISMATCH_ADVISORY"


# ─── Test 9: Kill switch remains authoritative RISK_VETO ─────────────────────

def test_kill_switch_remains_authoritative():
    # intelligence_bridge computes _INTEL_IS_LIVE at module import time from AP_MODE.
    # In the test environment AP_MODE is not set → _INTEL_IS_LIVE=False → allow_collect=True
    # → hard veto returns RISK_VETO_OVERRIDE (1-contract collect gate), not RISK_VETO.
    # Patch the module-level flag directly so the hard block path is exercised.
    from intelligence_bridge import _risk_allows_trade, _map_result
    import intelligence_bridge

    risk = {
        "approved": False, "reason": "DAILY KILL SWITCH",
        "reason_code": "DAILY_LOSS_KILL_SWITCH",
        "hard_veto": True, "max_contracts": 0, "max_position_usd": 0.0,
    }
    ok, _ = _risk_allows_trade(risk)
    assert ok is False

    with patch.object(intelligence_bridge, "_INTEL_IS_LIVE", True):
        gate = _map_result({
            "action": "skip", "confidence": 75.0, "score": 75.0, "contracts": 0,
            "reasoning": "kill_switch", "risk_detail": risk, "ticker": "AAPL",
        }, fallback_score=78.0)
    assert gate["intel_status"] == "RISK_VETO", (
        f"Kill switch must produce RISK_VETO in LIVE mode. Got: {gate['intel_status']}"
    )
    assert gate["approved"] is False


# ─── Test 10: Admission policy — REGIME_MISMATCH_ADVISORY → allowed, non-auth ─

def test_admission_policy_regime_mismatch_non_authoritative():
    from ap.intelligence_admission_policy import (
        adjudicate_intelligence_result, INTEL_REGIME_MISMATCH_ADVISORY,
    )
    verdict = adjudicate_intelligence_result(
        {"approved": True, "intel_status": "REGIME_MISMATCH_ADVISORY",
         "score": 78.0, "contracts": 2,
         "reasoning": "regime_mismatch_advisory: CALL blocked",
         "_available": True},
        signal={"signal_id": "test-001", "ticker": "AAPL", "side": "CALL"},
        execution_mode="live",
    )
    assert verdict.allowed is True
    assert verdict.authoritative is False
    assert verdict.reason_code == INTEL_REGIME_MISMATCH_ADVISORY


# ─── Test 11: INSUFFICIENT_CAPITAL is hard_veto=True ────────────────────────

def test_insufficient_capital_is_hard_veto():
    import pandas as pd, numpy as np
    rm = _rm()
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
