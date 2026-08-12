"""
tests/test_p0_forward_regime_advisory_flow.py
=============================================
P0: forward-flow regime advisory tests.

Covers producer → transport → bridge → admission seams:
  - Normal APPROVED result remains valid
  - SPY regime disagreement is advisory (CALL/PUT both sides)
  - VIX, contract-quality, sector-cap, daily-loss, zero-capital remain hard vetoes
  - Bridge structured allowlist: APPROVED + APPROVED_WITH_REGIME_MISMATCH pass
  - Bridge fails closed on contradictory, malformed, unknown-code, zero-size inputs
  - Hard vetoes block in LIVE and PAPER identically (no RISK_VETO_OVERRIDE)
  - Advisory zero PM contracts stay zero — never manufactured
  - Legacy payloads (no hard_veto field) retain existing behavior
  - Admission policy classifies REGIME_MISMATCH_ADVISORY as allowed, non-authoritative
  - Normal APPROVED remains authoritative
  - RISK_VETO remains authoritative denial
  - Pipeline transports all three authority fields through both surfaces
"""
from __future__ import annotations

import datetime as dt
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest


# ─── helpers ──────────────────────────────────────────────────────────────────

def _spy(trend: str) -> dict:
    return {"trend": trend}


def _vix(*, tradeable: bool = True, value: float = 18.0) -> dict:
    return {
        "vix": value,
        "tradeable": tradeable,
        "source": "yfinance:^VIX.fast_info.lastPrice",
        "observed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "classification": "PRODUCTION_EXACT",
    }


def _prices(start: float = 100.0, end: float = 110.0) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "open":   np.linspace(start, end, 30),
            "high":   np.linspace(start + 1, end + 1, 30),
            "low":    np.linspace(start - 1, end - 1, 30),
            "close":  np.linspace(start, end, 30),
            "volume": np.full(30, 1_000_000),
        },
        index=pd.date_range("2026-06-01", periods=30),
    )


def _risk_manager(portfolio_value: float = 25_000):
    from ap_intelligence.agents.ap_risk_manager import APRiskManager
    return APRiskManager(portfolio_value=portfolio_value, allow_0dte=True)


_CALL = dict(
    ticker="AAPL",
    direction="bullish",
    underlying_price=190.0,
    atr_value=2.5,
    option_delta=0.45,
    option_premium=2.50,
    bid_ask_spread_pct=0.05,
    open_interest=500,
    daily_volume_options=200,
    dte=1,
)

_PUT = dict(
    ticker="MSFT",
    direction="bearish",
    underlying_price=420.0,
    atr_value=5.0,
    option_delta=0.45,
    option_premium=3.00,
    bid_ask_spread_pct=0.05,
    open_interest=500,
    daily_volume_options=200,
    dte=1,
)


# ─── producer tests ───────────────────────────────────────────────────────────

def test_01_normal_approval_remains_valid():
    """SPY BULL + bullish CALL → APPROVED, hard_veto=False, max_contracts>=1."""
    rm = _risk_manager()
    with (
        patch("ap_intelligence.agents.ap_risk_manager.get_spy_trend", return_value=_spy("BULL")),
        patch("ap_intelligence.agents.ap_risk_manager.get_vix",       return_value=_vix()),
        patch("ap_intelligence.agents.ap_risk_manager.get_prices",    return_value=_prices()),
    ):
        r = rm.evaluate(**_CALL)

    assert r.approved is True,            f"approved={r.approved}"
    assert r.reason_code == "APPROVED",   f"reason_code={r.reason_code}"
    assert r.hard_veto is False,          f"hard_veto={r.hard_veto}"
    assert r.max_contracts >= 1,          f"max_contracts={r.max_contracts}"


def test_02_bearish_put_vs_spy_bull_is_advisory():
    """SPY BULL + bearish PUT → APPROVED_WITH_REGIME_MISMATCH, non-blocking."""
    rm = _risk_manager()
    with (
        patch("ap_intelligence.agents.ap_risk_manager.get_spy_trend", return_value=_spy("BULL")),
        patch("ap_intelligence.agents.ap_risk_manager.get_vix",       return_value=_vix()),
        patch("ap_intelligence.agents.ap_risk_manager.get_prices",    return_value=_prices()),
    ):
        r = rm.evaluate(**_PUT)

    assert r.approved is True,                                 f"approved={r.approved}"
    assert r.reason_code == "APPROVED_WITH_REGIME_MISMATCH",   f"reason_code={r.reason_code}"
    assert r.veto_category == "MARKET_CONTEXT",                f"veto_category={r.veto_category}"
    assert r.hard_veto is False,                               f"hard_veto={r.hard_veto}"
    assert r.max_contracts >= 1,                               f"max_contracts={r.max_contracts}"


def test_03_bullish_call_vs_spy_bear_is_advisory():
    """SPY BEAR + bullish CALL → APPROVED_WITH_REGIME_MISMATCH, non-blocking."""
    rm = _risk_manager()
    with (
        patch("ap_intelligence.agents.ap_risk_manager.get_spy_trend", return_value=_spy("BEAR")),
        patch("ap_intelligence.agents.ap_risk_manager.get_vix",       return_value=_vix()),
        patch("ap_intelligence.agents.ap_risk_manager.get_prices",    return_value=_prices()),
    ):
        r = rm.evaluate(**_CALL)

    assert r.approved is True,                                 f"approved={r.approved}"
    assert r.reason_code == "APPROVED_WITH_REGIME_MISMATCH",   f"reason_code={r.reason_code}"
    assert r.veto_category == "MARKET_CONTEXT",                f"veto_category={r.veto_category}"
    assert r.hard_veto is False,                               f"hard_veto={r.hard_veto}"
    assert r.max_contracts >= 1,                               f"max_contracts={r.max_contracts}"


def test_04_mismatch_does_not_bypass_vix():
    """Regime mismatch must not skip the VIX hard gate."""
    rm = _risk_manager()
    with (
        patch("ap_intelligence.agents.ap_risk_manager.get_spy_trend", return_value=_spy("BEAR")),
        patch("ap_intelligence.agents.ap_risk_manager.get_vix",       return_value=_vix(tradeable=False, value=45.0)),
    ):
        r = rm.evaluate(**_CALL)

    assert r.approved is False,                   f"approved={r.approved}"
    assert r.reason_code == "VIX_POLICY_HARD_CAP", f"reason_code={r.reason_code}"
    assert r.hard_veto is True,                   f"hard_veto={r.hard_veto}"


def test_05_mismatch_does_not_bypass_contract_quality():
    """Wide bid-ask spread must block even with regime mismatch."""
    rm = _risk_manager()
    with (
        patch("ap_intelligence.agents.ap_risk_manager.get_spy_trend", return_value=_spy("BEAR")),
        patch("ap_intelligence.agents.ap_risk_manager.get_vix",       return_value=_vix()),
    ):
        r = rm.evaluate(**{**_CALL, "bid_ask_spread_pct": 0.50})

    assert r.approved is False,                        f"approved={r.approved}"
    assert r.reason_code == "CONTRACT_QUALITY_FAILED", f"reason_code={r.reason_code}"
    assert r.hard_veto is True,                        f"hard_veto={r.hard_veto}"


def test_06_mismatch_does_not_bypass_sector_cap():
    """Sector cap must block even with regime mismatch."""
    rm = _risk_manager()
    # Fill two same-sector (tech) positions — MAX_SAME_SECTOR_POSITIONS == 2
    rm.open_positions = {
        "MSFT": {"direction": "bullish", "cost_usd": 2_000, "sector": "tech"},
        "NVDA": {"direction": "bullish", "cost_usd": 2_000, "sector": "tech"},
    }
    with (
        patch("ap_intelligence.agents.ap_risk_manager.get_spy_trend", return_value=_spy("BEAR")),
        patch("ap_intelligence.agents.ap_risk_manager.get_vix",       return_value=_vix()),
    ):
        r = rm.evaluate(**_CALL)   # AAPL is tech sector

    assert r.approved is False,                   f"approved={r.approved}"
    assert r.reason_code == "SECTOR_EXPOSURE_CAP", f"reason_code={r.reason_code}"
    assert r.hard_veto is True,                   f"hard_veto={r.hard_veto}"


def test_07_daily_loss_remains_hard_veto():
    """Daily loss kill switch must remain a hard veto regardless of regime."""
    rm = _risk_manager(portfolio_value=25_000)
    rm.daily_pnl = -2_000   # exceeds -5% threshold (-1,250)
    with (
        patch("ap_intelligence.agents.ap_risk_manager.get_spy_trend", return_value=_spy("BULL")),
        patch("ap_intelligence.agents.ap_risk_manager.get_vix",       return_value=_vix()),
    ):
        r = rm.evaluate(**_CALL)

    assert r.approved is False,                        f"approved={r.approved}"
    assert r.reason_code == "DAILY_LOSS_KILL_SWITCH",  f"reason_code={r.reason_code}"
    assert r.hard_veto is True,                        f"hard_veto={r.hard_veto}"


def test_08_zero_affordable_contracts_is_hard_veto():
    """Zero affordable contracts must produce a hard veto, not an advisory."""
    rm = _risk_manager(portfolio_value=25_000)
    # Deploy nearly all capital so nothing remains
    rm.open_positions = {
        "X": {"direction": "bullish", "cost_usd": 21_000, "sector": "other"},
    }
    with (
        patch("ap_intelligence.agents.ap_risk_manager.get_spy_trend", return_value=_spy("BULL")),
        patch("ap_intelligence.agents.ap_risk_manager.get_vix",       return_value=_vix()),
        patch("ap_intelligence.agents.ap_risk_manager.get_prices",    return_value=_prices()),
    ):
        r = rm.evaluate(**_CALL)

    assert r.approved is False,                                         f"approved={r.approved}"
    assert r.reason_code == "INSUFFICIENT_CAPITAL_OR_ZERO_CONTRACTS",   f"reason_code={r.reason_code}"
    assert r.hard_veto is True,                                         f"hard_veto={r.hard_veto}"
    assert r.max_contracts == 0,                                        f"max_contracts={r.max_contracts}"


# ─── bridge unit tests ────────────────────────────────────────────────────────

import intelligence_bridge as bridge


def test_09_normal_structured_approval_passes():
    """reason_code=APPROVED, hard_veto=False, positive sizing → True."""
    risk = {
        "approved": True,
        "reason": "APPROVED",
        "reason_code": "APPROVED",
        "veto_category": "NONE",
        "hard_veto": False,
        "max_contracts": 2,
        "max_position_usd": 500.0,
    }
    ok, _ = bridge._risk_allows_trade(risk)
    assert ok is True, "_risk_allows_trade must return True for APPROVED"

    gate = bridge._map_result(
        {
            "action": "execute",
            "confidence": 78.0,
            "score": 78.0,
            "contracts": 2,
            "reasoning": "approved",
            "ticker": "AAPL",
            "risk_detail": risk,
        },
        fallback_score=78.0,
    )
    assert gate["approved"] is True,            f"approved={gate['approved']}"
    assert gate["intel_status"] == "APPROVED",  f"intel_status={gate['intel_status']}"
    assert gate["contracts"] == 2,              f"contracts={gate['contracts']}"


def test_10_advisory_structured_approval_passes():
    """reason_code=APPROVED_WITH_REGIME_MISMATCH → REGIME_MISMATCH_ADVISORY."""
    risk = {
        "approved": True,
        "reason": "SPY regime mismatch advisory",
        "reason_code": "APPROVED_WITH_REGIME_MISMATCH",
        "veto_category": "MARKET_CONTEXT",
        "hard_veto": False,
        "max_contracts": 2,
        "max_position_usd": 500.0,
    }
    ok, _ = bridge._risk_allows_trade(risk)
    assert ok is True, "_risk_allows_trade must return True for advisory"

    gate = bridge._map_result(
        {
            "action": "execute",
            "confidence": 78.0,
            "score": 78.0,
            "contracts": 2,
            "reasoning": "approved",
            "ticker": "AAPL",
            "risk_detail": risk,
        },
        fallback_score=78.0,
    )
    assert gate["approved"] is True,                          f"approved={gate['approved']}"
    assert gate["intel_status"] == "REGIME_MISMATCH_ADVISORY", f"intel_status={gate['intel_status']}"
    assert gate["contracts"] == 2,                            f"contracts={gate['contracts']}"


def test_11_advisory_zero_pm_contracts_stay_zero():
    """PM contracts=0 must not be manufactured into 1 on the advisory path."""
    risk = {
        "approved": True,
        "reason": "SPY regime mismatch advisory",
        "reason_code": "APPROVED_WITH_REGIME_MISMATCH",
        "veto_category": "MARKET_CONTEXT",
        "hard_veto": False,
        "max_contracts": 2,
        "max_position_usd": 500.0,
    }
    gate = bridge._map_result(
        {
            "action": "execute",
            "confidence": 78.0,
            "score": 78.0,
            "contracts": 0,   # PM chose zero
            "reasoning": "pm_zero",
            "ticker": "AAPL",
            "risk_detail": risk,
        },
        fallback_score=78.0,
    )
    assert gate["contracts"] == 0, (
        f"Bridge must not manufacture contracts from PM=0. Got {gate['contracts']}"
    )
    assert gate["intel_status"] == "REGIME_MISMATCH_ADVISORY"
    assert gate["approved"] is True


def test_12_structured_hard_veto_blocks_in_live_and_paper():
    """Structured hard_veto=True must block in both collection modes."""
    risk = {
        "approved": False,
        "reason": "DAILY KILL SWITCH",
        "reason_code": "DAILY_LOSS_KILL_SWITCH",
        "veto_category": "ACCOUNT_SAFETY",
        "hard_veto": True,
        "max_contracts": 0,
        "max_position_usd": 0.0,
    }
    payload = {
        "action": "execute",
        "confidence": 78.0,
        "score": 78.0,
        "contracts": 0,
        "reasoning": "kill_switch",
        "ticker": "AAPL",
        "risk_detail": risk,
    }

    for collect_return in (False, True):
        with patch.object(bridge, "_data_collection_allowed", return_value=collect_return):
            with patch.object(bridge, "_INTEL_IS_LIVE", True):
                gate = bridge._map_result(payload, fallback_score=78.0)
        assert gate["approved"] is False, (
            f"structured hard veto must block regardless of collection mode "
            f"(collect={collect_return}): approved={gate['approved']}"
        )
        assert gate["intel_status"] == "RISK_VETO", (
            f"intel_status must be RISK_VETO not RISK_VETO_OVERRIDE "
            f"(collect={collect_return}): intel_status={gate['intel_status']}"
        )
        assert gate.get("contracts", 0) == 0


def test_13_contradictory_structured_result_fails_closed():
    """approved=False + hard_veto=False + known code → fail closed."""
    ok, reason = bridge._risk_allows_trade({
        "approved": False,
        "reason_code": "APPROVED",
        "hard_veto": False,
        "max_contracts": 2,
        "max_position_usd": 500.0,
    })
    assert ok is False, f"contradictory payload must fail closed: ok={ok} reason={reason}"


def test_14_unknown_structured_approval_code_fails_closed():
    """Unknown reason_code with hard_veto=False → fail closed."""
    ok, reason = bridge._risk_allows_trade({
        "approved": True,
        "reason_code": "FUTURE_UNKNOWN_APPROVAL",
        "hard_veto": False,
        "max_contracts": 2,
        "max_position_usd": 500.0,
    })
    assert ok is False, f"unknown approval code must fail closed: ok={ok} reason={reason}"


def test_15_malformed_hard_veto_type_fails_closed():
    """Non-bool hard_veto values must fail closed."""
    for bad_value in (None, 0, 1, "false", "true"):
        ok, reason = bridge._risk_allows_trade({
            "approved": True,
            "reason_code": "APPROVED",
            "hard_veto": bad_value,
            "max_contracts": 2,
            "max_position_usd": 500.0,
        })
        assert ok is False, (
            f"hard_veto={bad_value!r} must fail closed: ok={ok} reason={reason}"
        )


def test_16_approved_structured_with_zero_sizing_fails_closed():
    """Structured approval with zero contracts/usd → fail closed (no executable size)."""
    ok, reason = bridge._risk_allows_trade({
        "approved": True,
        "reason_code": "APPROVED",
        "hard_veto": False,
        "max_contracts": 0,
        "max_position_usd": 0,
    })
    assert ok is False, (
        f"zero-size approval must fail closed: ok={ok} reason={reason}"
    )


def test_17_legacy_behavior_remains_compatible():
    """Payloads without hard_veto must use the legacy compatibility parser."""
    ok_true, _ = bridge._risk_allows_trade({"approved": True, "reason": "legacy approved"})
    assert ok_true is True, "legacy approved=True must pass"

    ok_false, _ = bridge._risk_allows_trade({"approved": False, "reason": "legacy rejected"})
    assert ok_false is False, "legacy approved=False must block"


# ─── admission policy tests ───────────────────────────────────────────────────

def test_18_advisory_policy_classification_live_and_paper():
    """REGIME_MISMATCH_ADVISORY → allowed=True, authoritative=False, both modes."""
    from ap.intelligence_admission_policy import (
        INTEL_REGIME_MISMATCH_ADVISORY,
        adjudicate_intelligence_result,
    )
    intel = {
        "approved": True,
        "intel_status": "REGIME_MISMATCH_ADVISORY",
        "score": 78.0,
        "contracts": 2,
        "reasoning": "SPY regime mismatch advisory",
        "_available": True,
    }
    signal = {"signal_id": "test-018", "ticker": "AAPL", "side": "CALL"}

    for mode in ("LIVE", "PAPER"):
        v = adjudicate_intelligence_result(intel, signal=signal, execution_mode=mode)
        assert v.allowed is True,                                f"[{mode}] allowed={v.allowed}"
        assert v.authoritative is False,                         f"[{mode}] authoritative={v.authoritative}"
        assert v.reason_code == INTEL_REGIME_MISMATCH_ADVISORY,  f"[{mode}] reason_code={v.reason_code}"


def test_19_normal_approval_remains_authoritative():
    """APPROVED intel_status → allowed=True, authoritative=True."""
    from ap.intelligence_admission_policy import (
        INTEL_AUTHORITATIVE_APPROVED,
        adjudicate_intelligence_result,
    )
    intel = {
        "approved": True,
        "intel_status": "APPROVED",
        "score": 78.0,
        "contracts": 2,
        "reasoning": "approved",
        "_available": True,
    }
    signal = {"signal_id": "test-019", "ticker": "AAPL", "side": "CALL"}

    v = adjudicate_intelligence_result(intel, signal=signal, execution_mode="LIVE")
    assert v.allowed is True,                                f"allowed={v.allowed}"
    assert v.authoritative is True,                          f"authoritative={v.authoritative}"
    assert v.reason_code == INTEL_AUTHORITATIVE_APPROVED,    f"reason_code={v.reason_code}"


def test_20_risk_veto_remains_authoritative_denial():
    """RISK_VETO → allowed=False, authoritative=True in LIVE and PAPER."""
    from ap.intelligence_admission_policy import (
        INTEL_AUTHORITATIVE_VETO_RISK,
        adjudicate_intelligence_result,
    )
    intel = {
        "approved": False,
        "intel_status": "RISK_VETO",
        "score": 50.0,
        "contracts": 0,
        "reasoning": "risk_veto: DAILY KILL SWITCH",
        "_available": True,
    }
    signal = {"signal_id": "test-020", "ticker": "AAPL", "side": "CALL"}

    for mode in ("LIVE", "PAPER"):
        v = adjudicate_intelligence_result(intel, signal=signal, execution_mode=mode)
        assert v.allowed is False,                            f"[{mode}] allowed={v.allowed}"
        assert v.authoritative is True,                       f"[{mode}] authoritative={v.authoritative}"
        assert v.reason_code == INTEL_AUTHORITATIVE_VETO_RISK, f"[{mode}] reason_code={v.reason_code}"


# ─── transport test ───────────────────────────────────────────────────────────

def test_21_pipeline_transports_all_authority_fields():
    """Pipeline must transport reason_code, veto_category, hard_veto in both dicts."""
    from ap_intelligence.ap_signal_pipeline import APSignalPipeline
    from ap_intelligence.agents.ap_risk_manager import ContractQuality, RiskResult

    pipeline = APSignalPipeline.__new__(APSignalPipeline)

    cq = ContractQuality(
        passes=True, spread_ok=True, oi_ok=True,
        volume_ok=True, delta_ok=True, dte_ok=True,
        rejection_reason="",
    )
    known_risk = RiskResult(
        ticker="AAPL",
        approved=True,
        max_contracts=2,
        max_position_usd=500.0,
        risk_dollars=2_500.0,
        stop_distance_pct=1.31,
        expected_loss_per_contract=11.77,
        volatility_pct=22.3,
        position_limit_pct=10.0,
        correlation_multiplier=1.0,
        sector_multiplier=1.0,
        contract_quality=cq,
        spy_trend="BEAR",
        vix=18.0,
        reason="SPY regime mismatch advisory: BULLISH setup while SPY 20-day trend=BEAR",
        reason_code="APPROVED_WITH_REGIME_MISMATCH",
        veto_category="MARKET_CONTEXT",
        hard_veto=False,
    )

    captured_signals: dict = {}

    def fake_decide(ticker, signals):
        captured_signals.update(signals)
        return SimpleNamespace(
            action="execute",
            direction="bullish",
            contracts=2,
            max_usd=500.0,
            confidence=78.0,
            score=78.0,
            ev_score=78.0,
            reasoning="approved",
            signal_breakdown={},
        )

    pipeline.technical    = MagicMock()
    pipeline.technical.analyze.return_value = {"signal": "bullish", "confidence": 78, "breakdown": {}}
    pipeline.sentiment    = MagicMock()
    pipeline.sentiment.analyze.return_value = {"signal": "neutral", "confidence": 50}
    pipeline.fundamentals = MagicMock()
    pipeline.fundamentals.analyze.return_value = {"signal": "neutral", "confidence": 50}
    pipeline.risk_manager = MagicMock()
    pipeline.risk_manager.evaluate.return_value = known_risk
    pipeline.pm           = MagicMock()
    pipeline.pm.decide.side_effect = fake_decide
    pipeline.audit        = MagicMock()
    pipeline.audit.record = MagicMock()
    pipeline.use_sentiment    = False
    pipeline.use_fundamentals = False
    pipeline.portfolio_value  = 25_000

    mode_cfg = MagicMock()
    mode_cfg.mode = "LIVE"
    pipeline.mode_cfg = mode_cfg

    with patch("ap_intelligence.ap_signal_pipeline.get_prices", return_value=_prices()):
        result = pipeline.run(
            ticker="AAPL",
            scanner_signal="bullish",
            scanner_confidence=78.0,
            underlying_price=190.0,
            atr_value=2.5,
            option_premium=2.50,
            option_delta=0.45,
        )

    # Validate risk_detail surface (return value)
    rd = result["risk_detail"]
    assert rd["approved"] is True,                                  f"rd.approved={rd['approved']}"
    assert rd["reason_code"] == "APPROVED_WITH_REGIME_MISMATCH",    f"rd.reason_code={rd['reason_code']}"
    assert rd["veto_category"] == "MARKET_CONTEXT",                 f"rd.veto_category={rd['veto_category']}"
    assert rd["hard_veto"] is False,                                f"rd.hard_veto={rd['hard_veto']}"
    assert rd["max_contracts"] == 2,                                f"rd.max_contracts={rd['max_contracts']}"
    assert rd["max_position_usd"] == 500.0,                         f"rd.max_position_usd={rd['max_position_usd']}"

    # Validate signals["risk"] surface (passed to pm.decide)
    sr = captured_signals.get("risk", {})
    assert sr.get("approved") is True,                               f"sr.approved={sr.get('approved')}"
    assert sr.get("reason_code") == "APPROVED_WITH_REGIME_MISMATCH", f"sr.reason_code={sr.get('reason_code')}"
    assert sr.get("veto_category") == "MARKET_CONTEXT",              f"sr.veto_category={sr.get('veto_category')}"
    assert sr.get("hard_veto") is False,                             f"sr.hard_veto={sr.get('hard_veto')}"
    assert sr.get("max_contracts") == 2,                             f"sr.max_contracts={sr.get('max_contracts')}"
    assert sr.get("max_position_usd") == 500.0,                      f"sr.max_position_usd={sr.get('max_position_usd')}"
