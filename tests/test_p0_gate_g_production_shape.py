"""Production-shaped regressions for the P0 Gate G truth boundary."""

from __future__ import annotations

import datetime as dt
import sys
import types
import pytest

import intelligence_bridge as ib


def _now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _selected_signal(**contract_overrides):
    contract = {
        "contract_symbol": "AAPL260821C00190000",
        "selected_client_id": "client-a",
        "selected_execution_mode": "LIVE",
        "canonical_signal_id": "sig-1",
        "quote_ts": _now(),
        "bid": 1.00,
        "ask": 1.10,
        "delta": 0.45,
        "open_interest": 500,
        "volume": 100,
        "dte": 0,
    }
    contract.update(contract_overrides)
    return {
        "ticker": "AAPL",
        "side": "CALL",
        "score": 78.0,
        "signal_id": "sig-1",
        "_gate_client_id": "client-a",
        "_gate_execution_mode": "live",
        "selected_contract": contract,
    }


class _FakePipeline:
    def __init__(self):
        self.calls = []

    @staticmethod
    def _result(ticker):
        return {
            "ticker": ticker,
            "action": "execute",
            "score": 72.0,
            "confidence": 0.72,
            "contracts": 1,
            "reasoning": "setup context available",
            "risk_detail": {
                "approved": True,
                "reason_code": "ADVISORY_DATA_UNAVAILABLE",
                "hard_veto": False,
                "max_contracts": 1,
                "max_position_usd": 0.0,
                "account_state_authoritative": False,
            },
        }

    def run_quick(self, **kwargs):
        self.calls.append(("run_quick", kwargs))
        return self._result(kwargs["ticker"])

    def run(self, **kwargs):
        self.calls.append(("run", kwargs))
        return self._result(kwargs["ticker"])


def _run_with_fake_pipeline(monkeypatch, signal, pipeline=None):
    pipeline = pipeline or _FakePipeline()
    monkeypatch.setattr(
        ib,
        "_get_pipeline",
        lambda **_: pipeline,
    )
    monkeypatch.setattr(ib._AUDIT_EXECUTOR, "submit", lambda *a, **k: None)
    return ib.run_intelligence_check(signal, underlying_price=190.0), pipeline


def test_zero_numeric_parser_preserves_zero():
    assert ib._safe_float(0, field="x") == (0.0, "ok")
    assert ib._safe_int("0", field="dte") == (0, "ok")
    assert ib._safe_float(False, field="x")[0] is None


def test_explicit_numeric_zero_dte_survives_bridge(monkeypatch):
    signal = {
        "ticker": "AAPL", "side": "CALL", "score": 0,
        "signal_id": "sig-zero", "_gate_client_id": "client-a",
        "_gate_execution_mode": "PAPER", "dte": 0,
    }
    gate, pipeline = _run_with_fake_pipeline(monkeypatch, signal)
    assert pipeline.calls[0][0] == "run_quick"
    assert pipeline.calls[0][1]["dte"] == 0
    assert gate["gate_diagnostics"]["dte_raw"] == 0
    assert gate["gate_diagnostics"]["dte_resolved"] == 0
    assert gate["gate_diagnostics"]["scanner_score"] == 0.0


def test_string_zero_dte_parses_to_zero(monkeypatch):
    signal = {
        "ticker": "AAPL", "side": "CALL", "score": 70,
        "signal_id": "sig-zero-string", "dte": "0",
    }
    _, pipeline = _run_with_fake_pipeline(monkeypatch, signal)
    assert pipeline.calls[0][1]["dte"] == 0


@pytest.mark.parametrize("raw_dte", [None, "", "unknown", 1.5, True])
def test_missing_or_malformed_dte_is_advisory_fallback(monkeypatch, raw_dte):
    signal = {"ticker": "AAPL", "side": "CALL", "score": 70, "signal_id": "sig-dte"}
    if raw_dte is not None:
        signal["dte"] = raw_dte
    gate, pipeline = _run_with_fake_pipeline(monkeypatch, signal)
    assert pipeline.calls[0][1]["dte"] == 1
    dte_provenance = gate["gate_diagnostics"]["input_provenance"]["dte"]
    assert dte_provenance["authoritative"] is False
    assert dte_provenance["classification"] == ib.DEFAULT_ADVISORY


def test_loose_preselector_quote_fields_are_not_selected_authority(monkeypatch):
    signal = {
        "ticker": "AAPL", "side": "CALL", "score": 78,
        "signal_id": "sig-loose", "dte": 0,
        "option_bid": 0.0, "option_ask": 0.01,
        "spread_pct": 0.0, "open_interest": 0, "option_volume": 0,
    }
    gate, pipeline = _run_with_fake_pipeline(monkeypatch, signal)
    assert pipeline.calls[0][0] == "run_quick"
    evidence = gate["gate_diagnostics"]["selected_contract_evidence"]
    assert evidence["exists"] is False
    assert gate["risk_detail"]["reason_code"] != "CONTRACT_QUALITY_FAILED"
    assert gate["gate_diagnostics"]["input_provenance"]["spread_pct"]["authoritative"] is False


def test_exact_selected_contract_activates_only_exact_quality_path(monkeypatch):
    signal = _selected_signal()
    gate, pipeline = _run_with_fake_pipeline(monkeypatch, signal)
    name, kwargs = pipeline.calls[0]
    assert name == "run"
    assert kwargs["contract_quality_authoritative"] is True
    assert kwargs["account_state_authoritative"] is False
    assert kwargs["dte"] == 0
    assert gate["gate_diagnostics"]["selected_contract_evidence"]["exists"] is True


def test_stale_selected_contract_is_quarantined():
    old = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=10)).isoformat()
    evidence = ib._extract_selected_contract_evidence(
        _selected_signal(quote_ts=old), client_id="client-a", execution_mode="LIVE"
    )
    assert evidence["exists"] is False
    assert evidence["classification"] == ib.UNAVAILABLE
    assert evidence["reason"] == "selected_contract_quote_stale"


def test_selected_contract_client_mismatch_has_no_authority():
    evidence = ib._extract_selected_contract_evidence(
        _selected_signal(selected_client_id="client-b"),
        client_id="client-a", execution_mode="LIVE",
    )
    assert evidence["exists"] is False
    assert evidence["reason"] == "selected_contract_client_mismatch"


def test_paper_contract_cannot_authorize_live():
    evidence = ib._extract_selected_contract_evidence(
        _selected_signal(selected_execution_mode="PAPER"),
        client_id="client-a", execution_mode="LIVE",
    )
    assert evidence["exists"] is False
    assert evidence["reason"] == "selected_contract_execution_mode_mismatch"


def test_missing_signal_identity_cannot_authorize_contract():
    signal = _selected_signal()
    signal.pop("signal_id")
    signal["selected_contract"].pop("canonical_signal_id")
    evidence = ib._extract_selected_contract_evidence(
        signal, client_id="client-a", execution_mode="LIVE"
    )
    assert evidence["exists"] is False
    assert evidence["reason"] == "selected_contract_signal_identity_mismatch"


def test_conflicting_signal_and_metadata_contracts_are_quarantined():
    signal = _selected_signal()
    signal["metadata"] = {
        "selected_contract": {
            **signal["selected_contract"],
            "contract_symbol": "AAPL260821P00190000",
        }
    }
    evidence = ib._extract_selected_contract_evidence(
        signal, client_id="client-a", execution_mode="LIVE"
    )
    assert evidence["exists"] is False
    assert evidence["reason"] == "selected_contract_identity_conflict"


def test_non_occ_contract_like_metadata_is_not_selected_evidence():
    signal = {
        "ticker": "AAPL", "score": 78, "signal_id": "sig-legacy",
        "metadata": {"contract_symbol": "AAPL_CALL_190", "bid": 1.0, "ask": 1.1},
    }
    evidence = ib._extract_selected_contract_evidence(
        signal, client_id="client-a", execution_mode="LIVE"
    )
    assert evidence["exists"] is False
    assert evidence["classification"] in {ib.NOT_AVAILABLE_YET, ib.UNAVAILABLE}


def test_environment_equity_is_diagnostic_only(monkeypatch):
    monkeypatch.setenv("ACCOUNT_EQUITY", "25000")
    inputs = ib._build_gate_inputs(
        {"ticker": "AAPL", "signal_id": "sig-equity", "dte": 0},
        score=80.0, underlying_price=190.0, client_id="client-a",
        execution_mode="LIVE", contract_evidence={"exists": False, "classification": ib.NOT_AVAILABLE_YET},
    )
    assert inputs["account_state_authoritative"] is False
    assert inputs["input_provenance"]["account_equity"]["authoritative"] is False
    assert inputs["input_provenance"]["account_equity"]["value"] == 25000.0


def test_no_canonical_account_snapshot_demotes_legacy_account_risk():
    from unittest.mock import patch
    from ap_intelligence.agents.ap_risk_manager import APRiskManager

    rm = APRiskManager(portfolio_value=25_000)
    rm.daily_pnl = -100_000
    rm.open_positions = {"AAPL": {"sector": "technology", "cost_usd": 1_000_000}}
    with (
        patch("ap_intelligence.agents.ap_risk_manager.get_spy_trend", return_value={"trend": "BULL"}),
        patch("ap_intelligence.agents.ap_risk_manager.get_vix", return_value={"vix": 18.0, "tradeable": True}),
    ):
        result = rm.evaluate(
            ticker="AAPL", direction="bullish", underlying_price=190.0,
            atr_value=2.5, option_delta=0.0, option_premium=0.0,
            bid_ask_spread_pct=0.50, open_interest=0, daily_volume_options=0,
            dte=0, contract_quality_authoritative=False,
            account_state_authoritative=False,
        )
    assert result.approved is True
    assert result.hard_veto is False
    assert result.reason_code == "ADVISORY_DATA_UNAVAILABLE"
    assert result.authority_diagnostics["account_state"]["authoritative"] is False


def test_exact_account_daily_loss_authority_still_blocks():
    from ap_intelligence.agents.ap_risk_manager import APRiskManager

    rm = APRiskManager(portfolio_value=25_000)
    rm.daily_pnl = -2_000
    result = rm.evaluate(
        ticker="AAPL", direction="bullish", underlying_price=190.0,
        atr_value=2.5, option_delta=0.45, option_premium=2.5,
        bid_ask_spread_pct=0.05, open_interest=500,
        daily_volume_options=200, dte=1,
    )
    assert result.approved is False
    assert result.reason_code == "DAILY_LOSS_KILL_SWITCH"
    assert result.hard_veto is True
    assert result.account_state_authoritative is True


def test_exact_contract_quality_can_block_when_selected_contract_is_real():
    from unittest.mock import patch
    from ap_intelligence.agents.ap_risk_manager import APRiskManager

    rm = APRiskManager(portfolio_value=25_000)
    with (
        patch("ap_intelligence.agents.ap_risk_manager.get_spy_trend", return_value={"trend": "BULL"}),
        patch("ap_intelligence.agents.ap_risk_manager.get_vix", return_value={"vix": 18.0, "tradeable": True}),
    ):
        result = rm.evaluate(
            ticker="AAPL", direction="bullish", underlying_price=190.0,
            atr_value=2.5, option_delta=0.45, option_premium=1.0,
            bid_ask_spread_pct=0.50, open_interest=0, daily_volume_options=0,
            dte=0, contract_quality_authoritative=True,
            account_state_authoritative=False,
        )
    assert result.approved is False
    assert result.reason_code == "CONTRACT_QUALITY_FAILED"
    assert result.hard_veto is True


def test_regime_mismatch_remains_advisory_without_account_snapshot():
    from unittest.mock import patch
    from ap_intelligence.agents.ap_risk_manager import APRiskManager

    with (
        patch("ap_intelligence.agents.ap_risk_manager.get_spy_trend", return_value={"trend": "BULL"}),
        patch("ap_intelligence.agents.ap_risk_manager.get_vix", return_value={"vix": 18.0, "tradeable": True}),
    ):
        result = APRiskManager(portfolio_value=25_000).evaluate(
            ticker="AAPL", direction="bearish", underlying_price=190.0,
            atr_value=2.5, option_delta=0.45, option_premium=2.5,
            bid_ask_spread_pct=0.05, open_interest=500,
            daily_volume_options=200, dte=1,
            contract_quality_authoritative=False,
            account_state_authoritative=False,
        )
    assert result.approved is True
    assert result.reason_code == "APPROVED_WITH_REGIME_MISMATCH"
    assert result.hard_veto is False


def test_vix_missing_price_cannot_become_fake_safe_value(monkeypatch):
    from ap_intelligence.tools import ap_data_tools

    fake_yf = types.ModuleType("yfinance")
    fake_yf.Ticker = lambda _: types.SimpleNamespace(fast_info={"lastPrice": None})
    monkeypatch.setitem(sys.modules, "yfinance", fake_yf)
    monkeypatch.setattr(ap_data_tools, "_cache_get", lambda _: None)
    monkeypatch.setattr(ap_data_tools, "_cache_set", lambda *_: None)
    result = ap_data_tools.get_vix()
    assert result["vix"] is None
    assert result["tradeable"] is False
    assert result["classification"] == "UNAVAILABLE"


def test_vix_observation_has_provenance(monkeypatch):
    from ap_intelligence.tools import ap_data_tools

    fake_yf = types.ModuleType("yfinance")
    fake_yf.Ticker = lambda _: types.SimpleNamespace(fast_info={"lastPrice": 18.5})
    monkeypatch.setitem(sys.modules, "yfinance", fake_yf)
    monkeypatch.setattr(ap_data_tools, "_cache_get", lambda _: None)
    monkeypatch.setattr(ap_data_tools, "_cache_set", lambda *_: None)
    result = ap_data_tools.get_vix()
    assert result["vix"] == 18.5
    assert result["tradeable"] is True
    assert result["source"] == "yfinance:^VIX.fast_info.lastPrice"
    assert result["observed_at"]


def test_structured_hard_risk_veto_remains_authoritative():
    gate = ib._map_result(
        {
            "action": "execute", "score": 80, "contracts": 1,
            "ticker": "AAPL", "reasoning": "risk",
            "risk_detail": {
                "approved": False, "hard_veto": True,
                "reason_code": "VIX_POLICY_HARD_CAP",
                "max_contracts": 0, "max_position_usd": 0,
            },
        },
        fallback_score=80,
    )
    assert gate["approved"] is False
    assert gate["intel_status"] == "RISK_VETO"


def test_malformed_structured_risk_does_not_gain_authority_from_text():
    allowed, reason = ib._risk_allows_trade(
        {"approved": True, "hard_veto": "false", "reason_code": "APPROVED"}
    )
    assert allowed is False
    assert "hard_veto" in reason


def test_scanner_and_legacy_scores_remain_separate_in_observe_path():
    gate = ib._map_result(
        {
            "action": "skip", "score": 30, "confidence": 30,
            "contracts": 1, "ticker": "AAPL",
            "reasoning": "Score 30.0 < 60 — insufficient edge",
            "risk_detail": {"approved": True},
        },
        fallback_score=70,
    )
    assert gate["approved"] is True
    assert gate["score"] == 70.0
    assert gate["intel_score"] == 30.0
    assert gate["gate_diagnostics"]["scanner_score"] == 70.0
    assert gate["gate_diagnostics"]["legacy_intel_score"] == 30.0


def test_client_and_mode_are_bound_through_real_bridge_call(monkeypatch):
    signal = {
        "ticker": "AAPL", "side": "CALL", "score": 78,
        "signal_id": "sig-bound", "dte": 0,
        "_gate_client_id": "client-runtime",
        "_gate_execution_mode": "PaPeR",
    }
    gate, _ = _run_with_fake_pipeline(monkeypatch, signal)
    diagnostics = gate["gate_diagnostics"]
    assert diagnostics["client_id"] == "client-runtime"
    assert diagnostics["execution_mode"] == "PAPER"
    assert diagnostics["canonical_signal_id"] == "sig-bound"
