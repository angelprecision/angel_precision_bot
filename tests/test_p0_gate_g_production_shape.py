"""Production-shaped regressions for the P0 Gate G truth boundary."""

from __future__ import annotations

import datetime as dt
import sys
import types
import pytest

import intelligence_bridge as ib


def _now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _fresh_vix(value: float = 18.0, **overrides) -> dict:
    result = {
        "vix": value,
        "tradeable": 12.0 <= value <= 35.0,
        "source": "yfinance:^VIX.fast_info.lastPrice",
        "observed_at": _now(),
        "classification": "PRODUCTION_EXACT",
    }
    result.update(overrides)
    return result


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
    signal = _selected_signal(
        quote_source=ib.SELECTED_CONTRACT_TRUSTED_SOURCE,
    )
    gate, pipeline = _run_with_fake_pipeline(monkeypatch, signal)
    name, kwargs = pipeline.calls[0]
    assert name == "run"
    assert kwargs["contract_quality_authoritative"] is True
    assert kwargs["account_state_authoritative"] is False
    assert kwargs["dte"] == 0
    evidence = gate["gate_diagnostics"]["selected_contract_evidence"]
    assert evidence["exists"] is True
    assert evidence["source"] == ib.SELECTED_CONTRACT_TRUSTED_SOURCE


def test_trusted_quote_source_can_be_attested_by_selected_container():
    signal = _selected_signal()
    signal["quote_source"] = ib.SELECTED_CONTRACT_TRUSTED_SOURCE

    evidence = ib._extract_selected_contract_evidence(
        signal, client_id="client-a", execution_mode="LIVE"
    )

    assert evidence["exists"] is True
    assert evidence["authoritative"] is True
    assert evidence["source"] == ib.SELECTED_CONTRACT_TRUSTED_SOURCE


@pytest.mark.parametrize("quote_source", [None, "whatever", "tradier_live"])
def test_unrecognized_selected_contract_quote_source_cannot_create_authority(
    quote_source,
):
    contract = _selected_signal()["selected_contract"]
    if quote_source is None:
        contract.pop("quote_source", None)
    else:
        contract["quote_source"] = quote_source

    evidence = ib._extract_selected_contract_evidence(
        {"selected_contract": contract, "signal_id": "sig-1"},
        client_id="client-a", execution_mode="LIVE",
    )

    assert evidence["exists"] is False
    assert evidence["authoritative"] is False
    assert evidence["classification"] == ib.UNAVAILABLE
    assert evidence["reason"] == "selected_contract_quote_source_untrusted"


@pytest.mark.parametrize("source_field", ["source", "provider"])
def test_trusted_source_alias_can_attest_selected_contract(source_field):
    signal = _selected_signal()
    signal["selected_contract"][source_field] = ib.SELECTED_CONTRACT_TRUSTED_SOURCE

    evidence = ib._extract_selected_contract_evidence(
        signal, client_id="client-a", execution_mode="LIVE"
    )

    assert evidence["exists"] is True
    assert evidence["authoritative"] is True
    assert evidence["source"] == ib.SELECTED_CONTRACT_TRUSTED_SOURCE


@pytest.mark.parametrize("source_field", ["source", "provider"])
def test_conflicting_explicit_source_alias_cannot_create_authority(source_field):
    signal = _selected_signal(
        quote_source=ib.SELECTED_CONTRACT_TRUSTED_SOURCE,
    )
    signal["selected_contract"][source_field] = "untrusted-provider"

    evidence = ib._extract_selected_contract_evidence(
        signal, client_id="client-a", execution_mode="LIVE"
    )

    assert evidence["exists"] is False
    assert evidence["authoritative"] is False
    assert evidence["classification"] == ib.UNAVAILABLE
    assert evidence["reason"] == "selected_contract_quote_source_conflict"


def test_duplicate_identity_with_conflicting_source_provenance_is_quarantined():
    signal = _selected_signal(
        quote_source=ib.SELECTED_CONTRACT_TRUSTED_SOURCE,
    )
    signal["metadata"] = {
        "selected_contract": {
            **signal["selected_contract"],
            "quote_source": "untrusted-provider",
        }
    }

    evidence = ib._extract_selected_contract_evidence(
        signal, client_id="client-a", execution_mode="LIVE"
    )

    assert evidence["exists"] is False
    assert evidence["authoritative"] is False
    assert evidence["classification"] == ib.UNAVAILABLE
    assert evidence["reason"] == "selected_contract_quote_source_conflict"


def test_outer_and_nested_source_provenance_conflict_is_quarantined():
    signal = _selected_signal(
        quote_source=ib.SELECTED_CONTRACT_TRUSTED_SOURCE,
    )
    signal["quote_source"] = ib.SELECTED_CONTRACT_TRUSTED_SOURCE
    signal["selected_contract"]["quote_source"] = "untrusted-provider"

    evidence = ib._extract_selected_contract_evidence(
        signal, client_id="client-a", execution_mode="LIVE"
    )

    assert evidence["exists"] is False
    assert evidence["authoritative"] is False
    assert evidence["classification"] == ib.UNAVAILABLE
    assert evidence["reason"] == "selected_contract_quote_source_conflict"


def test_malformed_duplicate_identity_alias_cannot_be_ignored():
    signal = _selected_signal(
        quote_source=ib.SELECTED_CONTRACT_TRUSTED_SOURCE,
    )
    signal["contract_symbol"] = "not-an-occ-contract"

    evidence = ib._extract_selected_contract_evidence(
        signal, client_id="client-a", execution_mode="LIVE"
    )

    assert evidence["exists"] is False
    assert evidence["authoritative"] is False
    assert evidence["classification"] == ib.UNAVAILABLE
    assert evidence["reason"] == "selected_contract_identity_conflict"


def test_same_occ_conflicting_client_identity_is_quarantined():
    signal = _selected_signal()
    signal["metadata"] = {
        "selected_contract": {
            **signal["selected_contract"],
            "selected_client_id": "client-b",
        }
    }

    evidence = ib._extract_selected_contract_evidence(
        signal, client_id="client-a", execution_mode="LIVE"
    )

    assert evidence["exists"] is False
    assert evidence["authoritative"] is False
    assert evidence["classification"] == ib.UNAVAILABLE
    assert evidence["reason"] == "selected_contract_identity_conflict"


def test_same_occ_conflicting_execution_mode_identity_is_quarantined():
    signal = _selected_signal()
    signal["metadata"] = {
        "selected_contract": {
            **signal["selected_contract"],
            "selected_execution_mode": "PAPER",
        }
    }

    evidence = ib._extract_selected_contract_evidence(
        signal, client_id="client-a", execution_mode="LIVE"
    )

    assert evidence["exists"] is False
    assert evidence["authoritative"] is False
    assert evidence["classification"] == ib.UNAVAILABLE
    assert evidence["reason"] == "selected_contract_identity_conflict"


def test_same_occ_conflicting_signal_identity_is_quarantined():
    signal = _selected_signal()
    signal["metadata"] = {
        "selected_contract": {
            **signal["selected_contract"],
            "canonical_signal_id": "sig-2",
        }
    }

    evidence = ib._extract_selected_contract_evidence(
        signal, client_id="client-a", execution_mode="LIVE"
    )

    assert evidence["exists"] is False
    assert evidence["authoritative"] is False
    assert evidence["classification"] == ib.UNAVAILABLE
    assert evidence["reason"] == "selected_contract_identity_conflict"


@pytest.mark.parametrize("container_name", ["selector_metadata", "approved_plan"])
def test_same_occ_identity_conflict_is_checked_in_every_candidate_container(
    container_name,
):
    signal = _selected_signal()
    conflicting = {
        **signal["selected_contract"],
        "selected_client_id": "client-b",
    }
    metadata = {"selected_contract": conflicting}
    if container_name == "approved_plan":
        signal[container_name] = types.SimpleNamespace(metadata=metadata)
    else:
        signal[container_name] = metadata

    evidence = ib._extract_selected_contract_evidence(
        signal, client_id="client-a", execution_mode="LIVE"
    )

    assert evidence["exists"] is False
    assert evidence["authoritative"] is False
    assert evidence["classification"] == ib.UNAVAILABLE
    assert evidence["reason"] == "selected_contract_identity_conflict"


def test_duplicate_selected_contracts_with_identical_identity_remain_eligible():
    signal = _selected_signal(
        quote_source=ib.SELECTED_CONTRACT_TRUSTED_SOURCE,
    )
    signal["metadata"] = {
        "selected_contract": dict(signal["selected_contract"]),
    }

    evidence = ib._extract_selected_contract_evidence(
        signal, client_id="client-a", execution_mode="LIVE"
    )

    assert evidence["exists"] is True
    assert evidence["authoritative"] is True
    assert evidence["classification"] == ib.PRODUCTION_EXACT
    assert evidence["source"] == ib.SELECTED_CONTRACT_TRUSTED_SOURCE


def test_top_level_and_nested_occ_identity_conflict_is_quarantined():
    signal = _selected_signal(
        quote_source=ib.SELECTED_CONTRACT_TRUSTED_SOURCE,
    )
    signal["contract_symbol"] = "AAPL260821P00190000"

    evidence = ib._extract_selected_contract_evidence(
        signal, client_id="client-a", execution_mode="LIVE"
    )

    assert evidence["exists"] is False
    assert evidence["authoritative"] is False
    assert evidence["classification"] == ib.UNAVAILABLE
    assert evidence["reason"] == "selected_contract_identity_conflict"


def test_outer_signal_timestamp_cannot_attest_selected_contract_quote():
    signal = _selected_signal(
        quote_source=ib.SELECTED_CONTRACT_TRUSTED_SOURCE,
    )
    signal["timestamp"] = _now()
    signal["selected_contract"].pop("quote_ts")

    evidence = ib._extract_selected_contract_evidence(
        signal, client_id="client-a", execution_mode="LIVE"
    )

    assert evidence["exists"] is False
    assert evidence["authoritative"] is False
    assert evidence["reason"] == "selected_contract_quote_timestamp_missing_or_invalid"


def test_outer_signal_observed_at_cannot_attest_nested_selected_contract_quote():
    signal = _selected_signal(
        quote_source=ib.SELECTED_CONTRACT_TRUSTED_SOURCE,
    )
    signal["observed_at"] = _now()
    signal["selected_contract"].pop("quote_ts")

    evidence = ib._extract_selected_contract_evidence(
        signal, client_id="client-a", execution_mode="LIVE"
    )

    assert evidence["exists"] is False
    assert evidence["authoritative"] is False
    assert evidence["reason"] == "selected_contract_quote_timestamp_missing_or_invalid"


def test_stale_selected_contract_is_quarantined():
    old = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=10)).isoformat()
    evidence = ib._extract_selected_contract_evidence(
        _selected_signal(
            quote_ts=old,
            quote_source=ib.SELECTED_CONTRACT_TRUSTED_SOURCE,
        ),
        client_id="client-a", execution_mode="LIVE"
    )
    assert evidence["exists"] is False
    assert evidence["classification"] == ib.UNAVAILABLE
    assert evidence["reason"] == "selected_contract_quote_stale"


@pytest.mark.parametrize("raw_limit", ["nan", "inf", "-inf", "0", "-1", "garbage"])
def test_invalid_selected_quote_age_limit_cannot_create_authority(monkeypatch, raw_limit):
    monkeypatch.setenv("GATE_G_MAX_SELECTED_QUOTE_AGE_SECONDS", raw_limit)
    configured_limit = ib._positive_finite_env_float(
        "GATE_G_MAX_SELECTED_QUOTE_AGE_SECONDS", 120.0
    )
    assert configured_limit is None
    monkeypatch.setattr(ib, "_MAX_SELECTED_QUOTE_AGE_SECONDS", configured_limit)
    old = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=3)).isoformat()
    evidence = ib._extract_selected_contract_evidence(
        _selected_signal(
            quote_ts=old,
            quote_source=ib.SELECTED_CONTRACT_TRUSTED_SOURCE,
        ),
        client_id="client-a", execution_mode="LIVE",
    )

    assert evidence["exists"] is False
    assert evidence["authoritative"] is False
    assert evidence["classification"] == ib.UNAVAILABLE
    assert evidence["reason"] == "selected_contract_quote_freshness_config_invalid"


def test_invalid_selected_quote_age_cannot_activate_contract_quality_authority(monkeypatch):
    monkeypatch.setattr(ib, "_MAX_SELECTED_QUOTE_AGE_SECONDS", None)
    old = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=3)).isoformat()
    gate, pipeline = _run_with_fake_pipeline(
        monkeypatch,
        _selected_signal(
            quote_ts=old,
            quote_source=ib.SELECTED_CONTRACT_TRUSTED_SOURCE,
        ),
    )

    assert pipeline.calls[0][0] == "run_quick"
    evidence = gate["gate_diagnostics"]["selected_contract_evidence"]
    assert evidence["exists"] is False
    assert evidence["authoritative"] is False
    assert gate["risk_detail"]["reason_code"] != "CONTRACT_QUALITY_FAILED"
    assert gate["intel_status"] != "RISK_VETO"


def test_timezone_naive_selected_quote_timestamp_cannot_create_authority():
    naive_now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None).isoformat()
    evidence = ib._extract_selected_contract_evidence(
        _selected_signal(
            quote_ts=naive_now,
            quote_source=ib.SELECTED_CONTRACT_TRUSTED_SOURCE,
        ),
        client_id="client-a", execution_mode="LIVE",
    )

    assert evidence["exists"] is False
    assert evidence["authoritative"] is False
    assert evidence["classification"] == ib.UNAVAILABLE
    assert evidence["reason"] == "selected_contract_quote_timestamp_missing_or_invalid"


def test_authoritative_quote_timestamp_accepts_aware_and_numeric_values():
    aware = dt.datetime.now(dt.timezone.utc)
    assert ib._parse_authoritative_quote_timestamp(aware.isoformat()) is not None
    assert ib._parse_authoritative_quote_timestamp(aware.timestamp()) is not None
    assert ib._parse_authoritative_quote_timestamp(
        aware.replace(tzinfo=None).isoformat()
    ) is None


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
        patch("ap_intelligence.agents.ap_risk_manager.get_vix", return_value=_fresh_vix()),
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
        patch("ap_intelligence.agents.ap_risk_manager.get_vix", return_value=_fresh_vix()),
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
        patch("ap_intelligence.agents.ap_risk_manager.get_vix", return_value=_fresh_vix()),
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


def _evaluate_vix_payload(payload):
    from unittest.mock import patch
    from ap_intelligence.agents.ap_risk_manager import APRiskManager

    with (
        patch(
            "ap_intelligence.agents.ap_risk_manager.get_spy_trend",
            return_value={"trend": "BULL"},
        ),
        patch(
            "ap_intelligence.agents.ap_risk_manager.get_vix",
            return_value=payload,
        ),
    ):
        return APRiskManager(portfolio_value=25_000).evaluate(
            ticker="AAPL",
            direction="bullish",
            underlying_price=190.0,
            atr_value=2.5,
            option_delta=0.45,
            option_premium=2.5,
            bid_ask_spread_pct=0.05,
            open_interest=500,
            daily_volume_options=200,
            dte=1,
            contract_quality_authoritative=False,
            account_state_authoritative=False,
        )


def test_unavailable_vix_fails_open_as_advisory():
    result = _evaluate_vix_payload({
        "vix": None,
        "tradeable": False,
        "source": "yfinance:^VIX.fast_info.lastPrice",
        "observed_at": None,
        "classification": "UNAVAILABLE",
    })

    assert result.approved is True
    assert result.hard_veto is False
    assert result.reason_code == "VIX_UNAVAILABLE"
    assert result.authority_diagnostics["vix"]["authoritative"] is False
    assert result.authority_diagnostics["vix"]["policy_outcome"] == "VIX_ADVISORY"


@pytest.mark.parametrize(
    "payload",
    [
        _fresh_vix(float("nan")),
        _fresh_vix(18.0, observed_at="not-a-timestamp"),
        _fresh_vix(18.0, source="untrusted:vix-provider"),
    ],
    ids=["non_finite", "bad_timestamp", "unproven_source"],
)
def test_nonfinite_or_malformed_vix_fails_open(payload):
    result = _evaluate_vix_payload(payload)

    assert result.approved is True
    assert result.hard_veto is False
    assert result.reason_code == "VIX_UNAVAILABLE"
    assert result.authority_diagnostics["vix"]["authoritative"] is False


def test_stale_real_vix_fails_open_as_advisory():
    from ap_intelligence.tools.ap_data_tools import VIX_MAX_AGE_SECONDS

    stale_at = (
        dt.datetime.now(dt.timezone.utc)
        - dt.timedelta(seconds=VIX_MAX_AGE_SECONDS + 1)
    ).isoformat()
    result = _evaluate_vix_payload(_fresh_vix(18.0, observed_at=stale_at))

    assert result.approved is True
    assert result.hard_veto is False
    assert result.reason_code == "VIX_UNAVAILABLE"
    assert result.authority_diagnostics["vix"]["reason"] == "vix_timestamp_stale"


def test_fresh_real_vix_inside_policy_passes_vix_gate():
    result = _evaluate_vix_payload(_fresh_vix(18.0))

    assert result.approved is True
    assert result.hard_veto is False
    assert result.authority_diagnostics["vix"]["authoritative"] is True
    assert result.authority_diagnostics["vix"]["policy_outcome"] == "VIX_PASS"
    assert result.reason_code == "ADVISORY_DATA_UNAVAILABLE"


def test_fresh_real_vix_outside_policy_is_hard_cap():
    result = _evaluate_vix_payload(_fresh_vix(40.0))

    assert result.approved is False
    assert result.hard_veto is True
    assert result.reason_code == "VIX_POLICY_HARD_CAP"
    assert result.authority_diagnostics["vix"]["authoritative"] is True
    assert result.authority_diagnostics["vix"]["policy_outcome"] == "VIX_POLICY_HARD_CAP"


def test_stale_vix_cache_is_refetched_before_authority(monkeypatch):
    from ap_intelligence.tools import ap_data_tools
    from ap_intelligence.tools.ap_data_tools import VIX_MAX_AGE_SECONDS

    stale_at = (
        dt.datetime.now(dt.timezone.utc)
        - dt.timedelta(seconds=VIX_MAX_AGE_SECONDS + 1)
    ).isoformat()
    fake_yf = types.ModuleType("yfinance")
    fake_yf.Ticker = lambda _: types.SimpleNamespace(fast_info={"lastPrice": 18.5})
    monkeypatch.setitem(sys.modules, "yfinance", fake_yf)
    monkeypatch.setattr(ap_data_tools, "_cache_get", lambda _: _fresh_vix(18.0, observed_at=stale_at))
    monkeypatch.setattr(ap_data_tools, "_cache_set", lambda *_: None)

    result = ap_data_tools.get_vix()

    assert result["vix"] == 18.5
    assert result["classification"] == "PRODUCTION_EXACT"
    assert result["observed_at"] != stale_at


def test_unavailable_vix_flows_get_vix_to_pipeline_risk_detail_and_bridge(monkeypatch):
    from unittest.mock import MagicMock, patch
    from ap_intelligence.agents.ap_risk_manager import APRiskManager
    from ap_intelligence.ap_signal_pipeline import APSignalPipeline
    from ap_intelligence.tools import ap_data_tools

    fake_yf = types.ModuleType("yfinance")
    fake_yf.Ticker = lambda _: types.SimpleNamespace(fast_info={"lastPrice": None})
    monkeypatch.setitem(sys.modules, "yfinance", fake_yf)
    monkeypatch.setattr(ap_data_tools, "_cache_get", lambda _: None)
    monkeypatch.setattr(ap_data_tools, "_cache_set", lambda *_: None)

    pipeline = APSignalPipeline.__new__(APSignalPipeline)
    pipeline.technical = MagicMock()
    pipeline.technical.analyze.return_value = {
        "signal": "bullish",
        "confidence": 78.0,
        "breakdown": {},
    }
    pipeline.sentiment = MagicMock()
    pipeline.fundamentals = MagicMock()
    pipeline.risk_manager = APRiskManager(portfolio_value=25_000)
    pipeline.pm = MagicMock()
    pipeline.pm.decide.return_value = types.SimpleNamespace(
        action="execute",
        direction="bullish",
        contracts=1,
        max_usd=0.0,
        confidence=78.0,
        score=78.0,
        ev_score=78.0,
        reasoning="approved with VIX advisory",
        signal_breakdown={},
    )
    pipeline.audit = MagicMock()
    pipeline.use_sentiment = False
    pipeline.use_fundamentals = False
    pipeline.portfolio_value = 25_000
    pipeline.mode_cfg = types.SimpleNamespace(mode="LIVE")

    with (
        patch("ap_intelligence.ap_signal_pipeline.get_prices", return_value=None),
        patch(
            "ap_intelligence.agents.ap_risk_manager.get_spy_trend",
            return_value={"trend": "BULL"},
        ),
        patch(
            "ap_intelligence.agents.ap_risk_manager.get_vix",
            side_effect=ap_data_tools.get_vix,
        ),
    ):
        result = pipeline.run(
            ticker="AAPL",
            scanner_signal="bullish",
            scanner_confidence=78.0,
            underlying_price=190.0,
            atr_value=2.5,
            option_premium=2.5,
            option_delta=0.45,
            contract_quality_authoritative=False,
            account_state_authoritative=False,
        )

    risk_detail = result["risk_detail"]
    gate = ib._map_result(result, fallback_score=78.0)
    assert risk_detail["reason_code"] == "VIX_UNAVAILABLE"
    assert risk_detail["hard_veto"] is False
    assert risk_detail["authority_diagnostics"]["vix"]["classification"] == "UNAVAILABLE"
    assert risk_detail["authority_diagnostics"]["vix"]["authoritative"] is False
    assert gate["approved"] is True
    assert gate["intel_status"] == "VIX_ADVISORY"
    assert (
        gate["gate_diagnostics"]["canonical_admission_reason_code"]
        == "INTEL_VIX_ADVISORY"
    )
    assert gate["risk_detail"]["reason_code"] == "VIX_UNAVAILABLE"
    assert gate["risk_detail"]["hard_veto"] is False


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
