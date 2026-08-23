"""Phase 1 Gate G production-shape regressions.

These tests stay on the intelligence/admission seam. They intentionally do not
instantiate a broker, selector, queue, proof writer, or position manager.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import intelligence_bridge as bridge
import ap.intelligence_admission_policy as policy


def _signal(**overrides):
    signal = {
        "ticker": "AAPL",
        "side": "CALL",
        "score": 70.0,
        "signal_id": "sig-001",
        "canonical_signal_id": "canon-001",
    }
    signal.update(overrides)
    return signal


def _selected(**overrides):
    signal = _signal(
        client_id="client-a",
        execution_mode="LIVE",
        selected_occ="AAPL260821C00150000",
        quote_timestamp=datetime.now(timezone.utc).isoformat(),
        quote_source="tradier",
        bid=1.00,
        ask=1.10,
        spread=0.10,
        delta=0.45,
        open_interest=500,
        option_volume=100,
        dte=0,
    )
    signal.update(overrides)
    return signal


def _run(signal, *, client_id="client-a", execution_mode="LIVE"):
    return bridge.run_intelligence_check(
        signal,
        underlying_price=200.0,
        client_id=client_id,
        execution_mode=execution_mode,
    )


def test_no_selected_occ_is_advisory_and_never_builds_legacy_pipeline(monkeypatch):
    monkeypatch.setattr(bridge, "_get_pipeline", Mock(side_effect=AssertionError("legacy pipeline called")))
    result = _run(_signal())
    assert result["intel_status"] == "CONTRACT_EVIDENCE_UNAVAILABLE"
    assert result["contract_quality_state"] == "NOT_AVAILABLE_YET"
    assert result["contract_authority"] is False
    assert result["contracts"] == 0
    assert result["intel_score"] is None


def test_explicit_dte_zero_survives_and_is_not_replaced_by_one():
    result = _run(_selected(dte=0))
    assert result["contract_evidence"]["dte"] == 0
    assert result["contract_quality_state"] == "AVAILABLE_ADVISORY"
    assert result["contract_authority"] is False


def test_explicit_dte_one_survives():
    result = _run(_selected(dte=1))
    assert result["contract_evidence"]["dte"] == 1


def test_missing_dte_is_unavailable_not_fabricated():
    signal = _selected()
    signal.pop("dte")
    result = _run(signal)
    assert result["intel_status"] == "CONTRACT_EVIDENCE_INVALID"
    assert result["contract_quality_state"] == "UNAVAILABLE"


def test_malformed_dte_is_unavailable_not_favorable():
    result = _run(_selected(dte="0.0"))
    assert result["intel_status"] == "CONTRACT_EVIDENCE_INVALID"
    assert result["contracts"] == 0


def test_exact_occ_identity_and_fresh_quote_are_only_advisory_in_phase_one():
    result = _run(_selected())
    assert result["intel_status"] == "CONTRACT_EVIDENCE_AVAILABLE_ADVISORY"
    assert result["execution_mode"] == "LIVE"
    assert result["client_id"] == "client-a"
    assert result["signal_id"] == "sig-001"
    assert result["canonical_signal_id"] == "canon-001"
    assert result["contract_evidence"]["occ_symbol"] == "AAPL260821C00150000"
    assert result["contract_evidence"]["source"] == "tradier"


def test_wrong_client_is_unavailable_hold():
    result = _run(_selected(), client_id="client-b")
    assert result["intel_status"] == "CONTRACT_EVIDENCE_INVALID"
    assert result["contract_quality_state"] == "UNAVAILABLE"
    assert result["contract_authority"] is False


def test_wrong_execution_mode_is_unavailable_hold():
    result = _run(_selected(), execution_mode="PAPER")
    assert result["intel_status"] == "CONTRACT_EVIDENCE_INVALID"
    assert result["contract_authority"] is False


def test_conflicting_occ_aliases_are_unavailable():
    result = _run(_selected(contract_symbol="MSFT260821C00400000"))
    assert result["intel_status"] == "CONTRACT_EVIDENCE_INVALID"
    assert result["contract_quality_state"] == "UNAVAILABLE"


def test_stale_quote_is_unavailable():
    stale = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
    result = _run(_selected(quote_timestamp=stale))
    assert result["intel_status"] == "CONTRACT_EVIDENCE_INVALID"
    assert result["contract_evidence"].get("quote_age_seconds", 0) > 120


def test_malformed_or_unordered_quote_is_unavailable():
    malformed = _run(_selected(bid=True))
    unordered = _run(_selected(bid=1.20, ask=1.10))
    assert malformed["intel_status"] == "CONTRACT_EVIDENCE_INVALID"
    assert unordered["intel_status"] == "CONTRACT_EVIDENCE_INVALID"


def test_conflicting_duplicate_quote_sources_are_unavailable():
    signal = _selected(selected_contract={"source": "polygon"})
    result = _run(signal)
    assert result["intel_status"] == "CONTRACT_EVIDENCE_INVALID"
    assert result["contract_quality_state"] == "UNAVAILABLE"


def test_scanner_score_zero_stays_zero_and_missing_intelligence_has_no_second_score():
    result = _run(_signal(score=0))
    assert result["score"] == 0.0
    assert result["scanner_score"] == 0.0
    assert result["intel_score"] is None
    assert result["contracts"] == 0


def test_malformed_scanner_score_does_not_become_a_passing_default():
    result = _run(_signal(score="nan"))
    assert result["score"] == 0.0
    assert result["intel_score"] is None
    assert result["contract_authority"] is False


def test_account_equity_is_diagnostic_only_for_all_environment_shapes(monkeypatch):
    for raw, expected in ((None, "MISSING"), ("not-a-number", "MALFORMED"), ("25000", "PRESENT")):
        if raw is None:
            monkeypatch.delenv("ACCOUNT_EQUITY", raising=False)
        else:
            monkeypatch.setenv("ACCOUNT_EQUITY", raw)
        result = _run(_signal())
        assert result["account_equity_diagnostic"]["state"] == expected
        assert result["account_equity_diagnostic"]["authority"] == "DIAGNOSTIC_ONLY"
        assert "account_equity" not in result
        assert result["contract_authority"] is False


def test_paper_and_live_mode_are_not_inferred_or_cross_contaminated():
    paper = _run(_signal(), execution_mode="PAPER")
    live = _run(_signal(), execution_mode="LIVE")
    assert paper["execution_mode"] == "PAPER"
    assert live["execution_mode"] == "LIVE"
    assert paper["contracts"] == live["contracts"] == 0
    assert paper["intel_score"] is live["intel_score"] is None


def test_admission_policy_classifies_phase_one_evidence_as_non_authoritative():
    signal = _signal(client_id="client-a")
    for status, available in (
        ("CONTRACT_EVIDENCE_UNAVAILABLE", False),
        ("CONTRACT_EVIDENCE_INVALID", False),
        ("CONTRACT_EVIDENCE_AVAILABLE_ADVISORY", True),
    ):
        result = {
            "approved": True,
            "score": 70.0,
            "intel_score": None,
            "intel_status": status,
            "_available": available,
            "contract_quality_state": "AVAILABLE_ADVISORY" if available else "UNAVAILABLE",
            "contract_authority": False,
            "client_id": "client-a",
            "execution_mode": "LIVE",
            "signal_id": "sig-001",
            "canonical_signal_id": "canon-001",
        }
        verdict = policy.adjudicate_intelligence_result(
            result,
            signal=signal,
            execution_mode="LIVE",
        )
        assert verdict.allowed is True
        assert verdict.authoritative is False
        assert verdict.reason_code == policy.INTEL_ADVISORY_ONLY


def test_master_preserves_bridge_availability_and_passes_exact_identity(monkeypatch):
    import ap_master_control as master

    calls = []

    def fake_check(*args, **kwargs):
        calls.append((args, kwargs))
        return {
            "approved": True,
            "score": 70.0,
            "intel_score": None,
            "contracts": 0,
            "intel_status": "CONTRACT_EVIDENCE_UNAVAILABLE",
            "_available": False,
            "contract_quality_state": "NOT_AVAILABLE_YET",
            "contract_authority": False,
        }

    monkeypatch.setattr(master, "_INTEL_AVAILABLE", True)
    monkeypatch.setattr(master, "_run_intel_check", fake_check)
    instance = object.__new__(master.APMasterControl)
    result = instance._run_intelligence(
        _signal(),
        client_id="client-a",
        execution_mode="LIVE",
    )
    assert calls[0][1]["client_id"] == "client-a"
    assert calls[0][1]["execution_mode"] == "LIVE"
    assert result["_available"] is False
    assert result["contracts"] == 0
    assert result["contract_authority"] is False


def test_gate_g_helper_has_no_money_path_calls(monkeypatch):
    for name in ("_get_pipeline", "_persist_audit"):
        monkeypatch.setattr(bridge, name, Mock(side_effect=AssertionError(f"{name} called")))
    result = _run(_signal())
    assert result["intel_status"] == "CONTRACT_EVIDENCE_UNAVAILABLE"
    assert result["contracts"] == 0
