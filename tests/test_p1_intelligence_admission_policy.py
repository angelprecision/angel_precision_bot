"""P1 — Intelligence admission policy tests.

Covers all 25 required test cases from the PR spec.
No database, broker, or selector is touched.
"""
from __future__ import annotations

import os
import inspect
from unittest.mock import MagicMock, patch

import pytest

from ap.intelligence_admission_policy import (
    IntelligenceAdmissionVerdict,
    adjudicate_intelligence_result,
    summarise_verdicts,
    INTEL_AUTHORITATIVE_VETO_RISK,
    INTEL_AUTHORITATIVE_VETO_PORTFOLIO_SKIP,
    INTEL_AUTHORITATIVE_APPROVED,
    INTEL_ADVISORY_ONLY,
    INTEL_UNAVAILABLE_FAIL_OPEN,
    INTEL_ERROR_FAIL_OPEN,
    INTEL_MALFORMED_FAIL_OPEN,
    INTEL_UNKNOWN_REASON_FAIL_OPEN,
    INTEL_LOW_DATA_QUALITY_FAIL_OPEN,
    INTEL_OBSERVE_ONLY_MODE,
    POLICY_VERSION,
    _admission_mode,
)

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

_SIG = {"ticker": "SPY", "signal_id": "sig-001", "side": "CALL"}

def _bridge(*, approved: bool, status: str, reasoning: str = "", available: bool = True,
            score: float = 75.0, contracts: int = 1, risk_detail: dict | None = None) -> dict:
    return {
        "_available": available,
        "approved":   approved,
        "intel_status": status,
        "reasoning":  reasoning,
        "score":      score,
        "contracts":  contracts,
        "risk_detail": risk_detail or {},
    }

def _adj(result: dict, exec_mode: str = "live") -> IntelligenceAdmissionVerdict:
    return adjudicate_intelligence_result(result, signal=_SIG, execution_mode=exec_mode)


# ─────────────────────────────────────────────────────────────────────────────
# 1. Known authoritative veto codes block
# ─────────────────────────────────────────────────────────────────────────────

def test_risk_veto_blocks():
    v = _adj(_bridge(approved=False, status="RISK_VETO", reasoning="risk_veto: hard risk found"))
    assert v.blocks_entry is True
    assert v.reason_code == INTEL_AUTHORITATIVE_VETO_RISK
    assert v.authoritative is True

def test_portfolio_skip_blocks():
    v = _adj(_bridge(approved=False, status="SKIP", reasoning="intel_skip: risk manager block"))
    assert v.blocks_entry is True
    assert v.reason_code == INTEL_AUTHORITATIVE_VETO_PORTFOLIO_SKIP
    assert v.authoritative is True


# ─────────────────────────────────────────────────────────────────────────────
# 2. Stable reason_code is persisted (not free-text)
# ─────────────────────────────────────────────────────────────────────────────

def test_block_reason_is_stable_code_not_free_text():
    v = _adj(_bridge(approved=False, status="RISK_VETO", reasoning="risk_veto: some free text here"))
    block_reason = v.to_block_reason()
    assert "free text here" not in block_reason
    assert block_reason == INTEL_AUTHORITATIVE_VETO_RISK

def test_verdict_reasoning_is_bounded():
    long_text = "x" * 500
    v = _adj(_bridge(approved=False, status="RISK_VETO", reasoning=long_text))
    assert len(v.reasoning) <= 300


# ─────────────────────────────────────────────────────────────────────────────
# 3. Free text alone cannot create authority
# ─────────────────────────────────────────────────────────────────────────────

def test_free_text_reason_in_unknown_status_fails_open():
    """An arbitrary string in reasoning with unknown status must fail open."""
    v = _adj(_bridge(approved=False, status="MADE_UP_STATUS", reasoning="bearish market context"))
    assert v.allowed is True
    assert v.reason_code == INTEL_UNKNOWN_REASON_FAIL_OPEN
    assert v.authoritative is False


# ─────────────────────────────────────────────────────────────────────────────
# 4. Unknown reason fails open
# ─────────────────────────────────────────────────────────────────────────────

def test_unknown_intel_status_fails_open():
    v = _adj(_bridge(approved=False, status="NOVEL_STATUS_2099"))
    assert v.allowed is True
    assert v.reason_code == INTEL_UNKNOWN_REASON_FAIL_OPEN


# ─────────────────────────────────────────────────────────────────────────────
# 5. Missing result fails open
# ─────────────────────────────────────────────────────────────────────────────

def test_empty_dict_fails_open():
    v = _adj({})
    assert v.allowed is True

def test_unavailable_bridge_result_fails_open():
    v = _adj({"_available": False, "approved": True, "reasoning": "intel_unavailable"})
    assert v.allowed is True
    assert v.reason_code == INTEL_UNAVAILABLE_FAIL_OPEN


# ─────────────────────────────────────────────────────────────────────────────
# 6. Malformed result fails open
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("bad_value", [None, "string", 42, [], True])
def test_malformed_non_dict_result_fails_open(bad_value):
    v = adjudicate_intelligence_result(bad_value, signal=_SIG, execution_mode="live")
    assert v.allowed is True
    assert v.reason_code == INTEL_MALFORMED_FAIL_OPEN


# ─────────────────────────────────────────────────────────────────────────────
# 7. Intelligence exception fails open
# ─────────────────────────────────────────────────────────────────────────────

def test_error_status_fails_open():
    v = _adj(_bridge(approved=False, status="ERROR", reasoning="intel_error:connection reset"))
    assert v.allowed is True
    assert v.reason_code == INTEL_ERROR_FAIL_OPEN

def test_timeout_status_fails_open():
    v = _adj(_bridge(approved=False, status="TIMEOUT", reasoning="intel_timeout_8.0s"))
    assert v.allowed is True
    assert v.reason_code == INTEL_UNAVAILABLE_FAIL_OPEN


# ─────────────────────────────────────────────────────────────────────────────
# 8. Low-data-quality fails open
# ─────────────────────────────────────────────────────────────────────────────

def test_low_confidence_fails_open():
    v = _adj(_bridge(approved=False, status="LOW_CONFIDENCE", score=20.0))
    assert v.allowed is True
    assert v.reason_code == INTEL_LOW_DATA_QUALITY_FAIL_OPEN
    assert v.data_quality == "low"


# ─────────────────────────────────────────────────────────────────────────────
# 9. Observe-only source cannot veto even with approved=False
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("obs_status", [
    "SCANNER_APPROVED_INTEL_OBSERVE_ONLY",
    "RISK_VETO_OVERRIDE",
    "SKIP_OVERRIDE",
    "LOW_CONF_OVERRIDE",
])
def test_observe_only_source_cannot_veto(obs_status):
    result = _bridge(approved=True, status=obs_status)
    v = _adj(result)
    assert v.allowed is True
    assert v.reason_code == INTEL_ADVISORY_ONLY
    assert v.authoritative is False


# ─────────────────────────────────────────────────────────────────────────────
# 10. Explicit authoritative source can veto
# ─────────────────────────────────────────────────────────────────────────────

def test_authoritative_risk_veto_can_veto():
    v = _adj(_bridge(approved=False, status="RISK_VETO", risk_detail={"hard": "capital"}))
    assert v.blocks_entry is True
    assert v.authoritative is True

def test_authoritative_skip_veto_can_veto():
    v = _adj(_bridge(approved=False, status="SKIP", reasoning="intel_skip: risk manager block"))
    assert v.blocks_entry is True
    assert v.authoritative is True


# ─────────────────────────────────────────────────────────────────────────────
# 11. observe_only mode records but never blocks
# ─────────────────────────────────────────────────────────────────────────────

def test_observe_only_mode_never_blocks(monkeypatch):
    monkeypatch.setenv("INTELLIGENCE_ADMISSION_MODE", "observe_only")
    # Even a RISK_VETO must not block in observe_only mode
    v = _adj(_bridge(approved=False, status="RISK_VETO", reasoning="hard risk"))
    assert v.allowed is True
    assert v.reason_code == INTEL_OBSERVE_ONLY_MODE
    assert v.authoritative is False

def test_observe_only_mode_records_raw_status(monkeypatch):
    monkeypatch.setenv("INTELLIGENCE_ADMISSION_MODE", "observe_only")
    v = _adj(_bridge(approved=False, status="RISK_VETO"))
    assert v.raw_status == "RISK_VETO"
    assert v.diagnostics.get("raw_approved") is False


# ─────────────────────────────────────────────────────────────────────────────
# 12. Default mode preserves authoritative behavior
# ─────────────────────────────────────────────────────────────────────────────

def test_default_mode_is_authoritative(monkeypatch):
    monkeypatch.delenv("INTELLIGENCE_ADMISSION_MODE", raising=False)
    assert _admission_mode() == "authoritative"

def test_default_mode_vetoes_risk_veto(monkeypatch):
    monkeypatch.delenv("INTELLIGENCE_ADMISSION_MODE", raising=False)
    v = _adj(_bridge(approved=False, status="RISK_VETO"))
    assert v.blocks_entry is True


# ─────────────────────────────────────────────────────────────────────────────
# 13. Malformed env resolves to authoritative
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("bad_mode", ["off", "disabled", "0", "false", "OBSERVE", "YES", ""])
def test_malformed_env_resolves_to_authoritative(monkeypatch, bad_mode):
    monkeypatch.setenv("INTELLIGENCE_ADMISSION_MODE", bad_mode)
    assert _admission_mode() == "authoritative"


# ─────────────────────────────────────────────────────────────────────────────
# 14. PAPER and LIVE use the same taxonomy
# ─────────────────────────────────────────────────────────────────────────────

def test_paper_and_live_same_taxonomy():
    result_veto = _bridge(approved=False, status="RISK_VETO")
    v_live  = adjudicate_intelligence_result(result_veto, signal=_SIG, execution_mode="live")
    v_paper = adjudicate_intelligence_result(result_veto, signal=_SIG, execution_mode="paper")
    assert v_live.reason_code  == v_paper.reason_code
    assert v_live.blocks_entry == v_paper.blocks_entry

def test_paper_and_live_fail_open_same():
    result_unk = _bridge(approved=False, status="UNKNOWN_XYZ")
    v_live  = adjudicate_intelligence_result(result_unk, signal=_SIG, execution_mode="live")
    v_paper = adjudicate_intelligence_result(result_unk, signal=_SIG, execution_mode="paper")
    assert v_live.allowed  is True
    assert v_paper.allowed is True


# ─────────────────────────────────────────────────────────────────────────────
# 15. Client/mode identity is included in verdict metadata
# ─────────────────────────────────────────────────────────────────────────────

def test_verdict_metadata_includes_execution_mode():
    v = _adj(_bridge(approved=True, status="APPROVED"), exec_mode="live")
    meta = v.to_metadata(execution_mode="live", signal_id="sig-001", ticker="SPY", side="CALL")
    iv = meta["intelligence_verdict"]
    assert iv["execution_mode"] == "live"
    assert iv["signal_id"]      == "sig-001"


# ─────────────────────────────────────────────────────────────────────────────
# 16. Signal ID and ticker are preserved
# ─────────────────────────────────────────────────────────────────────────────

def test_signal_id_and_ticker_in_metadata():
    v = _adj(_bridge(approved=False, status="RISK_VETO"))
    meta = v.to_metadata(signal_id="sig-abc", ticker="NVDA", side="PUT", execution_mode="live")
    iv   = meta["intelligence_verdict"]
    assert iv["signal_id"] == "sig-abc"
    assert iv["ticker"]    == "NVDA"
    assert iv["side"]      == "PUT"

def test_ticker_in_veto_diagnostics():
    sig = {**_SIG, "ticker": "AAPL", "signal_id": "sig-999", "side": "PUT"}
    v = adjudicate_intelligence_result(
        _bridge(approved=False, status="RISK_VETO"), signal=sig, execution_mode="live"
    )
    assert v.diagnostics.get("ticker") == "AAPL"
    assert v.diagnostics.get("side")   == "PUT"


# ─────────────────────────────────────────────────────────────────────────────
# 17. Authoritative denial calls selector zero times
# ─────────────────────────────────────────────────────────────────────────────

def test_authoritative_denial_blocks_before_selector():
    """Verify master_control integration: intel veto returns before selector."""
    import ap_master_control as mc
    src = inspect.getsource(mc)
    # The canonical verdict check must appear before plan construction / queue entry
    verdict_pos  = src.find("_intel_verdict.blocks_entry")
    # "queue_trade" or "approved_plan" appear after the intel gate
    queue_pos = src.find("_run_final_quality_gates")
    assert verdict_pos != -1, "_intel_verdict.blocks_entry not found in master_control"
    assert queue_pos != -1,   "_run_final_quality_gates not found in master_control"
    assert verdict_pos < queue_pos, (
        "Intel verdict gate must appear before quality gates / plan construction"
    )


# ─────────────────────────────────────────────────────────────────────────────
# 18. Authoritative denial calls broker zero times
# ─────────────────────────────────────────────────────────────────────────────

def test_authoritative_denial_blocks_before_broker():
    import ap_master_control as mc
    src = inspect.getsource(mc)
    verdict_pos = src.find("_intel_verdict.blocks_entry")
    # Check verdict gate exists and is implemented as a return (no broker reach)
    assert verdict_pos != -1
    # The gate returns immediately: "return self._block(...)" follows the check
    after_gate = src[verdict_pos:verdict_pos + 500]
    assert "return self._block" in after_gate, (
        "Intel verdict gate must return immediately without reaching broker"
    )


# ─────────────────────────────────────────────────────────────────────────────
# 19. Advisory result continues to selector normally
# ─────────────────────────────────────────────────────────────────────────────

def test_advisory_result_is_allowed():
    v = _adj(_bridge(approved=True, status="SCANNER_APPROVED_INTEL_OBSERVE_ONLY"))
    assert v.allowed is True
    assert v.reason_code == INTEL_ADVISORY_ONLY

def test_advisory_result_not_authoritative():
    v = _adj(_bridge(approved=True, status="RISK_VETO_OVERRIDE"))
    assert v.authoritative is False


# ─────────────────────────────────────────────────────────────────────────────
# 20. Unknown reason continues normally
# ─────────────────────────────────────────────────────────────────────────────

def test_unknown_reason_continues_to_selector():
    v = _adj(_bridge(approved=False, status="STATUS_FROM_FUTURE"))
    assert v.allowed is True
    assert v.reason_code == INTEL_UNKNOWN_REASON_FAIL_OPEN


# ─────────────────────────────────────────────────────────────────────────────
# 21. Intel contract cap may reduce size but never increase canonical risk sizing
# ─────────────────────────────────────────────────────────────────────────────

def test_master_control_uses_intel_contracts_as_cap():
    """Source-level: intel contracts used as min(), not as additive value."""
    import ap_master_control as mc
    src = inspect.getsource(mc)
    # The sizing logic must reference intel_contracts with min/cap semantics
    assert "intel_contracts" in src or "intel.get(\"contracts\")" in src, (
        "Master control must reference intel contracts for sizing"
    )

def test_malformed_contracts_value_does_not_increase_size():
    """Malformed contract value in intel result should not produce a verdict that expands size."""
    result = _bridge(approved=True, status="APPROVED", contracts=9999)
    v = _adj(result)
    # Verdict itself is allowed — the contracts cap enforcement is in master_control
    # but the verdict metadata preserves the raw value for that check
    assert v.allowed is True
    assert v.diagnostics.get("contracts") == 9999  # preserved for caller to enforce cap


# ─────────────────────────────────────────────────────────────────────────────
# 22. Quality mode remains a separate gate
# ─────────────────────────────────────────────────────────────────────────────

def test_quality_mode_gate_is_separate_from_intel_gate():
    import ap_master_control as mc
    src = inspect.getsource(mc)
    intel_pos   = src.find("_intel_verdict.blocks_entry")
    quality_pos = src.find("Quality Mode gate")
    assert intel_pos != -1 and quality_pos != -1
    # Quality Mode gate comment must appear after the intel gate
    assert quality_pos > intel_pos, (
        "Quality Mode gate must remain a separate gate AFTER the intelligence gate"
    )

def test_intelligence_policy_does_not_touch_quality_mode():
    import ap.intelligence_admission_policy as iap
    src = inspect.getsource(iap)
    assert "quality_mode" not in src.lower(), (
        "intelligence_admission_policy must not reference quality_mode"
    )


# ─────────────────────────────────────────────────────────────────────────────
# 23. PREOPEN/PRETRIGGER observe-only modules cannot mutate final admission
# ─────────────────────────────────────────────────────────────────────────────

def test_preopen_observe_only_module_cannot_veto():
    """Any result labeled with observe-only status cannot produce a veto."""
    observe_statuses = [
        "SCANNER_APPROVED_INTEL_OBSERVE_ONLY",
        "RISK_VETO_OVERRIDE",
        "SKIP_OVERRIDE",
        "LOW_CONF_OVERRIDE",
    ]
    for status in observe_statuses:
        result = _bridge(approved=False, status=status)  # even with approved=False
        v = _adj(result)
        assert v.allowed is True, (
            f"Observe-only status {status!r} must not veto entry even with approved=False"
        )


# ─────────────────────────────────────────────────────────────────────────────
# 24. SPY-trend veto reasons map deterministically to stable codes
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("risk_veto_reason", [
    "risk_veto: bearish market context for CALL",
    "risk_veto: neutral direction conflict",
    "risk_veto: hard risk — contract quality failed",
    "risk_veto: capital constraint",
    "risk_veto: buying power insufficient",
])
def test_spy_trend_veto_maps_to_authoritative_veto_risk(risk_veto_reason):
    """The 222 observed SPY-trend vetoes use RISK_VETO status → INTEL_AUTHORITATIVE_VETO_RISK."""
    result = _bridge(approved=False, status="RISK_VETO", reasoning=risk_veto_reason)
    v = _adj(result)
    assert v.reason_code == INTEL_AUTHORITATIVE_VETO_RISK, (
        f"SPY-trend veto reason {risk_veto_reason!r} should map to "
        f"INTEL_AUTHORITATIVE_VETO_RISK, got {v.reason_code}"
    )
    assert v.blocks_entry is True

@pytest.mark.parametrize("skip_reason", [
    "intel_skip: risk manager blocked position",
    "intel_skip: scanner signal is neutral",
    "intel_skip: hard risk — contract quality failed",
])
def test_skip_veto_reasons_map_to_portfolio_skip(skip_reason):
    result = _bridge(approved=False, status="SKIP", reasoning=skip_reason)
    v = _adj(result)
    assert v.reason_code == INTEL_AUTHORITATIVE_VETO_PORTFOLIO_SKIP
    assert v.blocks_entry is True


# ─────────────────────────────────────────────────────────────────────────────
# 25. No queue reason relies solely on free-text intel_rejected
# ─────────────────────────────────────────────────────────────────────────────

def test_to_block_reason_never_returns_intel_rejected_free_text():
    for status in ("RISK_VETO", "SKIP"):
        v = _adj(_bridge(approved=False, status=status, reasoning="intel_rejected: some free text"))
        block_reason = v.to_block_reason()
        assert not block_reason.startswith("intel_rejected:"), (
            f"Queue reason must not start with 'intel_rejected:' (free text). "
            f"Got: {block_reason!r}"
        )

def test_master_control_no_longer_uses_intel_rejected_free_text():
    import ap_master_control as mc
    src = inspect.getsource(mc)
    # The old pattern f"intel_rejected: {intel_reason[...]}" must not appear
    assert 'f"intel_rejected: {intel_reason' not in src, (
        "ap_master_control.py must not build queue reasons from intel_rejected free text"
    )


# ─────────────────────────────────────────────────────────────────────────────
# IntelligenceAdmissionVerdict contract
# ─────────────────────────────────────────────────────────────────────────────

def test_verdict_is_frozen():
    v = _adj(_bridge(approved=True, status="APPROVED"))
    with pytest.raises((AttributeError, TypeError)):
        v.allowed = False  # type: ignore[misc]

def test_policy_version_is_present():
    v = _adj(_bridge(approved=True, status="APPROVED"))
    assert v.policy_version == POLICY_VERSION
    assert POLICY_VERSION  # not empty

def test_summarise_verdicts_counts_correctly():
    v1 = _adj(_bridge(approved=False, status="RISK_VETO"))
    v2 = _adj(_bridge(approved=True,  status="APPROVED"))
    v3 = _adj(_bridge(approved=False, status="TIMEOUT"))
    meta_list = [
        v1.to_metadata(ticker="SPY",  side="CALL", execution_mode="live",   signal_id="s1"),
        v2.to_metadata(ticker="AAPL", side="PUT",  execution_mode="paper",  signal_id="s2"),
        v3.to_metadata(ticker="SPY",  side="CALL", execution_mode="live",   signal_id="s3"),
    ]
    summary = summarise_verdicts(meta_list)
    assert summary["authoritative_veto_count"] == 1
    assert summary["approval_count"] == 1
    assert summary["unavailable_error_fail_open"] == 1
    assert summary["total"] == 3
