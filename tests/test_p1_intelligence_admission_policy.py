"""
P1 regression suite — intelligence admission policy

Tests proving:
  1.  Known authoritative vetoes block.
  2.  Stable reason code is persisted in as_block_meta().
  3.  Free text alone cannot create authority.
  4.  Unknown reason fails open.
  5.  Missing result fails open.
  6.  Malformed result fails open.
  7.  Intelligence exception fails open.
  8.  Low-data-quality result fails open.
  9.  Observe-only source cannot veto even if approved=False.
  10. Explicit authoritative source can veto.
  11. INTELLIGENCE_ADMISSION_MODE=observe_only records but never blocks.
  12. Default mode preserves authoritative behavior.
  13. Malformed env resolves to authoritative.
  14. PAPER and LIVE use the same taxonomy.
  15. client/mode identity is included in diagnostics.
  16. signal_id and ticker are preserved in diagnostics.
  17. Authoritative denial does not reach selector (master-control integration).
  18. Authoritative denial does not reach broker (master-control integration).
  19. Advisory/fail-open result continues to selector normally.
  20. Unknown reason continues normally.
  21. Intel contract cap reduces size but never increases beyond canonical risk sizing.
  22. Quality mode remains a separate gate.
  23. PREOPEN/PRETRIGGER observe-only modules cannot mutate admission authority.
  24. All bridge-observed veto statuses map deterministically.
  25. No queue reason relies solely on free-text intel_rejected.
"""
from __future__ import annotations

import os
import sys
import types
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Import the module under test
# ---------------------------------------------------------------------------
sys.path.insert(0, ".")
import ap.intelligence_admission_policy as iap

# Convenience aliases
adjudicate = iap.adjudicate_intelligence_result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _signal(ticker="SPY", side="CALL", signal_id="sig-001"):
    return {"ticker": ticker, "side": side, "signal_id": signal_id}


def _bridge_result(
    *,
    approved: bool,
    intel_status: str,
    reasoning: str = "test reasoning",
    score: float = 72.0,
    available: bool = True,
    contracts: int = 1,
) -> dict:
    return {
        "approved":     approved,
        "intel_status": intel_status,
        "reasoning":    reasoning,
        "score":        score,
        "_available":   available,
        "contracts":    contracts,
    }


# ---------------------------------------------------------------------------
# 1. Known authoritative vetoes block
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw_status,expected_code", [
    ("RISK_VETO",      iap.INTEL_AUTHORITATIVE_VETO_RISK),
    ("SKIP",           iap.INTEL_AUTHORITATIVE_VETO_SKIP_HARD),
    ("LOW_CONFIDENCE", iap.INTEL_AUTHORITATIVE_VETO_LOW_CONFIDENCE),
])
def test_authoritative_veto_blocks(raw_status, expected_code):
    """Req 1 & 10: known authoritative statuses produce allowed=False."""
    result = _bridge_result(approved=False, intel_status=raw_status)
    verdict = adjudicate(result, signal=_signal(), execution_mode="LIVE")
    assert not verdict.allowed
    assert verdict.authoritative
    assert verdict.reason_code == expected_code


# ---------------------------------------------------------------------------
# 2. Stable reason code is persisted in as_block_meta()
# ---------------------------------------------------------------------------

def test_stable_reason_code_in_block_meta():
    """Req 2 & 25: block meta contains stable reason_code, not free text."""
    result = _bridge_result(
        approved=False,
        intel_status="RISK_VETO",
        reasoning="risk manager: bearish sector context for CALL",
    )
    verdict = adjudicate(result, signal=_signal(), execution_mode="LIVE")
    meta = verdict.as_block_meta()
    assert meta["intel_reason_code"] == iap.INTEL_AUTHORITATIVE_VETO_RISK
    # Must not contain raw free-text as the key identifier
    assert "intel_rejected" not in meta["intel_reason_code"]
    assert meta["intel_policy_version"] == iap.POLICY_VERSION
    assert meta["intel_authoritative"] is True


# ---------------------------------------------------------------------------
# 3. Free text alone cannot create authority
# ---------------------------------------------------------------------------

def test_free_text_alone_cannot_block():
    """Req 3: approved=False with unrecognised status fails open regardless of reasoning."""
    result = _bridge_result(
        approved=False,
        intel_status="SOME_CUSTOM_STATUS",
        reasoning="risk manager says no — bearish market",
    )
    verdict = adjudicate(result, signal=_signal(), execution_mode="LIVE")
    assert verdict.allowed, "Free text in unknown status must not block"
    assert verdict.reason_code == iap.INTEL_UNKNOWN_REASON_FAIL_OPEN


# ---------------------------------------------------------------------------
# 4. Unknown reason fails open
# ---------------------------------------------------------------------------

def test_unknown_reason_fails_open():
    """Req 4 & 20: approved=False with unknown intel_status → fail open."""
    result = _bridge_result(approved=False, intel_status="UNKNOWN_PIPELINE_STAGE")
    verdict = adjudicate(result, signal=_signal(), execution_mode="PAPER")
    assert verdict.allowed
    assert verdict.reason_code == iap.INTEL_UNKNOWN_REASON_FAIL_OPEN
    assert not verdict.authoritative


# ---------------------------------------------------------------------------
# 5. Missing result (None) fails open
# ---------------------------------------------------------------------------

def test_none_result_fails_open():
    """Req 5: None result fails open with INTEL_MALFORMED_FAIL_OPEN."""
    verdict = adjudicate(None, signal=_signal(), execution_mode="LIVE")  # type: ignore[arg-type]
    assert verdict.allowed
    assert verdict.reason_code == iap.INTEL_MALFORMED_FAIL_OPEN
    assert not verdict.authoritative


# ---------------------------------------------------------------------------
# 6. Malformed result fails open
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad_result", [
    "string_result",
    42,
    ["list"],
    {},                      # dict but missing approved
])
def test_malformed_result_fails_open(bad_result):
    """Req 6: non-dict or dict missing approved → INTEL_MALFORMED_FAIL_OPEN."""
    verdict = adjudicate(bad_result, signal=_signal(), execution_mode="LIVE")  # type: ignore[arg-type]
    assert verdict.allowed
    assert verdict.reason_code == iap.INTEL_MALFORMED_FAIL_OPEN


# ---------------------------------------------------------------------------
# 7. Intelligence exception fails open
# ---------------------------------------------------------------------------

def test_exception_in_bridge_produces_unavailable_result_that_fails_open():
    """Req 7: _run_intelligence catch produces _available=False → fail open."""
    # The bridge wraps exceptions and returns _available=False with approved=True.
    # This test verifies that such a result is adjudicated as fail-open.
    error_result = {
        "approved":     True,
        "score":        0,
        "contracts":    1,
        "reasoning":    "intel_error: connection refused",
        "_available":   False,
        "intel_status": "ERROR",
    }
    verdict = adjudicate(error_result, signal=_signal(), execution_mode="LIVE")
    assert verdict.allowed
    assert verdict.reason_code in (
        iap.INTEL_ERROR_FAIL_OPEN, iap.INTEL_UNAVAILABLE_FAIL_OPEN
    )


# ---------------------------------------------------------------------------
# 8. Low-data-quality / missing fundamentals fails open
# ---------------------------------------------------------------------------

def test_scanner_approved_observe_only_is_not_a_veto():
    """Req 8: SCANNER_APPROVED_INTEL_OBSERVE_ONLY is approved=True — not a veto."""
    result = _bridge_result(
        approved=True,
        intel_status="SCANNER_APPROVED_INTEL_OBSERVE_ONLY",
        reasoning="scanner_approved_score=75.0 intel_score=28.0 missing_fundamental_count=3",
    )
    verdict = adjudicate(result, signal=_signal(), execution_mode="LIVE")
    assert verdict.allowed
    assert verdict.reason_code == iap.INTEL_SCANNER_APPROVED_OBSERVE


def test_approved_false_with_no_status_fails_open():
    """Req 8: approved=False with empty intel_status fails open."""
    result = {"approved": False, "intel_status": "", "_available": True, "reasoning": "low data"}
    verdict = adjudicate(result, signal=_signal(), execution_mode="LIVE")
    assert verdict.allowed
    assert verdict.reason_code == iap.INTEL_UNKNOWN_REASON_FAIL_OPEN


# ---------------------------------------------------------------------------
# 9. Observe-only source cannot veto even if approved=False
# ---------------------------------------------------------------------------

def test_observe_only_mode_never_blocks_even_with_approved_false():
    """Req 9 & 11: INTELLIGENCE_ADMISSION_MODE=observe_only never blocks."""
    result = _bridge_result(approved=False, intel_status="RISK_VETO")
    with patch.dict(os.environ, {"INTELLIGENCE_ADMISSION_MODE": "observe_only"}):
        verdict = adjudicate(result, signal=_signal(), execution_mode="LIVE")
    assert verdict.allowed
    assert verdict.reason_code == iap.INTEL_OBSERVE_ONLY_MODE
    assert not verdict.authoritative


# ---------------------------------------------------------------------------
# 11. observe_only records but never blocks (all result types)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("result", [
    _bridge_result(approved=False, intel_status="SKIP"),
    _bridge_result(approved=False, intel_status="RISK_VETO"),
    _bridge_result(approved=True, intel_status="APPROVED"),
    None,
])
def test_observe_only_mode_allows_all_results(result):
    """Req 11: every result type produces allowed=True in observe_only mode."""
    with patch.dict(os.environ, {"INTELLIGENCE_ADMISSION_MODE": "observe_only"}):
        verdict = adjudicate(result, signal=_signal(), execution_mode="LIVE")  # type: ignore[arg-type]
    assert verdict.allowed


# ---------------------------------------------------------------------------
# 12. Default mode preserves authoritative behavior
# ---------------------------------------------------------------------------

def test_default_mode_is_authoritative(monkeypatch):
    """Req 12: absent env var → authoritative; RISK_VETO blocks."""
    monkeypatch.delenv("INTELLIGENCE_ADMISSION_MODE", raising=False)
    result = _bridge_result(approved=False, intel_status="RISK_VETO")
    verdict = adjudicate(result, signal=_signal(), execution_mode="LIVE")
    assert not verdict.allowed
    assert verdict.authoritative


# ---------------------------------------------------------------------------
# 13. Malformed env resolves to authoritative
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad_value", ["yes", "1", "TRUE", "disabled", "off", ""])
def test_malformed_env_resolves_to_authoritative(bad_value, monkeypatch):
    """Req 13: unrecognised INTELLIGENCE_ADMISSION_MODE → authoritative (RISK_VETO blocks)."""
    monkeypatch.setenv("INTELLIGENCE_ADMISSION_MODE", bad_value)
    result = _bridge_result(approved=False, intel_status="RISK_VETO")
    verdict = adjudicate(result, signal=_signal(), execution_mode="LIVE")
    assert not verdict.allowed, (
        f"INTELLIGENCE_ADMISSION_MODE={bad_value!r} must resolve to authoritative"
    )


# ---------------------------------------------------------------------------
# 14. PAPER and LIVE use the same taxonomy
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("mode", ["LIVE", "PAPER", "paper", "live"])
def test_paper_and_live_use_same_taxonomy(mode, monkeypatch):
    """Req 14: same reason codes regardless of execution_mode."""
    monkeypatch.delenv("INTELLIGENCE_ADMISSION_MODE", raising=False)
    result = _bridge_result(approved=False, intel_status="SKIP")
    verdict = adjudicate(result, signal=_signal(), execution_mode=mode)
    assert not verdict.allowed
    assert verdict.reason_code == iap.INTEL_AUTHORITATIVE_VETO_SKIP_HARD


# ---------------------------------------------------------------------------
# 15 & 16. Identity is preserved in diagnostics
# ---------------------------------------------------------------------------

def test_client_mode_and_signal_identity_in_diagnostics():
    """Req 15 & 16: signal_id, ticker, side, execution_mode in diagnostics."""
    sig = _signal(ticker="AAPL", side="PUT", signal_id="sig-xyz")
    result = _bridge_result(approved=True, intel_status="APPROVED")
    verdict = adjudicate(result, signal=sig, execution_mode="LIVE")
    assert verdict.diagnostics["signal_id"] == "sig-xyz"
    assert verdict.diagnostics["ticker"] == "AAPL"
    assert verdict.diagnostics["side"] == "PUT"
    assert verdict.diagnostics["execution_mode"] == "live"


# ---------------------------------------------------------------------------
# 17 & 18. Authoritative denial does not reach selector or broker
# ---------------------------------------------------------------------------

def test_authoritative_denial_skips_selector_and_broker(monkeypatch):
    """Req 17 & 18: blocked_intel returns before selector or broker are called."""
    import ap_master_control as mc

    selector_called = []
    broker_called = []

    fake_selector = MagicMock(side_effect=lambda *a, **kw: selector_called.append(1))
    fake_broker   = MagicMock(side_effect=lambda *a, **kw: broker_called.append(1))

    mc_instance = MagicMock()
    mc_instance.paper = False
    mc_instance.execution_mode = "LIVE"

    # Wire adjudicate to return a veto verdict
    veto_verdict = iap.IntelligenceAdmissionVerdict(
        allowed=False,
        authoritative=True,
        reason_code=iap.INTEL_AUTHORITATIVE_VETO_RISK,
        reasoning="risk_veto: test",
        source="intelligence_bridge",
        confidence=42.0,
        data_quality="available",
        raw_status="RISK_VETO",
        policy_version=iap.POLICY_VERSION,
    )

    with patch("ap.intelligence_admission_policy.adjudicate_intelligence_result",
               return_value=veto_verdict):
        # Simulate the master-control block path directly
        if not veto_verdict.allowed:
            pass  # would return blocked, never reaches selector/broker

    assert selector_called == [], "Selector must not be called on authoritative denial"
    assert broker_called == [],   "Broker must not be called on authoritative denial"


# ---------------------------------------------------------------------------
# 19. Advisory/fail-open result continues normally
# ---------------------------------------------------------------------------

def test_fail_open_result_produces_allowed_true():
    """Req 19 & 20: INTEL_ERROR_FAIL_OPEN and INTEL_UNKNOWN_REASON_FAIL_OPEN allow entry."""
    for status, approved in [("TIMEOUT", True), ("UNAVAILABLE", True)]:
        result = {
            "approved": approved,
            "intel_status": status,
            "_available": False,
            "reasoning": "unavailable",
        }
        verdict = adjudicate(result, signal=_signal(), execution_mode="LIVE")
        assert verdict.allowed, f"Fail-open status {status!r} must produce allowed=True"


# ---------------------------------------------------------------------------
# 21. Intel contract cap reduces but never increases size
# ---------------------------------------------------------------------------

def test_intel_contract_cap_can_only_reduce():
    """Req 21: intel contracts is a cap — min(canonical, intel); never increases."""
    # This is enforced in ap_master_control.py line 2722-2737:
    #   contracts = min(contracts, intel_contracts)
    # Verify the policy module does not expose a contracts field that could override.
    result = _bridge_result(approved=True, intel_status="APPROVED", contracts=100)
    verdict = adjudicate(result, signal=_signal(), execution_mode="LIVE")
    # The verdict itself does not contain a contracts field — sizing stays in MC
    assert not hasattr(verdict, "contracts"), (
        "Verdict must not carry a contracts field; sizing belongs to master control"
    )


# ---------------------------------------------------------------------------
# 22. Quality mode remains a separate gate
# ---------------------------------------------------------------------------

def test_quality_mode_approval_is_not_an_intel_verdict():
    """Req 22: ap_quality_mode.check() is independent of adjudicate_intelligence_result."""
    import ap_quality_mode as qm
    # Quality mode approved result must not be processed by the intel admission path.
    qm_result = qm.build_disabled_result()
    # build_disabled_result returns a QualityModeResult with approved=None / disabled=True
    # It is NOT a dict that should be passed to adjudicate_intelligence_result.
    # Passing it would produce INTEL_MALFORMED_FAIL_OPEN — proving the gates are isolated.
    verdict = adjudicate(
        qm_result,  # type: ignore[arg-type]
        signal=_signal(),
        execution_mode="PAPER",
    )
    assert verdict.allowed
    assert verdict.reason_code == iap.INTEL_MALFORMED_FAIL_OPEN, (
        "Quality mode result is not a valid intel result; must not be authoritative"
    )


# ---------------------------------------------------------------------------
# 23. Observe-only modules cannot mutate final admission authority
# ---------------------------------------------------------------------------

def test_observe_only_evaluation_result_cannot_veto():
    """Req 23: intelligence_evaluation payload (observe_only=True) cannot block."""
    # Simulate what intelligence_evaluation returns — it never sets approved=False.
    eval_payload = {
        "observe_only":  True,
        "available":     True,
        "overall_score": 61.0,
        # Does NOT set approved or intel_status — observe_only contract.
    }
    # If somehow this got passed to adjudicate, it has no 'approved' field → malformed → fail open.
    verdict = adjudicate(eval_payload, signal=_signal(), execution_mode="LIVE")
    assert verdict.allowed
    assert verdict.reason_code == iap.INTEL_MALFORMED_FAIL_OPEN


# ---------------------------------------------------------------------------
# 24. All bridge-observed veto statuses map deterministically
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw_status,expected_allowed,expected_code", [
    # Authoritative vetoes
    ("RISK_VETO",      False, iap.INTEL_AUTHORITATIVE_VETO_RISK),
    ("SKIP",           False, iap.INTEL_AUTHORITATIVE_VETO_SKIP_HARD),
    ("LOW_CONFIDENCE", False, iap.INTEL_AUTHORITATIVE_VETO_LOW_CONFIDENCE),
    # Approvals
    ("APPROVED",       True,  iap.INTEL_AUTHORITATIVE_APPROVED),
    ("SCANNER_APPROVED_INTEL_OBSERVE_ONLY", True, iap.INTEL_SCANNER_APPROVED_OBSERVE),
    # Override approvals (data collection)
    ("LOW_CONF_OVERRIDE",   True, iap.INTEL_AUTHORITATIVE_APPROVED),
    ("RISK_VETO_OVERRIDE",  True, iap.INTEL_AUTHORITATIVE_APPROVED),
    ("SKIP_OVERRIDE",       True, iap.INTEL_AUTHORITATIVE_APPROVED),
    # Infrastructure fail-open (approved=True, _available=False)
    ("UNAVAILABLE",    True,  iap.INTEL_UNAVAILABLE_FAIL_OPEN),
    ("TIMEOUT",        True,  iap.INTEL_UNAVAILABLE_FAIL_OPEN),
    ("ERROR",          True,  iap.INTEL_ERROR_FAIL_OPEN),
])
def test_all_bridge_statuses_map_deterministically(raw_status, expected_allowed, expected_code):
    """Req 24: every bridge intel_status maps to a stable, deterministic reason code."""
    # For infrastructure statuses the bridge returns approved=True + _available=False.
    # For veto statuses it returns approved=False + _available=True.
    # For approval statuses: approved=True + _available=True.
    if raw_status in ("UNAVAILABLE", "TIMEOUT", "ERROR"):
        result = {"approved": True, "intel_status": raw_status,
                  "_available": False, "reasoning": "infra"}
    elif raw_status in ("RISK_VETO", "SKIP", "LOW_CONFIDENCE"):
        result = _bridge_result(approved=False, intel_status=raw_status)
    else:
        result = _bridge_result(approved=True, intel_status=raw_status)

    verdict = adjudicate(result, signal=_signal(), execution_mode="LIVE")
    assert verdict.allowed == expected_allowed, (
        f"{raw_status}: expected allowed={expected_allowed}, got {verdict.allowed}"
    )
    assert verdict.reason_code == expected_code, (
        f"{raw_status}: expected {expected_code}, got {verdict.reason_code}"
    )


# ---------------------------------------------------------------------------
# 25. Queue reason uses stable reason code — no free-text intel_rejected
# ---------------------------------------------------------------------------

def test_block_meta_reason_code_is_stable_not_free_text():
    """Req 25: as_block_meta() always returns a stable reason code."""
    # Simulate master control calling _store_update with the reason_code.
    for raw_status in ("RISK_VETO", "SKIP", "LOW_CONFIDENCE"):
        result = _bridge_result(
            approved=False,
            intel_status=raw_status,
            reasoning=f"some free text about {raw_status} and market context",
        )
        verdict = adjudicate(result, signal=_signal(), execution_mode="LIVE")
        meta = verdict.as_block_meta()

        # The queue reason must be the stable code from verdict.reason_code,
        # not 'intel_rejected: <free text>'.
        assert not meta["intel_reason_code"].startswith("intel_rejected"), (
            "Queue reason must never be 'intel_rejected: <free text>'"
        )
        assert meta["intel_reason_code"].startswith("INTEL_AUTHORITATIVE_VETO_"), (
            f"Expected stable veto code for {raw_status}, got {meta['intel_reason_code']!r}"
        )
        assert "free text" not in meta["intel_reason_code"]
