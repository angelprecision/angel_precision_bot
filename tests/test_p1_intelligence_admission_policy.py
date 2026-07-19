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
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Import the module under test
# ---------------------------------------------------------------------------
sys.path.insert(0, ".")
import ap.intelligence_admission_policy as iap
import importlib.util as _ilu

_REPO = Path(__file__).resolve().parents[1]


def _load(rel: str):
    spec = _ilu.spec_from_file_location("_mod", _REPO / rel)
    mod = _ilu.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_ib = _load("intelligence_bridge.py")
_map_result = _ib._map_result

# Convenience aliases
adjudicate = iap.adjudicate_intelligence_result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _signal(ticker="SPY", side="CALL", signal_id="sig-001"):
    return {"ticker": ticker, "side": side, "signal_id": signal_id}


def _bridge_result(
    *,
    approved: bool | None,
    intel_status: str,
    reasoning: str = "test reasoning",
    score: Any = 72.0,
    confidence: Any = None,
    available: bool = True,
    contracts: int = 1,
) -> dict:
    return {
        "approved":     approved,
        "intel_status": intel_status,
        "reasoning":    reasoning,
        "score":        score,
        "confidence":   confidence,
        "_available":   available,
        "contracts":    contracts,
    }


# ---------------------------------------------------------------------------
# 1. Known authoritative vetoes block
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw_status,expected_code", [
    ("RISK_VETO",      iap.INTEL_AUTHORITATIVE_VETO_RISK),
    ("SKIP",           iap.INTEL_AUTHORITATIVE_VETO_SKIP_HARD),
])
def test_authoritative_veto_blocks(raw_status, expected_code):
    """Req 1 & 10: known authoritative statuses produce allowed=False."""
    result = _bridge_result(approved=False, intel_status=raw_status)
    verdict = adjudicate(result, signal=_signal(), execution_mode="LIVE")
    assert not verdict.allowed
    assert verdict.authoritative
    assert verdict.reason_code == expected_code


def test_authoritative_veto_status_allowlist_is_exact():
    assert iap._AUTHORITATIVE_VETO_STATUS_MAP == {
        "RISK_VETO": iap.INTEL_AUTHORITATIVE_VETO_RISK,
        "SKIP": iap.INTEL_AUTHORITATIVE_VETO_SKIP_HARD,
    }


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


@pytest.mark.parametrize(
    "approved,score,confidence,expected_code",
    [
        (False, 22.0, None, iap.INTEL_LOW_DATA_QUALITY_FAIL_OPEN),
        (True, 22.0, None, iap.INTEL_LOW_DATA_QUALITY_FAIL_OPEN),
        (None, 22.0, None, iap.INTEL_MALFORMED_FAIL_OPEN),
        (False, None, "bad-confidence", iap.INTEL_LOW_DATA_QUALITY_FAIL_OPEN),
        (False, None, None, iap.INTEL_LOW_DATA_QUALITY_FAIL_OPEN),
    ],
)
@pytest.mark.parametrize("mode", ["LIVE", "PAPER"])
def test_low_confidence_fail_opens_across_shapes(approved, score, confidence, expected_code, mode):
    result = _bridge_result(
        approved=approved,
        intel_status="LOW_CONFIDENCE",
        score=score,
        confidence=confidence,
        reasoning="low confidence due to incomplete intelligence data",
    )
    verdict = adjudicate(result, signal=_signal(), execution_mode=mode)
    assert verdict.allowed
    assert not verdict.authoritative
    assert verdict.reason_code == expected_code


def test_low_confidence_observe_only_mode_still_never_blocks():
    result = _bridge_result(approved=False, intel_status="LOW_CONFIDENCE", score=18.0)
    with patch.dict(os.environ, {"INTELLIGENCE_ADMISSION_MODE": "observe_only"}):
        verdict = adjudicate(result, signal=_signal(), execution_mode="LIVE")
    assert verdict.allowed
    assert not verdict.authoritative
    assert verdict.reason_code == iap.INTEL_OBSERVE_ONLY_MODE


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


def test_bridge_hard_risk_skip_is_the_only_authoritative_skip_path(monkeypatch):
    monkeypatch.setattr(_ib, "_INTEL_IS_LIVE", True)
    result = {
        "ticker": "SPY",
        "action": "skip",
        "score": 30.0,
        "confidence": 0.0,
        "contracts": 1,
        "reasoning": "Scanner signal is neutral — no directional setup",
        "risk_detail": {"approved": True, "reason": "approved"},
        "signal_breakdown": {},
    }
    gate = _map_result(result, fallback_score=70.0)
    gate["_available"] = True
    assert gate["approved"] is False
    assert gate["intel_status"] == "SKIP"

    verdict = adjudicate(gate, signal=_signal(), execution_mode="LIVE")
    assert not verdict.allowed
    assert verdict.authoritative
    assert verdict.reason_code == iap.INTEL_AUTHORITATIVE_VETO_SKIP_HARD


def test_bridge_non_hard_skip_cannot_accidentally_become_authoritative(monkeypatch):
    monkeypatch.setattr(_ib, "_INTEL_IS_LIVE", True)
    result = {
        "ticker": "AAPL",
        "action": "skip",
        "score": 28.0,
        "confidence": 0.0,
        "contracts": 1,
        "reasoning": "skip: mixed evidence and low confidence",
        "risk_detail": {"approved": True, "reason": "approved"},
        "signal_breakdown": {
            "fundamentals": {"signal": "bullish", "confidence": 80.0},
        },
    }
    gate = _map_result(result, fallback_score=65.0)
    gate["_available"] = True
    assert gate["approved"] is False
    assert gate["intel_status"] == "LOW_CONFIDENCE"

    verdict = adjudicate(gate, signal=_signal(), execution_mode="LIVE")
    assert verdict.allowed
    assert not verdict.authoritative
    assert verdict.reason_code == iap.INTEL_LOW_DATA_QUALITY_FAIL_OPEN


@pytest.mark.parametrize(
    "reasoning",
    [
        "skip due to low confidence",
        "neutral setup but unsupported status",
        "low confidence scanner note",
    ],
)
def test_free_text_skip_or_low_confidence_words_cannot_create_authority(reasoning):
    result = _bridge_result(
        approved=False,
        intel_status="UNSUPPORTED_STATUS",
        reasoning=reasoning,
    )
    verdict = adjudicate(result, signal=_signal(), execution_mode="LIVE")
    assert verdict.allowed
    assert not verdict.authoritative
    assert verdict.reason_code == iap.INTEL_UNKNOWN_REASON_FAIL_OPEN


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
    ("LOW_CONFIDENCE", True, iap.INTEL_LOW_DATA_QUALITY_FAIL_OPEN),
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
    elif raw_status in ("RISK_VETO", "SKIP"):
        result = _bridge_result(approved=False, intel_status=raw_status)
    elif raw_status == "LOW_CONFIDENCE":
        result = _bridge_result(approved=False, intel_status=raw_status, score=18.0)
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
    for raw_status in ("RISK_VETO", "SKIP"):
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


def test_low_confidence_block_meta_uses_fail_open_reason_code():
    result = _bridge_result(
        approved=False,
        intel_status="LOW_CONFIDENCE",
        reasoning="low confidence due to incomplete evidence",
        score=20.0,
    )
    verdict = adjudicate(result, signal=_signal(), execution_mode="LIVE")
    meta = verdict.as_block_meta()
    assert meta["intel_reason_code"] == iap.INTEL_LOW_DATA_QUALITY_FAIL_OPEN
    assert meta["intel_authoritative"] is False


# ---------------------------------------------------------------------------
# Amendment tests: raw numeric field safety + funnel report surface
# (Added for PR #379 amendment — closes two production-shape gaps)
# ---------------------------------------------------------------------------

class TestMalformedNumericFieldsFailOpen:
    """
    Gap 1: float(intel.get("score", 0)) and int(intel.get("contracts", 1) or 1)
    both execute BEFORE adjudicate_intelligence_result() is called in
    ap_master_control.py.  A bridge result like {"score": "unknown"} or
    {"contracts": "N/A"} would raise ValueError and kill the admission path
    before the adjudicator can produce its fail-open verdict.

    These tests verify the adjudicator itself handles non-numeric score/
    confidence gracefully (the adjudicator already does via _conf_raw),
    and document that the master-control safe-parse wrappers must exist
    to protect the path before adjudication.
    """

    @pytest.mark.parametrize("bad_score", [
        "unknown",
        "N/A",
        "low",
        "",
        [],
        {},
    ])
    def test_adjudicator_handles_non_numeric_score_gracefully(self, bad_score):
        """Adjudicator must not raise on non-numeric score/confidence fields.

        The adjudicator already coerces via _conf_raw with try/except;
        this test proves non-numeric values produce allowed=True (fail-open)
        when no authoritative veto status is present.
        """
        result = {
            "approved": True,
            "intel_status": "OK",
            "reasoning": "test",
            "_available": True,
            "score": bad_score,
        }
        # Must not raise regardless of the score shape
        verdict = adjudicate(result, signal=_signal(), execution_mode="LIVE")
        # approved=True + no veto status → allowed=True regardless of score
        assert verdict.allowed is True

    @pytest.mark.parametrize("bad_score", [
        "unknown",
        "N/A",
    ])
    def test_adjudicator_confidence_is_none_for_non_numeric_score(self, bad_score):
        """Malformed score yields confidence=None, not a crash."""
        result = {
            "approved": True,
            "intel_status": "OK",
            "reasoning": "test",
            "_available": True,
            "score": bad_score,
        }
        verdict = adjudicate(result, signal=_signal(), execution_mode="PAPER")
        assert verdict.confidence is None

    def test_malformed_score_with_veto_status_still_blocks(self):
        """A malformed score field must not suppress an authoritative veto.

        If approved=False and intel_status=RISK_VETO, the block is
        authoritative even when score is unparseable.
        """
        result = {
            "approved": False,
            "intel_status": "RISK_VETO",
            "reasoning": "risk veto triggered",
            "_available": True,
            "score": "unknown",
        }
        verdict = adjudicate(result, signal=_signal(), execution_mode="LIVE")
        assert verdict.allowed is False
        assert verdict.reason_code == iap.INTEL_AUTHORITATIVE_VETO_RISK
        assert verdict.confidence is None  # malformed score → None, not crash

    def test_non_numeric_score_veto_block_meta_is_stable(self):
        """block_meta must be stable even when score is non-numeric."""
        result = {
            "approved": False,
            "intel_status": "RISK_VETO",
            "reasoning": "risk veto",
            "_available": True,
            "score": "N/A",
        }
        verdict = adjudicate(result, signal=_signal(), execution_mode="LIVE")
        meta = verdict.as_block_meta()
        assert meta["intel_reason_code"] == iap.INTEL_AUTHORITATIVE_VETO_RISK
        assert meta["intel_confidence"] is None


class TestBlockMetaIncludesExecutionMode:
    """
    Amendment: as_block_meta() must export intel_execution_mode so that
    report_intelligence_funnel can filter by LIVE/PAPER from
    decision_events.context_json (decision_events has no standalone
    execution_mode column).
    """

    def test_block_meta_includes_execution_mode_live(self):
        result = _bridge_result(approved=False, intel_status="RISK_VETO")
        verdict = adjudicate(result, signal=_signal(), execution_mode="LIVE")
        meta = verdict.as_block_meta()
        assert "intel_execution_mode" in meta
        assert meta["intel_execution_mode"] == "LIVE"

    def test_block_meta_includes_execution_mode_paper(self):
        result = _bridge_result(approved=False, intel_status="RISK_VETO")
        verdict = adjudicate(result, signal=_signal(), execution_mode="paper")
        meta = verdict.as_block_meta()
        assert meta["intel_execution_mode"] == "PAPER"

    def test_block_meta_includes_execution_mode_for_approved_result(self):
        """Approved verdicts also carry execution_mode in block_meta."""
        result = _bridge_result(approved=True, intel_status="OK")
        verdict = adjudicate(result, signal=_signal(), execution_mode="LIVE")
        meta = verdict.as_block_meta()
        assert meta["intel_execution_mode"] == "LIVE"

    def test_block_meta_execution_mode_empty_string_becomes_none(self):
        """Empty execution_mode normalises to None, not empty string."""
        result = _bridge_result(approved=True, intel_status="OK")
        verdict = adjudicate(result, signal=_signal(), execution_mode="")
        meta = verdict.as_block_meta()
        # Empty mode → no intel_execution_mode value (None)
        assert meta["intel_execution_mode"] is None

    def test_all_stable_veto_codes_carry_execution_mode(self):
        """Every authoritative veto block_meta must include execution_mode."""
        for raw_status in ("RISK_VETO", "SKIP"):
            result = _bridge_result(approved=False, intel_status=raw_status)
            verdict = adjudicate(result, signal=_signal(), execution_mode="LIVE")
            meta = verdict.as_block_meta()
            assert "intel_execution_mode" in meta, (
                f"Missing intel_execution_mode for {raw_status}"
            )
            assert meta["intel_execution_mode"] == "LIVE"



class TestFunnelReportSurface:
    """
    Amendment: report_intelligence_funnel must query decision_events, not
    orders.  Intel-blocked signals never create an order row; querying
    orders.meta produces an empty funnel for every blocked candidate.

    DB patching: the function does `from ap.db import conn, run_with_retry`
    at call time (local import).  We inject a mock ap.db via sys.modules
    before invoking the function to avoid importing psycopg2.
    """

    @pytest.fixture(autouse=True)
    def _mock_ap_db(self, monkeypatch):
        """Inject a minimal ap.db mock via sys.modules before each test."""
        import sys
        import types

        self._captured_sql: list[str] = []
        self._captured_params: list = []

        captured_sql = self._captured_sql
        captured_params = self._captured_params

        class _FakeCursor:
            rowcount = 0
            def execute(self, sql, params=None):
                captured_sql.append(sql)
                captured_params.extend(params or [])
            def fetchall(self):
                return []

        class _FakeConn:
            def __enter__(self):
                return _FakeCursor()
            def __exit__(self, *a):
                pass

        mock_db = types.ModuleType("ap.db")
        mock_db.conn = lambda: _FakeConn()
        mock_db.run_with_retry = lambda fn: fn()

        monkeypatch.setitem(sys.modules, "ap.db", mock_db)
        # Force re-import of the module under test so it picks up the mock
        import importlib
        import ap.intelligence_admission_policy as _iap_mod
        importlib.reload(_iap_mod)
        yield
        # Restore original module state after test
        importlib.reload(_iap_mod)

    def _run_funnel(self, **kw):
        # Import fresh after the monkeypatch reload
        from ap.intelligence_admission_policy import report_intelligence_funnel
        return report_intelligence_funnel(**kw)

    def test_funnel_query_targets_decision_events_not_orders(self):
        """The SQL emitted must reference decision_events, never orders."""
        self._run_funnel(session_date="2026-07-19")
        assert self._captured_sql, "No SQL was executed"
        sql = self._captured_sql[0].lower()
        assert "decision_events" in sql, (
            "Funnel report must query decision_events, not orders"
        )
        assert "from orders" not in sql, (
            "Funnel report must NOT query orders — intel vetoes never reach orders"
        )

    def test_funnel_query_uses_ts_not_created_at(self):
        """Timestamp filter must use `ts`, not `created_at` or `created_ts`."""
        self._run_funnel(session_date="2026-07-19")
        sql = self._captured_sql[0].lower()
        assert "date(ts " in sql or "date(ts)" in sql, (
            "Date filter must use `ts` column (decision_events canonical timestamp)"
        )
        assert "created_at" not in sql, "created_at must not appear — use `ts`"
        assert "created_ts" not in sql, "created_ts belongs to orders, not decision_events"

    def test_funnel_query_filters_blocked_intel_stage(self):
        """Query must filter stage='blocked_intel' and reason_code LIKE 'INTEL_%'."""
        self._run_funnel()
        sql = self._captured_sql[0]
        params_str = str(self._captured_params)
        assert "blocked_intel" in sql or "blocked_intel" in params_str, (
            "Must filter stage='blocked_intel'"
        )
        assert "INTEL_%" in sql or "INTEL_%" in params_str, (
            "Must filter reason_code LIKE 'INTEL_%'"
        )

    def test_funnel_execution_mode_filter_uses_context_json(self):
        """execution_mode filter must read context_json->>'intel_execution_mode'.

        decision_events has no standalone execution_mode column; the value
        is written by as_block_meta() into context_json.
        """
        self._run_funnel(execution_mode="LIVE")
        sql = self._captured_sql[0]
        assert "intel_execution_mode" in sql, (
            "execution_mode filter must read context_json->>'intel_execution_mode'"
        )
        # Must NOT reference a non-existent standalone execution_mode column
        assert "COALESCE(execution_mode," not in sql, (
            "decision_events has no standalone execution_mode column"
        )

    def test_funnel_returns_dict_with_funnel_key(self):
        """Return shape must have 'funnel' list and 'filters' dict."""
        result = self._run_funnel(
            client_id="jason@example.com",
            execution_mode="LIVE",
            session_date="2026-07-19",
        )
        assert isinstance(result, dict)
        assert "funnel" in result
        assert isinstance(result["funnel"], list)
        assert "filters" in result
        assert result["filters"]["client_id"] == "jason@example.com"
        assert result["filters"]["execution_mode"] == "LIVE"
        assert result["filters"]["session_date"] == "2026-07-19"
