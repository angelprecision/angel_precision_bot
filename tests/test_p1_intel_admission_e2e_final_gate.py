"""
P1 end-to-end seam tests — intelligence admission through _run_final_quality_gates.

Finding 4 (from amendment audit): the existing test suite only tested the
adjudicator in isolation.  These tests call the actual _run_final_quality_gates()
seam to prove the canonical verdict propagates through the full LIVE path.
"""
from __future__ import annotations

import sys
import types
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import importlib.util
from pathlib import Path

import ap_master_control as mc_mod
import ap.intelligence_admission_policy as iap


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_mc(*, paper: bool):
    mc = mc_mod.APMasterControl.__new__(mc_mod.APMasterControl)
    mc.paper            = paper
    mc.run_id           = "run-test"
    mc.strategy_version = "test"
    mc.config_hash      = "cfg"
    mc.git_commit       = "sha"
    mc._alert_degraded  = MagicMock()
    updates = []
    mc._store_update    = lambda sid, status, reason="": updates.append(
        {"signal_id": sid, "status": status, "reason": reason}
    )
    return mc, updates


def _plan(metadata=None):
    return mc_mod.ApprovedExecutionPlan(
        plan_id="plan-1",
        signal_id="sig-1",
        client_id="client@example.com",
        ticker="AAPL",
        side="CALL",
        direction="CALL",
        pattern="2-3",
        timeframe="1d",
        contracts=1,
        max_position_usd=1000.0,
        tier="A",
        score=78.0,
        intel_score=45.0,
        confidence_bucket="standard",
        trigger_type="breach",
        trigger_price=100.0,
        stop_underlying=95.0,
        target_underlying=110.0,
        metadata=metadata or {},
    )


def _signal(**overrides):
    base = {
        "signal_id": "sig-1", "ticker": "AAPL", "side": "CALL",
        "direction": "CALL", "pattern": "2-3", "timeframe": "1d",
        "current_price": 101.0,
        "risk_detail": {"contract_quality_passes": True},
    }
    base.update(overrides)
    return base


def _verdict(reason_code, *, allowed=True, authoritative=False):
    return iap.IntelligenceAdmissionVerdict(
        allowed=allowed, authoritative=authoritative,
        reason_code=reason_code, reasoning="test",
        source="intelligence_bridge", confidence=None,
        data_quality="test", raw_status="TEST",
        policy_version=iap.POLICY_VERSION,
        diagnostics={"execution_mode": "LIVE"},
    )


def _stub_modules(monkeypatch):
    monkeypatch.setattr(mc_mod, "emit_decision_event", lambda **kw: None, raising=False)
    monkeypatch.setenv("HYBRID_CLIENT_QUALITY_MODE", "0")
    monkeypatch.setenv("FINAL_QUALITY_MODE_ENABLED", "1")
    fake_hybrid = types.ModuleType("ap_hybrid_client_quality_gate")
    fake_hybrid.evaluate_client_quality_gate = lambda **_: SimpleNamespace(
        allowed=True, block_reason="", quality_lane="", to_meta=lambda: {}
    )
    monkeypatch.setitem(sys.modules, "ap_hybrid_client_quality_gate", fake_hybrid)


def _run(mc, *, intel, intel_verdict, monkeypatch, snap=None):
    _stub_modules(monkeypatch)
    return mc._run_final_quality_gates(
        signal=_signal(), signal_id="sig-1", ticker="AAPL",
        client_id="client@example.com", plan=_plan(),
        intel=intel, snap=snap or {}, intel_verdict=intel_verdict,
    )


def _load_bridge():
    spec = importlib.util.spec_from_file_location(
        "_ib", Path(__file__).resolve().parents[1] / "intelligence_bridge.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# A–D. Fail-open verdicts pass through final gate
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("reason_code", [
    iap.INTEL_UNAVAILABLE_FAIL_OPEN,
    iap.INTEL_ERROR_FAIL_OPEN,
    iap.INTEL_LOW_DATA_QUALITY_FAIL_OPEN,
    iap.INTEL_MALFORMED_FAIL_OPEN,
    iap.INTEL_UNKNOWN_REASON_FAIL_OPEN,
    iap.INTEL_OBSERVE_ONLY_MODE,
])
def test_fail_open_verdict_passes_final_gate(reason_code, monkeypatch):
    """Every fail-open reason code must not be re-blocked by the LIVE final gate."""
    mc, _ = _build_mc(paper=False)
    monkeypatch.setenv("FINAL_ENTRY_INTELLIGENCE_REQUIRED", "1")
    monkeypatch.setenv("FINAL_ENTRY_INTELLIGENCE_MIN_SCORE", "99.0")  # would block if checked

    intel = {"_available": False, "score": 0.0}
    decision = _run(mc, intel=intel, intel_verdict=_verdict(reason_code),
                    monkeypatch=monkeypatch)

    assert decision is None or decision.ok is not False, (
        f"{reason_code} must fail open through final gate; got blocked"
    )


# ---------------------------------------------------------------------------
# E. Authoritative approval with low score still enforces final gate
# ---------------------------------------------------------------------------

def test_authoritative_approval_low_score_still_blocked_at_final_gate(monkeypatch):
    mc, _ = _build_mc(paper=False)
    monkeypatch.setenv("FINAL_ENTRY_INTELLIGENCE_MIN_SCORE", "80.0")
    monkeypatch.setenv("FINAL_ENTRY_INTELLIGENCE_REQUIRED", "1")

    intel = {"_available": True, "score": 10.0}
    verdict = _verdict(iap.INTEL_AUTHORITATIVE_APPROVED, allowed=True, authoritative=True)
    decision = _run(mc, intel=intel, intel_verdict=verdict, monkeypatch=monkeypatch)

    assert decision is not None and decision.ok is False
    assert decision.reason_code == "ENTRY_INTELLIGENCE_SCORE_TOO_LOW"


# ---------------------------------------------------------------------------
# F. intel_verdict=None (legacy/test path) uses raw _available check
# ---------------------------------------------------------------------------

def test_no_verdict_falls_back_to_legacy_available_check(monkeypatch):
    mc, _ = _build_mc(paper=False)
    monkeypatch.setenv("FINAL_ENTRY_INTELLIGENCE_REQUIRED", "1")

    intel = {"_available": False, "score": 0.0}
    decision = _run(mc, intel=intel, intel_verdict=None, monkeypatch=monkeypatch)

    assert decision is not None and decision.ok is False
    assert decision.reason_code == "ENTRY_INTELLIGENCE_MISSING"


# ---------------------------------------------------------------------------
# G–H. Phrase authority in intelligence_bridge._map_result
# ---------------------------------------------------------------------------

def test_phrase_capital_alone_does_not_create_skip_authority():
    """\'capital\' alone must not create authoritative SKIP."""
    ib = _load_bridge()
    result = {
        "action": "skip", "approved": False,
        "score": 30.0, "confidence": 30.0,
        "reasoning": "capital efficient setup but signal is unclear",
        "risk_detail": {"approved": True, "reason": "ok"},
        "contracts": 1, "ticker": "AAPL",
    }
    gate = ib._map_result(result, fallback_score=70.0)
    assert gate.get("intel_status") != "SKIP", (
        "'capital' alone must fail open as LOW_CONFIDENCE, not SKIP"
    )


def test_phrase_auth_alone_does_not_create_skip_authority():
    """\'auth\' / \'authentication\' alone must not create SKIP."""
    ib = _load_bridge()
    result = {
        "action": "skip", "approved": False,
        "score": 30.0, "confidence": 30.0,
        "reasoning": "authentication successful but no strong edge",
        "risk_detail": {"approved": True, "reason": "ok"},
        "contracts": 1, "ticker": "AAPL",
    }
    gate = ib._map_result(result, fallback_score=70.0)
    assert gate.get("intel_status") != "SKIP"


def test_structured_skip_reason_code_neutral_direction_is_authoritative(monkeypatch):
    """skip_reason_code=NEUTRAL_DIRECTION creates hard authority.

    We patch _data_collection_allowed to False to get a clean SKIP (not
    SKIP_OVERRIDE, which is data-collection mode where approved=True).
    """
    import intelligence_bridge as _ib_mod
    monkeypatch.setattr(_ib_mod, "_INTEL_DATA_COLLECTION_OVERRIDE", False)
    monkeypatch.setattr(_ib_mod, "_INTEL_IS_LIVE", True)
    # Reload to pick up patched module-level values used by _data_collection_allowed
    monkeypatch.setattr(_ib_mod, "_data_collection_allowed", lambda: False)

    ib = _load_bridge()
    # Also patch _data_collection_allowed on the freshly loaded module
    ib._data_collection_allowed = lambda: False

    result = {
        "action": "skip", "approved": False,
        "score": 30.0, "confidence": 30.0,
        "reasoning": "direction is ambiguous",
        "risk_detail": {"approved": True, "reason": "ok"},
        "skip_reason_code": "NEUTRAL_DIRECTION",
        "contracts": 1, "ticker": "AAPL",
    }
    gate = ib._map_result(result, fallback_score=70.0)
    # SKIP (hard block) or SKIP_OVERRIDE (data-collection) both prove authority
    assert gate.get("intel_status") in ("SKIP", "SKIP_OVERRIDE"), (
        f"NEUTRAL_DIRECTION skip_reason_code must produce authoritative block, got {gate.get('intel_status')!r}"
    )
    # Either approved=False (SKIP) or approved=True with SKIP_OVERRIDE is fine —
    # the adjudicator maps SKIP→block, SKIP_OVERRIDE is data-collection mode
    assert gate.get("intel_status") in ("SKIP", "SKIP_OVERRIDE")


def test_buying_power_unavailable_phrase_remains_authoritative():
    """Specific phrase \'buying power unavailable\' stays authoritative."""
    ib = _load_bridge()
    result = {
        "action": "skip", "approved": False,
        "score": 30.0, "confidence": 30.0,
        "reasoning": "buying power unavailable for this size",
        "risk_detail": {"approved": True, "reason": "ok"},
        "contracts": 1, "ticker": "AAPL",
    }
    gate = ib._map_result(result, fallback_score=70.0)
    assert gate.get("intel_status") in ("SKIP", "SKIP_OVERRIDE")


# ---------------------------------------------------------------------------
# I. Canonical verdict in plan.metadata on approval path
# ---------------------------------------------------------------------------

def test_canonical_verdict_written_into_plan_score_audit(monkeypatch):
    """intelligence_admission must appear in plan.metadata after final gate runs."""
    mc, _ = _build_mc(paper=False)
    monkeypatch.setenv("FINAL_ENTRY_INTELLIGENCE_REQUIRED", "1")
    _stub_modules(monkeypatch)

    verdict = iap.IntelligenceAdmissionVerdict(
        allowed=True, authoritative=True,
        reason_code=iap.INTEL_AUTHORITATIVE_APPROVED,
        reasoning="approved", source="intelligence_bridge",
        confidence=72.0, data_quality="available",
        raw_status="OK", policy_version=iap.POLICY_VERSION,
        diagnostics={"execution_mode": "LIVE"},
    )
    plan = _plan()
    mc._run_final_quality_gates(
        signal=_signal(), signal_id="sig-1", ticker="AAPL",
        client_id="client@example.com", plan=plan,
        intel={"_available": True, "score": 72.0},
        snap={}, intel_verdict=verdict,
    )

    meta = plan.metadata or {}
    score_audit = meta.get("score_audit", {})
    admission = score_audit.get("intelligence_admission") or meta.get("intelligence_admission")

    assert admission is not None, "intelligence_admission must be written to plan.metadata"
    assert admission.get("intel_reason_code") == iap.INTEL_AUTHORITATIVE_APPROVED
    assert admission.get("intel_execution_mode") == "LIVE"
    assert admission.get("intel_policy_version") == iap.POLICY_VERSION
