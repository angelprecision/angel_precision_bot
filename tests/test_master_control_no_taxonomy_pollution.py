from __future__ import annotations

import inspect
from unittest.mock import MagicMock

import ap_master_control as mc_mod


MC_SRC = inspect.getsource(mc_mod.APMasterControl.evaluate)


def _build_mc(*, paper: bool):
    events = []
    updates = []

    mc = mc_mod.APMasterControl.__new__(mc_mod.APMasterControl)
    mc.paper = paper
    mc.run_id = "run-1"
    mc.strategy_version = "test"
    mc.config_hash = "cfg"
    mc.git_commit = "sha"
    mc._alert_degraded = MagicMock()
    mc._store_update = lambda signal_id, status, reason="": updates.append(
        {"signal_id": signal_id, "status": status, "reason": reason}
    )
    return mc, events, updates


def test_final_quality_gate_runs_before_dedup_queue_and_approve():
    gate_idx = MC_SRC.find("self._run_final_quality_gates(")
    dedup_idx = MC_SRC.find("self._persist_dedup(")
    queued_idx = MC_SRC.find('self._store_update(signal_id, "queued"')
    approve_idx = MC_SRC.find('decision="APPROVE"')

    assert gate_idx > 0, "evaluate() must call _run_final_quality_gates()"
    assert dedup_idx > gate_idx, "final gate must run before dedup persistence"
    # PR #225: master_control no longer stamps queued directly — that status
    # is now owned by the queue/worker layer, written only after
    # create_entry_order() succeeds (see tests/test_queue_admission_truth.py).
    # The old "queued before queue admission is real" bug (BUG-MC-2) is fixed
    # by removing the stamp from evaluate() entirely, not by reordering it.
    assert queued_idx == -1, "master_control must not stamp queued directly"
    assert approve_idx > gate_idx, "final gate must run before APPROVE event emission"


def test_live_reject_does_not_write_paper_metadata(monkeypatch):
    mc, events, updates = _build_mc(paper=False)
    monkeypatch.setattr(mc_mod, "emit_decision_event", lambda **kw: events.append(kw))

    decision = mc._run_final_quality_gates(
        signal={
            "signal_id": "sig-live",
            "ticker": "AAPL",
            "side": "CALL",
            "direction": "CALL",
            "timeframe": "1d",
            "current_price": 101.0,
            "risk_detail": {"contract_quality_passes": True},
        },
        signal_id="sig-live",
        ticker="AAPL",
        client_id="client@example.com",
        plan=mc_mod.ApprovedExecutionPlan(
            plan_id="plan-live",
            signal_id="sig-live",
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
            intel_score=0.0,
            confidence_bucket="standard",
            trigger_type="breach",
            trigger_price=100.0,
            stop_underlying=95.0,
            target_underlying=110.0,
            metadata={},
        ),
        intel={"_available": False, "score": 0.0, "risk_detail": {"contract_quality_passes": True}},
        snap={},
    )

    assert decision is not None and decision.reason_code == "ENTRY_INTELLIGENCE_MISSING"
    assert [evt["decision"] for evt in events] == ["REJECT"]
    assert all(evt.get("context") in (None, {}) for evt in events)
    assert all("paper" not in update["reason"].lower() for update in updates)


def test_paper_reject_does_not_pollute_live_proof_stream(monkeypatch):
    mc, events, updates = _build_mc(paper=True)
    monkeypatch.setattr(mc_mod, "emit_decision_event", lambda **kw: events.append(kw))

    decision = mc._run_final_quality_gates(
        signal={
            "signal_id": "sig-paper",
            "ticker": "AAPL",
            "side": "CALL",
            "direction": "CALL",
            "timeframe": "1d",
            "current_price": 101.0,
            "contract_quality_failed": True,
        },
        signal_id="sig-paper",
        ticker="AAPL",
        client_id="client@example.com",
        plan=mc_mod.ApprovedExecutionPlan(
            plan_id="plan-paper",
            signal_id="sig-paper",
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
            intel_score=50.0,
            confidence_bucket="standard",
            trigger_type="breach",
            trigger_price=100.0,
            stop_underlying=95.0,
            target_underlying=110.0,
            metadata={},
        ),
        intel={"_available": True, "score": 50.0, "risk_detail": {"contract_quality_passes": False}},
        snap={},
    )

    assert decision is not None and decision.reason_code == "CONTRACT_QUALITY_BLOCK"
    assert [evt["decision"] for evt in events] == ["REJECT"]
    assert all(evt.get("context") in (None, {}) for evt in events)
    assert all("live" not in update["reason"].lower() for update in updates)
