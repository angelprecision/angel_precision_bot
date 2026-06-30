from __future__ import annotations

import sys
import types
from types import SimpleNamespace
from unittest.mock import MagicMock

import ap_master_control as mc_mod


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


def _plan():
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
        metadata={},
    )


def _signal(**overrides):
    base = {
        "signal_id": "sig-1",
        "ticker": "AAPL",
        "side": "CALL",
        "direction": "CALL",
        "pattern": "2-3",
        "timeframe": "1d",
        "current_price": 101.0,
        "risk_detail": {"contract_quality_passes": True},
    }
    base.update(overrides)
    return base


def test_hybrid_gate_blocks_before_any_approve_or_queue_mutation(monkeypatch):
    mc, events, updates = _build_mc(paper=False)
    monkeypatch.setattr(mc_mod, "emit_decision_event", lambda **kw: events.append(kw))

    fake_hybrid = types.ModuleType("ap_hybrid_client_quality_gate")
    fake_hybrid.evaluate_client_quality_gate = lambda **_: SimpleNamespace(
        allowed=False,
        block_reason="client_daily_pattern_not_whitelisted",
        quality_lane="DAILY_CLIENT",
        to_meta=lambda: {"confirmation_required": True},
    )
    monkeypatch.setitem(sys.modules, "ap_hybrid_client_quality_gate", fake_hybrid)
    monkeypatch.setenv("HYBRID_CLIENT_QUALITY_MODE", "1")

    decision = mc._run_final_quality_gates(
        signal=_signal(),
        signal_id="sig-1",
        ticker="AAPL",
        client_id="client@example.com",
        plan=_plan(),
        intel={"_available": True, "score": 45.0, "risk_detail": {"contract_quality_passes": True}},
        snap={"total_trades": 0, "daily_trades": 0, "intraday_trades": 0, "symbol_trades": {}},
    )

    assert decision is not None
    assert decision.ok is False
    assert decision.stage == "REJECTED"
    assert decision.reason_code == "HYBRID_CLIENT_QUALITY_BLOCK"
    assert all(evt["decision"] != "APPROVE" for evt in events)
    assert [evt["decision"] for evt in events] == ["REJECT"]
    assert all(update["status"] != "queued" for update in updates)


def test_missing_intelligence_metadata_live_rejects_before_queue_mutation(monkeypatch):
    mc, events, updates = _build_mc(paper=False)
    monkeypatch.setattr(mc_mod, "emit_decision_event", lambda **kw: events.append(kw))
    monkeypatch.setenv("HYBRID_CLIENT_QUALITY_MODE", "0")

    decision = mc._run_final_quality_gates(
        signal=_signal(),
        signal_id="sig-1",
        ticker="AAPL",
        client_id="client@example.com",
        plan=_plan(),
        intel={"_available": False, "score": 0.0, "risk_detail": {"contract_quality_passes": True}},
        snap={},
    )

    assert decision is not None
    assert decision.ok is False
    assert decision.reason_code == "ENTRY_INTELLIGENCE_MISSING"
    assert all(evt["decision"] != "APPROVE" for evt in events)
    assert all(update["status"] != "queued" for update in updates)


def test_low_intelligence_score_live_rejects_before_queue_mutation(monkeypatch):
    mc, events, updates = _build_mc(paper=False)
    monkeypatch.setattr(mc_mod, "emit_decision_event", lambda **kw: events.append(kw))
    monkeypatch.setenv("HYBRID_CLIENT_QUALITY_MODE", "0")
    monkeypatch.setenv("FINAL_ENTRY_INTELLIGENCE_MIN_SCORE", "35")

    decision = mc._run_final_quality_gates(
        signal=_signal(),
        signal_id="sig-1",
        ticker="AAPL",
        client_id="client@example.com",
        plan=_plan(),
        intel={"_available": True, "score": 20.0, "risk_detail": {"contract_quality_passes": True}},
        snap={},
    )

    assert decision is not None
    assert decision.ok is False
    assert decision.reason_code == "ENTRY_INTELLIGENCE_SCORE_TOO_LOW"
    assert all(evt["decision"] != "APPROVE" for evt in events)
    assert all(update["status"] != "queued" for update in updates)


def test_valid_high_quality_signal_passes_final_gate_without_emitting_approve(monkeypatch):
    mc, events, updates = _build_mc(paper=False)
    monkeypatch.setattr(mc_mod, "emit_decision_event", lambda **kw: events.append(kw))
    monkeypatch.setenv("HYBRID_CLIENT_QUALITY_MODE", "0")
    monkeypatch.setenv("FINAL_ENTRY_INTELLIGENCE_MIN_SCORE", "35")

    decision = mc._run_final_quality_gates(
        signal=_signal(),
        signal_id="sig-1",
        ticker="AAPL",
        client_id="client@example.com",
        plan=_plan(),
        intel={"_available": True, "score": 55.0, "risk_detail": {"contract_quality_passes": True}},
        snap={},
    )

    assert decision is None
    assert events == []
    assert updates == []
