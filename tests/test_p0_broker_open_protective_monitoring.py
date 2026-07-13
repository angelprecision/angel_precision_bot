from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")

import ap.exit_safety as exit_safety
import ap_exit_engine as exit_engine_mod
from ap_exit_engine import APExitEngine, ExitDecision, ManagedPosition, evaluate_exit


def _pos(**overrides) -> ManagedPosition:
    data = dict(
        ticker="SPY",
        option_symbol="SPY260717C00500000",
        side="CALL",
        quantity=1,
        quantity_remaining=1,
        entry_price=1.00,
        underlying_entry=500.0,
        underlying_target=510.0,
        underlying_stop=495.0,
        position_id="pos-protective-1",
        client_id="jason@example.com",
        signal_id="sig-1",
        execution_mode="live",
        current_bid=1.18,
        current_ask=1.22,
        current_option_price=1.20,
        current_underlying=500.0,
        opened_at=datetime.now(timezone.utc) - timedelta(minutes=20),
        last_option_quote_update_ts=datetime.now(timezone.utc),
    )
    data.update(overrides)
    return ManagedPosition(**data)


def _engine(monkeypatch, *, broker_qty=1):
    broker = MagicMock()
    engine = APExitEngine(broker=broker, email="jason@example.com")
    engine.master_control = type("MC", (), {"mode": "live"})()
    engine._emit_exit_event = lambda *args, **kwargs: None
    engine._persist_degraded_monitoring_state = lambda *args, **kwargs: None
    engine._mark_broker_flat_stale_position = APExitEngine._mark_broker_flat_stale_position.__get__(engine, APExitEngine)
    monkeypatch.setattr(
        exit_safety,
        "resolve_exit_broker_truth",
        lambda **kwargs: {
            "is_fresh_exact": True,
            "broker_truth_open_qty": broker_qty,
            "audit": {"source": "test"},
        },
    )
    monkeypatch.setattr(
        exit_safety,
        "evaluate_exit_submission_safety",
        lambda **kwargs: {"blocked": False, "reason": None},
    )
    return engine


def test_stale_quote_creates_owned_retry_and_requests_quote_retry(monkeypatch):
    engine = _engine(monkeypatch, broker_qty=2)
    pos = _pos(last_option_quote_update_ts=datetime.now(timezone.utc) - timedelta(seconds=90))
    decision = ExitDecision("CLOSE_ALL", 1, "IMMEDIATE TP -- +20%", "HIGH", 0.20, reason_code="IMMEDIATE_TP")
    retry_requests = []
    engine.quote_monitor = type(
        "QM",
        (),
        {"request_immediate_refresh": lambda self, *symbols: retry_requests.append(symbols)},
    )()

    broker_flat = engine._own_stale_exit_retry(
        pos,
        decision,
        option_quote_state="stale_option_quote",
        option_quote_age_sec=90.0,
        stage="exit_decision",
    )

    assert broker_flat is False
    assert pos.protective_monitoring_state == "PROTECTIVE_MONITORING_DEGRADED"
    assert pos.behavior_quieted is False
    assert pos.exit_retry_owner == "ap_exit_engine"
    assert pos.exit_retry_decision_code == "IMMEDIATE_TP"
    assert retry_requests == [("SPY260717C00500000", "SPY")]


def test_quieted_logging_does_not_suppress_valid_quote_exit_submission(monkeypatch):
    engine = _engine(monkeypatch, broker_qty=1)
    pos = _pos()
    engine._positions = [pos]
    engine._positions_by_id = {pos.position_id: pos}
    pos.log_quieted = True
    pos.behavior_quieted = False
    decision = ExitDecision("CLOSE_ALL", 1, "IMMEDIATE TP -- +20%", "HIGH", 0.20, suggested_limit=1.18)
    submitted = []
    engine.on_exit = lambda p, d: submitted.append((p.position_id, d.quantity)) or {
        "accepted": True,
        "local_order_id": "L-1",
        "broker_order_id": "B-1",
    }

    monkeypatch.setattr(exit_engine_mod, "_is_option_quote_stale", lambda pos, now_utc: (False, 0.0, "fresh"))

    assert engine._submit_exit_decision(pos, decision) is True
    assert submitted == [("pos-protective-1", 1)]


def test_stale_quote_submission_failure_remains_retry_owned(monkeypatch):
    engine = _engine(monkeypatch, broker_qty=1)
    pos = _pos(last_option_quote_update_ts=datetime.now(timezone.utc) - timedelta(seconds=90))
    decision = ExitDecision("CLOSE_ALL", 1, "IMMEDIATE TP -- +20%", "HIGH", 0.20, suggested_limit=1.18)
    engine.on_exit = lambda p, d: (_ for _ in ()).throw(AssertionError("stale retry must not submit"))
    monkeypatch.setattr(
        exit_engine_mod,
        "_is_option_quote_stale",
        lambda pos, now_utc: (True, 90.0, "stale_option_quote"),
    )

    assert engine._submit_exit_decision(pos, decision) is False
    assert pos.exit_retry_owner == "ap_exit_engine"
    assert pos.protective_monitoring_state == "PROTECTIVE_MONITORING_DEGRADED"


def test_emergency_exit_bypasses_stale_quote_retry(monkeypatch):
    engine = _engine(monkeypatch, broker_qty=1)
    pos = _pos(last_option_quote_update_ts=datetime.now(timezone.utc) - timedelta(seconds=90))
    engine._positions = [pos]
    engine._positions_by_id = {pos.position_id: pos}
    decision = ExitDecision("CLOSE_ALL", 1, "HARD STOP -- down 33%", "IMMEDIATE", -0.33, suggested_limit=1.18)
    submitted = []
    engine.on_exit = lambda p, d: submitted.append(d.reason_code) or {
        "accepted": True,
        "local_order_id": "L-2",
        "broker_order_id": "B-2",
    }
    monkeypatch.setattr(
        exit_engine_mod,
        "_is_option_quote_stale",
        lambda pos, now_utc: (True, 90.0, "stale_option_quote"),
    )

    assert engine._submit_exit_decision(pos, decision) is True
    assert submitted == ["HARD_STOP"]
    assert getattr(pos, "exit_retry_owner", "") == ""


def test_normal_stop_evaluates_before_hard_emergency_threshold_with_valid_quote():
    now = datetime.now(exit_engine_mod.ET)
    pos = _pos(
        entry_price=1.00,
        current_option_price=0.79,
        current_bid=0.79,
        current_ask=0.81,
        opened_at=datetime.now(timezone.utc) - timedelta(minutes=20),
    )
    pos._stop_breach_ts = datetime.now(timezone.utc) - timedelta(seconds=45)

    decision = evaluate_exit(pos, now)
    decision.reason_code = exit_engine_mod._classify_exit_decision(decision)

    assert decision.should_act is True
    assert decision.reason_code != "HARD_STOP"
    assert "DEEP_LOSS_STOP" in decision.reason


def test_broker_flat_position_is_safely_removed_from_monitoring(monkeypatch):
    engine = _engine(monkeypatch, broker_qty=0)
    monkeypatch.setattr(engine, "_persist_degraded_monitoring_state", lambda *args, **kwargs: None)
    monkeypatch.setattr(engine, "_mark_broker_flat_stale_position", APExitEngine._mark_broker_flat_stale_position.__get__(engine, APExitEngine))
    pos = _pos(last_option_quote_update_ts=datetime.now(timezone.utc) - timedelta(seconds=90))
    engine._positions = [pos]
    engine._positions_by_id = {pos.position_id: pos}
    decision = ExitDecision("CLOSE_ALL", 1, "IMMEDIATE TP -- +20%", "HIGH", 0.20, reason_code="IMMEDIATE_TP")

    broker_flat = engine._own_stale_exit_retry(
        pos,
        decision,
        option_quote_state="stale_option_quote",
        option_quote_age_sec=90.0,
        stage="exit_decision",
    )

    assert broker_flat is True
    assert pos.closed is True
    assert pos.quantity_remaining == 0
    assert engine.active_positions() == []


def test_restart_seed_restores_broker_open_quieted_position_monitoring(monkeypatch):
    engine = _engine(monkeypatch, broker_qty=1)
    pm = type(
        "PM",
        (),
        {
            "get_active_positions": lambda self: [
                {
                    "id": "pos-restart-quieted",
                    "client_id": "jason@example.com",
                    "signal_id": "sig-restart",
                    "underlying": "SPY",
                    "contract": "SPY260717C00500000",
                    "direction": "CALL",
                    "qty": 1,
                    "quantity_remaining": 1,
                    "avg_fill": 1.0,
                    "underlying_entry": 500.0,
                    "target_underlying": 510.0,
                    "stop_underlying": 495.0,
                    "execution_mode": "live",
                    "meta": {"protective_monitoring_state": "PROTECTIVE_MONITORING_DEGRADED"},
                }
            ]
        },
    )()
    monkeypatch.setattr(engine, "hydrate_pending_exit_identity_from_db", lambda pos: False)

    engine.seed_from_db(pm)

    active = engine.active_positions()
    assert len(active) == 1
    assert active[0].position_id == "pos-restart-quieted"
    assert active[0].quantity_remaining == 1
