from datetime import datetime, timezone
import sys
import types

import pytest

db_stub = types.ModuleType("ap.db")
db_stub.conn = lambda: None
db_stub.run_with_retry = lambda fn, *args, **kwargs: fn()
sys.modules.setdefault("ap.db", db_stub)

import ap.observability as observability
import ap_exit_engine as exit_engine
from ap_exit_engine import APExitEngine, ManagedPosition, evaluate_exit


def _position(**overrides):
    data = {
        "ticker": "SPY",
        "option_symbol": "SPY260717C00500000",
        "side": "CALL",
        "quantity": 2,
        "entry_price": 1.00,
        "underlying_entry": 500.00,
        "underlying_target": 510.00,
        "underlying_stop": 490.00,
        "position_id": "pos-123",
        "client_id": "paper@example.com",
        "execution_mode": "paper",
        "current_option_price": 1.05,
        "current_bid": 1.00,
        "current_ask": 1.10,
        "current_underlying": 501.00,
        "quantity_remaining": 2,
        "opened_at": datetime(2026, 7, 3, 14, 0, tzinfo=timezone.utc),
    }
    data.update(overrides)
    return ManagedPosition(**data)


def _engine(monkeypatch, captured):
    monkeypatch.setattr(exit_engine, "emit_exit_decision_stamp", lambda payload: captured.append(payload))
    engine = APExitEngine(broker=object(), email="paper@example.com")
    engine.run_id = "test-run"
    engine.strategy_version = "test-strategy"
    engine.git_commit = "test-sha"
    return engine


def test_w1_skipped_emits_exit_decision_stamp(monkeypatch):
    captured = []
    engine = _engine(monkeypatch, captured)
    now_et = datetime(2026, 7, 3, 11, 5, tzinfo=exit_engine.ET)
    pos = _position(current_option_price=1.05, current_bid=0.0, current_ask=0.0)

    decision = evaluate_exit(pos, now_et)
    engine._emit_exit_decision_stamp(pos, decision, now_et=now_et)

    assert decision.action == "HOLD"
    assert captured[-1]["event"] == "exit_decision"
    assert captured[-1]["window"] == "W1"
    assert captured[-1]["action"] == "skipped"
    assert captured[-1]["option_bid"] is None
    assert captured[-1]["option_ask"] is None
    assert captured[-1]["option_mid"] == 1.05
    assert captured[-1]["underlying_price"] == 501.00


def test_tp_fired_emits_exit_decision_stamp(monkeypatch):
    captured = []
    engine = _engine(monkeypatch, captured)
    now_et = datetime(2026, 7, 3, 10, 0, tzinfo=exit_engine.ET)
    pos = _position(current_underlying=510.25)

    decision = evaluate_exit(pos, now_et)
    engine._emit_exit_decision_stamp(pos, decision, now_et=now_et)

    assert decision.action == "CLOSE_ALL"
    assert captured[-1]["window"] == "TP"
    assert captured[-1]["action"] == "fired"
    assert captured[-1]["reason"].startswith("TARGET HIT")


def test_hard_stop_fired_emits_exit_decision_stamp(monkeypatch):
    captured = []
    engine = _engine(monkeypatch, captured)
    now_et = datetime(2026, 7, 3, 10, 0, tzinfo=exit_engine.ET)
    pos = _position(
        current_option_price=0.60,
        current_bid=0.58,
        current_ask=0.62,
        scale_outs_done=1,
        touched_profit=True,
    )

    decision = evaluate_exit(pos, now_et)
    engine._emit_exit_decision_stamp(pos, decision, now_et=now_et)

    assert decision.action == "STOP"
    assert captured[-1]["window"] == "HARD_STOP"
    assert captured[-1]["action"] == "fired"
    assert captured[-1]["option_mid"] == 0.60
    assert captured[-1]["pnl_pct_at_decision"] == pytest.approx(-0.40)


def test_eod_fired_emits_exit_decision_stamp(monkeypatch):
    captured = []
    engine = _engine(monkeypatch, captured)
    now_et = datetime(2026, 7, 3, 15, 50, tzinfo=exit_engine.ET)
    pos = _position(current_option_price=0.98, current_bid=0.94, current_ask=1.02)

    decision = evaluate_exit(pos, now_et)
    engine._emit_exit_decision_stamp(pos, decision, now_et=now_et)

    assert decision.action == "CLOSE_ALL"
    assert captured[-1]["window"] == "EOD"
    assert captured[-1]["action"] == "fired"


def test_observability_emit_exception_does_not_propagate(monkeypatch):
    engine = _engine(monkeypatch, [])
    monkeypatch.setattr(
        exit_engine,
        "emit_exit_decision_stamp",
        lambda payload: (_ for _ in ()).throw(RuntimeError("observer down")),
    )
    pos = _position()
    decision = evaluate_exit(pos, datetime(2026, 7, 3, 11, 5, tzinfo=exit_engine.ET))

    engine._emit_exit_decision_stamp(pos, decision)


def test_observability_wrapper_swallows_decision_event_failure(monkeypatch):
    monkeypatch.setattr(
        observability,
        "emit_decision_event",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("db down")),
    )

    event = observability.emit_exit_decision_stamp({
        "window": "W1",
        "action": "skipped",
        "reason": "No exit condition met",
        "client_id": "paper@example.com",
        "position_id": "pos-123",
    })

    assert event["event"] == "exit_decision"
    assert event["window"] == "W1"


def test_replay_harness_exit_actions_identical_after_stamping(monkeypatch):
    captured = []
    engine = _engine(monkeypatch, captured)
    cases = [
        (_position(), datetime(2026, 7, 3, 11, 5, tzinfo=exit_engine.ET)),
        (_position(current_underlying=510.25), datetime(2026, 7, 3, 10, 0, tzinfo=exit_engine.ET)),
        (_position(current_option_price=0.60, scale_outs_done=1, touched_profit=True), datetime(2026, 7, 3, 10, 0, tzinfo=exit_engine.ET)),
        (_position(current_option_price=0.98), datetime(2026, 7, 3, 15, 50, tzinfo=exit_engine.ET)),
    ]

    for pos, now_et in cases:
        decision = evaluate_exit(pos, now_et)
        before = (decision.action, decision.quantity, decision.reason, decision.urgency, decision.pnl_pct)
        engine._emit_exit_decision_stamp(pos, decision, now_et=now_et)
        after = (decision.action, decision.quantity, decision.reason, decision.urgency, decision.pnl_pct)
        assert after == before

    assert len(captured) == len(cases)
