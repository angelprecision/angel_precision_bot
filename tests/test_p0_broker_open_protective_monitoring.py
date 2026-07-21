from __future__ import annotations

import os
from decimal import Decimal
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")

import ap.db as ap_db
import ap.exit_safety as exit_safety
import ap.position_quote_monitor as qpm_mod
import ap_exit_engine as exit_engine_mod
from ap.position_quote_monitor import APPositionQuoteMonitor
from ap_exit_engine import (
    APExitEngine,
    BrokerPositionTruth,
    BrokerFlatCloseResult,
    DegradedMonitoringPersistResult,
    ExitDecision,
    ManagedPosition,
    PROTECTIVE_STATE_BROKER_FLAT_PENDING,
    PROTECTIVE_STATE_DEGRADED,
    PROTECTIVE_STATE_RETRY_EXHAUSTED,
    PROTECTIVE_STATE_UNPERSISTED,
    STALE_EXIT_RETRY_DELAY_SEC,
    STALE_EXIT_RETRY_MAX_ATTEMPTS,
    _classify_exact_broker_open_qty,
    evaluate_exit,
)


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
        last_underlying_quote_update_ts=datetime.now(timezone.utc),
    )
    data.update(overrides)
    pos = ManagedPosition(**data)
    pos.lastunderlyingquoteupdatets = pos.last_underlying_quote_update_ts
    return pos


def _decision(code="IMMEDIATE_TP", reason="IMMEDIATE TP -- +20%", qty=1) -> ExitDecision:
    return ExitDecision("CLOSE_ALL", qty, reason, "HIGH", 0.20, suggested_limit=1.18, reason_code=code)


def _engine(monkeypatch, *, broker_qty=1):
    broker = MagicMock()
    engine = APExitEngine(broker=broker, email="jason@example.com")
    engine.master_control = type("MC", (), {"mode": "live"})()
    engine._emit_exit_event = lambda *args, **kwargs: None
    monkeypatch.setattr(
        exit_safety,
        "resolve_exit_broker_truth",
        lambda **kwargs: {
            "is_fresh_exact": True,
            "broker_truth_open_qty": broker_qty,
            "audit": {"source": "test", "contract": kwargs.get("contract")},
        },
    )
    monkeypatch.setattr(
        exit_safety,
        "evaluate_exit_submission_safety",
        lambda **kwargs: {"blocked": False, "reason": None},
    )
    return engine


class _DbConn:
    def __init__(self, *, rowcount=1, row=None, raises=None):
        self.rowcount = rowcount
        self.row = row if row is not None else {
            "id": "pos-protective-1",
            "client_id": "jason@example.com",
            "execution_mode": "live",
            "contract": "SPY260717C00500000",
            "status": "CLOSED",
            "quantity_remaining": 0,
        }
        self.raises = raises
        self.queries = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=None):
        if self.raises:
            raise self.raises
        self.queries.append((sql, params))

    def fetchone(self):
        return self.row


def _patch_db(monkeypatch, *, rowcount=1, row=None, raises=None):
    fake = _DbConn(rowcount=rowcount, row=row, raises=raises)
    monkeypatch.setattr(ap_db, "conn", lambda: fake)
    monkeypatch.setattr(ap_db, "run_with_retry", lambda fn, *a, **k: fn())
    return fake


@pytest.mark.parametrize("value", [None, "", "unknown", "N/A", "null", float("nan"), float("inf"), float("-inf"), -1, -0.5, 0.5, True, False, {}, [], object()])
def test_strict_broker_quantity_parser_unknown_values(value):
    assert _classify_exact_broker_open_qty(value) == (BrokerPositionTruth.UNKNOWN, None)


@pytest.mark.parametrize("value", [0, 0.0, "0", "0.0", Decimal("0")])
def test_strict_broker_quantity_parser_flat_values(value):
    assert _classify_exact_broker_open_qty(value) == (BrokerPositionTruth.FLAT, 0)


@pytest.mark.parametrize("value, qty", [(1, 1), (2, 2), ("1", 1), ("2.0", 2), (Decimal("3"), 3)])
def test_strict_broker_quantity_parser_open_values(value, qty):
    assert _classify_exact_broker_open_qty(value) == (BrokerPositionTruth.OPEN, qty)


def _assert_unknown_qty_does_not_flat_close(monkeypatch, broker_qty):
    engine = _engine(monkeypatch, broker_qty=broker_qty)
    pos = _pos()
    engine._positions = [pos]
    engine._positions_by_id = {pos.position_id: pos}
    _patch_db(monkeypatch, rowcount=1)
    close_calls = []
    engine._mark_broker_flat_stale_position = lambda *a, **k: close_calls.append((a, k)) or BrokerFlatCloseResult(True, 1, True, "closed", None)
    alerts = []
    engine._emit_degraded_critical = lambda pos, code, message, extra=None: alerts.append((code, extra))

    result = engine._own_stale_exit_retry(
        pos,
        _decision(),
        option_quote_state="stale",
        option_quote_age_sec=90,
        stage="exit_decision",
    )

    assert result is False
    assert close_calls == []
    assert pos.closed is False
    assert pos.quantity_remaining == 1
    assert engine.active_positions() == [pos]
    assert pos.broker_truth_state == BrokerPositionTruth.UNKNOWN.value
    assert any(code == "BROKER_TRUTH_QUANTITY_UNKNOWN" for code, _extra in alerts)


def test_none_broker_quantity_is_not_flat(monkeypatch):
    _assert_unknown_qty_does_not_flat_close(monkeypatch, None)


@pytest.mark.parametrize("broker_qty", ["unknown", "N/A", "", "null", "abc"])
def test_malformed_string_broker_quantity_is_not_flat(monkeypatch, broker_qty):
    _assert_unknown_qty_does_not_flat_close(monkeypatch, broker_qty)


@pytest.mark.parametrize("broker_qty", [float("nan"), float("inf"), float("-inf")])
def test_nan_and_infinity_broker_quantity_are_not_flat(monkeypatch, broker_qty):
    _assert_unknown_qty_does_not_flat_close(monkeypatch, broker_qty)


@pytest.mark.parametrize("broker_qty", [-1, -0.5, 0.5, "1.5"])
def test_negative_and_fractional_broker_quantity_are_unknown(monkeypatch, broker_qty):
    _assert_unknown_qty_does_not_flat_close(monkeypatch, broker_qty)


def test_missing_execution_mode_blocks_degraded_persistence(monkeypatch):
    engine = _engine(monkeypatch, broker_qty=1)
    pos = _pos(execution_mode="")
    fake = _patch_db(monkeypatch)
    alerts = []
    engine._emit_degraded_critical = lambda pos, code, message, extra=None: alerts.append(code)

    result = engine._persist_degraded_monitoring_state(
        pos,
        state=PROTECTIVE_STATE_DEGRADED,
        intent={},
        persist_reason="test",
    )

    assert result.persisted is False
    assert result.reason == "identity_unproven"
    assert fake.queries == []
    assert pos.protective_monitoring_state == PROTECTIVE_STATE_UNPERSISTED
    assert pos in [pos]
    assert alerts == ["PROTECTIVE_MONITORING_IDENTITY_UNPROVEN"]


def test_missing_execution_mode_blocks_broker_flat_close(monkeypatch):
    engine = _engine(monkeypatch, broker_qty=0)
    pos = _pos(execution_mode="")
    engine._positions = [pos]
    engine._positions_by_id = {pos.position_id: pos}
    fake = _patch_db(monkeypatch)
    alerts = []
    engine._emit_degraded_critical = lambda pos, code, message, extra=None: alerts.append(code)

    result = engine._mark_broker_flat_stale_position(pos, {"is_fresh_exact": True, "broker_truth_open_qty": 0})

    assert result.closed is False
    assert result.verified is False
    assert fake.queries == []
    assert pos.closed is False
    assert pos.quantity_remaining == 1
    assert pos.protective_monitoring_state == PROTECTIVE_STATE_BROKER_FLAT_PENDING
    assert engine.active_positions() == [pos]
    assert alerts == ["BROKER_FLAT_CLOSE_IDENTITY_UNPROVEN"]


def test_missing_contract_blocks_all_protective_mutation(monkeypatch):
    engine = _engine(monkeypatch, broker_qty=0)
    pos = _pos(option_symbol="")
    engine._positions = [pos]
    engine._positions_by_id = {pos.position_id: pos}
    fake = _patch_db(monkeypatch)
    alerts = []
    engine._emit_degraded_critical = lambda pos, code, message, extra=None: alerts.append(code)

    persist = engine._persist_degraded_monitoring_state(
        pos,
        state=PROTECTIVE_STATE_DEGRADED,
        intent={},
        persist_reason="test",
    )
    close = engine._mark_broker_flat_stale_position(pos, {"is_fresh_exact": True, "broker_truth_open_qty": 0})

    assert persist.persisted is False
    assert close.closed is False
    assert close.verified is False
    assert fake.queries == []
    assert pos.closed is False
    assert pos.quantity_remaining == 1
    assert pos.protective_monitoring_state == PROTECTIVE_STATE_BROKER_FLAT_PENDING
    assert alerts == ["PROTECTIVE_MONITORING_IDENTITY_UNPROVEN", "BROKER_FLAT_CLOSE_IDENTITY_UNPROVEN"]


def test_cross_mode_isolation_uses_strict_execution_mode_equality(monkeypatch):
    engine = _engine(monkeypatch, broker_qty=1)
    live_pos = _pos(execution_mode="live")
    paper_pos = _pos(execution_mode="paper")
    live_fake = _patch_db(monkeypatch, rowcount=1)
    engine._persist_degraded_monitoring_state(live_pos, state=PROTECTIVE_STATE_DEGRADED, intent={}, persist_reason="live")
    live_sql, live_params = live_fake.queries[0]

    paper_fake = _patch_db(monkeypatch, rowcount=1)
    engine._persist_degraded_monitoring_state(paper_pos, state=PROTECTIVE_STATE_DEGRADED, intent={}, persist_reason="paper")
    paper_sql, paper_params = paper_fake.queries[0]

    assert "LOWER(COALESCE(execution_mode, '')) = %s" in live_sql
    assert "(%s = '' OR" not in live_sql
    assert live_params[-2:] == ("live", "SPY260717C00500000")
    assert paper_params[-2:] == ("paper", "SPY260717C00500000")
    assert "LOWER(COALESCE(execution_mode, '')) = %s" in paper_sql


def test_exact_numeric_zero_closes_only_after_durable_identity_verification(monkeypatch):
    engine = _engine(monkeypatch, broker_qty=0)
    pos = _pos()
    engine._positions = [pos]
    engine._positions_by_id = {pos.position_id: pos}
    events = []

    class OrderedCloseConn(_DbConn):
        def fetchone(self):
            events.append(("reread", pos.closed, pos.quantity_remaining, pos.position_id in engine._positions_by_id))
            return self.row

    fake = OrderedCloseConn(rowcount=1)
    monkeypatch.setattr(ap_db, "conn", lambda: fake)
    monkeypatch.setattr(ap_db, "run_with_retry", lambda fn, *a, **k: fn())

    result = engine._mark_broker_flat_stale_position(pos, {"is_fresh_exact": True, "broker_truth_open_qty": 0})

    assert result == BrokerFlatCloseResult(True, 1, True, "closed", None)
    assert events == [("reread", False, 1, True)]
    assert pos.closed is True
    assert pos.quantity_remaining == 0
    assert engine.active_positions() == []


def test_broker_flat_reread_wrong_mode_fails_verification(monkeypatch):
    engine = _engine(monkeypatch, broker_qty=0)
    pos = _pos()
    engine._positions = [pos]
    engine._positions_by_id = {pos.position_id: pos}
    _patch_db(monkeypatch, rowcount=1, row={
        "id": pos.position_id,
        "client_id": pos.client_id,
        "execution_mode": "paper",
        "contract": pos.option_symbol,
        "status": "CLOSED",
        "quantity_remaining": 0,
    })

    result = engine._mark_broker_flat_stale_position(pos, {"is_fresh_exact": True, "broker_truth_open_qty": 0})

    assert result.closed is False
    assert result.verified is False
    assert pos.closed is False
    assert pos.quantity_remaining == 1
    assert engine.active_positions() == [pos]


def test_broker_flat_reread_wrong_contract_fails_verification(monkeypatch):
    engine = _engine(monkeypatch, broker_qty=0)
    pos = _pos()
    engine._positions = [pos]
    engine._positions_by_id = {pos.position_id: pos}
    _patch_db(monkeypatch, rowcount=1, row={
        "id": pos.position_id,
        "client_id": pos.client_id,
        "execution_mode": pos.execution_mode,
        "contract": "SPY260717P00500000",
        "status": "CLOSED",
        "quantity_remaining": 0,
    })

    result = engine._mark_broker_flat_stale_position(pos, {"is_fresh_exact": True, "broker_truth_open_qty": 0})

    assert result.closed is False
    assert result.verified is False
    assert pos.closed is False
    assert pos.quantity_remaining == 1
    assert engine.active_positions() == [pos]


def test_broker_flat_reread_malformed_remaining_quantity_fails_verification(monkeypatch):
    engine = _engine(monkeypatch, broker_qty=0)
    pos = _pos()
    engine._positions = [pos]
    engine._positions_by_id = {pos.position_id: pos}
    _patch_db(monkeypatch, rowcount=1, row={
        "id": pos.position_id,
        "client_id": pos.client_id,
        "execution_mode": pos.execution_mode,
        "contract": pos.option_symbol,
        "status": "CLOSED",
        "quantity_remaining": "unknown",
    })

    result = engine._mark_broker_flat_stale_position(pos, {"is_fresh_exact": True, "broker_truth_open_qty": 0})

    assert result.closed is False
    assert result.verified is False
    assert pos.closed is False
    assert pos.quantity_remaining == 1
    assert engine.active_positions() == [pos]


def test_recovered_state_clear_is_mode_and_contract_scoped(monkeypatch):
    engine = _engine(monkeypatch, broker_qty=1)
    pos = _pos()
    pos.protective_monitoring_state = PROTECTIVE_STATE_DEGRADED
    fake = _patch_db(monkeypatch, rowcount=1)

    engine._clear_degraded_monitoring_state(pos)

    sql, params = fake.queries[0]
    assert "id = %s" in sql
    assert "client_id = %s" in sql
    assert "LOWER(COALESCE(execution_mode, '')) = %s" in sql
    assert "COALESCE(contract, '') = %s" in sql
    assert params[-4:] == (pos.position_id, pos.client_id, pos.execution_mode, pos.option_symbol)


def test_retry_exhaustion_with_unknown_broker_truth_remains_monitored(monkeypatch):
    engine = _engine(monkeypatch, broker_qty=None)
    pos = _pos()
    pos.exit_retry_attempt = STALE_EXIT_RETRY_MAX_ATTEMPTS
    pos.exit_retry_first_requested_at = datetime.now(timezone.utc) - timedelta(seconds=60)
    pos.exit_retry_deadline = datetime.now(timezone.utc) + timedelta(seconds=60)
    pos.exit_retry_next_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    engine._positions = [pos]
    engine._positions_by_id = {pos.position_id: pos}
    _patch_db(monkeypatch, rowcount=1)
    close_calls = []
    engine._mark_broker_flat_stale_position = lambda *a, **k: close_calls.append((a, k)) or BrokerFlatCloseResult(True, 1, True, "closed", None)
    alerts = []
    engine._emit_degraded_critical = lambda pos, code, message, extra=None: alerts.append((code, extra))

    result = engine._own_stale_exit_retry(pos, _decision(), option_quote_state="blind", option_quote_age_sec=None, stage="exit_decision")

    assert result is False
    assert pos.exit_retry_attempt == STALE_EXIT_RETRY_MAX_ATTEMPTS
    assert pos.protective_monitoring_state == PROTECTIVE_STATE_RETRY_EXHAUSTED
    assert pos.behavior_quieted is False
    assert engine.active_positions() == [pos]
    assert close_calls == []
    assert any(code == "PROTECTIVE_RETRY_EXHAUSTED_BROKER_QTY_UNKNOWN" for code, _extra in alerts)


def test_retry_throttling_no_increment_or_refresh_before_next_at(monkeypatch):
    engine = _engine(monkeypatch, broker_qty=2)
    pos = _pos(last_option_quote_update_ts=datetime.now(timezone.utc) - timedelta(seconds=90))
    refreshes = []
    engine.quote_monitor = type(
        "QM",
        (),
        {"request_immediate_refresh": lambda self, *symbols: refreshes.append(symbols) or True},
    )()
    _patch_db(monkeypatch)

    assert engine._own_stale_exit_retry(pos, _decision(), option_quote_state="stale", option_quote_age_sec=90, stage="exit_decision") is False
    assert pos.exit_retry_attempt == 1
    assert refreshes == [("SPY260717C00500000", "SPY")]

    before_broker_probe = exit_safety.resolve_exit_broker_truth
    calls = {"broker": 0}
    monkeypatch.setattr(
        exit_safety,
        "resolve_exit_broker_truth",
        lambda **kwargs: calls.__setitem__("broker", calls["broker"] + 1) or before_broker_probe(**kwargs),
    )
    assert engine._own_stale_exit_retry(pos, _decision(), option_quote_state="stale", option_quote_age_sec=91, stage="exit_decision") is False
    assert pos.exit_retry_attempt == 1
    assert len(refreshes) == 1
    assert calls["broker"] == 0

    pos.exit_retry_next_at = datetime.now(timezone.utc) - timedelta(milliseconds=1)
    assert engine._own_stale_exit_retry(pos, _decision(), option_quote_state="stale", option_quote_age_sec=92, stage="exit_decision") is False
    assert pos.exit_retry_attempt == 2
    assert len(refreshes) == 2


def test_retry_exhaustion_does_not_hot_loop_or_wake(monkeypatch):
    engine = _engine(monkeypatch, broker_qty=1)
    pos = _pos()
    pos.exit_retry_attempt = STALE_EXIT_RETRY_MAX_ATTEMPTS
    pos.exit_retry_first_requested_at = datetime.now(timezone.utc) - timedelta(seconds=60)
    pos.exit_retry_deadline = datetime.now(timezone.utc) + timedelta(seconds=60)
    pos.exit_retry_next_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    refreshes = []
    engine.quote_monitor = type(
        "QM",
        (),
        {"request_immediate_refresh": lambda self, *symbols: refreshes.append(symbols) or True},
    )()
    _patch_db(monkeypatch)

    assert engine._own_stale_exit_retry(pos, _decision(), option_quote_state="blind", option_quote_age_sec=None, stage="exit_decision") is False
    assert pos.protective_monitoring_state == PROTECTIVE_STATE_RETRY_EXHAUSTED
    assert pos.exit_retry_attempt == STALE_EXIT_RETRY_MAX_ATTEMPTS
    assert refreshes == []


def test_retry_deadline_exhaustion_without_attempt_increment(monkeypatch):
    engine = _engine(monkeypatch, broker_qty=1)
    pos = _pos()
    pos.exit_retry_attempt = 2
    pos.exit_retry_first_requested_at = datetime.now(timezone.utc) - timedelta(seconds=120)
    pos.exit_retry_deadline = datetime.now(timezone.utc) - timedelta(seconds=1)
    pos.exit_retry_next_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    _patch_db(monkeypatch)

    engine._own_stale_exit_retry(pos, _decision(), option_quote_state="missing", option_quote_age_sec=None, stage="exit_decision")

    assert pos.protective_monitoring_state == PROTECTIVE_STATE_RETRY_EXHAUSTED
    assert pos.exit_retry_attempt == 2


def test_degraded_ownership_persist_result_rowcount_one(monkeypatch):
    engine = _engine(monkeypatch, broker_qty=1)
    pos = _pos()
    fake = _patch_db(monkeypatch, rowcount=1)
    intent = engine._build_retry_intent(
        pos, _decision(), status=PROTECTIVE_STATE_DEGRADED, attempt=1,
        now=datetime.now(timezone.utc),
        first_requested=datetime.now(timezone.utc),
        next_at=datetime.now(timezone.utc) + timedelta(seconds=1),
        deadline=datetime.now(timezone.utc) + timedelta(seconds=30),
        option_quote_state="stale", option_quote_age_sec=90,
    )

    result = engine._persist_degraded_monitoring_state(
        pos, state=PROTECTIVE_STATE_DEGRADED, intent=intent, persist_reason="test"
    )

    assert result == DegradedMonitoringPersistResult(True, 1, "persisted", None)
    sql, params = fake.queries[0]
    assert "client_id = %s" in sql
    assert "COALESCE(contract, '') = %s" in sql
    assert params[-1] == "SPY260717C00500000"


def test_degraded_ownership_persist_zero_rowcount_is_critical_state(monkeypatch):
    engine = _engine(monkeypatch, broker_qty=1)
    pos = _pos()
    _patch_db(monkeypatch, rowcount=0)
    alerts = []
    engine._emit_degraded_critical = lambda pos, code, message, extra=None: alerts.append((code, message, extra))

    engine._own_stale_exit_retry(pos, _decision(), option_quote_state="stale", option_quote_age_sec=90, stage="exit_decision")

    assert pos.protective_monitoring_state == PROTECTIVE_STATE_UNPERSISTED
    assert pos.exit_retry_persisted is False
    assert pos.exit_retry_persist_error
    assert alerts and alerts[0][0] == "PROTECTIVE_MONITORING_UNPERSISTED"


def test_degraded_ownership_persist_exception_is_critical_state(monkeypatch):
    engine = _engine(monkeypatch, broker_qty=1)
    pos = _pos()
    _patch_db(monkeypatch, raises=RuntimeError("db down"))
    alerts = []
    engine._emit_degraded_critical = lambda pos, code, message, extra=None: alerts.append((code, message, extra))

    engine._own_stale_exit_retry(pos, _decision(), option_quote_state="stale", option_quote_age_sec=90, stage="exit_decision")

    assert pos.protective_monitoring_state == PROTECTIVE_STATE_UNPERSISTED
    assert "db down" in pos.exit_retry_persist_error
    assert alerts and alerts[0][0] == "PROTECTIVE_MONITORING_UNPERSISTED"


def test_broker_flat_close_removes_memory_only_after_verified_durable_close(monkeypatch):
    engine = _engine(monkeypatch, broker_qty=0)
    pos = _pos()
    engine._positions = [pos]
    engine._positions_by_id = {pos.position_id: pos}
    _patch_db(monkeypatch, rowcount=1, row={
        "id": pos.position_id,
        "client_id": pos.client_id,
        "execution_mode": pos.execution_mode,
        "contract": pos.option_symbol,
        "status": "CLOSED",
        "quantity_remaining": 0,
    })

    result = engine._mark_broker_flat_stale_position(pos, {"is_fresh_exact": True, "broker_truth_open_qty": 0})

    assert result == BrokerFlatCloseResult(True, 1, True, "closed", None)
    assert pos.closed is True
    assert pos.quantity_remaining == 0
    assert engine.active_positions() == []


def test_broker_flat_zero_rowcount_keeps_position_monitored(monkeypatch):
    engine = _engine(monkeypatch, broker_qty=0)
    pos = _pos()
    engine._positions = [pos]
    engine._positions_by_id = {pos.position_id: pos}
    _patch_db(monkeypatch, rowcount=0)

    result = engine._mark_broker_flat_stale_position(pos, {"is_fresh_exact": True, "broker_truth_open_qty": 0})

    assert result.closed is False
    assert pos.closed is False
    assert pos.quantity_remaining == 1
    assert pos.protective_monitoring_state == PROTECTIVE_STATE_BROKER_FLAT_PENDING
    assert engine.active_positions() == [pos]


def test_broker_flat_db_exception_keeps_position_monitored(monkeypatch):
    engine = _engine(monkeypatch, broker_qty=0)
    pos = _pos()
    engine._positions = [pos]
    engine._positions_by_id = {pos.position_id: pos}
    _patch_db(monkeypatch, raises=RuntimeError("db fail"))

    result = engine._mark_broker_flat_stale_position(pos, {"is_fresh_exact": True, "broker_truth_open_qty": 0})

    assert result.closed is False
    assert pos.closed is False
    assert pos.quantity_remaining == 1
    assert pos.protective_monitoring_state == PROTECTIVE_STATE_BROKER_FLAT_PENDING


def test_restart_seed_restores_degraded_retry_owner_and_deadline(monkeypatch):
    engine = _engine(monkeypatch, broker_qty=1)
    first = datetime.now(timezone.utc) - timedelta(seconds=10)
    next_at = datetime.now(timezone.utc) + timedelta(seconds=20)
    deadline = datetime.now(timezone.utc) + timedelta(seconds=40)
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
                    "meta": {
                        "protective_monitoring_state": PROTECTIVE_STATE_DEGRADED,
                        "exit_retry_owner": "ap_exit_engine",
                        "exit_retry_status": PROTECTIVE_STATE_DEGRADED,
                        "exit_retry_action": "CLOSE_ALL",
                        "exit_retry_quantity": 1,
                        "exit_retry_decision_code": "IMMEDIATE_TP",
                        "exit_retry_attempt": 2,
                        "exit_retry_first_requested_at": first.isoformat(),
                        "exit_retry_next_at": next_at.isoformat(),
                        "exit_retry_deadline": deadline.isoformat(),
                    },
                }
            ]
        },
    )()
    monkeypatch.setattr(engine, "hydrate_pending_exit_identity_from_db", lambda pos: False)

    engine.seed_from_db(pm)

    active = engine.active_positions()
    assert len(active) == 1
    assert active[0].position_id == "pos-restart-quieted"
    assert active[0].exit_retry_owner == "ap_exit_engine"
    assert active[0].exit_retry_attempt == 2
    assert active[0].exit_retry_next_at == next_at
    assert active[0].exit_retry_deadline == deadline


def test_restart_restored_retry_not_due_does_not_duplicate_refresh(monkeypatch):
    engine = _engine(monkeypatch, broker_qty=1)
    pos = _pos()
    pos.protective_monitoring_state = PROTECTIVE_STATE_DEGRADED
    pos.exit_retry_status = PROTECTIVE_STATE_DEGRADED
    pos.exit_retry_attempt = 2
    pos.exit_retry_next_at = datetime.now(timezone.utc) + timedelta(seconds=30)
    refreshes = []
    engine.quote_monitor = type(
        "QM",
        (),
        {"request_immediate_refresh": lambda self, *symbols: refreshes.append(symbols) or True},
    )()

    engine._own_stale_exit_retry(pos, _decision(), option_quote_state="stale", option_quote_age_sec=90, stage="exit_decision")

    assert pos.exit_retry_attempt == 2
    assert refreshes == []


def test_forced_risk_submit_path_bypasses_stale_quote(monkeypatch):
    engine = _engine(monkeypatch, broker_qty=1)
    pos = _pos(last_option_quote_update_ts=datetime.now(timezone.utc) - timedelta(seconds=90))
    engine._positions = [pos]
    engine._positions_by_id = {pos.position_id: pos}
    submitted = []
    engine.on_exit = lambda p, d: submitted.append(d.reason_code) or {
        "accepted": True,
        "local_order_id": "L-2",
        "broker_order_id": "B-2",
    }
    monkeypatch.setattr(exit_engine_mod, "_is_option_quote_stale", lambda pos, now_utc: (True, 90.0, "stale_option_quote"))

    assert engine._submit_exit_decision(pos, _decision("STOP_HIT", "STOP HIT -- thesis failed")) is True
    assert submitted == ["STOP_HIT"]
    assert getattr(pos, "exit_retry_owner", "") == ""


def test_eod_forced_risk_submit_path_bypasses_stale_quote(monkeypatch):
    engine = _engine(monkeypatch, broker_qty=1)
    pos = _pos(last_option_quote_update_ts=datetime.now(timezone.utc) - timedelta(seconds=90))
    engine._positions = [pos]
    engine._positions_by_id = {pos.position_id: pos}
    submitted = []
    engine.on_exit = lambda p, d: submitted.append(d.reason_code) or {
        "accepted": True,
        "local_order_id": "L-3",
        "broker_order_id": "B-3",
    }
    monkeypatch.setattr(exit_engine_mod, "_is_option_quote_stale", lambda pos, now_utc: (True, 90.0, "stale_option_quote"))

    assert engine._submit_exit_decision(pos, _decision("EOD_FORCE_CLOSE", "EOD FORCE CLOSE")) is True
    assert submitted == ["EOD_FORCE_CLOSE"]


def test_take_profit_stale_quote_uses_bounded_retry_not_forced_path(monkeypatch):
    engine = _engine(monkeypatch, broker_qty=1)
    pos = _pos(last_option_quote_update_ts=datetime.now(timezone.utc) - timedelta(seconds=90))
    engine._positions = [pos]
    engine._positions_by_id = {pos.position_id: pos}
    engine.on_exit = lambda p, d: (_ for _ in ()).throw(AssertionError("TP must not submit on stale quote"))
    _patch_db(monkeypatch)
    monkeypatch.setattr(exit_engine_mod, "_is_option_quote_stale", lambda pos, now_utc: (True, 90.0, "stale_option_quote"))

    assert engine._submit_exit_decision(pos, _decision("IMMEDIATE_TP", "IMMEDIATE TP -- +20%")) is False
    assert pos.protective_monitoring_state == PROTECTIVE_STATE_DEGRADED
    assert pos.exit_retry_attempt == 1


def test_fresh_recovery_clears_degraded_state_and_reevaluates_current_decision(monkeypatch):
    engine = _engine(monkeypatch, broker_qty=1)
    pos = _pos()
    pos.protective_monitoring_state = PROTECTIVE_STATE_DEGRADED
    pos.exit_retry_owner = "ap_exit_engine"
    pos.exit_retry_attempt = 1
    _patch_db(monkeypatch)

    engine._clear_degraded_monitoring_state(pos)
    current = evaluate_exit(pos, datetime.now(exit_engine_mod.ET))

    assert pos.protective_monitoring_state == "RESOLVED"
    assert pos.exit_retry_owner == ""
    assert current.action in {"HOLD", "CLOSE_ALL", "SCALE_OUT"}


def test_quote_monitor_coalesces_duplicate_immediate_refresh(monkeypatch):
    qpm_mod._SHARED_CACHE.clear()
    qpm_mod._SHARED_BACKOFF_UNTIL = 0.0
    qpm_mod._SHARED_CACHE["SPY260717C00500000"] = {"quote": {"symbol": "SPY260717C00500000"}, "ts": 1.0}
    monitor = APPositionQuoteMonitor(MagicMock(), "jason@example.com", MagicMock())

    assert monitor.request_immediate_refresh("SPY260717C00500000") is True
    assert monitor.request_immediate_refresh("SPY260717C00500000") is False
    metrics = monitor.metrics_snapshot()
    assert metrics["immediate_retry_requests"] == 2
    assert metrics["immediate_retry_evictions"] == 1
    assert metrics["immediate_retry_coalesced"] == 1


def test_quote_monitor_backoff_suppresses_immediate_refresh(monkeypatch):
    qpm_mod._SHARED_BACKOFF_UNTIL = qpm_mod.time.time() + 30
    monitor = APPositionQuoteMonitor(MagicMock(), "jason@example.com", MagicMock())

    assert monitor.request_immediate_refresh("SPY260717C00500000") is False
    assert monitor.metrics_snapshot()["immediate_retry_backoff_suppressed"] == 1
    qpm_mod._SHARED_BACKOFF_UNTIL = 0.0


def test_retry_config_is_clamped_at_import_time():
    assert 1 <= STALE_EXIT_RETRY_MAX_ATTEMPTS <= 20
    assert 0.5 <= STALE_EXIT_RETRY_DELAY_SEC <= 60


def test_normal_stop_evaluates_before_hard_emergency_threshold_with_valid_quote():
    pos = _pos(
        entry_price=1.00,
        current_option_price=0.79,
        current_bid=0.79,
        current_ask=0.81,
        opened_at=datetime.now(timezone.utc) - timedelta(minutes=20),
    )
    pos._stop_breach_ts = datetime.now(timezone.utc) - timedelta(seconds=45)

    decision = evaluate_exit(pos, datetime.now(exit_engine_mod.ET))
    decision.reason_code = exit_engine_mod._classify_exit_decision(decision)

    assert decision.should_act is True
    assert decision.reason_code != "HARD_STOP"
    assert "DEEP_LOSS_STOP" in decision.reason
