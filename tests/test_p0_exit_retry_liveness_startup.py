"""PR #423 startup terminal-EXIT bridge regressions."""

from __future__ import annotations

from contextlib import contextmanager
import os
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")

from ap_recovery import APStartupRecovery


CLIENT_ID = "client-startup-pr423"
POSITION_ID = "pos-startup-pr423"
CONTRACT = "AVGO260814C00350000"
LOCAL_ID = "loc-startup-pr423"
BROKER_ID = "bro-startup-pr423"


class _Broker:
    def __init__(self, payload):
        self.payload = dict(payload)
        self.get_order_calls = []
        self.cancel_calls = 0

    def get_order(self, broker_order_id):
        self.get_order_calls.append(broker_order_id)
        return dict(self.payload)

    def cancel_order(self, broker_order_id):  # pragma: no cover - safety fence
        self.cancel_calls += 1
        raise AssertionError("startup terminal recovery must not mutate the broker")


class _OSM:
    def __init__(self, *, filled_qty):
        self.row = {
            "local_order_id": LOCAL_ID,
            "broker_order_id": BROKER_ID,
            "position_id": POSITION_ID,
            "client_id": CLIENT_ID,
            "execution_mode": "paper",
            "kind": "EXIT",
            "contract": CONTRACT,
            "status": "EXIT_PARTIAL_FILL" if filled_qty else "EXIT_ACKNOWLEDGED",
            "qty": 3,
            "filled_qty": filled_qty,
        }
        self.transitions = []

    def get_order(self, local_order_id):
        if local_order_id != LOCAL_ID:
            return None
        return dict(self.row)

    def transition(self, local_order_id, status, **kwargs):
        self.transitions.append((local_order_id, status, dict(kwargs)))
        if local_order_id != LOCAL_ID:
            return False
        self.row["status"] = status
        if kwargs.get("filled_qty") is not None:
            self.row["filled_qty"] = kwargs["filled_qty"]
        if kwargs.get("broker_order_id"):
            self.row["broker_order_id"] = kwargs["broker_order_id"]
        return True


class _Engine:
    def __init__(self, *, quantity_remaining, bridge_ok=True):
        self.position = SimpleNamespace(
            position_id=POSITION_ID,
            client_id=CLIENT_ID,
            execution_mode="paper",
            option_symbol=CONTRACT,
            quantity_remaining=quantity_remaining,
            closed=False,
        )
        self.bridge_ok = bridge_ok
        self.seed_calls = []
        self.partial_fill_calls = []
        self.bridge_calls = []

    def seed_from_db(self, position_manager):
        self.seed_calls.append(position_manager)

    def get_position(self, position_id):
        return self.position if position_id == POSITION_ID else None

    def note_partial_exit_fill(self, position_id, qty_filled=0, **kwargs):
        self.partial_fill_calls.append((position_id, qty_filled, dict(kwargs)))

    def reconcile_exit_fill_consumption(self, position_id, **kwargs):
        self.bridge_calls.append((position_id, dict(kwargs)))
        if not self.bridge_ok:
            return {"ok": False, "reason": "position_applied_cumulative_unavailable"}
        cumulative = kwargs["cumulative_filled_qty"]
        self.position.quantity_remaining = 5 - (cumulative - 1)
        return {
            "ok": True,
            "applied_delta": cumulative - kwargs["prior_cumulative_filled"],
            "applied_cumulative_qty": cumulative,
            "quantity_remaining": self.position.quantity_remaining,
        }


class _Cursor:
    def __init__(self):
        self.rowcount = 0
        self.statements = []

    def execute(self, statement, params=()):
        self.statements.append((str(statement), tuple(params)))
        if "UPDATE positions SET status='OPEN'" in str(statement):
            self.rowcount = 1
        return self


def _recovery(monkeypatch, *, broker_payload, osm, engine, position):
    import ap.db as db_module

    cursor = _Cursor()

    @contextmanager
    def _conn():
        yield cursor

    monkeypatch.setattr(db_module, "conn", _conn)
    monkeypatch.setattr(db_module, "run_with_retry", lambda fn, *a, **k: fn(*a, **k))
    broker = _Broker(broker_payload)
    recovery = APStartupRecovery(
        client_id=CLIENT_ID,
        broker=broker,
        osm=osm,
        pm=object(),
        master_control=SimpleNamespace(mode="paper"),
        exit_engine=engine,
    )
    recovery._load_closing_positions = lambda: [position]
    recovery._resolve_active_exit_order_for_position = (
        lambda _position, _result: dict(osm.row)
    )
    return recovery, broker, cursor


def _position():
    return {
        "id": POSITION_ID,
        "client_id": CLIENT_ID,
        "execution_mode": "paper",
        "contract": CONTRACT,
        "status": "CLOSING",
    }


def test_startup_terminal_exit_reconciles_late_fill_before_reopen(monkeypatch):
    osm = _OSM(filled_qty=1)
    engine = _Engine(quantity_remaining=5)
    recovery, broker, cursor = _recovery(
        monkeypatch,
        broker_payload={
            "id": BROKER_ID,
            "contract": CONTRACT,
            "status": "canceled",
            "quantity": 3,
            "exec_quantity": 2,
        },
        osm=osm,
        engine=engine,
        position=_position(),
    )
    result = {"errors": [], "exits_reattached": 0}

    live_orders = recovery._recover_exit_fills_that_occurred_during_downtime(result)

    assert live_orders == []
    assert result["errors"] == []
    assert result["exits_reattached"] == 1
    assert result["exit_fill_reconciliations_attempted"] == 1
    assert result["exit_fill_reconciliations_reconciled"] == 1
    assert broker.get_order_calls == [BROKER_ID]
    assert broker.cancel_calls == 0
    assert engine.seed_calls == [recovery.pm]
    assert engine.bridge_calls[0][1]["cumulative_filled_qty"] == 2
    assert engine.bridge_calls[0][1]["prior_cumulative_filled"] == 1
    assert [status for _, status, _ in osm.transitions] == [
        "EXIT_PARTIAL_FILL",
        "CANCELED",
    ]
    assert osm.transitions[-1][2]["filled_qty"] == 2
    assert any("UPDATE positions SET status='OPEN'" in sql for sql, _ in cursor.statements)


def test_startup_terminal_exit_zero_fill_is_proven_and_reopened(monkeypatch):
    osm = _OSM(filled_qty=0)
    engine = _Engine(quantity_remaining=5)
    recovery, broker, cursor = _recovery(
        monkeypatch,
        broker_payload={
            "id": BROKER_ID,
            "contract": CONTRACT,
            "status": "expired",
            "quantity": 3,
            "exec_quantity": 0,
        },
        osm=osm,
        engine=engine,
        position=_position(),
    )
    result = {"errors": [], "exits_reattached": 0}

    recovery._recover_exit_fills_that_occurred_during_downtime(result)

    assert result["errors"] == []
    assert result["exits_reattached"] == 1
    assert [status for _, status, _ in osm.transitions] == ["EXPIRED"]
    assert engine.partial_fill_calls == []
    assert engine.bridge_calls[0][1]["cumulative_filled_qty"] == 0
    assert any("UPDATE positions SET status='OPEN'" in sql for sql, _ in cursor.statements)
    assert broker.cancel_calls == 0


def test_startup_terminal_exit_missing_broker_cumulative_holds_without_clear_or_reopen(
    monkeypatch,
):
    osm = _OSM(filled_qty=1)
    engine = _Engine(quantity_remaining=5)
    recovery, broker, cursor = _recovery(
        monkeypatch,
        broker_payload={
            "id": BROKER_ID,
            "contract": CONTRACT,
            "status": "rejected",
            "quantity": 3,
        },
        osm=osm,
        engine=engine,
        position=_position(),
    )
    result = {"errors": [], "exits_reattached": 0}

    recovery._recover_exit_fills_that_occurred_during_downtime(result)

    assert result["exits_reattached"] == 0
    assert result.get("exit_fill_reconciliations_reconciled", 0) == 0
    assert any("startup_terminal_exit_broker_cumulative_unavailable" in error for error in result["errors"])
    assert osm.transitions == []
    assert engine.bridge_calls == []
    assert not any("UPDATE positions SET status='OPEN'" in sql for sql, _ in cursor.statements)
    assert broker.cancel_calls == 0


def test_startup_terminal_exit_missing_position_watermark_holds_before_terminal_cas(
    monkeypatch,
):
    osm = _OSM(filled_qty=1)
    engine = _Engine(quantity_remaining=5, bridge_ok=False)
    recovery, broker, cursor = _recovery(
        monkeypatch,
        broker_payload={
            "id": BROKER_ID,
            "contract": CONTRACT,
            "status": "canceled",
            "quantity": 3,
            "exec_quantity": 1,
        },
        osm=osm,
        engine=engine,
        position=_position(),
    )
    result = {"errors": [], "exits_reattached": 0}

    recovery._recover_exit_fills_that_occurred_during_downtime(result)

    assert result["exits_reattached"] == 0
    assert any("startup_terminal_exit_fill_bridge_failed" in error for error in result["errors"])
    assert osm.transitions == []
    assert len(engine.bridge_calls) == 1
    assert not any("UPDATE positions SET status='OPEN'" in sql for sql, _ in cursor.statements)
    assert broker.cancel_calls == 0
