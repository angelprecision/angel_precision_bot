"""PR #423 crash-boundary regressions for durable replacement ownership."""

from __future__ import annotations

import os
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
os.environ.setdefault("ENCRYPTION_KEY", "ap-pr423-restart-boundary-test")

from ap.exit_autonomous_recovery import recover_exit_engine
from ap_exit_engine import APExitEngine, ManagedPosition


POSITION_ID = "pos-restart-boundary"
CLIENT_ID = "client-restart-boundary"
CONTRACT = "AVGO260814C00350000"
OLD_LOCAL = "loc-restart-old"
OLD_BROKER = "bro-restart-old"


def _lifecycle(state: str) -> dict:
    if state == "NONE":
        return {
            "state": "NONE",
            "replace_attempt": 0,
            "replacement_generation": 0,
            "replace_quantity": 0,
            "last_ack_identity": "",
        }
    payload = {
        "state": state,
        "replace_attempt": 1,
        "replacement_generation": 1,
        "replace_quantity": 1,
        "position_id": POSITION_ID,
        "client_id": CLIENT_ID,
        "execution_mode": "paper",
        "old_local_order_id": OLD_LOCAL,
        "old_broker_order_id": OLD_BROKER,
        "last_ack_identity": OLD_BROKER,
    }
    if state == "REPLACEMENT_OWNED_BY_NEW_GENERATION":
        payload.update({
            "new_local_order_id": "loc-restart-new",
            "new_broker_order_id": "bro-restart-new",
        })
    return payload


def _position(state: str = "REPLACEMENT_PENDING") -> ManagedPosition:
    position = ManagedPosition(
        ticker="AVGO",
        option_symbol=CONTRACT,
        side="CALL",
        quantity=5,
        entry_price=2.49,
        underlying_entry=350.0,
        underlying_target=360.0,
        underlying_stop=340.0,
        position_id=POSITION_ID,
        client_id=CLIENT_ID,
        execution_mode="paper",
        quantity_remaining=5,
        exit_in_flight=False,
        pending_exit_local_order_id=OLD_LOCAL,
        pending_exit_broker_order_id=OLD_BROKER,
        pending_exit_qty=2,
    )
    position.exit_retry_liveness = _lifecycle(state)
    return position


class _BrokerReadBoundary:
    def __init__(self):
        self.open_order_calls = 0
        self.position_calls = 0
        self.cancel_calls = 0

    def get_order(self, broker_order_id):
        assert broker_order_id == OLD_BROKER
        return {"status": "canceled"}

    def list_open_orders(self):
        self.open_order_calls += 1
        return []

    def list_positions(self):
        self.position_calls += 1
        return [{"contract": CONTRACT, "quantity": 5}]

    def cancel_order(self, broker_order_id):
        self.cancel_calls += 1
        raise AssertionError("durable replacement recovery must not DELETE")


class _BrokerReplacementFilled(_BrokerReadBoundary):
    def get_order(self, broker_order_id):
        if broker_order_id == "bro-restart-new":
            return {"status": "filled", "exec_quantity": 1}
        return super().get_order(broker_order_id)


class _OSM:
    def __init__(self, *, reserved_row=None, transition_result=True, old_status="CANCELED"):
        self.old_row = {
            "local_order_id": OLD_LOCAL,
            "broker_order_id": OLD_BROKER,
            "position_id": POSITION_ID,
            "client_id": CLIENT_ID,
            "execution_mode": "paper",
            "kind": "EXIT",
            "status": old_status,
            "qty": 2,
            "filled_qty": 1,
        }
        self.reserved_row = reserved_row
        self.transition_result = transition_result
        self.transitions = []

    def get_order(self, local_order_id):
        if local_order_id == OLD_LOCAL:
            return dict(self.old_row)
        if self.reserved_row and local_order_id == self.reserved_row.get("local_order_id"):
            return dict(self.reserved_row)
        return None

    def _get_active_exit_order(self, position_id):
        if self.reserved_row and position_id == POSITION_ID:
            return dict(self.reserved_row)
        return None

    def transition(self, local_order_id, status, **kwargs):
        self.transitions.append((local_order_id, status, dict(kwargs)))
        if local_order_id != OLD_LOCAL:
            return False
        if self.transition_result:
            self.old_row["status"] = status
        return self.transition_result


def _engine(position, osm, *, persist=True):
    engine = APExitEngine(broker=MagicMock(), email=CLIENT_ID)
    engine._emit_exit_event = MagicMock()
    engine.order_state_machine = osm
    engine.osm = osm
    engine._positions.append(position)
    engine._positions_by_id[position.position_id] = position
    engine._persist_exit_replace_attempt_to_db = MagicMock(return_value=persist)
    engine._sync_replacement_runtime_from_lifecycle(position, revalidated=False)
    return engine


def test_restart_after_fresh_terminal_proof_replays_staged_fence_without_delete():
    """Crash A: terminal proof before OSM CAS is recoverable from STAGED."""
    position = _position("STAGED")
    position.exit_in_flight = True
    osm = _OSM(old_status="EXIT_ACKNOWLEDGED")
    broker = _BrokerReadBoundary()
    engine = _engine(position, osm)

    actions = recover_exit_engine(engine, broker=broker, osm=osm, order_monitor=None)

    assert [action.action for action in actions] == ["REPLACEMENT_SAFE"]
    assert osm.transitions[-1][0:2] == (OLD_LOCAL, "CANCELED")
    assert broker.cancel_calls == 0
    assert position.quantity_remaining == 5
    assert position.exit_retry_liveness["state"] == "REPLACEMENT_PENDING"
    assert position.exit_in_flight is False


def test_restart_after_terminal_cas_and_failed_lifecycle_commit_retains_staged():
    """Crash B: a failed post-CAS commit keeps a retryable STAGED fence."""
    position = _position("NONE")
    position.exit_in_flight = True
    position.pending_exit_local_order_id = OLD_LOCAL
    position.pending_exit_broker_order_id = OLD_BROKER
    position.pending_exit_qty = 2
    osm = _OSM()
    broker = _BrokerReadBoundary()
    engine = _engine(position, osm)
    engine._persist_exit_replace_attempt_to_db = MagicMock(side_effect=[True, False])

    assert engine.mark_exit_replacement_safe(
        POSITION_ID,
        reason="terminal old generation",
        local_order_id=OLD_LOCAL,
        broker_order_id=OLD_BROKER,
        replacement_qty=1,
        defer_attempt=True,
    ) is True
    assert position.exit_retry_liveness["state"] == "STAGED"

    from ap.exit_autonomous_recovery import _mark_replacement_safe

    failed = _mark_replacement_safe(
        engine,
        POSITION_ID,
        osm=osm,
        reason="post-cas crash injection",
        local_id=OLD_LOCAL,
        broker_id=OLD_BROKER,
        details={},
        replacement_qty=1,
    )
    assert failed.action == "NOOP"
    assert failed.reason == "replacement_generation_commit_failed_staged_retained"
    assert position.exit_retry_liveness["state"] == "STAGED"

    engine._persist_exit_replace_attempt_to_db = MagicMock(return_value=True)
    recovered = recover_exit_engine(engine, broker=broker, osm=osm, order_monitor=None)
    assert [action.action for action in recovered] == ["REPLACEMENT_SAFE"]
    assert position.exit_retry_liveness["state"] == "REPLACEMENT_PENDING"
    assert position.quantity_remaining == 5


def test_repeated_restart_with_pending_authority_revalidates_once_without_osm_or_delete():
    """Crashes C/D plus a second restart never lose or duplicate the obligation."""
    position = _position("REPLACEMENT_PENDING")
    osm = _OSM()
    broker = _BrokerReadBoundary()
    engine = _engine(position, osm)

    first = recover_exit_engine(engine, broker=broker, osm=osm, order_monitor=None)
    second = recover_exit_engine(engine, broker=broker, osm=osm, order_monitor=None)

    assert [action.action for action in first] == ["REPLACEMENT_PENDING"]
    assert [action.action for action in second] == ["REPLACEMENT_PENDING"]
    assert position.pending_exit_replace_revalidated is True
    assert position.pending_exit_replace_qty == 1
    assert position.quantity_remaining == 5
    assert osm.transitions == []
    assert broker.cancel_calls == 0
    assert broker.open_order_calls == 2
    assert broker.position_calls == 2


def test_restart_after_replacement_fill_consumes_owned_lifecycle_without_delete():
    """A crash between fill persistence and lifecycle consumption is replay-safe."""
    position = _position("REPLACEMENT_OWNED_BY_NEW_GENERATION")
    position.quantity_remaining = 4
    position.pending_exit_local_order_id = "loc-restart-new"
    position.pending_exit_broker_order_id = "bro-restart-new"
    position.pending_exit_qty = 1
    position.pending_exit_filled_qty = 1
    position.exit_in_flight = True
    osm = _OSM(
        reserved_row={
            "local_order_id": "loc-restart-new",
            "broker_order_id": "bro-restart-new",
            "position_id": POSITION_ID,
            "client_id": CLIENT_ID,
            "execution_mode": "paper",
            "kind": "EXIT",
            "status": "EXIT_FILLED",
            "qty": 1,
            "filled_qty": 1,
        }
    )
    broker = _BrokerReplacementFilled()
    engine = _engine(position, osm)

    actions = recover_exit_engine(engine, broker=broker, osm=osm, order_monitor=None)

    assert [action.action for action in actions] == ["REPLACEMENT_FILL_CONSUMED"]
    assert position.exit_retry_liveness["state"] == "NONE"
    assert position.exit_replace_attempt == 0
    assert position.exit_in_flight is False
    assert broker.cancel_calls == 0


def test_reserved_generation_without_broker_ack_is_not_consumed_or_re_reserved():
    """Crashes E/F: an exact local reservation blocks a second reservation/POST."""
    position = _position("REPLACEMENT_PENDING")
    reserved = {
        "local_order_id": "loc-restart-new",
        "broker_order_id": "",
        "position_id": POSITION_ID,
        "client_id": CLIENT_ID,
        "execution_mode": "paper",
        "kind": "EXIT",
        "status": "EXIT_REQUESTED",
        "qty": 1,
    }
    osm = _OSM(reserved_row=reserved)
    broker = _BrokerReadBoundary()
    engine = _engine(position, osm)

    first = recover_exit_engine(engine, broker=broker, osm=osm, order_monitor=None)
    second = recover_exit_engine(engine, broker=broker, osm=osm, order_monitor=None)

    assert [action.action for action in first] == ["REPLACEMENT_RESERVED"]
    assert [action.action for action in second] == ["REPLACEMENT_RESERVED"]
    assert position.exit_retry_liveness["state"] == "REPLACEMENT_PENDING"
    assert position.pending_exit_local_order_id == "loc-restart-new"
    assert position.pending_exit_broker_order_id == ""
    assert position.exit_in_flight is True
    assert broker.open_order_calls == 0
    assert broker.position_calls == 0
    assert broker.cancel_calls == 0


def test_malformed_replacement_metadata_grants_no_restart_authority():
    """Malformed lifecycle JSONB is a diagnostic HOLD, never a replacement grant."""
    position = _position("REPLACEMENT_PENDING")
    position.exit_retry_liveness["replace_quantity"] = True
    osm = _OSM()
    broker = _BrokerReadBoundary()
    engine = _engine(position, osm)

    actions = recover_exit_engine(engine, broker=broker, osm=osm, order_monitor=None)

    assert [action.action for action in actions] == ["NOOP"]
    assert actions[0].reason == "replacement_lifecycle_invalid_fail_closed"
    assert position.pending_exit_replace_allowed is False
    assert broker.open_order_calls == 0
    assert broker.position_calls == 0
    assert broker.cancel_calls == 0


@pytest.mark.parametrize("row_updates", [{}, {"execution_mode": "live"}])
def test_restart_hydration_never_defaults_replacement_row_mode(row_updates, monkeypatch):
    """A newer replacement row must carry exact client and mode identity."""
    position = _position("REPLACEMENT_PENDING")
    engine = _engine(position, _OSM())
    row = {
        "local_order_id": "loc-restart-new",
        "broker_order_id": "",
        "status": "EXIT_REQUESTED",
        "client_id": CLIENT_ID,
        "qty": 1,
        "filled_qty": 0,
    }
    row.update(row_updates)

    class _Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, *args):
            return self

        def fetchone(self):
            return row

    import ap.db as db_module

    monkeypatch.setattr(db_module, "conn", lambda: _Connection())
    monkeypatch.setattr(db_module, "run_with_retry", lambda fn, *args, **kwargs: fn(*args, **kwargs))

    assert engine.hydrate_pending_exit_identity_from_db(position) is False
    assert position.exit_in_flight is False
    assert position.pending_exit_local_order_id == OLD_LOCAL
    assert position.pending_exit_broker_order_id == OLD_BROKER
