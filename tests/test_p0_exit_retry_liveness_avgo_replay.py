# tests/test_p0_exit_retry_liveness_avgo_replay.py
# =============================================================================
# PR #423 integration proof: a real APOrderMonitor and a real APExitEngine
# share the stale-exit cancel/replacement handoff for the motivating AVGO
# qty=7 / scale-out qty=2 shape.
# =============================================================================

from __future__ import annotations

import os
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
os.environ.setdefault("ENCRYPTION_KEY", "ap-pr423-avgo-replay-test")

from ap.order_monitor import APOrderMonitor  # noqa: E402
from ap_exit_engine import APExitEngine, ManagedPosition  # noqa: E402


class _ReplayBroker:
    def __init__(self, payloads):
        self.payloads = [dict(payload) for payload in payloads]
        self.get_calls = []
        self.cancel_calls = []

    def get_order(self, broker_order_id):
        self.get_calls.append(broker_order_id)
        if not self.payloads:
            return {"status": "unknown"}
        return dict(self.payloads.pop(0))

    def cancel_order(self, broker_order_id):
        self.cancel_calls.append(broker_order_id)
        return {"status": "pending"}


class _ReplayOSM:
    """Small durable-state stand-in that invokes the real exit-engine hooks."""

    def __init__(self, exit_engine, *, partial=False):
        self.exit_engine = exit_engine
        self.partial = partial
        self.order = {
            "local_order_id": "loc-avgo",
            "broker_order_id": "bro-avgo",
            "kind": "EXIT",
            "position_id": "pos-avgo",
            "status": "EXIT_ACKNOWLEDGED",
            "qty": 2,
            "filled_qty": 0,
        }
        self.transitions = []

    def get_order(self, local_order_id):
        if local_order_id != self.order["local_order_id"]:
            return None
        return dict(self.order)

    def transition(self, local_order_id, new_status, **kwargs):
        self.transitions.append((local_order_id, new_status, dict(kwargs)))
        if new_status == "EXIT_PARTIAL_FILL":
            self.order["status"] = new_status
            self.order["filled_qty"] = int(kwargs["filled_qty"])
            self.exit_engine.note_partial_exit_fill(
                self.order["position_id"],
                local_order_id=local_order_id,
                broker_order_id=self.order["broker_order_id"],
                cumulative_filled=int(kwargs["filled_qty"]),
                fill_price=kwargs.get("fill_price"),
            )
            return True
        if new_status == "CANCELED":
            self.order["status"] = new_status
            self.exit_engine.on_exit_failure(
                self.order["position_id"],
                reason=kwargs.get("last_error", ""),
                local_order_id=local_order_id,
                broker_order_id=self.order["broker_order_id"],
            )
            return True
        return True


def _engine(*, partial=False):
    engine = APExitEngine(broker=MagicMock())
    engine._emit_exit_event = MagicMock()
    engine._persist_exit_replace_attempt_to_db = MagicMock(return_value=True)
    position = ManagedPosition(
        ticker="AVGO",
        option_symbol="AVGO260814C00350000",
        side="CALL",
        quantity=7,
        entry_price=2.49,
        underlying_entry=350.0,
        underlying_target=360.0,
        underlying_stop=340.0,
        position_id="pos-avgo",
        client_id="client-avgo",
        execution_mode="paper",
        exit_in_flight=True,
        pending_exit_local_order_id="loc-avgo",
        pending_exit_broker_order_id="bro-avgo",
        pending_exit_qty=2,
    )
    engine._positions.append(position)
    engine._positions_by_id[position.position_id] = position
    return engine, position


def _monitor(monkeypatch, broker, osm, engine, pm):
    import ap.order_monitor as monitor_module

    # The production status cache is intentionally process-global.  Clear it
    # here so this replay is independent from another test's same broker id.
    with monitor_module._BROKER_STATUS_CACHE_LOCK:
        monitor_module._BROKER_STATUS_CACHE.clear()

    monkeypatch.setattr(monitor_module, "ORDER_MONITOR_MODE", "watchdog")
    monkeypatch.setattr(monitor_module, "ORDER_MONITOR_CAN_ACT", False)
    monkeypatch.setattr(
        monitor_module, "ALLOW_STALE_EXIT_RECOVERY_IN_WATCHDOG", True,
    )
    monitor = APOrderMonitor(
        client_id="client-avgo",
        broker=broker,
        order_state_machine=osm,
        position_manager=pm,
        exit_engine=engine,
        entry_watcher=MagicMock(),
        client_mode="PAPER",
    )
    monitor._emit_order_event = MagicMock()
    monitor._alert = MagicMock()
    return monitor


def test_avgo_real_monitor_and_exit_engine_cancel_replay_preserves_identity(monkeypatch):
    engine, position = _engine()
    broker = _ReplayBroker([
        {"status": "working"},
        {"status": "canceled"},
    ])
    osm = _ReplayOSM(engine)
    pm = MagicMock()
    monitor = _monitor(monkeypatch, broker, osm, engine, pm)

    monitor._handle_stale_exit(
        local_order_id="loc-avgo",
        status="WORKING",
        contract="AVGO260814C00350000",
        age_secs=120.0,
        position_id="pos-avgo",
        reason="stale AVGO scale-out",
    )

    assert broker.cancel_calls == ["bro-avgo"]
    assert [status for _, status, _ in osm.transitions] == ["CANCELED"]
    assert position.exit_in_flight is False
    assert position.pending_exit_local_order_id == ""
    assert position.pending_exit_broker_order_id == ""
    assert position.pending_exit_replace_allowed is True
    assert position.pending_exit_replace_qty == 2
    assert position.exit_replace_attempt == 1
    assert position.quantity_remaining == 7
    pm.update_position.assert_not_called()


def test_avgo_partial_fill_replay_cancels_only_unfilled_remainder(monkeypatch):
    engine, position = _engine(partial=True)
    broker = _ReplayBroker([
        {"status": "partially_filled", "exec_quantity": 1, "avg_fill_price": 2.40},
        {"status": "partially_filled", "exec_quantity": 1, "avg_fill_price": 2.40},
        {"status": "canceled"},
    ])
    osm = _ReplayOSM(engine, partial=True)
    pm = MagicMock()
    monitor = _monitor(monkeypatch, broker, osm, engine, pm)

    monitor._handle_stale_exit(
        local_order_id="loc-avgo",
        status="WORKING",
        contract="AVGO260814C00350000",
        age_secs=120.0,
        position_id="pos-avgo",
        reason="stale AVGO scale-out after partial fill",
    )

    assert broker.cancel_calls == ["bro-avgo"]
    assert [status for _, status, _ in osm.transitions] == [
        "EXIT_PARTIAL_FILL", "CANCELED",
    ]
    assert osm.order["filled_qty"] == 1
    assert position.quantity_remaining == 6
    assert position.exit_in_flight is False
    assert position.pending_exit_replace_allowed is True
    assert position.pending_exit_replace_qty == 1
    assert position.exit_replace_attempt == 1
    pm.update_position.assert_not_called()
