from __future__ import annotations

import sys
import types
from types import SimpleNamespace

from ap import repair_position_flat_guard as guard


class FakeExitEngine:
    def __init__(self, pos):
        import threading
        self._lock = threading.Lock()
        self._positions_by_id = {pos.position_id: pos}
        self.events = []
        self.original_called = False

    def _emit_exit_event(self, pos, **kwargs):
        self.events.append(kwargs)


def test_flat_repair_guard_returns_before_original_and_clears_memory(monkeypatch):
    def original(self, pos, decision, *args, **kwargs):
        self.original_called = True
        return True

    FakeExitEngine._submit_exit_decision = original
    fake_module = types.SimpleNamespace(APExitEngine=FakeExitEngine)
    monkeypatch.setitem(sys.modules, "ap_exit_engine", fake_module)
    if hasattr(FakeExitEngine, "_AP_FLAT_REPAIR_RUNTIME_GUARD"):
        delattr(FakeExitEngine, "_AP_FLAT_REPAIR_RUNTIME_GUARD")

    guard.install_repair_position_flat_guard()

    pos = SimpleNamespace(
        position_id="broker-repair-client@example.com-VZ260717C00040000",
        quantity_remaining=0,
        closed=False,
        close_reason="",
        exit_in_flight=True,
        pending_exit_reason="old",
        pending_exit_action="CLOSE_ALL",
        pending_exit_qty=1,
        pending_exit_local_order_id="L-EXIT",
        pending_exit_broker_order_id="B-EXIT",
    )
    engine = FakeExitEngine(pos)

    result = engine._submit_exit_decision(pos, SimpleNamespace(action="CLOSE_ALL"))

    assert result is False
    assert engine.original_called is False
    assert pos.closed is True
    assert pos.close_reason == "broker_truth_position_flat"
    assert pos.quantity_remaining == 0
    assert pos.exit_in_flight is False
    assert pos.pending_exit_reason == ""
    assert pos.pending_exit_action == ""
    assert pos.pending_exit_qty == 0
    assert pos.pending_exit_local_order_id == ""
    assert pos.pending_exit_broker_order_id == ""
    assert pos.position_id not in engine._positions_by_id
    assert engine.events[-1]["reason_code"] == "BROKER_TRUTH_POSITION_FLAT"


def test_non_repair_position_calls_original(monkeypatch):
    def original(self, pos, decision, *args, **kwargs):
        self.original_called = True
        return True

    FakeExitEngine._submit_exit_decision = original
    fake_module = types.SimpleNamespace(APExitEngine=FakeExitEngine)
    monkeypatch.setitem(sys.modules, "ap_exit_engine", fake_module)
    if hasattr(FakeExitEngine, "_AP_FLAT_REPAIR_RUNTIME_GUARD"):
        delattr(FakeExitEngine, "_AP_FLAT_REPAIR_RUNTIME_GUARD")

    guard.install_repair_position_flat_guard()

    pos = SimpleNamespace(position_id="pos-normal", quantity_remaining=0, closed=False)
    engine = FakeExitEngine(pos)

    assert engine._submit_exit_decision(pos, SimpleNamespace(action="CLOSE_ALL")) is True
    assert engine.original_called is True
