from __future__ import annotations

import inspect

from ap.order_monitor import APOrderMonitor


class DummyBroker:
    def __init__(self, quote=None, data_broker=None):
        self.quote = quote or {"bid": 1.0, "ask": 1.2}
        self.data_broker = data_broker

    def get_quote(self, symbol):
        return dict(self.quote)

    def place_order(self, **kwargs):
        return {"broker_order_id": "new-broker-id", "status": "SUBMITTED"}

    def cancel_order(self, broker_order_id):
        return {"status": "canceled"}


class DummyOSM:
    def __init__(self, row=None):
        self.row = row or {"kind": "ENTRY", "position_id": None}
        self.transitions = []

    def get_order(self, local_order_id):
        return dict(self.row)

    def transition(self, local_order_id, new_status, **kwargs):
        self.transitions.append((local_order_id, new_status, kwargs))
        return True


class DummyPM:
    pass


def make_monitor(broker=None, data_broker=None, row=None):
    return APOrderMonitor(
        client_id="test-client",
        broker=broker or DummyBroker(),
        order_state_machine=DummyOSM(row=row),
        position_manager=DummyPM(),
        client_mode="PAPER",
        data_broker=data_broker,
    )


def test_status_only_entry_fill_is_deferred_to_fill_monitor():
    monitor = make_monitor(row={"kind": "ENTRY", "position_id": None})
    events = []
    monitor._emit_order_event = lambda **kwargs: events.append(kwargs)

    monitor._advance_from_broker_status("local-1", "filled", "SPY260717C00500000")

    assert monitor.osm.transitions == []
    assert events[0]["reason_code"] == "BROKER_FILL_SEEN_DEFER_TO_FILL_MONITOR"
    assert events[0]["inputs"]["requires_fill_monitor"] is True


def test_status_only_exit_fill_is_deferred_to_fill_monitor():
    monitor = make_monitor(row={"kind": "EXIT", "position_id": "pos-1"})
    events = []
    monitor._emit_order_event = lambda **kwargs: events.append(kwargs)

    monitor._advance_from_broker_status("exit-1", "partially_filled", "SPY260717P00500000")

    assert monitor.osm.transitions == []
    assert events[0]["reason_code"] == "BROKER_FILL_SEEN_DEFER_TO_FILL_MONITOR"
    assert events[0]["position_id"] == "pos-1"


def test_non_fill_broker_status_still_advances_state_machine():
    monitor = make_monitor(row={"kind": "ENTRY"})

    monitor._advance_from_broker_status("local-1", "open", "SPY260717C00500000")

    assert monitor.osm.transitions == [("local-1", "ACKNOWLEDGED", {})]


def test_lost_handoff_rearm_does_not_default_missing_side_to_call():
    monitor = make_monitor()
    order = {"local_order_id": "local-1", "symbol": "SPY", "trigger_price": 500.0, "contract": "", "meta": {}}

    assert monitor._build_lost_handoff_plan_from_order(order) is None


def test_lost_handoff_rearm_derives_put_from_occ_contract():
    monitor = make_monitor()
    order = {"local_order_id": "local-1", "symbol": "SPY", "trigger_price": 500.0, "contract": "SPY260717P00500000", "meta": {}}

    plan = monitor._build_lost_handoff_plan_from_order(order)

    assert plan is not None
    assert plan.direction == "PUT"
    assert plan.side == "PUT"


def test_lost_handoff_rearm_derives_call_from_occ_contract():
    monitor = make_monitor()
    order = {"local_order_id": "local-1", "symbol": "SPY", "trigger_price": 500.0, "contract": "SPY260717C00500000", "meta": {}}

    plan = monitor._build_lost_handoff_plan_from_order(order)

    assert plan is not None
    assert plan.direction == "CALL"
    assert plan.side == "CALL"


def test_get_option_price_uses_explicit_data_broker():
    execution_broker = DummyBroker({"bid": 1.0, "ask": 1.2})
    data_broker = DummyBroker({"bid": 3.0, "ask": 5.0})
    monitor = make_monitor(broker=execution_broker, data_broker=data_broker)

    assert monitor._get_option_price("SPY260717C00500000") == 4.0


def test_active_entry_select_hydrates_retry_repeg_context():
    src = inspect.getsource(APOrderMonitor._get_active_entry_orders)

    for expected in ("qty", "direction", "execution_mode", "reserved_cost", "stop_underlying", "target_underlying"):
        assert expected in src
