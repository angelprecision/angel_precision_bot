from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import ap_execution_core


def _row(crash=False):
    meta = {
        "lifecycle_state": "BROKER_READY", "broker_ready": True,
        "trigger_crossed_at": (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat(),
        "trigger_price": 600.0, "observed_underlying_price": 600.1,
        "materialization_generation": 3,
    }
    if crash:
        meta.update(lifecycle_state="SUBMITTING", submit_intent_at=datetime.now(timezone.utc).isoformat(), broker_submit_key="oid-1")
    return {
        "local_order_id": "oid-1", "client_id": "jason@example.com",
        "execution_mode": "live", "signal_id": "sig-1", "plan_id": "plan-1",
        "kind": "ENTRY", "status": "PENDING_TRIGGER", "broker_order_id": None,
        "submitted_ts": None, "symbol": "SPY", "direction": "CALL",
        "contract": "SPY260717C00600000", "qty": 1, "limit_price": 2.10,
        "reserved_cost": 210.0, "meta": meta,
    }


def _core():
    core = SimpleNamespace(
        client_id="jason@example.com", email="jason@example.com",
        execution_mode="live", mode="LIVE", paper=False,
        order_state_machine=MagicMock(), broker=MagicMock(),
    )
    core._is_real_occ_contract = ap_execution_core.APExecutionCore._is_real_occ_contract
    core.resume_deferred_broker_ready_order = ap_execution_core.APExecutionCore.resume_deferred_broker_ready_order.__get__(core, type(core))
    core.reconcile_deferred_broker_intent = ap_execution_core.APExecutionCore.reconcile_deferred_broker_intent.__get__(core, type(core))
    return core


def test_broker_ready_claims_then_enters_canonical_callback_once():
    core = _core()
    after = {**_row(), "status": "SUBMITTED", "broker_order_id": "TR-1"}
    core.order_state_machine.get_order.side_effect = [_row(), after]
    core.order_state_machine.claim_deferred_broker_ready_submit.return_value = True
    core._on_entry_trigger = MagicMock()
    result = core.resume_deferred_broker_ready_order(local_order_id="oid-1")
    assert result["disposition"] == "SUBMITTED"
    core.order_state_machine.claim_deferred_broker_ready_submit.assert_called_once()
    core._on_entry_trigger.assert_called_once()
    watched = core._on_entry_trigger.call_args.args[0]
    assert watched.signal["_approved_plan"].contract_symbol == _row()["contract"]
    assert watched.signal["contract_deferred"] is False
    core.order_state_machine.submit_existing_entry.assert_not_called()


def test_losing_recovery_worker_never_enters_canonical_callback():
    core = _core()
    core.order_state_machine.get_order.return_value = _row()
    core.order_state_machine.claim_deferred_broker_ready_submit.return_value = False
    core._on_entry_trigger = MagicMock()
    result = core.resume_deferred_broker_ready_order(local_order_id="oid-1")
    assert result["reason_code"] == "RECOVERY_SUBMIT_CLAIM_NOT_ACQUIRED"
    core._on_entry_trigger.assert_not_called()


def _remote(status="open", **overrides):
    value = {"id": "TR-9", "tag": "oid-1", "option_symbol": "SPY260717C00600000", "side": "buy_to_open", "quantity": "1", "status": status}
    value.update(overrides)
    return value


def test_matching_working_order_is_adopted_and_monitor_owned_without_post():
    core = _core()
    core.order_state_machine.get_order.return_value = _row(crash=True)
    core.order_state_machine.transition.return_value = True
    core.broker.list_orders.return_value = [_remote()]
    result = core.reconcile_deferred_broker_intent(local_order_id="oid-1")
    assert result["reason_code"] == "BROKER_ORDER_ADOPTED"
    assert core.order_state_machine.update_order_meta.call_args.args[1]["current_owner"] == "ORDER_MONITOR"
    core.broker.place_order.assert_not_called()


def test_matching_fill_advances_through_existing_state_machine():
    core = _core()
    core.order_state_machine.get_order.return_value = _row(crash=True)
    core.order_state_machine.transition.return_value = True
    core.broker.list_orders.return_value = [_remote("filled", exec_quantity=1, avg_fill_price=2.08)]
    result = core.reconcile_deferred_broker_intent(local_order_id="oid-1")
    assert result["status"] == "FILLED"
    assert core.order_state_machine.transition.call_count == 2
    assert core.order_state_machine.transition.call_args.args[:2] == ("oid-1", "FILLED")
    core.broker.place_order.assert_not_called()


def test_wrong_contract_and_multiple_matches_fail_closed():
    core = _core()
    core.order_state_machine.get_order.return_value = _row(crash=True)
    core.broker.list_orders.return_value = [_remote(option_symbol="QQQ260717C00500000")]
    assert core.reconcile_deferred_broker_intent(local_order_id="oid-1")["reason_code"] == "RECONCILE_TAG_IDENTITY_MISMATCH"
    core.broker.list_orders.return_value = [_remote(), _remote(id="TR-10")]
    assert core.reconcile_deferred_broker_intent(local_order_id="oid-1")["reason_code"] == "RECONCILE_MULTIPLE_MATCHES"
    core.order_state_machine.transition.assert_not_called()


def test_query_failure_is_retryable_and_never_posts():
    core = _core()
    core.order_state_machine.get_order.return_value = _row(crash=True)
    core.broker.list_orders.side_effect = TimeoutError("timeout")
    result = core.reconcile_deferred_broker_intent(local_order_id="oid-1")
    assert result["disposition"] == "RECONCILE_PENDING"
    assert result["reason_code"] == "RECONCILE_BROKER_QUERY_FAILED:TimeoutError"
    core.broker.place_order.assert_not_called()
