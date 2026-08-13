from datetime import datetime, timedelta, timezone
import inspect
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import ap_execution_core
from ap.order_state_machine import APOrderStateMachine


def _row(crash=False):
    meta = {
        "lifecycle_state": "BROKER_READY", "broker_ready": True,
        "trigger_crossed_at": (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat(),
        "trigger_price": 600.0, "observed_underlying_price": 600.1,
        "materialization_generation": 3,
        "selected_contract": "SPY260717C00600000",
        "selected_limit": 2.10,
        "selected_qty": 1,
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
    assert watched.signal["recovery_submit_owner"].startswith("recovery_submit:oid-1:")
    assert watched.signal["_approved_plan"].metadata["recovery_submit_generation"] == 3
    core.order_state_machine.submit_existing_entry.assert_not_called()


def test_losing_recovery_worker_never_enters_canonical_callback():
    core = _core()
    core.order_state_machine.get_order.return_value = _row()
    core.order_state_machine.claim_deferred_broker_ready_submit.return_value = False
    core._on_entry_trigger = MagicMock()
    result = core.resume_deferred_broker_ready_order(local_order_id="oid-1")
    assert result["reason_code"] == "RECOVERY_SUBMIT_CLAIM_NOT_ACQUIRED"
    core._on_entry_trigger.assert_not_called()


def test_placeholder_broker_ready_row_fails_closed_before_claim():
    core = _core()
    bad = _row()
    bad["contract"] = "DEFERRED:SPY"
    bad["limit_price"] = 0.01
    bad["meta"]["selected_contract"] = "SPY260717C00600000"
    bad["meta"]["selected_limit"] = 2.10
    bad["meta"]["selected_qty"] = 1
    core.order_state_machine.get_order.return_value = bad
    result = core.resume_deferred_broker_ready_order(local_order_id="oid-1")
    assert result["disposition"] == "TERMINAL_DURABLE"
    assert result["reason_code"] == "RECOVERY_INVALID_OCC_CONTRACT"
    core.order_state_machine.claim_deferred_broker_ready_submit.assert_not_called()


def test_deferred_breach_path_has_penny_limit_fail_closed_guard():
    source = inspect.getsource(ap_execution_core.APExecutionCore._on_entry_trigger)
    assert "_plan_limit <= 0.01" in source
    assert "deferred selection produced penny limit" in source


def _remote(status="open", **overrides):
    value = {"id": "TR-9", "tag": "oid-1", "option_symbol": "SPY260717C00600000", "side": "buy_to_open", "quantity": "1", "status": status}
    value.update(overrides)
    return value


def _reconcile_existing_broker_order(status="filled", **overrides):
    core = _core()
    row = _row(crash=True)
    transition_calls = []

    def _transition(local_order_id, next_status, **kwargs):
        transition_calls.append((local_order_id, next_status, dict(kwargs)))
        row["status"] = next_status
        if kwargs.get("broker_order_id"):
            row["broker_order_id"] = kwargs["broker_order_id"]
        if kwargs.get("filled_qty") is not None:
            row["filled_qty"] = kwargs["filled_qty"]
        if kwargs.get("fill_price") is not None:
            row["fill_price"] = kwargs["fill_price"]
        return True

    core.order_state_machine.get_order.return_value = row
    core.order_state_machine.transition.side_effect = _transition
    core.broker.list_orders.return_value = [_remote(status, **overrides)]
    result = core.reconcile_deferred_broker_intent(local_order_id="oid-1")
    return core, row, transition_calls, result


def test_matching_working_order_is_adopted_and_monitor_owned_without_post():
    core = _core()
    core.order_state_machine.get_order.return_value = _row(crash=True)
    core.order_state_machine.transition.return_value = True
    core.broker.list_orders.return_value = [_remote()]
    result = core.reconcile_deferred_broker_intent(local_order_id="oid-1")
    assert result["reason_code"] == "BROKER_ORDER_ADOPTED"
    assert core.order_state_machine.update_order_meta.call_args.args[1]["current_owner"] == "ORDER_MONITOR"
    core.broker.place_order.assert_not_called()


def test_matching_fill_stays_submitted_for_canonical_fill_monitor():
    core = _core()
    core.order_state_machine.get_order.return_value = _row(crash=True)
    core.order_state_machine.transition.return_value = True
    core.broker.list_orders.return_value = [_remote("filled", exec_quantity=1, avg_fill_price=2.08)]
    result = core.reconcile_deferred_broker_intent(local_order_id="oid-1")
    assert result["status"] == "SUBMITTED"
    assert core.order_state_machine.transition.call_count == 1
    assert core.order_state_machine.transition.call_args.args[:2] == ("oid-1", "SUBMITTED")
    core.broker.place_order.assert_not_called()


def test_reconciled_filled_order_hands_off_once_to_fill_monitor_and_position_owner(
    monkeypatch,
):
    from ap import fill_monitor as fm

    core, row, transition_calls, result = _reconcile_existing_broker_order(
        "filled", exec_quantity=1, avg_fill_price=2.08
    )
    assert result["status"] == "SUBMITTED"
    assert row["status"] == "SUBMITTED"
    assert row["broker_order_id"] == "TR-9"

    order = {**row, "meta": dict(row.get("meta") or {}), "filled_qty": 0}
    core.broker.get_order.return_value = {
        "id": "TR-9",
        "status": "filled",
        "exec_quantity": 1,
        "avg_fill_price": 2.08,
    }
    opened = []
    bound = []
    seeded = []
    verified = []
    monkeypatch.setattr(fm, "audit", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "trace_gate", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "_cancel_pair_opposite", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        fm,
        "_open_position_safe",
        lambda *args, **kwargs: opened.append((args, kwargs)) or "position-1",
    )
    monkeypatch.setattr(
        fm,
        "_establish_canonical_handoff_standing_stop",
        lambda **kwargs: {
            "state": "SUBMITTED",
            "standing_stop_attempted": True,
            "protection_proven": True,
        },
    )
    monkeypatch.setattr(
        fm,
        "_bind_filled_entry_position_id",
        lambda *args: bound.append(args) or {"ok": True, "disposition": "BOUND"},
    )
    monkeypatch.setattr(
        fm,
        "_seed_exit_engine",
        lambda *args: seeded.append(args) or {"ok": True, "disposition": "SEEDED"},
    )
    monkeypatch.setattr(
        fm,
        "_verify_canonical_entry_owner",
        lambda *args: verified.append(args) or {"ok": True},
    )
    monkeypatch.setattr(
        fm, "_clear_canonical_owner_handoff_retry", lambda *_args: True
    )
    monkeypatch.setattr(fm, "_release_entry_guards", lambda *_args: None)

    fm.process_pending_order(
        core.broker,
        order,
        osm=core.order_state_machine,
        pm=object(),
        exit_engine=SimpleNamespace(),
        runtime_execution_mode="live",
    )

    assert core.broker.get_order.call_count == 1
    assert [call[1] for call in transition_calls] == ["SUBMITTED", "FILLED"]
    assert transition_calls[-1][2] == {
        "filled_qty": 1,
        "fill_price": 2.08,
        "broker_order_id": "TR-9",
    }
    assert len(opened) == 1
    assert len(bound) == len(seeded) == len(verified) == 1
    assert row["status"] == "FILLED"
    assert row["filled_qty"] == 1
    assert row["fill_price"] == 2.08


@pytest.mark.parametrize(
    ("broker_status", "expected_status", "remote_fields"),
    [
        (
            "partially_filled",
            "PARTIAL_FILL",
            {"exec_quantity": 1, "avg_fill_price": 2.08},
        ),
        ("rejected", "REJECTED", {}),
        ("canceled", "CANCELED", {}),
        ("expired", "EXPIRED", {}),
    ],
)
def test_reconciled_nonfilled_statuses_are_consumed_by_canonical_monitor(
    monkeypatch, broker_status, expected_status, remote_fields
):
    from ap import fill_monitor as fm

    core, row, _reconcile_calls, result = _reconcile_existing_broker_order(
        broker_status, **remote_fields
    )
    assert result["status"] == "SUBMITTED"
    order = {**row, "meta": dict(row.get("meta") or {}), "filled_qty": 0}
    core.broker.get_order.return_value = {
        "id": "TR-9",
        "status": broker_status,
        **remote_fields,
    }
    monkeypatch.setattr(fm, "audit", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "_release_entry_guards", lambda *_args: None)
    monkeypatch.setattr(
        fm,
        "_open_position_safe",
        lambda *_args, **_kwargs: pytest.fail(
            "partial/terminal monitor status must not create a position"
        ),
    )

    core.order_state_machine.transition.reset_mock()
    fm.process_pending_order(
        core.broker,
        order,
        osm=core.order_state_machine,
        pm=None,
        exit_engine=None,
        runtime_execution_mode="live",
    )

    core.order_state_machine.transition.assert_called_once()
    call = core.order_state_machine.transition.call_args
    assert call.args[:2] == ("oid-1", expected_status)
    assert row["status"] == expected_status


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


def _submit_osm(intent_results):
    osm = object.__new__(APOrderStateMachine)
    osm.client_id = "jason@example.com"
    osm.execution_mode = "live"
    row = _row()
    osm._get_order = MagicMock(return_value=row)
    results = iter(intent_results)

    def _persist_intent(*_args, **kwargs):
        persisted = next(results)
        if persisted:
            submit_key = kwargs["broker_submit_key"]
            row["meta"].update({
                "lifecycle_state": "SUBMITTING",
                "submit_intent_at": datetime.now(timezone.utc).isoformat(),
                "broker_submit_key": submit_key,
                "broker_submit_payload_hash": kwargs["payload_hash"],
                "current_owner": f"broker_submit:{submit_key}",
            })
        return persisted

    osm.persist_deferred_submit_intent = MagicMock(side_effect=_persist_intent)
    osm.update_order_meta = MagicMock(return_value=True)
    osm.transition = MagicMock(return_value=True)
    osm._submit_order_with_retry = MagicMock(return_value=(
        {"id": "TR-1"}, None, "TR-1", "ACK",
    ))
    osm._lookup_order_by_tag = MagicMock(return_value=None)
    osm._flag_split_brain_order = MagicMock()
    return osm


def _recovery_plan(owner):
    row = _row()
    return SimpleNamespace(
        contract_symbol=row["contract"], contracts=row["qty"],
        signal_id=row["signal_id"], client_id=row["client_id"],
        execution_mode=row["execution_mode"], ticker=row["symbol"],
        side="CALL", direction="CALL", timeframe="1d", score=90.0,
        trigger_price=600.0, underlying_entry=599.5,
        target_underlying=605.0, stop_underlying=595.0,
        metadata={
            "recovery_submit_fenced": True,
            "recovery_submit_owner": owner,
            "recovery_submit_generation": 3,
        },
    )


def test_lease_expiry_race_real_submit_path_allows_exactly_one_post():
    """A lost worker's intent CAS fails; the replacement alone reaches POST."""
    osm = _submit_osm([False, True])
    broker = MagicMock(base_url="https://api.tradier.com")

    stale = osm.submit_existing_entry(
        local_order_id="oid-1", broker=broker,
        plan=_recovery_plan("worker-A"), limit_price=2.10,
    )
    winner = osm.submit_existing_entry(
        local_order_id="oid-1", broker=broker,
        plan=_recovery_plan("worker-B"), limit_price=2.10,
    )

    assert stale["error"] == "RECOVERY_SUBMIT_INTENT_FENCE_LOST"
    assert winner["ok"] is True
    assert osm._submit_order_with_retry.call_count == 1
    assert osm.persist_deferred_submit_intent.call_args_list[0].kwargs["owner"] == "worker-A"
    assert osm.persist_deferred_submit_intent.call_args_list[1].kwargs["owner"] == "worker-B"


def test_any_recovery_intent_cas_failure_blocks_broker_bytes():
    """Owner/generation/lease/intent/DB CAS misses share one fail-closed result."""
    for owner in ("wrong-owner", "expired-lease", "wrong-generation", "db-failure"):
        osm = _submit_osm([False])
        result = osm.submit_existing_entry(
            local_order_id="oid-1", broker=MagicMock(),
            plan=_recovery_plan(owner), limit_price=2.10,
        )
        assert result["error"] == "RECOVERY_SUBMIT_INTENT_FENCE_LOST"
        osm._submit_order_with_retry.assert_not_called()


def test_submit_intent_cas_declares_all_irreversible_boundary_predicates():
    import inspect
    source = inspect.getsource(APOrderStateMachine.persist_deferred_submit_intent)
    for proof in (
        "local_order_id = %s", "client_id = %s", "execution_mode,'')) = %s",
        "status,'')) = 'PENDING_TRIGGER'", "broker_order_id IS NULL",
        "submitted_ts IS NULL", "lifecycle_state','') = 'BROKER_READY'",
        "broker_ready')::boolean", "materialization_generation')::int",
        "recovery_submit_owner','') = %s", "recovery_submit_lease_until",
        "submit_intent_at','') = ''",
    ):
        assert proof in source
