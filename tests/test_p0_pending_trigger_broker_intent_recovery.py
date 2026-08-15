"""Focused P0 regression coverage for broker-owned PENDING_TRIGGER recovery."""

from __future__ import annotations

import os
import sys
import types
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/intelligence_test?sslmode=disable",
)
os.environ.setdefault("PGSSLMODE", "disable")

import ap.db as db
import ap_execution_core
import ap_reconciler
from ap.order_monitor import APOrderMonitor
from ap_recovery import APStartupRecovery
from ap_reconciler import APBrokerReconciler, _empty_summary


CLIENT_ID = "tradefluencehq@gmail.com"
LOCAL_ORDER_ID = "9f1907fe-7ab4-4e5a-b305-7fc9129f684d"
CONTRACT = "JNJ260821C00260000"
TAG = LOCAL_ORDER_ID[:32]


def _row(**overrides):
    row = {
        "local_order_id": LOCAL_ORDER_ID,
        "client_id": CLIENT_ID,
        "execution_mode": "paper",
        "kind": "ENTRY",
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "submitted_ts": None,
        "symbol": "JNJ",
        "contract": CONTRACT,
        "qty": 4,
        "limit_price": 4.57,
        "filled_qty": None,
        "fill_price": None,
        "meta": {
            "watcher_audit": {"reason_code": "trigger_ready"},
            "selected_contract": CONTRACT,
            "selected_qty": 4,
            "live_submit_gate": {"all_passed": True},
            "current_owner": f"broker_submit:{LOCAL_ORDER_ID}",
            "lifecycle_state": "SUBMITTING",
            "submit_intent_at": "2026-08-14T16:29:30+00:00",
            "submit_started_at": "2026-08-14T16:29:30+00:00",
            "broker_submit_key": TAG,
            "broker_submit_payload_hash": "sha256:fixture",
        },
    }
    row.update(overrides)
    if "meta" not in overrides:
        row["meta"] = deepcopy(row["meta"])
    return row


def _remote(status="open", **overrides):
    order = {
        "id": "TR-JNJ-472",
        "tag": TAG,
        "option_symbol": CONTRACT,
        "side": "buy_to_open",
        "quantity": "4",
        "status": status,
    }
    order.update(overrides)
    return order


def _core(row, broker_orders):
    osm = MagicMock()
    osm.get_order.return_value = row
    osm.transition.return_value = True
    broker = MagicMock()
    broker.list_orders.return_value = broker_orders
    core = types.SimpleNamespace(
        client_id=CLIENT_ID,
        email=CLIENT_ID,
        execution_mode="paper",
        mode="PAPER",
        broker=broker,
        order_state_machine=osm,
    )
    core._is_real_occ_contract = ap_execution_core.APExecutionCore._is_real_occ_contract
    core.reconcile_deferred_broker_intent = (
        ap_execution_core.APExecutionCore.reconcile_deferred_broker_intent.__get__(
            core, type(core)
        )
    )
    return core, broker, osm


def _assert_no_broker_mutations(broker):
    for method_name in (
        "place_order",
        "submit_order",
        "replace_order",
        "cancel_order",
        "post",
    ):
        assert not getattr(broker, method_name).called, method_name


class _RecoveryCursor:
    def __init__(self, rows):
        self.rows = rows
        self.rowcount = 0

    def execute(self, *_args, **_kwargs):
        return self

    def fetchall(self):
        return self.rows


class _RecoveryConn:
    def __init__(self, rows):
        self.cursor = _RecoveryCursor(rows)

    def __enter__(self):
        return self.cursor

    def __exit__(self, *_args):
        return False


def _run_deferred_recovery(monkeypatch, row, execution_core):
    monkeypatch.setattr(db, "conn", lambda: _RecoveryConn([row]))
    monkeypatch.setattr(db, "run_with_retry", lambda fn, *args, **kwargs: fn())
    osm = MagicMock()
    osm.client_id = CLIENT_ID
    osm.retain_recovery_ownership_if_no_watcher = None
    osm.update_order_meta.return_value = True
    recovery = APStartupRecovery(
        client_id=CLIENT_ID,
        broker=MagicMock(),
        osm=osm,
        pm=None,
        master_control=types.SimpleNamespace(mode="PAPER"),
        execution_core=execution_core,
    )
    result = {"deferred_lifecycles_recovered": 0}
    recovery._recover_deferred_breach_lifecycles(result)
    return osm, result


def test_exact_match_adopts_only_submitted_for_every_remote_status():
    for remote_status in (
        "open",
        "filled",
        "partially_filled",
        "rejected",
        "canceled",
        "expired",
    ):
        core, broker, osm = _core(_row(), [_remote(remote_status)])

        result = core.reconcile_deferred_broker_intent(
            local_order_id=LOCAL_ORDER_ID
        )

        assert result["reason_code"] == "BROKER_ORDER_ADOPTED"
        assert result["status"] == "SUBMITTED"
        osm.transition.assert_called_once()
        assert osm.transition.call_args.args[:2] == (LOCAL_ORDER_ID, "SUBMITTED")
        transition_kwargs = osm.transition.call_args.kwargs
        assert transition_kwargs["broker_order_id"] == "TR-JNJ-472"
        assert transition_kwargs["submitted_ts"]
        assert "filled_qty" not in transition_kwargs
        assert "fill_price" not in transition_kwargs
        patch = osm.update_order_meta.call_args.args[1]
        assert patch["broker_reconcile_status"] == remote_status
        assert patch["lifecycle_state"] == "SUBMITTED"
        assert "filled_qty" not in patch
        assert "fill_price" not in patch
        _assert_no_broker_mutations(broker)


def test_existing_broker_id_repairs_pending_trigger_to_submitted():
    core, broker, osm = _core(_row(broker_order_id="TR-JNJ-472"), [])

    result = core.reconcile_deferred_broker_intent(
        local_order_id=LOCAL_ORDER_ID
    )

    assert result["reason_code"] == "RECONCILE_BROKER_ORDER_PRESENT"
    assert result["status"] == "SUBMITTED"
    osm.transition.assert_called_once()
    assert osm.transition.call_args.args[:2] == (LOCAL_ORDER_ID, "SUBMITTED")
    transition_kwargs = osm.transition.call_args.kwargs
    assert transition_kwargs["broker_order_id"] == "TR-JNJ-472"
    assert transition_kwargs["submitted_ts"]
    broker.list_orders.assert_not_called()
    osm.update_order_meta.assert_not_called()
    _assert_no_broker_mutations(broker)


def test_placeholder_broker_id_is_reconciled_as_missing():
    core, broker, osm = _core(_row(broker_order_id="N/A"), [_remote()])

    result = core.reconcile_deferred_broker_intent(
        local_order_id=LOCAL_ORDER_ID
    )

    assert result["reason_code"] == "BROKER_ORDER_ADOPTED"
    assert result["status"] == "SUBMITTED"
    osm.transition.assert_called_once()
    assert osm.transition.call_args.args[:2] == (LOCAL_ORDER_ID, "SUBMITTED")
    transition_kwargs = osm.transition.call_args.kwargs
    assert transition_kwargs["broker_order_id"] == "TR-JNJ-472"
    assert transition_kwargs["submitted_ts"]
    _assert_no_broker_mutations(broker)


def test_non_entry_kind_is_held_before_broker_read():
    core, broker, osm = _core(_row(kind="EXIT"), [_remote()])

    result = core.reconcile_deferred_broker_intent(
        local_order_id=LOCAL_ORDER_ID
    )

    assert result["disposition"] == "KEEP_WATCHER"
    assert result["reason_code"] == "RECONCILE_KIND_MISMATCH"
    broker.list_orders.assert_not_called()
    osm.transition.assert_not_called()
    osm.update_order_meta.assert_not_called()
    _assert_no_broker_mutations(broker)


def test_existing_broker_id_transition_cas_miss_holds_without_followup_mutation():
    core, broker, osm = _core(_row(broker_order_id="TR-JNJ-472"), [])
    osm.transition.return_value = False

    result = core.reconcile_deferred_broker_intent(
        local_order_id=LOCAL_ORDER_ID
    )

    assert result["disposition"] == "KEEP_WATCHER"
    assert result["reason_code"] == "RECONCILE_ADOPTION_TRANSITION_FAILED"
    broker.list_orders.assert_not_called()
    osm.update_order_meta.assert_not_called()
    _assert_no_broker_mutations(broker)


def test_non_string_submit_intent_is_held_before_broker_read():
    row = _row(meta={**_row()["meta"], "submit_intent_at": 12345})
    core, broker, osm = _core(row, [_remote()])

    result = core.reconcile_deferred_broker_intent(
        local_order_id=LOCAL_ORDER_ID
    )

    assert result["disposition"] == "KEEP_WATCHER"
    assert result["reason_code"] == "RECONCILE_SUBMIT_INTENT_MALFORMED"
    broker.list_orders.assert_not_called()
    osm.transition.assert_not_called()
    osm.update_order_meta.assert_not_called()
    _assert_no_broker_mutations(broker)


def test_reconcile_is_restart_idempotent_for_adopted_and_ambiguous_rows():
    core, broker, osm = _core(
        _row(status="SUBMITTED", broker_order_id="TR-JNJ-472"),
        [],
    )
    first = core.reconcile_deferred_broker_intent(local_order_id=LOCAL_ORDER_ID)
    second = core.reconcile_deferred_broker_intent(local_order_id=LOCAL_ORDER_ID)
    assert first["reason_code"] == "RECONCILE_BROKER_ORDER_PRESENT"
    assert second["reason_code"] == "RECONCILE_BROKER_ORDER_PRESENT"
    broker.list_orders.assert_not_called()
    osm.transition.assert_not_called()

    core, broker, osm = _core(
        _row(),
        [_remote(), _remote(id="TR-JNJ-473")],
    )
    first = core.reconcile_deferred_broker_intent(local_order_id=LOCAL_ORDER_ID)
    second = core.reconcile_deferred_broker_intent(local_order_id=LOCAL_ORDER_ID)
    assert first["reason_code"] == "RECONCILE_MULTIPLE_MATCHES"
    assert second["reason_code"] == "RECONCILE_MULTIPLE_MATCHES"
    osm.transition.assert_not_called()
    osm.update_order_meta.assert_not_called()
    _assert_no_broker_mutations(broker)


def test_empty_or_ambiguous_broker_truth_holds_without_post_or_cancel():
    cases = (
        [],
        [_remote(), _remote(id="TR-JNJ-473")],
        [_remote(option_symbol="JNJ260821P00260000")],
        [_remote(side="sell_to_open")],
        [_remote(quantity="4.5")],
        [_remote(quantity="malformed")],
        [_remote(quantity="0")],
        [_remote(id="N/A")],
        [_remote(id="0")],
    )
    for broker_orders in cases:
        core, broker, osm = _core(_row(), broker_orders)

        result = core.reconcile_deferred_broker_intent(
            local_order_id=LOCAL_ORDER_ID
        )

        assert result["disposition"] == "RECONCILE_PENDING"
        assert result["reason_code"] in {
            "RECONCILE_BROKER_NO_MATCH_HELD",
            "RECONCILE_MULTIPLE_MATCHES",
            "RECONCILE_TAG_IDENTITY_MISMATCH",
            "RECONCILE_MATCH_MISSING_ORDER_ID",
        }
        osm.transition.assert_not_called()
        osm.update_order_meta.assert_not_called()
        _assert_no_broker_mutations(broker)


def test_wrong_client_or_mode_is_untouched_before_broker_read():
    for overrides in (
        {"client_id": "other@example.com"},
        {"execution_mode": "live"},
    ):
        core, broker, osm = _core(_row(**overrides), [_remote()])

        result = core.reconcile_deferred_broker_intent(
            local_order_id=LOCAL_ORDER_ID
        )

        assert result["disposition"] == "KEEP_WATCHER"
        assert result["reason_code"] in {
            "RECONCILE_CLIENT_ID_MISMATCH",
            "RECONCILE_EXECUTION_MODE_MISMATCH",
        }
        broker.list_orders.assert_not_called()
        osm.transition.assert_not_called()
        _assert_no_broker_mutations(broker)


def test_malformed_broker_list_and_missing_submit_key_hold():
    core, broker, osm = _core(_row(), {"orders": [_remote()]})
    result = core.reconcile_deferred_broker_intent(local_order_id=LOCAL_ORDER_ID)
    assert result["reason_code"] == "RECONCILE_BROKER_LIST_MALFORMED"
    osm.transition.assert_not_called()
    _assert_no_broker_mutations(broker)

    row = _row()
    row["meta"].pop("broker_submit_key")
    core, broker, osm = _core(row, [_remote()])
    result = core.reconcile_deferred_broker_intent(local_order_id=LOCAL_ORDER_ID)
    assert result["reason_code"] == "RECONCILE_SUBMIT_KEY_MISSING"
    broker.list_orders.assert_not_called()
    osm.transition.assert_not_called()
    _assert_no_broker_mutations(broker)


def test_submit_key_must_match_local_order_before_broker_read():
    row = _row()
    row["meta"]["broker_submit_key"] = "other-local-order"
    core, broker, osm = _core(
        row,
        [_remote(tag="other-local-order")],
    )

    result = core.reconcile_deferred_broker_intent(
        local_order_id=LOCAL_ORDER_ID
    )

    assert result["reason_code"] == "RECONCILE_SUBMIT_KEY_LOCAL_ORDER_MISMATCH"
    broker.list_orders.assert_not_called()
    osm.transition.assert_not_called()
    _assert_no_broker_mutations(broker)


def test_malformed_exact_equal_occ_contract_is_held():
    malformed = f"{CONTRACT}TRAILING"
    core, broker, osm = _core(
        _row(contract=malformed),
        [_remote(option_symbol=malformed)],
    )

    result = core.reconcile_deferred_broker_intent(
        local_order_id=LOCAL_ORDER_ID
    )

    assert result["reason_code"] == "RECONCILE_CONTRACT_MALFORMED"
    osm.transition.assert_not_called()
    _assert_no_broker_mutations(broker)


def test_malformed_ticker_root_is_held():
    malformed = "J!J260821C00260000"
    row = _row(
        symbol="J!J",
        contract=malformed,
        meta={**_row()["meta"], "selected_contract": malformed},
    )
    core, broker, osm = _core(row, [_remote(option_symbol=malformed)])

    result = core.reconcile_deferred_broker_intent(
        local_order_id=LOCAL_ORDER_ID
    )

    assert result["reason_code"] == "RECONCILE_CONTRACT_MALFORMED"
    osm.transition.assert_not_called()
    _assert_no_broker_mutations(broker)


def test_deferred_ghost_sweep_fences_submit_evidence(monkeypatch):
    import ap.order_monitor as order_monitor

    class Cursor:
        def __init__(self):
            self.sql = ""

        def execute(self, sql, _params):
            self.sql = " ".join(str(sql).split())
            return self

        def fetchall(self):
            return []

    cursor = Cursor()

    @contextmanager
    def fake_conn():
        yield cursor

    monkeypatch.setattr(order_monitor, "EOD_DEFERRED_GHOST_SWEEP_ENABLED", True)
    monkeypatch.setattr(order_monitor, "conn", fake_conn)
    monkeypatch.setattr(order_monitor, "run_with_retry", lambda fn: fn())

    monitor = object.__new__(APOrderMonitor)
    monitor.client_id = CLIENT_ID
    monitor._is_after_pt_eod_cutoff = lambda: False

    assert monitor._sweep_stale_deferred_ghosts() == 0
    assert "submitted_ts IS NULL" in cursor.sql
    assert "submit_intent_at" in cursor.sql


def test_startup_recovery_holds_stale_submit_intent_before_terminalization(monkeypatch):
    row = _row(
        broker_order_id="N/A",
        created_ts=datetime.now(timezone.utc) - timedelta(days=4),
    )
    execution_core = MagicMock()
    execution_core.reconcile_deferred_broker_intent.return_value = {
        "disposition": "RECONCILE_PENDING",
        "reason_code": "RECONCILE_BROKER_NO_MATCH_HELD",
    }

    osm, _result = _run_deferred_recovery(monkeypatch, row, execution_core)

    execution_core.reconcile_deferred_broker_intent.assert_called_once_with(
        local_order_id=LOCAL_ORDER_ID
    )
    osm.terminalize_deferred_breach.assert_not_called()
    osm.submit_existing_entry.assert_not_called()


def test_osm_transition_accepts_placeholder_as_missing_broker_identity(monkeypatch):
    import ap.order_state_machine as order_state_machine

    class Cursor:
        rowcount = 1

        def execute(self, sql, params):
            self.sql = " ".join(str(sql).split())
            self.params = params
            return self

    cursor = Cursor()

    @contextmanager
    def fake_conn():
        yield cursor

    monkeypatch.setattr(order_state_machine, "conn", fake_conn)
    monkeypatch.setattr(
        order_state_machine,
        "run_with_retry",
        lambda fn, *args, **kwargs: fn(),
    )

    osm = object.__new__(order_state_machine.APOrderStateMachine)
    osm.client_id = CLIENT_ID
    osm._get_order = MagicMock(
        return_value={
            "local_order_id": LOCAL_ORDER_ID,
            "client_id": CLIENT_ID,
            "kind": "ENTRY",
            "status": "PENDING_TRIGGER",
            "broker_order_id": "N/A",
            "filled_qty": 0,
        }
    )
    osm._emit_transition_event = MagicMock()
    osm._notify_opportunity_ledger = MagicMock()
    osm._handle_exit_engine_hooks = MagicMock()
    osm._record_error = MagicMock()

    assert osm.transition(
        LOCAL_ORDER_ID,
        "SUBMITTED",
        broker_order_id="TR-JNJ-472",
    ) is True
    assert "'N/A','NA','NONE','NULL','PENDING','UNKNOWN','ERROR','0','FALSE'" in cursor.sql


def test_osm_terminalization_reasserts_submit_intent_cas(monkeypatch):
    import ap.order_state_machine as order_state_machine

    class Cursor:
        rowcount = 0

        def execute(self, sql, params):
            self.sql = " ".join(str(sql).split())
            self.params = params
            return self

    cursor = Cursor()

    @contextmanager
    def fake_conn():
        yield cursor

    monkeypatch.setattr(order_state_machine, "conn", fake_conn)
    monkeypatch.setattr(
        order_state_machine,
        "run_with_retry",
        lambda fn, *args, **kwargs: fn(),
    )

    osm = object.__new__(order_state_machine.APOrderStateMachine)
    osm.client_id = CLIENT_ID

    assert osm.terminalize_deferred_breach(
        LOCAL_ORDER_ID,
        reason_code="RECOVERY_STALE_PENDING_TRIGGER",
        terminal_status="EXPIRED",
    ) is False
    assert (
        "NULLIF(BTRIM(COALESCE(meta->>'submit_intent_at','')), '') IS NULL"
        in cursor.sql
    )


def test_startup_watcher_reseed_holds_submit_intent_before_ptr(monkeypatch):
    row = _row(created_ts=datetime.now(timezone.utc))
    watcher = MagicMock()
    watcher.has_order.return_value = False
    recovery = APStartupRecovery(
        client_id=CLIENT_ID,
        broker=MagicMock(),
        osm=MagicMock(client_id=CLIENT_ID),
        pm=None,
        master_control=types.SimpleNamespace(mode="PAPER"),
        entry_watcher=watcher,
    )
    monkeypatch.setattr(db, "conn", lambda: _RecoveryConn([row]))
    monkeypatch.setattr(db, "run_with_retry", lambda fn, *args, **kwargs: fn())

    recovery._reseed_watchers({"watchers_requeued": 0})

    watcher.watch.assert_not_called()


def test_startup_phantom_cleanup_excludes_submit_intent_before_reconciliation(monkeypatch):
    import client_runner

    sql_calls = []
    sql_params = []

    class Cursor:
        rowcount = 0

        def execute(self, sql, _params=None):
            sql_calls.append(" ".join(str(sql).split()))
            sql_params.append(_params)
            return self

        def fetchone(self):
            return (0, 0)

    @contextmanager
    def fake_conn():
        yield Cursor()

    monkeypatch.setattr(db, "conn", fake_conn)
    monkeypatch.setattr(client_runner, "run_with_retry", lambda fn, *args, **kwargs: fn())

    runner = object.__new__(client_runner.ClientRunner)
    runner.email = CLIENT_ID
    runner.mode = "PAPER"
    runner._runner_startup_ts = 0
    runner._clear_old_phantom_orders()

    candidate_sql = sql_calls[0]
    assert "NULLIF(BTRIM(COALESCE(o.meta->>'submit_intent_at','')), '') IS NULL" in candidate_sql
    write_guard = (
        "o.status IN ('CREATED','PENDING_TRIGGER','PENDING','DEFERRED')"
        " AND o.submitted_ts IS NULL"
        " AND NULLIF(BTRIM(COALESCE(o.meta->>'submit_intent_at','')), '') IS NULL"
        " AND o.filled_ts IS NULL"
        " AND o.position_id IS NULL"
    )
    for target in ("cancel_targets AS (", "skip_targets AS ("):
        assert target in candidate_sql
        target_sql = candidate_sql.split(target, 1)[1].split("RETURNING", 1)[0]
        assert write_guard in target_sql
    assert "v3_submit_intent_cas" in sql_params[0]
    assert (
        "'N/A','NA','NONE','NULL','PENDING','UNKNOWN','ERROR','0','FALSE'"
        in candidate_sql
    )


def test_startup_watcher_reseed_sql_holds_submit_evidence_before_queue_reset(monkeypatch):
    sql_calls = []

    class Cursor:
        rowcount = 0

        def execute(self, sql, _params=None):
            sql_calls.append(" ".join(str(sql).split()))
            return self

        def fetchall(self):
            return []

    @contextmanager
    def fake_conn():
        yield Cursor()

    monkeypatch.setattr(db, "conn", fake_conn)
    monkeypatch.setattr(db, "run_with_retry", lambda fn, *args, **kwargs: fn())

    recovery = APStartupRecovery(
        client_id=CLIENT_ID,
        broker=MagicMock(),
        osm=MagicMock(client_id=CLIENT_ID),
        pm=None,
        master_control=types.SimpleNamespace(mode="PAPER"),
        entry_watcher=MagicMock(),
    )
    recovery._reseed_watchers({"watchers_requeued": 0})

    reset_sql, orphan_sql = sql_calls
    for sql in (reset_sql, orphan_sql):
        assert "submit_intent_at" in sql
        assert "submitted_ts IS NOT NULL" in sql
    assert "NULLIF(BTRIM(COALESCE(o.broker_order_id, '')), '') IS NULL" in reset_sql
    assert "o.broker_order_id" in orphan_sql
    assert "o.submitted_ts" in orphan_sql


def test_order_monitor_holds_submit_intent_before_hydration_or_rearm():
    row = _row(created_ts=datetime.now(timezone.utc) - timedelta(days=4))
    monitor = object.__new__(APOrderMonitor)
    monitor.client_id = CLIENT_ID
    monitor._get_active_entry_orders = lambda: [row]
    monitor._parse_ts = lambda raw: raw if isinstance(raw, datetime) else None
    monitor._maybe_hydrate_deferred_order = MagicMock()
    monitor._check_pending_trigger_order = MagicMock()

    monitor._check_entry_orders()

    monitor._maybe_hydrate_deferred_order.assert_not_called()
    monitor._check_pending_trigger_order.assert_not_called()


def test_adoption_timestamp_prevents_created_ts_stale_entry_cancel(monkeypatch):
    adoption_created = datetime.now(timezone.utc) - timedelta(days=1)
    row = _row(
        created_ts=adoption_created,
    )
    core, broker, osm = _core(row, [_remote()])

    def persist_transition(_local_id, status, **kwargs):
        row["status"] = status
        row["broker_order_id"] = kwargs.get("broker_order_id")
        row["submitted_ts"] = kwargs.get("submitted_ts")
        return True

    osm.transition.side_effect = persist_transition
    result = core.reconcile_deferred_broker_intent(local_order_id=LOCAL_ORDER_ID)
    assert result["reason_code"] == "BROKER_ORDER_ADOPTED"
    assert row["submitted_ts"]

    monitor = object.__new__(APOrderMonitor)
    monitor.client_id = CLIENT_ID
    monitor._get_active_entry_orders = lambda: [row]

    def parse_ts(raw):
        if isinstance(raw, datetime):
            return raw
        if isinstance(raw, str):
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return None

    monitor._parse_ts = parse_ts
    monitor._check_stale_entry_cancel = MagicMock(return_value=False)
    monitor._check_entry_orders()

    age_secs = monitor._check_stale_entry_cancel.call_args.args[4]
    assert 0 <= age_secs < 5
    _assert_no_broker_mutations(broker)


def test_reconciler_routes_evidence_row_and_never_missing_id_cleanup(monkeypatch):
    row = _row()
    execution_core = MagicMock()
    execution_core.reconcile_deferred_broker_intent.return_value = {
        "disposition": "RECONCILE_PENDING",
        "reason_code": "RECONCILE_BROKER_NO_MATCH_HELD",
    }
    broker = MagicMock()
    monkeypatch.setattr(APBrokerReconciler, "_register_health", lambda self: None)
    rec = APBrokerReconciler(
        broker=broker,
        client_id=CLIENT_ID,
        osm=MagicMock(),
        pm=MagicMock(),
        execution_mode="paper",
        execution_core=execution_core,
    )
    rec._check_ghost_fills = MagicMock()
    rec._handle_order_without_broker_id = MagicMock()

    fake_db = types.ModuleType("ap.db")
    fake_db.get_open_orders_with_invalid_execution_mode = lambda **_: []
    fake_db.get_open_orders_for_reconcile = lambda **_: [row]
    fake_db.run_with_retry = lambda fn, *args, **kwargs: fn()
    monkeypatch.setitem(sys.modules, "ap.db", fake_db)

    summary = _empty_summary(CLIENT_ID)
    rec._reconcile_orders(summary)

    execution_core.reconcile_deferred_broker_intent.assert_called_once_with(
        local_order_id=LOCAL_ORDER_ID
    )
    rec._handle_order_without_broker_id.assert_not_called()
    broker.get_order.assert_not_called()
    assert summary["orders_alerted"] == 1


def test_reconciler_preserves_core_exception_reason(monkeypatch):
    row = _row()
    execution_core = MagicMock()
    execution_core.reconcile_deferred_broker_intent.side_effect = RuntimeError("boom")
    broker = MagicMock()
    monkeypatch.setattr(APBrokerReconciler, "_register_health", lambda self: None)
    rec = APBrokerReconciler(
        broker=broker,
        client_id=CLIENT_ID,
        osm=MagicMock(),
        pm=MagicMock(),
        execution_mode="paper",
        execution_core=execution_core,
    )
    rec._check_ghost_fills = MagicMock()
    rec._handle_order_without_broker_id = MagicMock()

    fake_db = types.ModuleType("ap.db")
    fake_db.get_open_orders_with_invalid_execution_mode = lambda **_: []
    fake_db.get_open_orders_for_reconcile = lambda **_: [row]
    fake_db.run_with_retry = lambda fn, *args, **kwargs: fn()
    monkeypatch.setitem(sys.modules, "ap.db", fake_db)

    summary = _empty_summary(CLIENT_ID)
    rec._reconcile_orders(summary)

    assert summary["errors"] == [
        "pending_trigger_submit_intent_hold:RECONCILE_CORE_CALL_FAILED:RuntimeError"
    ]


def test_reconciler_without_core_holds_evidence_row(monkeypatch):
    monkeypatch.setattr(APBrokerReconciler, "_register_health", lambda self: None)
    rec = APBrokerReconciler(
        broker=MagicMock(),
        client_id=CLIENT_ID,
        osm=MagicMock(),
        pm=MagicMock(),
        execution_mode="paper",
        execution_core=None,
    )
    rec._check_ghost_fills = MagicMock()
    rec._handle_order_without_broker_id = MagicMock()
    fake_db = types.ModuleType("ap.db")
    fake_db.get_open_orders_with_invalid_execution_mode = lambda **_: []
    fake_db.get_open_orders_for_reconcile = lambda **_: [_row()]
    fake_db.run_with_retry = lambda fn, *args, **kwargs: fn()
    monkeypatch.setitem(sys.modules, "ap.db", fake_db)

    summary = _empty_summary(CLIENT_ID)
    rec._reconcile_orders(summary)

    rec._handle_order_without_broker_id.assert_not_called()
    assert summary["errors"] == [
        "pending_trigger_submit_intent_hold:RECONCILE_EXECUTION_CORE_UNAVAILABLE"
    ]


def test_reconcile_query_keeps_watcher_rows_out_and_fences_client_mode(monkeypatch):
    calls = []

    class Cursor:
        def execute(self, sql, params):
            calls.append((" ".join(str(sql).split()), params))
            return self

        def fetchall(self):
            return []

    @contextmanager
    def fake_conn():
        yield Cursor()

    monkeypatch.setattr(db, "conn", fake_conn)
    monkeypatch.setattr(db, "run_with_retry", lambda fn, *args, **kwargs: fn())

    assert db.get_open_orders_for_reconcile(
        client_id=CLIENT_ID,
        execution_mode="paper",
    ) == []

    sql, params = calls[-1]
    assert params == (CLIENT_ID, "paper", 200)
    assert "'CREATED','SUBMITTED','ACKNOWLEDGED','PARTIAL_FILL'" in sql
    assert "status = 'PENDING_TRIGGER'" in sql
    assert "AND kind = 'ENTRY'" in sql
    assert "submitted_ts IS NOT NULL" in sql
    assert "NULLIF(BTRIM(COALESCE(meta->>'submit_intent_at','')), '') IS NOT NULL" in sql
    assert "LOWER(TRIM(COALESCE(execution_mode,'')))=%s" in sql

    assert db.get_open_orders_for_reconcile() == []
    sql, params = calls[-1]
    assert params == (200,)
    assert "WHERE 1=1 AND (" in sql
    assert "WHERE AND" not in sql


def test_client_runner_passes_the_existing_core_instance(monkeypatch):
    import client_runner

    captured = {}

    class FakeReconciler:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def start(self):
            return None

    monkeypatch.setattr(client_runner, "APBrokerReconciler", FakeReconciler)
    monkeypatch.setattr(client_runner, "ENABLE_BROKER_RECONCILER", True)
    runner = object.__new__(client_runner.ClientRunner)
    runner.email = CLIENT_ID
    runner.mode = "PAPER"
    runner.core = object()
    runner.order_state_machine = object()
    runner.position_manager = object()
    runner.supabase = None
    runner._start_reconciler(MagicMock(), MagicMock())

    assert captured["execution_core"] is runner.core


def test_tradier_list_orders_requests_tags_and_full_session_limit():
    from ap.brokers.tradier import TradierBroker

    broker = object.__new__(TradierBroker)
    broker.cfg = types.SimpleNamespace(account_id="ACCT-1")
    broker._get = MagicMock(
        return_value={"orders": {"order": {"id": "TR-1", "tag": TAG}}}
    )

    assert broker.list_orders() == [{"id": "TR-1", "tag": TAG}]
    broker._get.assert_called_once_with(
        "/v1/accounts/ACCT-1/orders",
        params={"includeTags": "true", "limit": 1500},
    )
