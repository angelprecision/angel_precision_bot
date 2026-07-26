from __future__ import annotations

import sys
import types
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import ap.db as db_module
import ap.queue as queue
import ap_execution_core as core_mod
import ap_handoff_run_lock as handoff_lock
import ap_recovery
from ap_reconciler import APBrokerReconciler, _empty_summary


def test_queue_dispatch_rejects_unknown_execution_mode_before_submit(monkeypatch):
    mark_calls = []
    rejection_logs = []
    mc = SimpleNamespace(
        mode="paper",
        evaluate=lambda *a, **k: pytest.fail("master control must not run"),
    )

    monkeypatch.setattr(queue, "_mark_job", lambda *a, **k: mark_calls.append((a, k)))
    monkeypatch.setattr(queue, "_log_rejection_to_db", lambda **k: rejection_logs.append(k))
    monkeypatch.setattr(queue, "ALLOW_IMMEDIATE_EXECUTION", False)

    queue._dispatch(
        1,
        "client@example.com",
        "sig-unknown-mode",
        {"ticker": "SPY", "side": "CALL", "score": 80, "execution_mode": "staging"},
        master_control=mc,
        contract_selector=None,
        order_state_machine=MagicMock(),
        entry_watcher=MagicMock(),
    )

    assert mark_calls
    assert mark_calls[0][1]["error"] == "metadata_invalid:unknown_execution_mode"
    assert rejection_logs[0]["reason_code"] == "metadata_invalid:unknown_execution_mode"


def test_queue_dispatch_rejects_payload_runtime_execution_mode_mismatch(monkeypatch):
    mark_calls = []
    rejection_logs = []
    mc = SimpleNamespace(
        mode="live",
        evaluate=lambda *a, **k: pytest.fail("master control must not run"),
    )

    monkeypatch.setattr(queue, "_mark_job", lambda *a, **k: mark_calls.append((a, k)))
    monkeypatch.setattr(queue, "_log_rejection_to_db", lambda **k: rejection_logs.append(k))
    monkeypatch.setattr(queue, "ALLOW_IMMEDIATE_EXECUTION", False)

    queue._dispatch(
        1,
        "client@example.com",
        "sig-mode-mismatch",
        {"ticker": "SPY", "side": "CALL", "score": 80, "execution_mode": "paper"},
        master_control=mc,
        contract_selector=None,
        order_state_machine=MagicMock(),
        entry_watcher=MagicMock(),
    )

    assert mark_calls
    assert mark_calls[0][1]["error"] == "metadata_invalid:execution_mode_mismatch"
    assert rejection_logs[0]["reason_code"] == "metadata_invalid:execution_mode_mismatch"


def test_queue_dispatch_stamps_missing_payload_mode_from_runtime_and_keeps_live_protections(monkeypatch):
    mark_calls = []
    payload = {"ticker": "SPY", "side": "CALL", "score": 80}
    mc = SimpleNamespace(
        mode="live",
        evaluate=lambda *a, **k: pytest.fail("master control must not run"),
    )

    monkeypatch.setattr(queue, "_mark_job", lambda *a, **k: mark_calls.append((a, k)))
    monkeypatch.setattr(queue, "ALLOW_IMMEDIATE_EXECUTION", True)

    queue._dispatch(
        1,
        "client@example.com",
        "sig-stamp-live",
        payload,
        master_control=mc,
        contract_selector=None,
        order_state_machine=MagicMock(),
        entry_watcher=MagicMock(),
    )

    assert payload["execution_mode"] == "live"
    assert mark_calls
    assert mark_calls[0][1]["error"] == "LIVE_FATAL_IMMEDIATE_EXECUTION_ENABLED"


def test_queue_dispatch_rejects_unknown_runtime_mode_before_submit(monkeypatch):
    mark_calls = []
    rejection_logs = []
    mc = SimpleNamespace(
        mode="staging",
        evaluate=lambda *a, **k: pytest.fail("master control must not run"),
    )

    monkeypatch.setattr(queue, "_mark_job", lambda *a, **k: mark_calls.append((a, k)))
    monkeypatch.setattr(queue, "_log_rejection_to_db", lambda **k: rejection_logs.append(k))
    monkeypatch.setattr(queue, "ALLOW_IMMEDIATE_EXECUTION", False)

    queue._dispatch(
        1,
        "client@example.com",
        "sig-runtime-unknown",
        {"ticker": "SPY", "side": "CALL", "score": 80, "execution_mode": "live"},
        master_control=mc,
        contract_selector=None,
        order_state_machine=MagicMock(),
        entry_watcher=MagicMock(),
    )

    assert mark_calls
    assert mark_calls[0][1]["error"] == "metadata_invalid:unknown_execution_mode"
    assert rejection_logs[0]["reason_code"] == "metadata_invalid:unknown_execution_mode"


@pytest.mark.parametrize("mode_value", ["paper", "live"])
def test_execution_core_accepts_approved_plan_mode_alias(monkeypatch, mode_value):
    plan = SimpleNamespace(
        contract_symbol="SPY260626C00500000",
        execution_price_per_share=1.25,
        ask=1.25,
        mid=1.20,
        affordable_contracts=1,
        premium_per_contract=125.0,
        contracts=1,
        limit_price=1.25,
        side="CALL",
        mode=mode_value,
        signal_id="sig-core-unknown-mode",
        metadata={"queue_id": 11},
    )
    watched = SimpleNamespace(
        signal={
            "ticker": "SPY",
            "side": "CALL",
            "entry_price": 600.0,
            "stop_price": 595.0,
            "target_price": 610.0,
            "signal_id": "sig-core-unknown-mode",
            "local_order_id": "local-core-1",
            "client_id": "client@example.com",
            "score": 80,
        },
        trigger_price=600.5,
        ticker="SPY",
    )
    core = core_mod.APExecutionCore.__new__(core_mod.APExecutionCore)
    core.paper = True
    core.mode = "PAPER"
    core.client_id = "client@example.com"
    core.client_email = "client@example.com"
    # PR #391: the final market-validity gate now fetches a fresh underlying
    # quote in PAPER as in LIVE. Give the broker a get_quote that returns a
    # quote consistent with the plan (CALL, trigger=600.5, stop=595, target=610)
    # so the gate passes and the mode-alias seam under test is exercised.
    core.broker = SimpleNamespace(
        cfg=SimpleNamespace(base_url="https://api.tradier.com"),
        get_quote=lambda _sym: {
            "bid": 600.60,
            "ask": 600.70,
            "quote_age_ms": 100,
            "source": "live_broker",
        },
    )
    core.store = MagicMock()
    core.order_state_machine = MagicMock()
    core.order_state_machine.client_id = "client@example.com"
    core.order_state_machine.execution_mode = "paper"
    durable_order = {
        "client_id": "client@example.com",
        "meta": {},
    }
    core.order_state_machine.get_order.side_effect = lambda _local_id: durable_order

    def persist_meta(_local_id, patch):
        durable_order["meta"].update(patch)
        return True

    core.order_state_machine.update_order_meta.side_effect = persist_meta
    core.order_state_machine.expire_pending_entry.return_value = True
    core.contract_selector = MagicMock()
    core._breach_risk_check = MagicMock(return_value=True)
    core._recover_plan_for_revalidation = MagicMock(return_value=plan)
    core._cleanup_pending_entry_order = types.MethodType(
        core_mod.APExecutionCore._cleanup_pending_entry_order,
        core,
    )

    fake_execution_mod = types.ModuleType("ap.execution")
    fake_execution_mod._refresh_ask_at_submit = lambda broker, contract: (
        1.25,
        5,
        True,
        "",
        {
            "submit_bid": 1.20,
            "submit_ask": 1.25,
            "submit_last": 1.23,
            "submit_mid": 1.225,
            "spread_pct": 0.04,
        },
    )
    monkeypatch.setitem(sys.modules, "ap.execution", fake_execution_mod)
    core.order_state_machine.submit_existing_entry.return_value = {"ok": False, "error": "submit_failed"}

    core_mod.APExecutionCore._on_entry_trigger(core, watched)

    assert core.order_state_machine.submit_existing_entry.call_count == 1, (
        core.order_state_machine.mock_calls
    )
    core.order_state_machine.expire_pending_entry.assert_not_called()


def test_execution_core_blocks_submit_on_unknown_execution_mode(monkeypatch):
    plan = SimpleNamespace(
        contract_symbol="SPY260626C00500000",
        execution_price_per_share=1.25,
        ask=1.25,
        mid=1.20,
        affordable_contracts=1,
        premium_per_contract=125.0,
        contracts=1,
        limit_price=1.25,
        side="CALL",
        execution_mode="staging",
        signal_id="sig-core-unknown-mode",
        metadata={"queue_id": 11},
    )
    watched = SimpleNamespace(
        signal={
            "ticker": "SPY",
            "side": "CALL",
            "entry_price": 600.0,
            "stop_price": 595.0,
            "target_price": 610.0,
            "signal_id": "sig-core-unknown-mode",
            "local_order_id": "local-core-1",
            "client_id": "client@example.com",
            "score": 80,
        },
        trigger_price=600.5,
        ticker="SPY",
    )
    core = core_mod.APExecutionCore.__new__(core_mod.APExecutionCore)
    core.paper = True
    core.mode = "PAPER"
    core.client_id = "client@example.com"
    core.client_email = "client@example.com"
    core.broker = SimpleNamespace(cfg=SimpleNamespace(base_url="https://api.tradier.com"))
    core.store = MagicMock()
    core.order_state_machine = MagicMock()
    core.order_state_machine.expire_pending_entry.return_value = True
    core.contract_selector = MagicMock()
    core._breach_risk_check = MagicMock(return_value=True)
    core._recover_plan_for_revalidation = MagicMock(return_value=plan)
    core._cleanup_pending_entry_order = types.MethodType(
        core_mod.APExecutionCore._cleanup_pending_entry_order,
        core,
    )

    fake_execution_mod = types.ModuleType("ap.execution")
    fake_execution_mod._refresh_ask_at_submit = lambda broker, contract: (
        1.25,
        5,
        True,
        "",
        {
            "submit_bid": 1.20,
            "submit_ask": 1.25,
            "submit_last": 1.23,
            "submit_mid": 1.225,
            "spread_pct": 0.04,
        },
    )
    monkeypatch.setitem(sys.modules, "ap.execution", fake_execution_mod)

    core_mod.APExecutionCore._on_entry_trigger(core, watched)

    core.order_state_machine.submit_existing_entry.assert_not_called()
    core.order_state_machine.expire_pending_entry.assert_called_once_with(
        "local-core-1",
        reason="metadata_invalid:unknown_execution_mode",
    )


def _fake_reconciler_module():
    mod = types.ModuleType("ap_reconciler")
    mod.BROKER_FILLED = frozenset({"filled", "partially_filled"})
    mod.BROKER_TERMINAL = frozenset({"canceled", "cancelled", "rejected", "expired"})
    mod.BROKER_TO_OSM = {
        "filled": "FILLED",
        "partially_filled": "PARTIAL_FILL",
        "canceled": "CANCELED",
        "cancelled": "CANCELED",
        "rejected": "REJECTED",
        "expired": "EXPIRED",
    }
    return mod


def test_recovery_unknown_mode_skips_order_mutation():
    rec = ap_recovery.APStartupRecovery(
        client_id="client@example.com",
        broker=MagicMock(),
        osm=MagicMock(),
        pm=MagicMock(),
        master_control=SimpleNamespace(mode="staging"),
    )
    result = {"errors": [], "entries_corrected": 0}

    rec._verify_pending_entries(result)

    rec.broker.get_order.assert_not_called()
    rec.osm.transition.assert_not_called()
    assert "recovery_unknown_execution_mode" in result["errors"]


def test_reconciler_unknown_mode_skips_terminal_correction(monkeypatch):
    broker = MagicMock()
    def _boom(*args, **kwargs):
        pytest.fail("broker get_order must not run")
    broker.get_order.side_effect = _boom
    rec = APBrokerReconciler(
        broker=broker,
        client_id="client@example.com",
        osm=MagicMock(),
        pm=MagicMock(),
        execution_mode="live",
    )
    summary = _empty_summary("client@example.com")
    fake_db = types.ModuleType("ap.db")
    fake_db.get_open_orders_for_reconcile = lambda client_id=None, execution_mode=None: [
        {
            "local_order_id": "local-rec-1",
            "broker_order_id": "broker-rec-1",
            "status": "ACKNOWLEDGED",
            "kind": "ENTRY",
            "contract": "SPY260626C00500000",
            "execution_mode": "staging",
        }
    ]
    fake_db.get_open_orders_with_invalid_execution_mode = lambda client_id=None: []
    fake_db.run_with_retry = lambda fn, *a, **k: fn()

    @contextmanager
    def fake_conn():
        class _Conn:
            rowcount = 1

            def execute(self, *args, **kwargs):
                return None

        yield _Conn()

    fake_db.conn = fake_conn

    monkeypatch.setitem(sys.modules, "ap.db", fake_db)
    rec._reconcile_orders(summary)

    rec.osm.transition.assert_not_called()
    assert "reconciler_unknown_execution_mode" in summary["errors"]


def test_paper_reconciler_queries_paper_only_and_rejects_returned_live_row(monkeypatch):
    """Defense in depth: a bad DB result must not cross the mode boundary."""
    queried = []
    broker = MagicMock()
    osm = MagicMock()
    rec = APBrokerReconciler(
        broker=broker,
        client_id="client@example.com",
        osm=osm,
        pm=MagicMock(),
        execution_mode="paper",
    )
    rec._write_order_last_error = MagicMock()
    rec._check_ghost_fills = MagicMock()
    summary = _empty_summary("client@example.com")

    fake_db = types.ModuleType("ap.db")

    def get_open_orders_for_reconcile(*, client_id=None, execution_mode=None):
        queried.append((client_id, execution_mode))
        # Simulate a broken repository filter returning a LIVE row to a PAPER
        # reconciler. The reconciler's row-level fence must still stop it.
        return [
            {
                "local_order_id": "local-live-returned-to-paper",
                "broker_order_id": "broker-live-1",
                "status": "ACKNOWLEDGED",
                "kind": "ENTRY",
                "contract": "SPY260626C00500000",
                "execution_mode": "live",
                "last_error": "SPLIT_BRAIN:broker_identity_unknown",
                "meta": {"split_brain_quarantine": True},
            }
        ]

    fake_db.get_open_orders_for_reconcile = get_open_orders_for_reconcile
    fake_db.get_open_orders_with_invalid_execution_mode = lambda client_id=None: []
    fake_db.run_with_retry = lambda fn, *args, **kwargs: fn()
    monkeypatch.setitem(sys.modules, "ap.db", fake_db)

    rec._reconcile_orders(summary)

    assert queried == [("client@example.com", "paper")]
    rec._write_order_last_error.assert_not_called()
    assert "reconciler_execution_mode_mismatch" in summary["errors"]
    broker.get_order.assert_not_called()
    osm.resolve_split_brain_quarantine.assert_not_called()
    osm.transition.assert_not_called()


def test_reconciler_audits_real_invalid_mode_query_without_broker_or_osm(monkeypatch):
    """NULL/malformed modes are observable but never assigned to LIVE/PAPER."""
    queries = []
    invalid_rows = [
        {
            "local_order_id": "local-null-mode",
            "broker_order_id": "broker-null-mode",
            "status": "SUBMITTED",
            "kind": "ENTRY",
            "contract": "SPY260626C00500000",
            "execution_mode": None,
        },
        {
            "local_order_id": "local-staging-mode",
            "broker_order_id": None,
            "status": "PENDING_TRIGGER",
            "kind": "ENTRY",
            "contract": "QQQ260626P00450000",
            "execution_mode": "staging",
        },
    ]

    class _Conn:
        def __init__(self):
            self._rows = []

        def execute(self, sql, params):
            normalized = " ".join(str(sql).split())
            queries.append((normalized, params))
            self._rows = (
                invalid_rows
                if "NOT IN ('live','paper')" in normalized
                else []
            )
            return self

        def fetchall(self):
            return list(self._rows)

    @contextmanager
    def fake_conn():
        yield _Conn()

    monkeypatch.setattr(db_module, "conn", fake_conn)
    monkeypatch.setattr(APBrokerReconciler, "_register_health", lambda self: None)

    broker = MagicMock()
    osm = MagicMock()
    rec = APBrokerReconciler(
        broker=broker,
        client_id="client@example.com",
        osm=osm,
        pm=MagicMock(),
        execution_mode="live",
    )
    rec._write_order_last_error = MagicMock()
    rec._check_ghost_fills = MagicMock()
    summary = _empty_summary("client@example.com")

    rec._reconcile_orders(summary)

    invalid_query = next(q for q in queries if "NOT IN ('live','paper')" in q[0])
    assert "'PENDING_TRIGGER'" in invalid_query[0]
    assert invalid_query[1] == ("client@example.com", 200)
    assert any(
        "LOWER(TRIM(COALESCE(execution_mode,'')))=%s" in sql
        and params == ("client@example.com", "live", 200)
        for sql, params in queries
    )
    assert summary["orders_invalid_execution_mode"] == 2
    assert summary["orders_checked"] == 0
    assert summary["errors"].count("reconciler_unknown_execution_mode") == 2
    assert summary["orders_alerted"] == 2
    assert rec._write_order_last_error.call_args_list == [
        (("local-null-mode", "reconciler_unknown_execution_mode"),),
        (("local-staging-mode", "reconciler_unknown_execution_mode"),),
    ]
    broker.get_order.assert_not_called()
    osm.transition.assert_not_called()


def test_handoff_unknown_mode_rejected_with_reason():
    result = handoff_lock.try_acquire_run_lock(
        run_key="morning_job:2026-07-03:x:staging:abc",
        job_name="x",
        execution_mode="staging",
        client_scope="client@example.com",
    )

    assert result["acquired"] is False
    assert result["reason"] == "metadata_invalid:unknown_execution_mode"


@pytest.mark.parametrize("mode", ["paper", "live"])
def test_handoff_paper_and_live_modes_still_pass(mode):
    class _Conn:
        rowcount = 0

        def execute(self, sql, params=None):
            if sql.strip().upper().startswith("INSERT"):
                self.rowcount = 1
            else:
                self.rowcount = 0

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    fake_db = types.ModuleType("ap.db")
    fake_db.conn = lambda: _Conn()
    fake_db.run_with_retry = lambda fn, *a, **k: fn()

    with pytest.MonkeyPatch.context() as mp:
        mp.setitem(sys.modules, "ap.db", fake_db)
        result = handoff_lock.try_acquire_run_lock(
            run_key=f"morning_job:2026-07-03:x:{mode}:abc",
            job_name="x",
            execution_mode=mode,
            client_scope="client@example.com",
        )

    assert result["acquired"] is True
