"""P0 tests for Amendment §6: broker-ambiguity reconciler (crash window).

The submit path persists submit_intent_at + the Tradier idempotency tag
BEFORE any broker bytes leave the process. If the process crashes after
the broker accepted the order but before broker_order_id was committed,
the durable row shows submit_intent_at present and broker_order_id absent
while a LIVE order may exist at the broker.

Invariants proven here:
  * such a crash-window row is NEVER resumed toward resubmission (the §2
    path) and NEVER terminalized — it is routed to the fail-closed
    reconciler and retained with durable ownership
  * the reconciler never calls the broker and never mutates the row
  * ALREADY_RECONCILED when a broker_order_id is present
  * NOT_IN_CRASH_WINDOW when there is no submit_intent_at (safe to resume)
  * RECONCILE_PENDING (reason RECONCILE_BROKER_QUERY_NOT_YET_WIRED) in the
    crash window until the broker-query adoption gate is wired
  * inspection failures collapse to KEEP_WATCHER (never a resume)
"""

from __future__ import annotations

import types
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

import ap_execution_core
from ap_recovery import APStartupRecovery


# ─────────────────────────── Reconciler subject ─────────────────────────


def _make_core(client_id="jason@example.com"):
    core = types.SimpleNamespace()
    core.client_id = client_id
    core.email = client_id
    core.order_state_machine = MagicMock()
    core.broker = MagicMock()
    core.broker.list_orders.side_effect = TimeoutError("broker unavailable")
    core.reconcile_deferred_broker_intent = (
        ap_execution_core.APExecutionCore.reconcile_deferred_broker_intent.__get__(
            core, type(core)
        )
    )
    return core


def _row(**overrides):
    base = {
        "local_order_id": "oid-1",
        "client_id": "jason@example.com",
        "kind": "ENTRY",
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "meta": {
            "lifecycle_state": "SUBMITTING",
            "submit_intent_at": datetime.now(timezone.utc).isoformat(),
            "broker_submit_key": "oid-1",
            "broker_submit_payload_hash": "abc123",
        },
    }
    base.update(overrides)
    return base


# ═══════════════════════════════════════════════════════════════════════
# Reconciler classification
# ═══════════════════════════════════════════════════════════════════════


def test_crash_window_query_failure_returns_reconcile_pending():
    core = _make_core()
    core.order_state_machine.get_order.return_value = _row()
    result = core.reconcile_deferred_broker_intent(local_order_id="oid-1")
    assert result["disposition"] == "RECONCILE_PENDING"
    assert result["reason_code"] == "RECONCILE_BROKER_QUERY_FAILED:TimeoutError"
    assert result["broker_submit_key"] == "oid-1"


def test_broker_order_present_returns_already_reconciled():
    core = _make_core()
    core.order_state_machine.get_order.return_value = _row(broker_order_id="TR-99")
    result = core.reconcile_deferred_broker_intent(local_order_id="oid-1")
    assert result["disposition"] == "ALREADY_RECONCILED"
    assert result["broker_order_id"] == "TR-99"


def test_no_submit_intent_returns_not_in_crash_window():
    core = _make_core()
    row = _row()
    row["meta"]["submit_intent_at"] = None
    core.order_state_machine.get_order.return_value = row
    result = core.reconcile_deferred_broker_intent(local_order_id="oid-1")
    assert result["disposition"] == "NOT_IN_CRASH_WINDOW"
    assert result["reason_code"] == "RECONCILE_NO_SUBMIT_INTENT"


# ═══════════════════════════════════════════════════════════════════════
# Hard invariants: never broker, never mutate
# ═══════════════════════════════════════════════════════════════════════


def test_reconciler_queries_broker_but_never_posts():
    core = _make_core()
    broker = MagicMock()
    core.broker = broker
    core.order_state_machine.get_order.return_value = _row()
    core.reconcile_deferred_broker_intent(local_order_id="oid-1")
    assert broker.list_orders.called
    for m in ("place_order", "buy_option", "post"):
        assert not getattr(broker, m).called, f"reconciler touched broker.{m}"


def test_reconciler_never_mutates_row():
    core = _make_core()
    core.order_state_machine.get_order.return_value = _row()
    core.reconcile_deferred_broker_intent(local_order_id="oid-1")
    core.order_state_machine.update_order_meta.assert_not_called()
    core.order_state_machine.terminalize_deferred_breach.assert_not_called()
    core.order_state_machine.submit_existing_entry.assert_not_called()
    core.order_state_machine.transition.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════
# Inspection failures → KEEP_WATCHER (never resume)
# ═══════════════════════════════════════════════════════════════════════


def test_no_osm_returns_keep_watcher():
    core = _make_core()
    core.order_state_machine = None
    result = core.reconcile_deferred_broker_intent(local_order_id="oid-1")
    assert result["disposition"] == "KEEP_WATCHER"
    assert result["reason_code"] == "RECONCILE_OSM_UNAVAILABLE"


def test_row_read_raises_returns_keep_watcher():
    core = _make_core()
    core.order_state_machine.get_order.side_effect = RuntimeError("db down")
    result = core.reconcile_deferred_broker_intent(local_order_id="oid-1")
    assert result["disposition"] == "KEEP_WATCHER"
    assert result["reason_code"].startswith("RECONCILE_ROW_READ_ERROR")


def test_row_missing_returns_keep_watcher():
    core = _make_core()
    core.order_state_machine.get_order.return_value = None
    result = core.reconcile_deferred_broker_intent(local_order_id="oid-1")
    assert result["disposition"] == "KEEP_WATCHER"
    assert result["reason_code"] == "RECONCILE_ROW_MISSING"


def test_client_id_mismatch_returns_keep_watcher():
    core = _make_core(client_id="jason@example.com")
    core.order_state_machine.get_order.return_value = _row(client_id="other@example.com")
    result = core.reconcile_deferred_broker_intent(local_order_id="oid-1")
    assert result["disposition"] == "KEEP_WATCHER"
    assert result["reason_code"] == "RECONCILE_CLIENT_ID_MISMATCH"


# ═══════════════════════════════════════════════════════════════════════
# Recovery routing: crash-window rows never resubmitted
# ═══════════════════════════════════════════════════════════════════════


class _RecoveryCursor:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, *_a, **_k):
        return self

    def fetchall(self):
        return self.rows


class _RecoveryConn:
    def __init__(self, rows):
        self.cursor = _RecoveryCursor(rows)

    def __enter__(self):
        return self.cursor

    def __exit__(self, *_):
        return False


def _crash_window_recovery_row():
    meta = {
        "lifecycle_state": "SUBMITTING",
        "materialization_status": "RUNNING",
        "materialization_generation": 2,
        "broker_ready": True,
        "submit_intent_at": datetime.now(timezone.utc).isoformat(),
        "broker_submit_key": "oid-crash",
        "execution_mode": "paper",
    }
    return {
        "local_order_id": "oid-crash",
        "client_id": "client@example.com",
        "signal_id": "sig-crash",
        "plan_id": "plan-1",
        "symbol": "SPY",
        "contract": "SPY260717C00600000",
        "direction": "CALL",
        "score": 80,
        "tier": "A",
        "trigger_price": 500.0,
        "stop_underlying": 495.0,
        "target_underlying": 510.0,
        "pattern": "2-1-2",
        "timeframe": "1d",
        "execution_mode": "paper",
        "qty": 1,
        "limit_price": 1.25,
        "reserved_cost": 125.0,
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "submitted_ts": None,
        "created_ts": datetime.now(timezone.utc),
        "meta": meta,
    }


def _run(monkeypatch, rows, *, osm, entry_watcher, execution_core):
    import ap.db as ap_db
    monkeypatch.setattr(ap_db, "conn", lambda: _RecoveryConn(rows))
    monkeypatch.setattr(ap_db, "run_with_retry", lambda fn, *a, **k: fn())
    recovery = APStartupRecovery(
        client_id="client@example.com",
        broker=object(),
        osm=osm,
        pm=None,
        master_control=types.SimpleNamespace(mode="PAPER"),
        entry_watcher=entry_watcher,
        execution_core=execution_core,
    )
    result = {"deferred_lifecycles_recovered": 0}
    recovery._recover_deferred_breach_lifecycles(result)
    return result


def _osm():
    return types.SimpleNamespace(
        client_id="client@example.com",
        terminalize_deferred_breach=MagicMock(return_value=True),
        update_order_meta=MagicMock(return_value=True),
        submit_existing_entry=MagicMock(return_value={"ok": True}),
    )


def test_recovery_routes_crash_window_to_reconciler_never_resumes(monkeypatch):
    """A crash-window row must go to the reconciler, NOT the §2 resume
    scaffold, and must never reach submit_existing_entry. On
    RECONCILE_PENDING the row is retained with durable ownership."""
    osm = _osm()
    ec = types.SimpleNamespace(
        reconcile_deferred_broker_intent=MagicMock(return_value={
            "disposition": "RECONCILE_PENDING",
            "reason_code": "RECONCILE_BROKER_QUERY_NOT_YET_WIRED",
        }),
        resume_deferred_broker_ready_order=MagicMock(return_value={
            "disposition": "RETRY_WAIT", "reason_code": "x",
        }),
    )
    watcher = types.SimpleNamespace(
        has_order=lambda _oid: False, watch=MagicMock(return_value=True),
    )
    _run(monkeypatch, [_crash_window_recovery_row()],
         osm=osm, entry_watcher=watcher, execution_core=ec)

    ec.reconcile_deferred_broker_intent.assert_called_once()
    # NEVER resumed toward resubmission
    ec.resume_deferred_broker_ready_order.assert_not_called()
    osm.submit_existing_entry.assert_not_called()
    # NEVER terminalized (a live order may exist)
    osm.terminalize_deferred_breach.assert_not_called()
    # Retained with durable ownership
    osm.update_order_meta.assert_called_once()
    _, patch = osm.update_order_meta.call_args.args
    assert patch["recovery_retention_reason"] == "RECONCILE_BROKER_QUERY_NOT_YET_WIRED"


def test_recovery_crash_window_without_execution_core_retains(monkeypatch):
    """Without a reconciler wired, a crash-window row is retained, never
    resumed, never terminalized."""
    osm = _osm()
    watcher = types.SimpleNamespace(
        has_order=lambda _oid: False, watch=MagicMock(return_value=True),
    )
    _run(monkeypatch, [_crash_window_recovery_row()],
         osm=osm, entry_watcher=watcher, execution_core=None)
    osm.submit_existing_entry.assert_not_called()
    osm.terminalize_deferred_breach.assert_not_called()
    osm.update_order_meta.assert_called_once()
    _, patch = osm.update_order_meta.call_args.args
    assert patch["recovery_retention_reason"] == "crash_window_reconciler_unavailable"


def test_recovery_not_in_crash_window_falls_through_to_resume(monkeypatch):
    """A row the reconciler classifies NOT_IN_CRASH_WINDOW proceeds to the
    normal §2 resume path."""
    osm = _osm()
    ec = types.SimpleNamespace(
        reconcile_deferred_broker_intent=MagicMock(return_value={
            "disposition": "NOT_IN_CRASH_WINDOW",
            "reason_code": "RECONCILE_NO_SUBMIT_INTENT",
        }),
        resume_deferred_broker_ready_order=MagicMock(return_value={
            "disposition": "RETRY_WAIT",
            "reason_code": "RECOVERY_SUBMIT_GATES_NOT_YET_WIRED",
            "attempt": 1, "max_attempts": 20,
            "owner": "recovery_scheduler:client@example.com",
            "generation": 2, "next_retry_at": None,
        }),
    )
    watcher = types.SimpleNamespace(
        has_order=lambda _oid: False, watch=MagicMock(return_value=True),
    )
    _run(monkeypatch, [_crash_window_recovery_row()],
         osm=osm, entry_watcher=watcher, execution_core=ec)
    ec.reconcile_deferred_broker_intent.assert_called_once()
    # Fell through to the §2 resume path
    ec.resume_deferred_broker_ready_order.assert_called_once()
    osm.submit_existing_entry.assert_not_called()
