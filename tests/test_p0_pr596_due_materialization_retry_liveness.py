"""Fail-first coverage for PR #596's due canonical retry liveness seam."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")
os.environ.setdefault("SCHEMA_ATTESTATION_ENABLED", "0")


CLIENT_ID = "jason@example.com"
LOCAL_ORDER_ID = "oid-pr596-liveness"


@pytest.fixture(autouse=True)
def _open_retry_cutoff(monkeypatch):
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_CUTOFF_ET", "2359")


def _row(*, due: bool = True) -> dict:
    now = datetime.now(timezone.utc)
    retry_at = now - timedelta(minutes=1) if due else now + timedelta(minutes=5)
    return {
        "local_order_id": LOCAL_ORDER_ID,
        "client_id": CLIENT_ID,
        "execution_mode": "live",
        "signal_id": "sig-pr596-liveness",
        "plan_id": "plan-pr596-liveness",
        "kind": "ENTRY",
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "submitted_ts": None,
        "symbol": "MO",
        "direction": "CALL",
        "contract": "DEFERRED:MO",
        "trigger_price": 300.0,
        "created_ts": now - timedelta(minutes=10),
        "meta": {
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
            "materialization_outcome": "RETRY_LATER_DATA_UNAVAILABLE",
            "materialization_generation": 1,
            "retry_attempt": 1,
            "breach_attempt_count": 1,
            "materialization_attempts": 1,
            "retry_max_attempts": 3,
            "materialization_next_retry_at": retry_at.isoformat(),
            "materialization_last_failure_at": (
                now - timedelta(minutes=2)
            ).isoformat(),
            "materialization_reason": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
            "materialization_selector_failure": {
                "reason_code": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
            },
            "broker_ready": False,
            "materialization_in_flight": False,
            "trigger_crossed_at": (now - timedelta(minutes=3)).isoformat(),
            "absolute_entry_deadline": (now + timedelta(hours=1)).isoformat(),
            # This is the production shape that was stranded: the watcher
            # recorded trigger_ready, but the legacy provenance stamp is absent.
            "watcher_audit": {"reason_code": "trigger_ready"},
        },
    }


def _db_rows(monkeypatch, rows):
    from ap import db as db_mod

    class _Cursor:
        rowcount = len(rows)

        def execute(self, *args, **kwargs):
            return self

        def fetchall(self):
            return rows

    class _Conn:
        def __enter__(self):
            return _Cursor()

        def __exit__(self, *args):
            return False

    monkeypatch.setattr(db_mod, "conn", lambda: _Conn())
    monkeypatch.setattr(db_mod, "run_with_retry", lambda fn, *a, **kw: fn())


@pytest.mark.parametrize("due", [False, True])
def test_trigger_ready_canonical_retry_is_waiting_retryable(due):
    from ap.pending_trigger_classifier import (
        PendingTriggerClassification,
        classify_pending_trigger_row,
    )

    assert classify_pending_trigger_row(_row(due=due), watcher_owned=False) == (
        PendingTriggerClassification.WAITING_RETRYABLE
    )


@pytest.mark.parametrize(
    "mutation",
    [
        lambda meta: meta.update({"retry_attempt": 2}),
        lambda meta: meta.update({"submit_intent_at": "2026-09-08T20:00:00+00:00"}),
        lambda meta: meta.update({
            "trigger_crossed_at_provenance": {
                "canonical_signal_id": "wrong-signal",
                "client_id": CLIENT_ID,
                "execution_mode": "live",
                "local_order_id": LOCAL_ORDER_ID,
            }
        }),
    ],
    ids=["conflicting-attempt", "broker-handoff", "conflicting-provenance"],
)
def test_contradictory_retry_authority_stays_stuck(mutation):
    from ap.pending_trigger_classifier import (
        PendingTriggerClassification,
        classify_pending_trigger_row,
    )

    row = _row()
    mutation(row["meta"])
    assert classify_pending_trigger_row(row, watcher_owned=False) == (
        PendingTriggerClassification.STUCK_TRIGGER_READY
    )


def test_due_trigger_ready_canonical_retry_reaches_due_consumer(monkeypatch):
    from ap_recovery import APStartupRecovery

    row = _row(due=True)
    core = MagicMock()
    core.resume_deferred_materialization_retry.return_value = {
        "disposition": "CLAIM_LOST",
        "reason_code": "RETRY_ALREADY_CLAIMED",
    }
    recovery = APStartupRecovery(
        client_id=CLIENT_ID,
        broker=MagicMock(),
        osm=SimpleNamespace(client_id=CLIENT_ID),
        pm=MagicMock(),
        master_control=SimpleNamespace(mode="LIVE"),
        entry_watcher=None,
        execution_core=core,
    )
    _db_rows(monkeypatch, [row])

    result = {"deferred_lifecycles_recovered": 0, "errors": []}
    recovery._recover_deferred_breach_lifecycles(result)

    core.resume_deferred_materialization_retry.assert_called_once_with(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=2,
        owner=f"recovery_retry:{CLIENT_ID}:{LOCAL_ORDER_ID}:2",
    )


def test_due_retry_deadline_after_claim_terminalizes_post_claim_once(monkeypatch):
    """A post-claim deadline decision must clear the active claim exactly once."""
    import ap_execution_core as core_mod
    from ap_recovery import APStartupRecovery
    from ap_execution_core import APExecutionCore

    row = _row(due=True)
    row.update({
        "score": 78.0,
        "tier": "B",
        "timeframe": "1d",
        "stop_underlying": 298.0,
        "target_underlying": 304.0,
    })
    start = datetime.now(timezone.utc)
    row["meta"]["absolute_entry_deadline"] = (
        start + timedelta(seconds=6)
    ).isoformat()
    owner = f"recovery_retry:{CLIENT_ID}:{LOCAL_ORDER_ID}:2"
    state = {"row": row, "terminal_calls": []}

    class _OSM:
        client_id = CLIENT_ID

        def get_order(self, local_order_id):
            assert local_order_id == LOCAL_ORDER_ID
            return state["row"]

        def claim_deferred_materialization(self, local_order_id, **kwargs):
            assert local_order_id == LOCAL_ORDER_ID
            assert kwargs["owner"] == owner
            assert kwargs["new_generation"] == 2
            assert kwargs["retry_attempt"] == 1
            meta = state["row"]["meta"]
            meta.update({
                "lifecycle_state": "MATERIALIZING",
                "materialization_status": "RUNNING",
                "materialization_in_flight": True,
                "materialization_owner": owner,
                "current_owner": owner,
                "watcher_token": owner,
                "materialization_generation": 2,
                "materialization_market_truth_pending": True,
            })
            return True

        def terminalize_materialization_retry(self, local_order_id, **kwargs):
            assert local_order_id == LOCAL_ORDER_ID
            state["terminal_calls"].append(dict(kwargs))
            assert kwargs["reason"] == "RETRY_DEADLINE_EXHAUSTED"
            assert kwargs["owner"] == owner
            assert kwargs["generation"] == 2
            assert kwargs["retry_attempt"] == 1
            assert kwargs["client_id"] == CLIENT_ID
            assert kwargs["execution_mode"] == "live"
            state["row"]["status"] = kwargs["terminal_status"]
            state["row"]["meta"].update({
                "lifecycle_state": kwargs["terminal_status"],
                "materialization_status": "FAILED_TERMINAL",
                "materialization_in_flight": False,
                "materialization_owner": "",
                "current_owner": "",
                "watcher_token": "",
            })
            return True

    osm = _OSM()
    core = SimpleNamespace(
        client_id=CLIENT_ID,
        email=CLIENT_ID,
        execution_mode="live",
        mode="LIVE",
        paper=False,
        order_state_machine=osm,
        broker=MagicMock(),
    )
    core.resume_deferred_materialization_retry = (
        APExecutionCore.resume_deferred_materialization_retry.__get__(
            core, type(core)
        )
    )
    core._on_entry_trigger = MagicMock(side_effect=RuntimeError("late callback"))

    real_datetime = datetime
    clock_calls = {"count": 0}

    class _SteppingDatetime(real_datetime):
        @classmethod
        def now(cls, tz=None):
            clock_calls["count"] += 1
            return start if clock_calls["count"] == 1 else start + timedelta(seconds=15)

    monkeypatch.setattr(core_mod, "datetime", _SteppingDatetime)
    _db_rows(monkeypatch, [row])
    recovery = APStartupRecovery(
        client_id=CLIENT_ID,
        broker=core.broker,
        osm=osm,
        pm=MagicMock(),
        master_control=SimpleNamespace(mode="LIVE"),
        entry_watcher=None,
        execution_core=core,
    )

    result = {"deferred_lifecycles_recovered": 0, "errors": []}
    recovery._recover_deferred_breach_lifecycles(result)

    assert clock_calls["count"] >= 2
    assert len(state["terminal_calls"]) == 1
    assert state["row"]["status"] == "EXPIRED"
    assert state["row"]["meta"]["lifecycle_state"] == "EXPIRED"
    assert state["row"]["meta"]["materialization_status"] == "FAILED_TERMINAL"
    assert state["row"]["meta"]["materialization_in_flight"] is False
    assert state["row"]["meta"]["materialization_owner"] == ""


def test_restart_recovery_accepts_the_same_canonical_retry_authority():
    from ap.pending_trigger_restart_recovery import (
        PendingTriggerRestartRecovery,
        _RowOutcome,
    )

    row = _row(due=False)

    class _OSM:
        client_id = CLIENT_ID

        def get_order(self, local_order_id):
            return row

        def update_order_meta(self, local_order_id, patch):
            row["meta"].update(patch)
            return True

    recovery = PendingTriggerRestartRecovery(
        client_id=CLIENT_ID,
        execution_mode="live",
        osm=_OSM(),
        broker=MagicMock(),
        quote_check_fn=lambda *args, **kwargs: pytest.fail(
            "canonical materialization retry must not request a quote"
        ),
    )

    assert recovery.recover_one_row(row) == _RowOutcome.RETRY_OWNED
