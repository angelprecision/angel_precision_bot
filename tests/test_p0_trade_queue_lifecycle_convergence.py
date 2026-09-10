"""Focused P0 coverage for terminal ENTRY -> trade_queue convergence."""
from __future__ import annotations

import copy
import json
import os
import sys
from contextlib import contextmanager
from pathlib import Path

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
os.environ.setdefault("AP_ENV", "test")

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from ap import order_state_machine as osm_module  # noqa: E402
from ap import db as db_module  # noqa: E402
from ap.order_state_machine import APOrderStateMachine  # noqa: E402
from ap.trade_queue_lifecycle_reconciler import (  # noqa: E402
    KEEP_NONTERMINAL,
    TERMINALIZE,
    classify_queue_lifecycle,
)


CLIENT_ID = "jason@example.com"
MODE = "live"
SIGNAL_ID = "sig-604-filled"
CANONICAL_SIGNAL_ID = SIGNAL_ID
LOCAL_ORDER_ID = "order-604-filled"
BROKER_ORDER_ID = "broker-604-filled"
QUEUE_ID = 60401


class _Cursor:
    def __init__(self, db):
        self.db = db
        self.rowcount = 0

    def execute(self, sql, params=()):
        normalized = " ".join(str(sql).split()).upper()
        params = tuple(params)
        self.rowcount = 0
        self.db.statements.append((normalized, params))

        if normalized.startswith("UPDATE ORDERS SET"):
            local_index = params.index(LOCAL_ORDER_ID)
            local_id, client_id, old_status = params[local_index:local_index + 3]
            row = self.db.order
            if (
                row["local_order_id"] == local_id
                and row["client_id"] == client_id
                and row["status"] == old_status
            ):
                row["status"] = params[0]
                row["broker_order_id"] = BROKER_ORDER_ID
                self.rowcount = 1
            return

        if normalized.startswith("SELECT ID, CLIENT_ID, SIGNAL_ID, STATUS, PAYLOAD, RESULT_JSON"):
            row = self.db.queue
            if len(params) == 2:
                queue_id, client_id = params
                matches = row["id"] == queue_id and row["client_id"] == client_id
            else:
                client_id, signal_id, local_id = params
                matches = (
                    row["client_id"] == client_id
                    and row["signal_id"] == signal_id
                    and row["result_json"].get("local_order_id") == local_id
                )
            if matches:
                self.db.selected = copy.deepcopy(row)
            else:
                self.db.selected = None
            return

        if normalized.startswith("UPDATE TRADE_QUEUE SET"):
            target_status, diagnostics_json, last_error, queue_id, client_id, signal_id, expected_status, local_id, mode = params
            row = self.db.queue
            if (
                row["id"] == queue_id
                and row["client_id"] == client_id
                and row["signal_id"] == signal_id
                and row["status"] == expected_status
                and row["result_json"].get("local_order_id") == local_id
                and row["result_json"].get("execution_mode") == mode
            ):
                row["status"] = target_status
                row["last_error"] = last_error
                row["finished_ts"] = "set"
                row["result_json"].update(json.loads(diagnostics_json))
                self.rowcount = 1
            return

        raise AssertionError(f"unexpected SQL: {normalized}")

    def fetchall(self):
        return [copy.deepcopy(self.db.selected)] if self.db.selected else []

    def fetchone(self):
        return copy.deepcopy(self.db.selected)


class _DB:
    def __init__(self):
        self.order = {
            "local_order_id": LOCAL_ORDER_ID,
            "client_id": CLIENT_ID,
            "signal_id": SIGNAL_ID,
            "canonical_signal_id": CANONICAL_SIGNAL_ID,
            "execution_mode": MODE,
            "kind": "ENTRY",
            "status": "SUBMITTED",
            "broker_order_id": None,
            "filled_qty": 0,
            "filled_ts": None,
            "fill_price": None,
            "meta": {"queue_id": QUEUE_ID, "canonical_signal_id": CANONICAL_SIGNAL_ID},
        }
        self.queue = {
            "id": QUEUE_ID,
            "client_id": CLIENT_ID,
            "signal_id": SIGNAL_ID,
            "status": "WATCHING",
            "payload": {"execution_mode": MODE},
            "result_json": {
                "local_order_id": LOCAL_ORDER_ID,
                "canonical_signal_id": CANONICAL_SIGNAL_ID,
                "execution_mode": MODE,
            },
            "last_error": None,
            "finished_ts": None,
        }
        self.selected = None
        self.statements = []

    @contextmanager
    def conn(self):
        yield _Cursor(self)


def test_terminal_entry_transition_converges_exact_queue_row(monkeypatch):
    db = _DB()
    monkeypatch.setattr(osm_module, "conn", db.conn)
    monkeypatch.setattr(osm_module, "run_with_retry", lambda fn: fn())
    monkeypatch.setattr(db_module, "conn", db.conn)
    monkeypatch.setattr(db_module, "run_with_retry", lambda fn, **_kwargs: fn())

    osm = APOrderStateMachine(CLIENT_ID)
    monkeypatch.setattr(osm, "_get_order", lambda _local_id: copy.deepcopy(db.order))
    monkeypatch.setattr(osm, "_emit_transition_event", lambda **_kwargs: None)
    monkeypatch.setattr(osm, "_notify_opportunity_ledger", lambda **_kwargs: None)
    monkeypatch.setattr(osm, "_handle_exit_engine_hooks", lambda **_kwargs: None)

    assert osm.transition(
        LOCAL_ORDER_ID,
        "FILLED",
        broker_order_id=BROKER_ORDER_ID,
        filled_qty=1,
        fill_price=1.25,
        filled_ts="2026-09-10T16:00:00+00:00",
    ) is True

    assert db.queue["status"] == "FILLED"
    assert db.queue["finished_ts"] == "set"
    assert db.queue["result_json"]["queue_lifecycle_reason"] == "ENTRY_BROKER_FILL_CONFIRMED"


def _queue_row(**overrides):
    row = {
        "id": QUEUE_ID,
        "client_id": CLIENT_ID,
        "signal_id": SIGNAL_ID,
        "status": "WATCHING",
        "payload": {"execution_mode": MODE},
        "result_json": {
            "local_order_id": LOCAL_ORDER_ID,
            "canonical_signal_id": CANONICAL_SIGNAL_ID,
            "execution_mode": MODE,
        },
    }
    row.update(overrides)
    return row


def _entry_order(**overrides):
    row = {
        "local_order_id": LOCAL_ORDER_ID,
        "client_id": CLIENT_ID,
        "signal_id": SIGNAL_ID,
        "canonical_signal_id": CANONICAL_SIGNAL_ID,
        "execution_mode": MODE,
        "kind": "ENTRY",
        "status": "FILLED",
        "broker_order_id": BROKER_ORDER_ID,
        "filled_qty": 1,
        "meta": {"queue_id": QUEUE_ID, "canonical_signal_id": CANONICAL_SIGNAL_ID},
    }
    row.update(overrides)
    return row


def test_classifier_preserves_active_entry_ownership():
    decision = classify_queue_lifecycle(
        queue_row=_queue_row(),
        durable_state=_entry_order(status="PENDING_TRIGGER", broker_order_id=None, filled_qty=0),
        expected_client_id=CLIENT_ID,
    )

    assert decision.action == KEEP_NONTERMINAL
    assert decision.reason_code == "ENTRY_LIFECYCLE_ACTIVE"


def test_classifier_treats_already_terminal_queue_as_idempotent():
    decision = classify_queue_lifecycle(
        queue_row=_queue_row(status="FILLED"),
        durable_state=_entry_order(),
        expected_client_id=CLIENT_ID,
    )

    assert decision.action == KEEP_NONTERMINAL
    assert decision.target_status == "FILLED"
    assert decision.reason_code == "QUEUE_ALREADY_TERMINAL"


@pytest.mark.parametrize(
    ("order_status", "target_status", "reason_code"),
    [
        ("REJECTED", "REJECTED", "ENTRY_TERMINAL_NO_FILL:REJECTED"),
        ("CANCELED", "CANCELED", "ENTRY_TERMINAL_NO_FILL:CANCELED"),
        ("EXPIRED", "EXPIRED", "ENTRY_TERMINAL_NO_FILL:EXPIRED"),
    ],
)
def test_classifier_projects_terminal_no_fill_without_broker_calls(
    order_status, target_status, reason_code
):
    decision = classify_queue_lifecycle(
        queue_row=_queue_row(),
        durable_state=_entry_order(
            status=order_status,
            broker_order_id=None,
            filled_qty=0,
        ),
        expected_client_id=CLIENT_ID,
    )

    assert decision.action == TERMINALIZE
    assert decision.target_status == target_status
    assert decision.reason_code == reason_code


@pytest.mark.parametrize(
    "order_overrides",
    [
        {"client_id": "other@example.com"},
        {"execution_mode": "paper"},
        {"canonical_signal_id": "wrong-canonical"},
        {"meta": {"queue_id": QUEUE_ID, "execution_mode": "paper"}},
        {"meta": {"queue_id": QUEUE_ID, "submit_intent_at": "2026-09-10T16:00:00Z"}},
    ],
)
def test_classifier_fails_closed_on_identity_or_broker_ambiguity(order_overrides):
    decision = classify_queue_lifecycle(
        queue_row=_queue_row(),
        durable_state=_entry_order(**order_overrides),
        expected_client_id=CLIENT_ID,
    )

    assert decision.action == KEEP_NONTERMINAL
    assert decision.target_status is None
    assert decision.reason_code.startswith(("IDENTITY_UNPROVEN", "BROKER_TRUTH_UNRESOLVED"))


def test_classifier_does_not_use_ticker_as_queue_link():
    decision = classify_queue_lifecycle(
        queue_row=_queue_row(
            result_json={
                "local_order_id": "different-order",
                "execution_mode": MODE,
                "canonical_signal_id": CANONICAL_SIGNAL_ID,
            }
        ),
        durable_state=_entry_order(),
        expected_client_id=CLIENT_ID,
    )

    assert decision.action == KEEP_NONTERMINAL
    assert decision.reason_code == "IDENTITY_UNPROVEN_LOCAL_ORDER_ID_MISMATCH"
