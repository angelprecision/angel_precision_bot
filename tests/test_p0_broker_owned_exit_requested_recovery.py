from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
os.environ.setdefault("AP_ENV", "test")

from ap import exit_decision_idempotency_guard as guard  # noqa: E402
from ap import fill_monitor as fm  # noqa: E402
from ap import order_monitor as om  # noqa: E402
from ap import order_state_machine as osm_module  # noqa: E402
from ap import self_healing as self_healing_module  # noqa: E402
from ap.order_state_machine import APOrderStateMachine, OrderStatus  # noqa: E402


def _row(**overrides):
    value = {
        "client_id": "tradefluence",
        "local_order_id": "exit-orcl-1",
        "broker_order_id": None,
        "position_id": "position-orcl-1",
        "kind": "EXIT",
        "symbol": "ORCL",
        "contract": "ORCL260807P00155000",
        "direction": "PUT",
        "qty": 4,
        "status": "EXIT_REQUESTED",
        "execution_mode": "paper",
        "created_ts": datetime.now(timezone.utc) - timedelta(minutes=10),
        "submitted_ts": None,
        "filled_ts": None,
        "filled_qty": 0,
        "fill_price": None,
        "meta": {"original_failure": "submit_transition_failed"},
    }
    value.update(overrides)
    return value


class _FakeDB:
    def __init__(self, rows):
        self.rows = {str(row["local_order_id"]): row for row in rows}


class _FakeCursor:
    def __init__(self, db: _FakeDB):
        self.db = db
        self.rowcount = None
        self._row = None
        self._rows = []

    def execute(self, sql, params=()):
        normalized = " ".join(str(sql).split())
        self._row = None
        self._rows = []

        if normalized.startswith("UPDATE orders SET status=%s, broker_order_id=%s"):
            (
                new_status,
                broker_id,
                submitted_ts,
                diagnostic,
                local_id,
                client_id,
                execution_mode,
                broker_match,
                *tail,
            ) = params
            row = self.db.rows.get(str(local_id))
            index = 0
            expected_position = None
            expected_qty = None
            if "AND position_id::text=%s" in normalized:
                expected_position = str(tail[index])
                index += 1
            if "AND qty=%s" in normalized:
                expected_qty = int(tail[index])

            matches = bool(
                row
                and str(row.get("client_id") or "") == str(client_id)
                and row.get("kind") == "EXIT"
                and row.get("status") == "EXIT_REQUESTED"
                and row.get("execution_mode") == execution_mode
                and str(row.get("position_id") or "").strip()
                and int(row.get("qty") or 0) > 0
                and (
                    not str(row.get("broker_order_id") or "").strip()
                    or str(row.get("broker_order_id")) == str(broker_match)
                )
                and (
                    expected_position is None
                    or row.get("position_id") == expected_position
                )
                and (expected_qty is None or int(row.get("qty") or 0) == expected_qty)
            )
            if matches:
                row["status"] = str(new_status)
                row["broker_order_id"] = str(broker_id)
                row["submitted_ts"] = row.get("submitted_ts") or submitted_ts
                row["updated_ts"] = "updated-ts"
                patch = json.loads(diagnostic)
                row["meta"] = {**(row.get("meta") or {}), **patch}
                self.rowcount = 1
            else:
                self.rowcount = 0
            return self

        if normalized.startswith("UPDATE orders SET status=%s, updated_ts=NOW()"):
            new_status = params[0]
            index = 1
            updates = {"status": new_status}
            set_clause, _, where_clause = normalized.partition(" WHERE ")
            for column in (
                "broker_order_id",
                "filled_qty",
                "fill_price",
                "last_error",
                "submitted_ts",
                "position_id",
                "filled_ts",
            ):
                if f"{column}=%s" in set_clause:
                    updates[column] = params[index]
                    index += 1
            local_id = str(params[index])
            client_id = str(params[index + 1])
            expected_status = params[index + 2]
            index += 3
            expected_broker_id = None
            if "AND (broker_order_id IS NULL OR broker_order_id='' OR broker_order_id=%s)" in where_clause:
                expected_broker_id = str(params[index])

            row = self.db.rows.get(local_id)
            matches = bool(
                row
                and str(row.get("client_id")) == client_id
                and row.get("status") == expected_status
                and (
                    expected_broker_id is None
                    or not str(row.get("broker_order_id") or "")
                    or str(row.get("broker_order_id")) == expected_broker_id
                )
            )
            if matches:
                row.update(updates)
                row["updated_ts"] = "updated-ts"
                self.rowcount = 1
            else:
                self.rowcount = 0
            return self

        if normalized.startswith("SELECT * FROM orders WHERE local_order_id=%s"):
            local_id = str(params[0])
            client_id = str(params[1])
            row = self.db.rows.get(local_id)
            if row and str(row.get("client_id")) == client_id:
                self._row = dict(row)
            return self

        self.rowcount = 1
        return self

    def fetchone(self):
        return self._row

    def fetchall(self):
        return list(self._rows)


class _FakeConnection:
    def __init__(self, db: _FakeDB):
        self.cursor = _FakeCursor(db)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=()):
        return self.cursor.execute(sql, params)

    @property
    def rowcount(self):
        return self.cursor.rowcount

    def fetchone(self):
        return self.cursor.fetchone()

    def fetchall(self):
        return self.cursor.fetchall()


@pytest.fixture
def fake_osm_db(monkeypatch):
    db = _FakeDB([_row()])
    monkeypatch.setattr(osm_module, "conn", lambda: _FakeConnection(db))
    monkeypatch.setattr(osm_module, "run_with_retry", lambda fn, **kwargs: fn())
    osm = APOrderStateMachine("tradefluence")
    osm._emit_transition_event = MagicMock()
    osm._handle_exit_engine_hooks = MagicMock()
    return db, osm


def test_orcl_shape_adopts_once_and_preserves_diagnostics(fake_osm_db):
    db, osm = fake_osm_db

    first = osm.adopt_broker_owned_exit_request(
        "exit-orcl-1",
        broker_order_id="36661364",
        execution_mode="paper",
        client_id="tradefluence",
        position_id="position-orcl-1",
        expected_qty=4,
        source="orcl_replay",
    )
    second = osm.adopt_broker_owned_exit_request(
        "exit-orcl-1",
        broker_order_id="36661364",
        execution_mode="paper",
        client_id="tradefluence",
        position_id="position-orcl-1",
        expected_qty=4,
        source="restart_replay",
    )

    assert first["disposition"] == "ADOPTED"
    assert second["disposition"] == "ALREADY_BROKER_OWNED_ACTIVE"
    assert db.rows["exit-orcl-1"]["status"] == "EXIT_SUBMITTED"
    assert db.rows["exit-orcl-1"]["broker_order_id"] == "36661364"
    assert db.rows["exit-orcl-1"]["submitted_ts"] is None
    assert db.rows["exit-orcl-1"]["meta"]["original_failure"] == "submit_transition_failed"
    assert db.rows["exit-orcl-1"]["meta"]["broker_ownership_adopted_from_exit_requested"] is True
    assert db.rows["exit-orcl-1"]["meta"]["broker_ownership_submitted_ts_proven"] is False
    assert db.rows["exit-orcl-1"]["meta"]["broker_ownership_submitted_ts_source"] == (
        "unproven_recovery"
    )
    assert db.rows["exit-orcl-1"]["meta"]["broker_ownership_stale_age_reference"] == (
        "created_ts"
    )
    assert not OrderStatus.can_transition(OrderStatus.EXIT_REQUESTED, OrderStatus.EXIT_FILLED)
    osm._handle_exit_engine_hooks.assert_called_once()


def test_adoption_uses_only_explicit_broker_acceptance_timestamp(fake_osm_db):
    db, osm = fake_osm_db

    result = osm.adopt_broker_owned_exit_request(
        "exit-orcl-1",
        broker_order_id="36661364",
        broker_submitted_ts="2026-08-08T15:00:00Z",
        execution_mode="paper",
        client_id="tradefluence",
        position_id="position-orcl-1",
        expected_qty=4,
    )

    assert result["disposition"] == "ADOPTED"
    assert db.rows["exit-orcl-1"]["submitted_ts"] == "2026-08-08T15:00:00+00:00"
    assert db.rows["exit-orcl-1"]["meta"]["broker_ownership_submitted_ts_proven"] is True
    assert db.rows["exit-orcl-1"]["meta"]["broker_ownership_submitted_ts_source"] == (
        "broker_acceptance_evidence"
    )
    assert db.rows["exit-orcl-1"]["meta"]["broker_ownership_stale_age_reference"] == (
        "submitted_ts_or_created_ts"
    )


def test_invalid_broker_acceptance_timestamp_fails_closed(fake_osm_db):
    db, osm = fake_osm_db

    result = osm.adopt_broker_owned_exit_request(
        "exit-orcl-1",
        broker_order_id="36661364",
        broker_submitted_ts="not-a-timestamp",
        execution_mode="paper",
        client_id="tradefluence",
        position_id="position-orcl-1",
        expected_qty=4,
    )

    assert result["disposition"] == "IDENTITY_MISMATCH"
    assert result["adopted"] is False
    assert db.rows["exit-orcl-1"]["status"] == "EXIT_REQUESTED"
    assert db.rows["exit-orcl-1"]["submitted_ts"] is None


def test_callback_trace_preserves_explicit_broker_acceptance_timestamp():
    identity = guard._extract_callback_trace_identity(
        {
            "identity": {},
            "result": {
                "order": {
                    "order_id": "36661364",
                    "accepted_at": "2026-08-08T15:00:00Z",
                }
            },
        }
    )

    assert identity["broker_order_id"] == "36661364"
    assert identity["broker_submitted_ts"] == "2026-08-08T15:00:00Z"
    assert identity["broker_submitted_ts_source"] == "accepted_at"


@pytest.mark.parametrize(
    ("call_kwargs", "expected"),
    [
        ({"client_id": "other-client"}, "IDENTITY_MISMATCH"),
        ({"execution_mode": "unknown"}, "IDENTITY_MISMATCH"),
        ({"execution_mode": "PAPER"}, "IDENTITY_MISMATCH"),
        ({"position_id": "other-position"}, "IDENTITY_MISMATCH"),
        ({"expected_qty": 3}, "IDENTITY_MISMATCH"),
        ({"broker_order_id": "N/A"}, "IDENTITY_MISMATCH"),
        ({"position_id": ""}, "IDENTITY_MISMATCH"),
        ({"source": ""}, "IDENTITY_MISMATCH"),
    ],
)
def test_adoption_fails_closed_on_identity_or_mode(call_kwargs, expected, fake_osm_db):
    db, osm = fake_osm_db
    kwargs = {
        "broker_order_id": "36661364",
        "execution_mode": "paper",
        "client_id": "tradefluence",
        "position_id": "position-orcl-1",
        "expected_qty": 4,
    }
    kwargs.update(call_kwargs)

    result = osm.adopt_broker_owned_exit_request("exit-orcl-1", **kwargs)

    assert result["disposition"] == expected
    assert db.rows["exit-orcl-1"]["status"] == "EXIT_REQUESTED"
    assert db.rows["exit-orcl-1"]["broker_order_id"] is None


def test_adoption_database_error_is_not_treated_as_submit_success(monkeypatch):
    osm = APOrderStateMachine("tradefluence")
    osm._emit_transition_event = MagicMock()
    osm._handle_exit_engine_hooks = MagicMock()

    def _raise():
        raise RuntimeError("db unavailable")

    monkeypatch.setattr(osm_module, "conn", _raise)
    monkeypatch.setattr(osm_module, "run_with_retry", lambda fn, **kwargs: fn())

    result = osm.adopt_broker_owned_exit_request(
        "exit-orcl-1",
        broker_order_id="36661364",
        execution_mode="paper",
        client_id="tradefluence",
        position_id="position-orcl-1",
        expected_qty=4,
    )

    assert result["disposition"] == "DB_ERROR"
    assert result["adopted"] is False
    osm._handle_exit_engine_hooks.assert_not_called()


def test_existing_broker_id_mismatch_cannot_be_replaced(fake_osm_db):
    db, osm = fake_osm_db
    db.rows["exit-orcl-1"]["broker_order_id"] = "historical-broker-id"

    result = osm.adopt_broker_owned_exit_request(
        "exit-orcl-1",
        broker_order_id="different-broker-id",
        execution_mode="paper",
        client_id="tradefluence",
        position_id="position-orcl-1",
        expected_qty=4,
    )

    assert result["disposition"] == "IDENTITY_MISMATCH"
    assert db.rows["exit-orcl-1"]["status"] == "EXIT_REQUESTED"
    assert db.rows["exit-orcl-1"]["broker_order_id"] == "historical-broker-id"


@pytest.mark.parametrize("durable_mode", ["PAPER", " paper "])
def test_noncanonical_durable_mode_cannot_be_laundered_by_recovery(
    fake_osm_db, durable_mode
):
    db, osm = fake_osm_db
    db.rows["exit-orcl-1"]["execution_mode"] = durable_mode

    result = osm.adopt_broker_owned_exit_request(
        "exit-orcl-1",
        broker_order_id="36661364",
        execution_mode="paper",
        client_id="tradefluence",
        position_id="position-orcl-1",
        expected_qty=4,
    )

    assert result["disposition"] == "IDENTITY_MISMATCH"
    assert db.rows["exit-orcl-1"]["status"] == "EXIT_REQUESTED"


@pytest.mark.parametrize("terminal_status", ["EXIT_FILLED", "CANCELED", "REJECTED", "EXPIRED"])
def test_terminal_reread_with_malformed_mode_is_identity_mismatch(
    fake_osm_db, terminal_status
):
    db, osm = fake_osm_db
    db.rows["exit-orcl-1"].update(
        status=terminal_status,
        broker_order_id="36661364",
        execution_mode=" paper ",
    )

    result = osm.adopt_broker_owned_exit_request(
        "exit-orcl-1",
        broker_order_id="36661364",
        execution_mode="paper",
        client_id="tradefluence",
        position_id="position-orcl-1",
        expected_qty=4,
    )

    assert result["disposition"] == "IDENTITY_MISMATCH"
    assert result["already_terminal"] is False
    assert db.rows["exit-orcl-1"]["status"] == terminal_status


def test_adoption_reread_exception_returns_db_error(fake_osm_db, monkeypatch):
    db, osm = fake_osm_db

    def _raise(_local_order_id):
        raise RuntimeError("reread unavailable")

    monkeypatch.setattr(osm, "_get_order", _raise)
    result = osm.adopt_broker_owned_exit_request(
        "exit-orcl-1",
        broker_order_id="36661364",
        execution_mode="paper",
        client_id="tradefluence",
        position_id="position-orcl-1",
        expected_qty=4,
    )

    assert result["disposition"] == "DB_ERROR"
    assert result["adopted"] is False
    assert db.rows["exit-orcl-1"]["status"] == "EXIT_SUBMITTED"
    osm._emit_transition_event.assert_not_called()


def test_adoption_success_without_reread_proof_returns_db_error(fake_osm_db, monkeypatch):
    db, osm = fake_osm_db
    monkeypatch.setattr(osm, "_get_order", lambda _local_order_id: None)

    result = osm.adopt_broker_owned_exit_request(
        "exit-orcl-1",
        broker_order_id="36661364",
        execution_mode="paper",
        client_id="tradefluence",
        position_id="position-orcl-1",
        expected_qty=4,
    )

    assert result["disposition"] == "DB_ERROR"
    assert result["error"] == "adoption_reload_unconfirmed"
    assert db.rows["exit-orcl-1"]["status"] == "EXIT_SUBMITTED"
    osm._emit_transition_event.assert_not_called()


def test_adoption_disposition_contract_is_closed(fake_osm_db, monkeypatch):
    db, osm = fake_osm_db
    allowed = {
        "ADOPTED",
        "ALREADY_BROKER_OWNED_ACTIVE",
        "ALREADY_TERMINAL",
        "IDENTITY_MISMATCH",
        "DB_ERROR",
    }

    results = [
        osm.adopt_broker_owned_exit_request(
            "exit-orcl-1",
            broker_order_id="36661364",
            execution_mode="paper",
            client_id="tradefluence",
            position_id="position-orcl-1",
            expected_qty=4,
        )
    ]
    results.append(
        osm.adopt_broker_owned_exit_request(
            "exit-orcl-1",
            broker_order_id="36661364",
            execution_mode="paper",
            client_id="tradefluence",
            position_id="position-orcl-1",
            expected_qty=4,
        )
    )
    db.rows["exit-orcl-1"].update(status="EXIT_FILLED")
    results.append(
        osm.adopt_broker_owned_exit_request(
            "exit-orcl-1",
            broker_order_id="36661364",
            execution_mode="paper",
            client_id="tradefluence",
            position_id="position-orcl-1",
            expected_qty=4,
        )
    )
    db.rows["exit-orcl-1"].update(
        status="EXIT_REQUESTED",
        broker_order_id=None,
        execution_mode=" paper ",
    )
    results.append(
        osm.adopt_broker_owned_exit_request(
            "exit-orcl-1",
            broker_order_id="36661364",
            execution_mode="paper",
            client_id="tradefluence",
            position_id="position-orcl-1",
            expected_qty=4,
        )
    )

    def _raise(_local_order_id):
        raise RuntimeError("reread unavailable")

    monkeypatch.setattr(osm, "_get_order", _raise)
    results.append(
        osm.adopt_broker_owned_exit_request(
            "exit-orcl-1",
            broker_order_id="36661364",
            execution_mode="paper",
            client_id="tradefluence",
            position_id="position-orcl-1",
            expected_qty=4,
        )
    )

    assert {result["disposition"] for result in results} == allowed


@pytest.mark.parametrize("terminal_status", ["EXIT_FILLED", "CANCELED", "REJECTED", "EXPIRED"])
def test_terminal_re_read_is_idempotent_and_identity_fenced(fake_osm_db, terminal_status):
    db, osm = fake_osm_db
    db.rows["exit-orcl-1"].update(
        status=terminal_status,
        broker_order_id="36661364",
    )

    result = osm.adopt_broker_owned_exit_request(
        "exit-orcl-1",
        broker_order_id="36661364",
        execution_mode="paper",
        client_id="tradefluence",
        position_id="position-orcl-1",
        expected_qty=4,
    )

    assert result["disposition"] == "ALREADY_TERMINAL"
    assert result["already_terminal"] is True
    assert result["status"] == terminal_status
    assert db.rows["exit-orcl-1"]["status"] == terminal_status

    mismatch = osm.adopt_broker_owned_exit_request(
        "exit-orcl-1",
        broker_order_id="different-broker-id",
        execution_mode="paper",
        client_id="tradefluence",
        position_id="position-orcl-1",
        expected_qty=4,
    )
    assert mismatch["disposition"] == "IDENTITY_MISMATCH"
    assert mismatch["error"] == "terminal_broker_order_id_mismatch"


class _Broker:
    def __init__(self, raw):
        self.raw = dict(raw)
        self.calls = []

    def get_order(self, broker_order_id):
        self.calls.append(str(broker_order_id))
        return dict(self.raw)


class _RecoveryOSM:
    def __init__(self, real_osm, db):
        self.real_osm = real_osm
        self.db = db
        self.client_id = real_osm.client_id
        self.transition_calls = []

    def adopt_broker_owned_exit_request(self, *args, **kwargs):
        return self.real_osm.adopt_broker_owned_exit_request(*args, **kwargs)

    def get_order(self, local_order_id):
        return self.real_osm.get_order(local_order_id)

    def transition(self, local_order_id, new_status, **kwargs):
        self.transition_calls.append((local_order_id, new_status, kwargs))
        row = self.db.rows[local_order_id]
        row["status"] = new_status
        if kwargs.get("filled_qty") is not None:
            row["filled_qty"] = int(kwargs["filled_qty"])
        if kwargs.get("fill_price") is not None:
            row["fill_price"] = kwargs["fill_price"]
        return True

    def increment_retry(self, local_order_id):
        return None


def test_fill_monitor_orcl_replay_adopts_then_uses_canonical_fill_path(
    fake_osm_db, monkeypatch
):
    db, real_osm = fake_osm_db
    db.rows["exit-orcl-1"]["broker_order_id"] = "36661364"
    recovery_osm = _RecoveryOSM(real_osm, db)
    broker = _Broker(
        {
            "status": "FILLED",
            "exec_quantity": 4,
            "avg_fill_price": 1.25,
            "quantity": 4,
        }
    )
    monkeypatch.setattr(fm, "audit", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *args, **kwargs: True)
    monkeypatch.setattr(fm, "_sync_exit_price", lambda *args, **kwargs: None)

    original_order = dict(db.rows["exit-orcl-1"])
    fm.process_pending_order(
        broker,
        original_order,
        osm=recovery_osm,
        runtime_execution_mode="paper",
    )
    # A replay of the same broker-owned request must not poll/finalize it a
    # second time after the durable row has reached its terminal fill state.
    fm.process_pending_order(
        broker,
        original_order,
        osm=recovery_osm,
        runtime_execution_mode="paper",
    )

    assert broker.calls == ["36661364"]
    assert [call[1] for call in recovery_osm.transition_calls] == ["EXIT_FILLED"]
    assert db.rows["exit-orcl-1"]["status"] == "EXIT_FILLED"
    assert db.rows["exit-orcl-1"]["filled_qty"] == 4


@pytest.mark.parametrize(
    ("raw_status", "raw_qty", "expected_status"),
    [
        ("OPEN", 0, "EXIT_ACKNOWLEDGED"),
        ("PARTIALLY_FILLED", 1, "EXIT_PARTIAL_FILL"),
        ("CANCELED", 0, "CANCELED"),
        ("REJECTED", 0, "REJECTED"),
        ("EXPIRED", 0, "EXPIRED"),
    ],
)
def test_fill_monitor_recovery_uses_existing_status_reducer(
    fake_osm_db,
    monkeypatch,
    raw_status,
    raw_qty,
    expected_status,
):
    db, real_osm = fake_osm_db
    db.rows["exit-orcl-1"]["broker_order_id"] = "36661364"
    recovery_osm = _RecoveryOSM(real_osm, db)
    broker = _Broker(
        {
            "status": raw_status,
            "exec_quantity": raw_qty,
            "avg_fill_price": 1.25,
            "quantity": 4,
        }
    )
    monkeypatch.setattr(fm, "audit", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *args, **kwargs: True)
    monkeypatch.setattr(fm, "_sync_exit_price", lambda *args, **kwargs: None)

    fm.process_pending_order(
        broker,
        dict(db.rows["exit-orcl-1"]),
        osm=recovery_osm,
        runtime_execution_mode="paper",
    )

    assert broker.calls == ["36661364"]
    assert [call[1] for call in recovery_osm.transition_calls] == [expected_status]
    assert db.rows["exit-orcl-1"]["status"] == expected_status


def test_broker_lookup_failure_holds_without_replacement_or_cancel(fake_osm_db, monkeypatch):
    db, real_osm = fake_osm_db
    db.rows["exit-orcl-1"]["broker_order_id"] = "36661364"
    recovery_osm = _RecoveryOSM(real_osm, db)

    class _LookupFailureBroker(_Broker):
        def get_order(self, broker_order_id):
            self.calls.append(str(broker_order_id))
            raise RuntimeError("broker lookup unavailable")

    broker = _LookupFailureBroker({})
    monkeypatch.setattr(fm, "audit", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *args, **kwargs: True)

    fm.process_pending_order(
        broker,
        dict(db.rows["exit-orcl-1"]),
        osm=recovery_osm,
        runtime_execution_mode="paper",
    )

    assert broker.calls == ["36661364"]
    assert recovery_osm.transition_calls == []
    assert db.rows["exit-orcl-1"]["status"] == "EXIT_SUBMITTED"


def test_order_monitor_unknown_broker_status_holds_without_cancel(monkeypatch):
    order = _row(broker_order_id="unknown-broker-425")
    osm = _MonitorOSM(order)

    class _LookupFailureBroker(_Broker):
        def get_order(self, broker_order_id):
            self.calls.append(str(broker_order_id))
            raise RuntimeError("broker lookup unavailable")

        def cancel_order(self, broker_order_id):
            self.calls.append(f"cancel:{broker_order_id}")
            return {"status": "canceled"}

    broker = _LookupFailureBroker({})
    monitor = _monitor(order, osm, broker)
    monkeypatch.setattr(om, "TIMEOUT_EXIT_PENDING", 0)
    monkeypatch.setattr(om, "ORDER_MONITOR_CAN_ACT", True)

    monitor._check_exit_orders()

    assert broker.calls == ["unknown-broker-425"]
    assert osm.transition_calls == []
    assert any(
        call.kwargs.get("reason_code") == "BROKER_STATUS_UNKNOWN"
        for call in monitor._emit_order_event.call_args_list
    )


def test_self_healing_restart_does_not_invent_live_recovery_mode(monkeypatch):
    captured = {}

    class _RestartedMonitor:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def start(self):
            return None

    monkeypatch.setattr(om, "APOrderMonitor", _RestartedMonitor, raising=False)
    runner = SimpleNamespace(
        email="tradefluence",
        order_monitor=None,
        order_state_machine=MagicMock(),
        position_manager=MagicMock(),
        core=SimpleNamespace(
            broker=object(),
            exit_eng=MagicMock(),
            entry_watcher=None,
            contract_selector=None,
        ),
    )
    system = self_healing_module.APSelfHealingSystem()
    self_healing_module.restart_registry.unregister_client(runner.email)
    system._register_restart_fns(runner)
    try:
        restart = self_healing_module.restart_registry.get(runner.email, "order_monitor")
        assert restart is not None
        assert restart() is True
        assert captured["client_mode"] is None
    finally:
        self_healing_module.restart_registry.unregister_client(runner.email)


class _CanonicalExitEngine:
    def __init__(self):
        self.client_id = "tradefluence"
        self.pending_calls = []
        self.closed_calls = []
        self.position = SimpleNamespace(position_id="position-orcl-1", quantity_remaining=4)

    def get_position(self, position_id):
        return self.position if position_id == self.position.position_id else None

    def set_pending_exit_order(self, position_id, **kwargs):
        self.pending_calls.append((position_id, kwargs))

    def mark_position_closed(self, position_id, **kwargs):
        self.closed_calls.append((position_id, kwargs))
        self.position.quantity_remaining = max(
            0,
            self.position.quantity_remaining - int(kwargs.get("qty_filled") or 0),
        )


def test_orcl_replay_uses_real_osm_transition_and_applies_close_once(
    fake_osm_db, monkeypatch
):
    db, osm = fake_osm_db
    db.rows["exit-orcl-1"]["broker_order_id"] = "36661364"
    engine = _CanonicalExitEngine()
    monkeypatch.setitem(osm_module._exit_engine_registry, "tradefluence", engine)
    osm._handle_exit_engine_hooks = APOrderStateMachine._handle_exit_engine_hooks.__get__(
        osm, APOrderStateMachine
    )
    osm._finalize_position_from_exit_order = MagicMock()
    broker = _Broker(
        {
            "status": "FILLED",
            "exec_quantity": 4,
            "avg_fill_price": 1.25,
            "quantity": 4,
        }
    )
    monkeypatch.setattr(fm, "audit", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *args, **kwargs: True)
    monkeypatch.setattr(fm, "_sync_exit_price", lambda *args, **kwargs: None)
    original_order = dict(db.rows["exit-orcl-1"])

    fm.process_pending_order(
        broker,
        original_order,
        osm=osm,
        runtime_execution_mode="paper",
    )
    fm.process_pending_order(
        broker,
        original_order,
        osm=osm,
        runtime_execution_mode="paper",
    )

    assert broker.calls == ["36661364"]
    assert db.rows["exit-orcl-1"]["status"] == "EXIT_FILLED"
    assert db.rows["exit-orcl-1"]["filled_qty"] == 4
    assert len(engine.pending_calls) == 1
    assert len(engine.closed_calls) == 1


def test_orcl_replay_proves_real_postgres_cas_and_single_economic_fill(monkeypatch):
    """Use disposable PostgreSQL for adoption CAS, fill transition, and replay fencing."""
    from contextlib import contextmanager
    import uuid

    psycopg2 = pytest.importorskip("psycopg2")
    extras = pytest.importorskip("psycopg2.extras")
    database_url = os.getenv("DATABASE_URL", "")
    if not database_url:
        pytest.skip("DATABASE_URL not configured")

    try:
        admin = psycopg2.connect(database_url, connect_timeout=2)
    except Exception as exc:
        pytest.skip(f"disposable PostgreSQL unavailable: {type(exc).__name__}")

    schema = f"pr425_{uuid.uuid4().hex}"

    @contextmanager
    def _pg_conn():
        connection = psycopg2.connect(database_url, connect_timeout=2)
        cursor = connection.cursor(cursor_factory=extras.RealDictCursor)
        try:
            cursor.execute(f'SET search_path TO "{schema}"')
            yield _RealPostgresConnection(connection, cursor)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()
            connection.close()

    class _RealPostgresConnection:
        def __init__(self, connection, cursor):
            self.connection = connection
            self.cursor = cursor

        @property
        def rowcount(self):
            return self.cursor.rowcount

        def execute(self, sql, params=None):
            self.cursor.execute(sql, params)
            return self

        def fetchone(self):
            return self.cursor.fetchone()

        def fetchall(self):
            return self.cursor.fetchall()

    try:
        admin.autocommit = True
        with admin.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA "{schema}"')
            cursor.execute(
                f"""
                CREATE TABLE "{schema}".orders (
                    local_order_id TEXT PRIMARY KEY,
                    client_id TEXT NOT NULL,
                    position_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    broker_order_id TEXT,
                    submitted_ts TIMESTAMPTZ,
                    filled_ts TIMESTAMPTZ,
                    filled_qty INTEGER NOT NULL DEFAULT 0,
                    fill_price NUMERIC,
                    execution_mode TEXT NOT NULL,
                    qty INTEGER NOT NULL,
                    meta JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    last_error TEXT,
                    symbol TEXT,
                    contract TEXT,
                    created_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE "{schema}".positions (
                    id TEXT PRIMARY KEY,
                    client_id TEXT NOT NULL,
                    quantity_remaining INTEGER NOT NULL,
                    qty INTEGER NOT NULL
                )
                """
            )
            cursor.execute(
                f"""
                INSERT INTO "{schema}".positions
                    (id, client_id, quantity_remaining, qty)
                VALUES ('position-orcl-1', 'tradefluence', 4, 4)
                """
            )
            cursor.execute(
                f"""
                INSERT INTO "{schema}".orders
                    (local_order_id, client_id, position_id, kind, status,
                     broker_order_id, execution_mode, qty, symbol, contract)
                VALUES
                    ('exit-orcl-concurrent', 'tradefluence', 'position-orcl-1',
                     'EXIT', 'EXIT_REQUESTED', NULL, 'paper', 4, 'ORCL',
                     'ORCL260807P00155000'),
                    ('exit-orcl-real-pg', 'tradefluence', 'position-orcl-1',
                     'EXIT', 'EXIT_REQUESTED', '36661364', 'paper', 4, 'ORCL',
                     'ORCL260807P00155000')
                """
            )

        monkeypatch.setattr(osm_module, "conn", _pg_conn)
        monkeypatch.setattr(osm_module, "run_with_retry", lambda fn, **kwargs: fn())
        engine = _CanonicalExitEngine()
        monkeypatch.setitem(osm_module._exit_engine_registry, "tradefluence", engine)

        osm_a = APOrderStateMachine("tradefluence")
        osm_b = APOrderStateMachine("tradefluence")
        for state_machine in (osm_a, osm_b):
            state_machine._emit_transition_event = MagicMock()
            state_machine._notify_opportunity_ledger = MagicMock()

        barrier = threading.Barrier(2)
        adoption_results = []
        adoption_errors = []

        def _concurrent_adopt(state_machine):
            try:
                barrier.wait(timeout=5)
                adoption_results.append(
                    state_machine.adopt_broker_owned_exit_request(
                        "exit-orcl-concurrent",
                        broker_order_id="concurrent-broker-425",
                        execution_mode="paper",
                        client_id="tradefluence",
                        position_id="position-orcl-1",
                        expected_qty=4,
                        source="real_postgres_concurrency_replay",
                    )
                )
            except Exception as exc:
                adoption_errors.append(exc)

        threads = [
            threading.Thread(target=_concurrent_adopt, args=(osm_a,)),
            threading.Thread(target=_concurrent_adopt, args=(osm_b,)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        assert adoption_errors == []
        assert all(not thread.is_alive() for thread in threads)
        assert {result["disposition"] for result in adoption_results} == {
            "ADOPTED",
            "ALREADY_BROKER_OWNED_ACTIVE",
        }
        with _pg_conn() as connection:
            connection.execute(
                "SELECT status, broker_order_id FROM orders "
                "WHERE local_order_id=%s AND client_id=%s",
                ("exit-orcl-concurrent", "tradefluence"),
            )
            concurrent_row = dict(connection.fetchone())
        assert concurrent_row == {
            "status": "EXIT_SUBMITTED",
            "broker_order_id": "concurrent-broker-425",
        }
        assert len(engine.pending_calls) == 1

        osm = APOrderStateMachine("tradefluence")
        osm._emit_transition_event = MagicMock()
        osm._notify_opportunity_ledger = MagicMock()
        osm._finalize_position_from_exit_order = MagicMock()
        broker = _Broker(
            {
                "status": "FILLED",
                "exec_quantity": 4,
                "avg_fill_price": 1.25,
                "quantity": 4,
            }
        )
        monkeypatch.setattr(fm, "audit", lambda *args, **kwargs: None)
        monkeypatch.setattr(fm, "emit_fill_event", lambda *args, **kwargs: True)
        monkeypatch.setattr(fm, "_sync_exit_price", lambda *args, **kwargs: None)
        with _pg_conn() as connection:
            connection.execute(
                "SELECT * FROM orders WHERE local_order_id=%s AND client_id=%s",
                ("exit-orcl-real-pg", "tradefluence"),
            )
            original_order = dict(connection.fetchone())

        fm.process_pending_order(
            broker,
            original_order,
            osm=osm,
            runtime_execution_mode="paper",
        )
        fm.process_pending_order(
            broker,
            original_order,
            osm=osm,
            runtime_execution_mode="paper",
        )

        with _pg_conn() as connection:
            connection.execute(
                "SELECT status, filled_qty, fill_price FROM orders "
                "WHERE local_order_id=%s AND client_id=%s",
                ("exit-orcl-real-pg", "tradefluence"),
            )
            filled_row = dict(connection.fetchone())
        assert broker.calls == ["36661364"]
        assert filled_row["status"] == "EXIT_FILLED"
        assert filled_row["filled_qty"] == 4
        assert float(filled_row["fill_price"]) == 1.25
        assert len(engine.pending_calls) == 2
        assert len(engine.closed_calls) == 1
        assert engine.position.quantity_remaining == 0
        osm._finalize_position_from_exit_order.assert_called_once()
    finally:
        try:
            with admin.cursor() as cursor:
                cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            admin.close()


def test_fill_monitor_holds_on_runtime_mode_conflict_before_broker_poll(
    fake_osm_db, monkeypatch
):
    db, real_osm = fake_osm_db
    db.rows["exit-orcl-1"]["broker_order_id"] = "36661364"
    recovery_osm = _RecoveryOSM(real_osm, db)
    broker = _Broker({"status": "FILLED", "exec_quantity": 4})
    monkeypatch.setattr(fm, "audit", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *args, **kwargs: True)

    fm.process_pending_order(
        broker,
        dict(db.rows["exit-orcl-1"]),
        osm=recovery_osm,
        runtime_execution_mode="live",
    )

    assert broker.calls == []
    assert recovery_osm.transition_calls == []
    assert db.rows["exit-orcl-1"]["status"] == "EXIT_REQUESTED"


def test_brokerless_exit_requested_is_not_polled_or_advanced(monkeypatch):
    broker = _Broker({"status": "FILLED", "exec_quantity": 4})
    osm = MagicMock()
    order = _row(broker_order_id=None)
    monkeypatch.setattr(fm, "audit", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *args, **kwargs: True)

    fm.process_pending_order(broker, order, osm=osm)

    assert broker.calls == []
    osm.transition.assert_not_called()
    osm.adopt_broker_owned_exit_request.assert_not_called()


def test_broker_owned_query_excludes_local_intents(monkeypatch):
    captured = {}

    class _QueryCursor:
        def execute(self, sql, params):
            captured["sql"] = " ".join(str(sql).split()).lower()
            captured["params"] = params
            return self

        def fetchall(self):
            return []

    class _QueryConn:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params):
            return _QueryCursor().execute(sql, params)

    monkeypatch.setattr(fm, "conn", lambda: _QueryConn())
    monkeypatch.setattr(fm, "run_with_retry", lambda fn, **kwargs: fn())

    assert fm.get_broker_owned_exit_requests("tradefluence") == []
    sql = captured["sql"]
    assert "kind = 'exit'" in sql
    assert "status = 'exit_requested'" in sql
    assert "btrim(broker_order_id) <> ''" in sql
    assert "upper(btrim(broker_order_id)) <> 'n/a'" in sql
    assert "qty > 0" in sql
    assert "execution_mode in ('live','paper')" in sql


def test_order_monitor_active_exit_select_carries_adoption_identity_fields(monkeypatch):
    captured = {}

    class _QueryCursor:
        def execute(self, sql, params):
            captured["sql"] = " ".join(str(sql).split()).lower()
            return self

        def fetchall(self):
            return []

    class _QueryConn:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params):
            return _QueryCursor().execute(sql, params)

    monitor = om.APOrderMonitor(
        client_id="tradefluence",
        broker=MagicMock(),
        order_state_machine=MagicMock(),
        position_manager=MagicMock(),
        exit_engine=MagicMock(),
        client_mode="PAPER",
    )
    monkeypatch.setattr(om, "conn", lambda: _QueryConn())
    monkeypatch.setattr(om, "run_with_retry", lambda fn, **kwargs: fn())

    assert monitor._get_active_exit_orders() == []
    sql = captured["sql"]
    assert "qty, execution_mode" in sql
    assert "meta," in sql
    assert "meta->>'broker_submitted_ts'" in sql


class _MonitorOSM:
    def __init__(self, order, *, adopt=True):
        self.order = dict(order)
        self.adopt = adopt
        self.adopt_calls = []
        self.transition_calls = []

    def get_order(self, local_order_id):
        return dict(self.order) if local_order_id == self.order["local_order_id"] else None

    def adopt_broker_owned_exit_request(self, local_order_id, **kwargs):
        self.adopt_calls.append((local_order_id, kwargs))
        if not self.adopt:
            return {
                "disposition": "IDENTITY_MISMATCH",
                "adopted": False,
                "reason_code": "EXIT_BROKER_OWNERSHIP_ADOPTION_FAILED",
                "error": "identity_or_status_mismatch",
            }
        self.order["status"] = "EXIT_SUBMITTED"
        # Adoption must not manufacture broker submission chronology.  A test
        # row may opt into an explicit broker timestamp when that evidence is
        # part of the input.
        if self.order.get("broker_submitted_ts"):
            self.order["submitted_ts"] = self.order["broker_submitted_ts"]
        return {
            "disposition": "ADOPTED",
            "adopted": True,
            "status": "EXIT_SUBMITTED",
            "reason_code": "EXIT_BROKER_OWNERSHIP_ADOPTED_FROM_REQUESTED",
        }

    def transition(self, local_order_id, new_status, **kwargs):
        self.transition_calls.append((local_order_id, new_status, kwargs))
        self.order["status"] = new_status
        return True


class _GuardOSM:
    def __init__(self, active_order):
        self.active_order = active_order
        self.adopt_broker_owned_exit_request = MagicMock()

    def get_active_exit_order(self, position_id):
        if str(self.active_order.get("position_id") or "") == str(position_id or ""):
            return self.active_order
        return None


def _guard_pos(**overrides):
    values = {
        "position_id": "position-guard-1",
        "client_id": "tradefluence",
        "ticker": "ORCL",
        "option_symbol": "ORCL260807P00155000",
        "side": "PUT",
        "quantity_remaining": 4,
        "closed": False,
        "exit_in_flight": False,
        "pending_exit_local_order_id": "",
        "pending_exit_broker_order_id": "",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _guard_reserved_order(pos, *, local_order_id="exit-local-reserved", execution_mode="paper", **overrides):
    order = {
        "client_id": pos.client_id,
        "local_order_id": local_order_id,
        "broker_order_id": "",
        "position_id": pos.position_id,
        "kind": "EXIT",
        "qty": 4,
        "status": "EXIT_REQUESTED",
        "execution_mode": execution_mode,
    }
    order.update(overrides)
    return order


def _guard_submit_engine(pos, active_order, callback, *, mode="PAPER"):
    osm = _GuardOSM(active_order)
    engine = SimpleNamespace(
        _lock=threading.RLock(),
        client_id=pos.client_id,
        order_state_machine=osm,
        osm=None,
        master_control=SimpleNamespace(mode=mode),
        on_scale=callback,
        on_exit=callback,
    )
    engine._extract_exit_order_identity = lambda result: {
        "accepted": bool(result.get("accepted")),
        "local_order_id": result.get("local_order_id"),
        "broker_order_id": result.get("broker_order_id"),
        "raw_status": result.get("status"),
    }
    return engine, osm


def _patch_guard_submit_claim(monkeypatch):
    updates = []
    monkeypatch.setattr(
        guard,
        "_durable_exit_generation",
        lambda *_args: ("tradefluence|position-guard-1|4|1", 1),
    )
    monkeypatch.setattr(
        guard,
        "_claim_durable_decision_generation",
        lambda **_kwargs: {"claimed": True},
    )
    monkeypatch.setattr(
        guard,
        "_update_durable_decision_generation",
        lambda generation_key, **kwargs: updates.append((generation_key, kwargs)),
    )
    return updates


@pytest.mark.parametrize(
    ("runtime_mode", "durable_mode"),
    [("LIVE", "paper"), ("PAPER", "live")],
)
def test_reserved_mode_mismatch_blocks_before_broker_callback(
    monkeypatch,
    runtime_mode,
    durable_mode,
):
    pos = _guard_pos(pending_exit_local_order_id="exit-local-reserved")
    active_order = _guard_reserved_order(
        pos,
        execution_mode=durable_mode,
    )
    callback = MagicMock()
    engine, osm = _guard_submit_engine(
        pos,
        active_order,
        callback,
        mode=runtime_mode,
    )
    wrapped = guard.wrap_submit(
        lambda submit_engine, submit_pos, decision: bool(
            submit_engine.on_scale(submit_pos, decision)
        )
    )

    assert wrapped(engine, pos, SimpleNamespace(action="SCALE_OUT", should_act=True)) is False
    assert callback.call_count == 0
    assert osm.adopt_broker_owned_exit_request.call_count == 0


@pytest.mark.parametrize("durable_mode", ["LIVE", "PAPER", " live ", "", None])
def test_malformed_reserved_durable_mode_blocks_before_broker_callback(
    durable_mode,
):
    pos = _guard_pos(pending_exit_local_order_id="exit-local-reserved")
    active_order = _guard_reserved_order(pos, execution_mode=durable_mode)
    callback = MagicMock()
    engine, osm = _guard_submit_engine(pos, active_order, callback, mode="LIVE")
    wrapped = guard.wrap_submit(
        lambda submit_engine, submit_pos, decision: bool(
            submit_engine.on_scale(submit_pos, decision)
        )
    )

    assert wrapped(engine, pos, SimpleNamespace(action="SCALE_OUT", should_act=True)) is False
    assert callback.call_count == 0
    assert osm.adopt_broker_owned_exit_request.call_count == 0


def test_unproven_runtime_mode_blocks_before_broker_callback():
    pos = _guard_pos(pending_exit_local_order_id="exit-local-reserved")
    active_order = _guard_reserved_order(pos, execution_mode="paper")
    callback = MagicMock()
    engine, osm = _guard_submit_engine(pos, active_order, callback, mode="UNKNOWN")
    wrapped = guard.wrap_submit(
        lambda submit_engine, submit_pos, decision: bool(
            submit_engine.on_scale(submit_pos, decision)
        )
    )

    assert wrapped(engine, pos, SimpleNamespace(action="SCALE_OUT", should_act=True)) is False
    assert callback.call_count == 0
    assert osm.adopt_broker_owned_exit_request.call_count == 0


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("kind", "ENTRY"),
        ("client_id", "other-client"),
        ("position_id", "other-position"),
        ("qty", 0),
        ("broker_order_id", "already-owned"),
    ],
)
def test_reserved_identity_mismatch_blocks_before_broker_callback(field, value):
    pos = _guard_pos(pending_exit_local_order_id="exit-local-reserved")
    active_order = _guard_reserved_order(pos, **{field: value})
    callback = MagicMock()
    engine, osm = _guard_submit_engine(pos, active_order, callback, mode="PAPER")
    wrapped = guard.wrap_submit(
        lambda submit_engine, submit_pos, decision: bool(
            submit_engine.on_scale(submit_pos, decision)
        )
    )

    assert wrapped(engine, pos, SimpleNamespace(action="SCALE_OUT", should_act=True)) is False
    assert callback.call_count == 0
    assert osm.adopt_broker_owned_exit_request.call_count == 0


def test_reserved_local_id_is_only_a_pointer_when_active_row_is_missing():
    pos = _guard_pos(pending_exit_local_order_id="exit-local-missing")
    callback = MagicMock()

    class _NoActiveRowOSM(_GuardOSM):
        def __init__(self):
            super().__init__({})

        def get_active_exit_order(self, position_id):
            return None

    osm = _NoActiveRowOSM()
    engine = SimpleNamespace(
        _lock=threading.RLock(),
        client_id=pos.client_id,
        order_state_machine=osm,
        osm=None,
        master_control=SimpleNamespace(mode="LIVE"),
        on_scale=callback,
        on_exit=callback,
    )
    wrapped = guard.wrap_submit(
        lambda submit_engine, submit_pos, decision: bool(
            submit_engine.on_scale(submit_pos, decision)
        )
    )

    assert wrapped(engine, pos, SimpleNamespace(action="SCALE_OUT", should_act=True)) is False
    assert callback.call_count == 0


def test_live_submit_fails_closed_when_active_exit_lookup_raises():
    pos = _guard_pos(pending_exit_local_order_id="exit-local-lookup-error")
    callback = MagicMock()

    class _LookupFailureOSM(_GuardOSM):
        def __init__(self):
            super().__init__({})

        def get_active_exit_order(self, position_id):
            raise RuntimeError("active exit lookup unavailable")

    engine = SimpleNamespace(
        _lock=threading.RLock(),
        client_id=pos.client_id,
        order_state_machine=_LookupFailureOSM(),
        osm=None,
        master_control=SimpleNamespace(mode="LIVE"),
        on_scale=callback,
        on_exit=callback,
    )
    wrapped = guard.wrap_submit(
        lambda submit_engine, submit_pos, decision: bool(
            submit_engine.on_scale(submit_pos, decision)
        )
    )

    assert wrapped(engine, pos, SimpleNamespace(action="SCALE_OUT", should_act=True)) is False
    assert callback.call_count == 0


@pytest.mark.parametrize(
    ("runtime_mode", "durable_mode"),
    [("LIVE", "live"), ("PAPER", "paper")],
)
def test_exact_reserved_mode_allows_one_broker_submit_and_adoption(
    monkeypatch,
    runtime_mode,
    durable_mode,
):
    pos = _guard_pos(pending_exit_local_order_id="exit-local-reserved")
    active_order = _guard_reserved_order(pos, execution_mode=durable_mode)
    callback_calls = []

    def callback(callback_pos, _decision):
        callback_calls.append(True)
        callback_pos.exit_in_flight = True
        callback_pos.pending_exit_broker_order_id = "broker-exact-1"
        active_order["broker_order_id"] = "broker-exact-1"
        return {
            "ok": True,
            "accepted": True,
            "status": "EXIT_SUBMITTED",
            "local_order_id": "exit-local-reserved",
            "broker_order_id": "broker-exact-1",
        }

    engine, osm = _guard_submit_engine(
        pos,
        active_order,
        callback,
        mode=runtime_mode,
    )
    osm.adopt_broker_owned_exit_request.return_value = {
        "disposition": "ADOPTED",
        "adopted": True,
        "status": "EXIT_SUBMITTED",
        "reason_code": "EXIT_BROKER_OWNERSHIP_ADOPTED_FROM_REQUESTED",
    }
    updates = _patch_guard_submit_claim(monkeypatch)
    wrapped = guard.wrap_submit(
        lambda submit_engine, submit_pos, decision: bool(
            submit_engine.on_scale(submit_pos, decision)
        )
    )

    assert wrapped(engine, pos, SimpleNamespace(action="SCALE_OUT", should_act=True)) is True
    assert callback_calls == [True]
    osm.adopt_broker_owned_exit_request.assert_called_once()
    assert osm.adopt_broker_owned_exit_request.call_args.kwargs["execution_mode"] == durable_mode
    assert updates[0][1]["claim_state"] == guard._CLAIM_STATE_BROKER_OWNED
    assert updates[0][1]["broker_order_id"] == "broker-exact-1"


@pytest.mark.parametrize(
    ("runtime_mode", "post_acceptance_mode"),
    [("LIVE", "paper"), ("PAPER", "live")],
)
def test_post_acceptance_mode_conflict_quarantines_broker_owned_claim(
    monkeypatch,
    runtime_mode,
    post_acceptance_mode,
):
    pos = _guard_pos(pending_exit_local_order_id="exit-local-reserved")
    active_order = _guard_reserved_order(
        pos,
        execution_mode=runtime_mode.lower(),
    )
    callback_calls = []

    def callback(callback_pos, _decision):
        callback_calls.append(True)
        callback_pos.exit_in_flight = True
        callback_pos.pending_exit_broker_order_id = "broker-mode-conflict"
        # Simulate a durable row observed in the wrong mode after the broker
        # accepted the request. The broker id must remain quarantined.
        active_order["execution_mode"] = post_acceptance_mode
        active_order["broker_order_id"] = "broker-mode-conflict"
        return {
            "ok": True,
            "accepted": True,
            "status": "EXIT_SUBMITTED",
            "local_order_id": "exit-local-reserved",
            "broker_order_id": "broker-mode-conflict",
        }

    engine, osm = _guard_submit_engine(
        pos,
        active_order,
        callback,
        mode=runtime_mode,
    )
    updates = _patch_guard_submit_claim(monkeypatch)
    wrapped = guard.wrap_submit(
        lambda submit_engine, submit_pos, decision: bool(
            submit_engine.on_scale(submit_pos, decision)
        )
    )

    assert wrapped(engine, pos, SimpleNamespace(action="SCALE_OUT", should_act=True)) is True
    assert callback_calls == [True]
    osm.adopt_broker_owned_exit_request.assert_not_called()
    assert updates[0][1]["claim_state"] == guard._CLAIM_STATE_BROKER_OWNED
    assert updates[0][1]["local_order_id"] == "exit-local-reserved"
    assert updates[0][1]["broker_order_id"] == "broker-mode-conflict"
    assert updates[0][1]["error_text"].startswith(
        "BROKER_OWNED_DURABILITY_GAP:EXECUTION_MODE_IDENTITY_CONFLICT:"
    )
    assert pos.exit_in_flight is True
    assert pos.pending_exit_broker_order_id == "broker-mode-conflict"


def test_callback_local_identity_conflict_holds_without_osm_adoption():
    pos = _guard_pos(pending_exit_local_order_id="exit-local-reserved")
    active_order = {
        "client_id": pos.client_id,
        "kind": "EXIT",
        "local_order_id": "exit-local-reserved",
        "broker_order_id": "",
        "status": "EXIT_REQUESTED",
        "position_id": pos.position_id,
        "qty": 4,
        "execution_mode": "paper",
    }
    osm = _GuardOSM(active_order)
    engine = SimpleNamespace(
        _lock=threading.RLock(),
        client_id=pos.client_id,
        order_state_machine=osm,
        osm=None,
        master_control=SimpleNamespace(mode="PAPER"),
    )

    result = guard._adopt_callback_broker_ownership(
        engine,
        pos,
        {
            "identity": {
                "local_order_id": "different-local-id",
                "broker_order_id": "broker-exact-1",
            },
            "result": None,
        },
    )

    assert result["attempted"] is True
    assert result["adopted"] is False
    assert result["identity_conflict"] is True
    assert result["reason_code"] == "EXIT_BROKER_OWNERSHIP_IDENTITY_CONFLICT"
    assert pos.exit_in_flight is True
    assert pos.pending_exit_local_order_id == "exit-local-reserved"
    assert pos.pending_exit_broker_order_id == "broker-exact-1"
    osm.adopt_broker_owned_exit_request.assert_not_called()


def test_callback_broker_identity_conflict_holds_without_choosing_an_id():
    pos = _guard_pos(pending_exit_local_order_id="exit-local-reserved")
    active_order = {
        "client_id": pos.client_id,
        "kind": "EXIT",
        "local_order_id": "exit-local-reserved",
        "broker_order_id": "broker-active-1",
        "status": "EXIT_REQUESTED",
        "position_id": pos.position_id,
        "qty": 4,
        "execution_mode": "paper",
    }
    osm = _GuardOSM(active_order)
    engine = SimpleNamespace(
        _lock=threading.RLock(),
        client_id=pos.client_id,
        order_state_machine=osm,
        osm=None,
        master_control=SimpleNamespace(mode="PAPER"),
    )

    result = guard._adopt_callback_broker_ownership(
        engine,
        pos,
        {
            "identity": {"broker_order_id": "broker-callback-1"},
            "result": None,
        },
    )

    assert result["attempted"] is True
    assert result["adopted"] is False
    assert result["identity_conflict"] is True
    assert result["broker_order_id"] == ""
    assert result["broker_order_ids"] == ["broker-callback-1", "broker-active-1"]
    assert pos.exit_in_flight is True
    assert pos.pending_exit_local_order_id == "exit-local-reserved"
    assert pos.pending_exit_broker_order_id == ""
    osm.adopt_broker_owned_exit_request.assert_not_called()


def test_submit_wrapper_reconciles_callback_exception_before_reraising(monkeypatch):
    pos = _guard_pos(pending_exit_local_order_id="exit-local-uncertain")
    active_order = {
        "client_id": pos.client_id,
        "kind": "EXIT",
        "local_order_id": "exit-local-uncertain",
        "broker_order_id": "",
        "status": "EXIT_REQUESTED",
        "position_id": pos.position_id,
        "qty": 4,
        "execution_mode": "paper",
        "meta": {"broker_submitted_ts": "2026-08-08T15:00:00Z"},
    }
    osm = _GuardOSM(active_order)
    adoption = osm.adopt_broker_owned_exit_request
    adoption.return_value = {
        "disposition": "ADOPTED",
        "adopted": True,
        "status": "EXIT_SUBMITTED",
        "reason_code": "EXIT_BROKER_OWNERSHIP_ADOPTED_FROM_REQUESTED",
    }

    engine = SimpleNamespace(
        _lock=threading.RLock(),
        client_id=pos.client_id,
        order_state_machine=osm,
        osm=None,
        master_control=SimpleNamespace(mode="PAPER"),
    )

    def callback(callback_pos, _decision):
        callback_pos.exit_in_flight = True
        callback_pos.pending_exit_broker_order_id = "broker-uncertain-1"
        engine.order_state_machine.active_order["broker_order_id"] = "broker-uncertain-1"
        raise RuntimeError("submit response uncertain")

    engine.on_scale = callback
    engine.on_exit = callback
    updates = []
    monkeypatch.setattr(
        guard,
        "_durable_exit_generation",
        lambda *_args: ("tradefluence|position-guard-1|4|1", 1),
    )
    monkeypatch.setattr(
        guard,
        "_claim_durable_decision_generation",
        lambda **_kwargs: {"claimed": True},
    )
    monkeypatch.setattr(
        guard,
        "_update_durable_decision_generation",
        lambda generation_key, **kwargs: updates.append((generation_key, kwargs)),
    )
    wrapped = guard.wrap_submit(
        lambda submit_engine, submit_pos, decision: bool(
            submit_engine.on_scale(submit_pos, decision)
        )
    )

    with pytest.raises(RuntimeError, match="submit response uncertain"):
        wrapped(engine, pos, SimpleNamespace(action="SCALE_OUT", quantity=1, should_act=True))

    adoption.assert_called_once()
    assert adoption.call_args.kwargs["broker_order_id"] == "broker-uncertain-1"
    assert adoption.call_args.kwargs["broker_submitted_ts"] == "2026-08-08T15:00:00Z"
    assert updates[0][1]["claim_state"] == guard._CLAIM_STATE_BROKER_OWNED
    assert updates[0][1]["broker_order_id"] == "broker-uncertain-1"
    assert pos.exit_in_flight is True
    assert pos.pending_exit_broker_order_id == "broker-uncertain-1"


def test_submit_wrapper_broker_identity_conflict_keeps_claim_unresolved(monkeypatch):
    pos = _guard_pos(pending_exit_local_order_id="exit-local-conflict")
    active_order = {
        "client_id": pos.client_id,
        "kind": "EXIT",
        "local_order_id": "exit-local-conflict",
        "broker_order_id": "",
        "status": "EXIT_REQUESTED",
        "position_id": pos.position_id,
        "qty": 4,
        "execution_mode": "paper",
    }
    osm = _GuardOSM(active_order)
    engine = SimpleNamespace(
        _lock=threading.RLock(),
        client_id=pos.client_id,
        order_state_machine=osm,
        osm=None,
        master_control=SimpleNamespace(mode="PAPER"),
    )
    callback_calls = []

    def callback(callback_pos, _decision):
        callback_calls.append(True)
        callback_pos.pending_exit_local_order_id = "callback-local-conflict"
        engine.order_state_machine.active_order["broker_order_id"] = "broker-active-1"
        return {
            "ok": True,
            "accepted": True,
            "status": "EXIT_SUBMITTED",
            "local_order_id": "callback-local-conflict",
            "broker_order_id": "broker-callback-1",
        }

    engine.on_scale = callback
    engine.on_exit = callback
    updates = []
    monkeypatch.setattr(
        guard,
        "_durable_exit_generation",
        lambda *_args: ("tradefluence|position-guard-1|4|1", 1),
    )
    monkeypatch.setattr(
        guard,
        "_claim_durable_decision_generation",
        lambda **_kwargs: {"claimed": True},
    )
    monkeypatch.setattr(
        guard,
        "_update_durable_decision_generation",
        lambda generation_key, **kwargs: updates.append((generation_key, kwargs)),
    )
    wrapped = guard.wrap_submit(
        lambda submit_engine, submit_pos, decision: bool(
            submit_engine.on_scale(submit_pos, decision).get("ok")
        )
    )

    assert wrapped(
        engine,
        pos,
        SimpleNamespace(action="SCALE_OUT", quantity=1, should_act=True),
    ) is True

    assert callback_calls == [True]
    osm.adopt_broker_owned_exit_request.assert_not_called()
    assert updates[0][1]["claim_state"] == guard._CLAIM_STATE_BROKER_OWNED
    assert updates[0][1]["broker_order_id"] == ""
    assert updates[0][1]["local_order_id"] == "exit-local-conflict"
    assert updates[0][1]["error_text"].startswith(
        "BROKER_OWNED_DURABILITY_GAP:EXIT_BROKER_OWNERSHIP_IDENTITY_CONFLICT:"
    )
    assert pos.exit_in_flight is True
    assert pos.pending_exit_local_order_id == "exit-local-conflict"


def _monitor(order, osm, broker, *, client_mode="PAPER"):
    monitor = om.APOrderMonitor(
        client_id="tradefluence",
        broker=broker,
        order_state_machine=osm,
        position_manager=MagicMock(),
        exit_engine=MagicMock(),
        client_mode=client_mode,
    )
    monitor._emit_order_event = MagicMock()
    monitor._alert = MagicMock()
    monitor._get_active_exit_orders = MagicMock(return_value=[dict(order)])
    return monitor


def test_order_monitor_adopts_nonstale_open_without_submit_or_cancel(monkeypatch):
    order = _row(broker_order_id="36661364")
    order["created_ts"] = datetime.now(timezone.utc)
    osm = _MonitorOSM(order)
    broker = _Broker({"status": "OPEN"})
    monitor = _monitor(order, osm, broker)
    monitor._query_broker_order = MagicMock()
    monitor._handle_stale_exit = MagicMock()
    monitor._cancel_broker_order = MagicMock()
    monkeypatch.setattr(om, "TIMEOUT_EXIT_PENDING", 300)

    monitor._check_exit_orders()

    assert len(osm.adopt_calls) == 1
    assert broker.calls == []
    assert osm.transition_calls == []
    monitor._query_broker_order.assert_not_called()
    monitor._handle_stale_exit.assert_not_called()
    monitor._cancel_broker_order.assert_not_called()
    assert monitor._emit_order_event.call_args_list[0].kwargs["reason_code"] == (
        "EXIT_BROKER_OWNERSHIP_ADOPTED_FROM_REQUESTED"
    )


def test_adopted_stale_open_uses_existing_stale_exit_path_once(monkeypatch):
    order = _row(broker_order_id="36661364")
    osm = _MonitorOSM(order)
    monitor = _monitor(order, osm, _Broker({"status": "OPEN"}))
    monitor._query_broker_order = MagicMock(return_value="open")
    monitor._handle_stale_exit = MagicMock()
    monkeypatch.setattr(om, "TIMEOUT_EXIT_PENDING", 0)

    monitor._check_exit_orders()

    assert len(osm.adopt_calls) == 1
    monitor._query_broker_order.assert_called_once_with("36661364")
    monitor._handle_stale_exit.assert_called_once()
    assert monitor._handle_stale_exit.call_args.args[1] == "EXIT_SUBMITTED"
    assert osm.transition_calls == []


@pytest.mark.parametrize("raw_status", ["OPEN", "WORKING"])
def test_existing_stale_exit_submitted_active_broker_status_uses_stale_path(
    monkeypatch, raw_status
):
    order = _row(status="EXIT_SUBMITTED", broker_order_id="36661364")
    osm = _MonitorOSM(order)
    monitor = _monitor(order, osm, _Broker({"status": raw_status}))
    monitor._query_broker_order = MagicMock(return_value=raw_status)
    monitor._handle_stale_exit = MagicMock()
    monkeypatch.setattr(om, "TIMEOUT_EXIT_PENDING", 0)

    monitor._check_exit_orders()

    assert osm.adopt_calls == []
    monitor._query_broker_order.assert_called_once_with("36661364")
    monitor._handle_stale_exit.assert_called_once()
    assert monitor._handle_stale_exit.call_args.args[1] == "EXIT_SUBMITTED"
    assert osm.transition_calls == []


def test_actor_mode_stale_open_exit_keeps_existing_cancel_logic_reachable(monkeypatch):
    order = _row(status="EXIT_SUBMITTED", broker_order_id="36661364")
    osm = _MonitorOSM(order)
    monitor = _monitor(order, osm, _Broker({"status": "OPEN"}))
    monitor._query_broker_order = MagicMock(return_value="open")
    monitor._cancel_broker_order = MagicMock(return_value={"status": "canceled"})
    monitor._guarded_revert_position_open_after_exit_cancel = MagicMock()
    monkeypatch.setattr(om, "TIMEOUT_EXIT_PENDING", 0)
    monkeypatch.setattr(om, "ORDER_MONITOR_CAN_ACT", True)

    monitor._check_exit_orders()

    monitor._cancel_broker_order.assert_called_once_with("36661364")
    assert [call[1] for call in osm.transition_calls] == ["CANCELED"]
    assert monitor._guarded_revert_position_open_after_exit_cancel.call_count == 1
    assert any(
        call.kwargs.get("reason_code") == "STALE_EXIT_TIMEOUT"
        for call in monitor._emit_order_event.call_args_list
    )


def test_watchdog_mode_stale_open_exit_alerts_without_cancel(monkeypatch):
    order = _row(status="EXIT_SUBMITTED", broker_order_id="36661364")
    osm = _MonitorOSM(order)
    monitor = _monitor(order, osm, _Broker({"status": "OPEN"}))
    monitor._query_broker_order = MagicMock(return_value="open")
    monitor._cancel_broker_order = MagicMock()
    monkeypatch.setattr(om, "TIMEOUT_EXIT_PENDING", 0)
    monkeypatch.setattr(om, "ORDER_MONITOR_CAN_ACT", False)

    monitor._check_exit_orders()

    monitor._cancel_broker_order.assert_not_called()
    assert osm.transition_calls == []
    assert any(
        call.kwargs.get("reason_code") == "STALE_EXIT_TIMEOUT"
        for call in monitor._emit_order_event.call_args_list
    )
    assert any("WATCHDOG ONLY" in str(call.args[0]) for call in monitor._alert.call_args_list)


@pytest.mark.parametrize(
    "raw_status",
    ["accepted", "working", "live", "queued", "held", "routed", "new", "pending_review"],
)
def test_generic_entry_aliases_remain_unmapped(raw_status):
    order = _row(kind="ENTRY", status="SUBMITTED", broker_order_id="entry-broker-1")
    osm = _MonitorOSM(order)
    monitor = _monitor(order, osm, _Broker({"status": raw_status}))

    monitor._advance_from_broker_status("exit-orcl-1", raw_status, "ORCL")

    assert osm.transition_calls == []


def test_order_monitor_holds_when_adoption_cas_fails(monkeypatch):
    order = _row(broker_order_id="36661364")
    osm = _MonitorOSM(order, adopt=False)
    broker = _Broker({"status": "FILLED", "exec_quantity": 4})
    monitor = _monitor(order, osm, broker)
    monkeypatch.setattr(om, "TIMEOUT_EXIT_PENDING", 0)

    monitor._check_exit_orders()

    assert len(osm.adopt_calls) == 1
    assert broker.calls == []
    assert osm.transition_calls == []
    assert monitor._emit_order_event.call_args.kwargs["reason_code"] == (
        "BROKER_OWNED_EXIT_REQUEST_RECOVERY_HOLD"
    )


def test_order_monitor_holds_when_runtime_mode_is_not_explicitly_proven(monkeypatch):
    order = _row(broker_order_id="36661364")
    osm = _MonitorOSM(order)
    broker = _Broker({"status": "FILLED", "exec_quantity": 4})
    monitor = _monitor(order, osm, broker, client_mode=None)
    monkeypatch.setattr(om, "TIMEOUT_EXIT_PENDING", 0)

    monitor._check_exit_orders()

    assert osm.adopt_calls == []
    assert broker.calls == []
    assert osm.transition_calls == []
    assert monitor._emit_order_event.call_args.kwargs["reason_code"] == (
        "BROKER_OWNED_EXIT_REQUEST_RECOVERY_HOLD"
    )


def test_order_monitor_does_not_repeat_same_submitted_transition():
    order = _row(status="EXIT_SUBMITTED", broker_order_id="36661364")
    osm = _MonitorOSM(order)
    monitor = _monitor(order, osm, _Broker({"status": "PENDING"}))

    monitor._advance_from_broker_status("exit-orcl-1", "PENDING", "ORCL")

    assert osm.transition_calls == []


def test_fill_monitor_loop_deduplicates_recovery_and_normal_snapshots(monkeypatch):
    recovery = _row(broker_order_id="36661364")
    normal = dict(recovery)
    normal["status"] = "EXIT_SUBMITTED"
    calls = []

    monkeypatch.setattr(fm, "get_pending_orders", lambda _client_id: [normal])
    monkeypatch.setattr(fm, "get_broker_owned_exit_requests", lambda _client_id: [recovery])
    monkeypatch.setattr(
        fm,
        "process_pending_order",
        lambda _broker, order, **kwargs: calls.append((dict(order), kwargs)),
    )

    class _Stop:
        def __init__(self):
            self.wait_calls = 0

        def is_set(self):
            return self.wait_calls > 0

        def wait(self, _seconds):
            self.wait_calls += 1

    stop = _Stop()
    exit_engine = SimpleNamespace(master_control=SimpleNamespace(mode="PAPER"))
    fm.fill_monitor_loop(
        broker=MagicMock(),
        poll_seconds=0,
        osm=SimpleNamespace(client_id="tradefluence"),
        exit_engine=exit_engine,
        stop_event=stop,
        client_id="tradefluence",
    )

    assert len(calls) == 1
    assert calls[0][0]["status"] == "EXIT_REQUESTED"
    assert calls[0][1]["runtime_execution_mode"] == "paper"


def test_idempotency_claim_stays_broker_owned_on_adoption_gap(monkeypatch):
    pos = SimpleNamespace(
        position_id="position-gap",
        client_id="tradefluence",
        ticker="ORCL",
        option_symbol="ORCL260807P00155000",
        side="PUT",
        quantity_remaining=4,
        closed=False,
        exit_in_flight=False,
        pending_exit_local_order_id="exit-gap-1",
        pending_exit_broker_order_id="",
    )
    active_order = {
        "client_id": "tradefluence",
        "local_order_id": "exit-gap-1",
        "position_id": "position-gap",
        "kind": "EXIT",
        "status": "EXIT_REQUESTED",
        "broker_order_id": "",
        "qty": 4,
        "execution_mode": "paper",
    }

    class _GapOSM:
        def get_active_exit_order(self, position_id):
            return dict(active_order) if position_id == "position-gap" else None

        def adopt_broker_owned_exit_request(self, *args, **kwargs):
            return {
                "disposition": "DB_ERROR",
                "adopted": False,
                "reason_code": "EXIT_BROKER_OWNERSHIP_ADOPTION_FAILED",
                "error": "rowcount_unconfirmed",
            }

    engine = SimpleNamespace(
        _lock=threading.RLock(),
        order_state_machine=_GapOSM(),
        osm=None,
        client_id="tradefluence",
        master_control=SimpleNamespace(mode="PAPER"),
    )

    def callback(_pos, _decision):
        return {
            "ok": False,
            "accepted": True,
            "status": "EXIT_SUBMITTED",
            "local_order_id": "exit-gap-1",
            "broker_order_id": "36661364",
        }

    engine.on_scale = callback
    engine.on_exit = callback
    engine._extract_exit_order_identity = lambda result: {
        "accepted": bool(result.get("accepted")),
        "local_order_id": result.get("local_order_id"),
        "broker_order_id": result.get("broker_order_id"),
        "raw_status": result.get("status"),
    }
    updates = []
    monkeypatch.setattr(
        guard,
        "_durable_exit_generation",
        lambda *_args: ("tradefluence|position-gap|4|1", 1),
    )
    monkeypatch.setattr(
        guard,
        "_claim_durable_decision_generation",
        lambda **_kwargs: {"claimed": True},
    )
    monkeypatch.setattr(
        guard,
        "_update_durable_decision_generation",
        lambda generation_key, **kwargs: updates.append((generation_key, kwargs)),
    )

    wrapped = guard.wrap_submit(
        lambda _engine, _pos, _decision: bool(
            _engine.on_scale(_pos, _decision).get("ok")
        )
    )
    result = wrapped(engine, pos, SimpleNamespace(action="SCALE_OUT", quantity=1, should_act=True))

    assert result is False
    assert pos.exit_in_flight is True
    assert pos.pending_exit_local_order_id == "exit-gap-1"
    assert pos.pending_exit_broker_order_id == "36661364"
    assert updates[0][1]["claim_state"] == guard._CLAIM_STATE_BROKER_OWNED
    assert updates[0][1]["broker_order_id"] == "36661364"
    assert updates[0][1]["error_text"].startswith("BROKER_OWNED_DURABILITY_GAP:")
