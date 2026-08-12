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
                expected_position_raw,
                expected_qty_raw,
                broker_match,
            ) = params
            row = self.db.rows.get(str(local_id))
            # Binding audit correction (Blocker 3): the CAS now includes
            # exact position and exact qty unconditionally.
            expected_position = str(expected_position_raw)
            expected_qty = int(expected_qty_raw)

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
                and row.get("position_id") == expected_position
                and int(row.get("qty") or 0) == expected_qty
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
    # F2: an adopted row whose broker acceptance time was never proven must
    # age off the adoption timestamp.  Aging off created_ts made a row
    # recovered today instantly stale and cancel/reprice eligible on the very
    # first monitor pass after recovery.
    assert db.rows["exit-orcl-1"]["meta"]["broker_ownership_stale_age_reference"] == (
        "broker_ownership_adopted_at"
    )
    assert db.rows["exit-orcl-1"]["meta"]["broker_ownership_adopted_at"]
    # No proven acceptance evidence means no round-trippable key is written.
    assert "broker_submitted_ts" not in db.rows["exit-orcl-1"]["meta"]
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
        "submitted_ts"
    )
    # F2: the recovery predicates read meta->>'broker_submitted_ts'.  Proven
    # acceptance evidence must round trip through that exact key, or the
    # read and write sides stay permanently disjoint.
    assert db.rows["exit-orcl-1"]["meta"]["broker_submitted_ts"] == (
        "2026-08-08T15:00:00+00:00"
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


@pytest.mark.parametrize(
    ("field", "durable_value"),
    [
        ("client_id", " tradefluence "),
        ("execution_mode", " paper "),
        ("position_id", " position-orcl-1 "),
        ("kind", "exit"),
    ],
)
def test_post_cas_reread_does_not_normalize_durable_identity(
    fake_osm_db, field, durable_value
):
    db, osm = fake_osm_db
    db.rows["exit-orcl-1"].update(
        status="EXIT_SUBMITTED",
        broker_order_id="36661364",
        **{field: durable_value},
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
    assert db.rows["exit-orcl-1"]["status"] == "EXIT_SUBMITTED"


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
    osm._handle_exit_engine_hooks.assert_not_called()


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
            "transaction_date": "2026-08-10T19:00:00+00:00",
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
            "transaction_date": "2026-08-10T19:00:00+00:00",
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

        def cancel_order(self, broker_order_id):
            self.calls.append(f"cancel:{broker_order_id}")
            return {"status": "canceled"}

        def submit_order(self, *args, **kwargs):
            self.calls.append("submit")
            return {"status": "accepted"}

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

        def submit_order(self, *args, **kwargs):
            self.calls.append("submit")
            return {"status": "accepted"}

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


@pytest.mark.parametrize("raw_status", ["UNKNOWN", "BROKER_MAYBE"])
def test_order_monitor_unproven_broker_status_never_reaches_cancel(
    monkeypatch, raw_status
):
    broker_id = f"unproven-{raw_status.lower()}-425"
    order = _row(broker_order_id=broker_id)
    osm = _MonitorOSM(order)

    class _UnprovenStatusBroker(_Broker):
        def cancel_order(self, broker_order_id):
            self.calls.append(f"cancel:{broker_order_id}")
            return {"status": "canceled"}

        def submit_order(self, *args, **kwargs):
            self.calls.append("submit")
            return {"status": "accepted"}

    broker = _UnprovenStatusBroker({"status": raw_status})
    monitor = _monitor(order, osm, broker)
    monkeypatch.setattr(om, "TIMEOUT_EXIT_PENDING", 0)
    monkeypatch.setattr(om, "ORDER_MONITOR_CAN_ACT", True)

    monitor._check_exit_orders()

    assert broker.calls == [broker_id]
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
    def __init__(self, *, position_id="position-orcl-1", quantity_remaining=4):
        self.client_id = "tradefluence"
        self.pending_calls = []
        self.partial_calls = []
        self.closed_calls = []
        self.position = SimpleNamespace(
            position_id=position_id,
            quantity_remaining=quantity_remaining,
        )

    def get_position(self, position_id):
        return self.position if position_id == self.position.position_id else None

    def set_pending_exit_order(self, position_id, **kwargs):
        self.pending_calls.append((position_id, kwargs))

    def note_partial_exit_fill(self, position_id, delta, **kwargs):
        self.partial_calls.append((position_id, delta, kwargs))
        self.position.quantity_remaining = max(
            0,
            self.position.quantity_remaining - int(delta or 0),
        )

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
            "transaction_date": "2026-08-10T19:00:00+00:00",
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


def test_partial_exit_replay_preserves_exact_exit_quantity_and_remaining_position(
    fake_osm_db, monkeypatch
):
    db, osm = fake_osm_db
    db.rows["exit-orcl-1"].update(
        broker_order_id="partial-broker-425",
        position_id="position-partial-1",
        qty=3,
    )
    engine = _CanonicalExitEngine(
        position_id="position-partial-1",
        quantity_remaining=10,
    )
    monkeypatch.setitem(osm_module._exit_engine_registry, "tradefluence", engine)
    osm._handle_exit_engine_hooks = APOrderStateMachine._handle_exit_engine_hooks.__get__(
        osm, APOrderStateMachine
    )
    osm._finalize_position_from_exit_order = MagicMock()
    broker = _Broker(
        {
            "status": "FILLED",
            "exec_quantity": 3,
            "avg_fill_price": 1.25,
            "quantity": 3,
            "transaction_date": "2026-08-10T19:00:00+00:00",
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

    assert broker.calls == ["partial-broker-425"]
    assert len(engine.pending_calls) == 1
    assert engine.pending_calls[0][1]["qty"] == 3
    assert [(call[0], call[1]) for call in engine.partial_calls] == [
        ("position-partial-1", 3)
    ]
    assert engine.position.quantity_remaining == 7
    assert engine.closed_calls == []
    osm._finalize_position_from_exit_order.assert_not_called()


def test_orcl_replay_proves_real_postgres_cas_and_single_economic_fill(monkeypatch):
    """Two full reducers race one recovered EXIT; Postgres permits one economy."""
    from contextlib import contextmanager
    import uuid

    from ap import position_manager as pm_module

    in_github_actions = os.getenv("GITHUB_ACTIONS", "").strip().lower() == "true"
    try:
        import psycopg2
        import psycopg2.extras as extras
    except ImportError:
        if in_github_actions:
            raise
        pytest.skip("psycopg2 is unavailable outside GitHub Actions")
    database_url = os.getenv("DATABASE_URL", "")
    if not database_url:
        if in_github_actions:
            pytest.fail("DATABASE_URL must be configured in GitHub Actions")
        pytest.skip("DATABASE_URL not configured")

    try:
        admin = psycopg2.connect(database_url, connect_timeout=2)
    except Exception as exc:
        if in_github_actions:
            raise AssertionError(
                "GitHub Actions PostgreSQL service is unavailable"
            ) from exc
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
            cursor.execute(f"""
                CREATE TABLE "{schema}".positions (
                    id TEXT PRIMARY KEY,
                    client_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    quantity_remaining INTEGER NOT NULL,
                    qty INTEGER NOT NULL,
                    avg_fill NUMERIC NOT NULL,
                    entry_price NUMERIC NOT NULL,
                    contract TEXT NOT NULL,
                    underlying TEXT NOT NULL,
                    ticker TEXT NOT NULL,
                    side TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    entry_ts TIMESTAMPTZ NOT NULL,
                    exit_ts TIMESTAMPTZ,
                    exit_price NUMERIC,
                    realized_pnl NUMERIC,
                    realized_pnl_pct NUMERIC,
                    exit_reason TEXT,
                    close_source TEXT,
                    close_confidence TEXT,
                    execution_mode TEXT NOT NULL,
                    local_order_id TEXT,
                    broker_order_id TEXT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cursor.execute(f"""
                CREATE TABLE "{schema}".proof_trades (
                    id BIGSERIAL PRIMARY KEY,
                    client_email TEXT NOT NULL,
                    system_version TEXT NOT NULL DEFAULT 'v2',
                    synthetic_entry BOOLEAN NOT NULL DEFAULT FALSE,
                    position_id TEXT NOT NULL,
                    local_order_id TEXT,
                    exit_option_price NUMERIC,
                    option_pnl_pct NUMERIC,
                    win BOOLEAN,
                    exit_reason TEXT,
                    UNIQUE (client_email, position_id)
                )
            """)
            cursor.execute(f"""
                CREATE TABLE "{schema}".exit_filled_lifecycle_events (
                    id BIGSERIAL PRIMARY KEY,
                    local_order_id TEXT NOT NULL,
                    old_status TEXT NOT NULL,
                    new_status TEXT NOT NULL,
                    filled_qty INTEGER NOT NULL,
                    broker_order_id TEXT NOT NULL
                )
            """)
            cursor.execute(f"""
                CREATE FUNCTION "{schema}".record_exit_filled_transition()
                RETURNS TRIGGER LANGUAGE plpgsql AS $fn$
                BEGIN
                    IF OLD.status IS DISTINCT FROM NEW.status
                       AND NEW.status = 'EXIT_FILLED' THEN
                        INSERT INTO "{schema}".exit_filled_lifecycle_events
                            (local_order_id, old_status, new_status, filled_qty,
                             broker_order_id)
                        VALUES
                            (NEW.local_order_id, OLD.status, NEW.status,
                             NEW.filled_qty, NEW.broker_order_id);
                    END IF;
                    RETURN NEW;
                END;
                $fn$
            """)
            cursor.execute(f"""
                CREATE TRIGGER record_exit_filled_transition
                AFTER UPDATE ON "{schema}".orders
                FOR EACH ROW EXECUTE FUNCTION
                    "{schema}".record_exit_filled_transition()
            """)
            cursor.execute(f"""
                CREATE TABLE "{schema}".position_economic_finalizations (
                    id BIGSERIAL PRIMARY KEY,
                    position_id TEXT NOT NULL,
                    old_quantity_remaining INTEGER NOT NULL,
                    new_quantity_remaining INTEGER NOT NULL,
                    old_status TEXT NOT NULL,
                    new_status TEXT NOT NULL,
                    broker_order_id TEXT
                )
            """)
            cursor.execute(f"""
                CREATE FUNCTION "{schema}".record_position_finalization()
                RETURNS TRIGGER LANGUAGE plpgsql AS $fn$
                BEGIN
                    IF OLD.quantity_remaining IS DISTINCT FROM NEW.quantity_remaining
                       OR OLD.status IS DISTINCT FROM NEW.status THEN
                        INSERT INTO "{schema}".position_economic_finalizations
                            (position_id, old_quantity_remaining,
                             new_quantity_remaining, old_status, new_status,
                             broker_order_id)
                        VALUES
                            (NEW.id, OLD.quantity_remaining,
                             NEW.quantity_remaining, OLD.status, NEW.status,
                             NEW.broker_order_id);
                    END IF;
                    RETURN NEW;
                END;
                $fn$
            """)
            cursor.execute(f"""
                CREATE TRIGGER record_position_finalization
                AFTER UPDATE ON "{schema}".positions
                FOR EACH ROW EXECUTE FUNCTION
                    "{schema}".record_position_finalization()
            """)
            cursor.execute(
                f"""
                INSERT INTO "{schema}".positions
                    (id, client_id, status, quantity_remaining, qty, avg_fill,
                     entry_price, contract, underlying, ticker, side, direction,
                     entry_ts, execution_mode, local_order_id)
                VALUES
                    ('position-orcl-1', 'tradefluence', 'OPEN', 4, 4, 1.00,
                     1.00, 'ORCL260807P00155000', 'ORCL', 'ORCL', 'PUT', 'PUT',
                     '2026-08-08T13:30:00Z', 'paper', 'entry-orcl-425')
                """
            )
            cursor.execute(
                f"""
                INSERT INTO "{schema}".orders
                    (local_order_id, client_id, position_id, kind, status,
                     broker_order_id, execution_mode, qty, symbol, contract)
                VALUES
                    ('exit-orcl-real-pg', 'tradefluence', 'position-orcl-1',
                     'EXIT', 'EXIT_REQUESTED', '36661364', 'paper', 4, 'ORCL',
                     'ORCL260807P00155000')
                """
            )

        monkeypatch.setattr(osm_module, "conn", _pg_conn)
        monkeypatch.setattr(osm_module, "run_with_retry", lambda fn, **kwargs: fn())
        monkeypatch.setattr(pm_module, "conn", _pg_conn)
        monkeypatch.setattr(pm_module, "run_with_retry", lambda fn, **kwargs: fn())
        position_columns = {
            "id", "client_id", "status", "quantity_remaining", "qty",
            "avg_fill", "entry_price", "contract", "underlying", "ticker",
            "side", "direction", "entry_ts", "exit_ts", "exit_price",
            "realized_pnl", "realized_pnl_pct", "exit_reason", "close_source",
            "close_confidence", "execution_mode", "local_order_id",
            "broker_order_id", "updated_at",
        }
        monkeypatch.setattr(
            pm_module.APPositionManager,
            "_position_columns",
            lambda self: set(position_columns),
        )

        # Keep proof publication at its external boundary while the canonical
        # orders -> positions economic reducer remains completely real.  The
        # durable test proof uses the production position identity and a unique
        # key, so duplicate economic accounting is directly queryable.
        def _persist_test_terminal_proof(self, **kwargs):
            with _pg_conn() as connection:
                connection.execute(
                    "INSERT INTO proof_trades "
                    "(client_email, position_id, local_order_id, "
                    " exit_option_price, option_pnl_pct, win, exit_reason) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (client_email, position_id) DO NOTHING",
                    (
                        self.client_id,
                        kwargs["position_id"],
                        kwargs.get("local_order_id"),
                        kwargs.get("exit_option_price"),
                        kwargs.get("option_pnl_pct"),
                        float(kwargs.get("option_pnl_pct") or 0) > 0,
                        kwargs.get("exit_reason"),
                    ),
                )
            return True

        monkeypatch.setattr(
            pm_module.APPositionManager,
            "_ensure_terminal_close_proof",
            _persist_test_terminal_proof,
        )
        engine = _CanonicalExitEngine()
        monkeypatch.setitem(osm_module._exit_engine_registry, "tradefluence", engine)

        osm_a = APOrderStateMachine("tradefluence")
        osm_b = APOrderStateMachine("tradefluence")
        for state_machine in (osm_a, osm_b):
            state_machine._emit_transition_event = lambda **kwargs: None

        monkeypatch.setattr(
            "ap.performance_tracker.record_trade_outcome_from_position",
            lambda *_args, **_kwargs: None,
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

        broker_truth_barrier = threading.Barrier(2)
        money_path = {"get": [], "post": [], "cancel": [], "delete": []}

        class _ConcurrentRecoveryBroker:
            def get_order(self, broker_order_id):
                money_path["get"].append(str(broker_order_id))
                broker_truth_barrier.wait(timeout=10)
                return {
                    "status": "FILLED",
                    "exec_quantity": 4,
                    "avg_fill_price": 1.25,
                    "quantity": 4,
                }

            def submit_order(self, *args, **kwargs):
                money_path["post"].append((args, kwargs))
                raise AssertionError("recovery must never submit a replacement EXIT")

            def cancel_order(self, *args, **kwargs):
                money_path["cancel"].append((args, kwargs))
                raise AssertionError("recovery must never cancel the owned EXIT")

            def delete_order(self, *args, **kwargs):
                money_path["delete"].append((args, kwargs))
                raise AssertionError("recovery must never delete the owned EXIT")

        worker_errors = []
        worker_start = threading.Barrier(2)

        def _process(state_machine):
            try:
                worker_start.wait(timeout=10)
                fm.process_pending_order(
                    _ConcurrentRecoveryBroker(),
                    dict(original_order),
                    osm=state_machine,
                    runtime_execution_mode="paper",
                )
            except Exception as exc:
                worker_errors.append(exc)

        threads = [
            threading.Thread(target=_process, args=(osm_a,)),
            threading.Thread(target=_process, args=(osm_b,)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=20)

        assert worker_errors == []
        assert all(not thread.is_alive() for thread in threads)

        with _pg_conn() as connection:
            connection.execute(
                "SELECT status, broker_order_id, client_id, execution_mode, "
                "position_id, qty, filled_qty, fill_price FROM orders "
                "WHERE local_order_id=%s AND client_id=%s",
                ("exit-orcl-real-pg", "tradefluence"),
            )
            filled_row = dict(connection.fetchone())
            connection.execute(
                "SELECT * FROM positions WHERE id=%s AND client_id=%s",
                ("position-orcl-1", "tradefluence"),
            )
            position_row = dict(connection.fetchone())
            connection.execute(
                "SELECT * FROM exit_filled_lifecycle_events "
                "WHERE local_order_id=%s ORDER BY id",
                ("exit-orcl-real-pg",),
            )
            lifecycle_events = [dict(row) for row in connection.fetchall()]
            connection.execute(
                "SELECT * FROM position_economic_finalizations "
                "WHERE position_id=%s ORDER BY id",
                ("position-orcl-1",),
            )
            economic_events = [dict(row) for row in connection.fetchall()]
            connection.execute(
                "SELECT COUNT(*) AS count FROM proof_trades "
                "WHERE client_email=%s AND position_id=%s",
                ("tradefluence", "position-orcl-1"),
            )
            proof_count = int(connection.fetchone()["count"])
            connection.execute(
                "SELECT COUNT(*) AS count FROM orders "
                "WHERE client_id=%s AND position_id=%s AND kind='EXIT'",
                ("tradefluence", "position-orcl-1"),
            )
            exit_count = int(connection.fetchone()["count"])

        assert money_path["get"] == ["36661364", "36661364"]
        assert money_path["post"] == []
        assert money_path["cancel"] == []
        assert money_path["delete"] == []
        assert filled_row == {
            "status": "EXIT_FILLED",
            "broker_order_id": "36661364",
            "client_id": "tradefluence",
            "execution_mode": "paper",
            "position_id": "position-orcl-1",
            "qty": 4,
            "filled_qty": 4,
            "fill_price": filled_row["fill_price"],
        }
        assert float(filled_row["fill_price"]) == 1.25
        assert len(lifecycle_events) == 1
        assert lifecycle_events[0]["old_status"] == "EXIT_SUBMITTED"
        assert lifecycle_events[0]["new_status"] == "EXIT_FILLED"
        assert lifecycle_events[0]["filled_qty"] == 4
        assert lifecycle_events[0]["broker_order_id"] == "36661364"
        assert position_row["status"] == "CLOSED"
        assert position_row["quantity_remaining"] == 0
        assert position_row["quantity_remaining"] >= 0
        assert position_row["qty"] == 4
        assert position_row["client_id"] == "tradefluence"
        assert position_row["execution_mode"] == "paper"
        assert position_row["broker_order_id"] == "36661364"
        assert len(economic_events) == 1
        assert economic_events[0]["old_quantity_remaining"] == 4
        assert economic_events[0]["new_quantity_remaining"] == 0
        assert economic_events[0]["old_status"] == "OPEN"
        assert economic_events[0]["new_status"] == "CLOSED"
        assert proof_count == 1
        assert exit_count == 1
        assert len(engine.pending_calls) == 1
        assert len(engine.closed_calls) == 1
        assert engine.position.quantity_remaining == 0

        # Sequential restart/replay uses the same full processing path.  The
        # terminal adoption reread must stop before another broker poll or any
        # economic mutation.
        class _ReplayBroker(_ConcurrentRecoveryBroker):
            def get_order(self, broker_order_id):
                raise AssertionError("terminal replay must not poll the broker")

        replay_osm = APOrderStateMachine("tradefluence")
        replay_osm._emit_transition_event = lambda **kwargs: None
        fm.process_pending_order(
            _ReplayBroker(),
            dict(original_order),
            osm=replay_osm,
            runtime_execution_mode="paper",
        )

        with _pg_conn() as connection:
            connection.execute(
                "SELECT status, filled_qty FROM orders WHERE local_order_id=%s",
                ("exit-orcl-real-pg",),
            )
            replay_order = dict(connection.fetchone())
            connection.execute(
                "SELECT status, quantity_remaining FROM positions WHERE id=%s",
                ("position-orcl-1",),
            )
            replay_position = dict(connection.fetchone())
            connection.execute(
                "SELECT COUNT(*) AS count FROM exit_filled_lifecycle_events "
                "WHERE local_order_id=%s",
                ("exit-orcl-real-pg",),
            )
            replay_lifecycle_count = int(connection.fetchone()["count"])
            connection.execute(
                "SELECT COUNT(*) AS count FROM position_economic_finalizations "
                "WHERE position_id=%s",
                ("position-orcl-1",),
            )
            replay_economic_count = int(connection.fetchone()["count"])
            connection.execute(
                "SELECT COUNT(*) AS count FROM proof_trades "
                "WHERE client_email=%s AND position_id=%s",
                ("tradefluence", "position-orcl-1"),
            )
            replay_proof_count = int(connection.fetchone()["count"])

        assert replay_order == {"status": "EXIT_FILLED", "filled_qty": 4}
        assert replay_position == {"status": "CLOSED", "quantity_remaining": 0}
        assert replay_lifecycle_count == 1
        assert replay_economic_count == 1
        assert replay_proof_count == 1
        assert money_path["post"] == []
        assert money_path["cancel"] == []
        assert money_path["delete"] == []
        assert len(engine.closed_calls) == 1
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

    def get_order(self, local_order_id):
        if str(self.active_order.get("local_order_id") or "") == str(local_order_id or ""):
            return self.active_order
        return None

    def retire_unsubmitted_exit_intent(self, local_order_id, *, last_error):
        meta = dict(self.active_order.get("meta") or {})
        if (
            str(self.active_order.get("local_order_id") or "") != str(local_order_id or "")
            or self.active_order.get("client_id") != "tradefluence"
            or self.active_order.get("kind") != "EXIT"
            or self.active_order.get("status") != "EXIT_REQUESTED"
            or str(self.active_order.get("broker_order_id") or "")
            or self.active_order.get("submitted_ts")
            or str(meta.get("submit_intent_at") or "")
        ):
            return False
        self.active_order["status"] = "ERROR"
        self.active_order["last_error"] = last_error
        return True


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

    assert wrapped(engine, pos, SimpleNamespace(action="SCALE_OUT", quantity=4, should_act=True)) is False
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

    assert wrapped(engine, pos, SimpleNamespace(action="SCALE_OUT", quantity=4, should_act=True)) is False
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

    assert wrapped(engine, pos, SimpleNamespace(action="SCALE_OUT", quantity=4, should_act=True)) is False
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

    assert wrapped(engine, pos, SimpleNamespace(action="SCALE_OUT", quantity=4, should_act=True)) is False
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

    assert wrapped(engine, pos, SimpleNamespace(action="SCALE_OUT", quantity=4, should_act=True)) is False
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

    assert wrapped(engine, pos, SimpleNamespace(action="SCALE_OUT", quantity=4, should_act=True)) is False
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

    assert wrapped(engine, pos, SimpleNamespace(action="SCALE_OUT", quantity=4, should_act=True)) is True
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

    assert wrapped(engine, pos, SimpleNamespace(action="SCALE_OUT", quantity=4, should_act=True)) is True
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
        wrapped(engine, pos, SimpleNamespace(action="SCALE_OUT", quantity=4, should_act=True))

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
        "qty": 1,
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
    exit_engine = SimpleNamespace(master_control=SimpleNamespace(mode="paper"))
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
        "qty": 1,
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


# =====================================================================
# 2026-08-08 amendment — audit findings F1, F2, F3, F4, F6, F7
# =====================================================================


def _adopted_row(**overrides):
    """A row already recovered by this PR in a previous monitor pass."""
    meta = {
        "original_failure": "submit_transition_failed",
        "broker_ownership_adopted_from_exit_requested": True,
        "broker_ownership_adopted_at": (
            datetime.now(timezone.utc) - timedelta(seconds=30)
        ).isoformat(),
    }
    meta.update(overrides.pop("meta", {}) or {})
    return _row(
        status="EXIT_SUBMITTED",
        broker_order_id="36661364",
        meta=meta,
        **overrides,
    )


@pytest.mark.parametrize("unmapped_status", ["held", "calculated"])
def test_f1_unmapped_broker_status_still_reaches_stale_handling_for_unowned_rows(
    monkeypatch, unmapped_status
):
    """F1: authoritative-but-unmapped broker truth is not a lookup failure.

    Rows this PR did not recover must keep their pre-existing stale-exit
    liveness.  Treating every unrecognized string as unknown silently
    converted real Tradier states into permanent holds with no cancel,
    reprice, or retry — a regression on money-at-risk exits and an invasion
    of the surface owned by #423.
    """
    order = _row(status="EXIT_SUBMITTED", broker_order_id="unowned-425")
    osm = _MonitorOSM(order)
    broker = _Broker({"status": unmapped_status})
    monitor = _monitor(order, osm, broker)
    monitor._handle_stale_exit = MagicMock()
    monkeypatch.setattr(om, "TIMEOUT_EXIT_PENDING", 0)
    monkeypatch.setattr(om, "ORDER_MONITOR_CAN_ACT", True)

    monitor._check_exit_orders()

    monitor._handle_stale_exit.assert_called_once()
    assert not any(
        call.kwargs.get("reason_code") == "BROKER_STATUS_UNKNOWN"
        for call in monitor._emit_order_event.call_args_list
    )


@pytest.mark.parametrize("recognized_status", ["held", "calculated"])
def test_blocker4_recognized_active_states_preserve_stale_liveness_on_recovered_rows(
    monkeypatch, recognized_status
):
    """Binding Blocker 4: recovery provenance does not destroy liveness.

    A recognized active broker state (``working``, ``held``, ``calculated``,
    ``accepted``, ``new``) reaches the pre-existing stale-exit management
    path even when the row was recovered by this PR.  Only truly unproven
    broker truth (``None``, ``error``, ``unavailable``) fences.
    """
    order = _adopted_row()
    osm = _MonitorOSM(order)
    broker = _Broker({"status": recognized_status})
    monitor = _monitor(order, osm, broker)
    monitor._handle_stale_exit = MagicMock()
    monkeypatch.setattr(om, "TIMEOUT_EXIT_PENDING", 0)
    monkeypatch.setattr(om, "ORDER_MONITOR_CAN_ACT", True)

    monitor._check_exit_orders()

    monitor._handle_stale_exit.assert_called_once()
    assert not any(
        call.kwargs.get("reason_code") == "BROKER_STATUS_UNKNOWN"
        for call in monitor._emit_order_event.call_args_list
    )


@pytest.mark.parametrize("recognized_status", ["working", "accepted", "new"])
def test_blocker4_working_liveness_matches_ordinary_and_recovered(
    monkeypatch, recognized_status
):
    """Blocker 4 explicit contract: same durable state + same broker truth
    -> same existing stale-exit management path, regardless of provenance."""
    ordinary = _row(status="EXIT_SUBMITTED", broker_order_id="ordinary-425")
    ordinary_osm = _MonitorOSM(ordinary)
    ordinary_monitor = _monitor(ordinary, ordinary_osm, _Broker({"status": recognized_status}))
    ordinary_monitor._handle_stale_exit = MagicMock()

    recovered = _adopted_row()
    recovered_osm = _MonitorOSM(recovered)
    recovered_monitor = _monitor(recovered, recovered_osm, _Broker({"status": recognized_status}))
    recovered_monitor._handle_stale_exit = MagicMock()

    monkeypatch.setattr(om, "TIMEOUT_EXIT_PENDING", 0)
    monkeypatch.setattr(om, "ORDER_MONITOR_CAN_ACT", True)

    ordinary_monitor._check_exit_orders()
    recovered_monitor._check_exit_orders()

    ordinary_monitor._handle_stale_exit.assert_called_once()
    recovered_monitor._handle_stale_exit.assert_called_once()


def test_f1_lookup_failure_holds_only_for_broker_owned_recovery_rows(monkeypatch):
    """F1: a failed lookup fences rows this PR owns and nothing else.

    Holding on lookup failure is correct for a row we adopted, but applying
    it to every exit row would be a behavior change against main on the
    stale working-exit surface owned by #423.  This PR must leave that
    surface byte-identical.
    """
    owned = _adopted_row()
    osm = _MonitorOSM(owned)
    monitor = _monitor(owned, osm, _Broker({}))
    monitor._query_broker_order = MagicMock(return_value=None)
    monitor._handle_stale_exit = MagicMock()
    monitor._cancel_broker_order = MagicMock()
    monkeypatch.setattr(om, "TIMEOUT_EXIT_PENDING", 0)
    monkeypatch.setattr(om, "ORDER_MONITOR_CAN_ACT", True)

    monitor._check_exit_orders()

    monitor._handle_stale_exit.assert_not_called()
    monitor._cancel_broker_order.assert_not_called()
    assert any(
        call.kwargs.get("reason_code") == "BROKER_STATUS_UNKNOWN"
        for call in monitor._emit_order_event.call_args_list
    )


def test_f1_lookup_failure_preserves_main_behavior_for_unowned_rows(monkeypatch):
    """F1: #423's surface is unchanged — a failed lookup still escalates."""
    unowned = _row(status="EXIT_SUBMITTED", broker_order_id="unowned-425")
    osm = _MonitorOSM(unowned)
    monitor = _monitor(unowned, osm, _Broker({}))
    monitor._query_broker_order = MagicMock(return_value=None)
    monitor._handle_stale_exit = MagicMock()
    monkeypatch.setattr(om, "TIMEOUT_EXIT_PENDING", 0)
    monkeypatch.setattr(om, "ORDER_MONITOR_CAN_ACT", True)

    monitor._check_exit_orders()

    monitor._handle_stale_exit.assert_called_once()
    assert not any(
        call.kwargs.get("reason_code") == "BROKER_STATUS_UNKNOWN"
        for call in monitor._emit_order_event.call_args_list
    )


def test_f2_adopted_row_ages_off_adoption_timestamp_not_created_ts(monkeypatch):
    """F2: an adopted row must not be instantly stale after recovery.

    Adoption leaves ``submitted_ts`` NULL when acceptance time was never
    proven.  Aging off ``created_ts`` made a row recovered today look days
    stale on the very first pass and therefore cancel/reprice eligible.
    """
    order = _adopted_row()
    order["created_ts"] = datetime.now(timezone.utc) - timedelta(days=3)
    osm = _MonitorOSM(order)
    broker = _Broker({"status": "open"})
    monitor = _monitor(order, osm, broker)
    monitor._handle_stale_exit = MagicMock()
    monitor._cancel_broker_order = MagicMock()
    # 5 minutes: far below the 3-day created_ts age, far above the 30s
    # adoption age.  Only the adoption reference keeps this row non-stale.
    monkeypatch.setattr(om, "TIMEOUT_EXIT_PENDING", 300)
    monkeypatch.setattr(om, "ORDER_MONITOR_CAN_ACT", True)

    monitor._check_exit_orders()

    monitor._handle_stale_exit.assert_not_called()
    monitor._cancel_broker_order.assert_not_called()


def test_f2_adoption_marker_survives_json_encoded_meta():
    """F2: meta arrives as a JSON string from some query paths."""
    encoded = _adopted_row()
    encoded["meta"] = json.dumps(encoded["meta"])
    assert om._is_broker_ownership_adopted_row(encoded) is True
    assert om._broker_ownership_adopted_at(encoded)
    assert om._is_broker_ownership_adopted_row({"meta": "not json"}) is False
    assert om._is_broker_ownership_adopted_row({}) is False


def test_f3_runtime_mode_resolves_per_iteration(monkeypatch):
    """F3: a mode hydrated after loop start must not be pinned to unknown.

    Resolving the fence once before the loop stranded every recovery for the
    life of the process whenever master_control was not yet wired at startup
    — reproducing the exact condition this PR exists to close.
    """
    exit_engine = SimpleNamespace(master_control=SimpleNamespace(mode=None))
    observed: list[str] = []

    monkeypatch.setattr(fm, "get_pending_orders", lambda client_id: [])
    monkeypatch.setattr(fm, "get_broker_owned_exit_requests", lambda client_id: [
        _row(broker_order_id="36661364")
    ])

    stop_event = threading.Event()

    def _fake_process(broker, order, **kwargs):
        observed.append(kwargs.get("runtime_execution_mode"))
        # Hydrate the runtime mode only after the first pass.
        exit_engine.master_control.mode = "paper"
        if len(observed) >= 2:
            stop_event.set()

    monkeypatch.setattr(fm, "process_pending_order", _fake_process)

    fm.fill_monitor_loop(
        MagicMock(),
        poll_seconds=0.01,
        client_id="tradefluence",
        osm=MagicMock(),
        exit_engine=exit_engine,
        stop_event=stop_event,
    )

    assert observed[0] == ""
    assert observed[1] == "paper"


def test_fill_monitor_does_not_launder_malformed_explicit_mode_on_replay(
    monkeypatch,
):
    """A malformed present source must HOLD on every loop iteration."""
    exit_engine = SimpleNamespace(
        master_control=SimpleNamespace(
            mode="PAPER",
            runtime_execution_mode="paper",
        )
    )
    observed = []
    stop_event = threading.Event()

    monkeypatch.setattr(fm, "get_pending_orders", lambda client_id: [])
    monkeypatch.setattr(
        fm,
        "get_broker_owned_exit_requests",
        lambda client_id: [_row(broker_order_id="36661364")],
    )

    def _fake_process(broker, order, **kwargs):
        observed.append(kwargs.get("runtime_execution_mode"))
        if len(observed) >= 2:
            stop_event.set()

    monkeypatch.setattr(fm, "process_pending_order", _fake_process)

    fm.fill_monitor_loop(
        MagicMock(),
        poll_seconds=0.01,
        client_id="tradefluence",
        osm=MagicMock(),
        exit_engine=exit_engine,
        stop_event=stop_event,
        runtime_execution_mode="PAPER",
    )

    assert observed == ["", ""]


def test_f6_whitespace_padded_client_id_binds_the_normalized_value(fake_osm_db):
    """F6: the CAS must bind the same client id the guard validated."""
    db, osm = fake_osm_db
    osm.client_id = " tradefluence "

    result = osm.adopt_broker_owned_exit_request(
        "exit-orcl-1",
        broker_order_id="36661364",
        execution_mode="paper",
        client_id=" tradefluence ",
        position_id="position-orcl-1",
        expected_qty=4,
    )

    # Before F6 the guard compared the stripped id while the CAS bound the
    # raw padded one and the authoritative reload bound a third variant.
    # A money-path mutation must not proceed on a non-canonical identity, so
    # adoption fails closed and leaves the durable row untouched.
    assert result["disposition"] == "IDENTITY_MISMATCH"
    assert result["adopted"] is False
    assert result["error"] == "invalid_recovery_identity"
    assert db.rows["exit-orcl-1"]["status"] == "EXIT_REQUESTED"
    assert not db.rows["exit-orcl-1"].get("broker_order_id")


def test_f6_canonical_client_id_still_adopts(fake_osm_db):
    """F6: the fail-closed check must not block the normal canonical path."""
    db, osm = fake_osm_db

    result = osm.adopt_broker_owned_exit_request(
        "exit-orcl-1",
        broker_order_id="36661364",
        execution_mode="paper",
        client_id="tradefluence",
        position_id="position-orcl-1",
        expected_qty=4,
    )

    assert result["disposition"] == "ADOPTED"
    assert db.rows["exit-orcl-1"]["status"] == OrderStatus.EXIT_SUBMITTED


def test_f7_recovered_fill_keeps_canonical_exit_filled_reason_code(monkeypatch):
    """F7: post-adoption fills must stay visible to EXIT_FILLED consumers."""
    order = _row(broker_order_id="36661364")
    osm = _MonitorOSM(order)
    emitted = []
    monkeypatch.setattr(
        fm, "emit_fill_event",
        lambda o, **kw: emitted.append(kw) or True,
    )
    monkeypatch.setattr(
        fm, "check_order_with_broker",
        lambda broker, o: {"status": "EXIT_FILLED", "filled_qty": 4, "avg_fill": 1.25},
    )
    monkeypatch.setattr(fm, "reduce_position_on_fill", MagicMock(), raising=False)

    fm.process_pending_order(
        MagicMock(),
        dict(order),
        osm=osm,
        exit_engine=SimpleNamespace(master_control=SimpleNamespace(mode="paper")),
        runtime_execution_mode="paper",
    )

    fill_events = [e for e in emitted if e.get("reason_code") == "EXIT_FILLED"]
    assert fill_events, f"no canonical EXIT_FILLED emitted: {emitted}"
    assert fill_events[-1]["extra_context"]["broker_owned_exit_request_recovered"] is True
    assert not any(
        e.get("reason_code") == "EXIT_REQUESTED_BROKER_FILLED_RECOVERED"
        for e in emitted
    )


def test_f4_concurrent_adoption_produces_exactly_one_transition_and_hook(fake_osm_db):
    """F4: only the CAS winner may emit the transition and hydrate the owner.

    ``ALREADY_BROKER_OWNED_ACTIVE`` deliberately reports ``adopted=True`` so
    the loser's caller can proceed to canonical broker polling, but it must
    not replay the transition event or the exit-engine hook.  Without this,
    two workers racing the same stranded row would double-hydrate ownership.
    """
    db, osm = fake_osm_db
    barrier = threading.Barrier(2)
    results: list[dict] = []
    lock = threading.Lock()

    def _race(source):
        barrier.wait()
        outcome = osm.adopt_broker_owned_exit_request(
            "exit-orcl-1",
            broker_order_id="36661364",
            execution_mode="paper",
            client_id="tradefluence",
            position_id="position-orcl-1",
            expected_qty=4,
            source=source,
        )
        with lock:
            results.append(outcome)

    threads = [
        threading.Thread(target=_race, args=(f"concurrent_worker_{i}",))
        for i in range(2)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert len(results) == 2
    dispositions = sorted(r["disposition"] for r in results)
    assert dispositions == ["ADOPTED", "ALREADY_BROKER_OWNED_ACTIVE"]

    # Both callers are cleared to continue to canonical broker polling.
    assert all(r["adopted"] is True for r in results)

    # But exactly one durable mutation, one transition event, one hook.
    assert osm._emit_transition_event.call_count == 1
    assert osm._handle_exit_engine_hooks.call_count == 1
    assert db.rows["exit-orcl-1"]["status"] == OrderStatus.EXIT_SUBMITTED
    assert db.rows["exit-orcl-1"]["broker_order_id"] == "36661364"


def test_f4_repeated_processing_after_terminal_makes_no_further_mutation(fake_osm_db):
    """F4: a replay after the exit already closed must be inert."""
    db, osm = fake_osm_db

    first = osm.adopt_broker_owned_exit_request(
        "exit-orcl-1",
        broker_order_id="36661364",
        execution_mode="paper",
        client_id="tradefluence",
        position_id="position-orcl-1",
        expected_qty=4,
    )
    assert first["disposition"] == "ADOPTED"

    # The canonical fill reducer closes the row.
    db.rows["exit-orcl-1"]["status"] = OrderStatus.EXIT_FILLED
    osm._emit_transition_event.reset_mock()
    osm._handle_exit_engine_hooks.reset_mock()

    replay = osm.adopt_broker_owned_exit_request(
        "exit-orcl-1",
        broker_order_id="36661364",
        execution_mode="paper",
        client_id="tradefluence",
        position_id="position-orcl-1",
        expected_qty=4,
        source="post_terminal_replay",
    )

    assert replay["disposition"] == "ALREADY_TERMINAL"
    assert replay["adopted"] is False
    assert replay["already_terminal"] is True
    assert osm._emit_transition_event.call_count == 0
    assert osm._handle_exit_engine_hooks.call_count == 0
    assert db.rows["exit-orcl-1"]["status"] == OrderStatus.EXIT_FILLED


# =====================================================================
# 2026-08-08 binding audit amendment — Blockers 1–4 regression tests
# =====================================================================


def _run_blocker1_fresh_intent_wrapper_case(monkeypatch, *, final_status):
    """Drive the real wrap_submit boundary around a fresh local EXIT intent."""
    events = []
    broker_side_effects = []
    claim_updates = []
    reserved_id = "exit-fresh-wrapper-425"

    pos = SimpleNamespace(
        position_id="position-fresh-wrapper-425",
        client_id="tradefluence",
        ticker="ORCL",
        option_symbol="ORCL260807P00155000",
        side="PUT",
        quantity_remaining=4,
        closed=False,
        exit_in_flight=False,
        pending_exit_local_order_id="",
        pending_exit_broker_order_id="",
    )

    class _WrapperOSM:
        def __init__(self):
            self.row = None
            self.active_lookup_count = 0
            self.create_count = 0
            self.adopt_count = 0

        def get_active_exit_order(self, position_id):
            assert position_id == pos.position_id
            self.active_lookup_count += 1
            events.append(
                "initial_read" if self.active_lookup_count == 1 else "final_read"
            )
            return dict(self.row) if self.row is not None else None

        def create_exit_order(self, **kwargs):
            self.create_count += 1
            events.append("create_intent")
            assert kwargs["position_id"] == pos.position_id
            assert kwargs["qty"] == 4
            self.row = {
                "local_order_id": reserved_id,
                "client_id": "tradefluence",
                "position_id": pos.position_id,
                "kind": "EXIT",
                "status": "EXIT_REQUESTED",
                "execution_mode": "paper",
                "qty": 4,
                "broker_order_id": "",
                "meta": {},
            }
            return reserved_id

        def update_order_meta(self, local_order_id, patch):
            assert local_order_id == reserved_id
            self.row["meta"].update(dict(patch))
            return True

        def get_order(self, local_order_id):
            return dict(self.row) if local_order_id == reserved_id else None

        def adopt_broker_owned_exit_request(self, *args, **kwargs):
            self.adopt_count += 1
            return {"disposition": "ADOPTED", "adopted": True}

    osm = _WrapperOSM()
    callback = MagicMock(
        side_effect=lambda *_a, **_kw: broker_side_effects.append("broker_post")
        or {"ok": True}
    )
    engine = SimpleNamespace(
        _lock=threading.RLock(),
        client_id="tradefluence",
        order_state_machine=osm,
        osm=None,
        on_exit=callback,
        on_scale=callback,
        master_control=SimpleNamespace(mode="paper"),
        _extract_exit_order_identity=lambda result: {
            "accepted": bool((result or {}).get("ok")),
            "local_order_id": reserved_id,
            "broker_order_id": "",
            "raw_status": "",
        },
    )

    monkeypatch.setattr(
        guard,
        "_durable_exit_generation",
        lambda *_args: ("tradefluence|position-fresh-wrapper-425|4|1", 1),
    )

    def _claim_generation(**kwargs):
        events.append("claim_generation")
        assert kwargs["local_order_id"] == reserved_id
        assert osm.row["status"] == "EXIT_REQUESTED"
        osm.row["status"] = final_status
        return {"claimed": True, "local_order_id": reserved_id}

    monkeypatch.setattr(guard, "_claim_durable_decision_generation", _claim_generation)
    adoption = MagicMock(return_value={"attempted": False, "adopted": False})
    monkeypatch.setattr(guard, "_adopt_callback_broker_ownership", adoption)
    monkeypatch.setattr(
        guard,
        "_classify_submit_claim_outcome",
        lambda *_args, **_kwargs: (
            guard._CLAIM_STATE_RELEASED_NO_SUBMIT,
            reserved_id,
            "",
            "positive_control_complete",
        ),
    )
    monkeypatch.setattr(
        guard,
        "_update_durable_decision_generation",
        lambda generation_key, **kwargs: claim_updates.append(
            (generation_key, dict(kwargs))
        ),
    )
    monkeypatch.setattr(
        guard, "_retire_local_exit_intent_after_no_submit", lambda *_a, **_kw: True
    )

    wrapped = guard.wrap_submit(
        lambda active_engine, active_pos, decision: bool(
            active_engine.on_exit(active_pos, decision)
        )
    )
    result = wrapped(
        engine,
        pos,
        SimpleNamespace(
            action="EXIT",
            quantity=4,
            reason_code="EXIT_FILLED_RECOVERY_FENCE",
            should_act=True,
        ),
    )
    return {
        "result": result,
        "events": events,
        "osm": osm,
        "callback": callback,
        "broker_side_effects": broker_side_effects,
        "adoption": adoption,
        "claim_updates": claim_updates,
    }


def test_blocker1_fresh_intent_terminalized_before_callback_blocks_broker_post(
    monkeypatch,
):
    """The real wrapper rereads a fresh reservation and blocks EXIT_FILLED."""
    proof = _run_blocker1_fresh_intent_wrapper_case(
        monkeypatch, final_status="EXIT_FILLED"
    )

    assert proof["result"] is False
    assert proof["events"] == [
        "initial_read",
        "create_intent",
        "claim_generation",
        "final_read",
    ]
    assert proof["osm"].active_lookup_count == 2
    assert proof["osm"].create_count == 1
    assert proof["callback"].call_count == 0
    assert proof["broker_side_effects"] == []
    assert proof["adoption"].call_count == 0
    assert proof["osm"].adopt_count == 0
    assert proof["claim_updates"] == [
        (
            "tradefluence|position-fresh-wrapper-425|4|1",
            {
                "claim_state": guard._CLAIM_STATE_BROKER_OWNED,
                "local_order_id": "exit-fresh-wrapper-425",
                "broker_order_id": "",
                "error_text": "FINAL_RESERVED_EXIT_IDENTITY_FENCE_FAILED",
            },
        )
    ]
    assert proof["osm"].row["status"] == "EXIT_FILLED"


def test_production_scale_out_reuses_exact_reserved_identity_and_quantity(monkeypatch):
    """Drive APExitEngine -> APExecutionCore._on_position_scale -> real OSM submit."""
    import ap.authorization as authorization_module
    import ap.exit_safety as exit_safety_module
    from ap_exit_engine import APExitEngine, ExitDecision, ManagedPosition
    from ap_execution_core import APExecutionCore

    reserved_id = "exit-scale-production-425"
    broker_id = "broker-scale-production-425"

    class _ProductionScaleOSM:
        submit_exit = APOrderStateMachine.submit_exit

        def __init__(self):
            self.client_id = "tradefluence"
            self.row = None
            self.create_calls = []
            self.transitions = []
            self.adopt_calls = []

        def create_exit_order(self, **kwargs):
            self.create_calls.append(dict(kwargs))
            assert kwargs["local_order_id"] == reserved_id
            self.row = {
                "client_id": self.client_id,
                "local_order_id": reserved_id,
                "broker_order_id": "",
                "position_id": kwargs["position_id"],
                "kind": "EXIT",
                "contract": kwargs["contract"],
                "symbol": kwargs["symbol"],
                "direction": kwargs["direction"],
                "qty": kwargs["qty"],
                "status": "EXIT_REQUESTED",
                "execution_mode": kwargs["execution_mode"],
                "meta": {},
            }
            return reserved_id

        def get_active_exit_order(self, position_id):
            if self.row and self.row["position_id"] == position_id:
                return dict(self.row)
            return None

        def _get_active_exit_order(self, position_id):
            return self.get_active_exit_order(position_id)

        def get_order(self, local_order_id):
            if self.row and self.row["local_order_id"] == local_order_id:
                return dict(self.row)
            return None

        def update_order_meta(self, local_order_id, patch):
            assert self.row and local_order_id == self.row["local_order_id"]
            self.row["meta"].update(dict(patch))
            return True

        def persist_exit_submit_intent(
            self,
            local_order_id,
            *,
            position_id,
            execution_mode,
            contract,
            qty,
            payload_hash,
            broker_submit_key,
        ):
            assert self.row and local_order_id == self.row["local_order_id"]
            assert self.row["position_id"] == position_id
            assert self.row["execution_mode"] == execution_mode
            assert self.row["contract"] == contract
            assert self.row["qty"] == qty
            self.row["meta"].update({
                "lifecycle_state": "SUBMITTING",
                "submit_intent_at": "2026-08-08T12:00:00+00:00",
                "broker_submit_key": broker_submit_key,
                "broker_submit_payload_hash": payload_hash,
                "current_owner": f"broker_submit:{broker_submit_key}",
            })
            return True

        def transition(self, local_order_id, new_status, **kwargs):
            assert self.row and local_order_id == self.row["local_order_id"]
            self.transitions.append((local_order_id, new_status, dict(kwargs)))
            self.row["status"] = new_status
            if kwargs.get("broker_order_id"):
                self.row["broker_order_id"] = str(kwargs["broker_order_id"])
            return True

        def adopt_broker_owned_exit_request(self, local_order_id, **kwargs):
            self.adopt_calls.append((local_order_id, dict(kwargs)))
            assert local_order_id == reserved_id
            assert kwargs["expected_qty"] == 1
            return {
                "disposition": "ALREADY_BROKER_OWNED_ACTIVE",
                "adopted": False,
                "already_broker_owned": True,
            }

        def retire_unsubmitted_exit_intent(self, local_order_id, *, last_error):
            raise AssertionError(
                f"successful scale reservation must not retire: {local_order_id} {last_error}"
            )

        @staticmethod
        def _resolve_underlying_symbol(*, symbol, contract):
            return symbol

        @staticmethod
        def _is_broker_accept_status(status):
            return status in {"open", "pending", "accepted", "ok"}

        @staticmethod
        def _emit_transition_event(**_kwargs):
            return None

        @staticmethod
        def _flag_split_brain_order(*_args, **_kwargs):
            return None

        @staticmethod
        def _lookup_order_by_tag(*_args, **_kwargs):
            return None

    osm = _ProductionScaleOSM()
    broker = MagicMock()
    broker.base_url = "https://sandbox.tradier.com"
    broker.account_id = "paper-account"
    response = MagicMock(status_code=200, text="")
    response.json.return_value = {
        "order": {"id": broker_id, "status": "open"}
    }
    broker.session.post.return_value = response

    # The full P0 process includes legacy tests that replace ap.authorization
    # in sys.modules.  Bind this regression to its declared PAPER broker mode
    # so the real OSM boundary checks the durable identity deterministically.
    monkeypatch.setattr(
        authorization_module,
        "execution_mode_for_broker",
        lambda _broker: "paper",
    )

    monkeypatch.setattr(
        exit_safety_module,
        "resolve_exit_broker_truth",
        lambda **_kwargs: {
            "is_fresh_exact": True,
            "broker_truth_open_qty": 4,
            "audit": {},
        },
    )
    monkeypatch.setattr(
        exit_safety_module,
        "evaluate_exit_submission_safety",
        lambda **_kwargs: {"blocked": False, "reason": None},
    )
    monkeypatch.setattr(
        osm_module,
        "resolve_exit_broker_truth",
        exit_safety_module.resolve_exit_broker_truth,
    )
    monkeypatch.setattr(
        osm_module,
        "evaluate_exit_submission_safety",
        exit_safety_module.evaluate_exit_submission_safety,
    )

    pos = ManagedPosition(
        ticker="ORCL",
        option_symbol="ORCL260807P00155000",
        side="PUT",
        quantity=4,
        entry_price=1.00,
        underlying_entry=155.00,
        underlying_target=150.00,
        underlying_stop=158.00,
        position_id="position-scale-production-425",
        client_id="tradefluence",
        execution_mode="paper",
        current_option_price=1.20,
        current_bid=1.15,
        current_ask=1.25,
        current_underlying=153.00,
        quantity_remaining=4,
        last_option_quote_update_ts=datetime.now(timezone.utc),
    )
    pos.signal = {"signal_id": "signal-scale-production-425"}
    decision = ExitDecision(
        action="SCALE_OUT",
        quantity=1,
        reason="TP SCALE OUT",
        urgency="NORMAL",
        pnl_pct=0.20,
    )

    core = APExecutionCore.__new__(APExecutionCore)
    core.order_state_machine = osm
    core.broker = broker

    engine = APExitEngine.__new__(APExitEngine)
    engine._lock = threading.RLock()
    engine._positions = [pos]
    engine._positions_by_id = {pos.position_id: pos}
    engine.client_id = "tradefluence"
    engine._email = "tradefluence"
    engine.master_control = SimpleNamespace(mode="paper")
    engine.broker = broker
    engine.order_state_machine = osm
    engine.osm = None
    engine.on_scale = core._on_position_scale
    engine.on_exit = None
    engine._can_submit_exit = lambda *_args, **_kwargs: True
    engine._clear_degraded_monitoring_state = lambda *_args, **_kwargs: None
    engine._emit_exit_event = lambda *_args, **_kwargs: None
    engine._emit_degraded_critical = lambda *_args, **_kwargs: None
    engine._mark_exit_submitted = lambda active_pos, active_decision, **kwargs: (
        setattr(active_pos, "exit_in_flight", True),
        setattr(active_pos, "pending_exit_action", active_decision.action),
        setattr(active_pos, "pending_exit_qty", active_decision.quantity),
        setattr(
            active_pos,
            "pending_exit_local_order_id",
            str(kwargs.get("local_order_id") or ""),
        ),
        setattr(
            active_pos,
            "pending_exit_broker_order_id",
            str(kwargs.get("broker_order_id") or ""),
        ),
    )

    monkeypatch.setattr(
        guard,
        "uuid",
        SimpleNamespace(uuid4=lambda: reserved_id),
    )
    monkeypatch.setattr(
        guard,
        "_durable_exit_generation",
        lambda *_args: ("tradefluence|position-scale-production-425|4|1", 1),
    )
    monkeypatch.setattr(
        guard,
        "_claim_durable_decision_generation",
        lambda **kwargs: {
            "claimed": True,
            "local_order_id": kwargs["local_order_id"],
        },
    )
    claim_updates = []
    monkeypatch.setattr(
        guard,
        "_update_durable_decision_generation",
        lambda generation_key, **kwargs: claim_updates.append(
            (generation_key, dict(kwargs))
        ),
    )

    original_submit = getattr(
        APExitEngine,
        guard._ORIGINAL_SUBMIT_ATTR,
        APExitEngine._submit_exit_decision,
    )
    guarded_submit = guard.wrap_submit(original_submit)
    result = guarded_submit(engine, pos, decision)

    assert result is True
    assert len(osm.create_calls) == 1
    assert osm.create_calls[0]["qty"] == 1
    assert osm.row["local_order_id"] == reserved_id
    assert osm.row["qty"] == 1
    assert osm.row["broker_order_id"] == broker_id
    assert osm.row["client_id"] == "tradefluence"
    assert osm.row["execution_mode"] == "paper"
    assert osm.row["position_id"] == pos.position_id
    assert broker.session.post.call_count == 1
    posted = broker.session.post.call_args.kwargs["data"]
    assert posted["quantity"] == 1
    assert posted["tag"] == osm_module.canonical_broker_submit_key(reserved_id)
    assert osm.adopt_calls == [
        (
            reserved_id,
            {
                "broker_order_id": broker_id,
                "execution_mode": "paper",
                "client_id": "tradefluence",
                "position_id": pos.position_id,
                "expected_qty": 1,
                "broker_submitted_ts": None,
                "source": "exit_decision_callback",
            },
        )
    ]
    assert claim_updates[-1][1]["local_order_id"] == reserved_id
    assert claim_updates[-1][1]["broker_order_id"] == broker_id

    # Replay the same economic SCALE_OUT generation through the same real
    # producer-to-broker chain.  The already broker-owned durable row must
    # fence the replay before any second create or POST.
    replay_result = guarded_submit(engine, pos, decision)
    assert replay_result is False
    assert len(osm.create_calls) == 1
    assert broker.session.post.call_count == 1


def test_osm_exit_submit_intent_cas_binds_exact_durable_identity(monkeypatch):
    captured = {}

    class _Cursor:
        rowcount = 1

        def execute(self, sql, params):
            captured["sql"] = " ".join(str(sql).split())
            captured["params"] = params
            return self

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(osm_module, "conn", lambda: _Cursor())
    monkeypatch.setattr(osm_module, "run_with_retry", lambda fn, **_kwargs: fn())
    osm = APOrderStateMachine.__new__(APOrderStateMachine)
    osm.client_id = "tradefluence"

    assert osm.persist_exit_submit_intent(
        "exit-intent-exact-425",
        position_id="position-intent-exact-425",
        execution_mode="paper",
        contract="ORCL260807P00155000",
        qty=1,
        payload_hash="payload-hash-425",
        broker_submit_key="exit-intent-exact-425",
    ) is True
    assert "kind='EXIT' AND status=%s" in captured["sql"]
    assert "COALESCE(qty,0)=%s" in captured["sql"]
    assert "COALESCE(meta->>'submit_intent_at','')=''" in captured["sql"]
    assert captured["params"][1:] == (
        "exit-intent-exact-425",
        "tradefluence",
        "position-intent-exact-425",
        "EXIT_REQUESTED",
        "paper",
        "ORCL260807P00155000",
        1,
    )
    persisted_meta = json.loads(captured["params"][0])
    assert persisted_meta["broker_submit_key"] == osm_module.canonical_broker_submit_key(
        "exit-intent-exact-425"
    )
    assert persisted_meta["broker_submit_payload_hash"] == "payload-hash-425"
    assert persisted_meta["submit_intent_at"]


def test_production_scale_out_qty_mismatch_holds_before_osm_and_broker():
    """Durable qty=4 versus SCALE_OUT qty=1 is a zero-POST HOLD."""
    from ap_exit_engine import APExitEngine, ExitDecision, ManagedPosition
    from ap_execution_core import APExecutionCore

    reserved_id = "exit-scale-mismatch-425"
    pos = ManagedPosition(
        ticker="ORCL",
        option_symbol="ORCL260807P00155000",
        side="PUT",
        quantity=4,
        entry_price=1.00,
        underlying_entry=155.00,
        underlying_target=150.00,
        underlying_stop=158.00,
        position_id="position-scale-mismatch-425",
        client_id="tradefluence",
        execution_mode="paper",
        current_option_price=1.20,
        current_bid=1.15,
        current_ask=1.25,
        current_underlying=153.00,
        quantity_remaining=4,
        pending_exit_local_order_id=reserved_id,
    )
    decision = ExitDecision(
        action="SCALE_OUT",
        quantity=1,
        reason="TP SCALE OUT",
        urgency="NORMAL",
        pnl_pct=0.20,
    )
    active_order = _guard_reserved_order(
        pos,
        local_order_id=reserved_id,
        execution_mode="paper",
        qty=4,
    )
    osm = _GuardOSM(active_order)
    osm.submit_exit = MagicMock(side_effect=AssertionError("OSM must not be reached"))
    broker = MagicMock()

    core = APExecutionCore.__new__(APExecutionCore)
    core.order_state_machine = osm
    core.broker = broker

    engine = APExitEngine.__new__(APExitEngine)
    engine._lock = threading.RLock()
    engine.client_id = "tradefluence"
    engine._email = "tradefluence"
    engine.master_control = SimpleNamespace(mode="paper")
    engine.order_state_machine = osm
    engine.osm = None
    engine.on_scale = core._on_position_scale
    engine.on_exit = None

    original_submit = getattr(
        APExitEngine,
        guard._ORIGINAL_SUBMIT_ATTR,
        APExitEngine._submit_exit_decision,
    )
    result = guard.wrap_submit(original_submit)(engine, pos, decision)

    assert result is False
    assert active_order["qty"] == 4
    assert active_order["broker_order_id"] == ""
    osm.submit_exit.assert_not_called()
    assert broker.session.post.call_count == 0


def test_production_close_callback_returns_broker_identity_for_adoption():
    """CLOSE_ALL must preserve broker ownership across local persistence gaps."""
    from ap_exit_engine import ExitDecision, ManagedPosition
    from ap_execution_core import APExecutionCore

    broker_owned_gap = {
        "ok": False,
        "local_order_id": "exit-close-gap-425",
        "broker_order_id": "broker-close-gap-425",
        "status": "ERROR",
        "error": "exit_submitted_transition_failed_after_broker_accept",
        "split_brain": True,
    }
    osm = SimpleNamespace(submit_exit=MagicMock(return_value=broker_owned_gap))
    core = APExecutionCore.__new__(APExecutionCore)
    core._pos_lock = threading.RLock()
    core._position_count = 1
    core._sector_lock = threading.RLock()
    core._sector_counts = {"OTHER": 1}
    core.paper = True
    core.order_state_machine = osm
    core.broker = MagicMock()
    pos = ManagedPosition(
        ticker="ORCL",
        option_symbol="ORCL260807P00155000",
        side="PUT",
        quantity=1,
        entry_price=1.00,
        underlying_entry=155.00,
        underlying_target=150.00,
        underlying_stop=158.00,
        position_id="position-close-gap-425",
        client_id="tradefluence",
        current_option_price=1.20,
        current_bid=1.15,
        current_ask=1.25,
        current_underlying=153.00,
        quantity_remaining=1,
        pending_exit_local_order_id="exit-close-gap-425",
    )
    pos.signal = {"signal_id": "signal-close-gap-425"}
    decision = ExitDecision(
        action="CLOSE_ALL",
        quantity=1,
        reason="HARD STOP",
        urgency="HIGH",
        reason_code="HARD_STOP",
        suggested_limit=1.15,
    )

    result = core._on_position_close(pos, decision)

    assert result == broker_owned_gap
    assert result["local_order_id"] == "exit-close-gap-425"
    assert result["broker_order_id"] == "broker-close-gap-425"
    osm.submit_exit.assert_called_once()
    assert osm.submit_exit.call_args.kwargs["qty"] == 1
    assert osm.submit_exit.call_args.kwargs["local_order_id"] == "exit-close-gap-425"


@pytest.mark.parametrize(
    ("reason_code", "urgency"),
    [
        ("HARD_STOP", "IMMEDIATE"),
        ("PROFIT_LOCK", "HIGH"),
        ("EOD_FORCE_CLOSE", "IMMEDIATE"),
    ],
)
def test_production_close_paths_reuse_exact_reserved_qty_and_local_id(
    reason_code,
    urgency,
):
    from ap_exit_engine import ExitDecision, ManagedPosition
    from ap_execution_core import APExecutionCore

    local_id = f"exit-close-{reason_code.lower()}-425"
    osm = SimpleNamespace(submit_exit=MagicMock(return_value={
        "ok": False,
        "local_order_id": local_id,
        "broker_order_id": f"broker-{reason_code.lower()}-425",
        "status": "ERROR",
        "error": "exit_submitted_transition_failed_after_broker_accept",
        "split_brain": True,
    }))
    core = APExecutionCore.__new__(APExecutionCore)
    core._pos_lock = threading.RLock()
    core._position_count = 1
    core._sector_lock = threading.RLock()
    core._sector_counts = {"OTHER": 1}
    core.paper = reason_code != "HARD_STOP"
    core.order_state_machine = osm
    core.broker = MagicMock()
    core.master_control = SimpleNamespace()
    pos = ManagedPosition(
        ticker="ORCL",
        option_symbol="ORCL260807P00155000",
        side="PUT",
        quantity=2,
        entry_price=1.00,
        underlying_entry=155.00,
        underlying_target=150.00,
        underlying_stop=158.00,
        position_id=f"position-{reason_code.lower()}-425",
        client_id="tradefluence",
        current_option_price=1.20,
        current_bid=1.15,
        current_ask=1.25,
        current_underlying=153.00,
        quantity_remaining=2,
        pending_exit_local_order_id=local_id,
    )
    pos.signal = {"signal_id": f"signal-{reason_code.lower()}-425"}
    decision = ExitDecision(
        action="CLOSE_ALL",
        quantity=2,
        reason=reason_code.replace("_", " "),
        urgency=urgency,
        reason_code=reason_code,
        suggested_limit=1.15,
    )
    decision.reserved_local_order_id = local_id
    decision.reserved_exit_quantity = 2

    core._on_position_close(pos, decision)

    call = osm.submit_exit.call_args.kwargs
    assert call["qty"] == 2
    assert call["local_order_id"] == local_id
    assert call["order_type"] == (
        "market" if reason_code in {"HARD_STOP", "EOD_FORCE_CLOSE"} else "limit"
    )


@pytest.mark.parametrize(
    ("decision_qty", "reserved_qty"),
    [
        ("1", 1),
        (1.0, 1),
        (True, 1),
        (1, "1"),
        (1, 1.0),
        (1, True),
    ],
)
def test_production_scale_callback_rejects_coerced_quantity_identity(
    decision_qty,
    reserved_qty,
):
    """The real SCALE callback rejects malformed producer/reservation qty."""
    from ap_execution_core import APExecutionCore

    osm = SimpleNamespace(
        submit_exit=MagicMock(side_effect=AssertionError("OSM must not be reached"))
    )
    broker = MagicMock()
    core = APExecutionCore.__new__(APExecutionCore)
    core.order_state_machine = osm
    core.broker = broker
    pos = SimpleNamespace(
        ticker="ORCL",
        option_pnl_pct=0.20,
        signal={"signal_id": "signal-scale-invalid-qty-425"},
        position_id="position-scale-invalid-qty-425",
        current_bid=1.15,
        current_option_price=1.20,
        option_symbol="ORCL260807P00155000",
        side="PUT",
    )
    decision = SimpleNamespace(
        action="SCALE_OUT",
        quantity=decision_qty,
        reserved_exit_quantity=reserved_qty,
        reserved_local_order_id="exit-scale-invalid-qty-425",
        reason="TP SCALE OUT",
    )

    result = core._on_position_scale(pos, decision)

    assert result["ok"] is False
    assert result["error"] == "reserved_exit_quantity_invalid"
    osm.submit_exit.assert_not_called()
    assert broker.session.post.call_count == 0


@pytest.mark.parametrize(
    ("field", "invalid_value"),
    [
        ("local_order_id", "exit-other-425"),
        ("position_id", "position-other-425"),
        ("kind", "ENTRY"),
        ("status", "EXIT_SUBMITTED"),
        ("client_id", "jason"),
        ("execution_mode", "live"),
        ("contract", "ORCL260807C00155000"),
        ("qty", 4),
        ("broker_order_id", "broker-already-owned-425"),
    ],
)
def test_osm_reserved_exit_identity_mismatch_never_posts(field, invalid_value):
    """OSM independently enforces every reserved identity field at POST."""
    reserved_id = "exit-osm-boundary-425"
    position_id = "position-osm-boundary-425"
    contract = "ORCL260807P00155000"
    row = {
        "client_id": "tradefluence",
        "local_order_id": reserved_id,
        "broker_order_id": "",
        "position_id": position_id,
        "kind": "EXIT",
        "contract": contract,
        "qty": 1,
        "status": "EXIT_REQUESTED",
        "execution_mode": "paper",
    }
    row[field] = invalid_value

    class _BoundaryOSM:
        submit_exit = APOrderStateMachine.submit_exit
        client_id = "tradefluence"

        def _get_active_exit_order(self, _position_id):
            return dict(row)

    broker = MagicMock()
    result = _BoundaryOSM().submit_exit(
        broker=broker,
        position_id=position_id,
        contract=contract,
        symbol="ORCL",
        direction="PUT",
        qty=1,
        limit_price=1.15,
        execution_mode="paper",
        local_order_id=reserved_id,
    )

    assert result["ok"] is False
    assert result["error"].startswith("reserved_exit_identity_mismatch:")
    assert field in result["error"]
    assert broker.session.post.call_count == 0


def test_osm_reserved_exit_id_without_durable_row_never_posts():
    """A caller cannot fabricate a reservation by supplying an orphan ID."""
    class _MissingReservedOSM:
        submit_exit = APOrderStateMachine.submit_exit
        client_id = "tradefluence"

        @staticmethod
        def _get_active_exit_order(_position_id):
            return None

    broker = MagicMock()
    result = _MissingReservedOSM().submit_exit(
        broker=broker,
        position_id="position-osm-missing-425",
        contract="ORCL260807P00155000",
        symbol="ORCL",
        direction="PUT",
        qty=1,
        limit_price=1.15,
        execution_mode="paper",
        local_order_id="exit-osm-missing-425",
    )

    assert result["ok"] is False
    assert result["error"] == "reserved_exit_row_missing:exit-osm-missing-425"
    assert broker.session.post.call_count == 0


def test_blocker1_fresh_exact_requested_intent_reaches_callback_once(monkeypatch):
    """Positive control: an exact fresh EXIT_REQUESTED survives the final fence."""
    proof = _run_blocker1_fresh_intent_wrapper_case(
        monkeypatch, final_status="EXIT_REQUESTED"
    )

    assert proof["result"] is True
    assert proof["events"] == [
        "initial_read",
        "create_intent",
        "claim_generation",
        "final_read",
    ]
    assert proof["osm"].active_lookup_count == 2
    assert proof["osm"].create_count == 1
    assert proof["callback"].call_count == 1
    assert proof["broker_side_effects"] == ["broker_post"]
    assert proof["adoption"].call_count == 1
    assert proof["osm"].adopt_count == 0
    assert all(
        update[1].get("claim_state") != guard._CLAIM_STATE_BROKER_OWNED
        for update in proof["claim_updates"]
    )


@pytest.mark.parametrize(
    "expected_qty,expected_disposition",
    [
        (None, "IDENTITY_MISMATCH"),
        (0, "IDENTITY_MISMATCH"),
        (-1, "IDENTITY_MISMATCH"),
        ("4", "IDENTITY_MISMATCH"),
        ("junk", "IDENTITY_MISMATCH"),
        (4.0, "IDENTITY_MISMATCH"),
        (True, "IDENTITY_MISMATCH"),  # bool must not be accepted as int
    ],
)
def test_blocker3_expected_qty_must_be_exact_positive_int(
    fake_osm_db, expected_qty, expected_disposition
):
    """Blocker 3: expected_qty must be an exact positive integer."""
    db, osm = fake_osm_db

    kwargs = dict(
        broker_order_id="36661364",
        execution_mode="paper",
        client_id="tradefluence",
        position_id="position-orcl-1",
    )
    if expected_qty is not None:
        kwargs["expected_qty"] = expected_qty

    result = osm.adopt_broker_owned_exit_request("exit-orcl-1", **kwargs)

    assert result["disposition"] == expected_disposition
    assert result["adopted"] is False
    assert db.rows["exit-orcl-1"]["status"] == "EXIT_REQUESTED"
    assert not db.rows["exit-orcl-1"].get("broker_order_id")
    osm._emit_transition_event.assert_not_called()
    osm._handle_exit_engine_hooks.assert_not_called()


def test_blocker3_qty_mismatch_returns_identity_mismatch(fake_osm_db):
    """Blocker 3: a durable qty that does not equal expected_qty rejects."""
    db, osm = fake_osm_db
    # Durable qty is 4; ask for 3.
    result = osm.adopt_broker_owned_exit_request(
        "exit-orcl-1",
        broker_order_id="36661364",
        execution_mode="paper",
        client_id="tradefluence",
        position_id="position-orcl-1",
        expected_qty=3,
    )
    assert result["disposition"] == "IDENTITY_MISMATCH"
    assert db.rows["exit-orcl-1"]["status"] == "EXIT_REQUESTED"


def test_blocker3_exact_qty_adopts_successfully(fake_osm_db):
    """Blocker 3 positive case: exact positive integer qty proceeds."""
    _db, osm = fake_osm_db
    result = osm.adopt_broker_owned_exit_request(
        "exit-orcl-1",
        broker_order_id="36661364",
        execution_mode="paper",
        client_id="tradefluence",
        position_id="position-orcl-1",
        expected_qty=4,
    )
    assert result["disposition"] == "ADOPTED"
    assert result["adopted"] is True


@pytest.mark.parametrize(
    "master_mode,exit_engine_mode,expected",
    [
        ("paper", "live", ""),          # CONFLICT
        ("live", "paper", ""),          # CONFLICT
        (None, None, ""),               # UNPROVEN
        ("paper", None, "paper"),       # single valid source
        (None, "live", "live"),         # single valid source
        ("paper", "paper", "paper"),    # agreement
        ("live", "live", "live"),       # agreement
        ("PAPER", "paper", ""),         # malformed explicitly-present -> HOLD
        (" live ", "live", ""),         # malformed explicitly-present -> HOLD
        ("junk", "live", ""),           # malformed -> HOLD
    ],
)
def test_blocker2_runtime_mode_conflict_and_malformed_hold(
    master_mode, exit_engine_mode, expected
):
    """Blocker 2: independent sources; CONFLICT or malformed -> HOLD."""
    engine = SimpleNamespace(
        master_control=SimpleNamespace(mode=master_mode),
        execution_mode=exit_engine_mode,
    )
    assert fm._resolve_runtime_execution_mode(exit_engine=engine) == expected


def test_blocker2_explicit_argument_conflicts_with_source():
    """Blocker 2: explicit arg + conflicting other source -> HOLD."""
    engine = SimpleNamespace(
        master_control=SimpleNamespace(mode="paper"),
        execution_mode=None,
    )
    result = fm._resolve_runtime_execution_mode(
        runtime_execution_mode="live", exit_engine=engine
    )
    assert result == ""


def test_runtime_mode_reconciles_runner_and_master_control_canonical_provenance():
    """The production uppercase enum is not itself normalized into authority."""
    master_control = SimpleNamespace(
        mode="PAPER",
        runtime_execution_mode="paper",
    )
    engine = SimpleNamespace(master_control=master_control, execution_mode=None)

    assert fm._resolve_runtime_execution_mode(
        runtime_execution_mode="paper",
        exit_engine=engine,
    ) == "paper"


def test_client_runner_passes_exact_canonical_mode_to_fill_monitor(monkeypatch):
    """Drive the real production runner boundary that starts fill_monitor."""
    import client_runner

    calls = []
    runner = client_runner.ClientRunner.__new__(client_runner.ClientRunner)
    runner.mode = "PAPER"
    runner.email = "tradefluence"
    runner.stopped = threading.Event()
    runner.order_state_machine = SimpleNamespace(client_id="tradefluence")
    runner.position_manager = SimpleNamespace(client_id="tradefluence")
    runner.master_control = SimpleNamespace(mode="PAPER")
    runner.reconciler = None
    exit_engine = SimpleNamespace(master_control=runner.master_control)

    def _fill_monitor_loop(**kwargs):
        calls.append(kwargs)
        runner.stopped.set()

    monkeypatch.setattr(fm, "fill_monitor_loop", _fill_monitor_loop)

    runner._start_fill_monitor(MagicMock(), exit_engine)
    runner.fill_monitor_thread.join(timeout=2)

    assert not runner.fill_monitor_thread.is_alive()
    assert len(calls) == 1
    assert calls[0]["runtime_execution_mode"] == "paper"
    assert runner.master_control.mode == "PAPER"
    assert runner.master_control.runtime_execution_mode == "paper"
    assert fm._resolve_runtime_execution_mode(
        runtime_execution_mode=calls[0]["runtime_execution_mode"],
        exit_engine=exit_engine,
    ) == "paper"


def test_blocker2_runtime_conflict_blocks_broker_get(monkeypatch):
    """Blocker 2 end-to-end: with CONFLICT resolved, recovery must HOLD
    before the broker GET, and no adoption is attempted."""
    order = _row(broker_order_id="36661364")
    osm = _MonitorOSM(order)
    adopt_calls = []
    osm.adopt_broker_owned_exit_request = lambda *a, **kw: adopt_calls.append((a, kw)) or {
        "disposition": "ADOPTED", "adopted": True, "status": "EXIT_SUBMITTED",
    }
    check_calls = []
    monkeypatch.setattr(
        fm, "check_order_with_broker",
        lambda broker, o: check_calls.append(o) or {"status": "OPEN"},
    )

    engine = SimpleNamespace(
        master_control=SimpleNamespace(mode="paper"),
        execution_mode="live",
    )

    fm.process_pending_order(
        MagicMock(),
        dict(order),
        osm=osm,
        exit_engine=engine,
        runtime_execution_mode=None,
    )

    assert adopt_calls == []
    assert check_calls == []
