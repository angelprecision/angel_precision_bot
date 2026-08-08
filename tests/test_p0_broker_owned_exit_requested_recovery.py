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
                and str(row.get("kind") or "").upper() == "EXIT"
                and str(row.get("status") or "").upper() == "EXIT_REQUESTED"
                and str(row.get("execution_mode") or "").strip()
                == str(execution_mode).strip()
                and str(row.get("position_id") or "").strip()
                and int(row.get("qty") or 0) > 0
                and (
                    not str(row.get("broker_order_id") or "").strip()
                    or str(row.get("broker_order_id")) == str(broker_match)
                )
                and (
                    expected_position is None
                    or str(row.get("position_id")) == expected_position
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
        ({"position_id": "other-position"}, "CAS_MISS"),
        ({"expected_qty": 3}, "CAS_MISS"),
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

    assert result["disposition"] == "CAS_MISS"
    assert db.rows["exit-orcl-1"]["status"] == "EXIT_REQUESTED"
    assert db.rows["exit-orcl-1"]["broker_order_id"] == "historical-broker-id"


def test_noncanonical_durable_mode_cannot_be_laundered_by_recovery(fake_osm_db):
    db, osm = fake_osm_db
    db.rows["exit-orcl-1"]["execution_mode"] = "PAPER"

    result = osm.adopt_broker_owned_exit_request(
        "exit-orcl-1",
        broker_order_id="36661364",
        execution_mode="paper",
        client_id="tradefluence",
        position_id="position-orcl-1",
        expected_qty=4,
    )

    assert result["disposition"] == "CAS_MISS"
    assert db.rows["exit-orcl-1"]["status"] == "EXIT_REQUESTED"


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
                "disposition": "CAS_MISS",
                "adopted": False,
                "reason_code": "EXIT_BROKER_OWNERSHIP_ADOPTION_FAILED",
                "error": "identity_or_status_cas_miss",
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


def test_callback_local_identity_conflict_holds_without_osm_adoption():
    pos = _guard_pos(pending_exit_local_order_id="exit-local-reserved")
    active_order = {
        "local_order_id": "exit-local-reserved",
        "broker_order_id": "",
        "status": "EXIT_REQUESTED",
        "position_id": pos.position_id,
        "qty": 4,
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
        "local_order_id": "exit-local-reserved",
        "broker_order_id": "broker-active-1",
        "status": "EXIT_REQUESTED",
        "position_id": pos.position_id,
        "qty": 4,
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
        "local_order_id": "exit-gap-1",
        "position_id": "position-gap",
        "kind": "EXIT",
        "status": "EXIT_REQUESTED",
        "broker_order_id": "",
        "qty": 4,
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
