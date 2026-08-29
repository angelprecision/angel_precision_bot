"""P0 regression coverage for immutable reconciler entry-underlying truth."""

from __future__ import annotations

import json
import os
import pytest
from contextlib import contextmanager
from datetime import datetime, timezone
import sys
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@127.0.0.1:5432/test")

import ap.db as db
import ap_reconciler as rec


CLIENT = "historical-entry@example.com"
CONTRACT = "NOW260828P00122000"
POSITION_ID = "canonical-position-1"


class _Broker:
    def __init__(self, quote: float = 126.62):
        self.quote = quote
        self.quote_calls: list[str] = []

    def get_quote(self, symbol):
        self.quote_calls.append(symbol)
        return {"last": self.quote, "bid": self.quote - 0.02, "ask": self.quote + 0.02}


class _Cursor:
    def __init__(self, rows, executions):
        self.rows = rows
        self.executions = executions

    def execute(self, sql, params=None):
        self.executions.append((str(sql), tuple(params or ())))

    def fetchall(self):
        return self.rows


def _install_db(monkeypatch, rows):
    executions = []

    @contextmanager
    def _conn():
        yield _Cursor(rows, executions)

    monkeypatch.setattr(db, "conn", _conn)
    monkeypatch.setattr(db, "run_with_retry", lambda fn: fn())
    return executions


class _RecordingPostgresCursor:
    """Record SQL/parameters while executing against a real PostgreSQL cursor."""

    def __init__(self, cursor, executions):
        self._cursor = cursor
        self._executions = executions

    def execute(self, sql, params=None):
        bound = tuple(params or ())
        self._executions.append((str(sql), bound))
        self._cursor.execute(sql, bound)
        return self

    def fetchall(self):
        return self._cursor.fetchall()

    def close(self):
        self._cursor.close()

    def __getattr__(self, name):
        return getattr(self._cursor, name)


class _PostgresHistoryHarness:
    def __init__(self, connection, cursor_factory, conn_wrapper):
        self.connection = connection
        self.cursor_factory = cursor_factory
        self.conn_wrapper = conn_wrapper
        self.executions = []

    @contextmanager
    def conn(self):
        cursor = self.connection.cursor(cursor_factory=self.cursor_factory)
        recording_cursor = _RecordingPostgresCursor(cursor, self.executions)
        try:
            yield self.conn_wrapper(self.connection, recording_cursor)
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        finally:
            recording_cursor.close()

    def execute(self, sql, params=()):
        with self.connection.cursor() as cursor:
            cursor.execute(sql, params)
        self.connection.commit()

    def insert_entry(
        self,
        *,
        client_id=CLIENT,
        execution_mode="live",
        meta=None,
        position_id=POSITION_ID,
        contract=CONTRACT,
        kind="ENTRY",
        status="FILLED",
        fill_price=1.30,
        filled_qty=1,
        filled_ts=None,
    ):
        self.execute(
            """
            INSERT INTO orders (
                client_id, execution_mode, contract, kind, status,
                position_id, fill_price, filled_qty, filled_ts, meta
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                client_id,
                execution_mode,
                contract,
                kind,
                status,
                position_id,
                fill_price,
                filled_qty,
                filled_ts or datetime(2026, 8, 25, 13, 54, tzinfo=timezone.utc),
                json.dumps(meta or {}),
            ),
        )


@pytest.fixture()
def postgres_history_harness():
    psycopg2 = pytest.importorskip("psycopg2")
    extras = pytest.importorskip("psycopg2.extras")

    database_url = (
        os.getenv("INTELLIGENCE_POSTGRES_TEST_URL")
        or os.getenv("DATABASE_URL")
        or ""
    ).strip()
    if not database_url:
        pytest.skip("PostgreSQL integration URL not configured")

    try:
        connection = psycopg2.connect(database_url)
    except Exception as exc:
        if os.getenv("GITHUB_ACTIONS") == "true":
            pytest.fail(f"PostgreSQL integration unavailable in GitHub Actions: {exc}")
        pytest.skip(f"PostgreSQL integration unavailable: {exc}")

    from ap.db import _ConnWrapper

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TEMP TABLE orders (
                    client_id TEXT NOT NULL,
                    execution_mode TEXT,
                    contract TEXT,
                    kind TEXT,
                    status TEXT,
                    position_id TEXT,
                    fill_price NUMERIC,
                    filled_qty NUMERIC,
                    filled_ts TIMESTAMPTZ,
                    meta JSONB
                )
                """
            )
        connection.commit()
        yield _PostgresHistoryHarness(
            connection,
            extras.RealDictCursor,
            _ConnWrapper,
        )
    finally:
        connection.rollback()
        connection.close()


def _reconciler(broker=None):
    reconciler = rec.APBrokerReconciler.__new__(rec.APBrokerReconciler)
    reconciler.client_id = CLIENT
    reconciler.execution_mode = "live"
    reconciler.broker = broker or _Broker()
    return reconciler


def _filled_entry_row(**overrides):
    row = {
        # Production-shaped fields returned by RealDictCursor.
        "client_id": CLIENT,
        "execution_mode": "live",
        "contract": CONTRACT,
        "kind": "ENTRY",
        "status": "FILLED",
        "position_id": POSITION_ID,
        "fill_price": 1.30,
        "filled_qty": 1,
        "filled_ts": "2026-08-25T13:54:01.682835+00:00",
        "meta": {"underlying_entry": 127.425},
    }
    row.update(overrides)
    return row


def test_exact_filled_entry_metadata_is_the_only_recovery_fallback(monkeypatch):
    executions = _install_db(monkeypatch, [_filled_entry_row()])
    reconciler = _reconciler()

    value = reconciler._derive_underlying_entry_from_position(
        {"id": POSITION_ID, "underlying_entry": None},
        underlying="NOW",
        contract=CONTRACT,
    )

    assert value == 127.425
    assert executions
    sql, params = executions[0]
    assert "client_id=%s" in sql
    assert "execution_mode" in sql
    assert "contract" in sql
    assert "position_id::text=%s" in sql
    assert params == (CLIENT, "live", CONTRACT, POSITION_ID)


def test_missing_history_does_not_call_current_quote_or_fabricate_entry(monkeypatch):
    broker = _Broker()
    _install_db(monkeypatch, [])
    reconciler = _reconciler(broker)

    value = reconciler._derive_underlying_entry_from_position(
        {
            "id": POSITION_ID,
            "underlying_entry": None,
            "trigger_price": 127.40,
            "opened_underlying": 127.30,
        },
        underlying="NOW",
        contract=CONTRACT,
    )

    assert value == 0.0
    assert broker.quote_calls == []


def test_ambiguous_or_malformed_filled_entry_evidence_stays_untrusted(monkeypatch):
    for rows in (
        [_filled_entry_row(), _filled_entry_row()],
        [_filled_entry_row(fill_price=0)],
        [_filled_entry_row(filled_qty=1.5)],
        [_filled_entry_row(filled_ts="not-a-timestamp")],
        [_filled_entry_row(meta={"current_underlying_price": 127.425})],
        [_filled_entry_row(meta={
            "underlying_entry": 127.425,
            "underlying_entry_price": 126.62,
        })],
    ):
        _install_db(monkeypatch, rows)
        reconciler = _reconciler()
        assert (
            reconciler._derive_underlying_entry_from_position(
                {"id": POSITION_ID, "underlying_entry": None},
                underlying="NOW",
                contract=CONTRACT,
            )
            == 0.0
        )


def test_broker_snapshot_current_fields_are_not_historical_entry(monkeypatch):
    broker = _Broker()
    reconciler = _reconciler(broker)

    value = reconciler._derive_underlying_entry_from_broker_position(
        {
            "underlying_price": 127.40,
            "underlier_price": 127.30,
            "current_underlying": 127.20,
        },
        underlying="NOW",
        contract=CONTRACT,
    )

    assert value == 0.0
    assert broker.quote_calls == []


def test_explicit_historical_broker_field_is_allowed():
    reconciler = _reconciler()

    value = reconciler._derive_underlying_entry_from_broker_position(
        {"underlying_entry": 127.425},
        underlying="NOW",
        contract=CONTRACT,
    )

    assert value == 127.425


def test_nonfinite_historical_values_stay_untrusted(monkeypatch):
    _install_db(monkeypatch, [])
    reconciler = _reconciler()

    assert (
        reconciler._derive_underlying_entry_from_position(
            {"id": POSITION_ID, "underlying_entry": float("inf")},
            underlying="NOW",
            contract=CONTRACT,
        )
        == 0.0
    )
    assert (
        reconciler._derive_underlying_entry_from_broker_position(
            {"underlying_entry": float("nan")},
            underlying="NOW",
            contract=CONTRACT,
        )
        == 0.0
    )


def test_import_seed_keeps_unknown_underlying_zero_and_untrusted(monkeypatch):
    class _ManagedPosition:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class _ExitEngine:
        def __init__(self):
            self.positions = []

        def add_position(self, position):
            self.positions.append(position)

    broker = _Broker()
    reconciler = _reconciler(broker)
    reconciler.exit_engine = _ExitEngine()
    reconciler._alert = lambda _message: None
    reconciler._record_position_reseeded = lambda **_kwargs: None
    reconciler._heartbeat = lambda *_args, **_kwargs: None
    monkeypatch.setitem(
        sys.modules,
        "ap_exit_engine",
        SimpleNamespace(ManagedPosition=_ManagedPosition),
    )

    reconciler._seed_exit_engine_from_import(
        pos_id=POSITION_ID,
        contract=CONTRACT,
        underlying="NOW",
        side="PUT",
        qty=1,
        entry_px=1.30,
        stop_underlying=130.44,
        target_underlying=124.78,
        underlying_entry=0.0,
    )

    seeded = reconciler.exit_engine.positions[0]
    assert seeded.underlying_entry == 0.0
    assert seeded.underlying_entry_untrusted is True
    assert broker.quote_calls == []


def _bind_postgres_history(monkeypatch, harness):
    monkeypatch.setattr(db, "conn", harness.conn)
    monkeypatch.setattr(db, "run_with_retry", lambda fn: fn())


def test_real_postgres_historical_entry_lookup_executes_with_durable_row_shape(
    postgres_history_harness,
    monkeypatch,
):
    harness = postgres_history_harness
    harness.insert_entry(
        meta={
            "execution_mode": "live",
            "underlying_entry": 127.425,
        }
    )
    _bind_postgres_history(monkeypatch, harness)

    value = _reconciler()._derive_underlying_entry_from_position(
        {"id": POSITION_ID, "underlying_entry": None},
        underlying="NOW",
        contract=CONTRACT,
    )

    assert value == pytest.approx(127.425)
    assert len(harness.executions) == 1
    sql, params = harness.executions[0]
    assert "meta->>'execution_mode'" in sql
    assert "NULLIF(TRIM(execution_mode), '') IS NULL" in sql
    assert params == (CLIENT, "live", CONTRACT, POSITION_ID)


def test_real_postgres_conflicting_mode_authorities_fail_closed(
    postgres_history_harness,
    monkeypatch,
):
    harness = postgres_history_harness
    harness.insert_entry(
        meta={
            "execution_mode": "paper",
            "underlying_entry": 127.425,
        }
    )
    _bind_postgres_history(monkeypatch, harness)

    value = _reconciler()._derive_underlying_entry_from_position(
        {"id": POSITION_ID, "underlying_entry": None},
        underlying="NOW",
        contract=CONTRACT,
    )

    assert value == 0.0


def test_real_postgres_metadata_only_mode_is_recoverable(
    postgres_history_harness,
    monkeypatch,
):
    harness = postgres_history_harness
    harness.insert_entry(
        execution_mode=None,
        meta={
            "execution_mode": "live",
            "underlying_entry": 127.425,
        }
    )
    _bind_postgres_history(monkeypatch, harness)

    value = _reconciler()._derive_underlying_entry_from_position(
        {"id": POSITION_ID, "underlying_entry": None},
        underlying="NOW",
        contract=CONTRACT,
    )

    assert value == pytest.approx(127.425)


def test_real_postgres_zero_plus_positive_historical_aliases_fail_closed(
    postgres_history_harness,
    monkeypatch,
):
    harness = postgres_history_harness
    harness.insert_entry(
        meta={
            "execution_mode": "live",
            "underlying_entry": 0,
            "underlying_entry_price": 127.425,
        }
    )
    _bind_postgres_history(monkeypatch, harness)

    value = _reconciler()._derive_underlying_entry_from_position(
        {"id": POSITION_ID, "underlying_entry": None},
        underlying="NOW",
        contract=CONTRACT,
    )

    assert value == 0.0
