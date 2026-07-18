from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime, timezone
from uuid import uuid4

import pytest

import ap.db as db_mod
import ap.position_manager as pm_mod
from ap.position_manager import APPositionManager
from ap_reconciler import APBrokerReconciler, _empty_summary


def _postgres_or_skip():
    psycopg2 = pytest.importorskip("psycopg2")
    url = os.getenv("INTELLIGENCE_POSTGRES_TEST_URL")
    if not url:
        if os.getenv("GITHUB_ACTIONS") == "true":
            pytest.fail("INTELLIGENCE_POSTGRES_TEST_URL is required in GitHub Actions")
        pytest.skip("PostgreSQL integration URL unavailable")
    return psycopg2.connect(url)


@pytest.fixture()
def production_db(monkeypatch):
    psycopg2 = pytest.importorskip("psycopg2")
    extras = pytest.importorskip("psycopg2.extras")
    connection = _postgres_or_skip()
    connection.autocommit = True
    schema = f"pr1_import_{uuid4().hex}"
    with connection.cursor() as cursor:
        cursor.execute(f'CREATE SCHEMA "{schema}"')
        cursor.execute(f'SET search_path TO "{schema}"')
        cursor.execute(
            """
            CREATE TABLE positions (
                id TEXT PRIMARY KEY,
                client_id TEXT NOT NULL,
                execution_mode TEXT,
                plan_id TEXT,
                signal_id TEXT,
                underlying TEXT,
                contract TEXT,
                direction TEXT,
                qty INTEGER,
                quantity_remaining INTEGER,
                avg_fill NUMERIC,
                entry_price NUMERIC,
                underlying_entry NUMERIC,
                tier TEXT,
                score NUMERIC,
                pattern TEXT,
                tp_pct NUMERIC,
                sl_pct NUMERIC,
                stop_underlying NUMERIC,
                target_underlying NUMERIC,
                status TEXT,
                entry_ts TIMESTAMPTZ,
                created_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ,
                opened_at TIMESTAMPTZ,
                exit_ts TIMESTAMPTZ,
                exit_reason TEXT,
                close_source TEXT,
                close_confidence TEXT,
                local_order_id TEXT,
                broker_order_id TEXT
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE orders (
                id TEXT PRIMARY KEY,
                client_id TEXT,
                contract TEXT,
                kind TEXT,
                signal_id TEXT,
                status TEXT,
                created_ts TIMESTAMPTZ
            )
            """
        )

    @contextmanager
    def _conn():
        cursor = connection.cursor(cursor_factory=extras.RealDictCursor)
        try:
            yield cursor
        finally:
            cursor.close()

    monkeypatch.setattr(db_mod, "conn", _conn)
    monkeypatch.setattr(db_mod, "run_with_retry", lambda fn, *a, **k: fn())
    monkeypatch.setattr(pm_mod, "conn", _conn)
    monkeypatch.setattr(pm_mod, "run_with_retry", lambda fn, *a, **k: fn())
    try:
        yield connection
    finally:
        with connection.cursor() as cursor:
            cursor.execute("SET search_path TO public")
            cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        connection.close()


class NoMutationBroker:
    def submit_order(self, *_args, **_kwargs):
        raise AssertionError("broker submit is outside PR1 mutation boundary")

    def cancel_order(self, *_args, **_kwargs):
        raise AssertionError("broker cancel is outside PR1 mutation boundary")


def _manager(client_id: str) -> APPositionManager:
    manager = APPositionManager(client_id)
    manager._position_columns_cache = {
        "id", "client_id", "execution_mode", "plan_id", "signal_id",
        "underlying", "contract", "direction", "qty", "quantity_remaining",
        "avg_fill", "entry_price", "underlying_entry", "tier", "score",
        "pattern", "tp_pct", "sl_pct", "stop_underlying",
        "target_underlying", "status", "entry_ts", "created_at", "updated_at",
        "opened_at", "exit_ts", "exit_reason", "close_source",
        "close_confidence", "local_order_id", "broker_order_id",
    }
    return manager


def _reconciler(client_id: str, mode: str | None, diagnostics: list[dict]):
    reconciler = APBrokerReconciler.__new__(APBrokerReconciler)
    reconciler.broker = NoMutationBroker()
    reconciler.client_id = client_id
    reconciler.execution_mode = mode
    reconciler.pm = _manager(client_id)
    reconciler.exit_engine = None
    reconciler._alert_fn = lambda _message: None
    reconciler._record_reconciler_rejection = lambda **payload: diagnostics.append(payload)
    reconciler._alert = lambda _message: None
    reconciler._seed_exit_engine_from_position = lambda _row: None
    reconciler._seed_exit_engine_from_import = lambda **_kwargs: None
    reconciler._derive_underlying_entry_from_broker_position = lambda *_a, **_k: 0.0
    return reconciler


def _broker_position(contract: str, *, lot_id: str = "", acquired: str = "2026-07-18T14:30:00Z"):
    row = {
        "symbol": contract,
        "quantity": 4,
        "cost_basis": 244.0,
        "date_acquired": acquired,
    }
    if lot_id:
        row["position_id"] = lot_id
    return row


def _poll(reconciler, positions):
    summary = _empty_summary(reconciler.client_id)
    reconciler._import_broker_positions_missing_from_db(
        broker_positions=positions,
        db_contracts=set(),
        summary=summary,
    )
    return summary


def _count(connection, client_id: str | None = None) -> int:
    with connection.cursor() as cursor:
        if client_id:
            cursor.execute("SELECT count(*) FROM positions WHERE client_id=%s", (client_id,))
        else:
            cursor.execute("SELECT count(*) FROM positions")
        return int(cursor.fetchone()[0])


def test_500_expired_polls_create_zero_rows_and_preserve_diagnostic(production_db):
    diagnostics: list[dict] = []
    reconciler = _reconciler("jose@example.com", "live", diagnostics)
    expired = _broker_position("SPY260716P00751000")
    for _ in range(500):
        _poll(reconciler, [expired])
    assert _count(production_db) == 0
    assert len(diagnostics) == 500
    assert diagnostics[-1]["reason_code"] == "BROKER_IMPORT_EXPIRED_CONTRACT_QUARANTINED"
    assert diagnostics[-1]["execution_mode"] == "live"
    assert diagnostics[-1]["broker_quantity"] == 4
    assert diagnostics[-1]["broker_cost_basis"] == 244.0


def test_500_valid_polls_create_one_row_and_timestamp_does_not_change_identity(production_db):
    diagnostics: list[dict] = []
    reconciler = _reconciler("jose@example.com", "live", diagnostics)
    contract = "SPY260821P00751000"
    for index in range(500):
        _poll(reconciler, [_broker_position(contract, acquired=f"2026-07-18T14:{index % 60:02d}:00Z")])
    assert _count(production_db) == 1
    with production_db.cursor() as cursor:
        cursor.execute("SELECT execution_mode, plan_id FROM positions")
        mode, plan_id = cursor.fetchone()
    assert mode == "live"
    assert plan_id.startswith(f"reconciled:{contract}:")


def test_terminal_import_is_recognized_on_next_poll(production_db):
    diagnostics: list[dict] = []
    reconciler = _reconciler("jose@example.com", "paper", diagnostics)
    position = _broker_position("QQQ260821C00500000")
    _poll(reconciler, [position])
    with production_db.cursor() as cursor:
        cursor.execute("UPDATE positions SET status='EXPIRED', quantity_remaining=0")
    summary = _poll(reconciler, [position])
    assert _count(production_db) == 1
    assert summary["positions_import_idempotent"] == 1


def test_client_and_mode_isolation_and_distinct_durable_lots(production_db):
    first = _reconciler("jose@example.com", "live", [])
    second = _reconciler("jason@example.com", "paper", [])
    contract = "AAPL260821C00200000"
    _poll(first, [
        _broker_position(contract, lot_id="lot-live-1"),
        _broker_position(contract, lot_id="lot-live-2"),
    ])
    _poll(second, [_broker_position(contract, lot_id="lot-paper-1")])
    assert _count(production_db, "jose@example.com") == 2
    assert _count(production_db, "jason@example.com") == 1
    with production_db.cursor() as cursor:
        cursor.execute(
            "SELECT client_id, execution_mode, count(*) FROM positions "
            "GROUP BY client_id, execution_mode ORDER BY client_id"
        )
        assert cursor.fetchall() == [
            ("jason@example.com", "paper", 1),
            ("jose@example.com", "live", 2),
        ]


def test_missing_execution_identity_fails_closed(production_db):
    diagnostics: list[dict] = []
    reconciler = _reconciler("jose@example.com", None, diagnostics)
    summary = _poll(reconciler, [_broker_position("MSFT260821C00400000")])
    assert _count(production_db) == 0
    assert summary["positions_alerted"] == 1
    assert diagnostics[-1]["reason_code"] == "BROKER_IMPORT_IDENTITY_UNPROVEN"


def test_existing_expired_row_is_closed_without_replacement(production_db):
    diagnostics: list[dict] = []
    reconciler = _reconciler("jose@example.com", "live", diagnostics)
    contract = "SPY260716P00751000"
    now = datetime.now(timezone.utc)
    with production_db.cursor() as cursor:
        cursor.execute(
            "INSERT INTO positions "
            "(id,client_id,execution_mode,plan_id,signal_id,underlying,contract,direction,qty,"
            "quantity_remaining,avg_fill,status,entry_ts,created_at,updated_at) "
            "VALUES ('existing','jose@example.com','live','real-plan','real-signal','SPY',%s,'PUT',4,4,0.61,'OPEN',%s,%s,%s)",
            (contract, now, now, now),
        )
    _poll(reconciler, [_broker_position(contract)])
    assert _count(production_db) == 1
    with production_db.cursor() as cursor:
        cursor.execute("SELECT status, close_source FROM positions WHERE id='existing'")
        status, close_source = cursor.fetchone()
    assert status == "EXPIRED"
    assert close_source == "expired_contract_cleanup"
