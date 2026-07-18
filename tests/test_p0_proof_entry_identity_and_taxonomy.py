from __future__ import annotations

from contextlib import contextmanager
import os
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

import ap.proof_taxonomy_guard as guard


def _identity(**overrides) -> guard.EntryIdentity:
    values = {
        "client_id": "client@example.com",
        "position_id": "position-1",
        "local_order_id": "entry-order-1",
        "broker_order_id": "entry-broker-1",
        "execution_mode": "paper",
        "signal_id": "signal-1",
        "canonical_signal_id": "canonical-1",
        "filled_qty": 1,
        "fill_price": 1.25,
        "filled_ts": "2026-07-16T18:40:46Z",
        "synthetic_entry": False,
    }
    values.update(overrides)
    return guard.EntryIdentity(**values)


def test_taxonomy_only_official_live_is_training_eligible() -> None:
    official = guard.classify_performance_taxonomy(
        {"execution_mode": "live", "official_live_performance_eligible": True}
    )
    assert official["performance_taxonomy"] == "LIVE_OFFICIAL"
    assert official["training_eligible"] is True
    assert official["quote_domain_consistent"] is True

    live_unreconciled = guard.classify_performance_taxonomy(
        {"execution_mode": "live", "official_live_performance_eligible": False}
    )
    assert live_unreconciled["performance_taxonomy"] == "LIVE_UNRECONCILED"
    assert live_unreconciled["training_eligible"] is False
    assert live_unreconciled["quote_domain_consistent"] is False

    paper = guard.classify_performance_taxonomy(
        {"execution_mode": "paper", "official_live_performance_eligible": False}
    )
    assert paper["performance_taxonomy"] == "PAPER_UNVERIFIED"
    assert paper["training_eligible"] is False
    assert paper["quote_domain_consistent"] is False

    unknown = guard.classify_performance_taxonomy({"execution_mode": "unknown"})
    assert unknown["performance_taxonomy"] == "UNKNOWN_QUARANTINED"
    assert unknown["training_eligible"] is False


def test_taxonomy_migration_uses_exact_partial_training_index() -> None:
    migration = (
        Path(__file__).resolve().parents[1]
        / "migrations"
        / "20260716_proof_performance_taxonomy.sql"
    ).read_text()

    assert "ON proof_trades (client_email, position_id)" in migration
    assert "WHERE training_eligible IS TRUE" in migration
    assert "quote_domain_consistent = CASE" in migration


def test_proof_writer_uses_originating_entry_mode_not_explicit_live(monkeypatch) -> None:
    captured = {}

    def original(
        self,
        ticker,
        *,
        position_id="",
        local_order_id="",
        execution_mode="",
        synthetic_entry=False,
    ):
        captured.update(
            position_id=position_id,
            local_order_id=local_order_id,
            execution_mode=execution_mode,
            synthetic_entry=synthetic_entry,
        )
        return {
            "ticker": ticker,
            "position_id": position_id,
            "local_order_id": local_order_id,
            "execution_mode": execution_mode,
            "_proof_persisted": False,
        }

    monkeypatch.setattr(guard, "resolve_originating_entry_identity", lambda **kwargs: _identity())
    monkeypatch.setattr(
        guard,
        "_lifecycle_proof_stamp",
        lambda identity: {
            "position_id": identity.position_id,
            "local_order_id": identity.local_order_id,
            "execution_mode": identity.execution_mode,
            "performance_taxonomy": "PAPER_UNVERIFIED",
            "training_eligible": False,
        },
    )
    monkeypatch.setattr(guard, "_persist_stamp", lambda *args, **kwargs: None)

    wrapped = guard.wrap_log_trade(original)
    logger = SimpleNamespace(email="client@example.com")
    result = wrapped(
        logger,
        "SPY",
        position_id="position-1",
        local_order_id="exit-order-wrong",
        execution_mode="live",
    )

    assert captured == {
        "position_id": "position-1",
        "local_order_id": "entry-order-1",
        "execution_mode": "paper",
        "synthetic_entry": False,
    }
    assert result["execution_mode"] == "paper"
    assert result["training_eligible"] is False


def test_unresolved_supplied_order_cannot_become_live_proof(monkeypatch) -> None:
    captured = {}

    def original(
        self,
        ticker,
        *,
        position_id="",
        local_order_id="",
        execution_mode="",
        synthetic_entry=False,
    ):
        captured.update(local_order_id=local_order_id, execution_mode=execution_mode)
        return {"_proof_persisted": False}

    monkeypatch.setattr(guard, "resolve_originating_entry_identity", lambda **kwargs: None)
    monkeypatch.setattr(
        guard,
        "_lifecycle_proof_stamp",
        lambda identity: {
            "execution_mode": "unknown",
            "performance_taxonomy": "UNKNOWN_QUARANTINED",
            "training_eligible": False,
        },
    )
    monkeypatch.setattr(guard, "_persist_stamp", lambda *args, **kwargs: None)

    wrapped = guard.wrap_log_trade(original)
    result = wrapped(
        SimpleNamespace(email="client@example.com"),
        "SPY",
        position_id="position-1",
        local_order_id="exit-order-1",
        execution_mode="live",
    )

    assert captured == {"local_order_id": "", "execution_mode": "unknown"}
    assert result["performance_taxonomy"] == "UNKNOWN_QUARANTINED"
    assert result["training_eligible"] is False


def test_master_control_stamps_sizer_with_runtime_mode() -> None:
    def original(self, *, mode, position_sizer):
        self.mode = str(mode).upper()
        self.sizer = position_sizer

    wrapped = guard.wrap_master_control_init(original)
    control = SimpleNamespace()
    sizer = SimpleNamespace()
    wrapped(control, mode="LIVE", position_sizer=sizer)
    assert sizer.execution_mode == "live"


def test_paper_and_unknown_history_never_query_database() -> None:
    calls = []

    def original(self, client_id):
        calls.append(client_id)
        return [{"realized_pnl": 999}]

    wrapped = guard.wrap_fetch_history(original)
    assert wrapped(SimpleNamespace(execution_mode="paper"), "paper@example.com") == []
    assert wrapped(SimpleNamespace(execution_mode="unknown"), "unknown@example.com") == []
    assert calls == []


def test_live_kelly_history_requires_mode_and_training_eligible(monkeypatch) -> None:
    captured = {}

    class Cursor:
        def execute(self, sql, params):
            captured["sql"] = sql
            captured["params"] = params
            return self

        def fetchall(self):
            return [
                {
                    "realized_pnl": 80.0,
                    "avg_fill": 1.20,
                    "exit_price": 2.00,
                    "qty": 1,
                }
            ]

    @contextmanager
    def fake_conn():
        yield Cursor()

    monkeypatch.setattr(guard.db, "conn", fake_conn)
    monkeypatch.setattr(guard.db, "run_with_retry", lambda fn: fn())

    wrapped = guard.wrap_fetch_history(lambda self, client_id: [{"realized_pnl": -999}])
    rows = wrapped(SimpleNamespace(execution_mode="live"), "live@example.com")

    assert rows == [
        {
            "realized_pnl": 80.0,
            "avg_fill": 1.20,
            "exit_price": 2.00,
            "qty": 1,
        }
    ]
    assert "LOWER(COALESCE(p.execution_mode,''))='live'" in captured["sql"]
    assert "pt.training_eligible IS TRUE" in captured["sql"]
    assert "EXISTS" in captured["sql"]
    assert captured["params"] == ("live@example.com",)


def test_missing_taxonomy_schema_falls_back_to_tier_history(monkeypatch) -> None:
    monkeypatch.setattr(
        guard.db,
        "run_with_retry",
        lambda fn: (_ for _ in ()).throw(RuntimeError("missing column")),
    )
    wrapped = guard.wrap_fetch_history(lambda self, client_id: [{"realized_pnl": 999}])
    assert wrapped(SimpleNamespace(execution_mode="live"), "live@example.com") == []


def test_supplied_entry_for_different_position_is_rejected(monkeypatch) -> None:
    class Result:
        def __init__(self, row=None, rows=None):
            self._row = row
            self._rows = rows or []

        def fetchone(self):
            return self._row

        def fetchall(self):
            return self._rows

    class Cursor:
        def execute(self, sql, params):
            if "FROM positions" in sql:
                return Result({"id": "position-1", "local_order_id": "entry-order-canonical"})
            if params[1] == "entry-order-wrong":
                return Result(
                    {
                        "local_order_id": "entry-order-wrong",
                        "position_id": "position-2",
                        "kind": "ENTRY",
                        "status": "FILLED",
                        "execution_mode": "live",
                        "filled_qty": 1,
                        "fill_price": 1.0,
                    }
                )
            if params[1] == "entry-order-canonical":
                return Result(
                    {
                        "local_order_id": "entry-order-canonical",
                        "position_id": "position-1",
                        "kind": "ENTRY",
                        "status": "FILLED",
                        "execution_mode": "paper",
                        "filled_qty": 1,
                        "fill_price": 1.0,
                    }
                )
            raise AssertionError((sql, params))

    @contextmanager
    def fake_conn():
        yield Cursor()

    monkeypatch.setattr(guard.db, "conn", fake_conn)
    monkeypatch.setattr(guard.db, "run_with_retry", lambda fn: fn())

    identity = guard.resolve_originating_entry_identity(
        client_id="client@example.com",
        position_id="position-1",
        supplied_local_order_id="entry-order-wrong",
    )
    assert identity is not None
    assert identity.local_order_id == "entry-order-canonical"
    assert identity.position_id == "position-1"
    assert identity.execution_mode == "paper"


def _postgres_connection_or_skip():
    psycopg2 = pytest.importorskip("psycopg2")
    url = os.getenv("INTELLIGENCE_POSTGRES_TEST_URL")
    if not url:
        if os.getenv("GITHUB_ACTIONS") == "true":
            pytest.fail("INTELLIGENCE_POSTGRES_TEST_URL is required in GitHub Actions")
        pytest.skip("PostgreSQL integration URL unavailable")
    try:
        return psycopg2.connect(url)
    except Exception as exc:
        if os.getenv("GITHUB_ACTIONS") == "true":
            pytest.fail(f"PostgreSQL integration unavailable: {exc}")
        pytest.skip(f"PostgreSQL integration unavailable: {exc}")


def _apply_postgres_proof_migrations(connection) -> None:
    root = Path(__file__).resolve().parents[1]
    generation_sql = (root / "migrations" / "20260717_exit_decision_generation_claims.sql").read_text()
    taxonomy_sql = (root / "migrations" / "20260716_proof_performance_taxonomy.sql").read_text()
    with connection.cursor() as cursor:
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS public.proof_trades ("
            "id BIGSERIAL PRIMARY KEY, "
            "client_email TEXT, "
            "position_id TEXT, "
            "closed_at TIMESTAMPTZ)"
        )
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS public.positions ("
            "id TEXT PRIMARY KEY, "
            "client_id TEXT, "
            "execution_mode TEXT, "
            "status TEXT, "
            "realized_pnl NUMERIC, "
            "avg_fill NUMERIC, "
            "exit_price NUMERIC, "
            "qty INTEGER, "
            "entry_ts TIMESTAMPTZ)"
        )
        cursor.execute(generation_sql)
        cursor.execute(
            "SELECT to_regclass('public.exit_decision_generation_claims') IS NOT NULL"
        )
        assert cursor.fetchone()[0] is True
        cursor.execute(taxonomy_sql)
        cursor.execute(taxonomy_sql)


def _cleanup_postgres_proof_rows(connection, client_id: str, position_id: str) -> None:
    with connection.cursor() as cursor:
        cursor.execute("DELETE FROM public.proof_trades WHERE client_email=%s OR position_id=%s", (client_id, position_id))
        cursor.execute("DELETE FROM public.positions WHERE client_id=%s OR id=%s", (client_id, position_id))


def test_migrations_run_in_stack_order_and_are_idempotent_on_postgres() -> None:
    connection = _postgres_connection_or_skip()
    connection.autocommit = True
    try:
        _apply_postgres_proof_migrations(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema='public' AND table_name='proof_trades'"
            )
            columns = {row[0] for row in cursor.fetchall()}
            assert {
                "execution_mode",
                "official_live_performance_eligible",
                "performance_taxonomy",
                "training_eligible",
                "taxonomy_reason",
                "quote_domain_consistent",
            } <= columns
            cursor.execute(
                "SELECT indexname, indexdef FROM pg_indexes "
                "WHERE schemaname='public' AND tablename='proof_trades' "
                "AND indexname='idx_proof_trades_live_training_position'"
            )
            index_row = cursor.fetchone()
            assert index_row is not None
            assert "WHERE training_eligible IS TRUE" in index_row[1]
    finally:
        connection.close()


def test_duplicate_official_proofs_do_not_duplicate_live_kelly_history(monkeypatch) -> None:
    connection = _postgres_connection_or_skip()
    connection.autocommit = True
    client_id = f"live-proof-{uuid4()}@example.com"
    position_id = f"position-{uuid4()}"
    try:
        from psycopg2.extras import RealDictCursor
        _apply_postgres_proof_migrations(connection)
        _cleanup_postgres_proof_rows(connection, client_id, position_id)

        cursor = connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(
            "INSERT INTO public.positions "
            "(id, client_id, execution_mode, status, realized_pnl, avg_fill, exit_price, qty, entry_ts) "
            "VALUES (%s, %s, 'live', 'CLOSED', 80, 1.2, 2.0, 1, NOW())",
            (position_id, client_id),
        )
        cursor.execute(
            "INSERT INTO public.proof_trades "
            "(client_email, position_id, closed_at, execution_mode, "
            " official_live_performance_eligible, performance_taxonomy, training_eligible, "
            " taxonomy_reason, quote_domain_consistent) "
            "VALUES "
            "(%s, %s, NOW(), 'live', TRUE, 'LIVE_OFFICIAL', TRUE, 'existing_tradier_exit_proof_lock_passed', TRUE), "
            "(%s, %s, NOW(), 'live', TRUE, 'LIVE_OFFICIAL', TRUE, 'existing_tradier_exit_proof_lock_passed', TRUE)",
            (client_id, position_id, client_id, position_id),
        )

        @contextmanager
        def same_connection():
            yield cursor

        monkeypatch.setattr(guard.db, "conn", same_connection)
        monkeypatch.setattr(guard.db, "run_with_retry", lambda fn: fn())
        wrapped = guard.wrap_fetch_history(lambda *_: [])
        rows = wrapped(SimpleNamespace(execution_mode="live"), client_id)
        assert len(rows) == 1
        assert float(rows[0]["realized_pnl"]) == 80.0
    finally:
        try:
            _cleanup_postgres_proof_rows(connection, client_id, position_id)
        finally:
            connection.close()
