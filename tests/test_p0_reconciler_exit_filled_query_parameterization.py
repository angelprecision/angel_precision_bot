"""PR #527: driver-safe EXIT_FILLED backup-healing query regressions.

The production defect is DB-API interpolation: a literal percent sign in a
query that also has bound parameters is interpreted as another placeholder by
psycopg2.  The integration test below runs the actual reconciler method
through the project's ``_ConnWrapper`` and a real PostgreSQL connection, then
exercises the ownership and idempotency matrix around that query.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

# Importing ap.db requires a URL even when the integration test is skipped.
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@127.0.0.1:5432/test")

from ap_reconciler import APBrokerReconciler, _empty_summary


CLIENT = "pr527-client@example.com"
OTHER_CLIENT = "pr527-other@example.com"
FILLED_TS = datetime(2026, 8, 27, 16, 0, tzinfo=timezone.utc)


class _RecordingCursor:
    """Record SQL/parameters while preserving a real psycopg2 cursor."""

    def __init__(self, cursor, executions: list[tuple[str, tuple]], returned_rows: list[dict]):
        self._cursor = cursor
        self._executions = executions
        self._returned_rows = returned_rows

    def execute(self, sql, params=None):
        bound = tuple(params or ())
        self._executions.append((str(sql), bound))
        self._cursor.execute(sql, bound)
        return self

    def fetchall(self):
        rows = self._cursor.fetchall()
        self._returned_rows.extend(dict(row) for row in rows)
        return rows

    def fetchone(self):
        return self._cursor.fetchone()

    def close(self):
        self._cursor.close()

    def __getattr__(self, name):
        return getattr(self._cursor, name)


class _PostgresHarness:
    def __init__(self, connection, cursor_factory, conn_wrapper):
        self.connection = connection
        self.cursor_factory = cursor_factory
        self.conn_wrapper = conn_wrapper
        self.executions: list[tuple[str, tuple]] = []
        self.returned_rows: list[dict] = []

    @contextmanager
    def conn(self):
        cursor = self.connection.cursor(cursor_factory=self.cursor_factory)
        recording_cursor = _RecordingCursor(
            cursor,
            self.executions,
            self.returned_rows,
        )
        try:
            yield self.conn_wrapper(self.connection, recording_cursor)
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        finally:
            recording_cursor.close()

    def execute(self, sql: str, params: tuple = ()):
        with self.connection.cursor() as cursor:
            cursor.execute(sql, params)
        self.connection.commit()

    def insert_position(
        self,
        position_id: str,
        client_id: str,
        *,
        complete: bool = False,
    ) -> None:
        if complete:
            finalization = (0.99, 1.41, 90.0, 0)
        else:
            finalization = (None, None, None, 3)
        self.execute(
            """
            INSERT INTO positions (
                id, client_id, avg_fill, exit_price, realized_pnl,
                realized_pnl_pct, quantity_remaining
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (position_id, client_id, 0.52, *finalization),
        )

    def insert_order(
        self,
        local_order_id: str,
        position_id: str,
        client_id: str = CLIENT,
        *,
        kind: str = "EXIT",
        status: str = "EXIT_FILLED",
        fill_price: float | None = 0.99,
        filled_qty: int | None = 3,
        meta: dict | None = None,
    ) -> None:
        self.execute(
            """
            INSERT INTO orders (
                local_order_id, position_id, client_id, kind, status,
                fill_price, filled_qty, filled_ts, broker_order_id, meta
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                local_order_id,
                position_id,
                client_id,
                kind,
                status,
                fill_price,
                filled_qty,
                FILLED_TS,
                f"broker-{local_order_id}",
                json.dumps(meta or {}),
            ),
        )

    def mark_position_complete(self, position_id: str, client_id: str) -> None:
        self.execute(
            """
            UPDATE positions
            SET exit_price = 0.99,
                realized_pnl = 1.41,
                realized_pnl_pct = 90.0,
                quantity_remaining = 0
            WHERE id = %s AND client_id = %s
            """,
            (position_id, client_id),
        )


@pytest.fixture()
def postgres_harness():
    psycopg2 = pytest.importorskip("psycopg2")
    extras = pytest.importorskip("psycopg2.extras")

    database_url = (
        os.getenv("INTELLIGENCE_POSTGRES_TEST_URL")
        or os.getenv("DATABASE_URL")
        or ""
    ).strip()
    if not database_url:
        pytest.skip("PostgreSQL URL not configured")

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
                CREATE TEMP TABLE positions (
                    id TEXT NOT NULL,
                    client_id TEXT NOT NULL,
                    avg_fill NUMERIC,
                    exit_price NUMERIC,
                    realized_pnl NUMERIC,
                    realized_pnl_pct NUMERIC,
                    quantity_remaining INTEGER
                )
                """
            )
            cursor.execute(
                """
                CREATE TEMP TABLE orders (
                    local_order_id TEXT,
                    position_id TEXT,
                    client_id TEXT NOT NULL,
                    kind TEXT,
                    status TEXT,
                    fill_price NUMERIC,
                    filled_qty INTEGER,
                    filled_ts TIMESTAMPTZ,
                    broker_order_id TEXT,
                    meta JSONB
                )
                """
            )
        connection.commit()
        yield _PostgresHarness(connection, extras.RealDictCursor, _ConnWrapper)
    finally:
        connection.rollback()
        connection.close()


def _reconciler(client_id: str = CLIENT, broker=None) -> APBrokerReconciler:
    return APBrokerReconciler(
        broker=broker or MagicMock(),
        client_id=client_id,
        osm=MagicMock(),
        pm=MagicMock(),
        execution_mode="live",
    )


def test_real_postgres_query_enforces_ownership_and_is_idempotent(
    postgres_harness,
    monkeypatch,
):
    """The real driver executes the fixed query and only heals bot-owned truth."""
    harness = postgres_harness

    # Positive control: production-shaped bot-owned EXIT_FILLED row.
    harness.insert_position("pos-positive", CLIENT)
    harness.insert_order(
        "bot-exit-positive",
        "pos-positive",
        meta={"selected_row_marker": "present"},
    )

    # External/manual rows: both ownership fences must exclude these.
    harness.insert_position("pos-prefix", CLIENT)
    harness.insert_order("external-exit:manual-1", "pos-prefix")
    harness.insert_position("pos-meta", CLIENT)
    harness.insert_order(
        "bot-exit-meta",
        "pos-meta",
        meta={"external_broker_order": True},
    )

    # Client and join isolation controls.
    harness.insert_position("pos-other-client", OTHER_CLIENT)
    harness.insert_order("other-client-exit", "pos-other-client", OTHER_CLIENT)
    harness.insert_position("cross-order-own-position-other", OTHER_CLIENT)
    harness.insert_order("cross-order-own-position-other", "cross-order-own-position-other")
    harness.insert_position("cross-order-other-position-own", CLIENT)
    harness.insert_order(
        "cross-order-other-position-own",
        "cross-order-other-position-own",
        OTHER_CLIENT,
    )

    # Non-EXIT and wrong-status controls.
    harness.insert_position("pos-entry", CLIENT)
    harness.insert_order("entry-filled", "pos-entry", kind="ENTRY")
    for status in ("SUBMITTED", "PARTIAL", "CANCELED", "EXPIRED"):
        position_id = f"pos-status-{status.lower()}"
        harness.insert_position(position_id, CLIENT)
        harness.insert_order(
            f"exit-status-{status.lower()}",
            position_id,
            status=status,
        )

    # Fill-truth controls.
    harness.insert_position("pos-null-price", CLIENT)
    harness.insert_order("exit-null-price", "pos-null-price", fill_price=None)
    harness.insert_position("pos-zero-qty", CLIENT)
    harness.insert_order("exit-zero-qty", "pos-zero-qty", filled_qty=0)

    # Already-finalized positions must not be re-finalized.
    harness.insert_position("pos-complete", CLIENT, complete=True)
    harness.insert_order("bot-exit-complete", "pos-complete")

    import ap.db as db_mod
    import ap.position_manager as pm_mod

    monkeypatch.setattr(db_mod, "conn", harness.conn)
    monkeypatch.setattr(db_mod, "run_with_retry", lambda fn: fn())

    finalizer_calls: list[dict] = []

    def _finalize(**kwargs):
        finalizer_calls.append(dict(kwargs))
        harness.mark_position_complete(kwargs["position_id"], CLIENT)
        return True

    finalizer = MagicMock(side_effect=_finalize)
    monkeypatch.setattr(
        pm_mod,
        "APPositionManager",
        lambda client_id: SimpleNamespace(close_position_from_exit_fill=finalizer),
    )

    broker = MagicMock()
    rec = _reconciler(broker=broker)
    summary = _empty_summary(CLIENT)
    rec._heal_exit_filled_positions_from_orders(summary)

    assert summary["errors"] == []
    assert summary["positions_corrected"] == 1
    assert [call["position_id"] for call in finalizer_calls] == ["pos-positive"]
    assert finalizer_calls[0]["exit_price"] == pytest.approx(0.99)
    assert finalizer_calls[0]["filled_qty"] == 3

    # The production row shape includes o.meta and the real wrapper returns it.
    assert len(harness.returned_rows) == 1
    selected_row = harness.returned_rows[0]
    assert selected_row["local_order_id"] == "bot-exit-positive"
    assert selected_row["position_id"] == "pos-positive"
    assert float(selected_row["fill_price"]) == pytest.approx(0.99)
    assert selected_row["filled_qty"] == 3
    assert selected_row["broker_order_id"] == "broker-bot-exit-positive"
    assert selected_row["meta"] == {"selected_row_marker": "present"}

    query, params = harness.executions[0]
    # Finalizer updates use the direct connection so the bound reconciler query
    # remains easy to inspect.
    compact_query = " ".join(query.split())
    assert "o.meta" in compact_query
    assert "COALESCE(o.local_order_id, '') NOT LIKE %s" in compact_query
    assert params == (CLIENT, "external-exit:%")

    # No broker mutation surface is touched by this database-only repair.
    broker.submit_order.assert_not_called()
    broker.cancel_order.assert_not_called()

    # A completed position no longer matches the SQL finalization-gap fence.
    second_summary = _empty_summary(CLIENT)
    rec._heal_exit_filled_positions_from_orders(second_summary)
    assert second_summary["errors"] == []
    assert second_summary["positions_corrected"] == 0
    assert len(finalizer_calls) == 1


def test_application_fence_rechecks_returned_metadata_and_preserves_bound_pattern(monkeypatch):
    """Legacy/test-double rows cannot bypass the metadata external-row fence."""
    import ap.db as db_mod
    import ap.position_manager as pm_mod

    row = {
        "local_order_id": "bot-looking-external",
        "position_id": "pos-meta-double",
        "fill_price": 0.99,
        "filled_qty": 3,
        "filled_ts": "2026-08-27T16:00:00+00:00",
        "broker_order_id": "broker-meta-double",
        "meta": {"external_broker_order": True},
    }
    executions: list[tuple[str, tuple]] = []

    class _Cursor:
        def execute(self, sql, params=None):
            executions.append((" ".join(str(sql).split()), tuple(params or ())))
            return self

        def fetchall(self):
            return [row]

    @contextmanager
    def _conn():
        yield _Cursor()

    finalizer = MagicMock()
    monkeypatch.setattr(db_mod, "conn", lambda: _conn())
    monkeypatch.setattr(db_mod, "run_with_retry", lambda fn: fn())
    monkeypatch.setattr(
        pm_mod,
        "APPositionManager",
        lambda client_id: SimpleNamespace(close_position_from_exit_fill=finalizer),
    )

    rec = _reconciler()
    summary = _empty_summary(CLIENT)
    rec._heal_exit_filled_positions_from_orders(summary)

    finalizer.assert_not_called()
    assert summary["errors"] == []
    query, params = executions[0]
    assert "o.meta" in query
    assert "NOT LIKE %s" in query
    assert params == (CLIENT, "external-exit:%")
