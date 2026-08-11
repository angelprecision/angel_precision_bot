"""PR #423 replacement lifecycle proof against real PostgreSQL JSONB/CAS semantics."""

from __future__ import annotations

import copy
import json
import os
import uuid
from contextlib import contextmanager

import pytest

psycopg2 = pytest.importorskip("psycopg2")
from psycopg2 import sql  # noqa: E402

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
os.environ.setdefault("ENCRYPTION_KEY", "ap-pr423-postgres-lifecycle-test")

from ap_exit_engine import APExitEngine, ManagedPosition  # noqa: E402


def _database_url() -> str:
    return (
        os.getenv("EXIT_RETRY_POSTGRES_TEST_URL")
        or os.getenv("INTELLIGENCE_POSTGRES_TEST_URL")
        or ""
    )


@pytest.mark.skipif(not _database_url(), reason="PR #423 PostgreSQL URL is not configured")
def test_replacement_lifecycle_jsonb_merge_and_generation_fence(monkeypatch):
    """The pending authority survives hydration and cannot cross client/gen CAS."""
    database_url = _database_url()
    schema = f"pr423_{uuid.uuid4().hex}"
    setup_conn = psycopg2.connect(database_url)
    setup_conn.autocommit = True
    setup_cur = setup_conn.cursor()
    try:
        setup_cur.execute(sql.SQL("CREATE SCHEMA {}" ).format(sql.Identifier(schema)))
        setup_cur.execute(
            sql.SQL(
                "CREATE TABLE {}.positions ("
                "id text PRIMARY KEY, client_id text NOT NULL, execution_mode text NOT NULL, "
                "meta jsonb NOT NULL DEFAULT '{}'::jsonb, updated_at timestamptz NOT NULL DEFAULT NOW()"
                ")"
            ).format(sql.Identifier(schema))
        )
        position_id = f"pos-{uuid.uuid4().hex}"
        client_id = "client-pr423"
        mode = "paper"
        old_local = "loc-old-pr423"
        old_broker = "bro-old-pr423"
        initial_lifecycle = {
            "state": "NONE",
            "replace_attempt": 0,
            "replacement_generation": 0,
            "replace_quantity": 0,
            "last_ack_identity": "",
        }
        setup_cur.execute(
            sql.SQL("INSERT INTO {}.positions (id, client_id, execution_mode, meta) VALUES (%s, %s, %s, %s::jsonb)")
            .format(sql.Identifier(schema)),
            (position_id, client_id, mode, json.dumps({"unrelated": {"keep": True}, "exit_retry_liveness": initial_lifecycle})),
        )
    finally:
        setup_cur.close()
        setup_conn.close()

    import ap.db as db_module

    @contextmanager
    def _real_pg_conn():
        connection = psycopg2.connect(database_url)
        connection.autocommit = False
        cursor = connection.cursor()
        cursor.execute(sql.SQL("SET search_path TO {}" ).format(sql.Identifier(schema)))
        try:
            yield cursor
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()
            connection.close()

    monkeypatch.setattr(db_module, "conn", _real_pg_conn)
    monkeypatch.setattr(db_module, "run_with_retry", lambda fn, *args, **kwargs: fn(*args, **kwargs))

    try:
        position = ManagedPosition(
            ticker="AVGO",
            option_symbol="AVGO260814C00350000",
            side="CALL",
            quantity=5,
            entry_price=2.49,
            underlying_entry=350.0,
            underlying_target=360.0,
            underlying_stop=340.0,
            position_id=position_id,
            client_id=client_id,
            execution_mode=mode,
            quantity_remaining=5,
        )
        position.exit_retry_liveness = {
            "state": "REPLACEMENT_PENDING",
            "replace_attempt": 1,
            "replacement_generation": 1,
            "replace_quantity": 1,
            "position_id": position_id,
            "client_id": client_id,
            "execution_mode": mode,
            "old_local_order_id": old_local,
            "old_broker_order_id": old_broker,
            "last_ack_identity": old_broker,
        }
        engine = APExitEngine(broker=object(), email=client_id)

        assert engine._persist_replacement_lifecycle(
            position, expected_state="NONE", expected_generation=0,
        ) is True

        with _real_pg_conn() as cursor:
            cursor.execute("SELECT client_id, execution_mode, meta FROM positions WHERE id=%s", (position_id,))
            row = cursor.fetchone()
        assert row[0] == client_id
        assert row[1] == mode
        assert row[2]["unrelated"] == {"keep": True}
        assert row[2]["exit_retry_liveness"]["state"] == "REPLACEMENT_PENDING"
        assert row[2]["exit_retry_liveness"]["replace_quantity"] == 1

        # Fresh-object hydration consumes only the exact nested lifecycle.
        hydrated = APExitEngine(broker=object(), email=client_id)
        fresh_position = copy.copy(position)
        fresh_position.exit_retry_liveness = dict(row[2]["exit_retry_liveness"])
        lifecycle, valid, reason = hydrated._replacement_lifecycle_for_position(fresh_position)
        assert valid, reason
        assert lifecycle["state"] == "REPLACEMENT_PENDING"
        assert lifecycle["client_id"] == client_id
        assert lifecycle["execution_mode"] == mode

        # Wrong client is an exact-row miss, not a broad position update.
        wrong_client = copy.copy(position)
        wrong_client.client_id = "other-client"
        wrong_client.exit_retry_liveness = dict(position.exit_retry_liveness)
        wrong_client.exit_retry_liveness["client_id"] = "other-client"
        assert hydrated._persist_replacement_lifecycle(
            wrong_client, expected_state="REPLACEMENT_PENDING", expected_generation=1,
        ) is False

        # A newer replacement generation wins the CAS; an old process cannot
        # overwrite it even when position/client/mode still match.
        newer = dict(position.exit_retry_liveness)
        newer["replacement_generation"] = 2
        newer["replace_attempt"] = 2
        with _real_pg_conn() as cursor:
            cursor.execute(
                "UPDATE positions SET meta=jsonb_set(meta, '{exit_retry_liveness}', %s::jsonb) WHERE id=%s",
                (json.dumps(newer), position_id),
            )
        assert hydrated._persist_replacement_lifecycle(
            position, expected_state="REPLACEMENT_PENDING", expected_generation=1,
        ) is False

        # Malformed generation metadata fails closed instead of becoming an
        # implicit zero-generation match.
        malformed = dict(newer)
        malformed["replacement_generation"] = "bad"
        with _real_pg_conn() as cursor:
            cursor.execute(
                "UPDATE positions SET meta=jsonb_set(meta, '{exit_retry_liveness}', %s::jsonb) WHERE id=%s",
                (json.dumps(malformed), position_id),
            )
        assert hydrated._persist_replacement_lifecycle(
            position, expected_state="REPLACEMENT_PENDING", expected_generation=1,
        ) is False

        # Restore a valid pending row, consume it once with the exact new
        # generation, then prove the stale pending writer cannot consume again.
        with _real_pg_conn() as cursor:
            cursor.execute(
                "UPDATE positions SET meta=jsonb_set(meta, '{exit_retry_liveness}', %s::jsonb) WHERE id=%s",
                (json.dumps(position.exit_retry_liveness), position_id),
            )
        owned = dict(position.exit_retry_liveness)
        owned.update({
            "state": "REPLACEMENT_OWNED_BY_NEW_GENERATION",
            "new_local_order_id": "loc-new-pr423",
            "new_broker_order_id": "bro-new-pr423",
        })
        position.exit_retry_liveness = owned
        assert hydrated._persist_replacement_lifecycle(
            position, expected_state="REPLACEMENT_PENDING", expected_generation=1,
        ) is True
        assert hydrated._persist_replacement_lifecycle(
            copy.copy(position), expected_state="REPLACEMENT_PENDING", expected_generation=1,
        ) is False
        with _real_pg_conn() as cursor:
            cursor.execute("SELECT meta FROM positions WHERE id=%s", (position_id,))
            final_meta = cursor.fetchone()[0]
        assert final_meta["unrelated"] == {"keep": True}
        assert final_meta["exit_retry_liveness"]["state"] == "REPLACEMENT_OWNED_BY_NEW_GENERATION"
        assert final_meta["exit_retry_liveness"]["new_broker_order_id"] == "bro-new-pr423"
    finally:
        cleanup_conn = psycopg2.connect(database_url)
        cleanup_conn.autocommit = True
        cleanup_cur = cleanup_conn.cursor()
        try:
            cleanup_cur.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
        finally:
            cleanup_cur.close()
            cleanup_conn.close()
