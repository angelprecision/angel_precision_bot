"""PR #423 replacement lifecycle proof against real PostgreSQL JSONB/CAS semantics."""

from __future__ import annotations

import copy
import json
import os
import uuid
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock

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
                "contract text NOT NULL, status text NOT NULL, qty integer NOT NULL, "
                "quantity_remaining integer NOT NULL, "
                "updated_at timestamptz NOT NULL DEFAULT NOW()"
                ")"
            ).format(sql.Identifier(schema))
        )
        position_id = f"pos-{uuid.uuid4().hex}"
        client_id = "client-pr423"
        mode = "paper"
        old_local = "loc-old-pr423"
        old_broker = "bro-old-pr423"
        setup_cur.execute(
            sql.SQL(
                "INSERT INTO {}.positions ("
                "id, client_id, execution_mode, contract, status, qty, quantity_remaining"
                ") VALUES (%s, %s, %s, %s, %s, %s, %s)"
            )
            .format(sql.Identifier(schema)),
            (
                position_id,
                client_id,
                mode,
                "AVGO260814C00350000",
                "OPEN",
                5,
                5,
            ),
        )
        # This is deliberately a production-shaped pre-migration table.  The
        # test must fail before lifecycle/fill persistence if the amendment's
        # migration is missing or does not actually add the authority column.
        setup_cur.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema=%s AND table_name='positions' AND column_name='meta'",
            (schema,),
        )
        assert setup_cur.fetchone() is None

        migration_path = (
            Path(__file__).resolve().parents[1]
            / "migrations/20260811_positions_exit_retry_liveness_authority.sql"
        )
        migration_sql = migration_path.read_text(encoding="utf-8")
        qualified_positions = (
            sql.Identifier(schema).as_string(setup_conn) + ".positions"
        )
        setup_cur.execute(migration_sql.replace("public.positions", qualified_positions))
        setup_cur.execute(
            "SELECT data_type, is_nullable, column_default "
            "FROM information_schema.columns "
            "WHERE table_schema=%s AND table_name='positions' AND column_name='meta'",
            (schema,),
        )
        meta_column = setup_cur.fetchone()
        assert meta_column is not None
        assert meta_column[0] == "jsonb"
        assert meta_column[1] == "NO"
        assert meta_column[2] and "{}" in meta_column[2]
        setup_cur.execute(
            "SELECT qty, quantity_remaining, status FROM "
            f"{qualified_positions} WHERE id=%s",
            (position_id,),
        )
        assert setup_cur.fetchone() == (5, 5, "OPEN")
        setup_cur.execute(
            f"SELECT meta FROM {qualified_positions} WHERE id=%s",
            (position_id,),
        )
        assert setup_cur.fetchone()[0] == {}
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
        newer["replacement_generation"] = 5
        newer["replace_attempt"] = 4
        with _real_pg_conn() as cursor:
            cursor.execute(
                "UPDATE positions SET meta=jsonb_set(meta, '{exit_retry_liveness}', %s::jsonb) WHERE id=%s",
                (json.dumps(newer), position_id),
            )
        assert hydrated._persist_replacement_lifecycle(
            position, expected_state="REPLACEMENT_PENDING", expected_generation=1,
        ) is False

        # Explicit ABA regression: a stale process holding generation 4 must
        # not overwrite a newer durable generation 5.
        stale_generation = copy.copy(position)
        stale_generation.exit_retry_liveness = dict(position.exit_retry_liveness)
        stale_generation.exit_retry_liveness.update({
            "replace_attempt": 4,
            "replacement_generation": 4,
        })
        broker = MagicMock()
        stale_engine = APExitEngine(broker=broker, email=client_id)
        assert stale_engine._persist_replacement_lifecycle(
            stale_generation,
            expected_state="REPLACEMENT_PENDING",
            expected_generation=4,
        ) is False
        assert stale_generation.exit_retry_liveness["replacement_generation"] == 4
        assert broker.mock_calls == []
        with _real_pg_conn() as cursor:
            cursor.execute(
                "SELECT meta->'exit_retry_liveness'->>'replacement_generation' "
                "FROM positions WHERE id=%s",
                (position_id,),
            )
            assert cursor.fetchone()[0] == "5"

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

        wrong_identity = copy.copy(position)
        wrong_identity.exit_retry_liveness = dict(position.exit_retry_liveness)
        assert hydrated._persist_replacement_lifecycle(
            wrong_identity,
            expected_state="REPLACEMENT_OWNED_BY_NEW_GENERATION",
            expected_generation=1,
            expected_new_local_order_id="loc-stale-pr423",
            expected_new_broker_order_id="bro-new-pr423",
            expected_filled_qty=1,
        ) is False

        # The filled replacement may release liveness only with the exact
        # owned generation, both broker identities, and the filled tranche.
        consumed = copy.copy(position)
        consumed.exit_retry_liveness = dict(position.exit_retry_liveness)
        consumed.exit_retry_liveness.update({
            "state": "NONE",
            "replace_attempt": 0,
            "replace_quantity": 0,
            "last_ack_identity": "",
        })
        assert hydrated._persist_replacement_lifecycle(
            consumed,
            expected_state="REPLACEMENT_OWNED_BY_NEW_GENERATION",
            expected_generation=1,
            expected_new_local_order_id="loc-new-pr423",
            expected_new_broker_order_id="bro-new-pr423",
            expected_filled_qty=1,
        ) is True
        with _real_pg_conn() as cursor:
            cursor.execute("SELECT meta FROM positions WHERE id=%s", (position_id,))
            final_meta = cursor.fetchone()[0]
        assert final_meta["unrelated"] == {"keep": True}
        assert final_meta["exit_retry_liveness"]["state"] == "NONE"
        assert final_meta["exit_retry_liveness"]["replacement_generation"] == 1

        # The same migrated row now proves the position-side fill bridge.  A
        # cumulative replay is idempotent, while a later cumulative fill
        # consumes only the exact delta.
        fill_position = copy.copy(position)
        fill_position.quantity_remaining = 5
        fill_position.exit_retry_liveness = dict(final_meta["exit_retry_liveness"])
        fill_engine = APExitEngine(broker=object(), email=client_id)
        fill_engine._positions.append(fill_position)
        fill_engine._positions_by_id[position_id] = fill_position

        first_fill = fill_engine.reconcile_exit_fill_consumption(
            position_id,
            local_order_id=old_local,
            broker_order_id=old_broker,
            cumulative_filled_qty=1,
            prior_cumulative_filled=0,
        )
        assert first_fill["ok"] is True
        assert first_fill["applied_delta"] == 1
        assert first_fill["quantity_remaining"] == 4

        replay_fill = fill_engine.reconcile_exit_fill_consumption(
            position_id,
            local_order_id=old_local,
            broker_order_id=old_broker,
            cumulative_filled_qty=1,
            prior_cumulative_filled=1,
        )
        assert replay_fill["ok"] is True
        assert replay_fill["applied_delta"] == 0
        assert replay_fill["quantity_remaining"] == 4

        second_fill = fill_engine.reconcile_exit_fill_consumption(
            position_id,
            local_order_id=old_local,
            broker_order_id=old_broker,
            cumulative_filled_qty=2,
            prior_cumulative_filled=1,
        )
        assert second_fill["ok"] is True
        assert second_fill["applied_delta"] == 1
        assert second_fill["quantity_remaining"] == 3

        with _real_pg_conn() as cursor:
            cursor.execute(
                "SELECT quantity_remaining, meta FROM positions WHERE id=%s",
                (position_id,),
            )
            consumed_remaining, consumed_meta = cursor.fetchone()
        assert consumed_remaining == 3
        assert consumed_meta["exit_fill_consumption"]["applied_cumulative_qty"] == 2

        # A fresh process hydrates the exact marker and replays cumulative=2
        # without consuming the position a second time.
        from ap_exit_engine import _restore_exit_fill_consumption_from_meta

        restored_marker, marker_valid, marker_reason = _restore_exit_fill_consumption_from_meta(
            consumed_meta
        )
        assert marker_valid, marker_reason
        assert restored_marker["applied_cumulative_qty"] == 2
        restart_position = copy.copy(position)
        restart_position.quantity_remaining = 3
        restart_position.exit_retry_liveness = dict(
            consumed_meta["exit_retry_liveness"]
        )
        restart_position.exit_fill_consumption = dict(restored_marker)
        restart_engine = APExitEngine(broker=object(), email=client_id)
        restart_engine._positions.append(restart_position)
        restart_engine._positions_by_id[position_id] = restart_position
        restart_replay = restart_engine.reconcile_exit_fill_consumption(
            position_id,
            local_order_id=old_local,
            broker_order_id=old_broker,
            cumulative_filled_qty=2,
            prior_cumulative_filled=2,
        )
        assert restart_replay["ok"] is True
        assert restart_replay["applied_delta"] == 0
        assert restart_replay["quantity_remaining"] == 3

        # Migration-boundary proof: an old active partial exit with a nonzero
        # OSM watermark but no exact position watermark is HOLD, never an
        # inferred zero/nonzero fill.
        legacy_position_id = f"legacy-{uuid.uuid4().hex}"
        with _real_pg_conn() as cursor:
            cursor.execute(
                "INSERT INTO positions ("
                "id, client_id, execution_mode, contract, status, qty, quantity_remaining, meta"
                ") VALUES (%s, %s, %s, %s, %s, %s, %s, '{}'::jsonb)",
                (
                    legacy_position_id,
                    client_id,
                    mode,
                    "AVGO260814C00350000",
                    "CLOSING",
                    5,
                    5,
                ),
            )
        legacy_position = copy.copy(position)
        legacy_position.position_id = legacy_position_id
        legacy_position.quantity_remaining = 5
        legacy_position.exit_retry_liveness = dict(final_meta["exit_retry_liveness"])
        legacy_engine = APExitEngine(broker=object(), email=client_id)
        legacy_engine._positions.append(legacy_position)
        legacy_engine._positions_by_id[legacy_position_id] = legacy_position
        legacy_result = legacy_engine.reconcile_exit_fill_consumption(
            legacy_position_id,
            local_order_id=old_local,
            broker_order_id=old_broker,
            cumulative_filled_qty=1,
            prior_cumulative_filled=1,
        )
        assert legacy_result["ok"] is False
        assert legacy_result["reason"] == "position_applied_cumulative_unavailable"
        with _real_pg_conn() as cursor:
            cursor.execute(
                "SELECT quantity_remaining, meta FROM positions WHERE id=%s",
                (legacy_position_id,),
            )
            legacy_remaining, legacy_meta = cursor.fetchone()
        assert legacy_remaining == 5
        assert legacy_meta == {}
    finally:
        cleanup_conn = psycopg2.connect(database_url)
        cleanup_conn.autocommit = True
        cleanup_cur = cleanup_conn.cursor()
        try:
            cleanup_cur.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
        finally:
            cleanup_cur.close()
            cleanup_conn.close()
