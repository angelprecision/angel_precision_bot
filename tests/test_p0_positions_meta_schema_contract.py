"""P0 regression: production positions.meta schema must match QPM runtime SQL.

Pins the 2026-08-11 production incident where QPM persisted
hard_exit_reference into positions.meta every quote cycle while production had
no such column. The runtime caught the SQL exception as non-fatal, so the
persistence watermark never advanced and the missing-column write repeated.
"""
from __future__ import annotations

import ast
import os
from pathlib import Path
from uuid import uuid4

import pytest

import ap.schema_attestation as schema_attestation


REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = REPO_ROOT / "migrations" / "20260811_positions_meta_hard_exit_reference.sql"
QPM_PATH = REPO_ROOT / "ap" / "position_quote_monitor.py"


def _production_qpm_positions_update_sql() -> str:
    """Extract the real SQL literal from _persist_quote_to_db, not a formula copy."""
    tree = ast.parse(QPM_PATH.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.name != "_persist_quote_to_db":
            continue
        for child in ast.walk(node):
            if not isinstance(child, ast.Call) or not child.args:
                continue
            func = child.func
            if not isinstance(func, ast.Attribute) or func.attr != "execute":
                continue
            sql_arg = child.args[0]
            if isinstance(sql_arg, ast.Constant) and isinstance(sql_arg.value, str):
                sql = sql_arg.value
                if "UPDATE positions" in sql and "jsonb_set" in sql:
                    return sql
    raise AssertionError("Could not find QPM positions persistence SQL")


def test_required_schema_declares_positions_meta():
    assert "positions" in schema_attestation.REQUIRED_SCHEMA
    assert "meta" in schema_attestation.REQUIRED_SCHEMA["positions"]


def test_live_attestation_fails_closed_when_positions_meta_is_missing(monkeypatch):
    required = {"positions": frozenset({"id", "client_id", "meta"})}
    monkeypatch.setenv("BOT_MODE", "LIVE")
    monkeypatch.delenv("SCHEMA_ATTESTATION_STRICT", raising=False)
    monkeypatch.setattr(
        schema_attestation,
        "_fetch_actual_schema",
        lambda _required: {"positions": {"id", "client_id"}},
    )

    with pytest.raises(schema_attestation.SchemaAttestationError, match="meta"):
        schema_attestation.attest_schema(required=required)


def test_live_attestation_succeeds_when_positions_meta_is_present(monkeypatch):
    required = {"positions": frozenset({"id", "client_id", "meta"})}
    monkeypatch.setenv("BOT_MODE", "LIVE")
    monkeypatch.delenv("SCHEMA_ATTESTATION_STRICT", raising=False)
    monkeypatch.setattr(
        schema_attestation,
        "_fetch_actual_schema",
        lambda _required: {"positions": {"id", "client_id", "meta"}},
    )

    report = schema_attestation.attest_schema(required=required)

    assert report["ok"] is True
    assert report["strict"] is True
    assert report["missing_columns"] == {}


def test_migration_is_idempotent_jsonb_contract():
    sql = MIGRATION_PATH.read_text(encoding="utf-8")
    normalized = " ".join(sql.split()).lower()
    assert "alter table positions add column if not exists meta jsonb" in normalized
    assert "alter column meta set default '{}'::jsonb" in normalized
    assert "alter column meta set not null" in normalized


def test_migration_closes_exact_production_qpm_sql_gap():
    psycopg2 = pytest.importorskip("psycopg2")
    dsn = os.getenv("P0_SCHEMA_TEST_DATABASE_URL", "").strip()
    if not dsn:
        pytest.skip("P0_SCHEMA_TEST_DATABASE_URL not configured")

    schema_name = f"p0_positions_meta_{uuid4().hex[:12]}"
    runtime_sql = _production_qpm_positions_update_sql()
    migration_sql = MIGRATION_PATH.read_text(encoding="utf-8")
    hard_ref_json = (
        '{"price":1.25,"pnl_pct":0.10,"source":"bid",'
        '"validity":"proven","ts":"2026-08-11T18:00:00+00:00",'
        '"refresh_needed":false}'
    )
    params = (1.25, 100.0, 0.10, hard_ref_json, hard_ref_json, "pos-1", "client-1")

    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema_name}"')
            cur.execute(f'SET search_path TO "{schema_name}"')
            cur.execute(
                """
                CREATE TABLE positions (
                    id TEXT PRIMARY KEY,
                    client_id TEXT NOT NULL,
                    current_option_price DOUBLE PRECISION,
                    current_underlying DOUBLE PRECISION,
                    option_pnl_pct DOUBLE PRECISION,
                    updated_at TIMESTAMPTZ,
                    status TEXT,
                    quantity_remaining INTEGER
                )
                """
            )
            cur.execute(
                "INSERT INTO positions (id, client_id, status, quantity_remaining) "
                "VALUES ('pos-1', 'client-1', 'OPEN', 1)"
            )

            with pytest.raises(psycopg2.errors.UndefinedColumn):
                cur.execute(runtime_sql, params)

            cur.execute(migration_sql)
            cur.execute(
                "UPDATE positions SET meta = %s::jsonb WHERE id='pos-1'",
                ('{"existing":{"keep":true}}',),
            )
            # Idempotency: applying the same migration twice must remain safe
            # and preserve existing non-null metadata.
            cur.execute(migration_sql)
            cur.execute("SELECT meta->'existing' FROM positions WHERE id='pos-1'")
            assert cur.fetchone() == ({"keep": True},)

            cur.execute(runtime_sql, params)
            assert cur.rowcount == 1
            cur.execute(
                "SELECT meta->'existing', "
                "       meta->'hard_exit_reference'->>'source', "
                "       meta->'hard_exit_reference'->>'validity' "
                "FROM positions WHERE id='pos-1'"
            )
            assert cur.fetchone() == ({"keep": True}, "bid", "proven")

            cur.execute(
                """
                SELECT data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name = 'positions'
                  AND column_name = 'meta'
                """,
                (schema_name,),
            )
            assert cur.fetchone() == ("jsonb", "NO")
    finally:
        try:
            with conn.cursor() as cur:
                cur.execute("SET search_path TO public")
                cur.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
        finally:
            conn.close()
