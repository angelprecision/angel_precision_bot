"""P0 #433 — production DB hot-path and schema-shape contracts.

The first three integration cases execute the exact current-main SQL before
and after the index migration.  The remaining cases pin the non-performance
contracts that must not move while the scan is optimized: malformed JSON is
still parsed in Python, client/mode ownership stays explicit, and known
production table shapes are not guessed by diagnostics.
"""
from __future__ import annotations

import ast
import json
import os
import re
import textwrap
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = REPO_ROOT / "migrations" / "20260811_orders_retry_hotpath_indexes.sql"
ORDER_MONITOR_PATH = REPO_ROOT / "ap" / "order_monitor.py"
MORNING_HANDOFF_PATH = REPO_ROOT / "ap" / "morning_handoff.py"
PREOPEN_READINESS_PATH = REPO_ROOT / "ap" / "preopen_readiness.py"
SCHEMA_ATTESTATION_PATH = REPO_ROOT / "ap" / "schema_attestation.py"

ARMED_INDEX = "idx_orders_entry_canceled_retry_armed_updated"
STALE_INDEX = "idx_orders_entry_canceled_retry_inflight_updated"
PENDING_INDEX = "idx_orders_entry_pending_trigger_recovery_created"


def _function_source(path: Path, name: str) -> str:
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return textwrap.dedent(ast.get_source_segment(source, node) or "")
    raise AssertionError(f"function {name!r} not found in {path}")


def _literal_sql(function_source: str, marker: str) -> str:
    tree = ast.parse(function_source)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        if not isinstance(node.func, ast.Attribute) or node.func.attr != "execute":
            continue
        sql_arg = node.args[0]
        if isinstance(sql_arg, ast.Constant) and isinstance(sql_arg.value, str):
            if marker in sql_arg.value:
                return " ".join(sql_arg.value.split())
    raise AssertionError(f"SQL literal containing {marker!r} not found")


def _armed_sql() -> str:
    return _literal_sql(
        _function_source(ORDER_MONITOR_PATH, "_check_armed_retries"),
        "meta ->> 'retry_status' = 'ARMED'",
    )


def _stale_sql() -> str:
    return _literal_sql(
        _function_source(ORDER_MONITOR_PATH, "_recover_stale_inflight_retries"),
        "meta ->> 'retry_status' IN ('IN_FLIGHT', 'SUBMITTING')",
    )


def _pending_sql() -> str:
    return _literal_sql(
        _function_source(MORNING_HANDOFF_PATH, "_has_unowned_pending_trigger_orders"),
        "AND filled_ts IS NULL",
    )


def _active_pending_sql() -> str:
    return _literal_sql(
        _function_source(PREOPEN_READINESS_PATH, "_query_client_state"),
        "AND filled_ts IS NULL",
    )


def _pg_dsn() -> str:
    return (
        os.getenv("P0_DB_HOTPATH_TEST_DATABASE_URL")
        or os.getenv("DATABASE_URL")
        or ""
    ).strip()


@pytest.fixture
def hotpath_db():
    psycopg2 = pytest.importorskip("psycopg2")
    extras = pytest.importorskip("psycopg2.extras")
    dsn = _pg_dsn()
    if not dsn:
        pytest.skip("P0_DB_HOTPATH_TEST_DATABASE_URL not configured")

    schema = f"p0_db_hotpath_{uuid4().hex[:12]}"
    try:
        db = psycopg2.connect(dsn, cursor_factory=extras.RealDictCursor)
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"PostgreSQL integration unavailable: {exc}")
    db.autocommit = True
    try:
        with db.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"')
            cur.execute(f'SET search_path TO "{schema}"')
            cur.execute(
                """
                CREATE TABLE orders (
                    local_order_id TEXT,
                    signal_id TEXT,
                    client_id TEXT NOT NULL,
                    kind TEXT,
                    status TEXT,
                    contract TEXT,
                    symbol TEXT,
                    direction TEXT,
                    execution_mode TEXT,
                    broker_order_id TEXT,
                    submitted_ts TIMESTAMPTZ,
                    filled_ts TIMESTAMPTZ,
                    created_ts TIMESTAMPTZ NOT NULL,
                    updated_ts TIMESTAMPTZ NOT NULL,
                    meta JSONB NOT NULL DEFAULT '{}'::jsonb
                )
                """
            )
        yield db, schema
    finally:
        try:
            with db.cursor() as cur:
                cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            db.close()


def _insert_order(cur, *, local_id: str, client: str = "client-A", kind: str = "ENTRY",
                  status: str = "CANCELED", mode: str | None = "paper",
                  broker_id: str | None = None, submitted: datetime | None = None,
                  filled: datetime | None = None, created: datetime | None = None,
                  updated: datetime | None = None, meta: dict | None = None) -> None:
    now = datetime.now(timezone.utc)
    cur.execute(
        """
        INSERT INTO orders (
            local_order_id, client_id, kind, status, contract, symbol, direction,
            execution_mode, broker_order_id, submitted_ts, filled_ts, created_ts,
            updated_ts, meta
        ) VALUES (%s, %s, %s, %s, 'AAPL260101C00100000', 'AAPL', 'CALL',
                  %s, %s, %s, %s, %s, %s, %s::jsonb)
        """,
        (
            local_id, client, kind, status, mode, broker_id, submitted, filled,
            created or now, updated or now, json.dumps(meta or {}),
        ),
    )


def _apply_migration(cur) -> None:
    cur.execute(MIGRATION_PATH.read_text(encoding="utf-8"))


def _fetch(cur, sql: str, params: tuple) -> list[dict]:
    cur.execute(sql, params)
    return [dict(row) for row in (cur.fetchall() or [])]


def _mode_params(mode: str) -> tuple[str, str, str, str, str]:
    return (mode, mode, mode.upper(), mode, mode.upper())


def _plan_nodes(node: dict):
    yield node
    for child in node.get("Plans") or ():
        yield from _plan_nodes(child)


def _explain_json(cur, sql: str, params: tuple) -> dict:
    cur.execute("SET enable_seqscan = off")
    cur.execute("SET enable_bitmapscan = off")
    cur.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, params)
    explain_row = cur.fetchone()
    assert explain_row and explain_row["QUERY PLAN"]
    return explain_row["QUERY PLAN"][0]["Plan"]


def _assert_index_scan(
    plan: dict,
    *,
    index_name: str,
    limit_rows: int | None = None,
    expected_rows: int | None = None,
    expected_removed: int | None = None,
) -> dict:
    nodes = list(_plan_nodes(plan))
    scans = [node for node in nodes if node.get("Index Name") == index_name]
    assert len(scans) == 1, f"expected one scan using {index_name}: {nodes}"
    scan = scans[0]
    assert scan.get("Node Type") in {"Index Scan", "Index Only Scan"}
    assert scan is not plan or limit_rows is None
    assert int(scan.get("Actual Loops", 0)) == 1
    actual_rows = int(scan.get("Actual Rows", 0))
    removed_rows = int(scan.get("Rows Removed by Filter", 0))
    assert "Shared Hit Blocks" in scan
    assert "Shared Read Blocks" in scan
    assert not any(node.get("Node Type") == "Sort" for node in nodes)

    if limit_rows is not None:
        limits = [node for node in nodes if node.get("Node Type") == "Limit"]
        assert len(limits) == 1, f"expected a Limit node: {nodes}"
        assert limits[0] is plan
        assert int(limits[0].get("Actual Rows", 0)) <= limit_rows
        assert actual_rows <= limit_rows
        assert actual_rows + removed_rows <= limit_rows * 2
    if expected_rows is not None:
        assert actual_rows == expected_rows
    if expected_removed is not None:
        assert removed_rows == expected_removed
    return scan


def _canonical_index_predicate(predicate: str) -> str:
    normalized = predicate.lower()
    normalized = re.sub(r"::text\b", "", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    normalized = normalized.replace("(", "").replace(")", "")
    return normalized.strip()


def _predicate_terms(predicate: str) -> set[str]:
    return {term.strip() for term in _canonical_index_predicate(predicate).split(" and ")}


def _seed_retry_rows(cur, now: datetime) -> None:
    _insert_order(
        cur, local_id="armed-paper", updated=now - timedelta(seconds=40),
        meta={"retry_status": "ARMED", "retry_ready_at": "not-a-number", "retry_attempts": 1,
              "execution_mode": "paper", "mode": "PAPER"},
    )
    _insert_order(
        cur, local_id="armed-paper-null-column", mode=None,
        updated=now - timedelta(seconds=30),
        meta={"retry_status": "ARMED", "retry_ready_at": 1, "retry_attempts": 1,
              "execution_mode": "paper"},
    )
    _insert_order(
        cur, local_id="armed-live", mode="live", updated=now - timedelta(seconds=20),
        meta={"retry_status": "ARMED", "retry_ready_at": 1, "retry_attempts": 1,
              "execution_mode": "live", "mode": "LIVE"},
    )
    _insert_order(
        cur, local_id="armed-other-client", client="client-B",
        updated=now - timedelta(seconds=10),
        meta={"retry_status": "ARMED", "retry_ready_at": 1, "retry_attempts": 1,
              "execution_mode": "paper", "mode": "PAPER"},
    )
    _insert_order(
        cur, local_id="armed-future", updated=now + timedelta(hours=1),
        meta={"retry_status": "ARMED", "retry_ready_at": 9999999999, "retry_attempts": 1,
              "execution_mode": "paper", "mode": "PAPER"},
    )
    _insert_order(
        cur, local_id="armed-conflicting-meta", updated=now - timedelta(seconds=5),
        meta={"retry_status": "ARMED", "retry_ready_at": 1, "retry_attempts": 1,
              "execution_mode": "paper", "mode": "LIVE"},
    )
    _insert_order(
        cur, local_id="stale-inflight", updated=now - timedelta(minutes=10),
        meta={"retry_status": "IN_FLIGHT", "execution_mode": "paper", "mode": "PAPER"},
    )
    _insert_order(
        cur, local_id="stale-submitting", updated=now - timedelta(minutes=9),
        meta={"retry_status": "SUBMITTING", "execution_mode": "paper", "mode": "PAPER"},
    )
    _insert_order(
        cur, local_id="stale-armed", updated=now - timedelta(minutes=8),
        meta={"retry_status": "ARMED", "execution_mode": "paper", "mode": "PAPER"},
    )


def _seed_explain_rows(cur, now: datetime) -> None:
    for i in range(500):
        mode = "paper" if i % 2 == 0 else "live"
        _insert_order(
            cur,
            local_id=f"explain-armed-{i}",
            mode=mode,
            updated=now - timedelta(seconds=i),
            meta={"retry_status": "ARMED", "execution_mode": mode, "mode": mode.upper()},
        )
        stale_status = "IN_FLIGHT" if i % 2 == 0 else "SUBMITTING"
        _insert_order(
            cur,
            local_id=f"explain-stale-{i}",
            mode=mode,
            updated=now - timedelta(minutes=10, seconds=i),
            meta={"retry_status": stale_status, "execution_mode": mode, "mode": mode.upper()},
        )
        _insert_order(
            cur,
            local_id=f"explain-pending-{i}",
            status="PENDING_TRIGGER",
            filled=now if i % 50 == 0 else None,
            created=now - timedelta(seconds=i),
            updated=now,
        )


def test_armed_retry_query_returns_byte_equivalent_rows_before_after_indexes(hotpath_db):
    db, _ = hotpath_db
    now = datetime.now(timezone.utc)
    with db.cursor() as cur:
        _seed_retry_rows(cur, now)
        sql = _armed_sql()
        params = ("client-A", *_mode_params("paper"))
        before = _fetch(cur, sql, params)
        _apply_migration(cur)
        after = _fetch(cur, sql, params)
    assert after == before
    assert [row["local_order_id"] for row in after] == [
        "stale-armed", "armed-paper", "armed-paper-null-column",
        "armed-future"
    ]


def test_stale_inflight_query_returns_byte_equivalent_rows_before_after_indexes(hotpath_db):
    db, _ = hotpath_db
    now = datetime.now(timezone.utc)
    with db.cursor() as cur:
        _seed_retry_rows(cur, now)
        sql = _stale_sql()
        params = ("client-A", 60, *_mode_params("paper"))
        before = _fetch(cur, sql, params)
        _apply_migration(cur)
        after = _fetch(cur, sql, params)
    assert after == before
    assert [row["local_order_id"] for row in after] == ["stale-inflight", "stale-submitting"]


def test_pending_trigger_query_returns_byte_equivalent_rows_before_after_indexes(hotpath_db):
    db, _ = hotpath_db
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=48)
    with db.cursor() as cur:
        _insert_order(cur, local_id="pending-paper", status="PENDING_TRIGGER", created=now - timedelta(hours=1), updated=now)
        _insert_order(cur, local_id="pending-live", status="PENDING_TRIGGER", mode="live", created=now - timedelta(hours=2), updated=now)
        _insert_order(cur, local_id="pending-broker", status="PENDING_TRIGGER", broker_id="broker-1", created=now - timedelta(hours=3), updated=now)
        _insert_order(cur, local_id="pending-submitted", status="PENDING_TRIGGER", submitted=now, created=now - timedelta(hours=4), updated=now)
        _insert_order(cur, local_id="pending-filled", status="PENDING_TRIGGER", filled=now, created=now - timedelta(hours=5), updated=now)
        _insert_order(cur, local_id="pending-old", status="PENDING_TRIGGER", created=now - timedelta(hours=49), updated=now)
        sql = _pending_sql()
        params = ("client-A", cutoff)
        before = _fetch(cur, sql, params)
        _apply_migration(cur)
        after = _fetch(cur, sql, params)
    assert after == before
    assert [row["local_order_id"] for row in after] == ["pending-live", "pending-paper"]


def test_live_and_paper_retry_rows_remain_isolated():
    sql = _armed_sql()
    assert "client_id = %s" in sql
    assert "execution_mode = %s" in sql
    assert "meta ->> 'execution_mode' = %s" in sql
    assert "meta ->> 'mode' = %s" in sql
    assert "COALESCE(execution_mode, 'live')" not in sql.lower()


def test_null_execution_mode_has_no_implicit_live_default():
    for sql in (_armed_sql(), _stale_sql()):
        assert "execution_mode IS NULL" in sql
        assert "execution_mode = %s" in sql
        assert "COALESCE(execution_mode" not in sql
        assert "OR meta ->> 'mode' = %s" in sql


def test_legacy_valid_metadata_mode_fence_is_preserved():
    sql = _armed_sql()
    # Legacy rows may carry the owner only in either metadata key.  The query
    # retains both exact comparisons and does not normalize them by casting.
    assert sql.count("meta ->> 'execution_mode'") >= 3
    assert sql.count("meta ->> 'mode'") >= 3
    assert "::" not in sql


def test_malformed_retry_timestamp_is_not_cast_in_sql():
    sql = _armed_sql()
    assert "retry_ready_at" not in sql
    assert "::numeric" not in sql.lower()
    assert "::double" not in sql.lower()
    assert "::float" not in sql.lower()


def test_negative_retry_timestamp_and_counter_remain_python_classified():
    source = _function_source(ORDER_MONITOR_PATH, "_check_armed_retries")
    assert "parse_retry_attempt_count(meta)" in source
    assert "float(meta.get(\"retry_ready_at\"))" in source
    assert "math.isfinite(ready_at)" in source
    assert "attempt <= 0" in source


def test_duplicate_rows_and_order_identities_are_not_deduplicated():
    sql = _armed_sql()
    assert "DISTINCT" not in sql.upper()
    assert "local_order_id" in sql
    assert "ORDER BY updated_ts ASC" in sql


def test_retry_ordering_remains_updated_timestamp_ascending():
    assert "ORDER BY updated_ts ASC" in _armed_sql()
    assert "ORDER BY updated_ts ASC" in _stale_sql()


def test_retry_limit_semantics_remain_unchanged():
    assert re.search(r"ORDER BY updated_ts ASC LIMIT 64\b", _armed_sql())
    assert re.search(r"ORDER BY updated_ts ASC LIMIT 16\b", _stale_sql())


def test_index_and_shape_tests_do_not_import_or_call_broker_paths():
    migration = MIGRATION_PATH.read_text(encoding="utf-8").lower()
    assert "broker.submit" not in migration
    assert "broker.cancel" not in migration
    assert "place_order" not in migration
    assert "cancel_order" not in migration


def test_migration_is_idempotent_and_uses_three_narrow_partial_indexes():
    migration = MIGRATION_PATH.read_text(encoding="utf-8")
    assert migration.count("CREATE INDEX IF NOT EXISTS") == 3
    for name in (ARMED_INDEX, STALE_INDEX, PENDING_INDEX):
        assert name in migration
    assert re.search(r"(?im)^\s*CREATE INDEX CONCURRENTLY", migration) is None
    assert re.search(r"(?im)^\s*BEGIN\s*;", migration) is None
    assert re.search(r"(?im)^\s*COMMIT\s*;", migration) is None


def test_clean_postgres_can_apply_migration_twice(hotpath_db):
    db, _ = hotpath_db
    with db.cursor() as cur:
        _apply_migration(cur)
        _apply_migration(cur)
        cur.execute(
            """
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = current_schema()
              AND tablename = 'orders'
              AND indexname = ANY(%s)
            ORDER BY indexname
            """,
            ([ARMED_INDEX, STALE_INDEX, PENDING_INDEX],),
        )
        assert [row["indexname"] for row in cur.fetchall()] == sorted(
            [ARMED_INDEX, PENDING_INDEX, STALE_INDEX]
        )
        cur.execute(
            """
            SELECT c.relname AS indexname,
                   pg_get_indexdef(c.oid) AS indexdef,
                   pg_get_expr(i.indpred, i.indrelid) AS predicate
            FROM pg_index i
            JOIN pg_class c ON c.oid = i.indexrelid
            JOIN pg_class t ON t.oid = i.indrelid
            WHERE t.relnamespace = current_schema()::regnamespace
              AND t.relname = 'orders'
              AND c.relname = ANY(%s)
            """,
            ([ARMED_INDEX, STALE_INDEX, PENDING_INDEX],),
        )
        definitions = {row["indexname"]: row for row in cur.fetchall()}
        expected = {
            ARMED_INDEX: {
                "keys": "using btree (client_id, updated_ts) where",
                "terms": {
                    "kind = 'entry'",
                    "status = 'canceled'",
                    "meta ->> 'retry_status' = 'armed'",
                },
            },
            STALE_INDEX: {
                "keys": "using btree (client_id, updated_ts) where",
                "terms": {
                    "kind = 'entry'",
                    "status = 'canceled'",
                    "meta ->> 'retry_status' = any array['in_flight', 'submitting']",
                },
            },
            PENDING_INDEX: {
                "keys": "using btree (client_id, created_ts) where",
                "terms": {
                    "kind = 'entry'",
                    "status = 'pending_trigger'",
                    "broker_order_id is null",
                    "submitted_ts is null",
                },
            },
        }
        assert set(definitions) == set(expected)
        for name, shape in expected.items():
            actual = definitions[name]
            indexdef = " ".join(actual["indexdef"].lower().split())
            assert shape["keys"] in indexdef, actual["indexdef"]
            assert _predicate_terms(actual["predicate"]) == shape["terms"], actual["predicate"]
        assert "filled_ts" not in definitions[PENDING_INDEX]["indexdef"].lower()


def test_explain_proves_all_three_indexes_are_eligible_and_used(hotpath_db):
    db, _ = hotpath_db
    now = datetime.now(timezone.utc)
    with db.cursor() as cur:
        _seed_explain_rows(cur, now)
        _apply_migration(cur)
        cur.execute("ANALYZE orders")
        armed_plan = _explain_json(cur, _armed_sql(), ("client-A", *_mode_params("paper")))
        _assert_index_scan(armed_plan, index_name=ARMED_INDEX, limit_rows=64)
        stale_plan = _explain_json(cur, _stale_sql(), ("client-A", 60, *_mode_params("paper")))
        _assert_index_scan(stale_plan, index_name=STALE_INDEX, limit_rows=16)
        pending_plan = _explain_json(
            cur,
            _active_pending_sql(),
            ("client-A", now - timedelta(hours=48)),
        )
        _assert_index_scan(
            pending_plan,
            index_name=PENDING_INDEX,
            expected_rows=490,
            expected_removed=10,
        )


def test_production_shaped_scale_scan_is_bounded_by_existing_limit(hotpath_db):
    db, _ = hotpath_db
    now = datetime.now(timezone.utc)
    with db.cursor() as cur:
        for i in range(2000):
            mode = "paper" if i % 2 == 0 else "live"
            _insert_order(
                cur,
                local_id=f"scale-{i}",
                mode=mode,
                updated=now - timedelta(seconds=i),
                meta={"retry_status": "ARMED", "execution_mode": mode, "mode": mode.upper()},
            )
        _apply_migration(cur)
        cur.execute("ANALYZE orders")
        plan = _explain_json(cur, _armed_sql(), ("client-A", *_mode_params("paper")))
    scan = _assert_index_scan(plan, index_name=ARMED_INDEX, limit_rows=64)
    assert int(scan.get("Rows Removed by Filter", 0)) > 0


def test_schema_shape_rejects_trade_queue_ticker_assumption():
    runtime_files = list((REPO_ROOT / "ap").rglob("*.py")) + list((REPO_ROOT / "sql").rglob("*.sql"))
    offenders = []
    for path in runtime_files:
        text = path.read_text(encoding="utf-8")
        if re.search(r"\b(?:trade_queue|tq|q)\.ticker\b", text, flags=re.IGNORECASE):
            offenders.append(str(path.relative_to(REPO_ROOT)))
    assert offenders == []
    schema_source = SCHEMA_ATTESTATION_PATH.read_text(encoding="utf-8")
    assert "\"ticker\"" not in schema_source.split('"trade_queue"', 1)[1].split('}', 1)[0]


def test_schema_shape_rejects_absent_trade_queue_meta_column():
    runtime_files = list((REPO_ROOT / "ap").rglob("*.py")) + list((REPO_ROOT / "sql").rglob("*.sql"))
    offenders = []
    for path in runtime_files:
        text = path.read_text(encoding="utf-8")
        if path.suffix == ".py":
            # Inspect SQL string literals, not the whole Python module: a
            # nearby order-row ``meta`` reference must not be mistaken for a
            # trade_queue projection.
            tree = ast.parse(text)
            sql_literals = [
                node.value
                for node in ast.walk(tree)
                if isinstance(node, ast.Constant)
                and isinstance(node.value, str)
            ]
        else:
            sql_literals = re.split(r";", re.sub(r"--[^\n]*", "", text))
        for statement in sql_literals:
            if re.search(r"\bFROM\s+(?:public\.)?trade_queue\b", statement, flags=re.IGNORECASE) and re.search(r"\bmeta\b", statement, flags=re.IGNORECASE):
                offenders.append(str(path.relative_to(REPO_ROOT)))
    assert offenders == []
    schema_source = SCHEMA_ATTESTATION_PATH.read_text(encoding="utf-8")
    queue_block = schema_source.split('"trade_queue"', 1)[1].split('}', 1)[0]
    assert '"payload"' in queue_block
    assert '"result_json"' in queue_block
    assert '"meta"' not in queue_block


def test_hotpath_sql_keeps_exact_client_id_fence():
    for sql in (_armed_sql(), _stale_sql(), _pending_sql()):
        assert "client_id = %s" in sql
        assert "LOWER(client_id)" not in sql.upper()
        assert "client_id::" not in sql.lower()
        assert "COALESCE(client_id" not in sql


def test_hotpath_sql_keeps_execution_mode_fence_without_taxonomy_rewrite():
    for sql in (_armed_sql(), _stale_sql()):
        assert "execution_mode = %s" in sql
        assert "execution_mode IS NULL" in sql
        assert "meta ->> 'execution_mode' = %s" in sql
        assert "meta ->> 'mode' = %s" in sql
        assert "COALESCE(execution_mode" not in sql
        assert "LOWER(TRIM" not in sql
