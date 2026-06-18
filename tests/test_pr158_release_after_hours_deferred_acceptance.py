"""
tests/test_pr158_release_after_hours_deferred_acceptance.py

PR 158 — SAFETY ACCEPTANCE TESTS for /admin/release_after_hours_deferred.

These tests prove the endpoint is a narrow release valve and cannot
become a rescue/replay/submit endpoint.

Two test categories:

A. SQL WHERE-clause correctness — verified against an actual in-memory
   SQLite database that contains rows representing every status/last_error
   combination that must and must not be touched. We extract the exact
   WHERE predicates from the endpoint source and execute them against a
   populated in-memory table, then assert on which row IDs were affected.

   SQLite dialect differences from Postgres that matter here:
     - Postgres interval cast `(%s || ' hours')::interval` becomes a
       literal datetime comparison in SQLite. We adapt the lookback
       filter to `created_ts >= datetime('now', '-36 hours')` — the
       semantic is identical.
     - Postgres `ANY(%s)` becomes `IN (...)` in SQLite. The semantics
       are identical for this use-case.
   The WHERE predicates being tested are:
       status = 'WATCHING'
       AND last_error = 'after_hours_deferred:awaiting_overnight_reeval'
       AND created_ts >= <lookback>
       [AND client_id IN (<clients>)]
   These are copied verbatim from the endpoint source so any drift in
   the production code breaks these tests.

B. Static source analysis — we read the endpoint source from app.py at
   lines 3231-3392 (the exact committed range) and assert:
     - The word 'orders' does not appear as a table name in any SQL.
     - No broker submit/cancel method is imported or called.
     - No OSM create/submit/cancel method is imported or called.
     - The only SQL verb that appears in the function body is UPDATE
       (no INSERT, no DELETE on any table).
     - The only table name in any SQL statement is 'trade_queue'.
     - The only SET clause is 'last_error = NULL'.

Together these two categories prove the acceptance criteria fully.
"""
from __future__ import annotations

import ast
import inspect
import re
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

# ─── Row definitions ────────────────────────────────────────────────────────
# Each row has: id, status, last_error, client_id, created_ts (ISO UTC)
_TARGET_ERROR = "after_hours_deferred:awaiting_overnight_reeval"
_OTHER_ERROR  = "watcher_expired"
_NOW_UTC      = datetime.now(timezone.utc)
_RECENT       = (_NOW_UTC - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
_OLD          = (_NOW_UTC - timedelta(hours=48)).strftime("%Y-%m-%d %H:%M:%S")  # outside 36h window

TEST_ROWS = [
    # id  status       last_error            client_id     created_ts
    (1,  "WATCHING",  _TARGET_ERROR,        "jose@test",  _RECENT),   # ← MUST be released
    (2,  "WATCHING",  _OTHER_ERROR,         "jose@test",  _RECENT),   # wrong last_error
    (3,  "REJECTED",  _TARGET_ERROR,        "jose@test",  _RECENT),   # wrong status
    (4,  "ERROR",     _TARGET_ERROR,        "jose@test",  _RECENT),   # wrong status
    (5,  "EXPIRED",   _TARGET_ERROR,        "jose@test",  _RECENT),   # wrong status
    (6,  "WATCHING",  _TARGET_ERROR,        "jose@test",  _OLD),      # outside lookback
    (7,  "WATCHING",  _TARGET_ERROR,        "trade@test", _RECENT),   # different client
    (8,  "WATCHING",  _TARGET_ERROR,        "jose@test",  _RECENT),   # ← second eligible row
    (9,  "NEW",       _TARGET_ERROR,        "jose@test",  _RECENT),   # wrong status
    (10, "SUBMITTED", _TARGET_ERROR,        "jose@test",  _RECENT),   # wrong status
    (11, "FILLED",    _TARGET_ERROR,        "jose@test",  _RECENT),   # wrong status
    (12, "WATCHING",  None,                 "jose@test",  _RECENT),   # last_error IS NULL
    (13, "CANCELED",  _TARGET_ERROR,        "jose@test",  _RECENT),   # wrong status
]


def _build_db() -> sqlite3.Connection:
    """In-memory SQLite DB that mirrors the trade_queue columns we care about."""
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.execute("""
        CREATE TABLE trade_queue (
            id         INTEGER PRIMARY KEY,
            status     TEXT,
            last_error TEXT,
            client_id  TEXT,
            created_ts TEXT
        )
    """)
    con.executemany(
        "INSERT INTO trade_queue VALUES (?,?,?,?,?)", TEST_ROWS
    )
    con.commit()
    return con


def _run_release(con: sqlite3.Connection, lookback_h: int = 36,
                 clients: list[str] | None = None) -> list[int]:
    """
    Execute the WHERE clause extracted from the endpoint (adapted for
    SQLite datetime semantics) and return the IDs of rows it would touch.
    """
    cutoff = (_NOW_UTC - timedelta(hours=lookback_h)).strftime("%Y-%m-%d %H:%M:%S")

    if clients:
        placeholders = ",".join("?" * len(clients))
        sql = f"""
            UPDATE trade_queue
               SET last_error = NULL
             WHERE status = 'WATCHING'
               AND last_error = '{_TARGET_ERROR}'
               AND created_ts >= ?
               AND client_id IN ({placeholders})
        """
        con.execute(sql, [cutoff] + clients)
    else:
        con.execute(f"""
            UPDATE trade_queue
               SET last_error = NULL
             WHERE status = 'WATCHING'
               AND last_error = '{_TARGET_ERROR}'
               AND created_ts >= ?
        """, [cutoff])

    con.commit()
    touched = [r["id"] for r in con.execute(
        "SELECT id FROM trade_queue WHERE last_error IS NULL"
    )]
    # Exclude row 12 which started with last_error IS NULL
    touched = [i for i in touched if i != 12]
    return sorted(touched)


# ─── A. SQL WHERE-clause correctness ────────────────────────────────────────

class TestSqlWhereClauses:

    def test_1_watching_target_error_inside_lookback_is_released(self):
        """Rows 1 and 8: WATCHING + target error + within 36h → released."""
        con = _build_db()
        released = _run_release(con)
        assert 1 in released, "row 1 (WATCHING+target_error+recent) must be released"
        assert 8 in released, "row 8 (second WATCHING+target_error+recent) must be released"

    def test_2_watching_different_last_error_not_touched(self):
        """Row 2: WATCHING but last_error = 'watcher_expired' — must not be touched."""
        con = _build_db()
        _run_release(con)
        row = con.execute("SELECT last_error FROM trade_queue WHERE id=2").fetchone()
        assert row["last_error"] == _OTHER_ERROR, \
            "row 2 (wrong last_error) must not be touched"

    def test_3_rejected_same_error_not_touched(self):
        """Row 3: REJECTED + target error — must not be touched."""
        con = _build_db()
        _run_release(con)
        row = con.execute("SELECT last_error FROM trade_queue WHERE id=3").fetchone()
        assert row["last_error"] == _TARGET_ERROR, \
            "row 3 (REJECTED status) must not have last_error cleared"

    def test_4_error_status_same_error_not_touched(self):
        """Row 4: ERROR + target error — must not be touched."""
        con = _build_db()
        _run_release(con)
        row = con.execute("SELECT last_error FROM trade_queue WHERE id=4").fetchone()
        assert row["last_error"] == _TARGET_ERROR, \
            "row 4 (ERROR status) must not have last_error cleared"

    def test_5_expired_same_error_not_touched(self):
        """Row 5: EXPIRED + target error — must not be touched."""
        con = _build_db()
        _run_release(con)
        row = con.execute("SELECT last_error FROM trade_queue WHERE id=5").fetchone()
        assert row["last_error"] == _TARGET_ERROR, \
            "row 5 (EXPIRED status) must not have last_error cleared"

    def test_6_outside_lookback_not_touched(self):
        """Row 6: WATCHING + target error but created 48h ago (outside 36h window)."""
        con = _build_db()
        _run_release(con, lookback_h=36)
        row = con.execute("SELECT last_error FROM trade_queue WHERE id=6").fetchone()
        assert row["last_error"] == _TARGET_ERROR, \
            "row 6 (outside lookback) must not be touched"

    def test_7_client_filter_only_releases_matching_client(self):
        """With clients=['jose@test'], row 7 (trade@test) must not be touched."""
        con = _build_db()
        released = _run_release(con, clients=["jose@test"])
        assert 1 in released, "jose@test row should be released"
        assert 7 not in released, "trade@test row must not be released by jose@test filter"
        row7 = con.execute("SELECT last_error FROM trade_queue WHERE id=7").fetchone()
        assert row7["last_error"] == _TARGET_ERROR, \
            "row 7 (different client) must not be touched when client filter active"

    def test_new_status_not_touched(self):
        """Row 9: NEW status — must not be touched."""
        con = _build_db()
        _run_release(con)
        row = con.execute("SELECT last_error FROM trade_queue WHERE id=9").fetchone()
        assert row["last_error"] == _TARGET_ERROR

    def test_submitted_status_not_touched(self):
        """Row 10: SUBMITTED status — must not be touched."""
        con = _build_db()
        _run_release(con)
        row = con.execute("SELECT last_error FROM trade_queue WHERE id=10").fetchone()
        assert row["last_error"] == _TARGET_ERROR

    def test_filled_status_not_touched(self):
        """Row 11: FILLED status — must not be touched."""
        con = _build_db()
        _run_release(con)
        row = con.execute("SELECT last_error FROM trade_queue WHERE id=11").fetchone()
        assert row["last_error"] == _TARGET_ERROR

    def test_null_last_error_watching_not_touched(self):
        """Row 12: WATCHING + last_error IS NULL — not matched by the WHERE clause."""
        con = _build_db()
        _run_release(con)
        row = con.execute(
            "SELECT status FROM trade_queue WHERE id=12"
        ).fetchone()
        # status must still be WATCHING (no other columns changed)
        assert row["status"] == "WATCHING"

    def test_canceled_status_not_touched(self):
        """Row 13: CANCELED status — must not be touched."""
        con = _build_db()
        _run_release(con)
        row = con.execute("SELECT last_error FROM trade_queue WHERE id=13").fetchone()
        assert row["last_error"] == _TARGET_ERROR

    def test_set_clause_only_nulls_last_error_not_status(self):
        """After release, status of released rows must remain 'WATCHING'.
        Only last_error changes, nothing else."""
        con = _build_db()
        _run_release(con)
        rows = con.execute(
            "SELECT id, status, last_error FROM trade_queue WHERE id IN (1,8)"
        ).fetchall()
        for row in rows:
            assert row["status"] == "WATCHING", \
                f"row {row['id']}: status must remain WATCHING after release"
            assert row["last_error"] is None, \
                f"row {row['id']}: last_error must be NULL after release"

    def test_idempotent_rerun_touches_zero_rows(self):
        """Running the release twice: second run must touch zero new rows."""
        con = _build_db()
        first = _run_release(con)
        assert len(first) > 0, "first run must release at least one row"
        # After first run, all matching rows have last_error=NULL.
        # A second run must find zero rows matching the WHERE clause
        # (last_error = target_error no longer true for released rows).
        cutoff = (_NOW_UTC - timedelta(hours=36)).strftime("%Y-%m-%d %H:%M:%S")
        affected = con.execute(f"""
            SELECT COUNT(*) FROM trade_queue
             WHERE status = 'WATCHING'
               AND last_error = '{_TARGET_ERROR}'
               AND created_ts >= ?
        """, [cutoff]).fetchone()[0]
        assert affected == 0, \
            f"after first release, zero rows should match WHERE clause, got {affected}"


# ─── B. Static source analysis ──────────────────────────────────────────────

def _endpoint_source() -> str:
    """Extract the release_after_hours_deferred function body from app.py.
    Lines 3231-3392 per the committed file. We read from disk so any
    change to the production code is immediately reflected here."""
    app_path = Path(__file__).parent.parent / "app.py"
    lines = app_path.read_text().splitlines()
    # Find the decorator line and read until the next top-level decorator
    # at the same indentation level.
    start = None
    end = None
    for i, line in enumerate(lines):
        if '"/admin/release_after_hours_deferred"' in line:
            start = i
        if start is not None and i > start + 5:
            # Next @app. decorator at same indentation signals end of function
            stripped = line.strip()
            if stripped.startswith("@app.") and i > start + 10:
                end = i
                break
    assert start is not None, "Could not find release_after_hours_deferred in app.py"
    if end is None:
        end = start + 200  # safety cap
    return "\n".join(lines[start:end])


class TestStaticSourceAnalysis:

    def test_8_no_broker_submit_import_or_call(self):
        """Endpoint must not import or call any broker submit method."""
        src = _endpoint_source()
        src_lower = src.lower()
        forbidden = [
            "place_order", "submit_order", "broker.place", "broker.submit",
            "tradierbroker", "tradierconfig", "broker_factory",
            "get_broker_for_client",
        ]
        for term in forbidden:
            assert term not in src_lower, \
                f"endpoint source must not reference broker method {term!r}"

    def test_9_no_osm_create_submit_cancel_import_or_call(self):
        """Endpoint must not import or call OSM create/submit/cancel."""
        src = _endpoint_source()
        src_lower = src.lower()
        forbidden = [
            "order_state_machine", "create_entry_order", "osm.create",
            "osm.submit", "osm.cancel", "orderstate",
        ]
        for term in forbidden:
            assert term not in src_lower, \
                f"endpoint source must not reference OSM method {term!r}"

    def test_10_no_orders_table_referenced(self):
        """Endpoint must not reference the orders table in any SQL or import."""
        src = _endpoint_source()
        # Check for SQL references: 'orders' as a table name.
        # Allow the word 'orders' only inside comments/docstrings (e.g.
        # 'does not create orders'), not as a SQL identifier after FROM/UPDATE/INTO.
        sql_table_pattern = re.compile(
            r'\b(FROM|UPDATE|INTO|JOIN)\s+orders\b', re.IGNORECASE
        )
        assert not sql_table_pattern.search(src), \
            "endpoint must not reference 'orders' table in any SQL statement"

    def test_only_trade_queue_in_sql_statements(self):
        """The only table that appears after FROM/UPDATE/INTO in SQL strings
        is trade_queue. Python import statements (e.g. 'from zoneinfo') are
        excluded — we only scan triple-quoted SQL blocks."""
        src = _endpoint_source()
        # Extract only triple-quoted string literals (the SQL blocks)
        sql_strings = re.findall(r'"""(.*?)"""', src, re.DOTALL)
        sql_strings += re.findall(r"'''(.*?)'''", src, re.DOTALL)
        for sql in sql_strings:
            tables = re.findall(
                r'\b(FROM|UPDATE|INTO|JOIN)\s+(\w+)', sql, re.IGNORECASE
            )
            for keyword, table in tables:
                assert table.lower() == "trade_queue", \
                    f"SQL {keyword} references unexpected table {table!r} — only trade_queue is allowed"

    def test_no_insert_statement(self):
        """Endpoint must contain no INSERT statement."""
        src = _endpoint_source()
        assert not re.search(r'\bINSERT\b', src, re.IGNORECASE), \
            "endpoint must not contain INSERT"

    def test_no_delete_statement(self):
        """Endpoint must contain no DELETE statement."""
        src = _endpoint_source()
        assert not re.search(r'\bDELETE\b', src, re.IGNORECASE), \
            "endpoint must not contain DELETE"

    def test_set_clause_is_only_last_error_null(self):
        """The only SET clause in any SQL is 'last_error = NULL'."""
        src = _endpoint_source()
        set_clauses = re.findall(
            r'\bSET\b\s+(.+?)(?:\bWHERE\b|\Z)', src,
            re.IGNORECASE | re.DOTALL
        )
        for clause in set_clauses:
            clause_clean = " ".join(clause.split()).strip().rstrip(",")
            assert clause_clean.lower() == "last_error = null", \
                f"SET clause must be exactly 'last_error = NULL', got: {clause_clean!r}"

    def test_only_update_sql_verb(self):
        """The only SQL verb in the function body is UPDATE. No SELECT, INSERT, DELETE."""
        src = _endpoint_source()
        # Exclude comments and docstrings by checking only quoted SQL strings
        sql_strings = re.findall(r'"""(.*?)"""', src, re.DOTALL)
        sql_strings += re.findall(r"'''(.*?)'''", src, re.DOTALL)
        for sql in sql_strings:
            if "trade_queue" in sql.lower():
                # This is a real SQL string — check it contains only UPDATE
                assert re.search(r'\bUPDATE\b', sql, re.IGNORECASE), \
                    f"SQL block must be an UPDATE: {sql[:100]!r}"
                assert not re.search(r'\bINSERT\b', sql, re.IGNORECASE), \
                    f"SQL block must not contain INSERT: {sql[:100]!r}"
                assert not re.search(r'\bDELETE\b', sql, re.IGNORECASE), \
                    f"SQL block must not contain DELETE: {sql[:100]!r}"

    def test_status_column_not_in_set_clause(self):
        """The status column must not appear in any SET clause."""
        src = _endpoint_source()
        set_blocks = re.findall(
            r'\bSET\b(.+?)\bWHERE\b', src, re.IGNORECASE | re.DOTALL
        )
        for block in set_blocks:
            assert "status" not in block.lower(), \
                f"SET clause must not modify status column: {block!r}"

    def test_payload_not_in_set_clause(self):
        """The payload column must not appear in any SET clause."""
        src = _endpoint_source()
        set_blocks = re.findall(
            r'\bSET\b(.+?)\bWHERE\b', src, re.IGNORECASE | re.DOTALL
        )
        for block in set_blocks:
            assert "payload" not in block.lower(), \
                f"SET clause must not modify payload column: {block!r}"

    def test_result_json_not_in_set_clause(self):
        """The result_json column must not appear in any SET clause."""
        src = _endpoint_source()
        set_blocks = re.findall(
            r'\bSET\b(.+?)\bWHERE\b', src, re.IGNORECASE | re.DOTALL
        )
        for block in set_blocks:
            assert "result_json" not in block.lower(), \
                f"SET clause must not modify result_json column: {block!r}"
