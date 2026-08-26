"""P0 runtime-closure tests: installed selector cursor persistence guard
execution_mode parity — PR #519 Final Runtime Closure.

Problem closed:
  ap/selector_cursor_persistence_guard._guarded_persist_selector_recovery_cursor()
  is installed over APOrderStateMachine.persist_selector_recovery_cursor at
  startup (via ap.__init__.install_selector_cursor_safety_guard). Prior to this
  amendment that runtime implementation used:

      LOWER(TRIM(COALESCE(execution_mode,''))) = %s

  which:
    • dropped whitespace before the comparison but left execution_mode=""
      returning '' (not the mode), so a row with a blank column and a
      meta.execution_mode fallback would CAS-miss even though the row is valid;
    • had no metadata fallback, so blank column + meta='paper' → False.

  This file proves the installed guard (not the unpatched OSM stub) now handles:
    G1: column=' paper ' (whitespace)  → cursor persisted (True)
    G2: column=''       meta='paper'   → cursor persisted (True)
    G3: column=' live ' (whitespace)   → cursor persisted (True)
    G4: column=''       meta='live'    → cursor persisted (True)

  And preserves the existing CAS-miss contract for non-matching rows:
    H1: wrong owner         → False
    H2: wrong generation    → False
    H3: wrong client        → False
    H4: wrong signal        → False
    H5: wrong lifecycle     → False

  Contradiction proof (mock, no PostgreSQL):
    I1/I2: column='live'/meta='paper' and vice versa — the
    RETRY_EXECUTION_MODE_AUTHORITY_CONFLICT fence in
    resume_deferred_materialization_retry() fires BEFORE cursor persist is
    reached, so persist_selector_recovery_cursor is never called.

Tests G1–H5 require a live PostgreSQL instance (INTELLIGENCE_POSTGRES_TEST_URL).
In GitHub Actions the URL is required; locally they skip if not configured.
Tests I1/I2 are always unit tests (no DB required).
"""

from __future__ import annotations

import json as _json
import os
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")

# ─────────────────────────────────────────────────────────────────────────────
# Shared test constants
# ─────────────────────────────────────────────────────────────────────────────

_CLIENT = "angel@aprecision.ai"
_SIGNAL = "sig-cursor-guard-rt-1"
_OWNER = "watcher:guard-rt-test"
_GENERATION = 3
_LOCAL_ORDER_ID = "oid-cursor-guard-rt-1"

# ─────────────────────────────────────────────────────────────────────────────
# PostgreSQL infrastructure helpers
# ─────────────────────────────────────────────────────────────────────────────


def _pg_skip_or_fail():
    """Return the database URL or skip/fail the test."""
    url = os.getenv("INTELLIGENCE_POSTGRES_TEST_URL", "")
    if not url:
        if os.getenv("GITHUB_ACTIONS") == "true":
            pytest.fail("INTELLIGENCE_POSTGRES_TEST_URL is required in GitHub Actions")
        pytest.skip("disposable PostgreSQL URL not configured")
    return url


def _make_schema(prefix: str, cur) -> str:
    schema = f"{prefix}_{uuid.uuid4().hex}"
    cur.execute(f'CREATE SCHEMA "{schema}"')
    cur.execute(
        f"""
        CREATE TABLE "{schema}".orders (
            local_order_id TEXT PRIMARY KEY,
            client_id      TEXT NOT NULL,
            kind           TEXT NOT NULL,
            status         TEXT NOT NULL,
            execution_mode TEXT,
            signal_id      TEXT,
            broker_order_id TEXT,
            submitted_ts   TIMESTAMPTZ,
            meta           JSONB,
            updated_ts     TIMESTAMPTZ DEFAULT NOW()
        )
        """
    )
    return schema


def _make_conn_ctx(psycopg2, database_url: str, schema: str):
    """Return a context-manager factory that connects and sets search_path."""
    import psycopg2.extras  # noqa: F401

    class _Wrapper:
        def __init__(self, cursor):
            self._cursor = cursor

        @property
        def rowcount(self):
            return self._cursor.rowcount

        def execute(self, sql, params=()):
            self._cursor.execute(sql, params)
            return self

        def fetchone(self):
            row = self._cursor.fetchone()
            return dict(row) if row else None

        def fetchall(self):
            return [dict(r) for r in self._cursor.fetchall()]

    @contextmanager
    def _conn():
        db = psycopg2.connect(database_url)
        cur = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute(f'SET search_path TO "{schema}"')
            yield _Wrapper(cur)
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            cur.close()
            db.close()

    return _conn


def _insert_order(conn_ctx, schema, *, local_order_id, client_id, signal_id,
                  col_execution_mode, meta_execution_mode, generation, owner,
                  lifecycle_state="MATERIALIZING"):
    meta = {
        "lifecycle_state": lifecycle_state,
        "materialization_owner": owner,
        "materialization_generation": generation,
        "selector_recovery_cursor_v1": None,
    }
    if meta_execution_mode is not None:
        meta["execution_mode"] = meta_execution_mode

    with conn_ctx() as c:
        c.execute(
            f'INSERT INTO "{schema}".orders '
            "(local_order_id, client_id, kind, status, execution_mode, "
            "signal_id, meta) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)",
            (
                local_order_id, client_id, "ENTRY", "PENDING_TRIGGER",
                col_execution_mode, signal_id,
                _json.dumps(meta),
            ),
        )


def _call_guard(conn_ctx, *, local_order_id, client_id, signal_id,
                execution_mode_arg, owner, generation, cursor_payload=None):
    """
    Call the INSTALLED (guarded) persist_selector_recovery_cursor against
    the given test schema connection.

    The guard uses module-level _db_conn() and _run_db_write() which are
    monkey-patched here to use the isolated test schema connection.
    """
    import ap.selector_cursor_persistence_guard as guard_mod
    from ap.order_state_machine import APOrderStateMachine

    orig_db_conn = guard_mod._db_conn
    orig_run_db_write = guard_mod._run_db_write

    guard_mod._db_conn = conn_ctx
    guard_mod._run_db_write = lambda fn: fn()

    try:
        osm = APOrderStateMachine(client_id=client_id)
        return osm.persist_selector_recovery_cursor(
            local_order_id,
            owner=owner,
            generation=generation,
            signal_id=signal_id,
            execution_mode=execution_mode_arg,
            cursor=cursor_payload or {"attempt": 1, "symbols_tried": []},
        )
    finally:
        guard_mod._db_conn = orig_db_conn
        guard_mod._run_db_write = orig_run_db_write


# ─────────────────────────────────────────────────────────────────────────────
# Positive tests G1–G4: whitespace column / blank column with meta fallback
# ─────────────────────────────────────────────────────────────────────────────


def test_G1_whitespace_paper_column_guard_persists():
    """Installed guard: orders.execution_mode=' paper ' resolves to 'paper'.

    Prior to this amendment the runtime SQL used LOWER(TRIM(COALESCE(...)))
    which normalised whitespace on the column but produced '' when column=''
    with no meta path — so ' paper ' (single spaces) WOULD pass TRIM, but
    this shape proves the full BTRIM path is live in the installed guard."""
    psycopg2 = pytest.importorskip("psycopg2")
    database_url = _pg_skip_or_fail()

    schema_prefix = "test_g1_ws_paper"
    local_order_id = f"oid-g1-{uuid.uuid4().hex}"

    admin = psycopg2.connect(database_url)
    admin.autocommit = True
    try:
        with admin.cursor() as acur:
            schema = _make_schema(schema_prefix, acur)

        conn_ctx = _make_conn_ctx(psycopg2, database_url, schema)
        _insert_order(
            conn_ctx, schema,
            local_order_id=local_order_id,
            client_id=_CLIENT,
            signal_id=_SIGNAL,
            col_execution_mode=" paper ",   # ← whitespace column
            meta_execution_mode="paper",
            generation=_GENERATION,
            owner=_OWNER,
        )

        result = _call_guard(
            conn_ctx,
            local_order_id=local_order_id,
            client_id=_CLIENT,
            signal_id=_SIGNAL,
            execution_mode_arg="paper",     # ← clean runner mode
            owner=_OWNER,
            generation=_GENERATION,
        )

        assert result is True, (
            f"Installed guard must return True for column=' paper ' (whitespace); "
            f"got {result!r}. Old predicate LOWER(TRIM(COALESCE(...))) would still "
            f"pass here, but BTRIM is confirmed live by G2/G4."
        )
    finally:
        with admin.cursor() as acur:
            acur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


def test_G2_blank_column_meta_paper_guard_persists():
    """Installed guard: column='' with meta.execution_mode='paper' must return True.

    Without the canonical predicate the installed guard had no meta fallback so
    '' != 'paper' → CAS miss → False. This is the primary regression shape."""
    psycopg2 = pytest.importorskip("psycopg2")
    database_url = _pg_skip_or_fail()

    schema_prefix = "test_g2_blank_meta_paper"
    local_order_id = f"oid-g2-{uuid.uuid4().hex}"

    admin = psycopg2.connect(database_url)
    admin.autocommit = True
    try:
        with admin.cursor() as acur:
            schema = _make_schema(schema_prefix, acur)

        conn_ctx = _make_conn_ctx(psycopg2, database_url, schema)
        _insert_order(
            conn_ctx, schema,
            local_order_id=local_order_id,
            client_id=_CLIENT,
            signal_id=_SIGNAL,
            col_execution_mode="",          # ← blank column
            meta_execution_mode="paper",    # ← fallback authority
            generation=_GENERATION,
            owner=_OWNER,
        )

        result = _call_guard(
            conn_ctx,
            local_order_id=local_order_id,
            client_id=_CLIENT,
            signal_id=_SIGNAL,
            execution_mode_arg="paper",
            owner=_OWNER,
            generation=_GENERATION,
        )

        assert result is True, (
            f"Installed guard must return True for blank column with meta='paper'; "
            f"got {result!r}. Old LOWER(TRIM(COALESCE(execution_mode,'')))='', "
            f"which does not equal 'paper' → was a false CAS miss."
        )
    finally:
        with admin.cursor() as acur:
            acur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


def test_G3_whitespace_live_column_guard_persists():
    """Installed guard: orders.execution_mode=' live ' resolves to 'live'."""
    psycopg2 = pytest.importorskip("psycopg2")
    database_url = _pg_skip_or_fail()

    schema_prefix = "test_g3_ws_live"
    local_order_id = f"oid-g3-{uuid.uuid4().hex}"

    admin = psycopg2.connect(database_url)
    admin.autocommit = True
    try:
        with admin.cursor() as acur:
            schema = _make_schema(schema_prefix, acur)

        conn_ctx = _make_conn_ctx(psycopg2, database_url, schema)
        _insert_order(
            conn_ctx, schema,
            local_order_id=local_order_id,
            client_id=_CLIENT,
            signal_id=_SIGNAL,
            col_execution_mode=" live ",    # ← whitespace column
            meta_execution_mode="live",
            generation=_GENERATION,
            owner=_OWNER,
        )

        result = _call_guard(
            conn_ctx,
            local_order_id=local_order_id,
            client_id=_CLIENT,
            signal_id=_SIGNAL,
            execution_mode_arg="live",
            owner=_OWNER,
            generation=_GENERATION,
        )

        assert result is True, (
            f"Installed guard must return True for column=' live ' (whitespace); "
            f"got {result!r}."
        )
    finally:
        with admin.cursor() as acur:
            acur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


def test_G4_blank_column_meta_live_guard_persists():
    """Installed guard: column='' with meta.execution_mode='live' must return True."""
    psycopg2 = pytest.importorskip("psycopg2")
    database_url = _pg_skip_or_fail()

    schema_prefix = "test_g4_blank_meta_live"
    local_order_id = f"oid-g4-{uuid.uuid4().hex}"

    admin = psycopg2.connect(database_url)
    admin.autocommit = True
    try:
        with admin.cursor() as acur:
            schema = _make_schema(schema_prefix, acur)

        conn_ctx = _make_conn_ctx(psycopg2, database_url, schema)
        _insert_order(
            conn_ctx, schema,
            local_order_id=local_order_id,
            client_id=_CLIENT,
            signal_id=_SIGNAL,
            col_execution_mode="",          # ← blank column
            meta_execution_mode="live",     # ← fallback authority
            generation=_GENERATION,
            owner=_OWNER,
        )

        result = _call_guard(
            conn_ctx,
            local_order_id=local_order_id,
            client_id=_CLIENT,
            signal_id=_SIGNAL,
            execution_mode_arg="live",
            owner=_OWNER,
            generation=_GENERATION,
        )

        assert result is True, (
            f"Installed guard must return True for blank column with meta='live'; "
            f"got {result!r}."
        )
    finally:
        with admin.cursor() as acur:
            acur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


# ─────────────────────────────────────────────────────────────────────────────
# Negative controls H1–H5: wrong identity / lifecycle → False (CAS miss)
# ─────────────────────────────────────────────────────────────────────────────
# These prove the guard's CAS fence is intact: only an exact match across ALL
# WHERE predicates produces a rowcount=1 commit.  A mismatch on any single
# predicate yields rowcount=0 → False (not an exception).
# ─────────────────────────────────────────────────────────────────────────────


def _negative_setup(psycopg2, database_url, schema_prefix):
    """Shared setup for H tests: create schema, insert a known-good row,
    return (schema, local_order_id, conn_ctx, admin)."""
    local_order_id = f"oid-h-{uuid.uuid4().hex}"
    admin = psycopg2.connect(database_url)
    admin.autocommit = True
    with admin.cursor() as acur:
        schema = _make_schema(schema_prefix, acur)
    conn_ctx = _make_conn_ctx(psycopg2, database_url, schema)
    _insert_order(
        conn_ctx, schema,
        local_order_id=local_order_id,
        client_id=_CLIENT,
        signal_id=_SIGNAL,
        col_execution_mode="paper",
        meta_execution_mode=None,
        generation=_GENERATION,
        owner=_OWNER,
    )
    return schema, local_order_id, conn_ctx, admin


def test_H1_wrong_owner_returns_false():
    """Guard returns False (CAS miss) when materialization_owner mismatches."""
    psycopg2 = pytest.importorskip("psycopg2")
    database_url = _pg_skip_or_fail()
    schema, local_order_id, conn_ctx, admin = _negative_setup(
        psycopg2, database_url, "test_h1_wrong_owner"
    )
    try:
        result = _call_guard(
            conn_ctx,
            local_order_id=local_order_id,
            client_id=_CLIENT,
            signal_id=_SIGNAL,
            execution_mode_arg="paper",
            owner="watcher:WRONG_OWNER",    # ← does not match row
            generation=_GENERATION,
        )
        assert result is False, (
            f"Wrong owner must yield CAS miss (False); got {result!r}"
        )
    finally:
        with admin.cursor() as acur:
            acur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


def test_H2_wrong_generation_returns_false():
    """Guard returns False when materialization_generation mismatches."""
    psycopg2 = pytest.importorskip("psycopg2")
    database_url = _pg_skip_or_fail()
    schema, local_order_id, conn_ctx, admin = _negative_setup(
        psycopg2, database_url, "test_h2_wrong_gen"
    )
    try:
        result = _call_guard(
            conn_ctx,
            local_order_id=local_order_id,
            client_id=_CLIENT,
            signal_id=_SIGNAL,
            execution_mode_arg="paper",
            owner=_OWNER,
            generation=_GENERATION + 999,   # ← wrong generation
        )
        assert result is False, (
            f"Wrong generation must yield CAS miss (False); got {result!r}"
        )
    finally:
        with admin.cursor() as acur:
            acur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


def test_H3_wrong_client_returns_false():
    """Guard returns False when client_id on the OSM instance mismatches."""
    psycopg2 = pytest.importorskip("psycopg2")
    database_url = _pg_skip_or_fail()
    schema, local_order_id, conn_ctx, admin = _negative_setup(
        psycopg2, database_url, "test_h3_wrong_client"
    )
    try:
        result = _call_guard(
            conn_ctx,
            local_order_id=local_order_id,
            client_id="wrong_client@example.com",  # ← does not match row
            signal_id=_SIGNAL,
            execution_mode_arg="paper",
            owner=_OWNER,
            generation=_GENERATION,
        )
        assert result is False, (
            f"Wrong client_id must yield CAS miss (False); got {result!r}"
        )
    finally:
        with admin.cursor() as acur:
            acur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


def test_H4_wrong_signal_returns_false():
    """Guard returns False when signal_id argument mismatches the DB row."""
    psycopg2 = pytest.importorskip("psycopg2")
    database_url = _pg_skip_or_fail()
    schema, local_order_id, conn_ctx, admin = _negative_setup(
        psycopg2, database_url, "test_h4_wrong_signal"
    )
    try:
        result = _call_guard(
            conn_ctx,
            local_order_id=local_order_id,
            client_id=_CLIENT,
            signal_id="sig-WRONG-9999",     # ← does not match row
            execution_mode_arg="paper",
            owner=_OWNER,
            generation=_GENERATION,
        )
        assert result is False, (
            f"Wrong signal_id must yield CAS miss (False); got {result!r}"
        )
    finally:
        with admin.cursor() as acur:
            acur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


def test_H5_wrong_lifecycle_returns_false():
    """Guard returns False when lifecycle_state != 'MATERIALIZING'."""
    psycopg2 = pytest.importorskip("psycopg2")
    database_url = _pg_skip_or_fail()

    schema_prefix = "test_h5_wrong_lifecycle"
    local_order_id = f"oid-h5-{uuid.uuid4().hex}"
    admin = psycopg2.connect(database_url)
    admin.autocommit = True
    try:
        with admin.cursor() as acur:
            schema = _make_schema(schema_prefix, acur)

        conn_ctx = _make_conn_ctx(psycopg2, database_url, schema)
        _insert_order(
            conn_ctx, schema,
            local_order_id=local_order_id,
            client_id=_CLIENT,
            signal_id=_SIGNAL,
            col_execution_mode="paper",
            meta_execution_mode=None,
            generation=_GENERATION,
            owner=_OWNER,
            lifecycle_state="RETRY_WAIT",   # ← wrong lifecycle
        )

        result = _call_guard(
            conn_ctx,
            local_order_id=local_order_id,
            client_id=_CLIENT,
            signal_id=_SIGNAL,
            execution_mode_arg="paper",
            owner=_OWNER,
            generation=_GENERATION,
        )
        assert result is False, (
            f"Wrong lifecycle_state must yield CAS miss (False); got {result!r}"
        )
    finally:
        with admin.cursor() as acur:
            acur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


# ─────────────────────────────────────────────────────────────────────────────
# Contradiction proof I1/I2 — mock-based, no PostgreSQL required
#
# Contract: when column and meta authority DISAGREE (both non-blank, different
# values), resume_deferred_materialization_retry() MUST return
# RETRY_EXECUTION_MODE_AUTHORITY_CONFLICT **before** any OSM method is called.
# persist_selector_recovery_cursor therefore can never receive a contradictory
# row in the production call path.
#
# This replaces the need for a SQL predicate that would silently reclassify an
# authority conflict as an ownership-loss CAS miss (False), which the guard's
# contract forbids — False is reserved exclusively for exact fenced CAS misses.
# ─────────────────────────────────────────────────────────────────────────────

_CLIENT_I = "jose@example.com"
_SIGNAL_I = "sig-due-retry-1"
_LOCAL_ORDER_I = "oid-due-retry-1"
_GENERATION_I = 1
_RETRY_ATTEMPT_I = 1


def _row_raw_contradiction(col_mode: str, meta_mode: str) -> dict:
    now = datetime.now(timezone.utc)
    meta = {
        "lifecycle_state": "RETRY_WAIT",
        "materialization_status": "RETRY_PENDING",
        "materialization_generation": _GENERATION_I,
        "watcher_token": "",
        "retry_attempt": _RETRY_ATTEMPT_I,
        "breach_attempt_count": _RETRY_ATTEMPT_I,
        "materialization_attempts": _RETRY_ATTEMPT_I,
        "retry_max_attempts": 3,
        "next_retry_at": (now - timedelta(seconds=60)).isoformat(),
        "materialization_next_retry_at": (now - timedelta(seconds=60)).isoformat(),
        "trigger_crossed_at": (now - timedelta(seconds=120)).isoformat(),
        "trigger_price": 130.0,
        "observed_underlying_price": 130.05,
        "client_id": _CLIENT_I,
        "signal_id": _SIGNAL_I,
        "canonical_signal_id": _SIGNAL_I,
        "execution_mode": meta_mode,          # ← contradicts column
        "trigger_crossed_at_provenance": {
            "canonical_signal_id": _SIGNAL_I,
            "client_id": _CLIENT_I,
            "execution_mode": meta_mode,
            "local_order_id": _LOCAL_ORDER_I,
        },
    }
    return {
        "local_order_id": _LOCAL_ORDER_I,
        "client_id": _CLIENT_I,
        "execution_mode": col_mode,           # ← contradicts meta
        "signal_id": _SIGNAL_I,
        "plan_id": "plan-i-1",
        "kind": "ENTRY",
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "submitted_ts": None,
        "symbol": "RTX",
        "direction": "CALL",
        "score": 78.0,
        "tier": "B",
        "trigger_price": 130.0,
        "stop_underlying": 128.0,
        "target_underlying": 133.0,
        "pattern": "3-1-2",
        "timeframe": "1d",
        "contract": "DEFERRED:RTX",
        "qty": 1,
        "limit_price": 0.01,
        "reserved_cost": 0.0,
        "created_ts": now - timedelta(minutes=10),
        "meta": meta,
    }


def _core_i(execution_mode: str):
    import ap_execution_core

    core = SimpleNamespace(
        client_id=_CLIENT_I,
        email=_CLIENT_I,
        execution_mode=execution_mode,
        mode=execution_mode.upper(),
        paper=(execution_mode == "paper"),
        order_state_machine=MagicMock(),
        broker=MagicMock(),
    )
    # Remove retain_recovery_ownership_if_no_watcher so tests exercise the
    # update_order_meta() fallback path (same reasoning as _core() in the
    # deferred-retry ownership test file).
    del core.order_state_machine.retain_recovery_ownership_if_no_watcher
    core.resume_deferred_materialization_retry = (
        ap_execution_core.APExecutionCore
        .resume_deferred_materialization_retry.__get__(core, type(core))
    )
    core._on_entry_trigger = MagicMock()
    return core


@pytest.mark.parametrize("col_mode,meta_mode", [
    ("live", "paper"),
    ("paper", "live"),
])
def test_I_contradiction_fence_fires_before_cursor_persist(col_mode, meta_mode):
    """RETRY_EXECUTION_MODE_AUTHORITY_CONFLICT is raised in
    resume_deferred_materialization_retry() before any OSM method is invoked.

    Specifically: claim_deferred_materialization, schedule_deferred_materialization_retry,
    _on_entry_trigger, AND persist_selector_recovery_cursor are all
    assert_not_called().  This is the production call path guarantee that a
    contradictory row is never silently reclassified into a False CAS miss by
    the guard's SQL — it is rejected before reaching the guard at all.
    """
    core = _core_i(execution_mode=col_mode)
    row = _row_raw_contradiction(col_mode=col_mode, meta_mode=meta_mode)
    core.order_state_machine.get_order.return_value = row

    result = core.resume_deferred_materialization_retry(
        local_order_id=_LOCAL_ORDER_I,
        expected_generation=_GENERATION_I,
        expected_retry_attempt=_RETRY_ATTEMPT_I,
        owner="watcher:test-I",
    )

    # Must hard-fail with the contradiction code before ANY cursor work.
    assert result.get("reason_code") == "RETRY_EXECUTION_MODE_AUTHORITY_CONFLICT", (
        f"Expected AUTHORITY_CONFLICT for col={col_mode!r}/meta={meta_mode!r}; "
        f"got {result.get('reason_code')!r}"
    )
    assert result.get("disposition") == "TERMINAL_REQUIRED", (
        f"Contradiction must be TERMINAL_REQUIRED; got {result.get('disposition')!r}"
    )

    # Zero broker/selector/claim/cursor calls — contradiction is caught upstream.
    core.order_state_machine.claim_deferred_materialization.assert_not_called()
    core.order_state_machine.schedule_deferred_materialization_retry.assert_not_called()
    core.order_state_machine.persist_selector_recovery_cursor.assert_not_called()
    core._on_entry_trigger.assert_not_called()
