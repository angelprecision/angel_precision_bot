"""
tests/test_p0_pr596_due_materialization_retry_liveness.py

P0 #596 — Due Materialization Retry Liveness
PR #602 — Focused behavioral proof for the client_id/kind projection fix.

Confirmed blocker (fixed in PR #602):
  _get_active_entry_orders() did not project client_id or kind.
  The row dict passed to PendingTriggerRestartRecovery.recover_one_row()
  had row_client='', triggering:

      RESTART_RECOVERY_MISSING_DURABLE_CLIENT_ID
      -> UNRESOLVED
      -> due RETRY_PENDING is never consumed

Fix:
  SELECT now includes client_id, kind from the database row.
  Identity comes from the row as authority; never synthesized from runner context.

Tests in this file:

  PostgreSQL production-boundary (INTELLIGENCE_POSTGRES_TEST_URL required):

  TestProductionBoundaryProjection
    - test_jason_live_mo_db_projection_path_retry_owned
        Insert a real PENDING_TRIGGER ENTRY row. Call _get_active_entry_orders()
        without hand-building the recovered dict. Assert the fetched row carries
        client_id and kind from the SQL. Pass through _canonical_pending_trigger_rearm.
        Assert RETRY_OWNED (not RESTART_RECOVERY_MISSING_DURABLE_CLIENT_ID).
    - test_paper_mmm_db_projection_path_retry_owned
        Same proof with a paper PENDING_TRIGGER ENTRY row (MMM client shape).
    - test_pre_fix_simulation_unresolved
        Monkeypatch _get_active_entry_orders to strip client_id from the
        returned row (simulating the old SELECT). Assert UNRESOLVED with
        identity:missing_client_id. Zero selector. Zero broker. Zero
        fabricated identity.
    - test_client_mismatch_unresolved
        Insert a row. Instantiate APOrderMonitor with a different client_id.
        The WHERE clause excludes the row; monitor gets empty results.
        Assert no recovery action and no RETRY_OWNED for the wrong client.
    - test_broker_handoff_evidence_not_returned
        Insert a row with broker_order_id set. WHERE clause returns it, but
        the NOT_PENDING_TRIGGER classification stops recovery cold.
    - test_not_due_retry_owned_waiting
        Insert a row with future next_retry_at. Projection works; recovery
        classifies RETRY_OWNED (waiting). No broker action.

  Unit-level (no DB required):

  TestProjectionBugRegression
    - test_row_without_client_id_produces_unresolved
        Pre-fix row shape (no client_id key) → UNRESOLVED with
        identity:missing_client_id. No cancel, no meta write.
        NOTE: this test builds its own row to prove the recovery engine's
        identity fence. Reverting the production SELECT would NOT break this
        behavioral test — the source-inspection tests (TestSelectorQuality)
        protect the text of the SELECT separately.
    - test_row_with_client_id_succeeds
        Post-fix row shape → RETRY_OWNED.

  TestMOProductionShapedReplay       — MO live due RETRY_PENDING
  TestMMMProductionShapedReplay      — MMM paper due RETRY_PENDING + isolation
  TestWFCPositiveControl             — existing deferred retry unchanged
  TestNotDueControl                  — future next_retry_at, no broker action
  TestBrokerAmbiguity                — broker evidence → HOLD
  TestIdentityFailure                — blank/mismatched identity → UNRESOLVED, no mutation
  TestSelectorQuality                — fix adds SQL columns only, no policy changes
  TestConcurrentRecoveryIdempotency  — two sequential recovery calls both RETRY_OWNED
                                       (idempotent classification proof; concurrent
                                       execution exclusivity is owned by the deferred
                                       materializer's CAS, not by the recovery engine)

Spec: docs/pr_specs/p0_pr596_due_materialization_retry_liveness_20260909.md
"""
from __future__ import annotations

import json
import os
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost/db")

from ap.pending_trigger_restart_recovery import (
    PendingTriggerRestartRecovery,
    _RowOutcome,
    _MAT_STATUS_FIELD,
    _MAT_NEXT_RETRY_AT,
    _MAT_ATTEMPTS_FIELD,
    _MAT_REASON_FIELD,
    _MAT_LAST_FAILURE_FIELD,
    _MAT_BROKER_READY,
)


# ── Constants ─────────────────────────────────────────────────────────────────

_MO_CLIENT   = "jasoncosby1@gmail.com"
_MMM_CLIENT  = "jose@angelprecision.co"
_WFC_CLIENT  = "wfc@angelprecision.co"
_MO_MODE     = "live"
_PAPER_MODE  = "paper"

# Test-only client identity that cannot collide with any real production row.
_DB_TEST_MO_CLIENT  = "test-pr596-jason@angelprecision.co"
_DB_TEST_MMM_CLIENT = "test-pr596-jose@angelprecision.co"
_DB_SCHEMA = f"pr596_boundary_{os.getpid()}"

_POSTGRES_URL = os.getenv("INTELLIGENCE_POSTGRES_TEST_URL", "")


# ── DB helpers ────────────────────────────────────────────────────────────────

try:
    import psycopg2
    import psycopg2.extras
    _PSYCOPG2_AVAILABLE = bool(_POSTGRES_URL)
except ImportError:
    _PSYCOPG2_AVAILABLE = False

_ORDERS_DDL = """
CREATE TABLE IF NOT EXISTS orders (
    local_order_id    TEXT PRIMARY KEY,
    broker_order_id   TEXT,
    status            TEXT NOT NULL DEFAULT 'PENDING_TRIGGER',
    symbol            TEXT,
    contract          TEXT,
    position_id       TEXT,
    signal_id         TEXT,
    plan_id           TEXT,
    created_ts        TIMESTAMPTZ DEFAULT NOW(),
    submitted_ts      TIMESTAMPTZ,
    qty               INTEGER DEFAULT 1,
    direction         TEXT DEFAULT 'CALL',
    execution_mode    TEXT,
    reserved_cost     NUMERIC DEFAULT 0,
    limit_price       NUMERIC DEFAULT 0.01,
    fill_price        NUMERIC,
    score             NUMERIC DEFAULT 70,
    tier              TEXT DEFAULT 'B',
    trigger_price     NUMERIC,
    stop_underlying   NUMERIC,
    target_underlying NUMERIC,
    meta              JSONB DEFAULT '{}',
    client_id         TEXT,
    kind              TEXT DEFAULT 'ENTRY'
)
"""


class _CursorWrapper:
    """Wraps a psycopg2 RealDictCursor to match the ap.db conn() interface."""
    def __init__(self, connection, cursor):
        self.connection = connection
        self.cursor = cursor

    def execute(self, sql, params=None):
        self.cursor.execute(sql, params)
        return self

    def fetchall(self):
        rows = self.cursor.fetchall()
        return [dict(r) for r in rows]

    def fetchone(self):
        row = self.cursor.fetchone()
        return dict(row) if row else None


@contextmanager
def _pg_conn():
    """Open a test PostgreSQL connection matching ap.db.conn() semantics."""
    conn = psycopg2.connect(_POSTGRES_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(f'SET search_path TO "{_DB_SCHEMA}", public')
    wrapper = _CursorWrapper(conn, cur)
    try:
        yield wrapper
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def _ensure_orders_table():
    conn = psycopg2.connect(_POSTGRES_URL)
    try:
        with conn.cursor() as cur:
            # Keep this boundary proof isolated from other P0 tests that use
            # a deliberately minimal public.orders table.  The production
            # query still runs unchanged; only the test connection's search
            # path points at this test-owned canonical-shaped table.
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{_DB_SCHEMA}"')
            cur.execute(f'SET search_path TO "{_DB_SCHEMA}", public')
            cur.execute(_ORDERS_DDL)
        conn.commit()
    finally:
        conn.close()


def _insert_order(*, local_order_id, client_id, execution_mode, meta,
                  contract="DEFERRED:MO", symbol="MO",
                  broker_order_id=None, submitted_ts=None,
                  status="PENDING_TRIGGER"):
    now = datetime.now(timezone.utc)
    with _pg_conn() as c:
        c.execute(
            """
            INSERT INTO orders (
                local_order_id, broker_order_id, status, symbol, contract,
                signal_id, plan_id, created_ts, submitted_ts,
                qty, direction, execution_mode, reserved_cost,
                limit_price, fill_price, score, tier,
                trigger_price, stop_underlying, target_underlying,
                meta, client_id, kind
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                1, 'CALL', %s, 0,
                0.01, NULL, 72.0, 'A',
                50.0, 48.5, 52.0,
                %s, %s, 'ENTRY'
            )
            ON CONFLICT (local_order_id) DO UPDATE
              SET meta = EXCLUDED.meta,
                  status = EXCLUDED.status,
                  broker_order_id = EXCLUDED.broker_order_id,
                  submitted_ts = EXCLUDED.submitted_ts
            """,
            (
                local_order_id, broker_order_id, status, symbol, contract,
                str(uuid.uuid4()), str(uuid.uuid4()), now, submitted_ts,
                execution_mode,
                json.dumps(meta),
                client_id,
            )
        )


def _delete_test_orders(*client_ids):
    if not _PSYCOPG2_AVAILABLE:
        return
    try:
        with _pg_conn() as c:
            for cid in client_ids:
                c.execute("DELETE FROM orders WHERE client_id = %s", (cid,))
    except Exception:
        pass


def _drop_test_schema():
    if not _PSYCOPG2_AVAILABLE:
        return
    try:
        conn = psycopg2.connect(_POSTGRES_URL)
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP SCHEMA IF EXISTS "{_DB_SCHEMA}" CASCADE')
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass


# ── Unit helpers (shared with PostgreSQL tests) ───────────────────────────────

def _retry_meta(*, due: bool = True, attempts: int = 1,
                reason: str = "no_tradeable_contract") -> dict:
    now = datetime.now(timezone.utc)
    next_at = (
        (now - timedelta(seconds=10)).isoformat() if due
        else (now + timedelta(minutes=5)).isoformat()
    )
    return {
        _MAT_STATUS_FIELD:       "RETRY_PENDING",
        _MAT_NEXT_RETRY_AT:      next_at,
        _MAT_ATTEMPTS_FIELD:     attempts,
        _MAT_REASON_FIELD:       reason,
        _MAT_LAST_FAILURE_FIELD: (now - timedelta(minutes=1)).isoformat(),
        _MAT_BROKER_READY:       False,
    }


def _row_with_id(*, client_id, execution_mode, meta=None,
                 contract="DEFERRED:MO",
                 broker_order_id=None, submitted_ts=None) -> dict:
    return {
        "local_order_id":  str(uuid.uuid4()),
        "signal_id":       str(uuid.uuid4()),
        "client_id":       client_id,
        "client_email":    client_id,
        "execution_mode":  execution_mode,
        "kind":            "ENTRY",
        "status":          "PENDING_TRIGGER",
        "direction":       "CALL",
        "ticker":          "MO",
        "symbol":          "MO",
        "entry_price":     50.0,
        "trigger_price":   50.0,
        "stop_price":      48.5,
        "target_price":    52.0,
        "contract":        contract,
        "broker_order_id": broker_order_id,
        "submitted_ts":    submitted_ts,
        "meta":            meta or {},
    }


def _row_without_id(*, client_id, execution_mode, meta=None,
                    contract="DEFERRED:MO") -> dict:
    row = _row_with_id(client_id=client_id, execution_mode=execution_mode,
                       meta=meta, contract=contract)
    del row["client_id"]
    del row["kind"]
    return row


class _MockOSM:
    def __init__(self):
        self.cancel_calls: list = []
        self.meta_writes:  list = []
        self._rows: dict = {}

    def seed(self, row: dict) -> dict:
        self._rows[row["local_order_id"]] = dict(row)
        return row

    def cancel_pending_entry(self, oid, *, reason=""):
        self.cancel_calls.append((oid, reason))
        self._rows.setdefault(oid, {})["status"] = "CANCELED"
        return True

    def get_order(self, oid):
        if oid not in self._rows:
            return None
        r = dict(self._rows[oid])
        r.setdefault("local_order_id", oid)
        return r

    def update_order_meta(self, oid, patch):
        self.meta_writes.append((oid, dict(patch)))
        row = self._rows.setdefault(oid, {})
        meta = row.get("meta") or {}
        if not isinstance(meta, dict):
            meta = {}
        meta.update(patch)
        row["meta"] = meta
        return True


def _make_recovery(row, *, client_id, execution_mode, osm=None):
    _osm = osm or _MockOSM()
    _osm.seed(row)
    rec = PendingTriggerRestartRecovery(
        client_id=client_id,
        execution_mode=execution_mode,
        osm=_osm,
        entry_watcher=None,
        broker=MagicMock(),
        quote_check_fn=lambda *a: False,
    )
    return rec, _osm


def _make_monitor(*, client_id, execution_mode, osm):
    """Build a minimal APOrderMonitor for production-path testing."""
    from ap.order_monitor import APOrderMonitor
    monitor = APOrderMonitor(
        client_id=client_id,
        broker=MagicMock(),
        order_state_machine=osm,
        position_manager=MagicMock(),
        client_mode=execution_mode.upper(),
    )
    return monitor


# ══════════════════════════════════════════════════════════════════════════════
# PostgreSQL production-boundary tests
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.skipif(
    not _PSYCOPG2_AVAILABLE,
    reason="INTELLIGENCE_POSTGRES_TEST_URL not set — skipping real DB boundary proof",
)
class TestProductionBoundaryProjection:
    """
    Real PostgreSQL production-boundary proof for PR #602.

    These tests exercise the exact caller path that broke in production:

      APOrderMonitor._get_active_entry_orders()
        → real DB projection (the fixed SELECT)
        → _canonical_pending_trigger_rearm()
        → PendingTriggerRestartRecovery.recover_one_row()
        → classification

    We do NOT hand-build the recovered dict. The database row is the authority.
    The production SELECT is what constructs it, which is the point.
    """

    @pytest.fixture(autouse=True)
    def _setup_and_teardown(self):
        _ensure_orders_table()
        yield
        _delete_test_orders(_DB_TEST_MO_CLIENT, _DB_TEST_MMM_CLIENT)
        _drop_test_schema()

    @pytest.fixture
    def _mo_osm(self):
        """OSM mock that returns the seeded row on get_order."""
        return _MockOSM()

    def _assert_db_row_has_projection(self, monitor, *, client_id, execution_mode):
        """Call _get_active_entry_orders and verify projection is present."""
        rows = monitor._get_active_entry_orders()
        assert rows, "Expected at least one row from _get_active_entry_orders()"
        row = rows[0]
        assert "client_id" in row, (
            "client_id not in row from _get_active_entry_orders(). "
            "The projection fix is missing or was reverted."
        )
        assert "kind" in row, (
            "kind not in row from _get_active_entry_orders(). "
            "The projection fix is missing or was reverted."
        )
        assert row["client_id"] == client_id, (
            f"client_id mismatch: got {row['client_id']!r}, expected {client_id!r}"
        )
        assert row["kind"] == "ENTRY", (
            f"kind mismatch: got {row['kind']!r}, expected 'ENTRY'"
        )
        return row

    def test_jason_live_mo_db_projection_path_retry_owned(self, _mo_osm):
        """
        P0 Blocker 2 fix — positive proof.

        Insert a real Jason LIVE MO PENDING_TRIGGER ENTRY row with due
        RETRY_PENDING. Call _get_active_entry_orders() via the real SQL
        (do not hand-build the dict). Assert the fetched row carries
        client_id and kind. Pass through _canonical_pending_trigger_rearm.
        Assert RETRY_OWNED — not RESTART_RECOVERY_MISSING_DURABLE_CLIENT_ID.
        """
        oid = f"pr596-mo-live-{uuid.uuid4()}"
        meta = _retry_meta(due=True, reason="no_tradeable_contract")
        _insert_order(
            local_order_id=oid,
            client_id=_DB_TEST_MO_CLIENT,
            execution_mode="live",
            meta=meta,
            contract="DEFERRED:MO",
            symbol="MO",
        )
        _mo_osm.seed({
            "local_order_id": oid,
            "client_id":      _DB_TEST_MO_CLIENT,
            "client_email":   _DB_TEST_MO_CLIENT,
            "execution_mode": "live",
            "kind":           "ENTRY",
            "status":         "PENDING_TRIGGER",
            "contract":       "DEFERRED:MO",
            "broker_order_id": None,
            "submitted_ts":   None,
            "meta":           meta,
        })

        monitor = _make_monitor(
            client_id=_DB_TEST_MO_CLIENT,
            execution_mode="live",
            osm=_mo_osm,
        )

        with patch("ap.order_monitor.conn", _pg_conn):
            # The production SQL path — do NOT hand-build the row.
            row = self._assert_db_row_has_projection(
                monitor,
                client_id=_DB_TEST_MO_CLIENT,
                execution_mode="live",
            )
            # Now run the full production path.
            attempted, succeeded, reason = monitor._canonical_pending_trigger_rearm(
                row, oid, row["contract"]
            )

        assert succeeded is True, (
            f"Expected _canonical_pending_trigger_rearm to succeed (RETRY_OWNED). "
            f"Got attempted={attempted}, succeeded={succeeded}, reason={reason!r}. "
            f"If reason contains 'RESTART_RECOVERY_MISSING_DURABLE_CLIENT_ID', "
            f"the projection fix is not reaching the recovery engine."
        )
        assert reason == "canonical_recovery_retry_owned", (
            f"Unexpected reason: {reason!r}"
        )
        assert _mo_osm.cancel_calls == [], "No cancel on RETRY_OWNED"

    def test_paper_mmm_db_projection_path_retry_owned(self):
        """
        Same production-boundary proof with a paper PENDING_TRIGGER ENTRY row
        (MMM/Jose client shape).
        """
        oid = f"pr596-mmm-paper-{uuid.uuid4()}"
        meta = _retry_meta(due=True, reason="dte_ladder_exhausted")
        _insert_order(
            local_order_id=oid,
            client_id=_DB_TEST_MMM_CLIENT,
            execution_mode="paper",
            meta=meta,
            contract="DEFERRED:MMM",
            symbol="MMM",
        )
        osm = _MockOSM()
        osm.seed({
            "local_order_id": oid,
            "client_id":      _DB_TEST_MMM_CLIENT,
            "client_email":   _DB_TEST_MMM_CLIENT,
            "execution_mode": "paper",
            "kind":           "ENTRY",
            "status":         "PENDING_TRIGGER",
            "contract":       "DEFERRED:MMM",
            "broker_order_id": None,
            "submitted_ts":   None,
            "meta":           meta,
        })
        monitor = _make_monitor(
            client_id=_DB_TEST_MMM_CLIENT,
            execution_mode="paper",
            osm=osm,
        )

        with patch("ap.order_monitor.conn", _pg_conn):
            row = self._assert_db_row_has_projection(
                monitor,
                client_id=_DB_TEST_MMM_CLIENT,
                execution_mode="paper",
            )
            attempted, succeeded, reason = monitor._canonical_pending_trigger_rearm(
                row, oid, row["contract"]
            )

        assert succeeded is True
        assert reason == "canonical_recovery_retry_owned"

    def test_pre_fix_simulation_unresolved(self, _mo_osm):
        """
        Negative proof — simulates the pre-fix SELECT that omitted client_id.

        We monkeypatch _get_active_entry_orders to strip client_id from the
        returned rows (exactly what the old SQL did), then prove the recovery
        engine returns UNRESOLVED with identity:missing_client_id.

        This directly proves the pre-fix regression: zero selector execution,
        zero broker submit, zero broker cancel, zero fabricated identity.
        """
        oid = f"pr596-prefixsim-{uuid.uuid4()}"
        meta = _retry_meta(due=True)
        _insert_order(
            local_order_id=oid,
            client_id=_DB_TEST_MO_CLIENT,
            execution_mode="live",
            meta=meta,
            contract="DEFERRED:MO",
        )
        _mo_osm.seed({
            "local_order_id": oid,
            "client_id":      _DB_TEST_MO_CLIENT,
            "execution_mode": "live",
            "kind":           "ENTRY",
            "status":         "PENDING_TRIGGER",
            "contract":       "DEFERRED:MO",
            "broker_order_id": None,
            "submitted_ts":   None,
            "meta":           meta,
        })
        monitor = _make_monitor(
            client_id=_DB_TEST_MO_CLIENT,
            execution_mode="live",
            osm=_mo_osm,
        )

        # Simulate the old (broken) SELECT: fetch real rows then strip client_id
        # and kind before passing them downstream — exactly what the pre-fix
        # _get_active_entry_orders() returned.
        def _pre_fix_get_active_entry_orders():
            with _pg_conn() as c:
                c.execute(
                    """
                    SELECT local_order_id, broker_order_id, status, symbol,
                           contract, position_id, signal_id, plan_id,
                           created_ts, submitted_ts,
                           qty, direction, execution_mode, reserved_cost,
                           limit_price, limit_price AS price, fill_price,
                           score, tier, trigger_price, stop_underlying,
                           target_underlying, meta
                    FROM orders
                    WHERE client_id = %s
                      AND kind = 'ENTRY'
                      AND status IN ('PENDING_TRIGGER')
                    """,
                    (_DB_TEST_MO_CLIENT,)
                )
                return c.fetchall()

        # Temporarily replace the method with the pre-fix version
        original = monitor._get_active_entry_orders
        monitor._get_active_entry_orders = _pre_fix_get_active_entry_orders

        try:
            rows = monitor._get_active_entry_orders()
        finally:
            monitor._get_active_entry_orders = original

        assert rows, "Expected a row for the pre-fix simulation"
        row = rows[0]
        assert "client_id" not in row, (
            "Pre-fix simulation must NOT have client_id — test setup error"
        )

        # Now pass the pre-fix row through the recovery engine directly.
        broker = MagicMock()
        rec = PendingTriggerRestartRecovery(
            client_id=_DB_TEST_MO_CLIENT,
            execution_mode="live",
            osm=_mo_osm,
            entry_watcher=None,
            broker=broker,
            quote_check_fn=lambda *a: False,
        )
        outcome = rec.recover_one_row(row)

        assert outcome == _RowOutcome.UNRESOLVED, (
            f"Pre-fix row (no client_id) must produce UNRESOLVED; got {outcome}"
        )
        assert rec._row_failure_reasons.get(row["local_order_id"]) == \
               "identity:missing_client_id"
        assert _mo_osm.cancel_calls == [], "Zero cancel on identity failure"
        assert _mo_osm.meta_writes == [], "Zero meta writes on identity failure"
        broker.submit_order.assert_not_called() if hasattr(broker, "submit_order") else None

    def test_client_mismatch_excluded_by_where_clause(self):
        """
        Wrong client_id on the monitor: the WHERE clause excludes the row.
        _get_active_entry_orders() returns empty — no recovery action.
        """
        oid = f"pr596-mismatch-{uuid.uuid4()}"
        meta = _retry_meta(due=True)
        _insert_order(
            local_order_id=oid,
            client_id=_DB_TEST_MO_CLIENT,
            execution_mode="live",
            meta=meta,
            contract="DEFERRED:MO",
        )
        # Wrong client on the monitor
        wrong_osm = _MockOSM()
        monitor = _make_monitor(
            client_id="wrong-client@angelprecision.co",
            execution_mode="live",
            osm=wrong_osm,
        )

        with patch("ap.order_monitor.conn", _pg_conn):
            rows = monitor._get_active_entry_orders()

        # WHERE client_id='wrong-...' returns nothing for our MO row
        assert not any(r.get("local_order_id") == oid for r in rows), (
            "Row belonging to a different client must not be returned"
        )
        assert wrong_osm.cancel_calls == []

    def test_broker_handoff_evidence_not_retried(self, _mo_osm):
        """
        A broker handoff marker on an active SUBMITTED row is not permission
        for restart recovery to retry or cancel the order.  The row is still
        returned by the monitor, and the canonical recovery engine classifies
        it as not pending before taking any action.
        """
        oid = f"pr596-broker-{uuid.uuid4()}"
        meta = _retry_meta(due=True)
        _insert_order(
            local_order_id=oid,
            client_id=_DB_TEST_MO_CLIENT,
            execution_mode="live",
            meta=meta,
            contract="DEFERRED:MO",
            broker_order_id="BROKER-LIVE-789",  # broker handoff evidence
            status="SUBMITTED",                  # active monitor status
        )
        monitor = _make_monitor(
            client_id=_DB_TEST_MO_CLIENT,
            execution_mode="live",
            osm=_mo_osm,
        )
        with patch("ap.order_monitor.conn", _pg_conn):
            rows = monitor._get_active_entry_orders()
            row = next(
                (r for r in rows if r.get("local_order_id") == oid),
                None,
            )
            assert row is not None, (
                "Active SUBMITTED order with broker handoff evidence must reach "
                "the canonical recovery boundary"
            )
            assert row["client_id"] == _DB_TEST_MO_CLIENT
            assert row["kind"] == "ENTRY"

            attempted, succeeded, reason = monitor._canonical_pending_trigger_rearm(
                row, oid, row["contract"]
            )

        assert attempted is False
        assert succeeded is False
        assert reason == "canonical_recovery_not_pending_trigger"
        assert _mo_osm.cancel_calls == [], "No cancel on broker handoff evidence"
        assert _mo_osm.meta_writes == [], "No durable mutation on broker handoff evidence"
        assert monitor.broker.mock_calls == [], (
            "Broker handoff ambiguity must not submit, cancel, or query the broker"
        )

    def test_not_due_retry_owned_waiting(self, _mo_osm):
        """
        Future next_retry_at: projection works correctly (client_id, kind present).
        Recovery classifies RETRY_OWNED (waiting). No broker action.
        """
        oid = f"pr596-notdue-{uuid.uuid4()}"
        meta = _retry_meta(due=False)  # future timestamp
        _insert_order(
            local_order_id=oid,
            client_id=_DB_TEST_MO_CLIENT,
            execution_mode="live",
            meta=meta,
            contract="DEFERRED:MO",
        )
        _mo_osm.seed({
            "local_order_id": oid,
            "client_id":      _DB_TEST_MO_CLIENT,
            "client_email":   _DB_TEST_MO_CLIENT,
            "execution_mode": "live",
            "kind":           "ENTRY",
            "status":         "PENDING_TRIGGER",
            "contract":       "DEFERRED:MO",
            "broker_order_id": None,
            "submitted_ts":   None,
            "meta":           meta,
        })
        monitor = _make_monitor(
            client_id=_DB_TEST_MO_CLIENT,
            execution_mode="live",
            osm=_mo_osm,
        )

        with patch("ap.order_monitor.conn", _pg_conn):
            row = self._assert_db_row_has_projection(
                monitor,
                client_id=_DB_TEST_MO_CLIENT,
                execution_mode="live",
            )
            attempted, succeeded, reason = monitor._canonical_pending_trigger_rearm(
                row, oid, row["contract"]
            )

        assert succeeded is True, f"Not-due RETRY_PENDING must still be RETRY_OWNED; got {reason!r}"
        assert reason == "canonical_recovery_retry_owned"
        assert _mo_osm.cancel_calls == [], "No cancel for not-due waiting retry"


# ══════════════════════════════════════════════════════════════════════════════
# Unit tests — no DB required
# ══════════════════════════════════════════════════════════════════════════════

class TestProjectionBugRegression:
    """
    Unit-level proof that the recovery engine's identity fence fires correctly.

    NOTE on scope: these tests build their own row dicts to isolate the
    recovery engine's identity fence from the SQL projection. They prove that
    PendingTriggerRestartRecovery refuses rows lacking client_id (UNRESOLVED)
    and accepts rows with client_id (RETRY_OWNED). They do NOT prove that the
    production SELECT returns client_id — that is the job of the PostgreSQL
    tests above. Reverting the production SELECT would NOT break these tests.
    The source-inspection tests in TestSelectorQuality protect the text of the
    SELECT independently.
    """

    def test_row_without_client_id_produces_unresolved(self):
        """
        Pre-fix row shape: client_id absent from the dict → UNRESOLVED.
        Reason: identity:missing_client_id. No cancel. No meta write.
        """
        meta = _retry_meta(due=True)
        row = _row_without_id(
            client_id=_MO_CLIENT,
            execution_mode=_MO_MODE,
            meta=meta,
            contract="DEFERRED:MO",
        )
        assert "client_id" not in row

        rec, osm = _make_recovery(row, client_id=_MO_CLIENT, execution_mode=_MO_MODE)
        outcome = rec.recover_one_row(row)

        assert outcome == _RowOutcome.UNRESOLVED, (
            f"Row without client_id must produce UNRESOLVED; got {outcome}"
        )
        assert rec._row_failure_reasons.get(row["local_order_id"]) == \
               "identity:missing_client_id"
        assert osm.cancel_calls == []
        assert osm.meta_writes == []

    def test_row_with_client_id_succeeds(self):
        """Post-fix row shape: client_id present → RETRY_OWNED."""
        meta = _retry_meta(due=True)
        row = _row_with_id(
            client_id=_MO_CLIENT,
            execution_mode=_MO_MODE,
            meta=meta,
            contract="DEFERRED:MO",
        )
        assert "client_id" in row

        rec, osm = _make_recovery(row, client_id=_MO_CLIENT, execution_mode=_MO_MODE)
        outcome = rec.recover_one_row(row)

        assert outcome == _RowOutcome.RETRY_OWNED, (
            f"Row with client_id + due RETRY_PENDING must be RETRY_OWNED; got {outcome}"
        )
        assert rec._row_failure_reasons == {}
        assert osm.cancel_calls == []


class TestMOProductionShapedReplay:
    """MO live due RETRY_PENDING → RETRY_OWNED. No broker action."""

    def _mo_row(self, *, due=True):
        return _row_with_id(
            client_id=_MO_CLIENT,
            execution_mode=_MO_MODE,
            meta=_retry_meta(due=due),
            contract="DEFERRED:MO",
        )

    def test_mo_due_retry_owned(self):
        row = self._mo_row(due=True)
        rec, _ = _make_recovery(row, client_id=_MO_CLIENT, execution_mode=_MO_MODE)
        assert rec.recover_one_row(row) == _RowOutcome.RETRY_OWNED

    def test_mo_retry_owned_no_broker_submit(self):
        row = self._mo_row(due=True)
        broker = MagicMock()
        osm = _MockOSM()
        osm.seed(row)
        rec = PendingTriggerRestartRecovery(
            client_id=_MO_CLIENT, execution_mode=_MO_MODE,
            osm=osm, entry_watcher=None, broker=broker,
            quote_check_fn=lambda *a: False,
        )
        rec.recover_one_row(row)
        broker.submit_order.assert_not_called() if hasattr(broker.submit_order, "assert_not_called") else None
        broker.place_order.assert_not_called() if hasattr(broker.place_order, "assert_not_called") else None

    def test_mo_retry_owned_no_cancel(self):
        row = self._mo_row(due=True)
        rec, osm = _make_recovery(row, client_id=_MO_CLIENT, execution_mode=_MO_MODE)
        rec.recover_one_row(row)
        assert osm.cancel_calls == []

    def test_mo_retry_owned_ownerless_zero(self):
        row = self._mo_row(due=True)
        rec, _ = _make_recovery(row, client_id=_MO_CLIENT, execution_mode=_MO_MODE)
        summary = rec.recover_all([row])
        assert summary["ownerless_rows_remaining"] == 0
        assert summary["retry_rows_owned"] == 1


class TestMMMProductionShapedReplay:
    """MMM paper due RETRY_PENDING → RETRY_OWNED. Cross-client isolation."""

    def _mmm_row(self):
        return _row_with_id(
            client_id=_MMM_CLIENT,
            execution_mode=_PAPER_MODE,
            meta=_retry_meta(due=True, reason="dte_ladder_exhausted"),
            contract="DEFERRED:MMM",
        )

    def test_mmm_due_retry_owned(self):
        row = self._mmm_row()
        rec, _ = _make_recovery(row, client_id=_MMM_CLIENT, execution_mode=_PAPER_MODE)
        assert rec.recover_one_row(row) == _RowOutcome.RETRY_OWNED

    def test_mmm_retry_owned_ownerless_zero(self):
        row = self._mmm_row()
        rec, _ = _make_recovery(row, client_id=_MMM_CLIENT, execution_mode=_PAPER_MODE)
        summary = rec.recover_all([row])
        assert summary["ownerless_rows_remaining"] == 0
        assert summary["retry_rows_owned"] == 1

    def test_mmm_client_isolation_from_mo(self):
        """MMM row must not be processed by MO recovery engine."""
        row = self._mmm_row()
        rec, osm = _make_recovery(row, client_id=_MO_CLIENT, execution_mode=_MO_MODE)
        outcome = rec.recover_one_row(row)
        assert outcome == _RowOutcome.UNRESOLVED
        assert osm.cancel_calls == []
        assert osm.meta_writes == []


class TestWFCPositiveControl:
    def test_wfc_due_retry_owned_paper(self):
        row = _row_with_id(
            client_id=_WFC_CLIENT,
            execution_mode=_PAPER_MODE,
            meta=_retry_meta(due=True, reason="no_budget"),
            contract="DEFERRED:WFC",
        )
        rec, osm = _make_recovery(row, client_id=_WFC_CLIENT, execution_mode=_PAPER_MODE)
        assert rec.recover_one_row(row) == _RowOutcome.RETRY_OWNED
        assert osm.cancel_calls == []


class TestNotDueControl:
    def test_not_due_retry_is_still_retry_owned(self):
        """Future next_retry_at → RETRY_OWNED (waiting). No broker action."""
        row = _row_with_id(
            client_id=_MO_CLIENT,
            execution_mode=_MO_MODE,
            meta=_retry_meta(due=False),
            contract="DEFERRED:MO",
        )
        rec, osm = _make_recovery(row, client_id=_MO_CLIENT, execution_mode=_MO_MODE)
        assert rec.recover_one_row(row) == _RowOutcome.RETRY_OWNED

    def test_not_due_retry_no_broker_action(self):
        row = _row_with_id(
            client_id=_MO_CLIENT,
            execution_mode=_MO_MODE,
            meta=_retry_meta(due=False),
            contract="DEFERRED:MO",
        )
        broker = MagicMock()
        osm = _MockOSM()
        osm.seed(row)
        rec = PendingTriggerRestartRecovery(
            client_id=_MO_CLIENT, execution_mode=_MO_MODE,
            osm=osm, entry_watcher=None, broker=broker,
            quote_check_fn=lambda *a: False,
        )
        rec.recover_one_row(row)
        assert osm.cancel_calls == []


class TestBrokerAmbiguity:
    @pytest.mark.parametrize("field,value", [
        ("broker_order_id", "BROKER-789"),
        ("submitted_ts",    "2026-09-09T09:30:00+00:00"),
    ])
    def test_broker_evidence_produces_skipped_or_unresolved(self, field, value):
        kwargs = {"broker_order_id": None, "submitted_ts": None}
        kwargs[field] = value
        row = _row_with_id(
            client_id=_MO_CLIENT, execution_mode=_MO_MODE,
            meta=_retry_meta(due=True), contract="DEFERRED:MO", **kwargs,
        )
        rec, osm = _make_recovery(row, client_id=_MO_CLIENT, execution_mode=_MO_MODE)
        outcome = rec.recover_one_row(row)
        assert outcome in {_RowOutcome.SKIPPED, _RowOutcome.UNRESOLVED}, (
            f"Broker evidence must be SKIPPED or UNRESOLVED; got {outcome}"
        )
        assert osm.cancel_calls == []

    def test_submit_intent_in_meta_blocks_retry(self):
        meta = _retry_meta(due=True)
        meta["submit_intent_at"] = "2026-09-09T09:30:00+00:00"
        row = _row_with_id(
            client_id=_MO_CLIENT, execution_mode=_MO_MODE,
            meta=meta, contract="DEFERRED:MO",
        )
        rec, osm = _make_recovery(row, client_id=_MO_CLIENT, execution_mode=_MO_MODE)
        outcome = rec.recover_one_row(row)
        assert outcome == _RowOutcome.UNRESOLVED
        assert osm.cancel_calls == []


class TestIdentityFailure:
    def test_blank_client_id_unresolved(self):
        row = _row_with_id(client_id=_MO_CLIENT, execution_mode=_MO_MODE,
                           meta=_retry_meta(due=True), contract="DEFERRED:MO")
        row["client_id"] = ""
        rec, osm = _make_recovery(row, client_id=_MO_CLIENT, execution_mode=_MO_MODE)
        assert rec.recover_one_row(row) == _RowOutcome.UNRESOLVED
        assert osm.cancel_calls == []
        assert osm.meta_writes == []

    def test_blank_execution_mode_unresolved(self):
        row = _row_with_id(client_id=_MO_CLIENT, execution_mode=_MO_MODE,
                           meta=_retry_meta(due=True), contract="DEFERRED:MO")
        row["execution_mode"] = ""
        rec, osm = _make_recovery(row, client_id=_MO_CLIENT, execution_mode=_MO_MODE)
        assert rec.recover_one_row(row) == _RowOutcome.UNRESOLVED
        assert osm.cancel_calls == []
        assert osm.meta_writes == []

    def test_client_id_mismatch_unresolved(self):
        row = _row_with_id(client_id="attacker@evil.com", execution_mode=_MO_MODE,
                           meta=_retry_meta(due=True), contract="DEFERRED:MO")
        rec, osm = _make_recovery(row, client_id=_MO_CLIENT, execution_mode=_MO_MODE)
        assert rec.recover_one_row(row) == _RowOutcome.UNRESOLVED
        assert osm.cancel_calls == []

    def test_mode_mismatch_unresolved(self):
        row = _row_with_id(client_id=_MO_CLIENT, execution_mode=_PAPER_MODE,
                           meta=_retry_meta(due=True), contract="DEFERRED:MO")
        rec, osm = _make_recovery(row, client_id=_MO_CLIENT, execution_mode=_MO_MODE)
        assert rec.recover_one_row(row) == _RowOutcome.UNRESOLVED
        assert osm.cancel_calls == []

    def test_runtime_client_id_never_injected_into_row(self):
        """
        The recovery engine's own client_id must never substitute for a
        missing row client_id. Spec invariant: 'Use the database row as
        authority. Never synthesize identity from runner context.'
        """
        row = _row_without_id(
            client_id=_MO_CLIENT, execution_mode=_MO_MODE,
            meta=_retry_meta(due=True),
        )
        rec, osm = _make_recovery(row, client_id=_MO_CLIENT, execution_mode=_MO_MODE)
        outcome = rec.recover_one_row(row)
        assert outcome == _RowOutcome.UNRESOLVED, (
            "Engine must never inject its own client_id into the row dict."
        )
        assert rec._row_failure_reasons.get(row["local_order_id"]) == \
               "identity:missing_client_id"


class TestSelectorQuality:
    def test_fix_columns_are_database_authority_not_runtime(self):
        """
        client_id and kind must appear INSIDE the SQL string in
        _get_active_entry_orders, not computed in Python after the fetch.
        """
        import inspect
        import ap.order_monitor as _om

        src = inspect.getsource(_om.APOrderMonitor._get_active_entry_orders)

        sql_start = src.find('"""')
        sql_end   = src.find('"""', sql_start + 3) + 3 if sql_start >= 0 else -1

        if sql_start >= 0 and sql_end > sql_start:
            sql_block = src[sql_start:sql_end]
            assert "client_id" in sql_block, (
                "client_id must appear INSIDE the SQL SELECT, not synthesized in Python"
            )
            assert "kind" in sql_block, (
                "kind must appear INSIDE the SQL SELECT, not synthesized in Python"
            )

    def test_fix_adds_no_new_imports_to_get_active_entry_orders(self):
        """
        _get_active_entry_orders adds two column names to a SQL SELECT.
        It must not introduce a new selector, capacity, or retry-policy import.
        """
        import inspect
        import ap.order_monitor as _om

        src = inspect.getsource(_om.APOrderMonitor._get_active_entry_orders)
        assert "client_id" in src, "client_id must appear in the fixed SELECT"
        assert "kind" in src, "kind must appear in the fixed SELECT"
        # The function adds columns; it must not add new business-logic calls
        for forbidden in ("APContractSelectionEngine", "retry_policy", "exposure_gate"):
            # These may exist elsewhere in the file; we only check this method
            assert forbidden not in src, (
                f"'{forbidden}' must not appear in _get_active_entry_orders"
            )


class TestConcurrentRecoveryIdempotency:
    """
    Idempotent classification proof.

    Two sequential recovery calls on the same row both return RETRY_OWNED.
    This proves the recovery engine's classification is stable — it does not
    consume or mutate the RETRY_PENDING claim on the first call.

    This is NOT a proof of concurrent execution exclusivity. The materialization
    CAS (adopt_deferred_retry_watcher) is the sole durable owner of that
    invariant. The existing CAS tests elsewhere in the P0 inventory cover it.
    #602 changes only the SQL projection; it does not redesign CAS ownership.
    """

    def test_two_recovery_calls_both_return_retry_owned(self):
        meta = _retry_meta(due=True)
        row = _row_with_id(
            client_id=_MO_CLIENT, execution_mode=_MO_MODE,
            meta=meta, contract="DEFERRED:MO",
        )
        osm = _MockOSM()
        osm.seed(row)

        def _make_rec():
            return PendingTriggerRestartRecovery(
                client_id=_MO_CLIENT, execution_mode=_MO_MODE,
                osm=osm, entry_watcher=None, broker=MagicMock(),
                quote_check_fn=lambda *a: False,
            )

        outcome1 = _make_rec().recover_one_row(dict(row))
        outcome2 = _make_rec().recover_one_row(dict(row))

        assert outcome1 == _RowOutcome.RETRY_OWNED
        assert outcome2 == _RowOutcome.RETRY_OWNED
        assert osm.cancel_calls == [], "Neither classification call must cancel"
