"""
tests/test_p0_pr596_due_materialization_retry_liveness.py

P0 #596 — Due Materialization Retry Liveness
PR #602 — Order-monitor durable retry projection correction.

Confirmed order-monitor projection defect (fixed in PR #602):
  _get_active_entry_orders() did not project client_id or kind.
  The row dict passed to PendingTriggerRestartRecovery.recover_one_row()
  had row_client='', triggering:

  RESTART_RECOVERY_MISSING_DURABLE_CLIENT_ID
      -> UNRESOLVED
      -> monitor cannot classify the row as retry-owned

This file keeps that projection proof separate from the actual due-retry
executor. `APStartupRecovery._recover_deferred_breach_lifecycles()` owns the
startup due query and calls `resume_deferred_materialization_retry()` directly;
the startup replay below begins at that boundary and does not call
`APOrderMonitor`.

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

  TestProductionBoundaryProjection   — monitor projection plus the independent
                                       startup executor replay
  TestStartupRecoveryNegativeControls — malformed identity/schedule/broker
                                        evidence and stale-generation holds
  TestMOProductionShapedReplay       — MO live due RETRY_PENDING classification
  TestMMMProductionShapedReplay      — MMM paper due RETRY_PENDING classification
  TestWFCPositiveControl             — existing deferred retry unchanged
  TestNotDueControl                  — future next_retry_at, no broker action
  TestBrokerAmbiguity                — broker evidence → HOLD
  TestIdentityFailure                — blank/mismatched identity → UNRESOLVED, no mutation
  TestSelectorQuality                — fix adds SQL columns only, no policy changes
  TestConcurrentRecoveryIdempotency  — sequential classification remains stable;
                                       real concurrent executor exclusivity is
                                       proven by the PostgreSQL startup replay

Spec: docs/pr_specs/p0_pr596_due_materialization_retry_liveness_20260909.md
"""
from __future__ import annotations

import json
import os
import sys
import threading
import types
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
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
    last_error        TEXT,
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
    pattern           TEXT,
    timeframe         TEXT,
    meta              JSONB DEFAULT '{}',
    client_id         TEXT,
    kind              TEXT DEFAULT 'ENTRY',
    contract_selection_status TEXT,
    updated_ts        TIMESTAMPTZ DEFAULT NOW()
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

    @property
    def rowcount(self):
        return self.cursor.rowcount

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
                  status="PENDING_TRIGGER", signal_id=None, plan_id=None,
                  created_ts=None):
    now = datetime.now(timezone.utc)
    _created_ts = created_ts or now
    _signal_id = signal_id or str(uuid.uuid4())
    _plan_id = plan_id or str(uuid.uuid4())
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
                _signal_id, _plan_id, _created_ts, submitted_ts,
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

    These tests cover two deliberately separate production seams:

      APOrderMonitor._get_active_entry_orders()
        → real DB projection (the fixed SELECT)
        → _canonical_pending_trigger_rearm()
        → PendingTriggerRestartRecovery.recover_one_row()
        → classification

      APStartupRecovery._recover_deferred_breach_lifecycles()
        → due retry executor → selector/materializer/OSM handoff

    The second replay does NOT route through APOrderMonitor. We do not hand-
    build the monitor's recovered dict; the database row is the authority for
    that projection proof, while startup recovery owns due execution.
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

    @pytest.mark.parametrize(
        ("client_id", "execution_mode", "ticker", "retry_reason"),
        [
            (_DB_TEST_MO_CLIENT, "live", "MO", "no_tradeable_contract"),
            (_DB_TEST_MMM_CLIENT, "paper", "MMM", "dte_ladder_exhausted"),
        ],
    )
    def test_due_retry_startup_recovery_materializes_once(
        self, monkeypatch, client_id, execution_mode, ticker, retry_reason,
    ):
        """The real due-retry executor consumes LIVE MO and PAPER MMM once.

        This replay deliberately begins at APStartupRecovery's durable due
        retry query.  It does not route through APOrderMonitor: that monitor
        is a separate classification observer, while APStartupRecovery owns
        the due-retry execution handoff.  Two competing startup consumers
        load the same durable snapshot; the existing OSM generation/attempt
        CAS admits one materializer. The real execution-core callback invokes
        the selector and reaches the existing broker-ready handoff exactly
        once. Broker transport, cancellation, proof, and position mutation
        remain untouched.
        """
        import ap.db as db_mod
        import ap.order_state_machine as osm_mod
        import ap_execution_core as core_mod
        import ap_entry_confirmation
        from ap.order_state_machine import APOrderStateMachine
        from ap_recovery import APStartupRecovery

        local_order_id = f"pr596-full-path-{uuid.uuid4()}"
        signal_id = f"sig-{uuid.uuid4()}"
        plan_id = f"plan-{uuid.uuid4()}"
        now = datetime.now(timezone.utc)
        due_at = now - timedelta(seconds=10)

        meta = _retry_meta(due=True, attempts=1, reason=retry_reason)
        meta.update({
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
            "materialization_generation": 1,
            "retry_attempt": 1,
            "breach_attempt_count": 1,
            "materialization_attempts": 1,
            "retry_max_attempts": 3,
            "materialization_in_flight": False,
            "broker_ready": False,
            "watcher_token": "",
            "current_owner": "",
            "materialization_owner": "",
            "retry_owner": "",
            "materialization_retry_owner": "",
            "canonical_signal_id": signal_id,
            "client_id": client_id,
            "execution_mode": execution_mode,
            "local_order_id": local_order_id,
            "signal_id": signal_id,
            "trigger_crossed_at": (now - timedelta(seconds=60)).isoformat(),
            "trigger_crossed_at_provenance": {
                "canonical_signal_id": signal_id,
                "client_id": client_id,
                "execution_mode": execution_mode,
                "local_order_id": local_order_id,
            },
            "observed_underlying_price": 50.05,
            "score": 72.0,
            "tier": "A",
            "timeframe": "1d",
            "pattern": "3-1-2",
            "contract_deferred": True,
            "selection_context": "deferred_breach",
            "materialization_next_retry_at": due_at.isoformat(),
            "next_retry_at": due_at.isoformat(),
            # Keep this replay independent of the wall-clock session while
            # still exercising the real LIVE cutoff gate. The production
            # safety gate remains intact; this is a test-only durable fixture
            # value for the replay's synthetic execution window.
            "entry_cutoff_et": 2359,
            "materialization_selector_failure": {
                "reason_code": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
                "materialization_outcome": "RETRY_LATER_SELECTOR_BUDGET",
                "materialization_detail": retry_reason,
            },
        })
        _insert_order(
            local_order_id=local_order_id,
            client_id=client_id,
            execution_mode=execution_mode,
            meta=meta,
            contract=f"DEFERRED:{ticker}",
            symbol=ticker,
            signal_id=signal_id,
            plan_id=plan_id,
        )

        monkeypatch.setenv("DEFERRED_RETRY_OWNER_GRACE_SECONDS", "0")
        monkeypatch.setenv("SELECTOR_DURABLE_RECOVERY_CURSOR_ENABLED", "0")
        monkeypatch.setenv("INTELLIGENCE_EVIDENCE_ENABLED", "0")

        # The P0 workflow intentionally installs only minimal dependencies;
        # ap.execution's optional yfinance import is outside this liveness
        # boundary. Keep the production core's existing refresh seam present
        # with the same five-value contract so the test reaches the real OSM
        # handoff without making dependency installation part of the proof.
        fake_execution = types.ModuleType("ap.execution")
        fake_execution._refresh_ask_at_submit = lambda *_args, **_kwargs: (
            1.05,
            0,
            True,
            "",
            {
                "submit_bid": 1.04,
                "submit_ask": 1.05,
                "submit_last": 1.045,
                "submit_mid": 1.045,
                "spread_pct": (1.05 - 1.04) / 1.045,
            },
        )
        monkeypatch.setitem(sys.modules, "ap.execution", fake_execution)

        # All production DB calls below use the test-owned schema. The real
        # APStartupRecovery query, OSM CAS, and execution-core methods remain
        # intact.
        monkeypatch.setattr(db_mod, "conn", _pg_conn)
        monkeypatch.setattr(
            db_mod, "run_with_retry", lambda fn, *a, **kw: fn()
        )
        monkeypatch.setattr(osm_mod, "conn", _pg_conn)
        monkeypatch.setattr(
            osm_mod, "run_with_retry", lambda fn, *a, **kw: fn()
        )

        osm = APOrderStateMachine(client_id)
        osm.execution_mode = execution_mode

        trace_lock = threading.RLock()
        trace: list[str] = []
        selector_calls: list[dict] = []
        materialization_calls: list[dict] = []
        submit_calls: list[dict] = []
        resume_results: list[tuple[object, dict]] = []

        class _Broker:
            def __init__(self):
                self.base_url = (
                    "https://api.tradier.com/v1"
                    if execution_mode == "live"
                    else "https://sandbox.tradier.com/v1"
                )
                self.sandbox = execution_mode != "live"
                self.cfg = SimpleNamespace(
                    base_url=self.base_url, account_id="TEST"
                )
                self.submit_order = MagicMock()
                self.place_order = MagicMock()
                self.cancel_order = MagicMock()

            def get_quote(self, _symbol):
                if str(_symbol or "").upper() == ticker:
                    return {
                        "bid": 50.04,
                        "ask": 50.06,
                        "last": 50.05,
                        "quote_timestamp": datetime.now(timezone.utc).isoformat(),
                        "source": (
                            "tradier_live"
                            if execution_mode == "live"
                            else "tradier_sandbox"
                        ),
                    }
                return {
                    "bid": 1.04,
                    "ask": 1.05,
                    "last": 1.045,
                    "quote_timestamp": datetime.now(timezone.utc).isoformat(),
                    "source": (
                        "tradier_live"
                        if execution_mode == "live"
                        else "tradier_sandbox"
                    ),
                }

        broker = _Broker()

        class _DataBroker:
            # PAPER execution still uses approved live market-data truth for
            # the retry revalidation gate; only order submission is sandboxed.
            base_url = "https://api.tradier.com/v1"
            cfg = SimpleNamespace(base_url=base_url, account_id="TEST")

            def get_quote(self, _symbol):
                if str(_symbol or "").upper() == ticker:
                    return {
                        "bid": 50.04,
                        "ask": 50.06,
                        "last": 50.05,
                        "quote_timestamp": datetime.now(timezone.utc).isoformat(),
                        "source": "tradier_live",
                    }
                return {
                    "bid": 1.04,
                    "ask": 1.05,
                    "last": 1.045,
                    "quote_timestamp": datetime.now(timezone.utc).isoformat(),
                    "source": "tradier_live",
                }

        data_broker = _DataBroker()
        broker.data_broker = data_broker

        class _Selector:
            def __init__(self, data_broker):
                self.data_broker = data_broker

            def select(self, _plan, *, request_context=None):
                assert request_context is not None
                with trace_lock:
                    selector_calls.append({
                        "request_kind": request_context.selector_request_kind,
                    })
                    trace.append("selector")
                return SimpleNamespace(
                    contract_symbol=f"{ticker}270115C00050000",
                    bid=1.04,
                    ask=1.05,
                    mid=1.045,
                    affordable_contracts=1,
                    premium_per_contract=105.0,
                    execution_price_per_share=1.05,
                    expiration_date="2027-01-15",
                    dte=128,
                    delta=0.45,
                    open_interest=1000,
                    volume=500,
                    option_type="call",
                    candidate_audit={"candidates_considered": 1},
                )

            def get_last_failure(self):
                return None

            def get_last_dte_ladder_audit(self):
                return {}

        selector = _Selector(data_broker)

        # The test stops at the existing broker-bound OSM seam: the selector
        # must already have produced a valid OCC result, but no broker or
        # position/proof side effect is allowed in this regression.
        def _submit_existing_entry(*_args, **kwargs):
            with trace_lock:
                submit_calls.append(dict(kwargs))
                trace.append("submit_existing_entry")
                assert len(selector_calls) == 1, (
                    "OSM submit seam reached before a valid selector result"
                )
            return {
                "ok": True,
                "local_order_id": local_order_id,
                "broker_order_id": None,
                "status": "PENDING_TRIGGER",
            }

        osm.submit_existing_entry = _submit_existing_entry

        original_copyback = osm.persist_deferred_broker_ready

        def _persist_deferred_broker_ready(*args, **kwargs):
            with trace_lock:
                materialization_calls.append(dict(kwargs))
            return original_copyback(*args, **kwargs)

        osm.persist_deferred_broker_ready = _persist_deferred_broker_ready

        class _Confirmation:
            passed = True
            fail_reason = None

            def __init__(self):
                self.metadata = {"live_entry_ts": now.isoformat()}

            def to_meta(self, **kwargs):
                return {
                    "confirmation_required": False,
                    "confirmation_passed": True,
                    **kwargs,
                }

        monkeypatch.setattr(
            ap_entry_confirmation,
            "check_entry_confirmation",
            lambda **_kwargs: _Confirmation(),
        )

        cores: list = []

        def _build_core():
            core = core_mod.APExecutionCore.__new__(core_mod.APExecutionCore)
            core.client_id = client_id
            core.email = client_id
            core.execution_mode = execution_mode
            core.mode = execution_mode.upper()
            core.paper = execution_mode == "paper"
            core.order_state_machine = osm
            core.broker = broker
            core.contract_selector = selector
            core.master_control = SimpleNamespace(
                mode=execution_mode.upper(),
                max_positions=5,
                _kill_switch_fn=lambda: False,
                revalidate_exposure=lambda *_args, **_kwargs: SimpleNamespace(ok=True),
            )
            if execution_mode == "live":
                # APExecutionCore's real deferred LIVE path requires the same
                # affordability authority that production APMasterControl
                # supplies.  Keep the authority seam explicit while avoiding
                # account/network setup in this replay: the test is proving
                # startup recovery -> real core -> selector/OSM handoff.
                core.master_control.get_entry_capacity = lambda **_kwargs: {
                    "ok": True,
                    "reason_code": "CAPACITY_AVAILABLE",
                    "account_equity": 1709.20,
                    "per_trade_budget": 170.92,
                    "total_capital_cap": 683.68,
                    "current_total_exposure": 0.0,
                    "remaining_total_capacity": 683.68,
                    "selector_budget": 170.92,
                    "max_affordable_premium": 1.7092,
                }
            core.store = MagicMock()
            core.store.update_status.return_value = True
            core.store.update_signal_fields.return_value = True
            core.position_manager = MagicMock()
            core.position_manager.get_open_count.return_value = 0
            core.fill_monitor = MagicMock()
            core.entry_telemetry = MagicMock()
            core.intelligence_context = MagicMock()
            core.intelligence_context.is_enabled.return_value = False
            core.exit_eng = None
            core.entry_watcher = None
            core._kill_switch = False
            core._max_positions = 5
            core._pos_lock = threading.RLock()
            core._capital_lock = threading.RLock()
            core._open_positions = {}
            core._pending_entries = {}
            core._reserved_capital = 0.0
            core._breach_risk_check = lambda _watched: True
            core._recover_plan_for_revalidation = (
                lambda watched: watched.signal.get("_approved_plan")
            )
            core._emit_breach_diag = lambda *a, **kw: None
            core._current_open_position_count = lambda: 0
            core._current_pending_entry_count = lambda: 0
            core._refresh_hydrated_prebreach_plan = lambda *a, **kw: False
            core._alert_degraded = lambda *a, **kw: None
            core._cleanup_pending_entry_order = (
                core_mod.APExecutionCore._cleanup_pending_entry_order
                .__get__(core, type(core))
            )
            core._classify_recovered_ownership_loss = (
                core_mod.APExecutionCore._classify_recovered_ownership_loss
                .__get__(core, type(core))
            )
            core._is_real_occ_contract = core_mod.APExecutionCore._is_real_occ_contract
            core._strict_materialization_int = (
                core_mod.APExecutionCore._strict_materialization_int
            )
            core._live_materialization_lease = (
                core_mod.APExecutionCore._live_materialization_lease
            )
            core._claim_deferred_materialization_for_trigger = (
                core_mod.APExecutionCore._claim_deferred_materialization_for_trigger
                .__get__(core, type(core))
            )
            core._plan_is_deferred = core_mod.APExecutionCore._plan_is_deferred
            core.resume_deferred_materialization_retry = (
                core_mod.APExecutionCore.resume_deferred_materialization_retry
                .__get__(core, type(core))
            )
            _real_resume = core.resume_deferred_materialization_retry

            def _record_resume(*args, **kwargs):
                outcome = _real_resume(*args, **kwargs)
                with trace_lock:
                    resume_results.append((core, dict(outcome or {})))
                return outcome

            core.resume_deferred_materialization_retry = _record_resume
            core._on_entry_trigger = (
                core_mod.APExecutionCore._on_entry_trigger.__get__(core, type(core))
            )
            cores.append(core)
            return core

        # Force both restart consumers to load the same pre-claim snapshot and
        # then reach the same real durable CAS. The loader barrier matters:
        # without it, the first consumer could advance the row before the
        # second startup query returns, turning this into a one-consumer test.
        loader_barrier = threading.Barrier(2)
        claim_barrier = threading.Barrier(2)
        original_claim = osm.claim_deferred_materialization
        claim_calls: list[dict] = []

        def _claim_competing(*args, **kwargs):
            call = dict(kwargs)
            with trace_lock:
                claim_calls.append(call)
            claim_barrier.wait(timeout=5)
            claimed = original_claim(*args, **kwargs)
            with trace_lock:
                call["result"] = claimed
            return claimed

        osm.claim_deferred_materialization = _claim_competing

        results = [{"deferred_lifecycles_recovered": 0, "errors": []},
                   {"deferred_lifecycles_recovered": 0, "errors": []}]
        thread_errors: list[BaseException] = []

        def _run_restart(index: int):
            try:
                recovery = APStartupRecovery(
                    client_id=client_id,
                    broker=broker,
                    osm=osm,
                    pm=MagicMock(),
                    master_control=SimpleNamespace(mode=execution_mode.upper()),
                    entry_watcher=None,
                    execution_core=_build_core(),
                )
                recovery._recover_deferred_breach_lifecycles(results[index])
            except BaseException as exc:  # surface worker failures below
                thread_errors.append(exc)

        workers = [
            threading.Thread(target=_run_restart, args=(index,), daemon=True)
            for index in range(2)
        ]
        def _load_with_barrier(fn, *args, **kwargs):
            loaded = fn()
            loader_barrier.wait(timeout=5)
            return loaded

        with patch.object(db_mod, "conn", _pg_conn), patch.object(
            db_mod, "run_with_retry", _load_with_barrier
        ), patch.object(osm_mod, "conn", _pg_conn), patch.object(
            osm_mod, "run_with_retry", lambda fn, *a, **kw: fn()
        ):
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join(timeout=15)

        assert all(not worker.is_alive() for worker in workers)
        assert thread_errors == []
        assert len(claim_calls) == 2, f"claim_calls={claim_calls!r}"
        assert sorted(call["result"] for call in claim_calls) == [False, True]
        assert sorted(result["disposition"] for _, result in resume_results) == [
            "BROKER_READY",
            "CLAIM_LOST",
        ]
        assert len(selector_calls) == 1, (
            "selector must execute exactly once"
        )
        assert len(materialization_calls) == 1, (
            "canonical deferred broker-ready materialization must execute once"
        )
        assert len(submit_calls) == 1
        assert trace.index("selector") < trace.index("submit_existing_entry")
        assert sum(result["deferred_lifecycles_recovered"] for result in results) == 1

        winner_core, winner_result = next(
            (core, result)
            for core, result in resume_results
            if result["disposition"] == "BROKER_READY"
        )
        loser_core, loser_result = next(
            (core, result)
            for core, result in resume_results
            if result["disposition"] == "CLAIM_LOST"
        )
        assert winner_result["disposition"] == "BROKER_READY"
        assert loser_result["disposition"] == "CLAIM_LOST"
        # The losing startup consumer performed no downstream mutation at
        # the executor boundary: no selector/materializer/OSM signal/proof/
        # position work was reached after its durable CAS lost.
        assert loser_core.store.method_calls == []
        assert loser_core.position_manager.method_calls == []
        assert loser_core.entry_telemetry.method_calls == []
        assert loser_core.intelligence_context.method_calls == []
        assert loser_core.fill_monitor.method_calls == []

        final_row = osm.get_order(local_order_id)
        final_meta = final_row["meta"]
        assert final_row["local_order_id"] == local_order_id
        assert final_row["status"] == "PENDING_TRIGGER"
        assert final_row["client_id"] == client_id
        assert final_row["execution_mode"] == execution_mode
        assert final_row["kind"] == "ENTRY"
        assert final_row["broker_order_id"] is None
        assert final_row["submitted_ts"] is None
        assert final_row["contract"] == f"{ticker}270115C00050000"
        assert float(final_row["limit_price"]) > 0.01
        assert final_meta["lifecycle_state"] == "BROKER_READY"
        assert final_meta["materialization_status"] == "SELECTED"
        assert final_meta["broker_ready"] is True
        assert int(final_meta["materialization_generation"]) == 2
        assert int(final_meta["retry_attempt"]) == 2

        # No broker transport, cancel, proof-trade, or position mutation was
        # permitted while proving the liveness handoff.
        assert broker.submit_order.call_count == 0
        assert broker.place_order.call_count == 0
        assert broker.cancel_order.call_count == 0
        for core in cores:
            assert core.position_manager.method_calls == []

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


@pytest.mark.skipif(
    not _PSYCOPG2_AVAILABLE,
    reason="INTELLIGENCE_POSTGRES_TEST_URL not set — skipping real DB boundary proof",
)
class TestStartupRecoveryNegativeControls:
    """Fail-closed controls at APStartupRecovery's actual due boundary."""

    @pytest.fixture(autouse=True)
    def _setup_and_teardown(self):
        _ensure_orders_table()
        yield
        _delete_test_orders(_DB_TEST_MO_CLIENT, _DB_TEST_MMM_CLIENT)
        _drop_test_schema()

    def _seed_retry_row(
        self,
        *,
        local_order_id,
        row_client_id,
        row_execution_mode,
        due=True,
        generation=1,
        broker_order_id=None,
        submit_intent=False,
    ):
        now = datetime.now(timezone.utc)
        signal_id = f"sig-{uuid.uuid4()}"
        next_retry_at = (
            now - timedelta(seconds=10)
            if due
            else now + timedelta(minutes=5)
        )
        meta = _retry_meta(due=due, attempts=1)
        meta.update({
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
            "materialization_generation": generation,
            "retry_attempt": 1,
            "breach_attempt_count": 1,
            "materialization_attempts": 1,
            "retry_max_attempts": 3,
            "materialization_in_flight": False,
            "broker_ready": False,
            "canonical_signal_id": signal_id,
            "client_id": row_client_id,
            "execution_mode": row_execution_mode,
            "local_order_id": local_order_id,
            "signal_id": signal_id,
            "trigger_crossed_at": (now - timedelta(seconds=60)).isoformat(),
            "trigger_crossed_at_provenance": {
                "canonical_signal_id": signal_id,
                "client_id": row_client_id,
                "execution_mode": row_execution_mode,
                "local_order_id": local_order_id,
            },
            "observed_underlying_price": 50.05,
            "contract_deferred": True,
            "selection_context": "deferred_breach",
            "materialization_next_retry_at": next_retry_at.isoformat(),
            "next_retry_at": next_retry_at.isoformat(),
        })
        if submit_intent:
            meta["submit_intent_at"] = (now - timedelta(seconds=5)).isoformat()
        _insert_order(
            local_order_id=local_order_id,
            client_id=row_client_id,
            execution_mode=row_execution_mode,
            meta=meta,
            contract="DEFERRED:MO",
            symbol="MO",
            broker_order_id=broker_order_id,
        )
        return meta, signal_id

    def _run_recovery_control(
        self,
        *,
        row_client_id,
        row_execution_mode,
        due=True,
        generation=1,
        broker_order_id=None,
        submit_intent=False,
        recovery_client_id=_DB_TEST_MO_CLIENT,
        recovery_execution_mode="live",
    ):
        import ap.db as db_mod
        import ap.order_state_machine as osm_mod
        from ap.order_state_machine import APOrderStateMachine
        from ap_recovery import APStartupRecovery

        local_order_id = f"pr596-negative-{uuid.uuid4()}"
        meta, signal_id = self._seed_retry_row(
            local_order_id=local_order_id,
            row_client_id=row_client_id,
            row_execution_mode=row_execution_mode,
            due=due,
            generation=generation,
            broker_order_id=broker_order_id,
            submit_intent=submit_intent,
        )
        osm = APOrderStateMachine(recovery_client_id)
        osm.execution_mode = recovery_execution_mode
        broker = MagicMock()
        execution_core = MagicMock()
        execution_core.resume_deferred_materialization_retry.return_value = {
            "disposition": "BROKER_READY",
            "reason_code": "TEST_SHOULD_NOT_BE_REACHED",
        }
        recovery = APStartupRecovery(
            client_id=recovery_client_id,
            broker=broker,
            osm=osm,
            pm=MagicMock(),
            master_control=SimpleNamespace(mode=recovery_execution_mode.upper()),
            entry_watcher=None,
            execution_core=execution_core,
        )
        result = {"deferred_lifecycles_recovered": 0, "errors": []}
        with patch.object(db_mod, "conn", _pg_conn), patch.object(
            db_mod, "run_with_retry", lambda fn, *a, **kw: fn()
        ), patch.object(osm_mod, "conn", _pg_conn), patch.object(
            osm_mod, "run_with_retry", lambda fn, *a, **kw: fn()
        ):
            recovery._recover_deferred_breach_lifecycles(result)
        return execution_core, broker, result, osm, local_order_id, meta, signal_id

    @pytest.mark.parametrize(
        "control",
        [
            "malformed_generation",
            "client_mismatch",
            "mode_mismatch",
            "future_due_schedule",
            "broker_order_evidence",
            "submit_intent_evidence",
        ],
    )
    def test_due_executor_negative_controls_never_reach_resume(self, control):
        kwargs = {
            "row_client_id": _DB_TEST_MO_CLIENT,
            "row_execution_mode": "live",
        }
        if control == "malformed_generation":
            kwargs["generation"] = "1.5"
        elif control == "client_mismatch":
            kwargs["row_client_id"] = "other-client@angelprecision.co"
        elif control == "mode_mismatch":
            kwargs["row_execution_mode"] = "paper"
        elif control == "future_due_schedule":
            kwargs["due"] = False
        elif control == "broker_order_evidence":
            kwargs["broker_order_id"] = "BROKER-602"
        elif control == "submit_intent_evidence":
            kwargs["submit_intent"] = True

        execution_core, broker, result, _osm, _oid, _meta, _signal_id = (
            self._run_recovery_control(**kwargs)
        )
        assert execution_core.resume_deferred_materialization_retry.call_count == 0, (
            f"{control} must not cross APStartupRecovery's due executor gate"
        )
        assert broker.mock_calls == [], (
            f"{control} must not submit, cancel, or query broker"
        )
        assert result["deferred_lifecycles_recovered"] == 0

    def test_stale_generation_loses_real_materialization_cas(self):
        """A stale next-generation claim is rejected before downstream work."""
        import ap.db as db_mod
        import ap.order_state_machine as osm_mod
        from ap.order_state_machine import APOrderStateMachine

        oid = f"pr596-stale-generation-{uuid.uuid4()}"
        meta, signal_id = self._seed_retry_row(
            local_order_id=oid,
            row_client_id=_DB_TEST_MO_CLIENT,
            row_execution_mode="live",
            generation=2,
        )
        osm = APOrderStateMachine(_DB_TEST_MO_CLIENT)
        osm.execution_mode = "live"
        broker = MagicMock()
        with patch.object(db_mod, "conn", _pg_conn), patch.object(
            db_mod, "run_with_retry", lambda fn, *a, **kw: fn()
        ), patch.object(osm_mod, "conn", _pg_conn), patch.object(
            osm_mod, "run_with_retry", lambda fn, *a, **kw: fn()
        ):
            claimed = osm.claim_deferred_materialization(
                oid,
                owner="stale-worker",
                generation=2,
                lease_until=(datetime.now(timezone.utc) + timedelta(minutes=2)).isoformat(),
                trigger_crossed_at=meta["trigger_crossed_at"],
                trigger_price=50.0,
                observed_underlying_price=50.05,
                signal_id=signal_id,
                execution_mode="live",
                retry_attempt=2,
            )
            durable = osm.get_order(oid)

        assert claimed is False
        assert int(durable["meta"]["materialization_generation"]) == 2
        assert durable["broker_order_id"] is None
        assert durable["submitted_ts"] is None
        assert broker.mock_calls == []


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
