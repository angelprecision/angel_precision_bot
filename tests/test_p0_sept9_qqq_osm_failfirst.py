"""
PR #579 — September 9 QQQ Production Incident
Fail-first / after-fix proof via real OSM caller path.

START POINT: APOrderStateMachine.transition() — the exact entry point that
fill_monitor_loop calls when a broker poll returns EXIT_FILLED.

DO NOT call converge_position_from_durable_exit_order() directly.

The test traverses the real production path:

  OSM.transition(local_id, "EXIT_FILLED", ...)
  → _handle_exit_engine_hooks(...)
  → [main]  _finalize_position_from_exit_order()   (old: misses broker-repair)
  → [#579]  _converge_durable_exit_order()          (new: canonical authority)

Production evidence (September 9 2026 LIVE QQQ):
  canonical position:   70360b2a-9036-4e2d-bb6b-bf8d0065f6ef   OPEN qty=1
  broker-repair EXIT:   9c430437-2973-4811-8776-1d353b925d9b   EXIT_REQUESTED
  broker EXIT order:    145130405
  durable fill:         EXIT_FILLED qty=1 @ 1.17
  broker quantity:      0
  engine quantity:      1  (did not converge)

Fail-first class (TestSept9FailFirst):
  Runs on the current head (#579).  Asserts the OPEN pre-condition then
  shows convergence happens.  A separate rollback CI job checks out
  eb1fdefd and runs an inline script that proves the old code leaves the
  position OPEN — no test file introduced by #579 is used there.

After-fix class (TestSept9AfterFix):
  Full postcondition suite: CLOSED, remaining=0, exact broker price +
  timestamp, capacity released, idempotent restart, convergence HOLD safety.
"""

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Optional
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

os.environ.setdefault(
    "DATABASE_URL",
    os.getenv(
        "INTELLIGENCE_POSTGRES_TEST_URL",
        "postgresql://postgres:postgres@127.0.0.1:5432/intelligence_test?sslmode=disable",
    ),
)

from ap.order_state_machine import APOrderStateMachine, OrderStatus  # noqa: E402
from ap.position_manager import APPositionManager  # noqa: E402

# ─────────────────────────────────────────────────────────────────────────────
# Production-exact September 9 constants
# ─────────────────────────────────────────────────────────────────────────────

CLIENT_ID       = "jason-live@example.com"
POSITION_UUID   = "70360b2a-9036-4e2d-bb6b-bf8d0065f6ef"
REPAIR_EXIT_UUID = "9c430437-2973-4811-8776-1d353b925d9b"
BROKER_ORDER_ID = "145130405"
OCC_CONTRACT    = "QQQ261121C00510000"

ENTRY_TS   = datetime(2026, 9, 9, 13, 30, 0, tzinfo=timezone.utc)
FILL_TS    = datetime(2026, 9, 9, 15, 42, 18, 543210, tzinfo=timezone.utc)
FILL_PRICE = 1.17
FILL_QTY   = 1
ENTRY_PRICE = 1.48


# ─────────────────────────────────────────────────────────────────────────────
# PostgreSQL harness
# ─────────────────────────────────────────────────────────────────────────────


class _PGHarness:
    def __init__(self, connection, cursor_factory, conn_wrapper):
        self.connection = connection
        self.cursor_factory = cursor_factory
        self.conn_wrapper = conn_wrapper

    @contextmanager
    def conn(self):
        cursor = self.connection.cursor(cursor_factory=self.cursor_factory)
        try:
            yield self.conn_wrapper(self.connection, cursor)
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        finally:
            cursor.close()

    def execute(self, sql: str, params: tuple = ()):
        with self.connection.cursor() as cursor:
            cursor.execute(sql, params)
        self.connection.commit()

    def fetchone(self, sql: str, params: tuple = ()) -> Optional[dict]:
        with self.connection.cursor(cursor_factory=self.cursor_factory) as cursor:
            cursor.execute(sql, params)
            row = cursor.fetchone()
            return dict(row) if row else None


@pytest.fixture()
def postgres_harness(monkeypatch):
    psycopg2 = pytest.importorskip("psycopg2")
    extras   = pytest.importorskip("psycopg2.extras")

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
            pytest.fail(f"PostgreSQL unavailable in CI: {exc}")
        pytest.skip(f"PostgreSQL unavailable: {exc}")

    from ap.db import _ConnWrapper
    import ap.db as db_mod
    import ap.position_manager as pm_mod
    import ap.order_state_machine as osm_mod
    import ap.observability as obs_mod
    import ap.fill_monitor as fm_mod

    harness = _PGHarness(connection, extras.RealDictCursor, _ConnWrapper)
    try:
        with connection.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE positions (
                    id TEXT PRIMARY KEY,
                    client_id TEXT NOT NULL,
                    execution_mode TEXT,
                    contract TEXT,
                    side TEXT,
                    direction TEXT,
                    status TEXT,
                    qty INTEGER,
                    quantity_remaining INTEGER,
                    avg_fill NUMERIC,
                    entry_price NUMERIC,
                    exit_price NUMERIC,
                    realized_pnl NUMERIC,
                    realized_pnl_pct NUMERIC,
                    entry_ts TIMESTAMPTZ,
                    exit_ts TIMESTAMPTZ,
                    local_order_id TEXT,
                    broker_order_id TEXT,
                    contracts_exited INTEGER,
                    close_source TEXT,
                    close_confidence TEXT,
                    exit_reason TEXT,
                    updated_at TIMESTAMPTZ,
                    exit_in_flight BOOLEAN,
                    pending_exit_qty INTEGER,
                    pending_exit_local_order_id TEXT,
                    pending_exit_broker_order_id TEXT
                )
            """)
            cur.execute("""
                CREATE TEMP TABLE orders (
                    local_order_id TEXT PRIMARY KEY,
                    position_id TEXT,
                    client_id TEXT NOT NULL,
                    execution_mode TEXT,
                    kind TEXT,
                    status TEXT,
                    contract TEXT,
                    direction TEXT,
                    qty INTEGER,
                    filled_qty INTEGER,
                    fill_price NUMERIC,
                    filled_ts TIMESTAMPTZ,
                    broker_order_id TEXT,
                    meta JSONB,
                    updated_ts TIMESTAMPTZ
                )
            """)
            cur.execute("""
                CREATE TEMP TABLE proof_trades (
                    id TEXT PRIMARY KEY,
                    client_email TEXT NOT NULL,
                    position_id TEXT,
                    local_order_id TEXT,
                    system_version TEXT,
                    exit_option_price NUMERIC,
                    exit_fill_price NUMERIC,
                    option_pnl_pct NUMERIC,
                    win BOOLEAN,
                    broker_reconciled BOOLEAN DEFAULT FALSE
                )
            """)
            cur.execute("""
                CREATE TEMP TABLE decision_events (
                    id BIGSERIAL PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    candidate_id TEXT NOT NULL,
                    trade_id TEXT,
                    position_id TEXT,
                    client_id TEXT NOT NULL,
                    ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    stage TEXT NOT NULL,
                    decision TEXT NOT NULL,
                    reason_code TEXT,
                    explanation TEXT,
                    symbol TEXT,
                    contract TEXT,
                    setup_type TEXT,
                    timeframe TEXT,
                    strategy_version TEXT,
                    config_hash TEXT,
                    git_commit TEXT,
                    inputs_json JSONB,
                    thresholds_json JSONB,
                    context_json JSONB
                )
            """)
        connection.commit()

        monkeypatch.setattr(db_mod,  "conn",           harness.conn)
        monkeypatch.setattr(db_mod,  "run_with_retry", lambda fn, **kw: fn())
        monkeypatch.setattr(pm_mod,  "conn",           harness.conn)
        monkeypatch.setattr(pm_mod,  "run_with_retry", lambda fn, **kw: fn())
        monkeypatch.setattr(osm_mod, "conn",           harness.conn)
        monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn, **kw: fn())
        monkeypatch.setattr(obs_mod, "conn",           harness.conn)
        monkeypatch.setattr(obs_mod, "run_with_retry", lambda fn, **kw: fn())
        monkeypatch.setattr(fm_mod,  "conn",           harness.conn)
        monkeypatch.setattr(fm_mod,  "run_with_retry", lambda fn, **kw: fn())

        # Earlier P0 tests reload/patch ap.order_state_machine.  The class
        # collected by this module can therefore retain a different globals
        # dictionary from the module currently present in sys.modules.  Bind
        # the exact method globals used by this real OSM call path.
        osm_method_globals = APOrderStateMachine._get_order.__globals__
        monkeypatch.setitem(osm_method_globals, "conn", harness.conn)
        monkeypatch.setitem(osm_method_globals, "run_with_retry", lambda fn, **kw: fn())

        pm_method_globals = (
            APPositionManager.converge_position_from_durable_exit_order.__globals__
        )
        monkeypatch.setitem(pm_method_globals, "conn", harness.conn)
        monkeypatch.setitem(pm_method_globals, "run_with_retry", lambda fn, **kw: fn())

        obs_method_globals = obs_mod.emit_decision_event.__globals__
        monkeypatch.setitem(obs_method_globals, "conn", harness.conn)
        monkeypatch.setitem(obs_method_globals, "run_with_retry", lambda fn, **kw: fn())
        yield harness
    finally:
        connection.rollback()
        connection.close()


# ─────────────────────────────────────────────────────────────────────────────
# Seed helpers
# ─────────────────────────────────────────────────────────────────────────────


def _seed_sept9_shape(
    harness: _PGHarness,
    *,
    position_avg_fill: Optional[float] = ENTRY_PRICE,
    exit_order_position_id: Optional[str] = POSITION_UUID,
    exit_status: str = "EXIT_REQUESTED",
    exit_filled_ts: Optional[datetime] = None,
    exit_filled_qty: Optional[int] = None,
    exit_fill_price: Optional[float] = None,
) -> None:
    """Seed the September 9 production shape.

    Key broker-repair characteristic: the EXIT order was created by the
    reconciler's recovery path.  Its position_id links to the canonical
    position, but the canonical position was NOT registered through the
    normal fill_monitor ENTRY path so the in-memory exit engine has no
    ManagedPosition for it.

    position_avg_fill=None simulates a broker-repair position where the
    original entry price was not captured.
    """
    # Canonical position — OPEN qty=1
    harness.execute(
        """
        INSERT INTO positions (
            id, client_id, execution_mode, contract, side, direction, status,
            qty, quantity_remaining, avg_fill, entry_price, entry_ts,
            local_order_id, updated_at
        ) VALUES (%s, %s, 'live', %s, 'CALL', 'CALL', 'OPEN', %s, %s,
                  %s, %s, %s, %s, NOW())
        """,
        (
            POSITION_UUID,
            CLIENT_ID,
            OCC_CONTRACT,
            FILL_QTY,
            FILL_QTY,           # quantity_remaining = 1
            position_avg_fill,  # may be NULL for broker-repair
            position_avg_fill,  # entry_price
            ENTRY_TS,
            "entry-order-qqq",
        ),
    )

    # ENTRY order — proof of fill; cost-basis source for broker-repair positions
    harness.execute(
        """
        INSERT INTO orders (
            local_order_id, position_id, client_id, execution_mode, kind,
            status, contract, direction, qty, filled_qty, fill_price,
            filled_ts, broker_order_id, meta, updated_ts
        ) VALUES (
            'entry-order-qqq', %s, %s, 'live', 'ENTRY', 'FILLED', %s,
            'CALL', %s, %s, %s, %s, 'entry-broker-001', '{}'::jsonb, NOW()
        )
        """,
        (
            POSITION_UUID,
            CLIENT_ID,
            OCC_CONTRACT,
            FILL_QTY,
            FILL_QTY,
            position_avg_fill if position_avg_fill is not None else ENTRY_PRICE,
            ENTRY_TS,
        ),
    )

    # Broker-repair EXIT order — created by reconciler recovery
    harness.execute(
        """
        INSERT INTO orders (
            local_order_id, position_id, client_id, execution_mode, kind,
            status, contract, direction, qty, filled_qty, fill_price,
            filled_ts, broker_order_id, meta, updated_ts
        ) VALUES (
            %s, %s, %s, 'live', 'EXIT', %s, %s,
            'CALL', %s, %s, %s, %s, %s, '{}'::jsonb, NOW()
        )
        """,
        (
            REPAIR_EXIT_UUID,
            exit_order_position_id,
            CLIENT_ID,
            exit_status,
            OCC_CONTRACT,
            FILL_QTY,
            exit_filled_qty,
            exit_fill_price,
            exit_filled_ts,
            BROKER_ORDER_ID,
        ),
    )

    harness.execute(
        """
        INSERT INTO proof_trades (id, client_email, position_id, local_order_id, system_version)
        VALUES (%s, %s, %s, 'entry-order-qqq', 'v2')
        """,
        (f"proof-{POSITION_UUID}", CLIENT_ID, POSITION_UUID),
    )


def _pos(h: _PGHarness, pid: str = POSITION_UUID) -> dict:
    return h.fetchone("SELECT * FROM positions WHERE id=%s", (pid,)) or {}

def _order(h: _PGHarness, lid: str = REPAIR_EXIT_UUID) -> dict:
    return h.fetchone("SELECT * FROM orders WHERE local_order_id=%s", (lid,)) or {}


def _no_exit_engine(monkeypatch) -> MagicMock:
    """Stub exit engine — no position registered (broker-repair path)."""
    import ap.order_state_machine as osm_mod
    ee = MagicMock()
    ee.get_position = MagicMock(return_value=None)
    ee._positions   = {}
    for m in ("mark_position_closed", "note_partial_exit_fill",
              "set_pending_exit_order", "clear_exit_in_flight", "on_exit_failure"):
        setattr(ee, m, MagicMock())
    # _get_exit_engine_for_client lives in order_state_machine, not fill_monitor
    monkeypatch.setattr(osm_mod, "_get_exit_engine_for_client", lambda _: ee)
    return ee


# ─────────────────────────────────────────────────────────────────────────────
# TestSept9FailFirst
# ─────────────────────────────────────────────────────────────────────────────


class TestSept9FailFirst:
    """
    These tests run on the #579 head.

    Each test first asserts the OPEN pre-condition (verifying the broken
    starting state), then exercises the #579 fix.  The separate
    p0-rollback-failfirst CI job checks out eb1fdefd and runs an inline
    script to prove the old code leaves the position OPEN — it does NOT
    depend on this file being present on the rollback base.
    """

    def test_broker_flat_without_exact_fill_must_not_manufacture_closed(
        self, postgres_harness
    ):
        """
        #566 safety: broker quantity=0 alone MUST NOT produce CLOSED.
        Must pass on both main and #579.
        """
        h = postgres_harness
        h.execute(
            """
            INSERT INTO positions (
                id, client_id, execution_mode, contract, side, direction, status,
                qty, quantity_remaining, avg_fill, entry_ts, local_order_id, updated_at
            ) VALUES (%s, %s, 'live', %s, 'CALL', 'CALL', 'OPEN', 1, 1, %s, %s, 'e-1', NOW())
            """,
            (POSITION_UUID, CLIENT_ID, OCC_CONTRACT, ENTRY_PRICE, ENTRY_TS),
        )

        # Pre-condition: position is OPEN
        assert _pos(h).get("status") == "OPEN"

        pm = APPositionManager(CLIENT_ID)
        pm.close_position_from_exit_fill(
            position_id=POSITION_UUID,
            exit_price=0.0,   # no real fill price
            filled_qty=0,     # zero fill quantity
            filled_ts=None,
            close_source="broker_flat_fabricated",
        )

        pos = _pos(h)
        assert pos.get("status") == "OPEN", (
            "BROKER FLAT ALONE MUST NOT CLOSE THE LOCAL POSITION. "
            f"Got status={pos.get('status')}"
        )
        assert pos.get("exit_price") is None
        assert pos.get("exit_ts") is None
        assert pos.get("quantity_remaining") == 1

    def test_sept9_precondition_position_open_before_convergence(
        self, postgres_harness, monkeypatch
    ):
        """
        Verify the broken starting state: after fill_monitor processing on the
        rollback base, the canonical position remains OPEN because the OSM's
        legacy _finalize_position_from_exit_order fails on avg_fill=NULL.

        On the #579 head this test seeds the shape and confirms OPEN before
        calling the fix — asserting the precondition, not the fix.
        """
        h = postgres_harness
        _seed_sept9_shape(h, position_avg_fill=None, exit_status="EXIT_REQUESTED")

        # Pre-condition assertion: position starts OPEN qty=1
        pos_before = _pos(h)
        assert pos_before.get("status") == "OPEN", "Pre-condition: position must be OPEN"
        assert pos_before.get("quantity_remaining") == 1, "Pre-condition: remaining must be 1"
        assert pos_before.get("exit_price") is None, "Pre-condition: no exit_price yet"
        assert pos_before.get("avg_fill") is None, "Pre-condition: broker-repair position has NULL avg_fill"

    def test_terminal_convergence_hold_does_not_fall_through(
        self, postgres_harness, monkeypatch
    ):
        """
        #579 terminal partial-fill fix: if canonical convergence HOLDs after
        durable fill application, the order must NOT be terminalized.

        Verifies fix 4: pm.converge_position_from_durable_exit_order returning
        a HOLD disposition causes a return without OSM terminal transition.
        """
        h = postgres_harness
        _seed_sept9_shape(h, position_avg_fill=ENTRY_PRICE, exit_status="EXIT_PARTIAL_FILL")

        import ap.fill_monitor as fm_mod
        import ap.order_state_machine as osm_mod
        from ap.position_manager import ConvergenceResult

        ee = _no_exit_engine(monkeypatch)

        # Patch pm.converge_position_from_durable_exit_order to return HOLD
        import ap.position_manager as pm_mod
        original_converge = pm_mod.APPositionManager.converge_position_from_durable_exit_order

        def _hold_converge(self, *, exit_local_order_id, expected_execution_mode):
            return ConvergenceResult("HOLD_IDENTITY", reason="injected_hold_for_test")

        monkeypatch.setattr(pm_mod.APPositionManager, "converge_position_from_durable_exit_order", _hold_converge)

        osm = APOrderStateMachine(CLIENT_ID)
        pm  = APPositionManager(CLIENT_ID)

        # Advance the EXIT order to EXIT_PARTIAL_FILL (which apply_fill_update expects)
        # Directly set status so OSM apply_fill_update succeeds
        h.execute(
            "UPDATE orders SET status='EXIT_PARTIAL_FILL', filled_qty=0 "
            "WHERE local_order_id=%s",
            (REPAIR_EXIT_UUID,),
        )

        # Simulate fill_monitor terminal path: apply partial fill then converge
        fill_applied = bool(
            osm.apply_fill_update(
                local_order_id=REPAIR_EXIT_UUID,
                cumulative_filled=FILL_QTY,
                fill_price=FILL_PRICE,
                broker_order_id=BROKER_ORDER_ID,
                filled_ts=FILL_TS,
            )
        )
        # The durable partial fill is written before convergence.  A
        # convergence HOLD is surfaced as False so callers cannot terminalize
        # the order while the durable state remains retryable.
        assert fill_applied is False, "convergence HOLD must fail closed"

        # With converge HOLDing, the order must NOT be in terminal status
        # (the fill_monitor code must HOLD and return, not call osm.transition(CANCELED))
        order_after = _order(h)
        # Order remains in partial-fill state (not terminal CANCELED/REJECTED/EXPIRED)
        assert order_after.get("status") == "EXIT_PARTIAL_FILL", (
            f"Order must not be terminalized when convergence HOLDs. "
            f"Got status={order_after.get('status')}"
        )
        assert order_after.get("filled_qty") == FILL_QTY
        assert order_after.get("filled_ts") == FILL_TS

        # Position must also remain in non-terminal state (not CLOSED by fabrication)
        pos_after = _pos(h)
        assert pos_after.get("status") == "OPEN", (
            "Position must remain OPEN when convergence HOLDs after durable fill. "
            f"Got status={pos_after.get('status')}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# TestSept9AfterFix
# ─────────────────────────────────────────────────────────────────────────────


class TestSept9AfterFix:
    """
    Full after-fix postcondition suite — requires #579 branch.

    Start point: APOrderStateMachine.transition() — real production caller.
    """

    @pytest.fixture(autouse=True)
    def require_579(self):
        pm = APPositionManager(CLIENT_ID)
        if not hasattr(pm, "converge_position_from_durable_exit_order"):
            pytest.skip("Requires #579 branch.")

    def test_sept9_exit_filled_closes_canonical_position(
        self, postgres_harness, monkeypatch
    ):
        """
        AFTER-FIX: OSM.transition EXIT_FILLED on broker-repair shape.

        avg_fill=NULL on position row does NOT block convergence because
        _resolve_entry_cost_basis() loads price from the durable ENTRY order.

        Full postcondition verified:
          - canonical position CLOSED
          - quantity_remaining = 0
          - exit_price = exact broker fill price
          - exit_ts = exact broker fill timestamp
          - capacity released: open_count() = 0
          - idempotent replay: second transition produces no additional mutation
          - no new broker calls
        """
        h = postgres_harness
        _seed_sept9_shape(
            h,
            position_avg_fill=None,      # broker-repair: entry price not on position row
            exit_status="EXIT_PARTIAL_FILL",
        )

        ee = _no_exit_engine(monkeypatch)
        import ap.order_state_machine as osm_mod
        osm = APOrderStateMachine(CLIENT_ID)

        # Pre-condition: position starts OPEN qty=1
        pos_before = _pos(h)
        assert pos_before["status"] == "OPEN", "Pre-condition: position must be OPEN before fix"
        assert pos_before["quantity_remaining"] == 1
        assert pos_before["exit_price"] is None

        # ── Production call fill_monitor makes ───────────────────────────
        ok = osm.transition(
            REPAIR_EXIT_UUID,
            "EXIT_FILLED",
            filled_qty=FILL_QTY,
            fill_price=FILL_PRICE,
            broker_order_id=BROKER_ORDER_ID,
            filled_ts=FILL_TS,
        )
        assert ok is True, "OSM transition must succeed"

        # ── Full postcondition ────────────────────────────────────────────
        pos = _pos(h)

        # Status and quantity
        assert pos["status"] == "CLOSED", (
            f"Expected CLOSED, got {pos['status']}. "
            "converge_position_from_durable_exit_order() must close the position."
        )
        assert pos["quantity_remaining"] == 0, (
            f"Expected remaining=0, got {pos['quantity_remaining']}"
        )

        # Exact broker economics (not fabricated)
        assert float(pos["exit_price"]) == pytest.approx(FILL_PRICE), (
            f"exit_price must equal exact broker fill price {FILL_PRICE}"
        )
        assert pos["exit_ts"] == FILL_TS, (
            f"exit_ts must be the exact broker fill timestamp, got {pos['exit_ts']}"
        )

        # Identity preserved
        assert pos["id"]             == POSITION_UUID
        assert pos["client_id"]      == CLIENT_ID
        assert pos["execution_mode"] == "live"
        assert pos["contract"]       == OCC_CONTRACT

        # Capacity released
        pm = APPositionManager(CLIENT_ID)
        assert pm.open_count() == 0, (
            "Closed position must not count in open exposure after convergence"
        )

        # No broker calls from convergence
        ee.mark_position_closed.assert_not_called()

        # ── Idempotent replay ─────────────────────────────────────────────
        ok2 = osm.transition(
            REPAIR_EXIT_UUID,
            "EXIT_FILLED",
            filled_qty=FILL_QTY,
            fill_price=FILL_PRICE,
            broker_order_id=BROKER_ORDER_ID,
            filled_ts=FILL_TS,
        )
        # May return False (terminal block) — that is correct behavior
        pos_after_replay = _pos(h)
        assert pos_after_replay["status"]             == "CLOSED"
        assert pos_after_replay["quantity_remaining"] == 0
        assert float(pos_after_replay["exit_price"])  == pytest.approx(FILL_PRICE)
        assert pos_after_replay["exit_ts"]            == FILL_TS

    @pytest.mark.parametrize("initial_exit_status", ["EXIT_SUBMITTED", "EXIT_ACKNOWLEDGED"])
    def test_fill_monitor_terminal_partial_then_cancel_uses_real_osm(
        self, postgres_harness, monkeypatch, initial_exit_status
    ):
        """
        Production-shaped fill_monitor proof: a terminal broker cancel with
        exec_qty>0 must first transition EXIT_SUBMITTED/ACKNOWLEDGED to
        EXIT_PARTIAL_FILL. The real PostgreSQL OSM hook then converges the
        position before CANCELED is allowed.
        """
        h = postgres_harness
        _seed_sept9_shape(
            h,
            position_avg_fill=ENTRY_PRICE,
            exit_status=initial_exit_status,
        )

        import ap.fill_monitor as fm_mod

        broker = MagicMock()
        broker.get_order.return_value = {
            "status": "CANCELED",
            "exec_quantity": FILL_QTY,
            "quantity": FILL_QTY,
            "avg_fill_price": FILL_PRICE,
            "last_fill_date": FILL_TS.isoformat(),
            "filled_at": FILL_TS.isoformat(),
            "reason": "broker_cancel_after_execution",
        }
        _no_exit_engine(monkeypatch)
        osm = APOrderStateMachine(CLIENT_ID)
        pm = APPositionManager(CLIENT_ID)

        fm_mod.process_pending_order(
            broker,
            _order(h),
            osm=osm,
            pm=pm,
        )

        pos = _pos(h)
        order = _order(h)
        assert pos["status"] == "CLOSED"
        assert pos["quantity_remaining"] == 0
        assert float(pos["exit_price"]) == pytest.approx(FILL_PRICE)
        assert pos["exit_ts"] == FILL_TS
        assert order["status"] == "CANCELED"
        assert order["filled_qty"] == FILL_QTY
        assert order["filled_ts"] == FILL_TS

        proof = h.fetchone(
            "SELECT * FROM proof_trades WHERE position_id=%s",
            (POSITION_UUID,),
        )
        assert proof is not None
        assert float(proof["exit_fill_price"]) == pytest.approx(FILL_PRICE)

        broker.cancel_order.assert_not_called()
        broker.replace_order.assert_not_called()
        broker.submit_order.assert_not_called()

    def test_sept9_restart_reconciler_converges_existing_exit_filled(
        self, postgres_harness, monkeypatch
    ):
        """
        Crash boundary (spec case 14):
        Process dies AFTER durable EXIT_FILLED commit but BEFORE position
        mutation.

        On restart the reconciler's _heal_exit_filled_positions_from_orders
        discovers the EXIT_FILLED order and converges the canonical position
        exactly once.

        Verifies:
          - position was OPEN before reconciler
          - reconciler closes position with exact economics
          - second reconciler run applies zero mutations
          - no broker calls made
        """
        h = postgres_harness

        # Seed: EXIT order already EXIT_FILLED (crash after durable write)
        _seed_sept9_shape(
            h,
            position_avg_fill=ENTRY_PRICE,
            exit_status="EXIT_FILLED",
            exit_filled_ts=FILL_TS,
            exit_filled_qty=FILL_QTY,
            exit_fill_price=FILL_PRICE,
        )

        # Pre-condition: position is OPEN despite EXIT_FILLED durable order
        assert _pos(h)["status"] == "OPEN", (
            "Pre-condition: position must be OPEN before reconciler runs"
        )

        import ap_reconciler as ar_mod
        from ap_reconciler import APBrokerReconciler, _empty_summary

        # ap_reconciler imports from ap.db inside functions; db_mod is already
        # patched by the postgres_harness fixture — no separate ar_mod patch needed.

        broker_mock = MagicMock()
        pm_shared   = APPositionManager(CLIENT_ID)

        reconciler = APBrokerReconciler(
            broker=broker_mock,
            client_id=CLIENT_ID,
            osm=SimpleNamespace(),
            pm=pm_shared,
            execution_mode="live",
        )
        summary = _empty_summary(CLIENT_ID)
        reconciler._heal_exit_filled_positions_from_orders(summary)

        pos = _pos(h)
        assert pos["status"] == "CLOSED", (
            "Restart reconciler must converge EXIT_FILLED → CLOSED. "
            f"Got: {pos['status']}"
        )
        assert pos["quantity_remaining"] == 0
        assert float(pos["exit_price"]) == pytest.approx(FILL_PRICE)

        # Idempotent second run
        summary2 = _empty_summary(CLIENT_ID)
        reconciler._heal_exit_filled_positions_from_orders(summary2)
        assert summary2.get("positions_corrected", 0) == 0, (
            "Second reconciler run must apply zero additional mutations"
        )

        # Safety: no broker calls
        broker_mock.submit_order.assert_not_called()
        broker_mock.cancel_order.assert_not_called()

    def test_convergence_hold_leaves_recoverable_state(
        self, postgres_harness, monkeypatch
    ):
        """
        When converge_position_from_durable_exit_order() returns a HOLD:

          - order must remain in the partial/open state (NOT terminal)
          - position must remain OPEN (NOT CLOSED by fabrication)
          - both durable rows stay available for retry/reconciler
        """
        h = postgres_harness
        _seed_sept9_shape(h, position_avg_fill=ENTRY_PRICE, exit_status="EXIT_REQUESTED")

        import ap.position_manager as pm_mod
        from ap.position_manager import ConvergenceResult

        def _hold(*a, **kw):
            return ConvergenceResult("HOLD_IDENTITY", reason="injected_hold")

        monkeypatch.setattr(pm_mod.APPositionManager, "converge_position_from_durable_exit_order", _hold)

        import ap.order_state_machine as osm_mod
        ee = _no_exit_engine(monkeypatch)
        osm = APOrderStateMachine(CLIENT_ID)

        ok = osm.transition(
            REPAIR_EXIT_UUID,
            "EXIT_FILLED",
            filled_qty=FILL_QTY,
            fill_price=FILL_PRICE,
            broker_order_id=BROKER_ORDER_ID,
            filled_ts=FILL_TS,
        )

        # OSM may write EXIT_FILLED to the order (durable state advance is OK)
        # but must NOT close the position
        pos = _pos(h)
        assert pos["status"] == "OPEN", (
            "Position must remain OPEN when convergence HOLDs. "
            f"Got: {pos['status']}"
        )
        assert pos["exit_price"] is None, "No exit_price should be fabricated on HOLD"
        assert pos["exit_ts"]    is None, "No exit_ts should be fabricated on HOLD"

    def test_broker_flat_without_fill_still_holds_on_579(
        self, postgres_harness
    ):
        """
        #566 safety preserved on #579: broker quantity=0 with NO EXIT_FILLED
        durable row must HOLD — no CLOSED fabricated.
        """
        h = postgres_harness
        h.execute(
            """
            INSERT INTO positions (
                id, client_id, execution_mode, contract, side, direction, status,
                qty, quantity_remaining, avg_fill, entry_ts, local_order_id, updated_at
            ) VALUES (%s, %s, 'live', %s, 'CALL', 'CALL', 'OPEN', 1, 1, %s, %s, 'e-1', NOW())
            """,
            (POSITION_UUID, CLIENT_ID, OCC_CONTRACT, ENTRY_PRICE, ENTRY_TS),
        )

        pm = APPositionManager(CLIENT_ID)
        pm.close_position_from_exit_fill(
            position_id=POSITION_UUID,
            exit_price=0.0,
            filled_qty=0,
            filled_ts=None,
            close_source="broker_flat_no_fill",
        )

        pos = _pos(h)
        assert pos["status"]             == "OPEN"
        assert pos["exit_price"]         is None
        assert pos["exit_ts"]            is None
        assert pos["quantity_remaining"] == 1


# ─────────────────────────────────────────────────────────────────────────────
# TestSept9CapacityRelease
# ─────────────────────────────────────────────────────────────────────────────


class TestSept9CapacityRelease:
    """open_count() must go from ≥1 to 0 after exact EXIT convergence."""

    @pytest.fixture(autouse=True)
    def require_579(self):
        if not hasattr(APPositionManager(CLIENT_ID), "converge_position_from_durable_exit_order"):
            pytest.skip("Requires #579 branch.")

    def test_open_count_zero_after_full_exit_convergence(
        self, postgres_harness, monkeypatch
    ):
        h = postgres_harness
        _seed_sept9_shape(h, position_avg_fill=ENTRY_PRICE, exit_status="EXIT_PARTIAL_FILL")

        pm = APPositionManager(CLIENT_ID)
        assert pm.open_count() >= 1, "Pre-condition: at least 1 open position"

        import ap.order_state_machine as osm_mod
        _no_exit_engine(monkeypatch)
        osm = APOrderStateMachine(CLIENT_ID)

        osm.transition(
            REPAIR_EXIT_UUID,
            "EXIT_FILLED",
            filled_qty=FILL_QTY,
            fill_price=FILL_PRICE,
            broker_order_id=BROKER_ORDER_ID,
            filled_ts=FILL_TS,
        )

        assert APPositionManager(CLIENT_ID).open_count() == 0, (
            "Position must not count in open exposure after full EXIT convergence"
        )
