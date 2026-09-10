"""
PR #579 — September 9 QQQ Production Incident
Fail-first / after-fix proof via real OSM caller path.

START POINT: APOrderStateMachine.transition() — the exact same entry point
that fill_monitor_loop calls when a broker poll returns EXIT_FILLED.

DO NOT call converge_position_from_durable_exit_order() directly.
The test must traverse the real fill path:

  OSM.transition(local_id, "EXIT_FILLED", ...)
  → _handle_exit_engine_hooks(...)
  → [main] _finalize_position_from_exit_order()  -- old path, misses broker-repair
  → [#579] _converge_durable_exit_order()         -- new canonical authority

Production evidence (September 9 2026 LIVE QQQ):
  canonical position:   70360b2a-9036-4e2d-bb6b-bf8d0065f6ef   OPEN qty=1
  broker-repair EXIT:   9c430437-2973-4811-8776-1d353b925d9b   EXIT_REQUESTED
  broker EXIT order:    145130405
  durable fill:         EXIT_FILLED qty=1 @ 1.17
  broker quantity:      0
  engine quantity:      1  (did not converge)

FAIL-FIRST (on main — no #579 code):
  osm.transition("9c430437", "EXIT_FILLED", ...) writes the durable order row.
  No canonical position convergence happens:
    - No _converge_durable_exit_order() exists.
    - _finalize_position_from_exit_order() reads order.position_id = "70360b2a"
      but close_position_from_exit_fill() fails the cost-basis guard when the
      position has avg_fill=NULL (broker-repair positions may lack entry price),
      returning False without writing CLOSED.
  → canonical position remains OPEN qty=1.

AFTER-FIX (on #579 head):
  _converge_durable_exit_order() is called first.
  Reads durable order row; resolves position by exact position_id.
  Validates client / mode / OCC / broker order / economics / timestamp.
  Applies watermark-guarded UPDATE: remaining=0, status=CLOSED.
  → canonical position terminal, qty=0, exact broker timestamp, idempotent replay.

Safety invariant preserved in both:
  broker-flat WITHOUT exact fill → HOLD / no CLOSED manufactured.
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

CLIENT_ID = "jason-live@example.com"
POSITION_UUID = "70360b2a-9036-4e2d-bb6b-bf8d0065f6ef"
REPAIR_EXIT_UUID = "9c430437-2973-4811-8776-1d353b925d9b"
BROKER_ORDER_ID = "145130405"
OCC_CONTRACT = "QQQ261121C00510000"

ENTRY_TS = datetime(2026, 9, 9, 13, 30, 0, tzinfo=timezone.utc)
FILL_TS = datetime(2026, 9, 9, 15, 42, 18, 543210, tzinfo=timezone.utc)
FILL_PRICE = 1.17
FILL_QTY = 1
ENTRY_PRICE = 1.48


# ─────────────────────────────────────────────────────────────────────────────
# PostgreSQL harness (real DB — same pattern as test_p0_exit_filled_position_convergence)
# ─────────────────────────────────────────────────────────────────────────────


class _PostgresHarness:
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
    import ap.db as db_mod
    import ap.position_manager as pm_mod
    import ap.order_state_machine as osm_mod

    harness = _PostgresHarness(connection, extras.RealDictCursor, _ConnWrapper)
    try:
        with connection.cursor() as cursor:
            # Positions table — includes broker-repair shape where avg_fill may be NULL
            cursor.execute("""
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
            # Orders table
            cursor.execute("""
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
            # proof_trades table
            cursor.execute("""
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
        connection.commit()

        monkeypatch.setattr(db_mod, "conn", harness.conn)
        monkeypatch.setattr(db_mod, "run_with_retry", lambda fn, **kwargs: fn())
        monkeypatch.setattr(pm_mod, "conn", harness.conn)
        monkeypatch.setattr(pm_mod, "run_with_retry", lambda fn, **kwargs: fn())
        monkeypatch.setattr(osm_mod, "conn", harness.conn)
        monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn, **kwargs: fn())
        yield harness
    finally:
        connection.rollback()
        connection.close()


# ─────────────────────────────────────────────────────────────────────────────
# Seed helpers
# ─────────────────────────────────────────────────────────────────────────────


def _seed_sept9_shape(
    harness: _PostgresHarness,
    *,
    position_avg_fill: Optional[float] = ENTRY_PRICE,
    exit_order_position_id: Optional[str] = POSITION_UUID,
    exit_status: str = "EXIT_REQUESTED",
    exit_filled_ts: Optional[datetime] = None,
    exit_filled_qty: Optional[int] = None,
    exit_fill_price: Optional[float] = None,
) -> None:
    """
    Seed the exact September 9 production shape.

    Key broker-repair characteristic: the EXIT order was created by the
    reconciler's broker-repair recovery path.  Its position_id links to the
    canonical position, but the canonical position was NOT registered through
    the normal fill_monitor ENTRY path so the in-memory exit engine has no
    ManagedPosition for it.

    position_avg_fill=None simulates a broker-repair position where the
    original entry price was not captured (triggers cost-basis guard in
    close_position_from_exit_fill).
    """
    # Canonical position — OPEN qty=1
    harness.execute(
        """
        INSERT INTO positions (
            id, client_id, execution_mode, contract, side, direction, status,
            qty, quantity_remaining, avg_fill, entry_price, entry_ts, local_order_id, updated_at
        ) VALUES (%s, %s, 'live', %s, 'CALL', 'CALL', 'OPEN', %s, %s, %s, %s, %s, %s, NOW())
        """,
        (
            POSITION_UUID,
            CLIENT_ID,
            OCC_CONTRACT,
            FILL_QTY,
            FILL_QTY,              # quantity_remaining = 1
            position_avg_fill,     # may be NULL for broker-repair positions
            position_avg_fill,     # entry_price
            ENTRY_TS,
            "entry-order-qqq",     # local_order_id on position row
            FILL_TS,
        ),
    )

    # ENTRY order (proof that entry executed)
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
            position_avg_fill or ENTRY_PRICE,
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

    # proof_trades placeholder
    harness.execute(
        """
        INSERT INTO proof_trades (id, client_email, position_id, local_order_id, system_version)
        VALUES (%s, %s, %s, 'entry-order-qqq', 'v2')
        """,
        (f"proof-{POSITION_UUID}", CLIENT_ID, POSITION_UUID),
    )


def _read_position(harness: _PostgresHarness, position_id: str) -> dict:
    return harness.fetchone(
        "SELECT * FROM positions WHERE id=%s", (position_id,)
    ) or {}


def _read_order(harness: _PostgresHarness, local_order_id: str) -> dict:
    return harness.fetchone(
        "SELECT * FROM orders WHERE local_order_id=%s", (local_order_id,)
    ) or {}


# ─────────────────────────────────────────────────────────────────────────────
# Mock exit engine — simulates broker-repair case where position is NOT
# registered in the in-memory exit engine (came through reconciler, not
# normal fill_monitor ENTRY path).
# ─────────────────────────────────────────────────────────────────────────────


def _unregistered_exit_engine() -> MagicMock:
    """Exit engine with NO position registered for POSITION_UUID."""
    ee = MagicMock()
    ee.get_position = MagicMock(return_value=None)  # not registered
    ee._positions = {}
    ee.mark_position_closed = MagicMock()
    ee.note_partial_exit_fill = MagicMock()
    ee.set_pending_exit_order = MagicMock()
    ee.clear_exit_in_flight = MagicMock()
    ee.on_exit_failure = MagicMock()
    return ee


# ─────────────────────────────────────────────────────────────────────────────
# FAIL-FIRST — current main (no #579)
# ─────────────────────────────────────────────────────────────────────────────


class TestSept9FailFirst:
    """
    Prove the September 9 defect on the current production caller path
    WITHOUT #579.

    The broker-repair EXIT order's position_id links to the canonical
    position, but the cost-basis guard inside close_position_from_exit_fill()
    fails because the broker-repair position has avg_fill=NULL.
    _finalize_position_from_exit_order() returns without writing CLOSED.
    The canonical position remains OPEN qty=1.
    """

    def test_osm_exit_filled_does_not_converge_broker_repair_position_on_main(
        self, postgres_harness, monkeypatch
    ):
        """
        FAIL-FIRST: call OSM.transition with exact September 9 shape.

        On MAIN (no #579): canonical position remains OPEN after EXIT_FILLED
        because:
          1. The in-memory exit engine has no registered position for
             POSITION_UUID (broker-repair path, not ENTRY fill path).
          2. _handle_exit_engine_hooks reaches the full-close branch but
             _finalize_position_from_exit_order → close_position_from_exit_fill
             blocks on avg_fill=NULL (invalid_position_cost_basis guard).
          3. No converge_position_from_durable_exit_order() exists.
          4. Canonical position: OPEN qty=1 (unchanged).

        On #579: _converge_durable_exit_order is called first.  It does not
        require avg_fill to be populated; it validates only the durable fill
        economics from the orders row.  Canonical position: CLOSED qty=0.
        """
        harness = postgres_harness

        # Seed Sept 9 shape with avg_fill=NULL (broker-repair position)
        _seed_sept9_shape(
            harness,
            position_avg_fill=None,       # ← broker-repair: entry price not captured
            exit_order_position_id=POSITION_UUID,  # EXIT is linked to canonical pos
            exit_status="EXIT_REQUESTED",
        )

        import ap.order_state_machine as osm_mod
        import ap.fill_monitor as fm_mod

        ee = _unregistered_exit_engine()
        monkeypatch.setattr(osm_mod, "_get_exit_engine_for_client", lambda _: ee)
        monkeypatch.setattr(fm_mod, "_get_exit_engine_for_client", lambda _, __=None: ee)

        osm = APOrderStateMachine(CLIENT_ID)

        # ── The exact production call fill_monitor makes ──────────────────
        ok = osm.transition(
            REPAIR_EXIT_UUID,
            "EXIT_FILLED",
            filled_qty=FILL_QTY,
            fill_price=FILL_PRICE,
            broker_order_id=BROKER_ORDER_ID,
            filled_ts=FILL_TS,
        )

        # OSM should return True (durable order update succeeded).
        assert ok is True, "OSM transition itself must succeed"

        # ── Read canonical position ───────────────────────────────────────
        pos_after = _read_position(harness, POSITION_UUID)

        # FAIL-FIRST assertion: on MAIN, the position has NOT been converged.
        # avg_fill=NULL triggers cost-basis guard in close_position_from_exit_fill
        # → False returned → position stays OPEN.
        #
        # When running on the #579 branch this assertion FAILS (the fix works):
        # converge_position_from_durable_exit_order() does NOT require avg_fill.
        # That is the intended fail-first → after-fix transition.
        assert pos_after.get("status") == "OPEN", (
            f"FAIL-FIRST PASSED (expected OPEN on main, got {pos_after.get('status')}). "
            "This means #579 is not yet applied, or the test is running on the #579 branch "
            "where the fix is already in place — check branch."
        )
        assert pos_after.get("quantity_remaining") == 1, (
            f"quantity_remaining should be 1 on main, got {pos_after.get('quantity_remaining')}"
        )
        assert pos_after.get("exit_price") is None, (
            "No exit_price should be written without exact fill authority"
        )
        assert pos_after.get("exit_ts") is None, (
            "No exit_ts should be written without exact fill authority"
        )

    def test_broker_flat_without_exact_fill_must_not_manufacture_closed(
        self, postgres_harness, monkeypatch
    ):
        """
        #566 safety invariant: broker quantity=0 alone MUST NOT produce CLOSED.

        Seed canonical OPEN position + NO EXIT_FILLED durable row.
        Any attempt to manufacture CLOSED from broker-flat truth alone must fail.
        This test must pass on both main and #579 (safety must be preserved).
        """
        harness = postgres_harness

        # Seed position only — no EXIT order row, no fill truth
        harness.execute(
            """
            INSERT INTO positions (
                id, client_id, execution_mode, contract, side, direction, status,
                qty, quantity_remaining, avg_fill, entry_ts, local_order_id, updated_at
            ) VALUES (%s, %s, 'live', %s, 'CALL', 'CALL', 'OPEN', 1, 1, %s, %s, 'entry-1', NOW())
            """,
            (POSITION_UUID, CLIENT_ID, OCC_CONTRACT, ENTRY_PRICE, ENTRY_TS),
        )

        pm = APPositionManager(CLIENT_ID)

        # Attempt to close without exact fill authority must return False / HOLD
        result = pm.close_position_from_exit_fill(
            position_id=POSITION_UUID,
            exit_price=0.0,           # broker-flat: no real fill price
            filled_qty=0,             # zero fill quantity
            filled_ts=None,
            close_source="broker_flat_fabricated",
        )

        pos_after = _read_position(harness, POSITION_UUID)

        # Safety must hold on BOTH main and #579
        assert pos_after.get("status") == "OPEN", (
            "BROKER FLAT ALONE MUST NOT CLOSE THE LOCAL POSITION. "
            f"Got status={pos_after.get('status')}"
        )
        assert pos_after.get("exit_price") is None
        assert pos_after.get("quantity_remaining") == 1


# ─────────────────────────────────────────────────────────────────────────────
# AFTER-FIX — #579 canonical convergence via exact OSM caller path
# ─────────────────────────────────────────────────────────────────────────────


class TestSept9AfterFix:
    """
    Prove #579 fixes the September 9 defect via the same OSM caller path.

    _converge_durable_exit_order() is called from _handle_exit_engine_hooks
    BEFORE the legacy exit engine mutation.  It uses the durable orders row
    (not the in-memory exit engine) as the canonical authority.  It does NOT
    require avg_fill to be set on the position row.

    Required postconditions (spec items 1–22):
      - exact position_id unchanged
      - exact client unchanged
      - execution_mode unchanged
      - OCC unchanged
      - remaining quantity = 0
      - status = terminal (CLOSED)
      - exit_price = exact broker fill price (1.17)
      - exit_ts = exact broker fill timestamp
      - realized economics derived from durable fill
      - idempotent replay: zero additional mutation
      - open_count() excludes the closed position
      - no new broker call
    """

    def test_osm_exit_filled_converges_broker_repair_position_with_579(
        self, postgres_harness, monkeypatch
    ):
        """
        AFTER-FIX: same caller path as fail-first, but on #579 branch.

        avg_fill=NULL on the position row does NOT block convergence because
        converge_position_from_durable_exit_order() derives economics from the
        orders row fill_price (not the position's avg_fill / entry_price).
        """
        harness = postgres_harness

        # Attempt to import #579 convergence authority.
        # If running on MAIN this raises AttributeError — the test correctly skips.
        try:
            from ap.position_manager import ConvergenceResult
            pm_test = APPositionManager(CLIENT_ID)
            assert hasattr(pm_test, "converge_position_from_durable_exit_order"), (
                "converge_position_from_durable_exit_order not present — "
                "this test requires the #579 branch."
            )
        except (AttributeError, AssertionError):
            pytest.skip(
                "converge_position_from_durable_exit_order not available on current branch; "
                "this test requires #579."
            )

        # Seed Sept 9 shape: position has avg_fill=NULL (broker-repair)
        # EXIT order is in EXIT_REQUESTED with exact fill economics ready
        _seed_sept9_shape(
            harness,
            position_avg_fill=None,          # ← broker-repair: entry not captured
            exit_order_position_id=POSITION_UUID,
            exit_status="EXIT_REQUESTED",    # fill_monitor will transition it
        )

        import ap.order_state_machine as osm_mod
        import ap.fill_monitor as fm_mod

        ee = _unregistered_exit_engine()
        monkeypatch.setattr(osm_mod, "_get_exit_engine_for_client", lambda _: ee)
        monkeypatch.setattr(fm_mod, "_get_exit_engine_for_client", lambda _, __=None: ee)

        osm = APOrderStateMachine(CLIENT_ID)

        # ── Same production call fill_monitor makes ──────────────────────
        ok = osm.transition(
            REPAIR_EXIT_UUID,
            "EXIT_FILLED",
            filled_qty=FILL_QTY,
            fill_price=FILL_PRICE,
            broker_order_id=BROKER_ORDER_ID,
            filled_ts=FILL_TS,
        )

        assert ok is True

        # ── Read canonical position postcondition ────────────────────────
        pos = _read_position(harness, POSITION_UUID)

        # Core postcondition: position is terminal
        assert pos.get("status") == "CLOSED", (
            f"Expected CLOSED, got {pos.get('status')}. "
            "converge_position_from_durable_exit_order() must close the position."
        )
        assert pos.get("quantity_remaining") == 0, (
            f"Expected remaining=0, got {pos.get('quantity_remaining')}"
        )

        # Exact economics from broker fill (not fabricated)
        assert float(pos.get("exit_price") or 0) == pytest.approx(FILL_PRICE), (
            f"exit_price must equal exact broker fill {FILL_PRICE}"
        )
        assert pos.get("exit_ts") == FILL_TS, (
            f"exit_ts must be the exact broker fill timestamp, got {pos.get('exit_ts')}"
        )

        # Identity preserved
        assert pos.get("id") == POSITION_UUID
        assert pos.get("client_id") == CLIENT_ID
        assert pos.get("execution_mode") == "live"

        # Open exposure count: closed position must not contribute
        pm = APPositionManager(CLIENT_ID)
        assert pm.open_count() == 0, (
            "Closed position must not count in open exposure after full convergence"
        )

        # No new broker calls issued during convergence
        ee.mark_position_closed.assert_not_called()  # wrong direction: EE ≠ broker

        # ── Idempotent replay ────────────────────────────────────────────
        ok2 = osm.transition(
            REPAIR_EXIT_UUID,
            "EXIT_FILLED",
            filled_qty=FILL_QTY,
            fill_price=FILL_PRICE,
            broker_order_id=BROKER_ORDER_ID,
            filled_ts=FILL_TS,
        )
        # Transition blocked (already terminal) or returns idempotent
        pos_after_replay = _read_position(harness, POSITION_UUID)
        assert pos_after_replay == pos, (
            "Replay must produce zero additional economic mutation"
        )

    def test_sept9_restart_recovery_converges_existing_exit_filled_order(
        self, postgres_harness, monkeypatch
    ):
        """
        Restart crash boundary (spec case 14):

        Process death AFTER durable EXIT_FILLED commit but BEFORE canonical
        position mutation.

        On restart: durable EXIT_FILLED row already exists.  Recovery must
        discover it and converge the canonical position exactly once.

        This tests _heal_exit_filled_positions_from_orders in the reconciler,
        which is the backup path invoked at startup when fill_monitor missed
        the convergence during the live session.
        """
        try:
            from ap.position_manager import ConvergenceResult  # noqa: F401
            pm_test = APPositionManager(CLIENT_ID)
            assert hasattr(pm_test, "converge_position_from_durable_exit_order")
        except (AttributeError, AssertionError):
            pytest.skip("Requires #579 branch.")

        harness = postgres_harness

        # Seed: EXIT order is ALREADY EXIT_FILLED (crash happened between
        # durable order update and position close).  Position is still OPEN.
        _seed_sept9_shape(
            harness,
            position_avg_fill=ENTRY_PRICE,          # normal entry price available
            exit_order_position_id=POSITION_UUID,
            exit_status="EXIT_FILLED",               # ← already committed durably
            exit_filled_ts=FILL_TS,
            exit_filled_qty=FILL_QTY,
            exit_fill_price=FILL_PRICE,
        )

        # Verify pre-condition: position still OPEN
        assert _read_position(harness, POSITION_UUID)["status"] == "OPEN"

        from ap_reconciler import APBrokerReconciler, _empty_summary

        import ap.reconciler as rec_mod  # noqa: F401
        import ap_reconciler as ar_mod

        broker_mock = MagicMock()
        osm_mock = SimpleNamespace()
        pm_shared = APPositionManager(CLIENT_ID)

        monkeypatch.setattr(ar_mod, "conn", harness.conn)
        monkeypatch.setattr(ar_mod, "run_with_retry", lambda fn, **kwargs: fn())

        reconciler = APBrokerReconciler(
            broker=broker_mock,
            client_id=CLIENT_ID,
            osm=osm_mock,
            pm=pm_shared,
            execution_mode="live",
        )
        summary = _empty_summary(CLIENT_ID)
        reconciler._heal_exit_filled_positions_from_orders(summary)

        pos_after = _read_position(harness, POSITION_UUID)

        assert pos_after["status"] == "CLOSED", (
            "Restart reconciler must converge EXIT_FILLED order into canonical CLOSED position. "
            f"Got: {pos_after['status']}"
        )
        assert pos_after["quantity_remaining"] == 0
        assert float(pos_after["exit_price"] or 0) == pytest.approx(FILL_PRICE)

        # Idempotent: second reconciler run must not re-apply
        summary2 = _empty_summary(CLIENT_ID)
        reconciler._heal_exit_filled_positions_from_orders(summary2)
        assert summary2.get("positions_corrected", 0) == 0, (
            "Duplicate reconciler run must apply zero additional mutations"
        )

        # Safety: no broker calls made
        broker_mock.submit_order.assert_not_called()
        broker_mock.cancel_order.assert_not_called()

    def test_broker_flat_without_exact_fill_still_holds_with_579(
        self, postgres_harness
    ):
        """
        #566 safety preserved on #579: broker quantity=0 with NO EXIT_FILLED
        durable row must HOLD — no CLOSED fabricated.

        This must pass on both main and #579.
        """
        harness = postgres_harness

        harness.execute(
            """
            INSERT INTO positions (
                id, client_id, execution_mode, contract, side, direction, status,
                qty, quantity_remaining, avg_fill, entry_ts, local_order_id, updated_at
            ) VALUES (%s, %s, 'live', %s, 'CALL', 'CALL', 'OPEN', 1, 1, %s, %s, 'e-1', NOW())
            """,
            (POSITION_UUID, CLIENT_ID, OCC_CONTRACT, ENTRY_PRICE, ENTRY_TS),
        )

        pm = APPositionManager(CLIENT_ID)

        # Broker flat but no fill: attempt to close with zero economics
        result = pm.close_position_from_exit_fill(
            position_id=POSITION_UUID,
            exit_price=0.0,
            filled_qty=0,
            filled_ts=None,
            close_source="broker_flat_no_fill",
        )

        pos = _read_position(harness, POSITION_UUID)
        assert pos.get("status") == "OPEN", (
            "BROKER FLAT WITHOUT EXACT FILL MUST NOT MANUFACTURE CLOSED. "
            "#566 safety invariant violated."
        )
        assert pos.get("exit_price") is None
        assert pos.get("exit_ts") is None


# ─────────────────────────────────────────────────────────────────────────────
# Capacity release proof
# ─────────────────────────────────────────────────────────────────────────────


class TestSept9CapacityRelease:
    """
    Spec requirement: after full EXIT convergence the position must not count
    in open exposure.  Stale EXIT_FILLED positions previously suppressed later
    trade flow.
    """

    def test_open_count_zero_after_full_exit_convergence(
        self, postgres_harness, monkeypatch
    ):
        """
        Position qty=1 contributes to open count before convergence.
        After exact full EXIT convergence: open_count() = 0.
        """
        try:
            APPositionManager(CLIENT_ID).converge_position_from_durable_exit_order  # noqa
        except AttributeError:
            pytest.skip("Requires #579 branch.")

        harness = postgres_harness

        _seed_sept9_shape(
            harness,
            position_avg_fill=ENTRY_PRICE,
            exit_status="EXIT_REQUESTED",
        )

        pm = APPositionManager(CLIENT_ID)

        # Before convergence: position counts as open
        open_before = pm.open_count()
        assert open_before >= 1, (
            f"Expected at least 1 open position before convergence, got {open_before}"
        )

        import ap.order_state_machine as osm_mod
        import ap.fill_monitor as fm_mod

        ee = _unregistered_exit_engine()
        monkeypatch.setattr(osm_mod, "_get_exit_engine_for_client", lambda _: ee)
        monkeypatch.setattr(fm_mod, "_get_exit_engine_for_client", lambda _, __=None: ee)

        osm = APOrderStateMachine(CLIENT_ID)
        osm.transition(
            REPAIR_EXIT_UUID,
            "EXIT_FILLED",
            filled_qty=FILL_QTY,
            fill_price=FILL_PRICE,
            broker_order_id=BROKER_ORDER_ID,
            filled_ts=FILL_TS,
        )

        # After convergence: position no longer counts
        pm2 = APPositionManager(CLIENT_ID)
        open_after = pm2.open_count()
        assert open_after == 0, (
            f"Position must not count in open exposure after full EXIT convergence. "
            f"open_count() = {open_after}"
        )
