"""PR #579 durable EXIT -> canonical position convergence regressions."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
import os
from types import SimpleNamespace
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

from ap_reconciler import APBrokerReconciler, _empty_summary
from ap.position_manager import APPositionManager, ConvergenceResult


CLIENT_ID = "jason-pr579@example.com"
CONTRACT = "QQQ260904C00719000"
ENTRY_TS = datetime(2026, 9, 4, 14, 42, 0, tzinfo=timezone.utc)
FILLED_TS = datetime(2026, 9, 4, 14, 42, 34, 680787, tzinfo=timezone.utc)


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

    def fetchone(self, sql: str, params: tuple = ()) -> dict | None:
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

    harness = _PostgresHarness(connection, extras.RealDictCursor, _ConnWrapper)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
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
                    -- PR #579 amendment: pending-exit ownership columns.
                    -- Missing here previously, which meant _field_text()
                    -- returned "" and the identity fence in
                    -- converge_position_from_durable_exit_order was
                    -- effectively bypassed by the whole test suite.
                    exit_in_flight BOOLEAN,
                    pending_exit_qty INTEGER,
                    pending_exit_local_order_id TEXT,
                    pending_exit_broker_order_id TEXT
                )
                """
            )
            cursor.execute(
                """
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
                """
            )
            cursor.execute(
                """
                CREATE TEMP TABLE proof_trades (
                    id TEXT PRIMARY KEY,
                    client_email TEXT NOT NULL,
                    position_id TEXT,
                    local_order_id TEXT,
                    system_version TEXT,
                    synthetic_entry BOOLEAN DEFAULT FALSE,
                    exit_option_price NUMERIC,
                    exit_fill_price NUMERIC,
                    option_pnl_pct NUMERIC,
                    win BOOLEAN,
                    exit_reason TEXT,
                    broker_reconciled BOOLEAN DEFAULT FALSE
                )
                """
            )
        connection.commit()

        # Use the real psycopg2 driver and project connection wrapper while
        # keeping the tables isolated to this test transaction/session.
        monkeypatch.setattr(db_mod, "conn", harness.conn)
        monkeypatch.setattr(db_mod, "run_with_retry", lambda fn, **kwargs: fn())
        monkeypatch.setattr(pm_mod, "conn", harness.conn)
        monkeypatch.setattr(pm_mod, "run_with_retry", lambda fn, **kwargs: fn())
        yield harness
    finally:
        connection.rollback()
        connection.close()


def _ids(label: str) -> tuple[str, str, str, str]:
    suffix = uuid4().hex
    return (
        f"pr579-{label}-position-{suffix}",
        f"pr579-{label}-entry-{suffix}",
        f"pr579-{label}-exit-{suffix}",
        f"pr579-{label}-broker-{suffix}",
    )


def _insert_trade(
    harness: _PostgresHarness,
    label: str,
    *,
    client_id: str = CLIENT_ID,
    mode: str | None = "live",
    contract: str = CONTRACT,
    position_status: str = "OPEN",
    qty: int = 1,
    remaining: int | None = 1,
    avg_fill: float = 1.48,
    exit_price: float | None = None,
    realized_pnl: float | None = None,
    realized_pnl_pct: float | None = None,
    exit_status: str = "EXIT_FILLED",
    filled_qty: int | None = 1,
    fill_price: float | None = 1.17,
    filled_ts=FILLED_TS,
    order_mode: str | None = "__inherit__",
    order_contract: str | None = "__inherit__",
    order_position_id: str | None = None,
    broker_order_id: str | None = None,
    meta: dict | None = None,
) -> tuple[str, str, str, str]:
    position_id, entry_id, exit_id, generated_broker_id = _ids(label)
    if order_mode == "__inherit__":
        order_mode = mode
    if order_contract == "__inherit__":
        order_contract = contract
    order_position_id = position_id if order_position_id is None else order_position_id
    broker_order_id = generated_broker_id if broker_order_id is None else broker_order_id
    harness.execute(
        """
        INSERT INTO positions (
            id, client_id, execution_mode, contract, side, direction, status,
            qty, quantity_remaining, avg_fill, exit_price, realized_pnl,
            realized_pnl_pct, entry_ts, local_order_id, updated_at
        ) VALUES (%s, %s, %s, %s, 'CALL', 'CALL', %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """,
        (
            position_id,
            client_id,
            mode,
            contract,
            position_status,
            qty,
            remaining,
            avg_fill,
            exit_price,
            realized_pnl,
            realized_pnl_pct,
            ENTRY_TS,
            entry_id,
        ),
    )
    harness.execute(
        """
        INSERT INTO orders (
            local_order_id, position_id, client_id, execution_mode, kind,
            status, contract, direction, qty, filled_qty, fill_price,
            filled_ts, broker_order_id, meta, updated_ts
        ) VALUES (%s, %s, %s, %s, 'ENTRY', 'FILLED', %s, 'CALL', %s, %s, %s, %s, %s, '{}'::jsonb, NOW())
        """,
        (
            entry_id,
            position_id,
            client_id,
            mode,
            contract,
            qty,
            qty,
            avg_fill,
            ENTRY_TS,
            f"entry-{generated_broker_id}",
        ),
    )
    harness.execute(
        """
        INSERT INTO orders (
            local_order_id, position_id, client_id, execution_mode, kind,
            status, contract, direction, qty, filled_qty, fill_price,
            filled_ts, broker_order_id, meta, updated_ts
        ) VALUES (%s, %s, %s, %s, 'EXIT', %s, %s, 'CALL', %s, %s, %s, %s, %s, %s::jsonb, NOW())
        """,
        (
            exit_id,
            order_position_id,
            client_id,
            order_mode,
            exit_status,
            order_contract,
            qty,
            filled_qty,
            fill_price,
            filled_ts,
            broker_order_id,
            json.dumps(meta or {}),
        ),
    )
    harness.execute(
        """
        INSERT INTO proof_trades (
            id, client_email, position_id, local_order_id, system_version,
            exit_option_price, option_pnl_pct, win
        ) VALUES (%s, %s, %s, %s, 'v2', 0.50, 0.0, FALSE)
        """,
        (f"proof-{position_id}", client_id, position_id, entry_id),
    )
    return position_id, entry_id, exit_id, broker_order_id


def _read_position(harness: _PostgresHarness, position_id: str) -> dict:
    return harness.fetchone("SELECT * FROM positions WHERE id=%s", (position_id,)) or {}


def _read_order(harness: _PostgresHarness, local_order_id: str) -> dict:
    return harness.fetchone("SELECT * FROM orders WHERE local_order_id=%s", (local_order_id,)) or {}


def _read_proof(harness: _PostgresHarness, position_id: str) -> dict:
    return harness.fetchone("SELECT * FROM proof_trades WHERE position_id=%s", (position_id,)) or {}


def _seed_broker_repair(
    harness: _PostgresHarness,
    label: str,
    *,
    entry_contract=CONTRACT,
    entry_broker_order_id="entry-authoritative-1",
    entry_filled_ts=ENTRY_TS,
    raw_entry_timestamp: bool = False,
):
    position_id, entry_id, exit_id, _ = _insert_trade(
        harness,
        label,
        avg_fill=None,
        exit_price=None,
        realized_pnl=None,
        realized_pnl_pct=None,
        exit_status="EXIT_FILLED",
        filled_qty=1,
        fill_price=1.17,
        filled_ts=FILLED_TS,
    )
    if raw_entry_timestamp:
        # PostgreSQL TIMESTAMPTZ normalizes a naive value before the driver
        # returns it.  Preserve the malformed raw transport value for this
        # parser negative without changing the production schema.
        harness.execute(
            "ALTER TABLE orders ALTER COLUMN filled_ts TYPE TEXT USING filled_ts::text"
        )
    harness.execute(
        """
        UPDATE orders
           SET contract=%s,
               broker_order_id=%s,
               filled_ts=%s,
               fill_price=%s
         WHERE local_order_id=%s
        """,
        (entry_contract, entry_broker_order_id, entry_filled_ts, 1.48, entry_id),
    )
    return position_id, entry_id, exit_id


def test_qqq_exact_exit_fill_converges_and_replays_idempotently(postgres_harness):
    harness = postgres_harness
    position_id, _, exit_id, broker_id = _insert_trade(
        harness,
        "qqq",
        exit_price=1.17,
        realized_pnl=-31.00,
        realized_pnl_pct=-20.9459,
    )
    pm = APPositionManager(CLIENT_ID)

    first = pm.converge_position_from_durable_exit_order(
        exit_local_order_id=exit_id,
        expected_execution_mode="live",
    )
    position_after_first = _read_position(harness, position_id)
    order_after_first = _read_order(harness, exit_id)
    proof_after_first = harness.fetchone(
        "SELECT * FROM proof_trades WHERE position_id=%s",
        (position_id,),
    )

    assert first.disposition == "APPLIED_FULL"
    assert first.applied_delta_qty == 1
    assert first.remaining_qty == 0
    assert first.terminal is True
    assert position_after_first["status"] == "CLOSED"
    assert position_after_first["quantity_remaining"] == 0
    assert float(position_after_first["exit_price"]) == pytest.approx(1.17)
    assert float(position_after_first["realized_pnl"]) == pytest.approx(-31.00)
    assert float(position_after_first["realized_pnl_pct"]) == pytest.approx(-20.9459, abs=0.0001)
    assert position_after_first["exit_ts"] == FILLED_TS
    assert order_after_first["status"] == "EXIT_FILLED"
    assert order_after_first["broker_order_id"] == broker_id
    watermark = order_after_first["meta"]["position_projection_v1"]
    assert watermark["applied_cumulative_qty"] == 1
    assert watermark["applied_cumulative_notional"] == pytest.approx(1.17)
    assert watermark["last_applied_filled_ts"] == FILLED_TS.isoformat()
    assert float(proof_after_first["exit_option_price"]) == pytest.approx(1.17)
    assert float(proof_after_first["option_pnl_pct"]) == pytest.approx(-20.9459, abs=0.0001)
    assert proof_after_first["broker_reconciled"] is True

    second = pm.converge_position_from_durable_exit_order(
        exit_local_order_id=exit_id,
        expected_execution_mode="live",
    )
    assert second.disposition == "ALREADY_APPLIED"
    assert second.applied_delta_qty == 0
    assert _read_position(harness, position_id) == position_after_first
    assert _read_order(harness, exit_id)["meta"] == order_after_first["meta"]
    assert harness.fetchone(
        "SELECT COUNT(*) AS n FROM proof_trades WHERE position_id=%s",
        (position_id,),
    )["n"] == 1
    assert APPositionManager(CLIENT_ID).open_count() == 0


def test_broker_repair_valid_entry_evidence_converges(postgres_harness):
    """Positive control: the fallback uses one exact durable ENTRY fill."""
    harness = postgres_harness
    position_id, _, exit_id = _seed_broker_repair(
        harness,
        "entry-valid",
        entry_filled_ts=FILLED_TS,  # equality is valid: ENTRY <= EXIT
    )

    result = APPositionManager(CLIENT_ID).converge_position_from_durable_exit_order(
        exit_local_order_id=exit_id,
        expected_execution_mode="live",
    )

    assert result.disposition == "APPLIED_FULL"
    position = _read_position(harness, position_id)
    assert position["status"] == "CLOSED"
    assert position["quantity_remaining"] == 0
    assert float(position["exit_price"]) == pytest.approx(1.17)
    assert float(position["realized_pnl"]) == pytest.approx(-31.00)
    assert _read_order(harness, exit_id)["meta"]["position_projection_v1"][
        "applied_cumulative_qty"
    ] == 1
    assert _read_proof(harness, position_id)["broker_reconciled"] is True


@pytest.mark.parametrize(
    ("case_id", "entry_contract", "entry_broker_order_id", "entry_filled_ts", "raw_entry_timestamp"),
    [
        ("entry-contract-missing", None, "entry-valid", ENTRY_TS, False),
        ("entry-contract-wrong", "SPY260904C00719000", "entry-valid", ENTRY_TS, False),
        ("entry-broker-zero", CONTRACT, 0, ENTRY_TS, False),
        ("entry-broker-dash", CONTRACT, "-", ENTRY_TS, False),
        ("entry-broker-unknown", CONTRACT, "unknown", ENTRY_TS, False),
        ("entry-broker-na", CONTRACT, "n/a", ENTRY_TS, False),
        ("entry-filled-ts-missing", CONTRACT, "entry-valid", None, False),
        ("entry-filled-ts-naive", CONTRACT, "entry-valid", "2026-09-04T15:00:00", True),
        (
            "entry-filled-ts-after-exit",
            CONTRACT,
            "entry-valid",
            datetime(2026, 9, 4, 14, 42, 34, 680788, tzinfo=timezone.utc),
            False,
        ),
    ],
)
def test_broker_repair_bad_entry_evidence_holds_without_any_mutation(
    postgres_harness,
    case_id,
    entry_contract,
    entry_broker_order_id,
    entry_filled_ts,
    raw_entry_timestamp,
):
    """Every resolver authority failure is a true zero-mutation HOLD."""
    harness = postgres_harness
    position_id, _, exit_id = _seed_broker_repair(
        harness,
        case_id,
        entry_contract=entry_contract,
        entry_broker_order_id=entry_broker_order_id,
        entry_filled_ts=entry_filled_ts,
        raw_entry_timestamp=raw_entry_timestamp,
    )
    position_before = _read_position(harness, position_id)
    order_before = _read_order(harness, exit_id)
    proof_before = _read_proof(harness, position_id)

    result = APPositionManager(CLIENT_ID).converge_position_from_durable_exit_order(
        exit_local_order_id=exit_id,
        expected_execution_mode="live",
    )

    assert result.disposition == "HOLD_ECONOMICS", (
        f"{case_id}: invalid ENTRY evidence must fail closed; "
        f"got {result.disposition=} {result.reason=}"
    )
    assert _read_position(harness, position_id) == position_before, (
        f"{case_id}: position mutated on resolver HOLD"
    )
    order_after = _read_order(harness, exit_id)
    assert order_after == order_before, f"{case_id}: EXIT order mutated on resolver HOLD"
    assert order_after["meta"] == {}, f"{case_id}: watermark was written on resolver HOLD"
    assert _read_proof(harness, position_id) == proof_before, (
        f"{case_id}: proof row mutated on resolver HOLD"
    )


def test_reconciler_discovers_populated_economics_blind_spot(postgres_harness):
    """Discovery is durable fill vs watermark, not NULL output columns."""
    harness = postgres_harness
    position_id, _, exit_id, _ = _insert_trade(
        harness,
        "blind-spot",
        position_status="OPEN",
        remaining=1,
        exit_price=1.17,
        realized_pnl=-31.00,
        realized_pnl_pct=-20.9459,
    )
    broker = MagicMock()
    reconciler = APBrokerReconciler(
        broker=broker,
        client_id=CLIENT_ID,
        osm=SimpleNamespace(),
        pm=SimpleNamespace(),
        execution_mode="live",
    )
    summary = _empty_summary(CLIENT_ID)
    reconciler._heal_exit_filled_positions_from_orders(summary)

    assert summary["positions_corrected"] == 1
    assert _read_position(harness, position_id)["status"] == "CLOSED"
    assert _read_order(harness, exit_id)["meta"]["position_projection_v1"]["applied_cumulative_qty"] == 1
    assert broker.submit_order.call_count == 0
    assert broker.cancel_order.call_count == 0

    second_summary = _empty_summary(CLIENT_ID)
    reconciler._heal_exit_filled_positions_from_orders(second_summary)
    assert second_summary["positions_corrected"] == 0


def test_paper_exact_exit_fill_uses_the_same_durable_converger(postgres_harness):
    harness = postgres_harness
    position_id, _, exit_id, _ = _insert_trade(
        harness,
        "paper",
        mode="paper",
        exit_price=0.50,
        realized_pnl=0.0,
        realized_pnl_pct=0.0,
    )

    result = APPositionManager(CLIENT_ID).converge_position_from_durable_exit_order(
        exit_local_order_id=exit_id,
        expected_execution_mode="paper",
    )

    assert result.disposition == "APPLIED_FULL"
    assert result.terminal is True
    assert _read_position(harness, position_id)["execution_mode"] == "paper"
    assert _read_position(harness, position_id)["quantity_remaining"] == 0


def test_partial_cumulative_exit_fill_applies_only_new_delta(postgres_harness):
    harness = postgres_harness
    position_id, _, exit_id, _ = _insert_trade(
        harness,
        "partial",
        qty=3,
        remaining=3,
        avg_fill=1.00,
        exit_status="EXIT_PARTIAL_FILL",
        filled_qty=1,
        fill_price=1.10,
        filled_ts=datetime(2026, 9, 4, 14, 43, tzinfo=timezone.utc),
    )
    pm = APPositionManager(CLIENT_ID)

    first = pm.converge_position_from_durable_exit_order(
        exit_local_order_id=exit_id,
        expected_execution_mode="live",
    )
    assert first.disposition == "APPLIED_PARTIAL"
    assert first.applied_delta_qty == 1
    first_position = _read_position(harness, position_id)
    assert first_position["quantity_remaining"] == 2
    assert first_position["contracts_exited"] == 1

    replay = pm.converge_position_from_durable_exit_order(
        exit_local_order_id=exit_id,
        expected_execution_mode="live",
    )
    assert replay.disposition == "ALREADY_APPLIED"
    assert replay.applied_delta_qty == 0
    assert float(_read_position(harness, position_id)["realized_pnl"]) == pytest.approx(10.00)

    harness.execute(
        "UPDATE orders SET filled_qty=2, fill_price=1.20, filled_ts=%s WHERE local_order_id=%s",
        (datetime(2026, 9, 4, 14, 44, tzinfo=timezone.utc), exit_id),
    )
    second = pm.converge_position_from_durable_exit_order(
        exit_local_order_id=exit_id,
        expected_execution_mode="live",
    )
    second_position = _read_position(harness, position_id)
    second_order = _read_order(harness, exit_id)
    assert second.disposition == "APPLIED_PARTIAL"
    assert second.applied_delta_qty == 1
    assert second_position["quantity_remaining"] == 1
    assert second_position["contracts_exited"] == 2
    assert float(second_position["exit_price"]) == pytest.approx(1.20)
    assert float(second_position["realized_pnl"]) == pytest.approx(40.00)
    assert float(second_position["realized_pnl_pct"]) == pytest.approx(20.0)
    assert second_order["meta"]["position_projection_v1"]["applied_cumulative_qty"] == 2
    assert second_order["meta"]["position_projection_v1"]["applied_cumulative_notional"] == pytest.approx(2.40)

    harness.execute(
        "UPDATE orders SET status='EXIT_FILLED', filled_qty=3, fill_price=1.25, filled_ts=%s WHERE local_order_id=%s",
        (datetime(2026, 9, 4, 14, 45, tzinfo=timezone.utc), exit_id),
    )
    final = pm.converge_position_from_durable_exit_order(
        exit_local_order_id=exit_id,
        expected_execution_mode="live",
    )
    final_position = _read_position(harness, position_id)
    assert final.disposition == "APPLIED_FULL"
    assert final.applied_delta_qty == 1
    assert final_position["status"] == "CLOSED"
    assert final_position["quantity_remaining"] == 0
    assert final_position["contracts_exited"] == 3
    assert float(final_position["exit_price"]) == pytest.approx(1.25)
    assert float(final_position["realized_pnl"]) == pytest.approx(75.00)
    assert float(final_position["realized_pnl_pct"]) == pytest.approx(25.0)


@pytest.mark.parametrize(
    ("case_id", "order_kwargs", "expected_disposition"),
    [
        ("missing-ts", {"filled_ts": None}, "HOLD_TIMESTAMP"),
        ("missing-mode", {"order_mode": None}, "HOLD_IDENTITY"),
        ("mode-mismatch", {"order_mode": "paper"}, "HOLD_IDENTITY"),
        ("bad-occ", {"order_contract": "QQQ-NOT-OCC"}, "HOLD_IDENTITY"),
        ("missing-broker", {"broker_order_id": ""}, "HOLD_IDENTITY"),
        ("zero-qty", {"filled_qty": 0}, "HOLD_ECONOMICS"),
        ("overfill", {"filled_qty": 2}, "HOLD_WATERMARK"),
        ("bad-price", {"fill_price": float("nan")}, "HOLD_ECONOMICS"),
    ],
)
def test_malformed_durable_exit_holds_without_position_mutation(
    postgres_harness,
    case_id,
    order_kwargs,
    expected_disposition,
):
    harness = postgres_harness
    position_id, _, exit_id, _ = _insert_trade(harness, case_id, **order_kwargs)
    before = _read_position(harness, position_id)
    result = APPositionManager(CLIENT_ID).converge_position_from_durable_exit_order(
        exit_local_order_id=exit_id,
        expected_execution_mode="live",
    )

    assert result.disposition == expected_disposition
    assert _read_position(harness, position_id) == before
    assert _read_order(harness, exit_id)["meta"] == {}


def test_mismatched_watermark_holds_without_new_delta(postgres_harness):
    harness = postgres_harness
    position_id, _, exit_id, broker_id = _insert_trade(harness, "watermark")
    bad_watermark = {
        "position_id": "another-position",
        "client_id": CLIENT_ID,
        "execution_mode": "live",
        "contract": CONTRACT,
        "broker_order_id": broker_id,
        "applied_cumulative_qty": 1,
        "applied_cumulative_notional": 1.17,
        "last_applied_filled_ts": FILLED_TS.isoformat(),
    }
    harness.execute(
        "UPDATE orders SET meta=%s::jsonb WHERE local_order_id=%s",
        (json.dumps({"position_projection_v1": bad_watermark}), exit_id),
    )
    before = _read_position(harness, position_id)
    result = APPositionManager(CLIENT_ID).converge_position_from_durable_exit_order(
        exit_local_order_id=exit_id,
        expected_execution_mode="live",
    )
    assert result.disposition == "HOLD_WATERMARK"
    assert _read_position(harness, position_id) == before


def test_legacy_partial_position_without_watermark_holds(postgres_harness):
    harness = postgres_harness
    position_id, _, exit_id, _ = _insert_trade(
        harness,
        "legacy-partial",
        qty=3,
        remaining=2,
        avg_fill=1.00,
        exit_status="EXIT_FILLED",
        filled_qty=1,
        fill_price=1.10,
    )
    before = _read_position(harness, position_id)

    result = APPositionManager(CLIENT_ID).converge_position_from_durable_exit_order(
        exit_local_order_id=exit_id,
        expected_execution_mode="live",
    )

    assert result.disposition == "HOLD_WATERMARK"
    assert _read_position(harness, position_id) == before


def test_osm_runs_durable_convergence_before_runtime_close_hook(monkeypatch):
    import ap.order_state_machine as osm_module
    from ap.order_state_machine import APOrderStateMachine, OrderStatus

    events = []

    class _Engine:
        def mark_position_closed(self, position_id, **kwargs):
            events.append(("mark", position_id, kwargs))

    result = ConvergenceResult(
        disposition="APPLIED_FULL",
        applied_delta_qty=1,
        cumulative_applied_qty=1,
        terminal=True,
        position_id="position-osm",
        broker_order_id="broker-durable",
        exit_price=1.30,
    )
    osm = APOrderStateMachine(CLIENT_ID)
    osm._converge_durable_exit_order = lambda local_id: (
        events.append(("converge", local_id)) or result
    )
    monkeypatch.setattr(
        osm_module,
        "_get_exit_engine_for_client",
        lambda client_id: _Engine(),
    )

    osm._handle_exit_engine_hooks(
        current={
            "kind": "EXIT",
            "position_id": "position-osm",
            "local_order_id": "exit-osm",
            "broker_order_id": "broker-caller-value",
            "qty": 1,
        },
        new_status=OrderStatus.EXIT_FILLED,
        position_id="position-osm",
        filled_qty=1,
        fill_price=9.99,
        broker_order_id="broker-caller-value",
        local_order_id="exit-osm",
    )

    assert events[0] == ("converge", "exit-osm")
    assert events[1][0:2] == ("mark", "position-osm")
    assert events[1][2]["qty_filled"] == 1
    assert events[1][2]["fill_price"] == pytest.approx(1.30)
    assert events[1][2]["broker_order_id"] == "broker-durable"


def test_osm_keeps_runtime_exit_owner_on_convergence_hold(monkeypatch):
    import ap.order_state_machine as osm_module
    from ap.order_state_machine import APOrderStateMachine, OrderStatus

    calls = []

    class _Engine:
        def mark_position_closed(self, *args, **kwargs):
            calls.append(("mark", args, kwargs))

        def note_partial_exit_fill(self, *args, **kwargs):
            calls.append(("partial", args, kwargs))

        def clear_exit_in_flight(self, *args, **kwargs):
            calls.append(("clear", args, kwargs))

    osm = APOrderStateMachine(CLIENT_ID)
    osm._converge_durable_exit_order = lambda local_id: ConvergenceResult(
        disposition="HOLD_TIMESTAMP",
        position_id="position-held",
        reason="missing_or_invalid_exact_fill_timestamp",
    )
    monkeypatch.setattr(
        osm_module,
        "_get_exit_engine_for_client",
        lambda client_id: _Engine(),
    )

    osm._handle_exit_engine_hooks(
        current={
            "kind": "EXIT",
            "position_id": "position-held",
            "local_order_id": "exit-held",
        },
        new_status=OrderStatus.EXIT_FILLED,
        position_id="position-held",
        local_order_id="exit-held",
    )

    assert calls == []


def test_osm_exit_filled_without_broker_timestamp_stays_unprojected(
    postgres_harness,
    monkeypatch,
):
    """PR #605: OSM fail-closes before durable EXIT fill mutation when
    timezone-aware broker filled_ts is missing. The row must remain
    nonterminal/pollable and the position must stay unprojected.
    """
    import ap.order_state_machine as osm_module
    from ap.order_state_machine import APOrderStateMachine, OrderStatus

    harness = postgres_harness
    position_id, _, exit_id, _ = _insert_trade(
        harness,
        "osm-missing-ts",
        exit_status="EXIT_PARTIAL_FILL",
        filled_ts=None,
    )
    monkeypatch.setattr(osm_module, "conn", harness.conn)
    monkeypatch.setattr(osm_module, "run_with_retry", lambda fn, **kwargs: fn())
    osm = APOrderStateMachine(CLIENT_ID)
    osm._emit_transition_event = MagicMock()
    # Isolate the timestamp persistence assertion from runtime hook behavior;
    # the prior tests exercise that handler's ordering directly.
    osm._handle_exit_engine_hooks = MagicMock()

    before_order = _read_order(harness, exit_id)
    before = _read_position(harness, position_id)
    assert osm.transition(
        exit_id,
        OrderStatus.EXIT_FILLED,
        broker_order_id=before_order["broker_order_id"],
        filled_qty=1,
        fill_price=1.17,
    ) is False
    after_order = _read_order(harness, exit_id)
    assert after_order["status"] == "EXIT_PARTIAL_FILL"
    assert after_order["filled_ts"] is None
    assert after_order["filled_qty"] == before_order["filled_qty"]
    event_kwargs = osm._emit_transition_event.call_args.kwargs
    assert event_kwargs["decision"] == "HOLD"
    assert event_kwargs["reason_code"] == "EXIT_FILL_TIMESTAMP_MISSING_OR_INVALID"
    result = APPositionManager(CLIENT_ID).converge_position_from_durable_exit_order(
        exit_local_order_id=exit_id,
        expected_execution_mode="live",
    )
    assert result.disposition == "HOLD_TIMESTAMP"
    assert _read_position(harness, position_id) == before


@pytest.mark.parametrize("position_status", ["PARTIAL", "ACTIVE"])
def test_valid_legacy_active_position_statuses_converge(
    postgres_harness,
    position_status,
):
    harness = postgres_harness
    position_id, _, exit_id, _ = _insert_trade(
        harness,
        f"legacy-{position_status.lower()}",
        position_status=position_status,
        qty=2,
        remaining=2,
        exit_status="EXIT_PARTIAL_FILL",
        filled_qty=1,
        fill_price=1.17,
        filled_ts=FILLED_TS,
    )

    result = APPositionManager(CLIENT_ID).converge_position_from_durable_exit_order(
        exit_local_order_id=exit_id,
        expected_execution_mode="live",
    )

    assert result.disposition == "APPLIED_PARTIAL"
    assert result.remaining_qty == 1
    position = _read_position(harness, position_id)
    assert position["quantity_remaining"] == 1
    assert position["status"] == "CLOSING"


def test_exit_timestamp_before_entry_holds_before_position_mutation(postgres_harness):
    harness = postgres_harness
    position_id, _, exit_id, _ = _insert_trade(
        harness,
        "reversed-timestamp",
        filled_ts=ENTRY_TS - timedelta(seconds=1),
    )
    before = _read_position(harness, position_id)

    result = APPositionManager(CLIENT_ID).converge_position_from_durable_exit_order(
        exit_local_order_id=exit_id,
        expected_execution_mode="live",
    )

    assert result.disposition == "HOLD_TIMESTAMP"
    assert result.reason == "exit_timestamp_before_entry"
    assert _read_position(harness, position_id) == before


def test_fill_monitor_carries_only_explicit_broker_fill_timestamp():
    from ap.fill_monitor import check_order_with_broker

    class _Broker:
        def __init__(self, raw):
            self.raw = raw

        def get_order(self, broker_order_id):
            return dict(self.raw)

    order = {
        "client_id": CLIENT_ID,
        "local_order_id": "exit-timestamp-transport",
        "broker_order_id": "broker-timestamp-transport",
        "kind": "EXIT",
    }
    raw = {
        "status": "FILLED",
        "exec_quantity": 1,
        "avg_fill_price": 1.17,
        "transaction_date": FILLED_TS.isoformat(),
    }
    result = check_order_with_broker(_Broker(raw), order)
    assert result["status"] == "EXIT_FILLED"
    assert result["filled_ts"] == FILLED_TS.isoformat()

    partial_raw = dict(raw)
    partial_raw["status"] = "PARTIALLY_FILLED"
    partial_result = check_order_with_broker(_Broker(partial_raw), order)
    assert partial_result["status"] == "EXIT_PARTIAL_FILL"
    assert partial_result["filled_ts"] is None


# =====================================================================
#  PR #579 amendment — P1 pending-exit identity fence
#
#  The original guard at converge_position_from_durable_exit_order used
#  AND between local-id and broker-id conflict checks, so a mismatch on
#  ONE side alone passed through. This class exercises both sides of
#  that split-identity failure — which the pre-existing suite could not
#  reach because the temp positions table lacked the pending-exit
#  columns. The fixture is now extended so these tests actually bind.
#
#  Required behavior:
#    - If a non-blank pending_exit_local_order_id disagrees with this
#      exit's local_order_id, HOLD.
#    - If a non-blank pending_exit_broker_order_id disagrees with this
#      exit's broker_order_id, HOLD.
#    - HOLD means zero position, proof, or runtime mutation.
# =====================================================================


def _set_pending_exit_owner(
    harness: _PostgresHarness,
    position_id: str,
    *,
    local_order_id: str | None,
    broker_order_id: str | None,
    qty: int | None = 1,
) -> None:
    """Stamp the pending-exit ownership fields on an already-inserted
    position. Simulates the durable state left behind when a prior EXIT
    was already routed and is waiting on broker confirmation.
    """
    harness.execute(
        """
        UPDATE positions
           SET exit_in_flight = TRUE,
               pending_exit_qty = %s,
               pending_exit_local_order_id = %s,
               pending_exit_broker_order_id = %s,
               updated_at = NOW()
         WHERE id = %s
        """,
        (qty, local_order_id, broker_order_id, position_id),
    )


def _make_pm(client_id: str = CLIENT_ID):
    """Fresh position manager pinned to the harness DB."""
    return APPositionManager(client_id=client_id)


def test_pending_exit_local_match_but_broker_conflict_holds_p1(postgres_harness):
    """
    Split identity: this exit's local_order_id MATCHES the durable
    pending_exit_local_order_id, but its broker_order_id DISAGREES
    with the durable pending_exit_broker_order_id. Under the pre-fix
    AND-guard this passes (only one field disagrees), producing an
    unauthorized position mutation. Post-fix it must HOLD.
    """
    harness = postgres_harness
    position_id, entry_id, exit_id, broker_id = _insert_trade(
        harness, "p1-local-match", exit_status="EXIT_FILLED", filled_qty=1,
        fill_price=1.17, filled_ts=FILLED_TS,
    )
    # Durable pending-exit owner: same local, DIFFERENT broker.
    _set_pending_exit_owner(
        harness, position_id,
        local_order_id=exit_id,                       # match
        broker_order_id="OTHER-BROKER-ID-CONFLICT",   # conflict
    )
    pos_before = _read_position(harness, position_id)
    order_before = _read_order(harness, exit_id)

    pm = _make_pm()
    result = pm.converge_position_from_durable_exit_order(
        exit_local_order_id=exit_id,
        expected_execution_mode="live",
    )

    # HOLD outcome
    assert result.disposition == "HOLD_IDENTITY", (
        f"broker-side identity conflict must HOLD; got disposition="
        f"{result.disposition!r} reason={result.reason!r}. This is the "
        f"P1 leak: AND-fence let a broker-id mismatch through."
    )
    # No position mutation
    pos_after = _read_position(harness, position_id)
    for key in ("status", "quantity_remaining", "exit_price",
                "realized_pnl", "contracts_exited", "exit_ts"):
        assert pos_after.get(key) == pos_before.get(key), (
            f"pending-exit-owner HOLD must not mutate position.{key}; "
            f"was {pos_before.get(key)!r} became {pos_after.get(key)!r}"
        )
    # No order mutation
    order_after = _read_order(harness, exit_id)
    assert order_after.get("status") == order_before.get("status")


def test_pending_exit_broker_match_but_local_conflict_holds_p1(postgres_harness):
    """
    Split identity: this exit's broker_order_id MATCHES the durable
    pending_exit_broker_order_id, but its local_order_id DISAGREES
    with the durable pending_exit_local_order_id. Mirror of the case
    above; pre-fix AND-guard also lets this through.
    """
    harness = postgres_harness
    position_id, entry_id, exit_id, broker_id = _insert_trade(
        harness, "p1-broker-match", exit_status="EXIT_FILLED", filled_qty=1,
        fill_price=1.17, filled_ts=FILLED_TS,
    )
    _set_pending_exit_owner(
        harness, position_id,
        local_order_id="OTHER-LOCAL-ID-CONFLICT",   # conflict
        broker_order_id=broker_id,                  # match
    )
    pos_before = _read_position(harness, position_id)

    pm = _make_pm()
    result = pm.converge_position_from_durable_exit_order(
        exit_local_order_id=exit_id,
        expected_execution_mode="live",
    )

    assert result.disposition == "HOLD_IDENTITY", (
        f"local-side identity conflict must HOLD; got disposition="
        f"{result.disposition!r} reason={result.reason!r}."
    )
    pos_after = _read_position(harness, position_id)
    for key in ("status", "quantity_remaining", "exit_price", "realized_pnl"):
        assert pos_after.get(key) == pos_before.get(key)


_CONVERGE_OK_DISPOSITIONS = {"APPLIED_FULL", "ALREADY_APPLIED"}


def test_pending_exit_owner_matches_both_ids_converges(postgres_harness):
    """Positive control: durable pending-exit owner matches both this
    exit's identifiers exactly. Convergence proceeds normally."""
    harness = postgres_harness
    position_id, entry_id, exit_id, broker_id = _insert_trade(
        harness, "p1-both-match", exit_status="EXIT_FILLED", filled_qty=1,
        fill_price=1.17, filled_ts=FILLED_TS,
    )
    _set_pending_exit_owner(
        harness, position_id,
        local_order_id=exit_id,
        broker_order_id=broker_id,
    )

    pm = _make_pm()
    result = pm.converge_position_from_durable_exit_order(
        exit_local_order_id=exit_id,
        expected_execution_mode="live",
    )

    assert result.disposition in _CONVERGE_OK_DISPOSITIONS, (
        f"exact-match pending-exit owner must not HOLD; got "
        f"disposition={result.disposition!r} reason={result.reason!r}"
    )


def test_no_pending_exit_owner_converges_normally(postgres_harness):
    """Positive control: when no durable pending-exit owner exists
    (fields NULL / blank), the identity fence must not fire — that's
    the normal path for the first exit against a position."""
    harness = postgres_harness
    position_id, entry_id, exit_id, broker_id = _insert_trade(
        harness, "p1-no-owner", exit_status="EXIT_FILLED", filled_qty=1,
        fill_price=1.17, filled_ts=FILLED_TS,
    )
    # Intentionally do NOT stamp pending-exit fields — leave them NULL.

    pm = _make_pm()
    result = pm.converge_position_from_durable_exit_order(
        exit_local_order_id=exit_id,
        expected_execution_mode="live",
    )

    assert result.disposition in _CONVERGE_OK_DISPOSITIONS, (
        f"no pending-exit owner should allow convergence; got "
        f"disposition={result.disposition!r} reason={result.reason!r}"
    )
