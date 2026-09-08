"""P0 proof for strict broker truth at the CLOSED-position repair seam."""

from __future__ import annotations

from contextlib import contextmanager
import os
from unittest.mock import MagicMock

import pytest
import requests

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@127.0.0.1:5432/test")

from ap.brokers.tradier import TradierBroker, TradierConfig
from ap_reconciler import APBrokerReconciler, _empty_summary


CLIENT = "pr594@example.com"
ACCOUNT = "PR594-ACCOUNT"
CONTRACT = "AAPL260620C00155000"
PADDED_CONTRACT = "GS  260717C00465000"
PADDED_COMPACT_CONTRACT = "GS260717C00465000"


def _tradier(*, payload=None, error=None) -> TradierBroker:
    broker = TradierBroker(
        TradierConfig(
            base_url="https://sandbox.tradier.test",
            access_token="token",
            account_id=ACCOUNT,
        )
    )

    def _get(_path):
        if error is not None:
            raise error
        return payload

    broker._get = _get
    broker.session.post = MagicMock(name="post")
    broker.session.delete = MagicMock(name="delete")
    return broker


def _reconciler(broker, *, execution_mode="live") -> APBrokerReconciler:
    return APBrokerReconciler(
        broker=broker,
        client_id=CLIENT,
        osm=MagicMock(),
        pm=MagicMock(),
        execution_mode=execution_mode,
    )


def _assert_no_broker_mutations(broker) -> None:
    assert broker.session.post.call_count == 0
    assert broker.session.delete.call_count == 0


def _postgres_url() -> str:
    return (
        os.getenv("PR594_POSTGRES_TEST_URL")
        or os.getenv("INTELLIGENCE_POSTGRES_TEST_URL")
        or os.getenv("DATABASE_URL")
        or ""
    ).strip()


@pytest.fixture
def postgres_closed_row(monkeypatch):
    psycopg2 = pytest.importorskip("psycopg2")
    extras = pytest.importorskip("psycopg2.extras")
    url = _postgres_url()
    if not url:
        pytest.skip("PostgreSQL URL not configured")
    try:
        connection = psycopg2.connect(url)
    except Exception as exc:
        pytest.skip(f"PostgreSQL integration unavailable: {exc}")

    connection.autocommit = True
    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TEMP TABLE positions (
                id                  TEXT PRIMARY KEY,
                contract            TEXT,
                option_symbol       TEXT,
                underlying           TEXT,
                ticker              TEXT,
                qty                 INTEGER,
                quantity_remaining  INTEGER,
                avg_fill            NUMERIC,
                entry_price         NUMERIC,
                close_source        TEXT,
                client_id           TEXT,
                status              TEXT,
                execution_mode      TEXT,
                entry_ts            TIMESTAMPTZ,
                updated_at          TIMESTAMPTZ
            )
            """
        )

    executed_sql: list[str] = []

    class _TrackedCursor:
        def __init__(self, cursor):
            self._cursor = cursor

        def execute(self, sql, params=None):
            executed_sql.append(" ".join(str(sql).split()).upper())
            return self._cursor.execute(sql, params)

        def __getattr__(self, name):
            return getattr(self._cursor, name)

    @contextmanager
    def _conn():
        with connection.cursor(cursor_factory=extras.RealDictCursor) as cursor:
            yield _TrackedCursor(cursor)

    import ap.db as db_module

    monkeypatch.setattr(db_module, "conn", _conn)
    monkeypatch.setattr(db_module, "run_with_retry", lambda fn: fn())

    def insert(**overrides):
        row = {
            "id": "position-pr594",
            "contract": CONTRACT,
            "option_symbol": CONTRACT,
            "underlying": "AAPL",
            "ticker": "AAPL",
            "qty": 3,
            "quantity_remaining": 2,
            "avg_fill": 1.0,
            "entry_price": 1.0,
            "close_source": "LEGACY_CLOSE",
            "client_id": CLIENT,
            "status": "CLOSED",
            "execution_mode": "live",
        }
        row.update(overrides)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO positions (
                    id, contract, option_symbol, underlying, ticker, qty,
                    quantity_remaining, avg_fill, entry_price, close_source,
                    client_id, status, execution_mode
                ) VALUES (
                    %(id)s, %(contract)s, %(option_symbol)s, %(underlying)s,
                    %(ticker)s, %(qty)s, %(quantity_remaining)s, %(avg_fill)s,
                    %(entry_price)s, %(close_source)s, %(client_id)s, %(status)s,
                    %(execution_mode)s
                )
                """,
                row,
            )

    def read():
        with connection.cursor(cursor_factory=extras.RealDictCursor) as cursor:
            cursor.execute(
                "SELECT status, quantity_remaining, close_source "
                "FROM positions"
            )
            return dict(cursor.fetchone())

    yield insert, read, executed_sql
    connection.close()


def test_legacy_positions_reader_still_collapses_transport_failure():
    broker = _tradier(error=RuntimeError("positions down"))

    assert broker.list_positions() == []


def test_strict_positions_reader_propagates_transport_failure():
    broker = _tradier(error=RuntimeError("positions down"))

    with pytest.raises(RuntimeError, match="positions down"):
        broker.list_positions_strict()


@pytest.mark.parametrize(
    "payload",
    [
        None,
        "ok",
        [],
        {},
        {"error": "unavailable"},
        {"message": "unavailable"},
        {"status": "error", "positions": {"position": []}},
        {"status": "failed", "positions": {"position": []}},
        {"status": "failure", "positions": {"position": []}},
        {"status": "unavailable", "positions": {"position": []}},
        {"status": {"code": 500}, "positions": {"position": []}},
        {"status": "unknown", "positions": {"position": []}},
        {"positions": {}},
        {"positions": {"status": "ok"}},
        {"positions": {"metadata": "x"}},
        {"positions": {"position": [], "error": "unavailable"}},
        {"positions": {"position": [], "errors": ["unavailable"]}},
        {"positions": {"position": [], "message": "unavailable"}},
        {"positions": {"position": [], "status": "error"}},
        {"positions": {"position": [], "status": {"code": 500}}},
        {"positions": {"position": [], "status": "unknown"}},
        {"positions": {"position": [None]}},
        {"positions": {"position": ["row"]}},
        {"positions": {"position": [{"quantity": 1}]}},
        {"positions": {"position": [{"symbol": "", "quantity": 1}]}},
        {"positions": {"position": [{"symbol": CONTRACT}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": 0}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": 1.5}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": True}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": False}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": float("nan")}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": float("inf")}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": float("-inf")}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": ""}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": "garbage"}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": 1, "qty": 2}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": 1, "short_quantity": 2}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": -1, "side": "long"}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "side": "short", "position_type": "long"}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": 1, "cost_basis": "nan"}]}},
        {"positions": {"position": [
            {"symbol": CONTRACT, "quantity": 5},
            {"symbol": CONTRACT, "quantity": 5},
        ]}},
    ],
)
def test_strict_positions_reader_rejects_unusable_payloads(payload):
    with pytest.raises(ValueError, match="TRADIER_POSITIONS_PAYLOAD_MALFORMED"):
        _tradier(payload=payload).list_positions_strict()


def test_strict_positions_reader_accepts_only_documented_empty_and_valid_long():
    assert _tradier(payload={"positions": "null"}).list_positions_strict() == []
    assert _tradier(payload={"positions": {"position": []}}).list_positions_strict() == []
    assert _tradier(
        payload={"positions": {"status": "ok", "position": []}}
    ).list_positions_strict() == []

    rows = _tradier(
        payload={
            "positions": {
                "position": [
                    {
                        "symbol": CONTRACT,
                        "quantity": "1",
                        "cost_basis": "155.0",
                    }
                ]
            }
        }
    ).list_positions_strict()
    assert rows == [
        {
            "symbol": CONTRACT,
            "quantity": 1,
            "cost_basis": 155.0,
            "side": "CALL",
            "raw": {
                "symbol": CONTRACT,
                "quantity": "1",
                "cost_basis": "155.0",
            },
        }
    ]

    padded_rows = _tradier(
        payload={
            "positions": {
                "position": [{"symbol": PADDED_CONTRACT, "quantity": 1}]
            }
        }
    ).list_positions_strict()
    assert padded_rows[0]["symbol"] == PADDED_COMPACT_CONTRACT
    assert padded_rows[0]["raw"]["symbol"] == PADDED_CONTRACT


@pytest.mark.parametrize(
    "error",
    [
        RuntimeError("transport down"),
        requests.HTTPError("401 unauthorized"),
        requests.HTTPError("429 rate limited"),
    ],
)
def test_postgres_transport_failure_leaves_closed_row_unchanged(
    postgres_closed_row, error
):
    insert, read, executed_sql = postgres_closed_row
    insert()
    broker = _tradier(error=error)
    rec = _reconciler(broker)

    rec._repair_closed_positions_with_remaining_qty(_empty_summary(CLIENT))

    assert read() == {
        "status": "CLOSED",
        "quantity_remaining": 2,
        "close_source": "LEGACY_CLOSE",
    }
    assert not any(sql.startswith("UPDATE POSITIONS") for sql in executed_sql)
    assert all(sql.startswith("SELECT") for sql in executed_sql)
    _assert_no_broker_mutations(broker)


@pytest.mark.parametrize(
    "payload",
    [
        {"positions": {"status": "ok"}},
        {"positions": {"status": {"code": 500}, "position": []}},
        {"positions": {"status": "unknown", "position": []}},
        {"positions": {"position": [], "error": "unavailable"}},
        {"positions": {"position": [None]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": 0}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": -1}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": 1.5}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": True}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "short_quantity": 1}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": 1, "qty": 2}]}},
    ],
)
def test_postgres_malformed_truth_leaves_closed_row_unchanged(
    postgres_closed_row, payload
):
    insert, read, executed_sql = postgres_closed_row
    insert()
    broker = _tradier(payload=payload)
    rec = _reconciler(broker)

    rec._repair_closed_positions_with_remaining_qty(_empty_summary(CLIENT))

    assert read() == {
        "status": "CLOSED",
        "quantity_remaining": 2,
        "close_source": "LEGACY_CLOSE",
    }
    assert not any(sql.startswith("UPDATE POSITIONS") for sql in executed_sql)
    assert all(sql.startswith("SELECT") for sql in executed_sql)
    _assert_no_broker_mutations(broker)


@pytest.mark.parametrize("mode", ["live", "paper"])
def test_postgres_valid_long_preserves_existing_restore_behavior(
    postgres_closed_row, mode
):
    insert, read, _executed_sql = postgres_closed_row
    insert(qty=10, quantity_remaining=2, execution_mode=mode)
    broker = _tradier(
        payload={
            "positions": {
                "position": [{"symbol": CONTRACT, "quantity": 5}]
            }
        }
    )
    rec = _reconciler(broker, execution_mode=mode)
    rec._find_db_position_by_id = MagicMock(return_value={"id": "position-pr594"})
    rec._seed_exit_engine_from_position = MagicMock(return_value=True)

    rec._repair_closed_positions_with_remaining_qty(_empty_summary(CLIENT))

    assert read() == {
        "status": "PARTIAL",
        "quantity_remaining": 2,
        "close_source": "PARTIAL_CLOSE_REPAIR",
    }


@pytest.mark.parametrize(
    "broker_contract",
    [PADDED_CONTRACT, PADDED_COMPACT_CONTRACT],
    ids=["padded-broker", "compact-broker"],
)
def test_postgres_padded_db_occ_preserves_restore_behavior(
    postgres_closed_row, broker_contract
):
    insert, read, _executed_sql = postgres_closed_row
    insert(
        contract=PADDED_CONTRACT,
        option_symbol=PADDED_CONTRACT,
        underlying="GS",
        ticker="GS",
    )
    broker = _tradier(
        payload={
            "positions": {
                "position": [{"symbol": broker_contract, "quantity": 5}]
            }
        }
    )
    rec = _reconciler(broker)
    rec._find_db_position_by_id = MagicMock(return_value={"id": "position-pr594"})
    rec._seed_exit_engine_from_position = MagicMock(return_value=True)

    rec._repair_closed_positions_with_remaining_qty(_empty_summary(CLIENT))

    assert read() == {
        "status": "PARTIAL",
        "quantity_remaining": 2,
        "close_source": "PARTIAL_CLOSE_REPAIR",
    }


def test_postgres_duplicate_padded_and_compact_rows_with_identical_qty_holds_safely(
    postgres_closed_row,
):
    """Amendment: an exact OCC identity appearing twice — even with agreeing
    quantities — is ambiguous broker truth, not a safe collapse. Matching
    quantities do not prove which row is real (duplicate transport
    representation, duplicate lots, duplicate account data, or a malformed
    payload are all indistinguishable from here)."""
    insert, read, executed_sql = postgres_closed_row
    insert(
        contract=PADDED_CONTRACT,
        option_symbol=PADDED_CONTRACT,
        underlying="GS",
        ticker="GS",
    )
    broker = _tradier(
        payload={
            "positions": {
                "position": [
                    {"symbol": PADDED_CONTRACT, "quantity": 5},
                    {"symbol": PADDED_COMPACT_CONTRACT, "quantity": 5},
                ]
            }
        }
    )
    rec = _reconciler(broker)

    rec._repair_closed_positions_with_remaining_qty(_empty_summary(CLIENT))

    assert read() == {
        "status": "CLOSED",
        "quantity_remaining": 2,
        "close_source": "LEGACY_CLOSE",
    }
    assert not any(sql.startswith("UPDATE POSITIONS") for sql in executed_sql)
    assert all(sql.startswith("SELECT") for sql in executed_sql)
    _assert_no_broker_mutations(broker)


def test_postgres_duplicate_padded_and_compact_rows_with_conflicting_qty_hold_safely(
    postgres_closed_row,
):
    insert, read, executed_sql = postgres_closed_row
    insert(
        contract=PADDED_CONTRACT,
        option_symbol=PADDED_CONTRACT,
        underlying="GS",
        ticker="GS",
    )
    broker = _tradier(
        payload={
            "positions": {
                "position": [
                    {"symbol": PADDED_CONTRACT, "quantity": 5},
                    {"symbol": PADDED_COMPACT_CONTRACT, "quantity": 3},
                ]
            }
        }
    )
    rec = _reconciler(broker)

    rec._repair_closed_positions_with_remaining_qty(_empty_summary(CLIENT))

    assert read() == {
        "status": "CLOSED",
        "quantity_remaining": 2,
        "close_source": "LEGACY_CLOSE",
    }
    assert not any(sql.startswith("UPDATE POSITIONS") for sql in executed_sql)
    assert all(sql.startswith("SELECT") for sql in executed_sql)
    _assert_no_broker_mutations(broker)


def test_postgres_same_raw_occ_symbol_twice_same_qty_holds_safely(
    postgres_closed_row,
):
    """Same exact OCC repeated twice with identical spelling and quantity —
    still ambiguous, still HOLD."""
    insert, read, executed_sql = postgres_closed_row
    insert()
    broker = _tradier(
        payload={
            "positions": {
                "position": [
                    {"symbol": CONTRACT, "quantity": 5},
                    {"symbol": CONTRACT, "quantity": 5},
                ]
            }
        }
    )
    rec = _reconciler(broker)

    rec._repair_closed_positions_with_remaining_qty(_empty_summary(CLIENT))

    assert read() == {
        "status": "CLOSED",
        "quantity_remaining": 2,
        "close_source": "LEGACY_CLOSE",
    }
    assert not any(sql.startswith("UPDATE POSITIONS") for sql in executed_sql)
    assert all(sql.startswith("SELECT") for sql in executed_sql)
    _assert_no_broker_mutations(broker)


@pytest.mark.parametrize("mode", ["live", "paper"])
def test_postgres_authoritative_empty_preserves_existing_flatten_behavior(
    postgres_closed_row, mode
):
    insert, read, _executed_sql = postgres_closed_row
    insert(execution_mode=mode)
    broker = _tradier(payload={"positions": {"position": []}})
    rec = _reconciler(broker, execution_mode=mode)

    rec._repair_closed_positions_with_remaining_qty(_empty_summary(CLIENT))

    assert read() == {
        "status": "CLOSED",
        "quantity_remaining": 0,
        "close_source": "CLOSED_REPAIR",
    }


@pytest.mark.parametrize(
    ("reconciler_mode", "row_mode"),
    [("live", "paper"), ("paper", "live")],
)
def test_postgres_execution_mode_mismatch_does_not_mutate_closed_row(
    postgres_closed_row, reconciler_mode, row_mode
):
    insert, read, executed_sql = postgres_closed_row
    insert(execution_mode=row_mode)
    broker = _tradier(
        payload={
            "positions": {
                "position": [{"symbol": CONTRACT, "quantity": 5}]
            }
        }
    )
    rec = _reconciler(broker, execution_mode=reconciler_mode)

    rec._repair_closed_positions_with_remaining_qty(_empty_summary(CLIENT))

    assert read() == {
        "status": "CLOSED",
        "quantity_remaining": 2,
        "close_source": "LEGACY_CLOSE",
    }
    assert not any(sql.startswith("UPDATE POSITIONS") for sql in executed_sql)
    assert all(sql.startswith("SELECT") for sql in executed_sql)
    _assert_no_broker_mutations(broker)


@pytest.mark.parametrize("row_mode", [None, ""])
def test_postgres_unproven_execution_mode_does_not_mutate_closed_row(
    postgres_closed_row, row_mode
):
    insert, read, executed_sql = postgres_closed_row
    insert(execution_mode=row_mode)
    broker = _tradier(payload={"positions": {"position": []}})
    rec = _reconciler(broker, execution_mode="live")

    rec._repair_closed_positions_with_remaining_qty(_empty_summary(CLIENT))

    assert read() == {
        "status": "CLOSED",
        "quantity_remaining": 2,
        "close_source": "LEGACY_CLOSE",
    }
    assert not any(sql.startswith("UPDATE POSITIONS") for sql in executed_sql)
    assert all(sql.startswith("SELECT") for sql in executed_sql)
    _assert_no_broker_mutations(broker)


# ──────────────────────────────────────────────────────────────────────────
# PR #594 AMENDMENT — Correction 1: signed broker quantities must not poison
# an otherwise valid snapshot.
# ──────────────────────────────────────────────────────────────────────────

def test_strict_positions_reader_returns_negative_quantity_for_short_row():
    rows = _tradier(
        payload={"positions": {"position": [{"symbol": CONTRACT, "quantity": -3}]}}
    ).list_positions_strict()
    assert rows[0]["quantity"] == -3


def test_strict_positions_reader_derives_negative_quantity_from_short_quantity_field():
    rows = _tradier(
        payload={"positions": {"position": [{"symbol": CONTRACT, "short_quantity": 3}]}}
    ).list_positions_strict()
    assert rows[0]["quantity"] == -3


def test_strict_positions_reader_derives_negative_quantity_from_direction_hint():
    rows = _tradier(
        payload={
            "positions": {
                "position": [{"symbol": CONTRACT, "quantity": 3, "side": "short"}]
            }
        }
    ).list_positions_strict()
    assert rows[0]["quantity"] == -3


def test_postgres_unrelated_negative_equity_does_not_poison_target_restore(
    postgres_closed_row,
):
    """Signed matrix #1: exact target long option + unrelated negative
    equity position — target repair proceeds normally."""
    insert, read, _executed_sql = postgres_closed_row
    insert()
    broker = _tradier(
        payload={
            "positions": {
                "position": [
                    {"symbol": CONTRACT, "quantity": 5},
                    {"symbol": "MSFT", "quantity": -100},
                ]
            }
        }
    )
    rec = _reconciler(broker)
    rec._find_db_position_by_id = MagicMock(return_value={"id": "position-pr594"})
    rec._seed_exit_engine_from_position = MagicMock(return_value=True)

    rec._repair_closed_positions_with_remaining_qty(_empty_summary(CLIENT))

    assert read() == {
        "status": "PARTIAL",
        "quantity_remaining": 2,
        "close_source": "PARTIAL_CLOSE_REPAIR",
    }
    _assert_no_broker_mutations(broker)


def test_postgres_unrelated_negative_option_does_not_poison_target_restore(
    postgres_closed_row,
):
    """Signed matrix #2: exact target long option + unrelated negative
    option position — target repair proceeds normally."""
    insert, read, _executed_sql = postgres_closed_row
    insert()
    broker = _tradier(
        payload={
            "positions": {
                "position": [
                    {"symbol": CONTRACT, "quantity": 5},
                    {"symbol": "MSFT260620P00300000", "quantity": -2},
                ]
            }
        }
    )
    rec = _reconciler(broker)
    rec._find_db_position_by_id = MagicMock(return_value={"id": "position-pr594"})
    rec._seed_exit_engine_from_position = MagicMock(return_value=True)

    rec._repair_closed_positions_with_remaining_qty(_empty_summary(CLIENT))

    assert read() == {
        "status": "PARTIAL",
        "quantity_remaining": 2,
        "close_source": "PARTIAL_CLOSE_REPAIR",
    }
    _assert_no_broker_mutations(broker)


def test_postgres_exact_target_negative_quantity_holds_without_mutation(
    postgres_closed_row,
):
    """Signed matrix #3: exact target OCC itself is negative (short) — must
    not be accepted as long RESTORE authority, and must not be silently
    treated as flat either. Zero mutation."""
    insert, read, executed_sql = postgres_closed_row
    insert()
    broker = _tradier(
        payload={"positions": {"position": [{"symbol": CONTRACT, "quantity": -2}]}}
    )
    rec = _reconciler(broker)

    rec._repair_closed_positions_with_remaining_qty(_empty_summary(CLIENT))

    assert read() == {
        "status": "CLOSED",
        "quantity_remaining": 2,
        "close_source": "LEGACY_CLOSE",
    }
    assert not any(sql.startswith("UPDATE POSITIONS") for sql in executed_sql)
    assert all(sql.startswith("SELECT") for sql in executed_sql)
    _assert_no_broker_mutations(broker)


# ──────────────────────────────────────────────────────────────────────────
# PR #594 2ND AMENDMENT — Correction 1: eliminate the raw-vs-normalized
# double-parser contradiction. A provider row shaped like
# quantity=3, side="short" normalizes to signed quantity=-3, but the
# ORIGINAL (pre-sign-adjustment) raw quantity of +3 must never be
# independently re-parsed and compared against the already-normalized -3 —
# doing so manufactures a false "contradiction" on every legitimate
# short-via-side-flag row and reintroduces the exact poisoning bug the
# first amendment was supposed to remove. These tests use quantity+side
# (not a bare negative number) specifically to exercise that raw path.
# ──────────────────────────────────────────────────────────────────────────

def test_postgres_unrelated_short_via_side_flag_equity_does_not_poison_restore(
    postgres_closed_row,
):
    """Required regression #1: target long OCC + unrelated equity
    (quantity=100, side="short") — unrelated short must not poison target
    RESTORE, and must not raise via the raw-vs-normalized double-parse."""
    insert, read, _executed_sql = postgres_closed_row
    insert()
    broker = _tradier(
        payload={
            "positions": {
                "position": [
                    {"symbol": CONTRACT, "quantity": 5},
                    {"symbol": "MSFT", "quantity": 100, "side": "short"},
                ]
            }
        }
    )
    rec = _reconciler(broker)
    rec._find_db_position_by_id = MagicMock(return_value={"id": "position-pr594"})
    rec._seed_exit_engine_from_position = MagicMock(return_value=True)

    rec._repair_closed_positions_with_remaining_qty(_empty_summary(CLIENT))

    assert read() == {
        "status": "PARTIAL",
        "quantity_remaining": 2,
        "close_source": "PARTIAL_CLOSE_REPAIR",
    }
    _assert_no_broker_mutations(broker)


def test_postgres_unrelated_short_via_side_flag_option_does_not_poison_restore(
    postgres_closed_row,
):
    """Required regression #2: target long OCC + unrelated option
    (quantity=2, side="short") — unrelated short option must not poison
    target RESTORE."""
    insert, read, _executed_sql = postgres_closed_row
    insert()
    broker = _tradier(
        payload={
            "positions": {
                "position": [
                    {"symbol": CONTRACT, "quantity": 5},
                    {
                        "symbol": "MSFT260620P00300000",
                        "quantity": 2,
                        "side": "short",
                    },
                ]
            }
        }
    )
    rec = _reconciler(broker)
    rec._find_db_position_by_id = MagicMock(return_value={"id": "position-pr594"})
    rec._seed_exit_engine_from_position = MagicMock(return_value=True)

    rec._repair_closed_positions_with_remaining_qty(_empty_summary(CLIENT))

    assert read() == {
        "status": "PARTIAL",
        "quantity_remaining": 2,
        "close_source": "PARTIAL_CLOSE_REPAIR",
    }
    _assert_no_broker_mutations(broker)


def test_postgres_exact_target_short_via_side_flag_holds_without_mutation(
    postgres_closed_row,
):
    """Required regression #3: exact target OCC itself is quantity=2,
    side="short" — HOLD with zero mutation, never RESTORE as long, never
    treated as absent/FLAT."""
    insert, read, executed_sql = postgres_closed_row
    insert()
    broker = _tradier(
        payload={
            "positions": {
                "position": [{"symbol": CONTRACT, "quantity": 2, "side": "short"}]
            }
        }
    )
    rec = _reconciler(broker)

    rec._repair_closed_positions_with_remaining_qty(_empty_summary(CLIENT))

    assert read() == {
        "status": "CLOSED",
        "quantity_remaining": 2,
        "close_source": "LEGACY_CLOSE",
    }
    assert not any(sql.startswith("UPDATE POSITIONS") for sql in executed_sql)
    assert all(sql.startswith("SELECT") for sql in executed_sql)
    _assert_no_broker_mutations(broker)


def test_closed_repair_position_qty_trusts_normalized_quantity_not_raw():
    """Direct unit proof of the fix: a strict-reader row shaped exactly like
    what TradierBroker.list_positions_strict() produces for a
    quantity=3/side="short" provider row (normalized quantity=-3, raw
    quantity=+3 preserved for diagnostics) must resolve to -3, not raise."""
    row = {
        "symbol": CONTRACT,
        "quantity": -3,
        "cost_basis": 100.0,
        "side": "PUT",
        "raw": {"symbol": CONTRACT, "quantity": 3, "side": "short"},
    }
    assert APBrokerReconciler._closed_repair_position_qty(row) == -3


def test_closed_repair_position_qty_requires_normalized_quantity_key():
    """Required: for test doubles/strict adapters that return a row without
    raw evidence, the reconciler still requires a valid integral non-zero
    signed quantity — read from the normalized "quantity" key directly."""
    assert APBrokerReconciler._closed_repair_position_qty({"quantity": -7}) == -7
    assert APBrokerReconciler._closed_repair_position_qty({"quantity": 4}) == 4

    for bad_row in (
        {},
        {"quantity": 0},
        {"quantity": None},
        {"quantity": True},
        {"quantity": 1.5},
        {"quantity": "garbage"},
        {"quantity": float("nan")},
    ):
        with pytest.raises(ValueError, match="closed_repair_broker_position_malformed"):
            APBrokerReconciler._closed_repair_position_qty(bad_row)


def _stub_broker(rows) -> MagicMock:
    """A bare strict-position-reader double that bypasses TradierBroker
    entirely — used to prove the reconciler enforces its own invariants
    (signed-quantity trust, duplicate-OCC ambiguity) independent of the
    Tradier-specific normalizer."""
    broker = MagicMock()
    broker.list_positions_strict = MagicMock(return_value=rows)
    broker.session = MagicMock()
    broker.session.post = MagicMock(name="post")
    broker.session.delete = MagicMock(name="delete")
    return broker


def test_postgres_duplicate_exact_occ_without_raw_evidence_holds_safely(
    postgres_closed_row,
):
    """Duplicate exact OCC ambiguity is enforced by the reconciler itself,
    not only by the Tradier-specific normalizer — proven here via bare rows
    with no "raw" field at all."""
    insert, read, executed_sql = postgres_closed_row
    insert()
    broker = _stub_broker(
        [
            {"symbol": CONTRACT, "quantity": 5},
            {"symbol": CONTRACT, "quantity": 5},
        ]
    )
    rec = _reconciler(broker)

    rec._repair_closed_positions_with_remaining_qty(_empty_summary(CLIENT))

    assert read() == {
        "status": "CLOSED",
        "quantity_remaining": 2,
        "close_source": "LEGACY_CLOSE",
    }
    assert not any(sql.startswith("UPDATE POSITIONS") for sql in executed_sql)
    assert all(sql.startswith("SELECT") for sql in executed_sql)
    _assert_no_broker_mutations(broker)


def test_postgres_generic_ticker_durable_target_identity_unproven_holds(
    postgres_closed_row,
):
    """Cross-asset #7: durable CLOSED target is a generic ticker only —
    target identity unproven, HOLD, zero mutation."""
    insert, read, executed_sql = postgres_closed_row
    insert(contract="AAPL", option_symbol="AAPL", underlying="AAPL", ticker="AAPL")
    broker = _tradier(
        payload={"positions": {"position": [{"symbol": "AAPL", "quantity": 100}]}}
    )
    rec = _reconciler(broker)

    rec._repair_closed_positions_with_remaining_qty(_empty_summary(CLIENT))

    assert read() == {
        "status": "CLOSED",
        "quantity_remaining": 2,
        "close_source": "LEGACY_CLOSE",
    }
    assert not any(sql.startswith("UPDATE POSITIONS") for sql in executed_sql)
    assert all(sql.startswith("SELECT") for sql in executed_sql)
    _assert_no_broker_mutations(broker)


def test_postgres_exact_occ_target_absent_flattens_despite_unrelated_equity(
    postgres_closed_row,
):
    """Cross-asset #6: durable target is exact OCC; broker contains only the
    same underlying's equity row — no RESTORE from equity, and absence
    inference is still valid because the snapshot is complete and
    unambiguous, so FLATTEN proceeds."""
    insert, read, _executed_sql = postgres_closed_row
    insert()
    broker = _tradier(
        payload={"positions": {"position": [{"symbol": "AAPL", "quantity": 100}]}}
    )
    rec = _reconciler(broker)

    rec._repair_closed_positions_with_remaining_qty(_empty_summary(CLIENT))

    assert read() == {
        "status": "CLOSED",
        "quantity_remaining": 0,
        "close_source": "CLOSED_REPAIR",
    }
    _assert_no_broker_mutations(broker)


def test_postgres_equity_and_option_same_root_only_option_is_target_authority(
    postgres_closed_row,
):
    """Cross-asset #11: broker holds equity AAPL and option AAPL OCC
    simultaneously — only the exact OCC is target authority."""
    insert, read, _executed_sql = postgres_closed_row
    insert()
    broker = _tradier(
        payload={
            "positions": {
                "position": [
                    {"symbol": "AAPL", "quantity": 100},
                    {"symbol": CONTRACT, "quantity": 5},
                ]
            }
        }
    )
    rec = _reconciler(broker)
    rec._find_db_position_by_id = MagicMock(return_value={"id": "position-pr594"})
    rec._seed_exit_engine_from_position = MagicMock(return_value=True)

    rec._repair_closed_positions_with_remaining_qty(_empty_summary(CLIENT))

    assert read() == {
        "status": "PARTIAL",
        "quantity_remaining": 2,
        "close_source": "PARTIAL_CLOSE_REPAIR",
    }
    _assert_no_broker_mutations(broker)
