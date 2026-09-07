"""PR #591: broker-position truth must fail closed at the adapter/reconciler seam."""

from __future__ import annotations

import os
from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@127.0.0.1:5432/test")

from ap.brokers.tradier import TradierBroker, TradierConfig
from ap_reconciler import (
    APBrokerReconciler,
    BROKER_POSITIONS_AMBIGUOUS,
    BROKER_POSITIONS_AVAILABLE_EMPTY,
    BROKER_POSITIONS_AVAILABLE_OPEN,
    BROKER_POSITIONS_MALFORMED,
    BROKER_POSITIONS_UNAVAILABLE,
    _empty_summary,
)


CLIENT = "pr591@example.com"
ACCOUNT = "PR591-ACCOUNT"
CONTRACT = "AAPL260620C00155000"


def _reconciler(broker=None):
    return APBrokerReconciler(
        broker=broker or object(),
        client_id=CLIENT,
        osm=MagicMock(),
        pm=MagicMock(),
        execution_mode="live",
    )


def _tradier(payload=None, error=None):
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
    return broker


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        ({"quantity": -1}, None),
        ({"quantity": 1.5}, None),
        ({"quantity": True}, None),
        ({"quantity": False}, None),
        ({"quantity": float("nan")}, None),
        ({"quantity": float("inf")}, None),
        ({"quantity": float("-inf")}, None),
        ({"quantity": " "}, None),
        ({"quantity": "garbage"}, None),
        ({"quantity": 0, "long_quantity": 1}, None),
        ({"long_quantity": 1, "short_quantity": 1}, None),
        ({"short_quantity": 1}, None),
        ({"quantity": 0}, 0),
        ({"quantity": "1"}, 1),
        ({"quantity": 1, "qty": 1, "long_quantity": 1}, 1),
    ],
)
def test_reconciler_quantity_authority_is_strict(row, expected):
    quantity, _reason = _reconciler()._broker_position_qty_result(row)
    assert quantity == expected


def test_reconciler_rejects_short_direction_even_with_positive_quantity():
    quantity, reason = _reconciler()._broker_position_qty_result(
        {"quantity": 1, "side": "short"}
    )
    assert quantity is None
    assert reason == "short_direction"


def test_tradier_non_strict_error_still_collapses_but_strict_seam_raises():
    broker = _tradier(error=RuntimeError("positions down"))

    # This is the historical fail-first behavior retained for legacy callers.
    assert broker.list_positions() == []
    with pytest.raises(RuntimeError, match="positions down"):
        broker.list_positions_strict()


@pytest.mark.parametrize(
    "payload",
    [
        {"positions": {}},
        {"positions": {"position": [{"symbol": CONTRACT}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": -1}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": 1.5}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": True}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": "garbage"}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": 1, "cost_basis": "nan"}]}},
    ],
)
def test_tradier_strict_rejects_malformed_position_payloads(payload):
    with pytest.raises(ValueError, match="TRADIER_POSITIONS_PAYLOAD_MALFORMED"):
        _tradier(payload=payload).list_positions_strict()


def test_tradier_strict_preserves_valid_signed_shape_without_lossy_casting():
    broker = _tradier(
        payload={
            "positions": {
                "position": [
                    {"symbol": CONTRACT, "quantity": "1", "cost_basis": "155.0"}
                ]
            }
        }
    )
    rows = broker.list_positions_strict()
    assert rows == [
        {
            "symbol": CONTRACT,
            "quantity": 1,
            "cost_basis": 155.0,
            "side": "CALL",
            "raw": {"symbol": CONTRACT, "quantity": "1", "cost_basis": "155.0"},
        }
    ]


def test_reconciler_snapshot_distinguishes_empty_from_adapter_failure():
    failed = _reconciler(_tradier(error=RuntimeError("transport down")))._safe_get_broker_positions()
    assert failed.state == BROKER_POSITIONS_UNAVAILABLE
    assert not failed.is_available

    empty = _reconciler(_tradier(payload={"positions": "null"}))._safe_get_broker_positions()
    assert empty.state == BROKER_POSITIONS_AVAILABLE_EMPTY
    assert empty.is_available
    assert empty == []

    open_snapshot = _reconciler(
        _tradier(
            payload={
                "positions": {
                    "position": [{"symbol": CONTRACT, "quantity": 1}]
                }
            }
        )
    )._safe_get_broker_positions()
    assert open_snapshot.state == BROKER_POSITIONS_AVAILABLE_OPEN
    assert open_snapshot.is_available


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("account_id", "WRONG-ACCOUNT"),
        ("client_id", "other@example.com"),
        ("execution_mode", "paper"),
    ],
)
def test_reconciler_holds_snapshot_with_identity_mismatch(field, value):
    row = {"symbol": CONTRACT, "quantity": 1, field: value}
    snapshot = _reconciler(
        _tradier(payload={"positions": {"position": [row]}})
    )._safe_get_broker_positions()

    assert snapshot.state == BROKER_POSITIONS_MALFORMED
    assert not snapshot.is_available


def test_reconciler_reclassifies_retry_after_prior_unavailable_snapshot():
    broker = _tradier(error=RuntimeError("positions down"))
    rec = _reconciler(broker)

    first = rec._safe_get_broker_positions()
    assert first.state == BROKER_POSITIONS_UNAVAILABLE

    broker._get = lambda _path: {"positions": {"position": []}}
    second = rec._safe_get_broker_positions()

    assert second.state == BROKER_POSITIONS_AVAILABLE_EMPTY
    assert second.is_available
    assert second == []


def test_unavailable_snapshot_holds_before_reconcile_mutation():
    rec = _reconciler(_tradier(error=RuntimeError("transport down")))
    rec._get_open_db_positions = lambda: pytest.fail(
        "unknown broker truth must stop before DB position reconciliation"
    )
    rec._import_broker_positions_missing_from_db = lambda **_kwargs: pytest.fail(
        "unknown broker truth must stop before broker-position import"
    )
    summary = _empty_summary(CLIENT)

    rec._reconcile_positions(summary)

    assert summary["errors"] == ["broker_positions_unavailable"]
    assert summary["positions_imported"] == 0


def test_reconciler_rejects_conflicting_aliases_and_duplicate_contracts():
    rec = _reconciler()
    conflicting = rec._coerce_broker_position_snapshot(
        [{"symbol": CONTRACT, "quantity": 0, "qty": 1}]
    )
    assert conflicting.state == BROKER_POSITIONS_MALFORMED
    assert not conflicting.is_available

    duplicate = rec._coerce_broker_position_snapshot(
        [
            {"symbol": CONTRACT, "quantity": 1},
            {"symbol": CONTRACT, "quantity": 1},
        ]
    )
    assert duplicate.state == BROKER_POSITIONS_AMBIGUOUS
    assert not duplicate.is_available


def _postgres_url():
    return (
        os.getenv("PR591_POSTGRES_TEST_URL")
        or os.getenv("INTELLIGENCE_POSTGRES_TEST_URL")
        or os.getenv("DATABASE_URL")
        or ""
    ).strip()


@pytest.fixture
def postgres_positions(monkeypatch):
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
                execution_mode     TEXT,
                entry_ts            TIMESTAMPTZ,
                updated_at          TIMESTAMPTZ
            )
            """
        )

    @contextmanager
    def _conn():
        with connection.cursor(cursor_factory=extras.RealDictCursor) as cursor:
            yield cursor

    import ap.db as db_module

    monkeypatch.setattr(db_module, "conn", _conn)
    monkeypatch.setattr(db_module, "run_with_retry", lambda fn: fn())

    def insert(**overrides):
        row = {
            "id": "position-pr591",
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
                "SELECT quantity_remaining, close_source, status FROM positions"
            )
            return dict(cursor.fetchone())

    yield insert, read
    connection.close()


def test_postgres_adapter_failure_leaves_closed_row_unchanged(postgres_positions):
    insert, read = postgres_positions
    insert()
    broker = _tradier(error=RuntimeError("Tradier transport unavailable"))
    # Prove the historical public adapter shape really collapses the error to
    # [], then exercise the strict production repair seam on the same broker.
    assert broker.list_positions() == []
    rec = _reconciler(broker)
    summary = _empty_summary(CLIENT)

    rec._repair_closed_positions_with_remaining_qty(summary)

    assert read() == {
        "quantity_remaining": 2,
        "close_source": "LEGACY_CLOSE",
        "status": "CLOSED",
    }
    assert "broker_positions_unavailable" in summary["errors"]
    assert summary["broker_positions_hidden_by_closed_status_count"] == 1


def test_postgres_authoritative_open_qty_one_restores_open_row(postgres_positions):
    insert, read = postgres_positions
    insert(qty=1, quantity_remaining=1)
    broker = _tradier(
        payload={
            "positions": {
                "position": [
                    {"symbol": CONTRACT, "quantity": 1, "cost_basis": "155.0"}
                ]
            }
        }
    )
    rec = _reconciler(broker)
    rec._seed_exit_engine_from_position = lambda _row: True

    rec._repair_closed_positions_with_remaining_qty(_empty_summary(CLIENT))

    assert read() == {
        "quantity_remaining": 1,
        "close_source": "PARTIAL_CLOSE_REPAIR",
        "status": "OPEN",
    }


def test_postgres_authoritative_qty_replaces_stale_remainder_without_undercount(
    postgres_positions,
):
    insert, read = postgres_positions
    insert(qty=10, quantity_remaining=2)
    broker = _tradier(
        payload={
            "positions": {
                "position": [
                    {"symbol": CONTRACT, "quantity": 5, "cost_basis": "155.0"}
                ]
            }
        }
    )
    rec = _reconciler(broker)
    rec._seed_exit_engine_from_position = lambda _row: True

    rec._repair_closed_positions_with_remaining_qty(_empty_summary(CLIENT))

    assert read() == {
        "quantity_remaining": 5,
        "close_source": "PARTIAL_CLOSE_REPAIR",
        "status": "PARTIAL",
    }


def test_postgres_retry_after_unavailable_snapshot_rechecks_authority(
    postgres_positions,
):
    insert, read = postgres_positions
    insert()
    broker = _tradier(error=RuntimeError("Tradier transport unavailable"))
    rec = _reconciler(broker)

    rec._repair_closed_positions_with_remaining_qty(_empty_summary(CLIENT))
    assert read()["status"] == "CLOSED"

    broker._get = lambda _path: {"positions": {"position": []}}
    rec._repair_closed_positions_with_remaining_qty(_empty_summary(CLIENT))

    assert read() == {
        "quantity_remaining": 0,
        "close_source": "CLOSED_REPAIR",
        "status": "CLOSED",
    }


def test_closed_repair_update_failure_is_fail_closed(monkeypatch):
    bad_row = {
        "id": "position-pr591",
        "contract": CONTRACT,
        "option_symbol": CONTRACT,
        "underlying": "AAPL",
        "ticker": "AAPL",
        "qty": 3,
        "quantity_remaining": 2,
        "client_id": CLIENT,
        "status": "CLOSED",
        "execution_mode": "live",
    }

    class _Cursor:
        rowcount = 0

        def __init__(self):
            self._rows = []

        def execute(self, sql, _params=None):
            if "SELECT" in " ".join(sql.split()).upper():
                self._rows = [bad_row]

        def fetchall(self):
            return self._rows

    cursor = _Cursor()

    @contextmanager
    def _conn():
        yield cursor

    import ap.db as db_module

    monkeypatch.setattr(db_module, "conn", _conn)
    monkeypatch.setattr(db_module, "run_with_retry", lambda fn: fn())
    rec = _reconciler(
        _tradier(payload={"positions": {"position": []}})
    )
    summary = _empty_summary(CLIENT)

    rec._repair_closed_positions_with_remaining_qty(summary)

    assert "closed_repair_update_failed" in summary["errors"]
    assert bad_row["quantity_remaining"] == 2
    assert bad_row["status"] == "CLOSED"


def test_postgres_authoritative_empty_snapshot_still_repairs_flat_row(postgres_positions):
    insert, read = postgres_positions
    insert()
    broker = _tradier(payload={"positions": {"position": []}})
    rec = _reconciler(broker)

    rec._repair_closed_positions_with_remaining_qty(_empty_summary(CLIENT))

    assert read() == {
        "quantity_remaining": 0,
        "close_source": "CLOSED_REPAIR",
        "status": "CLOSED",
    }
