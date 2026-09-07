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


def _reconciler(broker) -> APBrokerReconciler:
    return APBrokerReconciler(
        broker=broker,
        client_id=CLIENT,
        osm=MagicMock(),
        pm=MagicMock(),
        execution_mode="live",
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
        }
        row.update(overrides)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO positions (
                    id, contract, option_symbol, underlying, ticker, qty,
                    quantity_remaining, avg_fill, entry_price, close_source,
                    client_id, status
                ) VALUES (
                    %(id)s, %(contract)s, %(option_symbol)s, %(underlying)s,
                    %(ticker)s, %(qty)s, %(quantity_remaining)s, %(avg_fill)s,
                    %(entry_price)s, %(close_source)s, %(client_id)s, %(status)s
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
        {"positions": {}},
        {"positions": {"status": "ok"}},
        {"positions": {"metadata": "x"}},
        {"positions": {"position": [], "error": "unavailable"}},
        {"positions": {"position": [], "errors": ["unavailable"]}},
        {"positions": {"position": [], "message": "unavailable"}},
        {"positions": {"position": [], "status": "error"}},
        {"positions": {"position": [None]}},
        {"positions": {"position": ["row"]}},
        {"positions": {"position": [{"quantity": 1}]}},
        {"positions": {"position": [{"symbol": "", "quantity": 1}]}},
        {"positions": {"position": [{"symbol": CONTRACT}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": -1}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": 1.5}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": True}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": False}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": float("nan")}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": float("inf")}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": float("-inf")}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": ""}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": "garbage"}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": 1, "qty": 2}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "short_quantity": 1}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": 1, "side": "short"}]}},
        {"positions": {"position": [{"symbol": CONTRACT, "quantity": 1, "cost_basis": "nan"}]}},
    ],
)
def test_strict_positions_reader_rejects_unusable_payloads(payload):
    with pytest.raises(ValueError, match="TRADIER_POSITIONS_PAYLOAD_MALFORMED"):
        _tradier(payload=payload).list_positions_strict()


def test_strict_positions_reader_accepts_only_documented_empty_and_valid_long():
    assert _tradier(payload={"positions": "null"}).list_positions_strict() == []
    assert _tradier(payload={"positions": {"position": []}}).list_positions_strict() == []

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
        {"positions": {"position": [], "error": "unavailable"}},
        {"positions": {"position": [None]}},
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


def test_postgres_valid_long_preserves_existing_restore_behavior(postgres_closed_row):
    insert, read, _executed_sql = postgres_closed_row
    insert(qty=10, quantity_remaining=2)
    broker = _tradier(
        payload={
            "positions": {
                "position": [{"symbol": CONTRACT, "quantity": 5}]
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


def test_postgres_authoritative_empty_preserves_existing_flatten_behavior(
    postgres_closed_row,
):
    insert, read, _executed_sql = postgres_closed_row
    insert()
    broker = _tradier(payload={"positions": {"position": []}})
    rec = _reconciler(broker)

    rec._repair_closed_positions_with_remaining_qty(_empty_summary(CLIENT))

    assert read() == {
        "status": "CLOSED",
        "quantity_remaining": 0,
        "close_source": "CLOSED_REPAIR",
    }
