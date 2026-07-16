from __future__ import annotations

import inspect
import re

from ap.operator_queue_read_model import dashboard_queue_bucket, empty_queue_counts
import ap.operator_queue_read_model as read_model

_APP_SRC = open("app.py").read()


def test_operator_trade_queue_status_mapping_matches_dashboard_buckets():
    assert dashboard_queue_bucket("NEW") == "NEW"
    assert dashboard_queue_bucket("WATCHING") == "WATCHING"
    assert dashboard_queue_bucket("PENDING_TRIGGER") == "WATCHING"
    assert dashboard_queue_bucket("TRIGGERED") == "TRIGGERED"
    assert dashboard_queue_bucket("SUBMITTED") == "TRIGGERED"
    assert dashboard_queue_bucket("ACK") == "TRIGGERED"
    assert dashboard_queue_bucket("ACKNOWLEDGED") == "TRIGGERED"
    assert dashboard_queue_bucket("WORKING") == "TRIGGERED"
    assert dashboard_queue_bucket("REJECTED") == "REJECTED"
    assert dashboard_queue_bucket("EXPIRED") == "EXPIRED"


def test_operator_trade_queue_counts_match_console_cards():
    assert empty_queue_counts() == {
        "NEW": 0,
        "WATCHING": 0,
        "TRIGGERED": 0,
        "REJECTED": 0,
        "EXPIRED": 0,
    }


def test_trade_queue_query_does_not_use_missing_updated_ts_column():
    src = inspect.getsource(read_model)
    trade_queue_section = src.split('tq_sql = (', 1)[1].split('order_sql = (', 1)[0]
    assert "updated_ts" not in trade_queue_section
    assert "started_ts" in trade_queue_section
    assert "finished_ts" in trade_queue_section


def test_order_query_selects_limit_price_for_hydrated_rows():
    src = inspect.getsource(read_model)
    order_section = src.split('order_sql = (', 1)[1].split('def _fetch():', 1)[0]
    assert "limit_price" in order_section


def test_order_row_shows_real_hydrated_limit_price():
    row = {
        "local_order_id": "local-123",
        "client_id": "client@example.com",
        "status": "PENDING_TRIGGER",
        "symbol": "AAPL",
        "side": "CALL",
        "contract": "AAPL260717C00200000",
        "limit_price": 2.5,
        "score": 88.0,
        "signal_id": "sig-123",
        "created_ts": "2026-07-03T13:40:00Z",
        "updated_ts": "2026-07-03T13:41:00Z",
        "last_error": None,
        "meta": {
            "contract_selection_status": "HYDRATED_PRE_BREACH",
            "deferred_hydration": {
                "attempted": True,
                "success": True,
            },
        },
    }

    out = read_model._order_row(row)

    assert out["limit_price"] == 2.5
    assert out["display_limit_price"] == 2.5
    assert out["contract"] == "AAPL260717C00200000"


def test_order_sql_uses_direction_not_side():
    """orders table has `direction` column — not `side`. Selecting `side` raises
    a Postgres 42703 column-does-not-exist error and silently zeroes the entire
    trade queue dashboard. This test guards against regression."""
    src = inspect.getsource(read_model)
    order_section = src.split("order_sql = (", 1)[1].split("def _fetch():", 1)[0]
    # Must NOT contain bare `side` as a selected column
    assert ", side," not in order_section, (
        "order_sql selects `side` which does not exist on orders table — use `direction`"
    )
    assert "direction" in order_section, (
        "order_sql must select `direction` (the real orders column)"
    )


def test_order_row_reads_direction_not_side():
    """_order_row must read `direction` from the DB row (not `side`) and expose
    it as `side` in the dashboard payload."""
    src = inspect.getsource(read_model._order_row)
    # The row read must use r.get("direction"), not r.get("side") as primary
    assert 'r.get("direction")' in src, (
        "_order_row must read direction from the DB row as the primary field"
    )


def test_entry_queue_endpoint_is_registered():
    """GET /admin/operator/entry-queue must be declared in app.py."""
    assert '"/admin/operator/entry-queue"' in _APP_SRC, (
        "/admin/operator/entry-queue not registered in app.py — dashboard trade queue will return 0 rows"
    )
    assert "def admin_operator_entry_queue" in _APP_SRC, (
        "Handler function admin_operator_entry_queue not found in app.py"
    )


def test_order_row_direction_surfaces_as_side():
    """When the DB row has `direction`, _order_row must surface it as `side`
    in the dashboard payload so the frontend column renders correctly."""
    row = {
        "local_order_id": "local-456",
        "client_id": "client@example.com",
        "status": "PENDING_TRIGGER",
        "symbol": "TSLA",
        "direction": "CALL",   # real orders column
        "contract": "TSLA260718C00400000",
        "limit_price": 3.10,
        "score": 82.0,
        "signal_id": "sig-456",
        "created_ts": "2026-07-16T13:00:00Z",
        "updated_ts": "2026-07-16T13:01:00Z",
        "last_error": None,
        "meta": {},
    }
    out = read_model._order_row(row)
    assert out["side"] == "CALL", (
        f"Expected side=CALL from direction column, got {out['side']!r}"
    )
