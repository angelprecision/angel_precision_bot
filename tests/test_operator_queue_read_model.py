from __future__ import annotations

import inspect

from ap.operator_queue_read_model import dashboard_queue_bucket, empty_queue_counts
import ap.operator_queue_read_model as read_model


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
