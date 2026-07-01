from __future__ import annotations

from ap.operator_queue_counts import _dashboard_bucket, _empty_counts


def test_operator_queue_status_compatibility_mapping():
    assert _dashboard_bucket("NEW") == "NEW"
    assert _dashboard_bucket("WATCHING") == "WATCHING"
    assert _dashboard_bucket("PENDING_TRIGGER") == "WATCHING"
    assert _dashboard_bucket("SUBMITTED") == "TRIGGERED"
    assert _dashboard_bucket("ACK") == "TRIGGERED"
    assert _dashboard_bucket("ACKNOWLEDGED") == "TRIGGERED"
    assert _dashboard_bucket("WORKING") == "TRIGGERED"
    assert _dashboard_bucket("REJECTED") == "REJECTED"
    assert _dashboard_bucket("EXPIRED") == "EXPIRED"


def test_operator_queue_default_counts_match_dashboard_cards():
    assert _empty_counts() == {
        "NEW": 0,
        "WATCHING": 0,
        "TRIGGERED": 0,
        "REJECTED": 0,
        "EXPIRED": 0,
    }


def test_operator_queue_module_does_not_reference_trade_queue_updated_ts():
    import inspect
    import ap.operator_queue_counts as oqc

    src = inspect.getsource(oqc)
    assert "trade_queue.updated_ts" not in src
    assert "updated_ts >=" not in src.split("FROM trade_queue")[1].split("FROM orders")[0]
