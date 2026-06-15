"""
tests/test_p0_rearm_meta_column.py

P0 follow-up to PR #141: persistence target is orders.meta (JSONB), not
result_json. PR #141 merged with the wrong column name — this test verifies
the fix and prevents regression.

orders table has no result_json column. The SQL UPDATE in PR #141 would
silently fail (caught at warning level), leaving the prior-attempt check
unable to see auto_rearm_attempted, which means the watchdog could re-arm
the same order on every cycle.
"""
from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest


def _make_monitor():
    sys.modules.setdefault("ap.logger", MagicMock())
    if "ap.order_monitor" in sys.modules:
        del sys.modules["ap.order_monitor"]
    from ap.order_monitor import APOrderMonitor

    watcher = MagicMock()
    watcher.has_order = MagicMock(return_value=False)
    watcher.watch = MagicMock(return_value=True)
    watcher._last_reject_reason = None

    monitor = APOrderMonitor.__new__(APOrderMonitor)
    monitor.client_id = "jason_test@example.com"
    monitor.entry_watcher = watcher
    monitor._emit_order_event = MagicMock()
    monitor._alert = MagicMock()
    return monitor


def test_persistence_targets_meta_jsonb_not_result_json():
    """SQL UPDATE must touch orders.meta, never result_json."""
    monitor = _make_monitor()

    captured = {}
    def fake_run_with_retry(fn, *a, **k):
        fake_cursor = MagicMock()
        def _execute(sql, params):
            captured["sql"] = sql
            captured["params"] = params
        fake_cursor.execute = _execute
        fake_conn = MagicMock()
        fake_conn.__enter__ = MagicMock(return_value=fake_cursor)
        fake_conn.__exit__ = MagicMock(return_value=False)
        with patch("ap.db.conn", return_value=fake_conn):
            return fn()

    with patch("ap.db.run_with_retry", side_effect=fake_run_with_retry), \
         patch("ap.db.conn"):
        monitor._record_lost_handoff_rearm(
            "abc-local",
            attempted=True,
            succeeded=True,
            reason="lost_handoff_recovery",
        )

    sql = captured.get("sql", "")
    assert "SET meta" in sql, (
        f"persistence must target orders.meta, got SQL: {sql!r}"
    )
    assert "COALESCE(meta" in sql, (
        "must use COALESCE(meta, '{}'::jsonb) || payload merge"
    )
    assert "result_json" not in sql, (
        "must NOT touch result_json — column does not exist"
    )

    params = captured.get("params", ())
    assert "abc-local" in params

    import json as _json
    payload_json = next(
        (p for p in params if isinstance(p, str) and "auto_rearm" in p),
        None,
    )
    assert payload_json is not None, "payload JSON not found in params"
    payload = _json.loads(payload_json)
    assert payload["auto_rearm_attempted"] is True
    assert payload["auto_rearm_succeeded"] is True
    assert payload["auto_rearm_reason"] == "lost_handoff_recovery"


def test_no_result_json_anywhere_in_module():
    """Regression assertion: no result_json reference anywhere in
    ap/order_monitor.py production code. orders has no such column."""
    from pathlib import Path
    om_path = Path(__file__).resolve().parents[1] / "ap" / "order_monitor.py"
    src = om_path.read_text()
    assert "result_json" not in src, (
        "ap/order_monitor.py still references result_json — "
        "orders table has no such column; use meta JSONB instead"
    )


def test_meta_dict_shape_read_correctly():
    """When psycopg2 returns JSONB as dict, prior-attempt check reads it."""
    monitor = _make_monitor()
    # Indirect: a fresh order without auto_rearm in meta dict should allow re-arm
    order = {
        "local_order_id": "abc",
        "symbol":         "SPY",
        "direction":      "CALL",
        "trigger_price":  500.0,
        "contract":       "SPY_X",
        "meta":           {"timeframe": "1d"},   # dict shape
    }
    attempted, succeeded, _ = monitor._attempt_lost_handoff_rearm(
        order, "abc", "SPY_X"
    )
    assert attempted is True
    assert succeeded is True


def test_meta_string_shape_tolerated():
    """When psycopg2 returns JSONB as JSON string, plan rebuild still works."""
    monitor = _make_monitor()
    order = {
        "local_order_id": "abc",
        "symbol":         "SPY",
        "direction":      "CALL",
        "trigger_price":  500.0,
        "contract":       "SPY_X",
        "meta":           '{"timeframe": "1d"}',   # string shape
    }
    plan = monitor._build_lost_handoff_plan_from_order(order)
    assert plan is not None
    assert plan.ticker == "SPY"
    assert plan.metadata.get("timeframe") == "1d"
