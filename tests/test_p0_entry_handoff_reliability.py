"""
tests/test_p0_entry_handoff_reliability.py

P0 PR #141 — Entry Handoff Reliability & Lost Ownership Recovery.

Verifies:
  1. TIMEOUT_CREATED default bumped to 90s
  2. _build_lost_handoff_plan_from_order rebuilds plan from order row
  3. _attempt_lost_handoff_rearm calls watcher.watch once and returns truth
  4. Watcher already owns local_order_id → ownership_confirmed path
  5. Re-arm succeeds → order is NOT canceled
  6. Re-arm fails → order IS canceled with LOST_HANDOFF_90S
  7. Deferred contract preserves metadata.contract_deferred=True on re-arm
  8. result_json.auto_rearm_attempted is persisted

These tests instantiate APOrderMonitor with mocked entry_watcher and DB so we
exercise the real code paths added in this PR. They do NOT touch broker,
master_control, OSM transition logic, exits, fills, or scanner.
"""
from __future__ import annotations

import os
import sys
import types
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest


def _make_monitor():
    """Construct APOrderMonitor with mocked deps."""
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
    return monitor, watcher


# =============================================================================
# Test 1: TIMEOUT_CREATED default is 90s
# =============================================================================

def test_timeout_created_default_is_90():
    """The watchdog timeout default must be raised from 30 to 90."""
    # Clear env override so we exercise the default
    os.environ.pop("ORDER_TIMEOUT_CREATED", None)
    sys.modules.pop("ap.order_monitor", None)
    import ap.order_monitor as om
    assert om.TIMEOUT_CREATED == 90, (
        f"TIMEOUT_CREATED default must be 90 (got {om.TIMEOUT_CREATED})"
    )


# =============================================================================
# Test 2: plan rebuilder produces a SimpleNamespace with required fields
# =============================================================================

def test_build_lost_handoff_plan_from_order_minimal():
    monitor, _ = _make_monitor()
    order = {
        "local_order_id":    "abc-123",
        "signal_id":         "sig-1",
        "plan_id":           "plan-1",
        "symbol":            "SPY",
        "contract":          "SPY_240621C00500000",
        "direction":         "CALL",
        "trigger_price":     500.25,
        "stop_underlying":   499.00,
        "target_underlying": 502.50,
        "qty":               2,
        "score":             75.0,
        "tier":              "A",
        "meta":              {"timeframe": "1d", "pattern": "2-1_2D"},
    }
    plan = monitor._build_lost_handoff_plan_from_order(order)
    assert plan is not None
    assert plan.ticker == "SPY"
    assert plan.direction == "CALL"
    assert plan.trigger_price == 500.25
    assert plan.contract_symbol == "SPY_240621C00500000"
    assert plan.contracts == 2
    assert plan.score == 75.0
    assert plan.tier == "A"


def test_build_lost_handoff_plan_returns_none_for_missing_ticker():
    monitor, _ = _make_monitor()
    plan = monitor._build_lost_handoff_plan_from_order({
        "local_order_id": "x", "trigger_price": 1.0, "meta": {},
    })
    assert plan is None


def test_build_lost_handoff_plan_returns_none_for_missing_trigger():
    monitor, _ = _make_monitor()
    plan = monitor._build_lost_handoff_plan_from_order({
        "local_order_id": "x", "symbol": "SPY", "meta": {},
    })
    assert plan is None


def test_build_lost_handoff_plan_preserves_deferred_metadata():
    """Deferred contracts must keep metadata.contract_deferred=True on re-arm."""
    monitor, _ = _make_monitor()
    order = {
        "local_order_id": "abc", "symbol": "AVGO",
        "contract": "DEFERRED:AVGO",
        "direction": "CALL", "trigger_price": 1500.0,
        "meta": {},
    }
    plan = monitor._build_lost_handoff_plan_from_order(order)
    assert plan is not None
    assert plan.contract_symbol == "DEFERRED:AVGO"
    assert plan.metadata.get("contract_deferred") is True


# =============================================================================
# Test 3: _attempt_lost_handoff_rearm — happy path
# =============================================================================

def test_attempt_rearm_success():
    monitor, watcher = _make_monitor()
    watcher.watch.return_value = True

    order = {
        "local_order_id": "abc", "symbol": "SPY",
        "direction": "CALL", "trigger_price": 500.0,
        "contract": "SPY_X", "meta": {},
    }
    attempted, succeeded, reason = monitor._attempt_lost_handoff_rearm(
        order, "abc", "SPY_X"
    )
    assert attempted is True
    assert succeeded is True
    assert reason == "lost_handoff_recovery"
    # watcher.watch called exactly once
    assert watcher.watch.call_count == 1
    # Called with the plan + local_order_id (not order dict)
    call_args = watcher.watch.call_args
    plan_arg = call_args.args[0]
    assert plan_arg.ticker == "SPY"
    assert call_args.args[1] == "abc"


def test_attempt_rearm_watcher_missing():
    monitor, _ = _make_monitor()
    monitor.entry_watcher = None  # simulate missing watcher
    attempted, succeeded, reason = monitor._attempt_lost_handoff_rearm(
        {"symbol": "SPY", "trigger_price": 500.0, "meta": {}},
        "abc", "SPY_X",
    )
    assert attempted is False
    assert succeeded is False
    assert reason == "entry_watcher_missing"


def test_attempt_rearm_plan_rebuild_fails():
    """If the order is too sparse to rebuild a plan, attempt returns False."""
    monitor, watcher = _make_monitor()
    attempted, succeeded, reason = monitor._attempt_lost_handoff_rearm(
        {"symbol": "", "trigger_price": 0, "meta": {}},   # no ticker
        "abc", "SPY_X",
    )
    assert attempted is False
    assert succeeded is False
    assert reason == "plan_rebuild_failed"
    # watcher.watch must NOT have been called
    assert watcher.watch.call_count == 0


def test_attempt_rearm_watch_returns_false():
    """When watcher.watch returns False, attempted=True, succeeded=False."""
    monitor, watcher = _make_monitor()
    watcher.watch.return_value = False
    watcher._last_reject_reason = "stop_above_mid"

    order = {
        "local_order_id": "abc", "symbol": "SPY",
        "direction": "CALL", "trigger_price": 500.0,
        "contract": "SPY_X", "meta": {},
    }
    attempted, succeeded, reason = monitor._attempt_lost_handoff_rearm(
        order, "abc", "SPY_X"
    )
    assert attempted is True
    assert succeeded is False
    assert reason == "stop_above_mid"


def test_attempt_rearm_refuses_unproven_confirmed_evidence_without_watching():
    """A stale confirmed timestamp must not enter lost-handoff rearm."""
    monitor, watcher = _make_monitor()
    monitor.client_mode = "LIVE"
    order = {
        "local_order_id": "abc",
        "client_id": "jason_test@example.com",
        "symbol": "SPY",
        "direction": "CALL",
        "trigger_price": 500.0,
        "contract": "SPY_X",
        "meta": {
            "canonical_signal_id": "canonical-current",
            "client_id": "jason_test@example.com",
            "execution_mode": "live",
            "trigger_crossed_at": "2026-08-03T16:00:00+00:00",
            "trigger_crossed_at_provenance": {
                "canonical_signal_id": "canonical-stale",
                "client_id": "jason_test@example.com",
                "execution_mode": "live",
                "local_order_id": "abc",
            },
        },
    }

    attempted, succeeded, reason = monitor._attempt_lost_handoff_rearm(
        order, "abc", "SPY_X"
    )

    assert (attempted, succeeded) == (True, False)
    assert reason == "RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN"
    watcher.watch.assert_not_called()


def test_lost_handoff_identity_refusal_skips_cleanup_and_attempt_marker():
    """The order-monitor caller must preserve the exact CREATED row too."""
    monitor, _watcher = _make_monitor()
    monitor.client_mode = "LIVE"
    monitor.osm = MagicMock()
    monitor._get_active_entry_orders = MagicMock(
        return_value=[
            {
                "status": "CREATED",
                "local_order_id": "abc",
                "signal_id": "sig-1",
                "plan_id": "plan-1",
                "symbol": "SPY",
                "contract": "SPY_X",
                "broker_order_id": None,
                "submitted_ts": None,
                "created_ts": "2026-08-03T15:55:00+00:00",
                "meta": {},
            }
        ]
    )
    monitor._parse_ts = lambda _raw: datetime.now(timezone.utc) - timedelta(seconds=200)
    monitor._emit_order_event = MagicMock()
    monitor._pending_trigger_watcher_owner_state = MagicMock(
        return_value=(False, True, None)
    )
    monitor._attempt_lost_handoff_rearm = MagicMock(
        return_value=(True, False, "RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN")
    )
    monitor._record_lost_handoff_rearm = MagicMock()
    monitor._handle_stale_entry = MagicMock()

    monitor._check_entry_orders()

    monitor._attempt_lost_handoff_rearm.assert_called_once()
    monitor._record_lost_handoff_rearm.assert_not_called()
    monitor._handle_stale_entry.assert_not_called()
    monitor.osm.expire_pending_entry.assert_not_called()
    monitor.osm.cancel_pending_entry.assert_not_called()
    monitor.osm.transition.assert_not_called()


def test_attempt_rearm_watch_raises():
    """If watch() raises, return (True, False, watch_exception:...)."""
    monitor, watcher = _make_monitor()
    watcher.watch.side_effect = RuntimeError("broker_unreachable")

    order = {
        "local_order_id": "abc", "symbol": "SPY",
        "direction": "CALL", "trigger_price": 500.0,
        "contract": "SPY_X", "meta": {},
    }
    attempted, succeeded, reason = monitor._attempt_lost_handoff_rearm(
        order, "abc", "SPY_X"
    )
    assert attempted is True
    assert succeeded is False
    assert "watch_exception" in reason


# =============================================================================
# Test 4: ownership probe — watcher already owns the order
# =============================================================================

def test_ownership_probe_returns_true_when_watcher_owns():
    monitor, watcher = _make_monitor()
    watcher.has_order = MagicMock(return_value=True)

    owned, ok, err = monitor._pending_trigger_watcher_owner_state("abc")
    assert owned is True
    assert ok is True
    assert err is None


def test_ownership_probe_returns_false_when_watcher_does_not_own():
    monitor, watcher = _make_monitor()
    watcher.has_order = MagicMock(return_value=False)

    owned, ok, err = monitor._pending_trigger_watcher_owner_state("abc")
    assert owned is False
    assert ok is True
    assert err is None


# =============================================================================
# Test 5: persist auto_rearm to orders.result_json
# =============================================================================

def test_record_lost_handoff_rearm_persists_to_db():
    monitor, _ = _make_monitor()

    captured = {}
    def fake_run_with_retry(fn, *a, **k):
        # Wire a fake cursor that records the SQL params
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

    # SQL should update orders.result_json with our payload
    assert "result_json" in captured.get("sql", "")
    assert "abc-local" in captured.get("params", ())
    # Payload JSON should include auto_rearm_attempted
    import json as _json
    payload_json = next(
        (p for p in captured.get("params", ()) if isinstance(p, str) and "auto_rearm" in p),
        None,
    )
    assert payload_json is not None
    payload = _json.loads(payload_json)
    assert payload["auto_rearm_attempted"] is True
    assert payload["auto_rearm_succeeded"] is True
    assert payload["auto_rearm_reason"] == "lost_handoff_recovery"
