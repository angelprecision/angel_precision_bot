"""Direction reversal must create a clean, watcher-owned first attempt."""
from __future__ import annotations

import json
from types import SimpleNamespace

import ap.order_state_machine as osm_mod
from ap.order_state_machine import APOrderStateMachine
from ap_execution_core import _reset_direction_reversal_runtime_state


def test_runtime_reset_archives_trigger_and_clears_attempt_state():
    provenance = {
        "canonical_signal_id": "sig-1",
        "client_id": "jason@example.com",
        "execution_mode": "live",
        "local_order_id": "order-1",
    }
    plan = SimpleNamespace(metadata={
        "trigger_crossed_at": "2026-08-06T14:00:00+00:00",
        "trigger_crossed_at_provenance": dict(provenance),
        "retry_attempt": 2,
        "breach_attempt_count": 2,
        "materialization_attempts": 2,
        "selector_recovery_cursor_v1": {"version": 1},
    })
    signal = {
        "trigger_crossed_at": "2026-08-06T14:00:00+00:00",
        "trigger_crossed_at_provenance": dict(provenance),
        "retry_attempt": 2,
        "metadata": dict(plan.metadata),
    }
    watched = SimpleNamespace(
        breach_count=2,
        trigger_crossed_at="2026-08-06T14:00:00+00:00",
        trigger_crossed_at_provenance=dict(provenance),
    )

    _reset_direction_reversal_runtime_state(watched, plan, signal)

    for target in (plan.metadata, signal["metadata"]):
        assert target["first_trigger_crossed_at"] == "2026-08-06T14:00:00+00:00"
        assert target["first_trigger_crossed_at_provenance"] == provenance
        assert target["retry_attempt"] == 0
        assert target["breach_attempt_count"] == 0
        assert target["materialization_attempts"] == 0
        assert target["next_retry_at"] == ""
        assert "selector_recovery_cursor_v1" not in target
        assert "trigger_crossed_at" not in target
        assert "trigger_crossed_at_provenance" not in target

    assert signal["retry_attempt"] == 0
    assert "trigger_crossed_at" not in signal
    assert watched.breach_count == 0
    assert watched.trigger_crossed_at is None
    assert watched.trigger_crossed_at_provenance is None


class _CaptureCursor:
    rowcount = 1

    def __init__(self):
        self.sql = None
        self.params = None

    def execute(self, sql, params):
        self.sql = sql
        self.params = params
        return self


class _CaptureConn:
    def __init__(self, cursor):
        self.cursor = cursor

    def __enter__(self):
        return self.cursor

    def __exit__(self, *_args):
        return False


def test_osm_rearm_resets_counters_and_preserves_watcher_owner(monkeypatch):
    cursor = _CaptureCursor()
    monkeypatch.setattr(osm_mod, "conn", lambda: _CaptureConn(cursor))
    monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn: fn())

    osm = object.__new__(APOrderStateMachine)
    osm.client_id = "jason@example.com"

    assert osm.rearm_deferred_materialization_direction_reversal(
        "order-1",
        owner="watcher-token-1",
        generation=3,
        signal_id="sig-1",
        execution_mode="live",
        market_truth_audit={"reason": "CALL_NO_LONGER_ABOVE_TRIGGER"},
    ) is True

    patch = json.loads(cursor.params[0])
    assert patch["current_owner"] == "watcher-token-1"
    assert patch["watcher_token"] == "watcher-token-1"
    assert patch["materialization_owner"] == ""
    assert patch["retry_attempt"] == 0
    assert patch["retry_attempt_in_flight"] == 0
    assert patch["breach_attempt_count"] == 0
    assert patch["materialization_attempts"] == 0
    assert patch["next_retry_at"] == ""
    assert patch["materialization_next_retry_at"] == ""
    assert patch["selector_recovery_cursor_v1"] is None

    assert "first_trigger_crossed_at" in cursor.sql
    assert "first_trigger_crossed_at_provenance" in cursor.sql
    assert "- 'selector_recovery_cursor_v1'" in cursor.sql
    assert "- 'trigger_crossed_at'" in cursor.sql
    assert "- 'trigger_crossed_at_provenance'" in cursor.sql
    assert cursor.params[1:] == (
        "order-1",
        "jason@example.com",
        "sig-1",
        "live",
        "watcher-token-1",
        3,
    )
