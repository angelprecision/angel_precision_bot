"""Direction reversal must create a clean, watcher-owned first attempt.

Covers two ownership paths distinctly:
  * An uninterrupted, registered APEntryWatcher retains its exact watcher
    token through rearm.
  * A synthetic restart/due-retry callback (no registered watcher) never has
    its recovery takeover claim persisted as watcher ownership; the row is
    explicitly marked as still requiring a real watcher attachment.
"""
from __future__ import annotations

import json
from types import SimpleNamespace

import ap.order_state_machine as osm_mod
from ap.order_state_machine import APOrderStateMachine
from ap_execution_core import _reset_direction_reversal_runtime_state


def test_runtime_reset_archives_trigger_and_clears_attempt_state_uninterrupted_watcher():
    """Uninterrupted real watcher: current_owner/watcher_token retain the
    real token, and the row is NOT marked as requiring a watcher."""
    provenance = {
        "canonical_signal_id": "sig-1",
        "client_id": "jason@example.com",
        "execution_mode": "live",
        "local_order_id": "order-1",
    }
    plan = SimpleNamespace(metadata={
        "trigger_crossed_at": "2026-08-06T14:00:00+00:00",
        "trigger_crossed_at_provenance": dict(provenance),
        "trigger_confirmed_at": "2026-08-06T14:00:15+00:00",
        "last_confirmed_trigger_at": "2026-08-06T14:01:00+00:00",
        "original_trigger_crossed_at": "2026-08-06T14:00:00+00:00",
        "first_breach_bid": 100.0,
        "first_breach_ask": 100.05,
        "last_trigger_confirmation_quote": {"bid": 100.0, "ask": 100.05},
        "retry_attempt": 2,
        "breach_attempt_count": 2,
        "materialization_attempts": 2,
        "deferred_retry_next_attempt_at": "2026-08-06T14:02:00+00:00",
        "materialization_retry_owner": "recovery-owner",
        "selector_recovery_cursor_v1": {"version": 1},
        "materialization_selector_failure": {"reason": "CHAIN_EMPTY"},
        "materialization_outcome": "RETRY_LATER_DATA_UNAVAILABLE",
    })
    signal = {
        "trigger_crossed_at": "2026-08-06T14:00:00+00:00",
        "trigger_crossed_at_provenance": dict(provenance),
        "trigger_confirmed_at": "2026-08-06T14:00:15+00:00",
        "first_breach_bid": 100.0,
        "first_breach_ask": 100.05,
        "retry_attempt": 2,
        "metadata": dict(plan.metadata),
    }
    watched = SimpleNamespace(
        breach_count=2,
        trigger_crossed_at="2026-08-06T14:00:00+00:00",
        trigger_crossed_at_provenance=dict(provenance),
        triggered_at="2026-08-06T14:00:15+00:00",
        _pending_first_breach_at="2026-08-06T14:00:00+00:00",
        first_breach_bid=100.0,
        first_breach_ask=100.05,
        trigger_price=100.05,
        breach_price=100.05,
        deferred_retry_not_before="2026-08-06T14:02:00+00:00",
    )

    audit = {"reason": "CALL_NO_LONGER_ABOVE_TRIGGER"}
    _reset_direction_reversal_runtime_state(
        watched,
        plan,
        signal,
        watcher_token="watcher-token-1",
        generation=3,
        market_truth_audit=audit,
    )

    for target in (plan.metadata, signal["metadata"]):
        assert target["materialization_status"] == "WAITING_FOR_TRIGGER"
        assert target["current_owner"] == "watcher-token-1"
        assert target["watcher_token"] == "watcher-token-1"
        assert target["recovery_ownership"] == ""
        assert target["recovery_owner"] == ""
        assert target["direction_reversal_rearm_requires_watcher"] is False
        assert target["watcher_generation"] == 3
        assert target["final_market_truth"] == audit
        assert target["retry_attempt"] == 0
        assert target["breach_attempt_count"] == 0
        assert target["materialization_attempts"] == 0
        assert target["next_retry_at"] == ""
        assert target["deferred_retry_next_attempt_at"] == ""
        assert target["first_trigger_crossed_at"] == "2026-08-06T14:00:00+00:00"
        assert target["first_trigger_crossed_at_provenance"] == provenance
        assert target["first_trigger_confirmed_at"] == "2026-08-06T14:00:15+00:00"
        assert target["first_trigger_breach_bid"] == 100.0
        assert target["first_trigger_breach_ask"] == 100.05
        assert "selector_recovery_cursor_v1" not in target
        assert "trigger_crossed_at" not in target
        assert "trigger_crossed_at_provenance" not in target
        assert "trigger_confirmed_at" not in target
        assert "last_confirmed_trigger_at" not in target
        assert "original_trigger_crossed_at" not in target
        assert "first_breach_bid" not in target
        assert "first_breach_ask" not in target

    assert watched.breach_count == 0
    assert watched.trigger_crossed_at is None
    assert watched.triggered_at is None
    assert watched._pending_first_breach_at is None
    assert watched.first_breach_bid == 0.0
    assert watched.first_breach_ask == 0.0
    assert watched.trigger_price is None
    assert watched.breach_price == 0.0
    assert watched.deferred_retry_not_before is None


def test_runtime_reset_synthetic_recovery_never_fabricates_watcher():
    """Synthetic restart/due-retry callback: watcher_token stays blank,
    recovery ownership fields are set, and all _recovery_pre_claimed*
    fields are stripped so the next real breach doesn't inherit stale
    claim authority."""
    provenance = {
        "canonical_signal_id": "sig-1",
        "client_id": "jason@example.com",
        "execution_mode": "live",
        "local_order_id": "order-1",
    }
    recovery_owner = "recovery_retry:jason@example.com:order-1:4"
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
        "_recovery_pre_claimed": True,
        "_recovery_pre_claimed_owner": recovery_owner,
        "_recovery_pre_claimed_generation": 4,
        "_recovery_pre_claimed_attempt": 2,
        "_recovery_pre_claimed_client_id": "jason@example.com",
        "_recovery_pre_claimed_mode": "live",
        "recovery_submit_owner": recovery_owner,
        "recovery_submit_generation": 4,
        "recovery_submit_fenced": True,
        "ownership_kind": "materialization_retry",
        "owner": recovery_owner,
        "fenced": True,
        "metadata": dict(plan.metadata),
    }
    watched = SimpleNamespace(
        breach_count=2,
        trigger_crossed_at="2026-08-06T14:00:00+00:00",
        trigger_crossed_at_provenance=dict(provenance),
        triggered_at="2026-08-06T14:00:15+00:00",
        _pending_first_breach_at="2026-08-06T14:00:00+00:00",
        first_breach_bid=100.0,
        first_breach_ask=100.05,
        trigger_price=100.05,
        breach_price=100.05,
    )

    audit = {"reason": "CALL_NO_LONGER_ABOVE_TRIGGER"}
    _reset_direction_reversal_runtime_state(
        watched,
        plan,
        signal,
        recovery_owner=recovery_owner,
        generation=4,
        market_truth_audit=audit,
    )

    for target in (plan.metadata, signal["metadata"]):
        assert target["materialization_status"] == "WAITING_FOR_TRIGGER"
        assert target["current_owner"] == recovery_owner
        assert target["watcher_token"] == ""
        assert target["recovery_ownership"] == "recovery_scheduler"
        assert target["recovery_owner"] == recovery_owner
        assert target["direction_reversal_rearm_requires_watcher"] is True
        assert target["watcher_generation"] == 0
        assert target["retry_attempt"] == 0
        assert "selector_recovery_cursor_v1" not in target

    assert "_recovery_pre_claimed" not in signal
    assert "_recovery_pre_claimed_owner" not in signal
    assert "_recovery_pre_claimed_generation" not in signal
    assert "_recovery_pre_claimed_attempt" not in signal
    assert "_recovery_pre_claimed_client_id" not in signal
    assert "_recovery_pre_claimed_mode" not in signal
    assert "recovery_submit_owner" not in signal
    assert "recovery_submit_generation" not in signal
    assert "recovery_submit_fenced" not in signal
    assert "ownership_kind" not in signal
    assert "owner" not in signal
    assert "fenced" not in signal

    assert watched.breach_count == 0
    assert watched.trigger_crossed_at is None
    assert watched.triggered_at is None
    assert watched.first_breach_bid == 0.0
    assert watched.first_breach_ask == 0.0
    assert watched.trigger_price is None
    assert watched.breach_price == 0.0


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


def test_osm_rearm_uninterrupted_watcher_retains_real_token(monkeypatch):
    """3B: the uninterrupted-watcher OSM path -- real watcher_token supplied,
    row lands in WAITING_FOR_TRIGGER with the real token retained."""
    cursor = _CaptureCursor()
    monkeypatch.setattr(osm_mod, "conn", lambda: _CaptureConn(cursor))
    monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn: fn())

    osm = object.__new__(APOrderStateMachine)
    osm.client_id = "jason@example.com"

    assert osm.rearm_deferred_materialization_direction_reversal(
        "order-1",
        owner="watcher-token-1",
        watcher_token="watcher-token-1",
        generation=3,
        signal_id="sig-1",
        execution_mode="live",
        market_truth_audit={"reason": "CALL_NO_LONGER_ABOVE_TRIGGER"},
    ) is True

    patch = json.loads(cursor.params[0])
    assert patch["materialization_status"] == "WAITING_FOR_TRIGGER"
    assert patch["current_owner"] == "watcher-token-1"
    assert patch["watcher_token"] == "watcher-token-1"
    assert patch["recovery_ownership"] == ""
    assert patch["recovery_owner"] == ""
    assert patch["direction_reversal_rearm_requires_watcher"] is False
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


def test_osm_rearm_from_recovery_does_not_fabricate_watcher(monkeypatch):
    """3C: the synthetic-recovery OSM path -- no watcher_token supplied, the
    recovery takeover token is used only as the CAS claim owner and is never
    persisted as watcher_token."""
    cursor = _CaptureCursor()
    monkeypatch.setattr(osm_mod, "conn", lambda: _CaptureConn(cursor))
    monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn: fn())

    osm = object.__new__(APOrderStateMachine)
    osm.client_id = "jason@example.com"

    recovery_owner = "recovery_retry:jason@example.com:order-1:4"
    assert osm.rearm_deferred_materialization_direction_reversal(
        "order-1",
        owner=recovery_owner,
        watcher_token="",
        generation=4,
        signal_id="sig-1",
        execution_mode="live",
        market_truth_audit={"reason": "CALL_NO_LONGER_ABOVE_TRIGGER"},
    ) is True

    patch = json.loads(cursor.params[0])
    assert patch["materialization_status"] == "WAITING_FOR_TRIGGER"
    assert patch["current_owner"] == recovery_owner
    assert patch["watcher_token"] == ""
    assert patch["recovery_ownership"] == "recovery_scheduler"
    assert patch["recovery_owner"] == recovery_owner
    assert patch["direction_reversal_rearm_requires_watcher"] is True

    assert "- 'selector_recovery_cursor_v1'" in cursor.sql
    assert "- 'trigger_crossed_at'" in cursor.sql
    assert "- 'trigger_crossed_at_provenance'" in cursor.sql
    assert cursor.params[1:] == (
        "order-1",
        "jason@example.com",
        "sig-1",
        "live",
        recovery_owner,
        4,
    )


def test_osm_rearm_rejects_watcher_token_owner_mismatch(monkeypatch):
    """A watcher_token that disagrees with the claim owner is an ownership
    conflict and must fail closed rather than silently overriding either
    value."""
    cursor = _CaptureCursor()
    monkeypatch.setattr(osm_mod, "conn", lambda: _CaptureConn(cursor))
    monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn: fn())

    osm = object.__new__(APOrderStateMachine)
    osm.client_id = "jason@example.com"

    assert osm.rearm_deferred_materialization_direction_reversal(
        "order-1",
        owner="watcher-token-1",
        watcher_token="a-different-token",
        generation=3,
        signal_id="sig-1",
        execution_mode="live",
        market_truth_audit={"reason": "CALL_NO_LONGER_ABOVE_TRIGGER"},
    ) is False
    # No SQL should have been attempted at all.
    assert cursor.sql is None
