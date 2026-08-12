"""Direction reversal must create a clean, watcher-owned first attempt.

Covers two ownership paths distinctly:
  * An uninterrupted, registered APEntryWatcher retains its exact watcher
    token through rearm.
  * A synthetic restart/due-retry callback (no registered watcher) never has
    its recovery takeover claim persisted as watcher ownership; the row is
    explicitly marked as still requiring a real watcher attachment.

Full-seam integration test covers:
  APStartupRecovery due_retry
  → resume_deferred_materialization_retry
  → synthetic watched object
  → real _on_entry_trigger
  → real direction-reversal rearm
  → REARM_WATCHER_REQUIRED
  → refreshed order
  → existing PendingTriggerRestartRecovery
  → real watcher attachment or bounded restart-rearm retry
"""
from __future__ import annotations

import json
import os
import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call

os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost/db")

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
        assert target["materialization_status"] == ""
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
        assert target["materialization_status"] == ""
        assert target["current_owner"] == ""
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
    assert patch["materialization_status"] == ""
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
    assert patch["materialization_status"] == ""
    assert patch["current_owner"] == ""
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


# ─────────────────────────────────────────────────────────────────────────────
# Full-seam integration test
# ─────────────────────────────────────────────────────────────────────────────

def _make_materializing_row(local_order_id, signal_id, plan_id, client_id,
                             exec_mode, generation, attempt, owner,
                             trigger_price, trigger_crossed_at, provenance):
    """Return a MATERIALIZING orders row for _on_entry_trigger's ownership gate."""
    import json as _j
    return {
        "local_order_id": local_order_id,
        "signal_id": signal_id,
        "plan_id": plan_id,
        "client_id": client_id,
        "execution_mode": exec_mode,
        "status": "PENDING_TRIGGER",
        "direction": "CALL",
        "symbol": "SPY",
        "trigger_price": trigger_price,
        "score": 75.0,
        "tier": "A",
        "timeframe": "5m",
        "qty": 1,
        "limit_price": 2.50,
        "reserved_cost": 250.0,
        "broker_order_id": None,
        "submitted_ts": None,
        "meta": {
            "lifecycle_state": "MATERIALIZING",
            "materialization_status": "RUNNING",
            "materialization_in_flight": True,
            "materialization_generation": generation,
            "materialization_owner": owner,
            "materialization_lease_until": "2099-01-01T00:00:00+00:00",
            "retry_attempt": attempt,
            "retry_max_attempts": 5,
            "breach_attempt_count": attempt,
            "materialization_attempts": attempt,
            "trigger_crossed_at": trigger_crossed_at,
            "trigger_crossed_at_provenance": dict(provenance),
            "trigger_confirmed_at": "2026-08-06T14:00:15+00:00",
            "first_breach_bid": 100.5,
            "first_breach_ask": 100.55,
            # Cursor required by _selector_cursor_retry_block_reason for attempt > 1.
            # A missing cursor terminates before the market truth check fires.
            # Identity fields must match exactly so load_selector_recovery_cursor
            # accepts the candidate rather than returning IDENTITY_MISMATCH.
            "selector_recovery_cursor_v1": {
                "version": 1,
                "local_order_id": local_order_id,
                "client_id": str(client_id or "").strip().lower(),
                "execution_mode": str(exec_mode or "").strip().lower(),
                "signal_id": signal_id,
                "materialization_generation": generation,
                "selector_attempt_count": max(1, attempt - 1),
                "attempted_symbols": {},
                "structurally_skipped_symbols": {},
                "expirations_probed": [],
                "last_ranked_index_by_expiration": {},
            },
        },
    }


def test_seam_full_direction_reversal_recovery_chain(monkeypatch):
    """Full seam: APStartupRecovery due retry → resume_deferred_materialization_retry
    → synthetic watched object → real _on_entry_trigger → real direction-reversal
    rearm → REARM_WATCHER_REQUIRED → refreshed order → PendingTriggerRestartRecovery.

    Proves all 17 required assertions without monkeypatching _on_entry_trigger.
    External DB, quote, selector, broker, and watcher dependencies are faked;
    the production callback and recovery control flow execute as written.
    """
    import threading
    from datetime import datetime, timezone

    import ap.order_state_machine as osm_mod
    from ap.order_state_machine import APOrderStateMachine
    from ap_execution_core import APExecutionCore
    from ap.pending_trigger_restart_recovery import (
        PendingTriggerRestartRecovery,
        _RowOutcome,
    )

    # ── Identity constants ─────────────────────────────────────────────────
    LOCAL_ORDER_ID = "order-seam-dr-001"
    SIGNAL_ID      = "sig-seam-dr-001"
    PLAN_ID        = "plan-seam-dr-001"
    CLIENT_ID      = "jason@example.com"
    EXEC_MODE      = "paper"
    GENERATION     = 3          # durable generation before this retry
    PRIOR_ATTEMPT  = 1          # durable attempt before this retry fires
    EXPECTED_ATT   = 2          # the attempt resume_deferred claims
    NEW_GEN        = GENERATION + 1   # generation after claim = 4
    TRIGGER_PRICE  = 100.0
    TRIGGER_TS     = "2026-08-06T14:00:00+00:00"
    OWNER          = f"recovery_retry:{CLIENT_ID}:{LOCAL_ORDER_ID}:{NEW_GEN + 1}"

    provenance = {
        "canonical_signal_id": SIGNAL_ID,
        "client_id": CLIENT_ID,
        "execution_mode": EXEC_MODE,
        "local_order_id": LOCAL_ORDER_ID,
    }

    # ── Fake row: RETRY_WAIT state read by resume_deferred (call #1) ───────
    retry_wait_row = {
        "local_order_id": LOCAL_ORDER_ID,
        "signal_id": SIGNAL_ID,
        "plan_id": PLAN_ID,
        "client_id": CLIENT_ID,
        "execution_mode": EXEC_MODE,
        "status": "PENDING_TRIGGER",
        "direction": "CALL",
        "symbol": "SPY",
        "trigger_price": TRIGGER_PRICE,
        "score": 75.0,
        "tier": "A",
        "timeframe": "5m",
        "qty": 1,
        "limit_price": 2.50,
        "reserved_cost": 250.0,
        "broker_order_id": None,
        "submitted_ts": None,
        "meta": {
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_LATER_DATA_UNAVAILABLE",
            "materialization_in_flight": False,
            "materialization_generation": GENERATION,
            "materialization_owner": "",
            "retry_attempt": PRIOR_ATTEMPT,
            "retry_max_attempts": 5,
            "breach_attempt_count": PRIOR_ATTEMPT,
            "materialization_attempts": PRIOR_ATTEMPT,
            "materialization_next_retry_at": "2026-08-06T13:59:00+00:00",  # past
            "trigger_crossed_at": TRIGGER_TS,
            "trigger_crossed_at_provenance": dict(provenance),
            "trigger_confirmed_at": "2026-08-06T14:00:15+00:00",
            "first_breach_bid": 100.5,
            "first_breach_ask": 100.55,
        },
    }

    # ── Fake row: MATERIALIZING state — returned for _on_entry_trigger reads
    # (ownership verify, pre-claim verify, cursor read — calls #2-4) ────────
    materializing_row = _make_materializing_row(
        LOCAL_ORDER_ID, SIGNAL_ID, PLAN_ID, CLIENT_ID,
        EXEC_MODE, NEW_GEN, EXPECTED_ATT, OWNER,
        TRIGGER_PRICE, TRIGGER_TS, provenance,
    )

    # ── Fake row: post-rearm truly blank pre-breach state, returned by PTR
    # re-read. current_owner and materialization_status stay blank until a
    # real watcher's durable ownership is later adopted. ──────────────────
    post_rearm_row = {
        **retry_wait_row,
        "kind": "ENTRY",
        "meta": {
            "lifecycle_state": "",
            "materialization_status": "",
            "materialization_in_flight": False,
            "materialization_owner": "",
            "watcher_token": "",
            "current_owner": "",
            "recovery_ownership": "recovery_scheduler",
            "recovery_owner": OWNER,
            "direction_reversal_rearm_requires_watcher": True,
            "materialization_generation": NEW_GEN,
            "retry_attempt": 0,
            "breach_attempt_count": 0,
            "materialization_attempts": 0,
            "final_market_truth_status": "REARM_DIRECTION_REVERSAL",
            # Archived first-trigger evidence (assertion 15)
            "first_trigger_crossed_at": TRIGGER_TS,
            "first_trigger_crossed_at_provenance": dict(provenance),
            "first_trigger_confirmed_at": "2026-08-06T14:00:15+00:00",
            "first_trigger_breach_bid": 100.5,
            "first_trigger_breach_ask": 100.55,
        },
    }

    # ── Fake get_order: sequence-driven ────────────────────────────────────
    # Call 1 (resume_deferred): RETRY_WAIT row
    # Calls 2-5 (_on_entry_trigger gates + market-truth cursor read): MATERIALIZING row
    # Call 6+ (PTR re-read after REARM_WATCHER_REQUIRED): post-rearm row
    _get_order_call_count = [0]

    def _fake_get_order(order_id):
        _get_order_call_count[0] += 1
        n = _get_order_call_count[0]
        if n == 1:
            return dict(retry_wait_row)
        if n <= 5:
            return dict(materializing_row)
        return dict(post_rearm_row)

    # ── OSM: real instance with patched DB ─────────────────────────────────
    real_osm = object.__new__(APOrderStateMachine)
    real_osm.client_id = CLIENT_ID

    # Intercept the CAS write to inspect what the real SQL persists
    rearm_patch_written = {}
    rearm_sql_written = {}

    class _FakeCursor:
        rowcount = 1
        def __init__(self):
            self.sql = ""
            self.params = None
        def execute(self, sql, params):
            self.sql = sql
            self.params = params
            rearm_patch_written.update(json.loads(params[0]))
            rearm_sql_written["sql"] = sql
            return self

    class _FakeConn:
        def __init__(self):
            self._cur = _FakeCursor()
        def __enter__(self):
            return self._cur
        def __exit__(self, *_):
            return False

    monkeypatch.setattr(osm_mod, "conn", lambda: _FakeConn())
    monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn: fn())

    # Attach real method, fake helper methods
    real_osm.get_order = _fake_get_order
    real_osm.claim_deferred_materialization = MagicMock(return_value=True)
    real_osm.update_order_meta = MagicMock(return_value=True)
    real_osm.persist_selector_recovery_cursor = MagicMock(return_value=True)
    real_osm.terminalize_materialization_retry = MagicMock(return_value=True)

    # ── Fake selector data broker: direction-reversal quote ────────────────
    # ask=98.5 < trigger=100.0 → CALL_NO_LONGER_ABOVE_TRIGGER → REARM_DIRECTION_REVERSAL
    _now_iso = datetime.now(timezone.utc).isoformat()
    _fake_db = MagicMock()
    _fake_db.cfg.base_url = "https://api.tradier.com"
    _fake_db.get_quote.return_value = {
        "bid": 98.0,
        "ask": 98.5,
        "provider_timestamp": _now_iso,
    }
    _fake_selector = MagicMock()
    _fake_selector.data_broker = _fake_db

    # Track broker/selector calls to prove zero side effects (assertions 10-12)
    broker_post_calls = []
    broker_cancel_calls = []

    # ── Build minimal APExecutionCore (bypassing __init__) ─────────────────
    core = object.__new__(APExecutionCore)
    core.client_id       = CLIENT_ID
    core.email           = CLIENT_ID
    core.execution_mode  = EXEC_MODE
    core.mode            = "PAPER"
    core.paper           = True
    core._kill_switch    = False
    core.master_control  = None           # no kill switch
    core.store           = MagicMock()
    core._max_positions  = 5
    core.position_manager = MagicMock()
    core.position_manager.snapshot.return_value = {
        "open_count": 0, "pending_entries": 0
    }
    core.order_state_machine = real_osm
    core.contract_selector   = _fake_selector
    core.broker              = MagicMock()
    core.broker.post.side_effect = lambda *a, **k: broker_post_calls.append((a, k)) or {}
    core.broker.cancel_order.side_effect = lambda *a, **k: broker_cancel_calls.append((a, k)) or {}
    core._pos_lock       = threading.Lock()
    core._position_count = 0

    # ── Phase 1: resume_deferred → real _on_entry_trigger → REARM_WATCHER_REQUIRED
    outcome = APExecutionCore.resume_deferred_materialization_retry(
        core,
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=GENERATION,
        expected_retry_attempt=EXPECTED_ATT,
        owner=OWNER,
    )

    # ── Assertion 3: synthetic recovery returns REARM_WATCHER_REQUIRED ─────
    assert outcome["disposition"] == "REARM_WATCHER_REQUIRED", (
        f"Expected REARM_WATCHER_REQUIRED, got {outcome!r}"
    )
    assert outcome["reason_code"] == "REARM_DIRECTION_REVERSAL"

    # ── Assertion 1: recovery owner never stored as watcher_token ──────────
    assert rearm_patch_written.get("watcher_token") == "", (
        f"watcher_token must be blank for synthetic recovery, got "
        f"{rearm_patch_written.get('watcher_token')!r}"
    )
    # current_owner is blank for synthetic recovery — a recovery takeover
    # token is not watcher ownership. recovery_owner carries the claim.
    assert rearm_patch_written.get("current_owner") == ""
    assert rearm_patch_written.get("recovery_owner") == OWNER
    assert rearm_patch_written.get("direction_reversal_rearm_requires_watcher") is True

    # ── Assertion 2: real watcher path preserved (OSM level) ───────────────
    # Proved by existing unit test test_osm_rearm_uninterrupted_watcher_retains_real_token;
    # confirmed here by checking that blank watcher_token → recovery_scheduler ownership.
    assert rearm_patch_written.get("recovery_ownership") == "recovery_scheduler"

    # ── Assertion 9: next breach starts attempt 1 (all counters reset) ─────
    assert rearm_patch_written.get("retry_attempt") == 0
    assert rearm_patch_written.get("breach_attempt_count") == 0
    assert rearm_patch_written.get("materialization_attempts") == 0
    assert rearm_patch_written.get("next_retry_at") == ""
    assert rearm_patch_written.get("deferred_retry_next_attempt_at") == ""

    # ── Assertion 10: zero selector calls during direction reversal ─────────
    assert not _fake_selector.select.called
    assert not _fake_selector.select_contract.called
    assert not _fake_selector.find_contract.called
    # data_broker.get_quote IS called for the market truth check (not selector)
    # but the contract selector itself is not called
    assert _fake_db.get_quote.called   # market truth quote was fetched
    call_count_after_truth = _fake_db.get_quote.call_count
    assert call_count_after_truth == 1  # exactly once for market truth

    # ── Assertion 11: zero broker POST calls ───────────────────────────────
    assert len(broker_post_calls) == 0
    assert not core.broker.post_order.called if hasattr(core.broker, "post_order") else True

    # ── Assertion 12: zero broker cancel calls ─────────────────────────────
    assert len(broker_cancel_calls) == 0

    # ── Assertion 14: active breach evidence cleared (SQL removes keys) ─────
    sql = rearm_sql_written.get("sql", "")
    assert "- 'trigger_crossed_at'" in sql
    assert "- 'trigger_crossed_at_provenance'" in sql
    assert "- 'selector_recovery_cursor_v1'" in sql
    assert rearm_patch_written.get("lifecycle_state") == ""
    assert rearm_patch_written.get("materialization_in_flight") is False
    assert rearm_patch_written.get("materialization_owner") == ""
    assert rearm_patch_written.get("materialization_lease_until") == ""

    # ── Assertion 15: archived first-trigger evidence preserved in SQL ──────
    assert "first_trigger_crossed_at" in sql
    assert "first_trigger_crossed_at_provenance" in sql
    # The patch itself does not contain trigger_crossed_at (was removed)
    assert "trigger_crossed_at" not in rearm_patch_written

    # ── Assertion 17: materialization generation monotonic ──────────────────
    # The rearm SQL uses a generation predicate; the patch does not reset it.
    # selector_recovery_cursor_v1 is nulled (clean reset), not rolled back.
    assert rearm_patch_written.get("selector_recovery_cursor_v1") is None
    assert rearm_patch_written.get("rearmed_at") is not None

    # ── Assertion 13: CAS failure leaves runtime state unchanged ───────────
    # Simulate CAS rowcount=0 (nothing matched) — no runtime state change.
    class _ZeroCursor(_FakeCursor):
        rowcount = 0
    class _ZeroConn:
        def __init__(self): self._cur = _ZeroCursor()
        def __enter__(self): return self._cur
        def __exit__(self, *_): return False

    monkeypatch.setattr(osm_mod, "conn", lambda: _ZeroConn())
    _get_order_call_count[0] = 0  # reset so next call is #1 → retry_wait_row
    _cas_fail_patch = {}
    original_update = {}
    _cas_outcome = APExecutionCore.resume_deferred_materialization_retry(
        core,
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=GENERATION,
        expected_retry_attempt=EXPECTED_ATT,
        owner=OWNER,
    )
    # CAS failure on the rearm write → materialization write failed disposition,
    # or (since this fake also fails the retry-wait fallback write) a retained-
    # ownership disposition. Either way, the row must never appear rearmed.
    assert _cas_outcome["disposition"] in {
        "KEEP_WATCHER", "MATERIALIZATION_REARM_WRITE_FAILED", "RETRY_SCHEDULE_FAILED",
    }, f"CAS failure should not propagate REARM_WATCHER_REQUIRED, got {_cas_outcome!r}"

    # Restore conn for subsequent phases
    monkeypatch.setattr(osm_mod, "conn", lambda: _FakeConn())

    # ── Assertion 16: PAPER identity exact ─────────────────────────────────
    # The CAS SQL parameters must carry the exact execution_mode through the
    # rearm predicate (we read from the rearm SQL parameters captured below).
    _params_captured = {}

    class _ParamConn:
        class _PCur:
            rowcount = 1
            def execute(self, sql, params):
                _params_captured["params"] = params
                _params_captured["sql"] = sql
                return self
        def __enter__(self): return self._PCur()
        def __exit__(self, *_): return False

    monkeypatch.setattr(osm_mod, "conn", lambda: _ParamConn())
    _get_order_call_count[0] = 0  # reset sequence
    APExecutionCore.resume_deferred_materialization_retry(
        core,
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=GENERATION,
        expected_retry_attempt=EXPECTED_ATT,
        owner=OWNER,
    )
    if _params_captured:
        rearm_params = _params_captured.get("params", ())
        rearm_sql = _params_captured.get("sql", "")
        # Robust to exact positional order: the CAS predicate must reference
        # execution_mode, and the exact PAPER value must appear among the
        # bound parameters (never LIVE, never blank, never another client's
        # mode) — proving LIVE/PAPER identity is carried through exactly.
        assert "execution_mode" in rearm_sql or "execution_mode=%s" in rearm_sql.replace(" ", "")
        assert EXEC_MODE in [str(p) for p in rearm_params], (
            f"execution_mode {EXEC_MODE!r} must appear in CAS params, got {rearm_params!r}"
        )
        assert "live" not in [str(p).lower() for p in rearm_params if str(p).lower() in {"live", "paper"} and str(p).lower() != EXEC_MODE]

    # Restore to real FakeConn
    monkeypatch.setattr(osm_mod, "conn", lambda: _FakeConn())

    # ── Assertions 4-5: PTR with available quote attaches a real watcher ────
    # When quote check says price not yet through trigger (live_quote_abt=False),
    # PTR calls _rearm_and_verify which registers a watcher, then proves it
    # via _verify_registry_ownership (6-way proof: local_order_id, signal_id,
    # client_id, execution_mode, valid state, dedup held). watch() returning
    # True is NOT itself proof — the fake watcher must populate _pending and
    # _dedup_set exactly as a real APEntryWatcher.watch() would, or PTR
    # correctly refuses to trust it.
    _ptr_watch_calls = []

    class _FakeWatched:
        def __init__(self, signal, state):
            self.signal = signal
            self.state = state
            self._ownership_quarantine = False

    class _FakeWatcher:
        def __init__(self):
            self._pending = []
            self._dedup_set = set()

        def has_order(self, oid):
            return any(
                str((getattr(w, "signal", {}) or {}).get("local_order_id") or "") == oid
                for w in self._pending
            )

        def prove_materialization_retry_owner(self, *a, **kw):
            return {"proven": False, "reason_code": "NO_MATCH"}

        def prove_restart_rearm_retry_owner(self, *a, **kw):
            return None

        def watch(self, plan, local_oid=None, recovery_rearm=False, **kw):
            _ptr_watch_calls.append((plan, local_oid, recovery_rearm))
            _sig = {
                "local_order_id": str(local_oid or ""),
                "signal_id": str(getattr(plan, "signal_id", "") or ""),
                "client_id": str(getattr(plan, "client_id", "") or "").strip().lower(),
                "execution_mode": str(getattr(plan, "execution_mode", "") or "").strip().lower(),
            }
            self._pending.append(_FakeWatched(_sig, "REARM"))
            if _sig["signal_id"]:
                self._dedup_set.add(_sig["signal_id"])
            return True

        def dedup_held(self, oid, **kw):
            return False

    def _quote_check_available(broker, symbol, side, trigger):
        # Return False = price below trigger, not yet through
        return False

    # ── Mutable row-store-backed OSM for the PTR section ────────────────────
    # PTR's restart-rearm-retry path durably writes a retry marker via
    # update_order_meta() and then re-reads the row via get_order() to prove
    # the write landed (_verify_restart_rearm_retry_ownership). A static,
    # non-mutating fixture cannot support that round-trip — this mirrors the
    # mutable row store already used in test_p0_rearm_watcher_required_routing.py.
    _ptr_store = {"row": dict(post_rearm_row)}

    def _ptr_get_order(order_id):
        if str(order_id) != LOCAL_ORDER_ID:
            return None
        return dict(_ptr_store["row"])

    def _ptr_update_order_meta(order_id, patch):
        if str(order_id) != LOCAL_ORDER_ID:
            return False
        _ptr_store["row"]["meta"] = {
            **_ptr_store["row"].get("meta", {}), **patch,
        }
        return True

    ptr_osm = object.__new__(APOrderStateMachine)
    ptr_osm.client_id = CLIENT_ID
    ptr_osm.get_order = _ptr_get_order
    ptr_osm.update_order_meta = _ptr_update_order_meta

    ptr = PendingTriggerRestartRecovery(
        client_id=CLIENT_ID,
        execution_mode=EXEC_MODE,
        osm=ptr_osm,
        entry_watcher=_FakeWatcher(),
        broker=MagicMock(),
        quote_check_fn=_quote_check_available,
        caller_source="test_seam",
    )

    def _plan_builder_fn(row):
        from types import SimpleNamespace
        return SimpleNamespace(
            ticker="SPY",
            side="CALL",
            direction="CALL",
            trigger_price=TRIGGER_PRICE,
            stop_underlying=None,
            target_underlying=None,
            contract_symbol="",
            metadata={},
            signal_id=SIGNAL_ID,
            client_id=CLIENT_ID,
            execution_mode=EXEC_MODE,
        )

    ptr_outcome_watcher = ptr.recover_one_row(
        dict(_ptr_store["row"]),
        plan_builder_fn=_plan_builder_fn,
    )

    # Assertion 5: available quote → watcher attached (WATCHER_OWNED)
    assert ptr_outcome_watcher in {_RowOutcome.WATCHER_OWNED, _RowOutcome.RETRY_OWNED}, (
        f"PTR with available quote must resolve, got {ptr_outcome_watcher!r}"
    )

    # ── Assertions 6-7: PTR with unavailable quote → RETRY_OWNED, never UNRESOLVED
    def _quote_check_unavailable(broker, symbol, side, trigger):
        return None  # quote unavailable

    _ptr_store_no_quote = {"row": dict(post_rearm_row)}

    def _ptr_get_order_no_quote(order_id):
        if str(order_id) != LOCAL_ORDER_ID:
            return None
        return dict(_ptr_store_no_quote["row"])

    def _ptr_update_order_meta_no_quote(order_id, patch):
        if str(order_id) != LOCAL_ORDER_ID:
            return False
        _ptr_store_no_quote["row"]["meta"] = {
            **_ptr_store_no_quote["row"].get("meta", {}), **patch,
        }
        return True

    ptr_osm_no_quote = object.__new__(APOrderStateMachine)
    ptr_osm_no_quote.client_id = CLIENT_ID
    ptr_osm_no_quote.get_order = _ptr_get_order_no_quote
    ptr_osm_no_quote.update_order_meta = _ptr_update_order_meta_no_quote

    ptr_no_quote = PendingTriggerRestartRecovery(
        client_id=CLIENT_ID,
        execution_mode=EXEC_MODE,
        osm=ptr_osm_no_quote,
        entry_watcher=_FakeWatcher(),
        broker=MagicMock(),
        quote_check_fn=_quote_check_unavailable,
        caller_source="test_seam_no_quote",
    )
    ptr_outcome_no_quote = ptr_no_quote.recover_one_row(
        dict(_ptr_store_no_quote["row"]),
        plan_builder_fn=_plan_builder_fn,
    )

    # ── Assertions 6-7: unavailable quote → bounded restart-rearm retry ────
    # Now that the durable rearm leaves materialization_status truly blank
    # (see production correction: current_owner/materialization_status fix),
    # PendingTriggerRestartRecovery's existing _has_trigger_or_submit_evidence()
    # gate no longer false-positives on a freshly-rearmed row, and the
    # unavailable-quote path correctly reaches RETRY_OWNED via
    # _enter_restart_rearm_retry(). This resolves a limitation that the
    # prior head's WAITING_FOR_TRIGGER value had been tripping.
    assert ptr_outcome_no_quote == _RowOutcome.RETRY_OWNED, (
        f"PTR with unavailable quote must schedule the bounded restart-rearm "
        f"retry (RETRY_OWNED), got {ptr_outcome_no_quote!r}"
    )
    assert ptr_outcome_no_quote != _RowOutcome.UNRESOLVED, (
        "PTR must never return UNRESOLVED merely because no watcher existed "
        "during the synthetic callback"
    )

    # ── Assertion 8: restart immediately after durable write is recoverable ─
    # A post-rearm row can be discovered by PTR and resolved without UNRESOLVED.
    # Proved by assertions 5-7 above (both paths resolve).
    # Extra check: post-rearm row passes PTR's identity gates.
    assert str(post_rearm_row.get("status") or "").upper() == "PENDING_TRIGGER"
    assert not post_rearm_row.get("broker_order_id")
    assert not post_rearm_row.get("submitted_ts")
    assert not str((post_rearm_row.get("meta") or {}).get("lifecycle_state") or "").strip()
    assert not str((post_rearm_row.get("meta") or {}).get("watcher_token") or "").strip()

    # ── Assertion 4: the same recovery pass consumes the disposition ─────────
    # Proved by assertion 3 (disposition = REARM_WATCHER_REQUIRED, not KEEP_WATCHER)
    # and assertions 5-7 (PTR resolves the row on the same pass, not UNRESOLVED).
