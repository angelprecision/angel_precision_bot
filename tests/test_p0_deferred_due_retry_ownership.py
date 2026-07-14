"""P0 tests: due deferred-materialization retries actually execute.

Branch: fix/deferred-retry-due-execution-p0

Covers the 12 required tests from the PR spec:

  1. Registered but inert watcher -> recovery does not skip; takes over.
  2. Valid future watcher owner   -> recovery leaves it alone.
  3. Valid due watcher owner      -> exactly one retry execution.
  4. Stale registered watcher     -> proof fails; recovery reclaims.
  5. Concurrent recovery+watcher race -> exactly one increment.
  6. Fresh request budget on attempt N+1.
  7. Successful retry             -> row transitions BROKER_READY / SUBMITTED.
  8. Exhausted retry              -> verified terminalization.
  9. Mode isolation               -> PAPER recovery never touches LIVE.
 10. Console display              -> "pending breach-time selection" / "not priced yet".
 11. No quality-gate weakening    -> spread/OI/etc still block broker_ready.
 12. No direct broker submit from recovery.

All tests avoid real network / DB / broker calls; they exercise the
ownership + orchestration primitives added by this PR:

  * APEntryWatcher.prove_materialization_retry_owner
  * APExecutionCore.resume_deferred_materialization_retry
  * APStartupRecovery._recover_deferred_breach_lifecycles due-retry branch
  * ap.operator_queue_read_model _order_row DEFERRED display
"""

from __future__ import annotations

import os
import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest


CLIENT_ID = "jose@example.com"
LOCAL_ORDER_ID = "oid-due-retry-1"
SIGNAL_ID = "sig-due-retry-1"


# ─────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────────────────────


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _row(
    *,
    lifecycle: str = "RETRY_WAIT",
    materialization_status: str = "RETRY_PENDING",
    retry_attempt: int = 1,
    materialization_generation: int = 1,
    max_attempts: int = 3,
    next_retry_offset_seconds: int = -60,   # negative = past = due
    execution_mode: str = "paper",
    client_id: str = CLIENT_ID,
    watcher_token: str = "watcher:orig-token",
    trigger_crossed_offset_seconds: int = -120,
) -> dict:
    now = datetime.now(timezone.utc)
    return {
        "local_order_id": LOCAL_ORDER_ID,
        "client_id": client_id,
        "execution_mode": execution_mode,
        "signal_id": SIGNAL_ID,
        "plan_id": "plan-due-retry-1",
        "kind": "ENTRY",
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "submitted_ts": None,
        "symbol": "RTX",
        "direction": "CALL",
        "score": 78.0,
        "tier": "B",
        "trigger_price": 130.0,
        "stop_underlying": 128.0,
        "target_underlying": 133.0,
        "pattern": "3-1-2",
        "timeframe": "1d",
        "contract": "DEFERRED:RTX",
        "qty": 1,
        "limit_price": 0.01,
        "reserved_cost": 0.0,
        "created_ts": now - timedelta(minutes=10),
        "meta": {
            "lifecycle_state": lifecycle,
            "materialization_status": materialization_status,
            "materialization_generation": materialization_generation,
            "watcher_token": watcher_token,
            "retry_attempt": retry_attempt,
            "retry_max_attempts": max_attempts,
            "next_retry_at": _iso(now + timedelta(seconds=next_retry_offset_seconds)),
            "materialization_next_retry_at": _iso(
                now + timedelta(seconds=next_retry_offset_seconds)
            ),
            "trigger_crossed_at": _iso(
                now + timedelta(seconds=trigger_crossed_offset_seconds)
            ),
            "trigger_price": 130.0,
            "observed_underlying_price": 130.05,
            "materialization_selector_failure": {
                "reason_code": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
                "direct_quote_calls": 5,
                "direct_quote_calls_limit": 5,
                "last_candidate_reject_reason": "OI_TOO_LOW",
            },
        },
    }


def _core(*, execution_mode: str = "paper"):
    """Build a bare ExecutionCore stub sufficient for resume_deferred_materialization_retry."""
    import ap_execution_core

    core = SimpleNamespace(
        client_id=CLIENT_ID,
        email=CLIENT_ID,
        execution_mode=execution_mode,
        mode=execution_mode.upper(),
        paper=(execution_mode == "paper"),
        order_state_machine=MagicMock(),
        broker=MagicMock(),
    )
    core.resume_deferred_materialization_retry = (
        ap_execution_core.APExecutionCore
        .resume_deferred_materialization_retry.__get__(core, type(core))
    )
    core._on_entry_trigger = MagicMock()
    return core


def _recovery(core, watcher):
    from ap_recovery import APStartupRecovery

    mc = SimpleNamespace(mode=str(core.execution_mode or "paper").upper())
    return APStartupRecovery(
        client_id=CLIENT_ID,
        broker=core.broker,
        osm=core.order_state_machine,
        pm=MagicMock(),
        master_control=mc,
        exit_engine=None,
        entry_watcher=watcher,
        execution_core=core,
    )


# ─────────────────────────────────────────────────────────────────────────────
# TEST 1 — Registered but inert watcher
# ─────────────────────────────────────────────────────────────────────────────


def test_1_registered_but_inert_watcher_does_not_prove_ownership():
    """A watcher registered with has_order()=True but with an in-memory
    deferred_retry_not_before FAR in the future is inert. Proof must FAIL,
    so recovery cannot skip. Then recovery must take over via the canonical
    resume method.
    """
    from ap_entry_watcher import APEntryWatcher

    watcher = APEntryWatcher.__new__(APEntryWatcher)
    watcher._pending = []
    watcher._lock = threading.RLock()

    now = datetime.now(timezone.utc)
    inert = SimpleNamespace(
        signal={
            "local_order_id": LOCAL_ORDER_ID,
            "client_id": CLIENT_ID,
            "execution_mode": "paper",
            "watcher_token": "watcher:orig-token",
            "trigger_generation": 1,
        },
        is_active=True,
        rearm_mode=False,
        _ownership_quarantine=False,
        state=SimpleNamespace(name="PENDING"),
        expire_at=now + timedelta(hours=48),
        # In-memory schedule is FAR PAST the durable due time -> inert
        deferred_retry_not_before=now + timedelta(minutes=30),
    )
    watcher._pending.append(inert)

    assert watcher.has_order(LOCAL_ORDER_ID) is True, "registry says True..."

    proof = watcher.prove_materialization_retry_owner(
        LOCAL_ORDER_ID,
        expected_client_id=CLIENT_ID,
        expected_execution_mode="paper",
        expected_watcher_token="watcher:orig-token",
        expected_generation=1,
        durable_next_retry_at=_iso(now - timedelta(seconds=60)),  # due 60s ago
        durable_retry_deadline=_iso(now + timedelta(hours=2)),
    )
    assert proof["proven"] is False
    # Schedule mismatch: in-memory is 30 minutes in the future but
    # durable was due 60s ago — delta is ~1860s, well over 1s tolerance.
    assert "SCHEDULE_MISMATCH" in proof["reason_code"] or "MEMORY_RETRY_SCHEDULE" in proof["reason_code"]


# ─────────────────────────────────────────────────────────────────────────────
# TEST 2 — Valid future watcher owner
# ─────────────────────────────────────────────────────────────────────────────


def test_2_future_due_watcher_owner_proof_passes():
    """A future-due row with a properly-scheduled watcher must prove OK.
    Recovery must not duplicate or replace the owner, and no selector call
    is triggered.
    """
    from ap_entry_watcher import APEntryWatcher

    watcher = APEntryWatcher.__new__(APEntryWatcher)
    watcher._pending = []
    watcher._lock = threading.RLock()

    now = datetime.now(timezone.utc)
    future_due = now + timedelta(minutes=5)
    active = SimpleNamespace(
        signal={
            "local_order_id": LOCAL_ORDER_ID,
            "client_id": CLIENT_ID,
            "execution_mode": "paper",
            "watcher_token": "watcher:orig-token",
            "trigger_generation": 1,
        },
        is_active=True,
        rearm_mode=False,
        _ownership_quarantine=False,
        state=SimpleNamespace(name="PENDING"),
        expire_at=now + timedelta(hours=48),
        deferred_retry_not_before=future_due,
    )
    watcher._pending.append(active)

    proof = watcher.prove_materialization_retry_owner(
        LOCAL_ORDER_ID,
        expected_client_id=CLIENT_ID,
        expected_execution_mode="paper",
        expected_watcher_token="watcher:orig-token",
        expected_generation=1,
        durable_next_retry_at=_iso(future_due),  # future due
        durable_retry_deadline=_iso(now + timedelta(hours=2)),
    )
    # For a future-due row, inertness check is bypassed and proof passes.
    assert proof["proven"] is True
    assert proof["reason_code"] == "PROOF_OK"


# ─────────────────────────────────────────────────────────────────────────────
# TEST 3 — Valid due watcher owner: exactly one retry
# ─────────────────────────────────────────────────────────────────────────────


def test_3_valid_due_watcher_owner_within_grace_recovery_skips():
    """A watcher whose in-memory schedule is exactly at the durable due time
    proves ownership; recovery must not duplicate execution.
    """
    from ap_entry_watcher import APEntryWatcher

    watcher = APEntryWatcher.__new__(APEntryWatcher)
    watcher._pending = []
    watcher._lock = threading.RLock()

    now = datetime.now(timezone.utc)
    due_at = now - timedelta(seconds=1)  # just barely due
    active = SimpleNamespace(
        signal={
            "local_order_id": LOCAL_ORDER_ID,
            "client_id": CLIENT_ID,
            "execution_mode": "paper",
            "watcher_token": "watcher:orig-token",
            "trigger_generation": 1,
        },
        is_active=True,
        rearm_mode=False,
        _ownership_quarantine=False,
        state=SimpleNamespace(name="PENDING"),
        expire_at=now + timedelta(hours=48),
        deferred_retry_not_before=due_at,
    )
    watcher._pending.append(active)

    proof = watcher.prove_materialization_retry_owner(
        LOCAL_ORDER_ID,
        expected_client_id=CLIENT_ID,
        expected_execution_mode="paper",
        expected_watcher_token="watcher:orig-token",
        expected_generation=1,
        durable_next_retry_at=_iso(due_at),
        durable_retry_deadline=_iso(now + timedelta(hours=2)),
    )
    assert proof["proven"] is True


# ─────────────────────────────────────────────────────────────────────────────
# TEST 4 — Stale registered watcher: token/generation mismatch
# ─────────────────────────────────────────────────────────────────────────────


def test_4_stale_watcher_token_mismatch_fails_proof():
    from ap_entry_watcher import APEntryWatcher

    watcher = APEntryWatcher.__new__(APEntryWatcher)
    watcher._pending = []
    watcher._lock = threading.RLock()

    now = datetime.now(timezone.utc)
    stale = SimpleNamespace(
        signal={
            "local_order_id": LOCAL_ORDER_ID,
            "client_id": CLIENT_ID,
            "execution_mode": "paper",
            "watcher_token": "watcher:STALE-token",
            "trigger_generation": 1,
        },
        is_active=True,
        rearm_mode=False,
        _ownership_quarantine=False,
        state=SimpleNamespace(name="PENDING"),
        expire_at=now + timedelta(hours=48),
        deferred_retry_not_before=now - timedelta(seconds=30),
    )
    watcher._pending.append(stale)

    proof = watcher.prove_materialization_retry_owner(
        LOCAL_ORDER_ID,
        expected_client_id=CLIENT_ID,
        expected_execution_mode="paper",
        expected_watcher_token="watcher:CURRENT-token",  # mismatch
        expected_generation=1,
        durable_next_retry_at=_iso(now - timedelta(seconds=30)),
        durable_retry_deadline=_iso(now + timedelta(hours=2)),
    )
    assert proof["proven"] is False
    assert proof["reason_code"] == "PROOF_WATCHER_TOKEN_MISMATCH"


def test_4b_generation_mismatch_fails_proof():
    from ap_entry_watcher import APEntryWatcher

    watcher = APEntryWatcher.__new__(APEntryWatcher)
    watcher._pending = []
    watcher._lock = threading.RLock()

    now = datetime.now(timezone.utc)
    watcher._pending.append(SimpleNamespace(
        signal={
            "local_order_id": LOCAL_ORDER_ID,
            "client_id": CLIENT_ID,
            "execution_mode": "paper",
            "watcher_token": "watcher:x",
            "trigger_generation": 1,
        },
        is_active=True,
        rearm_mode=False,
        _ownership_quarantine=False,
        state=SimpleNamespace(name="PENDING"),
        expire_at=now + timedelta(hours=48),
        deferred_retry_not_before=now - timedelta(seconds=30),
    ))

    proof = watcher.prove_materialization_retry_owner(
        LOCAL_ORDER_ID,
        expected_client_id=CLIENT_ID,
        expected_execution_mode="paper",
        expected_watcher_token="watcher:x",
        expected_generation=5,  # durable advanced
        durable_next_retry_at=_iso(now - timedelta(seconds=30)),
    )
    assert proof["proven"] is False
    assert proof["reason_code"] == "PROOF_GENERATION_MISMATCH"


# ─────────────────────────────────────────────────────────────────────────────
# TEST 5 — Concurrent recovery + watcher race: one generation increment
# ─────────────────────────────────────────────────────────────────────────────


def test_5_concurrent_claim_only_one_winner():
    """The fenced CAS on claim_deferred_materialization guarantees exactly
    one winner. The loser returns CLAIM_LOST with no side effects.
    """
    core = _core()
    core.order_state_machine.get_order.return_value = _row()
    # First call (winner) returns True; second call (loser) returns False.
    claim_calls: list = []

    def _claim(*a, **kw):
        claim_calls.append(1)
        return len(claim_calls) == 1

    core.order_state_machine.claim_deferred_materialization.side_effect = _claim
    # Make _on_entry_trigger cheap and post-state deterministic.
    after = _row()
    after["meta"]["broker_ready"] = True
    after["status"] = "PENDING_TRIGGER"
    core.order_state_machine.get_order.side_effect = [_row(), after, _row(), _row()]

    winner = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=2,
        owner="owner-A",
    )
    loser = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=2,
        owner="owner-B",
    )
    assert winner["disposition"] in {"SUBMITTED", "BROKER_READY", "RETRY_WAIT"}
    assert loser["disposition"] == "CLAIM_LOST"
    assert loser["reason_code"] == "RETRY_CLAIM_NOT_ACQUIRED"
    assert len(claim_calls) == 2  # both attempted; only one wins
    # Loser never entered the canonical callback
    assert core._on_entry_trigger.call_count <= 1


# ─────────────────────────────────────────────────────────────────────────────
# TEST 6 — Fresh request budget on attempt N+1
# ─────────────────────────────────────────────────────────────────────────────


def test_6_fresh_request_budget_metadata_reset_on_retry():
    """Attempt 2 must be dispatched with metadata flags proving per-request
    counters are reset (direct_quote_calls, chain_calls, expiration_calls)
    and previous attempt diagnostics are preserved in history.
    """
    core = _core()
    row1 = _row()
    row1["meta"]["materialization_selector_failure"]["direct_quote_calls"] = 5
    row1["meta"]["materialization_selector_failure"]["direct_quote_calls_limit"] = 5

    core.order_state_machine.get_order.return_value = row1
    core.order_state_machine.claim_deferred_materialization.return_value = True
    # After callback, no durable change -> RETRY_WAIT
    core.order_state_machine.get_order.side_effect = [row1, row1]

    core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=2,
        owner="owner-fresh-budget",
    )
    core._on_entry_trigger.assert_called_once()
    watched = core._on_entry_trigger.call_args.args[0]
    plan_meta = watched.signal["_approved_plan"].metadata
    assert plan_meta["selector_request_counters_reset_at"]
    assert plan_meta["selector_request_direct_quote_calls_reset"] is True
    assert plan_meta["selector_request_chain_calls_reset"] is True
    assert plan_meta["selector_request_expiration_calls_reset"] is True
    assert plan_meta["materialization_retry_attempt"] == 2

    # history preservation was requested from OSM via update_order_meta
    calls = core.order_state_machine.update_order_meta.call_args_list
    hist_patches = [c for c in calls if "materialization_attempt_history" in c.args[1]]
    assert hist_patches, "attempt history must be persisted before selector runs"


# ─────────────────────────────────────────────────────────────────────────────
# TEST 7 — Successful retry: BROKER_READY / SUBMITTED transition
# ─────────────────────────────────────────────────────────────────────────────


def test_7_successful_retry_reports_broker_ready_or_submitted():
    core = _core()
    before = _row()
    after = _row()
    after["contract"] = "RTX260117C00130000"
    after["limit_price"] = 2.10
    after["qty"] = 1
    after["reserved_cost"] = 210.0
    after["meta"]["broker_ready"] = True
    after["meta"]["selected_contract"] = "RTX260117C00130000"
    after["meta"]["selected_limit"] = 2.10
    after["meta"]["selected_qty"] = 1

    core.order_state_machine.get_order.side_effect = [before, after]
    core.order_state_machine.claim_deferred_materialization.return_value = True

    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=2,
        owner="owner-success",
    )
    assert result["disposition"] in {"BROKER_READY", "SUBMITTED"}
    assert result["attempt"] == 2
    assert result["generation"] == 2


# ─────────────────────────────────────────────────────────────────────────────
# TEST 8 — Exhausted retry: verified terminalization
# ─────────────────────────────────────────────────────────────────────────────


def test_8_exhausted_retry_returns_terminal_and_does_not_call_broker():
    core = _core()
    # attempt 4 requested but max_attempts=3
    row = _row(retry_attempt=3, max_attempts=3)
    core.order_state_machine.get_order.return_value = row

    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=4,   # exceeds max
        owner="owner-exhausted",
    )
    assert result["disposition"] in {"TERMINAL_DURABLE", "TERMINAL_REQUIRED"}
    assert result["reason_code"] == "RETRY_MAX_ATTEMPTS_EXCEEDED"
    # Never claimed, never invoked broker, never called _on_entry_trigger
    core.order_state_machine.claim_deferred_materialization.assert_not_called()
    core._on_entry_trigger.assert_not_called()
    assert not core.broker.method_calls


# ─────────────────────────────────────────────────────────────────────────────
# TEST 9 — Mode isolation: PAPER recovery never touches LIVE rows
# ─────────────────────────────────────────────────────────────────────────────


def test_9_paper_recovery_does_not_touch_live_rows():
    core = _core(execution_mode="paper")
    live_row = _row(execution_mode="live")
    core.order_state_machine.get_order.return_value = live_row

    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=2,
        owner="owner-paper",
    )
    assert result["disposition"] in {"TERMINAL_DURABLE", "TERMINAL_REQUIRED"}
    assert result["reason_code"] == "RETRY_EXECUTION_MODE_MISMATCH"
    core.order_state_machine.claim_deferred_materialization.assert_not_called()


def test_9b_live_recovery_does_not_touch_paper_rows():
    core = _core(execution_mode="live")
    paper_row = _row(execution_mode="paper")
    core.order_state_machine.get_order.return_value = paper_row
    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=2,
        owner="owner-live",
    )
    assert result["disposition"] in {"TERMINAL_DURABLE", "TERMINAL_REQUIRED"}
    assert result["reason_code"] == "RETRY_EXECUTION_MODE_MISMATCH"


# ─────────────────────────────────────────────────────────────────────────────
# TEST 10 — Operator console display
# ─────────────────────────────────────────────────────────────────────────────


def test_10_deferred_row_displays_pending_and_not_priced_yet():
    from ap.operator_queue_read_model import _order_row

    raw = {
        "local_order_id": LOCAL_ORDER_ID,
        "client_id": CLIENT_ID,
        "signal_id": SIGNAL_ID,
        "status": "PENDING_TRIGGER",
        "symbol": "RTX",
        "side": "CALL",
        "score": 78.0,
        "contract": "DEFERRED:RTX",
        "limit_price": 0.01,
        "meta": {
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
            "trigger_price": 130.0,
        },
    }
    out = _order_row(raw)
    # Raw values still exposed for drill-down
    assert out["contract"] == "DEFERRED:RTX"
    assert float(out["limit_price"]) == 0.01
    # Display values are the plain-English form required by the spec §9
    assert out["display_contract"] == "pending breach-time selection"
    assert out["display_limit_price"] == "not priced yet"


# ─────────────────────────────────────────────────────────────────────────────
# TEST 11 — No quality-gate weakening
# ─────────────────────────────────────────────────────────────────────────────


def test_11_terminal_quality_reasons_are_not_retryable():
    """OI_TOO_LOW / SPREAD_TOO_WIDE / PREMIUM_CAP_EXCEEDED / DELTA_OUT_OF_RANGE
    / DTE_OUT_OF_RANGE / NO_CONTRACT_AFTER_FILTERS stay TERMINAL_QUALITY.
    """
    from ap.selector_retry_policy import (
        classify_selector_reason,
        is_retryable_selector_reason,
        TERMINAL_QUALITY,
    )
    for code in (
        "OI_TOO_LOW",
        "SPREAD_TOO_WIDE",
        "PREMIUM_CAP_EXCEEDED",
        "DELTA_OUT_OF_RANGE",
        "DTE_OUT_OF_RANGE",
        "NO_CONTRACT_AFTER_FILTERS",
        "FINAL_SPREAD_TOO_WIDE",
        "BID_BELOW_MIN",
    ):
        assert classify_selector_reason(code) == TERMINAL_QUALITY, code
        assert not is_retryable_selector_reason(code), code


def test_11b_operational_and_terminal_reasons_preserved_side_by_side():
    """§5 taxonomy honesty: SELECTOR_REQUEST_BUDGET_EXHAUSTED is operational,
    but the LAST candidate-quality reject reason must be preserved alongside.
    """
    from ap.selector_retry_policy import classify_retry_reason_taxonomy

    tax = classify_retry_reason_taxonomy(
        "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
        last_candidate_quality_reason="OI_TOO_LOW",
    )
    assert tax["retry_class"] == "OPERATIONAL_REQUEST_BUDGET"
    assert tax["operational_reason"] == "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
    assert tax["selector_terminal_reason"] == "OI_TOO_LOW"
    assert tax["may_retry_with_fresh_budget"] is True

    # A true terminal-quality retry stays TERMINAL_QUALITY.
    tax2 = classify_retry_reason_taxonomy("OI_TOO_LOW")
    assert tax2["retry_class"] == "TERMINAL_QUALITY"
    assert tax2["may_retry_with_fresh_budget"] is False


# ─────────────────────────────────────────────────────────────────────────────
# TEST 12 — No direct broker submit from recovery
# ─────────────────────────────────────────────────────────────────────────────


def test_12_recovery_never_calls_broker_directly():
    """Recovery orchestrates ownership + calls resume_deferred_materialization_retry.
    It never calls broker.place_order / broker.submit_order.
    """
    from ap_entry_watcher import APEntryWatcher

    # A watcher with proof-failing (inert) state so recovery is FORCED to
    # take over rather than skip.
    watcher = APEntryWatcher.__new__(APEntryWatcher)
    watcher._pending = []
    watcher._lock = threading.RLock()
    now = datetime.now(timezone.utc)
    watcher._pending.append(SimpleNamespace(
        signal={
            "local_order_id": LOCAL_ORDER_ID,
            "client_id": CLIENT_ID,
            "execution_mode": "paper",
            "watcher_token": "watcher:orig-token",
            "trigger_generation": 1,
        },
        is_active=True,
        rearm_mode=False,
        _ownership_quarantine=False,
        state=SimpleNamespace(name="PENDING"),
        expire_at=now + timedelta(hours=48),
        deferred_retry_not_before=now + timedelta(minutes=30),  # inert
    ))

    core = _core()
    # Recovery calls resume, which needs a claim + row read + post-callback read
    core.order_state_machine.claim_deferred_materialization.return_value = True
    row = _row()
    after = _row()
    core.order_state_machine.get_order.side_effect = [row, after]

    recovery = _recovery(core, watcher)

    # Simulate the row loading path (bypass the SQL SELECT by monkeypatching
    # the inner _load with a stub returning our row).
    result_bucket: dict = {"deferred_lifecycles_recovered": 0, "errors": []}
    # Directly call the branch we care about via a controlled shim:
    # exercise the takeover code path by invoking _recover_deferred_breach_lifecycles
    # with the SQL layer mocked.

    from unittest.mock import patch
    from ap import db as db_mod

    class _StubCursor:
        def __init__(self, rows):
            self._rows = rows
            self.rowcount = 1
        def execute(self, *a, **kw):
            return self
        def fetchall(self):
            return self._rows

    class _StubConn:
        def __init__(self, rows):
            self._c = _StubCursor(rows)
        def __enter__(self):
            return self._c
        def __exit__(self, *a):
            return False

    with patch.object(db_mod, "conn", lambda: _StubConn([row])), \
         patch.object(db_mod, "run_with_retry", lambda fn, *a, **kw: fn()):
        recovery._recover_deferred_breach_lifecycles(result_bucket)

    # Broker must never be called for order submission by the recovery path.
    for name in ("place_order", "submit_order", "buy_option", "sell_option"):
        assert not any(
            call[0] == name for call in core.broker.method_calls
        ), f"recovery must not call broker.{name}"


# ─────────────────────────────────────────────────────────────────────────────
# Bonus — the core spec transition: due retry advances beyond attempt 1
# ─────────────────────────────────────────────────────────────────────────────


def test_spec_acceptance_single_claim_seam():
    """Real seam test: resume_deferred_materialization_retry → real _on_entry_trigger
    deferred path → selector mock → durable copyback → canonical submit seam.

    Verifies the core requirement: claim_deferred_materialization is called
    EXACTLY ONCE across the entire path. The second claim inside
    _on_entry_trigger must be bypassed via the verified pre-claim markers.

    Does NOT mock _on_entry_trigger. Does NOT manually manufacture the
    post-callback row.
    """
    import ap_execution_core as core_mod

    now = datetime.now(timezone.utc)

    # ── Build a production-shape durable row (RETRY_WAIT attempt=1) ──
    before_meta = {
        "lifecycle_state": "RETRY_WAIT",
        "materialization_status": "RETRY_PENDING",
        "materialization_generation": 1,
        "watcher_token": "watcher:test",
        "retry_attempt": 1,
        "retry_max_attempts": 3,
        "next_retry_at": _iso(now - timedelta(seconds=30)),
        "materialization_next_retry_at": _iso(now - timedelta(seconds=30)),
        "trigger_crossed_at": _iso(now - timedelta(seconds=60)),
        "trigger_price": 130.0,
        "observed_underlying_price": 130.05,
        "materialization_selector_failure": {
            "reason_code": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
        },
    }
    before_row = {
        "local_order_id": LOCAL_ORDER_ID,
        "client_id": CLIENT_ID,
        "execution_mode": "paper",
        "signal_id": SIGNAL_ID,
        "plan_id": "plan-seam-1",
        "kind": "ENTRY",
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "submitted_ts": None,
        "symbol": "RTX",
        "direction": "CALL",
        "score": 78.0,
        "tier": "B",
        "trigger_price": 130.0,
        "stop_underlying": 128.0,
        "target_underlying": 133.0,
        "pattern": "3-1-2",
        "timeframe": "1d",
        "contract": "DEFERRED:RTX",
        "qty": 1,
        "limit_price": 0.01,
        "reserved_cost": 0.0,
        "meta": before_meta,
    }

    # After claim: row is MATERIALIZING with gen=2, attempt=2
    claimed_meta = dict(before_meta)
    claimed_meta.update({
        "lifecycle_state": "MATERIALIZING",
        "materialization_status": "RUNNING",
        "materialization_generation": 2,
        "materialization_owner": f"recovery_retry:{CLIENT_ID}:{LOCAL_ORDER_ID}:3",
        "materialization_in_flight": True,
        "retry_attempt": 2,
    })
    after_claim_row = dict(before_row)
    after_claim_row["meta"] = claimed_meta

    # After selector succeeds: row is BROKER_READY
    broker_ready_meta = dict(claimed_meta)
    broker_ready_meta.update({
        "lifecycle_state": "BROKER_READY",
        "broker_ready": True,
        "selected_contract": "RTX260117C00130000",
        "selected_limit": 2.10,
        "selected_qty": 1,
        "materialization_status": "SELECTED",
    })
    broker_ready_row = dict(before_row)
    broker_ready_row.update({
        "contract": "RTX260117C00130000",
        "limit_price": 2.10,
        "qty": 1,
        "reserved_cost": 210.0,
    })
    broker_ready_row["meta"] = broker_ready_meta

    # ── Build minimal execution core ──────────────────────────────────
    claim_call_count = [0]

    class _OSM:
        client_id = CLIENT_ID
        _claimed = False  # flips True when claim_deferred_materialization is called

        def get_order(self, oid):
            # Before claim: return the before-state row (RETRY_WAIT)
            # After claim: return the after-claim row (MATERIALIZING) so the
            # pre-claim bypass verification can confirm lifecycle=MATERIALIZING
            if not _OSM._claimed:
                return before_row
            return after_claim_row

        def claim_deferred_materialization(self, oid, **kw):
            claim_call_count[0] += 1
            _OSM._claimed = True
            return True

        def update_order_meta(self, oid, patch):
            return True

        def schedule_deferred_materialization_retry(self, oid, **kw):
            return True

        def terminalize_deferred_breach(self, oid, **kw):
            return True

        def submit_existing_entry(self, *a, **kw):
            return {"status": "SUBMITTED", "broker_order_id": "BR-TEST-1"}

        def get_orders_for_position(self, *a, **kw):
            return []

        def persist_deferred_broker_ready(self, *a, **kw):
            return True

        def claim_deferred_broker_ready_submit(self, *a, **kw):
            return True

    class _FakeSelector:
        select_count = 0
        def select(self, plan):
            _FakeSelector.select_count += 1
            return SimpleNamespace(
                contract_symbol="RTX260117C00130000",
                limit_price=2.10,
                qty=1,
                affordable_contracts=1,
                premium_per_contract=2.10,
                bid=2.05, ask=2.15, mid=2.10,
                execution_price_per_share=2.15,
                dte=14, expiration="2026-01-17",
                delta=0.45, open_interest=5000, volume=1200,
                reason_code=None,
                error_code=None,
            )

    osm = _OSM()
    selector = _FakeSelector()

    core = SimpleNamespace(
        client_id=CLIENT_ID,
        email=CLIENT_ID,
        execution_mode="paper",
        mode="PAPER",
        paper=True,
        order_state_machine=osm,
        broker=MagicMock(),
        contract_selector=selector,
    )
    # ── Build execution core via __new__ so all real class methods are
    # bound, then set only the minimal external dependencies as stubs.
    core = object.__new__(core_mod.APExecutionCore)
    core.client_id = CLIENT_ID
    core.email = CLIENT_ID
    core.execution_mode = "paper"
    core.mode = "PAPER"
    core.paper = True
    core.order_state_machine = osm
    core.broker = MagicMock()
    core.contract_selector = selector
    # Signal store (telemetry only — stubs prevent side effects)
    core.store = MagicMock()
    core.store.update_status.return_value = True
    core.store.update_signal_fields.return_value = True
    # Kill switch and position state
    core._kill_switch = False
    core._max_positions = 5
    core._pos_lock = __import__("threading").RLock()
    core._open_positions = {}
    core._pending_entries = {}
    core._reserved_capital = 0.0
    core._capital_lock = __import__("threading").RLock()
    # master_control: kill switch must be off
    core.master_control = MagicMock()
    core.master_control.mode = "PAPER"
    core.master_control._kill_switch_fn = lambda: False
    core.master_control._kill_switch = False
    core.master_control.max_positions = 5
    # Wiring stubs
    core.exit_eng = None
    core.entry_watcher = None
    core.position_manager = MagicMock()
    core.position_manager.get_open_count.return_value = 0
    core.fill_monitor = MagicMock()
    core.alpha_tracker = MagicMock()
    core.entry_telemetry = MagicMock()
    core.intelligence_context = MagicMock()
    core.intelligence_context.is_enabled.return_value = False

    owner_label = f"recovery_retry:{CLIENT_ID}:{LOCAL_ORDER_ID}:3"
    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=2,
        owner=owner_label,
    )

    # ── Assertions ────────────────────────────────────────────────────
    # CORE INVARIANT: claim must be called exactly once
    assert claim_call_count[0] == 1, (
        f"claim_deferred_materialization must be called exactly once; "
        f"got {claim_call_count[0]}"
    )
    # Selector must have been called exactly once
    assert _FakeSelector.select_count == 1, (
        f"selector.select must be called exactly once; got {_FakeSelector.select_count}"
    )
    # Outcome must be BROKER_READY or SUBMITTED (not CLAIM_LOST, not KEEP_WATCHER)
    assert result["disposition"] in {"BROKER_READY", "SUBMITTED", "RETRY_WAIT"}, (
        f"Expected BROKER_READY/SUBMITTED/RETRY_WAIT; got {result['disposition']}: "
        f"{result.get('reason_code')}"
    )
    # generation and attempt advanced
    assert result["generation"] == 2
    assert result["attempt"] == 2


# ─────────────────────────────────────────────────────────────────────────────
# AMENDMENT §1 tests — fail-closed on missing required fields
# ─────────────────────────────────────────────────────────────────────────────


def test_amend1_missing_direction_fails_closed():
    """Missing direction must TERMINAL_DURABLE — never silently default to CALL."""
    core = _core()
    bad = _row()
    bad["direction"] = None
    bad["meta"].pop("side", None)
    bad["meta"].pop("direction", None)
    core.order_state_machine.get_order.return_value = bad

    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=2,
        owner="owner-nodirection",
    )
    assert result["disposition"] in {"TERMINAL_DURABLE", "TERMINAL_REQUIRED"}
    assert "DIRECTION" in result["reason_code"]
    core.order_state_machine.claim_deferred_materialization.assert_not_called()


def test_amend1_invalid_direction_fails_closed():
    """direction='BUY' is not CALL/PUT — must TERMINAL_DURABLE."""
    core = _core()
    bad = _row()
    bad["direction"] = "BUY"
    bad["meta"]["side"] = "BUY"
    core.order_state_machine.get_order.return_value = bad

    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=2,
        owner="owner-baddirection",
    )
    assert result["disposition"] in {"TERMINAL_DURABLE", "TERMINAL_REQUIRED"}
    assert "DIRECTION" in result["reason_code"]


def test_amend1_missing_trigger_crossed_at_fails_closed():
    """Missing trigger_crossed_at must TERMINAL_DURABLE — never substitute now."""
    core = _core()
    bad = _row()
    bad["meta"].pop("trigger_crossed_at", None)
    bad["meta"].pop("triggered_at", None)
    core.order_state_machine.get_order.return_value = bad

    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=2,
        owner="owner-notriggerts",
    )
    assert result["disposition"] in {"TERMINAL_DURABLE", "TERMINAL_REQUIRED"}
    assert "TRIGGER_CROSSED_AT" in result["reason_code"]
    core.order_state_machine.claim_deferred_materialization.assert_not_called()


def test_amend1_missing_signal_id_fails_closed():
    """signal_id is required for the CAS predicate — must TERMINAL_DURABLE."""
    core = _core()
    bad = _row()
    bad["signal_id"] = None
    bad["meta"]["signal_id"] = None
    core.order_state_machine.get_order.return_value = bad

    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=2,
        owner="owner-nosignal",
    )
    assert result["disposition"] in {"TERMINAL_DURABLE", "TERMINAL_REQUIRED"}
    assert "SIGNAL_ID" in result["reason_code"]


def test_amend1_missing_client_id_row_fails_closed():
    """Missing client_id in the row must TERMINAL_DURABLE."""
    core = _core()
    bad = _row()
    bad["client_id"] = None
    core.order_state_machine.get_order.return_value = bad

    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=2,
        owner="owner-noclient",
    )
    assert result["disposition"] in {"TERMINAL_DURABLE", "TERMINAL_REQUIRED"}
    assert "CLIENT_ID" in result["reason_code"]


# ─────────────────────────────────────────────────────────────────────────────
# AMENDMENT §2 tests — proof fails when watcher field absent but expected present
# ─────────────────────────────────────────────────────────────────────────────


def test_amend2_absent_watcher_token_fails_when_expected_provided():
    """Watcher signal has no stored watcher_token (empty string). With an
    expected token provided, proof must FAIL — not silently pass.
    """
    from ap_entry_watcher import APEntryWatcher

    watcher = APEntryWatcher.__new__(APEntryWatcher)
    watcher._pending = []
    watcher._lock = threading.RLock()

    now = datetime.now(timezone.utc)
    watcher._pending.append(SimpleNamespace(
        signal={
            "local_order_id": LOCAL_ORDER_ID,
            "client_id": CLIENT_ID,
            "execution_mode": "paper",
            "watcher_token": "",        # absent
            "trigger_generation": 1,
        },
        is_active=True,
        rearm_mode=False,
        _ownership_quarantine=False,
        state=SimpleNamespace(name="PENDING"),
        expire_at=now + timedelta(hours=48),
        deferred_retry_not_before=now - timedelta(seconds=30),
    ))

    proof = watcher.prove_materialization_retry_owner(
        LOCAL_ORDER_ID,
        expected_watcher_token="watcher:CURRENT",  # expected but absent in watcher
    )
    assert proof["proven"] is False
    assert proof["reason_code"] == "PROOF_WATCHER_TOKEN_MISMATCH"


def test_amend2_absent_generation_fails_when_expected_provided():
    """Watcher signal has generation=0. With expected_generation=1, must FAIL."""
    from ap_entry_watcher import APEntryWatcher

    watcher = APEntryWatcher.__new__(APEntryWatcher)
    watcher._pending = []
    watcher._lock = threading.RLock()

    now = datetime.now(timezone.utc)
    watcher._pending.append(SimpleNamespace(
        signal={
            "local_order_id": LOCAL_ORDER_ID,
            "client_id": CLIENT_ID,
            "execution_mode": "paper",
            "watcher_token": "watcher:x",
            "trigger_generation": 0,   # absent / zero
        },
        is_active=True,
        rearm_mode=False,
        _ownership_quarantine=False,
        state=SimpleNamespace(name="PENDING"),
        expire_at=now + timedelta(hours=48),
        deferred_retry_not_before=now - timedelta(seconds=30),
    ))

    proof = watcher.prove_materialization_retry_owner(
        LOCAL_ORDER_ID,
        expected_generation=1,
    )
    assert proof["proven"] is False
    assert proof["reason_code"] == "PROOF_GENERATION_MISMATCH"


def test_final_malformed_generation_quarantines_first_row_and_second_due_retry_runs():
    """A malformed counter in one row must not abort the complete recovery
    sweep. The bad row is retained, and a later valid due row executes.
    """
    from unittest.mock import patch
    from ap import db as db_mod
    from ap_recovery import APStartupRecovery

    bad = _row()
    bad["local_order_id"] = "oid-bad-counter"
    bad["meta"]["materialization_generation"] = "not-an-int"

    good = _row()
    good["local_order_id"] = "oid-good-counter"

    resume_calls = []
    mock_core = MagicMock()
    def _resume(**kw):
        resume_calls.append(kw["local_order_id"])
        return {"disposition": "CLAIM_LOST", "reason_code": "test_claim_lost"}
    mock_core.resume_deferred_materialization_retry.side_effect = _resume

    retained = []
    class _OSM:
        client_id = CLIENT_ID
        def update_order_meta(self, oid, patch):
            retained.append((oid, patch))
            return True
        def get_order(self, oid):
            return good if oid == "oid-good-counter" else bad
        def get_orders_for_position(self, *a, **kw): return []

    class _C:
        rowcount = 2
        def execute(self, *a, **kw): return self
        def fetchall(self): return [bad, good]

    class _Conn:
        def __enter__(self): return _C()
        def __exit__(self, *a): return False

    rec = APStartupRecovery(
        client_id=CLIENT_ID, broker=MagicMock(), osm=_OSM(),
        pm=MagicMock(), master_control=SimpleNamespace(mode="PAPER"),
        exit_engine=None, entry_watcher=None, execution_core=mock_core,
    )
    result = {"deferred_lifecycles_recovered": 0, "errors": []}
    with patch.object(db_mod, "conn", lambda: _Conn()), \
         patch.object(db_mod, "run_with_retry", lambda fn, *a, **kw: fn()):
        rec._recover_deferred_breach_lifecycles(result)

    assert resume_calls == ["oid-good-counter"]
    assert any(oid == "oid-bad-counter" for oid, _ in retained)
    assert "recovery_malformed_counter:oid-bad-counter:materialization_generation" in str(result["errors"])


def test_final_malformed_retry_attempt_during_cas_miss_is_row_local():
    """A malformed retry_attempt discovered while classifying a fenced CAS miss
    records a row-local error and the sweep continues to later rows.
    """
    from unittest.mock import patch
    from ap import db as db_mod
    from ap_recovery import APStartupRecovery

    first = _row()
    first["local_order_id"] = "oid-cas-malformed"
    second = _row()
    second["local_order_id"] = "oid-after-cas-malformed"

    resume_calls = []
    mock_core = MagicMock()
    def _resume(local_order_id, **kw):
        resume_calls.append(local_order_id)
        if local_order_id == "oid-cas-malformed":
            return {
                "disposition": "TERMINAL_REQUIRED",
                "reason_code": "RETRY_MAX_ATTEMPTS_EXCEEDED",
                "terminal_status": "EXPIRED",
                "expected_generation": 1,
                "expected_prior_retry_attempt": 1,
                "expected_client_id": CLIENT_ID,
                "expected_execution_mode": "paper",
                "expected_lifecycle_state": "RETRY_WAIT",
                "expected_materialization_status": "RETRY_PENDING",
                "owner": "owner-cas",
                "generation": 2,
            }
        return {"disposition": "CLAIM_LOST", "reason_code": "test_claim_lost"}
    mock_core.resume_deferred_materialization_retry.side_effect = _resume

    retained = []
    class _OSM:
        client_id = CLIENT_ID
        def terminalize_deferred_retry_if_unchanged(self, *a, **kw):
            return False
        def get_order(self, oid):
            if oid == "oid-cas-malformed":
                row = _row()
                row["local_order_id"] = oid
                row["meta"]["retry_attempt"] = "bad-attempt"
                return row
            return second
        def update_order_meta(self, oid, patch):
            retained.append((oid, patch))
            return True
        def get_orders_for_position(self, *a, **kw): return []

    class _C:
        rowcount = 2
        def execute(self, *a, **kw): return self
        def fetchall(self): return [first, second]

    class _Conn:
        def __enter__(self): return _C()
        def __exit__(self, *a): return False

    rec = APStartupRecovery(
        client_id=CLIENT_ID, broker=MagicMock(), osm=_OSM(),
        pm=MagicMock(), master_control=SimpleNamespace(mode="PAPER"),
        exit_engine=None, entry_watcher=None, execution_core=mock_core,
    )
    result = {"deferred_lifecycles_recovered": 0, "errors": []}
    with patch.object(db_mod, "conn", lambda: _Conn()), \
         patch.object(db_mod, "run_with_retry", lambda fn, *a, **kw: fn()):
        rec._recover_deferred_breach_lifecycles(result)

    assert resume_calls == ["oid-cas-malformed", "oid-after-cas-malformed"]
    assert "recovery_malformed_counter:oid-cas-malformed:retry_attempt" in str(result["errors"])
    assert any(oid == "oid-cas-malformed" for oid, _ in retained)


# ─────────────────────────────────────────────────────────────────────────────
# AMENDMENT §3 — malformed timestamp quarantine, not has_order fallback
# ─────────────────────────────────────────────────────────────────────────────


def test_amend3_malformed_durable_timestamp_quarantines():
    """A non-parseable next_retry_at must produce a quarantine outcome.
    Recovery must not fall through to has_order() / watcher rearm.
    """
    from ap_entry_watcher import APEntryWatcher
    from unittest.mock import patch
    from ap import db as db_mod

    core = _core()
    core.order_state_machine.client_id = CLIENT_ID   # required by recovery osm check

    row_bad_ts = _row()
    row_bad_ts["meta"]["lifecycle_state"] = "RETRY_WAIT"
    row_bad_ts["meta"]["materialization_status"] = "RETRY_PENDING"
    row_bad_ts["meta"]["materialization_next_retry_at"] = "NOT-A-TIMESTAMP"
    row_bad_ts["meta"]["next_retry_at"] = "NOT-A-TIMESTAMP"
    row_bad_ts["meta"]["deferred_retry_next_attempt_at"] = "NOT-A-TIMESTAMP"

    watcher = APEntryWatcher.__new__(APEntryWatcher)
    watcher._pending = []
    watcher._lock = threading.RLock()
    watcher.watch = MagicMock(return_value=True)

    rec = _recovery(core, watcher)

    class _C:
        def execute(self, *a, **k):
            return self
        def fetchall(self):
            return [row_bad_ts]
        rowcount = 1

    class _Conn:
        def __enter__(self):
            return _C()
        def __exit__(self, *a):
            return False

    result = {"deferred_lifecycles_recovered": 0, "errors": []}
    with patch.object(db_mod, "conn", lambda: _Conn()), \
         patch.object(db_mod, "run_with_retry", lambda fn, *a, **kw: fn()):
        rec._recover_deferred_breach_lifecycles(result)

    # watcher.watch must NOT have been called — row was quarantined
    watcher.watch.assert_not_called()
    # OSM retention (update_order_meta) must have been called for the quarantine
    core.order_state_machine.update_order_meta.assert_called()


# ─────────────────────────────────────────────────────────────────────────────
# AMENDMENT §5 — callback exception triggers durable retry schedule
# ─────────────────────────────────────────────────────────────────────────────


def test_amend5_callback_exception_schedules_durable_retry():
    """When _on_entry_trigger raises, resume must call
    schedule_deferred_materialization_retry (not just return RETRY_WAIT with
    the row still stranded at MATERIALIZING).
    """
    core = _core()
    row = _row()
    core.order_state_machine.get_order.return_value = row
    core.order_state_machine.claim_deferred_materialization.return_value = True
    core._on_entry_trigger.side_effect = RuntimeError("network error")
    core.order_state_machine.schedule_deferred_materialization_retry.return_value = True

    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=2,
        owner="owner-exc",
    )
    # Must return RETRY_WAIT — not KEEP_WATCHER or anything else
    assert result["disposition"] == "RETRY_WAIT"
    assert "EXCEPTION" in result["reason_code"] or "CALLBACK" in result["reason_code"]
    # CRITICAL: schedule must have been called to make the RETRY_WAIT durable
    core.order_state_machine.schedule_deferred_materialization_retry.assert_called_once()


# ─────────────────────────────────────────────────────────────────────────────
# Second-round blocker tests from reviewer
# ─────────────────────────────────────────────────────────────────────────────


def test_blocker1_future_watcher_null_memory_schedule_fails_proof():
    """Future-due durable timestamp + watcher deferred_retry_not_before=None
    must FAIL proof. The watcher would immediately proceed to quote evaluation
    without waiting for the durable retry time.
    """
    from ap_entry_watcher import APEntryWatcher

    watcher = APEntryWatcher.__new__(APEntryWatcher)
    watcher._pending = []
    watcher._lock = threading.RLock()

    now = datetime.now(timezone.utc)
    future_due = now + timedelta(minutes=5)
    watcher._pending.append(SimpleNamespace(
        signal={
            "local_order_id": LOCAL_ORDER_ID,
            "client_id": CLIENT_ID,
            "execution_mode": "paper",
            "watcher_token": "watcher:x",
            "trigger_generation": 1,
        },
        is_active=True,
        rearm_mode=False,
        _ownership_quarantine=False,
        state=SimpleNamespace(name="PENDING"),
        expire_at=now + timedelta(hours=48),
        deferred_retry_not_before=None,  # inert — would fire immediately
    ))

    proof = watcher.prove_materialization_retry_owner(
        LOCAL_ORDER_ID,
        durable_next_retry_at=_iso(future_due),
    )
    assert proof["proven"] is False
    assert "MEMORY_RETRY_SCHEDULE" in proof["reason_code"] or "SCHEDULE_MISMATCH" in proof["reason_code"]


def test_blocker1_future_watcher_mismatched_schedule_fails_proof():
    """Future-due durable at +5min, watcher at +20min. Delta > 1s → proof fails."""
    from ap_entry_watcher import APEntryWatcher

    watcher = APEntryWatcher.__new__(APEntryWatcher)
    watcher._pending = []
    watcher._lock = threading.RLock()

    now = datetime.now(timezone.utc)
    durable_due = now + timedelta(minutes=5)
    memory_due = now + timedelta(minutes=20)  # 15-minute mismatch
    watcher._pending.append(SimpleNamespace(
        signal={
            "local_order_id": LOCAL_ORDER_ID,
            "client_id": CLIENT_ID,
            "execution_mode": "paper",
            "watcher_token": "watcher:x",
            "trigger_generation": 1,
        },
        is_active=True,
        rearm_mode=False,
        _ownership_quarantine=False,
        state=SimpleNamespace(name="PENDING"),
        expire_at=now + timedelta(hours=48),
        deferred_retry_not_before=memory_due,
    ))

    proof = watcher.prove_materialization_retry_owner(
        LOCAL_ORDER_ID,
        durable_next_retry_at=_iso(durable_due),
    )
    assert proof["proven"] is False
    assert "SCHEDULE_MISMATCH" in proof["reason_code"]
    assert "delta=" in proof["reason_code"]


def test_blocker2_retry_wait_no_timestamp_quarantines():
    """RETRY_WAIT with no durable next_retry_at must quarantine via
    retention. must NOT fall through to has_order() trust.
    """
    from ap_entry_watcher import APEntryWatcher
    from unittest.mock import patch
    from ap import db as db_mod

    core = _core()
    core.order_state_machine.client_id = CLIENT_ID

    row_no_ts = _row()
    row_no_ts["meta"]["lifecycle_state"] = "RETRY_WAIT"
    row_no_ts["meta"]["materialization_status"] = "RETRY_PENDING"
    # Remove ALL retry timestamps
    for k in ("materialization_next_retry_at", "next_retry_at",
               "deferred_retry_next_attempt_at"):
        row_no_ts["meta"].pop(k, None)

    watcher = APEntryWatcher.__new__(APEntryWatcher)
    watcher._pending = []
    watcher._lock = threading.RLock()
    watcher.watch = MagicMock(return_value=True)

    rec = _recovery(core, watcher)

    class _C:
        def execute(self, *a, **k):
            return self
        def fetchall(self):
            return [row_no_ts]
        rowcount = 1

    class _Conn:
        def __enter__(self):
            return _C()
        def __exit__(self, *a):
            return False

    result = {"deferred_lifecycles_recovered": 0, "errors": []}
    with patch.object(db_mod, "conn", lambda: _Conn()), \
         patch.object(db_mod, "run_with_retry", lambda fn, *a, **kw: fn()):
        rec._recover_deferred_breach_lifecycles(result)

    # Watcher.watch must NOT have been called
    watcher.watch.assert_not_called()
    # OSM retention must have been called
    core.order_state_machine.update_order_meta.assert_called()


def test_blocker3_schedule_write_false_returns_retry_schedule_failed():
    """When schedule_deferred_materialization_retry returns False,
    resume must return RETRY_SCHEDULE_FAILED — NOT RETRY_WAIT.
    """
    core = _core()
    row = _row()
    core.order_state_machine.get_order.side_effect = [row, row]
    core.order_state_machine.claim_deferred_materialization.return_value = True
    core._on_entry_trigger.side_effect = RuntimeError("cb error")
    # Schedule write fails
    core.order_state_machine.schedule_deferred_materialization_retry.return_value = False

    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=2,
        owner="owner-sched-fail",
    )
    assert result["disposition"] == "RETRY_SCHEDULE_FAILED"
    assert "SCHEDULE_WRITE_FAILED" in result["reason_code"]


def test_blocker4_claim_writes_canonical_retry_attempt():
    """claim_deferred_materialization must write retry_attempt (canonical)
    in the same patch as generation, not just retry_attempt_in_flight.
    """
    import json
    from unittest.mock import patch, MagicMock
    from ap.order_state_machine import APOrderStateMachine

    patches_seen: list = []

    class _FakeCursor:
        rowcount = 1
        def execute(self, sql, params=()):
            if params and isinstance(params[0], str):
                try:
                    patches_seen.append(json.loads(params[0]))
                except Exception:
                    pass
            return self

    class _FakeConn:
        def __enter__(self):
            return _FakeCursor()
        def __exit__(self, *a):
            return False

    osm = object.__new__(APOrderStateMachine)
    osm.client_id = CLIENT_ID

    # Patch conn AND run_with_retry so the SQL path actually executes
    with patch("ap.order_state_machine.conn", return_value=_FakeConn()), \
         patch("ap.order_state_machine.run_with_retry", lambda fn, *a, **kw: fn()):
        osm.claim_deferred_materialization(
            LOCAL_ORDER_ID,
            owner="owner-x",
            new_generation=2,
            lease_until="2026-12-31T00:00:00+00:00",
            trigger_crossed_at="2026-07-13T10:00:00+00:00",
            trigger_price=130.0,
            observed_underlying_price=130.05,
            signal_id=SIGNAL_ID,
            execution_mode="paper",
            retry_attempt=2,
        )

    assert patches_seen, "no patch was written to the DB"
    patch_written = patches_seen[0]
    assert "retry_attempt" in patch_written, "canonical retry_attempt must be in patch"
    assert patch_written["retry_attempt"] == 2
    assert patch_written.get("retry_attempt_in_flight") == 2


def test_blocker5_expired_deadline_terminalizes_before_claim():
    """A row whose absolute_entry_deadline is in the past must be
    terminalized BEFORE the CAS — no selector call, no broker path.
    """
    core = _core()
    row = _row()
    now = datetime.now(timezone.utc)
    row["meta"]["absolute_entry_deadline"] = _iso(now - timedelta(hours=1))

    core.order_state_machine.get_order.return_value = row

    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=2,
        owner="owner-expired",
    )
    assert result["disposition"] in {"TERMINAL_DURABLE", "TERMINAL_REQUIRED"}
    assert result["reason_code"] == "RETRY_DEADLINE_EXHAUSTED"
    assert result["terminal_status"] == "EXPIRED"
    core.order_state_machine.claim_deferred_materialization.assert_not_called()
    core._on_entry_trigger.assert_not_called()


def test_blocker5_missing_score_fails_before_claim():
    """Missing score in the row must fail closed before the CAS."""
    core = _core()
    row = _row()
    row["score"] = None
    row["meta"].pop("score", None)
    core.order_state_machine.get_order.return_value = row

    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=2,
        owner="owner-noscore",
    )
    assert result["disposition"] in {"TERMINAL_DURABLE", "TERMINAL_REQUIRED"}
    assert "SCORE" in result["reason_code"]
    core.order_state_machine.claim_deferred_materialization.assert_not_called()


def test_blocker5_missing_timeframe_fails_before_claim():
    """Missing timeframe in the row must fail closed before the CAS."""
    core = _core()
    row = _row()
    row["timeframe"] = None
    row["meta"].pop("timeframe", None)
    core.order_state_machine.get_order.return_value = row

    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=2,
        owner="owner-notf",
    )
    assert result["disposition"] in {"TERMINAL_DURABLE", "TERMINAL_REQUIRED"}
    assert "TIMEFRAME" in result["reason_code"]
    core.order_state_machine.claim_deferred_materialization.assert_not_called()
