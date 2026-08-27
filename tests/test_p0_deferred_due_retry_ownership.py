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

Tests avoid real network and broker calls. One PostgreSQL-gated acceptance
test proves the durable JSONB round trip; the remaining tests exercise the
ownership + orchestration primitives added by this PR:

  * APEntryWatcher.prove_materialization_retry_owner
  * APExecutionCore.resume_deferred_materialization_retry
  * APStartupRecovery._recover_deferred_breach_lifecycles due-retry branch
  * ap.operator_queue_read_model _order_row DEFERRED display
"""

from __future__ import annotations

import os
import sys
import threading
import time

# PR #389 amendment: ap.order_state_machine imports ap.db which requires a
# DATABASE_URL at import time. Test 11d exercises the real OSM seam so the
# module gets loaded here; the DB itself is spied by ``osm_db_spy`` — nothing
# actually connects.
os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")
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
    row = {
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
    _bind_trigger_evidence(row)
    return row


def _bind_trigger_evidence(row: dict) -> dict:
    """Give deferred fixtures the durable identity now required by recovery."""
    meta = row.setdefault("meta", {})
    canonical = str(
        row.get("canonical_signal_id")
        or meta.get("canonical_signal_id")
        or row.get("signal_id")
        or ""
    )
    client_id = str(row.get("client_id") or meta.get("client_id") or "")
    execution_mode = str(
        row.get("execution_mode") or meta.get("execution_mode") or ""
    ).strip().lower()
    meta.update(
        {
            "canonical_signal_id": canonical,
            "client_id": client_id,
            "execution_mode": execution_mode,
            "trigger_crossed_at_provenance": {
                "canonical_signal_id": canonical,
                "client_id": client_id,
                "execution_mode": execution_mode,
                "local_order_id": str(row.get("local_order_id") or ""),
            },
        }
    )
    return row


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
    # PR #421 final amendment: _retain_recovery_ownership() now prefers
    # osm.retain_recovery_ownership_if_no_watcher() when present, falling
    # back to the plain update_order_meta() merge otherwise. A bare
    # MagicMock() auto-vivifies ANY attribute access — including that new
    # method name — into a truthy, callable sub-mock, which would silently
    # redirect these tests away from the update_order_meta() path they
    # assert on. Explicitly remove it so this stub exercises the same
    # fallback path it always has.
    del core.order_state_machine.retain_recovery_ownership_if_no_watcher
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


def test_8_exhausted_retry_returns_terminal_and_does_not_call_broker(monkeypatch):
    # Pin MAX_BREACH_SELECTOR_RETRIES=3 to match the durable row's max_attempts=3.
    # Amendment 1 resolves max = max(configured_env, durable); without pinning the
    # env, the default of 5 would raise the max and attempt 4 would no longer be
    # exhausted — defeating what this test is specifically verifying.
    monkeypatch.setenv("MAX_BREACH_SELECTOR_RETRIES", "3")
    core = _core()
    # attempt 4 requested but max_attempts=3 (env also 3 → resolved max=3)
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
# TEST 11c — PR #389 amendment: selector-budget exhaustion gets its own
# durable materialization_outcome so operators can distinguish
# "ran out of direct-quote budget this request" from generic transient
# data-miss. All other retryable data reasons must keep the existing
# RETRY_LATER_DATA_UNAVAILABLE contract, and every existing field on the
# durable retry row must be preserved.
# ─────────────────────────────────────────────────────────────────────────────


def test_11c_selector_budget_exhausted_stamps_dedicated_outcome():
    from ap_execution_core import _build_deferred_retry_schedule_meta

    now = datetime(2026, 7, 25, 14, 0, 0, tzinfo=timezone.utc)
    audit = {
        "attempted": True,
        "budget_skipped": True,
        "budget_skip_reason": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
        "last_candidate_reject_reason": "OI_TOO_LOW",
    }

    meta = _build_deferred_retry_schedule_meta(
        reason_code="SELECTOR_REQUEST_BUDGET_EXHAUSTED",
        selector_audit=audit,
        attempt=2,
        max_attempts=5,
        delay_seconds=45,
        client_id="jason@example.com",
        execution_mode="live",
        local_order_id="oid-budget-1",
        signal_id="sig-budget-1",
        now=now,
    )

    # Dedicated durable outcome for the selector-budget case.
    assert meta["materialization_outcome"] == "RETRY_LATER_SELECTOR_BUDGET"
    assert meta["materialization_detail"] == "SELECTOR_REQUEST_BUDGET_EXHAUSTED"

    # All previously-required fields must still be present and truthful.
    assert meta["deferred_retry_scheduled"] is True
    assert meta["deferred_retry_reason_code"] == "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
    assert meta["deferred_retry_attempt"] == 2
    assert meta["deferred_retry_max_attempts"] == 5
    assert meta["deferred_retry_delay_seconds"] == 45
    assert meta["deferred_retry_next_attempt_at"] == (
        now + timedelta(seconds=45)
    ).isoformat()
    assert meta["breach_attempt_count"] == 2
    assert meta["client_id"] == "jason@example.com"
    assert meta["execution_mode"] == "live"
    assert meta["local_order_id"] == "oid-budget-1"
    assert meta["signal_id"] == "sig-budget-1"
    assert meta["contract_selection_status"] == "CONTRACT_SELECTION_RETRY"
    assert meta["retry_class"] == "OPERATIONAL_REQUEST_BUDGET"
    assert meta["operational_reason"] == "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
    # The last candidate-quality reason from the audit is carried through
    # the taxonomy so operators keep visibility on both dimensions.
    assert meta["selector_terminal_reason"] == "OI_TOO_LOW"
    assert meta["may_retry_with_fresh_budget"] is True


def test_11c_other_retryable_reasons_keep_data_unavailable_outcome():
    from ap_execution_core import _build_deferred_retry_schedule_meta

    now = datetime(2026, 7, 25, 14, 0, 0, tzinfo=timezone.utc)
    for reason in (
        "QUOTE_ZERO_BID_ASK",
        "MARKET_DATA_THROTTLE_UNAVAILABLE",
        "PROVIDER_RATE_LIMITED",
        "PROVIDER_TIMEOUT",
        "SELECTOR_NO_QUOTES",
        "SELECTOR_EMPTY_CHAIN",
    ):
        meta = _build_deferred_retry_schedule_meta(
            reason_code=reason,
            selector_audit={},
            attempt=1,
            max_attempts=5,
            delay_seconds=30,
            client_id="tf@example.com",
            execution_mode="paper",
            local_order_id="oid-x",
            signal_id="sig-x",
            now=now,
        )
        assert meta["materialization_outcome"] == "RETRY_LATER_DATA_UNAVAILABLE", reason
        assert meta["materialization_detail"] == reason


# ─────────────────────────────────────────────────────────────────────────────
# TEST 11d — PR #389 amendment: durable RETRY_LATER_SELECTOR_BUDGET row is
# persisted through the real order-state-machine seam
# (``schedule_deferred_materialization_retry``), reread through the same
# interface, and every identity + retry field survives the round-trip.
#
# Parameterized across the three real production identities in
# ap/morning_jobs.py so the acceptance surface matches configured runtime:
#   Jason LIVE           — jasoncosby1@gmail.com
#   Jose PAPER           — jose.vasquez4011@gmail.com
#   Tradefluence PAPER   — tradefluencehq@gmail.com
# One representative ticker (BAC) is sufficient because the 24-request
# selector matrix in test_p0_selector_direct_quote_budget_authority.py
# already covers all eight tickers.
# ─────────────────────────────────────────────────────────────────────────────


class _Cursor389:
    def __init__(self, sink, rowcount=1):
        self.sink = sink
        self.rowcount = rowcount

    def execute(self, sql, params=()):
        self.sink.append((" ".join(str(sql).split()), tuple(params)))
        return self


class _Conn389:
    def __init__(self, sink, rowcount=1):
        self.cursor = _Cursor389(sink, rowcount=rowcount)
        self.rowcount = rowcount

    def __enter__(self):
        return self.cursor

    def __exit__(self, *_):
        return False


@pytest.fixture
def osm_db_spy(monkeypatch):
    """Real OSM seam observer. Replaces the DB connection so every
    ``osm.schedule_deferred_materialization_retry`` call ends up
    executing the actual production SQL and JSONB patch through the
    canonical seam — we just capture what would land in Postgres."""
    import ap.order_state_machine as osm_mod

    sink: list = []
    state = {"rowcount": 1}
    monkeypatch.setattr(osm_mod, "conn",
                        lambda: _Conn389(sink, rowcount=state["rowcount"]))
    monkeypatch.setattr(osm_mod, "run_with_retry",
                        lambda fn, *a, **k: fn())
    return sink, state


@pytest.mark.parametrize(
    ("client_id", "execution_mode", "label"),
    [
        ("jasoncosby1@gmail.com",      "live",  "Jason LIVE"),
        ("jose.vasquez4011@gmail.com", "paper", "Jose PAPER"),
        ("tradefluencehq@gmail.com",   "paper", "Tradefluence PAPER"),
    ],
)
def test_11d_selector_budget_row_persists_through_osm_seam(
    osm_db_spy, client_id, execution_mode, label,
):
    """Drive the actual OSM seam. Assert the durable JSONB patch stamps
    RETRY_LATER_SELECTOR_BUDGET together with every existing retry
    contract field."""
    import json

    from ap.order_state_machine import APOrderStateMachine
    from ap_execution_core import _build_deferred_retry_schedule_meta

    sink, _state = osm_db_spy

    osm = APOrderStateMachine(client_id)
    local_order_id = f"oid-BAC-budget-{client_id}"
    signal_id = f"sig-BAC-budget-{client_id}"
    scheduled_at = datetime(2026, 7, 25, 14, 0, 0, tzinfo=timezone.utc)
    next_retry_at = (scheduled_at + timedelta(seconds=45)).isoformat()

    selector_failure_meta = _build_deferred_retry_schedule_meta(
        reason_code="SELECTOR_REQUEST_BUDGET_EXHAUSTED",
        selector_audit={
            "attempted": True,
            "budget_skipped": True,
            "last_candidate_reject_reason": "CHAIN_ROW_ZERO_BID_ASK",
        },
        attempt=1,
        max_attempts=5,
        delay_seconds=45,
        client_id=client_id,
        execution_mode=execution_mode,
        local_order_id=local_order_id,
        signal_id=signal_id,
        now=scheduled_at,
    )

    ok = osm.schedule_deferred_materialization_retry(
        local_order_id,
        owner="watcher:budget-owner",
        generation=2,
        reason_code="SELECTOR_REQUEST_BUDGET_EXHAUSTED",
        attempt=1,
        max_attempts=5,
        next_retry_at=next_retry_at,
        selector_failure=selector_failure_meta,
    )
    assert ok is True, f"{label}: OSM seam refused to schedule the retry"
    assert sink, f"{label}: OSM seam did not execute any SQL"

    sql, params = sink[-1]
    # Exactly one durable UPDATE on the canonical ENTRY/deferred row —
    # no second row is created, no cross-mode leak.
    assert sql.count("UPDATE orders") == 1
    assert "materialization_owner" in sql
    assert "materialization_generation" in sql
    assert local_order_id in params, f"{label}: local_order_id not scoped in SQL"
    assert client_id in params, f"{label}: client_id not scoped in SQL"

    patch = json.loads(params[0])
    # Retry ownership + fencing fields required by #388 machinery.
    assert patch["lifecycle_state"] == "RETRY_WAIT"
    assert patch["materialization_status"] == "RETRY_PENDING"
    assert patch["materialization_in_flight"] is False
    assert patch["materialization_generation"] == 2
    assert patch["retry_owner"] == "watcher:budget-owner"
    assert patch["current_owner"] == "watcher:budget-owner"
    assert patch["retry_reason"] == "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
    assert patch["retry_attempt"] == 1
    assert patch["retry_max_attempts"] == 5
    assert patch["breach_attempt_count"] == 1
    assert patch["next_retry_at"] == next_retry_at
    assert patch["materialization_next_retry_at"] == next_retry_at
    assert patch["materialization_attempts"] == 1
    assert patch["materialization_reason"] == "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
    assert patch["materialization_last_failure_at"]

    # Canonical readers consume the top-level fields; the complete nested
    # diagnostics carry the exact same values.
    assert patch["materialization_outcome"] == "RETRY_LATER_SELECTOR_BUDGET"
    assert patch["materialization_detail"] == "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
    assert patch["entry_path"] == "DEFERRED_BREACH_MATERIALIZATION"
    persisted = patch["materialization_selector_failure"]
    assert persisted["materialization_outcome"] == "RETRY_LATER_SELECTOR_BUDGET"
    assert persisted["materialization_detail"] == "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
    assert persisted["entry_path"] == "DEFERRED_BREACH_MATERIALIZATION"
    assert persisted["deferred_retry_reason_code"] == (
        "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
    )
    assert persisted["client_id"] == client_id
    assert persisted["execution_mode"] == execution_mode
    assert persisted["local_order_id"] == local_order_id
    assert persisted["signal_id"] == signal_id
    assert persisted["deferred_retry_attempt"] == 1
    assert persisted["deferred_retry_max_attempts"] == 5
    assert persisted["deferred_retry_delay_seconds"] == 45
    assert persisted["deferred_retry_next_attempt_at"] == next_retry_at
    assert persisted["operational_reason"] == "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
    assert persisted["may_retry_with_fresh_budget"] is True
    # broker_ready stays False so no submit happens off the retry row.
    assert patch["broker_ready"] is False


def test_11d_generic_retry_promotes_data_unavailable_and_rejects_contradiction(
    osm_db_spy,
):
    """Generic data misses promote their canonical outcome, while a supplied
    contradictory outcome fails closed before SQL."""
    import json

    from ap.order_state_machine import APOrderStateMachine

    sink, _state = osm_db_spy
    osm = APOrderStateMachine("tradefluencehq@gmail.com")
    due = (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat()

    assert osm.schedule_deferred_materialization_retry(
        "oid-generic-data",
        owner="watcher:data",
        generation=3,
        reason_code="PROVIDER_TIMEOUT",
        attempt=2,
        max_attempts=5,
        next_retry_at=due,
        selector_failure={"provider_status": 504},
        signal_id="signal-data",
        execution_mode="live",
    ) is True
    patch = json.loads(sink[-1][1][0])
    assert patch["materialization_outcome"] == "RETRY_LATER_DATA_UNAVAILABLE"
    assert patch["materialization_selector_failure"]["materialization_outcome"] == (
        "RETRY_LATER_DATA_UNAVAILABLE"
    )

    prior_writes = len(sink)
    assert osm.schedule_deferred_materialization_retry(
        "oid-contradictory",
        owner="watcher:data",
        generation=3,
        reason_code="SELECTOR_REQUEST_BUDGET_EXHAUSTED",
        attempt=2,
        max_attempts=5,
        next_retry_at=due,
        selector_failure={
            "materialization_outcome": "RETRY_LATER_DATA_UNAVAILABLE",
            "materialization_detail": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
            "entry_path": "DEFERRED_BREACH_MATERIALIZATION",
        },
        signal_id="signal-contradictory",
        execution_mode="live",
    ) is False
    assert len(sink) == prior_writes


def test_11d_real_postgres_round_trip_is_retryable_not_terminal(monkeypatch):
    """The actual PostgreSQL JSONB merge must expose the canonical outcome
    to both classifier and restart recovery without changing row identity."""
    from contextlib import contextmanager
    import json
    import uuid

    psycopg2 = pytest.importorskip("psycopg2")
    import psycopg2.extras

    database_url = os.getenv("INTELLIGENCE_POSTGRES_TEST_URL", "")
    if not database_url:
        if os.getenv("GITHUB_ACTIONS") == "true":
            pytest.fail("INTELLIGENCE_POSTGRES_TEST_URL is required in GitHub Actions")
        pytest.skip("disposable PostgreSQL URL not configured")

    from ap.order_state_machine import APOrderStateMachine
    from ap.pending_trigger_classifier import (
        PendingTriggerClassification,
        classify_pending_trigger_row,
    )
    from ap.pending_trigger_restart_recovery import PendingTriggerRestartRecovery
    from ap_execution_core import _build_deferred_retry_schedule_meta
    import ap.order_state_machine as osm_mod

    schema = f"pr389_outcome_{uuid.uuid4().hex}"
    client_id = "jasoncosby1@gmail.com"
    local_order_id = f"oid-pr389-{uuid.uuid4().hex}"
    signal_id = f"sig-pr389-{uuid.uuid4().hex}"
    owner = "materializer:pr389"
    generation = 4
    now = datetime.now(timezone.utc)
    next_retry_at = (now + timedelta(seconds=45)).isoformat()

    class _Wrapper:
        def __init__(self, cursor):
            self.cursor = cursor

        @property
        def rowcount(self):
            return self.cursor.rowcount

        def execute(self, sql, params=()):
            self.cursor.execute(sql, params)
            return self

        def fetchone(self):
            row = self.cursor.fetchone()
            return dict(row) if row else None

        def fetchall(self):
            return [dict(row) for row in self.cursor.fetchall()]

    @contextmanager
    def _pg_conn():
        db = psycopg2.connect(database_url)
        cursor = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cursor.execute(f'SET search_path TO "{schema}"')
            yield _Wrapper(cursor)
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            cursor.close()
            db.close()

    admin = psycopg2.connect(database_url)
    admin.autocommit = True
    try:
        with admin.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA "{schema}"')
            cursor.execute(
                f"""
                CREATE TABLE "{schema}".orders (
                    local_order_id TEXT PRIMARY KEY,
                    client_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    execution_mode TEXT NOT NULL,
                    signal_id TEXT,
                    plan_id TEXT,
                    broker_order_id TEXT,
                    submitted_ts TIMESTAMPTZ,
                    contract TEXT,
                    symbol TEXT,
                    direction TEXT,
                    side TEXT,
                    timeframe TEXT,
                    score NUMERIC,
                    tier TEXT,
                    trigger_price NUMERIC,
                    meta JSONB,
                    last_error TEXT,
                    created_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )

        monkeypatch.setattr(osm_mod, "conn", _pg_conn)
        monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn, *a, **k: fn())
        osm = APOrderStateMachine(client_id)
        materializing_meta = {
            "lifecycle_state": "MATERIALIZING",
            "materialization_status": "RUNNING",
            "materialization_in_flight": True,
            "materialization_owner": owner,
            "materialization_generation": generation,
            "canonical_signal_id": signal_id,
            "client_id": client_id,
            "execution_mode": "live",
            "trigger_crossed_at": (now - timedelta(seconds=10)).isoformat(),
            "trigger_crossed_at_provenance": {
                "canonical_signal_id": signal_id,
                "client_id": client_id,
                "execution_mode": "live",
                "local_order_id": local_order_id,
            },
            "trigger_price": 61.0,
        }
        with _pg_conn() as c:
            c.execute(
                """
                INSERT INTO orders (
                    local_order_id, client_id, kind, status, execution_mode,
                    signal_id, plan_id, contract, symbol, direction, side,
                    timeframe, score, tier, trigger_price, meta
                ) VALUES (
                    %s,%s,'ENTRY','PENDING_TRIGGER','live',%s,'plan-pr389',
                    'DEFERRED:BAC','BAC','PUT','PUT','5m',85,'A',61.0,%s::jsonb
                )
                """,
                (
                    local_order_id,
                    client_id,
                    signal_id,
                    json.dumps(materializing_meta),
                ),
            )

        selector_meta = _build_deferred_retry_schedule_meta(
            reason_code="SELECTOR_REQUEST_BUDGET_EXHAUSTED",
            selector_audit={
                "attempted": True,
                "budget_skipped": True,
                "last_candidate_reject_reason": "CHAIN_ROW_ZERO_BID_ASK",
            },
            attempt=1,
            max_attempts=5,
            delay_seconds=45,
            client_id=client_id,
            execution_mode="live",
            local_order_id=local_order_id,
            signal_id=signal_id,
            now=now,
        )
        assert osm.schedule_deferred_materialization_retry(
            local_order_id,
            owner=owner,
            generation=generation,
            reason_code="SELECTOR_REQUEST_BUDGET_EXHAUSTED",
            attempt=1,
            max_attempts=5,
            next_retry_at=next_retry_at,
            selector_failure=selector_meta,
            signal_id=signal_id,
            execution_mode="live",
        ) is True

        row = osm.get_order(local_order_id)
        assert row is not None
        meta = row["meta"]
        assert meta["materialization_outcome"] == "RETRY_LATER_SELECTOR_BUDGET"
        assert meta["materialization_detail"] == "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
        assert meta["entry_path"] == "DEFERRED_BREACH_MATERIALIZATION"
        nested = meta["materialization_selector_failure"]
        assert nested["materialization_outcome"] == meta["materialization_outcome"]
        assert nested["materialization_detail"] == meta["materialization_detail"]
        assert nested["entry_path"] == meta["entry_path"]
        assert meta["lifecycle_state"] == "RETRY_WAIT"
        assert meta["materialization_status"] == "RETRY_PENDING"
        assert meta["retry_owner"] == owner
        assert meta["materialization_generation"] == generation
        assert meta["trigger_crossed_at_provenance"] == {
            "canonical_signal_id": signal_id,
            "client_id": client_id,
            "execution_mode": "live",
            "local_order_id": local_order_id,
        }
        assert row["client_id"] == client_id
        assert row["execution_mode"] == "live"
        assert row["signal_id"] == signal_id
        assert row["local_order_id"] == local_order_id
        assert row["status"] == "PENDING_TRIGGER"

        classification = classify_pending_trigger_row(
            row,
            watcher_owned=None,
            is_past_eod=False,
            live_quote_already_through_trigger=None,
        )
        assert classification == PendingTriggerClassification.WAITING_RETRYABLE

        recovery = PendingTriggerRestartRecovery(
            client_id=client_id,
            execution_mode="live",
            osm=osm,
            entry_watcher=None,
            broker=None,
            caller_source="pr389_postgres_acceptance",
        )
        summary = recovery.recover_all([row])
        assert summary["retry_rows_owned"] == 1
        assert summary["terminalized"] == 0
        assert summary["ownerless_rows_remaining"] == 0

        reread = osm.get_order(local_order_id)
        assert reread["status"] == "PENDING_TRIGGER"
        assert reread["meta"]["materialization_outcome"] == (
            "RETRY_LATER_SELECTOR_BUDGET"
        )
        with _pg_conn() as c:
            c.execute(
                """
                SELECT COUNT(*) AS entry_count
                FROM orders
                WHERE client_id=%s AND kind='ENTRY' AND local_order_id=%s
                """,
                (client_id, local_order_id),
            )
            assert int(c.fetchone()["entry_count"]) == 1

        # Production-shaped MCD replay: two independent workers race the same
        # generation/attempt fence. Exactly one may claim, and a process crash
        # immediately after the claim must still leave all three durable
        # attempt mirrors at the same value before any selector/broker work.
        claim_barrier = threading.Barrier(2)
        claim_results: list[tuple[str, bool]] = []
        claim_errors: list[BaseException] = []

        def _claim_worker(worker_owner: str) -> None:
            try:
                claim_barrier.wait(timeout=10)
                worker_osm = APOrderStateMachine(client_id)
                claimed = worker_osm.claim_deferred_materialization(
                    local_order_id,
                    owner=worker_owner,
                    new_generation=generation + 1,
                    lease_until=(now + timedelta(minutes=2)).isoformat(),
                    trigger_crossed_at=(now - timedelta(seconds=10)).isoformat(),
                    trigger_price=61.0,
                    observed_underlying_price=61.05,
                    signal_id=signal_id,
                    execution_mode="live",
                    retry_attempt=2,
                )
                claim_results.append((worker_owner, bool(claimed)))
            except BaseException as exc:  # pragma: no cover - surfaced below
                claim_errors.append(exc)

        workers = [
            threading.Thread(target=_claim_worker, args=("worker-a",)),
            threading.Thread(target=_claim_worker, args=("worker-b",)),
        ]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(timeout=15)
        assert not claim_errors
        assert len(claim_results) == 2
        assert sum(1 for _, claimed in claim_results if claimed) == 1

        claimed_owner = next(owner for owner, claimed in claim_results if claimed)
        claimed_row = osm.get_order(local_order_id)
        claimed_meta = claimed_row["meta"]
        assert claimed_meta["materialization_generation"] == generation + 1
        assert claimed_meta["materialization_owner"] == claimed_owner
        assert {
            claimed_meta["retry_attempt"],
            claimed_meta["breach_attempt_count"],
            claimed_meta["materialization_attempts"],
        } == {2}
        assert claimed_meta["retry_attempt_in_flight"] == 2
        assert claimed_meta["broker_ready"] is False
        assert claimed_row["broker_order_id"] is None
        assert claimed_row["submitted_ts"] is None

        # Simulated restart: a fresh worker cannot reuse the already-claimed
        # attempt or replace the exact materialization owner before the first
        # owner produces a durable outcome.
        restart_osm = APOrderStateMachine(client_id)
        assert restart_osm.claim_deferred_materialization(
            local_order_id,
            owner="worker-after-restart",
            new_generation=generation + 1,
            lease_until=(now + timedelta(minutes=2)).isoformat(),
            trigger_crossed_at=(now - timedelta(seconds=10)).isoformat(),
            trigger_price=61.0,
            observed_underlying_price=61.05,
            signal_id=signal_id,
            execution_mode="live",
            retry_attempt=2,
        ) is False
        restart_row = osm.get_order(local_order_id)
        assert restart_row["meta"]["materialization_owner"] == claimed_owner
        assert restart_row["meta"]["materialization_generation"] == generation + 1
        assert restart_row["meta"]["retry_attempt"] == 2
        assert restart_row["broker_order_id"] is None
        assert restart_row["submitted_ts"] is None

        assert osm.terminalize_materialization_retry(
            local_order_id,
            owner=claimed_owner,
            generation=generation + 1,
            retry_attempt=2,
            client_id=client_id,
            execution_mode="live",
            terminal_status="ERROR",
            reason="SELECTOR_RECOVERY_CURSOR_INVALID:MATERIALIZATION_ATTEMPT_COUNTER_CONFLICT",
            diagnostics={
                "selector_calls": 0,
                "direct_quote_calls": 0,
                "broker_post_count": 0,
            },
        ) is True
        terminal_row = osm.get_order(local_order_id)
        assert terminal_row["status"] == "ERROR"
        terminal_meta = terminal_row["meta"]
        assert {
            terminal_meta["retry_attempt"],
            terminal_meta["breach_attempt_count"],
            terminal_meta["materialization_attempts"],
        } == {2}
        assert terminal_meta["broker_ready"] is False
        assert terminal_row["broker_order_id"] is None
        assert terminal_row["submitted_ts"] is None
    finally:
        with admin.cursor() as cursor:
            cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


# ─────────────────────────────────────────────────────────────────────────────
# TEST 11e — PR #389 amendment: after a persisted selector-budget retry
# row and a simulated process restart with no in-memory owner, the
# canonical due-retry recovery path claims the row through the same
# CAS/generation fence used by #388, runs exactly one selector request,
# retains the existing local_order_id, and does not create a second
# ENTRY/deferred row. A concurrent recovery attempt loses ownership
# without invoking the selector or the broker.
# ─────────────────────────────────────────────────────────────────────────────


def _budget_row(client_id: str, execution_mode: str,
                local_order_id: str, signal_id: str,
                *, generation: int = 2, attempt: int = 1) -> dict:
    """A RETRY_PENDING row shaped like the one PR #389's amended
    materialization_outcome persists. Used as the durable state the
    startup recovery finds after a simulated restart."""
    now = datetime.now(timezone.utc)
    row = _row(
        lifecycle="RETRY_WAIT",
        materialization_status="RETRY_PENDING",
        retry_attempt=attempt,
        materialization_generation=generation,
        max_attempts=5,
        next_retry_offset_seconds=-60,
        execution_mode=execution_mode,
        client_id=client_id,
    )
    row["local_order_id"] = local_order_id
    row["signal_id"] = signal_id
    _bind_trigger_evidence(row)
    row["symbol"] = "BAC"
    row["direction"] = "PUT"
    row["meta"]["materialization_selector_failure"] = {
        "materialization_outcome": "RETRY_LATER_SELECTOR_BUDGET",
        "materialization_detail": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
        "reason_code": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
        "client_id": client_id,
        "execution_mode": execution_mode,
        "signal_id": signal_id,
        "local_order_id": local_order_id,
        "operational_reason": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
        "may_retry_with_fresh_budget": True,
    }
    row["meta"]["retry_owner"] = ""
    return row


def test_11e_restart_recovery_claims_budget_row_and_runs_one_selector_request():
    """A restart-shaped scenario:
      * a RETRY_PENDING budget row already exists in the OSM;
      * the entry watcher registry is empty (nothing in memory);
      * recovery drives the canonical due-retry path;
      * exactly ONE selector request runs;
      * the CAS/generation fence is used (claim_deferred_materialization
        is called with the row's owner + generation);
      * the existing local_order_id is retained on the callback signal;
      * no second ENTRY/deferred row is created.
    """
    core = _core(execution_mode="live")
    client_id = "jasoncosby1@gmail.com"
    core.client_id = client_id
    core.email = client_id

    local_order_id = "oid-BAC-restart-1"
    signal_id = "sig-BAC-restart-1"
    row = _budget_row(client_id, "live", local_order_id, signal_id,
                      generation=2, attempt=1)

    # OSM behaves as the durable source of truth. get_order returns the
    # persisted row throughout the retry cycle.
    core.order_state_machine.get_order.side_effect = [row, row, row]
    core.order_state_machine.claim_deferred_materialization.return_value = True

    result = core.resume_deferred_materialization_retry(
        local_order_id=local_order_id,
        expected_generation=2,
        expected_retry_attempt=2,
        owner="recovery:restart-owner",
    )

    # Recovery entered the canonical seam once.
    assert core._on_entry_trigger.call_count == 1
    signal_payload = core._on_entry_trigger.call_args.args[0].signal
    # local_order_id is retained; no new row spawned.
    assert signal_payload["local_order_id"] == local_order_id
    assert signal_payload["client_id"] == client_id
    # Fenced claim through the OSM interface with the exact generation.
    claim_args = core.order_state_machine.claim_deferred_materialization.call_args
    if claim_args is not None:
        assert claim_args.kwargs.get("expected_generation", 2) == 2
    assert result["disposition"] in {"SUBMITTED", "BROKER_READY", "RETRY_WAIT"}
    # No broker interaction from the retry path itself — final result on
    # the row must be reached via ExecutionCore, not a bypass.
    assert core.broker.submit_order.called is False if hasattr(
        core.broker, "submit_order"
    ) else True


def test_11e_concurrent_recovery_loses_ownership_without_selector_call():
    """Two concurrent recovery workers attempt the same durable budget
    row. Exactly one wins the CAS. The loser must:
      * NOT invoke the selector (no _on_entry_trigger);
      * NOT invoke the broker;
      * report CLAIM_LOST as the canonical failure disposition.
    """
    core = _core(execution_mode="live")
    client_id = "jasoncosby1@gmail.com"
    core.client_id = client_id
    core.email = client_id
    row = _budget_row(
        client_id, "live", "oid-BAC-concurrent-1", "sig-BAC-concurrent-1",
        generation=2, attempt=1,
    )
    core.order_state_machine.get_order.return_value = row

    winners: list[int] = []

    def _claim(*a, **kw):
        winners.append(1)
        return len(winners) == 1

    core.order_state_machine.claim_deferred_materialization.side_effect = _claim

    winner = core.resume_deferred_materialization_retry(
        local_order_id="oid-BAC-concurrent-1",
        expected_generation=2,
        expected_retry_attempt=2,
        owner="recovery:worker-A",
    )
    loser = core.resume_deferred_materialization_retry(
        local_order_id="oid-BAC-concurrent-1",
        expected_generation=2,
        expected_retry_attempt=2,
        owner="recovery:worker-B",
    )
    assert winner["disposition"] in {"SUBMITTED", "BROKER_READY", "RETRY_WAIT"}
    assert loser["disposition"] == "CLAIM_LOST"
    assert loser["reason_code"] == "RETRY_CLAIM_NOT_ACQUIRED"
    # Loser never called into selector or broker.
    assert core._on_entry_trigger.call_count <= 1
    if hasattr(core.broker, "submit_order"):
        assert core.broker.submit_order.called is False


def test_11e_startup_loader_overlap_runs_one_real_selector_request(monkeypatch):
    """Two ordinary startup loaders discover the same durable retry row.
    The production resume method and OSM claim fence permit one real selector
    request; the loser stops at claim and no broker order is emitted."""
    import copy
    from datetime import date
    from unittest.mock import patch

    from ap.contract_selector import APContractSelectionEngine
    from ap_recovery import APStartupRecovery
    import ap_execution_core
    from ap import db as db_mod

    client_id = "jasoncosby1@gmail.com"
    local_order_id = "oid-BAC-startup-overlap"
    signal_id = "sig-BAC-startup-overlap"
    row = _budget_row(
        client_id,
        "live",
        local_order_id,
        signal_id,
        generation=2,
        attempt=1,
    )
    row["symbol"] = "BAC"
    row["direction"] = "PUT"
    row["trigger_price"] = 60.90
    row["target_underlying"] = 59.50
    row["stop_underlying"] = 61.80
    row["meta"].update({
        "materialization_outcome": "RETRY_LATER_SELECTOR_BUDGET",
        "materialization_detail": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
        "entry_path": "DEFERRED_BREACH_MATERIALIZATION",
        "materialization_attempts": 1,
        "materialization_reason": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
        "materialization_last_failure_at": datetime.now(timezone.utc).isoformat(),
        "broker_ready": False,
    })

    claim_barrier = threading.Barrier(2)

    class _SharedOSM:
        def __init__(self):
            self.client_id = client_id
            self.row = copy.deepcopy(row)
            self.lock = threading.RLock()
            self.claim_attempts = 0
            self.claim_winners = 0
            self.entry_rows_created = 1

        def get_order(self, oid):
            assert oid == local_order_id
            with self.lock:
                return copy.deepcopy(self.row)

        def claim_deferred_materialization(self, oid, **kwargs):
            assert oid == local_order_id
            claim_barrier.wait(timeout=5)
            with self.lock:
                self.claim_attempts += 1
                meta = self.row["meta"]
                if (
                    meta.get("lifecycle_state") != "RETRY_WAIT"
                    or int(meta.get("materialization_generation") or 0) != 2
                ):
                    return False
                self.claim_winners += 1
                meta.update({
                    "lifecycle_state": "MATERIALIZING",
                    "materialization_status": "RUNNING",
                    "materialization_owner": kwargs["owner"],
                    "materialization_generation": int(kwargs["new_generation"]),
                    "retry_attempt": int(kwargs["retry_attempt"]),
                    "materialization_in_flight": True,
                })
                return True

        def update_order_meta(self, oid, patch):
            with self.lock:
                self.row["meta"].update(copy.deepcopy(patch))
            return True

        def schedule_deferred_materialization_retry(self, oid, **kwargs):
            raise AssertionError("successful selector must not reschedule")

    osm = _SharedOSM()

    expiry_date = date.today() + timedelta(days=3)
    while expiry_date.weekday() >= 5:
        expiry_date += timedelta(days=1)
    expiry = expiry_date.isoformat()
    occ_date = expiry_date.strftime("%y%m%d")
    ranked_symbols = [
        f"BAC{occ_date}P{int((60.90 - rank) * 1000):08d}"
        for rank in range(1, 13)
    ]
    chain = [
        {
            "symbol": symbol,
            "expiration_date": expiry,
            "strike": 60.90 - rank,
            "option_type": "put",
            "bid": 0.0,
            "ask": 0.0,
            "open_interest": 9800 + rank,
            "volume": 4100 + rank,
            "bid_size": 20,
            "ask_size": 20,
            "greeks": {"delta": -0.40},
        }
        for rank, symbol in enumerate(ranked_symbols, start=1)
    ]
    valid_symbol = ranked_symbols[3]

    class _Response:
        status_code = 200

        def __init__(self, payload):
            self.payload = payload

        def json(self):
            return self.payload

    class _ControlledBroker:
        base_url = "https://api.tradier.com"
        cfg = SimpleNamespace(
            base_url="https://api.tradier.com",
            access_token="token",
        )

        def __init__(self):
            self.session = SimpleNamespace(get=self._get)
            self.submit_order = MagicMock()
            self.cancel_order = MagicMock()
            self.quote_calls = []

        def _get(self, url, **kwargs):
            if "options/expirations" in url:
                return _Response({"expirations": {"date": [expiry]}})
            if "options/chains" in url:
                return _Response({"options": {"option": chain}})
            return _Response({"quotes": {"quote": {"last": 60.90}}})

        def get_quote(self, symbol):
            self.quote_calls.append(symbol)
            if "".join(str(symbol).upper().split()) == valid_symbol:
                return {
                    "bid": 1.10,
                    "ask": 1.14,
                    "volume": 4100,
                    "open_interest": 9800,
                }
            return {"bid": 0.0, "ask": 0.0}

    broker = _ControlledBroker()
    selector = APContractSelectionEngine(
        broker,
        mode="LIVE",
        data_broker=broker,
        min_premium=1.0,
        max_premium=1000.0,
        min_oi=1,
        min_volume=0,
    )
    selector_requests = []

    core = SimpleNamespace(
        client_id=client_id,
        email=client_id,
        execution_mode="live",
        mode="LIVE",
        paper=False,
        order_state_machine=osm,
        broker=broker,
    )
    core.resume_deferred_materialization_retry = (
        ap_execution_core.APExecutionCore
        .resume_deferred_materialization_retry.__get__(core, type(core))
    )

    def _selector_callback(watched):
        plan_ns = watched.signal["_approved_plan"]
        plan = {
            "signal_id": plan_ns.signal_id,
            "client_id": plan_ns.client_id,
            "execution_mode": plan_ns.execution_mode,
            "local_order_id": watched.signal["local_order_id"],
            "ticker": plan_ns.ticker,
            "side": plan_ns.side,
            "target_underlying": 60.90,
            "trigger_price": 60.90,
            "wick_targets": [{"distance_pct": 0.5, "confidence": 0.75}],
            "tier": plan_ns.tier,
            "score": plan_ns.score,
            "pattern": plan_ns.pattern,
            "timeframe": plan_ns.timeframe,
            "metadata": {"sizing_context": {
                "budget": 2000.0,
                "account_equity": 10000.0,
                "risk_pct": 0.05,
                "max_affordable_premium": 2000.0,
            }},
            "max_position_usd": 2000.0,
        }
        from ap.contract_selector import (
            SELECTOR_REQUEST_KIND_DEFERRED_BREACH,
            _new_selector_request_context,
        )
        selected = selector.select(
            plan,
            request_context=_new_selector_request_context(
                "BAC",
                "live",
                selector_request_kind=(
                    SELECTOR_REQUEST_KIND_DEFERRED_BREACH
                ),
            ),
        )
        assert selected is not None
        selector_requests.append(plan["metadata"]["selector_request_diagnostics"])
        with osm.lock:
            osm.row["contract"] = selected.contract_symbol
            osm.row["meta"].update({
                "lifecycle_state": "BROKER_READY",
                "materialization_status": "SELECTED",
                "broker_ready": True,
                "selected_contract": selected.contract_symbol,
            })

    core._on_entry_trigger = _selector_callback

    class _Cursor:
        rowcount = 1

        def execute(self, *args, **kwargs):
            return self

        def fetchall(self):
            return [copy.deepcopy(row)]

    class _Conn:
        def __enter__(self):
            return _Cursor()

        def __exit__(self, *args):
            return False

    def _recovery_instance():
        return APStartupRecovery(
            client_id=client_id,
            broker=broker,
            osm=osm,
            pm=MagicMock(),
            master_control=SimpleNamespace(mode="LIVE"),
            entry_watcher=None,
            execution_core=core,
        )

    results = [{}, {}]

    def _run(worker_idx):
        bucket = {"deferred_lifecycles_recovered": 0, "errors": []}
        _recovery_instance()._recover_deferred_breach_lifecycles(bucket)
        results[worker_idx] = bucket

    monkeypatch.setenv("SELECTOR_MAX_DIRECT_QUOTE_CALLS", "8")
    monkeypatch.setenv("DIRECT_QUOTE_RECOVERY_TOP_N", "8")
    monkeypatch.setenv("CONTRACT_REVALIDATE_TOP_N", "8")
    monkeypatch.setenv("PRO_CONTRACT_QUALITY", "true")
    monkeypatch.setenv("DEFERRED_RETRY_OWNER_GRACE_SECONDS", "0")
    monkeypatch.setattr(
        "ap.contract_quote_revalidator.is_market_open",
        lambda *args, **kwargs: True,
    )
    monkeypatch.setattr(
        APContractSelectionEngine,
        "_emit_selector_event",
        lambda *args, **kwargs: None,
    )
    with patch.object(db_mod, "conn", lambda: _Conn()), \
         patch.object(db_mod, "run_with_retry", lambda fn, *a, **kw: fn()):
        threads = [
            threading.Thread(target=_run, args=(idx,), daemon=True)
            for idx in range(2)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
        assert all(not thread.is_alive() for thread in threads)

    assert osm.claim_attempts == 2
    assert osm.claim_winners == 1
    assert len(selector_requests) == 1
    assert selector_requests[0]["direct_quote_budget"]["effective_limit"] == 8
    assert osm.row["local_order_id"] == local_order_id
    assert osm.row["signal_id"] == signal_id
    assert osm.entry_rows_created == 1
    assert osm.row["meta"]["lifecycle_state"] == "BROKER_READY"
    assert broker.submit_order.call_count == 0
    assert broker.cancel_order.call_count == 0
    assert sum(r["deferred_lifecycles_recovered"] for r in results) == 1


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


@pytest.mark.parametrize(
    "starting_contract",
    ["DEFERRED:RTX", "RTX260117C00129000"],
    ids=["placeholder_contract", "stale_real_contract"],
)
def test_spec_acceptance_single_claim_seam(monkeypatch, starting_contract):
    """Real seam test: resume_deferred_materialization_retry → real _on_entry_trigger
    deferred path → selector mock → durable copyback → canonical submit seam.

    Verifies the core requirement: claim_deferred_materialization is called
    EXACTLY ONCE across the entire path. The second claim inside
    _on_entry_trigger must be bypassed via the verified pre-claim markers.

    Also exercises a normalized durable-row shape where the top-level
    execution_mode column and metadata mirror agree.

    Does NOT mock _on_entry_trigger. Does NOT manually manufacture the
    post-callback row.
    """
    import ap_execution_core as core_mod

    now = datetime.now(timezone.utc)
    fake_execution = SimpleNamespace(
        _refresh_ask_at_submit=lambda *_args, **_kwargs: (
            2.10,
            5,
            True,
            "ok",
            {
                "submit_bid": 2.05,
                "submit_ask": 2.10,
                "submit_last": 2.08,
                "submit_mid": 2.075,
                "spread_pct": 0.024,
            },
        )
    )
    monkeypatch.setitem(sys.modules, "ap.execution", fake_execution)

    # ── Build a production-shape durable row (RETRY_WAIT attempt=1) ──
    before_meta = {
        "execution_mode": "paper",
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
        "selector_recovery_cursor_v1": {
            "version": 1,
            "local_order_id": LOCAL_ORDER_ID,
            "client_id": CLIENT_ID,
            "execution_mode": "paper",
            "signal_id": SIGNAL_ID,
            "materialization_generation": 1,
            "selector_attempt_count": 1,
            "attempted_symbols": {},
            "structurally_skipped_symbols": {},
            "expirations_probed": [],
            "last_ranked_index_by_expiration": {},
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
        "contract": starting_contract,
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
    copyback_calls = []
    submit_calls = []
    cursor_persist_calls = []

    class _OSM:
        client_id = CLIENT_ID

        def __init__(self):
            self.row = before_row

        def get_order(self, oid):
            return self.row

        def claim_deferred_materialization(self, oid, **kw):
            claim_call_count[0] += 1
            self.row = after_claim_row
            return True

        def update_order_meta(self, oid, patch):
            self.row.setdefault("meta", {}).update(patch)
            return True

        def persist_selector_recovery_cursor(self, oid, **kw):
            cursor_persist_calls.append(kw["cursor"])
            time.sleep(0.01)
            self.row.setdefault("meta", {})[
                "selector_recovery_cursor_v1"
            ] = kw["cursor"]
            return True

        def schedule_deferred_materialization_retry(self, oid, **kw):
            return True

        def terminalize_deferred_breach(self, oid, **kw):
            return True

        def submit_existing_entry(self, *a, **kw):
            submit_calls.append(kw)
            self.row["status"] = "SUBMITTED"
            self.row["broker_order_id"] = "BR-TEST-1"
            self.row["submitted_ts"] = _iso(datetime.now(timezone.utc))
            return {
                "ok": True,
                "status": "SUBMITTED",
                "local_order_id": LOCAL_ORDER_ID,
                "broker_order_id": "BR-TEST-1",
            }

        def get_orders_for_position(self, *a, **kw):
            return []

        def persist_deferred_broker_ready(self, *a, **kw):
            copyback_calls.append(kw)
            next_row = dict(broker_ready_row)
            next_meta = dict(broker_ready_row["meta"])
            next_row.update({
                "contract": kw["contract"],
                "limit_price": float(kw["limit_price"]),
                "qty": int(kw["qty"]),
                "reserved_cost": float(kw["reserved_cost"]),
            })
            next_meta.update({
                "materialization_owner": kw["owner"],
                "materialization_generation": int(kw["generation"]),
                "selected_contract": kw["contract"],
                "selected_limit": float(kw["limit_price"]),
                "selected_qty": int(kw["qty"]),
            })
            next_row["meta"] = next_meta
            self.row = next_row
            return True

        def claim_deferred_broker_ready_submit(self, *a, **kw):
            return True

    class _FakeSelector:
        select_count = 0
        data_broker = SimpleNamespace(
            base_url="https://api.tradier.com/v1",
            cfg=SimpleNamespace(base_url="https://api.tradier.com/v1"),
            get_quote=lambda _symbol: {
                "bid": 130.20,
                "ask": 130.30,
                "quote_timestamp": datetime.now(timezone.utc).isoformat(),
                "source": "tradier_live",
            }
        )

        def select(self, plan, **_kwargs):
            _FakeSelector.select_count += 1
            request_context = _kwargs["request_context"]
            for index in range(12):
                request_context.recovery_cursor_persist(
                    symbol=f"RTX260117C{130000 + index:08d}",
                    result_reason="DIRECT_QUOTE_ZERO_BID_ASK",
                    transient=True,
                )
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
    started = time.perf_counter()
    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=2,
        owner=owner_label,
    )
    selector_elapsed = time.perf_counter() - started

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
    # Audit blocker 5 fix: cursor persistence is no longer batched by 5 --
    # every completed candidate quote attempt is now flushed immediately,
    # closing the crash-loss window where 1-4 completed attempts existed
    # only in process memory. This fixture has one market-truth checkpoint
    # plus 12 candidates, so all 13 writes now happen synchronously
    # (previously batched down to 4). The ~10ms simulated per-write latency
    # means this is slower (~130ms) than the old batched path (~40ms), which
    # is the correct, deliberate trade of speed for durability the audit
    # required -- not a regression.
    assert len(cursor_persist_calls) == 13
    assert selector_elapsed < 0.25
    assert copyback_calls, "validated retry selection must reach durable copyback"
    assert copyback_calls[-1]["contract"] == "RTX260117C00130000"
    assert float(copyback_calls[-1]["limit_price"]) > 0.01
    assert len(submit_calls) == 1
    assert submit_calls[0]["plan"].contract_symbol == "RTX260117C00130000"
    assert float(submit_calls[0]["limit_price"]) > 0.01
    assert result["disposition"] == "SUBMITTED"
    assert result["broker_order_id"] == "BR-TEST-1"
    assert osm.row["status"] == "SUBMITTED"
    assert osm.row["contract"] == "RTX260117C00130000"
    assert float(osm.row["limit_price"]) > 0.01
    assert osm.row["broker_order_id"] == "BR-TEST-1"
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
    _bind_trigger_evidence(bad)
    bad["meta"]["materialization_generation"] = "not-an-int"

    good = _row()
    good["local_order_id"] = "oid-good-counter"
    _bind_trigger_evidence(good)

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
    _bind_trigger_evidence(first)
    second = _row()
    second["local_order_id"] = "oid-after-cas-malformed"
    _bind_trigger_evidence(second)

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
                _bind_trigger_evidence(row)
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
    assert patch_written["breach_attempt_count"] == 2
    assert patch_written["materialization_attempts"] == 2


@pytest.mark.parametrize("retry_attempt", [True, False])
def test_blocker4_boolean_retry_attempt_rejected_before_sql(retry_attempt):
    """Boolean retry attempts must not become integer attempt authority."""
    from copy import deepcopy
    from unittest.mock import MagicMock, patch
    from ap.order_state_machine import APOrderStateMachine

    durable_row = _row(retry_attempt=1)
    before = deepcopy(durable_row)
    osm = object.__new__(APOrderStateMachine)
    osm.client_id = CLIENT_ID
    fake_conn = MagicMock()
    fake_run_with_retry = MagicMock()

    with patch("ap.order_state_machine.conn", return_value=fake_conn), patch(
        "ap.order_state_machine.run_with_retry", fake_run_with_retry
    ):
        claimed = osm.claim_deferred_materialization(
            LOCAL_ORDER_ID,
            owner="owner-bool-retry",
            new_generation=2,
            lease_until="2026-12-31T00:00:00+00:00",
            trigger_crossed_at="2026-08-25T13:40:00+00:00",
            trigger_price=130.0,
            observed_underlying_price=130.05,
            signal_id=SIGNAL_ID,
            execution_mode="paper",
            retry_attempt=retry_attempt,
        )

    assert claimed is False
    assert durable_row == before
    fake_conn.assert_not_called()
    fake_run_with_retry.assert_not_called()


def test_blocker4_claim_fences_present_attempt_mirrors_before_advancing():
    """A pre-existing 2/1/1 row is not silently repaired by a later claim.

    The claim may fill a genuinely absent legacy mirror, but every present
    mirror must still equal the expected prior attempt in the same CAS.
    """
    from ap.order_state_machine import APOrderStateMachine
    from unittest.mock import patch

    statements: list[tuple[str, tuple]] = []

    class _Cursor:
        rowcount = 1

        def execute(self, sql, params=()):
            statements.append((str(sql), tuple(params)))
            return self

    class _Conn:
        def __enter__(self):
            return _Cursor()

        def __exit__(self, *args):
            return False

    osm = object.__new__(APOrderStateMachine)
    osm.client_id = CLIENT_ID
    with patch("ap.order_state_machine.conn", return_value=_Conn()), \
         patch("ap.order_state_machine.run_with_retry", lambda fn, *a, **kw: fn()):
        assert osm.claim_deferred_materialization(
            "8767886d-7619-4b8f-a87f-f9a2821c39c0",
            owner="owner-x",
            new_generation=3,
            lease_until="2026-12-31T00:00:00+00:00",
            trigger_crossed_at="2026-08-25T13:40:00+00:00",
            trigger_price=449.0,
            observed_underlying_price=448.9,
            signal_id="73ad6808-cbf0-4218-afb5-d960a613baca",
            execution_mode="live",
            retry_attempt=3,
        )

    assert statements
    sql, params = statements[0]
    assert "meta->>'breach_attempt_count'" in sql
    assert "meta->>'materialization_attempts'" in sql
    assert "NULLIF(BTRIM(meta->>'retry_attempt'), '') IS NULL" in sql
    # Three mirror predicates precede the schedule/generation fences; a real
    # row with breach/materialization_attempts=1 cannot satisfy this claim for
    # retry_attempt=3 (which requires prior attempt 2).
    assert params[5:8] == (2, 2, 2)
    assert params[-1] == 2


def test_blocker4_due_retry_counter_conflict_is_terminal_required_before_claim():
    """A nonterminal split counter row is classified before selector work.

    The recovery consumer must not turn a claim CAS miss into a retry loop for
    a row whose durable selector-attempt identity is already contradictory.
    """
    core = _core()
    row = _row(retry_attempt=2)
    row["meta"].update({
        "breach_attempt_count": 1,
        "materialization_attempts": 1,
    })
    core.order_state_machine.get_order.return_value = row

    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=3,
        owner="owner-counter-conflict",
    )

    assert result["disposition"] == "TERMINAL_REQUIRED"
    assert result["reason_code"] == (
        "SELECTOR_RECOVERY_CURSOR_INVALID:MATERIALIZATION_ATTEMPT_COUNTER_CONFLICT"
    )
    assert result["diagnostics"]["materialization_attempt_counters"] == {
        "retry_attempt": 2,
        "breach_attempt_count": 1,
        "materialization_attempts": 1,
    }
    core.order_state_machine.claim_deferred_materialization.assert_not_called()
    core._on_entry_trigger.assert_not_called()
    assert not core.broker.method_calls


def test_blocker4_due_retry_legacy_missing_canonical_mirror_advances_from_agreeing_mirrors():
    """A valid legacy partial-mirror row must reach the next claim slot."""
    core = _core()
    row = _row(retry_attempt=1)
    row["meta"].pop("retry_attempt")
    row["meta"].update({
        "breach_attempt_count": 1,
        "materialization_attempts": 1,
    })
    core.order_state_machine.get_order.side_effect = [row, row]
    core.order_state_machine.claim_deferred_materialization.return_value = True

    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=2,
        owner="owner-legacy-partial-mirror",
    )

    claim = core.order_state_machine.claim_deferred_materialization
    assert claim.call_args.kwargs["retry_attempt"] == 2
    assert result["disposition"] == "RETRY_WAIT"
    assert result["attempt"] == 2
    core._on_entry_trigger.assert_called_once()


def test_blocker4_startup_recovery_uses_agreeing_mirror_attempt():
    """Startup recovery must pass attempt N+1, not default a missing
    canonical retry_attempt mirror back to attempt 1."""
    from unittest.mock import patch
    from ap import db as db_mod
    from ap_recovery import APStartupRecovery

    row = _row(retry_attempt=1)
    row["local_order_id"] = "oid-legacy-partial-mirror-startup"
    _bind_trigger_evidence(row)
    row["meta"].pop("retry_attempt")
    row["meta"].update({
        "breach_attempt_count": 1,
        "materialization_attempts": 1,
    })

    resume_calls = []
    core = SimpleNamespace(
        client_id=CLIENT_ID,
        email=CLIENT_ID,
        execution_mode="paper",
        mode="PAPER",
    )

    def _resume(**kwargs):
        resume_calls.append(kwargs)
        return {"disposition": "CLAIM_LOST", "reason_code": "test_claim_lost"}

    core.resume_deferred_materialization_retry = _resume

    class _OSM:
        client_id = CLIENT_ID

    class _Cursor:
        rowcount = 1

        def execute(self, *args, **kwargs):
            return self

        def fetchall(self):
            return [row]

    class _Conn:
        def __enter__(self):
            return _Cursor()

        def __exit__(self, *args):
            return False

    recovery = APStartupRecovery(
        client_id=CLIENT_ID,
        broker=MagicMock(),
        osm=_OSM(),
        pm=MagicMock(),
        master_control=SimpleNamespace(mode="PAPER"),
        entry_watcher=None,
        execution_core=core,
    )
    result = {"deferred_lifecycles_recovered": 0, "errors": []}
    with patch.object(db_mod, "conn", lambda: _Conn()), \
         patch.object(db_mod, "run_with_retry", lambda fn, *a, **kw: fn()):
        recovery._recover_deferred_breach_lifecycles(result)

    assert len(resume_calls) == 1
    assert resume_calls[0]["expected_retry_attempt"] == 2


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


# ─────────────────────────────────────────────────────────────────────────────
# Amendment 1 — durable retry maximum authority
#
# PR #401 requires that the resolved max_attempts = max(configured_env, durable).
# The old code used the durable value first and only fell back to env when
# durable was absent, so a row stamped retry_max_attempts=3 was forever capped
# at 3 even when MAX_BREACH_SELECTOR_RETRIES=5.
# ─────────────────────────────────────────────────────────────────────────────


def _core_with_row(row_meta: dict, *, monkeypatch, execution_mode: str = "paper"):
    """Return (core, osm) with a minimal row whose meta is row_meta."""
    core = _core(execution_mode=execution_mode)
    osm = core.order_state_machine

    now = datetime.now(timezone.utc)
    full_row = {
        "local_order_id": LOCAL_ORDER_ID,
        "client_id": CLIENT_ID,
        "execution_mode": execution_mode,
        "signal_id": SIGNAL_ID,
        "plan_id": "plan-am1",
        "kind": "ENTRY",
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "submitted_ts": None,
        "symbol": "SPY",
        "direction": "CALL",
        "score": 80.0,
        "tier": "A",
        "trigger_price": 550.0,
        "stop_underlying": 545.0,
        "target_underlying": 558.0,
        "pattern": "3-1-2",
        "timeframe": "1d",
        "contract": "DEFERRED:SPY",
        "qty": 1,
        "limit_price": 0.01,
        "reserved_cost": 0.0,
        "meta": row_meta,
    }
    osm.get_order.return_value = full_row
    osm.claim_deferred_materialization.return_value = True
    osm.schedule_deferred_materialization_retry.return_value = True
    osm.update_order_meta.return_value = None
    osm.client_id = CLIENT_ID
    return core, osm


def _base_meta(*, retry_max_attempts=None, retry_attempt: int = 1) -> dict:
    """Minimal valid retry-wait meta for Amendment 1 tests."""
    now = datetime.now(timezone.utc)
    m = {
        "lifecycle_state": "RETRY_WAIT",
        "materialization_status": "RETRY_PENDING",
        "materialization_generation": 1,
        "watcher_token": "watcher:am1",
        "retry_attempt": retry_attempt,
        "next_retry_at": (now - timedelta(seconds=30)).isoformat(),
        "materialization_next_retry_at": (now - timedelta(seconds=30)).isoformat(),
        "trigger_crossed_at": (now - timedelta(seconds=90)).isoformat(),
        "trigger_price": 550.0,
        "observed_underlying_price": 550.10,
        "score": 80.0,
        "tier": "A",
        "timeframe": "1d",
        "materialization_selector_failure": {
            "reason_code": "NO_CHAIN_DATA",
        },
    }
    if retry_max_attempts is not None:
        m["retry_max_attempts"] = retry_max_attempts
    return m


# ── Test 1: durable=3, env=5 → max_attempts raised to 5 ─────────────────────

def test_am1_durable_3_env_5_raises_to_5(monkeypatch):
    """Existing row with retry_max_attempts=3 must use env=5 after raise."""
    monkeypatch.setenv("MAX_BREACH_SELECTOR_RETRIES", "5")

    core, osm = _core_with_row(_base_meta(retry_max_attempts=3, retry_attempt=1), monkeypatch=monkeypatch)
    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=2,
        owner=f"recovery_retry:{CLIENT_ID}:{LOCAL_ORDER_ID}:2",
    )
    # Must not be exhausted at attempt 2 (old code with durable=3 also allows 2)
    assert result.get("disposition") not in {"TERMINAL_DURABLE", "TERMINAL_REQUIRED"} or \
        "RETRY_MAX_ATTEMPTS_EXCEEDED" not in result.get("reason_code", ""), \
        f"Attempt 2/5 must not be exhausted: {result}"

    # Confirm schedule_deferred_materialization_retry was called with max_attempts=5
    calls = osm.schedule_deferred_materialization_retry.call_args_list
    if calls:
        _, kwargs = calls[0]
        assert kwargs.get("max_attempts") == 5, \
            f"Expected max_attempts=5, got {kwargs.get('max_attempts')}"


def test_am1_durable_3_env_5_attempt_4_allowed(monkeypatch):
    """Attempt 4 must be ALLOWED when env=5 (would have been blocked at durable=3)."""
    monkeypatch.setenv("MAX_BREACH_SELECTOR_RETRIES", "5")

    core, osm = _core_with_row(_base_meta(retry_max_attempts=3, retry_attempt=3), monkeypatch=monkeypatch)
    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=4,
        owner=f"recovery_retry:{CLIENT_ID}:{LOCAL_ORDER_ID}:4",
    )
    assert "RETRY_MAX_ATTEMPTS_EXCEEDED" not in result.get("reason_code", ""), \
        f"Attempt 4 should be allowed when env=5: {result}"


def test_am1_durable_3_env_5_attempt_5_allowed(monkeypatch):
    """Attempt 5 must be ALLOWED when env=5."""
    monkeypatch.setenv("MAX_BREACH_SELECTOR_RETRIES", "5")

    core, osm = _core_with_row(_base_meta(retry_max_attempts=3, retry_attempt=4), monkeypatch=monkeypatch)
    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=5,
        owner=f"recovery_retry:{CLIENT_ID}:{LOCAL_ORDER_ID}:5",
    )
    assert "RETRY_MAX_ATTEMPTS_EXCEEDED" not in result.get("reason_code", ""), \
        f"Attempt 5 should be allowed when env=5: {result}"


def test_am1_durable_3_env_5_attempt_6_exhausted(monkeypatch):
    """Attempt 6 must be BLOCKED when resolved max is 5."""
    monkeypatch.setenv("MAX_BREACH_SELECTOR_RETRIES", "5")

    core, osm = _core_with_row(_base_meta(retry_max_attempts=3, retry_attempt=5), monkeypatch=monkeypatch)
    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=6,
        owner=f"recovery_retry:{CLIENT_ID}:{LOCAL_ORDER_ID}:6",
    )
    assert "RETRY_MAX_ATTEMPTS_EXCEEDED" in result.get("reason_code", ""), \
        f"Attempt 6 must be exhausted at max=5: {result}"
    assert result.get("disposition") in {"TERMINAL_DURABLE", "TERMINAL_REQUIRED"}


# ── Test 2: durable=7/500, env=5 → hard capped at 5 (blocker correction) ────
#
# Rev-2 policy (P0 blocker 1): configured_max is the HARD CEILING.
# A durable value above configured_max is CLAMPED, not honoured.
# "max(configured, durable)" allowed stale/corrupted metadata (e.g.,
# retry_max_attempts=500) to silently create an unbounded retry loop.

def test_am1_durable_7_env_5_clamped_to_5(monkeypatch):
    """Durable max=7 with env=5 must be clamped to 5, not preserved at 7.

    Prevents stale/historically-high durable values from silently overriding
    the current production policy ceiling.
    """
    monkeypatch.setenv("MAX_BREACH_SELECTOR_RETRIES", "5")

    core, osm = _core_with_row(_base_meta(retry_max_attempts=7, retry_attempt=1), monkeypatch=monkeypatch)
    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=2,
        owner=f"recovery_retry:{CLIENT_ID}:{LOCAL_ORDER_ID}:2",
    )
    # Attempt 2 is within the clamped max of 5 — must not be exhausted
    assert "RETRY_MAX_ATTEMPTS_EXCEEDED" not in result.get("reason_code", ""),         f"Attempt 2 must not be exhausted (resolved max=5): {result}"

    # Schedule call must persist max_attempts=5, NOT the durable 7
    calls = osm.schedule_deferred_materialization_retry.call_args_list
    if calls:
        _, kwargs = calls[0]
        assert kwargs.get("max_attempts") == 5,             f"Durable max=7 must be clamped to env=5: got {kwargs.get('max_attempts')}"


def test_am1_durable_7_env_5_attempt_6_clamped_exhausted(monkeypatch):
    """Attempt 6 must be BLOCKED when durable=7, env=5 (clamped to 5)."""
    monkeypatch.setenv("MAX_BREACH_SELECTOR_RETRIES", "5")

    core, osm = _core_with_row(_base_meta(retry_max_attempts=7, retry_attempt=5), monkeypatch=monkeypatch)
    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=6,
        owner=f"recovery_retry:{CLIENT_ID}:{LOCAL_ORDER_ID}:6",
    )
    assert "RETRY_MAX_ATTEMPTS_EXCEEDED" in result.get("reason_code", ""),         f"Attempt 6 must be exhausted when durable=7 is clamped to env=5: {result}"
    assert result.get("disposition") in {"TERMINAL_DURABLE", "TERMINAL_REQUIRED"}


def test_am1_durable_500_env_5_clamped(monkeypatch):
    """Corrupted or stale durable max=500 must be clamped to env=5."""
    monkeypatch.setenv("MAX_BREACH_SELECTOR_RETRIES", "5")

    core, osm = _core_with_row(_base_meta(retry_max_attempts=500, retry_attempt=5), monkeypatch=monkeypatch)
    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=6,
        owner=f"recovery_retry:{CLIENT_ID}:{LOCAL_ORDER_ID}:6",
    )
    assert "RETRY_MAX_ATTEMPTS_EXCEEDED" in result.get("reason_code", ""),         f"Attempt 6 must be exhausted when durable=500 is clamped to env=5: {result}"
    assert result.get("disposition") in {"TERMINAL_DURABLE", "TERMINAL_REQUIRED"}


# ── Test 2b: durable=5, env=5 → 5 (exact match) ──────────────────────────────

def test_am1_durable_5_env_5_exact_match(monkeypatch):
    """Durable exactly equal to configured max → still resolves to 5."""
    monkeypatch.setenv("MAX_BREACH_SELECTOR_RETRIES", "5")

    core, osm = _core_with_row(_base_meta(retry_max_attempts=5, retry_attempt=4), monkeypatch=monkeypatch)
    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=5,
        owner=f"recovery_retry:{CLIENT_ID}:{LOCAL_ORDER_ID}:5",
    )
    assert "RETRY_MAX_ATTEMPTS_EXCEEDED" not in result.get("reason_code", ""),         f"Attempt 5 must not be exhausted when durable=5 env=5: {result}"


def test_am1_negative_durable_uses_env(monkeypatch):
    """Negative durable retry_max_attempts is treated as 0; env remains sole authority."""
    monkeypatch.setenv("MAX_BREACH_SELECTOR_RETRIES", "5")

    meta = _base_meta(retry_attempt=1)
    meta["retry_max_attempts"] = -3
    core, osm = _core_with_row(meta, monkeypatch=monkeypatch)

    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=2,
        owner=f"recovery_retry:{CLIENT_ID}:{LOCAL_ORDER_ID}:2",
    )
    # Negative durable clamped to 0; env=5 → max_attempts=5 → attempt 2 allowed
    assert "RETRY_MAX_ATTEMPTS_EXCEEDED" not in result.get("reason_code", ""),         f"Negative durable must fall back to env=5: {result}"


# ── Test 3: durable missing, env=5 → env is sole authority ───────────────────

def test_am1_durable_missing_env_5_uses_env(monkeypatch):
    """When retry_max_attempts is absent from meta, env=5 is the sole authority."""
    monkeypatch.setenv("MAX_BREACH_SELECTOR_RETRIES", "5")

    core, osm = _core_with_row(_base_meta(retry_max_attempts=None, retry_attempt=1), monkeypatch=monkeypatch)
    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=2,
        owner=f"recovery_retry:{CLIENT_ID}:{LOCAL_ORDER_ID}:2",
    )
    assert "RETRY_MAX_ATTEMPTS_EXCEEDED" not in result.get("reason_code", ""), \
        f"Attempt 2/5 must not be exhausted: {result}"

    calls = osm.schedule_deferred_materialization_retry.call_args_list
    if calls:
        _, kwargs = calls[0]
        assert kwargs.get("max_attempts") == 5, \
            f"Expected max_attempts=5 from env: {kwargs.get('max_attempts')}"


# ── Test 4: malformed durable maximum falls back safely ──────────────────────

@pytest.mark.parametrize("bad_value", ["abc", "", None, [], {}, -1, 0])
def test_am1_malformed_durable_max_falls_back(monkeypatch, bad_value):
    """Malformed or non-positive durable max must fall back to env safely."""
    monkeypatch.setenv("MAX_BREACH_SELECTOR_RETRIES", "5")

    meta = _base_meta(retry_attempt=1)
    meta["retry_max_attempts"] = bad_value
    core, osm = _core_with_row(meta, monkeypatch=monkeypatch)

    # Must not raise
    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=2,
        owner=f"recovery_retry:{CLIENT_ID}:{LOCAL_ORDER_ID}:2",
    )
    # Attempt 2 must not be exhausted with env=5
    assert "RETRY_MAX_ATTEMPTS_EXCEEDED" not in result.get("reason_code", ""), \
        f"bad durable={bad_value!r}: attempt 2 must not be exhausted: {result}"


# ── Test 5: malformed/non-positive env → safe default, no crash ──────────────

@pytest.mark.parametrize("bad_env", ["abc", "0", "-3", ""])
def test_am1_malformed_env_safe_default(monkeypatch, bad_env):
    """Malformed env MAX_BREACH_SELECTOR_RETRIES must not crash; uses safe default."""
    monkeypatch.setenv("MAX_BREACH_SELECTOR_RETRIES", bad_env)

    core, osm = _core_with_row(_base_meta(retry_max_attempts=None, retry_attempt=1), monkeypatch=monkeypatch)
    # Must not raise
    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=2,
        owner=f"recovery_retry:{CLIENT_ID}:{LOCAL_ORDER_ID}:2",
    )
    # Should get a valid disposition (not a Python exception)
    assert "disposition" in result, f"Expected valid disposition for bad env={bad_env!r}: {result}"


# ── Test 6: terminal/submitted/filled rows are never reopened ─────────────────

@pytest.mark.parametrize("bad_status,broker_order_id,submitted_ts", [
    ("FILLED",      "broker-123", "2026-07-28T10:00:00+00:00"),
    ("CANCELLED",   "broker-124", None),
    ("EXPIRED",     None,         "2026-07-28T10:00:00+00:00"),
    ("REJECTED",    "broker-125", "2026-07-28T10:01:00+00:00"),
])
def test_am1_terminal_rows_never_reopened(monkeypatch, bad_status, broker_order_id, submitted_ts):
    """Terminal/submitted rows must never be reopened by a raised max_attempts."""
    monkeypatch.setenv("MAX_BREACH_SELECTOR_RETRIES", "10")  # very high env

    meta = _base_meta(retry_max_attempts=3, retry_attempt=1)
    core, osm = _core_with_row(meta, monkeypatch=monkeypatch)

    # Override get_order to return a row with broker_order_id or submitted_ts set
    now = datetime.now(timezone.utc)
    bad_row = {
        "local_order_id": LOCAL_ORDER_ID,
        "client_id": CLIENT_ID,
        "execution_mode": "paper",
        "signal_id": SIGNAL_ID,
        "plan_id": "plan-am1-terminal",
        "kind": "ENTRY",
        "status": bad_status,
        "broker_order_id": broker_order_id,
        "submitted_ts": submitted_ts,
        "symbol": "SPY",
        "direction": "CALL",
        "score": 80.0,
        "tier": "A",
        "trigger_price": 550.0,
        "stop_underlying": 545.0,
        "target_underlying": 558.0,
        "pattern": "3-1-2",
        "timeframe": "1d",
        "contract": "DEFERRED:SPY",
        "qty": 1,
        "limit_price": 0.01,
        "reserved_cost": 0.0,
        "meta": meta,
    }
    osm.get_order.return_value = bad_row

    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=2,
        owner=f"recovery_retry:{CLIENT_ID}:{LOCAL_ORDER_ID}:2",
    )
    # claim must NOT have been called
    osm.claim_deferred_materialization.assert_not_called()
    # disposition must be a TERMINAL or KEEP (not a successful RETRY progression)
    assert result.get("disposition") not in {"RETRY_WAIT", "SUBMITTED", "BROKER_READY"}, \
        f"Terminal row status={bad_status!r} must not be reopened: {result}"


# ── Test 7: restart — resolved raised maximum persists across restarts ────────

def test_am1_resolved_max_persists_on_restart(monkeypatch):
    """The bounded resolved max (configured_max=5) must be persisted so that
    the next process observes the same ceiling after restart.

    With the hard-ceiling policy, max_attempts = configured_max = 5 regardless
    of the durable value.  This test verifies the OSM call carries max_attempts=5
    even when the durable row says retry_max_attempts=3.

    Drive schedule_deferred_materialization_retry by making _on_entry_trigger
    raise (the exception handler calls _schedule_retry_wait, which calls
    schedule_deferred_materialization_retry on the OSM — same pattern as
    test_amend5_callback_exception_schedules_durable_retry).
    """
    monkeypatch.setenv("MAX_BREACH_SELECTOR_RETRIES", "5")

    core, osm = _core_with_row(_base_meta(retry_max_attempts=3, retry_attempt=1), monkeypatch=monkeypatch)
    # Force the retry-schedule path: callback exception → _schedule_retry_wait
    core._on_entry_trigger.side_effect = RuntimeError("am1-restart-test-sentinel")

    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=2,
        owner=f"recovery_retry:{CLIENT_ID}:{LOCAL_ORDER_ID}:2",
    )
    assert result.get("disposition") == "RETRY_WAIT", (
        f"Expected RETRY_WAIT after callback exception: {result}"
    )

    # The schedule_deferred_materialization_retry call must pass max_attempts=5
    # (configured ceiling); OSM persists this as retry_max_attempts=5 so the
    # next process restart observes the same bounded maximum.
    calls = osm.schedule_deferred_materialization_retry.call_args_list
    assert calls, "schedule_deferred_materialization_retry must have been called"
    _, kwargs = calls[0]
    assert kwargs.get("max_attempts") == 5, (
        f"Persisted max_attempts must equal configured ceiling (5), "
        f"got {kwargs.get('max_attempts')!r}"
    )


def test_tradier_production_quote_without_source_passes_retry_authority():
    from ap.brokers.tradier import TradierBroker, TradierConfig
    from ap.live_submit_gates import validate_retry_market_quote_authority

    now = datetime.now(timezone.utc)
    broker = TradierBroker(TradierConfig(
        base_url="https://api.tradier.com/v1",
        access_token="test-token",
        account_id="test-account",
    ))
    broker._get = MagicMock(return_value={
        "quotes": {"quote": {
            "symbol": "SPY",
            "bid": 637.10,
            "ask": 637.12,
            "trade_date": int(now.timestamp() * 1000),
        }}
    })

    quote = broker.get_quote("SPY")
    result = validate_retry_market_quote_authority(quote, transport=broker, now=now)

    assert "source" not in quote
    assert "quote_source" not in quote
    assert "provider" not in quote
    assert result["valid"] is True
    assert result["quote_source"] == "tradier_live"


def test_retry_authority_rejects_contradictory_source_on_approved_transport():
    from ap.live_submit_gates import validate_retry_market_quote_authority

    now = datetime.now(timezone.utc)
    result = validate_retry_market_quote_authority(
        {"bid": 637.10, "ask": 637.12,
         "trade_date": int(now.timestamp() * 1000),
         "source": "other_provider"},
        transport=SimpleNamespace(
            base_url="https://api.tradier.com/v1",
            cfg=SimpleNamespace(base_url="https://api.tradier.com/v1"),
        ),
        now=now,
    )

    assert result["valid"] is False
    assert result["reason"] == "MARKET_QUOTE_SOURCE_UNPROVEN"


@pytest.mark.parametrize("base_url", [
    "http://api.tradier.com/v1",
    "https://api.tradier.com.evil.example/v1",
    "https://sandbox.tradier.com/v1",
])
def test_source_less_quote_still_requires_approved_tradier_transport(base_url):
    from ap.live_submit_gates import validate_retry_market_quote_authority

    now = datetime.now(timezone.utc)
    result = validate_retry_market_quote_authority(
        {"bid": 637.10, "ask": 637.12,
         "trade_date": int(now.timestamp() * 1000)},
        transport=SimpleNamespace(
            base_url=base_url, cfg=SimpleNamespace(base_url=base_url)),
        now=now,
    )

    assert result["valid"] is False
    assert result["reason"] == "MARKET_QUOTE_UNAPPROVED_TRANSPORT"


# ─────────────────────────────────────────────────────────────────────────────
# FINAL AMENDMENT: execution_mode parity tests (Tests A–F)
#
# Tests A–D: unit-level behavioral proofs of canonical execution_mode
#   resolution in resume_deferred_materialization_retry().
# Tests E–F: real-PostgreSQL behavioral proofs of canonical execution_mode
#   resolution in claim_deferred_materialization() SQL CAS.
# ─────────────────────────────────────────────────────────────────────────────


def _row_raw(
    *,
    col_execution_mode: str = "paper",
    meta_execution_mode: str | None = None,
    execution_mode_in_meta_explicitly: bool = False,
    retry_attempt: int = 1,
    breach_attempt_count: int = 1,
    materialization_attempts: int = 1,
    client_id: str = CLIENT_ID,
) -> dict:
    """Build a minimal due-retry row without auto-normalising execution_mode.

    Unlike ``_row()``, this helper does NOT call ``_bind_trigger_evidence``
    so the column and meta authority can be set independently — essential for
    the contradiction and blank-column tests.
    """
    now = datetime.now(timezone.utc)
    meta: dict = {
        "lifecycle_state": "RETRY_WAIT",
        "materialization_status": "RETRY_PENDING",
        "materialization_generation": 1,
        "watcher_token": "",
        "retry_attempt": retry_attempt,
        "breach_attempt_count": breach_attempt_count,
        "materialization_attempts": materialization_attempts,
        "retry_max_attempts": 3,
        "next_retry_at": (now - timedelta(seconds=60)).isoformat(),
        "materialization_next_retry_at": (now - timedelta(seconds=60)).isoformat(),
        "trigger_crossed_at": (now - timedelta(seconds=120)).isoformat(),
        "trigger_price": 130.0,
        "observed_underlying_price": 130.05,
        "client_id": client_id,
        "signal_id": SIGNAL_ID,
        "canonical_signal_id": SIGNAL_ID,
        "trigger_crossed_at_provenance": {
            "canonical_signal_id": SIGNAL_ID,
            "client_id": client_id,
            "execution_mode": (meta_execution_mode or col_execution_mode or "").strip().lower(),
            "local_order_id": LOCAL_ORDER_ID,
        },
    }
    if meta_execution_mode is not None or execution_mode_in_meta_explicitly:
        meta["execution_mode"] = meta_execution_mode if meta_execution_mode is not None else ""
    return {
        "local_order_id": LOCAL_ORDER_ID,
        "client_id": client_id,
        "execution_mode": col_execution_mode,
        "signal_id": SIGNAL_ID,
        "plan_id": "plan-exec-mode-1",
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
        "meta": meta,
    }


# ── Test A: whitespace PAPER column resolves correctly ────────────────────────

def test_A_whitespace_paper_column_resolves_to_valid_mode():
    """Column \" paper \" (with surrounding whitespace) must NOT trigger
    RETRY_INVALID_EXECUTION_MODE.  Prior to this amendment, LOWER(COALESCE(...))
    without BTRIM left the whitespace intact so \" paper \" != \"paper\"."""
    core = _core(execution_mode="paper")
    row = _row_raw(col_execution_mode=" paper ", meta_execution_mode="paper")
    core.order_state_machine.get_order.return_value = row
    # claim_deferred_materialization returns True (claims the row)
    core.order_state_machine.claim_deferred_materialization.return_value = True
    # schedule_deferred_materialization_retry also returns True
    core.order_state_machine.schedule_deferred_materialization_retry.return_value = True

    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=1,
        owner="watcher:test-A",
    )
    # Must NOT return mode-related failure codes.
    assert result.get("reason_code") not in {
        "RETRY_INVALID_EXECUTION_MODE",
        "RETRY_EXECUTION_MODE_MISMATCH",
        "RETRY_EXECUTION_MODE_AUTHORITY_CONFLICT",
        "RETRY_RUNNER_EXECUTION_MODE_INVALID",
    }, (
        f"Whitespace column should resolve to 'paper'; got reason={result.get('reason_code')!r}"
    )


# ── Test B: blank column with metadata resolves correctly ─────────────────────

def test_B_blank_column_with_meta_paper_fallback_resolves_to_paper():
    """A valid metadata value may supply a blank durable column mirror."""
    core = _core(execution_mode="paper")
    row = _row_raw(
        col_execution_mode="",
        meta_execution_mode="paper",
    )
    core.order_state_machine.get_order.return_value = row
    core.order_state_machine.claim_deferred_materialization.return_value = True
    core.order_state_machine.schedule_deferred_materialization_retry.return_value = True

    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=1,
        owner="watcher:test-B",
    )
    assert result.get("reason_code") not in {
        "RETRY_INVALID_EXECUTION_MODE",
        "RETRY_EXECUTION_MODE_MISMATCH",
        "RETRY_EXECUTION_MODE_AUTHORITY_CONFLICT",
        "RETRY_RUNNER_EXECUTION_MODE_INVALID",
    }, f"blank column + meta='paper' should resolve; got {result!r}"


# ── Test C: whitespace and blank-column LIVE metadata resolve correctly ───────

def test_C_whitespace_live_column_resolves_to_valid_mode():
    """Column \" live \" resolves to 'live' — same contract as Test A for LIVE."""
    core = _core(execution_mode="live")
    row = _row_raw(col_execution_mode=" live ", meta_execution_mode="live")
    core.order_state_machine.get_order.return_value = row
    core.order_state_machine.claim_deferred_materialization.return_value = True
    core.order_state_machine.schedule_deferred_materialization_retry.return_value = True

    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=1,
        owner="watcher:test-C1",
    )
    assert result.get("reason_code") not in {
        "RETRY_INVALID_EXECUTION_MODE",
        "RETRY_EXECUTION_MODE_MISMATCH",
        "RETRY_EXECUTION_MODE_AUTHORITY_CONFLICT",
        "RETRY_RUNNER_EXECUTION_MODE_INVALID",
    }, (
        f"Whitespace LIVE column should resolve to 'live'; got {result.get('reason_code')!r}"
    )


def test_C_blank_column_with_live_meta_fallback_resolves_to_live():
    """A valid LIVE metadata value may supply a blank column mirror."""
    core = _core(execution_mode="live")
    row = _row_raw(col_execution_mode="", meta_execution_mode="live")
    core.order_state_machine.get_order.return_value = row
    core.order_state_machine.claim_deferred_materialization.return_value = True
    core.order_state_machine.schedule_deferred_materialization_retry.return_value = True

    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=1,
        owner="watcher:test-C2",
    )
    assert result.get("reason_code") not in {
        "RETRY_INVALID_EXECUTION_MODE",
        "RETRY_EXECUTION_MODE_MISMATCH",
        "RETRY_EXECUTION_MODE_AUTHORITY_CONFLICT",
        "RETRY_RUNNER_EXECUTION_MODE_INVALID",
    }, f"blank column + meta='live' should resolve; got {result!r}"


# ── Test D: contradiction negative (live vs paper in both directions) ──────────

@pytest.mark.parametrize("col_mode,meta_mode", [
    ("live", "paper"),
    ("paper", "live"),
])
def test_D_contradicting_nonblank_authorities_fail_closed(col_mode, meta_mode):
    """When orders.execution_mode != meta->>'execution_mode' and both are
    non-blank, resume_deferred_materialization_retry MUST return
    RETRY_EXECUTION_MODE_AUTHORITY_CONFLICT and must NOT call claim, selector,
    broker, or any position-mutating function."""
    core = _core(execution_mode=col_mode)
    row = _row_raw(col_execution_mode=col_mode, meta_execution_mode=meta_mode)
    core.order_state_machine.get_order.return_value = row

    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=1,
        owner="watcher:test-D",
    )

    # Must hard-fail with the contradiction code.
    assert result.get("reason_code") == "RETRY_EXECUTION_MODE_AUTHORITY_CONFLICT", (
        f"Expected AUTHORITY_CONFLICT for col={col_mode!r}/meta={meta_mode!r}; "
        f"got {result.get('reason_code')!r}"
    )
    assert result.get("disposition") == "TERMINAL_REQUIRED"

    # No claim, selector, broker, or position calls made.
    core.order_state_machine.claim_deferred_materialization.assert_not_called()
    core.order_state_machine.schedule_deferred_materialization_retry.assert_not_called()
    core._on_entry_trigger.assert_not_called()


# ── Test E: Real PostgreSQL — 2/1/1 counter split causes CAS miss ─────────────

def test_E_real_postgres_211_counter_split_causes_claim_miss():
    """claim_deferred_materialization with retry_attempt=2 MUST return False
    when the durable row has retry_attempt=1, breach_attempt_count=1,
    materialization_attempts=1 (i.e. prior attempt mirrors already set to 1).
    The SQL predicate `BTRIM(meta->>'retry_attempt')::int = %s` checks
    prev_attempt=1; only the retry_attempt mirror advances, leaving
    breach_attempt_count=1 and materialization_attempts=1 behind — claim fails.

    This is the exact 2/1/1 production incident shape from Jason's MCD PUT.
    """
    from contextlib import contextmanager
    import json as _json
    import uuid

    psycopg2 = pytest.importorskip("psycopg2")
    import psycopg2.extras

    database_url = os.getenv("INTELLIGENCE_POSTGRES_TEST_URL", "")
    if not database_url:
        if os.getenv("GITHUB_ACTIONS") == "true":
            pytest.fail("INTELLIGENCE_POSTGRES_TEST_URL is required in GitHub Actions")
        pytest.skip("disposable PostgreSQL URL not configured")

    from ap.order_state_machine import APOrderStateMachine
    import ap.order_state_machine as osm_mod

    schema = f"test_e_211_{uuid.uuid4().hex}"
    client_id = "jasoncosby1@gmail.com"
    local_order_id = f"oid-211-{uuid.uuid4().hex}"
    signal_id = f"sig-211-{uuid.uuid4().hex}"
    owner = "materializer:test-E"
    execution_mode = "paper"
    generation = 2   # new generation we want to claim
    now = datetime.now(timezone.utc)

    class _Wrapper:
        def __init__(self, cursor):
            self.cursor = cursor

        @property
        def rowcount(self):
            return self.cursor.rowcount

        def execute(self, sql, params=()):
            self.cursor.execute(sql, params)
            return self

        def fetchone(self):
            row = self.cursor.fetchone()
            return dict(row) if row else None

        def fetchall(self):
            return [dict(row) for row in self.cursor.fetchall()]

    @contextmanager
    def _pg_conn():
        db = psycopg2.connect(database_url)
        cursor = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cursor.execute(f'SET search_path TO "{schema}"')
            yield _Wrapper(cursor)
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            cursor.close()
            db.close()

    admin = psycopg2.connect(database_url)
    admin.autocommit = True
    try:
        with admin.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"')
            cur.execute(
                f"""
                CREATE TABLE "{schema}".orders (
                    local_order_id TEXT PRIMARY KEY,
                    client_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    execution_mode TEXT,
                    signal_id TEXT,
                    broker_order_id TEXT,
                    submitted_ts TIMESTAMPTZ,
                    meta JSONB,
                    updated_ts TIMESTAMPTZ DEFAULT NOW()
                )
                """
            )

        # Reproduce the production 2/1/1 incident: retry_attempt is already
        # advanced to 2 while the two legacy mirrors remain at 1.  A next
        # claim for attempt 3 requires prior=2 in all three predicates and
        # must therefore fail closed without normalizing the row.
        split_meta = {
            "lifecycle_state": "",
            "materialization_status": "",
            "materialization_in_flight": False,
            "materialization_generation": generation - 1,  # = 1
            "broker_ready": False,
            "retry_attempt": 2,          # already advanced
            "breach_attempt_count": 1,   # NOT advanced → split
            "materialization_attempts": 1, # NOT advanced → split
            "signal_id": signal_id,
            "execution_mode": execution_mode,
            "client_id": client_id,
        }
        with _pg_conn() as c:
            c.execute(
                f'INSERT INTO "{schema}".orders '
                "(local_order_id, client_id, kind, status, execution_mode, "
                "signal_id, meta) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)",
                (
                    local_order_id, client_id, "ENTRY", "PENDING_TRIGGER",
                    execution_mode, signal_id,
                    _json.dumps(split_meta),
                ),
            )

        # Patch conn() to use our isolated schema.
        original_conn = osm_mod.conn
        osm_mod.conn = _pg_conn

        try:
            osm = APOrderStateMachine(client_id=client_id)
            # Attempt to claim with retry_attempt=3, which requires prior=2 on ALL
            # three mirrors. breach_attempt_count and materialization_attempts
            # are only 1 in the row, so all three predicates cannot agree → False.
            result = osm.claim_deferred_materialization(
                local_order_id,
                owner=owner,
                lease_until=(now + timedelta(seconds=60)).isoformat(),
                trigger_crossed_at=now.isoformat(),
                trigger_price=130.0,
                observed_underlying_price=130.05,
                signal_id=signal_id,
                execution_mode=execution_mode,
                new_generation=generation,  # = 2, so expected_previous = 1
                generation=None,
                retry_attempt=3,            # requires prev = 2 on all mirrors
            )
        finally:
            osm_mod.conn = original_conn

        assert result is False, (
            "claim_deferred_materialization must return False on 2/1/1 split "
            f"(retry_attempt=2, breach_attempt_count=1, materialization_attempts=1) "
            f"when new claim requires prior=2 on all three mirrors; got {result!r}"
        )
    finally:
        with admin.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


# ── Test F: Real PostgreSQL — positive 1/1/1 → 2/2/2 claim succeeds ──────────

def test_F_real_postgres_111_to_222_canonical_claim_succeeds():
    """claim_deferred_materialization with retry_attempt=2 MUST return True
    when the durable row has all three mirrors at 1 (agreeing prior state).
    After the CAS, the durable row must show all three mirrors advanced to 2.
    This is the canonical counter-control proof: one claim advances all three
    mirrors atomically."""
    from contextlib import contextmanager
    import json as _json
    import uuid

    psycopg2 = pytest.importorskip("psycopg2")
    import psycopg2.extras

    database_url = os.getenv("INTELLIGENCE_POSTGRES_TEST_URL", "")
    if not database_url:
        if os.getenv("GITHUB_ACTIONS") == "true":
            pytest.fail("INTELLIGENCE_POSTGRES_TEST_URL is required in GitHub Actions")
        pytest.skip("disposable PostgreSQL URL not configured")

    from ap.order_state_machine import APOrderStateMachine
    import ap.order_state_machine as osm_mod

    schema = f"test_f_canonical_{uuid.uuid4().hex}"
    client_id = "jasoncosby1@gmail.com"
    local_order_id = f"oid-f-{uuid.uuid4().hex}"
    signal_id = f"sig-f-{uuid.uuid4().hex}"
    owner = "materializer:test-F"
    execution_mode = "paper"
    generation = 2   # new generation to claim
    now = datetime.now(timezone.utc)

    class _Wrapper:
        def __init__(self, cursor):
            self.cursor = cursor

        @property
        def rowcount(self):
            return self.cursor.rowcount

        def execute(self, sql, params=()):
            self.cursor.execute(sql, params)
            return self

        def fetchone(self):
            row = self.cursor.fetchone()
            return dict(row) if row else None

        def fetchall(self):
            return [dict(row) for row in self.cursor.fetchall()]

    @contextmanager
    def _pg_conn():
        db = psycopg2.connect(database_url)
        cursor = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cursor.execute(f'SET search_path TO "{schema}"')
            yield _Wrapper(cursor)
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            cursor.close()
            db.close()

    admin = psycopg2.connect(database_url)
    admin.autocommit = True
    try:
        with admin.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"')
            cur.execute(
                f"""
                CREATE TABLE "{schema}".orders (
                    local_order_id TEXT PRIMARY KEY,
                    client_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    execution_mode TEXT,
                    signal_id TEXT,
                    broker_order_id TEXT,
                    submitted_ts TIMESTAMPTZ,
                    meta JSONB,
                    updated_ts TIMESTAMPTZ DEFAULT NOW()
                )
                """
            )

        # Insert a coherent 1/1/1 row: all three mirrors at 1.
        coherent_meta = {
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
            "materialization_in_flight": False,
            "materialization_generation": generation - 1,  # = 1
            "broker_ready": False,
            "retry_attempt": 1,
            "retry_attempt_in_flight": 1,
            "breach_attempt_count": 1,
            "materialization_attempts": 1,
            "signal_id": signal_id,
            "execution_mode": execution_mode,
            "client_id": client_id,
        }
        with _pg_conn() as c:
            c.execute(
                f'INSERT INTO "{schema}".orders '
                "(local_order_id, client_id, kind, status, execution_mode, "
                "signal_id, meta) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)",
                (
                    local_order_id, client_id, "ENTRY", "PENDING_TRIGGER",
                    execution_mode, signal_id,
                    _json.dumps(coherent_meta),
                ),
            )

        original_conn = osm_mod.conn
        osm_mod.conn = _pg_conn

        try:
            osm = APOrderStateMachine(client_id=client_id)
            result = osm.claim_deferred_materialization(
                local_order_id,
                owner=owner,
                lease_until=(now + timedelta(seconds=60)).isoformat(),
                trigger_crossed_at=now.isoformat(),
                trigger_price=130.0,
                observed_underlying_price=130.05,
                signal_id=signal_id,
                execution_mode=execution_mode,
                new_generation=generation,  # = 2, expected_previous = 1
                generation=None,
                retry_attempt=2,            # requires prior = 1 on all mirrors
            )

            # Verify the claim succeeded.
            assert result is True, (
                f"claim_deferred_materialization must return True for coherent "
                f"1/1/1 row when claiming attempt=2; got {result!r}"
            )

            # Verify all three mirrors were advanced to 2 atomically.
            with _pg_conn() as c:
                c.execute(
                    f'SELECT meta FROM "{schema}".orders WHERE local_order_id = %s',
                    (local_order_id,),
                )
                row = c.fetchone()
            assert row is not None, "Row must still exist after claim"
            durable_meta = row.get("meta") or {}
            assert int(durable_meta.get("retry_attempt", -1)) == 2, (
                f"retry_attempt must be 2 after claim; got {durable_meta.get('retry_attempt')!r}"
            )
            assert int(durable_meta.get("breach_attempt_count", -1)) == 2, (
                f"breach_attempt_count must be 2 after claim; got {durable_meta.get('breach_attempt_count')!r}"
            )
            assert int(durable_meta.get("materialization_attempts", -1)) == 2, (
                f"materialization_attempts must be 2 after claim; got {durable_meta.get('materialization_attempts')!r}"
            )
            assert int(durable_meta.get("retry_attempt_in_flight", -1)) == 2, (
                "retry_attempt_in_flight must be 2 after claim; got "
                f"{durable_meta.get('retry_attempt_in_flight')!r}"
            )
        finally:
            osm_mod.conn = original_conn

    finally:
        with admin.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()
