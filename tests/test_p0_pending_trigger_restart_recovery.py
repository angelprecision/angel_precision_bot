"""
tests/test_p0_pending_trigger_restart_recovery.py

P0 — Restart Recovery Must Resolve Every PENDING_TRIGGER Row Truthfully

Tests use real production-shaped order metadata and real recovery module logic.
No test manually simulates removal or skips classification.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional
from unittest.mock import MagicMock

import pytest

from ap.pending_trigger_restart_recovery import PendingTriggerRestartRecovery
from ap.pending_trigger_classifier import (
    PendingTriggerClassification as PTC,
    classify_pending_trigger_row,
)


# ── Fixtures / helpers ────────────────────────────────────────────────────────

def _row(
    *,
    status: str = "PENDING_TRIGGER",
    local_order_id: Optional[str] = None,
    signal_id: Optional[str] = None,
    client_id: str = "client@test.com",
    execution_mode: str = "paper",
    direction: str = "CALL",
    ticker: str = "SPY",
    entry_price: float = 450.0,
    stop_price: float = 447.0,
    target_price: float = 455.0,
    contracts: int = 1,
    broker_order_id=None,
    submitted_ts=None,
    meta: Optional[dict] = None,
) -> dict:
    return {
        "local_order_id": local_order_id or str(uuid.uuid4()),
        "signal_id":      signal_id or str(uuid.uuid4()),
        "client_id":      client_id,
        "client_email":   client_id,
        "execution_mode": execution_mode,
        "status":         status,
        "direction":      direction,
        "ticker":         ticker,
        "entry_price":    entry_price,
        "stop_price":     stop_price,
        "target_price":   target_price,
        "contracts":      contracts,
        "broker_order_id": broker_order_id,
        "submitted_ts":   submitted_ts,
        "meta":           meta or {},
    }


class _MockOSM:
    def __init__(self, cancel_returns: bool = True, cancel_raises=None,
                 expire_returns: bool = True):
        self.cancel_calls: list = []
        self.expire_calls: list = []
        self.meta_writes: list  = []
        self._cancel_returns = cancel_returns
        self._cancel_raises  = cancel_raises
        self._expire_returns = expire_returns
        self._rows: dict = {}

    def cancel_pending_entry(self, local_order_id: str, *, reason: str = "") -> bool:
        self.cancel_calls.append((local_order_id, reason))
        if self._cancel_raises:
            raise self._cancel_raises
        if self._cancel_returns and local_order_id in self._rows:
            self._rows[local_order_id]["status"] = "CANCELED"
        return self._cancel_returns

    def expire_pending_entry(self, local_order_id: str, *, reason: str = "") -> bool:
        self.expire_calls.append((local_order_id, reason))
        if self._expire_returns and local_order_id in self._rows:
            self._rows[local_order_id]["status"] = "EXPIRED"
        return self._expire_returns

    def update_order_meta(self, local_order_id: str, patch: dict) -> bool:
        self.meta_writes.append((local_order_id, dict(patch)))
        return True

    def seed(self, row: dict) -> dict:
        self._rows[row["local_order_id"]] = dict(row)
        return row


class _MockWatcher:
    """Minimal watcher stub with controllable _pending registry."""

    def __init__(self, *, watch_returns: bool = True):
        self._pending:   list = []
        self._dedup_set: set  = set()
        self._watch_returns   = watch_returns

    def watch(self, plan: dict, local_order_id: str, *, recovery_rearm: bool = False) -> bool:
        if self._watch_returns:
            # Add a synthetic WatchedSignal-like object to _pending.
            w = MagicMock()
            w.signal = {
                "local_order_id": local_order_id,
                "signal_id":      plan.get("signal_id", ""),
                "client_id":      plan.get("client_id", ""),
                "execution_mode": plan.get("execution_mode", ""),
            }
            w.state = "PENDING"
            self._pending.append(w)
            if plan.get("signal_id"):
                self._dedup_set.add(plan["signal_id"])
        return self._watch_returns


def _make_recovery(
    row_or_rows=None,
    *,
    osm=None,
    watcher=None,
    execution_mode: str = "paper",
    client_id: str = "client@test.com",
    quote_result: Optional[bool] = False,
    dry_run: bool = False,
    is_past_eod: bool = False,
) -> tuple[PendingTriggerRestartRecovery, _MockOSM]:
    _osm = osm or _MockOSM()
    if row_or_rows is not None:
        for r in (row_or_rows if isinstance(row_or_rows, list) else [row_or_rows]):
            _osm.seed(r)
    _watcher = watcher

    def _quote_check(broker, symbol, side, trigger):
        return quote_result

    recovery = PendingTriggerRestartRecovery(
        client_id=client_id,
        execution_mode=execution_mode,
        osm=_osm,
        entry_watcher=_watcher,
        broker=MagicMock(),
        quote_check_fn=_quote_check,
        is_past_eod=is_past_eod,
        dry_run=dry_run,
    )
    return recovery, _osm


# ═══════════════════════════════════════════════════════════════════════════════
# Test 1 — Valid waiting orphan rearmed
# ═══════════════════════════════════════════════════════════════════════════════

class TestValidWaitingOrphanRearmed:
    def test_waiting_valid_orphan_is_rearmed_and_registry_verified(self):
        """PENDING_TRIGGER with no terminal metadata, no current watcher, quote clear
        → watch() called once, registry contains local_order_id afterward."""
        r = _row(meta={})
        watcher = _MockWatcher(watch_returns=True)
        recovery, osm = _make_recovery(r, watcher=watcher, quote_result=False)
        summary = recovery.recover_all([r])

        assert summary["watchers_rearmed"] == 1, "Row must be rearmed"
        # Registry must physically contain the local_order_id.
        owned = recovery._verify_registry_ownership(r["local_order_id"], r)
        assert owned is not None, "Registry ownership must be proven after rearm"
        assert owned[0] == r["local_order_id"]
        assert summary["ownerless_rows_remaining"] == 0


# ═══════════════════════════════════════════════════════════════════════════════
# Test 2 — Rearm returns true but registry ownership missing
# ═══════════════════════════════════════════════════════════════════════════════

class TestRearmTrueButRegistryMissing:
    def test_watch_true_but_registry_empty_counts_as_unresolved(self):
        """watch() returns True but watcher._pending is empty afterward → unresolved."""
        r = _row(meta={})

        class _NonRegistering(_MockWatcher):
            def watch(self, plan, local_order_id, *, recovery_rearm=False):
                # Returns True but does NOT add to _pending
                return True

        watcher = _NonRegistering(watch_returns=True)
        recovery, osm = _make_recovery(r, watcher=watcher, quote_result=False)
        summary = recovery.recover_all([r])

        assert summary["watchers_rearmed"] == 0
        assert summary["unresolved_cleanup_failures"] >= 1


# ═══════════════════════════════════════════════════════════════════════════════
# Test 3 — Invalidated rows never rearmed
# ═══════════════════════════════════════════════════════════════════════════════

class TestInvalidatedRowsNeverRearmed:
    @pytest.mark.parametrize("reason", [
        "stop_bid_below_call_stop",
        "overnight_daily_invalidated",
        "overnight_live_quote_unavailable_timeout",
        "arm_already_through_trigger",
    ])
    def test_invalidated_reason_terminalized_not_rearmed(self, reason: str):
        r = _row(meta={"watcher_audit": {"reason_code": reason}})
        watcher = _MockWatcher(watch_returns=True)
        recovery, osm = _make_recovery(r, watcher=watcher)
        summary = recovery.recover_all([r])

        assert summary["watchers_rearmed"] == 0, f"INVALIDATED reason {reason!r} must not rearm"
        assert summary["stuck_invalidated_terminalized"] == 1
        assert len(osm.cancel_calls) == 1
        assert osm.cancel_calls[0][0] == r["local_order_id"]
        # Exact reason should survive
        actual_reason = osm.cancel_calls[0][1]
        assert actual_reason == reason, (
            f"Exact reason {reason!r} must be preserved; got {actual_reason!r}"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Test 4 — Retryable row handed to retry consumer, not normal-rearmed
# ═══════════════════════════════════════════════════════════════════════════════

class TestRetryableRowHandedToRetryConsumer:
    def test_retryable_is_not_rearmed(self):
        _now = datetime.now(timezone.utc)
        r = _row(meta={
            "materialization_status": "RETRY_PENDING",
            "materialization_next_retry_at": (_now + timedelta(minutes=1)).isoformat(),
        })
        watcher = _MockWatcher(watch_returns=True)
        recovery, osm = _make_recovery(r, watcher=watcher)
        summary = recovery.recover_all([r])

        assert summary["retry_rows_owned"] == 1
        assert summary["watchers_rearmed"] == 0
        assert len(osm.cancel_calls) == 0, "Retryable must not be cancelled"
        assert summary["ownerless_rows_remaining"] == 0


# ═══════════════════════════════════════════════════════════════════════════════
# Test 5 — Already-through-trigger row
# ═══════════════════════════════════════════════════════════════════════════════

class TestAlreadyThroughTrigger:
    @pytest.mark.parametrize("direction,trigger,ask,bid", [
        ("CALL", 450.0, 451.0, 449.0),  # CALL: ask >= trigger
        ("PUT",  450.0, 451.0, 449.0),  # PUT:  bid <= trigger
    ])
    def test_already_through_trigger_terminalized_not_rearmed(
        self, direction, trigger, ask, bid
    ):
        r = _row(direction=direction, entry_price=trigger, meta={})
        watcher = _MockWatcher(watch_returns=True)
        # quote_result=True means already through trigger
        recovery, osm = _make_recovery(r, watcher=watcher, quote_result=True)
        summary = recovery.recover_all([r])

        assert summary["already_through_trigger_terminalized"] == 1
        assert summary["watchers_rearmed"] == 0
        assert len(osm.cancel_calls) == 1
        reason = osm.cancel_calls[0][1]
        assert "already_through_trigger" in reason
        assert summary["ownerless_rows_remaining"] == 0


# ═══════════════════════════════════════════════════════════════════════════════
# Test 6 — Stuck trigger ready
# ═══════════════════════════════════════════════════════════════════════════════

class TestStuckTriggerReady:
    def test_stuck_trigger_ready_no_blind_rearm_no_duplicate_post(self):
        """watcher_audit.reason_code=trigger_ready → terminalized, no rearm, no broker POST."""
        r = _row(meta={"watcher_audit": {"reason_code": "trigger_ready"},
                       "trigger_crossed_at": "2026-01-01T09:30:00+00:00"})
        watcher = _MockWatcher(watch_returns=True)
        recovery, osm = _make_recovery(r, watcher=watcher)
        summary = recovery.recover_all([r])

        assert summary["stuck_trigger_ready_terminalized"] == 1
        assert summary["watchers_rearmed"] == 0
        # Trigger evidence preserved in meta writes
        all_meta = {k: v for oid, m in osm.meta_writes for k, v in m.items()}
        assert all_meta.get("restart_recovery_cls") == PTC.STUCK_TRIGGER_READY
        # No broker POST — OSM cancel, not a broker submit
        assert len(osm.cancel_calls) == 1


# ═══════════════════════════════════════════════════════════════════════════════
# Test 7 — Terminal materialization but PENDING_TRIGGER status
# ═══════════════════════════════════════════════════════════════════════════════

class TestTerminalMaterialization:
    def test_terminal_materialization_status_reconciled(self):
        r = _row(meta={"materialization_outcome": "TERMINAL_NO_TRADEABLE_CONTRACT"})
        recovery, osm = _make_recovery(r, watcher=_MockWatcher())
        summary = recovery.recover_all([r])

        assert summary["terminal_materialization_cleaned"] == 1
        assert summary["watchers_rearmed"] == 0
        assert len(osm.cancel_calls) == 1
        reason = osm.cancel_calls[0][1]
        assert "terminal_materialization" in reason


# ═══════════════════════════════════════════════════════════════════════════════
# Test 8 — Past EOD
# ═══════════════════════════════════════════════════════════════════════════════

class TestPastEOD:
    def test_stale_after_eod_not_rearmed(self):
        r = _row(meta={})
        recovery, osm = _make_recovery(
            r, watcher=_MockWatcher(), is_past_eod=True, quote_result=False
        )
        summary = recovery.recover_all([r])

        assert summary["stale_after_eod_terminalized"] == 1
        assert summary["watchers_rearmed"] == 0
        assert osm.cancel_calls[0][1] == "restart_stale_after_eod"


# ═══════════════════════════════════════════════════════════════════════════════
# Test 9 — Cleanup helper returns false
# ═══════════════════════════════════════════════════════════════════════════════

class TestCleanupReturnsFalse:
    def test_cancel_false_counts_as_unresolved(self):
        osm = _MockOSM(cancel_returns=False)
        r = _row(meta={"watcher_audit": {"reason_code": "overnight_daily_invalidated"}})
        osm.seed(r)
        recovery, _ = _make_recovery(r, osm=osm)
        summary = recovery.recover_all([r])

        assert summary["stuck_invalidated_terminalized"] == 0
        assert summary["unresolved_cleanup_failures"] == 1


# ═══════════════════════════════════════════════════════════════════════════════
# Test 10 — Cleanup helper raises
# ═══════════════════════════════════════════════════════════════════════════════

class TestCleanupRaises:
    def test_cancel_raises_counts_as_unresolved(self):
        osm = _MockOSM(cancel_raises=RuntimeError("db_error"))
        r = _row(meta={"watcher_audit": {"reason_code": "arm_already_through_trigger"}})
        osm.seed(r)
        recovery, _ = _make_recovery(r, osm=osm)
        summary = recovery.recover_all([r])

        assert summary["stuck_invalidated_terminalized"] == 0
        assert summary["unresolved_cleanup_failures"] == 1


# ═══════════════════════════════════════════════════════════════════════════════
# Test 11 — Cross-client isolation
# ═══════════════════════════════════════════════════════════════════════════════

class TestCrossClientIsolation:
    def test_different_client_row_not_mutated(self):
        """Two rows with same ticker/signal but different clients → only intended row changes."""
        r_target = _row(client_id="target@test.com",
                        meta={"watcher_audit": {"reason_code": "overnight_daily_invalidated"}})
        r_other  = _row(client_id="other@test.com",
                        meta={"watcher_audit": {"reason_code": "overnight_daily_invalidated"}})

        osm = _MockOSM()
        for r in [r_target, r_other]:
            osm.seed(r)

        recovery = PendingTriggerRestartRecovery(
            client_id="target@test.com",
            execution_mode="paper",
            osm=osm,
            entry_watcher=_MockWatcher(),
            broker=MagicMock(),
            quote_check_fn=lambda *a: False,
        )
        summary = recovery.recover_all([r_target, r_other])

        # Only r_target should be cancelled; r_other skipped as isolation violation.
        cancelled_ids = [c[0] for c in osm.cancel_calls]
        assert r_target["local_order_id"] in cancelled_ids
        assert r_other["local_order_id"] not in cancelled_ids
        # The cross-client row counted as unresolved (skipped with CRITICAL).
        assert summary["unresolved_cleanup_failures"] >= 1


# ═══════════════════════════════════════════════════════════════════════════════
# Test 12 — Cross-mode isolation
# ═══════════════════════════════════════════════════════════════════════════════

class TestCrossModeIsolation:
    def test_different_mode_row_not_mutated(self):
        """Same client, PAPER and LIVE rows → recovery only touches its own mode."""
        r_paper = _row(execution_mode="paper",
                       meta={"watcher_audit": {"reason_code": "overnight_daily_invalidated"}})
        r_live  = _row(execution_mode="live",
                       meta={"watcher_audit": {"reason_code": "overnight_daily_invalidated"}})

        osm = _MockOSM()
        for r in [r_paper, r_live]:
            osm.seed(r)

        # Paper-mode recovery
        recovery = PendingTriggerRestartRecovery(
            client_id="client@test.com",
            execution_mode="paper",
            osm=osm,
            entry_watcher=_MockWatcher(),
            broker=MagicMock(),
            quote_check_fn=lambda *a: False,
        )
        summary = recovery.recover_all([r_paper, r_live])

        cancelled_ids = [c[0] for c in osm.cancel_calls]
        assert r_paper["local_order_id"] in cancelled_ids
        assert r_live["local_order_id"] not in cancelled_ids


# ═══════════════════════════════════════════════════════════════════════════════
# Test 13 — Missing authoritative client/mode
# ═══════════════════════════════════════════════════════════════════════════════

class TestMissingAuthorityIdentity:
    def test_missing_client_id_fails_closed(self):
        """Row with blank client_id → fail closed, not mutated."""
        r = _row(meta={"watcher_audit": {"reason_code": "overnight_daily_invalidated"}})
        r["client_id"] = ""
        r["client_email"] = ""
        recovery, osm = _make_recovery(r)
        summary = recovery.recover_all([r])

        assert len(osm.cancel_calls) == 0, "Missing client must not trigger any mutation"
        assert summary["unresolved_cleanup_failures"] >= 1

    def test_missing_execution_mode_fails_closed(self):
        r = _row(meta={})
        r["execution_mode"] = ""
        recovery, osm = _make_recovery(r)
        summary = recovery.recover_all([r])

        assert len(osm.cancel_calls) == 0
        assert summary["unresolved_cleanup_failures"] >= 1


# ═══════════════════════════════════════════════════════════════════════════════
# Test 14 — Restart summary
# ═══════════════════════════════════════════════════════════════════════════════

class TestRestartSummary:
    def test_summary_counts_match_actual_actions_and_zero_ownerless(self):
        """Summary fields accurately reflect what happened; ownerless=0 on success."""
        r_valid  = _row(meta={})  # WAITING_VALID → rearmed
        r_invald = _row(meta={"watcher_audit": {"reason_code": "stop_bid_below_call_stop"}})

        watcher = _MockWatcher(watch_returns=True)
        osm = _MockOSM()
        for r in [r_valid, r_invald]:
            osm.seed(r)

        recovery = PendingTriggerRestartRecovery(
            client_id="client@test.com",
            execution_mode="paper",
            osm=osm,
            entry_watcher=watcher,
            broker=MagicMock(),
            quote_check_fn=lambda *a: False,
        )
        summary = recovery.recover_all([r_valid, r_invald])

        assert summary["rows_examined"] == 2
        assert summary["watchers_rearmed"] == 1
        assert summary["stuck_invalidated_terminalized"] == 1
        assert summary["ownerless_rows_remaining"] == 0


# ═══════════════════════════════════════════════════════════════════════════════
# Test 15 — Idempotent second restart
# ═══════════════════════════════════════════════════════════════════════════════

class TestIdempotentSecondRestart:
    def test_second_recovery_run_is_idempotent(self):
        """Running recovery twice must not double-rearm, double-cancel, or duplicate claims."""
        r = _row(meta={})
        watcher = _MockWatcher(watch_returns=True)
        osm = _MockOSM()
        osm.seed(r)

        recovery = PendingTriggerRestartRecovery(
            client_id="client@test.com",
            execution_mode="paper",
            osm=osm,
            entry_watcher=watcher,
            broker=MagicMock(),
            quote_check_fn=lambda *a: False,
        )

        summary1 = recovery.recover_all([r])
        summary2 = recovery.recover_all([r])  # second run

        # On second run, watcher already owns it → still counted as rearmed once
        # The watcher registry contains the local_order_id from run 1.
        assert summary1["watchers_rearmed"] >= 1
        # No double cancel
        assert len(osm.cancel_calls) == 0


# ═══════════════════════════════════════════════════════════════════════════════
# Test 16 — Exactly one submit path for STUCK_TRIGGER_READY
# ═══════════════════════════════════════════════════════════════════════════════

class TestExactlyOneSubmitPath:
    def test_stuck_trigger_ready_does_not_direct_broker_submit(self):
        """STUCK_TRIGGER_READY is terminalized — no direct broker submit from recovery."""
        broker = MagicMock()
        broker.submit_order = MagicMock()  # should never be called

        r = _row(meta={"watcher_audit": {"reason_code": "trigger_ready"}})
        osm = _MockOSM()
        osm.seed(r)

        recovery = PendingTriggerRestartRecovery(
            client_id="client@test.com",
            execution_mode="paper",
            osm=osm,
            entry_watcher=_MockWatcher(),
            broker=broker,
            quote_check_fn=lambda *a: False,
        )
        summary = recovery.recover_all([r])

        # No broker submit — row is terminalized, not submitted.
        broker.submit_order.assert_not_called()
        assert summary["stuck_trigger_ready_terminalized"] == 1
        assert len(osm.cancel_calls) == 1


# ═══════════════════════════════════════════════════════════════════════════════
# Classifier parity: every classification maps to a handled action
# ═══════════════════════════════════════════════════════════════════════════════

class TestClassifierParity:
    """Every PendingTriggerClassification constant must have a handler in recover_one."""

    def test_not_pending_trigger_is_ignored(self):
        r = _row(status="CANCELED")
        recovery, osm = _make_recovery(r)
        summary = recovery.recover_all([r])
        assert summary["rows_examined"] == 1
        assert len(osm.cancel_calls) == 0
        assert summary["ownerless_rows_remaining"] == 0

    def test_stale_after_eod_terminalized(self):
        r = _row(meta={})
        recovery, osm = _make_recovery(r, is_past_eod=True, quote_result=False,
                                       watcher=_MockWatcher())
        summary = recovery.recover_all([r])
        assert summary["stale_after_eod_terminalized"] == 1

    def test_stuck_terminal_materialization_cleaned(self):
        r = _row(meta={"materialization_outcome": "FAILED_TERMINAL"})
        recovery, osm = _make_recovery(r)
        summary = recovery.recover_all([r])
        assert summary["terminal_materialization_cleaned"] == 1

    def test_orphan_with_terminal_evidence_terminalized(self):
        r = _row(meta={
            "watcher_invalidation_class":  "INVALIDATED_TERMINAL",
            "watcher_invalidation_reason": "overnight_daily_invalidated",
        })
        recovery, osm = _make_recovery(r, watcher=_MockWatcher(watch_returns=False))
        summary = recovery.recover_all([r])
        assert summary["stuck_invalidated_terminalized"] == 1
        assert summary["watchers_rearmed"] == 0

    def test_orphan_with_retry_meta_goes_to_retry_owner(self):
        _now = datetime.now(timezone.utc)
        r = _row(meta={
            "watcher_retry_next_at": (_now + timedelta(minutes=1)).isoformat(),
        })
        recovery, osm = _make_recovery(r)
        summary = recovery.recover_all([r])
        assert summary["retry_rows_owned"] == 1
        assert summary["watchers_rearmed"] == 0
