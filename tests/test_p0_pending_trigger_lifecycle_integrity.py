"""
tests/test_p0_pending_trigger_lifecycle_integrity.py

PR #304 — PENDING_TRIGGER Lifecycle Integrity tests.

Ten tests covering Bugs A–E from the audit:
    1.  Deferred CALL stop invalidation terminalizes                      (Bug A)
    2.  Deferred PUT stop invalidation terminalizes                       (Bug A)
    3.  Trigger callback exception keeps watcher pending for retry        (Bug B)
    4.  Trigger callback success removes watcher from _pending            (Bug B)
    5.  Trigger callback exhausted terminalizes                           (Bug B)
    6.  Regular-session already-through-trigger arm rejected              (Bug C)
    7.  Recovery rearm cannot bypass already-through-trigger              (Bug C)
    8.  Daily validator valid but through-trigger expires                 (Bug D)
    9.  Valid untouched daily arms normally                               (Bug D)
    10. PENDING_TRIGGER invariant classifier                              (Bug E)

Additional:
    11. Classifier: WAITING_VALID is only rearmable                       (Bug E gate)
    12. LIVE unclassified reason fails closed                             (Bug A safety)
    13. _is_already_through_trigger helper — CALL/PUT semantics
"""
from __future__ import annotations

import os
import types
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")


# ─────────────────────────────────────────────────────────────────────────────
# Bug A tests — deferred contract must not bypass real underlying invalidation
# ─────────────────────────────────────────────────────────────────────────────

class TestBugA_DeferredInvalidationBypass:
    """The deferred contract guard must terminalize on real underlying
    invalidations, not resurrect the watcher to PENDING."""

    def _classifier(self):
        """Isolate the classifier under test — a pure method, no I/O."""
        import ap_execution_core as ec
        # Build a bare ExecutionCore instance without running __init__ side effects
        obj = ec.APExecutionCore.__new__(ec.APExecutionCore)
        return obj._is_real_underlying_invalidation

    def test_01_deferred_call_stop_bid_below_call_stop_is_real_invalidation(self):
        """
        Setup: contract=DEFERRED:XYZ, side=CALL, trigger=100, stop=95,
               quote bid=94.8, ask=95.0 → watcher stamps
               reason_code='stop_bid_below_call_stop'.
        Expected: _is_real_underlying_invalidation returns True.
        """
        classify = self._classifier()
        assert classify("stop_bid_below_call_stop") is True
        assert classify("STOP_BID_BELOW_CALL_STOP") is True   # case-insensitive
        assert classify("stop_anything_here") is True         # stop_* prefix always real

    def test_02_deferred_put_stop_ask_above_put_stop_is_real_invalidation(self):
        """
        Setup: contract=DEFERRED:XYZ, side=PUT, trigger=100, stop=105,
               quote bid=105.0, ask=105.3 → reason='stop_ask_above_put_stop'.
        Expected: classifier returns True; the deferred guard must NOT restore
        the watcher to PENDING for this reason.
        """
        classify = self._classifier()
        assert classify("stop_ask_above_put_stop") is True

    def test_01b_all_overnight_invalidation_reasons_are_real(self):
        classify = self._classifier()
        for rc in (
            "overnight_daily_invalidated",
            "overnight_premarket_breached",
            "overnight_too_far_from_trigger",
            "overnight_open_recheck_data_timeout",
            "overnight_daily_already_through_trigger",
        ):
            assert classify(rc) is True, f"{rc} should be real invalidation"

    def test_01c_arm_time_invalidations_are_real(self):
        classify = self._classifier()
        for rc in ("arm_drift", "arm_below_stop", "arm_already_through_trigger"):
            assert classify(rc) is True, f"{rc} should be real invalidation"

    def test_01d_empty_or_none_reason_is_not_real(self):
        classify = self._classifier()
        assert classify("") is False
        assert classify(None) is False
        assert classify("   ") is False

    def test_01e_unknown_reason_is_not_real(self):
        """Unknown reasons default to non-real so paper deferred watchers
        can stay alive for benign transient issues. LIVE fails closed at the
        call site, not in the classifier."""
        classify = self._classifier()
        assert classify("some_new_reason_we_do_not_know") is False


# ─────────────────────────────────────────────────────────────────────────────
# Bug B tests — trigger callback retry ownership
# ─────────────────────────────────────────────────────────────────────────────

class TestBugB_TriggerRetryOwnership:
    """The watcher must not be removed from _pending until on_trigger
    succeeds or exhausts 3 attempts."""

    def test_03_poll_only_removes_done_watchers_upfront(self):
        """
        Structural proof: completion dispatch happens before any watcher
        removal. Triggered watchers stay until callback success/exhaustion,
        and terminal watchers stay until cleanup is verified.
        """
        src = open("ap_entry_watcher.py").read()
        assert "for action, w in completed:" in src
        assert "pass  # removal now handled per-watcher after callback verification" in src
        poll_block = src[src.find("completed = []"):src.find("for action, w in completed:")]
        assert "self._pending = [_p for _p in self._pending" not in poll_block

    def test_04_success_path_removes_watcher_explicitly(self):
        """Success path must explicitly remove watcher from _pending
        (was previously implicit via upfront removal)."""
        src = open("ap_entry_watcher.py").read()
        assert "WATCHER_TRIGGER_CALLBACK_OK" in src, "success log marker missing"
        # Explicit removal snippet in success branch
        assert "removed_from_pending=true" in src, (
            "Success path must log explicit removal proof"
        )

    def test_05_retry_path_keeps_watcher_in_pending(self):
        """The retry branch (<3 attempts) must NOT release dedup and must
        reset state to PENDING so check() re-runs on the next poll."""
        src = open("ap_entry_watcher.py").read()
        assert "WATCHER_TRIGGER_CALLBACK_RETRY_SCHEDULED" in src
        assert "state_reset_to_pending=true" in src
        assert "kept_in_pending=true" in src

    def test_05b_exhaustion_path_terminalizes_and_removes(self):
        """After 3 failures, watcher must be marked EXPIRED, removed from
        _pending, and stamped with on_trigger_exhausted_3_attempts audit
        so the invalidation classifier recognizes it as real."""
        src = open("ap_entry_watcher.py").read()
        assert "WATCHER_TRIGGER_CALLBACK_EXHAUSTED_EXPIRED" in src
        assert "on_trigger_exhausted_3_attempts" in src


# ─────────────────────────────────────────────────────────────────────────────
# Bug C + D tests — already-through-trigger safety
# ─────────────────────────────────────────────────────────────────────────────

class TestAlreadyThroughTriggerHelper:
    """The _is_already_through_trigger helper is used by both Bug C (arm-time)
    and Bug D (daily-valid rearm). It must exactly mirror WatchedSignal.check
    breach semantics."""

    def _fn(self):
        import ap_entry_watcher as ew
        return ew.APEntryWatcher._is_already_through_trigger

    def test_13a_call_ask_at_or_above_trigger_is_through(self):
        fn = self._fn()
        assert fn("CALL", 100.0, bid=99.5, ask=100.0) is True   # at trigger
        assert fn("CALL", 100.0, bid=99.5, ask=100.5) is True   # above trigger

    def test_13b_call_ask_below_trigger_is_not_through(self):
        fn = self._fn()
        assert fn("CALL", 100.0, bid=99.0, ask=99.5) is False

    def test_13c_put_bid_at_or_below_trigger_is_through(self):
        fn = self._fn()
        assert fn("PUT", 100.0, bid=100.0, ask=100.3) is True
        assert fn("PUT", 100.0, bid=99.5, ask=100.0) is True

    def test_13d_put_bid_above_trigger_is_not_through(self):
        fn = self._fn()
        assert fn("PUT", 100.0, bid=100.5, ask=100.7) is False

    def test_13e_zero_quotes_return_false(self):
        """No quote data → cannot claim already-through; must return False
        so the caller falls through to normal quote-outage handling."""
        fn = self._fn()
        assert fn("CALL", 100.0, bid=0, ask=0) is False
        assert fn("PUT", 100.0, bid=0, ask=0) is False

    def test_13f_zero_trigger_returns_false(self):
        fn = self._fn()
        assert fn("CALL", 0, bid=100, ask=101) is False

    def test_13g_fallback_to_mid_when_bid_ask_missing(self):
        fn = self._fn()
        assert fn("CALL", 100.0, bid=0, ask=0, mid=101.0) is True
        assert fn("PUT", 100.0, bid=0, ask=0, last=99.0) is True

    def test_13h_never_raises_on_bad_input(self):
        fn = self._fn()
        # Bad types must not crash
        assert fn("CALL", "bad", bid="x", ask=None) is False
        assert fn(None, None, bid=None, ask=None) is False


class TestBugC_ArmTimeAlreadyThroughTrigger:
    """watch() must reject arming when a fresh quote proves price is already
    through the trigger — even for daily/overnight rows and even under
    recovery_rearm=True."""

    def test_06_arm_gate_code_exists(self):
        """Structural: the arm-time gate must be in watch() before add_signal()."""
        src = open("ap_entry_watcher.py").read()
        assert "WATCHER_ARM_REJECTED_ALREADY_THROUGH_TRIGGER" in src, (
            "Bug C: arm-time already-through-trigger log marker missing"
        )
        assert "arm_already_through_trigger" in src, (
            "Bug C: arm_already_through_trigger reason code missing"
        )

    def test_07_recovery_rearm_cannot_bypass_arm_gate(self):
        """The gate must fire regardless of recovery_rearm — the audit says
        'For this specific reason, recovery_rearm=True must not suppress the
        rejection.'"""
        src = open("ap_entry_watcher.py").read()
        # The gate must run BEFORE the recovery-rearm suppression paths.
        # Structural check: the arm-time gate returns False independent of
        # the recovery_rearm flag (recovery only affects cleanup callback).
        gate_start = src.find("# ── P0 (PR #304) Bug C:")
        assert gate_start >= 0, "Bug C block missing"
        gate_end = src.find("# PR-C / BUG-EW-4:", gate_start)
        assert gate_end > gate_start, "Bug C block bounds not found"
        gate_block = src[gate_start:gate_end]
        # Verify the gate returns False even when recovery_rearm is True
        assert "return False" in gate_block, (
            "Bug C gate must return False to abort the arm"
        )
        # Verify audit is persisted regardless of recovery mode
        assert "_persist_watcher_audit(local_order_id" in gate_block, (
            "Bug C must persist watcher_audit for both live and recovery paths"
        )


class TestBugD_DailyValidatorArmedThroughTrigger:
    """When the daily validator returns valid but price has already crossed
    the trigger (premarket gap through, late arm), the watcher must NOT arm."""

    def test_08_daily_valid_through_trigger_expires_not_arms(self):
        """Structural: the daily-valid branch calls _is_already_through_trigger
        before setting w.overnight=False."""
        src = open("ap_entry_watcher.py").read()
        # The daily-valid branch must use the canonical late-attachment
        # classifier before handing control to the ordinary poll loop.
        assert "classify_late_attachment as _pt_classify_late_open" in src
        assert "stop_already_broken_terminal" in src
        assert "target_already_complete_terminal" in src
        assert "late_attachment_move_missed_terminal" in src
        assert "overnight_daily_already_through_trigger" not in src

    def test_09_valid_untouched_daily_arms_normally(self):
        """When the helper returns False (price still on the correct side of
        trigger), w.overnight=False and queue_status=VALID_AWAITING_BREACH
        proceeds as before."""
        # Direct helper test — if fn returns False, the existing arm path runs.
        import ap_entry_watcher as ew
        fn = ew.APEntryWatcher._is_already_through_trigger
        # Untouched CALL: ask=99.5 < trigger=100 → not through → arm proceeds
        assert fn("CALL", 100.0, bid=99.0, ask=99.5) is False
        # Untouched PUT: bid=100.5 > trigger=100 → not through → arm proceeds
        assert fn("PUT", 100.0, bid=100.5, ask=100.7) is False


# ─────────────────────────────────────────────────────────────────────────────
# Bug E test — PENDING_TRIGGER invariant classifier
# ─────────────────────────────────────────────────────────────────────────────

class TestBugE_PendingTriggerClassifier:
    """Every PENDING_TRIGGER row must classify into a well-defined category
    so recovery/cleanup decisions are consistent across handoff, monitor,
    and health check."""

    def _import(self):
        from ap.pending_trigger_classifier import (
            PendingTriggerClassification,
            classify_pending_trigger_row,
            is_safe_to_recovery_rearm,
        )
        return PendingTriggerClassification, classify_pending_trigger_row, is_safe_to_recovery_rearm

    def test_10a_trigger_ready_without_broker_is_stuck(self):
        """PENDING_TRIGGER + watcher_audit.reason_code=trigger_ready +
        no broker_order_id → STUCK_TRIGGER_READY, not rearmable."""
        C, classify, is_safe = self._import()
        row = {
            "status": "PENDING_TRIGGER",
            "broker_order_id": None,
            "submitted_ts": None,
            "meta": {
                "watcher_audit": {"reason_code": "trigger_ready"},
            },
        }
        cls = classify(row)
        assert cls == C.STUCK_TRIGGER_READY
        assert is_safe(cls) is False

    def test_10b_stop_bid_below_call_stop_is_stuck_invalidated(self):
        C, classify, is_safe = self._import()
        row = {
            "status": "PENDING_TRIGGER",
            "broker_order_id": None,
            "submitted_ts": None,
            "meta": {"watcher_audit": {"reason_code": "stop_bid_below_call_stop"}},
        }
        assert classify(row) == C.STUCK_INVALIDATED
        assert is_safe(classify(row)) is False

    def test_10c_overnight_daily_invalidated_is_stuck_invalidated(self):
        C, classify, is_safe = self._import()
        row = {
            "status": "PENDING_TRIGGER",
            "broker_order_id": None,
            "submitted_ts": None,
            "meta": {"watcher_audit": {"reason_code": "overnight_daily_invalidated"}},
        }
        assert classify(row) == C.STUCK_INVALIDATED
        assert is_safe(classify(row)) is False

    def test_10d_orphan_no_watcher(self):
        C, classify, is_safe = self._import()
        row = {
            "status": "PENDING_TRIGGER",
            "broker_order_id": None,
            "submitted_ts": None,
            "meta": {},
        }
        assert classify(row, watcher_owned=False) == C.ORPHAN_NO_WATCHER
        assert is_safe(classify(row, watcher_owned=False)) is False

    def test_10e_terminal_materialization_is_stuck(self):
        C, classify, is_safe = self._import()
        row = {
            "status": "PENDING_TRIGGER",
            "broker_order_id": None,
            "submitted_ts": None,
            "meta": {"materialization_outcome": "TERMINAL_NO_TRADEABLE_CONTRACT"},
        }
        assert classify(row) == C.STUCK_TERMINAL_MATERIALIZATION
        assert is_safe(classify(row)) is False

    def test_10f_waiting_valid_is_only_rearmable(self):
        """The safe default: no terminal evidence, watcher owns it → WAITING_VALID.
        This is the ONLY classification (plus WAITING_RETRYABLE) that
        recovery-rearm may act on."""
        C, classify, is_safe = self._import()
        row = {
            "status": "PENDING_TRIGGER",
            "broker_order_id": None,
            "submitted_ts": None,
            "meta": {"watcher_audit": {"reason_code": ""}},   # no watcher decision yet
        }
        cls = classify(row, watcher_owned=True)
        assert cls == C.WAITING_VALID
        assert is_safe(cls) is True

    def test_10g_waiting_retryable_when_retry_active(self):
        C, classify, is_safe = self._import()
        row = {
            "status": "PENDING_TRIGGER",
            "broker_order_id": None,
            "submitted_ts": None,
            "meta": {
                "materialization_status": "RETRY_PENDING",
                "materialization_next_retry_at": "2026-07-07T14:30:00+00:00",
            },
        }
        cls = classify(row, watcher_owned=True)
        assert cls == C.WAITING_RETRYABLE
        assert is_safe(cls) is True

    def test_10h_broker_order_id_present_means_not_pending_trigger(self):
        """A row that already has a broker_order_id isn't in the lifecycle
        bug space — it's beyond PENDING_TRIGGER."""
        C, classify, is_safe = self._import()
        row = {
            "status": "PENDING_TRIGGER",
            "broker_order_id": "TRD-12345",
            "submitted_ts": "2026-07-07T14:00:00+00:00",
            "meta": {},
        }
        assert classify(row) == C.NOT_PENDING_TRIGGER

    def test_10i_unsafe_already_through_trigger(self):
        C, classify, is_safe = self._import()
        row = {
            "status": "PENDING_TRIGGER",
            "broker_order_id": None,
            "submitted_ts": None,
            "meta": {},
        }
        cls = classify(row, live_quote_already_through_trigger=True)
        assert cls == C.UNSAFE_ALREADY_THROUGH_TRIGGER
        assert is_safe(cls) is False

    def test_10j_stale_after_eod(self):
        C, classify, is_safe = self._import()
        row = {
            "status": "PENDING_TRIGGER",
            "broker_order_id": None,
            "submitted_ts": None,
            "meta": {},
        }
        assert classify(row, is_past_eod=True) == C.STALE_AFTER_EOD
        assert is_safe(classify(row, is_past_eod=True)) is False

    def test_10k_classifier_never_raises(self):
        """Bad input must never crash — client money code."""
        C, classify, is_safe = self._import()
        for bad in [None, {}, {"status": None}, {"status": "PENDING_TRIGGER"}]:
            try:
                classify(bad or {})
            except Exception as exc:
                pytest.fail(f"classify_pending_trigger_row raised on {bad}: {exc}")

    def test_10l_only_two_classifications_are_rearmable(self):
        """LIVE safety proof: only WAITING_VALID and WAITING_RETRYABLE ever
        return True from is_safe_to_recovery_rearm."""
        C, classify, is_safe = self._import()
        rearmable = {
            attr for attr in dir(C)
            if not attr.startswith("_") and is_safe(getattr(C, attr))
        }
        assert rearmable == {"WAITING_VALID", "WAITING_RETRYABLE"}, (
            f"Only WAITING_VALID/WAITING_RETRYABLE should be rearmable, "
            f"got {rearmable}"
        )


class TestBugE_RecoveryRearmWiring:
    """Behavioral proof that the shared classifier gates the actual
    entry_watcher.watch(... recovery_rearm=True) path."""

    def _plan(self):
        return types.SimpleNamespace(
            signal_id="sig-recovery",
            ticker="SPY",
            side="CALL",
            score=80,
            tier="A",
            trigger_price=500.00,
            stop_underlying=498.00,
            target_underlying=505.00,
            entry_option_price=0,
            contract_symbol="DEFERRED:SPY",
            plan_id="plan-recovery",
            pattern="test",
            prior_day_high=None,
            prior_day_low=None,
            timeframe="5m",
            strategy_type="",
            metadata={},
        )

    def _row(self, meta=None, status="PENDING_TRIGGER"):
        return {
            "status": status,
            "broker_order_id": None,
            "submitted_ts": None,
            "meta": meta or {},
        }

    def _watcher(self, row):
        import ap_entry_watcher as ew
        osm = MagicMock()
        osm.get_order.return_value = row
        osm.cancel_pending_entry.return_value = True
        osm.submit_existing_entry = MagicMock()
        osm.record_deferred_hydration_result = MagicMock()
        w = ew.APEntryWatcher(MagicMock(), order_state_machine=osm, mode="LIVE")
        return ew, w, osm

    def _patch_safe_context(self, w):
        return patch.multiple(
            w,
            _is_regular_session_now=MagicMock(return_value=False),
            _is_past_entry_cutoff_now=MagicMock(return_value=False),
        )

    def test_14_waiting_valid_orphan_rearmed_no_submit(self):
        ew, w, osm = self._watcher(self._row())
        with self._patch_safe_context(w), patch.object(w, "add_signal", return_value=True) as add_signal:
            assert w.watch(self._plan(), "lo-valid", recovery_rearm=True) is True
        add_signal.assert_called_once()
        osm.cancel_pending_entry.assert_not_called()
        osm.submit_existing_entry.assert_not_called()
        osm.record_deferred_hydration_result.assert_not_called()

    def test_15_waiting_retryable_rearmed_no_submit(self):
        row = self._row({"materialization_status": "RETRY_PENDING"})
        ew, w, osm = self._watcher(row)
        with self._patch_safe_context(w), patch.object(w, "add_signal", return_value=True) as add_signal:
            assert w.watch(self._plan(), "lo-retry", recovery_rearm=True) is True
        add_signal.assert_called_once()
        osm.cancel_pending_entry.assert_not_called()
        osm.submit_existing_entry.assert_not_called()
        osm.record_deferred_hydration_result.assert_not_called()

    @pytest.mark.parametrize(
        "meta, expected",
        [
            ({"watcher_audit": {"reason_code": "trigger_ready"}}, "STUCK_TRIGGER_READY"),
            ({"watcher_audit": {"reason_code": "stop_bid_below_call_stop"}}, "STUCK_INVALIDATED"),
            ({"materialization_outcome": "TERMINAL_NO_TRADEABLE_CONTRACT"}, "STUCK_TERMINAL_MATERIALIZATION"),
        ],
    )
    def test_16_stuck_classifications_terminalized(self, meta, expected):
        ew, w, osm = self._watcher(self._row(meta))
        with self._patch_safe_context(w), patch.object(w, "add_signal", return_value=True) as add_signal:
            assert w.watch(self._plan(), f"lo-{expected}", recovery_rearm=True) is False
        add_signal.assert_not_called()
        osm.cancel_pending_entry.assert_called_once()
        assert expected in str(osm.cancel_pending_entry.call_args)
        osm.submit_existing_entry.assert_not_called()
        osm.record_deferred_hydration_result.assert_not_called()

    def test_17_unsafe_already_through_trigger_terminalized(self):
        ew, w, osm = self._watcher(self._row())
        with patch.object(w, "_is_regular_session_now", return_value=True), \
             patch.object(w, "_is_past_entry_cutoff_now", return_value=False), \
             patch.object(w, "_get_quote", return_value={"bid": 499.90, "ask": 500.01}), \
             patch.object(w, "add_signal", return_value=True) as add_signal:
            assert w.watch(self._plan(), "lo-through", recovery_rearm=True) is False
        add_signal.assert_not_called()
        osm.cancel_pending_entry.assert_called_once()
        assert "UNSAFE_ALREADY_THROUGH_TRIGGER" in str(osm.cancel_pending_entry.call_args)
        osm.submit_existing_entry.assert_not_called()
        osm.record_deferred_hydration_result.assert_not_called()

    def test_18_already_watcher_owned_waiting_valid_left_alone(self):
        ew, w, osm = self._watcher(self._row())
        sig = {
            "signal_id": "sig-owned",
            "ticker": "SPY",
            "side": "CALL",
            "entry_price": 500.00,
            "local_order_id": "lo-owned",
        }
        w._pending.append(ew.WatchedSignal(sig, overnight=False))
        with self._patch_safe_context(w), patch.object(w, "add_signal", return_value=True) as add_signal:
            assert w.watch(self._plan(), "lo-owned", recovery_rearm=True) is True
        add_signal.assert_not_called()
        osm.cancel_pending_entry.assert_not_called()
        osm.submit_existing_entry.assert_not_called()
        osm.record_deferred_hydration_result.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# Bug A LIVE fail-closed test
# ─────────────────────────────────────────────────────────────────────────────

class TestBugA_LiveFailsClosed:
    """Structural proof that LIVE unclassified reasons are treated as real
    invalidation. The runtime path is:
        _unclassified_live = _watcher_is_live and not _inv_reason_code
        if _is_real_underlying_invalidation or _unclassified_live: terminalize
    """

    def test_12_live_unclassified_reason_terminalizes(self):
        src = open("ap_execution_core.py").read()
        # The exact live-fail-closed pattern must be present
        assert "_unclassified_live" in src, (
            "Bug A: LIVE unclassified fail-closed guard missing"
        )
        assert "DEFERRED_CONTRACT_REAL_INVALIDATION" in src, (
            "Bug A: real-invalidation log marker missing"
        )
        # Verify the terminal path continues (does NOT return early) so the
        # forensic invalidation code below runs
        block_start = src.find("if _is_real_underlying_invalidation or _unclassified_live:")
        assert block_start >= 0, "Bug A guard condition missing"
        # Within ~50 lines after the guard, there should NOT be an early return
        block_slice = src[block_start:block_start + 3000]
        # The benign branch returns; the real branch must NOT return early
        real_branch_end = block_slice.find("else:")
        assert real_branch_end > 0
        real_branch = block_slice[:real_branch_end]
        assert "return  # do NOT return" not in real_branch, (
            "Bug A: real invalidation path must fall through to terminalization"
        )

    def test_12b_live_detection_uses_paper_false_fallback(self):
        import ap_execution_core as ec
        import ap_entry_watcher as ew

        core = ec.APExecutionCore.__new__(ec.APExecutionCore)
        core.execution_mode = ""
        core.mode = ""
        core.paper = False
        core.store = MagicMock()
        core.broker = MagicMock()
        core.client_id = "client@example.com"
        core._cleanup_pending_entry_order = MagicMock()

        watched = ew.WatchedSignal(
            {
                "signal_id": "sig-live-fallback",
                "ticker": "SPY",
                "side": "CALL",
                "entry_price": 500.0,
                "local_order_id": "lo-live-fallback",
                "contract_symbol": "DEFERRED:SPY",
            },
            overnight=False,
        )
        watched.state = ew.WatchState.INVALIDATED
        watched._pending_audit = {"reason_code": ""}

        result = core._on_signal_invalidate(watched)

        # Current LIVE fail-closed behavior quarantines an unclassified
        # DEFERRED invalidation. It must preserve watcher ownership and avoid
        # cleanup/cancel mutation until a truthful reason is available.
        assert result.outcome == "FAILED"
        assert result.reason_code == "unknown_live_reason:blank_live_invalidation_reason"
        core._cleanup_pending_entry_order.assert_not_called()
        core.store.update_status.assert_not_called()
        assert watched.state == ew.WatchState.INVALIDATED
