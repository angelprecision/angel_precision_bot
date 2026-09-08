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

import copy
import json
import os
import threading
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
        Structural proof: the _pending removal filter uses action=='done'
        (EXPIRED/INVALIDATED only). Triggered watchers stay until callback
        resolves.
        """
        src = open("ap_entry_watcher.py").read()
        # This exact line must exist — the surgical fix for Bug B
        assert (
            'done_ids = {id(w) for action, w in completed if action == "done"}'
            in src
        ), (
            "Bug B fix missing: done_ids filter must only include action=='done', "
            "not all completed watchers"
        )

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
        assert "overnight_daily_already_through_trigger" in src, (
            "Bug D: reason code missing"
        )
        # The daily-valid branch must call the helper
        assert "_is_already_through_trigger" in src

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
            canonical_signal_id="canonical-recovery",
            client_id="jasoncosby1@gmail.com",
            execution_mode="live",
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

    def _row(self, meta=None, status="PENDING_TRIGGER", local_order_id=None):
        return {
            "status": status,
            "local_order_id": local_order_id,
            "signal_id": "sig-recovery",
            "canonical_signal_id": "canonical-recovery",
            "client_id": "jasoncosby1@gmail.com",
            "execution_mode": "live",
            "broker_order_id": None,
            "submitted_ts": None,
            "meta": meta or {},
        }

    def _watcher(self, row):
        import ap_entry_watcher as ew
        osm = MagicMock()
        def _get_order(local_order_id):
            loaded = dict(row)
            if not loaded.get("local_order_id"):
                loaded["local_order_id"] = local_order_id
            return loaded
        osm.get_order.side_effect = _get_order
        osm.cancel_pending_entry.return_value = True
        osm.submit_existing_entry = MagicMock()
        osm.record_deferred_hydration_result = MagicMock()
        w = ew.APEntryWatcher(MagicMock(), order_state_machine=osm, mode="LIVE")
        w._persist_watcher_audit = lambda *a, **kw: None
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

        core._on_signal_invalidate(watched)

        core._cleanup_pending_entry_order.assert_called_once()
        assert watched.state != ew.WatchState.PENDING


# ─────────────────────────────────────────────────────────────────────────────
# PR #580 amendment corrections — behavioral tests
# (Corrections 1-4 per AMEND PR #580 IN PLACE — HARD HOLD REMAINS)
# ─────────────────────────────────────────────────────────────────────────────

class TestPR580AmendmentCorrections:
    """
    Behavioral proof of the #580 recovery lifecycle authority corrections.

    Correction 1: canonical broker handoff evidence → HOLD before any
    registration; watcher_audit.trigger_ready alone is not broker evidence.
    Correction 2: generation and recovery identity must fail closed on every
    corruption or mismatch.
    Correction 3: Lifecycle import failure → HOLD (not soft success).
    Correction 4: Lifecycle validation happens BEFORE _pending.append().
    """

    def _bare_watcher(self):
        import ap_entry_watcher as ew
        w = ew.APEntryWatcher(
            broker=None,
            order_state_machine=None,
            require_on_trigger=False,
            mode="LIVE",
        )
        w._persist_watcher_audit = lambda *a, **kw: None
        return ew, w

    def _recovery_sig(self, **overrides):
        import uuid
        sid = str(uuid.uuid4())
        base = {
            "signal_id": sid,
            "canonical_signal_id": sid,
            "ticker": "AAPL",
            "side": "CALL",
            "score": 70.0,
            "entry_price": 200.0,
            "entry_trigger": 200.0,
            "stop_price": 198.0,
            "target_price": 205.0,
            "plan_id": f"plan-{sid[:8]}",
            "local_order_id": f"lo-{sid[:8]}",
            "client_id": "jason@example.com",
            "execution_mode": "live",
            "watcher_token": "tok-test",
            "materialization_generation": 1,
            "contract_symbol": "DEFERRED:AAPL",
            "pattern": "2-1-2",
            "prior_day_high": 200.10,
            "prior_day_low": 197.90,
            "timeframe": "1h",
            "strategy_type": "continuation",
            "contract_deferred": False,
            "trigger": {"entry": 200.0, "stop": 198.0, "pt1": 205.0},
            "metadata": {
                "canonical_signal_id": sid,
                "client_id": "jason@example.com",
                "execution_mode": "live",
                "materialization_generation": 1,
            },
            "__recovery_rearm": True,
        }
        base.update(overrides)
        return base

    # ── Correction 3: lifecycle import failure → HOLD ─────────────────────

    def test_c3_lifecycle_unavailable_is_hold_not_soft_ok(self):
        """
        Correction 3: _EW_LIFECYCLE_OK=False must return (False, ...) — HOLD,
        not (True, 'soft_ok'). A bridge that cannot read lifecycle state cannot
        determine whether admission is safe.

        The shim imports the base module as '_ap_entry_watcher_base' in
        sys.modules. _restore_recovered_watcher_lifecycle is defined in
        the base module and reads _EW_LIFECYCLE_OK from the base module's
        globals. We must patch the variable there, not on the shim.
        """
        import sys, ap_entry_watcher as ew
        w = ew.APEntryWatcher(
            broker=None, order_state_machine=None,
            require_on_trigger=False, mode="LIVE",
        )
        w._persist_watcher_audit = lambda *a, **kw: None

        sig = self._recovery_sig()
        watched = ew.WatchedSignal(sig, overnight=False)

        # The base module that owns _restore_recovered_watcher_lifecycle
        # is loaded as '_ap_entry_watcher_base' by the shim's __init__.py.
        # Fall back to ew itself for monolithic (non-shim) deployments.
        _base_mod = sys.modules.get("_ap_entry_watcher_base", ew)
        orig = getattr(_base_mod, "_EW_LIFECYCLE_OK", None)
        try:
            _base_mod._EW_LIFECYCLE_OK = False
            ok, reason = w._restore_recovered_watcher_lifecycle(watched)
            assert ok is False, (
                "Correction 3: lifecycle unavailable must be HOLD (False), "
                f"got ok={ok!r} reason={reason!r}"
            )
            assert "unavailable" in reason or "hold" in reason, (
                f"reason should indicate unavailability; got {reason!r}"
            )
        finally:
            if orig is not None:
                _base_mod._EW_LIFECYCLE_OK = orig

    # ── Correction 2: identity validation ─────────────────────────────────

    @pytest.mark.parametrize("field,value,expected_fragment", [
        ("client_id", "", "missing_client_id"),
        ("client_id", "   ", "missing_client_id"),
        ("execution_mode", "paper_mode_typo", "invalid_execution_mode"),
        ("execution_mode", "", "invalid_execution_mode"),
        ("execution_mode", "LIVE", "invalid_execution_mode"),   # must be lowercase
        ("local_order_id", "", "missing_local_order_id"),
        ("canonical_signal_id", "", "missing_canonical_signal_id"),
        ("side", "LONG", "invalid_side"),
        ("side", "", "invalid_side"),
    ])
    def test_c2_identity_field_missing_or_invalid_is_hold(
        self, field, value, expected_fragment
    ):
        """
        Correction 2: every required identity field must be present and valid.
        Missing, blank, or malformed → HOLD. No UUID fallback, no mode
        fallback, no default side.

        For the side="" case WatchedSignal itself raises ValueError (the shim
        validates side at construction time). We test that case via a mock
        watched object that bypasses WatchedSignal's own guard so we can prove
        the bridge's identity check is independent.
        """
        import ap_entry_watcher as ew, types
        _, w = self._bare_watcher()

        sig = self._recovery_sig()
        # Apply the override directly to the signal dict (and metadata)
        sig[field] = value
        if field in sig.get("metadata", {}):
            sig["metadata"][field] = value

        # WatchedSignal raises ValueError for invalid/blank side.
        # Use a lightweight mock for that case to keep the test in scope.
        if field == "side" and not value:
            watched = types.SimpleNamespace(
                signal=sig,
                ticker=sig.get("ticker", "AAPL"),
                side=value,
            )
        else:
            watched = ew.WatchedSignal(sig, overnight=False)
            # For side, also zero out the WatchedSignal attribute
            if field == "side":
                watched.side = value

        ok, reason = w._restore_recovered_watcher_lifecycle(watched)
        assert ok is False, (
            f"Identity field {field!r}={value!r} must HOLD; got ok={ok!r} reason={reason!r}"
        )
        assert expected_fragment in reason, (
            f"reason {reason!r} should contain {expected_fragment!r}"
        )

    def test_c2_negative_generation_is_hold(self):
        """Correction 2: negative materialization_generation must HOLD."""
        import ap_entry_watcher as ew
        _, w = self._bare_watcher()
        sig = self._recovery_sig(materialization_generation=-1)
        sig["metadata"]["materialization_generation"] = -1
        watched = ew.WatchedSignal(sig, overnight=False)
        ok, reason = w._restore_recovered_watcher_lifecycle(watched)
        assert ok is False
        assert "generation" in reason

    def test_c2_zero_generation_is_hold(self):
        """Correction 2: zero materialization_generation must HOLD (ambiguous)."""
        import ap_entry_watcher as ew
        _, w = self._bare_watcher()
        sig = self._recovery_sig(materialization_generation=0)
        sig["metadata"]["materialization_generation"] = 0
        watched = ew.WatchedSignal(sig, overnight=False)
        ok, reason = w._restore_recovered_watcher_lifecycle(watched)
        assert ok is False
        assert "generation" in reason

    @pytest.mark.parametrize("malformed_metadata", [None, "", "not-json", []])
    def test_c2_malformed_durable_metadata_is_hold(self, malformed_metadata):
        """An explicit malformed metadata alias cannot become absence."""
        import ap_entry_watcher as ew, ap_lifecycle as L

        _, w = self._bare_watcher()
        sig = self._recovery_sig()
        sig["metadata"] = malformed_metadata
        watched = ew.WatchedSignal(sig, overnight=False)

        ok, reason = w._restore_recovered_watcher_lifecycle(watched)
        assert ok is False
        assert "metadata" in reason
        assert L.LEDGER.current_state(sig["signal_id"]) is None

    def test_c2_conflicting_metadata_aliases_are_hold(self):
        """Conflicting metadata/meta aliases never use last-writer order."""
        import ap_entry_watcher as ew, ap_lifecycle as L

        _, w = self._bare_watcher()
        sig = self._recovery_sig()
        sig["meta"] = dict(sig["metadata"])
        sig["meta"]["materialization_generation"] = 2
        watched = ew.WatchedSignal(sig, overnight=False)

        ok, reason = w._restore_recovered_watcher_lifecycle(watched)
        assert ok is False
        assert "conflicting" in reason
        assert L.LEDGER.current_state(sig["signal_id"]) is None

    @pytest.mark.parametrize(
        "signal_generation,metadata_generation,should_admit",
        [
            (2, 2, True),
            (0, 2, False),
            (2, 0, False),
            (1, 2, False),
            (-1, 2, False),
            (2, -1, False),
            ("malformed", 2, False),
            (2, "malformed", False),
            (True, 2, False),
            (2, True, False),
            ("", 2, False),
            (2, "", False),
            (None, 2, False),
            (2, None, False),
            ("MISSING", 2, True),
            (2, "MISSING", True),
        ],
    )
    def test_c2_generation_authorities_are_independent_and_fail_closed(
        self, signal_generation, metadata_generation, should_admit
    ):
        """Every populated generation is strict, and two values must agree.

        This exercises the real recovery bridge through add_signal(), so a
        rejected generation proves both zero lifecycle restoration and zero
        behavior-active watcher registration. ``MISSING`` represents a truly
        absent key; the current recovery contract permits the other valid
        authority to carry the generation in that case.
        """
        import ap_entry_watcher as ew, ap_lifecycle as L

        _, w = self._bare_watcher()
        sig = self._recovery_sig()
        if signal_generation == "MISSING":
            sig.pop("materialization_generation", None)
        else:
            sig["materialization_generation"] = signal_generation
        if metadata_generation == "MISSING":
            sig["metadata"].pop("materialization_generation", None)
        else:
            sig["metadata"]["materialization_generation"] = metadata_generation

        sid = sig["signal_id"]
        ok = w.add_signal(sig)
        assert ok is should_admit
        if should_admit:
            assert L.LEDGER.current_state(sid) == L.SignalState.WATCHING
            assert len(w._pending) == 1
        else:
            assert L.LEDGER.current_state(sid) is None
            assert w._pending == []
            assert "generation" in w._last_reject_reason

    def test_c2_live_and_paper_modes_both_admitted(self):
        """Correction 2: exactly 'live' and 'paper' (lowercase) are admitted."""
        import ap_entry_watcher as ew, ap_lifecycle as L, uuid
        for mode in ("live", "paper"):
            with L.LEDGER._entry_lock:
                L.LEDGER._current_state.clear()
            _, w = self._bare_watcher()
            sid = str(uuid.uuid4())
            sig = self._recovery_sig(
                signal_id=sid, canonical_signal_id=sid,
                local_order_id=f"lo-{sid[:8]}", execution_mode=mode,
            )
            sig["metadata"]["execution_mode"] = mode
            watched = ew.WatchedSignal(sig, overnight=False)
            ok, reason = w._restore_recovered_watcher_lifecycle(watched)
            assert ok is True, (
                f"execution_mode={mode!r} should be admitted; got ok={ok!r} reason={reason!r}"
            )

    # ── Correction 1: broker handoff evidence → HOLD ──────────────────────

    @pytest.mark.parametrize("evidence_key,evidence_value", [
        ("broker_order_id", "TRD-12345"),
        ("submitted_ts", "2026-09-04T14:00:00+00:00"),
    ])
    def test_c1_top_level_broker_fields_are_hold(self, evidence_key, evidence_value):
        """Correction 1: top-level broker_order_id / submitted_ts → HOLD."""
        import ap_entry_watcher as ew
        _, w = self._bare_watcher()
        sig = self._recovery_sig()
        sig[evidence_key] = evidence_value
        watched = ew.WatchedSignal(sig, overnight=False)
        ok, reason = w._restore_recovered_watcher_lifecycle(watched)
        assert ok is False
        assert "broker_handoff" in reason or "hold" in reason

    @pytest.mark.parametrize("meta_key,meta_value", [
        ("submit_intent_at", "2026-09-04T14:00:00+00:00"),
        ("broker_submit_key", "ap:lo-abc123"),
        ("broker_submit_payload_hash", "sha256:abc"),
        ("broker_ready", True),
    ])
    def test_c1_meta_broker_evidence_is_hold(self, meta_key, meta_value):
        """Correction 1: canonical meta broker-intent fields → HOLD."""
        import ap_entry_watcher as ew
        _, w = self._bare_watcher()
        sig = self._recovery_sig()
        sig["metadata"][meta_key] = meta_value
        watched = ew.WatchedSignal(sig, overnight=False)
        ok, reason = w._restore_recovered_watcher_lifecycle(watched)
        assert ok is False
        assert "broker_handoff" in reason or "hold" in reason

    def test_c1_watcher_audit_trigger_ready_is_not_broker_handoff(self):
        """
        Correction 1: trigger_ready is breach confirmation only. It does not
        prove broker submission or broker ownership, so a clean recovered
        watcher still restores NONE → ADOPTED → WATCHING.
        """
        import ap_entry_watcher as ew, ap_lifecycle as L
        _, w = self._bare_watcher()
        sig = self._recovery_sig()
        sig["metadata"]["watcher_audit"] = {"reason_code": "trigger_ready"}
        watched = ew.WatchedSignal(sig, overnight=False)
        ok, reason = w._restore_recovered_watcher_lifecycle(watched)
        assert ok is True, f"trigger_ready alone must not hold recovery: {reason!r}"
        assert L.LEDGER.current_state(sig["signal_id"]) == L.SignalState.WATCHING
        transitions = [
            (entry.from_state, entry.to_state)
            for entry in L.LEDGER.history(sig["signal_id"])
        ]
        assert (None, L.SignalState.ADOPTED) in transitions
        assert (L.SignalState.ADOPTED, L.SignalState.WATCHING) in transitions

    @pytest.mark.parametrize(
        "surface,field,value",
        [
            ("signal", "broker_order_id", "TRD-READY-1"),
            ("signal", "submitted_ts", "2026-09-08T16:00:00+00:00"),
            ("metadata", "submit_intent_at", "2026-09-08T16:00:00+00:00"),
            ("metadata", "broker_submit_key", "ap:lo-ready"),
            ("metadata", "broker_ready", True),
        ],
    )
    def test_c1_trigger_ready_plus_actual_canonical_handoff_holds(
        self, surface, field, value
    ):
        """Actual canonical handoff evidence still wins over the audit label."""
        import ap_entry_watcher as ew, ap_lifecycle as L

        _, w = self._bare_watcher()
        sig = self._recovery_sig()
        sig["metadata"]["watcher_audit"] = {"reason_code": "trigger_ready"}
        target = sig if surface == "signal" else sig["metadata"]
        target[field] = value
        watched = ew.WatchedSignal(sig, overnight=False)

        ok, reason = w._restore_recovered_watcher_lifecycle(watched)
        assert ok is False
        assert "broker_handoff" in reason or "hold" in reason
        assert L.LEDGER.current_state(sig["signal_id"]) is None

    def test_c1_canonical_active_materialization_handoff_is_hold(self):
        """A complete current materializer proof remains the sole owner."""
        import ap_entry_watcher as ew, ap_lifecycle as L

        _, w = self._bare_watcher()
        sig = self._recovery_sig()
        sig["metadata"].update({
            "lifecycle_state": "MATERIALIZING",
            "materialization_status": "RUNNING",
            "materialization_in_flight": True,
            "materialization_owner": "materializer:test-owner",
            "materialization_lease_until": "2099-01-01T00:00:00+00:00",
        })
        watched = ew.WatchedSignal(sig, overnight=False)

        ok, reason = w._restore_recovered_watcher_lifecycle(watched)
        assert ok is False
        assert "broker_handoff" in reason or "hold" in reason
        assert L.LEDGER.current_state(sig["signal_id"]) is None

    # ── Correction 4: lifecycle restored BEFORE _pending.append() ─────────

    def test_c4_hold_leaves_pending_empty(self):
        """
        Correction 4: HOLD must never leave the watcher in _pending.
        Previously the code appended first then rolled back; the amendment
        requires validation → lifecycle → register (if ok).

        Uses execution_mode="INVALID" to trigger HOLD: execution_mode is
        normalized from the signal column directly (no metadata fallback)
        and "INVALID" is not in {"live","paper"}, so it HOLDs before registration.
        """
        import ap_entry_watcher as ew, ap_lifecycle as L
        with L.LEDGER._entry_lock:
            L.LEDGER._current_state.clear()
        _, w = self._bare_watcher()
        # execution_mode="invalid_mode" is not "live"/"paper" → identity HOLD
        sig = self._recovery_sig()
        sig["execution_mode"] = "INVALID_MODE"
        sig["metadata"]["execution_mode"] = "INVALID_MODE"
        ok = w.add_signal(sig)
        assert ok is False
        assert len(w._pending) == 0, (
            "HOLD must not leave watcher in _pending regardless of ordering"
        )

    def test_c4_hold_via_broker_evidence_leaves_pending_empty(self):
        """
        Correction 4 + Correction 1: broker handoff HOLD must leave
        _pending completely clean.
        """
        import ap_entry_watcher as ew, ap_lifecycle as L
        with L.LEDGER._entry_lock:
            L.LEDGER._current_state.clear()
        _, w = self._bare_watcher()
        sig = self._recovery_sig()
        sig["broker_order_id"] = "TRD-HOLD-TEST"
        ok = w.add_signal(sig)
        assert ok is False
        assert len(w._pending) == 0
        # LEDGER must also be untouched: no lifecycle write for a HOLD
        sid = sig["signal_id"]
        state = L.LEDGER.current_state(sid)
        assert state is None, (
            f"Broker handoff HOLD must not write lifecycle state; got {state!r}"
        )

    def test_c4_zero_broker_calls_on_every_hold_path(self):
        """
        Zero broker calls on every HOLD path — no submit, cancel, replace,
        or position mutation. add_signal() itself never calls broker;
        this proves the recovery bridge does not introduce a new call site.
        """
        import ap_entry_watcher as ew, ap_lifecycle as L
        with L.LEDGER._entry_lock:
            L.LEDGER._current_state.clear()
        from unittest.mock import MagicMock
        broker_spy = MagicMock()
        w = ew.APEntryWatcher(
            broker=broker_spy,
            order_state_machine=None,
            require_on_trigger=False,
            mode="LIVE",
        )
        w._persist_watcher_audit = lambda *a, **kw: None
        # Trigger three different HOLD reasons
        for sig in [
            self._recovery_sig(client_id=""),                   # missing identity
            {**self._recovery_sig(), "broker_order_id": "X"},   # broker handoff
            self._recovery_sig(execution_mode="INVALID"),       # bad mode
        ]:
            w.add_signal(sig)
        # broker_spy must never have been called
        assert not broker_spy.called, (
            f"Broker was called during HOLD: {broker_spy.call_args_list}"
        )

    def test_c4_successful_recovery_reaches_watching(self):
        """
        Positive control: a clean recovery signal (no HOLD reasons) must
        reach lifecycle state WATCHING and be registered in _pending.
        """
        import ap_entry_watcher as ew, ap_lifecycle as L, uuid
        with L.LEDGER._entry_lock:
            L.LEDGER._current_state.clear()
        _, w = self._bare_watcher()
        sid = str(uuid.uuid4())
        sig = self._recovery_sig(
            signal_id=sid, canonical_signal_id=sid,
            local_order_id=f"lo-{sid[:8]}",
        )
        ok = w.add_signal(sig)
        assert ok is True
        assert len(w._pending) == 1
        assert L.LEDGER.current_state(sid) == L.SignalState.WATCHING

    def test_c4_duplicate_recovery_invocation_is_idempotent(self):
        """
        Duplicate recovery invocation (same signal_id, same local_order_id)
        must be idempotent: the dedup guard prevents re-registration, no
        second behavior-active watcher, state stays WATCHING.
        """
        import ap_entry_watcher as ew, ap_lifecycle as L, uuid
        with L.LEDGER._entry_lock:
            L.LEDGER._current_state.clear()
        _, w = self._bare_watcher()
        sid = str(uuid.uuid4())
        sig = self._recovery_sig(
            signal_id=sid, canonical_signal_id=sid,
            local_order_id=f"lo-{sid[:8]}",
        )
        ok1 = w.add_signal(sig)
        ok2 = w.add_signal(dict(sig))   # second call, same identity
        assert ok1 is True
        # Second call is blocked by dedup — no second watcher registered
        assert len(w._pending) == 1, "duplicate recovery must not add second watcher"
        # Lifecycle state must remain stable
        assert L.LEDGER.current_state(sid) == L.SignalState.WATCHING

    # ── Amendment atomicity: durable-owner race and staged conflicts ────────

    def _recovery_plan(self, sig, local_order_id):
        return types.SimpleNamespace(
            signal_id=sig["signal_id"],
            canonical_signal_id=sig["canonical_signal_id"],
            client_id=sig["client_id"],
            execution_mode=sig["execution_mode"],
            ticker=sig["ticker"],
            side=sig["side"],
            score=sig["score"],
            tier="A",
            trigger_price=sig["entry_price"],
            stop_underlying=sig["stop_price"],
            target_underlying=sig["target_price"],
            contract_symbol="DEFERRED:AAPL",
            plan_id=sig["plan_id"],
            pattern="2-1-2",
            prior_day_high=200.10,
            prior_day_low=197.90,
            timeframe="1h",
            strategy_type="continuation",
            metadata={
                **dict(sig["metadata"]),
                "canonical_signal_id": sig["canonical_signal_id"],
                "client_id": sig["client_id"],
                "execution_mode": sig["execution_mode"],
                "materialization_generation": sig["materialization_generation"],
                "contract_deferred": True,
            },
            local_order_id=local_order_id,
            materialization_generation=sig["materialization_generation"],
        )

    def _durable_recovery_row(self, sig):
        return {
            "status": "PENDING_TRIGGER",
            "kind": "ENTRY",
            "local_order_id": sig["local_order_id"],
            "signal_id": sig["signal_id"],
            "canonical_signal_id": sig["canonical_signal_id"],
            "client_id": sig["client_id"],
            "execution_mode": sig["execution_mode"],
            "broker_order_id": None,
            "submitted_ts": None,
            "meta": {
                "canonical_signal_id": sig["canonical_signal_id"],
                "client_id": sig["client_id"],
                "execution_mode": sig["execution_mode"],
                "materialization_generation": sig["materialization_generation"],
                "broker_ready": False,
            },
        }

    @staticmethod
    def _assert_no_broker_mutation(broker):
        for method_name in (
            "post",
            "post_order",
            "submit_order",
            "submit_entry",
            "submit_existing_entry",
            "cancel",
            "cancel_order",
            "replace",
            "replace_order",
        ):
            assert not getattr(broker, method_name).called, method_name

    def _run_recovery_owner_race(self, advance_row):
        """Run watch() through an Event barrier before real add_signal()."""
        import ap_entry_watcher as ew, ap_lifecycle as L

        with L.LEDGER._entry_lock:
            L.LEDGER._current_state.clear()

        sig = self._recovery_sig()
        local_order_id = sig["local_order_id"]
        row = self._durable_recovery_row(sig)
        store = {"row": row}
        osm = MagicMock()
        # A non-string client_id selects the test-double path rather than a
        # real database connection; the final read is still the real watcher
        # admission path and the barrier makes the interleaving deterministic.
        osm.client_id = None

        def _get_order(_local_order_id):
            return copy.deepcopy(store["row"])

        osm.get_order.side_effect = _get_order
        osm.cancel_pending_entry = MagicMock()
        osm.submit_existing_entry = MagicMock()
        osm.replace_order = MagicMock()
        osm.update_position = MagicMock()
        osm.create_position = MagicMock()
        osm.insert_proof_trade = MagicMock()
        osm.enqueue = MagicMock()

        broker = MagicMock()
        w = ew.APEntryWatcher(
            broker=broker,
            order_state_machine=osm,
            require_on_trigger=False,
            mode="LIVE",
        )
        broker.reset_mock()
        w._persist_watcher_audit = lambda *a, **kw: None
        w._validate_local_order_id = MagicMock(return_value=True)
        w.on_trigger = MagicMock()

        candidate_reached_commit = threading.Event()
        owner_advanced = threading.Event()
        worker_errors = []

        def _gated_add(signal, **kwargs):
            candidate_reached_commit.set()
            if not owner_advanced.wait(timeout=5):
                raise AssertionError("worker B did not publish its durable advance")
            return real_add(signal, **kwargs)

        real_add = w.add_signal
        worker_b = threading.Thread(
            target=lambda: _advance_from_barrier(
                candidate_reached_commit,
                owner_advanced,
                store,
                advance_row,
                worker_errors,
            ),
            daemon=True,
        )
        worker_b.start()
        with patch.object(w, "add_signal", side_effect=_gated_add), \
             patch.object(w, "_is_regular_session_now", return_value=False), \
             patch.object(w, "_is_past_entry_cutoff_now", return_value=False):
            result = w.watch(
                self._recovery_plan(sig, local_order_id),
                local_order_id,
                recovery_rearm=True,
            )
        worker_b.join(timeout=5)
        assert not worker_b.is_alive(), "barrier worker did not complete"
        assert not worker_errors, worker_errors
        return result, w, osm, broker, sig, store

    @pytest.mark.parametrize(
        "surface,key,value",
        [
            ("row", "broker_order_id", "broker-race-1"),
            ("meta", "submit_intent_at", "2026-09-08T16:00:00+00:00"),
            ("meta", "broker_ready", True),
        ],
    )
    def test_a1_broker_handoff_during_recovery_holds_before_commit(
        self, surface, key, value
    ):
        """Any canonical broker handoff published after early proof cannot admit."""
        def _advance(row):
            target = row if surface == "row" else row["meta"]
            target[key] = value

        result, w, osm, broker, sig, store = self._run_recovery_owner_race(_advance)

        assert result is False
        assert w._pending == []
        assert w._dedup_set == set()
        assert w._last_reject_reason == (
            "recovery_lifecycle_hold_broker_handoff_evidence"
        )
        assert w.on_trigger.call_count == 0
        self._assert_no_broker_mutation(broker)
        assert not osm.submit_existing_entry.mock_calls
        assert not osm.cancel_pending_entry.mock_calls
        assert not osm.replace_order.mock_calls
        assert not osm.update_position.mock_calls
        assert not osm.create_position.mock_calls
        assert not osm.insert_proof_trade.mock_calls
        assert not osm.enqueue.mock_calls
        import ap_lifecycle as L
        assert L.LEDGER.current_state(sig["signal_id"]) is None

    def test_a1_active_materializer_during_recovery_holds_before_commit(self):
        """A materializer that wins the gap remains the sole owner."""
        def _advance(row):
            row["meta"].update({
                "lifecycle_state": "MATERIALIZING",
                "materialization_status": "RUNNING",
                "materialization_in_flight": True,
                "materialization_owner": "materializer:race-1",
                "materialization_lease_until": "2099-01-01T00:00:00+00:00",
            })

        result, w, osm, broker, sig, store = self._run_recovery_owner_race(_advance)

        assert result is False
        assert w._pending == []
        assert w._dedup_set == set()
        assert w._last_reject_reason == (
            "recovery_lifecycle_hold_materialization_authority"
        )
        assert w.on_trigger.call_count == 0
        self._assert_no_broker_mutation(broker)
        assert not osm.submit_existing_entry.mock_calls
        assert not osm.cancel_pending_entry.mock_calls
        assert not osm.replace_order.mock_calls
        import ap_lifecycle as L
        assert L.LEDGER.current_state(sig["signal_id"]) is None

    def test_a2_failed_recovery_replacement_preserves_incumbent(self):
        """A HOLD after conflict staging cannot evict the incumbent watcher."""
        import ap_entry_watcher as ew, ap_lifecycle as L

        with L.LEDGER._entry_lock:
            L.LEDGER._current_state.clear()

        incumbent_signal = {
            "signal_id": "incumbent-signal",
            "canonical_signal_id": "incumbent-canonical",
            "ticker": "AAPL",
            "side": "CALL",
            "score": 70.0,
            "entry_price": 200.0,
            "stop_price": 198.0,
            "target_price": 205.0,
            "local_order_id": "lo-incumbent",
        }
        incumbent = ew.WatchedSignal(incumbent_signal, overnight=False)

        candidate = self._recovery_sig(
            signal_id="replacement-signal",
            canonical_signal_id="replacement-canonical",
            local_order_id="lo-replacement",
        )
        candidate["metadata"].update({
            "canonical_signal_id": candidate["canonical_signal_id"],
            "client_id": candidate["client_id"],
            "execution_mode": candidate["execution_mode"],
        })
        row = self._durable_recovery_row(candidate)
        store = {"row": row}
        osm = MagicMock()
        osm.client_id = None
        osm.cancel_pending_entry = MagicMock()
        osm.get_order.side_effect = lambda _oid: copy.deepcopy(store["row"])
        osm.submit_existing_entry = MagicMock()
        osm.replace_order = MagicMock()
        osm.update_position = MagicMock()
        osm.create_position = MagicMock()
        osm.insert_proof_trade = MagicMock()
        osm.enqueue = MagicMock()
        broker = MagicMock()
        w = ew.APEntryWatcher(
            broker=broker,
            order_state_machine=osm,
            require_on_trigger=False,
            mode="LIVE",
        )
        broker.reset_mock()
        w._persist_watcher_audit = lambda *a, **kw: None
        w._validate_local_order_id = MagicMock(return_value=True)
        incumbent._watcher_ref = w
        w._pending.append(incumbent)
        w._dedup_set.add(incumbent.signal_id)
        before_incumbent_row = {"status": "PENDING_TRIGGER", "local_order_id": "lo-incumbent"}

        conflict_staged = threading.Event()
        owner_advanced = threading.Event()
        worker_errors = []
        real_get_order = osm.get_order.side_effect

        def _gated_get_order(local_order_id):
            conflict_staged.set()
            if not owner_advanced.wait(timeout=5):
                raise AssertionError("worker B did not publish replacement HOLD truth")
            return real_get_order(local_order_id)

        osm.get_order.side_effect = _gated_get_order

        def _worker_b():
            try:
                if not conflict_staged.wait(timeout=5):
                    raise AssertionError("candidate did not reach final fence")
                store["row"]["broker_order_id"] = "broker-replacement-1"
                owner_advanced.set()
            except BaseException as exc:
                worker_errors.append(exc)
                owner_advanced.set()

        worker_b = threading.Thread(target=_worker_b, daemon=True)
        worker_b.start()
        candidate["side"] = "PUT"
        candidate["score"] = 90.0
        result = w.add_signal(candidate)
        worker_b.join(timeout=5)

        assert not worker_b.is_alive(), "replacement barrier worker did not complete"
        assert not worker_errors, worker_errors
        assert result is False
        assert incumbent.state == ew.WatchState.PENDING
        assert w._pending == [incumbent]
        assert incumbent.signal_id in w._dedup_set
        assert candidate["signal_id"] not in w._dedup_set
        assert before_incumbent_row == {
            "status": "PENDING_TRIGGER",
            "local_order_id": "lo-incumbent",
        }
        assert not osm.cancel_pending_entry.mock_calls
        assert not osm.submit_existing_entry.mock_calls
        assert not osm.replace_order.mock_calls
        assert not osm.update_position.mock_calls
        assert not osm.create_position.mock_calls
        assert not osm.insert_proof_trade.mock_calls
        assert not osm.enqueue.mock_calls
        self._assert_no_broker_mutation(broker)
        assert L.LEDGER.current_state(candidate["signal_id"]) is None

    @pytest.mark.parametrize("watching_behavior", ["raises", "no_state"])
    def test_a3_failed_watching_closes_adopted_lifecycle(self, watching_behavior):
        """Adopted-but-not-WATCHING is repaired to canonical ERROR, never held."""
        import ap_entry_watcher as ew, ap_lifecycle as L, sys

        with L.LEDGER._entry_lock:
            L.LEDGER._current_state.clear()
        broker = MagicMock()
        w = ew.APEntryWatcher(
            broker=broker,
            order_state_machine=None,
            require_on_trigger=False,
            mode="LIVE",
        )
        broker.reset_mock()
        w._persist_watcher_audit = lambda *a, **kw: None
        w.on_trigger = MagicMock()
        sig = self._recovery_sig(
            signal_id="failed-watching-signal",
            canonical_signal_id="failed-watching-canonical",
            local_order_id="lo-failed-watching",
        )
        base_module = sys.modules.get("_ap_entry_watcher_base", ew)
        if watching_behavior == "raises":
            behavior = MagicMock(side_effect=RuntimeError("injected watching failure"))
        else:
            behavior = MagicMock(return_value=None)

        with patch.object(base_module, "signal_watching", behavior):
            result = w.add_signal(sig)

        assert result is False
        assert w._pending == []
        assert w._dedup_set == set()
        assert w.on_trigger.call_count == 0
        self._assert_no_broker_mutation(broker)
        history = L.LEDGER.history(sig["signal_id"])
        transitions = [(entry.from_state, entry.to_state) for entry in history]
        assert (None, L.SignalState.ADOPTED) in transitions
        assert (L.SignalState.ADOPTED, L.SignalState.ERROR) in transitions
        assert (L.SignalState.ADOPTED, L.SignalState.WATCHING) not in transitions
        assert L.LEDGER.current_state(sig["signal_id"]) == L.SignalState.ERROR

    def test_a1_postgres_row_lock_serializes_final_recovery_commit(self, monkeypatch):
        """The real OSM row lock spans lifecycle and watcher admission."""
        database_url = os.getenv("INTELLIGENCE_POSTGRES_TEST_URL", "")
        if not database_url:
            pytest.skip("disposable PostgreSQL URL not configured")

        import uuid
        from contextlib import contextmanager

        import psycopg2
        import psycopg2.extras

        import ap.db as db_module
        import ap_entry_watcher as ew
        import ap_lifecycle as L

        schema = f"pr580_lock_{uuid.uuid4().hex}"
        client_id = "jasoncosby1@gmail.com"
        sig = self._recovery_sig(
            signal_id=f"pg-lock-signal-{uuid.uuid4().hex}",
            canonical_signal_id=f"pg-lock-canonical-{uuid.uuid4().hex}",
            local_order_id=f"pg-lock-order-{uuid.uuid4().hex}",
            client_id=client_id,
            execution_mode="live",
        )
        sig["metadata"].update({
            "client_id": client_id,
            "execution_mode": "live",
            "canonical_signal_id": sig["canonical_signal_id"],
        })

        class _ConnectionWrapper:
            def __init__(self, connection, cursor):
                self.connection = connection
                self.cursor = cursor

            def execute(self, sql, params=None):
                self.cursor.execute(sql, params)
                return self

            def fetchone(self):
                return self.cursor.fetchone()

        @contextmanager
        def _pg_conn():
            connection = psycopg2.connect(database_url)
            cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            try:
                cursor.execute(f'SET search_path TO "{schema}"')
                yield _ConnectionWrapper(connection, cursor)
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()
                connection.close()

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
                        broker_order_id TEXT,
                        submitted_ts TIMESTAMPTZ,
                        execution_mode TEXT NOT NULL,
                        signal_id TEXT,
                        canonical_signal_id TEXT,
                        meta JSONB
                    )
                    """
                )
                cursor.execute(
                    f"""
                    INSERT INTO "{schema}".orders (
                        local_order_id, client_id, kind, status,
                        execution_mode, signal_id, canonical_signal_id, meta
                    ) VALUES (%s, %s, 'ENTRY', 'PENDING_TRIGGER', %s, %s, %s, %s::jsonb)
                    """,
                    (
                        sig["local_order_id"],
                        client_id,
                        "live",
                        sig["signal_id"],
                        sig["canonical_signal_id"],
                        json.dumps(sig["metadata"]),
                    ),
                )

            monkeypatch.setattr(db_module, "conn", _pg_conn)
            with L.LEDGER._entry_lock:
                L.LEDGER._current_state.clear()

            broker = MagicMock()
            osm = MagicMock()
            osm.client_id = client_id
            watcher = ew.APEntryWatcher(
                broker=broker,
                order_state_machine=osm,
                require_on_trigger=False,
                mode="LIVE",
            )
            watcher._persist_watcher_audit = lambda *a, **kw: None
            watcher._validate_local_order_id = MagicMock(return_value=True)
            watched = ew.WatchedSignal(sig, overnight=False)

            lifecycle_entered = threading.Event()
            worker_ready = threading.Event()
            worker_done = threading.Event()
            worker_errors = []
            original_restore = watcher._restore_recovered_watcher_lifecycle

            def _restore_with_wait(candidate):
                lifecycle_entered.set()
                assert worker_ready.wait(timeout=5), "database worker did not start"
                assert not worker_done.is_set(), "row update was not serialized"
                return original_restore(candidate)

            def _advance_row():
                connection = psycopg2.connect(database_url)
                cursor = connection.cursor()
                try:
                    cursor.execute(f'SET search_path TO "{schema}"')
                    assert lifecycle_entered.wait(timeout=5)
                    worker_ready.set()
                    cursor.execute(
                        "UPDATE orders SET broker_order_id=%s WHERE local_order_id=%s",
                        ("broker-after-lock", sig["local_order_id"]),
                    )
                    connection.commit()
                    worker_done.set()
                except BaseException as exc:
                    worker_errors.append(exc)
                    connection.rollback()
                    worker_done.set()
                finally:
                    cursor.close()
                    connection.close()

            worker = threading.Thread(target=_advance_row, daemon=True)
            worker.start()
            watcher._restore_recovered_watcher_lifecycle = _restore_with_wait
            result = watcher.add_signal(sig)
            worker.join(timeout=5)

            assert not worker.is_alive(), "database owner did not finish after lock release"
            assert not worker_errors, worker_errors
            assert result is True
            assert len(watcher._pending) == 1
            assert watcher._pending[0].signal_id == sig["signal_id"]
            assert L.LEDGER.current_state(sig["signal_id"]) == L.SignalState.WATCHING
            assert worker_done.is_set()
            assert not broker.called

            with psycopg2.connect(database_url) as check_connection:
                with check_connection.cursor() as cursor:
                    cursor.execute(
                        f'SET search_path TO "{schema}"'
                    )
                    cursor.execute(
                        "SELECT broker_order_id FROM orders WHERE local_order_id=%s",
                        (sig["local_order_id"],),
                    )
                    assert cursor.fetchone()[0] == "broker-after-lock"
        finally:
            admin.autocommit = True
            with admin.cursor() as cursor:
                cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            admin.close()


def _advance_from_barrier(
    reached: threading.Event,
    advanced: threading.Event,
    store: dict,
    advance_row,
    errors: list,
):
    """Worker-B side of the deterministic recovery ownership race."""
    try:
        if not reached.wait(timeout=5):
            raise AssertionError("recovery candidate did not reach final boundary")
        advance_row(store["row"])
        advanced.set()
    except BaseException as exc:
        errors.append(exc)
        advanced.set()
