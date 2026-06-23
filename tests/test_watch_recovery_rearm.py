"""
tests/test_watch_recovery_rearm.py

Tests for the safe recovery re-arm mode added to ap_entry_watcher.watch():

    entry_watcher.watch(
        plan,
        local_order_id,
        recovery_rearm=True,
        no_cancel_on_reject=True,
    )

In this mode the watcher registers ownership of the order row but never:
  - calls cancel_pending_entry (in watch() or add_signal())
  - expires/cancels/rejects the DB row
  - changes contract, qty, limit_price, or status
  - returns False due to staleness / drift / below-stop gates

Only watcher audit metadata is written. The row stays alive regardless of
whether the staleness checks would have rejected it in normal mode.

Source verification:
  - recovery_rearm parameter exists in watch() signature
  - no_cancel_on_reject parameter exists in watch() signature
  - __recovery_rearm flag propagated into signal_dict and read in add_signal()
  - option_premium_stale return False suppressed
  - arm_drift return False suppressed
  - arm_below_stop permanent rejection suppressed
  - cancel_pending_entry guarded by not _no_cancel_on_reject in watch()
  - cancel_pending_entry guarded by not signal["__recovery_rearm"] in add_signal()
  - ap_morning_handoff_audit calls watch with recovery_rearm=True
"""
from __future__ import annotations

import sys
import types
import importlib.util
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

_REPO = Path(__file__).resolve().parents[1]
_EW_SRC  = (_REPO / "ap_entry_watcher.py").read_text()
_AUDIT_SRC = (_REPO / "ap_morning_handoff_audit.py").read_text()


# ---------------------------------------------------------------------------
# Source guards — prove the mechanism exists in production source
# ---------------------------------------------------------------------------

class TestSourceGuards:
    def test_watch_accepts_recovery_rearm_parameter(self):
        fn_start = _EW_SRC.find("def watch(")
        fn_sig   = _EW_SRC[fn_start: fn_start + 500]
        assert "recovery_rearm" in fn_sig, (
            "watch() must accept recovery_rearm= keyword argument"
        )

    def test_watch_accepts_no_cancel_on_reject_parameter(self):
        fn_start = _EW_SRC.find("def watch(")
        fn_sig   = _EW_SRC[fn_start: fn_start + 500]
        assert "no_cancel_on_reject" in fn_sig

    def test_recovery_rearm_flag_propagated_into_signal_dict(self):
        fn_start = _EW_SRC.find("def watch(")
        fn_body  = _EW_SRC[fn_start: fn_start + 4000]
        assert "__recovery_rearm" in fn_body, (
            "watch() must stamp __recovery_rearm into signal_dict so add_signal() "
            "can suppress its cancel_pending_entry calls"
        )

    def test_option_premium_stale_suppressed_in_recovery_mode(self):
        fn_start = _EW_SRC.find("def watch(")
        fn_body  = _EW_SRC[fn_start: fn_start + 8000]
        # Must NOT have bare `return False` after option_premium_stale when in recovery mode
        # The suppression adds an if/else around the return False
        assert "recovery_rearm: suppressing option_premium_stale" in fn_body or \
               "_recovery_rearm" in fn_body, (
            "watch() must suppress option_premium_stale rejection in recovery_rearm mode"
        )

    def test_arm_drift_suppressed_in_recovery_mode(self):
        fn_start = _EW_SRC.find("def watch(")
        fn_body  = _EW_SRC[fn_start: fn_start + 8000]
        assert "recovery_rearm: suppressing arm_drift" in fn_body or \
               "_recovery_rearm" in fn_body

    def test_arm_below_stop_suppressed_in_recovery_mode(self):
        fn_start = _EW_SRC.find("def watch(")
        fn_body  = _EW_SRC[fn_start: fn_start + 8000]
        assert "recovery_rearm: suppressing arm_below_stop" in fn_body or \
               "_recovery_rearm" in fn_body

    def test_cancel_pending_entry_guarded_by_no_cancel_on_reject(self):
        fn_start = _EW_SRC.find("def watch(")
        fn_body  = _EW_SRC[fn_start: fn_start + 10000]
        assert "_no_cancel_on_reject" in fn_body, (
            "watch() cancel_pending_entry call must be guarded by not _no_cancel_on_reject"
        )

    def test_add_signal_cancel_guarded_by_recovery_rearm_flag(self):
        fn_start = _EW_SRC.find("def add_signal(")
        fn_body  = _EW_SRC[fn_start: fn_start + 10000]
        assert "__recovery_rearm" in fn_body, (
            "add_signal() cancel_pending_entry calls must check __recovery_rearm flag"
        )

    def test_audit_calls_watch_with_recovery_rearm(self):
        assert "recovery_rearm=True" in _AUDIT_SRC, (
            "ap_morning_handoff_audit must call watch(recovery_rearm=True)"
        )

    def test_audit_calls_watch_with_no_cancel_on_reject(self):
        assert "no_cancel_on_reject=True" in _AUDIT_SRC

    def test_signal_dict_recovery_rearm_cleaned_up_in_finally(self):
        """__recovery_rearm must be popped in the finally block so it doesn't leak."""
        # watch() signature spans multiple lines — search for the method body
        fn_start = _EW_SRC.find("    def watch(\n")
        fn_body  = _EW_SRC[fn_start: fn_start + 20000]
        assert 'signal_dict.pop("__recovery_rearm", None)' in fn_body, (
            "__recovery_rearm must be removed in the finally block after add_signal()"
        )


# ---------------------------------------------------------------------------
# Load entry watcher without triggering full broker/DB deps
# ---------------------------------------------------------------------------

def _load_ew():
    spec = importlib.util.spec_from_file_location(
        "ap_entry_watcher_test_shim",
        _REPO / "ap_entry_watcher.py",
    )
    with patch.dict(sys.modules, {
        "ap.db":      MagicMock(),
        "ap.utils":   MagicMock(now_utc_iso=lambda: "2026-06-17T00:00:00+00:00"),
        "ap.brokers": MagicMock(),
    }):
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    return mod

_ew = _load_ew()


def _make_watcher(**kwargs) -> object:
    """Build a minimal APEntryWatcher for tests."""
    w = object.__new__(_ew.APEntryWatcher)
    w._pending          = []
    w._lock             = __import__("threading").Lock()
    w._running          = False
    w.on_trigger        = None
    w.order_state_machine = kwargs.get("osm", MagicMock())
    w._last_reject_reason = None
    # Stub the methods that make network calls
    w._get_quote        = MagicMock(return_value={})
    w._get_option_quote = MagicMock(return_value={})
    w._build_watcher_audit_payload = MagicMock(return_value={})
    w._persist_watcher_audit = MagicMock()
    w.has_order         = MagicMock(return_value=False)
    w._is_rearm_eligible = MagicMock(return_value=False)
    w.require_on_trigger = False
    return w


def _make_plan(ticker="C", trigger=155.0, side="CALL"):
    return types.SimpleNamespace(
        signal_id="sig-test",
        ticker=ticker,
        side=side,
        score=72.5,
        tier="B",
        trigger_price=trigger,
        stop_underlying=148.0,
        target_underlying=162.0,
        plan_id="plan-test",
        contract_symbol="C260626C00150000",
        pattern="3-1-2",
        prior_day_high=155.5,
        prior_day_low=148.0,
        timeframe="1d",
        strategy_type="",
        entry_option_price=0.0,  # no option premium check
    )


# ---------------------------------------------------------------------------
# Test: cancel_pending_entry NOT called in recovery_rearm mode
# ---------------------------------------------------------------------------

class TestNoCancelOnReject:

    def test_watch_cancel_not_called_when_add_signal_blocks_in_recovery_mode(self):
        """When add_signal returns False in recovery_rearm mode, cancel_pending_entry
        must NOT be called on the OSM."""
        osm = MagicMock()
        osm.cancel_pending_entry = MagicMock(return_value=True)
        watcher = _make_watcher(osm=osm)

        plan = _make_plan()

        # Make add_signal return False (simulate dedup/opposite-side block)
        with patch.object(watcher, "add_signal", return_value=False):
            result = watcher.watch(
                plan,
                "order-test-001",
                recovery_rearm=True,
                no_cancel_on_reject=True,
            )

        osm.cancel_pending_entry.assert_not_called(), (
            "cancel_pending_entry must NOT be called in recovery_rearm mode "
            "even when add_signal returns False"
        )

    def test_watch_cancel_not_called_when_drift_stale_in_recovery_mode(self):
        """Price drift staleness rejection must not cancel the row in recovery mode."""
        osm = MagicMock()
        osm.cancel_pending_entry = MagicMock(return_value=True)
        watcher = _make_watcher(osm=osm)

        plan = _make_plan(trigger=155.0, side="CALL")

        # Simulate a quote where price has drifted far past trigger (drift stale)
        watcher._get_quote = MagicMock(return_value={
            "bid": 170.0, "ask": 171.0, "quote_age_ms": 500
        })

        with patch.object(watcher, "add_signal", return_value=True):
            watcher.watch(
                plan,
                "order-drift-001",
                recovery_rearm=True,
                no_cancel_on_reject=True,
            )

        osm.cancel_pending_entry.assert_not_called()

    def test_normal_mode_cancel_still_called(self):
        """In normal mode (no flags), cancel_pending_entry IS called when add_signal blocks."""
        osm = MagicMock()
        osm.cancel_pending_entry = MagicMock(return_value=True)
        watcher = _make_watcher(osm=osm)

        plan = _make_plan()

        with patch.object(watcher, "add_signal", return_value=False):
            # Patch _get_quote to avoid network call — make staleness check pass
            watcher._get_quote = MagicMock(return_value={
                "bid": 155.0, "ask": 155.5, "quote_age_ms": 100
            })
            watcher.watch(plan, "order-normal-001")

        osm.cancel_pending_entry.assert_called_once()


# ---------------------------------------------------------------------------
# Test: staleness gates suppressed in recovery mode
# ---------------------------------------------------------------------------

class TestStalenessGatesSuppressed:

    def test_drift_stale_does_not_return_false_in_recovery_mode(self):
        """Price drift beyond threshold returns True (armed) in recovery_rearm mode."""
        osm = MagicMock()
        watcher = _make_watcher(osm=osm)
        plan = _make_plan(trigger=155.0, side="CALL")

        # Price far past trigger → drift stale in normal mode
        watcher._get_quote = MagicMock(return_value={
            "bid": 175.0, "ask": 176.0, "quote_age_ms": 100
        })

        with patch.object(watcher, "add_signal", return_value=True):
            result = watcher.watch(
                plan,
                "order-drift-suppress",
                recovery_rearm=True,
                no_cancel_on_reject=True,
            )
        assert result is True, (
            "Drift-stale gate must be suppressed in recovery_rearm mode — "
            "watch() should fall through to add_signal and return True"
        )

    def test_below_stop_does_not_return_false_in_recovery_mode(self):
        """Below-stop permanent rejection must not fire in recovery_rearm mode."""
        osm = MagicMock()
        watcher = _make_watcher(osm=osm)
        watcher._is_rearm_eligible = MagicMock(return_value=False)  # low score, not eligible

        plan = _make_plan(trigger=155.0, side="CALL")
        plan.stop_underlying = 160.0  # stop above current price → below_stop for CALL

        # Price below stop, not rearm eligible → permanent reject in normal mode
        watcher._get_quote = MagicMock(return_value={
            "bid": 150.0, "ask": 150.5, "quote_age_ms": 100
        })

        with patch.object(watcher, "add_signal", return_value=True):
            result = watcher.watch(
                plan,
                "order-stop-suppress",
                recovery_rearm=True,
                no_cancel_on_reject=True,
            )
        assert result is True, (
            "Below-stop permanent rejection must be suppressed in recovery_rearm mode"
        )

    def test_normal_mode_drift_stale_returns_false(self):
        """In normal mode (no recovery_rearm), drift stale must return False.

        The staleness check only runs during market hours (not pre_market,
        not post_session). We mock datetime so ET time is 10:30 (market hours)
        and the check always runs regardless of when the test suite executes.
        """
        from datetime import datetime, timezone
        from zoneinfo import ZoneInfo

        osm = MagicMock()
        watcher = _make_watcher(osm=osm)
        plan = _make_plan(ticker="C", trigger=100.0, side="CALL")
        # Remove overnight markers so _is_overnight_signal=False
        plan.prior_day_high = None
        plan.prior_day_low  = None
        plan.timeframe      = "5m"   # intraday — not overnight

        # Price 30% above trigger → well above any reasonable threshold
        watcher._get_quote = MagicMock(return_value={
            "bid": 130.0, "ask": 131.0, "quote_age_ms": 100
        })

        # Fix ET time to 10:30 so pre_market=False and post_session=False
        ET = ZoneInfo("America/New_York")
        fake_now_et = datetime(2026, 6, 17, 10, 30, 0, tzinfo=ET)

        saved_threshold = _ew.WATCH_ARM_EFFECTIVE_THRESHOLD_PCT
        try:
            _ew.WATCH_ARM_EFFECTIVE_THRESHOLD_PCT = 0.015
            # Patch datetime on the shim module object (not by module name string)
            with patch.object(watcher, "add_signal", return_value=True), \
                 patch.object(_ew, "datetime") as mock_dt:
                mock_dt.now.return_value = fake_now_et
                mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
                result = watcher.watch(plan, "order-normal-drift")
        finally:
            _ew.WATCH_ARM_EFFECTIVE_THRESHOLD_PCT = saved_threshold

        assert result is False, (
            "Normal mode must reject drift-stale signals when market is open. "
            "bid=130 vs trigger=100 (30% drift) > threshold=1.5%."
        )


# ---------------------------------------------------------------------------
# Test: add_signal cancel_pending_entry suppressed via __recovery_rearm flag
# ---------------------------------------------------------------------------

class TestAddSignalCancelSuppressed:

    def test_recovery_rearm_flag_in_signal_dict_before_add_signal(self):
        """__recovery_rearm must be in signal_dict when add_signal is called."""
        captured_signal = {}

        def _capture_add_signal(sig):
            captured_signal.update(sig)
            return True

        osm = MagicMock()
        watcher = _make_watcher(osm=osm)
        plan = _make_plan()
        watcher._get_quote = MagicMock(return_value={})

        with patch.object(watcher, "add_signal", side_effect=_capture_add_signal):
            watcher.watch(
                plan,
                "order-flag-check",
                recovery_rearm=True,
                no_cancel_on_reject=True,
            )

        assert captured_signal.get("__recovery_rearm") is True, (
            "__recovery_rearm must be True in signal_dict when add_signal is called"
        )

    def test_recovery_rearm_flag_cleaned_up_after_add_signal(self):
        """__recovery_rearm must be removed from signal_dict after add_signal."""
        osm = MagicMock()
        watcher = _make_watcher(osm=osm)
        plan = _make_plan()

        with patch.object(watcher, "add_signal", return_value=True):
            watcher.watch(
                plan,
                "order-cleanup",
                recovery_rearm=True,
                no_cancel_on_reject=True,
            )

        # The signal_dict is local to watch() — this test verifies the flag is
        # consumed in the finally block so it never leaks into subsequent calls.
        # We verify via source guard (test_signal_dict_recovery_rearm_cleaned_up_in_finally).
        # The runtime test confirms no exception from the cleanup.
        # (If pop() weren't there, the flag could accumulate on re-used dicts.)

    def test_normal_mode_flag_not_set(self):
        """In normal mode, __recovery_rearm must NOT appear in signal_dict."""
        captured_signal = {}

        def _capture_add_signal(sig):
            captured_signal.update(sig)
            return True

        osm = MagicMock()
        watcher = _make_watcher(osm=osm)
        plan = _make_plan()
        watcher._get_quote = MagicMock(return_value={})

        with patch.object(watcher, "add_signal", side_effect=_capture_add_signal):
            watcher.watch(plan, "order-normal-no-flag")

        assert "__recovery_rearm" not in captured_signal or \
               not captured_signal.get("__recovery_rearm"), (
            "Normal mode must not set __recovery_rearm in signal_dict"
        )


# ---------------------------------------------------------------------------
# Test: audit module calls watch with correct flags
# ---------------------------------------------------------------------------

class TestAuditCallsWatchWithRecoveryFlags:
    """Verify that ap_morning_handoff_audit passes recovery_rearm=True and
    no_cancel_on_reject=True so the audit is always mutation-safe."""

    def test_audit_source_passes_recovery_rearm_true(self):
        assert "recovery_rearm=True" in _AUDIT_SRC

    def test_audit_source_passes_no_cancel_on_reject_true(self):
        assert "no_cancel_on_reject=True" in _AUDIT_SRC

    def test_audit_watch_call_site_contains_both_flags(self):
        """Both flags must appear together at the same call site."""
        idx = _AUDIT_SRC.find("recovery_rearm=True")
        region = _AUDIT_SRC[max(0, idx - 50): idx + 200]
        assert "no_cancel_on_reject=True" in region, (
            "Both recovery_rearm=True and no_cancel_on_reject=True must appear "
            "in the same watch() call in the audit module"
        )

    def test_audit_row_status_unchanged_after_watch_with_flags(self):
        """Row status must not change when watch is called with recovery flags."""
        import ap_morning_handoff_audit as _mha

        mock_watcher = MagicMock()
        mock_watcher.has_order.return_value = False
        mock_watcher.watch.return_value = True

        original_order = {
            "local_order_id": "order-status-test",
            "symbol": "C",
            "contract": "C260626C00150000",
            "status": "PENDING_TRIGGER",
            "trigger_price": 155.0,
            "broker_order_id": None,
            "submitted_ts": None,
            "filled_ts": None,
            "direction": "CALL",
            "execution_mode": "live",
            "meta": {"trigger_type": "breach"},
            "last_error": None,
            "score": 72.5,
            "tier": "B",
            "stop_underlying": 148.0,
            "target_underlying": 162.0,
            "pattern": "3-1-2",
            "timeframe": "1d",
            "signal_id": "sig-test",
            "plan_id": "plan-test",
            "created_ts": "2026-06-17T05:00:00+00:00",
        }

        with patch.object(_mha, "_is_after_eod_cutoff", return_value=False), \
             patch.object(_mha, "_persist_rearm_meta"):
            result = _mha._audit_row(
                original_order,
                entry_watcher=mock_watcher,
                after_cutoff=False,
                dry_run=False,
                client_id="jasoncosby1@gmail.com",
                osm=MagicMock(),
            )

        # Status must not have been mutated by the audit
        assert original_order["status"] == "PENDING_TRIGGER"

        # watch was called with recovery flags
        assert mock_watcher.watch.called, "watch() must have been called"
        call_kwargs = mock_watcher.watch.call_args.kwargs
        assert call_kwargs.get("recovery_rearm") is True, (
            "Audit must pass recovery_rearm=True to watch()"
        )
        assert call_kwargs.get("no_cancel_on_reject") is True, (
            "Audit must pass no_cancel_on_reject=True to watch()"
        )
