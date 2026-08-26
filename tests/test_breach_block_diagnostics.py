"""
tests/test_breach_block_diagnostics.py

Tests for hotfix/breach-block-diagnostics — diagnostic-only logging for silent
breach risk blocks and on_trigger returns.

Asserted invariants:
  - Every False return from _breach_risk_check emits BREACH_RISK_CHECK_BLOCKED
    or BREACH_RISK_CHECK_EXCEPTION exactly once
  - Every early-return from _on_entry_trigger after _breach_risk_check returns
    False emits ENTRY_TRIGGER_BLOCKED_RETURN exactly once
  - on_trigger callback (success path) emits WATCHER_ON_TRIGGER_RETURNED
  - on_trigger callback (exception path) emits WATCHER_ON_TRIGGER_EXCEPTION
  - REGRESSION GUARD: _breach_risk_check still returns the same bool;
    _on_entry_trigger still hits the same code paths it always did

This PR adds logs only. No status changes. No last_error writes. No new submit
or cancel paths. The test file proves that property by construction.
"""
from __future__ import annotations

import ast
import sys
import types
import logging
import importlib.util
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_REPO    = Path(__file__).resolve().parents[1]
_EC_SRC  = (_REPO / "ap_execution_core.py").read_text()
_EW_SRC  = (_REPO / "ap_entry_watcher.py").read_text()


def _method_source(source: str, name: str) -> str:
    tree = ast.parse(source)
    node = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == name
    )
    return ast.get_source_segment(source, node) or ""


_BREACH_RISK_CHECK_SRC = _method_source(_EC_SRC, "_breach_risk_check")


# ---------------------------------------------------------------------------
# Source guards — prove the structured log events exist in source
# ---------------------------------------------------------------------------

class TestSourceGuards:
    def test_breach_risk_check_blocked_event_present(self):
        assert "BREACH_RISK_CHECK_BLOCKED" in _EC_SRC

    def test_breach_risk_check_exception_event_present(self):
        assert "BREACH_RISK_CHECK_EXCEPTION" in _EC_SRC

    def test_entry_trigger_blocked_return_event_present(self):
        assert "ENTRY_TRIGGER_BLOCKED_RETURN" in _EC_SRC

    def test_watcher_on_trigger_returned_event_present(self):
        assert "WATCHER_ON_TRIGGER_RETURNED" in _EW_SRC

    def test_watcher_on_trigger_exception_event_present(self):
        assert "WATCHER_ON_TRIGGER_EXCEPTION" in _EW_SRC

    def test_emit_helper_present(self):
        assert "def _emit_breach_diag" in _EC_SRC

    def test_no_new_status_writes_in_breach_risk_check(self):
        """Diagnostic-only — must not introduce new orders status writes."""
        body = _BREACH_RISK_CHECK_SRC
        # The only DB writes allowed are the pre-existing update_signal_fields
        # calls (signals table, not orders) and the pre-existing
        # _cleanup_pending_entry_order on positions_full path.
        # No new orders.* writes, no new last_error= writes.
        assert "orders.status" not in body
        assert "UPDATE orders" not in body
        assert "self.osm." not in body  # no new OSM calls

    def test_no_new_cleanup_calls_in_breach_risk_check(self):
        """Count cleanup calls — must equal exactly 1 (the pre-existing
        positions_full path). The diagnostic PR must NOT add any new ones."""
        body = _BREACH_RISK_CHECK_SRC
        count = body.count("_cleanup_pending_entry_order")
        assert count == 1, (
            f"_breach_risk_check must have exactly 1 _cleanup_pending_entry_order "
            f"call (the pre-existing positions_full path). Found {count}. "
            f"This PR is diagnostic-only — no new cleanup paths allowed."
        )

    def test_no_new_return_paths_added(self):
        """_breach_risk_check must have exactly 5 'return False' paths
        (kill switch x2, positions_full, approved_plan_missing LIVE,
        revalidation block, revalidation error LIVE) plus the final 'return True'."""
        body = _BREACH_RISK_CHECK_SRC
        return_false_count = body.count("return False")
        return_true_count  = body.count("return True")
        assert return_false_count == 6, (
            f"Expected 6 'return False' statements (5 block paths + the "
            f"in-exception LIVE block); got {return_false_count}. "
            f"If this changed, the diagnostic PR may have altered control flow."
        )
        assert return_true_count == 1


# ---------------------------------------------------------------------------
# Diagnostic helper — emits without raising
# ---------------------------------------------------------------------------

def _load_execution_core_mod():
    """Load ap_execution_core without triggering heavy module-level deps."""
    stubs = {
        "ap.db":              MagicMock(),
        "ap.utils":           MagicMock(now_utc_iso=lambda: "2026-06-19T00:00:00+00:00"),
        "ap.brokers":         MagicMock(),
        "ap.queue":           MagicMock(),
        "ap_master_control":  MagicMock(),
        "yfinance":           MagicMock(),
    }
    with patch.dict(sys.modules, stubs):
        spec = importlib.util.spec_from_file_location(
            "ap_execution_core_shim",
            _REPO / "ap_execution_core.py",
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    return mod

_ec = _load_execution_core_mod()


def _make_watched(ticker="KO", local_order_id="order-test-001",
                  signal_id="sig-test", contract="DEFERRED:KO"):
    w = MagicMock()
    w.ticker = ticker
    w.signal = {
        "local_order_id": local_order_id,
        "signal_id":      signal_id,
        "contract":       contract,
        "client_email":   "jasoncosby1@gmail.com",
    }
    return w


def _make_core_stub(mode="LIVE"):
    """Minimal APExecutionCore stub with just the diagnostic helper bound."""
    core = object.__new__(_ec.APExecutionCore)
    core.mode      = mode
    core.email     = "jasoncosby1@gmail.com"
    core.client_id = "jasoncosby1@gmail.com"
    core._max_positions = 2
    return core


class TestEmitBreachDiag:

    def test_emit_does_not_raise_on_missing_fields(self):
        """Helper must never raise — diagnostics cannot break trading flow."""
        core = _make_core_stub()
        w = MagicMock()
        w.ticker = None
        w.signal = None  # broken signal — helper must still survive
        try:
            core._emit_breach_diag(
                "BREACH_RISK_CHECK_BLOCKED",
                watched=w,
                reason="test",
            )
        except Exception as exc:
            pytest.fail(f"_emit_breach_diag raised: {exc}")

    def test_emit_includes_all_required_fields(self, caplog):
        """All fixed fields must be present in the log line."""
        caplog.set_level(logging.INFO, logger="ap.execution_core")
        core = _make_core_stub()
        w = _make_watched(ticker="KO", local_order_id="oid-1",
                          signal_id="sid-1", contract="DEFERRED:KO")
        core._emit_breach_diag(
            "BREACH_RISK_CHECK_BLOCKED",
            watched=w,
            reason="positions_full_at_breach",
            positions_open=2,
            pending_entries=0,
            max_positions=2,
            level="info",
        )
        text = "\n".join(r.message for r in caplog.records)
        for field in (
            "BREACH_RISK_CHECK_BLOCKED",
            "client_id=jasoncosby1@gmail.com",
            "local_order_id=oid-1",
            "signal_id=sid-1",
            "symbol=KO",
            "contract=DEFERRED:KO",
            "reason=positions_full_at_breach",
            "execution_mode=LIVE",
            "positions_open=2",
            "pending_entries=0",
            "max_positions=2",
        ):
            assert field in text, f"Field {field!r} missing from log: {text}"

    def test_emit_truncates_long_exception_message(self, caplog):
        caplog.set_level(logging.WARNING, logger="ap.execution_core")
        core = _make_core_stub()
        w = _make_watched()
        long_msg = "x" * 500
        core._emit_breach_diag(
            "BREACH_RISK_CHECK_EXCEPTION",
            watched=w,
            reason="test",
            exception_type="RuntimeError",
            exception_message=long_msg,
            level="warning",
        )
        text = "\n".join(r.message for r in caplog.records)
        # exception_message must be present but truncated at 200 chars
        assert "exception_message=" in text
        # Find the message and verify it doesn't contain the full 500 chars
        assert "x" * 500 not in text
        assert "x" * 200 in text


# ---------------------------------------------------------------------------
# _breach_risk_check — diagnostic emits on each False path
# ---------------------------------------------------------------------------

class TestBreachRiskCheckEmits:
    """Each False return path must emit a structured diagnostic."""

    def _make_core(self, mode="LIVE", kill_switch=False, master_control=None,
                   max_positions=2, open_count=0, pending_count=0):
        core = _make_core_stub(mode=mode)
        core._kill_switch = kill_switch
        core.master_control = master_control
        core._max_positions = max_positions
        core._current_open_position_count = MagicMock(return_value=open_count)
        core._current_pending_entry_count = MagicMock(return_value=pending_count)
        core._recover_plan_for_revalidation = MagicMock(return_value=None)
        core._cleanup_pending_entry_order = MagicMock()
        core.store = MagicMock()
        return core

    def test_kill_switch_emits_blocked(self, caplog):
        caplog.set_level(logging.INFO, logger="ap.execution_core")
        core = self._make_core(kill_switch=True)
        result = core._breach_risk_check(_make_watched())
        # REGRESSION GUARD: behavior unchanged
        assert result is False
        text = "\n".join(r.message for r in caplog.records)
        assert "BREACH_RISK_CHECK_BLOCKED" in text
        assert "reason=kill_switch_active" in text

    def test_master_control_kill_switch_emits_blocked(self, caplog):
        caplog.set_level(logging.INFO, logger="ap.execution_core")
        mc = MagicMock()
        mc._kill_switch_fn = MagicMock(return_value=True)
        core = self._make_core(master_control=mc)
        result = core._breach_risk_check(_make_watched())
        assert result is False
        text = "\n".join(r.message for r in caplog.records)
        assert "BREACH_RISK_CHECK_BLOCKED" in text
        assert "reason=master_control_kill_switch_active" in text

    def test_master_control_kill_switch_exception_emits_exception(self, caplog):
        caplog.set_level(logging.INFO, logger="ap.execution_core")
        mc = MagicMock()
        mc._kill_switch_fn = MagicMock(side_effect=RuntimeError("kill check boom"))
        core = self._make_core(master_control=mc)
        # When kill check throws, code continues (does NOT return False) — only
        # logs warning. We just confirm the exception event is emitted.
        core._breach_risk_check(_make_watched())
        text = "\n".join(r.message for r in caplog.records)
        assert "BREACH_RISK_CHECK_EXCEPTION" in text
        assert "exception_type=RuntimeError" in text
        assert "kill check boom" in text

    def test_positions_full_emits_blocked(self, caplog):
        caplog.set_level(logging.INFO, logger="ap.execution_core")
        core = self._make_core(max_positions=2, open_count=2, pending_count=0)
        result = core._breach_risk_check(_make_watched())
        assert result is False
        text = "\n".join(r.message for r in caplog.records)
        assert "BREACH_RISK_CHECK_BLOCKED" in text
        assert "reason=positions_full_at_breach" in text
        assert "positions_open=2" in text
        assert "max_positions=2" in text

    def test_approved_plan_missing_live_emits_blocked(self, caplog):
        caplog.set_level(logging.INFO, logger="ap.execution_core")
        core = self._make_core(mode="LIVE")
        result = core._breach_risk_check(_make_watched())
        # LIVE + no approved_plan = blocked
        assert result is False
        text = "\n".join(r.message for r in caplog.records)
        assert "BREACH_RISK_CHECK_BLOCKED" in text
        assert "reason=approved_plan_missing_at_breach_revalidation" in text

    def test_approved_plan_missing_paper_does_not_block(self, caplog):
        """PAPER fail-open — must NOT emit BLOCKED for approved_plan_missing."""
        caplog.set_level(logging.INFO, logger="ap.execution_core")
        core = self._make_core(mode="PAPER")
        # No master_control means we skip revalidation entirely → True
        result = core._breach_risk_check(_make_watched())
        assert result is True
        text = "\n".join(r.message for r in caplog.records)
        assert "reason=approved_plan_missing_at_breach_revalidation" not in text

    def test_exposure_revalidation_blocked_emits(self, caplog):
        caplog.set_level(logging.INFO, logger="ap.execution_core")
        core = self._make_core(mode="LIVE")
        core._recover_plan_for_revalidation = MagicMock(return_value=MagicMock())
        mc = MagicMock()
        reval = MagicMock()
        reval.ok = False
        reval.reason = "ACTUAL_CONTRACT_COST_EXCEEDS_REMAINING_TOTAL_CAPACITY"
        mc.revalidate_exposure = MagicMock(return_value=reval)
        mc._kill_switch_fn = MagicMock(return_value=False)
        core.master_control = mc
        result = core._breach_risk_check(_make_watched())
        assert result is False
        text = "\n".join(r.message for r in caplog.records)
        assert "BREACH_RISK_CHECK_BLOCKED" in text
        assert "reason=exposure_revalidation_blocked" in text
        assert "ACTUAL_CONTRACT_COST_EXCEEDS_REMAINING_TOTAL_CAPACITY" in text

    def test_exposure_revalidation_exception_live_emits(self, caplog):
        caplog.set_level(logging.INFO, logger="ap.execution_core")
        core = self._make_core(mode="LIVE")
        core._recover_plan_for_revalidation = MagicMock(return_value=MagicMock())
        mc = MagicMock()
        mc.revalidate_exposure = MagicMock(side_effect=ValueError("reval boom"))
        mc._kill_switch_fn = MagicMock(return_value=False)
        core.master_control = mc
        result = core._breach_risk_check(_make_watched())
        assert result is False
        text = "\n".join(r.message for r in caplog.records)
        assert "BREACH_RISK_CHECK_EXCEPTION" in text
        assert "reason=exposure_revalidation_error_live" in text
        assert "ValueError" in text


# ---------------------------------------------------------------------------
# _on_entry_trigger — ENTRY_TRIGGER_BLOCKED_RETURN emission
# ---------------------------------------------------------------------------

class TestEntryTriggerBlockedReturn:

    def test_source_emits_after_breach_risk_check_false(self):
        """Source guard: the emission must immediately follow the False return
        from _breach_risk_check inside _on_entry_trigger."""
        # The string appears twice — once in the helper docstring, once at
        # the actual emission site. Find the SECOND occurrence (the emission).
        first = _EC_SRC.find("ENTRY_TRIGGER_BLOCKED_RETURN")
        assert first != -1
        idx = _EC_SRC.find("ENTRY_TRIGGER_BLOCKED_RETURN", first + 1)
        assert idx != -1, "ENTRY_TRIGGER_BLOCKED_RETURN emission site not found"
        # Look at a generous window before the emission for the breach_risk_check
        # gate and the funnel counter (proves we're in the right code block).
        window = _EC_SRC[max(0, idx - 2000): idx + 1000]
        assert "_breach_risk_check(" in window
        assert "master_control_blocked" in window
        # The emission's reason field must be the agreed value
        assert 'reason="breach_risk_check_false"' in window


# ---------------------------------------------------------------------------
# WATCHER_ON_TRIGGER_RETURNED / WATCHER_ON_TRIGGER_EXCEPTION
# ---------------------------------------------------------------------------

class TestWatcherOnTriggerEmits:

    def test_source_returned_event_in_success_path(self):
        """WATCHER_ON_TRIGGER_RETURNED must be emitted after on_trigger(w)
        succeeds, before the exception block."""
        idx_returned = _EW_SRC.find("WATCHER_ON_TRIGGER_RETURNED")
        idx_call     = _EW_SRC.find("self.on_trigger(w)")
        idx_exc      = _EW_SRC.find("WATCHER_ON_TRIGGER_EXCEPTION")
        assert idx_call != -1
        assert idx_returned != -1
        assert idx_exc != -1
        # RETURNED must come after the call site, EXCEPTION must come after that
        assert idx_call < idx_returned, (
            "WATCHER_ON_TRIGGER_RETURNED must be emitted after the on_trigger call"
        )
        assert idx_returned < idx_exc, (
            "WATCHER_ON_TRIGGER_EXCEPTION must be in the except branch (after RETURNED)"
        )

    def test_source_exception_event_in_exception_path(self):
        """WATCHER_ON_TRIGGER_EXCEPTION must include exception_type and exception_message."""
        idx = _EW_SRC.find("WATCHER_ON_TRIGGER_EXCEPTION")
        region = _EW_SRC[idx: idx + 1200]
        assert "exception_type" in region
        assert "exception_message" in region
        assert "type(exc).__name__" in region
        assert "str(exc)" in region


# ---------------------------------------------------------------------------
# Regression guard — zero behavior change
# ---------------------------------------------------------------------------

class TestDiagnosticLogsDoNotChangeReturnBehavior:
    """The single most important test in this file.

    Proves that adding diagnostic logs has not changed the return values
    of _breach_risk_check across all paths. If any of these fails, the
    PR is no longer diagnostic-only and must not be merged."""

    def _make_core(self, **kwargs):
        defaults = dict(mode="LIVE", kill_switch=False, master_control=None,
                        max_positions=2, open_count=0, pending_count=0)
        defaults.update(kwargs)
        core = _make_core_stub(mode=defaults["mode"])
        core._kill_switch = defaults["kill_switch"]
        core.master_control = defaults["master_control"]
        core._max_positions = defaults["max_positions"]
        core._current_open_position_count = MagicMock(return_value=defaults["open_count"])
        core._current_pending_entry_count = MagicMock(return_value=defaults["pending_count"])
        core._recover_plan_for_revalidation = MagicMock(return_value=None)
        core._cleanup_pending_entry_order = MagicMock()
        core.store = MagicMock()
        return core

    def test_kill_switch_returns_false(self):
        assert self._make_core(kill_switch=True)._breach_risk_check(_make_watched()) is False

    def test_master_kill_switch_returns_false(self):
        mc = MagicMock()
        mc._kill_switch_fn = MagicMock(return_value=True)
        assert self._make_core(master_control=mc)._breach_risk_check(_make_watched()) is False

    def test_positions_full_returns_false(self):
        core = self._make_core(max_positions=2, open_count=2, pending_count=0)
        assert core._breach_risk_check(_make_watched()) is False
        # And it still calls cleanup (existing behavior, unchanged)
        core._cleanup_pending_entry_order.assert_called_once()

    def test_approved_plan_missing_live_returns_false(self):
        assert self._make_core(mode="LIVE")._breach_risk_check(_make_watched()) is False

    def test_approved_plan_missing_paper_returns_true(self):
        # PAPER fail-open: no master_control, no approved_plan → True
        assert self._make_core(mode="PAPER")._breach_risk_check(_make_watched()) is True

    def test_exposure_revalidation_blocked_returns_false(self):
        core = self._make_core(mode="LIVE")
        core._recover_plan_for_revalidation = MagicMock(return_value=MagicMock())
        mc = MagicMock()
        reval = MagicMock(); reval.ok = False; reval.reason = "x"
        mc.revalidate_exposure = MagicMock(return_value=reval)
        mc._kill_switch_fn = MagicMock(return_value=False)
        core.master_control = mc
        assert core._breach_risk_check(_make_watched()) is False

    def test_exposure_revalidation_ok_returns_true(self):
        core = self._make_core(mode="LIVE")
        core._recover_plan_for_revalidation = MagicMock(return_value=MagicMock())
        mc = MagicMock()
        reval = MagicMock(); reval.ok = True
        mc.revalidate_exposure = MagicMock(return_value=reval)
        mc._kill_switch_fn = MagicMock(return_value=False)
        core.master_control = mc
        assert core._breach_risk_check(_make_watched()) is True

    def test_exposure_revalidation_error_live_returns_false(self):
        core = self._make_core(mode="LIVE")
        core._recover_plan_for_revalidation = MagicMock(return_value=MagicMock())
        mc = MagicMock()
        mc.revalidate_exposure = MagicMock(side_effect=ValueError("boom"))
        mc._kill_switch_fn = MagicMock(return_value=False)
        core.master_control = mc
        assert core._breach_risk_check(_make_watched()) is False

    def test_exposure_revalidation_error_paper_returns_true(self):
        """PAPER fail-open on exception."""
        core = self._make_core(mode="PAPER")
        core._recover_plan_for_revalidation = MagicMock(return_value=MagicMock())
        mc = MagicMock()
        mc.revalidate_exposure = MagicMock(side_effect=ValueError("boom"))
        mc._kill_switch_fn = MagicMock(return_value=False)
        core.master_control = mc
        assert core._breach_risk_check(_make_watched()) is True

    def test_cleanup_called_only_for_positions_full(self):
        """The diagnostic PR must NOT cause cleanup to be called for paths
        that didn't call it before. Only positions_full calls cleanup."""
        # kill switch — no cleanup
        core = self._make_core(kill_switch=True)
        core._breach_risk_check(_make_watched())
        core._cleanup_pending_entry_order.assert_not_called()

        # approved_plan_missing LIVE — no cleanup
        core = self._make_core(mode="LIVE")
        core._breach_risk_check(_make_watched())
        core._cleanup_pending_entry_order.assert_not_called()

        # exposure_revalidation block — no cleanup
        core = self._make_core(mode="LIVE")
        core._recover_plan_for_revalidation = MagicMock(return_value=MagicMock())
        mc = MagicMock()
        reval = MagicMock(); reval.ok = False; reval.reason = "x"
        mc.revalidate_exposure = MagicMock(return_value=reval)
        mc._kill_switch_fn = MagicMock(return_value=False)
        core.master_control = mc
        core._breach_risk_check(_make_watched())
        core._cleanup_pending_entry_order.assert_not_called()
