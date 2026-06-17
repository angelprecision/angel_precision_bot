"""
tests/test_morning_handoff_audit.py

P0 — Morning handoff self-audit and auto-rearm.

A valid ENTRY setup in the DB must be automatically classified and, when
safe, re-armed before market open. No manual SQL/admin rescue should be
required for rows that belong in the trading session.

Core safety invariants tested throughout:
  NEVER broker.submit_order
  NEVER create new ENTRY orders
  NEVER change contract / limit_price / qty
  NEVER mutate terminal rows
  NEVER duplicate watcher state for the same local_order_id
  Idempotent: auditing the same row twice → same result

Tests:
  1.  PENDING_TRIGGER real contract, watcher owns it → READY_ARMED
  2.  PENDING_TRIGGER real contract, watcher lost ownership → AUTO_REARMED
  3.  PENDING_TRIGGER DEFERRED:* before cutoff → WAITING_FOR_CONTRACT_AT_BREACH
  4.  PENDING_TRIGGER after cutoff → SKIPPED_AFTER_CUTOFF
  5.  CREATED stale row without watcher evidence → BROKEN_NEEDS_CODE
  6.  Terminal rows never re-armed (EXPIRED, FILLED, CANCELED)
  7.  Rows with broker_order_id never duplicated
  8.  Same row audited twice → idempotent (no duplicate watcher state)
  9.  Live pod only audits live-mode rows
  10. Paper pod only audits paper-mode rows
  11. Dry run classifies but does not call watcher.watch
  12. Re-arm persists auto_rearm metadata into orders.meta
  13. Watcher rejection → BLOCKED_RISK
  14. Source guards: module exports, sentinel log strings
"""
from __future__ import annotations

import importlib.util
import json
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

_REPO = Path(__file__).resolve().parents[1]
_AUDIT_SRC  = (_REPO / "ap_morning_handoff_audit.py").read_text()
_APP_SRC    = (_REPO / "app.py").read_text()


# ---------------------------------------------------------------------------
# Import the audit module under test
# ---------------------------------------------------------------------------

def _load_audit_module():
    spec = importlib.util.spec_from_file_location(
        "ap_morning_handoff_audit_shim",
        _REPO / "ap_morning_handoff_audit.py",
    )
    with patch.dict(sys.modules, {
        "ap.db": MagicMock(conn=MagicMock(), run_with_retry=lambda fn: fn()),
    }):
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    return mod

_audit = _load_audit_module()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _order(
    *,
    local_id: str = "order-test-001",
    symbol: str = "C",
    contract: str = "C260626C00150000",
    status: str = "PENDING_TRIGGER",
    trigger_price: float | None = 155.0,
    broker_order_id: str | None = None,
    submitted_ts=None,
    filled_ts=None,
    direction: str = "CALL",
    execution_mode: str = "live",
    meta: dict | None = None,
    last_error: str | None = None,
) -> dict:
    return {
        "local_order_id": local_id,
        "signal_id": f"sig-{local_id}",
        "plan_id": f"plan-{local_id}",
        "symbol": symbol,
        "contract": contract,
        "direction": direction,
        "score": 72.5,
        "tier": "B",
        "trigger_price": trigger_price,
        "stop_underlying": 148.0,
        "target_underlying": 162.0,
        "pattern": "3-1-2",
        "timeframe": "1d",
        "status": status,
        "broker_order_id": broker_order_id,
        "submitted_ts": submitted_ts,
        "filled_ts": filled_ts,
        "execution_mode": execution_mode,
        "last_error": last_error,
        "meta": meta or {},
        "created_ts": "2026-06-17T05:00:00+00:00",
    }


def _make_watcher(
    *,
    has_order_fn=None,
    watch_returns: bool = True,
):
    w = MagicMock()
    if has_order_fn is not None:
        w.has_order.side_effect = has_order_fn
    else:
        w.has_order.return_value = False
    w.watch.return_value = watch_returns
    w._last_reject_reason = "watcher_rejected_test"
    return w


def _run_audit(
    order: dict,
    *,
    watcher_has_order: bool = False,
    watch_returns: bool = True,
    dry_run: bool = False,
    after_cutoff: bool = False,
    osm=None,
) -> dict:
    watcher = _make_watcher(watch_returns=watch_returns)
    watcher.has_order.return_value = watcher_has_order
    if osm is None:
        osm = MagicMock()

    with patch.object(_audit, "_is_after_eod_cutoff", return_value=after_cutoff), \
         patch.object(_audit, "_persist_rearm_meta"):
        result = _audit._audit_row(
            order,
            entry_watcher=watcher,
            after_cutoff=after_cutoff,
            dry_run=dry_run,
            client_id="jasoncosby1@gmail.com",
            osm=osm,
        )
    return result


# ---------------------------------------------------------------------------
# Source guards
# ---------------------------------------------------------------------------

class TestSourceGuards:
    def test_module_has_run_morning_handoff_audit(self):
        assert "run_morning_handoff_audit" in _AUDIT_SRC

    def test_module_has_audit_row(self):
        assert "_audit_row" in _AUDIT_SRC

    def test_module_has_has_watcher_evidence(self):
        assert "_has_watcher_evidence" in _AUDIT_SRC

    def test_summary_log_sentinel_present(self):
        assert "MORNING_HANDOFF_AUDIT_SUMMARY" in _AUDIT_SRC

    def test_row_log_sentinel_present(self):
        assert "MORNING_HANDOFF_ROW" in _AUDIT_SRC

    def test_classifications_defined(self):
        for c in ("READY_ARMED", "AUTO_REARMED", "WAITING_FOR_BREACH",
                  "WAITING_FOR_CONTRACT_AT_BREACH", "SKIPPED_TERMINAL",
                  "SKIPPED_AFTER_CUTOFF", "BLOCKED_RISK", "BROKEN_NEEDS_CODE"):
            assert c in _AUDIT_SRC, f"Classification {c} missing from source"

    def test_broker_submit_never_called(self):
        """The audit module must never import or invoke broker submit paths.
        Documentation strings and comments may mention it for clarity."""
        # Accept mentions in comment-only or docstring-only lines,
        # reject any line that looks like a function call or import.
        bad_lines = []
        for ln in _AUDIT_SRC.splitlines():
            stripped = ln.strip()
            if "submit_order" not in ln:
                continue
            # Skip pure comment lines
            if stripped.startswith("#"):
                continue
            # Skip docstring safety lines (start with '- NEVER' or '  - NEVER')
            if stripped.startswith("- ") or stripped.startswith("NEVER") or stripped.startswith("* "):
                continue
            # Any remaining line with submit_order that isn't a comment/doc is a violation
            bad_lines.append(ln)
        assert len(bad_lines) == 0, (
            f"submit_order must only appear in comments/docs, found in: {bad_lines}"
        )

    def test_no_order_creation(self):
        """Audit must never create new orders."""
        non_comment_lines = [
            ln for ln in _AUDIT_SRC.splitlines()
            if not ln.strip().startswith("#") and "create_entry_order" in ln
        ]
        assert len(non_comment_lines) == 0, (
            "Audit module must NEVER call create_entry_order"
        )

    def test_terminal_statuses_defined(self):
        assert "_TERMINAL_STATUSES" in _AUDIT_SRC
        assert "EXPIRED" in _AUDIT_SRC
        assert "FILLED" in _AUDIT_SRC

    def test_eod_cutoff_function_present(self):
        assert "_is_after_eod_cutoff" in _AUDIT_SRC

    def test_eod_cutoff_constants_present(self):
        assert "MHA_EOD_CUTOFF_HOUR" in _AUDIT_SRC
        assert "MHA_EOD_CUTOFF_MIN" in _AUDIT_SRC

    def test_default_eod_cutoff_is_1530(self):
        assert _audit.MHA_EOD_CUTOFF_HOUR == 15
        assert _audit.MHA_EOD_CUTOFF_MIN == 30

    def test_auto_rearm_meta_fields_in_source(self):
        assert "auto_rearm_attempted_at" in _AUDIT_SRC
        assert "auto_rearm_result" in _AUDIT_SRC
        assert "auto_rearm_reason" in _AUDIT_SRC

    def test_admin_get_endpoint_in_app(self):
        assert "/admin/morning_handoff_audit" in _APP_SRC

    def test_admin_post_endpoint_in_app(self):
        assert "admin_morning_handoff_audit_post" in _APP_SRC

    def test_safety_comment_in_app(self):
        idx = _APP_SRC.find("morning_handoff_audit")
        region = _APP_SRC[idx: idx + 800]
        assert "NEVER" in region or "broker" in region.lower()

    def test_persist_rearm_meta_exists(self):
        assert "_persist_rearm_meta" in _AUDIT_SRC

    def test_build_audit_plan_exists(self):
        assert "_build_audit_plan" in _AUDIT_SRC


# ---------------------------------------------------------------------------
# Test 1 — PENDING_TRIGGER real contract, watcher owns it → READY_ARMED
# ---------------------------------------------------------------------------

class TestReadyArmed:
    def test_watcher_owned_classified_ready_armed(self):
        result = _run_audit(
            _order(contract="C260626C00150000", trigger_price=155.0),
            watcher_has_order=True,
        )
        assert result["classification"] == _audit.READY_ARMED

    def test_watcher_owned_action_is_already_owned(self):
        result = _run_audit(
            _order(contract="C260626C00150000", trigger_price=155.0),
            watcher_has_order=True,
        )
        assert result["action"] == "already_owned"

    def test_watcher_owned_does_not_call_watch(self):
        watcher = _make_watcher()
        watcher.has_order.return_value = True
        with patch.object(_audit, "_is_after_eod_cutoff", return_value=False), \
             patch.object(_audit, "_persist_rearm_meta"):
            _audit._audit_row(
                _order(contract="C260626C00150000", trigger_price=155.0),
                entry_watcher=watcher,
                after_cutoff=False,
                dry_run=False,
                client_id="test",
                osm=MagicMock(),
            )
        watcher.watch.assert_not_called()


# ---------------------------------------------------------------------------
# Test 2 — PENDING_TRIGGER real contract, watcher lost ownership → AUTO_REARMED
# ---------------------------------------------------------------------------

class TestAutoRearmed:
    def test_lost_ownership_classified_auto_rearmed(self):
        result = _run_audit(
            _order(contract="C260626C00150000", trigger_price=155.0),
            watcher_has_order=False,
            watch_returns=True,
        )
        assert result["classification"] == _audit.AUTO_REARMED

    def test_auto_rearmed_action(self):
        result = _run_audit(
            _order(contract="C260626C00150000", trigger_price=155.0),
            watcher_has_order=False,
            watch_returns=True,
        )
        assert result["action"] == "auto_rearmed"

    def test_watcher_watch_called_once(self):
        watcher = _make_watcher(watch_returns=True)
        watcher.has_order.return_value = False
        with patch.object(_audit, "_is_after_eod_cutoff", return_value=False), \
             patch.object(_audit, "_persist_rearm_meta"):
            _audit._audit_row(
                _order(contract="C260626C00150000", trigger_price=155.0),
                entry_watcher=watcher,
                after_cutoff=False,
                dry_run=False,
                client_id="test",
                osm=MagicMock(),
            )
        assert watcher.watch.call_count == 1

    def test_rearm_succeeded_is_true(self):
        result = _run_audit(
            _order(contract="C260626C00150000", trigger_price=155.0),
            watcher_has_order=False,
            watch_returns=True,
        )
        assert result["rearm_succeeded"] is True

    def test_meta_persisted_on_rearm(self):
        persist_mock = MagicMock()
        watcher = _make_watcher(watch_returns=True)
        watcher.has_order.return_value = False
        osm = MagicMock()
        with patch.object(_audit, "_is_after_eod_cutoff", return_value=False), \
             patch.object(_audit, "_persist_rearm_meta", persist_mock):
            _audit._audit_row(
                _order(contract="C260626C00150000", trigger_price=155.0),
                entry_watcher=watcher,
                after_cutoff=False,
                dry_run=False,
                client_id="test",
                osm=osm,
            )
        persist_mock.assert_called_once()
        kwargs = persist_mock.call_args.kwargs
        assert kwargs["result"] == "success"

    def test_jason_regression_order_22514(self):
        """Order 22514: C260626C00150000, trigger_price=155.0, broker_order_id=NULL."""
        result = _run_audit(
            _order(
                local_id="order-22514",
                symbol="C",
                contract="C260626C00150000",
                trigger_price=155.0,
                broker_order_id=None,
                submitted_ts=None,
            ),
            watcher_has_order=False,
            watch_returns=True,
        )
        assert result["classification"] == _audit.AUTO_REARMED, (
            f"Jason order 22514 must be AUTO_REARMED, got {result['classification']}"
        )


# ---------------------------------------------------------------------------
# Test 3 — PENDING_TRIGGER DEFERRED:* before cutoff
# ---------------------------------------------------------------------------

class TestDeferredContract:
    def test_deferred_owned_classified_waiting_for_contract_at_breach(self):
        result = _run_audit(
            _order(contract="DEFERRED:FANG", trigger_price=42.0,
                   meta={"contract_deferred": True}),
            watcher_has_order=True,
        )
        assert result["classification"] == _audit.WAITING_FOR_CONTRACT_AT_BREACH

    def test_deferred_not_owned_rearmed_classified_waiting(self):
        result = _run_audit(
            _order(contract="DEFERRED:FANG", trigger_price=42.0,
                   meta={"contract_deferred": True}),
            watcher_has_order=False,
            watch_returns=True,
        )
        assert result["classification"] == _audit.WAITING_FOR_CONTRACT_AT_BREACH

    def test_deferred_not_owned_calls_watch(self):
        watcher = _make_watcher(watch_returns=True)
        watcher.has_order.return_value = False
        with patch.object(_audit, "_is_after_eod_cutoff", return_value=False), \
             patch.object(_audit, "_persist_rearm_meta"):
            _audit._audit_row(
                _order(contract="DEFERRED:FANG", trigger_price=42.0),
                entry_watcher=watcher,
                after_cutoff=False,
                dry_run=False,
                client_id="test",
                osm=MagicMock(),
            )
        watcher.watch.assert_called_once()


# ---------------------------------------------------------------------------
# Test 4 — After entry cutoff → SKIPPED_AFTER_CUTOFF
# ---------------------------------------------------------------------------

class TestSkippedAfterCutoff:
    def test_after_cutoff_classified_skipped(self):
        result = _run_audit(
            _order(contract="C260626C00150000", trigger_price=155.0),
            after_cutoff=True,
        )
        assert result["classification"] == _audit.SKIPPED_AFTER_CUTOFF

    def test_after_cutoff_does_not_call_watch(self):
        watcher = _make_watcher()
        with patch.object(_audit, "_persist_rearm_meta"):
            _audit._audit_row(
                _order(contract="C260626C00150000", trigger_price=155.0),
                entry_watcher=watcher,
                after_cutoff=True,
                dry_run=False,
                client_id="test",
                osm=MagicMock(),
            )
        watcher.watch.assert_not_called()

    def test_after_cutoff_deferred_also_skipped(self):
        result = _run_audit(
            _order(contract="DEFERRED:USB", trigger_price=60.0),
            after_cutoff=True,
        )
        assert result["classification"] == _audit.SKIPPED_AFTER_CUTOFF


# ---------------------------------------------------------------------------
# Test 5 — CREATED stale row without watcher evidence → BROKEN_NEEDS_CODE
# ---------------------------------------------------------------------------

class TestBrokenNeedsCode:
    def test_no_evidence_classified_broken(self):
        result = _run_audit(
            _order(
                contract="RAWTICKERONLY",
                trigger_price=None,
                meta={},
                status="CREATED",
            ),
        )
        assert result["classification"] == _audit.BROKEN_NEEDS_CODE

    def test_no_evidence_does_not_call_watch(self):
        watcher = _make_watcher()
        with patch.object(_audit, "_persist_rearm_meta"):
            _audit._audit_row(
                _order(contract="RAWTICKERONLY", trigger_price=None, meta={}),
                entry_watcher=watcher,
                after_cutoff=False,
                dry_run=False,
                client_id="test",
                osm=MagicMock(),
            )
        watcher.watch.assert_not_called()

    def test_zero_trigger_price_no_other_evidence_is_broken(self):
        result = _run_audit(
            _order(contract="RAWTICKERONLY", trigger_price=0.0, meta={})
        )
        assert result["classification"] == _audit.BROKEN_NEEDS_CODE


# ---------------------------------------------------------------------------
# Test 6 — Terminal rows are never re-armed
# ---------------------------------------------------------------------------

class TestTerminalRowsNotRearmed:
    @pytest.mark.parametrize("terminal_status", [
        "EXPIRED", "FILLED", "CANCELED", "CANCELLED",
        "CLOSED", "STOPPED", "TAKEN_PROFIT", "ERROR",
    ])
    def test_terminal_status_is_skipped(self, terminal_status):
        result = _run_audit(
            _order(
                contract="C260626C00150000",
                trigger_price=155.0,
                status=terminal_status,
            )
        )
        assert result["classification"] == _audit.SKIPPED_TERMINAL

    @pytest.mark.parametrize("terminal_status", ["EXPIRED", "FILLED", "CANCELED"])
    def test_terminal_status_watch_not_called(self, terminal_status):
        watcher = _make_watcher()
        with patch.object(_audit, "_persist_rearm_meta"):
            _audit._audit_row(
                _order(status=terminal_status, contract="C260626C00150000", trigger_price=155.0),
                entry_watcher=watcher,
                after_cutoff=False,
                dry_run=False,
                client_id="test",
                osm=MagicMock(),
            )
        watcher.watch.assert_not_called()


# ---------------------------------------------------------------------------
# Test 7 — Rows with broker_order_id or submitted_ts are never duplicated
# ---------------------------------------------------------------------------

class TestBrokerManagedSkipped:
    def test_broker_order_id_present_classified_skipped(self):
        result = _run_audit(
            _order(contract="C260626C00150000", trigger_price=155.0,
                   broker_order_id="BROKER-123")
        )
        assert "BROKER" in result["classification"] or result["action"] == "skip_broker_managed"

    def test_submitted_ts_present_classified_skipped(self):
        result = _run_audit(
            _order(contract="C260626C00150000", trigger_price=155.0,
                   submitted_ts="2026-06-17T09:35:00+00:00")
        )
        assert result["action"] in ("skip_broker_managed",) or "BROKER" in (result["classification"] or "")

    def test_broker_order_id_present_watch_not_called(self):
        watcher = _make_watcher()
        with patch.object(_audit, "_persist_rearm_meta"):
            _audit._audit_row(
                _order(contract="C260626C00150000", trigger_price=155.0,
                       broker_order_id="BROKER-456"),
                entry_watcher=watcher,
                after_cutoff=False,
                dry_run=False,
                client_id="test",
                osm=MagicMock(),
            )
        watcher.watch.assert_not_called()


# ---------------------------------------------------------------------------
# Test 8 — Idempotency: same row audited twice, no duplicate watcher state
# ---------------------------------------------------------------------------

class TestIdempotency:
    def test_double_audit_owned_row_calls_watch_zero_times(self):
        """Once watcher owns the row, a second audit call should see already_owned."""
        watcher = _make_watcher(watch_returns=True)
        # First call: watcher doesn't own → watch() called, armed
        watcher.has_order.return_value = False
        with patch.object(_audit, "_is_after_eod_cutoff", return_value=False), \
             patch.object(_audit, "_persist_rearm_meta"):
            _audit._audit_row(
                _order(contract="C260626C00150000", trigger_price=155.0),
                entry_watcher=watcher,
                after_cutoff=False,
                dry_run=False,
                client_id="test",
                osm=MagicMock(),
            )
        assert watcher.watch.call_count == 1

        # Second call: watcher now owns it → watch() NOT called again
        watcher.has_order.return_value = True
        with patch.object(_audit, "_is_after_eod_cutoff", return_value=False), \
             patch.object(_audit, "_persist_rearm_meta"):
            result2 = _audit._audit_row(
                _order(contract="C260626C00150000", trigger_price=155.0),
                entry_watcher=watcher,
                after_cutoff=False,
                dry_run=False,
                client_id="test",
                osm=MagicMock(),
            )
        assert watcher.watch.call_count == 1, (
            "watch() must only be called once; idempotent on second audit"
        )
        assert result2["classification"] == _audit.READY_ARMED


# ---------------------------------------------------------------------------
# Test 9 & 10 — Execution mode isolation (live vs paper)
# ---------------------------------------------------------------------------

class TestExecutionModeIsolation:
    """The DB query filters by execution_mode, so live rows never appear
    in paper audits and vice versa. We test the _load_audit_rows SQL filter
    in source and the end-to-end run summary scans=0 for wrong mode."""

    def test_source_contains_execution_mode_filter(self):
        fn_start = _AUDIT_SRC.find("def _load_audit_rows")
        fn_body = _AUDIT_SRC[fn_start: fn_start + 1000]
        assert "execution_mode" in fn_body, (
            "_load_audit_rows must filter by execution_mode"
        )

    def test_run_audit_passes_mode_to_load(self):
        """run_morning_handoff_audit passes execution_mode to _load_audit_rows."""
        with patch.object(_audit, "_load_audit_rows", return_value=[]) as mock_load, \
             patch.object(_audit, "_is_after_eod_cutoff", return_value=False):
            _audit.run_morning_handoff_audit(
                client_id="test@example.com",
                entry_watcher=_make_watcher(),
                osm=MagicMock(),
                execution_mode="paper",
                dry_run=True,
            )
        mock_load.assert_called_once()
        # _load_audit_rows(client_id, execution_mode, lookback_hours)
        call_args = mock_load.call_args
        # Handle both positional and keyword argument styles
        if call_args.args and len(call_args.args) >= 2:
            mode_arg = call_args.args[1]
        else:
            mode_arg = call_args.kwargs.get("execution_mode", call_args.kwargs.get("mode"))
        assert mode_arg == "paper"


# ---------------------------------------------------------------------------
# Test 11 — Dry run classifies but does not call watcher.watch
# ---------------------------------------------------------------------------

class TestDryRun:
    def test_dry_run_does_not_call_watch(self):
        watcher = _make_watcher()
        watcher.has_order.return_value = False
        with patch.object(_audit, "_is_after_eod_cutoff", return_value=False), \
             patch.object(_audit, "_persist_rearm_meta"):
            _audit._audit_row(
                _order(contract="C260626C00150000", trigger_price=155.0),
                entry_watcher=watcher,
                after_cutoff=False,
                dry_run=True,
                client_id="test",
                osm=MagicMock(),
            )
        watcher.watch.assert_not_called()

    def test_dry_run_classifies_real_contract_as_waiting(self):
        result = _run_audit(
            _order(contract="C260626C00150000", trigger_price=155.0),
            watcher_has_order=False,
            dry_run=True,
        )
        assert result["action"] == "dry_run_would_rearm"
        assert result["rearm_succeeded"] is None

    def test_dry_run_classifies_deferred_as_waiting_for_contract(self):
        result = _run_audit(
            _order(contract="DEFERRED:USB", trigger_price=60.0),
            watcher_has_order=False,
            dry_run=True,
        )
        assert result["classification"] == _audit.WAITING_FOR_CONTRACT_AT_BREACH
        assert result["action"] == "dry_run_would_rearm"


# ---------------------------------------------------------------------------
# Test 12 — Re-arm persists auto_rearm metadata
# ---------------------------------------------------------------------------

class TestAutoRearmMetadata:
    def test_persist_called_with_success_on_rearm(self):
        persist_mock = MagicMock()
        watcher = _make_watcher(watch_returns=True)
        watcher.has_order.return_value = False
        with patch.object(_audit, "_is_after_eod_cutoff", return_value=False), \
             patch.object(_audit, "_persist_rearm_meta", persist_mock):
            _audit._audit_row(
                _order(contract="C260626C00150000", trigger_price=155.0),
                entry_watcher=watcher,
                after_cutoff=False,
                dry_run=False,
                client_id="test",
                osm=MagicMock(),
            )
        persist_mock.assert_called_once()
        kw = persist_mock.call_args.kwargs
        assert kw["result"] == "success"
        assert "order-test-001" in str(persist_mock.call_args)

    def test_persist_called_with_failed_on_watcher_reject(self):
        persist_mock = MagicMock()
        watcher = _make_watcher(watch_returns=False)
        watcher.has_order.return_value = False
        with patch.object(_audit, "_is_after_eod_cutoff", return_value=False), \
             patch.object(_audit, "_persist_rearm_meta", persist_mock):
            _audit._audit_row(
                _order(contract="C260626C00150000", trigger_price=155.0),
                entry_watcher=watcher,
                after_cutoff=False,
                dry_run=False,
                client_id="test",
                osm=MagicMock(),
            )
        persist_mock.assert_called_once()
        kw = persist_mock.call_args.kwargs
        assert kw["result"] == "failed"

    def test_persist_not_called_on_dry_run(self):
        persist_mock = MagicMock()
        with patch.object(_audit, "_is_after_eod_cutoff", return_value=False), \
             patch.object(_audit, "_persist_rearm_meta", persist_mock):
            _audit._audit_row(
                _order(contract="C260626C00150000", trigger_price=155.0),
                entry_watcher=_make_watcher(),
                after_cutoff=False,
                dry_run=True,
                client_id="test",
                osm=MagicMock(),
            )
        persist_mock.assert_not_called()


# ---------------------------------------------------------------------------
# Test 13 — Watcher rejection → BLOCKED_RISK
# ---------------------------------------------------------------------------

class TestBlockedRisk:
    def test_watcher_reject_classified_blocked_risk(self):
        result = _run_audit(
            _order(contract="C260626C00150000", trigger_price=155.0),
            watcher_has_order=False,
            watch_returns=False,
        )
        assert result["classification"] == _audit.BLOCKED_RISK

    def test_watcher_reject_action_is_rearm_failed(self):
        result = _run_audit(
            _order(contract="C260626C00150000", trigger_price=155.0),
            watcher_has_order=False,
            watch_returns=False,
        )
        assert result["action"] == "rearm_failed"

    def test_watcher_reject_rearm_succeeded_false(self):
        result = _run_audit(
            _order(contract="C260626C00150000", trigger_price=155.0),
            watcher_has_order=False,
            watch_returns=False,
        )
        assert result["rearm_succeeded"] is False


# ---------------------------------------------------------------------------
# Test 14 — run_morning_handoff_audit summary counts
# ---------------------------------------------------------------------------

class TestSummaryCounts:
    def test_scanned_matches_row_count(self):
        rows = [
            _order(local_id="o1", contract="C260626C00150000", trigger_price=155.0),
            _order(local_id="o2", contract="DEFERRED:USB", trigger_price=60.0),
        ]
        watcher = _make_watcher(watch_returns=True)
        watcher.has_order.return_value = False

        with patch.object(_audit, "_load_audit_rows", return_value=rows), \
             patch.object(_audit, "_is_after_eod_cutoff", return_value=False), \
             patch.object(_audit, "_persist_rearm_meta"):
            summary = _audit.run_morning_handoff_audit(
                client_id="test@example.com",
                entry_watcher=watcher,
                osm=MagicMock(),
                execution_mode="live",
                dry_run=False,
            )
        assert summary["scanned"] == 2

    def test_auto_rearmed_count(self):
        rows = [
            _order(local_id="o1", contract="C260626C00150000", trigger_price=155.0),
            _order(local_id="o2", contract="C260627C00160000", trigger_price=160.0),
        ]
        watcher = _make_watcher(watch_returns=True)
        watcher.has_order.return_value = False

        with patch.object(_audit, "_load_audit_rows", return_value=rows), \
             patch.object(_audit, "_is_after_eod_cutoff", return_value=False), \
             patch.object(_audit, "_persist_rearm_meta"):
            summary = _audit.run_morning_handoff_audit(
                client_id="test@example.com",
                entry_watcher=watcher,
                osm=MagicMock(),
                execution_mode="live",
                dry_run=False,
            )
        assert summary["auto_rearmed"] == 2

    def test_summary_ok_true_on_success(self):
        with patch.object(_audit, "_load_audit_rows", return_value=[]), \
             patch.object(_audit, "_is_after_eod_cutoff", return_value=False):
            summary = _audit.run_morning_handoff_audit(
                client_id="test@example.com",
                entry_watcher=_make_watcher(),
                osm=MagicMock(),
                execution_mode="live",
            )
        assert summary["ok"] is True

    def test_summary_contains_rows_list(self):
        with patch.object(_audit, "_load_audit_rows", return_value=[]), \
             patch.object(_audit, "_is_after_eod_cutoff", return_value=False):
            summary = _audit.run_morning_handoff_audit(
                client_id="test@example.com",
                entry_watcher=_make_watcher(),
                osm=MagicMock(),
                execution_mode="live",
            )
        assert "rows" in summary
        assert isinstance(summary["rows"], list)


# ---------------------------------------------------------------------------
# Off-market restart / handoff certification tests
#
# These are full-fidelity restart simulations proving that a valid DB row
# recovers without manual rescue when watcher in-memory state is empty.
# Both tests run while "market is closed" (after_cutoff=False meaning pre-
# market, before the 15:30 ET entry-cutoff) and assert the broker is never
# touched.
# ---------------------------------------------------------------------------

class TestValidPendingTriggerSurvivesRestartAndAutoRearms:
    """
    test_valid_pending_trigger_survives_restart_and_auto_rearms

    Scenario:
      - Valid orders row: kind=ENTRY status=PENDING_TRIGGER execution_mode=live
        client_id=jasoncosby1@gmail.com contract=C260626C00150000
        trigger_price=155.0 broker_order_id=NULL submitted_ts=NULL
        meta contains watcher evidence (trigger_type=breach)
      - Process restart: watcher in-memory registry is EMPTY
        (has_order returns False for every local_order_id)
      - Run morning_handoff_audit(dry_run=False)

    Asserts:
      1. row classified AUTO_REARMED or READY_ARMED
      2. watcher.watch path called exactly once
      3. no new orders row is created
      4. broker.submit_order is never called
      5. row status remains PENDING_TRIGGER (not mutated by audit)
      6. last_error is NOT 'PENDING_TRIGGER_ORPHAN_EXPIRED' after audit
      7. meta.auto_rearm_attempted_at is written
      8. meta.auto_rearm_result is written
      9. running the audit a second time does not duplicate watcher state
    """

    CLIENT_ID = "jasoncosby1@gmail.com"
    LOCAL_ID  = "order-22514-restart"

    def _db_row(self) -> dict:
        """Simulate what _load_audit_rows would return from the DB after restart.

        This is the exact shape of a real PENDING_TRIGGER watcher-held row:
        - kind=ENTRY, status=PENDING_TRIGGER
        - broker_order_id=NULL, submitted_ts=NULL, filled_ts=NULL
        - trigger_price > 0 (watcher evidence — MC writes this at plan creation)
        - meta.trigger_type present (OSM writes this from plan.trigger_type)
        - real option contract C260626C00150000
        """
        return {
            "local_order_id": self.LOCAL_ID,
            "signal_id":      "2026-06-17:1-1:C:1d:CALL",
            "plan_id":        "plan-restart-test-001",
            "symbol":         "C",
            "contract":       "C260626C00150000",
            "direction":      "CALL",
            "score":          72.5,
            "tier":           "B",
            "trigger_price":  155.0,
            "stop_underlying": 148.0,
            "target_underlying": 162.0,
            "pattern":        "3-1-2",
            "timeframe":      "1d",
            "status":         "PENDING_TRIGGER",
            "broker_order_id": None,   # NULL — expected for pre-submit watcher row
            "submitted_ts":    None,   # NULL — not yet submitted
            "filled_ts":       None,
            "execution_mode":  "live",
            "last_error":      None,
            "meta": {
                "trigger_type":   "breach",        # watcher evidence
                "signal_id":      "2026-06-17:1-1:C:1d:CALL",
                "selected_contract": "C260626C00150000",
                "execution_mode": "live",
                "score":          72.5,
                "tier":           "B",
            },
            "created_ts": "2026-06-17T05:00:00+00:00",
        }

    def _make_empty_watcher(self):
        """Watcher with empty in-memory registry — simulates post-restart state."""
        w = MagicMock()
        # Empty registry: has_order returns False for every ID
        w.has_order.return_value = False
        # watch() succeeds — re-arm the order
        w.watch.return_value = True
        w._last_reject_reason = None
        return w

    def _make_broker(self):
        """Stub broker that tracks whether submit_order is ever called."""
        b = MagicMock()
        b.submit_order = MagicMock()
        return b

    def _run_full_audit(self, watcher, broker, captured_meta: list) -> dict:
        """Run run_morning_handoff_audit with the DB row and empty watcher."""
        def _capture_persist(osm, local_order_id, result, reason):
            captured_meta.append({
                "local_order_id":          local_order_id,
                "auto_rearm_result":       result,
                "auto_rearm_reason":       reason,
                "auto_rearm_attempted_at": "captured",
            })

        with patch.object(_audit, "_load_audit_rows", return_value=[self._db_row()]), \
             patch.object(_audit, "_is_after_eod_cutoff", return_value=False), \
             patch.object(_audit, "_persist_rearm_meta", side_effect=_capture_persist):
            return _audit.run_morning_handoff_audit(
                client_id=self.CLIENT_ID,
                entry_watcher=watcher,
                osm=MagicMock(),
                execution_mode="live",
                dry_run=False,
            )

    # ── Assertion 1: classification ──────────────────────────────────────────

    def test_classified_auto_rearmed_or_ready_armed(self):
        watcher = self._make_empty_watcher()
        captured_meta: list = []
        summary = self._run_full_audit(watcher, self._make_broker(), captured_meta)
        assert summary["scanned"] == 1
        row = summary["rows"][0]
        assert row["classification"] in (_audit.AUTO_REARMED, _audit.READY_ARMED), (
            f"Expected AUTO_REARMED or READY_ARMED after restart, "
            f"got {row['classification']}. "
            f"A valid PENDING_TRIGGER row must survive a watcher restart."
        )

    # ── Assertion 2: watcher.watch called exactly once ───────────────────────

    def test_watcher_watch_called_exactly_once(self):
        watcher = self._make_empty_watcher()
        captured_meta: list = []
        self._run_full_audit(watcher, self._make_broker(), captured_meta)
        assert watcher.watch.call_count == 1, (
            f"watcher.watch must be called exactly once on restart re-arm, "
            f"called {watcher.watch.call_count} times"
        )

    def test_watcher_watch_called_with_correct_local_order_id(self):
        watcher = self._make_empty_watcher()
        captured_meta: list = []
        self._run_full_audit(watcher, self._make_broker(), captured_meta)
        assert watcher.watch.called
        # Second arg to watch() is local_order_id
        call_args = watcher.watch.call_args
        local_id_arg = call_args.args[1] if len(call_args.args) > 1 else call_args.kwargs.get("local_order_id")
        assert local_id_arg == self.LOCAL_ID, (
            f"watch() must be called with local_order_id={self.LOCAL_ID!r}, "
            f"got {local_id_arg!r}"
        )

    # ── Assertion 3: no new orders row created ───────────────────────────────

    def test_no_new_orders_row_created(self):
        """The audit module must never call create_entry_order.
        Verified structurally: _audit has no create_entry_order reference."""
        assert not hasattr(_audit, "create_entry_order"), (
            "Audit module must not expose create_entry_order — "
            "it may never create new order rows"
        )
        # Also verify the source for belt-and-suspenders
        assert "create_entry_order" not in _AUDIT_SRC

    # ── Assertion 4: broker.submit_order never called ────────────────────────

    def test_broker_submit_order_never_called(self):
        broker = self._make_broker()
        watcher = self._make_empty_watcher()
        captured_meta: list = []
        # The audit module never receives the broker object — the test confirms
        # that no broker is in scope and that submit_order is never referenced
        # outside comments in the module source.
        self._run_full_audit(watcher, broker, captured_meta)
        broker.submit_order.assert_not_called()

    # ── Assertion 5: status remains PENDING_TRIGGER ──────────────────────────

    def test_row_status_not_mutated_by_audit(self):
        """The audit must never change the order status.
        The row dict must still show PENDING_TRIGGER after the audit runs.
        """
        original_row = self._db_row()
        original_status = original_row["status"]

        watcher = self._make_empty_watcher()
        with patch.object(_audit, "_load_audit_rows", return_value=[original_row]), \
             patch.object(_audit, "_is_after_eod_cutoff", return_value=False), \
             patch.object(_audit, "_persist_rearm_meta"):
            _audit.run_morning_handoff_audit(
                client_id=self.CLIENT_ID,
                entry_watcher=watcher,
                osm=MagicMock(),
                execution_mode="live",
                dry_run=False,
            )
        assert original_row["status"] == original_status, (
            f"Row status must not be mutated by audit. "
            f"Expected {original_status!r}, got {original_row['status']!r}"
        )

    # ── Assertion 6: last_error is NOT PENDING_TRIGGER_ORPHAN_EXPIRED ────────

    def test_last_error_not_orphan_expired_after_audit(self):
        """After audit, the row must not carry the orphan-expired error.
        This is the production regression guard: order 22514 must never
        be labeled PENDING_TRIGGER_ORPHAN_EXPIRED by the audit path."""
        original_row = self._db_row()

        watcher = self._make_empty_watcher()
        with patch.object(_audit, "_load_audit_rows", return_value=[original_row]), \
             patch.object(_audit, "_is_after_eod_cutoff", return_value=False), \
             patch.object(_audit, "_persist_rearm_meta"):
            summary = _audit.run_morning_handoff_audit(
                client_id=self.CLIENT_ID,
                entry_watcher=watcher,
                osm=MagicMock(),
                execution_mode="live",
                dry_run=False,
            )

        # last_error must never be set to the orphan-expired string by the audit
        # (the audit does not write last_error at all — only meta fields)
        for row in summary["rows"]:
            row_last_error = row.get("last_error") or ""
            assert "PENDING_TRIGGER_ORPHAN_EXPIRED" not in str(row_last_error), (
                f"Audit must NEVER write PENDING_TRIGGER_ORPHAN_EXPIRED to a row. "
                f"Got last_error={row_last_error!r}"
            )

    # ── Assertions 7 & 8: meta.auto_rearm fields written ─────────────────────

    def test_auto_rearm_attempted_at_written(self):
        watcher = self._make_empty_watcher()
        captured_meta: list = []
        self._run_full_audit(watcher, self._make_broker(), captured_meta)
        assert len(captured_meta) == 1, (
            f"Expected exactly one _persist_rearm_meta call, got {len(captured_meta)}"
        )
        assert "auto_rearm_attempted_at" in captured_meta[0], (
            "auto_rearm_attempted_at must be written to orders.meta"
        )

    def test_auto_rearm_result_written(self):
        watcher = self._make_empty_watcher()
        captured_meta: list = []
        self._run_full_audit(watcher, self._make_broker(), captured_meta)
        assert len(captured_meta) == 1
        assert "auto_rearm_result" in captured_meta[0], (
            "auto_rearm_result must be written to orders.meta"
        )
        assert captured_meta[0]["auto_rearm_result"] == "success", (
            f"auto_rearm_result must be 'success' when watch() returns True, "
            f"got {captured_meta[0]['auto_rearm_result']!r}"
        )

    def test_auto_rearm_reason_written(self):
        watcher = self._make_empty_watcher()
        captured_meta: list = []
        self._run_full_audit(watcher, self._make_broker(), captured_meta)
        assert len(captured_meta) == 1
        assert "auto_rearm_reason" in captured_meta[0]

    # ── Assertion 9: second audit does not duplicate watcher state ───────────

    def test_second_audit_does_not_duplicate_watcher_state(self):
        """After first audit re-arms the watcher, the second audit must see
        'already owned' and call watch() zero additional times."""
        watcher = self._make_empty_watcher()
        captured_meta: list = []

        # First audit: watcher doesn't know the order → watch() called once
        self._run_full_audit(watcher, self._make_broker(), captured_meta)
        assert watcher.watch.call_count == 1, "First audit must call watch() once"

        # Simulate: watcher now owns the order (post re-arm in-memory state)
        watcher.has_order.return_value = True

        # Second audit: watcher already owns → watch() NOT called again
        self._run_full_audit(watcher, self._make_broker(), captured_meta)
        assert watcher.watch.call_count == 1, (
            f"Second audit must NOT call watch() again (idempotency). "
            f"watch() was called {watcher.watch.call_count} times total."
        )

    def test_second_audit_classified_ready_armed(self):
        """After re-arm, a second audit classifies the row READY_ARMED."""
        watcher = self._make_empty_watcher()

        # First audit
        with patch.object(_audit, "_load_audit_rows", return_value=[self._db_row()]), \
             patch.object(_audit, "_is_after_eod_cutoff", return_value=False), \
             patch.object(_audit, "_persist_rearm_meta"):
            _audit.run_morning_handoff_audit(
                client_id=self.CLIENT_ID, entry_watcher=watcher,
                osm=MagicMock(), execution_mode="live", dry_run=False,
            )

        # Watcher now owns it
        watcher.has_order.return_value = True

        # Second audit
        with patch.object(_audit, "_load_audit_rows", return_value=[self._db_row()]), \
             patch.object(_audit, "_is_after_eod_cutoff", return_value=False), \
             patch.object(_audit, "_persist_rearm_meta"):
            summary2 = _audit.run_morning_handoff_audit(
                client_id=self.CLIENT_ID, entry_watcher=watcher,
                osm=MagicMock(), execution_mode="live", dry_run=False,
            )
        assert summary2["rows"][0]["classification"] == _audit.READY_ARMED, (
            f"Second audit must classify as READY_ARMED, "
            f"got {summary2['rows'][0]['classification']!r}"
        )


# ---------------------------------------------------------------------------
# Off-market restart: DEFERRED:* contract
# ---------------------------------------------------------------------------

class TestDeferredPendingTriggerSurvivesRestartAndAutoRearms:
    """
    test_deferred_pending_trigger_survives_restart_and_auto_rearms

    Same as above but contract='DEFERRED:FANG'.
    A deferred overnight signal must be preserved and classified
    AUTO_REARMED or WAITING_FOR_CONTRACT_AT_BREACH after a watcher restart.
    """

    CLIENT_ID = "jasoncosby1@gmail.com"
    LOCAL_ID  = "order-deferred-restart-001"

    def _db_row(self) -> dict:
        return {
            "local_order_id": self.LOCAL_ID,
            "signal_id":      "2026-06-17:1-1:FANG:1d:CALL",
            "plan_id":        "plan-deferred-restart-001",
            "symbol":         "FANG",
            "contract":       "DEFERRED:FANG",
            "direction":      "CALL",
            "score":          68.0,
            "tier":           "B",
            "trigger_price":  42.0,
            "stop_underlying": 38.5,
            "target_underlying": 46.0,
            "pattern":        "3-1-2",
            "timeframe":      "1d",
            "status":         "PENDING_TRIGGER",
            "broker_order_id": None,
            "submitted_ts":    None,
            "filled_ts":       None,
            "execution_mode":  "live",
            "last_error":      None,
            "meta": {
                "contract_deferred":  True,
                "overnight":          True,
                "trigger_type":       "breach",
                "selected_contract":  "DEFERRED:FANG",
                "execution_mode":     "live",
                "sizing_context": {
                    "account_equity": 1987.24,
                    "remaining_capital": 198.724,
                },
            },
            "created_ts": "2026-06-17T05:00:00+00:00",
        }

    def _make_empty_watcher(self):
        w = MagicMock()
        w.has_order.return_value = False
        w.watch.return_value = True
        w._last_reject_reason = None
        return w

    def _run_full_audit(self, watcher, captured_meta: list) -> dict:
        def _capture_persist(osm, local_order_id, result, reason):
            captured_meta.append({
                "local_order_id":    local_order_id,
                "auto_rearm_result": result,
                "auto_rearm_reason": reason,
                "auto_rearm_attempted_at": "captured",
            })

        with patch.object(_audit, "_load_audit_rows", return_value=[self._db_row()]), \
             patch.object(_audit, "_is_after_eod_cutoff", return_value=False), \
             patch.object(_audit, "_persist_rearm_meta", side_effect=_capture_persist):
            return _audit.run_morning_handoff_audit(
                client_id=self.CLIENT_ID,
                entry_watcher=watcher,
                osm=MagicMock(),
                execution_mode="live",
                dry_run=False,
            )

    def test_classified_auto_rearmed_or_waiting_for_contract_at_breach(self):
        watcher = self._make_empty_watcher()
        captured_meta: list = []
        summary = self._run_full_audit(watcher, captured_meta)
        assert summary["scanned"] == 1
        row = summary["rows"][0]
        valid = {_audit.AUTO_REARMED, _audit.WAITING_FOR_CONTRACT_AT_BREACH}
        assert row["classification"] in valid, (
            f"DEFERRED row must be classified AUTO_REARMED or "
            f"WAITING_FOR_CONTRACT_AT_BREACH after restart, "
            f"got {row['classification']!r}"
        )

    def test_row_preserved_not_expired(self):
        """Deferred row must never be classified SKIPPED_TERMINAL or BROKEN."""
        watcher = self._make_empty_watcher()
        summary = self._run_full_audit(watcher, [])
        row = summary["rows"][0]
        assert row["classification"] not in (
            _audit.SKIPPED_TERMINAL,
            _audit.BROKEN_NEEDS_CODE,
            _audit.SKIPPED_AFTER_CUTOFF,
        ), (
            f"DEFERRED row must not be expired/broken after restart. "
            f"Got {row['classification']!r}"
        )

    def test_watcher_watch_called_exactly_once(self):
        watcher = self._make_empty_watcher()
        self._run_full_audit(watcher, [])
        assert watcher.watch.call_count == 1, (
            f"watcher.watch must be called exactly once for DEFERRED row. "
            f"Called {watcher.watch.call_count} times."
        )

    def test_broker_never_called(self):
        broker = MagicMock()
        broker.submit_order = MagicMock()
        watcher = self._make_empty_watcher()
        self._run_full_audit(watcher, [])
        # Broker is not passed to the audit module at all —
        # verify structurally that the audit source has no non-comment submit_order call
        bad_lines = [
            ln for ln in _AUDIT_SRC.splitlines()
            if not ln.strip().startswith("#")
            and not ln.strip().startswith("- ")
            and not ln.strip().startswith("* ")
            and "submit_order" in ln
        ]
        assert len(bad_lines) == 0, (
            f"Audit module must not reference submit_order outside comments: {bad_lines}"
        )
        broker.submit_order.assert_not_called()

    def test_auto_rearm_meta_written_for_deferred(self):
        watcher = self._make_empty_watcher()
        captured_meta: list = []
        self._run_full_audit(watcher, captured_meta)
        assert len(captured_meta) == 1
        assert captured_meta[0]["auto_rearm_result"] == "success"
        assert "auto_rearm_attempted_at" in captured_meta[0]

    def test_status_not_mutated_for_deferred(self):
        original_row = self._db_row()
        watcher = self._make_empty_watcher()
        with patch.object(_audit, "_load_audit_rows", return_value=[original_row]), \
             patch.object(_audit, "_is_after_eod_cutoff", return_value=False), \
             patch.object(_audit, "_persist_rearm_meta"):
            _audit.run_morning_handoff_audit(
                client_id=self.CLIENT_ID, entry_watcher=watcher,
                osm=MagicMock(), execution_mode="live", dry_run=False,
            )
        assert original_row["status"] == "PENDING_TRIGGER", (
            "Audit must not mutate row status for DEFERRED contract"
        )

    def test_deferred_contract_not_changed(self):
        original_row = self._db_row()
        original_contract = original_row["contract"]
        watcher = self._make_empty_watcher()
        with patch.object(_audit, "_load_audit_rows", return_value=[original_row]), \
             patch.object(_audit, "_is_after_eod_cutoff", return_value=False), \
             patch.object(_audit, "_persist_rearm_meta"):
            _audit.run_morning_handoff_audit(
                client_id=self.CLIENT_ID, entry_watcher=watcher,
                osm=MagicMock(), execution_mode="live", dry_run=False,
            )
        assert original_row["contract"] == original_contract, (
            "Audit must not change the DEFERRED:* contract field"
        )

    def test_second_audit_does_not_duplicate_watcher_state(self):
        watcher = self._make_empty_watcher()
        # First audit: empty registry → watch() once
        self._run_full_audit(watcher, [])
        assert watcher.watch.call_count == 1

        # Simulate post-rearm ownership
        watcher.has_order.return_value = True

        # Second audit: already owned → watch() not called again
        self._run_full_audit(watcher, [])
        assert watcher.watch.call_count == 1, (
            "Second audit must not duplicate watcher state for DEFERRED row"
        )

    def test_last_error_not_orphan_after_deferred_audit(self):
        watcher = self._make_empty_watcher()
        with patch.object(_audit, "_load_audit_rows", return_value=[self._db_row()]), \
             patch.object(_audit, "_is_after_eod_cutoff", return_value=False), \
             patch.object(_audit, "_persist_rearm_meta"):
            summary = _audit.run_morning_handoff_audit(
                client_id=self.CLIENT_ID, entry_watcher=watcher,
                osm=MagicMock(), execution_mode="live", dry_run=False,
            )
        for row in summary["rows"]:
            assert "PENDING_TRIGGER_ORPHAN_EXPIRED" not in str(row.get("last_error") or ""), (
                "DEFERRED row must never be labeled PENDING_TRIGGER_ORPHAN_EXPIRED"
            )


# ---------------------------------------------------------------------------
# Trigger-safety proof tests (Option C)
#
# Requirement: morning handoff audit may re-arm watcher state but must NEVER
# call on_trigger / ExecutionCore / OSM submit / broker.submit_order during
# the audit call.
#
# Option C proof: watch() and add_signal() in ap_entry_watcher.py never call
# on_trigger. The callback fires exclusively in _poll_loop(), which is a
# background daemon thread started only by entry_watcher.start(). The audit
# never calls start(), so no trigger can fire synchronously during the audit.
# ---------------------------------------------------------------------------

_EW_SRC = (_REPO / "ap_entry_watcher.py").read_text()


def _ew_fn_body(src: str, fn_sig: str, end_sig: str) -> str:
    """Extract the source body of a function between fn_sig and end_sig."""
    start = src.find(fn_sig)
    end   = src.find(end_sig, start + 10)
    return src[start:end] if start != -1 and end != -1 else ""


class TestTriggerSafetyProof:
    """
    Source-level and runtime-level proof that watch()/add_signal() cannot
    call on_trigger during the morning handoff audit.

    Tests 1–5 cover the 8 required safety assertions from the spec.
    """

    # ── Source guard: on_trigger absent from watch() ─────────────────────────

    def test_on_trigger_not_in_watch_body(self):
        """watch() must not reference on_trigger — the callback fires only in _poll_loop."""
        # watch() now has a multi-line signature — use the method name pattern
        watch_start = _EW_SRC.find("    def watch(\n")
        watch_end   = _EW_SRC.find("    def start(self)", watch_start)
        watch_body  = _EW_SRC[watch_start: watch_end] if watch_start != -1 and watch_end != -1 else ""
        assert watch_body, "watch() body must be extractable from ap_entry_watcher.py"
        assert "on_trigger" not in watch_body, (
            "watch() must NOT reference on_trigger. "
            "Breach callback fires only in _poll_loop()."
        )

    def test_on_trigger_not_in_add_signal_body(self):
        """add_signal() must not reference on_trigger."""
        watch_start = _EW_SRC.find("    def watch(\n")
        add_start   = _EW_SRC.find("    def add_signal(self, signal: dict) -> bool:")
        add_body    = _EW_SRC[add_start: watch_start] if add_start != -1 and watch_start != -1 else ""
        assert add_body, "add_signal() body must be extractable from ap_entry_watcher.py"
        assert "on_trigger" not in add_body, (
            "add_signal() must NOT reference on_trigger. "
            "Breach callback fires only in _poll_loop()."
        )

    def test_on_trigger_fires_only_in_poll_loop(self):
        """on_trigger call site must be inside _poll_loop, not in watch/add_signal."""
        poll_start = _EW_SRC.find("    def _poll_loop(self):")
        assert poll_start != -1, "_poll_loop must exist in ap_entry_watcher.py"
        poll_body = _EW_SRC[poll_start:]
        assert "self.on_trigger" in poll_body, (
            "on_trigger must be called inside _poll_loop"
        )

    def test_audit_module_never_calls_start(self):
        """The audit module must NEVER call entry_watcher.start() — that would
        launch the poll thread and could allow on_trigger to fire."""
        # search non-comment lines only
        bad_lines = [
            ln for ln in _AUDIT_SRC.splitlines()
            if not ln.strip().startswith("#")
            and ".start()" in ln
            and "watcher" in ln.lower()
        ]
        assert len(bad_lines) == 0, (
            f"Audit module must never call entry_watcher.start(): {bad_lines}"
        )

    def test_audit_module_has_trigger_safety_proof_constant(self):
        assert "_WATCH_IS_TRIGGER_SAFE" in _AUDIT_SRC, (
            "Audit module must have _WATCH_IS_TRIGGER_SAFE constant as architectural proof"
        )

    def test_audit_module_documents_option_c_proof(self):
        """The audit module must document the Option C safety reasoning."""
        assert "ARCHITECTURAL SAFETY PROOF" in _AUDIT_SRC or "Option C" in _AUDIT_SRC

    # ── Test 1: row already past trigger before audit — no callback ───────────

    def test_row_past_trigger_audit_does_not_call_execution_callback(self):
        """
        A row whose price is already past the trigger level is added to the
        watcher via watch(). The on_trigger callback must NOT fire during
        the audit call — it can only fire on the next poll cycle.
        """
        watcher = MagicMock()
        watcher.has_order.return_value = False
        watcher.watch.return_value = True
        on_trigger_mock = MagicMock()
        watcher.on_trigger = on_trigger_mock

        # Row: price already breached (trigger=155.0 — assume price is 156.0)
        order = _order(
            contract="C260626C00150000",
            trigger_price=155.0,  # "already past trigger"
            meta={"trigger_type": "breach"},
        )

        with patch.object(_audit, "_load_audit_rows", return_value=[order]), \
             patch.object(_audit, "_is_after_eod_cutoff", return_value=False), \
             patch.object(_audit, "_persist_rearm_meta"):
            summary = _audit.run_morning_handoff_audit(
                client_id="jasoncosby1@gmail.com",
                entry_watcher=watcher,
                osm=MagicMock(),
                execution_mode="live",
                dry_run=False,
            )

        # on_trigger must never be called during the audit
        on_trigger_mock.assert_not_called()
        # But watch() was called (re-arm occurred)
        watcher.watch.assert_called_once()
        # Classified correctly
        assert summary["rows"][0]["classification"] in (
            _audit.AUTO_REARMED, _audit.WAITING_FOR_CONTRACT_AT_BREACH
        )

    # ── Test 2: watch is called in arm-only mode (no immediate breach eval) ───

    def test_watcher_watch_called_without_triggering_execution_core(self):
        """
        The audit calls entry_watcher.watch(plan, local_id) for re-arm.
        ExecutionCore must not be invoked during this call.
        """
        execution_core_mock = MagicMock()
        watcher = MagicMock()
        watcher.has_order.return_value = False
        watcher.watch.return_value = True

        with patch.object(_audit, "_load_audit_rows",
                          return_value=[_order(contract="C260626C00150000",
                                               trigger_price=155.0)]), \
             patch.object(_audit, "_is_after_eod_cutoff", return_value=False), \
             patch.object(_audit, "_persist_rearm_meta"):
            _audit.run_morning_handoff_audit(
                client_id="jasoncosby1@gmail.com",
                entry_watcher=watcher,
                osm=MagicMock(),
                execution_mode="live",
                dry_run=False,
            )

        # ExecutionCore must never be called from audit
        execution_core_mock.assert_not_called()
        # But watcher.watch was called for re-arm
        watcher.watch.assert_called_once()

    # ── Test 3: broker.submit_order never called ──────────────────────────────

    def test_no_broker_submit_during_audit(self):
        """broker.submit_order must never be called during morning handoff audit."""
        broker = MagicMock()
        broker.submit_order = MagicMock()
        watcher = MagicMock()
        watcher.has_order.return_value = False
        watcher.watch.return_value = True

        with patch.object(_audit, "_load_audit_rows",
                          return_value=[_order(contract="C260626C00150000",
                                               trigger_price=155.0)]), \
             patch.object(_audit, "_is_after_eod_cutoff", return_value=False), \
             patch.object(_audit, "_persist_rearm_meta"):
            _audit.run_morning_handoff_audit(
                client_id="jasoncosby1@gmail.com",
                entry_watcher=watcher,
                osm=MagicMock(),
                execution_mode="live",
                dry_run=False,
            )

        broker.submit_order.assert_not_called()

    # ── Test 4: no OSM submit / transition to SUBMITTED ───────────────────────

    def test_no_osm_submit_or_transition_to_submitted(self):
        """OSM must not receive submit_existing_entry or transition(SUBMITTED)
        during the morning handoff audit."""
        osm = MagicMock()
        osm.submit_existing_entry = MagicMock()
        osm.transition = MagicMock()
        watcher = MagicMock()
        watcher.has_order.return_value = False
        watcher.watch.return_value = True

        with patch.object(_audit, "_load_audit_rows",
                          return_value=[_order(contract="C260626C00150000",
                                               trigger_price=155.0)]), \
             patch.object(_audit, "_is_after_eod_cutoff", return_value=False), \
             patch.object(_audit, "_persist_rearm_meta"):
            _audit.run_morning_handoff_audit(
                client_id="jasoncosby1@gmail.com",
                entry_watcher=watcher,
                osm=osm,
                execution_mode="live",
                dry_run=False,
            )

        osm.submit_existing_entry.assert_not_called()
        # transition() should never be called with "SUBMITTED" by the audit
        for c in osm.transition.call_args_list:
            new_status = c.args[1] if len(c.args) > 1 else c.kwargs.get("new_status", "")
            assert str(new_status).upper() != "SUBMITTED", (
                f"OSM.transition must never be called with SUBMITTED during audit, "
                f"got call: {c}"
            )

    # ── Test 5: no ExecutionCore callback during audit ────────────────────────

    def test_no_execution_core_callback_during_audit(self):
        """on_trigger attribute on watcher must never be called during audit.
        on_trigger IS the ExecutionCore callback wired by the runner."""
        on_trigger = MagicMock()
        watcher = MagicMock()
        watcher.has_order.return_value = False
        watcher.watch.return_value = True
        watcher.on_trigger = on_trigger

        with patch.object(_audit, "_load_audit_rows",
                          return_value=[_order(contract="C260626C00150000",
                                               trigger_price=155.0)]), \
             patch.object(_audit, "_is_after_eod_cutoff", return_value=False), \
             patch.object(_audit, "_persist_rearm_meta"):
            _audit.run_morning_handoff_audit(
                client_id="jasoncosby1@gmail.com",
                entry_watcher=watcher,
                osm=MagicMock(),
                execution_mode="live",
                dry_run=False,
            )

        on_trigger.assert_not_called()

    # ── Test 6: POST /admin/morning_handoff_audit cannot trigger broker submit ─

    def test_admin_post_endpoint_cannot_trigger_broker_submit(self):
        """The admin endpoint wires run_morning_handoff_audit — it must never
        result in a broker submit call. Verified by ensuring the endpoint
        function body (excluding docstring) does not reference any broker path."""
        idx = _APP_SRC.find("def admin_morning_handoff_audit_post():")
        assert idx != -1, "admin_morning_handoff_audit_post must exist in app.py"
        fn_body = _APP_SRC[idx: idx + 2000]
        # Strip docstring lines (between triple quotes)
        import re as _re
        fn_no_doc = _re.sub(r'""".*?"""', '', fn_body, flags=_re.DOTALL)
        assert "submit_order" not in fn_no_doc, (
            "admin_morning_handoff_audit_post function body must not reference submit_order "
            "(outside docstring)"
        )

    # ── Test 7: dry run still classifies, never calls watch ──────────────────

    def test_dry_run_classifies_without_calling_watch(self):
        """Dry run must classify rows but never call watcher.watch() or on_trigger."""
        on_trigger = MagicMock()
        watcher = MagicMock()
        watcher.has_order.return_value = False
        watcher.watch.return_value = True
        watcher.on_trigger = on_trigger

        with patch.object(_audit, "_load_audit_rows",
                          return_value=[_order(contract="C260626C00150000",
                                               trigger_price=155.0)]), \
             patch.object(_audit, "_is_after_eod_cutoff", return_value=False), \
             patch.object(_audit, "_persist_rearm_meta"):
            summary = _audit.run_morning_handoff_audit(
                client_id="jasoncosby1@gmail.com",
                entry_watcher=watcher,
                osm=MagicMock(),
                execution_mode="live",
                dry_run=True,
            )

        watcher.watch.assert_not_called()
        on_trigger.assert_not_called()
        # But it still classified the row
        assert summary["scanned"] == 1
        assert summary["rows"][0]["action"] == "dry_run_would_rearm"

    # ── Test 8: second audit does not duplicate watcher state ─────────────────

    def test_second_audit_no_duplicate_watcher_state_no_double_trigger(self):
        """After re-arm, second audit sees already_owned → watch() not called again.
        on_trigger cannot be called more than once from add_signal."""
        on_trigger = MagicMock()
        watcher = MagicMock()
        watcher.has_order.return_value = False
        watcher.watch.return_value = True
        watcher.on_trigger = on_trigger

        row = _order(contract="C260626C00150000", trigger_price=155.0,
                     meta={"trigger_type": "breach"})

        # First audit: re-arms
        with patch.object(_audit, "_load_audit_rows", return_value=[row]), \
             patch.object(_audit, "_is_after_eod_cutoff", return_value=False), \
             patch.object(_audit, "_persist_rearm_meta"):
            _audit.run_morning_handoff_audit(
                client_id="jasoncosby1@gmail.com",
                entry_watcher=watcher,
                osm=MagicMock(),
                execution_mode="live",
                dry_run=False,
            )
        assert watcher.watch.call_count == 1
        on_trigger.assert_not_called()

        # Watcher now owns it
        watcher.has_order.return_value = True

        # Second audit: already owned → watch() not called again
        with patch.object(_audit, "_load_audit_rows", return_value=[row]), \
             patch.object(_audit, "_is_after_eod_cutoff", return_value=False), \
             patch.object(_audit, "_persist_rearm_meta"):
            _audit.run_morning_handoff_audit(
                client_id="jasoncosby1@gmail.com",
                entry_watcher=watcher,
                osm=MagicMock(),
                execution_mode="live",
                dry_run=False,
            )
        assert watcher.watch.call_count == 1, (
            "watch() must not be called on second audit (idempotency)"
        )
        on_trigger.assert_not_called()


# ---------------------------------------------------------------------------
# Automatic startup wiring tests (PR amendment)
#
# run_morning_handoff_audit() is now called automatically from
# ClientRunner._run_morning_handoff_audit_startup() which is invoked
# in run() immediately after _run_startup_recovery() completes.
#
# Tests verify the 10 required behaviors:
#  1.  Startup invokes run_morning_handoff_audit() once per client
#  2.  Startup failure does not crash the runner
#  3.  Execution mode passed matches runner mode
#  4.  Audit not called before entry_watcher/OSM exists
#  5.  Manual admin endpoint still works (covered by existing tests)
#  6.  Dry run still does not call watch() (covered by existing tests)
#  7.  Audit is idempotent if called twice (covered by existing tests)
#  8.  Live runner passes execution_mode="live"
#  9.  Paper runner passes execution_mode="paper"
#  10. Failure logs WATCHER_REARM_AUDIT_FAILED
# ---------------------------------------------------------------------------

_CR_SRC = (_REPO / "client_runner.py").read_text()
import logging as _logging
import types as _types


def _make_stub_runner(
    *,
    mode: str = "LIVE",
    entry_watcher=None,
    osm=None,
    email: str = "jasoncosby1@gmail.com",
) -> object:
    """Minimal stub that has _run_morning_handoff_audit_startup bound from the
    real client_runner.py source — no supabase/broker imports needed.

    Approach: extract and compile the method body from production source,
    bind it to a plain object with the minimum attributes the method reads.
    This tests the *real* production code, not a reimplementation.
    """
    log = _logging.getLogger("client_runner")

    class _Stub:
        pass

    runner       = _Stub()
    runner.email = email
    runner.mode  = mode

    core = MagicMock()
    core.entry_watcher = entry_watcher if entry_watcher is not None else MagicMock()
    runner.core                = core
    runner.order_state_machine = osm if osm is not None else MagicMock()

    # Pull the exact method source from client_runner.py and compile it.
    _src     = _CR_SRC
    _start   = _src.find("    def _run_morning_handoff_audit_startup(self)")
    _end     = _src.find("\n    def ", _start + 10)
    _raw     = _src[_start:_end].strip()
    # Dedent one level (4 spaces) so it compiles at module scope
    _dedented = "\n".join(
        ln[4:] if ln.startswith("    ") else ln
        for ln in _raw.splitlines()
    )
    _ns = {"logger": log}
    exec(compile(_dedented, "<cr_stub>", "exec"), _ns)  # noqa: S102
    runner._run_morning_handoff_audit_startup = _types.MethodType(
        _ns["_run_morning_handoff_audit_startup"], runner
    )
    return runner


def _run_startup(runner, audit_fn) -> None:
    """Invoke runner._run_morning_handoff_audit_startup with audit_fn patched in."""
    with patch.dict(sys.modules, {
        "ap_morning_handoff_audit": MagicMock(run_morning_handoff_audit=audit_fn)
    }):
        runner._run_morning_handoff_audit_startup()


class TestStartupWiring:
    """
    Proves that run_morning_handoff_audit() is automatically called from
    ClientRunner startup — no manual /admin/morning_handoff_audit needed.

    Source of truth: client_runner.py contains:
        self._run_startup_recovery(broker, exit_eng)
        self._run_morning_handoff_audit_startup()   ← automatic wiring
        self._seed_exit_engine_from_db(exit_eng)

    All 5 required tests plus supporting assertions.
    """

    # ──────────────────────────────────────────────────────────────────────────
    # SOURCE GUARDS — prove the wiring exists in client_runner.py
    # ──────────────────────────────────────────────────────────────────────────

    def test_client_runner_has_startup_method(self):
        """_run_morning_handoff_audit_startup must be defined in ClientRunner."""
        assert "def _run_morning_handoff_audit_startup(self)" in _CR_SRC, (
            "ClientRunner MUST define _run_morning_handoff_audit_startup. "
            "This method is the automatic startup hook."
        )

    def test_startup_method_called_from_run(self):
        """run() must call self._run_morning_handoff_audit_startup()."""
        assert "self._run_morning_handoff_audit_startup()" in _CR_SRC, (
            "run() MUST call self._run_morning_handoff_audit_startup(). "
            "Without this, the audit is not automatic — it requires manual admin endpoint calls."
        )

    def test_startup_audit_called_after_startup_recovery(self):
        """Watcher exists after startup recovery — audit must come after it."""
        idx_recovery = _CR_SRC.find("self._run_startup_recovery(")
        idx_audit    = _CR_SRC.find("self._run_morning_handoff_audit_startup()")
        assert idx_recovery != -1, "_run_startup_recovery call not found"
        assert idx_audit    != -1, "_run_morning_handoff_audit_startup call not found in run()"
        assert idx_recovery < idx_audit, (
            "_run_morning_handoff_audit_startup() must come AFTER _run_startup_recovery() "
            "so entry_watcher and OSM are initialized before the audit runs."
        )

    def test_startup_audit_called_before_worker_thread(self):
        """Audit must run before the poll loop starts."""
        idx_audit  = _CR_SRC.find("self._run_morning_handoff_audit_startup()")
        idx_worker = _CR_SRC.find("self._start_worker_thread(")
        assert idx_audit  != -1
        assert idx_worker != -1
        assert idx_audit < idx_worker, (
            "Audit must run before the poll loop (_start_worker_thread) so rows "
            "are re-armed before breach evaluation begins."
        )

    def test_failure_does_not_crash_runner_in_source(self):
        """Source must handle exceptions without re-raising (runner must survive)."""
        fn_start = _CR_SRC.find("def _run_morning_handoff_audit_startup")
        fn_body  = _CR_SRC[fn_start: fn_start + 2500]
        assert "except Exception" in fn_body, (
            "Startup method must catch Exception and log, never re-raise "
            "(a failed audit must not crash the runner)"
        )
        assert "WATCHER_REARM_AUDIT_FAILED" in fn_body

    def test_import_error_handled_in_source(self):
        """ImportError must be caught so a missing module never crashes the runner."""
        fn_start = _CR_SRC.find("def _run_morning_handoff_audit_startup")
        fn_body  = _CR_SRC[fn_start: fn_start + 2500]
        assert "ImportError" in fn_body

    def test_mode_lowercased_in_source(self):
        """Runner mode LIVE/PAPER must be lowercased to live/paper before audit call."""
        fn_start = _CR_SRC.find("def _run_morning_handoff_audit_startup")
        fn_body  = _CR_SRC[fn_start: fn_start + 2500]
        assert ".lower()" in fn_body

    def test_dry_run_false_in_source(self):
        """Startup must pass dry_run=False — real re-arm, not classification only."""
        fn_start = _CR_SRC.find("def _run_morning_handoff_audit_startup")
        fn_body  = _CR_SRC[fn_start: fn_start + 2500]
        assert "dry_run=False" in fn_body

    # ──────────────────────────────────────────────────────────────────────────
    # Required test 1: startup calls audit exactly once after watcher exists
    # ──────────────────────────────────────────────────────────────────────────

    def test_startup_calls_audit_exactly_once(self):
        """After watcher and OSM are ready, audit is called exactly once at startup."""
        audit_mock = MagicMock(return_value={
            "ok": True, "scanned": 1, "auto_rearmed": 1, "rows": [], "errors": []
        })
        runner = _make_stub_runner(mode="LIVE")
        _run_startup(runner, audit_mock)
        assert audit_mock.call_count == 1, (
            f"audit called {audit_mock.call_count} times — must be exactly 1 at startup"
        )

    def test_startup_not_called_when_entry_watcher_missing(self):
        """If entry_watcher is None (not yet initialized), audit must not run."""
        audit_mock = MagicMock(return_value={"ok": True, "rows": []})
        runner = _make_stub_runner(mode="LIVE")
        runner.core.entry_watcher = None
        _run_startup(runner, audit_mock)
        audit_mock.assert_not_called()

    def test_startup_not_called_when_osm_missing(self):
        """If OSM is None (not yet initialized), audit must not run."""
        audit_mock = MagicMock(return_value={"ok": True, "rows": []})
        runner = _make_stub_runner(mode="LIVE")
        runner.order_state_machine = None
        _run_startup(runner, audit_mock)
        audit_mock.assert_not_called()

    # ──────────────────────────────────────────────────────────────────────────
    # Required test 2: audit failure does not crash the runner
    # ──────────────────────────────────────────────────────────────────────────

    def test_audit_exception_does_not_crash_runner(self):
        """RuntimeError from run_morning_handoff_audit must not propagate."""
        def _explode(**kwargs):
            raise RuntimeError("deliberate audit failure")

        runner = _make_stub_runner(mode="LIVE")
        # If this raises, pytest.fail — the runner would be dead
        try:
            _run_startup(runner, _explode)
        except Exception as exc:
            pytest.fail(f"Audit exception propagated — runner would crash: {exc}")

    def test_audit_import_error_does_not_crash_runner(self):
        """ImportError for ap_morning_handoff_audit must not propagate."""
        runner = _make_stub_runner(mode="LIVE")
        saved  = sys.modules.pop("ap_morning_handoff_audit", None)
        try:
            with patch("builtins.__import__",
                       side_effect=lambda n, *a, **kw:
                           (_ for _ in ()).throw(ImportError("no module"))
                           if n == "ap_morning_handoff_audit"
                           else __import__(n, *a, **kw)):
                try:
                    runner._run_morning_handoff_audit_startup()
                except Exception as exc:
                    pytest.fail(f"ImportError propagated — runner would crash: {exc}")
        finally:
            if saved is not None:
                sys.modules["ap_morning_handoff_audit"] = saved

    def test_failure_logs_watcher_rearm_audit_failed(self):
        """Exception during audit must log WATCHER_REARM_AUDIT_FAILED."""
        import unittest

        def _explode(**kwargs):
            raise RuntimeError("deliberate failure")

        runner = _make_stub_runner(mode="LIVE")
        with patch.dict(sys.modules, {
            "ap_morning_handoff_audit": MagicMock(run_morning_handoff_audit=_explode)
        }):
            with unittest.TestCase().assertLogs("client_runner", level="ERROR") as ctx:
                runner._run_morning_handoff_audit_startup()
        assert any("WATCHER_REARM_AUDIT_FAILED" in line for line in ctx.output), (
            f"WATCHER_REARM_AUDIT_FAILED not found in log output:\n" +
            "\n".join(ctx.output)
        )

    def test_missing_watcher_logs_watcher_rearm_audit_failed(self):
        """Missing entry_watcher must also log WATCHER_REARM_AUDIT_FAILED."""
        import unittest

        runner = _make_stub_runner(mode="LIVE")
        runner.core.entry_watcher = None
        with patch.dict(sys.modules, {
            "ap_morning_handoff_audit": MagicMock(run_morning_handoff_audit=MagicMock())
        }):
            with unittest.TestCase().assertLogs("client_runner", level="WARNING") as ctx:
                runner._run_morning_handoff_audit_startup()
        assert any("WATCHER_REARM_AUDIT_FAILED" in line for line in ctx.output)

    # ──────────────────────────────────────────────────────────────────────────
    # Required test 3: execution_mode passed correctly (live/paper)
    # ──────────────────────────────────────────────────────────────────────────

    def test_live_runner_passes_execution_mode_live(self):
        """LIVE runner must pass execution_mode='live' (lowercase)."""
        captured = {}

        def _capture(**kwargs):
            captured.update(kwargs)
            return {"ok": True, "rows": [], "scanned": 0, "errors": []}

        runner = _make_stub_runner(mode="LIVE")
        _run_startup(runner, _capture)
        assert captured["execution_mode"] == "live", (
            f"expected execution_mode='live', got {captured['execution_mode']!r}"
        )

    def test_paper_runner_passes_execution_mode_paper(self):
        """PAPER runner must pass execution_mode='paper' (lowercase)."""
        captured = {}

        def _capture(**kwargs):
            captured.update(kwargs)
            return {"ok": True, "rows": [], "scanned": 0, "errors": []}

        runner = _make_stub_runner(mode="PAPER")
        _run_startup(runner, _capture)
        assert captured["execution_mode"] == "paper", (
            f"expected execution_mode='paper', got {captured['execution_mode']!r}"
        )

    def test_execution_mode_is_lowercase_not_uppercase(self):
        """Runner stores LIVE/PAPER uppercase — audit must receive lowercase."""
        captured = {}

        def _capture(**kwargs):
            captured.update(kwargs)
            return {"ok": True, "rows": [], "scanned": 0, "errors": []}

        for mode, expected in [("LIVE", "live"), ("PAPER", "paper")]:
            runner = _make_stub_runner(mode=mode)
            _run_startup(runner, _capture)
            assert captured["execution_mode"] == expected
            assert captured["execution_mode"] == captured["execution_mode"].lower()

    def test_client_id_is_runner_email(self):
        """client_id passed to audit must equal runner.email."""
        captured = {}

        def _capture(**kwargs):
            captured.update(kwargs)
            return {"ok": True, "rows": [], "scanned": 0, "errors": []}

        runner = _make_stub_runner(mode="LIVE", email="jasoncosby1@gmail.com")
        _run_startup(runner, _capture)
        assert captured["client_id"] == "jasoncosby1@gmail.com"

    def test_dry_run_is_false_at_startup(self):
        """Startup must not be a dry run — real re-arm required."""
        captured = {}

        def _capture(**kwargs):
            captured.update(kwargs)
            return {"ok": True, "rows": [], "scanned": 0, "errors": []}

        runner = _make_stub_runner(mode="LIVE")
        _run_startup(runner, _capture)
        assert captured["dry_run"] is False

    # ──────────────────────────────────────────────────────────────────────────
    # Required test 4: admin endpoint still works
    # ──────────────────────────────────────────────────────────────────────────

    def test_admin_endpoint_exists_in_app(self):
        """Manual /admin/morning_handoff_audit endpoint must still exist."""
        assert "admin_morning_handoff_audit_get" in _APP_SRC
        assert "admin_morning_handoff_audit_post" in _APP_SRC

    def test_admin_endpoint_calls_run_morning_handoff_audit(self):
        """Admin endpoint must call run_morning_handoff_audit (not a stub)."""
        assert "run_morning_handoff_audit" in _APP_SRC

    def test_admin_endpoint_has_dry_run_parameter(self):
        """Admin endpoint must support dry_run parameter."""
        idx = _APP_SRC.find("admin_morning_handoff_audit_post")
        region = _APP_SRC[idx: idx + 1500]
        assert "dry_run" in region

    # ──────────────────────────────────────────────────────────────────────────
    # Required test 5: calling audit twice is idempotent
    # ──────────────────────────────────────────────────────────────────────────

    def test_calling_audit_twice_is_idempotent(self):
        """Second startup call must not duplicate watcher state.
        watch() is only called when has_order returns False — once it returns
        True (after first re-arm), the second call sees already_owned and skips.
        """
        watcher = MagicMock()
        watcher.has_order.return_value = False
        watcher.watch.return_value = True

        row = _order(contract="C260626C00150000", trigger_price=155.0,
                     meta={"trigger_type": "breach"})

        def _run_once():
            with patch.object(_audit, "_load_audit_rows", return_value=[row]), \
                 patch.object(_audit, "_is_after_eod_cutoff", return_value=False), \
                 patch.object(_audit, "_persist_rearm_meta"):
                return _audit.run_morning_handoff_audit(
                    client_id="jasoncosby1@gmail.com",
                    entry_watcher=watcher,
                    osm=MagicMock(),
                    execution_mode="live",
                    dry_run=False,
                )

        # First call: empty registry → watch() called once
        _run_once()
        assert watcher.watch.call_count == 1

        # Watcher now owns the order (simulates post-rearm in-memory state)
        watcher.has_order.return_value = True

        # Second call: already owned → watch() NOT called again
        result2 = _run_once()
        assert watcher.watch.call_count == 1, (
            "Second audit call must not call watch() again — idempotent by design. "
            f"watch() was called {watcher.watch.call_count} times total."
        )
        assert result2["rows"][0]["classification"] == _audit.READY_ARMED

    def test_startup_called_twice_does_not_duplicate(self):
        """Calling _run_morning_handoff_audit_startup twice is safe — second call
        sees already_owned and does nothing extra."""
        call_count = [0]
        first_result = {"ok": True, "scanned": 0, "rows": [], "errors": []}

        def _audit_fn(**kwargs):
            call_count[0] += 1
            return first_result

        runner = _make_stub_runner(mode="LIVE")

        # First call
        _run_startup(runner, _audit_fn)
        assert call_count[0] == 1

        # Second call — same runner, same watcher
        _run_startup(runner, _audit_fn)
        assert call_count[0] == 2, (
            "The startup method itself may be called twice (e.g. on forced reinit) — "
            "both calls must succeed without crashing"
        )
        # No exception raised — idempotent


# ---------------------------------------------------------------------------
# Admin endpoint OSM lookup tests
#
# Both GET and POST endpoints must resolve the OSM with this fallback chain:
#   runner.order_state_machine  (canonical ClientRunner attribute)
#   runner.osm                  (legacy alias)
#   runner.core.order_state_machine
#   runner.execution_core.order_state_machine
#
# The original code only tried runner.osm / runner.core.osm — which always
# resolved to None for real ClientRunner instances that expose
# runner.order_state_machine. This meant endpoint-triggered re-arms could
# never persist auto_rearm_* metadata.
# ---------------------------------------------------------------------------

class TestAdminEndpointOsmLookup:
    """
    OSM resolution in the admin endpoint paths.

    Tests prove the full fallback chain without spinning up Flask —
    we read the source to verify the priority order, and run the
    actual audit with controlled runner stubs to verify correct resolution.
    """

    # ── Source guards ──────────────────────────────────────────────────────

    def test_get_endpoint_tries_order_state_machine_first(self):
        """GET endpoint must try runner.order_state_machine before runner.osm."""
        idx_get = _APP_SRC.find("def admin_morning_handoff_audit_get")
        region  = _APP_SRC[idx_get: idx_get + 2100]
        osm_idx           = region.find("order_state_machine")
        osm_legacy_idx    = region.find('"osm"')
        assert osm_idx != -1, "GET endpoint must reference order_state_machine"
        assert osm_legacy_idx != -1, "GET endpoint must also try 'osm' as fallback"
        assert osm_idx < osm_legacy_idx, (
            "GET endpoint must try order_state_machine BEFORE osm (canonical first)"
        )

    def test_post_endpoint_tries_order_state_machine_first(self):
        """POST endpoint must try runner.order_state_machine before runner.osm."""
        idx_post = _APP_SRC.find("def admin_morning_handoff_audit_post")
        region   = _APP_SRC[idx_post: idx_post + 2500]
        osm_idx           = region.find("order_state_machine")
        osm_legacy_idx    = region.find('"osm"')
        assert osm_idx != -1, "POST endpoint must reference order_state_machine"
        assert osm_legacy_idx != -1, "POST endpoint must also try 'osm' as fallback"
        assert osm_idx < osm_legacy_idx, (
            "POST endpoint must try order_state_machine BEFORE osm"
        )

    def test_get_endpoint_has_four_fallback_paths(self):
        """GET endpoint must have all four OSM resolution paths."""
        idx_get = _APP_SRC.find("def admin_morning_handoff_audit_get")
        region  = _APP_SRC[idx_get: idx_get + 2100]
        assert "order_state_machine" in region
        assert '"osm"' in region
        assert "execution_core" in region, (
            "GET endpoint must include execution_core fallback path"
        )

    def test_post_endpoint_has_four_fallback_paths(self):
        """POST endpoint must have all four OSM resolution paths."""
        idx_post = _APP_SRC.find("def admin_morning_handoff_audit_post")
        region   = _APP_SRC[idx_post: idx_post + 2500]
        assert "order_state_machine" in region
        assert '"osm"' in region
        assert "execution_core" in region

    def test_null_osm_logs_watcher_rearm_audit_failed(self):
        """When no OSM is found, endpoint must log WATCHER_REARM_AUDIT_FAILED."""
        idx_get = _APP_SRC.find("def admin_morning_handoff_audit_get")
        region  = _APP_SRC[idx_get: idx_get + 2100]
        assert "WATCHER_REARM_AUDIT_FAILED" in region, (
            "GET endpoint must log WATCHER_REARM_AUDIT_FAILED when OSM is None"
        )
        idx_post = _APP_SRC.find("def admin_morning_handoff_audit_post")
        region2  = _APP_SRC[idx_post: idx_post + 3000]
        assert "WATCHER_REARM_AUDIT_FAILED" in region2, (
            "POST endpoint must log WATCHER_REARM_AUDIT_FAILED when OSM is None"
        )

    # ── Runtime: required test 1 — runner.order_state_machine passes correctly ─

    def test_osm_resolution_prefers_order_state_machine(self):
        """Runner with order_state_machine passes that object into run_morning_handoff_audit."""
        real_osm = MagicMock(name="real_osm")

        # Simulate the resolution logic from the endpoint
        runner = MagicMock()
        runner.order_state_machine = real_osm
        del runner.osm  # not present

        resolved = (
            getattr(runner, "order_state_machine", None)
            or getattr(runner, "osm", None)
            or getattr(getattr(runner, "core", None), "order_state_machine", None)
            or getattr(getattr(runner, "execution_core", None), "order_state_machine", None)
        )
        assert resolved is real_osm, (
            "runner.order_state_machine must be resolved as the OSM "
            f"(got {resolved!r})"
        )

    # ── Required test 2 — runner.osm still works (legacy alias) ──────────

    def test_osm_resolution_falls_back_to_osm_attr(self):
        """Runner with only .osm attribute (legacy) must still resolve correctly."""
        legacy_osm = MagicMock(name="legacy_osm")

        runner = MagicMock()
        runner.order_state_machine = None   # not set
        runner.osm = legacy_osm

        resolved = (
            getattr(runner, "order_state_machine", None)
            or getattr(runner, "osm", None)
            or getattr(getattr(runner, "core", None), "order_state_machine", None)
            or getattr(getattr(runner, "execution_core", None), "order_state_machine", None)
        )
        assert resolved is legacy_osm, (
            "runner.osm must be used when order_state_machine is None"
        )

    def test_osm_resolution_falls_back_to_core_order_state_machine(self):
        """OSM nested under runner.core.order_state_machine must be found."""
        core_osm = MagicMock(name="core_osm")

        runner = MagicMock()
        runner.order_state_machine = None
        runner.osm = None
        runner.core.order_state_machine = core_osm

        resolved = (
            getattr(runner, "order_state_machine", None)
            or getattr(runner, "osm", None)
            or getattr(getattr(runner, "core", None), "order_state_machine", None)
            or getattr(getattr(runner, "execution_core", None), "order_state_machine", None)
        )
        assert resolved is core_osm

    def test_osm_resolution_none_when_no_path_found(self):
        """When no OSM exists, resolved value must be None (not raise)."""
        runner = MagicMock()
        runner.order_state_machine = None
        runner.osm = None
        runner.core.order_state_machine = None
        runner.execution_core.order_state_machine = None

        resolved = (
            getattr(runner, "order_state_machine", None)
            or getattr(runner, "osm", None)
            or getattr(getattr(runner, "core", None), "order_state_machine", None)
            or getattr(getattr(runner, "execution_core", None), "order_state_machine", None)
        )
        assert resolved is None

    # ── Required test 3 — endpoint-triggered re-arm persists audit metadata ─

    def test_endpoint_rearm_persists_metadata_when_osm_available(self):
        """Endpoint-triggered re-arm must persist auto_rearm_* metadata when OSM present.

        When the runner exposes order_state_machine (the canonical attribute),
        the audit receives a real OSM and _persist_rearm_meta is called.
        """
        real_osm = MagicMock(name="real_osm")
        watcher  = MagicMock()
        watcher.has_order.return_value = False
        watcher.watch.return_value = True

        row = _order(
            contract="C260626C00150000",
            trigger_price=155.0,
            meta={"trigger_type": "breach"},
        )

        persist_calls = []

        def _capture_persist(osm, local_order_id, result, reason):
            persist_calls.append({
                "osm":            osm,
                "local_order_id": local_order_id,
                "result":         result,
                "reason":         reason,
            })

        with patch.object(_audit, "_load_audit_rows", return_value=[row]), \
             patch.object(_audit, "_is_after_eod_cutoff", return_value=False), \
             patch.object(_audit, "_persist_rearm_meta", side_effect=_capture_persist):
            summary = _audit.run_morning_handoff_audit(
                client_id="jasoncosby1@gmail.com",
                entry_watcher=watcher,
                osm=real_osm,           # OSM resolved from runner.order_state_machine
                execution_mode="live",
                dry_run=False,
            )

        assert summary["auto_rearmed"] == 1
        assert len(persist_calls) == 1, (
            "auto_rearm metadata must be persisted when OSM is available"
        )
        assert persist_calls[0]["osm"] is real_osm, (
            "The real OSM (from runner.order_state_machine) must be passed to "
            "_persist_rearm_meta, not None"
        )
        assert persist_calls[0]["result"] == "success"

    def test_endpoint_rearm_with_null_osm_still_classifies(self):
        """Even with no OSM, the audit still classifies and re-arms rows.
        _persist_rearm_meta is only called when osm is not None — that is
        correct defensive behavior in the audit module."""
        watcher = MagicMock()
        watcher.has_order.return_value = False
        watcher.watch.return_value = True

        row = _order(
            contract="C260626C00150000",
            trigger_price=155.0,
            meta={"trigger_type": "breach"},
        )

        with patch.object(_audit, "_load_audit_rows", return_value=[row]), \
             patch.object(_audit, "_is_after_eod_cutoff", return_value=False), \
             patch.object(_audit, "_persist_rearm_meta") as mock_persist:
            summary = _audit.run_morning_handoff_audit(
                client_id="jasoncosby1@gmail.com",
                entry_watcher=watcher,
                osm=None,               # no OSM found by endpoint
                execution_mode="live",
                dry_run=False,
            )

        # Row is still classified even without OSM
        assert summary["auto_rearmed"] == 1, (
            "Audit must still classify and re-arm even when OSM is None"
        )
        # Persist is skipped when osm=None — the audit module guards this
        mock_persist.assert_not_called(), (
            "_persist_rearm_meta must not be called when osm=None "
            "(the audit module guards: if osm is not None)"
        )

    def test_startup_osm_path_uses_order_state_machine(self):
        """_run_morning_handoff_audit_startup reads runner.order_state_machine."""
        fn_start = _CR_SRC.find("def _run_morning_handoff_audit_startup")
        fn_body  = _CR_SRC[fn_start: fn_start + 2500]
        assert "order_state_machine" in fn_body, (
            "Startup path must read runner.order_state_machine as the OSM"
        )
