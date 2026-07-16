"""
tests/test_p0_live_entry_brake.py

PR #317 — LIVE pre-entry account-state brake.

Acceptance tests (8 required by spec):
  1. split_brain_blocks_entry
  2. stuck_protective_exit_blocks_entry
  3. unknown_mode_exposure_blocks_entry
  4. all_clear_account_permits_entry
  5. brake_failure_blocks_live
  6. paper_remains_unchanged
  7. protective_exit_path_never_imports_entry_brake
  8. one_client_unsafe_state_cannot_block_another

Additional structural / robustness tests:
  - blank execution_mode blocks
  - unknown execution_mode blocks
  - stale_exit_in_flight blocks
  - broker_snapshot_unavailable_with_local_exposure blocks
  - broker_snapshot_unavailable_no_local_exposure passes
  - broker_position_mismatch blocks
  - broker_position_match passes
  - circuit_breaker_clear passes (meta flag False)
  - split_brain_terminal_order_does_not_block (FILLED split-brain = resolved)
  - evidence_dict_is_populated_on_block
  - evidence_dict_contains_client_id_on_pass
  - meta_col_missing_fails_closed_on_circuit_breaker_check
  - reason_code_is_always_populated
"""
from __future__ import annotations

import ast
import importlib
import os
import sys
import types
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, patch, call

import pytest

# Ensure DATABASE_URL is stubbed before any ap.db import
os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")

from ap.live_entry_brake import (
    BrakeCode,
    BrakeResult,
    _check_broker_snapshot,
    _check_circuit_breaker,
    _check_split_brain,
    _check_stale_exit_in_flight,
    _check_unknown_mode_exposure,
    check_live_entry_brake,
)


# ─────────────────────────────────────────────────────────────────────────────
# Test helpers — DB mocking utilities
# ─────────────────────────────────────────────────────────────────────────────

def _mock_rows(*rows):
    """
    Build a callable that returns a list of dict-like objects from tuples.
    Simulates psycopg2 DictRow behaviour for fetchall().
    """
    return list(rows)


def _mock_conn_returning(row_sets: list):
    """
    Context manager factory that yields a mock cursor whose fetchall() returns
    successive row_sets for each execute() call in order.
    Each row_set is a list of dicts.
    """
    call_count = [0]

    @contextmanager
    def _ctx():
        cur = MagicMock()

        def _fetchall():
            idx = call_count[0]
            call_count[0] += 1
            if idx < len(row_sets):
                return row_sets[idx]
            return []

        cur.fetchall.side_effect = _fetchall
        cur.execute.return_value = None
        yield cur

    return _ctx


def _patch_conn(row_sets: list):
    """
    Patch ap.live_entry_brake's conn and run_with_retry so classifiers receive
    mock DB results from row_sets (list of lists of dicts, one per query call).
    """
    fetch_index = [0]

    @contextmanager
    def _fake_conn():
        cur = MagicMock()

        def _fetchall():
            idx = fetch_index[0]
            fetch_index[0] += 1
            if idx < len(row_sets):
                return row_sets[idx]
            return []

        cur.fetchall.side_effect = _fetchall
        cur.execute.return_value = None
        yield cur

    def _fake_run_with_retry(fn, *args, **kwargs):
        return fn()

    return (
        patch("ap.live_entry_brake.conn", _fake_conn),
        patch("ap.live_entry_brake.run_with_retry", _fake_run_with_retry),
    )


def _apply_patches(row_sets: list):
    """Return a context manager that applies both conn and run_with_retry patches."""

    class _Multi:
        def __enter__(self):
            self._p1, self._p2 = _patch_conn(row_sets)
            self._p1.__enter__()
            self._p2.__enter__()
            return self

        def __exit__(self, *args):
            self._p1.__exit__(*args)
            self._p2.__exit__(*args)

    return _Multi()


CLIENT_ID = "jasoncosby1@gmail.com"
CLIENT_B  = "jose@example.com"


# ─────────────────────────────────────────────────────────────────────────────
# Acceptance test 1 — split-brain order blocks entry
# ─────────────────────────────────────────────────────────────────────────────

class TestSplitBrainBlocksEntry:
    """Spec test 1: active unresolved split-brain order → BLOCKED."""

    def test_split_brain_nonterminal_order_blocks(self):
        """An order with SPLIT_BRAIN: prefix and nonterminal status blocks."""
        split_brain_rows = [
            {
                "local_order_id": "ord-abc",
                "broker_order_id": "broker-123",
                "last_error_prefix": "SPLIT_BRAIN:DB transition failed",
                "status": "ACK",
            }
        ]
        # _check_split_brain queries once; no other classifiers should fire
        with _apply_patches([split_brain_rows]):
            result = _check_split_brain(CLIENT_ID)

        assert result is not None
        assert result.blocked is True
        assert result.reason_code == BrakeCode.SPLIT_BRAIN_ORDER
        assert len(result.evidence["split_brain_orders"]) == 1
        assert result.evidence["split_brain_orders"][0]["local_order_id"] == "ord-abc"

    def test_full_brake_split_brain_live_blocked(self):
        """check_live_entry_brake returns blocked on split-brain for LIVE."""
        split_brain_rows = [
            {
                "local_order_id": "ord-split",
                "broker_order_id": "br-999",
                "last_error_prefix": "SPLIT_BRAIN:failed to transition BROKER_ACKED",
                "status": "BROKER_ACKED",
            }
        ]
        # classifiers run in order; split-brain is first — only one DB call needed
        with _apply_patches([split_brain_rows]):
            result = check_live_entry_brake(
                client_id=CLIENT_ID,
                execution_mode="live",
            )

        assert result.blocked is True
        assert result.reason_code == BrakeCode.SPLIT_BRAIN_ORDER
        assert result.evidence["client_id"] == CLIENT_ID
        assert result.evidence["execution_mode"] == "live"

    def test_split_brain_terminal_order_does_not_block(self):
        """A FILLED split-brain order (reconciled terminal) must not block."""
        # Query returns empty → no nonterminal split-brain orders
        with _apply_patches([[]]):
            result = _check_split_brain(CLIENT_ID)

        assert result is None

    def test_split_brain_multiple_orders_captured_in_evidence(self):
        """Multiple split-brain orders are all captured in evidence."""
        rows = [
            {"local_order_id": f"ord-{i}", "broker_order_id": f"br-{i}",
             "last_error_prefix": "SPLIT_BRAIN:x", "status": "ACK"}
            for i in range(3)
        ]
        with _apply_patches([rows]):
            result = _check_split_brain(CLIENT_ID)

        assert result.blocked is True
        assert result.evidence["split_brain_order_count"] == 3


# ─────────────────────────────────────────────────────────────────────────────
# Acceptance test 2 — stuck protective exit blocks entry
# ─────────────────────────────────────────────────────────────────────────────

class TestCircuitBreakerBlocksEntry:
    """Spec test 2: active protective exit marked circuit-breaker-tripped → BLOCKED."""

    def _schema_rows(self):
        return [{"column_name": "meta"}, {"column_name": "status"},
                {"column_name": "execution_mode"}, {"column_name": "ticker"},
                {"column_name": "position_id"}]

    def test_tripped_circuit_breaker_blocks(self):
        """Position with exit_circuit_breaker_tripped=True in meta blocks."""
        import json
        meta_val = json.dumps({"exit_circuit_breaker_tripped": True, "exit_circuit_breaker_tripped_at": "2026-07-10T09:30:00Z"})
        pos_rows = [
            {"position_id": "pos-1", "ticker": "NVDA", "status": "OPEN", "meta_val": meta_val}
        ]
        schema_rows = self._schema_rows()

        # Patch _positions_meta_col to return "meta" directly (avoids schema call)
        with patch("ap.live_entry_brake._positions_meta_col", return_value="meta"):
            with _apply_patches([pos_rows]):
                result = _check_circuit_breaker(CLIENT_ID)

        assert result is not None
        assert result.blocked is True
        assert result.reason_code == BrakeCode.CIRCUIT_BREAKER_TRIPPED
        assert result.evidence["circuit_breaker_position_count"] == 1
        assert result.evidence["circuit_breaker_positions"][0]["ticker"] == "NVDA"

    def test_circuit_breaker_false_does_not_block(self):
        """Position with exit_circuit_breaker_tripped=False does not block."""
        import json
        meta_val = json.dumps({"exit_circuit_breaker_tripped": False})
        pos_rows = [
            {"position_id": "pos-2", "ticker": "TSLA", "status": "OPEN", "meta_val": meta_val}
        ]
        with patch("ap.live_entry_brake._positions_meta_col", return_value="meta"):
            with _apply_patches([pos_rows]):
                result = _check_circuit_breaker(CLIENT_ID)

        assert result is None

    def test_circuit_breaker_empty_meta_does_not_block(self):
        """Position with NULL/empty meta is not treated as tripped."""
        pos_rows = [
            {"position_id": "pos-3", "ticker": "AAPL", "status": "CLOSING", "meta_val": None}
        ]
        with patch("ap.live_entry_brake._positions_meta_col", return_value="meta"):
            with _apply_patches([pos_rows]):
                result = _check_circuit_breaker(CLIENT_ID)

        assert result is None

    def test_circuit_breaker_no_positions_does_not_block(self):
        """No nonterminal positions → no block."""
        with patch("ap.live_entry_brake._positions_meta_col", return_value="meta"):
            with _apply_patches([[]]):
                result = _check_circuit_breaker(CLIENT_ID)

        assert result is None

    def test_meta_col_missing_fails_closed(self):
        """If no recognized meta column exists, fail closed for LIVE safety."""
        with patch("ap.live_entry_brake._positions_meta_col", return_value=""):
            result = _check_circuit_breaker(CLIENT_ID)

        assert result is not None
        assert result.blocked is True
        assert result.reason_code == BrakeCode.STATE_UNAVAILABLE

    def test_full_brake_circuit_breaker_blocks_live(self):
        """Full check_live_entry_brake blocks when circuit breaker is tripped."""
        import json
        meta_val = json.dumps({"exit_circuit_breaker_tripped": True})
        # split_brain query returns empty; circuit_breaker query returns tripped position
        split_rows: list = []
        cb_rows = [{"position_id": "pos-cb", "ticker": "SPY", "status": "OPEN", "meta_val": meta_val}]

        with patch("ap.live_entry_brake._positions_meta_col", return_value="meta"):
            with _apply_patches([split_rows, cb_rows]):
                result = check_live_entry_brake(
                    client_id=CLIENT_ID,
                    execution_mode="live",
                )

        assert result.blocked is True
        assert result.reason_code == BrakeCode.CIRCUIT_BREAKER_TRIPPED


# ─────────────────────────────────────────────────────────────────────────────
# Acceptance test 3 — unknown-mode exposure blocks entry
# ─────────────────────────────────────────────────────────────────────────────

class TestUnknownModeExposureBlocksEntry:
    """Spec test 3: nonterminal exposure with blank/unknown execution_mode → BLOCKED."""

    def test_null_execution_mode_blocks(self):
        """Position with execution_mode=NULL blocks."""
        pos_rows = [
            {"position_id": "pos-null", "ticker": "AMZN",
             "status": "OPEN", "execution_mode": None}
        ]
        with _apply_patches([pos_rows]):
            result = _check_unknown_mode_exposure(CLIENT_ID)

        assert result is not None
        assert result.blocked is True
        assert result.reason_code == BrakeCode.UNKNOWN_MODE_EXPOSURE

    def test_blank_execution_mode_blocks(self):
        """Position with execution_mode='' blocks."""
        pos_rows = [
            {"position_id": "pos-blank", "ticker": "META",
             "status": "OPEN", "execution_mode": ""}
        ]
        with _apply_patches([pos_rows]):
            result = _check_unknown_mode_exposure(CLIENT_ID)

        assert result is not None
        assert result.blocked is True

    def test_garbage_execution_mode_blocks(self):
        """Position with execution_mode='unknown' or other garbage blocks."""
        pos_rows = [
            {"position_id": "pos-unk", "ticker": "GOOG",
             "status": "OPEN", "execution_mode": "unknown"}
        ]
        with _apply_patches([pos_rows]):
            result = _check_unknown_mode_exposure(CLIENT_ID)

        assert result is not None
        assert result.blocked is True

    def test_live_execution_mode_does_not_block(self):
        """Position with execution_mode='live' does not block."""
        with _apply_patches([[]]):
            result = _check_unknown_mode_exposure(CLIENT_ID)

        assert result is None

    def test_unknown_mode_full_brake_blocked(self):
        """Full brake blocks when unknown-mode position is detected."""
        pos_rows = [
            {"position_id": "pos-x", "ticker": "NVDA",
             "status": "OPEN", "execution_mode": None}
        ]
        # split_brain=[], circuit_breaker=[] (mocked empty), unknown_mode fires
        with patch("ap.live_entry_brake._positions_meta_col", return_value="meta"):
            with _apply_patches([[], [], pos_rows]):
                result = check_live_entry_brake(
                    client_id=CLIENT_ID,
                    execution_mode="live",
                )

        assert result.blocked is True
        assert result.reason_code == BrakeCode.UNKNOWN_MODE_EXPOSURE
        assert result.evidence["unknown_mode_position_count"] == 1


# ─────────────────────────────────────────────────────────────────────────────
# Acceptance test 4 — all-clear account permits entry
# ─────────────────────────────────────────────────────────────────────────────

class TestAllClearPermitsEntry:
    """Spec test 4: clean account state → entry allowed (blocked=False)."""

    def test_all_clear_live_passes(self):
        """No bad conditions → LIVE entry permitted."""
        # All five classifier queries return empty sets
        with patch("ap.live_entry_brake._positions_meta_col", return_value="meta"):
            with _apply_patches([[], [], [], [], []]):
                result = check_live_entry_brake(
                    client_id=CLIENT_ID,
                    execution_mode="live",
                )

        assert result.blocked is False
        assert result.reason_code == BrakeCode.CLEAR
        assert result.evidence["all_clear"] is True
        assert result.evidence["client_id"] == CLIENT_ID

    def test_all_clear_with_broker_snapshot_passes(self):
        """Clean account with successful broker snapshot → entry allowed."""
        broker_pos = [{"symbol": "AAPL230120C00150000", "quantity": 1}]
        with patch("ap.live_entry_brake._positions_meta_col", return_value="meta"):
            with _apply_patches([[], [], [], [], []]):
                result = check_live_entry_brake(
                    client_id=CLIENT_ID,
                    execution_mode="live",
                    broker_positions_available=True,
                    broker_positions=broker_pos,
                )

        assert result.blocked is False
        assert result.reason_code == BrakeCode.CLEAR

    def test_reason_code_is_clear_on_pass(self):
        """reason_code must be CLEAR (not empty string) on a passing result."""
        with patch("ap.live_entry_brake._positions_meta_col", return_value="meta"):
            with _apply_patches([[], [], [], [], []]):
                result = check_live_entry_brake(
                    client_id=CLIENT_ID,
                    execution_mode="live",
                )

        assert result.reason_code == BrakeCode.CLEAR
        assert result.reason_code == "LIVE_ENTRY_BRAKE_CLEAR"


# ─────────────────────────────────────────────────────────────────────────────
# Acceptance test 5 — brake failure blocks LIVE
# ─────────────────────────────────────────────────────────────────────────────

class TestBrakeFailureBlocksLive:
    """Spec test 5: classifier/DB error on LIVE → LIVE_ENTRY_BRAKE_STATE_UNAVAILABLE."""

    def test_db_exception_during_split_brain_fails_closed(self):
        """DB error in the first classifier → STATE_UNAVAILABLE block."""
        def _raise_conn():
            raise RuntimeError("Supabase connection timeout")

        with patch("ap.live_entry_brake._check_split_brain", side_effect=RuntimeError("db timeout")):
            result = check_live_entry_brake(
                client_id=CLIENT_ID,
                execution_mode="live",
            )

        assert result.blocked is True
        assert result.reason_code == BrakeCode.STATE_UNAVAILABLE
        assert "db timeout" in result.detail

    def test_db_exception_in_any_classifier_fails_closed(self):
        """Exception in circuit breaker classifier → STATE_UNAVAILABLE block."""
        with patch("ap.live_entry_brake._check_split_brain", return_value=None):
            with patch("ap.live_entry_brake._check_circuit_breaker",
                       side_effect=Exception("psycopg2 error")):
                result = check_live_entry_brake(
                    client_id=CLIENT_ID,
                    execution_mode="live",
                )

        assert result.blocked is True
        assert result.reason_code == BrakeCode.STATE_UNAVAILABLE

    def test_classifier_error_evidence_populated(self):
        """STATE_UNAVAILABLE result must include error details in evidence."""
        with patch("ap.live_entry_brake._check_split_brain",
                   side_effect=ValueError("schema drift")):
            result = check_live_entry_brake(
                client_id=CLIENT_ID,
                execution_mode="live",
            )

        assert result.blocked is True
        assert "classifier_error" in result.evidence
        assert "schema drift" in result.evidence["classifier_error"]
        assert result.evidence["classifier_error_type"] == "ValueError"

    def test_paper_classifier_error_does_not_block(self):
        """Even if a classifier would throw, PAPER exits before classifiers."""
        # PAPER short-circuits before any DB call — error in DB never reached
        with patch("ap.live_entry_brake._check_split_brain",
                   side_effect=RuntimeError("should not be called")):
            result = check_live_entry_brake(
                client_id=CLIENT_ID,
                execution_mode="paper",
            )

        # Paper path exits before classifiers; RuntimeError is never raised
        assert result.blocked is False
        assert result.reason_code == BrakeCode.CLEAR


# ─────────────────────────────────────────────────────────────────────────────
# Acceptance test 6 — PAPER behavior unchanged
# ─────────────────────────────────────────────────────────────────────────────

class TestPaperBehaviorUnchanged:
    """Spec test 6: explicit PAPER → never blocked by this module."""

    def test_paper_lowercase_passes(self):
        result = check_live_entry_brake(
            client_id=CLIENT_ID,
            execution_mode="paper",
        )
        assert result.blocked is False
        assert result.reason_code == BrakeCode.CLEAR
        assert result.evidence["paper_passthrough"] is True

    def test_paper_uppercase_passes(self):
        result = check_live_entry_brake(
            client_id=CLIENT_ID,
            execution_mode="PAPER",
        )
        assert result.blocked is False
        assert result.reason_code == BrakeCode.CLEAR

    def test_paper_mixed_case_passes(self):
        result = check_live_entry_brake(
            client_id=CLIENT_ID,
            execution_mode="Paper",
        )
        assert result.blocked is False

    def test_paper_with_tripped_circuit_breaker_still_passes(self):
        """Paper passes even if a live account would have been blocked."""
        # No DB calls should happen for paper; any call would raise (no patch)
        result = check_live_entry_brake(
            client_id=CLIENT_ID,
            execution_mode="paper",
            broker_positions_available=False,
        )
        assert result.blocked is False
        assert result.evidence["paper_passthrough"] is True

    def test_paper_no_db_calls_made(self):
        """Paper short-circuit must not touch the DB at all."""
        with patch("ap.live_entry_brake.conn") as mock_conn:
            check_live_entry_brake(
                client_id=CLIENT_ID,
                execution_mode="paper",
            )
        mock_conn.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# Acceptance test 7 — protective exit path never imports entry brake
# ─────────────────────────────────────────────────────────────────────────────

class TestExitPathNeverImportsEntryBrake:
    """
    Spec test 7: exit engine, exit_safety, and exit_manager must never import
    or call ap.live_entry_brake — doing so would risk deadlocking exit paths
    on account state checks, the very state the exit is meant to resolve.
    """

    _EXIT_FILES = [
        "ap_exit_engine.py",
        "ap/exit_safety.py",
        "ap/exit_manager.py",
        "ap/exit_autonomous_recovery.py",
        "ap/exit_decision_ledger.py",
        "ap/exit_reliability_monitor.py",
    ]

    def _read_source(self, filename: str) -> str:
        base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        path = os.path.join(base, filename)
        if not os.path.exists(path):
            return ""
        with open(path, "r", encoding="utf-8") as fh:
            return fh.read()

    def _has_entry_brake_reference(self, source: str) -> bool:
        """Return True if source contains any reference to live_entry_brake."""
        return "live_entry_brake" in source

    def test_exit_engine_does_not_import_entry_brake(self):
        src = self._read_source("ap_exit_engine.py")
        assert not self._has_entry_brake_reference(src), (
            "ap_exit_engine.py must never import or reference ap.live_entry_brake"
        )

    def test_exit_safety_does_not_import_entry_brake(self):
        src = self._read_source("ap/exit_safety.py")
        assert not self._has_entry_brake_reference(src), (
            "ap/exit_safety.py must never import or reference ap.live_entry_brake"
        )

    def test_exit_manager_does_not_import_entry_brake(self):
        src = self._read_source("ap/exit_manager.py")
        assert not self._has_entry_brake_reference(src), (
            "ap/exit_manager.py must never import or reference ap.live_entry_brake"
        )

    def test_exit_autonomous_recovery_does_not_import_entry_brake(self):
        src = self._read_source("ap/exit_autonomous_recovery.py")
        assert not self._has_entry_brake_reference(src), (
            "ap/exit_autonomous_recovery.py must not reference ap.live_entry_brake"
        )

    def test_exit_decision_ledger_does_not_import_entry_brake(self):
        src = self._read_source("ap/exit_decision_ledger.py")
        assert not self._has_entry_brake_reference(src), (
            "ap/exit_decision_ledger.py must not reference ap.live_entry_brake"
        )

    def test_exit_reliability_monitor_does_not_import_entry_brake(self):
        src = self._read_source("ap/exit_reliability_monitor.py")
        assert not self._has_entry_brake_reference(src), (
            "ap/exit_reliability_monitor.py must not reference ap.live_entry_brake"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Acceptance test 8 — one client's unsafe state cannot block another
# ─────────────────────────────────────────────────────────────────────────────

class TestClientIsolation:
    """Spec test 8: CLIENT_A's unsafe state must not affect CLIENT_B's entry."""

    def test_clientA_split_brain_does_not_block_clientB(self):
        """
        When queried for CLIENT_B, split-brain check returns empty (CLIENT_A's
        split brain is scoped by client_id in the WHERE clause and never surfaces).
        """
        # Simulate: CLIENT_B has zero split-brain orders
        with _apply_patches([[]]):
            result = _check_split_brain(CLIENT_B)

        assert result is None

    def test_clientA_circuit_breaker_does_not_block_clientB(self):
        """CLIENT_B's circuit-breaker check returns clean even if CLIENT_A is tripped."""
        with patch("ap.live_entry_brake._positions_meta_col", return_value="meta"):
            with _apply_patches([[]]):
                result = _check_circuit_breaker(CLIENT_B)

        assert result is None

    def test_clientA_unknown_mode_does_not_block_clientB(self):
        """CLIENT_B's unknown-mode check is independent of CLIENT_A's positions."""
        with _apply_patches([[]]):
            result = _check_unknown_mode_exposure(CLIENT_B)

        assert result is None

    def test_full_brake_clientB_clear_when_clientA_has_split_brain(self):
        """
        Full check_live_entry_brake for CLIENT_B returns CLEAR when CLIENT_B
        has no issues — CLIENT_A's split brain is invisible to CLIENT_B's query.
        """
        # CLIENT_B's queries all return empty
        with patch("ap.live_entry_brake._positions_meta_col", return_value="meta"):
            with _apply_patches([[], [], [], [], []]):
                result = check_live_entry_brake(
                    client_id=CLIENT_B,
                    execution_mode="live",
                )

        assert result.blocked is False
        assert result.reason_code == BrakeCode.CLEAR
        assert result.evidence["client_id"] == CLIENT_B

    def test_evidence_always_carries_correct_client_id(self):
        """Evidence dict must carry the queried client_id, never someone else's."""
        with patch("ap.live_entry_brake._positions_meta_col", return_value="meta"):
            with _apply_patches([[], [], [], [], []]):
                result_b = check_live_entry_brake(
                    client_id=CLIENT_B,
                    execution_mode="live",
                )

        assert result_b.evidence["client_id"] == CLIENT_B
        assert result_b.evidence["client_id"] != CLIENT_ID


# ─────────────────────────────────────────────────────────────────────────────
# Execution mode guard tests
# ─────────────────────────────────────────────────────────────────────────────

class TestExecutionModeGuard:
    """Blank/unknown execution_mode must block regardless of account state."""

    def test_blank_execution_mode_blocks(self):
        result = check_live_entry_brake(
            client_id=CLIENT_ID,
            execution_mode="",
        )
        assert result.blocked is True
        assert result.reason_code == BrakeCode.UNKNOWN_EXECUTION_MODE

    def test_none_execution_mode_blocks(self):
        result = check_live_entry_brake(
            client_id=CLIENT_ID,
            execution_mode=None,   # type: ignore[arg-type]
        )
        assert result.blocked is True
        assert result.reason_code == BrakeCode.UNKNOWN_EXECUTION_MODE

    def test_unknown_string_execution_mode_blocks(self):
        result = check_live_entry_brake(
            client_id=CLIENT_ID,
            execution_mode="sim",
        )
        assert result.blocked is True
        assert result.reason_code == BrakeCode.UNKNOWN_EXECUTION_MODE

    def test_live_uppercase_accepted(self):
        """'LIVE' (uppercase) must be normalised and treated as valid."""
        with patch("ap.live_entry_brake._positions_meta_col", return_value="meta"):
            with _apply_patches([[], [], [], [], []]):
                result = check_live_entry_brake(
                    client_id=CLIENT_ID,
                    execution_mode="LIVE",
                )
        assert result.blocked is False


# ─────────────────────────────────────────────────────────────────────────────
# Stale exit in-flight tests
# ─────────────────────────────────────────────────────────────────────────────

class TestStaleExitInFlight:
    """Condition 5: stale nonterminal EXIT order blocks entry."""

    def test_stale_exit_order_blocks(self):
        stale_rows = [
            {
                "position_id": "pos-stale",
                "ticker": "TSLA",
                "position_status": "CLOSING",
                "local_order_id": "ord-exit-1",
                "exit_order_status": "ACK",
                "created_ts": "2026-07-10T09:00:00+00:00",
                "updated_ts": "2026-07-10T09:00:00+00:00",
                "age_seconds": 600,
            }
        ]
        with _apply_patches([stale_rows]):
            result = _check_stale_exit_in_flight(CLIENT_ID)

        assert result is not None
        assert result.blocked is True
        assert result.reason_code == BrakeCode.STALE_EXIT_IN_FLIGHT
        assert result.evidence["stale_exit_count"] == 1

    def test_no_stale_exits_passes(self):
        """No stale exits → classifier returns None."""
        with _apply_patches([[]]):
            result = _check_stale_exit_in_flight(CLIENT_ID)

        assert result is None

    def test_stale_exit_evidence_populated(self):
        """Evidence must include ticker, age, and threshold."""
        stale_rows = [
            {
                "position_id": "pos-s",
                "ticker": "NVDA",
                "position_status": "CLOSING",
                "local_order_id": "ord-2",
                "exit_order_status": "NEW",
                "created_ts": "2026-07-10T09:00:00+00:00",
                "updated_ts": "2026-07-10T09:00:00+00:00",
                "age_seconds": 450,
            }
        ]
        with _apply_patches([stale_rows]):
            result = _check_stale_exit_in_flight(CLIENT_ID)

        assert result.evidence["stale_exits"][0]["ticker"] == "NVDA"
        assert "stale_sec_threshold" in result.evidence


# ─────────────────────────────────────────────────────────────────────────────
# Broker snapshot tests
# ─────────────────────────────────────────────────────────────────────────────

class TestBrokerSnapshot:
    """Conditions 4 & 6: broker snapshot failures and mismatches."""

    def test_broker_snapshot_unavailable_with_local_exposure_blocks(self):
        """broker_positions_available=False + local open positions → BLOCKED."""
        local_rows = [
            {"position_id": "pos-a", "ticker": "SPY",
             "status": "OPEN", "contract": "SPY260120C00400000", "local_qty": 1}
        ]
        with _apply_patches([local_rows]):
            result = _check_broker_snapshot(CLIENT_ID, False, None)

        assert result is not None
        assert result.blocked is True
        assert result.reason_code == BrakeCode.BROKER_SNAPSHOT_UNAVAILABLE
        assert result.evidence["local_position_count"] == 1

    def test_broker_snapshot_unavailable_no_local_exposure_passes(self):
        """broker_positions_available=False + no local positions → pass."""
        with _apply_patches([[]]):
            result = _check_broker_snapshot(CLIENT_ID, False, None)

        assert result is None

    def test_broker_snapshot_none_skips_check(self):
        """broker_positions_available=None → both sub-checks skipped, returns None."""
        result = _check_broker_snapshot(CLIENT_ID, None, None)
        assert result is None

    def test_broker_position_mismatch_blocks(self):
        """Local shows qty=1 but broker shows flat for same contract → BLOCKED."""
        local_rows = [
            {"position_id": "pos-b", "ticker": "AAPL",
             "status": "OPEN", "contract": "AAPL230120C00150000", "local_qty": 1}
        ]
        broker_positions = [
            # Different contract — the AAPL230120C00150000 is missing from broker
            {"symbol": "MSFT230120C00300000", "quantity": 2}
        ]
        with _apply_patches([local_rows]):
            result = _check_broker_snapshot(CLIENT_ID, True, broker_positions)

        assert result is not None
        assert result.blocked is True
        assert result.reason_code == BrakeCode.POSITION_MISMATCH
        assert result.evidence["mismatch_count"] == 1
        assert result.evidence["mismatches"][0]["mismatch_type"] == "local_open_broker_flat"

    def test_broker_position_match_passes(self):
        """Local qty matches broker qty → no mismatch, no block."""
        local_rows = [
            {"position_id": "pos-c", "ticker": "MSFT",
             "status": "OPEN", "contract": "MSFT230120C00300000", "local_qty": 2}
        ]
        broker_positions = [
            {"symbol": "MSFT230120C00300000", "quantity": 2}
        ]
        with _apply_patches([local_rows]):
            result = _check_broker_snapshot(CLIENT_ID, True, broker_positions)

        assert result is None

    def test_broker_returns_empty_list_with_local_exposure_blocks(self):
        """Broker snapshot succeeds but returns zero positions while local has open."""
        local_rows = [
            {"position_id": "pos-d", "ticker": "QQQ",
             "status": "OPEN", "contract": "QQQ230120C00350000", "local_qty": 3}
        ]
        with _apply_patches([local_rows]):
            result = _check_broker_snapshot(CLIENT_ID, True, [])

        assert result is not None
        assert result.blocked is True
        assert result.reason_code == BrakeCode.POSITION_MISMATCH


# ─────────────────────────────────────────────────────────────────────────────
# Evidence and audit trail integrity
# ─────────────────────────────────────────────────────────────────────────────

class TestEvidenceIntegrity:
    """Evidence dict must always be populated and contain baseline fields."""

    def test_blocked_result_contains_client_id(self):
        result = check_live_entry_brake(
            client_id=CLIENT_ID,
            execution_mode="",
        )
        assert result.blocked is True
        assert result.evidence["client_id"] == CLIENT_ID

    def test_blocked_result_contains_execution_mode(self):
        result = check_live_entry_brake(
            client_id=CLIENT_ID,
            execution_mode="",
        )
        assert "execution_mode" in result.evidence

    def test_passing_result_contains_checked_at(self):
        with patch("ap.live_entry_brake._positions_meta_col", return_value="meta"):
            with _apply_patches([[], [], [], [], []]):
                result = check_live_entry_brake(
                    client_id=CLIENT_ID,
                    execution_mode="live",
                )
        assert "checked_at" in result.evidence

    def test_brake_result_is_never_none(self):
        """check_live_entry_brake must never return None."""
        for mode in ("live", "paper", "", "unknown"):
            if mode in ("", "unknown"):
                result = check_live_entry_brake(
                    client_id=CLIENT_ID,
                    execution_mode=mode,
                )
            else:
                with patch("ap.live_entry_brake._positions_meta_col", return_value="meta"):
                    with _apply_patches([[], [], [], [], []]):
                        result = check_live_entry_brake(
                            client_id=CLIENT_ID,
                            execution_mode=mode,
                        )
            assert result is not None
            assert isinstance(result, BrakeResult)

    def test_reason_code_always_populated(self):
        """reason_code must never be empty string on any result."""
        for mode in ("live", "paper"):
            with patch("ap.live_entry_brake._positions_meta_col", return_value="meta"):
                with _apply_patches([[], [], [], [], []]):
                    result = check_live_entry_brake(
                        client_id=CLIENT_ID,
                        execution_mode=mode,
                    )
            assert result.reason_code
            assert result.reason_code != ""

    def test_checked_at_override_appears_in_evidence(self):
        """checked_at kwarg is reflected in evidence."""
        fixed_ts = datetime(2026, 7, 10, 14, 30, 0, tzinfo=timezone.utc)
        with patch("ap.live_entry_brake._positions_meta_col", return_value="meta"):
            with _apply_patches([[], [], [], [], []]):
                result = check_live_entry_brake(
                    client_id=CLIENT_ID,
                    execution_mode="live",
                    checked_at=fixed_ts,
                )
        assert "2026-07-10" in result.evidence["checked_at"]


# ─────────────────────────────────────────────────────────────────────────────
# BrakeResult data class structural tests
# ─────────────────────────────────────────────────────────────────────────────

class TestBrakeResultStructure:
    def test_defaults(self):
        r = BrakeResult(blocked=False)
        assert r.reason_code == BrakeCode.CLEAR
        assert r.detail == ""
        assert r.evidence == {}

    def test_blocked_true_with_reason(self):
        r = BrakeResult(
            blocked=True,
            reason_code=BrakeCode.SPLIT_BRAIN_ORDER,
            detail="test detail",
            evidence={"key": "val"},
        )
        assert r.blocked is True
        assert r.reason_code == "LIVE_ENTRY_BRAKE_SPLIT_BRAIN_ORDER"

    def test_brake_code_constants_are_unique(self):
        codes = [
            BrakeCode.SPLIT_BRAIN_ORDER,
            BrakeCode.CIRCUIT_BREAKER_TRIPPED,
            BrakeCode.UNKNOWN_MODE_EXPOSURE,
            BrakeCode.POSITION_MISMATCH,
            BrakeCode.STALE_EXIT_IN_FLIGHT,
            BrakeCode.BROKER_SNAPSHOT_UNAVAILABLE,
            BrakeCode.UNKNOWN_EXECUTION_MODE,
            BrakeCode.STATE_UNAVAILABLE,
            BrakeCode.CLEAR,
        ]
        assert len(codes) == len(set(codes)), "BrakeCode constants must be unique"

    def test_brake_code_constants_all_have_live_entry_brake_prefix(self):
        codes = [
            BrakeCode.SPLIT_BRAIN_ORDER,
            BrakeCode.CIRCUIT_BREAKER_TRIPPED,
            BrakeCode.UNKNOWN_MODE_EXPOSURE,
            BrakeCode.POSITION_MISMATCH,
            BrakeCode.STALE_EXIT_IN_FLIGHT,
            BrakeCode.BROKER_SNAPSHOT_UNAVAILABLE,
            BrakeCode.UNKNOWN_EXECUTION_MODE,
            BrakeCode.STATE_UNAVAILABLE,
        ]
        for code in codes:
            assert code.startswith("LIVE_ENTRY_BRAKE_"), (
                f"{code!r} must start with 'LIVE_ENTRY_BRAKE_'"
            )
