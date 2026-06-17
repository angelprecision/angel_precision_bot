"""
tests/test_p0_closed_filled_entry_exposure.py

P0 regression suite: closed/terminal position fills must not consume capital.

Root cause: _summarize_fill_truth_rows() previously only checked filled
entry orders against ACTIVE positions. A FILLED order whose linked position
was CLOSED (status=CLOSED, quantity_remaining=0) fell through the active-
position match and was treated as unreconciled exposure — directly causing
the Jason budget regression (198.724 → 15.724).

Covered:
  Test 1 — Jason regression: BAC+WFC filled orders linked to CLOSED positions
            must produce zero unreconciled capital (budget stays at 198.724).
  Test 2 — True unreconciled fill (no position_id) still counts.
  Test 3 — Fill with position_id pointing to a non-existent position counts
            as unreconciled (fail-closed) and the warning sentinel is present.
  Test 4 — Fill linked to an OPEN (active) position does not double-count.
  Test 5 — Mode isolation: paper fills excluded from live snapshot and vice versa.

Amendment (PR #152 amend):
  Test 6 — CLOSED_REPAIR status is treated as terminal.
  Test 7 — position with quantity_remaining=0 and exit_ts set is terminal
            even if status is not in the canonical set.
  Test 8 — OPEN position with qty>0 and no exit_ts is NOT treated as terminal
            (defensive condition must not fire on live positions).
  Test 9 — Fail-closed preserved: fill with missing position_id still
            counts as unreconciled even after amendment.
"""
from __future__ import annotations

import importlib
import re
import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

_REPO = Path(__file__).resolve().parents[1]
PM_SRC = (_REPO / "ap" / "position_manager.py").read_text()

# ---------------------------------------------------------------------------
# Import helpers — load the module without triggering DB/env imports
# ---------------------------------------------------------------------------

def _load_pm_functions():
    """Import only the pure Python helpers from position_manager without side-effects."""
    # We need: _summarize_fill_truth_rows, _build_terminal_position_id_set,
    # PositionStatus — all pure Python with no DB/env dependency.
    spec = importlib.util.spec_from_file_location(
        "ap.position_manager_test_shim",
        _REPO / "ap" / "position_manager.py",
    )
    # Stub out the heavy imports so the module loads in test context
    db_mock = MagicMock()
    db_mock.conn = MagicMock()
    db_mock.run_with_retry = lambda fn: fn()

    with patch.dict(sys.modules, {
        "ap.db": db_mock,
        "ap.utils": MagicMock(now_utc_iso=lambda: "2026-06-17T00:00:00+00:00"),
    }):
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    return mod


_pm = _load_pm_functions()
_summarize = _pm._summarize_fill_truth_rows
_build_terminal_set = _pm._build_terminal_position_id_set
_is_terminal_row = _pm._is_position_row_terminal
_TERMINAL_STATUS_SET = _pm._TERMINAL_STATUS_SET
PositionStatus = _pm.PositionStatus

# ---------------------------------------------------------------------------
# Source-level sanity checks
# ---------------------------------------------------------------------------

class TestSourceGuards:
    def test_build_terminal_position_id_set_exists(self):
        assert "_build_terminal_position_id_set" in PM_SRC

    def test_is_position_row_terminal_exists(self):
        assert "_is_position_row_terminal" in PM_SRC

    def test_terminal_status_set_constant_exists(self):
        assert "_TERMINAL_STATUS_SET" in PM_SRC

    def test_summarize_accepts_terminal_positions_kwarg(self):
        assert "terminal_positions" in PM_SRC

    def test_summarize_accepts_client_id_kwarg(self):
        src_sig_area = PM_SRC[PM_SRC.find("def _summarize_fill_truth_rows"):
                               PM_SRC.find("def _summarize_fill_truth_rows") + 600]
        assert "client_id" in src_sig_area

    def test_summarize_accepts_execution_mode_kwarg(self):
        src_sig_area = PM_SRC[PM_SRC.find("def _summarize_fill_truth_rows"):
                               PM_SRC.find("def _summarize_fill_truth_rows") + 600]
        assert "execution_mode" in src_sig_area

    def test_terminal_set_checked_before_active_match(self):
        fn_start = PM_SRC.find("def _summarize_fill_truth_rows")
        fn_body = PM_SRC[fn_start: fn_start + 4000]
        terminal_check_pos = fn_body.find("terminal_position_id_set")
        active_match_pos = fn_body.find("_match_fill_row_to_active_position")
        assert terminal_check_pos < active_match_pos, (
            "terminal_position_id_set check must occur before _match_fill_row_to_active_position"
        )

    def test_info_log_sentinel_present(self):
        assert "SNAPSHOT_RECONCILED_TERMINAL_FILLED_IGNORED" in PM_SRC

    def test_warning_log_sentinel_present(self):
        assert "SNAPSHOT_FILLED_ENTRY_MISSING_POSITION_COUNTS_UNRECONCILED" in PM_SRC

    def test_snapshot_fetches_terminal_positions_with_expanded_statuses(self):
        snap_start = PM_SRC.find("def snapshot(self")
        snap_body = PM_SRC[snap_start: snap_start + 10000]
        assert "CLOSED_REPAIR" in snap_body, (
            "snapshot() terminal query must include CLOSED_REPAIR"
        )
        assert "CANCELED" in snap_body, (
            "snapshot() terminal query must include CANCELED"
        )
        assert "CANCELLED" in snap_body, (
            "snapshot() terminal query must include CANCELLED"
        )

    def test_snapshot_terminal_query_fetches_quantity_remaining_and_exit_ts(self):
        snap_start = PM_SRC.find("def snapshot(self")
        snap_body = PM_SRC[snap_start: snap_start + 10000]
        assert "quantity_remaining" in snap_body, (
            "snapshot() terminal query must SELECT quantity_remaining"
        )
        assert "exit_ts" in snap_body, (
            "snapshot() terminal query must SELECT exit_ts"
        )

    def test_snapshot_terminal_query_has_defensive_or_condition(self):
        snap_start = PM_SRC.find("def snapshot(self")
        snap_body = PM_SRC[snap_start: snap_start + 10000]
        # The defensive condition: quantity_remaining=0 AND exit_ts IS NOT NULL
        assert "exit_ts IS NOT NULL" in snap_body, (
            "snapshot() terminal query must have defensive exit_ts IS NOT NULL condition"
        )

    def test_snapshot_passes_terminal_positions_to_summarize(self):
        snap_start = PM_SRC.find("def snapshot(self")
        snap_body = PM_SRC[snap_start:]
        assert "terminal_positions=terminal_positions" in snap_body

    def test_snapshot_return_contains_p0_audit_fields(self):
        assert "terminal_ignored_order_ids" in PM_SRC
        assert "terminal_ignored_position_ids" in PM_SRC
        assert "terminal_ignored_capital" in PM_SRC
        assert "missing_position_rows" in PM_SRC

    def test_position_status_terminal_set_covers_required_statuses(self):
        for s in ("CLOSED", "CLOSED_REPAIR", "EXPIRED", "STOPPED",
                  "TAKEN_PROFIT", "ERROR", "CANCELED", "CANCELLED"):
            assert s in PositionStatus.TERMINAL, f"{s} must be in PositionStatus.TERMINAL"

    def test_terminal_status_set_constant_covers_required_statuses(self):
        for s in ("CLOSED", "CLOSED_REPAIR", "EXPIRED", "STOPPED",
                  "TAKEN_PROFIT", "ERROR", "CANCELED", "CANCELLED"):
            assert s in _TERMINAL_STATUS_SET, f"{s} must be in _TERMINAL_STATUS_SET"


# ---------------------------------------------------------------------------
# Helper builders
# ---------------------------------------------------------------------------

def _order(
    *,
    id: int = 1,
    client_id: str = "jasoncosby1@gmail.com",
    kind: str = "ENTRY",
    status: str = "FILLED",
    direction: str = "CALL",
    fill_price: float = 1.08,
    filled_qty: int = 1,
    reserved_cost: float = 108.0,
    position_id: str | None = None,
    broker_order_id: str | None = None,
    local_order_id: str | None = None,
    signal_id: str | None = None,
    plan_id: str | None = None,
    contract: str = "BAC260626C00056000",
    execution_mode: str = "live",
) -> dict:
    return dict(
        id=id, client_id=client_id, kind=kind, status=status,
        direction=direction, fill_price=fill_price, filled_qty=filled_qty,
        reserved_cost=reserved_cost, position_id=position_id,
        broker_order_id=broker_order_id, local_order_id=local_order_id,
        signal_id=signal_id, plan_id=plan_id, contract=contract,
        execution_mode=execution_mode,
    )


def _position(
    *,
    id: str,
    status: str = "OPEN",
    quantity_remaining: int = 1,
    exit_ts: str | None = None,
    direction: str = "CALL",
    underlying: str = "BAC",
    client_id: str = "jasoncosby1@gmail.com",
    contract: str = "BAC260626C00056000",
    avg_fill: float = 1.08,
    qty: int = 1,
) -> dict:
    return dict(
        id=id, status=status, quantity_remaining=quantity_remaining,
        exit_ts=exit_ts, direction=direction, underlying=underlying,
        client_id=client_id, contract=contract, avg_fill=avg_fill, qty=qty,
    )


# ---------------------------------------------------------------------------
# Test 1 — Jason regression
# BAC (fill=$1.08, qty=1 → $108) + WFC (fill=$0.75, qty=1 → $75)
# Both linked to CLOSED positions.
# Expected: zero unreconciled capital — budget remains 198.724, not 15.724.
# ---------------------------------------------------------------------------

class TestJasonRegression:
    BAC_POS_ID = "8b6f6ae1-0f64-4314-a62d-a8db3350f644"
    WFC_POS_ID = "60f0e990-e80b-4d40-89cc-b844bc9cfe13"

    def _orders(self) -> list[dict]:
        return [
            _order(id=22202, contract="BAC260626C00056000",
                   fill_price=1.08, filled_qty=1, reserved_cost=108.0,
                   position_id=self.BAC_POS_ID, execution_mode="live"),
            _order(id=22212, contract="WFC260626C00086000",
                   fill_price=0.75, filled_qty=1, reserved_cost=77.0,
                   position_id=self.WFC_POS_ID, execution_mode="live"),
        ]

    def _terminal_positions(self) -> list[dict]:
        return [
            _position(id=self.BAC_POS_ID, status="CLOSED", quantity_remaining=0,
                      exit_ts="2026-06-17T15:30:00+00:00"),
            _position(id=self.WFC_POS_ID, status="CLOSED", quantity_remaining=0,
                      exit_ts="2026-06-17T15:30:00+00:00"),
        ]

    def test_zero_unreconciled_capital(self):
        result = _summarize(
            self._orders(),
            active_positions=[],
            terminal_positions=self._terminal_positions(),
            client_id="jasoncosby1@gmail.com",
            execution_mode="live",
        )
        assert result["filled_unreconciled_entry_capital"] == 0.0, (
            f"Expected 0 unreconciled capital, got {result['filled_unreconciled_entry_capital']}"
        )

    def test_zero_pending_entries(self):
        result = _summarize(
            self._orders(),
            active_positions=[],
            terminal_positions=self._terminal_positions(),
            client_id="jasoncosby1@gmail.com",
            execution_mode="live",
        )
        assert result["pending_entries"] == 0

    def test_terminal_fills_appear_in_audit_fields(self):
        result = _summarize(
            self._orders(),
            active_positions=[],
            terminal_positions=self._terminal_positions(),
            client_id="jasoncosby1@gmail.com",
            execution_mode="live",
        )
        assert result["terminal_ignored_capital"] == pytest.approx(108.0 + 75.0, abs=0.01)
        assert len(result["terminal_ignored_order_ids"]) == 2
        assert len(result["terminal_ignored_position_ids"]) == 2

    def test_budget_math_matches_expected(self):
        """Budget should be close to 198.724, not 15.724."""
        equity = 1987.24
        max_capital_pct = 0.10
        expected_budget = equity * max_capital_pct  # 198.724

        result = _summarize(
            self._orders(),
            active_positions=[],
            terminal_positions=self._terminal_positions(),
            client_id="jasoncosby1@gmail.com",
            execution_mode="live",
        )
        consumed = result["filled_unreconciled_entry_capital"]
        available_budget = expected_budget - consumed
        assert available_budget == pytest.approx(198.724, abs=0.01), (
            f"Available budget {available_budget} should be ~198.724, not ~15.724. "
            f"Unreconciled capital was {consumed}"
        )

    def test_all_terminal_statuses_suppressed(self):
        """Each PositionStatus.TERMINAL value must be suppressed."""
        for terminal_status in PositionStatus.TERMINAL:
            pos_id = f"pos-{terminal_status.lower()}"
            orders = [_order(id=1, fill_price=1.0, filled_qty=1,
                             reserved_cost=100.0, position_id=pos_id)]
            terminals = [_position(id=pos_id, status=terminal_status, quantity_remaining=0)]
            result = _summarize(
                orders, active_positions=[],
                terminal_positions=terminals,
                client_id="test", execution_mode="live",
            )
            assert result["filled_unreconciled_entry_capital"] == 0.0, (
                f"Status={terminal_status} should suppress fill but did not"
            )


# ---------------------------------------------------------------------------
# Test 2 — True unreconciled fill (no position_id) still counts
# ---------------------------------------------------------------------------

class TestTrueUnreconciledFill:
    def test_no_position_id_counts_as_unreconciled(self):
        order = _order(id=99, fill_price=2.00, filled_qty=1,
                       reserved_cost=200.0, position_id=None)
        result = _summarize(
            [order],
            active_positions=[],
            terminal_positions=[],
            client_id="test", execution_mode="live",
        )
        assert result["filled_unreconciled_entry_capital"] == pytest.approx(200.0, abs=0.01)
        assert result["pending_entries"] == 1

    def test_empty_string_position_id_counts_as_unreconciled(self):
        order = _order(id=99, fill_price=2.00, filled_qty=1,
                       reserved_cost=200.0, position_id="")
        result = _summarize(
            [order],
            active_positions=[],
            terminal_positions=[],
            client_id="test", execution_mode="live",
        )
        assert result["filled_unreconciled_entry_capital"] == pytest.approx(200.0, abs=0.01)
        assert result["pending_entries"] == 1


# ---------------------------------------------------------------------------
# Test 3 — Fill with position_id that resolves to nothing counts as
#           unreconciled (fail-closed) and warning sentinel fires
# ---------------------------------------------------------------------------

class TestMissingLinkedPosition:
    def test_orphan_position_id_counts_as_unreconciled(self):
        order = _order(id=55, fill_price=1.50, filled_qty=2,
                       reserved_cost=300.0, position_id="does-not-exist-pos-id")
        result = _summarize(
            [order],
            active_positions=[],
            terminal_positions=[],  # position_id not in any known set
            client_id="test", execution_mode="live",
        )
        assert result["filled_unreconciled_entry_capital"] == pytest.approx(300.0, abs=0.01)
        assert result["pending_entries"] == 1

    def test_orphan_position_id_emits_missing_position_row(self):
        order = _order(id=55, fill_price=1.50, filled_qty=2,
                       reserved_cost=300.0, position_id="ghost-pos-xyz")
        result = _summarize(
            [order],
            active_positions=[],
            terminal_positions=[],
            client_id="test", execution_mode="live",
        )
        assert len(result["missing_position_rows"]) == 1
        row = result["missing_position_rows"][0]
        assert row["position_id"] == "ghost-pos-xyz"
        assert row["capital"] == pytest.approx(300.0, abs=0.01)

    def test_orphan_does_not_appear_in_terminal_ignored(self):
        order = _order(id=55, fill_price=1.50, filled_qty=2,
                       reserved_cost=300.0, position_id="ghost-pos-xyz")
        result = _summarize(
            [order],
            active_positions=[],
            terminal_positions=[],
            client_id="test", execution_mode="live",
        )
        assert len(result["terminal_ignored_order_ids"]) == 0


# ---------------------------------------------------------------------------
# Test 4 — Fill linked to OPEN (active) position does not double-count
# ---------------------------------------------------------------------------

class TestActiveLinkDoesNotDoubleCound:
    ACTIVE_POS_ID = "active-pos-abc"

    def test_fill_linked_to_active_pos_suppressed(self):
        """The active position already represents exposure — don't add fill on top."""
        active = _position(id=self.ACTIVE_POS_ID, status="OPEN", quantity_remaining=1)
        order = _order(id=10, fill_price=1.00, filled_qty=1,
                       reserved_cost=100.0, position_id=self.ACTIVE_POS_ID)
        result = _summarize(
            [order],
            active_positions=[active],
            terminal_positions=[],
            client_id="test", execution_mode="live",
        )
        # The active position has position_id in the active set — matched via position_id key
        # so this should be ignored_already_reconciled, not unreconciled
        assert result["filled_unreconciled_entry_capital"] == 0.0
        assert result["pending_entries"] == 0

    def test_active_linked_fill_not_in_terminal_ignored(self):
        active = _position(id=self.ACTIVE_POS_ID, status="OPEN")
        order = _order(id=10, fill_price=1.00, filled_qty=1,
                       reserved_cost=100.0, position_id=self.ACTIVE_POS_ID)
        result = _summarize(
            [order],
            active_positions=[active],
            terminal_positions=[],
            client_id="test", execution_mode="live",
        )
        assert len(result["terminal_ignored_order_ids"]) == 0


# ---------------------------------------------------------------------------
# Test 5 — Mode isolation
# Paper fills must not count against live snapshot and vice versa.
# This tests _build_terminal_position_id_set is mode-agnostic (terminal set
# itself is not mode-filtered — it's scoped by client_id at the DB level),
# but the fill_rows fed to _summarize are already mode-filtered by snapshot().
# We verify the function correctly handles mode-tagged rows when fed directly.
# ---------------------------------------------------------------------------

class TestModeIsolation:
    CLOSED_POS_ID = "closed-pos-mode-test"
    OPEN_POS_ID = "open-pos-mode-test"

    def _terminal(self):
        return [_position(id=self.CLOSED_POS_ID, status="CLOSED",
                          quantity_remaining=0, exit_ts="2026-06-17T15:30:00+00:00")]

    def test_live_fill_linked_to_closed_does_not_leak_to_live_budget(self):
        """Live fill linked to closed position → zero unreconciled capital."""
        order = _order(id=1, fill_price=1.08, filled_qty=1, reserved_cost=108.0,
                       position_id=self.CLOSED_POS_ID, execution_mode="live")
        result = _summarize(
            [order], active_positions=[], terminal_positions=self._terminal(),
            client_id="test", execution_mode="live",
        )
        assert result["filled_unreconciled_entry_capital"] == 0.0

    def test_paper_fill_linked_to_closed_does_not_count(self):
        """Paper fill linked to closed position → zero unreconciled capital."""
        order = _order(id=2, fill_price=1.08, filled_qty=1, reserved_cost=108.0,
                       position_id=self.CLOSED_POS_ID, execution_mode="paper")
        result = _summarize(
            [order], active_positions=[], terminal_positions=self._terminal(),
            client_id="test", execution_mode="paper",
        )
        assert result["filled_unreconciled_entry_capital"] == 0.0

    def test_mixed_mode_fills_each_handled_correctly(self):
        """Feed two orders: one linked to closed pos, one truly unreconciled.
        Only the unreconciled one should consume capital."""
        orders = [
            _order(id=1, fill_price=1.08, filled_qty=1, reserved_cost=108.0,
                   position_id=self.CLOSED_POS_ID, execution_mode="live"),
            _order(id=2, fill_price=2.00, filled_qty=1, reserved_cost=200.0,
                   position_id=None, execution_mode="live"),
        ]
        result = _summarize(
            orders, active_positions=[], terminal_positions=self._terminal(),
            client_id="test", execution_mode="live",
        )
        assert result["filled_unreconciled_entry_capital"] == pytest.approx(200.0, abs=0.01)
        assert result["pending_entries"] == 1
        assert result["terminal_ignored_capital"] == pytest.approx(108.0, abs=0.01)


# ---------------------------------------------------------------------------
# Terminal set helper tests
# ---------------------------------------------------------------------------

class TestBuildTerminalPositionIdSet:
    def test_empty_input_returns_empty_set(self):
        assert _build_terminal_set([]) == set()

    def test_none_input_returns_empty_set(self):
        assert _build_terminal_set(None) == set()

    def test_returns_lowercased_ids(self):
        positions = [{"id": "ABC-123", "status": "CLOSED"}]
        result = _build_terminal_set(positions)
        assert "abc-123" in result
        assert "ABC-123" not in result

    def test_ignores_rows_with_empty_id(self):
        positions = [{"id": "", "status": "CLOSED"}, {"id": None, "status": "EXPIRED"}]
        result = _build_terminal_set(positions)
        assert len(result) == 0

    def test_multiple_ids_all_present(self):
        positions = [
            {"id": "pos-1", "status": "CLOSED"},
            {"id": "pos-2", "status": "EXPIRED"},
            {"id": "pos-3", "status": "STOPPED"},
        ]
        result = _build_terminal_set(positions)
        assert result == {"pos-1", "pos-2", "pos-3"}

# ---------------------------------------------------------------------------
# Test 6 — CLOSED_REPAIR status is treated as terminal
# Production DB has used CLOSED_REPAIR for reconciler repair paths.
# A FILLED ENTRY linked to CLOSED_REPAIR must not count as exposure.
# ---------------------------------------------------------------------------

class TestClosedRepairTerminal:
    CLOSED_REPAIR_POS_ID = "pos-closed-repair-001"

    def test_closed_repair_position_suppresses_fill(self):
        """FILLED ENTRY linked to CLOSED_REPAIR must be suppressed."""
        order = _order(id=301, fill_price=1.50, filled_qty=1, reserved_cost=150.0,
                       position_id=self.CLOSED_REPAIR_POS_ID, execution_mode="live")
        terminal = [_position(id=self.CLOSED_REPAIR_POS_ID, status="CLOSED_REPAIR",
                              quantity_remaining=0, exit_ts="2026-06-17T15:30:00+00:00")]
        result = _summarize(
            [order], active_positions=[], terminal_positions=terminal,
            client_id="test", execution_mode="live",
        )
        assert result["filled_unreconciled_entry_capital"] == 0.0, (
            "CLOSED_REPAIR position must suppress linked fill from exposure"
        )
        assert result["pending_entries"] == 0
        assert result["terminal_ignored_capital"] == pytest.approx(150.0, abs=0.01)

    def test_closed_repair_in_terminal_status_set(self):
        assert "CLOSED_REPAIR" in _TERMINAL_STATUS_SET

    def test_closed_repair_in_position_status_terminal(self):
        assert "CLOSED_REPAIR" in PositionStatus.TERMINAL

    def test_closed_repair_is_position_row_terminal(self):
        row = {"id": "pos-x", "status": "CLOSED_REPAIR",
               "quantity_remaining": 0, "exit_ts": "2026-06-17T15:30:00+00:00"}
        assert _is_terminal_row(row) is True

    def test_canceled_status_suppresses_fill(self):
        """CANCELED (broker spelling) must also be terminal."""
        pos_id = "pos-canceled-001"
        order = _order(id=302, fill_price=1.00, filled_qty=1, reserved_cost=100.0,
                       position_id=pos_id, execution_mode="live")
        terminal = [_position(id=pos_id, status="CANCELED", quantity_remaining=0,
                              exit_ts="2026-06-17T15:30:00+00:00")]
        result = _summarize(
            [order], active_positions=[], terminal_positions=terminal,
            client_id="test", execution_mode="live",
        )
        assert result["filled_unreconciled_entry_capital"] == 0.0

    def test_cancelled_alternate_spelling_suppresses_fill(self):
        """CANCELLED (alternate spelling) must also be terminal."""
        pos_id = "pos-cancelled-002"
        order = _order(id=303, fill_price=1.00, filled_qty=1, reserved_cost=100.0,
                       position_id=pos_id, execution_mode="live")
        terminal = [_position(id=pos_id, status="CANCELLED", quantity_remaining=0,
                              exit_ts="2026-06-17T15:30:00+00:00")]
        result = _summarize(
            [order], active_positions=[], terminal_positions=terminal,
            client_id="test", execution_mode="live",
        )
        assert result["filled_unreconciled_entry_capital"] == 0.0


# ---------------------------------------------------------------------------
# Test 7 — Defensive condition: quantity_remaining=0 AND exit_ts IS NOT NULL
# A position with an unexpected/unknown status but qty=0 and exit_ts set must
# be treated as terminal. This covers future repair variants and edge cases
# where the status field has an unexpected value.
# ---------------------------------------------------------------------------

class TestDefensiveTerminalCondition:
    UNKNOWN_STATUS_POS_ID = "pos-unknown-status-001"

    def test_qty_zero_plus_exit_ts_is_terminal_for_unknown_status(self):
        """qty_remaining=0 AND exit_ts set makes a position terminal
        even when its status is not in the canonical terminal set."""
        order = _order(id=401, fill_price=2.00, filled_qty=1, reserved_cost=200.0,
                       position_id=self.UNKNOWN_STATUS_POS_ID, execution_mode="live")
        # Use a status not in _TERMINAL_STATUS_SET (e.g. a hypothetical future variant)
        terminal = [_position(id=self.UNKNOWN_STATUS_POS_ID,
                              status="CLOSED_LEGACY",   # not in canonical set
                              quantity_remaining=0,
                              exit_ts="2026-06-17T15:30:00+00:00")]
        result = _summarize(
            [order], active_positions=[], terminal_positions=terminal,
            client_id="test", execution_mode="live",
        )
        assert result["filled_unreconciled_entry_capital"] == 0.0, (
            "qty_remaining=0 + exit_ts must suppress fill even for unknown status"
        )
        assert result["pending_entries"] == 0
        assert result["terminal_ignored_capital"] == pytest.approx(200.0, abs=0.01)

    def test_is_position_row_terminal_qty_zero_with_exit_ts(self):
        row = {"id": "pos-x", "status": "CLOSED_LEGACY",
               "quantity_remaining": 0, "exit_ts": "2026-06-17T15:30:00+00:00"}
        assert _is_terminal_row(row) is True

    def test_is_position_row_terminal_qty_nonzero_with_exit_ts_is_not_terminal(self):
        """qty_remaining > 0 with exit_ts — defensive condition must NOT fire."""
        row = {"id": "pos-x", "status": "CLOSING",
               "quantity_remaining": 1, "exit_ts": "2026-06-17T15:30:00+00:00"}
        assert _is_terminal_row(row) is False

    def test_is_position_row_terminal_qty_zero_without_exit_ts_is_not_terminal(self):
        """qty_remaining=0 but no exit_ts — defensive condition must NOT fire alone."""
        row = {"id": "pos-x", "status": "CLOSING",
               "quantity_remaining": 0, "exit_ts": None}
        assert _is_terminal_row(row) is False

    def test_qty_zero_no_exit_ts_not_suppressed(self):
        """qty_remaining=0 but no exit_ts — fail-closed: count as unreconciled."""
        pos_id = "pos-qty-zero-no-exit"
        order = _order(id=402, fill_price=1.00, filled_qty=1, reserved_cost=100.0,
                       position_id=pos_id, execution_mode="live")
        # Status not in terminal set, qty=0 but NO exit_ts
        terminal = [_position(id=pos_id, status="CLOSING",
                              quantity_remaining=0, exit_ts=None)]
        result = _summarize(
            [order], active_positions=[], terminal_positions=terminal,
            client_id="test", execution_mode="live",
        )
        # This row is not terminal (status=CLOSING, no exit_ts), and not in
        # active_positions either — so it's an orphan (missing from active set)
        # and falls to the fail-closed unreconciled path.
        assert result["pending_entries"] == 1
        assert result["filled_unreconciled_entry_capital"] == pytest.approx(100.0, abs=0.01)


# ---------------------------------------------------------------------------
# Test 8 — OPEN position with qty>0 and no exit_ts is NOT terminal
# The defensive condition must never fire on a genuinely live position.
# ---------------------------------------------------------------------------

class TestActivePositionNotTerminal:
    def test_open_position_qty_gt_zero_no_exit_ts_not_terminal(self):
        """An OPEN position with qty>0 and no exit_ts must never be terminal."""
        row = {"id": "pos-open", "status": "OPEN",
               "quantity_remaining": 1, "exit_ts": None}
        assert _is_terminal_row(row) is False

    def test_closing_position_qty_gt_zero_not_terminal(self):
        row = {"id": "pos-closing", "status": "CLOSING",
               "quantity_remaining": 2, "exit_ts": None}
        assert _is_terminal_row(row) is False

    def test_open_position_still_suppresses_fill_via_active_match(self):
        """An OPEN position should suppress its fill via the active-position
        match path, NOT via the terminal path. Terminal path must not fire."""
        pos_id = "open-pos-live-test"
        active = _position(id=pos_id, status="OPEN", quantity_remaining=1)
        order = _order(id=501, fill_price=1.00, filled_qty=1,
                       reserved_cost=100.0, position_id=pos_id)
        result = _summarize(
            [order],
            active_positions=[active],
            terminal_positions=[],  # not in terminal — it's live
            client_id="test", execution_mode="live",
        )
        assert result["filled_unreconciled_entry_capital"] == 0.0
        assert result["pending_entries"] == 0
        # Must NOT appear in terminal_ignored — it was reconciled via active match
        assert result["terminal_ignored_capital"] == 0.0
        assert len(result["terminal_ignored_order_ids"]) == 0


# ---------------------------------------------------------------------------
# Test 9 — Fail-closed preserved after amendment
# Fills with a missing/orphan position_id must still count as unreconciled
# after the terminal status expansion. The amendment must not widen the
# suppression beyond genuinely terminal positions.
# ---------------------------------------------------------------------------

class TestFailClosedPreservedAfterAmendment:
    def test_orphan_position_id_still_unreconciled(self):
        """position_id not found in any set → fail-closed, count as unreconciled."""
        order = _order(id=601, fill_price=1.50, filled_qty=1, reserved_cost=150.0,
                       position_id="pos-does-not-exist-anywhere")
        # Empty terminal list — position_id resolves to nothing
        result = _summarize(
            [order], active_positions=[], terminal_positions=[],
            client_id="test", execution_mode="live",
        )
        assert result["pending_entries"] == 1
        assert result["filled_unreconciled_entry_capital"] == pytest.approx(150.0, abs=0.01)
        assert len(result["missing_position_rows"]) == 1

    def test_null_position_id_still_unreconciled(self):
        """No position_id at all → truly unreconciled, count as exposure."""
        order = _order(id=602, fill_price=2.00, filled_qty=1,
                       reserved_cost=200.0, position_id=None)
        result = _summarize(
            [order], active_positions=[], terminal_positions=[],
            client_id="test", execution_mode="live",
        )
        assert result["pending_entries"] == 1
        assert result["filled_unreconciled_entry_capital"] == pytest.approx(200.0, abs=0.01)

    def test_only_terminal_fills_suppressed_others_count(self):
        """Mixed batch: one terminal-linked fill (suppressed) + one orphan (counts)."""
        terminal_pos_id = "pos-closed-repair-batch"
        orphan_pos_id = "pos-orphan-batch"
        orders = [
            _order(id=603, fill_price=1.08, filled_qty=1, reserved_cost=108.0,
                   position_id=terminal_pos_id, execution_mode="live"),
            _order(id=604, fill_price=2.00, filled_qty=1, reserved_cost=200.0,
                   position_id=orphan_pos_id, execution_mode="live"),
        ]
        terminal = [_position(id=terminal_pos_id, status="CLOSED_REPAIR",
                              quantity_remaining=0, exit_ts="2026-06-17T15:30:00+00:00")]
        result = _summarize(
            orders, active_positions=[], terminal_positions=terminal,
            client_id="test", execution_mode="live",
        )
        assert result["terminal_ignored_capital"] == pytest.approx(108.0, abs=0.01)
        assert result["filled_unreconciled_entry_capital"] == pytest.approx(200.0, abs=0.01)
        assert result["pending_entries"] == 1
        assert len(result["missing_position_rows"]) == 1
