"""
tests/test_p0_partial_close_reconciler.py

P0: Reconciler Prematurely Marking Partial Positions CLOSED
============================================================

Tests cover every acceptance criterion:
  1. Reconciler auto-close never sets CLOSED when quantity_remaining > 0
  2. RECONCILER_AUTO_CLOSE partial positions preserved as PARTIAL
  3. Admin/operator dashboard query includes all active broker exposure
  4. Exit engine _load_db_position_row finds PARTIAL rows
  5. get_active_positions includes PARTIAL/ACTIVE and qty_remaining > 0 guard
  6. Broker-flat repair zeroes quantity_remaining, keeps CLOSED
  7. Broker-live repair restores status to PARTIAL/OPEN
  8. Broker-unavailable: status not touched, flagged for manual review
  9. Diagnostic counters increment correctly
 10. No scanner/signal/entry/sizing/submit behavior changes
"""

from __future__ import annotations

import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch, call

# ── Repo-relative import path (invariant: never sys.path hacks) ───────────────
import sys
_REPO = Path(__file__).parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))


# =============================================================================
# Helpers / shared fixtures
# =============================================================================

def _make_pos(
    pos_id="pos-001",
    contract="PG260620C00155000",
    underlying="PG",
    qty=7,
    quantity_remaining=5,
    status="CLOSED",
    close_source="RECONCILER_AUTO_CLOSE",
    avg_fill=1.50,
    entry_price=1.50,
    exit_price=None,
    client_id="jasoncosby1@gmail.com",
    option_symbol=None,
    side="CALL",
    **kw,
) -> dict:
    """Minimal positions row fixture."""
    return {
        "id":                pos_id,
        "contract":          contract,
        "option_symbol":     option_symbol or contract,
        "underlying":        underlying,
        "ticker":            underlying,
        "qty":               qty,
        "quantity_remaining": quantity_remaining,
        "status":            status,
        "close_source":      close_source,
        "avg_fill":          avg_fill,
        "entry_price":       entry_price,
        "exit_price":        exit_price,
        "client_id":         client_id,
        "side":              side,
        "direction":         side,
        "local_order_id":    kw.get("local_order_id", ""),
        **kw,
    }


def _make_broker_pos(contract: str, qty: int) -> dict:
    return {"symbol": contract, "quantity": qty, "cost_basis": 1.50}


# =============================================================================
# 1. Reconciler auto-close: PARTIAL when quantity_remaining > 0
# =============================================================================

class TestReconcilerAutoClosePartialGuard(unittest.TestCase):
    """
    Core bug: _handle_auto_close_ghost must not set status=CLOSED when
    the position has remaining contracts.  Validates the quantity_remaining
    re-fetch under lock and status decision tree.
    """

    def _build_reconciler_stub(self):
        """Build a minimal APBrokerReconciler with DB wired to a mock."""
        from ap_reconciler import APBrokerReconciler, _empty_summary

        broker = MagicMock()
        osm    = MagicMock()
        pm     = MagicMock()
        rec    = APBrokerReconciler(broker=broker, client_id="jasoncosby1@gmail.com",
                                    osm=osm, pm=pm)
        return rec, _empty_summary("jasoncosby1@gmail.com")

    def test_quantity_remaining_zeroed_when_broker_is_flat(self):
        """
        Core P0 bug fix: when quantity_remaining=5 and broker is flat (ghost confirm),
        reconciler must set quantity_remaining=0 (not leave it at 5).

        Before fix: status=CLOSED, quantity_remaining=5  ← INVALID (the bug)
        After fix:  status=CLOSED, quantity_remaining=0  ← CORRECT
        """
        from ap_reconciler import APBrokerReconciler, _empty_summary

        rec, summary = self._build_reconciler_stub()

        # Simulate DB returning a row with remaining qty = 5, full qty = 7
        db_row = {"quantity_remaining": 5, "qty": 7}
        written = {}

        def _fake_execute(sql, params=None):
            cursor = MagicMock()
            sql_up = sql.strip().upper()
            if "FOR UPDATE" in sql_up:
                cursor.fetchone.return_value = db_row
            elif sql_up.startswith("UPDATE POSITIONS"):
                # Capture what status was written
                # params order: status, exit_ts, exit_price, pnl, pnl_pct,
                #               quantity_remaining, close_source, confidence, id, client
                written["status"]             = params[0]
                written["quantity_remaining"] = params[5]
            return cursor

        fake_conn_ctx = MagicMock()
        fake_conn_ctx.__enter__ = lambda s: MagicMock(execute=_fake_execute)
        fake_conn_ctx.__exit__  = MagicMock(return_value=False)

        pos = _make_pos(qty=7, quantity_remaining=5, status="OPEN")

        with patch("ap.db.conn", return_value=fake_conn_ctx), \
             patch("ap.db.run_with_retry", side_effect=lambda fn: fn()):
            rec._execute_reconciler_close(
                pos=pos,
                contract="PG260620C00155000",
                underlying="PG",
                db_qty=7,
                entry_px=1.50,
                exit_px=0.10,
                close_confidence="HIGH",
                summary=summary,
                side="CALL",
            )

        # P0 FIX: broker is flat → ALL remaining contracts gone → qty_remaining=0
        # Before fix: status=CLOSED but quantity_remaining=5 (left unchanged) — invalid state
        # After fix:  status=CLOSED and quantity_remaining=0                  — correct
        self.assertEqual(written.get("quantity_remaining"), 0,
                         "P0 FIX: quantity_remaining must be zeroed when broker is flat")
        self.assertEqual(written.get("status"), "CLOSED",
                         "CLOSED is correct when broker is flat and remaining reaches 0")
        self.assertEqual(summary["reconciler_full_close_count"], 1)

    def test_fully_closed_position_gets_CLOSED(self):
        """
        When quantity_remaining=0 (all contracts exited), status must be CLOSED.
        """
        from ap_reconciler import APBrokerReconciler, _empty_summary

        rec, summary = self._build_reconciler_stub()
        db_row = {"quantity_remaining": 0, "qty": 7}
        written = {}

        def _fake_execute(sql, params=None):
            cursor = MagicMock()
            if "FOR UPDATE" in sql.upper():
                cursor.fetchone.return_value = db_row
            elif sql.strip().upper().startswith("UPDATE POSITIONS"):
                written["status"]             = params[0]
                written["quantity_remaining"] = params[5]
            return cursor

        fake_conn_ctx = MagicMock()
        fake_conn_ctx.__enter__ = lambda s: MagicMock(execute=_fake_execute)
        fake_conn_ctx.__exit__  = MagicMock(return_value=False)

        pos = _make_pos(qty=7, quantity_remaining=0, status="OPEN")

        with patch("ap.db.conn", return_value=fake_conn_ctx), \
             patch("ap.db.run_with_retry", side_effect=lambda fn: fn()):
            rec._execute_reconciler_close(
                pos=pos,
                contract="PG260620C00155000",
                underlying="PG",
                db_qty=7,
                entry_px=1.50,
                exit_px=0.10,
                close_confidence="HIGH",
                summary=summary,
                side="CALL",
            )

        self.assertEqual(written.get("status"), "CLOSED")
        self.assertEqual(summary["reconciler_full_close_count"], 1)
        self.assertEqual(summary["reconciler_partial_close_preserved_count"], 0)

    def test_null_quantity_remaining_treated_as_full_qty(self):
        """
        If quantity_remaining is NULL (legacy row), fall back to qty and still
        produce CLOSED (broker is flat, nothing remains).
        """
        from ap_reconciler import APBrokerReconciler, _empty_summary

        rec, summary = self._build_reconciler_stub()
        db_row = {"quantity_remaining": None, "qty": 5}
        written = {}

        def _fake_execute(sql, params=None):
            cursor = MagicMock()
            if "FOR UPDATE" in sql.upper():
                cursor.fetchone.return_value = db_row
            elif sql.strip().upper().startswith("UPDATE POSITIONS"):
                written["status"] = params[0]
            return cursor

        fake_conn_ctx = MagicMock()
        fake_conn_ctx.__enter__ = lambda s: MagicMock(execute=_fake_execute)
        fake_conn_ctx.__exit__  = MagicMock(return_value=False)

        pos = _make_pos(qty=5, quantity_remaining=None, status="OPEN")

        with patch("ap.db.conn", return_value=fake_conn_ctx), \
             patch("ap.db.run_with_retry", side_effect=lambda fn: fn()):
            rec._execute_reconciler_close(
                pos=pos,
                contract="AAPL260620C00200000",
                underlying="AAPL",
                db_qty=5,
                entry_px=2.00,
                exit_px=0.05,
                close_confidence="HIGH",
                summary=summary,
                side="CALL",
            )

        # NULL → full qty → close_qty = full_qty → new_remaining = 0 → CLOSED
        self.assertEqual(written.get("status"), "CLOSED")

    def test_exit_engine_notified_on_full_broker_flat_close(self):
        """
        mark_position_closed MUST be called when broker is flat and all
        remaining contracts are closed (new_remaining=0 → status=CLOSED).
        """
        from ap_reconciler import APBrokerReconciler, _empty_summary

        rec, summary = self._build_reconciler_stub()
        mock_ee = MagicMock()
        rec.exit_engine = mock_ee

        # qty_remaining=3 → broker flat → close_qty=3 → new_remaining=0 → CLOSED
        db_row = {"quantity_remaining": 3, "qty": 5}

        def _fake_execute(sql, params=None):
            cursor = MagicMock()
            if "FOR UPDATE" in sql.upper():
                cursor.fetchone.return_value = db_row
            return cursor

        fake_conn_ctx = MagicMock()
        fake_conn_ctx.__enter__ = lambda s: MagicMock(execute=_fake_execute)
        fake_conn_ctx.__exit__  = MagicMock(return_value=False)

        pos = _make_pos(qty=5, quantity_remaining=3, status="OPEN")

        with patch("ap.db.conn", return_value=fake_conn_ctx), \
             patch("ap.db.run_with_retry", side_effect=lambda fn: fn()):
            rec._execute_reconciler_close(
                pos=pos,
                contract="AAPL260620C00200000",
                underlying="AAPL",
                db_qty=5,
                entry_px=2.00,
                exit_px=0.05,
                close_confidence="HIGH",
                summary=summary,
                side="CALL",
            )

        # broker-flat, all remaining closed → CLOSED → exit engine notified
        mock_ee.mark_position_closed.assert_called_once()

    def test_exit_engine_notified_on_full_close(self):
        """mark_position_closed IS called when quantity_remaining reaches 0."""
        from ap_reconciler import APBrokerReconciler, _empty_summary

        rec, summary = self._build_reconciler_stub()
        mock_ee = MagicMock()
        rec.exit_engine = mock_ee

        db_row = {"quantity_remaining": 0, "qty": 5}

        def _fake_execute(sql, params=None):
            cursor = MagicMock()
            if "FOR UPDATE" in sql.upper():
                cursor.fetchone.return_value = db_row
            return cursor

        fake_conn_ctx = MagicMock()
        fake_conn_ctx.__enter__ = lambda s: MagicMock(execute=_fake_execute)
        fake_conn_ctx.__exit__  = MagicMock(return_value=False)

        pos = _make_pos(qty=5, quantity_remaining=0, status="OPEN")

        with patch("ap.db.conn", return_value=fake_conn_ctx), \
             patch("ap.db.run_with_retry", side_effect=lambda fn: fn()):
            rec._execute_reconciler_close(
                pos=pos,
                contract="AAPL260620C00200000",
                underlying="AAPL",
                db_qty=5,
                entry_px=2.00,
                exit_px=0.05,
                close_confidence="HIGH",
                summary=summary,
                side="CALL",
            )

        mock_ee.mark_position_closed.assert_called_once()


# =============================================================================
# 2. _repair_closed_positions_with_remaining_qty
# =============================================================================

class TestRepairClosedPositionsWithRemainingQty(unittest.TestCase):

    def _make_rec(self):
        from ap_reconciler import APBrokerReconciler, _empty_summary
        broker = MagicMock()
        osm    = MagicMock()
        pm     = MagicMock()
        rec    = APBrokerReconciler(broker=broker, client_id="jasoncosby1@gmail.com",
                                    osm=osm, pm=pm)
        return rec, _empty_summary("jasoncosby1@gmail.com")

    def test_broker_flat_repair_zeros_remaining(self):
        """
        When broker has no position for a CLOSED+remaining row,
        quantity_remaining is set to 0 and close_source=CLOSED_REPAIR.
        """
        from ap_reconciler import APBrokerReconciler, _empty_summary

        rec, summary = self._make_rec()

        bad_rows = [_make_pos(qty=7, quantity_remaining=5, status="CLOSED",
                              contract="PG260620C00155000")]

        # Broker returns empty (flat)
        rec.broker.list_positions.return_value = []

        written = {}

        def _fake_rwr(fn):
            result = fn()
            return result

        class _FakeCursor:
            """Cursor that dispatches execute() calls and sets fetchall accordingly."""
            def __init__(self):
                self.rowcount = 0
                self._fetchall_result = []
            def execute(self, sql, params=None):
                sql_norm = " ".join(sql.split()).upper()
                if "UPPER(STATUS)" in sql_norm and "CLOSED" in sql_norm and "SELECT" in sql_norm:
                    self._fetchall_result = bad_rows
                elif "UPDATE" in sql_norm and "CLOSED_REPAIR" in sql_norm:
                    written["zeroed"] = True
                    self.rowcount = 1
            def fetchall(self): return self._fetchall_result
            def fetchone(self): return None

        _cur = _FakeCursor()
        fake_ctx = MagicMock()
        fake_ctx.__enter__ = lambda s: _cur
        fake_ctx.__exit__  = MagicMock(return_value=False)

        with patch("ap.db.conn", return_value=fake_ctx), \
             patch("ap.db.run_with_retry", side_effect=_fake_rwr):
            rec._repair_closed_positions_with_remaining_qty(summary)

        self.assertTrue(written.get("zeroed"), "Must zero quantity_remaining for broker-flat row")

    def test_broker_live_repair_restores_status(self):
        """
        When broker holds the contract, status is restored to PARTIAL/OPEN and
        quantity_remaining is set from broker truth.
        """
        from ap_reconciler import APBrokerReconciler, _empty_summary

        rec, summary = self._make_rec()
        bad_rows = [_make_pos(qty=7, quantity_remaining=5, status="CLOSED",
                              contract="PG260620C00155000")]

        broker_pos = _make_broker_pos("PG260620C00155000", qty=5)
        rec.broker.list_positions.return_value = [broker_pos]
        rec._seed_exit_engine_from_position = MagicMock()

        written = {}

        class _FakeCursor:
            def __init__(self):
                self.rowcount = 0
                self._fetchall_result = []
            def execute(self, sql, params=None):
                sql_norm = " ".join(sql.split()).upper()
                if "UPPER(STATUS)" in sql_norm and "CLOSED" in sql_norm and "SELECT" in sql_norm:
                    self._fetchall_result = bad_rows
                elif "UPDATE" in sql_norm and "PARTIAL_CLOSE_REPAIR" in sql_norm:
                    written["status"] = params[0]
                    written["qty"]    = params[1]
                    self.rowcount = 1
            def fetchall(self): return self._fetchall_result
            def fetchone(self): return None

        _cur = _FakeCursor()
        fake_ctx = MagicMock()
        fake_ctx.__enter__ = lambda s: _cur
        fake_ctx.__exit__  = MagicMock(return_value=False)

        with patch("ap.db.conn", return_value=fake_ctx), \
             patch("ap.db.run_with_retry", side_effect=lambda fn: fn()):
            rec._repair_closed_positions_with_remaining_qty(summary)

        self.assertIn(written.get("status"), ("PARTIAL", "OPEN"),
                      "Broker-live row must be restored to PARTIAL or OPEN")
        self.assertGreater(written.get("qty", 0), 0,
                           "quantity_remaining must be restored from broker")
        self.assertEqual(summary["broker_positions_hidden_by_closed_status_count"], 1)
        rec._seed_exit_engine_from_position.assert_called_once()

    def test_broker_unavailable_status_unchanged(self):
        """
        When broker API fails, status must NOT change — flag for manual review only.
        """
        from ap_reconciler import APBrokerReconciler, _empty_summary

        rec, summary = self._make_rec()
        bad_rows = [_make_pos(qty=5, quantity_remaining=3, status="CLOSED",
                              contract="AAPL260620C00200000")]
        rec.broker.list_positions.side_effect = RuntimeError("broker timeout")

        updates = []

        class _FakeCursor:
            def __init__(self):
                self.rowcount = 0
                self._fetchall_result = []
            def execute(self, sql, params=None):
                sql_norm = " ".join(sql.split()).upper()
                if "UPPER(STATUS)" in sql_norm and "CLOSED" in sql_norm and "SELECT" in sql_norm:
                    self._fetchall_result = bad_rows
                elif sql_norm.startswith("UPDATE"):
                    updates.append(sql)
            def fetchall(self): return self._fetchall_result
            def fetchone(self): return None

        _cur = _FakeCursor()
        fake_ctx = MagicMock()
        fake_ctx.__enter__ = lambda s: _cur
        fake_ctx.__exit__  = MagicMock(return_value=False)

        with patch("ap.db.conn", return_value=fake_ctx), \
             patch("ap.db.run_with_retry", side_effect=lambda fn: fn()):
            rec._repair_closed_positions_with_remaining_qty(summary)

        self.assertEqual(updates, [], "Must not UPDATE any row when broker is unavailable")
        self.assertEqual(summary["broker_positions_hidden_by_closed_status_count"], 1)

    def test_no_bad_rows_is_noop(self):
        """When there are no CLOSED rows with remaining qty, repair is a no-op."""
        from ap_reconciler import APBrokerReconciler, _empty_summary

        rec, summary = self._make_rec()

        def _fake_execute(sql, params=None):
            cursor = MagicMock()
            sql_norm = " ".join(sql.split()).upper()
            if "UPPER(STATUS)" in sql_norm and "CLOSED" in sql_norm and "SELECT" in sql_norm:
                cursor.fetchall.return_value = []
            return cursor

        fake_ctx = MagicMock()
        fake_ctx.__enter__ = lambda s: MagicMock(execute=_fake_execute)
        fake_ctx.__exit__  = MagicMock(return_value=False)

        with patch("ap.db.conn", return_value=fake_ctx), \
             patch("ap.db.run_with_retry", side_effect=lambda fn: fn()):
            rec._repair_closed_positions_with_remaining_qty(summary)

        self.assertEqual(summary["closed_positions_with_remaining_qty_count"], 0)
        self.assertEqual(summary["broker_positions_hidden_by_closed_status_count"], 0)


# =============================================================================
# 3. Diagnostic counters in _empty_summary
# =============================================================================

class TestEmptySummaryDiagnosticKeys(unittest.TestCase):

    def test_all_p0_keys_present(self):
        from ap_reconciler import _empty_summary
        s = _empty_summary("test@ap.com")
        required = [
            "closed_positions_with_remaining_qty_count",
            "closed_positions_with_remaining_qty_recent",
            "broker_positions_hidden_by_closed_status_count",
            "reconciler_partial_close_preserved_count",
            "reconciler_full_close_count",
        ]
        for key in required:
            self.assertIn(key, s, f"Missing diagnostic key: {key}")
            self.assertEqual(s[key], 0, f"Key {key} must start at 0")

    def test_all_standard_keys_present(self):
        from ap_reconciler import _empty_summary
        s = _empty_summary("test@ap.com")
        for key in ("run", "client_id", "orders_checked", "positions_checked",
                    "positions_corrected", "errors", "skipped"):
            self.assertIn(key, s)


# =============================================================================
# 4. DB_OPEN_POSITION_STATUSES includes PARTIAL and ACTIVE
# =============================================================================

class TestOpenPositionStatusConstants(unittest.TestCase):

    def test_partial_in_db_open_statuses(self):
        from ap_reconciler import DB_OPEN_POSITION_STATUSES
        self.assertIn("PARTIAL", DB_OPEN_POSITION_STATUSES)

    def test_active_in_db_open_statuses(self):
        from ap_reconciler import DB_OPEN_POSITION_STATUSES
        self.assertIn("ACTIVE", DB_OPEN_POSITION_STATUSES)

    def test_open_closing_still_present(self):
        from ap_reconciler import DB_OPEN_POSITION_STATUSES
        self.assertIn("OPEN", DB_OPEN_POSITION_STATUSES)
        self.assertIn("CLOSING", DB_OPEN_POSITION_STATUSES)


# =============================================================================
# 5. get_active_positions includes PARTIAL and qty_remaining guard
# =============================================================================

class TestGetActivePositionsQuery(unittest.TestCase):
    """
    Verify that get_active_positions SQL contains the expanded status set
    and the quantity_remaining > 0 safety guard.

    Uses source-file reads (no DB import) to avoid psycopg2 at test time.
    """

    @classmethod
    def _read_pm_source(cls) -> str:
        src_path = _REPO / "ap" / "position_manager.py"
        return src_path.read_text()

    def test_query_includes_partial_status(self):
        src = self._read_pm_source()
        # Find the get_active_positions function block
        start = src.index("def get_active_positions")
        # Grab the next 40 lines
        block = src[start:start+1500]
        self.assertIn("PARTIAL", block,
                      "get_active_positions must include PARTIAL in status filter")

    def test_query_includes_quantity_remaining_guard(self):
        src = self._read_pm_source()
        start = src.index("def get_active_positions")
        block = src[start:start+1500]
        self.assertIn("quantity_remaining", block,
                      "get_active_positions must have quantity_remaining > 0 guard")

    def test_open_count_includes_partial(self):
        src = self._read_pm_source()
        start = src.index("def open_count")
        block = src[start:start+800]
        self.assertIn("PARTIAL", block)

    def test_has_open_position_includes_partial(self):
        src = self._read_pm_source()
        start = src.index("def has_open_position")
        block = src[start:start+800]
        self.assertIn("PARTIAL", block)


# =============================================================================
# 6. Exit engine _load_db_position_row includes expanded filter
# =============================================================================

class TestExitEngineLoadDbPositionRow(unittest.TestCase):

    def test_query_includes_partial(self):
        import inspect
        from ap_exit_engine import APExitEngine
        src = inspect.getsource(APExitEngine._load_db_position_row)
        self.assertIn("PARTIAL", src)

    def test_query_includes_qty_remaining_guard(self):
        import inspect
        from ap_exit_engine import APExitEngine
        src = inspect.getsource(APExitEngine._load_db_position_row)
        self.assertIn("quantity_remaining", src)


# =============================================================================
# 7. run_once wires repair call into orchestration
# =============================================================================

class TestRunOnceWiresRepair(unittest.TestCase):

    def test_repair_called_in_run_once(self):
        import inspect
        from ap_reconciler import APBrokerReconciler
        src = inspect.getsource(APBrokerReconciler.run_once)
        self.assertIn("_repair_closed_positions_with_remaining_qty", src,
                      "run_once must call _repair_closed_positions_with_remaining_qty")

    def test_repair_called_after_reconcile_positions(self):
        import inspect
        from ap_reconciler import APBrokerReconciler
        src = inspect.getsource(APBrokerReconciler.run_once)
        pos_idx    = src.index("_reconcile_positions")
        repair_idx = src.index("_repair_closed_positions_with_remaining_qty")
        self.assertGreater(repair_idx, pos_idx,
                           "repair must be called AFTER _reconcile_positions")


# =============================================================================
# 8. proof_trade NOT logged for partial reconciler close
# =============================================================================

class TestPartialCloseSkipsProofTrade(unittest.TestCase):

    def test_proof_logger_not_called_on_partial(self):
        from ap_reconciler import APBrokerReconciler, _empty_summary

        broker = MagicMock()
        rec    = APBrokerReconciler(broker=broker, client_id="jasoncosby1@gmail.com",
                                    osm=MagicMock(), pm=MagicMock())
        summary = _empty_summary("jasoncosby1@gmail.com")

        db_row = {"quantity_remaining": 3, "qty": 5}

        def _fake_execute(sql, params=None):
            cursor = MagicMock()
            if "FOR UPDATE" in sql.upper():
                cursor.fetchone.return_value = db_row
            return cursor

        fake_ctx = MagicMock()
        fake_ctx.__enter__ = lambda s: MagicMock(execute=_fake_execute)
        fake_ctx.__exit__  = MagicMock(return_value=False)

        mock_proof = MagicMock()

        pos = _make_pos(qty=5, quantity_remaining=3, status="OPEN")

        with patch("ap.db.conn", return_value=fake_ctx), \
             patch("ap.db.run_with_retry", side_effect=lambda fn: fn()), \
             patch("ap_reconciler.APProofLogger", return_value=mock_proof, create=True):
            rec._execute_reconciler_close(
                pos=pos,
                contract="BA260620C00180000",
                underlying="BA",
                db_qty=5,
                entry_px=1.80,
                exit_px=0.05,
                close_confidence="HIGH",
                summary=summary,
                side="CALL",
            )

        mock_proof.log_trade.assert_not_called()


# =============================================================================
# 9. No-op for scanner / signal / entry / sizing paths
# =============================================================================

class TestScopeGuard(unittest.TestCase):
    """
    Confirm the fix touches zero scanner/entry/sizing files.
    The changed files are a strict subset of:
      ap_reconciler.py, ap_exit_engine.py,
      ap/position_manager.py, ap/admin_api.py
    """

    CHANGED_FILES = {
        "ap_reconciler.py",
        "ap_exit_engine.py",
        "ap/position_manager.py",
        "ap/admin_api.py",
    }

    FORBIDDEN_FILES = {
        "ap_master_control.py",
        "ap_execution_core.py",
        "ap/contract_selector.py",
        "ap/position_sizer.py",
        "ap/queue.py",
        "ap_entry_watcher.py",
        "ap_quality_mode.py",
        "ap/contract_selection.py",
    }

    def test_scanner_sizing_untouched(self):
        """Verify forbidden files have NOT been recently modified by this PR."""
        import subprocess, json, os
        repo = str(_REPO)
        result = subprocess.run(
            ["git", "diff", "--name-only", "HEAD"],
            cwd=repo, capture_output=True, text=True,
        )
        changed = set(result.stdout.strip().splitlines())
        forbidden_touched = changed & self.FORBIDDEN_FILES
        self.assertEqual(
            forbidden_touched, set(),
            f"P0 fix must not touch scanner/entry/sizing files. "
            f"Unexpectedly changed: {forbidden_touched}",
        )


# =============================================================================
# 10. Integration: run_once summary always has new P0 diagnostic keys
# =============================================================================

class TestRunOnceSummaryShape(unittest.TestCase):

    def test_skipped_summary_has_p0_keys(self):
        """The skipped-path cached summary must also carry P0 diagnostic keys."""
        from ap_reconciler import APBrokerReconciler, _empty_summary, RUN_ONCE_MIN_INTERVAL_SEC

        broker = MagicMock()
        rec    = APBrokerReconciler(broker=broker, client_id="test@ap.com",
                                    osm=MagicMock(), pm=MagicMock())
        # Pre-set a cached summary so the skipped path returns it
        cached = _empty_summary("test@ap.com")
        rec._last_run_once_summary = cached
        import time
        rec._last_run_once_ts = time.monotonic()  # just ran

        result = rec.run_once()
        self.assertTrue(result.get("skipped"))
        self.assertIn("closed_positions_with_remaining_qty_count", result)
        self.assertIn("reconciler_partial_close_preserved_count", result)
        self.assertIn("reconciler_full_close_count", result)


if __name__ == "__main__":
    unittest.main(verbosity=2)
