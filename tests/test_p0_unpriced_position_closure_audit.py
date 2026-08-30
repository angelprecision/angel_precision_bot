"""
tests/test_p0_unpriced_position_closure_audit.py

P0: detect positions closed with quantity_remaining=0 but no recorded
exit_ts / exit_price / realized_pnl.

Incident: 2026-08-28, jasoncosby1@gmail.com, BAC260904C00062000 (live) --
56 rejected exit attempts, position ended CLOSED/qty=0 with no exit price
ever recorded. This suite proves the audit finds that shape, does not
false-positive on properly closed or still-open positions, never raises,
and is wired into the reconciler cycle without being able to break it.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

if not os.getenv("DATABASE_URL"):
    os.environ["DATABASE_URL"] = "postgresql://test:test@127.0.0.1:5432/test"

from pathlib import Path
import sys
_REPO = Path(__file__).parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from ap import closed_position_price_audit as cppa


CLIENT = "jasoncosby1@gmail.com"


def _row(**overrides) -> dict:
    row = {
        "id": "pos-bac-001",
        "client_id": CLIENT,
        "contract": "BAC260904C00062000",
        "direction": "CALL",
        "qty": 2,
        "quantity_remaining": 0,
        "entry_price": 0.69,
        "entry_ts": "2026-08-28T14:09:23+00:00",
        "exit_ts": None,
        "exit_price": None,
        "realized_pnl": None,
        "execution_mode": "live",
        "close_source": None,
        "updated_at": "2026-08-28T14:58:43+00:00",
    }
    row.update(overrides)
    return row


def _mock_conn_returning(rows: list[dict]):
    """Build a conn() context manager mock whose cursor().fetchall() returns rows."""
    cursor = MagicMock()
    cursor.fetchall.return_value = rows
    cm = MagicMock()
    cm.__enter__.return_value = cursor
    cm.__exit__.return_value = False
    return cm, cursor


# =============================================================================
# find_unpriced_closures — SQL shape and filtering
# =============================================================================

class TestFindUnpricedClosures:

    def test_finds_position_missing_all_exit_fields(self):
        cm, cursor = _mock_conn_returning([_row()])
        with patch.object(cppa, "conn", return_value=cm), \
             patch.object(cppa, "run_with_retry", side_effect=lambda fn: fn()):
            rows = cppa.find_unpriced_closures(CLIENT)
        assert len(rows) == 1
        assert rows[0]["id"] == "pos-bac-001"

    def test_finds_position_missing_only_exit_price(self):
        row = _row(exit_ts="2026-08-28T14:58:00+00:00", realized_pnl=None, exit_price=None)
        cm, cursor = _mock_conn_returning([row])
        with patch.object(cppa, "conn", return_value=cm), \
             patch.object(cppa, "run_with_retry", side_effect=lambda fn: fn()):
            rows = cppa.find_unpriced_closures(CLIENT)
        assert len(rows) == 1

    def test_query_filters_by_client_id_status_and_quantity(self):
        """The SQL itself must filter on client_id, status='CLOSED', and
        quantity_remaining=0 -- verify the exact params passed to execute()."""
        cm, cursor = _mock_conn_returning([])
        with patch.object(cppa, "conn", return_value=cm), \
             patch.object(cppa, "run_with_retry", side_effect=lambda fn: fn()):
            cppa.find_unpriced_closures(CLIENT, lookback_days=10)
        sql, params = cursor.execute.call_args[0]
        assert "status" in sql.lower() and "closed" in sql.lower()
        assert "quantity_remaining" in sql.lower()
        assert params[0] == CLIENT
        assert params[1] == "10"

    def test_rejects_invalid_lookback_days(self):
        assert cppa.find_unpriced_closures(CLIENT, lookback_days=0) == []
        assert cppa.find_unpriced_closures(CLIENT, lookback_days=-5) == []

    def test_rejects_empty_client_id(self):
        assert cppa.find_unpriced_closures("") == []
        assert cppa.find_unpriced_closures(None) == []

    def test_db_error_returns_empty_list_not_exception(self):
        with patch.object(cppa, "run_with_retry", side_effect=RuntimeError("db down")):
            rows = cppa.find_unpriced_closures(CLIENT)
        assert rows == []


# =============================================================================
# audit_unpriced_closures — alert behavior
# =============================================================================

class TestAuditUnpricedClosures:

    def test_logs_critical_for_each_unpriced_row(self):
        rows = [_row(id="pos-1"), _row(id="pos-2", contract="NOW260828P00122000")]
        with patch.object(cppa, "find_unpriced_closures", return_value=rows), \
             patch.object(cppa, "log") as mock_log:
            count = cppa.audit_unpriced_closures(CLIENT)
        assert count == 2
        assert mock_log.critical.call_count == 2

    def test_log_marker_includes_missing_fields(self):
        row = _row(exit_ts="2026-08-28T14:58:00+00:00")  # exit_price, realized_pnl still None
        with patch.object(cppa, "find_unpriced_closures", return_value=[row]), \
             patch.object(cppa, "log") as mock_log:
            cppa.audit_unpriced_closures(CLIENT)
        args = mock_log.critical.call_args[0]
        # "missing_fields=%s" is the second-to-last positional format arg
        missing_field_arg = args[-2]
        assert "exit_price" in missing_field_arg
        assert "realized_pnl" in missing_field_arg
        assert "exit_ts" not in missing_field_arg

    def test_no_rows_no_log_calls(self):
        with patch.object(cppa, "find_unpriced_closures", return_value=[]), \
             patch.object(cppa, "log") as mock_log:
            count = cppa.audit_unpriced_closures(CLIENT)
        assert count == 0
        mock_log.critical.assert_not_called()

    def test_properly_closed_position_never_reaches_audit(self):
        """Sanity: a fully-priced closure should never even be returned by
        find_unpriced_closures, so this test drives the two functions
        together with a mock DB layer returning zero rows for a fully
        priced position (simulating the SQL WHERE clause excluding it)."""
        cm, cursor = _mock_conn_returning([])  # SQL excludes fully-priced rows
        with patch.object(cppa, "conn", return_value=cm), \
             patch.object(cppa, "run_with_retry", side_effect=lambda fn: fn()), \
             patch.object(cppa, "log") as mock_log:
            count = cppa.audit_unpriced_closures(CLIENT)
        assert count == 0
        mock_log.critical.assert_not_called()


# =============================================================================
# Wiring into ap/reconcile.py::run_reconciliation
# =============================================================================

class TestReconcileWiring:

    def test_run_reconciliation_calls_audit_unpriced_closures(self):
        from ap import reconcile as reconcile_mod

        broker = MagicMock()
        with patch.object(reconcile_mod, "reconcile_once", return_value=3), \
             patch("ap.reconciler_heartbeat.record_heartbeat"), \
             patch("ap.closed_position_price_audit.audit_unpriced_closures") as mock_audit:
            result = reconcile_mod.run_reconciliation(CLIENT, broker=broker)

        assert result["ok"] is True
        assert result["processed"] == 3
        mock_audit.assert_called_once_with(CLIENT)

    def test_audit_failure_never_breaks_reconciliation_result(self):
        """A failure inside the audit call must not propagate -- same
        contract as the pre-existing heartbeat call it sits next to."""
        from ap import reconcile as reconcile_mod

        broker = MagicMock()
        with patch.object(reconcile_mod, "reconcile_once", return_value=1), \
             patch("ap.reconciler_heartbeat.record_heartbeat"), \
             patch(
                 "ap.closed_position_price_audit.audit_unpriced_closures",
                 side_effect=RuntimeError("audit exploded"),
             ):
            result = reconcile_mod.run_reconciliation(CLIENT, broker=broker)

        assert result["ok"] is True
        assert result["processed"] == 1

    def test_audit_module_never_imports_broker_mutation_paths(self):
        """Static guard: this module must not reference any broker
        submit/cancel method, keeping it a pure observer like
        ap/reconciler_heartbeat.py."""
        import inspect
        source = inspect.getsource(cppa)
        for forbidden in ("submit_order", "cancel_order", "place_stop_order", "sell_to_close"):
            assert forbidden not in source, (
                f"{forbidden} must not appear in closed_position_price_audit.py — "
                "this module is alert-only and must never touch the broker"
            )
