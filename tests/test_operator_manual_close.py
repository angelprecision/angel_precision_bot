"""
tests/test_operator_manual_close.py
─────────────────────────────────────────────────────────────────────────────
PR: feat/operator-manual-close-endpoint

Run:
    DATABASE_URL="postgresql://test:test@localhost:5432/test" \
    ENCRYPTION_KEY="ap-test-key" \
    python3 -m pytest tests/test_operator_manual_close.py -v
─────────────────────────────────────────────────────────────────────────────
"""
from pathlib import Path
import re

import pytest
from unittest.mock import MagicMock, patch
from ap.operator_manual_close import (
    _calculate_pnl,
    _cancel_pending_exit_orders,
    execute_operator_manual_close,
)

CLIENT_ID   = "jasoncosby1@gmail.com"
POSITION_ID = "74af11c2-6b5f-407e-a5cc-7cac2a59aebf"
CONTRACT    = "SMCI260626P00032500"


def _mock_cursor(return_rows=None, rowcount=1):
    cur = MagicMock()
    cur.fetchone.return_value = return_rows
    cur.rowcount = rowcount
    return cur


def _open_position():
    return {
        "id": POSITION_ID, "client_id": CLIENT_ID,
        "status": "CLOSED",  # already closed by reconciler
        "contract": CONTRACT, "qty": 1,
        "avg_fill": 1.40, "cost_basis": 140.0,
        "entry_ts": None, "entry_option_price": None,
        "exit_price": 1.06, "exit_reason": None,
        "close_source": "RECONCILER_AUTO_CLOSE",
    }


# ── _calculate_pnl ────────────────────────────────────────────────────────────

class TestCalculatePnl:
    def test_loss_trade(self):
        pos = {"qty": 1, "cost_basis": 140.0, "avg_fill": None, "entry_option_price": None}
        pnl, pct = _calculate_pnl(pos, true_fill_price=1.05)
        assert pnl == -35.0
        assert round(pct, 2) == -25.0

    def test_win_trade(self):
        pos = {"qty": 1, "cost_basis": 140.0, "avg_fill": None, "entry_option_price": None}
        pnl, pct = _calculate_pnl(pos, true_fill_price=1.75)
        assert pnl == 35.0
        assert round(pct, 2) == 25.0

    def test_falls_back_to_avg_fill(self):
        pos = {"qty": 1, "cost_basis": None, "avg_fill": 1.40, "entry_option_price": None}
        pnl, pct = _calculate_pnl(pos, true_fill_price=1.05)
        assert pnl == -35.0

    def test_multi_contract(self):
        pos = {"qty": 3, "cost_basis": 420.0, "avg_fill": None, "entry_option_price": None}
        pnl, pct = _calculate_pnl(pos, true_fill_price=1.20)
        # exit_proceeds = 1.20 * 3 * 100 = 360; entry = 420; pnl = -60
        assert pnl == -60.0

    def test_no_cost_data_returns_zero_pnl(self):
        pos = {"qty": 1, "cost_basis": None, "avg_fill": None, "entry_option_price": None}
        pnl, pct = _calculate_pnl(pos, true_fill_price=1.05)
        assert pnl == 0.0
        assert pct == 0.0


# ── _cancel_pending_exit_orders ───────────────────────────────────────────────

class TestCancelPendingExitOrders:
    def test_cancels_open_exits_only_with_execution_mode_scope(self):
        cur = _mock_cursor(rowcount=3)
        result = _cancel_pending_exit_orders(cur, CLIENT_ID, CONTRACT, "live")
        assert result == 3
        sql, params = cur.execute.call_args[0]
        assert "status IN" in sql
        assert "execution_mode = %s" in sql
        assert "execution_mode IS NULL" in sql
        for active_status in (
            "NEW",
            "PROCESSING",
            "CREATED",
            "WATCHING",
            "PENDING_TRIGGER",
            "SUBMITTED",
            "ACKNOWLEDGED",
            "PARTIAL_FILL",
            "PARTIALLY_FILLED",
        ):
            assert active_status in sql

    def test_does_not_rewrite_terminal_exit_statuses(self):
        cur = _mock_cursor(rowcount=0)
        _cancel_pending_exit_orders(cur, CLIENT_ID, CONTRACT, "paper")
        sql, _ = cur.execute.call_args[0]
        for terminal_status in ("EXPIRED", "ARCHIVED", "ERROR"):
            assert terminal_status not in sql

    def test_uses_parameterized_query(self):
        cur = _mock_cursor(rowcount=0)
        _cancel_pending_exit_orders(cur, CLIENT_ID, CONTRACT, "paper")
        _, params = cur.execute.call_args[0]
        assert CLIENT_ID in params
        assert CONTRACT in params
        assert "paper" in params


# ── execute_operator_manual_close ─────────────────────────────────────────────

class TestExecuteOperatorManualClose:

    def _run(self, true_fill_price=1.05, pos_override=None):
        pos = pos_override or _open_position()

        with patch("ap.operator_manual_close.run_with_retry") as mock_retry, \
             patch("ap.operator_manual_close.conn") as mock_conn_ctx:

            def call_fn(fn):
                return fn()

            mock_retry.side_effect = call_fn

            mock_conn = MagicMock()
            mock_conn.__enter__ = MagicMock(return_value=mock_conn)
            mock_conn.__exit__  = MagicMock(return_value=False)
            mock_conn_ctx.return_value = mock_conn

            with patch("ap.operator_manual_close._audit_log_exists", return_value=True), \
                 patch("ap.operator_manual_close._fetch_position", return_value=pos), \
                 patch("ap.operator_manual_close._derive_execution_mode", return_value="live"), \
                 patch("ap.operator_manual_close._count_pending_exit_orders", return_value=2), \
                 patch("ap.operator_manual_close._update_position", return_value=1), \
                 patch("ap.operator_manual_close._cancel_pending_exit_orders", return_value=2), \
                 patch("ap.operator_manual_close._write_audit_log"):
                return execute_operator_manual_close(
                    position_id=POSITION_ID,
                    client_id=CLIENT_ID,
                    true_fill_price=true_fill_price,
                )

    def test_returns_ok_true(self):
        result = self._run()
        assert result["ok"] is True

    def test_correct_exit_price_in_result(self):
        result = self._run(true_fill_price=1.05)
        assert result["exit_price"] == 1.05

    def test_pnl_calculated_correctly(self):
        result = self._run(true_fill_price=1.05)
        assert result["realized_pnl"] == -35.0

    def test_returns_404_when_position_not_found(self):
        with patch("ap.operator_manual_close.run_with_retry") as mock_retry, \
             patch("ap.operator_manual_close.conn") as mock_conn_ctx, \
             patch("ap.operator_manual_close._audit_log_exists", return_value=True), \
             patch("ap.operator_manual_close._fetch_position", return_value=None):

            def call_fn(fn): return fn()
            mock_retry.side_effect = call_fn
            mock_conn = MagicMock()
            mock_conn.__enter__ = MagicMock(return_value=mock_conn)
            mock_conn.__exit__  = MagicMock(return_value=False)
            mock_conn_ctx.return_value = mock_conn

            result = execute_operator_manual_close(
                position_id="nonexistent",
                client_id=CLIENT_ID,
                true_fill_price=1.05,
            )
        assert result["ok"] is False
        assert result["status_code"] == 404

    def test_was_already_closed_flag(self):
        result = self._run()
        assert result["was_already_closed"] is True

    def test_open_position_is_rejected(self):
        pos = _open_position()
        pos["status"] = "OPEN"
        result = self._run(pos_override=pos)
        assert result["ok"] is False
        assert result["status_code"] == 409
        assert result["error"] == "repair_only_closed_position_required"

    def test_true_fill_price_zero_rejected(self):
        result = self._run(true_fill_price=0)
        assert result["ok"] is False
        assert result["status_code"] == 400
        assert result["error"] == "true_fill_price_must_be_positive"

    def test_audit_table_missing_returns_safe_error_before_mutation(self):
        with patch("ap.operator_manual_close.run_with_retry") as mock_retry, \
             patch("ap.operator_manual_close.conn") as mock_conn_ctx, \
             patch("ap.operator_manual_close._audit_log_exists", return_value=False), \
             patch("ap.operator_manual_close._update_position") as update_mock:
            mock_retry.side_effect = lambda fn: fn()
            mock_conn = MagicMock()
            mock_conn.__enter__ = MagicMock(return_value=mock_conn)
            mock_conn.__exit__ = MagicMock(return_value=False)
            mock_conn_ctx.return_value = mock_conn
            result = execute_operator_manual_close(
                position_id=POSITION_ID,
                client_id=CLIENT_ID,
                true_fill_price=1.05,
            )
        assert result["ok"] is False
        assert result["status_code"] == 503
        assert result["error"] == "operator_audit_log_missing"
        update_mock.assert_not_called()

    def test_execution_mode_is_preserved_when_canceling_exit_orders(self):
        with patch("ap.operator_manual_close.run_with_retry") as mock_retry, \
             patch("ap.operator_manual_close.conn") as mock_conn_ctx, \
             patch("ap.operator_manual_close._audit_log_exists", return_value=True), \
             patch("ap.operator_manual_close._fetch_position", return_value=_open_position()), \
             patch("ap.operator_manual_close._derive_execution_mode", return_value="paper"), \
             patch("ap.operator_manual_close._count_pending_exit_orders", return_value=1), \
             patch("ap.operator_manual_close._update_position", return_value=1), \
             patch("ap.operator_manual_close._cancel_pending_exit_orders", return_value=1) as cancel_mock, \
             patch("ap.operator_manual_close._write_audit_log"):
            mock_retry.side_effect = lambda fn: fn()
            mock_conn = MagicMock()
            mock_conn.__enter__ = MagicMock(return_value=mock_conn)
            mock_conn.__exit__ = MagicMock(return_value=False)
            mock_conn_ctx.return_value = mock_conn
            result = execute_operator_manual_close(
                position_id=POSITION_ID,
                client_id=CLIENT_ID,
                true_fill_price=1.05,
            )
        assert result["ok"] is True
        cancel_mock.assert_called_once_with(mock_conn, CLIENT_ID, CONTRACT, "paper")

    def test_dry_run_proves_exact_projected_mutation(self):
        with patch("ap.operator_manual_close.run_with_retry") as mock_retry, \
             patch("ap.operator_manual_close.conn") as mock_conn_ctx, \
             patch("ap.operator_manual_close._audit_log_exists", return_value=True), \
             patch("ap.operator_manual_close._fetch_position", return_value=_open_position()), \
             patch("ap.operator_manual_close._derive_execution_mode", return_value="live"), \
             patch("ap.operator_manual_close._count_pending_exit_orders", return_value=3), \
             patch("ap.operator_manual_close._update_position") as update_mock:
            mock_retry.side_effect = lambda fn: fn()
            mock_conn = MagicMock()
            mock_conn.__enter__ = MagicMock(return_value=mock_conn)
            mock_conn.__exit__ = MagicMock(return_value=False)
            mock_conn_ctx.return_value = mock_conn
            result = execute_operator_manual_close(
                position_id=POSITION_ID,
                client_id=CLIENT_ID,
                true_fill_price=1.05,
                dry_run=True,
            )
        assert result["ok"] is True
        assert result["dry_run"] is True
        assert result["contract"] == CONTRACT
        assert result["execution_mode"] == "live"
        assert result["active_exit_orders"] == 3
        assert result["operator_audit_log_exists"] is True
        update_mock.assert_not_called()


def test_admin_auth_decorator_is_used_for_manual_close_endpoint():
    src = Path("/private/tmp/apb-pr-amend/app.py").read_text()
    pattern = re.compile(
        r'@app\.post\("/admin/operator/manual-close"\)\s+@_require_admin\s+def admin_operator_manual_close',
        re.MULTILINE,
    )
    assert pattern.search(src)
