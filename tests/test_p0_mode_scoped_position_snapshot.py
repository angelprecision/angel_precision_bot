from __future__ import annotations

from contextlib import contextmanager
import threading

import pytest

from ap_execution_core import APExecutionCore


class _RecordingPositionManager:
    def __init__(self, *, result=None, error=None):
        self.calls = []
        self.result = result or {"open_count": 2, "pending_entries": 3}
        self.error = error

    def snapshot(self, *, mode):
        self.calls.append(mode)
        if self.error is not None:
            raise self.error
        return dict(self.result)


def _core(position_manager, *, mode="LIVE", execution_mode="live"):
    core = object.__new__(APExecutionCore)
    core.mode = mode
    core.execution_mode = execution_mode
    core.email = "client@example.com"
    core.position_manager = position_manager
    core._pos_lock = threading.Lock()
    core._position_count = 99
    core._max_positions = 7
    return core


@pytest.mark.parametrize(
    ("mode", "execution_mode", "expected"),
    [
        ("LIVE", "live", "live"),
        (" live ", " LIVE ", "live"),
        ("PAPER", "paper", "paper"),
    ],
)
def test_position_counts_use_explicit_canonical_mode(mode, execution_mode, expected):
    manager = _RecordingPositionManager()
    core = _core(manager, mode=mode, execution_mode=execution_mode)

    assert core._current_open_position_count() == 2
    assert core._current_pending_entry_count() == 3
    assert manager.calls == [expected, expected]


def test_successful_scoped_snapshot_wins_over_local_fallback():
    manager = _RecordingPositionManager(
        result={"open_count": 0, "pending_entries": 0}
    )
    core = _core(manager)

    assert core._current_open_position_count() == 0
    assert core._current_pending_entry_count() == 0
    assert manager.calls == ["live", "live"]


def test_real_scoped_snapshot_failure_preserves_existing_fallback():
    manager = _RecordingPositionManager(error=RuntimeError("database unavailable"))
    core = _core(manager)

    assert core._current_open_position_count() == 99
    assert core._current_pending_entry_count() == 0
    assert manager.calls == ["live", "live"]


@pytest.mark.parametrize(
    ("mode", "execution_mode"),
    [
        ("", ""),
        ("staging", "staging"),
        ("LIVE", "staging"),
        ("LIVE", "paper"),
    ],
)
def test_invalid_or_conflicting_identity_never_calls_unscoped_snapshot(
    mode, execution_mode
):
    manager = _RecordingPositionManager()
    core = _core(manager, mode=mode, execution_mode=execution_mode)

    # Invalid identity fails closed at the capacity gate; it does not invent
    # PAPER/LIVE and it does not fall back to an unscoped database read.
    assert core._current_open_position_count() == 7
    assert manager.calls == []

    # The pending-count helper also performs no read for invalid identity.
    assert core._current_pending_entry_count() == 0
    assert manager.calls == []


def test_position_manager_snapshot_scopes_position_rows_and_capital_by_mode(monkeypatch):
    import ap.position_manager as position_manager

    executed = []
    active_rows = [
        {
            "id": "live-position",
            "status": "OPEN",
            "underlying": "SPY",
            "direction": "CALL",
            "execution_mode": "live",
            "entry_ts": "2026-08-31T15:00:00+00:00",
            "created_at": "2026-08-31T15:00:00+00:00",
        },
        {
            "id": "paper-position",
            "status": "OPEN",
            "underlying": "QQQ",
            "direction": "PUT",
            "execution_mode": "paper",
            "entry_ts": "2026-08-31T14:00:00+00:00",
            "created_at": "2026-08-31T14:00:00+00:00",
        },
    ]

    class Cursor:
        def __init__(self):
            self.rows = []
            self.last_sql = ""

        def execute(self, sql, params=()):
            self.last_sql = " ".join(str(sql).split())
            params = tuple(params)
            executed.append((self.last_sql, params))

            if self.last_sql.startswith("SELECT * FROM positions"):
                mode = params[-1] if "LOWER(TRIM(COALESCE(execution_mode" in self.last_sql else None
                self.rows = [
                    row for row in active_rows
                    if mode is None or row["execution_mode"] == mode
                ]
            elif self.last_sql.startswith("SELECT id, status, quantity_remaining"):
                self.rows = []
            elif "COALESCE(SUM(realized_pnl)" in self.last_sql:
                mode = params[-1] if "LOWER(TRIM(COALESCE(execution_mode" in self.last_sql else None
                self.rows = [{
                    "realized_pnl_today": 11.0 if mode == "live" else 22.0,
                    "capital_deployed": 125.0 if mode == "live" else 225.0,
                }]
            elif self.last_sql.startswith("SELECT COUNT(*) AS n FROM orders WHERE client_id = %s AND status IN"):
                self.rows = [{"n": 0}]
            elif self.last_sql.startswith("SELECT COUNT(*) FILTER"):
                self.rows = [{
                    "n": 0,
                    "calls_unreconciled": 0,
                    "puts_unreconciled": 0,
                    "orphan_null_mode": 0,
                    "orphan_wrong_mode": 0,
                }]
            elif self.last_sql.startswith("SELECT COUNT(*) AS n, COALESCE(SUM"):
                self.rows = [{"n": 0, "reserved": 0.0}]
            elif "SELECT COUNT(*) AS n FROM orders WHERE client_id = %s AND kind = 'ENTRY'" in self.last_sql:
                self.rows = [{"n": 0}]
            elif "SELECT COUNT(*) AS n FROM orders WHERE client_id=%s AND kind='EXIT'" in self.last_sql:
                self.rows = [{"n": 0}]
            elif self.last_sql.startswith("SELECT COALESCE(SUM("):
                self.rows = [{"cap": 0.0}]
            elif self.last_sql.startswith("SELECT id, position_id, broker_order_id"):
                self.rows = []
            else:
                self.rows = []
            return self

        def fetchall(self):
            return list(self.rows)

        def fetchone(self):
            return dict(self.rows[0]) if self.rows else None

    cursor = Cursor()

    @contextmanager
    def fake_conn():
        yield cursor

    monkeypatch.setattr(position_manager, "conn", fake_conn)
    monkeypatch.setattr(position_manager, "run_with_retry", lambda fn, **_: fn())
    monkeypatch.setattr(
        position_manager,
        "_broker_confirmed_entry_trades_today",
        lambda *args, **kwargs: {
            "trades_today": 0,
            "trades_today_source": "test",
            "synthetic_position_rows_ignored": 0,
            "null_mode_fills_ignored": 0,
            "wrong_mode_fills_ignored": 0,
            "missing_identity_fills_ignored": 0,
            "trade_count_query_status": "ok",
        },
    )

    manager = position_manager.APPositionManager("client@example.com")
    manager._market_day_bounds_utc = lambda: ("start", "end", "2026-08-31")

    snapshot = manager.snapshot(mode="live")

    assert snapshot["open_count"] == 1
    assert snapshot["open_position_ids"] == ["live-position"]
    assert snapshot["capital_deployed"] == 125.0
    assert snapshot["realized_pnl_today"] == 11.0

    active_sql, active_params = next(
        (sql, params)
        for sql, params in executed
        if sql.startswith("SELECT * FROM positions")
    )
    assert "LOWER(TRIM(COALESCE(execution_mode, ''))) = %s" in active_sql
    assert active_params == ("client@example.com", "live")

    terminal_sql, terminal_params = next(
        (sql, params)
        for sql, params in executed
        if sql.startswith("SELECT id, status, quantity_remaining")
    )
    assert "LOWER(TRIM(COALESCE(execution_mode, ''))) = %s" in terminal_sql
    assert terminal_params == ("client@example.com", "live")

    summary_sql, summary_params = next(
        (sql, params)
        for sql, params in executed
        if "COALESCE(SUM(realized_pnl)" in sql
    )
    assert "LOWER(TRIM(COALESCE(execution_mode, ''))) = %s" in summary_sql
    assert summary_params == ("start", "end", "client@example.com", "live")
