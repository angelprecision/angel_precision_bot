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


def test_position_manager_snapshot_scopes_position_rows_and_capital_by_mode_structural_mock(monkeypatch):
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


# ---------------------------------------------------------------------------
# Structural/mock SQL coverage (not PostgreSQL-backed)
# These tests use fake cursors and verify the SQL shape/parameters passed by
# this code path. They do not establish PostgreSQL driver behavior.
# ---------------------------------------------------------------------------

def _make_structural_mock_conn_factory(rows_by_query: dict, executed: list):
    """
    Build a context-manager conn() factory that:
    - accepts SET TRANSACTION … silently
    - routes SELECT queries to rows_by_query keyed on a substring of the SQL
    - records every (sql, params) pair in `executed`
    """
    import contextlib

    class _FakeCursor:
        def __init__(self):
            self._rows = []

        def execute(self, sql, params=()):
            executed.append((sql.strip(), params))
            self._rows = []
            for key, rows in rows_by_query.items():
                if key in sql:
                    self._rows = rows
                    break

        def fetchall(self):
            return list(self._rows)

        def fetchone(self):
            return self._rows[0] if self._rows else None

    @contextlib.contextmanager
    def _conn_ctx():
        yield _FakeCursor()

    return _conn_ctx


def _standard_broker_trades_stub():
    return {
        "trades_today": 0,
        "trades_today_source": "test",
        "synthetic_position_rows_ignored": 0,
        "null_mode_fills_ignored": 0,
        "wrong_mode_fills_ignored": 0,
        "missing_identity_fills_ignored": 0,
        "trade_count_query_status": "ok",
    }


def test_live_snapshot_sees_only_live_open_positions_structural_mock(monkeypatch):
    """LIVE runner snapshot: only LIVE open positions are returned; PAPER ignored."""
    executed = []
    rows_by_query = {
        # active positions — return one LIVE row, one PAPER row for the DB
        # but the SQL itself must filter by mode so only LIVE comes back
        "SELECT *": [
            {
                "id": "live-pos-1",
                "status": "OPEN",
                "execution_mode": "live",
                "entry_price": 1.00,
                "quantity": 1,
                "quantity_remaining": 1,
                "capital_allocated": 100.0,
                "symbol": "SPY260919C00560000",
                "underlying": "SPY",
                "direction": "CALL",
                "meta": {},
            }
        ],
        # terminal-order counts
        "SELECT id, status, quantity_remaining": [],
        # realized P&L
        "COALESCE(SUM(realized_pnl)": [{"total": 0.0}],
    }

    import ap.position_manager as position_manager

    monkeypatch.setattr(position_manager, "conn", _make_structural_mock_conn_factory(rows_by_query, executed))
    monkeypatch.setattr(position_manager, "run_with_retry", lambda fn, **_: fn())
    monkeypatch.setattr(
        position_manager,
        "_broker_confirmed_entry_trades_today",
        lambda *a, **kw: _standard_broker_trades_stub(),
    )

    manager = position_manager.APPositionManager("client@example.com")
    manager._market_day_bounds_utc = lambda: ("start", "end", "2026-09-02")

    snapshot = manager.snapshot(mode="live")

    # Snapshot returns the one LIVE position
    assert snapshot["open_count"] == 1
    assert snapshot["open_position_ids"] == ["live-pos-1"]

    # Every positions SELECT must carry the mode predicate and param
    positions_queries = [
        (sql, params)
        for sql, params in executed
        if "LOWER(TRIM(COALESCE(execution_mode" in sql
    ]
    assert positions_queries, "Expected at least one mode-scoped positions query"
    for sql, params in positions_queries:
        assert "LOWER(TRIM(COALESCE(execution_mode, ''))) = %s" in sql, (
            f"Mode predicate missing from positions query:\n{sql}"
        )
        assert "live" in params, (
            f"Mode param 'live' missing from params: {params}"
        )


def test_paper_snapshot_sees_only_paper_open_positions_structural_mock(monkeypatch):
    """PAPER runner snapshot: only PAPER open positions returned; LIVE ignored."""
    executed = []
    rows_by_query = {
        "SELECT *": [
            {
                "id": "paper-pos-1",
                "status": "OPEN",
                "execution_mode": "paper",
                "entry_price": 2.00,
                "quantity": 2,
                "quantity_remaining": 2,
                "capital_allocated": 200.0,
                "symbol": "QQQ260919C00480000",
                "underlying": "QQQ",
                "direction": "CALL",
                "meta": {},
            }
        ],
        "SELECT id, status, quantity_remaining": [],
        "COALESCE(SUM(realized_pnl)": [{"total": 5.0}],
    }

    import ap.position_manager as position_manager

    monkeypatch.setattr(position_manager, "conn", _make_structural_mock_conn_factory(rows_by_query, executed))
    monkeypatch.setattr(position_manager, "run_with_retry", lambda fn, **_: fn())
    monkeypatch.setattr(
        position_manager,
        "_broker_confirmed_entry_trades_today",
        lambda *a, **kw: _standard_broker_trades_stub(),
    )

    manager = position_manager.APPositionManager("client@example.com")
    manager._market_day_bounds_utc = lambda: ("start", "end", "2026-09-02")

    snapshot = manager.snapshot(mode="paper")

    assert snapshot["open_count"] == 1
    assert snapshot["open_position_ids"] == ["paper-pos-1"]

    for sql, params in executed:
        if "FROM positions" in sql or "COALESCE(SUM(realized_pnl)" in sql:
            assert "LOWER(TRIM(COALESCE(execution_mode, ''))) = %s" in sql, (
                f"Mode predicate missing:\n{sql}"
            )
            assert "paper" in params, f"Mode param 'paper' missing: {params}"


def test_capital_and_pnl_stay_mode_scoped_structural_mock(monkeypatch):
    """capital_deployed and realized_pnl_today are derived from mode-scoped queries only."""
    executed = []
    rows_by_query = {
        "SELECT *": [
            {
                "id": "live-pos-A",
                "status": "OPEN",
                "execution_mode": "live",
                "entry_price": 3.50,
                "quantity": 4,
                "quantity_remaining": 4,
                "capital_allocated": 350.0,
                "symbol": "SPY260919C00570000",
                "underlying": "SPY",
                "direction": "CALL",
                "meta": {},
            }
        ],
        "SELECT id, status, quantity_remaining": [],
        "COALESCE(SUM(realized_pnl)": [{"realized_pnl_today": 42.0, "capital_deployed": 350.0}],
    }

    import ap.position_manager as position_manager

    monkeypatch.setattr(position_manager, "conn", _make_structural_mock_conn_factory(rows_by_query, executed))
    monkeypatch.setattr(position_manager, "run_with_retry", lambda fn, **_: fn())
    monkeypatch.setattr(
        position_manager,
        "_broker_confirmed_entry_trades_today",
        lambda *a, **kw: _standard_broker_trades_stub(),
    )

    manager = position_manager.APPositionManager("client@example.com")
    manager._market_day_bounds_utc = lambda: ("start", "end", "2026-09-02")

    snapshot = manager.snapshot(mode="live")

    assert snapshot["capital_deployed"] == 350.0
    assert snapshot["realized_pnl_today"] == 42.0

    # Confirm P&L query is also mode-scoped
    pnl_queries = [(s, p) for s, p in executed if "COALESCE(SUM(realized_pnl)" in s]
    assert pnl_queries, "Expected at least one realized_pnl query"
    for sql, params in pnl_queries:
        assert "LOWER(TRIM(COALESCE(execution_mode, ''))) = %s" in sql
        assert "live" in params


def test_invalid_mode_raises_before_any_snapshot_read_structural_mock(monkeypatch):
    """snapshot(mode=None/invalid) must raise before touching any DB query."""
    executed = []

    import ap.position_manager as position_manager
    import contextlib

    class _SentinelCursor:
        def execute(self, sql, params=()):
            executed.append((sql, params))

        def fetchall(self):
            return []

        def fetchone(self):
            return None

    @contextlib.contextmanager
    def _sentinel_conn():
        yield _SentinelCursor()

    monkeypatch.setattr(position_manager, "conn", _sentinel_conn)
    monkeypatch.setattr(position_manager, "run_with_retry", lambda fn, **_: fn())

    manager = position_manager.APPositionManager("client@example.com")
    manager._market_day_bounds_utc = lambda: ("start", "end", "2026-09-02")

    import pytest as _pytest

    # None mode — must raise, zero DB reads
    executed.clear()
    with _pytest.raises((ValueError, RuntimeError)):
        manager.snapshot(mode=None)

    data_reads = [(s, p) for s, p in executed if "FROM positions" in s or "COALESCE" in s]
    assert not data_reads, (
        f"snapshot(mode=None) made {len(data_reads)} data read(s) before raising: {data_reads}"
    )

    # Invalid string mode
    executed.clear()
    with _pytest.raises((ValueError, RuntimeError)):
        manager.snapshot(mode="staging")

    data_reads = [(s, p) for s, p in executed if "FROM positions" in s or "COALESCE" in s]
    assert not data_reads, (
        f"snapshot(mode='staging') made {len(data_reads)} data read(s) before raising: {data_reads}"
    )
