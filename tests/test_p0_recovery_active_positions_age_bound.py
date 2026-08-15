"""
P0 surgical regression: startup recovery must not re-register economically
stale historical positions.

Incident (2026-08-14 market open): APStartupRecovery._load_active_positions
pulled every positions row where status was OPEN/CLOSING/PARTIAL/ACTIVE or
quantity_remaining > 0, with NO lower bound on age. Positions from
May/June 2026 that were never cleanly closed (NULL execution_mode,
residual quantity_remaining) matched on every startup, were re-registered
with PositionManager, bumped the in-memory position count, and were then
polled against the live broker -- triggering cancel/submit side effects
against long-expired contracts.

Invariant enforced here (and ONLY this):
    _load_active_positions must bound recovered rows to a recent trading
    window. Rows whose entry/creation timestamp predate the configured
    RECOVERY_ACTIVE_POSITION_LOOKBACK_HOURS window must be excluded at the
    SQL boundary.

This test asserts the query carries an age lower-bound. It does NOT assert
anything about how the rest of recovery behaves -- that is out of scope.
"""

from __future__ import annotations

import sys
import types
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock


def _make_recovery():
    sys.modules.pop("ap_recovery", None)
    import ap_recovery

    rec = ap_recovery.APStartupRecovery(
        client_id="trader@example.com",
        broker=MagicMock(),
        osm=MagicMock(),
        pm=MagicMock(),
        master_control=SimpleNamespace(mode="paper"),
    )
    return rec


def _capture_active_positions_query():
    """Run _load_active_positions against a fake conn and capture the SQL."""
    captured = {"sql": None, "params": None}

    class _Cursor:
        def execute(self, sql, params=None):
            captured["sql"] = sql
            captured["params"] = params
            return self

        def fetchall(self):
            return []

    @contextmanager
    def _conn():
        yield _Cursor()

    fake_db = types.ModuleType("ap.db")
    fake_db.conn = _conn
    fake_db.run_with_retry = lambda fn, *a, **k: fn()

    import ap_recovery
    rec = _make_recovery()

    # _load_active_positions imports conn/run_with_retry from ap.db at call time.
    orig = sys.modules.get("ap.db")
    sys.modules["ap.db"] = fake_db
    try:
        rec._load_active_positions()
    finally:
        if orig is not None:
            sys.modules["ap.db"] = orig
        else:
            sys.modules.pop("ap.db", None)

    return captured


def test_active_positions_query_has_age_lower_bound():
    """The recovery query must bound recovered positions to a recent window."""
    captured = _capture_active_positions_query()
    sql = (captured["sql"] or "").lower()

    assert sql, "expected _load_active_positions to execute a query"

    # A lower time bound must appear on entry_ts / created_at. We accept any
    # of the conventional forms: an explicit >= comparison against a cutoff,
    # or a bound parameter. The point is that unbounded historical rows can
    # no longer match.
    has_lower_bound = (
        ("entry_ts" in sql and ">=" in sql)
        or ("created_at" in sql and ">=" in sql)
        or ("interval" in sql)  # e.g. NOW() - INTERVAL
    )

    assert has_lower_bound, (
        "recovery active-position query has no age lower bound; historical "
        "positions can be re-registered and polled against the live broker. "
        f"query was:\n{captured['sql']}"
    )
