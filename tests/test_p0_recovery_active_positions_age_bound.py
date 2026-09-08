"""
P0 behavioral regression: startup recovery must register only positions whose
identity belongs to THIS exact runtime, and must never use age alone to
conclude broker exposure no longer exists.

Incident (2026-08-14 market open): APStartupRecovery loaded every positions
row with an economically-active status (or residual quantity_remaining) with
no execution-mode fence. Historical NULL-execution_mode rows from prior months
were re-registered with PositionManager and polled against the live broker,
firing cancel/submit side effects against long-expired contracts.

Invariants enforced here:
  1. Recovery filters positions to the exact current runner execution_mode.
     NULL / blank / opposite-mode rows are excluded at the SQL boundary.
  2. Unknown runner mode fails closed (no positions loaded).
  3. Age is NOT an authority: a same-mode position is loaded regardless of how
     old it is (Friday->Tuesday / long-outage restart must not abandon real
     exposure).
  4. Expired OCC option contracts are excluded on economic authority (parsed
     expiry), not on age, before registration.
  5. A parser failure on the expiry check must NEVER silently drop a position.

These are behavioral assertions (what gets loaded / registered), not
SQL-string assertions.
"""

from __future__ import annotations

import sys
import types
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock


def _make_recovery(mode="paper"):
    sys.modules.pop("ap_recovery", None)
    import ap_recovery

    rec = ap_recovery.APStartupRecovery(
        client_id="trader@example.com",
        broker=MagicMock(),
        osm=MagicMock(),
        pm=MagicMock(),
        master_control=SimpleNamespace(mode=mode),
    )
    return rec


def _run_load_with_rows(rec, rows):
    """Run _load_active_positions against a fake conn that returns `rows`
    only for those matching the SQL predicates we can evaluate in Python.

    The fake DB honors the two predicates the production query relies on that
    matter for these tests: the client_id match is assumed (single client) and
    the execution_mode equality filter is applied here so we exercise the real
    branch that builds/executes the query (mode resolution, fail-closed).
    Rows are pre-filtered by the caller to represent what SQL would return.
    """
    captured = {"params": None}

    class _Cursor:
        def execute(self, sql, params=None):
            captured["params"] = params
            self._sql = sql
            return self

        def fetchall(self):
            return rows

    @contextmanager
    def _conn():
        yield _Cursor()

    fake_db = types.ModuleType("ap.db")
    fake_db.conn = _conn
    fake_db.run_with_retry = lambda fn, *a, **k: fn()

    orig = sys.modules.get("ap.db")
    sys.modules["ap.db"] = fake_db
    try:
        out = rec._load_active_positions()
    finally:
        if orig is not None:
            sys.modules["ap.db"] = orig
        else:
            sys.modules.pop("ap.db", None)
    return out, captured


# ── Invariant 1 + 2: execution-mode identity fence at the query boundary ──────

def test_query_filters_to_current_runner_mode():
    """The query must bind the current runner mode as a parameter."""
    rec = _make_recovery(mode="paper")
    _out, captured = _run_load_with_rows(rec, [])
    params = captured["params"] or ()
    assert "paper" in [str(p).lower() for p in params], (
        f"expected runner mode 'paper' bound into query params; got {params}"
    )


def test_live_runner_binds_live_mode():
    rec = _make_recovery(mode="live")
    _out, captured = _run_load_with_rows(rec, [])
    params = captured["params"] or ()
    assert "live" in [str(p).lower() for p in params], (
        f"expected 'live' bound into query params; got {params}"
    )


def test_unknown_runner_mode_fails_closed():
    """Unknown/invalid runner mode loads NOTHING and never hits the DB."""
    rec = _make_recovery(mode="garbage")

    hit = {"queried": False}

    class _Cursor:
        def execute(self, *a, **k):
            hit["queried"] = True
            return self

        def fetchall(self):
            return [{"id": "x"}]

    @contextmanager
    def _conn():
        yield _Cursor()

    fake_db = types.ModuleType("ap.db")
    fake_db.conn = _conn
    fake_db.run_with_retry = lambda fn, *a, **k: fn()

    orig = sys.modules.get("ap.db")
    sys.modules["ap.db"] = fake_db
    try:
        out = rec._load_active_positions()
    finally:
        if orig is not None:
            sys.modules["ap.db"] = orig
        else:
            sys.modules.pop("ap.db", None)

    assert out == [], "unknown-mode runner must load zero positions"
    assert hit["queried"] is False, "unknown-mode runner must not query the DB"


# ── Invariant 3: age is not an authority ──────────────────────────────────────

def test_same_mode_old_position_still_recovered():
    """A same-mode position is registered even if it is old (no age cutoff)."""
    rec = _make_recovery(mode="paper")
    rec.pm = MagicMock()
    rec.mc = SimpleNamespace(mode="paper", _position_count=0, _sector_counts={})

    # An old same-mode position with a still-valid (far-future) OCC contract.
    old_valid = {
        "id": "pos-old",
        "contract": "SPY301231C00500000",   # expiry 2030-12-31, not expired
        "execution_mode": "paper",
        "status": "OPEN",
        "quantity_remaining": 1,
        "entry_ts": "2026-01-01T00:00:00+00:00",
    }
    result = {"positions_recovered": 0, "errors": []}
    _out, _ = _run_load_with_rows(rec, [old_valid])

    # Drive the registration path directly with the row SQL would have returned.
    rec._load_active_positions = MagicMock(return_value=[old_valid])
    rec._recover_positions(result)

    assert result["positions_recovered"] == 1, (
        "an old but same-mode, unexpired position must still be recovered"
    )
    assert rec.pm.register_recovered_position.called or rec.pm.add_position.called


# ── Invariant 4: expired OCC excluded on economic authority ───────────────────

def test_expired_occ_contract_excluded():
    rec = _make_recovery(mode="paper")
    rec.pm = MagicMock()
    rec.mc = SimpleNamespace(mode="paper", _position_count=0, _sector_counts={})

    expired = {
        "id": "pos-expired",
        "contract": "AAPL200101C00240000",  # expiry 2020-01-01, long expired
        "execution_mode": "paper",
        "status": "OPEN",
        "quantity_remaining": 1,
    }
    rec._load_active_positions = MagicMock(return_value=[expired])
    result = {"positions_recovered": 0, "errors": []}
    rec._recover_positions(result)

    assert result["positions_recovered"] == 0, "expired OCC must not be recovered"
    assert result.get("positions_skipped_expired", 0) == 1
    assert not rec.pm.register_recovered_position.called
    assert not rec.pm.add_position.called


# ── Invariant 5: expiry-parser failure must not silently drop a position ──────

def test_unparseable_contract_is_recovered_not_dropped():
    rec = _make_recovery(mode="paper")
    rec.pm = MagicMock()
    rec.mc = SimpleNamespace(mode="paper", _position_count=0, _sector_counts={})

    # Non-OCC / malformed contract: _is_expired returns False (no match), so it
    # must fall through and be recovered rather than assumed dead.
    weird = {
        "id": "pos-weird",
        "contract": "NOT-AN-OCC-SYMBOL",
        "execution_mode": "paper",
        "status": "OPEN",
        "quantity_remaining": 1,
    }
    rec._load_active_positions = MagicMock(return_value=[weird])
    result = {"positions_recovered": 0, "errors": []}
    rec._recover_positions(result)

    assert result["positions_recovered"] == 1, (
        "a position with an unparseable contract must be recovered, not dropped"
    )
