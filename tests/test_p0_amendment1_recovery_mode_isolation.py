"""P0 tests for Amendment §1: exact execution_mode scoping in deferred breach recovery.

Proves the required matrix from PR #323 amendment §1:

  * paper recovery cannot load or mutate LIVE rows
  * LIVE recovery cannot load or mutate paper rows
  * blank execution mode is rejected
  * malformed execution mode is rejected
  * matching client but wrong mode cannot be claimed
  * matching mode but wrong client cannot be claimed

Also proves:

  * runner mode is resolved once at the top and unknown mode short-circuits
  * OSM.client_id mismatch short-circuits before any DB call
  * SQL query includes the canonical column/meta execution-mode fallback,
    contradiction fence, and binds the runner mode as its second parameter
  * `_build_recovery_plan_from_order` no longer infers execution_mode from
    the runner (Amendment §1 fail-closed rule)

The tests exercise the *real* `_recover_deferred_breach_lifecycles` code
path with `conn` and `run_with_retry` monkey-patched, mirroring the
convention used by `test_p0_deferred_breach_lifecycle_completion.py`.
"""

from __future__ import annotations

import types
from unittest.mock import MagicMock

import pytest

import ap_recovery
from ap_recovery import APStartupRecovery
from ap import db as ap_db


# ─────────────────────────── DB spy fixtures ────────────────────────────
#
# Minimal cursor / conn recorders so we can prove the executed SQL and
# bound parameters without touching Postgres.  Rows returned from
# `_load()` are supplied by the test via the `rows` list.
# ────────────────────────────────────────────────────────────────────────


class _Cursor:
    def __init__(self, sink, rows):
        self.sink = sink
        self._rows = rows

    def execute(self, sql, params=()):
        self.sink.append((" ".join(str(sql).split()), tuple(params)))
        return self

    def fetchall(self):
        return list(self._rows)


class _Conn:
    def __init__(self, sink, rows):
        self.cursor = _Cursor(sink, rows)

    def __enter__(self):
        return self.cursor

    def __exit__(self, *_):
        return False


@pytest.fixture
def db_spy(monkeypatch):
    sink = []
    state = {"rows": []}
    monkeypatch.setattr(
        ap_db,
        "conn",
        lambda: _Conn(sink, state["rows"]),
    )
    monkeypatch.setattr(ap_db, "run_with_retry", lambda fn, *a, **k: fn())
    return sink, state


# ───────────────────────── Recovery construction ────────────────────────


def _make_recovery(client_id="jason-live", mode="LIVE"):
    """Build APStartupRecovery with MagicMock collaborators, OSM.client_id
    aligned to the recovery client_id so tests can toggle only the vars
    they care about."""
    osm = MagicMock()
    osm.client_id = client_id.strip().lower()
    # Give the OSM methods realistic return shapes
    osm.terminalize_deferred_breach = MagicMock(return_value=True)
    osm.submit_existing_entry = MagicMock(return_value={"ok": True})

    pm = MagicMock()
    mc = MagicMock()
    mc.mode = mode  # what `_execution_mode()` reads
    broker = MagicMock()

    watcher = MagicMock()
    watcher.has_order = MagicMock(return_value=False)
    watcher.watch = MagicMock(return_value=True)

    rec = APStartupRecovery(
        client_id=client_id,
        broker=broker,
        osm=osm,
        pm=pm,
        master_control=mc,
        exit_engine=None,
        entry_watcher=watcher,
    )
    return rec, osm, watcher


def _row(**overrides):
    """Minimum row shape produced by the SQL SELECT.  Reasonable defaults
    give a PENDING_TRIGGER row that would normally be picked up for
    rearm."""
    base = {
        "local_order_id": "loc-1",
        "client_id": "jason-live",
        "signal_id": "sig-1",
        "plan_id": "plan-1",
        "symbol": "AAPL",
        "contract": "AAPL240119C00200000",
        "direction": "CALL",
        "score": 70.0,
        "tier": "A",
        "trigger_price": 200.0,
        "stop_underlying": 195.0,
        "target_underlying": 210.0,
        "pattern": "3-1-2",
        "timeframe": "5m",
        "execution_mode": "LIVE",
        "qty": 1,
        "limit_price": 2.10,
        "reserved_cost": 210.0,
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "submitted_ts": None,
        "meta": {},
        "created_ts": "2026-07-10T12:00:00+00:00",
    }
    base.update(overrides)
    return base


# ═══════════════════════════════════════════════════════════════════════
# SQL predicate and bindings
# ═══════════════════════════════════════════════════════════════════════


def test_sql_predicate_includes_execution_mode_scoping(db_spy):
    """The SELECT must use the canonical mode fallback and contradiction
    fence, then bind the runner mode (lowercase) as the second parameter."""
    sink, state = db_spy
    rec, _, _ = _make_recovery(client_id="jason-live", mode="LIVE")
    state["rows"] = []  # no rows needed to inspect the SQL

    result = {"errors": []}
    rec._recover_deferred_breach_lifecycles(result)

    assert sink, "expected exactly one SQL execute"
    sql, params = sink[0]
    assert "LOWER(BTRIM(COALESCE(NULLIF(BTRIM(execution_mode), ''), meta->>'execution_mode', '')))" in sql
    assert "NULLIF(BTRIM(meta->>'execution_mode'), '') IS NULL" in sql
    assert "LOWER(BTRIM(execution_mode)) = LOWER(BTRIM(meta->>'execution_mode'))" in sql
    assert params == ("jason-live", "live")


def test_paper_runner_binds_paper_mode(db_spy):
    """Paper runner must bind 'paper' — proves runner mode resolution."""
    sink, state = db_spy
    rec, _, _ = _make_recovery(client_id="jason-live", mode="PAPER")
    state["rows"] = []
    rec._recover_deferred_breach_lifecycles({"errors": []})
    _, params = sink[0]
    assert params == ("jason-live", "paper")


# ═══════════════════════════════════════════════════════════════════════
# Runner mode / OSM identity short-circuits
# ═══════════════════════════════════════════════════════════════════════


def test_unknown_runner_mode_short_circuits_without_db_call(db_spy):
    """Amendment §1: unknown/blank runner mode records
    `recovery_unknown_execution_mode` and returns before any SQL."""
    sink, state = db_spy
    rec, _, _ = _make_recovery(client_id="jason-live", mode="")
    result = {"errors": []}
    rec._recover_deferred_breach_lifecycles(result)
    assert sink == [], "no DB call may occur when runner mode is unknown"
    assert "recovery_unknown_execution_mode" in result["errors"]


def test_malformed_runner_mode_short_circuits(db_spy):
    """Malformed runner mode (e.g. 'live-ish') is rejected identically to
    blank — no DB call, error surfaced."""
    sink, state = db_spy
    rec, _, _ = _make_recovery(client_id="jason-live", mode="live-ish")
    result = {"errors": []}
    rec._recover_deferred_breach_lifecycles(result)
    assert sink == []
    assert "recovery_unknown_execution_mode" in result["errors"]


def test_osm_client_id_mismatch_short_circuits(db_spy, monkeypatch):
    """Amendment §1: if the OSM was constructed for a different client
    than the recovery pass, no DB call may occur — critical config bug."""
    sink, state = db_spy
    rec, osm, _ = _make_recovery(client_id="jason-live", mode="LIVE")
    osm.client_id = "someone-else"  # simulate misconfigured OSM

    result = {"errors": []}
    rec._recover_deferred_breach_lifecycles(result)

    assert sink == []
    assert "recovery_osm_client_id_mismatch" in result["errors"]
    osm.terminalize_deferred_breach.assert_not_called()
    osm.submit_existing_entry.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════
# Cross-mode rows: SKIP (never mutate someone else's row)
# ═══════════════════════════════════════════════════════════════════════


def test_paper_runner_skips_live_row_that_slipped_through(db_spy):
    """A LIVE row that (defensively) slipped past the SQL predicate must
    be SKIPPED, not terminalized, not rearmed, not submitted."""
    sink, state = db_spy
    rec, osm, watcher = _make_recovery(client_id="jason-live", mode="PAPER")
    # Row belongs to LIVE, would normally trigger rearm, would normally
    # be filtered by SQL — we shove it in to verify the Python check.
    state["rows"] = [_row(execution_mode="LIVE")]

    result = {"errors": []}
    rec._recover_deferred_breach_lifecycles(result)

    watcher.watch.assert_not_called()
    osm.terminalize_deferred_breach.assert_not_called()
    osm.submit_existing_entry.assert_not_called()
    assert result.get("deferred_lifecycles_recovered", 0) == 0


def test_live_runner_skips_paper_row_that_slipped_through(db_spy):
    """Symmetric: LIVE runner must skip a defensively-loaded PAPER row."""
    sink, state = db_spy
    rec, osm, watcher = _make_recovery(client_id="jason-live", mode="LIVE")
    state["rows"] = [_row(execution_mode="paper")]

    result = {"errors": []}
    rec._recover_deferred_breach_lifecycles(result)

    watcher.watch.assert_not_called()
    osm.terminalize_deferred_breach.assert_not_called()
    osm.submit_existing_entry.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════
# Blank / malformed row execution_mode
# ═══════════════════════════════════════════════════════════════════════


def test_blank_row_execution_mode_is_skipped_never_mutated(db_spy):
    """A row with a blank execution_mode is quarantined via skip.  We
    prefer skip over terminalize because a blank mode has unknown
    provenance and terminalizing could destroy a row that actually
    belongs to a different mode."""
    sink, state = db_spy
    rec, osm, watcher = _make_recovery(client_id="jason-live", mode="LIVE")
    state["rows"] = [_row(execution_mode="")]

    result = {"errors": []}
    rec._recover_deferred_breach_lifecycles(result)

    watcher.watch.assert_not_called()
    osm.terminalize_deferred_breach.assert_not_called()
    osm.submit_existing_entry.assert_not_called()


def test_malformed_row_execution_mode_is_skipped(db_spy):
    """Malformed persisted execution_mode (e.g. 'papr') is quarantined."""
    sink, state = db_spy
    rec, osm, watcher = _make_recovery(client_id="jason-live", mode="LIVE")
    state["rows"] = [_row(execution_mode="papr")]

    result = {"errors": []}
    rec._recover_deferred_breach_lifecycles(result)

    watcher.watch.assert_not_called()
    osm.terminalize_deferred_breach.assert_not_called()
    osm.submit_existing_entry.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════
# Matching mode, wrong client
# ═══════════════════════════════════════════════════════════════════════


def test_matching_mode_wrong_client_is_skipped(db_spy):
    """A row with matching execution_mode but foreign client_id (SQL
    should have filtered it but defence in depth) is SKIPPED — never
    terminalized, never rearmed."""
    sink, state = db_spy
    rec, osm, watcher = _make_recovery(client_id="jason-live", mode="LIVE")
    state["rows"] = [_row(client_id="other-client", execution_mode="LIVE")]

    result = {"errors": []}
    rec._recover_deferred_breach_lifecycles(result)

    watcher.watch.assert_not_called()
    osm.terminalize_deferred_breach.assert_not_called()
    osm.submit_existing_entry.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════
# Happy path — matching client + mode still works
# ═══════════════════════════════════════════════════════════════════════


def test_matching_mode_and_client_row_is_processed_normally(db_spy):
    """Sanity: with matching client and mode the pre-existing rearm
    behaviour is preserved (Amendment §1 is additive-only for compliant
    rows)."""
    sink, state = db_spy
    rec, osm, watcher = _make_recovery(client_id="jason-live", mode="LIVE")
    # Row triggers the "orphan waiting_for_trigger" rearm branch
    state["rows"] = [_row(
        client_id="jason-live",
        execution_mode="LIVE",
        meta={"materialization_status": "WAITING_FOR_TRIGGER"},
    )]

    result = {"errors": []}
    rec._recover_deferred_breach_lifecycles(result)

    watcher.watch.assert_called_once()
    osm.terminalize_deferred_breach.assert_not_called()
    assert result.get("deferred_lifecycles_recovered", 0) == 1


# ═══════════════════════════════════════════════════════════════════════
# Plan builder no-runner-infer proof
# ═══════════════════════════════════════════════════════════════════════


def test_plan_builder_never_infers_execution_mode_from_runner():
    """Amendment §1 explicit rule: `_build_recovery_plan_from_order`
    must not fall back to `self._execution_mode()` when the order's
    execution_mode is blank.  The resulting plan carries a blank mode,
    which the caller's identity proof rejects."""
    rec, _, _ = _make_recovery(client_id="jason-live", mode="LIVE")

    plan = rec._build_recovery_plan_from_order({
        "local_order_id": "loc-x",
        "client_id": "jason-live",
        "signal_id": "sig-x",
        "plan_id": "plan-x",
        "symbol": "AAPL",
        "contract": "AAPL240119C00200000",
        "direction": "CALL",
        "execution_mode": "",       # ← blank persisted mode
        "trigger_price": 200.0,
        "stop_underlying": 195.0,
        "target_underlying": 210.0,
        "qty": 1,
        "limit_price": 2.10,
        "reserved_cost": 210.0,
        "meta": {},                 # meta.execution_mode also absent
    })

    assert plan is not None, "plan builder should still return a plan object"
    assert plan.execution_mode == "", (
        "plan.execution_mode must NOT be inferred from the runner "
        "when the persisted row has a blank mode "
        f"(got {plan.execution_mode!r})"
    )


def test_plan_from_row_with_valid_mode_lowercases_it():
    """The plan carries the row's mode, lowercased.  This preserves the
    downstream contract that plan.execution_mode is lowercase."""
    rec, _, _ = _make_recovery(client_id="jason-live", mode="PAPER")

    plan = rec._build_recovery_plan_from_order({
        "local_order_id": "loc-y",
        "client_id": "jason-live",
        "signal_id": "sig-y",
        "plan_id": "plan-y",
        "symbol": "MSFT",
        "contract": "MSFT240119C00400000",
        "direction": "CALL",
        "execution_mode": "  LIVE  ",  # whitespace and case
        "trigger_price": 400.0,
        "qty": 1,
        "limit_price": 2.0,
        "reserved_cost": 200.0,
        "meta": {},
    })

    assert plan is not None
    assert plan.execution_mode == "live"


def test_plan_from_row_with_blank_column_uses_valid_metadata_mode():
    """Legacy rows may use metadata when the top-level mode is blank."""
    rec, _, _ = _make_recovery(client_id="jason-live", mode="PAPER")

    plan = rec._build_recovery_plan_from_order({
        "local_order_id": "loc-z",
        "client_id": "jason-live",
        "signal_id": "sig-z",
        "plan_id": "plan-z",
        "symbol": "MSFT",
        "contract": "MSFT240119C00400000",
        "direction": "CALL",
        "execution_mode": "",
        "trigger_price": 400.0,
        "qty": 1,
        "limit_price": 2.0,
        "reserved_cost": 200.0,
        "meta": {"execution_mode": "paper"},
    })

    assert plan is not None
    assert plan.execution_mode == "paper"


def test_plan_from_row_with_conflicting_mode_mirrors_is_rejected():
    """A contradictory durable identity must not become a recovery plan."""
    rec, _, _ = _make_recovery(client_id="jason-live", mode="LIVE")

    plan = rec._build_recovery_plan_from_order({
        "local_order_id": "loc-conflict",
        "client_id": "jason-live",
        "signal_id": "sig-conflict",
        "plan_id": "plan-conflict",
        "symbol": "MSFT",
        "contract": "MSFT240119C00400000",
        "direction": "CALL",
        "execution_mode": "LIVE",
        "trigger_price": 400.0,
        "qty": 1,
        "limit_price": 2.0,
        "reserved_cost": 200.0,
        "meta": {"execution_mode": "paper"},
    })

    assert plan is None
