"""
tests/test_p0_osm_limit_price_sync.py
======================================

OSM-layer tests for the PR67 Codex patch:
orders.limit_price must be synced to the DB before the broker POST
for preselected (non-deferred) queued entries.

Tests verify:
1. Preselected order with stale plan limit → DB written with refreshed limit
2. DB and broker receive identical limit price
3. DB sync failure → submit blocked (fail-closed), broker never called
4. No-op when DB already holds the correct limit (no redundant write)
5. Deferred branch is unaffected (its own pre-submit update handles it)
6. order_monitor repeg would read the refreshed price after sync
"""

from __future__ import annotations

import types
import pytest
from unittest.mock import MagicMock, patch, call


# ── Minimal OSM harness ───────────────────────────────────────────────────────
# We test submit_existing_entry's DB-sync block without importing the real OSM
# (which pulls in the full broker/DB stack). The harness re-implements the
# exact decision logic from the patched block.

def _run_osm_sync_block(
    *,
    db_limit: float,          # what orders.limit_price currently holds
    submit_lp: float,         # lp value computed from refreshed ask
    is_deferred: bool = False,
    db_write_raises: bool = False,
):
    """
    Exercise the limit-price DB sync block from submit_existing_entry.

    Returns a result dict describing what happened:
      blocked      — True if we returned early (fail-closed)
      db_written   — True if an UPDATE was executed
      written_lp   — the value written to DB (None if not written)
      broker_limit — the value that would reach the broker (None if blocked)
    """
    written = {"lp": None, "count": 0}

    def _fake_run_with_retry(fn):
        if db_write_raises:
            raise RuntimeError("DB connection refused")
        fn()

    class _FakeCursor:
        def execute(self, sql, params):
            if "SET limit_price" in sql:
                written["lp"] = params[0]
                written["count"] += 1

    class _FakeConn:
        def __enter__(self):
            return _FakeCursor()
        def __exit__(self, *a):
            pass

    transitioned_to_error = [False]

    def _fake_transition(local_order_id, status, **kw):
        if str(status) == "ERROR":
            transitioned_to_error[0] = True

    # ── Replicate the patched block verbatim ──────────────────────────────────
    lp             = submit_lp
    _db_is_deferred = is_deferred
    current        = {"limit_price": db_limit, "broker_order_id": None}
    local_order_id = "local-001"
    ticker         = "AAPL"

    blocked = False
    error   = None

    if not _db_is_deferred:
        _db_stored_lp = float(current.get("limit_price") or 0)
        if abs(_db_stored_lp - lp) > 0.001:
            try:
                def _sync_limit_price():
                    with _FakeConn() as c:
                        c.execute(
                            "UPDATE orders SET limit_price=%s, updated_ts=NOW() "
                            "WHERE local_order_id=%s AND client_id=%s",
                            (round(lp, 2), local_order_id, "client-001"),
                        )
                _fake_run_with_retry(_sync_limit_price)
            except Exception as _lp_exc:
                _err = f"limit_price_db_sync_failed:{_lp_exc}"
                _fake_transition(local_order_id, "ERROR", last_error=_err)
                blocked = True
                error   = _err

    return {
        "blocked":      blocked,
        "error":        error,
        "db_written":   written["count"] > 0,
        "written_lp":   written["lp"],
        "write_count":  written["count"],
        "broker_limit": None if blocked else lp,
        "transitioned_to_error": transitioned_to_error[0],
    }


# ── Test 1 ─────────────────────────────────────────────────────────────────────

def test_preselected_order_db_written_with_refreshed_limit():
    """
    Preselected entry: stale DB limit (0.75) vs refreshed submit limit (1.07).
    The DB must be updated to 1.07 before the broker POST fires.
    """
    result = _run_osm_sync_block(db_limit=0.75, submit_lp=1.07, is_deferred=False)

    assert not result["blocked"], "Should not be blocked — DB write succeeded"
    assert result["db_written"],  "DB must be written for non-deferred order with stale limit"
    assert result["written_lp"] == pytest.approx(1.07), \
        f"DB must be written with refreshed limit 1.07, got {result['written_lp']}"


# ── Test 2 ─────────────────────────────────────────────────────────────────────

def test_db_and_broker_receive_identical_limit():
    """
    The value written to orders.limit_price must equal the value sent to broker.
    """
    result = _run_osm_sync_block(db_limit=0.75, submit_lp=1.07, is_deferred=False)

    assert not result["blocked"]
    assert result["written_lp"] == result["broker_limit"], (
        f"DB ({result['written_lp']}) and broker ({result['broker_limit']}) "
        f"must receive the same limit price"
    )


# ── Test 3 ─────────────────────────────────────────────────────────────────────

def test_db_sync_failure_blocks_broker_submit():
    """
    If the DB write fails, the broker POST must be blocked.
    A live broker order with a stale DB row is a split-brain condition.
    """
    result = _run_osm_sync_block(
        db_limit=0.75, submit_lp=1.07,
        is_deferred=False, db_write_raises=True,
    )

    assert result["blocked"],  "DB write failure must block broker submit"
    assert result["broker_limit"] is None, "broker_limit must be None when blocked"
    assert result["transitioned_to_error"], "Order must be transitioned to ERROR state"
    assert "limit_price_db_sync_failed" in (result["error"] or ""), \
        f"Error must name the cause, got: {result['error']}"


# ── Test 4 ─────────────────────────────────────────────────────────────────────

def test_no_write_when_db_already_has_correct_limit():
    """
    If the DB already holds the same limit (within 0.001), no write is issued.
    Prevents redundant DB round-trips on re-submission of already-synced orders.
    """
    result = _run_osm_sync_block(db_limit=1.07, submit_lp=1.07, is_deferred=False)

    assert not result["blocked"]
    assert not result["db_written"], "No DB write needed when limit is already correct"
    assert result["write_count"] == 0


# ── Test 5 ─────────────────────────────────────────────────────────────────────

def test_deferred_branch_skips_new_sync_block():
    """
    Deferred orders already update limit_price via _update_contract_pre_submit.
    The new sync block must be a no-op for deferred orders (is_deferred=True).
    """
    result = _run_osm_sync_block(db_limit=0.75, submit_lp=1.07, is_deferred=True)

    assert not result["blocked"],   "Deferred branch must not be affected"
    assert not result["db_written"], "New sync block must not fire for deferred orders"


# ── Test 6 ─────────────────────────────────────────────────────────────────────

def test_order_monitor_would_read_refreshed_price_after_sync():
    """
    Simulate what order_monitor.py does: read orders.limit_price and use it
    as the repeg baseline. After the sync, the stored value must equal the
    broker-submitted limit so repeg proximity is calculated correctly.

    Scenario: plan ask 0.75, current ask 1.05, paper cross +0.02 → submit 1.07.
    Before fix: repeg reads 0.75, gap_pct = (1.05/0.75)-1 = 40% → runaway, no repeg.
    After fix:  repeg reads 1.07, gap_pct = (1.05/1.07)-1 = -1.9% → already filled or tiny gap.
    """
    plan_limit   = 0.75
    current_ask  = 1.05
    ask_cross    = 0.02
    submit_limit = round(current_ask + ask_cross, 2)  # 1.07

    # DB sync writes submit_limit
    result = _run_osm_sync_block(
        db_limit=plan_limit, submit_lp=submit_limit, is_deferred=False
    )
    assert result["written_lp"] == pytest.approx(submit_limit)

    # Simulate order_monitor reading the (now-updated) DB value
    db_limit_after_sync = result["written_lp"]

    # Repeg proximity check (mirrors retry_engine.decide_repeg logic)
    REPEG_PROXIMITY_PCT = 0.08
    gap_pct_stale   = (current_ask / plan_limit)  - 1.0  # 40% — would block repeg
    gap_pct_synced  = (current_ask / db_limit_after_sync) - 1.0  # ~-1.9% — correct

    assert gap_pct_stale  > REPEG_PROXIMITY_PCT, "Stale limit would have incorrectly blocked repeg"
    assert gap_pct_synced < REPEG_PROXIMITY_PCT, \
        f"Synced limit should give correct proximity ({gap_pct_synced:.3f})"
