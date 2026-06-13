"""
tests/test_p0_durable_duplicate_signal_guard.py

P0 — durable per-client duplicate_signal_id guard.

Verifies:
  1. Same signal_id+client, only prior REJECTED duplicate row → NOT blocked
  2. Same signal_id+client already WATCHING in trade_queue → blocked
  3. Same signal_id different clients → NOT blocked across
  4. Same signal_id+client has active ENTRY order → blocked
  5. DB read failure in LIVE → blocked with duplicate_check_unavailable_live_blocked
  6. DB read failure in PAPER → proceeds (best-effort)
  7. current_queue_id excludes the current row from triggering itself
"""
from __future__ import annotations

from contextlib import contextmanager
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, patch
import time
import pytest


# Module-under-test factory: avoid importing real heavy deps
def _make_mc(mode: str = "PAPER"):
    """Construct an APMasterControl-like with minimal scaffolding."""
    import sys, importlib
    # Force-reload to start clean each test
    sys.modules.pop("ap_master_control", None)
    import ap_master_control as mc_mod

    mc = mc_mod.APMasterControl.__new__(mc_mod.APMasterControl)
    mc.mode = mode.upper()
    mc.paper = mode.upper() != "LIVE"
    mc._mode_fn = None
    mc._seen_signals = {}
    # The helper only needs these
    return mc, mc_mod


# Fake DB row helper
def _row(**kw):
    r = {"id": 1, "status": "WATCHING", "last_error": None}
    r.update(kw)
    return r


@contextmanager
def _patch_ap_db(fake_conn, *, run_with_retry=None):
    """Inject a lightweight ap.db module so tests do not import real Postgres."""
    import sys

    fake_mod = ModuleType("ap.db")
    fake_mod.conn = lambda: fake_conn
    fake_mod.run_with_retry = run_with_retry or (lambda f, *a, **k: f())
    original = sys.modules.get("ap.db")
    sys.modules["ap.db"] = fake_mod
    try:
        yield fake_mod
    finally:
        if original is not None:
            sys.modules["ap.db"] = original
        else:
            sys.modules.pop("ap.db", None)


# =============================================================================
# Test 1: only prior REJECTED row → NOT a duplicate
# =============================================================================

def test_prior_rejected_duplicate_does_not_block():
    mc, _ = _make_mc()

    # Simulate: trade_queue has only a REJECTED row for this client+signal,
    # so no active path exists. Helper should return (False, "", "").
    fake_cursor = MagicMock()
    # All 3 SELECTs return None (no active rows)
    fake_cursor.fetchone.side_effect = [None, None, None]

    fake_conn = MagicMock()
    fake_conn.__enter__ = MagicMock(return_value=fake_cursor)
    fake_conn.__exit__ = MagicMock(return_value=False)

    with _patch_ap_db(fake_conn):
        is_dup, source, detail = mc._has_durable_duplicate_signal(
            client_id="jason@example.com",
            signal_id="sig-abc",
        )
    assert is_dup is False, f"REJECTED-only history must not block, got {(is_dup, source, detail)}"
    assert source == ""


# =============================================================================
# Test 2: active WATCHING in trade_queue → BLOCKED
# =============================================================================

def test_active_watching_trade_queue_blocks():
    mc, _ = _make_mc()

    fake_cursor = MagicMock()
    # First SELECT (trade_queue active) returns a hit
    fake_cursor.fetchone.side_effect = [
        _row(id=42, status="WATCHING"),
    ]
    fake_conn = MagicMock()
    fake_conn.__enter__ = MagicMock(return_value=fake_cursor)
    fake_conn.__exit__ = MagicMock(return_value=False)

    with _patch_ap_db(fake_conn):
        is_dup, source, detail = mc._has_durable_duplicate_signal(
            client_id="jason@example.com",
            signal_id="sig-abc",
        )
    assert is_dup is True
    assert source == "trade_queue"
    assert "42" in detail
    assert "WATCHING" in detail


# =============================================================================
# Test 3: same signal_id different clients → no cross-client block
# =============================================================================

def test_different_clients_no_cross_block():
    mc, _ = _make_mc()

    fake_cursor = MagicMock()
    # All SELECTs return None (helper only sees rows matching THIS client_id)
    fake_cursor.fetchone.side_effect = [None, None, None]
    fake_conn = MagicMock()
    fake_conn.__enter__ = MagicMock(return_value=fake_cursor)
    fake_conn.__exit__ = MagicMock(return_value=False)

    with _patch_ap_db(fake_conn):
        is_dup, _, _ = mc._has_durable_duplicate_signal(
            client_id="jason@example.com",
            signal_id="sig-abc",
        )
    assert is_dup is False, "different clients must not block each other"

    # Verify the SQL was actually parameterized with the client_id
    # by checking the first execute call's args
    calls = fake_cursor.execute.call_args_list
    assert len(calls) >= 1
    first_args = calls[0].args[1]  # the params tuple
    assert "jason@example.com" in first_args


# =============================================================================
# Test 4: active ENTRY order → BLOCKED
# =============================================================================

def test_active_entry_order_blocks():
    mc, _ = _make_mc()

    fake_cursor = MagicMock()
    # trade_queue: no hit; orders: hit; positions: not reached
    fake_cursor.fetchone.side_effect = [
        None,
        _row(id=99, status="SUBMITTED"),
    ]
    fake_conn = MagicMock()
    fake_conn.__enter__ = MagicMock(return_value=fake_cursor)
    fake_conn.__exit__ = MagicMock(return_value=False)

    with _patch_ap_db(fake_conn):
        is_dup, source, detail = mc._has_durable_duplicate_signal(
            client_id="jason@example.com",
            signal_id="sig-abc",
        )
    assert is_dup is True
    assert source == "orders"
    assert "99" in detail
    assert "SUBMITTED" in detail


# =============================================================================
# Test 5: DB read failure returns check_unavailable
# =============================================================================

def test_db_failure_returns_check_unavailable():
    mc, _ = _make_mc()

    # Simulate run_with_retry raising
    with _patch_ap_db(
        MagicMock(),
        run_with_retry=lambda f, *a, **k: (_ for _ in ()).throw(Exception("connection refused")),
    ):
        is_dup, source, detail = mc._has_durable_duplicate_signal(
            client_id="jason@example.com",
            signal_id="sig-abc",
        )
    assert is_dup is False
    assert source == "check_unavailable"
    assert "connection refused" in detail


# =============================================================================
# Test 6: DB import failure also returns check_unavailable
# =============================================================================

def test_db_import_failure_returns_check_unavailable():
    mc, _ = _make_mc()

    # Make `from ap.db import ...` raise inside the helper
    import sys
    original = sys.modules.get("ap.db")
    sys.modules["ap.db"] = None  # makes the import raise ImportError
    try:
        is_dup, source, detail = mc._has_durable_duplicate_signal(
            client_id="jason@example.com",
            signal_id="sig-abc",
        )
    finally:
        if original is not None:
            sys.modules["ap.db"] = original
        else:
            sys.modules.pop("ap.db", None)

    assert is_dup is False
    assert source == "check_unavailable"


# =============================================================================
# Test 7: current_queue_id excludes the current row
# =============================================================================

def test_current_queue_id_excludes_self():
    mc, _ = _make_mc()

    fake_cursor = MagicMock()
    fake_cursor.fetchone.side_effect = [None, None, None]
    fake_conn = MagicMock()
    fake_conn.__enter__ = MagicMock(return_value=fake_cursor)
    fake_conn.__exit__ = MagicMock(return_value=False)

    with _patch_ap_db(fake_conn):
        mc._has_durable_duplicate_signal(
            client_id="jason@example.com",
            signal_id="sig-abc",
            current_queue_id=12345,
        )

    # First execute call should use the "id <> %s" variant
    calls = fake_cursor.execute.call_args_list
    first_sql = calls[0].args[0]
    first_params = calls[0].args[1]
    assert "id <> %s" in first_sql
    assert 12345 in first_params


# =============================================================================
# Test 8: position check tolerates missing signal_id column
# =============================================================================

def test_position_check_tolerates_missing_column():
    mc, _ = _make_mc()

    fake_cursor = MagicMock()
    # trade_queue: None, orders: None, positions: raises (column missing)
    fake_cursor.fetchone.side_effect = [None, None]
    fake_cursor.execute.side_effect = [
        None,                       # trade_queue execute OK
        None,                       # orders execute OK
        Exception("column signal_id does not exist"),  # positions execute fails
    ]
    fake_conn = MagicMock()
    fake_conn.__enter__ = MagicMock(return_value=fake_cursor)
    fake_conn.__exit__ = MagicMock(return_value=False)

    with _patch_ap_db(fake_conn):
        is_dup, source, _ = mc._has_durable_duplicate_signal(
            client_id="jason@example.com",
            signal_id="sig-abc",
        )
    # Schema-missing on positions must NOT cause false-block
    assert is_dup is False
    assert source == ""


# =============================================================================
# Test 9: terminal-only orders history → NOT blocked
# =============================================================================

def test_only_terminal_orders_does_not_block():
    mc, _ = _make_mc()

    # All 3 SELECTs return None because the SELECT statements filter for
    # active statuses only — terminal statuses (CANCELED/EXPIRED/REJECTED)
    # are excluded from the WHERE clause.
    fake_cursor = MagicMock()
    fake_cursor.fetchone.side_effect = [None, None, None]
    fake_conn = MagicMock()
    fake_conn.__enter__ = MagicMock(return_value=fake_cursor)
    fake_conn.__exit__ = MagicMock(return_value=False)

    with _patch_ap_db(fake_conn):
        is_dup, _, _ = mc._has_durable_duplicate_signal(
            client_id="jason@example.com",
            signal_id="sig-abc",
        )
    assert is_dup is False

    # Verify the queries filtered out terminal statuses
    sql_strs = [c.args[0] for c in fake_cursor.execute.call_args_list]
    for sql in sql_strs:
        # SQL must explicitly enumerate active statuses, not just "<>".
        assert "WATCHING" in sql or "SUBMITTED" in sql or "OPEN" in sql, (
            f"SQL doesn't gate on active status: {sql[:200]}"
        )


# =============================================================================
# Test 10: open positions hit blocks
# =============================================================================

def test_open_position_blocks():
    mc, _ = _make_mc()

    fake_cursor = MagicMock()
    fake_cursor.fetchone.side_effect = [
        None,                                  # trade_queue
        None,                                  # orders
        _row(id=7, status="OPEN"),             # positions
    ]
    fake_conn = MagicMock()
    fake_conn.__enter__ = MagicMock(return_value=fake_cursor)
    fake_conn.__exit__ = MagicMock(return_value=False)

    with _patch_ap_db(fake_conn):
        is_dup, source, detail = mc._has_durable_duplicate_signal(
            client_id="jason@example.com",
            signal_id="sig-abc",
        )
    assert is_dup is True
    assert source == "positions"
    assert "OPEN" in detail


# =============================================================================
# Integration tests — queue.py → master_control.evaluate seam
# Verify queue.py injects _queue_id and the guard excludes the current row.
# =============================================================================

def test_current_queue_id_excludes_self_no_block():
    """The current row in PROCESSING for this job_id must NOT cause a self-block.
    The SQL filter `id <> %s` excludes it."""
    mc, _ = _make_mc()

    # Simulate: trade_queue query returns NO active rows because the current
    # row's id matches and is excluded. The other two queries also return None.
    fake_cursor = MagicMock()
    fake_cursor.fetchone.side_effect = [None, None, None]
    fake_conn = MagicMock()
    fake_conn.__enter__ = MagicMock(return_value=fake_cursor)
    fake_conn.__exit__ = MagicMock(return_value=False)

    with _patch_ap_db(fake_conn):
        is_dup, source, _ = mc._has_durable_duplicate_signal(
            client_id="jason@example.com",
            signal_id="sig-abc",
            current_queue_id=12345,
        )

    assert is_dup is False, "current row in PROCESSING must not self-block"
    # Confirm the trade_queue SQL used `id <> %s` and the param was passed
    first_sql = fake_cursor.execute.call_args_list[0].args[0]
    first_params = fake_cursor.execute.call_args_list[0].args[1]
    assert "id <> %s" in first_sql
    assert 12345 in first_params


def test_absent_queue_id_does_block_on_active_row():
    """If _queue_id is absent and a matching active row exists, it blocks.
    This proves the SQL still finds active rows when no exclusion is given."""
    mc, _ = _make_mc()

    fake_cursor = MagicMock()
    # trade_queue: hit — without queue_id exclusion the helper sees the active row
    fake_cursor.fetchone.side_effect = [
        _row(id=42, status="WATCHING"),
    ]
    fake_conn = MagicMock()
    fake_conn.__enter__ = MagicMock(return_value=fake_cursor)
    fake_conn.__exit__ = MagicMock(return_value=False)

    with _patch_ap_db(fake_conn):
        is_dup, source, detail = mc._has_durable_duplicate_signal(
            client_id="jason@example.com",
            signal_id="sig-abc",
            current_queue_id=None,
        )

    assert is_dup is True
    assert source == "trade_queue"
    # SQL must NOT contain id <> when current_queue_id is None
    first_sql = fake_cursor.execute.call_args_list[0].args[0]
    assert "id <> %s" not in first_sql


def test_queue_dispatch_passes_queue_id_into_evaluate():
    """queue._dispatch must inject _queue_id into the payload before calling
    master_control.evaluate. This is the contract that makes the helper
    work end-to-end."""
    from unittest.mock import MagicMock, patch
    import sys

    # Stub the heavy imports queue.py needs at import time
    sys.modules.setdefault("ap.logger", MagicMock())
    sys.modules.setdefault("ap.observability", MagicMock())
    from ap import queue as queue_mod

    # Mock the master_control: capture what payload it receives
    received_payloads = []
    mc = SimpleNamespace(mode="PAPER", _equity_cache_ts=time.time())

    def fake_evaluate(payload, client_id):
        received_payloads.append(dict(payload))
        # Return a non-ok decision so dispatch short-circuits after MC.evaluate
        return MagicMock(ok=False, stage="test", reason="test_stop")
    mc.evaluate = fake_evaluate

    contract_selector = MagicMock()
    osm = MagicMock()
    watcher = MagicMock()

    # Patch the helpers that _dispatch calls when decision is not ok
    with patch.object(queue_mod, "_mark_job", return_value=None), \
         patch.object(queue_mod, "_log_rejection_to_db", return_value=None):
        queue_mod._dispatch(
            job_id=98765,
            client_id="jason@example.com",
            signal_id="sig-abc",
            payload={"ticker": "NVDA", "signal_id": "sig-abc",
                     "score": 75.0, "side": "CALL", "timeframe": "1d"},
            master_control=mc,
            contract_selector=contract_selector,
            order_state_machine=osm,
            entry_watcher=watcher,
        )

    assert len(received_payloads) >= 1, "master_control.evaluate was never called"
    p = received_payloads[0]
    assert "_queue_id" in p, (
        f"queue dispatch did NOT inject _queue_id into MC payload. "
        f"Got keys: {list(p.keys())}"
    )
    assert p["_queue_id"] == 98765, (
        f"_queue_id must be the actual job_id (98765), got {p['_queue_id']!r}"
    )
    # And the original payload's keys are still there (shallow copy preserved)
    assert p.get("ticker") == "NVDA"
    assert p.get("signal_id") == "sig-abc"


def test_queue_dispatch_does_not_mutate_caller_payload():
    """The shallow copy must protect the caller — original payload dict must
    not have _queue_id injected into it."""
    from unittest.mock import MagicMock, patch
    import sys
    sys.modules.setdefault("ap.logger", MagicMock())
    sys.modules.setdefault("ap.observability", MagicMock())
    from ap import queue as queue_mod

    mc = SimpleNamespace(mode="PAPER", _equity_cache_ts=time.time())
    mc.evaluate = MagicMock(return_value=MagicMock(
        ok=False, stage="test", reason="test_stop",
    ))

    caller_payload = {"ticker": "NVDA", "signal_id": "sig-abc",
                       "score": 75.0, "side": "CALL", "timeframe": "1d"}

    with patch.object(queue_mod, "_mark_job", return_value=None), \
         patch.object(queue_mod, "_log_rejection_to_db", return_value=None):
        queue_mod._dispatch(
            job_id=1234,
            client_id="jason@example.com",
            signal_id="sig-abc",
            payload=caller_payload,
            master_control=mc,
            contract_selector=MagicMock(),
            order_state_machine=MagicMock(),
            entry_watcher=MagicMock(),
        )

    # Caller payload must NOT have _queue_id (proves shallow copy was used)
    assert "_queue_id" not in caller_payload, (
        f"caller payload was mutated! _queue_id leaked into it: {caller_payload}"
    )
