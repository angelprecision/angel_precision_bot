"""
Integration test for apply_repeg() proving the BLOCKER-2 fix.

What the reviewer challenged: "apply_repeg() cancels broker order but
not actually resubmits immediately. CREATED orders are not picked up by
any worker. Otherwise this turns 'missed move cancel' into 'cancel +
local state reset' with no new live order."

This file proves end-to-end:

  1. Happy path: apply_repeg cancels old broker order AND submits a new one
     at the new limit price. The new broker_order_id is stored. Local order
     ends in SUBMITTED, not stuck in CREATED.

  2. Resubmit failure -> slot is freed cleanly (CANCELED, not stuck CREATED).

  3. Resubmit exception -> same, slot is freed cleanly.

  4. Cancel failure -> we never proceeded to resubmit (no double-order risk).

  5. Required fields missing (symbol, contract, qty) -> declined safely.

Run:
    pytest tests/test_repeg_resubmit.py -xvs
"""
from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock, patch

import pytest


# Stub ap.db before importing retry_engine so update_order is observable.
#
# IMPORTANT (2026-05-26 audit fix): the previous implementation replaced
# the entire ap.db module in sys.modules with a stub that only had
# update_order. That polluted later tests across many files because
# downstream `from ap.db import conn` / `from ap.db import run_with_retry`
# fell through to the stub and got AttributeError -> ImportError.
#
# This version preserves whatever symbols already exist on the real
# ap.db (conn, run_with_retry, _get_pool, etc.) and only overrides
# update_order, then RESTORES sys.modules at module teardown via the
# pytest teardown_module hook (atexit alone is insufficient because
# pytest collects all modules in one process).
import os as _os_for_db
_os_for_db.environ.setdefault(
    "DATABASE_URL", "postgresql://test:test@127.0.0.1:5432/test_repeg"
)

# Capture whatever ap.db currently is (real module if importable, or None).
_prior_ap_db = sys.modules.get("ap.db")

# Try to import the real ap.db first; copy its non-update_order names
# into the stub so downstream `from ap.db import conn / run_with_retry`
# still works for sibling test files.
_db_stub = types.ModuleType("ap.db")
_db_stub.update_order = MagicMock()
try:
    import importlib as _importlib_for_db
    _real_db = _importlib_for_db.import_module("ap.db")
    for _attr in ("conn", "run_with_retry", "_get_pool", "init_db",
                  "insert_order", "new_local_order_id",
                  "already_processed_signal", "mark_signal_processed",
                  "create_client", "get_all_clients",
                  "get_position_by_id", "get_all_orders", "get_order_by_id"):
        if hasattr(_real_db, _attr):
            setattr(_db_stub, _attr, getattr(_real_db, _attr))
    _prior_ap_db = _real_db
except Exception:
    # Real module unimportable. Stub still works for retry_engine's
    # needs (only update_order is observed). Downstream sibling tests
    # that depend on a real ap.db also can't run without a DB, so the
    # breakage is contained.
    _real_db = None

sys.modules["ap.db"] = _db_stub

# Now safe to import.
from ap.retry_engine import apply_repeg, RepegDecision  # noqa: E402


def teardown_module(module):
    """Restore sys.modules['ap.db'] when this test module finishes.

    Pytest does not automatically undo sys.modules mutations made at
    module import time. Without this, every later test in the same
    pytest session that imports from ap.db would still see the stub.
    """
    if _prior_ap_db is not None:
        sys.modules["ap.db"] = _prior_ap_db
    else:
        sys.modules.pop("ap.db", None)


@pytest.fixture(autouse=True)
def reset_update_mock():
    _db_stub.update_order.reset_mock()
    yield


def _decision(new_limit: float = 2.24, attempts_used: int = 1) -> RepegDecision:
    return RepegDecision(
        ok=True,
        reason="aligned_repeg",
        new_limit_price=new_limit,
        attempts_used=attempts_used,
        detail={"prev_limit": 2.17},
    )


def _order(**overrides):
    base = {
        "id":                 "local-abc",
        "broker_order_id":    "broker-old-1",
        "symbol":             "MSFT",
        "contract":           "MSFT260522C00427500",
        "qty":                1,
        "limit_price":        2.17,
        "direction":          "CALL",
        "signal_entry_price": 427.50,
        "repeg_attempts":     0,
        "last_repeg_ts":      0,
        "meta":               {"score": 78},
    }
    base.update(overrides)
    return base


# ─── Happy path ─────────────────────────────────────────────────────────────

def test_apply_repeg_produces_new_broker_order():
    """The core proof: a successful re-peg creates a NEW broker order at the
    new limit price, and the local order transitions to SUBMITTED with the
    new broker_order_id."""
    broker = MagicMock()
    broker.cancel_order.return_value = {"ok": True}
    broker.place_order.return_value  = {
        "broker_order_id": "broker-NEW-2",
        "status":          "ACK",
    }

    ok = apply_repeg(
        broker=broker, osm=MagicMock(),
        order_row=_order(), decision=_decision(), client_id="client@x.com",
    )
    assert ok is True, "happy path should return True"

    broker.cancel_order.assert_called_once_with("broker-old-1")
    broker.place_order.assert_called_once()
    pa = broker.place_order.call_args.kwargs
    assert pa["symbol"]      == "MSFT"
    assert pa["contract"]    == "MSFT260522C00427500"
    assert pa["qty"]         == 1
    assert pa["limit_price"] == 2.24
    assert pa["side"]        == "buy_to_open"

    # Two DB updates: pre-submit (CREATED + new limit + meta + null broker_oid),
    # and post-submit (SUBMITTED + new broker_oid).
    calls = _db_stub.update_order.call_args_list
    assert len(calls) == 2

    pre = calls[0].kwargs
    assert pre["status"]           == "CREATED"
    assert pre["limit_price"]      == 2.24
    assert pre["broker_order_id"]  is None
    assert pre["meta"]["repeg_attempts"]       == 1
    assert pre["meta"]["prev_broker_order_id"] == "broker-old-1"

    post = calls[1].kwargs
    assert post["status"]           == "SUBMITTED"
    assert post["broker_order_id"]  == "broker-NEW-2"


# ─── Failure modes ──────────────────────────────────────────────────────────

def test_resubmit_failure_marks_canceled_to_free_slot():
    """If place_order raises, we must transition to CANCELED so the daily-cap
    gate sees the slot as free (not stuck in CREATED). This was the exact
    failure mode the reviewer caught."""
    broker = MagicMock()
    broker.cancel_order.return_value = {"ok": True}
    broker.place_order.side_effect   = RuntimeError("broker_unreachable")

    ok = apply_repeg(
        broker=broker, osm=MagicMock(),
        order_row=_order(), decision=_decision(), client_id="client@x.com",
    )
    assert ok is False

    calls = _db_stub.update_order.call_args_list
    # First call: CREATED + new limit (pre-submit intent)
    # Second call: CANCELED (cleanup so slot is freed)
    assert len(calls) == 2
    assert calls[0].kwargs["status"]  == "CREATED"
    assert calls[1].kwargs["status"]  == "CANCELED"
    assert "repeg_resubmit_exception" in calls[1].kwargs["last_error"]


def test_resubmit_rejected_response_marks_rejected():
    """Broker returns a non-success status (e.g. 'REJECTED', 'ERROR'). Slot
    must be freed via REJECTED status, not stuck in CREATED."""
    broker = MagicMock()
    broker.cancel_order.return_value = {"ok": True}
    broker.place_order.return_value  = {
        "broker_order_id": None,
        "status":          "REJECTED",
        "error":           "insufficient_buying_power",
    }

    ok = apply_repeg(
        broker=broker, osm=MagicMock(),
        order_row=_order(), decision=_decision(), client_id="client@x.com",
    )
    assert ok is False

    calls = _db_stub.update_order.call_args_list
    assert len(calls) == 2
    assert calls[1].kwargs["status"]  == "REJECTED"
    assert "insufficient_buying_power" in calls[1].kwargs["last_error"]


def test_cancel_failure_aborts_before_resubmit():
    """If the broker cancel fails, we MUST NOT re-submit (would create a
    second live order at the broker). The DB must also be untouched."""
    broker = MagicMock()
    broker.cancel_order.return_value = {"error": "order_not_found"}

    ok = apply_repeg(
        broker=broker, osm=MagicMock(),
        order_row=_order(), decision=_decision(), client_id="client@x.com",
    )
    assert ok is False
    broker.place_order.assert_not_called(), "must not resubmit when cancel failed"
    _db_stub.update_order.assert_not_called(), "no DB writes when cancel fails"


def test_cancel_exception_aborts_before_resubmit():
    broker = MagicMock()
    broker.cancel_order.side_effect = RuntimeError("connection_lost")

    ok = apply_repeg(
        broker=broker, osm=MagicMock(),
        order_row=_order(), decision=_decision(), client_id="client@x.com",
    )
    assert ok is False
    broker.place_order.assert_not_called()
    _db_stub.update_order.assert_not_called()


# ─── Missing-field guards ───────────────────────────────────────────────────

def test_missing_symbol_declines_safely():
    """If we can't reconstruct the resubmit (missing symbol/contract/qty),
    we must NOT cancel the live order. Caller will fall through to
    normal stale-cancel path."""
    broker = MagicMock()
    ok = apply_repeg(
        broker=broker, osm=MagicMock(),
        order_row=_order(symbol=None), decision=_decision(), client_id="x",
    )
    assert ok is False
    broker.cancel_order.assert_not_called()
    broker.place_order.assert_not_called()


def test_missing_qty_declines_safely():
    broker = MagicMock()
    ok = apply_repeg(
        broker=broker, osm=MagicMock(),
        order_row=_order(qty=0), decision=_decision(), client_id="x",
    )
    assert ok is False
    broker.cancel_order.assert_not_called()


def test_missing_contract_declines_safely():
    broker = MagicMock()
    ok = apply_repeg(
        broker=broker, osm=MagicMock(),
        order_row=_order(contract=None), decision=_decision(), client_id="x",
    )
    assert ok is False
    broker.cancel_order.assert_not_called()


def test_bad_qty_type_declines_safely():
    broker = MagicMock()
    ok = apply_repeg(
        broker=broker, osm=MagicMock(),
        order_row=_order(qty="not-a-number"), decision=_decision(), client_id="x",
    )
    assert ok is False
    broker.cancel_order.assert_not_called()


# ─── Resubmit-status acceptance edges ───────────────────────────────────────

@pytest.mark.parametrize("status_word", [
    "ACK", "ACKED", "FILLED", "SUBMITTED", "OK", "ACCEPTED", "PENDING", "OPEN",
])
def test_resubmit_accepted_status_words(status_word):
    broker = MagicMock()
    broker.cancel_order.return_value = {"ok": True}
    broker.place_order.return_value  = {"broker_order_id": "b-2", "status": status_word}
    ok = apply_repeg(
        broker=broker, osm=MagicMock(),
        order_row=_order(), decision=_decision(), client_id="x",
    )
    assert ok is True


@pytest.mark.parametrize("status_word", [
    "REJECTED", "ERROR", "EXPIRED", "CANCELED", "UNKNOWN", "",
])
def test_resubmit_rejected_status_words(status_word):
    broker = MagicMock()
    broker.cancel_order.return_value = {"ok": True}
    broker.place_order.return_value  = {"broker_order_id": None, "status": status_word}
    ok = apply_repeg(
        broker=broker, osm=MagicMock(),
        order_row=_order(), decision=_decision(), client_id="x",
    )
    assert ok is False, f"status={status_word} should be rejected"
