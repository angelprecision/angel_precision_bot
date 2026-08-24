"""
tests/test_p0_exit_autonomous_recovery_quantity_guard.py
=========================================================
P0 regression tests — PR #481 amendment

Defect addressed
----------------
ap/exit_autonomous_recovery.py::recover_exit_position() re-implemented
broker-position quantity semantics locally (via a raw broker.list_positions()
call), bypassing the canonical quantity-conflict semantics introduced in
PR #481 (resolve_exit_broker_truth / _extract_long_position_qty).

The false-flat seam: an exact-OCC row with quantity=0 / boolean / fractional /
negative was silently evaluated as "contract not held" and mark_position_closed()
was called — real exposure was falsely terminalized.

Additionally the exception path fell through to _mark_replacement_safe(), so
a failed broker position query could by itself authorize replacement.

This test file proves all 8 required cases from the amendment spec are
correctly handled by the repaired decision path.

Money-path invariant under test
--------------------------------
BROKER QUANTITY UNCERTAINTY OR CONFLICT MUST NEVER BECOME ZERO EXPOSURE.
BROKER POSITION TRUTH UNKNOWN MUST NOT INDEPENDENTLY AUTHORIZE REPLACEMENT.

Test anatomy
------------
Each test drives recover_exit_position() into the "negative proof" branch
(no pending broker order, no matching open sell-to-close orders) and then
controls what broker.list_positions() returns.  Tests assert on what
exit_engine methods were / were not called and on the RecoveryAction returned.
"""

from __future__ import annotations

import types
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Module under test
# ---------------------------------------------------------------------------
import os
os.environ.setdefault("DATABASE_URL", "postgresql://x:x@localhost/x")

from ap.exit_autonomous_recovery import recover_exit_position, RecoveryAction  # noqa: E402

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------
_CONTRACT = "AAPL250117C00200000"   # canonical OCC contract


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _make_pos(
    contract: str = _CONTRACT,
    client_id: str = "test_client_001",
    position_id: str = "pos-ar-test-001",
) -> Any:
    """Minimal position object that drives recover_exit_position into the
    negative-proof branch (no pending broker order id, no open exit orders)."""
    pos = types.SimpleNamespace(
        position_id=position_id,
        client_id=client_id,
        # No pending broker order → skip exact-order-id resolution path.
        pending_exit_local_order_id="",
        pending_exit_broker_order_id="",
        pending_exit_qty=1,
        contracts=1,
        # option_symbol satisfies _position_contract()
        option_symbol=contract,
        # Quote timestamps — leave None so quote_health() doesn't panic
        last_option_quote_update_ts=None,
        last_underlying_quote_update_ts=None,
        last_option_quote_missing_ts=None,
        last_underlying_quote_missing_ts=None,
    )
    return pos


def _make_broker(
    *,
    list_positions_rows: list[dict] | None = None,
    list_positions_raises: Exception | None = None,
) -> Any:
    """
    Broker stub that:
    - Returns [] from list_open_orders (no matching open exit orders found)
      so the caller always reaches the negative-proof block.
    - Drives list_positions() to return the given rows or raise the given
      exception.
    """
    broker = types.SimpleNamespace()

    # No open exit orders — drives code to negative-proof block.
    def _list_open_orders(**_kwargs: Any) -> list:
        return []

    broker.list_open_orders = _list_open_orders

    # list_positions — the payload under test.
    if list_positions_raises is not None:
        def _list_positions() -> list:
            raise list_positions_raises  # type: ignore[misc]
    else:
        rows = list(list_positions_rows or [])

        def _list_positions() -> list:
            return rows

    broker.list_positions = _list_positions
    return broker


class _ExitEngine:
    """Minimal exit-engine stub that records all call signatures."""

    def __init__(self) -> None:
        self.mark_position_closed_calls: list[str] = []
        self.mark_exit_replacement_safe_calls: list[str] = []
        self.clear_exit_in_flight_calls: list[str] = []

    def mark_position_closed(self, pid: str, **_: Any) -> None:
        self.mark_position_closed_calls.append(pid)

    def mark_exit_replacement_safe(self, pid: str, **_: Any) -> None:
        self.mark_exit_replacement_safe_calls.append(pid)

    def clear_exit_in_flight(self, pid: str, **_: Any) -> None:
        self.clear_exit_in_flight_calls.append(pid)

    @property
    def any_replacement_authorized(self) -> bool:
        return bool(
            self.mark_exit_replacement_safe_calls or self.clear_exit_in_flight_calls
        )


# ---------------------------------------------------------------------------
# Case 1 — exact OCC quantity=0
# ---------------------------------------------------------------------------

def test_case01_exact_occ_quantity_zero_does_not_close():
    """
    PR #481 amendment — case 1:
    An exact-match row with quantity=0 must be treated as UNKNOWN, not flat.
    -> zero mark_position_closed()
    -> zero false autonomous_recovery_contract_flat_at_broker
    -> no unsafe replacement unlock
    """
    pos = _make_pos()
    ee = _ExitEngine()
    broker = _make_broker(
        list_positions_rows=[
            {"symbol": _CONTRACT, "quantity": 0},
        ]
    )

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert len(ee.mark_position_closed_calls) == 0, (
        f"mark_position_closed must NOT be called for quantity=0 row; "
        f"got calls={ee.mark_position_closed_calls}"
    )
    assert not ee.any_replacement_authorized, (
        "replacement must NOT be authorized when broker position truth is UNKNOWN (qty=0 row)"
    )
    assert action.action != "MARKED_CLOSED", (
        f"RecoveryAction must not be MARKED_CLOSED for quantity=0; got action={action.action}"
    )


# ---------------------------------------------------------------------------
# Case 2 — exact OCC quantity=False (boolean)
# ---------------------------------------------------------------------------

def test_case02_exact_occ_quantity_false_does_not_close():
    """
    Case 2: quantity=False is a boolean — must be UNKNOWN, not zero exposure.
    -> zero mark_position_closed()
    -> no unsafe replacement unlock
    """
    pos = _make_pos()
    ee = _ExitEngine()
    broker = _make_broker(
        list_positions_rows=[
            {"symbol": _CONTRACT, "quantity": False},
        ]
    )

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert len(ee.mark_position_closed_calls) == 0, (
        f"mark_position_closed must NOT be called for quantity=False; "
        f"got calls={ee.mark_position_closed_calls}"
    )
    assert not ee.any_replacement_authorized, (
        "replacement must NOT be authorized when broker position truth is UNKNOWN (qty=False)"
    )
    assert action.action != "MARKED_CLOSED", (
        f"RecoveryAction must not be MARKED_CLOSED for quantity=False; got action={action.action}"
    )


# ---------------------------------------------------------------------------
# Case 3 — exact OCC quantity=0.5 (fractional)
# ---------------------------------------------------------------------------

def test_case03_exact_occ_quantity_fractional_does_not_close():
    """
    Case 3: quantity=0.5 is fractional — must be UNKNOWN, not truncated to 0.
    -> zero mark_position_closed()
    -> no unsafe replacement unlock
    """
    pos = _make_pos()
    ee = _ExitEngine()
    broker = _make_broker(
        list_positions_rows=[
            {"symbol": _CONTRACT, "quantity": 0.5},
        ]
    )

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert len(ee.mark_position_closed_calls) == 0, (
        f"mark_position_closed must NOT be called for quantity=0.5; "
        f"got calls={ee.mark_position_closed_calls}"
    )
    assert not ee.any_replacement_authorized, (
        "replacement must NOT be authorized when broker position truth is UNKNOWN (qty=0.5)"
    )
    assert action.action != "MARKED_CLOSED"


# ---------------------------------------------------------------------------
# Case 4 — exact OCC quantity=-1 (negative integer)
# ---------------------------------------------------------------------------

def test_case04_exact_occ_quantity_negative_one_does_not_close():
    """
    Case 4: quantity=-1 is negative — must be UNKNOWN, never coerced to 0.
    -> zero mark_position_closed()
    -> no unsafe replacement unlock
    """
    pos = _make_pos()
    ee = _ExitEngine()
    broker = _make_broker(
        list_positions_rows=[
            {"symbol": _CONTRACT, "quantity": -1},
        ]
    )

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert len(ee.mark_position_closed_calls) == 0, (
        f"mark_position_closed must NOT be called for quantity=-1; "
        f"got calls={ee.mark_position_closed_calls}"
    )
    assert not ee.any_replacement_authorized, (
        "replacement must NOT be authorized when broker position truth is UNKNOWN (qty=-1)"
    )
    assert action.action != "MARKED_CLOSED"


# ---------------------------------------------------------------------------
# Case 5 — exact OCC quantity=-0.5 (negative fractional)
# ---------------------------------------------------------------------------

def test_case05_exact_occ_quantity_negative_fractional_does_not_close():
    """
    Case 5: quantity=-0.5 — negative AND fractional → UNKNOWN.
    -> zero mark_position_closed()
    -> no unsafe replacement unlock
    """
    pos = _make_pos()
    ee = _ExitEngine()
    broker = _make_broker(
        list_positions_rows=[
            {"symbol": _CONTRACT, "quantity": -0.5},
        ]
    )

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert len(ee.mark_position_closed_calls) == 0, (
        f"mark_position_closed must NOT be called for quantity=-0.5; "
        f"got calls={ee.mark_position_closed_calls}"
    )
    assert not ee.any_replacement_authorized, (
        "replacement must NOT be authorized when broker position truth is UNKNOWN (qty=-0.5)"
    )
    assert action.action != "MARKED_CLOSED"


# ---------------------------------------------------------------------------
# Case 6 — broker list_positions() raises exception
# ---------------------------------------------------------------------------

def test_case06_broker_list_positions_exception_does_not_close_or_unlock():
    """
    Case 6: broker.list_positions() raises → position truth UNKNOWN.
    -> zero mark_position_closed()
    -> replacement must NOT be authorized solely because broker position
       query failed (old code fell through to _mark_replacement_safe())
    """
    pos = _make_pos()
    ee = _ExitEngine()
    broker = _make_broker(
        list_positions_raises=RuntimeError("Tradier 503 Service Unavailable")
    )

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert len(ee.mark_position_closed_calls) == 0, (
        f"mark_position_closed must NOT be called when list_positions raises; "
        f"got calls={ee.mark_position_closed_calls}"
    )
    assert not ee.any_replacement_authorized, (
        "replacement must NOT be authorized based solely on failed broker position truth; "
        f"action={action.action} replacement_calls={ee.mark_exit_replacement_safe_calls}"
    )
    assert action.action == "NOOP", (
        f"Expected NOOP when broker position truth is unknown via exception; "
        f"got action={action.action} reason={action.reason}"
    )


# ---------------------------------------------------------------------------
# Case 7 — exact OCC with valid positive quantity — position remains held
# ---------------------------------------------------------------------------

def test_case07_exact_occ_positive_quantity_not_falsely_closed():
    """
    Case 7: exact OCC match with quantity=2 — broker confirms position held.
    -> zero mark_position_closed()  (no false close)
    -> replacement authorization MAY occur (no open exit orders + held) but
       mark_position_closed must never be called
    """
    pos = _make_pos()
    ee = _ExitEngine()
    broker = _make_broker(
        list_positions_rows=[
            {"symbol": _CONTRACT, "quantity": 2},
        ]
    )

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert len(ee.mark_position_closed_calls) == 0, (
        f"mark_position_closed must NOT be called when broker reports qty=2 held; "
        f"got calls={ee.mark_position_closed_calls}"
    )
    assert action.action != "MARKED_CLOSED", (
        f"RecoveryAction must not be MARKED_CLOSED when position is broker-held; "
        f"got action={action.action}"
    )


# ---------------------------------------------------------------------------
# Case 8 — authoritative flat: exact OCC genuinely absent from fresh snapshot
# ---------------------------------------------------------------------------

def test_case08_authoritative_flat_when_contract_absent_from_fresh_snapshot():
    """
    Case 8: broker.list_positions() returns a valid non-empty list that does
    NOT include the target OCC contract — authoritative broker-flat truth.
    -> mark_position_closed() IS called (existing behavior preserved)
    -> action is MARKED_CLOSED with reason autonomous_recovery_contract_flat_at_broker
    """
    pos = _make_pos()
    ee = _ExitEngine()
    # Broker returns a different contract row — target OCC is absent.
    broker = _make_broker(
        list_positions_rows=[
            {"symbol": "SPY250117P00400000", "quantity": 5},
        ]
    )

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert len(ee.mark_position_closed_calls) == 1, (
        f"mark_position_closed MUST be called when OCC is genuinely absent from fresh snapshot; "
        f"got calls={ee.mark_position_closed_calls}"
    )
    assert ee.mark_position_closed_calls[0] == "pos-ar-test-001"
    assert action.action == "MARKED_CLOSED", (
        f"Expected MARKED_CLOSED for authoritative flat; got action={action.action}"
    )
    assert "autonomous_recovery_contract_flat_at_broker" in action.reason, (
        f"Expected canonical flat reason; got reason={action.reason}"
    )


# ---------------------------------------------------------------------------
# Case 8b — authoritative flat on empty snapshot
# ---------------------------------------------------------------------------

def test_case08b_authoritative_flat_on_empty_snapshot():
    """
    Variant: broker.list_positions() returns [] (no positions at all).
    Empty list = successful snapshot with all contracts absent = authoritative flat.
    -> mark_position_closed() IS called
    """
    pos = _make_pos()
    ee = _ExitEngine()
    broker = _make_broker(list_positions_rows=[])

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert len(ee.mark_position_closed_calls) == 1, (
        f"mark_position_closed MUST be called when snapshot is empty (all contracts absent); "
        f"got calls={ee.mark_position_closed_calls}"
    )
    assert action.action == "MARKED_CLOSED"


# ---------------------------------------------------------------------------
# Additional edge cases — quantity=True (boolean True)
# ---------------------------------------------------------------------------

def test_extra_exact_occ_quantity_true_boolean_does_not_close():
    """
    quantity=True is bool — must be treated as UNKNOWN, not as quantity=1.
    -> zero mark_position_closed()
    -> no unsafe replacement unlock
    """
    pos = _make_pos()
    ee = _ExitEngine()
    broker = _make_broker(
        list_positions_rows=[
            {"symbol": _CONTRACT, "quantity": True},
        ]
    )

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert len(ee.mark_position_closed_calls) == 0, (
        f"mark_position_closed must NOT be called for quantity=True (boolean); "
        f"got calls={ee.mark_position_closed_calls}"
    )
    assert not ee.any_replacement_authorized, (
        "replacement must NOT be authorized when broker position truth is UNKNOWN (qty=True)"
    )
    assert action.action != "MARKED_CLOSED"


# ---------------------------------------------------------------------------
# Additional edge — HTTP/network exception variants
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("exc", [
    ConnectionError("network error"),
    TimeoutError("timed out"),
    ValueError("malformed JSON"),
    OSError("socket error"),
])
def test_extra_various_broker_exceptions_do_not_close_or_unlock(exc):
    """
    Various exception types from list_positions() must all → NOOP, never
    MARKED_CLOSED and never REPLACEMENT_SAFE.
    """
    pos = _make_pos()
    ee = _ExitEngine()
    broker = _make_broker(list_positions_raises=exc)

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert len(ee.mark_position_closed_calls) == 0, (
        f"mark_position_closed must NOT be called for {type(exc).__name__}; "
        f"got calls={ee.mark_position_closed_calls}"
    )
    assert not ee.any_replacement_authorized, (
        f"replacement must NOT be authorized for {type(exc).__name__}; "
        f"action={action.action}"
    )
    assert action.action == "NOOP", (
        f"Expected NOOP for broker exception; got action={action.action} reason={action.reason}"
    )


# ---------------------------------------------------------------------------
# Additional — confirm no new broker submit/cancel authority added
# ---------------------------------------------------------------------------

def test_no_new_broker_submit_or_cancel_authority():
    """
    The amended file must not introduce any new broker.submit / broker.cancel
    / broker.place_order authority that wasn't present before.
    This is a source-level assertion.
    """
    import ast
    from pathlib import Path

    src = Path("ap/exit_autonomous_recovery.py").read_text()
    tree = ast.parse(src)

    FORBIDDEN_CALLS = {
        "submit_order", "place_order", "create_order",
        "submit_exit", "place_exit",
        # cancel_order IS already present (legitimate — cancel existing in-flight exit)
        # new *entry* submit authority is forbidden:
        "submit_entry", "place_entry",
    }

    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and node.attr in FORBIDDEN_CALLS:
            pytest.fail(
                f"Forbidden broker authority '{node.attr}' found in "
                f"ap/exit_autonomous_recovery.py (line {node.lineno})"
            )
