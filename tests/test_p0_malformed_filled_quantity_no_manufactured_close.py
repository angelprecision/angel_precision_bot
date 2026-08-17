"""
tests/test_p0_malformed_filled_quantity_no_manufactured_close.py
====================================================================
P0 regression tests — PR #481 final narrow merge-gate amendment, Blocker 2

Defect addressed
-----------------
ap/exit_autonomous_recovery.py::_qty() performs logic equivalent to
`abs(int(float(val)))` and returns 0 on parse failure. The FILLED-status
recovery branch did `filled_qty = _qty(raw) or pending_exit_qty`.

This meant malformed broker quantity could manufacture a fill:

  - a fractional quantity (0.5) truncates to a believable-looking 0 via
    int(0.5)==0, which is falsy, triggering the `or pending_exit_qty`
    fallback and manufacturing a FULL close from unproven quantity truth
    (e.g. quantity_remaining=4, pending_exit_qty=4, broker reports
    quantity=0.5 on a genuine partial SCALE_OUT fill -> old code closed
    the entire position).
  - a negative quantity (-1) sign-flips to a believable-looking +1 via
    abs(), silently applying a WRONG partial fill.
  - a boolean (True) masquerades as a real quantity of 1 via
    float(True)==1.0, again applying a WRONG fill.

FILLED status and FILLED quantity are separate broker truth fields.
MALFORMED is conflict evidence, not absence.

Binding invariant under test
------------------------------
A dedicated strict parser (_strict_filled_exit_qty) distinguishes:

  VALID    (non-boolean, finite, > 0, mathematically integral)
           -> use this exact quantity, no fallback
  ABSENT   (no recognized quantity key present at all)
           -> may apply the existing, narrowly-scoped fallback to this
              position's own pending_exit_qty (broker_order_id + status
              == "filled" already exactly confirms this is the order we
              submitted)
  MALFORMED/CONFLICTING (boolean, zero, negative, fractional, non-finite,
           unparseable -- present but invalid)
           -> HOLD / NOOP. No pending_exit_qty fallback. No cumulative
              fill application. No mark_position_closed. No terminal
              proof/economics.

Test matrix (per amendment spec)
-----------------------------------
Position: quantity_remaining=4, pending_exit_qty=4.

For each malformed FILLED quantity (False, True, 0.5, -1, -0.5, NaN,
Infinity, "garbage"): no cumulative_filled=4 manufactured, remaining
stays 4, position remains OPEN, mark_position_closed not called, no
terminal proof/economics, HOLD/NOOP result.

Also: ABSENT quantity (documented fallback policy), VALID qty=1 (partial,
remaining 4->3, OPEN), VALID qty=4 (final close allowed), duplicate
FILLED callback (cumulative idempotency preserved, no double decrement).
"""

from __future__ import annotations

import os
import types
from typing import Any

os.environ.setdefault("DATABASE_URL", "postgresql://x:x@localhost/x")

import pytest  # noqa: E402

from ap.exit_autonomous_recovery import recover_exit_position  # noqa: E402

_TARGET_OCC = "SMCI260626P00032500"


# ---------------------------------------------------------------------------
# Helpers -- reused pattern from tests/test_p0_scale_out_fill_recovery_no_false_close.py
# ---------------------------------------------------------------------------

def _make_pos(*, quantity_remaining: int = 4, pending_exit_qty: int = 4, pending_broker_order_id: str = "exit-bid-1", position_id: str = "pos-malformed-fill-001") -> Any:
    return types.SimpleNamespace(
        position_id=position_id,
        client_id="test_client_001",
        pending_exit_local_order_id="",
        pending_exit_broker_order_id=pending_broker_order_id,
        pending_exit_qty=pending_exit_qty,
        contracts=4,
        quantity_remaining=quantity_remaining,
        closed=False,
        option_symbol=_TARGET_OCC,
        last_option_quote_update_ts=None,
        last_underlying_quote_update_ts=None,
        last_option_quote_missing_ts=None,
        last_underlying_quote_missing_ts=None,
    )


def _make_broker(*, get_order_result: dict | None) -> Any:
    broker = types.SimpleNamespace()

    def _list_open_orders(**_kwargs: Any) -> list:
        return []

    def _get_order(_broker_order_id: str):
        return get_order_result

    def _list_positions() -> list:
        return [{"symbol": _TARGET_OCC, "quantity": 4}]

    broker.list_open_orders = _list_open_orders
    broker.get_order = _get_order
    broker.list_positions = _list_positions
    return broker


class _RealisticExitEngine:
    """Faithfully mirrors ap_exit_engine.py::APExitEngine.note_partial_exit_fill's
    cumulative-fill-per-order-key tracking and closed-on-zero-remaining
    semantics, reused from tests/test_p0_scale_out_fill_recovery_no_false_close.py."""

    def __init__(self, pos: Any) -> None:
        self._pos = pos
        self._cum_fill_by_order: dict[str, int] = {}
        self.note_partial_exit_fill_calls: list[dict] = []
        self.mark_position_closed_calls: list[str] = []

    def note_partial_exit_fill(
        self,
        position_id: str,
        qty_filled: int = 0,
        *,
        fill_price: float | None = None,
        local_order_id: str = "",
        broker_order_id: str = "",
        cumulative_filled: int | None = None,
        cumulative_filled_qty: int | None = None,
        **kwargs: Any,
    ) -> None:
        self.note_partial_exit_fill_calls.append({
            "position_id": position_id,
            "qty_filled": qty_filled,
            "cumulative_filled": cumulative_filled,
        })
        if cumulative_filled_qty is None and cumulative_filled is not None:
            cumulative_filled_qty = cumulative_filled

        order_key = broker_order_id or local_order_id or f"pending:{position_id}"

        if cumulative_filled_qty is not None:
            cum = max(0, int(cumulative_filled_qty or 0))
            prev = self._cum_fill_by_order.get(order_key, 0)
            delta = max(0, cum - prev)
            self._cum_fill_by_order[order_key] = max(prev, cum)
        else:
            delta = max(0, int(qty_filled or 0))
            prev = self._cum_fill_by_order.get(order_key, 0)
            self._cum_fill_by_order[order_key] = prev + delta

        if delta <= 0:
            return

        self._pos.quantity_remaining = max(0, int(self._pos.quantity_remaining or 0) - delta)
        if self._pos.quantity_remaining <= 0:
            self._pos.closed = True

    def mark_position_closed(self, pid: str, **_: Any) -> None:
        self.mark_position_closed_calls.append(pid)
        self._pos.closed = True
        self._pos.quantity_remaining = 0


# ---------------------------------------------------------------------------
# Main required test matrix -- malformed FILLED quantity must HOLD
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad_qty,label", [
    (False, "boolean_false"),
    (True, "boolean_true"),
    (0.5, "fractional_positive"),
    (-1, "negative_integer"),
    (-0.5, "negative_fractional"),
    (float("nan"), "nan"),
    (float("inf"), "infinity"),
    ("garbage", "unparseable_string"),
])
def test_malformed_filled_quantity_holds_no_manufactured_fill(bad_qty, label):
    pos = _make_pos(quantity_remaining=4, pending_exit_qty=4, pending_broker_order_id="malformed-bid")
    ee = _RealisticExitEngine(pos)
    broker = _make_broker(get_order_result={"id": "malformed-bid", "status": "filled", "quantity": bad_qty})

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert pos.quantity_remaining == 4, (
        f"[{label}] malformed FILLED quantity {bad_qty!r} must NOT manufacture any "
        f"fill; expected remaining=4 (unchanged), got {pos.quantity_remaining}"
    )
    assert pos.closed is False, f"[{label}] position must remain OPEN"
    assert len(ee.mark_position_closed_calls) == 0, (
        f"[{label}] mark_position_closed must NOT be called for malformed FILLED "
        f"quantity {bad_qty!r}"
    )
    assert len(ee.note_partial_exit_fill_calls) == 0, (
        f"[{label}] note_partial_exit_fill must NOT be called (no cumulative fill "
        f"application) for malformed FILLED quantity {bad_qty!r}"
    )
    assert action.action == "NOOP", (
        f"[{label}] Expected NOOP/HOLD for malformed FILLED quantity {bad_qty!r}; "
        f"got action={action.action} reason={action.reason}"
    )
    assert action.reason == "broker_filled_quantity_malformed_hold", (
        f"[{label}] Expected the malformed-FILLED-quantity hold reason; "
        f"got reason={action.reason}"
    )


# ---------------------------------------------------------------------------
# ABSENT quantity -- documented chosen policy: narrow fallback preserved
# ---------------------------------------------------------------------------

def test_absent_quantity_field_falls_back_to_pending_exit_qty():
    """Genuine absence (no recognized quantity key at all) is NOT the same
    as malformed. The existing narrow ABSENT-only fallback to this
    position's own pending_exit_qty is preserved, since broker_order_id +
    status=='filled' for the EXACT submitted order already establishes
    strong identity -- this is a documented, bounded exception, not a
    general "guess the quantity" fallback."""
    pos = _make_pos(quantity_remaining=4, pending_exit_qty=4, pending_broker_order_id="absent-qty-bid")
    ee = _RealisticExitEngine(pos)
    # No quantity/qty/filled_qty/filled_quantity/exec_quantity key at all.
    broker = _make_broker(get_order_result={"id": "absent-qty-bid", "status": "filled"})

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert pos.quantity_remaining == 0, (
        f"Expected the documented ABSENT-only fallback to apply the full "
        f"pending_exit_qty=4; got remaining={pos.quantity_remaining}"
    )
    assert pos.closed is True
    assert action.action == "MARKED_CLOSED"


# ---------------------------------------------------------------------------
# VALID qty=1 -- partial fill, remaining 4->3, OPEN
# ---------------------------------------------------------------------------

def test_valid_partial_quantity_reduces_remaining_stays_open():
    pos = _make_pos(quantity_remaining=4, pending_exit_qty=4, pending_broker_order_id="valid-partial-bid")
    ee = _RealisticExitEngine(pos)
    broker = _make_broker(get_order_result={"id": "valid-partial-bid", "status": "filled", "quantity": 1})

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert pos.quantity_remaining == 3
    assert pos.closed is False
    assert action.action == "PARTIAL_FILL_APPLIED"


# ---------------------------------------------------------------------------
# VALID qty=4 -- final close allowed
# ---------------------------------------------------------------------------

def test_valid_full_quantity_closes():
    pos = _make_pos(quantity_remaining=4, pending_exit_qty=4, pending_broker_order_id="valid-full-bid")
    ee = _RealisticExitEngine(pos)
    broker = _make_broker(get_order_result={"id": "valid-full-bid", "status": "filled", "quantity": 4})

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert pos.quantity_remaining == 0
    assert pos.closed is True
    assert action.action == "MARKED_CLOSED"


# ---------------------------------------------------------------------------
# DUPLICATE same broker FILLED callback -- cumulative idempotency preserved
# ---------------------------------------------------------------------------

def test_duplicate_filled_callback_same_order_idempotent():
    pos = _make_pos(quantity_remaining=4, pending_exit_qty=4, pending_broker_order_id="dup-bid")
    ee = _RealisticExitEngine(pos)
    broker = _make_broker(get_order_result={"id": "dup-bid", "status": "filled", "quantity": 1})

    action1 = recover_exit_position(pos, broker=broker, exit_engine=ee)
    assert pos.quantity_remaining == 3

    action2 = recover_exit_position(pos, broker=broker, exit_engine=ee)
    assert pos.quantity_remaining == 3, (
        f"Duplicate FILLED callback for the same order must NOT double-decrement "
        f"quantity_remaining; got {pos.quantity_remaining}"
    )
    assert pos.closed is False
    assert action2.action == "PARTIAL_FILL_APPLIED"


def test_duplicate_filled_callback_malformed_quantity_also_idempotent_noop():
    """A duplicate FILLED callback where the SECOND report is malformed
    must still hold -- never manufacture a fill from the malformed second
    report, and must not corrupt state already correctly applied by the
    first (valid) report."""
    pos = _make_pos(quantity_remaining=4, pending_exit_qty=4, pending_broker_order_id="dup-malformed-bid")
    ee = _RealisticExitEngine(pos)

    valid_broker = _make_broker(get_order_result={"id": "dup-malformed-bid", "status": "filled", "quantity": 1})
    recover_exit_position(pos, broker=valid_broker, exit_engine=ee)
    assert pos.quantity_remaining == 3

    malformed_broker = _make_broker(get_order_result={"id": "dup-malformed-bid", "status": "filled", "quantity": 0.5})
    action2 = recover_exit_position(pos, broker=malformed_broker, exit_engine=ee)

    assert pos.quantity_remaining == 3, (
        f"A malformed second report for the same order must not mutate state "
        f"already correctly applied; got remaining={pos.quantity_remaining}"
    )
    assert pos.closed is False
    assert action2.action == "NOOP"
    assert action2.reason == "broker_filled_quantity_malformed_hold"
