"""
tests/test_p0_scale_out_fill_recovery_no_false_close.py
==========================================================
P0 regression tests — PR #481 amendment 5, Blocker 3

Defect addressed
-----------------
ap/exit_autonomous_recovery.py::recover_exit_position() interpreted every
broker order status "filled" on the pending exit as full closure of the
ENTIRE managed position, unconditionally calling exit_engine.mark_
position_closed() -- which hard-sets quantity_remaining=0 and closed=True.

A SCALE_OUT tranche independently reports "filled" for just that
tranche's quantity while genuine broker exposure remains open on the rest
of the position (e.g. position holds 4 contracts, a SCALE_OUT order for 1
contract fills -- the other 3 contracts are still a live, broker-held
position that needs continued management). Routing that fill through
mark_position_closed() would silently drop 3 real contracts from
management, understating live exposure.

Binding invariant under test
------------------------------
Reuse the existing canonical partial-fill/full-close classification
(ap_exit_engine.py::APExitEngine.note_partial_exit_fill) rather than
duplicating a second scale-out algorithm in autonomous recovery:

  filled exit qty < remaining exposure
      -> apply/note partial exit fill, reduce quantity_remaining
         correctly, keep the position OPEN/PARTIAL, remaining exposure
         stays managed, do NOT call mark_position_closed

  filled exit consumes exact remaining exposure
      -> full-close path allowed (quantity_remaining reaches 0)

  filled quantity / remaining quantity uncertain (no canonical handler
  available AND remaining exposure is tracked but not proven consumed)
      -> HOLD / conservative partial treatment, never manufacture CLOSED

Test cases (per amendment spec)
---------------------------------
A. position qty=4, SCALE_OUT fills 1 -> remaining=3, not CLOSED
B. position qty=4, SCALE_OUT fills 2 -> remaining=2, not CLOSED
C. position previously scaled 4->3, next scale fills 1 (different
   broker_order_id) -> remaining=2, cumulative accounting correct, no
   double subtraction
D. exact final EXIT fills all remaining -> CLOSED is allowed
E. duplicate FILLED callback/recovery pass (same broker_order_id, same
   fill amount) -> idempotent, no double decrement
F. broker FILLED but fill quantity unavailable/ambiguous -> no
   fabricated full close
G. broker FILLED scale-out with no canonical handler available, but
   quantity_remaining is tracked and proves the fill is partial ->
   remaining broker truth wins, never CLOSED while exposure remains
"""

from __future__ import annotations

import os
import types
from typing import Any

os.environ.setdefault("DATABASE_URL", "postgresql://x:x@localhost/x")

from ap.exit_autonomous_recovery import recover_exit_position  # noqa: E402

_TARGET_OCC = "SMCI260626P00032500"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_pos(
    *,
    quantity_remaining: int = 4,
    pending_broker_order_id: str = "exit-bid-1",
    client_id: str = "test_client_001",
    position_id: str = "pos-scaleout-001",
) -> Any:
    return types.SimpleNamespace(
        position_id=position_id,
        client_id=client_id,
        pending_exit_local_order_id="",
        pending_exit_broker_order_id=pending_broker_order_id,
        pending_exit_qty=quantity_remaining,
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
    semantics closely enough to prove correct integration, without
    depending on the full production exit engine."""

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
            "fill_price": fill_price,
            "local_order_id": local_order_id,
            "broker_order_id": broker_order_id,
            "cumulative_filled": cumulative_filled,
            "cumulative_filled_qty": cumulative_filled_qty,
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
            return  # duplicate / zero-delta fill ignored, exactly like production

        self._pos.quantity_remaining = max(0, int(self._pos.quantity_remaining or 0) - delta)
        if self._pos.quantity_remaining <= 0:
            self._pos.closed = True

    def mark_position_closed(self, pid: str, **_: Any) -> None:
        self.mark_position_closed_calls.append(pid)
        self._pos.closed = True
        self._pos.quantity_remaining = 0


class _MinimalExitEngine:
    """Legacy-style exit engine test double: only implements
    mark_position_closed, no canonical partial-fill helper. Used to prove
    the fallback path never fabricates a full close when quantity_remaining
    proves the fill is partial."""

    def __init__(self) -> None:
        self.mark_position_closed_calls: list[str] = []

    def mark_position_closed(self, pid: str, **_: Any) -> None:
        self.mark_position_closed_calls.append(pid)


# ---------------------------------------------------------------------------
# CASE A -- position qty=4, SCALE_OUT fills 1 -> remaining=3, not CLOSED
# ---------------------------------------------------------------------------

def test_case_a_scale_out_fills_one_of_four_remains_open():
    pos = _make_pos(quantity_remaining=4, pending_broker_order_id="scale-bid-1")
    ee = _RealisticExitEngine(pos)
    broker = _make_broker(get_order_result={"id": "scale-bid-1", "status": "filled", "quantity": 1})

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert pos.quantity_remaining == 3, f"Expected remaining=3 after 1-of-4 fill; got {pos.quantity_remaining}"
    assert pos.closed is False, "Position must NOT be closed after a partial SCALE_OUT fill"
    assert len(ee.mark_position_closed_calls) == 0, (
        f"mark_position_closed must NOT be called for a partial fill; "
        f"got calls={ee.mark_position_closed_calls}"
    )
    assert action.action == "PARTIAL_FILL_APPLIED", (
        f"Expected PARTIAL_FILL_APPLIED; got action={action.action} reason={action.reason}"
    )


# ---------------------------------------------------------------------------
# CASE B -- position qty=4, SCALE_OUT fills 2 -> remaining=2, not CLOSED
# ---------------------------------------------------------------------------

def test_case_b_scale_out_fills_two_of_four_remains_open():
    pos = _make_pos(quantity_remaining=4, pending_broker_order_id="scale-bid-1")
    ee = _RealisticExitEngine(pos)
    broker = _make_broker(get_order_result={"id": "scale-bid-1", "status": "filled", "quantity": 2})

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert pos.quantity_remaining == 2
    assert pos.closed is False
    assert len(ee.mark_position_closed_calls) == 0
    assert action.action == "PARTIAL_FILL_APPLIED"


# ---------------------------------------------------------------------------
# CASE C -- previously scaled 4->3, next scale fills 1 (different order) ->
# remaining=2, cumulative accounting correct, no double subtraction
# ---------------------------------------------------------------------------

def test_case_c_second_scale_out_tranche_different_order_no_double_subtraction():
    pos = _make_pos(quantity_remaining=3, pending_broker_order_id="scale-bid-2")
    ee = _RealisticExitEngine(pos)
    # Simulate the first tranche having already been applied against a
    # DIFFERENT broker_order_id.
    ee._cum_fill_by_order["scale-bid-1"] = 1
    broker = _make_broker(get_order_result={"id": "scale-bid-2", "status": "filled", "quantity": 1})

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert pos.quantity_remaining == 2, (
        f"Expected remaining=2 (3 - 1 from this NEW tranche); got {pos.quantity_remaining}"
    )
    assert pos.closed is False
    assert action.action == "PARTIAL_FILL_APPLIED"


# ---------------------------------------------------------------------------
# CASE D -- exact final EXIT fills all remaining -> CLOSED is allowed
# ---------------------------------------------------------------------------

def test_case_d_final_fill_consumes_all_remaining_closes():
    pos = _make_pos(quantity_remaining=2, pending_broker_order_id="final-bid")
    ee = _RealisticExitEngine(pos)
    broker = _make_broker(get_order_result={"id": "final-bid", "status": "filled", "quantity": 2})

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert pos.quantity_remaining == 0
    assert pos.closed is True
    assert action.action == "MARKED_CLOSED", (
        f"Expected MARKED_CLOSED when fill consumes all remaining exposure; "
        f"got action={action.action} reason={action.reason}"
    )


# ---------------------------------------------------------------------------
# CASE E -- duplicate FILLED callback/recovery pass -> idempotent
# ---------------------------------------------------------------------------

def test_case_e_duplicate_recovery_pass_same_order_idempotent():
    pos = _make_pos(quantity_remaining=4, pending_broker_order_id="scale-bid-1")
    ee = _RealisticExitEngine(pos)
    broker = _make_broker(get_order_result={"id": "scale-bid-1", "status": "filled", "quantity": 1})

    action1 = recover_exit_position(pos, broker=broker, exit_engine=ee)
    assert pos.quantity_remaining == 3

    # Second recovery pass for the SAME still-pending broker_order_id and
    # SAME reported fill quantity (e.g. recovery ran again before the
    # position's pending identity was cleared).
    action2 = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert pos.quantity_remaining == 3, (
        f"Duplicate recovery pass for the same order must NOT double-decrement "
        f"quantity_remaining; got {pos.quantity_remaining}"
    )
    assert pos.closed is False
    assert action2.action == "PARTIAL_FILL_APPLIED"


# ---------------------------------------------------------------------------
# CASE F -- broker FILLED but fill quantity unavailable/ambiguous
# ---------------------------------------------------------------------------

def test_case_f_filled_status_ambiguous_quantity_no_fabricated_close():
    """When the order carries no usable quantity field at all, _qty()
    falls back to pos.pending_exit_qty. This is still routed through the
    canonical partial-fill helper rather than a blind full close -- if
    pending_exit_qty understates true remaining exposure, the position
    must not be closed out from under real broker exposure."""
    pos = _make_pos(quantity_remaining=4, pending_broker_order_id="ambiguous-bid")
    pos.pending_exit_qty = 1  # only the scale-out tranche was pending, not all 4
    ee = _RealisticExitEngine(pos)
    # No usable quantity field on the order at all.
    broker = _make_broker(get_order_result={"id": "ambiguous-bid", "status": "filled"})

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert pos.quantity_remaining == 3, (
        f"Ambiguous fill quantity must fall back to pending_exit_qty and still "
        f"be treated as a partial fill via the canonical handler, never a "
        f"fabricated full close; got remaining={pos.quantity_remaining}"
    )
    assert pos.closed is False
    assert len(ee.mark_position_closed_calls) == 0
    assert action.action == "PARTIAL_FILL_APPLIED"


# ---------------------------------------------------------------------------
# CASE G -- no canonical handler available, quantity_remaining proves
# partial -> HOLD, never CLOSED while exposure remains
# ---------------------------------------------------------------------------

def test_case_g_no_canonical_handler_quantity_remaining_proves_partial_holds():
    pos = _make_pos(quantity_remaining=4, pending_broker_order_id="scale-bid-1")
    ee = _MinimalExitEngine()  # no note_partial_exit_fill available
    broker = _make_broker(get_order_result={"id": "scale-bid-1", "status": "filled", "quantity": 1})

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert len(ee.mark_position_closed_calls) == 0, (
        f"mark_position_closed must NOT be called when quantity_remaining "
        f"proves the fill is partial and no canonical partial-fill handler "
        f"is available; got calls={ee.mark_position_closed_calls}"
    )
    assert action.action == "NOOP", (
        f"Expected NOOP/HOLD when the fill is provably partial but no "
        f"canonical handler can apply it; got action={action.action} reason={action.reason}"
    )
    assert pos.quantity_remaining == 4, "quantity_remaining must be untouched when holding"
    assert pos.closed is False


def test_case_g_variant_no_canonical_handler_full_fill_still_closes():
    """Regression / normal-path preservation: when NO canonical handler is
    available but the fill quantity IS proven to consume all remaining
    exposure, the legacy full-close fallback still applies (existing
    behavior for exit_engine doubles that predate this amendment)."""
    pos = _make_pos(quantity_remaining=1, pending_broker_order_id="final-bid")
    ee = _MinimalExitEngine()
    broker = _make_broker(get_order_result={"id": "final-bid", "status": "filled", "quantity": 1})

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert len(ee.mark_position_closed_calls) == 1
    assert action.action == "MARKED_CLOSED"


def test_case_g_variant_no_quantity_remaining_tracked_preserves_legacy_behavior():
    """Full regression guard: a position object that doesn't track
    quantity_remaining AT ALL (e.g. a minimal/legacy test double, as used
    throughout amendments 1-4's test suites) must preserve the exact
    pre-amendment-5 behavior -- full close via mark_position_closed --
    since there is no information available to determine partial-vs-full."""
    pos = types.SimpleNamespace(
        position_id="pos-legacy-001",
        client_id="test_client_001",
        pending_exit_local_order_id="",
        pending_exit_broker_order_id="exact-filled-bid",
        pending_exit_qty=1,
        contracts=1,
        option_symbol=_TARGET_OCC,
        last_option_quote_update_ts=None,
        last_underlying_quote_update_ts=None,
        last_option_quote_missing_ts=None,
        last_underlying_quote_missing_ts=None,
        # deliberately no quantity_remaining attribute at all
    )
    ee = _MinimalExitEngine()
    broker = _make_broker(get_order_result={"id": "exact-filled-bid", "status": "filled", "quantity": 1})

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert len(ee.mark_position_closed_calls) == 1, (
        "Legacy position objects without quantity_remaining tracking must "
        "preserve pre-amendment-5 full-close behavior"
    )
    assert action.action == "MARKED_CLOSED"
