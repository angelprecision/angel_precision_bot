"""
tests/test_p0_filled_quantity_conflict_final_guard.py
========================================================
P0 regression tests — PR #481 final merge-gate (FILLED quantity
absent-vs-malformed + multi-field conflict detection)

Defect addressed
-----------------
_strict_filled_exit_qty() previously checked `val = raw.get(key); if val
is None or val == "": continue`, which treated a key that is PRESENT with
value None/""/whitespace identically to a key that is truly ABSENT from
the payload. That let a present-but-unusable field silently qualify for
the ABSENT-only pending_exit_qty fallback, when it should have HELD as
conflict evidence instead.

The parser also only consulted the FIRST recognized key that carried a
non-empty value, so a valid secondary field could mask a malformed
primary field, and mutually disagreeing present fields (e.g. a generic
"quantity" and a fill-specific "exec_quantity" reporting different
amounts) were never detected as conflicting.

Binding invariant under test
------------------------------
A. key missing entirely from the payload -> ABSENT -> the narrowly
   bounded pending_exit_qty fallback may still apply.
   key present with None / "" / whitespace-only -> MALFORMED/CONFLICT ->
   HOLD, no fill application, no close.

B. ALL recognized FILLED quantity fields are inspected:
   - zero keys present -> ABSENT
   - one proven positive integral value -> VALID
   - multiple present fields that all agree on the same positive integer
     -> VALID
   - any present malformed field -> CONFLICT/HOLD
   - multiple valid fields that disagree -> CONFLICT/HOLD

No fallback on conflict. No cumulative fill. No mark_position_closed.
No new broker submit/cancel authority.
"""

from __future__ import annotations

import os
import types
from typing import Any

os.environ.setdefault("DATABASE_URL", "postgresql://x:x@localhost/x")

import pytest  # noqa: E402

from ap.exit_autonomous_recovery import (  # noqa: E402
    recover_exit_position,
    _strict_filled_exit_qty,
)

_TARGET_OCC = "SMCI260626P00032500"


# ---------------------------------------------------------------------------
# Helpers -- reused pattern from prior amendment test files
# ---------------------------------------------------------------------------

def _make_pos(*, quantity_remaining: int = 4, pending_exit_qty: int = 4, pending_broker_order_id: str = "exit-bid-1", position_id: str = "pos-conflict-final-001") -> Any:
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
    """Mirrors ap_exit_engine.py::APExitEngine.note_partial_exit_fill's
    cumulative-fill-per-order-key tracking, reused from prior amendment
    test files."""

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
# Direct unit tests of _strict_filled_exit_qty
# ---------------------------------------------------------------------------

def test_key_truly_absent_is_absent():
    result = _strict_filled_exit_qty({"id": "x", "status": "filled"})
    assert result == (None, False)


def test_quantity_none_present_is_conflict_not_absent():
    result = _strict_filled_exit_qty({"quantity": None})
    assert result == (None, True), f"expected CONFLICT (None, True); got {result!r}"


def test_quantity_empty_string_is_conflict():
    result = _strict_filled_exit_qty({"quantity": ""})
    assert result == (None, True)


def test_quantity_whitespace_string_is_conflict():
    result = _strict_filled_exit_qty({"quantity": "   "})
    assert result == (None, True)


def test_filled_qty_none_is_conflict():
    result = _strict_filled_exit_qty({"filled_qty": None})
    assert result == (None, True)


def test_exec_quantity_none_is_conflict():
    result = _strict_filled_exit_qty({"exec_quantity": None})
    assert result == (None, True)


def test_quantity_and_exec_quantity_agree_is_valid():
    result = _strict_filled_exit_qty({"quantity": 4, "exec_quantity": 4})
    assert result == (4, True)


def test_quantity_and_exec_quantity_disagree_is_conflict():
    result = _strict_filled_exit_qty({"quantity": 4, "exec_quantity": 1})
    assert result == (None, True)


def test_quantity_and_filled_quantity_disagree_is_conflict():
    result = _strict_filled_exit_qty({"quantity": 1, "filled_quantity": 4})
    assert result == (None, True)


def test_quantity_valid_exec_quantity_negative_is_conflict():
    result = _strict_filled_exit_qty({"quantity": 4, "exec_quantity": -1})
    assert result == (None, True)


def test_quantity_valid_filled_quantity_fractional_is_conflict():
    result = _strict_filled_exit_qty({"quantity": 4, "filled_quantity": 0.5})
    assert result == (None, True)


def test_no_recognized_keys_whatsoever_is_absent():
    result = _strict_filled_exit_qty({"id": "x", "status": "filled", "unrelated_field": 123})
    assert result == (None, False)


def test_single_valid_quantity_one():
    assert _strict_filled_exit_qty({"quantity": 1}) == (1, True)


def test_single_valid_quantity_four():
    assert _strict_filled_exit_qty({"quantity": 4}) == (4, True)


def test_filled_qty_and_filled_quantity_agree_is_valid():
    result = _strict_filled_exit_qty({"filled_qty": 1, "filled_quantity": 1})
    assert result == (1, True)


# ---------------------------------------------------------------------------
# End-to-end tests through recover_exit_position
# ---------------------------------------------------------------------------

def test_e2e_no_recognized_keys_uses_absent_fallback():
    pos = _make_pos(quantity_remaining=4, pending_exit_qty=4, pending_broker_order_id="e2e-absent")
    ee = _RealisticExitEngine(pos)
    broker = _make_broker(get_order_result={"id": "e2e-absent", "status": "filled"})
    action = recover_exit_position(pos, broker=broker, exit_engine=ee)
    assert pos.quantity_remaining == 0
    assert pos.closed is True
    assert action.action == "MARKED_CLOSED"


@pytest.mark.parametrize("raw_extra,label", [
    ({"quantity": None}, "quantity_none"),
    ({"quantity": ""}, "quantity_empty_string"),
    ({"quantity": "   "}, "quantity_whitespace"),
    ({"filled_qty": None}, "filled_qty_none"),
    ({"exec_quantity": None}, "exec_quantity_none"),
    ({"quantity": 4, "exec_quantity": 1}, "quantity_exec_quantity_disagree"),
    ({"quantity": 1, "filled_quantity": 4}, "quantity_filled_quantity_disagree"),
    ({"quantity": 4, "exec_quantity": -1}, "quantity_valid_exec_negative"),
    ({"quantity": 4, "filled_quantity": 0.5}, "quantity_valid_filled_fractional"),
])
def test_e2e_conflicting_or_malformed_holds_no_manufactured_fill(raw_extra, label):
    pos = _make_pos(quantity_remaining=4, pending_exit_qty=4, pending_broker_order_id="e2e-conflict")
    ee = _RealisticExitEngine(pos)
    raw = {"id": "e2e-conflict", "status": "filled", **raw_extra}
    broker = _make_broker(get_order_result=raw)

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert pos.quantity_remaining == 4, f"[{label}] must not manufacture any fill; remaining changed"
    assert pos.closed is False, f"[{label}] must remain OPEN"
    assert len(ee.mark_position_closed_calls) == 0, f"[{label}] must not close"
    assert len(ee.note_partial_exit_fill_calls) == 0, f"[{label}] must not apply any fill"
    assert action.action == "NOOP", f"[{label}] expected NOOP; got {action.action}"
    assert action.reason == "broker_filled_quantity_malformed_hold", (
        f"[{label}] expected malformed-hold reason; got {action.reason}"
    )


def test_e2e_quantity_and_exec_quantity_agree_valid_full_close():
    pos = _make_pos(quantity_remaining=4, pending_exit_qty=4, pending_broker_order_id="e2e-agree")
    ee = _RealisticExitEngine(pos)
    broker = _make_broker(get_order_result={"id": "e2e-agree", "status": "filled", "quantity": 4, "exec_quantity": 4})
    action = recover_exit_position(pos, broker=broker, exit_engine=ee)
    assert pos.quantity_remaining == 0
    assert pos.closed is True
    assert action.action == "MARKED_CLOSED"


def test_e2e_valid_single_quantity_one_partial_still_works():
    pos = _make_pos(quantity_remaining=4, pending_exit_qty=4, pending_broker_order_id="e2e-partial")
    ee = _RealisticExitEngine(pos)
    broker = _make_broker(get_order_result={"id": "e2e-partial", "status": "filled", "quantity": 1})
    action = recover_exit_position(pos, broker=broker, exit_engine=ee)
    assert pos.quantity_remaining == 3
    assert pos.closed is False
    assert action.action == "PARTIAL_FILL_APPLIED"


def test_e2e_valid_single_quantity_four_full_close_still_works():
    pos = _make_pos(quantity_remaining=4, pending_exit_qty=4, pending_broker_order_id="e2e-full")
    ee = _RealisticExitEngine(pos)
    broker = _make_broker(get_order_result={"id": "e2e-full", "status": "filled", "quantity": 4})
    action = recover_exit_position(pos, broker=broker, exit_engine=ee)
    assert pos.quantity_remaining == 0
    assert pos.closed is True
    assert action.action == "MARKED_CLOSED"
