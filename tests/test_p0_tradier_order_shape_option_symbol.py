"""
tests/test_p0_tradier_order_shape_option_symbol.py
=====================================================
P0 regression tests — PR #481 amendment 4, Blocker 2

Defect addressed
-----------------
ap/exit_autonomous_recovery.py::_contract(raw) checked broker order fields
in this order:

    contract OR symbol OR option_symbol OR instrument

Real Tradier option orders carry TWO distinct fields:

    symbol         = the underlying ticker (e.g. "SMCI")
    option_symbol  = the exact OCC option contract (e.g.
                      "SMCI260626P00032500")

Because `symbol` was checked before `option_symbol`, _contract(raw) for a
production-shaped order row returned the UNDERLYING ("SMCI") instead of
the exact OCC contract. An already-live same-contract sell-to-close order
could therefore be missed by the exact-OCC matching logic used throughout
ap/exit_autonomous_recovery.py, potentially allowing autonomous recovery
to authorize a replacement/duplicate exit while a real live exit for the
same contract was already working at the broker.

Binding invariant under test
------------------------------
The raw underlying `symbol` must NEVER shadow a valid `option_symbol`.
Every autonomous-recovery contract-based broker-order match must compare
proven exact OCC identity, not whichever field happens to be checked
first.

Coverage (per amendment spec, cases A-F)
------------------------------------------
A. one same-OCC live sell_to_close -> RECOVERED_BROKER_ID, exactly that
   order adopted
B. terminal old broker id + one different broker id OPEN on same OCC ->
   existing live EXIT recognized, replacement NOT authorized
C. two live exits on same exact OCC -> bounded existing duplicate
   cancel-then-replacement-safe behavior preserved
D. same underlying but DIFFERENT OCC -> must NOT match/adopt/cancel
E. unrelated underlying/OCC -> must NOT match/adopt/cancel
F. missing/malformed broker-order option identity -> must NOT become
   wildcard, must NOT authorize cancel/adoption
"""

from __future__ import annotations

import os
import types
from typing import Any

os.environ.setdefault("DATABASE_URL", "postgresql://x:x@localhost/x")

from ap.exit_autonomous_recovery import recover_exit_position, _contract  # noqa: E402

_TARGET_UNDERLYING = "SMCI"
_TARGET_OCC = "SMCI260626P00032500"          # the position's actual contract
_SAME_UNDERLYING_DIFFERENT_OCC = "SMCI260626P00035000"  # different strike, same underlying
_UNRELATED_UNDERLYING = "TSLA"
_UNRELATED_OCC = "TSLA260320C00250000"


# ---------------------------------------------------------------------------
# Helpers -- realistic Tradier order shape
# ---------------------------------------------------------------------------

def _tradier_order(
    broker_order_id: str,
    *,
    underlying: str,
    option_symbol: str,
    status: str = "open",
    side: str = "sell_to_close",
) -> dict:
    """Realistic Tradier option-order row: `symbol` is the underlying,
    `option_symbol` is the exact OCC contract -- these are DIFFERENT
    fields carrying different values, matching real production shape."""
    return {
        "id": broker_order_id,
        "class": "option",
        "symbol": underlying,
        "option_symbol": option_symbol,
        "side": side,
        "status": status,
        "quantity": 1,
    }


def _make_pos(
    contract: str,
    *,
    pending_broker_order_id: str = "",
    client_id: str = "test_client_001",
    position_id: str = "pos-order-shape-001",
) -> Any:
    return types.SimpleNamespace(
        position_id=position_id,
        client_id=client_id,
        pending_exit_local_order_id="",
        pending_exit_broker_order_id=pending_broker_order_id,
        pending_exit_qty=1,
        contracts=1,
        option_symbol=contract,
        last_option_quote_update_ts=None,
        last_underlying_quote_update_ts=None,
        last_option_quote_missing_ts=None,
        last_underlying_quote_missing_ts=None,
    )


def _make_broker(
    *,
    open_orders: list[dict] | None = None,
    get_order_result: dict | None = None,
) -> Any:
    broker = types.SimpleNamespace()

    def _list_open_orders(**_kwargs: Any) -> list:
        return list(open_orders or [])

    def _get_order(_broker_order_id: str):
        return get_order_result

    def _list_positions() -> list:
        return [{"symbol": _TARGET_OCC, "quantity": 1}]

    broker.list_open_orders = _list_open_orders
    broker.get_order = _get_order
    broker.list_positions = _list_positions
    return broker


def _make_cancel_tracking_broker(open_orders: list[dict]) -> tuple[Any, list[str]]:
    canceled: list[str] = []
    broker = types.SimpleNamespace()

    def _list_open_orders(**_kwargs: Any) -> list:
        return list(open_orders)

    def _get_order(_broker_order_id: str):
        return None

    def _cancel_order(broker_order_id: str) -> dict:
        canceled.append(broker_order_id)
        return {"status": "canceled", "ok": True}

    def _list_positions() -> list:
        return [{"symbol": _TARGET_OCC, "quantity": 1}]

    broker.list_open_orders = _list_open_orders
    broker.get_order = _get_order
    broker.cancel_order = _cancel_order
    broker.list_positions = _list_positions
    return broker, canceled


class _ExitEngine:
    def __init__(self) -> None:
        self.set_pending_exit_order_calls: list[dict] = []
        self.mark_position_closed_calls: list[str] = []
        self.mark_exit_replacement_safe_calls: list[str] = []
        self.clear_exit_in_flight_calls: list[str] = []

    def set_pending_exit_order(self, pid: str, **kwargs: Any) -> None:
        self.set_pending_exit_order_calls.append({"pid": pid, **kwargs})

    def mark_position_closed(self, pid: str, **_: Any) -> None:
        self.mark_position_closed_calls.append(pid)

    def mark_exit_replacement_safe(self, pid: str, **_: Any) -> None:
        self.mark_exit_replacement_safe_calls.append(pid)

    def clear_exit_in_flight(self, pid: str, **_: Any) -> None:
        self.clear_exit_in_flight_calls.append(pid)

    @property
    def any_replacement_authorized(self) -> bool:
        return bool(self.mark_exit_replacement_safe_calls or self.clear_exit_in_flight_calls)


# ---------------------------------------------------------------------------
# Direct unit test of _contract() against real Tradier order shape
# ---------------------------------------------------------------------------

def test_contract_extractor_prefers_option_symbol_over_underlying_symbol():
    raw = _tradier_order("bid-1", underlying=_TARGET_UNDERLYING, option_symbol=_TARGET_OCC)
    result = _contract(raw)
    assert result == _TARGET_OCC, (
        f"_contract() must extract the exact OCC option_symbol, not the underlying "
        f"symbol; got {result!r} (underlying was {_TARGET_UNDERLYING!r})"
    )


# ---------------------------------------------------------------------------
# CASE A -- one same-OCC live sell_to_close is correctly matched and adopted
# ---------------------------------------------------------------------------

def test_case_a_same_occ_live_exit_recovered_via_broker_id():
    pos = _make_pos(_TARGET_OCC)
    ee = _ExitEngine()
    broker = _make_broker(
        open_orders=[
            _tradier_order("matching-bid-1", underlying=_TARGET_UNDERLYING, option_symbol=_TARGET_OCC),
        ],
    )

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert action.action == "RECOVERED_BROKER_ID", (
        f"Expected RECOVERED_BROKER_ID for a same-OCC live exit in real Tradier "
        f"order shape; got action={action.action} reason={action.reason}"
    )
    assert len(ee.set_pending_exit_order_calls) == 1
    assert ee.set_pending_exit_order_calls[0]["broker_order_id"] == "matching-bid-1"


# ---------------------------------------------------------------------------
# CASE B -- terminal old broker id + one different broker id OPEN on same OCC
# ---------------------------------------------------------------------------

def test_case_b_terminal_old_id_different_live_exit_same_occ_recognized():
    pos = _make_pos(_TARGET_OCC, pending_broker_order_id="old-terminal-bid")
    ee = _ExitEngine()
    broker = _make_broker(
        open_orders=[
            _tradier_order("different-live-bid", underlying=_TARGET_UNDERLYING, option_symbol=_TARGET_OCC),
        ],
        get_order_result={"id": "old-terminal-bid", "status": "canceled"},
    )

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert action.action == "CONFIRMED_OPEN", (
        f"Expected the different live exit on the same exact OCC to be recognized "
        f"in real Tradier order shape; got action={action.action} reason={action.reason}"
    )
    assert not ee.any_replacement_authorized, (
        "replacement must NOT be authorized when a different live exit on the "
        "same exact OCC contract is still open"
    )
    assert len(ee.set_pending_exit_order_calls) == 1
    assert ee.set_pending_exit_order_calls[0]["broker_order_id"] == "different-live-bid"


# ---------------------------------------------------------------------------
# CASE C -- two live exits on same exact OCC: bounded cancel behavior preserved
# ---------------------------------------------------------------------------

def test_case_c_two_live_exits_same_occ_bounded_cancel_preserved():
    pos = _make_pos(_TARGET_OCC)
    ee = _ExitEngine()
    broker, canceled = _make_cancel_tracking_broker([
        _tradier_order("dup-bid-1", underlying=_TARGET_UNDERLYING, option_symbol=_TARGET_OCC),
        _tradier_order("dup-bid-2", underlying=_TARGET_UNDERLYING, option_symbol=_TARGET_OCC),
    ])

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert set(canceled) == {"dup-bid-1", "dup-bid-2"}, (
        f"Both duplicate same-OCC live exits must be canceled in real Tradier "
        f"order shape; got canceled={canceled}"
    )
    assert ee.any_replacement_authorized, (
        "After successfully canceling all matched same-OCC duplicates, "
        "replacement-safe should be authorized (unchanged existing behavior)"
    )


# ---------------------------------------------------------------------------
# CASE D -- same underlying, DIFFERENT OCC (different strike) must not match
# ---------------------------------------------------------------------------

def test_case_d_same_underlying_different_occ_never_matches():
    pos = _make_pos(_TARGET_OCC)
    ee = _ExitEngine()
    broker, canceled = _make_cancel_tracking_broker([
        _tradier_order("wrong-strike-bid", underlying=_TARGET_UNDERLYING, option_symbol=_SAME_UNDERLYING_DIFFERENT_OCC),
    ])

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert len(canceled) == 0, (
        f"An order on the same underlying but a DIFFERENT exact OCC contract "
        f"must NEVER be canceled; got canceled={canceled}"
    )
    assert len(ee.set_pending_exit_order_calls) == 0, (
        f"An order on the same underlying but a DIFFERENT exact OCC contract "
        f"must NEVER be adopted; got calls={ee.set_pending_exit_order_calls}"
    )


# ---------------------------------------------------------------------------
# CASE E -- unrelated underlying/OCC must not match/adopt/cancel
# ---------------------------------------------------------------------------

def test_case_e_unrelated_underlying_and_occ_never_matches():
    pos = _make_pos(_TARGET_OCC)
    ee = _ExitEngine()
    broker, canceled = _make_cancel_tracking_broker([
        _tradier_order("unrelated-bid", underlying=_UNRELATED_UNDERLYING, option_symbol=_UNRELATED_OCC),
    ])

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert len(canceled) == 0
    assert len(ee.set_pending_exit_order_calls) == 0


# ---------------------------------------------------------------------------
# CASE F -- missing/malformed broker-order option identity must not wildcard
# ---------------------------------------------------------------------------

def test_case_f_missing_option_identity_on_broker_order_never_wildcards():
    """A broker order row with no usable option identity at all (no
    option_symbol, and a non-OCC-shaped or missing symbol) must never be
    treated as a match for ANY position's exact OCC target."""
    pos = _make_pos(_TARGET_OCC)
    ee = _ExitEngine()
    malformed_order = {
        "id": "malformed-bid",
        "class": "option",
        "symbol": "",           # no usable underlying either
        "option_symbol": "",
        "side": "sell_to_close",
        "status": "open",
        "quantity": 1,
    }
    broker, canceled = _make_cancel_tracking_broker([malformed_order])

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert len(canceled) == 0, (
        f"A broker order with no usable option identity must never be canceled "
        f"as a phantom match; got canceled={canceled}"
    )
    assert len(ee.set_pending_exit_order_calls) == 0, (
        f"A broker order with no usable option identity must never be adopted; "
        f"got calls={ee.set_pending_exit_order_calls}"
    )


def test_case_f_variant_underlying_only_no_option_symbol_never_matches():
    """A broker order that only carries the bare underlying symbol (e.g. a
    stock order accidentally swept into the open-orders scan) must never
    match an option position's exact OCC target."""
    pos = _make_pos(_TARGET_OCC)
    ee = _ExitEngine()
    stock_shaped_order = {
        "id": "stock-order-bid",
        "class": "equity",
        "symbol": _TARGET_UNDERLYING,   # bare underlying, no option_symbol at all
        "side": "sell_to_close",
        "status": "open",
        "quantity": 1,
    }
    broker, canceled = _make_cancel_tracking_broker([stock_shaped_order])

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert len(canceled) == 0, (
        f"A bare-underlying (non-OCC) order must never match an option "
        f"position's exact OCC target; got canceled={canceled}"
    )
    assert len(ee.set_pending_exit_order_calls) == 0
