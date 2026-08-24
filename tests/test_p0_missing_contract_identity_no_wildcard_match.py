"""
tests/test_p0_missing_contract_identity_no_wildcard_match.py
===============================================================
P0 regression tests — PR #481 amendment 3

Defect addressed
-----------------
Amendment 2 correctly prevented missing/unproven contract identity from
becoming authoritative broker-flat truth, but its HOLD guard executed too
late in ap/exit_autonomous_recovery.py::recover_exit_position(). Before
that guard runs, recover_exit_position() calls _matching_open_exit_orders()
directly at two call sites:

  1. the terminal-pending-broker-id path (scanning for a "different live
     exit" before authorizing replacement), and
  2. the generic/missing-broker-id path (scanning for any matching live
     exit order).

_matching_open_exit_orders() treats an empty/falsy contract as a wildcard:

    if contract and _contract(raw) != contract:
        continue

When contract == "", this condition is always False, so no row is ever
filtered out by contract — every exit-like open order in the account
becomes a "match" regardless of which position it actually belongs to.

This creates real cross-position order-identity risk:
  - a single unrelated live sell-to-close order could be adopted via
    set_pending_exit_order() for the wrong position;
  - multiple unrelated live sell-to-close orders could be CANCELED via
    _cancel_order_with_proof() -- broker cancel authority exercised
    against orders that were never proven to belong to this position;
  - a terminal-pending-broker-id recovery could adopt an unrelated
    broker order as the "different live exit still open" for this
    position.

Binding invariant under test
------------------------------
UNPROVEN CONTRACT IDENTITY MUST NEVER AUTHORIZE:
  - wildcard broker-order matching
  - broker-order adoption
  - broker cancel
  - replacement-safe
  - broker-flat close

Cases (per amendment 3 spec)
------------------------------
A. missing contract, no pending broker id, ONE unrelated live STC order
B. missing contract, no pending broker id, TWO unrelated live STC orders
C. missing contract, terminal pending broker id, one unrelated live STC
D. missing contract, pending broker-id lookup unavailable, unrelated live STC
E. valid contract + one matching live exit (normal-path preservation)
F. valid contract + multiple matching live exits (normal-path preservation)
G. missing contract + exact pending broker id confirmed OPEN (normal-path)
H. missing contract + exact pending broker id confirmed FILLED (normal-path)
"""

from __future__ import annotations

import os
import types
from typing import Any

os.environ.setdefault("DATABASE_URL", "postgresql://x:x@localhost/x")

from ap.exit_autonomous_recovery import recover_exit_position  # noqa: E402

_VALID_OCC = "SMCI260626P00032500"
_UNRELATED_OCC_1 = "TSLA260320C00250000"
_UNRELATED_OCC_2 = "NVDA260117P00120000"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _stc_order(broker_order_id: str, contract: str, *, status: str = "open") -> dict:
    """A live sell-to-close broker order row belonging to an unrelated position."""
    return {
        "id": broker_order_id,
        "symbol": contract,
        "side": "sell_to_close",
        "status": status,
        "quantity": 1,
    }


def _make_pos(
    contract: str,
    *,
    pending_broker_order_id: str = "",
    client_id: str = "test_client_001",
    position_id: str = "pos-wildcard-test-001",
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
    get_order_raises: bool = False,
    list_positions_rows: list[dict] | None = None,
) -> Any:
    broker = types.SimpleNamespace()

    def _list_open_orders(**_kwargs: Any) -> list:
        return list(open_orders or [])

    def _get_order(_broker_order_id: str):
        if get_order_raises:
            raise RuntimeError("Tradier 503")
        return get_order_result

    def _list_positions() -> list:
        return list(list_positions_rows or [])

    broker.list_open_orders = _list_open_orders
    broker.get_order = _get_order
    broker.list_positions = _list_positions
    return broker


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
    def any_order_adopted(self) -> bool:
        return bool(self.set_pending_exit_order_calls)

    @property
    def any_replacement_authorized(self) -> bool:
        return bool(self.mark_exit_replacement_safe_calls or self.clear_exit_in_flight_calls)


def _make_cancel_tracking_broker(open_orders: list[dict]) -> tuple[Any, list[str]]:
    """Broker whose cancel_order records every call, so we can assert
    zero unrelated broker orders were canceled."""
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
        return []

    broker.list_open_orders = _list_open_orders
    broker.get_order = _get_order
    broker.cancel_order = _cancel_order
    broker.list_positions = _list_positions
    return broker, canceled


# ---------------------------------------------------------------------------
# CASE A — missing contract, no pending broker id, ONE unrelated live STC
# ---------------------------------------------------------------------------

def test_case_a_missing_contract_no_pending_id_one_unrelated_order_holds():
    pos = _make_pos("")  # no usable contract identity, no pending broker id
    ee = _ExitEngine()
    broker = _make_broker(
        open_orders=[_stc_order("unrelated-bid-1", _UNRELATED_OCC_1)],
    )

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert action.action == "NOOP", (
        f"Expected NOOP/HOLD for missing contract identity; got action={action.action} reason={action.reason}"
    )
    assert not ee.any_order_adopted, (
        f"An unrelated broker order must NEVER be adopted via set_pending_exit_order "
        f"when contract identity is unproven; got calls={ee.set_pending_exit_order_calls}"
    )
    assert not ee.any_replacement_authorized, (
        "replacement must NOT be authorized when contract identity is unproven"
    )
    assert len(ee.mark_position_closed_calls) == 0


# ---------------------------------------------------------------------------
# CASE B — missing contract, no pending broker id, TWO unrelated live STC
# ---------------------------------------------------------------------------

def test_case_b_missing_contract_no_pending_id_two_unrelated_orders_holds_no_cancel():
    pos = _make_pos("")
    ee = _ExitEngine()
    broker, canceled = _make_cancel_tracking_broker([
        _stc_order("unrelated-bid-1", _UNRELATED_OCC_1),
        _stc_order("unrelated-bid-2", _UNRELATED_OCC_2),
    ])

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert action.action == "NOOP", (
        f"Expected NOOP/HOLD for missing contract identity; got action={action.action} reason={action.reason}"
    )
    assert len(canceled) == 0, (
        f"broker.cancel_order must NEVER be called against unrelated orders when "
        f"contract identity is unproven; got canceled={canceled}"
    )
    assert not ee.any_replacement_authorized, (
        "replacement must NOT be authorized when contract identity is unproven"
    )
    assert len(ee.mark_position_closed_calls) == 0


# ---------------------------------------------------------------------------
# CASE C — missing contract, terminal pending broker id, one unrelated live STC
# ---------------------------------------------------------------------------

def test_case_c_missing_contract_terminal_pending_id_one_unrelated_order_holds():
    pos = _make_pos("", pending_broker_order_id="old-terminal-bid")
    ee = _ExitEngine()
    broker = _make_broker(
        open_orders=[_stc_order("unrelated-bid-1", _UNRELATED_OCC_1)],
        get_order_result={"id": "old-terminal-bid", "status": "canceled"},
    )

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert not ee.any_order_adopted, (
        f"An unrelated broker order must NEVER be adopted as 'different live exit' "
        f"when contract identity is unproven; got calls={ee.set_pending_exit_order_calls}"
    )
    assert not ee.any_replacement_authorized, (
        "replacement-safe must NOT be authorized from terminal status alone when "
        "the same-contract duplicate scan cannot be performed (contract unknown)"
    )
    assert len(ee.mark_position_closed_calls) == 0
    assert action.action == "NOOP", (
        f"Expected NOOP/HOLD; got action={action.action} reason={action.reason}"
    )


# ---------------------------------------------------------------------------
# CASE D — missing contract, pending broker-id lookup unavailable, unrelated live STC
# ---------------------------------------------------------------------------

def test_case_d_missing_contract_pending_id_lookup_unavailable_holds():
    pos = _make_pos("", pending_broker_order_id="unreachable-bid")
    ee = _ExitEngine()
    broker = _make_broker(
        open_orders=[_stc_order("unrelated-bid-1", _UNRELATED_OCC_1)],
        get_order_raises=True,  # broker.get_order() fails -> raw is None
    )

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert not ee.any_order_adopted, (
        f"An unrelated broker order must NEVER be adopted when the pending-broker-id "
        f"lookup is unavailable AND contract identity is unproven; "
        f"got calls={ee.set_pending_exit_order_calls}"
    )
    assert not ee.any_replacement_authorized
    assert len(ee.mark_position_closed_calls) == 0
    assert action.action == "NOOP", (
        f"Expected NOOP/HOLD; got action={action.action} reason={action.reason}"
    )


# ---------------------------------------------------------------------------
# CASE E — normal-path preservation: valid contract + one matching live exit
# ---------------------------------------------------------------------------

def test_case_e_valid_contract_one_matching_exit_still_recovers():
    pos = _make_pos(_VALID_OCC)
    ee = _ExitEngine()
    broker = _make_broker(
        open_orders=[_stc_order("matching-bid-1", _VALID_OCC)],
    )

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert action.action == "RECOVERED_BROKER_ID", (
        f"Existing broker-id recovery must still work for a valid contract match; "
        f"got action={action.action} reason={action.reason}"
    )
    assert len(ee.set_pending_exit_order_calls) == 1
    assert ee.set_pending_exit_order_calls[0]["broker_order_id"] == "matching-bid-1"


# ---------------------------------------------------------------------------
# CASE F — normal-path preservation: valid contract + multiple matching exits
# ---------------------------------------------------------------------------

def test_case_f_valid_contract_multiple_matching_exits_bounded_cancel_unchanged():
    pos = _make_pos(_VALID_OCC)
    ee = _ExitEngine()
    broker, canceled = _make_cancel_tracking_broker([
        _stc_order("matching-bid-1", _VALID_OCC),
        _stc_order("matching-bid-2", _VALID_OCC),
    ])

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    # Existing bounded cancel/proof behavior: both matching orders (same
    # contract, genuinely ambiguous) get canceled, then replacement-safe.
    assert set(canceled) == {"matching-bid-1", "matching-bid-2"}, (
        f"Existing multi-match same-contract cancel behavior must be unchanged; "
        f"got canceled={canceled}"
    )
    assert ee.any_replacement_authorized, (
        "After successfully canceling all matched same-contract duplicates, "
        "replacement-safe should still be authorized (unchanged existing behavior)"
    )


# ---------------------------------------------------------------------------
# CASE G — normal-path preservation: missing contract + exact pending id OPEN
# ---------------------------------------------------------------------------

def test_case_g_missing_contract_exact_pending_id_open_still_confirms():
    pos = _make_pos("", pending_broker_order_id="exact-open-bid")
    ee = _ExitEngine()
    broker = _make_broker(
        get_order_result={"id": "exact-open-bid", "status": "open"},
    )

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert action.action == "CONFIRMED_OPEN", (
        f"Exact broker-id OPEN truth must remain valid even when contract-based "
        f"scanning is unavailable; got action={action.action} reason={action.reason}"
    )


# ---------------------------------------------------------------------------
# CASE H — normal-path preservation: missing contract + exact pending id FILLED
# ---------------------------------------------------------------------------

def test_case_h_missing_contract_exact_pending_id_filled_still_closes():
    pos = _make_pos("", pending_broker_order_id="exact-filled-bid")
    ee = _ExitEngine()
    broker = _make_broker(
        get_order_result={"id": "exact-filled-bid", "status": "filled", "filled_qty": 1, "avg_fill_price": 1.5},
    )

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert action.action == "MARKED_CLOSED", (
        f"Exact broker-id FILLED truth must remain valid and NOT be blocked merely "
        f"because contract-based scanning is unavailable; "
        f"got action={action.action} reason={action.reason}"
    )
    assert len(ee.mark_position_closed_calls) == 1


# ---------------------------------------------------------------------------
# Defense-in-depth — _matching_open_exit_orders() itself never wildcards
# ---------------------------------------------------------------------------

def test_defense_in_depth_matching_helper_never_wildcards_empty_contract():
    """Direct unit test of the helper: an empty/falsy contract must never
    match any row, regardless of caller-level guards."""
    from ap.exit_autonomous_recovery import _matching_open_exit_orders

    broker = _make_broker(
        open_orders=[
            _stc_order("unrelated-bid-1", _UNRELATED_OCC_1),
            _stc_order("unrelated-bid-2", _UNRELATED_OCC_2),
        ],
    )

    for empty_contract in ("", None):
        matches = _matching_open_exit_orders(broker, empty_contract)  # type: ignore[arg-type]
        assert matches == [], (
            f"_matching_open_exit_orders must return [] for empty/None contract "
            f"(contract={empty_contract!r}); got {matches!r}"
        )
