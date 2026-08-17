"""
tests/test_p0_broker_order_query_unknown_never_empty.py
==========================================================
P0 regression tests — PR #481 amendment 5, Blocker 2

Defect addressed
-----------------
ap/exit_autonomous_recovery.py::_list_open_orders() caught broker-order
query failures (timeouts, exceptions, unusable/malformed responses, no
supported query method available) and ultimately returned []. That result
is indistinguishable from an authoritative successful broker response
stating that zero open orders exist.

These are two different states that were being collapsed into one:

  SUCCESSFUL BROKER ORDER QUERY, ZERO RESULTS:
    broker request succeeds, zero open orders returned -> authoritative
    empty truth, safe to use as negative proof.

  BROKER ORDER QUERY FAILURE:
    timeout / network exception / broker exception / malformed response /
    no candidate query method available / every candidate method fails
    -> UNKNOWN broker-order truth, NOT proof that zero matching exit
    orders exist.

Money-path consequence: if a real live exit order for the exact position's
contract is still OPEN/WORKING at the broker, but the open-order query
happens to fail during a recovery pass, the failure was silently
interpreted as "no live exit exists" and autonomous recovery could
authorize a duplicate replacement exit -- while the real one was still
working. This violates the module's own stated safety rule: "If broker
truth is ambiguous, alert/no-op."

Binding invariant under test
------------------------------
BROKER ORDER TRUTH MUST BE TRI-STATE:
  1. AVAILABLE_NONEMPTY -- use orders as authoritative evidence.
  2. AVAILABLE_EMPTY -- may serve as negative broker-order proof.
  3. UNKNOWN/UNAVAILABLE -- must NEVER be normalized to [] and used as
     negative proof. Recovery must HOLD/NOOP: never mark replacement
     safe, never clear exit-in-flight, never mark position closed based
     on assumed order absence, never cancel based on incomplete scan,
     never adopt a broker order based on ambiguous identity.
"""

from __future__ import annotations

import os
import types
from typing import Any

os.environ.setdefault("DATABASE_URL", "postgresql://x:x@localhost/x")

from ap.exit_autonomous_recovery import (  # noqa: E402
    recover_exit_position,
    _list_open_orders,
    _matching_open_exit_orders,
)

_TARGET_OCC = "SMCI260626P00032500"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _stc_order(broker_order_id: str, contract: str, *, status: str = "open") -> dict:
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
    position_id: str = "pos-order-truth-001",
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


def _cancel_tracking_broker_kwargs() -> dict:
    canceled: list[str] = []

    def _cancel_order(broker_order_id: str) -> dict:
        canceled.append(broker_order_id)
        return {"status": "canceled", "ok": True}

    return {"cancel_order": _cancel_order}, canceled


# ---------------------------------------------------------------------------
# Direct unit tests of _list_open_orders() tri-state
# ---------------------------------------------------------------------------

def test_list_open_orders_raises_is_unknown_not_empty():
    def _raising_list_open_orders(**_kwargs: Any) -> list:
        raise RuntimeError("Tradier 503")

    broker = types.SimpleNamespace(list_open_orders=_raising_list_open_orders)
    result = _list_open_orders(broker)
    assert result is None, f"Expected None (UNKNOWN) for raising query; got {result!r}"


def test_get_open_orders_raises_is_unknown_not_empty():
    def _raising_get_open_orders(**_kwargs: Any) -> list:
        raise ConnectionError("network unreachable")

    broker = types.SimpleNamespace(get_open_orders=_raising_get_open_orders)
    result = _list_open_orders(broker)
    assert result is None, f"Expected None (UNKNOWN) for raising query; got {result!r}"


def test_all_supported_methods_raise_is_unknown():
    def _boom(**_kwargs: Any) -> list:
        raise RuntimeError("boom")

    broker = types.SimpleNamespace(
        list_open_orders=_boom,
        get_open_orders=_boom,
        list_orders=_boom,
        orders=_boom,
    )
    result = _list_open_orders(broker)
    assert result is None, f"Expected None (UNKNOWN) when all methods raise; got {result!r}"


def test_methods_return_none_is_unknown():
    def _returns_none(**_kwargs: Any) -> None:
        return None

    broker = types.SimpleNamespace(list_open_orders=_returns_none)
    result = _list_open_orders(broker)
    assert result is None, f"Expected None (UNKNOWN) when method returns None; got {result!r}"


def test_methods_return_unusable_shape_is_unknown():
    def _returns_string(**_kwargs: Any) -> str:
        return "not a list or dict"

    broker = types.SimpleNamespace(list_open_orders=_returns_string)
    result = _list_open_orders(broker)
    assert result is None, f"Expected None (UNKNOWN) for unusable payload shape; got {result!r}"


def test_no_supported_method_exists_is_unknown():
    broker = types.SimpleNamespace()  # no list_open_orders/get_open_orders/list_orders/orders at all
    result = _list_open_orders(broker)
    assert result is None, f"Expected None (UNKNOWN) when no query method exists; got {result!r}"


def test_successful_empty_list_is_available_empty_not_unknown():
    def _empty(**_kwargs: Any) -> list:
        return []

    broker = types.SimpleNamespace(list_open_orders=_empty)
    result = _list_open_orders(broker)
    assert result == [], f"Expected [] (AVAILABLE_EMPTY) for a genuine empty result; got {result!r}"
    assert result is not None


def test_successful_nonempty_list_is_available_nonempty():
    def _nonempty(**_kwargs: Any) -> list:
        return [_stc_order("bid-1", _TARGET_OCC)]

    broker = types.SimpleNamespace(list_open_orders=_nonempty)
    result = _list_open_orders(broker)
    assert result == [_stc_order("bid-1", _TARGET_OCC)]


# ---------------------------------------------------------------------------
# _matching_open_exit_orders() propagates UNKNOWN
# ---------------------------------------------------------------------------

def test_matching_open_exit_orders_propagates_unknown():
    def _raising(**_kwargs: Any) -> list:
        raise RuntimeError("boom")

    broker = types.SimpleNamespace(list_open_orders=_raising)
    result = _matching_open_exit_orders(broker, _TARGET_OCC)
    assert result is None, f"Expected None (UNKNOWN) to propagate; got {result!r}"


def test_matching_open_exit_orders_empty_contract_still_confirmed_empty_not_unknown():
    """Amendment 3's empty-contract guard takes precedence -- confirmed []
    by construction (we refuse to scan), distinct from amendment 5's
    UNKNOWN (we tried to scan and couldn't)."""
    def _raising(**_kwargs: Any) -> list:
        raise RuntimeError("boom")

    broker = types.SimpleNamespace(list_open_orders=_raising)
    result = _matching_open_exit_orders(broker, "")
    assert result == [], f"Expected [] for empty contract regardless of broker query state; got {result!r}"


# ---------------------------------------------------------------------------
# End-to-end: generic/missing-broker-id path -- UNKNOWN holds
# ---------------------------------------------------------------------------

def test_missing_broker_id_open_order_scan_unknown_holds():
    pos = _make_pos(_TARGET_OCC)
    ee = _ExitEngine()

    def _raising(**_kwargs: Any) -> list:
        raise RuntimeError("Tradier 503")

    cancel_kwargs, canceled = _cancel_tracking_broker_kwargs()
    broker = types.SimpleNamespace(
        list_open_orders=_raising,
        get_order=lambda _bid: None,
        list_positions=lambda: [{"symbol": _TARGET_OCC, "quantity": 1}],
        **cancel_kwargs,
    )

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert action.action == "NOOP", (
        f"Expected NOOP/HOLD when broker order truth is UNKNOWN; "
        f"got action={action.action} reason={action.reason}"
    )
    assert not ee.any_order_adopted, "must NOT adopt any order when broker order truth is UNKNOWN"
    assert not ee.any_replacement_authorized, "must NOT authorize replacement when broker order truth is UNKNOWN"
    assert len(ee.mark_position_closed_calls) == 0, "must NOT close position when broker order truth is UNKNOWN"
    assert len(canceled) == 0, "must NOT cancel any broker order when broker order truth is UNKNOWN"


# ---------------------------------------------------------------------------
# End-to-end: terminal-pending-id path -- UNKNOWN holds
# ---------------------------------------------------------------------------

def test_terminal_pending_id_duplicate_scan_unknown_holds():
    pos = _make_pos(_TARGET_OCC, pending_broker_order_id="old-terminal-bid")
    ee = _ExitEngine()

    def _raising(**_kwargs: Any) -> list:
        raise RuntimeError("Tradier 503")

    cancel_kwargs, canceled = _cancel_tracking_broker_kwargs()
    broker = types.SimpleNamespace(
        list_open_orders=_raising,
        get_order=lambda _bid: {"id": "old-terminal-bid", "status": "canceled"},
        list_positions=lambda: [{"symbol": _TARGET_OCC, "quantity": 1}],
        **cancel_kwargs,
    )

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert action.action == "NOOP", (
        f"Expected NOOP/HOLD when broker order truth is UNKNOWN during "
        f"terminal-status duplicate-exit scan; got action={action.action} reason={action.reason}"
    )
    assert not ee.any_order_adopted
    assert not ee.any_replacement_authorized, (
        "must NOT authorize replacement from terminal status alone when the "
        "duplicate-exit scan itself failed with UNKNOWN truth"
    )
    assert len(ee.mark_position_closed_calls) == 0
    assert len(canceled) == 0


# ---------------------------------------------------------------------------
# Normal-path preservation: successful empty / nonempty still work
# ---------------------------------------------------------------------------

def test_successful_authoritative_empty_still_permits_negative_proof_flow():
    """A genuinely successful empty open-orders response must still allow
    the existing negative-proof flow (resolver-backed flat/held decision)
    to run -- this is AVAILABLE_EMPTY, not UNKNOWN."""
    pos = _make_pos(_TARGET_OCC)
    ee = _ExitEngine()

    def _empty(**_kwargs: Any) -> list:
        return []

    broker = types.SimpleNamespace(
        list_open_orders=_empty,
        get_order=lambda _bid: None,
        list_positions=lambda: [{"symbol": _TARGET_OCC, "quantity": 2}],
    )

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    # No open exit orders (confirmed empty) + broker confirms position held
    # -> falls through to replacement-safe (unchanged existing behavior).
    assert action.action != "NOOP" or action.reason != "broker_order_truth_unknown_hold", (
        f"A genuinely empty broker-order response must not be treated as UNKNOWN; "
        f"got action={action.action} reason={action.reason}"
    )
    assert ee.any_replacement_authorized, (
        f"Expected replacement-safe to be authorized after confirmed-empty scan "
        f"and broker-held target; got action={action.action} reason={action.reason}"
    )


def test_successful_nonempty_still_finds_and_adopts_exact_live_exit():
    """A genuinely successful nonempty open-orders response containing the
    exact-match live exit must still be found and adopted -- unaffected by
    the tri-state change."""
    pos = _make_pos(_TARGET_OCC)
    ee = _ExitEngine()

    def _nonempty(**_kwargs: Any) -> list:
        return [_stc_order("matching-bid-1", _TARGET_OCC)]

    broker = types.SimpleNamespace(
        list_open_orders=_nonempty,
        get_order=lambda _bid: None,
        list_positions=lambda: [{"symbol": _TARGET_OCC, "quantity": 1}],
    )

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert action.action == "RECOVERED_BROKER_ID", (
        f"Expected RECOVERED_BROKER_ID for a genuinely successful nonempty scan "
        f"finding the exact live exit; got action={action.action} reason={action.reason}"
    )
    assert len(ee.set_pending_exit_order_calls) == 1
    assert ee.set_pending_exit_order_calls[0]["broker_order_id"] == "matching-bid-1"
