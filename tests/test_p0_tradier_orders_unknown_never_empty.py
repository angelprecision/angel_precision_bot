"""
tests/test_p0_tradier_orders_unknown_never_empty.py
======================================================
P0 regression tests — PR #481 amendment 6

Defect addressed
-----------------
Amendment 5 correctly introduced tri-state broker-order truth in
ap/exit_autonomous_recovery.py::_list_open_orders() -- exceptions,
None returns, and unusable payloads from broker.list_orders() (or
whichever candidate method the broker exposes) are treated as UNKNOWN,
never silently coerced to [].

However, the production Tradier adapter
(ap/brokers/tradier.py::TradierBroker.list_orders()) could still
collapse an unusable broker response into [] BEFORE autonomous recovery
ever saw it -- defeating Amendment 5's protection at its source, exactly
the same class of defect Amendment 4 fixed for list_positions().

Two concrete false-AVAILABLE_EMPTY paths existed:

  CASE 1 -- malformed top-level payload:
    HTTP succeeds, JSON payload is a scalar/string/non-dict shape.
    Old: isinstance(j, dict) == False -> node = None -> orders = None
    -> return []. Wrong: unusable broker truth, not proof zero orders
    exist.

  CASE 2 -- malformed order rows silently filtered:
    {"orders": {"order": [None, 123]}}
    Old: list comprehension silently drops every non-dict row -> [].
    Wrong: the broker told us there WERE order rows but their identity
    could not be established -- that is UNKNOWN, not authoritative zero.
    One malformed row could be exactly the live EXIT this call exists to
    prove does or does not exist.

Binding invariant under test
------------------------------
BROKER RESPONSE UNUSABLE != BROKER EMPTY (the same invariant already
established for list_positions() in earlier amendments). list_orders()
must:
  1. propagate transport/auth/HTTP exceptions unchanged
  2. return [] ONLY for broker response shapes explicitly recognized as
     authoritative successful-empty order responses
  3. return normalized rows for a valid single-order dict or a valid
     list of order dicts
  4. RAISE TRADIER_ORDERS_PAYLOAD_MALFORMED for any unusable shape whose
     order truth cannot be established -- including a non-dict top-level
     payload, a non-list/non-dict order node, and an order list
     containing ANY non-dict row (fail closed, never silently filter).

Test cases (per amendment spec)
---------------------------------
A. malformed top-level successful JSON -> raises, never []
B. malformed order rows ({"order": [None]}) -> raises, never []
C. mixed valid + malformed rows -> raises, never partial []
D. legitimate successful empty broker response -> []
E. legitimate same-OCC live EXIT (real Tradier symbol/option_symbol
   shape) -> adapter returns row, autonomous recovery finds it,
   replacement NOT authorized
F. end-to-end: real TradierBroker + malformed successful HTTP payload
   wired into recover_exit_position() -> NOOP/HOLD, never
   replacement-safe/close/cancel/adopt
"""

from __future__ import annotations

import os
import types
from types import SimpleNamespace
from typing import Any

os.environ.setdefault("DATABASE_URL", "postgresql://x:x@localhost/x")

import pytest  # noqa: E402
import requests  # noqa: E402

from ap.brokers.tradier import TradierBroker, TradierConfig  # noqa: E402
from ap.exit_autonomous_recovery import recover_exit_position  # noqa: E402

_TARGET_UNDERLYING = "SMCI"
_TARGET_OCC = "SMCI260626P00032500"


# ---------------------------------------------------------------------------
# Adapter-level helpers (real TradierBroker with faked HTTP session)
# ---------------------------------------------------------------------------

class FakeResponse:
    def __init__(self, status_code=200, payload=None, *, content=True, raw_text=""):
        self.status_code = status_code
        self._payload = payload
        self.content = b"{}" if content else b""
        self.text = raw_text

    def raise_for_status(self):
        if self.status_code >= 400:
            err = requests.exceptions.HTTPError(f"HTTP {self.status_code}")
            err.response = self
            raise err

    def json(self):
        return self._payload


def _adapter_broker(payload: Any, *, account_id: str = "ACC-LIVE-1") -> TradierBroker:
    b = TradierBroker(TradierConfig(
        base_url="https://api.tradier.com",
        access_token="redacted-test-token",
        account_id=account_id,
    ))
    b.session = SimpleNamespace(
        get=lambda url, params=None, timeout=None: FakeResponse(200, payload)
    )
    return b


def _order_row(broker_order_id: str, *, underlying: str, option_symbol: str, status: str = "open", side: str = "sell_to_close") -> dict:
    return {
        "id": broker_order_id,
        "class": "option",
        "symbol": underlying,
        "option_symbol": option_symbol,
        "side": side,
        "status": status,
        "quantity": 1,
    }


def _make_pos(*, pending_broker_order_id: str = "", position_id: str = "pos-orders-adapter-001") -> Any:
    return types.SimpleNamespace(
        position_id=position_id,
        client_id="test_client_001",
        pending_exit_local_order_id="",
        pending_exit_broker_order_id=pending_broker_order_id,
        pending_exit_qty=1,
        contracts=1,
        option_symbol=_TARGET_OCC,
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


# ---------------------------------------------------------------------------
# CASE A -- malformed top-level successful JSON
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad_payload,label", [
    ("unexpected", "bare_string"),
    (12345, "bare_int"),
    (True, "bare_bool"),
    ([1, 2, 3], "bare_list"),
])
def test_case_a_malformed_toplevel_payload_raises_never_empty(bad_payload, label):
    broker = _adapter_broker(bad_payload)
    with pytest.raises(ValueError, match="TRADIER_ORDERS_PAYLOAD_MALFORMED"):
        broker.list_orders()


# ---------------------------------------------------------------------------
# CASE B -- malformed order rows
# ---------------------------------------------------------------------------

def test_case_b_malformed_order_row_none_raises_never_empty():
    broker = _adapter_broker({"orders": {"order": [None]}})
    with pytest.raises(ValueError, match="TRADIER_ORDERS_PAYLOAD_MALFORMED"):
        broker.list_orders()


def test_case_b_variant_malformed_single_order_scalar_raises():
    broker = _adapter_broker({"orders": {"order": [None, 123]}})
    with pytest.raises(ValueError, match="TRADIER_ORDERS_PAYLOAD_MALFORMED"):
        broker.list_orders()


def test_case_b_variant_order_node_not_list_or_dict_raises():
    broker = _adapter_broker({"orders": {"order": "unexpected_scalar"}})
    with pytest.raises(ValueError, match="TRADIER_ORDERS_PAYLOAD_MALFORMED"):
        broker.list_orders()


# ---------------------------------------------------------------------------
# CASE C -- mixed valid + malformed rows
# ---------------------------------------------------------------------------

def test_case_c_mixed_valid_and_malformed_rows_fails_closed():
    valid_row = _order_row("bid-1", underlying=_TARGET_UNDERLYING, option_symbol=_TARGET_OCC)
    broker = _adapter_broker({"orders": {"order": [valid_row, None]}})
    with pytest.raises(ValueError, match="TRADIER_ORDERS_PAYLOAD_MALFORMED"):
        broker.list_orders()


def test_case_c_variant_malformed_row_before_valid_row_fails_closed():
    valid_row = _order_row("bid-1", underlying=_TARGET_UNDERLYING, option_symbol=_TARGET_OCC)
    broker = _adapter_broker({"orders": {"order": ["not_a_dict", valid_row]}})
    with pytest.raises(ValueError, match="TRADIER_ORDERS_PAYLOAD_MALFORMED"):
        broker.list_orders()


# ---------------------------------------------------------------------------
# CASE D -- legitimate successful empty broker response
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("empty_payload,label", [
    ({}, "toplevel_empty_dict"),
    ({"orders": "null"}, "orders_string_null"),
    ({"orders": None}, "orders_none"),
    ({"orders": ""}, "orders_empty_string"),
    ({"orders": {}}, "orders_empty_dict"),
    ({"orders": {"order": None}}, "order_none"),
    ({"orders": {"order": "null"}}, "order_string_null"),
    ({"orders": {"order": ""}}, "order_empty_string"),
    ({"orders": {"order": []}}, "order_empty_list"),
])
def test_case_d_legitimate_empty_shapes_return_empty_list(empty_payload, label):
    broker = _adapter_broker(empty_payload)
    result = broker.list_orders()
    assert result == [], f"[{label}] expected [] for legitimate empty shape {empty_payload!r}; got {result!r}"


# ---------------------------------------------------------------------------
# CASE E -- legitimate same-OCC live EXIT, real Tradier shape
# ---------------------------------------------------------------------------

def test_case_e_single_valid_order_dict_returned_correctly():
    row = _order_row("bid-1", underlying=_TARGET_UNDERLYING, option_symbol=_TARGET_OCC)
    broker = _adapter_broker({"orders": {"order": row}})  # single dict, not wrapped in a list
    result = broker.list_orders()
    assert result == [row]


def test_case_e_list_of_valid_orders_returned_correctly():
    row1 = _order_row("bid-1", underlying=_TARGET_UNDERLYING, option_symbol=_TARGET_OCC)
    row2 = _order_row("bid-2", underlying="TSLA", option_symbol="TSLA260320C00250000")
    broker = _adapter_broker({"orders": {"order": [row1, row2]}})
    result = broker.list_orders()
    assert result == [row1, row2]


def test_case_e_end_to_end_same_occ_live_exit_recognized_no_replacement():
    """Real TradierBroker returning a valid same-OCC live EXIT must still
    be found by autonomous recovery, with replacement NOT authorized --
    normal-path preservation through the hardened adapter."""
    pos = _make_pos()  # no pending_broker_order_id -> generic scan path
    ee = _ExitEngine()
    row = _order_row("matching-bid-1", underlying=_TARGET_UNDERLYING, option_symbol=_TARGET_OCC)
    broker = _adapter_broker({"orders": {"order": row}})
    # list_positions must also be usable for downstream negative-proof
    # paths, though this test should resolve via the open-order match.
    broker.session.get = lambda url, params=None, timeout=None: FakeResponse(
        200,
        {"orders": {"order": row}} if "/orders" in url else {"positions": {"position": {"symbol": _TARGET_OCC, "quantity": 1}}},
    )

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert action.action == "RECOVERED_BROKER_ID", (
        f"Expected the real adapter's valid same-OCC live EXIT to be recognized; "
        f"got action={action.action} reason={action.reason}"
    )
    assert not ee.any_replacement_authorized, (
        "replacement must NOT be authorized when a live same-OCC EXIT was found"
    )
    assert len(ee.set_pending_exit_order_calls) == 1
    assert ee.set_pending_exit_order_calls[0]["broker_order_id"] == "matching-bid-1"


# ---------------------------------------------------------------------------
# CASE F -- end-to-end malformed adapter response -> NOOP/HOLD
# ---------------------------------------------------------------------------

def test_case_f_end_to_end_malformed_orders_payload_holds_generic_path():
    """The critical production-shape regression: wire a real TradierBroker
    returning a malformed successful HTTP order payload into
    recover_exit_position() via the generic/missing-broker-id path.
    Must NEVER authorize replacement, close, cancel, or adopt."""
    pos = _make_pos()  # no pending_broker_order_id -> generic scan path
    ee = _ExitEngine()
    broker = _adapter_broker("unexpected_scalar_payload")  # malformed top-level
    # Ensure list_positions still resolves the target as held, so the only
    # thing standing between this test and a false REPLACEMENT_SAFE is the
    # open-order query truth.
    broker.session.get = lambda url, params=None, timeout=None: FakeResponse(
        200,
        "unexpected_scalar_payload" if "/orders" in url else {"positions": {"position": {"symbol": _TARGET_OCC, "quantity": 1}}},
    )

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert action.action == "NOOP", (
        f"Expected NOOP/HOLD when the real adapter's order query returns a "
        f"malformed payload; got action={action.action} reason={action.reason}"
    )
    assert action.reason in (
        "broker_order_truth_unknown_hold",
    ), f"Expected the UNKNOWN-order-truth hold reason; got reason={action.reason}"
    assert not ee.any_order_adopted, "must NOT adopt any order from a malformed payload"
    assert not ee.any_replacement_authorized, "must NOT authorize replacement from a malformed payload"
    assert len(ee.mark_position_closed_calls) == 0, "must NOT close the position from a malformed payload"


def test_case_f_variant_malformed_order_row_holds_generic_path():
    """Same end-to-end proof, but for the malformed-row shape (Case B)
    rather than the malformed-top-level shape."""
    pos = _make_pos()
    ee = _ExitEngine()
    broker = _adapter_broker({"orders": {"order": [None]}})
    broker.session.get = lambda url, params=None, timeout=None: FakeResponse(
        200,
        {"orders": {"order": [None]}} if "/orders" in url else {"positions": {"position": {"symbol": _TARGET_OCC, "quantity": 1}}},
    )

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert action.action == "NOOP"
    assert action.reason == "broker_order_truth_unknown_hold"
    assert not ee.any_order_adopted
    assert not ee.any_replacement_authorized
    assert len(ee.mark_position_closed_calls) == 0


def test_case_f_variant_terminal_pending_id_malformed_orders_holds():
    """End-to-end proof for the terminal-pending-broker-id duplicate-scan
    path, not just the generic path -- both call sites in
    recover_exit_position() must hold on a malformed adapter response."""
    pos = _make_pos(pending_broker_order_id="old-terminal-bid")
    ee = _ExitEngine()
    broker = _adapter_broker("unexpected_scalar_payload")

    def _get(_broker_order_id: str):
        return {"id": "old-terminal-bid", "status": "canceled"}
    broker.get_order = _get

    broker.session.get = lambda url, params=None, timeout=None: FakeResponse(
        200,
        "unexpected_scalar_payload" if "/orders" in url else {"positions": {"position": {"symbol": _TARGET_OCC, "quantity": 1}}},
    )

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert action.action == "NOOP"
    assert action.reason == "broker_order_truth_unknown_hold"
    assert not ee.any_order_adopted
    assert not ee.any_replacement_authorized
    assert len(ee.mark_position_closed_calls) == 0


# ---------------------------------------------------------------------------
# Normal-path preservation -- valid held position + proven empty order
# snapshot -> replacement-safe behavior unchanged
# ---------------------------------------------------------------------------

def test_normal_path_proven_empty_orders_plus_held_position_replacement_safe():
    pos = _make_pos()
    ee = _ExitEngine()
    broker = _adapter_broker({})  # will be overridden per-endpoint below
    broker.session.get = lambda url, params=None, timeout=None: FakeResponse(
        200,
        {} if "/orders" in url else {"positions": {"position": {"symbol": _TARGET_OCC, "quantity": 1}}},
    )

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert ee.any_replacement_authorized, (
        f"Expected replacement-safe after a proven-empty order snapshot and a "
        f"broker-held target; got action={action.action} reason={action.reason}"
    )
