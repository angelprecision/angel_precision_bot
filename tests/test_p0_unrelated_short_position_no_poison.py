"""
tests/test_p0_unrelated_short_position_no_poison.py
======================================================
P0 regression tests — PR #481 amendment 4, Blocker 3

Defect addressed
-----------------
ap/brokers/tradier.py::TradierBroker.list_positions() raised
TRADIER_POSITIONS_PAYLOAD_CONFLICT for the ENTIRE account snapshot the
moment ANY row -- anywhere in the account, on ANY contract -- carried a
negative quantity. Tradier position quantity is signed broker data:
negative quantity legitimately represents a short position. Because
list_positions() reads the WHOLE brokerage account in one call, a
completely unrelated legitimate short position (a short stock position,
or a short option on a different underlying) caused the adapter to raise
for the target AP contract's resolution too -- even though the target
contract's own row was perfectly valid. Broker truth for the target
became UNAVAILABLE (UNKNOWN) purely because of noise elsewhere in the
account.

Binding invariant under test
------------------------------
Separate BROKER PAYLOAD VALIDITY (bool/missing/unparseable/NaN/inf/
structurally-malformed -- correctly still rejected globally, since these
represent genuine structural garbage) from AP EXACT-CONTRACT LONG-ONLY
LIFECYCLE AUTHORITY (negative/zero/fractional quantity ON THE TARGET's
OWN row -- rejected at the resolver boundary, not the adapter boundary).
An unrelated valid signed quantity must never poison the whole snapshot;
a negative quantity ON THE TARGET's OWN exact-match row must still
resolve UNKNOWN, never authoritative flat.

Coverage (per amendment spec, required tests 1-6)
----------------------------------------------------
1. target OCC qty=2, unrelated position qty=-1 -> target resolves qty=2
   fresh exact
2. target OCC absent, unrelated position qty=-1 -> target exact OCC
   absence still resolves authoritative flat
3. target exact OCC qty=-1 -> UNKNOWN, never flat
4. target exact OCC qty=0 -> UNKNOWN, never flat (pre-existing #481
   invariant, re-verified alongside the new unrelated-row coverage)
5. target valid long OCC + unrelated short option -> target remains
   valid positive truth
6. malformed/nonfinite unrelated row -> preserve existing fail-closed
   structural policy (still raises globally)

Plus a direct adapter-level test proving the REAL TradierBroker.list_
positions() no longer raises for an unrelated negative-quantity row.
"""

from __future__ import annotations

import os
import types
from types import SimpleNamespace
from typing import Any

os.environ.setdefault("DATABASE_URL", "postgresql://x:x@localhost/x")

import pytest  # noqa: E402
import requests  # noqa: E402

from ap.exit_safety import resolve_exit_broker_truth  # noqa: E402
from ap.brokers.tradier import TradierBroker, TradierConfig  # noqa: E402

_TARGET_OCC = "SMCI260626P00032500"
_UNRELATED_SHORT_STOCK = "TSLA"
_UNRELATED_SHORT_OPTION = "NVDA260117P00120000"


# ---------------------------------------------------------------------------
# Resolver-level helpers (stub broker matching the interface)
# ---------------------------------------------------------------------------

def _broker_with_positions(rows: list[dict]) -> Any:
    broker = types.SimpleNamespace()

    def _list_positions() -> list:
        return rows

    broker.list_positions = _list_positions
    return broker


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


def _adapter_broker(payload: dict, *, account_id: str = "ACC-LIVE-1") -> TradierBroker:
    b = TradierBroker(TradierConfig(
        base_url="https://api.tradier.com",
        access_token="redacted-test-token",
        account_id=account_id,
    ))
    b.session = SimpleNamespace(
        get=lambda url, params=None, timeout=None: FakeResponse(200, payload)
    )
    return b


def _position_row(symbol: str, quantity: Any, **extra: Any) -> dict:
    row = {
        "cost_basis": 1234.0,
        "date_acquired": "2026-06-20T14:41:11.000Z",
        "id": 130089,
        "quantity": quantity,
        "symbol": symbol,
    }
    row.update(extra)
    return row


def _payload(row_or_rows: Any) -> dict:
    return {"positions": {"position": row_or_rows}}


# ---------------------------------------------------------------------------
# Required test 1 -- target OCC qty=2, unrelated position qty=-1
# ---------------------------------------------------------------------------

def test_1_target_positive_unrelated_negative_target_resolves_positive():
    broker = _broker_with_positions([
        {"symbol": _TARGET_OCC, "quantity": 2},
        {"symbol": _UNRELATED_SHORT_STOCK, "quantity": -1},
    ])
    result = resolve_exit_broker_truth(broker=broker, client_id="test_client_001", contract=_TARGET_OCC)
    assert result["broker_truth_open_qty"] == 2, (
        f"Target's own positive quantity must resolve correctly even with an "
        f"unrelated negative-quantity row present; got {result['broker_truth_open_qty']!r}"
    )
    assert result["is_fresh_exact"] is True


# ---------------------------------------------------------------------------
# Required test 2 -- target OCC absent, unrelated position qty=-1
# ---------------------------------------------------------------------------

def test_2_target_absent_unrelated_negative_still_authoritative_flat():
    broker = _broker_with_positions([
        {"symbol": _UNRELATED_SHORT_STOCK, "quantity": -1},
    ])
    result = resolve_exit_broker_truth(broker=broker, client_id="test_client_001", contract=_TARGET_OCC)
    assert result["broker_truth_open_qty"] == 0, (
        f"Target's genuine absence must still resolve authoritative flat even "
        f"with an unrelated negative-quantity row present; got "
        f"{result['broker_truth_open_qty']!r}"
    )
    assert result["is_fresh_exact"] is True


# ---------------------------------------------------------------------------
# Required test 3 -- target exact OCC qty=-1 -> UNKNOWN, never flat
# ---------------------------------------------------------------------------

def test_3_target_own_negative_quantity_unknown_never_flat():
    broker = _broker_with_positions([
        {"symbol": _TARGET_OCC, "quantity": -1},
    ])
    result = resolve_exit_broker_truth(broker=broker, client_id="test_client_001", contract=_TARGET_OCC)
    assert result["broker_truth_open_qty"] is None, (
        f"Target's OWN negative quantity must resolve UNKNOWN, never flat; "
        f"got {result['broker_truth_open_qty']!r}"
    )
    assert result["is_fresh_exact"] is False


# ---------------------------------------------------------------------------
# Required test 4 -- target exact OCC qty=0 -> UNKNOWN, never flat (pre-existing)
# ---------------------------------------------------------------------------

def test_4_target_own_explicit_zero_quantity_unknown_never_flat():
    broker = _broker_with_positions([
        {"symbol": _TARGET_OCC, "quantity": 0},
    ])
    result = resolve_exit_broker_truth(broker=broker, client_id="test_client_001", contract=_TARGET_OCC)
    assert result["broker_truth_open_qty"] is None
    assert result["is_fresh_exact"] is False


# ---------------------------------------------------------------------------
# Required test 5 -- target valid long OCC + unrelated SHORT OPTION (not stock)
# ---------------------------------------------------------------------------

def test_5_target_valid_unrelated_short_option_target_remains_valid():
    broker = _broker_with_positions([
        {"symbol": _TARGET_OCC, "quantity": 3},
        {"symbol": _UNRELATED_SHORT_OPTION, "quantity": -2},
    ])
    result = resolve_exit_broker_truth(broker=broker, client_id="test_client_001", contract=_TARGET_OCC)
    assert result["broker_truth_open_qty"] == 3, (
        f"Target's valid positive truth must be unaffected by an unrelated "
        f"short OPTION position (not just short stock); got "
        f"{result['broker_truth_open_qty']!r}"
    )
    assert result["is_fresh_exact"] is True


# ---------------------------------------------------------------------------
# Required test 6 -- malformed/nonfinite unrelated row: existing policy preserved
# ---------------------------------------------------------------------------

def test_6_malformed_unrelated_row_still_raises_at_adapter():
    """Genuinely malformed structural garbage (missing quantity key) on an
    UNRELATED row must still raise at the adapter -- this is not the sign
    issue Blocker 3 addresses; it's real structural malformedness."""
    broker = _adapter_broker(_payload([
        _position_row(_TARGET_OCC, 3),
        {"symbol": _UNRELATED_SHORT_STOCK},  # missing "quantity" key entirely
    ]))
    with pytest.raises(ValueError, match="TRADIER_POSITIONS_PAYLOAD_MALFORMED"):
        broker.list_positions()


def test_6_nonfinite_unrelated_row_still_raises_at_adapter():
    broker = _adapter_broker(_payload([
        _position_row(_TARGET_OCC, 3),
        _position_row(_UNRELATED_SHORT_STOCK, float("nan")),
    ]))
    with pytest.raises(ValueError, match="TRADIER_POSITIONS_PAYLOAD_MALFORMED"):
        broker.list_positions()


def test_6_boolean_unrelated_row_still_raises_at_adapter():
    broker = _adapter_broker(_payload([
        _position_row(_TARGET_OCC, 3),
        _position_row(_UNRELATED_SHORT_STOCK, False),
    ]))
    with pytest.raises(ValueError, match="TRADIER_POSITIONS_PAYLOAD_MALFORMED"):
        broker.list_positions()


# ---------------------------------------------------------------------------
# Direct adapter-level proof: real TradierBroker no longer raises globally
# for an unrelated negative-quantity row
# ---------------------------------------------------------------------------

def test_adapter_unrelated_negative_quantity_does_not_raise():
    """The real production adapter must return all rows -- including the
    negative-quantity unrelated one, passed through as valid signed broker
    data -- rather than raising and making the WHOLE snapshot unavailable."""
    broker = _adapter_broker(_payload([
        _position_row(_TARGET_OCC, 3),
        _position_row(_UNRELATED_SHORT_STOCK, -1),
    ]))
    rows = broker.list_positions()
    assert len(rows) == 2, f"Expected both rows returned; got {rows!r}"
    symbols_to_qty = {r["symbol"]: r["quantity"] for r in rows}
    assert symbols_to_qty[_TARGET_OCC] == 3.0
    assert symbols_to_qty[_UNRELATED_SHORT_STOCK] == -1.0, (
        "Negative quantity must pass through as valid signed broker data, "
        "not be dropped or coerced"
    )


def test_adapter_target_own_negative_quantity_still_passes_through_unraised():
    """The adapter itself must not raise for ANY negative quantity --
    including the target's own row. Rejection of the target's own negative
    quantity as UNKNOWN/never-flat happens at the resolver boundary
    (ap/exit_safety.py::_extract_long_position_qty), not the adapter."""
    broker = _adapter_broker(_payload([
        _position_row(_TARGET_OCC, -1),
    ]))
    rows = broker.list_positions()
    assert len(rows) == 1
    assert rows[0]["quantity"] == -1.0


def test_adapter_fractional_still_raises_regardless_of_sign():
    """Fractional quantity is structural malformedness (not a valid whole
    contract/share count for anyone), independent of Blocker 3's sign fix
    -- must continue to raise."""
    broker = _adapter_broker(_payload([
        _position_row(_UNRELATED_SHORT_STOCK, -0.5),
    ]))
    with pytest.raises(ValueError, match="TRADIER_POSITIONS_PAYLOAD_MALFORMED"):
        broker.list_positions()
