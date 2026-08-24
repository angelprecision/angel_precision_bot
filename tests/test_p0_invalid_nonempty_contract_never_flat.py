"""
tests/test_p0_invalid_nonempty_contract_never_flat.py
=======================================================
P0 regression tests — PR #481 amendment 4, Blocker 1

Defect addressed
-----------------
Amendments 2 and 3 correctly prevented an EMPTY contract identity from
becoming authoritative broker-flat truth or a wildcard broker-order match.
But _normalize_contract() only strips/uppercases/removes spaces -- it does
NOT prove the value is a complete, exact OCC option symbol. A non-empty
but malformed/incomplete token ("UNKNOWN", a bare underlying ticker like
"SMCI", a placeholder like "DEFERRED:SMCI", a truncated or malformed OCC
shape) passed the bare `if not normalized_contract` guard and could then
match ZERO rows in any successful broker snapshot -- manufacturing
authoritative broker-flat truth for a contract that was never actually
proven to exist.

Binding invariant under test
------------------------------
ABSENCE CAN ONLY PROVE FLATNESS WHEN THE EXACT OCC CONTRACT IDENTITY HAS
BEEN PROVEN. A non-empty malformed token must never count as established
identity.

Coverage
--------
- resolve_exit_broker_truth() rejects every non-exact-OCC value tested,
  against BOTH an empty and a non-empty successful broker snapshot.
- recover_exit_position() end-to-end: a position whose option_symbol is
  one of these garbage values must HOLD, never close, never authorize
  replacement -- proving the same predicate propagates through
  ap/exit_autonomous_recovery.py via _position_contract().
- Regression: a valid exact OCC contract genuinely absent from a
  successful snapshot still resolves authoritative flat (unchanged).
"""

from __future__ import annotations

import os
import types
from typing import Any

os.environ.setdefault("DATABASE_URL", "postgresql://x:x@localhost/x")

import pytest  # noqa: E402

from ap.exit_safety import resolve_exit_broker_truth  # noqa: E402
from ap.exit_autonomous_recovery import recover_exit_position  # noqa: E402

_VALID_OCC = "SMCI260626P00032500"

# Every value here must be treated as UNPROVEN identity -- never authoritative.
_INVALID_NONEMPTY_CONTRACTS = [
    ("UNKNOWN", "placeholder_unknown"),
    ("SMCI", "bare_underlying_ticker"),
    ("DEFERRED:SMCI", "deferred_placeholder"),
    ("SMCI260626X00032500", "invalid_side_letter"),
    ("SMCI26062P00032500", "truncated_date_5digits"),
    ("SMCI260626P0003250", "truncated_strike_7digits"),
    ("SMCI260626PP0032500", "malformed_double_side_letter"),
    ("260626P00032500", "missing_root_symbol"),
]

_EMPTY_LIKE_CONTRACTS = [
    ("", "empty_string"),
    (None, "none"),
    ("   ", "whitespace_only"),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _broker_with_positions(rows: list[dict]) -> Any:
    broker = types.SimpleNamespace()

    def _list_positions() -> list:
        return rows

    def _list_open_orders(**_kwargs: Any) -> list:
        return []

    broker.list_positions = _list_positions
    broker.list_open_orders = _list_open_orders
    return broker


def _make_pos(
    contract: Any,
    *,
    client_id: str = "test_client_001",
    position_id: str = "pos-invalid-contract-001",
) -> Any:
    return types.SimpleNamespace(
        position_id=position_id,
        client_id=client_id,
        pending_exit_local_order_id="",
        pending_exit_broker_order_id="",
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
        return bool(self.mark_exit_replacement_safe_calls or self.clear_exit_in_flight_calls)


# ---------------------------------------------------------------------------
# Resolver-level: non-empty invalid contract, against EMPTY snapshot
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad_contract,label", _INVALID_NONEMPTY_CONTRACTS)
def test_resolver_invalid_nonempty_contract_empty_snapshot_is_unknown(bad_contract, label):
    broker = _broker_with_positions([])

    result = resolve_exit_broker_truth(broker=broker, client_id="test_client_001", contract=bad_contract)

    assert result["broker_truth_open_qty"] is None, (
        f"[{label}] broker_truth_open_qty must be None for invalid non-empty contract "
        f"{bad_contract!r} even against an empty snapshot; got {result['broker_truth_open_qty']!r}"
    )
    assert result["is_fresh_exact"] is False, f"[{label}] is_fresh_exact must be False"


# ---------------------------------------------------------------------------
# Resolver-level: non-empty invalid contract, against NON-EMPTY snapshot
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad_contract,label", _INVALID_NONEMPTY_CONTRACTS)
def test_resolver_invalid_nonempty_contract_nonempty_snapshot_is_unknown(bad_contract, label):
    broker = _broker_with_positions([
        {"symbol": _VALID_OCC, "quantity": 3},
    ])

    result = resolve_exit_broker_truth(broker=broker, client_id="test_client_001", contract=bad_contract)

    assert result["broker_truth_open_qty"] is None, (
        f"[{label}] broker_truth_open_qty must be None for invalid non-empty contract "
        f"{bad_contract!r}; got {result['broker_truth_open_qty']!r} -- a malformed target "
        f"identity must never be treated as 'absent from snapshot = flat'"
    )
    assert result["is_fresh_exact"] is False, f"[{label}] is_fresh_exact must be False"


# ---------------------------------------------------------------------------
# End-to-end: recover_exit_position() must hold for invalid contracts too
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad_contract,label", _INVALID_NONEMPTY_CONTRACTS)
def test_recovery_invalid_nonempty_contract_holds_never_closes(bad_contract, label):
    pos = _make_pos(bad_contract)
    ee = _ExitEngine()
    broker = _broker_with_positions([
        {"symbol": _VALID_OCC, "quantity": 3},
    ])

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert len(ee.mark_position_closed_calls) == 0, (
        f"[{label}] mark_position_closed must NOT be called for invalid non-empty "
        f"contract {bad_contract!r}; got calls={ee.mark_position_closed_calls}"
    )
    assert not ee.any_replacement_authorized, (
        f"[{label}] replacement must NOT be authorized for invalid non-empty contract {bad_contract!r}"
    )
    assert action.action == "NOOP", (
        f"[{label}] Expected NOOP/HOLD; got action={action.action} reason={action.reason}"
    )


# ---------------------------------------------------------------------------
# Empty-like values -- regression guard, must still hold (Amendment 2/3 coverage)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("empty_contract,label", _EMPTY_LIKE_CONTRACTS)
def test_resolver_empty_like_contract_still_unknown(empty_contract, label):
    broker = _broker_with_positions([
        {"symbol": _VALID_OCC, "quantity": 3},
    ])
    result = resolve_exit_broker_truth(broker=broker, client_id="test_client_001", contract=empty_contract)
    assert result["broker_truth_open_qty"] is None, f"[{label}] must remain UNKNOWN"
    assert result["is_fresh_exact"] is False, f"[{label}] must remain UNKNOWN"


# ---------------------------------------------------------------------------
# Regression: valid exact OCC genuinely absent still proves flat
# ---------------------------------------------------------------------------

def test_regression_valid_exact_occ_absent_still_authoritative_flat():
    broker = _broker_with_positions([
        {"symbol": "SPY250117P00400000", "quantity": 5},
    ])

    result = resolve_exit_broker_truth(broker=broker, client_id="test_client_001", contract=_VALID_OCC)
    assert result["broker_truth_open_qty"] == 0
    assert result["is_fresh_exact"] is True

    pos = _make_pos(_VALID_OCC)
    ee = _ExitEngine()
    action = recover_exit_position(pos, broker=broker, exit_engine=ee)
    assert len(ee.mark_position_closed_calls) == 1
    assert action.action == "MARKED_CLOSED"


def test_regression_valid_exact_occ_positive_quantity_still_open():
    broker = _broker_with_positions([
        {"symbol": _VALID_OCC, "quantity": 2},
    ])
    result = resolve_exit_broker_truth(broker=broker, client_id="test_client_001", contract=_VALID_OCC)
    assert result["broker_truth_open_qty"] == 2
    assert result["is_fresh_exact"] is True
