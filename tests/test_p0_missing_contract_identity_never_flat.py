"""
tests/test_p0_missing_contract_identity_never_flat.py
=======================================================
P0 regression tests — PR #481 amendment 2 (surgical)

Defect addressed
-----------------
resolve_exit_broker_truth() normalized an empty/missing contract identity
to "" and then treated "no row matches ''" as authoritative broker-flat
(broker_truth_open_qty=0, is_fresh_exact=True). Autonomous recovery could
then call mark_position_closed() for a position whose exact broker contract
was never actually established.

Core invariant under test
--------------------------
ABSENCE CAN ONLY PROVE FLATNESS WHEN WE KNOW EXACTLY WHICH BROKER CONTRACT
WE WERE TRYING TO FIND. Missing/unproven contract identity is UNKNOWN truth,
never broker-flat truth.

Cases covered (per amendment spec)
-----------------------------------
Case A — resolve_exit_broker_truth() with empty contract identity
Case B — recover_exit_position() with a position missing contract identity
Case C — preserve known-good authoritative flat (valid contract, genuinely absent)
Case D — preserve known-good open position (valid contract, positive qty)
Case E — preserve existing #481 malformed/conflicting quantity protections
Normal-path preservation — valid contract + positive qty never falsely closed
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
    contract: str,
    *,
    client_id: str = "test_client_001",
    position_id: str = "pos-contract-identity-001",
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
# CASE A — resolver with empty contract identity
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad_contract", ["", None, "   "])
def test_case_a_resolver_empty_contract_identity_is_unknown_not_flat(bad_contract):
    """
    Empty/None/whitespace-only contract identity must resolve UNKNOWN,
    never authoritative fresh zero — even against a valid successful
    non-empty broker positions snapshot.
    """
    broker = _broker_with_positions([
        {"symbol": _VALID_OCC, "quantity": 3},
    ])

    result = resolve_exit_broker_truth(broker=broker, client_id="test_client_001", contract=bad_contract)

    assert result["broker_truth_open_qty"] is None, (
        f"broker_truth_open_qty must be None for missing contract identity "
        f"(contract={bad_contract!r}); got {result['broker_truth_open_qty']!r}"
    )
    assert result["is_fresh_exact"] is False, (
        f"is_fresh_exact must be False for missing contract identity "
        f"(contract={bad_contract!r}); got {result['is_fresh_exact']!r}"
    )
    assert result["audit"].get("snapshot_status") == "contract_identity_unavailable", (
        f"audit.snapshot_status must be 'contract_identity_unavailable'; "
        f"got {result['audit'].get('snapshot_status')!r}"
    )


def test_case_a_resolver_empty_contract_identity_even_on_empty_snapshot():
    """Empty contract identity must be UNKNOWN even when the broker snapshot
    is itself empty — the empty-snapshot authoritative-flat path must never
    be reached without a proven contract identity."""
    broker = _broker_with_positions([])

    result = resolve_exit_broker_truth(broker=broker, client_id="test_client_001", contract="")

    assert result["broker_truth_open_qty"] is None
    assert result["is_fresh_exact"] is False
    assert result["audit"].get("snapshot_status") == "contract_identity_unavailable"


# ---------------------------------------------------------------------------
# CASE B — recover_exit_position() with unestablished contract identity
# ---------------------------------------------------------------------------

def test_case_b_recovery_missing_contract_identity_holds():
    """
    A recovery position whose exact OCC contract cannot be established
    (empty option_symbol/contract/symbol) must result in NOOP/HOLD.
    Must NOT: mark_position_closed(), mark replacement safe, submit/cancel.
    """
    pos = _make_pos("")  # no usable contract identity
    ee = _ExitEngine()
    broker = _broker_with_positions([
        {"symbol": _VALID_OCC, "quantity": 3},
    ])

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert len(ee.mark_position_closed_calls) == 0, (
        f"mark_position_closed must NOT be called with unestablished contract identity; "
        f"got calls={ee.mark_position_closed_calls}"
    )
    assert not ee.any_replacement_authorized, (
        "replacement must NOT be authorized with unestablished contract identity"
    )
    assert action.action == "NOOP", (
        f"Expected NOOP for missing contract identity; got action={action.action} reason={action.reason}"
    )


def test_case_b_recovery_missing_contract_identity_even_on_empty_broker_snapshot():
    """Even with an empty broker positions snapshot, a position lacking a
    usable contract identity must still hold — never manufacture flat."""
    pos = _make_pos("")
    ee = _ExitEngine()
    broker = _broker_with_positions([])

    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert len(ee.mark_position_closed_calls) == 0, (
        f"mark_position_closed must NOT be called with unestablished contract identity "
        f"even on empty broker snapshot; got calls={ee.mark_position_closed_calls}"
    )
    assert not ee.any_replacement_authorized
    assert action.action == "NOOP"


# ---------------------------------------------------------------------------
# CASE C — preserve known-good authoritative flat
# ---------------------------------------------------------------------------

def test_case_c_valid_contract_genuinely_absent_still_authoritative_flat():
    """
    Existing working behavior MUST remain: a valid, known OCC contract that
    is genuinely absent from a successful non-empty broker snapshot is still
    authoritative flat truth, and autonomous recovery may mark it closed.
    """
    broker = _broker_with_positions([
        {"symbol": "SPY250117P00400000", "quantity": 5},
    ])

    result = resolve_exit_broker_truth(broker=broker, client_id="test_client_001", contract=_VALID_OCC)
    assert result["broker_truth_open_qty"] == 0
    assert result["is_fresh_exact"] is True

    pos = _make_pos(_VALID_OCC)
    ee = _ExitEngine()
    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert len(ee.mark_position_closed_calls) == 1, (
        f"mark_position_closed MUST still be called for genuinely-absent valid contract; "
        f"got calls={ee.mark_position_closed_calls}"
    )
    assert action.action == "MARKED_CLOSED"


# ---------------------------------------------------------------------------
# CASE D — preserve known-good open position
# ---------------------------------------------------------------------------

def test_case_d_valid_contract_positive_quantity_remains_open():
    """
    Existing working behavior MUST remain: a valid OCC contract with a
    positive supported quantity resolves as held, and autonomous recovery
    must NOT mark it closed.
    """
    broker = _broker_with_positions([
        {"symbol": _VALID_OCC, "quantity": 4},
    ])

    result = resolve_exit_broker_truth(broker=broker, client_id="test_client_001", contract=_VALID_OCC)
    assert result["broker_truth_open_qty"] == 4
    assert result["is_fresh_exact"] is True

    pos = _make_pos(_VALID_OCC)
    ee = _ExitEngine()
    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert len(ee.mark_position_closed_calls) == 0, (
        f"mark_position_closed must NOT be called for open position; "
        f"got calls={ee.mark_position_closed_calls}"
    )
    assert action.action != "MARKED_CLOSED"


# ---------------------------------------------------------------------------
# CASE E — preserve existing #481 malformed/conflicting quantity protections
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad_qty,label", [
    (False, "boolean_false"),
    (0.5, "fractional_positive"),
    (-1, "negative_integer"),
    (-0.5, "negative_fractional"),
    (0, "explicit_zero_exact_match"),
])
def test_case_e_existing_malformed_quantity_protections_preserved(bad_qty, label):
    """
    All existing #481 protections against malformed/conflicting exact-match
    quantities must remain intact after the contract-identity hardening.
    """
    broker = _broker_with_positions([
        {"symbol": _VALID_OCC, "quantity": bad_qty},
    ])

    result = resolve_exit_broker_truth(broker=broker, client_id="test_client_001", contract=_VALID_OCC)
    assert result["broker_truth_open_qty"] is None, (
        f"[{label}] broker_truth_open_qty must remain None; got {result['broker_truth_open_qty']!r}"
    )
    assert result["is_fresh_exact"] is False, f"[{label}] is_fresh_exact must remain False"

    pos = _make_pos(_VALID_OCC)
    ee = _ExitEngine()
    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert len(ee.mark_position_closed_calls) == 0, (
        f"[{label}] mark_position_closed must NOT be called; got calls={ee.mark_position_closed_calls}"
    )
    assert not ee.any_replacement_authorized, (
        f"[{label}] replacement must NOT be authorized"
    )


def test_case_e_broker_exception_protection_preserved():
    """Existing #481 protection: broker.list_positions() raising must remain
    UNKNOWN, never authoritative flat, never independently authorize
    replacement."""
    broker = types.SimpleNamespace()

    def _list_positions() -> list:
        raise RuntimeError("Tradier 503")

    def _list_open_orders(**_kwargs: Any) -> list:
        return []

    broker.list_positions = _list_positions
    broker.list_open_orders = _list_open_orders

    result = resolve_exit_broker_truth(broker=broker, client_id="test_client_001", contract=_VALID_OCC)
    assert result["broker_truth_open_qty"] is None
    assert result["is_fresh_exact"] is False

    pos = _make_pos(_VALID_OCC)
    ee = _ExitEngine()
    action = recover_exit_position(pos, broker=broker, exit_engine=ee)

    assert len(ee.mark_position_closed_calls) == 0
    assert not ee.any_replacement_authorized
    assert action.action == "NOOP"


# ---------------------------------------------------------------------------
# NORMAL-PATH PRESERVATION — ordinary AP lifecycle unaffected
# ---------------------------------------------------------------------------

def test_normal_path_valid_contract_positive_quantity_no_false_close_no_new_authority():
    """
    Proves the ordinary, known-good AP position lifecycle is unaffected:
    valid exact OCC contract -> positive broker quantity -> resolver returns
    exact positive truth -> position remains OPEN -> no false CLOSED
    mutation -> no new broker order/cancel authority introduced.
    """
    broker = _broker_with_positions([
        {"symbol": _VALID_OCC, "quantity": 2},
    ])

    # Resolver truth is exact and positive.
    result = resolve_exit_broker_truth(broker=broker, client_id="test_client_001", contract=_VALID_OCC)
    assert result["broker_truth_open_qty"] == 2
    assert result["is_fresh_exact"] is True
    assert result["audit"]["snapshot_status"] == "exact_match"

    # Autonomous recovery does not falsely close the position.
    pos = _make_pos(_VALID_OCC)
    ee = _ExitEngine()
    action = recover_exit_position(pos, broker=broker, exit_engine=ee)
    assert len(ee.mark_position_closed_calls) == 0
    assert action.action != "MARKED_CLOSED"

    # No new broker submit/cancel authority in either module touched by this amendment.
    import ast
    from pathlib import Path

    forbidden = {"submit_order", "place_order", "create_order", "submit_entry", "place_entry"}
    for path in ("ap/exit_safety.py", "ap/exit_autonomous_recovery.py"):
        tree = ast.parse(Path(path).read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr in forbidden:
                pytest.fail(f"Forbidden broker authority '{node.attr}' found in {path} (line {node.lineno})")
