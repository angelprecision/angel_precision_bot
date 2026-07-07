"""Pure helpers for broker-truth flat repair exits.

These helpers are intentionally side-effect free so the exit engine can call them
without changing normal position behavior.  A synthetic/broker-repair position
with broker-truth open quantity of zero has no risk left to reduce; it should be
classified as flat/stale rather than repeatedly submitted to the exit path.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

FLAT_REASON = "broker_truth_position_flat"


@dataclass(frozen=True)
class BrokerTruthFlatDecision:
    flat: bool
    reason: str = ""
    broker_truth_open_qty: int = 0
    repair_context: bool = False


def safe_int(value: Any) -> int:
    try:
        return int(float(value or 0))
    except Exception:
        return 0


def is_broker_repair_position_id(position_id: Any) -> bool:
    return str(position_id or "").startswith("broker-repair-")


def classify_broker_truth_flat(
    *,
    position_id: Any,
    broker_truth_open_qty: Any,
    allow_missing_position_with_broker_truth: bool = False,
) -> BrokerTruthFlatDecision:
    qty = safe_int(broker_truth_open_qty)
    repair_context = bool(
        allow_missing_position_with_broker_truth
        or is_broker_repair_position_id(position_id)
    )
    if repair_context and qty <= 0:
        return BrokerTruthFlatDecision(
            flat=True,
            reason=FLAT_REASON,
            broker_truth_open_qty=qty,
            repair_context=True,
        )
    return BrokerTruthFlatDecision(
        flat=False,
        reason="",
        broker_truth_open_qty=qty,
        repair_context=repair_context,
    )


def clear_flat_repair_position_memory(pos: Any) -> None:
    """Best-effort in-memory cleanup for a flat synthetic repair position.

    The caller remains responsible for holding any engine lock and emitting any
    audit event. This function only mutates the position object passed to it.
    """
    pos.closed = True
    pos.close_reason = FLAT_REASON
    pos.quantity_remaining = 0
    pos.exit_in_flight = False
    pos.pending_exit_reason = ""
    pos.pending_exit_action = ""
    pos.pending_exit_qty = 0
    pos.pending_exit_local_order_id = ""
    pos.pending_exit_broker_order_id = ""
