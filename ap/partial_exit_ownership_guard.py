"""Verify durable ownership while an EXIT broker order is partially filled.

The canonical fill projection intentionally recomputes position quantities from
broker fills. A partially filled EXIT order, however, is still broker-owned and
must keep ``exit_in_flight`` plus its local/broker IDs durable until that order
fills, cancels, rejects, or expires.

The canonical reconciler now writes those fields in its existing transaction.
This guard verifies that result without a second write that could race a final
fill. Completed scale-outs remain free to continue into runner evaluation.
"""
from __future__ import annotations

from typing import Any, Callable

from ap.logger import get_logger

log = get_logger("ap.partial_exit_ownership_guard")

_PATCHED_ATTR = "_AP_PARTIAL_EXIT_OWNERSHIP_GUARD_PATCHED"
_ORIGINAL_ATTR = "_AP_PARTIAL_EXIT_OWNERSHIP_GUARD_ORIGINAL"
_PARTIAL_STATUSES = {
    "PARTIAL_FILL",
    "PARTIALLY_FILLED",
    "PARTIAL",
    "EXIT_PARTIAL_FILL",
}


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def is_partial_exit_result(result: dict[str, Any]) -> bool:
    return str(result.get("status") or "").strip().upper() in _PARTIAL_STATUSES


def partial_exit_ownership_fields(order: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    ordered_qty = max(0, _int(order.get("qty")))
    filled_qty = max(0, _int(result.get("filled_qty"), _int(order.get("filled_qty"))))
    remaining_on_order = max(0, ordered_qty - filled_qty)
    return {
        "exit_in_flight": True,
        "pending_exit_local_order_id": str(order.get("local_order_id") or "").strip() or None,
        "pending_exit_broker_order_id": str(
            result.get("broker_order_id") or order.get("broker_order_id") or ""
        ).strip() or None,
        "pending_exit_qty": remaining_on_order or None,
    }


def wrap_exit_fill_reconcile(original: Callable[[dict, dict], dict]) -> Callable[[dict, dict], dict]:
    def guarded(order: dict, result: dict) -> dict:
        reconciled = original(order, result)
        if not isinstance(reconciled, dict) or not is_partial_exit_result(result):
            return reconciled

        projection = reconciled.get("projection")
        if bool(getattr(projection, "closed", False)):
            return reconciled

        client_id = str(order.get("client_id") or "").strip()
        position_id = str(reconciled.get("position_id") or "").strip()
        expected = partial_exit_ownership_fields(order, result)
        persisted = reconciled.get("exit_ownership")
        verified = isinstance(persisted, dict) and all(
            persisted.get(key) == value for key, value in expected.items()
        )
        reconciled["partial_exit_ownership_verified"] = verified
        if not verified:
            log.critical(
                "[%s] PARTIAL_EXIT_OWNERSHIP_INVARIANT_FAILED position=%s local_order=%s "
                "expected=%s reconciled=%s",
                client_id,
                position_id,
                order.get("local_order_id"),
                expected,
                persisted,
            )
        else:
            log.info(
                "[%s] PARTIAL_EXIT_OWNERSHIP_PRESERVED position=%s local_order=%s "
                "broker_order=%s remaining_on_order=%s",
                client_id,
                position_id,
                order.get("local_order_id"),
                result.get("broker_order_id") or order.get("broker_order_id"),
                expected.get("pending_exit_qty"),
            )
        return reconciled

    return guarded


def install_partial_exit_ownership_guard() -> None:
    from ap import exit_fill_truth_guard

    if getattr(exit_fill_truth_guard, _PATCHED_ATTR, False):
        return
    original = exit_fill_truth_guard._reconcile_exit_fill
    setattr(exit_fill_truth_guard, _ORIGINAL_ATTR, original)
    exit_fill_truth_guard._reconcile_exit_fill = wrap_exit_fill_reconcile(original)
    setattr(exit_fill_truth_guard, _PATCHED_ATTR, True)
