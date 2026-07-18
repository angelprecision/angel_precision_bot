"""Verify durable ownership while an EXIT broker order is partially filled.

The canonical fill projection recomputes position quantities from broker fills.
A partially filled EXIT order is still broker-owned and must keep
``exit_in_flight`` plus its local/broker IDs durable until that order reaches
a terminal state.

The canonical reconciler writes those fields in its existing transaction.
This guard verifies that result without a second write.

Architecture — single wrap point
─────────────────────────────────
Both production entry paths call the same leaf:

    _reconcile_exit_fill(order, result)
        → _run_reconciliation_attempt(order, result, *, attempt_count)

    retry_exit_fill_reconciliation(*, client_id, local_order_id)
        → _run_reconciliation_attempt(order, result, *, attempt_count)

Wrapping ``_run_reconciliation_attempt`` covers both paths with one wrapper
and receives the original ``order`` and ``result`` dicts directly — no
reconstruction from the return value, no proxy, no missing fields.

This module performs zero database writes and zero broker calls.
"""
from __future__ import annotations

from typing import Any, Callable

from ap.logger import get_logger

log = get_logger("ap.partial_exit_ownership_guard")

_PATCHED_ATTR  = "_AP_PARTIAL_EXIT_OWNERSHIP_GUARD_PATCHED"
_ORIGINAL_ATTR = "_AP_PARTIAL_EXIT_OWNERSHIP_GUARD_ORIGINAL"

# Removed _RETRY_PATCHED_ATTR / _RETRY_ORIGINAL_ATTR: the retry path is now
# covered by wrapping _run_reconciliation_attempt, not the outer retry function.

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


def is_partial_exit_result(order: dict[str, Any], result: dict[str, Any]) -> bool:
    """Return True iff this is an active partial EXIT.

    Status resolution mirrors the canonical predicate in
    ``exit_fill_truth_guard._is_partial_result`` exactly:

        result["status"] → result["state"] → order["status"]

    Resolved inline to avoid importing ``exit_fill_truth_guard`` at call time
    (which pulls in ``ap.db`` and requires DATABASE_URL during tests).
    ``order["state"]`` is not checked because the canonical predicate does not
    check it.
    """
    status = str(
        result.get("status")
        or result.get("state")
        or order.get("status")
        or ""
    ).strip().upper()
    return status in _PARTIAL_STATUSES


def partial_exit_ownership_fields(
    order: dict[str, Any], result: dict[str, Any]
) -> dict[str, Any]:
    """Compute the expected durable ownership fields for an active partial EXIT.

    ``pending_exit_qty`` is the unfilled remainder on THIS order only:
        remaining = max(0, ordered_qty - cumulative_filled_qty)
    """
    ordered_qty = max(0, _int(order.get("qty")))
    filled_qty  = max(0, _int(result.get("filled_qty"), _int(order.get("filled_qty"))))
    remaining   = max(0, ordered_qty - filled_qty)
    return {
        "exit_in_flight": True,
        "pending_exit_local_order_id":  str(order.get("local_order_id") or "").strip() or None,
        "pending_exit_broker_order_id": str(
            result.get("broker_order_id") or order.get("broker_order_id") or ""
        ).strip() or None,
        "pending_exit_qty": remaining or None,
    }


def verify_partial_exit_ownership(
    order: dict[str, Any],
    result: dict[str, Any],
    reconciled: dict[str, Any],
) -> dict[str, Any]:
    """Verification-only: inspect the canonical reconciliation result.

    Called after ``_run_reconciliation_attempt`` returns, covering both
    the direct fill-monitor path and the startup retry path.

    May only:
    - inspect order/result/reconciled fields
    - compute expected ownership fields
    - set ``partial_exit_ownership_verified`` diagnostic metadata
    - emit critical log on invariant failure

    Must never:
    - open a database connection or execute SQL
    - update positions, orders, proof_trades, queues, or signals
    - make a broker call, submit or cancel orders
    - reopen a closed position
    - become a second EXIT-fill reducer or mutation authority
    """
    if not isinstance(reconciled, dict):
        return reconciled

    if not is_partial_exit_result(order, result):
        return reconciled

    projection = reconciled.get("projection")
    if bool(getattr(projection, "closed", False)):
        return reconciled

    client_id   = str(order.get("client_id") or "").strip()
    position_id = str(reconciled.get("position_id") or "").strip()
    expected    = partial_exit_ownership_fields(order, result)
    persisted   = reconciled.get("exit_ownership")

    verified = isinstance(persisted, dict) and all(
        persisted.get(k) == v for k, v in expected.items()
    )
    reconciled["partial_exit_ownership_verified"] = verified

    if not verified:
        log.critical(
            "[%s] PARTIAL_EXIT_OWNERSHIP_INVARIANT_FAILED position=%s local_order=%s "
            "expected=%s reconciled=%s",
            client_id, position_id, order.get("local_order_id"),
            expected, persisted,
        )
    else:
        log.info(
            "[%s] PARTIAL_EXIT_OWNERSHIP_PRESERVED position=%s local_order=%s "
            "broker_order=%s remaining_on_order=%s",
            client_id, position_id, order.get("local_order_id"),
            result.get("broker_order_id") or order.get("broker_order_id"),
            expected.get("pending_exit_qty"),
        )
    return reconciled


def wrap_run_reconciliation_attempt(
    original: Callable[..., dict],
) -> Callable[..., dict]:
    """Wrap ``_run_reconciliation_attempt`` — the shared leaf of both paths.

    ``_reconcile_exit_fill`` and ``retry_exit_fill_reconciliation`` both call
    this function with the original ``order`` and ``result`` dicts as positional
    arguments, so verification always has the real fields available.

    This replaces the previous approach of wrapping ``_reconcile_exit_fill`` and
    ``retry_exit_fill_reconciliation`` separately, which required reconstructing
    order/result from the return dict — fields the retry return does not contain.
    """
    def guarded(order: dict, result: dict, *, attempt_count: int) -> dict:
        reconciled = original(order, result, attempt_count=attempt_count)
        return verify_partial_exit_ownership(order, result, reconciled)

    return guarded


def install_partial_exit_ownership_guard() -> None:
    """Patch ``_run_reconciliation_attempt`` — single point covering both paths.

    Idempotent; a second call is a no-op.
    """
    from ap import exit_fill_truth_guard

    if getattr(exit_fill_truth_guard, _PATCHED_ATTR, False):
        return

    original = exit_fill_truth_guard._run_reconciliation_attempt
    setattr(exit_fill_truth_guard, _ORIGINAL_ATTR, original)
    exit_fill_truth_guard._run_reconciliation_attempt = wrap_run_reconciliation_attempt(original)
    setattr(exit_fill_truth_guard, _PATCHED_ATTR, True)
