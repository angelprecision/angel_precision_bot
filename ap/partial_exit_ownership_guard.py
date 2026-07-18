"""Verify durable ownership while an EXIT broker order is partially filled.

The canonical fill projection intentionally recomputes position quantities from
broker fills. A partially filled EXIT order, however, is still broker-owned and
must keep ``exit_in_flight`` plus its local/broker IDs durable until that order
fills, cancels, rejects, or expires.

The canonical reconciler writes those fields in its existing transaction.
This guard verifies that result without a second write.

Two canonical entry paths exist — both are covered:

    _reconcile_exit_fill(order, result)              <- fill monitor / direct
    retry_exit_fill_reconciliation(...)              <- startup retry path

Both delegate to ``_run_reconciliation_attempt``.  The shared
``verify_partial_exit_ownership()`` function is called after either path
returns, ensuring the invariant is checked regardless of which entry point
was used.

This module performs zero database writes and zero broker calls.
"""
from __future__ import annotations

from typing import Any, Callable

from ap.logger import get_logger

log = get_logger("ap.partial_exit_ownership_guard")

_PATCHED_ATTR        = "_AP_PARTIAL_EXIT_OWNERSHIP_GUARD_PATCHED"
_ORIGINAL_ATTR       = "_AP_PARTIAL_EXIT_OWNERSHIP_GUARD_ORIGINAL"
_RETRY_PATCHED_ATTR  = "_AP_PARTIAL_EXIT_OWNERSHIP_GUARD_RETRY_PATCHED"
_RETRY_ORIGINAL_ATTR = "_AP_PARTIAL_EXIT_OWNERSHIP_GUARD_RETRY_ORIGINAL"


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError):
        return default


# ── Finding 1: delegate to the canonical predicate — no parallel taxonomy ─────

def is_partial_exit_result(order: dict[str, Any], result: dict[str, Any]) -> bool:
    """Return True iff the canonical reducer considers this an active partial EXIT.

    Delegates to ``exit_fill_truth_guard._is_partial_result(order, result)``
    — the exact same predicate the reducer itself uses — so verification runs
    under identical conditions.  Sources checked (in canonical priority order):

        result["status"]  →  result["state"]  →  order["status"]

    ``order["state"]`` is not checked because the canonical predicate does not
    check it.  Tests confirm this boundary explicitly so any future canonical
    extension automatically propagates here.
    """
    from ap.exit_fill_truth_guard import _is_partial_result  # canonical predicate
    return _is_partial_result(order, result)


# ── Shared verification function (Finding 2) ──────────────────────────────────

def partial_exit_ownership_fields(
    order: dict[str, Any], result: dict[str, Any]
) -> dict[str, Any]:
    """Compute the expected durable ownership fields for an active partial EXIT.

    ``pending_exit_qty`` is the unfilled remainder on THIS order only:
        remaining = max(0, ordered_qty - cumulative_filled_qty)
    """
    ordered_qty  = max(0, _int(order.get("qty")))
    filled_qty   = max(0, _int(result.get("filled_qty"), _int(order.get("filled_qty"))))
    remaining    = max(0, ordered_qty - filled_qty)
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

    Called after either canonical reconciliation entry point returns.  May:
    - inspect order/result/reconciled fields
    - compute expected ownership fields
    - set ``partial_exit_ownership_verified`` diagnostic metadata
    - emit critical log on invariant failure

    Must never:
    - open a database connection
    - execute SQL
    - update positions, orders, proof_trades, queues, or signals
    - make a broker call
    - submit or cancel orders
    - reopen a closed position
    - become a second EXIT-fill reducer
    """
    if not isinstance(reconciled, dict):
        return reconciled

    # Only verify when the canonical reducer considered this an active partial.
    if not is_partial_exit_result(order, result):
        return reconciled

    projection = reconciled.get("projection")
    if bool(getattr(projection, "closed", False)):
        # Closed projection: ownership was already released canonically.
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


# ── Wrappers ──────────────────────────────────────────────────────────────────

def wrap_exit_fill_reconcile(
    original: Callable[[dict, dict], dict],
) -> Callable[[dict, dict], dict]:
    """Wrap ``_reconcile_exit_fill`` — the fill-monitor / direct entry path."""

    def guarded(order: dict, result: dict) -> dict:
        reconciled = original(order, result)
        return verify_partial_exit_ownership(order, result, reconciled)

    return guarded


def wrap_retry_exit_fill_reconciliation(
    original: Callable[..., dict | None],
) -> Callable[..., dict | None]:
    """Wrap ``retry_exit_fill_reconciliation`` — the startup retry entry path.

    ``retry_exit_fill_reconciliation`` calls ``_run_reconciliation_attempt``
    directly, bypassing ``_reconcile_exit_fill``.  Without this wrapper the
    verification guard would never run on that path.

    The wrapper reconstructs the ``order`` and ``result`` dicts from the
    return value so verification can compare expected vs recorded ownership.
    When the retry returns ``None`` or ``{"already_reconciled": True}`` there
    is no active partial fill to verify, so the value is returned unchanged.
    """

    def guarded(*, client_id: str, local_order_id: str) -> dict | None:
        reconciled = original(client_id=client_id, local_order_id=local_order_id)

        if not isinstance(reconciled, dict):
            return reconciled
        if reconciled.get("already_reconciled"):
            return reconciled

        # Reconstruct a minimal order/result from what the retry path returns.
        # The retry builds its own result dict from the stored order row; the
        # reconciled dict contains enough information for the predicate check.
        order_proxy: dict[str, Any] = {
            "client_id":        client_id,
            "local_order_id":   local_order_id,
            "status":           reconciled.get("status"),
            "broker_order_id":  reconciled.get("broker_order_id"),
            "filled_qty":       reconciled.get("filled_qty"),
            "qty":              reconciled.get("qty"),
        }
        result_proxy: dict[str, Any] = {
            "status":           reconciled.get("status"),
            "state":            reconciled.get("state"),
            "broker_order_id":  reconciled.get("broker_order_id"),
            "filled_qty":       reconciled.get("filled_qty"),
        }
        return verify_partial_exit_ownership(order_proxy, result_proxy, reconciled)

    return guarded


# ── Installer ─────────────────────────────────────────────────────────────────

def install_partial_exit_ownership_guard() -> None:
    """Patch both canonical EXIT-fill entry paths.  Idempotent."""
    from ap import exit_fill_truth_guard

    if not getattr(exit_fill_truth_guard, _PATCHED_ATTR, False):
        original = exit_fill_truth_guard._reconcile_exit_fill
        setattr(exit_fill_truth_guard, _ORIGINAL_ATTR, original)
        exit_fill_truth_guard._reconcile_exit_fill = wrap_exit_fill_reconcile(original)
        setattr(exit_fill_truth_guard, _PATCHED_ATTR, True)

    if not getattr(exit_fill_truth_guard, _RETRY_PATCHED_ATTR, False):
        retry_original = exit_fill_truth_guard.retry_exit_fill_reconciliation
        setattr(exit_fill_truth_guard, _RETRY_ORIGINAL_ATTR, retry_original)
        exit_fill_truth_guard.retry_exit_fill_reconciliation = (
            wrap_retry_exit_fill_reconciliation(retry_original)
        )
        setattr(exit_fill_truth_guard, _RETRY_PATCHED_ATTR, True)
