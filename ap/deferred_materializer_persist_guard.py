"""
ap/deferred_materializer_persist_guard.py — fail-closed guard for selected materialization persistence.

PR #300 introduced meta.broker_ready as the broker-submit lifecycle gate.
That only works if the SELECTED/broker_ready=True stamp is durably persisted
before execution core mirrors broker_ready into the in-memory plan and reaches
pre-submit.

This guard wraps ap.deferred_materializer.stamp_selected so a valid real-OCC
selection that fails to persist cannot silently return False and let execution
continue. Invalid placeholder inputs still return False exactly as the underlying
helper intended; only the dangerous case is escalated:

    real OCC + limit>0.01 + qty>=1 + update_order_meta failed

In that case we raise MaterializationSelectedPersistError. The existing
execution-core try/except around stamp_selected will skip the in-memory
broker_ready=True mirror, and the downstream pre-submit broker_ready gate blocks
broker POST. We also attempt to terminalize the row with a precise last_error.
"""
from __future__ import annotations

from typing import Any

from ap.logger import get_logger

log = get_logger("ap.deferred_materializer_persist_guard")

_PATCHED_ATTR = "_AP_SELECTED_PERSIST_GUARD_PATCHED"
_ORIGINAL_ATTR = "_AP_SELECTED_PERSIST_GUARD_ORIGINAL"
_REASON = "materialization_selected_meta_persist_failed"


class MaterializationSelectedPersistError(RuntimeError):
    """Raised when SELECTED/broker_ready=True could not be durably persisted."""


def _looks_like_valid_selected_payload(contract: Any, limit_price: Any, qty: Any) -> bool:
    try:
        contract_s = str(contract or "").strip()
        limit_f = float(limit_price or 0)
        qty_i = int(qty or 0)
    except Exception:
        return False
    return bool(contract_s) and not contract_s.upper().startswith("DEFERRED:") and limit_f > 0.01 and qty_i >= 1


def _best_effort_terminalize(osm: Any, local_order_id: str) -> None:
    """Try to stamp the durable row with the exact persistence failure reason."""
    if not osm or not local_order_id:
        return
    try:
        transition = getattr(osm, "transition", None)
        if callable(transition):
            transition(local_order_id, "EXPIRED", last_error=_REASON)
    except Exception as exc:
        log.debug(
            "selected materialization persist failure terminalize failed local_order_id=%s: %s",
            local_order_id,
            exc,
        )


def install_selected_materialization_persist_guard() -> None:
    """Patch deferred_materializer.stamp_selected to fail closed on persist failure."""
    from ap import deferred_materializer as dm

    if getattr(dm, _PATCHED_ATTR, False):
        return

    original = dm.stamp_selected
    setattr(dm, _ORIGINAL_ATTR, original)

    def guarded_stamp_selected(osm, local_order_id: str, *args, **kwargs):
        ok = original(osm, local_order_id, *args, **kwargs)
        if ok:
            return True

        contract = kwargs.get("contract")
        limit_price = kwargs.get("limit_price")
        qty = kwargs.get("qty")
        if not _looks_like_valid_selected_payload(contract, limit_price, qty):
            # Preserve original invalid-input behavior: invalid DEFERRED/0.01/qty=0
            # payloads simply refuse broker_ready=True and return False.
            return False

        client_id = str(kwargs.get("client_id") or "")
        execution_mode = str(kwargs.get("execution_mode") or "")
        symbol = str(kwargs.get("symbol") or "")
        log.critical(
            "MATERIALIZATION_PRE_SUBMIT_INVARIANT_FAILED "
            "order_id=%s client_id=%s execution_mode=%s symbol=%s "
            "failure_reason=%s contract_after=%s limit_after=%.4f qty=%d "
            "materialization_status=selected_persist_failed broker_ready=false",
            str(local_order_id or ""),
            client_id,
            execution_mode,
            symbol,
            _REASON,
            str(contract or ""),
            float(limit_price or 0),
            int(qty or 0),
        )
        _best_effort_terminalize(osm, str(local_order_id or ""))
        raise MaterializationSelectedPersistError(_REASON)

    dm.stamp_selected = guarded_stamp_selected
    setattr(dm, _PATCHED_ATTR, True)
