"""Converge post-fill proof repair with performance taxonomy.

Proof insertion and fill reconciliation can occur in either order. This wrapper
runs after canonical EXIT fill reconciliation and stamps taxonomy on any proof
row that already exists. If proof is inserted later, proof_taxonomy_guard's
writer wrapper performs the same classification.
"""
from __future__ import annotations

from typing import Any, Callable

from ap import db
from ap.logger import get_logger
from ap.proof_taxonomy_guard import classify_performance_taxonomy

log = get_logger("ap.proof_taxonomy_fill_bridge")

_PATCHED_ATTR = "_AP_PROOF_TAXONOMY_FILL_BRIDGE_PATCHED"
_ORIGINAL_ATTR = "_AP_PROOF_TAXONOMY_FILL_BRIDGE_ORIGINAL"


def _stamp_reconciled_taxonomy(*, client_id: str, position_id: str, stamp: dict[str, Any]) -> int:
    client_id = str(client_id or "").strip()
    position_id = str(position_id or "").strip()
    if not client_id or not position_id:
        return 0

    def _update() -> int:
        with db.conn() as c:
            columns = {
                str(dict(row).get("column_name") or "")
                for row in c.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema='public' AND table_name='proof_trades'"
                ).fetchall()
            }
            updates = {key: value for key, value in stamp.items() if key in columns}
            if not updates:
                return 0
            set_sql = ", ".join(f"{key}=%s" for key in updates)
            cur = c.execute(
                f"UPDATE proof_trades SET {set_sql} "
                "WHERE client_email=%s AND position_id::text=%s",
                tuple(updates.values()) + (client_id, position_id),
            )
            return int(getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0)

    try:
        return int(db.run_with_retry(_update) or 0)
    except Exception as exc:
        log.critical(
            "post-fill proof taxonomy convergence failed client=%s position=%s error=%s",
            client_id,
            position_id,
            exc,
        )
        return 0


def wrap_exit_fill_reconcile(original: Callable[[dict, dict], dict]) -> Callable[[dict, dict], dict]:
    def guarded(order: dict, result: dict) -> dict:
        reconciled = original(order, result)
        if not isinstance(reconciled, dict):
            return reconciled

        base = {
            "execution_mode": str(reconciled.get("execution_mode") or "unknown").lower(),
            "official_live_performance_eligible": bool(
                reconciled.get("official_live_performance_eligible")
            ),
        }
        taxonomy = classify_performance_taxonomy(base)
        client_id = str(order.get("client_id") or "").strip()
        position_id = str(reconciled.get("position_id") or "").strip()
        updated = _stamp_reconciled_taxonomy(
            client_id=client_id,
            position_id=position_id,
            stamp=taxonomy,
        )
        reconciled.update(taxonomy)
        reconciled["proof_taxonomy_rows_updated"] = updated
        return reconciled

    return guarded


def install_proof_taxonomy_fill_bridge() -> None:
    from ap import exit_fill_truth_guard

    if getattr(exit_fill_truth_guard, _PATCHED_ATTR, False):
        return
    original = exit_fill_truth_guard._reconcile_exit_fill
    setattr(exit_fill_truth_guard, _ORIGINAL_ATTR, original)
    exit_fill_truth_guard._reconcile_exit_fill = wrap_exit_fill_reconcile(original)
    setattr(exit_fill_truth_guard, _PATCHED_ATTR, True)
