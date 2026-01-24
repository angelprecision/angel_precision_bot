# ap/reconcile.py
from typing import Optional, Any, Dict

from ap.logger import get_logger
from ap.utils import now_utc_iso, json_dumps
from ap.db import conn, run_with_retry, update_order

log = get_logger("ap.reconcile")


def audit(level: str, event: str, payload: dict):
    with conn() as c:
        run_with_retry(lambda: c.execute(
            "INSERT INTO audit_log (ts, level, event, payload) VALUES (?,?,?,?)",
            (now_utc_iso(), level, event, json_dumps(payload)),
        ))


def _tradier_status_to_local(remote: dict) -> str:
    """
    Tradier order status mapping.
    Tradier status examples: open, filled, partially_filled, rejected, canceled
    """
    s = str(remote.get("status", "")).lower().strip()
    if s == "filled":
        return "FILLED"
    if s in ("partially_filled", "partial_filled", "partially-filled"):
        return "PARTIAL"
    if s == "rejected":
        return "REJECTED"
    if s in ("canceled", "cancelled"):
        return "CANCELED"
    if s in ("open", "pending", "submitted"):
        return "ACK"
    # unknown -> keep reconciling
    return "ACK"


def _extract_error(remote: dict) -> Optional[str]:
    # Tradier sometimes returns reason/message/error
    for k in ("reason", "message", "error"):
        v = remote.get(k)
        if v:
            return str(v)
    return None


def _to_int(x: Any) -> Optional[int]:
    try:
        return int(float(x))
    except Exception:
        return None


def _extract_filled_qty(remote: Dict[str, Any]) -> Optional[int]:
    """
    Try common Tradier keys for filled quantity.
    """
    for k in ("exec_quantity", "filled_quantity", "filled", "quantity_executed"):
        if k in remote and remote.get(k) is not None:
            v = _to_int(remote.get(k))
            if v is not None:
                return v
    return None


def _get_open_orders(limit: int = 50):
    """
    Orders that are not final and have broker_order_id.
    """
    with conn() as c:
        rows = run_with_retry(lambda: c.execute(
            """
            SELECT local_order_id, broker_order_id, status, kind, symbol, contract, qty, position_id
            FROM orders
            WHERE broker_order_id IS NOT NULL
              AND broker_order_id != 'N/A'
              AND status IN ('NEW','ACK','PARTIAL')
            ORDER BY updated_ts ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall())
        return [dict(r) for r in rows]


def reconcile_once(broker, limit: int = 50) -> int:
    """
    Poll broker.get_order() for each open order and update local orders table.
    Returns number of orders processed.
    """
    orders = _get_open_orders(limit=limit)
    if not orders:
        return 0

    processed = 0

    for o in orders:
        broker_id = o.get("broker_order_id")
        if not broker_id or broker_id == "N/A":
            continue

        try:
            remote = broker.get_order(broker_id)  # TradierBroker implements this
            local_status = _tradier_status_to_local(remote)
            err = _extract_error(remote) if local_status == "REJECTED" else None
            filled_qty = _extract_filled_qty(remote)

            # Update local order record
            update_order(
                o["local_order_id"],
                status=local_status,
                broker_order_id=broker_id,
                last_error=err,
                filled_qty=filled_qty,
            )

            audit("INFO", "RECONCILE_ORDER_UPDATE", {
                "local_order_id": o["local_order_id"],
                "broker_order_id": broker_id,
                "prev_status": o.get("status"),
                "new_status": local_status,
                "filled_qty": filled_qty,
            })

            processed += 1

        except Exception as e:
            log.exception(f"Reconcile failed for broker_order_id={broker_id}: {e}")

    return processed
