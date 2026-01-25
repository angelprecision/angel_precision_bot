# ap/reconcile.py
import re
import uuid
from typing import Optional, Any, Dict

from ap.logger import get_logger
from ap.utils import now_utc_iso, json_dumps
from ap.db import conn, run_with_retry, update_order
from ap.config import Config

log = get_logger("ap.reconcile")
cfg = Config()

OPT_MULTIPLIER = 100  # standard US equity options


def audit(level: str, event: str, payload: dict):
    with conn() as c:
        run_with_retry(lambda: c.execute(
            "INSERT INTO audit_log (ts, level, event, payload) VALUES (?,?,?,?)",
            (now_utc_iso(), level, event, json_dumps(payload)),
        ))


def _tradier_status_to_local(remote: dict) -> str:
    """
    Tradier order status mapping.
    Tradier examples: pending, open, filled, partially_filled, rejected, canceled
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
    return "ACK"


def _extract_error(remote: dict) -> Optional[str]:
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


def _to_float(x: Any) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None


def _extract_filled_qty(remote: Dict[str, Any]) -> Optional[int]:
    for k in ("exec_quantity", "filled_quantity", "filled", "quantity_executed", "last_fill_quantity"):
        if k in remote and remote.get(k) is not None:
            v = _to_int(remote.get(k))
            if v is not None:
                return v
    return None


def _extract_fill_price(remote: Dict[str, Any]) -> Optional[float]:
    # Prefer avg_fill_price; fallback last_fill_price
    for k in ("avg_fill_price", "last_fill_price"):
        fv = _to_float(remote.get(k))
        if fv is not None and fv > 0:
            return fv
    return None


def _infer_direction_from_contract(contract: str) -> str:
    # Example: AAPL260130C00200000 -> CALL
    m = re.search(r"\d{6}([CP])", contract)
    if m:
        return "CALL" if m.group(1) == "C" else "PUT"
    return "CALL"


def _get_open_orders(limit: int = 50):
    with conn() as c:
        rows = run_with_retry(lambda: c.execute(
            """
            SELECT id, local_order_id, broker_order_id, status, kind, symbol, contract, qty, position_id, filled_qty
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


def _create_position_from_entry(order_row: dict, fill_price: float, filled_qty: int) -> str:
    pos_id = str(uuid.uuid4())
    direction = _infer_direction_from_contract(order_row["contract"])
    tp_pct = float(cfg.TAKE_PROFIT_PCT)
    sl_pct = float(cfg.STOP_LOSS_PCT)

    with conn() as c:
        run_with_retry(lambda: c.execute(
            """
            INSERT INTO positions (
                id, underlying, contract, direction, qty, avg_fill,
                entry_ts, tp_pct, sl_pct, status
            )
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (
                pos_id,
                order_row["symbol"],
                order_row["contract"],
                direction,
                int(filled_qty),
                float(fill_price),
                now_utc_iso(),
                tp_pct,
                sl_pct,
                "OPEN",
            ),
        ))

        # link order -> position
        run_with_retry(lambda: c.execute(
            "UPDATE orders SET position_id=?, updated_ts=? WHERE local_order_id=?",
            (pos_id, now_utc_iso(), order_row["local_order_id"]),
        ))

    return pos_id


def _close_position_from_exit(order_row: dict, fill_price: float):
    pos_id = order_row.get("position_id")
    if not pos_id:
        return

    with conn() as c:
        pos = run_with_retry(lambda: c.execute(
            "SELECT id, qty, avg_fill, status FROM positions WHERE id=?",
            (pos_id,),
        ).fetchone())
        if not pos:
            return

        entry_price = float(pos["avg_fill"])
        qty = int(pos["qty"])
        realized = (float(fill_price) - entry_price) * qty * OPT_MULTIPLIER

        run_with_retry(lambda: c.execute(
            """
            UPDATE positions
            SET status='CLOSED', exit_ts=?, exit_reason=?, realized_pnl=?
            WHERE id=?
            """,
            (now_utc_iso(), "EXIT_FILLED", float(realized), pos_id),
        ))


def reconcile_once(broker, limit: int = 50) -> int:
    orders = _get_open_orders(limit=limit)
    if not orders:
        return 0

    processed = 0

    for o in orders:
        broker_id = o.get("broker_order_id")
        if not broker_id or broker_id == "N/A":
            continue

        try:
            remote = broker.get_order(broker_id)
            new_status = _tradier_status_to_local(remote)
            err = _extract_error(remote) if new_status == "REJECTED" else None

            new_filled_qty = _extract_filled_qty(remote)
            if new_filled_qty is None:
                new_filled_qty = int(o.get("filled_qty") or 0)

            # ✅ only write + audit when something changed
            changed = (o.get("status") != new_status) or (int(o.get("filled_qty") or 0) != int(new_filled_qty or 0))

            if changed:
                update_order(
                    o["local_order_id"],
                    status=new_status,
                    broker_order_id=broker_id,
                    last_error=err,
                    filled_qty=new_filled_qty,
                )

                audit("INFO", "RECONCILE_ORDER_UPDATE", {
                    "local_order_id": o["local_order_id"],
                    "broker_order_id": broker_id,
                    "prev_status": o.get("status"),
                    "new_status": new_status,
                    "filled_qty": int(new_filled_qty or 0),
                })

            # ✅ ENTRY FILLED -> create position if missing
            if new_status == "FILLED" and o.get("kind") == "ENTRY" and not o.get("position_id"):
                fill_price = _extract_fill_price(remote)
                if fill_price and int(new_filled_qty or 0) > 0:
                    pos_id = _create_position_from_entry(o, fill_price, int(new_filled_qty))
                    audit("INFO", "POSITION_CREATED_FROM_FILL", {
                        "local_order_id": o["local_order_id"],
                        "position_id": pos_id,
                        "fill_price": fill_price,
                        "filled_qty": int(new_filled_qty),
                    })

            # ✅ EXIT FILLED -> close linked position
            if new_status == "FILLED" and o.get("kind") == "EXIT" and o.get("position_id"):
                fill_price = _extract_fill_price(remote)
                if fill_price:
                    _close_position_from_exit(o, fill_price)
                    audit("INFO", "POSITION_CLOSED_FROM_EXIT_FILL", {
                        "local_order_id": o["local_order_id"],
                        "position_id": o.get("position_id"),
                        "fill_price": fill_price,
                    })

            processed += 1

        except Exception as e:
            log.exception(f"Reconcile failed for broker_order_id={broker_id}: {e}")

    return processed
