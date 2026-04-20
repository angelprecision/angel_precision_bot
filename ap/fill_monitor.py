# ap/fill_monitor.py - PRODUCTION SAFE (FIXED)
"""
Fill Monitor - Polls broker to confirm order fills
THE ACCURACY LAYER - Don't trust ACK, verify fills!

CRITICAL RULE:
- Fill monitor MUST NEVER pause on kill switch. It reconciles reality.
- Releases symbol locks + reserved equity for ENTRY when FILLED/REJECTED/CANCELED/EXPIRED.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

from ap.db import conn, run_with_retry
from ap.utils import now_utc_iso, json_dumps
from ap.logger import get_logger
from ap.config import Config
from ap.state import release_equity, release_symbol_lock
from ap.broker import BrokerAdapter

log = get_logger("ap.fill_monitor")
cfg = Config()

OPT_MULTIPLIER = 100


def audit(client_id: str, level: str, event: str, payload: dict):
    with conn() as c:
        run_with_retry(lambda: c.execute(
            "INSERT INTO audit_log (ts, level, event, payload, client_id) VALUES (%s,%s,%s,%s,%s)",
            (now_utc_iso(), level, event, json_dumps(payload), client_id),
        ))


def get_pending_orders():
    with conn() as c:
        rows = run_with_retry(lambda: c.execute("""
            SELECT
                client_id,
                local_order_id,
                broker_order_id,
                position_id,
                kind,
                symbol,
                contract,
                direction,
                qty,
                limit_price,
                reserved_cost,
                status,
                created_ts
            FROM orders
            WHERE kind IN ('ENTRY','EXIT')
              AND status IN (
              'CREATED',
              'SUBMITTED',
              'ACKNOWLEDGED',
              'PARTIAL_FILL',
              'EXIT_SUBMITTED',
              'EXIT_ACKNOWLEDGED',
              'EXIT_PARTIAL_FILL'
            ) -- canonical statuses from APOrderStateMachine
              AND broker_order_id IS NOT NULL
              AND broker_order_id != 'N/A'
            ORDER BY created_ts ASC
        """).fetchall())
        return [dict(r) for r in rows]


def update_order_status(local_order_id: str, status: str, filled_qty: int | None = None, error: str | None = None):
    with conn() as c:
        updates = ["status=%s", "updated_ts=%s"]
        params = [status, now_utc_iso()]

        if filled_qty is not None:
            updates.append("filled_qty=%s")
            params.append(int(filled_qty))

        if error is not None:
            updates.append("last_error=%s")
            params.append(error)

        params.append(local_order_id)
        sql = f"UPDATE orders SET {', '.join(updates)} WHERE local_order_id=%s"
        run_with_retry(lambda: c.execute(sql, params))


def create_position_from_fill(order: dict, avg_fill_price: float, filled_qty: int):
    """
    Create position after confirmed ENTRY fill
    """
    import uuid

    pos_id = str(uuid.uuid4())
    client_id = order["client_id"]
    direction = (order.get("direction") or "CALL").upper()

    with conn() as c:
        run_with_retry(lambda: c.execute(
            """
            INSERT INTO positions (
                id, client_id, underlying, contract, direction, qty, avg_fill,
                entry_ts, tp_pct, sl_pct, status
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                pos_id,
                client_id,
                order["symbol"],
                order["contract"],
                direction,
                int(filled_qty),
                float(avg_fill_price),
                now_utc_iso(),
                float(cfg.TAKE_PROFIT_PCT),
                float(cfg.STOP_LOSS_PCT),
                "OPEN",
            ),
        ))

    with conn() as c:
        run_with_retry(lambda: c.execute(
            "UPDATE orders SET position_id=%s WHERE local_order_id=%s",
            (pos_id, order["local_order_id"]),
        ))

    audit(client_id, "INFO", "POSITION_CREATED_FROM_FILL", {
        "position_id": pos_id,
        "local_order_id": order["local_order_id"],
        "broker_order_id": order.get("broker_order_id"),
        "contract": order["contract"],
        "qty": int(filled_qty),
        "avg_fill": float(avg_fill_price),
        "direction": direction,
    })

    return pos_id


def close_position_from_exit_fill(order: dict, avg_fill_price: float):
    """
    Close position after confirmed EXIT fill
    """
    client_id = order["client_id"]
    position_id = order.get("position_id")

    if not position_id:
        log.error(f"Exit order has no position_id: {order.get('local_order_id')}")
        return

    with conn() as c:
        pos_row = run_with_retry(lambda: c.execute(
            "SELECT * FROM positions WHERE id=%s AND client_id=%s",
            (position_id, client_id),
        ).fetchone())

    if not pos_row:
        log.error(f"Position not found: {position_id}")
        return

    pos = dict(pos_row)
    entry_price = float(pos["avg_fill"])
    qty = int(pos["qty"])
    realized_pnl = (float(avg_fill_price) - entry_price) * qty * OPT_MULTIPLIER

    with conn() as c:
        run_with_retry(lambda: c.execute(
            """
            UPDATE positions
            SET status='CLOSED', exit_ts=%s, realized_pnl=%s
            WHERE id=%s AND client_id=%s
            """,
            (now_utc_iso(), float(realized_pnl), position_id, client_id),
        ))

    # Update client_state realized_pnl_today atomically (no races)
    with conn() as c:
        run_with_retry(lambda: c.execute(
            """
            UPDATE client_state
            SET realized_pnl_today = COALESCE(realized_pnl_today, 0.0) + %s
            WHERE client_id=%s
            """,
            (float(realized_pnl), client_id),
        ))

    audit(client_id, "INFO", "POSITION_CLOSED_FROM_EXIT", {
        "position_id": position_id,
        "contract": pos["contract"],
        "entry_price": entry_price,
        "exit_price": float(avg_fill_price),
        "qty": qty,
        "realized_pnl": float(realized_pnl),
    })


def check_order_with_broker(broker: BrokerAdapter, order: dict) -> dict:
    """
    Query broker for actual order status.
    Returns: {"status": str, "filled_qty": int, "avg_fill": float, "reason": str, "raw": dict}
    """
    broker_order_id = order.get("broker_order_id")
    if not broker_order_id or broker_order_id == "N/A":
        return {"status": "UNKNOWN", "reason": "NO_BROKER_ID"}

    try:
        raw = broker.get_order(broker_order_id)
        status = (raw.get("status") or "").upper()

        status_map = {
            "FILLED":           "FILLED",
            "OPEN":             "ACKNOWLEDGED",
            "PENDING":          "ACKNOWLEDGED",
            "PARTIALLY_FILLED": "PARTIAL_FILL",
            "CANCELED":         "CANCELED",
            "REJECTED":         "REJECTED",
            "EXPIRED":          "EXPIRED",
        }
        our = status_map.get(status, "UNKNOWN")

        filled_qty = int(raw.get("exec_quantity") or raw.get("filled_quantity") or raw.get("quantity") or 0)
        avg_fill = float(raw.get("avg_fill_price") or raw.get("price") or 0.0)

        return {
            "status": our,
            "filled_qty": filled_qty,
            "avg_fill": avg_fill,
            "reason": raw.get("reason") or status,
            "raw": raw,
        }

    except Exception as e:
        audit(order["client_id"], "ERROR", "FILL_CHECK_FAILED", {
            "error": str(e),
            "broker_order_id": broker_order_id,
            "local_order_id": order.get("local_order_id"),
        })
        return {"status": "ERROR", "reason": str(e)}


def _release_entry_guards(order: dict):
    """
    Releases reserved equity and symbol lock for ENTRY orders.
    Uses orders.reserved_cost if present, else falls back to limit_price*qty*100.
    """
    client_id = order["client_id"]
    symbol = order["symbol"]

    cost = None
    if order.get("reserved_cost") is not None:
        try:
            cost = float(order["reserved_cost"])
        except Exception:
            cost = None

    if cost is None:
        cost = float(order.get("limit_price") or 0.0) * int(order.get("qty") or 0) * OPT_MULTIPLIER

    if cost and cost > 0:
        release_equity(client_id, cost)

    release_symbol_lock(client_id, symbol)


def process_pending_order(broker: BrokerAdapter, order: dict):
    client_id = order["client_id"]
    local_id = order["local_order_id"]
    broker_id = order.get("broker_order_id")
    kind = (order.get("kind") or "ENTRY").upper()

    result = check_order_with_broker(broker, order)

    if result["status"] == "FILLED":
        update_order_status(local_id, "FILLED", filled_qty=result["filled_qty"])

        if kind == "ENTRY":
            create_position_from_fill(order, result["avg_fill"], result["filled_qty"])
            _release_entry_guards(order)

        elif kind == "EXIT":
            close_position_from_exit_fill(order, result["avg_fill"])

        audit(client_id, "INFO", "ORDER_FILLED", {
            "local_order_id": local_id,
            "broker_order_id": broker_id,
            "kind": kind,
            "filled_qty": result["filled_qty"],
            "avg_fill": result["avg_fill"],
        })
        return

    if result["status"] == "PARTIAL_FILL":
        update_order_status(local_id, "PARTIAL_FILL", filled_qty=result["filled_qty"])
        audit(client_id, "INFO", "ORDER_PARTIAL", {
            "local_order_id": local_id,
            "broker_order_id": broker_id,
            "kind": kind,
            "filled_qty": result["filled_qty"],
            "total_qty": int(order["qty"]),
        })
        return

    if result["status"] in ("REJECTED", "CANCELED", "EXPIRED"):
        update_order_status(local_id, result["status"], error=result.get("reason"))

        if kind == "ENTRY":
            _release_entry_guards(order)

        if kind == "EXIT" and order.get("position_id"):
            with conn() as c:
                run_with_retry(lambda: c.execute(
                    "UPDATE positions SET status='OPEN', exit_reason=NULL WHERE id=%s AND client_id=%s",
                    (order["position_id"], client_id),
                ))

        audit(client_id, "WARNING", f"ORDER_{result['status']}", {
            "local_order_id": local_id,
            "broker_order_id": broker_id,
            "kind": kind,
            "reason": result.get("reason"),
        })
        return

    if result["status"] == "ACKNOWLEDGED":
        # Still pending - log a warning if it's been pending too long
        try:
            created = datetime.fromisoformat(order["created_ts"])
            age = (datetime.now(timezone.utc) - created).total_seconds()
            if age > 300:
                audit(client_id, "WARNING", "ORDER_PENDING_LONG", {
                    "local_order_id": local_id,
                    "broker_order_id": broker_id,
                    "kind": kind,
                    "age_seconds": age,
                })
        except Exception:
            pass
        return

    if result["status"] == "ERROR":
        audit(client_id, "ERROR", "ORDER_CHECK_ERROR", {
            "local_order_id": local_id,
            "broker_order_id": broker_id,
            "reason": result.get("reason"),
        })


def fill_monitor_loop(broker: BrokerAdapter, poll_seconds: float = 10.0):
    """
    Fill monitor must NEVER pause on kill switch.
    It's the reconciliation layer.
    """
    log.info("🔍 Fill monitor started")

    while True:
        try:
            pending = get_pending_orders()
            for order in pending:
                try:
                    process_pending_order(broker, order)
                except Exception as e:
                    log.exception(f"Failed to process order {order.get('local_order_id')}: {e}")

            time.sleep(poll_seconds)

        except Exception as e:
            log.exception(f"Fill monitor loop error: {e}")
            time.sleep(poll_seconds * 2)

