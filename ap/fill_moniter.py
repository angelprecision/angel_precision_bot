# ap/fill_monitor.py
"""
Fill Monitor - Polls Tradier to confirm order fills
THE ACCURACY LAYER - Don't trust ACK, verify fills!
"""
import time
from datetime import datetime, timezone

from ap.db import conn, run_with_retry
from ap.utils import now_utc_iso, json_dumps
from ap.logger import get_logger
from ap.state import load_state
from ap.broker import BrokerAdapter

log = get_logger("ap.fill_monitor")


def audit(level: str, event: str, payload: dict):
    """Log to audit table"""
    with conn() as c:
        run_with_retry(lambda: c.execute(
            "INSERT INTO audit_log (ts, level, event, payload) VALUES (?,?,?,?)",
            (now_utc_iso(), level, event, json_dumps(payload)),
        ))


def get_pending_orders():
    """Get all orders that need fill confirmation"""
    with conn() as c:
        rows = run_with_retry(lambda: c.execute("""
            SELECT 
                local_order_id,
                broker_order_id,
                symbol,
                contract,
                qty,
                limit_price,
                status,
                created_ts
            FROM orders
            WHERE status IN ('NEW', 'ACK', 'PARTIAL')
            AND kind = 'ENTRY'
            ORDER BY created_ts ASC
        """).fetchall())
        return [dict(r) for r in rows]


def update_order_status(local_order_id: str, status: str, filled_qty: int = None, avg_fill: float = None, error: str = None):
    """Update order in database"""
    with conn() as c:
        updates = ["status=?", "updated_ts=?"]
        params = [status, now_utc_iso()]
        
        if filled_qty is not None:
            updates.append("filled_qty=?")
            params.append(filled_qty)
        
        if error is not None:
            updates.append("last_error=?")
            params.append(error)
        
        params.append(local_order_id)
        
        sql = f"UPDATE orders SET {', '.join(updates)} WHERE local_order_id=?"
        run_with_retry(lambda: c.execute(sql, params))
        
        # Store avg_fill in metadata if needed
        if avg_fill is not None:
            # We'll use position table for this
            pass


def create_position_from_fill(order: dict, avg_fill_price: float, filled_qty: int):
    """
    Create position after confirmed fill
    This is the ONLY place positions should be created for Tradier orders!
    """
    import uuid
    
    # Get TP/SL from config
    from ap.config import Config
    cfg = Config()
    
    pos_id = str(uuid.uuid4())
    
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
                order["symbol"],
                order["contract"],
                "CALL",  # TODO: Need to store direction in orders table
                filled_qty,
                avg_fill_price,
                now_utc_iso(),
                cfg.TAKE_PROFIT_PCT,
                cfg.STOP_LOSS_PCT,
                "OPEN",
            ),
        ))
    
    # Link position to order
    with conn() as c:
        run_with_retry(lambda: c.execute(
            "UPDATE orders SET position_id=? WHERE local_order_id=?",
            (pos_id, order["local_order_id"])
        ))
    
    log.info(f"✅ Position created: {pos_id} for {order['contract']} @ ${avg_fill_price:.2f}")
    
    audit("INFO", "POSITION_CREATED_FROM_FILL", {
        "position_id": pos_id,
        "local_order_id": order["local_order_id"],
        "broker_order_id": order.get("broker_order_id"),
        "contract": order["contract"],
        "qty": filled_qty,
        "avg_fill": avg_fill_price
    })
    
    return pos_id


def check_order_with_broker(broker: BrokerAdapter, order: dict) -> dict:
    """
    Query Tradier for actual order status
    Returns: {"status": str, "filled_qty": int, "avg_fill": float, "reason": str}
    """
    broker_order_id = order.get("broker_order_id")
    
    if not broker_order_id or broker_order_id == "N/A":
        log.warning(f"Order {order['local_order_id']} has no broker_order_id")
        return {"status": "UNKNOWN", "reason": "NO_BROKER_ID"}
    
    try:
        # Query Tradier
        broker_order = broker.get_order(broker_order_id)
        
        # Parse Tradier response
        status = (broker_order.get("status") or "").upper()
        
        # Map Tradier statuses to our statuses
        status_map = {
            "FILLED": "FILLED",
            "OPEN": "ACK",  # Still pending
            "PENDING": "ACK",
            "PARTIALLY_FILLED": "PARTIAL",
            "CANCELED": "CANCELED",
            "REJECTED": "REJECTED",
            "EXPIRED": "EXPIRED"
        }
        
        our_status = status_map.get(status, "UNKNOWN")
        
        # Get fill details
        filled_qty = int(broker_order.get("exec_quantity") or broker_order.get("quantity_filled") or 0)
        avg_fill_price = float(broker_order.get("avg_fill_price") or broker_order.get("price") or 0.0)
        
        return {
            "status": our_status,
            "filled_qty": filled_qty,
            "avg_fill": avg_fill_price if filled_qty > 0 else 0.0,
            "reason": broker_order.get("reason") or broker_order.get("status"),
            "raw": broker_order
        }
        
    except Exception as e:
        log.error(f"Failed to check order {broker_order_id} with broker: {e}")
        audit("ERROR", "FILL_CHECK_FAILED", {
            "local_order_id": order["local_order_id"],
            "broker_order_id": broker_order_id,
            "error": str(e)
        })
        return {"status": "ERROR", "reason": str(e)}


def process_pending_order(broker: BrokerAdapter, order: dict):
    """Check one pending order and update accordingly"""
    
    # Check with broker
    result = check_order_with_broker(broker, order)
    
    local_id = order["local_order_id"]
    broker_id = order.get("broker_order_id")
    
    # Handle different statuses
    if result["status"] == "FILLED":
        log.info(f"🎯 Order FILLED: {broker_id} - {order['contract']} x{result['filled_qty']} @ ${result['avg_fill']:.2f}")
        
        # Update order
        update_order_status(local_id, "FILLED", result["filled_qty"], result["avg_fill"])
        
        # Create position
        pos_id = create_position_from_fill(order, result["avg_fill"], result["filled_qty"])
        
        # Update state
        state = load_state()
        update_state({"trades_taken_today": int(state.get("trades_taken_today", 0)) + 1})
        
        audit("INFO", "ORDER_FILLED", {
            "local_order_id": local_id,
            "broker_order_id": broker_id,
            "position_id": pos_id,
            "filled_qty": result["filled_qty"],
            "avg_fill": result["avg_fill"]
        })
        
    elif result["status"] == "PARTIAL":
        log.info(f"⏳ Order PARTIAL: {broker_id} - {result['filled_qty']}/{order['qty']}")
        
        update_order_status(local_id, "PARTIAL", result["filled_qty"])
        
        audit("INFO", "ORDER_PARTIAL", {
            "local_order_id": local_id,
            "broker_order_id": broker_id,
            "filled_qty": result["filled_qty"],
            "total_qty": order["qty"]
        })
        
    elif result["status"] in ("REJECTED", "CANCELED", "EXPIRED"):
        log.warning(f"❌ Order {result['status']}: {broker_id} - {result.get('reason')}")
        
        update_order_status(local_id, result["status"], error=result.get("reason"))
        
        audit("WARNING", f"ORDER_{result['status']}", {
            "local_order_id": local_id,
            "broker_order_id": broker_id,
            "reason": result.get("reason")
        })
        
    elif result["status"] == "ACK":
        # Still pending, check age
        from datetime import datetime
        created = datetime.fromisoformat(order["created_ts"])
        age_seconds = (datetime.now(timezone.utc) - created).total_seconds()
        
        if age_seconds > 300:  # 5 minutes
            log.warning(f"⚠️ Order pending for {age_seconds:.0f}s: {broker_id}")
            
            audit("WARNING", "ORDER_PENDING_LONG", {
                "local_order_id": local_id,
                "broker_order_id": broker_id,
                "age_seconds": age_seconds
            })
    
    elif result["status"] == "ERROR":
        log.error(f"💥 Error checking order: {broker_id} - {result['reason']}")
        # Don't update status on transient errors, will retry next loop


def fill_monitor_loop(broker: BrokerAdapter, poll_seconds: float = 10.0):
    """
    Main fill monitoring loop
    Runs continuously, checking pending orders
    """
    log.info("🔍 Fill monitor started")
    
    while True:
        try:
            # Check kill switch
            state = load_state()
            if state.get("kill_switch"):
                log.warning("Kill switch enabled, fill monitor paused")
                time.sleep(poll_seconds)
                continue
            
            # Get all pending orders
            pending = get_pending_orders()
            
            if pending:
                log.info(f"Checking {len(pending)} pending order(s)...")
                
                for order in pending:
                    try:
                        process_pending_order(broker, order)
                    except Exception as e:
                        log.exception(f"Failed to process order {order.get('local_order_id')}: {e}")
            
            # Sleep before next check
            time.sleep(poll_seconds)
            
        except Exception as e:
            log.exception(f"Fill monitor error: {e}")
            time.sleep(poll_seconds * 2)  # Back off on errors
