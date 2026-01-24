# ap/exit_manager.py
import time
from datetime import datetime, timezone
from ap.db import conn, run_with_retry
from ap.utils import now_utc_iso, json_dumps
from ap.logger import get_logger
from ap.state import load_state, update_state
from ap.contract_pricing import get_contract_price
from ap.broker import BrokerAdapter

log = get_logger("ap.exit")

def audit(level: str, event: str, payload: dict):
    with conn() as c:
        run_with_retry(lambda: c.execute(
            "INSERT INTO audit_log (ts, level, event, payload) VALUES (?,?,?,?)",
            (now_utc_iso(), level, event, json_dumps(payload)),
        ))


def get_open_positions():
    """Get all open positions"""
    with conn() as c:
        rows = run_with_retry(lambda: c.execute("""
            SELECT id, underlying, contract, direction, qty, avg_fill, 
                   tp_pct, sl_pct, entry_ts
            FROM positions
            WHERE status='OPEN'
            ORDER BY entry_ts ASC
        """).fetchall())
        return [dict(r) for r in rows]


def close_position(position_id: str, exit_price: float, reason: str):
    """Mark position as closed"""
    with conn() as c:
        run_with_retry(lambda: c.execute("""
            UPDATE positions
            SET status='CLOSED', exit_ts=?, exit_reason=?, realized_pnl=?
            WHERE id=?
        """, (now_utc_iso(), reason, 0.0, position_id)))  # P&L calculated separately


def submit_exit_order(broker: BrokerAdapter, position: dict, reason: str) -> bool:
    """
    Submit market order to close position.
    Returns True if successful.
    """
    try:
        contract = position["contract"]
        qty = int(position["qty"])
        
        # For options, we sell to close
        resp = broker.place_order(
            symbol=position["underlying"],
            contract=contract,
            qty=qty,
            limit_price=None  # Market order
        )
        
        if resp.status in ("ACK", "FILLED"):
            audit("INFO", "EXIT_ORDER_SUBMITTED", {
                "position_id": position["id"],
                "contract": contract,
                "qty": qty,
                "reason": reason,
                "broker_order_id": resp.broker_order_id
            })
            return True
        else:
            audit("ERROR", "EXIT_ORDER_REJECTED", {
                "position_id": position["id"],
                "contract": contract,
                "reason": reason,
                "error": resp.error
            })
            return False
            
    except Exception as e:
        audit("ERROR", "EXIT_ORDER_EXCEPTION", {
            "position_id": position["id"],
            "error": str(e)
        })
        return False


def is_market_closing_soon() -> bool:
    """
    Check if market closes in next 15 minutes.
    Market hours: 9:30am - 4:00pm EST
    """
    now = datetime.now(timezone.utc)
    # Convert to EST (UTC-5)
    est_hour = (now.hour - 5) % 24
    est_minute = now.minute
    
    # Check if between 3:45pm - 4:00pm EST (15:45 - 16:00)
    if est_hour == 15 and est_minute >= 45:
        return True
    if est_hour >= 16:
        return True
    
    # Also check if before market open (before 9:30am EST)
    if est_hour < 9 or (est_hour == 9 and est_minute < 30):
        return True
    
    return False


def check_exit_conditions(position: dict, current_price: float) -> tuple[bool, str]:
    """
    Check if position should be exited.
    Returns (should_exit, reason)
    """
    avg_fill = float(position["avg_fill"])
    tp_pct = float(position["tp_pct"])
    sl_pct = float(position["sl_pct"])
    
    # Calculate target prices
    tp_price = avg_fill * (1.0 + tp_pct)
    sl_price = avg_fill * (1.0 - sl_pct)
    
    # Check TP
    if current_price >= tp_price:
        return (True, "TAKE_PROFIT")
    
    # Check SL
    if current_price <= sl_price:
        return (True, "STOP_LOSS")
    
    # Check EOD
    if is_market_closing_soon():
        return (True, "EOD_FLATTEN")
    
    return (False, "")


def exit_manager_loop(broker: BrokerAdapter, poll_seconds: float = 30.0):
    """
    Monitor open positions and exit when conditions met.
    Runs every 30 seconds.
    """
    log.info("Exit manager started")
    
    while True:
        try:
            # Check kill switch and mode
            state = load_state()
            if state.get("kill_switch") or state.get("mode") == "READ_ONLY":
                log.debug("Exit manager paused (kill switch or READ_ONLY mode)")
                time.sleep(poll_seconds)
                continue
            
            # Get all open positions
            positions = get_open_positions()
            
            if not positions:
                time.sleep(poll_seconds)
                continue
            
            log.debug(f"Monitoring {len(positions)} open positions")
            
            for pos in positions:
                try:
                    contract = pos["contract"]
                    
                    # Get current price
                    current_price = get_contract_price(broker, contract)
                    
                    if current_price <= 0:
                        log.warning(f"Invalid price for {contract}, skipping")
                        continue
                    
                    # Check exit conditions
                    should_exit, reason = check_exit_conditions(pos, current_price)
                    
                    if should_exit:
                        log.info(f"Exit triggered for {contract}: {reason} (price={current_price:.2f}, avg_fill={pos['avg_fill']:.2f})")
                        
                        # Submit exit order
                        if submit_exit_order(broker, pos, reason):
                            # Mark as closed (in real system, wait for fill confirmation)
                            close_position(pos["id"], current_price, reason)
                            
                            # Update realized P&L
                            pnl = (current_price - float(pos["avg_fill"])) * int(pos["qty"])
                            new_realized = float(state.get("realized_pnl_today", 0.0)) + pnl
                            update_state({"realized_pnl_today": new_realized})
                            
                            log.info(f"✅ Position closed: {contract} P&L=${pnl:.2f}")
                        else:
                            log.error(f"Failed to submit exit order for {contract}")
                    
                except Exception as e:
                    log.exception(f"Error processing position {pos.get('id')}: {e}")
                    continue
            
        except Exception as e:
            log.exception(f"Exit manager error: {e}")
        
        time.sleep(poll_seconds)
