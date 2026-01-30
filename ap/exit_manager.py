# ap/exit_manager.py - FIXED VERSION with Circuit Breaker
"""
Exit Manager with Circuit Breaker

KEY FIXES:
1. Circuit breaker - stops after 3 failed exit attempts
2. Checks if entry order actually filled before allowing exit
3. Rate limiting on exit attempts
4. Better error handling
"""
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from ap.db import conn, run_with_retry, insert_order, update_order, new_local_order_id
from ap.utils import now_utc_iso, json_dumps
from ap.logger import get_logger
from ap.state import load_state
from ap.contract_pricing import get_contract_price
from ap.broker import BrokerAdapter

log = get_logger("ap.exit")

NY = ZoneInfo("America/New_York")

# Circuit breaker settings
MAX_EXIT_ATTEMPTS = 3
MIN_EXIT_RETRY_SECONDS = 60  # Don't retry exit more than once per minute


def audit(level: str, event: str, payload: dict):
    with conn() as c:
        run_with_retry(lambda: c.execute(
            "INSERT INTO audit_log (ts, level, event, payload) VALUES (?,?,?,?)",
            (now_utc_iso(), level, event, json_dumps(payload)),
        ))


def get_open_positions():
    with conn() as c:
        rows = run_with_retry(lambda: c.execute("""
            SELECT id, underlying, contract, direction, qty, avg_fill,
                   tp_pct, sl_pct, entry_ts, status, client_id
            FROM positions
            WHERE status IN ('OPEN','CLOSING')
            ORDER BY entry_ts ASC
        """).fetchall())
        return [dict(r) for r in rows]


def mark_position_closing(position_id: str, reason: str):
    with conn() as c:
        run_with_retry(lambda: c.execute("""
            UPDATE positions
            SET status='CLOSING', exit_reason=?
            WHERE id=? AND status='OPEN'
        """, (reason, position_id)))


def mark_position_stuck(position_id: str, reason: str):
    """Mark a position as STUCK after multiple failed exit attempts"""
    with conn() as c:
        run_with_retry(lambda: c.execute("""
            UPDATE positions
            SET status='STUCK', exit_reason=?
            WHERE id=?
        """, (f"STUCK:{reason}", position_id)))
    
    audit("ERROR", "POSITION_STUCK", {
        "position_id": position_id,
        "reason": reason
    })
    
    log.error(f"🚨 Position {position_id} marked as STUCK: {reason}")


def revert_position_open(position_id: str):
    with conn() as c:
        run_with_retry(lambda: c.execute(
            "UPDATE positions SET status='OPEN', exit_reason=NULL WHERE id=?",
            (position_id,)
        ))


def get_failed_exit_count(position_id: str) -> int:
    """
    Count how many times we've tried (and failed) to exit this position
    Circuit breaker: stop trying after MAX_EXIT_ATTEMPTS failures
    """
    with conn() as c:
        row = run_with_retry(lambda: c.execute("""
            SELECT COUNT(*) as cnt
            FROM orders
            WHERE position_id=? 
              AND kind='EXIT' 
              AND status='REJECTED'
        """, (position_id,)).fetchone())
        
        return row["cnt"] if row else 0


def get_last_exit_attempt_time(position_id: str) -> datetime | None:
    """Get timestamp of last exit attempt to prevent spam"""
    with conn() as c:
        row = run_with_retry(lambda: c.execute("""
            SELECT MAX(created_ts) as last_attempt
            FROM orders
            WHERE position_id=? 
              AND kind='EXIT'
        """, (position_id,)).fetchone())
        
        if row and row["last_attempt"]:
            try:
                return datetime.fromisoformat(row["last_attempt"])
            except:
                return None
        
        return None


def has_pending_exit_order(position_id: str) -> bool:
    """
    Prevent double-submitting EXIT orders.
    If an EXIT order exists for this position and isn't final, skip.
    """
    with conn() as c:
        row = run_with_retry(lambda: c.execute(
            """
            SELECT 1
            FROM orders
            WHERE position_id=?
              AND kind='EXIT'
              AND status IN ('NEW','ACK','PARTIAL')
            LIMIT 1
            """,
            (position_id,)
        ).fetchone())
        return row is not None


def position_actually_exists_at_broker(position_id: str) -> bool:
    """
    CRITICAL CHECK: Verify the entry order actually filled.
    Don't try to exit positions that were never opened!
    """
    with conn() as c:
        row = run_with_retry(lambda: c.execute("""
            SELECT 1
            FROM orders
            WHERE position_id=?
              AND kind='ENTRY'
              AND status='FILLED'
            LIMIT 1
        """, (position_id,)).fetchone())
        
        return row is not None


def market_is_open_now() -> bool:
    """
    US equities: 9:30–16:00 America/New_York, Mon–Fri.
    (No holiday calendar here yet — beta OK.)
    """
    now_ny = datetime.now(timezone.utc).astimezone(NY)
    if now_ny.weekday() >= 5:
        return False
    h, m = now_ny.hour, now_ny.minute
    if (h < 9) or (h == 9 and m < 30):
        return False
    if h > 16 or (h == 16 and m > 0):
        return False
    return True


def market_closing_soon(minutes: int = 15) -> bool:
    now_ny = datetime.now(timezone.utc).astimezone(NY)
    if now_ny.weekday() >= 5:
        return True
    close_min = 16 * 60
    now_min = now_ny.hour * 60 + now_ny.minute
    return (close_min - now_min) <= minutes


def check_exit_conditions(position: dict, current_price: float) -> tuple[bool, str]:
    avg_fill = float(position["avg_fill"])
    tp_pct = float(position["tp_pct"])
    sl_pct = float(position["sl_pct"])

    tp_price = avg_fill * (1.0 + tp_pct)
    sl_price = avg_fill * (1.0 - sl_pct)

    if current_price >= tp_price:
        return True, "TAKE_PROFIT"

    if current_price <= sl_price:
        return True, "STOP_LOSS"

    if not market_is_open_now() or market_closing_soon(15):
        return True, "EOD_FLATTEN"

    return False, ""


def submit_exit_order(broker: BrokerAdapter, position: dict, reason: str) -> tuple[bool, str | None, str | None]:
    """
    Submit exit order (SELL_TO_CLOSE).
    Returns: (ok, broker_order_id, error)
    """
    contract = position["contract"]
    qty = int(position["qty"])

    try:
        resp = broker.place_order(
            symbol=position["underlying"],
            contract=contract,
            qty=qty,
            limit_price=None,
            side="sell_to_close"
        )

        broker_order_id = getattr(resp, "broker_order_id", None)
        status = getattr(resp, "status", None)
        error = getattr(resp, "error", None)

        if (status or "").upper() in ("ACK", "ACKED", "FILLED", "SUBMITTED"):
            audit("INFO", "EXIT_ORDER_SUBMITTED", {
                "position_id": position["id"],
                "contract": contract,
                "qty": qty,
                "reason": reason,
                "broker_order_id": broker_order_id
            })
            return True, broker_order_id, None

        audit("ERROR", "EXIT_ORDER_REJECTED", {
            "position_id": position["id"],
            "contract": contract,
            "qty": qty,
            "reason": reason,
            "broker_order_id": broker_order_id,
            "error": error or status
        })
        return False, broker_order_id, (error or status or "UNKNOWN_REJECT")

    except Exception as e:
        audit("ERROR", "EXIT_ORDER_EXCEPTION", {
            "position_id": position["id"],
            "contract": contract,
            "qty": qty,
            "reason": reason,
            "error": str(e)
        })
        return False, None, str(e)


def get_exit_price_safe(broker: BrokerAdapter, contract: str) -> float:
    """
    Compatibility helper: if get_contract_price() doesn't accept side=, fallback.
    """
    try:
        return float(get_contract_price(broker, contract, side="SELL"))
    except TypeError:
        return float(get_contract_price(broker, contract))


def exit_manager_loop(broker: BrokerAdapter, poll_seconds: float = 30.0):
    """
    Monitor OPEN positions and submit EXIT orders when conditions hit.

    IMPORTANT:
    - This file does NOT close positions.
    - Reconcile closes positions when the EXIT order is FILLED.
    
    NEW FEATURES:
    - Circuit breaker: stops after MAX_EXIT_ATTEMPTS failed exits
    - Rate limiting: won't retry exit within MIN_EXIT_RETRY_SECONDS
    - Validation: only exits positions that actually filled
    """
    log.info("Exit manager started (with circuit breaker)")

    while True:
        try:
            state = load_state()
            if state.get("kill_switch") or state.get("mode") == "READ_ONLY":
                time.sleep(poll_seconds)
                continue

            positions = get_open_positions()
            if not positions:
                time.sleep(poll_seconds)
                continue

            for pos in positions:
                # Skip if already closing (reconcile will finish it)
                if pos.get("status") == "CLOSING":
                    continue

                position_id = pos["id"]
                
                # CRITICAL CHECK: Does this position actually exist at broker?
                if not position_actually_exists_at_broker(position_id):
                    log.warning(
                        f"⚠️ Position {position_id} has no FILLED entry order, "
                        f"cannot exit (position was never opened at broker)"
                    )
                    # Don't mark as STUCK - fill monitor will handle this
                    continue

                # Circuit breaker: check failed attempt count
                failed_count = get_failed_exit_count(position_id)
                if failed_count >= MAX_EXIT_ATTEMPTS:
                    log.error(
                        f"🛑 Position {position_id} hit circuit breaker "
                        f"({failed_count} failed exits), marking as STUCK"
                    )
                    mark_position_stuck(position_id, f"EXIT_FAILED_{failed_count}x")
                    continue

                # Rate limiting: check last attempt time
                last_attempt = get_last_exit_attempt_time(position_id)
                if last_attempt:
                    elapsed = (datetime.now(timezone.utc) - last_attempt).total_seconds()
                    if elapsed < MIN_EXIT_RETRY_SECONDS:
                        log.debug(
                            f"Rate limiting: waiting {MIN_EXIT_RETRY_SECONDS - elapsed:.0f}s "
                            f"before next exit attempt for {position_id}"
                        )
                        continue

                # Extra guard: avoid duplicate exits
                if has_pending_exit_order(position_id):
                    continue

                contract = pos["contract"]

                # SELL side pricing for exits
                current_price = get_exit_price_safe(broker, contract)
                if current_price <= 0:
                    log.warning(f"Invalid price for {contract}, skipping")
                    continue

                should_exit, reason = check_exit_conditions(pos, current_price)
                if not should_exit:
                    continue

                log.info(
                    f"Exit triggered for {contract}: {reason} "
                    f"(price={current_price:.2f}, avg_fill={float(pos['avg_fill']):.2f})"
                )

                # Mark CLOSING immediately
                mark_position_closing(position_id, reason)

                # Persist EXIT order row
                local_order_id = new_local_order_id()
                insert_order(
                    local_order_id=local_order_id,
                    position_id=position_id,
                    kind="EXIT",
                    status="NEW",
                    symbol=pos["underlying"],
                    contract=contract,
                    qty=int(pos["qty"]),
                    limit_price=None
                )

                ok, broker_order_id, err = submit_exit_order(broker, pos, reason)

                if ok:
                    update_order(local_order_id, status="ACK", broker_order_id=broker_order_id)

                    log.info(
                        f"⏳ Exit order submitted: {contract} "
                        f"(waiting for fill) broker_order_id={broker_order_id}"
                    )

                    audit("INFO", "EXIT_ORDER_PENDING", {
                        "position_id": position_id,
                        "contract": contract,
                        "local_order_id": local_order_id,
                        "broker_order_id": broker_order_id,
                        "reason": reason
                    })
                else:
                    # Exit failed - revert to OPEN
                    revert_position_open(position_id)

                    update_order(local_order_id, status="REJECTED", broker_order_id=broker_order_id, last_error=err)
                    
                    # Log with attempt count
                    new_failed_count = failed_count + 1
                    log.error(
                        f"❌ Exit order failed for {contract}: {err} "
                        f"(attempt {new_failed_count}/{MAX_EXIT_ATTEMPTS})"
                    )

                    audit("ERROR", "EXIT_ORDER_FAILED", {
                        "position_id": position_id,
                        "contract": contract,
                        "error": err,
                        "failed_attempt_count": new_failed_count
                    })

        except Exception as e:
            log.exception(f"Exit manager error: {e}")

        time.sleep(poll_seconds)
