# ap/exit_manager.py
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from ap.db import conn, run_with_retry, insert_order, update_order, new_local_order_id
from ap.utils import now_utc_iso, json_dumps
from ap.logger import get_logger
from ap.state import load_state, update_state
from ap.contract_pricing import get_contract_price
from ap.broker import BrokerAdapter

log = get_logger("ap.exit")

NY = ZoneInfo("America/New_York")
OPT_MULTIPLIER = 100  # standard US equity options

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
                   tp_pct, sl_pct, entry_ts, status
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

def close_position(position_id: str, exit_price: float, reason: str, realized_pnl: float):
    with conn() as c:
        run_with_retry(lambda: c.execute("""
            UPDATE positions
            SET status='CLOSED', exit_ts=?, exit_reason=?, realized_pnl=?
            WHERE id=?
        """, (now_utc_iso(), reason, realized_pnl, position_id)))

def market_is_open_now() -> bool:
    """
    US equities: 9:30–16:00 America/New_York, Mon–Fri.
    (No holiday calendar here yet — beta OK.)
    """
    now_ny = datetime.now(timezone.utc).astimezone(NY)
    if now_ny.weekday() >= 5:
        return False
    h, m = now_ny.hour, now_ny.minute
    # before 9:30
    if (h < 9) or (h == 9 and m < 30):
        return False
    # after 16:00
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

    # Flatten if market is closed or near close
    if not market_is_open_now() or market_closing_soon(15):
        return True, "EOD_FLATTEN"

    return False, ""

def submit_exit_order(broker: BrokerAdapter, position: dict, reason: str) -> tuple[bool, str | None, str | None]:
    """
    Submit exit order.
    Returns: (ok, broker_order_id, error)
    """
    contract = position["contract"]
    qty = int(position["qty"])

    # NOTE: Your BrokerAdapter MUST support selling to close.
    # If your adapter needs an explicit side/action, change here to:
    # action="SELL_TO_CLOSE"
    try:
        resp = broker.place_order(
            symbol=position["underlying"],
            contract=contract,
            qty=qty,
            limit_price=None
        )

        # Support either dict or response object
        broker_order_id = None
        status = None
        error = None

        if isinstance(resp, dict):
            broker_order_id = resp.get("order_id") or resp.get("id") or resp.get("broker_order_id")
            status = resp.get("status") or resp.get("state")
            error = resp.get("error")
        else:
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

def exit_manager_loop(broker: BrokerAdapter, poll_seconds: float = 30.0):
    """
    Monitor open positions and exit when conditions met.
    """
    log.info("Exit manager started")

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
                # Skip if already closing (until you add fill confirmation)
                if pos.get("status") == "CLOSING":
                    continue

                contract = pos["contract"]

                # SELL side pricing for exits
                current_price = get_contract_price(broker, contract, side="SELL")
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

                # mark CLOSING immediately for truth
                mark_position_closing(pos["id"], reason)

                # persist EXIT order row
                local_order_id = new_local_order_id()
                insert_order(
                    local_order_id=local_order_id,
                    position_id=pos["id"],
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

                    # Beta simplification: assume filled near bid price.
                    # Later: confirm fill and use actual fill price.
                    entry = float(pos["avg_fill"])
                    qty = int(pos["qty"])
                    realized = (current_price - entry) * qty * OPT_MULTIPLIER

                    # close immediately (beta). If you want safer: leave CLOSING until fill confirm.
                    close_position(pos["id"], current_price, reason, realized)

                    new_realized = float(state.get("realized_pnl_today", 0.0)) + realized
                    update_state({"realized_pnl_today": new_realized})

                    log.info(f"✅ Position closed: {contract} realized=${realized:.2f}")
                else:
                    update_order(local_order_id, status="REJECTED", broker_order_id=broker_order_id, last_error=err)
                    log.error(f"Exit order failed for {contract}: {err}")

        except Exception as e:
            log.exception(f"Exit manager error: {e}")

        time.sleep(poll_seconds)

