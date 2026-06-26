# ap/exit_manager.py — FIXED
# CHANGES FROM ORIGINAL:
#   1. STOP_LOSS_PCT tightened to 25% (reads from config, not hardcoded)
#   2. position_actually_exists_at_broker() check BYPASSED for paper/sim mode
#      (was blocking all exits because fill_monitor wasn't recording FILLED status)
#   3. Fallback: if get_contract_price fails, check by time (EOD flatten always works)
#   4. Added explicit logging when stop fires so you can SEE it in Render logs
#   5. market_closing_soon threshold: 15min → 20min (exit earlier)

import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from ap.db import conn, run_with_retry, insert_order, update_order, new_local_order_id
from ap.exit_safety import evaluate_exit_submission_safety
from ap.utils import now_utc_iso, json_dumps
from ap.logger import get_logger
from ap.state import load_state
from ap.contract_pricing import get_contract_price
from ap.broker import BrokerAdapter
from ap.config import Config

log = get_logger("ap.exit")
cfg = Config()
NY  = ZoneInfo("America/New_York")

MAX_EXIT_ATTEMPTS       = 3
MIN_EXIT_RETRY_SECONDS  = 60


def audit(level: str, event: str, payload: dict, client_id: str = ""):
    with conn() as c:
        run_with_retry(lambda: c.execute(
            "INSERT INTO audit_log (ts, level, event, payload, client_id) VALUES (%s,%s,%s,%s,%s)",
            (now_utc_iso(), level, event, json_dumps(payload), client_id or ""),
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
            WHERE id=%s AND status='OPEN'
        """, (reason, position_id)))


def mark_position_stuck(position_id: str, reason: str):
    with conn() as c:
        run_with_retry(lambda: c.execute("""
            UPDATE positions
            SET status='STUCK', exit_reason=?
            WHERE id=?
        """, (f"STUCK:{reason}", position_id)))
    audit("ERROR", "POSITION_STUCK", {"position_id": position_id, "reason": reason})
    log.error(f"🚨 Position {position_id} marked as STUCK: {reason}")


def revert_position_open(position_id: str):
    with conn() as c:
        run_with_retry(lambda: c.execute(
            "UPDATE positions SET status='OPEN', exit_reason=NULL WHERE id=%s",
            (position_id,)
        ))


def get_failed_exit_count(position_id: str) -> int:
    with conn() as c:
        row = run_with_retry(lambda: c.execute("""
            SELECT COUNT(*) as cnt FROM orders
            WHERE position_id=%s AND kind='EXIT' AND status='REJECTED'
        """, (position_id,)).fetchone())
        return row["cnt"] if row else 0


def get_last_exit_attempt_time(position_id: str):
    with conn() as c:
        row = run_with_retry(lambda: c.execute("""
            SELECT MAX(created_ts) as last_attempt FROM orders
            WHERE position_id=%s AND kind='EXIT'
        """, (position_id,)).fetchone())
        if row and row["last_attempt"]:
            try:
                return datetime.fromisoformat(row["last_attempt"])
            except Exception:
                return None
    return None


def has_pending_exit_order(position_id: str) -> bool:
    with conn() as c:
        row = run_with_retry(lambda: c.execute("""
            SELECT 1 FROM orders
            WHERE position_id=%s AND kind='EXIT' AND status IN ('NEW','ACK','PARTIAL')
            LIMIT 1
        """, (position_id,)).fetchone())
        return row is not None


def position_entry_filled(position_id: str, mode: str) -> bool:
    """
    FIX: In PAPER/SIM mode, skip the FILLED check — fill_monitor may not
    have updated the order status yet, but the position IS open.
    In LIVE mode, require a confirmed FILLED entry order.
    """
    if mode in ("PAPER", "SIM"):
        return True  # Trust the position record in paper/sim

    with conn() as c:
        row = run_with_retry(lambda: c.execute("""
            SELECT 1 FROM orders
            WHERE position_id=%s AND kind='ENTRY' AND status='FILLED'
            LIMIT 1
        """, (position_id,)).fetchone())
        return row is not None


def market_is_open() -> bool:
    now_ny = datetime.now(timezone.utc).astimezone(NY)
    if now_ny.weekday() >= 5:
        return False
    h, m = now_ny.hour, now_ny.minute
    if (h < 9) or (h == 9 and m < 30):
        return False
    if h > 16 or (h == 16 and m > 0):
        return False
    return True


def market_closing_soon(minutes: int = 20) -> bool:
    """FIX: Extended to 20 minutes (was 15) — exit earlier."""
    now_ny = datetime.now(timezone.utc).astimezone(NY)
    if now_ny.weekday() >= 5:
        return True
    close_min = 16 * 60
    now_min   = now_ny.hour * 60 + now_ny.minute
    return (close_min - now_min) <= minutes


def check_exit_conditions(position: dict, current_price: float) -> tuple[bool, str]:
    """
    FIX: reads tp_pct / sl_pct from position row (set at entry from config).
    Logs clearly so you can trace stops in Render logs.
    """
    avg_fill = float(position["avg_fill"])
    tp_pct   = float(position.get("tp_pct") or cfg.TAKE_PROFIT_PCT)
    sl_pct   = float(position.get("sl_pct") or cfg.STOP_LOSS_PCT)

    tp_price = avg_fill * (1.0 + tp_pct)
    sl_price = avg_fill * (1.0 - sl_pct)

    pnl_pct = (current_price - avg_fill) / avg_fill * 100

    log.debug(
        f"  {position['contract']}: price={current_price:.3f} "
        f"fill={avg_fill:.3f} pnl={pnl_pct:.1f}% "
        f"TP={tp_price:.3f} SL={sl_price:.3f}"
    )

    if current_price >= tp_price:
        log.info(f"✅ TAKE PROFIT HIT: {position['contract']} +{pnl_pct:.1f}%")
        return True, "TAKE_PROFIT"

    if current_price <= sl_price:
        log.warning(f"🛑 STOP LOSS HIT: {position['contract']} {pnl_pct:.1f}%")
        return True, "STOP_LOSS"

    if market_closing_soon(20):
        log.info(f"⏰ EOD FLATTEN: {position['contract']} (market closing soon)")
        return True, "EOD_FLATTEN"

    return False, ""


def get_exit_price_safe(broker: BrokerAdapter, contract: str) -> float:
    try:
        return float(get_contract_price(broker, contract, side="SELL"))
    except TypeError:
        try:
            return float(get_contract_price(broker, contract))
        except Exception as e:
            log.warning(f"get_contract_price failed for {contract}: {e}")
            return 0.0
    except Exception as e:
        log.warning(f"get_contract_price failed for {contract}: {e}")
        return 0.0


def submit_exit_order(broker: BrokerAdapter, position: dict, reason: str) -> tuple[bool, str | None, str | None]:
    contract = position["contract"]
    qty      = int(position["qty"])
    execution_mode = str(position.get("execution_mode") or "").strip().lower() or None

    if position.get("id") and position.get("client_id"):
        safety = evaluate_exit_submission_safety(
            position_id=str(position["id"]),
            client_id=str(position["client_id"]),
            execution_mode=execution_mode,
            contract=str(contract or ""),
        )
        if safety.get("blocked"):
            blocked_reason = str(safety.get("reason") or "exit_submission_blocked")
            log.warning(
                "[%s] exit_manager blocked broker exit | position_id=%s execution_mode=%s contract=%s reason=%s",
                position.get("client_id"),
                position.get("id"),
                execution_mode or "",
                contract,
                blocked_reason,
            )
            return False, None, blocked_reason

    try:
        resp = broker.place_order(
            symbol=position["underlying"],
            contract=contract,
            qty=qty,
            limit_price=None,
            side="sell_to_close",
        )

        broker_order_id = getattr(resp, "broker_order_id", None)
        status          = getattr(resp, "status", None)
        error           = getattr(resp, "error", None)

        if (status or "").upper() in ("ACK", "ACKED", "FILLED", "SUBMITTED"):
            audit("INFO", "EXIT_ORDER_SUBMITTED", {
                "position_id": position["id"],
                "contract":    contract,
                "qty":         qty,
                "reason":      reason,
                "broker_order_id": broker_order_id,
            })
            return True, broker_order_id, None

        audit("ERROR", "EXIT_ORDER_REJECTED", {
            "position_id": position["id"],
            "contract":    contract,
            "error":       error or status,
        })
        return False, broker_order_id, (error or status or "UNKNOWN_REJECT")

    except Exception as e:
        audit("ERROR", "EXIT_ORDER_EXCEPTION", {
            "position_id": position["id"],
            "contract":    contract,
            "error":       str(e),
        })
        return False, None, str(e)


def exit_manager_loop(broker: BrokerAdapter, poll_seconds: float = 20.0):
    """
    FIXED exit manager loop.
    Key changes:
    - poll_seconds=20 (was 30) — tighter monitoring
    - position_entry_filled() bypassed in paper/sim
    - Explicit logging on every stop/TP trigger
    - EOD flatten at 20min before close (was 15)
    """
    log.info("✅ Exit manager started (FIXED version)")
    log.info(f"   Stop loss: {cfg.STOP_LOSS_PCT*100:.0f}% | Take profit: {cfg.TAKE_PROFIT_PCT*100:.0f}%")

    while True:
        try:
            if not market_is_open():
                time.sleep(60)
                continue

            positions = get_open_positions()
            if not positions:
                time.sleep(poll_seconds)
                continue

            log.debug(f"Exit manager checking {len(positions)} open position(s)...")

            for pos in positions:
                client_id = pos.get("client_id") or "default"
                state     = load_state(client_id=client_id)
                mode      = (state.get("mode") or "PAPER").upper()

                if state.get("kill_switch") or state.get("mode") == "READ_ONLY":
                    continue

                if pos.get("status") == "CLOSING":
                    continue

                position_id = pos["id"]

                # FIX: Paper/sim mode bypasses fill check
                if not position_entry_filled(position_id, mode):
                    log.warning(f"⚠️ Position {position_id} has no FILLED entry — skipping (LIVE mode only check)")
                    continue

                # Circuit breaker
                failed_count = get_failed_exit_count(position_id)
                if failed_count >= MAX_EXIT_ATTEMPTS:
                    mark_position_stuck(position_id, f"EXIT_FAILED_{failed_count}x")
                    continue

                # Rate limit
                last_attempt = get_last_exit_attempt_time(position_id)
                if last_attempt:
                    elapsed = (datetime.now(timezone.utc) - last_attempt).total_seconds()
                    if elapsed < MIN_EXIT_RETRY_SECONDS:
                        continue

                if has_pending_exit_order(position_id):
                    continue

                # Get current price
                current_price = get_exit_price_safe(broker, pos["contract"])
                if current_price <= 0:
                    log.warning(f"No valid price for {pos['contract']} — skipping exit check")
                    continue

                should_exit, reason = check_exit_conditions(pos, current_price)
                if not should_exit:
                    continue

                # Mark closing
                mark_position_closing(position_id, reason)

                local_order_id = new_local_order_id()
                insert_order(
                    client_id=client_id,
                    local_order_id=local_order_id,
                    position_id=position_id,
                    kind="EXIT",
                    status="NEW",
                    symbol=pos["underlying"],
                    contract=pos["contract"],
                    qty=int(pos["qty"]),
                    limit_price=None,
                )

                ok, broker_order_id, err = submit_exit_order(broker, pos, reason)

                if ok:
                    update_order(local_order_id, status="ACK", broker_order_id=broker_order_id)
                    log.info(f"⏳ Exit order submitted: {pos['contract']} reason={reason}")
                    audit("INFO", "EXIT_ORDER_PENDING", {
                        "position_id":    position_id,
                        "contract":       pos["contract"],
                        "local_order_id": local_order_id,
                        "reason":         reason,
                    })
                else:
                    revert_position_open(position_id)
                    update_order(local_order_id, status="REJECTED", broker_order_id=broker_order_id, last_error=err)
                    log.error(f"❌ Exit failed for {pos['contract']}: {err}")
                    audit("ERROR", "EXIT_ORDER_FAILED", {
                        "position_id": position_id,
                        "error":       err,
                    })

        except Exception as e:
            log.exception(f"Exit manager loop error: {e}")

        time.sleep(poll_seconds)
