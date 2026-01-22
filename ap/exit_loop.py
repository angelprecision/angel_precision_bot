import time
from ap.db import conn
from ap.utils import now_utc_iso, json_dumps
from ap.logger import get_logger
from ap.state import load_state, update_state

log = get_logger("ap.exit")

def audit(level: str, event: str, payload: dict):
    with conn() as c:
        c.execute(
            "INSERT INTO audit_log (ts, level, event, payload) VALUES (?,?,?,?)",
            (now_utc_iso(), level, event, json_dumps(payload)),
        )

def list_open_positions():
    with conn() as c:
        rows = c.execute("""
            SELECT id, underlying, contract, direction, qty, avg_fill, tp_pct, sl_pct, entry_ts
            FROM positions
            WHERE status='OPEN'
            ORDER BY entry_ts ASC
        """).fetchall()
        return [dict(r) for r in rows]

def close_position_db(position_id: str, exit_price: float, reason: str):
    with conn() as c:
        c.execute("""
            UPDATE positions
            SET status='CLOSED'
            WHERE id=?
        """, (position_id,))
        c.execute("""
            INSERT INTO fills (position_id, ts, fill_price, reason)
            VALUES (?,?,?,?)
        """, (position_id, now_utc_iso(), exit_price, reason))

def exit_manager_loop(broker, poll_seconds: float = 1.0):
    """
    Closes OPEN positions when TP/SL hit.
    Assumes LONG options (you can extend later).
    TP/SL thresholds are based on avg_fill premium.
    """
    log.info("Exit loop started")
    while True:
        st = load_state()

        # Don't manage exits if kill switch? (up to you)
        if st.get("kill_switch"):
            time.sleep(poll_seconds)
            continue

        positions = list_open_positions()
        if not positions:
            time.sleep(poll_seconds)
            continue

        for p in positions:
            contract = p["contract"]
            qty = int(p["qty"])
            avg_fill = float(p["avg_fill"])
            tp_pct = float(p["tp_pct"])
            sl_pct = float(p["sl_pct"])

            tp_price = avg_fill * (1.0 + tp_pct)
            sl_price = avg_fill * (1.0 - sl_pct)

            try:
                last = float(broker.get_contract_last(contract))  # <— add to BrokerAdapter + brokers
            except Exception as e:
                audit("ERROR", "QUOTE_FAIL", {"contract": contract, "err": str(e)})
                continue

            hit_tp = last >= tp_price
            hit_sl = last <= sl_price

            if not (hit_tp or hit_sl):
                continue

            reason = "TP" if hit_tp else "SL"

            try:
                fill = broker.close_contract_market(contract, qty)  # <— add to BrokerAdapter + brokers
                exit_px = float(fill.avg_fill_price) if hasattr(fill, "avg_fill_price") else float(fill["avg_fill_price"])
            except Exception as e:
                audit("ERROR", "CLOSE_FAIL", {"pos_id": p["id"], "contract": contract, "err": str(e)})
                continue

            close_position_db(p["id"], exit_px, reason)
            audit("INFO", "POSITION_CLOSED", {"pos_id": p["id"], "exit": exit_px, "reason": reason})

            # Update state PnL (realized)
            pnl = (exit_px - avg_fill) * qty
            new_realized = float(st.get("realized_pnl_today", 0.0)) + pnl
            new_equity = float(st.get("current_equity_last", 0.0)) + pnl

            update_state({
                "realized_pnl_today": new_realized,
                "current_equity_last": new_equity,
                "last_exit_ts_iso": now_utc_iso(),
            })

        time.sleep(poll_seconds)
