# app.py - Angel Precision Bot (Render + gunicorn safe)

import os
import threading
import uuid
from datetime import datetime, timezone

from flask import Flask, request, jsonify
from pydantic import ValidationError

from ap.config import Config
from ap.db import init_db, conn
from ap.logger import get_logger
from ap.models import Signal
from ap.queue import enqueue_signal, worker_loop
from ap.state import load_state, update_state
from ap.broker import SimBroker

# Scanner + Tradier
from ap.parsers import parse_scanner_text
from ap.brokers.tradier import TradierBroker, TradierConfig

# Exit Manager
from ap.exit_manager import exit_manager_loop

cfg = Config()
log = get_logger("app")

app = Flask(__name__)

# -------------------------
# Thread safety: start once
# -------------------------
THREADS_STARTED = False
THREAD_LOCK = threading.Lock()

# -------------------------
# Broker init
# -------------------------
def build_broker():
    if cfg.BOT_MODE in ("PAPER", "LIVE"):
        base_url = os.getenv("TRADIER_BASE_URL", "https://sandbox.tradier.com")
        access_token = os.getenv("TRADIER_ACCESS_TOKEN", "").strip()
        account_id = os.getenv("TRADIER_ACCOUNT_ID", "").strip()

        if not access_token or not account_id:
            raise RuntimeError("Missing TRADIER_ACCESS_TOKEN or TRADIER_ACCOUNT_ID")

        log.info(f"Initializing Tradier broker in {cfg.BOT_MODE} mode")
        b = TradierBroker(TradierConfig(
            base_url=base_url,
            access_token=access_token,
            account_id=account_id,
        ))
        log.info("Tradier broker initialized")
        return b

    log.info(f"Initializing Simulator broker in {cfg.BOT_MODE} mode")
    b = SimBroker(starting_equity=10000.0)
    log.info("Simulator broker initialized")
    return b


BROKER = build_broker()


# =========================
# ROOT / HEALTH / STATE
# =========================

@app.get("/")
def root():
    return jsonify({
        "service": "Angel Precision Bot",
        "status": "online",
        "endpoints": {
            "health": "/health",
            "state": "/state",
            "signal_json": "/signal",
            "signal_discord": "/scanner/discord",
            "dashboard": "/dashboard",
            "reset_equity": "/reset_equity",
            "reports": {
                "orders": "/report/orders",
                "positions": "/report/positions",
                "audit": "/report/audit",
                "summary": "/report/summary"
            },
            "debug": {
                "order": "/debug/order/<order_id>",
                "test_signal": "POST /debug/test_signal"
            },
            "monitor": {
                "positions": "/monitor/positions"
            },
            "control": {
                "force_exit": "POST /control/force_exit/<position_id>",
                "flatten_all": "POST /control/flatten_all"
            }
        }
    })


@app.get("/health")
def health():
    try:
        st = load_state()

        heartbeat_ok = False
        heartbeat_age = None
        if st.get("last_heartbeat_ts"):
            try:
                hb_time = datetime.fromisoformat(st["last_heartbeat_ts"])
                heartbeat_age = (datetime.now(timezone.utc) - hb_time).total_seconds()
                heartbeat_ok = heartbeat_age < 120
            except Exception:
                pass

        all_ok = heartbeat_ok or heartbeat_age is None

        return jsonify({
            "ok": all_ok,
            "status": "healthy" if all_ok else "degraded",
            "mode": st.get("mode", "UNKNOWN"),
            "kill_switch": st.get("kill_switch", False),
            "heartbeat": st.get("last_heartbeat_ts"),
            "heartbeat_age_seconds": heartbeat_age,
            "worker_alive": heartbeat_ok
        }), 200 if all_ok else 503

    except Exception as e:
        log.error(f"Health check failed: {e}")
        return jsonify({"ok": False, "status": "error", "error": str(e)}), 503


@app.get("/state")
def state():
    try:
        return jsonify(load_state())
    except Exception as e:
        log.error(f"State retrieval failed: {e}")
        return jsonify({"error": str(e)}), 500


@app.post("/kill_switch/on")
def kill_on():
    log.warning("🔴 KILL SWITCH ENABLED")
    update_state({"kill_switch": True, "mode": "READ_ONLY"})
    return jsonify({"ok": True, "kill_switch": True, "mode": "READ_ONLY"})


@app.post("/kill_switch/off")
def kill_off():
    log.info("🟢 KILL SWITCH DISABLED")
    update_state({"kill_switch": False})
    return jsonify({"ok": True, "kill_switch": False})


@app.post("/mode")
def set_mode():
    body = request.get_json(force=True) or {}
    mode = str(body.get("mode", "")).upper()
    if mode not in ("SIM", "PAPER", "LIVE", "READ_ONLY"):
        return jsonify({"ok": False, "error": "Invalid mode"}), 400
    log.info(f"Mode changed to: {mode}")
    update_state({"mode": mode})
    return jsonify({"ok": True, "mode": mode})


# =========================
# SIGNAL INGESTION
# =========================

@app.post("/signal")
def signal():
    body = request.get_json(force=True) or {}
    try:
        sig = Signal(**body)
    except ValidationError as e:
        log.warning(f"Invalid signal payload: {e.errors()}")
        return jsonify({
            "ok": False,
            "error": "Invalid signal payload",
            "details": e.errors(),
            "example": {
                "signal_id": "uuid-string",
                "symbol": "SPY",
                "direction": "CALL",
                "pattern_id": "MANUAL_TEST",
                "timestamp_iso": "2026-01-22T00:00:00Z",
                "trigger": {"strike": 475.0, "expiry_hint": "Weekly"}
            }
        }), 400

    enqueue_signal(sig)
    log.info(f"Signal queued: {sig.symbol} {sig.direction}")
    return jsonify({"ok": True, "queued": True, "signal_id": sig.signal_id})


@app.post("/scanner/discord")
def scanner_discord():
    body = request.get_json(silent=True) or {}
    if not body:
        return jsonify({"ok": False, "error": "Expected JSON body", "example": {"content": "paste scanner text here"}}), 400

    text = body.get("content") or ""
    if not text:
        return jsonify({"ok": False, "error": "No content provided"}), 400

    if len(text) > 20000:
        return jsonify({"ok": False, "error": "content too large"}), 413

    try:
        parsed = parse_scanner_text(text)
        queued = 0

        for msg in parsed:
            now_iso = datetime.now(timezone.utc).isoformat()

            if msg.calls and msg.calls.strike:
                sig = Signal(
                    signal_id=str(uuid.uuid4()),
                    symbol=msg.symbol,
                    direction="CALL",
                    pattern_id="SCANNER_V1",
                    confidence_tag="standard_pool",
                    timestamp_iso=now_iso,
                    trigger={
                        "source": "discord",
                        "scanned_at": msg.scanned_at,
                        "current": msg.current,
                        "entry": msg.calls.entry,
                        "stop": msg.calls.stop,
                        "pt1": msg.calls.pt1,
                        "pt2": msg.calls.pt2,
                        "pt3": msg.calls.pt3,
                        "strike": msg.calls.strike,
                        "expiry_hint": msg.calls.expiry_hint,
                        "raw_strike": msg.calls.raw_strike_line
                    }
                )
                enqueue_signal(sig)
                queued += 1
                log.info(f"Queued CALL: {msg.symbol} strike={msg.calls.strike}")

            if msg.puts and msg.puts.strike:
                sig = Signal(
                    signal_id=str(uuid.uuid4()),
                    symbol=msg.symbol,
                    direction="PUT",
                    pattern_id="SCANNER_V1",
                    confidence_tag="standard_pool",
                    timestamp_iso=now_iso,
                    trigger={
                        "source": "discord",
                        "scanned_at": msg.scanned_at,
                        "current": msg.current,
                        "entry": msg.puts.entry,
                        "stop": msg.puts.stop,
                        "pt1": msg.puts.pt1,
                        "pt2": msg.puts.pt2,
                        "pt3": msg.puts.pt3,
                        "strike": msg.puts.strike,
                        "expiry_hint": msg.puts.expiry_hint,
                        "raw_strike": msg.puts.raw_strike_line
                    }
                )
                enqueue_signal(sig)
                queued += 1
                log.info(f"Queued PUT: {msg.symbol} strike={msg.puts.strike}")

        log.info(f"Parsed {len(parsed)} setups, queued {queued} signals")
        return jsonify({"ok": True, "parsed": len(parsed), "queued": queued})

    except Exception as e:
        log.error(f"Failed to parse scanner message: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


# =========================
# UTIL
# =========================

@app.get("/tradier/test")
def tradier_test():
    if cfg.BOT_MODE not in ("PAPER", "LIVE"):
        return jsonify({"ok": False, "error": "Only available in PAPER/LIVE mode"}), 400
    try:
        equity = BROKER.get_account_equity()
        log.info(f"Tradier test successful: equity=${equity}")
        return jsonify({"ok": True, "equity": equity})
    except Exception as e:
        log.error(f"Tradier test failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/dashboard")
def dashboard():
    try:
        st = load_state()
        with conn() as c:
            pos_row = c.execute("SELECT COUNT(*) as n FROM positions WHERE status='OPEN'").fetchone()
            open_positions = int(pos_row["n"]) if pos_row else 0

            queue_row = c.execute("SELECT COUNT(*) as n FROM trade_queue WHERE status='NEW'").fetchone()
            pending_signals = int(queue_row["n"]) if queue_row else 0

        return jsonify({
            "status": "healthy" if not st.get("kill_switch") else "stopped",
            "mode": st.get("mode"),
            "kill_switch": st.get("kill_switch"),
            "stats": {
                "open_positions": open_positions,
                "pending_signals": pending_signals,
                "trades_today": st.get("trades_taken_today", 0),
                "current_equity": st.get("current_equity_last", 0)
            }
        })
    except Exception as e:
        log.error(f"Dashboard error: {e}")
        return jsonify({"error": str(e)}), 500


@app.post("/reset_equity")
def reset_equity():
    try:
        equity = BROKER.get_account_equity()
        update_state({
            "initial_equity_run": equity,
            "starting_equity_today": equity,
            "current_equity_last": equity,
            "realized_pnl_today": 0.0,
            "trades_taken_today": 0,
            "daily_stop_hit": False
        })
        log.info(f"🔄 Equity reset to ${equity}")
        return jsonify({"ok": True, "equity": equity, "message": "Equity reset successful"})
    except Exception as e:
        log.error(f"Equity reset failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


# =========================
# REPORTING
# =========================

@app.get("/report/orders")
def report_orders():
    limit = int(request.args.get("limit", "200"))
    status = request.args.get("status")
    with conn() as c:
        if status:
            rows = c.execute(
                "SELECT * FROM orders WHERE status=? ORDER BY created_ts DESC LIMIT ?",
                (status, limit)
            ).fetchall()
        else:
            rows = c.execute(
                "SELECT * FROM orders ORDER BY created_ts DESC LIMIT ?",
                (limit,)
            ).fetchall()
    return jsonify({"ok": True, "count": len(rows), "orders": [dict(r) for r in rows]})


@app.get("/report/positions")
def report_positions():
    status = str(request.args.get("status", "OPEN")).upper()
    limit = int(request.args.get("limit", "200"))
    if status not in ("ALL", "OPEN", "CLOSING", "CLOSED"):
        return jsonify({"ok": False, "error": "status must be ALL|OPEN|CLOSING|CLOSED"}), 400

    with conn() as c:
        if status == "ALL":
            rows = c.execute(
                "SELECT * FROM positions ORDER BY entry_ts DESC LIMIT ?",
                (limit,)
            ).fetchall()
        else:
            rows = c.execute(
                "SELECT * FROM positions WHERE status=? ORDER BY entry_ts DESC LIMIT ?",
                (status, limit)
            ).fetchall()

    return jsonify({"ok": True, "count": len(rows), "positions": [dict(r) for r in rows]})


@app.get("/report/audit")
def report_audit():
    limit = int(request.args.get("limit", "200"))
    level = request.args.get("level")
    event = request.args.get("event")

    sql = "SELECT * FROM audit_log WHERE 1=1"
    params = []

    if level:
        sql += " AND level=?"
        params.append(level)
    if event:
        sql += " AND event=?"
        params.append(event)

    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)

    with conn() as c:
        rows = c.execute(sql, params).fetchall()

    return jsonify({"ok": True, "count": len(rows), "events": [dict(r) for r in rows]})


@app.get("/report/summary")
def report_summary():
    try:
        with conn() as c:
            order_stats = c.execute("SELECT status, COUNT(*) as count FROM orders GROUP BY status").fetchall()
            pos_stats = c.execute("SELECT status, COUNT(*) as count FROM positions GROUP BY status").fetchall()
            queue_stats = c.execute("SELECT status, COUNT(*) as count FROM trade_queue GROUP BY status").fetchall()

            errors = c.execute("""
                SELECT event, COUNT(*) as count
                FROM audit_log
                WHERE level='ERROR'
                  AND ts > datetime('now', '-1 day')
                GROUP BY event
            """).fetchall()

        state = load_state()

        return jsonify({
            "ok": True,
            "state": state,
            "orders": {row["status"]: row["count"] for row in order_stats},
            "positions": {row["status"]: row["count"] for row in pos_stats},
            "queue": {row["status"]: row["count"] for row in queue_stats},
            "recent_errors": {row["event"]: row["count"] for row in errors}
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# =========================
# DEBUG & CONTROL
# =========================

@app.get("/debug/order/<order_id>")
def debug_order(order_id):
    """Raw Tradier order data for debugging"""
    try:
        return jsonify({"ok": True, "order": BROKER.get_order(order_id)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/debug/test_signal")
def debug_test_signal():
    """Submit manual test signal for testing"""
    body = request.get_json(force=True) or {}
    
    symbol = body.get("symbol", "SPY")
    direction = body.get("direction", "CALL")
    strike = body.get("strike")
    expiry_hint = body.get("expiry_hint", "Weekly")
    
    if not strike:
        return jsonify({
            "ok": False,
            "error": "Missing strike price",
            "example": {
                "symbol": "AAPL",
                "direction": "CALL",
                "strike": 240,
                "expiry_hint": "Weekly"
            }
        }), 400
    
    try:
        sig = Signal(
            signal_id=str(uuid.uuid4()),
            symbol=symbol,
            direction=direction.upper(),
            pattern_id="MANUAL_TEST",
            confidence_tag="test",
            timestamp_iso=datetime.now(timezone.utc).isoformat(),
            trigger={
                "source": "manual_test",
                "strike": float(strike),
                "expiry_hint": expiry_hint,
                "entry": 0,
                "stop": 0,
                "pt1": 0,
                "pt2": 0,
                "pt3": 0
            }
        )
        
        enqueue_signal(sig)
        log.info(f"Manual test signal queued: {symbol} {direction} {strike}")
        
        return jsonify({
            "ok": True,
            "message": "Test signal queued",
            "signal_id": sig.signal_id,
            "symbol": symbol,
            "direction": direction,
            "strike": strike
        })
        
    except Exception as e:
        log.error(f"Test signal failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/monitor/positions")
def monitor_positions():
    """Real-time position monitoring with current prices and P&L"""
    try:
        from ap.contract_pricing import get_contract_price
        
        # Get all open/closing positions
        with conn() as c:
            rows = c.execute("""
                SELECT * FROM positions 
                WHERE status IN ('OPEN', 'CLOSING')
                ORDER BY entry_ts DESC
            """).fetchall()
        
        positions = []
        
        for row in rows:
            pos = dict(row)
            
            try:
                # Get current market price
                current_price = get_contract_price(BROKER, pos["contract"], side="SELL")
                
                # Calculate unrealized P&L
                entry_price = float(pos["avg_fill"])
                qty = int(pos["qty"])
                unrealized_pnl = (current_price - entry_price) * qty * 100
                
                # Calculate TP/SL levels
                tp_price = entry_price * (1.0 + float(pos["tp_pct"]))
                sl_price = entry_price * (1.0 - float(pos["sl_pct"]))
                
                # Distance to targets
                to_tp_pct = ((tp_price - current_price) / current_price) * 100 if current_price > 0 else 0
                to_sl_pct = ((current_price - sl_price) / current_price) * 100 if current_price > 0 else 0
                
                positions.append({
                    "position_id": pos["id"],
                    "contract": pos["contract"],
                    "status": pos["status"],
                    "qty": qty,
                    "entry_price": entry_price,
                    "current_price": current_price,
                    "unrealized_pnl": unrealized_pnl,
                    "tp_price": tp_price,
                    "sl_price": sl_price,
                    "to_tp_pct": to_tp_pct,
                    "to_sl_pct": to_sl_pct,
                    "entry_ts": pos["entry_ts"],
                    "exit_reason": pos.get("exit_reason")
                })
                
            except Exception as e:
                log.error(f"Failed to get price for {pos['contract']}: {e}")
                positions.append({
                    "position_id": pos["id"],
                    "contract": pos["contract"],
                    "status": pos["status"],
                    "error": "Price unavailable"
                })
        
        # Get state
        state = load_state()
        
        return jsonify({
            "ok": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "mode": state.get("mode"),
            "kill_switch": state.get("kill_switch"),
            "positions": positions,
            "total_unrealized_pnl": sum(p.get("unrealized_pnl", 0) for p in positions),
            "realized_pnl_today": state.get("realized_pnl_today", 0)
        })
        
    except Exception as e:
        log.error(f"Position monitor failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/control/force_exit/<position_id>")
def force_exit_position(position_id: str):
    """Manually force exit a position"""
    try:
        # Get position
        with conn() as c:
            pos_row = c.execute(
                "SELECT * FROM positions WHERE id=? AND status IN ('OPEN', 'CLOSING')",
                (position_id,)
            ).fetchone()
        
        if not pos_row:
            return jsonify({
                "ok": False,
                "error": f"Position {position_id} not found or already closed"
            }), 404
        
        pos = dict(pos_row)
        
        # Mark as CLOSING
        with conn() as c:
            c.execute(
                "UPDATE positions SET status='CLOSING', exit_reason='MANUAL_EXIT' WHERE id=?",
                (position_id,)
            )
        
        # Create exit order
        from ap.db import insert_order, update_order, new_local_order_id
        local_order_id = new_local_order_id()
        
        insert_order(
            local_order_id=local_order_id,
            position_id=position_id,
            kind="EXIT",
            status="NEW",
            symbol=pos["underlying"],
            contract=pos["contract"],
            qty=int(pos["qty"]),
            limit_price=None
        )
        
        # Submit exit order
        resp = BROKER.place_order(
            symbol=pos["underlying"],
            contract=pos["contract"],
            qty=int(pos["qty"]),
            limit_price=None,
            side="sell_to_close"
        )
        
        broker_order_id = getattr(resp, "broker_order_id", None)
        status = getattr(resp, "status", None)
        
        # Update order
        update_order(local_order_id, status=status, broker_order_id=broker_order_id)
        
        log.warning(f"🚨 MANUAL EXIT: {pos['contract']} by user request")
        
        return jsonify({
            "ok": True,
            "message": "Manual exit order submitted",
            "position_id": position_id,
            "contract": pos["contract"],
            "local_order_id": local_order_id,
            "broker_order_id": broker_order_id,
            "status": status
        })
        
    except Exception as e:
        log.error(f"Force exit failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/control/flatten_all")
def flatten_all_positions():
    """EMERGENCY: Close all positions immediately"""
    try:
        # Get all open positions
        with conn() as c:
            rows = c.execute(
                "SELECT * FROM positions WHERE status='OPEN'"
            ).fetchall()
        
        positions = [dict(r) for r in rows]
        
        if not positions:
            return jsonify({
                "ok": True,
                "message": "No open positions to close",
                "closed": 0
            })
        
        closed = []
        failed = []
        
        for pos in positions:
            try:
                # Mark CLOSING
                with conn() as c:
                    c.execute(
                        "UPDATE positions SET status='CLOSING', exit_reason='FLATTEN_ALL' WHERE id=?",
                        (pos["id"],)
                    )
                
                # Create and submit exit order
                from ap.db import insert_order, update_order, new_local_order_id
                local_order_id = new_local_order_id()
                
                insert_order(
                    local_order_id=local_order_id,
                    position_id=pos["id"],
                    kind="EXIT",
                    status="NEW",
                    symbol=pos["underlying"],
                    contract=pos["contract"],
                    qty=int(pos["qty"]),
                    limit_price=None
                )
                
                resp = BROKER.place_order(
                    symbol=pos["underlying"],
                    contract=pos["contract"],
                    qty=int(pos["qty"]),
                    limit_price=None,
                    side="sell_to_close"
                )
                
                broker_order_id = getattr(resp, "broker_order_id", None)
                status = getattr(resp, "status", None)
                
                update_order(local_order_id, status=status, broker_order_id=broker_order_id)
                
                closed.append({
                    "position_id": pos["id"],
                    "contract": pos["contract"],
                    "order_id": broker_order_id
                })
                
            except Exception as e:
                failed.append({
                    "position_id": pos["id"],
                    "contract": pos["contract"],
                    "error": str(e)
                })
        
        log.warning(f"🚨 FLATTEN ALL: {len(closed)} positions, {len(failed)} failed")
        
        return jsonify({
            "ok": True,
            "message": f"Submitted exit orders for {len(closed)} positions",
            "closed": closed,
            "failed": failed
        })
        
    except Exception as e:
        log.error(f"Flatten all failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


# =========================
# WORKER THREADS
# =========================

def start_worker():
    log.info("Starting worker thread...")
    t = threading.Thread(target=worker_loop, args=(BROKER,), daemon=True, name="WorkerThread")
    t.start()
    log.info("✅ Worker thread started")


def start_exit_manager():
    log.info("Starting exit manager thread...")
    t = threading.Thread(target=exit_manager_loop, args=(BROKER,), daemon=True, name="ExitManagerThread")
    t.start()
    log.info("✅ Exit manager thread started")


def start_fill_monitor():
    """Start fill monitoring thread - THE ACCURACY LAYER!"""
    log.info("Starting fill monitor thread...")
    from ap.fill_monitor import fill_monitor_loop
    t = threading.Thread(target=fill_monitor_loop, args=(BROKER,), daemon=True, name="FillMonitorThread")
    t.start()
    log.info("✅ Fill monitor thread started")


def start_background_threads_once():
    global THREADS_STARTED
    with THREAD_LOCK:
        if THREADS_STARTED:
            log.info("Background threads already started; skipping.")
            return
        start_worker()
        start_exit_manager()
        start_fill_monitor()  # ← CRITICAL FIX!
        THREADS_STARTED = True


# =========================
# INITIALIZATION
# =========================

log.info("=" * 60)
log.info("ANGEL PRECISION BOT - STARTING")
log.info("=" * 60)
log.info(f"Mode: {cfg.BOT_MODE}")
log.info(f"Database: {cfg.DB_FILE}")
log.info("=" * 60)

log.info("Initializing database...")
init_db()
log.info("✅ Database initialized")

log.info(f"Setting mode to: {cfg.BOT_MODE}")
update_state({"mode": cfg.BOT_MODE})
log.info("✅ State initialized")

start_background_threads_once()

log.info("=" * 60)
log.info("✅ INITIALIZATION COMPLETE")
log.info("=" * 60)


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    log.info(f"Starting Flask development server on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
