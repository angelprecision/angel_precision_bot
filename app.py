# app.py - Angel Precision Bot (Render + gunicorn safe)
import os
import threading
import uuid
from datetime import datetime, timezone
from flask import Flask, request, jsonify
from pydantic import ValidationError
from ap.config import Config
from ap.db import init_db
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
# HEALTH & STATE ENDPOINTS
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
        from ap.db import conn

        with conn() as c:
            pos_row = c.execute("SELECT COUNT(*) as n FROM positions WHERE status='OPEN'").fetchone()
            open_positions = pos_row["n"] if pos_row else 0

            queue_row = c.execute("SELECT COUNT(*) as n FROM trade_queue WHERE status='NEW'").fetchone()
            pending_signals = queue_row["n"] if queue_row else 0

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
# REPORTING ENDPOINTS
# =========================

@app.get("/report/orders")
def report_orders():
    """Get all orders with full details"""
    try:
        from ap.db import conn
        
        limit = request.args.get("limit", 50, type=int)
        status_filter = request.args.get("status")
        
        with conn() as c:
            if status_filter:
                rows = c.execute("""
                    SELECT * FROM orders 
                    WHERE status=?
                    ORDER BY created_ts DESC 
                    LIMIT ?
                """, (status_filter, limit)).fetchall()
            else:
                rows = c.execute("""
                    SELECT * FROM orders 
                    ORDER BY created_ts DESC 
                    LIMIT ?
                """, (limit,)).fetchall()
        
        orders = [dict(r) for r in rows]
        
        with conn() as c:
            stats = c.execute("""
                SELECT 
                    status,
                    COUNT(*) as count
                FROM orders
                GROUP BY status
            """).fetchall()
        
        summary = {row["status"]: row["count"] for row in stats}
        
        return jsonify({
            "ok": True,
            "count": len(orders),
            "summary": summary,
            "orders": orders
        })
    except Exception as e:
        log.error(f"Report orders failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/report/positions")
def report_positions():
    """Get all positions with full details"""
    try:
        from ap.db import conn
        
        status_filter = request.args.get("status", "OPEN")
        
        with conn() as c:
            if status_filter == "ALL":
                rows = c.execute("""
                    SELECT * FROM positions 
                    ORDER BY entry_ts DESC
                """).fetchall()
            else:
                rows = c.execute("""
                    SELECT * FROM positions 
                    WHERE status=?
                    ORDER BY entry_ts DESC
                """, (status_filter,)).fetchall()
        
        positions = [dict(r) for r in rows]
        
        total_pnl = sum(float(p.get("realized_pnl") or 0) for p in positions if p.get("status") == "CLOSED")
        
        return jsonify({
            "ok": True,
            "count": len(positions),
            "total_realized_pnl": total_pnl,
            "positions": positions
        })
    except Exception as e:
        log.error(f"Report positions failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/report/audit")
def report_audit():
    """Get recent audit log events"""
    try:
        from ap.db import conn
        import json as json_lib
        
        limit = request.args.get("limit", 100, type=int)
        event_filter = request.args.get("event")
        level_filter = request.args.get("level")
        
        with conn() as c:
            sql = "SELECT * FROM audit_log WHERE 1=1"
            params = []
            
            if event_filter:
                sql += " AND event=?"
                params.append(event_filter)
            
            if level_filter:
                sql += " AND level=?"
                params.append(level_filter)
            
            sql += " ORDER BY id DESC LIMIT ?"
            params.append(limit)
            
            rows = c.execute(sql, params).fetchall()
        
        events = []
        for r in rows:
            event = dict(r)
            try:
                event["payload"] = json_lib.loads(event["payload"])
            except:
                pass
            events.append(event)
        
        return jsonify({
            "ok": True,
            "count": len(events),
            "events": events
        })
    except Exception as e:
        log.error(f"Report audit failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/report/summary")
def report_summary():
    """Get overall bot health summary"""
    try:
        from ap.db import conn
        
        with conn() as c:
            order_stats = c.execute("""
                SELECT status, COUNT(*) as count
                FROM orders
                GROUP BY status
            """).fetchall()
            
            pos_stats = c.execute("""
                SELECT status, COUNT(*) as count
                FROM positions
                GROUP BY status
            """).fetchall()
            
            queue_stats = c.execute("""
                SELECT status, COUNT(*) as count
                FROM trade_queue
                GROUP BY status
            """).fetchall()
            
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
        log.error(f"Report summary failed: {e}")
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


def start_background_threads_once():
    global THREADS_STARTED
    with THREAD_LOCK:
        if THREADS_STARTED:
            log.info("Background threads already started; skipping.")
            return
        start_worker()
        start_exit_manager()
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
