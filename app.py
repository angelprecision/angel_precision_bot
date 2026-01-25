# app.py - Angel Precision Bot (Render + gunicorn safe)
# ✅ Production-safe /signal (HMAC auth + idempotency + rate limit)
# ✅ Debug endpoints hidden in prod
# ✅ Gunicorn-safe: no background threads started at import time (starts on first request per worker)

import os
import time
import json
import hmac
import hashlib
import threading
import uuid
from collections import defaultdict, deque
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


# ============================================================
# GLOBALS (safe for gunicorn import)
# ============================================================

cfg = Config()
log = get_logger("app")

APP_ENV = os.getenv("APP_ENV", "dev").lower()

# HMAC signing secret (required for production /signal)
SIGNING_SECRET = os.getenv("SIGNING_SECRET", "").encode()

# Rate limiting (in-memory per process). Upgrade to Redis later.
_RATE = defaultdict(lambda: deque())
RATE_LIMIT_PER_MIN = int(os.getenv("RATE_LIMIT_PER_MIN", "60"))

# Idempotency (in-memory per process). Upgrade to DB/Redis later.
_IDEMP = {}
IDEMP_TTL_SECONDS = int(os.getenv("IDEMP_TTL_SECONDS", "300"))

# Threads
THREADS_STARTED = False
THREAD_LOCK = threading.Lock()


# ============================================================
# SECURITY HELPERS
# ============================================================

def _client_ip() -> str:
    xff = request.headers.get("X-Forwarded-For", "")
    return (xff.split(",")[0].strip() if xff else request.remote_addr) or "unknown"


def _rate_limited(bucket: str) -> bool:
    now = time.time()
    q = _RATE[bucket]
    while q and (now - q[0] > 60):
        q.popleft()
    if len(q) >= RATE_LIMIT_PER_MIN:
        return True
    q.append(now)
    return False


def _idem_cleanup():
    now = time.time()
    dead = [k for k, (ts, _) in _IDEMP.items() if now - ts > IDEMP_TTL_SECONDS]
    for k in dead:
        _IDEMP.pop(k, None)


def _idem_seen(key: str):
    if not key:
        return None
    _idem_cleanup()
    hit = _IDEMP.get(key)
    return hit[1] if hit else None


def _idem_store(key: str, payload: dict):
    if key:
        _IDEMP[key] = (time.time(), payload)


def _verify_hmac(req) -> bool:
    """
    Client sends:
      X-AP-Timestamp: unix seconds
      X-AP-Signature: hex(hmac_sha256(secret, f"{ts}.{raw_body}"))
    Replay window: ±60s
    """
    if not SIGNING_SECRET:
        return False

    ts = req.headers.get("X-AP-Timestamp", "")
    sig = req.headers.get("X-AP-Signature", "")
    if not ts or not sig:
        return False

    try:
        ts_i = int(ts)
    except ValueError:
        return False

    if abs(int(time.time()) - ts_i) > 60:
        return False

    raw = req.get_data(cache=False, as_text=False) or b""
    msg = str(ts_i).encode() + b"." + raw
    expected = hmac.new(SIGNING_SECRET, msg, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, sig)


def _require_prod_auth() -> bool:
    """Return True if request is authorized (or auth not required)."""
    if APP_ENV != "prod":
        return True
    return _verify_hmac(request)


# ============================================================
# BROKER INIT
# ============================================================

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


# ============================================================
# BACKGROUND THREADS (per gunicorn worker)
# ============================================================

def start_worker(broker):
    log.info("Starting worker thread...")
    t = threading.Thread(target=worker_loop, args=(broker,), daemon=True, name="WorkerThread")
    t.start()
    log.info("✅ Worker thread started")


def start_exit_manager(broker):
    log.info("Starting exit manager thread...")
    t = threading.Thread(target=exit_manager_loop, args=(broker,), daemon=True, name="ExitManagerThread")
    t.start()
    log.info("✅ Exit manager thread started")


def start_fill_monitor(broker):
    """Start fill monitoring thread - THE ACCURACY LAYER!"""
    log.info("Starting fill monitor thread...")
    try:
        from ap.fill_monitor import fill_monitor_loop
    except Exception as e:
        log.warning(f"⚠️ Fill monitor not started (import failed): {e}")
        return

    t = threading.Thread(
        target=fill_monitor_loop,
        args=(broker,),
        daemon=True,
        name="FillMonitorThread"
    )
    t.start()
    log.info("✅ Fill monitor thread started")


def start_background_threads_once(broker):
    global THREADS_STARTED
    with THREAD_LOCK:
        if THREADS_STARTED:
            return
        start_worker(broker)
        start_exit_manager(broker)
        start_fill_monitor(broker)
        THREADS_STARTED = True
        log.info("✅ Background threads started (once per worker)")


# ============================================================
# APP FACTORY (gunicorn safe)
# ============================================================

def create_app() -> Flask:
    app = Flask(__name__)

    # --- init (safe on import; executed when gunicorn creates worker and imports)
    log.info("=" * 60)
    log.info("ANGEL PRECISION BOT - STARTING")
    log.info("=" * 60)
    log.info(f"APP_ENV: {APP_ENV}")
    log.info(f"Mode: {cfg.BOT_MODE}")
    log.info(f"Database: {cfg.DB_FILE}")
    log.info("=" * 60)

    log.info("Initializing database...")
    init_db()
    log.info("✅ Database initialized")

    log.info(f"Setting mode to: {cfg.BOT_MODE}")
    update_state({"mode": cfg.BOT_MODE})
    log.info("✅ State initialized")

    # Broker per worker
    broker = build_broker()
    app.config["BROKER"] = broker

    # Start background threads on first request (per worker)
    @app.before_request
    def _ensure_threads_started():
        start_background_threads_once(app.config["BROKER"])

    # ============================================================
    # ROOT / HEALTH / STATE
    # ============================================================

    @app.get("/")
    def root():
        return jsonify({
            "service": "Angel Precision Bot",
            "status": "online",
            "env": APP_ENV,
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
                "worker_alive": heartbeat_ok "app_env": APP_ENV,
             
                "signing_secret_loaded": bool(SIGNING_SECRET),
                "signing_secret_len": len(SIGNING_SECRET) if SIGNING_SECRET else 0,

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

    # ============================================================
    # SIGNAL INGESTION (HARDENED)
    # ============================================================

    @app.post("/signal")
    def signal():
        ip = _client_ip()

        # Rate limit BEFORE doing any heavy work
        if _rate_limited(f"signal:{ip}"):
            return jsonify({"ok": False, "error": "rate_limited"}), 429

        # Require auth in prod
        if not _require_prod_auth():
            return jsonify({"ok": False, "error": "unauthorized"}), 401

        body = request.get_json(force=True) or {}

        # Idempotency key preference:
        # 1) signal_id (if present)
        # 2) Idempotency-Key header
        # 3) idempotency_key in body
        idem_key = (
            str(body.get("signal_id") or "").strip()
            or request.headers.get("Idempotency-Key", "").strip()
            or str(body.get("idempotency_key") or "").strip()
        )

        cached = _idem_seen(idem_key)
        if cached:
            return jsonify(cached), 200

        try:
            sig = Signal(**body)
        except ValidationError as e:
            log.warning(f"Invalid signal payload: {e.errors()}")
            payload = {
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
            }
            _idem_store(idem_key, payload)
            return jsonify(payload), 400

        st = load_state()
        if st.get("kill_switch") or st.get("mode") == "READ_ONLY":
            payload = {
                "ok": False,
                "error": "bot_in_read_only",
                "mode": st.get("mode"),
                "kill_switch": st.get("kill_switch", False)
            }
            _idem_store(idem_key, payload)
            return jsonify(payload), 403

        enqueue_signal(sig)
        log.info(f"Signal queued: {sig.symbol} {sig.direction} (ip={ip})")

        payload = {"ok": True, "queued": True, "signal_id": sig.signal_id}
        _idem_store(idem_key, payload)
        return jsonify(payload), 202

    @app.post("/scanner/discord")
    def scanner_discord():
        # Optional: require auth in prod (recommended)
        if APP_ENV == "prod" and not _require_prod_auth():
            return jsonify({"ok": False, "error": "unauthorized"}), 401

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

    # ============================================================
    # UTIL
    # ============================================================

    @app.get("/tradier/test")
    def tradier_test():
        broker = app.config["BROKER"]
        if cfg.BOT_MODE not in ("PAPER", "LIVE"):
            return jsonify({"ok": False, "error": "Only available in PAPER/LIVE mode"}), 400
        try:
            equity = broker.get_account_equity()
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
        broker = app.config["BROKER"]
        try:
            equity = broker.get_account_equity()
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

    # ============================================================
    # REPORTING
    # ============================================================

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

    # ============================================================
    # DEBUG & CONTROL
    # ============================================================

    @app.get("/debug/order/<order_id>")
    def debug_order(order_id):
        broker = app.config["BROKER"]
        if APP_ENV == "prod":
            return jsonify({"ok": False, "error": "not_found"}), 404
        try:
            return jsonify({"ok": True, "order": broker.get_order(order_id)})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.post("/debug/test_signal")
    def debug_test_signal():
        if APP_ENV == "prod":
            return jsonify({"ok": False, "error": "not_found"}), 404

        body = request.get_json(force=True) or {}

        symbol = (body.get("symbol", "SPY") or "SPY").upper().strip()
        direction = (body.get("direction", "CALL") or "CALL").upper().strip()
        strike = body.get("strike")
        expiry_hint = body.get("expiry_hint", "Weekly")

        if strike is None:
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
                direction=direction,
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
        broker = app.config["BROKER"]
        try:
            from ap.contract_pricing import get_contract_price

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
                    current_price = get_contract_price(broker, pos["contract"], side="SELL")
                    entry_price = float(pos["avg_fill"])
                    qty = int(pos["qty"])
                    unrealized_pnl = (current_price - entry_price) * qty * 100

                    tp_price = entry_price * (1.0 + float(pos["tp_pct"]))
                    sl_price = entry_price * (1.0 - float(pos["sl_pct"]))

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
        broker = app.config["BROKER"]
        try:
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

            with conn() as c:
                c.execute(
                    "UPDATE positions SET status='CLOSING', exit_reason='MANUAL_EXIT' WHERE id=?",
                    (position_id,)
                )

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

            resp = broker.place_order(
                symbol=pos["underlying"],
                contract=pos["contract"],
                qty=int(pos["qty"]),
                limit_price=None,
                side="sell_to_close"
            )

            broker_order_id = getattr(resp, "broker_order_id", None)
            status = getattr(resp, "status", None)

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
        broker = app.config["BROKER"]
        try:
            with conn() as c:
                rows = c.execute("SELECT * FROM positions WHERE status='OPEN'").fetchall()

            positions = [dict(r) for r in rows]
            if not positions:
                return jsonify({"ok": True, "message": "No open positions to close", "closed": 0})

            closed = []
            failed = []

            for pos in positions:
                try:
                    with conn() as c:
                        c.execute(
                            "UPDATE positions SET status='CLOSING', exit_reason='FLATTEN_ALL' WHERE id=?",
                            (pos["id"],)
                        )

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

                    resp = broker.place_order(
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

    log.info("=" * 60)
    log.info("✅ APP FACTORY READY")
    log.info("=" * 60)
    return app


# gunicorn entrypoint
app = create_app()


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    log.info(f"Starting Flask dev server on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
