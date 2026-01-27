# app.py - ANGEL PRECISION BOT (FROZEN VERSION)
# =====================================================================
# THIS FILE IS NOW STABLE. DO NOT EDIT.
# All business logic lives in blueprints (client_api, admin_api, etc).
# Deploy once. Maintain blueprints only.
# =====================================================================

import os
import time
import hmac
import hashlib
import threading
import uuid
from collections import defaultdict, deque
from datetime import datetime, timezone
from functools import wraps
from typing import Optional

from flask import Flask, request, jsonify
from flask_cors import CORS
from pydantic import ValidationError

from ap.config import Config
from ap.db import init_db, conn, get_client
from ap.logger import get_logger
from ap.models import Signal
from ap.queue import enqueue_signal, worker_loop
from ap.state import load_state, update_state
from ap.broker import SimBroker

from ap.parsers import parse_scanner_text
from ap.brokers.tradier import TradierBroker, TradierConfig
from ap.exit_manager import exit_manager_loop

from ap.client_api import client_bp
from ap.admin_api import admin_bp

# ============================================================
# GLOBALS (gunicorn safe - no threads at import time)
# ============================================================

cfg = Config()
log = get_logger("app")

APP_ENV = os.getenv("APP_ENV", "dev").lower().strip()
SIGNING_SECRET = os.getenv("SIGNING_SECRET", "").encode()

# Rate limiting (per worker - upgrade to Redis later)
_RATE = defaultdict(lambda: deque())
RATE_LIMIT_PER_MIN = int(os.getenv("RATE_LIMIT_PER_MIN", "60"))

# Idempotency cache (per worker - upgrade to Redis later)
_IDEMP = {}
IDEMP_TTL_SECONDS = int(os.getenv("IDEMP_TTL_SECONDS", "300"))

# Threads
THREADS_STARTED = False
THREAD_LOCK = threading.Lock()

MAX_CONTENT_LENGTH = int(os.getenv("MAX_CONTENT_LENGTH_BYTES", str(256 * 1024)))

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


def _idem_get(key: str):
    if not key:
        return None
    _idem_cleanup()
    hit = _IDEMP.get(key)
    return hit[1] if hit else None


def _idem_set(key: str, payload: dict):
    if key:
        _IDEMP[key] = (time.time(), payload)


def _verify_hmac(req) -> bool:
    """HMAC-SHA256 signature verification (required in prod)"""
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

    # CRITICAL: cache=True allows Flask to parse JSON later
    raw = req.get_data(cache=True, as_text=False) or b""
    msg = str(ts_i).encode() + b"." + raw
    expected = hmac.new(SIGNING_SECRET, msg, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, sig)


def require_hmac(fn):
    """Require HMAC in prod; no-op in dev"""
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if APP_ENV == "prod" and not _verify_hmac(request):
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        return fn(*args, **kwargs)
    return wrapper


def _require_client_id_header() -> Optional[str]:
    """Multi-client routing via X-Client-Id header"""
    cid = (request.headers.get("X-Client-Id", "") or "").strip()
    if cid:
        return cid
    if APP_ENV != "prod":
        return "default"
    return None


def _validate_active_client(client_id: str):
    """Validate client is active"""
    client = get_client(client_id)
    if (client.get("status") or "").upper() != "ACTIVE":
        raise ValueError(f"client_not_active:{client.get('status')}")
    return client


# ============================================================
# BROKER INITIALIZATION
# ============================================================

def build_broker():
    if cfg.BOT_MODE in ("PAPER", "LIVE"):
        base_url = os.getenv("TRADIER_BASE_URL", "https://sandbox.tradier.com")
        access_token = os.getenv("TRADIER_ACCESS_TOKEN", "").strip()
        account_id = os.getenv("TRADIER_ACCOUNT_ID", "").strip()

        if not access_token or not account_id:
            raise RuntimeError("Missing TRADIER_ACCESS_TOKEN or TRADIER_ACCOUNT_ID")

        log.info(f"Initializing Tradier broker ({cfg.BOT_MODE})")
        b = TradierBroker(TradierConfig(
            base_url=base_url,
            access_token=access_token,
            account_id=account_id,
        ))
        return b

    log.info(f"Initializing Simulator broker ({cfg.BOT_MODE})")
    return SimBroker(starting_equity=10000.0)


# ============================================================
# BACKGROUND THREADS (start once per worker)
# ============================================================

def start_worker(broker):
    log.info("Starting worker thread...")
    t = threading.Thread(target=worker_loop, args=(broker,), daemon=True, name="WorkerThread")
    t.start()


def start_exit_manager(broker):
    log.info("Starting exit manager thread...")
    t = threading.Thread(target=exit_manager_loop, args=(broker,), daemon=True, name="ExitManagerThread")
    t.start()


def start_fill_monitor(broker):
    """Optional: fill monitor thread"""
    try:
        from ap.fill_monitor import fill_monitor_loop
        log.info("Starting fill monitor thread...")
        t = threading.Thread(target=fill_monitor_loop, args=(broker,), daemon=True, name="FillMonitorThread")
        t.start()
    except Exception as e:
        log.warning(f"Fill monitor not available: {e}")


def start_background_threads_once(broker):
    global THREADS_STARTED
    with THREAD_LOCK:
        if THREADS_STARTED:
            return
        start_worker(broker)
        start_exit_manager(broker)
        start_fill_monitor(broker)
        THREADS_STARTED = True
        log.info("✅ All background threads started")


# ============================================================
# APP FACTORY (gunicorn safe)
# ============================================================

def create_app() -> Flask:
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH

    log.info("=" * 70)
    log.info("ANGEL PRECISION BOT - INITIALIZING")
    log.info("=" * 70)
    log.info(f"ENV: {APP_ENV} | MODE: {cfg.BOT_MODE} | DB: {cfg.DB_FILE}")
    log.info("=" * 70)

    init_db()
    log.info("✅ Database initialized")

    update_state({"mode": cfg.BOT_MODE})
    log.info("✅ State initialized")

    broker = build_broker()
    app.config["BROKER"] = broker
    log.info("✅ Broker initialized")

    @app.before_request
    def _ensure_threads_started():
        start_background_threads_once(app.config["BROKER"])

    # Register blueprints
    app.register_blueprint(client_bp)
    app.register_blueprint(admin_bp)

    # CORS
    allow_headers = [
        "Content-Type", "Authorization", "X-Client-Id", "X-AP-Timestamp",
        "X-AP-Signature", "Idempotency-Key", "X-API-Key", "X-Admin-Key",
    ]
    CORS(app, resources={
        r"/client/*": {"origins": ["*"], "methods": ["GET", "POST", "PATCH"], "allow_headers": allow_headers},
        r"/admin/*": {"origins": ["*"], "methods": ["GET", "POST", "PATCH"], "allow_headers": allow_headers},
        r"/signal": {"origins": ["*"], "methods": ["POST"], "allow_headers": allow_headers},
        r"/scanner/*": {"origins": ["*"], "methods": ["POST"], "allow_headers": allow_headers},
        r"/control/*": {"origins": ["*"], "methods": ["POST"], "allow_headers": allow_headers},
        r"/rental/*": {"origins": ["*"], "methods": ["GET", "POST"], "allow_headers": allow_headers},
    })

    # Security headers
    @app.after_request
    def add_security_headers(response):
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"
        if APP_ENV == "prod":
            response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
            response.headers["Content-Security-Policy"] = "default-src 'self'"
        return response

    # =============================================
    # ROOT / HEALTH / STATE (core infrastructure)
    # =============================================

    @app.get("/")
    def root():
        return jsonify({
            "service": "Angel Precision Bot",
            "status": "online",
            "env": APP_ENV,
            "mode": cfg.BOT_MODE,
            "docs": {
                "health": "GET /health",
                "state": "GET /state",
                "ingest": ["POST /signal", "POST /scanner/discord"],
                "client_api": "GET /client/*",
                "admin_api": "GET /admin/*",
                "control": "POST /control/*, POST /kill_switch/*, POST /mode",
                "rental": "POST /rental/subscribe, GET /rental/<client_id>/status",
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
            resp = {
                "ok": all_ok,
                "status": "healthy" if all_ok else "degraded",
                "mode": st.get("mode", "UNKNOWN"),
                "kill_switch": st.get("kill_switch", False),
                "heartbeat_age_seconds": heartbeat_age,
            }

            if APP_ENV != "prod":
                resp["signing_secret_loaded"] = bool(SIGNING_SECRET)

            return jsonify(resp), 200 if all_ok else 503
        except Exception as e:
            log.error(f"Health check failed: {e}")
            return jsonify({"ok": False, "status": "error"}), 503

    @app.get("/state")
    def state():
        try:
            return jsonify(load_state())
        except Exception as e:
            log.error(f"State failed: {e}")
            return jsonify({"error": str(e)}), 500

    @app.get("/dashboard")
    def dashboard():
        try:
            st = load_state()
            with conn() as c:
                pos_row = c.execute("SELECT COUNT(*) as n FROM positions WHERE status='OPEN'").fetchone()
                queue_row = c.execute("SELECT COUNT(*) as n FROM trade_queue WHERE status='NEW'").fetchone()
            
            return jsonify({
                "status": "healthy" if not st.get("kill_switch") else "stopped",
                "mode": st.get("mode"),
                "open_positions": int(pos_row["n"]) if pos_row else 0,
                "pending_signals": int(queue_row["n"]) if queue_row else 0,
                "trades_today": st.get("trades_taken_today", 0),
                "current_equity": st.get("current_equity_last", 0),
            })
        except Exception as e:
            log.error(f"Dashboard failed: {e}")
            return jsonify({"error": str(e)}), 500

    # =============================================
    # GLOBAL CONTROL (kill switch, mode)
    # =============================================

    @app.post("/kill_switch/on")
    @require_hmac
    def kill_on():
        log.warning("🔴 KILL SWITCH ENABLED")
        update_state({"kill_switch": True, "mode": "READ_ONLY"})
        return jsonify({"ok": True, "kill_switch": True})

    @app.post("/kill_switch/off")
    @require_hmac
    def kill_off():
        log.info("🟢 KILL SWITCH DISABLED")
        update_state({"kill_switch": False})
        return jsonify({"ok": True, "kill_switch": False})

    @app.post("/mode")
    @require_hmac
    def set_mode():
        body = request.get_json(force=True) or {}
        mode = str(body.get("mode", "")).upper()
        if mode not in ("SIM", "PAPER", "LIVE", "READ_ONLY"):
            return jsonify({"ok": False, "error": "invalid_mode"}), 400
        log.info(f"Mode changed: {mode}")
        update_state({"mode": mode})
        return jsonify({"ok": True, "mode": mode})

    # =============================================
    # SIGNAL INGESTION (multi-client, HMAC protected)
    # =============================================

    @app.post("/signal")
    @require_hmac
    def signal():
        ip = _client_ip()
        client_id = _require_client_id_header()
        if not client_id:
            return jsonify({"ok": False, "error": "missing_client_id"}), 400

        if _rate_limited(f"signal:{client_id}:{ip}"):
            return jsonify({"ok": False, "error": "rate_limited"}), 429

        try:
            _validate_active_client(client_id)
        except Exception as e:
            msg = str(e)
            if msg.startswith("client_not_active:"):
                return jsonify({"ok": False, "error": "client_not_active"}), 403
            return jsonify({"ok": False, "error": "unknown_client"}), 404

        body = request.get_json(force=True) or {}
        idem_key = (
            str(body.get("signal_id") or "").strip()
            or request.headers.get("Idempotency-Key", "").strip()
        )

        cached = _idem_get(idem_key)
        if cached:
            return jsonify(cached), 200

        try:
            sig = Signal(**body)
        except ValidationError as e:
            payload = {"ok": False, "error": "invalid_signal", "details": e.errors()}
            _idem_set(idem_key, payload)
            return jsonify(payload), 400

        st = load_state()
        if st.get("kill_switch") or st.get("mode") == "READ_ONLY":
            payload = {"ok": False, "error": "bot_in_read_only"}
            _idem_set(idem_key, payload)
            return jsonify(payload), 403

        enqueue_signal(sig, client_id=client_id)
        log.info(f"Signal queued: {client_id} {sig.symbol} {sig.direction}")

        payload = {"ok": True, "queued": True, "signal_id": sig.signal_id}
        _idem_set(idem_key, payload)
        return jsonify(payload), 202

    @app.post("/scanner/discord")
    @require_hmac
    def scanner_discord():
        ip = _client_ip()
        client_id = _require_client_id_header()
        if not client_id:
            return jsonify({"ok": False, "error": "missing_client_id"}), 400

        if _rate_limited(f"scanner:{client_id}:{ip}"):
            return jsonify({"ok": False, "error": "rate_limited"}), 429

        try:
            _validate_active_client(client_id)
        except Exception as e:
            msg = str(e)
            if msg.startswith("client_not_active:"):
                return jsonify({"ok": False, "error": "client_not_active"}), 403
            return jsonify({"ok": False, "error": "unknown_client"}), 404

        body = request.get_json(silent=True) or {}
        text = body.get("content") or ""
        if not text:
            return jsonify({"ok": False, "error": "no_content"}), 400
        if len(text) > 20000:
            return jsonify({"ok": False, "error": "content_too_large"}), 413

        try:
            parsed = parse_scanner_text(text)
            queued = 0
            now_iso = datetime.now(timezone.utc).isoformat()

            for msg in parsed:
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
                            "strike": msg.calls.strike,
                            "entry": msg.calls.entry,
                            "stop": msg.calls.stop,
                            "pt1": msg.calls.pt1,
                            "pt2": msg.calls.pt2,
                            "pt3": msg.calls.pt3,
                            "expiry_hint": msg.calls.expiry_hint,
                        },
                    )
                    enqueue_signal(sig, client_id=client_id)
                    queued += 1

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
                            "strike": msg.puts.strike,
                            "entry": msg.puts.entry,
                            "stop": msg.puts.stop,
                            "pt1": msg.puts.pt1,
                            "pt2": msg.puts.pt2,
                            "pt3": msg.puts.pt3,
                            "expiry_hint": msg.puts.expiry_hint,
                        },
                    )
                    enqueue_signal(sig, client_id=client_id)
                    queued += 1

            return jsonify({"ok": True, "parsed": len(parsed), "queued": queued})

        except Exception as e:
            log.error(f"Scanner parse failed: {e}")
            return jsonify({"ok": False, "error": str(e)}), 500

    # =============================================
    # BROKER TEST
    # =============================================

    @app.get("/tradier/test")
    @require_hmac
    def tradier_test():
        broker = app.config["BROKER"]
        if cfg.BOT_MODE not in ("PAPER", "LIVE"):
            return jsonify({"ok": False, "error": "only_paper_live"}), 400
        try:
            equity = broker.get_account_equity()
            return jsonify({"ok": True, "equity": equity})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500
# =============================================
    # RENTAL SUBSCRIPTIONS (fee-based tiers)
    # =============================================

    @app.post("/rental/subscribe")
    @require_hmac
    def create_rental():
        from ap.subscription_tiers import create_rental_subscription
        
        body = request.get_json(force=True) or {}
        result = create_rental_subscription(
            client_id=body.get("client_id"),
            tier=body.get("tier"),
            start_date=body.get("start_date")
        )
        return jsonify(result), 201 if result.get("ok") else 400

    @app.get("/rental/<client_id>/status")
    @require_hmac
    def rental_status(client_id: str):
        from ap.subscription_tiers import get_rental_status
        
        result = get_rental_status(client_id)
        return jsonify(result), 200 if result.get("ok") else 404

    @app.get("/rental/tiers")
    def list_tiers():
        from ap.subscription_tiers import CorrectRentalTiers
        
        tiers = {}
        for tier_key, tier_def in CorrectRentalTiers.TIERS.items():
            tiers[tier_key] = {
                "name": tier_def["name"],
                "description": tier_def["description"],
                "client_capital": tier_def["client_capital"],
                "rental_fee": tier_def["rental_fee"],
                "working_capital": tier_def["working_capital"],
                "profit_targets": {
                    "min": tier_def["profit_target_min"],
                    "max": tier_def["profit_target_max"]
                },
                "end_balance": {
                    "min": tier_def["end_balance_min"],
                    "max": tier_def["end_balance_max"]
                }
            }
        
        return jsonify({"ok": True, "tiers": tiers})
    log.info("=" * 70)
    log.info("✅ APP READY")
    log.info("=" * 70)
    return app


# Gunicorn entrypoint
app = create_app()

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    log.info(f"Starting on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
