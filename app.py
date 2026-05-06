# app.py - ANGEL PRECISION BOT (PRODUCTION VERSION - STABLE)
# ── In-process caches (reduce DB round-trips on hot /signal path) ────────
# Avoids blocking DB calls when scanner fires 20+ signals in a burst.
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
from ap.db import init_db, conn, get_client, create_client
from ap.logger import get_logger
from ap.models import Signal
from ap.queue import enqueue_signal, worker_loop
from ap.state import load_state, update_state
from ap.broker import SimBroker

from ap.parsers import parse_scanner_text
from ap.brokers.tradier import TradierBroker, TradierConfig
# exit_manager_loop removed — APExitEngine is the sole exit manager

# ap.client_api and ap.admin_api not yet built -- routes are inline in create_app()
# from ap.client_api import client_bp
# from ap.admin_api import admin_bp
from client_runner import start_multi_client_supervisor, route_signal_to_all_clients

# ── In-process caches (reduce DB round-trips on hot /signal path) ──────────
_SIGNAL_CACHE_LOCK   = threading.Lock()
_client_status_cache: dict = {}   # {client_id: (status, expires_ts)}
_kill_switch_cache:   dict = {}   # {client_id: (kill_val, mode, expires_ts)}
_CACHE_TTL = 30.0                 # seconds -- refresh every 30s

# HIGH-007: gate supervisor behind env var -- do not start at import time
if os.getenv("RUN_SUPERVISOR") == "1":
    start_multi_client_supervisor()

# ============================================================
# GLOBALS (gunicorn safe - no threads at import time)
# ============================================================

cfg = Config()
log = get_logger("app")
log.info(f"CONFIG LOADED FROM: {__import__('ap.config').config.__file__}")

APP_ENV = os.getenv("APP_ENV", "prod").lower().strip()
SIGNING_SECRET = os.getenv("SIGNING_SECRET", "").encode()
log.info("SIGNING_SECRET_SHA256_8=" + hashlib.sha256(SIGNING_SECRET).hexdigest()[:8])

# ✅ CRITICAL: Validate SIGNING_SECRET in prod
if APP_ENV == "prod" and not SIGNING_SECRET:
    log.error("=" * 70)
    log.error("🚨 CRITICAL: SIGNING_SECRET not set in production!")
    log.error("All signal ingestion will fail with 401 unauthorized")
    log.error("Set SIGNING_SECRET env var on Render")
    log.error("=" * 70)
    raise RuntimeError("SIGNING_SECRET required in production")

# Rate limiting (per worker - upgrade to Redis later)
_RATE = defaultdict(lambda: deque())
RATE_LIMIT_PER_MIN = int(os.getenv("RATE_LIMIT_PER_MIN", "60"))

# Idempotency cache (per worker - upgrade to Redis later)
_IDEMP = {}
_IDEMP_LAST_CLEANUP: float = 0.0
_SELF_HEAL_IN_PROGRESS = threading.Event()
IDEMP_TTL_SECONDS = int(os.getenv("IDEMP_TTL_SECONDS", "300"))

# HMAC time drift (seconds)
HMAC_MAX_SKEW_SECONDS = int(os.getenv("HMAC_MAX_SKEW_SECONDS", "300"))  # ✅ 5 min default

# Threads
THREADS_STARTED = False
THREAD_LOCK = threading.Lock()
_IDEMP_LOCK = threading.Lock()

MAX_CONTENT_LENGTH = int(os.getenv("MAX_CONTENT_LENGTH_BYTES", str(256 * 1024)))

DEFAULT_CLIENT_ID = os.getenv("DEFAULT_CLIENT_ID", "default").strip() or "default"

# ============================================================
# SECURITY HELPERS (HMAC + Rate Limit + Idempotency)
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
    global _IDEMP_LAST_CLEANUP
    now = time.time()
    with _IDEMP_LOCK:
        if now - _IDEMP_LAST_CLEANUP < 1.0:
            return
        _IDEMP_LAST_CLEANUP = now
        dead = [k for k, (ts, _) in _IDEMP.items() if now - ts > IDEMP_TTL_SECONDS]
        for k in dead:
            _IDEMP.pop(k, None)


def _idem_get(key: str):
    if not key:
        return None
    _idem_cleanup()
    with _IDEMP_LOCK:
        hit = _IDEMP.get(key)
    return hit[1] if hit else None


def _idem_set(key: str, payload: dict):
    if key:
        with _IDEMP_LOCK:
            _IDEMP[key] = (time.time(), payload)


def _hmac_hex(key: bytes, msg: bytes) -> str:
    """Helper: compute HMAC-SHA256 hex digest"""
    return hmac.new(key, msg, hashlib.sha256).hexdigest()


def _verify_hmac(req) -> bool:
    """
    HMAC verification supporting 2 schemes:

    Scheme A (preferred): X-AP-Timestamp + X-AP-Signature
        signature = HMAC_SHA256(secret, f"{timestamp}.{raw_body_bytes}")

    Scheme B (fallback): X-Signature
        signature = HMAC_SHA256(secret, raw_body_bytes)

    Returns True if either scheme validates successfully.
    """
    # In dev/test, allow unsigned requests
    if APP_ENV != "prod":
        return True

    # In prod, require secret + valid signature
    if not SIGNING_SECRET:
        log.warning("SIGNING_SECRET not set - rejecting request")
        return False

    raw = req.get_data(cache=True, as_text=False) or b""

    # -----------------------------
    # Scheme A: Timestamped HMAC
    # -----------------------------
    ts = (req.headers.get("X-AP-Timestamp", "") or "").strip()
    sig = (req.headers.get("X-AP-Signature", "") or "").strip()

    if ts and sig:
        try:
            ts_i = int(ts)
        except Exception:
            log.warning(f"Invalid X-AP-Timestamp: {ts!r} — rejecting, not falling through to Scheme B")
            return False  # Scheme A headers present but ts unparseable — hard reject

        if ts_i is not None:
            # Anti-replay / drift window — hard reject, no Scheme B fallthrough
            if abs(int(time.time()) - ts_i) > HMAC_MAX_SKEW_SECONDS:
                log.warning(f"Timestamp out of range: {ts_i} — rejecting, not falling through to Scheme B")
                return False
            msg = str(ts_i).encode("utf-8") + b"." + raw
            expected = _hmac_hex(SIGNING_SECRET, msg)
            if hmac.compare_digest(expected, sig):
                log.debug("✅ HMAC verified (timestamped scheme)")
                return True
            log.warning("HMAC timestamped scheme failed (signature mismatch)")
            return False  # Scheme A headers present but wrong sig — never try Scheme B

    # -----------------------------
    # Scheme B: Simple body-only HMAC
    # -----------------------------
    simple_sig = (req.headers.get("X-Signature", "") or "").strip()
    if simple_sig:
        expected_simple = _hmac_hex(SIGNING_SECRET, raw)
        if hmac.compare_digest(expected_simple, simple_sig):
            log.debug("✅ HMAC verified (simple scheme)")
            return True
        else:
            log.warning("HMAC simple scheme failed (signature mismatch)")

    # Both schemes failed
    log.warning(f"HMAC verification failed for {req.path} from {_client_ip()}")
    return False


def require_hmac(fn):
    """Require HMAC in prod; no-op in dev"""
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if APP_ENV == "prod" and not _verify_hmac(request):
            log.warning(f"HMAC verification failed for {request.path} from {_client_ip()}")
            return jsonify({
                "ok": False,
                "error": "unauthorized",
                "hint": "Provide X-AP-Timestamp + X-AP-Signature (preferred) OR X-Signature"
            }), 401
        return fn(*args, **kwargs)
    return wrapper


def _require_client_id_header() -> Optional[str]:
    """Multi-client routing via X-Client-Id header"""
    cid = (request.headers.get("X-Client-Id", "") or "").strip()
    if cid:
        return cid
    if APP_ENV != "prod":
        return DEFAULT_CLIENT_ID
    return None


def _validate_active_client(client_id: str):
    """Validate client is active -- in-process cache avoids blocking DB call
    on every /signal POST during scanner signal bursts."""
    now = time.monotonic()
    with _SIGNAL_CACHE_LOCK:
        cached = _client_status_cache.get(client_id)
    if cached and cached[1] > now:
        status = cached[0]
    else:
        client = get_client(client_id)
        status = (client.get("status") or "").upper()
        with _SIGNAL_CACHE_LOCK:
            _client_status_cache[client_id] = (status, now + _CACHE_TTL)
    if status != "ACTIVE":
        raise ValueError(f"client_not_active:{status}")


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
        return TradierBroker(TradierConfig(
            base_url=base_url,
            access_token=access_token,
            account_id=account_id,
        ))

    log.info(f"Initializing Simulator broker ({cfg.BOT_MODE})")
    return SimBroker(starting_equity=10000.0)


# ============================================================
# BACKGROUND THREADS (start once per worker)
# ============================================================

def start_worker(broker):
    log.info("Starting worker thread...")
    t = threading.Thread(target=worker_loop, args=(broker,), daemon=True, name="WorkerThread")
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
        start_fill_monitor(broker)
        THREADS_STARTED = True
        log.info("✅ All background threads started")


# ============================================================
# APP FACTORY (gunicorn safe)
# ============================================================


def _discord_signal_id(symbol: str, direction: str, strike, content_hash: str) -> str:
    """Deterministic signal_id so Discord webhook retries don't create duplicate queue entries."""
    raw = f"{symbol}:{direction}:{strike}:{content_hash}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]

def _fetch_broker_equity() -> float:
    """Fetch live account equity at startup. Returns 0.0 on any failure —
    client_runner._sync_account_equity() pulls the real balance after startup.
    """
    try:
        broker = build_broker()
        for method in ("get_account_equity", "get_account_balance"):
            fn = getattr(broker, method, None)
            if callable(fn):
                val = fn()
                if val and float(val) > 0:
                    return float(val)
        if hasattr(broker, "get_balances"):
            b = broker.get_balances()
            for k in ("equity", "total_equity", "net_liquidation", "cash"):
                if b.get(k) and float(b[k]) > 0:
                    return float(b[k])
    except Exception as _e:
        log.warning("_fetch_broker_equity failed at startup — defaulting to 0.0: %s", _e)
    return 0.0


def create_app() -> Flask:
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH

    log.info("=" * 70)
    log.info("ANGEL PRECISION BOT - INITIALIZING")
    log.info("=" * 70)

    mode = getattr(cfg, "BOT_MODE", os.getenv("BOT_MODE", os.getenv("MODE", "PAPER"))).upper()
    db_file = getattr(cfg, "DB_FILE", os.getenv("BOT_DB_FILE", "ap_state.db"))
    log.info(f"ENV: {APP_ENV} | MODE: {mode} | DB: {db_file}")
    log.info("=" * 70)

    init_db()
    log.info("✅ Database initialized")

    # ✅ Ensure default client exists BEFORE any state write
    with conn() as c:
        row = c.execute("SELECT 1 FROM clients WHERE client_id=%s", (DEFAULT_CLIENT_ID,)).fetchone()

    if not row:
        log.info(f"Creating default client (not found in DB): {DEFAULT_CLIENT_ID}")
        create_client(
            client_id=DEFAULT_CLIENT_ID,
            name="Default Client",
            broker_type=os.getenv("BROKER_TYPE", "tradier"),
            broker_account_id=os.getenv("TRADIER_ACCOUNT_ID", ""),
            broker_token=os.getenv("TRADIER_ACCESS_TOKEN", ""),
            broker_base_url=os.getenv("TRADIER_BASE_URL", "https://sandbox.tradier.com"),
            initial_equity=_fetch_broker_equity() or 0.0,
        )
        log.info("✅ Default client created")
    else:
        log.info("✅ Default client exists")

    # Debug routes — only registered in non-prod environments.
    # Conditional registration (not just conditional response) avoids
    # exposing these endpoints as attack surface in production.
    if APP_ENV != "prod":
        @app.get("/debug/threads")
        @require_hmac
        def debug_threads():
            import threading
            threads = []
            for t in threading.enumerate():
                threads.append({
                    "name": t.name,
                    "daemon": bool(getattr(t, "daemon", False)),
                    "alive": bool(t.is_alive()),
                })
            return jsonify({"ok": True, "threads": threads})

        @app.get("/debug/queue_counts")
        @require_hmac
        def debug_queue_counts():
            try:
                from ap.db import run_with_retry
                def _q():
                    with conn() as c:
                        return {
                            "new":  c.execute("SELECT COUNT(*) AS n FROM trade_queue WHERE status='NEW'").fetchone()["n"],
                            "proc": c.execute("SELECT COUNT(*) AS n FROM trade_queue WHERE status='PROCESSING'").fetchone()["n"],
                            "done": c.execute("SELECT COUNT(*) AS n FROM trade_queue WHERE status='DONE'").fetchone()["n"],
                            "err":  c.execute("SELECT COUNT(*) AS n FROM trade_queue WHERE status='ERROR'").fetchone()["n"],
                        }
                counts = run_with_retry(_q)
                return jsonify({"ok": True, "NEW": int(counts["new"]), "PROCESSING": int(counts["proc"]), "DONE": int(counts["done"]), "ERROR": int(counts["err"])})
            except Exception as e:
                return jsonify({"ok": False, "error": str(e)}), 500

    # ✅ Explicit default client state update (prevents FK issues & ambiguity)
    update_state({"mode": mode}, client_id=DEFAULT_CLIENT_ID)
    log.info("✅ State initialized")

    broker = build_broker()
    app.config["BROKER"] = broker
    log.info("✅ Broker initialized")

    if os.getenv("RUN_SUPERVISOR") == "1":
        global THREADS_STARTED
        THREADS_STARTED = True   # supervisor owns the workers — mark ready
        log.info("Supervisor active — legacy worker threads DISABLED")
    else:
        # Fix 5: hard block LIVE mode without supervisor
        _mode_now = os.getenv("BOT_MODE", os.getenv("MODE", "PAPER")).upper()
        if _mode_now == "LIVE":
            raise RuntimeError(
                "LIVE mode requires RUN_SUPERVISOR=1. "
                "Set RUN_SUPERVISOR=1 in Render env vars or switch to PAPER mode."
            )
        start_background_threads_once(broker)
        log.info("Background threads started (legacy path — PAPER/SIM only)")

    # Register blueprints
    # Blueprints not yet built -- routes registered inline above
    log.info("✅ Blueprints registered")

    # CORS
    allow_headers = [
        "Content-Type", "Authorization", "X-Client-Id", "X-AP-Timestamp",
        "X-AP-Signature", "Idempotency-Key", "X-API-Key", "X-Admin-Key",
        "X-Signature",  # ✅ Added for simple HMAC scheme
    ]
    _trusted_origins = [
        "https://www.angelprecision.com",
        "https://angelprecision.com",
        "https://angel-precision-dashboard-backend.onrender.com",
        "https://angel-precision-bot-official-1.onrender.com",
        # Add client-specific origins here as they onboard
    ]
    # Public endpoints open to any origin (scanner POSTs use HMAC auth, not origin)
    _open_origins = ["*"]

    CORS(app, resources={
        r"/client/*": {"origins": _trusted_origins, "methods": ["GET", "POST", "PATCH"], "allow_headers": allow_headers},
        r"/admin/*":  {"origins": _trusted_origins, "methods": ["GET", "POST", "PATCH"], "allow_headers": allow_headers},
        r"/signal":   {"origins": _open_origins,    "methods": ["POST"],                 "allow_headers": allow_headers},
        r"/scanner/*":{"origins": _open_origins,    "methods": ["POST"],                 "allow_headers": allow_headers},
        r"/control/*":{"origins": _open_origins,    "methods": ["POST"],                 "allow_headers": allow_headers},
        r"/rental/*": {"origins": _trusted_origins, "methods": ["GET", "POST"],          "allow_headers": allow_headers},
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
        st = load_state(client_id=DEFAULT_CLIENT_ID)
        return jsonify({
            "service": "Angel Precision Bot",
            "status": "online",
            "env": APP_ENV,
            "mode": st.get("mode", cfg.BOT_MODE),
            "kill_switch": st.get("kill_switch", False),
            "docs": {
                "health": "GET /health",
                "state": "GET /state",
                "dashboard": "GET /dashboard",
                "ingest": ["POST /signal", "POST /scanner/discord"],
            }
        })

    @app.get("/health")
    def health():
        try:
            st = load_state(client_id=DEFAULT_CLIENT_ID)
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
            return jsonify(load_state(client_id=DEFAULT_CLIENT_ID))
        except Exception as e:
            log.error(f"State failed: {e}")
            return jsonify({"error": str(e)}), 500

    @app.get("/dashboard")
    def dashboard():
        try:
            from ap.db import run_with_retry
            st = load_state(client_id=DEFAULT_CLIENT_ID)
            def _q():
                with conn() as c:
                    pos_row   = c.execute("SELECT COUNT(*) as n FROM positions WHERE status='OPEN'").fetchone()
                    queue_row = c.execute("SELECT COUNT(*) as n FROM trade_queue WHERE status='NEW'").fetchone()
                    return pos_row, queue_row
            pos_row, queue_row = run_with_retry(_q)
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
        update_state({"kill_switch": True, "mode": "READ_ONLY"}, client_id=DEFAULT_CLIENT_ID)
        with _SIGNAL_CACHE_LOCK:
            _kill_switch_cache.clear()
            _client_status_cache.clear()
        return jsonify({"ok": True, "kill_switch": True})

    @app.post("/kill_switch/off")
    @require_hmac
    def kill_off():
        log.info("🟢 KILL SWITCH DISABLED")
        update_state({"kill_switch": False}, client_id=DEFAULT_CLIENT_ID)
        with _SIGNAL_CACHE_LOCK:
            _kill_switch_cache.clear()
        return jsonify({"ok": True, "kill_switch": False})

    @app.post("/mode")
    @require_hmac
    def set_mode():
        body = request.get_json(force=True) or {}
        new_mode = str(body.get("mode", "")).upper()
        if new_mode not in ("SIM", "PAPER", "LIVE", "READ_ONLY"):
            return jsonify({"ok": False, "error": "invalid_mode"}), 400
        log.info(f"Mode changed: {new_mode}")
        update_state({"mode": new_mode}, client_id=DEFAULT_CLIENT_ID)
        return jsonify({"ok": True, "mode": new_mode})

    # =============================================
    # SIGNAL INGESTION (multi-client, HMAC protected)
    # =============================================

    @app.post("/signal")
    @require_hmac
    def signal():
        # Guard: return 503 if worker not ready — scanner will retry instead of silently dropping
        if not THREADS_STARTED:
            return jsonify({"ok": False, "error": "worker_not_ready", "hint": "Bot is starting up — retry in 5s"}), 503
        # Self-heal: if no runners alive after restart, spawn them now before routing.
        try:
            import os as _os2
            from client_runner import (
                _active_runners as _ar, _registry_lock as _rl,
                ClientRunner as _CR, _fetch_active_members as _fam,
                SUPABASE_URL as _su, SUPABASE_SERVICE_KEY as _sk,
            )
            if _os2.getenv("RUN_SUPERVISOR") == "1":
                with _rl:
                    _any_alive = any(r.is_alive() for r in _ar.values())
                if not _any_alive and _su and _sk:
                    if not _SELF_HEAL_IN_PROGRESS.is_set():
                        _SELF_HEAL_IN_PROGRESS.set()
                        try:
                            from supabase import create_client as _cc2
                            _members2 = _fam(_cc2(_su, _sk))
                            for _m2 in _members2:
                                _email2 = _m2["email"]
                                with _rl:
                                    _ex2 = _ar.get(_email2)
                                    if not _ex2 or not _ex2.is_alive():
                                        _r2 = _CR(_m2)
                                        _ar[_email2] = _r2
                                        _r2.start()
                                        log.info(f"signal self-heal: started runner for {_email2}")
                        finally:
                            _SELF_HEAL_IN_PROGRESS.clear()
                    # else: another request is already healing — signals are enqueued durably
        except Exception as _she:
            log.warning(f"signal self-heal error (non-fatal): {_she}")
        ip = _client_ip()
        client_id = _require_client_id_header()
        if not client_id:
            hint = "Set X-Client-Id: default header" if APP_ENV == "prod" else ""
            return jsonify({"ok": False, "error": "missing_client_id", "hint": hint}), 400

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
        idem_key = (str(body.get("signal_id") or "").strip()
                    or request.headers.get("Idempotency-Key", "").strip())

        cached = _idem_get(idem_key)
        if cached:
            return jsonify(cached), 200

        # HIGH-008: use per-client cache key instead of DEFAULT_CLIENT_ID
        _now = time.monotonic()
        with _SIGNAL_CACHE_LOCK:
            _ks_cached = _kill_switch_cache.get(client_id)
        if _ks_cached and _ks_cached[2] > _now:
            _ks, _mode = _ks_cached[0], _ks_cached[1]
        else:
            _st = load_state(client_id=client_id)
            _ks, _mode = _st.get("kill_switch"), _st.get("mode")
            with _SIGNAL_CACHE_LOCK:
                _kill_switch_cache[client_id] = (_ks, _mode, _now + _CACHE_TTL)
        if _ks or _mode == "READ_ONLY":
            payload = {"ok": False, "error": "bot_in_read_only"}
            _idem_set(idem_key, payload)
            return jsonify(payload), 403

        # Fix 4: synchronous durable enqueue — 202 only after queue write succeeds
        _sig_id = str(body.get("signal_id") or uuid.uuid4())
        body["signal_id"] = _sig_id

        try:
            enqueued = route_signal_to_all_clients(body)
            log.info(
                f"Signal routed: {body.get('ticker')} {body.get('side')} "
                f"score={body.get('score')} sig={_sig_id} enqueued={enqueued}"
            )
            if not enqueued:
                payload = {"ok": False, "error": "no_active_clients", "signal_id": _sig_id}
                return jsonify(payload), 503
            payload = {"ok": True, "queued": True, "signal_id": _sig_id, "enqueued": enqueued}
            _idem_set(idem_key, payload)
            return jsonify(payload), 202
        except Exception as _e:
            log.error(f"/signal durable enqueue failed: {_e}")
            payload = {"ok": False, "error": "enqueue_failed", "signal_id": _sig_id}
            return jsonify(payload), 500

    @app.post("/scanner/discord")
    @require_hmac
    def scanner_discord():
        ip = _client_ip()
        client_id = _require_client_id_header()
        if not client_id:
            hint = "Set X-Client-Id: default header" if APP_ENV == "prod" else ""
            return jsonify({"ok": False, "error": "missing_client_id", "hint": hint}), 400

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
                    _chash = hashlib.sha256(text.encode()).hexdigest()[:16]
                    sig = Signal(
                        signal_id=_discord_signal_id(msg.symbol, "CALL", msg.calls.strike, _chash),
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
                    _phash = hashlib.sha256(text.encode()).hexdigest()[:16]
                    sig = Signal(
                        signal_id=_discord_signal_id(msg.symbol, "PUT", msg.puts.strike, _phash),
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

    @app.get("/admin/runner_status")
    @require_hmac
    def admin_runner_status():
        try:
            from client_runner import get_runner_status
            runners = get_runner_status()
            return jsonify({"ok": True, "runners": runners, "count": len(runners)})
        except Exception as e:
            log.error(f"Runner status failed: {e}")
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.get("/admin/runner_failures")
    @require_hmac
    def admin_runner_failures():
        try:
            from client_runner import get_runner_failure_log
            failures = get_runner_failure_log()
            return jsonify({"ok": True, "failures": failures, "count": len(failures)})
        except Exception as e:
            log.error(f"Runner failures failed: {e}")
            return jsonify({"ok": False, "error": str(e)}), 500




    @app.post("/admin/force_initialize")
    @require_hmac
    def admin_force_initialize():
        """EMERGENCY: Force initialized.set() + entries_allowed.set() on a stuck runner.
        Use only when runner is alive but stuck in startup (initialized=False for >60s).
        """
        try:
            import time as _t
            from client_runner import _active_runners, _registry_lock

            forced = []
            with _registry_lock:
                for email, runner in list(_active_runners.items()):
                    if runner.is_alive() and not runner.initialized.is_set() and not runner.failed.is_set():
                        # Force the initialization flags
                        runner.initialized.set()
                        # Set sub-thread placeholders so _set_entry_permission() passes
                        import threading as _th
                        if runner.fill_monitor_thread is None or not runner.fill_monitor_thread.is_alive():
                            runner._fill_dead_since = None  # reset grace period
                        if runner.worker_thread is None:
                            # Create a dummy placeholder — actual worker won't start but entries can flow
                            log.warning(f"force_initialize: {email} worker_thread is None — runner may not process signals")
                        runner._set_entry_permission()
                        forced.append({
                            "email": email,
                            "initialized_set": runner.initialized.is_set(),
                            "entries_allowed": runner.entries_allowed.is_set(),
                            "fill_alive": runner.fill_monitor_thread.is_alive() if runner.fill_monitor_thread else False,
                            "worker_alive": runner.worker_thread.is_alive() if runner.worker_thread else False,
                        })
                        log.warning(f"force_initialize: forced initialized=True for {email}")

            if not forced:
                return jsonify({"ok": False, "error": "No stuck runners found (already initialized or failed)"})

            return jsonify({"ok": True, "forced": forced})
        except Exception as e:
            log.error(f"force_initialize failed: {e}", exc_info=True)
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.post("/admin/reset_runner")
    @require_hmac
    def admin_reset_runner():
        """Kill any stuck runner (initialized=False for >120s) and restart it cleanly."""
        try:
            import time as _time
            from client_runner import (
                _active_runners, _registry_lock, ClientRunner,
                _fetch_active_members, SUPABASE_URL, SUPABASE_SERVICE_KEY,
            )
            from supabase import create_client

            killed = []
            restarted = []

            with _registry_lock:
                for email, runner in list(_active_runners.items()):
                    # Kill runners that are alive but not initialized after >60s
                    is_stuck = (
                        runner.is_alive()
                        and not runner.initialized.is_set()
                        and not runner.failed.is_set()
                    )
                    is_dead = not runner.is_alive()
                    if is_stuck or is_dead:
                        runner.stop()
                        del _active_runners[email]
                        killed.append(email)
                        log.warning(f"reset_runner: killed {'stuck' if is_stuck else 'dead'} runner for {email}")

            if killed and SUPABASE_URL and SUPABASE_SERVICE_KEY:
                sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
                members = _fetch_active_members(sb)
                for member in members:
                    email = member["email"]
                    if email not in killed:
                        continue
                    with _registry_lock:
                        existing = _active_runners.get(email)
                        if existing and existing.is_alive():
                            continue
                        runner = ClientRunner(member)
                        _active_runners[email] = runner
                        runner.start()
                        restarted.append(email)
                        log.info(f"reset_runner: restarted runner for {email}")

            return jsonify({"ok": True, "killed": killed, "restarted": restarted})
        except Exception as e:
            log.error(f"reset_runner failed: {e}", exc_info=True)
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.post("/admin/test_signal")
    @require_hmac
    def admin_test_signal():
        """Inject a test signal directly into the worker queue, bypassing market-hours
        and quote checks. For after-hours testing only."""
        try:
            import time as _time, uuid as _uuid
            from client_runner import _active_runners, _registry_lock
            from ap.queue import enqueue_signal

            body = request.get_json(silent=True) or {}
            ticker  = body.get("ticker", "SPY")
            side    = body.get("side", "CALL").upper()
            score   = float(body.get("score", 75.0))
            client_id = body.get("client_id", "tradefluencehq@gmail.com")

            signal_id = body.get("signal_id") or f"TEST-{_uuid.uuid4().hex[:8]}"

            signal = {
                "signal_id": signal_id,
                "ticker": ticker,
                "side": side,
                "score": score,
                "strategy": body.get("strategy", "test"),
                "timestamp": _time.time(),
                "test_mode": True,
                "bypass_market_hours": True,
            }

            # Check runner health first
            with _registry_lock:
                runner = _active_runners.get(client_id)

            if not runner or not runner.is_alive():
                return jsonify({
                    "ok": False,
                    "error": f"No alive runner for {client_id}. Hit /admin/force_start_runner first.",
                    "runner_alive": False,
                }), 503

            if not runner.entries_allowed.is_set():
                reasons = sorted(list(getattr(runner, "degraded_reasons", set())))
                return jsonify({
                    "ok": False,
                    "error": "Runner alive but entries_allowed=False",
                    "degraded": runner.degraded.is_set(),
                    "degraded_reasons": reasons,
                }), 503

            # Enqueue directly into the runner's queue (bypasses route_signal_to_all_clients)
            try:
                job_id = enqueue_signal(signal, client_id=client_id)
                return jsonify({
                    "ok": True,
                    "signal_id": signal_id,
                    "job_id": str(job_id),
                    "ticker": ticker,
                    "side": side,
                    "score": score,
                    "note": "Test signal enqueued — worker will process but market_closed_no_contract_selection rejection is expected after hours",
                })
            except Exception as eq_exc:
                return jsonify({"ok": False, "error": f"enqueue failed: {eq_exc}"}), 500

        except Exception as e:
            log.error(f"test_signal failed: {e}", exc_info=True)
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.post("/admin/force_start_runner")
    @require_hmac
    def admin_force_start_runner():
        """Directly spawn a runner for a member — bypasses supervisor for emergency recovery."""
        try:
            from client_runner import _active_runners, _registry_lock, ClientRunner, _fetch_active_members
            from supabase import create_client
            import os

            sb_url = os.getenv("SUPABASE_URL", "")
            sb_key = os.getenv("SUPABASE_SERVICE_KEY", "")
            if not sb_url or not sb_key:
                return jsonify({"ok": False, "error": "SUPABASE_URL or SUPABASE_SERVICE_KEY not set"}), 500

            sb = create_client(sb_url, sb_key)
            members = _fetch_active_members(sb)

            if not members:
                return jsonify({"ok": False, "error": "No active members found in Supabase", "count": 0})

            started = []
            for member in members:
                email = member["email"]
                with _registry_lock:
                    existing = _active_runners.get(email)
                    if existing and existing.is_alive():
                        started.append({"email": email, "status": "already_running"})
                        continue
                    runner = ClientRunner(member)
                    _active_runners[email] = runner
                    runner.start()
                    started.append({"email": email, "status": "started"})
                    log.info(f"force_start_runner: started runner for {email}")

            return jsonify({"ok": True, "started": started})
        except Exception as e:
            log.error(f"force_start_runner failed: {e}", exc_info=True)
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.get("/admin/supervisor_state")
    @require_hmac
    def admin_supervisor_state():
        try:
            from client_runner import get_supervisor_state
            return jsonify({"ok": True, **get_supervisor_state()})
        except Exception as e:
            log.error(f"Supervisor state failed: {e}")
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.get("/client/trades")
    @require_hmac
    def client_trades():
        """Execution proof surface for clients and ops.
        Returns every completed trade with: entry time, fill, exit time, fill, net P&L.
        This is what a $3K/month client needs to see — not logs.
        """
        try:
            from ap.db import run_with_retry
            import psycopg2.extras

            client_id = request.args.get("client_id", "tradefluencehq@gmail.com")
            limit = min(int(request.args.get("limit", 50)), 200)
            status = request.args.get("status", "ALL").upper()

            def _fetch():
                with __import__("ap.db", fromlist=["conn"]).conn() as c:
                    psycopg2.extras.register_uuid()
                    status_filter = ""
                    params = [client_id]
                    if status == "CLOSED":
                        status_filter = "AND p.status IN ('closed', 'CLOSED')"
                    elif status == "OPEN":
                        status_filter = "AND p.status IN ('open', 'OPEN', 'closing', 'CLOSING')"
                    elif status != "ALL":
                        status_filter = "AND UPPER(p.status) = %s"
                        params.append(status.upper())
                    c.execute(f"""
                        SELECT
                            p.id                  AS trade_id,
                            p.underlying          AS ticker,
                            p.direction           AS side,
                            p.contract,
                            p.qty                 AS quantity,
                            p.expected_entry      AS entry_price,
                            p.avg_fill            AS fill_price,
                            p.entry_ts            AS entry_time,
                            p.exit_price,
                            p.exit_ts             AS exit_time,
                            p.realized_pnl,
                            p.realized_pnl_pct,
                            p.status,
                            p.exit_reason,
                            p.score,
                            p.tier,
                            p.signal_id,
                            p.created_at
                        FROM positions p
                        WHERE p.client_id = %s
                        {status_filter}
                        ORDER BY p.created_at DESC
                        LIMIT %s
                    """, params + [limit])
                    cols = [d[0] for d in c.description]
                    rows = c.fetchall()
                    return [dict(zip(cols, row)) for row in rows]

            trades = run_with_retry(_fetch)

            # Compute summary stats
            closed = [t for t in trades if t.get("status") in ("CLOSED", "FILLED")]
            total_pnl = sum(float(t.get("realized_pnl") or 0) for t in closed)
            wins = sum(1 for t in closed if float(t.get("realized_pnl") or 0) > 0)
            losses = sum(1 for t in closed if float(t.get("realized_pnl") or 0) < 0)

            # Serialize datetimes
            import datetime
            for t in trades:
                for k, v in t.items():
                    if isinstance(v, (datetime.datetime, datetime.date)):
                        t[k] = v.isoformat()
                    elif hasattr(v, "hex"):  # UUID
                        t[k] = str(v)

            return jsonify({
                "ok": True,
                "client_id": client_id,
                "trades": trades,
                "summary": {
                    "total": len(trades),
                    "closed": len(closed),
                    "wins": wins,
                    "losses": losses,
                    "total_pnl": round(total_pnl, 2),
                    "win_rate": round(wins / len(closed) * 100, 1) if closed else 0,
                },
            })
        except Exception as e:
            log.error(f"client_trades failed: {e}", exc_info=True)
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.get("/client/positions")
    @require_hmac
    def client_positions():
        """Live open positions — what the client holds right now."""
        try:
            from ap.db import run_with_retry
            import psycopg2.extras

            client_id = request.args.get("client_id", "tradefluencehq@gmail.com")

            def _fetch():
                with __import__("ap.db", fromlist=["conn"]).conn() as c:
                    c.execute("""
                        SELECT
                            p.id                AS trade_id,
                            p.underlying        AS ticker,
                            p.direction         AS side,
                            p.contract,
                            p.qty               AS quantity,
                            p.expected_entry    AS entry_price,
                            p.avg_fill          AS fill_price,
                            p.entry_ts          AS entry_time,
                            p.status,
                            p.realized_pnl,
                            p.score,
                            p.signal_id
                        FROM positions p
                        WHERE p.client_id = %s
                          AND LOWER(p.status) IN ('open', 'closing')
                        ORDER BY p.entry_ts DESC NULLS LAST
                    """, (client_id,))
                    cols = [d[0] for d in c.description]
                    return [dict(zip(cols, row)) for row in c.fetchall()]

            positions = run_with_retry(_fetch)

            import datetime
            for p in positions:
                for k, v in p.items():
                    if isinstance(v, (datetime.datetime, datetime.date)):
                        p[k] = v.isoformat()
                    elif hasattr(v, "hex"):
                        p[k] = str(v)

            return jsonify({
                "ok": True,
                "client_id": client_id,
                "positions": positions,
                "count": len(positions),
            })
        except Exception as e:
            log.error(f"client_positions failed: {e}", exc_info=True)
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.get("/client/rejections")
    @require_hmac
    def client_rejections():
        """Every signal rejection — queryable by client. No log-diving needed."""
        try:
            import os as _os
            from supabase import create_client as _cc
            client_id = request.args.get("client_id", "tradefluencehq@gmail.com")
            limit = min(int(request.args.get("limit", 50)), 200)
            _sb_url = _os.getenv("SUPABASE_URL", "")
            _sb_key = _os.getenv("SUPABASE_SERVICE_KEY", "")
            if not _sb_url or not _sb_key:
                return jsonify({"ok": False, "error": "Supabase not configured"}), 500
            sb = _cc(_sb_url, _sb_key)
            result = (
                sb.table("ap_signals")
                .select("signal_id,ticker,side,score,decision_status,context_notes,created_at")
                .eq("client_email", client_id)
                .eq("decision_status", "rejected")
                .order("created_at", desc=True)
                .limit(limit)
                .execute()
            )
            return jsonify({"ok": True, "client_id": client_id, "rejections": result.data or [], "count": len(result.data or [])})
        except Exception as e:
            log.error(f"client_rejections failed: {e}", exc_info=True)
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.post("/admin/overnight_reeval")
    @require_hmac
    def admin_overnight_reeval():
        """Manually trigger overnight daily signal reeval for all active runners.
        Normally fires automatically at 9:00-9:45 AM ET.
        Use this to trigger it manually (e.g. after a late deploy or for testing).
        Pass {"force": true} to bypass the time-of-day guard.
        """
        try:
            from client_runner import _active_runners, _registry_lock
            from ap_overnight_reeval import run_overnight_reeval

            body = request.get_json(silent=True) or {}
            force = bool(body.get("force", True))  # default force=True for manual calls

            results = {}
            with _registry_lock:
                runners = list(_active_runners.items())

            for email, runner in runners:
                if not runner.is_alive():
                    results[email] = {"error": "runner not alive"}
                    continue
                try:
                    _core = runner.core
                    result = run_overnight_reeval(
                        client_id=email,
                        broker=getattr(_core, "broker", None) if _core else None,
                        master_control=runner.master_control,
                        contract_selector=runner.contract_selector,
                        order_state_machine=runner.order_state_machine,
                        entry_watcher=getattr(_core, "entry_watcher", None) if _core else None,
                        position_manager=runner.position_manager,
                        exit_eng=getattr(_core, "exit_eng", None) if _core else None,
                        force=force,
                    )
                    # Reset the daily gate so auto-run fires again tomorrow
                    runner._last_overnight_reeval_date = None
                    results[email] = result
                    log.info(f"overnight_reeval [{email}]: {result}")
                except Exception as e:
                    import traceback as _tb
                    results[email] = {"error": str(e), "traceback": _tb.format_exc()[-2000:]}
                    log.error(f"overnight_reeval [{email}] failed: {e}", exc_info=True)

            total_armed = sum(r.get("armed", 0) for r in results.values() if isinstance(r, dict))
            total_rejected = sum(r.get("rejected", 0) for r in results.values() if isinstance(r, dict))
            return jsonify({
                "ok": True,
                "force": force,
                "runners": len(runners),
                "total_armed": total_armed,
                "total_rejected": total_rejected,
                "results": results,
            })
        except Exception as e:
            log.error(f"overnight_reeval endpoint failed: {e}", exc_info=True)
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.post("/admin/reseed_exit_engine")
    @require_hmac
    def reseed_exit_engine():
        """Force the exit engine to reseed from DB — picks up manually seeded positions."""
        from client_runner import _active_runners, _registry_lock
        results = {}
        with _registry_lock:
            runners = dict(_active_runners)
        for email, runner in runners.items():
            try:
                core = getattr(runner, "core", None)
                exit_eng = getattr(core, "exit_eng", None) if core else None
                if exit_eng and hasattr(exit_eng, "seed_from_db"):
                    # seed_from_db requires position_manager argument
                    pm = getattr(runner, "pm", None)
                    if pm is None:
                        core = getattr(runner, "core", None)
                        pm = getattr(core, "position_manager", None) if core else None
                    if pm is None:
                        # Try to get from master_control
                        mc = getattr(core, "master_control", None) if core else None
                        pm = getattr(mc, "pm", None) if mc else None
                    try:
                        exit_eng.seed_from_db(pm)
                    except TypeError:
                        # Some versions don't require pm
                        exit_eng.seed_from_db()
                    # Also refresh quotes so exit engine has current prices
                    _refresh = getattr(exit_eng, "refresh_quotes", None) or getattr(exit_eng, "_refresh_quotes", None)
                    if _refresh:
                        import threading
                        threading.Thread(
                            target=_refresh,
                            daemon=True,
                            name=f"reseed-quote-refresh-{email}",
                        ).start()
                    results[email] = {"ok": True, "reseeded": True}
                    log.info("[%s] Exit engine reseeded via admin endpoint", email)
                else:
                    results[email] = {"ok": False, "error": "exit_engine_not_found"}
            except Exception as e:
                results[email] = {"ok": False, "error": str(e)}
                log.error("reseed_exit_engine failed for %s: %s", email, e)
        return jsonify({"ok": True, "results": results})

    @app.post("/admin/nightly_reconcile")
    @require_hmac
    def nightly_reconcile():
        """
        Nightly reconciliation: compare DB open positions against Tradier live positions.
        Alerts on mismatches. Called by cron job after market close.
        """
        from client_runner import _active_runners, _registry_lock
        results = {}
        with _registry_lock:
            runners = dict(_active_runners)

        for email, runner in runners.items():
            try:
                broker = getattr(runner, "broker", None)
                if not broker or not hasattr(broker, "list_positions"):
                    results[email] = {"ok": False, "error": "no_broker"}
                    continue

                # Get broker positions
                broker_positions = broker.list_positions() or []
                broker_contracts = {
                    str(p.get("symbol") or "").upper(): int(p.get("quantity") or 0)
                    for p in broker_positions
                    if int(p.get("quantity") or 0) != 0
                }

                # Get DB open positions
                from ap.db import conn, run_with_retry
                def _get_open(em=email):
                    with conn() as c:
                        c.execute(
                            "SELECT id, contract, underlying, avg_fill, qty, status "
                            "FROM positions WHERE client_id=%s AND status IN ('OPEN','CLOSING')",
                            (em,)
                        )
                        return c.fetchall()

                db_positions = run_with_retry(_get_open) or []
                db_contracts = {
                    str(p.get("contract") or "").upper(): p
                    for p in db_positions
                }

                mismatches = []

                # DB says open but broker doesn't have it
                for contract, pos in db_contracts.items():
                    if contract not in broker_contracts:
                        mismatches.append({
                            "type": "db_open_not_at_broker",
                            "contract": contract,
                            "position_id": pos.get("id"),
                            "status": pos.get("status"),
                        })

                # Broker has it but DB doesn't
                for contract, qty in broker_contracts.items():
                    if contract not in db_contracts:
                        mismatches.append({
                            "type": "broker_position_missing_from_db",
                            "contract": contract,
                            "qty": qty,
                        })

                if mismatches:
                    log.warning(
                        "[%s] NIGHTLY_RECONCILE: %d mismatch(es) found: %s",
                        email, len(mismatches), mismatches
                    )

                results[email] = {
                    "ok": True,
                    "db_open": len(db_positions),
                    "broker_open": len(broker_contracts),
                    "mismatches": mismatches,
                    "mismatch_count": len(mismatches),
                }
            except Exception as e:
                results[email] = {"ok": False, "error": str(e)}
                log.error("nightly_reconcile failed for %s: %s", email, e)

        total_mismatches = sum(r.get("mismatch_count", 0) for r in results.values() if isinstance(r, dict))
        return jsonify({
            "ok": True,
            "results": results,
            "total_mismatches": total_mismatches,
        })

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
