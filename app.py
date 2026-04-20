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
from ap.exit_manager import exit_manager_loop

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

APP_ENV = os.getenv("APP_ENV", "dev").lower().strip()
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
IDEMP_TTL_SECONDS = int(os.getenv("IDEMP_TTL_SECONDS", "300"))

# HMAC time drift (seconds)
HMAC_MAX_SKEW_SECONDS = int(os.getenv("HMAC_MAX_SKEW_SECONDS", "300"))  # ✅ 5 min default

# Threads
THREADS_STARTED = False
THREAD_LOCK = threading.Lock()

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
            log.warning(f"Invalid X-AP-Timestamp: {ts}")
            ts_i = None

        if ts_i is not None:
            # Anti-replay / drift window
            if abs(int(time.time()) - ts_i) > HMAC_MAX_SKEW_SECONDS:
                log.warning(f"Timestamp out of range: {ts_i}")
            else:
                msg = str(ts_i).encode("utf-8") + b"." + raw
                expected = _hmac_hex(SIGNING_SECRET, msg)
                if hmac.compare_digest(expected, sig):
                    log.debug("✅ HMAC verified (timestamped scheme)")
                    return True
                else:
                    log.warning("HMAC timestamped scheme failed (signature mismatch)")

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
            initial_equity=100000.0,
        )
        log.info("✅ Default client created")
    else:
        log.info("✅ Default client exists")
        # =============================================
    # DEBUG (TEMP) - REMOVE LATER
    # =============================================

    @app.get("/debug/threads")
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
        return jsonify({"ok": True, "kill_switch": True})

    @app.post("/kill_switch/off")
    @require_hmac
    def kill_off():
        log.info("🟢 KILL SWITCH DISABLED")
        update_state({"kill_switch": False}, client_id=DEFAULT_CLIENT_ID)
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
