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

# ── Angel Precision Intelligence infrastructure ───────────────────────────────
try:
    from ap_bootstrap import bootstrap as _ap_bootstrap
    from ap_health_endpoints import health_bp as _health_bp
    _AP_INFRA_AVAILABLE = True
except Exception as _ap_infra_err:
    _AP_INFRA_AVAILABLE = False
    _ap_bootstrap = None
    _health_bp = None
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
# C3: kill switch is a safety control and is frequently changed OUT OF BAND
# (Supabase SQL, dashboard backend) without calling /kill_switch/off, so the
# cache cannot be explicitly invalidated on those paths. Keep this short so a
# lifted kill switch takes effect within seconds, not within _CACHE_TTL.
_KILL_SWITCH_CACHE_TTL = float(os.getenv("KILL_SWITCH_CACHE_TTL", "3.0"))

# HIGH-007: supervisor started in gunicorn post_worker_init hook ONLY.
# Do NOT call start_multi_client_supervisor() here -- this is module import scope.
# gunicorn.conf.py post_worker_init is the sole authoritative startup path.

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

# AUDIT P1-6: log SHA8 fingerprints of every secret at startup. Never log values.
# Lets you verify on Render that rotation actually took effect.
def _fp(val: str | bytes) -> str:
    if not val:
        return "<missing>"
    b = val.encode() if isinstance(val, str) else val
    return hashlib.sha256(b).hexdigest()[:8]

_SECRET_FP_SUMMARY = {
    "SIGNING_SECRET":       _fp(SIGNING_SECRET),
    "ADMIN_API_KEY":        _fp(os.getenv("ADMIN_API_KEY", "")),
    "TRADIER_ACCESS_TOKEN": _fp(os.getenv("TRADIER_ACCESS_TOKEN", "")),
    "SUPABASE_SERVICE_KEY": _fp(os.getenv("SUPABASE_SERVICE_KEY", "")),
    "ENCRYPTION_KEY":       _fp(os.getenv("ENCRYPTION_KEY", "")),
    "JWT_SECRET":           _fp(os.getenv("JWT_SECRET", "")),
}
log.info("SECRET_FINGERPRINTS_SHA8=%s", _SECRET_FP_SUMMARY)

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

# ── Single-client execution worker mode ──────────────────────────────────────
# When CLIENT_ID is set this process is a dedicated Render service for one
# client only (the isolated-per-client architecture). All runner discovery,
# order queries, state reads/writes are pre-filtered to this client.
# If CLIENT_ID is not set, the service runs in multi-client (shared) mode.
CLIENT_ID        = (os.getenv("SINGLE_CLIENT_EMAIL", "") or os.getenv("CLIENT_ID", "")).strip()
POD_ID           = os.getenv("POD_ID", "").strip()
MAX_POD_CLIENTS  = int(os.getenv("MAX_POD_CLIENTS", "5"))
BOT_INSTANCE_ID  = os.getenv("BOT_INSTANCE_ID", CLIENT_ID or POD_ID or "shared").strip()
_BOOT_MODE_LBL   = os.getenv("BOT_MODE", os.getenv("MODE", "paper")).upper()

if CLIENT_ID:
    DEFAULT_CLIENT_ID = CLIENT_ID
    log.info(
        "\n" + "=" * 70 + "\n"
        "  ANGEL PRECISION — DEDICATED CLIENT EXECUTION WORKER\n"
        "  SINGLE_CLIENT:   %s\n"
        "  BOT_INSTANCE_ID: %s\n"
        "  MODE:            %s\n"
        "  Runs ONLY this client. All writes scoped to this client.\n"
        + "=" * 70,
        CLIENT_ID, BOT_INSTANCE_ID, _BOOT_MODE_LBL,
    )
elif POD_ID:
    log.info(
        "\n" + "=" * 70 + "\n"
        "  ANGEL PRECISION — POD EXECUTION WORKER\n"
        "  POD_ID:          %s\n"
        "  MAX_POD_CLIENTS: %s\n"
        "  BOT_INSTANCE_ID: %s\n"
        "  MODE:            %s\n"
        "  Runs ONLY clients where execution_pod=%s. Fails boot if pod\n"
        "  exceeds MAX_POD_CLIENTS. Live clients require allow_live_trading.\n"
        + "=" * 70,
        POD_ID, MAX_POD_CLIENTS, BOT_INSTANCE_ID, _BOOT_MODE_LBL, POD_ID,
    )
else:
    log.info(
        "ANGEL PRECISION — SHARED MODE | mode=%s | all approved clients "
        "(paper/onboarding/load-test service)", _BOOT_MODE_LBL,
    )

# ============================================================
# SECURITY HELPERS (HMAC + Rate Limit + Idempotency)
# ============================================================

def _client_ip() -> str:
    xff = request.headers.get("X-Forwarded-For", "")
    return (xff.split(",")[0].strip() if xff else request.remote_addr) or "unknown"


# AUDIT P1-9: track last sweep so we can periodically drop empty buckets and
# prevent _RATE dict from growing forever on long-tail client IDs / IPs.
# AUDIT FOLLOWUP: needs a lock because we now iterate the dict during sweep.
# Without the lock, gunicorn's 4 gthread workers can mutate the dict while
# another thread iterates -> RuntimeError: dictionary changed size during iteration.
_RATE_LOCK = threading.Lock()
_RATE_LAST_SWEEP: float = 0.0
_RATE_SWEEP_INTERVAL = 60.0  # seconds


def _rate_limited(bucket: str) -> bool:
    global _RATE_LAST_SWEEP
    now = time.time()
    with _RATE_LOCK:
        q = _RATE[bucket]
        while q and (now - q[0] > 60):
            q.popleft()
        # Periodically prune empty buckets so the dict can't leak unbounded keys.
        # Snapshot keys with list(...) BEFORE iterating to avoid "dict changed
        # size during iteration" if a sibling thread were to mutate concurrently
        # (we hold the lock, but list() is also cheap and bulletproof).
        if now - _RATE_LAST_SWEEP > _RATE_SWEEP_INTERVAL:
            _RATE_LAST_SWEEP = now
            for k in list(_RATE.keys()):
                if not _RATE[k]:
                    _RATE.pop(k, None)
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
        # AUDIT P1-9: cleanup on writes too, so a write-heavy workload that never
        # reads (signals never replayed) can't leak unbounded entries.
        _idem_cleanup()
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

    # ── Bootstrap Angel Precision Intelligence infrastructure ─────────────────
    # Registers all organs, wires kill switch into health registry,
    # starts health sweep thread. Runs once — idempotent.
    if _AP_INFRA_AVAILABLE and _ap_bootstrap:
        try:
            # Wire Discord alert function if available in this scope.
            _discord_alert_fn = None
            try:
                from ap.notify import send_discord_alert as _discord_alert_fn
            except Exception:
                pass
            _ap_bootstrap(alert_fn=_discord_alert_fn)
            log.info("✅ AP Intelligence infrastructure bootstrapped")
        except Exception as _boot_err:
            log.error("AP bootstrap failed (non-fatal): %s", _boot_err)

    if _AP_INFRA_AVAILABLE and _health_bp:
        try:
            app.register_blueprint(_health_bp)
            log.info("✅ Health endpoints registered at /health/*")
        except Exception as _bp_err:
            log.error("Health blueprint registration failed (non-fatal): %s", _bp_err)

    # PR #27: telemetry endpoint exposes the Phase 6 entry_telemetry
    # projection over HTTP so the dashboard backend can consume it
    # without re-implementing the order.meta -> 23-field projection in JS.
    # All routes are read-only and auth-gated (X-Admin-Key or
    # X-Telemetry-Key). Registration is wrapped in try/except so a
    # blueprint import failure cannot block app boot.
    try:
        from ap.telemetry_api import telemetry_bp as _telemetry_bp
        app.register_blueprint(_telemetry_bp)
        log.info("✅ Telemetry endpoints registered at /telemetry/*")
    except Exception as _tbp_err:
        log.error(
            "Telemetry blueprint registration failed (non-fatal): %s",
            _tbp_err,
        )

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

    @app.post("/admin/client/<client_id>/pause_entries")
    @require_hmac
    def admin_client_pause_entries(client_id):
        """H5: pause ONE client's new entries. Exits keep running. Other
        clients unaffected (each runner has its own master_control). Operator
        control surfaced in the admin dashboard client section."""
        cid = (client_id or "").strip()
        if not cid:
            return jsonify({"ok": False, "error": "missing_client_id"}), 400
        log.info("⏸️ CLIENT ENTRIES PAUSED | %s", cid)
        update_state({"entries_paused": True}, client_id=cid)
        return jsonify({"ok": True, "client_id": cid, "entries_paused": True})

    @app.post("/admin/client/<client_id>/resume_entries")
    @require_hmac
    def admin_client_resume_entries(client_id):
        """H5: resume a paused client's entries."""
        cid = (client_id or "").strip()
        if not cid:
            return jsonify({"ok": False, "error": "missing_client_id"}), 400
        log.info("▶️ CLIENT ENTRIES RESUMED | %s", cid)
        update_state({"entries_paused": False}, client_id=cid)
        return jsonify({"ok": True, "client_id": cid, "entries_paused": False})

    @app.get("/admin/client/<client_id>/control_state")
    @require_hmac
    def admin_client_control_state(client_id):
        """H5/H6: report a client's live control state for the dashboard."""
        cid = (client_id or "").strip()
        if not cid:
            return jsonify({"ok": False, "error": "missing_client_id"}), 400
        try:
            st = load_state(client_id=cid) or {}
        except Exception as e:
            return jsonify({"ok": False, "error": f"state_read_failed:{e}"}), 500
        return jsonify({
            "ok": True,
            "client_id": cid,
            "entries_paused": bool(st.get("entries_paused", False)),
            "kill_switch": bool(st.get("kill_switch", False)),
            "mode": st.get("mode", "PAPER"),
            "realized_pnl_today": float(st.get("realized_pnl_today", 0.0) or 0.0),
            "trades_taken_today": int(st.get("trades_taken_today", 0) or 0),
        })

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
        # C3: the kill switch is a SAFETY control. External state changes
        # (Supabase SQL, dashboard backend writing client_state directly)
        # do NOT call /kill_switch/off, so they cannot clear this cache.
        # A 30s TTL means up to 30s of dropped signals after recovery.
        # Use a short dedicated TTL so external clears take effect fast.
        _now = time.monotonic()
        with _SIGNAL_CACHE_LOCK:
            _ks_cached = _kill_switch_cache.get(client_id)
        if _ks_cached and _ks_cached[2] > _now:
            _ks, _mode = _ks_cached[0], _ks_cached[1]
        else:
            _st = load_state(client_id=client_id)
            _ks, _mode = _st.get("kill_switch"), _st.get("mode")
            with _SIGNAL_CACHE_LOCK:
                _kill_switch_cache[client_id] = (_ks, _mode, _now + _KILL_SWITCH_CACHE_TTL)
        if _ks or _mode == "READ_ONLY":
            # C2: do NOT write this into the idempotency cache. bot_in_read_only
            # is a RECOVERABLE rejection — the kill switch can be lifted seconds
            # later. If we cache it under the signal_id, the scanner's retry of
            # the SAME signal_id returns this stale 403 for IDEMP_TTL_SECONDS
            # (300s) even though the bot is live again. Only terminal successes
            # may be cached. Leave idem_key unset so retries are reprocessed.
            payload = {"ok": False, "error": "bot_in_read_only"}
            return jsonify(payload), 403

        # Fix 4: synchronous durable enqueue — 202 only after queue write succeeds
        _sig_id = str(body.get("signal_id") or uuid.uuid4())
        body["signal_id"] = _sig_id

        try:
            enqueued = route_signal_to_all_clients(body)
            if not enqueued:
                # NOT routed — dropped. Do not call this "routed" in logs.
                # This is the exact symptom of the broken-deploy / zero-runners
                # state. Log CRITICAL so it is impossible to miss.
                log.critical(
                    "SIGNAL DROPPED — NO ACTIVE RUNNERS | %s %s score=%s "
                    "sig=%s enqueued=0. Trading is DOWN: approved members may "
                    "exist but zero runners started (schema mismatch / boot "
                    "failure). Check /execution/health and run migrations.",
                    body.get("ticker"), body.get("side"),
                    body.get("score"), _sig_id,
                )
                payload = {"ok": False, "error": "no_active_clients",
                           "signal_id": _sig_id, "enqueued": 0}
                return jsonify(payload), 503
            log.info(
                "Signal routed: %s %s score=%s sig=%s enqueued=%s",
                body.get("ticker"), body.get("side"),
                body.get("score"), _sig_id, enqueued,
            )
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

    @app.get("/admin/operator/signal-ledger")
    @require_hmac
    def admin_operator_signal_ledger():
        """Item 4 — READ-ONLY multi-account signal ledger.

        One row per (canonical signal, client, ENTRY order) from the
        ap_multi_account_signal_ledger view. Answers "what happened on each
        account for signal X?". No mutations — SELECT only.

        Query params (all optional):
          client_id, symbol, canonical_signal_id, status, bucket
          since_hours (default 24), limit (default 500, max 2000)
        """
        try:
            client_id   = (request.args.get("client_id") or "").strip()
            symbol      = (request.args.get("symbol") or "").strip().upper()
            canon_id    = (request.args.get("canonical_signal_id") or "").strip()
            status      = (request.args.get("status") or "").strip()
            bucket      = (request.args.get("bucket") or "").strip()
            try:
                since_hours = max(1, min(int(request.args.get("since_hours", 24)), 24 * 30))
            except (TypeError, ValueError):
                since_hours = 24
            try:
                limit = max(1, min(int(request.args.get("limit", 500)), 2000))
            except (TypeError, ValueError):
                limit = 500

            # Parameterized WHERE — never string-interpolate user input.
            where = ["order_created_ts > NOW() - (%s || ' hours')::interval"]
            params: list = [str(since_hours)]
            if client_id:
                where.append("client_id = %s");            params.append(client_id)
            if symbol:
                where.append("UPPER(symbol) = %s");         params.append(symbol)
            if canon_id:
                where.append("canonical_signal_id = %s");   params.append(canon_id)
            if status:
                where.append("order_status = %s");          params.append(status)
            if bucket:
                where.append("ledger_bucket = %s");         params.append(bucket)

            sql = (
                "SELECT * FROM ap_multi_account_signal_ledger "
                "WHERE " + " AND ".join(where) +
                " ORDER BY canonical_signal_id, order_created_ts DESC "
                "LIMIT %s"
            )
            params.append(limit)

            def _fetch():
                with conn() as c:
                    cur = c.execute(sql, tuple(params))
                    cols = [d[0] for d in cur.description]
                    return [dict(zip(cols, r)) for r in cur.fetchall()]

            from ap.db import run_with_retry
            rows = run_with_retry(_fetch)

            # Summary counts by bucket.
            buckets = {
                "pending_trigger_no_broker": 0,
                "broker_submitted":          0,
                "filled_or_partial":         0,
                "terminal_no_fill":          0,
                "no_order_for_client":       0,
            }
            _bmap = {
                "PENDING_TRIGGER_NO_BROKER": "pending_trigger_no_broker",
                "BROKER_SUBMITTED":          "broker_submitted",
                "FILLED_OR_PARTIAL":         "filled_or_partial",
                "TERMINAL_NO_FILL":          "terminal_no_fill",
                "NO_ORDER_FOR_CLIENT":       "no_order_for_client",
            }
            _clients, _signals = set(), set()
            for r in rows:
                _b = _bmap.get(r.get("ledger_bucket"))
                if _b:
                    buckets[_b] += 1
                if r.get("client_id"):
                    _clients.add(r["client_id"])
                if r.get("canonical_signal_id"):
                    _signals.add(r["canonical_signal_id"])

            return jsonify({
                "ok": True,
                "rows": rows,
                "summary": {
                    "total_rows":    len(rows),
                    "clients_seen":  len(_clients),
                    "signals_seen":  len(_signals),
                    **buckets,
                },
            })
        except Exception as e:
            log.error(f"signal-ledger failed: {e}")
            return jsonify({"ok": False, "error": str(e), "rows": [], "summary": {}}), 500




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
            # AUDIT P0-1: client_id must be explicit. NO hardcoded fallback.
            # Previously defaulted to a specific client email which leaked their data
            # when any caller forgot the param.
            client_id = (body.get("client_id") or "").strip()
            if not client_id:
                return jsonify({
                    "ok": False,
                    "error": "client_id required in body (no default — per-client routing is explicit).",
                }), 400

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

            # AUDIT P0-1: client_id must be explicit. No hardcoded default.
            client_id = (request.args.get("client_id") or "").strip()
            if not client_id:
                return jsonify({
                    "ok": False,
                    "error": "client_id query parameter required",
                }), 400
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

            # AUDIT P0-1: client_id must be explicit. No hardcoded default.
            client_id = (request.args.get("client_id") or "").strip()
            if not client_id:
                return jsonify({
                    "ok": False,
                    "error": "client_id query parameter required",
                }), 400

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
            # AUDIT P0-1: client_id must be explicit. No hardcoded default.
            client_id = (request.args.get("client_id") or "").strip()
            if not client_id:
                return jsonify({
                    "ok": False,
                    "error": "client_id query parameter required",
                }), 400
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


    @app.get("/client/orders")
    @require_hmac
    def client_orders():
        """Order lifecycle ledger for a client.

        AUDIT P0-3: this endpoint was previously called as `/client/me/orders` by the
        dashboard backend but did not exist on the bot — every call 404'd silently
        and the dashboard ledger panel was permanently empty. Now implemented.

        Returns: order_id, broker_order_id, symbol, contract, side, kind (ENTRY/EXIT),
                 qty, filled_qty, avg_fill, status, created_ts, updated_ts.
        Used by /api/trading/ledger on the dashboard backend.
        """
        try:
            from ap.db import run_with_retry
            import psycopg2.extras

            client_id = (request.args.get("client_id") or "").strip()
            if not client_id:
                return jsonify({
                    "ok": False,
                    "error": "client_id query parameter required",
                }), 400

            limit = min(int(request.args.get("limit", 100)), 500)
            status_filter_val = (request.args.get("status") or "").strip().upper()

            def _fetch():
                with __import__("ap.db", fromlist=["conn"]).conn() as c:
                    psycopg2.extras.register_uuid()
                    params = [client_id]
                    extra_where = ""
                    if status_filter_val:
                        extra_where = "AND UPPER(status) = %s"
                        params.append(status_filter_val)
                    c.execute(
                        f"""
                        SELECT
                            id                AS order_id,
                            broker_order_id,
                            symbol,
                            contract,
                            side,
                            kind,
                            qty,
                            filled_qty,
                            avg_fill,
                            status,
                            created_ts,
                            updated_ts,
                            position_id
                        FROM orders
                        WHERE client_id = %s
                        {extra_where}
                        ORDER BY created_ts DESC
                        LIMIT %s
                        """,
                        params + [limit],
                    )
                    cols = [d[0] for d in c.description]
                    return [dict(zip(cols, row)) for row in c.fetchall()]

            orders = run_with_retry(_fetch)

            # Serialize timestamps + UUIDs for JSON
            import datetime
            for o in orders:
                for k, v in o.items():
                    if isinstance(v, (datetime.datetime, datetime.date)):
                        o[k] = v.isoformat()
                    elif hasattr(v, "hex") and not isinstance(v, (bytes, bytearray)):
                        o[k] = str(v)

            return jsonify({
                "ok": True,
                "client_id": client_id,
                "count":     len(orders),
                "orders":    orders,
            })
        except Exception as e:
            log.error(f"client_orders failed: {e}", exc_info=True)
            return jsonify({"ok": False, "error": str(e)}), 500


    @app.post("/admin/reset_dedup")
    @require_hmac
    def reset_dedup():
        """Force reset the master control dedup cache — clears stale signal blocks."""
        from client_runner import _active_runners, _registry_lock
        results = {}
        with _registry_lock:
            runners = dict(_active_runners)
        for email, runner in runners.items():
            try:
                core = getattr(runner, "core", None)
                mc = getattr(core, "master_control", None) if core else None
                if mc and hasattr(mc, "reset_session"):
                    mc.reset_session(client_id=email)
                    results[email] = {"ok": True, "cleared": True}
                    log.info("[%s] Dedup cache cleared via admin endpoint", email)
                else:
                    results[email] = {"ok": False, "error": "master_control_not_found"}
            except Exception as e:
                results[email] = {"ok": False, "error": str(e)}
        return jsonify({"ok": True, "results": results})

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

# =========================================================================
# DEBUG + ADMIN: quote monitor observability + manual flatten
# =========================================================================
# These routes are registered after create_app() so the existing stable app body
# remains untouched while still exposing fleet-level quote-monitor telemetry and
# manual emergency flatten controls.
import os as _os_admin
import hmac as _hmac_admin
import logging as _logging_admin
from functools import wraps as _admin_wraps

admin_log = _logging_admin.getLogger("admin.controls")

ADMIN_API_KEY = _os_admin.getenv("ADMIN_API_KEY", "")
ALLOWED_ADMIN_IPS = {
    ip.strip()
    for ip in _os_admin.getenv("ALLOWED_ADMIN_IPS", "").split(",")
    if ip.strip()
}


def _admin_client_ip() -> str:
    return request.headers.get(
        "X-Forwarded-For",
        request.remote_addr or "",
    ).split(",")[0].strip()


def _require_admin(fn):
    @_admin_wraps(fn)
    def _wrap(*a, **kw):
        if not ADMIN_API_KEY:
            return jsonify({"ok": False, "error": "ADMIN_API_KEY not configured"}), 503

        ip = _admin_client_ip()

        if ALLOWED_ADMIN_IPS and ip not in ALLOWED_ADMIN_IPS:
            admin_log.critical("ADMIN_DENIED_IP path=%s ip=%s", request.path, ip)
            return jsonify({"ok": False, "error": "forbidden_ip"}), 403

        supplied = request.headers.get("X-Admin-Key", "")

        if not _hmac_admin.compare_digest(supplied, ADMIN_API_KEY):
            admin_log.critical("ADMIN_AUTH_FAIL path=%s ip=%s", request.path, ip)
            return jsonify({"ok": False, "error": "unauthorized"}), 401

        return fn(*a, **kw)

    return _wrap


def _iter_client_runners():
    """
    Yield (client_id, runner) for every active client runner.

    Supports both registry styles used across this codebase:
      - CLIENT_RUNNERS: dict[str, Runner]
      - _active_runners + _registry_lock: dict[str, Runner]
    """
    # Preferred explicit registry if present.
    try:
        from client_runner import CLIENT_RUNNERS  # type: ignore
        for cid, runner in list(CLIENT_RUNNERS.items()):
            yield cid, runner
        return
    except Exception as e:
        admin_log.debug("CLIENT_RUNERS unavailable; falling back to _active_runners: %s", e)

    # Current runner registry used by this app's existing admin routes.
    try:
        from client_runner import _active_runners, _registry_lock
        with _registry_lock:
            runners = list(_active_runners.items())
        for cid, runner in runners:
            yield cid, runner
        return
    except Exception as e:
        admin_log.error("runner registry unavailable: %s", e)
        return


def _get_quote_monitor(runner):
    # Monitors may be hung directly off the runner.
    qm = getattr(runner, "quote_monitor", None)
    if qm is not None:
        return qm

    # Or hung off the exit engine.
    ee = _get_exit_engine(runner)
    if ee is not None:
        return getattr(ee, "quote_monitor", None) or getattr(ee, "_quote_monitor", None)

    return None


def _get_exit_engine(runner):
    # Direct runner attribute.
    ee = getattr(runner, "exit_engine", None)
    if ee is not None:
        return ee

    # Core-owned engine variants used elsewhere in this app.
    core = getattr(runner, "core", None)
    if core is not None:
        for name in ("exit_eng", "exit_engine", "exiteng"):
            ee = getattr(core, name, None)
            if ee is not None:
                return ee

    # Last-resort common aliases.
    for name in ("exit_eng", "exiteng"):
        ee = getattr(runner, name, None)
        if ee is not None:
            return ee

    return None


def _call_flatten(ee, reason: str, force: bool = True):
    """
    Attempts common flatten method names.
    force=True is important so manual flatten can bypass quote gates.
    """
    for name in ("emergency_flatten", "flatten_all", "close_all_positions", "flatten"):
        fn = getattr(ee, name, None)
        if not callable(fn):
            continue

        try:
            varnames = getattr(getattr(fn, "__code__", None), "co_varnames", ())
            kwargs = {}

            if "reason" in varnames:
                kwargs["reason"] = reason
            if "force" in varnames:
                kwargs["force"] = force
            if "bypass_quote_gate" in varnames:
                kwargs["bypass_quote_gate"] = force
            if "emergency_override_quote_gate" in varnames:
                kwargs["emergency_override_quote_gate"] = force

            result = fn(**kwargs) if kwargs else fn()

            return True, name, None, result

        except Exception as e:
            return False, name, str(e), None

    return False, None, "no flatten method on exit_engine", None


@app.get("/debug/quote_metrics")
@_require_admin
def debug_quote_metrics():
    target = request.args.get("client_id")
    out = {}

    for cid, runner in _iter_client_runners():
        if target and cid != target:
            continue

        qm = _get_quote_monitor(runner)
        if qm is None:
            out[cid] = {"ok": False, "error": "no quote_monitor"}
            continue

        try:
            out[cid] = {
                "ok": True,
                "alive": qm.is_alive() if hasattr(qm, "is_alive") else None,
                "healthy": qm.is_healthy() if hasattr(qm, "is_healthy") else None,
                "metrics": qm.metrics_snapshot() if hasattr(qm, "metrics_snapshot") else {},
                "health": qm.health_snapshot() if hasattr(qm, "health_snapshot") else [],
            }
        except Exception as e:
            out[cid] = {"ok": False, "error": str(e)}

    return jsonify({"ok": True, "clients": out})


@app.get("/debug/quote_health")
@_require_admin
def debug_quote_health():
    order = {"fresh": 0, "degraded": 1, "stale": 2, "blind": 3}
    result = []

    for cid, runner in _iter_client_runners():
        qm = _get_quote_monitor(runner)

        if qm is None:
            result.append({"client_id": cid, "ok": False, "error": "no quote_monitor"})
            continue

        try:
            health = qm.health_snapshot() if hasattr(qm, "health_snapshot") else []
            worst = "fresh"

            for h in health:
                state = h.get("state", "fresh")
                if order.get(state, 0) > order.get(worst, 0):
                    worst = state

            result.append({
                "client_id": cid,
                "ok": True,
                "alive": qm.is_alive() if hasattr(qm, "is_alive") else None,
                "healthy": qm.is_healthy() if hasattr(qm, "is_healthy") else None,
                "last_cycle_age_sec": round(qm.last_cycle_age_sec(), 2) if hasattr(qm, "last_cycle_age_sec") else None,
                "position_count": len(health),
                "worst_state": worst,
            })

        except Exception as e:
            result.append({"client_id": cid, "ok": False, "error": str(e)})

    return jsonify({"ok": True, "clients": result})


@app.post("/admin/flatten_client")
@_require_admin
def admin_flatten_client():
    data = request.get_json(silent=True) or {}
    cid = (data.get("client_id") or data.get("email") or "").strip()
    reason = data.get("reason") or "manual_flatten"
    force = bool(data.get("force", True))

    if not cid:
        return jsonify({"ok": False, "error": "client_id required"}), 400

    for run_cid, runner in _iter_client_runners():
        if run_cid != cid:
            continue

        ee = _get_exit_engine(runner)
        if ee is None:
            return jsonify({"ok": False, "error": "no exit_engine for client"}), 500

        ok, method, err, result = _call_flatten(ee, reason=reason, force=force)

        if ok:
            admin_log.critical(
                "MANUAL_FLATTEN_CLIENT client=%s method=%s reason=%s force=%s result=%s ip=%s",
                cid,
                method,
                reason,
                force,
                result,
                _admin_client_ip(),
            )
            return jsonify({
                "ok": True,
                "client_id": cid,
                "method": method,
                "reason": reason,
                "force": force,
                "result": result,
            })

        return jsonify({"ok": False, "error": err, "method": method}), 500

    return jsonify({"ok": False, "error": f"client {cid} not found"}), 404


@app.post("/admin/flatten_all")
@_require_admin
def admin_flatten_all():
    data = request.get_json(silent=True) or {}
    reason = data.get("reason") or "manual_flatten_all"
    confirm = data.get("confirm")

    if confirm != "FLATTEN_ALL":
        return jsonify({
            "ok": False,
            "error": "confirmation required",
            "required_confirm": "FLATTEN_ALL",
        }), 400

    force = bool(data.get("force", True))

    admin_log.critical(
        "GLOBAL_FLATTEN_REQUEST reason=%s force=%s ip=%s",
        reason,
        force,
        _admin_client_ip(),
    )

    results = {}

    for cid, runner in _iter_client_runners():
        ee = _get_exit_engine(runner)

        if ee is None:
            results[cid] = {"ok": False, "error": "no exit_engine"}
            continue

        ok, method, err, result = _call_flatten(ee, reason=reason, force=force)

        if ok:
            results[cid] = {"ok": True, "method": method, "force": force, "result": result}
            admin_log.critical(
                "GLOBAL_FLATTEN_CLIENT client=%s method=%s reason=%s force=%s result=%s",
                cid,
                method,
                reason,
                force,
                result,
            )
        else:
            results[cid] = {"ok": False, "method": method, "error": err}

    return jsonify({
        "ok": True,
        "reason": reason,
        "force": force,
        "clients": results,
    })


@app.post("/admin/position/force_exit/<position_id>")
@_require_admin
def admin_force_exit_position(position_id: str):
    """
    Force-close a specific open position from the admin dashboard.
    Submits IMMEDIATE exit to the exit engine. Also writes proof_trades
    so the close appears in the ledger.
    """
    data      = request.get_json(silent=True) or {}
    client_id = data.get("client_id", "").strip()
    reason    = data.get("reason", "admin_dashboard_force_exit")

    admin_log.warning(
        "FORCE_EXIT_POSITION pos=%s client=%s reason=%s ip=%s",
        position_id, client_id, reason, _admin_client_ip(),
    )

    # Find the right runner
    target_runner = None
    for cid, runner in _iter_client_runners():
        if client_id and cid != client_id:
            continue
        target_runner = runner
        if client_id:
            break

    if not target_runner:
        return jsonify({"ok": False, "error": f"No active runner for {client_id}"}), 404

    ee = _get_exit_engine(target_runner)
    if not ee:
        return jsonify({"ok": False, "error": "Exit engine not available"}), 503

    # Find position in exit engine
    pos = None
    try:
        for p in ee.active_positions():
            if str(getattr(p, "position_id", "")) == position_id:
                pos = p
                break
    except Exception as e:
        return jsonify({"ok": False, "error": f"Position lookup failed: {e}"}), 500

    if pos is None:
        # Not in exit engine — close directly via position manager
        try:
            from ap.position_manager import APPositionManager
            from ap.db import conn, run_with_retry
            def _get_pos():
                with conn() as c:
                    c.execute("SELECT * FROM positions WHERE id=%s AND client_id=%s",
                              (position_id, target_runner.email))
                    return c.fetchone()
            row = run_with_retry(_get_pos)
            if not row:
                return jsonify({"ok": False, "error": "Position not found"}), 404
            row = dict(row)
            entry_px = float(row.get("avg_fill") or row.get("entry_price") or 0)
            pm = APPositionManager(target_runner.email)
            pm.close_position_from_exit_fill(
                position_id=position_id, exit_price=entry_px,
                filled_qty=int(row.get("qty") or 1),
                close_source="admin_force_exit", close_confidence="LOW",
                exit_reason=reason,
            )
            return jsonify({"ok": True, "position_id": position_id,
                            "method": "position_manager_direct",
                            "warning": "Closed at entry price — update exit_option_price in proof_trades manually."})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    # Submit immediate exit decision
    try:
        from ap_exit_engine import ExitDecision
        qty = int(getattr(pos, "quantity_remaining", 0) or getattr(pos, "quantity", 1))
        decision = ExitDecision(
            action="CLOSE_ALL", quantity=qty,
            reason=f"ADMIN FORCE EXIT — {reason}",
            urgency="IMMEDIATE",
            pnl_pct=getattr(pos, "option_pnl_pct", 0.0),
            reason_code="ADMIN_FORCE_EXIT",
        )
        submitted = ee._submit_exit_decision(pos, decision, kill_active=True)
        return jsonify({
            "ok": True, "position_id": position_id,
            "ticker": getattr(pos, "ticker", "?"),
            "method": "exit_engine_immediate",
            "submitted": bool(submitted),
            "current_pnl_pct": round(getattr(pos, "option_pnl_pct", 0.0) * 100, 1),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/health")
def health_basic():
    """Basic liveness check — no auth required."""
    import time as _t
    return jsonify({"ok": True, "status": "healthy", "ts": _t.time()}), 200


@app.get("/execution/health")
@require_hmac
def execution_health():
    """Execution component health — fill monitor, reconciler, order worker.

    CRITICAL READINESS: if approved+subscribed members exist in the DB for
    this service's mode but ZERO runners are active, the service is NOT
    READY (returns ok=false, http 503). This is the exact failure that hid
    the broken pod deploy: app reported live while no runners existed and
    every signal was dropped. Health must scream when that happens.
    """
    try:
        from client_runner import _active_runners, _registry_lock
        components = {}
        with _registry_lock:
            for email, runner in list(_active_runners.items()):
                components[email] = {
                    "order_worker_alive":  bool(getattr(runner, "worker_thread", None) and runner.worker_thread.is_alive()),
                    "fill_monitor_alive":  bool(getattr(runner, "fill_monitor_thread", None) and runner.fill_monitor_thread.is_alive()),
                    # PR fix/health-and-reconciler-startup-noise: runner exposes
                    # `reconciler` (the APBrokerReconciler instance), not a
                    # `reconciler_thread` attribute. The reconciler object has
                    # its own .is_alive() method that wraps self._thread.is_alive().
                    # Previous expression `runner.reconciler_thread.is_alive()`
                    # always evaluated False (nonexistent attribute), producing
                    # a false-negative in /execution/health output.
                    "reconciler_alive":    bool(getattr(runner, "reconciler", None) and runner.reconciler.is_alive()),
                    "equity_alive":        bool(getattr(runner, "equity_thread", None) and runner.equity_thread.is_alive()),
                    "mode":                getattr(runner, "mode", "UNKNOWN"),
                    "runner_alive":        runner.is_alive(),
                }
        runner_count = len(components)

        # Probe how many members SHOULD be running for this service.
        expected_members = None
        try:
            from client_runner import _fetch_active_members, SUPABASE_URL, SUPABASE_SERVICE_KEY
            from ap.db import get_client as _gc
            if SUPABASE_URL and SUPABASE_SERVICE_KEY:
                from supabase import create_client as _ccx
                _sbx = _ccx(SUPABASE_URL, SUPABASE_SERVICE_KEY)
                expected_members = len(_fetch_active_members(_sbx))
        except Exception:
            expected_members = None  # probe failed — don't block on it

        # The dangerous state: members are configured but no runners started.
        members_without_runners = (
            expected_members is not None
            and expected_members > 0
            and runner_count == 0
        )

        all_ok = all(
            v["order_worker_alive"] and v["fill_monitor_alive"]
            for v in components.values()
        ) if components else False

        if members_without_runners:
            return jsonify({
                "ok": False,
                "healthy": False,
                "ready": False,
                "critical": "MEMBERS_CONFIGURED_BUT_ZERO_RUNNERS",
                "detail": (
                    f"{expected_members} approved/subscribed member(s) for this "
                    f"mode but 0 active runners. Trading is DOWN. Likely a "
                    f"schema mismatch or boot failure — check logs and run "
                    f"pending migrations."
                ),
                "expected_members": expected_members,
                "runner_count": 0,
                "clients": components,
            }), 503

        return jsonify({
            "ok": True,
            "healthy": all_ok,
            "ready": runner_count > 0 and all_ok,
            "expected_members": expected_members,
            "runner_count": runner_count,
            "clients": components,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 200


@app.get("/scanner/health")
@require_hmac
def scanner_health():
    """Scanner health — reads from ap_signals for today's activity."""
    try:
        from ap.db import conn, run_with_retry
        from datetime import date as _date
        today = str(_date.today())
        def _q():
            with conn() as c:
                c.execute(
                    "SELECT COUNT(*) as total, "
                    "SUM(CASE WHEN decision_status='ARMED' OR decision_status='WATCHING' THEN 1 ELSE 0 END) as passed "
                    "FROM ap_signals WHERE DATE(created_at)=%s", (today,)
                )
                return dict(c.fetchone())
        row = run_with_retry(_q) or {}
        return jsonify({
            "ok":              True,
            "online":          True,
            "signals_today":   int(row.get("total") or 0),
            "signals_passed":  int(row.get("passed") or 0),
        })
    except Exception as e:
        return jsonify({"ok": True, "online": True, "signals_today": 0, "error": str(e)}), 200


@app.get("/intelligence/health")
@require_hmac
def intelligence_health():
    """Intelligence gate health — reads decision breakdown from ap_signals."""
    try:
        from ap.db import conn, run_with_retry
        from datetime import date as _date
        today = str(_date.today())
        def _q():
            with conn() as c:
                c.execute(
                    "SELECT decision_status, COUNT(*) as cnt FROM ap_signals "
                    "WHERE DATE(created_at)=%s GROUP BY decision_status", (today,)
                )
                return [dict(r) for r in c.fetchall()]
        rows = run_with_retry(_q) or []
        breakdown = {r["decision_status"]: r["cnt"] for r in rows}
        total = sum(breakdown.values())
        return jsonify({
            "ok":        True,
            "online":    True,
            "total":     total,
            "breakdown": breakdown,
            "approved":  breakdown.get("ARMED", 0) + breakdown.get("WATCHING", 0),
            "rejected":  breakdown.get("rejected", 0),
        })
    except Exception as e:
        return jsonify({"ok": True, "online": True, "total": 0, "error": str(e)}), 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    log.info(f"Starting on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
