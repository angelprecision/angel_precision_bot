# app.py - ANGEL PRECISION BOT (PRODUCTION VERSION - STABLE)
# ‚îÄ‚îÄ In-process caches (reduce DB round-trips on hot /signal path) ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
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

# ‚îÄ‚îÄ Angel Precision Intelligence infrastructure ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
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
# exit_manager_loop removed ‚Äî APExitEngine is the sole exit manager

# ap.client_api and ap.admin_api not yet built -- routes are inline in create_app()
# from ap.client_api import client_bp
# from ap.admin_api import admin_bp
from client_runner import start_multi_client_supervisor, route_signal_to_all_clients

# ‚îÄ‚îÄ In-process caches (reduce DB round-trips on hot /signal path) ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
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

# ‚úÖ CRITICAL: Validate SIGNING_SECRET in prod
if APP_ENV == "prod" and not SIGNING_SECRET:
    log.error("=" * 70)
    log.error("üö® CRITICAL: SIGNING_SECRET not set in production!")
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
HMAC_MAX_SKEW_SECONDS = int(os.getenv("HMAC_MAX_SKEW_SECONDS", "300"))  # ‚úÖ 5 min default

# Threads
THREADS_STARTED = False
THREAD_LOCK = threading.Lock()
_IDEMP_LOCK = threading.Lock()

MAX_CONTENT_LENGTH = int(os.getenv("MAX_CONTENT_LENGTH_BYTES", str(256 * 1024)))

DEFAULT_CLIENT_ID = os.getenv("DEFAULT_CLIENT_ID", "default").strip() or "default"

# ‚îÄ‚îÄ Single-client execution worker mode ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
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
        "  ANGEL PRECISION ‚Äî DEDICATED CLIENT EXECUTION WORKER\n"
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
        "  ANGEL PRECISION ‚Äî POD EXECUTION WORKER\n"
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
        "ANGEL PRECISION ‚Äî SHARED MODE | mode=%s | all approved clients "
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
            log.warning(f"Invalid X-AP-Timestamp: {ts!r} ‚Äî rejecting, not falling through to Scheme B")
            return False  # Scheme A headers present but ts unparseable ‚Äî hard reject

        if ts_i is not None:
            # Anti-replay / drift window ‚Äî hard reject, no Scheme B fallthrough
            if abs(int(time.time()) - ts_i) > HMAC_MAX_SKEW_SECONDS:
                log.warning(f"Timestamp out of range: {ts_i} ‚Äî rejecting, not falling through to Scheme B")
                return False
            msg = str(ts_i).encode("utf-8") + b"." + raw
            expected = _hmac_hex(SIGNING_SECRET, msg)
            if hmac.compare_digest(expected, sig):
                log.debug("‚úÖ HMAC verified (timestamped scheme)")
                return True
            log.warning("HMAC timestamped scheme failed (signature mismatch)")
            return False  # Scheme A headers present but wrong sig ‚Äî never try Scheme B

    # -----------------------------
    # Scheme B: Simple body-only HMAC
    # -----------------------------
    simple_sig = (req.headers.get("X-Signature", "") or "").strip()
    if simple_sig:
        expected_simple = _hmac_hex(SIGNING_SECRET, raw)
        if hmac.compare_digest(expected_simple, simple_sig):
            log.debug("‚úÖ HMAC verified (simple scheme)")
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
            # In multi-client supervisor mode, broker credentials are validated
            # per client ‚Äî global env vars are not required.
            # Only fail if SINGLE_CLIENT mode explicitly requires them.
            _single = os.getenv("SINGLE_CLIENT_EMAIL", "").strip()
            if _single:
                raise RuntimeError(
                    "Missing TRADIER_ACCESS_TOKEN or TRADIER_ACCOUNT_ID "
                    "(required in SINGLE_CLIENT mode)"
                )
            log.warning(
                "Global TRADIER_ACCESS_TOKEN / TRADIER_ACCOUNT_ID not set ‚Äî "
                "continuing because broker credentials are validated per client. "
                "Set these only for SINGLE_CLIENT / legacy single-process mode."
            )
            return None

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
        log.info("‚úÖ All background threads started")


# ============================================================
# APP FACTORY (gunicorn safe)
# ============================================================


def _discord_signal_id(symbol: str, direction: str, strike, content_hash: str) -> str:
    """Deterministic signal_id so Discord webhook retries don't create duplicate queue entries."""
    raw = f"{symbol}:{direction}:{strike}:{content_hash}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]

def _fetch_broker_equity() -> float:
    """Fetch live account equity at startup. Returns 0.0 on any failure ‚Äî
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
        log.warning("_fetch_broker_equity failed at startup ‚Äî defaulting to 0.0: %s", _e)
    return 0.0


# ‚îÄ‚îÄ Ghost-order shared helpers ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
#
# These constants and functions are shared between
#   GET  /admin/operator/ghost-orders/dry-run        (PR #62)
#   POST /admin/operator/ghost-orders/manual-cleanup (PR #63)
#
# Keeping them at module level enforces that both endpoints use the
# EXACT SAME classifier logic. Any divergence would mean the cleanup
# acts on a different set than the dry-run previewed.

_GHOST_LOOKBACK_BUFFER_HOURS = 168  # 1-week safety margin on top of max threshold

_GHOST_ALL_ACTIONS = (
    "would_expire_pending_entry",
    "would_cancel_pending_entry",
    "skip_recent",
    "skip_has_broker_order_id",
    "skip_not_entry",
    "skip_not_pending_trigger",
    "skip_missing_local_order_id",
    "skip_unclear_state",
)

_GHOST_ELIGIBLE_ACTIONS = frozenset({
    "would_expire_pending_entry",
    "would_cancel_pending_entry",
})

# Status an order transitions to for each eligible action.
_GHOST_ACTION_STATUS = {
    "would_cancel_pending_entry": "CANCELED",
    "would_expire_pending_entry": "EXPIRED",
}


def _ghost_classify_row(
    row: dict,
    *,
    recent_skip_hours: float,
    cancel_threshold: float,
    expire_threshold: float,
) -> str:
    """Classify a single orders-table row into one of the 8 documented action
    strings. Rule order matters ‚Äî earlier rules short-circuit later ones.

    This is the SINGLE authoritative classifier. Both the dry-run endpoint
    and the manual-cleanup endpoint call this function so their behaviour
    stays in exact sync.
    """
    kind      = row.get("kind")
    status    = row.get("status")
    broker_id = row.get("broker_order_id")
    loid      = row.get("local_order_id")
    age       = row.get("age_hours")
    try:
        age_f = float(age) if age is not None else None
    except (TypeError, ValueError):
        age_f = None

    # Structural skips ‚Äî these row shapes can never be ghost-cleanup
    # candidates regardless of age.
    if not loid:
        return "skip_missing_local_order_id"
    if broker_id:
        return "skip_has_broker_order_id"
    if (kind or "").upper() != "ENTRY":
        return "skip_not_entry"
    if (status or "").upper() != "PENDING_TRIGGER":
        return "skip_not_pending_trigger"

    # Age-based gates. If age cannot be determined we cannot safely
    # classify ‚Äî fall through to skip_unclear_state.
    if age_f is None:
        return "skip_unclear_state"
    if age_f < float(recent_skip_hours):
        return "skip_recent"
    if age_f >= float(expire_threshold):
        return "would_expire_pending_entry"
    if age_f >= float(cancel_threshold):
        return "would_cancel_pending_entry"

    return "skip_unclear_state"


def _ghost_build_sql_and_params(
    *,
    recent_skip_hours: int,
    cancel_threshold: int,
    expire_threshold: int,
    client_id_filter: str,
    symbol_filter: str,
    local_order_ids: list,
    limit: int,
) -> "tuple[str, list]":
    """Build the SELECT SQL and bound params for the ghost-orders scan.

    Shared between dry-run (GET) and manual-cleanup (POST) so they
    always scan the same candidate set.
    """
    lookback_hours = (
        max(expire_threshold, cancel_threshold, recent_skip_hours)
        + _GHOST_LOOKBACK_BUFFER_HOURS
    )
    where  = ["1 = 1"]
    params: list = []
    if client_id_filter:
        where.append("client_id = %s")
        params.append(client_id_filter)
    if symbol_filter:
        where.append("UPPER(symbol) = %s")
        params.append(symbol_filter)
    if local_order_ids:
        where.append("local_order_id = ANY(%s)")
        params.append(local_order_ids)
    where.append("updated_ts >= NOW() - (%s::text || ' hours')::interval")
    params.append(str(lookback_hours))
    sql = (
        "SELECT local_order_id, client_id, kind, status, broker_order_id, "
        "       symbol, contract, qty, limit_price, score, tier, last_error, "
        "       created_ts, updated_ts, meta, "
        "       EXTRACT(EPOCH FROM (NOW() - updated_ts)) / 3600.0 AS age_hours "
        "FROM orders "
        "WHERE " + " AND ".join(where) +
        "   AND (kind = 'ENTRY' OR status = 'PENDING_TRIGGER' "
        "        OR (kind IS NULL AND status IS NULL)) "
        " ORDER BY updated_ts ASC LIMIT %s"
    )
    params.append(limit)
    return sql, params, lookback_hours


# =============================================================================
# ADMIN AUTH INFRASTRUCTURE (HOISTED ‚Äî must be defined BEFORE create_app)
# =============================================================================
# These names are HOISTED above create_app() because create_app() contains
# @_require_admin decorator usages. Module-level `app = create_app()` runs at
# import time; if the decorator is defined after create_app() in source order,
# decorator lookup fails and the app can refuse to start.
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


# =============================================================================
# OVERNIGHT REEVAL ASYNC JOB STORE (HOISTED ‚Äî must be defined BEFORE create_app)
# =============================================================================
_OVERNIGHT_REEVAL_JOBS: dict = {}


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
    log.info("‚úÖ Database initialized")

    # ‚îÄ‚îÄ Bootstrap Angel Precision Intelligence infrastructure ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
    # Registers all organs, wires kill switch into health registry,
    # starts health sweep thread. Runs once ‚Äî idempotent.
    if _AP_INFRA_AVAILABLE and _ap_bootstrap:
        try:
            # Wire Discord alert function if available in this scope.
            _discord_alert_fn = None
            try:
                from ap.notify import send_discord_alert as _discord_alert_fn
            except Exception:
                pass
            _ap_bootstrap(alert_fn=_discord_alert_fn)
            log.info("‚úÖ AP Intelligence infrastructure bootstrapped")
        except Exception as _boot_err:
            log.error("AP bootstrap failed (non-fatal): %s", _boot_err)

    if _AP_INFRA_AVAILABLE and _health_bp:
        try:
            app.register_blueprint(_health_bp)
            log.info("‚úÖ Health endpoints registered at /health/*")
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
        log.info("‚úÖ Telemetry endpoints registered at /telemetry/*")
    except Exception as _tbp_err:
        log.error(
            "Telemetry blueprint registration failed (non-fatal): %s",
            _tbp_err,
        )

    # ‚úÖ Ensure default client exists BEFORE any state write
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
        log.info("‚úÖ Default client created")
    else:
        log.info("‚úÖ Default client exists")

    # Debug routes ‚Äî only registered in non-prod environments.
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

    # ‚úÖ Explicit default client state update (prevents FK issues & ambiguity)
    update_state({"mode": mode}, client_id=DEFAULT_CLIENT_ID)
    log.info("‚úÖ State initialized")

    broker = build_broker()
    if broker is None:
        # Multi-client supervisor mode ‚Äî per-client credentials used instead.
        log.info(
            "‚úÖ Global broker not initialized (supervisor/multi-client mode) ‚Äî "
            "per-client Tradier credentials will be used by each ClientRunner."
        )
    else:
        log.info("‚úÖ Broker initialized")
    app.config["BROKER"] = broker

    if os.getenv("RUN_SUPERVISOR") == "1":
        global THREADS_STARTED
        THREADS_STARTED = True   # supervisor owns the workers ‚Äî mark ready
        log.info("Supervisor active ‚Äî legacy worker threads DISABLED")
    else:
        # Fix 5: hard block LIVE mode without supervisor
        _mode_now = os.getenv("BOT_MODE", os.getenv("MODE", "PAPER")).upper()
        if _mode_now == "LIVE":
            raise RuntimeError(
                "LIVE mode requires RUN_SUPERVISOR=1. "
                "Set RUN_SUPERVISOR=1 in Render env vars or switch to PAPER mode."
            )
        start_background_threads_once(broker)
        log.info("Background threads started (legacy path ‚Äî PAPER/SIM only)")

    # Register blueprints
    # Blueprints not yet built -- routes registered inline above
    log.info("‚úÖ Blueprints registered")

    # CORS
    allow_headers = [
        "Content-Type", "Authorization", "X-Client-Id", "X-AP-Timestamp",
        "X-AP-Signature", "Idempotency-Key", "X-API-Key", "X-Admin-Key",
        "X-Signature",  # ‚úÖ Added for simple HMAC scheme
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
            from ap.morning_handoff import get_morning_handoff_health
            from ap.preopen_readiness import get_preopen_readiness_health
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
            morning_handoff = get_morning_handoff_health()
            preopen_readiness = get_preopen_readiness_health()
            if morning_handoff.get("live", {}).get("missing_after_929_et"):
                all_ok = False
            if morning_handoff.get("paper", {}).get("missing_after_929_et"):
                all_ok = False
            if preopen_readiness.get("enforcement_active") and preopen_readiness.get("status") != "OK":
                all_ok = False
            resp = {
                "ok": all_ok,
                "status": "healthy" if all_ok else "degraded",
                "mode": st.get("mode", "UNKNOWN"),
                "kill_switch": st.get("kill_switch", False),
                "heartbeat_age_seconds": heartbeat_age,
                "morning_handoff": morning_handoff,
                "preopen_readiness": preopen_readiness,
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
        log.warning("üî¥ KILL SWITCH ENABLED")
        update_state({"kill_switch": True, "mode": "READ_ONLY"}, client_id=DEFAULT_CLIENT_ID)
        with _SIGNAL_CACHE_LOCK:
            _kill_switch_cache.clear()
            _client_status_cache.clear()
        return jsonify({"ok": True, "kill_switch": True})

    @app.post("/kill_switch/off")
    @require_hmac
    def kill_off():
        log.info("üü¢ KILL SWITCH DISABLED")
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
        log.info("‚è∏Ô∏è CLIENT ENTRIES PAUSED | %s", cid)
        update_state({"entries_paused": True}, client_id=cid)
        return jsonify({"ok": True, "client_id": cid, "entries_paused": True})

    @app.post("/admin/client/<client_id>/resume_entries")
    @require_hmac
    def admin_client_resume_entries(client_id):
        """H5: resume a paused client's entries."""
        cid = (client_id or "").strip()
        if not cid:
            return jsonify({"ok": False, "error": "missing_client_id"}), 400
        log.info("‚ñ∂Ô∏è CLIENT ENTRIES RESUMED | %s", cid)
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
        # Guard: return 503 if worker not ready ‚Äî scanner will retry instead of silently dropping
        if not THREADS_STARTED:
            return jsonify({"ok": False, "error": "worker_not_ready", "hint": "Bot is starting up ‚Äî retry in 5s"}), 503
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
                    # else: another request is already healing ‚Äî signals are enqueued durably
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
            # is a RECOVERABLE rejection ‚Äî the kill switch can be lifted seconds
            # later. If we cache it under the signal_id, the scanner's retry of
            # the SAME signal_id returns this stale 403 for IDEMP_TTL_SECONDS
            # (300s) even though the bot is live again. Only terminal successes
            # may be cached. Leave idem_key unset so retries are reprocessed.
            payload = {"ok": False, "error": "bot_in_read_only"}
            return jsonify(payload), 403

        # Fix 4: synchronous durable enqueue ‚Äî 202 only after queue write succeeds
        _sig_id = str(body.get("signal_id") or uuid.uuid4())
        body["signal_id"] = _sig_id

        try:
            enqueued = route_signal_to_all_clients(body)
            if not enqueued:
                # NOT routed ‚Äî dropped. Do not call this "routed" in logs.
                # This is the exact symptom of the broken-deploy / zero-runners
                # state. Log CRITICAL so it is impossible to miss.
                log.critical(
                    "SIGNAL DROPPED ‚Äî NO ACTIVE RUNNERS | %s %s score=%s "
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
        """Item 4 ‚Äî READ-ONLY multi-account signal ledger.

        One row per (canonical signal, client, ENTRY order) from the
        ap_multi_account_signal_ledger view. Answers "what happened on each
        account for signal X?". No mutations ‚Äî SELECT only.

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

            # Parameterized WHERE ‚Äî never string-interpolate user input.
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
                    fetched = cur.fetchall()
                    rows = []
                    for r in fetched:
                        if isinstance(r, dict):
                            # psycopg2 RealDictCursor / psycopg3 Row ‚Äî already
                            # keyed by column name; copy directly to avoid
                            # dict(zip(cols, cols)) when iterating a dict.
                            rows.append(dict(r))
                        else:
                            rows.append(dict(zip(cols, r)))
                    return rows

            from ap.db import run_with_retry
            rows = run_with_retry(_fetch)

            # Summary counts by bucket. Mirrors the 9-bucket SQL CASE.
            buckets = {
                "no_order_for_client":       0,
                "pending_trigger_no_broker": 0,
                "watcher_invalidated":       0,
                "watcher_expired":           0,
                "broker_submitted":          0,
                "filled":                    0,
                "rejected":                  0,
                "canceled":                  0,
                "terminal_no_fill":          0,
            }
            _bmap = {
                "NO_ORDER_FOR_CLIENT":       "no_order_for_client",
                "PENDING_TRIGGER_NO_BROKER": "pending_trigger_no_broker",
                "WATCHER_INVALIDATED":       "watcher_invalidated",
                "WATCHER_EXPIRED":           "watcher_expired",
                "BROKER_SUBMITTED":          "broker_submitted",
                "FILLED":                    "filled",
                "REJECTED":                  "rejected",
                "CANCELED":                  "canceled",
                "TERMINAL_NO_FILL":          "terminal_no_fill",
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


    # =====================================================================
    # PR81 Amendment ¬ß8 ‚Äî Client Opportunity Ledger admin endpoints
    # =====================================================================

    @app.get("/admin/operator/client-opportunities")
    @require_hmac
    def admin_operator_client_opportunities():
        """READ-ONLY client_signal_opportunities query (Amendment ¬ß8).

        Query params (all optional):
          canonical_signal_id, signal_id, client_id, status, start, end
          (ISO timestamps), limit (default 500, max 2000).
        No mutation. Returns one row per matching opportunity.
        """
        try:
            from ap.queue import _get_sb_client
            sb = _get_sb_client()
            if not sb:
                return jsonify({"ok": False, "error": "supabase_unavailable",
                                "rows": []}), 503

            canonical = (request.args.get("canonical_signal_id") or "").strip()
            sig_id    = (request.args.get("signal_id") or "").strip()
            client_id = (request.args.get("client_id") or "").strip()
            status    = (request.args.get("status") or "").strip()
            start_ts  = (request.args.get("start") or "").strip()
            end_ts    = (request.args.get("end") or "").strip()
            try:
                limit = max(1, min(int(request.args.get("limit", 500)), 2000))
            except (TypeError, ValueError):
                limit = 500

            q = sb.table("client_signal_opportunities").select("*")
            if canonical: q = q.eq("canonical_signal_id", canonical)
            if sig_id:    q = q.eq("signal_id", sig_id)
            if client_id: q = q.eq("client_id", client_id)
            if status:    q = q.eq("opportunity_status", status)
            if start_ts:  q = q.gte("created_at", start_ts)
            if end_ts:    q = q.lte("created_at", end_ts)
            rows = q.order("created_at", desc=True).limit(limit).execute().data or []
            return jsonify({"ok": True, "count": len(rows), "rows": rows})
        except Exception as e:
            log.error(f"client-opportunities failed: {e}")
            return jsonify({"ok": False, "error": str(e), "rows": []}), 500


    @app.get("/admin/operator/client-parity-signal/<canonical_signal_id>")
    @require_hmac
    def admin_operator_client_parity_signal(canonical_signal_id):
        """READ-ONLY side-by-side parity view (Amendment ¬ß8).

        For one canonical_signal_id, returns every expected client's
        opportunity row with status, miss stage/reason, order IDs,
        broker IDs, timestamps, preflight snapshot, execution_continued,
        preflight_enforced, and fanout timing. No mutation.
        """
        try:
            from ap.queue import _get_sb_client
            sb = _get_sb_client()
            if not sb:
                return jsonify({"ok": False, "error": "supabase_unavailable",
                                "rows": []}), 503

            canonical = (canonical_signal_id or "").strip()
            if not canonical:
                return jsonify({"ok": False, "error": "missing canonical_signal_id",
                                "rows": []}), 400

            rows = (
                sb.table("client_signal_opportunities")
                .select("*")
                .eq("canonical_signal_id", canonical)
                .order("client_id")
                .execute().data
                or []
            )

            # Best-effort fan-out timing window (first-write ‚Üí last-write).
            timestamps = [r.get("created_at") for r in rows if r.get("created_at")]
            updates    = [r.get("updated_at") for r in rows if r.get("updated_at")]
            fanout = {
                "first_created_at": min(timestamps) if timestamps else None,
                "last_created_at":  max(timestamps) if timestamps else None,
                "last_updated_at":  max(updates)    if updates    else None,
            }

            # Build the side-by-side projection.
            clients = []
            for r in rows:
                meta = r.get("metadata") or {}
                clients.append({
                    "client_id":             r.get("client_id"),
                    "signal_id":             r.get("signal_id"),
                    "opportunity_status":    r.get("opportunity_status"),
                    "miss_stage":            r.get("miss_stage"),
                    "miss_reason":           r.get("miss_reason"),
                    "order_local_id":        r.get("order_local_id"),
                    "broker_order_id":       r.get("broker_order_id"),
                    "position_id":           r.get("position_id"),
                    "preflight_enforced":    r.get("preflight_enforced"),
                    "execution_continued":   r.get("execution_continued"),
                    "would_block_reason":    r.get("would_block_reason"),
                    "kill_switch_state":     r.get("kill_switch_state"),
                    "entries_paused_state":  r.get("entries_paused_state"),
                    "buying_power_snapshot": r.get("buying_power_snapshot"),
                    "created_at":            r.get("created_at"),
                    "updated_at":            r.get("updated_at"),
                    "preflight_snapshot":    meta.get("preflight"),
                    "cap_snapshot":          meta.get("cap_snapshot"),
                })

            return jsonify({
                "ok":                  True,
                "canonical_signal_id": canonical,
                "client_count":        len(clients),
                "fanout":              fanout,
                "clients":             clients,
            })
        except Exception as e:
            log.error(f"client-parity-signal failed: {e}")
            return jsonify({"ok": False, "error": str(e), "clients": []}), 500


    # =====================================================================
    # PR89 ‚Äî Trade Volume Funnel + Scanner Gap Audit (READ-ONLY)
    # =====================================================================
    @app.get("/admin/operator/trade-volume-funnel")
    @require_hmac
    def admin_operator_trade_volume_funnel():
        """PR89 ‚Äî READ-ONLY operator report explaining why we are not
        averaging 5 executable trades per day across all active clients.

        The report decomposes the lifecycle from scanner signal generation
        through filled trades into 11 ordered stages and surfaces, at each
        stage, the drop count, drop %, and top normalized drop reasons.
        Adds per-client, per-scanner, per-symbol, and per-pod breakdowns,
        followed by deterministic action-item conclusions.

        SELECT-only. NO trading behavior is touched.

        Query params (all optional):
          start, end     ISO-8601 timestamps. Defaults: today's market
                         open (09:30 ET) ‚Üí now.
          client_id      filter to one client (substring match on client_id).
          symbol         filter to one symbol (exact, uppercased).
          scanner        filter on scanner_name / scanner_type / pattern.
          timeframe      exact-match timeframe filter.
          mode           all | live | paper | intraday | daily.

        Response is JSON with keys:
          ok, generated_at, window, filters, data_quality, summary, funnel,
          client_breakdown, scanner_breakdown, symbol_breakdown,
          pod_delivery, action_items.

        Partial-data tolerance: if a source table is unavailable (missing
        on the deployment), the affected counts return 0 and a warning is
        appended to `data_quality` rather than masking the gap with fake
        zeros.
        """
        try:
            from ap.funnel_audit import build_funnel_report
            report = build_funnel_report(
                start     = (request.args.get("start") or "").strip() or None,
                end       = (request.args.get("end") or "").strip() or None,
                client_id = (request.args.get("client_id") or "").strip() or None,
                symbol    = (request.args.get("symbol") or "").strip() or None,
                scanner   = (request.args.get("scanner") or "").strip() or None,
                timeframe = (request.args.get("timeframe") or "").strip() or None,
                mode      = (request.args.get("mode") or "").strip() or None,
            )
            return jsonify(report)
        except Exception as e:
            log.error(f"trade-volume-funnel failed: {e}", exc_info=True)
            return jsonify({
                "ok":           False,
                "error":        str(e),
                "summary":      {},
                "funnel":       [],
                "action_items": [{
                    "severity": "critical",
                    "code":     "FUNNEL_REPORT_FAILED",
                    "message":  f"Funnel report failed to build: {e}",
                }],
            }), 500


    @app.get("/admin/operator/fairness-audit")
    @require_hmac
    def admin_operator_fairness_audit():
        """Item 6 ‚Äî READ-ONLY client fairness / fan-out mismatch audit.

        Aggregates the multi-account signal ledger by canonical_signal_id and
        reports, per signal:
          - n_clients               ‚Äî distinct clients that have a row
          - clients_seen            ‚Äî list of those client_ids
          - missing_clients         ‚Äî active clients with NO order row (if
                                      ?active_clients=a@x.com,b@y.com given)
          - n_filled                ‚Äî how many clients filled
          - n_blocked               ‚Äî how many clients in any *_NO_BROKER /
                                      WATCHER_* / REJECTED / CANCELED bucket
          - contract_mismatch       ‚Äî True if clients used different contracts
          - qty_mismatch            ‚Äî True if clients used different qty
          - bucket_breakdown        ‚Äî { bucket: n }
          - earliest_order_ts, latest_order_ts

        Read-only. SELECT only. No order mutation, no cleanup, no cancel.
        Mutation paths are intentionally NOT in this endpoint.
        """
        try:
            since_hours = max(1, min(int(request.args.get("since_hours", 24)), 24 * 30))
        except (TypeError, ValueError):
            since_hours = 24
        try:
            min_clients = max(0, int(request.args.get("min_clients", 0)))
        except (TypeError, ValueError):
            min_clients = 0
        try:
            limit = max(1, min(int(request.args.get("limit", 200)), 2000))
        except (TypeError, ValueError):
            limit = 200
        canonical_id = (request.args.get("canonical_signal_id") or "").strip()
        active_clients_raw = (request.args.get("active_clients") or "").strip()
        active_clients = [c.strip() for c in active_clients_raw.split(",") if c.strip()]
        only_mismatch = request.args.get("only_mismatch", "0").strip() in ("1", "true", "yes")

        where = ["order_created_ts > NOW() - (%s || ' hours')::interval"]
        params: list = [str(since_hours)]
        if canonical_id:
            where.append("canonical_signal_id = %s")
            params.append(canonical_id)

        sql = (
            "SELECT canonical_signal_id, client_id, symbol, contract, qty, "
            "       order_status, ledger_bucket, broker_order_id, "
            "       order_created_ts, order_updated_ts "
            "FROM ap_multi_account_signal_ledger "
            "WHERE " + " AND ".join(where) +
            " ORDER BY canonical_signal_id, order_created_ts"
        )

        try:
            def _fetch():
                with conn() as c:
                    cur = c.execute(sql, tuple(params))
                    cols = [d[0] for d in cur.description]
                    fetched = cur.fetchall()
                    rows = []
                    for r in fetched:
                        if isinstance(r, dict):
                            rows.append(dict(r))
                        else:
                            rows.append(dict(zip(cols, r)))
                    return rows

            from ap.db import run_with_retry
            rows = run_with_retry(_fetch)

            # Group by canonical_signal_id and compute mismatch flags.
            from collections import defaultdict
            groups: "dict[str, list[dict]]" = defaultdict(list)
            for r in rows:
                cid = r.get("canonical_signal_id") or ""
                if cid:
                    groups[cid].append(r)

            BLOCKED_BUCKETS = {
                "PENDING_TRIGGER_NO_BROKER", "WATCHER_INVALIDATED",
                "WATCHER_EXPIRED", "REJECTED", "CANCELED", "TERMINAL_NO_FILL",
            }

            signals_out = []
            for sig_id, sig_rows in groups.items():
                clients_seen = sorted({r.get("client_id") for r in sig_rows if r.get("client_id")})
                if min_clients and len(clients_seen) < min_clients:
                    continue
                missing_clients = sorted(set(active_clients) - set(clients_seen)) if active_clients else []
                contracts = {r.get("contract") for r in sig_rows if r.get("contract")}
                qtys = {r.get("qty") for r in sig_rows if r.get("qty") is not None}
                contract_mismatch = len(contracts) > 1
                qty_mismatch = len(qtys) > 1
                n_filled = sum(1 for r in sig_rows if r.get("ledger_bucket") == "FILLED")
                n_blocked = sum(1 for r in sig_rows if r.get("ledger_bucket") in BLOCKED_BUCKETS)

                bucket_breakdown: "dict[str,int]" = defaultdict(int)
                for r in sig_rows:
                    b = r.get("ledger_bucket")
                    if b:
                        bucket_breakdown[b] += 1

                ts_list = [r.get("order_created_ts") for r in sig_rows if r.get("order_created_ts")]
                # ts values may be datetime or strings; coerce to str for JSON
                ts_sorted = sorted(str(t) for t in ts_list) if ts_list else []

                # First row defines symbol display; symbol is COALESCED in view.
                symbol_display = sig_rows[0].get("symbol") if sig_rows else None

                # Mismatch flag: any of (contract/qty/missing_clients) differ across clients.
                has_mismatch = bool(contract_mismatch or qty_mismatch or missing_clients)
                if only_mismatch and not has_mismatch:
                    continue

                signals_out.append({
                    "canonical_signal_id": sig_id,
                    "symbol":              symbol_display,
                    "n_clients":           len(clients_seen),
                    "clients_seen":        clients_seen,
                    "missing_clients":     missing_clients,
                    "n_filled":            n_filled,
                    "n_blocked":           n_blocked,
                    "contract_mismatch":   contract_mismatch,
                    "qty_mismatch":        qty_mismatch,
                    "distinct_contracts":  sorted(c for c in contracts if c),
                    "distinct_qtys":       sorted(q for q in qtys if q is not None),
                    "bucket_breakdown":    dict(bucket_breakdown),
                    "earliest_order_ts":   ts_sorted[0] if ts_sorted else None,
                    "latest_order_ts":     ts_sorted[-1] if ts_sorted else None,
                    "has_mismatch":        has_mismatch,
                })

            # Sort: signals with mismatch first, then by latest_order_ts desc.
            signals_out.sort(key=lambda s: (
                not s["has_mismatch"],
                s["latest_order_ts"] or "",
            ), reverse=False)
            signals_out.sort(key=lambda s: s["latest_order_ts"] or "", reverse=True)
            signals_out.sort(key=lambda s: not s["has_mismatch"])

            # Cap.
            signals_out = signals_out[:limit]

            total_mismatch = sum(1 for s in signals_out if s["has_mismatch"])
            total_signals = len(signals_out)
            total_filled = sum(s["n_filled"] for s in signals_out)
            total_blocked = sum(s["n_blocked"] for s in signals_out)
            return jsonify({
                "ok": True,
                "signals": signals_out,
                "summary": {
                    "signals_examined":     total_signals,
                    "signals_with_mismatch": total_mismatch,
                    "total_filled_outcomes": total_filled,
                    "total_blocked_outcomes": total_blocked,
                    "active_clients_param": active_clients,
                },
            })
        except Exception as e:
            log.error(f"fairness-audit failed: {e}")
            return jsonify({"ok": False, "error": str(e), "signals": [], "summary": {}}), 500


    @app.get("/admin/operator/ghost-orders")
    @require_hmac
    def admin_operator_ghost_orders():
        """Item 7 ‚Äî READ-ONLY ghost-order REPORT (no cleanup, no cancel).

        Identifies orders that look stranded:
          - kind='ENTRY'
          - status='PENDING_TRIGGER'
          - broker_order_id IS NULL  (never submitted to broker)
          - updated_ts older than ?stale_hours (default 24)

        REPORT ONLY. This endpoint does NOT mutate, cancel, expire, or delete.
        Per spec, automated cleanup/dry-run endpoints come in a SEPARATE PR
        after this report proves what set of rows is safe to act on.

        Query params:
          stale_hours (default 24, min 1, max 720)
          client_id   (optional filter)
          symbol      (optional filter)
          limit       (default 500, max 2000)
        """
        try:
            stale_hours = max(1, min(int(request.args.get("stale_hours", 24)), 24 * 30))
        except (TypeError, ValueError):
            stale_hours = 24
        try:
            limit = max(1, min(int(request.args.get("limit", 500)), 2000))
        except (TypeError, ValueError):
            limit = 500
        client_id_filter = (request.args.get("client_id") or "").strip()
        symbol_filter = (request.args.get("symbol") or "").strip().upper()

        where = [
            "kind = 'ENTRY'",
            "status = 'PENDING_TRIGGER'",
            "broker_order_id IS NULL",
            "updated_ts < NOW() - (%s::text || ' hours')::interval",
        ]
        params: list = [str(stale_hours)]
        if client_id_filter:
            where.append("client_id = %s")
            params.append(client_id_filter)
        if symbol_filter:
            where.append("UPPER(symbol) = %s")
            params.append(symbol_filter)

        # Pull plan_id / signal_id / direction / reserved_cost / trigger_price /
        # pattern / timeframe from JSONB meta ‚Äî they live there, not as top-level
        # columns on orders. NULLIF guards numeric casts against empty strings.
        sql = (
            "SELECT local_order_id, "
            "       client_id, "
            "       meta->>'plan_id'                AS plan_id, "
            "       meta->>'signal_id'              AS signal_id, "
            "       symbol, "
            "       contract, "
            "       COALESCE(meta->>'direction', meta->>'side') AS direction, "
            "       qty, "
            "       limit_price, "
            "       NULLIF(meta->>'reserved_cost', '')::numeric  AS reserved_cost, "
            "       NULLIF(meta->>'trigger_price', '')::numeric  AS trigger_price, "
            "       score, "
            "       tier, "
            "       meta->>'pattern'                AS pattern, "
            "       meta->>'timeframe'              AS timeframe, "
            "       last_error, "
            "       created_ts, "
            "       updated_ts, "
            "       EXTRACT(EPOCH FROM (NOW() - updated_ts)) / 3600.0 AS stale_hours_age "
            "FROM orders "
            "WHERE " + " AND ".join(where) +
            " ORDER BY updated_ts ASC LIMIT %s"
        )
        params.append(limit)

        try:
            def _fetch():
                with conn() as c:
                    cur = c.execute(sql, tuple(params))
                    cols = [d[0] for d in cur.description]
                    fetched = cur.fetchall()
                    rows = []
                    for r in fetched:
                        if isinstance(r, dict):
                            rows.append(dict(r))
                        else:
                            rows.append(dict(zip(cols, r)))
                    return rows

            from ap.db import run_with_retry
            rows = run_with_retry(_fetch)

            # Summary: per-client counts + oldest age + total reserved capital.
            from collections import defaultdict
            per_client: "dict[str,int]" = defaultdict(int)
            total_reserved = 0.0
            oldest_age = 0.0
            for r in rows:
                if r.get("client_id"):
                    per_client[r["client_id"]] += 1
                rc = r.get("reserved_cost")
                if rc is not None:
                    try:
                        total_reserved += float(rc)
                    except (TypeError, ValueError):
                        pass
                age = r.get("stale_hours_age")
                if age is not None:
                    try:
                        oldest_age = max(oldest_age, float(age))
                    except (TypeError, ValueError):
                        pass

            return jsonify({
                "ok": True,
                "report_only": True,                       # explicit: no mutation
                "stale_hours_threshold": stale_hours,
                "ghost_orders": rows,
                "summary": {
                    "total_ghost_orders":    len(rows),
                    "clients_with_ghosts":   len(per_client),
                    "per_client_counts":     dict(per_client),
                    "total_reserved_capital": round(total_reserved, 2),
                    "oldest_age_hours":      round(oldest_age, 2),
                },
                "note": (
                    "READ-ONLY REPORT. No mutation, dry-run, or cleanup is "
                    "performed by this endpoint. Cleanup endpoints will be "
                    "added in a separate PR after this report is reviewed."
                ),
            })
        except Exception as e:
            log.error(f"ghost-orders report failed: {e}")
            return jsonify({"ok": False, "error": str(e), "ghost_orders": [], "summary": {}}), 500


    @app.get("/admin/operator/ghost-orders/dry-run")
    @require_hmac
    def admin_operator_ghost_orders_dry_run():
        """READ-ONLY DRY-RUN classifier for ghost orders.

        Classifies each candidate row into a proposed action WITHOUT mutating
        anything. This endpoint exists so we can review what a cleanup pass
        WOULD do before any cleanup endpoint is built.

        ‚îå‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îê
        ‚îÇ HARD RULE                                                        ‚îÇ
        ‚îÇ   This endpoint performs NO mutation under any circumstance.    ‚îÇ
        ‚îÇ   It does NOT call:                                              ‚îÇ
        ‚îÇ     - cancel_pending_entry / expire_pending_entry / transition  ‚îÇ
        ‚îÇ     - update_order_meta or any OSM mutation method              ‚îÇ
        ‚îÇ     - any broker submit/cancel                                   ‚îÇ
        ‚îÇ   It does NOT execute INSERT, UPDATE, DELETE, or DROP.          ‚îÇ
        ‚îÇ   It only runs SELECT and returns proposed_action strings.      ‚îÇ
        ‚îî‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îò

        Proposed actions (one per row):
          would_expire_pending_entry      ‚Äî stale beyond expire_threshold
          would_cancel_pending_entry      ‚Äî stale beyond cancel_threshold
                                            but not yet expire_threshold
          skip_recent                     ‚Äî within recent_skip_hours
          skip_has_broker_order_id        ‚Äî broker already saw this order
          skip_not_entry                  ‚Äî kind != 'ENTRY'
          skip_not_pending_trigger        ‚Äî status != 'PENDING_TRIGGER'
          skip_missing_local_order_id     ‚Äî local_order_id is null
          skip_unclear_state              ‚Äî none of the above apply

        Query params (all parameterized, all optional):
          recent_skip_hours    (default 1, range 0..168)
          cancel_threshold     (default 24, range 1..720) ‚Äî hours
          expire_threshold     (default 72, range 1..720) ‚Äî hours
          client_id            (optional filter)
          symbol               (optional filter)
          limit                (default 500, max 2000)
        """
        try:
            recent_skip_hours = max(0, min(int(request.args.get("recent_skip_hours", 1)), 168))
        except (TypeError, ValueError):
            recent_skip_hours = 1
        try:
            cancel_threshold = max(1, min(int(request.args.get("cancel_threshold", 24)), 24 * 30))
        except (TypeError, ValueError):
            cancel_threshold = 24
        try:
            expire_threshold = max(1, min(int(request.args.get("expire_threshold", 72)), 24 * 30))
        except (TypeError, ValueError):
            expire_threshold = 72
        try:
            limit = max(1, min(int(request.args.get("limit", 500)), 2000))
        except (TypeError, ValueError):
            limit = 500
        client_id_filter = (request.args.get("client_id") or "").strip()
        symbol_filter = (request.args.get("symbol") or "").strip().upper()

        # Sanity: expire_threshold must be > cancel_threshold so the classifier
        # buckets land in the intended order. If a caller inverts them, treat
        # them as equal (collapses 'would_cancel' into 'would_expire').
        if expire_threshold < cancel_threshold:
            expire_threshold = cancel_threshold

        # Use shared helper so dry-run and manual-cleanup scan the same rows.
        sql, params, lookback_hours = _ghost_build_sql_and_params(
            recent_skip_hours=recent_skip_hours,
            cancel_threshold=cancel_threshold,
            expire_threshold=expire_threshold,
            client_id_filter=client_id_filter,
            symbol_filter=symbol_filter,
            local_order_ids=[],
            limit=limit,
        )

        try:
            def _fetch():
                with conn() as c:
                    cur = c.execute(sql, tuple(params))
                    cols = [d[0] for d in cur.description]
                    fetched = cur.fetchall()
                    rows = []
                    for r in fetched:
                        if isinstance(r, dict):
                            rows.append(dict(r))
                        else:
                            rows.append(dict(zip(cols, r)))
                    return rows

            from ap.db import run_with_retry
            rows = run_with_retry(_fetch)

            # Use the shared module-level classifier.
            def _classify(row: dict) -> str:
                return _ghost_classify_row(
                    row,
                    recent_skip_hours=recent_skip_hours,
                    cancel_threshold=cancel_threshold,
                    expire_threshold=expire_threshold,
                )

            # Use the module-level action constants.
            ALL_ACTIONS     = _GHOST_ALL_ACTIONS
            ELIGIBLE_ACTIONS = _GHOST_ELIGIBLE_ACTIONS

            from collections import defaultdict
            action_counts: "dict[str,int]" = {k: 0 for k in ALL_ACTIONS}
            per_client: "dict[str,dict[str,int]]" = defaultdict(lambda: {k: 0 for k in ALL_ACTIONS})
            per_symbol: "dict[str,dict[str,int]]" = defaultdict(lambda: {k: 0 for k in ALL_ACTIONS})
            classified = []

            for r in rows:
                action = _classify(r)
                action_counts[action] = action_counts.get(action, 0) + 1
                if r.get("client_id"):
                    per_client[r["client_id"]][action] = per_client[r["client_id"]].get(action, 0) + 1
                if r.get("symbol"):
                    per_symbol[r["symbol"]][action] = per_symbol[r["symbol"]].get(action, 0) + 1
                row_out = dict(r)
                # Coerce age_hours to a clean float for JSON output.
                ah = row_out.get("age_hours")
                if ah is not None:
                    try:
                        row_out["age_hours"] = round(float(ah), 2)
                    except (TypeError, ValueError):
                        pass
                row_out["proposed_action"] = action
                classified.append(row_out)

            rows_eligible = sum(action_counts[a] for a in ELIGIBLE_ACTIONS)
            rows_skipped = len(classified) - rows_eligible

            # Compact per-client / per-symbol summaries (drop empty buckets).
            def _compact(d: dict) -> dict:
                return {k: v for k, v in d.items() if v > 0}

            per_client_summary = {
                cid: _compact(buckets) for cid, buckets in per_client.items()
            }
            per_symbol_summary = {
                sym: _compact(buckets) for sym, buckets in per_symbol.items()
            }

            return jsonify({
                "ok": True,
                "report_only": True,                # explicit invariants
                "dry_run": True,
                "mutation_performed": False,
                "thresholds": {
                    "recent_skip_hours":    recent_skip_hours,
                    "cancel_threshold":     cancel_threshold,
                    "expire_threshold":     expire_threshold,
                    "lookback_hours":       lookback_hours,
                },
                "rows_scanned":   len(classified),
                "rows_eligible":  rows_eligible,
                "rows_skipped":   rows_skipped,
                "proposed_actions": classified,
                "action_summary":   action_counts,
                "per_client_summary": per_client_summary,
                "per_symbol_summary": per_symbol_summary,
                "note": (
                    "DRY-RUN ONLY. Classifies candidate rows into proposed "
                    "actions WITHOUT mutating the database. No INSERT/UPDATE/"
                    "DELETE, no OSM cancel_pending_entry/expire_pending_entry/"
                    "transition calls, no broker calls. The 'would_*' labels "
                    "indicate what a cleanup pass would do ‚Äî actual cleanup "
                    "endpoints will be added in a separate, later PR."
                ),
            })
        except Exception as e:
            log.error(f"ghost-orders dry-run failed: {e}")
            return jsonify({
                "ok": False,
                "error": str(e),
                "report_only": True,
                "dry_run": True,
                "mutation_performed": False,
                "proposed_actions": [],
                "action_summary": {},
            }), 500


    @app.post("/admin/operator/ghost-orders/manual-cleanup")
    @require_hmac
    def admin_operator_ghost_orders_manual_cleanup():
        """Manual ghost-order cleanup ‚Äî HMAC-protected, audit-logged, narrowly scoped.

        DEFAULT: dry_run=true. The endpoint performs NO mutation unless the
        caller explicitly passes {"dry_run": false} in the JSON body.

        WHAT IT TOUCHES:
          Only orders matching ALL of the following at mutation time:
            kind           = 'ENTRY'
            status         = 'PENDING_TRIGGER'
            broker_order_id IS NULL
            local_order_id  IS NOT NULL
            proposed_action in ('would_cancel_pending_entry',
                                 'would_expire_pending_entry')
            meta->>'ghost_cleanup' IS DISTINCT FROM 'true'   (idempotency)

        WHAT IT NEVER DOES:
            No DELETE or DROP.
            No broker cancel/submit/place calls.
            No EXIT-order touches.
            No watcher / selector / sizing / score changes.
            No automatic / scheduled execution.

        Status mapping (OSM conventions):
            would_cancel_pending_entry  ‚Üí CANCELED + last_error='ghost_cleanup_manual'
            would_expire_pending_entry  ‚Üí EXPIRED  + last_error='ghost_cleanup_manual'

        Audit trail (JSONB merge, non-destructive):
            ghost_cleanup          : true
            ghost_cleanup_action   : proposed_action string
            ghost_cleanup_at       : ISO timestamp
            ghost_cleanup_by       : 'admin_manual_endpoint'
            prior_status           : 'PENDING_TRIGGER'
            prior_updated_ts       : row's updated_ts at scan time

        Idempotency:
            Re-running produces 0 mutations ‚Äî already-cleaned rows have
            meta->>'ghost_cleanup'='true' which the WHERE clause excludes,
            and their status is no longer PENDING_TRIGGER.

        Request body (JSON):
            dry_run             bool    default true
            recent_skip_hours   int     default 1    (0..168)
            cancel_threshold    int     default 24   (1..720) hours
            expire_threshold    int     default 72   (1..720) hours
            client_id           str     optional filter
            symbol              str     optional filter
            local_order_ids     list    optional filter (precise row targeting)
            limit               int     default 500  max 2000
        """
        import json as _json
        from datetime import datetime, timezone

        body = request.get_json(force=True, silent=True) or {}

        # ‚îÄ‚îÄ Parse + validate ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
        # dry_run defaults to true ‚Äî caller must explicitly pass false.
        dry_run = body.get("dry_run", True)
        if not isinstance(dry_run, bool):
            dry_run = str(dry_run).lower() not in ("false", "0", "no")

        try:
            recent_skip_hours = max(0, min(int(body.get("recent_skip_hours", 1)), 168))
        except (TypeError, ValueError):
            recent_skip_hours = 1
        try:
            cancel_threshold = max(1, min(int(body.get("cancel_threshold", 24)), 24 * 30))
        except (TypeError, ValueError):
            cancel_threshold = 24
        try:
            expire_threshold = max(1, min(int(body.get("expire_threshold", 72)), 24 * 30))
        except (TypeError, ValueError):
            expire_threshold = 72
        try:
            limit = max(1, min(int(body.get("limit", 500)), 2000))
        except (TypeError, ValueError):
            limit = 500

        # Clamp: expire_threshold must be >= cancel_threshold.
        if expire_threshold < cancel_threshold:
            expire_threshold = cancel_threshold

        client_id_filter = str(body.get("client_id") or "").strip()
        symbol_filter    = str(body.get("symbol") or "").strip().upper()
        raw_ids          = body.get("local_order_ids")
        local_order_ids  = [str(x) for x in raw_ids if x] if isinstance(raw_ids, list) else []

        # ‚îÄ‚îÄ Classify candidates (same SQL + classifier as dry-run) ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
        sql, params, lookback_hours = _ghost_build_sql_and_params(
            recent_skip_hours=recent_skip_hours,
            cancel_threshold=cancel_threshold,
            expire_threshold=expire_threshold,
            client_id_filter=client_id_filter,
            symbol_filter=symbol_filter,
            local_order_ids=local_order_ids,
            limit=limit,
        )

        try:
            def _fetch():
                with conn() as c:
                    cur = c.execute(sql, tuple(params))
                    cols = [d[0] for d in cur.description]
                    fetched = cur.fetchall()
                    rows = []
                    for r in fetched:
                        if isinstance(r, dict):
                            rows.append(dict(r))
                        else:
                            rows.append(dict(zip(cols, r)))
                    return rows

            from ap.db import run_with_retry
            scanned_rows = run_with_retry(_fetch)

            # Classify every scanned row using the shared module-level helper.
            classified = []
            for r in scanned_rows:
                action = _ghost_classify_row(
                    r,
                    recent_skip_hours=recent_skip_hours,
                    cancel_threshold=cancel_threshold,
                    expire_threshold=expire_threshold,
                )
                row_out = dict(r)
                ah = row_out.get("age_hours")
                if ah is not None:
                    try:
                        row_out["age_hours"] = round(float(ah), 2)
                    except (TypeError, ValueError):
                        pass
                row_out["proposed_action"] = action
                classified.append(row_out)

            # Split into eligible (will mutate) and ineligible (will skip).
            eligible = [r for r in classified if r["proposed_action"] in _GHOST_ELIGIBLE_ACTIONS]
            ineligible = [r for r in classified if r["proposed_action"] not in _GHOST_ELIGIBLE_ACTIONS]

            # Build summaries from classified rows (both dry_run paths use this).
            from collections import defaultdict
            action_counts: "dict[str,int]" = {k: 0 for k in _GHOST_ALL_ACTIONS}
            per_client: "dict[str,dict[str,int]]" = defaultdict(lambda: {k: 0 for k in _GHOST_ALL_ACTIONS})
            per_symbol: "dict[str,dict[str,int]]" = defaultdict(lambda: {k: 0 for k in _GHOST_ALL_ACTIONS})
            for r in classified:
                a = r["proposed_action"]
                action_counts[a] = action_counts.get(a, 0) + 1
                if r.get("client_id"):
                    per_client[r["client_id"]][a] = per_client[r["client_id"]].get(a, 0) + 1
                if r.get("symbol"):
                    per_symbol[r["symbol"]][a] = per_symbol[r["symbol"]].get(a, 0) + 1

            def _compact(d: dict) -> dict:
                return {k: v for k, v in d.items() if v > 0}

            per_client_summary = {cid: _compact(b) for cid, b in per_client.items()}
            per_symbol_summary = {sym: _compact(b) for sym, b in per_symbol.items()}

            # ‚îÄ‚îÄ DRY-RUN: return classification without touching DB ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
            if dry_run:
                return jsonify({
                    "ok": True,
                    "dry_run": True,
                    "mutation_performed": False,
                    "thresholds": {
                        "recent_skip_hours": recent_skip_hours,
                        "cancel_threshold":  cancel_threshold,
                        "expire_threshold":  expire_threshold,
                        "lookback_hours":    lookback_hours,
                    },
                    "rows_scanned":  len(classified),
                    "rows_eligible": len(eligible),
                    "rows_mutated":  0,
                    "rows_skipped":  len(ineligible),
                    "proposed_actions": classified,
                    "mutated":  [],
                    "skipped":  ineligible,
                    "action_summary":     action_counts,
                    "per_client_summary": per_client_summary,
                    "per_symbol_summary": per_symbol_summary,
                    "note": (
                        "DRY-RUN (default). Pass {\"dry_run\": false} in the "
                        "JSON body to execute. No rows were mutated."
                    ),
                })

            # ‚îÄ‚îÄ LIVE MUTATION: dry_run=false ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
            # Each eligible row gets ONE parameterized UPDATE with a full
            # WHERE safety gate. The gate re-checks every structural
            # constraint atomically inside Postgres, so even if a row's
            # state changed since the SELECT, the UPDATE is a no-op (0 rows
            # returned by RETURNING).
            #
            # Meta is merged non-destructively with COALESCE || JSONB so no
            # existing meta keys are overwritten.
            #
            # HARD RULES enforced by WHERE clause:
            #   kind = 'ENTRY'                         (no EXIT)
            #   status = 'PENDING_TRIGGER'             (no filled/acked/etc.)
            #   broker_order_id IS NULL                (no broker contact)
            #   local_order_id IS NOT NULL             (structural)
            #   local_order_id = %s                    (exact row)
            #   (meta->>'ghost_cleanup') IS DISTINCT   (idempotency)
            #     FROM 'true'

            now_iso = datetime.now(timezone.utc).isoformat()
            mutated_rows = []
            skipped_rows = list(ineligible)  # ineligible always skipped

            for r in eligible:
                loid      = r["local_order_id"]
                client_id = r.get("client_id", "")
                action    = r["proposed_action"]
                new_status = _GHOST_ACTION_STATUS[action]

                # Action-specific age threshold for the CAS staleness guard:
                # the row must still be old enough at UPDATE time, not just
                # at SELECT time.
                age_threshold = (
                    expire_threshold
                    if action == "would_expire_pending_entry"
                    else cancel_threshold
                )

                meta_patch = {
                    "ghost_cleanup":        True,
                    "ghost_cleanup_action": action,
                    "ghost_cleanup_at":     now_iso,
                    "ghost_cleanup_by":     "admin_manual_endpoint",
                    "prior_status":         "PENDING_TRIGGER",
                    "prior_updated_ts":     str(r.get("updated_ts") or ""),
                }
                meta_patch_json = _json.dumps(meta_patch, default=str)

                # Single atomic UPDATE with full CAS / staleness guards.
                # RETURNING local_order_id confirms the row was actually
                # changed. If RETURNING is empty, any WHERE condition
                # failed ‚Äî the row is skipped as where_guard_no_match.
                #
                # CAS guards added vs the initial WHERE set:
                #   updated_ts = %s
                #     ‚Äî exact snapshot match from the SELECT. If any
                #       process touched the row after the scan (e.g. a
                #       watcher re-arm that bumped updated_ts), this
                #       condition fails and we skip safely.
                #   updated_ts <= NOW() - (%s::text || ' hours')::interval
                #     ‚Äî row must still be old enough at UPDATE time using
                #       the action-specific threshold (cancel_threshold or
                #       expire_threshold). Belt-and-suspenders: even if
                #       updated_ts matches the snapshot, a freshly-armed
                #       row whose watcher re-computed something cannot
                #       accidentally pass just because it was stale at
                #       scan time.
                update_sql = (
                    "UPDATE orders "
                    "SET status         = %s, "
                    "    last_error     = %s, "
                    "    updated_ts     = NOW(), "
                    "    meta           = COALESCE(meta, '{}'::jsonb) || %s::jsonb "
                    "WHERE local_order_id  = %s "
                    "  AND kind            = 'ENTRY' "
                    "  AND status          = 'PENDING_TRIGGER' "
                    "  AND broker_order_id IS NULL "
                    "  AND local_order_id  IS NOT NULL "
                    "  AND (meta->>'ghost_cleanup') IS DISTINCT FROM 'true' "
                    "  AND updated_ts      = %s "
                    "  AND updated_ts      <= NOW() - (%s::text || ' hours')::interval "
                    "RETURNING local_order_id"
                )
                update_params = (
                    new_status,
                    "ghost_cleanup_manual",
                    meta_patch_json,
                    loid,
                    r.get("updated_ts"),       # CAS: exact snapshot timestamp
                    str(age_threshold),        # staleness: action-specific threshold
                )

                def _do_update(usql=update_sql, uparams=update_params):
                    with conn() as c:
                        cur = c.execute(usql, uparams)
                        cols = [d[0] for d in cur.description]
                        fetched = cur.fetchall()
                        return [
                            dict(row) if isinstance(row, dict) else dict(zip(cols, row))
                            for row in fetched
                        ]

                try:
                    returned = run_with_retry(_do_update)
                except Exception as upd_err:
                    log.error(
                        "ghost-cleanup UPDATE failed for local_order_id=%s: %s",
                        loid, upd_err,
                    )
                    r_skip = dict(r)
                    r_skip["skip_reason"] = f"update_error: {upd_err}"
                    skipped_rows.append(r_skip)
                    continue

                if returned:
                    # RETURNING produced a row ‚Üí UPDATE matched and changed.
                    r_out = dict(r)
                    r_out["new_status"]            = new_status
                    r_out["ghost_cleanup_at"]      = now_iso
                    r_out["ghost_cleanup_action"]  = action
                    mutated_rows.append(r_out)
                    log.info(
                        "ghost-cleanup: mutated local_order_id=%s client=%s "
                        "action=%s new_status=%s",
                        loid, client_id, action, new_status,
                    )
                else:
                    # RETURNING empty ‚Üí row no longer matched (already cleaned,
                    # status changed, or broker_order_id was set between scan
                    # and update ‚Äî all are correct safety outcomes).
                    r_skip = dict(r)
                    r_skip["skip_reason"] = "where_guard_no_match"
                    skipped_rows.append(r_skip)
                    log.info(
                        "ghost-cleanup: WHERE guard skipped local_order_id=%s "
                        "(already cleaned or state changed since scan)",
                        loid,
                    )

            mutation_performed = len(mutated_rows) > 0

            return jsonify({
                "ok": True,
                "dry_run": False,
                "mutation_performed": mutation_performed,
                "thresholds": {
                    "recent_skip_hours": recent_skip_hours,
                    "cancel_threshold":  cancel_threshold,
                    "expire_threshold":  expire_threshold,
                    "lookback_hours":    lookback_hours,
                },
                "rows_scanned":  len(classified),
                "rows_eligible": len(eligible),
                "rows_mutated":  len(mutated_rows),
                "rows_skipped":  len(skipped_rows),
                "proposed_actions": classified,
                "mutated":  mutated_rows,
                "skipped":  skipped_rows,
                "action_summary":     action_counts,
                "per_client_summary": per_client_summary,
                "per_symbol_summary": per_symbol_summary,
                "note": (
                    "Manual ghost-order cleanup. Only ENTRY/PENDING_TRIGGER/"
                    "no-broker rows whose age met the threshold were updated. "
                    "No broker calls were made. No rows were deleted. "
                    "Re-running this endpoint is idempotent: already-cleaned "
                    "rows are excluded by the WHERE guard."
                ),
            })

        except Exception as e:
            log.error("ghost-orders manual-cleanup failed: %s", e)
            return jsonify({
                "ok": False,
                "error": str(e),
                "dry_run": dry_run,
                "mutation_performed": False,
                "mutated":  [],
                "skipped":  [],
            }), 500


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
                            # Create a dummy placeholder ‚Äî actual worker won't start but entries can flow
                            log.warning(f"force_initialize: {email} worker_thread is None ‚Äî runner may not process signals")
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
                    "error": "client_id required in body (no default ‚Äî per-client routing is explicit).",
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
                    "note": "Test signal enqueued ‚Äî worker will process but market_closed_no_contract_selection rejection is expected after hours",
                })
            except Exception as eq_exc:
                return jsonify({"ok": False, "error": f"enqueue failed: {eq_exc}"}), 500

        except Exception as e:
            log.error(f"test_signal failed: {e}", exc_info=True)
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.post("/admin/force_start_runner")
    @require_hmac
    def admin_force_start_runner():
        """Directly spawn a runner for a member ‚Äî bypasses supervisor for emergency recovery."""
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
        This is what a $3K/month client needs to see ‚Äî not logs.
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
        """Live open positions ‚Äî what the client holds right now."""
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
        """Every signal rejection ‚Äî queryable by client. No log-diving needed."""
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
        dashboard backend but did not exist on the bot ‚Äî every call 404'd silently
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
        """Force reset the master control dedup cache ‚Äî clears stale signal blocks."""
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

    @app.post("/admin/operator/manual-rescue-restart-guard")
    @_require_admin
    def manual_rescue_restart_guard():
        body = request.get_json(silent=True) or {}
        lookback_hours = int(body.get("hours") or 48)

        try:
            from ap.db import conn, run_with_retry

            def _rescue():
                with conn() as c:
                    c.execute(
                        """
                        WITH rescued AS (
                            UPDATE trade_queue
                            SET    status = 'NEW',
                                   started_ts = NULL,
                                   finished_ts = NULL,
                                   last_error = 'manual_rescue_current_session',
                                   result_json = COALESCE(result_json, '{}'::jsonb) || jsonb_build_object(
                                       'manual_rescue', true,
                                       'manual_rescue_actor', 'operator_dashboard',
                                       'manual_rescue_ts', NOW()::text,
                                       'manual_rescue_previous_error', COALESCE(last_error, '')
                                   )
                            WHERE  status IN ('NEW', 'REJECTED', 'ERROR')
                              AND  created_ts >= NOW() - (%s || ' hours')::interval
                              AND (
                                     COALESCE(payload->>'timeframe', '') IN ('1d', 'daily', 'overnight')
                                  OR payload ? 'prior_day_high'
                                  OR payload ? 'prior_day_low'
                              )
                              AND (
                                     last_error IN (
                                         'restart_guard:overnight_skip',
                                         'manual_requeue_after_overnight_reeval_timeout',
                                         'manual_rescue_current_session'
                                     )
                                  OR COALESCE(result_json->>'manual_rescue', 'false') = 'true'
                              )
                            RETURNING id, client_id, signal_id
                        )
                        SELECT COUNT(*)::int AS n FROM rescued
                        """,
                        (str(lookback_hours),),
                    )
                    row = c.fetchone() or {"n": 0}
                    return int(row["n"] if isinstance(row, dict) else row[0])

            rescued = run_with_retry(_rescue)
            admin_log.warning(
                "MANUAL_RESCUE_RESTART_GUARD rows=%s lookback_hours=%s ip=%s",
                rescued,
                lookback_hours,
                _admin_client_ip(),
            )
            return jsonify({
                "ok": True,
                "rescued": int(rescued or 0),
                "lookback_hours": lookback_hours,
            })
        except Exception as e:
            admin_log.error("manual_rescue_restart_guard failed: %s", e, exc_info=True)
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.post("/admin/overnight_reeval")
    @require_hmac
    def admin_overnight_reeval():
        try:
            from client_runner import _active_runners, _registry_lock
            from ap_overnight_reeval import run_overnight_reeval
            from ap.morning_handoff import run_morning_handoff_audit
            import threading as _threading
            import time as _time
            import uuid as _uuid

            body = request.get_json(silent=True) or {}
            force = bool(body.get("force", True))
            clients_filter = body.get("clients") or None
            if clients_filter is not None:
                clients_filter = {str(e).strip().lower() for e in clients_filter if str(e).strip()}
            max_clients = int(body.get("max_clients", 5) or 5)
            time_budget_seconds = float(body.get("time_budget_seconds", 60) or 60)
            async_background = bool(body.get("async_background", False))
            offset = max(0, int(body.get("offset", 0) or 0))

            with _registry_lock:
                runners_all = list(_active_runners.items())

            def _eligible(email_runner):
                email, _runner = email_runner
                if clients_filter is not None:
                    return email.lower() in clients_filter
                return True

            runners_eligible = [er for er in runners_all if _eligible(er)]
            runners_window = runners_eligible[offset:offset + max_clients]

            if async_background:
                job_id = str(_uuid.uuid4())
                _OVERNIGHT_REEVAL_JOBS[job_id] = {
                    "job_id": job_id,
                    "status": "queued",
                    "started_ts": _time.time(),
                    "finished_ts": None,
                    "force": force,
                    "runners_queued": len(runners_window),
                    "runners_processed": 0,
                    "results": {},
                    "elapsed_seconds": None,
                }

                def _run_bg():
                    job = _OVERNIGHT_REEVAL_JOBS[job_id]
                    job["status"] = "running"
                    t0 = _time.time()
                    try:
                        for email, runner in runners_window:
                            elapsed = _time.time() - t0
                            if elapsed > time_budget_seconds:
                                job["results"][email] = {
                                    "skipped": "time_budget_exceeded",
                                    "elapsed_seconds": round(elapsed, 1),
                                }
                                continue
                            if not runner.is_alive():
                                job["results"][email] = {"error": "runner not alive"}
                                job["runners_processed"] += 1
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
                                runner._last_overnight_reeval_date = None
                                job["results"][email] = result
                                log.info(f"overnight_reeval[bg:{job_id}] [{email}]: {result}")
                            except Exception as _bg_err:
                                import traceback as _tb
                                job["results"][email] = {
                                    "error": str(_bg_err),
                                    "traceback": _tb.format_exc()[-2000:],
                                }
                                log.error(
                                    f"overnight_reeval[bg:{job_id}] [{email}] failed: {_bg_err}",
                                    exc_info=True,
                                )
                            job["runners_processed"] += 1
                    finally:
                        job["status"] = "done"
                        job["finished_ts"] = _time.time()
                        job["elapsed_seconds"] = round(job["finished_ts"] - t0, 1)

                _threading.Thread(
                    target=_run_bg,
                    daemon=True,
                    name=f"overnight_reeval_bg_{job_id[:8]}",
                ).start()
                return jsonify({
                    "ok": True,
                    "mode": "async",
                    "job_id": job_id,
                    "runners_queued": len(runners_window),
                    "force": force,
                    "status_endpoint": f"/admin/overnight_reeval/status?job_id={job_id}",
                })

            results: dict = {}
            handoff_results: dict = {}
            t0 = _time.time()
            processed = 0
            skipped_budget = 0
            for email, runner in runners_window:
                elapsed = _time.time() - t0
                if elapsed > time_budget_seconds:
                    results[email] = {
                        "skipped": "time_budget_exceeded",
                        "elapsed_seconds": round(elapsed, 1),
                    }
                    skipped_budget += 1
                    continue
                if not runner.is_alive():
                    results[email] = {"error": "runner not alive"}
                    processed += 1
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
                    runner._last_overnight_reeval_date = None
                    results[email] = result
                    armed = int(result.get("armed", 0) or 0) if isinstance(result, dict) else 0
                    if armed > 0:
                        runner_mode = str(
                            getattr(runner, "mode", None)
                            or getattr(runner.master_control, "mode", None)
                            or ""
                        ).strip().lower()
                        handoff_results[email] = run_morning_handoff_audit(
                            client_id=email,
                            execution_mode=runner_mode,
                            stage="post_overnight_reeval",
                            dry_run=False,
                            runner=runner,
                        )
                    log.info(f"overnight_reeval [{email}]: {result}")
                except Exception as e:
                    import traceback as _tb
                    results[email] = {"error": str(e), "traceback": _tb.format_exc()[-2000:]}
                    log.error(f"overnight_reeval [{email}] failed: {e}", exc_info=True)
                processed += 1

            elapsed_total = _time.time() - t0
            total_armed = sum(r.get("armed", 0) for r in results.values() if isinstance(r, dict))
            total_rejected = sum(r.get("rejected", 0) for r in results.values() if isinstance(r, dict))
            next_offset = offset + processed + skipped_budget

            return jsonify({
                "ok": True,
                "mode": "sync",
                "force": force,
                "runners_total": len(runners_all),
                "runners_eligible": len(runners_eligible),
                "runners_processed": processed,
                "runners_skipped_budget": skipped_budget,
                "offset": offset,
                "next_offset": next_offset,
                "remaining_after_window": max(0, len(runners_eligible) - next_offset),
                "time_budget_seconds": time_budget_seconds,
                "elapsed_seconds": round(elapsed_total, 2),
                "total_armed": total_armed,
                "total_rejected": total_rejected,
                "handoff_results": handoff_results,
                "results": results,
            })
        except Exception as e:
            log.error(f"overnight_reeval endpoint failed: {e}", exc_info=True)
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.get("/admin/overnight_reeval/status")
    @require_hmac
    def admin_overnight_reeval_status():
        job_id = request.args.get("job_id", "").strip()
        if not job_id:
            return jsonify({"ok": False, "error": "job_id required"}), 400
        job = _OVERNIGHT_REEVAL_JOBS.get(job_id)
        if not job:
            return jsonify({"ok": False, "error": "job_id_not_found"}), 404
        try:
            import time as _time
            now = _time.time()
            stale = [
                jid for jid, j in _OVERNIGHT_REEVAL_JOBS.items()
                if j.get("status") == "done"
                and j.get("finished_ts") is not None
                and (now - j["finished_ts"]) > 3600
            ]
            for jid in stale:
                _OVERNIGHT_REEVAL_JOBS.pop(jid, None)
        except Exception:
            pass
        return jsonify({"ok": True, "job": job})

    @app.post("/admin/release_after_hours_deferred")
    @require_hmac
    def release_after_hours_deferred():
        try:
            body = request.get_json(silent=True) or {}
            force = bool(body.get("force", False))
            _lh_raw = body.get("lookback_h")
            lookback_h = 36 if _lh_raw is None else max(1, int(_lh_raw))
            clients_filter = body.get("clients") or None
            if clients_filter is not None:
                clients_filter = [
                    str(e).strip().lower() for e in clients_filter if str(e).strip()
                ] or None

            from zoneinfo import ZoneInfo
            from datetime import datetime as _dt
            _et = _dt.now(ZoneInfo("America/New_York"))
            _et_minutes = _et.hour * 60 + _et.minute
            _gate_minutes = 9 * 60 + 35
            if not force and _et_minutes < _gate_minutes:
                admin_log.info(
                    "release_after_hours_deferred declined: pre-open et=%s gate=09:35 force=false",
                    _et.strftime("%H:%M:%S"),
                )
                return jsonify({
                    "ok": False,
                    "released": 0,
                    "reason": "pre_open_guard",
                    "et_now": _et.strftime("%H:%M:%S"),
                    "hint": "pass force=true to override",
                }), 412

            from ap.db import conn, run_with_retry

            def _release():
                with conn() as c:
                    if clients_filter:
                        c.execute(
                            """
                            UPDATE trade_queue
                               SET status      = 'NEW',
                                   last_error  = NULL,
                                   started_ts  = NULL,
                                   finished_ts = NULL
                             WHERE status = 'WATCHING'
                               AND last_error = 'after_hours_deferred:awaiting_overnight_reeval'
                               AND created_ts >= NOW() - (%s || ' hours')::interval
                               AND client_id = ANY(%s)
                            RETURNING id, client_id
                            """,
                            (str(lookback_h), clients_filter),
                        )
                    else:
                        c.execute(
                            """
                            UPDATE trade_queue
                               SET status      = 'NEW',
                                   last_error  = NULL,
                                   started_ts  = NULL,
                                   finished_ts = NULL
                             WHERE status = 'WATCHING'
                               AND last_error = 'after_hours_deferred:awaiting_overnight_reeval'
                               AND created_ts >= NOW() - (%s || ' hours')::interval
                            RETURNING id, client_id
                            """,
                            (str(lookback_h),),
                        )
                    rows = c.fetchall() or []
                    return len(rows)

            released = int(run_with_retry(_release) or 0)
            admin_log.warning(
                "RELEASE_AFTER_HOURS_DEFERRED released=%s force=%s lookback_h=%s clients=%s ip=%s",
                released,
                force,
                lookback_h,
                clients_filter,
                _admin_client_ip(),
            )
            return jsonify({
                "ok": True,
                "released": released,
                "force": force,
                "et_now": _et.strftime("%H:%M:%S"),
                "lookback_hours": lookback_h,
                "clients_filter": clients_filter,
            })
        except Exception as e:
            admin_log.error("release_after_hours_deferred failed: %s", e, exc_info=True)
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.get("/admin/morning_handoff_audit")
    @require_hmac
    def admin_morning_handoff_audit_get():
        from client_runner import _active_runners, _registry_lock
        from ap_morning_handoff_audit import run_morning_handoff_audit

        client_id_req = request.args.get("client_id", "").strip().lower()
        if not client_id_req:
            return jsonify({"ok": False, "error": "client_id required"}), 400
        mode = str(
            request.args.get("mode", request.args.get("execution_mode", "live"))
        ).lower().strip()
        dry_run = str(request.args.get("dry_run", "false")).lower() in ("1", "true", "yes")

        with _registry_lock:
            runner = _active_runners.get(client_id_req)

        if runner is None:
            return jsonify({"ok": False, "error": f"client_id not in active runners: {client_id_req}"}), 404

        entry_watcher = getattr(runner, "entry_watcher", None) or getattr(
            getattr(runner, "core", None), "entry_watcher", None
        )
        osm = (
            getattr(runner, "order_state_machine", None)
            or getattr(runner, "osm", None)
            or getattr(getattr(runner, "core", None), "order_state_machine", None)
            or getattr(getattr(runner, "execution_core", None), "order_state_machine", None)
        )
        if osm is None:
            log.warning(
                "morning_handoff_audit GET: OSM not found for client %s ‚Äî re-arm metadata will not be persisted. WATCHER_REARM_AUDIT_FAILED",
                client_id_req,
            )

        try:
            result = run_morning_handoff_audit(
                client_id=client_id_req,
                entry_watcher=entry_watcher,
                osm=osm,
                execution_mode=mode,
                dry_run=dry_run,
            )
            return jsonify(result)
        except Exception as exc:
            log.error("morning_handoff_audit GET failed: %s", exc, exc_info=True)
            return jsonify({"ok": False, "error": str(exc)}), 500

    @app.post("/admin/morning_handoff_audit")
    @require_hmac
    def admin_morning_handoff_audit_post():
        from client_runner import _active_runners, _registry_lock
        from ap_morning_handoff_audit import run_morning_handoff_audit
        import time as _time

        body = request.get_json(silent=True) or {}
        clients_filter = body.get("clients") or None
        client_id_alias = str(body.get("client_id") or "").strip().lower()
        mode_raw = body.get("mode")
        execution_mode_alias = str(body.get("execution_mode") or "").strip().lower()
        if mode_raw and execution_mode_alias and str(mode_raw).strip().lower() != execution_mode_alias:
            return jsonify({"ok": False, "error": "mode_execution_mode_mismatch"}), 400
        if clients_filter is not None:
            clients_filter = {str(e).strip().lower() for e in clients_filter if str(e).strip()}
        elif client_id_alias:
            clients_filter = {client_id_alias}
        mode = str(mode_raw or execution_mode_alias or "live").lower().strip()
        dry_run = bool(body.get("dry_run", False))

        with _registry_lock:
            runners_all = dict(_active_runners)

        _t0 = _time.monotonic()
        per_client_results = {}
        errors = {}

        for email, runner in runners_all.items():
            if clients_filter is not None and email not in clients_filter:
                continue
            entry_watcher = getattr(runner, "entry_watcher", None) or getattr(
                getattr(runner, "core", None), "entry_watcher", None
            )
            osm = (
                getattr(runner, "order_state_machine", None)
                or getattr(runner, "osm", None)
                or getattr(getattr(runner, "core", None), "order_state_machine", None)
                or getattr(getattr(runner, "execution_core", None), "order_state_machine", None)
            )
            if osm is None:
                log.warning(
                    "morning_handoff_audit POST: OSM not found for client %s ‚Äî re-arm metadata will not be persisted. WATCHER_REARM_AUDIT_FAILED",
                    email,
                )
            try:
                per_client_results[email] = run_morning_handoff_audit(
                    client_id=email,
                    entry_watcher=entry_watcher,
                    osm=osm,
                    execution_mode=mode,
                    dry_run=dry_run,
                )
            except Exception as exc:
                log.error(
                    "morning_handoff_audit POST failed for %s: %s", email, exc, exc_info=True
                )
                errors[email] = str(exc)
                per_client_results[email] = {"ok": False, "error": str(exc)}

        elapsed = _time.monotonic() - _t0
        return jsonify({
            "ok": len(errors) == 0,
            "mode": mode,
            "dry_run": dry_run,
            "clients_audited": len(per_client_results),
            "elapsed_seconds": round(elapsed, 2),
            "results": per_client_results,
            "errors": errors,
        })

    @app.route("/admin/preopen_readiness", methods=["GET", "POST"])
    @require_hmac
    def admin_preopen_readiness():
        try:
            from client_runner import _active_runners, _registry_lock
            from ap.morning_jobs import select_runner_items
            from ap.preopen_readiness import run_preopen_autonomous_readiness

            body = request.get_json(silent=True) or {}
            if request.method == "GET":
                body = {
                    "clients": request.args.getlist("clients"),
                    "execution_mode": request.args.get("execution_mode"),
                    "dry_run": request.args.get("dry_run", "true").lower() != "false",
                }

            requested_clients = body.get("clients") or []
            execution_mode = str(body.get("execution_mode") or "").strip().lower()
            dry_run = bool(body.get("dry_run", True))

            with _registry_lock:
                selected = select_runner_items(dict(_active_runners), requested_clients=requested_clients)

            results = {}
            for email, runner in selected:
                runner_mode = str(
                    getattr(runner, "mode", None)
                    or getattr(runner.master_control, "mode", None)
                    or ""
                ).strip().lower()
                if execution_mode and runner_mode != execution_mode:
                    continue
                results[email] = run_preopen_autonomous_readiness(
                    email,
                    runner_mode,
                    dry_run=dry_run,
                    stage="manual",
                    runner=runner,
                )

            ok = all(str(r.get("status")) == "OK" for r in results.values()) if results else False
            return jsonify({
                "ok": ok,
                "requested_clients": [str(x).strip() for x in requested_clients if str(x).strip()],
                "execution_mode": execution_mode or None,
                "dry_run": dry_run,
                "results": results,
            }), 200 if ok else 503
        except Exception as e:
            log.error("preopen_readiness endpoint failed: %s", e, exc_info=True)
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.post("/admin/paper_rescue_restart_guard")
    @require_hmac
    def admin_paper_rescue_restart_guard():
        from client_runner import _active_runners, _registry_lock
        from ap_paper_rescue_restart_guard import run_paper_rescue_restart_guard

        body = request.get_json(silent=True) or {}
        clients_filter = body.get("clients") or None
        if clients_filter is not None:
            clients_filter = {str(e).strip().lower() for e in clients_filter if str(e).strip()}
        lookback_h = max(1, int(body.get("lookback_h", 36) or 36))
        dry_run = bool(body.get("dry_run", False))

        with _registry_lock:
            runners_all = dict(_active_runners)

        paper_runners: dict = {}
        live_found: list[str] = []

        for email, runner in runners_all.items():
            if clients_filter is not None and email not in clients_filter:
                continue
            mode = str(getattr(runner, "mode", "") or "").upper().strip()
            if mode == "LIVE":
                live_found.append(email)
            else:
                paper_runners[email] = runner

        if live_found:
            return jsonify({
                "ok": False,
                "error": (
                    f"paper_rescue_restart_guard is paper-only. "
                    f"Live clients found in scope: {live_found}. "
                    f"No rows were modified."
                ),
                "live_clients": live_found,
            }), 400

        if clients_filter:
            missing = clients_filter - set(runners_all.keys())
            if missing:
                return jsonify({
                    "ok": False,
                    "error": f"clients not found in active runners: {sorted(missing)}",
                    "missing": sorted(missing),
                }), 404

        try:
            result = run_paper_rescue_restart_guard(
                paper_client_ids=list(paper_runners.keys()),
                runners=paper_runners,
                lookback_hours=lookback_h,
                dry_run=dry_run,
            )
            return jsonify(result)
        except ValueError as exc:
            log.error("paper_rescue_restart_guard rejected: %s", exc)
            return jsonify({"ok": False, "error": str(exc)}), 400
        except Exception as exc:
            log.error("paper_rescue_restart_guard failed: %s", exc, exc_info=True)
            return jsonify({"ok": False, "error": str(exc)}), 500

    @app.post("/admin/reseed_exit_engine")
    @require_hmac
    def reseed_exit_engine():
        """Force the exit engine to reseed from DB ‚Äî picks up manually seeded positions."""
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
        broker = app.config.get("BROKER")
        if broker is None:
            return jsonify({
                "ok": False,
                "error": "no_global_broker",
                "detail": (
                    "Global broker not initialized ‚Äî multi-client supervisor mode "
                    "uses per-client Tradier credentials. Use the per-client "
                    "health endpoint instead."
                ),
            }), 503
        if cfg.BOT_MODE not in ("PAPER", "LIVE"):
            return jsonify({"ok": False, "error": "only_paper_live"}), 400
        try:
            equity = broker.get_account_equity()
            return jsonify({"ok": True, "equity": equity})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    log.info("=" * 70)
    log.info("‚úÖ APP READY")
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
# P0 startup-order fix: the admin decorator infrastructure is defined before
# create_app() so @_require_admin usages inside create_app() resolve at app
# construction time.


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
        # Not in exit engine ‚Äî close directly via position manager
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
                            "warning": "Closed at entry price ‚Äî update exit_option_price in proof_trades manually."})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    # Submit immediate exit decision
    try:
        from ap_exit_engine import ExitDecision
        qty = int(getattr(pos, "quantity_remaining", 0) or getattr(pos, "quantity", 1))
        decision = ExitDecision(
            action="CLOSE_ALL", quantity=qty,
            reason=f"ADMIN FORCE EXIT ‚Äî {reason}",
            urgency="IMMEDIATE",
            pnl_pct=getattr(pos, "option_pnl_pct", 0.0),
            reason_code="ADMIN_FORCE_EXIT",
        )
        submitted = ee._submit_exit_decision(pos, decision, kill_active=True)

        # ‚îÄ‚îÄ PROOF STAGING: admin force exit bypasses _on_position_close ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
        # The normal exit path (exit engine ‚Üí _on_position_close) sets
        # pos._proof_staged so _finalize_proof can write proof_trades on fill
        # confirmation. Admin force exit calls _submit_exit_decision directly,
        # skipping that callback chain. Without this block, the trade closes
        # but never appears in proof_trades.
        #
        # CODEX PATCH: proof is ONLY written when submitted=True.
        # If _submit_exit_decision returns False (exit_in_flight block, kill-switch
        # guard, broker reject, or any _can_submit_exit failure) the position is
        # still open ‚Äî writing proof here would create a fake closed trade.
        if not submitted:
            admin_log.warning(
                "FORCE_EXIT_PROOF_SKIPPED pos=%s ticker=%s reason=submission_failed_or_not_accepted",
                position_id, getattr(pos, "ticker", "?"),
            )
        else:
            try:
                _proof_core = getattr(target_runner, "core", None)
                _proof_obj  = getattr(_proof_core, "proof", None) if _proof_core else None
                if _proof_obj and hasattr(_proof_obj, "log_trade"):
                    _ep      = float(getattr(pos, "entry_price",         0) or 0)
                    _xp      = float(getattr(pos, "current_bid",         0) or
                                     getattr(pos, "current_option_price", 0) or 0)
                    _ue      = float(getattr(pos, "underlying_entry",    0) or 0)
                    _ux      = float(getattr(pos, "current_underlying",  0) or 0)
                    _qty     = int(getattr(pos, "quantity_remaining",    0) or
                                   getattr(pos, "quantity",              1))
                    _sig     = getattr(pos, "signal", {}) or {}
                    _opt_pnl = round(((_xp - _ep) / _ep * 100) if _ep > 0 and _xp > 0 else 0, 2)
                    _u_pnl   = round(((_ux - _ue) / _ue * 100) if _ue > 0 and _ux > 0 else 0, 3)
                    _bb      = float(os.getenv("BREAKEVEN_BAND_PCT", "-2.0"))
                    _win     = _opt_pnl >= _bb
                    _proof_obj.log_trade(
                        ticker             = getattr(pos, "ticker",    "?"),
                        pattern            = _sig.get("pattern",       ""),
                        side               = getattr(pos, "side",       ""),
                        timeframe          = _sig.get("timeframe",     "1d"),
                        score              = float(_sig.get("score",    0) or 0),
                        tier               = _sig.get("tier",          ""),
                        context_score      = float((_sig.get("score_breakdown") or {}).get("real_time_ctx", 0) or 0),
                        setup_status       = "admin_force_exit",
                        entry_trigger      = _ue,
                        entry_option_price = _ep,
                        exit_option_price  = _xp,
                        underlying_entry   = _ue,
                        underlying_exit    = _ux,
                        contracts          = _qty,
                        exit_reason        = f"ADMIN FORCE EXIT ‚Äî {reason}",
                        option_pnl_pct     = _opt_pnl,
                        underlying_pnl_pct = _u_pnl,
                        win                = _win,
                        spread_pct         = float(_sig.get("spread_pct", 0) or 0),
                        opened_at          = getattr(pos, "opened_at", None),
                        position_id        = str(getattr(pos, "position_id", "") or ""),
                        local_order_id     = str(getattr(pos, "local_order_id", "") or ""),
                    )
                    admin_log.info(
                        "FORCE_EXIT_PROOF_LOGGED pos=%s ticker=%s pnl=%.1f%% win=%s",
                        position_id, getattr(pos, "ticker", "?"), _opt_pnl, _win,
                    )
            except Exception as _proof_exc:
                admin_log.error(
                    "FORCE_EXIT_PROOF_FAILED pos=%s err=%s ‚Äî trade closed but not in proof_trades",
                    position_id, _proof_exc,
                )

        return jsonify({
            "ok": True, "position_id": position_id,
            "ticker": getattr(pos, "ticker", "?"),
            "method": "exit_engine_immediate",
            "submitted": bool(submitted),
            "current_pnl_pct": round(getattr(pos, "option_pnl_pct", 0.0) * 100, 1),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# =============================================================================
# PR #90 ‚Äî Live Execution Journal (read-only, admin-protected)
#
# Two endpoints behind @_require_admin (same HMAC pattern as every other
# /admin/* route). Both READ-ONLY ‚Äî never write, never mutate trading state.
# =============================================================================

@app.get("/admin/operator/live-execution-journal")
@_require_admin
def admin_live_execution_journal():
    """Live Execution Truth Layer.

    Query params (all optional):
      start                ISO timestamp or YYYY-MM-DD (default: 24h ago)
      end                  ISO timestamp or YYYY-MM-DD (default: now)
      client_id            exact match
      symbol               case-insensitive match
      canonical_signal_id  exact match
      status               filter by current_lifecycle_stage

    Returns the PR-90 spec shape:
      summary, trades, client_breakdown, symbol_breakdown,
      failure_breakdown, action_items, data_quality, window, filters.

    Never mutates DB. Never exposes secrets, broker tokens, or HMAC keys.
    """
    try:
        from ap.operator.live_execution_journal import build_journal
    except Exception as e:
        return jsonify({"ok": False, "error": f"journal module unavailable: {e}"}), 503

    try:
        report = build_journal(
            start=request.args.get("start") or None,
            end=request.args.get("end") or None,
            client_id=request.args.get("client_id") or None,
            symbol=request.args.get("symbol") or None,
            canonical_signal_id=request.args.get("canonical_signal_id") or None,
            status=request.args.get("status") or None,
        )
    except Exception as e:
        # build_journal already swallows DB errors into data_quality; this
        # except catches a programming error in the module itself.
        admin_log.error("live-execution-journal failed: %s", e)
        return jsonify({"ok": False, "error": f"journal build failed: {e}"}), 500

    return jsonify({"ok": True, **report})


@app.get("/admin/operator/live-execution-truth-sample")
@_require_admin
def admin_live_execution_truth_sample():
    """Debug endpoint: return ONE end-to-end journey for a specific
    canonical_signal_id (and optionally client_id). Read-only."""
    canonical = (request.args.get("canonical_signal_id") or "").strip()
    if not canonical:
        return jsonify({"ok": False, "error": "canonical_signal_id query param required"}), 400
    try:
        from ap.operator.live_execution_journal import build_journey_sample
    except Exception as e:
        return jsonify({"ok": False, "error": f"journal module unavailable: {e}"}), 503

    try:
        sample = build_journey_sample(
            canonical_signal_id=canonical,
            client_id=(request.args.get("client_id") or "").strip() or None,
        )
    except Exception as e:
        admin_log.error("live-execution-truth-sample failed: %s", e)
        return jsonify({"ok": False, "error": f"sample build failed: {e}"}), 500

    return jsonify(sample)


# =============================================================================
# PR #91 ‚Äî Daily Operator Folders -> Weekly Archive Pipeline
# =============================================================================
# Archive/reporting only. No trading logic changes.
#
# Admin routes:
#   POST /admin/daily-rollup/run    build + upsert one day
#   GET  /admin/daily-rollup/       read one day (Supabase preferred,
#                                    disk fallback)
#   POST /admin/weekly-rollup/run   aggregate one week from daily artifacts
#   GET  /admin/weekly-rollup/      read one week
#   GET  /admin/operator/folder-status  day-by-day complete/partial/missing
#
# Cron routes (CRON_SECRET via X-Cron-Secret header):
#   POST /cron/daily-rollup    intended after market close
#   POST /cron/weekly-rollup   aggregate already-created daily folders
#
# Read-only routes serve from Supabase first; disk fallback returns
# source='disk_fallback' so consumers can tell which path they got.
# =============================================================================

import os as _os_pr91

CRON_SECRET = _os_pr91.environ.get("CRON_SECRET", "").strip()


def _require_cron_secret():
    """Header-based gate for /cron/* routes. Returns Flask response on
    failure or None on pass."""
    if not CRON_SECRET:
        return jsonify({"ok": False, "error": "CRON_SECRET not configured"}), 503
    supplied = request.headers.get("X-Cron-Secret", "")
    import hmac as _hmac_cron
    if not _hmac_cron.compare_digest(supplied, CRON_SECRET):
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    return None


def _parse_date_arg(value):
    """Accept 'YYYY-MM-DD' or None -> today (UTC). Raises ValueError on bad input."""
    from datetime import datetime as _dt, timezone as _tz
    if value is None or value == "":
        return _dt.now(_tz.utc).date()
    return _dt.strptime(str(value)[:10], "%Y-%m-%d").date()


def _run_daily_rollup(date_value, *, disk_export=True):
    """Shared helper for admin + cron daily rollup. Builds, upserts,
    and returns the response dict."""
    from ap_operator_daily_folders import (
        build_daily_operator_folder, upsert_daily_folder,
    )
    folder = build_daily_operator_folder(
        date_value,
        disk_export=disk_export,
    )
    wrote = upsert_daily_folder(folder)
    return {
        "ok":               True,
        "date":             folder.folder_date.isoformat(),
        "iso_week":         folder.iso_week,
        "sections_written": [s for s in folder.sections.keys()
                              if s not in folder.missing_sections],
        "missing_sections": list(folder.missing_sections),
        "section_errors":   dict(folder.section_errors),
        "source_status":    folder.source_status,
        "supabase_written": bool(wrote),
    }


def _run_weekly_rollup(*, iso_week=None, date_value=None, disk_export=True):
    from ap_operator_daily_folders import iso_week_string
    from ap_operator_weekly_rollup import (
        build_weekly_rollup, upsert_weekly_rollup,
    )
    if iso_week:
        target = iso_week
    elif date_value is not None:
        target = iso_week_string(date_value)
    else:
        from datetime import datetime as _dt, timezone as _tz
        target = iso_week_string(_dt.now(_tz.utc).date())
    rollup = build_weekly_rollup(target, disk_export=disk_export)
    wrote = upsert_weekly_rollup(rollup)
    public = rollup.to_public()
    public["supabase_written"] = bool(wrote)
    return public


@app.post("/admin/daily-rollup/run")
@_require_admin
def admin_daily_rollup_run():
    """Build the full daily operator folder, upsert to Supabase,
    optionally mirror to disk. Body: {date, force}."""
    try:
        body = request.get_json(silent=True) or {}
        target_date = _parse_date_arg(body.get("date"))
    except ValueError as e:
        return jsonify({"ok": False, "error": f"bad date: {e}"}), 400
    try:
        out = _run_daily_rollup(target_date, disk_export=True)
    except Exception as e:
        admin_log.error("daily-rollup/run failed: %s", e)
        return jsonify({"ok": False, "error": f"daily rollup failed: {e}"}), 500
    return jsonify(out)


@app.get("/admin/daily-rollup/")
@_require_admin
def admin_daily_rollup_get():
    """Return a stored daily folder. Supabase preferred, disk fallback.
    Response includes source='supabase' or source='disk_fallback'."""
    raw = request.args.get("date")
    try:
        target_date = _parse_date_arg(raw)
    except ValueError as e:
        return jsonify({"ok": False, "error": f"bad date: {e}"}), 400
    try:
        from ap_operator_daily_folders import fetch_daily_folder, read_disk_export
        row = fetch_daily_folder(target_date)
        if row is not None:
            return jsonify({"ok": True, "source": "supabase", "folder": row})
        disk = read_disk_export(target_date)
        if disk is not None:
            return jsonify({"ok": True, "source": "disk_fallback", "folder": disk})
        return jsonify({"ok": False, "error": "not_found", "date": target_date.isoformat()}), 404
    except Exception as e:
        admin_log.error("daily-rollup GET failed: %s", e)
        return jsonify({"ok": False, "error": f"daily rollup read failed: {e}"}), 500


@app.post("/admin/weekly-rollup/run")
@_require_admin
def admin_weekly_rollup_run():
    """Build weekly rollup from daily artifacts. Body: {date} or {iso_week}."""
    body = request.get_json(silent=True) or {}
    iso_week = body.get("iso_week")
    raw_date = body.get("date")
    try:
        date_value = _parse_date_arg(raw_date) if raw_date else None
    except ValueError as e:
        return jsonify({"ok": False, "error": f"bad date: {e}"}), 400
    try:
        out = _run_weekly_rollup(iso_week=iso_week, date_value=date_value, disk_export=True)
    except Exception as e:
        admin_log.error("weekly-rollup/run failed: %s", e)
        return jsonify({"ok": False, "error": f"weekly rollup failed: {e}"}), 500
    return jsonify({"ok": True, **out})


@app.get("/admin/weekly-rollup/")
@_require_admin
def admin_weekly_rollup_get():
    """Read a stored weekly rollup row by ISO week (or by date)."""
    iso_week = request.args.get("iso_week") or None
    raw_date = request.args.get("date") or None
    if not iso_week and raw_date:
        try:
            from ap_operator_daily_folders import iso_week_string
            iso_week = iso_week_string(_parse_date_arg(raw_date))
        except ValueError as e:
            return jsonify({"ok": False, "error": f"bad date: {e}"}), 400
    if not iso_week:
        return jsonify({"ok": False, "error": "iso_week or date required"}), 400
    try:
        from ap_operator_weekly_rollup import fetch_weekly_rollup
        row = fetch_weekly_rollup(iso_week)
        if row is None:
            return jsonify({"ok": False, "error": "not_found", "iso_week": iso_week}), 404
        return jsonify({"ok": True, "source": "supabase", "weekly": row})
    except Exception as e:
        admin_log.error("weekly-rollup GET failed: %s", e)
        return jsonify({"ok": False, "error": f"weekly rollup read failed: {e}"}), 500


@app.get("/admin/operator/folder-status")
@_require_admin
def admin_operator_folder_status():
    """Day-by-day status grid for the weekly archive dashboard. Returns
    the same daily_index / days_present / days_missing / section_errors_by_day
    structure as the weekly rollup, but built on-demand without writing."""
    iso_week = request.args.get("iso_week") or None
    raw_date = request.args.get("date") or None
    try:
        from ap_operator_daily_folders import iso_week_string
        if not iso_week:
            iso_week = iso_week_string(_parse_date_arg(raw_date))
        from ap_operator_weekly_rollup import build_weekly_rollup
        rollup = build_weekly_rollup(iso_week, disk_export=False)
        return jsonify({"ok": True, **rollup.to_public()})
    except Exception as e:
        admin_log.error("operator/folder-status failed: %s", e)
        return jsonify({"ok": False, "error": f"folder status failed: {e}"}), 500


@app.post("/cron/daily-rollup")
def cron_daily_rollup():
    """Cron entry point: build prior trading day daily folder. Triggered
    after market close. Protected by CRON_SECRET via X-Cron-Secret header."""
    guard = _require_cron_secret()
    if guard is not None:
        return guard
    body = request.get_json(silent=True) or {}
    raw_date = body.get("date")
    try:
        if raw_date:
            target_date = _parse_date_arg(raw_date)
        else:
            # After market close: rollup TODAY (US trading day already closed).
            # If invoked outside trading hours (overnight cron), default to today;
            # caller can override with explicit date.
            from datetime import datetime as _dt, timezone as _tz
            target_date = _dt.now(_tz.utc).date()
    except ValueError as e:
        return jsonify({"ok": False, "error": f"bad date: {e}"}), 400
    try:
        out = _run_daily_rollup(target_date, disk_export=True)
    except Exception as e:
        admin_log.error("cron/daily-rollup failed: %s", e)
        return jsonify({"ok": False, "error": f"daily rollup failed: {e}"}), 500
    return jsonify(out)


@app.post("/cron/weekly-rollup")
def cron_weekly_rollup():
    """Cron entry point: aggregate daily folders into the weekly archive.
    Does NOT silently skip missing days ‚Äî they are surfaced in days_missing."""
    guard = _require_cron_secret()
    if guard is not None:
        return guard
    body = request.get_json(silent=True) or {}
    iso_week = body.get("iso_week")
    raw_date = body.get("date")
    try:
        date_value = _parse_date_arg(raw_date) if raw_date else None
    except ValueError as e:
        return jsonify({"ok": False, "error": f"bad date: {e}"}), 400
    try:
        out = _run_weekly_rollup(iso_week=iso_week, date_value=date_value, disk_export=True)
    except Exception as e:
        admin_log.error("cron/weekly-rollup failed: %s", e)
        return jsonify({"ok": False, "error": f"weekly rollup failed: {e}"}), 500
    return jsonify({"ok": True, **out})


@app.get("/health")
def health_basic():
    """Basic liveness check ‚Äî no auth required."""
    import time as _t
    return jsonify({"ok": True, "status": "healthy", "ts": _t.time()}), 200


@app.get("/execution/health")
@require_hmac
def execution_health():
    """Execution component health ‚Äî fill monitor, reconciler, order worker.

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
            expected_members = None  # probe failed ‚Äî don't block on it

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
                    f"schema mismatch or boot failure ‚Äî check logs and run "
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
    """Scanner health ‚Äî reads from ap_signals for today's activity."""
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
    """Intelligence gate health ‚Äî reads decision breakdown from ap_signals."""
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
