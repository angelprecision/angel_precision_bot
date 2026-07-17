# client_runner.py -- Multi-client trading loop for Angel Precision Bot
# =============================================================================
# Each active member gets their own isolated trading thread with:
# - APMasterControl -- sole decision authority
# - APPositionManager -- Postgres-backed position truth
# - APOrderStateMachine -- enforced order lifecycle
# - APContractSelector -- real premium-based contract selection
# - APExecutionCore -- entry watcher, exit engine, signal tracker
# - fill_monitor_loop -- broker reconciliation, OSM-routed
# - worker_loop() -- queue dispatch through control stack
#
# Hardened lifecycle fixes included:
# 25. Runner registry cannot keep half-initialized runners forever.
# 26. Removed stop/delete race; runners are stopped, joined/observed, then removed.
# 27. Phantom order cleanup is age-gated and limited to truly orphaned orders.
# 28. Position sizer thresholds are per-client equity based by default.
# 29. Missing exit engine is fatal.
# 30. Missing/dead fill monitor is fatal.
# 31. Worker thread will not start without entry_watcher.
# 32. Runner owns final cleanup/unregistration; supervisor does not delete live threads prematurely.
# 33. Degraded-mode state freezes new entries without losing shutdown/exit visibility.
# 34. Startup manifest + control-stack validation required before initialized=True.
# 35. Runtime health loop continuously verifies worker/fill-monitor/core integrity.
#
# Wiring additions (see inline WIRE-N tags):
# WIRE-1  probe_db_rowcount() called once at supervisor startup. If the DB driver
#         returns None for rowcount and OSM_ROWCOUNT_NONE_IS_FATAL=1 (default),
#         the supervisor raises RuntimeError before spawning any runner thread.
# WIRE-2  get_split_brain_orders() audited per-client after OSM construction.
#         If leftover split-brain orders exist from a prior session, runner enters
#         degraded mode immediately, blocking new entries until reconciler recovers.
# WIRE-3  _on_split_brain_detected() callback passed to worker_loop so that any
#         submit that returns {"split_brain": True} at runtime freezes the client
#         immediately without waiting for the health loop.
# WIRE-4  install_exit_quarantine_patch imported from ap.order_state_machine
#         (now a no-op shim there) instead of the old standalone patch file.
# =============================================================================

from __future__ import annotations

import base64
import hashlib
import logging
import os
import threading
import time
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

from cryptography.fernet import Fernet
from supabase import create_client, Client

from ap.db import run_with_retry
from ap.queue import enqueue_signal, worker_loop
from ap.order_monitor import APOrderMonitor
from ap.position_sizer import APPositionSizer
from ap.market_intelligence import APEarningsGuard, APIVRankFilter
from ap.worker_health import get_monitor, init_monitor
from ap_reconciler import APBrokerReconciler
from ap_recovery import APStartupRecovery
from ap.self_healing import get_healer, init_self_healing

try:
    from ap.position_quote_monitor import APPositionQuoteMonitor
except Exception as _qpm_import_exc:
    APPositionQuoteMonitor = None
    # logger is defined just below; warning is emitted after logger initialization.

logger = logging.getLogger("client_runner")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)

if APPositionQuoteMonitor is None:
    try:
        logger.warning("APPositionQuoteMonitor import failed: %s", _qpm_import_exc)
    except Exception:
        pass

# COHESION-2 FIX: apply the (now no-op) shim once at module import time, not
# per-runner startup. A real class-level patch should only run once per process,
# not once per client thread restart. Keeping it here so if the shim is ever
# un-shimmed, it won't silently apply N times.
try:
    from ap.order_state_machine import install_exit_quarantine_patch, APOrderStateMachine as _OSM_CLASS
    install_exit_quarantine_patch(_OSM_CLASS)
    logger.debug("Exit quarantine shim confirmed (all protections native in APOrderStateMachine)")
except Exception as _patch_exc:
    logger.warning("Could not apply exit quarantine shim at import: %s", _patch_exc)

_ET = ZoneInfo("America/New_York")
_time_module: object = time
_members_cache: dict = {}


def _autonomy_log_context(execution_mode: str | None = None) -> dict:
    commit_sha = (
        os.getenv("RENDER_GIT_COMMIT")
        or os.getenv("COMMIT_SHA")
        or os.getenv("GITHUB_SHA")
        or "unknown"
    )
    mode = str(execution_mode or "").strip().lower()
    paper_client_count = None
    live_client_count = None
    try:
        with _registry_lock:
            paper_client_count = sum(
                1
                for runner in _active_runners.values()
                if str(getattr(runner, "mode", "") or "").strip().lower() == "paper"
            )
            live_client_count = sum(
                1
                for runner in _active_runners.values()
                if str(getattr(runner, "mode", "") or "").strip().lower() == "live"
            )
            client_count = sum(
                1
                for runner in _active_runners.values()
                if not mode or str(getattr(runner, "mode", "") or "").strip().lower() == mode
            )
    except Exception:
        client_count = None
    return {
        "commit_sha": str(commit_sha)[:12],
        "pod_id": os.getenv("POD_ID", "").strip() or "unknown",
        "client_count": client_count,
        "paper_client_count": paper_client_count,
        "live_client_count": live_client_count,
        "execution_mode": mode,
    }


SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")

# PR D / SECURITY CRITICAL (BUG-CR-1):
# The previous default "angel-precision-encrypt-2026" was a human-readable
# string committed to source control. Any operator who relied on that
# default had broker tokens encrypted with a key visible in the public
# repo. The default is now a deterministic-but-non-obvious sentinel that
# is BLOCKED in LIVE mode (see _run_inner's LIVE-safety section) so any
# operator who forgets to set ENCRYPTION_KEY in Render cannot accidentally
# go live with a known-key encryption. PAPER mode still accepts the
# default (dev convenience).
_DEFAULT_KEY_SENTINEL = "DEFAULT_INSECURE_KEY_SET_ENCRYPTION_KEY_IN_RENDER_ENV"
_raw_key = os.getenv("ENCRYPTION_KEY", _DEFAULT_KEY_SENTINEL).strip()

# Validate the key is usable as a Fernet key at import time — surfaces misconfiguration early
try:
    _key_bytes = hashlib.sha256(_raw_key.encode()).digest()
    _fernet_test = Fernet(base64.urlsafe_b64encode(_key_bytes))
    del _fernet_test, _key_bytes
    logger.debug("ENCRYPTION_KEY validated (Fernet-capable)")
except Exception as _key_err:
    logger.critical("ENCRYPTION_KEY is invalid or Fernet import failed: %s", _key_err)


def _env_bool(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


def _new_scheme_fernet(raw_key: str) -> "Fernet":
    """NEW scheme: ENCRYPTION_KEY is a raw 44-char base64 Fernet key used
    directly. Matches ap/crypto.encrypt_token.
    """
    return Fernet(raw_key.encode())


def _legacy_scheme_fernet(raw_key: str) -> "Fernet":
    """LEGACY scheme: SHA256(ENCRYPTION_KEY) → urlsafe-b64 → Fernet key.

    Kept so tokens encrypted by older versions of this runner still decrypt
    while we migrate. New tokens must use the NEW scheme.
    """
    key_bytes = hashlib.sha256(raw_key.encode()).digest()
    return Fernet(base64.urlsafe_b64encode(key_bytes))


def decrypt_token(ciphertext: str, mode: str = "PAPER") -> str:
    """Decrypt a Fernet-encrypted token.

    Tries TWO schemes (in order):
      1. NEW direct Fernet key  — matches ap.crypto.encrypt_token (the path
         tokens take when produced via dashboard/admin onboarding).
      2. LEGACY SHA256-derived  — tokens encrypted by the old runner code
         path; we still accept these so live deployments do not break
         during the migration window.

    PAPER mode: falls back to plaintext if value looks unencrypted or both
                schemes fail. Allows dev/sandbox setups to work without
                ENCRYPTION_KEY.
    LIVE mode:  any decrypt failure is fatal — raises RuntimeError.
                Plaintext tokens are never accepted in LIVE mode. No trade
                should execute with an unverified broker credential.
    """
    if not ciphertext:
        raise ValueError("empty token")

    is_live = str(mode).upper() == "LIVE"

    # Fernet tokens always start with "gAAAAA"
    looks_encrypted = ciphertext.startswith("gAAAAA")

    if not looks_encrypted:
        if is_live:
            raise RuntimeError(
                "LIVE mode requires an encrypted Tradier token (gAAAAA... prefix). "
                "Plaintext tokens are not permitted in LIVE mode — re-encrypt via dashboard."
            )
        # PAPER: plaintext is acceptable
        return ciphertext

    if not _raw_key:
        if is_live:
            raise RuntimeError(
                "LIVE mode requires ENCRYPTION_KEY env var to decrypt the Tradier token. "
                "Set ENCRYPTION_KEY in Render environment variables."
            )
        logger.error("ENCRYPTION_KEY not set — cannot decrypt Fernet token; PAPER fallback to ciphertext as plaintext")
        return ciphertext

    token_bytes = ciphertext.encode()

    # PR D / FIX-7: Tradier access tokens are ~28+ char alphanumeric.
    # Anything shorter than 6 chars indicates: (a) accidental double-
    # encryption (a Fernet ciphertext encrypted again), (b) a corrupted
    # token, or (c) a successful decrypt of a garbage value that happened
    # to be valid Fernet structure but wrong content. Reject before the
    # token is handed to a Tradier HTTP client that would silently 401.
    # Threshold set to 6 (not the audit-suggested 10) so existing dev
    # fixtures with deliberately-short test tokens (e.g., "token-xyz")
    # keep working while genuinely-garbage (1–5 char) decrypts are caught.
    _MIN_TOKEN_LEN = 6

    # Use a dedicated subclass so the validation-rejection path is
    # distinguishable from cryptography.fernet's own ValueError (which
    # is also raised by Fernet() construction for non-base64 raw keys).
    # Without this distinction the validation `except ValueError: raise`
    # would swallow Fernet-key-format errors and block fall-through to
    # the LEGACY scheme.
    class _PlaintextSanityError(ValueError):
        pass

    def _validate_plaintext(pt: str, *, scheme: str) -> str:
        if not pt:
            raise _PlaintextSanityError(
                f"decrypt_token: {scheme} scheme decrypted to EMPTY plaintext — "
                f"invalid token (mode={mode})"
            )
        if len(pt) < _MIN_TOKEN_LEN:
            raise _PlaintextSanityError(
                f"decrypt_token: {scheme} scheme decrypted to suspiciously short "
                f"plaintext (length={len(pt)}, min={_MIN_TOKEN_LEN}) — likely "
                f"corrupted, double-encrypted, or wrong key (mode={mode})"
            )
        return pt

    # 1) NEW scheme — direct Fernet key (matches ap/crypto.encrypt_token).
    new_err: Exception | None = None
    try:
        _pt = _new_scheme_fernet(_raw_key).decrypt(token_bytes).decode()
        return _validate_plaintext(_pt, scheme="NEW")
    except _PlaintextSanityError:
        # Sanity-validation rejection — do NOT fall through to LEGACY.
        # An empty/short plaintext means the key decrypted to garbage;
        # the legacy scheme will not produce something different.
        raise
    except Exception as _new_err:
        # Includes cryptography.fernet.InvalidToken AND Fernet-key-format
        # ValueError (raised when _raw_key is not a valid base64 Fernet
        # key, which is the common case for LEGACY-only deployments).
        new_err = _new_err

    # 2) LEGACY scheme — SHA256-derived Fernet key.
    try:
        plaintext = _legacy_scheme_fernet(_raw_key).decrypt(token_bytes).decode()
        plaintext = _validate_plaintext(plaintext, scheme="LEGACY")
        logger.warning(
            "Token decrypted with LEGACY scheme — re-encrypt via dashboard to migrate"
        )
        return plaintext
    except _PlaintextSanityError:
        # Sanity-validation rejection on the legacy path — propagate as-is.
        raise
    except Exception as legacy_err:
        if is_live:
            raise RuntimeError(
                "LIVE mode token decryption failed under BOTH schemes — wrong "
                "ENCRYPTION_KEY or corrupted token. "
                f"new_scheme_error={new_err!r}; "
                f"legacy_scheme_error={legacy_err!r}. "
                "Fix ENCRYPTION_KEY in Render env vars."
            ) from legacy_err
        # PAPER: log loudly but continue with sandbox so dev iteration keeps moving.
        logger.error(
            "decrypt_token: Fernet decrypt failed under BOTH schemes "
            "(key='%s...') — PAPER mode, returning raw value. "
            "This will fail at Tradier auth if the token is actually encrypted. "
            "new_err=%r legacy_err=%r",
            _raw_key[:6], new_err, legacy_err,
        )
        return ciphertext


_active_runners: dict[str, "ClientRunner"] = {}
_registry_lock = threading.RLock()

# Ring buffer of the last 20 runner failure events — survives runner removal
_runner_failure_log: list[dict] = []
_failure_log_lock = threading.Lock()


def _record_runner_failure(email: str, reason: str) -> None:
    import time as _time
    with _failure_log_lock:
        _runner_failure_log.append({"email": email, "reason": reason, "ts": _time.time()})
        if len(_runner_failure_log) > 20:
            _runner_failure_log.pop(0)


def get_runner_failure_log() -> list[dict]:
    with _failure_log_lock:
        return list(_runner_failure_log)


# Supervisor state — updated by the supervisor loop so we can inspect it via API
_supervisor_state: dict = {
    "started": False,
    "last_sync_ts": 0.0,
    "last_member_count": 0,
    "last_sync_error": "",
    "sync_count": 0,
    "thread_alive": False,
}
_supervisor_ref: dict = {"thread": None}  # mutable container to avoid global reassignment scope issues


def get_supervisor_state() -> dict:
    import time as _time
    return {
        **_supervisor_state,
        "thread_alive": _supervisor_ref["thread"].is_alive() if _supervisor_ref["thread"] else False,
        "active_runner_count": len(_active_runners),
        "now_ts": _time.time(),
    }

# Broker reconciler is disabled by default because the rich fill monitor is
# already the primary broker-order reconciliation path.
ENABLE_BROKER_RECONCILER = _env_bool("ENABLE_BROKER_RECONCILER", "0")

# PR D / FIX-5 (hardening): grace window between fill-monitor heartbeat
# loss and entries-blocked. Default 300s tolerates Render cold-starts
# with Supabase connection pooling (30-60s for first DB connection) +
# supervisor delay (15s) + runner init + QPM startup. Old 120s was too
# tight — caused false-degraded states during normal restarts.
# Resolved ONCE at module load to avoid os.getenv overhead in the hot
# path _set_entry_permission method (called ~3x/min per runner).
FILL_MONITOR_GRACE_SEC = int(os.getenv("FILL_MONITOR_GRACE_SEC", "300"))

# Cross-process fanout fallback can enqueue for members whose runner is not
# initialized in this worker. Keep it explicit so degraded-mode protection is
# not bypassed accidentally.
#
# Production default should stay OFF unless the receiving queue worker/master
# control performs its own client-health/degraded-state revalidation.
ALLOW_SUPABASE_FANOUT_FALLBACK = _env_bool("ALLOW_SUPABASE_FANOUT_FALLBACK", "0")


def resolve_tradier_credentials(member: dict) -> dict:
    """
    Resolve Tradier credentials for this Render service's mode.

    BOT_MODE=PAPER (default):
        Uses tradier_paper_account_id / tradier_paper_access_token.
        Falls back to tradier_account_id / tradier_access_token if paper-specific
        columns are empty (backward compat for existing rows).
        base_url always sandbox.tradier.com regardless of member row.

    BOT_MODE=LIVE:
        Requires tradier_live_account_id and tradier_live_access_token.
        Does NOT fall back to active/paper columns — fails closed if missing.
        Also requires AP_LIVE_TRADING=1 to be explicitly set on the service.

    This is the single source of truth for credential resolution.
    Call once in ClientRunner.__init__ and store results on self.
    """
    bot_mode     = os.getenv("BOT_MODE", os.getenv("MODE", "PAPER")).strip().upper()
    live_enabled = os.getenv("AP_LIVE_TRADING", "0").strip().lower() in {"1", "true", "yes", "on"}
    email        = member.get("email", "?")

    if bot_mode == "LIVE":
        if not live_enabled:
            raise RuntimeError(
                f"[{email}] BOT_MODE=LIVE but AP_LIVE_TRADING is not enabled — "
                "set AP_LIVE_TRADING=1 on the live Render service"
            )
        account_id = member.get("tradier_live_account_id")
        token      = member.get("tradier_live_access_token")
        if account_id:
            account_id = str(account_id).strip()
        if not account_id or not token:
            raise RuntimeError(
                f"[{email}] LIVE startup missing tradier_live_account_id or "
                "tradier_live_access_token — live mode fails closed"
            )
        return {
            "mode":         "LIVE",
            "account_id":   account_id,
            "access_token": token,
            "base_url":     "https://api.tradier.com",
        }

    # PAPER — use paper-specific columns, fall back to generic active columns.
    # Priority: tradier_paper_access_token > tradier_access_token (generic).
    # Handles early onboarding where paper-specific token not yet captured.
    # base_url is always sandbox regardless of which token source is used.
    account_id = (
        member.get("tradier_paper_account_id")
        or member.get("tradier_account_id")
    )
    token = (
        member.get("tradier_paper_access_token")
        or member.get("tradier_access_token")
    )
    if account_id:
        account_id = str(account_id).strip()

    # token_source logged so we can confirm in Render logs which path was taken
    if member.get("tradier_paper_access_token"):
        token_source = "paper_specific"
    elif member.get("tradier_access_token"):
        token_source = "generic_fallback"
    else:
        token_source = "missing"

    if not account_id or not token:
        raise RuntimeError(
            f"[{email}] PAPER startup: no usable Tradier credentials. "
            f"paper_account_id={'set' if member.get('tradier_paper_account_id') else 'MISSING'} "
            f"paper_token={'set' if member.get('tradier_paper_access_token') else 'MISSING'} "
            f"generic_token(fallback)={'set' if member.get('tradier_access_token') else 'MISSING'}"
        )

    logger.info(
        "[%s] PAPER credentials resolved | account=%s token_source=%s | base=sandbox",
        email,
        (account_id or "")[:4] + "****",
        token_source,
    )
    return {
        "mode":         "PAPER",
        "account_id":   account_id,
        "access_token": token,
        "base_url":     "https://sandbox.tradier.com",
        "token_source": token_source,
    }


# =============================================================================
# PR #150 — Paper Selector Market-Data Transport Split
# =============================================================================
#
# Splits market-data transport from execution transport for the contract
# selector. PAPER clients execute orders against the sandbox broker (15-min
# delayed quotes) but must source CHAIN / QUOTE / EARNINGS / IV data from a
# live read-only Tradier endpoint so:
#   - Strike selection uses real-time chains (not 15-min-stale chains).
#   - Spread / liquidity gates evaluate against real quotes.
#   - Earnings guard / IV filter run against live underlying data.
#
# The watcher (ap_entry_watcher._fetch_quotes) already implements this split
# and fails closed on PAPER if no market-data token resolves. This helper
# brings the contract selector / earnings guard / IV filter onto the SAME
# transport with the SAME token-priority order, so a paper Render pod that
# has TRADIER_MARKET_DATA_TOKEN configured for the watcher will automatically
# have it for the selector too — and a misconfigured pod fails closed at
# startup instead of silently picking strikes off stale sandbox chains.
#
# LIVE behavior is preserved exactly: if no market-data token resolves in
# LIVE mode, data_broker falls back to the live execution broker (which
# already has live market-data access via the live execution token), with
# only a warning log. There is no LIVE submit-path change.
#
# Submit / cancel / order-status routing is NEVER touched by this helper.
# Those continue to use the execution broker built from
# resolve_tradier_credentials() — which is sandbox for paper and live for
# live, by construction.
# =============================================================================

class PaperSelectorNoMarketDataTokenError(RuntimeError):
    """Raised when PAPER mode runner has no live market-data token configured.

    The selector cannot safely fall back to the sandbox execution broker for
    market data because sandbox quotes are delayed up to ~15 minutes, and the
    watcher (which uses live data) will then disagree with the selector on
    every breach evaluation. Fail closed at startup rather than ship bad
    paper trades.
    """


def resolve_market_data_transport(
    *,
    mode: str,
    account_id: str,
    execution_broker,
    env=None,
    broker_cls=None,
    broker_config_cls=None,
) -> dict:
    """Resolve the market-data transport for the contract selector / guards.

    Returns a dict:
        {
          "data_broker":            <TradierBroker | execution_broker>,
          "base_url":               "<resolved base url>",
          "token_source":           "TRADIER_MARKET_DATA_TOKEN"
                                    | "TRADIER_DATA_TOKEN"
                                    | "execution_broker_fallback_live",
          "is_dedicated":           bool,
          "execution_base_url":     "<execution broker base_url>",
        }

    Token priority (matches ap_entry_watcher._resolve_watcher_quote_transport):
        1. TRADIER_MARKET_DATA_TOKEN
        2. TRADIER_DATA_TOKEN

    Base URL priority:
        1. TRADIER_MARKET_DATA_BASE_URL
        2. TRADIER_DATA_BASE_URL
        3. https://api.tradier.com  (hardcoded live default — NEVER sandbox)

    PAPER mode: raises PaperSelectorNoMarketDataTokenError if no token resolves.
    LIVE mode:  returns execution_broker as data_broker if no token resolves.

    `env`, `broker_cls`, `broker_config_cls` are dependency-injection seams
    for unit tests — production callers pass None and the real env / classes
    are used. This keeps the resolver fully testable without touching real
    network / global env.
    """
    if env is None:
        env = os.environ
    if broker_cls is None:
        from ap.brokers.tradier import TradierBroker as _Broker
        broker_cls = _Broker
    if broker_config_cls is None:
        from ap.brokers.tradier import TradierConfig as _Cfg
        broker_config_cls = _Cfg

    mode_upper = str(mode or "").strip().upper()
    is_paper = (mode_upper == "PAPER")

    # ── Token resolution ──────────────────────────────────────────────────
    mdt = (env.get("TRADIER_MARKET_DATA_TOKEN") or "").strip()
    dt  = (env.get("TRADIER_DATA_TOKEN") or "").strip()
    if mdt:
        token, token_source = mdt, "TRADIER_MARKET_DATA_TOKEN"
    elif dt:
        token, token_source = dt, "TRADIER_DATA_TOKEN"
    else:
        token, token_source = "", "missing"

    # ── Base URL resolution ───────────────────────────────────────────────
    base_url = (
        (env.get("TRADIER_MARKET_DATA_BASE_URL") or "").strip()
        or (env.get("TRADIER_DATA_BASE_URL") or "").strip()
        or "https://api.tradier.com"
    )

    # Hard guard: market-data base URL must never be sandbox. Sandbox quotes
    # are delayed and using them as the selector's truth source defeats the
    # entire purpose of the split. Force live and warn loudly.
    if "sandbox" in base_url.lower():
        logger.critical(
            "PAPER_SELECTOR_MARKET_DATA_BASE_URL_SANDBOX_GUARD_TRIGGERED "
            "resolved_url=%s — sandbox URL must not be used for selector "
            "market data. Forcing https://api.tradier.com.",
            base_url,
        )
        base_url = "https://api.tradier.com"

    execution_base_url = getattr(getattr(execution_broker, "cfg", None), "base_url", "")

    # ── PAPER: fail closed if no token resolved ───────────────────────────
    if is_paper and not token:
        raise PaperSelectorNoMarketDataTokenError(
            "PAPER_SELECTOR_NO_MARKET_DATA_TOKEN mode=PAPER "
            f"base_url={base_url} token_source={token_source} "
            "— contract selector requires a live market-data token to avoid "
            "picking strikes against 15-min-delayed sandbox chains. "
            "Set TRADIER_MARKET_DATA_TOKEN (preferred) or TRADIER_DATA_TOKEN "
            "on the paper Render pod."
        )

    # ── Build the data broker ─────────────────────────────────────────────
    if token:
        cfg = broker_config_cls(
            base_url=base_url,
            access_token=token,
            account_id=account_id,
        )
        data_broker = broker_cls(cfg)
        is_dedicated = True
    else:
        # LIVE-only path: live execution broker already has live market data.
        data_broker = execution_broker
        token_source = "execution_broker_fallback_live"
        base_url = execution_base_url or base_url
        is_dedicated = False

    return {
        "data_broker":        data_broker,
        "base_url":           base_url,
        "token_source":       token_source,
        "is_dedicated":       is_dedicated,
        "execution_base_url": execution_base_url,
    }


class ClientRunner(threading.Thread):
    """
    One daemon thread per client. The runner owns all per-client subsystems and
    cleans them up in run(). The supervisor may request stop, but final removal
    from _active_runners is runner-owned to avoid cleanup-order races.
    """

    def __init__(self, member: dict):
        super().__init__(daemon=True, name=f"runner-{member['email']}")
        self.member = member
        self.email  = member["email"]
        # p0/guard-startup-phantom-clear: marker used by _clear_old_phantom_orders
        # so the cleanup never wipes ENTRY orders created during this runner's
        # startup grace window (default 180s). Set at construction so any
        # cleanup that fires before run() has the right reference.
        import time as _time
        self._runner_startup_ts = _time.time()

        # Resolve credentials once at construction — single source of truth.
        # Raises RuntimeError if credentials are missing or mode is misconfigured.
        _resolved                    = resolve_tradier_credentials(member)
        self.mode                    = _resolved["mode"]
        self.account_id              = _resolved["account_id"]
        self.base_url                = _resolved["base_url"]
        self._resolved_tradier_token = _resolved["access_token"]

        self.stopped = threading.Event()
        self.initialized = threading.Event()
        self.failed = threading.Event()
        self.stopping = threading.Event()
        self.degraded = threading.Event()
        self.entries_allowed = threading.Event()

        self.degraded_reasons: set[str] = set()
        # PR D / FIX-4: protect degraded_reasons mutations across threads.
        # _enter_degraded_mode (worker/health), _clear_degraded_reason_key
        # (reconciler callback), and _try_recover_degraded_mode (health
        # loop) all rebuild this set via comprehension — a non-atomic
        # read-then-write that races without this lock.
        self._degraded_lock = threading.Lock()
        # PR D / FIX-2 (BUG-CR-4): explicit kill-switch flag. The wired
        # lambda in master_control.wire() now reads self.kill_switch_active
        # (true single-source-of-truth) instead of getattr(self.core,
        # "_kill_switch", False), which was dead code because self.core._
        # kill_switch is never set anywhere in the codebase. Dashboard /
        # admin paths use trip_kill_switch(reason) to flip this flag.
        self.kill_switch_active: bool = False
        self.kill_switch_reason: str  = ""
        self.startup_manifest: dict = {}

        self.last_health_check_ts = 0.0
        self.last_fill_monitor_heartbeat_ts = 0.0
        self.last_worker_heartbeat_ts = 0.0
        self.last_equity_heartbeat_ts = 0.0

        self.core = None
        self.master_control = None
        self.reconciler = None
        self.position_manager = None
        self.order_state_machine = None
        self.contract_selector = None
        self.order_monitor = None
        self.quotemonitor = None
        self.quote_monitor = None
        self.broker = None
        self.data_broker = None
        self.databroker = None
        self.fill_monitor_thread = None
        self.worker_thread = None
        self.intelligence_context_thread = None
        self.equity_thread = None
        self.health_thread = None
        self.failure_reason: str = ""

    def trip_kill_switch(self, reason: str = "manual_trip") -> None:
        """PR D / FIX-2 (BUG-CR-4): public setter to trip the kill switch.

        Wired through master_control.kill_switch_fn — once True, master_control
        blocks all new entries with reason='kill_switch_active' AND, when
        kill_switch first transitions to True, fires request_force_close_all()
        on every open position. Designed to be called from the dashboard /
        admin tooling (NOT from the runner thread itself); the flag read is
        a single boolean lookup so there is no race on the master_control
        side.
        """
        reason = str(reason or "manual_trip")
        self.kill_switch_active = True
        self.kill_switch_reason = reason
        logger.critical(
            "[%s] KILL SWITCH TRIPPED: reason=%s — master_control will block "
            "new entries and force-close open positions on the next tick.",
            self.email, reason,
        )

    def reset_kill_switch(self, reason: str = "manual_reset") -> None:
        """PR D / FIX-2: explicit reset path. Logged loudly because a
        kill-switch reset during market hours is a high-trust operation.
        """
        reason = str(reason or "manual_reset")
        was_active = self.kill_switch_active
        self.kill_switch_active = False
        self.kill_switch_reason = ""
        if was_active:
            logger.critical(
                "[%s] KILL SWITCH RESET: reason=%s — entries can resume "
                "once entries_allowed re-evaluates.",
                self.email, reason,
            )

    def _get_token(self) -> str | None:
        try:
            raw = getattr(self, "_resolved_tradier_token", None) or self.member.get("tradier_access_token", "")
            if not raw:
                return None
            return decrypt_token(raw, mode=self.mode)
        except RuntimeError as exc:
            # LIVE mode: decrypt failure is fatal — block runner startup
            logger.critical("[%s] FATAL token decrypt error: %s", self.email, exc)
            return None
        except Exception as exc:
            logger.error("[%s] Token decrypt failed: %s", self.email, exc)
            return None

    def _run_live_preflight(self, broker) -> tuple[bool, str]:
        """
        P0 (PR #261): LIVE runners must not start half-configured.

        Existing LIVE startup already fail-closes on: missing token, missing
        account_id, missing DATABASE_URL, unsafe flags (legacy fill monitor,
        immediate execution, non-fatal rowcount), default encryption key,
        and incomplete risk profile. What it did NOT verify is that the
        execution credentials actually WORK before startup calibrates any
        capital gate to a fake default equity.

        Checks (each fail-closed, first failure wins):
          1. client email present
          2. execution mode resolves to LIVE
          3. account_id present
          4. broker auth round-trip: get_account_equity() succeeds
          5. account equity > 0 (a $0 live account is misconfiguration)
          6. entry confirmation module importable/callable

        Returns (ok, reason). Sets self.live_preflight_status. Kill switch:
        LIVE_PREFLIGHT_ENABLED=0 (logged loudly; intended for emergencies
        only). Paper runners never call this.
        """
        if os.getenv("LIVE_PREFLIGHT_ENABLED", "1").strip().lower() not in ("1", "true", "yes"):
            logger.critical(
                "[%s] LIVE_PREFLIGHT_DISABLED_BY_ENV — starting WITHOUT live "
                "preflight verification. This must never be set in normal "
                "operation.", self.email,
            )
            self.live_preflight_status = "disabled_by_env"
            return True, "disabled_by_env"

        def _fail(reason: str) -> tuple[bool, str]:
            self.live_preflight_status = f"failed:{reason}"
            logger.critical(
                "[%s] LIVE_PREFLIGHT_FAILED reason=%s — refusing to start "
                "half-configured LIVE runner", self.email, reason,
            )
            return False, reason

        if not str(self.email or "").strip():
            return _fail("missing_client_email")
        if str(self.mode).strip().upper() != "LIVE":
            return _fail(f"execution_mode_not_live:{self.mode}")
        if not str(self.account_id or "").strip():
            return _fail("missing_account_id")

        try:
            from ap.trade_lifecycle_guards import lifecycle_guard_preflight
            _guards_ok, _guards = lifecycle_guard_preflight("live")
        except Exception as _exc:
            return _fail(f"lifecycle_guard_preflight_unavailable:{type(_exc).__name__}")
        if not _guards_ok:
            _missing = ",".join(_guards.get("missing_required_guards") or [])
            if not _guards.get("generation_claims_table_exists"):
                _missing = f"{_missing},exit_decision_generation_claims_migration".strip(",")
            return _fail(f"lifecycle_guard_preflight_failed:{_missing}")

        # 4–5. Broker auth round-trip + funded account.
        try:
            if not hasattr(broker, "get_account_equity"):
                return _fail("broker_missing_get_account_equity")
            _equity = float(broker.get_account_equity() or 0.0)
        except Exception as _exc:
            return _fail(f"broker_auth_profile_unreadable:{type(_exc).__name__}")
        if _equity <= 0:
            return _fail("account_equity_not_positive")

        # 6. Entry confirmation availability (deep default-required
        #    enforcement lives in the execution core; here we prove the
        #    module a LIVE submit depends on can actually load).
        try:
            from ap_entry_confirmation import check_entry_confirmation as _cec
            if not callable(_cec):
                return _fail("entry_confirmation_not_callable")
        except Exception as _exc:
            return _fail(f"entry_confirmation_unavailable:{type(_exc).__name__}")

        self.live_preflight_status = "ok"
        self.live_preflight_equity = float(_equity)
        return True, "ok"

    def _run_live_market_data_preflight(
        self,
        data_broker,
        *,
        data_broker_source: str,
        data_base_url: str,
        data_broker_is_dedicated: bool,
    ) -> tuple[bool, str]:
        """Verify LIVE market data using the resolved data transport."""
        def _fail(reason: str) -> tuple[bool, str]:
            self.live_preflight_status = f"failed:{reason}"
            logger.critical(
                "[%s] LIVE_PREFLIGHT_FAILED reason=%s — refusing to start "
                "half-configured LIVE runner", self.email, reason,
            )
            return False, reason

        try:
            _q = data_broker.get_quote("SPY") or {}
            _px = 0.0
            for _k in ("last", "bid", "ask", "close", "prevclose"):
                try:
                    _v = float(_q.get(_k) or 0)
                except (TypeError, ValueError):
                    _v = 0.0
                if _v > 0:
                    _px = _v
                    break
        except Exception as _exc:
            return _fail(f"market_data_unusable:{type(_exc).__name__}")
        if _px <= 0:
            return _fail("market_data_quote_not_positive")

        self.live_preflight_status = "ok"
        self.live_preflight_quote_spy = float(_px)
        self.live_preflight_quote_source = str(data_broker_source or "")
        self.live_preflight_quote_base_url = str(data_base_url or "")
        self.live_preflight_quote_broker_mode = (
            "dedicated_market_data_broker"
            if data_broker_is_dedicated
            else "execution_broker_fallback"
        )
        logger.critical(
            "[%s] LIVE_PREFLIGHT_OK equity=%.2f quote_spy=%.2f account_id=%s "
            "data_quote_source=%s data_quote_broker_mode=%s data_base_url=%s "
            "confirmation_module=present",
            self.email,
            float(getattr(self, "live_preflight_equity", 0.0) or 0.0),
            self.live_preflight_quote_spy,
            self.account_id,
            self.live_preflight_quote_source,
            self.live_preflight_quote_broker_mode,
            self.live_preflight_quote_base_url,
        )
        return True, "ok"

    def _verify_live_enforcement(
        self,
        *,
        score_floor: float,
        capital_pct: float,
        sector_pct: float,
        ticker_pct: float,
        max_calls: int,
        max_puts: int,
        daily_max_loss_pct: float,
    ) -> tuple[bool, str]:
        """
        P0 (PR #264): durable proof that a LIVE runner is under the enforced
        live policy — not stale code, not a paper/default policy, not a
        bypassed PR180 pricing guard.

        Fail-closed conditions (LIVE only):
          - ap_execution_core PR180 constants unimportable → the enforcement
            module itself is missing/stale
          - named live client (PR180_LIVE_CLIENT_IDS, e.g. Jason) with
            PR180_ENABLED off → enforcement bypassed
          - live client NOT in the enforcement allowlist while
            LIVE_REQUIRE_NAMED_ENFORCEMENT=1 (default) → a live runner can
            never start under paper/default policy

        On success: builds the enforcement snapshot (client, mode, pod,
        commit sha, PR180 state, confirmation-required state, intraday
        FAILED_DIR state, effective risk limits), computes
        runtime_config_hash = sha256(canonical JSON), stores
        self.live_enforcement / self.runtime_config_hash, and emits
        LIVE_ENFORCEMENT_OK at CRITICAL. The hash is pinned onto the OSM by
        the caller so EVERY order row carries enforcement_config_hash.

        Kill switch LIVE_ENFORCEMENT_PROOF_ENABLED=0 (loud; emergencies only).
        """
        if os.getenv("LIVE_ENFORCEMENT_PROOF_ENABLED", "1").strip().lower() not in ("1", "true", "yes"):
            logger.critical(
                "[%s] LIVE_ENFORCEMENT_PROOF_DISABLED_BY_ENV — starting WITHOUT "
                "enforcement verification. This must never be set in normal "
                "operation.", self.email,
            )
            self.live_enforcement = {"status": "disabled_by_env"}
            return True, "disabled_by_env"

        def _fail(reason: str) -> tuple[bool, str]:
            logger.critical(
                "[%s] LIVE_ENFORCEMENT_FAILED reason=%s — refusing to start "
                "LIVE runner outside the enforced live policy",
                self.email, reason,
            )
            self.live_enforcement = {"status": f"failed:{reason}"}
            return False, reason

        try:
            from ap_execution_core import (
                PR180_ENABLED as _pr180_enabled,
                PR180_MODE as _pr180_mode,
                PR180_LIVE_CLIENT_IDS as _pr180_ids,
            )
        except Exception as _exc:
            return _fail(f"pr180_module_unavailable:{type(_exc).__name__}")

        _client_lower = str(self.email or "").lower().strip()
        _is_named = _client_lower in _pr180_ids

        if _is_named and not _pr180_enabled:
            return _fail("pr180_disabled_for_named_live_client")

        _require_named = os.getenv(
            "LIVE_REQUIRE_NAMED_ENFORCEMENT", "1"
        ).strip().lower() in ("1", "true", "yes")
        if not _is_named and _require_named:
            return _fail("live_client_not_in_enforcement_allowlist")

        import hashlib
        import json as _json
        import socket

        _commit_sha = (
            os.getenv("RENDER_GIT_COMMIT")
            or os.getenv("GIT_COMMIT")
            or "unknown"
        ).strip()
        _pod_id = (
            os.getenv("RENDER_INSTANCE_ID")
            or os.getenv("RENDER_SERVICE_NAME")
            or socket.gethostname()
            or "unknown"
        ).strip()

        _confirmation_required = os.getenv(
            "LIVE_CONFIRMATION_REQUIRED", "1"
        ).strip().lower() in ("1", "true", "yes")
        _intraday_failed_dir_allowed = os.getenv(
            "ALLOW_FAILED_DIR_CLIENT", "0"
        ).strip().lower() in ("1", "true", "yes")

        enforcement = {
            "client_id": _client_lower,
            "execution_mode": str(self.mode).strip().upper(),
            "pod_id": _pod_id,
            "commit_sha": _commit_sha,
            "pr180_enabled": bool(_pr180_enabled),
            "pr180_mode": str(_pr180_mode),
            "pr180_named_client": _is_named,
            "confirmation_required": _confirmation_required,
            "intraday_failed_dir_allowed": _intraday_failed_dir_allowed,
            "risk": {
                "score_floor": float(score_floor),
                "max_capital_pct": float(capital_pct),
                "max_sector_pct": float(sector_pct),
                "max_ticker_pct": float(ticker_pct),
                "max_calls": int(max_calls),
                "max_puts": int(max_puts),
                "daily_max_loss_pct": float(daily_max_loss_pct),
            },
        }
        _hash = hashlib.sha256(
            _json.dumps(enforcement, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()[:16]
        enforcement["runtime_config_hash"] = _hash
        enforcement["status"] = "ok"

        self.live_enforcement = enforcement
        self.runtime_config_hash = _hash

        logger.critical(
            "[%s] LIVE_ENFORCEMENT_OK config_hash=%s commit=%s pod=%s "
            "pr180=%s/%s named=%s confirmation_required=%s "
            "intraday_failed_dir_allowed=%s risk=%s",
            self.email, _hash, _commit_sha, _pod_id,
            _pr180_enabled, _pr180_mode, _is_named,
            _confirmation_required, _intraday_failed_dir_allowed,
            enforcement["risk"],
        )
        return True, "ok"

    def _mark_failed(self, reason: str):
        reason = str(reason or "failed")
        self.failed.set()
        self.degraded.set()
        self.entries_allowed.clear()
        self.degraded_reasons.add(reason)
        self.failure_reason = reason
        _record_runner_failure(self.email, reason)
        logger.error("[%s] Runner failed: %s", self.email, reason)

    def _enter_degraded_mode(self, reason: str, *, stop_runner: bool = False):
        reason = str(reason or "unknown_degraded_reason")
        # PR D / FIX-4: hold _degraded_lock for the degraded_reasons
        # mutation. The Event.set() / .clear() calls are themselves
        # atomic but the set mutation is not.
        with self._degraded_lock:
            self.degraded.set()
            self.entries_allowed.clear()
            self.degraded_reasons.add(reason)
        logger.error("[%s] ENTERING DEGRADED MODE: %s", self.email, reason)

        health_mon = get_monitor()
        if health_mon:
            try:
                if hasattr(health_mon, "raise_dashboard_alert"):
                    health_mon.raise_dashboard_alert(self.email, reason)
            except Exception as _e:
                logger.warning("runner_dashboard_alert_failed: %s", _e)

        healer = get_healer()
        if healer:
            try:
                if hasattr(healer, "request_action"):
                    healer.request_action(self.email, "degraded", reason=reason)
            except Exception as _e:
                logger.warning("runner_healer_request_action_failed: %s", _e)

        if stop_runner:
            self.stopped.set()

    # WIRE-3 (support method) ─────────────────────────────────────────────────
    def _on_split_brain_detected(
        self,
        local_order_id: str = "",
        broker_order_id: str = "",
    ) -> None:
        """
        Callback passed to worker_loop. Called when any submit returns
        {"split_brain": True} — broker accepted but OSM DB transition failed.

        Freezes new entries immediately while keeping the runner alive so the
        fill monitor and reconciler can advance the flagged order(s) to SUBMITTED
        and clear the split-brain state from the DB.

        worker_loop must call this as:
            if res.get("split_brain"):
                on_split_brain(
                    local_order_id=res.get("local_order_id", ""),
                    broker_order_id=res.get("broker_order_id", ""),
                )

        Note: _enter_degraded_mode already clears entries_allowed internally.
        """
        # Include the order ID in the reason key so the log and dashboard show
        # which exact order triggered the freeze. _reason_key() splits on ':'
        # so "split_brain_detected" is still the prefix for clearing logic.
        reason = f"split_brain_detected:{local_order_id or 'unknown'}"

        logger.critical(
            "[%s] SPLIT-BRAIN DETECTED at runtime | local=%s broker=%s — "
            "freezing entries until reconciler recovers",
            self.email,
            local_order_id or "?",
            broker_order_id or "?",
        )

        # Enter degraded mode (also clears entries_allowed internally).
        self._enter_degraded_mode(reason, stop_runner=False)

        # Notify the healer so self-healing infrastructure has full visibility
        # and can take any configured action (alert, escalation, etc.).
        healer = get_healer()
        if healer is not None:
            try:
                healer.request_action(
                    self.email,
                    "split_brain",
                    reason=reason,
                    local_order_id=str(local_order_id or ""),
                    broker_order_id=str(broker_order_id or ""),
                )
            except Exception:
                logger.debug(
                    "[%s] healer request_action failed for split-brain event",
                    self.email,
                    exc_info=True,
                )

    def _check_split_brain_recovery(self) -> bool:
        """
        BUG-5 FIX: poll get_split_brain_orders() and clear split-brain degraded
        reasons when the reconciler has resolved all flagged orders.

        Previously split_brain_residue and split_brain_detected were permanently
        sticky — the runner stayed degraded after a split-brain event even after
        the reconciler had advanced the flagged orders, until the runner was
        manually restarted.

        Called from both the main runner loop (60s cadence) and the health loop
        (RUNNER_HEALTH_CHECK_SEC cadence). Returns True if split-brain reasons
        were cleared.
        """
        has_sb_reason = any(
            self._reason_key(r) in {"split_brain_residue", "split_brain_detected"}
            for r in self.degraded_reasons
        )
        if not has_sb_reason:
            return False

        if self.order_state_machine is None:
            return False

        try:
            remaining = self.order_state_machine.get_split_brain_orders(
                execution_mode=str(self.mode).strip().lower(),
            )
        except Exception as exc:
            logger.warning("[%s] split-brain recovery check failed: %s", self.email, exc)
            return False

        if remaining:
            # Still outstanding — keep degraded.
            logger.debug(
                "[%s] split-brain recovery check: %d order(s) still unresolved",
                self.email, len(remaining),
            )
            return False

        # All split-brain orders resolved by reconciler.
        logger.warning(
            "[%s] SPLIT-BRAIN RECOVERED — all flagged orders resolved by reconciler; "
            "clearing degraded reasons and re-evaluating entry permission",
            self.email,
        )
        self._clear_degraded_reason_key("split_brain_residue")
        self._clear_degraded_reason_key("split_brain_detected")
        return True
    # ─────────────────────────────────────────────────────────────────────────

    def _reason_key(self, reason: str) -> str:
        return str(reason or "").split(":", 1)[0]

    def _clear_degraded_reason_key(self, key: str):
        """
        Remove all degraded reasons matching the given key prefix.

        COHESION-1 FIX: if removing the reason empties the set, proactively
        clear degraded.set() and re-evaluate entry permission. Previously there
        was a window where degraded_reasons was empty but degraded.is_set() was
        still True, until _try_recover_degraded_mode ran on the next health tick.
        """
        key = str(key or "")
        if not key:
            return
        # PR D / FIX-4: hold _degraded_lock for the compound read-then-write.
        # The set comprehension below is two operations (build new set,
        # assign) and races with concurrent _enter_degraded_mode / split-
        # brain callback writes from worker threads. Lock makes it atomic.
        with self._degraded_lock:
            self.degraded_reasons = {
                r for r in self.degraded_reasons
                if self._reason_key(r) != key
            }
            # Capture the post-mutation state inside the lock so the
            # downstream Event.clear() and logging see consistent data.
            _now_empty = not self.degraded_reasons
        # If the set is now empty and we're not in a hard-failed / stopping state,
        # clear the degraded flag immediately rather than waiting for the health loop.
        if _now_empty and not self.failed.is_set() and not self.stopping.is_set():
            if self.degraded.is_set():
                logger.warning("[%s] RECOVERED (reason cleared, no remaining degraded reasons)", self.email)
            self.degraded.clear()
            self._set_entry_permission()

    def _try_recover_degraded_mode(self):
        """
        Clear transient degraded state once the runtime stack is healthy again.

        Critical startup/control-stack failures remain sticky. Runtime worker/fill
        monitor failures are recoverable because those loops are self-restarting.

        split_brain_detected and split_brain_residue are also sticky — they must
        be cleared by the reconciler recovering the flagged orders, not by this
        method.
        """
        if self.failed.is_set() or self.stopping.is_set() or self.stopped.is_set():
            return False

        worker_alive = bool(self.worker_thread and self.worker_thread.is_alive())
        fill_alive = bool(self.fill_monitor_thread and self.fill_monitor_thread.is_alive())
        core_present = self.core is not None
        exit_present = getattr(self.core, "exit_eng", None) is not None if self.core else False

        if not (worker_alive and fill_alive and core_present and exit_present):
            return False

        recoverable = {
            "worker_dead",
            "fill_monitor_dead",
            "worker_loop_crashed",
            "worker_loop_returned",
            "fill_monitor_loop_crashed",
            "fill_monitor_loop_returned",
        }
        # split_brain reasons are NOT in recoverable — they are sticky until the
        # reconciler explicitly clears them by advancing flagged orders.

        # PR D / FIX-4: hold _degraded_lock for the compound read-then-write.
        with self._degraded_lock:
            remaining = {
                r for r in self.degraded_reasons
                if self._reason_key(r) not in recoverable
            }
            self.degraded_reasons = remaining
            _now_empty = not self.degraded_reasons
        if _now_empty:
            if self.degraded.is_set():
                logger.warning("[%s] RECOVERED from transient degraded mode", self.email)
            self.degraded.clear()
            self._set_entry_permission()
            return True

        return False

    def _is_market_hours_now(self) -> bool:
        """True only during NYSE market hours Mon–Fri 9:30–16:05 ET.
        NOTE: deliberately 9:30 not 9:25 — QPM may not have completed its
        first heartbeat by 9:25, causing entries_allowed to stay False through
        the most critical window of the day."""
        try:
            from zoneinfo import ZoneInfo
            from datetime import datetime as _dt, time as _t
            et = _dt.now(ZoneInfo("America/New_York"))
            return _t(9, 30) <= et.time() <= _t(16, 5) and et.weekday() < 5
        except Exception:
            return True   # fail open — don't block entries on tz error

    def _quote_monitor_healthy(self) -> bool:
        """Return True when the per-client quote monitor is alive and heartbeating."""
        qm = getattr(self, "quotemonitor", None) or getattr(self, "quote_monitor", None)
        if qm is None:
            return False
        try:
            return bool(qm.is_healthy())
        except Exception:
            return False

    def _start_position_quote_monitor(self, broker=None, exit_eng=None) -> None:
        """
        Start the per-client APPositionQuoteMonitor once broker + exit engine exist.

        Ownership:
          - ClientRunner owns monitor lifecycle.
          - ExitEngine owns position state and exit decisions.
          - QuoteMonitor hydrates quotes into ExitEngine and wakes the exit loop.
          - app.py only observes health/metrics through runner.quotemonitor/quote_monitor.
        """
        if APPositionQuoteMonitor is None:
            logger.warning("[%s] PositionQuoteMonitor unavailable: import failed", self.email)
            return

        existing = getattr(self, "quotemonitor", None) or getattr(self, "quote_monitor", None)
        if existing is not None:
            try:
                if existing.is_alive():
                    return
            except Exception:
                pass

        core = getattr(self, "core", None)
        broker = (
            broker
            or getattr(self, "data_broker", None)
            or getattr(self, "databroker", None)
            or getattr(self, "broker", None)
            or getattr(core, "data_broker", None)
            or getattr(core, "databroker", None)
            or getattr(core, "broker", None)
        )
        exit_eng = (
            exit_eng
            or getattr(self, "exit_eng", None)
            or getattr(self, "exiteng", None)
            or getattr(self, "exit_engine", None)
            or getattr(core, "exit_eng", None)
            or getattr(core, "exiteng", None)
            or getattr(core, "exit_engine", None)
        )

        if broker is None:
            logger.error("[%s] PositionQuoteMonitor not started: broker missing", self.email)
            return
        if exit_eng is None:
            logger.error("[%s] PositionQuoteMonitor not started: exit engine missing", self.email)
            return

        def _qpm_alert(msg: str) -> None:
            try:
                logger.warning("[%s] %s", self.email, msg)
                health_mon = get_monitor()
                if health_mon and hasattr(health_mon, "raise_dashboard_alert"):
                    health_mon.raise_dashboard_alert(self.email, msg)
            except Exception as _e:
                logger.warning("runner_dashboard_alert_failed_2: %s", _e)

        qm = APPositionQuoteMonitor(
            broker=broker,
            client_id=self.email,
            exit_engine=exit_eng,
            alert_fn=_qpm_alert,
        )

        attach = (
            getattr(exit_eng, "attach_quote_monitor", None)
            or getattr(exit_eng, "attachquotemonitor", None)
        )
        if callable(attach):
            attach(qm)
        else:
            try:
                setattr(exit_eng, "quote_monitor", qm)
                setattr(exit_eng, "quotemonitor", qm)
            except Exception as _e:
                logger.warning("runner_exit_eng_quote_monitor_setattr_failed: %s", _e)

        self.quote_monitor = qm
        self.quotemonitor = qm
        try:
            setattr(exit_eng, "quote_monitor", qm)
            setattr(exit_eng, "quotemonitor", qm)
        except Exception as _e:
            logger.warning("runner_exit_eng_quote_monitor_setattr_failed_2: %s", _e)

        qm.start()
        logger.info("[%s] PositionQuoteMonitor started and attached to exit engine", self.email)

        # ── Start ExitReliabilityMonitor alongside QPM ────────────────────────
        # Checks every 60s for: GREEN_NO_DECISION, DECISION_NO_ORDER, STALE_EXIT_ORDER
        try:
            from ap.exit_reliability_monitor import ExitReliabilityMonitor
            erm = ExitReliabilityMonitor(
                client_id=self.email,
                exit_engine=exit_eng,
                supabase_client=getattr(self, "supabase", None),
            )
            erm.start()
            self.exit_reliability_monitor = erm
            logger.info("[%s] ExitReliabilityMonitor started", self.email)
        except Exception as _erm_err:
            logger.warning("[%s] ExitReliabilityMonitor failed to start (non-fatal): %s", self.email, _erm_err)

    def _set_entry_permission(self):
        _worker_ok = self.worker_thread is not None and self.worker_thread.is_alive()
        _fill_ok   = self.fill_monitor_thread is not None and self.fill_monitor_thread.is_alive()
        _core_ok   = self.core is not None and getattr(self.core, "exit_eng", None) is not None
        _failed    = self.failed.is_set()

        _require_qpm = os.getenv("REQUIRE_QUOTE_MONITOR_FOR_ENTRIES", "1").strip().lower() in {"1", "true", "yes", "on"}

        # QPM is a real-time trading monitor — only required during market hours.
        # Off-hours (nights, weekends): QPM cycles slowly and monitors no positions.
        # Requiring it off-hours creates false degraded state that persists into open.
        _market_open = self._is_market_hours_now()
        _quote_ok = (self._quote_monitor_healthy() if (_require_qpm and _market_open) else True)

        if _require_qpm and _market_open and self.initialized.is_set() and not self.stopping.is_set() and not _failed:
            _was_degraded_qpm = "quote_monitor_unhealthy" in getattr(self, "degraded_reasons", set())
            if _quote_ok:
                if _was_degraded_qpm:
                    self.degraded_reasons.discard("quote_monitor_unhealthy")
                    if not self.degraded_reasons:
                        self.degraded.clear()
                    # QPM just recovered during market hours — fire entries_allowed
                    # immediately rather than waiting for the next 20s health loop tick.
                    logger.info("[%s] QPM recovered — entries_allowed unlocked immediately", self.email)
            else:
                self.degraded.set()
                self.degraded_reasons.add("quote_monitor_unhealthy")
        elif not _market_open:
            # Off-hours: clear any stale QPM degraded reason so it doesn't block
            # market-open entries when QPM hasn't had a chance to recover yet.
            if "quote_monitor_unhealthy" in getattr(self, "degraded_reasons", set()):
                self.degraded_reasons.discard("quote_monitor_unhealthy")
                if not self.degraded_reasons:
                    self.degraded.clear()

        _degraded  = self.degraded.is_set()
        # PR D / FIX-5: Fill-monitor grace period is now a module-level
        # constant FILL_MONITOR_GRACE_SEC (default 300s). Resolved once
        # at import — avoids os.getenv on every health tick (~3x/min).
        _fill_dead_since = getattr(self, "_fill_dead_since", None)
        if not _fill_ok:
            if _fill_dead_since is None:
                self._fill_dead_since = time.time()
            _fill_ok_for_entries = (time.time() - self._fill_dead_since) < FILL_MONITOR_GRACE_SEC
        else:
            self._fill_dead_since = None
            _fill_ok_for_entries = True
        ready = (
            self.is_alive()
            and self.initialized.is_set()
            and not _failed
            and not self.stopping.is_set()
            and not _degraded
            and _core_ok
            and _worker_ok
            and _fill_ok_for_entries
            and _quote_ok
        )
        if ready:
            self.entries_allowed.set()
        else:
            if not self.is_alive():
                logger.warning("[%s] entries_allowed BLOCKED: runner thread not alive", self.email)
            if not self.initialized.is_set():
                logger.warning("[%s] entries_allowed BLOCKED: not initialized", self.email)
            if not _core_ok:
                logger.warning("[%s] entries_allowed BLOCKED: core/exit_eng missing", self.email)
            if not _worker_ok:
                logger.warning("[%s] entries_allowed BLOCKED: worker_thread dead", self.email)
            if not _fill_ok_for_entries:
                # PR D / FIX-5: log the live grace value instead of the stale
                # hard-coded ">120s" literal. Default is now 300s.
                logger.warning(
                    "[%s] entries_allowed BLOCKED: fill_monitor dead >%ss (actual=%s)",
                    self.email, FILL_MONITOR_GRACE_SEC, _fill_ok,
                )
            if _require_qpm and not _quote_ok:
                logger.warning("[%s] entries_allowed BLOCKED: quote_monitor unhealthy/missing", self.email)
            if _degraded:
                logger.warning("[%s] entries_allowed BLOCKED: degraded reasons=%s", self.email, sorted(getattr(self, "degraded_reasons", set())))
            if _failed:
                logger.warning("[%s] entries_allowed BLOCKED: failed", self.email)
            if self.stopping.is_set():
                logger.warning("[%s] entries_allowed BLOCKED: stopping", self.email)
            self.entries_allowed.clear()
        return ready

    def _build_startup_manifest(
        self,
        *,
        equity: float,
        max_trades: int,
        max_pos: int,
        max_loss: float,
        throttle_threshold: float,
        stop_threshold: float,
        data_broker_is_dedicated: bool,
        exit_eng,
        # Risk profile effective values — passed from _start() locals
        mc_score_floor: float = 65.0,
        mc_ctx_floor: float   = 0.0,
        mc_capital_pct: float = 0.40,
        mc_sector_pct: float  = 0.25,
        mc_ticker_pct: float  = 0.10,
        risk_profile_source: str  = "GLOBAL_ENV_DEFAULT",
        risk_profile_valid: bool  = False,
        missing_risk_fields: list = None,
        daily_max_loss_pct: float = 0.06,
    ):
        if missing_risk_fields is None:
            missing_risk_fields = []
        try:
            self.startup_manifest = {
            "client_id": self.email,
            "account_id": self.account_id,
            "mode": self.mode,
            "live_preflight_status": getattr(self, "live_preflight_status", "not_applicable"),
            "live_enforcement": getattr(self, "live_enforcement", {"status": "not_applicable"}),
            "runtime_config_hash": getattr(self, "runtime_config_hash", None),
            "live_preflight_quote_source": getattr(self, "live_preflight_quote_source", ""),
            "live_preflight_quote_broker_mode": getattr(self, "live_preflight_quote_broker_mode", ""),
            "live_preflight_quote_base_url": getattr(self, "live_preflight_quote_base_url", ""),
            "base_url": self.base_url,
            "core_present": self.core is not None,
            "exit_engine_present": exit_eng is not None,
            "entry_watcher_present": getattr(self.core, "entry_watcher", None) is not None if self.core else False,
            "position_manager_present": self.position_manager is not None,
            "order_state_machine_present": self.order_state_machine is not None,
            "contract_selector_present": self.contract_selector is not None,
            "order_monitor_present": self.order_monitor is not None,
            "quote_monitor_present": self.quotemonitor is not None or self.quote_monitor is not None,
            "quote_monitor_alive": bool((self.quotemonitor or self.quote_monitor) and (self.quotemonitor or self.quote_monitor).is_alive()),
            "fill_monitor_alive": bool(self.fill_monitor_thread and self.fill_monitor_thread.is_alive()),
            "worker_alive": bool(self.worker_thread and self.worker_thread.is_alive()),
            "equity_alive": bool(self.equity_thread and self.equity_thread.is_alive()),
            "reconciler_enabled": bool(self.reconciler is not None),
            "data_broker_mode": "dedicated" if data_broker_is_dedicated else "shared_execution_broker",
            "equity": float(equity),
            "max_trades": int(max_trades),
            "max_positions": int(max_pos),
            "max_daily_loss": float(max_loss),
            "throttle_threshold": float(throttle_threshold),
            "stop_threshold": float(stop_threshold),
            "score_floor": mc_score_floor,
            "context_floor": mc_ctx_floor,
            "max_capital_pct": mc_capital_pct,
            "max_sector_pct": mc_sector_pct,
            "max_ticker_pct": mc_ticker_pct,
            "risk_profile_source": risk_profile_source,
            "risk_profile_valid": risk_profile_valid,
            "missing_risk_fields": missing_risk_fields,
            "effective_score_floor": mc_score_floor,
            "effective_context_floor": mc_ctx_floor,
            "effective_max_capital_pct": mc_capital_pct,
            "effective_max_sector_pct": mc_sector_pct,
            "effective_daily_max_loss_pct": daily_max_loss_pct,
        }
            logger.info("[%s] Startup manifest: %s", self.email, self.startup_manifest)
        except Exception as _manifest_exc:
            logger.error(
                "[%s] Startup manifest build failed (%s) — continuing with empty manifest. "
                "This is non-fatal.", self.email, _manifest_exc,
            )
            if not self.startup_manifest:
                self.startup_manifest = {
                    "client_id": self.email,
                    "mode":      getattr(self, "mode", "unknown"),
                    "live_enforcement": getattr(self, "live_enforcement", {"status": "not_applicable"}),
                    "runtime_config_hash": getattr(self, "runtime_config_hash", None),
                    "manifest_error": str(_manifest_exc),
                }

    def _validate_control_stack(self):
        problems = []

        if self.core is None:
            problems.append("core_missing")
        if self.master_control is None:
            problems.append("master_control_missing")
        if self.position_manager is None:
            problems.append("position_manager_missing")
        if self.order_state_machine is None:
            problems.append("order_state_machine_missing")
        if self.contract_selector is None:
            problems.append("contract_selector_missing")
        if self.order_monitor is None:
            problems.append("order_monitor_missing")

        exit_eng = getattr(self.core, "exit_eng", None) if self.core else None
        entry_watcher = getattr(self.core, "entry_watcher", None) if self.core else None

        if exit_eng is None:
            problems.append("exit_engine_missing")
        if entry_watcher is None:
            problems.append("entry_watcher_missing")
        if self.fill_monitor_thread is None or not self.fill_monitor_thread.is_alive():
            problems.append("fill_monitor_not_alive")
        if self.worker_thread is None or not self.worker_thread.is_alive():
            problems.append("worker_thread_not_alive")

        if problems:
            reason = "control_stack_validation_failed:" + ",".join(problems)
            self._mark_failed(reason)
            self.stopped.set()
            raise RuntimeError(f"[{self.email}] {reason}")

        logger.info("[%s] Control stack validation PASSED", self.email)
        return True

    def _assert_worker_alive(self):
        time.sleep(float(os.getenv("WORKER_START_GRACE_SEC", "0.5")))
        if not self.worker_thread or not self.worker_thread.is_alive():
            self._mark_failed("worker_thread_failed_to_start")
            self.stopped.set()
            raise RuntimeError(f"[{self.email}] worker_thread failed to start")

    def _get_broker(self):
        """Broker lives inside self.core — resolve it safely."""
        return getattr(self.core, "broker", None) if self.core else None

    def _run_overnight_reeval_if_due(self) -> None:
        """Fire overnight signal re-evaluation at 9:00-9:45 AM ET on trading days.
        Runs once per calendar day. Processes WATCHING signals, fetches prior-day
        levels, validates directional structure, selects contracts, arms watcher.
        """
        try:
            from zoneinfo import ZoneInfo
            import datetime as _dt
            now_et = _dt.datetime.now(ZoneInfo("America/New_York"))
            today = now_et.date()

            # Only Mon-Fri, 9:00-9:25 AM ET — must arm watcher before 9:30 open
            if now_et.weekday() >= 5:
                return
            if not (now_et.hour == 9 and 0 <= now_et.minute <= 45):
                return

            # Only once per day
            last_ran = getattr(self, "_last_overnight_reeval_date", None)
            if last_ran == today:
                return

            self._last_overnight_reeval_date = today
            logger.info("[%s] 🌅 Overnight daily signal reeval — %02d:%02d ET | checking WATCHING queue",
                        self.email, now_et.hour, now_et.minute)

            broker = self._get_broker()
            data_broker = getattr(self, "data_broker", None) or broker
            # All of these are stored directly on the runner (not on self.core)
            entry_watcher = getattr(self.core, "entry_watcher", None) if self.core else None
            contract_selector = self.contract_selector
            exit_eng = getattr(self.core, "exit_eng", None) if self.core else None

            from ap_overnight_reeval import run_overnight_reeval
            result = run_overnight_reeval(
                client_id=self.email,
                broker=broker,
                data_broker=data_broker,
                master_control=self.master_control,
                contract_selector=contract_selector,
                order_state_machine=self.order_state_machine,
                entry_watcher=entry_watcher,
                position_manager=self.position_manager,
                exit_eng=exit_eng,
                force=False,
            )
            logger.info(
                "[%s] Overnight reeval complete: armed=%d rejected=%d processed=%d errors=%d",
                self.email, result["armed"], result["rejected"],
                result["processed"], result["errors"],
            )
            # P0 (2026-07-02): durable, HONEST record of the reeval itself.
            # The only handoff_run_locks row previously written for this
            # window was stage='post_overnight_reeval' status='success' —
            # emitted by the handoff audit regardless of whether the reeval
            # actually drained anything. Paper ran 4 sessions with 486 rows
            # frozen behind a green lock. This dedicated stage row carries
            # the run counts and reports 'partial' when the run stalled
            # (fetched work, produced zero decisions). Best-effort: a lock
            # write failure must never take down the reeval path.
            try:
                from ap.morning_handoff import _upsert_handoff_run_lock
                _stalled = bool(result.get("stalled"))
                _upsert_handoff_run_lock(
                    client_id=self.email,
                    execution_mode=str(self.mode).lower(),
                    trading_date=datetime.now(_ET).date().isoformat(),
                    stage="overnight_reeval",
                    status="partial" if _stalled else "success",
                    last_error=(
                        "OVERNIGHT_REEVAL_STALLED:all_fetched_rows_deferred"
                        if _stalled else None
                    ),
                    details={
                        k: result.get(k)
                        for k in (
                            "fetched", "processed", "armed", "rejected",
                            "skipped", "errors", "stale_skipped",
                            "fresh_processed", "fresh_armed", "stalled",
                        )
                    },
                    mark_success=not _stalled,
                )
            except Exception as _lock_exc:
                logger.warning(
                    "[%s] overnight_reeval lock write failed (non-fatal): %s",
                    self.email, _lock_exc,
                )
            self._run_post_overnight_morning_handoff(result)
        except Exception as exc:
            logger.error("[%s] Overnight reeval error (non-fatal): %s", self.email, exc, exc_info=True)

    def _run_startup_morning_handoff(self) -> None:
        try:
            from ap.morning_handoff import run_morning_handoff_audit
            from ap.preopen_readiness import run_preopen_autonomous_readiness

            result = run_morning_handoff_audit(
                client_id=self.email,
                execution_mode=self.mode,
                stage="startup",
                dry_run=False,
                runner=self,
            )
            logger.info("[%s] Startup morning handoff result: %s", self.email, result)
            readiness = run_preopen_autonomous_readiness(
                self.email,
                self.mode,
                dry_run=False,
                stage="startup",
                runner=self,
            )
            logger.info("[%s] Startup preopen readiness result: %s", self.email, readiness)
            if str(self.mode).lower() == "live":
                if readiness.get("status") == "BLOCKED":
                    self._enter_degraded_mode(
                        "preopen_readiness_blocked:" + ",".join(readiness.get("errors") or ["unknown"])
                    )
                elif readiness.get("ok"):
                    self._clear_degraded_reason_key("preopen_readiness_blocked")
        except Exception as exc:
            logger.error("[%s] Startup morning handoff failed (non-fatal): %s", self.email, exc, exc_info=True)

    def _run_post_overnight_morning_handoff(self, overnight_result: dict | None) -> None:
        if not isinstance(overnight_result, dict):
            return
        try:
            from ap.morning_handoff import run_morning_handoff_audit
            from ap.preopen_readiness import run_preopen_autonomous_readiness

            result = run_morning_handoff_audit(
                client_id=self.email,
                execution_mode=self.mode,
                stage="post_overnight_reeval",
                dry_run=False,
                runner=self,
            )
            logger.info("[%s] Post-overnight morning handoff result: %s", self.email, result)
            readiness = run_preopen_autonomous_readiness(
                self.email,
                self.mode,
                dry_run=False,
                stage="post_overnight_reeval",
                runner=self,
            )
            logger.info("[%s] Post-overnight preopen readiness result: %s", self.email, readiness)
            if str(self.mode).lower() == "live":
                if readiness.get("status") == "BLOCKED":
                    self._enter_degraded_mode(
                        "preopen_readiness_blocked:" + ",".join(readiness.get("errors") or ["unknown"])
                    )
                elif readiness.get("ok"):
                    self._clear_degraded_reason_key("preopen_readiness_blocked")
        except Exception as exc:
            logger.error("[%s] Post-overnight morning handoff failed (non-fatal): %s", self.email, exc, exc_info=True)

    def _run_exit_autonomous_recovery(self):
        """
        Run exit engine recovery every ~60s to resolve CLOSING-forever positions.
        Calls recover_exit_engine which scans tracked positions for stale in-flight
        exits and resolves them via broker API — recovering from dropped fill callbacks,
        split-brain exit orders, and Render restart gaps.
        """
        # Rate-limit to every 60s (health loop runs every 20s → run every 3rd tick)
        _now = time.time()
        _last = getattr(self, "_last_exit_recovery_ts", 0.0)
        if _now - _last < 60.0:
            return
        self._last_exit_recovery_ts = _now

        try:
            from ap.exit_autonomous_recovery import recover_exit_engine
            exit_eng = getattr(self.core, "exit_eng", None) if self.core else None
            broker = getattr(self, "broker", None)
            if exit_eng is None or broker is None:
                return
            actions = recover_exit_engine(exit_eng, broker=broker)
            for action in (actions or []):
                if action.action not in ("NOOP",):
                    logger.info(
                        "[%s] exit_autonomous_recovery: %s | %s | pos=%s",
                        self.email, action.action, action.reason, action.position_id,
                    )
        except Exception as _exc:
            logger.debug("[%s] exit_autonomous_recovery error (non-fatal): %s", self.email, _exc)

    def _run_deferred_breach_lifecycle_recovery(self):
        """Continuously recover due/stale deferred-breach ownership rows."""
        _now = time.time()
        _last = getattr(self, "_last_deferred_breach_recovery_ts", 0.0)
        if _now - _last < 20.0:
            return
        self._last_deferred_breach_recovery_ts = _now
        broker = getattr(self, "broker", None)
        core = getattr(self, "core", None)
        watcher = getattr(core, "entry_watcher", None) if core is not None else None
        if broker is None or core is None or self.order_state_machine is None:
            return
        try:
            recovery = APStartupRecovery(
                client_id=self.email,
                broker=broker,
                osm=self.order_state_machine,
                pm=self.position_manager,
                master_control=self.master_control,
                exit_engine=getattr(core, "exit_eng", None),
                entry_watcher=watcher,
                execution_core=core,  # §2: required for safe BROKER_READY recovery
            )
            outcome = recovery.recover_deferred_lifecycles()
            if outcome.get("errors"):
                logger.error(
                    "[%s] deferred breach lifecycle recovery errors=%s",
                    self.email, outcome.get("errors"),
                )
            elif outcome.get("deferred_lifecycles_recovered"):
                logger.info(
                    "[%s] deferred breach lifecycles recovered=%s",
                    self.email, outcome.get("deferred_lifecycles_recovered"),
                )
        except Exception as exc:
            logger.error(
                "[%s] deferred breach lifecycle recovery failed: %s",
                self.email, exc, exc_info=True,
            )

    def _detect_manual_closes(self):
        """
        Detect positions that were manually closed at the broker but still show OPEN in DB.
        Runs every 120s — checks broker positions list against DB open positions.
        When a mismatch is found, marks the DB position as CLOSED with close_source='manual_client_close'.
        This handles the case where a client manually closes a trade on the Tradier dashboard.
        """
        _now = time.time()
        _last = getattr(self, "_last_manual_close_check_ts", 0.0)
        if _now - _last < 120.0:
            return
        self._last_manual_close_check_ts = _now

        try:
            broker = getattr(self, "broker", None)
            if not broker or not hasattr(broker, "list_positions"):
                return

            # Get live broker positions
            broker_positions = broker.list_positions() or []
            broker_contracts = {
                str(p.get("symbol") or "").upper()
                for p in broker_positions
                if int(p.get("quantity") or 0) != 0
            }

            # Get DB open positions
            from ap.db import conn, run_with_retry
            def _get_open():
                with conn() as c:
                    c.execute(
                        "SELECT id, contract, underlying, avg_fill, qty "
                        "FROM positions WHERE client_id=%s AND status='OPEN'",
                        (self.email,)
                    )
                    return c.fetchall()

            open_positions = run_with_retry(_get_open) or []

            for pos in open_positions:
                pos_id = pos.get("id")
                contract = str(pos.get("contract") or "").upper()
                if not contract or not pos_id:
                    continue

                # If this contract is no longer at the broker, it was manually closed
                if contract not in broker_contracts:
                    logger.warning(
                        "[%s] Manual close detected: %s not in broker positions — marking CLOSED",
                        self.email, contract
                    )
                    try:
                        from datetime import datetime, timezone
                        now_iso = datetime.now(timezone.utc).isoformat()
                        def _close(pid=pos_id, ts=now_iso):
                            with conn() as c:
                                c.execute(
                                    """UPDATE positions
                                       SET status='CLOSED',
                                           exit_ts=%s,
                                           exit_reason='MANUAL_CLIENT_CLOSE',
                                           close_source='manual_client_close',
                                           updated_at=%s
                                       WHERE id=%s AND status='OPEN'""",
                                    (ts, ts, pid)
                                )
                        run_with_retry(_close)

                        # Also notify exit engine to remove this position
                        core = getattr(self, "core", None)
                        exit_eng = getattr(core, "exit_eng", None) if core else None
                        if exit_eng and hasattr(exit_eng, "mark_position_closed"):
                            try:
                                exit_eng.mark_position_closed(pos_id)
                            except Exception as _e:
                                logger.warning("runner_exit_eng_mark_closed_failed: %s", _e)

                        logger.info(
                            "[%s] Position %s marked CLOSED (manual client close) | contract=%s",
                            self.email, pos_id, contract
                        )
                    except Exception as close_err:
                        logger.error(
                            "[%s] Failed to mark manual close for pos=%s: %s",
                            self.email, pos_id, close_err
                        )
        except Exception as exc:
            logger.debug("[%s] _detect_manual_closes error (non-fatal): %s", self.email, exc)

    def _start_runtime_health_loop(self):
        interval = float(os.getenv("RUNNER_HEALTH_CHECK_SEC", "20"))

        def _health_loop():
            healer_ref = get_healer()
            while not self.stopped.wait(interval):
                now = time.time()
                self.last_health_check_ts = now

                worker_alive = bool(self.worker_thread and self.worker_thread.is_alive())
                fill_alive = bool(self.fill_monitor_thread and self.fill_monitor_thread.is_alive())
                equity_alive = bool(self.equity_thread and self.equity_thread.is_alive())
                core_present = self.core is not None
                exit_present = getattr(self.core, "exit_eng", None) is not None if self.core else False

                if worker_alive:
                    self.last_worker_heartbeat_ts = now
                if fill_alive:
                    self.last_fill_monitor_heartbeat_ts = now
                if equity_alive:
                    self.last_equity_heartbeat_ts = now

                if not core_present:
                    self._enter_degraded_mode("core_missing_runtime", stop_runner=True)
                    break
                if not exit_present:
                    self._enter_degraded_mode("exit_engine_missing_runtime", stop_runner=True)
                    break
                # Grace period: fill_monitor self-restarts after crashes.
                # Only degrade after 2 consecutive missed checks (~40s) to
                # avoid false positives during brief restart windows.
                _fm_grace = float(os.getenv("FILL_MONITOR_DEAD_GRACE_SEC", "45"))
                _fm_last = self.last_fill_monitor_heartbeat_ts or now
                if not fill_alive and (now - _fm_last) > _fm_grace:
                    self._enter_degraded_mode("fill_monitor_dead", stop_runner=False)
                elif fill_alive:
                    self._clear_degraded_reason_key("fill_monitor_dead")

                _wk_grace = float(os.getenv("WORKER_DEAD_GRACE_SEC", "45"))
                _wk_last = self.last_worker_heartbeat_ts or now
                if not worker_alive and (now - _wk_last) > _wk_grace:
                    self._enter_degraded_mode("worker_dead", stop_runner=False)
                elif worker_alive:
                    self._clear_degraded_reason_key("worker_dead")

                self._try_recover_degraded_mode()
                self._check_split_brain_recovery()   # BUG-5 FIX: poll for reconciler resolution
                self._run_overnight_reeval_if_due()  # Arm WATCHING signals at 9:00-9:45 AM ET
                self._run_exit_autonomous_recovery() # Resolve CLOSING-forever positions every 60s
                self._run_deferred_breach_lifecycle_recovery()
                self._detect_manual_closes()         # Detect broker-closed positions not in DB
                self._set_entry_permission()

                try:
                    if healer_ref:
                        healer_ref.heartbeat(self.email, "runner_health")
                except Exception as _e:
                    logger.debug("runner_healer_heartbeat_failed: %s", _e)

        self.health_thread = threading.Thread(
            target=_health_loop,
            daemon=True,
            name=f"runner-health-{self.email}",
        )
        self.health_thread.start()
        logger.info("[%s] Runtime health loop started", self.email)

    def _validate_execution_core_started(self):
        """
        Fail fast immediately after APExecutionCore.start().

        This catches the exact class of bug where the core object exists but
        watcher/exit/tracker did not actually initialize or start correctly.
        """
        problems = []

        if self.core is None:
            problems.append("core_missing")
            self._mark_failed("execution_core_start_validation_failed:" + ",".join(problems))
            self.stopped.set()
            raise RuntimeError(f"[{self.email}] execution core validation failed: {problems}")

        entry_watcher = getattr(self.core, "entry_watcher", None)
        exit_eng = getattr(self.core, "exit_eng", None)
        tracker = getattr(self.core, "tracker", None)

        if entry_watcher is None:
            problems.append("entry_watcher_missing")
        if exit_eng is None:
            problems.append("exit_engine_missing")
        if tracker is None:
            problems.append("tracker_missing")

        # Thread liveness checks are intentionally conditional because exact
        # attribute names can vary across versions. If a component exposes a
        # thread handle and it is already dead immediately after start(), fail.
        thread_checks = [
            ("entry_watcher_thread_dead", getattr(entry_watcher, "_thread", None) if entry_watcher else None),
            ("exit_engine_thread_dead", getattr(exit_eng, "_thread", None) if exit_eng else None),
            ("tracker_thread_dead", getattr(tracker, "_thread", None) if tracker else None),
        ]
        for reason, thread in thread_checks:
            if thread is not None and not thread.is_alive():
                problems.append(reason)

        # Some watcher/tracker implementations use alternate names.
        alt_thread_checks = [
            ("entry_watcher_loop_dead", getattr(entry_watcher, "thread", None) if entry_watcher else None),
            ("tracker_loop_dead", getattr(tracker, "thread", None) if tracker else None),
        ]
        for reason, thread in alt_thread_checks:
            if thread is not None and not thread.is_alive():
                problems.append(reason)

        if problems:
            reason = "execution_core_start_validation_failed:" + ",".join(problems)
            self._mark_failed(reason)
            self.stopped.set()
            raise RuntimeError(f"[{self.email}] {reason}")

        logger.info("[%s] Execution core startup validation PASSED", self.email)
        return True

    def run(self):
        try:
            self._run_inner()
        except Exception as exc:
            self._mark_failed(str(exc))
            logger.error("[%s] Runner crashed", self.email, exc_info=True)
        finally:
            self._cleanup()
            with _registry_lock:
                current = _active_runners.get(self.email)
                if current is self:
                    _active_runners.pop(self.email, None)
            logger.info("[%s] ClientRunner stopped.", self.email)

    def _run_inner(self):
        logger.info("[%s] ClientRunner starting -- account %s @ %s", self.email, self.account_id, self.base_url)

        # MODE DETECTION FIRST — must happen before _get_token() so decrypt_token()
        # receives the correct mode and applies LIVE fail-closed policy.
        # URL is the authoritative source of mode truth — never trust AP_MODE env
        # when a production URL is present.
        if "sandbox" in self.base_url.lower():
            self.mode = "PAPER"
        elif "api.tradier.com" in self.base_url.lower():
            self.mode = "LIVE"
            logger.warning(
                "[%s] Forcing LIVE mode from base_url=%s — AP_MODE env is ignored when URL is production",
                self.email, self.base_url,
            )
        else:
            self.mode = os.getenv("AP_MODE", "PAPER").upper()
        logger.info("[%s] Client mode: %s (base_url=%s)", self.email, self.mode, self.base_url)

        token = self._get_token()   # decrypt_token() now receives the correct mode
        if not token:
            self._mark_failed("no_token")
            return

        if self.mode == "LIVE":
            missing = []
            # Note: token is guaranteed non-None here — checked and early-returned above.
            if not str(self.account_id or "").strip():
                missing.append("member.tradier_account_id")
            if not os.getenv("DATABASE_URL", "").strip():
                missing.append("DATABASE_URL")
            if missing:
                self._mark_failed("LIVE startup missing: " + ", ".join(missing))
                return
            # LIVE safety config — hard block unsafe settings before any thread starts
            try:
                from ap.fill_monitor import ALLOW_LEGACY_FILL_MONITOR
                if ALLOW_LEGACY_FILL_MONITOR:
                    self._mark_failed("LIVE_STARTUP_FATAL: ALLOW_LEGACY_FILL_MONITOR=1 is forbidden in LIVE")
                    return
            except ImportError:
                pass
            _allow_imm = os.getenv("ALLOW_IMMEDIATE_EXECUTION", "0").strip().lower()
            if _allow_imm in ("1", "true", "yes", "on"):
                self._mark_failed("LIVE_STARTUP_FATAL: ALLOW_IMMEDIATE_EXECUTION=1 is forbidden in LIVE")
                return
            try:
                from ap.order_state_machine import _ROWCOUNT_NONE_IS_FATAL
                if not _ROWCOUNT_NONE_IS_FATAL:
                    self._mark_failed("LIVE_STARTUP_FATAL: OSM_ROWCOUNT_NONE_IS_FATAL=0 in LIVE — unsafe")
                    return
            except ImportError:
                pass
            # PR D / SECURITY CRITICAL (BUG-CR-1): block LIVE startup if
            # ENCRYPTION_KEY is the documented default sentinel. The previous
            # default was a human-readable string committed to source control;
            # any operator who relied on it had broker tokens encrypted with a
            # key visible in the public repo. Any LIVE deploy MUST set a
            # unique ENCRYPTION_KEY in Render environment variables.
            if _raw_key == _DEFAULT_KEY_SENTINEL:
                self._mark_failed(
                    "LIVE_STARTUP_FATAL: ENCRYPTION_KEY is the default insecure sentinel. "
                    "Set a unique ENCRYPTION_KEY in Render env vars before going live."
                )
                return
            logger.critical(
                "[%s] LIVE SAFETY CONFIG VERIFIED: legacy_fill_monitor=OFF immediate_execution=OFF rowcount_fatal=ON encryption_key=CUSTOM",
                self.email,
            )
            logger.info("[%s] LIVE mode assertions PASSED | account_id=%s base_url=%s", self.email, self.account_id, self.base_url)

        try:
            from ap.brokers.tradier import TradierBroker, TradierConfig
            from ap_execution_core import APExecutionCore
            from ap_master_control import APMasterControl
            from ap.position_manager import APPositionManager
            from ap.order_state_machine import APOrderStateMachine
            from ap.contract_selector import APContractSelectionEngine
        except Exception as exc:
            self._mark_failed(f"import_failed:{exc}")
            return

        try:
            from ap.db import ensure_client_exists
            ensure_client_exists(self.email, equity=float(os.getenv("ACCOUNT_EQUITY", "25000")))
            logger.info("[%s] DB rows provisioned", self.email)
        except Exception as exc:
            self._mark_failed(f"ensure_client_exists_failed:{exc}")
            return

        broker_cfg = TradierConfig(base_url=self.base_url, access_token=token, account_id=self.account_id)
        broker = TradierBroker(broker_cfg)
        self.broker = broker

        # P0 (PR #261): LIVE preflight — credentials and market data must
        # actually WORK before any thread, gate, or capital calibration
        # runs. Paper runners skip this entirely (they fail-close on their
        # own market-data token requirement at transport split).
        if str(self.mode).strip().upper() == "LIVE":
            _pf_ok, _pf_reason = self._run_live_preflight(broker)
            if not _pf_ok:
                self._mark_failed(f"LIVE_PREFLIGHT_FAILED:{_pf_reason}")
                return

        self._clear_old_phantom_orders()
        # BUG-3 FIX: guard both URL and key — an empty service key produces a
        # confusing auth error inside Supabase rather than a clear None here.
        sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY) if (SUPABASE_URL and SUPABASE_SERVICE_KEY) else None
        self.supabase = sb  # Requirement 2: stored so reconciler and other subsystems share one client

        self.position_manager = APPositionManager(client_id=self.email)

        client_cfg = self._load_client_config()
        # Merge client_risk_profiles into client_cfg with highest priority.
        # Risk profile fields override anything from the clients table.
        _rp = getattr(self, "_risk_profile", {}) or {}
        _rp_field_map = {
            "max_capital_pct":        "max_capital_pct",
            "max_sector_pct":         "max_sector_pct",
            "max_ticker_pct":         "max_ticker_pct",
            "max_calls":              "max_calls",
            "max_puts":               "max_puts",
            "score_floor":            "score_floor",
            "context_floor":          "context_floor",
            "max_positions":          "max_concurrent_positions",  # map to clients table key
            "daily_max_loss_pct":     "daily_max_loss_pct",
            "entries_enabled":        "entries_enabled",
            "daily_profit_target_usd": "daily_profit_target_usd",
            # PR #155 — split-cap fields. These map directly (same key in both
            # client_risk_profiles and client_cfg). NULL in the risk profile
            # means "use the backward-compat fallback in APMasterControl".
            "max_position_pct":       "max_position_pct",
            "max_total_capital_pct":  "max_total_capital_pct",
        }
        _risk_profile_source = "GLOBAL_ENV_DEFAULT"
        for _rp_key, _cfg_key in _rp_field_map.items():
            if _rp.get(_rp_key) is not None:
                client_cfg[_cfg_key] = _rp[_rp_key]
                _risk_profile_source = "CLIENT_RISK_PROFILE"
        # Validate: in LIVE mode, every required field must come from risk profile
        _is_live_runner = str(self.mode).lower() == "live"
        _REQUIRED = [
            "max_capital_pct", "max_sector_pct", "max_ticker_pct",
            "max_calls", "max_puts", "score_floor", "context_floor",
            "max_concurrent_positions", "daily_max_loss_pct",
            "entries_enabled",   # PR84 amendment: must match PR83 boot list exactly
        ]
        _missing_live = [
            f for f in _REQUIRED
            if _rp.get(
                next((k for k, v in _rp_field_map.items() if v == f), f)
            ) is None
        ] if _is_live_runner else []
        # LIVE fail-closed: if any required risk field is missing, do not
        # allow the runner to start with global defaults.
        if _is_live_runner and _missing_live:
            raise RuntimeError(
                f"[{self.email}] LIVE RISK PROFILE INCOMPLETE — missing required "
                f"fields {_missing_live}. Runner will not start with global defaults. "
                f"Set all required fields in client_risk_profiles."
            )
        # Equity truth: fetch from Tradier FIRST before creating master_control.
        # This ensures capital gates are always calibrated to the client's actual
        # account balance — not a hardcoded default. $25K default is last resort only.
        _default_equity = float(client_cfg.get("initial_equity", os.getenv("ACCOUNT_EQUITY", "25000")) or 25000)
        try:
            if hasattr(broker, "get_account_equity"):
                _live_eq = broker.get_account_equity()
                if _live_eq and float(_live_eq) > 0:
                    equity = float(_live_eq)
                    logger.info("[%s] Startup equity from Tradier: $%.2f", self.email, equity)
                else:
                    # P0 (PR #261): in LIVE, zero equity is a fatal
                    # misconfiguration — capital gates must never calibrate
                    # to a fake default on a live account.
                    if str(self.mode).strip().upper() == "LIVE":
                        self._mark_failed("LIVE_PREFLIGHT_FAILED:equity_zero_at_calibration")
                        return
                    equity = _default_equity
                    logger.warning("[%s] Tradier returned zero equity — using default $%.2f", self.email, equity)
            else:
                if str(self.mode).strip().upper() == "LIVE":
                    self._mark_failed("LIVE_PREFLIGHT_FAILED:broker_missing_get_account_equity")
                    return
                equity = _default_equity
                logger.warning("[%s] Broker has no get_account_equity — using default $%.2f", self.email, equity)
        except Exception as _eq_startup_exc:
            if str(self.mode).strip().upper() == "LIVE":
                self._mark_failed(f"LIVE_PREFLIGHT_FAILED:equity_fetch_failed:{type(_eq_startup_exc).__name__}")
                return
            equity = _default_equity
            logger.warning("[%s] Equity fetch at startup failed: %s — using default $%.2f", self.email, _eq_startup_exc, equity)
        max_trades = int(client_cfg.get("max_trades_per_day", os.getenv("MAX_TRADES_TODAY", "10")) or 10)
        max_pos = int(client_cfg.get("max_concurrent_positions", os.getenv("MAX_POSITIONS", "10")) or 10)
        loss_pct = float(client_cfg.get("daily_max_loss_pct", 0.06) or 0.06)
        max_loss = -abs(equity * loss_pct)

        throttle_pct = float(os.getenv("THROTTLE_THRESHOLD_PCT", "0.02"))
        stop_pct = float(os.getenv("STOP_THRESHOLD_PCT", "0.05"))
        throttle_threshold = -abs(float(os.getenv("THROTTLE_THRESHOLD", str(equity * throttle_pct))))
        stop_threshold = -abs(float(os.getenv("STOP_THRESHOLD", str(equity * stop_pct))))

        # Per-client DB override takes precedence over env vars when set.
        # Operators set these via the admin panel or:
        #   UPDATE clients SET throttle_threshold_usd=-1500, stop_threshold_usd=-3000
        #   WHERE client_id='<email>';
        _db_throttle = client_cfg.get("throttle_threshold_usd")
        _db_stop     = client_cfg.get("stop_threshold_usd")
        if _db_throttle is not None:
            try:
                throttle_threshold = float(_db_throttle)
            except (TypeError, ValueError):
                logger.warning("[%s] Invalid throttle_threshold_usd in DB: %r — using env", self.email, _db_throttle)
        if _db_stop is not None:
            try:
                stop_threshold = float(_db_stop)
            except (TypeError, ValueError):
                logger.warning("[%s] Invalid stop_threshold_usd in DB: %r — using env", self.email, _db_stop)

        # Startup validation — warn immediately if thresholds are misordered.
        # The sizer's compute() will defensively swap them at runtime, but we
        # want operators to fix the source config, not rely on the swap.
        from ap.position_sizer import validate_sizer_thresholds
        validate_sizer_thresholds(
            throttle_threshold, stop_threshold,
            client_id=self.email, symbol="(startup)",
            log_fn=lambda msg, *a, **kw: logger.warning(msg, *a, **kw),
        )

        position_sizer = APPositionSizer(
            throttle_threshold=throttle_threshold,
            stop_threshold=stop_threshold,
            throttle_factor=float(os.getenv("THROTTLE_FACTOR", "0.5")),
            min_history=int(os.getenv("KELLY_MIN_HISTORY", "20")),
        )

        logger.info(
            "[%s] Client limits | equity=$%.2f max_trades=%s max_pos=%s daily_loss=$%.0f sizer(throttle=$%.0f stop=$%.0f)",
            self.email, equity, max_trades, max_pos, max_loss, throttle_threshold, stop_threshold,
        )

        # H3: per-client risk caps. _cfg_or_env returns the client's column
        # value when set, otherwise the exact global env default used before.
        _mc_capital_pct = self._cfg_or_env(client_cfg, "max_capital_pct", "MAX_CAPITAL_PCT", "0.40", float)
        _mc_sector_pct  = self._cfg_or_env(client_cfg, "max_sector_pct",  "MAX_SECTOR_PCT",  "0.25", float)
        _mc_ticker_pct  = self._cfg_or_env(client_cfg, "max_ticker_pct",  "MAX_TICKER_PCT",  "0.10", float)
        _mc_max_calls   = self._cfg_or_env(client_cfg, "max_calls",       "MAX_CALLS",       "10",   int)
        _mc_max_puts    = self._cfg_or_env(client_cfg, "max_puts",        "MAX_PUTS",        "10",   int)
        _mc_score_floor = self._cfg_or_env(client_cfg, "score_floor",     "SCORE_FLOOR",     "65",   float)
        _mc_ctx_floor   = self._cfg_or_env(client_cfg, "context_floor",   "CONTEXT_FLOOR",   "0.0",  float)

        # PR #155 — split per-position cap from total portfolio exposure cap.
        # max_position_pct: per-trade budget as fraction of equity.
        #   Falls back to max_capital_pct if column is NULL (backward compat).
        # max_total_capital_pct: total portfolio exposure cap.
        #   Falls back to DEFAULT_MAX_TOTAL_CAPITAL_PCT env (default 0.40).
        # Both fields are hydrated from client_risk_profiles via _rp_field_map
        # above before _cfg_or_env reads them. NULL in the risk profile means
        # "let APMasterControl.__init__ resolve the fallback".
        _mc_position_pct = self._cfg_or_env(
            client_cfg, "max_position_pct", "DEFAULT_MAX_POSITION_PCT", None, float
        )
        _mc_total_capital_pct = self._cfg_or_env(
            client_cfg, "max_total_capital_pct", "DEFAULT_MAX_TOTAL_CAPITAL_PCT", None, float
        )
        # H8: daily profit target (USD). NULL/0 = disabled. Per-client only —
        # no global env default, since a blanket target across all clients
        # would be wrong (different account sizes/goals).
        _mc_daily_target = client_cfg.get("daily_profit_target_usd")
        try:
            _mc_daily_target = float(_mc_daily_target) if _mc_daily_target is not None else 0.0
        except (TypeError, ValueError):
            _mc_daily_target = 0.0
        logger.info(
            "[%s] Risk profile | capital=%.0f%% sector=%.0f%% ticker=%.0f%% "
            "calls=%s puts=%s score_floor=%s ctx_floor=%s%s",
            self.email, _mc_capital_pct * 100, _mc_sector_pct * 100,
            _mc_ticker_pct * 100, _mc_max_calls, _mc_max_puts,
            _mc_score_floor, _mc_ctx_floor,
            " (per-client override active)" if any(
                client_cfg.get(k) is not None for k in
                ("max_capital_pct", "max_sector_pct", "max_ticker_pct",
                 "max_calls", "max_puts", "score_floor", "context_floor")
            ) else " (global defaults)",
        )

        self.master_control = APMasterControl(
            mode=self.mode.lower(),
            client_id=self.email,
            score_floor=_mc_score_floor,
            context_floor=_mc_ctx_floor,
            max_positions=max_pos,
            max_capital_pct=_mc_capital_pct,
            max_sector_pct=_mc_sector_pct,
            max_ticker_pct=_mc_ticker_pct,
            max_calls=_mc_max_calls,
            max_puts=_mc_max_puts,
            max_trades_today=max_trades,
            max_daily_loss=max_loss,
            daily_profit_target_usd=_mc_daily_target,
            account_equity=equity,
            max_position_pct=_mc_position_pct,
            max_total_capital_pct=_mc_total_capital_pct,
            position_manager=self.position_manager,
            position_sizer=position_sizer,
            supabase_client=sb,
        )

        self.order_state_machine = APOrderStateMachine(client_id=self.email)

        # WIRE-2: split-brain startup audit ───────────────────────────────────
        # Orders from a prior session where the broker accepted a submission but
        # the DB transition failed are flagged with SPLIT_BRAIN: in last_error.
        # Start in degraded mode if any exist — entries are blocked until the
        # reconciler advances the flagged orders on its next pass.
        try:
            split_brain_orders = self.order_state_machine.get_split_brain_orders(
                execution_mode=str(self.mode).strip().lower(),
            )
        except Exception as _sb_exc:
            logger.error("[%s] Split-brain startup audit failed: %s", self.email, _sb_exc)
            split_brain_orders = []

        if split_brain_orders:
            _sb_reason = f"split_brain_residue:{len(split_brain_orders)}"
            self._enter_degraded_mode(_sb_reason, stop_runner=False)
            logger.critical(
                "[%s] Starting DEGRADED — %d split-brain order(s) from prior session; "
                "broker_order_ids: %s | reconciler must recover before entries resume",
                self.email,
                len(split_brain_orders),
                [o.get("broker_order_id") for o in split_brain_orders],
            )
        # ─────────────────────────────────────────────────────────────────────

        # ─────────────────────────────────────────────────────────────────
        # PR #150 — Paper Selector Market-Data Transport Split
        # Resolve the selector / earnings / IV data_broker from the SAME
        # token priority the watcher uses:
        #   TRADIER_MARKET_DATA_TOKEN → TRADIER_DATA_TOKEN
        # PAPER fails closed if neither is set (cannot safely pick strikes
        # off 15-min-delayed sandbox chains). LIVE falls back to the live
        # execution broker, unchanged.
        # ─────────────────────────────────────────────────────────────────
        try:
            _md_transport = resolve_market_data_transport(
                mode=self.mode,
                account_id=self.account_id,
                execution_broker=broker,
            )
        except PaperSelectorNoMarketDataTokenError as _md_exc:
            # Hard fail-closed. The runner does not start with the selector
            # silently pointed at sandbox chains.
            logger.critical("[%s] %s", self.email, _md_exc)
            self._mark_failed(f"paper_selector_no_market_data_token:{_md_exc}")
            return

        data_broker = _md_transport["data_broker"]
        data_base_url = _md_transport["base_url"]
        data_token_source = _md_transport["token_source"]
        data_broker_is_dedicated = _md_transport["is_dedicated"]
        _exec_base_url = _md_transport["execution_base_url"] or self.base_url

        # ── Required audit logs (PR #150 acceptance) ─────────────────────
        # These two log lines are the operator's confirmation that the
        # transport split is wired correctly. Operators grep Render logs
        # for them after deploy.
        _mode_upper = str(self.mode).strip().upper()
        if _mode_upper == "PAPER":
            logger.info(
                "PAPER_MARKET_DATA_TRANSPORT_SELECTED client_id=%s "
                "source=tradier_live base_url=%s token_source=%s",
                self.email, data_base_url, data_token_source,
            )
            logger.info(
                "PAPER_EXECUTION_TRANSPORT_SELECTED client_id=%s "
                "source=tradier_sandbox base_url=%s",
                self.email, _exec_base_url,
            )
        else:
            logger.info(
                "[%s] Live data broker resolved | base_url=%s "
                "token_source=%s dedicated=%s",
                self.email, data_base_url, data_token_source,
                data_broker_is_dedicated,
            )

        if str(self.mode).strip().upper() == "LIVE" and getattr(self, "live_preflight_status", "") != "disabled_by_env":
            _md_ok, _md_reason = self._run_live_market_data_preflight(
                data_broker,
                data_broker_source=data_token_source,
                data_base_url=data_base_url,
                data_broker_is_dedicated=data_broker_is_dedicated,
            )
            if not _md_ok:
                self._mark_failed(f"LIVE_PREFLIGHT_FAILED:{_md_reason}")
                return

        self.data_broker = data_broker
        self.databroker = data_broker

        # P0 (PR #264): LIVE enforcement proof — runs after both preflight
        # halves (auth/equity + market data) so a runner that reaches here
        # has working credentials and data, and must now prove it is under
        # the ENFORCED live policy before any thread starts. Pins the
        # resulting runtime_config_hash on the OSM so every order row
        # created by any path carries enforcement provenance.
        if str(self.mode).strip().upper() == "LIVE":
            _enf_ok, _enf_reason = self._verify_live_enforcement(
                score_floor=_mc_score_floor,
                capital_pct=_mc_capital_pct,
                sector_pct=_mc_sector_pct,
                ticker_pct=_mc_ticker_pct,
                max_calls=_mc_max_calls,
                max_puts=_mc_max_puts,
                daily_max_loss_pct=loss_pct,
            )
            if not _enf_ok:
                self._mark_failed(f"LIVE_ENFORCEMENT_FAILED:{_enf_reason}")
                return
            try:
                if self.order_state_machine is not None:
                    self.order_state_machine.runtime_config_hash = self.runtime_config_hash
            except Exception:
                pass

        earnings_guard = APEarningsGuard(
            broker=data_broker,
            blackout_days=int(os.getenv("EARNINGS_BLACKOUT_DAYS", "3")),
        )
        iv_filter = APIVRankFilter(
            broker=data_broker,
            max_iv_rank=float(os.getenv("MAX_IV_RANK", "100")),
            hard_cap=float(os.getenv("IV_HARD_CAP", "150")),
            mode=os.getenv("BOT_MODE", "PAPER").upper(),
        )

        self.contract_selector = APContractSelectionEngine(
            broker=broker,
            data_broker=data_broker,
            mode=self.mode.lower(),
            target_delta=float(os.getenv("TARGET_DELTA", "0.50")),
            max_spread_pct=float(os.getenv("MAX_SPREAD_PCT", "0.50")),
            min_oi=int(os.getenv("MIN_OI", "1")),
            min_volume=int(os.getenv("MIN_VOLUME", "0")),
            max_dte=int(os.getenv("MAX_DTE", "21")),
            min_premium=float(os.getenv("MIN_PREMIUM", "10.0")),
            max_premium=float(os.getenv("MAX_PREMIUM", "350.0")),
            earnings_guard=earnings_guard,
            iv_filter=iv_filter,
        )

        self.core = APExecutionCore(
            broker=broker,
            supabase_client=sb,
            email=self.email,
            position_manager=self.position_manager,
            order_state_machine=self.order_state_machine,
            data_broker=data_broker,
            master_control=self.master_control,
            contract_selector=self.contract_selector,
        )
        self.core.start()
        self._validate_execution_core_started()

        exit_eng = getattr(self.core, "exit_eng", None)
        if exit_eng is None:
            self._mark_failed("exit_engine_missing")
            self.stopped.set()
            return
        exit_eng.order_state_machine = self.order_state_machine
        exit_eng.osm = self.order_state_machine

        # PR D / FIX-2 (BUG-CR-4): wire kill_switch_fn to read the explicit
        # self.kill_switch_active flag. The old lambda
        # `lambda: getattr(self.core, "_kill_switch", False)` was dead code
        # — self.core._kill_switch is never set anywhere, so the lambda
        # always returned False. The dashboard / admin path can now flip
        # the flag via runner.trip_kill_switch(reason) and master_control
        # will block new entries + fire force_close_all on the next tick.
        self.master_control.wire(
            kill_switch_fn=lambda: self.kill_switch_active,
            mode_fn=lambda: getattr(self.core, "mode", self.mode),
            entries_paused_fn=self._read_entries_paused,
        )

        self._register_exit_engine(exit_eng)
        self._run_startup_recovery(broker, exit_eng)
        # PR feature/morning-handoff-audit — automatic startup watcher re-arm.
        # Called immediately after startup recovery so any valid non-terminal
        # ENTRY rows in the DB (PENDING_TRIGGER, WATCHING, CREATED) are
        # classified and re-armed before the poll loop starts.
        # entry_watcher is guaranteed to exist here: _start_worker_thread
        # would have already blocked on entry_watcher_missing if absent.
        # Failures are logged but never crash the runner.
        self._seed_exit_engine_from_db(exit_eng)
        self._start_position_quote_monitor(data_broker if data_broker_is_dedicated else broker, exit_eng)
        # PR D / FIX-3 (BUG-CR-2): post-QPM quote refresh in LIVE.
        # The first refresh inside _seed_exit_engine_from_db runs BEFORE
        # QPM is attached. If that refresh fails (Render cold-start
        # network blip, broker auth race), the exit engine carries
        # stale entry-time underlyings until the first 8s poll cycle
        # — a brief but real window where a seeded position that
        # immediately hits a stop condition fires the stop at entry
        # price instead of current price. This second refresh runs
        # AFTER QPM is up so the engine has live prices before the
        # poll loop runs.
        if self.mode == "LIVE" and exit_eng is not None and hasattr(exit_eng, "_refresh_quotes"):
            try:
                exit_eng._refresh_quotes()
                logger.info("[%s] Post-QPM quote refresh complete", self.email)
            except Exception as qe:
                logger.warning(
                    "[%s] Post-QPM quote refresh failed: %s — first poll cycle will correct",
                    self.email, qe,
                )
        self._start_reconciler(broker, exit_eng)
        self._sync_account_equity(broker)

        health_mon = get_monitor()
        if health_mon:
            health_mon.register(self)
            health_mon.clear_dashboard_alert(self.email)

        healer = get_healer()
        if healer:
            healer.register(self)

        self.order_monitor = APOrderMonitor(
            client_id=self.email,
            broker=broker,
            order_state_machine=self.order_state_machine,
            position_manager=self.position_manager,
            exit_engine=exit_eng,
            entry_watcher=getattr(self.core, "entry_watcher", None),
            contract_selector=getattr(self.core, "contract_selector", None),
            client_mode=self.mode,   # PR66: pass PAPER/LIVE so max-age splits correctly
            data_broker=data_broker,
        )
        self.order_monitor.start()

        self._start_fill_monitor(broker, exit_eng)
        self._assert_fill_monitor_alive()

        self._start_equity_refresh(broker)
        self._start_worker_thread(broker)
        self._assert_worker_alive()

        self._build_startup_manifest(
            equity=equity,
            max_trades=max_trades,
            max_pos=max_pos,
            max_loss=max_loss,
            throttle_threshold=throttle_threshold,
            stop_threshold=stop_threshold,
            data_broker_is_dedicated=data_broker_is_dedicated,
            exit_eng=exit_eng,
            mc_score_floor=_mc_score_floor,
            mc_ctx_floor=_mc_ctx_floor,
            mc_capital_pct=_mc_capital_pct,
            mc_sector_pct=_mc_sector_pct,
            mc_ticker_pct=_mc_ticker_pct,
            risk_profile_source=_risk_profile_source,
            risk_profile_valid=(len(_missing_live) == 0),
            missing_risk_fields=_missing_live,
            daily_max_loss_pct=loss_pct,
        )

        self._validate_control_stack()

        self.initialized.set()
        self._set_entry_permission()
        self._run_startup_morning_handoff()
        self._start_runtime_health_loop()

        logger.info(
            "[%s] Control stack initialized | mode=%s max_pos=%s max_trades=%s daily_loss=$%.0f entries_allowed=%s",
            self.email, self.mode, max_pos, max_trades, max_loss, self.entries_allowed.is_set(),
        )

        healer_ref = get_healer()
        while not self.stopped.wait(60):
            try:
                if healer_ref:
                    healer_ref.heartbeat(self.email, "runner")
            except Exception as _e:
                logger.debug("runner_healer_heartbeat_failed_2: %s", _e)
            self._check_split_brain_recovery()   # BUG-5 FIX: clear sticky reasons when resolved
            self._try_recover_degraded_mode()
            self._set_entry_permission()

    def stop(self):
        self.stopping.set()
        self.entries_allowed.clear()
        self.stopped.set()

    def _cleanup(self):
        self.entries_allowed.clear()
        self.degraded.set()

        qm = getattr(self, "quotemonitor", None) or getattr(self, "quote_monitor", None)
        if qm is not None:
            try:
                qm.stop()
                logger.info("[%s] PositionQuoteMonitor stopped", self.email)
            except Exception as exc:
                logger.warning("[%s] PositionQuoteMonitor stop failed: %s", self.email, exc)
            finally:
                self.quotemonitor = None
                self.quote_monitor = None

        if getattr(self, "exit_reliability_monitor", None):
            try:
                self.exit_reliability_monitor.stop()
                logger.info("[%s] ExitReliabilityMonitor stopped", self.email)
            except Exception as _e:
                logger.warning("runner_exit_reliability_monitor_stop_failed: %s", _e)
            finally:
                self.exit_reliability_monitor = None

        if self.order_monitor:
            try:
                self.order_monitor.stop()
            except Exception as _e:
                logger.warning("runner_order_monitor_stop_failed: %s", _e)
        if self.reconciler:
            try:
                self.reconciler.stop()
            except Exception as _e:
                logger.warning("runner_reconciler_stop_failed: %s", _e)
        if self.core:
            try:
                self.core.stop()
            except Exception as _e:
                logger.warning("runner_core_stop_failed: %s", _e)

        # Join child threads briefly after stop signals to prevent zombie overlap.
        self._join_child_threads()

        try:
            from ap.order_state_machine import unregister_exit_engine
            unregister_exit_engine(self.email)
        except Exception as _e:
            logger.warning("runner_unregister_exit_engine_failed: %s", _e)

        health_mon = get_monitor()
        if health_mon:
            try:
                health_mon.unregister(self.email)
            except Exception as _e:
                logger.warning("runner_health_mon_unregister_failed: %s", _e)

        healer = get_healer()
        if healer:
            try:
                healer.unregister(self.email)
            except Exception as _e:
                logger.warning("runner_healer_unregister_failed: %s", _e)

    def _join_child_threads(self):
        timeout = float(os.getenv("RUNNER_CHILD_JOIN_TIMEOUT_SEC", "2"))
        current = threading.current_thread()
        for name in ("fill_monitor_thread", "worker_thread", "equity_thread", "health_thread"):
            thread = getattr(self, name, None)
            if thread and thread is not current and thread.is_alive():
                try:
                    thread.join(timeout=timeout)
                    if thread.is_alive():
                        logger.warning("[%s] %s still alive after %.1fs join", self.email, name, timeout)
                except RuntimeError:
                    pass

    def _clear_old_phantom_orders(self):
        try:
            from ap.db import conn as _conn

            # FIX-2: In LIVE mode, SUBMITTED/ACKNOWLEDGED orders without a broker_order_id
            # can be genuine split-brain events (broker accepted but DB write of broker_order_id
            # failed). Cancelling them at startup would discard the only local record of a real
            # live order, leaving an open position at Tradier with no tracking in DB.
            #
            # Two-tier defence:
            #   Tier A (safe, hardened by p0/guard-startup-phantom-clear AND
            #     p0/startup-cleanup-safe-skip):
            #     ENTRY kind only, CREATED/PENDING_TRIGGER, no broker_order_id,
            #     no submitted_ts, no filled_ts, no position_id, age >=
            #     STARTUP_PHANTOM_CLEAR_MIN_AGE_SECONDS (default 900s = 15 min,
            #     per 2026-06-03 parity incident), not currently watcher-armed,
            #     created BEFORE this runner's startup grace window, AND the
            #     canonical_signal_id has no active/recent sibling row on any
            #     client.
            #   Tier B (guarded): SUBMITTED/ACKNOWLEDGED with no broker ID — only cancel
            #     if last_error indicates a known pre-submission state (never reached broker)
            #     OR age exceeds PHANTOM_SUBMITTED_MIN_AGE_MINUTES (default 30 in LIVE,
            #     5 in PAPER). Any split-brain order with a real last_error should be left
            #     for the reconciler to resolve on the next pass.
            #
            # 2026-06-03 parity safety (p0/startup-cleanup-safe-skip):
            #   Tier A no longer blindly cancels. Candidates are partitioned into
            #   three buckets and stamped with explicit reason codes so SQL
            #   audits can distinguish them:
            #     - STARTUP_CLEANUP_CANCELED_STALE_ORPHAN  : truly stale, no peer activity
            #     - STARTUP_CLEANUP_SKIPPED_ACTIVE_SIGNAL  : canonical signal is alive
            #                                                on a peer client — mark
            #                                                RETRY_ELIGIBLE for re-eval
            #     - STARTUP_CLEANUP_SKIPPED_RECENT_ROW     : within age/grace window
            #                                                — never touched here
            #
            # p0 scope locks: only this method is modified. Tier B SQL, env
            # defaults, and audit logging are unchanged.

            # Tier A age gate — operator-tunable in seconds (preferred) with
            # legacy minutes fallback for back-compat. Default 600s = 10 min;
            # the previous default was 5 min and Friday's audit showed 407
            # cancels under that window, many of which were fresh pre-submit
            # entries that hadn't had a chance to submit yet.
            _legacy_min_age_min = int(os.getenv("PHANTOM_ORDER_MIN_AGE_MINUTES", "5"))
            # 2026-06-03 parity incident: raised default from 600s (10 min) to
            # 900s (15 min) per the safe-skip spec. Operators can override via
            # the env var; legacy minute fallback preserved for back-compat.
            min_age_seconds = int(os.getenv(
                "STARTUP_PHANTOM_CLEAR_MIN_AGE_SECONDS",
                str(max(900, _legacy_min_age_min * 60)),
            ))
            # Tier B keeps the original minutes-style param it always used
            # (PHANTOM_ORDER_MIN_AGE_MINUTES). PR p0/guard-startup-phantom-clear
            # only hardens Tier A; restoring the `min_age` name here preserves
            # the original Tier B SQL parameter exactly. Codex pre-merge review
            # caught the NameError this would otherwise raise inside
            # _clear_phantoms() and rollback Tier A.
            min_age = _legacy_min_age_min
            # Startup grace window: orders created within STARTUP_GRACE_SECONDS
            # of this runner's process start are NEVER cancelled by this path,
            # even if their absolute age exceeds min_age_seconds. Defends
            # against deploys/restarts that race against in-flight signals.
            startup_grace_seconds = int(os.getenv("STARTUP_PHANTOM_CLEAR_GRACE_SECONDS", "180"))
            cleanup_rule_version = "v2"
            startup_ts = getattr(self, "_runner_startup_ts", None)
            if startup_ts is None:
                # Defensive: if the runner didn't record its start time,
                # treat NOW as start to be maximally conservative.
                import time as _time
                startup_ts = _time.time()
            submitted_min_age = int(os.getenv(
                "PHANTOM_SUBMITTED_MIN_AGE_MINUTES",
                "30" if self.mode == "LIVE" else "5",
            ))

            def _clear_phantoms():
                with _conn() as c:
                    # Tier A (2026-06-03 parity-safe rewrite):
                    #
                    # Pre-submission ENTRY orders only. Hardened gates:
                    #   - kind = 'ENTRY'              (exits never go here)
                    #   - broker_order_id IS NULL / blank / sentinel
                    #   - submitted_ts IS NULL        (never reached broker submit)
                    #   - filled_ts IS NULL           (never partially/fully filled)
                    #   - position_id IS NULL         (never reached position lifecycle)
                    #   - status IN ('CREATED','PENDING_TRIGGER','PENDING','DEFERRED')
                    #   - age >= min_age_seconds      (default 900s = 15 min)
                    #   - created_ts <= NOW() - startup_grace_seconds
                    #                                 (don't wipe orders created
                    #                                  during this runner's grace
                    #                                  window — those become
                    #                                  STARTUP_CLEANUP_SKIPPED_RECENT_ROW)
                    #   - meta->'watcher_audit'->>'reason_code' IS NULL
                    #     OR the watcher audit indicates an invalidation/expiry.
                    #                                 (don't wipe orders that
                    #                                  the watcher still considers
                    #                                  pending; let the watcher
                    #                                  cancel them with the real
                    #                                  reason instead.)
                    #
                    # PARITY SAFETY (2026-06-03 fix, 2026-06-25 REEVAL amend):
                    #   Even after all the above pass, we partition the surviving
                    #   candidates by whether the same opportunity is still alive
                    #   anywhere in the system. For REEVAL rows we normalize:
                    #     REEVAL:<uuid>:<hash> -> <uuid>         for ap_signals
                    #     REEVAL:<uuid>:<hash> -> REEVAL:<uuid>  for order peers
                    #   and then check:
                    #     - active peer orders on the normalized canonical id
                    #     - active ap_signals rows on the underlying UUID
                    #     - active trade_queue rows on the UUID / REEVAL id
                    #   If any proof exists, the row is NOT canceled — it is
                    #   moved to RETRY_ELIGIBLE with reason
                    #   STARTUP_CLEANUP_SKIPPED_ACTIVE_SIGNAL so the recovery /
                    #   handoff path can re-evaluate it. Truly orphaned rows get
                    #   STARTUP_CLEANUP_CANCELED_STALE_ORPHAN.
                    #
                    # Audit: meta is MERGED (jsonb concat) so existing
                    # watcher_audit, sizing_context, signal_id, etc. are NEVER
                    # overwritten. The cleanup_phantom_audit key carries the
                    # full decision context for post-mortem.
                    #
                    # CAS safety: the SQL itself encodes every gate; an order
                    # whose updated_ts moved forward (e.g. transitioned to
                    # SUBMITTED by the worker between our SELECT and UPDATE)
                    # falls out of the status filter and is not touched.
                    #
                    # Forward-compat: the canonical_signal_id column is added
                    # by migrations/20260604_canonical_signal_id_and_ledger.sql
                    # (PR #79). The CTE below references it with a defensive
                    # COALESCE so a missing column (pre-migration) gracefully
                    # falls back to comparing on signal_id only.
                    parity_window_seconds = int(os.getenv(
                        "STARTUP_PHANTOM_PARITY_WINDOW_SECONDS", "900",
                    ))
                    c.execute(
                        """
                        WITH candidates AS (
                            SELECT  o.local_order_id,
                                    o.signal_id,
                                    CASE
                                        WHEN o.signal_id LIKE 'REEVAL:%:%'
                                            THEN split_part(o.signal_id, ':', 2)
                                        WHEN o.signal_id LIKE 'REEVAL:%'
                                            THEN split_part(o.signal_id, ':', 2)
                                        ELSE o.signal_id
                                    END AS real_signal_id,
                                    CASE
                                        WHEN TRIM(COALESCE(o.canonical_signal_id, '')) <> ''
                                            THEN o.canonical_signal_id
                                        WHEN o.signal_id LIKE 'REEVAL:%:%'
                                            THEN 'REEVAL:' || split_part(o.signal_id, ':', 2)
                                        WHEN o.signal_id LIKE 'REEVAL:%'
                                            THEN o.signal_id
                                        ELSE o.signal_id
                                    END AS order_canon_id,
                                    o.created_ts
                            FROM    orders o
                            WHERE   o.client_id = %s
                              AND   o.kind = 'ENTRY'
                              AND   o.status IN ('CREATED','PENDING_TRIGGER','PENDING','DEFERRED')
                              AND   o.created_ts < NOW() - (%s || ' seconds')::interval
                              AND   o.created_ts < to_timestamp(%s)
                              AND   o.submitted_ts IS NULL
                              AND   o.filled_ts IS NULL
                              AND   o.position_id IS NULL
                              AND   (
                                        o.broker_order_id IS NULL
                                    OR  TRIM(COALESCE(o.broker_order_id, '')) = ''
                                    OR  UPPER(TRIM(COALESCE(o.broker_order_id, ''))) IN ('N/A','NA','NONE','NULL')
                                    )
                              AND   (
                                        o.meta->'watcher_audit' IS NULL
                                    OR  o.meta->'watcher_audit'->>'reason_code' IN (
                                            'watcher_invalidated','watcher_expired',
                                            'stop_bid_below_call_stop','stop_ask_above_put_stop',
                                            'overnight_daily_invalidated',
                                            'overnight_premarket_breached',
                                            'overnight_too_far_from_trigger',
                                            'rearm_window_expired',
                                            'opposite_side_replaced_stale_or_weaker'
                                        )
                                    )
                        ),
                        active_proof AS (
                            SELECT DISTINCT ca.local_order_id
                            FROM   candidates ca
                            WHERE  EXISTS (
                                       SELECT 1
                                       FROM   orders p
                                       WHERE  p.local_order_id <> ca.local_order_id
                                         AND  COALESCE(p.canonical_signal_id, p.signal_id) = ca.order_canon_id
                                         AND  COALESCE(p.canonical_signal_id, p.signal_id) IS NOT NULL
                                         AND  (
                                                   p.status IN ('SUBMITTED','ACKNOWLEDGED','FILLED','PARTIALLY_FILLED')
                                               OR  p.filled_ts IS NOT NULL
                                               OR  p.submitted_ts IS NOT NULL
                                               OR  p.created_ts > NOW() - (%s || ' seconds')::interval
                                              )
                                   )
                               OR  EXISTS (
                                       SELECT 1
                                       FROM   ap_signals s
                                       WHERE  s.signal_id::text = ca.real_signal_id
                                         AND  UPPER(COALESCE(s.decision_status, '')) IN ('WATCHING','ARMED')
                                   )
                               OR  EXISTS (
                                       SELECT 1
                                       FROM   trade_queue tq
                                       WHERE  tq.client_id = %s
                                         AND  tq.status IN ('NEW','PROCESSING','WATCHING')
                                         AND  (
                                                   COALESCE(tq.signal_id, '') = ca.real_signal_id
                                               OR  COALESCE(tq.signal_id, '') = ca.signal_id
                                               OR  COALESCE(tq.signal_id, '') = ca.order_canon_id
                                               OR  COALESCE(tq.signal_id, '') LIKE (ca.order_canon_id || ':%')
                                              )
                                   )
                        ),
                        cancel_targets AS (
                            UPDATE orders o
                            SET    status = 'CANCELED',
                                   last_error = 'STARTUP_CLEANUP_CANCELED_STALE_ORPHAN',
                                   updated_ts = NOW(),
                                   meta = COALESCE(o.meta, '{}'::jsonb) || jsonb_build_object(
                                       'cleanup_phantom_audit', jsonb_build_object(
                                           'block_stage',         'startup_cleanup',
                                           'block_reason',        'STARTUP_CLEANUP_CANCELED_STALE_ORPHAN',
                                           'cleanup_decision',    'cancelled',
                                           'cleanup_rule_version',%s,
                                           'min_age_seconds',     %s,
                                           'startup_grace_seconds', %s,
                                           'startup_grace_active', false,
                                           'had_broker_order_id', false,
                                           'had_submitted_ts',    false,
                                           'had_filled_ts',       false,
                                           'had_position_id',     false,
                                           'canonical_signal_active', false,
                                           'parity_window_seconds', %s,
                                           'watcher_armed',       false,
                                           'age_seconds',         EXTRACT(EPOCH FROM (NOW() - o.created_ts)),
                                           'cleanup_ts',          NOW()::text
                                       )
                                   )
                            FROM   candidates ca
                            WHERE  o.local_order_id = ca.local_order_id
                              AND  NOT EXISTS (
                                       SELECT 1 FROM active_proof ap
                                       WHERE  ap.local_order_id = ca.local_order_id
                                   )
                            RETURNING o.local_order_id
                        ),
                        skip_targets AS (
                            UPDATE orders o
                            SET    status = 'RETRY_ELIGIBLE',
                                   last_error = 'STARTUP_CLEANUP_SKIPPED_ACTIVE_SIGNAL',
                                   updated_ts = NOW(),
                                   meta = COALESCE(o.meta, '{}'::jsonb) || jsonb_build_object(
                                       'cleanup_phantom_audit', jsonb_build_object(
                                           'block_stage',         'startup_cleanup',
                                           'block_reason',        'STARTUP_CLEANUP_SKIPPED_ACTIVE_SIGNAL',
                                           'cleanup_decision',    'skipped_retry_eligible',
                                           'cleanup_rule_version',%s,
                                           'min_age_seconds',     %s,
                                           'startup_grace_seconds', %s,
                                           'canonical_signal_active', true,
                                           'parity_window_seconds', %s,
                                           'age_seconds',         EXTRACT(EPOCH FROM (NOW() - o.created_ts)),
                                           'cleanup_ts',          NOW()::text
                                       )
                                   )
                            FROM   candidates ca
                            WHERE  o.local_order_id = ca.local_order_id
                              AND  EXISTS (
                                       SELECT 1 FROM active_proof ap
                                       WHERE  ap.local_order_id = ca.local_order_id
                                   )
                            RETURNING o.local_order_id
                        )
                        SELECT
                            (SELECT COUNT(*) FROM cancel_targets) AS canceled,
                            (SELECT COUNT(*) FROM skip_targets)   AS retry_eligible
                        """,
                        (
                            self.email,
                            str(min_age_seconds),
                            float(startup_ts) - startup_grace_seconds,
                            str(parity_window_seconds),
                            self.email,
                            cleanup_rule_version, min_age_seconds, startup_grace_seconds,
                            str(parity_window_seconds),
                            cleanup_rule_version, min_age_seconds, startup_grace_seconds,
                            str(parity_window_seconds),
                        ),
                    )
                    _row = c.fetchone() or (0, 0)
                    # Support dict-cursor and tuple-cursor results.
                    if isinstance(_row, dict):
                        tier_a_cancel = int(_row.get("canceled") or 0)
                        tier_a_skip   = int(_row.get("retry_eligible") or 0)
                    else:
                        tier_a_cancel = int(_row[0] or 0)
                        tier_a_skip   = int(_row[1] or 0)
                    tier_a = tier_a_cancel + tier_a_skip

                    # Tier B: submitted/acknowledged with no broker ID — only cancel if
                    # last_error signals a known never-reached-broker state, OR age exceeds
                    # the longer submitted threshold. Log at CRITICAL when this path fires
                    # in LIVE mode so any residual split-brain is immediately visible.
                    c.execute(
                        """
                        UPDATE orders
                        SET status = %s,
                            last_error = %s,
                            updated_ts = NOW()
                        WHERE client_id = %s
                          AND status IN ('SUBMITTED', 'ACKNOWLEDGED')
                          AND created_ts < NOW() - (%s || ' minutes')::interval
                          AND (
                                broker_order_id IS NULL
                             OR TRIM(COALESCE(broker_order_id, '')) = ''
                             OR UPPER(TRIM(COALESCE(broker_order_id, ''))) IN ('N/A', 'NA', 'NONE', 'NULL')
                          )
                          AND (
                                COALESCE(last_error, '') IN (
                                    'startup_phantom', 'never_submitted', 'pre_submit_canceled',
                                    'invalid_entry_limit_price', 'invalid_existing_entry_qty',
                                    'missing_existing_entry_contract', 'invalid_exit_limit_price'
                                )
                             OR created_ts < NOW() - (%s || ' minutes')::interval
                          )
                        """,
                        (
                            "CANCELED", "startup_phantom_clear_submitted",
                            self.email, str(min_age),
                            str(submitted_min_age),
                        ),
                    )
                    tier_b = c.rowcount or 0
                    return tier_a_cancel, tier_a_skip, tier_b

            tier_a_cancel, tier_a_skip, tier_b = run_with_retry(_clear_phantoms)
            if tier_a_cancel:
                logger.info(
                    "[%s] Startup: cleared %s pre-submit phantom orders "
                    "(STARTUP_CLEANUP_CANCELED_STALE_ORPHAN)",
                    self.email, tier_a_cancel,
                )
            if tier_a_skip:
                # Parity safety: peer client(s) have an alive row on the same
                # canonical_signal_id. These were NOT canceled — they are now
                # RETRY_ELIGIBLE so the peer-retry path can re-evaluate.
                logger.info(
                    "[%s] Startup: marked %s rows RETRY_ELIGIBLE "
                    "(STARTUP_CLEANUP_SKIPPED_ACTIVE_SIGNAL) — canonical signal "
                    "is active on a peer client",
                    self.email, tier_a_skip,
                )
            if tier_b:
                log_fn = logger.critical if self.mode == "LIVE" else logger.warning
                log_fn(
                    "[%s] Startup: cleared %s submitted/acknowledged phantom orders (mode=%s) "
                    "— verify no live Tradier positions are orphaned",
                    self.email, tier_b, self.mode,
                )
                if self.mode == "LIVE":
                    self._enter_degraded_mode(
                        f"startup_phantom_submitted_live:{tier_b}",
                        stop_runner=False,
                    )
        except Exception as exc:
            logger.warning("[%s] Startup phantom clear: %s", self.email, exc)

    def _read_entries_paused(self) -> bool:
        """H5: per-client entries pause flag from client_state.entries_paused.

        Read fresh so an operator toggle in the admin dashboard takes effect
        with no bot restart. Cached for a few seconds so a burst of signals
        does not hammer the DB. Fail-open: a transient read error must not
        halt a client's trading — the kill switch remains the hard safety
        stop, this is an operator-convenience pause.
        """
        import time as _t
        _now = _t.monotonic()
        _cached = getattr(self, "_entries_paused_cache", None)
        if _cached is not None and _cached[1] > _now:
            return _cached[0]
        paused = False
        try:
            from ap.state import load_state
            st = load_state(client_id=self.email) or {}
            paused = bool(st.get("entries_paused", False))
        except Exception as exc:
            logger.warning("[%s] entries_paused read failed (fail-open): %s",
                            self.email, exc)
            paused = False
        ttl = float(os.getenv("ENTRIES_PAUSED_CACHE_TTL", "5.0"))
        self._entries_paused_cache = (paused, _now + ttl)
        return paused

    def _load_client_config(self) -> dict:
        """Load per-client risk profile.

        Source priority (highest to lowest):
        1. client_risk_profiles row (explicit per-client limits — required for LIVE)
        2. clients table columns (legacy per-client overrides)
        3. Global env defaults (PAPER only — never for LIVE)

        H3: Risk caps were previously global env vars — every client shared
        MAX_CAPITAL_PCT / MAX_SECTOR_PCT / MAX_TICKER_PCT / MAX_CALLS /
        MAX_PUTS / SCORE_FLOOR. That cannot serve a $5K beta client and a
        proven $35K client simultaneously.
        """
        # Check if a validated risk profile was attached at boot
        _attached_rp = getattr(self, "_risk_profile", None)
        if not _attached_rp:
            # Try fetching from Supabase directly if not attached
            try:
                from ap.queue import _get_sb_client as _get_sb
                _sb_inst = _get_sb()
                if _sb_inst:
                    _rp_res = (
                        _sb_inst.table("client_risk_profiles")
                        .select("*")
                        .eq("client_email", self.email)
                        .limit(1)
                        .execute()
                    )
                    if _rp_res.data:
                        _attached_rp = _rp_res.data[0]
            except Exception as _rp_load_err:
                logger.warning(
                    "[%s] Could not load client_risk_profiles: %s",
                    self.email, _rp_load_err,
                )
        self._risk_profile = _attached_rp or {}
        try:
            from ap.db import conn as _conn

            def _load_cfg():
                with _conn() as c:
                    c.execute(
                        "SELECT max_trades_per_day, max_concurrent_positions, "
                        "daily_max_loss_pct, initial_equity, "
                        "max_capital_pct, max_sector_pct, max_ticker_pct, "
                        "max_calls, max_puts, score_floor, context_floor, "
                        "daily_profit_target_usd, entries_enabled "
                        "FROM clients WHERE client_id=%s",
                        (self.email,),
                    )
                    return c.fetchone()

            return run_with_retry(_load_cfg) or {}
        except Exception as exc:
            logger.warning("[%s] Could not load client config: %s", self.email, exc)
            return {}

    @staticmethod
    def _cfg_or_env(cfg: dict, key: str, env_name: str, env_default: str, cast):
        """Return per-client cfg[key] when set (non-None), else env fallback.

        Preserves exact current behavior when the column is NULL/absent.
        """
        val = cfg.get(key) if isinstance(cfg, dict) else None
        if val is None:
            return cast(os.getenv(env_name, env_default))
        try:
            return cast(val)
        except (TypeError, ValueError):
            return cast(os.getenv(env_name, env_default))

    def _run_morning_handoff_audit_startup(self) -> None:
        """Compatibility wrapper for the unified startup morning handoff path."""
        self._run_startup_morning_handoff()


    def _log_startup_recovery_complete(
        self,
        *,
        status: str,
        started_at: float,
        recovery_attempt_id: str,
        result: dict | None = None,
        errors: list[str] | None = None,
    ) -> None:
        result = result if isinstance(result, dict) else {}
        autonomy_ctx = _autonomy_log_context(self.mode)
        duration_ms = int(max(0.0, time.time() - started_at) * 1000)
        error_list = [str(item) for item in (errors or result.get("errors") or []) if str(item)]
        watchers_restored = int(
            result.get("watchers_restored")
            or result.get("watchers_requeued")
            or 0
        )
        deferred_retries_restored = int(
            result.get("deferred_retries_restored")
            or result.get("deferred_lifecycles_recovered")
            or 0
        )
        ownership_failures = result.get("ownership_failures")
        if ownership_failures is None:
            ownership_failures = len(error_list)
        recovered_orders = int(
            result.get("recovered_orders")
            or result.get("entries_corrected")
            or result.get("positions_recovered")
            or 0
        )
        logger.info(
            "STARTUP_RECOVERY_COMPLETE status=%s client_id=%s execution_mode=%s "
            "duration_ms=%s errors=%s client_count=%s paper_client_count=%s "
            "live_client_count=%s commit_sha=%s pod_id=%s watchers_restored=%s "
            "deferred_retries_restored=%s ownership_failures=%s recovered_orders=%s "
            "recovery_attempt_id=%s",
            status,
            self.email,
            str(self.mode).lower(),
            duration_ms,
            error_list,
            autonomy_ctx["client_count"],
            autonomy_ctx["paper_client_count"],
            autonomy_ctx["live_client_count"],
            autonomy_ctx["commit_sha"],
            autonomy_ctx["pod_id"],
            watchers_restored,
            deferred_retries_restored,
            ownership_failures,
            recovered_orders,
            recovery_attempt_id,
        )


    def _run_startup_recovery(self, broker, exit_eng):
        """Run startup recovery with a hard timeout to prevent blocking initialization.
        Recovery is best-effort — a timeout logs a warning but never blocks entries.
        """
        import concurrent.futures as _cf
        _RECOVERY_TIMEOUT = float(os.getenv("STARTUP_RECOVERY_TIMEOUT_SEC", "25"))
        _recovery_attempt_id = uuid.uuid4().hex
        _recovery_started_at = time.time()
        _completion_logged = False

        def _do_recovery():
            recovery = APStartupRecovery(
                client_id=self.email,
                broker=broker,
                osm=self.order_state_machine,
                pm=self.position_manager,
                master_control=self.master_control,
                exit_engine=exit_eng,
                entry_watcher=getattr(getattr(self, "core", None), "entry_watcher", None),
                execution_core=getattr(self, "core", None),  # §2: required for safe BROKER_READY recovery
            )
            return recovery.run(include_watcher_reseed=False)

        try:
            with _cf.ThreadPoolExecutor(max_workers=1) as _ex:
                _fut = _ex.submit(_do_recovery)
                try:
                    rec_result = _fut.result(timeout=_RECOVERY_TIMEOUT)
                    logger.info(
                        "[%s] Startup recovery complete: positions=%s entries_corrected=%s exits=%s dedup=%s",
                        self.email,
                        rec_result.get("positions_recovered"),
                        rec_result.get("entries_corrected"),
                        rec_result.get("exits_reattached"),
                        rec_result.get("dedup_seeded"),
                    )
                    self._log_startup_recovery_complete(
                        status="success",
                        started_at=_recovery_started_at,
                        recovery_attempt_id=_recovery_attempt_id,
                        result=rec_result,
                    )
                    _completion_logged = True
                except _cf.TimeoutError:
                    logger.warning(
                        "[%s] Startup recovery timed out after %.0fs — continuing without full recovery. "
                        "Open positions may not be reseeded until next restart.",
                        self.email, _RECOVERY_TIMEOUT,
                    )
                    _fut.cancel()
                    self._log_startup_recovery_complete(
                        status="timeout",
                        started_at=_recovery_started_at,
                        recovery_attempt_id=_recovery_attempt_id,
                        errors=[f"startup_recovery_timeout:{_RECOVERY_TIMEOUT:g}s"],
                    )
                    _completion_logged = True
                except Exception as exc:
                    self._log_startup_recovery_complete(
                        status="failed",
                        started_at=_recovery_started_at,
                        recovery_attempt_id=_recovery_attempt_id,
                        errors=[str(exc)],
                    )
                    _completion_logged = True
                    raise
        except Exception as exc:
            if not _completion_logged:
                self._log_startup_recovery_complete(
                    status="failed",
                    started_at=_recovery_started_at,
                    recovery_attempt_id=_recovery_attempt_id,
                    errors=[str(exc)],
                )
            logger.error("[%s] Startup recovery error: %s", self.email, exc)
            # FIX-6: in LIVE mode a failed recovery means open positions from a prior
            # session may not be reseeded to the exit engine, leaving live contracts
            # with no exit protection. Enter degraded mode to block new entries while
            # the existing positions remain tracked. Do not stop the runner — fill
            # monitor and exit engine can still protect positions already in memory.
            if self.mode == "LIVE":
                self._enter_degraded_mode(
                    f"startup_recovery_failed:{exc}", stop_runner=False
                )

    def _register_exit_engine(self, exit_eng):
        try:
            from ap.order_state_machine import register_exit_engine
            register_exit_engine(self.email, exit_eng)
            logger.info("[%s] Exit engine registered with OSM", self.email)
        except Exception as exc:
            logger.warning("[%s] Exit engine registration error: %s", self.email, exc)

    def _seed_exit_engine_from_db(self, exit_eng):
        # FIX-3: seed_from_db() sets current_underlying = underlying_entry on restart,
        # not a fresh live quote. In LIVE mode this means the exit engine's underlying
        # price is stale from entry time until the first 8-second poll cycle refreshes it.
        # The first quote fetch after startup corrects this, but any exit logic that fires
        # during that first poll window (especially EOD/sentinel checks) will use entry
        # price as current underlying. This is a known issue in seed_from_db() itself
        # that must be fixed there before relying on seeding for live positions.
        # Until fixed: log a visible warning in LIVE mode so the gap is not invisible.
        if self.mode == "LIVE":
            logger.warning(
                "[%s] _seed_exit_engine_from_db: seed_from_db sets current_underlying=underlying_entry "
                "not a live quote — exit engine may use stale underlying price until first poll cycle (~8s). "
                "Fix seed_from_db() to fetch live quotes before relying on seeding in LIVE mode.",
                self.email,
            )
        try:
            if exit_eng and hasattr(exit_eng, "seed_from_db"):
                exit_eng.seed_from_db(self.position_manager)
                logger.info("[%s] Exit engine reseeded from DB", self.email)
                # Immediately refresh underlying prices after seed so exit logic
                # does not fire on stale entry-time prices during first poll window.
                if self.mode == "LIVE" and hasattr(exit_eng, "_refresh_quotes"):
                    try:
                        exit_eng._refresh_quotes()
                        logger.info("[%s] Post-seed quote refresh complete", self.email)
                    except Exception as qe:
                        logger.warning(
                            "[%s] Post-seed quote refresh failed: %s — first poll cycle will correct",
                            self.email, qe,
                        )
        except Exception as exc:
            logger.warning("[%s] Exit engine DB seed error: %s", self.email, exc)

    def _start_reconciler(self, broker, exit_eng):
        if not ENABLE_BROKER_RECONCILER:
            logger.info("[%s] Broker reconciler disabled (ENABLE_BROKER_RECONCILER=0)", self.email)
            self.reconciler = None
            return

        try:
            self.reconciler = APBrokerReconciler(
                broker=broker,
                client_id=self.email,
                osm=self.order_state_machine,
                pm=self.position_manager,
                execution_mode=str(self.mode).strip().lower(),
                supabase_client=getattr(self, "supabase", None),  # Requirement 3
            )
            self.reconciler.exit_engine = exit_eng
            # P0-3: give reconciler master_control reference so it can self-
            # check daily-loss breach. Belt-and-suspenders alongside exit-engine
            # tick check — reconciler runs even if exit engine is degraded.
            try:
                self.reconciler.master_control = getattr(exit_eng, "master_control", None)
            except Exception as _e:
                logger.warning("reconciler_master_control_wire_failed: %s", _e)
            # Wire fill_monitor so reconciler can confirm it's alive
            # fill_monitor_thread is set by _start_fill_monitor() — pass it now
            # if already running, or it will be set after _start_fill_monitor() runs
            if hasattr(self, 'fill_monitor_thread') and self.fill_monitor_thread:
                self.reconciler.fill_monitor = self.fill_monitor_thread
            self.reconciler.start()
            logger.info("[%s] Broker reconciler started", self.email)
        except Exception as exc:
            logger.error("[%s] Reconciler start error: %s", self.email, exc)
            self.reconciler = None

    def _start_fill_monitor(self, broker, exit_eng):
        from ap.fill_monitor import fill_monitor_loop

        restart_sleep = float(os.getenv("FILL_MONITOR_RESTART_SLEEP_SEC", "2"))

        _fm_crash_count = 0
        _fm_last_crash_ts = 0.0
        _fm_last_success_ts = 0.0
        _FM_DB_ERR_PHRASES = ("ssl", "eof", "connection", "socket", "timeout", "pool", "interface")
        _FM_TRANSIENT_THRESHOLD = int(os.getenv("FILL_MONITOR_TRANSIENT_THRESHOLD", "5"))
        _FM_CRASH_WINDOW = float(os.getenv("FILL_MONITOR_CRASH_WINDOW_SEC", "300"))

        def _run_fill_monitor():
            nonlocal _fm_crash_count, _fm_last_crash_ts, _fm_last_success_ts
            while not self.stopped.is_set():
                _loop_start = time.time()
                try:
                    fill_monitor_loop(
                        broker=broker,
                        poll_seconds=10.0,
                        osm=self.order_state_machine,
                        pm=self.position_manager,
                        exit_engine=exit_eng,
                        stop_event=self.stopped,
                        client_id=self.email,
                    )
                    if self.stopped.is_set():
                        break
                    # Reset crash count if the loop ran for a meaningful period
                    if (time.time() - _loop_start) > 30:
                        _fm_crash_count = 0
                        _fm_last_success_ts = time.time()
                    # Unexpected clean return — only degrade if sustained
                    _fm_crash_count += 1
                    if _fm_crash_count >= _FM_TRANSIENT_THRESHOLD:
                        self._enter_degraded_mode("fill_monitor_loop_returned", stop_runner=False)
                    logger.warning("[%s] fill_monitor_loop returned unexpectedly (crash #%d) -- restarting in %.1fs", self.email, _fm_crash_count, restart_sleep)
                    time.sleep(restart_sleep)
                except Exception as exc:
                    if self.stopped.is_set():
                        break
                    exc_str = str(exc).lower()
                    _is_db_transient = any(p in exc_str for p in _FM_DB_ERR_PHRASES)
                    _now = time.time()
                    # Reset crash count if last crash was outside the window
                    # (independent transient events, not a sustained failure)
                    if _fm_last_crash_ts > 0 and (_now - _fm_last_crash_ts) >= _FM_CRASH_WINDOW:
                        _fm_crash_count = 0
                    # Also reset if fill_monitor ran successfully for >30s since last crash
                    if (_loop_start - _fm_last_crash_ts) > 30 and _fm_last_crash_ts > 0:
                        _fm_crash_count = 0
                    _fm_crash_count += 1
                    # Transient DB errors (SSL EOF, connection reset): restart silently
                    # without degrading entries_allowed. Only degrade after N rapid
                    # crashes within the window, or for non-DB errors.
                    if _is_db_transient and _fm_crash_count <= _FM_TRANSIENT_THRESHOLD:
                        logger.warning("[%s] fill_monitor DB transient crash #%d (%s) -- restarting silently in %.1fs",
                                       self.email, _fm_crash_count, exc, restart_sleep)
                    else:
                        self._enter_degraded_mode(f"fill_monitor_loop_crashed:{exc}", stop_runner=False)
                        logger.error("[%s] fill_monitor_loop crashed: %s -- restarting in %.1fs", self.email, exc, restart_sleep, exc_info=True)
                    _fm_last_crash_ts = _now
                    time.sleep(restart_sleep)

        self.fill_monitor_thread = threading.Thread(
            target=_run_fill_monitor,
            daemon=True,
            name=f"fill-monitor-{self.email}",
        )
        self.fill_monitor_thread.start()
        logger.info("[%s] Fill monitor thread launched", self.email)

        # Wire into reconciler now that thread exists — reconciler was started first
        if self.reconciler is not None:
            self.reconciler.fill_monitor = self.fill_monitor_thread
            logger.info("[%s] fill_monitor wired into reconciler", self.email)

    def _assert_fill_monitor_alive(self):
        time.sleep(float(os.getenv("FILL_MONITOR_START_GRACE_SEC", "0.5")))
        if not self.fill_monitor_thread or not self.fill_monitor_thread.is_alive():
            self._mark_failed("fill_monitor_failed_to_start")
            self.stopped.set()
            raise RuntimeError(f"[{self.email}] Fill monitor failed to start")

    def _sync_account_equity(self, broker):
        try:
            balance = None
            if hasattr(broker, "get_account_equity"):
                balance = broker.get_account_equity()
            elif hasattr(broker, "get_account_balance"):
                balance = broker.get_account_balance()
            elif hasattr(broker, "get_balances"):
                b = broker.get_balances()
                balance = b.get("equity") or b.get("total_equity") or b.get("net_liquidation") or b.get("cash")
            if balance and float(balance) > 0:
                self.master_control.set_account_equity(float(balance))
                logger.info("[%s] Live equity synced: $%.2f", self.email, float(balance))
            else:
                logger.warning("[%s] Could not pull live equity -- using default $%.2f", self.email, self.master_control.account_equity)
        except Exception as exc:
            logger.warning("[%s] Equity sync failed: %s -- using default", self.email, exc)

    def _start_equity_refresh(self, broker):
        # FIX-5: if the process starts after 9:30 AM ET the 900s equity thread loop
        # never fires reset_session() for that day because at_open only matches
        # 9:30–10:59 AM. Seed dedup state from DB reflects prior-session signals, so
        # a mid-morning restart blocks same-day signals that haven't been traded yet.
        # Run reset_session() once on startup if market is already open for the day.
        try:
            _now_et = datetime.now(_ET)
            _market_open = (
                (_now_et.hour == 9 and _now_et.minute >= 30)
                or _now_et.hour >= 10
            )
            if _market_open and self.master_control and hasattr(self.master_control, "reset_session"):
                self.master_control.reset_session(client_id=self.email)
                logger.info(
                    "[%s] Startup-time session reset — market already open at %s ET",
                    self.email, _now_et.strftime("%H:%M"),
                )
        except Exception as exc:
            logger.warning("[%s] Startup-time session reset failed: %s", self.email, exc)

        reset_done_for_date = datetime.now(_ET).strftime("%Y-%m-%d") if (
            datetime.now(_ET).hour >= 10
            or (datetime.now(_ET).hour == 9 and datetime.now(_ET).minute >= 30)
        ) else ""

        def _refresh_loop():
            nonlocal reset_done_for_date
            healer_ref = get_healer()
            _refresh_interval = int(os.getenv("EQUITY_REFRESH_INTERVAL_SEC", "900"))
            while not self.stopped.wait(_refresh_interval):
                self._sync_account_equity(broker)
                self.last_equity_heartbeat_ts = time.time()
                try:
                    if healer_ref:
                        healer_ref.heartbeat(self.email, "equity_refresh")
                except Exception as _e:
                    logger.debug("runner_healer_heartbeat_failed_3: %s", _e)

                try:
                    now_et = datetime.now(_ET)
                    today_str = now_et.strftime("%Y-%m-%d")
                    at_open = (now_et.hour == 9 and now_et.minute >= 15) or now_et.hour == 10  # 9:15 AM — before signals hit the queue at open
                    if at_open and reset_done_for_date != today_str:
                        if self.master_control and hasattr(self.master_control, "reset_session"):
                            self.master_control.reset_session(client_id=self.email)
                        logger.info("[%s] Session dedup reset at market open (%s)", self.email, now_et.strftime("%H:%M ET"))
                        reset_done_for_date = today_str
                except Exception as exc:
                    logger.debug("[%s] Session reset check failed: %s", self.email, exc)

        self.equity_thread = threading.Thread(target=_refresh_loop, daemon=True, name=f"equity-refresh-{self.email}")
        self.equity_thread.start()

    def _start_worker_thread(self, broker):
        entry_watcher = getattr(self.core, "entry_watcher", None)
        if entry_watcher is None:
            self._mark_failed("entry_watcher_missing")
            self.stopped.set()
            raise RuntimeError(f"[{self.email}] ENTRY WATCHER MISSING — worker startup aborted")

        is_live = self.mode == "LIVE"

        try:
            from ap.intelligence_context_worker import start_intelligence_context_worker

            self.intelligence_context_thread = start_intelligence_context_worker(
                client_id=self.email,
                execution_mode=self.mode,
                stop_event=self.stopped,
                broker=broker,
            )
        except Exception as _intel_worker_exc:
            logger.warning(
                "[%s] intelligence context worker startup skipped: %s",
                self.email,
                _intel_worker_exc,
            )

        # WIRE-3: capture the callback reference once here so the closure is
        # stable across worker restarts. worker_loop must call this with
        # on_split_brain(local_order_id=..., broker_order_id=...) when it sees
        # res.get("split_brain") == True from submit_existing_entry/submit_exit.
        _split_brain_cb = self._on_split_brain_detected

        def _run_worker():
            restart_sleep = float(os.getenv("WORKER_RESTART_SLEEP_SEC", "2"))
            while not self.stopped.is_set():
                try:
                    worker_loop(
                        broker,
                        master_control=self.master_control,
                        contract_selector=self.contract_selector,
                        order_state_machine=self.order_state_machine,
                        entry_watcher=entry_watcher,
                        position_manager=self.position_manager,
                        client_id=self.email,
                        stop_event=self.stopped,
                        live_mode=is_live,
                        exit_eng=getattr(self.core, "exit_eng", None),
                        # WIRE-3: freeze callback for split-brain runtime detection.
                        # worker_loop must accept and call this when it sees
                        # res.get("split_brain") from any submit result.
                        on_split_brain=_split_brain_cb,
                    )
                    if self.stopped.is_set():
                        break
                    self._enter_degraded_mode("worker_loop_returned", stop_runner=False)
                    logger.warning("[%s] worker_loop returned unexpectedly -- restarting in %.1fs", self.email, restart_sleep)
                    time.sleep(restart_sleep)
                except Exception as exc:
                    if self.stopped.is_set():
                        break
                    self._enter_degraded_mode(f"worker_loop_crashed:{exc}", stop_runner=False)
                    logger.error("[%s] worker_loop crashed: %s -- restarting in %.1fs", self.email, exc, restart_sleep, exc_info=True)
                    time.sleep(restart_sleep)

        self.worker_thread = threading.Thread(target=_run_worker, daemon=True, name=f"worker-{self.email}")
        self.worker_thread.start()
        if not self.worker_thread.is_alive():
            self._mark_failed("worker_thread_failed_to_start")
            self.stopped.set()
            raise RuntimeError(f"[{self.email}] worker_thread failed to start")

        logger.info("[%s] Queue subscriber started", self.email)


def route_signal_to_all_clients(signal: dict):
    signal_id = str(signal.get("signal_id") or uuid.uuid4())
    signal["signal_id"] = signal_id
    ticker = signal.get("ticker", "?")

    with _registry_lock:
        # ── ROUTING GATE ────────────────────────────────────────────────
        # A runner is eligible if it is alive, initialized, and not
        # permanently failed/stopping. We deliberately do NOT gate on
        # entries_allowed or degraded here — those are transient states
        # (QPM not yet heartbeated, fill monitor starting up) and should
        # not cause signals to be silently dropped. The signal goes into
        # the durable queue and the runner processes it once entries_allowed
        # recovers. Signals are only truly dropped if the runner is dead,
        # stopping, or in a hard-failed state.
        active_emails = [
            email
            for email, runner in _active_runners.items()
            if (
                runner.is_alive()
                and runner.initialized.is_set()
                and not runner.stopping.is_set()
                and not runner.failed.is_set()
            )
        ]
        # Log a warning (not a drop) if entries are temporarily blocked
        for email, runner in _active_runners.items():
            if (
                runner.is_alive()
                and runner.initialized.is_set()
                and not runner.stopping.is_set()
                and not runner.failed.is_set()
                and (runner.degraded.is_set() or not runner.entries_allowed.is_set())
            ):
                logger.warning(
                    "ROUTE_WARN [%s]: runner alive but entries_allowed=%s degraded=%s — "
                    "signal queued anyway, will execute when gate clears",
                    email,
                    runner.entries_allowed.is_set(),
                    runner.degraded.is_set(),
                )

    # PR1: Create one opportunity row per active client BEFORE fanout.
    # Uses canonical_signal_id as primary idempotency key (PR79 / PR81 amend).
    # Fail-safe — never blocks routing.
    if active_emails:
        try:
            from ap.opportunity_ledger import create_opportunities
            _canonical_sid = signal.get("canonical_signal_id") or signal_id
            try:
                from ap_canonical_signal import build_canonical_signal_id as _bcsid
                _canonical_sid = _bcsid(signal) or _canonical_sid
            except Exception:
                pass
            create_opportunities(
                signal_id=signal_id,
                canonical_signal_id=_canonical_sid,
                client_ids=active_emails,
                payload=signal,
            )
        except Exception as _ol_err:
            log.debug("opportunity_ledger.create_opportunities skipped: %s", _ol_err)

    if not active_emails:
        if not ALLOW_SUPABASE_FANOUT_FALLBACK:
            with _registry_lock:
                for _email, _r in _active_runners.items():
                    _alive = _r.is_alive()
                    _init = _r.initialized.is_set()
                    _stop = _r.stopping.is_set()
                    _fail = _r.failed.is_set()
                    _deg  = _r.degraded.is_set()
                    _ea   = _r.entries_allowed.is_set()
                    logger.warning(
                        "ROUTE_BLOCK [%s]: alive=%s init=%s stop=%s fail=%s degraded=%s entries_allowed=%s reasons=%s",
                        _email, _alive, _init, _stop, _fail, _deg, _ea,
                        sorted(getattr(_r, "degraded_reasons", set())) if _deg else []
                    )
            logger.warning(
                "Signal %s [%s] -- no local entries-allowed runners and Supabase fallback disabled; dropping",
                signal_id, ticker,
            )
            return 0

        now = _time_module.monotonic()
        with _registry_lock:
            cached = _members_cache.get("emails")
        if cached and cached[1] > now:
            active_emails = cached[0]
            logger.debug("Signal %s [%s] -- using cached member list (%s clients)", signal_id, ticker, len(active_emails))
        else:
            logger.warning(
                "Signal %s [%s] -- no initialized local runners, using explicit Supabase fallback",
                signal_id,
                ticker,
            )
            try:
                if not (SUPABASE_URL and SUPABASE_SERVICE_KEY):
                    # Consistent with BUG-3 fix: both credentials required before
                    # attempting create_client. An empty service key produces a
                    # confusing Supabase auth error rather than a clean empty list.
                    logger.error(
                        "Signal %s [%s] -- Supabase fallback requested but credentials incomplete; dropping",
                        signal_id, ticker,
                    )
                    active_emails = []
                else:
                    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
                    members = _fetch_active_members(sb)
                    active_emails = [m["email"] for m in members if m.get("email")]
                    with _registry_lock:
                        _members_cache["emails"] = (active_emails, now + 60.0)
            except Exception as exc:
                logger.error("Supabase members fallback failed: %s", exc)
                active_emails = []

    if not active_emails:
        logger.error("Signal %s [%s] -- no members found, dropping", signal_id, ticker)
        return 0

    enqueued = 0
    for email in active_emails:
        try:
            ok = enqueue_signal(signal, client_id=email, idempotency_key=f"{signal_id}:{email}")
            if ok:
                enqueued += 1
                logger.info("Signal %s [%s] -> queued for %s", signal_id, ticker, email)
            else:
                logger.debug("Signal %s duplicate for %s -- skipped", signal_id, email)
        except Exception as exc:
            logger.error("Failed to enqueue signal for %s: %s", email, exc)

    logger.info("Signal %s [%s] fan-out complete -- %s/%s clients queued", signal_id, ticker, enqueued, len(active_emails))
    return enqueued


def _fetch_active_members(sb: Client) -> list[dict]:
    """Load the members this Render service is responsible for.

    Modes (priority order):
      1. SINGLE_CLIENT_EMAIL set -> EXACTLY that one client. Hard-fail boot
         if not found.
      2. POD_ID set -> only members where execution_pod=POD_ID. Hard-fail
         boot if zero, or if eligible count > MAX_POD_CLIENTS.
      3. Neither -> SHARED mode. BLOCKED for live unless ALLOW_SHARED_LIVE=1.

    LIVE guardrails (BOT_MODE=live):
      - allow_live_trading must be true (else dropped, CRITICAL).
      - a valid client_risk_profiles row must exist (else dropped, CRITICAL).
      - shared mode is disabled unless ALLOW_SHARED_LIVE=1.

    Runtime overflow preserves ALREADY-ACTIVE runners — a newly assigned
    client never displaces a client whose runner is already trading.
    """
    try:
        _bot_mode = os.getenv("BOT_MODE", os.getenv("MODE", "paper")).strip().lower()
        _is_live  = _bot_mode == "live"

        # FIX 2: SINGLE_CLIENT_EMAIL is the canonical var. The generic
        # CLIENT_ID env is only honored as single-client if it looks like an
        # email (so a stray CLIENT_ID=default never forces single-client mode).
        _single = os.getenv("SINGLE_CLIENT_EMAIL", "").strip()
        if not _single:
            _legacy = os.getenv("CLIENT_ID", "").strip()
            if _legacy and "@" in _legacy:
                _single = _legacy
        _pod_id  = os.getenv("POD_ID", "").strip()
        _max_pod = int(os.getenv("MAX_POD_CLIENTS", "5"))

        # FIX 1: live shared mode is disabled unless explicitly allowed.
        if _is_live and not _single and not _pod_id:
            if os.getenv("ALLOW_SHARED_LIVE", "").strip() != "1":
                logger.critical(
                    "LIVE SHARED MODE DISABLED | BOT_MODE=live but neither "
                    "SINGLE_CLIENT_EMAIL nor POD_ID is set. A live service must "
                    "never load all clients implicitly. Set POD_ID or "
                    "SINGLE_CLIENT_EMAIL, or ALLOW_SHARED_LIVE=1 to override."
                )
                import sys as _sys
                _sys.stdout.flush(); _sys.stderr.flush()
                os._exit(4)

        # ── SCHEMA-RESILIENT FETCH ────────────────────────────────────────
        # A missing optional column (execution_pod, allow_live_trading) must
        # NEVER zero out every runner and halt all trading. That exact
        # failure happened on the pod-model deploy: execution_pod did not
        # exist yet, the whole members fetch threw, found 0 active members,
        # and BA/MS signals were dropped with 503. We try the full select
        # first; on a column error we fall back to the guaranteed-present
        # core columns and synthesize safe defaults.
        _FULL_COLS = (
            "id,email,name,tier,"
            "tradier_account_mode,"
            "tradier_account_id,tradier_access_token,tradier_base_url,"
            "tradier_paper_account_id,tradier_paper_access_token,"
            "tradier_live_account_id,tradier_live_access_token,"
            "subscription_active,approved,allow_live_trading,execution_pod"
        )
        _CORE_COLS = (
            "id,email,name,tier,"
            "tradier_account_mode,"
            "tradier_account_id,tradier_access_token,tradier_base_url,"
            "tradier_paper_account_id,tradier_paper_access_token,"
            "tradier_live_account_id,tradier_live_access_token,"
            "subscription_active,approved"
        )

        def _run_fetch(cols: str, with_pod_filter: bool):
            q = (
                sb.table("members")
                .select(cols)
                .eq("approved", True)
                .eq("subscription_active", True)
                .eq("tradier_account_mode", _bot_mode)
            )
            if _single:
                q = q.eq("email", _single)
            elif _pod_id and with_pod_filter:
                q = q.eq("execution_pod", _pod_id)
            return q.execute()

        mode_label = "SHARED"
        if _single:
            mode_label = f"SINGLE_CLIENT={_single}"
        elif _pod_id:
            mode_label = f"POD={_pod_id}"

        _schema_degraded = False
        try:
            res = _run_fetch(_FULL_COLS, with_pod_filter=True)
            members = res.data or []
        except Exception as _full_exc:
            _msg = str(_full_exc).lower()
            if "execution_pod" in _msg or "allow_live_trading" in _msg or "column" in _msg:
                _schema_degraded = True
                logger.critical(
                    "SCHEMA MISMATCH | members fetch with full columns failed "
                    "(%s). Falling back to core columns so trading is NOT "
                    "halted. RUN THE MIGRATION: "
                    "ALTER TABLE members ADD COLUMN IF NOT EXISTS "
                    "execution_pod text; "
                    "ALTER TABLE members ADD COLUMN IF NOT EXISTS "
                    "allow_live_trading boolean DEFAULT false;",
                    _full_exc,
                )
                # Core-column fetch (no pod filter possible — column missing).
                res = _run_fetch(_CORE_COLS, with_pod_filter=False)
                members = res.data or []
                for _m in members:
                    _m.setdefault("execution_pod", None)
                    # Missing allow_live_trading: default False = SAFE. A live
                    # service then drops everyone (correct) until migrated.
                    _m.setdefault("allow_live_trading", False)
            else:
                raise

        # If schema is degraded AND a pod filter was requested, we could not
        # filter by execution_pod in SQL. Without the column we cannot know
        # pod membership — fail SAFE: a pod service must not load unfiltered
        # clients. Hard-fail boot so the migration is run before trading.
        if _schema_degraded and _pod_id and not _single:
            try:
                _any_active = len(_active_runners) > 0
            except Exception:
                _any_active = False
            if not _any_active:
                logger.critical(
                    "POD MODE + MISSING execution_pod COLUMN | cannot determine "
                    "pod membership. Refusing to start a pod service that would "
                    "load clients it cannot pod-filter. RUN THE MIGRATION then "
                    "redeploy. Hard-exit so Render shows a failed deploy."
                )
                import sys as _sys
                _sys.stdout.flush(); _sys.stderr.flush()
                os._exit(6)

        # LIVE guardrail: allow_live_trading must be true.
        if _is_live:
            allowed = []
            for m in members:
                if not m.get("allow_live_trading"):
                    logger.critical(
                        "LIVE GUARDRAIL | %s loaded but allow_live_trading is "
                        "false — DROPPED. This client will NOT trade live.",
                        m.get("email"),
                    )
                    continue
                allowed.append(m)
            members = allowed

        # LIVE risk profile validation.
        # In LIVE mode: every client MUST have a client_risk_profiles row with
        # ALL required fields non-null. Null = "use global default" is
        # acceptable for PAPER but not for live client money.
        _REQUIRED_LIVE_FIELDS = [
            "max_capital_pct", "max_sector_pct", "max_ticker_pct",
            "max_calls", "max_puts", "score_floor", "context_floor",
            "max_positions", "daily_max_loss_pct", "entries_enabled",
        ]
        # Store the full risk profile rows keyed by email for later use
        _risk_profiles: dict = {}
        if _is_live and members:
            try:
                _emails = [m["email"] for m in members]
                _rp_res = (
                    sb.table("client_risk_profiles")
                    .select("*")
                    .in_("client_email", _emails)
                    .execute()
                )
                for _rp_row in (_rp_res.data or []):
                    _risk_profiles[_rp_row["client_email"]] = _rp_row
            except Exception as _rp_exc:
                logger.critical(
                    "LIVE RISK-PROFILE CHECK FAILED to query client_risk_profiles "
                    "(%s). Failing safe — no live client will load.", _rp_exc,
                )
            _rp_ok = []
            for m in members:
                _email = m["email"]
                if _email not in _risk_profiles:
                    logger.critical(
                        "LIVE RISK GUARDRAIL | %s has NO client_risk_profiles "
                        "row — DROPPED. Live clients must have explicit risk "
                        "limits, never global fallback defaults.",
                        _email,
                    )
                    continue
                # Validate all required fields are non-null
                _rp_row = _risk_profiles[_email]
                _missing = [
                    f for f in _REQUIRED_LIVE_FIELDS
                    if _rp_row.get(f) is None
                ]
                if _missing:
                    logger.critical(
                        "LIVE RISK GUARDRAIL | %s has NULL required risk fields "
                        "%s — DROPPED. Set all required fields in "
                        "client_risk_profiles before going live.",
                        _email, _missing,
                    )
                    continue
                # Attach validated risk profile to member dict for ClientRunner
                m["_risk_profile"] = _rp_row
                m["_risk_profile_valid"] = True
                _rp_ok.append(m)
            members = _rp_ok

        # Per-client broker credential validation (PR: Remove Global Tradier Guard)
        # Validates each assigned client has usable broker creds before running.
        # Skips only the failing client — never crashes the pod.
        if _is_live and members:
            _cred_ok     = []
            _cred_missing = []
            for m in members:
                _email   = m.get("email", "")
                _mode_m  = (m.get("tradier_active_mode") or "paper").lower()
                if _mode_m == "live":
                    _has_creds = bool(
                        m.get("tradier_live_account_id") and
                        m.get("tradier_live_access_token")
                    )
                    _allow_live = bool(m.get("allow_live_trading", False) or
                                       m.get("approved", False))
                else:
                    _has_creds  = bool(
                        m.get("tradier_account_id") and
                        m.get("tradier_access_token")
                    )
                    _allow_live = True  # paper always allowed

                if not _has_creds:
                    logger.error(
                        "Client skipped: missing Tradier credentials | "
                        "client_id=%s email=%s pod_id=%s mode=%s",
                        m.get("id"), _email, _pod_id, _mode_m,
                    )
                    _cred_missing.append(_email)
                    continue

                if _mode_m == "live" and not _allow_live:
                    logger.error(
                        "Client skipped: allow_live_trading not set | "
                        "client_id=%s email=%s pod_id=%s",
                        m.get("id"), _email, _pod_id,
                    )
                    _cred_missing.append(_email)
                    continue

                _cred_ok.append(m)

            members = _cred_ok

            # POD_BOOT summary log
            _global_fallback = bool(
                os.environ.get("TRADIER_ACCESS_TOKEN") and
                os.environ.get("TRADIER_ACCOUNT_ID")
            )
            logger.info(
                "[POD_BOOT] pod_id=%s mode=LIVE assigned_clients=%d "
                "broker_valid=%d broker_missing=%d "
                "skipped=%s global_tradier_fallback=%s",
                _pod_id,
                len(members) + len(_cred_missing),
                len(members),
                len(_cred_missing),
                _cred_missing or "none",
                str(_global_fallback).lower(),
            )

        # FIX 3: runtime overflow must preserve already-active runners.
        # Never sort all members and keep first N — that can drop a client
        # whose runner is already trading. Keep active pod runners first;
        # only admit new clients into remaining slots; reject extras.
        if _pod_id and len(members) > _max_pod:
            try:
                _active_now = set(_active_runners.keys())
            except Exception:
                _active_now = set()
            _already = [m for m in members if m["email"] in _active_now]
            _new     = [m for m in members if m["email"] not in _active_now]
            _slots   = max(0, _max_pod - len(_already))
            _admit   = _new[:_slots]
            _reject  = _already[_max_pod:] + _new[_slots:]
            # If somehow more than _max_pod are ALREADY active (shouldn't happen
            # because boot check prevents it), keep them all running — never
            # kill a live runner mid-day — but log it loudly.
            if len(_already) > _max_pod:
                logger.critical(
                    "POD OVERFLOW | POD_ID=%s has %d ALREADY-ACTIVE runners "
                    "(> MAX_POD_CLIENTS=%d). Keeping all active to avoid "
                    "killing live positions. Investigate immediately.",
                    _pod_id, len(_already), _max_pod,
                )
                members = _already
            else:
                members = _already + _admit
            if _reject:
                logger.critical(
                    "POD OVERFLOW AT RUNTIME | POD_ID=%s | preserving %d active "
                    "runner(s), admitting %d new; REJECTING (not loaded): %s. "
                    "Move these to another pod and redeploy.",
                    _pod_id, len(_already), len(_admit),
                    ", ".join(m.get("email", "?") for m in _reject),
                )

        # FIX 4: SINGLE or POD mode with ZERO eligible clients is a
        # misconfiguration — hard-fail boot so Render shows a failed deploy
        # instead of an alive bot trading nothing. (Only hard-exit when no
        # runners are active yet — i.e. at boot — never mid-day when runners
        # exist, to avoid killing live positions on a transient empty fetch.)
        if (_single or _pod_id) and not members:
            try:
                _any_active = len(_active_runners) > 0
            except Exception:
                _any_active = False
            if not _any_active:
                logger.critical(
                    "BOOT HARD FAILURE | %s resolved to ZERO eligible clients. "
                    "A configured dedicated/pod service with no clients is a "
                    "misconfiguration. Check member rows, execution_pod, "
                    "allow_live_trading, risk profiles, and env vars.",
                    mode_label,
                )
                import sys as _sys
                _sys.stdout.flush(); _sys.stderr.flush()
                os._exit(5)
            else:
                logger.critical(
                    "RUNTIME WARNING | %s fetch returned ZERO clients but %d "
                    "runner(s) already active — keeping them alive (transient "
                    "empty fetch?). NOT hard-failing mid-session.",
                    mode_label, len(_active_runners),
                )
                return []

        masked = ", ".join(
            (m.get("email", "?")[:3] + "***" + m.get("email", "?")[-8:])
            for m in members
        ) or "(none)"
        logger.info(
            "_fetch_active_members: BOT_MODE=%s %s → %d member(s) [%s]",
            _bot_mode, mode_label, len(members), masked,
        )
        return members
    except SystemExit:
        raise
    except Exception as exc:
        logger.error("Supabase fetch failed: %s", exc)
        return []


def _sync_runners(sb: Client):
    members = _fetch_active_members(sb)
    active_emails = {m["email"] for m in members}
    to_join: list[ClientRunner] = []

    with _registry_lock:
        for email, runner in list(_active_runners.items()):
            if email not in active_emails:
                if not runner.stopping.is_set():
                    logger.info("Stopping runner for %s -- no longer active", email)
                    runner.stop()
                if not runner.is_alive():
                    _active_runners.pop(email, None)
                else:
                    to_join.append(runner)

        for email, runner in list(_active_runners.items()):
            if not runner.is_alive():
                logger.warning("Removing dead runner from registry: %s", email)
                _active_runners.pop(email, None)

        for member in members:
            email = member["email"]
            existing = _active_runners.get(email)
            if existing and existing.is_alive():
                # Don't replace a runner that is alive — even if not yet initialized.
                # Replacing an initializing runner causes double-spawn and the new
                # runner overwrites the good one, leaving entries_allowed=False forever.
                if not existing.initialized.is_set():
                    logger.info(
                        "Skipping spawn for %s — runner alive and still initializing "
                        "(initialized=False). Will check again next sync cycle.",
                        email
                    )
                continue
            logger.info("Starting runner for %s", email)
            runner = ClientRunner(member)
            _active_runners[email] = runner
            runner.start()

    join_timeout = float(os.getenv("RUNNER_STOP_JOIN_TIMEOUT_SEC", "10"))
    for runner in to_join:
        runner.join(timeout=join_timeout)
        if not runner.is_alive():
            with _registry_lock:
                if _active_runners.get(runner.email) is runner:
                    _active_runners.pop(runner.email, None)
        else:
            logger.warning("[%s] Runner still stopping after %.1fs; will retry next sync", runner.email, join_timeout)


def start_multi_client_supervisor():
    # BUG-1 FIX: Supabase credential check runs FIRST. If this process is not
    # the multi-client supervisor (no Supabase), we return immediately without
    # probing DB rowcount — the probe is only meaningful when the supervisor is
    # actually going to start and spawn runner threads.
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        logger.warning("No Supabase credentials -- multi-client supervisor not starting")
        return

    # Validate ENCRYPTION_KEY before spawning any runner. Every ClientRunner calls
    # _get_token() → decrypt_token() on startup. A missing key means every runner
    # fails on first _get_token() with a RuntimeError — after appearing healthy.
    # Fail here at supervisor startup so the problem is immediately visible.
    if not _raw_key:
        raise RuntimeError(
            "ENCRYPTION_KEY env var is required for token decryption — "
            "set it in Render env vars before starting the supervisor"
        )

    # WIRE-1: DB rowcount probe — process-level, runs once before any runner
    # thread or APOrderStateMachine is created. If the active DB driver returns
    # None for rowcount and OSM_ROWCOUNT_NONE_IS_FATAL=1 (the default), every
    # call to transition() will refuse to advance state, stalling all order
    # lifecycle updates system-wide. Fail hard here so the problem surfaces at
    # startup rather than silently during live trading.
    try:
        from ap.order_state_machine import probe_db_rowcount, _ROWCOUNT_NONE_IS_FATAL
        _rowcount_result = probe_db_rowcount()
        if _rowcount_result is None and _ROWCOUNT_NONE_IS_FATAL:
            raise RuntimeError(
                "DB driver does not expose rowcount and OSM_ROWCOUNT_NONE_IS_FATAL=1 — "
                "all OSM transitions will stall; fix the DB wrapper or set "
                "OSM_ROWCOUNT_NONE_IS_FATAL=0 only after proving writes cannot silently fail"
            )
        logger.info(
            "DB rowcount probe: %s — OSM transition safety confirmed",
            repr(_rowcount_result),
        )
    except RuntimeError:
        raise  # propagate the hard-fail; do not swallow
    except Exception as _probe_exc:
        # Any other exception from probe_db_rowcount (DB not up, import error, etc.)
        # is also fatal — we cannot start trading without knowing DB truth behavior.
        raise RuntimeError(
            f"DB rowcount probe failed at supervisor startup: {_probe_exc}"
        ) from _probe_exc

    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    init_monitor(supabase_client=sb)
    logger.info("Worker health monitor initialized")
    init_self_healing(supabase_client=sb)
    logger.info("Self-healing system initialized")

    # ── ONE-TIME HARD BOOT GUARDRAIL ──────────────────────────────────────
    # Run the full validated _fetch_active_members ONCE before the supervisor
    # thread starts. It performs ALL guardrails and hard-exits the process
    # (os._exit) on any of: live-shared-without-override, pod overflow at
    # boot, single/pod resolving to zero clients. Calling it here (not just
    # inside the supervisor's try/except) guarantees a bad config FAILS THE
    # DEPLOY instead of looping forever — the supervisor's except cannot
    # swallow os._exit. Using the same function = zero logic drift between
    # boot validation and runtime re-sync.
    try:
        _boot_members = _fetch_active_members(sb)
        logger.info(
            "BOOT GUARDRAIL PASSED | %d eligible client(s) at startup",
            len(_boot_members),
        )
    except SystemExit:
        raise
    except Exception as _boot_exc:
        # A pure query failure (network blip) should not hard-fail boot —
        # the supervisor will retry. Only the explicit os._exit guardrails
        # inside _fetch_active_members hard-fail, and those bypass this except.
        logger.error("Boot member probe failed (supervisor will retry): %s", _boot_exc)

    _supervisor_state["started"] = True

    def _supervisor():
        logger.info("Multi-client supervisor started")
        # Outer try/except catches ANY crash including import errors, network
        # issues during first fetch, etc. Without this the thread dies silently.
        try:
            # Wait for post_worker_init runners to register before first sync.
            _initial_delay = float(os.getenv("SUPERVISOR_INITIAL_DELAY_SEC", "15"))
            time.sleep(_initial_delay)
            while True:
                try:
                    members = _fetch_active_members(sb)
                    _supervisor_state["last_member_count"] = len(members)
                    _supervisor_state["last_sync_error"] = ""
                    _sync_runners(sb)
                    _supervisor_state["sync_count"] += 1
                    _supervisor_state["last_sync_ts"] = time.time()
                except Exception as exc:
                    _supervisor_state["last_sync_error"] = str(exc)
                    logger.error("Supervisor sync error: %s", exc)

                # Liveness check: sleep in short intervals between full syncs.
                # Breaks early to trigger an immediate re-sync if any runner dies.
                _sync_interval = int(os.getenv("SUPERVISOR_SYNC_SEC", "60"))
                _check_interval = int(os.getenv("SUPERVISOR_LIVENESS_CHECK_SEC", "15"))
                _elapsed = 0
                while _elapsed < _sync_interval:
                    time.sleep(_check_interval)
                    _elapsed += _check_interval
                    with _registry_lock:
                        any_dead = any(
                            not r.is_alive()
                            for r in _active_runners.values()
                        )
                    if any_dead:
                        logger.warning("Supervisor: dead runner detected — triggering early sync")
                        break
        except BaseException as fatal:
            # Thread is about to die — log it so we can see why
            _supervisor_state["last_sync_error"] = f"FATAL:{fatal}"
            logger.critical(
                "Supervisor thread FATAL crash — thread will die. "
                "Bot will rely on post_worker_init hook for runner spawning. "
                "Error: %s", fatal, exc_info=True
            )

    _supervisor_ref["thread"] = threading.Thread(target=_supervisor, daemon=True, name="client-supervisor")
    _supervisor_ref["thread"].start()
    logger.info("Client supervisor thread launched")


def get_runner_status() -> list[dict]:
    with _registry_lock:
        return [
            {
                "email": email,
                "account_id": r.account_id,
                "base_url": r.base_url,
                "alive": r.is_alive(),
                "initialized": r.initialized.is_set(),
                "failed": r.failed.is_set(),
                "stopping": r.stopping.is_set(),
                "degraded": r.degraded.is_set(),
                "entries_allowed": r.entries_allowed.is_set(),
                "degraded_reasons": sorted(list(getattr(r, "degraded_reasons", set()))),
                "failure_reason": getattr(r, "failure_reason", ""),
                "startup_manifest_present": bool(getattr(r, "startup_manifest", {})),
                "last_health_check_ts": getattr(r, "last_health_check_ts", 0.0),
                "last_fill_monitor_heartbeat_ts": getattr(r, "last_fill_monitor_heartbeat_ts", 0.0),
                "last_worker_heartbeat_ts": getattr(r, "last_worker_heartbeat_ts", 0.0),
                "last_equity_heartbeat_ts": getattr(r, "last_equity_heartbeat_ts", 0.0),
                "core_active": r.core is not None,
                "master_control": r.master_control is not None,
                "position_mgr": r.position_manager is not None,
                "order_osm": r.order_state_machine is not None,
                "contract_sel": r.contract_selector is not None,
                # FIX-11: previously both fields reported contract_selector is not None,
                # which is always True whenever the selector is initialized. Now reports
                # whether the underlying guard/filter object is actually wired into the
                # selector by checking the canonical attribute names.
                "earnings_guard": (
                    getattr(r.contract_selector, "earnings_guard", None) is not None
                    if r.contract_selector else False
                ),
                "iv_filter": (
                    getattr(r.contract_selector, "iv_filter", None) is not None
                    if r.contract_selector else False
                ),
                "order_monitor": r.order_monitor is not None,
                "quote_monitor": (getattr(r, "quotemonitor", None) is not None or getattr(r, "quote_monitor", None) is not None),
                "quote_monitor_alive": (
                    bool((getattr(r, "quotemonitor", None) or getattr(r, "quote_monitor", None))
                         and (getattr(r, "quotemonitor", None) or getattr(r, "quote_monitor", None)).is_alive())
                ),
                "quote_monitor_healthy": r._quote_monitor_healthy() if hasattr(r, "_quote_monitor_healthy") else False,
                "worker_alive": getattr(r, "worker_thread", None) is not None and r.worker_thread.is_alive(),
                "fill_monitor_alive": getattr(r, "fill_monitor_thread", None) is not None and r.fill_monitor_thread.is_alive(),
                "equity_alive": getattr(r, "equity_thread", None) is not None and r.equity_thread.is_alive(),
                "mode": getattr(r, "mode", "PAPER"),
                "ready_for_entries": (
                    r.is_alive()
                    and r.initialized.is_set()
                    and not r.failed.is_set()
                    and not r.stopping.is_set()
                    and not r.degraded.is_set()
                    and r.entries_allowed.is_set()
                ),
            }
            for email, r in _active_runners.items()
        ]
