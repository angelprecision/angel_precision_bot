# ap/queue.py -- UNIFIED CONTROL QUEUE (Postgres)
# =============================================================================
# Architecture:
#   enqueue_signal() → trade_queue table
#   worker_loop()    → claim job → master_control.evaluate() →
#                      contract_selector.select() → order_state_machine.create_entry_order()
#                      → APEntryWatcher (breach) OR immediate execution
#
# What was removed vs old queue:
#   ❌ _check_rate_limit()     -- owned by master_control (trades_today gate)
#   ❌ _is_triggered()         -- owned by APEntryWatcher
#   ❌ _get_stock_price()      -- owned by APEntryWatcher
#   ❌ trigger requeue loop    -- owned by APEntryWatcher
#   ❌ process_signal() direct -- replaced by control path
#   ❌ ev_score legacy filter  -- all signals go through master control now
#   ❌ ? placeholders          -- all %s (Postgres)
#
# What was kept:
#   ✅ enqueue_signal()        -- idempotent insert
#   ✅ _claim_one_job()        -- atomic claim + stale reclaim
#   ✅ _mark_job()             -- terminal status
#   ✅ worker_loop()           -- poll + dispatch
#
# Upgrades in this version:
#   ✅ restart_guard           -- blocks overnight signals on mid-session restart
#   ✅ 1-1 pair manager        -- cancels opposite side on fill
#   ✅ rejection feed          -- every rejection posted to Discord
# =============================================================================

from __future__ import annotations

import json
import logging
import copy
from ap.trace import trace_gate
import os
import threading
import time
import uuid
from datetime import datetime as _datetime, timedelta as _timedelta, timezone as _timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo as _ZoneInfo

from ap.state import update_state
from ap_signal_store import (
    canonical_client_email,
    canonical_signal_id,
    upsert_ap_signal_row_with_fallback,
)

def _conn():
    from ap.db import conn
    return conn

def _run_with_retry(fn, *args, **kwargs):
    from ap.db import run_with_retry
    return run_with_retry(fn, *args, **kwargs)

def _init_db():
    from ap.db import init_db
    return init_db

from ap.utils import now_utc_iso
from zoneinfo import ZoneInfo
from datetime import datetime, timezone, time as dtime

log = logging.getLogger("ap.queue")

# Tracks consecutive worker claim failures per client.
# Reset to 0 on any successful job claim.
# Used to fire QUEUE_WORKER_DEGRADED alerts before the operator notices.
_claim_fail_count: dict = {}

ET = ZoneInfo("America/New_York")

def _now_et() -> datetime:
    """Current datetime in US/Eastern."""
    return datetime.now(ET)

def _is_regular_session_et(dt=None) -> bool:
    """True if dt (or now) is within regular market hours Mon-Fri 9:30-16:00 ET."""
    dt = dt or _now_et()
    return dt.weekday() < 5 and dtime(9, 30) <= dt.time() < dtime(16, 0)

POLL_INTERVAL         = float(os.getenv("QUEUE_POLL_INTERVAL", "1.0"))
PROCESSING_STALE_SECS = int(os.getenv("PROCESSING_STALE_SECS", "120"))
ALLOW_IMMEDIATE_EXECUTION = os.getenv("ALLOW_IMMEDIATE_EXECUTION", "0").lower() in {"1", "true", "yes", "on"}

# ── PR #143 amendment: per-client PAPER_RECOVERY_IMMEDIATE_PROMOTE audit counter ──
# Process-local dict {client_id: count} incremented every time a paper recovery
# row is promoted breach→immediate. Consulted by /admin/trade_flow_status and
# exists to make the LIVE-protection invariant provable from a single source:
# if any LIVE client_id ever appears in this dict, that is a bug, not normal
# flow. Counter resets on process restart (intentional — restart = fresh audit
# window). A durable per-row audit also writes to ap_signals.context_notes below.
_PAPER_RECOVERY_IMMEDIATE_COUNTS: dict[str, int] = {}
_PAPER_RECOVERY_IMMEDIATE_LOCK = threading.Lock()


def get_paper_recovery_immediate_counts() -> dict[str, int]:
    """Return a snapshot of the per-client paper-recovery-immediate counter.

    Callers (e.g. /admin/trade_flow_status) should treat this as read-only.
    The counter is process-local so a multi-pod deployment will return
    per-pod counts; aggregate across pods via the ap_signals durable audit
    instead.
    """
    with _PAPER_RECOVERY_IMMEDIATE_LOCK:
        return dict(_PAPER_RECOVERY_IMMEDIATE_COUNTS)

# PR F / queue truth hardening: ALLOW_IMMEDIATE_EXECUTION is a money-affecting
# kill switch — it disables the breach-watch path and lets queue.py submit
# orders to the broker the instant a signal is dispatched, with no level/trigger
# confirmation. Reading it silently leaves no audit trail. Emit a CRITICAL
# log at module import so the Render logs unmistakably record when the
# override is active. (worker_loop() repeats this on startup for operators
# who attach mid-process.)
if ALLOW_IMMEDIATE_EXECUTION:
    log.critical(
        "⚠️  ALLOW_IMMEDIATE_EXECUTION=1 — IMMEDIATE EXECUTION PATH ENABLED at "
        "module import. The queue worker will bypass breach-watch and submit "
        "entry orders to the broker immediately on dispatch. This is for "
        "controlled internal testing ONLY — set ALLOW_IMMEDIATE_EXECUTION=0 "
        "in production."
    )


# =============================================================================
# HELPERS
# =============================================================================

def _now_iso() -> str:
    return now_utc_iso()


def _json_dumps(data: Any) -> str:
    return json.dumps(data, separators=(",", ":"), ensure_ascii=False)


def _parse_payload(raw) -> dict:
    """Postgres JSONB returns dict; handle string fallback."""
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        return json.loads(raw)
    return {}


# =============================================================================
# ENQUEUE -- idempotent insert
# =============================================================================

def enqueue_signal(
    sig,
    client_id: str = "default",
    idempotency_key: str | None = None,
) -> bool:
    """
    Push a signal into trade_queue.
    Returns True if inserted, False if duplicate (idempotency_key conflict).
    """
    # H1: route_signal_to_all_clients passes the SAME signal dict to
    # enqueue_signal once per client. Below we mutate `payload` in place
    # (defaults for ticker/score/side, and future per-client fields). Binding
    # payload = sig would leak one client's mutations into the next client's
    # enqueue. Always work on a deep copy so each client's payload is isolated.
    if isinstance(sig, dict):
        payload = copy.deepcopy(sig)
    elif hasattr(sig, "model_dump"):
        payload = sig.model_dump()
    elif hasattr(sig, "dict"):
        payload = sig.dict()
    else:
        raise TypeError(f"Unsupported signal type: {type(sig)}")

    if not payload.get("ticker") and payload.get("symbol"):
        payload["ticker"] = payload["symbol"]
    if not payload.get("symbol") and payload.get("ticker"):
        payload["symbol"] = payload["ticker"]

    if payload.get("score") is None:
        payload["score"] = 65.0
    if payload.get("ev_score") is None:
        payload["ev_score"] = payload["score"]

    # PR #233: fail-CLOSED on missing/invalid side at enqueue time.  Pre-#233
    # this defaulted to CALL (or PUT if signal_id hinted ":PUT"), silently
    # routing malformed signals as bullish trades.  Now we preserve the failure
    # in the payload as `side_validation_error` so _dispatch can reject it with
    # the right stage/reason_code before Master Control sees it.
    side, side_error = _normalize_queue_side(payload)
    if side_error:
        payload["side_validation_error"] = side_error
        payload.pop("side", None)
        payload.pop("direction", None)
    else:
        payload["side"] = side
        payload["direction"] = side

    # P0: Normalize raw payload keys into canonical execution metadata fields.
    # Applied BEFORE trade_queue INSERT so the worker reads canonical field names
    # at execution time and validate_entry_metadata() does not block with
    # metadata_invalid:zero_underlying / zero_trigger on valid signals that
    # merely used non-canonical field names (trigger, underlying, last, close, mark).
    # Best-effort — falls back to original payload on any import error.
    try:
        from ap_signal_normalizer import normalize_raw_signal_payload as _norm_payload
        payload = _norm_payload(payload)
    except Exception as _norm_exc:
        log.warning("enqueue_signal: payload normalization failed (non-fatal): %s", _norm_exc)

    # PR F / queue truth hardening: scanner-provided signal_id is preserved
    # verbatim via `or` short-circuit. When the scanner omits it, the fallback
    # MUST include a uuid suffix — timestamp-only fallbacks collide on burst
    # enqueues within the same millisecond, producing duplicate
    # idempotency_keys and silently dropping signals as "duplicates".
    signal_id = payload.get("signal_id") or f"signal_{uuid.uuid4().hex[:12]}_{_now_iso()}"
    if not idempotency_key:
        idempotency_key = f"{client_id}:{signal_id}"

    def _ins():
        with _conn()() as c:
            c.execute(
                """
                INSERT INTO trade_queue (
                    client_id, signal_id, created_ts, status, payload, idempotency_key
                )
                VALUES (%s, %s, NOW(), 'NEW', %s, %s)
                ON CONFLICT (idempotency_key) DO NOTHING
                """,
                (client_id, signal_id, _json_dumps(payload), idempotency_key),
            )
            # rowcount is 0 when ON CONFLICT DO NOTHING fires (duplicate);
            # 1 when the row was actually inserted.
            return getattr(c, "rowcount", None)

    try:
        rowcount = _run_with_retry(_ins)
        if rowcount == 0:
            log.debug(f"Duplicate ignored: {signal_id}")
            return False
        log.info(f"✅ Enqueued: {signal_id} client={client_id}")
        return True
    except Exception as e:
        msg = str(e).lower()
        if "unique" in msg or "conflict" in msg:
            log.debug(f"Duplicate ignored (exception path): {signal_id}")
            return False
        raise


# =============================================================================
# JOB MANAGEMENT
# =============================================================================

_TERMINAL_QUEUE_STATUSES = {
    "REJECTED",
    "ERROR",
    "SUBMITTED",
    "FILLED",
    "CANCELED",
    "CANCELLED",
    "EXPIRED",
    "DONE",
}

# Explicit CAS outcome constants for _checked_watching_cas.
# Replaces the prior bool contract so callers can distinguish materially
# different zero-row outcomes and respond correctly to each without
# blind compensation that could overwrite valid WATCHING or terminal rows.
WATCHING_CAS_TRANSITIONED          = "TRANSITIONED"           # PROCESSING→WATCHING: 1 row updated
WATCHING_CAS_ALREADY_WATCHING      = "ALREADY_WATCHING"       # row was already WATCHING (idempotent)
WATCHING_CAS_TERMINAL              = "TERMINAL"               # row is in a terminal status
WATCHING_CAS_MISSING_OR_UNEXPECTED = "MISSING_OR_UNEXPECTED"  # row missing or unexpected state
WATCHING_CAS_DB_ERROR              = "DB_ERROR"               # UPDATE or classification SELECT raised

# P0 #405 — _compensate_watching_signal_if_unchanged outcome constants.
# Expected-state ap_signals compensation: only writes ERROR when the row is
# still exactly WATCHING. Replaces the prior blind _log_signal_to_db upsert.
SIGNAL_COMP_TRANSITIONED       = "SIGNAL_COMP_TRANSITIONED"      # 1 row updated to ERROR
SIGNAL_COMP_ALREADY_ADVANCED   = "SIGNAL_COMP_ALREADY_ADVANCED"  # row advanced past WATCHING; preserved
SIGNAL_COMP_MISSING            = "SIGNAL_COMP_MISSING"           # row not found
SIGNAL_COMP_DB_ERROR           = "SIGNAL_COMP_DB_ERROR"          # update or reread raised

# P0 #405 amendment — _persist_watching_signal_if_eligible outcome constants.
# The INITIAL ap_signals WATCHING establishment is now a guarded read/update/insert,
# NOT a blind upsert. A stale deferral worker must never overwrite a newer advanced
# lifecycle state (queued/triggered/submitted/filled/rejected/ERROR) with WATCHING.
SIGNAL_WATCH_INSERTED         = "SIGNAL_WATCH_INSERTED"          # create-only insert succeeded
SIGNAL_WATCH_TRANSITIONED     = "SIGNAL_WATCH_TRANSITIONED"      # approved pre-deferral state → WATCHING
SIGNAL_WATCH_ALREADY_WATCHING = "SIGNAL_WATCH_ALREADY_WATCHING"  # row already exactly WATCHING (idempotent)
SIGNAL_WATCH_ALREADY_ADVANCED = "SIGNAL_WATCH_ALREADY_ADVANCED"  # row advanced/terminal — preserved untouched
SIGNAL_WATCH_CONFLICT         = "SIGNAL_WATCH_CONFLICT"          # concurrent race left an unexpected state
SIGNAL_WATCH_DB_ERROR         = "SIGNAL_WATCH_DB_ERROR"          # read/update/insert raised, or no unique constraint

# Approved pre-deferral ap_signals statuses that may be TRANSITIONED to WATCHING.
# APSignalStore.insert_signal() defaults decision_status to "received", and
# production can contain received rows before queue deferral ownership is
# established. Treat received as the only known pre-deferral state.
_PRE_DEFERRAL_TRANSITIONABLE_STATUSES: frozenset[str] = frozenset({"received"})

# Explicit non-transitionable ap_signals classifications. Unknown statuses are
# NOT collapsed into advanced; they conflict so new lifecycle words require a
# deliberate policy update before queue deferral can act on them.
_ADVANCED_SIGNAL_STATUSES: frozenset[str] = frozenset({
    "queued", "armed", "triggered", "submitted", "executed", "filled",
})
_TERMINAL_SIGNAL_STATUSES: frozenset[str] = frozenset({
    "rejected", "error", "invalidated", "blocked_at_breach", "expired",
    "dropped", "submit_failed", "closed", "context_blocked", "canceled",
    "cancelled", "done",
})

# P0 #405 — _checked_processing_error_cas outcome constants.
# Guarded queue ERROR transition: only writes ERROR when the row is still PROCESSING.
# Replaces the prior unconditional _mark_job(ERROR) on early failure paths inside
# _persist_watching_deferral(), which could overwrite concurrently-advanced rows.
ERROR_CAS_TRANSITIONED         = "ERROR_CAS_TRANSITIONED"         # 1 row updated; now ERROR
ERROR_CAS_ALREADY_ERROR        = "ERROR_CAS_ALREADY_ERROR"        # row was already ERROR
ERROR_CAS_TERMINAL             = "ERROR_CAS_TERMINAL"             # other terminal status; preserved
ERROR_CAS_WATCHING_OR_ADVANCED = "ERROR_CAS_WATCHING_OR_ADVANCED" # WATCHING or other non-PROCESSING state
ERROR_CAS_MISSING              = "ERROR_CAS_MISSING"              # row not found
ERROR_CAS_DB_ERROR             = "ERROR_CAS_DB_ERROR"             # UPDATE or classification SELECT raised

_MANUAL_RESTART_GUARD_BYPASS_ERRORS = frozenset({
    "manual_requeue_after_overnight_reeval_timeout",
    "manual_rescue_current_session",
})

_PAPER_OVERNIGHT_REEVAL_ONLY_ERROR = "after_hours_deferred:awaiting_overnight_reeval"
_VALID_EXECUTION_MODES = frozenset({"PAPER", "LIVE"})

# ─────────────────────────────────────────────────────────────────────────────
# Canonical decision-status constants (P0: queue deferral truth)
# ─────────────────────────────────────────────────────────────────────────────
# ap_signals.decision_status is the value the overnight reeval discovery
# predicate matches on — exactly `.eq("decision_status", "WATCHING")`. Any
# lowercase "watching" written by queue code is invisible to that query, so the
# deferred setup silently never gets re-armed. These constants exist so every
# deferral path spells the discoverable value the SAME way. No queue path may
# write a bare lowercase "watching" for decision_status.
DECISION_REJECTED = "rejected"
DECISION_WATCHING = "WATCHING"
DECISION_QUEUED = "queued"

# Stable reason stamped on trade_queue.last_error when the ap_signals write that
# MUST precede a WATCHING mark fails or cannot be verified. The queue row is
# marked ERROR (never a phantom WATCHING) so the operator sees the failure.
WATCHING_SIGNAL_PERSISTENCE_FAILED = "WATCHING_SIGNAL_PERSISTENCE_FAILED"

# ── PAPER recovery restart-guard classifier ───────────────────────────────────
# Production incident 2026-07-16: APStartupRecovery legitimately reset Jose's
# current-session PAPER WATCHING rows to NEW and stamped recovery_rescue +
# recovery_rescue_ts. The queue restart guard did not recognise that marker, so
# 122 overnight-evaluated signals were terminally rejected as
# restart_guard:overnight_skip after market open.
#
# Root cause: producer-consumer contract mismatch. The recovery producer
# (APStartupRecovery._reseed_watchers) writes the three marker fields
# atomically; the consumer (_manual_restart_guard_bypass_enabled) did not read
# them. This block closes that gap.
#
# Scope: PAPER only. Current Eastern trading date only.
# LIVE remains fail-closed and byte-for-byte delegates to the legacy paths.

_ET = _ZoneInfo("America/New_York")
_MAX_RECOVERY_LOOKBACK_HOURS = 48
_MAX_FUTURE_CLOCK_SKEW = _timedelta(minutes=5)


def _parse_aware_timestamp(value: Any) -> _datetime | None:
    """Parse a timezone-aware ISO-8601 timestamp string.

    Accepts ``+00:00`` and ``Z`` suffixes; rejects naive timestamps.
    Returns a UTC-normalised datetime or None on any failure.
    """
    if isinstance(value, _datetime):
        if value.tzinfo is None:
            return None
        return value.astimezone(_timezone.utc)
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = _datetime.fromisoformat(raw)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(_timezone.utc)


def _is_current_session_paper_recovery(
    *,
    payload: dict | None,
    execution_mode: str | None,
    now: _datetime | None = None,
) -> bool:
    """Return True only for a legitimately proven current-session PAPER recovery row.

    All nine conditions must hold (fail-closed if any one fails):

    1. Runtime execution mode is exactly PAPER (case-insensitive).
    2. ``payload.recovery_rescue`` is the literal boolean ``True``.
    3. ``payload.recovery_rescue_ts`` is present and non-empty.
    4. The timestamp is parseable as ISO-8601.
    5. The timestamp is timezone-aware.
    6. The timestamp belongs to the current Eastern trading date.
    7. The timestamp is not more than five minutes in the future.
    8. ``recovery_rescue_lookback_hours`` is an integer (not a string).
    9. The lookback is between 1 and 48 hours (inclusive).

    LIVE is explicitly rejected in condition 1. Payload-embedded
    ``execution_mode`` fields are ignored; runtime mode is authoritative.
    """
    if str(execution_mode or "").strip().upper() != "PAPER":
        return False
    if not isinstance(payload, dict):
        return False
    if payload.get("recovery_rescue") is not True:
        return False

    rescued_at_utc = _parse_aware_timestamp(payload.get("recovery_rescue_ts"))
    if rescued_at_utc is None:
        return False

    raw_lookback = payload.get("recovery_rescue_lookback_hours")
    if not isinstance(raw_lookback, int) or isinstance(raw_lookback, bool):
        return False
    if not 1 <= raw_lookback <= _MAX_RECOVERY_LOOKBACK_HOURS:
        return False

    now_utc = (now or _datetime.now(_timezone.utc)).astimezone(_timezone.utc)
    if rescued_at_utc > now_utc + _MAX_FUTURE_CLOCK_SKEW:
        return False

    return rescued_at_utc.astimezone(_ET).date() == now_utc.astimezone(_ET).date()


# ─────────────────────────────────────────────────────────────────────────────
# PR #233 — Queue side fail-closed + breach diagnostics hardening
#
# Migrated in-place from the `ap/queue/__init__.py` package-shadow.  All six
# behaviors below previously lived in the shim; this module is now the single
# source of truth.  No new behavior beyond shim parity.
# ─────────────────────────────────────────────────────────────────────────────

# Side aliases recognized by enqueue_signal and _dispatch.  Anything outside
# {"CALL", "PUT"} after alias resolution is treated as missing/invalid and the
# row is fail-closed before Master Control sees it.
_SIDE_ALIASES: dict[str, str] = {
    "BUY":     "CALL",
    "LONG":    "CALL",
    "CALLS":   "CALL",
    "BULL":    "CALL",
    "BULLISH": "CALL",
    "SELL":    "PUT",
    "SHORT":   "PUT",
    "PUTS":    "PUT",
    "BEAR":    "PUT",
    "BEARISH": "PUT",
}

# Keys to merge from a captured selector failure (via get_last_failure()) into
# the queue job's result_json so the operator dashboard can attribute
# rejections without re-querying the selector.
_SELECTOR_FAILURE_RESULT_KEYS: tuple[str, ...] = (
    "queue_reason_code",
    "chain_rows",
    "survivor_count",
    "top_reject_buckets",
    "tradier_status_code",
    "retryable",
    "data_base_url",
    "selector_stage",
)

# Per-job stash for selector failures.  _dispatch writes here when
# contract_selector.select() returns None; _mark_job reads here when the
# terminating result has stage="contract_selection".  Cleared on read.
_selector_failure_by_job: dict[int, dict] = {}


def _normalize_queue_side(payload: dict | None) -> tuple[str | None, str | None]:
    """Canonical queue-level side normalizer.

    Returns (side, error) where exactly one is non-None:
      - (side, None)  when the payload carries a recognizable CALL/PUT.
      - (None, "invalid_or_missing_side:<detail>")  otherwise.

    Callers MUST treat a non-None error as a block condition.  Never substitute
    a default direction.  This is the queue-level twin of
    ap_master_control._normalize_signal_side; future consolidation will merge
    them onto a single shared helper.
    """
    if not isinstance(payload, dict):
        return None, "invalid_or_missing_side:payload_not_dict"
    raw = payload.get("side") or payload.get("direction")
    side = str(raw or "").strip().upper()
    side = _SIDE_ALIASES.get(side, side)
    if side in {"CALL", "PUT"}:
        return side, None
    return None, f"invalid_or_missing_side:{raw!r}"


def _normalize_execution_mode(value: Any) -> str | None:
    mode = str(value or "").strip().upper()
    return mode if mode in _VALID_EXECUTION_MODES else None


def _payload_execution_mode_value(payload: dict | None) -> Any:
    if not isinstance(payload, dict):
        return None
    if str(payload.get("execution_mode") or "").strip():
        return payload.get("execution_mode")
    if str(payload.get("mode") or "").strip():
        return payload.get("mode")
    return None


def _stamp_payload_execution_mode(job_id: int, mode: str) -> None:
    """Durably persist the resolved runtime execution-mode into trade_queue.payload.

    Called exactly once per dispatch when the owning worker resolved a missing
    payload ``execution_mode`` from the runtime master_control authority.  The
    UPDATE is guarded to ``status = 'PROCESSING'`` so a row that has already
    transitioned (WATCHING, terminal) is never retroactively mutated.

    Fail-soft: any DB error is logged but never re-raised into the dispatch
    path.  A missed stamp is caught on restart by the
    ``trade_queue_execution_mode_unresolved`` readiness fence — fail-closed
    behaviour is preserved even if this write does not land.

    Invariants:
    * Writes only a validated canonical mode ('live' or 'paper' — lowercase).
    * Never infers or defaults mode.  Caller must supply the already-resolved
      ``runtime_mode_for_dispatch`` value.
    * Never modifies status, result_json, last_error, or any other column.
    """
    _mode = str(mode or "").strip().lower()
    if _mode not in ("live", "paper"):
        log.error(
            "_stamp_payload_execution_mode: refusing to stamp invalid mode=%r for job_id=%s",
            mode, job_id,
        )
        return
    try:
        def _fn() -> None:
            with _conn()() as c:
                c.execute(
                    """
                    UPDATE trade_queue
                    SET    payload = COALESCE(payload, '{}'::jsonb)
                                     || %s::jsonb
                    WHERE  id     = %s
                      AND  status = 'PROCESSING'
                    """,
                    (_json_dumps({"execution_mode": _mode}), job_id),
                )
        _run_with_retry(_fn)
    except Exception:
        log.exception(
            "_stamp_payload_execution_mode: failed to persist mode=%r for job_id=%s "
            "(non-fatal — row may remain without durable mode on restart).",
            mode, job_id,
        )


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _positive_float_or_none(value: Any) -> float | None:
    try:
        val = float(value)
    except (TypeError, ValueError):
        return None
    return val if val > 0 else None


def _selector_failure_payload(failure: dict | None) -> dict | None:
    """Pick the safe subset of a captured selector failure for result_json merge."""
    if not isinstance(failure, dict):
        return None
    nested = failure.get("selector_failure")
    if isinstance(nested, dict):
        return copy.deepcopy(nested)
    picked = {
        key: copy.deepcopy(failure[key])
        for key in _SELECTOR_FAILURE_RESULT_KEYS
        if key in failure
    }
    return picked or copy.deepcopy(failure)


def _paper_overnight_reeval_only_enabled(*, payload: dict | None = None, execution_mode: str | None = None) -> bool:
    if str(execution_mode or "").strip().upper() != "PAPER":
        return False
    if not isinstance(payload, dict):
        return False
    return bool(payload.get("force_overnight_reeval_only")) and bool(payload.get("do_not_queue_directly"))


def _manual_restart_guard_bypass_enabled(
    *,
    job_last_error: str | None = None,
    job_result: dict | None = None,
    payload: dict | None = None,
    execution_mode: str | None = None,
) -> bool:
    if str(execution_mode or "").strip().upper() != "PAPER":
        return False
    # ── 1. Current-session PAPER recovery (production incident 2026-07-16) ──
    if _is_current_session_paper_recovery(
        payload=payload,
        execution_mode=execution_mode,
    ):
        return True
    # ── 2. Overnight-reeval-only deferred row ────────────────────────────────
    if _paper_overnight_reeval_only_enabled(payload=payload, execution_mode=execution_mode):
        return True
    # ── 3. Manual rescue markers ─────────────────────────────────────────────
    if isinstance(job_result, dict) and bool(job_result.get("manual_rescue")):
        return True
    return str(job_last_error or "").strip() in _MANUAL_RESTART_GUARD_BYPASS_ERRORS


def _derive_last_error(result: dict | None) -> str | None:
    """
    Derive a human-readable last_error string from a result dict.

    PR #120 — Queue Rejection Honesty.

    Rules (applied in order):
      1. result missing or not a dict          → None
      2. strip whitespace; ignore empty strings
      3. stage + reason present                → "{stage}:{reason}"
      4. reason only                           → "{reason}"
      5. stage only                            → "{stage}"
      6. reason_code only (no stage/reason)    → "{reason_code}"
      7. nothing useful                        → None

    Callers must strip their own values before passing; this function
    also strips defensively.  Empty strings are treated as absent.
    Does not raise.  Does not log.
    """
    if not isinstance(result, dict):
        return None

    def _s(key: str) -> str:
        return str(result.get(key) or "").strip()

    stage       = _s("stage")
    reason      = _s("reason")
    reason_code = _s("reason_code")

    if stage and reason:
        return f"{stage}:{reason}"
    if reason:
        return reason
    if stage:
        return stage
    if reason_code and not stage and not reason:
        return reason_code
    return None


def _mark_job(
    job_id: int,
    status: str,
    *,
    result: dict | None = None,
    error: str | None = None,
):
    """
    Update a queue job without falsely finishing non-terminal states.

    WATCHING is intentionally non-terminal: the entry watcher owns the trigger
    and later broker submission path. Marking finished_ts on WATCHING can make
    a live watched job look complete before any broker order fires.

    PR #120 — last_error honesty:
    - Explicit error= always wins.
    - For TERMINAL statuses with no explicit error, derive last_error from
      result_json (stage/reason/reason_code) so REJECTED rows are never null.
    - For NON-TERMINAL statuses with no explicit error, preserve the existing
      last_error in the DB rather than overwriting with NULL.
    """
    terminal = str(status).upper() in _TERMINAL_QUEUE_STATUSES

    # PR #233: when the terminating result is a contract_selection failure,
    # merge any selector failure metadata captured during _dispatch into
    # result_json so the operator dashboard can see chain_rows / survivor_count /
    # top_reject_buckets / tradier_status_code / retryable / data_base_url /
    # selector_stage without re-querying the selector.  Stash lives in
    # _selector_failure_by_job and is cleared on read.
    try:
        if isinstance(result, dict) and str(result.get("stage") or "") == "contract_selection":
            try:
                job_key = int(job_id)
            except Exception:
                job_key = job_id
            captured = _selector_failure_by_job.pop(job_key, None)
            selector_failure = _selector_failure_payload(captured)
            if selector_failure:
                merged = dict(result)
                merged["selector_failure"] = selector_failure
                for key in _SELECTOR_FAILURE_RESULT_KEYS:
                    if key in selector_failure and key not in merged:
                        merged[key] = selector_failure.get(key)
                if "stage" in selector_failure and "selector_stage" not in merged:
                    merged["selector_stage"] = selector_failure.get("stage")
                result = merged
    except Exception:
        # Merge is observability-only — must never block the actual job update.
        pass

    # Derive last_error for terminal statuses when caller did not supply error=.
    # Explicit error always wins.  Non-terminal statuses never derive last_error
    # from result_json — they preserve whatever last_error already exists in DB.
    derived_error: str | None = error
    if terminal and not derived_error:
        derived_error = _derive_last_error(result)

    def _fn():
        with _conn()() as c:
            c.execute(
                """
                UPDATE trade_queue
                SET status=%s,
                    finished_ts=CASE WHEN %s THEN NOW() ELSE finished_ts END,
                    result_json=%s,
                    last_error=CASE
                        WHEN %s IS NOT NULL THEN %s
                        WHEN %s            THEN %s
                        ELSE last_error
                    END
                WHERE id=%s
                """,
                (
                    status,
                    terminal,
                    _json_dumps(result) if result is not None else None,
                    # last_error CASE params:
                    #   param 4+5: explicit/derived error present → write it
                    #   param 6+7: terminal with no error → write NULL (nothing to derive)
                    #   ELSE:      non-terminal, no error → preserve existing last_error
                    derived_error,        # WHEN derived_error IS NOT NULL
                    derived_error,        # THEN derived_error
                    terminal,             # WHEN terminal (and derived_error IS NULL)
                    None,                 # THEN NULL  (terminal with no reason — honest null)
                    job_id,
                ),
            )
    _run_with_retry(_fn)


# ── PR #182: deferred breach failure write-back ───────────────────────────────
# GAP: when contract_selector.select() returns None on a retryable failure,
# the order is kept alive for the next trigger tick — no OSM cleanup runs.
# But nothing writes back to trade_queue.last_error, so the dashboard shows
# PENDING_TRIGGER / DEFERRED⌛ with a blank CONTRACT column forever.
# Operators cannot distinguish "trigger not hit yet" from "trigger fired 12×
# today and selection failed every time" without grepping Render logs.
#
# INVARIANTS:
#   - Does NOT change trade_queue.status (stays PENDING_TRIGGER for retry).
#   - Does NOT call _cleanup_pending_entry_order. Order remains live.
#   - Uses the existing `last_error` TEXT column — no schema migration needed.
#   - WHERE clause excludes terminal rows — safe against concurrent OSM expiry.
#   - Fails silently: must never block the retry loop or terminal cleanup path.
# ─────────────────────────────────────────────────────────────────────────────

def write_breach_last_error(
    queue_id: Optional[int],
    *,
    reason_code: str,
    explanation: str = "",
    attempt: int = 0,
    client_id: str = "",
    ticker: str = "",
    label: str = "BREACH_ENTRY_FAILED",
) -> None:
    """Write a non-terminal breach failure reason to trade_queue.last_error.

    PR #233 (migrated from shim): generalized successor of
    write_deferred_breach_last_error.  Used for execution-core quote /
    confirmation / submit failure diagnostics on rows still in flight (i.e.
    NOT in terminal status — REJECTED / ERROR / SUBMITTED / FILLED /
    CANCELED / CANCELLED / EXPIRED / DONE / ARCHIVED).

    Written value format (human-readable, dashboard-queryable):
        "<label>:<reason_code>:attempt_<N> - <explanation>"

    Args:
        queue_id:     trade_queue.id. If None or 0, no-op.
        reason_code:  canonical reason code (CHEAP_CONTRACT_NO_UPGRADE,
                      NO_CHAIN_DATA, SPREAD_TOO_WIDE, OI_TOO_LOW,
                      QUOTE_FETCH_FAILED, etc.)
        explanation:  human-readable detail (<=400 chars stored).
        attempt:      1-indexed attempt count. 0 = unknown (no suffix).
        client_id:    for logging only. Not written to DB.
        ticker:       for logging only. Not written to DB.
        label:        leading label.  Defaults to "BREACH_ENTRY_FAILED".  The
                      deferred-breach wrapper passes "DEFERRED_BREACH_CONTRACT_FAILED"
                      to preserve PR #182's historical label.
    """
    if not queue_id:
        return
    try:
        rc = str(reason_code or "BREACH_ENTRY_FAILED").strip()
        event_label = str(label or "BREACH_ENTRY_FAILED").strip() or "BREACH_ENTRY_FAILED"
        full_label = f"{event_label}:{rc}"
        if attempt > 0:
            full_label = f"{full_label}:attempt_{attempt}"
        detail = str(explanation or "").strip()[:400]
        last_error = f"{full_label} - {detail}" if detail else full_label

        def _write():
            with _conn()() as c:
                c.execute(
                    """
                    UPDATE public.trade_queue
                       SET last_error = %s
                     WHERE id = %s
                       AND UPPER(COALESCE(status, '')) NOT IN (
                           'REJECTED', 'ERROR', 'SUBMITTED', 'FILLED',
                           'CANCELED', 'CANCELLED', 'EXPIRED', 'DONE',
                           'ARCHIVED'
                       )
                    """,
                    (last_error, int(queue_id)),
                )

        _run_with_retry(_write)
        log.info(
            "[%s] breach_last_error_written ticker=%s queue_id=%s "
            "label=%s reason=%s attempt=%d",
            client_id or "?",
            ticker or "?",
            queue_id,
            event_label,
            rc,
            attempt,
        )
    except Exception as exc:
        # Non-critical: a write failure here MUST NOT propagate.
        # The retry loop and terminal cleanup path must continue regardless.
        log.debug(
            "[%s] write_breach_last_error failed (non-critical): %s",
            client_id or "?",
            exc,
        )


def write_deferred_breach_last_error(
    queue_id: Optional[int],
    *,
    reason_code: str,
    explanation: str = "",
    attempt: int = 0,
    client_id: str = "",
    ticker: str = "",
) -> None:
    """Backward-compatible wrapper.  Existing deferred-breach callers unchanged.

    PR #233: the body of this function moved into write_breach_last_error(),
    parameterized on `label`.  The DEFERRED_BREACH_CONTRACT_FAILED label is
    preserved verbatim so PR #182's selector failure write-back rows continue
    to grep identically in operator dashboards.
    """
    return write_breach_last_error(
        queue_id,
        reason_code=reason_code or "BREACH_SELECTOR_RETURNED_NONE",
        explanation=explanation,
        attempt=attempt,
        client_id=client_id,
        ticker=ticker,
        label="DEFERRED_BREACH_CONTRACT_FAILED",
    )


def _claim_one_job(client_id: str = "default") -> Optional[dict]:
    """
    Atomically claim one NEW job for this specific client_id.
    Uses a single CTE with FOR UPDATE SKIP LOCKED — no race conditions.
    Also reclaims stale PROCESSING jobs in the same transaction.
    """
    def _atomic_claim():
        with _conn()() as c:
            c.execute(
                """
                UPDATE trade_queue
                SET status      = 'NEW',
                    started_ts  = NULL,
                    last_error  = COALESCE(last_error,'') || ' | reclaimed_stale'
                WHERE status    = 'PROCESSING'
                  AND client_id = %s
                  AND started_ts IS NOT NULL
                  AND started_ts < NOW() - (%s || ' seconds')::interval
                """,
                (client_id, str(PROCESSING_STALE_SECS)),
            )
            # AUDIT PHASE-2: score-based admission.
            # Previously: ORDER BY created_ts ASC — strict FIFO. First-N-wins.
            # A low-quality early signal (low-liquidity ticker, score 72) burned
            # a slot that a higher-scored later signal (AAPL 92) needed.
            #
            # Now: ORDER BY score DESC, created_ts ASC. Best-by-score wins,
            # with arrival time as the tiebreaker. Score lives in the JSONB
            # payload (set by enqueue_signal; defaults to 65 if absent).
            #
            # BLOCKER-1 FIX (post-review): the previous
            #   COALESCE(NULLIF(payload->>'score','')::numeric, 65)
            # crashes Postgres on malformed scores like 'A+' because the cast
            # runs BEFORE COALESCE sees the result. A single bad score from any
            # scanner would kill the claim worker, which would hang the queue.
            #
            # Defensive: regex-guard the cast. The ~ operator returns false on
            # non-numeric strings, so we route those to the default value (65)
            # without ever attempting the cast.
            #
            # Regex: optional minus, digits, optional decimal fraction. No exponent,
            # no whitespace — scores live in a 0-100 range so we don't need them.
            c.execute(
                r"""
                WITH next_job AS (
                    SELECT id
                    FROM   trade_queue
                    WHERE  status    = 'NEW'
                      AND  client_id = %s
                    ORDER BY
                        CASE
                            WHEN payload ? 'score'
                             AND payload->>'score' ~ '^-?[0-9]+(\.[0-9]+)?$'
                            THEN (payload->>'score')::numeric
                            ELSE 65
                        END DESC,
                        created_ts ASC
                    LIMIT  1
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE trade_queue tq
                SET    status     = 'PROCESSING',
                       started_ts = NOW()
                FROM   next_job
                WHERE  tq.id = next_job.id
                RETURNING tq.id, tq.client_id, tq.signal_id, tq.payload, tq.last_error, tq.result_json
                """,
                (client_id,),
            )
            return c.fetchone()

    return _run_with_retry(_atomic_claim)


# =============================================================================
# DISPATCH -- master control path
# =============================================================================

# Module-level Supabase client singleton — initialized once, reused across all rejection logs.
_sb_client_singleton = None

def _get_sb_client():
    """Lazy-init module-level Supabase client. Returns None if env vars not set."""
    global _sb_client_singleton
    if _sb_client_singleton is None:
        try:
            import os as _os
            from supabase import create_client as _cc
            _url = _os.getenv("SUPABASE_URL", "")
            _key = _os.getenv("SUPABASE_SERVICE_KEY", "")
            if _url and _key:
                _sb_client_singleton = _cc(_url, _key)
        except Exception:
            pass
    return _sb_client_singleton


def _build_ap_signal_decision_row(
    *,
    signal_id: str,
    client_id: str,
    ticker: str,
    side: str,
    score: float,
    stage: str,
    reason_code: str,
    human_reason: str,
    payload: dict,
    decision_status: str,
    queued_at: str | None = None,
) -> dict:
    """Pure builder for an ap_signals decision row.

    Extracted (P0 #405 amendment) so BOTH the legacy upsert path
    (`_log_signal_to_db`) and the new guarded WATCHING helper
    (`_persist_watching_signal_if_eligible`) construct the exact same canonical
    row shape — no field drift between the two write paths.

    Never raises on malformed input:
      * a missing signal_id is replaced with a fresh uuid;
      * a malformed score (e.g. "A+") is coerced via _safe_float;
      * an invalid side is normalized to "UNKNOWN" with side_validation_error
        stamped into raw_payload.

    Does NOT touch the database — callers own the write semantics (idempotent
    upsert vs. guarded create-only insert / expected-state update).
    """
    import uuid as _uuid

    _status = str(decision_status or "rejected").strip() or "rejected"

    # PR #233: writing "CALL" for a missing/invalid side here used to
    # contaminate ap_signals diagnostics with a phantom direction.  We now
    # write "UNKNOWN" on invalid and surface the validation error inside
    # raw_payload so operators can grep on side_validation_error to find
    # malformed scanner output.
    _payload_for_row = dict(payload or {})
    _normalized_side, _side_error = _normalize_queue_side({
        "side":      side or _payload_for_row.get("side"),
        "direction": _payload_for_row.get("direction"),
    })
    _row_side = _normalized_side or "UNKNOWN"
    if _side_error:
        _payload_for_row["side_validation_error"] = _side_error
        _payload_for_row.pop("side", None)
        _payload_for_row.pop("direction", None)

    _row: dict = {
        "signal_id":       str(signal_id or _uuid.uuid4()),
        "client_email":    canonical_client_email(client_id),
        "system_version":  "v2",
        "ticker":          str(ticker),
        "side":            _row_side,
        # Malformed scanner scores (e.g. "A+") must never raise inside the
        # row build — _safe_float coerces bad input to the default rather
        # than throwing (the caller relies on the True/False return, not an
        # exception, to make its fail-closed decision).
        "score":           _safe_float(
            score if score not in (None, "") else _payload_for_row.get("score"),
            0.0,
        ),
        "tier":            str(_payload_for_row.get("tier") or "B"),
        "pattern":         str(_payload_for_row.get("pattern") or ""),
        "timeframe":       str(_payload_for_row.get("timeframe") or "1d"),
        "decision_status": _status,
        "context_notes":   f"stage={stage} | code={reason_code} | {human_reason}",
        "raw_payload": {
            "stage": stage,
            "reason_code": reason_code,
            "human_reason": human_reason,
            **{k: v for k, v in _payload_for_row.items()
               if k not in ("raw_payload", "signal_payload") and not callable(v)},
        },
    }

    # Preserve trigger/level data when present
    for _src_key, _dst_key in (
        ("entry_trigger",    "entry_trigger"),
        ("entry_price",      "entry_trigger"),
        ("stop_price",       "stop_price"),
        ("stop_underlying",  "stop_price"),
        ("target_price",     "target_price"),
        ("underlying_at_signal", "underlying_at_signal"),
        ("underlying_price", "underlying_at_signal"),
    ):
        try:
            val = _payload_for_row.get(_src_key)
            if val is not None and float(val) > 0 and _dst_key not in _row:
                _row[_dst_key] = float(val)
        except Exception:
            pass

    if queued_at:
        _row["queued_at"] = queued_at

    return _row


def _log_signal_to_db(
    signal_id: str,
    client_id: str,
    ticker: str,
    side: str,
    score: float,
    stage: str,
    reason_code: str,
    human_reason: str,
    payload: dict,
    *,
    decision_status: str = "rejected",
    queued_at: str | None = None,
) -> bool:
    """
    Write a structured signal decision to ap_signals.

    - True rejections:  decision_status='rejected'  (default, backward-compatible)
    - After-hours defer:decision_status='WATCHING'   so overnight_reeval can find them

    Returns:
        True  -- row successfully upserted to ap_signals.
        False -- supabase client unavailable OR upsert raised. Caller (especially
                 the after-hours WATCHING flow) must NOT mark the queue job
                 WATCHING if this returns False, otherwise the operator sees a
                 WATCHING queue job with no matching ap_signals row, which
                 overnight_reeval can never find. (PR F / queue truth hardening.)

    This fixes the bug where market_closed_deferred signals were logged as rejected
    making them invisible to overnight_reeval which queries ap_signals WHERE
    decision_status='WATCHING'.

    NOTE (P0 #405 amendment): the WATCHING lifecycle establishment inside
    `_persist_watching_deferral()` no longer flows through this unconditional
    upsert. It uses `_persist_watching_signal_if_eligible()` which performs a
    guarded read/update/insert so a stale deferral worker can never overwrite a
    newer advanced lifecycle state with WATCHING. This function retains the
    legacy idempotent upsert semantics for ordinary rejection diagnostics and
    for the terminal/failure compensation paths that intentionally overwrite.
    """
    try:
        _sbc = _get_sb_client()
        if _sbc is None:
            return False

        _row = _build_ap_signal_decision_row(
            signal_id=signal_id,
            client_id=client_id,
            ticker=ticker,
            side=side,
            score=score,
            stage=stage,
            reason_code=reason_code,
            human_reason=human_reason,
            payload=payload,
            decision_status=decision_status,
            queued_at=queued_at,
        )

        upsert_ap_signal_row_with_fallback(_sbc, _row)
        return True
    except Exception as _rlog_exc:
        log.debug("_log_signal_to_db failed (non-fatal): %s", _rlog_exc)
        return False


def _log_rejection_to_db(
    signal_id: str,
    client_id: str,
    ticker: str,
    side: str,
    score: float,
    stage: str,
    reason_code: str,
    human_reason: str,
    payload: dict,
) -> None:
    """Backward-compatible wrapper — all existing rejection calls unchanged."""
    _log_signal_to_db(
        signal_id=signal_id, client_id=client_id, ticker=ticker,
        side=side, score=score, stage=stage,
        reason_code=reason_code, human_reason=human_reason,
        payload=payload, decision_status=DECISION_REJECTED,
    )


def _compensate_watching_signal_if_unchanged(
    *,
    signal_id: str,
    client_email: str,
    watching_job_id: int,
    context_notes: str,
) -> str:
    """Expected-state compensation: update ap_signals.decision_status to ERROR
    only if the row is still exactly WATCHING for this
    (signal_id, client_email, watching_job_id) owner.

    P0 #405 — replaces the prior blind _log_signal_to_db(decision_status='ERROR')
    upsert that could overwrite any later valid lifecycle state (e.g. queued,
    triggered, submitted, filled, rejected) with ERROR.

    Contract:
      1. Uses an expected-state UPDATE … WHERE decision_status = 'WATCHING'
         AND raw_payload.watching_job_id matches the queue job that established
         the row. Status alone is not ownership proof.
      2. Never upserts; never touches a row that advanced past WATCHING.
      3. On zero rows updated: rerereads the row and logs the observed state.
      4. Returns an explicit outcome constant — never raises.
      5. Only touches ap_signals; never touches trade_queue, OSM, orders,
         positions, exits, proof_trades, or any other table.

    Returns:
      SIGNAL_COMP_TRANSITIONED     – exactly 1 row updated to ERROR.
      SIGNAL_COMP_ALREADY_ADVANCED – 0 rows updated; row advanced past WATCHING (state preserved).
      SIGNAL_COMP_MISSING          – 0 rows updated; no row found for (signal_id, client_email).
      SIGNAL_COMP_DB_ERROR         – update or re-read raised; Supabase client unavailable.
    """
    try:
        _sb = _get_sb_client()
        if _sb is None:
            log.critical(
                "_compensate_watching_signal_if_unchanged: no Supabase client — "
                "cannot compensate signal_id=%s client_email=%s",
                signal_id, client_email,
            )
            return SIGNAL_COMP_DB_ERROR

        resp = (
            _sb.table("ap_signals")
            .update({
                "decision_status": "ERROR",
                "context_notes": context_notes,
            })
            .eq("signal_id", signal_id)
            .eq("client_email", client_email)
            .eq("decision_status", "WATCHING")
            .eq("raw_payload->>watching_job_id", str(watching_job_id))
            .execute()
        )

        rows_updated = getattr(resp, "data", None) or []
        if len(rows_updated) == 1:
            log.info(
                "_compensate_watching_signal_if_unchanged: TRANSITIONED "
                "signal_id=%s client_email=%s → ERROR",
                signal_id, client_email,
            )
            return SIGNAL_COMP_TRANSITIONED

        # Zero rows updated — classify without mutating anything further.
        reread = (
            _sb.table("ap_signals")
            .select("decision_status,raw_payload")
            .eq("signal_id", signal_id)
            .eq("client_email", client_email)
            .execute()
        )
        existing = getattr(reread, "data", None) or []
        if not existing:
            log.warning(
                "_compensate_watching_signal_if_unchanged: zero-row update AND no "
                "row found for signal_id=%s client_email=%s — MISSING.",
                signal_id, client_email,
            )
            return SIGNAL_COMP_MISSING

        observed_status = existing[0].get("decision_status", "<unknown>")
        observed_payload = existing[0].get("raw_payload")
        observed_job_id = (
            observed_payload.get("watching_job_id")
            if isinstance(observed_payload, dict) else None
        )
        log.warning(
            "_compensate_watching_signal_if_unchanged: zero-row update for "
            "signal_id=%s client_email=%s — observed decision_status=%r "
            "observed_watching_job_id=%r requested_watching_job_id=%r "
            "(row advanced or is owned by another queue job; preserved).",
            signal_id, client_email, observed_status, observed_job_id,
            watching_job_id,
        )
        return SIGNAL_COMP_ALREADY_ADVANCED

    except Exception:
        log.critical(
            "_compensate_watching_signal_if_unchanged raised for "
            "signal_id=%s client_email=%s — returning SIGNAL_COMP_DB_ERROR",
            signal_id, client_email,
            exc_info=True,
        )
        return SIGNAL_COMP_DB_ERROR


def _is_unique_violation(exc: Exception) -> bool:
    """Best-effort detection of a composite-key uniqueness conflict.

    The ap_signals writes go through the Supabase (PostgREST) client, which
    surfaces a duplicate-key insert as an APIError carrying Postgres code 23505.
    We also recognize psycopg2's UniqueViolation for defensiveness. Anything we
    cannot positively identify as a uniqueness conflict is treated as NOT a
    unique violation, so the caller fails closed (SIGNAL_WATCH_DB_ERROR) rather
    than falling back to an unsafe upsert.
    """
    # Structured attributes first (PostgREST APIError exposes .code; some SDK
    # versions nest it under .args[0] as a dict).
    code = getattr(exc, "code", None)
    if code in ("23505", 23505):
        return True
    try:
        import psycopg2  # type: ignore
        from psycopg2 import errors as _pg_errors  # type: ignore
        if isinstance(exc, _pg_errors.UniqueViolation):
            return True
        if getattr(exc, "pgcode", None) == "23505":
            return True
    except Exception:
        pass
    # Fall back to a conservative message scan for the canonical Postgres text.
    msg = str(getattr(exc, "message", "") or exc).lower()
    return "23505" in msg or "duplicate key value" in msg or "unique constraint" in msg


def _classify_existing_signal_status(observed_status: str | None) -> str:
    """Map a re-read ap_signals.decision_status to a guarded WATCHING outcome.

    Known advanced/terminal statuses are preserved as already advanced.
    Transitionable or unknown statuses conflict on a race reread so they cannot
    be mislabeled as advanced. The initial guarded read path owns the actual
    received -> WATCHING expected-state update.
    """
    if observed_status is None or str(observed_status).strip() == "":
        return SIGNAL_WATCH_CONFLICT
    _status = str(observed_status).strip()
    if _status == DECISION_WATCHING:
        return SIGNAL_WATCH_ALREADY_WATCHING
    _normalized = _status.lower()
    if (
        _normalized in _ADVANCED_SIGNAL_STATUSES
        or _normalized in _TERMINAL_SIGNAL_STATUSES
    ):
        return SIGNAL_WATCH_ALREADY_ADVANCED
    return SIGNAL_WATCH_CONFLICT


def _classify_existing_watching_identity(
    *,
    row: dict,
    requested_execution_mode: str,
    requested_ticker: str,
    requested_side: str,
    requested_watching_job_id: int,
) -> str:
    """Decide whether an existing ap_signals row may be accepted as the SAME
    WATCHING lifecycle owner as the requested deferral.

    P0 #405 amendment — a matching (signal_id, client_email) is NOT sufficient
    for idempotency. A PAPER WATCHING row must not be accepted by a LIVE
    deferral, a CALL WATCHING row must not be accepted by a PUT deferral, and a
    SPY WATCHING row must not be accepted by a different-ticker or queue-job
    deferral merely because the signal_id was reused or malformed upstream.

    Returns ONLY:
      SIGNAL_WATCH_ALREADY_WATCHING – exact mode+ticker+side+job identity match.
      SIGNAL_WATCH_CONFLICT         – WATCHING but identity differs / provenance missing.
      SIGNAL_WATCH_ALREADY_ADVANCED – row is not exactly WATCHING (advanced/terminal).

    Never mutates the row. Never backfills missing provenance. Missing existing
    execution mode is treated as a conflict, NOT a match (no PAPER/LIVE assumption).
    """
    status = str(row.get("decision_status") or "")
    if status != "WATCHING":
        return SIGNAL_WATCH_ALREADY_ADVANCED

    raw_payload = row.get("raw_payload")
    if not isinstance(raw_payload, dict):
        # Missing provenance — cannot prove the existing row is the same mode.
        existing_mode = ""
        _existing_direction = None
        existing_watching_job_id = None
    else:
        existing_mode = str(raw_payload.get("execution_mode") or "").strip().lower()
        _existing_direction = raw_payload.get("direction")
        existing_watching_job_id = raw_payload.get("watching_job_id")
        if existing_watching_job_id is None:
            # Backward-compatible ownership proof for WATCHING rows written by
            # #398 before #405 introduced the explicit field.
            existing_watching_job_id = raw_payload.get("_queue_id")

    existing_ticker = str(row.get("ticker") or "").strip().upper()

    _existing_side, _existing_side_err = _normalize_queue_side({
        "side": row.get("side"),
        "direction": _existing_direction,
    })
    existing_side = _existing_side or ""

    if (
        existing_mode == requested_execution_mode
        and existing_ticker == requested_ticker
        and existing_side == requested_side
        and str(existing_watching_job_id) == str(requested_watching_job_id)
    ):
        return SIGNAL_WATCH_ALREADY_WATCHING

    return SIGNAL_WATCH_CONFLICT


def _persist_watching_signal_if_eligible(
    *,
    signal_id: str,
    client_email: str,
    job_id: int,
    execution_mode: str,
    ticker: str,
    side: str,
    score: float,
    stage: str,
    reason_code: str,
    human_reason: str,
    payload: dict,
    queued_at: str,
) -> str:
    """Guarded establishment of the ap_signals WATCHING state.

    P0 #405 amendment — replaces the prior unconditional
    `_log_signal_to_db(... decision_status=WATCHING)` initial write inside
    `_persist_watching_deferral()`. A stale deferral worker must never overwrite
    a newer advanced lifecycle state (queued/triggered/submitted/filled/rejected/
    ERROR/blocked_at_breach) with WATCHING.

    Owns ONLY ap_signals for this exact (signal_id, client_email). It accepts the
    queue job_id solely as an ownership token; it never touches trade_queue,
    OSM, orders, positions, exits, proof_trades, or any other table.

    Allowed ways to establish WATCHING:
      1. No (signal_id, client_email) row exists → create-only insert.
      2. The existing row is already WATCHING with EXACT identity match on
         execution_mode + ticker + side + queue job → idempotent no-op. A
         WATCHING row whose identity differs is a SIGNAL_WATCH_CONFLICT, not
         idempotent success.
      3. The existing row is in an approved pre-deferral status
         (`_PRE_DEFERRAL_TRANSITIONABLE_STATUSES`, currently "received") →
         expected-state update.

    Never falls back to the legacy single-key upsert: a fallback that could
    overwrite another client's row or an advanced lifecycle state would defeat
    the entire correction. If the composite uniqueness constraint is unavailable
    (create-only insert cannot be enforced), returns SIGNAL_WATCH_DB_ERROR and
    logs loudly rather than failing open to an unsafe write.

    Returns one of the SIGNAL_WATCH_* outcome constants. Never raises.
    """
    _client = str(client_email or "")
    _sig = str(signal_id or "")
    try:
        _requested_job_id = int(job_id)
    except (TypeError, ValueError):
        _requested_job_id = 0
    if _requested_job_id <= 0:
        log.critical(
            "WATCHING_IDENTITY_CONFLICT_INVALID_REQUEST_JOB signal_id=%s "
            "client_email=%s requested_job_id=%r — refusing WATCHING.",
            _sig, _client, job_id,
        )
        return SIGNAL_WATCH_CONFLICT

    # ── Validate the REQUESTED identity BEFORE any DB work ────────────────────
    # The explicit execution_mode argument is authoritative — not mutable payload.
    _requested_mode = str(execution_mode or "").strip().lower()
    if _requested_mode not in {"paper", "live"}:
        log.critical(
            "WATCHING_IDENTITY_CONFLICT_INVALID_REQUEST_MODE signal_id=%s "
            "client_email=%s requested_mode=%r — refusing to establish WATCHING.",
            _sig, _client, execution_mode,
        )
        return SIGNAL_WATCH_CONFLICT

    _requested_ticker = str(ticker or "").strip().upper()
    if not _requested_ticker:
        log.critical(
            "WATCHING_IDENTITY_CONFLICT_INVALID_REQUEST_TICKER signal_id=%s "
            "client_email=%s ticker=%r — refusing to establish WATCHING.",
            _sig, _client, ticker,
        )
        return SIGNAL_WATCH_CONFLICT

    _requested_side, _side_error = _normalize_queue_side({
        "side": side,
        "direction": (payload or {}).get("direction"),
    })
    if not _requested_side:
        log.critical(
            "WATCHING_IDENTITY_CONFLICT_INVALID_REQUEST_SIDE signal_id=%s "
            "client_email=%s side=%r err=%r — refusing to establish WATCHING "
            "(no CALL fallback, no UNKNOWN identity).",
            _sig, _client, side, _side_error,
        )
        return SIGNAL_WATCH_CONFLICT

    try:
        _sbc = _get_sb_client()
        if _sbc is None:
            log.critical(
                "_persist_watching_signal_if_eligible: no Supabase client — "
                "cannot safely establish WATCHING for signal_id=%s client_email=%s",
                _sig, _client,
            )
            return SIGNAL_WATCH_DB_ERROR

        # Build the canonical WATCHING row via the shared builder so there is no
        # field drift versus _log_signal_to_db.
        _watching_row = _build_ap_signal_decision_row(
            signal_id=_sig,
            client_id=_client,
            ticker=ticker,
            side=side,
            score=score,
            stage=stage,
            reason_code=reason_code,
            human_reason=human_reason,
            payload=payload,
            decision_status=DECISION_WATCHING,
            queued_at=queued_at,
        )
        _watching_row["raw_payload"]["watching_job_id"] = _requested_job_id
        # canonical_client_email may normalize the address — read back the exact
        # value the row will carry so every .eq() filter matches precisely.
        _client = str(_watching_row.get("client_email") or _client)
        _sig = str(_watching_row.get("signal_id") or _sig)

        def _reread_and_classify() -> str:
            """Reread the (signal_id, client_email) row after a race and classify.

            Uses the SINGLE pure identity helper so the WATCHING identity
            comparison is never duplicated across the four race sites:
              * no row               → SIGNAL_WATCH_CONFLICT (row vanished mid-race)
              * WATCHING row         → _classify_existing_watching_identity(...)
                                       (exact mode+ticker+side+job match required)
              * known advanced/terminal status → SIGNAL_WATCH_ALREADY_ADVANCED
              * transitionable/unknown status  → SIGNAL_WATCH_CONFLICT
            """
            _rr = (
                _sbc.table("ap_signals")
                .select("decision_status,raw_payload,ticker,side")
                .eq("signal_id", _sig)
                .eq("client_email", _client)
                .execute()
            )
            _rr_rows = getattr(_rr, "data", None) or []
            if not _rr_rows:
                return SIGNAL_WATCH_CONFLICT
            _rr_row = _rr_rows[0]
            _rr_status = str(_rr_row.get("decision_status") or "")
            if _rr_status == DECISION_WATCHING:
                return _classify_existing_watching_identity(
                    row=_rr_row,
                    requested_execution_mode=_requested_mode,
                    requested_ticker=_requested_ticker,
                    requested_side=_requested_side,
                    requested_watching_job_id=_requested_job_id,
                )
            return _classify_existing_signal_status(_rr_status)

        # ── (1) read the exact existing row (with identity fields) ───────────
        _existing = (
            _sbc.table("ap_signals")
            .select("decision_status,raw_payload,ticker,side")
            .eq("signal_id", _sig)
            .eq("client_email", _client)
            .execute()
        )
        _rows = getattr(_existing, "data", None) or []

        if _rows:
            _existing_row = _rows[0]
            _observed = str(_existing_row.get("decision_status") or "")

            # Already WATCHING → require exact mode+ticker+side+job identity.
            # A PAPER row must not be accepted by a LIVE deferral, a CALL row by a
            # PUT deferral, or a SPY row by a different-ticker deferral.
            if _observed == DECISION_WATCHING:
                _identity_outcome = _classify_existing_watching_identity(
                    row=_existing_row,
                    requested_execution_mode=_requested_mode,
                    requested_ticker=_requested_ticker,
                    requested_side=_requested_side,
                    requested_watching_job_id=_requested_job_id,
                )
                if _identity_outcome == SIGNAL_WATCH_ALREADY_WATCHING:
                    log.info(
                        "_persist_watching_signal_if_eligible: already WATCHING "
                        "(exact identity match) signal_id=%s client_email=%s "
                        "mode=%s ticker=%s side=%s job_id=%s (idempotent).",
                        _sig, _client, _requested_mode, _requested_ticker, _requested_side,
                        _requested_job_id,
                    )
                else:
                    log.critical(
                        "WATCHING_IDENTITY_CONFLICT signal_id=%s client_email=%s — "
                        "requested(mode=%s ticker=%s side=%s job_id=%s) vs "
                        "observed(mode=%r ticker=%r side=%r job_id=%r) → %s. Existing WATCHING "
                        "row preserved untouched.",
                        _sig, _client,
                        _requested_mode, _requested_ticker, _requested_side, _requested_job_id,
                        (_existing_row.get("raw_payload") or {}).get("execution_mode")
                        if isinstance(_existing_row.get("raw_payload"), dict) else None,
                        _existing_row.get("ticker"), _existing_row.get("side"),
                        (_existing_row.get("raw_payload") or {}).get("watching_job_id")
                        if isinstance(_existing_row.get("raw_payload"), dict) else None,
                        _identity_outcome,
                    )
                return _identity_outcome

            # Explicitly classify non-transitionable statuses. Known advanced
            # and terminal states are preserved; unknown statuses conflict so
            # schema/status drift cannot silently masquerade as advancement.
            if _observed not in _PRE_DEFERRAL_TRANSITIONABLE_STATUSES:
                _classified = _classify_existing_signal_status(_observed)
                if _classified == SIGNAL_WATCH_CONFLICT:
                    log.critical(
                        "_persist_watching_signal_if_eligible: existing unknown "
                        "status=%r for signal_id=%s client_email=%s — refusing "
                        "WATCHING (SIGNAL_WATCH_CONFLICT).",
                        _observed, _sig, _client,
                    )
                    return SIGNAL_WATCH_CONFLICT
                log.warning(
                    "_persist_watching_signal_if_eligible: existing status=%r is not "
                    "transitionable for signal_id=%s client_email=%s — preserving "
                    "advanced/terminal state (SIGNAL_WATCH_ALREADY_ADVANCED).",
                    _observed, _sig, _client,
                )
                return _classified

            # Approved pre-deferral state → expected-state update fenced on the
            # exact observed status (fails to zero rows if it changed under us).
            _upd = (
                _sbc.table("ap_signals")
                .update(_watching_row)
                .eq("signal_id", _sig)
                .eq("client_email", _client)
                .eq("decision_status", _observed)
                .execute()
            )
            _updated = getattr(_upd, "data", None) or []
            if len(_updated) == 1:
                log.info(
                    "_persist_watching_signal_if_eligible: TRANSITIONED %r→WATCHING "
                    "signal_id=%s client_email=%s.",
                    _observed, _sig, _client,
                )
                return SIGNAL_WATCH_TRANSITIONED

            # Zero rows updated — the row changed between read and update. Reread
            # and classify (identity-aware) without mutating anything.
            _outcome = _reread_and_classify()
            log.warning(
                "_persist_watching_signal_if_eligible: expected-state update race "
                "signal_id=%s client_email=%s expected=%r → %s",
                _sig, _client, _observed, _outcome,
            )
            return _outcome

        # ── (2) missing row → create-only insert ─────────────────────────────
        # A blind upsert here could overwrite a row created concurrently by a
        # newer worker. We require a create-only insert that fails on the
        # composite-key uniqueness conflict; on conflict we reread and preserve.
        try:
            _ins = (
                _sbc.table("ap_signals")
                .insert(_watching_row)
                .execute()
            )
        except Exception as _ins_exc:
            # A uniqueness conflict means another worker inserted the row first.
            # Reread and classify. Any OTHER insert failure (e.g. the composite
            # uniqueness constraint is unavailable, so create-only cannot be
            # enforced) must NOT fall back to an unsafe upsert.
            if _is_unique_violation(_ins_exc):
                _outcome = _reread_and_classify()
                log.warning(
                    "_persist_watching_signal_if_eligible: insert race "
                    "signal_id=%s client_email=%s → %s",
                    _sig, _client, _outcome,
                )
                return _outcome
            log.critical(
                "_persist_watching_signal_if_eligible: create-only insert failed "
                "for signal_id=%s client_email=%s and the error is NOT a uniqueness "
                "violation — refusing to fall back to an unsafe upsert. err=%s",
                _sig, _client, _ins_exc,
            )
            return SIGNAL_WATCH_DB_ERROR

        _ins_rows = getattr(_ins, "data", None) or []
        if _ins_rows:
            log.info(
                "_persist_watching_signal_if_eligible: INSERTED WATCHING "
                "signal_id=%s client_email=%s.",
                _sig, _client,
            )
            return SIGNAL_WATCH_INSERTED

        # Insert returned no rows and did not raise — treat as a conflict and
        # reread (identity-aware) to preserve any newer state rather than
        # assuming success.
        _outcome = _reread_and_classify()
        log.warning(
            "_persist_watching_signal_if_eligible: insert returned no rows "
            "signal_id=%s client_email=%s → %s",
            _sig, _client, _outcome,
        )
        return _outcome

    except Exception:
        log.critical(
            "_persist_watching_signal_if_eligible raised for signal_id=%s "
            "client_email=%s — returning SIGNAL_WATCH_DB_ERROR",
            _sig, _client, exc_info=True,
        )
        return SIGNAL_WATCH_DB_ERROR


def _persist_watching_deferral(
    *,
    job_id: int,
    client_id: str,
    signal_id: str,
    execution_mode: str,
    payload: dict,
    stage: str,
    reason_code: str,
    human_reason: str,
    watching_error: str | None = None,
    watching_result: dict | None = None,
    failure_error: str | None = None,
) -> bool:
    """Single authority for turning an intentional deferral into a durable,
    consistently-discoverable WATCHING row.

    P0 (queue deferral truth): before this existed, more than one queue path
    could defer a setup, and they disagreed. One path even marked the queue row
    REJECTED and then wrote a lowercase ap_signals decision_status="watching" —
    split truth the overnight reeval could not find (it discovers via
    `.eq("decision_status", "WATCHING")`). Every legitimate deferral now routes
    through this one function so trade_queue.status and ap_signals.decision_status
    always tell the SAME story.

    Contract:
      1. Requires an exact canonical client identity (rejects ownerless rows).
      2. Requires a valid execution mode of exactly "paper" or "live".
      3. Requires a canonical signal identity.
      4. Persists/confirms the ap_signals row FIRST with
         decision_status="WATCHING" (exact uppercase), the canonical client
         identity, the execution mode, the original signal identity, a truthful
         deferral reason, and the score/tier/direction/timeframe/trigger/stop/
         target fields already present on the payload.
      5. Confirms the signal write succeeded.
      6. Only THEN marks the exact queue row WATCHING.
      7. On signal-persistence failure (or unverifiable write): marks the exact
         queue row ERROR with a stable reason (WATCHING_SIGNAL_PERSISTENCE_FAILED
         unless the caller supplied a more specific one) — never WATCHING, never
         REJECTED+WATCHING split truth. No watcher / OSM / broker action is taken.
      8. Idempotent only for the same signal, client, mode, ticker, side, and
         queue job. Missing rows use create-only insert; existing WATCHING rows
         are never rewritten.
      9. Preserves CLIENT isolation: the canonical client_email is part of the
         ap_signals upsert key and the queue update is scoped to this job_id
         only, so one client's deferral can never claim/update another client's
         row. Mode, ticker, side, and queue-job ownership must also match before
         an existing WATCHING row is accepted as idempotent.
     10. Never converts a genuinely terminal rejection into WATCHING — it is the
         ONLY WATCHING authority and callers invoke it BEFORE any terminalization,
         so no terminal state is ever overwritten.

    Returns True iff the queue row ended WATCHING (TRANSITIONED or ALREADY_WATCHING).
    Returns False for any of the following outcomes — the specific failure is logged:

      * Signal-persistence failure or unverifiable write → queue marked ERROR.
      * Invalid client, mode, or signal identity → queue marked ERROR.
      * WATCHING_CAS_TERMINAL → queue row left untouched (terminal state is
        authoritative); signal revert attempted.
      * WATCHING_CAS_MISSING_OR_UNEXPECTED or WATCHING_CAS_DB_ERROR → queue row
        not blindly overwritten (current state was not confirmed as PROCESSING);
        signal revert attempted.

    In the TERMINAL / MISSING_OR_UNEXPECTED / DB_ERROR cases the queue row is
    never forced to ERROR — only signal compensation is attempted.
    """
    _fail = failure_error or WATCHING_SIGNAL_PERSISTENCE_FAILED

    # (1) canonical client identity — refuse ownerless deferrals. A WATCHING row
    # with no owner cannot preserve client isolation downstream.
    _client = canonical_client_email(client_id)
    if not str(client_id or "").strip() or _client == "__shared__":
        log.error(
            "DEFERRAL_REFUSED_INVALID_CLIENT job_id=%s signal_id=%s client=%r — "
            "guarded queue ERROR (a WATCHING row must have an exact client owner).",
            job_id, signal_id, client_id,
        )
        _err_cas = _checked_processing_error_cas(job_id, error=f"{_fail}:invalid_client")
        if _err_cas != ERROR_CAS_TRANSITIONED:
            log.warning(
                "DEFERRAL_EARLY_ERROR_CAS outcome=%r job_id=%s (invalid_client) — "
                "queue row was not PROCESSING; state preserved.",
                _err_cas, job_id,
            )
        return False

    # (2) valid execution mode — exactly paper or live.
    _mode = str(execution_mode or "").strip().lower()
    if _mode not in ("paper", "live"):
        log.error(
            "DEFERRAL_REFUSED_INVALID_MODE job_id=%s signal_id=%s mode=%r — "
            "guarded queue ERROR.",
            job_id, signal_id, execution_mode,
        )
        _err_cas = _checked_processing_error_cas(job_id, error=f"{_fail}:invalid_mode")
        if _err_cas != ERROR_CAS_TRANSITIONED:
            log.warning(
                "DEFERRAL_EARLY_ERROR_CAS outcome=%r job_id=%s (invalid_mode) — "
                "queue row was not PROCESSING; state preserved.",
                _err_cas, job_id,
            )
        return False

    # (3) canonical signal identity — MANDATORY. Never invent one: a random
    # uuid would persist a WATCHING row that no longer matches the queue
    # opportunity that produced it. Fail closed to ERROR instead.
    _sig_raw = str(signal_id or (payload or {}).get("signal_id") or "").strip()
    _sig = str(canonical_signal_id(_sig_raw) or "").strip()
    if not _sig:
        log.error(
            "DEFERRAL_REFUSED_INVALID_SIGNAL job_id=%s client=%s mode=%s — "
            "guarded queue ERROR (no canonical signal identity to defer).",
            job_id, _client, _mode,
        )
        _err_cas = _checked_processing_error_cas(job_id, error=f"{_fail}:invalid_signal_id")
        if _err_cas != ERROR_CAS_TRANSITIONED:
            log.warning(
                "DEFERRAL_EARLY_ERROR_CAS outcome=%r job_id=%s (invalid_signal_id) — "
                "queue row was not PROCESSING; state preserved.",
                _err_cas, job_id,
            )
        return False

    # (4)+(5) establish/confirm the ap_signals WATCHING row FIRST — GUARDED.
    # Stamp the execution mode into raw_payload for provenance.
    #
    # P0 #405 AMENDMENT: the initial WATCHING write is NO LONGER an unconditional
    # upsert. It goes through _persist_watching_signal_if_eligible(), which does a
    # guarded read/update/insert so a STALE deferral worker can never overwrite a
    # NEWER advanced lifecycle state (queued/triggered/submitted/filled/rejected/
    # ERROR) with WATCHING. The (signal_id, client_email) key gives CLIENT
    # isolation. Mode, ticker, side, and queue job are checked ownership
    # provenance for an existing WATCHING row; none changes the database key.
    _payload = dict(payload or {})
    _payload["execution_mode"] = _mode
    _payload["signal_id"] = _sig
    # Sanitize score BEFORE building the row: a malformed scanner score (e.g.
    # "A+") must not raise past the fail-closed persistence contract.
    _score = _safe_float(
        _payload.get("score") if _payload.get("score") not in (None, "")
        else _payload.get("ev_score"),
        0.0,
    )
    _payload["score"] = _score
    _signal_watch_outcome = _persist_watching_signal_if_eligible(
        signal_id=_sig,
        client_email=_client,
        job_id=job_id,
        execution_mode=_mode,
        ticker=str(_payload.get("ticker") or _payload.get("symbol") or ""),
        side=str(_payload.get("side") or _payload.get("direction") or ""),
        score=_score,
        stage=stage,
        reason_code=reason_code,
        human_reason=human_reason,
        payload=_payload,
        queued_at=datetime.now(timezone.utc).isoformat(),
    )

    # ── ADVANCED signal: a newer lifecycle state already exists ────────────────
    # The signal row advanced past WATCHING before this (stale) deferral ran.
    # Do NOT run the queue WATCHING CAS. Do NOT compensate the signal to ERROR —
    # the advanced state is authoritative and must be preserved. The still-
    # PROCESSING queue row for THIS stale job is genuinely dead work, so fail it
    # via the guarded ERROR CAS (only mutates if status is still PROCESSING).
    if _signal_watch_outcome == SIGNAL_WATCH_ALREADY_ADVANCED:
        log.warning(
            "DEFERRAL_SIGNAL_ALREADY_ADVANCED job_id=%s signal_id=%s client=%s "
            "mode=%s — ap_signals advanced past WATCHING before this deferral; "
            "preserving advanced signal, failing the stale queue row (guarded).",
            job_id, _sig, _client, _mode,
        )
        _err_cas = _checked_processing_error_cas(
            job_id, error="DEFERRAL_SIGNAL_ALREADY_ADVANCED"
        )
        if _err_cas != ERROR_CAS_TRANSITIONED:
            log.warning(
                "DEFERRAL_EARLY_ERROR_CAS outcome=%r job_id=%s "
                "(signal_already_advanced) — queue row was not PROCESSING; "
                "state preserved.",
                _err_cas, job_id,
            )
        return False

    # ── IDENTITY CONFLICT: existing WATCHING row belongs to a different
    # mode/ticker/side, or the requested identity was invalid, or a race left an
    # unexpected state. Do NOT run the queue WATCHING CAS. Do NOT compensate or
    # mutate the existing conflicting signal row. Fail the still-PROCESSING queue
    # row via the guarded ERROR CAS with a SPECIFIC identity-conflict reason.
    if _signal_watch_outcome == SIGNAL_WATCH_CONFLICT:
        log.critical(
            "WATCHING_SIGNAL_IDENTITY_CONFLICT job_id=%s signal_id=%s client=%s "
            "mode=%s stage=%s reason=%s — existing WATCHING identity differs (or "
            "requested identity invalid); refusing to mark queue WATCHING and "
            "preserving the existing signal row untouched.",
            job_id, _sig, _client, _mode, stage, reason_code,
        )
        _err_cas = _checked_processing_error_cas(
            job_id, error="WATCHING_SIGNAL_IDENTITY_CONFLICT"
        )
        if _err_cas != ERROR_CAS_TRANSITIONED:
            log.warning(
                "DEFERRAL_EARLY_ERROR_CAS outcome=%r job_id=%s "
                "(watching_signal_identity_conflict) — queue row was not "
                "PROCESSING; state preserved.",
                _err_cas, job_id,
            )
        return False

    # ── DB_ERROR: could not safely establish WATCHING (no client, no unique
    # constraint, or an unexpected DB failure). Preserve the existing failure
    # reason. Do NOT run the queue WATCHING CAS. Do NOT blindly overwrite the
    # signal. Guard the queue error transition through _checked_processing_error_cas.
    if _signal_watch_outcome == SIGNAL_WATCH_DB_ERROR:
        log.critical(
            "WATCHING_SIGNAL_PERSISTENCE_FAILED job_id=%s signal_id=%s client=%s "
            "mode=%s stage=%s reason=%s signal_outcome=%r — refusing to mark queue "
            "WATCHING; guarded queue ERROR so the operator sees the failure.",
            job_id, _sig, _client, _mode, stage, reason_code, _signal_watch_outcome,
        )
        _err_cas = _checked_processing_error_cas(job_id, error=_fail)
        if _err_cas != ERROR_CAS_TRANSITIONED:
            log.warning(
                "DEFERRAL_EARLY_ERROR_CAS outcome=%r job_id=%s signal_outcome=%r "
                "(signal_persistence_failure) — queue row was not PROCESSING; "
                "state preserved.",
                _err_cas, job_id, _signal_watch_outcome,
            )
        return False

    # ── SUCCESS: WATCHING established (INSERTED / TRANSITIONED / ALREADY_WATCHING)
    # Only now may the queue WATCHING CAS run.
    # (6) CAS the queue row to WATCHING, confirmed. A bare _mark_job call cannot
    # verify that the queue row actually transitioned — it returns None regardless
    # of rowcount. A failed or zero-row queue write after a successful ap_signals
    # write creates split truth (ap_signals=WATCHING, trade_queue=PROCESSING or
    # ERROR). _checked_watching_cas requires rowcount==1.
    #
    # NOTE: two separate databases (Supabase Postgres for ap_signals, primary
    # Postgres for trade_queue) means perfect atomicity is impossible. What this
    # guarantees is a checked transition plus loud, best-effort compensation on
    # failure. The PR description no longer claims "atomic consistency".
    _cas_outcome = _checked_watching_cas(
        job_id,
        watching_error=watching_error,
        watching_result=watching_result,
    )

    # ── success paths ──────────────────────────────────────────────────────────
    # TRANSITIONED: queue row is now canonical WATCHING.
    # ALREADY_WATCHING: repeated deferral against an already-canonical WATCHING
    #   row is idempotent success — do NOT revert ap_signals, do NOT mark ERROR.
    if _cas_outcome in {WATCHING_CAS_TRANSITIONED, WATCHING_CAS_ALREADY_WATCHING}:
        return True

    # ── terminal queue state — authoritative, must never be overwritten ────────
    if _cas_outcome == WATCHING_CAS_TERMINAL:
        log.critical(
            "DEFERRAL_QUEUE_CAS_TERMINAL job_id=%s signal_id=%s client=%s mode=%s — "
            "queue row is in a terminal state; WATCHING transition refused. "
            "Best-effort ap_signals revert attempted. Terminal queue state preserved.",
            job_id, _sig, _client, _mode,
        )
        # P0 #405: Best-effort expected-state compensation — only transitions the
        # ap_signals row to ERROR if it is still exactly WATCHING. A blind upsert
        # here could overwrite any newer valid lifecycle state (e.g. queued,
        # triggered, submitted, filled) that a concurrent owner may have written.
        _comp_outcome = _compensate_watching_signal_if_unchanged(
            signal_id=_sig,
            client_email=_client,
            watching_job_id=job_id,
            context_notes=(
                f"stage={stage} | DEFERRAL_QUEUE_CAS_TERMINAL | "
                "Queue row is terminal — ap_signals compensation attempted from WATCHING."
            ),
        )
        if _comp_outcome == SIGNAL_COMP_TRANSITIONED:
            log.info(
                "DEFERRAL_SIGNAL_COMP_OK job_id=%s — ap_signals set to ERROR "
                "(was WATCHING; queue row is terminal).",
                job_id,
            )
        elif _comp_outcome == SIGNAL_COMP_ALREADY_ADVANCED:
            log.warning(
                "DEFERRAL_SIGNAL_COMP_ADVANCED job_id=%s — ap_signals row advanced "
                "past WATCHING before compensation; newer state preserved. "
                "Manual review recommended.",
                job_id,
            )
        else:
            log.critical(
                "DEFERRAL_SIGNAL_COMP_FAILED job_id=%s outcome=%r — ap_signals may "
                "remain WATCHING paired with a terminal queue row. "
                "Operator action required.",
                job_id, _comp_outcome,
            )
        # Do NOT call _mark_job — the terminal queue state is authoritative and
        # must remain untouched.
        return False

    # ── genuine failure: MISSING_OR_UNEXPECTED or DB_ERROR ────────────────────
    log.critical(
        "DEFERRAL_QUEUE_CAS_FAILED job_id=%s signal_id=%s client=%s mode=%s "
        "outcome=%r — ap_signals is WATCHING but queue CAS did not confirm a "
        "transition. Best-effort signal revert attempted. Manual reconciliation "
        "required.",
        job_id, _sig, _client, _mode, _cas_outcome,
    )
    # P0 #405: Best-effort expected-state compensation — only transitions the
    # ap_signals row to ERROR if it is still exactly WATCHING. A blind upsert
    # here could overwrite any newer valid lifecycle state that a concurrent
    # owner may have written after we set WATCHING earlier in this call.
    _comp_outcome = _compensate_watching_signal_if_unchanged(
        signal_id=_sig,
        client_email=_client,
        watching_job_id=job_id,
        context_notes=(
            f"stage={stage} | DEFERRAL_QUEUE_CAS_FAILED outcome={_cas_outcome!r} | "
            "ap_signals compensation attempted from WATCHING."
        ),
    )
    if _comp_outcome == SIGNAL_COMP_TRANSITIONED:
        log.info(
            "DEFERRAL_SIGNAL_COMP_OK job_id=%s — ap_signals set to ERROR "
            "(was WATCHING; queue CAS outcome=%r).",
            job_id, _cas_outcome,
        )
    elif _comp_outcome == SIGNAL_COMP_ALREADY_ADVANCED:
        log.warning(
            "DEFERRAL_SIGNAL_COMP_ADVANCED job_id=%s outcome=%r — ap_signals row "
            "advanced past WATCHING before compensation; newer state preserved. "
            "Manual review recommended.",
            job_id, _cas_outcome,
        )
    else:
        log.critical(
            "DEFERRAL_SIGNAL_COMP_FAILED job_id=%s cas_outcome=%r comp_outcome=%r — "
            "ap_signals may remain WATCHING with no matching queue row. "
            "Operator action required.",
            job_id, _cas_outcome, _comp_outcome,
        )
    # Do NOT blindly call _mark_job — the queue row's current state was not
    # confirmed to be PROCESSING. Overwriting WATCHING, a terminal status, or a
    # missing row with ERROR would create or deepen split truth.
    return False


def _checked_processing_error_cas(
    job_id: int,
    *,
    error: str,
    result: dict | None = None,
) -> str:
    """Guarded queue ERROR transition: UPDATE trade_queue SET status='ERROR'
    WHERE id=%s AND status='PROCESSING'.

    P0 #405 — replaces the unconditional _mark_job(job_id, 'ERROR', ...) calls on
    early failure paths inside _persist_watching_deferral(). The prior code updated
    by id only, so a row concurrently advanced from PROCESSING (to WATCHING,
    SUBMITTED, FILLED, REJECTED, EXPIRED, DONE, ERROR, etc.) could be blindly
    overwritten with ERROR.

    Contract:
      1. Only transitions a row that is still exactly PROCESSING.
      2. On zero rows updated: reads current status and classifies the outcome.
      3. Never overwrites WATCHING, SUBMITTED, FILLED, CANCELED, CANCELLED,
         EXPIRED, DONE, REJECTED, or an already-ERROR row.
      4. Returns an explicit outcome constant — never raises.
      5. Only touches trade_queue; never touches ap_signals or any other table.

    Returns:
      ERROR_CAS_TRANSITIONED         – 1 row updated; row is now ERROR.
      ERROR_CAS_ALREADY_ERROR        – 0 rows; row was already ERROR (no-op, benign).
      ERROR_CAS_TERMINAL             – 0 rows; row is in another terminal status (preserved).
      ERROR_CAS_WATCHING_OR_ADVANCED – 0 rows; row advanced to WATCHING or other non-PROCESSING state.
      ERROR_CAS_MISSING              – 0 rows; row not found.
      ERROR_CAS_DB_ERROR             – UPDATE or classification SELECT raised.
    """
    try:
        def _fn() -> tuple:
            with _conn()() as c:
                # 1. Guarded PROCESSING → ERROR transition.
                c.execute(
                    """
                    UPDATE trade_queue
                    SET   status      = 'ERROR',
                          finished_ts = NOW(),
                          last_error  = %s,
                          result_json = COALESCE(result_json, '{}'::jsonb) || %s::jsonb
                    WHERE id     = %s
                      AND status = 'PROCESSING'
                    """,
                    (
                        error,
                        _json_dumps(result or {}),
                        job_id,
                    ),
                )
                if c.rowcount == 1:
                    return (1, None)
                # 2. Zero rows — read current status without mutating anything.
                c.execute(
                    "SELECT status FROM trade_queue WHERE id = %s",
                    (job_id,),
                )
                row = c.fetchone()
                current = str(row["status"]).upper() if row else None
                return (0, current)

        updated, current_status = _run_with_retry(_fn)

        if updated == 1:
            return ERROR_CAS_TRANSITIONED

        if current_status is None:
            log.warning(
                "_checked_processing_error_cas: job_id=%s not found — ERROR_CAS_MISSING",
                job_id,
            )
            return ERROR_CAS_MISSING
        if current_status == "ERROR":
            log.info(
                "_checked_processing_error_cas: job_id=%s already ERROR — "
                "ERROR_CAS_ALREADY_ERROR (no-op)",
                job_id,
            )
            return ERROR_CAS_ALREADY_ERROR
        if current_status in _TERMINAL_QUEUE_STATUSES:
            log.warning(
                "_checked_processing_error_cas: job_id=%s is terminal (status=%r) — "
                "ERROR transition refused; terminal state preserved. ERROR_CAS_TERMINAL",
                job_id, current_status,
            )
            return ERROR_CAS_TERMINAL
        # WATCHING, PENDING_TRIGGER, or another nonterminal-advanced state.
        log.warning(
            "_checked_processing_error_cas: job_id=%s has status=%r — not PROCESSING; "
            "ERROR transition refused (row advanced). ERROR_CAS_WATCHING_OR_ADVANCED",
            job_id, current_status,
        )
        return ERROR_CAS_WATCHING_OR_ADVANCED

    except Exception:
        log.exception(
            "_checked_processing_error_cas raised for job_id=%s — returning ERROR_CAS_DB_ERROR",
            job_id,
        )
        return ERROR_CAS_DB_ERROR


def _checked_watching_cas(
    job_id: int,
    *,
    watching_error: str | None = None,
    watching_result: dict | None = None,
) -> str:
    """CAS: UPDATE trade_queue SET status='WATCHING' WHERE id=%s AND status='PROCESSING'.

    Returns an explicit outcome constant rather than a bare bool so callers can
    distinguish materially different zero-row outcomes and respond correctly:

      WATCHING_CAS_TRANSITIONED          – 1 row updated; row is now WATCHING.
      WATCHING_CAS_ALREADY_WATCHING      – 0 rows updated; row was already WATCHING
                                           (idempotent success — do NOT revert signals).
      WATCHING_CAS_TERMINAL              – 0 rows updated; row is in a terminal status
                                           (authoritative — do NOT overwrite the row).
      WATCHING_CAS_MISSING_OR_UNEXPECTED – 0 rows updated; row is missing or in an
                                           unexpected nonterminal status.
      WATCHING_CAS_DB_ERROR              – the UPDATE or classification SELECT raised.

    Never raises — caller handles compensation per outcome.
    Uses the deployed JSONB result_json column. public.trade_queue does not have
    a meta column, so this helper must not write one.
    """
    try:
        def _fn() -> tuple:
            with _conn()() as c:
                # 1. Attempt the guarded PROCESSING → WATCHING transition.
                c.execute(
                    """
                    UPDATE trade_queue
                    SET   status     = 'WATCHING',
                          result_json = COALESCE(result_json, '{}'::jsonb) || %s::jsonb,
                          last_error = %s
                    WHERE id     = %s
                      AND status = 'PROCESSING'
                    """,
                    (
                        _json_dumps(watching_result or {}),
                        watching_error,
                        job_id,
                    ),
                )
                if c.rowcount == 1:
                    return (1, None)
                # 2. Zero rows updated — read current status to classify the outcome.
                #    Both operations occur in the same connection for consistency.
                c.execute(
                    "SELECT status FROM trade_queue WHERE id = %s",
                    (job_id,),
                )
                row = c.fetchone()
                current = str(row["status"]).upper() if row else None
                return (0, current)

        updated, current_status = _run_with_retry(_fn)

        if updated == 1:
            return WATCHING_CAS_TRANSITIONED

        # Classify the zero-row outcome without mutating any row.
        if current_status == "WATCHING":
            return WATCHING_CAS_ALREADY_WATCHING
        if current_status is not None and current_status in _TERMINAL_QUEUE_STATUSES:
            log.warning(
                "_checked_watching_cas: job_id=%s is already terminal (status=%r) — "
                "WATCHING transition refused; terminal state preserved.",
                job_id, current_status,
            )
            return WATCHING_CAS_TERMINAL
        # Row is missing (current_status is None) or in an unexpected nonterminal state.
        log.warning(
            "_checked_watching_cas: zero-row CAS for job_id=%s — current_status=%r "
            "(missing row or unexpected nonterminal state).",
            job_id, current_status,
        )
        return WATCHING_CAS_MISSING_OR_UNEXPECTED

    except Exception:
        log.exception(
            "_checked_watching_cas raised for job_id=%s — returning DB_ERROR",
            job_id,
        )
        return WATCHING_CAS_DB_ERROR


def _dispatch(
    job_id: int,
    client_id: str,
    signal_id: str,
    payload: dict,
    *,
    job_last_error: str | None = None,
    job_result: dict | None = None,
    master_control,
    contract_selector,
    order_state_machine,
    entry_watcher,
    position_manager=None,
    exit_eng=None,
    broker=None,
    on_split_brain=None,
):
    """
    Unified control path:
      0. restart_guard          → block overnight signals if bot restarted mid-session
      1. master_control.evaluate()      → gate with placeholder estimate
      2. contract_selector.select()     → real premium, updates plan in-place
      3. master_control.revalidate()    → re-check capital/sector/ticker with REAL premium
      4. order_state_machine.create_entry_order()
      5. breach → entry_watcher  |  immediate → SUBMITTED transition
    """
    ticker = (payload or {}).get("ticker") or (payload or {}).get("symbol", "?")

    # ── PR #233 PRE-CHECKS ──────────────────────────────────────────────────
    # These run before every gate below so a malformed payload or an
    # operationally-unsafe LIVE config can never reach Master Control,
    # the selector, OSM, the watcher, or the broker.

    # PR #233 (a): LIVE + ALLOW_IMMEDIATE_EXECUTION=1 is queue-fatal.  The
    # ALLOW_IMMEDIATE_EXECUTION env flag is intended for paper testing only;
    # in LIVE it would bypass the breach-watcher trigger gate and submit to
    # the broker immediately on every dispatched signal.  Marked ERROR (not
    # REJECTED) because this is an operator configuration error, not a
    # signal-level rejection.
    runtime_mode_for_dispatch = _normalize_execution_mode(getattr(master_control, "mode", None))
    if runtime_mode_for_dispatch is None:
        _clean_payload = dict(payload or {})
        raw_mode = getattr(master_control, "mode", None)
        _clean_payload["execution_mode_validation_error"] = "metadata_invalid:unknown_execution_mode"
        log.error(
            "[%s] QUEUE_RUNTIME_EXECUTION_MODE_REJECTED signal_id=%s client_id=%s raw_mode=%r",
            ticker, signal_id, client_id, raw_mode,
        )
        _mark_job(
            job_id,
            "REJECTED",
            result={
                "stage": "execution_mode_validation",
                "reason": "metadata_invalid:unknown_execution_mode",
                "reason_code": "metadata_invalid:unknown_execution_mode",
            },
            error="metadata_invalid:unknown_execution_mode",
        )
        _log_rejection_to_db(
            signal_id=signal_id,
            client_id=client_id,
            ticker=ticker,
            side=_clean_payload.get("side", "") or "",
            score=_safe_float(_clean_payload.get("score") or 0),
            stage="execution_mode_validation",
            reason_code="metadata_invalid:unknown_execution_mode",
            human_reason=f"unknown execution_mode: {raw_mode!r}",
            payload=_clean_payload,
        )
        return

    payload_execution_mode_raw = _payload_execution_mode_value(payload)
    payload_execution_mode = _normalize_execution_mode(payload_execution_mode_raw)
    if str(payload_execution_mode_raw or "").strip() and payload_execution_mode is None:
        _clean_payload = dict(payload or {})
        _clean_payload["execution_mode_validation_error"] = "metadata_invalid:unknown_execution_mode"
        log.error(
            "[%s] QUEUE_PAYLOAD_EXECUTION_MODE_REJECTED signal_id=%s client_id=%s raw_mode=%r runtime_mode=%s",
            ticker, signal_id, client_id, payload_execution_mode_raw, runtime_mode_for_dispatch,
        )
        _mark_job(
            job_id,
            "REJECTED",
            result={
                "stage": "execution_mode_validation",
                "reason": "metadata_invalid:unknown_execution_mode",
                "reason_code": "metadata_invalid:unknown_execution_mode",
            },
            error="metadata_invalid:unknown_execution_mode",
        )
        _log_rejection_to_db(
            signal_id=signal_id,
            client_id=client_id,
            ticker=ticker,
            side=_clean_payload.get("side", "") or "",
            score=_safe_float(_clean_payload.get("score") or 0),
            stage="execution_mode_validation",
            reason_code="metadata_invalid:unknown_execution_mode",
            human_reason=f"unknown payload execution_mode: {payload_execution_mode_raw!r}",
            payload=_clean_payload,
        )
        return
    if payload_execution_mode is not None and payload_execution_mode != runtime_mode_for_dispatch:
        _clean_payload = dict(payload or {})
        _clean_payload["execution_mode_validation_error"] = "metadata_invalid:execution_mode_mismatch"
        log.error(
            "[%s] QUEUE_EXECUTION_MODE_MISMATCH signal_id=%s client_id=%s payload_mode=%s runtime_mode=%s",
            ticker, signal_id, client_id, payload_execution_mode, runtime_mode_for_dispatch,
        )
        _mark_job(
            job_id,
            "REJECTED",
            result={
                "stage": "execution_mode_validation",
                "reason": "metadata_invalid:execution_mode_mismatch",
                "reason_code": "metadata_invalid:execution_mode_mismatch",
            },
            error="metadata_invalid:execution_mode_mismatch",
        )
        _log_rejection_to_db(
            signal_id=signal_id,
            client_id=client_id,
            ticker=ticker,
            side=_clean_payload.get("side", "") or "",
            score=_safe_float(_clean_payload.get("score") or 0),
            stage="execution_mode_validation",
            reason_code="metadata_invalid:execution_mode_mismatch",
            human_reason=(
                f"payload execution_mode {payload_execution_mode!r} "
                f"does not match runtime mode {runtime_mode_for_dispatch!r}"
            ),
            payload=_clean_payload,
        )
        return
    if isinstance(payload, dict) and not str(payload_execution_mode_raw or "").strip():
        payload["execution_mode"] = runtime_mode_for_dispatch.lower()
        # Persist the resolved mode durably so a restart can prove mode from DB
        # state (trade_queue.payload) rather than re-resolving from an absent
        # field.  This is the narrowest durable write boundary: the same point
        # where the in-memory payload is authorised, before WATCHING or any
        # mode-specific durable state.  Fail-soft — see _stamp_payload_execution_mode.
        _stamp_payload_execution_mode(job_id, runtime_mode_for_dispatch.lower())

    if runtime_mode_for_dispatch == "LIVE" and bool(ALLOW_IMMEDIATE_EXECUTION):
        log.critical("[%s] LIVE_FATAL_IMMEDIATE_EXECUTION_ENABLED signal_id=%s", ticker, signal_id)
        _mark_job(job_id, "ERROR", error="LIVE_FATAL_IMMEDIATE_EXECUTION_ENABLED")
        return

    # PR #233 (b): fail-CLOSED on missing/invalid side before any downstream
    # gate.  enqueue_signal already validates and stores side_validation_error
    # on the payload, but worker_loop may dispatch rows enqueued by an older
    # code path or rows where direct DB writes bypassed enqueue_signal.  This
    # is the last line of defense.
    _side, _side_error = _normalize_queue_side(payload)
    if _side_error:
        _clean_payload = dict(payload or {})
        _clean_payload["side_validation_error"] = _side_error
        _clean_payload.pop("side", None)
        _clean_payload.pop("direction", None)
        log.error(
            "[%s] QUEUE_SIDE_VALIDATION_REJECTED signal_id=%s client_id=%s reason=%s",
            ticker, signal_id, client_id, _side_error,
        )
        _mark_job(
            job_id,
            "REJECTED",
            result={
                "stage":       "queue_side_validation",
                "reason":      "INVALID_OR_MISSING_SIDE",
                "reason_code": "INVALID_OR_MISSING_SIDE",
                "details":     _side_error,
            },
            error="queue_side_validation:INVALID_OR_MISSING_SIDE",
        )
        _log_rejection_to_db(
            signal_id=signal_id,
            client_id=client_id,
            ticker=ticker,
            side="",
            score=_safe_float(_clean_payload.get("score") or 0),
            stage="queue_side_validation",
            reason_code="INVALID_OR_MISSING_SIDE",
            human_reason=_side_error,
            payload=_clean_payload,
        )
        try:
            from ap.rejection_feed import post_master_control_block
            post_master_control_block(
                ticker=ticker,
                side="",
                stage="queue_side_validation",
                reason="INVALID_OR_MISSING_SIDE",
                score=_safe_float(_clean_payload.get("score") or 0),
                pattern=_clean_payload.get("pattern_id") or _clean_payload.get("pattern", ""),
            )
        except Exception:
            pass
        return

    # Side is valid — normalize back onto the payload so downstream code sees
    # canonical CALL/PUT regardless of which alias the scanner emitted.
    payload["side"] = _side
    payload["direction"] = _side
    # ── END PR #233 PRE-CHECKS ──────────────────────────────────────────────

    # ── P0 (monday-trade-flow-readiness): entry ticker allowlist ────────────
    # AP_ENTRY_TICKER_ALLOWLIST is an OPERATOR gate for acceptance-testing
    # windows: when set (comma-separated, e.g. "SPY,QQQ,IWM"), any dispatched
    # signal whose ticker is not in the list is REJECTED here — before Master
    # Control, the selector, OSM, or the watcher — so nothing outside the
    # allowlist can arm a watcher or reserve capital. Fully reversible by
    # unsetting the env var (default: unset → gate inactive, behavior
    # byte-for-byte unchanged). Index aliases normalize through the same map
    # the selector uses (^GSPC→SPY etc.) so an allowlisted proxy is honored.
    # Rejection is LOUD (job REJECTED + rejection ledger row) — never silent.
    _allowlist_raw = os.getenv("AP_ENTRY_TICKER_ALLOWLIST", "").strip()
    if _allowlist_raw:
        _allowlist = {t.strip().upper() for t in _allowlist_raw.split(",") if t.strip()}
        _gate_ticker = str(ticker or "").upper()
        _INDEX_ALIAS = {"^GSPC": "SPY", "^NDX": "QQQ", "^RUT": "IWM"}
        _gate_ticker_mapped = _INDEX_ALIAS.get(_gate_ticker, _gate_ticker)
        if _allowlist and _gate_ticker_mapped not in _allowlist:
            _clean_payload = dict(payload or {})
            log.warning(
                "[%s] QUEUE_TICKER_NOT_IN_ALLOWLIST signal_id=%s client_id=%s allowlist=%s",
                ticker, signal_id, client_id, ",".join(sorted(_allowlist)),
            )
            _mark_job(
                job_id,
                "REJECTED",
                result={
                    "stage":       "entry_ticker_allowlist",
                    "reason":      "TICKER_NOT_IN_ALLOWLIST",
                    "reason_code": "TICKER_NOT_IN_ALLOWLIST",
                    "allowlist":   sorted(_allowlist),
                },
                error="entry_ticker_allowlist:TICKER_NOT_IN_ALLOWLIST",
            )
            _log_rejection_to_db(
                signal_id=signal_id,
                client_id=client_id,
                ticker=ticker,
                side=_side,
                score=_safe_float(_clean_payload.get("score") or 0),
                stage="entry_ticker_allowlist",
                reason_code="TICKER_NOT_IN_ALLOWLIST",
                human_reason=(
                    f"ticker {_gate_ticker_mapped} not in AP_ENTRY_TICKER_ALLOWLIST "
                    f"({_allowlist_raw})"
                ),
                payload=_clean_payload,
            )
            return
    # ── END entry ticker allowlist ───────────────────────────────────────────

    # Resolve canonical_signal_id — primary idempotency key for opportunity ledger.
    # PR79 build_canonical_signal_id prevents REEVAL suffix variants from
    # fragmenting opportunity rows.
    _canonical_signal_id = payload.get("canonical_signal_id") or signal_id
    try:
        from ap_canonical_signal import build_canonical_signal_id as _build_cid
        _canonical_signal_id = _build_cid(str(signal_id or ""), payload) or _canonical_signal_id
    except Exception:
        pass  # canonical module optional — fallback to signal_id

    # live_mode is derived from master_control.mode — it is NOT passed as a parameter.
    # master_control is the sole authority for LIVE vs PAPER mode in _dispatch.
    # worker_loop's live_mode parameter is only used for the mode label log and
    # legacy-fallback guard; it does not affect _dispatch fail-closed logic.
    _execution_mode = runtime_mode_for_dispatch
    live_mode: bool = _execution_mode == "LIVE"

    # Intelligence context PRETRIGGER enqueue: durable, observe-only, and
    # non-blocking. This is intentionally before synchronous Master Control
    # intelligence so slow evidence can materialize ahead of any trigger breach.
    try:
        from ap.intelligence_context_handoff import enqueue_pretrigger_context_best_effort

        _intel_enqueue = enqueue_pretrigger_context_best_effort(
            payload,
            client_id=client_id,
            execution_mode=_execution_mode,
            canonical_signal_id=_canonical_signal_id,
        )
        if not _intel_enqueue.get("ok"):
            log.warning(
                "[%s] PRETRIGGER intelligence enqueue failed signal_id=%s reason=%s",
                ticker,
                signal_id,
                _intel_enqueue.get("error"),
            )
    except Exception as _intel_exc:
        log.warning(
            "[%s] PRETRIGGER intelligence enqueue error signal_id=%s: %s",
            ticker,
            signal_id,
            _intel_exc,
        )

    if _paper_overnight_reeval_only_enabled(payload=payload, execution_mode=_execution_mode):
        log.warning(
            "[%s] PAPER OVERNIGHT RESCUE ROUTE — deferring queue row to overnight_reeval only | signal_id=%s job_id=%s",
            ticker,
            signal_id,
            job_id,
        )
        # Route through the single deferral authority: the overnight reeval only
        # discovers this row via ap_signals decision_status='WATCHING', so the
        # ap_signals row MUST be persisted (and confirmed) BEFORE the queue row
        # is marked WATCHING. If persistence fails the queue row becomes ERROR
        # (not a phantom WATCHING the rescue could never find).
        _persist_watching_deferral(
            job_id=job_id,
            client_id=client_id,
            signal_id=signal_id,
            execution_mode=_execution_mode,
            payload=payload,
            stage="overnight_reeval",
            reason_code="market_closed_deferred",
            human_reason=(
                "PAPER overnight rescue — deferred to overnight reeval. "
                "No action needed until the morning revaluation."
            ),
            watching_error=_PAPER_OVERNIGHT_REEVAL_ONLY_ERROR,
            watching_result={"stage": "overnight_reeval", "reason": "paper_force_overnight_only"},
            failure_error="ap_signals_write_failed:paper_overnight_reeval_only",
        )
        return

    # ── 0. RESTART GUARD ─────────────────────────────────────────────────────
    # Blocks overnight (previous-day) signals during market hours.
    # Prevents 47-signal mass re-fire when bot restarts mid-session.
    # Pre-market restarts are allowed through for morning revalidation.
    try:
        from ap.restart_guard import should_skip_on_restart
        _restart_skip = bool(should_skip_on_restart(payload))
        _manual_rescue_bypass = _manual_restart_guard_bypass_enabled(
            job_last_error=job_last_error,
            job_result=job_result,
            payload=payload,
            execution_mode=_execution_mode,
        )
        if _restart_skip and not _manual_rescue_bypass:
            log.warning("[%s] RESTART GUARD — overnight signal blocked", ticker)
            _mark_job(job_id, "REJECTED", error="restart_guard:overnight_skip")
            try:
                from ap.rejection_feed import post_master_control_block
                post_master_control_block(
                    ticker=ticker,
                    side=payload.get("side", ""),
                    stage="restart_guard",
                    reason="restart_guard:overnight_skip",
                    score=float(payload.get("score") or 0),
                    pattern=payload.get("pattern_id") or payload.get("pattern", ""),
                )
            except Exception:
                pass
            return
        if _restart_skip and _manual_rescue_bypass:
            # Emit a structured log so the reason for bypass is observable.
            _is_paper_recovery = _is_current_session_paper_recovery(
                payload=payload,
                execution_mode=_execution_mode,
            )
            if _is_paper_recovery:
                log.info(
                    "[%s] PAPER_RECOVERY_RESTART_GUARD_BYPASS "
                    "signal_id=%s job_id=%s client_id=%s execution_mode=%s "
                    "recovery_ts=%s lookback_hours=%s",
                    ticker,
                    signal_id,
                    job_id,
                    client_id,
                    str(_execution_mode or "").upper(),
                    payload.get("recovery_rescue_ts") if isinstance(payload, dict) else None,
                    payload.get("recovery_rescue_lookback_hours") if isinstance(payload, dict) else None,
                )
            else:
                log.warning(
                    "[%s] RESTART GUARD BYPASS — continuing manual rescue row | signal_id=%s job_id=%s last_error=%s",
                    ticker,
                    signal_id,
                    job_id,
                    job_last_error,
                )
    except ImportError:
        pass  # restart_guard not yet deployed — skip silently

    # ── 0.5 BROKER EQUITY REFRESH — cached 30s, fail-closed in LIVE ─────────────
    # Equity gates must reflect real account balance — not startup defaults.
    # Cache: 30s per client avoids blocking signal pipeline on multi-client setups.
    # LIVE: stale or unavailable equity = signal rejected (fail closed, no exceptions).
    # PAPER: stale/missing equity = log warning, continue with last known value.
    _EQ_CACHE_TTL = 30.0
    _eq_cache_key = f"_eq_cache:{client_id}"
    _eq_last_ts = getattr(master_control, "_equity_cache_ts", 0.0)
    _eq_stale = (time.time() - _eq_last_ts) > _EQ_CACHE_TTL

    if _eq_stale:
        try:
            if broker and hasattr(broker, "get_account_equity"):
                _fresh_equity = broker.get_account_equity()
                if _fresh_equity and float(_fresh_equity) > 0:
                    master_control.set_account_equity(float(_fresh_equity))
                    master_control._equity_cache_ts = time.time()
                    log.info("[%s] Equity refreshed: $%.2f (cache valid 30s)", ticker, float(_fresh_equity))
                else:
                    if live_mode:
                        log.error("[%s] LIVE: broker returned zero/null equity — rejecting (fail closed)", ticker)
                        _mark_job(job_id, "REJECTED", error="live_equity_unavailable:broker_returned_zero")
                        return
                    log.warning("[%s] PAPER: equity refresh returned zero — using last known value", ticker)
            else:
                if live_mode:
                    log.error("[%s] LIVE: no broker equity method — rejecting (fail closed)", ticker)
                    _mark_job(job_id, "REJECTED", error="live_equity_unavailable:no_broker_method")
                    return
        except Exception as _eq_exc:
            if live_mode:
                log.error("[%s] LIVE: equity refresh error %s — rejecting (fail closed)", ticker, _eq_exc)
                _mark_job(job_id, "REJECTED", error=f"live_equity_unavailable:{_eq_exc}")
                return
            log.warning("[%s] PAPER: equity refresh error %s — continuing with cached value", ticker, _eq_exc)
    else:
        log.debug("[%s] Equity cache fresh (%.0fs old) — skipping broker call", ticker, time.time() - _eq_last_ts)

    # ── 1. MASTER CONTROL (initial gate, placeholder estimate) ────────────────
    # Inject _queue_id so the durable duplicate guard in master_control can
    # exclude THIS row from "is there an active trade_queue row for the same
    # client+signal" check. Without this, the guard would see the current
    # PROCESSING row and reject every dispatched job as
    # duplicate_signal_id (durable:trade_queue). Shallow copy preserves the
    # caller's payload — we never mutate the incoming dict.
    payload_for_mc = dict(payload or {})
    payload_for_mc["_queue_id"] = job_id
    try:
        decision = master_control.evaluate(payload_for_mc, client_id=client_id)
    except Exception as e:
        log.error(f"[{ticker}] master_control.evaluate() failed: {e}")
        _mark_job(job_id, "ERROR", error=f"master_control_error: {e}")
        return

    # ── FVG TELEMETRY (observe-only, PR #221 activation) ──────────────────
    # Runs on EVERY dispatched signal regardless of decision outcome so the
    # dataset covers accepted AND rejected populations (required for the
    # path-risk hypothesis test). #283 REVIEW AMENDMENT: fire-and-forget
    # daemon thread — the synchronous form could spend up to one timesales
    # timeout (10s) on a ticker cache miss AHEAD of contract selection /
    # watcher creation, which is not observe-only for a live entry. The
    # thread reads market data (TTL-cached per ticker); its ONLY write is
    # score_breakdown->'fvg' on ap_signals. Never influences `decision`,
    # never raises (module invariant 2).
    try:
        from ap.fvg_telemetry import record_fvg_telemetry_async
        record_fvg_telemetry_async(
            signal_id=signal_id,
            client_email=client_id,
            payload=payload_for_mc,
            broker=broker,
        )
    except Exception as _fvg_exc:
        log.warning("[%s] fvg_telemetry wiring error (non-fatal): %s", ticker, _fvg_exc)

    if not decision.ok:
        log.info(f"[{ticker}] BLOCKED | stage={decision.stage} reason={decision.reason}")


        # ── CLASSIFY FIRST: deferral vs genuine terminal rejection ─────────────
        # A deferrable block (daily_stop / sizer_blocked / after_hours) that
        # lands OUTSIDE regular session is NOT a terminal rejection — it is a
        # setup to reevaluate overnight. Classify BEFORE calling mark_missed,
        # trace_gate, or the rejection feed: a deferred setup must NEVER be
        # terminalized in the opportunity ledger, traced as REJECT, or broadcast
        # as rejected — it remains live, awaiting overnight reeval.
        #
        # The previous ordering ran mark_missed → trace_gate(REJECT) → rejection
        # feed FIRST, then checked deferrability. That created two defects:
        #   1. float(payload["score"]) in trace_gate crashed on malformed scanner
        #      scores (e.g. "A+") before _persist_watching_deferral was reached.
        #   2. mark_missed stamped the opportunity MISSED (rank 90) in the ledger;
        #      a later overnight rearm could not repair that terminal status.
        # reason_code="market_closed_deferred" keeps the row recognizable to
        # the shared overnight fan-out (_is_shared_watch_signal_row).
        _block_reason = str(decision.reason or "")
        _is_deferrable = any(
            k in _block_reason for k in ("daily_stop", "sizer_blocked", "after_hours")
        )
        try:
            _in_session = _is_regular_session_et(_now_et())
        except Exception:
            # Fail toward terminal rejection — never a phantom deferral.
            _in_session = True
        if _is_deferrable and not _in_session:
            _persist_watching_deferral(
                job_id=job_id,
                client_id=client_id,
                signal_id=signal_id,
                execution_mode=_execution_mode,
                payload=payload,
                stage="master_control",
                reason_code="market_closed_deferred",
                human_reason=(
                    f"After market hours — master control deferred ({_block_reason}). "
                    "Awaiting overnight reeval. No action needed."
                ),
                watching_error=_PAPER_OVERNIGHT_REEVAL_ONLY_ERROR,
                failure_error="ap_signals_write_failed:master_control_deferred",
            )
            return

        # ── GENUINE TERMINAL REJECTION ─────────────────────────────────────────
        # Only reaches here when: in-session, or a non-deferrable reason.
        # Safe to terminalize the opportunity ledger, emit the REJECT trace,
        # and broadcast the rejection feed.

        # PR1 + Amendment §4: map MC reason to canonical miss stage instead
        # of always writing STAGE_UNKNOWN.
        try:
            from ap.opportunity_ledger import mark_missed, map_reason_to_stage
            _mc_stage = map_reason_to_stage(
                f"{decision.stage or ''} {decision.reason or ''}"
            )
            mark_missed(signal_id, client_id, _mc_stage,
                        str(decision.reason or "mc_blocked"),
                        canonical_signal_id=_canonical_signal_id,
                        extra_meta={"mc_decision_stage": str(decision.stage or ""),
                                    "mc_decision_reason": str(decision.reason or "")})
        except Exception: pass
        # _safe_float guards against malformed scanner scores ("A+") on the
        # terminal path — the float() call that was here previously would have
        # raised; we now use the same guard as _persist_watching_deferral.
        trace_gate(str(payload.get("signal_id","")), ticker, "MC_REJECTED", "REJECT",
                   reason=decision.reason, score=_safe_float(payload.get("score") or 0))

        # Post the master-control terminal block to Discord.
        try:
            from ap.rejection_feed import post_master_control_block
            post_master_control_block(
                ticker=ticker,
                side=payload.get("side", ""),
                stage=decision.stage,
                reason=decision.reason,
                score=_safe_float(payload.get("score") or 0),
                pattern=payload.get("pattern_id") or payload.get("pattern", ""),
            )
        except Exception:
            pass

        # Permanent structured terminal rejection.
        _mark_job(job_id, "REJECTED",
                  result={"stage": decision.stage, "reason": decision.reason})
        _log_rejection_to_db(
            signal_id=signal_id, client_id=client_id, ticker=ticker,
            side=payload.get("side", ""), score=_safe_float(payload.get("score") or 0),
            stage=decision.stage, reason_code=str(decision.reason or ""),
            human_reason=str(decision.reason or ""), payload=payload,
        )
        return

    plan = decision.plan

    # ── 1-1 PAIR MANAGER — register signal so opposite side cancels on fill ──
    try:
        from ap.signal_pair_manager import get_pair_manager
        _pm = get_pair_manager()
        _pair_key = _pm.register(
            ticker=ticker,
            side=payload.get("side", ""),
            local_order_id=signal_id,
            pattern_id=payload.get("pattern_id") or payload.get("pattern", ""),
        )
        if _pair_key:
            log.info("[%s] Registered as 1-1 pair: %s", ticker, _pair_key)
    except Exception as _e:
        log.warning("queue_pair_registration_failed: %s", _e)

    # ── 2. CONTRACT SELECTION -- skip outside market hours ──────────────────────
    _skip_contract_selection = False
    try:
        _now_et2 = _now_et()
        _in_mkt  = _is_regular_session_et(_now_et2)
        _bypass_mkt = bool(payload.get("bypass_market_hours") or payload.get("test_mode"))
        if not _in_mkt and contract_selector and not _bypass_mkt:
            _skip_contract_selection = True
            log.info(
                f"[{ticker}] Post-market signal — skipping contract selection "
                f"(stale quotes). Will select at breach time with live quotes."
            )
            # Explicitly mark this plan as watcher/breach-time contract selection.
            # Prevents downstream confusion: no broker order should be submitted
            # until live quotes are available at breach.
            try:
                plan.contract_symbol = None
                setattr(plan, "_needs_contract_selection", True)
                if hasattr(plan, "metadata") and isinstance(plan.metadata, dict):
                    plan.metadata["needs_contract_selection"] = True
                    plan.metadata["contract_selection_deferred"] = "outside_regular_session"
            except Exception:
                pass
            # Reject immediately — market is closed, live quotes unavailable for
            # contract selection. Accept only when breach-time re-selection is
            # wired end-to-end; until then dropping is safer than a stale contract.
            _sig_timeframe = str(payload.get("timeframe") or "1d").lower().strip()
            _is_overnight_candidate = _sig_timeframe in (
                "1d", "daily", "overnight", "1w", "weekly"
            )
            if _is_overnight_candidate:
                log.info(
                    "[%s] overnight_candidate_loaded=true signal=%s market_hours=false "
                    "timeframe=%s contract_selection=deferred "
                    "execution_pipeline=watching_for_morning_reeval",
                    ticker, signal_id, _sig_timeframe,
                )
            else:
                log.info(
                    "[%s] Signal %s after-hours intraday — market closed, "
                    "contract selection deferred. Informational only.",
                    ticker, signal_id,
                )
            # P0 #405 / P1: route through _persist_watching_deferral — the single
            # authority for all WATCHING deferrals. This removes the last direct
            # _log_signal_to_db + _mark_job(WATCHING) caller in _dispatch, so the
            # after-hours path now inherits:
            #   • guarded PROCESSING→WATCHING queue CAS (_checked_watching_cas)
            #   • guarded early ERROR CAS (_checked_processing_error_cas)
            #   • expected-state ap_signals compensation
            #     (_compensate_watching_signal_if_unchanged)
            #   • malformed-score sanitization
            #   • explicit outcome logging
            #
            # track_counterfactual_signal is called only after durable deferral
            # success — if _persist_watching_deferral returns False the signal
            # is not in a valid WATCHING state and counterfactual tracking would
            # record a phantom overnight candidate.
            _watching_ok = _persist_watching_deferral(
                job_id=job_id,
                client_id=client_id,
                signal_id=signal_id,
                execution_mode=str(getattr(master_control, "mode", "PAPER") or "PAPER"),
                payload=payload,
                stage="contract_selection",
                reason_code="market_closed_deferred",
                human_reason="After market hours — deferred to next session. No action needed.",
                watching_error=_PAPER_OVERNIGHT_REEVAL_ONLY_ERROR,
                watching_result={"after_hours_deferred": True},
                failure_error="ap_signals_write_failed:after_hours_deferred",
            )
            if _watching_ok:
                try:
                    from ap.counterfactual_tracker import track_counterfactual_signal

                    payload_copy = dict(payload or {})
                    payload_copy["signal_id"] = signal_id
                    track_counterfactual_signal(
                        signal=payload_copy,
                        client_id=client_id,
                        execution_mode=str(getattr(master_control, "mode", "PAPER") or "PAPER"),
                        block_stage="contract_selection",
                        block_reason="market_closed_deferred",
                        reason_code="market_closed_deferred",
                        source="watch",
                    )
                except Exception:
                    pass
            return
    except Exception as _mkt_err:
        log.warning("[%s] Market hours check failed: %s — proceeding", ticker, _mkt_err)

    if contract_selector and not _skip_contract_selection:
        try:
            selected = contract_selector.select(plan)
            if selected is None:
                log.warning(f"[{ticker}] Contract selection failed -- no suitable contract")
                trace_gate(str(payload.get("signal_id","")), ticker, "QUALITY_FILTER", "REJECT",
                           reason="no_eligible_contracts", score=float(payload.get("score") or 0))

                # PR #149 — Selector Reason Honesty.
                # Ask the selector for the most specific REJECT it captured
                # during this call (OI_TOO_LOW, NO_CHAIN_DATA, SPREAD_TOO_WIDE,
                # NO_AFFORDABLE_CONTRACT, etc). Keep umbrella stage as
                # "contract_selection" so downstream stage-filters and the
                # PR #120 _derive_last_error path continue to work, but write
                # the specific reason into result_json.reason and reason_code
                # so last_error becomes e.g. "contract_selection:OI_TOO_LOW"
                # instead of the umbrella "contract_selection:no_contract_found".
                # Fallback: if no specific reason was captured, preserve the
                # legacy "no_contract_found" label exactly.
                _sel_result: dict = {"stage": "contract_selection", "ticker": ticker}
                try:
                    _get_failure = getattr(contract_selector, "get_last_failure", None)
                    _failure = _get_failure() if callable(_get_failure) else None
                except Exception:
                    _failure = None
                # PR #233: stash the full failure dict so _mark_job can merge
                # chain_rows / survivor_count / top_reject_buckets / etc into
                # result_json for operator-dashboard attribution.  Cleared on
                # read inside _mark_job.
                if isinstance(_failure, dict):
                    try:
                        _selector_failure_by_job[int(job_id)] = copy.deepcopy(_failure)
                    except Exception:
                        pass
                if isinstance(_failure, dict) and str(_failure.get("reason_code") or "").strip():
                    _reason_code = str(_failure["reason_code"]).strip()
                    _sel_result["reason"]         = _reason_code
                    _sel_result["reason_code"]    = _reason_code
                    _explanation = str(_failure.get("explanation") or "").strip()
                    if _explanation:
                        _sel_result["details"] = _explanation
                    _selector_stage = str(_failure.get("stage") or "").strip()
                    if _selector_stage:
                        # Preserve the underlying selector stage (e.g.
                        # "quality_summary", "affordability_gate") for
                        # diagnosis without changing the umbrella stage.
                        _sel_result["selector_stage"] = _selector_stage
                else:
                    _sel_result["reason"] = "no_contract_found"

                _mark_job(job_id, "REJECTED", result=_sel_result)
                try:
                    from ap.rejection_feed import post_no_contracts
                    post_no_contracts(
                        ticker=ticker,
                        side=payload.get("side", ""),
                        chain_size=0,
                        top_rejections={},
                        score=float(payload.get("score") or 0),
                        pattern=payload.get("pattern_id") or payload.get("pattern", ""),
                    )
                except Exception:
                    pass
                return
            log.info(
                f"[{ticker}] Contract selected: {selected.contract_symbol} "
                f"@ ${selected.mid:.2f} x{plan.contracts} "
                f"cost=${plan.max_position_usd:.0f}"
            )
            # Item 3 (review fix) — stash candidate_audit on plan.metadata so
            # execution_core can persist it into orders.meta for the preselected
            # (non-deferred) queue path. Without this stash, only breach-time
            # deferred selection would produce selector_candidate_audit; the
            # normal preselected path would leave audit absent.
            try:
                _ca = getattr(selected, "candidate_audit", None)
                if _ca is not None:
                    if not hasattr(plan, "metadata") or not isinstance(plan.metadata, dict):
                        plan.metadata = {}
                    plan.metadata["selector_candidate_audit"] = _ca
                    plan.metadata["candidate_table"] = _ca
            except Exception:
                pass
        except Exception as e:
            log.error(f"[{ticker}] contract_selector.select() raised: {e}")
            _mark_job(job_id, "ERROR", error=f"contract_selector_error: {e}")
            return
    else:
        log.debug(f"[{ticker}] No contract selector -- plan uses placeholder sizing")

    # ── 3. RE-VALIDATE with REAL premium ──────────────────────────────────────
    revalidation = master_control.revalidate_exposure(plan, client_id=client_id)
    if not revalidation.ok:
        log.warning(
            f"[{ticker}] BLOCKED at re-validation (real premium) | "
            f"reason={revalidation.reason}"
        )
        try:
            from ap.opportunity_ledger import mark_missed, STAGE_CLIENT_PREFLIGHT
            mark_missed(signal_id, client_id, STAGE_CLIENT_PREFLIGHT,
                        str(revalidation.reason or "revalidation_block"),
                        canonical_signal_id=_canonical_signal_id)
        except Exception: pass
        _mark_job(job_id, "REJECTED",
                  result={"stage": "revalidation", "reason": revalidation.reason,
                          "real_cost": plan.max_position_usd},
                  error=(f"revalidation:{revalidation.reason}"
                         if revalidation.reason else "revalidation:block"))
        return

    # ── 4. ROUTE -- BREACH vs IMMEDIATE (resolved BEFORE OSM create) ──────────
    # PR F / queue truth hardening: route resolution AND the 3:15 PM ET
    # cutoff MUST run BEFORE the OSM entry-order create call below. The
    # previous order created an OSM PENDING_TRIGGER row and reserved
    # capital, THEN ran the cutoff — leaking a stale OSM row and locked
    # capital on every signal that arrived after 15:15 ET during the
    # regular session.
    #
    # Overnight signals always go breach-only — never immediate execution.
    # Use ET date to avoid UTC/ET day-boundary misclassification.
    _signal_date  = (payload.get("created_at") or payload.get("timestamp_iso") or "")[:10]
    _today_str    = _now_et().date().isoformat()
    forced_breach = False

    if _signal_date and _signal_date < _today_str:
        forced_breach = True
        log.warning("[%s] Overnight signal (created %s) — forcing breach-only path",
                    ticker, _signal_date)
        try:
            plan.trigger_type = "breach"
        except Exception:
            pass
        if not entry_watcher:
            log.error("[%s] Dropping overnight signal — no entry watcher", ticker)
            _mark_job(job_id, "REJECTED", error="overnight_signal_no_watcher")
            return

    # Breach-first policy:
    #   - forced_breach is sticky
    #   - immediate execution is disabled by default
    #   - set ALLOW_IMMEDIATE_EXECUTION=1 only for controlled internal testing
    raw_trigger_type = str(getattr(plan, "trigger_type", "breach") or "breach").lower()
    if forced_breach:
        trigger_type = "breach"
    elif raw_trigger_type == "immediate" and ALLOW_IMMEDIATE_EXECUTION:
        trigger_type = "immediate"
    else:
        if raw_trigger_type == "immediate" and not ALLOW_IMMEDIATE_EXECUTION:
            log.warning(
                "[%s] Immediate execution requested but disabled — forcing breach/watch path",
                ticker,
            )
        trigger_type = "breach"

    # ── PR #143: PAPER recovery-rescued immediate-entry promotion ─────────
    # When recovery rescued a stale WATCHING signal back to NEW, the trigger
    # event already happened (or the bot missed it). For PAPER mode we allow
    # the signal to submit immediately after the normal quality gates pass:
    #     - score floor (master_control already enforced above)
    #     - contract selected (verified below before promotion)
    #     - revalidation (master_control already enforced above)
    #     - duplicate guard (master_control already enforced above)
    #     - within entry cutoff (cutoff guard runs after this block, BEFORE
    #       the OSM create — the cutoff still applies)
    # LIVE rows are NEVER promoted — LIVE always waits for breach confirmation.
    # Forced-breach signals are also never promoted — explicit operator override.
    #
    # The promotion only flips trigger_type. The existing immediate-execution
    # branch at the bottom of _dispatch runs the actual submit, which goes
    # through submit_existing_entry → OSM → broker_submit. No bypass of any
    # quality gate, capital check, or compliance step.
    if (
        trigger_type == "breach"
        and not forced_breach
        and not live_mode
        and bool(payload.get("recovery_rescue"))
    ):
        # PAPER recovery row — verify a real contract is selected before
        # promotion. If selection deferred or produced a placeholder, fall
        # through to the normal breach path so the watcher can re-select
        # at breach time.
        _rec_contract = str(getattr(plan, "contract_symbol", "") or "").strip()
        _rec_contracts_qty = 0
        try:
            _rec_contracts_qty = int(getattr(plan, "contracts", 0) or 0)
        except (TypeError, ValueError):
            _rec_contracts_qty = 0
        _rec_limit = 0.0
        try:
            _rec_limit = float(getattr(plan, "limit_price", 0) or 0)
        except (TypeError, ValueError):
            _rec_limit = 0.0
        _rec_deferred = (
            not _rec_contract
            or _rec_contract.upper().startswith("DEFERRED:")
        )
        if _rec_deferred or _rec_contracts_qty <= 0 or _rec_limit <= 0:
            log.info(
                "[%s] PAPER_RECOVERY_IMMEDIATE_SKIP — contract not ready "
                "(contract=%r contracts=%s limit=%s) — falling back to breach path",
                ticker, _rec_contract, _rec_contracts_qty, _rec_limit,
            )
        else:
            # Increment per-client counter under lock — single source of truth
            # for the audit invariant: no LIVE client_id may ever appear here.
            with _PAPER_RECOVERY_IMMEDIATE_LOCK:
                _PAPER_RECOVERY_IMMEDIATE_COUNTS[client_id] = (
                    _PAPER_RECOVERY_IMMEDIATE_COUNTS.get(client_id, 0) + 1
                )
                _client_count_after = _PAPER_RECOVERY_IMMEDIATE_COUNTS[client_id]

            # Mode label from master_control — same source of truth used by
            # the LIVE/PAPER gate above, NOT BOT_MODE env (which could
            # disagree in a misconfig).
            _mc_mode_label = str(getattr(master_control, "mode", "PAPER") or "PAPER").upper()

            log.warning(
                "[%s] PAPER_RECOVERY_IMMEDIATE_PROMOTE | client_id=%s "
                "bot_mode=%s contract=%s qty=%s limit=$%.2f signal_id=%s "
                "client_count_after=%d | promoting trigger_type breach→immediate "
                "(LIVE clients are never promoted; this counter must never "
                "include any LIVE client_id)",
                ticker, client_id, _mc_mode_label,
                _rec_contract, _rec_contracts_qty, _rec_limit,
                signal_id, _client_count_after,
            )
            trigger_type = "immediate"
            # Persist the promotion decision on plan.metadata so the immediate
            # submit path can reference it in any downstream audit.
            try:
                if hasattr(plan, "metadata") and isinstance(plan.metadata, dict):
                    plan.metadata["paper_recovery_immediate"] = True
                    plan.metadata["paper_recovery_immediate_reason"] = (
                        "recovery_rescued_signal_paper_mode_all_gates_passed"
                    )
                    plan.metadata["paper_recovery_immediate_count_after"] = (
                        int(_client_count_after)
                    )
                    plan.metadata["paper_recovery_immediate_mode_observed"] = (
                        _mc_mode_label
                    )
            except Exception:
                pass

            # Durable per-row audit in ap_signals.context_notes — proves
            # from DB (not just logs) which client_id ever saw a promotion.
            # ap_signals already has a row for this signal from earlier in
            # dispatch; we only update context_notes. Audit is best-effort —
            # if the table is unavailable, log at debug and proceed.
            try:
                _sb = _get_sb_client()
                if _sb is not None:
                    _audit_blob = (
                        f"paper_recovery_immediate_promoted="
                        f"client={client_id};bot_mode={_mc_mode_label};"
                        f"contract={_rec_contract};qty={_rec_contracts_qty};"
                        f"limit={_rec_limit:.2f};"
                        f"client_count_after={_client_count_after}"
                    )
                    _sb.table("ap_signals").update({
                        "context_notes": _audit_blob,
                    }).eq("signal_id", signal_id).execute()
            except Exception as _audit_exc:
                log.debug(
                    "[%s] PAPER_RECOVERY_IMMEDIATE_AUDIT_WRITE_FAILED "
                    "client_id=%s signal_id=%s error=%s",
                    ticker, client_id, signal_id, _audit_exc,
                )

    if trigger_type == "breach" and not entry_watcher:
        log.critical("[%s] ENTRY WATCHER MISSING — cannot arm breach entry", ticker)
        _mark_job(job_id, "REJECTED", error="entry_watcher_missing")
        try:
            from ap.rejection_feed import post_master_control_block
            post_master_control_block(
                ticker=ticker,
                side=payload.get("side", ""),
                stage="entry_watcher",
                reason="entry_watcher_missing",
                score=float(payload.get("score") or 0),
                pattern=payload.get("pattern_id") or payload.get("pattern", ""),
            )
        except Exception:
            pass
        return

    # Block entries after 3:15 PM ET during market hours (weekdays only).
    # PR F: this MUST run BEFORE the OSM entry-order create call to avoid
    # leaking a PENDING_TRIGGER row and reserved capital on late-day signals.
    _now_et_cut  = _now_et()
    _in_session  = _is_regular_session_et(_now_et_cut)
    _too_late    = _in_session and (
        _now_et_cut.hour > 15 or (_now_et_cut.hour == 15 and _now_et_cut.minute >= 15)
    )
    if _too_late and trigger_type == "breach":
        log.warning(
            "[%s] ENTRY BLOCKED — too late in session (%02d:%02d ET, cutoff 15:15)",
            ticker, _now_et_cut.hour, _now_et_cut.minute,
        )
        _mark_job(job_id, "REJECTED", error="entry_cutoff: too_late_in_session")
        return

    # ── 4.5 LIVE AUTHORIZATION GATE (NEW LIVE entries only) ───────────────────
    # Server-side, runs BEFORE any entry order is created. Evaluated for a NEW
    # entry when the client either (a) routes to a LIVE broker but lacks a valid
    # one-time LIVE_TRADING + valid WEEKLY_TRADING authorization, or (b) has an
    # UNKNOWN/unverifiable broker mode (we never assume paper).
    #
    # Two enforcement modes (env LIVE_AUTHORIZATION_GATE_ENFORCE, default FALSE):
    #   • OBSERVE (false): log LIVE_AUTHORIZATION_WOULD_BLOCK and CONTINUE the
    #     entry — rollout/observation phase.
    #   • ENFORCE (true): REJECT the entry and log LIVE_ENTRY_BLOCKED.
    #
    # Known paper/sandbox clients are EXEMPT — is_live_broker() is False AND the
    # mode is known, so neither branch fires.
    #
    # This gate NEVER runs on exits, stop-losses, emergency exits, profit-taking,
    # reconciliation, position monitoring, or EOD flattening — none of those go
    # through _dispatch (which only creates NEW entries).
    try:
        from ap.authorization import (
            is_live_broker, broker_live_mode_known, check_live_authorization,
            authorization_gate_enforced, LIVE_AUTHORIZATION_GATE_UNAVAILABLE,
        )
        _gate_reason = None
        if not broker_live_mode_known(broker):
            # Unknown/unverifiable broker mode — fail closed, never assume paper.
            _gate_reason = LIVE_AUTHORIZATION_GATE_UNAVAILABLE
        elif is_live_broker(broker):
            _gate_reason = check_live_authorization(client_id)
        # Known paper/sandbox → _gate_reason stays None (exempt).

        if _gate_reason:
            _enforced = authorization_gate_enforced()
            if not _enforced:
                # OBSERVE MODE — log intent and CONTINUE (do not block the entry).
                log.warning(
                    "[%s] LIVE_AUTHORIZATION_WOULD_BLOCK — %s | client=%s (observe mode; entry allowed)",
                    ticker, _gate_reason, client_id,
                )
                try:
                    from ap.execution import audit
                    audit(client_id, "WARNING", "LIVE_AUTHORIZATION_WOULD_BLOCK", {
                        "reason_code": _gate_reason,
                        "ticker": ticker,
                        "signal_id": signal_id,
                        "enforced": False,
                    })
                except Exception as _audit_exc:
                    log.debug("[%s] WOULD_BLOCK audit write failed: %s", ticker, _audit_exc)
                # fall through — entry proceeds in observe mode.
            else:
                # ENFORCE MODE — reject the new live entry.
                log.warning(
                    "[%s] LIVE ENTRY BLOCKED — %s | client=%s",
                    ticker, _gate_reason, client_id,
                )
                _mark_job(job_id, "REJECTED",
                          result={"stage": "live_authorization",
                                  "reason": _gate_reason},
                          error=f"live_authorization:{_gate_reason}")
                # Permanent structured rejection record — operator dashboard reads
                # this to show WHY no trade was created.
                _log_rejection_to_db(
                    signal_id=signal_id, client_id=client_id, ticker=ticker,
                    side=payload.get("side", ""), score=float(payload.get("score") or 0),
                    stage="live_authorization", reason_code=_gate_reason,
                    human_reason=_gate_reason, payload=payload,
                )
                # audit_log row (LIVE_ENTRY_BLOCKED) for compliance trail.
                try:
                    from ap.execution import audit
                    audit(client_id, "WARNING", "LIVE_ENTRY_BLOCKED", {
                        "reason_code": _gate_reason,
                        "ticker": ticker,
                        "signal_id": signal_id,
                        "enforced": True,
                    })
                except Exception as _audit_exc:
                    log.debug("[%s] LIVE_ENTRY_BLOCKED audit write failed: %s", ticker, _audit_exc)
                try:
                    from ap.rejection_feed import post_master_control_block
                    post_master_control_block(
                        ticker=ticker,
                        side=payload.get("side", ""),
                        stage="live_authorization",
                        reason=_gate_reason,
                        score=float(payload.get("score") or 0),
                        pattern=payload.get("pattern_id") or payload.get("pattern", ""),
                    )
                except Exception:
                    pass
                return
    except Exception as _authz_exc:
        # Fail CLOSED for live clients when the gate ENFORCES: if the gate itself
        # errors, do NOT create a live entry. In observe mode, a gate error must
        # not block trading (the gate is non-blocking by design there).
        try:
            from ap.authorization import (
                is_live_broker as _ilb, broker_live_mode_known as _bmk,
                authorization_gate_enforced as _enf,
            )
            _enforced = _enf()
            # "Risky" = live OR unknown mode (anything that isn't known-paper).
            _risky = (not _bmk(broker)) or _ilb(broker)
        except Exception:
            _enforced, _risky = False, False
        if _enforced and _risky:
            log.error(
                "[%s] LIVE authorization gate error — failing closed (enforce): %s",
                ticker, _authz_exc,
            )
            _mark_job(job_id, "REJECTED",
                      error=f"live_authorization_gate_error: {_authz_exc}")
            return
        log.debug("[%s] authorization gate skipped (observe or non-live): %s", ticker, _authz_exc)

    # ── 4.6 PR81: Client State Preflight ─────────────────────────────────────
    # REBASE NOTE (Final Amendment v2 §4): PR #87 inserts the LIVE
    # authorization gate at section 4.5 — it MUST remain the hard control
    # gate and run BEFORE this client-state preflight. The final order is:
    #   1. master_control.evaluate()        (existing)
    #   2. live authorization gate          (PR #87, section 4.5)
    #   3. client-state preflight           (PR #81, section 4.6, OBSERVE ONLY)
    #   4. order_state_machine.create_entry_order(plan, execution_mode=...)
    # CLIENT_PREFLIGHT_ENFORCE defaults to false; preflight never weakens
    # the authorization gate.
    #
    # Runs preflight snapshot on every CLIENT_ELIGIBLE signal.
    #
    # Amendment 1+2: two distinct modes
    #   CLIENT_PREFLIGHT_ENFORCE=false (default) — OBSERVE ONLY:
    #     - build_client_trade_preflight() runs and writes full snapshot to ledger
    #     - if preflight fails → PREFLIGHT_WARNING + execution_continued=True
    #     - DO NOT call mark_skipped(); DO NOT return; DO NOT reject the job
    #     - existing order creation path continues unchanged
    #   CLIENT_PREFLIGHT_ENFORCE=true — ENFORCE:
    #     - if preflight fails → CLIENT_SKIPPED + execution_continued=False
    #     - block before order creation
    _pf_enforce = str(os.getenv("CLIENT_PREFLIGHT_ENFORCE", "false")).strip().lower() in ("true", "1")
    _pf_eligible = True   # default: allow through unless enforce=True + failed
    try:
        from ap.client_preflight import build_client_trade_preflight
        from ap.opportunity_ledger import (
            update_opportunity, mark_skipped,
            STAGE_CLIENT_PREFLIGHT, PREFLIGHT_PASSED, PREFLIGHT_WARNING, CLIENT_SKIPPED,
        )
        _preflight = build_client_trade_preflight(client_id, payload, plan)

        if _preflight.eligible:
            # Preflight passed — record PREFLIGHT_PASSED (Amendment 3: not ORDER_CREATED)
            _pf_status           = PREFLIGHT_PASSED
            _pf_enforced_flag    = _pf_enforce
            _pf_exec_continued   = True
            _pf_would_block      = None
        elif not _pf_enforce:
            # Amendment 1+2: observe only — PREFLIGHT_WARNING, execution continues
            _pf_status           = PREFLIGHT_WARNING
            _pf_enforced_flag    = False
            _pf_exec_continued   = True
            _pf_would_block      = str(_preflight.block_reason or "unknown_preflight_block")
        else:
            # Amendment 2: enforcement — CLIENT_SKIPPED, execution blocked
            _pf_status           = CLIENT_SKIPPED
            _pf_enforced_flag    = True
            _pf_exec_continued   = False
            _pf_would_block      = str(_preflight.block_reason or "unknown_preflight_block")

        # Amendment 5: named log tag on failure — never silent
        _ol_ok = update_opportunity(
            signal_id, client_id,
            _pf_status,
            canonical_signal_id=_canonical_signal_id,
            kill_switch_state=_preflight.kill_switch,
            entries_paused_state=_preflight.entries_paused,
            buying_power_snapshot=_preflight.buying_power,
            cap_snapshot={
                "daily_count":    _preflight.daily_trade_count,
                "lane_count":     _preflight.daily_lane_count,
                "open_positions": _preflight.open_positions_count,
                "pending_entries":_preflight.pending_entries_count,
            },
            # Amendment 2: persist enforcement context on every preflight write
            preflight_enforced=_pf_enforced_flag,
            execution_continued=_pf_exec_continued,
            would_block_reason=_pf_would_block,
            # Amendment 6: full snapshot including enforcement context
            extra_meta={"preflight": _preflight.to_dict(
                preflight_enforced=_pf_enforced_flag,
                execution_continued=_pf_exec_continued,
            )},
        )
        if not _ol_ok:
            log.warning(
                "[%s] CLIENT_OPPORTUNITY_LEDGER_WRITE_FAILED | "
                "stage=preflight_status client=%s signal=%s canonical=%s status=%s",
                ticker, client_id, signal_id, _canonical_signal_id, _pf_status,
            )

        if not _preflight.eligible:
            log.info(
                "[%s] PREFLIGHT %s | client=%s reason=%s enforce=%s",
                ticker,
                "BLOCK" if _pf_enforce else "WARN",
                client_id, _preflight.block_reason, _pf_enforce,
            )
            if _pf_enforce:
                # Enforcement mode: mark CLIENT_SKIPPED and block execution
                _sk_ok = mark_skipped(
                    signal_id, client_id,
                    STAGE_CLIENT_PREFLIGHT,
                    str(_preflight.block_reason or "unknown_preflight_block"),
                    canonical_signal_id=_canonical_signal_id,
                )
                if not _sk_ok:
                    log.warning(
                        "[%s] CLIENT_OPPORTUNITY_LEDGER_WRITE_FAILED | "
                        "stage=mark_skipped client=%s signal=%s",
                        ticker, client_id, signal_id,
                    )
                _mark_job(job_id, "REJECTED",
                          error=f"preflight_enforced:{_preflight.block_reason}")
                return
            # Observe mode: PREFLIGHT_WARNING written above; execution continues
            _pf_eligible = False  # captured for metrics; does NOT stop execution
    except ImportError:
        pass   # preflight module optional — skip silently
    except Exception as _pf_err:
        # Amendment §6: preflight failure is non-blocking. The duplicate
        # except clause was unreachable dead code — removed.
        log.warning("[%s] preflight check failed (non-blocking): %s", ticker, _pf_err)

    # ── 5. ORDER STATE MACHINE (create only after route + cutoff cleared) ────
    try:
        from ap.authorization import execution_mode_for_broker
        _entry_exec_mode = execution_mode_for_broker(broker)
        local_order_id = order_state_machine.create_entry_order(
            plan, execution_mode=_entry_exec_mode,
        )
        # PR E / FIX-1 (BUG-MC-1): stash local_order_id on plan.metadata
        # so any LATER revalidate_exposure (called from execution-core at
        # breach time, when the OSM row already exists) can pass it as
        # exclude_local_order_id to the pending-capital SUM, preventing
        # the current plan's reserved_cost from being double-counted.
        try:
            if hasattr(plan, "metadata") and isinstance(plan.metadata, dict):
                plan.metadata["local_order_id"] = str(local_order_id)
                # PR #182: stash job_id so breach-time write-back can resolve the
                # trade_queue row. Must happen before entry_watcher.watch() so
                # watch() can carry it into signal_dict → watched.signal → sig
                # inside _on_entry_trigger(). Without this, sig.get("queue_id")
                # returns None and write_deferred_breach_last_error() no-ops.
                plan.metadata["queue_id"] = job_id
                plan.metadata["trade_queue_id"] = job_id
        except Exception:
            pass
        log.info(
            f"[{ticker}] Entry order created: {local_order_id} "
            f"contract={getattr(plan, 'contract_symbol', '?')}"
        )
        try:
            _queued_logged = _log_signal_to_db(
                signal_id=signal_id,
                client_id=client_id,
                ticker=ticker,
                side=payload.get("side", ""),
                score=float(payload.get("score") or 0),
                stage="order_creation",
                reason_code="approved_for_execution",
                human_reason="Master control approved and entry order created.",
                payload=payload,
                decision_status="queued",
                queued_at=datetime.now(timezone.utc).isoformat(),
            )
            if not _queued_logged:
                # PR #225 amendment: order creation already succeeded — that
                # remains valid. But the ap_signals queued write failed, so
                # this is the ONLY trace an operator gets to find the real
                # order. Every field needed to locate it must be present.
                log.error(
                    "QUEUED_SIGNAL_WRITE_FAILED_AFTER_ORDER_CREATE "
                    "signal_id=%s client_id=%s local_order_id=%s ticker=%s "
                    "execution_mode=%s reason_code=QUEUED_SIGNAL_WRITE_FAILED_AFTER_ORDER_CREATE",
                    signal_id, client_id, local_order_id, ticker, _execution_mode,
                )
        except Exception as _queued_exc:
            # PR #225 amendment: same fields required on the exception path —
            # an exception here must not produce a diagnostic with less
            # information than the plain-failure branch above.
            log.error(
                "QUEUED_SIGNAL_WRITE_FAILED_AFTER_ORDER_CREATE "
                "signal_id=%s client_id=%s local_order_id=%s ticker=%s "
                "execution_mode=%s reason_code=QUEUED_SIGNAL_WRITE_FAILED_AFTER_ORDER_CREATE "
                "error=%s",
                signal_id, client_id, local_order_id, ticker, _execution_mode, _queued_exc,
            )
        # PR1: ORDER_CREATED — only mark AFTER create_entry_order() returns
        # local_order_id. Status was PREFLIGHT_PASSED before this point.
        try:
            from ap.opportunity_ledger import update_opportunity, ORDER_CREATED
            update_opportunity(signal_id, client_id, ORDER_CREATED,
                               canonical_signal_id=_canonical_signal_id,
                               order_local_id=str(local_order_id))
        except Exception: pass
    except Exception as e:
        log.error(f"[{ticker}] create_entry_order() failed: {e}")
        # Amendment §7: persist the failure to the opportunity ledger so the
        # row does not remain permanently at PREFLIGHT_PASSED.
        try:
            from ap.opportunity_ledger import (
                update_opportunity, MISSED, STAGE_ORDER_CREATION,
            )
            update_opportunity(
                signal_id, client_id, MISSED,
                canonical_signal_id=_canonical_signal_id,
                miss_stage=STAGE_ORDER_CREATION,
                miss_reason=f"order_create_error:{e}",
            )
        except Exception: pass
        _mark_job(job_id, "ERROR", error=f"order_create_error: {e}")
        return

    if trigger_type == "breach":
        try:
            # Move the local ENTRY order into explicit watcher-owned state before
            # arming. This proves the queue/OSM contract early and prevents a
            # fake WATCHING job when the OSM cannot represent PENDING_TRIGGER.
            try:
                if hasattr(order_state_machine, "mark_entry_pending_trigger"):
                    pending_ok = bool(order_state_machine.mark_entry_pending_trigger(local_order_id))
                else:
                    pending_ok = bool(order_state_machine.transition(
                        local_order_id, "PENDING_TRIGGER", submitted_ts=None,
                    ))
            except Exception as _st:
                log.error(
                    "[%s] PENDING_TRIGGER transition failed before watcher arm: %s",
                    ticker, _st, exc_info=True,
                )
                pending_ok = False

            if not pending_ok:
                _handoff_reason = "pending_trigger_transition_failed"
                _cleanup_method = "not_attempted"
                _cleanup_ok = False
                try:
                    _expire_fn = getattr(order_state_machine, "expire_pending_entry", None)
                    if callable(_expire_fn):
                        _cleanup_method = "expire_pending_entry"
                        _cleanup_ok = bool(_expire_fn(local_order_id, reason=_handoff_reason))

                    if not _cleanup_ok:
                        _cancel_fn = getattr(order_state_machine, "cancel_pending_entry", None)
                        if callable(_cancel_fn):
                            _cleanup_method = "cancel_pending_entry"
                            _cleanup_ok = bool(_cancel_fn(local_order_id, reason=_handoff_reason))

                    if not _cleanup_ok:
                        _cleanup_method = "transition_ERROR"
                        _cleanup_ok = bool(
                            order_state_machine.transition(
                                local_order_id, "ERROR", last_error=_handoff_reason,
                            )
                        )
                except Exception as _cleanup_exc:
                    log.error(
                        "[%s] WATCH_ARM_ABORTED cleanup failed | local=%s method=%s error=%s",
                        ticker, local_order_id, _cleanup_method, _cleanup_exc, exc_info=True,
                    )

                log.critical(
                    "[%s] WATCH_ARM_ABORTED_PENDING_TRIGGER_TRANSITION_FAILED | local=%s "
                    "| cleanup_method=%s cleanup_ok=%s",
                    ticker, local_order_id, _cleanup_method, _cleanup_ok,
                )
                _mark_job(job_id, "ERROR", error=_handoff_reason)
                return

            # Register with watcher — no broker submit yet. The watcher returns
            # False for stale signals, dedup blocks, opposite-side conflicts,
            # and failed OSM local-order validation. Do not mark WATCHING unless
            # this returns True.
            armed = bool(entry_watcher.watch(plan=plan, local_order_id=local_order_id))
            if not armed:
                # Pull the specific reason the watcher set before returning False
                _reject_reason = getattr(entry_watcher, "_last_reject_reason", None) or "unknown"
                _full_error    = f"watch_arm_failed:{_reject_reason}"

                log.warning(
                    "[%s] WATCH_ARM_FAILED | local=%s | reason=%s",
                    ticker, local_order_id, _reject_reason,
                )

                # =============================================================
                # PR #92 — Watcher Arm Audit (additive; cannot block cleanup)
                # =============================================================
                # Classify the raw_reason into a structured taxonomy and merge
                # a dossier-shaped audit into orders.meta.watcher_arm_audit.
                # Every step is wrapped so a failure here NEVER affects the
                # existing arm-failure cleanup path below.
                #
                # We do NOT wire the retry's arm_callable to entry_watcher.watch
                # in this PR — the retry module is plumbing only. Once we have
                # several sessions of audit data confirming the taxonomy, a
                # follow-up PR can hook the actual re-arm path.
                # =============================================================
                try:
                    from ap_watcher_arm_audit import (
                        classify_arm_failure, build_watcher_arm_audit,
                        BUCKET_QUOTE_READINESS_UNKNOWN,
                    )
                    from ap_watcher_arm_persist import persist_watcher_arm_audit
                    _pr92_classification = classify_arm_failure(_reject_reason)
                    _pr92_pod_id = (
                        getattr(plan, "pod_id", None)
                        or os.environ.get("POD_ID")
                        or None
                    )
                    # ── PR92 amendment: explicit quote snapshot coverage ──────
                    # No new quote fetches. Extract best-available quote from
                    # plan / watcher result / order meta / last reject context.
                    # If none available, flag explicitly — never fake values.
                    _pr92_underlying_q = (
                        getattr(plan, "underlying_quote", None)
                        or getattr(plan, "last_underlying_quote", None)
                        or (getattr(plan, "metadata", None) or {}).get("underlying_quote")
                        or (getattr(plan, "metadata", None) or {}).get("quote_snapshot", {}).get("underlying")
                    )
                    _pr92_option_q = (
                        getattr(plan, "option_quote", None)
                        or getattr(plan, "last_option_quote", None)
                        or (getattr(plan, "metadata", None) or {}).get("option_quote")
                        or (getattr(plan, "metadata", None) or {}).get("quote_snapshot", {}).get("option")
                    )
                    _pr92_quote_src = (
                        getattr(plan, "quote_source", None)
                        or (getattr(plan, "metadata", None) or {}).get("quote_source")
                    )
                    _pr92_snap_available = bool(_pr92_underlying_q or _pr92_option_q)
                    _pr92_audit = build_watcher_arm_audit(
                        classification=_pr92_classification,
                        symbol=ticker,
                        direction=getattr(plan, "direction", None),
                        timeframe=getattr(plan, "timeframe", None),
                        pattern=getattr(plan, "pattern", None),
                        signal_id=str(signal_id) if signal_id else None,
                        canonical_signal_id=_canonical_signal_id,
                        plan_id=getattr(plan, "plan_id", None),
                        client_id=str(client_id) if client_id else None,
                        pod_id=_pr92_pod_id,
                        local_order_id=str(local_order_id) if local_order_id else None,
                        trigger_price=getattr(plan, "trigger_price", None),
                        stop_price=getattr(plan, "stop_price", None),
                        target_price=getattr(plan, "target_price", None),
                        # Quote snapshot — pass what we found; None means truly absent
                        underlying=_pr92_underlying_q,
                        option=_pr92_option_q,
                        quote_source=_pr92_quote_src,
                        # Explicit coverage flags
                        quote_snapshot_available=_pr92_snap_available,
                        underlying_quote_available=bool(_pr92_underlying_q),
                        option_quote_available=bool(_pr92_option_q),
                        quote_snapshot_missing_reason=(
                            None if _pr92_snap_available
                            else "not_available_in_queue_arm_failure_context"
                        ),
                    )
                    log.info(
                        "WATCHER_ARM_AUDIT signal_id=%s canonical_signal_id=%s "
                        "client_id=%s symbol=%s direction=%s conceptual_bucket=%s "
                        "reason_code=%s final_decision=%s",
                        signal_id, _canonical_signal_id, client_id, ticker,
                        getattr(plan, "direction", None),
                        _pr92_classification.bucket,
                        _pr92_classification.reason_code,
                        _pr92_audit.get("final_decision"),
                    )
                    try:
                        persist_watcher_arm_audit(
                            local_order_id=local_order_id,
                            audit=_pr92_audit,
                        )
                    except Exception as _persist_exc:
                        log.warning(
                            "[%s] watcher_arm_audit persist skipped: %s",
                            ticker, _persist_exc,
                        )
                except Exception as _pr92_exc:
                    # PR92 is reporting-only — never let it break arm cleanup.
                    log.warning(
                        "[%s] watcher_arm_audit build skipped: %s",
                        ticker, _pr92_exc,
                    )
                # =============================================================

                # Amendment §3: persist watcher invalidation to the ledger.
                try:
                    from ap.opportunity_ledger import mark_watcher_invalidated
                    mark_watcher_invalidated(
                        signal_id, client_id, _reject_reason,
                        canonical_signal_id=_canonical_signal_id,
                        order_local_id=str(local_order_id),
                    )
                except Exception: pass
                try:
                    # Persist exact reason to orders.last_error so dashboard shows it
                    if hasattr(order_state_machine, "expire_pending_entry"):
                        order_state_machine.expire_pending_entry(local_order_id, reason=_full_error)
                    elif hasattr(order_state_machine, "cancel_pending_entry"):
                        order_state_machine.cancel_pending_entry(local_order_id, reason=_full_error)
                    else:
                        order_state_machine.transition(local_order_id, "EXPIRED", last_error=_full_error)
                except Exception as _cleanup_exc:
                    log.error(
                        "[%s] Failed to cleanup unarmed pending entry %s: %s",
                        ticker, local_order_id, _cleanup_exc, exc_info=True,
                    )
                _mark_job(job_id, "REJECTED", error=_full_error)
                return

            log.info(
                f"[{ticker}] Handed to entry watcher | "
                f"trigger=${getattr(plan, 'trigger_price', '?')}"
            )
            # Amendment §3: WATCHER_ARMED ledger update.
            try:
                from ap.opportunity_ledger import mark_watcher_armed
                mark_watcher_armed(
                    signal_id, client_id,
                    canonical_signal_id=_canonical_signal_id,
                    order_local_id=str(local_order_id),
                )
            except Exception: pass

            # Queue job = WATCHING only after watcher arm succeeded.
            _mark_job(job_id, "WATCHING",
                      result={"plan_id": plan.plan_id,
                              "local_order_id": local_order_id,
                              "contract": getattr(plan, "contract_symbol", ""),
                              "real_cost": plan.max_position_usd,
                              "trigger_type": "breach",
                              "needs_contract_selection": bool(getattr(plan, "_needs_contract_selection", False))})
        except Exception as e:
            log.error(f"[{ticker}] entry_watcher.watch() failed: {e}", exc_info=True)
            try:
                if hasattr(order_state_machine, "expire_pending_entry"):
                    order_state_machine.expire_pending_entry(local_order_id, reason=f"watcher_error:{e}")
                elif hasattr(order_state_machine, "cancel_pending_entry"):
                    order_state_machine.cancel_pending_entry(local_order_id, reason=f"watcher_error:{e}")
                else:
                    order_state_machine.transition(local_order_id, "ERROR", last_error=f"watcher_error:{e}")
            except Exception as _cancel_err:
                log.error("[%s] Failed to cancel/transition order after watcher error: %s", ticker, _cancel_err)
            # Amendment §3 + §7: persist INTERNAL_ERROR to the ledger.
            try:
                from ap.opportunity_ledger import mark_internal_error
                mark_internal_error(
                    signal_id, client_id, f"watcher_error:{e}",
                    canonical_signal_id=_canonical_signal_id,
                    order_local_id=str(local_order_id) if 'local_order_id' in dir() else None,
                )
            except Exception: pass
            _mark_job(job_id, "ERROR", error=f"watcher_error: {e}")
    else:
        log.warning(
            "[%s] IMMEDIATE EXECUTION PATH ENABLED — this should only run when ALLOW_IMMEDIATE_EXECUTION=1",
            ticker,
        )

        # Money-safe immediate path:
        # Never mark SUBMITTED before broker acceptance. We submit the existing
        # CREATED order through OSM, and OSM transitions to SUBMITTED only after
        # broker accepts and returns broker_order_id.
        try:
            if not hasattr(order_state_machine, "submit_existing_entry"):
                raise RuntimeError("order_state_machine_missing_submit_existing_entry")

            submit_res = order_state_machine.submit_existing_entry(
                local_order_id=local_order_id,
                broker=broker,
                plan=plan,
                limit_price=getattr(plan, "limit_price", None),
            )

            # WIRE-3: split-brain — broker accepted but OSM DB transition failed
            if submit_res.get("split_brain") and callable(on_split_brain):
                try:
                    on_split_brain(
                        local_order_id=submit_res.get("local_order_id", ""),
                        broker_order_id=submit_res.get("broker_order_id", ""),
                    )
                except Exception as _sb_err:
                    log.error("[%s] split_brain callback failed: %s", ticker, _sb_err)

            if not submit_res.get("ok"):
                log.error(
                    "[%s] Immediate submit failed safely | local=%s error=%s",
                    ticker, local_order_id, submit_res.get("error"),
                )
                # Amendment §3: persist broker-submit failure to ledger.
                try:
                    from ap.opportunity_ledger import (
                        update_opportunity, MISSED, STAGE_BROKER_SUBMIT,
                    )
                    update_opportunity(
                        signal_id, client_id, MISSED,
                        canonical_signal_id=_canonical_signal_id,
                        miss_stage=STAGE_BROKER_SUBMIT,
                        miss_reason=str(submit_res.get("error") or "submit_failed"),
                        order_local_id=str(local_order_id),
                    )
                except Exception: pass
                _mark_job(job_id, "ERROR", error=f"submit_error:{submit_res.get('error')}")
                return

            log.info(
                "[%s] Order submitted immediately after broker acceptance | local=%s broker=%s",
                ticker, local_order_id, submit_res.get("broker_order_id"),
            )
            # Amendment §3: BROKER_SUBMITTED ledger update.
            try:
                from ap.opportunity_ledger import mark_broker_submitted
                mark_broker_submitted(
                    signal_id, client_id,
                    canonical_signal_id=_canonical_signal_id,
                    order_local_id=str(local_order_id),
                    broker_order_id=str(submit_res.get("broker_order_id") or ""),
                )
            except Exception: pass

            _mark_job(job_id, "SUBMITTED",
                      result={"plan_id": plan.plan_id,
                              "local_order_id": local_order_id,
                              "broker_order_id": submit_res.get("broker_order_id"),
                              "contract": getattr(plan, "contract_symbol", ""),
                              "real_cost": plan.max_position_usd,
                              "trigger_type": "immediate"})
            return
        except Exception as e:
            log.error(f"[{ticker}] immediate submit failed: {e}", exc_info=True)
            # Amendment §3 + §7: persist INTERNAL_ERROR to the ledger.
            try:
                from ap.opportunity_ledger import mark_internal_error
                mark_internal_error(
                    signal_id, client_id, f"submit_error:{e}",
                    canonical_signal_id=_canonical_signal_id,
                    order_local_id=str(local_order_id) if 'local_order_id' in dir() else None,
                )
            except Exception: pass
            _mark_job(job_id, "ERROR", error=f"submit_error: {e}")


# =============================================================================
# WORKER LOOP
# =============================================================================

def worker_loop(
    broker,
    poll_seconds: float = POLL_INTERVAL,
    *,
    master_control=None,
    contract_selector=None,
    order_state_machine=None,
    entry_watcher=None,
    position_manager=None,
    exit_eng=None,
    client_id: str = "default",
    stop_event=None,
    live_mode: bool = False,
    on_split_brain=None,
):
    """
    Main queue worker.
    stop_event: threading.Event — set by ClientRunner.stop() to cleanly exit.
    live_mode:  if True, legacy process_signal() fallback is disabled entirely.
    on_split_brain: optional callback(local_order_id, broker_order_id) fired when
                    a submit returns split_brain=True (broker accepted, OSM DB failed).
    """
    _init_db()()
    mode_label = "control" if master_control else ("LIVE-NO-FALLBACK" if live_mode else "legacy")
    log.info(
        f"🤖 Worker started | client={client_id} poll={poll_seconds}s path={mode_label}"
    )

    # Live authorization gate mode — surface on every worker start so operators
    # can confirm whether the gate is BLOCKING (enforce) or only OBSERVING.
    try:
        from ap.authorization import authorization_gate_enforced
        _gate_enforced = authorization_gate_enforced()
        log.info(
            "[%s] LIVE_AUTHORIZATION_GATE mode=%s (LIVE_AUTHORIZATION_GATE_ENFORCE=%s) — %s",
            client_id,
            "ENFORCE" if _gate_enforced else "OBSERVE",
            os.getenv("LIVE_AUTHORIZATION_GATE_ENFORCE", "false"),
            "unauthorized/unknown-mode new live entries are BLOCKED"
            if _gate_enforced else
            "unauthorized/unknown-mode new live entries are LOGGED ONLY (allowed)",
        )
    except Exception as _gate_log_exc:
        log.warning("[%s] could not read live authorization gate mode: %s", client_id, _gate_log_exc)

    # Schema guard: confirm proof_trades.execution_mode exists before the proof
    # logger starts inserting it. Logs CRITICAL (does not crash) if missing so
    # live-only attribution cannot silently degrade unnoticed.
    try:
        from ap_proof_logger import ensure_proof_trades_schema
        ensure_proof_trades_schema(_get_sb_client())
    except Exception as _schema_exc:
        log.warning("[%s] proof_trades schema guard could not run: %s", client_id, _schema_exc)

    # PR F / queue truth hardening: re-emit the immediate-execution override
    # warning on worker startup. Module-import critical fires once per process;
    # this fires every worker start (per client), so operators attaching after
    # process launch still see the dangerous override in their logs.
    if ALLOW_IMMEDIATE_EXECUTION:
        log.critical(
            "[%s] ⚠️  ALLOW_IMMEDIATE_EXECUTION=1 at worker startup — "
            "IMMEDIATE EXECUTION PATH ENABLED. Breach-watch is BYPASSED; "
            "orders submit instantly on dispatch. Production must set "
            "ALLOW_IMMEDIATE_EXECUTION=0.",
            client_id,
        )

    # Log restart guard status on startup
    try:
        from ap.restart_guard import log_startup
        log_startup()
    except Exception:
        pass

    while True:
        if stop_event and stop_event.is_set():
            log.info(f"[{client_id}] Worker stop_event set -- exiting cleanly")
            return

        try:
            update_state({"last_heartbeat_ts": now_utc_iso()}, client_id=client_id)
        except Exception as _e:
            log.debug("queue_update_state_heartbeat_failed: %s", _e)
        try:
            from ap.self_healing import get_healer
            h = get_healer()
            if h:
                h.heartbeat(client_id, "worker")
        except Exception as _e:
            log.debug("queue_healer_heartbeat_failed: %s", _e)

        job    = None
        job_id = None

        try:
            job = _claim_one_job(client_id=client_id)
            if not job:
                time.sleep(poll_seconds)
                continue

            # Successful claim — reset consecutive failure counter
            _claim_fail_count[client_id] = 0

            job_id    = int(job["id"])
            job_cid   = job["client_id"]
            signal_id = job["signal_id"]

            try:
                payload = _parse_payload(job["payload"])
            except Exception as e:
                log.error(f"Payload parse error: {e}")
                _mark_job(job_id, "REJECTED", error=f"parse_error: {e}")
                continue

            if master_control is not None:
                _job_result = None
                if isinstance(job.get("result_json"), dict):
                    _job_result = dict(job.get("result_json") or {})
                _dispatch(
                    job_id, job_cid, signal_id, payload,
                    job_last_error=job.get("last_error"),
                    job_result=_job_result,
                    master_control=master_control,
                    contract_selector=contract_selector,
                    order_state_machine=order_state_machine,
                    entry_watcher=entry_watcher,
                    position_manager=position_manager,
                    exit_eng=exit_eng,
                    broker=broker,
                    on_split_brain=on_split_brain,
                )
            else:
                log.error(
                    f"[{client_id}] master_control not available — "
                    f"rejecting {signal_id}. Check runner startup logs."
                )
                _mark_job(job_id, "REJECTED",
                          error="master_control_required_not_initialized")

        except Exception as e:
            log.error(f"Worker loop error: {e}", exc_info=True)
            if job_id is not None:
                try:
                    _mark_job(job_id, "ERROR", error=str(e))
                except Exception:
                    pass
            # ── CLAIM FAILURE ALERTING ────────────────────────────────────
            # Track consecutive failures. If the worker fails repeatedly
            # without claiming any job, surface it as a CRITICAL log and
            # update the health registry so the dashboard shows it.
            # This catches the JSONB ? bug class (or any future claim crash)
            # within minutes instead of days.
            _claim_fail_count[client_id] = _claim_fail_count.get(client_id, 0) + 1
            _fail_count = _claim_fail_count[client_id]
            if _fail_count in (3, 10, 25) or _fail_count % 50 == 0:
                log.critical(
                    "[QUEUE_WORKER_DEGRADED] client=%s consecutive_failures=%d "
                    "last_error=%s — queue is NOT draining. "
                    "Check Render logs immediately.",
                    client_id, _fail_count, str(e)[:120],
                )
                try:
                    h = health_registry.get(client_id)
                    if h:
                        h.mark_degraded(
                            f"queue_worker_consecutive_failures:{_fail_count}",
                            f"last_error={str(e)[:80]}",
                        )
                except Exception:
                    pass
            time.sleep(poll_seconds)
