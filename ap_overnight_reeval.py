"""
ap_overnight_reeval.py — Overnight Daily Signal Re-Evaluation Engine
=====================================================================
This is the missing piece of the full trading loop.

FLOW:
  1. Market close → scanner runs → signals arrive with timeframe=1d
  2. Bot marks them WATCHING (audit record, not yet armed)
  3. *** THIS MODULE *** runs during the 9:00–9:45 AM ET pre-open/open handoff window
  4. For each WATCHING signal:
     a. Fetch prior-day high/low from Tradier history
     b. Run overnight_daily_validator (directional invalidation check)
     c. If VALID: create deferred OSM entry order and arm entry_watcher
     d. If INVALID: mark REJECTED with reason code, log to ap_signals
  5. After 9:30 AM ET open: entry_watcher polls quotes, waits for breach
  6. On breach: on_trigger fires → OSM submits entry → fill monitor takes over

WHEN IT RUNS:
  - Called by client_runner's health loop at ~9:15 AM ET on trading days
  - Also callable via /admin/overnight_reeval for manual trigger
  - ONLY processes signals with date == yesterday (no stale signals)

ENTRY TRIGGER LOGIC:
  - If scanner provides entry_trigger: use it directly
  - If not: use prior_day_high (CALL) or prior_day_low (PUT) as the breach level
    This matches The Strat: we enter on prior-day boundary breach

FAIL-CLOSED:
  - Missing prior levels → REJECTED
  - Broker unavailable → skip (will retry on next poll)
  - No contract found → REJECTED with reason
"""
from __future__ import annotations

import logging
import os
import inspect
import time
import threading
import uuid
from datetime import date, datetime, timezone, timedelta
from typing import TYPE_CHECKING, NamedTuple, Optional

from ap_signal_store import canonical_client_email, canonical_signal_id, upsert_ap_signal_row_with_fallback

log = logging.getLogger("ap.overnight_reeval")

if TYPE_CHECKING:
    pass

# How many calendar days back a signal is still considered "fresh"
# e.g. a Friday signal is valid Monday morning = 3 days
OVERNIGHT_SIGNAL_MAX_AGE_DAYS = int(os.getenv("OVERNIGHT_SIGNAL_MAX_AGE_DAYS", "4"))

# OVERNIGHT_SNAPSHOT_FAIL_CLOSED: false (default) = snapshot unavailable
# before market open is DATA_NOT_READY, not a true invalidation.
# Signal stays WATCHING; retry at next reeval window.
_OVERNIGHT_SNAPSHOT_FAIL_CLOSED = (
    os.getenv("OVERNIGHT_SNAPSHOT_FAIL_CLOSED", "false").strip().lower()
    in ("true", "1")
)

# OVERNIGHT_REEVAL_SCORE_RECHECK_ENABLED=false (default):
# Already-WATCHING signals must not be hard-killed by a fresh morning
# second/intel score. Hard safety checks (capital, kill switch, client
# enabled, auth, hard risk veto, contract quality) STILL apply — only
# the intel/second-score block is treated as skip→RETRY_LATER.
_OVERNIGHT_REEVAL_SCORE_RECHECK_ENABLED = (
    os.getenv("OVERNIGHT_REEVAL_SCORE_RECHECK_ENABLED", "false").strip().lower()
    in ("true", "1")
)

try:
    OVERNIGHT_WATCH_ARM_MAX_ATTEMPTS = max(
        1,
        int(os.getenv("OVERNIGHT_WATCH_ARM_MAX_ATTEMPTS", "3")),
    )
except (TypeError, ValueError):
    OVERNIGHT_WATCH_ARM_MAX_ATTEMPTS = 3


# PR #404 final amendment: durable-claim lease for stranded IN_PROGRESS
# recovery. A process crash after the atomic claim write must not leave the
# scope permanently owned. A bounded lease timestamp is written alongside the
# IN_PROGRESS scope; a later worker may reclaim only after the lease has
# expired AND exact ownership identity of the referenced local ENTRY (if any)
# has been proven terminal. Minimum lease 30s; default 5 minutes.
try:
    OVERNIGHT_WATCH_ARM_CLAIM_LEASE_SECONDS = max(
        30,
        int(os.getenv("OVERNIGHT_WATCH_ARM_CLAIM_LEASE_SECONDS", "300")),
    )
except (TypeError, ValueError):
    OVERNIGHT_WATCH_ARM_CLAIM_LEASE_SECONDS = 300


def _parse_watch_attempt_ts(raw) -> Optional[datetime]:
    """Parse a watcher-attempt lease timestamp to a timezone-aware UTC
    datetime. Accepts datetime objects, ISO strings with optional trailing
    ``Z``. Returns None for blank / malformed / unsupported values; never
    raises. Naive datetimes are treated as UTC."""
    if raw is None:
        return None
    try:
        if isinstance(raw, datetime):
            value = raw
        else:
            text = str(raw).strip()
            if not text:
                return None
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            value = datetime.fromisoformat(text)
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    except Exception:
        return None


# ── PR #404: Watcher-arm outcome classification ───────────────────────────────

WATCH_ARMED         = "WATCH_ARMED"
ALREADY_WATCHING    = "ALREADY_WATCHING"
RETRYABLE_NOT_ARMED = "RETRYABLE_NOT_ARMED"
TERMINAL_FAILURE    = "TERMINAL_FAILURE"
OWNERSHIP_CONFLICT  = "OWNERSHIP_CONFLICT"
UNKNOWN_FAILURE     = "UNKNOWN_FAILURE"

# Watcher ownership resolution constants (used by _resolve_existing_watcher_ownership)
WATCH_OWNER_EXACT        = "WATCH_OWNER_EXACT"
WATCH_OWNER_CONFLICT     = "WATCH_OWNER_CONFLICT"
WATCH_OWNER_MISSING      = "WATCH_OWNER_MISSING"
WATCH_OWNER_LOOKUP_ERROR = "WATCH_OWNER_LOOKUP_ERROR"

WATCH_ATTEMPT_ACQUIRED            = "WATCH_ATTEMPT_ACQUIRED"
WATCH_ATTEMPT_ALREADY_IN_PROGRESS = "WATCH_ATTEMPT_ALREADY_IN_PROGRESS"
WATCH_ATTEMPT_ALREADY_ARMED       = "WATCH_ATTEMPT_ALREADY_ARMED"
WATCH_ATTEMPT_EXHAUSTED           = "WATCH_ATTEMPT_EXHAUSTED"
WATCH_ATTEMPT_CONFLICT            = "WATCH_ATTEMPT_CONFLICT"
WATCH_ATTEMPT_DB_ERROR            = "WATCH_ATTEMPT_DB_ERROR"
# Returned when an expired IN_PROGRESS claim with blank local_order_id discovers
# exactly one active PENDING_TRIGGER / CREATED ENTRY in orders.  The claim
# function has already (a) rotated the token, (b) bound the discovered
# local_order_id to the scope, and (c) refreshed the claim lease — all under
# the row lock.  The caller MUST invoke the existing watcher reattachment path
# against the discovered order; it MUST NOT create a new order or submit to the
# broker.  On successful reattachment the caller completes the attempt as ARMED.
WATCH_ATTEMPT_REATTACH_REQUIRED   = "WATCH_ATTEMPT_REATTACH_REQUIRED"

WATCH_ATTEMPT_STATE_IN_PROGRESS = "IN_PROGRESS"
WATCH_ATTEMPT_STATE_RETRYABLE   = "RETRYABLE"
WATCH_ATTEMPT_STATE_ARMED       = "ARMED"
WATCH_ATTEMPT_STATE_EXHAUSTED   = "EXHAUSTED"
WATCH_ATTEMPT_STATE_ERROR       = "ERROR"
_WATCH_ATTEMPT_TERMINAL_STATES = {
    WATCH_ATTEMPT_STATE_ARMED,
    WATCH_ATTEMPT_STATE_EXHAUSTED,
    WATCH_ATTEMPT_STATE_ERROR,
}
_WATCH_ATTEMPT_LOCK = threading.Lock()


class _WatchAttemptClaim(NamedTuple):
    disposition: str
    token: str = ""
    attempt_count: int = 0
    local_order_id: str = ""
    reason: str = ""


class _WatchArmOutcome(NamedTuple):
    """Structured watcher-arm result replacing the raw Boolean."""
    disposition: str   # one of the WATCH_*/RETRYABLE_NOT_ARMED/TERMINAL_FAILURE constants
    terminal:    bool  # True only when signal is proven permanently invalid
    retryable:   bool  # True when the slot can be retried
    reason:      str


def _classify_watch_arm_outcome(
    watch_result: bool,
    already_watching: bool,
    terminal_conflict: bool,
    exception: Optional[Exception],
) -> _WatchArmOutcome:
    """Classify watch() result.  Generic False is NEVER TERMINAL_FAILURE."""
    if watch_result:
        return _WatchArmOutcome(WATCH_ARMED, False, False, "watch_returned_true")
    if already_watching:
        return _WatchArmOutcome(ALREADY_WATCHING, False, True, "already_watching_exact_owner")
    if terminal_conflict:
        return _WatchArmOutcome(TERMINAL_FAILURE, True, False, "proven_terminal_conflict")
    if exception is not None:
        return _WatchArmOutcome(
            RETRYABLE_NOT_ARMED, False, True,
            f"transient_exception:{type(exception).__name__}",
        )
    return _WatchArmOutcome(RETRYABLE_NOT_ARMED, False, True, "watch_false_no_evidence")


def _resolve_existing_watcher_ownership(
    *,
    entry_watcher,
    signal_id: str,
    client_id: str,
    execution_mode: str,
    ticker: str,
    side: str,
    generation=None,
    watcher_token=None,
    local_order_id=None,
) -> str:
    """Resolve watcher ownership for a signal_id after a dedup_block rejection.

    Iterates entry_watcher._pending (same pattern as _verify_registry_ownership
    in ap/pending_trigger_restart_recovery.py) and verifies all available
    identity fields.  Never returns a Boolean.

    Returns:
        WATCH_OWNER_EXACT        – all identity fields match; idempotent success
        WATCH_OWNER_CONFLICT     – watcher exists but identity mismatches
        WATCH_OWNER_MISSING      – dedup key held but no matching watcher in registry
        WATCH_OWNER_LOOKUP_ERROR – registry access error or incomplete expected identity
    """
    if entry_watcher is None:
        return WATCH_OWNER_LOOKUP_ERROR

    exp_sid    = str(signal_id      or "").strip()
    exp_client = str(client_id      or "").strip().lower()
    exp_mode   = str(execution_mode or "").strip().lower()
    exp_ticker = str(ticker         or "").upper().strip()
    exp_side   = str(side           or "").upper().strip()

    if not exp_sid or not exp_client or not exp_mode or not exp_ticker or not exp_side:
        log.warning(
            "[%s] _resolve_existing_watcher_ownership: incomplete identity "
            "sid=%s client=%s mode=%s side=%s",
            ticker, exp_sid, exp_client, exp_mode, exp_side,
        )
        return WATCH_OWNER_LOOKUP_ERROR

    try:
        _dedup = getattr(entry_watcher, "_dedup_set", None)
        if _dedup is None or exp_sid not in _dedup:
            return WATCH_OWNER_MISSING
    except Exception:
        return WATCH_OWNER_LOOKUP_ERROR

    try:
        _lock    = getattr(entry_watcher, "_lock", None)
        _pending = list(getattr(entry_watcher, "_pending", []) or [])
    except Exception:
        return WATCH_OWNER_LOOKUP_ERROR

    import contextlib
    _ctx = _lock if _lock is not None else contextlib.nullcontext()

    try:
        with _ctx:
            _snapshot = list(getattr(entry_watcher, "_pending", []) or [])
        for w in _snapshot:
            _wsig  = getattr(w, "signal", {}) or {}
            w_sid  = str(getattr(w, "signal_id", None) or _wsig.get("signal_id") or "").strip()
            if w_sid != exp_sid:
                continue
            w_client = str(_wsig.get("client_id")      or "").strip().lower()
            w_mode   = str(_wsig.get("execution_mode") or "").strip().lower()
            w_ticker = str(getattr(w, "ticker", "") or _wsig.get("ticker") or "").upper().strip()
            w_side   = str(getattr(w, "side",   "") or _wsig.get("side")   or "").upper().strip()
            if not w_client or w_client != exp_client:
                log.warning("[%s] watcher client mismatch sid=%s exp=%s got=%r",
                            ticker, exp_sid, exp_client, w_client)
                return WATCH_OWNER_CONFLICT
            if not w_mode or w_mode != exp_mode:
                log.warning("[%s] watcher mode mismatch sid=%s exp=%s got=%r",
                            ticker, exp_sid, exp_mode, w_mode)
                return WATCH_OWNER_CONFLICT
            if w_ticker and w_ticker != exp_ticker:
                log.warning("[%s] watcher ticker mismatch sid=%s exp=%s got=%r",
                            ticker, exp_sid, exp_ticker, w_ticker)
                return WATCH_OWNER_CONFLICT
            if w_side and w_side != exp_side:
                log.warning("[%s] watcher side mismatch sid=%s exp=%s got=%r",
                            ticker, exp_sid, exp_side, w_side)
                return WATCH_OWNER_CONFLICT
            if local_order_id:
                exp_oid = str(local_order_id or "").strip()
                w_oid = str(_wsig.get("local_order_id") or "").strip()
                if not w_oid or w_oid != exp_oid:
                    log.warning("[%s] watcher local_order_id mismatch sid=%s exp=%s got=%r",
                                ticker, exp_sid, exp_oid, w_oid)
                    return WATCH_OWNER_CONFLICT
            if generation is not None:
                try:
                    if int(_wsig.get("trigger_generation") or 0) != int(generation):
                        return WATCH_OWNER_CONFLICT
                except (TypeError, ValueError):
                    return WATCH_OWNER_CONFLICT
            if watcher_token:
                w_tok = str(_wsig.get("watcher_token") or "").strip()
                if not w_tok or w_tok != str(watcher_token).strip():
                    return WATCH_OWNER_CONFLICT
            return WATCH_OWNER_EXACT
    except Exception as exc:
        log.error("[%s] _resolve_existing_watcher_ownership exception sid=%s: %s",
                  ticker, exp_sid, exc)
        return WATCH_OWNER_LOOKUP_ERROR

    return WATCH_OWNER_MISSING


# ── PR #404 P0-4: real production ownership fields (with lease freshness) ────
#
# The prior recovery-owned check only looked at four retry-timestamp fields.
# Live production orders carry ownership in a wider set of fields, and mere
# presence of a stale timestamp is not evidence of an active owner. The rule:
# a row is currently owned when it carries either
#   (a) any of the ownership-identity fields, OR
#   (b) an unexpired materialization lease, OR
#   (c) materialization_in_flight is truthy, OR
#   (d) materialization_status is 'RUNNING' or 'IN_PROGRESS'.
# All four are checked below.

_RECOVERY_OWNER_IDENTITY_FIELDS = (
    "materialization_owner",
    "recovery_owner",
    "recovery_ownership",
    "current_owner",
    "watcher_token",
    "watcher_retry_owner",
)

_ACTIVE_MATERIALIZATION_STATUSES = frozenset({"RUNNING", "IN_PROGRESS"})


def _parse_iso_ts(raw) -> Optional[datetime]:
    if not raw:
        return None
    try:
        s = str(raw).strip()
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _pending_owner_lease_active(
    meta: dict,
    *,
    expected_client_id: Optional[str] = None,
    expected_execution_mode: Optional[str] = None,
) -> tuple[bool, str]:
    """Return (owned, reason). A row is 'owned' only when the ownership
    evidence is real AND (for time-bounded fields) still fresh. Reason is
    the fact that produced the True/False decision for diagnostics."""
    if not isinstance(meta, dict):
        return False, "no_meta"

    # PRE_SUBMIT_PROOF_RETRY: real deferred lifecycle whose ownership must
    # be honored while the proof-retry deadline is still in the future. Uses
    # the SHARED predicate defined in ap/order_state_machine.py so this
    # helper and the OSM stale-expiration guard agree on ownership — no
    # parallel rule sets. Certain rejection reasons (expired deadline,
    # malformed shape, malformed durable-recovery owner) are STRONGER than
    # plain "unowned" — they must force the classifier to CONFLICT, not
    # allow it to fall through to stale. Those reason strings are surfaced
    # verbatim; the classifier consults LEASE_REASONS_FORCE_CONFLICT.
    try:
        from ap.order_state_machine import (
            is_proof_retry_owner_active,
            is_durable_recovery_owner_active,
            LEASE_REASONS_FORCE_CONFLICT,  # noqa: F401 imported for callers
        )
        _proof_active, _proof_reason = is_proof_retry_owner_active(meta)
        if _proof_active:
            return True, _proof_reason
        if _proof_reason in ("proof_retry_deadline_expired",
                             "proof_retry_next_at_after_deadline"):
            # Recognized proof-retry shape but recovery-consumer-owned or
            # malformed. Surface the reason so the classifier can escalate
            # to CONFLICT (not stale).
            return False, _proof_reason
        # Durable recovery_scheduler retention — separate canonical contract.
        _dr_active, _dr_reason = is_durable_recovery_owner_active(
            meta,
            expected_client_id=expected_client_id,
            expected_execution_mode=expected_execution_mode,
        )
        if _dr_active:
            return True, _dr_reason
        if _dr_reason in LEASE_REASONS_FORCE_CONFLICT:
            return False, _dr_reason
    except Exception as _pr_exc:
        log.warning(
            "pending_owner_lease_active: shared ownership predicate import/"
            "call failed err=%s — treating as no proof-retry evidence",
            _pr_exc,
        )

    if meta.get("materialization_in_flight"):
        lease = _parse_iso_ts(meta.get("materialization_lease_until"))
        if lease is None:
            # In-flight flag with no lease → treat as owned but flag it.
            return True, "materialization_in_flight_no_lease"
        if lease > datetime.now(timezone.utc):
            return True, "materialization_lease_fresh"
        # In-flight but lease expired → not owned; this row is a ghost.
        return False, "materialization_lease_expired"

    status = str(meta.get("materialization_status") or "").strip().upper()
    if status in _ACTIVE_MATERIALIZATION_STATUSES:
        lease = _parse_iso_ts(meta.get("materialization_lease_until"))
        if lease is None or lease > datetime.now(timezone.utc):
            return True, f"materialization_status:{status}"
        return False, f"materialization_status_{status}_lease_expired"

    # A fresh retry timestamp is a real liveness signal — accept it whether or
    # not an identity field is also set. This is the "living worker" evidence.
    for _field in ("materialization_next_retry_at",
                   "restart_rearm_next_at",
                   "watcher_retry_next_at"):
        ts = _parse_iso_ts(meta.get(_field))
        if ts is not None and ts > datetime.now(timezone.utc):
            return True, f"retry_timestamp_fresh:{_field}"

    # A bare identity field (materialization_owner / recovery_owner /
    # recovery_ownership / current_owner / watcher_token / watcher_retry_owner)
    # is a HISTORICAL claim, not a living worker. Standing alone it can persist
    # indefinitely after a process dies and silently stop tomorrow's valid
    # trades. It only counts as an active owner when corroborated by one of:
    #   - a fresh materialization lease (materialization_lease_until > now)
    #   - a fresh retry timestamp (any of the retry-*_at fields above)
    #   - an ACTIVE materialization_status (already handled above)
    #   - materialization_in_flight (already handled above)
    # The lease and status checks above already returned early on a positive
    # match. Reaching this point means no fresh liveness evidence exists, so
    # bare identity fields alone are treated as ghost ownership.
    _identity_fields_present = [
        _f for _f in _RECOVERY_OWNER_IDENTITY_FIELDS
        if str(meta.get(_f) or "").strip()
    ]
    if _identity_fields_present:
        return False, (
            "recovery_identity_without_liveness:"
            + ",".join(_identity_fields_present)
        )

    return False, "no_active_owner"


def _terminalize_stale_pending_orders(
    *,
    order_state_machine,
    stale_orders,
    reason: str,
) -> tuple[bool, str]:
    """P0-2: durably terminalize every stale pending row before authorizing a
    replacement. For each row: (1) transition via expire_pending_entry (falls
    back to cancel_pending_entry, then transition('EXPIRED', ...)), (2) verify
    the readback status is in _TERMINAL_ENTRY_STATUSES. A single row that
    cannot be proven terminal fails the whole cleanup — the caller must not
    release the candidate.

    Returns (all_terminalized, failure_reason). failure_reason is stable
    (operator diagnostic) when the return is False.
    """
    if not stale_orders:
        return True, ""

    for _row in stale_orders:
        oid = str((_row or {}).get("local_order_id") or "").strip()
        if not oid:
            return False, "stale_row_missing_local_order_id"

        # The classifier read carries the exact status and updated_ts (row
        # version) the classifier saw. The cleanup write must atomically fence
        # on both — if either changed between the classifier read and this
        # write, another actor (broker submit intent, recovery owner, anything)
        # touched the row and terminalizing here would kill the rightful new
        # owner. expire_stale_pending_entry_cas() preserves the pending-entry
        # ownership guard AND adds row-version fencing; any refusal is a
        # legitimate ownership signal and MUST fail the cleanup.
        expected_status = str((_row or {}).get("status") or "").strip().upper()
        expected_updated_ts = (_row or {}).get("updated_ts")
        if not expected_status:
            return False, f"stale_row_missing_status:{oid}"
        if expected_updated_ts is None or (
            isinstance(expected_updated_ts, str) and not expected_updated_ts.strip()
        ):
            # Without a row-version fence the CAS can't detect a concurrent
            # writer. Refuse rather than falling back to a weaker write.
            return False, f"stale_row_missing_updated_ts:{oid}"

        terminalized = False
        failure_detail = ""
        try:
            if hasattr(order_state_machine, "expire_stale_pending_entry_cas"):
                ok, failure_detail = order_state_machine.expire_stale_pending_entry_cas(
                    oid,
                    expected_status=expected_status,
                    expected_updated_ts=expected_updated_ts,
                    reason=reason,
                )
                terminalized = bool(ok)
        except Exception as _cleanup_exc:
            log.error(
                "pending_entry_stale_terminalize_exception local_order_id=%s "
                "err=%s reason=%s",
                oid, _cleanup_exc, reason,
            )
            return False, f"terminalize_exception:{type(_cleanup_exc).__name__}"

        if not terminalized:
            return False, (
                f"terminalize_returned_false:{oid}:"
                f"{failure_detail or 'no_cas_helper'}"
            )

        # Verify readback: the row must actually be in a terminal status.
        try:
            row = (
                order_state_machine.get_order(oid)
                if hasattr(order_state_machine, "get_order")
                else None
            )
        except Exception as _rb_exc:
            log.error(
                "pending_entry_stale_readback_exception local_order_id=%s err=%s",
                oid, _rb_exc,
            )
            return False, f"readback_exception:{type(_rb_exc).__name__}"

        status = str((row or {}).get("status") or "").strip().upper()
        if status not in _TERMINAL_ENTRY_STATUSES:
            log.error(
                "pending_entry_stale_readback_nonterminal local_order_id=%s "
                "status=%r",
                oid, status,
            )
            return False, f"readback_nonterminal:{oid}:{status or 'blank'}"

    return True, ""


class _PendingEntryOutcome(NamedTuple):
    """Structured result of _classify_pending_entry_for_overnight.

    `disposition` is a PENDING_OWNER_* string constant preserving the existing
    contract with all callers and tests. `stale_orders` carries the exact
    same-client/mode rows the classifier saw and released; the caller must
    durably terminalize each before authorizing a replacement. `failure_reason`
    is a stable operator-facing string for DB_ERROR and CONFLICT paths.
    """
    disposition: str
    stale_orders: tuple = ()
    failure_reason: str = ""

    # Preserve legacy string-comparison call sites: `_pe_class == "PENDING_..."`
    # and `_pe_class in {...}`. Existing tests and callers that treated the
    # return as a plain string keep working; the new .stale_orders / .failure_
    # reason attributes are additive.
    def __eq__(self, other):
        if isinstance(other, str):
            return self.disposition == other
        return tuple.__eq__(self, other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.disposition)


def _classify_pending_entry_for_overnight(
    ticker: str,
    client_id: str,
    execution_mode: str,
    entry_watcher=None,
    candidate_signal: Optional[dict] = None,
) -> "_PendingEntryOutcome":
    """Query same-client/mode ENTRY orders for the ticker and classify the
    ownership disposition. Returns _PendingEntryOutcome — callers can still
    compare directly against PENDING_OWNER_* strings (backward compatible).
    The stale_orders attribute carries every same-client/mode row the loop
    released so the caller can terminalize each before authorizing a
    replacement (P0-2). Fail-closed on DB error."""
    cand_c = str(client_id      or "").strip().lower()
    cand_m = str(execution_mode or "").strip().lower()
    if not cand_c or not cand_m:
        log.warning("[%s] _classify_pending_entry_for_overnight: missing candidate "
                    "identity client=%r mode=%r — fail closed", ticker, cand_c, cand_m)
        return _PendingEntryOutcome(
            "PENDING_OWNER_DB_ERROR", (), "missing_candidate_identity"
        )

    try:
        from ap.order_monitor import (
            _classify_pending_entry_ownership,
            PENDING_OWNER_CONFLICT,
            PENDING_OWNER_DB_ERROR,
        )
    except ImportError as _ie:
        log.critical(
            "[%s] _classify_pending_entry_for_overnight: import failed: %s", ticker, _ie
        )
        return _PendingEntryOutcome(
            "PENDING_OWNER_DB_ERROR", (), f"import_failed:{type(_ie).__name__}"
        )

    # P0-3: exact-identity SQL. Filter by symbol AND same client AND same
    # execution_mode (case-insensitive) before status/kind. Removes the
    # LIMIT-10-hole (an older same-client/mode active owner can no longer be
    # dropped by ten newer cross-scope rows). Direction and canonical_signal_id
    # are used as additional filters when the candidate provides them, so
    # opposite-side and cross-canonical collisions cannot masquerade as
    # ownership of this candidate.
    cand_side = ""
    cand_canonical = ""
    if isinstance(candidate_signal, dict):
        cand_side = str(
            candidate_signal.get("side") or candidate_signal.get("direction") or ""
        ).strip().upper()
        cand_canonical = str(
            candidate_signal.get("canonical_signal_id")
            or candidate_signal.get("signal_id")
            or ""
        ).strip()

    # Full active-ownership status vocabulary. Any status listed in
    # _ACTIVE_ENTRY_OWN_STATUSES represents an in-flight (or terminally-
    # owned FILLED) ENTRY that must block admission of a replacement.
    # Omitting any of these here causes a live owner to be misclassified
    # PENDING_OWNER_MISSING and an authorized replacement to be issued.
    _pending_statuses = tuple(sorted(_ACTIVE_ENTRY_OWN_STATUSES))
    _pending_status_placeholders = ", ".join(["%s"] * len(_pending_statuses))

    _sql = (
        "SELECT local_order_id, client_id, "
        "LOWER(TRIM(COALESCE(execution_mode, ''))) AS execution_mode, "
        "status, broker_order_id, submitted_ts, created_ts, updated_ts, meta, "
        "direction, canonical_signal_id "
        "FROM orders "
        "WHERE symbol = %s AND kind = 'ENTRY' "
        "AND LOWER(TRIM(COALESCE(client_id, ''))) = %s "
        "AND LOWER(TRIM(COALESCE(execution_mode, ''))) = %s "
        f"AND status IN ({_pending_status_placeholders}) "
    )
    _params: list = [
        str(ticker or "").upper(),
        cand_c,
        cand_m,
        *_pending_statuses,
    ]
    if cand_side in {"CALL", "PUT"}:
        _sql += "AND UPPER(TRIM(COALESCE(direction, ''))) = %s "
        _params.append(cand_side)
    if cand_canonical:
        # Candidate-admission cleanup targets require EXACT canonical identity.
        # A NULL or blank canonical_signal_id does not prove the row belongs
        # to this candidate — it proves the opposite (ownership unknown), and
        # authorizing a candidate-specific cleanup against it would let a
        # legacy unidentified row masquerade as this candidate's stale prior.
        # Legacy unidentified rows are a separate maintenance concern and
        # must be handled by a dedicated maintenance path, not by admission.
        _sql += "AND canonical_signal_id = %s "
        _params.append(cand_canonical)
    _sql += "ORDER BY created_ts DESC"

    try:
        from ap.db import conn, run_with_retry

        def _pe_fn():
            with conn() as c:
                c.execute(_sql, tuple(_params))
                return c.fetchall()

        rows = run_with_retry(_pe_fn) or []
    except Exception as _dbe:
        log.warning(
            "[%s] _classify_pending_entry_for_overnight: DB query failed: %s", ticker, _dbe
        )
        return _PendingEntryOutcome(
            "PENDING_OWNER_DB_ERROR", (), f"db_query_failed:{type(_dbe).__name__}"
        )

    if not rows:
        return _PendingEntryOutcome("PENDING_OWNER_MISSING", (), "no_matching_rows")

    _has_order_fn = getattr(entry_watcher, "has_order", None) if entry_watcher else None
    _saw_conflict = False
    _saw_db_error = False
    _stale_orders: list[dict] = []
    _failure_reason = ""

    for _raw in rows:
        row = dict(_raw) if not isinstance(_raw, dict) else _raw
        row_client_id = str(row.get("client_id") or "").strip()
        row_client = row_client_id.lower()
        row_execution_mode = str(row.get("execution_mode") or "").strip()
        row_mode = row_execution_mode.lower()

        # Missing existing row identity → ambiguous, never release. (The SQL
        # filter already restricts to same client/mode; belt-and-suspenders.)
        if not row_client or not row_mode:
            _saw_conflict = True
            _failure_reason = _failure_reason or "row_missing_identity"
            continue
        if row_client != cand_c or row_mode != cand_m:
            # Should not occur given SQL filter, but preserve fail-closed
            # semantics if the DB somehow returns a non-matching row.
            _saw_conflict = True
            _failure_reason = _failure_reason or "row_scope_mismatch"
            continue

        row_local_oid = str(row.get("local_order_id") or "").strip()
        # P1: a watcher-registry exception is NOT evidence-of-no-owner. A
        # registry/lock/internal-state failure must surface as a structured
        # PENDING_OWNER_DB_ERROR so the caller preserves the exact diagnostic
        # and never authorizes a destructive stale-row cleanup on unproven
        # ownership.
        if callable(_has_order_fn) and row_local_oid:
            try:
                _watcher_owned = bool(_has_order_fn(row_local_oid))
            except Exception as _wexc:
                log.error(
                    "[%s] _classify_pending_entry_for_overnight: watcher "
                    "registry lookup failed local_order_id=%s err=%s",
                    ticker, row_local_oid, _wexc,
                )
                return _PendingEntryOutcome(
                    "PENDING_OWNER_DB_ERROR",
                    (),
                    f"watcher_registry_error:{type(_wexc).__name__}",
                )
        else:
            _watcher_owned = False

        # P1: unreadable / unexpected-shape metadata is NOT proof of "no
        # owner". For a destructive stale-row cleanup path, an unparseable
        # blob is ownership CONFLICT — the row must not be terminalized. A
        # JSONB scalar (string/list/number/bool) reaches Python as a non-dict
        # value even when JSON decoding succeeds; that shape carries no
        # ownership evidence we can read, so it too must fail closed.
        try:
            _meta_raw = row.get("meta")
            if _meta_raw is None or (
                isinstance(_meta_raw, str) and not _meta_raw.strip()
            ):
                _meta = {}
            elif isinstance(_meta_raw, dict):
                _meta = _meta_raw
            elif isinstance(_meta_raw, str):
                import json as _json
                _decoded = _json.loads(_meta_raw)
                if not isinstance(_decoded, dict):
                    _saw_conflict = True
                    _failure_reason = (
                        _failure_reason
                        or f"meta_non_object_shape:{type(_decoded).__name__}"
                    )
                    continue
                _meta = _decoded
            else:
                # Any other shape (list, int, float, bool coming back from
                # the driver) is unreadable ownership evidence — fail closed.
                _saw_conflict = True
                _failure_reason = (
                    _failure_reason
                    or f"meta_unexpected_type:{type(_meta_raw).__name__}"
                )
                continue
            _recovery_owned, _owner_reason = _pending_owner_lease_active(
                _meta,
                expected_client_id=row_client_id,
                expected_execution_mode=row_execution_mode,
            )
        except Exception as _mexc:
            log.error(
                "[%s] _classify_pending_entry_for_overnight: metadata parse "
                "failed local_order_id=%s err=%s",
                ticker, row_local_oid, _mexc,
            )
            _saw_conflict = True
            _failure_reason = (
                _failure_reason
                or f"meta_parse_exception:{type(_mexc).__name__}"
            )
            continue

        # A recognized-but-blocked ownership shape (expired proof-retry
        # deadline, malformed proof-retry shape, malformed durable-recovery
        # owner) is STRONGER than plain "unowned". Escalate to CONFLICT so
        # generic admission cleanup does not proceed against a canonical
        # ownership record whose terminalization belongs to a dedicated
        # recovery consumer. LEASE_REASONS_FORCE_CONFLICT is the shared
        # constant defined next to the predicates in ap.order_state_machine.
        from ap.order_state_machine import LEASE_REASONS_FORCE_CONFLICT
        if (not _recovery_owned) and _owner_reason in LEASE_REASONS_FORCE_CONFLICT:
            _saw_conflict = True
            _failure_reason = (
                _failure_reason or f"lease_forced_conflict:{_owner_reason}"
            )
            continue

        result = _classify_pending_entry_ownership(
            row,
            watcher_owned=_watcher_owned,
            recovery_owned=_recovery_owned,
            broker_terminal=False,
        )
        if result.blocks_candidate:
            return _PendingEntryOutcome(
                "PENDING_OWNER_ACTIVE", (),
                f"active_owner:{_owner_reason}" if _recovery_owned else "active_owner",
            )
        if result.disposition == PENDING_OWNER_DB_ERROR:
            _saw_db_error = True
            _failure_reason = _failure_reason or f"row_db_error:{result.reason}"
        elif result.disposition == PENDING_OWNER_CONFLICT:
            _saw_conflict = True
            _failure_reason = _failure_reason or f"row_conflict:{result.reason}"
        else:
            # STALE row — track exact identity so the caller can terminalize
            # it before authorizing a replacement (P0-2).
            _stale_orders.append({
                "local_order_id": row_local_oid,
                "client_id": row_client,
                "execution_mode": row_mode,
                "status": str(row.get("status") or "").strip().upper(),
                "updated_ts": row.get("updated_ts"),
                "canonical_signal_id": str(row.get("canonical_signal_id") or "").strip(),
                "direction": str(row.get("direction") or "").strip().upper(),
                "created_ts": row.get("created_ts"),
                "reason": result.reason,
                "owner_check": _owner_reason,
            })

    if _saw_db_error:
        return _PendingEntryOutcome(
            "PENDING_OWNER_DB_ERROR", tuple(_stale_orders),
            _failure_reason or "db_error",
        )
    if _saw_conflict:
        return _PendingEntryOutcome(
            "PENDING_OWNER_CONFLICT", tuple(_stale_orders),
            _failure_reason or "conflict",
        )
    if _stale_orders:
        return _PendingEntryOutcome(
            "PENDING_OWNER_STALE", tuple(_stale_orders), "stale_release",
        )
    return _PendingEntryOutcome("PENDING_OWNER_MISSING", (), "no_owner_evidence")


def _hydrate_plan_from_signal(signal, *, client_id=None, execution_mode=None):
    """Build a minimal TradePlan-compatible namespace from a WATCHING signal.
    Used when MC rejects on intel/second-score but recheck is disabled —
    the signal was already scanner-approved and has all the fields needed
    for contract deferment + watcher arming.

    P0-5: the plan MUST carry identity + risk fields the watcher reads
    directly (signal_id, canonical_signal_id, client_id, execution_mode,
    stop_underlying, target_underlying). Prior implementation dropped these,
    letting the watcher generate a random signal identity, a blank
    client_id, and no stop/target — silently manufacturing an identity-free,
    stop-free watcher when master_control returned an observe-only reject.
    """
    import types as _types
    _trig = float(signal.get("entry_trigger") or 0) or None
    def _coerce_float(v):
        try:
            f = float(v) if v is not None else None
            return f if f and f > 0 else None
        except (TypeError, ValueError):
            return None
    _stop   = _coerce_float(signal.get("stop_price"))
    _target = _coerce_float(signal.get("target_price"))
    _sid    = str(signal.get("signal_id") or "").strip()
    _canon  = str(
        signal.get("canonical_signal_id")
        or _resolve_canonical_signal_id(_sid, signal)
        or ""
    ).strip()
    _plan_id = str(signal.get("plan_id") or _canon or _sid).strip()
    _mode = str(execution_mode or "").strip().lower() or None
    return _types.SimpleNamespace(
        ticker              = signal.get("ticker") or signal.get("symbol"),
        side                = (signal.get("side") or "").upper(),
        direction           = (signal.get("side") or "").upper(),
        score               = float(signal.get("score") or 0),
        timeframe           = str(signal.get("timeframe") or "1d"),
        entry_trigger       = _trig,
        trigger_price       = _trig,
        trigger_type        = "breach",
        prior_day_high      = signal.get("prior_day_high"),
        prior_day_low       = signal.get("prior_day_low"),
        pattern             = signal.get("pattern"),
        tier                = signal.get("tier"),
        contract_symbol     = None,
        contracts           = None,
        limit_price         = None,
        # Identity + risk fields the watcher reads from the plan.
        signal_id           = _sid,
        canonical_signal_id = _canon,
        plan_id             = _plan_id,
        client_id           = client_id,
        execution_mode      = _mode,
        stop_underlying     = _stop,
        target_underlying   = _target,
        metadata            = {
            "overnight":             True,
            "hydrated_from_signal":  True,
            "second_score_mode":     "observe_only",
            "signal_id":             _sid,
            "canonical_signal_id":   _canon,
            "plan_id":               _plan_id,
            "client_id":             client_id,
            "execution_mode":        _mode,
            "stop_underlying":       _stop,
            "target_underlying":     _target,
        },
    )


def _normalize_overnight_side(value) -> str:
    raw = str(value or "").strip().upper()
    if raw in {"CALL", "BUY", "LONG", "BULL", "BULLISH", "CALLS"}:
        return "CALL"
    if raw in {"PUT", "SELL", "SHORT", "BEAR", "BEARISH", "PUTS"}:
        return "PUT"
    return "UNKNOWN"


# SIGNALS_LOOKBACK: hours-based override. Only applied when explicitly set.
# Without the env var, the cutoff is computed session-aware (see below) so a
# Friday scanner setup remains visible on Monday morning without every
# deployment having to override the environment to survive the weekend.
def _signals_lookback_hours_env() -> Optional[int]:
    raw = os.getenv("SIGNALS_LOOKBACK")
    if raw is None or str(raw).strip() == "":
        return None
    try:
        v = int(str(raw).strip())
    except (TypeError, ValueError):
        return None
    return v if v > 0 else None


def _et_now() -> datetime:
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("America/New_York"))


def _is_trading_day(dt: datetime) -> bool:
    # P0 (monday-trade-flow-readiness): delegate to the canonical NYSE
    # calendar in ap.flatline_alarm (single source of truth per #282) instead
    # of weekday-only logic. On 2026-07-03 (Independence Day observed) the
    # weekday-only check let the overnight reeval run against dead holiday
    # quotes. Fail-safe: if the calendar import ever breaks, fall back to the
    # previous weekday-only behavior rather than blocking reeval entirely.
    try:
        from ap.flatline_alarm import is_trading_day as _nyse_is_trading_day
        return _nyse_is_trading_day(dt.date())
    except Exception:
        return dt.weekday() < 5  # legacy fallback: Mon-Fri


def _prior_trading_session_date(ref: Optional[datetime] = None) -> date:
    """Return the date of the prior *trading* session relative to ref (ET).

    Walks back at least one day and skips weekends AND NYSE full-closure
    holidays (via ap.flatline_alarm.is_trading_day, the canonical calendar),
    so on a Monday morning after a Friday holiday the prior trading session is
    the preceding Thursday. This is the session a cached prior-day high/low
    MUST match to be considered fresh. Fail-safe: on calendar import failure,
    weekend-only walk-back (previous behavior) — a holiday-stamped cache row
    then simply fails the equality guard and is treated as stale.
    """
    d = (ref or _et_now()).date()
    d = d - timedelta(days=1)
    try:
        from ap.flatline_alarm import is_trading_day as _nyse_is_trading_day
        _guard = 0
        while not _nyse_is_trading_day(d) and _guard < 14:
            d = d - timedelta(days=1)
            _guard += 1
        return d
    except Exception:
        while d.weekday() >= 5:  # Sat/Sun
            d = d - timedelta(days=1)
        return d


def _signals_lookback_cutoff_iso(now: Optional[datetime] = None) -> str:
    """Return the ISO-8601 UTC cutoff to use for the ap_signals/trade_queue
    created_at filter.

    Rules (in order):
      1. If SIGNALS_LOOKBACK is explicitly set to a positive integer,
         cutoff = now - SIGNALS_LOOKBACK hours (backwards-compat override).
      2. Otherwise cutoff = start (00:00 ET) of the PRIOR TRADING SESSION.
         On Monday morning this reaches back to Friday 00:00 ET so a Friday
         scanner setup remains visible through the entire Monday morning
         reeval window. Skips weekends and NYSE full-closure holidays via
         the canonical calendar in _prior_trading_session_date.

    The prior default of 18 hours defeats the exact Friday-to-Monday recovery
    this PR was written to guarantee; that is why the default is now session-
    aware. The env var remains honored so operators can pin an explicit
    window for A/B experiments — but correctness does not depend on it.
    """
    _now = now or datetime.now(timezone.utc)
    hours = _signals_lookback_hours_env()
    if hours is not None:
        return (_now - timedelta(hours=hours)).isoformat()
    # Session-aware default. Compute prior-trading-session date in ET, then
    # anchor cutoff at 00:00 ET of that date and convert to UTC.
    try:
        from zoneinfo import ZoneInfo as _ZI
        _et = _ZI("America/New_York")
        prior = _prior_trading_session_date(_now.astimezone(_et))
        prior_start_et = datetime(prior.year, prior.month, prior.day, 0, 0, 0, tzinfo=_et)
        return prior_start_et.astimezone(timezone.utc).isoformat()
    except Exception:
        # Fail safe: 96h (Friday-to-Tuesday round-trip covers every weekend
        # + a Monday holiday). Never fall back to the old 18h default.
        return (_now - timedelta(hours=96)).isoformat()


# ── PR4 (prior-day-level cache fallback) ─────────────────────────────────────
# DEFAULT OFF. When PRIOR_LEVEL_CACHE_FALLBACK != "1", the re-arm path behaves
# exactly as before. When enabled, prior-day H/L successfully fetched at re-arm
# are cached stamped with the trading session they represent, and a later re-arm
# whose fresh fetch returns null may fall back to that cache ONLY if the stamped
# session matches the actual prior trading session (no stale weekend/holiday/halt
# levels). Cache is process-local and best-effort — purely a resilience layer.
_PRIOR_LEVEL_CACHE_ENABLED = os.getenv("PRIOR_LEVEL_CACHE_FALLBACK", "0").strip() in ("1", "true", "yes")
_PRIOR_LEVEL_CACHE: dict[str, dict] = {}


def _cache_prior_levels(ticker: str, broker_session_date: date, high, low, close=None) -> None:
    """Stamp and store prior-day levels for a ticker keyed by trading session.

    `broker_session_date` MUST be the broker-provided prior_day_date that the
    caller has already verified equals the expected prior trading session. We
    never stamp with a locally-computed date — see the call site in the re-arm
    loop. Best-effort; never raises."""
    try:
        if high is None and low is None:
            return
        _PRIOR_LEVEL_CACHE[(ticker or "").upper()] = {
            "session_date": broker_session_date.isoformat(),
            "prior_day_high": high,
            "prior_day_low": low,
            "prior_day_close": close,
        }
    except Exception:
        pass


def _get_cached_prior_levels(ticker: str, expected_session: date) -> Optional[dict]:
    """Return cached prior-day levels for ticker ONLY if the stamped session
    matches expected_session (the actual prior trading session). Otherwise None
    (stale → fail-safe). Never raises."""
    try:
        rec = _PRIOR_LEVEL_CACHE.get((ticker or "").upper())
        if not rec:
            return None
        if rec.get("session_date") != expected_session.isoformat():
            return None  # stale: wrong session, do not use
        return rec
    except Exception:
        return None


def _signal_date(signal: dict) -> Optional[date]:
    """Extract the date the signal was generated (not when we process it).
    Falls back to parsing the signal_id itself (format: YYYY-MM-DD:...).
    """
    for key in ("created_at", "signal_date", "date", "timestamp_iso"):
        val = signal.get(key, "")
        if val and len(str(val)) >= 10:
            try:
                return date.fromisoformat(str(val)[:10])
            except ValueError:
                continue
    # Try parsing from signal_id: "2026-05-05:1-1:AAPL:Weekly:CALL"
    signal_id = signal.get("signal_id", "")
    if signal_id and len(signal_id) >= 10:
        try:
            return date.fromisoformat(str(signal_id)[:10])
        except ValueError:
            pass
    return None


def _overnight_signal_sort_key(job: dict) -> tuple:
    """Tier A, score, recency, then stable row identity."""
    signal = job.get("payload") or {}
    if isinstance(signal, str):
        try:
            import json as _json
            signal = _json.loads(signal)
        except Exception:
            signal = {}
    tier = str(signal.get("tier") or "").strip().upper()
    tier_rank = {"A": 0, "B": 1}.get(tier, 2)
    try:
        score_rank = -float(signal.get("score") or 0)
    except (TypeError, ValueError):
        score_rank = 0.0
    created_raw = (
        signal.get("created_at")
        or signal.get("timestamp_iso")
        or signal.get("signal_date")
        or job.get("created_ts")
        or ""
    )
    try:
        created = datetime.fromisoformat(str(created_raw).replace("Z", "+00:00"))
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        created_rank = -created.timestamp()
    except Exception:
        created_rank = 0.0
    return tier_rank, score_rank, created_rank, str(job.get("id") or job.get("signal_id") or "")


def _log_overnight_watch_cleanup(
    event: str,
    *,
    level: str,
    client_id: str,
    signal_id: str,
    ticker: str,
    side: str,
    local_order_id: str,
    contract: str,
    contract_deferred: bool,
    entry_trigger,
    cleanup_method: str,
    cleanup_success: bool,
    reason: str,
) -> None:
    try:
        _entry_trigger = "None" if entry_trigger is None else f"{float(entry_trigger):.4f}"
    except Exception:
        _entry_trigger = str(entry_trigger)
    getattr(log, level)(
        "%s client_id=%s signal_id=%s ticker=%s side=%s local_order_id=%s "
        "contract=%s contract_deferred=%s entry_trigger=%s cleanup_method=%s "
        "cleanup_success=%s reason=%s",
        event,
        client_id,
        signal_id,
        ticker,
        side,
        local_order_id,
        contract,
        contract_deferred,
        _entry_trigger,
        cleanup_method,
        cleanup_success,
        reason,
    )


def _cleanup_overnight_watch_arm_failure(
    *,
    order_state_machine,
    client_id: str,
    signal_id: str,
    ticker: str,
    side: str,
    local_order_id: str,
    contract: str,
    contract_deferred: bool,
    entry_trigger,
    reason: str,
    done_event: str,
) -> tuple[bool, str]:
    _log_overnight_watch_cleanup(
        "OVERNIGHT_WATCH_ARM_FAILED_CLEANUP_START",
        level="warning",
        client_id=client_id,
        signal_id=signal_id,
        ticker=ticker,
        side=side,
        local_order_id=local_order_id,
        contract=contract,
        contract_deferred=contract_deferred,
        entry_trigger=entry_trigger,
        cleanup_method="pending",
        cleanup_success=False,
        reason=reason,
    )

    cleanup_method = "none"
    cleanup_success = False

    if local_order_id:
        cleanup_method = "expire_stale_pending_entry_cas"
        if (
            hasattr(order_state_machine, "get_order")
            and hasattr(order_state_machine, "expire_stale_pending_entry_cas")
        ):
            try:
                observed = order_state_machine.get_order(local_order_id)
                observed = dict(observed) if isinstance(observed, dict) else {}
                observed_status = str(observed.get("status") or "").strip().upper()
                observed_updated_ts = observed.get("updated_ts")
                if not observed or not observed_status or observed_updated_ts is None:
                    cleanup_success = False
                else:
                    cleanup_result = order_state_machine.expire_stale_pending_entry_cas(
                        local_order_id,
                        expected_status=observed_status,
                        expected_updated_ts=observed_updated_ts,
                        reason=reason,
                    )
                    cleanup_success = bool(
                        cleanup_result[0]
                        if isinstance(cleanup_result, tuple)
                        else cleanup_result
                    )
            except Exception as exp_exc:
                log.error(
                    "[%s] overnight_reeval: fenced pending-entry cleanup failed "
                    "| local_order_id=%s reason=%s error=%s",
                    ticker, local_order_id, reason, exp_exc,
                )
                cleanup_success = False
        # No weaker fallback is permitted. A refusal or row-version loss means
        # ownership changed or became ambiguous and the order must be preserved.
    else:
        cleanup_method = "missing_local_order_id"

    _log_overnight_watch_cleanup(
        done_event,
        level="info" if cleanup_success else "error",
        client_id=client_id,
        signal_id=signal_id,
        ticker=ticker,
        side=side,
        local_order_id=local_order_id,
        contract=contract,
        contract_deferred=contract_deferred,
        entry_trigger=entry_trigger,
        cleanup_method=cleanup_method,
        cleanup_success=cleanup_success,
        reason=reason,
    )
    return cleanup_success, cleanup_method


def _resolve_canonical_signal_id(signal_id: str, signal: dict) -> str:
    try:
        from ap_canonical_signal import build_canonical_signal_id
        return build_canonical_signal_id(signal_id, signal) or str(signal_id or "")
    except Exception:
        return str((signal or {}).get("canonical_signal_id") or signal_id or "")


def _overnight_reeval_session_key(now: Optional[datetime] = None) -> str:
    now = now or _et_now()
    return now.date().isoformat()


def _watch_arm_cleanup_failed_reason(original_reason: str) -> str:
    original_reason = str(original_reason or "unknown")
    return f"overnight_watch_arm_failed_cleanup_failed:{original_reason}"


def _paper_overnight_rescue_only_signal(
    signal: dict | None,
    *,
    job_source: str | None,
    execution_mode: str | None,
) -> bool:
    if str(job_source or "").strip().lower() != "trade_queue":
        return False
    if str(execution_mode or "").strip().upper() != "PAPER":
        return False
    payload = signal if isinstance(signal, dict) else {}
    return bool(payload.get("force_overnight_reeval_only")) and bool(payload.get("do_not_queue_directly"))


def _paper_rescue_queue_reason(kind: str, detail: str | None = None) -> str:
    kind = str(kind or "").strip()
    detail = str(detail or "").strip()
    return f"{kind}:{detail}" if detail else kind


def _is_terminal_watch_arm_failure_reason(reason: str) -> bool:
    reason = str(reason or "")
    return (
        reason.startswith("overnight_watch_arm_failed:")
        or reason.startswith("overnight_watch_arm_failed_cleanup_failed:")
        # PR #388: durable REATTACH ambiguity marker (see
        # _persist_reattach_post_watch_ambiguity). Written when watch()
        # returns False AND the post-watch verify cannot prove the order
        # is preserved — treat as terminal on the next reeval so no
        # replacement ENTRY is ever created.
        or reason.startswith("reattach_post_watch_")
    )


# ── Lookup result ─────────────────────────────────────────────────────────────

class _LookupResult(NamedTuple):
    """Explicit result from _get_client_opportunity_row.

    lookup_status values:
      FOUND        — a valid matching row was returned in .row
      NOT_FOUND    — query succeeded; definitively zero matching rows
      LOOKUP_FAILED — truth could not be established (Supabase unavailable,
                      exception, malformed response, missing identity, etc.)
    """
    canonical_signal_id: str
    lookup_status: str   # "FOUND" | "NOT_FOUND" | "LOOKUP_FAILED"
    row: Optional[dict]
    error: Optional[str]


_LS_FOUND         = "FOUND"
_LS_NOT_FOUND     = "NOT_FOUND"
_LS_LOOKUP_FAILED = "LOOKUP_FAILED"


def _get_client_opportunity_row(signal_id: str, client_id: str, signal: dict) -> _LookupResult:
    """Query the opportunity ledger for this client/canonical-signal pair.

    Returns an explicit _LookupResult — FOUND, NOT_FOUND, or LOOKUP_FAILED.
    LOOKUP_FAILED is NEVER collapsed into NOT_FOUND: a backing-store outage
    must not allow a second ENTRY order to be created.
    """
    canonical = _resolve_canonical_signal_id(signal_id, signal)
    if not canonical or not client_id:
        return _LookupResult(
            canonical or "",
            _LS_LOOKUP_FAILED,
            None,
            "missing_canonical_or_client_id",
        )

    try:
        from ap.opportunity_ledger import _get_sb
        sb = _get_sb()
        if not sb:
            return _LookupResult(canonical, _LS_LOOKUP_FAILED, None, "supabase_client_unavailable")

        res = (
            sb.table("client_signal_opportunities")
            .select(
                "canonical_signal_id, client_id, opportunity_status, miss_stage, "
                "miss_reason, metadata, order_local_id"
            )
            .eq("canonical_signal_id", canonical)
            .eq("client_id", client_id)
            .limit(1)
            .execute()
        )
        rows = getattr(res, "data", None)
        if rows is None:
            # Malformed response — cannot distinguish zero rows from error.
            return _LookupResult(canonical, _LS_LOOKUP_FAILED, None, "malformed_response_data_none")

        if not rows:
            return _LookupResult(canonical, _LS_NOT_FOUND, None, None)

        row = rows[0] if isinstance(rows[0], dict) else dict(rows[0])
        return _LookupResult(canonical, _LS_FOUND, row, None)

    except Exception as exc:
        log.debug(
            "[%s] overnight_reeval: opportunity lookup failed canonical=%s: %s",
            client_id, canonical, exc,
        )
        return _LookupResult(canonical, _LS_LOOKUP_FAILED, None, str(exc))


# ── Active-order statuses that prove entry ownership ──────────────────────────
# PENDING_TRIGGER / SUBMITTED / ACKNOWLEDGED / PARTIAL_FILL → order exists and
# may need a watcher reattached (Cases A/B) or is in-flight (Case C).
# FILLED → already entered; must not enter again.
# PR #388 amendment: full nonterminal ENTRY family. Prior amendment only
# recognized PENDING_TRIGGER/SUBMITTED/ACKNOWLEDGED/PARTIAL_FILL/FILLED,
# which meant CREATED/ACCEPTED/OPEN/PARTIALLY_FILLED slipped through the
# active-order fence and produced duplicate create_entry_order calls on
# retry. The set matches the repository's existing durable duplicate logic.
_ACTIVE_ENTRY_OWN_STATUSES = frozenset({
    "CREATED",
    "PENDING_TRIGGER",
    "SUBMITTED",
    "ACCEPTED",
    "ACKNOWLEDGED",
    "OPEN",
    "PARTIAL_FILL",
    "PARTIALLY_FILLED",
    "FILLED",
})

# Statuses that mean the order is in flight / done (never re-create).
_ALREADY_OWNED_STATUSES = frozenset({
    "SUBMITTED", "ACCEPTED", "ACKNOWLEDGED", "OPEN",
    "PARTIAL_FILL", "PARTIALLY_FILLED", "FILLED",
})


def _query_active_entry_order(
    client_id: str,
    execution_mode: str,
    canonical_sig_id: str,
) -> tuple[str, Optional[dict]]:
    """Query for an exact active ENTRY order for this client/mode/canonical-signal.

    Uses direct DB query with exact 5-tuple identity:
      client_id + execution_mode (exact, lower-normalized) + canonical_signal_id
      + kind='ENTRY' + status in _ACTIVE_ENTRY_OWN_STATUSES.

    Returns:
      (_LS_FOUND,         row)  — active order exists; row is a dict
      (_LS_NOT_FOUND,    None)  — definitively no matching order
      (_LS_LOOKUP_FAILED, None) — query failed; truth unknown
    """
    if not client_id or not canonical_sig_id:
        return _LS_LOOKUP_FAILED, None

    mode = str(execution_mode or "").strip().lower()
    if mode not in {"live", "paper"}:
        log.warning(
            "[%s] _query_active_entry_order: unknown execution_mode=%r canonical=%s — LOOKUP_FAILED",
            client_id, execution_mode, canonical_sig_id,
        )
        return _LS_LOOKUP_FAILED, None

    statuses = tuple(_ACTIVE_ENTRY_OWN_STATUSES)
    placeholders = ", ".join(["%s"] * len(statuses))

    try:
        from ap.db import conn, run_with_retry

        def _fn():
            with conn() as c:
                c.execute(
                    f"SELECT * FROM orders "
                    f"WHERE client_id = %s "
                    f"AND kind = 'ENTRY' "
                    f"AND LOWER(TRIM(COALESCE(execution_mode, ''))) = %s "
                    f"AND canonical_signal_id = %s "
                    f"AND status IN ({placeholders}) "
                    f"ORDER BY created_ts DESC LIMIT 1",
                    (client_id, mode, canonical_sig_id, *statuses),
                )
                return c.fetchone()

        row = run_with_retry(_fn)
        if row is None:
            return _LS_NOT_FOUND, None
        return _LS_FOUND, (dict(row) if not isinstance(row, dict) else row)

    except Exception as exc:
        log.warning(
            "[%s] _query_active_entry_order: DB query failed canonical=%s mode=%s: %s",
            client_id, canonical_sig_id, mode, exc,
        )
        return _LS_LOOKUP_FAILED, None


def _query_exact_entry_order_by_local_id(
    local_order_id: str,
    client_id: str,
    execution_mode: str,
    canonical_signal_id: str,
) -> tuple[str, Optional[dict]]:
    """Post-watch exact-order lookup with NO status filter.

    _query_active_entry_order() is the active-ownership fence and filters
    to _ACTIVE_ENTRY_OWN_STATUSES — it CANNOT see CANCELED / EXPIRED /
    REJECTED / ERROR rows. That is correct for the fence, but a REATTACH
    watch() that terminalizes the order needs a lookup that CAN see the
    terminal state so the caller can classify already_resolved rather
    than incorrectly labeling retryable and enabling a replacement.

    This helper queries by exact 4-tuple identity with NO status filter:
      local_order_id + client_id + execution_mode + canonical_signal_id + kind='ENTRY'.

    Returns (_LS_FOUND, row) / (_LS_NOT_FOUND, None) / (_LS_LOOKUP_FAILED, None).
    """
    if not local_order_id or not client_id or not canonical_signal_id:
        return _LS_LOOKUP_FAILED, None
    mode = str(execution_mode or "").strip().lower()
    if mode not in {"live", "paper"}:
        log.warning(
            "[%s] _query_exact_entry_order_by_local_id: unknown mode=%r local_order_id=%s"
            " — LOOKUP_FAILED",
            client_id, execution_mode, local_order_id,
        )
        return _LS_LOOKUP_FAILED, None
    try:
        from ap.db import conn, run_with_retry

        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT * FROM orders "
                    "WHERE local_order_id = %s "
                    "AND client_id = %s "
                    "AND LOWER(TRIM(COALESCE(execution_mode, ''))) = %s "
                    "AND canonical_signal_id = %s "
                    "AND kind = 'ENTRY' "
                    "LIMIT 1",
                    (local_order_id, client_id, mode, canonical_signal_id),
                )
                return c.fetchone()

        row = run_with_retry(_fn)
        if row is None:
            return _LS_NOT_FOUND, None
        return _LS_FOUND, (dict(row) if not isinstance(row, dict) else row)

    except Exception as exc:
        log.warning(
            "[%s] _query_exact_entry_order_by_local_id: DB query failed "
            "local_order_id=%s canonical=%s mode=%s: %s",
            client_id, local_order_id, canonical_signal_id, mode, exc,
        )
        return _LS_LOOKUP_FAILED, None


def _query_latest_entry_order_no_status(
    client_id: str,
    execution_mode: str,
    canonical_signal_id: str,
) -> tuple[str, Optional[dict]]:
    """Resolver-level latest ENTRY fence with no status filter.

    This is deliberately separate from _query_active_entry_order(), which
    remains the active ownership fence. This helper is the final resurrection
    guard before NEW: if an exact terminal ENTRY exists for this
    client/mode/canonical, a retry must not create a replacement just because
    the active-only fence cannot see terminal statuses.
    """
    if not client_id or not canonical_signal_id:
        return _LS_LOOKUP_FAILED, None
    mode = str(execution_mode or "").strip().lower()
    if mode not in {"live", "paper"}:
        log.warning(
            "[%s] _query_latest_entry_order_no_status: unknown mode=%r canonical=%s"
            " — LOOKUP_FAILED",
            client_id, execution_mode, canonical_signal_id,
        )
        return _LS_LOOKUP_FAILED, None
    try:
        from ap.db import conn, run_with_retry

        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT * FROM orders "
                    "WHERE client_id = %s "
                    "AND LOWER(TRIM(COALESCE(execution_mode, ''))) = %s "
                    "AND canonical_signal_id = %s "
                    "AND kind = 'ENTRY' "
                    "ORDER BY created_ts DESC LIMIT 1",
                    (client_id, mode, canonical_signal_id),
                )
                return c.fetchone()

        row = run_with_retry(_fn)
        if row is None:
            return _LS_NOT_FOUND, None
        return _LS_FOUND, (dict(row) if not isinstance(row, dict) else row)

    except Exception as exc:
        log.warning(
            "[%s] _query_latest_entry_order_no_status: DB query failed "
            "canonical=%s mode=%s: %s",
            client_id, canonical_signal_id, mode, exc,
        )
        return _LS_LOOKUP_FAILED, None


# Terminal ENTRY statuses the post-watch verifier recognises. Anything not in
# _ACTIVE_ENTRY_OWN_STATUSES and not in this set is treated as ambiguous.
_TERMINAL_ENTRY_STATUSES = frozenset({
    "CANCELED", "CANCELLED", "EXPIRED", "REJECTED", "ERROR", "FAILED",
    "TERMINAL", "CLOSED",
})


def _attempt_scope_key(execution_mode: str, session_key: str) -> str:
    return f"{str(execution_mode or '').strip().lower()}:{str(session_key or '').strip()}"


def _attempt_meta_scope(meta: dict, execution_mode: str, session_key: str) -> dict:
    scopes = meta.get("overnight_watch_arm_attempt_scopes")
    if not isinstance(scopes, dict):
        return {}
    scope = scopes.get(_attempt_scope_key(execution_mode, session_key))
    return dict(scope) if isinstance(scope, dict) else {}


def _attempt_meta_scope_strict(
    meta: dict, execution_mode: str, session_key: str
) -> tuple[dict, bool, str]:
    """Strict scope extractor. Returns (scope, malformed, reason).

    A missing scopes container OR a present container that omits the requested
    session key is a clean absent state ({}, False, ""). A container that is
    present but NOT a JSON object, or a scope value that is present but NOT a
    JSON object, is malformed and MUST fail closed as WATCH_ATTEMPT_CONFLICT
    rather than being silently converted into a fresh first-claim.
    """
    if "overnight_watch_arm_attempt_scopes" not in (meta or {}):
        return {}, False, ""
    scopes = meta.get("overnight_watch_arm_attempt_scopes")
    if not isinstance(scopes, dict):
        return {}, True, "attempt_scopes_container_not_object"
    key = _attempt_scope_key(execution_mode, session_key)
    if key not in scopes:
        return {}, False, ""
    scope = scopes.get(key)
    if not isinstance(scope, dict):
        return {}, True, "attempt_scope_value_not_object"
    return dict(scope), False, ""


def _attempt_count(scope: dict) -> int:
    try:
        return max(0, int(scope.get("count") or 0))
    except (TypeError, ValueError):
        return 0


def _attempt_state(scope: dict) -> str:
    return str(scope.get("state") or "").strip().upper()


_KNOWN_WATCH_ATTEMPT_STATES = frozenset({
    WATCH_ATTEMPT_STATE_IN_PROGRESS,
    WATCH_ATTEMPT_STATE_RETRYABLE,
    WATCH_ATTEMPT_STATE_ARMED,
    WATCH_ATTEMPT_STATE_EXHAUSTED,
    WATCH_ATTEMPT_STATE_ERROR,
})


def _parse_attempt_scope_strict(
    scope: dict,
) -> tuple[bool, str, int, str, str, str]:
    """Parse one durable watcher-attempt scope without silently repairing it.

    Returns:
        valid, state, count, token, local_order_id, failure_reason

    An entirely absent scope is represented by an empty dict and is a valid
    first-claim state. Any partially populated, malformed, or unknown state
    fails closed.
    """
    if not isinstance(scope, dict):
        return False, "", 0, "", "", "attempt_scope_not_object"

    state = str(scope.get("state") or "").strip().upper()
    token = str(scope.get("token") or "").strip()
    local_order_id = str(scope.get("local_order_id") or "").strip()

    raw_count = scope.get("count", 0)

    if isinstance(raw_count, bool):
        return False, state, 0, token, local_order_id, "attempt_count_boolean"

    if isinstance(raw_count, float) and not raw_count.is_integer():
        return False, state, 0, token, local_order_id, "attempt_count_fractional"

    try:
        count = int(raw_count)
    except (TypeError, ValueError, OverflowError):
        return False, state, 0, token, local_order_id, "attempt_count_invalid"

    if count < 0:
        return False, state, count, token, local_order_id, "attempt_count_negative"

    # A completely absent scope is the only valid blank-state representation.
    if not state:
        if count != 0 or token or local_order_id:
            return (
                False,
                state,
                count,
                token,
                local_order_id,
                "blank_attempt_state_has_owner_data",
            )
        return True, "", 0, "", "", ""

    if state not in _KNOWN_WATCH_ATTEMPT_STATES:
        return (
            False,
            state,
            count,
            token,
            local_order_id,
            f"unknown_attempt_state:{state}",
        )

    if state == WATCH_ATTEMPT_STATE_RETRYABLE:
        if count == 0:
            if token or local_order_id:
                return (
                    False,
                    state,
                    count,
                    token,
                    local_order_id,
                    "retryable_zero_count_has_owner_data",
                )
        elif not token or not local_order_id:
            return (
                False,
                state,
                count,
                token,
                local_order_id,
                "retryable_attempt_missing_token_or_order",
            )

    elif state == WATCH_ATTEMPT_STATE_IN_PROGRESS:
        if count < 1 or not token:
            return (
                False,
                state,
                count,
                token,
                local_order_id,
                "in_progress_attempt_missing_owner",
            )
        # local_order_id may be blank between claim and bind.

    elif state == WATCH_ATTEMPT_STATE_ARMED:
        if count < 1 or not token or not local_order_id:
            return (
                False,
                state,
                count,
                token,
                local_order_id,
                "armed_attempt_missing_owner",
            )

    elif state == WATCH_ATTEMPT_STATE_EXHAUSTED:
        if count < 1 or not token or not local_order_id:
            return (
                False,
                state,
                count,
                token,
                local_order_id,
                "exhausted_attempt_missing_owner",
            )

    elif state == WATCH_ATTEMPT_STATE_ERROR:
        if count < 1 or not token:
            return (
                False,
                state,
                count,
                token,
                local_order_id,
                "error_attempt_missing_owner",
            )

    return True, state, count, token, local_order_id, ""


def _attempt_meta_patch(
    *,
    existing_meta: dict,
    execution_mode: str,
    session_key: str,
    state: str,
    attempt_count: int,
    token: str,
    local_order_id: str = "",
    reason: str = "",
) -> dict:
    meta = dict(existing_meta or {})
    scopes = meta.get("overnight_watch_arm_attempt_scopes")
    if not isinstance(scopes, dict):
        scopes = {}
    key = _attempt_scope_key(execution_mode, session_key)
    now = datetime.now(timezone.utc)
    updated_at = now.isoformat()
    normalized_state = str(state or "").strip().upper()
    scope = {
        "count": int(attempt_count or 0),
        "state": normalized_state,
        "token": str(token or "").strip(),
        "local_order_id": str(local_order_id or "").strip(),
        "last_reason": str(reason or "").strip(),
        "updated_at": updated_at,
        "execution_mode": str(execution_mode or "").strip().lower(),
        "session_key": str(session_key or "").strip(),
    }
    # PR #404 final amendment: bounded durable-claim lease on IN_PROGRESS so
    # a crashed process cannot leave the scope permanently owned. Any non-
    # IN_PROGRESS write clears the lease so a stale timestamp cannot later
    # be mistaken for current ownership.
    if normalized_state == WATCH_ATTEMPT_STATE_IN_PROGRESS:
        scope["claim_started_at"] = updated_at
        scope["claim_lease_until"] = (
            now + timedelta(seconds=OVERNIGHT_WATCH_ARM_CLAIM_LEASE_SECONDS)
        ).isoformat()
    else:
        scope.pop("claim_started_at", None)
        scope.pop("claim_lease_until", None)
    scopes[key] = scope
    meta["overnight_watch_arm_attempt_scopes"] = scopes

    # Mirror the current owner scope into stable top-level keys for operators.
    meta["overnight_watch_arm_attempt_count"] = scope["count"]
    meta["overnight_watch_arm_attempt_state"] = scope["state"]
    meta["overnight_watch_arm_attempt_token"] = scope["token"]
    meta["overnight_watch_arm_local_order_id"] = scope["local_order_id"]
    meta["overnight_watch_arm_last_reason"] = scope["last_reason"]
    meta["overnight_watch_arm_updated_at"] = scope["updated_at"]
    meta["execution_mode"] = scope["execution_mode"]
    meta["overnight_reeval_session_key"] = scope["session_key"]
    return meta


def _write_attempt_meta(
    *,
    signal_id: str,
    client_id: str,
    canonical_signal_id: str,
    signal_payload: dict,
    execution_mode: str,
    session_key: str,
    state: str,
    attempt_count: int,
    token: str,
    local_order_id: str = "",
    reason: str = "",
) -> bool:
    try:
        from ap.opportunity_ledger import CREATED as _OL_CREATED
        from ap.opportunity_ledger import create_opportunities as _create_opps
        from ap.opportunity_ledger import update_opportunity as _update_opportunity

        payload = dict(signal_payload or {})
        payload.setdefault("signal_id", signal_id)
        _create_opps(signal_id, [client_id], payload, canonical_signal_id=canonical_signal_id)

        lookup = _get_client_opportunity_row(signal_id, client_id, payload)
        if lookup.lookup_status != _LS_FOUND or not isinstance(lookup.row, dict):
            return False
        meta = lookup.row.get("metadata") if isinstance(lookup.row.get("metadata"), dict) else {}
        patch_meta = _attempt_meta_patch(
            existing_meta=meta,
            execution_mode=execution_mode,
            session_key=session_key,
            state=state,
            attempt_count=attempt_count,
            token=token,
            local_order_id=local_order_id,
            reason=reason,
        )
        if not _update_opportunity(
            signal_id,
            client_id,
            _OL_CREATED,
            canonical_signal_id=canonical_signal_id,
            order_local_id=str(local_order_id or "") or None,
            extra_meta=patch_meta,
        ):
            return False

        readback = _get_client_opportunity_row(signal_id, client_id, payload)
        if readback.lookup_status != _LS_FOUND or not isinstance(readback.row, dict):
            return False
        rb_meta = readback.row.get("metadata") if isinstance(readback.row.get("metadata"), dict) else {}
        rb_scope = _attempt_meta_scope(rb_meta, execution_mode, session_key)
        return (
            _attempt_state(rb_scope) == str(state or "").strip().upper()
            and _attempt_count(rb_scope) == int(attempt_count or 0)
            and str(rb_scope.get("token") or "") == str(token or "")
            and str(rb_scope.get("local_order_id") or "") == str(local_order_id or "")
        )
    except Exception as exc:
        log.warning(
            "OVERNIGHT_WATCH_ATTEMPT_WRITE_FAILED client=%s mode=%s canonical=%s state=%s err=%s",
            client_id, execution_mode, canonical_signal_id, state, exc,
        )
        return False


def _local_order_terminal_state(
    order_state_machine,
    local_order_id: str,
    *,
    expected_client_id: str = "",
    expected_execution_mode: str = "",
    expected_canonical_signal_id: str = "",
) -> tuple[str, str]:
    """Prove the referenced prior local ENTRY order is durably terminal AND that
    it belongs to the expected owner. When any expected_* identity is supplied,
    the OSM row must match on client_id, execution_mode (case-insensitive),
    canonical_signal_id AND kind == 'ENTRY' before its terminal status can
    authorize a replacement claim. Any missing or contradictory identity fence
    is a WATCH_ATTEMPT_CONFLICT — stale or cross-scope terminal proof must
    never be trusted to reopen a claim.
    """
    if not local_order_id:
        return WATCH_ATTEMPT_CONFLICT, "missing_prior_local_order_id"
    try:
        if not hasattr(order_state_machine, "get_order"):
            return WATCH_ATTEMPT_CONFLICT, "osm_get_order_unavailable"
        row = order_state_machine.get_order(local_order_id)
    except Exception as exc:
        return WATCH_ATTEMPT_DB_ERROR, f"osm_get_order_exception:{type(exc).__name__}"
    if not isinstance(row, dict) or not row:
        return WATCH_ATTEMPT_CONFLICT, "prior_order_missing"

    requires_identity = any(
        (expected_client_id, expected_execution_mode, expected_canonical_signal_id)
    )
    if requires_identity:
        expected_order_id = str(local_order_id or "").strip()
        row_order_id = str(row.get("local_order_id") or "").strip()
        if not row_order_id:
            return WATCH_ATTEMPT_CONFLICT, "prior_order_local_id_missing"
        if row_order_id != expected_order_id:
            return WATCH_ATTEMPT_CONFLICT, "prior_order_local_id_mismatch"
        row_client = str(row.get("client_id") or "").strip()
        if str(expected_client_id or "").strip() and row_client != str(expected_client_id).strip():
            return WATCH_ATTEMPT_CONFLICT, "prior_order_client_mismatch"
        row_mode = str(row.get("execution_mode") or "").strip().lower()
        exp_mode = str(expected_execution_mode or "").strip().lower()
        if exp_mode and row_mode != exp_mode:
            return WATCH_ATTEMPT_CONFLICT, "prior_order_execution_mode_mismatch"
        row_canonical = str(row.get("canonical_signal_id") or "").strip()
        exp_canonical = str(expected_canonical_signal_id or "").strip()
        if exp_canonical and row_canonical != exp_canonical:
            return WATCH_ATTEMPT_CONFLICT, "prior_order_canonical_mismatch"
        row_kind = str(row.get("kind") or "").strip().upper()
        if not row_kind:
            return WATCH_ATTEMPT_CONFLICT, "prior_order_kind_missing"
        if row_kind != "ENTRY":
            return (
                WATCH_ATTEMPT_CONFLICT,
                f"prior_order_kind_not_entry:{row_kind}",
            )

    status = str(row.get("status") or "").strip().upper()
    if status in _TERMINAL_ENTRY_STATUSES:
        return WATCH_ATTEMPT_ACQUIRED, status
    if status in _ACTIVE_ENTRY_OWN_STATUSES:
        return WATCH_ATTEMPT_ALREADY_IN_PROGRESS, status
    return WATCH_ATTEMPT_CONFLICT, f"unknown_prior_order_status:{status or 'blank'}"


# ── PR #404 amendment: cross-process atomic watcher-arm claim ─────────────────
# The durable ownership authority is the PostgreSQL row for
# (canonical_signal_id, client_id) in client_signal_opportunities, locked with
# SELECT ... FOR UPDATE inside a single transaction. The scoped attempt record
# under metadata["overnight_watch_arm_attempt_scopes"][mode:session] is the ONLY
# ownership evidence; top-level mirror keys are operator-facing only. The
# module-local threading.Lock is a same-process optimization and is NEVER the
# durable owner — two pods/processes serialize on the Postgres row lock, not the
# in-process lock.


def _coerce_attempt_metadata(raw) -> tuple[dict, bool]:
    """Return (metadata_dict, malformed). NULL → ({}, False). A JSON object →
    (dict, False). A non-object (list/number/str-that-is-not-an-object) → ({},
    True). A malformed non-dict metadata value must NEVER be silently treated as
    a clean claimable state."""
    if raw is None:
        return {}, False
    if isinstance(raw, dict):
        return dict(raw), False
    if isinstance(raw, (bytes, str)):
        import json as _json
        try:
            parsed = _json.loads(raw.decode() if isinstance(raw, bytes) else raw)
        except Exception:
            return {}, True
        if isinstance(parsed, dict):
            return parsed, False
        return {}, True
    return {}, True


def _ensure_opportunity_row(
    signal_id: str, client_id: str, canonical_signal_id: str, signal_payload: dict
) -> None:
    """Idempotently ensure the shared opportunity row exists before the locked
    transaction. Best-effort: create_opportunities uses ignore_duplicates and
    never regresses an existing row. A failure here is not fatal — the locked
    SELECT will report attempt_row_missing_after_create if the row truly does
    not exist."""
    try:
        from ap.opportunity_ledger import create_opportunities as _create_opps
        payload = dict(signal_payload or {})
        payload.setdefault("signal_id", signal_id)
        _create_opps(signal_id, [client_id], payload, canonical_signal_id=canonical_signal_id)
    except Exception as exc:
        log.debug(
            "OVERNIGHT_WATCH_ATTEMPT_ENSURE_ROW_BEST_EFFORT client=%s canonical=%s err=%s",
            client_id, canonical_signal_id, exc,
        )


# Exact ownership row query. The unique index (canonical_signal_id, client_id)
# guarantees at most one row; FOR UPDATE holds it until the transaction commits.
_ATTEMPT_LOCK_SQL = (
    "SELECT id, signal_id, canonical_signal_id, client_id, metadata, "
    "opportunity_status, order_local_id "
    "FROM client_signal_opportunities "
    "WHERE canonical_signal_id = %s AND client_id = %s "
    "FOR UPDATE"
)


def _lock_attempt_row(c, canonical_signal_id: str, client_id: str):
    """Return ('FOUND', row) | ('MISSING', None) | ('DUPLICATE', None)."""
    c.execute(_ATTEMPT_LOCK_SQL, (canonical_signal_id, client_id))
    rows = c.fetchall() or []
    if not rows:
        return "MISSING", None
    if len(rows) > 1:
        return "DUPLICATE", None
    return "FOUND", rows[0]


def _lock_exact_entry_order(
    c,
    *,
    local_order_id: str,
    client_id: str,
    execution_mode: str,
    canonical_signal_id: str,
):
    """Lock and reread the exact ENTRY before binding it to durable ownership."""
    c.execute(
        "SELECT * FROM orders "
        "WHERE local_order_id = %s "
        "AND client_id = %s "
        "AND LOWER(TRIM(COALESCE(execution_mode, ''))) = %s "
        "AND canonical_signal_id = %s "
        "AND kind = 'ENTRY' "
        "FOR UPDATE",
        (
            str(local_order_id or "").strip(),
            str(client_id or "").strip(),
            str(execution_mode or "").strip().lower(),
            str(canonical_signal_id or "").strip(),
        ),
    )
    rows = c.fetchall() or []
    if not rows:
        return "MISSING", None
    if len(rows) > 1:
        return "DUPLICATE", None
    return "FOUND", rows[0]


def _read_attempt_scope_unlocked(
    canonical_signal_id: str, client_id: str, mode: str, session: str
) -> Optional[tuple]:
    """Non-locking preliminary read of the scoped attempt state, used ONLY to
    decide whether prior terminal proof is required before opening the locked
    transaction. Returns (state, count, token, local_order_id) or None when the
    row/metadata could not be read. This value is advisory — the locked reread
    is the sole authority for the acquisition decision."""
    try:
        from ap.db import conn, run_with_retry

        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT metadata FROM client_signal_opportunities "
                    "WHERE canonical_signal_id = %s AND client_id = %s LIMIT 1",
                    (canonical_signal_id, client_id),
                )
                return c.fetchone()

        row = run_with_retry(_fn)
    except Exception:
        return None
    if not row:
        return None
    meta, malformed = _coerce_attempt_metadata(row.get("metadata"))
    if malformed:
        return None
    scope, scope_malformed, _scope_reason = _attempt_meta_scope_strict(meta, mode, session)
    if scope_malformed:
        return None
    valid, state, count, token, local_order_id, _reason = (
        _parse_attempt_scope_strict(scope)
    )
    if not valid:
        return None
    return state, count, token, local_order_id


def _read_attempt_scope_probe_full(
    canonical_signal_id: str, client_id: str, mode: str, session: str
) -> Optional[dict]:
    """Non-locking probe returning the FULL scope dict (including the
    claim_lease_until lease field). Advisory only — the locked reread is the
    sole authority for the acquisition decision. Returns None when the row /
    metadata / scope cannot be read cleanly."""
    try:
        from ap.db import conn, run_with_retry

        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT metadata FROM client_signal_opportunities "
                    "WHERE canonical_signal_id = %s AND client_id = %s LIMIT 1",
                    (canonical_signal_id, client_id),
                )
                return c.fetchone()

        row = run_with_retry(_fn)
    except Exception:
        return None
    if not row:
        return None
    meta, malformed = _coerce_attempt_metadata(row.get("metadata"))
    if malformed:
        return None
    scope, scope_malformed, _scope_reason = _attempt_meta_scope_strict(meta, mode, session)
    if scope_malformed or not isinstance(scope, dict):
        return None
    return dict(scope)


def _atomic_claim_watch_arm_attempt(
    *,
    order_state_machine,
    signal_id: str,
    client_id: str,
    canonical_signal_id: str,
    signal_payload: dict,
    execution_mode: str,
    session_key: str,
    max_attempts: int,
) -> _WatchAttemptClaim:
    """Acquire the watcher-arm attempt using a single PostgreSQL compare-and-swap.

    The ownership decision and mutation happen inside one transaction while the
    (canonical_signal_id, client_id) row is held with SELECT ... FOR UPDATE, so
    exactly one process can observe the claimable prior state and increment it.
    Prior terminal-order proof (_local_order_terminal_state, which may reach the
    OSM) is resolved BEFORE the lock; the locked reread then requires the exact
    same durable prior order, so stale terminal proof can never authorize
    replacing a different order.
    """
    mode = str(execution_mode or "").strip().lower()
    session = str(session_key or "").strip()
    signal_id = str(signal_id or "").strip()
    client_id = str(client_id or "").strip()
    canonical_signal_id = str(canonical_signal_id or "").strip()
    try:
        max_attempts = int(max_attempts)
    except (TypeError, ValueError):
        max_attempts = 0
    if not (
        signal_id and client_id and canonical_signal_id
        and mode in {"live", "paper"} and session and max_attempts >= 1
    ):
        return _WatchAttemptClaim(WATCH_ATTEMPT_CONFLICT, reason="missing_attempt_owner_identity")

    # §5.2 — ensure the durable row exists (idempotent) before locking it.
    _ensure_opportunity_row(signal_id, client_id, canonical_signal_id, signal_payload)

    # §5.5 two-phase — resolve prior terminal-order truth OUTSIDE the row lock so
    # no OSM/broker-facing work is performed while holding the lock.
    required_prior_order = None
    retryable_bound_entry = None
    # PR #404 final amendment: expired IN_PROGRESS lease recovery. Resolve any
    # exact-terminal-order proof (Case A) OR active-entry lookup (Case B) OUTSIDE
    # the row lock so the locked reread need only verify identity and reclaim.
    stale_ip_expected = None          # tuple(count, token, order_id) captured pre-lock
    stale_ip_active_entry = None      # dict from _query_active_entry_order (Case B)
    pre = _read_attempt_scope_unlocked(canonical_signal_id, client_id, mode, session)
    if pre is not None:
        pre_state, pre_count, pre_token, pre_order = pre
        if pre_state == WATCH_ATTEMPT_STATE_RETRYABLE and pre_order:
            terminal_result, terminal_reason = _local_order_terminal_state(
                order_state_machine,
                pre_order,
                expected_client_id=client_id,
                expected_execution_mode=mode,
                expected_canonical_signal_id=canonical_signal_id,
            )
            if terminal_result == WATCH_ATTEMPT_ACQUIRED:
                required_prior_order = pre_order
            elif terminal_result != WATCH_ATTEMPT_ALREADY_IN_PROGRESS:
                return _WatchAttemptClaim(
                    terminal_result, pre_token, pre_count, pre_order,
                    terminal_reason,
                )
            else:
                lookup_status, exact_row = _query_exact_entry_order_by_local_id(
                    pre_order, client_id, mode, canonical_signal_id,
                )
                if lookup_status == _LS_LOOKUP_FAILED:
                    return _WatchAttemptClaim(
                        WATCH_ATTEMPT_DB_ERROR, pre_token, pre_count, pre_order,
                        "retryable_bound_order_lookup_failed",
                    )
                if lookup_status != _LS_FOUND or not exact_row:
                    return _WatchAttemptClaim(
                        WATCH_ATTEMPT_CONFLICT, pre_token, pre_count, pre_order,
                        "retryable_bound_order_missing",
                    )
                exact_status = str(exact_row.get("status") or "").strip().upper()
                if exact_status in _ACTIVE_ENTRY_OWN_STATUSES:
                    retryable_bound_entry = dict(exact_row)
                else:
                    return _WatchAttemptClaim(
                        WATCH_ATTEMPT_CONFLICT, pre_token, pre_count, pre_order,
                        f"retryable_bound_order_status_unknown:{exact_status or 'blank'}",
                    )
        elif pre_state == WATCH_ATTEMPT_STATE_IN_PROGRESS:
            # Determine whether the observed IN_PROGRESS claim is stranded by
            # inspecting its lease. Fresh leases short-circuit the recovery
            # attempt entirely — the locked reread will surface ALREADY_IN_
            # PROGRESS if the row is unchanged, and CONFLICT otherwise.
            pre_scope_probe = None
            try:
                _ppr = _read_attempt_scope_probe_full(
                    canonical_signal_id, client_id, mode, session,
                )
                pre_scope_probe = _ppr
            except Exception:
                pre_scope_probe = None
            pre_lease = _parse_watch_attempt_ts(
                (pre_scope_probe or {}).get("claim_lease_until")
            )
            if pre_lease is not None and pre_lease > datetime.now(timezone.utc):
                # Fresh lease — locked reread will honor ALREADY_IN_PROGRESS.
                pass
            elif pre_lease is None:
                # Missing / malformed lease is ambiguous — do NOT resolve any
                # OSM state pre-lock. Locked reread will refuse with a stable
                # diagnostic.
                pass
            else:
                # Expired lease → recovery candidate. Resolve identity proof
                # OUTSIDE the row lock so the locked reread only verifies.
                if pre_order:
                    terminal_result, terminal_reason = _local_order_terminal_state(
                        order_state_machine,
                        pre_order,
                        expected_client_id=client_id,
                        expected_execution_mode=mode,
                        expected_canonical_signal_id=canonical_signal_id,
                    )
                    if terminal_result == WATCH_ATTEMPT_ACQUIRED:
                        exact_row = None
                    elif terminal_result != WATCH_ATTEMPT_ALREADY_IN_PROGRESS:
                        return _WatchAttemptClaim(
                            terminal_result, pre_token, pre_count, pre_order,
                            terminal_reason,
                        )
                    else:
                        lookup_status, exact_row = _query_exact_entry_order_by_local_id(
                            pre_order, client_id, mode, canonical_signal_id,
                        )
                        if lookup_status == _LS_LOOKUP_FAILED:
                            return _WatchAttemptClaim(
                                WATCH_ATTEMPT_DB_ERROR, pre_token, pre_count, pre_order,
                                "expired_bound_order_lookup_failed",
                            )
                        if lookup_status != _LS_FOUND or not exact_row:
                            return _WatchAttemptClaim(
                                WATCH_ATTEMPT_CONFLICT, pre_token, pre_count, pre_order,
                                "expired_bound_order_missing",
                            )
                        exact_status = str(exact_row.get("status") or "").strip().upper()
                        if exact_status in _ACTIVE_ENTRY_OWN_STATUSES:
                            stale_ip_active_entry = dict(exact_row)
                        else:
                            return _WatchAttemptClaim(
                                WATCH_ATTEMPT_CONFLICT, pre_token, pre_count, pre_order,
                                f"expired_bound_order_status_unknown:{exact_status or 'blank'}",
                            )
                    stale_ip_expected = (pre_count, pre_token, pre_order)
                else:
                    # Case B: blank local_order_id. Discover whether an exact
                    # active ENTRY already exists for this canonical/client/
                    # mode. If it does, do NOT create a replacement — the
                    # existing order is the owner. If none, reclaim is safe.
                    lookup_status, active_row = _query_active_entry_order(
                        client_id, mode, canonical_signal_id,
                    )
                    if lookup_status == _LS_LOOKUP_FAILED:
                        return _WatchAttemptClaim(
                            WATCH_ATTEMPT_DB_ERROR, pre_token, pre_count, "",
                            "expired_in_progress_active_lookup_failed",
                        )
                    if lookup_status == _LS_FOUND and active_row:
                        # Preserve the full row so the locked branch can
                        # distinguish reattachable PENDING_TRIGGER entries from
                        # CREATED handoff recovery and broker-owned statuses
                        # (SUBMITTED / ACCEPTED / OPEN / PARTIAL / FILLED)
                        # before the authoritative locked revalidation.
                        stale_ip_active_entry = dict(active_row)
                    stale_ip_expected = (pre_count, pre_token, "")

    def _log_cas_miss(state, count, reason):
        log.info(
            "OVERNIGHT_WATCH_ATTEMPT_CAS_MISS client=%s mode=%s canonical=%s "
            "session=%s state=%s count=%s reason=%s",
            client_id, mode, canonical_signal_id, session, state, count, reason,
        )

    try:
        from ap.db import conn, run_with_retry

        def _fn() -> _WatchAttemptClaim:
            with conn() as c:
                status, row = _lock_attempt_row(c, canonical_signal_id, client_id)
                if status == "MISSING":
                    return _WatchAttemptClaim(
                        WATCH_ATTEMPT_DB_ERROR, reason="attempt_row_missing_after_create"
                    )
                if status == "DUPLICATE":
                    return _WatchAttemptClaim(
                        WATCH_ATTEMPT_CONFLICT, reason="duplicate_attempt_owner_rows"
                    )

                meta, malformed = _coerce_attempt_metadata(row.get("metadata"))
                if malformed:
                    return _WatchAttemptClaim(
                        WATCH_ATTEMPT_CONFLICT, reason="attempt_metadata_malformed"
                    )
                scope, scope_malformed, scope_malformed_reason = (
                    _attempt_meta_scope_strict(meta, mode, session)
                )
                if scope_malformed:
                    return _WatchAttemptClaim(
                        WATCH_ATTEMPT_CONFLICT, reason=scope_malformed_reason
                    )
                (
                    scope_valid,
                    state,
                    count,
                    prior_token,
                    prior_order_id,
                    scope_failure_reason,
                ) = _parse_attempt_scope_strict(scope)

                if not scope_valid:
                    return _WatchAttemptClaim(
                        WATCH_ATTEMPT_CONFLICT,
                        prior_token,
                        count,
                        prior_order_id,
                        scope_failure_reason,
                    )

                def _resolve_locked_existing_entry(order_hint, *, source: str):
                    """Converge or reclaim one exact order while both rows are locked."""
                    existing_oid = str(
                        (order_hint or {}).get("local_order_id") or ""
                    ).strip()
                    if not existing_oid:
                        return _WatchAttemptClaim(
                            WATCH_ATTEMPT_CONFLICT,
                            prior_token,
                            count,
                            prior_order_id,
                            f"{source}_active_entry_missing_oid",
                        )

                    order_lock_status, locked_order = _lock_exact_entry_order(
                        c,
                        local_order_id=existing_oid,
                        client_id=client_id,
                        execution_mode=mode,
                        canonical_signal_id=canonical_signal_id,
                    )
                    if order_lock_status != "FOUND" or not locked_order:
                        return _WatchAttemptClaim(
                            WATCH_ATTEMPT_CONFLICT,
                            prior_token,
                            count,
                            prior_order_id,
                            f"{source}_order_revalidation_{order_lock_status.lower()}",
                        )

                    locked_status = str(
                        locked_order.get("status") or ""
                    ).strip().upper()
                    if locked_status in _ALREADY_OWNED_STATUSES:
                        resolved_meta = _attempt_meta_patch(
                            existing_meta=meta,
                            execution_mode=mode,
                            session_key=session,
                            state=WATCH_ATTEMPT_STATE_ARMED,
                            attempt_count=count,
                            token=prior_token,
                            local_order_id=existing_oid,
                            reason=f"{source}_broker_owned_resolved:{locked_status}",
                        )
                        if not _write_locked_attempt_meta(
                            c, row["id"], resolved_meta, mode, session,
                            WATCH_ATTEMPT_STATE_ARMED, count, prior_token,
                            existing_oid,
                        ):
                            return _WatchAttemptClaim(
                                WATCH_ATTEMPT_DB_ERROR,
                                reason=f"{source}_broker_owned_resolve_write_failed",
                            )
                        return _WatchAttemptClaim(
                            WATCH_ATTEMPT_ALREADY_ARMED,
                            prior_token,
                            count,
                            existing_oid,
                            f"{source}_broker_owned_resolved:{locked_status}",
                        )

                    if locked_status == "CREATED":
                        # CREATED is not a pending-trigger watcher contract. It
                        # belongs to separate OSM handoff recovery and must never
                        # be sent through APEntryWatcher's pending classifier.
                        error_meta = _attempt_meta_patch(
                            existing_meta=meta,
                            execution_mode=mode,
                            session_key=session,
                            state=WATCH_ATTEMPT_STATE_ERROR,
                            attempt_count=count,
                            token=prior_token,
                            local_order_id=existing_oid,
                            reason=f"{source}_created_requires_osm_recovery",
                        )
                        if not _write_locked_attempt_meta(
                            c, row["id"], error_meta, mode, session,
                            WATCH_ATTEMPT_STATE_ERROR, count, prior_token,
                            existing_oid,
                        ):
                            return _WatchAttemptClaim(
                                WATCH_ATTEMPT_DB_ERROR,
                                reason=f"{source}_created_error_write_failed",
                            )
                        return _WatchAttemptClaim(
                            WATCH_ATTEMPT_CONFLICT,
                            prior_token,
                            count,
                            existing_oid,
                            "attempt_state_error",
                        )

                    if locked_status != "PENDING_TRIGGER":
                        return _WatchAttemptClaim(
                            WATCH_ATTEMPT_CONFLICT,
                            prior_token,
                            count,
                            existing_oid,
                            f"{source}_order_status_changed:{locked_status or 'blank'}",
                        )

                    if count >= max_attempts:
                        exhausted_meta = _attempt_meta_patch(
                            existing_meta=meta,
                            execution_mode=mode,
                            session_key=session,
                            state=WATCH_ATTEMPT_STATE_EXHAUSTED,
                            attempt_count=count,
                            token=prior_token,
                            local_order_id=existing_oid,
                            reason="overnight_watch_arm_retry_exhausted",
                        )
                        if not _write_locked_attempt_meta(
                            c, row["id"], exhausted_meta, mode, session,
                            WATCH_ATTEMPT_STATE_EXHAUSTED, count, prior_token,
                            existing_oid,
                        ):
                            return _WatchAttemptClaim(
                                WATCH_ATTEMPT_DB_ERROR,
                                reason=f"{source}_exhausted_write_failed",
                            )
                        return _WatchAttemptClaim(
                            WATCH_ATTEMPT_EXHAUSTED,
                            prior_token,
                            count,
                            existing_oid,
                        )

                    new_token = uuid.uuid4().hex
                    next_count = count + 1
                    reattach_meta = _attempt_meta_patch(
                        existing_meta=meta,
                        execution_mode=mode,
                        session_key=session,
                        state=WATCH_ATTEMPT_STATE_IN_PROGRESS,
                        attempt_count=next_count,
                        token=new_token,
                        local_order_id=existing_oid,
                        reason=f"{source}_pending_trigger_reattach_claimed",
                    )
                    if not _write_locked_attempt_meta(
                        c, row["id"], reattach_meta, mode, session,
                        WATCH_ATTEMPT_STATE_IN_PROGRESS, next_count, new_token,
                        existing_oid,
                    ):
                        return _WatchAttemptClaim(
                            WATCH_ATTEMPT_DB_ERROR,
                            reason=f"{source}_reattach_claim_write_failed",
                        )
                    return _WatchAttemptClaim(
                        WATCH_ATTEMPT_REATTACH_REQUIRED,
                        new_token,
                        next_count,
                        existing_oid,
                        f"{source}_pending_trigger_reattach_claimed",
                    )

                # §5.5 classification under the row lock. No automatic lease
                # stealing: IN_PROGRESS/ARMED/EXHAUSTED/ERROR are honored as-is.
                if state == WATCH_ATTEMPT_STATE_ARMED:
                    return _WatchAttemptClaim(
                        WATCH_ATTEMPT_ALREADY_ARMED, prior_token, count, prior_order_id
                    )
                if state == WATCH_ATTEMPT_STATE_EXHAUSTED:
                    return _WatchAttemptClaim(
                        WATCH_ATTEMPT_EXHAUSTED, prior_token, count, prior_order_id
                    )
                if state == WATCH_ATTEMPT_STATE_ERROR:
                    return _WatchAttemptClaim(
                        WATCH_ATTEMPT_CONFLICT, prior_token, count, prior_order_id,
                        "attempt_state_error",
                    )
                if state == WATCH_ATTEMPT_STATE_IN_PROGRESS:
                    # PR #404 final amendment: durable-claim lease + stranded
                    # IN_PROGRESS recovery. A fresh lease → honor ownership.
                    # A missing/malformed lease → fail closed (do NOT steal).
                    # An expired lease → reclaim ONLY when the pre-lock exact
                    # identity check proved the referenced order (or lack of
                    # one) safe, AND the locked reread matches the pre-lock
                    # observation byte-for-byte.
                    lease_until = _parse_watch_attempt_ts(
                        scope.get("claim_lease_until")
                    )
                    _now = datetime.now(timezone.utc)
                    if lease_until is not None and lease_until > _now:
                        return _WatchAttemptClaim(
                            WATCH_ATTEMPT_ALREADY_IN_PROGRESS,
                            prior_token, count, prior_order_id,
                            "attempt_claim_lease_active",
                        )
                    if lease_until is None:
                        # Missing / malformed lease — ambiguous. Never steal
                        # ownership without proving expiration.
                        return _WatchAttemptClaim(
                            WATCH_ATTEMPT_CONFLICT,
                            prior_token, count, prior_order_id,
                            "attempt_claim_lease_missing_or_malformed",
                        )
                    # Expired lease. Require that pre-lock resolution
                    # captured the exact same identity we now see, so stale
                    # terminal proof or a stale active-lookup cannot
                    # authorize reclaim against a changed row.
                    if stale_ip_expected is None:
                        return _WatchAttemptClaim(
                            WATCH_ATTEMPT_ALREADY_IN_PROGRESS,
                            prior_token, count, prior_order_id,
                            "attempt_claim_lease_recover_prelock_missing",
                        )
                    _exp_count, _exp_token, _exp_order = stale_ip_expected
                    if (count != _exp_count or prior_token != _exp_token
                            or prior_order_id != _exp_order):
                        _log_cas_miss(state, count, "stale_in_progress_owner_changed")
                        return _WatchAttemptClaim(
                            WATCH_ATTEMPT_CONFLICT,
                            prior_token, count, prior_order_id,
                            "stale_in_progress_owner_changed",
                        )
                    if stale_ip_active_entry is not None:
                        return _resolve_locked_existing_entry(
                            stale_ip_active_entry,
                            source="expired_in_progress",
                        )
                    if prior_order_id:
                        # Case A: exact prior order was proven terminal
                        # pre-lock. The bound order consumed this attempt, so
                        # replacement must consume the next bounded attempt.
                        # Only an expired *unbound* claim below keeps count.
                        if count >= max_attempts:
                            _exhausted_meta = _attempt_meta_patch(
                                existing_meta=meta,
                                execution_mode=mode,
                                session_key=session,
                                state=WATCH_ATTEMPT_STATE_EXHAUSTED,
                                attempt_count=count,
                                token=prior_token,
                                local_order_id=prior_order_id,
                                reason="expired_bound_terminal_attempt_exhausted",
                            )
                            if not _write_locked_attempt_meta(
                                c, row["id"], _exhausted_meta, mode, session,
                                WATCH_ATTEMPT_STATE_EXHAUSTED, count, prior_token,
                                prior_order_id,
                            ):
                                return _WatchAttemptClaim(
                                    WATCH_ATTEMPT_DB_ERROR,
                                    reason="expired_bound_terminal_exhausted_write_failed",
                                )
                            return _WatchAttemptClaim(
                                WATCH_ATTEMPT_EXHAUSTED,
                                prior_token,
                                count,
                                prior_order_id,
                                "expired_bound_terminal_attempt_exhausted",
                            )

                        _next_count = count + 1
                        _new_token = uuid.uuid4().hex
                        _reclaim_meta = _attempt_meta_patch(
                            existing_meta=meta,
                            execution_mode=mode,
                            session_key=session,
                            state=WATCH_ATTEMPT_STATE_IN_PROGRESS,
                            attempt_count=_next_count,
                            token=_new_token,
                            local_order_id="",
                            reason="expired_bound_terminal_attempt_advanced",
                        )
                        if not _write_locked_attempt_meta(
                            c, row["id"], _reclaim_meta, mode, session,
                            WATCH_ATTEMPT_STATE_IN_PROGRESS, _next_count,
                            _new_token, "",
                        ):
                            return _WatchAttemptClaim(
                                WATCH_ATTEMPT_DB_ERROR,
                                reason="expired_in_progress_reacquire_write_failed",
                            )
                        log.info(
                            "OVERNIGHT_WATCH_ATTEMPT_EXPIRED_BOUND_TERMINAL_ADVANCED "
                            "client=%s mode=%s canonical=%s session=%s count=%s "
                            "token_prefix=%s prior_order=%s",
                            client_id, mode, canonical_signal_id, session,
                            _next_count, _new_token[:8], prior_order_id,
                        )
                        return _WatchAttemptClaim(
                            WATCH_ATTEMPT_ACQUIRED, _new_token, _next_count, "",
                            "expired_bound_terminal_attempt_advanced",
                        )
                    _new_token = uuid.uuid4().hex
                    _reclaim_meta = _attempt_meta_patch(
                        existing_meta=meta,
                        execution_mode=mode,
                        session_key=session,
                        state=WATCH_ATTEMPT_STATE_IN_PROGRESS,
                        attempt_count=count,
                        token=_new_token,
                        local_order_id="",
                        reason="expired_in_progress_claim_reacquired",
                    )
                    if not _write_locked_attempt_meta(
                        c, row["id"], _reclaim_meta, mode, session,
                        WATCH_ATTEMPT_STATE_IN_PROGRESS, count, _new_token, "",
                    ):
                        return _WatchAttemptClaim(
                            WATCH_ATTEMPT_DB_ERROR,
                            reason="expired_in_progress_reacquire_write_failed",
                        )
                    log.info(
                        "OVERNIGHT_WATCH_ATTEMPT_EXPIRED_CLAIM_REACQUIRED "
                        "client=%s mode=%s canonical=%s session=%s count=%s "
                        "token_prefix=%s unbound",
                        client_id, mode, canonical_signal_id, session,
                        count, _new_token[:8],
                    )
                    return _WatchAttemptClaim(
                        WATCH_ATTEMPT_ACQUIRED, _new_token, count, "",
                        "expired_in_progress_claim_reacquired",
                    )

                if state not in {"", WATCH_ATTEMPT_STATE_RETRYABLE}:
                    return _WatchAttemptClaim(
                        WATCH_ATTEMPT_CONFLICT,
                        prior_token,
                        count,
                        prior_order_id,
                        f"attempt_state_not_claimable:{state or 'blank'}",
                    )

                if retryable_bound_entry is not None:
                    expected_oid = str(
                        retryable_bound_entry.get("local_order_id") or ""
                    ).strip()
                    if prior_order_id != expected_oid:
                        _log_cas_miss(state, count, "retryable_bound_order_changed")
                        return _WatchAttemptClaim(
                            WATCH_ATTEMPT_CONFLICT,
                            prior_token,
                            count,
                            prior_order_id,
                            "retryable_bound_order_changed",
                        )
                    return _resolve_locked_existing_entry(
                        retryable_bound_entry,
                        source="retryable_bound",
                    )

                # Claimable (RETRYABLE or empty/absent scope). A prior order may
                # be replaced ONLY when the durable order at lock time is the
                # exact one we proved terminal before the lock. If a different
                # order appeared (or none was proven), refuse — stale terminal
                # proof must not authorize replacing a different order.
                if prior_order_id and prior_order_id != required_prior_order:
                    _log_cas_miss(state, count, "attempt_prior_order_changed")
                    return _WatchAttemptClaim(
                        WATCH_ATTEMPT_CONFLICT, prior_token, count, prior_order_id,
                        "attempt_prior_order_changed",
                    )

                if count >= max_attempts:
                    exhausted_meta = _attempt_meta_patch(
                        existing_meta=meta,
                        execution_mode=mode,
                        session_key=session,
                        state=WATCH_ATTEMPT_STATE_EXHAUSTED,
                        attempt_count=count,
                        token=prior_token,
                        local_order_id=prior_order_id,
                        reason="overnight_watch_arm_retry_exhausted",
                    )
                    if not _write_locked_attempt_meta(c, row["id"], exhausted_meta,
                                                       mode, session,
                                                       WATCH_ATTEMPT_STATE_EXHAUSTED,
                                                       count, prior_token, prior_order_id):
                        return _WatchAttemptClaim(
                            WATCH_ATTEMPT_DB_ERROR, reason="exhausted_write_failed"
                        )
                    return _WatchAttemptClaim(
                        WATCH_ATTEMPT_EXHAUSTED, prior_token, count, prior_order_id
                    )

                token = uuid.uuid4().hex
                next_count = count + 1
                next_meta = _attempt_meta_patch(
                    existing_meta=meta,
                    execution_mode=mode,
                    session_key=session,
                    state=WATCH_ATTEMPT_STATE_IN_PROGRESS,
                    attempt_count=next_count,
                    token=token,
                    local_order_id="",
                    reason="attempt_acquired",
                )
                if not _write_locked_attempt_meta(c, row["id"], next_meta,
                                                   mode, session,
                                                   WATCH_ATTEMPT_STATE_IN_PROGRESS,
                                                   next_count, token, ""):
                    return _WatchAttemptClaim(
                        WATCH_ATTEMPT_DB_ERROR, reason="attempt_claim_write_failed"
                    )
                log.info(
                    "OVERNIGHT_WATCH_ATTEMPT_ACQUIRED client=%s mode=%s canonical=%s "
                    "session=%s attempt_count=%s token_prefix=%s",
                    client_id, mode, canonical_signal_id, session, next_count, token[:8],
                )
                return _WatchAttemptClaim(WATCH_ATTEMPT_ACQUIRED, token, next_count)

        return run_with_retry(_fn)
    except Exception as exc:
        log.warning(
            "OVERNIGHT_WATCH_ATTEMPT_DB_ERROR client=%s mode=%s canonical=%s session=%s err=%s",
            client_id, mode, canonical_signal_id, session, exc,
        )
        return _WatchAttemptClaim(
            WATCH_ATTEMPT_DB_ERROR, reason=f"attempt_claim_db_exception:{type(exc).__name__}"
        )


def _write_locked_attempt_meta(
    c, row_id, next_meta: dict, mode: str, session: str,
    expect_state: str, expect_count: int, expect_token: str, expect_order_id: str,
) -> bool:
    """Write metadata on the already-locked row by primary key and verify the
    scoped readback matches the intended transition. Called only while the row
    is held FOR UPDATE in the caller's transaction."""
    import json as _json
    c.execute(
        "UPDATE client_signal_opportunities "
        "SET metadata = %s::jsonb, updated_at = NOW() "
        "WHERE id = %s "
        "RETURNING metadata",
        (_json.dumps(next_meta), row_id),
    )
    rb = c.fetchone()
    if not rb:
        return False
    rb_meta, malformed = _coerce_attempt_metadata(rb.get("metadata"))
    if malformed:
        return False
    rb_scope = _attempt_meta_scope(rb_meta, mode, session)
    return (
        _attempt_state(rb_scope) == str(expect_state or "").strip().upper()
        and _attempt_count(rb_scope) == int(expect_count or 0)
        and str(rb_scope.get("token") or "") == str(expect_token or "")
        and str(rb_scope.get("local_order_id") or "") == str(expect_order_id or "")
    )


def _pg_cas_write_attempt(
    *,
    signal_id: str,
    client_id: str,
    canonical_signal_id: str,
    signal_payload: dict,
    execution_mode: str,
    session_key: str,
    predicate,
    new_state: str,
    new_count: int,
    new_token: str,
    new_local_order_id: str,
    reason: str,
    required_order_status: str = "",
) -> bool:
    """Exact-owner compare-and-swap for post-acquisition transitions (bind /
    complete). Locks the (canonical_signal_id, client_id) row, evaluates
    predicate(state, count, token, local_order_id) against the durable scope,
    and only on a match writes the new scope. Returns False on any predicate
    miss, missing/duplicate/malformed row, or DB error — the write is atomic
    under the same row lock, so no other process can replace ownership between
    the check and the write. When required_order_status is set, the exact ENTRY
    row is also locked and identity/status validated in this transaction before
    its local_order_id can be bound to the durable attempt."""
    mode = str(execution_mode or "").strip().lower()
    session = str(session_key or "").strip()
    canonical_signal_id = str(canonical_signal_id or "").strip()
    client_id = str(client_id or "").strip()
    required_order_status = str(required_order_status or "").strip().upper()
    new_local_order_id = str(new_local_order_id or "").strip()
    if not (client_id and canonical_signal_id and mode in {"live", "paper"} and session):
        return False
    if required_order_status and not new_local_order_id:
        return False

    _ensure_opportunity_row(signal_id, client_id, canonical_signal_id, signal_payload)

    try:
        from ap.db import conn, run_with_retry

        def _fn() -> bool:
            with conn() as c:
                status, row = _lock_attempt_row(c, canonical_signal_id, client_id)
                if status != "FOUND":
                    return False
                meta, malformed = _coerce_attempt_metadata(row.get("metadata"))
                if malformed:
                    return False
                scope = _attempt_meta_scope(meta, mode, session)
                (
                    scope_valid,
                    state,
                    count,
                    token,
                    order_id,
                    _scope_failure_reason,
                ) = _parse_attempt_scope_strict(scope)

                if not scope_valid:
                    return False
                if not predicate(state, count, token, order_id):
                    return False
                if required_order_status:
                    order_lock_status, locked_order = _lock_exact_entry_order(
                        c,
                        local_order_id=new_local_order_id,
                        client_id=client_id,
                        execution_mode=mode,
                        canonical_signal_id=canonical_signal_id,
                    )
                    if order_lock_status != "FOUND" or not locked_order:
                        return False
                    locked_status = str(
                        locked_order.get("status") or ""
                    ).strip().upper()
                    if locked_status != required_order_status:
                        return False
                next_meta = _attempt_meta_patch(
                    existing_meta=meta,
                    execution_mode=mode,
                    session_key=session,
                    state=new_state,
                    attempt_count=new_count,
                    token=new_token,
                    local_order_id=new_local_order_id,
                    reason=reason,
                )
                return _write_locked_attempt_meta(
                    c, row["id"], next_meta, mode, session,
                    str(new_state or "").strip().upper(), new_count, new_token,
                    str(new_local_order_id or ""),
                )

        return bool(run_with_retry(_fn))
    except Exception as exc:
        log.warning(
            "OVERNIGHT_WATCH_ATTEMPT_CAS_WRITE_DB_ERROR client=%s mode=%s canonical=%s "
            "reason=%s err=%s",
            client_id, mode, canonical_signal_id, reason, exc,
        )
        return False


def _claim_watch_arm_attempt(
    *,
    order_state_machine,
    signal_id: str,
    client_id: str,
    canonical_signal_id: str,
    signal_payload: dict,
    execution_mode: str,
    session_key: str,
) -> _WatchAttemptClaim:
    # The threading.Lock reduces same-process contention only. The PostgreSQL
    # row lock / CAS inside _atomic_claim_watch_arm_attempt is the authoritative
    # cross-process (cross-pod, cross-restart) owner.
    with _WATCH_ATTEMPT_LOCK:
        return _atomic_claim_watch_arm_attempt(
            order_state_machine=order_state_machine,
            signal_id=signal_id,
            client_id=client_id,
            canonical_signal_id=canonical_signal_id,
            signal_payload=signal_payload,
            execution_mode=execution_mode,
            session_key=session_key,
            max_attempts=OVERNIGHT_WATCH_ARM_MAX_ATTEMPTS,
        )


def _bind_watch_arm_attempt_order(
    *,
    signal_id: str,
    client_id: str,
    canonical_signal_id: str,
    signal_payload: dict,
    execution_mode: str,
    session_key: str,
    attempt: _WatchAttemptClaim,
    local_order_id: str,
) -> bool:
    if attempt.disposition != WATCH_ATTEMPT_ACQUIRED or not attempt.token or not local_order_id:
        return False

    # Atomic exact-owner CAS: bind only when the durable scope is still the
    # unbound IN_PROGRESS record owned by this exact token+count. Any other
    # token / count / state, or an already-bound local order, refuses the bind.
    def _predicate(state, count, token, order_id):
        return (
            state == WATCH_ATTEMPT_STATE_IN_PROGRESS
            and token == attempt.token
            and count == attempt.attempt_count
            and not order_id
        )

    return _pg_cas_write_attempt(
        signal_id=signal_id,
        client_id=client_id,
        canonical_signal_id=canonical_signal_id,
        signal_payload=signal_payload,
        execution_mode=execution_mode,
        session_key=session_key,
        predicate=_predicate,
        new_state=WATCH_ATTEMPT_STATE_IN_PROGRESS,
        new_count=attempt.attempt_count,
        new_token=attempt.token,
        new_local_order_id=str(local_order_id),
        reason="local_order_bound",
        required_order_status="PENDING_TRIGGER",
    )


def _reacquire_armed_watch_attempt_for_restart(
    *,
    signal_id: str,
    client_id: str,
    canonical_signal_id: str,
    signal_payload: dict,
    execution_mode: str,
    session_key: str,
    attempt: _WatchAttemptClaim,
) -> _WatchAttemptClaim:
    """Atomically reopen one durable ARMED owner for process-local reattachment.

    The exact PENDING_TRIGGER order is revalidated under the same transaction as
    the ARMED -> IN_PROGRESS CAS. The attempt count is deliberately unchanged:
    losing an in-memory watcher during restart is not a new arm failure.
    """
    if (
        attempt.disposition != WATCH_ATTEMPT_ALREADY_ARMED
        or not attempt.token
        or not attempt.local_order_id
    ):
        return _WatchAttemptClaim(
            WATCH_ATTEMPT_CONFLICT,
            attempt.token,
            attempt.attempt_count,
            attempt.local_order_id,
            "armed_restart_reacquire_invalid_owner",
        )

    new_token = uuid.uuid4().hex

    def _predicate(state, count, token, order_id):
        return (
            state == WATCH_ATTEMPT_STATE_ARMED
            and count == attempt.attempt_count
            and token == attempt.token
            and order_id == attempt.local_order_id
        )

    ok = _pg_cas_write_attempt(
        signal_id=signal_id,
        client_id=client_id,
        canonical_signal_id=canonical_signal_id,
        signal_payload=signal_payload,
        execution_mode=execution_mode,
        session_key=session_key,
        predicate=_predicate,
        new_state=WATCH_ATTEMPT_STATE_IN_PROGRESS,
        new_count=attempt.attempt_count,
        new_token=new_token,
        new_local_order_id=attempt.local_order_id,
        reason="armed_restart_watcher_reattach_claimed",
        required_order_status="PENDING_TRIGGER",
    )
    if not ok:
        return _WatchAttemptClaim(
            WATCH_ATTEMPT_CONFLICT,
            attempt.token,
            attempt.attempt_count,
            attempt.local_order_id,
            "armed_restart_reacquire_cas_failed",
        )
    return _WatchAttemptClaim(
        WATCH_ATTEMPT_REATTACH_REQUIRED,
        new_token,
        attempt.attempt_count,
        attempt.local_order_id,
        "armed_restart_watcher_reattach_claimed",
    )


_ALLOWED_WATCH_ATTEMPT_COMPLETION_STATES = frozenset({
    WATCH_ATTEMPT_STATE_RETRYABLE,
    WATCH_ATTEMPT_STATE_ARMED,
    WATCH_ATTEMPT_STATE_EXHAUSTED,
    WATCH_ATTEMPT_STATE_ERROR,
})


def _complete_watch_arm_attempt(
    *,
    signal_id: str,
    client_id: str,
    canonical_signal_id: str,
    signal_payload: dict,
    execution_mode: str,
    session_key: str,
    attempt: _WatchAttemptClaim,
    state: str,
    local_order_id: str,
    reason: str,
) -> bool:
    """Complete an exact watcher-attempt owner.

    Legal transitions:

        IN_PROGRESS -> ARMED
        IN_PROGRESS -> RETRYABLE
        IN_PROGRESS -> EXHAUSTED
        IN_PROGRESS -> ERROR

    Exact same-state replay is idempotent when token, count, and local order
    still match. ARMED, EXHAUSTED, and ERROR may never transition to a
    different state.
    """
    if not attempt.token:
        return False

    want_state = str(state or "").strip().upper()
    next_local_order_id = str(local_order_id or "").strip()

    if want_state not in _ALLOWED_WATCH_ATTEMPT_COMPLETION_STATES:
        return False

    def _predicate(cur_state, count, token, order_id):
        if token != attempt.token:
            return False
        if count != attempt.attempt_count:
            return False

        # Exact same-state replay is idempotent only when the order also matches.
        if cur_state == want_state:
            return order_id == next_local_order_id

        # No terminal/durable state may reopen or downgrade.
        if cur_state != WATCH_ATTEMPT_STATE_IN_PROGRESS:
            return False

        # Normal bound-owner completion.
        if order_id == next_local_order_id:
            return True

        # A local order can exist while the durable bind failed. Only ERROR may
        # record that exact local order from an otherwise unbound owner.
        return (
            want_state == WATCH_ATTEMPT_STATE_ERROR
            and not order_id
            and bool(next_local_order_id)
        )

    return _pg_cas_write_attempt(
        signal_id=signal_id,
        client_id=client_id,
        canonical_signal_id=canonical_signal_id,
        signal_payload=signal_payload,
        execution_mode=execution_mode,
        session_key=session_key,
        predicate=_predicate,
        new_state=want_state,
        new_count=attempt.attempt_count,
        new_token=attempt.token,
        new_local_order_id=next_local_order_id,
        reason=reason,
    )


def _complete_watch_arm_attempt_checked(
    *,
    signal_id: str,
    client_id: str,
    canonical_signal_id: str,
    signal_payload: dict,
    execution_mode: str,
    session_key: str,
    attempt: _WatchAttemptClaim,
    state: str,
    local_order_id: str,
    reason: str,
    ticker: str,
    caller: str,
) -> bool:
    ok = _complete_watch_arm_attempt(
        signal_id=signal_id,
        client_id=client_id,
        canonical_signal_id=canonical_signal_id,
        signal_payload=signal_payload,
        execution_mode=execution_mode,
        session_key=session_key,
        attempt=attempt,
        state=state,
        local_order_id=local_order_id,
        reason=reason,
    )

    if not ok:
        log.critical(
            "OVERNIGHT_WATCH_ATTEMPT_COMPLETION_FAILED | "
            "caller=%s ticker=%s client=%s mode=%s canonical=%s "
            "session=%s intended_state=%s attempt_count=%s "
            "token_prefix=%s local_order_id=%s reason=%s",
            caller,
            ticker,
            client_id,
            execution_mode,
            canonical_signal_id,
            session_key,
            str(state or "").strip().upper(),
            attempt.attempt_count,
            str(attempt.token or "")[:8],
            str(local_order_id or ""),
            reason,
        )

    return ok


class _EarlyWatchRecoveryResult(NamedTuple):
    handled: bool
    outcome: str = ""
    reason: str = ""
    local_order_id: str = ""


def _recover_materialized_watch_before_admission(
    *,
    order_state_machine,
    entry_watcher,
    signal_id: str,
    canonical_signal_id: str,
    client_id: str,
    execution_mode: str,
    session_key: str,
    signal_payload: dict,
    ticker: str,
    job_id,
    job_source: str,
) -> _EarlyWatchRecoveryResult:
    """Recover already-admitted durable work before new-admission validation.

    This seam is deliberately recovery-only. It does not claim an empty or
    otherwise new attempt scope. A claim is made only after the durable scope
    says recovery is in progress/retryable/armed *and* an exact active ENTRY is
    visible for the same client, mode, and canonical signal.
    """
    probe = _read_attempt_scope_unlocked(
        canonical_signal_id, client_id, execution_mode, session_key,
    )
    if probe is None:
        return _EarlyWatchRecoveryResult(False)

    state, attempt_count, _token, durable_order_id = probe
    if state not in {
        WATCH_ATTEMPT_STATE_IN_PROGRESS,
        WATCH_ATTEMPT_STATE_RETRYABLE,
        WATCH_ATTEMPT_STATE_ARMED,
    }:
        return _EarlyWatchRecoveryResult(False)

    if durable_order_id:
        lookup_status, order_row = _query_exact_entry_order_by_local_id(
            durable_order_id, client_id, execution_mode, canonical_signal_id,
        )
    else:
        lookup_status, order_row = _query_active_entry_order(
            client_id, execution_mode, canonical_signal_id,
        )

    if lookup_status == _LS_LOOKUP_FAILED:
        return _EarlyWatchRecoveryResult(
            True, "ERROR", "early_recovery_order_lookup_failed",
            str(durable_order_id or ""),
        )
    if lookup_status != _LS_FOUND or not order_row:
        if state == WATCH_ATTEMPT_STATE_ARMED:
            return _EarlyWatchRecoveryResult(
                True, "ERROR", "early_recovery_armed_order_missing",
                str(durable_order_id or ""),
            )
        # No active materialized owner: this is not the recovery-only seam.
        # Normal admission/replacement logic remains authoritative.
        return _EarlyWatchRecoveryResult(False)

    observed_status = str(order_row.get("status") or "").strip().upper()
    if observed_status not in _ACTIVE_ENTRY_OWN_STATUSES:
        if state == WATCH_ATTEMPT_STATE_ARMED:
            return _EarlyWatchRecoveryResult(
                True, "ALREADY_RESOLVED",
                f"early_recovery_armed_order_terminal:{observed_status or 'blank'}",
                str(durable_order_id or ""),
            )
        # A bound terminal order normally returns to the validated replacement
        # path. At the cap, however, the durable claim must be exhausted here,
        # before Master Control or any other new-admission work can run.
        if not (
            state in {
                WATCH_ATTEMPT_STATE_IN_PROGRESS,
                WATCH_ATTEMPT_STATE_RETRYABLE,
            }
            and bool(durable_order_id)
            and observed_status in _TERMINAL_ENTRY_STATUSES
            and attempt_count >= OVERNIGHT_WATCH_ARM_MAX_ATTEMPTS
        ):
            return _EarlyWatchRecoveryResult(False)

    attempt = _claim_watch_arm_attempt(
        order_state_machine=order_state_machine,
        signal_id=signal_id,
        client_id=client_id,
        canonical_signal_id=canonical_signal_id,
        signal_payload=signal_payload,
        execution_mode=execution_mode,
        session_key=session_key,
    )

    if attempt.disposition == WATCH_ATTEMPT_ALREADY_ARMED:
        # Durable ARMED proves the database transition, not the current
        # process's in-memory watcher registry. Broker-owned orders no longer
        # need a trigger watcher, but an active PENDING_TRIGGER order must be
        # verified locally after every restart and reattached when absent.
        if observed_status in _ALREADY_OWNED_STATUSES:
            return _EarlyWatchRecoveryResult(
                True, "ARMED",
                attempt.reason or "early_recovery_already_armed_broker_owned",
                attempt.local_order_id,
            )
        if observed_status != "PENDING_TRIGGER":
            return _EarlyWatchRecoveryResult(
                True, "ERROR",
                f"early_recovery_already_armed_non_pending:{observed_status or 'blank'}",
                attempt.local_order_id,
            )
        try:
            has_order = getattr(entry_watcher, "has_order", None)
            registry_armed = (
                bool(has_order(attempt.local_order_id))
                if callable(has_order) else False
            )
        except Exception as exc:
            return _EarlyWatchRecoveryResult(
                True, "ERROR",
                f"early_recovery_watcher_registry_error:{type(exc).__name__}",
                attempt.local_order_id,
            )
        if registry_armed:
            return _EarlyWatchRecoveryResult(
                True, "ARMED", attempt.reason or "early_recovery_already_armed",
                attempt.local_order_id,
            )
        attempt = _reacquire_armed_watch_attempt_for_restart(
            signal_id=signal_id,
            client_id=client_id,
            canonical_signal_id=canonical_signal_id,
            signal_payload=signal_payload,
            execution_mode=execution_mode,
            session_key=session_key,
            attempt=attempt,
        )
        if attempt.disposition != WATCH_ATTEMPT_REATTACH_REQUIRED:
            return _EarlyWatchRecoveryResult(
                True, "ERROR",
                attempt.reason or "early_recovery_armed_reacquire_failed",
                attempt.local_order_id,
            )
    if (
        attempt.disposition == WATCH_ATTEMPT_CONFLICT
        and str(attempt.reason or "") == "attempt_state_error"
    ):
        return _EarlyWatchRecoveryResult(
            True, "ALREADY_RESOLVED", attempt.reason, attempt.local_order_id,
        )
    if attempt.disposition == WATCH_ATTEMPT_ACQUIRED:
        # The advisory recovery probe and locked claim disagreed. Close the
        # accidentally acquired unbound owner and fail closed; never let this
        # race become a new order that bypassed admission validation.
        completion_ok = _complete_watch_arm_attempt_checked(
            signal_id=signal_id,
            client_id=client_id,
            canonical_signal_id=canonical_signal_id,
            signal_payload=signal_payload,
            execution_mode=execution_mode,
            session_key=session_key,
            attempt=attempt,
            state=WATCH_ATTEMPT_STATE_ERROR,
            local_order_id="",
            reason="early_recovery_preflight_changed",
            ticker=ticker,
            caller="early_recovery_preflight_changed",
        )
        return _EarlyWatchRecoveryResult(
            True,
            "ERROR",
            (
                "early_recovery_preflight_changed"
                if completion_ok else "early_recovery_completion_failed"
            ),
        )
    if attempt.disposition != WATCH_ATTEMPT_REATTACH_REQUIRED:
        outcome = (
            "ERROR"
            if attempt.disposition in {
                WATCH_ATTEMPT_DB_ERROR,
                WATCH_ATTEMPT_CONFLICT,
                WATCH_ATTEMPT_EXHAUSTED,
            }
            else "RETRYABLE"
        )
        return _EarlyWatchRecoveryResult(
            True,
            outcome,
            f"early_recovery_claim:{attempt.disposition}:{attempt.reason}",
            attempt.local_order_id,
        )

    local_order_id = str(attempt.local_order_id or "").strip()

    def _complete(state_value: str, reason: str) -> bool:
        return _complete_watch_arm_attempt_checked(
            signal_id=signal_id,
            client_id=client_id,
            canonical_signal_id=canonical_signal_id,
            signal_payload=signal_payload,
            execution_mode=execution_mode,
            session_key=session_key,
            attempt=attempt,
            state=state_value,
            local_order_id=local_order_id,
            reason=reason,
            ticker=ticker,
            caller="early_materialized_recovery",
        )

    if not local_order_id:
        completion_ok = _complete(
            WATCH_ATTEMPT_STATE_ERROR,
            "early_recovery_missing_local_order_id",
        )
        return _EarlyWatchRecoveryResult(
            True,
            "ERROR",
            (
                "early_recovery_missing_local_order_id"
                if completion_ok else "early_recovery_completion_failed"
            ),
        )

    # Claim-time FOR UPDATE is authoritative; this caller read constructs the
    # watcher plan and catches any post-claim status transition before watch().
    exact_status, exact_order = _query_exact_entry_order_by_local_id(
        local_order_id, client_id, execution_mode, canonical_signal_id,
    )
    if exact_status != _LS_FOUND or not exact_order:
        next_state = (
            WATCH_ATTEMPT_STATE_EXHAUSTED
            if attempt.attempt_count >= OVERNIGHT_WATCH_ARM_MAX_ATTEMPTS
            else WATCH_ATTEMPT_STATE_RETRYABLE
        )
        completion_ok = _complete(next_state, "early_recovery_exact_order_lookup_failed")
        return _EarlyWatchRecoveryResult(
            True,
            "ERROR" if not completion_ok else (
                "ERROR" if next_state == WATCH_ATTEMPT_STATE_EXHAUSTED else "RETRYABLE"
            ),
            "early_recovery_exact_order_lookup_failed",
            local_order_id,
        )

    exact_order_status = str(exact_order.get("status") or "").strip().upper()
    if exact_order_status in _ALREADY_OWNED_STATUSES:
        completion_ok = _complete(
            WATCH_ATTEMPT_STATE_ARMED, "early_recovery_broker_owned",
        )
        return _EarlyWatchRecoveryResult(
            True,
            "ARMED" if completion_ok else "ERROR",
            "early_recovery_broker_owned",
            local_order_id,
        )
    if exact_order_status != "PENDING_TRIGGER":
        completion_ok = _complete(
            WATCH_ATTEMPT_STATE_ERROR,
            f"early_recovery_non_pending_trigger:{exact_order_status or 'blank'}",
        )
        return _EarlyWatchRecoveryResult(
            True, "ERROR",
            (
                f"early_recovery_non_pending_trigger:{exact_order_status or 'blank'}"
                if completion_ok else "early_recovery_completion_failed"
            ),
            local_order_id,
        )

    order_meta = exact_order.get("meta") or {}
    if isinstance(order_meta, str):
        try:
            import json as _early_json
            order_meta = _early_json.loads(order_meta)
        except Exception:
            order_meta = {}
    if not isinstance(order_meta, dict):
        order_meta = {}

    def _number(order_key, signal_key, default=None):
        raw = exact_order.get(order_key)
        if raw is None:
            raw = signal_payload.get(signal_key)
        if raw is None:
            return default
        try:
            return float(raw)
        except (TypeError, ValueError, OverflowError):
            return default

    trigger = _number("trigger_price", "entry_trigger")
    if not trigger or trigger <= 0:
        next_state = (
            WATCH_ATTEMPT_STATE_EXHAUSTED
            if attempt.attempt_count >= OVERNIGHT_WATCH_ARM_MAX_ATTEMPTS
            else WATCH_ATTEMPT_STATE_RETRYABLE
        )
        completion_ok = _complete(next_state, "early_recovery_invalid_trigger")
        return _EarlyWatchRecoveryResult(
            True,
            "ERROR" if not completion_ok or next_state == WATCH_ATTEMPT_STATE_EXHAUSTED else "RETRYABLE",
            "early_recovery_invalid_trigger",
            local_order_id,
        )

    try:
        contracts = int(exact_order.get("qty") or 1)
    except (TypeError, ValueError, OverflowError):
        contracts = 0
    if contracts < 1:
        next_state = (
            WATCH_ATTEMPT_STATE_EXHAUSTED
            if attempt.attempt_count >= OVERNIGHT_WATCH_ARM_MAX_ATTEMPTS
            else WATCH_ATTEMPT_STATE_RETRYABLE
        )
        completion_ok = _complete(next_state, "early_recovery_invalid_contracts")
        return _EarlyWatchRecoveryResult(
            True,
            "ERROR" if not completion_ok or next_state == WATCH_ATTEMPT_STATE_EXHAUSTED else "RETRYABLE",
            "early_recovery_invalid_contracts",
            local_order_id,
        )

    import types as _early_types
    side = str(
        exact_order.get("direction")
        or signal_payload.get("side")
        or signal_payload.get("direction")
        or ""
    ).strip().upper()
    plan = _early_types.SimpleNamespace(
        ticker=str(exact_order.get("symbol") or ticker),
        side=side,
        direction=side,
        score=_number("score", "score", 0.0),
        timeframe=str(exact_order.get("timeframe") or signal_payload.get("timeframe") or "1d"),
        entry_trigger=trigger,
        trigger_price=trigger,
        stop_underlying=_number("stop_underlying", "stop_price"),
        target_underlying=_number("target_underlying", "target_price"),
        trigger_type="breach",
        prior_day_high=signal_payload.get("prior_day_high"),
        prior_day_low=signal_payload.get("prior_day_low"),
        pattern=exact_order.get("pattern") or signal_payload.get("pattern"),
        tier=exact_order.get("tier") or signal_payload.get("tier"),
        contract_symbol=str(exact_order.get("contract") or f"DEFERRED:{ticker}"),
        contracts=contracts,
        limit_price=_number("limit_price", "limit_price", 0.01),
        plan_id=str(exact_order.get("plan_id") or ""),
        signal_id=str(exact_order.get("signal_id") or signal_id),
        canonical_signal_id=canonical_signal_id,
        client_id=client_id,
        execution_mode=execution_mode,
        late_attachment_policy_eligible=True,
        metadata={
            **order_meta,
            "overnight": True,
            "reattach_watcher": True,
            "reattach_required_recovery": True,
            "contract_deferred": True,
            "contract_selection_deferred_to": "breach_time",
            "client_id": client_id,
            "execution_mode": execution_mode,
            "overnight_reeval_session_key": session_key,
            "canonical_signal_id": canonical_signal_id,
            "late_attachment_policy_eligible": True,
        },
    )

    try:
        has_order = getattr(entry_watcher, "has_order", None)
        watcher_armed = bool(has_order(local_order_id)) if callable(has_order) else False
    except Exception:
        watcher_armed = False

    if not watcher_armed:
        try:
            watcher_armed = bool(entry_watcher.watch(
                plan,
                local_order_id,
                recovery_rearm=True,
                no_cancel_on_reject=True,
            ))
        except Exception as exc:
            next_state = (
                WATCH_ATTEMPT_STATE_EXHAUSTED
                if attempt.attempt_count >= OVERNIGHT_WATCH_ARM_MAX_ATTEMPTS
                else WATCH_ATTEMPT_STATE_RETRYABLE
            )
            completion_ok = _complete(
                next_state,
                f"early_recovery_watch_exception:{type(exc).__name__}",
            )
            return _EarlyWatchRecoveryResult(
                True,
                "ERROR" if not completion_ok or next_state == WATCH_ATTEMPT_STATE_EXHAUSTED else "RETRYABLE",
                f"early_recovery_watch_exception:{type(exc).__name__}",
                local_order_id,
            )

    if not watcher_armed:
        next_state = (
            WATCH_ATTEMPT_STATE_EXHAUSTED
            if attempt.attempt_count >= OVERNIGHT_WATCH_ARM_MAX_ATTEMPTS
            else WATCH_ATTEMPT_STATE_RETRYABLE
        )
        completion_ok = _complete(next_state, "early_recovery_watch_false")
        return _EarlyWatchRecoveryResult(
            True,
            "ERROR" if not completion_ok or next_state == WATCH_ATTEMPT_STATE_EXHAUSTED else "RETRYABLE",
            "early_recovery_watch_false",
            local_order_id,
        )

    try:
        proof_ok = _persist_watcher_armed_proof(
            client_id=client_id,
            execution_mode=execution_mode,
            canonical_signal_id=canonical_signal_id,
            signal_id=signal_id,
            signal_payload=signal_payload,
            local_order_id=local_order_id,
            session_key=session_key,
            extra_meta={
                "source_table": job_source,
                "source_job_id": str(job_id),
                "ticker": ticker,
                "side": side,
                "contract_deferred": True,
                "contract_selection_deferred_to": "breach_time",
                "reattach_watcher": True,
                "early_materialized_recovery": True,
                "armed_at": datetime.now(timezone.utc).isoformat(),
            },
        )
    except Exception:
        proof_ok = False
    if not proof_ok:
        # watch() succeeded. Preserve that truth by completing the exact owner
        # ARMED; never downgrade an active watcher to RETRYABLE/EXHAUSTED merely
        # because the secondary opportunity proof failed.
        completion_ok = _complete(
            WATCH_ATTEMPT_STATE_ARMED,
            "early_recovery_watcher_armed_proof_failed",
        )
        return _EarlyWatchRecoveryResult(
            True,
            "ERROR",
            (
                "early_recovery_watcher_armed_proof_failed"
                if completion_ok else "early_recovery_completion_failed"
            ),
            local_order_id,
        )

    completion_ok = _complete(
        WATCH_ATTEMPT_STATE_ARMED, "early_materialized_recovery_armed",
    )
    return _EarlyWatchRecoveryResult(
        True,
        "ARMED" if completion_ok else "ERROR",
        (
            "early_materialized_recovery_armed"
            if completion_ok else "early_recovery_completion_failed"
        ),
        local_order_id,
    )


def _entry_order_disposition_from_status(
    *,
    client_id: str,
    canonical_signal_id: str,
    session_key: str,
    status: str,
    local_order_id: str,
    order_row: Optional[dict] = None,
    source: str,
) -> Optional[_DispositionResult]:
    _status = str(status or "").upper()
    _local_id = str(local_order_id or "").strip()
    if _status == "PENDING_TRIGGER":
        log.info(
            "[%s] reeval disposition=REATTACH_WATCHER canonical=%s session=%s "
            "local_order_id=%s source=%s",
            client_id, canonical_signal_id, session_key, _local_id, source,
        )
        return _DispositionResult(_DISPOSITION_REATTACH_WATCHER, _local_id, order_row)
    if _status == "CREATED":
        log.info(
            "[%s] reeval disposition=ACTIVE_CREATED_RETRY canonical=%s session=%s "
            "local_order_id=%s source=%s",
            client_id, canonical_signal_id, session_key, _local_id, source,
        )
        return _DispositionResult(_DISPOSITION_ACTIVE_CREATED_RETRY, _local_id, order_row)
    if _status in _ALREADY_OWNED_STATUSES:
        log.info(
            "[%s] reeval disposition=ALREADY_OWNED canonical=%s session=%s "
            "order_status=%s source=%s",
            client_id, canonical_signal_id, session_key, _status, source,
        )
        return _DispositionResult(_DISPOSITION_ALREADY_OWNED)
    if _status in _TERMINAL_ENTRY_STATUSES:
        log.info(
            "[%s] reeval disposition=ALREADY_TERMINAL (latest-entry-order) "
            "canonical=%s session=%s order_status=%s local_order_id=%s source=%s",
            client_id, canonical_signal_id, session_key, _status, _local_id, source,
        )
        return _DispositionResult(_DISPOSITION_ALREADY_TERMINAL)
    return None


def _persist_reattach_in_progress_fence(
    *,
    client_id: str,
    execution_mode: str,
    canonical_signal_id: str,
    signal_id: str,
    signal_payload: dict,
    local_order_id: str,
    session_key: str,
) -> bool:
    """Persist and verify current-session REATTACH ownership before watch().

    If watch() later terminalizes the order but the terminal suppression marker
    fails, this verified metadata keeps the next resolver attempt out of NEW.
    """
    _mode = str(execution_mode or "").strip().lower()
    _session = str(session_key or _overnight_reeval_session_key()).strip()
    if not (
        client_id and _mode in {"live", "paper"} and canonical_signal_id
        and signal_id and local_order_id and _session
    ):
        log.critical(
            "REATTACH_IN_PROGRESS_FENCE_INVALID_INPUT client=%s mode=%s "
            "canonical=%s signal=%s local_order_id=%s session=%s",
            client_id, execution_mode, canonical_signal_id,
            signal_id, local_order_id, session_key,
        )
        return False
    try:
        from ap.opportunity_ledger import (
            CREATED as _OL_CREATED,
            create_opportunities as _create_opps,
            update_opportunity as _update_opportunity,
        )

        _sig_payload = dict(signal_payload or {})
        _sig_payload.setdefault("signal_id", signal_id)
        _created = _create_opps(
            signal_id, [client_id], _sig_payload,
            canonical_signal_id=canonical_signal_id,
        )
        if int(_created or 0) < 1:
            log.critical(
                "REATTACH_IN_PROGRESS_FENCE_CREATE_FAILED client=%s mode=%s "
                "canonical=%s local_order_id=%s",
                client_id, _mode, canonical_signal_id, local_order_id,
            )
            return False

        _fence_meta = {
            "reattach_in_progress": True,
            "execution_mode": _mode,
            "overnight_reeval_session_key": _session,
            "source_table": "ap_signals",
            "original_signal_id": signal_id,
            "canonical_signal_id": canonical_signal_id,
            "local_order_id": str(local_order_id),
            "reattach_fenced_at": datetime.now(timezone.utc).isoformat(),
        }
        _updated = _update_opportunity(
            signal_id,
            client_id,
            _OL_CREATED,
            canonical_signal_id=canonical_signal_id,
            order_local_id=str(local_order_id),
            extra_meta=_fence_meta,
        )
        if not _updated:
            log.critical(
                "REATTACH_IN_PROGRESS_FENCE_UPDATE_FAILED client=%s mode=%s "
                "canonical=%s local_order_id=%s",
                client_id, _mode, canonical_signal_id, local_order_id,
            )
            return False

        _readback = _get_client_opportunity_row(signal_id, client_id, _sig_payload)
        if _readback.lookup_status != _LS_FOUND or not isinstance(_readback.row, dict):
            log.critical(
                "REATTACH_IN_PROGRESS_FENCE_READBACK_MISSING client=%s mode=%s "
                "canonical=%s local_order_id=%s status=%s err=%s",
                client_id, _mode, canonical_signal_id, local_order_id,
                _readback.lookup_status, _readback.error,
            )
            return False

        _row = _readback.row
        _meta = _row.get("metadata") if isinstance(_row.get("metadata"), dict) else {}
        _verified = (
            str(_row.get("canonical_signal_id") or _readback.canonical_signal_id or "") == canonical_signal_id
            and str(_row.get("client_id") or client_id) == client_id
            and str(_row.get("order_local_id") or _meta.get("local_order_id") or "") == str(local_order_id)
            and str(_meta.get("execution_mode") or _meta.get("mode") or "").strip().lower() == _mode
            and str(_meta.get("overnight_reeval_session_key") or "").strip() == _session
            and bool(_meta.get("reattach_in_progress")) is True
        )
        if not _verified:
            log.critical(
                "REATTACH_IN_PROGRESS_FENCE_READBACK_MISMATCH client=%s mode=%s "
                "canonical=%s local_order_id=%s row=%s metadata=%s",
                client_id, _mode, canonical_signal_id, local_order_id, _row, _meta,
            )
            return False
        return True
    except Exception as _fence_exc:
        log.critical(
            "REATTACH_IN_PROGRESS_FENCE_EXCEPTION client=%s mode=%s canonical=%s "
            "local_order_id=%s err=%s",
            client_id, execution_mode, canonical_signal_id,
            local_order_id, _fence_exc,
        )
        return False


def _persist_reattach_terminal_suppression_marker(
    *,
    client_id: str,
    execution_mode: str,
    canonical_signal_id: str,
    signal_id: str,
    signal_payload: dict,
    local_order_id: str,
    session_key: str,
    reason: str,
    post_status: str,
    post_row_status: str,
) -> bool:
    """Persist and verify a durable terminal marker for a REATTACH outcome.

    The caller may have proved the exact order is terminal, or may be unable
    to prove whether watch() terminalized it. In both cases, a retry must not
    classify the setup as NEW just because the active-order fence no longer
    sees a nonterminal ENTRY row.
    """
    _mode = str(execution_mode or "").strip().lower()
    _session = str(session_key or _overnight_reeval_session_key()).strip()
    _reason = str(reason or "reattach_post_watch_unknown").strip()
    if not (
        client_id and _mode in {"live", "paper"} and canonical_signal_id
        and signal_id and local_order_id and _session
    ):
        log.critical(
            "REATTACH_TERMINAL_SUPPRESSION_MARKER_INVALID_INPUT "
            "client=%s mode=%s canonical=%s signal=%s local_order_id=%s session=%s",
            client_id, execution_mode, canonical_signal_id,
            signal_id, local_order_id, session_key,
        )
        return False

    try:
        from ap.opportunity_ledger import (
            create_opportunities as _create_opps,
            mark_watcher_invalidated as _mark_watcher_invalidated,
        )

        _sig_payload = dict(signal_payload or {})
        _sig_payload.setdefault("signal_id", signal_id)
        _created = _create_opps(
            signal_id, [client_id], _sig_payload,
            canonical_signal_id=canonical_signal_id,
        )
        if int(_created or 0) < 1:
            log.critical(
                "REATTACH_TERMINAL_SUPPRESSION_CREATE_FAILED "
                "client=%s mode=%s canonical=%s local_order_id=%s reason=%s",
                client_id, _mode, canonical_signal_id, local_order_id, _reason,
            )
            return False

        _marker_meta = {
            "reattach_terminal_suppression": True,
            "execution_mode": _mode,
            "overnight_reeval_session_key": _session,
            "source_table": "ap_signals",
            "original_signal_id": signal_id,
            "canonical_signal_id": canonical_signal_id,
            "local_order_id": str(local_order_id),
            "reattach_post_watch_status": str(post_status or ""),
            "reattach_post_watch_row_status": str(post_row_status or ""),
            "reattach_terminal_suppressed_at": datetime.now(timezone.utc).isoformat(),
        }
        _marked = _mark_watcher_invalidated(
            signal_id,
            client_id,
            _reason,
            canonical_signal_id=canonical_signal_id,
            order_local_id=str(local_order_id),
            extra_meta=_marker_meta,
        )
        if not _marked:
            log.critical(
                "REATTACH_TERMINAL_SUPPRESSION_MARK_FAILED "
                "client=%s mode=%s canonical=%s local_order_id=%s reason=%s",
                client_id, _mode, canonical_signal_id, local_order_id, _reason,
            )
            return False

        _readback = _get_client_opportunity_row(signal_id, client_id, _sig_payload)
        if _readback.lookup_status != _LS_FOUND or not isinstance(_readback.row, dict):
            log.critical(
                "REATTACH_TERMINAL_SUPPRESSION_READBACK_MISSING "
                "client=%s mode=%s canonical=%s local_order_id=%s status=%s err=%s",
                client_id, _mode, canonical_signal_id, local_order_id,
                _readback.lookup_status, _readback.error,
            )
            return False

        _row = _readback.row
        _meta = _row.get("metadata") if isinstance(_row.get("metadata"), dict) else {}
        _row_canonical = str(_row.get("canonical_signal_id") or _readback.canonical_signal_id or "")
        _row_client = str(_row.get("client_id") or client_id)
        _row_status = str(_row.get("opportunity_status") or "").upper()
        _row_stage = str(_row.get("miss_stage") or "").upper()
        _row_reason = str(_row.get("miss_reason") or "")
        _row_order = str(_row.get("order_local_id") or _meta.get("local_order_id") or "")
        _row_mode = str(_meta.get("execution_mode") or _meta.get("mode") or "").strip().lower()
        _row_session = str(_meta.get("overnight_reeval_session_key") or "").strip()

        _verified = (
            _row_canonical == canonical_signal_id
            and _row_client == client_id
            and _row_status == "MISSED"
            and _row_stage == "WATCHER_ARM"
            and _row_reason == _reason
            and _row_mode == _mode
            and _row_session == _session
            and _row_order == str(local_order_id)
        )
        if not _verified:
            log.critical(
                "REATTACH_TERMINAL_SUPPRESSION_READBACK_MISMATCH "
                "client=%s mode=%s canonical=%s local_order_id=%s "
                "row_canonical=%s row_client=%s row_status=%s row_stage=%s "
                "row_reason=%s row_mode=%s row_session=%s row_order=%s",
                client_id, _mode, canonical_signal_id, local_order_id,
                _row_canonical, _row_client, _row_status, _row_stage,
                _row_reason, _row_mode, _row_session, _row_order,
            )
            return False

        return True
    except Exception as _marker_exc:
        log.critical(
            "REATTACH_TERMINAL_SUPPRESSION_MARKER_EXCEPTION "
            "client=%s mode=%s canonical=%s local_order_id=%s reason=%s err=%s",
            client_id, execution_mode, canonical_signal_id,
            local_order_id, _reason, _marker_exc,
        )
        return False


def _persist_watcher_armed_proof(
    *,
    client_id: str,
    execution_mode: str,
    canonical_signal_id: str,
    signal_id: str,
    signal_payload: dict,
    local_order_id: str,
    session_key: str,
    extra_meta: Optional[dict] = None,
    clear_reattach_in_progress: bool = False,
) -> bool:
    """Persist WATCHER_ARMED and verify the row actually transitioned.

    opportunity_ledger.update_opportunity() returns True when the update
    query succeeds, even if its monotonic guard preserved a prior terminal
    status and only merged metadata. This helper treats the write as durable
    proof only after readback shows WATCHER_ARMED for the exact
    client/mode/session/order.
    """
    _mode = str(execution_mode or "").strip().lower()
    _session = str(session_key or _overnight_reeval_session_key()).strip()
    if not (
        client_id and _mode in {"live", "paper"} and canonical_signal_id
        and signal_id and local_order_id and _session
    ):
        log.critical(
            "WATCHER_ARMED_PROOF_INVALID_INPUT client=%s mode=%s canonical=%s "
            "signal=%s local_order_id=%s session=%s",
            client_id, execution_mode, canonical_signal_id,
            signal_id, local_order_id, session_key,
        )
        return False

    try:
        from ap.opportunity_ledger import (
            WATCHER_ARMED as _OL_WATCHER_ARMED,
            create_opportunities as _create_opps,
            mark_watcher_armed as _mark_watcher_armed,
        )

        _sig_payload = dict(signal_payload or {})
        _sig_payload.setdefault("signal_id", signal_id)
        _created = _create_opps(
            signal_id, [client_id], _sig_payload,
            canonical_signal_id=canonical_signal_id,
        )
        if int(_created or 0) < 1:
            log.critical(
                "WATCHER_ARMED_PROOF_CREATE_FAILED client=%s mode=%s "
                "canonical=%s local_order_id=%s",
                client_id, _mode, canonical_signal_id, local_order_id,
            )
            return False

        _proof_meta = dict(extra_meta or {})
        _proof_meta.update({
            "overnight_reeval_session_key": _session,
            "execution_mode": _mode,
            "original_signal_id": signal_id,
            "canonical_signal_id": canonical_signal_id,
            "local_order_id": str(local_order_id),
        })
        _marked = _mark_watcher_armed(
            canonical_signal_id,
            client_id,
            canonical_signal_id=canonical_signal_id,
            order_local_id=str(local_order_id),
            extra_meta=_proof_meta,
        )
        if not _marked:
            log.critical(
                "WATCHER_ARMED_PROOF_MARK_FAILED client=%s mode=%s "
                "canonical=%s local_order_id=%s",
                client_id, _mode, canonical_signal_id, local_order_id,
            )
            return False

        def _readback_verified(*, expect_reattach_cleared: bool) -> bool:
            _readback = _get_client_opportunity_row(signal_id, client_id, _sig_payload)
            if _readback.lookup_status != _LS_FOUND or not isinstance(_readback.row, dict):
                log.critical(
                    "WATCHER_ARMED_PROOF_READBACK_MISSING client=%s mode=%s "
                    "canonical=%s local_order_id=%s status=%s err=%s",
                    client_id, _mode, canonical_signal_id, local_order_id,
                    _readback.lookup_status, _readback.error,
                )
                return False

            _row = _readback.row
            _meta = _row.get("metadata") if isinstance(_row.get("metadata"), dict) else {}
            _row_canonical = str(_row.get("canonical_signal_id") or _readback.canonical_signal_id or "")
            _row_client = str(_row.get("client_id") or client_id)
            _row_status = str(_row.get("opportunity_status") or "").upper()
            _row_order = str(_row.get("order_local_id") or _meta.get("local_order_id") or "")
            _row_mode = str(_meta.get("execution_mode") or _meta.get("mode") or "").strip().lower()
            _row_session = str(_meta.get("overnight_reeval_session_key") or "").strip()
            _reattach_flag = bool(_meta.get("reattach_in_progress"))
            _verified = (
                _row_canonical == canonical_signal_id
                and _row_client == client_id
                and _row_status == _OL_WATCHER_ARMED
                and _row_order == str(local_order_id)
                and _row_mode == _mode
                and _row_session == _session
                and (not expect_reattach_cleared or not _reattach_flag)
            )
            if not _verified:
                log.critical(
                    "WATCHER_ARMED_PROOF_READBACK_MISMATCH client=%s mode=%s "
                    "canonical=%s local_order_id=%s row_status=%s row_order=%s "
                    "row_mode=%s row_session=%s reattach_in_progress=%s row=%s metadata=%s",
                    client_id, _mode, canonical_signal_id, local_order_id,
                    _row_status, _row_order, _row_mode, _row_session,
                    _reattach_flag, _row, _meta,
                )
                return False
            return True

        if not _readback_verified(expect_reattach_cleared=False):
            return False

        if clear_reattach_in_progress:
            _clear_meta = dict(_proof_meta)
            _clear_meta.update({
                "reattach_in_progress": False,
                "reattach_in_progress_cleared_at": datetime.now(timezone.utc).isoformat(),
            })
            _cleared = _mark_watcher_armed(
                canonical_signal_id,
                client_id,
                canonical_signal_id=canonical_signal_id,
                order_local_id=str(local_order_id),
                extra_meta=_clear_meta,
            )
            if not _cleared:
                log.critical(
                    "WATCHER_ARMED_PROOF_CLEAR_FAILED client=%s mode=%s "
                    "canonical=%s local_order_id=%s",
                    client_id, _mode, canonical_signal_id, local_order_id,
                )
                return False
            return _readback_verified(expect_reattach_cleared=True)

        return True
    except Exception as _proof_exc:
        log.critical(
            "WATCHER_ARMED_PROOF_EXCEPTION client=%s mode=%s canonical=%s "
            "local_order_id=%s err=%s",
            client_id, execution_mode, canonical_signal_id,
            local_order_id, _proof_exc,
        )
        return False


def _persist_reattach_post_watch_ambiguity(
    *,
    client_id: str,
    execution_mode: str,
    canonical_signal_id: str,
    signal_id: str,
    signal_payload: dict,
    local_order_id: str,
    session_key: str,
    post_status: str,
    post_row_status: str,
) -> bool:
    """Persist a durable AMBIGUOUS opportunity so the NEXT reeval attempt
    cannot classify the setup as NEW and create a replacement ENTRY.

    Returns True only after create, terminal mark, and read-back verification.
    A False return is a hard failure that MUST be treated as fail-closed
    retryable by the caller.
    """
    return _persist_reattach_terminal_suppression_marker(
        client_id=client_id,
        execution_mode=execution_mode,
        canonical_signal_id=canonical_signal_id,
        signal_id=signal_id,
        signal_payload=signal_payload,
        local_order_id=local_order_id,
        session_key=session_key,
        reason="reattach_post_watch_ambiguous_terminal_or_missing",
        post_status=post_status,
        post_row_status=post_row_status,
    )


# ── Disposition constants ─────────────────────────────────────────────────────

_DISPOSITION_ALREADY_ARMED       = "ALREADY_ARMED"
_DISPOSITION_ALREADY_OWNED       = "ALREADY_OWNED"       # broker-submitted / filled
_DISPOSITION_ALREADY_TERMINAL    = "ALREADY_TERMINAL"
_DISPOSITION_REATTACH_WATCHER    = "REATTACH_WATCHER"    # PENDING_TRIGGER order exists, no watcher proof
_DISPOSITION_ACTIVE_CREATED_RETRY = "ACTIVE_CREATED_RETRY"  # exact CREATED order — retry, never re-enter
_DISPOSITION_RETRYABLE           = "RETRYABLE"
_DISPOSITION_NEW                 = "NEW"
_DISPOSITION_LOOKUP_FAILED       = "LOOKUP_FAILED"
_DISPOSITION_AMBIGUOUS_OWNERSHIP = "AMBIGUOUS_OWNERSHIP"  # fail closed as retryable


class _DispositionResult(NamedTuple):
    """Full disposition result for a shared ap_signals row.

    .disposition            — one of the _DISPOSITION_* constants
    .existing_local_order_id — set for REATTACH_WATCHER; the order to reuse
    .existing_order_row     — set for REATTACH_WATCHER; full order dict
    """
    disposition: str
    existing_local_order_id: Optional[str] = None
    existing_order_row: Optional[dict] = None


def _resolve_shared_setup_disposition(
    signal_id: str,
    client_id: str,
    signal: dict,
    execution_mode: str,
    *,
    session_key: Optional[str] = None,
) -> _DispositionResult:
    """Return the per-client disposition for a shared ap_signals row.

    Called ONLY for rows whose source is ap_signals (job_id starts 'sup:').
    Must run before master_control, OSM, selector, or broker calls.

    Processing order (10 steps per spec):
      1. Normalize canonical signal identity.
      2. Normalize exact execution mode.
      3. Determine overnight session key.
      4. Query opportunity ledger → FOUND / NOT_FOUND / LOOKUP_FAILED.
      5. If FOUND: inspect durable evidence (mode, session, status).
      6. Query exact active local ENTRY ownership.
      7. Resolve disposition from active order if found.
      8. Resolve NEW vs RETRYABLE from opportunity evidence.
      9. Ambiguous → fail closed.
     10. Return _DispositionResult.

    Mode isolation: blank stored mode never establishes ownership.
    Session isolation: blank stored session never counts as current session.
    Terminal coverage: uses canonical sets imported from opportunity_ledger.
    """
    from ap.opportunity_ledger import (
        WATCHER_ARMED            as _OL_WATCHER_ARMED,
        BROKER_SUBMITTED         as _OL_BROKER_SUBMITTED,
        BROKER_ACKED             as _OL_BROKER_ACKED,
        FILLED                   as _OL_FILLED,
        TERMINAL_STATUSES        as _OL_TERMINAL_STATUSES,
    )

    # Step 1: Normalize canonical signal identity.
    lookup = _get_client_opportunity_row(signal_id, client_id, signal)
    canonical = lookup.canonical_signal_id

    if not canonical:
        log.warning(
            "[%s] reeval disposition=LOOKUP_FAILED cannot resolve canonical_signal_id signal=%s",
            client_id, signal_id,
        )
        return _DispositionResult(_DISPOSITION_LOOKUP_FAILED)

    # Step 2: Normalize exact execution mode (blank = unknown = fail closed).
    _req_mode = str(execution_mode or "").strip().lower()
    if not _req_mode or _req_mode not in {"live", "paper"}:
        log.warning(
            "[%s] reeval disposition=LOOKUP_FAILED unknown execution_mode=%r canonical=%s",
            client_id, execution_mode, canonical,
        )
        return _DispositionResult(_DISPOSITION_LOOKUP_FAILED)

    # Step 3: Determine overnight session key.
    _session_key = session_key or _overnight_reeval_session_key()

    # Step 4: Query opportunity ledger.
    if lookup.lookup_status == _LS_LOOKUP_FAILED:
        log.warning(
            "[%s] reeval disposition=LOOKUP_FAILED opportunity-store error canonical=%s err=%s",
            client_id, canonical, lookup.error,
        )
        return _DispositionResult(_DISPOSITION_LOOKUP_FAILED)

    # Step 5: If FOUND, inspect durable evidence with exact mode + session guards.
    _has_current_session_proof = False
    _meta: dict = {}
    _attempt_scope: dict = {}
    _attempt_scope_state = ""
    _attempt_scope_count = 0
    _attempt_scope_token = ""
    _attempt_scope_order_id = ""
    _retry_scope_requires_terminal_proof = False

    if lookup.lookup_status == _LS_FOUND:
        row = lookup.row
        _status = str(row.get("opportunity_status") or "").upper()
        _stage  = str(row.get("miss_stage") or "").upper()
        _reason = str(row.get("miss_reason") or "")
        _meta   = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}

        _attempt_scope = _attempt_meta_scope(
            _meta,
            _req_mode,
            _session_key,
        )
        (
            _attempt_scope_valid,
            _attempt_scope_state,
            _attempt_scope_count,
            _attempt_scope_token,
            _attempt_scope_order_id,
            _attempt_scope_failure,
        ) = _parse_attempt_scope_strict(_attempt_scope)

        if not _attempt_scope_valid:
            log.critical(
                "[%s] reeval disposition=AMBIGUOUS_OWNERSHIP "
                "malformed watcher-attempt scope canonical=%s mode=%s "
                "session=%s reason=%s",
                client_id,
                canonical,
                _req_mode,
                _session_key,
                _attempt_scope_failure,
            )
            return _DispositionResult(_DISPOSITION_AMBIGUOUS_OWNERSHIP)

        _retry_scope_requires_terminal_proof = bool(
            _attempt_scope_state == WATCH_ATTEMPT_STATE_RETRYABLE
            and _attempt_scope_order_id
        )

        # Mode isolation: blank stored mode NEVER establishes ownership.
        _recorded_mode = str(_meta.get("execution_mode") or _meta.get("mode") or "").strip().lower()
        _mode_match = bool(_recorded_mode and _recorded_mode == _req_mode)

        # Session isolation: blank stored session NEVER counts as current.
        _recorded_session = str(_meta.get("overnight_reeval_session_key") or "").strip()
        _session_match = bool(_recorded_session and _recorded_session == _session_key)
        _reattach_in_progress = bool(_meta.get("reattach_in_progress"))

        if _mode_match and _session_match:
            _has_current_session_proof = True

            # PR #388 nonterminal-ownership-poisoning amendment
            # ────────────────────────────────────────────────
            # opportunity_ledger.update_opportunity() blocks lifecycle
            # regression but continues to write identifier + metadata fields
            # and returns True. That means a prior WATCHER_ARMED /
            # BROKER_SUBMITTED / BROKER_ACKED / FILLED row can retain its
            # stale lifecycle status while accepting current-session
            # metadata (mode, session key, local_order_id, and
            # reattach_in_progress=true) from _persist_reattach_in_progress_
            # fence(). Trusting that stale status immediately here would
            # return ALREADY_ARMED or ALREADY_OWNED before the exact
            # active-order fence runs, stranding a live PENDING_TRIGGER
            # order when watch() later fails and the marker stays set.
            #
            # Contract: when reattach_in_progress=true for the exact
            # current mode/session/order, defer ALL opportunity-based
            # ownership conclusions — WATCHER_ARMED, broker-owned,
            # terminal — and let the exact active-order fence decide.
            # The marker itself must remain until watch() succeeds and
            # _persist_watcher_armed_proof() explicitly clears it.

            # ALREADY_ARMED: durable WATCHER_ARMED proof for this client/mode/session.
            if _status == _OL_WATCHER_ARMED:
                if _reattach_in_progress:
                    log.warning(
                        "[%s] reeval: current-session WATCHER_ARMED opportunity "
                        "carries reattach_in_progress=true; deferring ALREADY_ARMED "
                        "disposition until exact active-order fence runs canonical=%s "
                        "session=%s",
                        client_id, canonical, _session_key,
                    )
                else:
                    log.info(
                        "[%s] reeval disposition=ALREADY_ARMED canonical=%s session=%s",
                        client_id, canonical, _session_key,
                    )
                    return _DispositionResult(_DISPOSITION_ALREADY_ARMED)

            # ALREADY_OWNED: broker-submitted, acked, or filled — entry is in flight.
            if _status in {_OL_BROKER_SUBMITTED, _OL_BROKER_ACKED, _OL_FILLED}:
                if _reattach_in_progress:
                    log.warning(
                        "[%s] reeval: current-session broker-owned opportunity "
                        "(status=%s) carries reattach_in_progress=true; deferring "
                        "ALREADY_OWNED disposition until exact active-order fence "
                        "runs canonical=%s session=%s",
                        client_id, _status, canonical, _session_key,
                    )
                else:
                    log.info(
                        "[%s] reeval disposition=ALREADY_OWNED canonical=%s session=%s status=%s",
                        client_id, canonical, _session_key, _status,
                    )
                    return _DispositionResult(_DISPOSITION_ALREADY_OWNED)

            # ALREADY_TERMINAL: canonical terminal set from opportunity_ledger.
            if _status in _OL_TERMINAL_STATUSES:
                if _reattach_in_progress:
                    log.warning(
                        "[%s] reeval: current-session terminal opportunity carries "
                        "reattach_in_progress=true; deferring terminal disposition "
                        "until exact active-order fence runs canonical=%s session=%s "
                        "status=%s stage=%s reason=%s",
                        client_id, canonical, _session_key, _status, _stage, _reason,
                    )
                elif _retry_scope_requires_terminal_proof:
                    log.info(
                        "[%s] reeval: deferring terminal opportunity evidence until "
                        "exact bound prior order is checked canonical=%s session=%s "
                        "status=%s scope_order=%s",
                        client_id, canonical, _session_key, _status,
                        _attempt_scope_order_id,
                    )
                else:
                    log.info(
                        "[%s] reeval disposition=ALREADY_TERMINAL canonical=%s session=%s "
                        "status=%s stage=%s reason=%s",
                        client_id, canonical, _session_key, _status, _stage, _reason,
                    )
                    return _DispositionResult(_DISPOSITION_ALREADY_TERMINAL)

            # Legacy terminal watcher-arm failure recorded in same-session.
            if (_status == "MISSED" and _stage == "WATCHER_ARM"
                    and _is_terminal_watch_arm_failure_reason(_reason)):
                if _reattach_in_progress:
                    log.warning(
                        "[%s] reeval: current-session watcher-arm terminal marker "
                        "carries reattach_in_progress=true; deferring terminal "
                        "disposition until exact active-order fence runs canonical=%s "
                        "session=%s reason=%s",
                        client_id, canonical, _session_key, _reason,
                    )
                elif _retry_scope_requires_terminal_proof:
                    log.info(
                        "[%s] reeval: deferring terminal watcher-arm opportunity "
                        "evidence until exact bound prior order is checked "
                        "canonical=%s session=%s scope_order=%s",
                        client_id, canonical, _session_key, _attempt_scope_order_id,
                    )
                else:
                    log.info(
                        "[%s] reeval disposition=ALREADY_TERMINAL (watcher-arm-failure) "
                        "canonical=%s session=%s",
                        client_id, canonical, _session_key,
                    )
                    return _DispositionResult(_DISPOSITION_ALREADY_TERMINAL)

            if (_status == "INTERNAL_ERROR"
                    and _is_terminal_watch_arm_failure_reason(_reason)):
                if _reattach_in_progress:
                    log.warning(
                        "[%s] reeval: current-session internal-error terminal marker "
                        "carries reattach_in_progress=true; deferring terminal "
                        "disposition until exact active-order fence runs canonical=%s "
                        "session=%s reason=%s",
                        client_id, canonical, _session_key, _reason,
                    )
                elif _retry_scope_requires_terminal_proof:
                    log.info(
                        "[%s] reeval: deferring terminal internal-error opportunity "
                        "evidence until exact bound prior order is checked "
                        "canonical=%s session=%s scope_order=%s",
                        client_id, canonical, _session_key, _attempt_scope_order_id,
                    )
                else:
                    log.info(
                        "[%s] reeval disposition=ALREADY_TERMINAL (internal-error) "
                        "canonical=%s session=%s",
                        client_id, canonical, _session_key,
                    )
                    return _DispositionResult(_DISPOSITION_ALREADY_TERMINAL)

        # Different mode or session: fall through to active-order check below.

    # Step 6: Query exact active local ENTRY ownership (durable DB truth).
    _order_status, _order_row = _query_active_entry_order(client_id, _req_mode, canonical)

    # Active-order lookup failure → fail closed (do not assume no order exists).
    if _order_status == _LS_LOOKUP_FAILED:
        log.warning(
            "[%s] reeval disposition=LOOKUP_FAILED active-order query failed canonical=%s mode=%s",
            client_id, canonical, _req_mode,
        )
        return _DispositionResult(_DISPOSITION_LOOKUP_FAILED)

    # Step 7: Resolve from active order.
    if _order_status == _LS_FOUND and _order_row:
        _active_status = str(_order_row.get("status") or "").upper()
        _existing_local_id = str(_order_row.get("local_order_id") or "").strip()

        _active_disp = _entry_order_disposition_from_status(
            client_id=client_id,
            canonical_signal_id=canonical,
            session_key=_session_key,
            status=_active_status,
            local_order_id=_existing_local_id,
            order_row=_order_row,
            source="active-entry-fence",
        )
        if _active_disp is not None:
            return _active_disp

        log.warning(
            "[%s] reeval disposition=AMBIGUOUS_OWNERSHIP active order has "
            "unrecognized status canonical=%s status=%s",
            client_id, canonical, _active_status,
        )
        return _DispositionResult(_DISPOSITION_AMBIGUOUS_OWNERSHIP)

    # Step 7b: No active order. Check latest exact ENTRY without a status
    # filter before any path can return NEW. Terminal order history is durable
    # no-replacement truth even when opportunity marker storage failed.
    _latest_status, _latest_row = _query_latest_entry_order_no_status(
        client_id, _req_mode, canonical,
    )
    if _latest_status == _LS_LOOKUP_FAILED:
        log.warning(
            "[%s] reeval disposition=LOOKUP_FAILED latest-entry query failed "
            "canonical=%s mode=%s",
            client_id,
            canonical,
            _req_mode,
        )
        return _DispositionResult(_DISPOSITION_LOOKUP_FAILED)

    if _latest_status == _LS_FOUND and _latest_row:
        _latest_order_status = str(
            _latest_row.get("status") or ""
        ).upper().strip()
        _latest_local_id = str(
            _latest_row.get("local_order_id") or ""
        ).strip()

        if _retry_scope_requires_terminal_proof:
            if _latest_local_id != _attempt_scope_order_id:
                log.critical(
                    "[%s] reeval disposition=AMBIGUOUS_OWNERSHIP "
                    "retry-scope order mismatch canonical=%s mode=%s "
                    "session=%s scope_order=%s latest_order=%s "
                    "latest_status=%s",
                    client_id,
                    canonical,
                    _req_mode,
                    _session_key,
                    _attempt_scope_order_id,
                    _latest_local_id,
                    _latest_order_status,
                )
                return _DispositionResult(
                    _DISPOSITION_AMBIGUOUS_OWNERSHIP
                )

            if _latest_order_status in _TERMINAL_ENTRY_STATUSES:
                log.info(
                    "[%s] reeval disposition=NEW exact RETRYABLE owner "
                    "proved prior order terminal canonical=%s mode=%s "
                    "session=%s local_order_id=%s status=%s count=%s",
                    client_id,
                    canonical,
                    _req_mode,
                    _session_key,
                    _latest_local_id,
                    _latest_order_status,
                    _attempt_scope_count,
                )
                return _DispositionResult(_DISPOSITION_NEW)

        _latest_disp = _entry_order_disposition_from_status(
            client_id=client_id,
            canonical_signal_id=canonical,
            session_key=_session_key,
            status=_latest_order_status,
            local_order_id=_latest_local_id,
            order_row=_latest_row,
            source="latest-entry-no-status-fence",
        )
        if _latest_disp is not None:
            return _latest_disp

        log.warning(
            "[%s] reeval disposition=AMBIGUOUS_OWNERSHIP latest entry has "
            "unrecognized status canonical=%s status=%s",
            client_id,
            canonical,
            _latest_order_status,
        )
        return _DispositionResult(_DISPOSITION_AMBIGUOUS_OWNERSHIP)

    # Step 8: No active order — resolve from opportunity evidence.
    if lookup.lookup_status == _LS_NOT_FOUND:
        # Both opportunity AND order queries definitively returned zero rows.
        log.debug(
            "[%s] reeval disposition=NEW canonical=%s session=%s",
            client_id, canonical, _session_key,
        )
        return _DispositionResult(_DISPOSITION_NEW)

    if _has_current_session_proof:
        # PR #404 Blocker 1: classify the exact current-session watcher-attempt
        # scope. The required ordering is:
        #   exact active order (Step 7)
        #   → exact RETRYABLE scope + exact same terminal prior order (Step 7b)
        #   → ordinary terminal-history suppression (here).
        # Terminal history is NOT globally ignored.
        if _attempt_scope_state == WATCH_ATTEMPT_STATE_ARMED:
            return _DispositionResult(_DISPOSITION_ALREADY_ARMED)

        if _attempt_scope_state in {
            WATCH_ATTEMPT_STATE_EXHAUSTED,
            WATCH_ATTEMPT_STATE_ERROR,
        }:
            return _DispositionResult(_DISPOSITION_ALREADY_TERMINAL)

        if _attempt_scope_state == WATCH_ATTEMPT_STATE_IN_PROGRESS:
            return _DispositionResult(
                _DISPOSITION_AMBIGUOUS_OWNERSHIP
            )

        if _attempt_scope_state == WATCH_ATTEMPT_STATE_RETRYABLE:
            if _attempt_scope_order_id:
                # An exact terminal order would already have returned NEW in
                # Step 7b. Reaching this point means terminal proof is missing.
                return _DispositionResult(
                    _DISPOSITION_AMBIGUOUS_OWNERSHIP
                )

            # Only the pristine RETRYABLE/count=0 seed may delegate its first
            # acquisition without a bound prior order.
            if (
                _attempt_scope_count == 0
                and not _attempt_scope_token
            ):
                return _DispositionResult(_DISPOSITION_NEW)

            return _DispositionResult(
                _DISPOSITION_AMBIGUOUS_OWNERSHIP
            )

        log.warning(
            "[%s] reeval disposition=AMBIGUOUS_OWNERSHIP "
            "unclassified current-session opportunity canonical=%s session=%s",
            client_id,
            canonical,
            _session_key,
        )
        return _DispositionResult(_DISPOSITION_AMBIGUOUS_OWNERSHIP)

    # Step 9: Old-session or different-mode row with no active order.
    # Safe to treat as NEW for this session + mode.
    log.debug(
        "[%s] reeval disposition=NEW (prior-session/mode record, no active order) "
        "canonical=%s session=%s mode=%s",
        client_id, canonical, _session_key, _req_mode,
    )
    return _DispositionResult(_DISPOSITION_NEW)


def _shared_watch_arm_failure_already_recorded(
    signal_id: str,
    client_id: str,
    signal: dict,
    *,
    session_key: Optional[str] = None,
) -> bool:
    """Legacy compatibility shim — delegates to the full disposition resolver.

    Returns True when disposition is ALREADY_ARMED or ALREADY_TERMINAL,
    preserving callers that used the old boolean interface.
    Do not add new callers; prefer _resolve_shared_setup_disposition directly.
    """
    disp_result = _resolve_shared_setup_disposition(
        signal_id, client_id, signal,
        execution_mode="paper",   # legacy callers don't pass mode; default paper avoids LOOKUP_FAILED
        session_key=session_key,
    )
    return disp_result.disposition in (_DISPOSITION_ALREADY_ARMED, _DISPOSITION_ALREADY_TERMINAL)


def _record_watch_arm_failure_proof(
    *,
    signal_id: str,
    client_id: str,
    signal: dict,
    reason: str,
    local_order_id: str,
    job_id,
    is_exception: bool,
    session_key: Optional[str] = None,
    cleanup_method: Optional[str] = None,
    cleanup_success: Optional[bool] = None,
    cleanup_failed: bool = False,
    original_reason: Optional[str] = None,
    retryable_exception: bool = False,
) -> None:
    try:
        from ap.opportunity_ledger import (
            CREATED as _OL_CREATED,
            STAGE_WATCHER_ARM as _OL_STAGE_WATCHER_ARM,
            create_opportunities,
            mark_internal_error,
            mark_watcher_invalidated,
            update_opportunity,
        )
        canonical_signal_id = _resolve_canonical_signal_id(signal_id, signal)
        _payload = dict(signal or {})
        _payload.setdefault("signal_id", signal_id)
        create_opportunities(
            signal_id,
            [client_id],
            _payload,
            canonical_signal_id=canonical_signal_id,
        )
        _extra_meta = {
            "overnight_watch_arm_failure": True,
            "overnight_source_job_id": str(job_id),
            "overnight_source_table": (
                "ap_signals" if str(job_id).startswith("sup:") else "trade_queue"
            ),
            "overnight_reeval_session_key": session_key or _overnight_reeval_session_key(),
        }
        if cleanup_method is not None:
            _extra_meta["cleanup_method"] = str(cleanup_method)
        if cleanup_success is not None:
            _extra_meta["cleanup_success"] = bool(cleanup_success)
        if cleanup_failed:
            _extra_meta["overnight_watch_arm_cleanup_failed"] = True
        if original_reason is not None:
            _extra_meta["original_reason"] = str(original_reason)
        if is_exception and retryable_exception:
            # A RETRYABLE watcher exception has already been recorded on
            # the durable attempt scope. Writing INTERNAL_ERROR / any other
            # terminal opportunity status here would poison the disposition
            # resolver on the next run (ALREADY_TERMINAL) and permanently
            # block the retry the attempt scope explicitly authorized.
            # Merge diagnostic metadata via update_opportunity(..., CREATED, ...)
            # so the monotonic status rule preserves any higher nonterminal
            # current status while still surfacing the exception context.
            update_opportunity(
                signal_id,
                client_id,
                _OL_CREATED,
                canonical_signal_id=canonical_signal_id,
                miss_stage=_OL_STAGE_WATCHER_ARM,
                miss_reason=reason,
                order_local_id=str(local_order_id or ""),
                extra_meta={
                    **_extra_meta,
                    "overnight_watch_arm_retryable_exception": True,
                    "retryable": True,
                },
            )
        elif is_exception:
            mark_internal_error(
                signal_id,
                client_id,
                reason,
                canonical_signal_id=canonical_signal_id,
                order_local_id=str(local_order_id or ""),
                extra_meta=_extra_meta,
            )
        else:
            mark_watcher_invalidated(
                signal_id,
                client_id,
                reason,
                canonical_signal_id=canonical_signal_id,
                order_local_id=str(local_order_id or ""),
                extra_meta=_extra_meta,
            )
    except Exception as exc:
        log.warning(
            "[%s] overnight_reeval: failed to persist watch-arm failure proof "
            "signal=%s local_order_id=%s error=%s",
            client_id,
            signal_id,
            local_order_id,
            exc,
        )


def _classify_overnight_reeval_result(result: dict) -> dict:
    """Attach the operational completion/retry contract to a result dict."""
    if not isinstance(result, dict):
        result = {}
    fetched = int(result.get("fetched", 0) or 0)
    errors = int(result.get("errors", 0) or 0)
    stalled = bool(result.get("stalled"))
    skipped = int(result.get("skipped", 0) or 0)
    armed = int(result.get("armed", 0) or 0)
    terminal_rejected = int(result.get("terminal_rejected", result.get("rejected", 0)) or 0)
    terminal_errors = int(result.get("terminal_errors", errors) or 0)
    retryable_deferred = int(
        result.get("retryable_deferred", fetched if stalled and fetched > 0 else 0) or 0
    )
    already_resolved = int(result.get("already_resolved", 0) or 0)
    unresolved = int(
        result.get(
            "unresolved",
            max(
                0,
                fetched
                - armed
                - terminal_rejected
                - terminal_errors
                - retryable_deferred
                - already_resolved,
            ),
        )
        or 0
    )

    if skipped == -1:
        result_class = "SKIPPED_NOT_DUE"
        completed = False
        retryable = False
        retry_reason = None
    elif errors > 0 or terminal_errors > 0:
        result_class = "RETRYABLE_ROW_ERRORS"
        completed = False
        retryable = True
        retry_reason = "row_errors"
    elif retryable_deferred > 0:
        if armed > 0 or terminal_rejected > 0 or already_resolved > 0:
            result_class = "RETRYABLE_PARTIAL_DEFERRED"
            retry_reason = "retryable_rows_remain"
        else:
            result_class = "RETRYABLE_ALL_DEFERRED"
            retry_reason = "all_fetched_rows_deferred"
        completed = False
        retryable = True
    elif unresolved > 0:
        result_class = "RETRYABLE_ROW_ERRORS"
        completed = False
        retryable = True
        retry_reason = "unresolved_rows"
    elif fetched == 0:
        result_class = "COMPLETED_NO_WORK"
        completed = True
        retryable = False
        retry_reason = None
    else:
        result_class = "COMPLETED_WITH_DECISIONS"
        completed = True
        retryable = False
        retry_reason = None

    result["armed"] = armed
    result["terminal_rejected"] = terminal_rejected
    result["terminal_errors"] = terminal_errors
    result["retryable_deferred"] = retryable_deferred
    result["already_resolved"] = already_resolved
    result["unresolved"] = unresolved
    result["stalled"] = bool(
        retryable_deferred > 0
        and armed == 0
        and terminal_rejected == 0
        and terminal_errors == 0
        and already_resolved == 0
    )
    # PR #388 P0-2: partial-source inventory override. If either source
    # lookup failed, we cannot certify the run complete — even when every
    # observed row was processed. Downgrade to a retryable classification so
    # the runner never sets success_date on incomplete truth.
    if bool(result.get("source_lookup_partial")) and completed:
        result_class = "RETRYABLE_PARTIAL_SOURCE_INVENTORY"
        completed = False
        retryable = True
        retry_reason = "partial_source_inventory"

    result["result_class"] = result_class
    result["completed"] = completed
    result["retryable"] = retryable
    result["retry_reason"] = retry_reason
    return result


def run_overnight_reeval(
    *,
    client_id: str,
    broker,
    data_broker=None,
    master_control,
    contract_selector,
    order_state_machine,
    entry_watcher,
    position_manager=None,
    exit_eng=None,
    on_split_brain=None,
    force: bool = False,
) -> dict:
    """
    Re-evaluate all WATCHING signals for client_id.
    Called during the 9:00–9:45 AM ET handoff window. Contract selection is
    always deferred to the breach-time execution seam, so watcher ownership
    is established without waiting for pre-market option-chain work.

    Returns summary dict with core counts plus stale/fresh visibility.
    """
    from ap.overnight_daily_validator import (
        validate_overnight_daily_signal,
        fetch_market_snapshot,
        InvalidationReason,
    )

    # ── Lifecycle ledger + health registry ───────────────────────────────────
    try:
        from ap_lifecycle import (
            LEDGER as _LEDGER,
            SignalState as _SS,
            LifecycleOwner as _LO,
            signal_watching,
            signal_invalidated,
            signal_armed,
            signal_rejected as _sig_rejected,
            RejectionCategory as _RC,
            RejectionSeverity as _RS,
        )
        _lifecycle_ok = True
    except Exception:
        _lifecycle_ok = False

    try:
        from ap_health_registry import HEALTH as _OR_HEALTH
        _OR_HEALTH.heartbeat("ap_overnight_signal_manager")
    except Exception as _e:
        log.warning("overnight_health_heartbeat_failed: %s", _e)

    attached_data_broker = None
    if broker is not None:
        try:
            _attached_data_broker = inspect.getattr_static(broker, "data_broker")
        except AttributeError:
            _attached_data_broker = None
        if _attached_data_broker is not None:
            attached_data_broker = getattr(broker, "data_broker", None)
    market_data_broker = data_broker or attached_data_broker or broker
    try:
        from ap.authorization import execution_mode_for_broker as _exec_mode_for_broker
        _run_execution_mode = str(_exec_mode_for_broker(broker) or "").upper()
    except Exception:
        _run_execution_mode = ""
    log.info(
        "OVERNIGHT_REEVAL_MARKET_DATA_BROKER_SELECTED "
        "client_id=%s execution_mode=%s data_broker_present=%s "
        "broker_class=%s data_broker_class=%s market_data_broker_class=%s",
        client_id,
        _run_execution_mode or "UNKNOWN",
        bool(data_broker is not None or attached_data_broker is not None),
        type(broker).__name__ if broker is not None else "None",
        type(data_broker).__name__ if data_broker is not None else "None",
        type(market_data_broker).__name__ if market_data_broker is not None else "None",
    )
    if (
        _run_execution_mode == "PAPER"
        and data_broker is None
        and attached_data_broker is None
        and market_data_broker is broker
    ):
        log.warning(
            "PAPER_OVERNIGHT_DATA_BROKER_MISSING_USING_EXECUTION_BROKER "
            "client_id=%s execution_mode=%s broker_class=%s market_data_broker_class=%s",
            client_id,
            _run_execution_mode,
            type(broker).__name__ if broker is not None else "None",
            type(market_data_broker).__name__ if market_data_broker is not None else "None",
        )
    result = {
        "processed": 0,
        "armed": 0,
        "rejected": 0,
        "terminal_rejected": 0,
        "skipped": 0,
        "errors": 0,
        "terminal_errors": 0,
        "retryable_deferred": 0,
        "already_resolved": 0,
        "unresolved": 0,
        "stale_skipped": 0,
        "fresh_processed": 0,
        "fresh_armed": 0,
        "stale_inventory_only": False,
        # P0 (2026-07-02): silent-stall detection. Production paper reeval
        # reported plain "success" for 4 consecutive sessions while every
        # fetched row hit the RETRY_LATER branches (sandbox broker cannot
        # serve prior-day levels) and stayed WATCHING forever — 486 rows
        # frozen at awaiting_overnight_reeval with a green lock row. These
        # two fields make that condition visible in the return value so the
        # caller can persist an honest status instead of "success".
        "fetched": 0,
        "stalled": False,
    }

    # Guard: only run on trading days, 9:00-9:45 AM ET window (unless force=True)
    now_et = _et_now()
    session_key = _overnight_reeval_session_key(now_et)
    if not force:
        if not _is_trading_day(now_et):
            log.info("[%s] overnight_reeval: skipping — not a trading day", client_id)
            result["skipped"] = -1
            return _classify_overnight_reeval_result(result)
        _in_window = (now_et.hour == 9 and 0 <= now_et.minute < 45)
        if not _in_window:
            log.info("[%s] overnight_reeval: skipping — outside 9:00-9:45 AM ET window (now=%02d:%02d)",
                     client_id, now_et.hour, now_et.minute)
            result["skipped"] = -1
            return _classify_overnight_reeval_result(result)

    # Fetch WATCHING signals from both sources of truth AND their status.
    # Source failure must NEVER be interpreted as "completed no work" — that
    # was the exact silent-success bug in the prior amendment where a Postgres
    # or Supabase outage produced zero rows and the runner marked the day
    # successful.
    _fetch_result = _fetch_watching_signals_with_status(client_id)
    watching_signals = sorted(_fetch_result.rows or [], key=_overnight_signal_sort_key)
    result["fetched"] = len(watching_signals or [])
    result["trade_queue_status"] = _fetch_result.trade_queue_status
    result["ap_signals_status"]  = _fetch_result.ap_signals_status
    result["trade_queue_error"]  = _fetch_result.trade_queue_error
    result["ap_signals_error"]   = _fetch_result.ap_signals_error

    _tq_failed  = _fetch_result.trade_queue_status != _SOURCE_STATUS_SUCCESS
    _sup_failed = _fetch_result.ap_signals_status  != _SOURCE_STATUS_SUCCESS

    if _tq_failed and _sup_failed and not watching_signals:
        # Both sources failed and no rows visible → cannot certify empty
        # inventory. Explicit retryable classification; do NOT let this
        # collapse into COMPLETED_NO_WORK.
        log.error(
            "[%s] overnight_reeval: SOURCE_LOOKUP_FAILED trade_queue=%s ap_signals=%s "
            "tq_err=%r sup_err=%r — refusing to certify empty inventory",
            client_id,
            _fetch_result.trade_queue_status, _fetch_result.ap_signals_status,
            _fetch_result.trade_queue_error, _fetch_result.ap_signals_error,
        )
        result["result_class"] = "RETRYABLE_SOURCE_LOOKUP_FAILED"
        result["completed"]    = False
        result["retryable"]    = True
        result["retry_reason"] = "source_lookup_failed"
        result["source_lookup_failed"] = True
        # Preserve counters and skip the classifier's COMPLETED_NO_WORK
        # branch; return this exact shape so the runner never sets
        # success_date and never runs post-overnight handoff.
        return result

    if _tq_failed or _sup_failed:
        # Partial inventory. Process the rows we did see, but mark the run
        # retryable so the runner does not set success_date on incomplete
        # truth. This is applied after the loop as an override below.
        log.warning(
            "[%s] overnight_reeval: PARTIAL_SOURCE_INVENTORY trade_queue=%s ap_signals=%s "
            "processing %d rows but classifying run retryable",
            client_id,
            _fetch_result.trade_queue_status, _fetch_result.ap_signals_status,
            len(watching_signals),
        )
        result["source_lookup_partial"] = True

    if not watching_signals:
        log.info("[%s] overnight_reeval: no WATCHING signals found", client_id)
        return _classify_overnight_reeval_result(result)

    log.info("[%s] overnight_reeval: found %d WATCHING signals to reeval", client_id, len(watching_signals))

    today = now_et.date()
    prior_levels_by_ticker: dict[str, dict] = {}
    snapshot_by_ticker: dict[str, dict] = {}
    for job in watching_signals:
        result["processed"] += 1
        job_id = job["id"]
        signal = job["payload"]
        if isinstance(signal, str):
            import json; signal = json.loads(signal)
        signal_id = job.get("signal_id") or signal.get("signal_id", "")
        job_source = job.get("_source") or ("ap_signals" if str(job_id).startswith("sup:") else "trade_queue")
        ticker = signal.get("ticker") or signal.get("symbol", "?")
        _execution_mode = _run_execution_mode
        _early_canonical = _resolve_canonical_signal_id(signal_id, signal)

        # PR #404 final placement correction: already-admitted/materialized
        # watcher recovery must run before age/timeframe, prior-level, snapshot,
        # validation, Master Control, selector, or authorization admission gates.
        # The helper is recovery-only and refuses to claim a new/empty scope.
        _early_recovery = _recover_materialized_watch_before_admission(
            order_state_machine=order_state_machine,
            entry_watcher=entry_watcher,
            signal_id=signal_id,
            canonical_signal_id=_early_canonical,
            client_id=client_id,
            execution_mode=_execution_mode,
            session_key=session_key,
            signal_payload=signal,
            ticker=ticker,
            job_id=job_id,
            job_source=job_source,
        )
        if _early_recovery.handled:
            if _early_recovery.outcome == "ARMED":
                _mark_job_watching_armed(
                    job_id, client_id,
                    f"early_recovery:{_early_recovery.local_order_id}",
                )
                result["armed"] += 1
                if _early_recovery.reason == "early_materialized_recovery_armed":
                    result["fresh_armed"] += 1
            elif _early_recovery.outcome == "ALREADY_RESOLVED":
                result["skipped"] = result.get("skipped", 0) + 1
                result["already_resolved"] += 1
            elif _early_recovery.outcome == "RETRYABLE":
                _mark_job_watching_reason(
                    job_id, client_id, _early_recovery.reason,
                )
                result["skipped"] = result.get("skipped", 0) + 1
                result["retryable_deferred"] += 1
            else:
                _mark_job_error(job_id, client_id, _early_recovery.reason)
                result["errors"] += 1
                result["terminal_errors"] += 1
            continue

        side = _normalize_overnight_side(signal.get("side") or signal.get("direction"))
        if side not in {"CALL", "PUT"}:
            log.warning(
                "[%s] overnight_reeval: INVALID_OR_MISSING_SIDE signal=%s raw_side=%r raw_direction=%r — rejecting before trigger math",
                ticker,
                signal_id,
                signal.get("side"),
                signal.get("direction"),
            )
            _mark_job_rejected(job_id, client_id, "invalid_or_missing_side")
            result["rejected"] += 1
            result["terminal_rejected"] += 1
            continue
        signal["side"] = side
        signal["direction"] = side
        _paper_rescue_only = _paper_overnight_rescue_only_signal(
            signal,
            job_source=job_source,
            execution_mode=_execution_mode,
        )

        try:
            # Age check: skip stale signals
            sig_date = _signal_date(signal)
            if sig_date:
                age_days = (today - sig_date).days
                if age_days > OVERNIGHT_SIGNAL_MAX_AGE_DAYS:
                    log.info("[%s] overnight_reeval: skipping stale signal %s (age=%dd)", ticker, signal_id, age_days)
                    _mark_job_rejected(job_id, client_id, f"stale_signal:age={age_days}d")
                    if _lifecycle_ok:
                        try:
                            _sig_rejected(signal_id, ticker, _LO.OVERNIGHT_EVAL,
                                          f"stale_signal age={age_days}d",
                                          _RC.VALIDATION, "STALE_SIGNAL", _RS.INFO,
                                          age_days=age_days)
                        except Exception:
                            pass
                    result["rejected"] += 1
                    result["terminal_rejected"] += 1
                    result["stale_skipped"] += 1
                    continue
            result["fresh_processed"] += 1

            # Timeframe guard: overnight reeval is DAILY signals only.
            # 60m, 5m, 15m, 1h signals are intraday — by 9 AM the thesis
            # is hours stale and the prior-day levels are meaningless.
            # Only 1d / daily / overnight timeframes are valid here.
            _OVERNIGHT_TIMEFRAMES = set(
                os.getenv("OVERNIGHT_VALID_TIMEFRAMES", "1d,daily,overnight,weekly,1w").lower().split(",")
            )
            _sig_tf = str(signal.get("timeframe") or "1d").lower().strip()
            if _sig_tf not in _OVERNIGHT_TIMEFRAMES:
                log.info(
                    "[%s] overnight_reeval: skipping intraday signal %s (timeframe=%s) — "
                    "overnight reeval is daily signals only",
                    ticker, signal_id, _sig_tf,
                )
                _mark_job_rejected(job_id, client_id, f"intraday_timeframe_rejected:{_sig_tf}")
                result["rejected"] += 1
                result["terminal_rejected"] += 1
                continue

            # ── Per-client/mode/session disposition lookup ────────────────────
            # Shared ap_signals rows stay WATCHING for all clients; this lookup
            # checks whether this exact client + mode + session already has durable
            # proof before touching master_control, OSM, selector, or broker.
            # _disp_result is set here for ap_signals rows; cleared for trade_queue.
            _disp_result: Optional[_DispositionResult] = None
            if job_source == "ap_signals":
                _disp_result = _resolve_shared_setup_disposition(
                    signal_id, client_id, signal,
                    execution_mode=_execution_mode,
                    session_key=session_key,
                )
                _disp = _disp_result.disposition

                if _disp == _DISPOSITION_ALREADY_ARMED:
                    log.info(
                        "[%s] overnight_reeval: ALREADY_ARMED %s — skip repeat arm",
                        ticker, signal_id,
                    )
                    result["skipped"] = result.get("skipped", 0) + 1
                    result["already_resolved"] += 1
                    continue

                if _disp == _DISPOSITION_ALREADY_OWNED:
                    log.info(
                        "[%s] overnight_reeval: ALREADY_OWNED %s — entry in-flight or filled",
                        ticker, signal_id,
                    )
                    result["skipped"] = result.get("skipped", 0) + 1
                    result["already_resolved"] += 1
                    continue

                if _disp == _DISPOSITION_ALREADY_TERMINAL:
                    log.info(
                        "[%s] overnight_reeval: ALREADY_TERMINAL %s — skip re-evaluation",
                        ticker, signal_id,
                    )
                    result["skipped"] = result.get("skipped", 0) + 1
                    result["already_resolved"] += 1
                    continue

                if _disp in (_DISPOSITION_LOOKUP_FAILED, _DISPOSITION_AMBIGUOUS_OWNERSHIP):
                    # Fail closed: truth is unavailable or ambiguous.
                    # Do NOT create an order while ownership is unresolved.
                    log.warning(
                        "[%s] overnight_reeval: disposition=%s for %s — "
                        "fail-closed as retryable_deferred",
                        ticker, _disp, signal_id,
                    )
                    result["skipped"] = result.get("skipped", 0) + 1
                    result["retryable_deferred"] += 1
                    continue

                if _disp == _DISPOSITION_REATTACH_WATCHER:
                    # Case B: PENDING_TRIGGER order exists but WATCHER_ARMED
                    # proof is missing. Reuse the existing local_order_id;
                    # never create a new order.  No master_control, no
                    # selector, no broker.
                    #
                    # P0-3/P0-4: reattachment must be idempotent (never
                    # cancel a valid order) and must preserve exact identity
                    # + risk levels for THIS row only (no cross-row leakage
                    # from outer-loop variables).
                    _existing_oid = _disp_result.existing_local_order_id or ""
                    _existing_ord = _disp_result.existing_order_row or {}
                    log.info(
                        "[%s] overnight_reeval: REATTACH_WATCHER %s local_order_id=%s — "
                        "reusing existing PENDING_TRIGGER order",
                        ticker, signal_id, _existing_oid,
                    )
                    if not _existing_oid:
                        log.error(
                            "[%s] overnight_reeval: REATTACH_WATCHER missing local_order_id "
                            "signal=%s — failing closed as retryable",
                            ticker, signal_id,
                        )
                        result["skipped"] = result.get("skipped", 0) + 1
                        result["retryable_deferred"] += 1
                        continue

                    # Explicit per-row locals — never fall back to outer-loop
                    # variables. `entry_trigger` is set later in the normal
                    # processing path, so referencing it here could either
                    # raise UnboundLocalError on the first affected row or
                    # (worse) reuse the previous ticker's trigger on a later
                    # iteration.
                    _reattach_trigger = None
                    _trig_raw = _existing_ord.get("trigger_price")
                    if _trig_raw is None:
                        _trig_raw = signal.get("entry_trigger")
                    try:
                        _reattach_trigger = float(_trig_raw) if _trig_raw is not None else None
                    except (TypeError, ValueError):
                        _reattach_trigger = None
                    if not _reattach_trigger or _reattach_trigger <= 0:
                        log.error(
                            "[%s] overnight_reeval: REATTACH_WATCHER cannot prove trigger "
                            "for signal=%s local_order_id=%s — failing closed as retryable "
                            "(never inherit trigger from a prior loop iteration)",
                            ticker, signal_id, _existing_oid,
                        )
                        result["skipped"] = result.get("skipped", 0) + 1
                        result["retryable_deferred"] += 1
                        continue

                    def _reattach_price(order_key, signal_key):
                        v = _existing_ord.get(order_key)
                        if v is None:
                            v = signal.get(signal_key)
                        try:
                            return float(v) if v is not None else None
                        except (TypeError, ValueError):
                            return None

                    _reattach_stop   = _reattach_price("stop_underlying",   "stop_price")
                    _reattach_target = _reattach_price("target_underlying", "target_price")
                    _reattach_client = client_id
                    _reattach_mode   = _execution_mode
                    _reattach_side   = str(_existing_ord.get("direction") or side).upper()
                    _reattach_signal_id = str(_existing_ord.get("signal_id") or signal_id)
                    _reattach_canonical = str(
                        _existing_ord.get("canonical_signal_id")
                        or _resolve_canonical_signal_id(signal_id, signal)
                    )

                    # Parse existing order metadata (best-effort).
                    _ord_meta = _existing_ord.get("meta") or {}
                    if isinstance(_ord_meta, str):
                        try:
                            import json as _json
                            _ord_meta = _json.loads(_ord_meta)
                        except Exception:
                            _ord_meta = {}
                    if not isinstance(_ord_meta, dict):
                        _ord_meta = {}

                    # Metadata merge: existing FIRST, canonical values LAST
                    # so proven session/mode/client/canonical always win over
                    # any stale or blank stored metadata.
                    _reattach_metadata = {
                        **_ord_meta,
                        "overnight":                        True,
                        "reattach_watcher":                 True,
                        "contract_deferred":                True,
                        "contract_selection_deferred_to":   "breach_time",
                        "client_id":                        _reattach_client,
                        "execution_mode":                   _reattach_mode,
                        "overnight_reeval_session_key":     session_key,
                        "signal_id":                        _reattach_signal_id,
                        "canonical_signal_id":              _reattach_canonical,
                        # PR #388 Blocker 1: REATTACH is a proven PR#388
                        # seam and opts in to the late-attachment classifier.
                        "late_attachment_policy_eligible":  True,
                    }

                    import types as _types_mod
                    _reattach_plan = _types_mod.SimpleNamespace(
                        ticker            = str(_existing_ord.get("symbol") or ticker),
                        side              = _reattach_side,
                        direction         = _reattach_side,
                        score             = float(_existing_ord.get("score") or signal.get("score") or 0),
                        timeframe         = str(_existing_ord.get("timeframe") or signal.get("timeframe") or "1d"),
                        entry_trigger     = _reattach_trigger,
                        trigger_price     = _reattach_trigger,
                        stop_underlying   = _reattach_stop,
                        target_underlying = _reattach_target,
                        trigger_type      = "breach",
                        prior_day_high    = signal.get("prior_day_high"),
                        prior_day_low     = signal.get("prior_day_low"),
                        pattern           = _existing_ord.get("pattern") or signal.get("pattern"),
                        tier              = _existing_ord.get("tier") or signal.get("tier"),
                        contract_symbol   = str(_existing_ord.get("contract") or f"DEFERRED:{ticker}"),
                        contracts         = int(_existing_ord.get("qty") or 1),
                        limit_price       = float(_existing_ord.get("limit_price") or 0.01),
                        plan_id           = str(_existing_ord.get("plan_id") or ""),
                        signal_id         = _reattach_signal_id,
                        canonical_signal_id = _reattach_canonical,
                        client_id         = _reattach_client,
                        execution_mode    = _reattach_mode,
                        late_attachment_policy_eligible = True,
                        metadata          = _reattach_metadata,
                    )

                    # P0-3 precheck: if the existing watcher already owns
                    # this exact local_order_id, do NOT call watch() again.
                    # A second watch() would hit add_signal() dedup, return
                    # False, and (without the recovery flags) attempt to
                    # cancel the valid PENDING_TRIGGER order. Just retry the
                    # durable proof write below.
                    _already_owned = False
                    try:
                        _has_order_fn = getattr(entry_watcher, "has_order", None)
                        if callable(_has_order_fn):
                            _already_owned = bool(_has_order_fn(_existing_oid))
                    except Exception as _has_exc:
                        log.warning(
                            "[%s] REATTACH_WATCHER has_order() check failed for %s: %s "
                            "— proceeding with recovery_rearm watch() (safe)",
                            ticker, _existing_oid, _has_exc,
                        )
                        _already_owned = False

                    if _already_owned:
                        log.info(
                            "[%s] REATTACH_WATCHER watcher already owns local_order_id=%s "
                            "— skipping second watch() call; retrying durable proof only",
                            ticker, _existing_oid,
                        )
                        _reattach_armed = True
                    else:
                        _pre_watch_fenced = _persist_reattach_in_progress_fence(
                            client_id=client_id,
                            execution_mode=_reattach_mode,
                            canonical_signal_id=_reattach_canonical,
                            signal_id=signal_id,
                            signal_payload=signal,
                            local_order_id=_existing_oid,
                            session_key=session_key,
                        )
                        if not _pre_watch_fenced:
                            log.critical(
                                "[%s] overnight_reeval: REATTACH_WATCHER pre-watch "
                                "ownership fence failed signal=%s local_order_id=%s "
                                "— watcher not called; classifying retryable_deferred",
                                ticker, signal_id, _existing_oid,
                            )
                            result["skipped"] = result.get("skipped", 0) + 1
                            result["retryable_deferred"] += 1
                            continue
                        try:
                            _reattach_armed = entry_watcher.watch(
                                _reattach_plan, _existing_oid,
                                recovery_rearm=True,
                                no_cancel_on_reject=True,
                            )
                        except Exception as _reat_exc:
                            log.error(
                                "[%s] overnight_reeval: REATTACH_WATCHER watch() exception "
                                "signal=%s local_order_id=%s: %s",
                                ticker, signal_id, _existing_oid, _reat_exc,
                            )
                            # Retryable, not terminal: the existing
                            # PENDING_TRIGGER order is untouched and the
                            # next retry can attempt reattach again.
                            result["skipped"] = result.get("skipped", 0) + 1
                            result["retryable_deferred"] += 1
                            continue

                    if not _reattach_armed:
                        # PR #388 truthful accounting on watch() False.
                        # Use the NO-STATUS-FILTER exact-order lookup so
                        # terminal statuses (CANCELED / EXPIRED / REJECTED
                        # / ERROR) are visible. The active-ownership fence
                        # helper _query_active_entry_order filters those
                        # out and would incorrectly return NOT_FOUND, which
                        # would cascade into a replacement ENTRY on the
                        # next reeval.
                        _post_status, _post_row = _query_exact_entry_order_by_local_id(
                            _existing_oid, client_id, _reattach_mode, _reattach_canonical,
                        )
                        _post_active_status = ""
                        if _post_status == _LS_FOUND and isinstance(_post_row, dict):
                            _post_active_status = str(_post_row.get("status") or "").upper()

                        # Case 1: row found, still preserved.
                        if _post_status == _LS_FOUND and _post_active_status in ("PENDING_TRIGGER", "CREATED"):
                            log.warning(
                                "[%s] overnight_reeval: REATTACH_WATCHER watch()"
                                " returned False signal=%s local_order_id=%s"
                                " — VERIFIED PRESERVED (order still %s);"
                                " classifying retryable_deferred",
                                ticker, signal_id, _existing_oid, _post_active_status,
                            )
                            result["skipped"] = result.get("skipped", 0) + 1
                            result["retryable_deferred"] += 1
                            continue

                        # Case 2: row found in active-ownership family
                        # (submitted / accepted / open / partial / filled)
                        # — already owned, no further reeval work.
                        if _post_status == _LS_FOUND and _post_active_status in _ALREADY_OWNED_STATUSES:
                            log.warning(
                                "[%s] overnight_reeval: REATTACH_WATCHER watch()"
                                " returned False signal=%s local_order_id=%s"
                                " — order in-flight/filled (%s);"
                                " classifying already_resolved",
                                ticker, signal_id, _existing_oid, _post_active_status,
                            )
                            result["skipped"] = result.get("skipped", 0) + 1
                            result["already_resolved"] += 1
                            continue

                        # Case 3: row found in an explicit terminal status.
                        if _post_status == _LS_FOUND and _post_active_status in _TERMINAL_ENTRY_STATUSES:
                            _terminal_marker_persisted = _persist_reattach_terminal_suppression_marker(
                                client_id=client_id,
                                execution_mode=_reattach_mode,
                                canonical_signal_id=_reattach_canonical,
                                signal_id=signal_id,
                                signal_payload=signal,
                                local_order_id=_existing_oid,
                                session_key=session_key,
                                reason=f"reattach_post_watch_terminal:{_post_active_status}",
                                post_status=_post_status,
                                post_row_status=_post_active_status,
                            )
                            if not _terminal_marker_persisted:
                                log.critical(
                                    "[%s] overnight_reeval: REATTACH_WATCHER watch()"
                                    " returned False signal=%s local_order_id=%s"
                                    " — order terminalized (%s) but durable"
                                    " terminal marker persist FAILED;"
                                    " classifying retryable_deferred (fail closed)",
                                    ticker, signal_id, _existing_oid, _post_active_status,
                                )
                                result["skipped"] = result.get("skipped", 0) + 1
                                result["retryable_deferred"] += 1
                                continue

                            log.warning(
                                "[%s] overnight_reeval: REATTACH_WATCHER watch()"
                                " returned False signal=%s local_order_id=%s"
                                " — order terminalized (%s); durable marker"
                                " persisted; classifying already_resolved"
                                " (no replacement now or on next attempt)",
                                ticker, signal_id, _existing_oid, _post_active_status,
                            )
                            result["skipped"] = result.get("skipped", 0) + 1
                            result["already_resolved"] += 1
                            continue

                        # Case 4: row found in an UNRECOGNISED status —
                        # persist ambiguity so the next reeval sees terminal.
                        # Case 5: row missing (NOT_FOUND) — persist ambiguity.
                        # Case 6: lookup failed — persist ambiguity.
                        # In every one of these, `retryable_deferred` alone
                        # is not enough: the next reeval could see no
                        # active order and classify NEW, creating a
                        # replacement ENTRY. Write a durable AMBIGUITY
                        # record via the opportunity ledger so the
                        # disposition resolver returns ALREADY_TERMINAL.
                        _ambiguity_persisted = _persist_reattach_post_watch_ambiguity(
                            client_id=client_id,
                            execution_mode=_reattach_mode,
                            canonical_signal_id=_reattach_canonical,
                            signal_id=signal_id,
                            signal_payload=signal,
                            local_order_id=_existing_oid,
                            session_key=session_key,
                            post_status=_post_status,
                            post_row_status=_post_active_status,
                        )
                        if _ambiguity_persisted:
                            log.critical(
                                "[%s] overnight_reeval: REATTACH_WATCHER watch()"
                                " returned False AND post-verify=%s row_status=%r"
                                " signal=%s local_order_id=%s"
                                " — durable ambiguity marker persisted;"
                                " next reeval will classify ALREADY_TERMINAL"
                                " (no replacement ENTRY)",
                                ticker, _post_status, _post_active_status,
                                signal_id, _existing_oid,
                            )
                            result["skipped"] = result.get("skipped", 0) + 1
                            result["already_resolved"] += 1
                            continue

                        # Ambiguity marker persist failed — fail closed
                        # by classifying the run RETRYABLE with an explicit
                        # unresolved counter so this attempt cannot mark
                        # success and the next attempt retries the whole
                        # path (which will hit the same ambiguity and
                        # attempt to persist again).
                        log.critical(
                            "[%s] overnight_reeval: REATTACH_WATCHER watch()"
                            " returned False AND post-verify=%s row_status=%r"
                            " AND ambiguity marker persist FAILED;"
                            " signal=%s local_order_id=%s"
                            " — classifying retryable_deferred (fail closed;"
                            " next reeval may still see this row)",
                            ticker, _post_status, _post_active_status,
                            signal_id, _existing_oid,
                        )
                        result["skipped"] = result.get("skipped", 0) + 1
                        result["retryable_deferred"] += 1
                        continue

                    # Persist durable WATCHER_ARMED proof after successful reattachment.
                    _canonical_for_proof = _resolve_canonical_signal_id(signal_id, signal)
                    _reat_proof_ok = _persist_watcher_armed_proof(
                        client_id=client_id,
                        execution_mode=_execution_mode,
                        canonical_signal_id=_canonical_for_proof,
                        signal_id=signal_id,
                        signal_payload=signal,
                        local_order_id=str(_existing_oid),
                        session_key=session_key,
                        clear_reattach_in_progress=True,
                        extra_meta={
                            "source_table": "ap_signals",
                            "source_job_id": str(job_id),
                            "ticker": ticker,
                            "side": side,
                            "contract_deferred": True,
                            "contract_selection_deferred_to": "breach_time",
                            "reattach_watcher": True,
                            "armed_at": datetime.now(timezone.utc).isoformat(),
                        },
                    )

                    if not _reat_proof_ok:
                        log.critical(
                            "OVERNIGHT_REEVAL_REATTACH_PROOF_WRITE_FAILED | "
                            "client=%s mode=%s canonical=%s local_order_id=%s session=%s | "
                            "watcher reattached but WATCHER_ARMED proof NOT persisted — "
                            "next retry will REATTACH again (idempotent)",
                            client_id, _execution_mode, _canonical_for_proof,
                            _existing_oid, session_key,
                        )
                        _mark_job_error(
                            job_id, client_id,
                            "reattach_watcher_armed_proof_persistence_failed",
                        )
                        result["errors"] += 1
                        result["terminal_errors"] += 1
                        continue

                    log.info(
                        "[%s] ✅ REATTACH_WATCHER ARMED — local_order_id=%s canonical=%s",
                        ticker, _existing_oid, _canonical_for_proof,
                    )
                    _mark_job_watching_armed(job_id, client_id, f"reattached:{_existing_oid}")
                    result["armed"] += 1
                    result["fresh_armed"] += 1
                    continue

                if _disp == _DISPOSITION_ACTIVE_CREATED_RETRY:
                    # PR #388 Blocker #6: an exact active ENTRY order in
                    # CREATED status already exists for this client + mode
                    # + canonical_signal_id. Preserve it. Do NOT run
                    # master_control, contract selector, OSM
                    # create_entry_order, or the broker. The next reeval
                    # (or the row's established owner) can transition it
                    # to PENDING_TRIGGER and take the REATTACH_WATCHER
                    # path; a duplicate create_entry_order here would
                    # defeat the whole active-order fence.
                    _created_oid = (_disp_result.existing_local_order_id or "")
                    log.info(
                        "[%s] overnight_reeval: ACTIVE_CREATED_RETRY %s "
                        "local_order_id=%s — preserving CREATED order; "
                        "classifying run retryable_deferred",
                        ticker, signal_id, _created_oid,
                    )
                    result["skipped"] = result.get("skipped", 0) + 1
                    result["retryable_deferred"] += 1
                    continue

                # RETRYABLE or NEW — fall through to normal processing.
            else:
                # trade_queue rows are genuinely per-client; use legacy boolean fence.
                _shared_watch_arm_recorded = _shared_watch_arm_failure_already_recorded(
                    signal_id, client_id, signal, session_key=session_key
                )
                if _shared_watch_arm_recorded and _paper_rescue_only:
                    _mark_job_rejected(
                        job_id,
                        client_id,
                        "duplicate_setup:same_session_watch_arm_or_terminal_failure_proof",
                    )
                    result["rejected"] += 1
                    result["terminal_rejected"] += 1
                    continue

            # Step 1: Fetch prior-day levels from broker
            _ticker_key = str(ticker or "").upper()
            prior_levels = prior_levels_by_ticker.get(_ticker_key)
            if prior_levels is None:
                prior_levels = {}
                if hasattr(market_data_broker, "get_prior_day_levels"):
                    try:
                        prior_levels = market_data_broker.get_prior_day_levels(ticker) or {}
                    except Exception as _prior_exc:
                        log.warning(
                            "[%s] overnight_reeval: prior-level fetch failed; retrying row later: %s",
                            ticker,
                            _prior_exc,
                        )
                        if _paper_rescue_only:
                            _mark_job_watching_reason(
                                job_id,
                                client_id,
                                "after_hours_deferred:overnight_prior_levels_fetch_failed",
                            )
                        result["skipped"] += 1
                        result["retryable_deferred"] += 1
                        continue
                else:
                    log.warning(
                        "[%s] market-data broker has no get_prior_day_levels — cannot validate overnight daily signal",
                        ticker,
                    )
                if prior_levels:
                    prior_levels_by_ticker[_ticker_key] = prior_levels

            prior_day_high = (
                float(signal.get("prior_day_high") or 0) or
                float(prior_levels.get("prior_day_high") or 0) or
                None
            )
            prior_day_low = (
                float(signal.get("prior_day_low") or 0) or
                float(prior_levels.get("prior_day_low") or 0) or
                None
            )

            # ── PR4: session-validated fresh + cache fallback ──────────────────
            # DEFAULT OFF (PRIOR_LEVEL_CACHE_FALLBACK). When on:
            #   (a) FRESH levels are only valid for use AND cache when the broker's
            #       own returned prior_day_date equals the expected prior trading
            #       session. (amendment) On mismatch/missing broker date we FAIL
            #       CLOSED on the fresh values themselves — clear them — so a
            #       stale/lagged/holiday bar can never arm a trade, not just never
            #       be cached.
            #   (b) if fresh is null or was invalidated, fall back to a cached
            #       level ONLY if its stamped (broker) session matches the
            #       expected prior trading session.
            _prior_levels_source = "fresh"
            if _PRIOR_LEVEL_CACHE_ENABLED:
                _expected_session = _prior_trading_session_date(_et_now())
                _broker_date_raw = prior_levels.get("prior_day_date")
                _broker_session = None
                if _broker_date_raw:
                    try:
                        _broker_session = date.fromisoformat(str(_broker_date_raw)[:10])
                    except Exception:
                        _broker_session = None

                _have_fresh = (prior_day_high is not None or prior_day_low is not None)
                _session_ok = (_broker_session is not None and _broker_session == _expected_session)

                if _have_fresh and _session_ok:
                    # Correct-session fresh data: use it AND cache it.
                    _cache_prior_levels(
                        ticker, _broker_session,
                        prior_day_high, prior_day_low,
                        prior_levels.get("prior_day_close"),
                    )
                elif _have_fresh and not _session_ok:
                    # FAIL CLOSED: broker date missing or wrong session. Do NOT
                    # use these levels directly and do NOT cache them — discard.
                    log.warning(
                        "[%s] PRIOR_LEVEL_SESSION_MISMATCH signal=%s broker_date=%s "
                        "expected_session=%s — discarding fresh levels (stale/lagged)",
                        ticker, signal_id,
                        (_broker_session.isoformat() if _broker_session else _broker_date_raw),
                        _expected_session.isoformat(),
                    )
                    prior_day_high = None
                    prior_day_low = None
                    # Only clear prior_day_close if it came from this same stale
                    # broker fetch (don't discard a trusted pre-existing value).
                    if prior_levels.get("prior_day_close") is not None:
                        prior_levels = dict(prior_levels)
                        prior_levels["prior_day_close"] = None

                # Single cache fallback: whenever we have no usable fresh value
                # (null fetch, or fresh just discarded on session mismatch), try
                # the session-matched cache. If the cache is wrong-session or
                # absent, levels stay None and the existing fail-safe
                # (INVALIDATED_MISSING_PRIOR_LEVELS) runs below.
                if prior_day_high is None and prior_day_low is None:
                    _cached = _get_cached_prior_levels(ticker, _expected_session)
                    if _cached:
                        prior_day_high = float(_cached.get("prior_day_high") or 0) or None
                        prior_day_low = float(_cached.get("prior_day_low") or 0) or None
                        _prior_levels_source = "cache"
                        log.warning(
                            "[%s] PRIOR_LEVEL_CACHE_FALLBACK_USED signal=%s session=%s "
                            "high=%s low=%s — using broker-session-matched cache",
                            ticker, signal_id, _expected_session.isoformat(),
                            prior_day_high, prior_day_low,
                        )

            # Enrich signal with fetched levels for watcher and validator
            signal["prior_day_high"] = prior_day_high
            signal["prior_day_low"] = prior_day_low
            signal["prior_levels_source"] = _prior_levels_source
            if prior_levels.get("prior_day_close"):
                signal["prior_day_close"] = prior_levels["prior_day_close"]

            # Side-specific prior-level check:
            #   CALL needs prior_day_high (or explicit entry_trigger)
            #   PUT  needs prior_day_low  (or explicit entry_trigger)
            # If the required level is unavailable due to broker/history fetch
            # failure, keep WATCHING and retry — do not permanently reject.
            _has_trigger  = bool(float(signal.get("entry_trigger") or 0))
            _side_upper   = (side or "").upper()
            _missing_level = None
            if not _has_trigger:
                if _side_upper == "CALL" and prior_day_high is None:
                    _missing_level = "prior_day_high"
                elif _side_upper == "PUT" and prior_day_low is None:
                    _missing_level = "prior_day_low"
            if _missing_level:
                log.warning(
                    "[%s] overnight_reeval: OVERNIGHT_PRIOR_LEVELS_UNAVAILABLE "
                    "signal=%s side=%s missing=%s "
                    "category=DATA_UNAVAILABLE severity=WARNING "
                    "final_decision=RETRY_LATER "
                    "reason_code=OVERNIGHT_PRIOR_LEVELS_UNAVAILABLE",
                    ticker, signal_id, _side_upper, _missing_level,
                )
                if _paper_rescue_only:
                    _mark_job_watching_reason(
                        job_id,
                        client_id,
                        _paper_rescue_queue_reason(
                            "after_hours_deferred",
                            f"overnight_reeval_data_pending:{_missing_level}",
                        ),
                    )
                result["skipped"] = result.get("skipped", 0) + 1
                result["retryable_deferred"] += 1
                continue  # leave job WATCHING for next reeval run

            # Step 2: Derive entry_trigger if not provided by scanner
            # The Strat: CALL entries breach prior-day high; PUT entries breach prior-day low
            entry_trigger = float(signal.get("entry_trigger") or 0) or None
            if entry_trigger is None:
                if side == "CALL":
                    entry_trigger = prior_day_high
                elif side == "PUT":
                    entry_trigger = prior_day_low
            if not entry_trigger and _paper_rescue_only:
                _mark_job_rejected(job_id, client_id, "trigger_invalid")
                result["rejected"] += 1
                result["terminal_rejected"] += 1
                continue
            if entry_trigger:
                signal["entry_trigger"] = entry_trigger

            # Step 3: Overnight daily structure validation
            # Fetch a fresh market snapshot (pre-market quote)
            snapshot = snapshot_by_ticker.get(_ticker_key)
            if snapshot is None:
                try:
                    snapshot = fetch_market_snapshot(ticker, market_data_broker)
                except Exception as _snapshot_exc:
                    log.warning(
                        "[%s] overnight_reeval: snapshot fetch failed; retrying row later: %s",
                        ticker,
                        _snapshot_exc,
                    )
                    if _paper_rescue_only:
                        _mark_job_watching_reason(
                            job_id,
                            client_id,
                            "after_hours_deferred:overnight_snapshot_fetch_failed",
                        )
                    result["skipped"] += 1
                    result["retryable_deferred"] += 1
                    continue
                if snapshot:
                    snapshot_by_ticker[_ticker_key] = snapshot
            validation = validate_overnight_daily_signal(
                ticker=ticker,
                side=side,
                prior_day_high=prior_day_high,
                prior_day_low=prior_day_low,
                snapshot=snapshot,
            )

            if not validation.valid:
                # Distinguish snapshot-unavailable (DATA_NOT_READY) from true invalidation.
                _snap_miss = "SNAPSHOT_UNAVAILABLE" in (validation.reason_code or "").upper()
                if _snap_miss and not _OVERNIGHT_SNAPSHOT_FAIL_CLOSED:
                    # Pre-market snapshot not ready — keep signal WATCHING for next retry.
                    # OVERNIGHT_SNAPSHOT_FAIL_CLOSED=false (default).
                    log.warning(
                        "[%s] overnight_reeval: OVERNIGHT_SNAPSHOT_UNAVAILABLE %s "
                        "category=DATA_UNAVAILABLE severity=WARNING "
                        "final_decision=RETRY_LATER "
                        "human_reason='Market snapshot unavailable — retry later'",
                        ticker, signal_id,
                    )
                    if _paper_rescue_only:
                        _mark_job_watching_reason(
                            job_id,
                            client_id,
                            "after_hours_deferred:overnight_snapshot_unavailable",
                        )
                    result["skipped"] = result.get("skipped", 0) + 1
                    result["retryable_deferred"] += 1
                    continue  # leave job WATCHING for next reeval run
                # True invalidation — reject
                log.info("[%s] overnight_reeval: OVERNIGHT_TRUE_INVALIDATION %s — %s",
                         ticker, signal_id, validation.reason_code)
                _mark_job_rejected(job_id, client_id,
                                   f"overnight_invalidated:{validation.reason_code}")
                _log_rejection_supabase(
                    signal_id=signal_id, client_id=client_id, ticker=ticker,
                    side=side, score=float(signal.get("score") or 0),
                    stage="overnight_reeval",
                    reason_code=validation.reason_code,
                    human_reason=validation.reason_text,
                    payload=signal,
                )
                if _lifecycle_ok:
                    try:
                        _sig_rejected(signal_id, ticker, _LO.OVERNIGHT_EVAL,
                                      f"overnight_invalidated: {validation.reason_text}",
                                      _RC.VALIDATION, validation.reason_code, _RS.INFO)
                    except Exception:
                        pass
                result["rejected"] += 1
                result["terminal_rejected"] += 1
                continue

            log.info("[%s] overnight_reeval: VALID %s — arming entry watcher | entry_trigger=%.4f",
                     ticker, signal_id, entry_trigger or 0)

            # Step 4: Master control pre-check (score gates, capital, etc.)
            # Use a fresh REEVAL: signal_id so master_control dedup doesn't block it.
            # The original signal was already deduped when it first arrived — overnight
            # reeval is a legitimate second evaluation of the same setup.
            _recovery_skip_mc = False
            _recovery_canonical = _resolve_canonical_signal_id(signal_id, signal)
            _recovery_probe = _read_attempt_scope_unlocked(
                _recovery_canonical, client_id, _execution_mode, session_key,
            )
            if _recovery_probe is not None:
                (_rp_state, _rp_count, _rp_token, _rp_order_id) = _recovery_probe
                if _rp_state in {
                    WATCH_ATTEMPT_STATE_IN_PROGRESS,
                    WATCH_ATTEMPT_STATE_RETRYABLE,
                    WATCH_ATTEMPT_STATE_ARMED,
                }:
                    if _rp_order_id:
                        _rp_lookup, _rp_row = _query_exact_entry_order_by_local_id(
                            _rp_order_id, client_id, _execution_mode,
                            _recovery_canonical,
                        )
                    else:
                        _rp_lookup, _rp_row = _query_active_entry_order(
                            client_id, _execution_mode, _recovery_canonical,
                        )
                    _rp_status = str((_rp_row or {}).get("status") or "").upper()
                    _recovery_skip_mc = (
                        _rp_lookup == _LS_FOUND
                        and _rp_status in _ACTIVE_ENTRY_OWN_STATUSES
                    )

            try:
                import uuid as _uuid2
                reeval_signal = {**signal, "signal_id": f"REEVAL:{signal_id}:{_uuid2.uuid4().hex[:6]}"}
                # Clear this ticker/side from dedup cache if possible
                try:
                    direction = signal.get("side", "").upper()
                    timeframe = signal.get("timeframe", "1d")
                    setup_key = f"{client_id}:{ticker.upper()}:{direction}:{timeframe}"
                    orig_key = f"sig:{signal_id}:{client_id}"
                    mc_seen = getattr(master_control, "_seen_signals", {})
                    mc_seen.pop(orig_key, None)
                    mc_seen.pop(setup_key, None)
                except Exception as contract_exc:
                    log.warning(
                        "[%s] overnight_reeval: failed to normalize deferred contract "
                        "symbol signal=%s client=%s err=%s",
                        ticker, signal_id, client_id, contract_exc,
                    )
                if _recovery_skip_mc:
                    import types as _types_recovery
                    decision = _types_recovery.SimpleNamespace(
                        ok=True,
                        plan=_hydrate_plan_from_signal(
                            signal,
                            client_id=client_id,
                            execution_mode=_execution_mode,
                        ),
                        reason="durable_watch_recovery_preflight",
                        score=float(signal.get("score") or 0),
                    )
                else:
                    decision = master_control.evaluate(
                        reeval_signal, client_id=client_id,
                    )
                if not decision.ok:
                    # Classify the rejection: intel/second-score vs hard safety.
                    # When OVERNIGHT_REEVAL_SCORE_RECHECK_ENABLED=false, morning
                    # score/intel rechecks on already-WATCHING signals are
                    # observe-only and must proceed to watcher arm.
                    # Hard safety blocks ALWAYS reject regardless of the flag.
                    _r = (decision.reason or "").lower()
                    _reason_code = str(getattr(decision, "reason_code", "") or "").upper()
                    _intel_phrases = (
                        "intel_skip", "blocked_intel", "intel_score",
                        "insufficient edge", "gate_g", "gate g",
                        "scanner_approved_intel_observe_only",
                        "data_unavailable_scanner_fallback",
                        "blocked_score",
                        "rejected_low_score",
                        "score_below_floor",
                        "score_below_priority_floor",
                        "context_below_floor",
                        "tier_reject",
                    )
                    _observe_only_reason_codes = {
                        "SCORE_BELOW_THRESHOLD",
                    }
                    _hard_safety_phrases = (
                        "capital", "buying_power", "buying power",
                        "kill_switch", "kill switch",
                        "entries_paused", "entries paused",
                        "client_disabled", "client disabled",
                        "auth", "invalid_side", "invalid side",
                        "invalid direction", "risk_limit", "risk limit",
                        "daily_loss", "daily loss",
                        "risk manager", "risk_manager",
                        "contract quality failed", "contract_quality",
                        "scanner signal is neutral", "neutral direction",
                    )
                    _is_intel_block        = (
                        any(p in _r for p in _intel_phrases)
                        or _reason_code in _observe_only_reason_codes
                    )
                    _is_hard_safety_block  = any(p in _r for p in _hard_safety_phrases)

                    if (_is_intel_block
                            and not _is_hard_safety_block
                            and not _OVERNIGHT_REEVAL_SCORE_RECHECK_ENABLED):
                        # Intel/second-score block on already-WATCHING signal.
                        # Do NOT skip and do NOT reject. Hydrate a minimal plan
                        # from the scanner-approved signal and proceed to arm path.
                        log.info(
                            "[%s] overnight_reeval: OVERNIGHT_SCORE_RECHECK_DISABLED "
                            "signal=%s mc_reason=%s "
                            "decision=PROCEED_TO_ARM second_score_mode=observe_only "
                            "— intel/second-score ignored, using scanner-approved signal",
                            ticker, signal_id, decision.reason,
                        )
                        _hydrated = _hydrate_plan_from_signal(
                            signal,
                            client_id=client_id,
                            execution_mode=_execution_mode,
                        )
                        try:
                            if getattr(decision, "plan", None) is None:
                                decision.plan = _hydrated
                            else:
                                # MC built a plan but rejected on intel — keep MC's
                                # plan and overlay signal fields the watcher needs.
                                for _attr in ("ticker", "side", "score", "timeframe",
                                              "entry_trigger", "trigger_price",
                                              "prior_day_high", "prior_day_low"):
                                    if getattr(decision.plan, _attr, None) in (None, 0, "", 0.0):
                                        setattr(decision.plan, _attr,
                                                getattr(_hydrated, _attr, None))
                            decision.ok = True
                        except Exception:
                            # decision is immutable — build a fresh namespace
                            import types as _types_p
                            decision = _types_p.SimpleNamespace(
                                ok=True, plan=_hydrated,
                                reason=f"observe_only:{decision.reason}",
                                score=float(signal.get("score") or 0),
                            )
                        # Fall through to Step 5 (contract selection / arm)
                    elif "pending_entry_exists" in _r and not _is_hard_safety_block:
                        # PR #404: classify ownership before terminal rejection.
                        # P0-3: pass candidate signal so the classifier can
                        # scope by exact side + canonical identity.
                        _pe_outcome = _classify_pending_entry_for_overnight(
                            ticker, client_id, _execution_mode,
                            entry_watcher=entry_watcher,
                            candidate_signal=signal,
                        )
                        # Test stubs and older callers may return a plain
                        # PENDING_OWNER_* string. Wrap so .disposition /
                        # .stale_orders / .failure_reason are always present.
                        if isinstance(_pe_outcome, str):
                            _pe_outcome = _PendingEntryOutcome(_pe_outcome, (), "")
                        _pe_class = _pe_outcome.disposition
                        if _pe_class == "PENDING_OWNER_ACTIVE":
                            log.info(
                                "[%s] pending_entry_owner_active %s — "
                                "genuine same-client/mode block maintained",
                                ticker, signal_id,
                            )
                            if _paper_rescue_only:
                                _mark_job_rejected(
                                    job_id, client_id,
                                    _paper_rescue_queue_reason("risk_blocked", str(decision.reason or "")),
                                )
                            else:
                                _mark_job_rejected(job_id, client_id, f"mc_blocked:{decision.reason}")
                            result["rejected"] += 1
                            result["terminal_rejected"] += 1
                            continue
                        elif _pe_class in {"PENDING_OWNER_DB_ERROR", "PENDING_OWNER_CONFLICT"}:
                            log.critical(
                                "[%s] pending_entry_ownership_unknown %s — "
                                "class=%s reason=%s fail closed as retryable_deferred",
                                ticker, signal_id, _pe_class,
                                _pe_outcome.failure_reason,
                            )
                            # P1: durably stamp the exact ownership-unknown
                            # class + reason on the queue row so operators can
                            # distinguish DB error from conflict and diagnose
                            # repeated silent deferrals.
                            _mark_job_watching_reason(
                                job_id, client_id,
                                f"pending_entry_owner_unknown:{_pe_class}:"
                                f"{_pe_outcome.failure_reason or 'unspecified'}",
                            )
                            result["skipped"] = result.get("skipped", 0) + 1
                            result["retryable_deferred"] += 1
                            continue
                        elif _pe_class in {
                            "PENDING_OWNER_STALE",
                            "PENDING_OWNER_MISSING",
                            "PENDING_OWNER_CROSS_CLIENT",
                            "PENDING_OWNER_CROSS_MODE",
                            "PENDING_OWNER_TERMINAL",
                        }:
                            # False suppression released. Two required steps
                            # before proceeding, in order:
                            #   1. P0-2: durably terminalize every stale row
                            #      the classifier saw. A stale row must not
                            #      outlive the run that released the candidate.
                            #   2. P0-1: re-invoke master_control.evaluate() so
                            #      score / 0DTE / priority / context / tier /
                            #      intelligence gates all run on the released
                            #      candidate. The prior fall-through skipped
                            #      those gates entirely.
                            _pe_label = _pe_class.lower().replace("pending_owner_", "owner_")
                            log.info(
                                "[%s] pending_entry_%s %s — false suppression "
                                "released; terminalizing %d stale row(s) and "
                                "re-invoking Master Control",
                                ticker, _pe_label, signal_id,
                                len(_pe_outcome.stale_orders),
                            )

                            _cleanup_ok, _cleanup_failure = (
                                _terminalize_stale_pending_orders(
                                    order_state_machine=order_state_machine,
                                    stale_orders=_pe_outcome.stale_orders,
                                    reason=(
                                        f"overnight_reeval_stale_release:{_pe_class}"
                                    ),
                                )
                            )
                            if not _cleanup_ok:
                                log.critical(
                                    "[%s] pending_entry_stale_cleanup_failed %s — "
                                    "class=%s reason=%s; refusing to release "
                                    "and marking retryable_deferred",
                                    ticker, signal_id, _pe_class, _cleanup_failure,
                                )
                                _mark_job_watching_reason(
                                    job_id, client_id,
                                    f"pending_entry_stale_cleanup_failed:{_cleanup_failure}",
                                )
                                result["skipped"] = result.get("skipped", 0) + 1
                                result["retryable_deferred"] += 1
                                continue

                            # P0-1: re-invoke Master Control end-to-end.
                            # pending_entry_exists is no longer true after the
                            # cleanup, so the remaining gates (score / 0DTE /
                            # priority / context / tier / intelligence) can now
                            # run. Any non-pending-entry rejection is honored.
                            try:
                                decision = master_control.evaluate(
                                    reeval_signal, client_id=client_id,
                                )
                            except Exception as _mc_exc:
                                log.error(
                                    "[%s] pending_entry_stale_release_reevaluate "
                                    "failed signal=%s err=%s",
                                    ticker, signal_id, _mc_exc,
                                )
                                _mark_job_watching_reason(
                                    job_id, client_id,
                                    "pending_entry_reevaluate_exception:"
                                    f"{type(_mc_exc).__name__}",
                                )
                                result["skipped"] = result.get("skipped", 0) + 1
                                result["retryable_deferred"] += 1
                                continue

                            if not getattr(decision, "ok", False):
                                _rr = str(getattr(decision, "reason", "") or "")
                                # If pending_entry_exists still fires after
                                # cleanup, something else is holding a row —
                                # fail closed and never bypass the gate a
                                # second time.
                                if "pending_entry_exists" in _rr.lower():
                                    log.critical(
                                        "[%s] pending_entry_still_present_after_"
                                        "cleanup %s reason=%s — fail closed",
                                        ticker, signal_id, _rr,
                                    )
                                    _mark_job_watching_reason(
                                        job_id, client_id,
                                        "pending_entry_still_present_after_cleanup",
                                    )
                                    result["skipped"] = result.get("skipped", 0) + 1
                                    result["retryable_deferred"] += 1
                                    continue
                                log.info(
                                    "[%s] pending_entry_release_reevaluate "
                                    "rejected %s reason=%s — honoring downstream "
                                    "gate",
                                    ticker, signal_id, _rr,
                                )
                                if _paper_rescue_only:
                                    _mark_job_rejected(
                                        job_id, client_id,
                                        _paper_rescue_queue_reason(
                                            "mc_reevaluate_blocked", _rr,
                                        ),
                                    )
                                else:
                                    _mark_job_rejected(
                                        job_id, client_id,
                                        f"mc_blocked_after_stale_release:{_rr}",
                                    )
                                result["rejected"] += 1
                                result["terminal_rejected"] += 1
                                continue

                            # Re-evaluation succeeded end-to-end. Fall through
                            # to Step 5 with the fresh decision.
                        else:
                            log.critical(
                                "[%s] pending_entry_ownership_unknown %s — "
                                "unexpected class=%s fail closed as retryable_deferred",
                                ticker, signal_id, _pe_class,
                            )
                            _mark_job_watching_reason(
                                job_id, client_id,
                                f"pending_entry_owner_unknown:unexpected:{_pe_class}",
                            )
                            result["skipped"] = result.get("skipped", 0) + 1
                            result["retryable_deferred"] += 1
                            continue
                    else:
                        # Hard safety block OR recheck enabled — reject as before
                        log.info(
                            "[%s] overnight_reeval: MC blocked %s — %s "
                            "(hard_safety=%s intel=%s recheck_enabled=%s)",
                            ticker, signal_id, decision.reason,
                            _is_hard_safety_block, _is_intel_block,
                            _OVERNIGHT_REEVAL_SCORE_RECHECK_ENABLED,
                        )
                        if _paper_rescue_only:
                            _reason_text = str(decision.reason or "")
                            _reason_prefix = "duplicate_setup" if "duplicate" in _reason_text.lower() else "risk_blocked"
                            _mark_job_rejected(
                                job_id,
                                client_id,
                                _paper_rescue_queue_reason(_reason_prefix, _reason_text),
                            )
                        else:
                            _mark_job_rejected(job_id, client_id,
                                               f"mc_blocked:{decision.reason}")
                        if _lifecycle_ok:
                            try:
                                _sig_rejected(signal_id, ticker, _LO.MASTER_CONTROL,
                                              f"mc_blocked: {decision.reason}",
                                              _RC.RISK, "MC_BLOCKED", _RS.INFO)
                            except Exception:
                                pass
                        result["rejected"] += 1
                        result["terminal_rejected"] += 1
                        continue
                # ── PR #388 P0-5 plan normalization ──────────────────────
                # Regardless of whether the plan came from MC or _hydrate_
                # plan_from_signal, force identity + risk fields to the
                # canonical scanner values so the watcher never inherits a
                # random REEVAL: id, a blank client_id, or a missing stop.
                if getattr(decision, "plan", None) is not None:
                    _plan = decision.plan
                    _canon_for_norm = _resolve_canonical_signal_id(signal_id, signal)
                    _sid_for_norm = str(signal.get("signal_id") or signal_id or "").strip()
                    setattr(_plan, "signal_id", _sid_for_norm)
                    setattr(_plan, "canonical_signal_id", _canon_for_norm)
                    setattr(_plan, "client_id", client_id)
                    setattr(_plan, "execution_mode", str(_execution_mode).strip().lower())
                    for _plan_attr, _sig_key in (
                        ("stop_underlying",   "stop_price"),
                        ("target_underlying", "target_price"),
                    ):
                        if getattr(_plan, _plan_attr, None) in (None, 0, 0.0):
                            _sv = signal.get(_sig_key)
                            try:
                                _sv_f = float(_sv) if _sv is not None else None
                                if _sv_f and _sv_f > 0:
                                    setattr(_plan, _plan_attr, _sv_f)
                            except (TypeError, ValueError):
                                pass
                    _meta = getattr(_plan, "metadata", None) or {}
                    if not isinstance(_meta, dict):
                        _meta = {}
                    _meta.update({
                        "signal_id":           _sid_for_norm,
                        "canonical_signal_id": _canon_for_norm,
                        "client_id":           client_id,
                        "execution_mode":      str(_execution_mode).strip().lower(),
                    })
                    setattr(_plan, "metadata", _meta)
            except Exception as mc_exc:
                log.error("[%s] overnight_reeval: master_control.evaluate failed: %s", ticker, mc_exc)
                if _paper_rescue_only:
                    _mark_job_error(
                        job_id,
                        client_id,
                        _paper_rescue_queue_reason(
                            "risk_blocked",
                            f"master_control_exception:{type(mc_exc).__name__}",
                        ),
                    )
                result["errors"] += 1
                result["terminal_errors"] += 1
                continue

            # Step 5: Establish watcher ownership before expensive option work.
            # Contract selection remains authoritative at the existing breach-time
            # execution seam, where live option quotes and every quality gate apply.
            contract_deferred = True
            log.info(
                "[%s] overnight_reeval: deferring contract selection to breach time "
                "and arming watcher immediately | trigger=%.4f",
                ticker, entry_trigger or 0,
            )
            decision.plan.contract_symbol = f"DEFERRED:{ticker}"
            decision.plan.limit_price = 0.01
            if not getattr(decision.plan, "contracts", None):
                decision.plan.contracts = int(os.getenv("MIN_CONTRACTS_PER_POSITION", "2"))
            if not hasattr(decision.plan, "metadata") or decision.plan.metadata is None:
                decision.plan.metadata = {}
            decision.plan.metadata.update({
                "contract_deferred": True,
                "deferred_breach_selection": True,
                "selection_context": "deferred_breach",
                "pre_market_contract_selection_skipped": True,
                "pre_market_contract_selection_failed": False,
                "pre_market_selector_failure": None,
                "pre_market_selector_reason_code": None,
                "contract_selection_deferred_to": "breach_time",
                # PR #388 Blocker 1: only PR#388 seams opt in to the
                # late-attachment continuation/reset classifier. Ordinary
                # intraday/direct arms keep committed-main's strict
                # arm_already_through_trigger invariant.
                "late_attachment_policy_eligible": True,
            })
            # Also expose the flag as a top-level plan attribute so
            # APEntryWatcher.watch() can read it without descending into
            # metadata (matches how client_id / execution_mode are wired).
            setattr(decision.plan, "late_attachment_policy_eligible", True)
            if _lifecycle_ok:
                try:
                    from ap_lifecycle import LEDGER as _L
                    _L.log_event(
                        signal_id,
                        "contract_deferred",
                        owner="overnight_reeval",
                        reason="watcher_first_deferred_to_breach",
                    )
                except Exception:
                    pass

            # Propagate entry_trigger and overnight flag to the plan.
            # CRITICAL: also propagate prior_day_high, prior_day_low, and
            # timeframe so APEntryWatcher.watch() correctly identifies this
            # as an overnight signal and SKIPS the intraday staleness check.
            # Without these fields, _is_overnight_signal = False, and the
            # 1.5% drift guard fires at 0.2% movement — killing valid setups.
            # (GOOGL, JPM, AAPL all expired today with watch_arm_failed:
            # stale_price because the plan didn't carry these fields forward.)
            if entry_trigger:
                try:
                    decision.plan.trigger_price  = float(entry_trigger)
                    decision.plan.trigger_type   = "breach"
                    decision.plan.metadata["overnight"] = True
                    # Overnight signal identification fields for entry watcher
                    decision.plan.prior_day_high = prior_day_high or None
                    decision.plan.prior_day_low  = prior_day_low  or None
                    decision.plan.timeframe      = str(signal.get("timeframe") or "1d")
                except Exception:
                    pass

            # Step 5.5: LIVE AUTHORIZATION GATE — overnight reeval arms NEW entries
            # that fire at next open, so a LIVE client must be evaluated here too.
            # Same semantics as ap/queue.py: an UNKNOWN/unverifiable broker mode
            # is treated as a block candidate (never assumed paper); known paper/
            # sandbox clients are exempt. OBSERVE mode logs WOULD_BLOCK and arms
            # the entry anyway; ENFORCE mode rejects it. Gate errors fail closed
            # only in ENFORCE mode for live/unknown brokers.
            try:
                from ap.authorization import (
                    is_live_broker, broker_live_mode_known, check_live_authorization,
                    authorization_gate_enforced, LIVE_AUTHORIZATION_GATE_UNAVAILABLE,
                )
                _gate_reason = None
                if not broker_live_mode_known(broker):
                    _gate_reason = LIVE_AUTHORIZATION_GATE_UNAVAILABLE
                elif is_live_broker(broker):
                    _gate_reason = check_live_authorization(client_id)

                if _gate_reason:
                    if not authorization_gate_enforced():
                        # OBSERVE MODE — log and ARM anyway.
                        log.warning("[%s] overnight_reeval: LIVE_AUTHORIZATION_WOULD_BLOCK — %s | client=%s (observe mode; armed)",
                                    ticker, _gate_reason, client_id)
                        try:
                            from ap.execution import audit
                            audit(client_id, "WARNING", "LIVE_AUTHORIZATION_WOULD_BLOCK", {
                                "reason_code": _gate_reason, "ticker": ticker,
                                "signal_id": signal_id, "path": "overnight_reeval",
                                "enforced": False,
                            })
                        except Exception:
                            pass
                        # fall through — entry is armed in observe mode.
                    else:
                        # ENFORCE MODE — reject the armed entry.
                        log.warning("[%s] overnight_reeval: LIVE ENTRY BLOCKED — %s | client=%s",
                                    ticker, _gate_reason, client_id)
                        _mark_job_rejected(job_id, client_id, f"live_authorization:{_gate_reason}")
                        _log_rejection_supabase(
                            signal_id=signal_id, client_id=client_id, ticker=ticker,
                            side=side, score=float(signal.get("score") or 0),
                            stage="live_authorization",
                            reason_code=_gate_reason, human_reason=_gate_reason,
                            payload=signal,
                        )
                        try:
                            from ap.execution import audit
                            audit(client_id, "WARNING", "LIVE_ENTRY_BLOCKED", {
                                "reason_code": _gate_reason, "ticker": ticker,
                                "signal_id": signal_id, "path": "overnight_reeval",
                                "enforced": True,
                            })
                        except Exception:
                            pass
                        result["rejected"] += 1
                        result["terminal_rejected"] += 1
                        continue
            except Exception as _authz_exc:
                try:
                    from ap.authorization import (
                        is_live_broker as _ilb, broker_live_mode_known as _bmk,
                        authorization_gate_enforced as _enf,
                    )
                    _enforced = _enf()
                    _risky = (not _bmk(broker)) or _ilb(broker)
                except Exception:
                    _enforced, _risky = False, False
                if _enforced and _risky:
                    log.error("[%s] overnight_reeval: LIVE authz gate error — failing closed (enforce): %s",
                              ticker, _authz_exc)
                    _mark_job_rejected(job_id, client_id, f"live_authorization_gate_error:{_authz_exc}")
                    result["errors"] += 1
                    result["terminal_errors"] += 1
                    continue

            _canonical_for_attempt = _resolve_canonical_signal_id(signal_id, signal)
            # PR #404 Blocker 1: shared ap_signals rows must take the SAME durable
            # cross-process attempt owner as trade_queue rows. Previously only
            # trade_queue claimed and ap_signals received a fake ACQUIRED with no
            # token, so its retry state could never persist — a first watcher
            # False could permanently suppress a legitimate shared signal. Both
            # sources now claim through the atomic PostgreSQL CAS.
            _claimed_watch_attempt = job_source in ("trade_queue", "ap_signals")
            _watch_attempt = _WatchAttemptClaim(WATCH_ATTEMPT_ACQUIRED)
            if _claimed_watch_attempt:
                _watch_attempt = _claim_watch_arm_attempt(
                    order_state_machine=order_state_machine,
                    signal_id=signal_id,
                    client_id=client_id,
                    canonical_signal_id=_canonical_for_attempt,
                    signal_payload=signal,
                    execution_mode=_execution_mode,
                    session_key=session_key,
                )
                if _watch_attempt.disposition == WATCH_ATTEMPT_ALREADY_ARMED:
                    log.info(
                        "[%s] overnight_watch_attempt_already_armed signal=%s canonical=%s "
                        "count=%s local_order_id=%s",
                        ticker, signal_id, _canonical_for_attempt,
                        _watch_attempt.attempt_count, _watch_attempt.local_order_id,
                    )
                    _mark_job_watching_armed(job_id, client_id, f"DEFERRED:{ticker}")
                    result["armed"] += 1
                    continue
                if _watch_attempt.disposition == WATCH_ATTEMPT_EXHAUSTED:
                    _exhausted_reason = "overnight_watch_arm_retry_exhausted"
                    log.critical(
                        "[%s] overnight_watch_attempt_exhausted signal=%s canonical=%s "
                        "count=%s max=%s",
                        ticker, signal_id, _canonical_for_attempt,
                        _watch_attempt.attempt_count, OVERNIGHT_WATCH_ARM_MAX_ATTEMPTS,
                    )
                    _mark_job_error(job_id, client_id, _exhausted_reason)
                    result["errors"] += 1
                    result["terminal_errors"] += 1
                    continue
                if _watch_attempt.disposition == WATCH_ATTEMPT_ALREADY_IN_PROGRESS:
                    _reason = "overnight_watch_arm_attempt_in_progress"
                    log.warning(
                        "[%s] overnight_watch_attempt_in_progress signal=%s canonical=%s "
                        "count=%s local_order_id=%s reason=%s",
                        ticker, signal_id, _canonical_for_attempt,
                        _watch_attempt.attempt_count, _watch_attempt.local_order_id,
                        _watch_attempt.reason,
                    )
                    _mark_job_watching_reason(job_id, client_id, _reason)
                    result["skipped"] = result.get("skipped", 0) + 1
                    result["retryable_deferred"] += 1
                    continue
                if _watch_attempt.disposition == WATCH_ATTEMPT_REATTACH_REQUIRED:
                    # The claim function found an expired IN_PROGRESS scope with
                    # an exact active PENDING_TRIGGER ENTRY in orders. It already
                    # (a) rotated the token,
                    # (b) bound the discovered local_order_id, and (c) refreshed
                    # the claim lease — all under the row lock.
                    #
                    # We must reattach the existing watcher to that order without
                    # creating a new order, without running master_control /
                    # selector / broker, and without cancelling the valid ENTRY.
                    # On exception the order is intentionally preserved; we
                    # complete as RETRYABLE / EXHAUSTED with no cleanup required.
                    _reattach_oid = str(_watch_attempt.local_order_id or "").strip()
                    log.info(
                        "[%s] overnight_reeval: REATTACH_REQUIRED signal=%s "
                        "canonical=%s count=%s existing_order=%s — "
                        "reusing existing order, no new order or broker submit",
                        ticker, signal_id, _canonical_for_attempt,
                        _watch_attempt.attempt_count, _reattach_oid,
                    )

                    def _rr_complete_and_classify(*, state, reason, caller):
                        completion_ok = _complete_watch_arm_attempt_checked(
                            signal_id=signal_id,
                            client_id=client_id,
                            canonical_signal_id=_canonical_for_attempt,
                            signal_payload=signal,
                            execution_mode=_execution_mode,
                            session_key=session_key,
                            attempt=_watch_attempt,
                            state=state,
                            local_order_id=_reattach_oid,
                            reason=reason,
                            ticker=ticker,
                            caller=caller,
                        )
                        if not completion_ok:
                            completion_reason = (
                                "overnight_watch_arm_attempt_completion_failed:"
                                f"{caller}"
                            )
                            _mark_job_error(job_id, client_id, completion_reason)
                            result["errors"] += 1
                            result["terminal_errors"] += 1
                            return False
                        if state == WATCH_ATTEMPT_STATE_RETRYABLE:
                            _mark_job_watching_reason(job_id, client_id, reason)
                            result["skipped"] = result.get("skipped", 0) + 1
                            result["retryable_deferred"] += 1
                        elif state == WATCH_ATTEMPT_STATE_EXHAUSTED:
                            _mark_job_error(
                                job_id, client_id,
                                "overnight_watch_arm_retry_exhausted",
                            )
                            result["errors"] += 1
                            result["terminal_errors"] += 1
                        elif state == WATCH_ATTEMPT_STATE_ERROR:
                            _mark_job_error(job_id, client_id, reason)
                            result["errors"] += 1
                            result["terminal_errors"] += 1
                        return True

                    if not _reattach_oid:
                        log.error(
                            "[%s] overnight_reeval: REATTACH_REQUIRED missing "
                            "local_order_id signal=%s — failing closed retryable",
                            ticker, signal_id,
                        )
                        _rr_complete_and_classify(
                            state=WATCH_ATTEMPT_STATE_ERROR,
                            reason="reattach_required_missing_local_order_id",
                            caller="reattach_required_missing_local_order_id",
                        )
                        continue

                    # Look up the full order row for plan construction.
                    _rr_status, _rr_row = _query_exact_entry_order_by_local_id(
                        _reattach_oid, client_id, _execution_mode,
                        _canonical_for_attempt,
                    )
                    if _rr_status != _LS_FOUND or not _rr_row:
                        log.error(
                            "[%s] overnight_reeval: REATTACH_REQUIRED order "
                            "lookup failed local_order_id=%s status=%s — "
                            "failing closed retryable",
                            ticker, _reattach_oid, _rr_status,
                        )
                        _rr_next = (
                            WATCH_ATTEMPT_STATE_EXHAUSTED
                            if _watch_attempt.attempt_count
                            >= OVERNIGHT_WATCH_ARM_MAX_ATTEMPTS
                            else WATCH_ATTEMPT_STATE_RETRYABLE
                        )
                        _rr_complete_and_classify(
                            state=_rr_next,
                            reason=f"reattach_required_order_lookup:{_rr_status}",
                            caller="reattach_required_order_lookup",
                        )
                        continue
                    _rr_order_status = str(_rr_row.get("status") or "").upper()
                    # Already-owned (in-flight / filled): treat as resolved.
                    if _rr_order_status in _ALREADY_OWNED_STATUSES:
                        log.info(
                            "[%s] overnight_reeval: REATTACH_REQUIRED order "
                            "already in-flight/filled local_order_id=%s "
                            "status=%s — classifying already_resolved",
                            ticker, _reattach_oid, _rr_order_status,
                        )
                        _rr_armed_ok = _complete_watch_arm_attempt_checked(
                            signal_id=signal_id,
                            client_id=client_id,
                            canonical_signal_id=_canonical_for_attempt,
                            signal_payload=signal,
                            execution_mode=_execution_mode,
                            session_key=session_key,
                            attempt=_watch_attempt,
                            state=WATCH_ATTEMPT_STATE_ARMED,
                            local_order_id=_reattach_oid,
                            reason="reattach_required_already_owned",
                            ticker=ticker,
                            caller="reattach_required_already_owned",
                        )
                        if not _rr_armed_ok:
                            _mark_job_error(
                                job_id, client_id,
                                "overnight_watch_arm_attempt_completion_failed:"
                                "reattach_required_already_owned",
                            )
                            result["errors"] += 1
                            result["terminal_errors"] += 1
                            continue
                        result["skipped"] = result.get("skipped", 0) + 1
                        result["already_resolved"] += 1
                        continue
                    # Terminal: the order is gone; something already cleaned it.
                    if _rr_order_status in _TERMINAL_ENTRY_STATUSES:
                        log.warning(
                            "[%s] overnight_reeval: REATTACH_REQUIRED order "
                            "terminal local_order_id=%s status=%s — "
                            "completing attempt RETRYABLE for next reeval",
                            ticker, _reattach_oid, _rr_order_status,
                        )
                        _rr_next_state = (
                            WATCH_ATTEMPT_STATE_EXHAUSTED
                            if _watch_attempt.attempt_count
                            >= OVERNIGHT_WATCH_ARM_MAX_ATTEMPTS
                            else WATCH_ATTEMPT_STATE_RETRYABLE
                        )
                        _rr_complete_and_classify(
                            state=_rr_next_state,
                            reason=f"reattach_required_order_terminal:{_rr_order_status}",
                            caller="reattach_required_terminal",
                        )
                        continue

                    if _rr_order_status != "PENDING_TRIGGER":
                        log.error(
                            "[%s] overnight_reeval: REATTACH_REQUIRED refuses "
                            "non-PENDING_TRIGGER order local_order_id=%s status=%s",
                            ticker, _reattach_oid, _rr_order_status or "blank",
                        )
                        _rr_complete_and_classify(
                            state=WATCH_ATTEMPT_STATE_ERROR,
                            reason=(
                                "reattach_required_non_pending_trigger:"
                                f"{_rr_order_status or 'blank'}"
                            ),
                            caller="reattach_required_non_pending_trigger",
                        )
                        continue

                    # Build a minimal reattach plan from the existing order row.
                    _rr_ord_meta = _rr_row.get("meta") or {}
                    if isinstance(_rr_ord_meta, str):
                        try:
                            import json as _rr_json
                            _rr_ord_meta = _rr_json.loads(_rr_ord_meta)
                        except Exception:
                            _rr_ord_meta = {}
                    if not isinstance(_rr_ord_meta, dict):
                        _rr_ord_meta = {}

                    _rr_trigger_raw = (
                        _rr_row.get("trigger_price")
                        or signal.get("entry_trigger")
                    )
                    try:
                        _rr_trigger = float(_rr_trigger_raw) if _rr_trigger_raw is not None else None
                    except (TypeError, ValueError):
                        _rr_trigger = None
                    if not _rr_trigger or _rr_trigger <= 0:
                        log.error(
                            "[%s] overnight_reeval: REATTACH_REQUIRED cannot "
                            "resolve trigger price local_order_id=%s — "
                            "failing closed retryable",
                            ticker, _reattach_oid,
                        )
                        _rr_next = (
                            WATCH_ATTEMPT_STATE_EXHAUSTED
                            if _watch_attempt.attempt_count
                            >= OVERNIGHT_WATCH_ARM_MAX_ATTEMPTS
                            else WATCH_ATTEMPT_STATE_RETRYABLE
                        )
                        _rr_complete_and_classify(
                            state=_rr_next,
                            reason="reattach_required_invalid_trigger",
                            caller="reattach_required_invalid_trigger",
                        )
                        continue

                    def _rr_price(order_key, signal_key):
                        v = _rr_row.get(order_key)
                        if v is None:
                            v = signal.get(signal_key)
                        try:
                            return float(v) if v is not None else None
                        except (TypeError, ValueError):
                            return None

                    _rr_side      = str(_rr_row.get("direction") or side).upper()
                    _rr_sig_id    = str(_rr_row.get("signal_id") or signal_id)
                    _rr_canonical = str(
                        _rr_row.get("canonical_signal_id") or _canonical_for_attempt
                    )
                    _rr_metadata = {
                        **_rr_ord_meta,
                        "overnight":                        True,
                        "reattach_watcher":                 True,
                        "reattach_required_recovery":       True,
                        "contract_deferred":                True,
                        "contract_selection_deferred_to":   "breach_time",
                        "client_id":                        client_id,
                        "execution_mode":                   _execution_mode,
                        "overnight_reeval_session_key":     session_key,
                        "signal_id":                        _rr_sig_id,
                        "canonical_signal_id":              _rr_canonical,
                        "late_attachment_policy_eligible":  True,
                    }
                    try:
                        _rr_score = float(
                            _rr_row.get("score") or signal.get("score") or 0
                        )
                        _rr_contracts = int(_rr_row.get("qty") or 1)
                        _rr_limit_price = float(
                            _rr_row.get("limit_price") or 0.01
                        )
                    except (TypeError, ValueError, OverflowError):
                        log.error(
                            "[%s] overnight_reeval: REATTACH_REQUIRED invalid "
                            "plan numerics local_order_id=%s",
                            ticker, _reattach_oid,
                        )
                        _rr_next = (
                            WATCH_ATTEMPT_STATE_EXHAUSTED
                            if _watch_attempt.attempt_count
                            >= OVERNIGHT_WATCH_ARM_MAX_ATTEMPTS
                            else WATCH_ATTEMPT_STATE_RETRYABLE
                        )
                        _rr_complete_and_classify(
                            state=_rr_next,
                            reason="reattach_required_invalid_plan_numerics",
                            caller="reattach_required_invalid_plan_numerics",
                        )
                        continue
                    import types as _rr_types
                    _rr_plan = _rr_types.SimpleNamespace(
                        ticker            = str(_rr_row.get("symbol") or ticker),
                        side              = _rr_side,
                        direction         = _rr_side,
                        score             = _rr_score,
                        timeframe         = str(_rr_row.get("timeframe") or signal.get("timeframe") or "1d"),
                        entry_trigger     = _rr_trigger,
                        trigger_price     = _rr_trigger,
                        stop_underlying   = _rr_price("stop_underlying", "stop_price"),
                        target_underlying = _rr_price("target_underlying", "target_price"),
                        trigger_type      = "breach",
                        prior_day_high    = signal.get("prior_day_high"),
                        prior_day_low     = signal.get("prior_day_low"),
                        pattern           = _rr_row.get("pattern") or signal.get("pattern"),
                        tier              = _rr_row.get("tier") or signal.get("tier"),
                        contract_symbol   = str(_rr_row.get("contract") or f"DEFERRED:{ticker}"),
                        contracts         = _rr_contracts,
                        limit_price       = _rr_limit_price,
                        plan_id           = str(_rr_row.get("plan_id") or ""),
                        signal_id         = _rr_sig_id,
                        canonical_signal_id = _rr_canonical,
                        client_id         = client_id,
                        execution_mode    = _execution_mode,
                        late_attachment_policy_eligible = True,
                        metadata          = _rr_metadata,
                    )

                    # Idempotent guard: if the watcher already owns this exact
                    # local_order_id, skip watch() and go directly to proof.
                    _rr_already_owned = False
                    try:
                        _rr_has_fn = getattr(entry_watcher, "has_order", None)
                        if callable(_rr_has_fn):
                            _rr_already_owned = bool(_rr_has_fn(_reattach_oid))
                    except Exception as _rr_has_exc:
                        log.warning(
                            "[%s] REATTACH_REQUIRED has_order() check failed %s: %s "
                            "— proceeding with watch() (safe)",
                            ticker, _reattach_oid, _rr_has_exc,
                        )

                    if not _rr_already_owned:
                        try:
                            _rr_armed = entry_watcher.watch(
                                _rr_plan, _reattach_oid,
                                recovery_rearm=True,
                                no_cancel_on_reject=True,
                            )
                        except Exception as _rr_exc:
                            # Exception path: the existing PENDING_TRIGGER order
                            # is untouched — do NOT clean it up.  Complete as
                            # RETRYABLE / EXHAUSTED so the next run can retry.
                            log.error(
                                "[%s] overnight_reeval: REATTACH_REQUIRED "
                                "watch() exception signal=%s "
                                "local_order_id=%s: %s",
                                ticker, signal_id, _reattach_oid, _rr_exc,
                            )
                            _rr_next = (
                                WATCH_ATTEMPT_STATE_EXHAUSTED
                                if _watch_attempt.attempt_count
                                >= OVERNIGHT_WATCH_ARM_MAX_ATTEMPTS
                                else WATCH_ATTEMPT_STATE_RETRYABLE
                            )
                            _rr_complete_and_classify(
                                state=_rr_next,
                                reason=f"reattach_required_watch_exception:{type(_rr_exc).__name__}",
                                caller="reattach_required_watch_exception",
                            )
                            continue
                    else:
                        _rr_armed = True

                    if not _rr_armed:
                        # watch() returned False: order is still valid, retry.
                        log.warning(
                            "[%s] overnight_reeval: REATTACH_REQUIRED watch() "
                            "False signal=%s local_order_id=%s "
                            "— order preserved; classifying retryable",
                            ticker, signal_id, _reattach_oid,
                        )
                        _rr_next = (
                            WATCH_ATTEMPT_STATE_EXHAUSTED
                            if _watch_attempt.attempt_count
                            >= OVERNIGHT_WATCH_ARM_MAX_ATTEMPTS
                            else WATCH_ATTEMPT_STATE_RETRYABLE
                        )
                        _rr_complete_and_classify(
                            state=_rr_next,
                            reason="reattach_required_watch_false",
                            caller="reattach_required_watch_false",
                        )
                        continue

                    # Watcher armed: persist durable proof then complete.
                    _rr_proof_ok = _persist_watcher_armed_proof(
                        client_id=client_id,
                        execution_mode=_execution_mode,
                        canonical_signal_id=_canonical_for_attempt,
                        signal_id=signal_id,
                        signal_payload=signal,
                        local_order_id=_reattach_oid,
                        session_key=session_key,
                        extra_meta={
                            "source_table": job_source,
                            "source_job_id": str(job_id),
                            "ticker": ticker,
                            "side": side,
                            "contract_deferred": True,
                            "contract_selection_deferred_to": "breach_time",
                            "reattach_watcher": True,
                            "reattach_required_recovery": True,
                            "armed_at": datetime.now(timezone.utc).isoformat(),
                        },
                    )
                    _rr_armed_completion_ok = _complete_watch_arm_attempt_checked(
                        signal_id=signal_id,
                        client_id=client_id,
                        canonical_signal_id=_canonical_for_attempt,
                        signal_payload=signal,
                        execution_mode=_execution_mode,
                        session_key=session_key,
                        attempt=_watch_attempt,
                        state=WATCH_ATTEMPT_STATE_ARMED,
                        local_order_id=_reattach_oid,
                        reason="reattach_required_watcher_armed",
                        ticker=ticker,
                        caller="reattach_required_watcher_armed",
                    )
                    if not _rr_armed_completion_ok:
                        _mark_job_error(
                            job_id, client_id,
                            "overnight_watch_arm_attempt_completion_failed:"
                            "reattach_required_armed",
                        )
                        result["errors"] += 1
                        result["terminal_errors"] += 1
                        continue

                    if not _rr_proof_ok:
                        log.critical(
                            "OVERNIGHT_REEVAL_REATTACH_REQUIRED_PROOF_FAILED | "
                            "client=%s mode=%s canonical=%s local_order_id=%s "
                            "session=%s | watcher active and attempt durably "
                            "ARMED; secondary proof missing",
                            client_id, _execution_mode, _canonical_for_attempt,
                            _reattach_oid, session_key,
                        )
                        _mark_job_error(
                            job_id, client_id,
                            "reattach_required_watcher_armed_proof_persistence_failed",
                        )
                        result["errors"] += 1
                        result["terminal_errors"] += 1
                        continue

                    log.info(
                        "[%s] ✅ REATTACH_REQUIRED ARMED — "
                        "local_order_id=%s canonical=%s",
                        ticker, _reattach_oid, _canonical_for_attempt,
                    )
                    _mark_job_watching_armed(
                        job_id, client_id, f"reattach_required:{_reattach_oid}"
                    )
                    result["armed"] += 1
                    result["fresh_armed"] += 1
                    continue

                if (
                    _watch_attempt.disposition == WATCH_ATTEMPT_CONFLICT
                    and str(_watch_attempt.reason or "") == "attempt_state_error"
                ):
                    # Durable attempt state is ERROR — a terminal ownership
                    # failure the resolver treats as non-replaceable. This
                    # run is already-resolved, never retryable.
                    log.info(
                        "[%s] overnight_watch_attempt_already_terminal signal=%s "
                        "canonical=%s state=ERROR count=%s local_order_id=%s",
                        ticker, signal_id, _canonical_for_attempt,
                        _watch_attempt.attempt_count, _watch_attempt.local_order_id,
                    )
                    result["skipped"] = result.get("skipped", 0) + 1
                    result["already_resolved"] += 1
                    continue
                if _watch_attempt.disposition != WATCH_ATTEMPT_ACQUIRED:
                    _reason = f"overnight_watch_arm_attempt_unavailable:{_watch_attempt.disposition}:{_watch_attempt.reason}"
                    log.critical(
                        "[%s] overnight_watch_attempt_unavailable signal=%s canonical=%s "
                        "disposition=%s reason=%s",
                        ticker, signal_id, _canonical_for_attempt,
                        _watch_attempt.disposition, _watch_attempt.reason,
                    )
                    _mark_job_watching_reason(job_id, client_id, _reason)
                    result["skipped"] = result.get("skipped", 0) + 1
                    result["retryable_deferred"] += 1
                    continue
                if _recovery_skip_mc:
                    # The advisory preflight saw an existing active owner, but
                    # the locked claim no longer agrees. Never turn that race
                    # into a replacement order that bypassed Master Control.
                    # The locked claim returned ACQUIRED, so close that exact
                    # owner before leaving; otherwise this retryable-looking
                    # exit strands a fresh IN_PROGRESS lease.
                    _preflight_completion_ok = _complete_watch_arm_attempt_checked(
                        signal_id=signal_id,
                        client_id=client_id,
                        canonical_signal_id=_canonical_for_attempt,
                        signal_payload=signal,
                        execution_mode=_execution_mode,
                        session_key=session_key,
                        attempt=_watch_attempt,
                        state=WATCH_ATTEMPT_STATE_ERROR,
                        local_order_id="",
                        reason="overnight_watch_recovery_preflight_changed",
                        ticker=ticker,
                        caller="recovery_preflight_changed_after_acquire",
                    )
                    _mark_job_error(
                        job_id,
                        client_id,
                        (
                            "overnight_watch_recovery_preflight_changed"
                            if _preflight_completion_ok
                            else "overnight_watch_arm_attempt_completion_failed:"
                            "recovery_preflight_changed_after_acquire"
                        ),
                    )
                    result["errors"] += 1
                    result["terminal_errors"] += 1
                    continue

            # Step 6: Create OSM entry order — let OSM generate a fresh UUID.
            # Do NOT pass local_order_id: static IDs cause OSM conflicts on retry.
            # For deferred contracts: ensure contract_symbol is NOT set to the
            # ticker symbol — store as DEFERRED so orders table is clean and
            # the execution core knows to select live at breach time.
            if contract_deferred:
                try:
                    if not getattr(decision.plan, "contract_symbol", None) or \
                       decision.plan.contract_symbol == decision.plan.ticker:
                        decision.plan.contract_symbol = f"DEFERRED:{ticker}"
                except Exception:
                    pass
            try:
                # PR fix/osm-watcher-handoff-and-entry-meta:
                # Insert OSM row already in PENDING_TRIGGER. Previously this path
                # created the row in CREATED and then called
                # mark_entry_pending_trigger() in Step 6b. The two-step approach
                # was racing the 30s LOST_HANDOFF_30S cancellation in
                # APOrderMonitor: 1,103 orders on 2026-05-26 went CREATED→CANCELED
                # with no intermediate PENDING_TRIGGER transition observable in
                # the OSM event log. Atomic initial_status='PENDING_TRIGGER'
                # eliminates the race entirely.
                #
                # P0 (hotfix/p0-preserve-overnight-sizing-meta):
                # Pass decision.plan.metadata into OSM so sizing_context,
                # contract_deferred, risk_profile_source, and snapshot_at_eval
                # are persisted on the order row. Without this, overnight/deferred
                # orders had null sizing_context — making budget debugging blind.
                # OSM merges caller meta on top of its auto-meta (caller wins on
                # conflict), so sizing_context is preserved without erasing OSM
                # auto-fields like score, tier, signal_id, etc.
                from ap.authorization import execution_mode_for_broker
                _plan_meta = getattr(decision.plan, "metadata", None) or {}
                local_order_id = order_state_machine.create_entry_order(
                    decision.plan,
                    initial_status="PENDING_TRIGGER",
                    execution_mode=execution_mode_for_broker(broker),
                    meta=_plan_meta,
                )
            except Exception as osm_exc:
                log.error("[%s] overnight_reeval: OSM create_entry_order failed: %s", ticker, osm_exc)
                _create_completion_ok = _complete_watch_arm_attempt_checked(
                    signal_id=signal_id,
                    client_id=client_id,
                    canonical_signal_id=_canonical_for_attempt,
                    signal_payload=signal,
                    execution_mode=_execution_mode,
                    session_key=session_key,
                    attempt=_watch_attempt,
                    state=WATCH_ATTEMPT_STATE_ERROR,
                    local_order_id="",
                    reason=f"create_entry_order:{type(osm_exc).__name__}",
                    ticker=ticker,
                    caller="create_entry_order_exception",
                )
                if not _create_completion_ok:
                    _mark_job_error(
                        job_id, client_id,
                        "overnight_watch_arm_attempt_completion_failed:"
                        "create_entry_order_exception",
                    )
                elif _paper_rescue_only:
                    _mark_job_error(
                        job_id,
                        client_id,
                        _paper_rescue_queue_reason(
                            "order_materialization_failed",
                            f"create_entry_order:{type(osm_exc).__name__}",
                        ),
                    )
                result["errors"] += 1
                result["terminal_errors"] += 1
                continue

            if not local_order_id:
                log.error("[%s] overnight_reeval: OSM returned no local_order_id for %s", ticker, signal_id)
                _missing_oid_completion_ok = _complete_watch_arm_attempt_checked(
                    signal_id=signal_id,
                    client_id=client_id,
                    canonical_signal_id=_canonical_for_attempt,
                    signal_payload=signal,
                    execution_mode=_execution_mode,
                    session_key=session_key,
                    attempt=_watch_attempt,
                    state=WATCH_ATTEMPT_STATE_ERROR,
                    local_order_id="",
                    reason="missing_local_order_id",
                    ticker=ticker,
                    caller="missing_local_order_id",
                )
                if not _missing_oid_completion_ok:
                    _mark_job_error(
                        job_id, client_id,
                        "overnight_watch_arm_attempt_completion_failed:"
                        "missing_local_order_id",
                    )
                elif _paper_rescue_only:
                    _mark_job_error(
                        job_id,
                        client_id,
                        "order_materialization_failed:missing_local_order_id",
                    )
                result["errors"] += 1
                result["terminal_errors"] += 1
                continue

            if _claimed_watch_attempt and not _bind_watch_arm_attempt_order(
                signal_id=signal_id,
                client_id=client_id,
                canonical_signal_id=_canonical_for_attempt,
                signal_payload=signal,
                execution_mode=_execution_mode,
                session_key=session_key,
                attempt=_watch_attempt,
                local_order_id=str(local_order_id),
            ):
                _bind_reason = "overnight_watch_arm_attempt_bind_failed"
                _cleanup_success, _cleanup_method = _cleanup_overnight_watch_arm_failure(
                    order_state_machine=order_state_machine,
                    client_id=client_id,
                    signal_id=signal_id,
                    ticker=ticker,
                    side=side,
                    local_order_id=str(local_order_id),
                    contract=f"DEFERRED:{ticker}",
                    contract_deferred=contract_deferred,
                    entry_trigger=entry_trigger,
                    reason=_bind_reason,
                    done_event="OVERNIGHT_WATCH_ARM_ATTEMPT_BIND_FAILED_CLEANUP_DONE",
                )
                _bind_completion_ok = _complete_watch_arm_attempt_checked(
                    signal_id=signal_id,
                    client_id=client_id,
                    canonical_signal_id=_canonical_for_attempt,
                    signal_payload=signal,
                    execution_mode=_execution_mode,
                    session_key=session_key,
                    attempt=_watch_attempt,
                    state=WATCH_ATTEMPT_STATE_ERROR,
                    local_order_id=str(local_order_id),
                    reason=_bind_reason,
                    ticker=ticker,
                    caller="bind_failure",
                )
                # A failed local-order bind is a terminal ownership failure —
                # the durable attempt is written as ERROR and the resolver
                # treats ERROR as non-replaceable. Reporting this run as
                # retryable would contradict the durable state and let the
                # runner appear to make progress while the next run refuses.
                if not _cleanup_success:
                    _mark_job_error(job_id, client_id, f"{_bind_reason}:cleanup_failed:{_cleanup_method}")
                elif not _bind_completion_ok:
                    _mark_job_error(
                        job_id,
                        client_id,
                        "overnight_watch_arm_bind_cleanup_completion_failed",
                    )
                else:
                    _mark_job_error(job_id, client_id, _bind_reason)
                result["errors"] += 1
                result["terminal_errors"] += 1
                continue

            # Step 6b: Mark order PENDING_TRIGGER so order_monitor does not
            # cancel it as a stale CREATED order before the trigger breach fires.
            # This mirrors the queue.py intraday path which does the same before arming.
            try:
                _current_status = ""
                if hasattr(order_state_machine, "get_order"):
                    _current = order_state_machine.get_order(local_order_id) or {}
                    _current_status = str((_current or {}).get("status") or "").upper()

                if _current_status == "PENDING_TRIGGER":
                    _pt_ok = True
                elif hasattr(order_state_machine, "mark_entry_pending_trigger"):
                    _pt_ok = order_state_machine.mark_entry_pending_trigger(local_order_id)
                else:
                    _pt_ok = order_state_machine.transition(
                        local_order_id,
                        "PENDING_TRIGGER",
                        submitted_ts=None,
                    )
                if not _pt_ok:
                    log.error(
                        "[%s] overnight_reeval: could not mark PENDING_TRIGGER for %s — skipping arm",
                        ticker,
                        local_order_id,
                    )
                    _pending_false_completion_ok = _complete_watch_arm_attempt_checked(
                        signal_id=signal_id,
                        client_id=client_id,
                        canonical_signal_id=_canonical_for_attempt,
                        signal_payload=signal,
                        execution_mode=_execution_mode,
                        session_key=session_key,
                        attempt=_watch_attempt,
                        state=WATCH_ATTEMPT_STATE_ERROR,
                        local_order_id=str(local_order_id),
                        reason="pending_trigger_transition_false",
                        ticker=ticker,
                        caller="pending_trigger_false",
                    )
                    if not _pending_false_completion_ok:
                        _mark_job_error(
                            job_id, client_id,
                            "overnight_watch_arm_attempt_completion_failed:"
                            "pending_trigger_false",
                        )
                    elif _paper_rescue_only:
                        _mark_job_error(
                            job_id,
                            client_id,
                            "order_materialization_failed:pending_trigger_transition_false",
                        )
                    result["errors"] += 1
                    result["terminal_errors"] += 1
                    continue
            except Exception as _pt_exc:
                log.error(
                    "[%s] overnight_reeval: pending-trigger transition failed for %s: %s",
                    ticker,
                    local_order_id,
                    _pt_exc,
                )
                _pending_exc_completion_ok = _complete_watch_arm_attempt_checked(
                    signal_id=signal_id,
                    client_id=client_id,
                    canonical_signal_id=_canonical_for_attempt,
                    signal_payload=signal,
                    execution_mode=_execution_mode,
                    session_key=session_key,
                    attempt=_watch_attempt,
                    state=WATCH_ATTEMPT_STATE_ERROR,
                    local_order_id=str(local_order_id),
                    reason=f"pending_trigger_transition:{type(_pt_exc).__name__}",
                    ticker=ticker,
                    caller="pending_trigger_exception",
                )
                if not _pending_exc_completion_ok:
                    _mark_job_error(
                        job_id, client_id,
                        "overnight_watch_arm_attempt_completion_failed:"
                        "pending_trigger_exception",
                    )
                elif _paper_rescue_only:
                    _mark_job_error(
                        job_id,
                        client_id,
                        f"order_materialization_failed:{type(_pt_exc).__name__}",
                    )
                result["errors"] += 1
                result["terminal_errors"] += 1
                continue

            # Step 7: Arm entry watcher — pass plan (not signal) and the OSM order ID
            _contract_sym = str(getattr(decision.plan, "contract_symbol", "") or "")
            _arm_label    = _contract_sym if _contract_sym else "DEFERRED_AT_BREACH"
            try:
                armed = entry_watcher.watch(decision.plan, local_order_id)
                if armed:
                    try:
                        from ap.intelligence_context_handoff import enqueue_preopen_context_best_effort

                        enqueue_preopen_context_best_effort(
                            signal,
                            client_id=client_id,
                            execution_mode=_execution_mode,
                            canonical_signal_id=_resolve_canonical_signal_id(signal_id, signal),
                            local_order_id=str(local_order_id),
                        )
                    except Exception as _preopen_intel_exc:
                        log.warning(
                            "[%s] PREOPEN intelligence handoff error signal_id=%s local_order_id=%s: %s",
                            ticker, signal_id, local_order_id, _preopen_intel_exc,
                        )
                    # Overnight reeval creates a LOCAL entry order before broker
                    # submission. That order waits for APEntryWatcher to see the
                    # breach. It MUST NOT remain in CREATED status, because
                    # APOrderMonitor treats CREATED/no broker_order_id as
                    # "never submitted" and cancels it after ORDER_TIMEOUT_CREATED.
                    #
                    # Correct overnight lifecycle:
                    #   CREATED          → immediately after local order creation
                    #   PENDING_TRIGGER  → after watcher arms (this block)
                    #   SUBMITTED        → after breach via
                    #                      APExecutionCore._on_entry_trigger()
                    #                      → APOrderStateMachine.submit_existing_entry()
                    #
                    # DO NOT submit to Tradier here. Submitting before breach
                    # would bypass the trigger-breach rule and enter prematurely.
                    # PENDING_TRIGGER was already set in Step 6b (pre-arm).
                    # Second call removed — duplicate transition on same local_order_id.

                    # ── Durable WATCHER_ARMED proof write (shared ap_signals rows) ──
                    # For ap_signals source rows _mark_job_watching_armed() only logs
                    # (correctly leaves shared row WATCHING for other clients).
                    # We MUST persist an explicit WATCHER_ARMED record in
                    # client_signal_opportunities so that the next retry can find
                    # ALREADY_ARMED instead of creating a second order.
                    # If the write fails: fail as retryable — the watcher is armed
                    # in memory but the next retry must discover the existing order
                    # via the active-order fence (REATTACH_WATCHER path) rather than
                    # creating a duplicate.
                    _arm_proof_ok = True
                    if job_source == "ap_signals":
                        _canonical_for_arm = _resolve_canonical_signal_id(signal_id, signal)
                        _arm_proof_ok = _persist_watcher_armed_proof(
                            client_id=client_id,
                            execution_mode=_execution_mode,
                            canonical_signal_id=_canonical_for_arm,
                            signal_id=signal_id,
                            signal_payload=signal,
                            local_order_id=str(local_order_id),
                            session_key=session_key,
                            extra_meta={
                                "source_table": "ap_signals",
                                "source_job_id": str(job_id),
                                "ticker": ticker,
                                "side": side,
                                "contract_deferred": contract_deferred,
                                "contract_selection_deferred_to": "breach_time",
                                "armed_at": datetime.now(timezone.utc).isoformat(),
                            },
                        )

                    _armed_completion_ok = _complete_watch_arm_attempt_checked(
                        signal_id=signal_id,
                        client_id=client_id,
                        canonical_signal_id=_canonical_for_attempt,
                        signal_payload=signal,
                        execution_mode=_execution_mode,
                        session_key=session_key,
                        attempt=_watch_attempt,
                        state=WATCH_ATTEMPT_STATE_ARMED,
                        local_order_id=str(local_order_id),
                        reason="watcher_armed",
                        ticker=ticker,
                        caller="watcher_armed",
                    )

                    if not _armed_completion_ok:
                        _completion_reason = (
                            "overnight_watch_arm_attempt_completion_failed:ARMED"
                        )
                        _mark_job_error(
                            job_id,
                            client_id,
                            _completion_reason,
                        )
                        result["errors"] += 1
                        result["terminal_errors"] += 1
                        continue

                    if not _arm_proof_ok:
                        # The watcher is active and the exact attempt is now
                        # durably ARMED. Surface the missing secondary proof as
                        # an operator-visible error, never as retryable progress
                        # that leaves an active watcher behind an IN_PROGRESS
                        # owner.
                        _proof_reason = "overnight_watcher_armed_proof_persistence_failed"
                        log.critical(
                            "OVERNIGHT_REEVAL_ARM_PROOF_WRITE_FAILED | "
                            "client=%s mode=%s canonical=%s local_order_id=%s "
                            "session=%s | watcher active and attempt durably ARMED; "
                            "secondary WATCHER_ARMED proof missing",
                            client_id, _execution_mode, _canonical_for_attempt,
                            local_order_id, session_key,
                        )
                        _mark_job_error(job_id, client_id, _proof_reason)
                        result["errors"] += 1
                        result["terminal_errors"] += 1
                        continue

                    _mark_job_watching_armed(job_id, client_id, _arm_label)
                    log.info(
                        "[%s] ✅ ARMED — contract=%s entry_trigger=%.4f contract_deferred=%s",
                        ticker, _arm_label, entry_trigger or 0, contract_deferred,
                    )
                    # ── THE BUG TRAP: signal is now WATCHING in the watcher ──
                    # If this signal disappears before market open, grep:
                    # [SIGNAL_TRACE] id=<signal_id>
                    # The next state change from WATCHING will reveal the killer.
                    if _lifecycle_ok:
                        try:
                            signal_armed(signal_id, ticker, _LO.OVERNIGHT_EVAL,
                                         "armed_in_entry_watcher",
                                         contract=_arm_label,
                                         local_order_id=str(local_order_id),
                                         entry_trigger=str(entry_trigger or 0),
                                         contract_deferred=str(contract_deferred))
                            signal_watching(signal_id, ticker, _LO.WATCHER,
                                            "watching_for_trigger_breach",
                                            contract=_arm_label,
                                            entry_trigger=str(entry_trigger or 0))
                        except Exception:
                            pass
                    result["armed"] += 1
                    result["fresh_armed"] += 1
                else:
                    # PR #404: classify before any terminal decision.
                    # Generic False is NEVER automatic terminal rejection.
                    _reject_reason = str(
                        getattr(entry_watcher, "_last_reject_reason", None)
                        or "watch_returned_false"
                    )
                    # dedup_block requires ownership verification, not direct ALREADY_WATCHING.
                    _already_watching = False
                    if _reject_reason == "dedup_block":
                        _owner_generation = (
                            getattr(decision.plan, "trigger_generation", None)
                            or (_plan_meta.get("trigger_generation") if isinstance(_plan_meta, dict) else None)
                            or signal.get("trigger_generation")
                        )
                        _owner_token = (
                            getattr(decision.plan, "watcher_token", None)
                            or (_plan_meta.get("watcher_token") if isinstance(_plan_meta, dict) else None)
                            or signal.get("watcher_token")
                        )
                        _owner_result = _resolve_existing_watcher_ownership(
                            entry_watcher=entry_watcher,
                            signal_id=signal_id,
                            client_id=client_id,
                            execution_mode=_execution_mode,
                            ticker=ticker,
                            side=side,
                            generation=_owner_generation,
                            watcher_token=_owner_token,
                            local_order_id=local_order_id,
                        )
                        if _owner_result == WATCH_OWNER_EXACT:
                            _already_watching = True
                        elif _owner_result == WATCH_OWNER_CONFLICT:
                            log.warning(
                                "[%s] overnight_watch_ownership_conflict signal=%s "
                                "— preserving existing owner, cleaning new duplicate order",
                                ticker, signal_id,
                            )
                            _conflict_cleanup_success, _conflict_cleanup_method = _cleanup_overnight_watch_arm_failure(
                                order_state_machine=order_state_machine,
                                client_id=client_id,
                                signal_id=signal_id,
                                ticker=ticker,
                                side=side,
                                local_order_id=str(local_order_id),
                                contract=_arm_label,
                                contract_deferred=contract_deferred,
                                entry_trigger=entry_trigger,
                                reason="overnight_watch_ownership_conflict",
                                done_event="OVERNIGHT_WATCH_OWNERSHIP_CONFLICT_CLEANUP_DONE",
                            )
                            _owner_conflict_completion_ok = (
                                _complete_watch_arm_attempt_checked(
                                    signal_id=signal_id,
                                    client_id=client_id,
                                    canonical_signal_id=_canonical_for_attempt,
                                    signal_payload=signal,
                                    execution_mode=_execution_mode,
                                    session_key=session_key,
                                    attempt=_watch_attempt,
                                    state=(
                                        WATCH_ATTEMPT_STATE_ERROR
                                        if not _conflict_cleanup_success
                                        else WATCH_ATTEMPT_STATE_RETRYABLE
                                    ),
                                    local_order_id=str(local_order_id),
                                    reason=(
                                        "overnight_watch_ownership_conflict:"
                                        f"{_conflict_cleanup_method}"
                                    ),
                                    ticker=ticker,
                                    caller="watcher_owner_conflict",
                                )
                            )
                            if not _owner_conflict_completion_ok:
                                _mark_job_error(
                                    job_id, client_id,
                                    "overnight_watch_arm_attempt_completion_failed:"
                                    "watcher_owner_conflict",
                                )
                                result["errors"] += 1
                                result["terminal_errors"] += 1
                                continue
                            if not _conflict_cleanup_success:
                                _mark_job_error(job_id, client_id, "overnight_watch_ownership_conflict_cleanup_failed")
                                result["errors"] += 1
                                result["terminal_errors"] += 1
                                continue
                            _mark_job_watching_reason(
                                job_id, client_id, "overnight_watch_ownership_conflict"
                            )
                            result["skipped"] = result.get("skipped", 0) + 1
                            result["retryable_deferred"] += 1
                            continue
                        else:
                            # WATCH_OWNER_MISSING or WATCH_OWNER_LOOKUP_ERROR:
                            # dedup fired but ownership unverifiable → retryable
                            log.warning(
                                "[%s] overnight_watch_owner_unverified signal=%s "
                                "owner_result=%s", ticker, signal_id, _owner_result,
                            )
                            # _already_watching stays False → RETRYABLE_NOT_ARMED below

                    # overnight NEW arm: terminal stale/stop checks bypassed (pre-market).
                    _arm_outcome = _classify_watch_arm_outcome(
                        watch_result=False,
                        already_watching=_already_watching,
                        terminal_conflict=False,
                        exception=None,
                    )

                    if _arm_outcome.disposition == ALREADY_WATCHING:
                        # Watcher already owns this signal_id; the new OSM order
                        # was canceled by the watcher's dedup path. Idempotent success.
                        _dup_cleanup_success, _dup_cleanup_method = _cleanup_overnight_watch_arm_failure(
                            order_state_machine=order_state_machine,
                            client_id=client_id,
                            signal_id=signal_id,
                            ticker=ticker,
                            side=side,
                            local_order_id=str(local_order_id),
                            contract=_arm_label,
                            contract_deferred=contract_deferred,
                            entry_trigger=entry_trigger,
                            reason="overnight_watch_already_watching_duplicate_cleanup",
                            done_event="OVERNIGHT_WATCH_ALREADY_WATCHING_DUPLICATE_CLEANUP_DONE",
                        )
                        if not _dup_cleanup_success:
                            _dup_error_completion_ok = _complete_watch_arm_attempt_checked(
                                signal_id=signal_id,
                                client_id=client_id,
                                canonical_signal_id=_canonical_for_attempt,
                                signal_payload=signal,
                                execution_mode=_execution_mode,
                                session_key=session_key,
                                attempt=_watch_attempt,
                                state=WATCH_ATTEMPT_STATE_ERROR,
                                local_order_id=str(local_order_id),
                                reason="already_watching_duplicate_cleanup_failed",
                                ticker=ticker,
                                caller="already_watching_cleanup_failed",
                            )
                            _mark_job_error(
                                job_id,
                                client_id,
                                (
                                    "already_watching_duplicate_cleanup_failed"
                                    if _dup_error_completion_ok
                                    else "overnight_watch_arm_attempt_completion_failed:"
                                    "already_watching_cleanup_failed"
                                ),
                            )
                            result["errors"] += 1
                            result["terminal_errors"] += 1
                            continue
                        _already_owner_completion_ok = (
                            _complete_watch_arm_attempt_checked(
                                signal_id=signal_id,
                                client_id=client_id,
                                canonical_signal_id=_canonical_for_attempt,
                                signal_payload=signal,
                                execution_mode=_execution_mode,
                                session_key=session_key,
                                attempt=_watch_attempt,
                                state=WATCH_ATTEMPT_STATE_ARMED,
                                local_order_id=str(local_order_id),
                                reason="already_watching_exact_owner",
                                ticker=ticker,
                                caller="already_watching_exact_owner",
                            )
                        )

                        if not _already_owner_completion_ok:
                            _mark_job_error(
                                job_id,
                                client_id,
                                "overnight_watch_arm_attempt_completion_failed:"
                                "already_watching_exact_owner",
                            )
                            result["errors"] += 1
                            result["terminal_errors"] += 1
                            continue

                        log.info(
                            "[%s] overnight_watch_already_watching %s reason=%s "
                            "— idempotent success; original watcher ownership preserved",
                            ticker, signal_id, _reject_reason,
                        )
                        _mark_job_watching_armed(job_id, client_id, _arm_label)
                        result["armed"] += 1
                        continue

                    _full_error = f"overnight_watch_arm_failed:{_reject_reason}"
                    log.error(
                        "[%s] overnight_reeval: entry_watcher.watch() returned False "
                        "| contract=%s reason=%s disposition=%s",
                        ticker, _arm_label, _reject_reason, _arm_outcome.disposition,
                    )
                    _cleanup_success, _cleanup_method = _cleanup_overnight_watch_arm_failure(
                        order_state_machine=order_state_machine,
                        client_id=client_id,
                        signal_id=signal_id,
                        ticker=ticker,
                        side=side,
                        local_order_id=str(local_order_id),
                        contract=_arm_label,
                        contract_deferred=contract_deferred,
                        entry_trigger=entry_trigger,
                        reason=_full_error,
                        done_event="OVERNIGHT_WATCH_ARM_FAILED_CLEANUP_DONE",
                    )
                    if not _cleanup_success:
                        _cleanup_failed_reason = _watch_arm_cleanup_failed_reason(_full_error)
                        _cleanup_error_completion_ok = _complete_watch_arm_attempt_checked(
                            signal_id=signal_id,
                            client_id=client_id,
                            canonical_signal_id=_canonical_for_attempt,
                            signal_payload=signal,
                            execution_mode=_execution_mode,
                            session_key=session_key,
                            attempt=_watch_attempt,
                            state=WATCH_ATTEMPT_STATE_ERROR,
                            local_order_id=str(local_order_id),
                            reason=_cleanup_failed_reason,
                            ticker=ticker,
                            caller="watch_cleanup_failed",
                        )
                        log.critical(
                            "[%s] OVERNIGHT_WATCH_ARM_FAILED_CLEANUP_FAILED | "
                            "watch arm failed but local ENTRY cleanup failed "
                            "| local_order_id=%s cleanup_method=%s reason=%s "
                            "| cleanup_success=%s cleanup_failed_reason=%s",
                            ticker,
                            local_order_id,
                            _cleanup_method,
                            _full_error,
                            _cleanup_success,
                            _cleanup_failed_reason,
                        )
                        _record_watch_arm_failure_proof(
                            signal_id=signal_id,
                            client_id=client_id,
                            signal=signal,
                            reason=_cleanup_failed_reason,
                            local_order_id=str(local_order_id),
                            job_id=job_id,
                            is_exception=True,
                            session_key=session_key,
                            cleanup_method=_cleanup_method,
                            cleanup_success=False,
                            cleanup_failed=True,
                            original_reason=_full_error,
                        )
                        _mark_job_error(
                            job_id,
                            client_id,
                            (
                                _cleanup_failed_reason
                                if _cleanup_error_completion_ok
                                else "overnight_watch_arm_attempt_completion_failed:"
                                "watch_cleanup_failed"
                            ),
                        )
                        result["errors"] += 1
                        result["terminal_errors"] += 1
                        continue

                    if _arm_outcome.disposition == TERMINAL_FAILURE:
                        _record_watch_arm_failure_proof(
                            signal_id=signal_id,
                            client_id=client_id,
                            signal=signal,
                            reason=_full_error,
                            local_order_id=str(local_order_id),
                            job_id=job_id,
                            is_exception=False,
                            session_key=session_key,
                        )
                        _mark_job_rejected(job_id, client_id, _full_error)
                        if _lifecycle_ok:
                            try:
                                _sig_rejected(signal_id, ticker, _LO.WATCHER,
                                              "entry_watcher.watch() returned False",
                                              _RC.EXECUTION, "WATCHER_ARM_FAILED", _RS.WARNING,
                                              contract=_arm_label)
                            except Exception:
                                pass
                        result["rejected"] += 1
                        result["terminal_rejected"] += 1
                    else:
                        # RETRYABLE_NOT_ARMED: no permanent proof, no terminal reject.
                        # Durably preserve WATCHING state for next reeval pickup.
                        _terminal_result, _terminal_reason = _local_order_terminal_state(
                            order_state_machine,
                            str(local_order_id),
                            expected_client_id=client_id,
                            expected_execution_mode=_execution_mode,
                            expected_canonical_signal_id=_canonical_for_attempt,
                        )
                        if _terminal_result != WATCH_ATTEMPT_ACQUIRED:
                            _unproven_completion_ok = _complete_watch_arm_attempt_checked(
                                signal_id=signal_id,
                                client_id=client_id,
                                canonical_signal_id=_canonical_for_attempt,
                                signal_payload=signal,
                                execution_mode=_execution_mode,
                                session_key=session_key,
                                attempt=_watch_attempt,
                                state=WATCH_ATTEMPT_STATE_ERROR,
                                local_order_id=str(local_order_id),
                                reason=f"cleanup_terminal_unproven:{_terminal_reason}",
                                ticker=ticker,
                                caller="cleanup_terminal_unproven",
                            )
                            _mark_job_error(
                                job_id,
                                client_id,
                                (
                                    f"overnight_watch_arm_cleanup_unproven:{_terminal_reason}"
                                    if _unproven_completion_ok
                                    else "overnight_watch_arm_attempt_completion_failed:"
                                    "cleanup_terminal_unproven"
                                ),
                            )
                            result["errors"] += 1
                            result["terminal_errors"] += 1
                            continue
                        _next_state = (
                            WATCH_ATTEMPT_STATE_EXHAUSTED
                            if _watch_attempt.attempt_count >= OVERNIGHT_WATCH_ARM_MAX_ATTEMPTS
                            else WATCH_ATTEMPT_STATE_RETRYABLE
                        )
                        _retry_completion_ok = (
                            _complete_watch_arm_attempt_checked(
                                signal_id=signal_id,
                                client_id=client_id,
                                canonical_signal_id=_canonical_for_attempt,
                                signal_payload=signal,
                                execution_mode=_execution_mode,
                                session_key=session_key,
                                attempt=_watch_attempt,
                                state=_next_state,
                                local_order_id=str(local_order_id),
                                reason=_full_error,
                                ticker=ticker,
                                caller="watch_retry_completion",
                            )
                        )

                        if not _retry_completion_ok:
                            _completion_reason = (
                                "overnight_watch_arm_attempt_completion_failed:"
                                f"{_next_state}"
                            )
                            _mark_job_error(
                                job_id,
                                client_id,
                                _completion_reason,
                            )
                            result["errors"] += 1
                            result["terminal_errors"] += 1
                            continue

                        if _next_state == WATCH_ATTEMPT_STATE_EXHAUSTED:
                            _mark_job_error(job_id, client_id, "overnight_watch_arm_retry_exhausted")
                            result["errors"] += 1
                            result["terminal_errors"] += 1
                            continue
                        log.critical(
                            "[%s] overnight_watch_arm_unknown signal=%s reason=%s "
                            "disp=%s — fail closed as retryable_deferred (not terminal)",
                            ticker, signal_id, _reject_reason, _arm_outcome.disposition,
                        )
                        _mark_job_watching_reason(
                            job_id, client_id, "overnight_watch_arm_retryable"
                        )
                        result["skipped"] = result.get("skipped", 0) + 1
                        result["retryable_deferred"] += 1
            except Exception as ew_exc:
                _full_error = f"overnight_watch_arm_failed:exception:{ew_exc}"
                _cleanup_success, _cleanup_method = _cleanup_overnight_watch_arm_failure(
                    order_state_machine=order_state_machine,
                    client_id=client_id,
                    signal_id=signal_id,
                    ticker=ticker,
                    side=side,
                    local_order_id=str(local_order_id),
                    contract=_arm_label,
                    contract_deferred=contract_deferred,
                    entry_trigger=entry_trigger,
                    reason=_full_error,
                    done_event="OVERNIGHT_WATCH_ARM_EXCEPTION_CLEANUP_DONE",
                )
                if not _cleanup_success:
                    _cleanup_failed_reason = _watch_arm_cleanup_failed_reason(_full_error)
                    _exception_cleanup_completion_ok = _complete_watch_arm_attempt_checked(
                        signal_id=signal_id,
                        client_id=client_id,
                        canonical_signal_id=_canonical_for_attempt,
                        signal_payload=signal,
                        execution_mode=_execution_mode,
                        session_key=session_key,
                        attempt=_watch_attempt,
                        state=WATCH_ATTEMPT_STATE_ERROR,
                        local_order_id=str(local_order_id),
                        reason=_cleanup_failed_reason,
                        ticker=ticker,
                        caller="watch_exception_cleanup_failed",
                    )
                    log.critical(
                        "[%s] OVERNIGHT_WATCH_ARM_FAILED_CLEANUP_FAILED | "
                        "entry_watcher.watch exception and local ENTRY cleanup failed "
                        "| local_order_id=%s cleanup_method=%s reason=%s "
                        "| cleanup_success=%s cleanup_failed_reason=%s",
                        ticker,
                        local_order_id,
                        _cleanup_method,
                        _full_error,
                        _cleanup_success,
                        _cleanup_failed_reason,
                    )
                    _record_watch_arm_failure_proof(
                        signal_id=signal_id,
                        client_id=client_id,
                        signal=signal,
                        reason=_cleanup_failed_reason,
                        local_order_id=str(local_order_id),
                        job_id=job_id,
                        is_exception=True,
                        session_key=session_key,
                        cleanup_method=_cleanup_method,
                        cleanup_success=False,
                        cleanup_failed=True,
                        original_reason=_full_error,
                    )
                    _mark_job_error(
                        job_id,
                        client_id,
                        (
                            _cleanup_failed_reason
                            if _exception_cleanup_completion_ok
                            else "overnight_watch_arm_attempt_completion_failed:"
                            "watch_exception_cleanup_failed"
                        ),
                    )
                    result["errors"] += 1
                    result["terminal_errors"] += 1
                    continue
                # PR #404 final amendment: cleanup succeeded on a watcher
                # exception. The classifier maps this to RETRYABLE_NOT_ARMED;
                # persisting ERROR would permanently block retries and
                # contradict the classifier. Complete as RETRYABLE (or
                # EXHAUSTED at max_attempts) ONLY after the exact local
                # ENTRY is proven terminal for the expected identity.
                _terminal_result, _terminal_reason = _local_order_terminal_state(
                    order_state_machine,
                    str(local_order_id),
                    expected_client_id=client_id,
                    expected_execution_mode=_execution_mode,
                    expected_canonical_signal_id=_canonical_for_attempt,
                )
                if _terminal_result != WATCH_ATTEMPT_ACQUIRED:
                    # Terminal proof missing / active / mismatched / DB error
                    # — fail closed as ERROR (do NOT claim retryability).
                    _exception_unproven_completion_ok = (
                        _complete_watch_arm_attempt_checked(
                            signal_id=signal_id,
                            client_id=client_id,
                            canonical_signal_id=_canonical_for_attempt,
                            signal_payload=signal,
                            execution_mode=_execution_mode,
                            session_key=session_key,
                            attempt=_watch_attempt,
                            state=WATCH_ATTEMPT_STATE_ERROR,
                            local_order_id=str(local_order_id),
                            reason=(
                                "watch_exception_cleanup_terminal_unproven:"
                                f"{_terminal_reason}"
                            ),
                            ticker=ticker,
                            caller="watch_exception_cleanup_terminal_unproven",
                        )
                    )
                    _record_watch_arm_failure_proof(
                        signal_id=signal_id,
                        client_id=client_id,
                        signal=signal,
                        reason=_full_error,
                        local_order_id=str(local_order_id),
                        job_id=job_id,
                        is_exception=True,
                        session_key=session_key,
                    )
                    _mark_job_error(
                        job_id,
                        client_id,
                        (
                            "overnight_watch_arm_exception_terminal_unproven:"
                            f"{_terminal_reason}"
                            if _exception_unproven_completion_ok
                            else "overnight_watch_arm_attempt_completion_failed:"
                            "watch_exception_cleanup_terminal_unproven"
                        ),
                    )
                    log.error(
                        "[%s] overnight_reeval: watcher exception cleanup terminal-"
                        "unproven err=%s terminal_reason=%s",
                        ticker, ew_exc, _terminal_reason,
                    )
                    result["errors"] += 1
                    result["terminal_errors"] += 1
                    continue

                _next_state = (
                    WATCH_ATTEMPT_STATE_EXHAUSTED
                    if _watch_attempt.attempt_count
                    >= OVERNIGHT_WATCH_ARM_MAX_ATTEMPTS
                    else WATCH_ATTEMPT_STATE_RETRYABLE
                )
                _exception_completion_ok = _complete_watch_arm_attempt_checked(
                    signal_id=signal_id,
                    client_id=client_id,
                    canonical_signal_id=_canonical_for_attempt,
                    signal_payload=signal,
                    execution_mode=_execution_mode,
                    session_key=session_key,
                    attempt=_watch_attempt,
                    state=_next_state,
                    local_order_id=str(local_order_id),
                    reason=_full_error,
                    ticker=ticker,
                    caller="watch_exception_retry_completion",
                )
                # Failure proof is written on every exception path so operator
                # visibility of the exception itself is never lost, whether
                # the classification winds up RETRYABLE, EXHAUSTED, or ERROR.
                _record_watch_arm_failure_proof(
                    signal_id=signal_id,
                    client_id=client_id,
                    signal=signal,
                    reason=_full_error,
                    local_order_id=str(local_order_id),
                    job_id=job_id,
                    is_exception=True,
                    session_key=session_key,
                    retryable_exception=(
                        _exception_completion_ok
                        and _next_state == WATCH_ATTEMPT_STATE_RETRYABLE
                    ),
                )
                log.error(
                    "[%s] overnight_reeval: entry_watcher.watch failed: %s "
                    "(cleanup ok, next_state=%s, completion_ok=%s)",
                    ticker, ew_exc, _next_state, _exception_completion_ok,
                )

                if not _exception_completion_ok:
                    _mark_job_error(
                        job_id, client_id,
                        "overnight_watch_arm_attempt_completion_failed:"
                        f"{_next_state}:watch_exception",
                    )
                    result["errors"] += 1
                    result["terminal_errors"] += 1
                    continue

                if _next_state == WATCH_ATTEMPT_STATE_EXHAUSTED:
                    _mark_job_error(
                        job_id, client_id,
                        "overnight_watch_arm_retry_exhausted",
                    )
                    result["errors"] += 1
                    result["terminal_errors"] += 1
                    continue

                # RETRYABLE: durably deferred, NOT terminal. Do not call
                # _mark_job_error. The classifier's RETRYABLE_NOT_ARMED
                # contract is now honored by the runtime as well.
                _mark_job_watching_reason(
                    job_id, client_id,
                    "overnight_watch_arm_retryable_exception",
                )
                result["skipped"] = result.get("skipped", 0) + 1
                result["retryable_deferred"] += 1

        except Exception as outer_exc:
            log.error("[%s] overnight_reeval: unexpected error for %s: %s", ticker, signal_id, outer_exc, exc_info=True)
            if _paper_rescue_only:
                _mark_job_error(
                    job_id,
                    client_id,
                    _paper_rescue_queue_reason(
                        "overnight_reeval_internal_error",
                        type(outer_exc).__name__,
                    ),
                )
            result["errors"] += 1
            result["terminal_errors"] += 1

    result["stale_inventory_only"] = bool(
        result["processed"] > 0
        and result["fresh_processed"] == 0
        and result["stale_skipped"] > 0
    )
    result["unresolved"] = max(
        0,
        result["processed"]
        - result["armed"]
        - result["terminal_rejected"]
        - result["terminal_errors"]
        - result["retryable_deferred"]
        - result["already_resolved"],
    )
    log.info(
        "[%s] overnight_reeval complete: processed=%d armed=%d terminal_rejected=%d "
        "terminal_errors=%d retryable_deferred=%d unresolved=%d skipped=%d "
        "stale_skipped=%d fresh_processed=%d fresh_armed=%d stale_inventory_only=%s",
        client_id,
        result["processed"],
        result["armed"],
        result["terminal_rejected"],
        result["terminal_errors"],
        result["retryable_deferred"],
        result["unresolved"],
        result["skipped"],
        result["stale_skipped"],
        result["fresh_processed"],
        result["fresh_armed"],
        result["stale_inventory_only"],
    )
    if result["stale_inventory_only"] and result["armed"] == 0:
        log.warning(
            "[%s] overnight_reeval stale inventory only: handoff/readiness must not imply fresh setup success | "
            "handoff_already_succeeded_for_stage_today may be expected downstream | fresh_armed=%d stale_inventory_only=%s",
            client_id,
            result["fresh_armed"],
            result["stale_inventory_only"],
        )
    if result["retryable_deferred"] > 0 and result["terminal_rejected"] == 0 and result["armed"] == 0:
        log.error(
            "[%s] OVERNIGHT_REEVAL_STALLED fetched=%d retryable_deferred=%d — "
            "every owned signal deferred to next run; queue is NOT draining. "
            "category=SILENT_STALL severity=ERROR "
            "likely_cause=market_data_unavailable_or_all_rows_retry_later",
            client_id,
            result["fetched"],
            result["retryable_deferred"],
        )
    return _classify_overnight_reeval_result(result)


_SOURCE_STATUS_SUCCESS = "SUCCESS"
_SOURCE_STATUS_FAILED  = "FAILED"


class _FetchWatchingSignalsResult(NamedTuple):
    """Source-truth result of _fetch_watching_signals.

    .rows                — list of job dicts (may be non-empty even if one
                           source failed; caller must inspect the statuses).
    .trade_queue_status  — SUCCESS / FAILED
    .ap_signals_status   — SUCCESS / FAILED
    .trade_queue_error   — short error string if FAILED
    .ap_signals_error    — short error string if FAILED

    A FAILED status must never be interpreted as "zero rows / completed no
    work". The caller must classify the run retryable when any source is
    FAILED (partial inventory) and RETRYABLE_SOURCE_LOOKUP_FAILED when both
    sources are FAILED and rows==0.
    """
    rows: list
    trade_queue_status: str
    ap_signals_status:  str
    trade_queue_error:  Optional[str]
    ap_signals_error:   Optional[str]


def _fetch_watching_signals(client_id: str) -> list:
    """Backward-compatible wrapper that returns rows only.

    Retained for callers that already expect a list AND for existing test
    harnesses that monkey-patch this function to inject fixture rows. The
    real reeval path calls _fetch_watching_signals_with_status which, when
    this function is patched, wraps the patched result with SUCCESS status
    so P0-2 source-truth behavior stays consistent for legacy fixtures."""
    return _fetch_watching_signals_with_status_impl(client_id).rows


# Sentinel attribute stamped on the built-in wrapper so
# _fetch_watching_signals_with_status can detect a monkey-patched
# replacement (which will not carry the sentinel) even across
# importlib.reload cycles, where an is-comparison against a captured
# original can fail because reload creates a new function object.
_fetch_watching_signals._is_original_wrapper = True


def _fetch_watching_signals_with_status(client_id: str) -> _FetchWatchingSignalsResult:
    """Public status-aware entry point used by run_overnight_reeval.

    Two-tier dispatch so legacy tests keep working:
      * If a test has monkey-patched _fetch_watching_signals (list-returning),
        call it and wrap the list in a SUCCESS status result.
      * Otherwise run the real implementation which captures per-source
        status alongside rows.

    A test that wants to inject a source FAILURE must monkey-patch this
    function directly (or _fetch_watching_signals_with_status_impl).

    Detection strategy: the built-in wrapper carries a sentinel attribute
    _is_original_wrapper=True. Any test-supplied replacement will not
    have that attribute. This is robust to importlib.reload cycles used
    by other tests (e.g. test_p0_paper_overnight_rescue_materialization
    reloads this module twice, which invalidates a captured-object
    identity check).

    We use globals() rather than sys.modules[__name__] because
    importlib.reload creates a NEW module object registered at the same
    name; a test file that imported this module BEFORE the reload holds
    an OLD reference and applies monkeypatch.setattr(ov, ...) to the OLD
    module, while sys.modules[__name__] returns the NEW post-reload
    module. globals() always returns the namespace of the module this
    function was DEFINED in — the same one whose _fetch_watching_signals
    the test just patched.
    """
    _current_legacy = globals().get("_fetch_watching_signals")
    _is_patched = (
        _current_legacy is not None
        and not getattr(_current_legacy, "_is_original_wrapper", False)
    )
    if _is_patched:
        # A test has replaced the legacy wrapper with its own function.
        # Call it and wrap the list result as SUCCESS from both sources.
        try:
            rows = list(_current_legacy(client_id) or [])
        except Exception as _pexc:
            log.error("patched _fetch_watching_signals raised: %s", _pexc)
            return _FetchWatchingSignalsResult(
                rows=[],
                trade_queue_status=_SOURCE_STATUS_FAILED,
                ap_signals_status=_SOURCE_STATUS_FAILED,
                trade_queue_error=str(_pexc),
                ap_signals_error=str(_pexc),
            )
        return _FetchWatchingSignalsResult(
            rows=rows,
            trade_queue_status=_SOURCE_STATUS_SUCCESS,
            ap_signals_status=_SOURCE_STATUS_SUCCESS,
            trade_queue_error=None,
            ap_signals_error=None,
        )
    return _fetch_watching_signals_with_status_impl(client_id)


# Captured at module import so _fetch_watching_signals_with_status can detect
# a test-time replacement of the legacy wrapper.
_ORIGINAL_FETCH_WATCHING_SIGNALS = _fetch_watching_signals


def _fetch_watching_signals_with_status_impl(client_id: str) -> _FetchWatchingSignalsResult:
    """
    Fetch WATCHING signals from both sources of truth AND report per-source
    lookup status. See _FetchWatchingSignalsResult docstring.

    1. trade_queue.status='WATCHING' for locally queued in-session jobs.
    2. ap_signals.decision_status='WATCHING' for scanner/audit signals written
       by APSignalStore.
    """
    results: list[dict] = []
    seen_signal_ids: set[str] = set()
    trade_queue_status: str = _SOURCE_STATUS_SUCCESS
    trade_queue_error:  Optional[str] = None
    ap_signals_status:  str = _SOURCE_STATUS_SUCCESS
    ap_signals_error:   Optional[str] = None

    # PR #388 P0-8: SINGLE fetch limit for BOTH sources. Previously the
    # shared ap_signals query was hardcoded at .limit(300) while trade_queue
    # honored OVERNIGHT_FETCH_LIMIT (default 500). Once the shared inventory
    # exceeded 300 rows, the newest 300 could permanently occupy every fetch
    # and older rows never entered the per-client disposition resolver.
    try:
        _fetch_limit = int(os.getenv("OVERNIGHT_FETCH_LIMIT", "500"))
    except (TypeError, ValueError):
        _fetch_limit = 500
    _fetch_limit = max(100, min(_fetch_limit, 2000))

    # Source 1: local Postgres trade_queue.
    try:
        from ap.db import conn, run_with_retry
        import json as _j

        def _fn():
            with conn() as c:
                c.execute("""
                    SELECT id, signal_id, payload, created_ts
                    FROM trade_queue
                    WHERE client_id = %s
                      AND status = 'WATCHING'
                    ORDER BY created_ts DESC
                    LIMIT %s
                """, (client_id, _fetch_limit))
                return c.fetchall()

        rows = run_with_retry(_fn) or []
        for row in rows:
            d = dict(row) if not isinstance(row, dict) else row
            if isinstance(d.get("payload"), str):
                try:
                    d["payload"] = _j.loads(d["payload"])
                except Exception:
                    pass
            d["_source"] = "trade_queue"
            results.append(d)
            if d.get("signal_id"):
                seen_signal_ids.add(str(d["signal_id"]))
    except Exception as e:
        log.error("_fetch_watching_signals[trade_queue] failed: %s", e)
        trade_queue_status = _SOURCE_STATUS_FAILED
        trade_queue_error = str(e)

    # Source 2: Supabase ap_signals.
    try:
        import json as _j2
        from supabase import create_client as _cc

        sb_url = os.getenv("SUPABASE_URL", "")
        sb_key = (
            os.getenv("SUPABASE_SERVICE_KEY", "")
            or os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
            or os.getenv("SUPABASE_ANON_KEY", "")
        )
        if not sb_url or not sb_key:
            log.error(
                "_fetch_watching_signals[ap_signals]: missing Supabase credentials "
                "— source lookup cannot proceed, treating as FAILED (not zero-rows)"
            )
            ap_signals_status = _SOURCE_STATUS_FAILED
            ap_signals_error = "missing_supabase_credentials"
        else:
            # Session-aware cutoff (see _signals_lookback_cutoff_iso). A
            # Friday scanner setup remains visible on Monday morning without
            # requiring an explicit SIGNALS_LOOKBACK override on every
            # deployment. Env var still honored when explicitly set.
            cutoff = _signals_lookback_cutoff_iso()
            sb = _cc(sb_url, sb_key)
            # ── MULTI-CLIENT FAN-OUT FIX ──────────────────────────────────
            # Do NOT filter ap_signals by client_email here. WATCHING rows in
            # ap_signals are SCANNER SETUPS (SMCI, BA, HOOD, SPY, ...), not
            # per-client jobs. The scanner writes a setup once, tagged to
            # whichever client_email it first ran for. Filtering by
            # client_email meant each client's overnight reeval only saw the
            # setups tagged to itself — so SMCI reached tradefluence but Jose's
            # reeval queried client_email=jose, found nothing, and SILENTLY
            # skipped SMCI entirely (no decision, no rejection, no trail).
            #
            # Every active client's reeval must see the SAME universe of
            # setups. Each client then runs its OWN risk/decision path via
            # master_control.evaluate(signal, client_id=client_id) downstream,
            # which logs an APPROVE or REJECT decision event per client. Same
            # setups for everyone; separate per-client execution decisions.
            #
            # (Source 1 trade_queue stays client-scoped — those are genuine
            # per-client in-session jobs, not shared scanner setups.)
            res = (
                sb.table("ap_signals")
                .select(
                    "signal_id, signal_payload, raw_payload, created_at, ticker, "
                    "side, score, timeframe, pattern, tier, decision_status, "
                    "entry_trigger, stop_price, target_price, underlying_at_signal"
                )
                .eq("decision_status", "WATCHING")
                .gte("created_at", cutoff)
                .order("created_at", desc=True)
                .limit(_fetch_limit)  # P0-8: unified with trade_queue limit
                .execute()
            )

            for row in (res.data or []):
                sid = str(row.get("signal_id") or "")
                if not sid or sid in seen_signal_ids:
                    continue

                payload = row.get("signal_payload") or row.get("raw_payload") or {}
                if isinstance(payload, str):
                    try:
                        payload = _j2.loads(payload)
                    except Exception:
                        payload = {}
                if not isinstance(payload, dict):
                    payload = {}

                # Hydrate required downstream fields from columns if the JSON
                # payload is sparse.
                for key in ("ticker", "side", "score", "timeframe", "pattern", "tier"):
                    if not payload.get(key) and row.get(key) is not None:
                        payload[key] = row.get(key)
                if not payload.get("symbol") and row.get("ticker"):
                    payload["symbol"] = row.get("ticker")
                if not payload.get("signal_id"):
                    payload["signal_id"] = sid
                if not payload.get("created_at") and row.get("created_at"):
                    payload["created_at"] = row.get("created_at")
                if not payload.get("entry_trigger") and row.get("entry_trigger") is not None:
                    payload["entry_trigger"] = row.get("entry_trigger")
                if not payload.get("stop_price") and row.get("stop_price") is not None:
                    payload["stop_price"] = row.get("stop_price")
                if not payload.get("target_price") and row.get("target_price") is not None:
                    payload["target_price"] = row.get("target_price")
                if not payload.get("underlying_price") and row.get("underlying_at_signal") is not None:
                    payload["underlying_price"] = row.get("underlying_at_signal")

                results.append({
                    "id": f"sup:{sid}",
                    "signal_id": sid,
                    "payload": payload,
                    "created_ts": row.get("created_at"),
                    "_source": "ap_signals",
                })
                seen_signal_ids.add(sid)
    except Exception as e:
        log.error("_fetch_watching_signals[ap_signals] failed: %s", e)
        ap_signals_status = _SOURCE_STATUS_FAILED
        ap_signals_error = str(e)

    # ── SETUP-IDENTITY DEDUP ──────────────────────────────────────────────
    # Now that ap_signals is fetched WITHOUT a client_email filter, the same
    # logical setup (e.g. SMCI PUT 1d) may appear multiple times if it was
    # stored under more than one client_email. signal_id dedup (above) only
    # catches identical signal_ids. Collapse to one row per unique setup
    # identity (ticker|side|timeframe|entry_trigger) so each client's reeval
    # evaluates each distinct setup exactly once. trade_queue rows are kept
    # as-is (genuine per-client jobs, never deduped against scanner setups).
    _deduped: list[dict] = []
    _seen_setup_keys: set = set()
    for r in results:
        if r.get("_source") == "trade_queue":
            _deduped.append(r)
            continue
        p = r.get("payload") or {}
        _tkr = str(p.get("ticker") or p.get("symbol") or "").upper().strip()
        _side = str(p.get("side") or "").upper().strip()
        _tf = str(p.get("timeframe") or "").lower().strip()
        _trig = p.get("entry_trigger")
        try:
            _trig_k = round(float(_trig), 4) if _trig is not None else None
        except (TypeError, ValueError):
            _trig_k = None
        _key = (_tkr, _side, _tf, _trig_k)
        if _tkr and _key in _seen_setup_keys:
            log.info(
                "[%s] dedup: skipping duplicate setup %s %s %s (already have one)",
                client_id, _tkr, _side, _tf,
            )
            continue
        if _tkr:
            _seen_setup_keys.add(_key)
        _deduped.append(r)
    results = _deduped

    tq_count = sum(1 for r in results if r.get("_source") == "trade_queue")
    sup_count = sum(1 for r in results if r.get("_source") == "ap_signals")
    log.info(
        "[%s] _fetch_watching_signals: total=%d trade_queue=%d ap_signals=%d "
        "trade_queue_status=%s ap_signals_status=%s "
        "(fan-out: all clients see same scanner setups)",
        client_id, len(results), tq_count, sup_count,
        trade_queue_status, ap_signals_status,
    )
    return _FetchWatchingSignalsResult(
        rows=results,
        trade_queue_status=trade_queue_status,
        ap_signals_status=ap_signals_status,
        trade_queue_error=trade_queue_error,
        ap_signals_error=ap_signals_error,
    )


def _mark_job_rejected(job_id, client_id: str, reason: str) -> None:
    """Mark a WATCHING job rejected in its original source table.

    MULTI-CLIENT NOTE: for ap_signals scanner setups (sup: prefix) we do NOT
    flip the shared row's decision_status. That row is shared across all
    clients now (fan-out fix) — if client A's reeval flipped it to 'rejected'
    it would vanish from the WATCHING pool for clients B, C, ... before their
    reevals ran, recreating the exact silent-skip bug we just fixed. The
    per-client rejection is ALREADY recorded by master_control.evaluate's
    DECISION_EVENT (client_id scoped) and there is no per-client column on
    ap_signals. The shared scanner row ages out naturally via the created_at
    cutoff. trade_queue rows ARE genuinely per-client and still update.
    """
    job_id_str = str(job_id)
    if job_id_str.startswith("sup:"):
        # Per-client decision already logged via master_control DECISION_EVENT.
        # Do not mutate the shared scanner signal row.
        log.info(
            "[%s] reeval rejected shared setup %s — per-client decision logged "
            "(shared ap_signals row left WATCHING for other clients): %s",
            client_id, job_id_str[4:], reason[:200],
        )
        return

    try:
        from ap.db import conn, run_with_retry

        def _fn():
            with conn() as c:
                c.execute("""
                    UPDATE trade_queue
                    SET status = 'REJECTED',
                        last_error = %s,
                        finished_ts = NOW()
                    WHERE id = %s AND client_id = %s
                """, (reason[:500], job_id, client_id))
        run_with_retry(_fn)
    except Exception as e:
        log.debug("_mark_job_rejected[trade_queue] failed non-fatal: %s", e)


def _mark_job_error(job_id, client_id: str, reason: str) -> None:
    """Mark a WATCHING job errored in its original source table.

    Shared ap_signals rows are not mutated; the per-client proof lives in the
    opportunity ledger. trade_queue rows remain client-scoped and can be
    terminally updated.
    """
    job_id_str = str(job_id)
    if job_id_str.startswith("sup:"):
        log.info(
            "[%s] reeval errored shared setup %s — per-client error proof logged "
            "(shared ap_signals row left WATCHING for other clients): %s",
            client_id, job_id_str[4:], reason[:200],
        )
        return

    try:
        from ap.db import conn, run_with_retry

        def _fn():
            with conn() as c:
                c.execute("""
                    UPDATE trade_queue
                    SET status = 'ERROR',
                        last_error = %s,
                        finished_ts = NOW()
                    WHERE id = %s AND client_id = %s
                """, (reason[:500], job_id, client_id))
        run_with_retry(_fn)
    except Exception as e:
        log.debug("_mark_job_error[trade_queue] failed non-fatal: %s", e)


def _mark_job_watching_reason(job_id, client_id: str, reason: str) -> None:
    """Keep WATCHING rows non-terminal but explicit when data is still pending."""
    job_id_str = str(job_id)
    if job_id_str.startswith("sup:"):
        log.info(
            "[%s] reeval waiting shared setup %s — shared row untouched: %s",
            client_id, job_id_str[4:], reason[:200],
        )
        return

    try:
        from ap.db import conn, run_with_retry

        def _fn():
            with conn() as c:
                c.execute("""
                    UPDATE trade_queue
                    SET status = 'WATCHING',
                        last_error = %s,
                        started_ts = COALESCE(started_ts, NOW()),
                        finished_ts = NULL
                    WHERE id = %s AND client_id = %s
                """, (reason[:500], job_id, client_id))
        run_with_retry(_fn)
    except Exception as e:
        log.debug("_mark_job_watching_reason[trade_queue] failed non-fatal: %s", e)


def _mark_job_watching_armed(job_id, client_id: str, contract: str) -> None:
    """Record that a WATCHING job has been armed in the entry watcher.

    MULTI-CLIENT NOTE: same rule as _mark_job_rejected. For shared ap_signals
    scanner setups (sup: prefix) we do NOT flip the shared row to 'armed'.
    The arm is tracked per-client by the entry watcher (keyed by client_id)
    and the per-client order/watcher rows. Flipping the shared row would hide
    the setup from other clients' reevals. trade_queue rows are per-client
    and still update normally.
    """
    job_id_str = str(job_id)
    label = f"armed:contract={contract}"[:500]
    if job_id_str.startswith("sup:"):
        log.info(
            "[%s] reeval armed shared setup %s for this client "
            "(per-client watcher created; shared ap_signals row left WATCHING "
            "for other clients) %s",
            client_id, job_id_str[4:], label,
        )
        return

    try:
        from ap.db import conn, run_with_retry

        def _fn():
            with conn() as c:
                c.execute("""
                    UPDATE trade_queue
                    SET status = 'ARMED',
                        last_error = %s,
                        started_ts = COALESCE(started_ts, NOW())
                    WHERE id = %s AND client_id = %s AND status = 'WATCHING'
                """, (label, job_id, client_id))
        run_with_retry(_fn)
    except Exception as e:
        log.debug("_mark_job_watching_armed[trade_queue] failed non-fatal: %s", e)


def _log_rejection_supabase(
    signal_id: str, client_id: str, ticker: str, side: str,
    score: float, stage: str, reason_code: str, human_reason: str, payload: dict,
) -> None:
    try:
        import os as _os
        from supabase import create_client as _cc
        sb_url = _os.getenv("SUPABASE_URL", "")
        sb_key = _os.getenv("SUPABASE_SERVICE_KEY", "")
        if not sb_url or not sb_key:
            return
        sbc = _cc(sb_url, sb_key)
        upsert_ap_signal_row_with_fallback(sbc, {
            "signal_id":       canonical_signal_id(str(signal_id)),
            "client_email":    canonical_client_email(client_id),
            "system_version":  "v2",
            "ticker":          str(ticker),
            "side":            str(side).upper(),
            "score":           float(score or 0),
            "tier":            str(payload.get("tier") or "B"),
            "pattern":         str(payload.get("pattern") or ""),
            "timeframe":       str(payload.get("timeframe") or "1d"),
            "decision_status": "rejected",
            "context_notes":   f"stage={stage} | {reason_code} | {human_reason}",
            "raw_payload": {
                "stage": stage,
                "reason_code": reason_code,
                "human_reason": human_reason,
                **{k: v for k, v in payload.items()
                   if k not in ("raw_payload",) and not callable(v)},
            },
        })
    except Exception as e:
        log.debug("_log_rejection_supabase failed: %s", e)
