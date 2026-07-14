"""
ap.operator_queue_read_model
────────────────────────────
Production-safe read model for the Operator Console Trade Queue card.

This module intentionally does not mutate anything.  It only translates the
bot's current queue/order statuses into the dashboard buckets the operator
page renders (NEW / WATCHING / TRIGGERED / REJECTED / EXPIRED) and — for
every ENTRY order row — derives a single, mutually exclusive
``execution_state`` that precisely answers:

    Why does this row have no broker_order_id?
    Is it healthy?
    Is it overdue?
    What should happen next?
    Does an operator need to act?

Classification precedence (strict — no row may match two states):

    A. broker_order_id present               → BROKER_SUBMITTED
    B. terminal order status                 → TERMINAL
    C. submit_intent_at present, no broker id
         current_owner = broker_reconciler:* → RECONCILE_PENDING
         otherwise                           → SUBMIT_INTENT_PENDING
    D. broker_ready = true                   → BROKER_READY
    E. lifecycle_state = MATERIALIZING
       OR materialization_in_flight = true   → MATERIALIZING
    F. lifecycle_state = RETRY_WAIT
         retry timestamp future              → FUTURE_RETRY_SCHEDULED
         retry timestamp due / past          → DUE_RETRY_PENDING
         timestamp missing / malformed       → INCONSISTENT_STATE
    G. final_market_validity.passed = false  → FINAL_GATE_BLOCKED
    H. status = PENDING_TRIGGER              → PRE_BREACH
    I. otherwise                             → INCONSISTENT_STATE

Invariant violations always produce INCONSISTENT_STATE regardless of which
branch the normal precedence would have selected.

Schema guard:
- trade_queue does not have updated_ts. Use created_ts/started_ts/finished_ts.
- orders does have updated_ts.

Read-only contract: this module must never issue UPDATE / INSERT / DELETE.
"""
from __future__ import annotations

import datetime as _dt
from typing import Any


# ─────────────────────────────────────────────────────────────────────────────
# Dashboard bucket constants (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

DASHBOARD_BUCKETS = ("NEW", "WATCHING", "TRIGGERED", "REJECTED", "EXPIRED")


def dashboard_queue_bucket(status: str | None) -> str:
    s = str(status or "").strip().upper()
    if s == "NEW":
        return "NEW"
    if s in {"WATCHING", "PENDING_TRIGGER"}:
        return "WATCHING"
    if s in {"TRIGGERED", "SUBMITTED", "ACK", "ACKNOWLEDGED", "WORKING"}:
        return "TRIGGERED"
    if s == "REJECTED":
        return "REJECTED"
    if s == "EXPIRED":
        return "EXPIRED"
    return "UNKNOWN"


def empty_queue_counts() -> dict[str, int]:
    return {bucket: 0 for bucket in DASHBOARD_BUCKETS}


# ─────────────────────────────────────────────────────────────────────────────
# Execution-state constants
# ─────────────────────────────────────────────────────────────────────────────

class EntryExecutionState:
    """Mutually exclusive execution states for every ENTRY order row."""
    PRE_BREACH             = "PRE_BREACH"
    FUTURE_RETRY_SCHEDULED = "FUTURE_RETRY_SCHEDULED"
    DUE_RETRY_PENDING      = "DUE_RETRY_PENDING"
    MATERIALIZING          = "MATERIALIZING"
    BROKER_READY           = "BROKER_READY"
    FINAL_GATE_BLOCKED     = "FINAL_GATE_BLOCKED"
    SUBMIT_INTENT_PENDING  = "SUBMIT_INTENT_PENDING"
    RECONCILE_PENDING      = "RECONCILE_PENDING"
    BROKER_SUBMITTED       = "BROKER_SUBMITTED"
    TERMINAL               = "TERMINAL"
    INCONSISTENT_STATE     = "INCONSISTENT_STATE"


# Orders whose status alone indicates the lifecycle is over.
_TERMINAL_ORDER_STATUSES: frozenset[str] = frozenset({
    "REJECTED", "EXPIRED", "CANCELED", "ERROR", "FILLED",
    "WATCHER_INVALIDATED", "INTERNAL_ERROR", "BROKER_REJECTED",
    "MISSED", "CLIENT_SKIPPED", "ENTRY_CONFIRMATION_FAILED",
})


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _dict(row: Any) -> dict:
    if isinstance(row, dict):
        return row
    try:
        return dict(row)
    except Exception:
        return {}


def _meta_bool(meta: dict, key: str) -> bool:
    """Safely extract a boolean from a meta JSONB dict.

    Postgres stores JSONB booleans as Python bools when fetched via
    RealDictCursor, but they may also arrive as the strings 'true'/'false'
    if the column was cast through a text path.
    """
    v = meta.get(key)
    if isinstance(v, bool):
        return v
    return str(v or "").strip().lower() == "true"


def _meta_str(meta: dict, key: str) -> str:
    return str(meta.get(key) or "").strip()


def _parse_iso_utc(ts: Any) -> _dt.datetime | None:
    """Parse an ISO-8601 timestamp string to a UTC-aware datetime.

    Returns None if the value is absent, blank, unparseable, or timezone-naive.

    Timezone-naive timestamps are treated as malformed and return None rather
    than being silently assumed to be UTC.  The execution lifecycle stamps all
    retry timestamps with explicit UTC offsets; a naive value means the
    metadata is corrupted or was written by a non-conforming code path.
    Returning None causes the invariant checker or the retry branch to produce
    INCONSISTENT_STATE, which is the correct fail-closed behaviour.
    """
    s = str(ts or "").strip()
    if not s:
        return None
    # Normalise the common trailing-Z form that Python < 3.11 doesn't accept.
    s = s.replace("Z", "+00:00")
    try:
        dt = _dt.datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        # Naive timestamp — treat as malformed, not as UTC.
        return None
    return dt


def _utcnow() -> _dt.datetime:
    return _dt.datetime.now(_dt.timezone.utc)


# ─────────────────────────────────────────────────────────────────────────────
# Invariant checker
# ─────────────────────────────────────────────────────────────────────────────

def _check_entry_invariants(
    status: str,
    contract: str | None,
    limit_price: Any,
    execution_mode: str | None,
    broker_order_id: str | None,
    meta: dict,
) -> tuple[bool, str]:
    """Check all structural invariants for a single ENTRY order row.

    Returns ``(violated, violation_code)``.  ``violation_code`` is the empty
    string when no invariant is violated.

    The caller must call this before the classification precedence chain.  Any
    violation short-circuits classification to INCONSISTENT_STATE.
    """
    broker_ready     = _meta_bool(meta, "broker_ready")
    submit_intent_at = _meta_str(meta, "submit_intent_at")
    lifecycle_state  = _meta_str(meta, "lifecycle_state").upper()
    mat_in_flight    = _meta_bool(meta, "materialization_in_flight")
    mat_generation   = meta.get("materialization_generation")
    mat_owner        = _meta_str(meta, "materialization_owner")
    current_owner    = _meta_str(meta, "current_owner")
    owner_for_check  = mat_owner or current_owner
    status_upper     = str(status or "").strip().upper()
    contract_upper   = str(contract or "").strip().upper()

    # Invariant 1: broker_ready=true with DEFERRED contract
    if broker_ready and contract_upper.startswith("DEFERRED:"):
        return True, "BROKER_READY_WITH_DEFERRED_CONTRACT"

    # Invariant 2: broker_ready=true with limit_price <= 0.01
    if broker_ready:
        try:
            if float(limit_price or 0) <= 0.01:
                return True, "BROKER_READY_WITH_INVALID_LIMIT_PRICE"
        except (TypeError, ValueError):
            return True, "BROKER_READY_WITH_NON_NUMERIC_LIMIT_PRICE"

    # Invariant 3: broker_order_id present while status=PENDING_TRIGGER
    if broker_order_id and status_upper == "PENDING_TRIGGER":
        return True, "BROKER_ORDER_ID_WITH_PENDING_TRIGGER_STATUS"

    # Invariant 4: submit_intent_at present after terminal status
    if submit_intent_at and status_upper in _TERMINAL_ORDER_STATUSES:
        return True, f"SUBMIT_INTENT_AFTER_TERMINAL_STATUS:{status_upper}"

    # Invariant 5: RETRY_WAIT with no retry timestamp at all.
    # All three timestamp fields are checked — same priority order as
    # _resolve_retry_timestamp — so the invariant only fires when every
    # known timestamp source is missing or blank.
    if lifecycle_state == "RETRY_WAIT":
        mat_next       = _meta_str(meta, "materialization_next_retry_at")
        next_retry     = _meta_str(meta, "next_retry_at")
        deferred_next  = _meta_str(meta, "deferred_retry_next_attempt_at")
        if not mat_next and not next_retry and not deferred_next:
            return True, "RETRY_WAIT_WITHOUT_RETRY_TIMESTAMP"

    # Invariant 6: MATERIALIZING lifecycle with no owner
    if lifecycle_state == "MATERIALIZING" and not owner_for_check:
        return True, "MATERIALIZING_WITHOUT_OWNER"

    # Invariant 7: materialization_in_flight=true with no generation
    if mat_in_flight and not mat_generation:
        return True, "MATERIALIZATION_IN_FLIGHT_WITHOUT_GENERATION"

    # Invariant 8: execution_mode present but not a known value
    mode = str(execution_mode or "").strip().lower()
    if mode and mode not in ("paper", "live", "unknown"):
        return True, f"INVALID_EXECUTION_MODE:{mode}"

    # Invariant 9: real OCC contract on a row whose lifecycle_state is still
    # RETRY_WAIT — contract selection succeeded (we have a real symbol) but
    # the lifecycle was never advanced out of the retry-wait state.
    if (contract_upper
            and not contract_upper.startswith("DEFERRED:")
            and len(contract_upper) >= 15          # minimum plausible OCC length
            and lifecycle_state == "RETRY_WAIT"):
        return True, "OCC_CONTRACT_WITH_RETRY_WAIT_LIFECYCLE"

    return False, ""


# ─────────────────────────────────────────────────────────────────────────────
# Strongest retry timestamp resolver
# ─────────────────────────────────────────────────────────────────────────────

def _resolve_retry_timestamp(meta: dict) -> tuple[_dt.datetime | None, str]:
    """Return (parsed_ts, field_name) for the most authoritative retry timestamp.

    Priority must match the merged deferred-lifecycle canonical order:
        1. materialization_next_retry_at  (canonical OSM field — claim_deferred_materialization)
        2. deferred_retry_next_attempt_at (pre-OSM deferred path — stamp_retry_metadata)
        3. next_retry_at                  (legacy adopt / watcher_next_retry_at field)

    This ordering matters when rows carry both legacy and canonical fields with
    different values.  If deferred_retry_next_attempt_at is already overdue but
    next_retry_at is still in the future, choosing next_retry_at would produce
    FUTURE_RETRY_SCHEDULED when DUE_RETRY_PENDING is correct.
    """
    for field in (
        "materialization_next_retry_at",
        "deferred_retry_next_attempt_at",
        "next_retry_at",
    ):
        val = _meta_str(meta, field)
        if val:
            parsed = _parse_iso_utc(val)
            if parsed:
                return parsed, field
    return None, ""


# ─────────────────────────────────────────────────────────────────────────────
# Main classifier
# ─────────────────────────────────────────────────────────────────────────────

def classify_entry_execution_state(
    row: dict,
    now_utc: _dt.datetime | None = None,
) -> dict:
    """Classify a single ENTRY orders row into exactly one execution state.

    Parameters
    ----------
    row:
        A dict corresponding to one row from the ``orders`` table, including
        the ``meta`` JSONB column already decoded to a Python dict.  All keys
        are optional; missing keys are treated as their zero values.
    now_utc:
        Reference wall-clock time (UTC-aware).  Defaults to ``datetime.now``
        when absent.  Pass a fixed value in tests to get deterministic results.

    Returns
    -------
    dict with keys:
        execution_state          – one of the EntryExecutionState constants
        no_broker_id_reason      – human-readable / machine-parseable string;
                                   None when broker_order_id is present
        action_required          – bool; True means operator should investigate
        action_required_reason   – string explanation
        next_expected_transition – string describing the likely next state
        overdue_seconds          – float; None unless DUE_RETRY_PENDING
        state_updated_at         – ISO-8601 string from orders.updated_ts;
                                   None if the column is absent
        diagnostics              – raw meta fields for drill-down
    """
    now = now_utc if now_utc is not None else _utcnow()

    meta: dict = row.get("meta") or {}
    if not isinstance(meta, dict):
        import json as _json
        try:
            meta = _json.loads(meta)
        except Exception:
            meta = {}

    # ── Raw field extraction ────────────────────────────────────────────────
    status        = str(row.get("status") or "").strip().upper()
    contract      = str(row.get("contract") or "").strip()
    broker_order_id = str(row.get("broker_order_id") or "").strip()
    limit_price   = row.get("limit_price")
    execution_mode = str(row.get("execution_mode") or "").strip().lower()
    updated_ts    = row.get("updated_ts")

    broker_ready      = _meta_bool(meta, "broker_ready")
    submit_intent_at  = _meta_str(meta, "submit_intent_at")
    broker_submit_key = _meta_str(meta, "broker_submit_key")
    lifecycle_state   = _meta_str(meta, "lifecycle_state").upper()
    mat_status        = _meta_str(meta, "materialization_status").upper()
    mat_in_flight     = _meta_bool(meta, "materialization_in_flight")
    mat_generation    = meta.get("materialization_generation")
    mat_owner         = _meta_str(meta, "materialization_owner")
    current_owner     = _meta_str(meta, "current_owner")
    retry_reason      = _meta_str(meta, "retry_reason") or _meta_str(meta, "deferred_retry_reason_code")
    retry_attempt     = meta.get("retry_attempt")
    retry_max         = meta.get("retry_max_attempts") or meta.get("deferred_retry_max_attempts")
    retry_deadline    = _meta_str(meta, "retry_deadline") or _meta_str(meta, "absolute_entry_deadline")

    final_mv: dict = meta.get("final_market_validity") or {}
    if not isinstance(final_mv, dict):
        final_mv = {}

    # ── state_updated_at ───────────────────────────────────────────────────
    state_updated_at_raw = updated_ts
    state_updated_at = (
        state_updated_at_raw.isoformat()
        if isinstance(state_updated_at_raw, _dt.datetime)
        else str(state_updated_at_raw or "").strip() or None
    )

    # ── Diagnostics payload (always included for drill-down) ───────────────
    diagnostics = {
        "lifecycle_state":             _meta_str(meta, "lifecycle_state") or None,
        "materialization_status":      _meta_str(meta, "materialization_status") or None,
        "materialization_generation":  mat_generation,
        "materialization_in_flight":   mat_in_flight,
        "materialization_owner":       mat_owner or None,
        "retry_attempt":               retry_attempt,
        "retry_max_attempts":          retry_max,
        "retry_reason":                retry_reason or None,
        "materialization_next_retry_at": _meta_str(meta, "materialization_next_retry_at") or None,
        "deferred_retry_next_attempt_at": _meta_str(meta, "deferred_retry_next_attempt_at") or None,
        "next_retry_at":               _meta_str(meta, "next_retry_at") or None,
        "retry_deadline":              retry_deadline or None,
        "absolute_entry_deadline":     _meta_str(meta, "absolute_entry_deadline") or None,
        "broker_ready":                broker_ready,
        "current_owner":               current_owner or None,
        "watcher_token":               _meta_str(meta, "watcher_token") or None,
        "submit_intent_at":            submit_intent_at or None,
        "broker_submit_key":           broker_submit_key or None,
        "recovery_retention_reason":   _meta_str(meta, "recovery_retention_reason") or None,
        "final_market_validity":       final_mv or None,
    }

    def _result(
        state: str,
        no_bid_reason: str | None,
        action: bool,
        action_reason: str,
        next_transition: str,
        overdue: float | None = None,
    ) -> dict:
        return {
            "execution_state":          state,
            "no_broker_id_reason":      no_bid_reason,
            "action_required":          action,
            "action_required_reason":   action_reason,
            "next_expected_transition": next_transition,
            "overdue_seconds":          overdue,
            "state_updated_at":         state_updated_at,
            "diagnostics":              diagnostics,
        }

    # ── Invariant checks (run before precedence chain) ─────────────────────
    violated, violation_code = _check_entry_invariants(
        status=status,
        contract=contract,
        limit_price=limit_price,
        execution_mode=execution_mode,
        broker_order_id=broker_order_id,
        meta=meta,
    )
    if violated:
        return _result(
            EntryExecutionState.INCONSISTENT_STATE,
            no_bid_reason=violation_code,
            action=True,
            action_reason=f"Invariant violation detected: {violation_code}",
            next_transition="Manual investigation required",
        )

    # ── Precedence chain ───────────────────────────────────────────────────

    # A. broker_order_id present → BROKER_SUBMITTED
    if broker_order_id:
        return _result(
            EntryExecutionState.BROKER_SUBMITTED,
            no_bid_reason=None,
            action=False,
            action_reason="None — order at broker",
            next_transition="FILLED or REJECTED or EXPIRED at broker",
        )

    # B. terminal order status → TERMINAL
    if status in _TERMINAL_ORDER_STATUSES:
        return _result(
            EntryExecutionState.TERMINAL,
            no_bid_reason=status,
            action=False,
            action_reason="None — terminal state",
            next_transition="None",
        )

    # C. submit_intent_at present and broker_order_id absent
    if submit_intent_at:
        # Distinguish RECONCILE_PENDING (reconciler has started) from
        # SUBMIT_INTENT_PENDING (crash window, not yet reconciled).
        # The OSM stamps current_owner = "broker_reconciler:<client_id>" when
        # the reconciler process takes ownership; before that it is
        # "broker_submit:<key>".
        owner_lower = current_owner.lower()
        if owner_lower.startswith("broker_reconciler:"):
            reconcile_reason = (
                _meta_str(meta, "reconcile_reason")
                or broker_submit_key
                or "BROKER_RECONCILIATION_PENDING"
            )
            return _result(
                EntryExecutionState.RECONCILE_PENDING,
                no_bid_reason=reconcile_reason,
                action=True,
                action_reason=(
                    "Broker reconciliation pending — broker truth unavailable "
                    "or ambiguous; manual verification recommended"
                ),
                next_transition=(
                    "BROKER_SUBMITTED if broker confirms order, "
                    "RETRY_WAIT if broker has no matching order"
                ),
            )
        return _result(
            EntryExecutionState.SUBMIT_INTENT_PENDING,
            no_bid_reason="SUBMIT_INTENT_WITHOUT_BROKER_ID",
            action=True,
            action_reason=(
                "Submit intent recorded but no broker_order_id received — "
                "potential crash window; reconciler should resolve on next restart"
            ),
            next_transition=(
                "BROKER_SUBMITTED if broker has order, RETRY_WAIT if not"
            ),
        )

    # D. broker_ready = true → BROKER_READY
    if broker_ready:
        return _result(
            EntryExecutionState.BROKER_READY,
            no_bid_reason="BROKER_READY_AWAITING_SUBMIT",
            action=False,
            action_reason="None — contract selected, awaiting broker submit thread",
            next_transition="BROKER_SUBMITTED after broker POST",
        )

    # E. active materialization → MATERIALIZING
    if lifecycle_state == "MATERIALIZING" or mat_in_flight:
        owner_display = mat_owner or current_owner or "unknown"
        return _result(
            EntryExecutionState.MATERIALIZING,
            no_bid_reason="MATERIALIZATION_IN_PROGRESS",
            action=False,
            action_reason=f"None — materialization in progress (owner={owner_display})",
            next_transition="BROKER_READY on successful contract selection",
        )

    # F. retry-wait lifecycle
    if lifecycle_state == "RETRY_WAIT" or mat_status == "RETRY_PENDING":
        retry_ts, retry_field = _resolve_retry_timestamp(meta)
        if retry_ts is None:
            # Invariant 5 should have caught this, but be defensive.
            return _result(
                EntryExecutionState.INCONSISTENT_STATE,
                no_bid_reason="RETRY_WAIT_WITHOUT_RETRY_TIMESTAMP",
                action=True,
                action_reason="RETRY_WAIT state with no parseable retry timestamp",
                next_transition="Manual investigation required",
            )
        if retry_ts > now:
            # Retry is in the future — healthy scheduled state.
            return _result(
                EntryExecutionState.FUTURE_RETRY_SCHEDULED,
                no_bid_reason=retry_reason or "RETRY_SCHEDULED",
                action=False,
                action_reason="None — retry scheduled",
                next_transition=f"DUE_RETRY_PENDING at {retry_ts.isoformat()}",
            )
        # Retry timestamp is due or overdue.
        overdue_sec = max(0.0, (now - retry_ts).total_seconds())
        return _result(
            EntryExecutionState.DUE_RETRY_PENDING,
            no_bid_reason="RETRY_OVERDUE",
            action=True,
            action_reason=(
                f"Retry is overdue by {overdue_sec:.0f}s — "
                "check watcher/recovery process health"
            ),
            next_transition="MATERIALIZING immediately (retry overdue)",
            overdue=overdue_sec,
        )

    # G. final gate blocked → FINAL_GATE_BLOCKED
    if final_mv:
        fmv_passed = final_mv.get("passed")
        # passed=False (explicit) or any falsy non-None value means blocked.
        if fmv_passed is False or (fmv_passed is not None and not fmv_passed):
            gate_reason = (
                final_mv.get("reason_code")
                or final_mv.get("reason")
                or "FINAL_GATE_BLOCKED"
            )
            return _result(
                EntryExecutionState.FINAL_GATE_BLOCKED,
                no_bid_reason=str(gate_reason),
                action=False,
                action_reason=(
                    f"Final gate blocked and order terminated "
                    f"(reason={gate_reason})"
                ),
                next_transition="TERMINAL (already terminated)",
            )

    # H. status = PENDING_TRIGGER, pre-breach healthy row
    if status == "PENDING_TRIGGER":
        return _result(
            EntryExecutionState.PRE_BREACH,
            no_bid_reason="WAITING_FOR_TRIGGER",
            action=False,
            action_reason="None — waiting for price trigger",
            next_transition="MATERIALIZING on price trigger breach",
        )

    # I. Catch-all — no branch matched
    return _result(
        EntryExecutionState.INCONSISTENT_STATE,
        no_bid_reason=f"UNCLASSIFIABLE:status={status}:lifecycle={lifecycle_state}",
        action=True,
        action_reason=(
            f"Row could not be classified into any known state "
            f"(status={status}, lifecycle={lifecycle_state})"
        ),
        next_transition="Manual investigation required",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Row builders
# ─────────────────────────────────────────────────────────────────────────────

def _trade_queue_row(row: Any) -> dict:
    r = _dict(row)
    payload = r.get("payload") or {}
    if not isinstance(payload, dict):
        payload = {}
    trigger = payload.get("trigger") or {}
    if not isinstance(trigger, dict):
        trigger = {}
    status = str(r.get("status") or "").upper()
    bucket = dashboard_queue_bucket(status)
    return {
        "source": "trade_queue",
        "id": r.get("id"),
        "client_id": r.get("client_id"),
        "signal_id": r.get("signal_id") or payload.get("signal_id"),
        "created_ts": r.get("created_ts"),
        "status": status,
        "dashboard_status": bucket,
        "ticker": payload.get("ticker") or payload.get("symbol"),
        "side": payload.get("side") or payload.get("direction"),
        "score": payload.get("score") or payload.get("ev_score"),
        "trigger_price": payload.get("entry_trigger") or payload.get("trigger_price") or trigger.get("entry"),
        "contract": payload.get("contract_symbol") or payload.get("contract"),
        "last_error": r.get("last_error"),
    }


def _order_row(row: Any) -> dict:
    r = _dict(row)
    meta: dict = r.get("meta") or {}
    if not isinstance(meta, dict):
        import json as _json
        try:
            meta = _json.loads(meta)
        except Exception:
            meta = {}
    meta = meta or {}

    deferred_hydration = meta.get("deferred_hydration") or {}
    if not isinstance(deferred_hydration, dict):
        deferred_hydration = {}

    status = str(r.get("status") or "").upper()
    bucket = dashboard_queue_bucket(status)

    # ── Contract / price display (hide raw DEFERRED placeholder) ──────────
    contract  = r.get("contract") or meta.get("contract_symbol")
    raw_limit = r.get("limit_price")
    contract_selection_status = (
        meta.get("contract_selection_status")
        or deferred_hydration.get("contract_selection_status")
    )
    display_contract = contract
    display_limit    = raw_limit
    if str(contract or "").upper().startswith("DEFERRED:"):
        # P0 amendment: never show raw DEFERRED:<ticker> / $0.01 as a real
        # contract + price. Show pending state; raw values remain in diagnostics.
        display_contract = "pending breach-time selection"
        if raw_limit is None or float(raw_limit or 0) <= 0.01:
            display_limit = "not priced yet"

    # ── Raw diagnostics pulled from meta ──────────────────────────────────
    raw_diagnostics = {
        "local_order_id":             r.get("local_order_id") or r.get("id"),
        "client_id":                  r.get("client_id"),
        "execution_mode":             r.get("execution_mode"),
        "signal_id":                  r.get("signal_id") or meta.get("signal_id"),
        "plan_id":                    r.get("plan_id") or meta.get("plan_id"),
        "status":                     status,
        "broker_order_id":            r.get("broker_order_id"),
        "submitted_ts":               r.get("submitted_ts"),
        "lifecycle_state":            _meta_str(meta, "lifecycle_state") or None,
        "materialization_status":     _meta_str(meta, "materialization_status") or None,
        "materialization_generation": meta.get("materialization_generation"),
        "retry_attempt":              meta.get("retry_attempt"),
        "retry_max_attempts":         meta.get("retry_max_attempts") or meta.get("deferred_retry_max_attempts"),
        "retry_reason":               _meta_str(meta, "retry_reason") or _meta_str(meta, "deferred_retry_reason_code") or None,
        "materialization_next_retry_at":   _meta_str(meta, "materialization_next_retry_at") or None,
        "deferred_retry_next_attempt_at":  _meta_str(meta, "deferred_retry_next_attempt_at") or None,
        "next_retry_at":              _meta_str(meta, "next_retry_at") or None,
        "retry_deadline":             _meta_str(meta, "retry_deadline") or _meta_str(meta, "absolute_entry_deadline") or None,
        "absolute_entry_deadline":    _meta_str(meta, "absolute_entry_deadline") or None,
        "broker_ready":               _meta_bool(meta, "broker_ready"),
        "current_owner":              _meta_str(meta, "current_owner") or None,
        "materialization_owner":      _meta_str(meta, "materialization_owner") or None,
        "watcher_token":              _meta_str(meta, "watcher_token") or None,
        "submit_intent_at":           _meta_str(meta, "submit_intent_at") or None,
        "broker_submit_key":          _meta_str(meta, "broker_submit_key") or None,
        "recovery_retention_reason":  _meta_str(meta, "recovery_retention_reason") or None,
        "final_market_validity":      meta.get("final_market_validity"),
        "last_error":                 r.get("last_error"),
        "updated_ts":                 r.get("updated_ts"),
    }

    # ── Execution-state classification ────────────────────────────────────
    # Build the row dict classify_entry_execution_state expects.
    classify_input = dict(r)
    classify_input["meta"] = meta
    execution_classification = classify_entry_execution_state(classify_input)

    return {
        "source":           "orders",
        "id":               r.get("local_order_id") or r.get("id"),
        "client_id":        r.get("client_id"),
        "signal_id":        r.get("signal_id") or meta.get("signal_id"),
        "plan_id":          r.get("plan_id") or meta.get("plan_id"),
        "created_ts":       r.get("created_ts"),
        "status":           status,
        "dashboard_status": bucket,
        "ticker":           r.get("symbol") or meta.get("ticker") or meta.get("symbol"),
        "side":             r.get("side") or meta.get("side") or meta.get("direction"),
        "score":            r.get("score") or meta.get("score"),
        "trigger_price":    meta.get("trigger_price") or meta.get("entry_trigger"),
        "contract":         contract,
        "display_contract": display_contract,
        "limit_price":      raw_limit,
        "display_limit_price":        display_limit,
        "last_hydration_attempt":     deferred_hydration.get("last_attempt_at"),
        "hydration_failure_reason":   deferred_hydration.get("failure_reason"),
        "hydration_retryability":     contract_selection_status,
        "contract_selection_status":  contract_selection_status,
        "last_error":                 r.get("last_error"),
        # ── Classification fields (every ENTRY row) ────────────────────────
        "execution_state":            execution_classification["execution_state"],
        "no_broker_id_reason":        execution_classification["no_broker_id_reason"],
        "action_required":            execution_classification["action_required"],
        "action_required_reason":     execution_classification["action_required_reason"],
        "next_expected_transition":   execution_classification["next_expected_transition"],
        "overdue_seconds":            execution_classification["overdue_seconds"],
        "state_updated_at":           execution_classification["state_updated_at"],
        # ── Raw drill-down diagnostics ─────────────────────────────────────
        "raw_diagnostics":            raw_diagnostics,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Public read-model builder
# ─────────────────────────────────────────────────────────────────────────────

def build_operator_queue_read_model(
    *,
    client_id: str | None = None,
    hours: int = 24,
    limit: int = 200,
) -> dict:
    from ap.db import conn, run_with_retry

    hours = max(1, min(int(hours or 24), 168))
    limit = max(1, min(int(limit or 200), 1000))

    tq_where    = ["created_ts >= NOW() - (%s::text || ' hours')::interval"]
    tq_params: list[Any] = [str(hours)]
    order_where = ["created_ts >= NOW() - (%s::text || ' hours')::interval"]
    order_params: list[Any] = [str(hours)]

    if client_id:
        tq_where.append("client_id = %s")
        tq_params.append(client_id)
        order_where.append("client_id = %s")
        order_params.append(client_id)

    tq_params.append(limit)
    order_params.append(limit)

    # IMPORTANT: trade_queue has no updated_ts column.
    tq_sql = (
        "SELECT id, client_id, signal_id, status, created_ts, started_ts, finished_ts, payload, last_error "
        "FROM trade_queue "
        "WHERE " + " AND ".join(tq_where) + " "
        "ORDER BY created_ts DESC LIMIT %s"
    )

    # Extended to include submitted_ts, execution_mode, and plan_id so the
    # execution-state classifier has all raw fields it needs.
    order_sql = (
        "SELECT local_order_id, client_id, kind, status, broker_order_id, symbol, side, "
        "       contract, limit_price, score, signal_id, plan_id, execution_mode, "
        "       submitted_ts, created_ts, updated_ts, last_error, meta "
        "FROM orders "
        "WHERE " + " AND ".join(order_where) + " "
        "  AND kind = 'ENTRY' "
        "  AND UPPER(COALESCE(status,'')) IN "
        "      ('PENDING_TRIGGER','SUBMITTED','ACK','ACKNOWLEDGED','WORKING','REJECTED','EXPIRED') "
        "ORDER BY updated_ts DESC LIMIT %s"
    )

    def _fetch():
        with conn() as c:
            return (
                c.execute(tq_sql, tuple(tq_params)).fetchall(),
                c.execute(order_sql, tuple(order_params)).fetchall(),
            )

    trade_queue_rows, order_rows = run_with_retry(_fetch)

    rows   = [_order_row(r) for r in (order_rows or [])] + \
             [_trade_queue_row(r) for r in (trade_queue_rows or [])]
    counts = empty_queue_counts()
    for row in rows:
        bucket = row.get("dashboard_status")
        if bucket in counts:
            counts[bucket] += 1

    active = counts["NEW"] + counts["WATCHING"] + counts["TRIGGERED"]
    return {
        "ok":                   True,
        "client_id":            client_id,
        "hours":                hours,
        "counts":               counts,
        "NEW":                  counts["NEW"],
        "WATCHING":             counts["WATCHING"],
        "TRIGGERED":            counts["TRIGGERED"],
        "REJECTED":             counts["REJECTED"],
        "EXPIRED":              counts["EXPIRED"],
        "active_queue_signals": active,
        "pending_signals":      active,
        "rows":                 rows[:limit],
    }
