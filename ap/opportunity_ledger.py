"""
ap/opportunity_ledger.py
========================
PR1 — Client Opportunity Ledger
PR81 FINAL AMENDMENT — non-regressing lifecycle ledger.

Creates and updates one row per active approved client per CLIENT_ELIGIBLE
signal in the client_signal_opportunities table.

Design:
- All writes are fail-safe (wrapped try/except) — never blocks execution.
- Primary idempotency key: (canonical_signal_id, client_id).
- create_opportunities() uses insert-with-ignore-duplicates so it can NEVER
  regress a row that has already progressed to PREFLIGHT_*, ORDER_CREATED,
  WATCHER_ARMED, BROKER_*, FILLED, MISSED, CLIENT_SKIPPED, or INTERNAL_ERROR.
- update_opportunity() preserves and merges metadata; the preflight snapshot
  written at PREFLIGHT_* survives later ORDER_CREATED / FILLED writes.
- All ledger write failures emit CLIENT_OPPORTUNITY_LEDGER_WRITE_FAILED.

Usage:
    from ap.opportunity_ledger import create_opportunities, update_opportunity

Migration:
    Run migrations/opportunity_ledger.sql before deploying.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

log = logging.getLogger("ap.opportunity_ledger")

# ── Status constants ──────────────────────────────────────────────────────────
CREATED                   = "CREATED"
CLIENT_ELIGIBLE           = "CLIENT_ELIGIBLE"
PREFLIGHT_PASSED          = "PREFLIGHT_PASSED"    # cleared preflight, continuing to order creation
PREFLIGHT_WARNING         = "PREFLIGHT_WARNING"   # preflight detected issue but enforce=false, execution continued
CLIENT_SKIPPED            = "CLIENT_SKIPPED"      # preflight blocked (enforce=true only)
ORDER_CREATED             = "ORDER_CREATED"       # written ONLY after create_entry_order() returns valid local_order_id
WATCHER_ARMED             = "WATCHER_ARMED"
WATCHER_INVALIDATED       = "WATCHER_INVALIDATED"
ENTRY_CONFIRMATION_FAILED = "ENTRY_CONFIRMATION_FAILED"
BROKER_SUBMITTED          = "BROKER_SUBMITTED"
BROKER_ACKED              = "BROKER_ACKED"
BROKER_REJECTED           = "BROKER_REJECTED"
FILLED                    = "FILLED"
EXPIRED                   = "EXPIRED"
CANCELED                  = "CANCELED"
MISSED                    = "MISSED"
INTERNAL_ERROR            = "INTERNAL_ERROR"
# NOTE: RETRY_* statuses are reserved for a future retry evaluator PR.
# PR81 must not create or consume these statuses automatically.
RETRY_ELIGIBLE            = "RETRY_ELIGIBLE"   # reserved — not written by PR81

# Statuses that must never be overwritten back to CREATED by repeated fanout.
# Used by create_opportunities() to guard against regression.
PROGRESSED_STATUSES = frozenset({
    PREFLIGHT_PASSED, PREFLIGHT_WARNING, CLIENT_SKIPPED,
    ORDER_CREATED, WATCHER_ARMED, WATCHER_INVALIDATED,
    ENTRY_CONFIRMATION_FAILED,
    BROKER_SUBMITTED, BROKER_ACKED, BROKER_REJECTED,
    FILLED, EXPIRED, CANCELED, MISSED, INTERNAL_ERROR,
})

TERMINAL_STATUSES = frozenset({
    FILLED, EXPIRED, CANCELED, MISSED, CLIENT_SKIPPED,
    BROKER_REJECTED, INTERNAL_ERROR,
    WATCHER_INVALIDATED, ENTRY_CONFIRMATION_FAILED,
})

# ── Monotonic status ranking (Final Amendment v2 §1) ────────────────────────
# update_opportunity() must NEVER regress a row to an earlier lifecycle state.
# Every status has a rank; an incoming status with a lower rank than the
# current status is rejected (status update skipped) and logged as
# CLIENT_OPPORTUNITY_STATUS_REGRESSION_BLOCKED. Metadata and identifier
# fields may still be enriched in the same write — only the status field is
# preserved.
#
# All terminal outcomes share rank 100 so one terminal cannot overwrite a
# different terminal already set (first-truth wins for terminals).
STATUS_RANK: dict[str, int] = {
    CREATED:                    10,
    CLIENT_ELIGIBLE:            20,
    PREFLIGHT_WARNING:          30,
    PREFLIGHT_PASSED:           40,
    ORDER_CREATED:              50,
    WATCHER_ARMED:              60,
    BROKER_SUBMITTED:           70,
    BROKER_ACKED:               80,
    FILLED:                    100,
    WATCHER_INVALIDATED:       100,
    ENTRY_CONFIRMATION_FAILED: 100,
    BROKER_REJECTED:           100,
    EXPIRED:                   100,
    CANCELED:                  100,
    MISSED:                    100,
    CLIENT_SKIPPED:            100,
    INTERNAL_ERROR:            100,
}


def status_rank(status: Optional[str]) -> int:
    """Return the monotonic rank for a status string. Unknown statuses get
    rank 0 so any known status will replace them."""
    if not status:
        return 0
    return STATUS_RANK.get(str(status), 0)


def _is_terminal(status: Optional[str]) -> bool:
    return bool(status) and str(status) in TERMINAL_STATUSES

# ── Miss stage constants (Amendment §4) ──────────────────────────────────────
STAGE_CLIENT_PREFLIGHT    = "CLIENT_PREFLIGHT"
STAGE_ORDER_CREATION      = "ORDER_CREATION"
STAGE_WATCHER_ARM         = "WATCHER_ARM"
STAGE_ENTRY_CONFIRMATION  = "ENTRY_CONFIRMATION"
STAGE_BROKER_SUBMIT       = "BROKER_SUBMIT"
STAGE_BROKER_ACK          = "BROKER_ACK"
STAGE_FILL_MONITOR        = "FILL_MONITOR"
STAGE_FILL_INTEGRITY      = "FILL_INTEGRITY"
STAGE_CAP_BLOCK           = "CAP_BLOCK"
STAGE_DUPLICATE_SYMBOL    = "DUPLICATE_SYMBOL"
STAGE_INTERNAL_ERROR      = "INTERNAL_ERROR"
STAGE_UNKNOWN             = "UNKNOWN"


# ── Reason → stage mapping (Amendment §4) ────────────────────────────────────
# Order matters: more specific keywords first.
_REASON_STAGE_MAP: tuple[tuple[tuple[str, ...], str], ...] = (
    # Cap / capital
    (("daily_stop", "sizer_blocked", "cap_block", "capital", "capital_cap",
      "max_position", "buying_power"), STAGE_CAP_BLOCK),
    # Duplicate / same symbol
    (("duplicate_symbol", "duplicate", "same_symbol", "opposite_side"),
     STAGE_DUPLICATE_SYMBOL),
    # Client state (kill switch, paused, approved, subscription, credentials, mode)
    (("kill_switch", "killswitch", "client_killswitch",
      "entries_paused", "entriespaused", "global_entriespaused",
      "subscription", "not_approved", "client_not_approved",
      "missing_broker_credentials", "broker_mode_mismatch",
      "maintenance_mode", "scanner_disabled", "client_preflight"),
     STAGE_CLIENT_PREFLIGHT),
    # Watcher
    (("watcher", "watch_arm", "arm_failed", "stale_signal", "dedup_block"),
     STAGE_WATCHER_ARM),
    # Entry confirmation
    (("confirmation", "confirmation_failed", "entry_confirmation"),
     STAGE_ENTRY_CONFIRMATION),
    # Broker submit / ack / reject
    (("submit_error", "broker_submit", "submit_failed"),
     STAGE_BROKER_SUBMIT),
    (("broker_reject", "broker_rejected", "broker_ack", "ack_failed"),
     STAGE_BROKER_ACK),
    # Fill monitor outcomes
    (("expired", "canceled", "cancelled", "no_fill", "stale", "fill_monitor"),
     STAGE_FILL_MONITOR),
    # Fill integrity
    (("unproven", "fill_integrity", "integrity"),
     STAGE_FILL_INTEGRITY),
    # Order creation
    (("order_create", "create_entry_order", "order_creation"),
     STAGE_ORDER_CREATION),
)


def map_reason_to_stage(reason: Optional[str]) -> str:
    """Map a free-form reason string to a canonical miss stage.

    Amendment §4: do not write every master_control rejection as UNKNOWN.
    Returns STAGE_UNKNOWN only when nothing matches.
    """
    if not reason:
        return STAGE_UNKNOWN
    r = str(reason).strip().lower()
    if not r:
        return STAGE_UNKNOWN
    for keywords, stage in _REASON_STAGE_MAP:
        for kw in keywords:
            if kw in r:
                return stage
    return STAGE_UNKNOWN


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolve_canonical(canonical_signal_id: Optional[str],
                        signal_id: str) -> str:
    """
    Return the canonical_signal_id for idempotency key.
    Falls back to signal_id if canonical is unavailable.
    REEVAL suffixes must NOT fragment opportunity rows — callers should
    pass the base canonical_signal_id, not the suffixed variant.
    """
    return (canonical_signal_id or signal_id or "").strip() or signal_id


def _get_sb():
    """Get the Supabase client from the queue module's shared client."""
    try:
        from ap.queue import _get_sb_client
        return _get_sb_client()
    except Exception:
        return None


def _emit_write_failed(stage: str, *, client_id: str, signal_id: str,
                       canonical: str, status: str = "", err: Any = "") -> None:
    """Standard CLIENT_OPPORTUNITY_LEDGER_WRITE_FAILED log emission."""
    log.warning(
        "CLIENT_OPPORTUNITY_LEDGER_WRITE_FAILED | "
        "stage=%s status=%s client=%s signal=%s canonical=%s err=%s",
        stage, status, client_id, signal_id, canonical, err,
    )


# ── Public API ────────────────────────────────────────────────────────────────

def create_opportunities(
    signal_id: str,
    client_ids: Iterable[str],
    payload: dict[str, Any],
    canonical_signal_id: Optional[str] = None,
    sb=None,
) -> int:
    """
    Create one opportunity row per client for this signal.
    Called from route_signal_to_all_clients() before fanout.

    Amendment §2: NEVER regresses an existing row. Uses
    ignore_duplicates=True so progressed rows (PREFLIGHT_*, ORDER_CREATED,
    WATCHER_ARMED, BROKER_*, FILLED, MISSED, CLIENT_SKIPPED, INTERNAL_ERROR)
    are preserved across repeated fanout / worker restart / REEVAL.

    Returns number of insert attempts that succeeded (no error). A row that
    was preserved by ignore_duplicates is counted as a successful no-op.
    """
    client_ids = list(client_ids or [])
    if not client_ids or not signal_id:
        return 0

    sb = sb or _get_sb()
    if not sb:
        log.debug("opportunity_ledger.create_opportunities: no sb client")
        return 0

    now     = _now()
    ticker  = str(payload.get("ticker") or payload.get("symbol") or "")
    created = 0

    canonical = _resolve_canonical(canonical_signal_id, signal_id)

    for client_id in client_ids:
        try:
            row = {
                "signal_id":              signal_id,
                "canonical_signal_id":    canonical,
                "client_id":              client_id,
                "symbol":                 ticker,
                "direction":              str(payload.get("direction") or payload.get("side") or ""),
                "side":                   str(payload.get("side") or payload.get("direction") or ""),
                "timeframe":              str(payload.get("timeframe") or ""),
                "pattern":                str(payload.get("pattern") or payload.get("pattern_id") or ""),
                "score":                  float(payload.get("score") or payload.get("ev_score") or 0),
                "tier":                   str(payload.get("tier") or ""),
                "scanner_type":           str(payload.get("scanner_type") or ""),
                "scanner_name":           str(payload.get("scanner_name") or ""),
                "signal_created_at":      str(payload.get("created_at") or payload.get("timestamp_iso") or now),
                "client_eligibility_status": "CLIENT_ELIGIBLE",
                "opportunity_status":     CREATED,
                "created_at":             now,
                "updated_at":             now,
                "metadata":               {
                    "trigger_price":  payload.get("trigger_price") or payload.get("entry_price"),
                    "stop_price":     payload.get("stop_price"),
                    "target_price":   payload.get("target_price"),
                },
            }
            # Amendment §2: ignore_duplicates means "INSERT ... ON CONFLICT
            # DO NOTHING" semantics — never overwrites a progressed row.
            try:
                (
                    sb.table("client_signal_opportunities")
                    .upsert(
                        row,
                        on_conflict="canonical_signal_id,client_id",
                        ignore_duplicates=True,
                    )
                    .execute()
                )
            except TypeError:
                # Older supabase-py without ignore_duplicates — fall back to
                # a guarded read-then-insert. Still safe: we explicitly
                # refuse to write if a row already exists.
                existing = (
                    sb.table("client_signal_opportunities")
                    .select("id,opportunity_status")
                    .eq("canonical_signal_id", canonical)
                    .eq("client_id", client_id)
                    .limit(1)
                    .execute()
                    .data
                    or []
                )
                if not existing:
                    sb.table("client_signal_opportunities").insert(row).execute()
                else:
                    log.debug(
                        "opportunity_ledger: preserved progressed row "
                        "client=%s canonical=%s existing_status=%s",
                        client_id, canonical,
                        (existing[0] or {}).get("opportunity_status"),
                    )
            created += 1
        except Exception as e:
            _emit_write_failed(
                "create_opportunities",
                client_id=client_id, signal_id=signal_id,
                canonical=canonical, status=CREATED, err=e,
            )

    log.debug("opportunity_ledger: insert-attempts ok %d/%d for signal=%s canonical=%s",
              created, len(client_ids), signal_id, canonical)
    return created


def update_opportunity(
    signal_id: str,
    client_id: str,
    status: str,
    *,
    canonical_signal_id: Optional[str] = None,
    miss_stage: Optional[str] = None,
    miss_reason: Optional[str] = None,
    order_local_id: Optional[str] = None,
    broker_order_id: Optional[str] = None,
    position_id: Optional[str] = None,
    quote_age_seconds: Optional[float] = None,
    spread_pct: Optional[float] = None,
    buying_power_snapshot: Optional[float] = None,
    cap_snapshot: Optional[dict] = None,
    kill_switch_state: Optional[bool] = None,
    entries_paused_state: Optional[bool] = None,
    entry_confirmation_result: Optional[str] = None,
    # Amendment §6: enforcement context
    preflight_enforced: Optional[bool] = None,    # True=enforce blocked; False=observe only
    execution_continued: Optional[bool] = None,   # True=trade was allowed despite preflight warning
    would_block_reason: Optional[str] = None,     # block reason when observe mode didn't stop execution
    retry_status: Optional[str] = None,
    retry_reason: Optional[str] = None,
    extra_meta: Optional[dict] = None,
    sb=None,
) -> bool:
    """
    Update an opportunity row's status and optional miss/fill metadata.

    Fail-safe — never raises; returns True if write succeeded.
    Logs CLIENT_OPPORTUNITY_LEDGER_WRITE_FAILED on failure (Amendment §7).

    Metadata is merged into the existing JSONB column (read-modify-write)
    so earlier snapshots (e.g. the preflight snapshot) are preserved when
    later writes (ORDER_CREATED, FILLED) carry their own extras.
    """
    if not signal_id or not client_id:
        return False

    sb = sb or _get_sb()
    if not sb:
        return False

    now    = _now()
    canonical = _resolve_canonical(canonical_signal_id, signal_id)

    # ── Monotonic guard (Final Amendment v2 §1) ─────────────────────────
    # Read the current row's status. If incoming would regress, drop the
    # status field from the patch (keep enrichment fields). Terminal
    # statuses are never overwritten by a different status.
    _current_status: Optional[str] = None
    try:
        existing = (
            sb.table("client_signal_opportunities")
            .select("opportunity_status")
            .eq("canonical_signal_id", canonical)
            .eq("client_id", client_id)
            .limit(1)
            .execute()
            .data
            or []
        )
        if existing:
            _current_status = (existing[0] or {}).get("opportunity_status")
    except Exception:
        # Non-fatal: if the read fails we fall through and let the update
        # attempt log its own write failure. We refuse to silently regress
        # though — unknown current status keeps the incoming write as-is.
        _current_status = None

    _incoming_rank = status_rank(status)
    _current_rank  = status_rank(_current_status)
    _write_status  = True

    if _current_status:
        # Terminal cannot be replaced by a different status (even another
        # terminal). FILLED stays FILLED, MISSED stays MISSED.
        if _is_terminal(_current_status) and _current_status != status:
            _write_status = False
        # Or lower-ranked incoming.
        elif _incoming_rank < _current_rank:
            _write_status = False

    if not _write_status:
        log.info(
            "CLIENT_OPPORTUNITY_STATUS_REGRESSION_BLOCKED | "
            "client=%s signal=%s canonical=%s current=%s incoming=%s",
            client_id, signal_id, canonical, _current_status, status,
        )

    patch: dict[str, Any] = {"updated_at": now}
    if _write_status:
        patch["opportunity_status"] = status

    if miss_stage              is not None: patch["miss_stage"]               = miss_stage
    if miss_reason             is not None: patch["miss_reason"]              = miss_reason
    if order_local_id          is not None: patch["order_local_id"]           = order_local_id
    if broker_order_id         is not None: patch["broker_order_id"]          = broker_order_id
    if position_id             is not None: patch["position_id"]              = position_id
    if quote_age_seconds       is not None: patch["quote_age_seconds"]        = quote_age_seconds
    if spread_pct              is not None: patch["spread_pct"]               = spread_pct
    if buying_power_snapshot   is not None: patch["buying_power_snapshot"]    = buying_power_snapshot
    if kill_switch_state       is not None: patch["kill_switch_state"]        = kill_switch_state
    if entries_paused_state    is not None: patch["entries_paused_state"]     = entries_paused_state
    if entry_confirmation_result is not None: patch["entry_confirmation_result"] = entry_confirmation_result
    if preflight_enforced      is not None: patch["preflight_enforced"]       = preflight_enforced
    if execution_continued     is not None: patch["execution_continued"]      = execution_continued
    if would_block_reason      is not None: patch["would_block_reason"]       = would_block_reason
    if retry_status            is not None: patch["retry_status"]             = retry_status
    if retry_reason            is not None: patch["retry_reason"]             = retry_reason

    # Metadata merge — read existing first so we don't clobber the preflight
    # snapshot when a later ORDER_CREATED / FILLED write adds new keys.
    if cap_snapshot or extra_meta:
        merged: dict[str, Any] = {}
        try:
            existing = (
                sb.table("client_signal_opportunities")
                .select("metadata")
                .eq("canonical_signal_id", canonical)
                .eq("client_id", client_id)
                .limit(1)
                .execute()
                .data
                or []
            )
            if existing and isinstance(existing[0].get("metadata"), dict):
                merged.update(existing[0]["metadata"])
        except Exception:
            # Non-fatal — fall through with empty base.
            pass
        if cap_snapshot:
            merged["cap_snapshot"] = cap_snapshot
        if extra_meta:
            merged.update(extra_meta)
        patch["metadata"] = merged

    try:
        sb.table("client_signal_opportunities").update(patch).eq(
            "canonical_signal_id", canonical
        ).eq("client_id", client_id).execute()
        return True
    except Exception as e:
        _emit_write_failed(
            "update_opportunity",
            client_id=client_id, signal_id=signal_id,
            canonical=canonical, status=status, err=e,
        )
        return False


def mark_skipped(
    signal_id: str,
    client_id: str,
    miss_stage: str,
    miss_reason: str,
    *,
    canonical_signal_id: Optional[str] = None,
    **kwargs,
) -> bool:
    """Convenience: mark a client as skipped before order creation."""
    return update_opportunity(
        signal_id, client_id, CLIENT_SKIPPED,
        canonical_signal_id=canonical_signal_id,
        miss_stage=miss_stage, miss_reason=miss_reason, **kwargs
    )


def mark_missed(
    signal_id: str,
    client_id: str,
    miss_stage: str,
    miss_reason: str,
    *,
    canonical_signal_id: Optional[str] = None,
    **kwargs,
) -> bool:
    """Convenience: mark a client as terminally missed."""
    return update_opportunity(
        signal_id, client_id, MISSED,
        canonical_signal_id=canonical_signal_id,
        miss_stage=miss_stage, miss_reason=miss_reason, **kwargs
    )


def mark_internal_error(
    signal_id: str,
    client_id: str,
    miss_reason: str,
    *,
    canonical_signal_id: Optional[str] = None,
    **kwargs,
) -> bool:
    """Convenience: mark INTERNAL_ERROR for an unhandled exception path."""
    return update_opportunity(
        signal_id, client_id, INTERNAL_ERROR,
        canonical_signal_id=canonical_signal_id,
        miss_stage=STAGE_INTERNAL_ERROR, miss_reason=miss_reason, **kwargs
    )


# ── Lifecycle hook helpers (Amendment §3) ────────────────────────────────────
# These are thin wrappers other modules (OSM, monitor, watcher) can call
# without each site re-importing constants.

def mark_watcher_armed(signal_id: str, client_id: str, *,
                       canonical_signal_id: Optional[str] = None,
                       order_local_id: Optional[str] = None, **kwargs) -> bool:
    return update_opportunity(
        signal_id, client_id, WATCHER_ARMED,
        canonical_signal_id=canonical_signal_id,
        order_local_id=order_local_id, **kwargs
    )


def mark_watcher_invalidated(signal_id: str, client_id: str, reason: str, *,
                              canonical_signal_id: Optional[str] = None,
                              order_local_id: Optional[str] = None, **kwargs) -> bool:
    return update_opportunity(
        signal_id, client_id, MISSED,
        canonical_signal_id=canonical_signal_id,
        miss_stage=STAGE_WATCHER_ARM, miss_reason=reason,
        order_local_id=order_local_id, **kwargs
    )


def mark_broker_submitted(signal_id: str, client_id: str, *,
                          canonical_signal_id: Optional[str] = None,
                          order_local_id: Optional[str] = None,
                          broker_order_id: Optional[str] = None, **kwargs) -> bool:
    return update_opportunity(
        signal_id, client_id, BROKER_SUBMITTED,
        canonical_signal_id=canonical_signal_id,
        order_local_id=order_local_id, broker_order_id=broker_order_id, **kwargs
    )


def mark_filled(signal_id: str, client_id: str, *,
                canonical_signal_id: Optional[str] = None,
                order_local_id: Optional[str] = None,
                broker_order_id: Optional[str] = None,
                position_id: Optional[str] = None, **kwargs) -> bool:
    return update_opportunity(
        signal_id, client_id, FILLED,
        canonical_signal_id=canonical_signal_id,
        order_local_id=order_local_id, broker_order_id=broker_order_id,
        position_id=position_id, **kwargs
    )
