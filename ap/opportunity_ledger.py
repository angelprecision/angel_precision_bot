"""
ap/opportunity_ledger.py
========================
PR1 — Client Opportunity Ledger

Creates and updates one row per active approved client per CLIENT_ELIGIBLE
signal in the client_signal_opportunities table.

Design:
- All writes are fail-safe (wrapped try/except) — never blocks execution
- Idempotent via upsert on (signal_id, client_id)
- Stateless — safe to call from any thread

Usage:
    from ap.opportunity_ledger import create_opportunities, update_opportunity

Migration:
    Run migrations/opportunity_ledger.sql before deploying.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

log = logging.getLogger("ap.opportunity_ledger")

# ── Status constants ──────────────────────────────────────────────────────────
CREATED            = "CREATED"
CLIENT_SKIPPED     = "CLIENT_SKIPPED"
ORDER_CREATED      = "ORDER_CREATED"
WATCHER_ARMED      = "WATCHER_ARMED"
BROKER_SUBMITTED   = "BROKER_SUBMITTED"
FILLED             = "FILLED"
PREFLIGHT_PASSED   = "PREFLIGHT_PASSED"  # client cleared preflight, order about to be created
MISSED             = "MISSED"
RETRY_ELIGIBLE     = "RETRY_ELIGIBLE"
RETRY_SUBMITTED    = "RETRY_SUBMITTED"
RETRY_FILLED       = "RETRY_FILLED"
RETRY_BLOCKED      = "RETRY_BLOCKED"

# ── Miss stage constants ──────────────────────────────────────────────────────
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


# ── Public API ────────────────────────────────────────────────────────────────

def create_opportunities(
    signal_id: str,
    client_ids: list[str],
    payload: dict[str, Any],
    canonical_signal_id: Optional[str] = None,
    sb=None,
) -> int:
    """
    Create one opportunity row per client for this signal.
    Called from route_signal_to_all_clients() before fanout.
    Idempotent — duplicate calls produce no duplicate rows.
    Returns number of rows successfully written.
    """
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
            sb.table("client_signal_opportunities").upsert(
                {
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
                },
                # Primary idempotency key: canonical_signal_id + client_id
                # Prevents REEVAL suffix variants from duplicating rows.
                on_conflict="canonical_signal_id,client_id",
            ).execute()
            created += 1
        except Exception as e:
            log.warning(
                "opportunity_ledger.create_opportunities failed | client=%s signal=%s err=%s",
                client_id, signal_id, e,
            )

    log.debug("opportunity_ledger: created %d/%d rows for signal=%s", created, len(client_ids), signal_id)
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
    retry_status: Optional[str] = None,
    retry_reason: Optional[str] = None,
    extra_meta: Optional[dict] = None,
    sb=None,
) -> bool:
    """
    Update an opportunity row's status and optional miss/fill metadata.
    Fail-safe — never raises; returns True if write succeeded.
    """
    if not signal_id or not client_id:
        return False

    sb = sb or _get_sb()
    if not sb:
        return False

    now    = _now()
    patch  = {"opportunity_status": status, "updated_at": now}

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
    if retry_status            is not None: patch["retry_status"]             = retry_status
    if retry_reason            is not None: patch["retry_reason"]             = retry_reason

    if cap_snapshot or extra_meta:
        patch["metadata"] = {}
        if cap_snapshot:  patch["metadata"]["cap_snapshot"]  = cap_snapshot
        if extra_meta:    patch["metadata"].update(extra_meta)

    canonical = _resolve_canonical(canonical_signal_id, signal_id)
    try:
        sb.table("client_signal_opportunities").update(patch).eq(
            "canonical_signal_id", canonical
        ).eq("client_id", client_id).execute()
        return True
    except Exception as e:
        log.warning(
            "opportunity_ledger.update_opportunity failed | client=%s signal=%s status=%s err=%s",
            client_id, signal_id, status, e,
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
