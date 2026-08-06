"""
ap/deferred_materializer.py — Deferred contract materialization lifecycle.

WHY THIS MODULE EXISTS
───────────────────────
A deferred `PENDING_TRIGGER` order (contract=DEFERRED:<TICKER>, limit_price=0.01)
is a SETUP waiting for two independent events:

  1. TRIGGER   — underlying price breaches the entry level  (owned by watcher)
  2. MATERIALIZE — real OCC contract selected from the live chain  (owned here)

Before this module, both happened inline in _on_entry_trigger, making the
lifecycle invisible to operators. When materialization failed, the row expired
silently with no indication of which stage failed, how many contracts were
scanned, or whether the failure was transient (chain warmup) vs structural
(OI_TOO_LOW, UNTRADEABLE).

LIFECYCLE (stored in orders.meta, never in the status column to preserve
dashboard queries that filter on the existing PENDING_TRIGGER status):

  meta.materialization_status:
    WAITING_FOR_TRIGGER  — initial; watcher is watching price only
    QUEUED               — price triggered; materializer not yet claimed
    RUNNING              — materializer claimed; selector is executing
    RETRY_PENDING        — transient failure; retry scheduled
    SELECTED             — real OCC selected; row is broker-ready
    FAILED_TERMINAL      — permanent failure; no further attempts

  meta.broker_ready:
    false   — always false until SELECTED; DEFERRED:*/0.01 never broker-ready
    true    — only after SELECTED with real OCC, limit>0.01, qty>=1

HARD INVARIANT
──────────────
Legacy callers set meta.broker_ready through this module's `stamp_selected()`
helper. The production breach path now uses the OSM atomic copyback CAS so the
contract, price, quantity, reserved cost, and broker_ready flag commit together.
Execution core checks readiness at pre-submit: if false, broker POST is blocked.

DESIGN NOTES
────────────
• All OSM writes use `update_order_meta()` (JSONB merge — non-destructive).
• Claims use a lock TTL so stuck rows are retriable after expiry.
• Idempotent: if order already has broker_ready=true, stamp_selected is a no-op.
• Never creates new DB tables or columns.
• All helpers are best-effort (never raise) — materialization must not crash
  the caller on unexpected input shapes.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, Any

from ap.logger import get_logger

log = get_logger("ap.deferred_materializer")

# ── Lifecycle state constants ─────────────────────────────────────────────────

WAITING_FOR_TRIGGER = "WAITING_FOR_TRIGGER"
QUEUED              = "QUEUED"
RUNNING             = "RUNNING"
RETRY_PENDING       = "RETRY_PENDING"
SELECTED            = "SELECTED"
FAILED_TERMINAL     = "FAILED_TERMINAL"

# ── Seam 3 (PR #323): canonical retry taxonomy — single source of truth ────────
# RETRYABLE_MATERIALIZATION_REASONS is now derived from the same policy table as
# RETRYABLE_BREACH_SELECTOR_REASONS.  They are guaranteed to be identical.
# Import from canonical module; the old inline frozenset is removed.
from ap.selector_retry_policy import (  # noqa: E402
    RETRYABLE_MATERIALIZATION_REASONS,
    RETRYABLE_BREACH_SELECTOR_REASONS as _RETRYABLE_BREACH_SELECTOR_REASONS_DM,  # noqa: F401
    resolve_deferred_materialization_max_attempts,
)


# ── Config helpers (hot-read from env; no restart needed for tuning) ──────────

def _cfg() -> dict:
    """Read materializer config from env.

    Raises DeferredMaterializationConfigConflict (from
    resolve_deferred_materialization_max_attempts) if
    MAX_BREACH_SELECTOR_RETRIES and DEFERRED_MATERIALIZATION_MAX_ATTEMPTS
    are both explicitly set to conflicting or malformed values. No local
    fallback is substituted -- per the audit requirement, no consumer of
    the canonical resolver may convert an invalid configuration into
    usable runtime policy. Callers must handle the conflict explicitly.
    """
    def _int(key: str, default: int) -> int:
        try:
            return max(1, int(os.getenv(key, str(default)).strip()))
        except (TypeError, ValueError):
            return default

    def _bool(key: str, default: bool) -> bool:
        return os.getenv(key, "1" if default else "0").strip().lower() in ("1", "true", "yes")

    def _hhmm(key: str, default: str) -> str:
        val = os.getenv(key, default).strip()
        try:
            h, m = val.split(":")
            if 0 <= int(h) <= 23 and 0 <= int(m) <= 59:
                return val
        except Exception:
            pass
        return default

    return {
        "enabled":           _bool("DEFERRED_MATERIALIZATION_BUCKET_ENABLED", True),
        "max_attempts":      resolve_deferred_materialization_max_attempts(),
        "retry_base_s":      _int("DEFERRED_MATERIALIZATION_RETRY_BASE_SECONDS", 15),
        "retry_max_s":       _int("DEFERRED_MATERIALIZATION_RETRY_MAX_SECONDS", 90),
        "lock_ttl_s":        _int("DEFERRED_MATERIALIZATION_LOCK_TTL_SECONDS", 120),
        "entry_cutoff_et":   _hhmm("DEFERRED_MATERIALIZATION_ENTRY_CUTOFF_ET", "15:30"),
    }


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _retry_delay_seconds(attempt: int, cfg: dict) -> int:
    """Bounded exponential backoff: base * 2^(attempt-1), capped at retry_max_s."""
    try:
        base = int(cfg.get("retry_base_s", 15))
        maximum = int(cfg.get("retry_max_s", 90))
        return min(maximum, base * (2 ** max(0, attempt - 1)))
    except Exception:
        return 15


def _is_past_entry_cutoff(cfg: dict) -> bool:
    """Return True if current ET time has passed the entry cutoff (e.g. 15:30)."""
    try:
        from zoneinfo import ZoneInfo
        from datetime import datetime
        now_et = datetime.now(ZoneInfo("America/New_York"))
        cutoff = cfg.get("entry_cutoff_et", "15:30")
        ch, cm = cutoff.split(":")
        cutoff_minutes = int(ch) * 60 + int(cm)
        now_minutes = now_et.hour * 60 + now_et.minute
        return now_minutes >= cutoff_minutes
    except Exception:
        return False


# ── OSM lifecycle meta helpers ────────────────────────────────────────────────

def _osm_update_meta(osm, local_order_id: str, patch: dict) -> bool:
    """Safely call osm.update_order_meta; returns True on success."""
    try:
        fn = getattr(osm, "update_order_meta", None)
        if callable(fn):
            return bool(fn(local_order_id, patch))
    except Exception as exc:
        log.debug("materialization meta update failed local_order_id=%s: %s",
                  local_order_id, exc)
    return False


def stamp_trigger_queued(
    osm,
    local_order_id: str,
    *,
    client_id: str,
    execution_mode: str,
    symbol: str,
    direction: str,
    triggered_price: float,
    signal_id: str = "",
) -> bool:
    """
    Stamp meta when price triggers a deferred row — moving it from
    WAITING_FOR_TRIGGER to QUEUED (materialization bucket).

    Called from the watcher breach handler BEFORE the selector runs.
    Returns True if the meta update succeeded.

    Log marker: DEFERRED_TRIGGER_MOVED_TO_MATERIALIZATION_BUCKET
    """
    now = _now_utc().isoformat()
    patch = {
        "materialization_status":         QUEUED,
        "broker_ready":                   False,
        "triggered_at":                   now,
        "triggered_underlying_price":     float(triggered_price or 0),
        "materialization_started_at":     now,
        "materialization_attempts":       0,
    }
    ok = _osm_update_meta(osm, local_order_id, patch)
    log.info(
        "DEFERRED_TRIGGER_MOVED_TO_MATERIALIZATION_BUCKET "
        "order_id=%s client_id=%s execution_mode=%s symbol=%s "
        "direction=%s contract_before=DEFERRED:* limit_before=0.01 "
        "new_status=QUEUED broker_ready=false triggered_price=%.4f",
        local_order_id, client_id, execution_mode, symbol,
        direction, float(triggered_price or 0),
    )
    return ok


def stamp_running(
    osm,
    local_order_id: str,
    *,
    client_id: str,
    execution_mode: str,
    symbol: str,
    attempt: int,
    lock_ttl_s: int = 120,
) -> bool:
    """
    Stamp meta when materializer claims a QUEUED row and starts selector.
    Sets a lock expiry so stuck rows can be retried by another worker.

    Log marker: DEFERRED_MATERIALIZATION_CLAIMED + DEFERRED_MATERIALIZATION_STARTED
    """
    lock_until = (_now_utc() + timedelta(seconds=lock_ttl_s)).isoformat()
    patch = {
        "materialization_status":    RUNNING,
        "broker_ready":              False,
        "materialization_lock_until": lock_until,
        "materialization_claimed_at": _now_utc().isoformat(),
        "materialization_attempts":   int(attempt),
    }
    ok = _osm_update_meta(osm, local_order_id, patch)
    log.info(
        "DEFERRED_MATERIALIZATION_CLAIMED "
        "order_id=%s client_id=%s execution_mode=%s symbol=%s "
        "attempt=%d lock_until=%s",
        local_order_id, client_id, execution_mode, symbol, attempt, lock_until,
    )
    log.info(
        "DEFERRED_MATERIALIZATION_STARTED "
        "order_id=%s client_id=%s execution_mode=%s symbol=%s "
        "direction=unknown retry_count=%d contract_before=DEFERRED:* limit_before=0.01 "
        "materialization_status=RUNNING broker_ready=false",
        local_order_id, client_id, execution_mode, symbol, attempt,
    )
    return ok


def stamp_selected(
    osm,
    local_order_id: str,
    *,
    client_id: str,
    execution_mode: str,
    symbol: str,
    direction: str,
    contract: str,
    bid: float,
    ask: float,
    mid: float,
    limit_price: float,
    qty: int,
    reserved_cost: float,
    dte: Optional[int] = None,
    expiration: Optional[str] = None,
    delta: Optional[float] = None,
    open_interest: Optional[int] = None,
    volume: Optional[int] = None,
    attempt: int = 1,
) -> bool:
    """
    Stamp meta when materialization succeeds — real OCC contract selected.
    Sets broker_ready=True. Only call when contract is a real OCC symbol
    (not DEFERRED:*), limit_price > 0.01, qty >= 1.

    This is the ONLY place broker_ready=True is set.

    Log markers: DEFERRED_MATERIALIZATION_SELECTED + READY_TO_SUBMIT_CREATED
    """
    # Invariant: never set broker_ready=True on a placeholder contract.
    _c = str(contract or "")
    if not _c or _c.upper().startswith("DEFERRED:") or float(limit_price or 0) <= 0.01 or int(qty or 0) < 1:
        log.critical(
            "stamp_selected called with invalid contract/limit/qty — "
            "refusing to set broker_ready=True: contract=%r limit=%.4f qty=%d",
            _c, float(limit_price or 0), int(qty or 0),
        )
        return False

    now = _now_utc().isoformat()
    patch = {
        "materialization_status":        SELECTED,
        "broker_ready":                  True,
        "selected_contract":             _c,
        "selected_bid":                  round(float(bid or 0), 4),
        "selected_ask":                  round(float(ask or 0), 4),
        "selected_mid":                  round(float(mid or 0), 4),
        "selected_limit":                round(float(limit_price), 4),
        "selected_qty":                  int(qty),
        "selected_reserved_cost":        round(float(reserved_cost or 0), 2),
        "selected_dte":                  int(dte) if dte is not None else None,
        "selected_expiration":           str(expiration) if expiration else None,
        "selected_delta":                round(float(delta), 4) if delta is not None else None,
        "selected_open_interest":        int(open_interest) if open_interest is not None else None,
        "selected_volume":               int(volume) if volume is not None else None,
        "materialization_finished_at":   now,
        "materialization_attempts":      int(attempt),
    }
    ok = _osm_update_meta(osm, local_order_id, patch)
    log.info(
        "DEFERRED_MATERIALIZATION_SELECTED "
        "order_id=%s client_id=%s execution_mode=%s symbol=%s "
        "direction=%s contract_after=%s limit_after=%.4f qty=%d "
        "bid=%.4f ask=%.4f mid=%.4f reserved_cost=%.2f "
        "retry_count=%d materialization_status=SELECTED broker_ready=true",
        local_order_id, client_id, execution_mode, symbol,
        direction, _c, float(limit_price), int(qty),
        float(bid or 0), float(ask or 0), float(mid or 0),
        float(reserved_cost or 0), int(attempt),
    )
    log.info(
        "READY_TO_SUBMIT_CREATED "
        "order_id=%s client_id=%s execution_mode=%s symbol=%s "
        "contract=%s limit=%.4f qty=%d broker_ready=true",
        local_order_id, client_id, execution_mode, symbol,
        _c, float(limit_price), int(qty),
    )
    return ok


def stamp_retry_pending(
    osm,
    local_order_id: str,
    *,
    client_id: str,
    execution_mode: str,
    symbol: str,
    direction: str,
    reason_code: str,
    attempt: int,
    max_attempts: int,
    retry_delay_s: int,
    selector_failure: "dict | None" = None,
) -> bool:
    """
    Stamp meta on retryable failure — chain warmup, zero quotes, provider error.
    Sets next_retry_at for the materializer worker to pick up.

    Log marker: DEFERRED_MATERIALIZATION_RETRY
    """
    next_retry = (_now_utc() + timedelta(seconds=retry_delay_s)).isoformat()
    patch = {
        "materialization_status":          RETRY_PENDING,
        "broker_ready":                    False,
        "materialization_attempts":        int(attempt),
        "materialization_next_retry_at":   next_retry,
        "materialization_reason":          str(reason_code or ""),
        "materialization_last_failure_at": _now_utc().isoformat(),
    }
    if selector_failure:
        patch["materialization_selector_failure"] = selector_failure
    ok = _osm_update_meta(osm, local_order_id, patch)
    log.warning(
        "DEFERRED_MATERIALIZATION_RETRY "
        "order_id=%s client_id=%s execution_mode=%s symbol=%s "
        "direction=%s failure_reason=%s retry_count=%d "
        "next_retry_at=%s contract_before=DEFERRED:* limit_before=0.01 "
        "materialization_status=RETRY_PENDING broker_ready=false",
        local_order_id, client_id, execution_mode, symbol,
        direction, reason_code, attempt, next_retry,
    )
    return ok


def stamp_failed_terminal(
    osm,
    local_order_id: str,
    *,
    client_id: str,
    execution_mode: str,
    symbol: str,
    direction: str,
    reason_code: str,
    attempt: int,
    selector_failure: "dict | None" = None,
) -> bool:
    """
    Stamp meta on terminal failure — quality rejects, exhausted retries, entry cutoff.
    broker_ready stays False. Log marker: DEFERRED_MATERIALIZATION_FAILED
    """
    patch = {
        "materialization_status":          FAILED_TERMINAL,
        "broker_ready":                    False,
        "materialization_attempts":        int(attempt),
        "materialization_reason":          str(reason_code or ""),
        "materialization_finished_at":     _now_utc().isoformat(),
        "materialization_last_failure_at": _now_utc().isoformat(),
    }
    if selector_failure:
        patch["materialization_selector_failure"] = selector_failure
    ok = _osm_update_meta(osm, local_order_id, patch)
    log.critical(
        "DEFERRED_MATERIALIZATION_FAILED "
        "order_id=%s client_id=%s execution_mode=%s symbol=%s "
        "direction=%s failure_reason=%s retry_count=%d "
        "contract_before=DEFERRED:* limit_before=0.01 "
        "materialization_status=FAILED_TERMINAL broker_ready=false",
        local_order_id, client_id, execution_mode, symbol,
        direction, reason_code, attempt,
    )
    return ok


def is_broker_ready_from_meta(plan_metadata: "dict | None") -> bool:
    """
    Returns True only if broker_ready=True is explicitly set in plan metadata.
    Used at pre-submit to gate broker POST.

    NOTE: plan_metadata here is approved_plan.metadata (in-memory), which must
    be kept in sync with orders.meta.broker_ready via stamp_selected().
    """
    if not isinstance(plan_metadata, dict):
        return False
    return plan_metadata.get("broker_ready") is True


def should_reprocess_claim(plan_metadata: "dict | None", cfg: dict) -> bool:
    """
    Return True if a RUNNING row's lock has expired and can be re-claimed.
    Prevents double-materialization while allowing stuck rows to be retried.
    """
    if not isinstance(plan_metadata, dict):
        return True   # no claim data → safe to claim
    if plan_metadata.get("materialization_status") != RUNNING:
        return True   # not locked → claimable
    lock_until_str = plan_metadata.get("materialization_lock_until")
    if not lock_until_str:
        return True   # no expiry → claimable
    try:
        from datetime import datetime, timezone
        lock_until = datetime.fromisoformat(str(lock_until_str))
        return datetime.now(timezone.utc) > lock_until
    except Exception:
        return True   # parse failure → claimable (conservative)


def is_reason_retryable(reason_code: str) -> bool:
    return str(reason_code or "").strip() in RETRYABLE_MATERIALIZATION_REASONS
