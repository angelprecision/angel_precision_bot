"""
ap/pending_trigger_classifier.py

PENDING_TRIGGER invariant classifier — Bug E from PR #304.

WHY THIS MODULE EXISTS
──────────────────────
A row with status=PENDING_TRIGGER is only valid while a live watcher owns it
AND no terminal decision has been made. Prior to this classifier, three code
paths (morning handoff audit, order monitor ghost sweep, watcher health) each
implemented their own "is this row safe to rescue?" logic — inconsistently.

The result: rows that had already been invalidated by the watcher (stop touch,
overnight breach, arm-time through-trigger) could still be re-armed by
recovery_rearm because the caller didn't check watcher_audit.reason_code.
Rows with terminal materialization_outcome could sit as PENDING_TRIGGER
forever. Trigger-ready rows whose callback failed could zombie.

This module gives all three callers ONE classification vocabulary so live
safety decisions are consistent across the system.

DESIGN CONTRACT
───────────────
• Pure classification — no DB writes, no side effects, never raises.
• Reads only orders.meta (JSONB) fields the row already carries.
• Callers decide the action based on classification (rescue, terminalize, ignore).
• LIVE callers must ONLY rescue WAITING_VALID or WAITING_RETRYABLE.
• All other classifications require terminal cleanup, not rescue.

USAGE
─────
    from ap.pending_trigger_classifier import (
        PendingTriggerClassification,
        classify_pending_trigger_row,
    )

    cls = classify_pending_trigger_row(row_dict, watcher_owned=owned_bool)
    if cls == PendingTriggerClassification.WAITING_VALID:
        # safe to recovery-rearm
        entry_watcher.watch(plan, local_order_id, recovery_rearm=True)
    else:
        # terminalize or leave alone — never rescue
        ...
"""

from __future__ import annotations

from typing import Optional

from ap.logger import get_logger

log = get_logger("ap.pending_trigger_classifier")


class PendingTriggerClassification:
    """Categorical labels for the state of a PENDING_TRIGGER row."""
    # Safe states — recovery rearm allowed
    WAITING_VALID              = "WAITING_VALID"
    WAITING_RETRYABLE          = "WAITING_RETRYABLE"

    # Unsafe — must NOT be rearmed; terminal cleanup required
    STUCK_TRIGGER_READY        = "STUCK_TRIGGER_READY"
    STUCK_INVALIDATED          = "STUCK_INVALIDATED"
    STUCK_TERMINAL_MATERIALIZATION = "STUCK_TERMINAL_MATERIALIZATION"
    ORPHAN_NO_WATCHER          = "ORPHAN_NO_WATCHER"
    STALE_AFTER_EOD            = "STALE_AFTER_EOD"
    UNSAFE_ALREADY_THROUGH_TRIGGER = "UNSAFE_ALREADY_THROUGH_TRIGGER"

    # Row shape is not PENDING_TRIGGER at all
    NOT_PENDING_TRIGGER        = "NOT_PENDING_TRIGGER"


# ─────────────────────────────────────────────────────────────────────────────
# Terminal reason sets — must stay in sync with
# ap_execution_core._REAL_UNDERLYING_INVALIDATION_REASONS (Bug A classifier)
# ─────────────────────────────────────────────────────────────────────────────

_INVALIDATION_REASON_CODES: frozenset[str] = frozenset({
    "overnight_daily_invalidated",
    "overnight_premarket_breached",
    "overnight_too_far_from_trigger",
    "overnight_open_recheck_data_timeout",
    "overnight_daily_validator_error",
    "overnight_live_quote_unavailable",
    "overnight_daily_already_through_trigger",
    "arm_drift",
    "arm_below_stop",
    "arm_already_through_trigger",
    "watcher_invalidated",
    "on_trigger_exhausted_3_attempts",
})

_TERMINAL_MATERIALIZATION_OUTCOMES: frozenset[str] = frozenset({
    "TERMINAL_NO_TRADEABLE_CONTRACT",
    "TERMINAL_QUALITY_REJECT",
    "TERMINAL_MATERIALIZATION_FAILED",
    "FAILED_TERMINAL",
})


def _reason_is_invalidation(reason_code: str) -> bool:
    """Match the classifier in ap_execution_core: stop_* prefix + known set."""
    rc = str(reason_code or "").strip().lower()
    if not rc:
        return False
    if rc.startswith("stop_"):
        return True
    return rc in _INVALIDATION_REASON_CODES


def _extract(row: dict, path: str, default=None):
    """Best-effort nested-dict extract. Never raises."""
    try:
        cur = row
        for part in path.split("."):
            if not isinstance(cur, dict):
                return default
            cur = cur.get(part)
            if cur is None:
                return default
        return cur
    except Exception:
        return default


def classify_pending_trigger_row(
    row: dict,
    *,
    watcher_owned: Optional[bool] = None,
    is_past_eod: bool = False,
    live_quote_already_through_trigger: Optional[bool] = None,
) -> str:
    """
    Classify a single orders row.

    Arguments:
        row: dict-like orders row. Expected keys:
            status, broker_order_id, submitted_ts, meta (dict)
        watcher_owned: caller-supplied — True if the running watcher currently
            has this local_order_id in _pending. When None, the ORPHAN_NO_WATCHER
            classification is skipped (caller has no ownership info).
        is_past_eod: caller-supplied — True if the row is past the entry cutoff.
        live_quote_already_through_trigger: caller-supplied — True if a fresh
            underlying quote confirms price already crossed the trigger. When
            None, the UNSAFE_ALREADY_THROUGH_TRIGGER check is skipped.

    Returns one of the PendingTriggerClassification constants.
    """
    try:
        status = str(row.get("status") or "").strip().upper()
        if status != "PENDING_TRIGGER":
            return PendingTriggerClassification.NOT_PENDING_TRIGGER

        broker_order_id = row.get("broker_order_id")
        submitted_ts    = row.get("submitted_ts")
        meta            = row.get("meta") or {}
        if not isinstance(meta, dict):
            meta = {}

        # If broker already accepted or submit already stamped, this is not
        # a lifecycle bug — it's a partially-applied write; not our concern.
        if broker_order_id or submitted_ts:
            return PendingTriggerClassification.NOT_PENDING_TRIGGER

        watcher_reason = str(_extract(meta, "watcher_audit.reason_code") or "").strip()
        materialization_outcome = str(
            meta.get("materialization_outcome")
            or _extract(meta, "materialization.outcome")
            or ""
        ).strip().upper()
        retry_status = str(
            meta.get("materialization_status")
            or meta.get("retry_status")
            or ""
        ).strip().upper()
        retry_next_at = (
            meta.get("materialization_next_retry_at")
            or _extract(meta, "materialization.next_retry_at")
        )

        # ── Priority 1: trigger_ready without broker_order_id is the zombie ──
        # A watcher decided the row should submit, but broker never accepted.
        # In LIVE this is NEVER rescuable — the trigger decision is stale.
        if watcher_reason == "trigger_ready":
            return PendingTriggerClassification.STUCK_TRIGGER_READY

        # ── Priority 2: real invalidation reason ──
        if watcher_reason == "orphan_no_watcher":
            return PendingTriggerClassification.ORPHAN_NO_WATCHER

        if _reason_is_invalidation(watcher_reason):
            return PendingTriggerClassification.STUCK_INVALIDATED

        # ── Priority 3: materialization already terminal ──
        if materialization_outcome in _TERMINAL_MATERIALIZATION_OUTCOMES:
            return PendingTriggerClassification.STUCK_TERMINAL_MATERIALIZATION

        # ── Priority 4: caller-supplied unsafe live-quote signal ──
        if live_quote_already_through_trigger is True:
            return PendingTriggerClassification.UNSAFE_ALREADY_THROUGH_TRIGGER

        # ── Priority 5: past EOD without an active retry ──
        if is_past_eod and retry_status not in ("RETRY_PENDING", "RUNNING", "QUEUED"):
            return PendingTriggerClassification.STALE_AFTER_EOD

        # ── Priority 6: watcher ownership check (when caller provided it) ──
        if watcher_owned is False:
            return PendingTriggerClassification.ORPHAN_NO_WATCHER

        # ── Priority 7: active retry state → retryable ──
        if retry_status in ("RETRY_PENDING", "RUNNING") or retry_next_at:
            return PendingTriggerClassification.WAITING_RETRYABLE

        # Default: watcher owns it, no terminal evidence — safe waiting state
        return PendingTriggerClassification.WAITING_VALID

    except Exception as exc:
        # Never raise — classification failure defaults to STUCK to fail closed.
        log.debug("classify_pending_trigger_row unexpected error: %s", exc)
        return PendingTriggerClassification.ORPHAN_NO_WATCHER


def is_safe_to_recovery_rearm(classification: str) -> bool:
    """
    LIVE rescue rule: ONLY WAITING_VALID and WAITING_RETRYABLE may be
    recovery-rearmed. All other classifications require terminal cleanup.

    Callers should treat this as the definitive live-safety gate — if this
    returns False, do NOT call entry_watcher.watch(recovery_rearm=True).
    """
    return classification in (
        PendingTriggerClassification.WAITING_VALID,
        PendingTriggerClassification.WAITING_RETRYABLE,
    )
