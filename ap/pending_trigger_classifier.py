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

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Optional, Union

from ap.logger import get_logger
from ap.selector_retry_policy import (
    DeferredMaterializationConfigConflict,
    is_retryable_selector_reason,
    resolve_deferred_materialization_max_attempts,
)

log = get_logger("ap.pending_trigger_classifier")


# ─────────────────────────────────────────────────────────────────────────────
# Trigger-continuation policy (PR #388 Block-2)
#
# When the watcher attaches to a live opportunity slightly after the underlying
# has already touched its entry trigger, the setup is not automatically dead —
# a small continuation window is an ordinary trade-flow event, not a missed
# move. This module owns the *only* canonical definitions of that window.
# Duplicating the formula in the watcher or elsewhere is forbidden.
#
# Defaults are the operator-tuned values from the Block-2 spec; override via
# env only for A/B experiments — production must run at the defaults unless a
# change is reviewed and merged.
# ─────────────────────────────────────────────────────────────────────────────

_DEC_ZERO   = Decimal("0")
_DEC_TEN_K  = Decimal("10000")
_DEC_HALF   = Decimal("0.5")
_DEC_MIN_RESET = Decimal("0.01")


def _positive_decimal_env(name: str, default: str) -> Decimal:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return Decimal(default)
    try:
        value = Decimal(str(raw).strip())
    except (InvalidOperation, ValueError):
        return Decimal(default)
    return value if value > _DEC_ZERO else Decimal(default)


ENTRY_TRIGGER_CONTINUATION_MAX_ABS: Decimal = _positive_decimal_env(
    "ENTRY_TRIGGER_CONTINUATION_MAX_ABS", "0.15"
)
ENTRY_TRIGGER_CONTINUATION_MAX_BPS: Decimal = _positive_decimal_env(
    "ENTRY_TRIGGER_CONTINUATION_MAX_BPS", "7.5"
)


def _safe_decimal(value) -> Optional[Decimal]:
    """Coerce numeric input to Decimal. Returns None for missing/invalid.

    Accepts int, float, str, Decimal. Rejects None, empty string, NaN,
    non-numeric values. Never raises.
    """
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value if value.is_finite() else None
    if isinstance(value, bool):
        return None
    try:
        s = str(value).strip()
    except Exception:
        return None
    if not s:
        return None
    try:
        d = Decimal(s)
    except (InvalidOperation, ValueError):
        return None
    return d if d.is_finite() else None


def compute_allowed_continuation(trigger_price) -> Optional[Decimal]:
    """Canonical formula for the late-attachment continuation window.

    allowed = min(MAX_ABS, trigger * MAX_BPS / 10000)

    Returns None when trigger_price is missing, non-numeric, or non-positive.
    Callers must never inline this formula; always route through here so the
    watcher, restart recovery, and diagnostics can never drift into
    conflicting policies.
    """
    trigger = _safe_decimal(trigger_price)
    if trigger is None or trigger <= _DEC_ZERO:
        return None
    bps_component = (trigger * ENTRY_TRIGGER_CONTINUATION_MAX_BPS) / _DEC_TEN_K
    return min(ENTRY_TRIGGER_CONTINUATION_MAX_ABS, bps_component)


def compute_reset_tolerance(trigger_price) -> Optional[Decimal]:
    """Reset tolerance used by the WAITING_RESET rebreach path.

    reset_tolerance = max(0.01, allowed_continuation * 0.5)

    Derived from compute_allowed_continuation so the arm-time,
    waiting-reset, and rebreach checks share one source of truth.
    Returns None when trigger_price is invalid.
    """
    allowed = compute_allowed_continuation(trigger_price)
    if allowed is None:
        return None
    return max(_DEC_MIN_RESET, allowed * _DEC_HALF)


# ─────────────────────────────────────────────────────────────────────────────
# Canonical trigger-quote lane
#
# CALL executes only when the market ASK is at/above the CALL trigger.
# PUT  executes only when the market BID is at/below the PUT  trigger.
#
# The watcher's ordinary breach check AND the new continuation / reset /
# rebreach checks MUST route through this helper — routing the old breach
# path through one lane while the continuation code selects another would
# reintroduce the exact drift this module exists to prevent.
#
# Missing truth is retryable — never fall back to mid, mark, last, or the
# opposite side. A missing canonical quote must never be interpreted as a
# breach, reset, stop failure, or missed move.
# ─────────────────────────────────────────────────────────────────────────────

TRIGGER_QUOTE_AVAILABLE       = "TRIGGER_QUOTE_AVAILABLE"
TRIGGER_QUOTE_UNAVAILABLE     = "TRIGGER_QUOTE_UNAVAILABLE"
TRIGGER_SIDE_INVALID          = "TRIGGER_SIDE_INVALID"
TRIGGER_TRIGGER_INVALID       = "TRIGGER_TRIGGER_INVALID"


@dataclass(frozen=True)
class TriggerQuoteResult:
    """Canonical result of a per-side trigger-quote lookup.

    .available    — True only when a positive quote was found on the correct side.
    .value        — the selected quote (Decimal) or None.
    .source       — "ask" for CALL, "bid" for PUT, or None on side error.
    .reason_code  — TRIGGER_QUOTE_AVAILABLE / _UNAVAILABLE / SIDE_INVALID.
    """
    available:   bool
    value:       Optional[Decimal]
    source:      Optional[str]
    reason_code: str


def _canonical_trigger_quote(*, side, bid, ask) -> TriggerQuoteResult:
    """Return the per-side canonical trigger quote (CALL=ask, PUT=bid).

    Never fetches quotes. Only selects from the fresh underlying bid/ask the
    caller already read from the watcher's existing quote lane. No fallback
    to mid/mark/last/opposite side — missing truth is retryable.
    """
    normalized_side = str(side or "").strip().upper()
    if normalized_side == "CALL":
        quote = _safe_decimal(ask)
        source = "ask"
    elif normalized_side == "PUT":
        quote = _safe_decimal(bid)
        source = "bid"
    else:
        return TriggerQuoteResult(
            available=False,
            value=None,
            source=None,
            reason_code=TRIGGER_SIDE_INVALID,
        )

    if quote is None or quote <= _DEC_ZERO:
        return TriggerQuoteResult(
            available=False,
            value=None,
            source=source,
            reason_code=TRIGGER_QUOTE_UNAVAILABLE,
        )

    return TriggerQuoteResult(
        available=True,
        value=quote,
        source=source,
        reason_code=TRIGGER_QUOTE_AVAILABLE,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Late-attachment classification (PR #388 Block-2)
# ─────────────────────────────────────────────────────────────────────────────

# Non-terminal lifecycle labels (watcher continues to own the setup).
LATE_ATTACHMENT_AWAITING_FIRST_TRUTH          = "LATE_ATTACHMENT_AWAITING_FIRST_TRUTH"
LATE_ATTACHMENT_WITHIN_CONTINUATION           = "LATE_ATTACHMENT_WITHIN_CONTINUATION"
LATE_CONTINUATION_CONFIRMED                   = "LATE_CONTINUATION_CONFIRMED"
MISSED_LATE_WATCHER_ATTACHMENT_WAITING_RESET  = "MISSED_LATE_WATCHER_ATTACHMENT_WAITING_RESET"
LATE_ATTACHMENT_RESET_CONFIRMED               = "LATE_ATTACHMENT_RESET_CONFIRMED"
LATE_ATTACHMENT_REBREACH_CONFIRMED            = "LATE_ATTACHMENT_REBREACH_CONFIRMED"
WATCHER_ARMED_BEFORE_OPEN                     = "WATCHER_ARMED_BEFORE_OPEN"

# Terminal decisions.
LATE_ATTACHMENT_MOVE_MISSED_TERMINAL          = "LATE_ATTACHMENT_MOVE_MISSED_TERMINAL"
STOP_ALREADY_BROKEN_TERMINAL                  = "STOP_ALREADY_BROKEN_TERMINAL"
TARGET_ALREADY_COMPLETE_TERMINAL              = "TARGET_ALREADY_COMPLETE_TERMINAL"

# Retryable (truth unavailable — try again next poll, never terminalize).
TRIGGER_TRUTH_UNAVAILABLE_RETRY               = "TRIGGER_TRUTH_UNAVAILABLE_RETRY"


@dataclass(frozen=True)
class LateAttachmentDecision:
    """Result of a single late-attachment classification call.

    .classification — one of the LATE_* / STOP_* / TARGET_* / TRIGGER_* constants above.
    .allowed_continuation — the applicable window (Decimal) or None if not computable.
    .quote — the canonical trigger quote used, or None if unavailable.
    .quote_source — "ask" / "bid" / None.
    .detail — short human-readable diagnostic (never used for control flow).
    """
    classification:       str
    allowed_continuation: Optional[Decimal]
    quote:                Optional[Decimal]
    quote_source:         Optional[str]
    detail:               str = ""


_STOP_BROKEN     = "STOP_BROKEN"
_STOP_NOT_BROKEN = "STOP_NOT_BROKEN"
_STOP_UNKNOWN    = "STOP_UNKNOWN"
_STOP_NO_STOP    = "STOP_NO_STOP"


def _evaluate_stop(side: str, stop, bid, ask) -> str:
    """Tri-state stop-broken evaluator returning one of:
      _STOP_NO_STOP      — no stop configured (or invalid); nothing to check
      _STOP_UNKNOWN      — valid stop exists but the STOP-SIDE quote is missing
      _STOP_BROKEN       — actionable exit-side price crossed the stop
      _STOP_NOT_BROKEN   — actionable exit-side price is on the safe side

    Stop invalidation uses the OPPOSITE side from trigger evaluation:
      CALL: stop broken when bid <= stop
      PUT:  stop broken when ask >= stop

    _STOP_UNKNOWN is the critical distinction from the old boolean
    signature. When a valid stop exists but the required stop-side quote is
    missing, the caller MUST NOT interpret this as 'not broken' and continue
    into WITHIN_CONTINUATION / WAITING_RESET / ordinary pre-trigger. The
    stop-side truth is unavailable — the whole classifier call is retryable.
    """
    stop_d = _safe_decimal(stop)
    if stop_d is None or stop_d <= _DEC_ZERO:
        return _STOP_NO_STOP
    normalized_side = str(side or "").strip().upper()
    if normalized_side == "CALL":
        stop_quote = _safe_decimal(bid)
        if stop_quote is None or stop_quote <= _DEC_ZERO:
            return _STOP_UNKNOWN
        return _STOP_BROKEN if stop_quote <= stop_d else _STOP_NOT_BROKEN
    if normalized_side == "PUT":
        stop_quote = _safe_decimal(ask)
        if stop_quote is None or stop_quote <= _DEC_ZERO:
            return _STOP_UNKNOWN
        return _STOP_BROKEN if stop_quote >= stop_d else _STOP_NOT_BROKEN
    return _STOP_NO_STOP


def _stop_broken(side: str, stop, bid, ask) -> bool:
    """Legacy boolean shim — True only when definitively BROKEN.

    Kept only for callers that still expect a boolean; classify_late_attachment
    uses _evaluate_stop directly so it can propagate _STOP_UNKNOWN as retry.
    """
    return _evaluate_stop(side, stop, bid=bid, ask=ask) == _STOP_BROKEN


def classify_late_attachment(
    *,
    side: str,
    trigger_price,
    bid,
    ask,
    stop=None,
    target_complete: bool = False,
    decisive_drift_exceeded: bool = False,
    trigger_previously_breached: bool = False,
) -> LateAttachmentDecision:
    """Classify a late-attachment observation.

    Called at the arm-time already-through-trigger site AND at each fresh poll
    while the watcher is in LATE_ATTACHMENT_WITHIN_CONTINUATION or
    MISSED_LATE_WATCHER_ATTACHMENT_WAITING_RESET state.

    Terminal outputs (caller must terminalize):
      STOP_ALREADY_BROKEN_TERMINAL
      TARGET_ALREADY_COMPLETE_TERMINAL
      LATE_ATTACHMENT_MOVE_MISSED_TERMINAL   (decisive drift exceeded)

    Retryable output (caller waits for next poll — never terminalizes):
      TRIGGER_TRUTH_UNAVAILABLE_RETRY

    Non-terminal lifecycle outputs:
      LATE_ATTACHMENT_WITHIN_CONTINUATION           (inside continuation zone)
      MISSED_LATE_WATCHER_ATTACHMENT_WAITING_RESET  (past zone, structurally valid, awaiting reset)

    ``trigger_previously_breached`` is durable lifecycle evidence supplied by
    the watcher.  The scanner stop is dormant until that evidence exists or
    the current canonical trigger quote itself proves the first breach.
    """
    normalized_side = str(side or "").strip().upper()
    if normalized_side not in ("CALL", "PUT"):
        return LateAttachmentDecision(
            classification=TRIGGER_TRUTH_UNAVAILABLE_RETRY,
            allowed_continuation=None,
            quote=None,
            quote_source=None,
            detail=f"invalid_side:{side!r}",
        )

    if target_complete:
        return LateAttachmentDecision(
            classification=TARGET_ALREADY_COMPLETE_TERMINAL,
            allowed_continuation=None,
            quote=None,
            quote_source=None,
            detail="target_already_complete",
        )

    allowed = compute_allowed_continuation(trigger_price)
    if allowed is None:
        return LateAttachmentDecision(
            classification=TRIGGER_TRUTH_UNAVAILABLE_RETRY,
            allowed_continuation=None,
            quote=None,
            quote_source=None,
            detail="invalid_or_missing_trigger",
        )
    trigger = _safe_decimal(trigger_price)  # non-None (allowed already computed)

    quote_result = _canonical_trigger_quote(side=normalized_side, bid=bid, ask=ask)
    if not quote_result.available or quote_result.value is None:
        return LateAttachmentDecision(
            classification=TRIGGER_TRUTH_UNAVAILABLE_RETRY,
            allowed_continuation=allowed,
            quote=None,
            quote_source=quote_result.source,
            detail=quote_result.reason_code,
        )

    canonical_quote = quote_result.value

    # A stop is not an entry-trigger substitute.  Before a CONFIRMED durable
    # breach, the opposite-side stop geometry is dormant; a wide spread must
    # not terminalize a still-eligible setup.  Once durable breach evidence
    # is known (trigger_previously_breached=True), keep the stop active even
    # if the current quote has subsequently reset.
    #
    # PR #407 correction: a single current trigger-side quote is NOT
    # activation.  The watcher only issues durable trigger_crossed_at after
    # MOMENTUM_POLLS_REQUIRED breaches confirm; the classifier must honor
    # that same invariant.  Only the STOP EVALUATION block is gated; other
    # classifications (WITHIN_CONTINUATION, WAITING_RESET, pre-trigger
    # ordinary-breach) still flow.
    _stop_active = trigger_previously_breached is True
    if _stop_active:
        _stop_state = _evaluate_stop(normalized_side, stop, bid=bid, ask=ask)
        if _stop_state == _STOP_BROKEN:
            _stop_source = "bid" if normalized_side == "CALL" else "ask"
            _stop_value = _safe_decimal(bid if normalized_side == "CALL" else ask)
            return LateAttachmentDecision(
                classification=STOP_ALREADY_BROKEN_TERMINAL,
                allowed_continuation=allowed,
                quote=canonical_quote,
                quote_source=quote_result.source,
                detail=f"stop_broken_at_{_stop_source}={_stop_value}",
            )
        if _stop_state == _STOP_UNKNOWN:
            # Valid stop exists but the STOP-SIDE quote is missing (CALL:
            # bid=0; PUT: ask=0). We cannot prove the stop is safe, so we
            # must NOT let the classifier continue into WITHIN_CONTINUATION
            # / WAITING_RESET / pre-trigger. Return the retry shape with
            # quote=None so the watcher seeds/preserves AWAITING_FIRST_TRUTH
            # and blocks the ordinary breach path until both sides have
            # truth.
            return LateAttachmentDecision(
                classification=TRIGGER_TRUTH_UNAVAILABLE_RETRY,
                allowed_continuation=allowed,
                quote=None,
                quote_source=quote_result.source,
                detail=f"stop_side_quote_unavailable_{normalized_side}",
            )
    # Dormant stop path: fall through to WITHIN / WAITING / pre-trigger
    # classification below.  The watcher's own check() owns breach counting
    # and only issues durable trigger_crossed_at after confirmation.

    if decisive_drift_exceeded:
        return LateAttachmentDecision(
            classification=LATE_ATTACHMENT_MOVE_MISSED_TERMINAL,
            allowed_continuation=allowed,
            quote=canonical_quote,
            quote_source=quote_result.source,
            detail="decisive_drift_exceeded",
        )

    if normalized_side == "CALL":
        upper = trigger + allowed
        if trigger <= canonical_quote <= upper:
            return LateAttachmentDecision(
                classification=LATE_ATTACHMENT_WITHIN_CONTINUATION,
                allowed_continuation=allowed,
                quote=canonical_quote,
                quote_source=quote_result.source,
                detail=f"call_within_continuation:{canonical_quote}<={upper}",
            )
        # canonical_quote < trigger → not yet breached; watcher's ordinary
        # breach path owns this. canonical_quote > upper → beyond zone.
        if canonical_quote > upper:
            return LateAttachmentDecision(
                classification=MISSED_LATE_WATCHER_ATTACHMENT_WAITING_RESET,
                allowed_continuation=allowed,
                quote=canonical_quote,
                quote_source=quote_result.source,
                detail=f"call_beyond_continuation:{canonical_quote}>{upper}",
            )
        # canonical_quote < trigger — not yet through; ordinary breach path applies.
        return LateAttachmentDecision(
            classification=TRIGGER_TRUTH_UNAVAILABLE_RETRY,
            allowed_continuation=allowed,
            quote=canonical_quote,
            quote_source=quote_result.source,
            detail="call_below_trigger_ordinary_breach_path",
        )

    # PUT
    lower = trigger - allowed
    if lower <= canonical_quote <= trigger:
        return LateAttachmentDecision(
            classification=LATE_ATTACHMENT_WITHIN_CONTINUATION,
            allowed_continuation=allowed,
            quote=canonical_quote,
            quote_source=quote_result.source,
            detail=f"put_within_continuation:{lower}<={canonical_quote}",
        )
    if canonical_quote < lower:
        return LateAttachmentDecision(
            classification=MISSED_LATE_WATCHER_ATTACHMENT_WAITING_RESET,
            allowed_continuation=allowed,
            quote=canonical_quote,
            quote_source=quote_result.source,
            detail=f"put_beyond_continuation:{canonical_quote}<{lower}",
        )
    return LateAttachmentDecision(
        classification=TRIGGER_TRUTH_UNAVAILABLE_RETRY,
        allowed_continuation=allowed,
        quote=canonical_quote,
        quote_source=quote_result.source,
        detail="put_above_trigger_ordinary_breach_path",
    )


def is_reset_confirmed(*, side: str, trigger_price, canonical_quote) -> bool:
    """True when the current canonical quote satisfies the reset condition.

    CALL: canonical_quote (ask) <= trigger - reset_tolerance
    PUT:  canonical_quote (bid) >= trigger + reset_tolerance

    Reset requires two consecutive fresh polls returning True — the poll-count
    bookkeeping is the caller's responsibility. This helper checks a single
    poll only. Returns False when inputs are missing/invalid.
    """
    normalized_side = str(side or "").strip().upper()
    if normalized_side not in ("CALL", "PUT"):
        return False
    trigger = _safe_decimal(trigger_price)
    quote = _safe_decimal(canonical_quote)
    tolerance = compute_reset_tolerance(trigger_price)
    if trigger is None or quote is None or tolerance is None:
        return False
    if normalized_side == "CALL":
        return quote <= (trigger - tolerance)
    return quote >= (trigger + tolerance)





# ─────────────────────────────────────────────────────────────────────────────
# Watcher completion acknowledgment — PR #324
# ─────────────────────────────────────────────────────────────────────────────

class WatcherCompletionOutcome:
    """Authoritative result outcomes for watcher lifecycle callbacks."""
    TERMINALIZED = "TERMINALIZED"  # order durably left PENDING_TRIGGER
    RETRY_OWNED  = "RETRY_OWNED"   # watcher remains registered with retry state
    REARMED      = "REARMED"       # watcher remains in explicit rearm state
    FAILED       = "FAILED"        # no durable outcome; watcher retained + quarantine


@dataclass(frozen=True)
class WatcherCompletionResult:
    """Typed, immutable result returned by watcher lifecycle callbacks.

    None / arbitrary dict / implicit return must never be interpreted as
    success.  Legacy callbacks that return None are normalized to FAILED
    unless the caller can independently prove a durable outcome.
    """
    outcome:        str
    reason_code:    str
    local_order_id: Optional[str] = None
    retry_next_at:  Optional[str] = None
    retry_deadline: Optional[str] = None
    detail:         Optional[str] = None


# ─────────────────────────────────────────────────────────────────────────────
# Watcher invalidation 5-class taxonomy — PR #324
# ─────────────────────────────────────────────────────────────────────────────

class WatcherInvalidationClass:
    """Lifecycle classifications for watcher invalidation decisions."""
    TERMINAL           = "INVALIDATED_TERMINAL"
    RETRYABLE          = "INVALIDATED_RETRYABLE"
    REARMABLE          = "INVALIDATED_REARMABLE"
    ALREADY_BREACHED   = "INVALIDATED_ALREADY_BREACHED"
    NO_WATCHER_OWNER   = "INVALIDATED_NO_WATCHER_OWNER"


# Canonical reason → class mapping.
# The live watcher and restart classifier share this single source of truth.
WATCHER_INVALIDATION_TAXONOMY: dict[str, str] = {
    # ── INVALIDATED_TERMINAL ─────────────────────────────────────────────────
    "stop_bid_below_call_stop":                  WatcherInvalidationClass.TERMINAL,
    "stop_ask_above_put_stop":                   WatcherInvalidationClass.TERMINAL,
    "overnight_daily_invalidated":               WatcherInvalidationClass.TERMINAL,
    "overnight_too_far_from_trigger":            WatcherInvalidationClass.TERMINAL,
    "arm_drift":                                 WatcherInvalidationClass.TERMINAL,
    "invalid_side":                              WatcherInvalidationClass.TERMINAL,
    "prior_high_breached":                       WatcherInvalidationClass.TERMINAL,
    "prior_low_breached":                        WatcherInvalidationClass.TERMINAL,
    "both_sides_breached":                       WatcherInvalidationClass.TERMINAL,
    "rearm_window_exhausted":                    WatcherInvalidationClass.TERMINAL,
    "rearm_window_expired":                      WatcherInvalidationClass.TERMINAL,
    "rearm_max_attempts_expired":                WatcherInvalidationClass.TERMINAL,
    "overnight_live_quote_unavailable_timeout":  WatcherInvalidationClass.TERMINAL,
    "overnight_open_recheck_data_timeout":       WatcherInvalidationClass.TERMINAL,
    "on_trigger_exhausted_3_attempts":           WatcherInvalidationClass.TERMINAL,
    # PR #388 Block-2 late-attachment terminal reasons
    "late_attachment_move_missed_terminal":      WatcherInvalidationClass.TERMINAL,
    "stop_already_broken_terminal":              WatcherInvalidationClass.TERMINAL,
    "target_already_complete_terminal":          WatcherInvalidationClass.TERMINAL,
    # ── INVALIDATED_RETRYABLE ────────────────────────────────────────────────
    "overnight_live_quote_unavailable":          WatcherInvalidationClass.RETRYABLE,
    "overnight_open_data_unavailable_retry_later": WatcherInvalidationClass.RETRYABLE,
    "overnight_daily_validator_error":           WatcherInvalidationClass.RETRYABLE,
    "snapshot_unavailable":                      WatcherInvalidationClass.RETRYABLE,
    "missing_prior_levels":                      WatcherInvalidationClass.RETRYABLE,
    "quote_fetch_failed":                        WatcherInvalidationClass.RETRYABLE,
    "validator_provider_timeout":                WatcherInvalidationClass.RETRYABLE,
    "temporary_database_failure":                WatcherInvalidationClass.RETRYABLE,
    "trigger_truth_unavailable_retry":           WatcherInvalidationClass.RETRYABLE,
    # ── INVALIDATED_REARMABLE ────────────────────────────────────────────────
    "temporary_wrong_side_of_stop":              WatcherInvalidationClass.REARMABLE,
    "arm_below_stop_reclaim_wait":               WatcherInvalidationClass.REARMABLE,
    # Note: arm_below_stop itself maps to REARMABLE only after rearm eligibility
    # succeeds; otherwise it is TERMINAL.  The watcher resolves this at runtime.
    "arm_below_stop":                            WatcherInvalidationClass.REARMABLE,
    # ── INVALIDATED_ALREADY_BREACHED ─────────────────────────────────────────
    "arm_already_through_trigger":               WatcherInvalidationClass.ALREADY_BREACHED,
    "overnight_daily_already_through_trigger":   WatcherInvalidationClass.ALREADY_BREACHED,
    "overnight_premarket_breached":              WatcherInvalidationClass.ALREADY_BREACHED,
    "trigger_stop_same_poll_collision":          WatcherInvalidationClass.ALREADY_BREACHED,
    # ── INVALIDATED_NO_WATCHER_OWNER (invariant violations) ─────────────────
    # watcher_invalidated alone is never authoritative; underlying reason must survive
    "watcher_invalidated":                       WatcherInvalidationClass.NO_WATCHER_OWNER,
}


def classify_watcher_reason(reason_code: str) -> str:
    """Return the WatcherInvalidationClass for a given reason code.

    Unknown reasons return NO_WATCHER_OWNER.  Callers must handle that class
    as an invariant violation (FAILED, retain watcher, retain dedup).

    stop_* prefix reasons are always TERMINAL regardless of the taxonomy dict
    (matches existing _reason_is_invalidation logic).
    """
    rc = str(reason_code or "").strip().lower()
    if not rc:
        return WatcherInvalidationClass.NO_WATCHER_OWNER
    if rc.startswith("stop_"):
        return WatcherInvalidationClass.TERMINAL
    cls = WATCHER_INVALIDATION_TAXONOMY.get(rc)
    if cls is not None:
        return cls
    return WatcherInvalidationClass.NO_WATCHER_OWNER


class PendingTriggerClassification:
    """Categorical labels for the state of a PENDING_TRIGGER row."""
    # Safe states — recovery rearm allowed
    WAITING_VALID              = "WAITING_VALID"
    WAITING_RETRYABLE          = "WAITING_RETRYABLE"

    # Active materialization owner — recovery must observe and leave read-only
    MATERIALIZATION_IN_FLIGHT  = "MATERIALIZATION_IN_FLIGHT"

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
# Terminal reason sets — derived from WATCHER_INVALIDATION_TAXONOMY (PR #324 §4).
# These must NOT be maintained separately; use classify_watcher_reason() instead.
# Kept for backward-compat with existing callers of _reason_is_invalidation().
# ─────────────────────────────────────────────────────────────────────────────

# Derived at module-load time from the canonical taxonomy.
# INVALIDATED_TERMINAL reasons → treated as "real invalidation" by legacy classifier.
# INVALIDATED_ALREADY_BREACHED reasons are also treated as real (setup is over).
# INVALIDATED_NO_WATCHER_OWNER ("watcher_invalidated" alone) is NOT terminal by itself
# but is kept here for backward compat with existing callers.
def _build_invalidation_reason_codes() -> frozenset:
    _codes = set()
    for reason, cls in WATCHER_INVALIDATION_TAXONOMY.items():
        if cls in (WatcherInvalidationClass.TERMINAL, WatcherInvalidationClass.ALREADY_BREACHED):
            _codes.add(reason)
    # Keep backward-compat entries that callers depend on
    _codes.update({"watcher_invalidated", "on_trigger_exhausted_3_attempts"})
    return frozenset(_codes)

_INVALIDATION_REASON_CODES: frozenset = _build_invalidation_reason_codes()

_TERMINAL_MATERIALIZATION_OUTCOMES: frozenset[str] = frozenset({
    "TERMINAL_NO_TRADEABLE_CONTRACT",
    "TERMINAL_QUALITY_REJECT",
    "TERMINAL_MATERIALIZATION_FAILED",
    "FAILED_TERMINAL",
})

_RETRY_MATERIALIZATION_OUTCOMES: frozenset[str] = frozenset({
    "RETRY_LATER_DATA_UNAVAILABLE",
    "RETRY_LATER_SELECTOR_BUDGET",
})


def _reason_is_invalidation(reason_code: str) -> bool:
    """PR #324 §4: delegate to classify_watcher_reason for canonical classification.

    stop_* prefix and TERMINAL/ALREADY_BREACHED classes are treated as
    "real invalidation" by the pending-trigger classifier.
    """
    cls = classify_watcher_reason(reason_code)
    return cls in (
        WatcherInvalidationClass.TERMINAL,
        WatcherInvalidationClass.ALREADY_BREACHED,
    )


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


def _coerce_classifier_meta(raw) -> dict:
    """Return a JSONB meta mapping without ever treating malformed data as proof."""
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _persisted_value_is_absent(raw) -> bool:
    """Whether a persisted scalar is genuinely blank/NULL, not merely falsy."""
    return raw is None or (isinstance(raw, str) and not raw.strip())


def _parse_iso_classifier(raw) -> Optional[datetime]:
    """Minimal ISO-8601 parser for the classifier. Returns None on any failure.

    Preserves tzinfo exactly as parsed — callers check for None tzinfo.
    Never raises.
    """
    if not raw:
        return None
    try:
        text = str(raw).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
    except Exception:
        return None


def _active_materialization_proof(meta: dict) -> bool:
    """Return True ONLY when durable, unexpired, current materialization proof exists.

    Binding invariant (PR #521):
      A PENDING_TRIGGER row with a durably proven, current, unexpired deferred
      materialization owner is NOT STUCK_TRIGGER_READY and pending-trigger
      recovery must not terminalize, cancel, rearm, reselect, or advance
      attempt counters for it.

    ALL of the following must hold — ANY single failure returns False (fail-closed):

      lifecycle_state         == "MATERIALIZING"
      materialization_status  == "RUNNING"
      materialization_in_flight  is exactly True (bool, not merely truthy)
      materialization_owner   non-empty string
      materialization_generation positive int (isinstance(bool) rejected as invalid)
      materialization_lease_until parseable, timezone-aware, strictly in the future
      no broker-ready or broker-submit intent, and no materialization outcome
      at either the top level or nested under meta.materialization

    Missing, malformed, expired, stale, or identity-conflicting proof receives
    no protection — existing fail-closed STUCK/recovery behavior remains intact.
    """
    if not isinstance(meta, dict):
        return False

    # A broker-ready/submit-intent marker means ownership has advanced beyond
    # this observer-only state.  Missing broker_ready is tolerated for legacy
    # rows; a present value must be an explicit false value.  Any submit intent
    # or broker submit key is a hard contradiction and receives no protection.
    broker_ready = meta.get("broker_ready")
    if broker_ready is not None and not (
        broker_ready is False
        or (isinstance(broker_ready, str) and broker_ready.strip().lower() == "false")
    ):
        return False
    for intent_key in (
        "submit_intent_at",
        "broker_submit_key",
        "broker_submit_payload_hash",
    ):
        if not _persisted_value_is_absent(meta.get(intent_key)):
            return False

    nested_materialization = meta.get("materialization")
    if nested_materialization is not None and not isinstance(nested_materialization, dict):
        return False

    # A bounded retry outcome may legitimately remain on a row when the next
    # materializer claim transitions it back to RUNNING.  Terminal, submitted,
    # unknown, and malformed outcomes are incompatible with active proof. In
    # particular, do not let a nested materialization.outcome hide a terminal
    # decision from the active-owner fence.
    for outcome_value in (
        meta.get("materialization_outcome"),
        _extract(meta, "materialization.outcome"),
        _extract(meta, "materialization.materialization_outcome"),
    ):
        if _persisted_value_is_absent(outcome_value):
            continue
        outcome_raw = str(outcome_value).strip().upper()
        if outcome_raw not in _RETRY_MATERIALIZATION_OUTCOMES:
            return False

    # lifecycle_state must be exactly MATERIALIZING.
    lifecycle_state = str(meta.get("lifecycle_state") or "").strip().upper()
    if lifecycle_state != "MATERIALIZING":
        return False

    # materialization_status must be exactly RUNNING.
    #
    # Write-order invariant (GC — PR #521 audit round 3):
    # The materializer MUST write materialization_status and
    # materialization_in_flight atomically in a single JSONB update when
    # transitioning from RUNNING to RETRY_PENDING.  If it writes status
    # separately (RUNNING → RETRY_PENDING first, in_flight=False second),
    # there is a window where this proof returns False (status != "RUNNING")
    # while in_flight is still True and the lease is still future.  Recovery
    # would then classify the row as STUCK_TRIGGER_READY and terminalize a
    # live materializer mid-retry.  Violation of this atomicity requirement
    # cannot be detected or corrected here — it must be enforced in the
    # materializer's db write path (ap/deferred_materializer.py).
    mat_status = str(meta.get("materialization_status") or "").strip().upper()
    if mat_status != "RUNNING":
        return False

    # materialization_in_flight must be the bool literal True.
    # Any other value — False, None, 1, "true", non-bool truthy — fails closed.
    in_flight = meta.get("materialization_in_flight")
    if in_flight is not True:
        return False

    # materialization_owner must be a non-empty string.
    owner = meta.get("materialization_owner")
    if not isinstance(owner, str) or not owner.strip():
        return False

    # materialization_generation must be a real positive int — no coercion.
    #
    # #524 writes this field as a PostgreSQL integer, so anything other than
    # a Python int here is a schema anomaly and must not receive protection.
    # bool is rejected even though bool subclasses int (True==1, False==0).
    # Explicit isinstance(int)-and-not-bool test — no int() coercion, no
    # str-to-int, no float-to-int; malformed schema values fail closed at
    # their exact durable shape.
    generation = meta.get("materialization_generation")
    if isinstance(generation, bool) or not isinstance(generation, int):
        return False
    if generation <= 0:
        return False

    # materialization_lease_until must be parseable, timezone-aware, in the future.
    lease_raw = meta.get("materialization_lease_until")
    if not lease_raw:
        return False
    lease_dt = _parse_iso_classifier(lease_raw)
    if lease_dt is None:
        return False
    if lease_dt.tzinfo is None:
        # Timezone-naive lease is rejected — cannot safely compare to UTC now.
        return False
    try:
        now_utc = datetime.now(timezone.utc)
    except Exception:
        return False
    if lease_dt <= now_utc:
        # Expired lease receives no protection.
        return False

    return True


def has_broker_handoff_evidence(row: dict) -> bool:
    """Return True when durable metadata indicates broker ownership advanced.

    This is deliberately separate from ``_active_materialization_proof``:
    failed or contradictory materializer proof must not authorize cleanup.
    Recovery consumers use this predicate to hold an ambiguous row before
    terminalization, rearm, selector work, or another broker attempt.
    """
    if not isinstance(row, dict):
        return False
    meta = _coerce_classifier_meta(row.get("meta"))
    surfaces = [meta]
    nested = meta.get("materialization")
    if isinstance(nested, dict):
        surfaces.append(nested)

    for surface in surfaces:
        broker_ready = surface.get("broker_ready")
        if not _persisted_value_is_absent(broker_ready) and not (
            broker_ready is False
            or (isinstance(broker_ready, str) and broker_ready.strip().lower() == "false")
        ):
            return True
        for key in (
            "submit_intent_at",
            "broker_submit_key",
            "broker_submit_payload_hash",
        ):
            if not _persisted_value_is_absent(surface.get(key)):
                return True
    return False


_TERMINAL_LIFECYCLE_STATES: frozenset[str] = frozenset({
    "CANCELED",
    "EXPIRED",
    "REJECTED",
    "ERROR",
})


def _has_canonical_materialization_retry_candidate(row: dict, meta: dict) -> bool:
    """Recognize a coherent deferred retry shape without claiming ownership.

    This is intentionally weaker than the durable reread verifier in
    ``PendingTriggerRestartRecovery``.  It only prevents a truthful canonical
    ``RETRY_PENDING`` state from being hidden behind the watcher's historical
    ``trigger_ready`` marker.  The recovery engine still proves identity,
    contract, attempt bounds, and the durable row before returning
    ``RETRY_OWNED``.

    Active materializer fields are not used as retry proof. The active
    ``APOrderStateMachine.schedule_deferred_materialization_retry`` handoff
    clears them atomically and writes the mirrored counters/generation consumed
    by startup recovery and the retry CAS. The legacy ``stamp_retry_pending``
    helper has no production call site and is not executable retry authority.
    ``materialization_status=RETRY_PENDING`` remains distinct from the #521
    ``RUNNING`` proof.
    """
    if not isinstance(row, dict) or not isinstance(meta, dict):
        return False

    # The active OSM writer persists retry authority at the top level and may
    # carry an optional nested materialization diagnostics mapping. A malformed
    # nested container is durable corruption, not an absent legacy surface.
    nested_materialization = meta.get("materialization")
    if nested_materialization is not None and not isinstance(
        nested_materialization, dict
    ):
        return False

    if str(meta.get("materialization_status") or "").strip().upper() != "RETRY_PENDING":
        return False

    # The retry owner is only an ENTRY owner and must carry the signal identity
    # that the durable recovery verifier will bind on reread.  Do not let this
    # new trigger_ready preservation branch broaden protection to another row
    # kind or to an identity-less row.
    if str(row.get("kind") or "").strip().upper() != "ENTRY":
        return False
    if not str(row.get("signal_id") or "").strip():
        return False

    # A real OCC contract is not a deferred materialization retry.  Blank is
    # allowed only for the explicit post-#526 blank-deferred ownership shape;
    # the marker prevents arbitrary blank-contract retry metadata from gaining
    # protection.  The durable recovery verifier remains the authority for
    # whether the marked shape can proceed.
    contract = str(row.get("contract") or "").strip().upper()
    if contract and not contract.startswith("DEFERRED:"):
        return False
    if not contract and meta.get("contract_deferred") is not True:
        return False

    # Canonical writer contract: broker_ready is the literal JSON boolean
    # false.  Do not accept string coercions or a missing value as proof.
    if meta.get("broker_ready") is not False:
        return False
    if has_broker_handoff_evidence(row):
        return False

    # The canonical writer stores a real JSON integer attempt count.  Reject
    # bools and string/float coercions so malformed retry metadata cannot
    # outrank a trigger-ready terminal boundary.
    attempts = meta.get("materialization_attempts")
    if isinstance(attempts, bool) or not isinstance(attempts, int) or attempts < 1:
        return False

    # The due executor and OSM CAS fence use the mirrored retry counters and
    # generation, not materialization_attempts alone.  Require the complete
    # durable shape here so a legacy/incomplete row is left on the existing
    # fail-closed stuck path instead of being advertised as RETRY_OWNED.
    retry_attempt = meta.get("retry_attempt")
    breach_attempt_count = meta.get("breach_attempt_count")
    generation = meta.get("materialization_generation")
    max_attempts = meta.get("retry_max_attempts")
    counters = (retry_attempt, breach_attempt_count, generation, max_attempts)
    if any(isinstance(value, bool) or not isinstance(value, int) for value in counters):
        return False
    if retry_attempt != attempts or breach_attempt_count != attempts:
        return False
    if generation < 1 or max_attempts < attempts:
        return False
    try:
        if max_attempts != resolve_deferred_materialization_max_attempts():
            return False
    except (DeferredMaterializationConfigConflict, TypeError, ValueError):
        return False

    reason = str(meta.get("materialization_reason") or "").strip()
    if not reason or not is_retryable_selector_reason(reason):
        return False

    next_retry_at = meta.get("materialization_next_retry_at")
    if _parse_iso_classifier(next_retry_at) is None:
        return False

    if not str(meta.get("materialization_reason") or "").strip():
        return False

    if _parse_iso_classifier(meta.get("materialization_last_failure_at")) is None:
        return False

    # A terminal lifecycle/outcome contradiction is stronger than retry
    # looking fields.  The active-owner fields are deliberately not checked:
    # they are historical/stale fields in the legacy writer shape and are not
    # retry ownership authority.
    lifecycle_state = str(meta.get("lifecycle_state") or "").strip().upper()
    if lifecycle_state in _TERMINAL_LIFECYCLE_STATES:
        return False

    for outcome_value in (
        meta.get("materialization_outcome"),
        _extract(meta, "materialization.outcome"),
        _extract(meta, "materialization.materialization_outcome"),
    ):
        if _persisted_value_is_absent(outcome_value):
            continue
        if str(outcome_value).strip().upper() not in _RETRY_MATERIALIZATION_OUTCOMES:
            return False

    return True


def is_active_materialization_in_flight(row: dict) -> bool:
    """Return whether a pending entry row has a current materializer owner.

    This is the shared read-only fence for callers that do not otherwise need
    the full classifier.  It deliberately checks row-level broker identity as
    well as the durable metadata proof so hydration and cleanup paths cannot
    act on an already-advanced handoff.
    """
    if not isinstance(row, dict):
        return False
    if str(row.get("status") or "").strip().upper() != "PENDING_TRIGGER":
        return False
    if row.get("kind") is not None and str(row.get("kind") or "").strip().upper() != "ENTRY":
        return False
    if not _persisted_value_is_absent(row.get("broker_order_id")):
        return False
    if not _persisted_value_is_absent(row.get("submitted_ts")):
        return False
    return _active_materialization_proof(_coerce_classifier_meta(row.get("meta")))


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
        meta            = _coerce_classifier_meta(row.get("meta"))

        # If broker already accepted or submit already stamped, this is not
        # a lifecycle bug — it's a partially-applied write; not our concern.
        if not _persisted_value_is_absent(broker_order_id) or not _persisted_value_is_absent(submitted_ts):
            return PendingTriggerClassification.NOT_PENDING_TRIGGER

        watcher_reason = str(_extract(meta, "watcher_audit.reason_code") or "").strip()
        materialization_outcome_values = tuple(
            str(value).strip().upper()
            for value in (
                meta.get("materialization_outcome"),
                _extract(meta, "materialization.outcome"),
                _extract(meta, "materialization.materialization_outcome"),
            )
            if not _persisted_value_is_absent(value)
        )
        retry_status = str(
            meta.get("materialization_status")
            or meta.get("retry_status")
            or ""
        ).strip().upper()
        retry_next_at = (
            meta.get("materialization_next_retry_at")
            or _extract(meta, "materialization.next_retry_at")
        )
        restart_rearm_status = str(meta.get("restart_rearm_status") or "").strip().upper()
        restart_rearm_next_at = meta.get("restart_rearm_next_at")

        # ── Priority 1: real watcher invalidation ──
        if watcher_reason == "orphan_no_watcher":
            return PendingTriggerClassification.ORPHAN_NO_WATCHER

        if _reason_is_invalidation(watcher_reason):
            return PendingTriggerClassification.STUCK_INVALIDATED

        # ── Priority 2: terminal materialization outcome ──
        # A terminal writer decision outranks both stale watcher diagnostics
        # and retry-looking fields, including nested legacy outcome surfaces.
        if any(
            outcome in _TERMINAL_MATERIALIZATION_OUTCOMES
            for outcome in materialization_outcome_values
        ):
            return PendingTriggerClassification.STUCK_TERMINAL_MATERIALIZATION

        # ── Priority 3: active materializer / canonical retry ──
        # When watcher_reason is trigger_ready the watcher fired a callback
        # but the broker never accepted.  In LIVE this is normally a zombie.
        # EXCEPTION (PR #521): if a durably proven, current, unexpired
        # deferred materialization owner holds this row, recovery must not
        # terminalize it — the materializer alone resolves the attempt.
        #
        # PR #528: a canonical RETRY_PENDING handoff is a second, distinct
        # owner state.  It must outrank the lower-information trigger_ready
        # diagnostic, but only after the retry shape is coherent; the durable
        # recovery reread remains the stronger RETRY_OWNED authority.
        if watcher_reason == "trigger_ready":
            if is_active_materialization_in_flight(row):
                return PendingTriggerClassification.MATERIALIZATION_IN_FLIGHT
            if _has_canonical_materialization_retry_candidate(row, meta):
                return PendingTriggerClassification.WAITING_RETRYABLE
            return PendingTriggerClassification.STUCK_TRIGGER_READY

        # ── Priority 4: caller-supplied unsafe live-quote signal ──
        if live_quote_already_through_trigger is True:
            return PendingTriggerClassification.UNSAFE_ALREADY_THROUGH_TRIGGER

        # ── Priority 5: past EOD without an active retry ──
        if (
            is_past_eod
            and retry_status not in ("RETRY_PENDING", "RUNNING", "QUEUED")
            and restart_rearm_status != "RETRY_PENDING"
        ):
            return PendingTriggerClassification.STALE_AFTER_EOD

        # ── Priority 6: INVALIDATED_RETRYABLE or INVALIDATED_REARMABLE with active retry → retryable ──
        # PR #324 §4: a retryable reason must never be STUCK_INVALIDATED.
        if (
            retry_status in ("RETRY_PENDING", "RUNNING")
            or retry_next_at
            or restart_rearm_status == "RETRY_PENDING"
            or restart_rearm_next_at
        ):
            return PendingTriggerClassification.WAITING_RETRYABLE

        # ── Priority 7: watcher ownership check (when caller provided it) ──
        if watcher_owned is False:
            return PendingTriggerClassification.ORPHAN_NO_WATCHER
        # If watcher_invalidation_class says RETRYABLE/REARMABLE, row is still waiting.
        _inv_class = str(meta.get("watcher_invalidation_class") or "").strip()
        if _inv_class in ("INVALIDATED_RETRYABLE", "INVALIDATED_REARMABLE"):
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
