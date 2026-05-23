"""
PHASE 5: post-cancel retry decision engine.

After an ENTRY order is canceled by the order monitor (typically because the
ask ran past us before we could fill at the original limit), the signal
itself may still be valid: the underlying is still in the right direction,
the spread is still acceptable, and we just didn't pay enough to get filled.
In that case we want to try again \u2014 once, maybe twice \u2014 with a short delay,
not let the opportunity die because of a stale limit.

This module is a pure decision engine. It takes:
  - the canceled order row (dict-shaped)
  - the cancel reason
  - the current underlying spot (or None if unknown)
  - the current ask (or None if unknown)
  - now_ts (for deterministic tests)

and returns a RetryDecision describing whether to ARM, ABORT, or SUBMIT,
plus the wait time, the new attempt count, and a reason code.

Caller is responsible for:
  - actually waiting the wait_secs (asyncio.sleep or scheduler)
  - calling process_signal with a derived retry payload
  - emitting the three log tokens (we provide the values; we don't log)
  - not re-arming an order that already has a position (we check meta but
    the caller also enforces position-table truth)

Retry rules (spec from audit prompt, Phase 5)
---------------------------------------------
  - Max 2 retries per original signal (configurable via ENTRY_RETRY_MAX_ATTEMPTS).
  - Delay 15-30s with jitter (ENTRY_RETRY_DELAY_MIN_SECS, ENTRY_RETRY_DELAY_MAX_SECS).
  - Alignment-gated: underlying must still satisfy the thesis-drift bound
    (re-uses ENTRY_RETRY_ALIGNMENT_DRIFT_PCT, defaults to REPEG_UNDERLYING_DRIFT_PCT
    so submit-time and repeg agree on what 'still aligned' means).
  - SKIP retry when cancel_reason is in NON_RETRYABLE_REASONS. Those reasons
    indicate the signal itself is dead (thesis_invalid, runaway_quote,
    spread_wide, etc) so re-submitting would just re-cancel.

Log tokens the caller must emit (the canonical strings the dashboard parses):
  - ENTRY_RETRY_ARMED      \u2014 after the cancel is confirmed and rules allow retry
  - ENTRY_RETRY_SUBMITTED  \u2014 after process_signal accepts the retry payload
  - ENTRY_RETRY_ABORTED    \u2014 when a retry is gated off (max attempts, alignment, etc)

Module env vars
---------------
  ENTRY_RETRY_MAX_ATTEMPTS        default 2
  ENTRY_RETRY_DELAY_MIN_SECS      default 15
  ENTRY_RETRY_DELAY_MAX_SECS      default 30
  ENTRY_RETRY_ALIGNMENT_DRIFT_PCT default 0.002  (same as REPEG_UNDERLYING_DRIFT_PCT)
  ENTRY_RETRY_ENABLED             default 1
"""

from __future__ import annotations

import os
import random
from dataclasses import dataclass, field
from typing import Optional


# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------

ENTRY_RETRY_MAX_ATTEMPTS        = int(os.getenv("ENTRY_RETRY_MAX_ATTEMPTS", "2"))
ENTRY_RETRY_DELAY_MIN_SECS      = int(os.getenv("ENTRY_RETRY_DELAY_MIN_SECS", "15"))
ENTRY_RETRY_DELAY_MAX_SECS      = int(os.getenv("ENTRY_RETRY_DELAY_MAX_SECS", "30"))
ENTRY_RETRY_ALIGNMENT_DRIFT_PCT = float(os.getenv("ENTRY_RETRY_ALIGNMENT_DRIFT_PCT", "0.002"))
ENTRY_RETRY_ENABLED             = os.getenv("ENTRY_RETRY_ENABLED", "1").strip().lower() in ("1", "true", "yes")


# ----------------------------------------------------------------------
# Cancel reason taxonomy
# ----------------------------------------------------------------------
#
# Reasons that mean THE SIGNAL IS DEAD \u2014 re-submitting would just re-cancel.
# Lower-cased canonical strings; callers normalize before passing in.
NON_RETRYABLE_REASONS = frozenset({
    "thesis_invalid",
    "stale_thesis_call",
    "stale_thesis_put",
    "spread_wide",
    "runaway_quote",
    "runaway_quote_at_submit",
    "positions_full",
    "daily_trade_cap",
    "daily_trade_cap_post_preempt",
    "lost_handoff",
    "lost_handoff_systemic",
    "lost_handoff_systemic_halt",
    "risk_gate_blocked",
    "kill_switch_active",
    "read_only_mode",
    "client_inactive",
})

# Reasons that mean THE SIGNAL IS STILL ALIVE \u2014 we just didn't pay enough.
# This is the explicit allow-list; anything not in either set is treated as
# NON-retryable (fail closed) to avoid accidentally re-arming on a new
# unknown reason.
RETRYABLE_REASONS = frozenset({
    "entry_max_age_normal_reached",
    "entry_max_age_aplus_reached",
    "stale_entry_timeout",
    "missed_move",
    "broker_transient_error",
    "broker_rejected_transient",
    "unfilled_at_ladder_top",
})


# ----------------------------------------------------------------------
# Decision shape
# ----------------------------------------------------------------------

@dataclass
class RetryDecision:
    """Result of evaluate_retry().

    Caller acts on `action`:
      - 'ARM'    \u2014 schedule the retry, wait `wait_secs`, then call process_signal
      - 'ABORT'  \u2014 do NOT retry; log ENTRY_RETRY_ABORTED with `reason_code`
    """
    action: str                          # 'ARM' | 'ABORT'
    reason_code: str                     # canonical token for logging/audit
    explanation: str = ""                # human-readable detail
    wait_secs: float = 0.0               # how long to wait before retry submit
    attempt_number: int = 0              # which retry this would be (1-based)
    max_attempts: int = ENTRY_RETRY_MAX_ATTEMPTS
    retry_payload: dict = field(default_factory=dict)
    # Fields exposed for the dashboard / decision_events:
    cancel_reason_normalized: str = ""
    alignment_ok: Optional[bool] = None
    underlying_spot: Optional[float] = None
    signal_entry_price: Optional[float] = None
    direction: str = ""


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def _normalize_reason(raw: str | None) -> str:
    if not raw:
        return ""
    s = str(raw).strip().lower()
    # Strip optional prefixes the audit tags sometimes carry.
    for prefix in ("cancel_reason:", "reason:", "stale_entry:"):
        if s.startswith(prefix):
            s = s[len(prefix):].strip()
    return s


def _alignment_ok(direction: str, signal_entry: Optional[float], spot: Optional[float],
                  drift_pct: float = ENTRY_RETRY_ALIGNMENT_DRIFT_PCT) -> Optional[bool]:
    """Re-uses the same alignment math as retry_engine.py's gate 3.

    Returns:
      True  \u2014 spot is still on the right side of signal_entry within drift_pct
      False \u2014 spot has drifted past the threshold; thesis is broken
      None  \u2014 we don't have enough info to decide; caller default is ALLOW
              (back-compat for legacy orders missing signal_entry_price)
    """
    direction = (direction or "").upper()
    if direction not in ("CALL", "PUT"):
        return None
    if signal_entry is None or spot is None:
        return None
    try:
        signal_entry = float(signal_entry)
        spot = float(spot)
    except (TypeError, ValueError):
        return None
    if signal_entry <= 0 or spot <= 0:
        return None

    if direction == "CALL":
        floor = signal_entry * (1.0 - drift_pct)
        return spot >= floor
    else:  # PUT
        ceil = signal_entry * (1.0 + drift_pct)
        return spot <= ceil


def _compute_wait_secs(rng: random.Random | None = None) -> float:
    """Uniform jitter inside [DELAY_MIN, DELAY_MAX] (inclusive)."""
    r = rng or random
    lo = float(ENTRY_RETRY_DELAY_MIN_SECS)
    hi = float(ENTRY_RETRY_DELAY_MAX_SECS)
    if hi < lo:
        return lo
    return r.uniform(lo, hi)


# ----------------------------------------------------------------------
# Main decision entry point
# ----------------------------------------------------------------------

def evaluate_retry(
    *,
    canceled_order: dict,
    cancel_reason: str | None,
    underlying_spot: Optional[float] = None,
    rng: random.Random | None = None,
) -> RetryDecision:
    """Decide whether to retry after a cancel.

    Args:
      canceled_order: the order row (dict) including .meta. Must carry:
        - direction        ('CALL' | 'PUT')
        - symbol           (str)  the ticker
        - meta.retry_attempts  (int, default 0)
        - meta.signal_entry_price (float, optional)
        - meta.score       (optional)
      cancel_reason:    free-text reason from the cancel path. Normalized
                        by us.
      underlying_spot:  current spot; if None we ALLOW alignment gate.
      rng:              optional random.Random for deterministic tests.
    """
    if not ENTRY_RETRY_ENABLED:
        return RetryDecision(
            action="ABORT", reason_code="RETRY_DISABLED",
            explanation="ENTRY_RETRY_ENABLED is off",
            cancel_reason_normalized=_normalize_reason(cancel_reason),
        )

    reason_norm = _normalize_reason(cancel_reason)
    meta = (canceled_order.get("meta") or {}) if canceled_order else {}
    direction = str(canceled_order.get("direction") or "").upper()
    signal_entry = meta.get("signal_entry_price")
    if signal_entry in (0, "0", "", None):
        signal_entry = None
    try:
        signal_entry = float(signal_entry) if signal_entry is not None else None
    except (TypeError, ValueError):
        signal_entry = None
    prior_retries = int(meta.get("retry_attempts") or 0)
    next_attempt = prior_retries + 1

    base = dict(
        cancel_reason_normalized=reason_norm,
        underlying_spot=underlying_spot,
        signal_entry_price=signal_entry,
        direction=direction,
        attempt_number=next_attempt,
        max_attempts=ENTRY_RETRY_MAX_ATTEMPTS,
    )

    # \u2500\u2500\u2500 Gate 1: cancel reason \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    if reason_norm in NON_RETRYABLE_REASONS:
        return RetryDecision(
            action="ABORT", reason_code="NON_RETRYABLE_REASON",
            explanation=f"cancel reason '{reason_norm}' indicates dead signal",
            **base,
        )
    if reason_norm not in RETRYABLE_REASONS:
        # Unknown reason: fail closed.
        return RetryDecision(
            action="ABORT", reason_code="UNKNOWN_REASON_FAIL_CLOSED",
            explanation=f"cancel reason '{reason_norm}' not in retryable allow-list",
            **base,
        )

    # \u2500\u2500\u2500 Gate 2: max attempts \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    if next_attempt > ENTRY_RETRY_MAX_ATTEMPTS:
        return RetryDecision(
            action="ABORT", reason_code="MAX_ATTEMPTS_REACHED",
            explanation=f"would be retry #{next_attempt}, max is {ENTRY_RETRY_MAX_ATTEMPTS}",
            **base,
        )

    # \u2500\u2500\u2500 Gate 3: alignment \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    align = _alignment_ok(direction, signal_entry, underlying_spot)
    base["alignment_ok"] = align
    if align is False:
        return RetryDecision(
            action="ABORT", reason_code="ALIGNMENT_LOST",
            explanation=f"underlying spot {underlying_spot} drifted past drift_pct "
                        f"vs signal_entry {signal_entry}",
            **base,
        )
    # align is True or None (None = legacy back-compat: allow)

    # \u2500\u2500\u2500 ALL GATES PASS: ARM \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    wait_secs = _compute_wait_secs(rng=rng)

    # Build a retry payload the caller can drop into process_signal. We carry
    # the original signal context plus our retry counter so the next loop\n    # can compose if it cancels again.
    retry_payload = {
        "signal_id":            meta.get("signal_id") or "",
        "source":               meta.get("source") or "post_cancel_retry",
        "ticker":               canceled_order.get("symbol"),
        "direction":            direction,
        "score":                meta.get("score") or 0,
        "signal_entry_price":   signal_entry,
        "exp_hint":             meta.get("exp_hint") or "DAILY",
        # Retry-specific bookkeeping the caller will persist into meta:
        "retry_attempt":        next_attempt,
        "retry_of_local_oid":   canceled_order.get("local_order_id"),
        "retry_cancel_reason":  reason_norm,
    }

    return RetryDecision(
        action="ARM", reason_code="RETRY_ARMED",
        explanation=(
            f"cancel_reason={reason_norm}; alignment_ok={align}; "
            f"attempt={next_attempt}/{ENTRY_RETRY_MAX_ATTEMPTS}; wait={wait_secs:.1f}s"
        ),
        wait_secs=wait_secs,
        retry_payload=retry_payload,
        **base,
    )
