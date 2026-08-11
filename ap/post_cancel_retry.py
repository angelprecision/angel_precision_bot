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
import re
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
    """BUG-D FIX (PR #29): normalize a free-text cancel reason to a canonical
    retry-engine token.

    The cancel paths in order_monitor produce sentences like:
        'STALE_ENTRY_CANCEL MISSED_MOVE — limit=$3.08 current=$3.30 ...'
        'ENTRY_MAX_AGE_NORMAL_REACHED unfilled at 90s ...'
        'ENTRY_MAX_AGE_APLUS_REACHED unfilled at 120s ...'
        'STALE_ENTRY_CANCEL ...'

    Prior shape only stripped a leading prefix and lowercased, so none of
    these matched RETRYABLE_REASONS / NON_RETRYABLE_REASONS. Effect: every
    cancel hit UNKNOWN_REASON_FAIL_CLOSED and no retry ever armed.

    Matching rules (priority order, first hit wins):
      contains 'ENTRY_MAX_AGE_NORMAL_REACHED' -> 'entry_max_age_normal_reached'
      contains 'ENTRY_MAX_AGE_APLUS_REACHED'  -> 'entry_max_age_aplus_reached'
      contains 'MISSED_MOVE'                  -> 'missed_move'
      contains 'STALE_ENTRY_TIMEOUT'
        or     'STALE_ENTRY_CANCEL'           -> 'stale_entry_timeout'
      contains 'RUNAWAY_QUOTE_AT_SUBMIT'      -> 'runaway_quote_at_submit'
      contains 'RUNAWAY_QUOTE'                -> 'runaway_quote'
      contains 'THESIS_INVALID'               -> 'thesis_invalid'
      contains 'SPREAD_WIDE'                  -> 'spread_wide'
      contains 'POSITIONS_FULL'               -> 'positions_full'
      contains 'LOST_HANDOFF'                 -> 'lost_handoff'
      contains 'RISK_GATE_BLOCKED'            -> 'risk_gate_blocked'
      contains 'KILL_SWITCH'                  -> 'kill_switch_active'
      contains 'BROKER_TRANSIENT'             -> 'broker_transient_error'
      contains 'UNFILLED_AT_LADDER_TOP'       -> 'unfilled_at_ladder_top'
      contains 'DAILY_TRADE_CAP'              -> 'daily_trade_cap'

    Anything that does not match returns the prefix-stripped lowercased
    string AS-IS so callers can still recognize legacy canonical tokens.
    Unknown sentences land in evaluate_retry's UNKNOWN_REASON_FAIL_CLOSED.
    """
    if not raw:
        return ""
    s = str(raw).strip()
    # Strip optional prefixes the audit tags sometimes carry.
    s_lower = s.lower()
    for prefix in ("cancel_reason:", "reason:", "stale_entry:"):
        if s_lower.startswith(prefix):
            s = s[len(prefix):].strip()
            s_lower = s.lower()

    s_upper = s.upper()

    # Specific tokens first so 'ENTRY_MAX_AGE_NORMAL_REACHED ...' wins over
    # the generic 'STALE_ENTRY_CANCEL' fallback.
    if "ENTRY_MAX_AGE_NORMAL_REACHED" in s_upper:
        return "entry_max_age_normal_reached"
    if "ENTRY_MAX_AGE_APLUS_REACHED" in s_upper:
        return "entry_max_age_aplus_reached"
    if "MISSED_MOVE" in s_upper:
        return "missed_move"
    if "RUNAWAY_QUOTE_AT_SUBMIT" in s_upper:
        return "runaway_quote_at_submit"
    if "RUNAWAY_QUOTE" in s_upper:
        return "runaway_quote"
    if "THESIS_INVALID" in s_upper:
        return "thesis_invalid"
    if "SPREAD_WIDE" in s_upper:
        return "spread_wide"
    if "POSITIONS_FULL" in s_upper:
        return "positions_full"
    if "LOST_HANDOFF" in s_upper:
        return "lost_handoff"
    if "RISK_GATE_BLOCKED" in s_upper:
        return "risk_gate_blocked"
    if "KILL_SWITCH" in s_upper:
        return "kill_switch_active"
    if "BROKER_TRANSIENT" in s_upper:
        return "broker_transient_error"
    if "UNFILLED_AT_LADDER_TOP" in s_upper:
        return "unfilled_at_ladder_top"
    if "DAILY_TRADE_CAP" in s_upper:
        return "daily_trade_cap"
    # Generic stale-entry tokens last so MISSED_MOVE wins.
    if "STALE_ENTRY_TIMEOUT" in s_upper or "STALE_ENTRY_CANCEL" in s_upper:
        return "stale_entry_timeout"

    # Truly unknown reasons: return prefix-stripped lowercased form.
    # evaluate_retry will fail closed with UNKNOWN_REASON_FAIL_CLOSED.
    return s_lower


import re as _re

# OCC option symbols end in 8-digit strike * 1000, e.g.
#   'QCOM260523C00185000' -> strike 185.000
# We trust the last 8 chars as the strike-block; the format is rigid.
_OCC_STRIKE_RE = _re.compile(r"([CP])(\d{8})$")


def _resolve_strike(canceled_order: dict, meta: dict) -> Optional[float]:
    """BUG-C FIX (PR #29): resolve trigger.strike from any of four sources.

    Priority:
      1. meta.trigger.strike  (the original signal's trigger)
      2. meta.strike          (a flat carry sometimes set during admission)
      3. canceled_order.strike (legacy column)
      4. OCC parse: take last (C|P)NNNNNNNN of the contract and / 1000

    Returns a positive float, or None if nothing yields one.
    """
    # 1) meta.trigger.strike
    try:
        t = meta.get("trigger") if isinstance(meta.get("trigger"), dict) else None
        if t and t.get("strike") is not None:
            v = float(t["strike"])
            if v > 0:
                return v
    except (TypeError, ValueError):
        pass

    # 2) meta.strike
    try:
        if meta.get("strike") is not None:
            v = float(meta["strike"])
            if v > 0:
                return v
    except (TypeError, ValueError):
        pass

    # 3) canceled_order.strike (legacy)
    try:
        if canceled_order.get("strike") is not None:
            v = float(canceled_order["strike"])
            if v > 0:
                return v
    except (TypeError, ValueError):
        pass

    # 4) OCC parse from contract
    contract = canceled_order.get("contract")
    if isinstance(contract, str) and contract:
        m = _OCC_STRIKE_RE.search(contract.strip().upper())
        if m:
            try:
                strike_raw = int(m.group(2))
                v = strike_raw / 1000.0
                if v > 0:
                    return v
            except (TypeError, ValueError):
                pass

    return None


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


class RetryAttemptCounterError(ValueError):
    """Raised when durable post-cancel retry accounting is not trustworthy."""


def parse_retry_attempt_count(meta: dict | None) -> int:
    """Return the monotonic durable retry count across old and new keys.

    ``retry_attempts`` is the canonical producer/consumer contract.  The
    singular ``retry_attempt`` key was written by the original monitor and is
    accepted only as a legacy source.  When both are present, the larger value
    wins so a stale compatibility mirror can never move the counter backward.

    Values must be JSON integers or canonical unsigned decimal strings.  Booleans,
    floats, whitespace-padded strings, negative values, and other malformed
    shapes fail closed instead of silently resetting the retry budget to zero.
    """
    if meta is None:
        return 0
    if not isinstance(meta, dict):
        raise RetryAttemptCounterError("retry metadata is not an object")

    parsed: list[int] = []
    for key in ("retry_attempts", "retry_attempt"):
        if key not in meta or meta.get(key) is None:
            continue
        raw = meta.get(key)
        if isinstance(raw, bool):
            raise RetryAttemptCounterError(f"{key} must be a non-negative integer")
        if isinstance(raw, int):
            value = raw
        elif isinstance(raw, str) and re.fullmatch(r"(?:0|[1-9][0-9]*)", raw):
            value = int(raw)
        else:
            raise RetryAttemptCounterError(f"{key} must be a canonical non-negative integer")
        if value < 0:
            raise RetryAttemptCounterError(f"{key} must not be negative")
        parsed.append(value)

    return max(parsed, default=0)


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
    canceled_order = canceled_order or {}
    direction = str(canceled_order.get("direction") or "").upper()
    raw_meta = canceled_order.get("meta")
    if raw_meta is None:
        meta = {}
    elif isinstance(raw_meta, dict):
        meta = raw_meta
    else:
        return RetryDecision(
            action="ABORT",
            reason_code="MALFORMED_RETRY_METADATA",
            explanation="retry metadata is not an object",
            cancel_reason_normalized=reason_norm,
            direction=direction,
            max_attempts=ENTRY_RETRY_MAX_ATTEMPTS,
        )
    signal_entry = meta.get("signal_entry_price")
    if signal_entry in (0, "0", "", None):
        signal_entry = None
    try:
        signal_entry = float(signal_entry) if signal_entry is not None else None
    except (TypeError, ValueError):
        signal_entry = None
    try:
        prior_retries = parse_retry_attempt_count(meta)
    except RetryAttemptCounterError as exc:
        return RetryDecision(
            action="ABORT",
            reason_code="MALFORMED_RETRY_ATTEMPT_COUNT",
            explanation=str(exc),
            cancel_reason_normalized=reason_norm,
            underlying_spot=underlying_spot,
            signal_entry_price=signal_entry,
            direction=direction,
            attempt_number=0,
            max_attempts=ENTRY_RETRY_MAX_ATTEMPTS,
        )
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

    # === GATE 4 (BUG-C FIX, PR #29): strike must be resolvable ============
    # process_signal requires trigger.strike (NOT signal_entry_price).
    # We try four sources in priority order:
    #   1. meta.trigger.strike
    #   2. meta.strike
    #   3. canceled_order.strike
    #   4. parsed from OCC contract last-8-digits / 1000
    # If none yield a positive float, ABORT with RETRY_MISSING_TRIGGER_STRIKE
    # so we never hand process_signal a payload it will reject as missing_strike.
    strike = _resolve_strike(canceled_order, meta)
    if strike is None:
        return RetryDecision(
            action="ABORT", reason_code="RETRY_MISSING_TRIGGER_STRIKE",
            explanation=(
                "cannot resolve trigger.strike from meta.trigger.strike / "
                "meta.strike / canceled_order.strike / contract OCC parse"
            ),
            **base,
        )

    # BUG-C FIX (PR #29): the symbol used by process_signal is the
    # canonical UNDERLYING ticker, not the OCC option contract. We pull it
    # from canceled_order.symbol (canonical) and fall back to meta.ticker.
    symbol = (
        (canceled_order.get("symbol") or "").strip().upper()
        or (meta.get("ticker") or "").strip().upper()
    )
    if not symbol:
        return RetryDecision(
            action="ABORT", reason_code="RETRY_MISSING_SYMBOL",
            explanation="cannot resolve underlying symbol from order or meta",
            **base,
        )

    # === ALL GATES PASS: ARM ==============================================
    wait_secs = _compute_wait_secs(rng=rng)

    # PR fix/entry-retry-context-and-submit-refresh:
    # The previous build read `meta.get(...)` only — which silently degraded
    # score=70/tier=B/signal_id=REEVAL:... entries into score=0 / signal_id=""
    # / signal_entry_price=None on rows whose meta JSON had been overwritten
    # by retry-engine stamps (retry_status, retry_abort_ts, ...). The
    # downstream submit then hit submit_reject:trend_gate / time_gate with
    # a useless audit trail ("why did this retry fail? score=0").
    #
    # Read priority for every field is:
    #   1) top-level canceled_order.<col>   (PR #44 ground truth)
    #   2) meta.<key>                       (older paths / nested copy)
    #   3) meta.retry_payload.<key>         (previous retry attempt mirror)
    #   4) safe default                     (only if genuinely missing)
    _prev_rp = (meta or {}).get("retry_payload") or {}
    if not isinstance(_prev_rp, dict):
        _prev_rp = {}

    def _coalesce(*vals, default=None):
        """Return the first value that is not None / not empty string / not 0
        unless 0 is genuine. For score/numeric fields we use the
        `_coalesce_numeric` variant below; this one is for identity fields
        (signal_id, contract, ticker, etc.) where '' is treated as missing."""
        for v in vals:
            if v is None:
                continue
            if isinstance(v, str) and not v.strip():
                continue
            return v
        return default

    def _coalesce_numeric(*vals, default=0.0):
        """Return the first value that parses to a float (None / unparseable
        are skipped). 0 is a legitimate value when it is the ONLY value the
        sources carry — see _coalesce_positive for score/price-style fields
        where 0 should be treated as 'missing' so a real positive elsewhere
        wins."""
        for v in vals:
            if v is None:
                continue
            try:
                return float(v)
            except (TypeError, ValueError):
                continue
        return default

    def _coalesce_positive(*vals, default=0.0):
        """Return the first value that parses to a POSITIVE float (>0).
        Treats 0 / None / unparseable as 'missing'. Used for score,
        signal_entry_price, trigger_price — fields where 0 is functionally
        equivalent to missing and a real non-zero value from a later source
        should win.

        Priority rule per operator (2026-05-27):
          score = top_level if top_level > 0
                  else meta_score if meta_score > 0
                  else prev_retry_score if prev_retry_score > 0
                  else 0
        """
        for v in vals:
            if v is None:
                continue
            try:
                f = float(v)
            except (TypeError, ValueError):
                continue
            if f > 0:
                return f
        return default

    # Score: top-level column wins ONLY if positive. Per operator rule,
    # a row with literal score=0 should not lock out a real meta.score=70
    # or a previous retry_payload.score=70. Critical for not degrading
    # winners (today's GOOGL pattern: row had score=70, meta got overwritten
    # to retry_status-only — we still want the 70 surfaced).
    score_preserved = _coalesce_positive(
        canceled_order.get("score"),
        meta.get("score"),
        _prev_rp.get("score"),
        default=0.0,
    )
    tier_preserved = _coalesce(
        canceled_order.get("tier"),
        meta.get("tier"),
        _prev_rp.get("tier"),
        default="",
    )
    signal_id_preserved = _coalesce(
        canceled_order.get("signal_id"),
        meta.get("signal_id"),
        _prev_rp.get("signal_id"),
        default="",
    )
    plan_id_preserved = _coalesce(
        canceled_order.get("plan_id"),
        meta.get("plan_id"),
        _prev_rp.get("plan_id"),
        default=None,
    )
    selected_contract_preserved = _coalesce(
        canceled_order.get("contract"),
        meta.get("selected_contract"),
        meta.get("contract"),
        _prev_rp.get("selected_contract"),
        _prev_rp.get("contract"),
        default=None,
    )
    pattern_preserved = _coalesce(
        canceled_order.get("pattern"),
        meta.get("pattern"),
        _prev_rp.get("pattern"),
        default=None,
    )
    timeframe_preserved = _coalesce(
        canceled_order.get("timeframe"),
        meta.get("timeframe"),
        meta.get("exp_hint"),
        _prev_rp.get("exp_hint"),
        default="DAILY",
    )
    # Underlying price priority: top-level trigger_price > meta.signal_entry_price
    # > meta.trigger.underlying_price > prev retry. Used for alignment.
    # Same positive-wins rule as score: 0 is functionally missing.
    underlying_price_preserved = _coalesce_positive(
        canceled_order.get("trigger_price"),
        meta.get("signal_entry_price"),
        (meta.get("trigger") or {}).get("underlying_price")
            if isinstance(meta.get("trigger"), dict) else None,
        _prev_rp.get("signal_entry_price"),
        (_prev_rp.get("trigger") or {}).get("underlying_price")
            if isinstance(_prev_rp.get("trigger"), dict) else None,
        default=0.0,
    )
    # If we couldn't recover an underlying price, leave the trigger.underlying_price
    # explicitly None so process_signal knows to refresh it from a fresh quote
    # instead of using a stale 0.0.
    underlying_price_for_payload = (
        float(underlying_price_preserved) if underlying_price_preserved > 0
        else (float(signal_entry) if signal_entry else None)
    )

    # BUG-C FIX (PR #29): payload now matches process_signal's required
    # shape: symbol (not ticker) and trigger.strike (not signal_entry_price).
    # Legacy alias 'ticker' is preserved so any consumer still keyed on it
    # keeps working.
    retry_payload = {
        "signal_id":            signal_id_preserved,
        "plan_id":              plan_id_preserved,
        "source":               meta.get("source") or "post_cancel_retry",
        "symbol":               symbol,
        "ticker":               symbol,                 # legacy alias
        "direction":            direction,
        "score":                score_preserved,
        "tier":                 tier_preserved,
        "selected_contract":    selected_contract_preserved,
        "contract":             selected_contract_preserved,  # legacy alias
        "pattern":              pattern_preserved,
        "exp_hint":             timeframe_preserved,
        "timeframe":            timeframe_preserved,
        "signal_entry_price":   underlying_price_for_payload
                                if underlying_price_for_payload is not None
                                else signal_entry,
        "trigger":              {
            "strike":            float(strike),
            # Carry the preserved entry price as underlying_price so
            # _resolve_option_contract has its alignment reference. If we
            # couldn't recover one, send None so submit refreshes from quote.
            "underlying_price":  underlying_price_for_payload,
        },
        # Retry-specific bookkeeping the caller will persist into meta:
        "retry_attempts":       next_attempt,
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
