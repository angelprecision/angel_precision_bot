# ap/overnight_daily_validator.py — Overnight Daily Signal Validator
# =============================================================================
# Enforces The Strat overnight daily rule.
#
# DIRECTIONAL INVALIDATION — locked definition:
#
#   CALL signal: invalidated when session_low_so_far < prior_day_low
#                Rationale: if the market already broke the prior-day low
#                before our entry, the bullish structure is compromised.
#                The daily candle is no longer "waiting to go up" —
#                it already showed downside follow-through.
#
#   PUT signal:  invalidated when session_high_so_far > prior_day_high
#                Rationale: if the market already broke the prior-day high
#                before our entry, the bearish structure is compromised.
#                The daily candle is no longer "waiting to go down" —
#                it already showed upside follow-through.
#
# DATA NOTE — Tradier quote fields used:
#   "high" = session high so far (regular + any premarket if extended enabled)
#   "low"  = session low so far
#   These are labeled session_high_so_far / session_low_so_far here to make
#   explicit they may not be pure extended-hours-only values. Verify field
#   behavior in your Tradier account settings if results seem off.
#
# FAIL OPEN policy:
#   - If snapshot unavailable → log WARNING, keep signal valid
#   - If prior levels missing → log WARNING, keep signal valid for NOW
#     (once signal creation reliably stores prior levels, change to expire)
#
# Integration:
#   1. ap/queue.py       — attach prior_day_high, prior_day_low, timeframe,
#                          strategy_type to the plan before handing to watcher
#   2. ap_entry_watcher.py — call recheck_overnight_daily() in _check_all()
#                            for overnight signals where _is_daily_signal(w)
# =============================================================================

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("ap.overnight_daily_validator")


# =============================================================================
# INVALIDATION REASON CODES
# =============================================================================

class InvalidationReason:
    PRIOR_HIGH_BREACHED = "INVALIDATED_PRIOR_HIGH_BREACHED"
    PRIOR_LOW_BREACHED  = "INVALIDATED_PRIOR_LOW_BREACHED"
    BOTH_SIDES_BREACHED = "INVALIDATED_BOTH_SIDES_BREACHED"
    EXPIRED_NO_TRIGGER  = "INVALIDATED_EXPIRED_NO_TRIGGER"
    INVALID_SIDE        = "INVALID_SIDE"


class OvernightWatchState:
    """
    Dedicated state enum for overnight daily signals.
    These states are stored in w.signal["queue_status"] and are separate
    from WatchState (PENDING/TRIGGERED/EXPIRED/INVALIDATED) which lives
    on the WatchedSignal object itself.
    Use these in dashboard queries and observability.
    """
    OVERNIGHT_QUEUED      = "OVERNIGHT_QUEUED"
    OPEN_RECHECK_PENDING  = "OPEN_RECHECK_PENDING"
    VALID_AWAITING_BREACH = "VALID_AWAITING_BREACH"   # ← explicit armed state
    INVALIDATED           = "INVALIDATED"
    ENTRY_SUBMITTED       = "ENTRY_SUBMITTED"
    ENTERED               = "ENTERED"
    EXPIRED_NO_TRIGGER    = "EXPIRED_NO_TRIGGER"


# =============================================================================
# RESULT
# =============================================================================

@dataclass
class ValidationResult:
    valid:             bool
    reason_code:       str
    reason_text:       str
    prior_high:        Optional[float] = None
    prior_low:         Optional[float] = None
    session_high:      Optional[float] = None   # session_high_so_far
    session_low:       Optional[float] = None   # session_low_so_far
    side:              Optional[str]   = None


# =============================================================================
# MARKET SNAPSHOT
# =============================================================================

@dataclass
class MarketSnapshot:
    """
    Session price data for overnight validity check.
    session_high_so_far / session_low_so_far come from Tradier quote
    fields "high" and "low" which represent the session range so far.
    These include premarket when Tradier extended-hours data is enabled.
    Verify in your account that these reflect the full overnight range.
    """
    ticker:                str
    session_high_so_far:   float
    session_low_so_far:    float
    last_price:            float
    fetched_at:            str


def fetch_market_snapshot(ticker: str, broker) -> Optional[MarketSnapshot]:
    """
    Fetch session high/low + last from Tradier.
    Returns None on any failure — caller fails open.
    """
    try:
        base_url = (
            getattr(broker, "base_url", None)
            or getattr(getattr(broker, "cfg", None), "base_url", None)
            or "https://sandbox.tradier.com"
        )
        resp = broker.session.get(
            f"{base_url}/v1/markets/quotes",
            params={"symbols": ticker, "greeks": "false"},
            headers={"Accept": "application/json"},
            timeout=8,
        )
        if resp.status_code != 200:
            log.warning("[%s] Snapshot fetch HTTP %d", ticker, resp.status_code)
            return None

        raw = resp.json().get("quotes", {}).get("quote", {})
        quote = raw[0] if isinstance(raw, list) and raw else raw

        session_high = float(quote.get("high") or 0)
        session_low  = float(quote.get("low")  or 0)
        last         = float(quote.get("last") or quote.get("bid") or 0)

        if not session_high or not session_low:
            log.warning("[%s] Snapshot missing high/low — fail open", ticker)
            return None

        return MarketSnapshot(
            ticker=ticker,
            session_high_so_far=session_high,
            session_low_so_far=session_low,
            last_price=last,
            fetched_at=datetime.now(timezone.utc).isoformat(),
        )
    except Exception as e:
        log.warning("[%s] Snapshot fetch failed (%s) — fail open", ticker, e)
        return None


# =============================================================================
# CORE VALIDATOR
# =============================================================================

def validate_overnight_daily_signal(
    *,
    ticker:         str,
    side:           str,
    prior_day_high: Optional[float],
    prior_day_low:  Optional[float],
    snapshot:       Optional[MarketSnapshot],
) -> ValidationResult:
    """
    The Strat overnight daily structural validity check.

    See module docstring for exact directional definitions.
    Fails OPEN on missing data to preserve signal rather than silently kill it.
    """
    side = (side or "").upper()

    # ── Missing prior levels ──────────────────────────────────────────────────
    # Log as WARNING — once signal creation reliably stores these fields,
    # consider changing to expire instead of fail open.
    if not prior_day_high or not prior_day_low:
        log.warning(
            "[%s] OVERNIGHT_DAILY_RECHECK | prior levels not stored — fail open "
            "| prior_high=%s prior_low=%s | side=%s",
            ticker, prior_day_high, prior_day_low, side,
        )
        # TODO: once signal creation reliably stores prior_day_high/low,
        # change this to return valid=False with reason_code=InvalidationReason.EXPIRED_NO_TRIGGER
        # For now, fail open to avoid silently killing signals during transition period.
        return ValidationResult(
            valid=True,
            reason_code="VALID_MISSING_LEVELS_FAIL_OPEN",
            reason_text="Prior-day levels not stored — failing open (fix signal creation, then harden)",
            prior_high=prior_day_high,
            prior_low=prior_day_low,
            side=side,
        )

    # ── No snapshot ───────────────────────────────────────────────────────────
    if snapshot is None:
        log.warning(
            "[%s] OVERNIGHT_DAILY_RECHECK | snapshot unavailable — fail open "
            "| prior_high=%.2f prior_low=%.2f | side=%s",
            ticker, prior_day_high, prior_day_low, side,
        )
        return ValidationResult(
            valid=True,
            reason_code="VALID_SNAPSHOT_UNAVAILABLE_FAIL_OPEN",
            reason_text="Session data unavailable — failing open to preserve signal",
            prior_high=prior_day_high,
            prior_low=prior_day_low,
            side=side,
        )

    sh = snapshot.session_high_so_far
    sl = snapshot.session_low_so_far

    high_breached = sh > prior_day_high
    low_breached  = sl < prior_day_low

    # ── Both sides breached ───────────────────────────────────────────────────
    if high_breached and low_breached:
        log.info(
            "[%s] OVERNIGHT_DAILY_INVALIDATED | BOTH_SIDES_BREACHED | side=%s "
            "| prior_high=%.2f session_high=%.2f | prior_low=%.2f session_low=%.2f",
            ticker, side, prior_day_high, sh, prior_day_low, sl,
        )
        return ValidationResult(
            valid=False,
            reason_code=InvalidationReason.BOTH_SIDES_BREACHED,
            reason_text=(
                f"Both prior-day boundaries breached: "
                f"session_high {sh:.2f} > prior_high {prior_day_high:.2f} AND "
                f"session_low {sl:.2f} < prior_low {prior_day_low:.2f}"
            ),
            prior_high=prior_day_high, prior_low=prior_day_low,
            session_high=sh, session_low=sl, side=side,
        )

    # ── CALL: invalid if session low breached prior-day low ───────────────────
    if side == "CALL":
        if low_breached:
            log.info(
                "[%s] OVERNIGHT_DAILY_INVALIDATED | PRIOR_LOW_BREACHED | side=CALL "
                "| session_low=%.2f < prior_low=%.2f — bullish structure compromised",
                ticker, sl, prior_day_low,
            )
            return ValidationResult(
                valid=False,
                reason_code=InvalidationReason.PRIOR_LOW_BREACHED,
                reason_text=(
                    f"Session low {sl:.2f} breached prior-day low {prior_day_low:.2f} "
                    f"— CALL setup invalidated (daily already showed downside)"
                ),
                prior_high=prior_day_high, prior_low=prior_day_low,
                session_high=sh, session_low=sl, side=side,
            )
        log.info(
            "[%s] OVERNIGHT_DAILY_VALID | side=CALL | prior_low=%.2f session_low=%.2f OK "
            "| prior_high=%.2f session_high=%.2f | arming for breach",
            ticker, prior_day_low, sl, prior_day_high, sh,
        )
        return ValidationResult(
            valid=True, reason_code="VALID",
            reason_text="CALL valid — prior-day low intact, arming for upside breach",
            prior_high=prior_day_high, prior_low=prior_day_low,
            session_high=sh, session_low=sl, side=side,
        )

    # ── PUT: invalid if session high breached prior-day high ──────────────────
    if side == "PUT":
        if high_breached:
            log.info(
                "[%s] OVERNIGHT_DAILY_INVALIDATED | PRIOR_HIGH_BREACHED | side=PUT "
                "| session_high=%.2f > prior_high=%.2f — bearish structure compromised",
                ticker, sh, prior_day_high,
            )
            return ValidationResult(
                valid=False,
                reason_code=InvalidationReason.PRIOR_HIGH_BREACHED,
                reason_text=(
                    f"Session high {sh:.2f} breached prior-day high {prior_day_high:.2f} "
                    f"— PUT setup invalidated (daily already showed upside)"
                ),
                prior_high=prior_day_high, prior_low=prior_day_low,
                session_high=sh, session_low=sl, side=side,
            )
        log.info(
            "[%s] OVERNIGHT_DAILY_VALID | side=PUT | prior_high=%.2f session_high=%.2f OK "
            "| prior_low=%.2f session_low=%.2f | arming for breach",
            ticker, prior_day_high, sh, prior_day_low, sl,
        )
        return ValidationResult(
            valid=True, reason_code="VALID",
            reason_text="PUT valid — prior-day high intact, arming for downside breach",
            prior_high=prior_day_high, prior_low=prior_day_low,
            session_high=sh, session_low=sl, side=side,
        )

    return ValidationResult(
        valid=False,
        reason_code=InvalidationReason.INVALID_SIDE,
        reason_text=f"Unknown side '{side}' — must be CALL or PUT",
        side=side,
    )


# =============================================================================
# WATCHER INTEGRATION
# =============================================================================

def recheck_overnight_daily(watched, broker) -> ValidationResult:
    """
    Called from APEntryWatcher._check_all() for overnight daily signals.
    Fetches live snapshot and runs structural validity check.

    EXACT WIRING in _check_all() overnight revalidation loop:

        from ap.overnight_daily_validator import recheck_overnight_daily, _is_daily_signal

        for w in overnight_active:
            if _is_daily_signal(w):
                result = recheck_overnight_daily(w, self.broker)
                if not result.valid:
                    w.state = WatchState.INVALIDATED
                    _to_expire_overnight.append(w)
                else:
                    # Set explicit armed state — do NOT just flip w.overnight=False
                    # This keeps the queue inspectable and truthful
                    w.overnight = False          # promote to same-day active watcher
                    # Use explicit state constant — easier to grep and dashboard-query
                    w.signal["queue_status"] = OvernightWatchState.VALID_AWAITING_BREACH
                    log.info("[%s] OVERNIGHT_DAILY_ARMED | side=%s | queue_status=%s | %s",
                             w.ticker, w.side,
                             OvernightWatchState.VALID_AWAITING_BREACH,
                             result.reason_text)
                    # TODO tomorrow morning: grep logs for OVERNIGHT_DAILY_VALID and verify
                    # that session_high/session_low values match what you see on your chart.
                    # Tradier "high"/"low" quote fields should reflect premarket range if
                    # extended hours are enabled on your account. Confirm once before trusting.
                continue  # skip generic stale/drift check below

            # existing generic intraday stale check continues here...
    """
    signal         = getattr(watched, "signal", {}) or {}
    prior_day_high = float(signal.get("prior_day_high") or 0) or None
    prior_day_low  = float(signal.get("prior_day_low")  or 0) or None
    snapshot       = fetch_market_snapshot(watched.ticker, broker)

    return validate_overnight_daily_signal(
        ticker=watched.ticker,
        side=watched.side,
        prior_day_high=prior_day_high,
        prior_day_low=prior_day_low,
        snapshot=snapshot,
    )


def _is_daily_signal(watched) -> bool:
    """Returns True if this is a daily timeframe overnight signal."""
    signal = getattr(watched, "signal", {}) or {}
    tf = (signal.get("timeframe") or "").upper()
    st = (signal.get("strategy_type") or "").upper()
    return tf in ("1D", "DAILY", "D") or st == "OVERNIGHT_DAILY"
