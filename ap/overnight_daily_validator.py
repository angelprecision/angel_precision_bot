# ap/overnight_daily_validator.py — Overnight Daily Signal Validator
# =============================================================================
# Enforces The Strat overnight daily rule.
#
# DIRECTIONAL INVALIDATION — locked definition:
#
#   CALL signal: invalidated when session_low_so_far < prior_day_low
#                Rationale: if the market already broke the prior-day low
#                before our entry, the bullish structure is compromised.
#
#   PUT signal:  invalidated when session_high_so_far > prior_day_high
#                Rationale: if the market already broke the prior-day high
#                before our entry, the bearish structure is compromised.
#
# CLIENT-MONEY POLICY:
#   Default is FAIL-CLOSED. Missing prior levels or missing snapshot means the
#   overnight daily signal is invalidated, not allowed through. Set
#   OVERNIGHT_DAILY_FAIL_OPEN=1 only for emergency/paper transitional testing.
#
# IMPORTANT PROFITABILITY NOTE:
#   This file validates daily structure. It does not, by itself, prove that the
#   candle on the selected timeframe has CLOSED directional. That requires a bar
#   history / candle-close data source. The watcher currently confirms breach
#   momentum over multiple quote polls, not a completed candle close.
# =============================================================================

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("ap.overnight_daily_validator")

OVERNIGHT_DAILY_FAIL_OPEN = os.getenv("OVERNIGHT_DAILY_FAIL_OPEN", "0").strip().lower() in {"1", "true", "yes", "on"}


class InvalidationReason:
    PRIOR_HIGH_BREACHED = "INVALIDATED_PRIOR_HIGH_BREACHED"
    PRIOR_LOW_BREACHED = "INVALIDATED_PRIOR_LOW_BREACHED"
    BOTH_SIDES_BREACHED = "INVALIDATED_BOTH_SIDES_BREACHED"
    EXPIRED_NO_TRIGGER = "INVALIDATED_EXPIRED_NO_TRIGGER"
    INVALID_SIDE = "INVALID_SIDE"
    MISSING_PRIOR_LEVELS = "INVALIDATED_MISSING_PRIOR_LEVELS"
    SNAPSHOT_UNAVAILABLE = "INVALIDATED_SNAPSHOT_UNAVAILABLE"


class OvernightWatchState:
    OVERNIGHT_QUEUED = "OVERNIGHT_QUEUED"
    OPEN_RECHECK_PENDING = "OPEN_RECHECK_PENDING"
    VALID_AWAITING_BREACH = "VALID_AWAITING_BREACH"
    INVALIDATED = "INVALIDATED"
    ENTRY_SUBMITTED = "ENTRY_SUBMITTED"
    ENTERED = "ENTERED"
    EXPIRED_NO_TRIGGER = "EXPIRED_NO_TRIGGER"


@dataclass
class ValidationResult:
    valid: bool
    reason_code: str
    reason_text: str
    prior_high: Optional[float] = None
    prior_low: Optional[float] = None
    session_high: Optional[float] = None
    session_low: Optional[float] = None
    side: Optional[str] = None


@dataclass
class MarketSnapshot:
    ticker: str
    session_high_so_far: float
    session_low_so_far: float
    last_price: float
    fetched_at: str


def fetch_market_snapshot(ticker: str, broker) -> Optional[MarketSnapshot]:
    """Fetch session high/low + last from Tradier. Returns None on failure."""
    # Guard: OCC option symbols contain digits after position 4 (e.g. TSLA260516C00180000)
    if len(ticker) > 6 and any(c.isdigit() for c in ticker[4:]):
        log.error(
            "[%s] fetch_market_snapshot called with option symbol — must be underlying equity symbol",
            ticker,
        )
        return None
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
        if not isinstance(quote, dict):
            return None

        session_high = float(quote.get("high") or 0)
        session_low = float(quote.get("low") or 0)
        last = float(quote.get("last") or quote.get("bid") or quote.get("ask") or 0)

        if not session_high or not session_low:
            log.warning("[%s] Snapshot missing high/low", ticker)
            return None

        return MarketSnapshot(
            ticker=ticker,
            session_high_so_far=session_high,
            session_low_so_far=session_low,
            last_price=last,
            fetched_at=datetime.now(timezone.utc).isoformat(),
        )
    except Exception as e:
        log.warning("[%s] Snapshot fetch failed (%s)", ticker, e)
        return None


def _missing_data_result(*, ticker: str, side: str, prior_day_high: Optional[float], prior_day_low: Optional[float], snapshot_missing: bool) -> ValidationResult:
    if OVERNIGHT_DAILY_FAIL_OPEN:
        reason_code = "VALID_MISSING_DATA_FAIL_OPEN"
        reason_text = "Missing overnight daily validation data — fail-open override enabled"
        log.warning("[%s] OVERNIGHT_DAILY_FAIL_OPEN | side=%s prior_high=%s prior_low=%s snapshot_missing=%s", ticker, side, prior_day_high, prior_day_low, snapshot_missing)
        return ValidationResult(True, reason_code, reason_text, prior_day_high, prior_day_low, side=side)

    if not prior_day_high or not prior_day_low:
        reason_code = InvalidationReason.MISSING_PRIOR_LEVELS
        reason_text = "Prior-day high/low missing — fail-closed for client-money safety"
    else:
        reason_code = InvalidationReason.SNAPSHOT_UNAVAILABLE
        reason_text = "Market snapshot unavailable — fail-closed for client-money safety"

    log.error("[%s] OVERNIGHT_DAILY_INVALIDATED | %s | side=%s prior_high=%s prior_low=%s snapshot_missing=%s", ticker, reason_code, side, prior_day_high, prior_day_low, snapshot_missing)
    return ValidationResult(False, reason_code, reason_text, prior_day_high, prior_day_low, side=side)


def validate_overnight_daily_signal(
    *,
    ticker: str,
    side: str,
    prior_day_high: Optional[float],
    prior_day_low: Optional[float],
    snapshot: Optional[MarketSnapshot],
) -> ValidationResult:
    side = (side or "").upper()

    if side not in {"CALL", "PUT"}:
        return ValidationResult(False, InvalidationReason.INVALID_SIDE, f"Unknown side '{side}' — must be CALL or PUT", side=side)

    # Sanity check: prior levels must be real prices, not stubs/zeroes
    _MIN_VALID_PRICE = 0.50
    prior_high_valid = bool(prior_day_high and float(prior_day_high) >= _MIN_VALID_PRICE)
    prior_low_valid  = bool(prior_day_low  and float(prior_day_low)  >= _MIN_VALID_PRICE)
    levels_sane = (
        prior_high_valid
        and prior_low_valid
        and float(prior_day_high) > float(prior_day_low)
    )

    if not levels_sane or snapshot is None:
    return _missing_data_result(
        ticker=ticker,
        side=side,
        prior_day_high=prior_day_high if prior_high_valid else None,
        prior_day_low=prior_day_low if prior_low_valid else None,
        snapshot_missing=snapshot is None,
    )

    sh = snapshot.session_high_so_far
    sl = snapshot.session_low_so_far
    high_breached = sh > prior_day_high
    low_breached = sl < prior_day_low

    if high_breached and low_breached:
        log.info("[%s] OVERNIGHT_DAILY_INVALIDATED | BOTH_SIDES_BREACHED | side=%s | prior_high=%.2f session_high=%.2f | prior_low=%.2f session_low=%.2f", ticker, side, prior_day_high, sh, prior_day_low, sl)
        return ValidationResult(False, InvalidationReason.BOTH_SIDES_BREACHED, f"Both prior-day boundaries breached: session_high {sh:.2f} > prior_high {prior_day_high:.2f} AND session_low {sl:.2f} < prior_low {prior_day_low:.2f}", prior_day_high, prior_day_low, sh, sl, side)

    if side == "CALL":
        if low_breached:
            log.info("[%s] OVERNIGHT_DAILY_INVALIDATED | PRIOR_LOW_BREACHED | side=CALL | session_low=%.2f < prior_low=%.2f", ticker, sl, prior_day_low)
            return ValidationResult(False, InvalidationReason.PRIOR_LOW_BREACHED, f"Session low {sl:.2f} breached prior-day low {prior_day_low:.2f} — CALL setup invalidated", prior_day_high, prior_day_low, sh, sl, side)
        log.info("[%s] OVERNIGHT_DAILY_VALID | side=CALL | prior_low=%.2f session_low=%.2f OK | prior_high=%.2f session_high=%.2f", ticker, prior_day_low, sl, prior_day_high, sh)
        return ValidationResult(True, "VALID", "CALL valid — prior-day low intact, arming for upside breach", prior_day_high, prior_day_low, sh, sl, side)

    if high_breached:
        log.info("[%s] OVERNIGHT_DAILY_INVALIDATED | PRIOR_HIGH_BREACHED | side=PUT | session_high=%.2f > prior_high=%.2f", ticker, sh, prior_day_high)
        return ValidationResult(False, InvalidationReason.PRIOR_HIGH_BREACHED, f"Session high {sh:.2f} breached prior-day high {prior_day_high:.2f} — PUT setup invalidated", prior_day_high, prior_day_low, sh, sl, side)

    log.info("[%s] OVERNIGHT_DAILY_VALID | side=PUT | prior_high=%.2f session_high=%.2f OK | prior_low=%.2f session_low=%.2f", ticker, prior_day_high, sh, prior_day_low, sl)
    return ValidationResult(True, "VALID", "PUT valid — prior-day high intact, arming for downside breach", prior_day_high, prior_day_low, sh, sl, side)


def recheck_overnight_daily(watched, broker) -> ValidationResult:
    signal = getattr(watched, "signal", {}) or {}
    try:
        prior_day_high = float(signal.get("prior_day_high") or 0) or None
    except Exception:
        prior_day_high = None
    try:
        prior_day_low = float(signal.get("prior_day_low") or 0) or None
    except Exception:
        prior_day_low = None
    snapshot = fetch_market_snapshot(watched.ticker, broker)

    return validate_overnight_daily_signal(
        ticker=watched.ticker,
        side=watched.side,
        prior_day_high=prior_day_high,
        prior_day_low=prior_day_low,
        snapshot=snapshot,
    )


def _is_daily_signal(watched) -> bool:
    signal = getattr(watched, "signal", {}) or watched or {}
    if not isinstance(signal, dict):
        signal = getattr(watched, "signal", {}) or {}
    tf = (signal.get("timeframe") or "").upper()
    st = (signal.get("strategy_type") or "").upper()
    return tf in ("1D", "DAILY", "D") or st == "OVERNIGHT_DAILY"
