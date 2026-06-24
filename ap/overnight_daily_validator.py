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
import requests  # PR #181: Polygon snapshot — no broker dependency
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("ap.overnight_daily_validator")

OVERNIGHT_DAILY_FAIL_OPEN = os.getenv("OVERNIGHT_DAILY_FAIL_OPEN", "0").strip().lower() in {"1", "true", "yes", "on"}

# PR #181: Polygon — canonical daily-bar data source for overnight validation.
# Using the same data provider as the scanner so prior_day_high/prior_day_low
# (scanner-set on the signal) and today's session H/L come from the same source.
# Polygon `day.h` / `day.l` on the snapshot endpoint are the regular-session
# daily bar H/L — exactly what The Strat overnight rule evaluates against.
POLYGON_API_KEY        = os.getenv("POLYGON_API_KEY", "")
POLYGON_SNAPSHOT_BASE  = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"


# ---------------------------------------------------------------------------
# PR #111 amend: market-data base URL resolution.
# Overnight validation is market-data work, NOT order submission.
# Must always use the live endpoint regardless of runtime execution mode.
# Resolution order:
#   1. broker.market_data_base_url
#   2. broker.quote_base_url
#   3. broker.cfg.market_data_base_url
#   4. env TRADIER_MARKET_DATA_BASE_URL
#   5. env TRADIER_DATA_BASE_URL
#   6. https://api.tradier.com  — hard-coded live market-data fallback
# ---------------------------------------------------------------------------
_LIVE_MARKET_DATA_BASE_URL = "https://api.tradier.com"


def _resolve_market_data_base_url(broker) -> str:
    """Return the live Tradier market-data base URL for this broker.

    Every candidate URL is sanitised before it is returned. If a candidate
    contains "sandbox.tradier.com" it is skipped with a warning and the
    resolver falls through to the next candidate. This prevents a
    misconfigured broker attribute or env var from silently routing overnight
    validation to stale sandbox market data.
    """

    def _accept(url: str, source: str) -> "str | None":
        clean = url.strip().rstrip("/")
        if not clean:
            return None
        if "sandbox.tradier.com" in clean.lower():
            log.warning(
                "OVERNIGHT_MARKET_DATA_SANDBOX_URL_IGNORED source=%s value=%s"
                " -- using live fallback instead",
                source, clean,
            )
            return None
        return clean

    # 1. broker.market_data_base_url / broker.quote_base_url
    for attr in ("market_data_base_url", "quote_base_url"):
        raw = getattr(broker, attr, None)
        if raw and isinstance(raw, str):
            accepted = _accept(raw, f"broker.{attr}")
            if accepted:
                return accepted

    # 2. broker.cfg.market_data_base_url
    cfg = getattr(broker, "cfg", None)
    if cfg is not None:
        raw = getattr(cfg, "market_data_base_url", None)
        if raw and isinstance(raw, str):
            accepted = _accept(raw, "broker.cfg.market_data_base_url")
            if accepted:
                return accepted

    # 3. env overrides
    for env_key in ("TRADIER_MARKET_DATA_BASE_URL", "TRADIER_DATA_BASE_URL"):
        raw = os.getenv(env_key, "").strip()
        if raw:
            accepted = _accept(raw, f"env.{env_key}")
            if accepted:
                return accepted

    # 4. Hard-coded live fallback
    return _LIVE_MARKET_DATA_BASE_URL


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


def _fetch_polygon_daily_snapshot(ticker: str) -> "dict | None":
    """
    PR #181: Fetch today's daily-bar high/low and last price from Polygon.

    Endpoint: GET /v2/snapshot/locale/us/markets/stocks/tickers/{ticker}
    Returns dict(day_high, day_low, last_price) or None (→ RETRY_LATER).

    Why Polygon and not Tradier timesales:
      - Tradier timesales uses broker.session (paper accounts carry the
        sandbox bearer token which is rejected by api.tradier.com → HTTP 401).
      - Polygon uses a simple API key — no broker dependency, works for
        paper and live accounts identically.
      - Polygon `day.h` / `day.l` are the daily-bar H/L, exactly the same
        reference frame as `prior_day_high` / `prior_day_low` that the
        scanner writes to ap_signals. Consistent data source throughout.

    Returns None when:
      - POLYGON_API_KEY is not set
      - HTTP error from Polygon
      - day.h or day.l are 0/missing (market not yet open or no data)
    """
    if not POLYGON_API_KEY:
        log.error(
            "[%s] OVERNIGHT_POLYGON_KEY_MISSING — POLYGON_API_KEY not set; "
            "snapshot unavailable → RETRY_LATER",
            ticker,
        )
        return None
    try:
        resp = requests.get(
            f"{POLYGON_SNAPSHOT_BASE}/{ticker}",
            params={"apiKey": POLYGON_API_KEY},
            timeout=8,
        )
        if resp.status_code != 200:
            log.warning("[%s] Polygon snapshot HTTP %d", ticker, resp.status_code)
            return None
        tkr  = resp.json().get("ticker") or {}
        day  = tkr.get("day") or {}
        last = tkr.get("lastTrade") or {}
        day_high  = float(day.get("h") or 0)
        day_low   = float(day.get("l") or 0)
        last_price = float(
            last.get("p") or day.get("c") or day.get("vw") or 0
        )
        if not day_high or not day_low:
            log.warning(
                "OVERNIGHT_POLYGON_SNAPSHOT_NO_HILO ticker=%s day_h=%s day_l=%s "
                "— session not yet started or data unavailable → RETRY_LATER",
                ticker, day_high, day_low,
            )
            return None
        log.info(
            "OVERNIGHT_POLYGON_SNAPSHOT_OK ticker=%s day_high=%.4f day_low=%.4f last=%.4f",
            ticker, day_high, day_low, last_price,
        )
        return {"day_high": day_high, "day_low": day_low, "last_price": last_price}
    except Exception as _e:
        log.warning("[%s] Polygon snapshot fetch failed: %s", ticker, _e)
        return None


def fetch_market_snapshot(ticker: str, broker=None) -> Optional[MarketSnapshot]:
    """
    Fetch today's session high/low and last price for overnight validation.

    PR #181: switched from Tradier timesales to Polygon daily-bar snapshot.
      - Prior: broker.session.get(timesales) → 401 for paper accounts because
        the sandbox bearer token is rejected by api.tradier.com.
      - Now:   requests.get(Polygon snapshot) → `day.h` / `day.l`
        consistent with prior_day_high/prior_day_low from the scanner.

    `broker` is kept in the signature for backward compatibility but is no
    longer used for the snapshot fetch.

    Returns None when data is not yet available (→ RETRY_LATER in caller).
    Returning None is NOT a hard invalidation; ap_overnight_reeval treats
    SNAPSHOT_UNAVAILABLE as DATA_NOT_READY when not fail-closed.
    """
    # Guard: OCC option symbols contain digits after position 4
    if len(ticker) > 6 and any(c.isdigit() for c in ticker[4:]):
        log.error(
            "[%s] fetch_market_snapshot called with option symbol — must be underlying equity symbol",
            ticker,
        )
        return None

    data = _fetch_polygon_daily_snapshot(ticker)
    if data is None:
        # No data yet — log and return None (→ RETRY_LATER in caller)
        log.warning(
            "OVERNIGHT_SESSION_BARS_NOT_READY ticker=%s "
            "final_decision=RETRY_LATER keep_watching=true",
            ticker,
        )
        return None

    return MarketSnapshot(
        ticker=ticker,
        session_high_so_far=data["day_high"],
        session_low_so_far=data["day_low"],
        last_price=data["last_price"],
        fetched_at=datetime.now(timezone.utc).isoformat(),
    )

def _missing_data_result(
    *,
    ticker: str,
    side: str,
    prior_day_high: Optional[float],
    prior_day_low: Optional[float],
    snapshot_missing: bool,
) -> ValidationResult:
    if OVERNIGHT_DAILY_FAIL_OPEN:
        reason_code = "VALID_MISSING_DATA_FAIL_OPEN"
        reason_text = "Missing overnight daily validation data — fail-open override enabled"
        log.warning(
            "[%s] OVERNIGHT_DAILY_FAIL_OPEN | side=%s prior_high=%s prior_low=%s snapshot_missing=%s",
            ticker, side, prior_day_high, prior_day_low, snapshot_missing,
        )
        return ValidationResult(True, reason_code, reason_text, prior_day_high, prior_day_low, side=side)

    if not prior_day_high or not prior_day_low:
        reason_code = InvalidationReason.MISSING_PRIOR_LEVELS
        reason_text = "Prior-day high/low missing — fail-closed for client-money safety"
    else:
        reason_code = InvalidationReason.SNAPSHOT_UNAVAILABLE
        reason_text = "Market snapshot unavailable — fail-closed for client-money safety"

    log.error(
        "[%s] OVERNIGHT_DAILY_INVALIDATED | %s | side=%s prior_high=%s prior_low=%s snapshot_missing=%s",
        ticker, reason_code, side, prior_day_high, prior_day_low, snapshot_missing,
    )
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
        return ValidationResult(
            False,
            InvalidationReason.INVALID_SIDE,
            f"Unknown side '{side}' — must be CALL or PUT",
            side=side,
        )

    # Sanity check: prior levels must be real prices, not stubs/zeroes
    _MIN_VALID_PRICE = 0.50
    prior_high_valid = bool(prior_day_high and float(prior_day_high) >= _MIN_VALID_PRICE)
    prior_low_valid = bool(prior_day_low and float(prior_day_low) >= _MIN_VALID_PRICE)
    levels_sane = (
        prior_high_valid
        and prior_low_valid
        and float(prior_day_high) > float(prior_day_low)
    )

    if not levels_sane:
        return _missing_data_result(
            ticker=ticker,
            side=side,
            prior_day_high=prior_day_high if prior_high_valid else None,
            prior_day_low=prior_day_low if prior_low_valid else None,
            snapshot_missing=False,
        )

    if snapshot is None:
        return _missing_data_result(
            ticker=ticker,
            side=side,
            prior_day_high=prior_day_high,
            prior_day_low=prior_day_low,
            snapshot_missing=True,
        )

    sh = snapshot.session_high_so_far
    sl = snapshot.session_low_so_far
    high_breached = sh > prior_day_high
    low_breached = sl < prior_day_low

    if high_breached and low_breached:
        log.info(
            "[%s] OVERNIGHT_DAILY_INVALIDATED | BOTH_SIDES_BREACHED | side=%s | "
            "prior_high=%.2f session_high=%.2f | prior_low=%.2f session_low=%.2f",
            ticker, side, prior_day_high, sh, prior_day_low, sl,
        )
        return ValidationResult(
            False,
            InvalidationReason.BOTH_SIDES_BREACHED,
            f"Both prior-day boundaries breached: session_high {sh:.2f} > prior_high {prior_day_high:.2f} "
            f"AND session_low {sl:.2f} < prior_low {prior_day_low:.2f}",
            prior_day_high, prior_day_low, sh, sl, side,
        )

    if side == "CALL":
        if low_breached:
            log.info(
                "[%s] OVERNIGHT_DAILY_INVALIDATED | PRIOR_LOW_BREACHED | side=CALL | "
                "session_low=%.2f < prior_low=%.2f",
                ticker, sl, prior_day_low,
            )
            return ValidationResult(
                False,
                InvalidationReason.PRIOR_LOW_BREACHED,
                f"Session low {sl:.2f} breached prior-day low {prior_day_low:.2f} — CALL setup invalidated",
                prior_day_high, prior_day_low, sh, sl, side,
            )
        log.info(
            "[%s] OVERNIGHT_DAILY_VALID | side=CALL | prior_low=%.2f session_low=%.2f OK | "
            "prior_high=%.2f session_high=%.2f",
            ticker, prior_day_low, sl, prior_day_high, sh,
        )
        return ValidationResult(
            True,
            "VALID",
            "CALL valid — prior-day low intact, arming for upside breach",
            prior_day_high, prior_day_low, sh, sl, side,
        )

    # PUT path
    if high_breached:
        log.info(
            "[%s] OVERNIGHT_DAILY_INVALIDATED | PRIOR_HIGH_BREACHED | side=PUT | "
            "session_high=%.2f > prior_high=%.2f",
            ticker, sh, prior_day_high,
        )
        return ValidationResult(
            False,
            InvalidationReason.PRIOR_HIGH_BREACHED,
            f"Session high {sh:.2f} breached prior-day high {prior_day_high:.2f} — PUT setup invalidated",
            prior_day_high, prior_day_low, sh, sl, side,
        )

    log.info(
        "[%s] OVERNIGHT_DAILY_VALID | side=PUT | prior_high=%.2f session_high=%.2f OK | "
        "prior_low=%.2f session_low=%.2f",
        ticker, prior_day_high, sh, prior_day_low, sl,
    )
    return ValidationResult(
        True,
        "VALID",
        "PUT valid — prior-day high intact, arming for downside breach",
        prior_day_high, prior_day_low, sh, sl, side,
    )


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
    _underlying = (
        getattr(watched, "underlying", None)
        or getattr(watched, "symbol", None)
        or watched.ticker
    )
    snapshot = fetch_market_snapshot(_underlying, broker)

    return validate_overnight_daily_signal(
        ticker=_underlying,
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
