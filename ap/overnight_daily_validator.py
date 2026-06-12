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
    """Return live Tradier market-data base URL. Never returns sandbox."""
    for attr in ("market_data_base_url", "quote_base_url"):
        v = getattr(broker, attr, None)
        if v and isinstance(v, str) and v.strip():
            return v.strip().rstrip("/")
    cfg = getattr(broker, "cfg", None)
    if cfg is not None:
        v = getattr(cfg, "market_data_base_url", None)
        if v and isinstance(v, str) and v.strip():
            return v.strip().rstrip("/")
    for env_key in ("TRADIER_MARKET_DATA_BASE_URL", "TRADIER_DATA_BASE_URL"):
        v = os.getenv(env_key, "").strip()
        if v:
            return v.rstrip("/")
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


def _fetch_intraday_bars(ticker: str, broker, start_str: str, end_str: str) -> list:
    """
    Fetch 1-minute regular-session bars from Tradier timesales.
    start_str / end_str: "YYYY-MM-DDTHH:MM:SS" in Eastern time.
    Returns list of bar dicts (may be empty). Never raises.
    """
    try:
        # PR #111 amend: live market-data only.
        base_url = _resolve_market_data_base_url(broker)
        resp = broker.session.get(
            f"{base_url}/v1/markets/timesales",
            params={
                "symbol":         ticker,
                "interval":       "1min",
                "start":          start_str,
                "end":            end_str,
                "session_filter": "open",
            },
            headers={"Accept": "application/json"},
            timeout=10,
        )
        if resp.status_code != 200:
            log.warning("[%s] timesales HTTP %d", ticker, resp.status_code)
            return []
        series = (resp.json().get("series") or {})
        if not series:
            return []
        data_node = series.get("data") or {}
        items = data_node.get("item") if isinstance(data_node, dict) else data_node
        if items is None:
            return []
        if isinstance(items, dict):
            items = [items]
        return [b for b in items if isinstance(b, dict)]
    except Exception as _e:
        log.warning("[%s] timesales fetch failed: %s", ticker, _e)
        return []


def fetch_market_snapshot(ticker: str, broker) -> Optional[MarketSnapshot]:
    """
    Compute session high/low from 1-minute regular-session intraday bars.

    Falls back to quote last/bid/ask for last_price ONLY — quote high/low
    are unreliable before/near open and are never used for validation.

    Returns None when data is not yet available (→ RETRY_LATER in caller).
    Returning None is NOT a hard invalidation; ap_overnight_reeval treats
    SNAPSHOT_UNAVAILABLE as DATA_NOT_READY when not fail-closed.

    Env-gate: OVERNIGHT_PREMARKET_HILO_ENABLED (default false).
    Premarket bars are never used for invalidation by default.
    """
    # ── Timezone helpers (zoneinfo stdlib ≥ 3.9, else fixed EDT offset) ───────
    try:
        from zoneinfo import ZoneInfo as _ZI
        _ET = _ZI("America/New_York")
    except ImportError:
        from datetime import timedelta as _td
        _ET = timezone(_td(hours=-4))  # EDT fallback — safe for 9 AM window

    _PREMARKET_HILO_ENABLED = os.getenv(
        "OVERNIGHT_PREMARKET_HILO_ENABLED", "false"
    ).strip().lower() in {"1", "true", "yes", "on"}

    # Guard: OCC option symbols contain digits after position 4
    if len(ticker) > 6 and any(c.isdigit() for c in ticker[4:]):
        log.error(
            "[%s] fetch_market_snapshot called with option symbol — must be underlying equity symbol",
            ticker,
        )
        return None

    now_et    = datetime.now(timezone.utc).astimezone(_ET)
    today_str = now_et.strftime("%Y-%m-%d")

    # Regular-session open: 09:30 ET
    try:
        session_open = datetime(
            now_et.year, now_et.month, now_et.day,
            9, 30, 0, tzinfo=_ET,
        )
    except Exception:
        # Fallback: construct as UTC offset
        from datetime import timedelta as _td2
        session_open = now_et.replace(
            hour=9, minute=30, second=0, microsecond=0
        )
    regular_session_started = now_et >= session_open

    # ── Step 1: Quote endpoint — last_price only ──────────────────────────────
    last_price = 0.0
    try:
        # PR #111 amend: live market-data only.
        base_url = _resolve_market_data_base_url(broker)
        qresp = broker.session.get(
            f"{base_url}/v1/markets/quotes",
            params={"symbols": ticker, "greeks": "false"},
            headers={"Accept": "application/json"},
            timeout=8,
        )
        if qresp.status_code == 200:
            raw = qresp.json().get("quotes", {}).get("quote", {})
            q   = raw[0] if isinstance(raw, list) and raw else raw
            if isinstance(q, dict):
                last_price = float(q.get("last") or q.get("bid") or q.get("ask") or 0)
                _qh = q.get("high")
                _ql = q.get("low")
                if not float(_qh or 0) or not float(_ql or 0):
                    log.warning(
                        "OVERNIGHT_SNAPSHOT_QUOTE_MISSING_HILO ticker=%s "
                        "keys=%s last=%.4f bid=%.4f ask=%.4f high=%s low=%s",
                        ticker, sorted(q.keys()),
                        last_price,
                        float(q.get("bid")  or 0),
                        float(q.get("ask")  or 0),
                        _qh, _ql,
                    )
    except Exception as _qe:
        log.warning("[%s] Quote fetch failed (non-fatal for snapshot): %s", ticker, _qe)

    # ── Step 2: Intraday bars for session high/low ────────────────────────────
    start_str = f"{today_str}T09:30:00"
    end_str   = now_et.strftime("%Y-%m-%dT%H:%M:%S")
    bars = _fetch_intraday_bars(ticker, broker, start_str, end_str)

    if bars:
        session_high = max(float(b.get("high", 0) or 0) for b in bars)
        session_low  = min(float(b.get("low",  0) or 0) for b in bars)
        bar_close    = float(
            bars[-1].get("close") or bars[-1].get("price") or bars[-1].get("last") or 0
        )
        if bar_close:
            last_price = bar_close
        log.info(
            "OVERNIGHT_SESSION_BARS_OK ticker=%s bars=%d "
            "session_high=%.4f session_low=%.4f last=%.4f",
            ticker, len(bars), session_high, session_low, last_price,
        )
        return MarketSnapshot(
            ticker=ticker,
            session_high_so_far=session_high,
            session_low_so_far=session_low,
            last_price=last_price,
            fetched_at=datetime.now(timezone.utc).isoformat(),
        )

    # ── Step 3: No bars — RETRY_LATER (not hard invalidation) ─────────────────
    if not regular_session_started:
        log.warning(
            "OVERNIGHT_SESSION_BARS_NOT_READY ticker=%s "
            "final_decision=RETRY_LATER keep_watching=true",
            ticker,
        )
    else:
        log.warning(
            "OVERNIGHT_SESSION_BARS_UNAVAILABLE ticker=%s "
            "final_decision=RETRY_LATER keep_watching=true",
            ticker,
        )
    return None


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
