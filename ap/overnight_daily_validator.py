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

import requests

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
    source: str = ""


@dataclass
class SnapshotFetchResult:
    ok: bool
    retry_later: bool
    reason_code: str
    reason: str
    snapshot: Optional[MarketSnapshot] = None


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


def _et_now():
    try:
        from zoneinfo import ZoneInfo as _ZI
        _et = _ZI("America/New_York")
    except ImportError:
        from datetime import timedelta as _td
        _et = timezone(_td(hours=-4))
    return datetime.now(timezone.utc).astimezone(_et)


def _regular_session_open_et(now_et: datetime) -> datetime:
    return datetime(
        now_et.year, now_et.month, now_et.day,
        9, 30, 0, tzinfo=now_et.tzinfo,
    )


def _fetch_polygon_regular_session_aggs(
    ticker: str,
    *,
    api_key: str,
    trading_date: str,
):
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{trading_date}/{trading_date}"
    try:
        resp = requests.get(
            url,
            params={
                "adjusted": "true",
                "sort": "asc",
                "limit": 50000,
                "apiKey": api_key,
            },
            timeout=10,
        )
        if resp.status_code != 200:
            log.warning(
                "[%s] POLYGON_REGULAR_SESSION_AGGS_HTTP_ERROR status=%s body=%s",
                ticker, resp.status_code, getattr(resp, "text", "")[:160],
            )
            return None
        payload = resp.json() or {}
        results = payload.get("results")
        if isinstance(results, list):
            return results
        return []
    except Exception as exc:
        log.warning("[%s] POLYGON_REGULAR_SESSION_AGGS_FETCH_FAILED %s", ticker, exc)
        return None


def _fetch_market_snapshot_result(ticker: str, broker=None) -> SnapshotFetchResult:
    """
    Resolve a regular-session-only overnight validation snapshot.

    Rules:
      - Before 09:30 ET: retry_later, not due yet.
      - At/after 09:30 ET: use Polygon minute aggregates for today.
      - Filter to regular-session bars only (>= 09:30 ET and <= now_et).
      - Missing/empty regular-session bars => retry_later, not invalidated.
      - No Tradier broker/session dependency.
    """
    if len(ticker) > 6 and any(c.isdigit() for c in ticker[4:]):
        log.error(
            "[%s] fetch_market_snapshot called with option symbol — must be underlying equity symbol",
            ticker,
        )
        return SnapshotFetchResult(
            ok=False,
            retry_later=True,
            reason_code="INVALID_UNDERLYING_SYMBOL",
            reason="Underlying symbol required for overnight daily validation",
        )

    now_et = _et_now()
    today_str = now_et.strftime("%Y-%m-%d")
    session_open = _regular_session_open_et(now_et)

    if now_et < session_open:
        return SnapshotFetchResult(
            ok=False,
            retry_later=True,
            reason_code="REGULAR_SESSION_NOT_OPEN",
            reason="Regular session snapshot not due before 9:30 ET",
        )

    api_key = os.getenv("POLYGON_API_KEY", "").strip()
    if not api_key:
        return SnapshotFetchResult(
            ok=False,
            retry_later=True,
            reason_code="POLYGON_API_KEY_MISSING",
            reason="Polygon API key missing for regular-session snapshot",
        )

    bars = _fetch_polygon_regular_session_aggs(
        ticker,
        api_key=api_key,
        trading_date=today_str,
    )
    if bars is None:
        return SnapshotFetchResult(
            ok=False,
            retry_later=True,
            reason_code="REGULAR_SESSION_SNAPSHOT_UNAVAILABLE",
            reason="Regular-session Polygon aggregate snapshot unavailable",
        )

    filtered = []
    for bar in bars:
        try:
            ts_ms = int(bar.get("t") or 0)
            if ts_ms <= 0:
                continue
            bar_et = datetime.fromtimestamp(ts_ms / 1000, timezone.utc).astimezone(now_et.tzinfo)
            if bar_et < session_open or bar_et > now_et:
                continue
            filtered.append(bar)
        except Exception:
            continue

    if not filtered:
        return SnapshotFetchResult(
            ok=False,
            retry_later=True,
            reason_code="REGULAR_SESSION_BARS_UNAVAILABLE",
            reason="No regular-session Polygon bars available yet",
        )

    session_high = max(float(b.get("h", 0) or 0) for b in filtered)
    session_low = min(float(b.get("l", 0) or 0) for b in filtered)
    last_price = float(
        filtered[-1].get("c")
        or filtered[-1].get("vw")
        or filtered[-1].get("h")
        or 0
    )
    snapshot = MarketSnapshot(
        ticker=ticker,
        session_high_so_far=session_high,
        session_low_so_far=session_low,
        last_price=last_price,
        fetched_at=datetime.now(timezone.utc).isoformat(),
        source="polygon_regular_session_aggs",
    )
    return SnapshotFetchResult(
        ok=True,
        retry_later=False,
        reason_code="OK",
        reason="Regular-session Polygon aggregate snapshot available",
        snapshot=snapshot,
    )


def fetch_market_snapshot(ticker: str, broker=None) -> Optional[MarketSnapshot]:
    result = _fetch_market_snapshot_result(ticker, broker)
    if result.ok and result.snapshot is not None:
        snap = result.snapshot
        log.info(
            "[%s] OVERNIGHT_POLYGON_REGULAR_SESSION_OK source=%s session_high=%.4f session_low=%.4f last=%.4f",
            ticker, snap.source, snap.session_high_so_far, snap.session_low_so_far, snap.last_price,
        )
        return snap

    if result.retry_later:
        log.warning(
            "[%s] OVERNIGHT_REGULAR_SESSION_RETRY_LATER %s %s",
            ticker, result.reason_code, result.reason,
        )
        return None

    log.error("[%s] OVERNIGHT_REGULAR_SESSION_INVALID_DATA %s %s", ticker, result.reason_code, result.reason)
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
