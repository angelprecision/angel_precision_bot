# ap/fvg_telemetry.py — P1: FVG diagnostics telemetry (observe-only)
# =============================================================================
# WHY THIS MODULE EXISTS
# ──────────────────────
# PR #221 (merged) built a complete FVG lifecycle + path-risk + target-guidance
# engine (ap/fair_value_gap.py) behind the observe-only score-profile path —
# but production never supplies candles to that path. Measured result:
# **0 of 4,331 signals in the 30 days ending 2026-07-03 carry any FVG output.**
# The FVG hypothesis ("signals whose path crosses an opposing unfilled 4H FVG
# underperform") has produced zero evidence, not because it is wrong but
# because the telemetry never ran.
#
# This module makes #221 live as a running experiment:
#   1. fetch 15-min RTH bars from Tradier timesales (same production shape as
#      ap/daily_continuation_validator + the overnight validator, #181)
#   2. aggregate to 1h and 4h session-anchored candles
#   3. run evaluate_fvg_context() from #221 unchanged
#   4. persist a compact diagnostic under score_breakdown->'fvg' on the
#      signal's ap_signals row, keyed (signal_id, client_email) per #260
#
# EXPLICITLY OBSERVE-ONLY — DESIGN INVARIANTS
# ───────────────────────────────────────────
# 1. NEVER affects the trade decision. Called after master_control.evaluate();
#    its output is not read by any gate, selector, sizer, or exit in this PR.
#    Promotion to a real gate requires its own PR backed by the data this
#    module collects (per the 2026-07-03 Decision Science audit standard).
# 2. NEVER raises into the dispatch path. Every failure returns a dict with
#    "status": "error"/"skipped" and is logged at WARNING.
# 3. NEVER mutates payload, orders, positions, trade_queue, proof_trades.
#    The ONLY write is a JSONB merge into ap_signals.score_breakdown.
# 4. Read-budget bounded: one Tradier timesales call per ticker per 15 minutes
#    process-wide (TTL cache), regardless of how many clients/signals share
#    the ticker. Dispatch bursts of 100+ same-ticker signals cost one call.
# 5. Candle shape matches #221's _candle_value exactly: dicts with
#    open/high/low/close floats, chronological order.
#
# 4H BAR DEFINITION (documented, deliberate)
# ──────────────────────────────────────────
# US-equity RTH is 6.5h, so "4h" bars are session-anchored buckets from 09:30
# ET: [09:30–13:30) and [13:30–16:00]. The second bar is 2.5h — this matches
# common charting-platform behavior for RTH-only 4H and keeps gap geometry
# consistent day to day. 1h bars are 09:30-anchored 60-min buckets (the last
# is 30 min). Lookback: 15 calendar days of 15-min bars ≈ 10 sessions ≈
# 65 one-hour bars / 20 four-hour bars. LIMITATION (stated honestly): unfilled
# 4H FVGs older than ~10 sessions are invisible to v1. Extending lookback via
# Polygon aggs is a follow-up once the telemetry proves worth the spend.
# =============================================================================

from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional
from zoneinfo import ZoneInfo

from ap.logger import get_logger

log = get_logger("ap.fvg_telemetry")

ET = ZoneInfo("America/New_York")

CANDLE_TTL_SEC = int(os.getenv("FVG_CANDLE_TTL_SEC", "900"))
LOOKBACK_DAYS = int(os.getenv("FVG_LOOKBACK_DAYS", "15"))
_CACHE_MAX_TICKERS = 256

_cache_lock = threading.Lock()
_candle_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}

TELEMETRY_ENABLED_ENV = "FVG_TELEMETRY_ENABLED"  # default ON; "0" disables


def telemetry_enabled() -> bool:
    return os.getenv(TELEMETRY_ENABLED_ENV, "1").strip() not in ("0", "false", "no")


# ── Tradier 15-min history (production market-data shape, cf. #181) ─────────

def _resolve_base_url(broker: Any) -> str:
    try:
        from ap.overnight_daily_validator import _resolve_market_data_base_url
        return _resolve_market_data_base_url(broker)
    except Exception:
        base = str(
            os.getenv("TRADIER_MARKET_DATA_BASE_URL")
            or os.getenv("TRADIER_DATA_BASE_URL")
            or getattr(getattr(broker, "cfg", None), "base_url", "")
            or "https://api.tradier.com"
        ).rstrip("/")
        if "sandbox.tradier.com" in base.lower():
            base = "https://api.tradier.com"
        return base


def fetch_15m_bars(ticker: str, broker: Any, *, now: Optional[datetime] = None) -> list[dict[str, Any]]:
    """15-min RTH bars for the trailing LOOKBACK_DAYS. Cached per ticker.

    Returns [] on any failure so callers degrade to 'no candles' — identical
    to the pre-PR state for that signal.
    """
    key = str(ticker).strip().upper()
    if not key or broker is None:
        return []

    now_mono = time.monotonic()
    with _cache_lock:
        hit = _candle_cache.get(key)
        if hit and (now_mono - hit[0]) < CANDLE_TTL_SEC:
            return hit[1]

    session = getattr(broker, "session", None)
    if session is None or not hasattr(session, "get"):
        return []

    now_et = (now or datetime.now(timezone.utc)).astimezone(ET)
    start = (now_et - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%dT09:30:00")
    end = now_et.strftime("%Y-%m-%dT%H:%M:%S")

    try:
        resp = session.get(
            f"{_resolve_base_url(broker)}/v1/markets/timesales",
            params={
                "symbol": key,
                "interval": "15min",
                "start": start,
                "end": end,
                "session_filter": "open",
            },
            headers={"Accept": "application/json"},
            timeout=10,
        )
        if hasattr(resp, "raise_for_status"):
            resp.raise_for_status()
        payload = resp.json() if hasattr(resp, "json") else {}
        series = (payload.get("series") or {}) if isinstance(payload, Mapping) else {}
        data_node = series.get("data") if isinstance(series, Mapping) else None
        items = data_node.get("item") if isinstance(data_node, Mapping) else data_node
        if items is None:
            items = []
        if isinstance(items, Mapping):
            items = [items]
        bars: list[dict[str, Any]] = []
        for it in items:
            if not isinstance(it, Mapping):
                continue
            try:
                bars.append({
                    "time": str(it.get("time") or it.get("timestamp") or ""),
                    "open": float(it["open"]),
                    "high": float(it["high"]),
                    "low": float(it["low"]),
                    "close": float(it["close"]),
                })
            except (KeyError, TypeError, ValueError):
                continue
        with _cache_lock:
            if len(_candle_cache) >= _CACHE_MAX_TICKERS:
                oldest = min(_candle_cache, key=lambda k: _candle_cache[k][0])
                _candle_cache.pop(oldest, None)
            _candle_cache[key] = (now_mono, bars)
        return bars
    except Exception as exc:
        log.warning("fvg_telemetry: timesales fetch failed for %s: %s", key, exc)
        return []


# ── session-anchored aggregation ─────────────────────────────────────────────

def _bar_dt(bar: Mapping[str, Any]) -> Optional[datetime]:
    raw = str(bar.get("time") or "")
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def aggregate_bars(bars_15m: list[dict[str, Any]], *, bucket_minutes: int) -> list[dict[str, Any]]:
    """Aggregate chronological 15-min bars into session-anchored buckets.

    Bucket id = (session date, minutes-since-09:30 // bucket_minutes), so a
    4h request yields [09:30–13:30) + [13:30–16:00] per session (see header).
    """
    out: list[dict[str, Any]] = []
    current_key: Optional[tuple[Any, int]] = None
    agg: Optional[dict[str, Any]] = None

    for bar in bars_15m:
        dt = _bar_dt(bar)
        if dt is None:
            continue
        local = dt.astimezone(ET) if dt.tzinfo else dt
        minutes = max(0, (local.hour - 9) * 60 + (local.minute - 30))
        key = (local.date(), minutes // bucket_minutes)
        if key != current_key:
            if agg is not None:
                out.append(agg)
            agg = {
                "time": bar.get("time"),
                "open": bar["open"],
                "high": bar["high"],
                "low": bar["low"],
                "close": bar["close"],
            }
            current_key = key
        else:
            agg["high"] = max(agg["high"], bar["high"])
            agg["low"] = min(agg["low"], bar["low"])
            agg["close"] = bar["close"]
    if agg is not None:
        out.append(agg)
    return out


# ── compact diagnostic for persistence ───────────────────────────────────────

def _compact(fvg_result: Mapping[str, Any], *, bars_1h: int, bars_4h: int) -> dict[str, Any]:
    diag = dict(fvg_result.get("diagnostics") or {})
    tf = {
        k: {"available": v.get("available"), "fvg_count": v.get("fvg_count")}
        for k, v in (diag.get("timeframes") or {}).items()
        if isinstance(v, Mapping)
    }
    return {
        "v": 1,
        "computed_at": datetime.now(timezone.utc).isoformat(),
        "score": fvg_result.get("score"),
        "max_score": fvg_result.get("max_score"),
        "status": fvg_result.get("status"),
        "block_recommendations": list(fvg_result.get("block_recommendations") or []),
        "target_guidance": fvg_result.get("target_guidance"),
        "opposing_fvg_in_path": bool(diag.get("opposing_fvg_in_path")),
        "opposing_4h_fvg_in_path": bool(diag.get("opposing_4h_fvg_in_path")),
        "entry_inside_opposing_fvg": bool(diag.get("entry_inside_opposing_fvg")),
        "entry_inside_aligned_fvg": bool(diag.get("entry_inside_aligned_fvg")),
        "aligned_support_or_resistance": bool(diag.get("aligned_support_or_resistance")),
        "nearest_fvg": diag.get("nearest_fvg"),
        "timeframes": tf,
        "bars": {"1h": bars_1h, "4h": bars_4h},
    }


def _persist(signal_id: str, client_email: str, compact: dict[str, Any]) -> bool:
    """JSONB-merge under score_breakdown->'fvg'. Composite key per #260.

    ap_signals.signal_id is uuid — compared via ::text to accept the payload's
    string form without a cast error on legacy non-uuid ids (those simply
    match nothing, which is the correct no-op).
    """
    from ap.db import conn, run_with_retry

    def _upd() -> int:
        with conn() as c:
            c.execute(
                """
                UPDATE ap_signals
                SET score_breakdown =
                    COALESCE(score_breakdown, '{}'::jsonb)
                    || jsonb_build_object('fvg', %s::jsonb)
                WHERE signal_id::text = %s AND client_email = %s
                """,
                (json.dumps(compact), str(signal_id), str(client_email)),
            )
            return getattr(c, "rowcount", 0) or 0

    try:
        return run_with_retry(_upd) > 0
    except Exception as exc:
        log.warning("fvg_telemetry: persist failed signal=%s client=%s: %s",
                    signal_id, client_email, exc)
        return False


# ── entry point ──────────────────────────────────────────────────────────────

def record_fvg_telemetry(
    *,
    signal_id: str,
    client_email: str,
    payload: Mapping[str, Any],
    broker: Any,
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    """Compute #221 FVG context for one dispatched signal and persist it.

    Observe-only. Never raises. Returns a summary dict for logging.
    """
    if not telemetry_enabled():
        return {"status": "disabled"}
    try:
        ticker = str(payload.get("ticker") or payload.get("symbol") or "").strip().upper()
        if not ticker or not signal_id or not client_email:
            return {"status": "skipped", "reason": "missing_ticker_or_identity"}

        bars_15 = fetch_15m_bars(ticker, broker, now=now)
        if not bars_15:
            return {"status": "skipped", "reason": "no_candles"}

        candles = {
            "1h": aggregate_bars(bars_15, bucket_minutes=60),
            "4h": aggregate_bars(bars_15, bucket_minutes=240),
        }

        from ap.fair_value_gap import evaluate_fvg_context
        result = evaluate_fvg_context(dict(payload), {"candles": candles})

        compact = _compact(result, bars_1h=len(candles["1h"]), bars_4h=len(candles["4h"]))
        persisted = _persist(signal_id, client_email, compact)
        summary = {
            "status": "recorded" if persisted else "persist_failed",
            "ticker": ticker,
            "fvg_status": compact.get("status"),
            "opposing_4h_in_path": compact.get("opposing_4h_fvg_in_path"),
            "blocks": compact.get("block_recommendations"),
        }
        log.info("fvg_telemetry: %s", summary)
        return summary
    except Exception as exc:  # invariant 2: never raise into dispatch
        log.warning("fvg_telemetry: unexpected failure signal=%s: %s", signal_id, exc)
        return {"status": "error", "reason": str(exc)}
