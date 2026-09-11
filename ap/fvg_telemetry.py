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
_candle_cache: dict[str, tuple[float, list[dict[str, Any]], Optional[str]]] = {}

# ── singleflight (#283 review amendment, round 2) ────────────────────────────
# The TTL cache alone protects read/write, not IN-FLIGHT fetches: a 50–100
# same-ticker dispatch burst can all miss the cache before the leader stores
# bars, producing N simultaneous timesales calls — violating the bounded-spend
# invariant. Per-ticker singleflight: exactly one leader fetches; followers
# wait on the leader's Event (bounded by FETCH_WAIT_SEC > request timeout),
# then re-read the cache. A leader failure releases followers, who observe the
# empty cache and degrade to 'no candles' — never a second herd.
_inflight_lock = threading.Lock()
_inflight: dict[str, threading.Event] = {}
FETCH_WAIT_SEC = 12.0  # > 10s request timeout so followers outlast the leader

# Telemetry thread cap: burst threads are cheap under singleflight (they park
# on an Event), but unbounded daemon spawn is still sloppy under a 500-signal
# dispatch storm. Beyond the cap we DROP and count — the drop counter makes
# the dataset bias visible instead of silent.
_thread_gate = threading.BoundedSemaphore(int(os.getenv("FVG_TELEMETRY_MAX_THREADS", "32")))
_drop_lock = threading.Lock()
_dropped_thread_cap = 0


def dropped_by_thread_cap() -> int:
    with _drop_lock:
        return _dropped_thread_cap

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




def _completed_15m_bucket_key(now: Optional[datetime]) -> str:
    """Cache bucket key for point-in-time 15m coverage.

    ``now is None`` (PRETRIGGER/PREOPEN live path) → ``"live"`` so TTL
    behavior stays ticker-scoped and as-of-agnostic.

    Otherwise return the UTC ISO key of the floored completed 15m boundary
    for ``now`` (minute % 15 == 0, seconds cleared). Distinct completed
    buckets therefore cannot share a cache entry.
    """
    if now is None:
        return "live"
    now_utc = now.astimezone(timezone.utc) if now.tzinfo else now.replace(tzinfo=timezone.utc)
    floored = now_utc.replace(second=0, microsecond=0)
    floored = floored - timedelta(minutes=floored.minute % 15)
    return floored.isoformat()


def cache_covers_as_of(bars: list[dict[str, Any]], now: Optional[datetime]) -> bool:
    """True if ``bars`` already cover every completed 15m bar required by ``now``.

    Live path (``now is None``) always covers. Empty bars skip the coverage
    check (caller still respects TTL / miss semantics). For non-empty bars,
    coverage_through = last bar open time + 15m; ``now`` needs the completed
    boundary at its UTC-floored 15m key — miss when that boundary is beyond
    coverage_through.
    """
    if now is None:
        return True
    if not bars:
        return True
    last_dt = _bar_dt(bars[-1])
    if last_dt is None:
        return False
    if last_dt.tzinfo is None:
        last_dt = last_dt.replace(tzinfo=timezone.utc)
    coverage_through = last_dt.astimezone(timezone.utc) + timedelta(minutes=15)
    now_utc = now.astimezone(timezone.utc) if now.tzinfo else now.replace(tzinfo=timezone.utc)
    needed = now_utc.replace(second=0, microsecond=0)
    needed = needed - timedelta(minutes=needed.minute % 15)
    return coverage_through >= needed


def fetch_15m_bars(ticker: str, broker: Any, *, now: Optional[datetime] = None) -> list[dict[str, Any]]:
    """15-min RTH bars for the trailing LOOKBACK_DAYS.

    Cached per ``ticker|bucket_key`` where bucket_key is ``live`` when
    ``now is None`` (PRETRIGGER/PREOPEN TTL path) or the UTC-floored completed
    15m boundary for BREACH as_of. Non-live hits also require
    ``cache_covers_as_of`` so a stale earlier bucket cannot satisfy a later
    as_of. Singleflighted per cache key (one in-flight network call max).

    Returns [] on any failure so callers degrade to 'no candles' — identical
    to the pre-PR state for that signal.
    """
    key = str(ticker).strip().upper()
    if not key or broker is None:
        return []

    bucket_key = _completed_15m_bucket_key(now)
    cache_key = f"{key}|{bucket_key}"

    def _hit_ok(hit: tuple[float, list[dict[str, Any]], Optional[str]], mono: float) -> bool:
        if (mono - hit[0]) >= CANDLE_TTL_SEC:
            return False
        if bucket_key == "live":
            return True
        return cache_covers_as_of(hit[1], now)

    while True:
        now_mono = time.monotonic()
        with _cache_lock:
            hit = _candle_cache.get(cache_key)
            if hit and _hit_ok(hit, now_mono):
                return hit[1]

        # Leader election for this ticker|bucket (preserves live vs as_of isolation).
        with _inflight_lock:
            evt = _inflight.get(cache_key)
            if evt is None:
                evt = threading.Event()
                _inflight[cache_key] = evt
                is_leader = True
            else:
                is_leader = False

        if not is_leader:
            # Follower: park until the leader finishes (or times out), then
            # re-read the cache exactly once. Empty cache after wait → degrade.
            evt.wait(FETCH_WAIT_SEC)
            with _cache_lock:
                hit = _candle_cache.get(cache_key)
                if hit and _hit_ok(hit, time.monotonic()):
                    return hit[1]
            return []

        try:
            return _fetch_15m_bars_network(key, broker, now=now, cache_key=cache_key)
        finally:
            # Release followers and clear in-flight marker even on failure —
            # a stuck marker would silently disable fetches for the ticker.
            with _inflight_lock:
                _inflight.pop(cache_key, None)
            evt.set()


def _fetch_15m_bars_network(key: str, broker: Any, *, now: Optional[datetime] = None, cache_key: Optional[str] = None) -> list[dict[str, Any]]:
    """The actual network fetch. Leader-only; caller owns singleflight."""
    # #283 REVIEW AMENDMENT: prefer the attached data_broker for market-data
    # reads, matching the quote-truth pattern (#248/#227 and #277's resolver).
    # Falls back to the broker itself when no data_broker is attached.
    quote_src = getattr(broker, "data_broker", None) or broker
    session = getattr(quote_src, "session", None)
    if session is None or not hasattr(session, "get"):
        return []

    now_et = (now or datetime.now(timezone.utc)).astimezone(ET)
    start = (now_et - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%dT09:30:00")
    end = now_et.strftime("%Y-%m-%dT%H:%M:%S")

    try:
        resp = session.get(
            f"{_resolve_base_url(quote_src)}/v1/markets/timesales",
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
                    "volume": float(it.get("volume") or 0),
                })
            except (KeyError, TypeError, ValueError):
                continue
        coverage_iso: Optional[str] = None
        if bars:
            last_dt = _bar_dt(bars[-1])
            if last_dt is not None:
                if last_dt.tzinfo is None:
                    last_dt = last_dt.replace(tzinfo=timezone.utc)
                coverage_iso = (last_dt.astimezone(timezone.utc) + timedelta(minutes=15)).isoformat()
        store_key = cache_key or f"{key}|{_completed_15m_bucket_key(now)}"
        with _cache_lock:
            if len(_candle_cache) >= _CACHE_MAX_TICKERS:
                oldest = min(_candle_cache, key=lambda k: _candle_cache[k][0])
                _candle_cache.pop(oldest, None)
            _candle_cache[store_key] = (time.monotonic(), bars, coverage_iso)
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
                "volume": float(bar.get("volume") or 0),
            }
            current_key = key
        else:
            agg["high"] = max(agg["high"], bar["high"])
            agg["low"] = min(agg["low"], bar["low"])
            agg["close"] = bar["close"]
            agg["volume"] = float(agg.get("volume") or 0) + float(bar.get("volume") or 0)
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


# ── price-shape normalization (#283 review amendment) ────────────────────────

def normalize_price_shape(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return an evaluation copy whose underlying-price keys match what
    evaluate_fvg_context() actually reads.

    #221 reads current_underlying / underlying_price / last_price etc. but NOT
    underlying_at_signal — the canonical execution-side key (ap_signals column,
    and what #277 hydration injects). Without this bridge, a payload carrying
    only underlying_at_signal falls back to generic entry_price, which #221
    itself warns is often the OPTION PREMIUM — silently polluting the
    experiment with premium-vs-underlying geometry.

    Rules:
    - never overwrite an existing positive current_underlying/underlying_price
    - bridge underlying_at_signal into both keys when they're absent
    - if NO real underlying exists under any key, REMOVE entry_price from the
      evaluation copy so #221's fallback cannot fire: for telemetry purposes
      'missing current price' is honest data, premium-as-underlying is not.
    """
    sig = dict(payload)

    def _pos(v: Any) -> Optional[float]:
        try:
            f = float(v)
            return f if f > 0 else None
        except (TypeError, ValueError):
            return None

    existing = _pos(sig.get("current_underlying")) or _pos(sig.get("underlying_price"))
    uas = _pos(sig.get("underlying_at_signal"))
    if existing is None and uas is not None:
        sig["current_underlying"] = uas
        sig["underlying_price"] = uas
        existing = uas
    if existing is None and _pos(sig.get("last_price")) is None:
        # No genuine underlying anywhere → block the entry_price fallback.
        sig.pop("entry_price", None)
    return sig


# ── entry points ─────────────────────────────────────────────────────────────

def record_fvg_telemetry_async(
    *,
    signal_id: str,
    client_email: str,
    payload: Mapping[str, Any],
    broker: Any,
) -> None:
    """Fire-and-forget wrapper for the dispatch hot path (#283 amendment).

    The synchronous path can spend up to one timesales timeout (10s) on a
    ticker cache miss. That is unacceptable ahead of live contract selection /
    watcher creation, so dispatch calls THIS: a daemon thread with a deep-ish
    payload copy (top-level dict copy; telemetry only reads). Returns
    immediately. Thread failures are logged inside record_fvg_telemetry —
    nothing propagates.
    """
    try:
        if not _thread_gate.acquire(blocking=False):
            global _dropped_thread_cap
            with _drop_lock:
                _dropped_thread_cap += 1
                dropped = _dropped_thread_cap
            log.warning(
                "fvg_telemetry: thread cap reached — dropping telemetry for "
                "signal=%s (total dropped=%d). Dataset bias is VISIBLE via "
                "dropped_by_thread_cap(); raise FVG_TELEMETRY_MAX_THREADS if "
                "this fires under normal load.", signal_id, dropped,
            )
            return

        def _run() -> None:
            try:
                record_fvg_telemetry(
                    signal_id=signal_id,
                    client_email=client_email,
                    payload=snapshot,
                    broker=broker,
                )
            finally:
                _thread_gate.release()

        snapshot = dict(payload or {})
        threading.Thread(
            target=_run,
            name=f"fvg-telemetry-{str(signal_id)[:8]}",
            daemon=True,
        ).start()
    except Exception as exc:
        try:
            _thread_gate.release()
        except ValueError:
            pass
        log.warning("fvg_telemetry: async spawn failed signal=%s: %s", signal_id, exc)


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
        result = evaluate_fvg_context(normalize_price_shape(payload), {"candles": candles})

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
