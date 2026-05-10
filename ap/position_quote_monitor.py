from __future__ import annotations

import os
import time
import logging
import threading
from datetime import datetime, timezone, time as dtime
from typing import Optional, Callable
from zoneinfo import ZoneInfo

log = logging.getLogger("ap.position_quote_monitor")
ET = ZoneInfo("America/New_York")

# ── Tunables ─────────────────────────────────────────────────────────────────
POLL_SEC                    = float(os.getenv("QUOTE_POLL_INTERVAL_SEC", "2.0"))
POLL_OFFHOURS_SEC           = float(os.getenv("QUOTE_POLL_OFFHOURS_SEC", "15.0"))
FRESH_MAX_SEC               = float(os.getenv("QUOTE_FRESH_SEC", "3.0"))
DEGRADED_MAX_SEC            = float(os.getenv("QUOTE_DEGRADED_SEC", "8.0"))
STALE_MAX_SEC               = float(os.getenv("QUOTE_STALE_SEC", "15.0"))
WAKE_PRICE_PCT              = float(os.getenv("QUOTE_WAKE_PRICE_PCT", "0.02"))
WAKE_COOLDOWN_SEC           = float(os.getenv("QUOTE_WAKE_COOLDOWN_SEC", "0.5"))
BLIND_ALERT_CYCLES          = int(os.getenv("QUOTE_BLIND_ALERT_CYCLES", "2"))
HTTP_TIMEOUT                = float(os.getenv("QUOTE_HTTP_TIMEOUT_SEC", "4.0"))
CACHE_TTL_SEC               = float(os.getenv("QUOTE_SHARED_CACHE_TTL_SEC", "1.5"))
RATE_LIMIT_BACKOFF_BASE_SEC = float(os.getenv("QUOTE_429_BACKOFF_BASE_SEC", "1.0"))
RATE_LIMIT_BACKOFF_MAX_SEC  = float(os.getenv("QUOTE_429_BACKOFF_MAX_SEC", "8.0"))
MAX_SPREAD_PCT              = float(os.getenv("MAX_OPTION_SPREAD_PCT", "0.18"))
MAX_SPREAD_ABS              = float(os.getenv("MAX_OPTION_SPREAD_ABS", "0.15"))
HEARTBEAT_DEGRADED_SEC      = float(os.getenv("QUOTE_HEARTBEAT_DEGRADED_SEC", "45.0"))
HEARTBEAT_GRACE_SEC         = float(os.getenv("QUOTE_HEARTBEAT_GRACE_SEC", "8.0"))
ENGINE_LOCK_TIMEOUT_SEC     = float(os.getenv("QUOTE_ENGINE_LOCK_TIMEOUT_SEC", "2.0"))

# Feature flag: migrate to snapshots-only ownership later by setting this to "0".
DIRECT_POSITION_WRITES      = os.getenv("QUOTE_MONITOR_DIRECT_WRITES", "1") == "1"

_SHARED_CACHE: dict[str, dict] = {}
_SHARED_CACHE_LOCK = threading.Lock()
_SHARED_BACKOFF_UNTIL = 0.0


def _safe_float(v, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def _is_market_hours() -> bool:
    try:
        et = datetime.now(ET)
        return dtime(9, 25) <= et.time() <= dtime(16, 5) and et.weekday() < 5
    except Exception:
        return True


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _get_attr(obj, *names, default=None):
    for name in names:
        try:
            v = getattr(obj, name, None)
            if v not in (None, ""):
                return v
        except Exception:
            pass
    return default


class APPositionQuoteMonitor:
    def __init__(
        self,
        broker,
        client_id: str,
        exit_engine,
        alert_fn: Optional[Callable[[str], None]] = None,
        poll_interval_sec: float = POLL_SEC,
    ):
        self.broker = broker
        self.client_id = client_id
        self.exit_engine = exit_engine
        self._alert_fn = alert_fn or (lambda m: log.warning(m))
        self._interval = poll_interval_sec

        self._stop = threading.Event()
        self._kick = threading.Event()
        self._thread: Optional[threading.Thread] = None

        self._health: dict[str, dict] = {}
        self._health_lock = threading.Lock()

        self._last_push_price: dict[str, float] = {}
        self._last_wake_price: dict[str, float] = {}
        self._last_wake_ts:    dict[str, float] = {}

        self._cycles = 0
        self._consecutive_failures = 0
        self._rate_limit_backoff_sec = RATE_LIMIT_BACKOFF_BASE_SEC
        now = time.time()
        self._last_cycle_ts = now
        self._last_cycle_started_ts = 0.0
        self._last_cycle_completed_ts = now
        self._refresh_in_progress = False

        self._metrics = {
            "cycles": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "rate_limited": 0,
            "batch_calls": 0,
            "fallback_calls": 0,
            "spread_rejects": 0,
            "blind_alerts": 0,
            "wakes_sent": 0,
            "wakes_suppressed": 0,
            "exits_gated_blind": 0,    # bumped by exit engine when it gates
            "exits_gated_stale": 0,    # bumped by exit engine when it gates
            "engine_lock_timeouts": 0,
        }

    # ── Lifecycle ────────────────────────────────────────────────────────────
    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._kick.clear()
        self._thread = threading.Thread(
            target=self._loop,
            daemon=True,
            name=f"quote-monitor-{self.client_id}",
        )
        self._thread.start()
        log.info("[%s] PositionQuoteMonitor started (direct_writes=%s)",
                 self.client_id, DIRECT_POSITION_WRITES)

    def stop(self):
        self._stop.set()
        self._kick.set()
        if self._thread:
            self._thread.join(timeout=10)

    def is_alive(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    def kick(self):
        self._kick.set()

    def last_cycle_age_sec(self) -> float:
        return time.time() - self._last_cycle_ts

    def _healthy_window_sec(self) -> float:
        """
        Health window must be longer than the monitor sleep interval.

        The old default was QUOTE_HEARTBEAT_DEGRADED_SEC=10 while off-hours
        polling sleeps 15s. That made the runner mark the quote monitor unhealthy
        during normal after-hours operation. This window keeps safety strict during
        market hours while preventing false degradation between off-hours polls.
        """
        interval = self._interval if _is_market_hours() else POLL_OFFHOURS_SEC
        return max(HEARTBEAT_DEGRADED_SEC, interval + HEARTBEAT_GRACE_SEC + HTTP_TIMEOUT)

    def is_healthy(self) -> bool:
        if not self.is_alive():
            return False
        now = time.time()
        window = self._healthy_window_sec()
        if self._refresh_in_progress:
            started = self._last_cycle_started_ts or self._last_cycle_ts
            return (now - started) <= window
        return (now - self._last_cycle_completed_ts) <= window

    def metrics_snapshot(self) -> dict:
        m = dict(self._metrics)
        m["last_cycle_age_sec"] = round(self.last_cycle_age_sec(), 2)
        m["last_cycle_completed_age_sec"] = round(time.time() - self._last_cycle_completed_ts, 2)
        m["refresh_in_progress"] = bool(self._refresh_in_progress)
        m["healthy_window_sec"] = round(self._healthy_window_sec(), 2)
        m["consecutive_failures"] = self._consecutive_failures
        m["direct_writes"] = DIRECT_POSITION_WRITES
        return m

    # Exit engine calls these so we have clean observability of what it gated.
    def note_exit_gated_blind(self) -> None:
        self._metrics["exits_gated_blind"] += 1

    def note_exit_gated_stale(self) -> None:
        self._metrics["exits_gated_stale"] += 1

    # ── Health snapshot ──────────────────────────────────────────────────────
    def health_snapshot(self) -> list[dict]:
        now = _utc_now()
        out = []
        with self._health_lock:
            for pid, raw in self._health.items():
                opt_ts = raw.get("last_option_quote_update_ts")
                und_ts = raw.get("last_underlying_quote_update_ts")
                opt_age = (now - opt_ts).total_seconds() if opt_ts else 999.0
                und_age = (now - und_ts).total_seconds() if und_ts else 999.0
                if opt_age <= FRESH_MAX_SEC:
                    state = "fresh"
                elif opt_age <= DEGRADED_MAX_SEC:
                    state = "degraded"
                elif opt_age <= STALE_MAX_SEC:
                    state = "stale"
                else:
                    state = "blind"
                item = dict(raw)
                item["position_id"] = pid
                item["option_age_sec"] = round(opt_age, 2)
                item["underlying_age_sec"] = round(und_age, 2)
                item["state"] = state
                out.append(item)
        return out

    # ── Main loop (race-safe wake) ───────────────────────────────────────────
    def _loop(self):
        time.sleep(min(1.0, self._interval))
        while not self._stop.is_set():
            interval = self._interval if _is_market_hours() else POLL_OFFHOURS_SEC
            try:
                self._refresh_in_progress = True
                self._last_cycle_started_ts = time.time()
                self._refresh_once()
                self._consecutive_failures = 0
            except Exception as e:
                self._consecutive_failures += 1
                log.error("[%s] QuoteMonitor cycle error (%d): %s",
                          self.client_id, self._consecutive_failures, e,
                          exc_info=self._consecutive_failures <= 3)
            finally:
                self._refresh_in_progress = False
                self._last_cycle_ts = time.time()
                self._last_cycle_completed_ts = self._last_cycle_ts

            triggered = self._kick.wait(timeout=interval)
            if triggered:
                self._kick.clear()
            if self._stop.is_set():
                break

    # ── Core refresh ─────────────────────────────────────────────────────────
    def _refresh_once(self):
        self._cycles += 1
        self._metrics["cycles"] += 1
        self._last_cycle_ts = time.time()

        positions = list(self.exit_engine.active_positions() or [])
        if not positions:
            self._prune_closed(set(), set())
            return

        active_ids: set[str] = set()
        active_contracts: set[str] = set()
        underlyings_set: set[str] = set()
        contracts_set: set[str] = set()

        for p in positions:
            pid = str(_get_attr(p, "positionid", "position_id", default="") or "")
            if pid:
                active_ids.add(pid)
            t = str(_get_attr(p, "ticker", "underlying", default="") or "").upper()
            c = str(_get_attr(p, "optionsymbol", "option_symbol", "contract", default="") or "").upper()
            if t:
                underlyings_set.add(t)
            if c:
                contracts_set.add(c)
                active_contracts.add(c)

        underlyings = sorted(underlyings_set)
        contracts   = sorted(contracts_set)

        und_quotes = self._fetch_batch_cached(underlyings)
        opt_quotes = self._fetch_batch_cached(contracts)

        now_utc = _utc_now()
        wake_engine = False
        snapshots = []

        engine_lock = getattr(self.exit_engine, "_lock", None)
        lock_ctx = _TimedLockCtx(engine_lock, ENGINE_LOCK_TIMEOUT_SEC, self.client_id)

        with lock_ctx as locked:
            if not locked:
                self._metrics["engine_lock_timeouts"] = self._metrics.get("engine_lock_timeouts", 0) + 1
                log.warning("[%s] QuoteMonitor skipped cycle: exit engine lock timeout after %.1fs",
                            self.client_id, ENGINE_LOCK_TIMEOUT_SEC)
                return
            for pos in positions:
                pid = str(_get_attr(pos, "positionid", "position_id", default="") or "")
                t = str(_get_attr(pos, "ticker", "underlying", default="") or "").upper()
                c = str(_get_attr(pos, "optionsymbol", "option_symbol", "contract", default="") or "").upper()

                uq = und_quotes.get(t) or {}
                oq = opt_quotes.get(c) or {}

                und_last = self._extract_underlying_price(uq)
                bid = _safe_float(oq.get("bid"), 0.0)
                ask = _safe_float(oq.get("ask"), 0.0)
                opt_price, price_source = self._extract_option_price(oq, bid, ask)

                if und_last > 0:
                    self._write_field(pos, "currentunderlying", und_last)
                    self._write_field(pos, "current_underlying", und_last)
                    self._write_field(pos, "lastunderlyingquoteupdatets", now_utc)
                    self._write_field(pos, "last_underlying_quote_update_ts", now_utc)
                    self._write_field(pos, "lastunderlyingquotemissingts", None)
                    self._write_field(pos, "last_underlying_quote_missing_ts", None)
                else:
                    self._write_field(pos, "lastunderlyingquotemissingts", now_utc)
                    self._write_field(pos, "last_underlying_quote_missing_ts", now_utc)

                if opt_price > 0:
                    self._write_field(pos, "currentoptionprice", opt_price)
                    self._write_field(pos, "current_option_price", opt_price)
                    if bid > 0:
                        self._write_field(pos, "currentbid", bid)
                        self._write_field(pos, "current_bid", bid)
                    if ask > 0:
                        self._write_field(pos, "currentask", ask)
                        self._write_field(pos, "current_ask", ask)
                    self._write_field(pos, "lastoptionquoteupdatets", now_utc)
                    self._write_field(pos, "last_option_quote_update_ts", now_utc)
                    self._write_field(pos, "lastquoteupdatets", now_utc)
                    self._write_field(pos, "last_option_price_source", price_source)
                    self._write_field(pos, "lastoptionpricesource", price_source)
                    self._write_field(pos, "lastoptionquotemissingts", None)
                    self._write_field(pos, "last_option_quote_missing_ts", None)

                    if self._should_wake(c, opt_price):
                        wake_engine = True
                    self._last_push_price[c] = opt_price
                else:
                    self._write_field(pos, "lastoptionquotemissingts", now_utc)
                    self._write_field(pos, "last_option_quote_missing_ts", now_utc)

                cost_basis = (
                    _safe_float(_get_attr(pos, "entryprice", "entry_price", default=None), 0.0)
                    or _safe_float(_get_attr(pos, "avgfill", "avg_fill_price", default=None), 0.0)
                    or _safe_float(_get_attr(pos, "entry_fill_price", default=None), 0.0)
                )
                cur_opt = _safe_float(_get_attr(pos, "currentoptionprice", "current_option_price", default=None), 0.0)
                if cost_basis > 0 and cur_opt > 0:
                    pnl_pct = (cur_opt - cost_basis) / cost_basis
                    peak = max(
                        pnl_pct,
                        _safe_float(_get_attr(pos, "peakpnlpct", "peak_pnl_pct", default=None), float("-inf")),
                        _safe_float(_get_attr(pos, "maxprofitseen", "max_profit_seen", default=None), float("-inf")),
                    )
                    self._write_field(pos, "peakpnlpct", peak)
                    self._write_field(pos, "peak_pnl_pct", peak)
                    self._write_field(pos, "maxprofitseen", peak)
                    self._write_field(pos, "max_profit_seen", peak)
                    if pnl_pct >= 0.05:
                        self._write_field(pos, "touchedprofit", True)
                        self._write_field(pos, "touched_profit", True)

                cur_underlying = _safe_float(_get_attr(pos, "currentunderlying", "current_underlying", default=None), 0.0)
                cur_option = _safe_float(_get_attr(pos, "currentoptionprice", "current_option_price", default=None), 0.0)
                cur_bid = _safe_float(_get_attr(pos, "currentbid", "current_bid", default=None), 0.0)
                cur_ask = _safe_float(_get_attr(pos, "currentask", "current_ask", default=None), 0.0)
                und_ts = _get_attr(pos, "lastunderlyingquoteupdatets", "last_underlying_quote_update_ts", default=None)
                opt_ts = _get_attr(pos, "lastoptionquoteupdatets", "last_option_quote_update_ts", default=None)

                snapshots.append({
                    "positionid": pid,
                    "position_id": pid,
                    "ticker": t,
                    "optionsymbol": c,
                    "option_symbol": c,
                    "currentunderlying": cur_underlying,
                    "current_underlying": cur_underlying,
                    "currentoptionprice": cur_option,
                    "current_option_price": cur_option,
                    "currentbid": cur_bid,
                    "current_bid": cur_bid,
                    "currentask": cur_ask,
                    "current_ask": cur_ask,
                    "pricesource": price_source,
                    "price_source": price_source,
                    "lastunderlyingquoteupdatets": und_ts,
                    "last_underlying_quote_update_ts": und_ts,
                    "lastoptionquoteupdatets": opt_ts,
                    "last_option_quote_update_ts": opt_ts,
                })

                self._classify_health(pid, c, t, pos)

        applier = (
            getattr(self.exit_engine, "applyquotesnapshots", None)
            or getattr(self.exit_engine, "apply_quote_snapshots", None)
        )
        if callable(applier):
            try:
                applier(snapshots)
            except Exception as exc:
                log.warning("[%s] apply_quote_snapshots failed: %s", self.client_id, exc)

        self._prune_closed(active_ids, active_contracts)

        if wake_engine:
            waker = (
                getattr(self.exit_engine, "quotearrivedevent", None)
                or getattr(self.exit_engine, "_quote_arrived_event", None)
                or getattr(self.exit_engine, "quote_arrived_event", None)
            )
            if waker is not None:
                try:
                    waker.set()
                except Exception:
                    pass

    # ── Wake gating (price threshold + cooldown) ────────────────────────────
    def _should_wake(self, contract: str, opt_price: float) -> bool:
        prev_wake = self._last_wake_price.get(contract, 0.0)
        threshold_hit = (
            prev_wake <= 0
            or abs(opt_price - prev_wake) / max(prev_wake, 0.01) >= WAKE_PRICE_PCT
        )
        if not threshold_hit:
            return False

        now = time.time()
        last_ts = self._last_wake_ts.get(contract, 0.0)
        if now - last_ts < WAKE_COOLDOWN_SEC:
            self._metrics["wakes_suppressed"] += 1
            return False

        self._last_wake_price[contract] = opt_price
        self._last_wake_ts[contract] = now
        self._metrics["wakes_sent"] += 1
        return True

    # ── Single write path (feature-flag gated) ───────────────────────────────
    def _write_field(self, pos, name: str, value) -> None:
        if not DIRECT_POSITION_WRITES:
            return
        try:
            setattr(pos, name, value)
        except Exception as exc:
            log.debug("[%s] write %s failed: %s", self.client_id, name, exc)

    # ── Contract-aware pruning ───────────────────────────────────────────────
    def _prune_closed(self, active_ids: set[str], active_contracts: set[str]) -> None:
        with self._health_lock:
            stale_pids = [k for k in self._health if k not in active_ids]
            for k in stale_pids:
                self._health.pop(k, None)

        stale_contracts = [k for k in self._last_push_price if k not in active_contracts]
        for k in stale_contracts:
            self._last_push_price.pop(k, None)
            self._last_wake_price.pop(k, None)
            self._last_wake_ts.pop(k, None)

    # ── Quote fetch (shared cache + 429 backoff) ─────────────────────────────
    def _fetch_batch_cached(self, symbols: list[str]) -> dict[str, dict]:
        if not symbols:
            return {}
        now = time.time()
        out: dict[str, dict] = {}
        stale: list[str] = []

        with _SHARED_CACHE_LOCK:
            for s in symbols:
                row = _SHARED_CACHE.get(s)
                if row and now - row.get("ts", 0.0) <= CACHE_TTL_SEC:
                    out[s] = row["quote"]
                    self._metrics["cache_hits"] += 1
                else:
                    stale.append(s)
                    self._metrics["cache_misses"] += 1

        if not stale:
            return out

        global _SHARED_BACKOFF_UNTIL
        if now < _SHARED_BACKOFF_UNTIL:
            return out

        fresh = self._fetch_batch(stale)
        if fresh:
            with _SHARED_CACHE_LOCK:
                ts = time.time()
                for s, q in fresh.items():
                    _SHARED_CACHE[s] = {"quote": q, "ts": ts}
                    out[s] = q
            self._rate_limit_backoff_sec = RATE_LIMIT_BACKOFF_BASE_SEC
        return out

    def _fetch_batch(self, symbols: list[str]) -> dict[str, dict]:
        if not symbols:
            return {}

        for method_name in ("get_quotes", "get_option_quotes", "quotes"):
            method = getattr(self.broker, method_name, None)
            if not callable(method):
                continue
            try:
                self._metrics["batch_calls"] += 1
                raw = method(symbols)
                norm = self._normalize_quotes(raw)
                if norm:
                    return norm
            except Exception as exc:
                if "429" in str(exc):
                    self._handle_429(exc)
                    return {}
                log.debug("[%s] batch %s failed: %s", self.client_id, method_name, exc)

        session = getattr(self.broker, "session", None)
        cfg = getattr(self.broker, "cfg", None)
        base_url = getattr(cfg, "baseurl", None) or getattr(self.broker, "base_url", None) or ""
        if session and base_url:
            try:
                self._metrics["batch_calls"] += 1
                resp = session.get(
                    f"{base_url}/v1/markets/quotes",
                    params={"symbols": ",".join(symbols), "greeks": "false"},
                    headers={"Accept": "application/json"},
                    timeout=HTTP_TIMEOUT,
                )
                if getattr(resp, "status_code", 200) == 429:
                    self._handle_429("HTTP 429")
                    return {}
                data = resp.json() or {}
                return self._normalize_quotes(data)
            except Exception as exc:
                if "429" in str(exc):
                    self._handle_429(exc)
                    return {}
                log.warning("[%s] direct Tradier batch fetch failed: %s", self.client_id, exc)

        out: dict[str, dict] = {}
        for s in symbols:
            for method_name in ("get_option_quote", "get_quote", "quote"):
                method = getattr(self.broker, method_name, None)
                if not callable(method):
                    continue
                try:
                    self._metrics["fallback_calls"] += 1
                    raw = method(s)
                    norm = self._normalize_quotes(raw)
                    if s in norm:
                        out[s] = norm[s]
                        break
                except Exception:
                    continue
        return out

    def _handle_429(self, exc):
        global _SHARED_BACKOFF_UNTIL
        _SHARED_BACKOFF_UNTIL = time.time() + self._rate_limit_backoff_sec
        self._metrics["rate_limited"] += 1
        log.warning("[%s] quote fetch rate limited; backing off %.1fs: %s",
                    self.client_id, self._rate_limit_backoff_sec, exc)
        self._rate_limit_backoff_sec = min(self._rate_limit_backoff_sec * 2.0, RATE_LIMIT_BACKOFF_MAX_SEC)

    def _normalize_quotes(self, raw) -> dict[str, dict]:
        if raw is None:
            return {}
        if isinstance(raw, dict):
            nested = raw.get("quotes")
            if isinstance(nested, dict) and "quote" in nested:
                return self._normalize_quotes(nested["quote"])
            if "symbol" in raw:
                return {str(raw["symbol"]).upper(): raw}
            out = {}
            for k, v in raw.items():
                if isinstance(v, dict) and (v.get("symbol") or k):
                    sym = str(v.get("symbol") or k).upper()
                    out[sym] = v
            return out
        if isinstance(raw, list):
            out = {}
            for q in raw:
                if isinstance(q, dict) and q.get("symbol"):
                    out[str(q["symbol"]).upper()] = q
            return out
        return {}

    # ── Price extraction + spread sanity ─────────────────────────────────────
    def _extract_underlying_price(self, q: dict) -> float:
        for key in ("last", "last_price", "price", "mark", "close", "mid"):
            v = _safe_float(q.get(key), 0.0)
            if v > 0:
                return v
        bid = _safe_float(q.get("bid"), 0.0)
        ask = _safe_float(q.get("ask"), 0.0)
        if bid > 0 and ask > 0:
            return round((bid + ask) / 2.0, 4)
        return 0.0

    def _spread_is_valid(self, bid: float, ask: float) -> bool:
        if bid <= 0 or ask <= 0 or ask < bid:
            return False
        mid = (bid + ask) / 2.0
        spread_pct = (ask - bid) / max(mid, 0.01)
        if spread_pct > MAX_SPREAD_PCT:
            return False
        if (ask - bid) > MAX_SPREAD_ABS:
            return False
        return True

    def _extract_option_price(self, q: dict, bid: float, ask: float) -> tuple[float, str]:
        if self._spread_is_valid(bid, ask):
            return round((bid + ask) / 2.0, 4), "mid"
        if bid > 0 and ask > 0:
            self._metrics["spread_rejects"] += 1
        for key in ("mark", "last", "last_price", "price", "close"):
            v = _safe_float(q.get(key), 0.0)
            if v > 0:
                return v, key
        if ask > 0:
            return ask, "ask_only"
        if bid > 0:
            return bid, "bid_only"
        return 0.0, "none"

    # ── Health classification (always sets quote_state on pos) ──────────────
    def _classify_health(self, pid: str, contract: str, underlying: str, pos):
        now = _utc_now()
        opt_ts = _get_attr(pos, "lastoptionquoteupdatets", "last_option_quote_update_ts", default=None)
        und_ts = _get_attr(pos, "lastunderlyingquoteupdatets", "last_underlying_quote_update_ts", default=None)
        last_price = _safe_float(_get_attr(pos, "currentoptionprice", "current_option_price", default=None), 0.0)
        opt_age = (now - opt_ts).total_seconds() if opt_ts else 999.0
        und_age = (now - und_ts).total_seconds() if und_ts else 999.0

        if opt_age <= FRESH_MAX_SEC:
            state = "fresh"
        elif opt_age <= DEGRADED_MAX_SEC:
            state = "degraded"
        elif opt_age <= STALE_MAX_SEC:
            state = "stale"
        else:
            state = "blind"

        with self._health_lock:
            prev = self._health.get(pid, {})
            blind_cycles = (prev.get("blind_cycles", 0) + 1) if state == "blind" else 0
            last_alert_ts = prev.get("last_alert_ts")
            entry = {
                "contract": contract,
                "underlying": underlying,
                "last_price": last_price,
                "blind_cycles": blind_cycles,
                "last_option_quote_update_ts": opt_ts,
                "last_underlying_quote_update_ts": und_ts,
                "last_alert_ts": last_alert_ts,
                "state": state,
            }
            self._health[pid] = entry

            if state == "blind" and blind_cycles >= BLIND_ALERT_CYCLES:
                if not last_alert_ts or (now - last_alert_ts).total_seconds() > 60:
                    entry["last_alert_ts"] = now
                    self._metrics["blind_alerts"] += 1
                    self._alert(
                        f"QUOTE_BLIND | {self.client_id} | {contract} | "
                        f"opt_age={opt_age:.1f}s und_age={und_age:.1f}s"
                    )

            prev_state = prev.get("state")
            if prev_state == "blind" and state in ("fresh", "degraded"):
                self._alert(
                    f"QUOTE_RECOVERED | {self.client_id} | {contract} | "
                    f"opt_age={opt_age:.1f}s state={state}"
                )

        # Always propagate quote state onto the position, even when direct writes are off.
        # Exit engine *must* see this, so it bypasses the feature flag.
        try:
            setattr(pos, "quotestate", state)
            setattr(pos, "quote_state", state)
            setattr(pos, "quoteoptagesec", opt_age)
            setattr(pos, "quote_opt_age_sec", opt_age)
            setattr(pos, "quoteundagesec", und_age)
            setattr(pos, "quote_und_age_sec", und_age)
        except Exception:
            pass

    def _alert(self, msg: str):
        try:
            self._alert_fn(msg)
        except Exception:
            pass


class _NullCtx:
    def __enter__(self): return self
    def __exit__(self, *a): return False
