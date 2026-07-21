from __future__ import annotations

import os
import json
import time
import logging
import threading
import math
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
HEARTBEAT_DEGRADED_SEC      = float(os.getenv("QUOTE_HEARTBEAT_DEGRADED_SEC", "10.0"))
IMMEDIATE_REFRESH_MIN_INTERVAL_SECONDS = float(os.getenv("IMMEDIATE_REFRESH_MIN_INTERVAL_SECONDS", "1.0"))

# Feature flag: migrate to snapshots-only ownership later by setting this to "0".
DIRECT_POSITION_WRITES      = os.getenv("QUOTE_MONITOR_DIRECT_WRITES", "1") == "1"

# P0 (PR #385 amendment): touched-profit arming threshold.
# Semantic authority: "touched green" = executable BID P&L has reached +5%,
# confirmed by two consecutive fresh polls.  This is the long-standing business
# rule (previously an inline 0.05 literal); it is intentionally NOT the
# per-position immediate-TP threshold from ap_exit_engine._effective_thresholds
# (20-25% depending on DTE/instrument), which governs trail activation.
TOUCHED_PROFIT_ARM_PCT      = float(os.getenv("TOUCHED_PROFIT_ARM_PCT", "0.05"))


# ══════════════════════════════════════════════════════════════════════════════
# AMENDMENT #6 (blockers 2, 4, 5): shared hard-exit reference selector.
# Long-option liquidation policy with provenance and freshness:
#
# Priority (fresh sources only):
#   BID     - executable liquidation price. Best evidence.
#   LAST    - actual last-traded price, but ONLY when fresh.  A stale LAST can
#             manufacture a false loss (opposite of the original bug where
#             stale data hid a loss).  When LAST is stale it drops in priority.
#   MARK    - broker-computed mark.  Fresh mark used when LAST is stale/missing.
#   ASK     - price to BUY. NEVER a liquidation price for a long option.
#             Special rule:
#               - May PROVE catastrophe when ASK is at/below hard_stop (self-proving).
#               - May NEVER clear or improve a prior proven reference.
#               - When only ASK is available and its P&L looks healthy, we
#                 return validity="unproven" so downstream can distinguish it.
#
# Return: HardExitRef(price, source, validity, ts)
#   validity: "proven"    - fresh BID / LAST / MARK / MID; safe for hard-exit auth
#             "unproven"  - ASK-only healthy; must not clear prior proven ref
#             "catastrophic_ask" - ASK-only but ASK itself catastrophic (self-proving)
#             "no_data"   - nothing available; hard-exit auth explicitly unavailable
# ══════════════════════════════════════════════════════════════════════════════
from dataclasses import dataclass as _dc_href

@_dc_href(frozen=True)
class HardExitRef:
    price: float
    source: str
    validity: str          # "proven" | "unproven" | "catastrophic_ask" | "no_data"
    ts: "Optional[datetime]"


def normalize_hard_ref_ts(
    ts,
    *,
    now_utc: "Optional[datetime]" = None,
    future_skew_sec: float = 60.0,
) -> "Optional[datetime]":
    """Normalize quote/reference timestamps to aware UTC, failing closed."""
    now_utc = now_utc or datetime.now(timezone.utc)
    try:
        if isinstance(ts, bool) or ts is None:
            return None
        if isinstance(ts, datetime):
            parsed = ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts.astimezone(timezone.utc)
        elif isinstance(ts, (int, float)):
            value = float(ts)
            if not math.isfinite(value):
                return None
            parsed = datetime.fromtimestamp(value, tz=timezone.utc)
        elif isinstance(ts, str):
            raw = ts.strip()
            if not raw:
                return None
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            parsed = parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)
        else:
            return None
    except Exception:
        return None

    try:
        if (parsed - now_utc).total_seconds() > float(future_skew_sec):
            return None
    except Exception:
        return None
    return parsed


def _select_hard_exit_reference(
    *,
    bid: float, ask: float, mark: float, last: float,
    bid_ts: "Optional[datetime]" = None,
    last_ts: "Optional[datetime]" = None,
    mark_ts: "Optional[datetime]" = None,
    ask_ts: "Optional[datetime]" = None,
    now_utc: "Optional[datetime]" = None,
    entry_price: float = 0.0,
    hard_stop_pct: float = -0.33,
    last_stale_sec: float = 30.0,
) -> "HardExitRef":
    """Provenance- and freshness-aware hard-exit reference selection.

    - Only FRESH sources count for proven authority.
    - ASK-only healthy quotes return validity='unproven'.
    - ASK-only catastrophic quotes return validity='catastrophic_ask'.
    """
    now_utc = now_utc or datetime.now(timezone.utc)
    _future_skew_sec = float(os.getenv("HARD_EXIT_REF_FUTURE_SKEW_SEC", "60"))
    bid_ts = normalize_hard_ref_ts(bid_ts, now_utc=now_utc, future_skew_sec=_future_skew_sec)
    last_ts = normalize_hard_ref_ts(last_ts, now_utc=now_utc, future_skew_sec=_future_skew_sec)
    mark_ts = normalize_hard_ref_ts(mark_ts, now_utc=now_utc, future_skew_sec=_future_skew_sec)
    ask_ts = normalize_hard_ref_ts(ask_ts, now_utc=now_utc, future_skew_sec=_future_skew_sec)

    def _fresh(ts):
        if ts is None: return False
        try:
            age = (now_utc - ts).total_seconds()
            return 0 <= age <= last_stale_sec
        except Exception:
            return False

    _bid = float(bid) if bid and bid > 0 else 0.0
    _ask = float(ask) if ask and ask > 0 else 0.0
    _mark = float(mark) if mark and mark > 0 else 0.0
    _last = float(last) if last and last > 0 else 0.0

    # BID wins only when its receipt timestamp is valid and fresh.
    if _bid > 0 and _fresh(bid_ts):
        return HardExitRef(price=_bid, source="bid", validity="proven", ts=bid_ts)

    # Prefer fresh LAST over MARK; stale/missing-ts values are retained only as
    # unproven fallbacks after all fresh sources are exhausted.
    _last_fresh = _last > 0 and _fresh(last_ts)
    _mark_fresh = _mark > 0 and _fresh(mark_ts)
    _ask_fresh = _ask > 0 and _fresh(ask_ts)

    if _last_fresh:
        return HardExitRef(price=_last, source="last", validity="proven", ts=last_ts)
    if _mark_fresh:
        return HardExitRef(price=_mark, source="mark", validity="proven", ts=mark_ts)

    # ASK-only path: never trusted for "healthy" and catastrophic only when
    # the ASK receipt timestamp is valid/fresh.
    if _ask > 0 and _ask_fresh:
        if entry_price > 0:
            _ask_pnl = (_ask - entry_price) / entry_price
            if _ask_pnl <= hard_stop_pct:
                # ASK itself is at/below hard stop — self-proving catastrophe.
                return HardExitRef(price=_ask, source="ask_catastrophic",
                                   validity="catastrophic_ask", ts=ask_ts)
        # ASK-only healthy: unproven — do not use as safety certification.
        return HardExitRef(price=_ask, source="ask_unproven",
                           validity="unproven", ts=ask_ts)

    if _bid > 0:
        return HardExitRef(price=_bid, source="bid_stale", validity="unproven", ts=bid_ts)
    if _last > 0:
        return HardExitRef(price=_last, source="last_stale", validity="unproven", ts=last_ts)
    if _mark > 0:
        return HardExitRef(price=_mark, source="mark_stale", validity="unproven", ts=mark_ts)
    if _ask > 0:
        return HardExitRef(price=_ask, source="ask_stale", validity="unproven", ts=ask_ts)

    return HardExitRef(price=0.0, source="", validity="no_data", ts=None)

# PR: position-lifecycle-integrity-and-sizing (P0 FIX-2)
# QPM must also persist live quote / PnL state to the positions table so the
# dashboard, audit trail, and restart recovery have non-NULL truth. The exit
# engine still reads in-memory state (unchanged); these are additive writes.
# Tunable: throttle (seconds) and price-delta-trigger (fraction).
QPM_DB_PERSIST_THROTTLE_SEC    = float(os.getenv("QPM_DB_PERSIST_THROTTLE_SEC", "5.0"))
QPM_DB_PERSIST_PRICE_DELTA_PCT = float(os.getenv("QPM_DB_PERSIST_PRICE_DELTA_PCT", "0.01"))
QPM_DB_PERSIST_ENABLED         = os.getenv("QPM_DB_PERSIST_ENABLED", "1") == "1"

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
        self._last_immediate_refresh_ts: dict[str, float] = {}

        # P0 (PR #385 amendment): touched_profit consecutive-confirmation tracking.
        # touched_profit must NEVER arm from a single poll or from midpoint P&L.
        # We require two consecutive fresh executable BID observations at or above
        # TOUCHED_PROFIT_ARM_PCT (+5% — the existing "touched green" business rule;
        # deliberately NOT the per-position immediate-TP threshold from
        # _effective_thresholds, which governs trail activation, not green-arming).
        #
        # KEYED BY DURABLE POSITION IDENTITY: client_id|execution_mode|position_id.
        # Contract symbol alone is NOT position identity — two entries, a reopen,
        # or two accounts can share one OCC contract, and one position's first
        # confirming poll must never become another position's second.
        # Fail-closed: positions without a position_id never accumulate pending
        # confirmation state (and therefore never arm via this path).
        self._tp_pending_confirm: dict[str, bool] = {}

        # PR: position-lifecycle-integrity-and-sizing (P0 FIX-2)
        # Throttle state for DB persistence of live quote/PnL fields.
        # QPM polls every ~1-2s; persisting every poll would hammer the DB.
        # We persist when (a) >= QPM_DB_PERSIST_THROTTLE_SEC has elapsed since
        # last persist for that position, OR (b) the option price has moved
        # materially (>= QPM_DB_PERSIST_PRICE_DELTA_PCT). This is BEHAVIOR-
        # PRESERVING for the exit engine (it still reads in-memory state via
        # the ManagedPosition object); the DB write is purely for dashboard,
        # audit, and restart-recovery integrity.
        self._last_db_persist_ts:    dict[str, float] = {}   # pid -> epoch
        self._last_db_persist_price: dict[str, float] = {}   # pid -> option price
        self._last_db_persist_hard_ref: dict[str, str] = {}
        self._orders_meta_available: Optional[bool] = None

        self._cycles = 0
        self._consecutive_failures = 0
        self._rate_limit_backoff_sec = RATE_LIMIT_BACKOFF_BASE_SEC
        self._last_cycle_ts = time.time()

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
            "immediate_retry_requests": 0,
            "immediate_retry_coalesced": 0,
            "immediate_retry_evictions": 0,
            "immediate_retry_backoff_suppressed": 0,
            "exits_gated_blind": 0,    # bumped by exit engine when it gates
            "exits_gated_stale": 0,    # bumped by exit engine when it gates
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

    def is_healthy(self) -> bool:
        return self.is_alive() and self.last_cycle_age_sec() <= HEARTBEAT_DEGRADED_SEC

    def metrics_snapshot(self) -> dict:
        m = dict(self._metrics)
        m["last_cycle_age_sec"] = round(self.last_cycle_age_sec(), 2)
        m["consecutive_failures"] = self._consecutive_failures
        m["direct_writes"] = DIRECT_POSITION_WRITES
        return m

    # Exit engine calls these so we have clean observability of what it gated.
    def note_exit_gated_blind(self) -> None:
        self._metrics["exits_gated_blind"] += 1

    def note_exit_gated_stale(self) -> None:
        self._metrics["exits_gated_stale"] += 1

    def request_immediate_refresh(self, *symbols: str) -> bool:
        """
        Ask the quote monitor to retry the next cycle immediately.

        Rate-limit backoff remains authoritative in _fetch_batch_cached(), but
        requested symbols are evicted from the shared cache so a degraded
        protective decision cannot wait behind a stale cache entry.
        """
        cleaned = {str(s or "").upper().strip() for s in symbols if str(s or "").strip()}
        self._metrics["immediate_retry_requests"] += 1
        if not cleaned:
            self._metrics["immediate_retry_coalesced"] += 1
            return False
        now = time.time()
        global _SHARED_BACKOFF_UNTIL
        if now < _SHARED_BACKOFF_UNTIL:
            self._metrics["immediate_retry_backoff_suppressed"] += 1
            return False
        due = set()
        for sym in cleaned:
            last = self._last_immediate_refresh_ts.get(sym, 0.0)
            if now - last < IMMEDIATE_REFRESH_MIN_INTERVAL_SECONDS:
                self._metrics["immediate_retry_coalesced"] += 1
                continue
            due.add(sym)
        if not due:
            return False
        if cleaned:
            with _SHARED_CACHE_LOCK:
                for sym in due:
                    if _SHARED_CACHE.pop(sym, None) is not None:
                        self._metrics["immediate_retry_evictions"] += 1
                    self._last_immediate_refresh_ts[sym] = now
        self._kick.set()
        return True

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
        # Register with health registry once at thread start.
        try:
            from ap_health_registry import HEALTH as _QPM_HEALTH, Criticality as _QPM_CRIT
            _QPM_HEALTH.ensure_registered(
                "ap_quote_monitor", _QPM_CRIT.CRITICAL, stale_after_s=30.0
            )
            _qpm_health_available = True
        except Exception:
            _qpm_health_available = False

        time.sleep(min(1.0, self._interval))
        while not self._stop.is_set():
            interval = self._interval if _is_market_hours() else POLL_OFFHOURS_SEC
            try:
                self._refresh_in_progress = True
                self._last_cycle_started_ts = time.time()
                self._refresh_once()
                self._consecutive_failures = 0
                # Heartbeat on every successful cycle so health registry knows QPM is alive.
                if _qpm_health_available:
                    try:
                        from ap_health_registry import HEALTH as _QPM_HEALTH
                        _QPM_HEALTH.heartbeat(
                            "ap_quote_monitor",
                            metrics={
                                "cycles":               self._metrics.get("cycles", 0),
                                "consecutive_failures": self._consecutive_failures,
                                "positions_tracked":    len(self._health),
                            },
                        )
                    except Exception:
                        pass
            except Exception as e:
                self._consecutive_failures += 1
                log.error("[%s] QuoteMonitor cycle error (%d): %s",
                          self.client_id, self._consecutive_failures, e,
                          exc_info=self._consecutive_failures <= 3)
                if _qpm_health_available and self._consecutive_failures >= 3:
                    try:
                        from ap_health_registry import HEALTH as _QPM_HEALTH
                        _QPM_HEALTH.report_error(
                            "ap_quote_monitor",
                            f"cycle_error_x{self._consecutive_failures}: {e}",
                            fatal=False,
                        )
                    except Exception:
                        pass
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
        lock_ctx = engine_lock if engine_lock is not None else _NullCtx()

        with lock_ctx:
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
                    self._write_field_unconditional(pos, "currentunderlying", und_last)
                    self._write_field_unconditional(pos, "current_underlying", und_last)
                    self._write_field_unconditional(pos, "lastunderlyingquoteupdatets", now_utc)
                    self._write_field_unconditional(pos, "last_underlying_quote_update_ts", now_utc)
                    self._write_field_unconditional(pos, "lastunderlyingquotemissingts", None)
                    self._write_field_unconditional(pos, "last_underlying_quote_missing_ts", None)
                else:
                    self._write_field_unconditional(pos, "lastunderlyingquotemissingts", now_utc)
                    self._write_field_unconditional(pos, "last_underlying_quote_missing_ts", now_utc)

                # ── P0 (PR #385 amendment #2, blocker 1): BID is CYCLE truth ─────────
                # current_bid/current_ask are ALWAYS overwritten from THIS cycle's
                # broker response — including 0.0 when the bid is absent.  Retaining
                # the previous poll's bid while a mark/last/ask-only quote refreshed
                # the general option timestamp made a stale bid masquerade as fresh
                # executable truth (peak updates, confirmation completion, exits).
                # A dedicated last_option_bid_update_ts advances ONLY when a real
                # positive bid arrives, so bid freshness can never be inherited from
                # a mark-only quote.
                self._write_field_unconditional(pos, "currentbid", bid if bid > 0 else 0.0)
                self._write_field_unconditional(pos, "current_bid", bid if bid > 0 else 0.0)
                self._write_field_unconditional(pos, "currentask", ask if ask > 0 else 0.0)
                self._write_field_unconditional(pos, "current_ask", ask if ask > 0 else 0.0)
                if bid > 0:
                    self._write_field_unconditional(pos, "lastoptionbidupdatets", now_utc)
                    self._write_field_unconditional(pos, "last_option_bid_update_ts", now_utc)

                if opt_price > 0:
                    self._write_field(pos, "currentoptionprice", opt_price)
                    self._write_field(pos, "current_option_price", opt_price)
                    self._write_field_unconditional(pos, "lastoptionquoteupdatets", now_utc)
                    self._write_field_unconditional(pos, "last_option_quote_update_ts", now_utc)
                    self._write_field_unconditional(pos, "lastquoteupdatets", now_utc)
                    self._write_field_unconditional(pos, "last_option_price_source", price_source)
                    self._write_field_unconditional(pos, "lastoptionpricesource", price_source)
                    self._write_field_unconditional(pos, "lastoptionquotemissingts", None)
                    self._write_field_unconditional(pos, "last_option_quote_missing_ts", None)

                    # ── Publish to QuoteAuthority so Exit Engine can consume ──
                    # QPM is the ONLY authorized writer. Exit engine reads from
                    # QUOTES.get_fresh() — never fetches independently.
                    # AMENDMENT #6 (blocker 4): 'last' must be raw broker LAST,
                    # never the synthetic opt_price fallback.  opt_price can
                    # itself be an ASK-fallback which would launder ASK as LAST
                    # for downstream hard-exit authority.
                    _raw_broker_last = _safe_float(oq.get("last"), 0.0)
                    try:
                        from ap_quote_authority import QUOTES as _QA
                        _und_px = _safe_float(
                            _get_attr(pos, "currentunderlying", "current_underlying", default=None), 0.0
                        )
                        _QA.write(
                            writer_id        = "ap_position_quote_monitor",
                            symbol           = c,
                            underlying       = t,
                            bid              = bid if bid > 0 else 0.0,
                            ask              = ask if ask > 0 else 0.0,
                            last             = _raw_broker_last,     # raw, not opt_price
                            underlying_price = _und_px,
                            source           = price_source,
                        )
                    except Exception as _qa_err:
                        log.debug("[%s] QuoteAuthority write failed for %s: %s",
                                  self.client_id, c, _qa_err)

                    if self._should_wake(c, opt_price):
                        wake_engine = True
                    self._last_push_price[c] = opt_price
                else:
                    self._write_field_unconditional(pos, "lastoptionquotemissingts", now_utc)
                    self._write_field_unconditional(pos, "last_option_quote_missing_ts", now_utc)

                cost_basis = (
                    _safe_float(_get_attr(pos, "entryprice", "entry_price", default=None), 0.0)
                    or _safe_float(_get_attr(pos, "avgfill", "avg_fill_price", default=None), 0.0)
                    or _safe_float(_get_attr(pos, "entry_fill_price", default=None), 0.0)
                )
                # cur_opt is the analytics mark (mid/mark) — retained for charting only.
                cur_opt = _safe_float(_get_attr(pos, "currentoptionprice", "current_option_price", default=None), 0.0)

                # ── P0 HARD-EXIT LOSS AUTHORITY (amendment #6) ────────────────────
                # Provenance-and-freshness-aware. See _select_hard_exit_reference.
                # - Fresh BID/LAST/MARK/MID: proven; safe to record as hard-loss auth.
                # - ASK-only healthy: unproven; must NOT overwrite a prior proven ref.
                # - ASK-only catastrophic: self-proving; allowed as authority.
                _mark_for_ref = _safe_float(oq.get("mark"), 0.0)
                _last_for_ref = _safe_float(oq.get("last"), 0.0)
                _receipt_ts = normalize_hard_ref_ts(oq.get("_ap_receipt_epoch"), now_utc=now_utc) or now_utc
                _last_trade_ts = normalize_hard_ref_ts(
                    oq.get("last_trade_ts") or oq.get("trade_date"),
                    now_utc=now_utc,
                )
                _mark_ts = normalize_hard_ref_ts(oq.get("mark_ts"), now_utc=now_utc)
                _bid_ts = normalize_hard_ref_ts(oq.get("bid_ts"), now_utc=now_utc) or (_receipt_ts if bid > 0 else None)
                _ask_ts = normalize_hard_ref_ts(oq.get("ask_ts"), now_utc=now_utc) or (_receipt_ts if ask > 0 else None)

                # AMENDMENT #6 blocker 4: pass per-position hard stop.
                # ASK-only at -25% on SPY 0DTE is catastrophic (stop=-18%),
                # but the global -0.33 check would call it unproven.
                # Get the position's actual threshold if possible.
                try:
                    _pos_hard_stop_for_href, _, _ = _effective_thresholds(pos)
                except Exception:
                    _pos_hard_stop_for_href = -0.33

                _href = _select_hard_exit_reference(
                    bid=bid, ask=ask, mark=_mark_for_ref, last=_last_for_ref,
                    bid_ts=_bid_ts, last_ts=_last_trade_ts,
                    mark_ts=_mark_ts, ask_ts=_ask_ts,
                    now_utc=now_utc,
                    entry_price=cost_basis,
                    hard_stop_pct=_pos_hard_stop_for_href,
                    last_stale_sec=float(os.getenv("LAST_TRADE_STALE_SEC", "30.0")),
                )
                _hard_ref_price  = _href.price
                _hard_ref_source = _href.source
                _hard_ref_validity = _href.validity
                _hard_ref_pnl = None

                # Prior proven-ref preservation: if we already have a proven ref on
                # the position and this new ref is unproven, DO NOT overwrite —
                # instead record a "refresh_needed" signal so downstream can act.
                _prior_validity = str(
                    _get_attr(pos, "hard_exit_reference_validity",
                              "hardexitreferencevalidity", default="") or ""
                )
                _prior_price = _safe_float(
                    _get_attr(pos, "hard_exit_reference_price",
                              "hardexitreferenceprice", default=None), 0.0
                )
                _prior_ts = _get_attr(pos, "hard_exit_reference_ts", "hardexitreferencets", default=None)
                _overwrite_allowed = True
                if (
                    (_hard_ref_validity == "unproven" and _prior_validity in ("proven", "catastrophic_ask") and _prior_price > 0)
                    or (_hard_ref_validity == "no_data" and _prior_price > 0)
                ):
                    _overwrite_allowed = False
                    self._write_field_unconditional(pos, "hard_exit_reference_refresh_needed", True)
                    self._write_field_unconditional(pos, "hardexitreferencerefreshneeded",     True)

                if _overwrite_allowed and _hard_ref_price > 0:
                    self._write_field_unconditional(pos, "hard_exit_reference_price",    _hard_ref_price)
                    self._write_field_unconditional(pos, "hardexitreferenceprice",       _hard_ref_price)
                    self._write_field_unconditional(pos, "hard_exit_reference_source",   _hard_ref_source)
                    self._write_field_unconditional(pos, "hardexitreferencesource",      _hard_ref_source)
                    self._write_field_unconditional(pos, "hard_exit_reference_validity", _hard_ref_validity)
                    self._write_field_unconditional(pos, "hardexitreferencevalidity",    _hard_ref_validity)
                    self._write_field_unconditional(pos, "hard_exit_reference_ts",       _href.ts)
                    self._write_field_unconditional(pos, "hardexitreferencets",          _href.ts)
                    if cost_basis > 0:
                        _hard_ref_pnl = (_hard_ref_price - cost_basis) / cost_basis
                        self._write_field_unconditional(pos, "hard_exit_reference_pnl_pct", _hard_ref_pnl)
                        self._write_field_unconditional(pos, "hardexitreferencepnlpct",     _hard_ref_pnl)
                    # Clear the refresh flag when we do write a proven ref.
                    if _hard_ref_validity == "proven":
                        self._write_field_unconditional(pos, "hard_exit_reference_refresh_needed", False)
                        self._write_field_unconditional(pos, "hardexitreferencerefreshneeded",     False)

                _final_href_price = _safe_float(_get_attr(pos, "hard_exit_reference_price", "hardexitreferenceprice", default=None), 0.0)
                _final_href_pnl_raw = _get_attr(pos, "hard_exit_reference_pnl_pct", "hardexitreferencepnlpct", default=None)
                try:
                    _final_href_pnl = float(_final_href_pnl_raw) if _final_href_pnl_raw is not None else None
                except Exception:
                    _final_href_pnl = None
                _final_href_source = str(_get_attr(pos, "hard_exit_reference_source", "hardexitreferencesource", default="") or "")
                _final_href_validity = str(_get_attr(pos, "hard_exit_reference_validity", "hardexitreferencevalidity", default="no_data") or "no_data")
                _final_href_ts = normalize_hard_ref_ts(
                    _get_attr(pos, "hard_exit_reference_ts", "hardexitreferencets", default=None),
                    now_utc=now_utc,
                )
                _final_href_refresh = bool(_get_attr(pos, "hard_exit_reference_refresh_needed", "hardexitreferencerefreshneeded", default=True))

                # ── Blocker 1: Three-tier execution mode classification ────────
                # Only exact "paper" may use midpoint/mark simulation.
                # Blank, unknown, None, or malformed must never use PAPER mid.
                _raw_exec_mode = str(
                    _get_attr(pos, "executionmode", "execution_mode", default="") or ""
                ).strip()
                _norm_mode = _raw_exec_mode.lower()
                if _norm_mode == "paper":
                    _pricing_mode = "paper"
                elif _norm_mode == "live":
                    _pricing_mode = "live"
                else:
                    # blank / "unknown" / malformed / missing → conservative path
                    _pricing_mode = "live_risk_unproven"

                self._write_field(pos, "pricing_mode",       _pricing_mode)
                self._write_field(pos, "raw_execution_mode", _raw_exec_mode)

                # Always write analytics mark for charting / observability.
                self._write_field(pos, "analytics_mark_price", cur_opt)
                self._write_field(pos, "analyticsmarkprice",   cur_opt)

                # ── Blocker 2: Executable price selection ──────────────────────
                # LIVE / live_risk_unproven: decision price is the bid only.
                # Missing bid → executable quote invalid; suppress all profit decisions.
                # mark/ask/mid may NOT overwrite current_option_price for live paths.
                cur_bid_now = _safe_float(_get_attr(pos, "currentbid", "current_bid", default=None), 0.0)

                if _pricing_mode == "paper":
                    exec_price = cur_opt
                    _exec_quote_valid = cur_opt > 0
                    self._write_field(pos, "live_executable_price_source", "paper_mid_simulation")
                    self._write_field(pos, "liveexecutablepricesource",    "paper_mid_simulation")
                    self._write_field(pos, "executable_quote_valid",       _exec_quote_valid)
                    self._write_field(pos, "executable_exit_price",        exec_price)
                else:
                    # LIVE or live_risk_unproven: bid is the only valid exit price.
                    if cur_bid_now > 0:
                        exec_price = cur_bid_now
                        _exec_quote_valid = True
                        _src = "bid" if _pricing_mode == "live" else "bid_live_risk_unproven"
                        self._write_field(pos, "live_executable_price_source", _src)
                        self._write_field(pos, "liveexecutablepricesource",    _src)
                        self._write_field(pos, "executable_quote_valid",       True)
                        self._write_field(pos, "executable_exit_price",        exec_price)
                        # Override current_option_price to bid so exit engine's
                        # option_pnl_pct returns executable P&L.
                        self._write_field(pos, "currentoptionprice",  cur_bid_now)
                        self._write_field(pos, "current_option_price", cur_bid_now)
                    else:
                        # Missing bid — do NOT let mark/ask remain as the exit engine's
                        # decision price.  The opt_price written above may be ask or
                        # mark; clear it from current_option_price now.
                        exec_price = 0.0
                        _exec_quote_valid = False
                        _src = "bid_missing" if _pricing_mode == "live" else "bid_missing_live_risk_unproven"
                        self._write_field(pos, "live_executable_price_source", _src)
                        self._write_field(pos, "liveexecutablepricesource",    _src)
                        self._write_field(pos, "executable_quote_valid",       False)
                        self._write_field(pos, "executable_exit_price",        0.0)
                        # Explicitly zero out current_option_price so the exit engine's
                        # option_pnl_pct cannot use the mark/ask that was written
                        # earlier in this cycle.
                        self._write_field(pos, "currentoptionprice",  0.0)
                        self._write_field(pos, "current_option_price", 0.0)
                        log.warning(
                            "[%s] LIVE_EXECUTABLE_MODE_OR_BID_UNPROVEN | "
                            "pos=%s contract=%s pricing_mode=%s raw_mode=%r "
                            "bid=0 analytics_mark=%.4f — "
                            "non-emergency profit decisions suppressed",
                            self.client_id, pid, c, _pricing_mode, _raw_exec_mode, cur_opt,
                        )
                        self.request_immediate_refresh(c)

                # ── P0: Executable exit authority fields (always BID-based, all modes) ──
                # Separate from the display/analytics mark.  For PAPER, the existing
                # exec_price is midpoint (kept for display), but the BID is the only
                # valid exit authority for soft exits per spec section 3.1/5.1.
                # AMENDMENT #2 (blocker 1): validity is derived from THIS CYCLE's
                # broker response (`bid` local), never from a retained position field.
                _bid_for_exit = bid if bid > 0 else 0.0
                _opt_bid_valid = _bid_for_exit > 0

                # Quote freshness: bid freshness is computed from the DEDICATED bid
                # timestamp (advanced only on real positive bids), so a mark-only
                # quote can never launder a stale bid into "fresh" executable truth.
                _opt_bid_ts_now = _get_attr(pos, "lastoptionbidupdatets", "last_option_bid_update_ts", default=None)
                _und_ts_now = _get_attr(pos, "lastunderlyingquoteupdatets", "last_underlying_quote_update_ts", default=None)
                _EXIT_STALE_SEC = float(os.getenv("EXIT_ENGINE_STALE_OPTION_QUOTE_SEC", "20"))
                _UNDERLYING_STALE_SEC = float(os.getenv("UNDERLYING_QUOTE_STALE_SEC", "40"))
                try:
                    _opt_age_sec = max(0.0, (now_utc - _opt_bid_ts_now).total_seconds()) if _opt_bid_ts_now else 999.0
                    _opt_quote_fresh = _opt_bid_valid and _opt_age_sec <= _EXIT_STALE_SEC
                except Exception:
                    _opt_age_sec, _opt_quote_fresh = 999.0, False
                try:
                    _und_age_sec = max(0.0, (now_utc - _und_ts_now).total_seconds()) if _und_ts_now else 999.0
                    _und_fresh = _und_age_sec <= _UNDERLYING_STALE_SEC
                except Exception:
                    _und_age_sec, _und_fresh = 999.0, False

                # AMENDMENT #2 (blocker 2): availability is THIS CYCLE's fetch result
                # (und_last local), never the retained position price.
                _und_available = und_last > 0

                # Display mark: always the mid/analytics mark written by existing logic.
                # Exit executable mark: always the BID (never mid/ask/last).
                _display_mark = _safe_float(_get_attr(pos, "analyticsmarkprice", "analytics_mark_price", default=None), 0.0) or cur_opt
                _exec_exit_mark = _bid_for_exit if _opt_bid_valid else 0.0
                _exec_exit_pnl: "Optional[float]" = None
                _display_pnl: "Optional[float]" = None
                if cost_basis > 0:
                    if _opt_bid_valid and _exec_exit_mark > 0:
                        _exec_exit_pnl = (_exec_exit_mark - cost_basis) / cost_basis
                    if _display_mark > 0:
                        _display_pnl = (_display_mark - cost_basis) / cost_basis

                # Write explicit truth fields onto position.
                # AMENDMENT #2: unavailable P&L is written as None, never 0.0 —
                # a zero sentinel is indistinguishable from a real breakeven.
                self._write_field_unconditional(pos, "option_bid_valid",        _opt_bid_valid)
                self._write_field_unconditional(pos, "optionbidvalid",          _opt_bid_valid)
                self._write_field_unconditional(pos, "option_quote_fresh",      _opt_quote_fresh)
                self._write_field_unconditional(pos, "optionquotefresh",        _opt_quote_fresh)
                self._write_field_unconditional(pos, "option_quote_age_sec",    _opt_age_sec)
                self._write_field_unconditional(pos, "underlying_available",    _und_available)
                self._write_field_unconditional(pos, "underlyingavailable",     _und_available)
                self._write_field_unconditional(pos, "underlying_fresh",        _und_fresh)
                self._write_field_unconditional(pos, "underlyingfresh",         _und_fresh)
                self._write_field_unconditional(pos, "underlying_age_sec",      _und_age_sec)
                self._write_field_unconditional(pos, "display_mark",            _display_mark)
                self._write_field_unconditional(pos, "display_pnl_pct",         _display_pnl)
                self._write_field_unconditional(pos, "exit_executable_mark",    _exec_exit_mark if _opt_bid_valid else None)
                self._write_field_unconditional(pos, "exit_executable_pnl_pct", _exec_exit_pnl)

                # ── P0 (PR #385 amendment): Position-scoped confirmation key ─────────
                # Durable identity: client_id|execution_mode|position_id.
                # Fail-closed: no position_id → no key → never accumulates pending
                # state and never arms via consecutive confirmation.
                _tp_key = ""
                if pid:
                    _tp_key = "|".join([
                        str(self.client_id or ""),
                        _raw_exec_mode.lower() or "unknown",
                        str(pid),
                    ])

                if cost_basis > 0 and exec_price > 0 and _exec_quote_valid:
                    pnl_pct = (exec_price - cost_basis) / cost_basis
                    # ── DISPLAY / HARD-EXIT AUTHORITY (mode-specific) ────────────────
                    # option_pnl_pct is mid-derived for PAPER and bid for LIVE; the
                    # exit engine's hard exits consume it (backward compatible).
                    self._write_field(pos, "optionpnlpct", pnl_pct)
                    self._write_field(pos, "option_pnl_pct", pnl_pct)

                    # ── EXECUTABLE PEAK AUTHORITY (BID-ONLY, ALL MODES) ──────────────
                    # P0 (PR #385 amendment): peak_pnl_pct, max_profit_seen, and the
                    # touched-profit confirmation state feed soft-exit decisions
                    # (touched-profit floors, runner trails, small-win capture).
                    # They must be updated ONLY from a valid, fresh executable BID.
                    # A PAPER midpoint spike must never inflate the historical peak
                    # that later soft exits are measured against.
                    if _opt_bid_valid and _opt_quote_fresh and _exec_exit_pnl is not None:
                        _exec_peak = max(
                            _exec_exit_pnl,
                            _safe_float(_get_attr(pos, "peakpnlpct", "peak_pnl_pct", default=None), float("-inf")),
                            _safe_float(_get_attr(pos, "maxprofitseen", "max_profit_seen", default=None), float("-inf")),
                        )
                        self._write_field_unconditional(pos, "peakpnlpct", _exec_peak)
                        self._write_field_unconditional(pos, "peak_pnl_pct", _exec_peak)
                        self._write_field_unconditional(pos, "maxprofitseen", _exec_peak)
                        self._write_field_unconditional(pos, "max_profit_seen", _exec_peak)

                    # ── touched_profit consecutive-confirmation arming ───────────────
                    # NEVER arm touched_profit from a single poll or from midpoint.
                    # Require two consecutive fresh executable BID observations at or
                    # above TOUCHED_PROFIT_ARM_PCT (+5%, the "touched green" rule —
                    # see constant docstring; NOT the immediate-TP trail threshold).
                    # Any interruption (bid missing, stale, or below threshold) resets
                    # pending state for THIS position only.
                    if (
                        _tp_key
                        and _opt_bid_valid
                        and _opt_quote_fresh
                        and _exec_exit_pnl is not None
                        and _exec_exit_pnl >= TOUCHED_PROFIT_ARM_PCT
                    ):
                        if not self._tp_pending_confirm.get(_tp_key, False):
                            # First qualifying observation — set pending, do NOT arm yet.
                            self._tp_pending_confirm[_tp_key] = True
                        else:
                            # Second consecutive qualifying observation — arm touched_profit.
                            self._write_field_unconditional(pos, "touchedprofit", True)
                            self._write_field_unconditional(pos, "touched_profit", True)
                    elif _tp_key:
                        # Bid unavailable, stale, or below threshold → reset pending.
                        # touched_profit, once True, is never reset here (only close clears it).
                        self._tp_pending_confirm[_tp_key] = False

                    self._persist_quote_to_db(
                        position_id     = pid,
                        option_price    = exec_price,
                        underlying_price= und_last,
                        option_pnl_pct  = pnl_pct,
                        now_utc         = now_utc,
                        hard_ref={
                            "price": _final_href_price if _final_href_price > 0 else None,
                            "pnl_pct": _final_href_pnl,
                            "source": _final_href_source,
                            "validity": _final_href_validity,
                            "ts": _final_href_ts.isoformat() if _final_href_ts else None,
                            "refresh_needed": _final_href_refresh,
                        },
                    )
                    self._persist_mfe_mae_to_orders(
                        position_id    = pid,
                        contract       = c,
                        option_pnl_pct = pnl_pct,
                        now_utc        = now_utc,
                        source         = ("bid" if _pricing_mode != "paper" else price_source),
                    )
                elif cost_basis > 0 and (exec_price <= 0 or not _exec_quote_valid):
                    self._persist_quote_to_db(
                        position_id=pid,
                        option_price=0.0,
                        underlying_price=und_last,
                        option_pnl_pct=None,
                        now_utc=now_utc,
                        hard_ref={
                            "price": _final_href_price if _final_href_price > 0 else None,
                            "pnl_pct": _final_href_pnl,
                            "source": _final_href_source,
                            "validity": _final_href_validity,
                            "ts": _final_href_ts.isoformat() if _final_href_ts else None,
                            "refresh_needed": _final_href_refresh,
                        },
                    )
                    self._mark_mfe_mae_unavailable(
                        position_id = pid,
                        contract = c,
                        reason   = "missing_option_quote",
                    )

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
                    # ── AMENDMENT #4 (blocker 4): money-safety fields ─────────
                    # If DIRECT_POSITION_WRITES=0 turns off _write_field_unconditional
                    # in a future migration, this snapshot payload is the ONLY way
                    # money-safety truth reaches the engine.  Every field the exit
                    # engine consults for defer/hard-exit decisions is carried here.
                    "option_bid_valid":            _opt_bid_valid,
                    "optionbidvalid":              _opt_bid_valid,
                    "option_quote_fresh":          _opt_quote_fresh,
                    "optionquotefresh":            _opt_quote_fresh,
                    "option_quote_age_sec":        _opt_age_sec,
                    "underlying_available":        _und_available,
                    "underlyingavailable":         _und_available,
                    "underlying_fresh":            _und_fresh,
                    "underlyingfresh":             _und_fresh,
                    "underlying_age_sec":          _und_age_sec,
                    "display_mark":                _display_mark,
                    "display_pnl_pct":             _display_pnl,
                    "exit_executable_mark":        _exec_exit_mark if _opt_bid_valid else None,
                    "exit_executable_pnl_pct":     _exec_exit_pnl,
                    "last_option_bid_update_ts":   _get_attr(pos, "lastoptionbidupdatets", "last_option_bid_update_ts", default=None),
                    "hard_exit_reference_price":   _final_href_price if _final_href_price > 0 else None,
                    "hard_exit_reference_source":  _final_href_source if _final_href_price > 0 else None,
                    "hard_exit_reference_ts":      _final_href_ts if _final_href_price > 0 else None,
                    "hard_exit_reference_pnl_pct": _final_href_pnl,
                    # AMENDMENT #6 blocker 1: validity and refresh_needed MUST be in
                    # the snapshot — every consumer requires validity to trust the pnl.
                    "hard_exit_reference_validity":       _final_href_validity,
                    "hard_exit_reference_refresh_needed": _final_href_refresh,
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
                except Exception as _e:
                    log.debug("quote_monitor_waker_set_failed: %s", _e)

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

    # ── P0 (PR #385 amendment #3, blocker 3): UNCONDITIONAL money-safety writer ─
    # DIRECT_POSITION_WRITES=0 turns _write_field() into a no-op, which was the
    # intended migration path away from direct mutation.  But it silently erases
    # every money-safety truth field the exit engine now depends on
    # (option_bid_valid, underlying_available, hard_exit_reference_*, dedicated
    # bid timestamp).  Under that toggle, the stale-retained-bid bug this PR
    # exists to close came back.  These specific writes are money-critical and
    # cannot be gated by a migration toggle — the apply_quote_snapshots() path
    # does not carry them (would require a schema expansion elsewhere), and a
    # missing money-safety field fails OPEN, not closed.
    def _write_field_unconditional(self, pos, name: str, value) -> None:
        try:
            setattr(pos, name, value)
        except Exception as exc:
            log.debug("[%s] unconditional write %s failed: %s", self.client_id, name, exc)

    # ── P0 FIX-2: persist live quote/PnL fields to positions table ───────────
    # The exit engine reads ManagedPosition (in memory) and remains the source
    # of truth for exit decisions. This writer is purely for dashboard /
    # audit / restart-recovery integrity — without it the positions table
    # reports option_pnl_pct=0, current_option_price=NULL, peak_pnl_pct=0 on
    # every open trade (today's MO/NOW/WFC/BA evidence).
    #
    # Throttled per-position: writes only when either
    #   (a) >= QPM_DB_PERSIST_THROTTLE_SEC elapsed since last write, OR
    #   (b) option price moved by >= QPM_DB_PERSIST_PRICE_DELTA_PCT.
    #
    # Scoped by (id, client_id) so a misrouted poll cannot cross-write
    # another client's positions.
    def _persist_quote_to_db(
        self,
        *,
        position_id: str,
        option_price: float,
        underlying_price: float,
        option_pnl_pct: Optional[float],
        now_utc=None,
        hard_ref: Optional[dict] = None,
    ) -> bool:
        """Persist QPM state to the positions row. Non-fatal, throttled."""
        if not QPM_DB_PERSIST_ENABLED:
            return False
        if not position_id:
            return False
        try:
            now = time.time()
            last_ts    = self._last_db_persist_ts.get(position_id, 0.0)
            last_price = self._last_db_persist_price.get(position_id, 0.0)
            elapsed = now - last_ts
            time_ok  = elapsed >= QPM_DB_PERSIST_THROTTLE_SEC
            if option_price > 0:
                price_ok = (
                    last_price <= 0
                    or abs(option_price - last_price) / last_price >= QPM_DB_PERSIST_PRICE_DELTA_PCT
                )
            else:
                price_ok = False
            hard_ref_payload = hard_ref if isinstance(hard_ref, dict) else None
            hard_ref_fingerprint = json.dumps(hard_ref_payload, sort_keys=True, default=str) if hard_ref_payload is not None else ""
            hard_ref_ok = bool(hard_ref_fingerprint and hard_ref_fingerprint != self._last_db_persist_hard_ref.get(position_id, ""))
            if not (time_ok or price_ok or hard_ref_ok):
                return False

            from ap.db import conn, run_with_retry  # local import avoids cycle

            def _do_update():
                with conn() as c:
                    c.execute(
                        """
                        UPDATE positions
                        SET current_option_price = COALESCE(%s, current_option_price),
                            current_underlying   = COALESCE(NULLIF(%s, 0), current_underlying),
                            option_pnl_pct       = COALESCE(%s, option_pnl_pct),
                            meta                 = CASE
                                WHEN %s::jsonb IS NULL THEN meta
                                ELSE jsonb_set(COALESCE(meta, '{}'::jsonb), '{hard_exit_reference}', %s::jsonb, true)
                            END,
                            updated_at           = NOW()
                        WHERE id        = %s
                          AND client_id = %s
                          AND (
                              UPPER(COALESCE(status, '')) IN ('OPEN', 'CLOSING', 'PARTIAL', 'ACTIVE')
                              OR COALESCE(quantity_remaining, 0) > 0
                          )
                        """,
                        (
                            float(option_price) if option_price > 0 else None,
                            float(underlying_price) if underlying_price > 0 else 0.0,
                            float(option_pnl_pct) if option_pnl_pct is not None else None,
                            json.dumps(hard_ref_payload, default=str) if hard_ref_payload is not None else None,
                            json.dumps(hard_ref_payload, default=str) if hard_ref_payload is not None else None,
                            position_id,
                            self.client_id,
                        ),
                    )
                    return c.rowcount

            rowcount = run_with_retry(_do_update) or 0
            if rowcount != 1:
                return False
            self._last_db_persist_ts[position_id] = now
            if option_price > 0:
                self._last_db_persist_price[position_id] = float(option_price)
            if hard_ref_fingerprint:
                self._last_db_persist_hard_ref[position_id] = hard_ref_fingerprint
            return True
        except Exception as exc:
            log.debug(
                "[%s] _persist_quote_to_db non-fatal failure for pos=%s: %s",
                self.client_id, position_id, exc,
            )
            return False

    def _orders_meta_column_exists(self) -> bool:
        """Verify orders.meta exists before mutating JSONB. Cached per monitor."""
        if self._orders_meta_available is not None:
            return bool(self._orders_meta_available)
        try:
            from ap.db import conn, run_with_retry  # local import avoids cycle

            def _check():
                with conn() as c:
                    c.execute(
                        """
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_schema = 'public'
                          AND table_name = 'orders'
                          AND column_name = 'meta'
                        LIMIT 1
                        """
                    )
                    return c.fetchone() is not None

            self._orders_meta_available = bool(run_with_retry(_check))
        except Exception as exc:
            log.debug("[%s] orders.meta availability check failed: %s", self.client_id, exc)
            self._orders_meta_available = False
        return bool(self._orders_meta_available)

    @staticmethod
    def _parse_order_meta(raw) -> dict:
        if isinstance(raw, dict):
            return dict(raw)
        if isinstance(raw, str) and raw.strip():
            try:
                parsed = json.loads(raw)
                return dict(parsed) if isinstance(parsed, dict) else {}
            except Exception:
                return {}
        return {}

    @staticmethod
    def _meta_float(meta: dict, key: str):
        try:
            value = meta.get(key)
            if value in (None, ""):
                return None
            return float(value)
        except Exception:
            return None

    @classmethod
    def _mfe_mae_patch(cls, prior_meta: dict, option_pnl_pct: float, now_utc, source: str) -> dict:
        """
        Build a JSONB patch for high/low option-PnL excursions.

        Prior orders.meta is the restart source of truth; missing prior values
        initialize to the first observed real PnL, never to fake zero.
        """
        try:
            pnl = float(option_pnl_pct)
        except Exception:
            return {}

        prior_mfe = cls._meta_float(prior_meta, "mfe_pct")
        prior_mae = cls._meta_float(prior_meta, "mae_pct")
        patch: dict = {}
        ts = now_utc.isoformat() if hasattr(now_utc, "isoformat") else str(now_utc)

        if prior_mfe is None or pnl > prior_mfe:
            patch["mfe_pct"] = pnl
            patch["mfe_at"] = ts
        if prior_mae is None or pnl < prior_mae:
            patch["mae_pct"] = pnl
            patch["mae_at"] = ts

        if patch:
            patch["mfe_mae_source"] = source or "position_quote_monitor"
            patch.pop("mfe_mae_unavailable_reason", None)
        return patch

    def _build_mfe_mae_order_locator(
        self,
        *,
        contract: str,
        position_id: str = "",
        local_order_id: str = "",
        broker_order_id: str = "",
        meta_position_id: str = "",
        created_ts_start=None,
        created_ts_end=None,
    ) -> tuple[str, tuple, str] | tuple[None, tuple, str]:
        contract = str(contract or "").strip().upper()
        position_id = str(position_id or "").strip()
        local_order_id = str(local_order_id or "").strip()
        broker_order_id = str(broker_order_id or "").strip()
        meta_position_id = str(meta_position_id or "").strip()

        if not contract:
            return None, (), "missing_contract"

        terminal_clause = "status IN ('FILLED','CLOSED','CANCELLED','CANCELED','EXPIRED')"
        base_client = "client_id = %s"

        if position_id:
            return (
                f"{base_client} AND contract = %s AND position_id = %s AND {terminal_clause}",
                (self.client_id, contract, position_id),
                "position_id",
            )
        if local_order_id:
            return (
                f"{base_client} AND local_order_id = %s AND contract = %s AND {terminal_clause}",
                (self.client_id, local_order_id, contract),
                "local_order_id",
            )
        if broker_order_id:
            return (
                f"{base_client} AND broker_order_id = %s AND contract = %s AND {terminal_clause}",
                (self.client_id, broker_order_id, contract),
                "broker_order_id",
            )
        if meta_position_id:
            return (
                f"""{base_client} AND contract = %s
                    AND (
                        COALESCE(meta, '{{}}'::jsonb)->>'position_id' = %s
                        OR COALESCE(meta, '{{}}'::jsonb)->>'mfe_mae_position_id' = %s
                    )
                    AND {terminal_clause}""",
                (self.client_id, contract, meta_position_id, meta_position_id),
                "meta_position_id",
            )
        if created_ts_start is not None and created_ts_end is not None:
            return (
                f"""{base_client} AND contract = %s
                    AND created_ts >= %s
                    AND created_ts < %s
                    AND {terminal_clause}""",
                (self.client_id, contract, created_ts_start, created_ts_end),
                "created_ts_window",
            )
        return None, (), "missing_safe_order_locator"

    def _fetch_prior_mfe_mae_meta(
        self,
        *,
        contract: str,
        position_id: str = "",
        local_order_id: str = "",
        broker_order_id: str = "",
        meta_position_id: str = "",
        created_ts_start=None,
        created_ts_end=None,
    ) -> dict:
        where_sql, where_params, _ = self._build_mfe_mae_order_locator(
            contract=contract,
            position_id=position_id,
            local_order_id=local_order_id,
            broker_order_id=broker_order_id,
            meta_position_id=meta_position_id,
            created_ts_start=created_ts_start,
            created_ts_end=created_ts_end,
        )
        if where_sql is None or not self._orders_meta_column_exists():
            return {}
        try:
            from ap.db import conn, run_with_retry  # local import avoids cycle

            def _select():
                with conn() as c:
                    c.execute(
                        f"""
                        SELECT meta
                        FROM orders
                        WHERE {where_sql}
                        ORDER BY created_ts DESC
                        LIMIT 1
                        """,
                        where_params,
                    )
                    row = c.fetchone()
                    if not row:
                        return {}
                    try:
                        return row.get("meta") if hasattr(row, "get") else row[0]
                    except Exception:
                        return row[0]

            return self._parse_order_meta(run_with_retry(_select))
        except Exception as exc:
            log.debug("[%s] prior MFE/MAE meta read failed for %s: %s", self.client_id, contract, exc)
            return {}

    def _persist_mfe_mae_to_orders(
        self,
        *,
        position_id: str,
        contract: str,
        option_pnl_pct: float,
        now_utc,
        source: str,
        local_order_id: str = "",
        broker_order_id: str = "",
        meta_position_id: str = "",
        created_ts_start=None,
        created_ts_end=None,
    ) -> bool:
        """Persist real MFE/MAE to orders.meta. Non-fatal, change-only."""
        where_sql, where_params, locator_used = self._build_mfe_mae_order_locator(
            contract=contract,
            position_id=position_id,
            local_order_id=local_order_id,
            broker_order_id=broker_order_id,
            meta_position_id=meta_position_id,
            created_ts_start=created_ts_start,
            created_ts_end=created_ts_end,
        )
        if where_sql is None or not self._orders_meta_column_exists():
            return False
        try:
            prior = self._fetch_prior_mfe_mae_meta(
                contract=contract,
                position_id=position_id,
                local_order_id=local_order_id,
                broker_order_id=broker_order_id,
                meta_position_id=meta_position_id,
                created_ts_start=created_ts_start,
                created_ts_end=created_ts_end,
            )
            patch = self._mfe_mae_patch(prior, option_pnl_pct, now_utc, source)
            if not patch:
                return False
            if position_id:
                patch["mfe_mae_position_id"] = position_id

            from ap.db import conn, run_with_retry  # local import avoids cycle

            def _update():
                with conn() as c:
                    c.execute(
                        f"""
                        UPDATE orders
                        SET meta = (COALESCE(meta, '{{}}'::jsonb) - 'mfe_mae_unavailable_reason') || %s::jsonb
                        WHERE {where_sql}
                        """,
                        (json.dumps(patch, default=str),) + tuple(where_params),
                    )
                    return c.rowcount

            return (run_with_retry(_update) or 0) > 0
        except Exception as exc:
            log.debug(
                "[%s] MFE/MAE orders.meta write failed for %s via %s: %s",
                self.client_id, contract, locator_used, exc,
            )
            return False

    def _mark_mfe_mae_unavailable(
        self,
        *,
        contract: str,
        reason: str,
        position_id: str = "",
        local_order_id: str = "",
        broker_order_id: str = "",
        meta_position_id: str = "",
        created_ts_start=None,
        created_ts_end=None,
    ) -> bool:
        """Mark uncovered rows explicitly unavailable without overwriting real MFE/MAE."""
        where_sql, where_params, locator_used = self._build_mfe_mae_order_locator(
            contract=contract,
            position_id=position_id,
            local_order_id=local_order_id,
            broker_order_id=broker_order_id,
            meta_position_id=meta_position_id,
            created_ts_start=created_ts_start,
            created_ts_end=created_ts_end,
        )
        if where_sql is None or not self._orders_meta_column_exists():
            return False
        try:
            prior = self._fetch_prior_mfe_mae_meta(
                contract=contract,
                position_id=position_id,
                local_order_id=local_order_id,
                broker_order_id=broker_order_id,
                meta_position_id=meta_position_id,
                created_ts_start=created_ts_start,
                created_ts_end=created_ts_end,
            )
            if (
                "mfe_pct" in prior
                or "mae_pct" in prior
                or "mfe_mae_unavailable_reason" in prior
            ):
                return False
            patch = {
                "mfe_mae_unavailable_reason": reason or "mfe_mae_unavailable",
                "mfe_mae_source": "position_quote_monitor",
                "mfe_mae_unavailable_at": _utc_now().isoformat(),
            }

            from ap.db import conn, run_with_retry  # local import avoids cycle

            def _update():
                with conn() as c:
                    c.execute(
                        f"""
                        UPDATE orders
                        SET meta = COALESCE(meta, '{{}}'::jsonb) || %s::jsonb
                        WHERE {where_sql}
                          AND NOT (
                              COALESCE(meta, '{{}}'::jsonb) ? 'mfe_pct'
                              OR COALESCE(meta, '{{}}'::jsonb) ? 'mae_pct'
                              OR COALESCE(meta, '{{}}'::jsonb) ? 'mfe_mae_unavailable_reason'
                          )
                        """,
                        (json.dumps(patch, default=str),) + tuple(where_params),
                    )
                    return c.rowcount

            return (run_with_retry(_update) or 0) > 0
        except Exception as exc:
            log.debug(
                "[%s] MFE/MAE unavailable write failed for %s via %s: %s",
                self.client_id, contract, locator_used, exc,
            )
            return False


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
            self._last_immediate_refresh_ts.pop(k, None)

        # P0 (PR #385 amendment): prune touched-profit confirmation state by
        # POSITION identity.  Key format: client_id|execution_mode|position_id —
        # the position_id is the final segment.  A closed position's pending
        # confirmation must never survive to a reopened position on the same
        # contract (clean-slate rule).
        _stale_tp_keys = [
            k for k in self._tp_pending_confirm
            if k.rsplit("|", 1)[-1] not in active_ids
        ]
        for k in _stale_tp_keys:
            self._tp_pending_confirm.pop(k, None)

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
                    cached = dict(row["quote"] or {})
                    cached["_ap_receipt_epoch"] = row.get("ts", 0.0)
                    out[s] = cached
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
                    q_with_receipt = dict(q or {})
                    q_with_receipt["_ap_receipt_epoch"] = ts
                    _SHARED_CACHE[s] = {"quote": q_with_receipt, "ts": ts}
                    out[s] = q_with_receipt
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
        except Exception as _e:
            log.debug("quote_attrs_setattr_failed: %s", _e)

    def _alert(self, msg: str):
        try:
            self._alert_fn(msg)
        except Exception as _e:
            log.warning("quote_monitor_alert_failed: %s", _e)


class _NullCtx:
    def __enter__(self): return self
    def __exit__(self, *a): return False
