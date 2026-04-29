# ap_entry_watcher.py — Angel Precision Real-Time Entry Watcher
# =============================================================================
# Overnight signal support:
#   Scanners fire post-market (~6 PM ET) for next-day setups.
#   Signals arriving after 3:30 PM ET are accepted and held overnight.
#   They activate at the next session open (9:30 AM ET).
#   TTL is 72 hours — covers weekend holds (Friday PM scanner → Monday open).
#   Force-expire at 4 PM only applies to same-day signals, not overnight ones.
#
# OVERNIGHT DAILY SIGNALS (The Strat daily timeframe):
#   Validated by ap.overnight_daily_validator — NOT by generic drift/stale logic.
#   CALL: invalid if session low < prior_day_low (downside already shown)
#   PUT:  invalid if session high > prior_day_high (upside already shown)
#   See ap/overnight_daily_validator.py for full spec.
# =============================================================================

from __future__ import annotations

import time
import threading
import logging
import uuid
from datetime import datetime, timezone, timedelta
from typing import Callable, Optional
from zoneinfo import ZoneInfo

from ap.overnight_daily_validator import (
    recheck_overnight_daily,
    _is_daily_signal,
    OvernightWatchState,
)

log = logging.getLogger("ap.entry_watcher")
ET  = ZoneInfo("America/New_York")

POLL_INTERVAL_SEC       = 15
MAX_WATCH_MINUTES       = 4320   # 72 hours — holds through weekend
EOD_CUTOFF_HOUR         = 15
EOD_CUTOFF_MIN          = 30
WRONG_DIR_BUFFER_PCT    = 0.001
OVERNIGHT_THRESHOLD_HOUR = 15
OVERNIGHT_THRESHOLD_MIN  = 30

MAX_INTRADAY_WATCH_MIN  = 5
MAX_INTRADAY_DRIFT_PCT  = 0.015
OVERNIGHT_MAX_DRIFT_PCT = 0.020  # still used for NON-daily overnight signals

# Opening auction / first-print protection.
# Prevents the watcher from firing multiple entries into the most chaotic window
# of the session while still allowing normal operation after the protection window.
OPEN_PROTECT_MINUTES   = 5
MAX_OPEN_TRIGGERS      = 1


class WatchState:
    PENDING     = "PENDING"
    TRIGGERED   = "TRIGGERED"
    EXPIRED     = "EXPIRED"
    INVALIDATED = "INVALIDATED"
    CANCELLED   = "CANCELLED"


class WatchedSignal:
    def __init__(self, signal: dict, overnight: bool = False):
        self.signal        = signal
        self.ticker        = signal["ticker"]
        self.side          = signal["side"]
        self.overnight     = overnight

        _trigger = signal.get("trigger") or {}
        _entry   = signal.get("entry_price") or _trigger.get("entry")
        _stop    = signal.get("stop_price")  or _trigger.get("stop")
        _target  = signal.get("target_price") or _trigger.get("pt1") or _trigger.get("pt2")

        self.entry_trigger = float(_entry  or 0) or None
        self.stop_level    = float(_stop   or 0) or None
        self.target_price  = float(_target or 0) or None

        if not self.entry_trigger:
            raise ValueError(
                f"[{signal.get('ticker','?')}] entry_price/trigger is None or zero — "
                f"signal payload incomplete: entry={_entry} stop={_stop} target={_target}"
            )
        self.score         = float(signal.get("score", 0))
        self.grade         = signal.get("grade", "B")

        self.state         = WatchState.PENDING
        self.created_at    = datetime.now(timezone.utc)
        self.triggered_at: Optional[datetime] = None
        self.trigger_price: Optional[float]   = None
        self.expire_at     = self.created_at + timedelta(minutes=MAX_WATCH_MINUTES)

        self.breach_count:    int   = 0
        self.breach_price:    float = 0.0
        self.last_quote_bid:  float = 0.0
        self.last_quote_ask:  float = 0.0
        self.signal_id:       str   = signal.get("signal_id") or str(uuid.uuid4())

        # Set initial queue_status for daily overnight signals
        if overnight and _is_daily_signal(self):
            self.signal["queue_status"] = OvernightWatchState.OVERNIGHT_QUEUED

        if overnight:
            log.info(
                f"[{self.ticker}] OVERNIGHT signal queued | "
                f"{self.side} | trigger=${self.entry_trigger} | stop=${self.stop_level} | "
                f"target=${self.target_price} | activates at next session open (9:30 AM ET)"
            )
        else:
            log.info(
                f"[{self.ticker}] Watching {self.side} | "
                f"trigger=${self.entry_trigger} | stop=${self.stop_level} | "
                f"target=${self.target_price} | expires {self.expire_at.strftime('%H:%M UTC')}"
            )

    MOMENTUM_POLLS_REQUIRED = 2

    def check(self, bid: float, ask: float) -> str:
        now = datetime.now(timezone.utc)
        self.last_quote_bid = bid
        self.last_quote_ask = ask

        if now >= self.expire_at:
            self.state = WatchState.EXPIRED
            log.info(f"[{self.ticker}] EXPIRED — no breach in {MAX_WATCH_MINUTES}min")
            try:
                if hasattr(self, "_watcher_ref") and self._watcher_ref:
                    _ds = getattr(self._watcher_ref, "_dedup_set", None)
                    if _ds and self.signal_id:
                        _ds.discard(str(self.signal_id))
            except Exception:
                pass
            return self.state

        # Intraday stale-move invalidation (NOT for daily overnight signals)
        if (
            not self.overnight
            and not _is_daily_signal(self)
            and self.minutes_watching >= MAX_INTRADAY_WATCH_MIN
        ):
            _mid = (bid + ask) / 2 if bid > 0 and ask > 0 else max(bid, ask)
            if _mid > 0 and self.entry_trigger:
                _drift = (_mid - self.entry_trigger) / self.entry_trigger
                _stale = False
                if self.side == "CALL" and _drift > MAX_INTRADAY_DRIFT_PCT:
                    _stale = True
                elif self.side == "PUT" and _drift < -MAX_INTRADAY_DRIFT_PCT:
                    _stale = True
                if _stale:
                    self.state = WatchState.EXPIRED
                    log.info(
                        "[%s] STALE ENTRY — watching %.1fmin, price drifted %.2f%% "
                        "from trigger $%.2f. Move missed — expiring.",
                        self.ticker, self.minutes_watching,
                        _drift * 100.0, self.entry_trigger,
                    )
                    return self.state

        if self.side == "CALL":
            if ask >= self.entry_trigger:
                if self.breach_count == 0:
                    self.breach_price = ask
                self.breach_count += 1
                if self.breach_count >= self.MOMENTUM_POLLS_REQUIRED:
                    self.state         = WatchState.TRIGGERED
                    self.triggered_at  = now
                    self.trigger_price = ask
                    log.info(
                        f"[{self.ticker}] CALL CONFIRMED — ask=${ask:.2f} "
                        f"held above ${self.entry_trigger:.2f} for {self.breach_count} polls"
                    )
            else:
                self.breach_count = 0
            if self.stop_level and bid <= self.stop_level * (1 - WRONG_DIR_BUFFER_PCT):
                self.state = WatchState.INVALIDATED
                self.breach_count = 0
                log.info(
                    f"[{self.ticker}] INVALIDATED — bid=${bid:.2f} "
                    f"broke stop=${self.stop_level:.2f} before trigger"
                )
        else:  # PUT
            if bid <= self.entry_trigger:
                if self.breach_count == 0:
                    self.breach_price = bid
                self.breach_count += 1
                if self.breach_count >= self.MOMENTUM_POLLS_REQUIRED:
                    self.state         = WatchState.TRIGGERED
                    self.triggered_at  = now
                    self.trigger_price = bid
                    log.info(
                        f"[{self.ticker}] PUT CONFIRMED — bid=${bid:.2f} "
                        f"held below ${self.entry_trigger:.2f} for {self.breach_count} polls"
                    )
            else:
                self.breach_count = 0
            if self.stop_level and ask >= self.stop_level * (1 + WRONG_DIR_BUFFER_PCT):
                self.state = WatchState.INVALIDATED
                self.breach_count = 0
                log.info(
                    f"[{self.ticker}] INVALIDATED — ask=${ask:.2f} "
                    f"broke stop=${self.stop_level:.2f} before trigger"
                )

        return self.state

    @property
    def is_active(self) -> bool:
        return self.state == WatchState.PENDING

    @property
    def minutes_watching(self) -> float:
        return (datetime.now(timezone.utc) - self.created_at).total_seconds() / 60


class APEntryWatcher:
    def __init__(self, broker):
        self.broker         = broker
        self._pending:  list[WatchedSignal] = []
        self._lock          = threading.Lock()
        self._running       = False
        self._thread: Optional[threading.Thread] = None

        self.on_trigger:    Optional[Callable] = None
        self.on_expire:     Optional[Callable] = None
        self.on_invalidate: Optional[Callable] = None

        # Per-session open protection state. Resets once per ET calendar date.
        self._open_trigger_count = 0
        self._open_protect_date  = None

    def add_signal(self, signal: dict) -> bool:
        now_et = datetime.now(ET)
        hour, minute = now_et.hour, now_et.minute
        post_session = (
            hour > OVERNIGHT_THRESHOLD_HOUR
            or (hour == OVERNIGHT_THRESHOLD_HOUR and minute >= OVERNIGHT_THRESHOLD_MIN)
        )
        pre_market = (hour < 9 or (hour == 9 and minute < 30))
        overnight = post_session or pre_market

        if overnight:
            log.info(
                f"[{signal.get('ticker')}] Post-session signal accepted — "
                f"holding overnight for next session open (9:30 AM ET). "
                f"trigger=${signal.get('entry_price') or (signal.get('trigger') or {}).get('entry', '?')}"
            )

        watched = WatchedSignal(signal, overnight=overnight)

        with self._lock:
            same_side = [
                w for w in self._pending
                if w.is_active
                and w.ticker == watched.ticker
                and w.side == watched.side
            ]

            opposite_side = [
                w for w in self._pending
                if w.is_active
                and w.ticker == watched.ticker
                and w.side != watched.side
            ]

            # SAFE MODE:
            # Calls and puts are allowed across the portfolio, but the watcher
            # must never keep both directions armed for the same ticker.
            # Higher score wins; equal/lower score is blocked.
            if opposite_side:
                best_opp = max(opposite_side, key=lambda w: w.score)

                if watched.score > best_opp.score:
                    for w in opposite_side:
                        w.state = WatchState.CANCELLED
                        log.info(
                            "[%s] SAFE_MODE_DIRECTION_FLIP — cancelling %s score=%.1f "
                            "for stronger %s score=%.1f",
                            watched.ticker, w.side, w.score, watched.side, watched.score,
                        )
                    self._pending = [w for w in self._pending if w not in opposite_side]
                else:
                    log.info(
                        "[%s] SAFE_MODE_BLOCK_OPPOSITE — keeping existing %s score=%.1f, "
                        "blocking new %s score=%.1f",
                        watched.ticker, best_opp.side, best_opp.score, watched.side, watched.score,
                    )
                    return False

            # Same-side dedup: keep only the strongest active watcher for the
            # same ticker/direction. This prevents duplicate queue entries while
            # still allowing a stronger refreshed setup to replace the weaker one.
            if same_side:
                best_same = max(same_side, key=lambda w: w.score)
                if watched.score > best_same.score:
                    for w in same_side:
                        w.state = WatchState.CANCELLED
                        log.info(
                            "[%s] SAME_SIDE_REPLACE — cancelling %s score=%.1f "
                            "for stronger same-side score=%.1f",
                            watched.ticker, w.side, w.score, watched.score,
                        )
                    self._pending = [w for w in self._pending if w not in same_side]
                else:
                    log.info(
                        "[%s] SAME_SIDE_BLOCK — keeping %s score=%.1f, "
                        "blocking weaker same-side score=%.1f",
                        watched.ticker, watched.side, best_same.score, watched.score,
                    )
                    return False

            self._pending.append(watched)

            overnight_count = sum(1 for w in self._pending if w.overnight and w.is_active)
            same_day_count  = sum(1 for w in self._pending if not w.overnight and w.is_active)
            active_total    = sum(1 for w in self._pending if w.is_active)

        log.info(
            f"[{watched.ticker}] Added to watch queue — "
            f"{same_day_count} same-day + {overnight_count} overnight = {active_total} active"
        )
        return True

    def watch(self, plan, local_order_id: str) -> bool:
        if plan is None:
            log.warning("watch() called with None plan -- skipping")
            return False

        signal_dict = {
            "signal_id":       getattr(plan, "signal_id",         str(uuid.uuid4())),
            "ticker":          getattr(plan, "ticker",            ""),
            "side":            getattr(plan, "side",              "CALL"),
            "score":           getattr(plan, "score",             65.0),
            "grade":           getattr(plan, "tier",              "B"),
            "entry_price":     getattr(plan, "trigger_price",     None),
            "stop_price":      getattr(plan, "stop_underlying",   None),
            "target_price":    getattr(plan, "target_underlying", None),
            "plan_id":         getattr(plan, "plan_id",           ""),
            "local_order_id":  local_order_id,
            "contract_symbol": getattr(plan, "contract_symbol",  ""),
            "pattern":         getattr(plan, "pattern",           ""),
            # ── OVERNIGHT DAILY FIELDS ───────────────────────────────────────
            "prior_day_high":  getattr(plan, "prior_day_high",   None),
            "prior_day_low":   getattr(plan, "prior_day_low",    None),
            "timeframe":       getattr(plan, "timeframe",        "1d"),
            "strategy_type":   getattr(plan, "strategy_type",    ""),
            # queue_status set in WatchedSignal.__init__ for OVERNIGHT_DAILY
            # ────────────────────────────────────────────────────────────────
            "trigger": {
                "entry": getattr(plan, "trigger_price",     None),
                "stop":  getattr(plan, "stop_underlying",   None),
                "pt1":   getattr(plan, "target_underlying", None),
            },
        }

        # Staleness check (skip for post-session)
        _now_et = datetime.now(ET)
        _post_session = (_now_et.hour > OVERNIGHT_THRESHOLD_HOUR or
                         (_now_et.hour == OVERNIGHT_THRESHOLD_HOUR and
                          _now_et.minute >= OVERNIGHT_THRESHOLD_MIN))
        _trigger = signal_dict.get("entry_price")
        _side    = signal_dict.get("side", "CALL").upper()
        _ticker  = signal_dict.get("ticker", "")
        _stop    = signal_dict.get("stop_price")

        if _post_session:
            log.info(
                "[%s] Post-session queue — skipping staleness check, will evaluate at next open "
                "(trigger=$%.2f side=%s)",
                _ticker, float(_trigger or 0), _side
            )
        elif _trigger and _trigger > 0:
            try:
                _q = self._get_quote(_ticker)
                _bid = float(_q.get("bid") or 0)
                _ask = float(_q.get("ask") or 0)
                _mid = (_bid + _ask) / 2 if _bid > 0 and _ask > 0 else 0
                if _mid > 0:
                    _pct_from_trigger = (_mid - _trigger) / _trigger
                    _stale = False
                    if _side == "CALL" and _pct_from_trigger > 0.030:
                        _stale = True
                    elif _side == "PUT" and _pct_from_trigger < -0.030:
                        _stale = True
                    if _stop and _stop > 0:
                        if _side == "CALL" and _mid < _stop:
                            _stale = True
                        elif _side == "PUT" and _mid > _stop:
                            _stale = True
                    if _stale:
                        log.warning(
                            "[%s] STALE SIGNAL — price $%.2f is %.1f%% from trigger $%.2f "
                            "(side=%s) — skipping stale entry",
                            _ticker, _mid, _pct_from_trigger*100, _trigger, _side
                        )
                        return False
            except Exception as _e:
                log.debug("[%s] Price staleness check failed (continuing): %s", _ticker, _e)

        log.info(
            f"[{signal_dict['ticker']}] watch() | plan={getattr(plan,'plan_id','')} "
            f"order={local_order_id} trigger=${signal_dict['entry_price']} "
            f"side={signal_dict['side']}"
        )
        return self.add_signal(signal_dict)

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread  = threading.Thread(
            target=self._poll_loop,
            daemon=True,
            name="ap-entry-watcher"
        )
        self._thread.start()
        log.info("APEntryWatcher started")

    def stop(self):
        self._running = False
        log.info("APEntryWatcher stopped")

    def status(self) -> list[dict]:
        with self._lock:
            return [
                {
                    "ticker":        w.ticker,
                    "side":          w.side,
                    "trigger":       w.entry_trigger,
                    "target":        w.target_price,
                    "state":         w.state,
                    "mins_watching": round(w.minutes_watching, 1),
                    "score":         w.score,
                    "grade":         w.grade,
                    "overnight":     w.overnight,
                    "queue_status":  w.signal.get("queue_status", ""),
                    "strategy_type": w.signal.get("strategy_type", ""),
                }
                for w in self._pending
            ]

    def _poll_loop(self):
        while self._running:
            try:
                self._check_all()
            except Exception as e:
                log.error(f"Watcher poll error: {e}", exc_info=True)
            time.sleep(POLL_INTERVAL_SEC)

    def _check_all(self):
        now_et = datetime.now(ET)

        # Reset open-protection trigger counter once per ET calendar date.
        today_et = now_et.date()
        if self._open_protect_date != today_et:
            self._open_protect_date = today_et
            self._open_trigger_count = 0

        open_protect_active = (
            now_et.hour == 9
            and 30 <= now_et.minute < 30 + OPEN_PROTECT_MINUTES
        )

        # EOD force-expire same-day signals only
        if now_et.hour >= 16:
            with self._lock:
                expired   = []
                surviving = []
                for w in self._pending:
                    if w.is_active and not w.overnight:
                        w.state = WatchState.EXPIRED
                        expired.append(w)
                        log.info(f"[{w.ticker}] Force-expired at market close (same-day signal)")
                        try:
                            if hasattr(self, "_dedup_set") and w.signal_id:
                                self._dedup_set.discard(str(w.signal_id))
                        except Exception:
                            pass
                    else:
                        surviving.append(w)
                self._pending = surviving
                if expired:
                    log.info(
                        f"EOD force-expire: {len(expired)} same-day signals expired, "
                        f"{len(surviving)} overnight signals held"
                    )
            return

        # Pre-market hold
        if now_et.hour < 9 or (now_et.hour == 9 and now_et.minute < 30):
            return

        # ── OVERNIGHT OPEN-TIME REVALIDATION ─────────────────────────────────
        with self._lock:
            overnight_active = [w for w in self._pending if w.is_active and w.overnight]

        if overnight_active:
            _to_expire_overnight = []

            for w in overnight_active:
                # ── DAILY TIMEFRAME: structural validator (The Strat rule) ────
                if _is_daily_signal(w):
                    w.signal["queue_status"] = OvernightWatchState.OPEN_RECHECK_PENDING
                    result = recheck_overnight_daily(w, self.broker)

                    if not result.valid:
                        w.state = WatchState.INVALIDATED
                        w.signal["queue_status"] = OvernightWatchState.INVALIDATED
                        log.info(
                            "[%s] OVERNIGHT_DAILY_INVALIDATED | side=%s | %s | %s",
                            w.ticker, w.side, result.reason_code, result.reason_text,
                        )
                        _to_expire_overnight.append(w)
                    else:
                        # Promote to same-day active watcher with explicit armed state
                        w.overnight = False
                        w.signal["queue_status"] = OvernightWatchState.VALID_AWAITING_BREACH
                        log.info(
                            "[%s] OVERNIGHT_DAILY_ARMED | side=%s | queue_status=%s | %s",
                            w.ticker, w.side,
                            OvernightWatchState.VALID_AWAITING_BREACH,
                            result.reason_text,
                        )
                    continue  # skip generic stale/drift check below

                # ── INTRADAY / NON-DAILY: existing drift/premarket logic ──────
                try:
                    quotes_overnight = self._fetch_quotes([w.ticker])
                except Exception as _qe:
                    log.warning("[WATCHER] Overnight quote fetch failed: %s", _qe)
                    quotes_overnight = {}

                q = quotes_overnight.get(w.ticker) or {}
                _bid = float(q.get("bid") or 0)
                _ask = float(q.get("ask") or 0)
                if _bid == 0 and _ask == 0:
                    _last = float(q.get("last") or 0)
                    _bid = _ask = _last
                if not (_bid or _ask) or not w.entry_trigger:
                    w.overnight = False
                    continue

                _mid = (_bid + _ask) / 2.0 if _bid and _ask else max(_bid, _ask)
                if not _mid:
                    w.overnight = False
                    continue

                _drift = (_mid - w.entry_trigger) / w.entry_trigger

                _premarket_breached = False
                if w.side == "CALL" and _mid >= w.entry_trigger * 1.005:
                    _premarket_breached = True
                elif w.side == "PUT" and _mid <= w.entry_trigger * 0.995:
                    _premarket_breached = True

                _too_far = False
                if w.side == "CALL" and _drift > OVERNIGHT_MAX_DRIFT_PCT:
                    _too_far = True
                elif w.side == "PUT" and _drift < -OVERNIGHT_MAX_DRIFT_PCT:
                    _too_far = True

                if _premarket_breached:
                    w.state = WatchState.EXPIRED
                    log.info(
                        "[%s] OVERNIGHT INVALIDATED — pre-market breach detected. "
                        "Price $%.2f already through trigger $%.2f. Move done; expiring.",
                        w.ticker, _mid, w.entry_trigger,
                    )
                    _to_expire_overnight.append(w)
                elif _too_far:
                    w.state = WatchState.EXPIRED
                    log.info(
                        "[%s] OVERNIGHT INVALIDATED — price $%.2f drifted %.2f%% "
                        "from trigger $%.2f overnight. Expiring stale setup.",
                        w.ticker, _mid, _drift * 100.0, w.entry_trigger,
                    )
                    _to_expire_overnight.append(w)
                else:
                    w.overnight = False
                    log.info(
                        "[%s] OVERNIGHT VALIDATED at open — price $%.2f within %.2f%% "
                        "of trigger $%.2f. Arming for breach detection.",
                        w.ticker, _mid, _drift * 100.0, w.entry_trigger,
                    )

            if _to_expire_overnight:
                with self._lock:
                    _done_ids = {id(w) for w in _to_expire_overnight}
                    self._pending = [w for w in self._pending if id(w) not in _done_ids]
                log.info("[WATCHER] Overnight revalidation: %d expired", len(_to_expire_overnight))

        # ── REGULAR POLL ──────────────────────────────────────────────────────
        with self._lock:
            active = [w for w in self._pending if w.is_active]

        if not active:
            return

        tickers = list({w.ticker for w in active})
        try:
            quotes = self._fetch_quotes(tickers)
        except Exception as e:
            log.warning(f"Quote fetch failed: {e}")
            return

        completed = []
        with self._lock:
            for w in active:
                quote = quotes.get(w.ticker)
                if not quote:
                    continue
                bid = float(quote.get("bid", 0) or 0)
                ask = float(quote.get("ask", 0) or 0)
                if bid == 0 and ask == 0:
                    last = float(quote.get("last", 0) or 0)
                    bid = ask = last

                new_state = w.check(bid, ask)
                if new_state == WatchState.TRIGGERED:
                    if open_protect_active and self._open_trigger_count >= MAX_OPEN_TRIGGERS:
                        w.state = WatchState.EXPIRED
                        completed.append(("done", w))
                        log.info(
                            "[%s] OPEN_PROTECTION_BLOCK — max open triggers reached (%s/%s)",
                            w.ticker, self._open_trigger_count, MAX_OPEN_TRIGGERS,
                        )
                    else:
                        self._open_trigger_count += 1
                        completed.append(("trigger", w))
                elif new_state in (WatchState.EXPIRED, WatchState.INVALIDATED):
                    completed.append(("done", w))

            done_set = {id(w) for _, w in completed}
            self._pending = [w for w in self._pending if id(w) not in done_set]

        for action, w in completed:
            if action == "trigger" and self.on_trigger:
                try:
                    self.on_trigger(w)
                except Exception as e:
                    log.error(f"[{w.ticker}] on_trigger callback failed: {e}")
            elif w.state == WatchState.EXPIRED and self.on_expire:
                try:
                    self.on_expire(w)
                except Exception as e:
                    log.error(f"on_expire callback failed: {e}")
            elif w.state == WatchState.INVALIDATED and self.on_invalidate:
                try:
                    self.on_invalidate(w)
                except Exception as e:
                    log.error(f"on_invalidate callback failed: {e}")

    def _get_quote(self, ticker: str) -> dict:
        try:
            quotes = self._fetch_quotes([ticker])
            return quotes.get(ticker, {})
        except Exception:
            return {}

    def _fetch_quotes(self, tickers: list[str]) -> dict:
        symbols = ",".join(tickers)
        try:
            _base = (
                getattr(self.broker, "base_url", None)
                or getattr(getattr(self.broker, "cfg", None), "base_url", None)
                or "https://sandbox.tradier.com"
            )
            resp = self.broker.session.get(
                f"{_base}/v1/markets/quotes",
                params={"symbols": symbols, "greeks": "false"},
                headers={"Accept": "application/json"},
                timeout=5,
            )
            data       = resp.json()
            quotes_raw = data.get("quotes", {}).get("quote", [])
            if isinstance(quotes_raw, dict):
                quotes_raw = [quotes_raw]
            return {q["symbol"]: q for q in quotes_raw if q.get("symbol")}
        except Exception as e:
            log.warning(f"Tradier quote fetch failed: {e}")
            return {}
