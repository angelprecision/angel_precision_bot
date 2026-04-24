
      # ap_entry_watcher.py — Angel Precision Real-Time Entry Watcher
# =============================================================================
# Overnight signal support:
#   Scanners fire post-market (~6 PM ET) for next-day setups.
#   Signals arriving after 3:30 PM ET are accepted and held overnight.
#   They activate at the next session open (9:30 AM ET).
#   TTL is 20 hours — covers any post-market send time through next day's close.
#   Force-expire at 4 PM only applies to same-day signals, not overnight ones.
# =============================================================================

from __future__ import annotations

import time
import threading
import logging
import uuid
from datetime import datetime, timezone, timedelta
from typing import Callable, Optional
from zoneinfo import ZoneInfo

log = logging.getLogger("ap.entry_watcher")
ET  = ZoneInfo("America/New_York")

POLL_INTERVAL_SEC       = 15
MAX_WATCH_MINUTES       = 1200   # 20 hours — holds overnight post-market → next session
EOD_CUTOFF_HOUR         = 15
EOD_CUTOFF_MIN          = 30
WRONG_DIR_BUFFER_PCT    = 0.001
OVERNIGHT_THRESHOLD_HOUR = 15
OVERNIGHT_THRESHOLD_MIN  = 30


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
            return self.state

        if self.side == "CALL":
            if ask >= self.entry_trigger:
                if self.breach_count == 0:
                    self.breach_price = ask
                    log.debug(f"[{self.ticker}] CALL breach candidate — ask=${ask:.2f} > ${self.entry_trigger:.2f} (poll 1/{self.MOMENTUM_POLLS_REQUIRED})")
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
                if self.breach_count > 0:
                    log.debug(f"[{self.ticker}] CALL breach reset — ask=${ask:.2f} pulled back")
                self.breach_count = 0

            if bid <= self.stop_level * (1 - WRONG_DIR_BUFFER_PCT):
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
                    log.debug(f"[{self.ticker}] PUT breach candidate — bid=${bid:.2f} < ${self.entry_trigger:.2f} (poll 1/{self.MOMENTUM_POLLS_REQUIRED})")
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
                if self.breach_count > 0:
                    log.debug(f"[{self.ticker}] PUT breach reset — bid=${bid:.2f} pulled back")
                self.breach_count = 0

            if ask >= self.stop_level * (1 + WRONG_DIR_BUFFER_PCT):
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
    """
    Watches pending signals for entry breach confirmation.
    Runs as a background thread.

    Supports overnight signals: post-market scanner setups are held
    overnight and activated at the next session open (9:30 AM ET).
    """

    def __init__(self, broker):
        self.broker         = broker
        self._pending:  list[WatchedSignal] = []
        self._lock          = threading.Lock()
        self._running       = False
        self._thread: Optional[threading.Thread] = None

        self.on_trigger:    Optional[Callable] = None
        self.on_expire:     Optional[Callable] = None
        self.on_invalidate: Optional[Callable] = None

    def add_signal(self, signal: dict) -> bool:
        """
        Add a signal to the watch queue.

        POLICY:
          - Session hours (9:30 AM – 3:30 PM ET): accept, watch immediately.
          - Post-session (3:30 PM – midnight ET): ACCEPT as overnight signal.
            Scanner fires post-market for next-day setups. Signal held overnight,
            activates at 9:30 AM next morning.
          - Pre-market (midnight – 9:29 AM ET): accept, waits for open.

        All signals use 20-hour TTL. Force-expire at 4 PM applies to same-day
        signals only — overnight signals survive to next session.
        """
        now_et = datetime.now(ET)
        hour, minute = now_et.hour, now_et.minute

        post_session = (hour > OVERNIGHT_THRESHOLD_HOUR or
                        (hour == OVERNIGHT_THRESHOLD_HOUR and minute >= OVERNIGHT_THRESHOLD_MIN))

        overnight = post_session

        if overnight:
            log.info(
                f"[{signal.get('ticker')}] Post-session signal accepted — "
                f"holding overnight for next session open (9:30 AM ET). "
                f"trigger=${signal.get('entry_price') or (signal.get('trigger') or {}).get('entry', '?')}"
            )

        watched = WatchedSignal(signal, overnight=overnight)
        with self._lock:
            existing = [w for w in self._pending
                        if w.ticker == watched.ticker and w.side == watched.side]
            if existing:
                log.info(
                    f"[{watched.ticker}] Already watching {watched.side} — "
                    f"replacing with higher-score signal"
                )
                if watched.score > existing[0].score:
                    self._pending = [w for w in self._pending if w not in existing]
                else:
                    return False

            self._pending.append(watched)

        overnight_count = sum(1 for w in self._pending if w.overnight)
        same_day_count  = sum(1 for w in self._pending if not w.overnight)
        log.info(
            f"[{watched.ticker}] Added to watch queue — "
            f"{same_day_count} same-day + {overnight_count} overnight = {len(self._pending)} total"
        )
        return True

    def watch(self, plan, local_order_id: str) -> bool:
        """
        Plan-aware entry called by ap/queue.py _dispatch().
        """
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
            "trigger": {
                "entry": getattr(plan, "trigger_price",     None),
                "stop":  getattr(plan, "stop_underlying",   None),
                "pt1":   getattr(plan, "target_underlying", None),
            },
        }
        # ── PRICE STALENESS CHECK ──────────────────────────────────────────────────
        # If current price has already run past the trigger by more than 1.5%,
        # the setup is stale — skip it rather than enter chasing a move.
        #
        # IMPORTANT: Skip this check for post-session / overnight signals.
        # Scanners fire post-market (~6 PM ET) for next-day setups using the
        # 4 PM close as the trigger. After-hours quotes drift away from that close,
        # which would cause 100% of overnight signals to be rejected at queue time
        # even though they should be (re-)evaluated at 9:30 AM next session.
        _now_et = datetime.now(ET)
        _post_session = (_now_et.hour > OVERNIGHT_THRESHOLD_HOUR or
                         (_now_et.hour == OVERNIGHT_THRESHOLD_HOUR and _now_et.minute >= OVERNIGHT_THRESHOLD_MIN))
        _trigger  = signal_dict.get("entry_price")
        _side     = signal_dict.get("side", "CALL").upper()
        _ticker   = signal_dict.get("ticker", "")
        _stop     = signal_dict.get("stop_price")
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
                    # CALL: price needs to be AT or BELOW trigger (breach = move up)
                    # PUT: price needs to be AT or ABOVE trigger (breach = move down)
                    _stale = False
                    if _side == "CALL" and _pct_from_trigger > 0.030:   # price already ran +3.0% past trigger
                        _stale = True
                    elif _side == "PUT" and _pct_from_trigger < -0.030:  # price already fell -3.0% past trigger
                        _stale = True
                    # Also skip if price has already moved through the STOP level
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
                    log.debug("[%s] Price check OK — $%.2f vs trigger $%.2f (%.1f%%)",
                              _ticker, _mid, _trigger, _pct_from_trigger*100)
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

        # At 4 PM ET: force-expire SAME-DAY signals only.
        # Overnight signals (post-market scanner setups) survive to next session.
        if now_et.hour >= 16:
            with self._lock:
                expired   = []
                surviving = []
                for w in self._pending:
                    if w.is_active and not w.overnight:
                        w.state = WatchState.EXPIRED
                        expired.append(w)
                        log.info(f"[{w.ticker}] Force-expired at market close (same-day signal)")
                    else:
                        surviving.append(w)
                self._pending = surviving
                if expired:
                    log.info(
                        f"EOD force-expire: {len(expired)} same-day signals expired, "
                        f"{len(surviving)} overnight signals held for next session"
                    )
            return

        # Outside session hours (before 9:30 AM ET): hold, don't poll
        if now_et.hour < 9 or (now_et.hour == 9 and now_et.minute < 30):
            return

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
        """Fetch single-ticker quote. Returns {} on error."""
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
