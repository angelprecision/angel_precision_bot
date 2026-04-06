# ap_entry_watcher.py — Angel Precision Real-Time Entry Watcher
# =============================================================================
# Gap 1 fix: The bot does NOT enter on signal. It enters on BREACH.
#
# How it works:
#   1. Signal arrives: "NVDA CALL, signal_bar_high=855.00, signal_bar_low=830.00"
#   2. Watcher parks that signal in a pending queue
#   3. Every N seconds, polls the live quote for each pending ticker
#   4. When bid/ask crosses the signal bar high (CALL) or low (PUT) → ENTER
#   5. If price never breaches within the watch window → EXPIRE, no trade
#   6. If price breaches in the WRONG direction first → INVALIDATE, no trade
#
# Why this matters:
#   Without this, the bot fires at the close of the signal bar.
#   That means you're entering BEFORE confirmation — chasing, not trading.
#   With this, you only enter when the market proves your direction.
#
# Integration:
#   watcher = APEntryWatcher(broker)
#   watcher.add_signal(signal_dict)       # from scanner
#   watcher.start()                       # background thread
#   watcher.on_trigger = execute_trade    # callback when breach confirmed
# =============================================================================

from __future__ import annotations

import time
import threading
import logging
from datetime import datetime, timezone, timedelta
from typing import Callable, Optional
from zoneinfo import ZoneInfo

log = logging.getLogger("ap.entry_watcher")
ET  = ZoneInfo("America/New_York")

# ── CONFIG ─────────────────────────────────────────────────────────────────────
POLL_INTERVAL_SEC    = 15     # check every 15 seconds
MAX_WATCH_MINUTES    = 90     # expire signal if no breach in 90 min
EOD_CUTOFF_HOUR      = 15     # 3:00 PM ET — stop adding new watches after this
EOD_CUTOFF_MIN       = 30     # 3:30 PM ET hard cutoff
WRONG_DIR_BUFFER_PCT = 0.001  # 0.1% buffer before declaring wrong-direction breach


# ── SIGNAL STATES ─────────────────────────────────────────────────────────────
class WatchState:
    PENDING     = "PENDING"      # watching, no breach yet
    TRIGGERED   = "TRIGGERED"    # breach confirmed → execute
    EXPIRED     = "EXPIRED"      # watch window closed, no breach
    INVALIDATED = "INVALIDATED"  # wrong direction breached first
    CANCELLED   = "CANCELLED"    # manually cancelled


# ── WATCHED SIGNAL ─────────────────────────────────────────────────────────────
class WatchedSignal:
    def __init__(self, signal: dict):
        self.signal        = signal
        self.ticker        = signal["ticker"]
        self.side          = signal["side"]           # CALL or PUT
        self.entry_trigger = float(signal["entry_price"])   # breach this
        self.stop_level    = float(signal["stop_price"])    # wrong-dir invalidation
        self.target_price  = float(signal["target_price"])
        self.score         = float(signal.get("score", 0))
        self.grade         = signal.get("grade", "B")

        self.state         = WatchState.PENDING
        self.created_at    = datetime.now(timezone.utc)
        self.triggered_at: Optional[datetime] = None
        self.trigger_price: Optional[float]   = None
        self.expire_at     = self.created_at + timedelta(minutes=MAX_WATCH_MINUTES)

        # Momentum confirmation: price must hold above trigger for N consecutive polls
        self.breach_count:    int   = 0      # consecutive polls above/below trigger
        self.breach_price:    float = 0.0    # price that initially breached
        self.last_quote_bid:  float = 0.0
        self.last_quote_ask:  float = 0.0

        log.info(
            f"[{self.ticker}] Watching {self.side} | "
            f"trigger=${self.entry_trigger} | stop=${self.stop_level} | "
            f"target=${self.target_price} | expires {self.expire_at.strftime('%H:%M UTC')}"
        )

    # Consecutive polls required ABOVE/BELOW trigger before firing
    MOMENTUM_POLLS_REQUIRED = 2   # hold for 2 polls (~30 sec) = no fake breakouts

    def check(self, bid: float, ask: float) -> str:
        """
        Check current quote against trigger/stop levels.
        Requires MOMENTUM_POLLS_REQUIRED consecutive polls confirming breach
        before triggering — eliminates fake breakouts and weak taps.
        Returns new state.
        """
        now = datetime.now(timezone.utc)
        self.last_quote_bid = bid
        self.last_quote_ask = ask

        # Expiry check
        if now >= self.expire_at:
            self.state = WatchState.EXPIRED
            log.info(f"[{self.ticker}] EXPIRED — no breach in {MAX_WATCH_MINUTES}min")
            return self.state

        if self.side == "CALL":
            # Breach candidate: ask above trigger
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
                        f"[{self.ticker}] 🟢 CALL CONFIRMED — ask=${ask:.2f} "
                        f"held above ${self.entry_trigger:.2f} for {self.breach_count} polls"
                    )
            else:
                # Price pulled back — reset momentum counter
                if self.breach_count > 0:
                    log.debug(f"[{self.ticker}] CALL breach reset — ask=${ask:.2f} pulled back below ${self.entry_trigger:.2f}")
                self.breach_count = 0

            # Invalidation: bid breaks BELOW stop level
            if bid <= self.stop_level * (1 - WRONG_DIR_BUFFER_PCT):
                self.state = WatchState.INVALIDATED
                self.breach_count = 0
                log.info(
                    f"[{self.ticker}] ❌ INVALIDATED — bid=${bid:.2f} "
                    f"broke stop=${self.stop_level:.2f} before trigger"
                )

        else:  # PUT
            # Breach candidate: bid below trigger
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
                        f"[{self.ticker}] 🔴 PUT CONFIRMED — bid=${bid:.2f} "
                        f"held below ${self.entry_trigger:.2f} for {self.breach_count} polls"
                    )
            else:
                if self.breach_count > 0:
                    log.debug(f"[{self.ticker}] PUT breach reset — bid=${bid:.2f} pulled back above ${self.entry_trigger:.2f}")
                self.breach_count = 0

            # Invalidation: ask breaks ABOVE stop level
            if ask >= self.stop_level * (1 + WRONG_DIR_BUFFER_PCT):
                self.state = WatchState.INVALIDATED
                self.breach_count = 0
                log.info(
                    f"[{self.ticker}] ❌ INVALIDATED — ask=${ask:.2f} "
                    f"broke stop=${self.stop_level:.2f} before trigger"
                )

        return self.state

    @property
    def is_active(self) -> bool:
        return self.state == WatchState.PENDING

    @property
    def minutes_watching(self) -> float:
        return (datetime.now(timezone.utc) - self.created_at).total_seconds() / 60


# ── MAIN WATCHER ──────────────────────────────────────────────────────────────

class APEntryWatcher:
    """
    Watches pending signals for entry breach confirmation.
    Runs as a background thread — non-blocking.

    Usage:
        def on_trigger(watched: WatchedSignal):
            broker.execute_option_trade(watched.signal, watched.trigger_price)

        watcher = APEntryWatcher(broker)
        watcher.on_trigger  = on_trigger
        watcher.on_expire   = lambda w: log.info(f"Expired: {w.ticker}")
        watcher.start()

        # When scanner fires a signal:
        watcher.add_signal(signal_dict)
    """

    def __init__(self, broker):
        self.broker         = broker
        self._pending:  list[WatchedSignal] = []
        self._lock          = threading.Lock()
        self._running       = False
        self._thread: Optional[threading.Thread] = None

        # Callbacks
        self.on_trigger:    Optional[Callable] = None
        self.on_expire:     Optional[Callable] = None
        self.on_invalidate: Optional[Callable] = None

    def add_signal(self, signal: dict) -> bool:
        """
        Add a signal to the watch queue.
        Returns False if EOD cutoff has passed (no new watches after 3:30 PM ET).
        """
        now_et = datetime.now(ET)
        if now_et.hour > EOD_CUTOFF_MIN // 60 or (
            now_et.hour == EOD_CUTOFF_HOUR and now_et.minute >= EOD_CUTOFF_MIN
        ):
            log.warning(
                f"[{signal.get('ticker')}] Signal rejected — past EOD cutoff "
                f"({EOD_CUTOFF_HOUR}:{EOD_CUTOFF_MIN:02d} ET)"
            )
            return False

        watched = WatchedSignal(signal)
        with self._lock:
            # Dedup: don't add same ticker + side twice
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

        log.info(f"[{watched.ticker}] Added to watch queue — {len(self._pending)} total watching")
        return True

    def start(self):
        """Start the background polling thread."""
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
        """Return status of all watched signals."""
        with self._lock:
            return [
                {
                    "ticker":    w.ticker,
                    "side":      w.side,
                    "trigger":   w.entry_trigger,
                    "target":    w.target_price,
                    "state":     w.state,
                    "mins_watching": round(w.minutes_watching, 1),
                    "score":     w.score,
                    "grade":     w.grade,
                }
                for w in self._pending
            ]

    def _poll_loop(self):
        """Background loop — polls quotes every POLL_INTERVAL_SEC seconds."""
        while self._running:
            try:
                self._check_all()
            except Exception as e:
                log.error(f"Watcher poll error: {e}", exc_info=True)
            time.sleep(POLL_INTERVAL_SEC)

    def _check_all(self):
        now_et = datetime.now(ET)

        # Hard EOD: force-expire all watches at market close
        if now_et.hour >= 16:
            with self._lock:
                for w in self._pending:
                    if w.is_active:
                        w.state = WatchState.EXPIRED
                        log.info(f"[{w.ticker}] Force-expired at market close")
                self._pending = []
            return

        with self._lock:
            active = [w for w in self._pending if w.is_active]

        if not active:
            return

        # Batch quote fetch — one API call for all active tickers
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
                    # Fall back to last price
                    last = float(quote.get("last", 0) or 0)
                    bid = ask = last

                new_state = w.check(bid, ask)

                if new_state == WatchState.TRIGGERED:
                    completed.append(("trigger", w))
                elif new_state in (WatchState.EXPIRED, WatchState.INVALIDATED):
                    completed.append(("done", w))

            # Remove completed from pending
            done_set = {id(w) for _, w in completed}
            self._pending = [w for w in self._pending if id(w) not in done_set]

        # Fire callbacks outside the lock
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

    def _fetch_quotes(self, tickers: list[str]) -> dict:
        """
        Fetch live quotes via Tradier for multiple tickers at once.
        Returns {ticker: {bid, ask, last}} dict.
        """
        symbols = ",".join(tickers)
        try:
            resp = self.broker.session.get(
                f"{self.broker.base_url}/v1/markets/quotes",
                params={"symbols": symbols, "greeks": "false"},
                headers={"Accept": "application/json"},
                timeout=5,
            )
            data  = resp.json()
            quotes_raw = data.get("quotes", {}).get("quote", [])
            if isinstance(quotes_raw, dict):
                quotes_raw = [quotes_raw]
            return {q["symbol"]: q for q in quotes_raw if q.get("symbol")}
        except Exception as e:
            log.warning(f"Tradier quote fetch failed: {e}")
            return {}
