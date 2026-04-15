# ap_exit_engine.py -- Angel Precision Time-Aware Exit Engine
# =============================================================================
# Gap 3 fix: 0DTE options have a hard enemy -- time. This engine knows that.
#
# The 4 exit conditions (checked in order every 30 seconds):
#
#   1. TARGET HIT -- underlying reaches the wick target price → EXIT full position
#
#   2. STOP HIT   -- underlying hits stop level → EXIT full position
#
#   3. PROFIT PROTECTION (time-aware)
#      If you're sitting on a good gain AND time is running out, lock it in.
#      Logic:
#        After 1:30 PM ET: if option P&L ≥ +150%, take 50% off the table
#        After 2:30 PM ET: if option P&L ≥ +80%, take 75% off
#        After 3:00 PM ET: if option P&L ≥ +30%, exit EVERYTHING
#        After 3:30 PM ET: EXIT EVERYTHING -- no exceptions
#
#   4. THETA STOP (decay kill switch)
#      If option has lost >50% of its value AND it's past noon → EXIT
#      Theta is accelerating, the trade is against you, don't let it go to zero.
#
# Philosophy:
#   A winning 0DTE trade that goes to zero because you held too long
#   is not a loss you can backtest away. This engine prevents it.
# =============================================================================

from __future__ import annotations

import time
import threading
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Callable
from zoneinfo import ZoneInfo

log = logging.getLogger("ap.exit_engine")
ET  = ZoneInfo("America/New_York")

# ── TIME THRESHOLDS (ET) ──────────────────────────────────────────────────────
PROFIT_PROTECT_1_HOUR = 13   # 1:30 PM -- take 50% if +150%
PROFIT_PROTECT_1_MIN  = 30
PROFIT_PROTECT_2_HOUR = 14   # 2:30 PM -- take 75% if +80%
PROFIT_PROTECT_2_MIN  = 30
PROFIT_PROTECT_3_HOUR = 15   # 3:00 PM -- exit all if +30%
PROFIT_PROTECT_3_MIN  = 0
EOD_HARD_CLOSE_HOUR   = 15   # 3:30 PM -- EXIT EVERYTHING
EOD_HARD_CLOSE_MIN    = 30
POLL_INTERVAL_SEC     = 30   # check every 30 seconds

# ── P&L THRESHOLDS ────────────────────────────────────────────────────────────
THETA_STOP_LOSS_PCT   = -0.50  # -50% on option → theta kill if past noon
SCALE_OUT_1_THRESHOLD = 1.50   # +150% → scale out 50% at window 1
SCALE_OUT_2_THRESHOLD = 0.80   # +80%  → scale out 75% at window 2
PROTECT_3_THRESHOLD   = 0.30   # +30%  → exit all at window 3


# ── POSITION TRACKER ─────────────────────────────────────────────────────────

@dataclass
class ManagedPosition:
    # Identity
    ticker:           str
    option_symbol:    str
    side:             str           # CALL or PUT
    quantity:         int           # contracts
    entry_price:      float         # option price paid (per share, so ×100)
    underlying_entry: float         # underlying price at entry

    # Levels (from signal)
    underlying_target: float
    underlying_stop:   float

    # Context flags (set from context engine at entry time)
    is_trend_day:         bool  = False   # relaxes exit thresholds
    trend_direction:      str   = ""      # "uptrend" / "downtrend" -- must match side

    # State
    current_option_price: float = 0.0
    current_underlying:   float = 0.0
    quantity_remaining:   int   = 0
    scale_outs_done:      int   = 0   # 0, 1, or 2
    closed:               bool  = False
    close_reason:         str   = ""
    opened_at:            datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # P&L tracking
    realized_pnl:   float = 0.0
    unrealized_pnl: float = 0.0

    def __post_init__(self):
        self.quantity_remaining = self.quantity

    @property
    def option_pnl_pct(self) -> float:
        """Current option P&L as percentage of entry cost."""
        if self.entry_price <= 0 or self.current_option_price <= 0:
            return 0.0
        return (self.current_option_price - self.entry_price) / self.entry_price

    @property
    def underlying_pnl_pct(self) -> float:
        if self.underlying_entry <= 0 or self.current_underlying <= 0:
            return 0.0
        if self.side == "CALL":
            return (self.current_underlying - self.underlying_entry) / self.underlying_entry
        else:
            return (self.underlying_entry - self.current_underlying) / self.underlying_entry

    @property
    def is_at_target(self) -> bool:
        if self.side == "CALL":
            return self.current_underlying >= self.underlying_target
        return self.current_underlying <= self.underlying_target

    @property
    def is_at_stop(self) -> bool:
        if self.side == "CALL":
            return self.current_underlying <= self.underlying_stop
        return self.current_underlying >= self.underlying_stop


# ── EXIT DECISION ─────────────────────────────────────────────────────────────

@dataclass
class ExitDecision:
    action:         str    # "HOLD", "SCALE_OUT", "CLOSE_ALL", "STOP"
    quantity:       int    # contracts to close (0 = hold)
    reason:         str
    urgency:        str    # "NORMAL", "HIGH", "IMMEDIATE"
    pnl_pct:        float  = 0.0

    @property
    def should_act(self) -> bool:
        return self.action != "HOLD"


# ── EXIT LOGIC ────────────────────────────────────────────────────────────────

def evaluate_exit(pos: ManagedPosition, now_et: Optional[datetime] = None) -> ExitDecision:
    """
    Core exit evaluation. Called every POLL_INTERVAL_SEC for each position.
    Returns ExitDecision.
    """
    if now_et is None:
        now_et = datetime.now(ET)

    hour, minute = now_et.hour, now_et.minute
    option_pnl   = pos.option_pnl_pct
    qty_rem      = pos.quantity_remaining

    # ── 1. TARGET HIT ────────────────────────────────────────────────────────
    if pos.is_at_target:
        return ExitDecision(
            action="CLOSE_ALL", quantity=qty_rem,
            reason=f"TARGET HIT -- underlying ${pos.current_underlying:.2f} reached ${pos.underlying_target:.2f}",
            urgency="IMMEDIATE", pnl_pct=option_pnl
        )

    # ── 2. STOP HIT ──────────────────────────────────────────────────────────
    if pos.is_at_stop:
        return ExitDecision(
            action="STOP", quantity=qty_rem,
            reason=f"STOP HIT -- underlying ${pos.current_underlying:.2f} at stop ${pos.underlying_stop:.2f}",
            urgency="IMMEDIATE", pnl_pct=option_pnl
        )

    # ── TREND DAY MULTIPLIERS (must be defined before all exit checks) ────────
    direction_aligns = (
        pos.is_trend_day and (
            (pos.side == "CALL" and pos.trend_direction == "uptrend") or
            (pos.side == "PUT"  and pos.trend_direction == "downtrend")
        )
    )
    trend_bonus_threshold = 1.35 if direction_aligns else 1.0
    trend_bonus_window    = 30   if direction_aligns else 0

    # ── 3. EOD HARD CLOSE ────────────────────────────────────────────────────
    past_eod = (hour > EOD_HARD_CLOSE_HOUR or
                (hour == EOD_HARD_CLOSE_HOUR and minute >= EOD_HARD_CLOSE_MIN))
    if past_eod:
        return ExitDecision(
            action="CLOSE_ALL", quantity=qty_rem,
            reason=f"EOD FORCE CLOSE -- {hour}:{minute:02d} ET past {EOD_HARD_CLOSE_HOUR}:{EOD_HARD_CLOSE_MIN:02d}",
            urgency="IMMEDIATE", pnl_pct=option_pnl
        )

    # ── 4. PROFIT PROTECTION -- WINDOW 3 (3:00 PM+) ───────────────────────────
    past_window3 = (hour > PROFIT_PROTECT_3_HOUR or
                    (hour == PROFIT_PROTECT_3_HOUR and minute >= PROFIT_PROTECT_3_MIN))
    protect3_thresh = 0.50 if direction_aligns else PROTECT_3_THRESHOLD
    if past_window3 and option_pnl >= protect3_thresh:
        trend_note = " [trend day -- raised to 50% threshold]" if direction_aligns else ""
        return ExitDecision(
            action="CLOSE_ALL", quantity=qty_rem,
            reason=f"PROFIT PROTECT W3 -- +{option_pnl*100:.0f}% at 3PM+{trend_note}",
            urgency="HIGH", pnl_pct=option_pnl
        )

    # ── 5. PROFIT PROTECTION -- WINDOW 2 (2:30 PM+, or 3:00 PM on trend day) ─
    w2_hour = PROFIT_PROTECT_2_HOUR
    w2_min  = PROFIT_PROTECT_2_MIN + trend_bonus_window
    if w2_min >= 60: w2_hour += 1; w2_min -= 60
    past_window2 = (hour > w2_hour or (hour == w2_hour and minute >= w2_min))
    scale2_threshold = SCALE_OUT_2_THRESHOLD * trend_bonus_threshold
    if past_window2 and option_pnl >= scale2_threshold and pos.scale_outs_done < 2:
        qty_close = max(1, round(qty_rem * (0.50 if direction_aligns else 0.75)))
        trend_note = " [TREND DAY -- reduced scale]" if direction_aligns else ""
        return ExitDecision(
            action="SCALE_OUT", quantity=qty_close,
            reason=f"PROFIT PROTECT W2 -- +{option_pnl*100:.0f}% at {w2_hour}:{w2_min:02d}PM+ scale{trend_note}",
            urgency="HIGH", pnl_pct=option_pnl
        )

    # ── 6. PROFIT PROTECTION -- WINDOW 1 (1:30 PM+, or 2:00 PM on trend day) ─
    w1_hour = PROFIT_PROTECT_1_HOUR
    w1_min  = PROFIT_PROTECT_1_MIN + trend_bonus_window
    if w1_min >= 60: w1_hour += 1; w1_min -= 60
    past_window1 = (hour > w1_hour or (hour == w1_hour and minute >= w1_min))
    scale1_threshold = SCALE_OUT_1_THRESHOLD * trend_bonus_threshold
    if past_window1 and option_pnl >= scale1_threshold and pos.scale_outs_done < 1:
        qty_close = max(1, round(qty_rem * (0.35 if direction_aligns else 0.50)))
        trend_note = " [TREND DAY -- let runner breathe]" if direction_aligns else ""
        return ExitDecision(
            action="SCALE_OUT", quantity=qty_close,
            reason=f"PROFIT PROTECT W1 -- +{option_pnl*100:.0f}% at {w1_hour}:{w1_min:02d}PM+ scale{trend_note}",
            urgency="NORMAL", pnl_pct=option_pnl
        )

    # ── 7. THETA KILL SWITCH (past noon, down >50%) ───────────────────────────
    past_noon = hour >= 12
    if past_noon and option_pnl <= THETA_STOP_LOSS_PCT:
        return ExitDecision(
            action="CLOSE_ALL", quantity=qty_rem,
            reason=f"THETA STOP -- option down {option_pnl*100:.0f}% after noon, cutting losses",
            urgency="NORMAL", pnl_pct=option_pnl
        )

    return ExitDecision(action="HOLD", quantity=0, reason="No exit condition met", urgency="NORMAL", pnl_pct=option_pnl)


# ── EXIT ENGINE ───────────────────────────────────────────────────────────────

class APExitEngine:
    """
    Manages all open positions with time-aware exit logic.
    Runs as a background thread.

    Usage:
        engine = APExitEngine(broker)
        engine.on_exit = lambda pos, decision: broker.close_position(pos, decision)
        engine.start()

        # When a trade is entered:
        engine.add_position(ManagedPosition(...))
    """

    def __init__(self, broker, kill_switch_fn=None, email: str = ""):
        self.broker           = broker
        self._email           = email              # used to name thread per-client
        self._positions: list[ManagedPosition] = []
        self._lock            = threading.Lock()
        self._running         = False
        self._thread: Optional[threading.Thread] = None
        self.on_exit: Optional[Callable]  = None   # callback(pos, ExitDecision)
        self.on_scale: Optional[Callable] = None   # callback(pos, ExitDecision, qty)
        self._kill_switch_fn  = kill_switch_fn     # callable() → bool | None

    def add_position(self, pos: ManagedPosition):
        with self._lock:
            self._positions.append(pos)
        log.info(
            f"[{pos.ticker}] Position added to exit engine | "
            f"{pos.side} {pos.quantity}x {pos.option_symbol} "
            f"@ ${pos.entry_price:.2f} | "
            f"target=${pos.underlying_target} stop=${pos.underlying_stop}"
        )

    def start(self):
        if self._running:
            return
        self._running = True
        # Thread name must match self_healing's components dict:
        # "exit_engine": (f"ap-exit-engine-{email}", ...)
        thread_name = f"ap-exit-engine-{self._email}" if self._email else "ap-exit-engine"
        self._thread  = threading.Thread(
            target=self._exit_loop,
            daemon=True,
            name=thread_name
        )
        self._thread.start()
        log.info(f"APExitEngine started [{thread_name}]")

    def stop(self):
        self._running = False

    def active_positions(self) -> list[ManagedPosition]:
        with self._lock:
            return [p for p in self._positions if not p.closed]

    def seed_from_db(self, position_manager):
        """
        Re-hydrate in-memory positions from DB on startup.
        Prevents open positions from losing exit protection after a restart.
        """
        try:
            rows = position_manager.get_active_positions()
            if not rows:
                log.info("seed_from_db: no active positions to seed")
                return
            seeded = 0
            for row in rows:
                try:
                    mp = ManagedPosition(
                        ticker=row.get("underlying", ""),
                        option_symbol=row.get("contract", ""),
                        side=row.get("direction", "CALL"),
                        quantity=int(row.get("qty", 1)),
                        entry_price=float(row.get("avg_fill", 0)),
                        underlying_entry=0.0,
                        underlying_target=float(row.get("target_underlying") or 0),
                        underlying_stop=float(row.get("stop_underlying") or 0),
                    )
                    mp.current_option_price = float(row.get("avg_fill", 0))
                    self.add_position(mp)
                    seeded += 1
                except Exception as e:
                    log.warning("seed_from_db: skipping row %s: %s", row.get("id"), e)
            log.info("seed_from_db: seeded %d position(s) into exit engine", seeded)
        except Exception as e:
            log.error("seed_from_db FAILED — open positions have NO exit protection: %s", e)

    def _exit_loop(self):
        while self._running:
            try:
                self._check_all_positions()
            except Exception as e:
                log.error(f"Exit engine error: {e}", exc_info=True)
            time.sleep(POLL_INTERVAL_SEC)

    def _check_all_positions(self):
        # ── Gap 1: Kill check at poll start (pre-fetch) ─────────────────────
        if self._kill_switch_fn and self._kill_switch_fn():
            log.debug("Exit engine poll skipped -- kill switch active")
            return
        now_et = datetime.now(ET)
        active = self.active_positions()
        if not active:
            return

        # Batch quote fetch for all tickers
        tickers = list({p.ticker for p in active})
        option_symbols = list({p.option_symbol for p in active})

        try:
            underlying_quotes = self._fetch_quotes(tickers)
            option_quotes      = self._fetch_option_quotes(option_symbols)
        except Exception as e:
            log.warning(f"Quote fetch error: {e}")
            return

        actions_to_take = []
        with self._lock:
            for pos in active:
                # Update prices
                uq = underlying_quotes.get(pos.ticker, {})
                oq = option_quotes.get(pos.option_symbol, {})

                if uq:
                    last = float(uq.get("last") or uq.get("bid") or 0)
                    if last > 0:
                        pos.current_underlying = last

                if oq:
                    bid = float(oq.get("bid", 0) or 0)
                    ask = float(oq.get("ask", 0) or 0)
                    if bid > 0 and ask > 0:
                        pos.current_option_price = (bid + ask) / 2

                # Evaluate exit
                if pos.current_underlying > 0 and pos.current_option_price > 0:
                    decision = evaluate_exit(pos, now_et)
                    if decision.should_act:
                        actions_to_take.append((pos, decision))

        # ── Gap 2: Kill check post-fetch, pre-execute ───────────────────────────
        # Quote fetch takes 1-5s on live Tradier. Kill may have fired during
        # that I/O window. Filter out non-protective exits but allow EOD/stop-loss.
        if self._kill_switch_fn and self._kill_switch_fn():
            protective = [(p, d) for p, d in actions_to_take
                          if (d.reason or "") in ("EOD_FORCE_CLOSE", "STOP_LOSS", "MAX_LOSS")]
            blocked = len(actions_to_take) - len(protective)
            if blocked:
                log.warning(
                    f"Exit engine: kill switch active -- blocking {blocked} non-protective exit(s), "
                    f"allowing {len(protective)} protective exit(s)"
                )
            actions_to_take = protective
            if not actions_to_take:
                return

        # Execute actions
        for pos, decision in actions_to_take:
            log.info(
                f"[{pos.ticker}] EXIT SIGNAL: {decision.action} "
                f"qty={decision.quantity} | {decision.reason} | "
                f"P&L={decision.pnl_pct*100:+.1f}%"
            )
            if decision.action == "SCALE_OUT":
                pos.scale_outs_done += 1
                pos.quantity_remaining -= decision.quantity
                if self.on_scale:
                    if self._kill_switch_fn and self._kill_switch_fn():
                        log.warning(
                            f"[{pos.ticker}] KILL ACTIVE at on_scale -- "
                            f"reverting scale_out and skipping broker order"
                        )
                        pos.scale_outs_done   -= 1
                        pos.quantity_remaining += decision.quantity
                        continue
                    self.on_scale(pos, decision)
            else:
                if self.on_exit:
                    # ── Gap 3: Final per-position kill check before broker call ──
                    # Catches kill that fires between execute loop iterations.
                    exit_reason = decision.reason or ""
                    if self._kill_switch_fn and self._kill_switch_fn():
                        if exit_reason not in ("EOD_FORCE_CLOSE", "STOP_LOSS", "MAX_LOSS"):
                            log.warning(
                                f"[{pos.ticker}] Kill switch active — blocking non-protective exit: {exit_reason}"
                            )
                            continue
                        else:
                            log.info(
                                f"[{pos.ticker}] Kill switch active but allowing protective exit: {exit_reason}"
                            )
                    # 1. Call broker first
                    try:
                        self.on_exit(pos, decision)
                    except Exception as e:
                        log.error("Exit order FAILED for %s — position remains tracked: %s", pos.symbol, e)
                        continue  # don't remove, retry next cycle
                # 2. Only mark closed after successful broker submission
                pos.closed = True
                pos.close_reason = decision.reason
                with self._lock:
                    self._positions = [p for p in self._positions if p is not pos]

    def _fetch_quotes(self, tickers: list[str]) -> dict:
        try:
            resp = self.broker.session.get(
                f"{self.broker.base_url}/v1/markets/quotes",
                params={"symbols": ",".join(tickers), "greeks": "false"},
                headers={"Accept": "application/json"},
                timeout=5,
            )
            data = resp.json()
            raw  = data.get("quotes", {}).get("quote", [])
            if isinstance(raw, dict): raw = [raw]
            return {q["symbol"]: q for q in raw if q.get("symbol")}
        except Exception as e:
            log.error("Quote fetch failed: %s", e, exc_info=True)
            return {}  # caller must handle empty dict as "no data available"

    def _fetch_option_quotes(self, symbols: list[str]) -> dict:
        try:
            resp = self.broker.session.get(
                f"{self.broker.base_url}/v1/markets/quotes",
                params={"symbols": ",".join(symbols), "greeks": "true"},
                headers={"Accept": "application/json"},
                timeout=5,
            )
            data = resp.json()
            raw  = data.get("quotes", {}).get("quote", [])
            if isinstance(raw, dict): raw = [raw]
            return {q["symbol"]: q for q in raw if q.get("symbol")}
        except Exception as e:
            log.error("Option quote fetch failed: %s", e, exc_info=True)
            return {}  # caller must handle empty dict as "no data available"
