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
PROFIT_PROTECT_1_HOUR = 11   # 11:00 AM -- scale out 50% if +40%
PROFIT_PROTECT_1_MIN  = 0
PROFIT_PROTECT_2_HOUR = 13   # 1:00 PM  -- scale out 75% if +25%
PROFIT_PROTECT_2_MIN  = 0
PROFIT_PROTECT_3_HOUR = 14   # 2:00 PM  -- exit all if +15%
PROFIT_PROTECT_3_MIN  = 0
EOD_HARD_CLOSE_HOUR   = 15   # 3:30 PM  -- EXIT EVERYTHING
EOD_HARD_CLOSE_MIN    = 30
POLL_INTERVAL_SEC     = 8    # check every 8 seconds — catch TP windows faster

# ── P&L THRESHOLDS ────────────────────────────────────────────────────────────
THETA_STOP_LOSS_PCT   = -0.35  # -35% on option → stop (was -50%)
SCALE_OUT_1_THRESHOLD = 0.40   # +40%  → scale out 50% at window 1 (was +150%)
SCALE_OUT_2_THRESHOLD = 0.25   # +25%  → scale out 75% at window 2 (was +80%)
PROTECT_3_THRESHOLD   = 0.15   # +15%  → exit all at window 3 (was +30%)

# ── IMMEDIATE TAKE-PROFIT (any time, no window gate) ──────────────────────────
IMMEDIATE_TP_PCT      = 0.25   # +25% → scale out 70% immediately
HARD_STOP_PCT         = -0.30  # -30% → exit immediately regardless of time
PROFIT_LOCK_PCT       = 0.12   # once at +25%, lock: don't fall below +12%
TRAIL_DROP_FROM_PEAK  = 0.10   # if peak was +25%+, exit if drops 10pts from peak
SMALL_WIN_PCT         = 0.10   # +10% → small win capture (see time gates below)
SMALL_WIN_TRAIL       = 0.07   # after +10% seen, don't let it fall below +3%


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

    # DB / order identity
    position_id:       str  = ""
    client_id:         str  = ""
    signal_id:         str  = ""

    # Context flags
    is_trend_day:         bool  = False
    trend_direction:      str   = ""

    # State
    current_option_price: float = 0.0
    current_bid:          float = 0.0
    current_ask:          float = 0.0
    current_underlying:   float = 0.0
    quantity_remaining:   int   = 0
    scale_outs_done:      int   = 0
    peak_pnl_pct:         float = 0.0   # highest option P&L seen
    touched_profit:       bool  = False  # True once position was ever green
    max_profit_seen:      float = 0.0   # highest positive P&L ever seen
    closed:               bool  = False
    close_reason:         str   = ""
    opened_at:            datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # Exit coordination — engine signals intent; order truth decides closure
    exit_in_flight:       bool  = False
    pending_exit_reason:  str   = ""
    pending_exit_qty:     int   = 0
    last_exit_signal_ts:  Optional[datetime] = None

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
        # Guard: zero or negative target means "not set" — rely on P&L exits only.
        # Prevents false TARGET HIT on first quote poll when scanner omits pt1.
        if self.underlying_target <= 0:
            return False
        if self.side == "CALL":
            return self.current_underlying >= self.underlying_target
        return self.current_underlying <= self.underlying_target

    @property
    def is_at_stop(self) -> bool:
        # Guard: zero or negative stop means "not set" — rely on theta stop / P&L.
        # Prevents false STOP HIT when scanner omits stop level.
        if self.underlying_stop <= 0:
            return False
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

    # ── IMMEDIATE TAKE-PROFIT (fires any time, no window gate) ──────────────────
    # Scale-out model: lock in the bulk, leave a runner.
    #   1 contract  → CLOSE_ALL (can't split)
    #   2 contracts → sell 1, run 1
    #   3+          → sell 70%, run 30% (min 1 runner)
    if option_pnl >= IMMEDIATE_TP_PCT and pos.scale_outs_done == 0:
        if qty_rem == 1:
            # Single contract — take it all
            return ExitDecision(
                action="CLOSE_ALL", quantity=qty_rem,
                reason=f"IMMEDIATE TP (full) -- +{option_pnl*100:.0f}% | 1 contract, no split",
                urgency="IMMEDIATE", pnl_pct=option_pnl
            )
        else:
            # Multi-contract — sell 70%, keep runner(s)
            qty_lock = max(1, round(qty_rem * 0.70))
            qty_run  = qty_rem - qty_lock
            return ExitDecision(
                action="SCALE_OUT", quantity=qty_lock,
                reason=(
                    f"IMMEDIATE TP (partial) -- +{option_pnl*100:.0f}% | "
                    f"locking {qty_lock}/{qty_rem} contracts, running {qty_run} with trail"
                ),
                urgency="IMMEDIATE", pnl_pct=option_pnl
            )

    # ── RUNNER TRAIL (after scale-out, protect the runner) ───────────────────
    # Once we've done a scale-out, trail the remaining contracts tightly.
    # Exit the runner if it drops more than 15pts from where we scaled.
    if pos.scale_outs_done >= 1 and pos.peak_pnl_pct > 0:
        runner_drop = pos.peak_pnl_pct - option_pnl
        if runner_drop >= 0.15 or option_pnl <= 0:
            return ExitDecision(
                action="CLOSE_ALL", quantity=qty_rem,
                reason=(
                    f"RUNNER TRAIL EXIT -- peaked +{pos.peak_pnl_pct*100:.0f}%, "
                    f"now +{option_pnl*100:.0f}%, protecting runner gains"
                ),
                urgency="HIGH", pnl_pct=option_pnl
            )

    # ── SMALL WIN CAPTURE ──────────────────────────────────────────────────────
    # Once peak >= +10%, protect the gain:
    #   Rule A: if it drops 7pts from peak while still positive → lock it in
    #   Rule B: if underlying reached 60%+ toward signal target → take option gain
    if pos.max_profit_seen >= SMALL_WIN_PCT:
        floor = max(0.03, pos.max_profit_seen - SMALL_WIN_TRAIL)  # at least +3%
        if 0 < option_pnl <= floor:
            return ExitDecision(
                action="CLOSE_ALL", quantity=qty_rem,
                reason=(
                    f"SMALL WIN LOCK — peaked +{pos.max_profit_seen*100:.0f}%, "
                    f"protecting +{option_pnl*100:.0f}%"
                ),
                urgency="HIGH", pnl_pct=option_pnl
            )

    # Underlying progress: if 60%+ toward scanner target, take option gain now
    _entry_u  = pos.underlying_entry
    _target_u = pos.underlying_target
    _curr_u   = pos.current_underlying
    if _entry_u and _target_u and _curr_u:
        _range = abs(_target_u - _entry_u)
        if _range > 0:
            _progress = abs(_curr_u - _entry_u) / _range
            if _progress >= 0.60 and option_pnl >= 0.05:
                return ExitDecision(
                    action="CLOSE_ALL", quantity=qty_rem,
                    reason=(
                        f"UNDERLYING PROGRESS EXIT — {_progress*100:.0f}% toward target, "
                        f"locking option +{option_pnl*100:.0f}%"
                    ),
                    urgency="HIGH", pnl_pct=option_pnl
                )

    # ── TOUCHED PROFIT PROTECTION ──────────────────────────────────────────────
    # Was ever green → went negative → exit immediately. Capital protection first.
    if pos.touched_profit and option_pnl <= -0.05:
        return ExitDecision(
            action="CLOSE_ALL", quantity=qty_rem,
            reason=(
                f"TOUCHED PROFIT STOP — was +{pos.max_profit_seen*100:.0f}% "
                f"now {option_pnl*100:.0f}% — protecting capital"
            ),
            urgency="IMMEDIATE", pnl_pct=option_pnl
        )

    # ── HARD STOP (fires any time, no time gate) ─────────────────────────────
    if option_pnl <= HARD_STOP_PCT:
        return ExitDecision(
            action="STOP", quantity=qty_rem,
            reason=f"HARD STOP -- {option_pnl*100:.0f}% exceeded -{abs(HARD_STOP_PCT)*100:.0f}% max loss",
            urgency="IMMEDIATE", pnl_pct=option_pnl
        )

    # ── PROFIT LOCK (once we hit peak, don't give it all back) ──────────────────
    if pos.peak_pnl_pct >= IMMEDIATE_TP_PCT:
        # Hard floor: don't fall below PROFIT_LOCK_PCT
        if option_pnl <= PROFIT_LOCK_PCT:
            return ExitDecision(
                action="CLOSE_ALL", quantity=qty_rem,
                reason=f"PROFIT LOCK -- peaked at +{pos.peak_pnl_pct*100:.0f}%, fell to +{option_pnl*100:.0f}% — locking in",
                urgency="HIGH", pnl_pct=option_pnl
            )
        # Trailing stop: if dropped 10+ points from peak, exit
        drop_from_peak = pos.peak_pnl_pct - option_pnl
        if drop_from_peak >= TRAIL_DROP_FROM_PEAK:
            return ExitDecision(
                action="CLOSE_ALL", quantity=qty_rem,
                reason=f"TRAILING STOP -- peak +{pos.peak_pnl_pct*100:.0f}%, dropped {drop_from_peak*100:.0f}pts to +{option_pnl*100:.0f}%",
                urgency="HIGH", pnl_pct=option_pnl
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

    def __init__(self, broker, kill_switch_fn=None, email: str = "",
                 data_broker=None):
        self.broker           = broker
        # data_broker: live api.tradier.com broker for real-time quotes.
        # Falls back to execution broker if not provided (sandbox = delayed).
        self._quote_broker    = data_broker or broker
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
            for existing in self._positions:
                same_id  = bool(pos.position_id and existing.position_id == pos.position_id)
                same_sym = (existing.ticker == pos.ticker and
                            existing.option_symbol == pos.option_symbol and
                            not existing.closed)
                if same_id or same_sym:
                    log.debug("[%s] Exit engine already tracking %s | pos_id=%s",
                              self._email or pos.ticker, pos.option_symbol, pos.position_id or "n/a")
                    return
            self._positions.append(pos)
        log.info(
            f"[{pos.ticker}] Position added to exit engine | "
            f"{pos.side} {pos.quantity}x {pos.option_symbol} "
            f"@ ${pos.entry_price:.2f} | "
            f"target=${pos.underlying_target} stop=${pos.underlying_stop} "
            f"| pos_id={pos.position_id or 'n/a'}"
        )

    def start(self):
        if self._thread and self._thread.is_alive():
            log.debug("APExitEngine already running [%s]", self._email or "default")
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

    # ── BROKER RECONCILIATION HOOKS ──────────────────────────────────────────
    # Called by APOrderMonitor / APBrokerReconciler after confirming broker truth.

    def mark_position_closed(self, position_id: str, reason: str = ""):
        """Called after broker + DB confirm full exit fill."""
        if not position_id:
            return
        with self._lock:
            for pos in self._positions:
                if pos.position_id == position_id:
                    pos.closed = True
                    pos.close_reason = reason or pos.close_reason
            self._positions = [p for p in self._positions
                                if not (p.position_id == position_id and p.closed)]
        log.info("[exit_eng] Closed (broker-confirmed) | pos_id=%s reason=%s", position_id, reason)

    def clear_exit_in_flight(self, position_id: str):
        """Called when exit order is canceled/rejected/expired — engine may retry."""
        if not position_id:
            return
        with self._lock:
            for pos in self._positions:
                if pos.position_id == position_id:
                    pos.exit_in_flight      = False
                    pos.pending_exit_reason = ""
                    pos.pending_exit_qty    = 0
                    pos.last_exit_signal_ts = None
        log.info("[exit_eng] Exit in-flight cleared | pos_id=%s", position_id)

    def note_partial_exit_fill(self, position_id: str, qty_filled: int):
        """Called when broker confirms a partial exit fill."""
        if not position_id or qty_filled <= 0:
            return
        with self._lock:
            for pos in self._positions:
                if pos.position_id == position_id:
                    pos.quantity_remaining = max(0, pos.quantity_remaining - int(qty_filled))
                    pos.exit_in_flight      = False
                    pos.pending_exit_reason = ""
                    pos.pending_exit_qty    = 0
                    pos.last_exit_signal_ts = None
                    if pos.quantity_remaining == 0:
                        pos.closed = True
            self._positions = [p for p in self._positions if not p.closed]
        log.info("[exit_eng] Partial fill noted | pos_id=%s qty_filled=%d", position_id, qty_filled)

    def _run_sentinels(self):
        """GPS trackers — scream loudly if a position is in a bad state with no action."""
        from datetime import datetime, timezone as _tz
        now = datetime.now(_tz.utc)
        try:
            from ap_proof_logger import funnel as _sf
        except Exception: _sf = None
        for pos in self._positions:
            age_min = (now - pos.opened_at).total_seconds() / 60 if pos.opened_at else 0
            pnl     = pos.option_pnl_pct
            peak    = pos.peak_pnl_pct

            # Sentinel 1: Hit TP threshold but no exit was ever submitted
            if peak >= IMMEDIATE_TP_PCT and not pos.exit_in_flight and age_min > 1:
                log.error(
                    "[SENTINEL] %s | MISSED TP — peaked +%.0f%% but no exit submitted! "
                    "pos=%s pnl=%.1f%% age=%.0fm",
                    pos.ticker, peak*100, pos.position_id, pnl*100, age_min
                )

            # Sentinel 2: Hit hard stop but no exit submitted
            if pnl <= HARD_STOP_PCT and not pos.exit_in_flight and age_min > 1:
                log.error(
                    "[SENTINEL] %s | MISSED STOP — at %.0f%% but no exit submitted! "
                    "pos=%s age=%.0fm — FORCING EXIT NOW",
                    pos.ticker, pnl*100, pos.position_id, age_min
                )
                # Force fire the exit callback directly
                try:
                    decision = ExitDecision(
                        action="CLOSE_ALL", quantity=pos.quantity_remaining,
                        reason=f"SENTINEL FORCED EXIT — {pnl*100:.0f}% with no exit order",
                        urgency="IMMEDIATE", pnl_pct=pnl
                    )
                    if self.on_exit:
                        self.on_exit(pos, decision)
                except Exception as _e:
                    log.error("[SENTINEL] Force exit failed for %s: %s", pos.ticker, _e)

            # Sentinel 3: Exit in flight for > 5 min with no fill → alert
            if pos.exit_in_flight and pos.last_exit_signal_ts:
                flight_sec = (now - pos.last_exit_signal_ts).total_seconds()
                if flight_sec > 300:
                    log.warning(
                        "[SENTINEL] %s | EXIT STUCK — in-flight %.0fs with no fill | "
                        "pos=%s broker may have rejected silently",
                        pos.ticker, flight_sec, pos.position_id
                    )

    def _eligible_for_new_exit(self, pos: "ManagedPosition", now_utc: datetime) -> bool:
        """Returns True if position can receive a new exit signal."""
        if not pos.exit_in_flight:
            return True
        # Safety valve: 5-min timeout if reconciler hasn't called back.
        # But if the last exit was REJECTED (e.g. expired contract), don't blindly retry
        # every 5 minutes — check broker status first via reconciler.
        if pos.last_exit_signal_ts:
            age = (now_utc - pos.last_exit_signal_ts).total_seconds()
            if age >= 300:
                # Check if last exit attempt was rejected (contract expired/invalid)
                if getattr(pos, "last_exit_rejected", False):
                    # Don't retry rejected exits — position needs reconciler to handle it
                    log.warning(
                        "[%s] Exit was REJECTED (likely expired contract) — "
                        "not retrying; reconciler will handle", pos.ticker
                    )
                    return False
                log.warning("[%s] Exit in-flight safety timeout (300s) — allowing retry", pos.ticker)
                pos.exit_in_flight = False
                return True
        return False

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
                        ticker=row.get("underlying", "") or row.get("symbol", ""),
                        option_symbol=row.get("contract", ""),
                        side=row.get("direction", "CALL"),
                        quantity=int(row.get("qty", 1) or 1),
                        entry_price=float(row.get("avg_fill", 0) or 0),
                        underlying_entry=float(row.get("underlying_entry", 0) or 0),
                        underlying_target=float(row.get("target_underlying") or 0),
                        underlying_stop=float(row.get("stop_underlying") or 0),
                        position_id=str(row.get("id") or ""),
                        client_id=str(row.get("client_id") or ""),
                        signal_id=str(row.get("signal_id") or ""),
                    )
                    mp.current_option_price = float(row.get("avg_fill", 0) or 0)
                    # Restore runner state from DB so restarts pick up mid-trade correctly
                    mp.scale_outs_done    = int(row.get("scale_outs_done", 0) or 0)
                    _db_qty = int(row.get("qty", 0) or 0)
                    if _db_qty > 0:
                        mp.quantity_remaining = _db_qty
                    mp.current_underlying = float(row.get("underlying_entry", 0) or 0)
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

            # Heartbeat so self-healer knows exit engine is alive and progressing
            try:
                from ap.self_healing import get_healer as _get_healer
                _healer = _get_healer()
                if _healer is not None and self._email:
                    _healer.heartbeat(self._email, "exit_engine")
            except Exception:
                pass

            time.sleep(POLL_INTERVAL_SEC)

    def _check_all_positions(self):
        # Run sentinels first — catch stuck/missed exits
        try:
            self._run_sentinels()
        except Exception as _se:
            log.debug("[exit_eng] Sentinel error (non-critical): %s", _se)
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
                        pos.current_bid          = bid
                        pos.current_ask          = ask
                        pos.current_option_price = (bid + ask) / 2

                # Gate: don't re-fire while an exit is in-flight
                now_utc = datetime.now(timezone.utc)
                if not self._eligible_for_new_exit(pos, now_utc):
                    continue

                # Evaluate exit
                if pos.current_underlying > 0 and pos.current_option_price > 0:
                    # Track peak P&L for profit lock
                    if pos.option_pnl_pct > pos.peak_pnl_pct:
                        pos.peak_pnl_pct = pos.option_pnl_pct
                    if pos.option_pnl_pct > 0:
                        pos.touched_profit   = True
                        if pos.option_pnl_pct > pos.max_profit_seen:
                            pos.max_profit_seen = pos.option_pnl_pct
                    decision = evaluate_exit(pos, now_et)
                    if decision.should_act:
                        actions_to_take.append((pos, decision))

        # ── Gap 2: Kill check post-fetch, pre-execute ───────────────────────────
        # Quote fetch takes 1-5s on live Tradier. Kill may have fired during
        # that I/O window. Filter out non-protective exits but allow EOD/stop-loss.
        if self._kill_switch_fn and self._kill_switch_fn():
            def _is_protective(reason: str) -> bool:
                """
                Determine if an exit is protective (must execute even under kill switch).
                Uses substring matching because reasons are descriptive strings like
                "EOD FORCE CLOSE -- 15:31 ET" or "STOP HIT -- underlying at $190".
                """
                r = (reason or "").upper()
                return any(k in r for k in (
                    "EOD", "STOP", "MAX_LOSS", "THETA", "PROTECTIVE",
                    "FORCE CLOSE", "STOP HIT", "STOP LOSS",
                ))
            protective = [(p, d) for p, d in actions_to_take if _is_protective(d.reason or "")]
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
            if pos.exit_in_flight:
                log.debug("[%s] Exit already in flight | %s | reason=%s",
                          pos.ticker, pos.option_symbol, pos.pending_exit_reason)
                continue

            if decision.action == "SCALE_OUT":
                if self.on_scale:
                    if self._kill_switch_fn and self._kill_switch_fn():
                        log.warning(f"[{pos.ticker}] KILL ACTIVE at on_scale -- skipping")
                        continue
                    try:
                        self.on_scale(pos, decision)
                        pos.exit_in_flight      = True
                        pos.pending_exit_reason = decision.reason
                        pos.pending_exit_qty    = decision.quantity
                        pos.last_exit_signal_ts = datetime.now(timezone.utc)
                        pos.scale_outs_done    += 1  # track so runner logic kicks in
                        pos.quantity_remaining  = max(0, pos.quantity_remaining - decision.quantity)
                        # Persist scale_outs_done to DB so restarts know runner is active
                        try:
                            from ap.db import conn, run_with_retry as _rwr_s
                            _sd = pos.scale_outs_done
                            _qr = pos.quantity_remaining
                            _pi = pos.position_id
                            if _pi:
                                def _save_scale():
                                    with conn() as _c:
                                        _c.execute(
                                            "UPDATE positions SET scale_outs_done=%s, qty=%s WHERE id=%s",
                                            (_sd, _qr, _pi)
                                        )
                                _rwr_s(_save_scale)
                        except Exception as _se:
                            log.debug("[%s] scale_outs_done persist failed (non-critical): %s",
                                      pos.ticker, _se)
                    except Exception as e:
                        log.error("[%s] Scale-out FAILED — remains tracked: %s", pos.ticker, e)
                continue
            else:
                if self.on_exit:
                    exit_reason = decision.reason or ""
                    if self._kill_switch_fn and self._kill_switch_fn():
                        if not _is_protective(exit_reason):
                            log.warning(
                                f"[{pos.ticker}] Kill switch active — blocking non-protective exit: {exit_reason}"
                            )
                            continue
                        else:
                            log.info(
                                f"[{pos.ticker}] Kill switch active but allowing protective exit: {exit_reason}"
                            )
                    try:
                        self.on_exit(pos, decision)
                        # Submission ≠ closure. Mark in-flight so we don't
                        # re-fire every 30s. Closure via mark_position_closed().
                        pos.exit_in_flight      = True
                        pos.pending_exit_reason = decision.reason
                        pos.pending_exit_qty    = decision.quantity
                        pos.last_exit_signal_ts = datetime.now(timezone.utc)
                    except Exception as e:
                        log.error(
                            "Exit order FAILED for %s — position remains tracked: %s",
                            pos.ticker, e,
                        )
                        continue
                else:
                    log.warning(
                        "[%s] Exit decision generated but no on_exit callback installed",
                        pos.ticker,
                    )
                    continue

                # 3. Log trade to edge intelligence (non-critical)
                try:
                    from ap_edge_intelligence import APTradeLogger
                    _tl = APTradeLogger()
                    _tl.log_trade(
                        position={
                            "ticker":           pos.ticker,
                            "direction":        pos.side,
                            "contract":         pos.option_symbol,
                            "underlying_entry": pos.underlying_entry,
                            "entry_price":      pos.entry_price,
                            "quantity":         pos.quantity_remaining,
                            "entry_ts":         getattr(pos, "entry_ts", None),
                            "signal_id":        getattr(pos, "signal_id", None),
                            "score":            getattr(pos, "score", None),
                            "tier":             getattr(pos, "tier", None),
                            "pattern":          getattr(pos, "pattern", None),
                            "timeframe":        getattr(pos, "timeframe", None),
                            "signal":           getattr(pos, "signal", {}),
                        },
                        exit_info={
                            "exit_price":          getattr(pos, "current_option_price", pos.entry_price),
                            "exit_reason":         decision.reason,
                            "underlying_exit":     getattr(pos, "current_underlying", None),
                        },
                        client_id=self._email or "default",
                    )
                except Exception as _tl_err:
                    log.debug("Trade logger (non-critical): %s", _tl_err)

    def _fetch_quotes(self, tickers: list[str]) -> dict:
        try:
            resp = self._quote_broker.session.get(
                f"{self._quote_broker.cfg.base_url}/v1/markets/quotes",
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
            resp = self._quote_broker.session.get(
                f"{self._quote_broker.cfg.base_url}/v1/markets/quotes",
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
