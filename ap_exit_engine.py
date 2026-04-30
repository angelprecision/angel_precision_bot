# ap_exit_engine.py -- Angel Precision Time-Aware Exit Engine
# =============================================================================
# 0DTE / short-dated option exit protection.
#
# Current constants in this file:
#   - Poll interval: 8 seconds
#   - Profit protect W1: 11:00 AM ET, scale out 50% if option P&L >= +40%
#   - Profit protect W2: 1:00 PM ET, scale out 75% if option P&L >= +25%
#   - Profit protect W3: 2:00 PM ET, close all if option P&L >= +15%
#   - EOD hard close: 3:45 PM ET, close all remaining contracts
#   - Theta stop: after noon, close if option P&L <= -35%
#   - Immediate TP: +18% option P&L; 1 contract closes all, multi-contract scales
#   - Hard stop: DTE/instrument-adjusted; default -30%, tighter on 0DTE index
#
# Money-safety invariant:
#   - Submitting an exit order is NOT a fill.
#   - scale_outs_done increments only after broker-confirmed exit fill.
#   - Every exit path, including sentinels, must respect exit_in_flight gating.
# =============================================================================

from __future__ import annotations

import os
import time
import threading
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Callable
from zoneinfo import ZoneInfo


try:
    from ap.observability import emit_decision_event, get_git_commit
except Exception:
    emit_decision_event = None

    def get_git_commit(default: str = "unknown") -> str:
        return default

log = logging.getLogger("ap.exit_engine")
ET  = ZoneInfo("America/New_York")

# ── TIME THRESHOLDS (ET) ──────────────────────────────────────────────────────
PROFIT_PROTECT_1_HOUR = 11   # 11:00 AM -- scale out 50% if +40%
PROFIT_PROTECT_1_MIN  = 0
PROFIT_PROTECT_2_HOUR = 13   # 1:00 PM  -- scale out 75% if +25%
PROFIT_PROTECT_2_MIN  = 0
PROFIT_PROTECT_3_HOUR = 14   # 2:00 PM  -- exit all if +15%
PROFIT_PROTECT_3_MIN  = 0
EOD_HARD_CLOSE_HOUR   = 15   # 3:45 PM  -- EXIT EVERYTHING (was 3:30, extended for runners)
EOD_HARD_CLOSE_MIN    = 45
POLL_INTERVAL_SEC     = 8    # check every 8 seconds — catch TP windows faster

# Kill switch policy: exits reduce risk, so the engine must never pause
# evaluation under kill switch. By default, all exit actions are allowed.
# Set EXIT_ENGINE_KILL_BLOCKS_NON_PROTECTIVE=1 only if you explicitly want
# kill switch to block non-protective exits while still allowing stops/EOD/theta.
KILL_BLOCKS_NON_PROTECTIVE_EXITS = (
    os.getenv("EXIT_ENGINE_KILL_BLOCKS_NON_PROTECTIVE", "0").strip().lower()
    in {"1", "true", "yes", "on"}
)

# ── P&L THRESHOLDS ────────────────────────────────────────────────────────────
THETA_STOP_LOSS_PCT   = -0.35  # -35% on option → stop (was -50%)
SCALE_OUT_1_THRESHOLD = 0.40   # +40%  → scale out 50% at window 1 (was +150%)
SCALE_OUT_2_THRESHOLD = 0.25   # +25%  → scale out 75% at window 2 (was +80%)
PROTECT_3_THRESHOLD   = 0.15   # +15%  → exit all at window 3 (was +30%)

# ── IMMEDIATE TAKE-PROFIT (any time, no window gate) ──────────────────────────
IMMEDIATE_TP_PCT      = 0.18   # +18% → scale out 70% immediately (was 0.25 — lock earlier, more runner time)
HARD_STOP_PCT         = -0.30  # -30% → exit immediately regardless of time
PROFIT_LOCK_PCT       = 0.12   # once at +25%, lock: don't fall below +12%

_INDEX_ETFS = {"QQQ", "SPY", "IWM", "DIA", "SPX"}


def _et_session_date():
    """Return the current market/session calendar date in America/New_York."""
    return datetime.now(ET).date()


def _option_expiration_date(option_symbol: str):
    """Parse OCC-style YYMMDD expiration from an option symbol. Returns date or None."""
    import re
    m = re.search(r"(\d{6})[CP]", option_symbol or "")
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%y%m%d").date()
    except Exception:
        return None


def _option_dte(option_symbol: str, *, session_date=None) -> int:
    """DTE anchored to ET session date, never host-local/UTC date."""
    exp = _option_expiration_date(option_symbol or "")
    if exp is None:
        return 999
    sd = session_date or _et_session_date()
    return (exp - sd).days


def _option_root(option_symbol: str) -> str:
    """Best-effort OCC/root extraction before YYMMDD date. Handles SPXW-style roots."""
    import re
    sym = (option_symbol or "").upper().strip()
    m = re.search(r"(\d{6})[CP]", sym)
    if not m:
        return sym[:8]
    return sym[:m.start()].strip()


def _option_profile(pos: "ManagedPosition") -> tuple[int, bool, str]:
    symbol = (pos.option_symbol or "").upper()
    ticker = (pos.ticker or "").upper()
    root = _option_root(symbol)
    dte = _option_dte(symbol)
    index_roots = _INDEX_ETFS | {"SPXW", "NDX", "NDXP", "RUT", "RUTW"}
    is_index = root in index_roots or ticker in index_roots or any(root.startswith(t) for t in _INDEX_ETFS)
    profile = "0DTE-idx" if (dte == 0 and is_index) else "0DTE-eq" if dte == 0 else f"{dte}DTE"
    return dte, is_index, profile


def _effective_thresholds(pos: "ManagedPosition") -> tuple:
    """
    Returns (hard_stop, immediate_tp, profit_lock) adjusted for ET-session DTE and instrument.
    """
    dte, is_index, _ = _option_profile(pos)
    if dte == 0 and is_index:
        return -0.18, 0.20, 0.08
    if dte == 0:
        return -0.22, 0.22, 0.10
    if dte <= 2:
        return -0.26, 0.25, 0.12
    return HARD_STOP_PCT, IMMEDIATE_TP_PCT, PROFIT_LOCK_PCT
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
    last_rejection_ts:    Optional[float] = None  # epoch when last exit was rejected
    last_exit_rejected:  bool = False
    _exit_stuck_count:   int = 0
    max_profit_seen:      float = 0.0   # highest positive P&L ever seen
    closed:               bool  = False
    close_reason:         str   = ""
    opened_at:            datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # Exit coordination — engine signals intent; order truth decides closure
    exit_in_flight:       bool  = False
    pending_exit_reason:  str   = ""
    pending_exit_qty:     int   = 0
    pending_exit_filled_qty: int = 0
    pending_scale_counted: bool = False
    # Exact broker/local exit order identity. These fields make broker fill
    # callbacks idempotent and prevent a stale event for one exit order from
    # mutating a different in-flight exit.
    pending_exit_local_order_id: str = ""
    pending_exit_broker_order_id: str = ""
    last_applied_exit_local_order_id: str = ""
    last_applied_exit_broker_order_id: str = ""
    last_applied_exit_cum_fill: int = 0
    last_exit_signal_ts:  Optional[datetime] = None

    # P&L tracking
    realized_pnl:   float = 0.0
    unrealized_pnl: float = 0.0

    def __post_init__(self):
        self.quantity = max(0, int(self.quantity or 0))
        self.quantity_remaining = self.quantity if int(self.quantity_remaining or 0) <= 0 else int(self.quantity_remaining)

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
    suggested_limit: float = 0.0  # live bid at decision time — use as sell limit price
                                  # 0.0 means caller should query fresh bid themselves

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
    # Use DTE-adjusted thresholds — tighter stops on 0DTE index options
    _hard_stop, _immediate_tp, _profit_lock = _effective_thresholds(pos)

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
    if option_pnl >= _immediate_tp and pos.scale_outs_done == 0:
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
        # Tiered trail — tighter as peak gets bigger, protect more of the gain
        # Peak >80%: 10pt trail (e.g. 86% peak → close at 76%)
        # Peak >60%: 13pt trail (e.g. 70% peak → close at 57%)
        # Peak >40%: 15pt trail
        # Below 40%: 20pt trail (more room at lower peaks)
        if pos.peak_pnl_pct >= 0.80:
            _runner_trail = 0.10
        elif pos.peak_pnl_pct >= 0.60:
            _runner_trail = 0.13
        elif pos.peak_pnl_pct >= 0.40:
            _runner_trail = 0.15
        else:
            _runner_trail = 0.20
        runner_drop   = pos.peak_pnl_pct - option_pnl
        if runner_drop >= _runner_trail or option_pnl <= 0:
            # 🏆 Discord alert when runner closes with meaningful gain — your sales machine
            if pos.peak_pnl_pct >= 0.50:
                try:
                    import os as _os, requests as _req, time as _time
                    _wh = _os.getenv("DISCORD_WEBHOOK_RUNNER", "") or _os.getenv("DISCORD_WEBHOOK_URL", "")
                    if _wh:
                        # opened_at is datetime — convert to timestamp before subtraction
                        _opened_ts = pos.opened_at.timestamp() if hasattr(pos.opened_at, "timestamp") else _time.time()
                        _dur = int((_time.time() - _opened_ts) / 60)
                        _req.post(_wh, json={"embeds": [{
                            "title":       f"🏆 RUNNER CLOSED · {pos.ticker}",
                            "description": (
                                f"**Peak: +{pos.peak_pnl_pct*100:.0f}%** → Exit: +{option_pnl*100:.0f}%\n"
                                f"Held {_dur}m | Trail: {_runner_trail*100:.0f}pts | Contracts: {qty_rem}"
                            ),
                            "color": 0xF1C40F,
                        }]}, timeout=3)
                except Exception:
                    pass  # never block exit on Discord failure
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
    # Was ever green → went negative → exit immediately.
    # After a confirmed scale-out, runner protection is handled by RUNNER TRAIL
    # and max_profit_seen logic. Do not let this global rule fight runners.
    if pos.scale_outs_done == 0 and pos.touched_profit and option_pnl <= -0.05:
        return ExitDecision(
            action="CLOSE_ALL", quantity=qty_rem,
            reason=(
                f"TOUCHED PROFIT STOP — was +{pos.max_profit_seen*100:.0f}% "
                f"now {option_pnl*100:.0f}% — protecting capital"
            ),
            urgency="IMMEDIATE", pnl_pct=option_pnl
        )

    # ── NEVER-GREEN ESCALATING STOP ─────────────────────────────────────────
    # If the trade NEVER touched profit, tighten the stop as time passes.
    # Logic: the longer it sits without going green, the less likely it works.
    # Preserves capital by getting out at -12% instead of -25%.
    #
    # DTE-aware — matches the same instrument classes as _effective_thresholds():
    #
    #   0DTE INDEX (SPY/QQQ/IWM): fastest confirmation required — moves happen NOW
    #     0-3 min: -12%   5-8 min: -10%   8-15 min: -8%   15+ min: -6%
    #
    #   0DTE EQUITY: slightly more breathing room — single names can lag index
    #     0-5 min: -15%   5-10 min: -12%   10-20 min: -10%   20+ min: -8%
    #
    #   1-2 DTE (any): more time to be right — theta slower, moves develop
    #     0-10 min: -18%   10-20 min: -15%   20-40 min: -12%   40+ min: -10%
    #
    #   DEFAULT (2DTE+): swing scalp profile — most breathing room
    #     0-5 min: -15%   5-10 min: -12%   10-20 min: -10%   20+ min: -8%
    if not pos.touched_profit:
        _age_min = (datetime.now(timezone.utc) - pos.opened_at).total_seconds() / 60                    if pos.opened_at else 0

        # Reuse ET-session DTE + instrument class from _effective_thresholds context
        _dte_ng, _is_idx_ng, _profile_ng = _option_profile(pos)

        if _dte_ng == 0 and _is_idx_ng:
            # 0DTE index — fastest leash: SPY/QQQ/IWM move hard and fast
            if _age_min < 3:    _ng_stop = -0.12
            elif _age_min < 8:  _ng_stop = -0.10
            elif _age_min < 15: _ng_stop = -0.08
            else:               _ng_stop = -0.06
        elif _dte_ng == 0:
            # 0DTE equity — slightly more room
            if _age_min < 5:    _ng_stop = -0.15
            elif _age_min < 10: _ng_stop = -0.12
            elif _age_min < 20: _ng_stop = -0.10
            else:               _ng_stop = -0.08
        elif _dte_ng <= 2:
            # 1-2 DTE — more time to develop, theta slower
            if _age_min < 10:   _ng_stop = -0.18
            elif _age_min < 20: _ng_stop = -0.15
            elif _age_min < 40: _ng_stop = -0.12
            else:               _ng_stop = -0.10
        else:
            # Default 2DTE+ swing profile
            if _age_min < 5:    _ng_stop = -0.15
            elif _age_min < 10: _ng_stop = -0.12
            elif _age_min < 20: _ng_stop = -0.10
            else:               _ng_stop = -0.08

        if option_pnl <= _ng_stop:
            _profile = ("0DTE-idx" if (_dte_ng==0 and _is_idx_ng)
                        else "0DTE-eq" if _dte_ng==0
                        else f"{_dte_ng}DTE")
            return ExitDecision(
                action="CLOSE_ALL", quantity=qty_rem,
                reason=(
                    f"NEVER GREEN STOP [{_profile}] — {option_pnl*100:.0f}% "
                    f"at {_age_min:.0f}min | threshold={_ng_stop*100:.0f}% | "
                    f"thesis never confirmed"
                ),
                urgency="IMMEDIATE", pnl_pct=option_pnl,
            )

    # ── HARD STOP (fires any time, no time gate) ─────────────────────────────
    if option_pnl <= _hard_stop:
        return ExitDecision(
            action="STOP", quantity=qty_rem,
            reason=f"HARD STOP -- {option_pnl*100:.0f}% exceeded -{abs(_hard_stop)*100:.0f}% max loss",
            urgency="IMMEDIATE", pnl_pct=option_pnl
        )

    # ── PROFIT LOCK (once we hit peak, don't give it all back) ──────────────────
    if pos.peak_pnl_pct >= _immediate_tp:
        # Hard floor: don't fall below PROFIT_LOCK_PCT
        if option_pnl <= _profit_lock:
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
    while w2_min >= 60:
        w2_hour += 1
        w2_min -= 60
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
    while w1_min >= 60:
        w1_hour += 1
        w1_min -= 60
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

def _is_protective_exit(reason: str) -> bool:
    """True if exit reason is protective — module-level so always in scope."""
    r = (reason or "").upper()
    return any(k in r for k in (
        "EOD", "STOP", "MAX_LOSS", "THETA", "PROTECTIVE", "FORCE CLOSE", "SENTINEL",
        "TARGET HIT", "IMMEDIATE TP", "PROFIT PROTECT", "SMALL WIN", "RUNNER TRAIL",
        "PROFIT LOCK", "TOUCHED PROFIT", "NEVER GREEN", "DEAD TRADE"
    ))



def _is_runner_protective_reason(reason: str) -> bool:
    """Narrow runner-protective classification for emergency override decisions."""
    r = (reason or "").upper()
    return (
        "RUNNER TRAIL" in r
        or "RUNNER EMERGENCY" in r
        or "SENTINEL MISSED TP" in r
        or "MISSED TP" in r
    )


def _is_same_or_equivalent_runner_protection(pending_reason: str, new_reason: str) -> bool:
    """True when both pending and new exits are already runner/profit-protection closes."""
    return _is_runner_protective_reason(pending_reason) and _is_runner_protective_reason(new_reason)


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
        self._lock            = threading.RLock()  # reentrant: helpers/submission paths can nest lock acquisition
        self._running         = False
        self._thread: Optional[threading.Thread] = None
        self.on_exit: Optional[Callable]  = None   # callback(pos, ExitDecision)
        self.on_scale: Optional[Callable] = None   # callback(pos, ExitDecision, qty)
        self._kill_switch_fn  = kill_switch_fn     # callable() → bool | None

        # Observability metadata. Never let analytics break live exits.
        self.run_id = os.getenv("AP_RUN_ID", "unknown")
        self.strategy_version = os.getenv("AP_STRATEGY_VERSION", "ap_live_beta")
        self.git_commit = get_git_commit()
    def start(self):
        """Start exit engine background loop."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._exit_loop,
            name=f"APExitEngine-{self._email or 'default'}",
            daemon=True,
        )
        self._thread.start()
        log.info("[%s] APExitEngine started", self._email or "default")

    def stop(self):
        """Stop exit engine background loop."""
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        log.info("[%s] APExitEngine stopped", self._email or "default")
    def _emit_exit_event(
        self,
        pos: ManagedPosition,
        decision: str,
        reason_code: Optional[str],
        explanation: str,
        stage: str = "exit_decision",
        extra_inputs: Optional[dict] = None,
        extra_context: Optional[dict] = None,
    ) -> None:
        """Emit structured exit telemetry without risking the exit loop."""
        if emit_decision_event is None:
            return
        try:
            emit_decision_event(
                run_id=self.run_id,
                candidate_id=getattr(pos, "signal_id", None) or getattr(pos, "position_id", None),
                trade_id=getattr(pos, "position_id", None),
                position_id=getattr(pos, "position_id", None),
                client_id=getattr(pos, "client_id", None) or self._email or "default",
                stage=stage,
                decision=decision,
                reason_code=reason_code,
                explanation=explanation,
                symbol=getattr(pos, "ticker", None),
                contract=getattr(pos, "option_symbol", None),
                strategy_version=self.strategy_version,
                git_commit=self.git_commit,
                inputs={
                    "option_pnl_pct": getattr(pos, "option_pnl_pct", 0.0),
                    "peak_pnl_pct": getattr(pos, "peak_pnl_pct", 0.0),
                    "max_profit_seen": getattr(pos, "max_profit_seen", 0.0),
                    "touched_profit": getattr(pos, "touched_profit", False),
                    "qty_remaining": getattr(pos, "quantity_remaining", 0),
                    "quantity_remaining": getattr(pos, "quantity_remaining", 0),
                    "quantity": getattr(pos, "quantity", 0),
                    "scale_outs_done": getattr(pos, "scale_outs_done", 0),
                    "exit_in_flight": getattr(pos, "exit_in_flight", False),
                    "pending_exit_qty": getattr(pos, "pending_exit_qty", 0),
                    "pending_exit_filled_qty": getattr(pos, "pending_exit_filled_qty", 0),
                    "pending_exit_local_order_id": getattr(pos, "pending_exit_local_order_id", ""),
                    "pending_exit_broker_order_id": getattr(pos, "pending_exit_broker_order_id", ""),
                    **(extra_inputs or {}),
                },
                context=extra_context or {},
            )
        except Exception as e:
            log.debug("Exit observability emit failed (non-critical): %s", e)

    def _exit_reason_code(self, decision: ExitDecision) -> Optional[str]:
        r = (getattr(decision, "reason", "") or "").upper()
        action = (getattr(decision, "action", "") or "").upper()
        if "SENTINEL" in r:
            return "SENTINEL_FORCED_EXIT"
        if "EOD FORCE CLOSE" in r:
            return "EOD_FORCE_CLOSE"
        if "THETA STOP" in r:
            return "THETA_STOP"
        if "HARD STOP" in r or "STOP HIT" in r:
            return "HARD_STOP"
        if "RUNNER TRAIL" in r:
            return "RUNNER_TRAIL"
        if "SMALL WIN LOCK" in r:
            return "SMALL_WIN_LOCK"
        if "TOUCHED PROFIT STOP" in r:
            return "TOUCHED_PROFIT_STOP"
        if "UNDERLYING PROGRESS EXIT" in r:
            return "UNDERLYING_PROGRESS_EXIT"
        if "NEVER GREEN STOP" in r:
            return "NEVER_GREEN_STOP"
        if "PROFIT LOCK" in r:
            return "PROFIT_LOCK"
        if "TRAILING STOP" in r:
            return "TRAILING_STOP"
        if "IMMEDIATE TP" in r or action == "SCALE_OUT":
            return "TP_SCALE_OUT" if action == "SCALE_OUT" else "IMMEDIATE_TP"
        if "TARGET HIT" in r:
            return "TARGET_HIT"
        if "TIME STOP" in r:
            return "TIME_STOP"
        return None

    def _assert_position_invariants(self, pos: ManagedPosition, context: str = "") -> None:
        if pos.quantity_remaining < 0 or pos.quantity_remaining > pos.quantity:
            raise RuntimeError(f"position invariant failed {context}: quantity_remaining={pos.quantity_remaining} quantity={pos.quantity} pos_id={pos.position_id}")
        if pos.pending_exit_filled_qty < 0:
            raise RuntimeError(f"position invariant failed {context}: pending_exit_filled_qty={pos.pending_exit_filled_qty} pos_id={pos.position_id}")
        if pos.pending_exit_qty > 0 and pos.pending_exit_filled_qty > pos.pending_exit_qty:
            raise RuntimeError(f"position invariant failed {context}: pending_exit_filled_qty={pos.pending_exit_filled_qty} > pending_exit_qty={pos.pending_exit_qty} pos_id={pos.position_id}")

    def _can_submit_exit(
        self,
        pos: ManagedPosition,
        now_utc: datetime,
        *,
        reason: str = "",
        allow_inflight_override: bool = False,
    ) -> bool:
        """Centralized gate for every path that can submit an exit order.

        Normal rule:
            exit_in_flight=True blocks new exits.

        Emergency override rule:
            allow_inflight_override=True may bypass in-flight only when:
            - existing in-flight order is stale for >= 20 seconds, OR
            - the new reason is runner-protective/emergency AND the pending
              reason is not already an equivalent runner-protective close.

        This prevents duplicate normal exits while allowing true runner rescue.
        """
        if pos.closed or int(pos.quantity_remaining or 0) <= 0:
            return False

        if pos.exit_in_flight and not allow_inflight_override:
            self._emit_exit_event(
                pos,
                "HOLD",
                "EXIT_SIGNAL_BLOCKED_IN_FLIGHT",
                f"Exit suppressed because exit already in flight: {reason or pos.pending_exit_reason}",
                extra_inputs={
                    "pending_exit_reason": pos.pending_exit_reason,
                    "pending_exit_qty": pos.pending_exit_qty,
                    "pending_exit_filled_qty": pos.pending_exit_filled_qty,
                    "last_exit_signal_ts": str(pos.last_exit_signal_ts or ""),
                },
            )
            return False

        if pos.exit_in_flight and allow_inflight_override:
            flight_sec = 0.0
            if pos.last_exit_signal_ts:
                flight_sec = (now_utc - pos.last_exit_signal_ts).total_seconds()

            stale_enough = flight_sec >= 20.0
            emergency_reason = _is_runner_protective_reason(reason)
            already_same_runner_protection = _is_same_or_equivalent_runner_protection(
                pos.pending_exit_reason,
                reason,
            )

            if already_same_runner_protection and not stale_enough:
                self._emit_exit_event(
                    pos,
                    "HOLD",
                    "EXIT_OVERRIDE_DENIED_EQUIVALENT_PENDING",
                    (
                        "Inflight override denied; pending exit is already an equivalent "
                        f"runner-protective close and is not stale ({flight_sec:.1f}s)"
                    ),
                    extra_inputs={
                        "flight_sec": flight_sec,
                        "pending_exit_reason": pos.pending_exit_reason,
                        "override_reason": reason,
                    },
                )
                return False

            if not stale_enough and not emergency_reason:
                self._emit_exit_event(
                    pos,
                    "HOLD",
                    "EXIT_OVERRIDE_DENIED",
                    f"Inflight override denied; existing exit not stale enough: {flight_sec:.1f}s",
                    extra_inputs={
                        "flight_sec": flight_sec,
                        "pending_exit_reason": pos.pending_exit_reason,
                        "override_reason": reason,
                    },
                )
                return False

            self._emit_exit_event(
                pos,
                "ALERT",
                "EXIT_INFLIGHT_OVERRIDE_ALLOWED",
                f"Inflight override allowed for emergency exit: {reason}",
                extra_inputs={
                    "flight_sec": flight_sec,
                    "pending_exit_reason": pos.pending_exit_reason,
                    "pending_exit_qty": pos.pending_exit_qty,
                    "pending_exit_filled_qty": pos.pending_exit_filled_qty,
                    "stale_enough": stale_enough,
                    "emergency_reason": emergency_reason,
                },
            )
            log.warning(
                "[%s] EXIT IN-FLIGHT OVERRIDE allowed | flight=%.1fs new=%s pending=%s",
                pos.ticker,
                flight_sec,
                reason,
                pos.pending_exit_reason,
            )

        if pos.last_rejection_ts is not None:
            elapsed = time.time() - pos.last_rejection_ts
            if elapsed < 30:
                self._emit_exit_event(
                    pos,
                    "HOLD",
                    "EXIT_REJECTION_COOLDOWN",
                    f"Exit suppressed during rejection cooldown: {elapsed:.0f}s",
                    extra_inputs={"cooldown_elapsed_sec": elapsed},
                )
                return False
            pos.last_rejection_ts = None
            pos.last_exit_rejected = False

        return True

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
                    # Bug fix: use quantity_remaining if stored, else fall back to original qty
                    # quantity_remaining reflects partial closes/scale-outs; qty is the original fill
                    _qty_remaining = int(row.get("quantity_remaining", 0) or 0)
                    _db_qty        = int(row.get("qty", 0) or 0)
                    _resolved_qty  = _qty_remaining if _qty_remaining > 0 else _db_qty
                    if _resolved_qty > 0:
                        mp.quantity          = _resolved_qty   # update base qty to remaining
                        mp.quantity_remaining = _resolved_qty
                        log.debug(
                            "seed_from_db: %s qty_remaining=%d (db_qty=%d scale_outs=%d)",
                            mp.ticker, _resolved_qty, _db_qty, mp.scale_outs_done
                        )
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
        # Detect contracts that expired before the current ET session date.
        # Do not silently discard without observability: emit a terminal cleanup
        # event so reconciler/dashboard gaps are visible.
        today_et = _et_session_date()
        to_remove = []
        for pos in self._positions:
            sym = getattr(pos, "option_symbol", "") or ""
            try:
                exp = _option_expiration_date(sym)
                if exp and exp < today_et:
                    log.warning(
                        "[exit_eng] EXPIRED CONTRACT detected | %s exp=%s today_et=%s — local engine cleanup",
                        sym, exp.isoformat(), today_et.isoformat()
                    )
                    self._emit_exit_event(
                        pos,
                        decision="ALERT",
                        reason_code="EXPIRED_CONTRACT_LOCAL_CLEANUP",
                        explanation=f"Expired contract removed from exit engine tracking: {sym}",
                        stage="system_alert",
                        extra_inputs={"expiration": exp.isoformat(), "session_date_et": today_et.isoformat()},
                    )
                    pos.closed = True
                    pos.close_reason = "expired_contract_local_cleanup"
                    to_remove.append(pos)
            except Exception as _exp_err:
                log.debug("[exit_eng] Expired-contract cleanup check failed for %s: %s", sym, _exp_err)
        if to_remove:
            self._positions = [p for p in self._positions if not p.closed]
            log.info("[exit_eng] Removed %d expired contract(s) from engine", len(to_remove))

        # Run sentinels first — catch stuck/missed exits
        try:
            self._run_sentinels()
        except Exception as _se:
            log.debug("[exit_eng] Sentinel error (non-critical): %s", _se)
        # ── Kill switch snapshot (DO NOT RETURN) ────────────────────────────
        # Kill switch means "do not add new risk". It must never stop the
        # exit engine from evaluating/placing risk-reducing exits.
        kill_active = False
        if self._kill_switch_fn:
            try:
                kill_active = bool(self._kill_switch_fn())
            except Exception as _ks_err:
                # Exit protection is more important than a failed kill-switch read.
                # Do not block exits because the kill-switch function errored.
                log.warning("Exit engine kill-switch check failed; continuing exit evaluation: %s", _ks_err)
                kill_active = False
        if kill_active:
            log.warning(
                "Exit engine kill switch active — continuing exit evaluation; "
                "risk-reducing exits remain enabled"
            )
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
                option_pnl = pos.option_pnl_pct
                # Runner trail bypass: if scale-out done and runner is giving
                # back massive gains, close regardless of exit_in_flight.
                # This prevents a pending scale-out fill from blocking the
                # runner trail while price collapses (NFLX 86%→35% case).
                _force_runner_check = False
                if pos.scale_outs_done >= 1 and pos.peak_pnl_pct >= 0.40:
                    _runner_drop_now = pos.peak_pnl_pct - option_pnl
                    _emergency_trail = 0.15  # tighter emergency trail
                    if _runner_drop_now >= _emergency_trail:
                        _force_runner_check = True
                        log.warning(
                            "[%s] RUNNER EMERGENCY — peak=%.0f%% now=%.0f%% "
                            "drop=%.0f%% > %.0f%% — emergency runner check",
                            pos.ticker, pos.peak_pnl_pct*100, option_pnl*100,
                            _runner_drop_now*100, _emergency_trail*100,
                        )

                if _force_runner_check:
                    log.warning(
                        "[%s] RUNNER EMERGENCY active — bypassing normal eligibility gate | %s | in_flight=%s reason=%s",
                        pos.ticker,
                        pos.option_symbol,
                        pos.exit_in_flight,
                        pos.pending_exit_reason,
                    )
                elif not self._eligible_for_new_exit(pos, now_utc):
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
                        self._emit_exit_event(
                            pos,
                            decision="SUBMIT",
                            reason_code=self._exit_reason_code(decision),
                            explanation=decision.reason,
                            stage="exit_decision",
                            extra_inputs={
                                "decision_action": decision.action,
                                "decision_qty": decision.quantity,
                                "decision_pnl_pct": decision.pnl_pct,
                                "suggested_limit": decision.suggested_limit,
                            },
                        )
                        actions_to_take.append((pos, decision, bool(_force_runner_check)))

        # ── Kill check post-fetch, pre-execute ───────────────────────────────
        # Default policy: kill switch DOES NOT block exits. Exits reduce risk.
        # Optional strict mode can block non-protective exits, but still allows
        # hard stops, theta stops, EOD, sentinel/time stops, and other protective exits.
        if kill_active and KILL_BLOCKS_NON_PROTECTIVE_EXITS:
            protective = [(p, d) for p, d in actions_to_take if _is_protective_exit(d.reason or "")]
            blocked_actions = [(p, d) for p, d in actions_to_take if not _is_protective_exit(d.reason or "")]
            blocked = len(blocked_actions)
            if blocked:
                log.warning(
                    "Exit engine: kill switch active strict mode -- blocking %d non-protective exit(s), "
                    "allowing %d protective exit(s)",
                    blocked,
                    len(protective),
                )
                for _bp, _bd in blocked_actions:
                    self._emit_exit_event(
                        _bp,
                        decision="REJECT",
                        reason_code="KILL_SWITCH_ACTIVE",
                        explanation=f"Kill switch blocked non-protective exit: {_bd.reason}",
                        stage="exit_decision",
                        extra_inputs={
                            "decision_action": _bd.action,
                            "decision_qty": _bd.quantity,
                            "decision_pnl_pct": _bd.pnl_pct,
                        },
                    )
            actions_to_take = protective
            if not actions_to_take:
                return
        elif kill_active and actions_to_take:
            log.warning(
                "Exit engine: kill switch active but allowing %d risk-reducing exit action(s)",
                len(actions_to_take),
            )

        # Execute actions through one centralized, gated submit path.
        # This preserves the full strategy logic above while avoiding direct
        # on_exit/on_scale calls and avoiding callback execution under lock.
        for _action_item in actions_to_take:
            if len(_action_item) == 3:
                pos, decision, _force_runner_submit = _action_item
            else:
                pos, decision = _action_item
                _force_runner_submit = False
            self._submit_exit_decision(
                pos,
                decision,
                from_sentinel=False,
                kill_active=kill_active,
                allow_inflight_override=bool(_force_runner_submit),
            )

    def _submit_exit_decision(
        self,
        pos: ManagedPosition,
        decision: ExitDecision,
        *,
        from_sentinel: bool = False,
        kill_active: bool = False,
        allow_inflight_override: bool = False,
    ) -> bool:
        """Single submit path for normal, scale, sentinel, and emergency exits.

        Locking rule:
        - Hold engine lock only for eligibility checks and internal state mutation.
        - Never call on_exit/on_scale while holding self._lock. Those callbacks are
          external code and may touch OSM/broker/reconciler paths.
        """
        now_utc = datetime.now(timezone.utc)

        # 1) Short critical section: validate and capture submit snapshot.
        with self._lock:
            if not self._can_submit_exit(
                pos,
                now_utc,
                reason=decision.reason,
                allow_inflight_override=allow_inflight_override,
            ):
                return False

            if kill_active and KILL_BLOCKS_NON_PROTECTIVE_EXITS and not _is_protective_exit(decision.reason or ""):
                self._emit_exit_event(
                    pos,
                    decision="REJECT",
                    reason_code="KILL_SWITCH_ACTIVE",
                    explanation=f"Kill switch blocked non-protective exit: {decision.reason}",
                    stage="exit_decision",
                )
                return False

            if decision.suggested_limit == 0.0 and pos.current_bid > 0:
                decision.suggested_limit = round(pos.current_bid * 0.99, 2)

            position_id = str(pos.position_id or "")
            option_symbol = str(pos.option_symbol or "")
            ticker = str(pos.ticker or "")
            pre_submit_qty = int(pos.quantity_remaining or 0)

            log.info(
                "[EXIT] client=%s ticker=%s sym=%s side=%s action=%s pnl=%.1f%% peak=%.1f%% qty_rem=%d qty_close=%d reason=%s",
                pos.client_id or "?",
                pos.ticker,
                pos.option_symbol,
                pos.side,
                decision.action,
                decision.pnl_pct * 100.0,
                pos.peak_pnl_pct * 100.0,
                pos.quantity_remaining,
                decision.quantity,
                decision.reason,
            )

        # 2) External callback outside lock.
        try:
            if decision.action == "SCALE_OUT":
                if not self.on_scale:
                    log.warning("[%s] Scale-out decision generated but no on_scale callback installed", ticker)
                    return False
                self.on_scale(pos, decision)
            else:
                if not self.on_exit:
                    log.warning("[%s] Exit decision generated but no on_exit callback installed", ticker)
                    return False
                self.on_exit(pos, decision)
        except Exception as exc:
            log.error("[%s] Exit submit failed; position remains tracked: %s", ticker, exc)
            self._emit_exit_event(
                pos,
                decision="ERROR",
                reason_code="EXIT_SUBMIT_FAILED",
                explanation=str(exc),
                stage="exit_submission",
                extra_inputs={"decision_action": decision.action, "decision_qty": decision.quantity},
            )
            return False

        # 3) Short critical section: revalidate and mark submitted.
        with self._lock:
            current_pos = None
            for tracked in self._positions:
                if position_id and str(tracked.position_id or "") == position_id and not tracked.closed:
                    current_pos = tracked
                    break
                if not position_id and tracked is pos and not tracked.closed:
                    current_pos = tracked
                    break

            if current_pos is None:
                log.warning(
                    "[%s] Exit callback returned but position is no longer tracked | pos_id=%s sym=%s",
                    ticker,
                    position_id or "?",
                    option_symbol,
                )
                return False

            # Do not overwrite a broker/OSM state change that arrived synchronously
            # during callback execution.
            if current_pos.closed or int(current_pos.quantity_remaining or 0) <= 0:
                log.info("[%s] Exit callback returned after position already closed | pos_id=%s", ticker, position_id or "?")
                return True

            if current_pos.exit_in_flight:
                self._emit_exit_event(
                    current_pos,
                    decision="HOLD",
                    reason_code="EXIT_SUBMIT_MARK_SKIPPED_ALREADY_IN_FLIGHT",
                    explanation="Exit callback returned but position was already marked in-flight by downstream path",
                    stage="exit_submission",
                    extra_inputs={
                        "decision_action": decision.action,
                        "decision_qty": decision.quantity,
                        "pre_submit_qty": pre_submit_qty,
                    },
                )
                return True

            self._mark_exit_submitted(current_pos, decision)
            self._assert_position_invariants(current_pos, "submit_exit_decision")

        self._emit_exit_event(
            pos,
            decision="SUBMITTED",
            reason_code=self._exit_reason_code(decision),
            explanation=decision.reason,
            stage="exit_submission",
            extra_inputs={
                "decision_action": decision.action,
                "decision_qty": decision.quantity,
                "from_sentinel": from_sentinel,
                "pre_submit_qty": pre_submit_qty,
            },
        )
        return True

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
            
