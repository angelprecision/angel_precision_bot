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
#   - v9: missing callback identity is a visible safe-lock, not a silent deadlock.
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
    pending_exit_action:  str   = ""  # SCALE_OUT / CLOSE_ALL / STOP; used for fill-safe reconciliation
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
    # Backward-compatible aggregate watermark; per-order map below is authoritative.
    last_applied_exit_cum_fill: int = 0
    last_applied_exit_cum_fill_by_order: dict[str, int] = field(default_factory=dict)
    last_exit_signal_ts:  Optional[datetime] = None
    last_callback_identity_missing: bool = False
    last_callback_identity_missing_ts: Optional[datetime] = None
    # v7 order-safety quarantine/replacement controls. Missing identity does
    # NOT auto-release because a broker order may still be live even when the
    # callback failed to return IDs. Replacement requires explicit external
    # cancel/reconcile proof via mark_exit_replacement_safe().
    exit_identity_quarantine: bool = False
    pending_exit_replace_allowed: bool = False
    pending_exit_replace_reason: str = ""
    pending_exit_replace_allowed_ts: Optional[datetime] = None
    # Quarantine escalation telemetry. Quarantine itself blocks duplicate exits;
    # these fields make safe-lock visible until reconciler/OSM resolves it.
    exit_identity_quarantine_alert_count: int = 0
    last_exit_identity_quarantine_alert_ts: Optional[datetime] = None
    last_exit_clear_reason: str = ""
    last_exit_clear_local_order_id: str = ""
    last_exit_clear_broker_order_id: str = ""
    last_exit_identity_quarantine_resolved_ts: Optional[datetime] = None
    last_exit_identity_reject_ts: Optional[datetime] = None

    # Quote-health fields are explicit dataclass state so dashboard/watchdog code
    # does not depend on dynamic attributes being added during polling.
    # Aggregate fields stay for backward compatibility; leg-specific fields make
    # it visible when the underlying is fresh but the option quote is stale.
    last_quote_update_ts: Optional[datetime] = None
    last_quote_missing_ts: Optional[datetime] = None
    last_underlying_quote_update_ts: Optional[datetime] = None
    last_underlying_quote_missing_ts: Optional[datetime] = None
    last_option_quote_update_ts: Optional[datetime] = None
    last_option_quote_missing_ts: Optional[datetime] = None

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
    reason_code:    str    = ""   # machine-readable control code; text reason is human-only

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

EXIT_RULE_PRECEDENCE = (
    # Lower number = higher priority. This is now executable, not decorative.
    "STOP_HIT",
    "HARD_STOP",
    "EOD_FORCE_CLOSE",
    "SENTINEL_FORCED_EXIT",
    "NEVER_GREEN_STOP",
    "TOUCHED_PROFIT_STOP",
    "RUNNER_TRAIL",
    "TARGET_HIT",
    "IMMEDIATE_TP",
    "PROFIT_LOCK",
    "TRAILING_STOP",
    "SMALL_WIN_LOCK",
    "UNDERLYING_PROGRESS_EXIT",
    "PROFIT_PROTECT_W3",
    "PROFIT_PROTECT_W2",
    "PROFIT_PROTECT_W1",
    "THETA_STOP",
    "TIME_STOP",
    "TP_SCALE_OUT",
)

EXIT_RULE_PRIORITY = {code: idx for idx, code in enumerate(EXIT_RULE_PRECEDENCE)}
LOWEST_EXIT_PRIORITY = len(EXIT_RULE_PRIORITY) + 100

# Quote and identity safety contract. Normal exits require fresh option marks.
# Only broker-independent/forced-risk exits may proceed in degraded quote mode.
STALE_OPTION_QUOTE_MAX_AGE_SEC = int(os.getenv("EXIT_ENGINE_STALE_OPTION_QUOTE_SEC", "20"))
FORCED_RISK_EXIT_CODES = {
    "EOD_FORCE_CLOSE",
    "STOP_HIT",
    "SENTINEL_FORCED_EXIT",
    "HARD_STOP",
    "NEVER_GREEN_STOP",
    "THETA_STOP",
    "TIME_STOP",
}


def _classify_exit_decision(decision: "ExitDecision") -> str:
    """Stable reason-code classifier used for precedence, telemetry, and gating.

    Preferred source is ExitDecision.reason_code. The string parser remains only
    as a backward-compatible fallback for older callers/tests.
    """
    explicit_code = (getattr(decision, "reason_code", "") or "").upper().strip()
    if explicit_code:
        return explicit_code
    r = (getattr(decision, "reason", "") or "").upper()
    action = (getattr(decision, "action", "") or "").upper()
    if "SENTINEL" in r:
        return "SENTINEL_FORCED_EXIT"
    if "EOD FORCE CLOSE" in r:
        return "EOD_FORCE_CLOSE"
    if "THETA STOP" in r:
        return "THETA_STOP"
    if "STOP HIT" in r:
        return "STOP_HIT"
    if "HARD STOP" in r:
        return "HARD_STOP"
    if "RUNNER TRAIL" in r or "RUNNER EMERGENCY" in r:
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
    if "IMMEDIATE TP" in r:
        return "IMMEDIATE_TP"
    if "TARGET HIT" in r:
        return "TARGET_HIT"
    if "PROFIT PROTECT W3" in r:
        return "PROFIT_PROTECT_W3"
    if "PROFIT PROTECT W2" in r:
        return "PROFIT_PROTECT_W2"
    if "PROFIT PROTECT W1" in r:
        return "PROFIT_PROTECT_W1"
    if "TIME STOP" in r or "DEAD TRADE" in r:
        return "TIME_STOP"
    if action == "SCALE_OUT":
        return "TP_SCALE_OUT"
    return "UNKNOWN_EXIT"


def _exit_priority(decision_or_code) -> int:
    code = decision_or_code if isinstance(decision_or_code, str) else _classify_exit_decision(decision_or_code)
    return EXIT_RULE_PRIORITY.get(code, LOWEST_EXIT_PRIORITY)


def _is_option_quote_stale(pos: "ManagedPosition", now_utc: Optional[datetime] = None) -> tuple[bool, Optional[float], str]:
    """Return (is_stale, age_sec, reason) for option quote freshness.

    Option marks drive option P&L and sell limits; normal exits must not submit
    from stale option quotes. Underlying/EOD/sentinel forced-risk exits are
    handled separately by the submit gate.
    """
    now_utc = now_utc or datetime.now(timezone.utc)
    ts = getattr(pos, "last_option_quote_update_ts", None)
    if ts is None:
        return True, None, "missing_option_quote"
    try:
        age = max(0.0, (now_utc - ts).total_seconds())
    except Exception:
        return True, None, "invalid_option_quote_ts"
    if age > STALE_OPTION_QUOTE_MAX_AGE_SEC:
        return True, age, "stale_option_quote"
    return False, age, "fresh_option_quote"


def _is_forced_risk_exit_code(code: str) -> bool:
    return (code or "").upper() in FORCED_RISK_EXIT_CODES


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
    def add_position(self, pos: ManagedPosition):
        """Track a newly broker-confirmed open position for exit protection."""
        if pos is None:
            return
        with self._lock:
            for existing in self._positions:
                same_id = bool(pos.position_id and existing.position_id == pos.position_id)
                same_sym = (
                    existing.ticker == pos.ticker
                    and existing.option_symbol == pos.option_symbol
                    and not existing.closed
                )
                if same_id or same_sym:
                    log.debug(
                        "[%s] Exit engine already tracking %s | pos_id=%s",
                        self._email or pos.ticker,
                        pos.option_symbol,
                        pos.position_id or "n/a",
                    )
                    return
            self._assert_position_invariants(pos, "add_position")
            self._positions.append(pos)
        log.info(
            "[%s] Position added to exit engine | %s %sx %s @ $%.2f | target=%s stop=%s | pos_id=%s",
            pos.ticker,
            pos.side,
            pos.quantity,
            pos.option_symbol,
            pos.entry_price,
            pos.underlying_target,
            pos.underlying_stop,
            pos.position_id or "n/a",
        )

    def start(self):
        """Start exit engine background loop."""
        if self._thread and self._thread.is_alive():
            log.debug("APExitEngine already running [%s]", self._email or "default")
            self._running = True
            return

        self._running = True
        # Keep compatibility with self-healing / supervisor component registry.
        thread_name = f"ap-exit-engine-{self._email}" if self._email else "ap-exit-engine"
        self._thread = threading.Thread(
            target=self._exit_loop,
            name=thread_name,
            daemon=True,
        )
        self._thread.start()
        log.info("APExitEngine started [%s]", thread_name)

    def stop(self):
        """Stop exit engine background loop and reset thread state for clean restart."""
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        self._thread = None
        log.info("[%s] APExitEngine stopped", self._email or "default")

    def active_positions(self) -> list[ManagedPosition]:
        """Return a snapshot of currently tracked, non-closed positions."""
        with self._lock:
            return [p for p in self._positions if not p.closed and int(p.quantity_remaining or 0) > 0]

    def set_pending_exit_order(
        self,
        position_id: str,
        *,
        local_order_id: str = "",
        broker_order_id: str = "",
        qty: int = 0,
        reason: str = "",
        **kwargs,
    ) -> None:
        """OSM v3-compatible hook for pending/working exit order state.

        OSM may call this after it has created/submitted a broker-side exit
        order. Keep the live ManagedPosition aligned with OSM identity without
        forcing callers to match an older/narrower APExitEngine signature.
        """
        if not position_id:
            return
        try:
            qty_i = max(0, int(qty or 0))
        except Exception:
            qty_i = 0
        local_order_id = str(local_order_id or "")
        broker_order_id = str(broker_order_id or "")
        reason = str(reason or "")

        with self._lock:
            for pos in self._positions:
                if str(pos.position_id or "") != str(position_id or ""):
                    continue
                if pos.closed or int(pos.quantity_remaining or 0) <= 0:
                    return

                pos.exit_in_flight = True
                pos.pending_exit_reason = reason or pos.pending_exit_reason or "osm_pending_exit_order"
                if qty_i > 0:
                    pos.pending_exit_qty = qty_i
                elif int(pos.pending_exit_qty or 0) <= 0:
                    pos.pending_exit_qty = int(pos.quantity_remaining or 0)
                pos.pending_exit_local_order_id = local_order_id or pos.pending_exit_local_order_id or ""
                pos.pending_exit_broker_order_id = broker_order_id or pos.pending_exit_broker_order_id or ""
                pos.last_exit_signal_ts = datetime.now(timezone.utc)
                pos.last_exit_rejected = False
                pos._exit_stuck_count = 0

                if local_order_id or broker_order_id:
                    pos.last_callback_identity_missing = False
                    pos.last_callback_identity_missing_ts = None
                    pos.exit_identity_quarantine = False
                    pos.exit_identity_quarantine_alert_count = 0
                    pos.last_exit_identity_quarantine_alert_ts = None

                self._assert_position_invariants(pos, "set_pending_exit_order")
                self._emit_exit_event(
                    pos,
                    decision="SUBMITTED",
                    reason_code="OSM_PENDING_EXIT_ORDER_SET",
                    explanation="OSM reported pending exit order identity to exit engine.",
                    stage="exit_reconciliation",
                    extra_inputs={
                        "local_order_id": local_order_id,
                        "broker_order_id": broker_order_id,
                        "qty": qty_i,
                        "reason": reason,
                    },
                )
                log.info(
                    "[exit_eng] OSM pending exit set | pos_id=%s local=%s broker=%s qty=%s reason=%s",
                    position_id,
                    local_order_id or "?",
                    broker_order_id or "?",
                    qty_i,
                    reason or "?",
                )
                return

        log.warning(
            "[exit_eng] OSM pending exit for unknown position | pos_id=%s local=%s broker=%s qty=%s reason=%s",
            position_id,
            local_order_id or "?",
            broker_order_id or "?",
            qty_i,
            reason or "?",
        )

    # ── BROKER / OSM RECONCILIATION HOOKS ───────────────────────────────────
    # These methods are intentionally backward compatible with older callers
    # while supporting exact OSM/local/broker order identity when supplied.

    def _pending_exit_has_identity(self, pos: ManagedPosition) -> bool:
        """True when current pending exit generation has local/broker identity."""
        return bool(
            getattr(pos, "pending_exit_local_order_id", "")
            or getattr(pos, "pending_exit_broker_order_id", "")
        )

    def _exit_identity_matches(
        self,
        pos: ManagedPosition,
        *,
        local_order_id: str = "",
        broker_order_id: str = "",
        allow_missing_when_no_pending_identity: bool = True,
    ) -> bool:
        """Validate an OSM/reconciler hook against the current pending exit generation."""
        local_order_id = str(local_order_id or "")
        broker_order_id = str(broker_order_id or "")
        pending_local = str(getattr(pos, "pending_exit_local_order_id", "") or "")
        pending_broker = str(getattr(pos, "pending_exit_broker_order_id", "") or "")
        supplied_identity = bool(local_order_id or broker_order_id)
        pending_identity = bool(pending_local or pending_broker)
        if not supplied_identity:
            return bool(allow_missing_when_no_pending_identity and not pending_identity)
        if local_order_id and pending_local and local_order_id != pending_local:
            return False
        if broker_order_id and pending_broker and broker_order_id != pending_broker:
            return False
        return True

    def _reject_stale_exit_hook(
        self,
        pos: ManagedPosition,
        *,
        hook_name: str,
        local_order_id: str = "",
        broker_order_id: str = "",
        reason: str = "",
    ) -> None:
        """Emit a loud event when a hook tries to mutate a different exit generation."""
        try:
            pos.last_exit_identity_reject_ts = datetime.now(timezone.utc)
            log.error(
                "[%s] STALE_EXIT_HOOK_REJECTED | hook=%s pos=%s got_local=%s got_broker=%s pending_local=%s pending_broker=%s reason=%s",
                getattr(pos, "ticker", "?"), hook_name, getattr(pos, "position_id", "?"),
                local_order_id or "?", broker_order_id or "?",
                getattr(pos, "pending_exit_local_order_id", "") or "?",
                getattr(pos, "pending_exit_broker_order_id", "") or "?",
                reason or "?",
            )
            self._emit_exit_event(
                pos,
                decision="REJECT",
                reason_code="STALE_EXIT_HOOK_REJECTED",
                explanation=f"Rejected {hook_name}; supplied identity does not match current pending exit generation.",
                stage="exit_reconciliation",
                extra_inputs={
                    "hook_name": hook_name,
                    "supplied_local_order_id": local_order_id,
                    "supplied_broker_order_id": broker_order_id,
                    "pending_exit_local_order_id": getattr(pos, "pending_exit_local_order_id", ""),
                    "pending_exit_broker_order_id": getattr(pos, "pending_exit_broker_order_id", ""),
                    "reason": reason,
                },
            )
        except Exception:
            pass

    def mark_position_closed(
        self,
        position_id: str,
        reason: str = "",
        *,
        qty_filled: Optional[int] = None,
        fill_price: Optional[float] = None,
        local_order_id: str = "",
        broker_order_id: str = "",
        cumulative_filled: Optional[int] = None,
        cumulative_filled_qty: Optional[int] = None,
        force: bool = False,
        reconciled: bool = False,
        **kwargs,
    ):
        """Called after broker + DB confirm the position is fully closed.

        v11: close hooks are identity-bound. If the engine has a current pending
        exit identity, a close for a different/no identity is rejected unless the
        caller explicitly marks the close as force/reconciled. This prevents stale
        close callbacks from mutating a newer live exit generation.
        """
        if not position_id:
            return
        reason_s = str(reason or "")
        local_order_id = str(local_order_id or "")
        broker_order_id = str(broker_order_id or "")
        force = bool(force or reconciled or kwargs.get("force") or kwargs.get("reconciled"))
        if "RECONCILER" in reason_s.upper():
            force = True

        with self._lock:
            for pos in self._positions:
                if str(pos.position_id or "") != str(position_id or ""):
                    continue
                if not force and not self._exit_identity_matches(
                    pos,
                    local_order_id=local_order_id,
                    broker_order_id=broker_order_id,
                    allow_missing_when_no_pending_identity=True,
                ):
                    self._reject_stale_exit_hook(
                        pos,
                        hook_name="mark_position_closed",
                        local_order_id=local_order_id,
                        broker_order_id=broker_order_id,
                        reason=reason_s,
                    )
                    return
                pos.closed = True
                pos.close_reason = reason_s or pos.close_reason or "broker_confirmed_closed"
                pos.quantity_remaining = 0
                try:
                    if fill_price is not None:
                        pos.current_option_price = float(fill_price)
                except Exception:
                    pass
                pos.exit_in_flight = False
                pos.pending_exit_reason = ""
                pos.pending_exit_action = ""
                pos.pending_exit_qty = 0
                pos.pending_exit_filled_qty = 0
                pos.pending_scale_counted = False
                pos.last_applied_exit_local_order_id = local_order_id or pos.pending_exit_local_order_id or pos.last_applied_exit_local_order_id
                pos.last_applied_exit_broker_order_id = broker_order_id or pos.pending_exit_broker_order_id or pos.last_applied_exit_broker_order_id
                pos.last_exit_signal_ts = None
                pos.last_callback_identity_missing = False
                pos.last_callback_identity_missing_ts = None
                pos.exit_identity_quarantine = False
                pos.last_exit_identity_quarantine_resolved_ts = datetime.now(timezone.utc)
                pos.exit_identity_quarantine_alert_count = getattr(pos, "exit_identity_quarantine_alert_count", 0)
                pos.last_exit_identity_quarantine_alert_ts = getattr(pos, "last_exit_identity_quarantine_alert_ts", None)
                pos.pending_exit_replace_allowed = False
                pos.pending_exit_replace_reason = ""
                pos.pending_exit_replace_allowed_ts = None
                self._emit_exit_event(
                    pos,
                    decision="CLOSED",
                    reason_code="BROKER_CONFIRMED_CLOSED",
                    explanation=pos.close_reason,
                    stage="exit_reconciliation",
                    extra_inputs={"local_order_id": local_order_id, "broker_order_id": broker_order_id, "force": force},
                )
        log.info("[exit_eng] Closed (broker-confirmed) | pos_id=%s reason=%s", position_id, reason)

    def clear_exit_in_flight(
        self,
        position_id: str,
        *,
        reason: str = "",
        local_order_id: str = "",
        broker_order_id: str = "",
        rejected: bool = False,
        force: bool = False,
        reconciled: bool = False,
        **kwargs,
    ):
        """Called when exit order is canceled/rejected/expired so the engine may retry.

        v11: clearing is identity-bound. A stale position-only clear cannot reopen
        submit eligibility or erase fill watermarks for a different pending exit
        generation. Use force=True/reconciled=True only after authoritative broker
        proof that no pending exit can still fill.
        """
        if not position_id:
            return
        reason_s = str(reason or "")
        local_order_id = str(local_order_id or "")
        broker_order_id = str(broker_order_id or "")
        force = bool(force or reconciled or kwargs.get("force") or kwargs.get("reconciled"))
        if "RECONCILER" in reason_s.upper() or "NEGATIVE_BROKER_CHECK" in reason_s.upper():
            force = True
        with self._lock:
            for pos in self._positions:
                if str(pos.position_id or "") != str(position_id or ""):
                    continue
                if not force and not self._exit_identity_matches(
                    pos,
                    local_order_id=local_order_id,
                    broker_order_id=broker_order_id,
                    allow_missing_when_no_pending_identity=True,
                ):
                    self._reject_stale_exit_hook(
                        pos,
                        hook_name="clear_exit_in_flight",
                        local_order_id=local_order_id,
                        broker_order_id=broker_order_id,
                        reason=reason_s,
                    )
                    return
                pos.exit_in_flight = False
                pos.pending_exit_reason = ""
                pos.pending_exit_action = ""
                pos.pending_exit_qty = 0
                pos.pending_exit_filled_qty = 0
                pos.pending_scale_counted = False
                pos.last_exit_clear_reason = reason_s
                pos.last_exit_clear_local_order_id = local_order_id
                pos.last_exit_clear_broker_order_id = broker_order_id
                pos.pending_exit_local_order_id = ""
                pos.pending_exit_broker_order_id = ""
                # Do not wipe last_applied_exit_cum_fill_by_order here. Late callbacks
                # for the just-cleared order may still arrive; new order submission resets it.
                pos.last_exit_signal_ts = None
                pos.last_exit_rejected = bool(rejected)
                pos.last_rejection_ts = time.time() if rejected else None
                pos.last_callback_identity_missing = False
                pos.last_callback_identity_missing_ts = None
                pos.exit_identity_quarantine = False
                pos.last_exit_identity_quarantine_resolved_ts = datetime.now(timezone.utc)
                pos.pending_exit_replace_allowed = False
                pos.pending_exit_replace_reason = ""
                pos.pending_exit_replace_allowed_ts = None
                self._assert_position_invariants(pos, "clear_exit_in_flight")
                self._emit_exit_event(
                    pos,
                    decision="ALERT" if rejected else "CLEARED",
                    reason_code="EXIT_REJECTED" if rejected else "EXIT_IN_FLIGHT_CLEARED",
                    explanation=reason_s or ("Exit order rejected/cleared" if rejected else "Exit in-flight cleared"),
                    stage="exit_reconciliation",
                    extra_inputs={"local_order_id": local_order_id, "broker_order_id": broker_order_id, "rejected": rejected, "force": force},
                )
        log.info("[exit_eng] Exit in-flight cleared | pos_id=%s rejected=%s reason=%s", position_id, rejected, reason)

    def on_exit_failure(

        self,
        position_id: str,
        *,
        reason: str = "",
        local_order_id: str = "",
        broker_order_id: str = "",
        **kwargs,
    ) -> None:
        """OSM v3-compatible hook for exit failure/rejection.

        Normalize failure into the same cleared/rejected state used by normal
        cancel/reject reconciliation. This prevents TypeError when OSM supplies
        keyword identity fields.
        """
        self.clear_exit_in_flight(
            position_id,
            reason=reason or "exit_failure",
            local_order_id=local_order_id,
            broker_order_id=broker_order_id,
            rejected=True,
            **kwargs,
        )

    def note_partial_exit_fill(
        self,
        position_id: str,
        qty_filled: int = 0,
        *,
        fill_price: Optional[float] = None,
        local_order_id: str = "",
        broker_order_id: str = "",
        cumulative_filled: Optional[int] = None,
        cumulative_filled_qty: Optional[int] = None,
        **kwargs,
    ):
        """Called when broker confirms an exit fill.

        qty_filled is treated as a delta by default. If cumulative_filled_qty is
        supplied by OSM/reconciler, it is treated as cumulative for the CURRENT
        exit order identity only. This prevents one order's fill watermark from
        corrupting the next order's fill math.
        """
        if not position_id:
            return
        if cumulative_filled_qty is None and cumulative_filled is not None:
            cumulative_filled_qty = cumulative_filled
        try:
            if fill_price is not None:
                fill_price = float(fill_price)
        except Exception:
            fill_price = None

        applied_delta = 0
        with self._lock:
            for pos in self._positions:
                if pos.position_id != position_id:
                    continue

                if local_order_id and pos.pending_exit_local_order_id and local_order_id != pos.pending_exit_local_order_id:
                    log.warning(
                        "[exit_eng] Ignoring stale local exit fill | pos_id=%s got=%s expected=%s",
                        position_id, local_order_id, pos.pending_exit_local_order_id,
                    )
                    return
                if broker_order_id and pos.pending_exit_broker_order_id and broker_order_id != pos.pending_exit_broker_order_id:
                    log.warning(
                        "[exit_eng] Ignoring stale broker exit fill | pos_id=%s got=%s expected=%s",
                        position_id, broker_order_id, pos.pending_exit_broker_order_id,
                    )
                    return

                # Prefer exact broker identity, then local identity, then the
                # current pending identity. Fall back to a synthetic key only for
                # legacy/no-identity callbacks so cumulative fills remain scoped
                # to this pending exit generation instead of the position lifetime.
                order_key = (
                    broker_order_id
                    or local_order_id
                    or pos.pending_exit_broker_order_id
                    or pos.pending_exit_local_order_id
                    or f"pending:{pos.position_id}:{pos.last_exit_signal_ts.isoformat() if pos.last_exit_signal_ts else 'unknown'}"
                )

                if cumulative_filled_qty is not None:
                    cum = max(0, int(cumulative_filled_qty or 0))
                    prev = int(pos.last_applied_exit_cum_fill_by_order.get(order_key, 0) or 0)
                    delta = max(0, cum - prev)
                    pos.last_applied_exit_cum_fill_by_order[order_key] = max(prev, cum)
                    # Keep aggregate field for old diagnostics only; it is not
                    # used for delta calculation anymore.
                    pos.last_applied_exit_cum_fill = max(int(pos.last_applied_exit_cum_fill or 0), cum)
                else:
                    delta = max(0, int(qty_filled or 0))
                    prev = int(pos.last_applied_exit_cum_fill_by_order.get(order_key, 0) or 0)
                    pos.last_applied_exit_cum_fill_by_order[order_key] = prev + delta
                    pos.last_applied_exit_cum_fill += delta

                if delta <= 0:
                    self._emit_exit_event(
                        pos,
                        decision="HOLD",
                        reason_code="DUPLICATE_EXIT_FILL_IGNORED",
                        explanation="Duplicate or zero-delta exit fill ignored for current exit order identity",
                        stage="exit_reconciliation",
                        extra_inputs={
                            "order_key": order_key,
                            "cumulative_filled_qty": cumulative_filled_qty,
                            "local_order_id": local_order_id,
                            "broker_order_id": broker_order_id,
                        },
                    )
                    return

                applied_delta = delta
                if fill_price is not None:
                    pos.current_option_price = float(fill_price)
                pos.pending_exit_filled_qty += delta
                pos.quantity_remaining = max(0, int(pos.quantity_remaining or 0) - delta)
                pos.last_applied_exit_local_order_id = local_order_id or pos.pending_exit_local_order_id or pos.last_applied_exit_local_order_id
                pos.last_applied_exit_broker_order_id = broker_order_id or pos.pending_exit_broker_order_id or pos.last_applied_exit_broker_order_id
                if local_order_id or broker_order_id or pos.pending_exit_local_order_id or pos.pending_exit_broker_order_id:
                    pos.last_callback_identity_missing = False
                    pos.last_callback_identity_missing_ts = None
                    pos.exit_identity_quarantine = False
                    pos.exit_identity_quarantine_alert_count = 0
                    pos.last_exit_identity_quarantine_alert_ts = None
                    pos.pending_exit_replace_allowed = False
                    pos.pending_exit_replace_reason = ""
                    pos.pending_exit_replace_allowed_ts = None

                # Count scale-out only after a meaningful confirmed tranche fill.
                # A tiny 1-contract partial on a large intended scale should not
                # immediately convert the strategy into runner mode. For small
                # tranches, one filled contract is necessarily meaningful.
                if (
                    (pos.pending_exit_action or "").upper() == "SCALE_OUT"
                    and pos.pending_exit_qty > 0
                    and not pos.pending_scale_counted
                    and pos.quantity_remaining > 0
                ):
                    # v7: do not advance runner/scale state until the intended
                    # scale tranche is fully broker-confirmed. Earlier v6 used
                    # a 50% meaningful-fill policy, but that can switch strategy
                    # state while the original scale order is still partially live.
                    fully_confirmed_scale = pos.pending_exit_filled_qty >= pos.pending_exit_qty
                    if fully_confirmed_scale:
                        pos.scale_outs_done += 1
                        pos.pending_scale_counted = True
                        try:
                            from ap.db import conn, run_with_retry as _rwr_s
                            _sd = pos.scale_outs_done
                            _qr = pos.quantity_remaining
                            _pi = pos.position_id
                            if _pi:
                                def _save_scale():
                                    with conn() as _c:
                                        _c.execute(
                                            "UPDATE positions SET scale_outs_done=%s, quantity_remaining=%s WHERE id=%s",
                                            (_sd, _qr, _pi),
                                        )
                                _rwr_s(_save_scale)
                        except Exception as _se:
                            log.debug("[%s] scale/qty persist failed (non-critical): %s", pos.ticker, _se)

                fully_filled_pending = (
                    pos.pending_exit_qty > 0
                    and pos.pending_exit_filled_qty >= pos.pending_exit_qty
                )
                if pos.quantity_remaining <= 0:
                    pos.closed = True
                    pos.close_reason = pos.pending_exit_reason or "exit_fill_closed"
                if fully_filled_pending or pos.closed:
                    pos.exit_in_flight = False
                    pos.pending_exit_reason = ""
                    pos.pending_exit_action = ""
                    pos.pending_exit_qty = 0
                    pos.pending_exit_filled_qty = 0
                    pos.pending_scale_counted = False
                    pos.pending_exit_local_order_id = ""
                    pos.pending_exit_broker_order_id = ""
                    pos.last_applied_exit_cum_fill_by_order = {}
                    pos.last_applied_exit_cum_fill = 0
                    pos.last_exit_signal_ts = None
                    pos._exit_stuck_count = 0

                self._assert_position_invariants(pos, "note_partial_exit_fill")
                self._emit_exit_event(
                    pos,
                    decision="FILL",
                    reason_code="EXIT_FILL_APPLIED",
                    explanation=f"Applied broker-confirmed exit fill delta={delta}",
                    stage="exit_reconciliation",
                    extra_inputs={
                        "delta_qty": delta,
                        "cumulative_filled_qty": cumulative_filled_qty,
                        "order_key": order_key,
                        "local_order_id": local_order_id,
                        "broker_order_id": broker_order_id,
                        "scale_counted": pos.pending_scale_counted,
                    },
                )
                break
            # Keep closed position object in memory for late broker callbacks and diagnostics.
            # active_positions() filters closed positions, so this does not affect trading decisions.
        log.info("[exit_eng] Exit fill noted | pos_id=%s qty_delta=%d", position_id, int(applied_delta or qty_filled or 0))

    def _mark_exit_submitted(
        self,
        pos: ManagedPosition,
        decision: ExitDecision,
        *,
        local_order_id: str = "",
        broker_order_id: str = "",
    ) -> None:
        """Mark an exit as submitted after downstream callback accepts the intent."""
        pos.exit_in_flight = True
        pos.pending_exit_reason = decision.reason or ""
        pos.pending_exit_action = (decision.action or "").upper()
        pos.pending_exit_qty = max(0, int(decision.quantity or 0))
        pos.pending_exit_filled_qty = 0
        pos.pending_scale_counted = False
        pos.pending_exit_local_order_id = local_order_id or pos.pending_exit_local_order_id or ""
        pos.pending_exit_broker_order_id = broker_order_id or pos.pending_exit_broker_order_id or ""
        # New pending order generation: per-order cumulative fill watermarks
        # must not inherit the prior order's cumulative semantics.
        pos.last_applied_exit_cum_fill_by_order = {}
        pos.last_applied_exit_cum_fill = 0
        pos.last_exit_signal_ts = datetime.now(timezone.utc)
        pos.last_exit_rejected = False
        pos._exit_stuck_count = 0
        pos.exit_identity_quarantine = bool(pos.last_callback_identity_missing)
        pos.pending_exit_replace_allowed = False
        pos.pending_exit_replace_reason = ""
        pos.pending_exit_replace_allowed_ts = None

    def mark_exit_replacement_safe(
        self,
        position_id: str,
        reason: str = "",
        *,
        local_order_id: str = "",
        broker_order_id: str = "",
        force: bool = False,
        reconciled: bool = False,
        **kwargs,
    ) -> None:
        """Allow exactly one replacement exit after external cancel/reconcile proof.

        v11: replacement proof is bound to the current pending exit generation.
        A misplaced reconciler call cannot authorize replacement against the wrong
        still-live pending exit unless it passes force=True/reconciled=True after
        authoritative broker proof.
        """
        if not position_id:
            return
        reason_s = str(reason or "")
        local_order_id = str(local_order_id or "")
        broker_order_id = str(broker_order_id or "")
        force = bool(force or reconciled or kwargs.get("force") or kwargs.get("reconciled"))
        if "NEGATIVE_BROKER_CHECK" in reason_s.upper() or "RECONCILER" in reason_s.upper():
            force = force or not bool(local_order_id or broker_order_id)
        with self._lock:
            for pos in self._positions:
                if str(pos.position_id or "") == str(position_id or "") and not pos.closed:
                    if not force and not self._exit_identity_matches(
                        pos,
                        local_order_id=local_order_id,
                        broker_order_id=broker_order_id,
                        allow_missing_when_no_pending_identity=False,
                    ):
                        self._reject_stale_exit_hook(
                            pos,
                            hook_name="mark_exit_replacement_safe",
                            local_order_id=local_order_id,
                            broker_order_id=broker_order_id,
                            reason=reason_s,
                        )
                        return
                    pos.pending_exit_replace_allowed = True
                    pos.pending_exit_replace_reason = reason_s or "external_cancel_or_reconcile_proof"
                    pos.pending_exit_replace_allowed_ts = datetime.now(timezone.utc)
                    pos.exit_identity_quarantine = False
                    pos.last_exit_identity_quarantine_resolved_ts = datetime.now(timezone.utc)
                    self._emit_exit_event(
                        pos,
                        "ALERT",
                        "EXIT_REPLACEMENT_MARKED_SAFE",
                        "External OSM/reconciler proof marked pending exit safe to replace once.",
                        stage="exit_reconciliation",
                        extra_inputs={
                            "reason": pos.pending_exit_replace_reason,
                            "proof_local_order_id": local_order_id,
                            "proof_broker_order_id": broker_order_id,
                            "force": force,
                            "pending_exit_reason": pos.pending_exit_reason,
                            "pending_exit_local_order_id": pos.pending_exit_local_order_id,
                            "pending_exit_broker_order_id": pos.pending_exit_broker_order_id,
                        },
                    )
                    log.critical(
                        "[%s] EXIT REPLACEMENT MARKED SAFE | pos=%s proof_local=%s proof_broker=%s reason=%s force=%s",
                        pos.ticker, position_id, local_order_id or "?", broker_order_id or "?", pos.pending_exit_replace_reason, force,
                    )
                    return

    def _eligible_for_new_exit(self, pos: ManagedPosition, now_utc: datetime) -> bool:
        """Compatibility wrapper around centralized submit gate."""
        return self._can_submit_exit(pos, now_utc, reason=pos.pending_exit_reason or "poll")

    def _run_sentinels(self):
        """Safety sentinels for missed TP/stop, stuck exits, and dead trades.

        Sentinels never submit directly. They route through _submit_exit_decision
        so duplicate/in-flight gating and observability remain consistent.
        """
        now = datetime.now(timezone.utc)
        for pos in list(self._positions):
            if pos.closed or int(pos.quantity_remaining or 0) <= 0:
                continue
            age_min = (now - pos.opened_at).total_seconds() / 60 if pos.opened_at else 0
            pnl = pos.option_pnl_pct
            peak = pos.peak_pnl_pct

            # Quarantine sentinel: missing callback identity is intentionally safe-locking.
            # Do not auto-clear it here; make it loud and repeated until one recovery
            # hook resolves truth: set_pending_exit_order(), note_partial_exit_fill(),
            # mark_position_closed(), clear_exit_in_flight(), or mark_exit_replacement_safe().
            if (
                pos.exit_in_flight
                and getattr(pos, "last_callback_identity_missing", False)
                and getattr(pos, "exit_identity_quarantine", False)
                and not getattr(pos, "pending_exit_replace_allowed", False)
            ):
                identity_started = pos.last_callback_identity_missing_ts or pos.last_exit_signal_ts or now
                try:
                    quarantine_age_sec = max(0.0, (now - identity_started).total_seconds())
                except Exception:
                    quarantine_age_sec = 0.0
                last_alert_ts = getattr(pos, "last_exit_identity_quarantine_alert_ts", None)
                should_alert = quarantine_age_sec >= 30.0 and (
                    last_alert_ts is None
                    or (now - last_alert_ts).total_seconds() >= 30.0
                )
                if should_alert:
                    pos.exit_identity_quarantine_alert_count = int(
                        getattr(pos, "exit_identity_quarantine_alert_count", 0) or 0
                    ) + 1
                    pos.last_exit_identity_quarantine_alert_ts = now
                    log.error(
                        "[%s] EXIT IDENTITY QUARANTINE ACTIVE %.1fs x%d — duplicate exits blocked; "
                        "reconciler/OSM must resolve | pos_id=%s action=%s qty=%s local=%s broker=%s reason=%s",
                        pos.ticker,
                        quarantine_age_sec,
                        pos.exit_identity_quarantine_alert_count,
                        pos.position_id or "?",
                        pos.pending_exit_action or "?",
                        pos.pending_exit_qty,
                        pos.pending_exit_local_order_id or "?",
                        pos.pending_exit_broker_order_id or "?",
                        pos.pending_exit_reason or "?",
                    )
                    self._emit_exit_event(
                        pos,
                        decision="ALERT",
                        reason_code="EXIT_IDENTITY_QUARANTINE_NEEDS_RECONCILE",
                        explanation=(
                            "Accepted exit callback returned no local/broker order identity. "
                            "Duplicate exits are blocked until OSM/reconciler proves fill, cancel, reject, "
                            "broker order identity, or safe replacement."
                        ),
                        stage="system_alert",
                        extra_inputs={
                            "quarantine_age_sec": quarantine_age_sec,
                            "alert_count": pos.exit_identity_quarantine_alert_count,
                            "pending_exit_action": pos.pending_exit_action,
                            "pending_exit_qty": pos.pending_exit_qty,
                            "pending_exit_reason": pos.pending_exit_reason,
                            "pending_exit_local_order_id": pos.pending_exit_local_order_id,
                            "pending_exit_broker_order_id": pos.pending_exit_broker_order_id,
                        },
                    )

            if peak >= IMMEDIATE_TP_PCT and not pos.exit_in_flight and age_min > 1:
                log.error(
                    "[SENTINEL] %s | MISSED TP — peaked +%.0f%% but no exit submitted | pos=%s pnl=%.1f%% age=%.0fm",
                    pos.ticker, peak * 100, pos.position_id, pnl * 100, age_min,
                )

            if pnl <= HARD_STOP_PCT and not pos.exit_in_flight and age_min > 1:
                decision = ExitDecision(
                    action="CLOSE_ALL",
                    quantity=pos.quantity_remaining,
                    reason=f"SENTINEL FORCED EXIT — {pnl*100:.0f}% with no exit order",
                    urgency="IMMEDIATE",
                    pnl_pct=pnl,
                )
                self._submit_exit_decision(pos, decision, from_sentinel=True, allow_inflight_override=True)
                continue

            if pos.exit_in_flight and pos.last_exit_signal_ts:
                flight_sec = (now - pos.last_exit_signal_ts).total_seconds()
                if flight_sec > 300:
                    pos._exit_stuck_count = int(getattr(pos, "_exit_stuck_count", 0) or 0) + 1
                    if pos._exit_stuck_count >= 2:
                        pos.last_exit_rejected = True
                        log.error(
                            "[SENTINEL] %s | EXIT STUCK x%d — %.0fs in-flight, no fill | pos=%s",
                            pos.ticker, pos._exit_stuck_count, flight_sec, pos.position_id,
                        )
                        self._emit_exit_event(
                            pos,
                            decision="ALERT",
                            reason_code="EXIT_STUCK",
                            explanation=f"Exit in flight for {flight_sec:.0f}s with no fill",
                            stage="system_alert",
                            extra_inputs={"flight_sec": flight_sec, "stuck_count": pos._exit_stuck_count},
                        )
                    else:
                        log.warning(
                            "[SENTINEL] %s | EXIT STUCK — %.0fs in-flight, no fill | pos=%s",
                            pos.ticker, flight_sec, pos.position_id,
                        )

            DEAD_TRADE_MIN = 45
            DEAD_TRADE_LOW = -0.08
            DEAD_TRADE_HIGH = 0.05
            if (
                not pos.exit_in_flight
                and age_min >= DEAD_TRADE_MIN
                and DEAD_TRADE_LOW <= pnl <= DEAD_TRADE_HIGH
                and pos.max_profit_seen < DEAD_TRADE_HIGH
            ):
                progress = 0.0
                if pos.underlying_entry and pos.underlying_target and pos.current_underlying:
                    denom = abs(pos.underlying_target - pos.underlying_entry)
                    if denom > 0:
                        progress = abs(pos.current_underlying - pos.underlying_entry) / denom
                if progress < 0.30:
                    decision = ExitDecision(
                        action="CLOSE_ALL",
                        quantity=pos.quantity_remaining,
                        reason=(
                            f"TIME STOP — thesis not confirmed after {age_min:.0f}min "
                            f"pnl={pnl*100:.1f}% progress={progress*100:.0f}% toward target"
                        ),
                        urgency="HIGH",
                        pnl_pct=pnl,
                    )
                    self._submit_exit_decision(pos, decision, from_sentinel=True)

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
                    "pending_exit_action": getattr(pos, "pending_exit_action", ""),
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
        code = _classify_exit_decision(decision)
        return None if code == "UNKNOWN_EXIT" else code

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

        if (
            pos.exit_in_flight
            and getattr(pos, "last_callback_identity_missing", False)
            and not getattr(pos, "pending_exit_replace_allowed", False)
        ):
            identity_age = 0.0
            if pos.last_callback_identity_missing_ts:
                try:
                    identity_age = (now_utc - pos.last_callback_identity_missing_ts).total_seconds()
                except Exception:
                    identity_age = 0.0
            elif pos.last_exit_signal_ts:
                try:
                    identity_age = (now_utc - pos.last_exit_signal_ts).total_seconds()
                except Exception:
                    identity_age = 0.0

            # v7: never auto-clear missing-identity quarantine. If the callback
            # placed a live broker order but returned no IDs, clearing in-flight
            # locally can create a duplicate exit. Only reconciler/OSM truth may
            # clear this via mark_position_closed(), clear_exit_in_flight(),
            # note_partial_exit_fill(), or mark_exit_replacement_safe().
            pos.exit_identity_quarantine = True
            severity = "ERROR" if identity_age >= 20.0 else "HOLD"
            reason_code = (
                "EXIT_IDENTITY_QUARANTINE_STALE_NEEDS_RECONCILE"
                if identity_age >= 20.0
                else "EXIT_IDENTITY_QUARANTINE_BLOCK"
            )
            if identity_age >= 20.0:
                log.error(
                    "[%s] EXIT IDENTITY QUARANTINE %.1fs — duplicate submits blocked; reconciler/OSM must resolve | pos_id=%s pending=%s",
                    pos.ticker, identity_age, pos.position_id, pos.pending_exit_reason,
                )
            self._emit_exit_event(
                pos,
                severity,
                reason_code,
                (
                    "Exit suppressed because the prior accepted callback returned no "
                    "local/broker order identity. Duplicate submissions remain blocked "
                    "until broker/OSM reconciliation confirms fill, rejection, cancel, or replacement safety."
                ),
                stage="exit_submission",
                extra_inputs={
                    "identity_age_sec": identity_age,
                    "pending_exit_reason": pos.pending_exit_reason,
                    "pending_exit_action": pos.pending_exit_action,
                    "pending_exit_qty": pos.pending_exit_qty,
                    "pending_exit_local_order_id": pos.pending_exit_local_order_id,
                    "pending_exit_broker_order_id": pos.pending_exit_broker_order_id,
                    "last_callback_identity_missing_ts": str(pos.last_callback_identity_missing_ts or ""),
                },
            )
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

            # v7: replacement/runner emergency override cannot submit a second
            # real-world exit unless an external broker/OSM path has explicitly
            # proven the prior working exit is canceled, rejected, or otherwise
            # safe to replace. Staleness or higher priority alone is not cancel proof.
            if not getattr(pos, "pending_exit_replace_allowed", False):
                self._emit_exit_event(
                    pos,
                    "HOLD",
                    "EXIT_OVERRIDE_NEEDS_CANCEL_OR_RECONCILE_PROOF",
                    (
                        "Inflight override denied. Pending exit may still be live at broker; "
                        "replacement requires mark_exit_replacement_safe() after cancel/reconcile proof."
                    ),
                    extra_inputs={
                        "flight_sec": flight_sec,
                        "pending_exit_reason": pos.pending_exit_reason,
                        "override_reason": reason,
                        "pending_exit_local_order_id": pos.pending_exit_local_order_id,
                        "pending_exit_broker_order_id": pos.pending_exit_broker_order_id,
                    },
                )
                return False

            # Consume one-shot replacement authorization. Once broker/OSM has
            # proven the older order cannot still fill, precedence should not
            # re-block the replacement solely because it has the same class.
            replacement_proof = True
            pos.pending_exit_replace_allowed = False
            pos.pending_exit_replace_reason = ""
            pos.pending_exit_replace_allowed_ts = None

            # Executable precedence guard:
            # do not allow a lower/equal priority exit to override a fresh higher/equal pending exit.
            # Stale pending exits may still be overridden so the system can rescue stuck risk.
            stale_enough = flight_sec >= 20.0
            new_code = _classify_exit_decision(ExitDecision("CLOSE_ALL", 0, reason or "", "IMMEDIATE"))
            pending_code = _classify_exit_decision(ExitDecision(pos.pending_exit_action or "CLOSE_ALL", 0, pos.pending_exit_reason or "", "IMMEDIATE"))
            new_pri = _exit_priority(new_code)
            pending_pri = _exit_priority(pending_code)
            if (not replacement_proof) and not stale_enough and pending_code != "UNKNOWN_EXIT" and new_pri >= pending_pri:
                self._emit_exit_event(
                    pos,
                    "HOLD",
                    "EXIT_OVERRIDE_DENIED_PRECEDENCE",
                    (
                        f"Inflight override denied by precedence; pending={pending_code} "
                        f"new={new_code} flight={flight_sec:.1f}s"
                    ),
                    extra_inputs={
                        "flight_sec": flight_sec,
                        "pending_exit_reason": pos.pending_exit_reason,
                        "override_reason": reason,
                        "pending_code": pending_code,
                        "new_code": new_code,
                        "pending_priority": pending_pri,
                        "new_priority": new_pri,
                    },
                )
                return False

            higher_priority_override = (new_pri < pending_pri)
            emergency_reason = _is_runner_protective_reason(reason)
            already_same_runner_protection = _is_same_or_equivalent_runner_protection(
                pos.pending_exit_reason,
                reason,
            )

            if (not replacement_proof) and already_same_runner_protection and not stale_enough and not higher_priority_override:
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

            if (not replacement_proof) and not stale_enough and not emergency_reason and not higher_priority_override:
                self._emit_exit_event(
                    pos,
                    "HOLD",
                    "EXIT_OVERRIDE_DENIED",
                    f"Inflight override denied; existing exit not stale enough: {flight_sec:.1f}s",
                    extra_inputs={
                        "flight_sec": flight_sec,
                        "pending_exit_reason": pos.pending_exit_reason,
                        "override_reason": reason,
                        "pending_code": pending_code,
                        "new_code": new_code,
                        "pending_priority": pending_pri,
                        "new_priority": new_pri,
                        "higher_priority_override": higher_priority_override,
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
                    "higher_priority_override": higher_priority_override,
                    "pending_code": pending_code,
                    "new_code": new_code,
                    "pending_priority": pending_pri,
                    "new_priority": new_pri,
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
                    original_qty = int(row.get("qty", 1) or 1)
                    mp = ManagedPosition(
                        ticker=row.get("underlying", "") or row.get("symbol", ""),
                        option_symbol=row.get("contract", ""),
                        side=row.get("direction", "CALL"),
                        quantity=original_qty,
                        entry_price=float(row.get("avg_fill", 0) or 0),
                        underlying_entry=float(row.get("underlying_entry", 0) or 0),
                        underlying_target=float(row.get("target_underlying") or 0),
                        underlying_stop=float(row.get("stop_underlying") or 0),
                        position_id=str(row.get("id") or ""),
                        client_id=str(row.get("client_id") or ""),
                        signal_id=str(row.get("signal_id") or ""),
                    )
                    mp.current_option_price = float(row.get("avg_fill", 0) or 0)
                    # Restore runner state from DB so restarts pick up mid-trade correctly.
                    mp.scale_outs_done = int(row.get("scale_outs_done", 0) or 0)

                    # Preserve original filled size in mp.quantity. Only restore
                    # quantity_remaining from DB so restart does not turn a 4-lot
                    # with 1 runner left into a fake original 1-lot.
                    _qty_remaining = int(row.get("quantity_remaining", 0) or 0)
                    if _qty_remaining > 0:
                        mp.quantity_remaining = min(_qty_remaining, mp.quantity) if mp.quantity > 0 else _qty_remaining
                    else:
                        mp.quantity_remaining = mp.quantity
                    log.debug(
                        "seed_from_db: %s original_qty=%d qty_remaining=%d scale_outs=%d",
                        mp.ticker, mp.quantity, mp.quantity_remaining, mp.scale_outs_done,
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

                now_utc = datetime.now(timezone.utc)
                underlying_progressed = False
                option_progressed = False

                if uq:
                    last = float(uq.get("last") or uq.get("bid") or 0)
                    if last > 0:
                        pos.current_underlying = last
                        underlying_progressed = True

                if oq:
                    bid = float(oq.get("bid", 0) or 0)
                    ask = float(oq.get("ask", 0) or 0)
                    if bid > 0 and ask > 0:
                        pos.current_bid          = bid
                        pos.current_ask          = ask
                        pos.current_option_price = (bid + ask) / 2
                        option_progressed = True

                if underlying_progressed:
                    pos.last_underlying_quote_update_ts = now_utc
                    pos.last_underlying_quote_missing_ts = None
                else:
                    pos.last_underlying_quote_missing_ts = now_utc

                if option_progressed:
                    pos.last_option_quote_update_ts = now_utc
                    pos.last_option_quote_missing_ts = None
                else:
                    pos.last_option_quote_missing_ts = now_utc

                if underlying_progressed or option_progressed:
                    pos.last_quote_update_ts = now_utc
                    pos.last_quote_missing_ts = None
                else:
                    pos.last_quote_missing_ts = now_utc

                # Gate: don't re-fire while an exit is in-flight
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
                    decision.reason_code = _classify_exit_decision(decision)
                    if decision.should_act:
                        # Decision-path quote freshness gate. This prevents stale option
                        # marks from even entering the submit queue for normal exits.
                        # _submit_exit_decision() repeats the same check as defense in depth.
                        option_quote_stale, option_quote_age_sec, option_quote_state = _is_option_quote_stale(pos, now_utc)
                        if option_quote_stale and not _is_forced_risk_exit_code(decision.reason_code):
                            self._emit_exit_event(
                                pos,
                                decision="HOLD",
                                reason_code="OPTION_QUOTE_STALE_DECISION_SUPPRESSED",
                                explanation=(
                                    f"Suppressed {decision.reason_code} before submit queue because option quote is not fresh: "
                                    f"{option_quote_state} age={option_quote_age_sec}"
                                ),
                                stage="exit_decision",
                                extra_inputs={
                                    "decision_action": decision.action,
                                    "decision_qty": decision.quantity,
                                    "decision_pnl_pct": decision.pnl_pct,
                                    "decision_reason_code": decision.reason_code,
                                    "option_quote_state": option_quote_state,
                                    "option_quote_age_sec": option_quote_age_sec,
                                    "last_option_quote_update_ts": (
                                        pos.last_option_quote_update_ts.isoformat()
                                        if getattr(pos, "last_option_quote_update_ts", None) else ""
                                    ),
                                },
                            )
                            log.error(
                                "[%s] EXIT DECISION SUPPRESSED: stale option quote | code=%s age=%s state=%s pos_id=%s",
                                pos.ticker, decision.reason_code, option_quote_age_sec, option_quote_state, pos.position_id or "?"
                            )
                            continue

                        if option_quote_stale and _is_forced_risk_exit_code(decision.reason_code):
                            decision.reason = f"{decision.reason} | DEGRADED_QUOTE_MODE:{option_quote_state}"

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
                                "decision_reason_code": decision.reason_code,
                                "option_quote_stale": option_quote_stale,
                                "option_quote_age_sec": option_quote_age_sec,
                                "option_quote_state": option_quote_state,
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

    def _extract_exit_order_identity(self, callback_result) -> dict:
        """Best-effort bridge from execution callback return value to OSM identity.

        Supported callback returns:
        - None: legacy callback, no identity returned
        - False: explicit failure, do not mark submitted
        - dict/object with local_order_id / broker_order_id / order_id / id
        - tuple(local_order_id, broker_order_id)
        """
        identity = {
            "accepted": True,
            "local_order_id": "",
            "broker_order_id": "",
            "raw_status": "",
        }
        if callback_result is False:
            identity["accepted"] = False
            identity["raw_status"] = "callback_false"
            return identity
        if callback_result is None:
            return identity

        def _get(obj, *names):
            for name in names:
                if isinstance(obj, dict) and name in obj:
                    return obj.get(name)
                if hasattr(obj, name):
                    return getattr(obj, name)
            return None

        if isinstance(callback_result, (tuple, list)):
            if len(callback_result) >= 1 and callback_result[0] is not None:
                identity["local_order_id"] = str(callback_result[0])
            if len(callback_result) >= 2 and callback_result[1] is not None:
                identity["broker_order_id"] = str(callback_result[1])
            return identity

        status = _get(callback_result, "status", "raw_status", "state")
        if status is not None:
            identity["raw_status"] = str(status)

        accepted = _get(callback_result, "accepted", "ok", "success")
        # OSM submit_exit may intentionally return ok=False while parking the
        # order in EXIT_SUBMITTED identity quarantine (broker accepted but did
        # not return broker_order_id). That is not a callback failure from the
        # exit engine's perspective: it is an active local OSM order that must
        # block duplicates until reconciler resolves it.
        identity_quarantine = bool(_get(callback_result, "identity_quarantine"))
        status_norm = str(identity.get("raw_status") or "").upper()
        if accepted is False and not (identity_quarantine or status_norm == "EXIT_SUBMITTED"):
            identity["accepted"] = False

        local_id = _get(
            callback_result,
            "local_order_id",
            "exit_local_order_id",
            "order_local_id",
            "client_order_id",
        )
        broker_id = _get(
            callback_result,
            "broker_order_id",
            "exit_broker_order_id",
            "order_id",
            "broker_id",
            "id",
        )

        if local_id is not None:
            identity["local_order_id"] = str(local_id)
        if broker_id is not None:
            identity["broker_order_id"] = str(broker_id)

        return identity

    def health_snapshot(self, *, stale_after_sec: float = 60.0) -> dict:
        """Return watchdog-friendly health state for dashboard/self-healing.

        This does not submit, cancel, or mutate orders. It is safe for polling.
        """
        now = datetime.now(timezone.utc)
        with self._lock:
            positions = []
            stale_inflight = []
            stale_quotes = []
            stale_underlying_quotes = []
            stale_option_quotes = []
            missing_callback_identity = []
            for pos in self._positions:
                flight_sec = None
                quote_age_sec = None
                underlying_quote_age_sec = None
                option_quote_age_sec = None
                ident = pos.position_id or pos.option_symbol or pos.ticker
                if pos.exit_in_flight and pos.last_exit_signal_ts:
                    flight_sec = max(0.0, (now - pos.last_exit_signal_ts).total_seconds())
                    if flight_sec >= stale_after_sec:
                        stale_inflight.append(ident)
                if pos.last_quote_update_ts:
                    quote_age_sec = max(0.0, (now - pos.last_quote_update_ts).total_seconds())
                    if quote_age_sec >= stale_after_sec and not pos.closed:
                        stale_quotes.append(ident)
                elif not pos.closed:
                    stale_quotes.append(ident)
                if pos.last_underlying_quote_update_ts:
                    underlying_quote_age_sec = max(0.0, (now - pos.last_underlying_quote_update_ts).total_seconds())
                    if underlying_quote_age_sec >= stale_after_sec and not pos.closed:
                        stale_underlying_quotes.append(ident)
                elif not pos.closed:
                    stale_underlying_quotes.append(ident)
                if pos.last_option_quote_update_ts:
                    option_quote_age_sec = max(0.0, (now - pos.last_option_quote_update_ts).total_seconds())
                    if option_quote_age_sec >= stale_after_sec and not pos.closed:
                        stale_option_quotes.append(ident)
                elif not pos.closed:
                    stale_option_quotes.append(ident)
                if getattr(pos, "last_callback_identity_missing", False) and pos.exit_in_flight:
                    missing_callback_identity.append(pos.position_id or pos.option_symbol or pos.ticker)
                positions.append({
                    "position_id": pos.position_id,
                    "ticker": pos.ticker,
                    "option_symbol": pos.option_symbol,
                    "quantity_remaining": pos.quantity_remaining,
                    "closed": pos.closed,
                    "exit_in_flight": pos.exit_in_flight,
                    "pending_exit_action": pos.pending_exit_action,
                    "pending_exit_qty": pos.pending_exit_qty,
                    "pending_exit_filled_qty": pos.pending_exit_filled_qty,
                    "pending_exit_local_order_id": pos.pending_exit_local_order_id,
                    "pending_exit_broker_order_id": pos.pending_exit_broker_order_id,
                    "flight_sec": flight_sec,
                    "quote_age_sec": quote_age_sec,
                    "underlying_quote_age_sec": underlying_quote_age_sec,
                    "option_quote_age_sec": option_quote_age_sec,
                    "last_quote_update_ts": pos.last_quote_update_ts.isoformat() if pos.last_quote_update_ts else "",
                    "last_quote_missing_ts": pos.last_quote_missing_ts.isoformat() if pos.last_quote_missing_ts else "",
                    "last_underlying_quote_update_ts": pos.last_underlying_quote_update_ts.isoformat() if pos.last_underlying_quote_update_ts else "",
                    "last_underlying_quote_missing_ts": pos.last_underlying_quote_missing_ts.isoformat() if pos.last_underlying_quote_missing_ts else "",
                    "last_option_quote_update_ts": pos.last_option_quote_update_ts.isoformat() if pos.last_option_quote_update_ts else "",
                    "last_option_quote_missing_ts": pos.last_option_quote_missing_ts.isoformat() if pos.last_option_quote_missing_ts else "",
                    "last_callback_identity_missing": getattr(pos, "last_callback_identity_missing", False),
                    "exit_identity_quarantine": getattr(pos, "exit_identity_quarantine", False),
                    "exit_identity_quarantine_alert_count": getattr(pos, "exit_identity_quarantine_alert_count", 0),
                    "last_exit_identity_quarantine_alert_ts": (
                        pos.last_exit_identity_quarantine_alert_ts.isoformat()
                        if getattr(pos, "last_exit_identity_quarantine_alert_ts", None) else ""
                    ),
                    "pending_exit_replace_allowed": getattr(pos, "pending_exit_replace_allowed", False),
                    "pending_exit_replace_reason": getattr(pos, "pending_exit_replace_reason", ""),
                    "pending_exit_replace_allowed_ts": (
                        pos.pending_exit_replace_allowed_ts.isoformat()
                        if getattr(pos, "pending_exit_replace_allowed_ts", None) else ""
                    ),
                    "last_callback_identity_missing_ts": (
                        pos.last_callback_identity_missing_ts.isoformat()
                        if pos.last_callback_identity_missing_ts else ""
                    ),
                })
            thread_alive = bool(self._thread and self._thread.is_alive())
            return {
                "running_flag": bool(self._running),
                "thread_alive": thread_alive,
                "thread_name": self._thread.name if self._thread else "",
                "position_count": len(positions),
                "stale_inflight_count": len(stale_inflight),
                "stale_inflight": stale_inflight,
                "stale_quote_count": len(stale_quotes),
                "stale_quotes": stale_quotes,
                "stale_underlying_quote_count": len(stale_underlying_quotes),
                "stale_underlying_quotes": stale_underlying_quotes,
                "stale_option_quote_count": len(stale_option_quotes),
                "stale_option_quotes": stale_option_quotes,
                "missing_callback_identity_count": len(missing_callback_identity),
                "missing_callback_identity": missing_callback_identity,
                "positions": positions,
            }

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
        ticker = str(pos.ticker or "")
        option_symbol = str(pos.option_symbol or "")
        position_id = str(pos.position_id or "")

        # 1) Short critical section: validate and capture submit snapshot.
        with self._lock:
            if not self._can_submit_exit(
                pos,
                now_utc,
                reason=decision.reason,
                allow_inflight_override=allow_inflight_override,
            ):
                return False

            decision.reason_code = _classify_exit_decision(decision)

            option_quote_stale, option_quote_age_sec, option_quote_state = _is_option_quote_stale(pos, now_utc)
            if option_quote_stale and not _is_forced_risk_exit_code(decision.reason_code):
                self._emit_exit_event(
                    pos,
                    decision="REJECT",
                    reason_code="OPTION_QUOTE_STALE_BLOCK",
                    explanation=(
                        f"Blocked {decision.reason_code} because option quote is not fresh: "
                        f"{option_quote_state} age={option_quote_age_sec}"
                    ),
                    stage="exit_submission",
                    extra_inputs={
                        "decision_action": decision.action,
                        "decision_qty": decision.quantity,
                        "decision_reason_code": decision.reason_code,
                        "option_quote_state": option_quote_state,
                        "option_quote_age_sec": option_quote_age_sec,
                        "last_option_quote_update_ts": (
                            pos.last_option_quote_update_ts.isoformat()
                            if getattr(pos, "last_option_quote_update_ts", None) else ""
                        ),
                    },
                )
                log.error(
                    "[%s] EXIT BLOCKED: stale option quote | code=%s age=%s state=%s pos_id=%s",
                    ticker, decision.reason_code, option_quote_age_sec, option_quote_state, pos.position_id or "?"
                )
                return False

            if option_quote_stale and _is_forced_risk_exit_code(decision.reason_code):
                self._emit_exit_event(
                    pos,
                    decision="ALERT",
                    reason_code="FORCED_EXIT_DEGRADED_OPTION_QUOTE",
                    explanation=(
                        f"Forced-risk exit allowed despite stale/missing option quote: "
                        f"{decision.reason_code} {option_quote_state} age={option_quote_age_sec}"
                    ),
                    stage="exit_submission",
                    extra_inputs={
                        "decision_action": decision.action,
                        "decision_qty": decision.quantity,
                        "decision_reason_code": decision.reason_code,
                        "option_quote_state": option_quote_state,
                        "option_quote_age_sec": option_quote_age_sec,
                    },
                )
                log.warning(
                    "[%s] FORCED EXIT IN DEGRADED QUOTE MODE | code=%s age=%s state=%s pos_id=%s",
                    ticker, decision.reason_code, option_quote_age_sec, option_quote_state, pos.position_id or "?"
                )

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
        callback_result = None
        callback_identity = {"accepted": True, "local_order_id": "", "broker_order_id": "", "raw_status": ""}
        try:
            if decision.action == "SCALE_OUT":
                if not self.on_scale:
                    log.warning("[%s] Scale-out decision generated but no on_scale callback installed", ticker)
                    return False
                callback_result = self.on_scale(pos, decision)
            else:
                if not self.on_exit:
                    log.warning("[%s] Exit decision generated but no on_exit callback installed", ticker)
                    return False
                callback_result = self.on_exit(pos, decision)

            callback_identity = self._extract_exit_order_identity(callback_result)
            if not callback_identity.get("accepted", True):
                self._emit_exit_event(
                    pos,
                    decision="ERROR",
                    reason_code="EXIT_CALLBACK_NOT_ACCEPTED",
                    explanation=f"Exit callback returned non-accepted status: {callback_identity.get('raw_status', '')}",
                    stage="exit_submission",
                    extra_inputs={"decision_action": decision.action, "decision_qty": decision.quantity},
                )
                return False
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

            self._mark_exit_submitted(
                current_pos,
                decision,
                local_order_id=callback_identity.get("local_order_id", ""),
                broker_order_id=callback_identity.get("broker_order_id", ""),
            )
            if callback_identity.get("raw_status", "").upper() == "EXIT_SUBMITTED" and callback_identity.get("local_order_id") and not callback_identity.get("broker_order_id"):
                # OSM has an active local EXIT order but broker identity is quarantined.
                # Keep duplicate suppression active and surface as quarantine until reconciler
                # backfills broker id, confirms fill, terminal-clears, or marks replacement safe.
                current_pos.last_callback_identity_missing = True
                current_pos.last_callback_identity_missing_ts = datetime.now(timezone.utc)
                current_pos.exit_identity_quarantine = True
                current_pos.exit_identity_quarantine_alert_count = 0
                current_pos.last_exit_identity_quarantine_alert_ts = None
                self._emit_exit_event(
                    current_pos,
                    decision="ALERT",
                    reason_code="EXIT_SUBMITTED_BROKER_ID_QUARANTINE",
                    explanation="OSM parked exit as submitted without broker_order_id; reconciler must resolve identity.",
                    stage="exit_submission",
                    extra_inputs={
                        "pending_exit_local_order_id": current_pos.pending_exit_local_order_id,
                        "pending_exit_broker_order_id": current_pos.pending_exit_broker_order_id,
                    },
                )
            if not current_pos.pending_exit_local_order_id and not current_pos.pending_exit_broker_order_id:
                current_pos.last_callback_identity_missing = True
                current_pos.last_callback_identity_missing_ts = datetime.now(timezone.utc)
                current_pos.exit_identity_quarantine = True
                current_pos.exit_identity_quarantine_alert_count = 0
                current_pos.last_exit_identity_quarantine_alert_ts = None
                log.error(
                    "[%s] EXIT SUBMITTED WITHOUT ORDER ID | pos_id=%s sym=%s action=%s qty=%s status=%s",
                    ticker,
                    current_pos.position_id or "?",
                    current_pos.option_symbol or option_symbol,
                    decision.action,
                    decision.quantity,
                    callback_identity.get("raw_status", ""),
                )
                self._emit_exit_event(
                    current_pos,
                    decision="ALERT",
                    reason_code="EXIT_SUBMITTED_WITHOUT_ORDER_ID",
                    explanation=(
                        "Exit callback accepted but returned no local/broker order identity; "
                        "reconciler must still write identity/fill truth back."
                    ),
                    stage="exit_submission",
                    extra_inputs={
                        "decision_action": decision.action,
                        "decision_qty": decision.quantity,
                        "callback_status": callback_identity.get("raw_status", ""),
                    },
                )
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
                "local_order_id": callback_identity.get("local_order_id", ""),
                "broker_order_id": callback_identity.get("broker_order_id", ""),
                "callback_status": callback_identity.get("raw_status", ""),
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
            
