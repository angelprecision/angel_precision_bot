# ap_exit_engine.py -- Angel Precision Time-Aware Exit Engine
# =============================================================================
# 0DTE / short-dated option exit protection.
#
# Current constants in this file:
#   - Poll interval: 8 seconds
#   - Profit protect W1: 11:00 AM ET, scale out 50% if option P&L >= +40%
#   - Profit protect W2:  1:00 PM ET, scale out 75% if option P&L >= +25%
#   - Profit protect W3:  2:00 PM ET, close all if option P&L >= +15%
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
#
# Bug-fix history (see inline FIX-N tags):
#   FIX-1  Discord webhook POST moved out of evaluate_exit() and out of the engine
#          lock. evaluate_exit() is now a pure function with no side effects.
#          Runner alert fires in _submit_exit_decision() after successful callback,
#          outside both lock sections. Previously the POST (timeout=3) held
#          self._lock for up to 3 seconds on every profitable runner close.
#   FIX-2  Kill-switch filter unpacked (pos, decision, bool) 3-tuples as 2-tuples,
#          raising ValueError on every iteration when KILL_BLOCKS_NON_PROTECTIVE_EXITS=1.
#          Fixed to unpack as (p, d, _) throughout.
#   FIX-3  Expired contract cleanup iterated and reassigned self._positions without
#          holding self._lock — race with concurrent add_position/fill hooks.
#          Entire expired-contract block now runs inside with self._lock.
#   FIX-4  replacement_proof was unconditionally set True, making all three
#          (not replacement_proof) guard blocks permanently dead code. Variable
#          removed; guards now execute unconditionally so stale-time, equivalent-runner,
#          and non-emergency checks actually run.
#   FIX-5  Inline W1/W2 window comments had wrong times (1:30 PM / 2:30 PM).
#          Corrected to match PROFIT_PROTECT_1_HOUR=11 (11:00 AM) and
#          PROFIT_PROTECT_2_HOUR=13 (1:00 PM).
#   FIX-6  Per-order cumulative fill watermark dict was cleared immediately on
#          full-fill. Late duplicate callbacks (fill monitor + reconciler dual path)
#          saw prev=0 and double-counted the fill. Dict is now preserved after
#          pending order completes and only reset in _mark_exit_submitted() when
#          a new exit order generation begins.
#   FIX-7  Added get_position(position_id) method for O(1) lookup. OSM's
#          _get_exit_engine_position() checks for this method first before
#          falling back to O(n) linear scan.
#   FIX-8  last_rejection_ts standardized from Optional[float] (epoch) to
#          Optional[datetime] (UTC) to match every other timestamp field on
#          ManagedPosition. Callers no longer need to know which fields are float.
#   FIX-9  Healer reference captured once before _exit_loop() while-loop instead
#          of re-importing every 8 seconds.
#   FIX-10 Extra blank line inside def on_exit_failure() signature removed.
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

# ── EXIT DECISION LEDGER — best-effort audit; never blocks live exits ─────────
try:
    from ap.exit_decision_ledger import record_exit_decision as _ledger_record
except Exception:
    _ledger_record = None  # type: ignore[assignment]


def _ledger_exit_decision(pos, decision, *, client_id: str = "") -> None:
    """Non-fatal wrapper — logs every exit decision cycle when position is green."""
    try:
        if _ledger_record is None:
            return
        _ledger_record(
            pos, decision,
            client_id=client_id or str(getattr(pos, "client_id", "") or ""),
            metadata={"event_context": "exit_loop"},
        )
    except Exception:
        pass
# ─────────────────────────────────────────────────────────────────────────────

# ── TIME THRESHOLDS (ET) ──────────────────────────────────────────────────────
PROFIT_PROTECT_1_HOUR = 11   # 11:00 AM -- scale out 50% if +40%
PROFIT_PROTECT_1_MIN  = 0
PROFIT_PROTECT_2_HOUR = 13   #  1:00 PM -- scale out 75% if +25%
PROFIT_PROTECT_2_MIN  = 0
PROFIT_PROTECT_3_HOUR = 14   #  2:00 PM -- exit all if +15%
PROFIT_PROTECT_3_MIN  = 0
EOD_HARD_CLOSE_HOUR   = 15   #  3:50 PM -- EXIT EVERYTHING before close
EOD_HARD_CLOSE_MIN    = 50   # Changed from 3:45 to give more time for fills
POLL_INTERVAL_SEC     = 8    # check every 8 seconds

# Kill switch policy: exits reduce risk, so the engine must never pause
# evaluation under kill switch. By default, all exit actions are allowed.
# Set EXIT_ENGINE_KILL_BLOCKS_NON_PROTECTIVE=1 only if you explicitly want
# kill switch to block non-protective exits while still allowing stops/EOD/theta.
KILL_BLOCKS_NON_PROTECTIVE_EXITS = (
    os.getenv("EXIT_ENGINE_KILL_BLOCKS_NON_PROTECTIVE", "0").strip().lower()
    in {"1", "true", "yes", "on"}
)

# ── P&L THRESHOLDS ────────────────────────────────────────────────────────────
THETA_STOP_LOSS_PCT   = -0.35  # -35% on option → stop
# ── SCALE-OUT LADDER (33/33/runner) ─────────────────────────────────────────
# Strategy: bank gains in thirds, let the runner ride as far as it goes.
# The TRAILING STOP (not a fixed cap) decides when the runner exits.
# No ceiling — if a position goes 87% like last week, the trail catches it there.
# Scale 1: +15% → sell 1/3  (first lock, still have 2/3 running)
# Scale 2: +25% → sell 1/3  (second lock, 1/3 runner remains)
# Runner:  trail exit at TRAIL_DROP_FROM_PEAK below peak — could be 30%, 50%, 87%
SCALE_OUT_1_THRESHOLD = 0.15   # +15% → sell first third
SCALE_OUT_2_THRESHOLD = 0.25   # +25% → sell second third
# NO SCALE_OUT_3 — runner exits via trailing stop only, no fixed ceiling
PROTECT_3_THRESHOLD   = 0.15   # +15% → EOD protection if past 2:00 PM

# ── PROFIT FLOORS — absolute guarantees regardless of trail/QPM state ─────────
# Once peak_pnl_pct crosses these, we never let the position exit below the floor.
# Fires BEFORE trail math — trail is reactive, floors are absolute.
# This is the primary fix for "went +22% then went red" — floor at +25% peak
# means worst case we exit at +10%, not at a loss.
PROFIT_FLOOR = {
    0.15: 0.04,   # touched +15% → minimum exit at +4%  (don't give it all back)
    0.25: 0.10,   # touched +25% → minimum exit at +10%
    0.40: 0.20,   # touched +40% → minimum exit at +20%
    0.60: 0.30,   # touched +60% → minimum exit at +30%
}

# ── IMMEDIATE TAKE-PROFIT (any time, no window gate) ──────────────────────────
# Lowered from 15% to 12% — arms trail sooner, less chance of giving back gains
# if QPM has a gap at peak.
IMMEDIATE_TP_PCT      = 0.12   # +12% → ACTIVATES trailing stop (was 15%)
HARD_STOP_PCT         = -0.33  # -33% → hard stop (gives one recovery breath vs -30%)
PROFIT_LOCK_PCT       = 0.12   # once past 15%, don't fall below +12% (protects a real gain)

# PR-A / BUG-4: Unified minimum-hold floor. Previously read twice via
# os.getenv("MIN_HOLD_MINUTES_BEFORE_SOFT_EXIT", default) with default
# "5" in the soft-loss path and default "3" in the never-green path.
# With the env unset (common in dev / fresh Render deploys) the two
# paths behaved asymmetrically: a 4-minute-old position could be
# never-green-stopped while the soft-loss path would still be holding
# it. Both call sites now read this single constant.
_MIN_HOLD_BEFORE_EXIT_MIN = float(os.getenv("MIN_HOLD_MINUTES_BEFORE_SOFT_EXIT", "5"))

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
    """Best-effort OCC/root extraction before YYMMDD date."""
    import re
    sym = (option_symbol or "").upper().strip()
    m = re.search(r"(\d{6})[CP]", sym)
    if not m:
        return sym[:8]
    return sym[:m.start()].strip()


def _normalize_ticker(ticker: str, option_symbol: str = "") -> str:
    """
    Normalize a stored underlying ticker against an OCC option symbol.

    This prevents the fill-monitor / restart bug where an OCC symbol such as
    META260515C00615000 is accidentally sliced into META26. If the stored
    ticker ends with a digit, it is almost certainly not a valid equity root
    for this bot's universe, so the root is recovered from the contract.
    """
    import re as _re

    t = str(ticker or "").strip().upper()
    root = _option_root(option_symbol or "").strip().upper()

    if not t and root:
        return root

    if t and _re.search(r"\d$", t) and root and not _re.search(r"\d$", root):
        log.warning(
            "TICKER NORMALIZED: '%s' -> '%s' from contract '%s'",
            t, root, option_symbol or "",
        )
        return root

    return t


def _option_profile(pos: "ManagedPosition") -> tuple[int, bool, str]:
    symbol = (pos.option_symbol or "").upper()
    ticker = (pos.ticker or "").upper()
    root   = _option_root(symbol)
    dte    = _option_dte(symbol)
    index_roots = _INDEX_ETFS | {"SPXW", "NDX", "NDXP", "RUT", "RUTW"}
    is_index = root in index_roots or ticker in index_roots or any(root.startswith(t) for t in _INDEX_ETFS)
    profile  = "0DTE-idx" if (dte == 0 and is_index) else "0DTE-eq" if dte == 0 else f"{dte}DTE"
    return dte, is_index, profile


def _effective_thresholds(pos: "ManagedPosition") -> tuple:
    """Returns (hard_stop, immediate_tp, profit_lock) adjusted for DTE and instrument."""
    dte, is_index, _ = _option_profile(pos)
    if dte == 0 and is_index:
        return -0.18, 0.20, 0.08
    if dte == 0:
        return -0.22, 0.22, 0.10
    if dte <= 2:
        return -0.26, 0.25, 0.12
    return HARD_STOP_PCT, IMMEDIATE_TP_PCT, PROFIT_LOCK_PCT

# TRAILING STOP — fires when position drops N points from its peak
# Wide enough to let winners run to 25-30%, tight enough to protect gains
TRAIL_DROP_FROM_PEAK  = 0.10   # 10pt drop from peak fires exit (e.g. +30% → exits at +20%)
SMALL_WIN_PCT         = 0.12   # trail kicks in once we've seen +12%
SMALL_WIN_TRAIL       = 0.08   # floor 8pt below peak (seen +20% → floor at +12%)


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
    peak_pnl_pct:         float = 0.0
    touched_profit:       bool  = False
    # FIX-8: standardized to Optional[datetime] (was Optional[float] / epoch seconds).
    # Previously set via time.time(); all other timestamp fields are Optional[datetime].
    last_rejection_ts:    Optional[datetime] = None
    last_exit_rejected:  bool = False
    _exit_stuck_count:   int = 0
    max_profit_seen:      float = 0.0
    closed:               bool  = False
    close_reason:         str   = ""
    opened_at:            datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # PR-A / BUG-1: Stop-breach confirmation timestamps. Previously
    # written via `pos._stop_breach_ts = time.time()` with
    # `# type: ignore[attr-defined]` — i.e. ghost attributes that were
    # not declared on the dataclass and would be dropped by any
    # dataclasses.replace() / asdict() roundtrip. Now declared
    # as Optional[datetime] to match every other timestamp field
    # (consistent with FIX-8 applied earlier to last_rejection_ts).
    _stop_breach_ts:              Optional[datetime] = None
    _underlying_stop_breach_ts:   Optional[datetime] = None

    # PR-B: Execution-core ghost fields. Previously assigned dynamically
    # in ap_execution_core.py via `# type: ignore[attr-defined]`:
    #   pos._exit_submit_ts  / pos._exit_attempts — drive step-down ladder
    #   pos._integrity_logged — once-per-position integrity log guard
    #   pos._proof_staged — staged proof dict, finalized on broker fill
    #   pos._proof_finalized / pos.proof_logged — idempotency guards
    # Declared here so they survive dataclasses.replace() and any future
    # asdict()/from-dict reconstruction (seed_from_db, etc).
    # TODO(post-proof-week): consolidate proof_logged and _proof_finalized
    # into a single proof_state field once the two guards' semantics are
    # auditable post-live.
    _exit_submit_ts:    float                  = 0.0
    _exit_attempts:     int                    = 0
    _integrity_logged:  bool                   = False
    _proof_staged:      Optional[dict]         = None
    _proof_finalized:   bool                   = False
    proof_logged:       bool                   = False

    # Exit coordination
    exit_in_flight:       bool  = False
    pending_exit_reason:  str   = ""
    pending_exit_action:  str   = ""
    pending_exit_qty:     int   = 0
    pending_exit_filled_qty: int = 0
    pending_scale_counted: bool = False
    pending_exit_local_order_id: str = ""
    pending_exit_broker_order_id: str = ""
    last_applied_exit_local_order_id: str = ""
    last_applied_exit_broker_order_id: str = ""
    last_applied_exit_cum_fill: int = 0
    last_applied_exit_cum_fill_by_order: dict[str, int] = field(default_factory=dict)
    last_exit_signal_ts:  Optional[datetime] = None
    last_callback_identity_missing: bool = False
    last_callback_identity_missing_ts: Optional[datetime] = None
    exit_identity_quarantine: bool = False
    pending_exit_replace_allowed: bool = False
    pending_exit_replace_reason: str = ""
    pending_exit_replace_allowed_ts: Optional[datetime] = None
    exit_identity_quarantine_alert_count: int = 0
    last_exit_identity_quarantine_alert_ts: Optional[datetime] = None
    last_exit_clear_reason: str = ""
    last_exit_clear_local_order_id: str = ""
    last_exit_clear_broker_order_id: str = ""
    last_exit_identity_quarantine_resolved_ts: Optional[datetime] = None
    last_exit_identity_reject_ts: Optional[datetime] = None

    # P2: Submit generation token. Monotonically incremented by _mark_exit_submitted()
    # each time a new exit order generation begins. _submit_exit_decision() captures
    # this before releasing the pre-submit lock, then verifies it matches in the
    # post-callback lock. A mismatch means another path (fill, reconciler, OSM) has
    # already advanced or closed this position while the callback was executing.
    _submit_generation: int = 0

    # Quote-health fields
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
        self.quantity_remaining = (
            self.quantity
            if int(self.quantity_remaining or 0) <= 0
            else int(self.quantity_remaining)
        )

    @property
    def option_pnl_pct(self) -> float:
        if self.entry_price <= 0 or self.current_option_price <= 0:
            return 0.0
        return (self.current_option_price - self.entry_price) / self.entry_price

    @property
    def underlying_pnl_pct(self) -> float:
        if self.underlying_entry <= 0 or self.current_underlying <= 0:
            return 0.0
        if self.side == "CALL":
            return (self.current_underlying - self.underlying_entry) / self.underlying_entry
        return (self.underlying_entry - self.current_underlying) / self.underlying_entry

    @property
    def is_at_target(self) -> bool:
        if self.underlying_target <= 0:
            return False
        if self.side == "CALL":
            return self.current_underlying >= self.underlying_target
        return self.current_underlying <= self.underlying_target

    @property
    def is_at_stop(self) -> bool:
        if self.underlying_stop <= 0:
            return False
        if self.side == "CALL":
            return self.current_underlying <= self.underlying_stop
        return self.current_underlying >= self.underlying_stop


# ── EXIT DECISION ─────────────────────────────────────────────────────────────

@dataclass
class ExitDecision:
    action:          str    # "HOLD", "SCALE_OUT", "CLOSE_ALL", "STOP"
    quantity:        int    # contracts to close (0 = hold)
    reason:          str
    urgency:         str    # "NORMAL", "HIGH", "IMMEDIATE"
    pnl_pct:         float = 0.0
    suggested_limit: float = 0.0
    reason_code:     str   = ""
    # FIX-1: runner alert metadata carried by the decision so the
    # pure function can signal intent without making a network call.
    # _submit_exit_decision() reads this and fires Discord after lock release.
    _runner_alert_peak_pct: float = 0.0
    _runner_trail_used:     float = 0.0
    _runner_duration_min:   int   = 0

    @property
    def should_act(self) -> bool:
        return self.action != "HOLD"


# ── EXIT LOGIC ────────────────────────────────────────────────────────────────

def _underlying_still_confirming(pos: ManagedPosition) -> tuple[bool, str]:
    """
    Check if the underlying is still moving in our direction.
    Used to give positions breathing room when option P&L dips but
    the trade thesis (underlying momentum) is still intact.

    Returns (confirming: bool, reason: str)
    """
    entry_u = getattr(pos, "underlying_entry", 0) or 0
    curr_u  = getattr(pos, "current_underlying", 0) or 0
    target_u = getattr(pos, "underlying_target", 0) or 0
    stop_u  = getattr(pos, "underlying_stop", 0) or 0
    side    = (getattr(pos, "side", "") or "").upper()

    if not entry_u or not curr_u:
        return False, "no_underlying_data"

    move_pct = (curr_u - entry_u) / entry_u  # positive = underlying went up

    if side == "CALL":
        confirming = move_pct > -0.005  # underlying hasn't dropped >0.5% from entry
        reason = f"underlying_move={move_pct*100:+.2f}%_from_entry"
        # Also check: are we still above entry? If yes, thesis alive
        if curr_u >= entry_u * 0.995:
            return True, f"call_underlying_holding_above_entry_{reason}"
        return False, f"call_underlying_broke_entry_{reason}"

    elif side == "PUT":
        confirming = move_pct < 0.005  # underlying hasn't risen >0.5% from entry
        reason = f"underlying_move={move_pct*100:+.2f}%_from_entry"
        if curr_u <= entry_u * 1.005:
            return True, f"put_underlying_holding_below_entry_{reason}"
        return False, f"put_underlying_broke_entry_{reason}"

    return False, "unknown_side"


def _position_age_minutes(pos: ManagedPosition) -> float:
    """Return how many minutes old the position is."""
    try:
        if pos.opened_at:
            return (datetime.now(timezone.utc) - pos.opened_at).total_seconds() / 60
    except Exception:
        pass
    return 999.0  # unknown age — do not block exits


def evaluate_exit(pos: ManagedPosition, now_et: Optional[datetime] = None) -> ExitDecision:
    """
    Core exit evaluation. Called every POLL_INTERVAL_SEC for each position.
    Returns ExitDecision.

    FIX-1: This is now a pure function with no side effects. The Discord runner
    alert that previously lived here (blocking requests.post under self._lock)
    has been moved to _submit_exit_decision(), which fires it after the
    callback returns, outside both lock sections.
    """
    if now_et is None:
        now_et = datetime.now(ET)
    _hard_stop, _immediate_tp, _profit_lock = _effective_thresholds(pos)

    hour, minute = now_et.hour, now_et.minute
    option_pnl   = pos.option_pnl_pct
    qty_rem      = pos.quantity_remaining

    # ── 1. TARGET HIT ────────────────────────────────────────────────────────
    if pos.is_at_target:
        return ExitDecision(
            action="CLOSE_ALL", quantity=qty_rem,
            reason=f"TARGET HIT -- underlying ${pos.current_underlying:.2f} reached ${pos.underlying_target:.2f}",
            urgency="IMMEDIATE", pnl_pct=option_pnl,
        )

    # ── 2. STOP HIT ──────────────────────────────────────────────────────────
    # Require 30s confirmation before exiting on underlying stop breach.
    # A single candle wick that immediately recovers should NOT trigger exit.
    # Uses HIGH urgency (bid-limit) not IMMEDIATE (market).
    if pos.is_at_stop:
        # PR-A / BUG-2: stamp is datetime now (was time.time() float).
        _now_dt = datetime.now(timezone.utc)
        _stop_dt = pos._underlying_stop_breach_ts
        _UNDERLYING_CONFIRM_SEC = float(os.getenv("UNDERLYING_STOP_CONFIRM_SECONDS", "30"))

        if _stop_dt is None:
            pos._underlying_stop_breach_ts = _now_dt
            log.info(
                "[%s] UNDERLYING_STOP_BREACH_STARTED — $%.2f at stop $%.2f "
                "| will exit if holds >%.0fs",
                pos.ticker, pos.current_underlying,
                pos.underlying_stop, _UNDERLYING_CONFIRM_SEC,
            )
        else:
            _breach_age_sec = (_now_dt - _stop_dt).total_seconds()
            if _breach_age_sec >= _UNDERLYING_CONFIRM_SEC:
                # Breach confirmed — exit with bid-limit
                pos._underlying_stop_breach_ts = None
                return ExitDecision(
                    action="STOP", quantity=qty_rem,
                    reason=(
                        f"STOP HIT — underlying ${pos.current_underlying:.2f} "
                        f"held below stop ${pos.underlying_stop:.2f} "
                        f"for {_breach_age_sec:.0f}s"
                    ),
                    urgency="HIGH", pnl_pct=option_pnl,
                )
            else:
                log.info(
                    "[%s] UNDERLYING_STOP_CONFIRMING — $%.2f below stop $%.2f "
                    "| breach=%.0fs/%.0fs",
                    pos.ticker, pos.current_underlying,
                    pos.underlying_stop, _breach_age_sec, _UNDERLYING_CONFIRM_SEC,
                )
    else:
        # Underlying recovered above stop — reset confirmation timer
        if pos._underlying_stop_breach_ts is not None:
            pos._underlying_stop_breach_ts = None
            log.info("[%s] UNDERLYING_STOP_RECOVERED — price reclaimed stop level", pos.ticker)

    # ── TOUCHED PROFIT PROTECTION ─────────────────────────────────────────────
    # Once green, we LOCK IN a minimum profit. Never let a green trade
    # become a loss. Average winner target = 25%.
    #
    # Floor logic (option_pnl must stay ABOVE floor or we exit):
    #   peak >= 25%  → floor +12%  (never give back more than 13pts of a big winner)
    #   peak >= 15%  → floor +8%   (protect 8% minimum from a 15%+ trade)
    #   peak >= 10%  → floor +5%   (protect 5% minimum from a 10%+ trade)
    #   peak >= 5%   → floor +3%   (MINIMUM green — never turn a 5%+ win into a loss)
    #   any green    → floor  0%   (breakeven floor — touched green = never go red)
    if pos.scale_outs_done == 0 and pos.touched_profit:
        _max = pos.max_profit_seen or 0
        if _max >= 0.25:
            _floor = 0.12   # secured 12% minimum
        elif _max >= 0.15:
            _floor = 0.08   # secured 8% minimum
        elif _max >= 0.10:
            _floor = 0.05   # secured 5% minimum
        elif _max >= 0.05:
            _floor = 0.03   # secured 3% minimum — goal is 25% avg winner
        else:
            _floor = 0.00   # any green: floor at breakeven

        if option_pnl <= _floor:
            # Before firing: check if underlying still confirming.
            # A single penny drop on a cheap contract (-9%) is not a real signal
            # if the underlying is still moving in our direction.
            # The thesis check applies regardless of age — if underlying
            # is still holding, we do not punish the option for spread noise.
            _age_min = _position_age_minutes(pos)
            _confirming, _confirm_reason = _underlying_still_confirming(pos)
            _MAX_THESIS_OVERRIDE = float(
                os.getenv("MAX_HOLD_MINUTES_WHILE_RED", "5")
            )
            if _confirming and _age_min < _MAX_THESIS_OVERRIDE:
                # Underlying still in our direction — hold, don't exit on noise
                log.info(
                    "[%s] TOUCHED_PROFIT_STOP suppressed — underlying still confirming "
                    "(%s) | age=%.1fmin < %.0fmin thesis window | pnl=%.1f%%",
                    pos.ticker, _confirm_reason, _age_min,
                    _MAX_THESIS_OVERRIDE, option_pnl * 100,
                )
            else:
                return ExitDecision(
                    action="CLOSE_ALL", quantity=qty_rem,
                    reason=(
                        f"TOUCHED PROFIT STOP — peaked +{_max*100:.0f}% "
                        f"now {option_pnl*100:.0f}% — floor={_floor*100:.0f}% | "
                        f"underlying={'confirming' if _confirming else 'not confirming'}"
                    ),
                    urgency="IMMEDIATE", pnl_pct=option_pnl,
                )

    # ── 33/33/34 SCALE-OUT LADDER ────────────────────────────────────────────
    # Scale-out ladder:
    # +15% (SCALE_OUT_1_THRESHOLD) → sell first third  — lock base gain
    # +25% (SCALE_OUT_2_THRESHOLD) → sell second third — lock extended gain
    # Runner: trails with no fixed ceiling — let winners run
    # Do NOT close everything at 15% — let winners run to 25-30%+ with trail.
    # Single-contract positions: hold until trail fires or 30%+ hit.

    # Scale 1: first +10% hit → sell first third
    if option_pnl >= SCALE_OUT_1_THRESHOLD and pos.scale_outs_done == 0:
        if qty_rem <= 1:
            # 1 contract — do NOT sell here, let it run to trail
            pass  # fall through to trail/profit-lock checks
        else:
            qty_s1 = max(1, round(qty_rem / 3))
            return ExitDecision(
                action="SCALE_OUT", quantity=qty_s1,
                reason=f"SCALE_1 (+15%) -- selling {qty_s1}/{qty_rem} | running {qty_rem-qty_s1} to +25%",
                urgency="HIGH", pnl_pct=option_pnl,
            )

    # Scale 2: +20% hit → sell second third
    if option_pnl >= SCALE_OUT_2_THRESHOLD and pos.scale_outs_done == 1:
        if qty_rem <= 1:
            pass  # 1 contract runner — let it run to +30% or trail
        else:
            qty_s2 = max(1, round(qty_rem / 2))  # half of what's left ≈ second third of original
            return ExitDecision(
                action="SCALE_OUT", quantity=qty_s2,
                reason=f"SCALE_2 (+25%) -- selling {qty_s2}/{qty_rem} runner | targeting +40%",
                urgency="HIGH", pnl_pct=option_pnl,
            )

    # Scale 3: +30% → close remainder (full exit for final runner)
    # Runner (scale_outs_done >= 2): exits via trail only — no fixed ceiling
    # Could reach 30%, 50%, 87% — trail catches it wherever it peaks

    # For single contracts or runners at +30%+: close at +30%
    # Single contract: no fixed ceiling — trail exit only
    # Let it go to 87%+ if the momentum is there

    # ── RUNNER TRAIL ──────────────────────────────────────────────────────────
    # FIX-1: Discord runner alert moved to _submit_exit_decision(). This branch
    # now returns the alert metadata on the decision object instead of calling
    # requests.post() here under self._lock.
    _single_contract = (qty_rem == 1 and pos.scale_outs_done == 0)
    if (pos.scale_outs_done >= 1 or _single_contract) and pos.peak_pnl_pct >= IMMEDIATE_TP_PCT:

        # ── PROFIT FLOOR CHECK — fires before trail math ──────────────────────
        # If peak crossed a floor threshold and current P&L is below that floor,
        # sell immediately. This protects against QPM gaps missing the peak.
        # Example: peaked at +25% (floor=10%), now at +7% → EXIT at +7%.
        _applicable_floor = 0.0
        for _floor_trigger, _floor_min in sorted(PROFIT_FLOOR.items(), reverse=True):
            if pos.peak_pnl_pct >= _floor_trigger:
                _applicable_floor = _floor_min
                break
        if _applicable_floor > 0 and 0 < option_pnl < _applicable_floor:
            return ExitDecision(
                action="CLOSE_ALL", quantity=qty_rem,
                reason=(
                    f"PROFIT LOCK — peaked +{pos.peak_pnl_pct*100:.0f}%, "
                    f"now +{option_pnl*100:.0f}% below floor +{_applicable_floor*100:.0f}%"
                ),
                urgency="IMMEDIATE",
                pnl_pct=option_pnl,
                reason_code="PROFIT_LOCK",
            )
        # ─────────────────────────────────────────────────────────────────────

        # Tightened trail thresholds — was 12-20%, now 8-15%
        # Tighter trail = less giveback on winners, especially for single contracts
        if pos.peak_pnl_pct >= 0.80:
            _runner_trail = 0.08   # gave back 8% from 80%+ peak → sell
        elif pos.peak_pnl_pct >= 0.60:
            _runner_trail = 0.10
        elif pos.peak_pnl_pct >= 0.40:
            _runner_trail = 0.12
        elif _single_contract:
            _runner_trail = 0.08   # single contracts: tight 8% trail (was 12%)
        else:
            _runner_trail = 0.15   # multi-contract runner: 15% trail (was 20%)
        runner_drop = pos.peak_pnl_pct - option_pnl
        if runner_drop >= _runner_trail or option_pnl <= 0:
            _dur_min = 0
            try:
                _opened_ts = pos.opened_at.timestamp() if hasattr(pos.opened_at, "timestamp") else time.time()
                _dur_min   = int((time.time() - _opened_ts) / 60)
            except Exception:
                pass
            d = ExitDecision(
                action="CLOSE_ALL", quantity=qty_rem,
                reason=(
                    f"RUNNER TRAIL EXIT -- peaked +{pos.peak_pnl_pct*100:.0f}%, "
                    f"now +{option_pnl*100:.0f}%, protecting runner gains"
                ),
                urgency="HIGH", pnl_pct=option_pnl,
            )
            # FIX-1: carry alert metadata so _submit_exit_decision() can fire
            # the Discord notification after callback returns (outside lock).
            if pos.peak_pnl_pct >= 0.50:
                d._runner_alert_peak_pct = pos.peak_pnl_pct
                d._runner_trail_used     = _runner_trail
                d._runner_duration_min   = _dur_min
            return d

    # ── SMALL WIN CAPTURE ─────────────────────────────────────────────────────
    if pos.max_profit_seen >= SMALL_WIN_PCT:
        floor = max(0.03, pos.max_profit_seen - SMALL_WIN_TRAIL)
        if 0 < option_pnl <= floor:
            return ExitDecision(
                action="CLOSE_ALL", quantity=qty_rem,
                reason=(
                    f"SMALL WIN LOCK — peaked +{pos.max_profit_seen*100:.0f}%, "
                    f"protecting +{option_pnl*100:.0f}%"
                ),
                urgency="HIGH", pnl_pct=option_pnl,
            )

    # Underlying progress: if 60%+ toward scanner target, log but do NOT close.
    # Closing the entire position at +5% because the underlying made partial
    # progress is exactly how we miss +50%/+100% runners.
    # Instead: log as a diagnostic signal only. Exits happen via trail/scale.
    _entry_u  = pos.underlying_entry
    _target_u = pos.underlying_target
    _curr_u   = pos.current_underlying
    if _entry_u and _target_u and _curr_u:
        _range = abs(_target_u - _entry_u)
        if _range > 0:
            _progress = abs(_curr_u - _entry_u) / _range
            if _progress >= 0.60 and option_pnl >= 0.05:
                # Log only — DO NOT exit. Trail/scale-out handles actual exit.
                log.debug(
                    "[%s] UNDERLYING_PROGRESS_INFO — %.0f%% toward target, "
                    "option +%.1f%% — holding for trail/scale",
                    pos.ticker, _progress * 100, option_pnl * 100,
                )

    # ── SOFT LOSS STOP — two-tier, thesis-aware ──────────────────────────────
    # -33% = hard stop emergency airbag (unchanged)
    # -12% = normal soft stop (underlying flat or holding)
    # -20% = extended room ONLY when underlying actively moving our direction
    #
    # This handles "trades that go -20% before popping" — the underlying
    # move is the real signal. The option price is noise on cheap contracts.
    #
    # Tier A: underlying moved 0.5%+ in our direction → breathe to -20%
    # Tier B: underlying flat/holding within 0.5% of entry → soft stop -12%
    # Tier C: underlying broke against us → exit immediately
    # Always: past -20% → exit regardless of confirmation
    _SOFT_LOSS_PCT      = float(os.getenv("SOFT_LOSS_STOP_PCT",           "-0.12"))
    _SOFT_LOSS_DEEP_PCT = float(os.getenv("SOFT_LOSS_STOP_DEEP_PCT",      "-0.20"))
    # Minimum minutes before any soft stop can fire — give the thesis time to develop
    # PR-A / BUG-4: read from unified module-level constant; both call
    # sites (this one and the never-green path) now share the same floor.
    _MIN_HOLD_SOFT      = _MIN_HOLD_BEFORE_EXIT_MIN
    # Stop confirmation window: breach must hold this many seconds before exit fires
    # Prevents exiting on intraday wicks that immediately recover
    _STOP_CONFIRM_SEC   = float(os.getenv("STOP_BREACH_CONFIRM_SECONDS", "45"))

    if option_pnl <= _SOFT_LOSS_PCT and not pos.touched_profit:
        _soft_age       = _position_age_minutes(pos)
        _soft_confirm, _soft_reason = _underlying_still_confirming(pos)

        # Measure how strongly the underlying is moving in our direction
        _entry_u = getattr(pos, "underlying_entry", 0) or 0
        _curr_u  = getattr(pos, "current_underlying", 0) or 0
        _side    = (getattr(pos, "side", "") or "").upper()
        _u_move  = 0.0
        if _entry_u > 0 and _curr_u > 0:
            raw_move = (_curr_u - _entry_u) / _entry_u
            _u_move  = raw_move if _side == "CALL" else -raw_move
        _strong_confirm = _soft_confirm and _u_move >= 0.005  # 0.5%+ move our way

        # Stop confirmation: track when this stop level was first breached
        # If stop just breached (< STOP_CONFIRM_SECONDS ago), give it time to recover
        # PR-A / BUG-2: stamp is datetime now (was time.time() float).
        _now_dt = datetime.now(timezone.utc)
        _breach_dt = pos._stop_breach_ts
        if _breach_dt is None:
            # First time we see this breach — stamp it, don't exit yet
            pos._stop_breach_ts = _now_dt
            log.info(
                "[%s] STOP_BREACH_STARTED — %.1f%% loss | confirming=%s | "
                "will exit if breach holds >%.0fs | age=%.1fmin",
                pos.ticker, option_pnl * 100, _soft_confirm, _STOP_CONFIRM_SEC, _soft_age,
            )
            return ExitDecision(
                action="HOLD", quantity=0,
                reason=f"STOP_BREACH_STARTED — {option_pnl*100:.1f}% loss | breach stamped, waiting for confirmation window",
                urgency="NORMAL", pnl_pct=option_pnl, reason_code="STOP_BREACH_STARTED",
            )

        _breach_age_sec = (_now_dt - _breach_dt).total_seconds()

        # If breach lasted < confirmation window AND underlying is recovering → reset
        if _breach_age_sec < _STOP_CONFIRM_SEC:
            if _strong_confirm:
                # Underlying moving our way — this looks like a wick, not a real break
                pos._stop_breach_ts = None
                log.info(
                    "[%s] STOP_BREACH_RESET — underlying recovered (%.2f%% move) "
                    "| option=%.1f%% | breach lasted %.0fs < %.0fs confirm window",
                    pos.ticker, _u_move * 100, option_pnl * 100,
                    _breach_age_sec, _STOP_CONFIRM_SEC,
                )
                return ExitDecision(
                    action="HOLD", quantity=0,
                    reason=f"STOP_BREACH_RESET — underlying recovered {_u_move*100:.2f}%, wick not confirmed",
                    urgency="NORMAL", pnl_pct=option_pnl, reason_code="STOP_BREACH_RESET",
                )
            # Still in confirmation window — log and wait
            log.info(
                "[%s] STOP_BREACH_CONFIRMING — %.1f%% loss | breach=%.0fs/%.0fs | "
                "underlying=%s",
                pos.ticker, option_pnl * 100, _breach_age_sec, _STOP_CONFIRM_SEC,
                _soft_reason,
            )
            return ExitDecision(
                action="HOLD", quantity=0,
                reason=f"STOP_BREACH_CONFIRMING — {option_pnl*100:.1f}% loss | {_breach_age_sec:.0f}s/{_STOP_CONFIRM_SEC:.0f}s window | {_soft_reason}",
                urgency="NORMAL", pnl_pct=option_pnl, reason_code="STOP_BREACH_CONFIRMING",
            )

        # ── Breach confirmed (held past confirmation window) ──────────────────
        # Now evaluate whether to exit

        # Past -20% with confirmed breach → exit, bid-limit
        if option_pnl <= _SOFT_LOSS_DEEP_PCT:
            pos._stop_breach_ts = None
            return ExitDecision(
                action="CLOSE_ALL", quantity=qty_rem,
                reason=(
                    f"DEEP_LOSS_STOP — {option_pnl*100:.0f}% exceeds deep floor "
                    f"{_SOFT_LOSS_DEEP_PCT*100:.0f}% | confirmed {_breach_age_sec:.0f}s | "
                    f"underlying={_soft_reason}"
                ),
                urgency="HIGH", pnl_pct=option_pnl,
            )

        # Young position with confirming underlying → suppress, give more time
        if _soft_age < _MIN_HOLD_SOFT and (_soft_confirm or _strong_confirm):
            log.info(
                "[%s] SOFT_STOP_SUPPRESSED — %.1f%% loss but age=%.1fmin < %.0fmin "
                "hold floor | underlying=%s | giving thesis time to develop",
                pos.ticker, option_pnl * 100, _soft_age, _MIN_HOLD_SOFT, _soft_reason,
            )
            return ExitDecision(
                action="HOLD", quantity=0,
                reason=f"SOFT_STOP_SUPPRESSED — age {_soft_age:.1f}min < {_MIN_HOLD_SOFT:.0f}min floor, underlying confirming",
                urgency="NORMAL", pnl_pct=option_pnl, reason_code="SOFT_STOP_SUPPRESSED",
            )

        if not _soft_confirm:
            # Thesis confirmed broken — underlying not holding, breach confirmed
            pos._stop_breach_ts = None
            return ExitDecision(
                action="CLOSE_ALL", quantity=qty_rem,
                reason=(
                    f"THESIS_FAIL_SOFT_STOP — {option_pnl*100:.0f}% loss "
                    f"and underlying not confirming ({_soft_reason}) | "
                    f"age={_soft_age:.1f}min | confirmed {_breach_age_sec:.0f}s"
                ),
                urgency="HIGH", pnl_pct=option_pnl,
            )

        # Thesis still valid — watch, don't exit on time alone
        log.warning(
            "[%s] SOFT_LOSS_WATCH — %.1f%% loss | underlying=%s | "
            "age=%.1fmin | breach confirmed %.0fs | waiting for thesis to break",
            pos.ticker, option_pnl * 100, _soft_reason,
            _soft_age, _breach_age_sec,
        )
        return ExitDecision(
            action="HOLD", quantity=0,
            reason=f"SOFT_LOSS_WATCH — {option_pnl*100:.1f}% loss | thesis holding, underlying={_soft_reason}",
            urgency="NORMAL", pnl_pct=option_pnl, reason_code="SOFT_LOSS_WATCH",
        )

    # ── NEVER-GREEN ESCALATING STOP ───────────────────────────────────────────
    if not pos.touched_profit:
        _age_min = (
            (datetime.now(timezone.utc) - pos.opened_at).total_seconds() / 60
            if pos.opened_at else 0
        )
        _dte_ng, _is_idx_ng, _profile_ng = _option_profile(pos)

        if _dte_ng == 0 and _is_idx_ng:
            if _age_min < 3:    _ng_stop = -0.12
            elif _age_min < 8:  _ng_stop = -0.10
            elif _age_min < 15: _ng_stop = -0.08
            else:               _ng_stop = -0.06
        elif _dte_ng == 0:
            if _age_min < 5:    _ng_stop = -0.15
            elif _age_min < 10: _ng_stop = -0.12
            elif _age_min < 20: _ng_stop = -0.10
            else:               _ng_stop = -0.08
        elif _dte_ng <= 2:
            if _age_min < 10:   _ng_stop = -0.18
            elif _age_min < 20: _ng_stop = -0.15
            elif _age_min < 40: _ng_stop = -0.12
            else:               _ng_stop = -0.10
        else:
            if _age_min < 5:    _ng_stop = -0.15
            elif _age_min < 10: _ng_stop = -0.12
            elif _age_min < 20: _ng_stop = -0.10
            else:               _ng_stop = -0.08

        if option_pnl <= _ng_stop:
            _ng_age_min = _position_age_minutes(pos)
            _ng_confirming, _ng_confirm_reason = _underlying_still_confirming(pos)
            # PR-A / BUG-4: read from unified module-level constant; previously
            # defaulted to 3 here vs 5 in the soft-loss path — asymmetric when
            # the env var was unset. Both call sites now share the same floor.
            _MIN_HOLD_NG = _MIN_HOLD_BEFORE_EXIT_MIN

            # Suppress never-green stop if:
            # 1. Underlying is still confirming (thesis alive), AND
            # 2. Position is young (< MIN_HOLD_MINUTES_BEFORE_SOFT_EXIT)
            # This allows cheap fragile contracts to breathe through
            # the initial spread/noise before the real move develops.
            if _ng_confirming and _ng_age_min < _MIN_HOLD_NG:
                log.info(
                    "[%s] NEVER_GREEN_STOP suppressed — underlying still confirming "
                    "(%s) | age=%.1fmin < %.0fmin hold floor | pnl=%.1f%%",
                    pos.ticker, _ng_confirm_reason, _ng_age_min,
                    _MIN_HOLD_NG, option_pnl * 100,
                )
            else:
                _profile = (
                    "0DTE-idx" if (_dte_ng == 0 and _is_idx_ng)
                    else "0DTE-eq" if _dte_ng == 0
                    else f"{_dte_ng}DTE"
                )
                return ExitDecision(
                    action="CLOSE_ALL", quantity=qty_rem,
                    reason=(
                        f"NEVER GREEN STOP [{_profile}] — {option_pnl*100:.0f}% "
                        f"at {_ng_age_min:.0f}min | threshold={_ng_stop*100:.0f}% | "
                        f"thesis never confirmed | underlying:{_ng_confirm_reason}"
                    ),
                    urgency="HIGH", pnl_pct=option_pnl,
                )

    # ── HARD STOP ─────────────────────────────────────────────────────────────
    if option_pnl <= _hard_stop:
        return ExitDecision(
            action="STOP", quantity=qty_rem,
            reason=f"HARD STOP -- {option_pnl*100:.0f}% exceeded -{abs(_hard_stop)*100:.0f}% max loss",
            urgency="IMMEDIATE", pnl_pct=option_pnl,
        )

    # ── PROFIT LOCK ───────────────────────────────────────────────────────────
    if pos.peak_pnl_pct >= _immediate_tp:
        if option_pnl <= _profit_lock:
            return ExitDecision(
                action="CLOSE_ALL", quantity=qty_rem,
                reason=f"PROFIT LOCK -- peaked at +{pos.peak_pnl_pct*100:.0f}%, fell to +{option_pnl*100:.0f}% — locking in",
                urgency="HIGH", pnl_pct=option_pnl,
            )
        drop_from_peak = pos.peak_pnl_pct - option_pnl
        if drop_from_peak >= TRAIL_DROP_FROM_PEAK:
            return ExitDecision(
                action="CLOSE_ALL", quantity=qty_rem,
                reason=f"TRAILING STOP -- peak +{pos.peak_pnl_pct*100:.0f}%, dropped {drop_from_peak*100:.0f}pts to +{option_pnl*100:.0f}%",
                urgency="HIGH", pnl_pct=option_pnl,
            )

    # ── TREND DAY MULTIPLIERS ─────────────────────────────────────────────────
    direction_aligns = (
        pos.is_trend_day and (
            (pos.side == "CALL" and pos.trend_direction == "uptrend") or
            (pos.side == "PUT"  and pos.trend_direction == "downtrend")
        )
    )
    trend_bonus_threshold = 1.35 if direction_aligns else 1.0
    trend_bonus_window    = 30   if direction_aligns else 0

    # ── 3. EOD HARD CLOSE ────────────────────────────────────────────────────
    # EOD close: fires at 3:50 PM ET OR any time market is closed (stale quotes)
    # The stale-quote check ensures positions don't survive overnight if the
    # exit engine missed the 3:50 window due to quote feed stopping at 4 PM.
    past_eod = (hour > EOD_HARD_CLOSE_HOUR or
                (hour == EOD_HARD_CLOSE_HOUR and minute >= EOD_HARD_CLOSE_MIN))
    # Also force-close if market is clearly closed (hour > 16 ET or < 9:30 ET next day)
    market_clearly_closed = (hour >= 16)
    if past_eod or (market_clearly_closed and not getattr(pos, "overnight_hold_approved", False)):
        return ExitDecision(
            action="CLOSE_ALL", quantity=qty_rem,
            reason=f"EOD FORCE CLOSE -- {hour}:{minute:02d} ET {'(market closed)' if market_clearly_closed else f'past {EOD_HARD_CLOSE_HOUR}:{EOD_HARD_CLOSE_MIN:02d}'}",
            urgency="IMMEDIATE", pnl_pct=option_pnl,
        )

    # ── 4. PROFIT PROTECTION -- WINDOW 3 (2:00 PM+) ──────────────────────────
    past_window3 = (hour > PROFIT_PROTECT_3_HOUR or
                    (hour == PROFIT_PROTECT_3_HOUR and minute >= PROFIT_PROTECT_3_MIN))
    protect3_thresh = 0.50 if direction_aligns else PROTECT_3_THRESHOLD
    if past_window3 and option_pnl >= protect3_thresh:
        trend_note = " [trend day -- raised to 50% threshold]" if direction_aligns else ""
        return ExitDecision(
            action="CLOSE_ALL", quantity=qty_rem,
            reason=f"PROFIT PROTECT W3 -- +{option_pnl*100:.0f}% at 2PM+{trend_note}",
            urgency="HIGH", pnl_pct=option_pnl,
        )

    # ── 5. PROFIT PROTECTION -- WINDOW 2 (1:00 PM+, or 1:30 PM on trend day) ─
    # FIX-5a: comment corrected from "2:30 PM+" to "1:00 PM+" to match
    # PROFIT_PROTECT_2_HOUR = 13 = 1:00 PM. Previous comment was 90 min wrong.
    w2_hour = PROFIT_PROTECT_2_HOUR
    w2_min  = PROFIT_PROTECT_2_MIN + trend_bonus_window
    while w2_min >= 60:
        w2_hour += 1
        w2_min  -= 60
    past_window2 = (hour > w2_hour or (hour == w2_hour and minute >= w2_min))
    scale2_threshold = SCALE_OUT_2_THRESHOLD * trend_bonus_threshold
    if past_window2 and option_pnl >= scale2_threshold and pos.scale_outs_done < 2:
        qty_close  = max(1, round(qty_rem * (0.50 if direction_aligns else 0.75)))
        trend_note = " [TREND DAY -- reduced scale]" if direction_aligns else ""
        return ExitDecision(
            action="SCALE_OUT", quantity=qty_close,
            reason=f"PROFIT PROTECT W2 -- +{option_pnl*100:.0f}% at {w2_hour}:{w2_min:02d}+ scale{trend_note}",
            urgency="HIGH", pnl_pct=option_pnl,
        )

    # ── 6. PROFIT PROTECTION -- WINDOW 1 (11:00 AM+, or 11:30 AM on trend day) ─
    # FIX-5b: comment corrected from "1:30 PM+" to "11:00 AM+" to match
    # PROFIT_PROTECT_1_HOUR = 11 = 11:00 AM. Previous comment was 2.5 hours wrong.
    w1_hour = PROFIT_PROTECT_1_HOUR
    w1_min  = PROFIT_PROTECT_1_MIN + trend_bonus_window
    while w1_min >= 60:
        w1_hour += 1
        w1_min  -= 60
    past_window1 = (hour > w1_hour or (hour == w1_hour and minute >= w1_min))
    scale1_threshold = SCALE_OUT_1_THRESHOLD * trend_bonus_threshold
    if past_window1 and option_pnl >= scale1_threshold and pos.scale_outs_done < 1:
        qty_close  = max(1, round(qty_rem * (0.35 if direction_aligns else 0.50)))
        trend_note = " [TREND DAY -- let runner breathe]" if direction_aligns else ""
        return ExitDecision(
            action="SCALE_OUT", quantity=qty_close,
            reason=f"PROFIT PROTECT W1 -- +{option_pnl*100:.0f}% at {w1_hour}:{w1_min:02d}+ scale{trend_note}",
            urgency="NORMAL", pnl_pct=option_pnl,
        )

    # ── 7. THETA KILL SWITCH (past noon, down >35%) ───────────────────────────
    past_noon = hour >= 12
    if past_noon and option_pnl <= THETA_STOP_LOSS_PCT:
        return ExitDecision(
            action="CLOSE_ALL", quantity=qty_rem,
            reason=f"THETA STOP -- option down {option_pnl*100:.0f}% after noon, cutting losses",
            urgency="NORMAL", pnl_pct=option_pnl,
        )

    return ExitDecision(
        action="HOLD", quantity=0,
        reason="No exit condition met", urgency="NORMAL", pnl_pct=option_pnl,
    )


# ── EXIT ENGINE ───────────────────────────────────────────────────────────────

EXIT_RULE_PRECEDENCE = (
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

STALE_OPTION_QUOTE_MAX_AGE_SEC = int(os.getenv("EXIT_ENGINE_STALE_OPTION_QUOTE_SEC", "20"))
FORCED_RISK_EXIT_CODES = {
    "EOD_FORCE_CLOSE",
    "STOP_HIT",
    "SENTINEL_FORCED_EXIT",
    "HARD_STOP",
    "NEVER_GREEN_STOP",
    "THETA_STOP",
    "TIME_STOP",
    # Winner-protection exits must be allowed through degraded quote mode.
    # If a position has already established a profit peak, a QPM gap should not
    # trap it and allow a winner to become a loser.
    "RUNNER_TRAIL",
    "PROFIT_LOCK",
    "TRAILING_STOP",
    "TOUCHED_PROFIT_STOP",
    "SMALL_WIN_LOCK",
    "PROFIT_PROTECT_W3",
    "PROFIT_PROTECT_W2",
    "PROFIT_PROTECT_W1",
}


def _active_exit_blocks_resubmit(order: dict | None) -> bool:
    """
    Return True if an in-flight exit order exists that must block resubmission.
    Prevents the PLTR-class bug: EXIT_ACKNOWLEDGED → EXIT_SUBMITTED illegal transition.
    The OSM correctly blocks the state machine transition; this guard stops the
    exit engine from even attempting to create a duplicate exit order.
    """
    if not order:
        return False
    status = str(order.get("status") or "").upper()
    return status in {
        "EXIT_REQUESTED",
        "EXIT_SUBMITTED",
        "EXIT_ACKNOWLEDGED",
        "EXIT_PARTIAL_FILL",
    }


def _classify_exit_decision(decision: "ExitDecision") -> str:
    explicit_code = (getattr(decision, "reason_code", "") or "").upper().strip()
    if explicit_code:
        return explicit_code
    r      = (getattr(decision, "reason", "") or "").upper()
    action = (getattr(decision, "action", "") or "").upper()
    if "SENTINEL" in r:              return "SENTINEL_FORCED_EXIT"
    if "EOD FORCE CLOSE" in r:       return "EOD_FORCE_CLOSE"
    if "THETA STOP" in r:            return "THETA_STOP"
    if "STOP HIT" in r:              return "STOP_HIT"
    if "HARD STOP" in r:             return "HARD_STOP"
    if "RUNNER TRAIL" in r or "RUNNER EMERGENCY" in r: return "RUNNER_TRAIL"
    if "SMALL WIN LOCK" in r:        return "SMALL_WIN_LOCK"
    if "TOUCHED PROFIT STOP" in r:   return "TOUCHED_PROFIT_STOP"
    if "UNDERLYING PROGRESS EXIT" in r: return "UNDERLYING_PROGRESS_EXIT"
    if "NEVER GREEN STOP" in r:      return "NEVER_GREEN_STOP"
    # Soft stops — these were returning UNKNOWN_EXIT, causing them to be
    # priced at mid (PROFIT_MID tier) instead of bid (RISK_BID). A position
    # being stopped out at a loss must price at bid for guaranteed fill.
    if "THESIS_FAIL_SOFT_STOP" in r: return "THESIS_FAIL_SOFT_STOP"
    if "THESIS_STALE_SOFT_STOP" in r: return "THESIS_STALE_SOFT_STOP"
    if "SOFT_STOP" in r or "SOFT STOP" in r: return "THESIS_FAIL_SOFT_STOP"
    if "PROFIT LOCK" in r:           return "PROFIT_LOCK"
    if "TRAILING STOP" in r:         return "TRAILING_STOP"
    if "IMMEDIATE TP" in r:          return "IMMEDIATE_TP"
    if "TARGET HIT" in r:            return "TARGET_HIT"
    if "PROFIT PROTECT W3" in r:     return "PROFIT_PROTECT_W3"
    if "PROFIT PROTECT W2" in r:     return "PROFIT_PROTECT_W2"
    if "PROFIT PROTECT W1" in r:     return "PROFIT_PROTECT_W1"
    if "TIME STOP" in r or "DEAD TRADE" in r: return "TIME_STOP"
    if action == "SCALE_OUT":        return "TP_SCALE_OUT"
    return "UNKNOWN_EXIT"


def _exit_priority(decision_or_code) -> int:
    code = decision_or_code if isinstance(decision_or_code, str) else _classify_exit_decision(decision_or_code)
    return EXIT_RULE_PRIORITY.get(code, LOWEST_EXIT_PRIORITY)


def _is_option_quote_stale(
    pos: "ManagedPosition",
    now_utc: Optional[datetime] = None,
) -> tuple[bool, Optional[float], str]:
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
    r = (reason or "").upper()
    return any(k in r for k in (
        "EOD", "STOP", "MAX_LOSS", "THETA", "PROTECTIVE", "FORCE CLOSE", "SENTINEL",
        "TARGET HIT", "IMMEDIATE TP", "PROFIT PROTECT", "SMALL WIN", "RUNNER TRAIL",
        "PROFIT LOCK", "TOUCHED PROFIT", "NEVER GREEN", "DEAD TRADE",
    ))


def _is_runner_protective_reason(reason: str) -> bool:
    r = (reason or "").upper()
    return (
        "RUNNER TRAIL" in r
        or "RUNNER EMERGENCY" in r
        or "SENTINEL MISSED TP" in r
        or "MISSED TP" in r
    )


def _is_same_or_equivalent_runner_protection(pending_reason: str, new_reason: str) -> bool:
    return _is_runner_protective_reason(pending_reason) and _is_runner_protective_reason(new_reason)


class APExitEngine:
    """
    Manages all open positions with time-aware exit logic.
    Runs as a background thread.
    """

    def __init__(self, broker, kill_switch_fn=None, email: str = "",
                 data_broker=None, master_control=None):
        self.broker        = broker
        self._quote_broker = data_broker or broker
        self._email        = email
        # PR-B / FIX-4: master_control is now accepted at construction
        # time so the post-assignment window (where exit_eng existed but
        # had no master_control reference) is closed. The execution core
        # still keeps the post-assignment as belt-and-suspenders.
        self.master_control = master_control
        self._positions: list[ManagedPosition] = []
        # P1: O(1) position index keyed by position_id.
        # Kept in sync with self._positions by add_position(), expired-contract
        # cleanup, mark_position_closed(), and note_partial_exit_fill() close path.
        self._positions_by_id: dict[str, ManagedPosition] = {}
        self._lock         = threading.RLock()

        # QPM/admin safety controls:
        # - _flattening prevents duplicate manual flatten loops inside this engine.
        # - _quote_arrived_event lets the PositionQuoteMonitor wake the exit loop immediately.
        # - quote_monitor is wired by the runner for metrics/observability.
        self._flattening   = threading.Event()
        self._quote_arrived_event = threading.Event()
        self.quote_monitor = None

        self._running      = False
        self._thread: Optional[threading.Thread] = None
        self.on_exit: Optional[Callable]  = None
        self.on_scale: Optional[Callable] = None
        self._kill_switch_fn = kill_switch_fn

        self.run_id           = os.getenv("AP_RUN_ID", "unknown")
        self.strategy_version = os.getenv("AP_STRATEGY_VERSION", "ap_live_beta")
        self.git_commit       = get_git_commit()

        # PR: position-lifecycle-integrity-and-sizing (P0 FIX-3)
        # Throttle state for persisting peak/high-water/touched_profit to the
        # positions table. The exit engine updates these in memory every
        # cycle; persisting every cycle would hammer the DB. We persist
        # when (a) >= EXIT_DB_PERSIST_THROTTLE_SEC has elapsed since last
        # persist for that position, OR (b) peak_pnl_pct or touched_profit
        # changed materially since last persist.
        self._last_peak_persist_ts:       dict[str, float] = {}   # pid -> epoch
        self._last_peak_persist_value:    dict[str, float] = {}   # pid -> peak
        self._last_peak_persist_touched:  dict[str, bool]  = {}   # pid -> touched
        self._EXIT_DB_PERSIST_THROTTLE_SEC = float(
            os.getenv("EXIT_DB_PERSIST_THROTTLE_SEC", "5.0")
        )
        self._EXIT_DB_PERSIST_PEAK_DELTA = float(
            os.getenv("EXIT_DB_PERSIST_PEAK_DELTA", "0.02")
        )
        self._EXIT_DB_PERSIST_ENABLED = (
            os.getenv("EXIT_DB_PERSIST_ENABLED", "1") == "1"
        )

    # ── P1: True O(1) position lookup ────────────────────────────────────────
    def get_position(self, position_id: str) -> Optional[ManagedPosition]:
        """
        O(1) position lookup by position_id via self._positions_by_id index.

        Previously still performed O(n) linear scan despite the method name.
        Now backed by a dict kept in sync by all write paths (add_position,
        expired-contract cleanup, mark_position_closed, note_partial_exit_fill).
        OSM's _get_exit_engine_position() calls this first; without the O(1)
        path every fill/close/hook callback would scan the full list.
        """
        pid = str(position_id or "")
        if not pid:
            return None
        with self._lock:
            return self._positions_by_id.get(pid)

    def _persist_peak_state_to_db(self, pos) -> bool:
        """
        P0 FIX-3: persist peak/high-water/touched_profit to the positions row.

        The exit engine updates these in memory every evaluation cycle. They
        are the protection layer for winning trades (PROFIT_FLOOR, trail).
        Without DB persistence:
          - dashboard shows peak_pnl_pct=0 on every position
          - restart-recovery loses every position's peak (no protection)
          - audit/proof cannot prove what protection was in force
        Throttled: writes when >= throttle sec have elapsed OR peak moved
        materially OR touched_profit transitioned False->True.

        Non-fatal: any DB error is logged and swallowed; never blocks the
        exit decision path. The exit engine continues to read in-memory
        state for decisions.
        """
        if not getattr(self, "_EXIT_DB_PERSIST_ENABLED", True):
            return False
        pid = str(getattr(pos, "position_id", "") or "")
        client_id = str(getattr(pos, "client_id", "") or getattr(self, "_email", "") or "")
        if not pid or not client_id:
            return False
        try:
            peak_now    = float(getattr(pos, "peak_pnl_pct", 0.0) or 0.0)
            max_profit  = float(getattr(pos, "max_profit_seen", 0.0) or 0.0)
            touched_now = bool(getattr(pos, "touched_profit", False))
            opt_pnl     = float(getattr(pos, "option_pnl_pct", 0.0) or 0.0)
            cur_opt     = float(getattr(pos, "current_option_price", 0.0) or 0.0)

            now = time.time()
            last_ts      = self._last_peak_persist_ts.get(pid, 0.0)
            last_peak    = self._last_peak_persist_value.get(pid, 0.0)
            last_touched = self._last_peak_persist_touched.get(pid, None)
            elapsed = now - last_ts
            peak_delta = abs(peak_now - last_peak)
            touched_changed = (last_touched is None) or (touched_now != last_touched)
            time_ok = elapsed >= float(getattr(self, "_EXIT_DB_PERSIST_THROTTLE_SEC", 5.0))
            peak_ok = peak_delta >= float(getattr(self, "_EXIT_DB_PERSIST_PEAK_DELTA", 0.02))
            if not (time_ok or peak_ok or touched_changed):
                return False

            from ap.db import conn, run_with_retry  # local import avoids cycle

            def _do_update():
                with conn() as c:
                    c.execute(
                        """
                        UPDATE positions
                        SET peak_pnl_pct        = %s,
                            max_profit_seen     = %s,
                            touched_profit      = %s,
                            option_pnl_pct      = %s,
                            current_option_price= COALESCE(NULLIF(%s, 0), current_option_price),
                            updated_at          = NOW()
                        WHERE id        = %s
                          AND client_id = %s
                          AND status   IN ('OPEN', 'CLOSING')
                        """,
                        (
                            peak_now,
                            max_profit,
                            touched_now,
                            opt_pnl,
                            cur_opt if cur_opt > 0 else 0.0,
                            pid,
                            client_id,
                        ),
                    )
                    return c.rowcount

            rowcount = run_with_retry(_do_update) or 0
            self._last_peak_persist_ts[pid]      = now
            self._last_peak_persist_value[pid]   = peak_now
            self._last_peak_persist_touched[pid] = touched_now
            return rowcount > 0
        except Exception as exc:
            log.debug(
                "[exit_eng] _persist_peak_state_to_db non-fatal failure for pos=%s: %s",
                pid, exc,
            )
            return False


    def add_position(self, pos: ManagedPosition):
        """Track a newly broker-confirmed open position for exit protection."""
        if pos is None:
            return

        # Normalize before duplicate checks and before QPM sees the position.
        # A corrupted ticker like META26 makes quote fetches fail and blinds exits.
        _raw_ticker = str(pos.ticker or "")
        _fixed_ticker = _normalize_ticker(_raw_ticker, pos.option_symbol or "")
        if _fixed_ticker != _raw_ticker:
            log.warning(
                "[exit_eng] add_position: ticker normalized '%s' -> '%s' | contract=%s | pos_id=%s",
                _raw_ticker, _fixed_ticker, pos.option_symbol or "?", pos.position_id or "?",
            )
            pos.ticker = _fixed_ticker

        with self._lock:
            for existing in self._positions:
                same_id  = bool(pos.position_id and existing.position_id == pos.position_id)
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
            # P1: keep O(1) index in sync.
            if pos.position_id:
                self._positions_by_id[pos.position_id] = pos
        log.info(
            "[%s] Position added to exit engine | %s %sx %s @ $%.2f | target=%s stop=%s | pos_id=%s",
            pos.ticker, pos.side, pos.quantity, pos.option_symbol, pos.entry_price,
            pos.underlying_target, pos.underlying_stop, pos.position_id or "n/a",
        )

    def start(self):
        if self._thread and self._thread.is_alive():
            log.debug("APExitEngine already running [%s]", self._email or "default")
            self._running = True
            return
        self._running = True
        thread_name   = f"ap-exit-engine-{self._email}" if self._email else "ap-exit-engine"
        self._thread  = threading.Thread(target=self._exit_loop, name=thread_name, daemon=True)
        self._thread.start()
        log.info("APExitEngine started [%s]", thread_name)

    def stop(self):
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        self._thread = None
        log.info("[%s] APExitEngine stopped", self._email or "default")

    def active_positions(self) -> list[ManagedPosition]:
        with self._lock:
            return [p for p in self._positions if not p.closed and int(p.quantity_remaining or 0) > 0]

    def attach_quote_monitor(self, monitor) -> None:
        """Wire the PositionQuoteMonitor for observability and wake-driven exits."""
        self.quote_monitor = monitor

    def apply_quote_snapshots(self, snapshots: list[dict]) -> None:
        """
        Stable quote-monitor -> exit-engine state update contract.

        The PositionQuoteMonitor can push quote snapshots here instead of relying
        only on arbitrary object mutation. This keeps the exit engine as the
        canonical owner of its ManagedPosition state while still allowing the
        monitor to hydrate prices at high frequency.
        """
        if not snapshots:
            return

        by_id = {
            str(s.get("position_id") or ""): s
            for s in snapshots
            if s.get("position_id")
        }
        if not by_id:
            return

        with self._lock:
            for pos in self._positions:
                if getattr(pos, "closed", False):
                    continue

                pid = str(getattr(pos, "position_id", "") or "")
                snap = by_id.get(pid)
                if not snap:
                    continue

                try:
                    current_underlying = float(snap.get("current_underlying") or 0.0)
                    current_option_price = float(snap.get("current_option_price") or 0.0)
                    current_bid = float(snap.get("current_bid") or 0.0)
                    current_ask = float(snap.get("current_ask") or 0.0)

                    if current_underlying > 0:
                        pos.current_underlying = current_underlying
                    if current_option_price > 0:
                        pos.current_option_price = current_option_price
                    if current_bid > 0:
                        pos.current_bid = current_bid
                    if current_ask > 0:
                        pos.current_ask = current_ask

                    if snap.get("last_underlying_quote_update_ts") is not None:
                        pos.last_underlying_quote_update_ts = snap.get("last_underlying_quote_update_ts")
                    if snap.get("last_option_quote_update_ts") is not None:
                        pos.last_option_quote_update_ts = snap.get("last_option_quote_update_ts")

                    if snap.get("price_source"):
                        setattr(pos, "last_option_price_source", snap.get("price_source"))

                except Exception as exc:
                    log.debug(
                        "apply_quote_snapshots failed pos_id=%s err=%s",
                        pid or "?",
                        exc,
                    )

    def emergency_flatten(
        self,
        reason: str = "manual_flatten",
        force: bool = True,
        bypass_quote_gate: bool = True,
    ) -> int:
        """
        Manual admin flatten path.

        Reuses the normal exit submission pipeline and marks the decision as
        forced-risk so stale/blind quote gates do not block emergency liquidation.
        This keeps fills, OSM state, reconciler state, and audit trails coherent.
        """
        if self._flattening.is_set():
            log.warning("emergency_flatten already running")
            return 0

        self._flattening.set()

        try:
            submitted = 0

            with self._lock:
                snapshot = [
                    p for p in self._positions
                    if not getattr(p, "closed", False)
                    and int(getattr(p, "quantity_remaining", 0) or 0) > 0
                ]

            for pos in snapshot:
                try:
                    qty = int(getattr(pos, "quantity_remaining", 0) or 0)
                    if qty <= 0:
                        continue

                    decision = ExitDecision(
                        action="CLOSE_ALL",
                        quantity=qty,
                        reason=f"SENTINEL FORCED EXIT -- {reason}",
                        urgency="IMMEDIATE",
                        pnl_pct=float(getattr(pos, "option_pnl_pct", 0.0) or 0.0),
                        reason_code="SENTINEL_FORCED_EXIT",
                    )

                    ok = self._submit_exit_decision(
                        pos,
                        decision,
                        from_sentinel=True,
                        allow_inflight_override=bool(force),
                    )

                    if ok:
                        submitted += 1

                except Exception as e:
                    log.error(
                        "emergency_flatten failed pos_id=%s symbol=%s err=%s",
                        getattr(pos, "position_id", "?"),
                        getattr(pos, "option_symbol", "?"),
                        e,
                        exc_info=True,
                    )

            log.critical(
                "EMERGENCY_FLATTEN submitted=%d reason=%s force=%s bypass_quote_gate=%s",
                submitted,
                reason,
                force,
                bypass_quote_gate,
            )

            return submitted

        finally:
            self._flattening.clear()

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
        if not position_id:
            return
        try:
            qty_i = max(0, int(qty or 0))
        except Exception:
            qty_i = 0
        local_order_id  = str(local_order_id or "")
        broker_order_id = str(broker_order_id or "")
        reason          = str(reason or "")

        # PR-A / BUG-3: O(1) lookup via self._positions_by_id (was O(n) scan).
        with self._lock:
            _matched = self._positions_by_id.get(str(position_id or ""))
            for pos in (_matched,) if _matched is not None else ():
                if pos.closed or int(pos.quantity_remaining or 0) <= 0:
                    return
                pos.exit_in_flight    = True
                pos.pending_exit_reason = reason or pos.pending_exit_reason or "osm_pending_exit_order"
                if qty_i > 0:
                    pos.pending_exit_qty = qty_i
                elif int(pos.pending_exit_qty or 0) <= 0:
                    pos.pending_exit_qty = int(pos.quantity_remaining or 0)
                pos.pending_exit_local_order_id  = local_order_id  or pos.pending_exit_local_order_id  or ""
                pos.pending_exit_broker_order_id = broker_order_id or pos.pending_exit_broker_order_id or ""
                pos.last_exit_signal_ts  = datetime.now(timezone.utc)
                pos.last_exit_rejected   = False
                pos._exit_stuck_count    = 0
                if local_order_id or broker_order_id:
                    pos.last_callback_identity_missing    = False
                    pos.last_callback_identity_missing_ts = None
                    pos.exit_identity_quarantine          = False
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
                    position_id, local_order_id or "?", broker_order_id or "?", qty_i, reason or "?",
                )
                return

        log.warning(
            "[exit_eng] OSM pending exit for unknown position | pos_id=%s local=%s broker=%s qty=%s reason=%s",
            position_id, local_order_id or "?", broker_order_id or "?", qty_i, reason or "?",
        )

    # ── BROKER / OSM RECONCILIATION HOOKS ────────────────────────────────────

    def _pending_exit_has_identity(self, pos: ManagedPosition) -> bool:
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
        local_order_id  = str(local_order_id or "")
        broker_order_id = str(broker_order_id or "")
        pending_local   = str(getattr(pos, "pending_exit_local_order_id", "") or "")
        pending_broker  = str(getattr(pos, "pending_exit_broker_order_id", "") or "")
        supplied_identity = bool(local_order_id or broker_order_id)
        pending_identity  = bool(pending_local or pending_broker)
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
        if not position_id:
            return
        reason_s        = str(reason or "")
        local_order_id  = str(local_order_id or "")
        broker_order_id = str(broker_order_id or "")
        force = bool(force or reconciled or kwargs.get("force") or kwargs.get("reconciled"))
        if "RECONCILER" in reason_s.upper():
            force = True

        # PR-A / BUG-3: O(1) lookup via self._positions_by_id (was O(n) scan).
        with self._lock:
            _matched = self._positions_by_id.get(str(position_id or ""))
            for pos in (_matched,) if _matched is not None else ():
                if not force and not self._exit_identity_matches(
                    pos,
                    local_order_id=local_order_id,
                    broker_order_id=broker_order_id,
                    allow_missing_when_no_pending_identity=True,
                ):
                    self._reject_stale_exit_hook(
                        pos, hook_name="mark_position_closed",
                        local_order_id=local_order_id,
                        broker_order_id=broker_order_id, reason=reason_s,
                    )
                    return
                pos.closed        = True
                pos.close_reason  = reason_s or pos.close_reason or "broker_confirmed_closed"
                pos.quantity_remaining = 0
                # P2: advance generation so concurrent _submit_exit_decision validators
                # detect this broker-confirmed close and do not mark stale in-flight state.
                pos._submit_generation += 1
                try:
                    if fill_price is not None:
                        pos.current_option_price = float(fill_price)
                except Exception as _fp_err:
                    log.error("Failed to set fill_price on position: %s", _fp_err)
                # FIX 2: fire proof finalization with broker-confirmed fill price.
                # This is the ONLY point where we have the real fill — submit_exit
                # only knew the limit price. on_exit_fill_confirmed calls
                # APExecutionCore._finalize_proof() which writes proof/P&L/feedback
                # using actual fill_price, not the estimated bid/mid at submit.
                try:
                    _fill_cb = getattr(self, "on_exit_fill_confirmed", None)
                    if _fill_cb is not None and callable(_fill_cb):
                        _fill_cb(pos, float(fill_price) if fill_price is not None else 0.0)
                except Exception as _cb_err:
                    log.error("on_exit_fill_confirmed callback failed (non-fatal): %s", _cb_err)
                pos.exit_in_flight   = False
                pos.pending_exit_reason = ""
                pos.pending_exit_action = ""
                pos.pending_exit_qty    = 0
                pos.pending_exit_filled_qty = 0
                pos.pending_scale_counted   = False
                pos.last_applied_exit_local_order_id  = local_order_id  or pos.pending_exit_local_order_id  or pos.last_applied_exit_local_order_id
                pos.last_applied_exit_broker_order_id = broker_order_id or pos.pending_exit_broker_order_id or pos.last_applied_exit_broker_order_id
                pos.last_exit_signal_ts               = None
                pos.last_callback_identity_missing    = False
                pos.last_callback_identity_missing_ts = None
                pos.exit_identity_quarantine          = False
                pos.last_exit_identity_quarantine_resolved_ts = datetime.now(timezone.utc)
                pos.exit_identity_quarantine_alert_count     = getattr(pos, "exit_identity_quarantine_alert_count", 0)
                pos.last_exit_identity_quarantine_alert_ts   = getattr(pos, "last_exit_identity_quarantine_alert_ts", None)
                pos.pending_exit_replace_allowed  = False
                pos.pending_exit_replace_reason   = ""
                pos.pending_exit_replace_allowed_ts = None
                self._emit_exit_event(
                    pos,
                    decision="CLOSED",
                    reason_code="BROKER_CONFIRMED_CLOSED",
                    explanation=pos.close_reason,
                    stage="exit_reconciliation",
                    extra_inputs={
                        "local_order_id": local_order_id,
                        "broker_order_id": broker_order_id,
                        "force": force,
                    },
                )
                # FIX-3: Remove from O(1) index on close to prevent memory leak
                # and get_position() returning closed positions to callers.
                if pos.position_id and pos.position_id in self._positions_by_id:
                    del self._positions_by_id[pos.position_id]
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
        if not position_id:
            return
        reason_s        = str(reason or "")
        local_order_id  = str(local_order_id or "")
        broker_order_id = str(broker_order_id or "")
        force = bool(force or reconciled or kwargs.get("force") or kwargs.get("reconciled"))
        if "RECONCILER" in reason_s.upper() or "NEGATIVE_BROKER_CHECK" in reason_s.upper():
            force = True

        # PR-A / BUG-3: O(1) lookup via self._positions_by_id (was O(n) scan).
        with self._lock:
            _matched = self._positions_by_id.get(str(position_id or ""))
            for pos in (_matched,) if _matched is not None else ():
                if not force and not self._exit_identity_matches(
                    pos,
                    local_order_id=local_order_id,
                    broker_order_id=broker_order_id,
                    allow_missing_when_no_pending_identity=True,
                ):
                    self._reject_stale_exit_hook(
                        pos, hook_name="clear_exit_in_flight",
                        local_order_id=local_order_id,
                        broker_order_id=broker_order_id, reason=reason_s,
                    )
                    return
                pos.exit_in_flight   = False
                pos.pending_exit_reason = ""
                pos.pending_exit_action = ""
                pos.pending_exit_qty    = 0
                pos.pending_exit_filled_qty = 0
                pos.pending_scale_counted   = False
                pos.last_exit_clear_reason           = reason_s
                pos.last_exit_clear_local_order_id   = local_order_id
                pos.last_exit_clear_broker_order_id  = broker_order_id
                pos.pending_exit_local_order_id  = ""
                pos.pending_exit_broker_order_id = ""
                # Do not wipe last_applied_exit_cum_fill_by_order here. Late callbacks
                # for the just-cleared order may still arrive; only _mark_exit_submitted()
                # resets it when a new order generation begins.
                pos.last_exit_signal_ts  = None
                # FIX-8: last_rejection_ts is now Optional[datetime], consistent with all
                # other timestamp fields on ManagedPosition.
                pos.last_exit_rejected   = bool(rejected)
                pos.last_rejection_ts    = datetime.now(timezone.utc) if rejected else None
                pos.last_callback_identity_missing    = False
                pos.last_callback_identity_missing_ts = None
                pos.exit_identity_quarantine          = False
                pos.last_exit_identity_quarantine_resolved_ts = datetime.now(timezone.utc)
                pos.pending_exit_replace_allowed   = False
                pos.pending_exit_replace_reason    = ""
                pos.pending_exit_replace_allowed_ts = None
                self._assert_position_invariants(pos, "clear_exit_in_flight")
                self._emit_exit_event(
                    pos,
                    decision="ALERT" if rejected else "CLEARED",
                    reason_code="EXIT_REJECTED" if rejected else "EXIT_IN_FLIGHT_CLEARED",
                    explanation=reason_s or ("Exit order rejected/cleared" if rejected else "Exit in-flight cleared"),
                    stage="exit_reconciliation",
                    extra_inputs={
                        "local_order_id": local_order_id,
                        "broker_order_id": broker_order_id,
                        "rejected": rejected,
                        "force": force,
                    },
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
        """OSM v3-compatible hook for exit failure/rejection."""
        # FIX-10: extra blank line between method signature and body removed.
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
        # PR-A / BUG-3: O(1) lookup via self._positions_by_id (was O(n) scan).
        with self._lock:
            _matched = self._positions_by_id.get(str(position_id or ""))
            for pos in (_matched,) if _matched is not None else ():
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

                order_key = (
                    broker_order_id
                    or local_order_id
                    or pos.pending_exit_broker_order_id
                    or pos.pending_exit_local_order_id
                    or f"pending:{pos.position_id}:{pos.last_exit_signal_ts.isoformat() if pos.last_exit_signal_ts else 'unknown'}"
                )

                if cumulative_filled_qty is not None:
                    cum   = max(0, int(cumulative_filled_qty or 0))
                    prev  = int(pos.last_applied_exit_cum_fill_by_order.get(order_key, 0) or 0)
                    delta = max(0, cum - prev)
                    pos.last_applied_exit_cum_fill_by_order[order_key] = max(prev, cum)
                    pos.last_applied_exit_cum_fill = max(int(pos.last_applied_exit_cum_fill or 0), cum)
                else:
                    delta = max(0, int(qty_filled or 0))
                    prev  = int(pos.last_applied_exit_cum_fill_by_order.get(order_key, 0) or 0)
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
                pos.last_applied_exit_local_order_id  = local_order_id  or pos.pending_exit_local_order_id  or pos.last_applied_exit_local_order_id
                pos.last_applied_exit_broker_order_id = broker_order_id or pos.pending_exit_broker_order_id or pos.last_applied_exit_broker_order_id
                if local_order_id or broker_order_id or pos.pending_exit_local_order_id or pos.pending_exit_broker_order_id:
                    pos.last_callback_identity_missing    = False
                    pos.last_callback_identity_missing_ts = None
                    pos.exit_identity_quarantine          = False
                    pos.exit_identity_quarantine_alert_count = 0
                    pos.last_exit_identity_quarantine_alert_ts = None
                    pos.pending_exit_replace_allowed  = False
                    pos.pending_exit_replace_reason   = ""
                    pos.pending_exit_replace_allowed_ts = None

                if (
                    (pos.pending_exit_action or "").upper() == "SCALE_OUT"
                    and pos.pending_exit_qty > 0
                    and not pos.pending_scale_counted
                    and pos.quantity_remaining > 0
                ):
                    fully_confirmed_scale = pos.pending_exit_filled_qty >= pos.pending_exit_qty
                    if fully_confirmed_scale:
                        pos.scale_outs_done     += 1
                        pos.pending_scale_counted = True
                        # Snapshot values under lock; persist AFTER lock releases
                        # to avoid blocking add_position/fill hooks for DB latency.
                        _scale_sd = pos.scale_outs_done
                        _scale_qr = pos.quantity_remaining
                        _scale_pi = pos.position_id

                fully_filled_pending = (
                    pos.pending_exit_qty > 0
                    and pos.pending_exit_filled_qty >= pos.pending_exit_qty
                )
                if pos.quantity_remaining <= 0:
                    pos.closed       = True
                    pos.close_reason = pos.pending_exit_reason or "exit_fill_closed"
                    # P2: advance generation so any concurrent _submit_exit_decision
                    # post-callback validator sees the close and does not overwrite truth.
                    pos._submit_generation += 1
                    # FIX-3: Remove from O(1) index so get_position() does not
                    # return this closed position to callers.
                    if pos.position_id and pos.position_id in self._positions_by_id:
                        del self._positions_by_id[pos.position_id]

                if fully_filled_pending or pos.closed:
                    pos.exit_in_flight      = False
                    pos.pending_exit_reason = ""
                    pos.pending_exit_action = ""
                    pos.pending_exit_qty    = 0
                    pos.pending_exit_filled_qty = 0
                    pos.pending_scale_counted   = False
                    pos.pending_exit_local_order_id  = ""
                    pos.pending_exit_broker_order_id = ""
                    # FIX-6: watermark dict is NOT cleared here. _mark_exit_submitted()
                    # resets it when a new order generation begins. Preserving it here
                    # means late duplicate callbacks for the just-completed order still
                    # see their prev watermark and compute delta=0, preventing double-counts
                    # from the fill monitor + reconciler dual-path.
                    pos.last_applied_exit_cum_fill = 0
                    pos.last_exit_signal_ts = None
                    pos._exit_stuck_count   = 0

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

        # FIX-2: DB persist for scale fill snapshot moved OUTSIDE self._lock
        # to prevent blocking exit loop for DB latency duration.
        if "_scale_pi" in dir() and _scale_pi:
            try:
                from ap.db import conn, run_with_retry as _rwr_s
                _sd_snap, _qr_snap, _pi_snap = _scale_sd, _scale_qr, _scale_pi
                def _save_scale():
                    with conn() as _c:
                        _c.execute(
                            "UPDATE positions SET scale_outs_done=%s, quantity_remaining=%s WHERE id=%s",
                            (_sd_snap, _qr_snap, _pi_snap),
                        )
                _rwr_s(_save_scale)
            except Exception as _se:
                log.debug("[exit_eng] scale/qty persist failed (non-critical): %s", _se)
        log.info("[exit_eng] Exit fill noted | pos_id=%s qty_delta=%d", position_id, int(applied_delta or qty_filled or 0))

    def _mark_exit_submitted(
        self,
        pos: ManagedPosition,
        decision: ExitDecision,
        *,
        local_order_id: str = "",
        broker_order_id: str = "",
    ) -> None:
        pos.exit_in_flight      = True
        pos.pending_exit_reason = decision.reason or ""
        pos.pending_exit_action = (decision.action or "").upper()
        pos.pending_exit_qty    = max(0, int(decision.quantity or 0))
        pos.pending_exit_filled_qty = 0
        pos.pending_scale_counted   = False
        pos.pending_exit_local_order_id  = local_order_id  or pos.pending_exit_local_order_id  or ""
        pos.pending_exit_broker_order_id = broker_order_id or pos.pending_exit_broker_order_id or ""
        # FIX-6: new order generation resets the per-order watermark dict so the
        # new order's cumulative fills start from zero.
        pos.last_applied_exit_cum_fill_by_order = {}
        pos.last_applied_exit_cum_fill  = 0
        pos.last_exit_signal_ts         = datetime.now(timezone.utc)
        pos.last_exit_rejected          = False
        pos._exit_stuck_count           = 0
        # P2: increment generation so concurrent post-callback validators can detect
        # that state was advanced while they were waiting outside the lock.
        pos._submit_generation         += 1
        pos.exit_identity_quarantine    = bool(pos.last_callback_identity_missing)
        pos.pending_exit_replace_allowed    = False
        pos.pending_exit_replace_reason     = ""
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
        if not position_id:
            return
        reason_s        = str(reason or "")
        local_order_id  = str(local_order_id or "")
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
                            pos, hook_name="mark_exit_replacement_safe",
                            local_order_id=local_order_id,
                            broker_order_id=broker_order_id, reason=reason_s,
                        )
                        return
                    pos.pending_exit_replace_allowed  = True
                    pos.pending_exit_replace_reason   = reason_s or "external_cancel_or_reconcile_proof"
                    pos.pending_exit_replace_allowed_ts = datetime.now(timezone.utc)
                    pos.exit_identity_quarantine = False
                    pos.last_exit_identity_quarantine_resolved_ts = datetime.now(timezone.utc)
                    self._emit_exit_event(
                        pos, "ALERT", "EXIT_REPLACEMENT_MARKED_SAFE",
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
                        pos.ticker, position_id,
                        local_order_id or "?", broker_order_id or "?",
                        pos.pending_exit_replace_reason, force,
                    )
                    return

    def _eligible_for_new_exit(self, pos: ManagedPosition, now_utc: datetime) -> bool:
        return self._can_submit_exit(pos, now_utc, reason=pos.pending_exit_reason or "poll")

    def _run_sentinels(self):
        now = datetime.now(timezone.utc)
        # Elite-2: take snapshot inside lock for consistency. list(self._positions)
        # without the lock can race with the locked expired-contract cleanup that
        # reassigns self._positions = [...]. Snapshot under lock, iterate outside.
        with self._lock:
            snapshot = list(self._positions)
        for pos in snapshot:
            if pos.closed or int(pos.quantity_remaining or 0) <= 0:
                continue
            age_min = (now - pos.opened_at).total_seconds() / 60 if pos.opened_at else 0
            pnl     = pos.option_pnl_pct
            peak    = pos.peak_pnl_pct

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
                should_alert  = quarantine_age_sec >= 30.0 and (
                    last_alert_ts is None
                    or (now - last_alert_ts).total_seconds() >= 30.0
                )
                if should_alert:
                    # Re-fetch live ref under lock before writing — position may
                    # have been closed/removed between snapshot and now.
                    with self._lock:
                        _live_q = self._positions_by_id.get(pos.position_id)
                        if not _live_q or _live_q.closed or int(_live_q.quantity_remaining or 0) <= 0:
                            continue
                        _live_q.exit_identity_quarantine_alert_count = int(
                            getattr(_live_q, "exit_identity_quarantine_alert_count", 0) or 0
                        ) + 1
                        _live_q.last_exit_identity_quarantine_alert_ts = now
                    pos = _live_q  # use live ref for remainder of this iteration

                    # P5: Quarantine escalation. After QUARANTINE_ESCALATE_AFTER_N alerts
                    # (default 5 = ~150s at 30s cadence), escalate to CRITICAL and emit a
                    # distinct reason code so dashboards/PagerDuty can alert differently.
                    # The only safe resolution remains an explicit reconciler/OSM hook call.
                    _qcount = pos.exit_identity_quarantine_alert_count
                    _escalate_after = int(os.getenv("EXIT_QUARANTINE_ESCALATE_AFTER_N", "5"))
                    _escalated = _qcount >= _escalate_after

                    log.critical(
                        "[%s] EXIT IDENTITY QUARANTINE %s %.1fs x%d — duplicate exits blocked; "
                        "reconciler/OSM must resolve | pos_id=%s action=%s qty=%s local=%s broker=%s reason=%s",
                        pos.ticker,
                        "ESCALATED" if _escalated else "ACTIVE",
                        quarantine_age_sec,
                        _qcount,
                        pos.position_id or "?",
                        pos.pending_exit_action or "?",
                        pos.pending_exit_qty,
                        pos.pending_exit_local_order_id  or "?",
                        pos.pending_exit_broker_order_id or "?",
                        pos.pending_exit_reason or "?",
                    ) if _escalated else log.error(
                        "[%s] EXIT IDENTITY QUARANTINE ACTIVE %.1fs x%d — duplicate exits blocked; "
                        "reconciler/OSM must resolve | pos_id=%s action=%s qty=%s local=%s broker=%s reason=%s",
                        pos.ticker, quarantine_age_sec, _qcount,
                        pos.position_id or "?",
                        pos.pending_exit_action or "?",
                        pos.pending_exit_qty,
                        pos.pending_exit_local_order_id  or "?",
                        pos.pending_exit_broker_order_id or "?",
                        pos.pending_exit_reason or "?",
                    )
                    self._emit_exit_event(
                        pos, decision="ALERT",
                        reason_code=(
                            "EXIT_IDENTITY_QUARANTINE_ESCALATED"
                            if _escalated
                            else "EXIT_IDENTITY_QUARANTINE_NEEDS_RECONCILE"
                        ),
                        explanation=(
                            f"ESCALATED after {_qcount} alerts: "
                            if _escalated else ""
                        ) + (
                            "Accepted exit callback returned no local/broker order identity. "
                            "Duplicate exits are blocked until OSM/reconciler proves fill, cancel, reject, "
                            "broker order identity, or safe replacement."
                        ),
                        stage="system_alert",
                        extra_inputs={
                            "quarantine_age_sec": quarantine_age_sec,
                            "alert_count": _qcount,
                            "escalated": _escalated,
                            "escalate_after": _escalate_after,
                            "pending_exit_action": pos.pending_exit_action,
                            "pending_exit_qty": pos.pending_exit_qty,
                            "pending_exit_reason": pos.pending_exit_reason,
                            "pending_exit_local_order_id":  pos.pending_exit_local_order_id,
                            "pending_exit_broker_order_id": pos.pending_exit_broker_order_id,
                        },
                    )

                    # Elite-1: Force-reconcile action on escalation.
                    # Two paths depending on what identity we have:
                    #
                    # PATH A — broker_order_id is known: ask broker for current order
                    #   status and apply the result directly. This resolves the most
                    #   common "accepted but slow fill confirmation" quarantine case
                    #   without waiting for the next reconciler cycle.
                    #
                    # PATH B — no broker_order_id: emit FORCE_RECONCILE_REQUEST so
                    #   the reconciler can perform fuzzy matching on the next pass.
                    #   The exit engine cannot replicate reconciler identity logic
                    #   (sell-to-close matching, time proximity scoring, etc.) so
                    #   attempting inline resolution without an order ID is unsafe.
                    if _escalated:
                        self._attempt_quarantine_force_reconcile(pos, quarantine_age_sec)

            if peak >= IMMEDIATE_TP_PCT and not pos.exit_in_flight and age_min > 1:
                log.error(
                    "[SENTINEL] %s | MISSED TP — peaked +%.0f%% but no exit submitted | pos=%s pnl=%.1f%% age=%.0fm",
                    pos.ticker, peak * 100, pos.position_id, pnl * 100, age_min,
                )

            if pnl <= HARD_STOP_PCT and not pos.exit_in_flight and age_min > 1:
                decision = ExitDecision(
                    action="CLOSE_ALL", quantity=pos.quantity_remaining,
                    reason=f"SENTINEL FORCED EXIT — {pnl*100:.0f}% with no exit order",
                    urgency="IMMEDIATE", pnl_pct=pnl,
                )
                self._submit_exit_decision(pos, decision, from_sentinel=True, allow_inflight_override=True)
                continue

            if pos.exit_in_flight and pos.last_exit_signal_ts:
                flight_sec = (now - pos.last_exit_signal_ts).total_seconds()
                if flight_sec > 300:
                    # Re-fetch live ref under lock before writing state.
                    with self._lock:
                        _live_s = self._positions_by_id.get(pos.position_id)
                        if not _live_s or _live_s.closed or int(_live_s.quantity_remaining or 0) <= 0:
                            continue
                        _live_s._exit_stuck_count = int(getattr(_live_s, "_exit_stuck_count", 0) or 0) + 1
                        if _live_s._exit_stuck_count >= 2:
                            _live_s.last_exit_rejected = True
                        _stuck_count = _live_s._exit_stuck_count
                    if _stuck_count >= 2:
                        log.error(
                            "[SENTINEL] %s | EXIT STUCK x%d — %.0fs in-flight, no fill | pos=%s",
                            pos.ticker, _stuck_count, flight_sec, pos.position_id,
                        )
                        self._emit_exit_event(
                            pos, decision="ALERT", reason_code="EXIT_STUCK",
                            explanation=f"Exit in flight for {flight_sec:.0f}s with no fill",
                            stage="system_alert",
                            extra_inputs={"flight_sec": flight_sec, "stuck_count": _stuck_count},
                        )
                    else:
                        log.warning(
                            "[SENTINEL] %s | EXIT STUCK — %.0fs in-flight, no fill | pos=%s",
                            pos.ticker, flight_sec, pos.position_id,
                        )

            DEAD_TRADE_MIN  = 45
            DEAD_TRADE_LOW  = -0.08
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
                        action="CLOSE_ALL", quantity=pos.quantity_remaining,
                        reason=(
                            f"TIME STOP — thesis not confirmed after {age_min:.0f}min "
                            f"pnl={pnl*100:.1f}% progress={progress*100:.0f}% toward target"
                        ),
                        urgency="HIGH", pnl_pct=pnl,
                    )
                    self._submit_exit_decision(pos, decision, from_sentinel=True)

    def _attempt_quarantine_force_reconcile(
        self,
        pos: ManagedPosition,
        quarantine_age_sec: float,
    ) -> None:
        """
        Elite-1: Force-reconcile action triggered when quarantine has escalated.

        Boundary rule: the exit engine must not replicate reconciler logic.
        - If broker_order_id is known → ask broker directly; apply confirmed state.
        - If broker_order_id is missing → emit FORCE_RECONCILE_REQUEST so the
          reconciler can use its fuzzy-match logic on the next cycle. Do not guess.

        This method must never make a blocking network call under self._lock.
        It reads pos fields without the lock (sentinel already has a snapshot),
        and only acquires the lock for state mutations after broker I/O completes.
        """
        broker_oid = str(pos.pending_exit_broker_order_id or "").strip()
        position_id = str(pos.position_id or "")
        ticker = str(pos.ticker or "")

        if not broker_oid:
            # PATH B: no broker identity → signal reconciler, do not guess.
            log.critical(
                "[%s] QUARANTINE_FORCE_RECONCILE_REQUEST | pos=%s | "
                "no broker_order_id — reconciler must perform fuzzy identity recovery on next cycle | "
                "quarantine_age=%.0fs",
                ticker, position_id or "?", quarantine_age_sec,
            )
            self._emit_exit_event(
                pos,
                decision="ALERT",
                reason_code="FORCE_RECONCILE_REQUEST",
                explanation=(
                    "Quarantine escalated with no broker_order_id. "
                    "Reconciler must perform fuzzy identity recovery (sell-to-close match, "
                    "time proximity, qty match) on next cycle."
                ),
                stage="system_alert",
                extra_inputs={
                    "quarantine_age_sec": quarantine_age_sec,
                    "pending_exit_local_order_id": pos.pending_exit_local_order_id,
                    "pending_exit_reason": pos.pending_exit_reason,
                },
            )
            return

        # PATH A: broker_order_id is known → lightweight broker status check.
        # This is the common case: broker accepted the order but fill confirmation
        # is delayed (e.g. broker API latency, network hiccup on callback).
        log.critical(
            "[%s] QUARANTINE_FORCE_RECONCILE_BROKER_CHECK | pos=%s broker=%s | "
            "asking broker for order status directly | quarantine_age=%.0fs",
            ticker, position_id or "?", broker_oid, quarantine_age_sec,
        )
        try:
            broker_raw = self.broker.get_order(broker_oid)
        except Exception as exc:
            log.error(
                "[%s] QUARANTINE_FORCE_RECONCILE_BROKER_FETCH_FAILED | pos=%s broker=%s | %s",
                ticker, position_id or "?", broker_oid, exc,
            )
            self._emit_exit_event(
                pos,
                decision="ALERT",
                reason_code="FORCE_RECONCILE_BROKER_FETCH_FAILED",
                explanation=f"Broker get_order failed during forced reconcile: {exc}",
                stage="system_alert",
                extra_inputs={"broker_order_id": broker_oid, "error": str(exc)},
            )
            return

        broker_status = str(broker_raw.get("status") or broker_raw.get("Status") or "").lower().strip()

        _broker_filled   = {"filled", "partially_filled"}
        _broker_terminal = {"canceled", "cancelled", "rejected", "expired"}

        # Best-effort qty and price extraction — shared by both fill branches.
        # Treated as cumulative (consistent with note_partial_exit_fill's
        # per-order watermark model). Broker field names scanned in priority order.
        _filled_qty = None
        _fill_price = None
        for _fk in ("filled_qty", "filled_quantity", "cumulative_filled_qty", "exec_quantity"):
            _v = broker_raw.get(_fk)
            if _v is not None:
                try: _filled_qty = int(float(_v)); break
                except Exception: pass
        for _pk in ("avg_fill_price", "average_fill_price", "fill_price", "avg_price"):
            _v = broker_raw.get(_pk)
            if _v is not None:
                try: _fill_price = float(_v); break
                except Exception: pass

        if broker_status == "filled":
            # Full fill confirmed — hard close is correct and safe.
            log.critical(
                "[%s] QUARANTINE_FORCE_RECONCILE_FILL_CONFIRMED | pos=%s broker=%s | "
                "broker status=%s filled_qty=%s fill_price=%s — applying force close",
                ticker, position_id or "?", broker_oid,
                broker_status, _filled_qty, _fill_price,
            )
            self._emit_exit_event(
                pos,
                decision="ALERT",
                reason_code="FORCE_RECONCILE_FILL_CONFIRMED",
                explanation=f"Forced reconcile confirmed full fill status={broker_status}; applying close.",
                stage="exit_reconciliation",
                extra_inputs={
                    "broker_order_id": broker_oid,
                    "broker_status": broker_status,
                    "filled_qty": _filled_qty,
                    "fill_price": _fill_price,
                },
            )
            self.mark_position_closed(
                position_id,
                reason=f"quarantine_force_reconcile_broker_{broker_status}",
                fill_price=_fill_price,
                broker_order_id=broker_oid,
                local_order_id=pos.pending_exit_local_order_id,
                force=True,
            )

        elif broker_status == "partially_filled":
            # Partial fill confirmed — do NOT hard-close. Apply the confirmed
            # cumulative tranche via note_partial_exit_fill() and let
            # quantity_remaining drive closure. Only close if the fill exhausts
            # the remaining position, verified by re-reading state after the write.
            #
            # Caution: _filled_qty is treated as cumulative here, consistent with
            # note_partial_exit_fill()'s per-order watermark model. If the broker
            # returns incremental quantities on get_order(), this would misapply;
            # but the field names scanned above are all cumulative-named, which is
            # the safest available assumption.
            log.critical(
                "[%s] QUARANTINE_FORCE_RECONCILE_PARTIAL_FILL | pos=%s broker=%s | "
                "broker partial fill confirmed — applying tranche, not hard-closing | "
                "filled_qty=%s fill_price=%s",
                ticker, position_id or "?", broker_oid, _filled_qty, _fill_price,
            )
            self._emit_exit_event(
                pos,
                decision="ALERT",
                reason_code="FORCE_RECONCILE_PARTIAL_FILL_CONFIRMED",
                explanation=f"Forced reconcile confirmed partial fill; applying tranche via note_partial_exit_fill.",
                stage="exit_reconciliation",
                extra_inputs={
                    "broker_order_id": broker_oid,
                    "broker_status": broker_status,
                    "filled_qty": _filled_qty,
                    "fill_price": _fill_price,
                    "quarantine_age_sec": quarantine_age_sec,
                },
            )
            if _filled_qty and _filled_qty > 0:
                self.note_partial_exit_fill(
                    position_id,
                    qty_filled=_filled_qty,
                    fill_price=_fill_price,
                    broker_order_id=broker_oid,
                    local_order_id=pos.pending_exit_local_order_id,
                    cumulative_filled=_filled_qty,
                )
                # Re-read state after the write — only close if the tranche
                # exhausted the remaining position. get_position() uses the O(1)
                # dict index so this always reflects post-fill truth, never the
                # snapshot captured before note_partial_exit_fill() ran.
               
            else:
                # No reliable qty — the broker returned partially_filled but no
                # usable quantity. Emit alert with full context so dashboards can
                # distinguish this from unknown-status and no-order-id cases, then
                # defer to the reconciler which has the fuzzy-match logic to resolve.
                self._emit_exit_event(
                    pos,
                    decision="ALERT",
                    reason_code="FORCE_RECONCILE_PARTIAL_FILL_QTY_UNKNOWN",
                    explanation=(
                        "Partial fill confirmed but qty unknown — reconciler must resolve. "
                        "Cannot safely apply delta without cumulative quantity."
                    ),
                    stage="exit_reconciliation",
                    extra_inputs={
                        "broker_order_id": broker_oid,
                        "broker_status": broker_status,
                        "quarantine_age_sec": quarantine_age_sec,
                    },
                )

        elif broker_status in _broker_terminal:
            # Broker confirms terminal (canceled/rejected/expired) — clear in-flight
            # and allow the engine to resubmit on the next evaluation cycle.
            log.critical(
                "[%s] QUARANTINE_FORCE_RECONCILE_TERMINAL_CONFIRMED | pos=%s broker=%s | "
                "broker status=%s — clearing quarantine so engine can resubmit",
                ticker, position_id or "?", broker_oid, broker_status,
            )
            self._emit_exit_event(
                pos,
                decision="ALERT",
                reason_code="FORCE_RECONCILE_TERMINAL_CONFIRMED",
                explanation=f"Forced reconcile confirmed broker terminal status={broker_status}; clearing in-flight.",
                stage="exit_reconciliation",
                extra_inputs={"broker_order_id": broker_oid, "broker_status": broker_status},
            )
            self.clear_exit_in_flight(
                position_id,
                reason=f"quarantine_force_reconcile_broker_{broker_status}",
                broker_order_id=broker_oid,
                local_order_id=pos.pending_exit_local_order_id,
                rejected=(broker_status in {"rejected"}),
                force=True,
            )

        elif broker_status in {"open", "pending", "working", "submitted", "acknowledged"}:
            # Order is still live at broker — quarantine is correct, keep blocking.
            log.warning(
                "[%s] QUARANTINE_FORCE_RECONCILE_ORDER_STILL_LIVE | pos=%s broker=%s | "
                "broker status=%s — quarantine remains; fill monitor will complete when filled",
                ticker, position_id or "?", broker_oid, broker_status,
            )
            self._emit_exit_event(
                pos,
                decision="HOLD",
                reason_code="FORCE_RECONCILE_ORDER_STILL_LIVE",
                explanation=f"Forced reconcile: broker order still live (status={broker_status}); quarantine maintained.",
                stage="exit_reconciliation",
                extra_inputs={"broker_order_id": broker_oid, "broker_status": broker_status},
            )

        else:
            # Unrecognized or empty broker status — can't safely act; defer to reconciler.
            log.error(
                "[%s] QUARANTINE_FORCE_RECONCILE_STATUS_UNKNOWN | pos=%s broker=%s | "
                "unrecognized broker status=%r — deferring to reconciler",
                ticker, position_id or "?", broker_oid, broker_status,
            )
            self._emit_exit_event(
                pos,
                decision="ALERT",
                reason_code="FORCE_RECONCILE_STATUS_UNKNOWN",
                explanation=f"Forced reconcile: unrecognized broker status={broker_status!r}; deferring to reconciler.",
                stage="exit_reconciliation",
                extra_inputs={"broker_order_id": broker_oid, "broker_status": broker_status},
            )

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
                    "option_pnl_pct":              getattr(pos, "option_pnl_pct", 0.0),
                    "peak_pnl_pct":                getattr(pos, "peak_pnl_pct", 0.0),
                    "max_profit_seen":             getattr(pos, "max_profit_seen", 0.0),
                    "touched_profit":              getattr(pos, "touched_profit", False),
                    "qty_remaining":               getattr(pos, "quantity_remaining", 0),
                    "quantity_remaining":          getattr(pos, "quantity_remaining", 0),
                    "quantity":                    getattr(pos, "quantity", 0),
                    "scale_outs_done":             getattr(pos, "scale_outs_done", 0),
                    "exit_in_flight":              getattr(pos, "exit_in_flight", False),
                    "pending_exit_action":         getattr(pos, "pending_exit_action", ""),
                    "pending_exit_qty":            getattr(pos, "pending_exit_qty", 0),
                    "pending_exit_filled_qty":     getattr(pos, "pending_exit_filled_qty", 0),
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
            raise RuntimeError(
                f"position invariant failed {context}: "
                f"quantity_remaining={pos.quantity_remaining} quantity={pos.quantity} pos_id={pos.position_id}"
            )
        if pos.pending_exit_filled_qty < 0:
            raise RuntimeError(
                f"position invariant failed {context}: "
                f"pending_exit_filled_qty={pos.pending_exit_filled_qty} pos_id={pos.position_id}"
            )
        if pos.pending_exit_qty > 0 and pos.pending_exit_filled_qty > pos.pending_exit_qty:
            raise RuntimeError(
                f"position invariant failed {context}: "
                f"pending_exit_filled_qty={pos.pending_exit_filled_qty} > "
                f"pending_exit_qty={pos.pending_exit_qty} pos_id={pos.position_id}"
            )

    def _can_submit_exit(
        self,
        pos: ManagedPosition,
        now_utc: datetime,
        *,
        reason: str = "",
        allow_inflight_override: bool = False,
    ) -> bool:
        """Centralized gate for every path that can submit an exit order."""
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

            pos.exit_identity_quarantine = True
            severity    = "ERROR" if identity_age >= 20.0 else "HOLD"
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
                pos, severity, reason_code,
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
                pos, "HOLD", "EXIT_SIGNAL_BLOCKED_IN_FLIGHT",
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

            if not getattr(pos, "pending_exit_replace_allowed", False):
                self._emit_exit_event(
                    pos, "HOLD", "EXIT_OVERRIDE_NEEDS_CANCEL_OR_RECONCILE_PROOF",
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

            # Consume one-shot replacement authorization.
            pos.pending_exit_replace_allowed    = False
            pos.pending_exit_replace_reason     = ""
            pos.pending_exit_replace_allowed_ts = None

            stale_enough = flight_sec >= 20.0
            new_code     = _classify_exit_decision(ExitDecision("CLOSE_ALL", 0, reason or "", "IMMEDIATE"))
            pending_code = _classify_exit_decision(ExitDecision(pos.pending_exit_action or "CLOSE_ALL", 0, pos.pending_exit_reason or "", "IMMEDIATE"))
            new_pri      = _exit_priority(new_code)
            pending_pri  = _exit_priority(pending_code)

            # FIX-4: replacement_proof variable removed. The three guard blocks
            # previously prefixed with '(not replacement_proof) and' were permanently
            # dead because replacement_proof was always True. They now execute
            # unconditionally so stale-time, equivalent-runner, and non-emergency
            # checks actually gate the override decision.
            if not stale_enough and pending_code != "UNKNOWN_EXIT" and new_pri >= pending_pri:
                self._emit_exit_event(
                    pos, "HOLD", "EXIT_OVERRIDE_DENIED_PRECEDENCE",
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
            emergency_reason         = _is_runner_protective_reason(reason)
            already_same_runner_protection = _is_same_or_equivalent_runner_protection(
                pos.pending_exit_reason, reason,
            )

            if already_same_runner_protection and not stale_enough and not higher_priority_override:
                self._emit_exit_event(
                    pos, "HOLD", "EXIT_OVERRIDE_DENIED_EQUIVALENT_PENDING",
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

            if not stale_enough and not emergency_reason and not higher_priority_override:
                self._emit_exit_event(
                    pos, "HOLD", "EXIT_OVERRIDE_DENIED",
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
                pos, "ALERT", "EXIT_INFLIGHT_OVERRIDE_ALLOWED",
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
                pos.ticker, flight_sec, reason, pos.pending_exit_reason,
            )

        if pos.last_rejection_ts is not None:
            # FIX-8: last_rejection_ts is now Optional[datetime]; compute elapsed with datetime arithmetic.
            try:
                elapsed = (datetime.now(timezone.utc) - pos.last_rejection_ts).total_seconds()
            except Exception:
                elapsed = 999.0
            if elapsed < 30:
                self._emit_exit_event(
                    pos, "HOLD", "EXIT_REJECTION_COOLDOWN",
                    f"Exit suppressed during rejection cooldown: {elapsed:.0f}s",
                    extra_inputs={"cooldown_elapsed_sec": elapsed},
                )
                return False
            pos.last_rejection_ts  = None
            pos.last_exit_rejected = False

        return True

    def hydrate_pending_exit_identity_from_db(self, pos: ManagedPosition) -> bool:
        """
        Reattach an active EXIT order from DB to the in-memory ManagedPosition.

        Fixes restart/reseed cases where:
          pos.exit_in_flight = True
          pos.pending_exit_local_order_id  = ""   ← blank
          pos.pending_exit_broker_order_id = ""   ← blank
        but the DB has a real EXIT_SUBMITTED / EXIT_ACKNOWLEDGED order.

        Without this, the engine cannot monitor, cancel, replace, or avoid
        resubmitting a broker order it has forgotten about.
        """
        if not pos or not getattr(pos, "position_id", ""):
            return False
        try:
            from ap.db import conn, run_with_retry

            def _fn():
                with conn() as c:
                    c.execute(
                        """
                        SELECT local_order_id, broker_order_id, status,
                               qty, filled_qty, created_ts, submitted_ts, updated_ts
                        FROM orders
                        WHERE client_id = %s
                          AND position_id = %s
                          AND kind = 'EXIT'
                          AND status IN (
                              'EXIT_REQUESTED','EXIT_SUBMITTED',
                              'EXIT_ACKNOWLEDGED','EXIT_PARTIAL_FILL'
                          )
                        ORDER BY updated_ts DESC NULLS LAST,
                                 created_ts DESC NULLS LAST
                        LIMIT 1
                        """,
                        (getattr(pos, "client_id", None) or self._email or "", pos.position_id),
                    )
                    return c.fetchone()

            row = run_with_retry(_fn)
            if not row:
                return False

            row = dict(row)
            local_id  = str(row.get("local_order_id")  or "")
            broker_id = str(row.get("broker_order_id") or "")
            status    = str(row.get("status")           or "")

            pos.exit_in_flight                = True
            pos.pending_exit_local_order_id   = local_id
            pos.pending_exit_broker_order_id  = broker_id
            pos.pending_exit_qty              = int(row.get("qty")        or getattr(pos, "quantity_remaining", 0) or 0)
            pos.pending_exit_filled_qty       = int(row.get("filled_qty") or 0)

            log.warning(
                "[%s] HYDRATED ACTIVE EXIT IDENTITY | pos=%s local=%s broker=%s status=%s",
                pos.ticker, pos.position_id, local_id, broker_id, status,
            )
            return True
        except Exception as exc:
            log.error(
                "[%s] hydrate_pending_exit_identity_from_db failed | pos=%s | %s",
                getattr(pos, "ticker", "?"), getattr(pos, "position_id", "?"),
                exc, exc_info=True,
            )
            return False

    def seed_from_db(self, position_manager):
        """Re-hydrate in-memory positions from DB on startup."""
        try:
            rows = position_manager.get_active_positions()
            if not rows:
                log.info("seed_from_db: no active positions to seed")
                return
            seeded = 0
            for row in rows:
                try:
                    original_qty = int(row.get("qty", 1) or 1)
                    _raw_ticker = str(row.get("underlying", "") or row.get("symbol", "") or "")
                    _contract_sym = str(row.get("contract", "") or "")
                    _ticker = _normalize_ticker(_raw_ticker, _contract_sym)
                    if _ticker != _raw_ticker:
                        log.warning(
                            "seed_from_db: ticker normalized '%s' -> '%s' | contract=%s | pos_id=%s",
                            _raw_ticker, _ticker, _contract_sym, row.get("id"),
                        )
                    _underlying_entry = float(row.get("underlying_entry", 0) or 0)

                    mp = ManagedPosition(
                        ticker=_ticker,
                        option_symbol=_contract_sym,
                        side=row.get("direction", "CALL"),
                        quantity=original_qty,
                        entry_price=float(row.get("avg_fill", 0) or 0),
                        underlying_entry=_underlying_entry,
                        underlying_target=float(row.get("target_underlying") or 0),
                        underlying_stop=float(row.get("stop_underlying") or 0),
                        position_id=str(row.get("id") or ""),
                        client_id=str(row.get("client_id") or ""),
                        signal_id=str(row.get("signal_id") or ""),
                    )
                    mp.current_option_price = float(row.get("avg_fill", 0) or 0)
                    mp.scale_outs_done      = int(row.get("scale_outs_done", 0) or 0)
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
                    # Reattach any active broker exit order so engine can
                    # monitor/cancel/replace without resubmitting blindly.
                    self.hydrate_pending_exit_identity_from_db(mp)
                    seeded += 1
                except Exception as e:
                    log.warning("seed_from_db: skipping row %s: %s", row.get("id"), e)
            log.info("seed_from_db: seeded %d position(s) into exit engine", seeded)
        except Exception as e:
            log.error("seed_from_db FAILED — open positions have NO exit protection: %s", e)

    def _exit_loop(self):
        # FIX-9: capture healer reference once before the loop so we don't
        # re-import on every 8-second iteration. The module cache makes
        # re-imports cheap but can return a stale reference if healer is
        # reregistered; capturing here avoids that edge case entirely.
        try:
            from ap.self_healing import get_healer as _get_healer_fn
            _cached_healer = _get_healer_fn()
        except Exception:
            _cached_healer = None

        while self._running:
            try:
                self._check_all_positions()
            except Exception as e:
                log.error("Exit engine error: %s", e, exc_info=True)

            try:
                if _cached_healer is not None and self._email:
                    _cached_healer.heartbeat(self._email, "exit_engine")
            except Exception as _e:
                log.debug("exit_engine_heartbeat_failed: %s", _e)

            # QPM: PositionQuoteMonitor can wake the exit loop immediately
            # when a material quote move arrives; otherwise this still behaves
            # like the original 8-second polling safety net.
            self._quote_arrived_event.wait(timeout=POLL_INTERVAL_SEC)
            self._quote_arrived_event.clear()

    # ── Broker precheck helpers ──────────────────────────────────────────────

    @staticmethod
    def _parse_occ_side(symbol: str) -> str:
        """Parse CALL/PUT from OCC option symbol using the standard C/P right character.
        OCC format: {underlying}{YYMMDD}{C|P}{strike_padded_8digits}
        E.g. RIVN260612P00016500 → PUT   C260612C00136000 → CALL
        Never use string-contains "C" or "P" — COIN/BAC/etc would be misread.
        """
        import re as _re
        m = _re.match(r'^([A-Z]+)(\d{6})([CP])(\d{8})$', symbol.strip().upper())
        if m:
            return "CALL" if m.group(3) == "C" else "PUT"
        # fallback: walk past root letters + 6 digits
        s = symbol.strip().upper()
        i = 0
        while i < len(s) and s[i].isalpha(): i += 1  # skip root
        i += 6                                          # skip YYMMDD
        if i < len(s) and s[i] in ("C", "P"):
            return "CALL" if s[i] == "C" else "PUT"
        return "UNKNOWN"

    @staticmethod
    def _underlying_from_occ(symbol: str) -> str:
        """Extract underlying ticker from OCC symbol (letters before the date)."""
        import re as _re
        m = _re.match(r'^([A-Z]+)\d{6}[CP]\d+$', symbol.strip().upper())
        return m.group(1) if m else symbol.strip().upper()[:5]

    def _load_db_position_row(self, sym: str) -> dict | None:
        """Look up an active positions row for this client + contract symbol.

        P0-PARTIAL-CLOSE: status filter expanded to include PARTIAL and ACTIVE,
        AND adds a quantity_remaining safety guard so a row incorrectly marked
        CLOSED but with remaining qty is still found and managed.
        """
        try:
            from ap.db import conn, run_with_retry
            def _q():
                with conn() as c:
                    c.execute(
                        """
                        SELECT id, underlying, contract, option_symbol, side, direction,
                               qty, quantity_remaining, avg_fill, entry_price,
                               entry_ts, status, signal_id
                        FROM positions
                        WHERE client_id = %s
                          AND (
                            UPPER(COALESCE(status,'')) IN ('OPEN','CLOSING','PARTIAL','ACTIVE')
                            OR COALESCE(quantity_remaining, 0) > 0
                          )
                          AND (
                            UPPER(contract)         = UPPER(%s)
                            OR UPPER(option_symbol) = UPPER(%s)
                          )
                        ORDER BY entry_ts DESC NULLS LAST
                        LIMIT 1
                        """,
                        (self._email, sym, sym),
                    )
                    row = c.fetchone()
                    if row:
                        cols = [d[0] for d in c.description]
                        return dict(zip(cols, row))
            return run_with_retry(_q)
        except Exception as _de:
            log.warning("[exit_eng] _load_db_position_row %s failed: %s", sym, _de)
            return None

    def _upsert_broker_position_to_db(self, sym: str, bp: dict) -> str | None:
        """
        Create a minimal OPEN positions row from broker data. Returns row id or None.

        Production-safe: does NOT write optional columns (source, account_id, etc.)
        that may not exist in the production schema. Uses only guaranteed columns.

        ON CONFLICT fallback: if INSERT returns None (row already exists), re-query
        by client_id + contract so repair can proceed with the existing id.
        """
        try:
            from ap.db import conn, run_with_retry
            underlying = self._underlying_from_occ(sym)
            side       = self._parse_occ_side(sym)
            qty        = int(bp.get("quantity") or 0)
            cost_basis = float(bp.get("cost_basis") or 0)
            entry_px   = round(cost_basis / max(qty, 1) / 100, 6) if qty > 0 and cost_basis > 0 else 0.0
            entry_ts   = bp.get("date_acquired")

            def _ins():
                with conn() as c:
                    c.execute(
                        """
                        INSERT INTO positions (
                            client_id, underlying, contract, option_symbol,
                            side, direction,
                            qty, quantity_remaining,
                            entry_price, avg_fill,
                            status, entry_ts, updated_at
                        ) VALUES (
                            %s, %s, %s, %s,
                            %s, %s,
                            %s, %s,
                            %s, %s,
                            'OPEN', %s, NOW()
                        )
                        ON CONFLICT DO NOTHING
                        RETURNING id
                        """,
                        (self._email, underlying, sym, sym,
                         side, side,
                         qty, qty,
                         entry_px, entry_px,
                         entry_ts),
                    )
                    row = c.fetchone()
                    if row:
                        return str(row[0])
                    # ON CONFLICT DO NOTHING — row already exists; re-query to get id
                    c.execute(
                        """
                        SELECT id FROM positions
                        WHERE client_id = %s
                          AND (
                            UPPER(contract)         = UPPER(%s)
                            OR UPPER(option_symbol) = UPPER(%s)
                          )
                        ORDER BY entry_ts DESC NULLS LAST, updated_at DESC NULLS LAST
                        LIMIT 1
                        """,
                        (self._email, sym, sym),
                    )
                    existing = c.fetchone()
                    if existing:
                        log.info(
                            "[exit_eng] _upsert_broker_position_to_db ON CONFLICT re-query "
                            "returned existing id for %s client=%s",
                            sym, self._email,
                        )
                        return str(existing[0])
                    return None
            return run_with_retry(_ins)
        except Exception as _ue:
            log.error("[exit_eng] _upsert_broker_position_to_db %s failed: %s: %s",
                      sym, type(_ue).__name__, _ue)
            return None

    def _managed_position_from_row(
        self, row: dict, qty_override: int = 0, *, prefer_qty_override: bool = False
    ) -> "ManagedPosition":
        """
        Build a ManagedPosition from a DB row (or minimal broker data dict).

        prefer_qty_override=False (default — DB-seed / normal load):
            Preserves existing safe behavior. quantity_remaining=0 stays 0.
            Does not resurrect closed DB-only rows.

        prefer_qty_override=True (broker precheck mode):
            When qty_override > 0, broker qty wins over stale DB quantity_remaining.
            Stale quantity_remaining=0 is overridden by broker truth.
            Only used inside _broker_position_precheck().
        """
        sym       = str(row.get("contract") or row.get("option_symbol") or "")
        ticker    = str(row.get("underlying") or self._underlying_from_occ(sym))
        side_raw  = str(row.get("side") or row.get("direction") or "").upper()
        side      = side_raw if side_raw in ("CALL", "PUT") else self._parse_occ_side(sym)
        _qr = row.get("quantity_remaining")
        _db_qty_before = int(_qr if _qr is not None else (row.get("qty") or 0))
        # Broker-truth mode: qty_override wins when prefer_qty_override=True and override>0.
        # Normal DB-seed mode: preserve P0-PARTIAL-CLOSE behavior (qr=0 stays 0).
        if prefer_qty_override and qty_override and int(qty_override) > 0:
            qty = int(qty_override)
        else:
            # P0-PARTIAL-CLOSE: do NOT use `or` — quantity_remaining=0 is a valid
            # value meaning fully closed. Falling back to qty would load original
            # entry size into the exit engine for a row that has zero contracts left.
            qty = int(_qr if _qr is not None else (row.get("qty") or qty_override or 1))
        entry_px  = float(row.get("entry_price") or row.get("avg_fill") or 0.0)
        pos_id    = str(row.get("id") or "")
        sig_id    = str(row.get("signal_id") or "")
        opened_at = None
        try:
            import datetime as _dt
            raw_ts = row.get("entry_ts")
            if raw_ts:
                if isinstance(raw_ts, str):
                    opened_at = _dt.datetime.fromisoformat(raw_ts.replace("Z","+00:00"))
                elif isinstance(raw_ts, _dt.datetime):
                    opened_at = raw_ts
        except Exception:
            pass
        if opened_at and opened_at.tzinfo is None:
            import datetime as _dt
            opened_at = opened_at.replace(tzinfo=_dt.timezone.utc)

        _now = datetime.now(timezone.utc)
        mp = ManagedPosition(
            ticker           = ticker,
            option_symbol    = sym,
            side             = side,
            quantity         = qty,
            entry_price      = entry_px,
            underlying_entry = 0.0,   # unknown from broker data — exits use current price
            underlying_target= 0.0,   # no target on repair — trailing/EOD rules still apply
            underlying_stop  = 0.0,   # no stop on repair — EOD/expiry rules protect
            position_id      = pos_id,
            client_id        = self._email,
            signal_id        = sig_id,
            quantity_remaining = qty,
            opened_at        = opened_at or _now,
        )
        # Final broker-truth enforcement: if prefer_qty_override is active,
        # ensure both quantity fields match broker qty regardless of constructor defaults.
        if prefer_qty_override and qty_override and int(qty_override) > 0:
            mp.quantity            = int(qty_override)
            mp.quantity_remaining  = int(qty_override)
            if _db_qty_before != int(qty_override):
                log.info(
                    "[exit_eng] EXIT_BROKER_POSITION_DB_QTY_STALE_REPAIRED_IN_MEMORY "
                    "sym=%s db_qty_before=%d broker_qty=%d loaded_qty=%d",
                    sym, _db_qty_before, int(qty_override), mp.quantity_remaining,
                )
        return mp

    def _fetch_broker_quote(self, sym: str) -> dict:
        """
        Fetch live bid/ask/mark for an option symbol from Tradier quotes API.
        Returns dict with keys: bid, ask, mid, last. All default 0.0 on failure.
        Never raises — exits must not crash on a missing quote.

        Money-safety invariant: never guess a broker host. If the broker
        instance cannot prove its quote base URL or token, fail closed and
        return an empty quote instead of implicitly routing to live Tradier.
        """
        _empty = {"bid": 0.0, "ask": 0.0, "mid": 0.0, "last": 0.0}
        try:
            import requests as _req
            base = getattr(self.broker, "base_url", None)
            token = (getattr(self.broker, "access_token", None)
                     or getattr(self.broker, "_access_token", None))
            if not base:
                log.warning(
                    "[exit_eng] BROKER_QUOTE_BASE_URL_MISSING client=%s sym=%s "
                    "— refusing implicit live Tradier fallback; returning empty quote",
                    getattr(self, "_email", "") or "unknown",
                    sym,
                )
                return _empty
            if not token:
                log.warning(
                    "[exit_eng] BROKER_QUOTE_TOKEN_MISSING client=%s sym=%s base=%s "
                    "— returning empty quote",
                    getattr(self, "_email", "") or "unknown",
                    sym,
                    base,
                )
                return _empty
            resp = _req.get(
                f"{base}/v1/markets/quotes",
                params={"symbols": sym, "greeks": "false"},
                headers={"Authorization": f"Bearer {token}",
                         "Accept": "application/json"},
                timeout=5,
            )
            if resp.status_code != 200:
                return _empty
            data = resp.json()
            q = (data or {}).get("quotes", {}).get("quote", {})
            if isinstance(q, list):
                q = q[0] if q else {}
            mark = float(q.get("mark") or 0.0)
            bid  = float(q.get("bid")  or 0.0)
            ask  = float(q.get("ask")  or 0.0)
            last = float(q.get("last") or 0.0)
            # mark → mid(bid,ask) → last → bid → ask (dashboard-aligned fallback)
            mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else last
            return {"mark": mark, "bid": bid, "ask": ask, "mid": mid, "last": last}
        except Exception:
            return _empty

    def _broker_position_precheck(self) -> bool:
        """
        Before every exit cycle: fetch broker positions and repair/load any
        that are missing from the exit engine. After loading, set live quote
        data and seed peak_pnl_pct / touched_profit so the normal exit loop
        can evaluate them in the same cycle.

        Safety rules:
          - Never submit an exit order here — that is the exit loop's job.
          - Never assume flat on a broker fetch failure (auth error ≠ no positions).
          - EXIT_UNSAFE_BROKER_TRUTH_UNAVAILABLE logged on broker fetch failure.
          - EXIT_UNSAFE_BROKER_POSITION_REPAIR_FAILED logged on repair failure.
          - Already-tracked engine positions are never duplicated.
          - Broker qty wins over stale DB quantity_remaining=0 (PR#P0-exit-broker-truth).
          - Quote failure never blocks position loading (quote_status=QUOTE_UNAVAILABLE).
          - source column never written — production schema may not have it.
        """
        _account_id = getattr(self.broker, "account_id", "?")

        if not self.broker or not hasattr(self.broker, "list_positions"):
            return True

        # ── 0. Count current engine positions for structured logging ──────────
        with self._lock:
            _local_count = sum(
                1 for p in self._positions
                if not p.closed and int(p.quantity_remaining or 0) > 0
            )

        # ── 1. Fetch broker positions ─────────────────────────────────────────
        try:
            broker_positions = self.broker.list_positions() or []
        except Exception as _bp_err:
            log.error(
                "[exit_eng] EXIT_UNSAFE_BROKER_TRUTH_UNAVAILABLE "
                "client=%s account=%s error=%s error_type=%s "
                "broker_truth_available=false local_engine_position_count=%d — "
                "continuing with local engine state; exits not blocked; "
                "never assumes broker is flat",
                self._email, _account_id,
                _bp_err, type(_bp_err).__name__,
                _local_count,
            )
            return False

        broker_map = {
            str(p.get("symbol") or "").upper(): p
            for p in broker_positions
            if int(p.get("quantity") or 0) > 0
        }
        broker_syms = set(broker_map.keys())

        # ── 2. Current engine symbols ─────────────────────────────────────────
        with self._lock:
            engine_syms = {
                str(getattr(p, "option_symbol", "") or "").upper()
                for p in self._positions
                if not p.closed and int(p.quantity_remaining or 0) > 0
            }

        missing_from_engine = broker_syms - engine_syms

        log.info(
            "[exit_eng] EXIT_BROKER_PRECHECK_START "
            "client=%s account=%s broker_position_count=%d engine_position_count=%d "
            "broker_symbols=%s engine_symbols=%s missing_from_engine=%s",
            self._email, _account_id,
            len(broker_syms), len(engine_syms),
            sorted(broker_syms), sorted(engine_syms),
            sorted(missing_from_engine),
        )

        if not missing_from_engine:
            # All broker positions already tracked — log summary and return
            log.info(
                "[exit_eng] EXIT_BROKER_PRECHECK_SUMMARY "
                "client=%s account=%s broker_position_count=%d "
                "engine_position_count=%d missing_from_engine=0 "
                "all_broker_positions_tracked=true",
                self._email, _account_id, len(broker_syms), len(engine_syms),
            )
            return True

        # ── 3. Repair / load each missing position ────────────────────────────
        repaired_syms               = []   # DB insert/re-query confirmed real id
        loaded_db_syms              = []   # existing DB row found and loaded
        repair_failed_syms          = []   # add_position never called
        engine_loaded_synthetic_syms = []  # engine loaded but no confirmed DB row

        for sym in sorted(missing_from_engine):
            bp         = broker_map[sym]
            broker_qty = int(bp.get("quantity") or 0)
            cost_basis = float(bp.get("cost_basis") or 0)
            entry_px   = cost_basis / max(broker_qty, 1) / 100

            db_seen            = False
            db_repaired        = False
            db_status_before   = None
            db_qty_before      = None
            pos                = None
            repair_failed_reason = ""

            log.warning(
                "[exit_eng] EXIT_BROKER_POSITION_MISSING_FROM_ENGINE "
                "client=%s account=%s contract_symbol=%s broker_qty=%d entry_px=%.4f "
                "— attempting broker-truth repair",
                self._email, _account_id, sym, broker_qty, entry_px,
            )

            # ── 3a. Try DB load ───────────────────────────────────────────────
            db_row = self._load_db_position_row(sym)
            if db_row:
                db_seen          = True
                db_status_before = db_row.get("status")
                db_qty_before    = int(db_row.get("quantity_remaining") or 0)
                log.info(
                    "[exit_eng] EXIT_BROKER_POSITION_DB_ROW_FOUND "
                    "client=%s contract_symbol=%s db_status_before=%s "
                    "db_qty_before=%d broker_qty=%d",
                    self._email, sym, db_status_before, db_qty_before, broker_qty,
                )
                # Repair stale quantity_remaining=0 in DB before loading into engine
                if db_qty_before == 0 and broker_qty > 0:
                    try:
                        from ap.db import conn, run_with_retry
                        def _repair_qty(pid=str(db_row.get("id") or ""), bq=broker_qty):
                            with conn() as c:
                                c.execute(
                                    """
                                    UPDATE positions
                                    SET quantity_remaining = %s,
                                        qty               = GREATEST(COALESCE(qty, 0), %s),
                                        status            = 'OPEN',
                                        updated_at        = NOW()
                                    WHERE id = %s AND client_id = %s
                                    """,
                                    (bq, bq, pid, self._email),
                                )
                        run_with_retry(_repair_qty)
                        db_repaired = True
                        log.info(
                            "[exit_eng] EXIT_BROKER_POSITION_DB_QTY_STALE_REPAIRED "
                            "client=%s contract_symbol=%s db_qty_before=%d "
                            "broker_qty=%d db_repaired=true",
                            self._email, sym, db_qty_before, broker_qty,
                        )
                    except Exception as _dre:
                        # Non-fatal — still load with broker qty even if DB repair fails
                        log.warning(
                            "[exit_eng] EXIT_BROKER_POSITION_DB_QTY_STALE_REPAIR_FAILED "
                            "client=%s contract_symbol=%s db_qty_before=%d "
                            "broker_qty=%d error=%s: %s "
                            "— loading with broker qty anyway",
                            self._email, sym, db_qty_before, broker_qty,
                            type(_dre).__name__, _dre,
                        )
                try:
                    # Broker-truth mode: prefer_qty_override ensures stale qr=0 is overridden
                    pos = self._managed_position_from_row(
                        db_row,
                        qty_override=broker_qty,
                        prefer_qty_override=True,
                    )
                    self.add_position(pos)
                    loaded_db_syms.append(sym)
                except Exception as _le:
                    log.warning(
                        "[exit_eng] DB row load failed for %s: %s: %s — trying upsert",
                        sym, type(_le).__name__, _le,
                    )
                    pos = None

            # ── 3b. Create DB row from broker truth if still no pos ───────────
            if pos is None:
                try:
                    new_id = self._upsert_broker_position_to_db(sym, bp)

                    # Determine real id: use DB id when available; otherwise a
                    # synthetic id so the engine can track this position without
                    # claiming a DB row exists.
                    if new_id:
                        _pos_id    = new_id
                        db_repaired = True
                    else:
                        # Upsert returned None — engine still loads with synthetic id
                        # so the position is visible and will evaluate this cycle.
                        # db_repaired stays False: no confirmed DB row.
                        _pos_id             = f"broker-repair-{self._email}-{sym}"
                        db_repaired         = False
                        repair_failed_reason = (
                            "db_upsert_returned_no_id_engine_loaded_synthetic"
                        )
                        log.warning(
                            "[exit_eng] EXIT_BROKER_POSITION_UPSERT_NO_ID "
                            "client=%s contract_symbol=%s — using synthetic position_id; "
                            "engine will still load and evaluate this position",
                            self._email, sym,
                        )

                    minimal_row = {
                        "id":                 _pos_id,
                        "contract":           sym,
                        "option_symbol":      sym,
                        "underlying":         self._underlying_from_occ(sym),
                        "side":               self._parse_occ_side(sym),
                        "qty":                broker_qty,
                        "quantity_remaining": broker_qty,
                        "entry_price":        entry_px,
                        "avg_fill":           entry_px,
                        "entry_ts":           bp.get("date_acquired"),
                    }
                    pos = self._managed_position_from_row(
                        minimal_row,
                        qty_override=broker_qty,
                        prefer_qty_override=True,
                    )
                    self.add_position(pos)
                    if new_id:
                        repaired_syms.append(sym)              # confirmed DB row
                    else:
                        engine_loaded_synthetic_syms.append(sym)  # engine-only, no DB row
                except Exception as _re_err:
                    repair_failed_syms.append(sym)
                    repair_failed_reason = f"{type(_re_err).__name__}: {_re_err}"
                    log.error(
                        "[exit_eng] EXIT_UNSAFE_BROKER_POSITION_REPAIR_FAILED "
                        "client=%s account=%s contract_symbol=%s error=%s",
                        self._email, _account_id, sym, repair_failed_reason,
                    )
                    log.error(
                        "[exit_eng] EXIT_BROKER_POSITION_ADDED_TO_ENGINE "
                        "client=%s account=%s contract_symbol=%s "
                        "db_seen_before=%s db_status_before=%s db_qty_before=%s "
                        "broker_qty=%d loaded_qty=0 db_repaired=%s "
                        "repair_failed_reason=%s added_to_engine=false "
                        "will_evaluate_this_cycle=false quote_status=N/A",
                        self._email, _account_id, sym,
                        db_seen, db_status_before, db_qty_before,
                        broker_qty, db_repaired, repair_failed_reason,
                    )
                    continue

            # ── 3c. Verify loaded_qty > 0 (fail-safe broker-truth enforcement) ─
            loaded_qty = int(getattr(pos, "quantity_remaining", 0) or 0)
            if loaded_qty <= 0 and broker_qty > 0:
                # Should not happen after prefer_qty_override, but enforce as safety net
                log.warning(
                    "[exit_eng] EXIT_BROKER_PRECHECK_QTY_ZERO_FORCE "
                    "client=%s contract_symbol=%s loaded_qty=%d broker_qty=%d "
                    "— forcing broker qty",
                    self._email, sym, loaded_qty, broker_qty,
                )
                pos.quantity           = broker_qty
                pos.quantity_remaining = broker_qty
                loaded_qty             = broker_qty

            # ── 3d. Fetch live quote and seed price state ─────────────────────
            quote       = self._fetch_broker_quote(sym)
            # mark → mid(bid,ask) → last → bid → ask (dashboard-aligned fallback)
            broker_mark = (quote.get("mark") or 0.0) or (quote.get("mid") or 0.0) or                           (quote.get("last") or 0.0) or (quote.get("bid") or 0.0) or                           (quote.get("ask") or 0.0)
            broker_bid  = quote.get("bid", 0.0)
            broker_ask  = quote.get("ask", 0.0)
            _quote_status = "OK" if broker_mark > 0 else "QUOTE_UNAVAILABLE"

            # Position is ALWAYS added regardless of quote availability
            if broker_mark > 0:
                pos.current_option_price = broker_mark
                if hasattr(pos, "current_bid"):
                    pos.current_bid = broker_bid
                if hasattr(pos, "current_ask"):
                    pos.current_ask = broker_ask
            else:
                log.warning(
                    "[exit_eng] EXIT_BROKER_PRECHECK_QUOTE_UNAVAILABLE "
                    "client=%s contract_symbol=%s quote_status=QUOTE_UNAVAILABLE "
                    "— position still added to engine; will_evaluate_this_cycle=true",
                    self._email, sym,
                )

            # Seed peak P&L / touched_profit if broker price shows a gain
            broker_pnl_pct = 0.0
            if pos.entry_price > 0 and broker_mark > 0:
                broker_pnl_pct = (broker_mark - pos.entry_price) / pos.entry_price
                if broker_pnl_pct > 0:
                    if broker_pnl_pct > pos.peak_pnl_pct:
                        pos.peak_pnl_pct = broker_pnl_pct
                    pos.touched_profit = True

            # ── 3e. Required structured log ────────────────────────────────────
            log.info(
                "[exit_eng] EXIT_BROKER_POSITION_ADDED_TO_ENGINE "
                "client=%s account=%s contract_symbol=%s "
                "db_seen_before=%s db_status_before=%s db_qty_before=%s "
                "broker_qty=%d loaded_qty=%d db_repaired=%s repair_failed_reason=%s "
                "added_to_engine=true will_evaluate_this_cycle=true quote_status=%s",
                self._email, _account_id, sym,
                db_seen, db_status_before, db_qty_before,
                broker_qty, loaded_qty, db_repaired, repair_failed_reason or "", _quote_status,
            )
            log.info(
                "[exit_eng] EXIT_ENGINE_REPAIRED_BROKER_POSITION_AND_EVALUATED_EXIT "
                "client=%s account=%s contract_symbol=%s broker_qty=%d "
                "broker_cost_basis=%.2f broker_mark_or_bid=%.4f broker_pnl_pct=%.2f "
                "engine_seen_before=false db_seen_before=%s db_repaired=%s "
                "exit_rule_triggered=false exit_order_submitted=false "
                "reason_no_exit=position_loaded_for_normal_exit_loop_evaluation "
                "will_evaluate_this_cycle=true quote_status=%s",
                self._email, _account_id, sym,
                broker_qty, cost_basis, broker_mark, broker_pnl_pct,
                db_seen, db_repaired, _quote_status,
            )

        # ── 4. Summary audit log ──────────────────────────────────────════════
        log.info(
            "[exit_eng] EXIT_BROKER_PRECHECK_SUMMARY "
            "client=%s account=%s broker_position_count=%d broker_symbols=%s "
            "engine_position_count=%d engine_symbols=%s missing_from_engine=%s "
            "loaded_from_db=%s repaired_from_broker=%s "
            "engine_loaded_synthetic=%s repair_failed=%s",
            self._email, _account_id,
            len(broker_syms), sorted(broker_syms),
            len(engine_syms), sorted(engine_syms),
            sorted(missing_from_engine),
            loaded_db_syms, repaired_syms,
            engine_loaded_synthetic_syms, repair_failed_syms,
        )

        return len(repair_failed_syms) == 0

    def _check_all_positions(self):
        today_et = _et_session_date()

        # Broker truth precheck: verify engine positions match broker before evaluating exits.
        # Prevents "no positions" assumption when DB/engine missed a fill.
        # EXIT_UNSAFE_BROKER_TRUTH_UNAVAILABLE is logged if check fails — execution continues.
        try:
            self._broker_position_precheck()
        except Exception as _pce:
            log.warning("[exit_eng] _broker_position_precheck error (non-blocking): %s", _pce)

        # FIX-3: expired contract cleanup now runs inside self._lock.
        # Previously this block iterated and reassigned self._positions without
        # the lock — a concurrent add_position or fill callback could corrupt
        # the list or silently drop a newly added position.
        with self._lock:
            to_remove = []
            for pos in self._positions:
                sym = getattr(pos, "option_symbol", "") or ""
                try:
                    exp = _option_expiration_date(sym)
                    if exp and exp < today_et:
                        log.warning(
                            "[exit_eng] EXPIRED CONTRACT detected | %s exp=%s today_et=%s — local engine cleanup",
                            sym, exp.isoformat(), today_et.isoformat(),
                        )
                        self._emit_exit_event(
                            pos,
                            decision="ALERT",
                            reason_code="EXPIRED_CONTRACT_LOCAL_CLEANUP",
                            explanation=f"Expired contract removed from exit engine tracking: {sym}",
                            stage="system_alert",
                            extra_inputs={
                                "expiration": exp.isoformat(),
                                "session_date_et": today_et.isoformat(),
                            },
                        )
                        pos.closed       = True
                        pos.close_reason = "expired_contract_local_cleanup"
                        to_remove.append(pos)
                except Exception as _exp_err:
                    log.debug("[exit_eng] Expired-contract cleanup check failed for %s: %s", sym, _exp_err)

            if to_remove:
                self._positions = [p for p in self._positions if not p.closed]
                # P1: prune expired positions from O(1) index.
                for _ep in to_remove:
                    self._positions_by_id.pop(_ep.position_id, None)
                log.info("[exit_eng] Removed %d expired contract(s) from engine", len(to_remove))

        try:
            self._run_sentinels()
        except Exception as _se:
            log.debug("[exit_eng] Sentinel error (non-critical): %s", _se)

        kill_active = False
        if self._kill_switch_fn:
            try:
                kill_active = bool(self._kill_switch_fn())
            except Exception as _ks_err:
                log.warning("Exit engine kill-switch check failed; continuing exit evaluation: %s", _ks_err)
                kill_active = False
        if kill_active:
            log.warning(
                "Exit engine kill switch active — continuing exit evaluation; "
                "risk-reducing exits remain enabled"
            )

        # ── Quote Authority: Exit Engine is a PURE CONSUMER ──────────────────
        # QPM is the single authorized writer for all quote fields.
        # Exit Engine NEVER fetches quotes independently — doing so caused a
        # race condition where our stale fetch overwrote QPM's fresher data.
        # We read from QUOTES (written by QPM) and from the QPM snapshot path
        # (apply_quote_snapshots, called above). If QPM has no fresh snapshot
        # for a position, we hold — we do not guess with stale broker data.
        try:
            from ap_quote_authority import QUOTES as _QUOTES
            _quote_authority_available = True
        except ImportError:
            _QUOTES = None
            _quote_authority_available = False

        # Emit health heartbeat so the registry knows exit engine is alive.
        try:
            from ap_health_registry import HEALTH as _EE_HEALTH
            _EE_HEALTH.heartbeat(
                "ap_exit_engine",
                metrics={"active_positions": len(self.active_positions())},
            )
        except Exception:
            pass

        now_et = datetime.now(ET)
        active = self.active_positions()
        if not active:
            return

        # ── P0-3 tick-level safety: daily-loss self-check ─────────────────────
        # The entry gate triggers the force-close breaker when a NEW signal hits
        # the daily-loss check. But if loss is breached mid-session by a fill
        # (no new signal arriving), the breaker would not fire until the next
        # signal — meanwhile open positions keep bleeding.
        # Fix: exit engine self-checks every tick. Read-only check; idempotent
        # request. If already requested, request_force_close_all() returns
        # False and we move on. Only ever fires once per session.
        _mc_tick = getattr(self, "master_control", None)
        if _mc_tick is not None:
            try:
                breached, _bsnap = _mc_tick.check_daily_loss_breach()
                if breached and not _mc_tick.is_force_close_requested():
                    pnl = float(_bsnap.get("realized_pnl_today", 0.0)) if isinstance(_bsnap, dict) else 0.0
                    limit = getattr(_mc_tick, "max_daily_loss", -0.0)
                    _mc_tick.request_force_close_all(
                        reason=(
                            f"daily_loss_limit_tick_detected "
                            f"${pnl:.2f} <= ${limit:.2f}"
                        )
                    )
                    log.critical(
                        "EXIT_ENGINE_DAILY_LOSS_BREACH_DETECTED | pnl=$%.2f "
                        "limit=$%.2f | force-close triggered from tick (no entry signal needed)",
                        pnl, limit,
                    )
            except Exception as _e:
                log.warning("exit_engine_daily_loss_self_check_failed: %s", _e)

        # ── Tick-level kill-switch self-check (semantic audit 2026-05-21) ─────
        # kill_switch semantics: operator hitting kill = full halt = entries
        # blocked AND positions closed. The entry gate triggers force-close
        # on kill, but only when a new signal arrives. If kill is flipped
        # mid-session and no new signal comes, existing positions would keep
        # running until next signal. This tick check closes that gap.
        # Idempotent — once force-close has been requested, subsequent calls
        # are no-ops.
        if _mc_tick is not None and not _mc_tick.is_force_close_requested():
            try:
                _kfn = getattr(_mc_tick, "_kill_switch_fn", None)
                if _kfn and _kfn():
                    _mc_tick.request_force_close_all(
                        reason="kill_switch_activated_tick_detected"
                    )
                    log.critical(
                        "EXIT_ENGINE_KILL_SWITCH_DETECTED | force-close triggered "
                        "from tick (operator activated kill mid-session)"
                    )
            except Exception as _e:
                log.warning("exit_engine_kill_switch_self_check_failed: %s", _e)

        actions_to_take = []
        with self._lock:
            for pos in active:
                now_utc = datetime.now(timezone.utc)

                # Enrich from QUOTES authority if available and QPM has a fresh snapshot.
                # This supplements the apply_quote_snapshots() path (which QPM calls
                # directly) — if both run, the most recent QPM data wins because QPM
                # is always the last writer of record.
                if _quote_authority_available and _QUOTES is not None:
                    _snap = _QUOTES.get_fresh(pos.option_symbol, max_age_s=12)
                    if _snap is not None:
                        if _snap.bid > 0:
                            pos.current_bid          = _snap.bid
                            pos.current_ask          = _snap.ask
                            pos.current_option_price = _snap.mid
                            pos.last_option_quote_update_ts  = now_utc
                            pos.last_option_quote_missing_ts = None
                            pos.last_quote_update_ts  = now_utc
                            pos.last_quote_missing_ts = None
                        if _snap.underlying_price > 0:
                            pos.current_underlying              = _snap.underlying_price
                            pos.last_underlying_quote_update_ts  = now_utc
                            pos.last_underlying_quote_missing_ts = None

                option_pnl = pos.option_pnl_pct

                # EOD PRE-GATE: this must run before any quote/eligibility gate.
                # After 3:50 PM ET or after market close, a zero/stale option quote
                # must never prevent risk-reducing liquidation.
                _eg_h = now_et.hour
                _eg_m = now_et.minute
                _eg_past_eod = (
                    _eg_h > EOD_HARD_CLOSE_HOUR
                    or (_eg_h == EOD_HARD_CLOSE_HOUR and _eg_m >= EOD_HARD_CLOSE_MIN)
                )
                _eg_market_closed = (_eg_h >= 16)
                if (
                    (_eg_past_eod or _eg_market_closed)
                    and not getattr(pos, "overnight_hold_approved", False)
                    and not pos.exit_in_flight
                    and not pos.closed
                    and int(pos.quantity_remaining or 0) > 0
                ):
                    _eod_qty = int(pos.quantity_remaining)
                    _eod_decision = ExitDecision(
                        action="CLOSE_ALL",
                        quantity=_eod_qty,
                        reason=(
                            f"EOD FORCE CLOSE -- {_eg_h}:{_eg_m:02d} ET "
                            + ("(market closed)" if _eg_market_closed else f"past {EOD_HARD_CLOSE_HOUR}:{EOD_HARD_CLOSE_MIN:02d}")
                            + " | quote_gate_bypassed=True"
                        ),
                        urgency="IMMEDIATE",
                        pnl_pct=option_pnl,
                        reason_code="EOD_FORCE_CLOSE",
                    )
                    log.warning(
                        "[%s] EOD PRE-GATE EXIT | pos_id=%s | %d:%02d ET | qty=%d | option_pnl=%.1f%% | bypassing quote gate",
                        pos.ticker, pos.position_id or "?", _eg_h, _eg_m, _eod_qty, option_pnl * 100,
                    )
                    actions_to_take.append((pos, _eod_decision, False))
                    continue

                # ── P0-3: FORCE-CLOSE-ALL PRE-GATE ────────────────────────────
                # Daily-loss limit hit (or other circuit-breaker trip) must
                # close every open position even with stale quotes. This is a
                # true emergency — entry gate blocked NEW trades, this closes
                # EXISTING ones that are still bleeding.
                # Mirrors the EOD pre-gate pattern: bypasses quote eligibility.
                _mc_pre = getattr(self, "master_control", None)
                _fc_requested = False
                _fc_reason = ""
                try:
                    if _mc_pre is not None and _mc_pre.is_force_close_requested():
                        _fc_requested = True
                        _state = _mc_pre.get_force_close_state()
                        _fc_reason = _state[1] if _state else "force_close_requested"
                except Exception as _fc_e:
                    log.warning("force_close_pre_gate_check_failed: %s", _fc_e)

                if (
                    _fc_requested
                    and not pos.exit_in_flight
                    and not pos.closed
                    and int(pos.quantity_remaining or 0) > 0
                ):
                    _fc_qty = int(pos.quantity_remaining)
                    _fc_decision = ExitDecision(
                        action="CLOSE_ALL",
                        quantity=_fc_qty,
                        reason=(
                            f"SENTINEL FORCED EXIT -- {_fc_reason} "
                            "| quote_gate_bypassed=True"
                        ),
                        urgency="IMMEDIATE",
                        pnl_pct=option_pnl,
                        reason_code="SENTINEL_FORCED_EXIT",
                    )
                    log.critical(
                        "[%s] FORCE_CLOSE PRE-GATE EXIT | pos_id=%s | qty=%d "
                        "| option_pnl=%.1f%% | reason=%s | bypassing quote gate",
                        pos.ticker, pos.position_id or "?", _fc_qty,
                        option_pnl * 100, _fc_reason,
                    )
                    actions_to_take.append((pos, _fc_decision, False))
                    continue

                _force_runner_check = False
                if pos.scale_outs_done >= 1 and pos.peak_pnl_pct >= 0.40:
                    _runner_drop_now   = pos.peak_pnl_pct - option_pnl
                    _emergency_trail   = 0.15
                    if _runner_drop_now >= _emergency_trail:
                        _force_runner_check = True
                        log.warning(
                            "[%s] RUNNER EMERGENCY — peak=%.0f%% now=%.0f%% "
                            "drop=%.0f%% > %.0f%% — emergency runner check",
                            pos.ticker, pos.peak_pnl_pct * 100, option_pnl * 100,
                            _runner_drop_now * 100, _emergency_trail * 100,
                        )

                if _force_runner_check:
                    log.warning(
                        "[%s] RUNNER EMERGENCY active — bypassing normal eligibility gate | %s | in_flight=%s reason=%s",
                        pos.ticker, pos.option_symbol, pos.exit_in_flight, pos.pending_exit_reason,
                    )
                elif not self._eligible_for_new_exit(pos, now_utc):
                    continue

                _has_live_quotes = (
                    pos.current_underlying > 0 and pos.current_option_price > 0
                )
                _has_peak_to_protect = (
                    pos.peak_pnl_pct >= IMMEDIATE_TP_PCT
                    and int(pos.quantity_remaining or 0) > 0
                )

                # ── PEAK TRACKING — runs BEFORE quote gate, always ────────────
                # The quote gate exists to prevent false exits on stale prices.
                # It must NOT prevent recording a new high-water mark.
                # If QPM has a gap exactly at peak, peak_pnl_pct never updates
                # and trail/profit-floor logic fires based on a false 0% peak.
                # Fix: advance peak from ANY non-zero option price, stale or not.
                # Never allow a QPM gap to erase a real peak.
                if pos.current_option_price > 0 and pos.entry_price > 0:
                    _raw_pnl = (pos.current_option_price - pos.entry_price) / pos.entry_price
                    if _raw_pnl > pos.peak_pnl_pct:
                        pos.peak_pnl_pct = _raw_pnl
                        log.debug(
                            "[%s] PEAK UPDATE (pre-gate) | peak=%.1f%% | option=$%.2f entry=$%.2f",
                            pos.ticker, _raw_pnl * 100,
                            pos.current_option_price, pos.entry_price,
                        )
                    if _raw_pnl > 0:
                        pos.touched_profit = True
                        if _raw_pnl > pos.max_profit_seen:
                            pos.max_profit_seen = _raw_pnl
                    # Keep in-memory option_pnl_pct fresh for the DB write
                    # below (property-backed in some paths; setter ensures
                    # the persistence layer sees the same value the engine
                    # decisioned on).
                    try:
                        pos.option_pnl_pct = _raw_pnl
                    except Exception:
                        pass
                    # PR: position-lifecycle-integrity-and-sizing (P0 FIX-3)
                    # Persist peak / max_profit / touched / option_pnl_pct
                    # to positions table. Throttled, non-fatal. The exit
                    # engine continues to read in-memory state for the
                    # exit decision itself; this is dashboard/audit/restart
                    # integrity only.
                    self._persist_peak_state_to_db(pos)
                # ─────────────────────────────────────────────────────────────

                # HOTFIX: quote gate must not block evaluate_exit() when:
                # 1. Position has no quotes but has been open long enough to
                #    have triggered a stop (> HARD_STOP_STALE_AGE_MINUTES mins)
                # 2. Last known option price is non-zero (can evaluate vs entry)
                # 3. Position is past time-stop threshold
                # Without this, a QPM gap silently freezes ALL exit logic for
                # the affected position — stops, force-closes, everything.
                _has_entry_price  = (getattr(pos, "entry_price", 0) or 0) > 0
                _age_mins         = _position_age_minutes(pos)
                _stale_age_thresh = float(os.getenv("HARD_STOP_STALE_AGE_MINUTES", "8"))
                _position_old_enough = _age_mins >= _stale_age_thresh
                # Any last-known option price counts — we can evaluate against entry
                _last_known_price = (
                    (getattr(pos, "current_option_price", 0) or 0) > 0
                    or (getattr(pos, "current_bid", 0) or 0) > 0
                )
                _should_evaluate = (
                    _has_live_quotes
                    or _has_peak_to_protect
                    or (_has_entry_price and _last_known_price)
                    or (_has_entry_price and _position_old_enough)
                )
                if _should_evaluate:
                    # Gate now only controls whether evaluate_exit() is called.
                    # (P0-3 force-close-all is handled by the pre-gate above
                    # so positions can close even with stale quotes.)
                    decision = evaluate_exit(pos, now_et)
                    decision.reason_code = _classify_exit_decision(decision)
                    _ledger_exit_decision(pos, decision, client_id=getattr(pos, "client_id", "") or getattr(self, "client_id", ""))

                    if decision.should_act:
                        option_quote_stale, option_quote_age_sec, option_quote_state = _is_option_quote_stale(pos, now_utc)
                        if option_quote_stale and not _is_forced_risk_exit_code(decision.reason_code):
                            self._emit_exit_event(
                                pos, decision="HOLD",
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
                                pos.ticker, decision.reason_code, option_quote_age_sec,
                                option_quote_state, pos.position_id or "?",
                            )
                            qm = getattr(self, "quote_monitor", None)
                            if qm is not None:
                                qpm_state = getattr(pos, "quote_state", "")
                                if qpm_state == "blind" and hasattr(qm, "note_exit_gated_blind"):
                                    qm.note_exit_gated_blind()
                                elif hasattr(qm, "note_exit_gated_stale"):
                                    qm.note_exit_gated_stale()
                            continue

                        if option_quote_stale and _is_forced_risk_exit_code(decision.reason_code):
                            decision.reason = f"{decision.reason} | DEGRADED_QUOTE_MODE:{option_quote_state}"

                        self._emit_exit_event(
                            pos, decision="SUBMIT",
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
        # FIX-2: actions_to_take contains 3-tuples (pos, decision, bool).
        # Previously unpacked as 2-tuples, raising ValueError when
        # KILL_BLOCKS_NON_PROTECTIVE_EXITS=1. Fixed to unpack as (p, d, _).
        if kill_active and KILL_BLOCKS_NON_PROTECTIVE_EXITS:
            protective      = [(p, d, f) for p, d, f in actions_to_take if _is_protective_exit(d.reason or "")]
            blocked_actions = [(p, d, f) for p, d, f in actions_to_take if not _is_protective_exit(d.reason or "")]
            blocked = len(blocked_actions)
            if blocked:
                log.warning(
                    "Exit engine: kill switch active strict mode -- blocking %d non-protective exit(s), "
                    "allowing %d protective exit(s)",
                    blocked, len(protective),
                )
                for _bp, _bd, _ in blocked_actions:
                    self._emit_exit_event(
                        _bp, decision="REJECT", reason_code="KILL_SWITCH_ACTIVE",
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

        for pos, decision, _force_runner_submit in actions_to_take:
            self._submit_exit_decision(
                pos,
                decision,
                from_sentinel=False,
                kill_active=kill_active,
                allow_inflight_override=bool(_force_runner_submit),
            )

    def _extract_exit_order_identity(self, callback_result) -> dict:
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
            # P3: Harden tuple contract. Tuples must contain string-like order IDs,
            # not status booleans or dicts. A tuple whose first element is a bool
            # (e.g. (True, None) meaning "accepted, no ID") or a non-string type
            # would previously be cast to "True"/"False" and poison identity matching.
            # Reject ambiguous shapes; require dict/object callbacks for structured state.
            first = callback_result[0] if len(callback_result) >= 1 else None
            second = callback_result[1] if len(callback_result) >= 2 else None

            if isinstance(first, bool) or isinstance(first, (dict, list)):
                # Callback returned (accepted_bool, ...) or (dict, ...) — not an order ID tuple.
                # Treat accepted=True (the bool value) but no identity.
                if isinstance(first, bool):
                    if not first:
                        identity["accepted"] = False
                        identity["raw_status"] = "callback_tuple_false"
                log.warning(
                    "[exit_eng] _extract_exit_order_identity: ambiguous tuple shape — "
                    "first element is %s, not a string order ID; treating as no identity. "
                    "Callback should return a dict with local_order_id/broker_order_id.",
                    type(first).__name__,
                )
                self._emit_exit_event(
                    None,  # pos not available here; caller emits with context
                    decision="ALERT",
                    reason_code="EXIT_CALLBACK_AMBIGUOUS_TUPLE",
                    explanation=(
                        f"Exit callback returned a tuple/list whose first element is {type(first).__name__}, "
                        "not a string order ID. No identity extracted; position may quarantine."
                    ),
                    stage="exit_submission",
                ) if False else None  # _emit_exit_event needs pos; caller handles quarantine
                return identity

            if first is not None and not isinstance(first, (str, int)):
                log.warning(
                    "[exit_eng] _extract_exit_order_identity: unexpected tuple element type %s; "
                    "expected str order ID. Callback should return a dict.",
                    type(first).__name__,
                )

            if first is not None:
                identity["local_order_id"] = str(first)
            if second is not None and not isinstance(second, bool):
                identity["broker_order_id"] = str(second)
            return identity

        status = _get(callback_result, "status", "raw_status", "state")
        if status is not None:
            identity["raw_status"] = str(status)

        accepted           = _get(callback_result, "accepted", "ok", "success")
        identity_quarantine = bool(_get(callback_result, "identity_quarantine"))
        status_norm        = str(identity.get("raw_status") or "").upper()
        if accepted is False and not (identity_quarantine or status_norm == "EXIT_SUBMITTED"):
            identity["accepted"] = False

        local_id = _get(
            callback_result,
            "local_order_id", "exit_local_order_id",
            "order_local_id", "client_order_id",
        )
        broker_id = _get(
            callback_result,
            "broker_order_id", "exit_broker_order_id",
            "order_id", "broker_id", "id",
        )
        if local_id  is not None: identity["local_order_id"]  = str(local_id)
        if broker_id is not None: identity["broker_order_id"] = str(broker_id)
        return identity

    def health_snapshot(self, *, stale_after_sec: float = 60.0) -> dict:
        now = datetime.now(timezone.utc)
        with self._lock:
            positions                  = []
            stale_inflight             = []
            stale_quotes               = []
            stale_underlying_quotes    = []
            stale_option_quotes        = []
            missing_callback_identity  = []

            for pos in self._positions:
                flight_sec                  = None
                quote_age_sec               = None
                underlying_quote_age_sec    = None
                option_quote_age_sec        = None
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
                    "position_id":                     pos.position_id,
                    "ticker":                          pos.ticker,
                    "option_symbol":                   pos.option_symbol,
                    "quantity_remaining":              pos.quantity_remaining,
                    "closed":                          pos.closed,
                    "exit_in_flight":                  pos.exit_in_flight,
                    "pending_exit_action":             pos.pending_exit_action,
                    "pending_exit_qty":                pos.pending_exit_qty,
                    "pending_exit_filled_qty":         pos.pending_exit_filled_qty,
                    "pending_exit_local_order_id":     pos.pending_exit_local_order_id,
                    "pending_exit_broker_order_id":    pos.pending_exit_broker_order_id,
                    "flight_sec":                      flight_sec,
                    "quote_age_sec":                   quote_age_sec,
                    "underlying_quote_age_sec":        underlying_quote_age_sec,
                    "option_quote_age_sec":            option_quote_age_sec,
                    "last_quote_update_ts":            pos.last_quote_update_ts.isoformat() if pos.last_quote_update_ts else "",
                    "last_quote_missing_ts":           pos.last_quote_missing_ts.isoformat() if pos.last_quote_missing_ts else "",
                    "last_underlying_quote_update_ts": pos.last_underlying_quote_update_ts.isoformat() if pos.last_underlying_quote_update_ts else "",
                    "last_underlying_quote_missing_ts": pos.last_underlying_quote_missing_ts.isoformat() if pos.last_underlying_quote_missing_ts else "",
                    "last_option_quote_update_ts":     pos.last_option_quote_update_ts.isoformat() if pos.last_option_quote_update_ts else "",
                    "last_option_quote_missing_ts":    pos.last_option_quote_missing_ts.isoformat() if pos.last_option_quote_missing_ts else "",
                    "last_callback_identity_missing":  getattr(pos, "last_callback_identity_missing", False),
                    "exit_identity_quarantine":        getattr(pos, "exit_identity_quarantine", False),
                    "exit_identity_quarantine_alert_count": getattr(pos, "exit_identity_quarantine_alert_count", 0),
                    "last_exit_identity_quarantine_alert_ts": (
                        pos.last_exit_identity_quarantine_alert_ts.isoformat()
                        if getattr(pos, "last_exit_identity_quarantine_alert_ts", None) else ""
                    ),
                    "pending_exit_replace_allowed":    getattr(pos, "pending_exit_replace_allowed", False),
                    "pending_exit_replace_reason":     getattr(pos, "pending_exit_replace_reason", ""),
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
                "running_flag":                   bool(self._running),
                "thread_alive":                   thread_alive,
                "thread_name":                    self._thread.name if self._thread else "",
                "position_count":                 len(positions),
                "stale_inflight_count":           len(stale_inflight),
                "stale_inflight":                 stale_inflight,
                "stale_quote_count":              len(stale_quotes),
                "stale_quotes":                   stale_quotes,
                "stale_underlying_quote_count":   len(stale_underlying_quotes),
                "stale_underlying_quotes":        stale_underlying_quotes,
                "stale_option_quote_count":       len(stale_option_quotes),
                "stale_option_quotes":            stale_option_quotes,
                "missing_callback_identity_count": len(missing_callback_identity),
                "missing_callback_identity":       missing_callback_identity,
                "positions":                       positions,
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
        - Never call on_exit/on_scale while holding self._lock.
        - FIX-1: Discord runner alert fires here after callback returns,
          outside both lock sections, using metadata carried by the decision.
        """
        now_utc       = datetime.now(timezone.utc)
        ticker        = str(pos.ticker or "")
        option_symbol = str(pos.option_symbol or "")
        position_id   = str(pos.position_id or "")

        # ── RESUBMIT GUARD ────────────────────────────────────────────────────
        # Block duplicate exit submission when an active exit order already exists
        # in the DB (EXIT_REQUESTED / EXIT_SUBMITTED / EXIT_ACKNOWLEDGED / PARTIAL).
        # The OSM correctly rejects the illegal state transition; this guard stops
        # the exit engine from even creating a duplicate order (PLTR-class bug).
        # Skipped for kill_active (emergency) and allow_inflight_override paths.
        if not kill_active and not allow_inflight_override:
            try:
                # If in-flight but identity is blank (restart/reseed gap), hydrate
                # from DB first so the guard can compare real order status.
                if (getattr(pos, "exit_in_flight", False)
                        and not getattr(pos, "pending_exit_local_order_id", "")):
                    self.hydrate_pending_exit_identity_from_db(pos)

                _osm = getattr(self, "order_state_machine", None) or getattr(self, "osm", None)
                _active_exit = None
                if _osm is not None:
                    if hasattr(_osm, "_get_active_exit_order"):
                        _active_exit = _osm._get_active_exit_order(position_id)
                    elif hasattr(_osm, "get_active_exit_order"):
                        _active_exit = _osm.get_active_exit_order(position_id)
                if _active_exit_blocks_resubmit(_active_exit):
                    log.warning(
                        "[%s] EXIT RESUBMIT BLOCKED | pos=%s | existing_order=%s | status=%s | broker=%s",
                        ticker, position_id,
                        _active_exit.get("local_order_id"),
                        _active_exit.get("status"),
                        _active_exit.get("broker_order_id"),
                    )
                    try:
                        pos.exit_in_flight                = True
                        pos.pending_exit_local_order_id   = _active_exit.get("local_order_id") or pos.pending_exit_local_order_id
                        pos.pending_exit_broker_order_id  = _active_exit.get("broker_order_id") or pos.pending_exit_broker_order_id
                    except Exception:
                        pass
                    return False
            except Exception as _rsg_err:
                log.debug("[%s] resubmit guard lookup failed (non-fatal): %s", ticker, _rsg_err)
        # ─────────────────────────────────────────────────────────────────────

        # 1) Short critical section: validate and capture submit snapshot.
        with self._lock:
            if not self._can_submit_exit(
                pos, now_utc,
                reason=decision.reason,
                allow_inflight_override=allow_inflight_override,
            ):
                return False

            decision.reason_code = _classify_exit_decision(decision)

            option_quote_stale, option_quote_age_sec, option_quote_state = _is_option_quote_stale(pos, now_utc)
            if option_quote_stale and not _is_forced_risk_exit_code(decision.reason_code):
                self._emit_exit_event(
                    pos, decision="REJECT",
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
                    ticker, decision.reason_code, option_quote_age_sec,
                    option_quote_state, pos.position_id or "?",
                )
                return False

            if option_quote_stale and _is_forced_risk_exit_code(decision.reason_code):
                self._emit_exit_event(
                    pos, decision="ALERT",
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
                    ticker, decision.reason_code, option_quote_age_sec,
                    option_quote_state, pos.position_id or "?",
                )

            if kill_active and KILL_BLOCKS_NON_PROTECTIVE_EXITS and not _is_protective_exit(decision.reason or ""):
                self._emit_exit_event(
                    pos, decision="REJECT", reason_code="KILL_SWITCH_ACTIVE",
                    explanation=f"Kill switch blocked non-protective exit: {decision.reason}",
                    stage="exit_decision",
                )
                return False

            if decision.suggested_limit == 0.0 and pos.current_bid > 0:
                # ── ADAPTIVE EXIT PRICING ─────────────────────────────────────
                # Static bid*0.99 gave away $1-5/contract per trade. Static mid
                # is better but can fail to fill on fast moves. Real solution:
                # urgency-based starting price + retry step-down toward bid.
                #
                # URGENCY TIERS (by exit reason code):
                #   RISK   — hard stop / EOD / theta / sentinel / never-green:
                #            fill speed > price → start at bid immediately
                #   TRAIL  — runner trail / profit lock / trailing stop:
                #            position may be reversing → start at (mid+bid)/2
                #   PROFIT — scale-out / target-hit / protect:
                #            time is on our side → start at mid
                #
                # RETRY STEP-DOWN (via _exit_stuck_count from order_monitor):
                #   attempt 0: tier starting price (mid / between / bid)
                #   attempt 1: step 33% toward bid from starting price
                #   attempt 2: step 66% toward bid
                #   attempt 3+: bid (guarantee fill)
                #
                # WIDE SPREAD OVERRIDE: spread > 20% → always start at bid
                # regardless of tier (illiquid, don't chase mid on thin book).
                # ─────────────────────────────────────────────────────────────
                _bid  = pos.current_bid
                _ask  = pos.current_ask if pos.current_ask > _bid else 0.0
                _mid  = round((_bid + _ask) / 2.0, 2) if _ask > 0 else _bid
                _spread_pct = ((_ask - _bid) / _bid) if (_ask > 0 and _bid > 0) else 1.0
                _attempt    = int(getattr(pos, "_exit_stuck_count", 0))

                # Classify urgency from the exit reason code
                _code = _classify_exit_decision(decision)
                _RISK_CODES = {
                    "EOD_FORCE_CLOSE", "HARD_STOP", "STOP_HIT", "THETA_STOP",
                    "SENTINEL_FORCED_EXIT", "NEVER_GREEN_STOP", "TIME_STOP",
                }
                _TRAIL_CODES = {
                    "RUNNER_TRAIL", "TRAILING_STOP", "PROFIT_LOCK",
                    "TOUCHED_PROFIT_STOP", "SMALL_WIN_LOCK",
                }
                _SOFT_CODES = {
                    "THESIS_FAIL_SOFT_STOP", "THESIS_STALE_SOFT_STOP",
                    "UNDERLYING_PROGRESS_EXIT",
                }

                # Wide spread override — always bid on thin books
                if _spread_pct > 0.20 or _ask == 0:
                    _start_price = _bid
                    _tier = "BID_FORCED_WIDE_SPREAD"
                elif _code in _RISK_CODES or _code in _SOFT_CODES:
                    # Risk/stop exits: fill speed matters most
                    _start_price = _bid
                    _tier = "RISK_BID"
                elif _code in _TRAIL_CODES:
                    # Trail exits: position may be reversing, don't chase mid
                    _start_price = round((_mid + _bid) / 2.0, 2)
                    _tier = "TRAIL_BETWEEN"
                elif _code == "UNKNOWN_EXIT":
                    # Unclassified reason — safe default is bid, not mid.
                    # Unknown exit type could be a stop; never assume it's safe
                    # to be patient. Log it so we can add it to the classifier.
                    log.warning("[EXIT PRICE] %s UNKNOWN_EXIT code — defaulting to bid for safety", ticker)
                    _start_price = _bid
                    _tier = "RISK_BID"
                else:
                    # Profit-taking exits: start at mid, fill usually fast
                    _start_price = _mid
                    _tier = "PROFIT_MID"

                # Retry step-down: each failed attempt steps toward bid
                if _attempt == 0:
                    _final_price = _start_price
                elif _attempt == 1:
                    _final_price = round(_start_price + (_bid - _start_price) * 0.33, 2)
                elif _attempt == 2:
                    _final_price = round(_start_price + (_bid - _start_price) * 0.66, 2)
                else:
                    _final_price = _bid   # attempt 3+: guarantee fill at bid

                # Never go below bid (floor)
                decision.suggested_limit = max(round(_final_price, 2), _bid)

                # Slippage metadata — logged below + available for proof logger
                decision._pricing_meta = {
                    "bid": _bid, "ask": _ask, "mid": _mid,
                    "spread_pct": round(_spread_pct * 100, 1),
                    "tier": _tier, "attempt": _attempt,
                    "suggested_limit": decision.suggested_limit,
                    "slippage_vs_mid": round(decision.suggested_limit - _mid, 3),
                    "slippage_vs_bid": round(decision.suggested_limit - _bid, 3),
                }
                log.info(
                    "[EXIT PRICE] %s | tier=%s attempt=%d | bid=%.2f ask=%.2f mid=%.2f "
                    "spread=%.0f%% → limit=%.2f | vs_mid=%+.3f vs_bid=%+.3f",
                    ticker, _tier, _attempt, _bid, _ask if _ask else 0.0, _mid,
                    _spread_pct * 100, decision.suggested_limit,
                    decision.suggested_limit - _mid,
                    decision.suggested_limit - _bid,
                )

            pre_submit_qty = int(pos.quantity_remaining or 0)
            # P2: snapshot the submit generation so the post-callback lock can
            # detect if a concurrent path (fill, OSM, reconciler) mutated this
            # position while the external callback was executing.
            pre_submit_generation = int(pos._submit_generation)

            log.info(
                "[EXIT] client=%s ticker=%s sym=%s side=%s action=%s pnl=%.1f%% peak=%.1f%% qty_rem=%d qty_close=%d reason=%s",
                pos.client_id or "?", pos.ticker, pos.option_symbol, pos.side,
                decision.action, decision.pnl_pct * 100.0, pos.peak_pnl_pct * 100.0,
                pos.quantity_remaining, decision.quantity, decision.reason,
            )

        # 2) External callback outside lock.
        callback_result  = None
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
                    pos, decision="ERROR",
                    reason_code="EXIT_CALLBACK_NOT_ACCEPTED",
                    explanation=f"Exit callback returned non-accepted status: {callback_identity.get('raw_status', '')}",
                    stage="exit_submission",
                    extra_inputs={"decision_action": decision.action, "decision_qty": decision.quantity},
                )
                return False
        except Exception as exc:
            log.error("[%s] Exit submit failed; position remains tracked: %s", ticker, exc)
            self._emit_exit_event(
                pos, decision="ERROR", reason_code="EXIT_SUBMIT_FAILED",
                explanation=str(exc), stage="exit_submission",
                extra_inputs={"decision_action": decision.action, "decision_qty": decision.quantity},
            )
            return False

        # FIX-1 + P4: Discord runner alert fires in a daemon thread so even a
        # 3-second webhook stall does not stretch the submission path thread.
        # Previously outside the lock but still inline on the submit thread.
        if (
            decision._runner_alert_peak_pct > 0
            and decision.action in ("CLOSE_ALL", "SCALE_OUT")
            and callback_identity.get("accepted", True)
        ):
            _alert_ticker   = pos.ticker
            _alert_peak     = decision._runner_alert_peak_pct
            _alert_pnl      = decision.pnl_pct
            _alert_dur      = decision._runner_duration_min
            _alert_trail    = decision._runner_trail_used
            _alert_qty      = decision.quantity

            def _fire_discord():
                try:
                    import os as _os, requests as _req
                    _wh = _os.getenv("DISCORD_WEBHOOK_RUNNER", "") or _os.getenv("DISCORD_WEBHOOK_URL", "")
                    if _wh:
                        _req.post(_wh, json={"embeds": [{
                            "title":       f"🏆 RUNNER CLOSED · {_alert_ticker}",
                            "description": (
                                f"**Peak: +{_alert_peak*100:.0f}%** → "
                                f"Exit: +{_alert_pnl*100:.0f}%\n"
                                f"Held {_alert_dur}m | "
                                f"Trail: {_alert_trail*100:.0f}pts | "
                                f"Contracts: {_alert_qty}"
                            ),
                            "color": 0xF1C40F,
                        }]}, timeout=3)
                except Exception:
                    pass  # never surface Discord errors to the trading path

            threading.Thread(target=_fire_discord, daemon=True, name="discord-runner-alert").start()

        # 3) Short critical section: revalidate and mark submitted.
        # PR-A / BUG-3: O(1) lookup via self._positions_by_id when
        # position_id is known; only fall back to the linear scan when
        # position_id is empty (the by-object-identity path).
        with self._lock:
            current_pos = None
            if position_id:
                _matched = self._positions_by_id.get(str(position_id))
                if _matched is not None and not _matched.closed:
                    current_pos = _matched
            else:
                for tracked in self._positions:
                    if tracked is pos and not tracked.closed:
                        current_pos = tracked
                        break

            if current_pos is None:
                log.warning(
                    "[%s] Exit callback returned but position is no longer tracked | pos_id=%s sym=%s",
                    ticker, position_id or "?", option_symbol,
                )
                return False

            if current_pos.closed or int(current_pos.quantity_remaining or 0) <= 0:
                log.info("[%s] Exit callback returned after position already closed | pos_id=%s", ticker, position_id or "?")
                return True

            # P2: generation mismatch means a concurrent path (note_partial_exit_fill,
            # mark_position_closed, OSM fill hook) advanced state while our callback
            # was executing. Do not overwrite a confirmed fill or close with a stale
            # submitted marker — the downstream path already owns truth.
            if int(current_pos._submit_generation) != pre_submit_generation:
                log.warning(
                    "[%s] EXIT SUBMIT GENERATION MISMATCH — state advanced concurrently | "
                    "pos_id=%s pre_gen=%d current_gen=%d; not marking in-flight",
                    ticker, position_id or "?",
                    pre_submit_generation, current_pos._submit_generation,
                )
                self._emit_exit_event(
                    current_pos, decision="HOLD",
                    reason_code="EXIT_SUBMIT_GENERATION_MISMATCH",
                    explanation=(
                        "Post-callback position state was advanced by a concurrent path "
                        "(fill/close/OSM) during callback execution; submit not marked in-flight."
                    ),
                    stage="exit_submission",
                    extra_inputs={
                        "pre_submit_generation": pre_submit_generation,
                        "current_generation": current_pos._submit_generation,
                        "decision_action": decision.action,
                        "pre_submit_qty": pre_submit_qty,
                    },
                )
                return True  # callback may have submitted; reconciler handles truth

            if current_pos.exit_in_flight:
                self._emit_exit_event(
                    current_pos, decision="HOLD",
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
                current_pos, decision,
                local_order_id=callback_identity.get("local_order_id", ""),
                broker_order_id=callback_identity.get("broker_order_id", ""),
            )

            if (
                callback_identity.get("raw_status", "").upper() == "EXIT_SUBMITTED"
                and callback_identity.get("local_order_id")
                and not callback_identity.get("broker_order_id")
            ):
                current_pos.last_callback_identity_missing    = True
                current_pos.last_callback_identity_missing_ts = datetime.now(timezone.utc)
                current_pos.exit_identity_quarantine          = True
                current_pos.exit_identity_quarantine_alert_count = 0
                current_pos.last_exit_identity_quarantine_alert_ts = None
                self._emit_exit_event(
                    current_pos, decision="ALERT",
                    reason_code="EXIT_SUBMITTED_BROKER_ID_QUARANTINE",
                    explanation="OSM parked exit as submitted without broker_order_id; reconciler must resolve identity.",
                    stage="exit_submission",
                    extra_inputs={
                        "pending_exit_local_order_id":  current_pos.pending_exit_local_order_id,
                        "pending_exit_broker_order_id": current_pos.pending_exit_broker_order_id,
                    },
                )

            if not current_pos.pending_exit_local_order_id and not current_pos.pending_exit_broker_order_id:
                current_pos.last_callback_identity_missing    = True
                current_pos.last_callback_identity_missing_ts = datetime.now(timezone.utc)
                current_pos.exit_identity_quarantine          = True
                current_pos.exit_identity_quarantine_alert_count = 0
                current_pos.last_exit_identity_quarantine_alert_ts = None
                log.error(
                    "[%s] EXIT SUBMITTED WITHOUT ORDER ID | pos_id=%s sym=%s action=%s qty=%s status=%s",
                    ticker, current_pos.position_id or "?",
                    current_pos.option_symbol or option_symbol,
                    decision.action, decision.quantity,
                    callback_identity.get("raw_status", ""),
                )
                self._emit_exit_event(
                    current_pos, decision="ALERT",
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
            pos, decision="SUBMITTED",
            reason_code=self._exit_reason_code(decision),
            explanation=decision.reason,
            stage="exit_submission",
            extra_inputs={
                "decision_action": decision.action,
                "decision_qty": decision.quantity,
                "from_sentinel": from_sentinel,
                "pre_submit_qty": pre_submit_qty,
                "local_order_id":  callback_identity.get("local_order_id", ""),
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
            if isinstance(raw, dict):
                raw = [raw]
            return {q["symbol"]: q for q in raw if q.get("symbol")}
        except Exception as e:
            log.error("Quote fetch failed: %s", e, exc_info=True)
            return {}

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
            if isinstance(raw, dict):
                raw = [raw]
            return {q["symbol"]: q for q in raw if q.get("symbol")}
        except Exception as e:
            log.error("Option quote fetch failed: %s", e, exc_info=True)
            return {}

    def _refresh_quotes(self) -> None:
        """
        Immediately refresh current_underlying and option prices for all active
        positions. Called after seed_from_db() on restart so exit logic does not
        fire on stale entry-time underlying prices during the first poll window.

        Mirrors the quote-fetch logic in _exit_loop but runs once synchronously.
        Fails silently — the first regular poll cycle (8s) corrects any miss.
        """
        active = self.active_positions()
        if not active:
            log.debug("_refresh_quotes: no active positions to refresh")
            return

        tickers        = list({p.ticker       for p in active})
        option_symbols = list({p.option_symbol for p in active})

        try:
            underlying_quotes = self._fetch_quotes(tickers)
            option_quotes     = self._fetch_option_quotes(option_symbols)
        except Exception as e:
            log.warning("_refresh_quotes: quote fetch failed — first poll cycle will correct: %s", e)
            return

        now_utc = datetime.now(timezone.utc)
        with self._lock:
            for pos in active:
                uq = underlying_quotes.get(pos.ticker, {})
                oq = option_quotes.get(pos.option_symbol, {})

                if uq:
                    last = float(uq.get("last") or uq.get("bid") or 0)
                    if last > 0:
                        pos.current_underlying = last
                        pos.last_underlying_quote_update_ts = now_utc
                        log.info(
                            "[%s] _refresh_quotes: current_underlying refreshed $%.2f -> $%.2f",
                            pos.ticker,
                            pos.underlying_entry,
                            last,
                        )

                if oq:
                    bid = float(oq.get("bid", 0) or 0)
                    ask = float(oq.get("ask", 0) or 0)
                    if bid > 0 and ask > 0:
                        pos.current_bid          = bid
                        pos.current_ask          = ask
                        pos.current_option_price = (bid + ask) / 2
                        pos.last_option_quote_update_ts = now_utc

        log.info("_refresh_quotes: refreshed %d position(s) with live quotes", len(active))
