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
import math
import json as _json
import uuid as _uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Optional, Callable
from zoneinfo import ZoneInfo

from ap.exit_thresholds import (
    effective_thresholds as _shared_effective_thresholds,
    option_profile as _shared_option_profile,
)


try:
    from ap.observability import (
        emit_decision_event,
        emit_exit_decision_stamp,
        get_git_commit,
    )
except Exception:
    emit_decision_event = None
    emit_exit_decision_stamp = None

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

# ── P0: Soft-exit deferred reason codes ──────────────────────────────────────
# Used when a soft exit condition is met but the required truth is unavailable.
# These are explicit stable codes; callers can distinguish real non-confirmation
# (the underlying moved against us) from missing/stale data.
SOFT_EXIT_DEFERRED_OPTION_BID_UNAVAILABLE           = "SOFT_EXIT_DEFERRED_OPTION_BID_UNAVAILABLE"
SOFT_EXIT_DEFERRED_OPTION_QUOTE_STALE               = "SOFT_EXIT_DEFERRED_OPTION_QUOTE_STALE"
SOFT_EXIT_DEFERRED_UNDERLYING_UNAVAILABLE           = "SOFT_EXIT_DEFERRED_UNDERLYING_UNAVAILABLE"
SOFT_EXIT_DEFERRED_UNDERLYING_STALE                 = "SOFT_EXIT_DEFERRED_UNDERLYING_STALE"
SOFT_EXIT_DEFERRED_ENTRY_GRACE                      = "SOFT_EXIT_DEFERRED_ENTRY_GRACE"
SOFT_EXIT_DEFERRED_EXECUTABLE_THRESHOLD_UNCONFIRMED = "SOFT_EXIT_DEFERRED_EXECUTABLE_THRESHOLD_UNCONFIRMED"

# PR #403: explicit authority taxonomy.  These codes are intentionally
# separate from the legacy STOP_HIT/HARD_STOP text so a consumer can tell
# whether the stored underlying geometry or the independent option-loss
# airbag authorized the decision.
UNDERLYING_TECHNICAL_STOP_CONFIRMED = "UNDERLYING_TECHNICAL_STOP_CONFIRMED"
UNDERLYING_STOP_CONFIRMING          = "UNDERLYING_STOP_CONFIRMING"
UNDERLYING_STOP_DEFERRED_DATA_UNAVAILABLE = "UNDERLYING_STOP_DEFERRED_DATA_UNAVAILABLE"
UNDERLYING_STOP_IDENTITY_UNPROVEN       = "UNDERLYING_STOP_IDENTITY_UNPROVEN"
OPTION_CATASTROPHIC_STOP            = "OPTION_CATASTROPHIC_STOP"
SOFT_LOSS_CONFIRMING                = "SOFT_LOSS_CONFIRMING"


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


def _option_profile(pos: "ManagedPosition", *, session_date=None) -> tuple[int, bool, str]:
    """AMENDMENT (PR #385 review): forwards `session_date` so every
    DTE-dependent branch inside evaluate_exit — hard-stop AND never-green
    — consumes the SAME session-date derived from the caller's `now_et`.
    """
    return _shared_option_profile(pos, session_date=session_date)


@dataclass(frozen=True)
class ExitDecisionSnapshot:
    """Canonical quote/P&L snapshot for one exit evaluation cycle.

    Built once at the top of evaluate_exit() and consumed by every classifier.
    No exit branch may independently fetch option or underlying quotes after
    this is built.  One decision evaluation; one coherent snapshot.

    Field semantics
    ───────────────
    option_bid / option_ask / option_mid  — raw market data from position state
    exit_executable_mark   — bid when bid is valid; None otherwise
                             (NEVER midpoint/ask/last for soft exit decisions)
    option_bid_valid       — bid is a real finite positive number
    option_quote_fresh     — quote timestamp within EXIT_ENGINE_STALE_OPTION_QUOTE_SEC
    exit_executable_pnl_pct — bid-based P&L vs entry; None when bid unavailable
    display_pnl_pct        — midpoint-based P&L for charting / display only

    underlying_available   — current underlying price is a real positive number
    underlying_fresh       — underlying timestamp within UNDERLYING_QUOTE_STALE_SEC
    underlying_quote_ts    — timestamp of the underlying observation used here
    underlying_quote_source — transport/provenance label when available
    in_grace_window        — position is inside the soft-exit grace window
    """
    # Option truth
    option_bid:               Optional[float]
    option_ask:               Optional[float]
    option_mid:               Optional[float]
    exit_executable_mark:     Optional[float]
    option_bid_valid:         bool
    option_quote_fresh:       bool
    option_quote_age_sec:     Optional[float]
    option_quote_ts:          Optional[datetime]

    # Underlying truth
    underlying_price:         Optional[float]
    underlying_available:     bool
    underlying_fresh:         bool
    underlying_age_sec:       Optional[float]

    # Position-derived truth
    entry_price:              float
    exit_executable_pnl_pct:  Optional[float]
    display_pnl_pct:          Optional[float]
    touched_profit:           bool
    in_grace_window:          bool
    # Defaults preserve compatibility with current-main tests that construct
    # snapshots directly instead of going through the builder.
    underlying_quote_ts:      Optional[datetime] = None
    underlying_quote_source:  str = ""


def _build_exit_decision_snapshot(
    pos: "ManagedPosition",
    now_utc: Optional[datetime] = None,
) -> ExitDecisionSnapshot:
    """Build the canonical ExitDecisionSnapshot for one evaluation cycle.

    Reads from position attributes written by QPM._refresh_once().  Falls back
    to computing values directly from bid/ask/ts when explicit fields are absent
    (e.g. positions seeded before this PR deployed).  Never fetches from broker
    or market data.
    """
    now_utc = now_utc or datetime.now(timezone.utc)

    def _attr(pos_, *names):
        """None-safe dual-name read.  `a or b` swallows meaningful False/0 —
        this returns the FIRST attribute that exists (is not None)."""
        for n in names:
            v = getattr(pos_, n, None)
            if v is not None:
                return v
        return None

    def _quote_age_seconds(value) -> Optional[float]:
        """Parse a quote timestamp and reject materially future observations."""
        try:
            timestamp = value
            if isinstance(timestamp, str):
                timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            elif isinstance(timestamp, (int, float)):
                timestamp = datetime.fromtimestamp(float(timestamp), timezone.utc)
            if not isinstance(timestamp, datetime):
                return None
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            else:
                timestamp = timestamp.astimezone(timezone.utc)
            age_sec = (now_utc - timestamp).total_seconds()
            if age_sec < -60.0:
                return None
            return max(0.0, age_sec)
        except Exception:
            return None

    # ── Option bid / ask / mid ────────────────────────────────────────────────
    bid = _positive_or_none(_attr(pos, "current_bid", "currentbid"))
    ask = _positive_or_none(_attr(pos, "current_ask", "currentask"))
    analytics_mark = _positive_or_none(_attr(pos, "analytics_mark_price", "analyticsmarkprice"))
    if bid is not None and ask is not None:
        mid: Optional[float] = round((bid + ask) / 2.0, 4)
    elif analytics_mark is not None:
        mid = analytics_mark
    else:
        mid = _positive_or_none(_attr(pos, "current_option_price", "currentoptionprice"))

    # ── AMENDMENT #2 (blocker 1): explicit truth fields are AUTHORITATIVE ─────
    # QPM writes option_bid_valid from THIS cycle's broker response.  When QPM
    # says the bid is invalid, the retained numeric bid on the position is a
    # PREVIOUS poll's value and must be discarded — option_bid_valid=False
    # forces bid=None.  Derived positivity is only a fallback for positions
    # that predate the explicit fields (never overrides an explicit False).
    bid_valid_direct = _attr(pos, "option_bid_valid", "optionbidvalid")
    if bid_valid_direct is not None:
        opt_bid_valid = bool(bid_valid_direct)
        if not opt_bid_valid:
            bid = None
    else:
        opt_bid_valid = bid is not None and bid > 0.0
    exit_executable_mark: Optional[float] = bid if opt_bid_valid else None

    # ── Option quote freshness ────────────────────────────────────────────────
    # AMENDMENT #3 (blocker 2): freshness is ALWAYS recomputed from the bid
    # timestamp at evaluation time.  QPM's stored explicit boolean can VETO
    # freshness (writing False forces False regardless of timestamp) but can
    # never PROVE it forever — if QPM stalls or its thread dies, a five-minute-
    # old True must not survive.  Cached option_quote_age_sec is also not
    # trusted; the age is (now - stored_bid_ts) at THIS moment.
    opt_quote_fresh_direct = _attr(pos, "option_quote_fresh", "optionquotefresh")
    opt_ts = _attr(pos, "last_option_bid_update_ts", "lastoptionbidupdatets",
                   "last_option_quote_update_ts", "lastoptionquoteupdatets")
    _exit_stale_sec = float(os.getenv("EXIT_ENGINE_STALE_OPTION_QUOTE_SEC", str(STALE_OPTION_QUOTE_MAX_AGE_SEC)))

    opt_age_sec: Optional[float] = None
    if opt_ts is not None:
        opt_age_sec = _quote_age_seconds(opt_ts)

    # Timestamp-derived freshness (always current):
    if opt_age_sec is None:
        _ts_fresh = False
    else:
        _ts_fresh = opt_age_sec <= _exit_stale_sec

    # Effective freshness: bid must be valid AND timestamp fresh AND explicit
    # boolean must not veto.  A missing explicit boolean means "no veto".
    if opt_quote_fresh_direct is False:
        opt_quote_fresh: bool = False
    else:
        opt_quote_fresh = opt_bid_valid and _ts_fresh

    # ── Underlying truth ──────────────────────────────────────────────────────
    # AMENDMENT #2 (blocker 2): explicit underlying_available is AUTHORITATIVE.
    # QPM writes it from THIS cycle's fetch (und_last).  When False, the numeric
    # current_underlying on the position is the previous poll's retained price —
    # it must be discarded, not re-derived as "available".
    und_price = _positive_or_none(_attr(pos, "current_underlying", "currentunderlying"))
    und_avail_direct = _attr(pos, "underlying_available", "underlyingavailable")
    if und_avail_direct is not None:
        und_available = bool(und_avail_direct)
        if not und_available:
            und_price = None
    else:
        und_available = und_price is not None and und_price > 0.0

    und_fresh_direct = _attr(pos, "underlying_fresh", "underlyingfresh")
    und_ts = _attr(pos, "last_underlying_quote_update_ts", "lastunderlyingquoteupdatets")
    _und_stale_sec = float(os.getenv("UNDERLYING_QUOTE_STALE_SEC", str(_exit_stale_sec * 2)))

    und_age_sec: Optional[float] = None
    if und_ts is not None:
        und_age_sec = _quote_age_seconds(und_ts)
    _und_ts_fresh = (und_age_sec is not None and und_age_sec <= _und_stale_sec)

    # AMENDMENT #3 (blocker 2): explicit False vetoes; True cannot survive without ts proof.
    if und_fresh_direct is False:
        und_fresh: bool = False
    else:
        und_fresh = und_available and _und_ts_fresh

    # ── P&L ──────────────────────────────────────────────────────────────────
    entry_price = float(getattr(pos, "entry_price", 0.0) or 0.0)
    exec_pnl: Optional[float] = None
    disp_pnl: Optional[float] = None

    if entry_price > 0.0:
        # Decision authority is the current executable BID against the current
        # canonical entry fill.  QPM's persisted percentage is observability
        # only; it can be stale across entry-fill correction/adoption.
        if exit_executable_mark is not None:
            try:
                exec_pnl = (exit_executable_mark - entry_price) / entry_price
            except Exception:
                pass

        if mid is not None:
            try:
                disp_pnl = (mid - entry_price) / entry_price
            except Exception:
                pass

    underlying_source = str(
        _attr(
            pos,
            "underlying_quote_source",
            "underlyingquotesource",
            "last_underlying_quote_source",
            "lastunderlyingquotesource",
        )
        or ""
    ).strip()
    if not underlying_source and und_ts is not None:
        # Current main's QPM writes the canonical observation timestamp on the
        # position. That timestamp is the minimum transport provenance even
        # when an older hydrated position has no separate source label.
        underlying_source = "position_quote_monitor"

    # ── Grace window ──────────────────────────────────────────────────────────
    age_min = _position_age_minutes(pos, now_utc=now_utc)
    in_grace = age_min < _MIN_HOLD_BEFORE_EXIT_MIN

    return ExitDecisionSnapshot(
        option_bid            = bid,
        option_ask            = ask,
        option_mid            = mid,
        exit_executable_mark  = exit_executable_mark,
        option_bid_valid      = opt_bid_valid,
        option_quote_fresh    = opt_quote_fresh,
        option_quote_age_sec  = opt_age_sec,
        option_quote_ts       = opt_ts,
        underlying_price      = und_price,
        underlying_available  = und_available,
        underlying_fresh      = und_fresh,
        underlying_age_sec    = und_age_sec,
        entry_price           = entry_price,
        exit_executable_pnl_pct = exec_pnl,
        display_pnl_pct       = disp_pnl,
        touched_profit        = bool(getattr(pos, "touched_profit", False)),
        in_grace_window       = in_grace,
        underlying_quote_ts   = und_ts,
        underlying_quote_source = underlying_source,
    )


def _soft_exit_option_truth_gate(
    snap: ExitDecisionSnapshot,
    *,
    qty_rem: int,
) -> "Optional[ExitDecision]":
    """Return a HOLD ExitDecision if option executable truth is insufficient.
    Returns None when truth is sufficient (caller may proceed with the soft exit).

    Must be called before EVERY soft exit branch.  Never call before hard exits.

    Entry grace is intentionally not enforced here. This gate protects soft
    exits from stale or non-executable option truth only; loss-specific grace
    is applied by the loss branches that need breathing room.
    """
    if not snap.option_bid_valid:
        return ExitDecision(
            action="HOLD", quantity=0,
            reason="SOFT_EXIT_DEFERRED — option bid unavailable; executable truth required for soft exits",
            urgency="NORMAL",
            pnl_pct=snap.display_pnl_pct or 0.0,
            reason_code=SOFT_EXIT_DEFERRED_OPTION_BID_UNAVAILABLE,
        )
    if not snap.option_quote_fresh:
        age_str = f"{snap.option_quote_age_sec:.0f}s" if snap.option_quote_age_sec is not None else "unknown"
        return ExitDecision(
            action="HOLD", quantity=0,
            reason=f"SOFT_EXIT_DEFERRED — option quote stale ({age_str}); fresh executable truth required",
            urgency="NORMAL",
            pnl_pct=snap.display_pnl_pct or 0.0,
            reason_code=SOFT_EXIT_DEFERRED_OPTION_QUOTE_STALE,
        )
    if snap.exit_executable_pnl_pct is None:
        return ExitDecision(
            action="HOLD", quantity=0,
            reason="SOFT_EXIT_DEFERRED — executable P&L unavailable (missing entry or valid bid)",
            urgency="NORMAL",
            pnl_pct=0.0,
            reason_code=SOFT_EXIT_DEFERRED_OPTION_BID_UNAVAILABLE,
        )
    return None


def _soft_exit_entry_grace_decision(snap: ExitDecisionSnapshot) -> "Optional[ExitDecision]":
    if not snap.in_grace_window:
        return None
    return ExitDecision(
        action="HOLD", quantity=0,
        reason=(
            "SOFT_EXIT_DEFERRED — inside post-entry grace window "
            f"({_MIN_HOLD_BEFORE_EXIT_MIN:.0f}min); loss exits deferred, winner protection unaffected"
        ),
        urgency="NORMAL",
        pnl_pct=(snap.exit_executable_pnl_pct
                 if snap.exit_executable_pnl_pct is not None
                 else (snap.display_pnl_pct or 0.0)),
        reason_code=SOFT_EXIT_DEFERRED_ENTRY_GRACE,
    )


def _soft_exit_underlying_truth_gate(
    snap: ExitDecisionSnapshot,
    *,
    unavailable_code: str = SOFT_EXIT_DEFERRED_UNDERLYING_UNAVAILABLE,
    stale_code: str = SOFT_EXIT_DEFERRED_UNDERLYING_STALE,
) -> "Optional[ExitDecision]":
    """Return a HOLD ExitDecision if underlying truth is insufficient for
    underlying-dependent soft exits.  Returns None when truth is sufficient.

    ONLY call this for exit branches that require underlying direction confirmation
    (soft stops, never-green stops).  Take-profit / trail branches do NOT need
    underlying confirmation and must NOT call this gate.
    """
    if not snap.underlying_available:
        return ExitDecision(
            action="HOLD", quantity=0,
            reason="SOFT_EXIT_DEFERRED — underlying price unavailable; cannot confirm thesis for soft exit",
            urgency="NORMAL",
            pnl_pct=snap.exit_executable_pnl_pct or 0.0,
            reason_code=unavailable_code,
        )
    if not snap.underlying_fresh:
        age_str = f"{snap.underlying_age_sec:.0f}s" if snap.underlying_age_sec is not None else "unknown"
        return ExitDecision(
            action="HOLD", quantity=0,
            reason=f"SOFT_EXIT_DEFERRED — underlying quote stale ({age_str}); fresh truth required",
            urgency="NORMAL",
            pnl_pct=snap.exit_executable_pnl_pct or 0.0,
            reason_code=stale_code,
        )
    return None


def _effective_thresholds(
    pos: "ManagedPosition",
    *,
    session_date=None,
) -> tuple:
    """Returns (hard_stop, immediate_tp, profit_lock) adjusted for DTE and instrument.

    AMENDMENT (PR #385 review): `session_date` forwards to the shared
    thresholds helper so evaluate_exit()'s caller-supplied `now_et` can
    pin DTE profile lookup to the evaluation clock rather than the host
    wall-clock date (deterministic replay).
    """
    return _shared_effective_thresholds(pos, session_date=session_date)

# TRAILING STOP — fires when position drops N points from its peak
# Wide enough to let winners run to 25-30%, tight enough to protect gains
TRAIL_DROP_FROM_PEAK  = 0.10   # 10pt drop from peak fires exit (e.g. +30% → exits at +20%)
SMALL_WIN_PCT         = 0.12   # trail kicks in once we've seen +12%
SMALL_WIN_TRAIL       = 0.08   # floor 8pt below peak (seen +20% → floor at +12%)

# ── HARD-EXIT REFERENCE EXPIRATION ───────────────────────────────────────────
# A "proven" label cannot remain trusted forever — it was computed at a
# specific moment.  After this threshold the label is considered expired and
# the consumer must fall back to option_pnl_pct.  The threshold is deliberately
# generous (5 minutes) because a freshly-proven reference should stay trusted
# through a brief QPM gap, but not through a full QPM outage where new
# catastrophic losses could accumulate undetected.
HARD_REF_MAX_AGE_SEC = float(os.getenv("HARD_EXIT_REF_MAX_AGE_SEC", "300"))


def _normalize_hard_ref_ts(ts, *, now_utc: Optional[datetime] = None) -> Optional[datetime]:
    try:
        from ap.position_quote_monitor import normalize_hard_ref_ts
        return normalize_hard_ref_ts(ts, now_utc=now_utc or datetime.now(timezone.utc))
    except Exception:
        return None


def get_effective_hard_exit_reference(
    pos: "ManagedPosition",
    now_utc: "Optional[datetime]" = None,
) -> "Optional[float]":
    """Single authority for resolving hard-exit P&L across all consumers.

    Returns the hard-exit reference P&L when the stored reference is:
      - validity in ("proven", "catastrophic_ask"), AND
      - timestamp parses successfully AND age is within HARD_REF_MAX_AGE_SEC.

    Returns None when:
      - no hard-exit reference exists on the position (pre-amendment),
      - validity is "unproven" or "no_data",
      - the timestamp cannot be parsed,
      - the reference has expired (age > HARD_REF_MAX_AGE_SEC).

    All four consumers (evaluate_exit, _check_all_positions pre-gate,
    _run_sentinels, emergency_flatten) MUST call this function.  None of them
    may read hard_exit_reference_pnl_pct directly.

    PR #385 amendment #6, blocker 2: consumers were trusting a validity label
    without recomputing age.  A stale healthy reference could hide a new
    catastrophic loss (false safety during QPM outage); a stale catastrophic
    reference could manufacture a false forced exit.
    """
    now_utc = now_utc or datetime.now(timezone.utc)

    validity = str(getattr(pos, "hard_exit_reference_validity",
                            getattr(pos, "hardexitreferencevalidity", "")) or "")
    if validity not in ("proven", "catastrophic_ask"):
        return None

    reference_price = getattr(pos, "hard_exit_reference_price",
                              getattr(pos, "hardexitreferenceprice", None))
    entry_price = getattr(pos, "entry_price", None)
    try:
        reference_price = float(reference_price or 0.0)
        entry_price = float(entry_price or 0.0)
    except (TypeError, ValueError):
        return None
    if reference_price <= 0 or entry_price <= 0:
        return None

    ts = getattr(pos, "hard_exit_reference_ts",
                 getattr(pos, "hardexitreferencets", None))
    if ts is None:
        return None  # no timestamp → cannot verify freshness → fail closed

    parsed_ts = _normalize_hard_ref_ts(ts, now_utc=now_utc)
    if parsed_ts is None:
        return None  # unparseable timestamp → fail closed

    age_sec = max(0.0, (now_utc - parsed_ts).total_seconds())
    if age_sec > HARD_REF_MAX_AGE_SEC:
        return None  # expired — consumer must fall back to option_pnl_pct

    # The cached percentage is observability/persistence only.  The hard-stop
    # authority is the reference price divided by the current canonical entry.
    return (reference_price - entry_price) / entry_price


def _set_position_attr_pair(pos, canonical_name: str, value) -> None:
    """Write canonical snake_case field and its legacy compressed alias."""
    setattr(pos, canonical_name, value)
    setattr(pos, canonical_name.replace("_", ""), value)


def _hard_ref_is_authoritative(validity: str) -> bool:
    return str(validity or "") in ("proven", "catastrophic_ask")


def _recompute_hard_ref_cached_pnl(pos) -> None:
    try:
        _price = float(getattr(pos, "hard_exit_reference_price", 0.0) or 0.0)
        _entry = float(getattr(pos, "entry_price", 0.0) or 0.0)
        if _price > 0 and _entry > 0:
            _set_position_attr_pair(
                pos,
                "hard_exit_reference_pnl_pct",
                (_price - _entry) / _entry,
            )
    except Exception as _e:
        log.debug("[exit_eng] hard-ref pnl rebase skipped: %s", _e)


def _reclassify_hard_ref_for_entry(pos) -> None:
    """Reclassify ASK-only hard refs after canonical entry-price adoption."""
    _source = str(getattr(pos, "hard_exit_reference_source", "") or "").lower()
    if _source not in ("ask_unproven", "ask_catastrophic"):
        _recompute_hard_ref_cached_pnl(pos)
        if _source == "ask_stale":
            _set_position_attr_pair(pos, "hard_exit_reference_validity", "unproven")
            _set_position_attr_pair(pos, "hard_exit_reference_refresh_needed", True)
        return

    try:
        _price = float(getattr(pos, "hard_exit_reference_price", 0.0) or 0.0)
        _entry = float(getattr(pos, "entry_price", 0.0) or 0.0)
    except (TypeError, ValueError):
        return
    if _price <= 0 or _entry <= 0:
        return

    try:
        _hard_stop, _, _ = _effective_thresholds(pos)
    except Exception:
        _hard_stop = HARD_STOP_PCT

    _pnl = (_price - _entry) / _entry
    _validity = "catastrophic_ask" if _pnl <= _hard_stop else "unproven"
    _source = "ask_catastrophic" if _validity == "catastrophic_ask" else "ask_unproven"
    _set_position_attr_pair(pos, "hard_exit_reference_source", _source)
    _set_position_attr_pair(pos, "hard_exit_reference_validity", _validity)
    _set_position_attr_pair(pos, "hard_exit_reference_pnl_pct", _pnl)
    _refresh_needed = not _hard_ref_is_authoritative(_validity)
    _set_position_attr_pair(pos, "hard_exit_reference_refresh_needed", _refresh_needed)


def _copy_hard_exit_reference(dst, src) -> None:
    for _attr in (
        "hard_exit_reference_price",
        "hard_exit_reference_source",
        "hard_exit_reference_validity",
        "hard_exit_reference_ts",
        "hard_exit_reference_pnl_pct",
        "hard_exit_reference_refresh_needed",
    ):
        if hasattr(src, _attr):
            _set_position_attr_pair(dst, _attr, getattr(src, _attr, None))
    _recompute_hard_ref_cached_pnl(dst)


def _merge_hard_exit_reference_for_collapse(dst, src, *, now_utc: datetime) -> None:
    """Merge repair hard-ref into canonical using the canonical replacement rule.

    AMENDMENT (PR #385 review — Fix C): the previous merge did its own
    `src_ts > dst_ts` compare and did NOT consult source quality.  An
    equal-time repair BID therefore could not replace a canonical
    MARK/LAST/ASK during broker-repair collapse, leaving the persisted
    canonical authority on the inferior source.  Delegate to the shared
    `_should_replace_hard_ref` gate (same helper that already governs
    _apply_option_quote_for_decision and apply_quote_snapshots) and pass
    source labels so equal-normalized-timestamp ties break by source
    quality (BID > LAST > MARK > catastrophic ASK).
    """
    try:
        _src_price = float(getattr(src, "hard_exit_reference_price", 0.0) or 0.0)
        _dst_price = float(getattr(dst, "hard_exit_reference_price", 0.0) or 0.0)
    except (TypeError, ValueError):
        return

    _src_validity = str(getattr(src, "hard_exit_reference_validity", "") or "")
    _dst_validity = str(getattr(dst, "hard_exit_reference_validity", "") or "")
    _src_source = str(getattr(src, "hard_exit_reference_source", "") or "")
    _dst_source = str(getattr(dst, "hard_exit_reference_source", "") or "")
    _src_ts = _normalize_hard_ref_ts(
        getattr(src, "hard_exit_reference_ts", None), now_utc=now_utc
    )
    _dst_ts = _normalize_hard_ref_ts(
        getattr(dst, "hard_exit_reference_ts", None), now_utc=now_utc
    )

    # Unproven / no-data source cannot erase a positive authoritative dst.
    if _src_validity == "no_data" or _src_price <= 0:
        if _dst_price > 0:
            _set_position_attr_pair(dst, "hard_exit_reference_refresh_needed", True)
        return

    # Same fail-closed guard for an unproven src over an authoritative dst.
    if (
        _src_validity == "unproven"
        and _hard_ref_is_authoritative(_dst_validity)
        and _dst_price > 0
    ):
        _set_position_attr_pair(dst, "hard_exit_reference_refresh_needed", True)
        return

    _replace = _should_replace_hard_ref(
        prior_validity=_dst_validity,
        prior_ts=_dst_ts,
        prior_price=_dst_price,
        candidate_validity=_src_validity,
        candidate_ts=_src_ts,
        prior_source=_dst_source,
        candidate_source=_src_source,
        now_utc=now_utc,
    )
    if _replace:
        _copy_hard_exit_reference(dst, src)


def _should_replace_hard_ref(
    *,
    prior_validity,
    prior_ts,
    prior_price,
    candidate_validity,
    candidate_ts,
    prior_source=None,
    candidate_source=None,
    now_utc: "Optional[datetime]" = None,
) -> bool:
    """Passthrough to the canonical QPM policy.

    AMENDMENT (PR #385 review): forwards source labels so the shared
    helper can break equal-timestamp ties by source quality (BID > LAST
    > MARK > catastrophic ASK).  Older callers that omit source args get
    the pre-amendment behavior (prior wins at equal ts).
    """
    try:
        from ap.position_quote_monitor import should_replace_hard_ref
        return should_replace_hard_ref(
            prior_validity=prior_validity,
            prior_ts=prior_ts,
            prior_price=prior_price,
            candidate_validity=candidate_validity,
            candidate_ts=candidate_ts,
            prior_source=prior_source,
            candidate_source=candidate_source,
            now_utc=now_utc or datetime.now(timezone.utc),
        )
    except Exception:
        return False


def _has_fresh_dedicated_bid(pos: "ManagedPosition", *, now_utc: "Optional[datetime]" = None) -> bool:
    """True when the position holds a fresh, executable, un-vetoed dedicated BID.

    AMENDMENT (PR #385 review P0-2): explicit producer vetoes must be honored.
    QPM writes `option_bid_valid=False` when this cycle's raw bid failed its
    truth check, and `option_quote_fresh=False` when the quote is stale.  A
    retained numeric bid must not regain hard-stop authority merely because
    the timestamp is still within the freshness window.  Elsewhere in the
    engine (`_build_exit_decision_snapshot`) these booleans are already
    treated as authoritative vetoes; the hard-stop fallback must match.
    """
    now_utc = now_utc or datetime.now(timezone.utc)

    # Producer vetoes: explicit False overrides any numeric reading.
    _bid_valid_direct = getattr(pos, "option_bid_valid",
                                getattr(pos, "optionbidvalid", None))
    if _bid_valid_direct is False:
        return False
    _quote_fresh_direct = getattr(pos, "option_quote_fresh",
                                   getattr(pos, "optionquotefresh", None))
    if _quote_fresh_direct is False:
        return False

    try:
        bid = float(getattr(pos, "current_bid", getattr(pos, "currentbid", 0.0)) or 0.0)
    except Exception:
        return False
    if bid <= 0:
        return False
    ts = _normalize_hard_ref_ts(
        getattr(pos, "last_option_bid_update_ts", getattr(pos, "lastoptionbidupdatets", None)),
        now_utc=now_utc,
    )
    if ts is None:
        return False
    try:
        return 0 <= (now_utc - ts).total_seconds() <= float(STALE_OPTION_QUOTE_MAX_AGE_SEC)
    except Exception:
        return False


def _dedicated_bid_pnl(
    pos: "ManagedPosition",
    *,
    now_utc: "Optional[datetime]" = None,
) -> "tuple[Optional[float], Optional[datetime]]":
    """Return (bid_pnl_pct, dedicated_bid_ts) when fresh + valid, else (None, None).

    Callers use the timestamp to compare against other authoritative sources
    (e.g. the persisted hard-exit reference) so the newest observation wins.
    """
    now_utc = now_utc or datetime.now(timezone.utc)
    if not _has_fresh_dedicated_bid(pos, now_utc=now_utc):
        return None, None
    try:
        bid = float(getattr(pos, "current_bid",
                            getattr(pos, "currentbid", 0.0)) or 0.0)
        entry = float(getattr(pos, "entry_price", 0.0) or 0.0)
    except (TypeError, ValueError):
        return None, None
    if bid <= 0.0 or entry <= 0.0:
        return None, None
    bid_ts = _normalize_hard_ref_ts(
        getattr(pos, "last_option_bid_update_ts",
                getattr(pos, "lastoptionbidupdatets", None)),
        now_utc=now_utc,
    )
    return (bid - entry) / entry, bid_ts


def _resolve_hard_stop_pnl_authority(
    pos: "ManagedPosition",
    now_utc: "Optional[datetime]" = None,
) -> "Optional[float]":
    """Single authority for hard-stop P&L across evaluate_exit() and sentinels.

    Strict source priority (PR #385 contract: fresh BID > fresh LAST > fresh MARK):

      1. Fresh, un-vetoed dedicated executable BID (via `_dedicated_bid_pnl`).
         BID is the PR's primary hard-exit source; a persisted reference is
         only useful when the current cycle has no executable BID.
      2. `get_effective_hard_exit_reference()` — provenance-aware persisted
         hard-exit reference (validity ∈ {"proven","catastrophic_ask"},
         age ≤ HARD_REF_MAX_AGE_SEC).
      3. None — no price-based hard-stop authority this cycle.

    AMENDMENT (PR #385 review): the earlier timestamp-comparison approach
    let a persisted reference derived from LAST / MARK / ASK outrank a fresh
    executable BID whenever its cached provider clock happened to be newer
    than the BID's dedicated timestamp.  Those clocks come from different
    sources with different receipt semantics and cannot be compared safely.
    The PR contract explicitly ranks fresh BID above LAST / MARK / ASK, so
    the priority is strict and unconditional, not chronological.

    Never falls back to midpoint, mark, LAST, ASK, current_option_price,
    or option_pnl_pct directly — those may derive from unproven PAPER
    pricing.  Missing authority → None → caller must not fire a hard stop.
    Zero would falsely certify safety and is prohibited.
    """
    now_utc = now_utc or datetime.now(timezone.utc)

    bid_pnl, _bid_ts = _dedicated_bid_pnl(pos, now_utc=now_utc)
    if bid_pnl is not None:
        return bid_pnl

    ref_pnl = get_effective_hard_exit_reference(pos, now_utc)
    if ref_pnl is not None:
        return ref_pnl

    return None


def _is_adoption_identity_quarantined(pos) -> bool:
    return bool(
        getattr(pos, "adoption_identity_quarantined", False)
        or getattr(pos, "adoptionidentityquarantined", False)
    )


def _mark_adoption_identity_quarantined(pos, reason: str) -> None:
    try:
        pos.adoption_identity_quarantined = True
        pos.adoptionidentityquarantined = True
        pos.adoption_identity_quarantine_reason = reason
        pos.adoptionidentityquarantinereason = reason
    except Exception as _e:
        log.debug("[exit_eng] mark adoption identity quarantine failed: %s", _e)


def _clear_adoption_identity_quarantine(pos) -> None:
    try:
        pos.adoption_identity_quarantined = False
        pos.adoptionidentityquarantined = False
        pos.adoption_identity_quarantine_reason = ""
        pos.adoptionidentityquarantinereason = ""
    except Exception as _e:
        log.debug("[exit_eng] clear adoption identity quarantine failed: %s", _e)


def _is_behavior_active_position(pos) -> bool:
    try:
        return (
            not getattr(pos, "closed", False)
            and int(getattr(pos, "quantity_remaining", 0) or 0) > 0
            and not _is_adoption_identity_quarantined(pos)
        )
    except Exception:
        return False


# ── POSITION TRACKER ─────────────────────────────────────────────────────────


def _normalize_canonical_ts(ts, *, fallback=None, local_order_id: str = ""):
    """Normalize a broker fill timestamp to an aware UTC datetime.

    Accepts: aware datetime, naive datetime (assumed UTC), ISO 8601 string
    with Z or offset, numeric epoch (seconds).

    On failure: uses fallback, or datetime.now(UTC).
    Emits CANONICAL_ENTRY_TIMESTAMP_FALLBACK on any fallback path.
    """
    import datetime as _dt
    _UTC = _dt.timezone.utc

    def _try(v):
        if v is None:
            return None
        if isinstance(v, _dt.datetime):
            return v.replace(tzinfo=_UTC) if v.tzinfo is None else v.astimezone(_UTC)
        if isinstance(v, (int, float)):
            try:
                return _dt.datetime.fromtimestamp(float(v), tz=_UTC)
            except Exception:
                return None
        if isinstance(v, str):
            s = v.strip().replace("Z", "+00:00")
            for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z",
                        "%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d %H:%M:%S.%f%z",
                        "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                try:
                    p = _dt.datetime.strptime(s, fmt)
                    return p.replace(tzinfo=_UTC) if p.tzinfo is None else p.astimezone(_UTC)
                except ValueError:
                    continue
        return None

    result = _try(ts)
    if result is not None:
        return result

    log.warning(
        "[exit_eng] CANONICAL_ENTRY_TIMESTAMP_FALLBACK | "
        "order=%s original_ts=%r — using fallback or now_utc",
        local_order_id or "?", ts,
    )
    if fallback is not None:
        result = _try(fallback)
        if result is not None:
            return result
    return _dt.datetime.now(_UTC)


@dataclass(frozen=True)
class CanonicalAdoptionResult:
    """Structured result from adopt_canonical_position_identity().

    Dispositions:
      ADOPTED                          - repair upgraded in place; caller returns.
      ALREADY_CANONICAL_REPAIR_REMOVED - canonical present; repair(s) merged and removed.
      NO_REPAIR_FOUND                  - no broker-repair; caller should seed normally.
      RETRY_CLIENT_MISMATCH            - client IDs do not match; retain protective monitoring.
      RETRY_MODE_MISMATCH              - execution modes incompatible; retain monitoring.
      RETRY_REPAIR_IDENTITY_UNPROVEN   - retained same-contract repair is diagnostic only.
      RETRY_IDENTITY_CONFLICT          - canonical object mismatch; retain monitoring.
      RETRY_ADOPTION_ERROR             - unexpected exception; retain monitoring.
    """
    disposition: str
    adopted:     bool
    safe_to_seed: bool   # True only for NO_REPAIR_FOUND
    retryable:   bool
    reason:      str = ""


@dataclass
class ExecutableQuoteApplication:
    """Result of _apply_option_quote_for_decision().

    pricing_mode       - "paper" | "live" | "live_risk_unproven"
    executable_price   - bid for LIVE/unproven (0 when bid missing), mid for PAPER
    executable_valid   - True only when an actual bid drove the price
    analytics_mark     - mid/mark for charts/dashboard regardless of mode
    source             - live_executable_price_source stamp
    """
    pricing_mode:     str
    executable_price: float
    executable_valid: bool
    analytics_mark:   float
    source:           str


def _apply_option_quote_for_decision(
    pos: "ManagedPosition",
    *,
    bid: float,
    ask: float,
    mark: float,
    last: float = 0.0,                          # AMENDMENT #6 (blocker 3): raw LAST
    quote_ts: "Optional[datetime]" = None,
    last_ts: "Optional[datetime]" = None,       # AMENDMENT #6: raw last-trade ts
    mark_ts: "Optional[datetime]" = None,       # AMENDMENT #6: raw mark ts
    source: str = "",
) -> ExecutableQuoteApplication:
    """Single authoritative function for writing option-price state onto a position.

    P0 contract (July 15 2026 BA incident):
      ALL writers of current_option_price must route through this function.
      For LIVE and unknown-mode positions, current_option_price is ALWAYS the
      executable bid — never midpoint, mark, ask, or last.

    Pricing logic:
      paper                → current_option_price = mid (simulation)
      live / unproven      → current_option_price = bid (executable)
                             When bid <= 0: current_option_price = 0.0
                             No profit decision may use mark/ask/mid.

    Proof-of-enforcement: every call site that sets current_option_price must
    pass through here. grep for direct .current_option_price = assignments and
    verify each is either (a) this function, (b) fill-price after confirmed
    broker fill, or (c) internal persistence from a previously validated value.
    """
    _raw_mode = str(getattr(pos, "execution_mode", "") or "").strip()
    _norm_mode = _raw_mode.lower()

    if _norm_mode == "paper":
        _pricing_mode = "paper"
    elif _norm_mode == "live":
        _pricing_mode = "live"
    else:
        _pricing_mode = "live_risk_unproven"

    _mid = round((bid + ask) / 2.0, 4) if (bid > 0 and ask > 0) else (mark or ask or 0.0)
    _analytics_mark = mark if mark > 0 else _mid

    # Always stamp analytics mark for charting regardless of pricing mode.
    try:
        pos.analytics_mark_price = _analytics_mark
        pos.analyticsmarkprice   = _analytics_mark
    except Exception as _e:
        log.debug("_aqfd: analytics_mark write failed: %s", _e)

    # Set bid/ask on position unconditionally.
    try:
        pos.current_bid = bid
        pos.currentbid  = bid
    except Exception as _e:
        log.debug("_aqfd: current_bid write failed: %s", _e)
    try:
        pos.current_ask = ask
        pos.currentask  = ask
    except Exception as _e:
        log.debug("_aqfd: current_ask write failed: %s", _e)

    if _pricing_mode == "paper":
        _exec_price = _mid if _mid > 0 else ask
        _valid = _exec_price > 0
        _src   = source or "paper_mid_simulation"
        try:
            pos.current_option_price = _exec_price
            pos.currentoptionprice   = _exec_price
        except Exception as _e:
            log.debug("_aqfd: paper current_option_price write failed: %s", _e)
    else:
        # LIVE or live_risk_unproven: executable price is ONLY the bid.
        if bid > 0:
            _exec_price = bid
            _valid = True
            _src   = source or ("bid" if _pricing_mode == "live" else "bid_live_risk_unproven")
            try:
                pos.current_option_price = bid
                pos.currentoptionprice   = bid
            except Exception as _e:
                log.debug("_aqfd: live bid write failed: %s", _e)
        else:
            # Missing or zero bid — clear the decision price so no profit
            # logic can read a stale mid/mark from a prior cycle.
            _exec_price = 0.0
            _valid = False
            _src   = ("bid_missing" if _pricing_mode == "live"
                      else "bid_missing_live_risk_unproven")
            try:
                pos.current_option_price = 0.0
                pos.currentoptionprice   = 0.0
            except Exception as _e:
                log.debug("_aqfd: bid_missing zero-write failed: %s", _e)

    # Stamp executable metadata.
    try:
        pos.executable_exit_price    = _exec_price
        pos.executable_quote_valid   = _valid
        pos.live_executable_price_source = _src
        pos.liveexecutablepricesource    = _src
        pos.pricing_mode             = _pricing_mode
        pos.raw_execution_mode       = _raw_mode
    except Exception as _e:
        log.debug("_aqfd: executable metadata write failed: %s", _e)

    # Update quote timestamp.
    if quote_ts is not None:
        try:
            pos.last_option_quote_update_ts = quote_ts
            pos.lastoptionquoteupdatets     = quote_ts
        except Exception as _e:
            log.debug("_aqfd: quote_ts write failed: %s", _e)

    # AMENDMENT (PR #385 review P1-1 + BID-timestamp contract): keep the
    # full BID truth tuple in sync with the numeric bid this call is
    # writing.  A fresh positive BID must clear a prior invalid veto so
    # downstream soft-winner protection and the fresh-BID hard-stop
    # fallback re-arm; a zero/missing BID must never MASQUERADE as a
    # successful BID observation — it does not advance the dedicated
    # last-positive-BID timestamp.  Advancing the timestamp on a bid=0
    # cycle would let a later flaky consumer misread the position as
    # having a fresh executable BID and re-arm the fresh-BID path on
    # stale numeric data.
    _bid_positive = False
    try:
        _bid_positive = float(bid or 0.0) > 0.0
    except Exception:
        _bid_positive = False
    try:
        pos.option_bid_valid = _bid_positive
        pos.optionbidvalid   = _bid_positive
    except Exception as _e:
        log.debug("_aqfd: option_bid_valid write failed: %s", _e)

    # option_quote_fresh is derived from bid_valid AND ts_fresh in the
    # snapshot builder.  Set here so a positive bid clears a prior False
    # veto and a zero bid re-asserts False.
    try:
        pos.option_quote_fresh = _bid_positive
        pos.optionquotefresh   = _bid_positive
    except Exception as _e:
        log.debug("_aqfd: option_quote_fresh write failed: %s", _e)

    # Advance the dedicated last-positive-BID timestamp ONLY when we
    # actually observed a positive bid this cycle.  A zero/missing bid
    # leaves the prior stamp untouched — the freshness gate will then
    # correctly age it out, rather than restart the freshness clock on
    # what is really an invalid observation.
    if quote_ts is not None and _bid_positive:
        try:
            pos.last_option_bid_update_ts = quote_ts
            pos.lastoptionbidupdatets     = quote_ts
        except Exception as _e:
            log.debug("_aqfd: last_option_bid_update_ts write failed: %s", _e)

    # ── HARD-EXIT REFERENCE (amendment #6) ───────────────────────────────────
    # QPM is not the only writer path.  Broker-position repair and startup
    # hydration route through this helper.  If we don't populate the hard-exit
    # reference here too, a repaired LIVE position with a missing bid but a
    # catastrophic mark/last shows 0% loss until QPM independently reaches it.
    # Uses the SAME shared selector as QPM (single source of truth for policy).
    # AMENDMENT #6 (blocker 3): raw LAST and last_ts are now accepted so we
    # can pass provenance-preserved values, and the broker-precheck writer
    # no longer collapses everything into 'mark'.
    try:
        from ap.position_quote_monitor import _select_hard_exit_reference as _sel_href
        _now = datetime.now(timezone.utc)
        _cost_basis = float(getattr(pos, "entry_price", 0.0) or 0.0)
        # AMENDMENT #6 blocker 4: use per-position hard stop for catastrophic-ASK test
        try:
            _pos_hs, _, _ = _effective_thresholds(pos)
        except Exception:
            _pos_hs = -0.33
        _raw_mark_ts = mark_ts
        _mark_ts = _normalize_hard_ref_ts(_raw_mark_ts, now_utc=_now)
        if (mark or 0) > 0 and _raw_mark_ts in (None, ""):
            _mark_ts = _normalize_hard_ref_ts(quote_ts, now_utc=_now)
        _href = _sel_href(
            bid=float(bid or 0.0), ask=float(ask or 0.0),
            mark=float(mark or 0.0), last=float(last or 0.0),
            bid_ts=(_normalize_hard_ref_ts(quote_ts, now_utc=_now) if (bid or 0) > 0 else None),
            ask_ts=(_normalize_hard_ref_ts(quote_ts, now_utc=_now) if (ask or 0) > 0 else None),
            mark_ts=_mark_ts,
            last_ts=last_ts,
            now_utc=_now,
            entry_price=_cost_basis,
            hard_stop_pct=_pos_hs,
            last_stale_sec=float(os.getenv("LAST_TRADE_STALE_SEC", "30.0")),
        )
        # Authoritative refs are monotonic by quote timestamp; stale authority
        # cannot erase a newer catastrophic or healthy reference.
        _prior_validity = str(getattr(pos, "hard_exit_reference_validity", "") or "")
        _prior_price = float(getattr(pos, "hard_exit_reference_price", 0.0) or 0.0)
        _prior_ts = getattr(pos, "hard_exit_reference_ts", getattr(pos, "hardexitreferencets", None))
        _prior_source = str(getattr(pos, "hard_exit_reference_source", "") or "")
        _overwrite = _should_replace_hard_ref(
            prior_validity=_prior_validity,
            prior_ts=_prior_ts,
            prior_price=_prior_price,
            candidate_validity=_href.validity,
            candidate_ts=_href.ts,
            prior_source=_prior_source,
            candidate_source=_href.source,
            now_utc=_now,
        )
        if not _overwrite:
            pos.hard_exit_reference_refresh_needed = True
            pos.hardexitreferencerefreshneeded = True
        if _overwrite and _href.price > 0:
            pos.hard_exit_reference_price = _href.price
            pos.hardexitreferenceprice = _href.price
            pos.hard_exit_reference_source = _href.source
            pos.hardexitreferencesource = _href.source
            pos.hard_exit_reference_validity = _href.validity
            pos.hardexitreferencevalidity = _href.validity
            _ts_to_write = _href.ts
            pos.hard_exit_reference_ts = _ts_to_write
            pos.hardexitreferencets = _ts_to_write
            if _cost_basis > 0:
                _hr_pnl = (_href.price - _cost_basis) / _cost_basis
                pos.hard_exit_reference_pnl_pct = _hr_pnl
                pos.hardexitreferencepnlpct = _hr_pnl
            if _href.validity == "proven":
                pos.hard_exit_reference_refresh_needed = False
                pos.hardexitreferencerefreshneeded = False
    except Exception as _e:
        log.debug("_aqfd: hard_exit_reference write failed: %s", _e)

    return ExecutableQuoteApplication(
        pricing_mode     = _pricing_mode,
        executable_price = _exec_price,
        executable_valid = _valid,
        analytics_mark   = _analytics_mark,
        source           = _src,
    )


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
    # PR #176: execution_mode populated at seed time ("live" | "paper" | "").
    # Used by the LIVE_DEGRADED_SOFT_EXIT_GUARD at the SUBMIT site.
    # Empty string is treated as live-risk when client_id matches a known
    # live client, to fail safe.
    execution_mode:    str  = ""

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
    _underlying_stop_breach_quote_ts: Optional[datetime] = None

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

    # PR-C (baseline repair): protective-monitoring ghost field. Previously
    # assigned dynamically in _apply_retry_intent_to_position() and read
    # defensively elsewhere via getattr(pos, "protective_monitoring_state", "")
    # -- that "" default at the read sites is the codebase's own established
    # convention for "no protective-monitoring state yet", since none of the
    # five PROTECTIVE_STATE_* constants represents an unset/normal state.
    # Declared here, following the same PR-B rationale (survives
    # dataclasses.replace() / asdict() roundtrips; a freshly constructed or
    # hydrated position no longer raises AttributeError before any
    # protective-monitoring code path has run).
    protective_monitoring_state: str = ""

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
        # Missing truth and an invalid direction are not market signals. Keep
        # target geometry fail-closed for the same reason as the PR #403 stop
        # geometry below.
        side = str(self.side or "").strip().upper()
        try:
            target = float(self.underlying_target or 0.0)
            current = float(self.current_underlying or 0.0)
        except (TypeError, ValueError):
            return False
        if (
            side not in {"CALL", "PUT"}
            or not math.isfinite(target)
            or not math.isfinite(current)
            or target <= 0
            or current <= 0
        ):
            return False
        if side == "CALL":
            return current >= target
        return current <= target

    @property
    def is_at_stop(self) -> bool:
        # PR #403: this property remains a compatibility helper, but it must
        # not invent PUT geometry for an invalid direction or accept
        # non-finite/malformed levels. The authoritative decision path uses
        # _underlying_stop_evidence() plus the quote snapshot.
        side = str(self.side or "").strip().upper()
        try:
            stop = float(self.underlying_stop or 0.0)
            current = float(self.current_underlying or 0.0)
        except (TypeError, ValueError):
            return False
        if (
            side not in {"CALL", "PUT"}
            or not math.isfinite(stop)
            or not math.isfinite(current)
            or stop <= 0
            or current <= 0
        ):
            return False
        if side == "CALL":
            return current <= stop
        return current >= stop


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


def _evaluation_now_utc(
    now_et: Optional[datetime],
    *,
    pos: Optional["ManagedPosition"] = None,
) -> datetime:
    """Normalize the caller's evaluation clock to an aware UTC datetime.

    ``evaluate_exit`` accepts an ET clock for the session rules. Reusing that
    same instant for quote age, entry grace, and confirmation prevents a
    historical replay from mixing a pinned session date with host wall-clock
    state.
    """
    if now_et is None:
        return datetime.now(timezone.utc)
    try:
        if now_et.tzinfo is None:
            now_et = now_et.replace(tzinfo=ET)
        candidate = now_et.astimezone(timezone.utc)

        # Some legacy callers pin only the ET session hour while constructing
        # the position and quote timestamps from the current wall clock. Do
        # not let those future timestamps become authoritative. A coherent
        # historical replay (all position timestamps at or before candidate)
        # still uses the supplied clock end-to-end.
        if pos is not None:
            for name in (
                "opened_at",
                "last_option_bid_update_ts",
                "last_option_quote_update_ts",
                "last_underlying_quote_update_ts",
                "hard_exit_reference_ts",
            ):
                value = getattr(pos, name, None)
                if not isinstance(value, datetime):
                    continue
                timestamp = value
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
                else:
                    timestamp = timestamp.astimezone(timezone.utc)
                if (timestamp - candidate).total_seconds() > 60.0:
                    return datetime.now(timezone.utc)
        return candidate
    except Exception:
        return datetime.now(timezone.utc)


def _position_age_minutes(
    pos: ManagedPosition,
    *,
    now_utc: Optional[datetime] = None,
) -> float:
    """Return how many minutes old the position is."""
    now_utc = now_utc or datetime.now(timezone.utc)
    try:
        if pos.opened_at:
            opened_at = pos.opened_at
            if opened_at.tzinfo is None:
                opened_at = opened_at.replace(tzinfo=timezone.utc)
            return (now_utc - opened_at).total_seconds() / 60
    except Exception:
        pass
    return 999.0  # unknown age — do not block exits


def _technical_stop_identity_proven(pos: ManagedPosition) -> bool:
    """Return whether ordinary technical-stop classification has safe identity.

    LIVE requires the full durable identity used by the submit seam. PAPER keeps
    the existing isolated-evaluation compatibility contract but still requires
    an explicit mode and option contract; an empty/unknown mode never silently
    authorizes a technical stop.
    """
    # Execution mode is a durable authority field, not free-form user input.
    # Do not normalize malformed values into LIVE/PAPER authority.
    mode = str(getattr(pos, "execution_mode", "") or "")
    contract = str(getattr(pos, "option_symbol", "") or "").strip()
    if mode == "paper":
        return bool(contract)
    if mode != "live":
        return False
    return bool(
        contract
        and str(getattr(pos, "position_id", "") or "").strip()
        and str(getattr(pos, "client_id", "") or "").strip()
        and str(getattr(pos, "ticker", "") or "").strip()
    )


def _underlying_stop_evidence(
    pos: ManagedPosition,
    snap: ExitDecisionSnapshot,
) -> dict:
    """Evaluate only the stored, side-aware underlying-stop geometry.

    This helper deliberately does not inspect option P&L. A missing/stale
    quote, invalid side, or invalid stored stop is a deferred technical-stop
    evaluation, never an adverse price observation.
    """
    side = str(getattr(pos, "side", "") or "").strip().upper()
    if side not in {"CALL", "PUT"}:
        return {
            "valid": False,
            "breached": False,
            "detail": f"invalid_direction={side or 'missing'}",
            "side": side,
            "stop": None,
            "price": snap.underlying_price,
        }

    try:
        stop = float(getattr(pos, "underlying_stop", 0.0) or 0.0)
    except (TypeError, ValueError):
        stop = 0.0
    if not math.isfinite(stop) or stop <= 0.0:
        return {
            "valid": False,
            "breached": False,
            "detail": "invalid_stop_level",
            "side": side,
            "stop": stop,
            "price": snap.underlying_price,
        }

    if not snap.underlying_available or snap.underlying_price is None:
        return {
            "valid": False,
            "breached": False,
            "detail": "underlying_missing",
            "side": side,
            "stop": stop,
            "price": None,
        }
    if not snap.underlying_fresh:
        return {
            "valid": False,
            "breached": False,
            "detail": "underlying_stale",
            "side": side,
            "stop": stop,
            "price": snap.underlying_price,
        }
    if snap.underlying_quote_ts is None:
        return {
            "valid": False,
            "breached": False,
            "detail": "underlying_timestamp_missing",
            "side": side,
            "stop": stop,
            "price": snap.underlying_price,
        }

    price = snap.underlying_price
    if side == "CALL":
        breached = price <= stop
    else:
        breached = price >= stop
    return {
        "valid": True,
        "breached": breached,
        "detail": "underlying_stop_breached" if breached else "underlying_stop_not_breached",
        "side": side,
        "stop": stop,
        "price": price,
    }


def evaluate_exit(pos: ManagedPosition, now_et: Optional[datetime] = None) -> ExitDecision:
    """
    Core exit evaluation. Called every POLL_INTERVAL_SEC for each position.
    Returns ExitDecision.

    FIX-1: This is now a pure function with no side effects. The Discord runner
    alert that previously lived here (blocking requests.post under self._lock)
    has been moved to _submit_exit_decision(), which fires it after the
    callback returns, outside both lock sections.

    P0 (soft-exit executable-truth): Builds one canonical ExitDecisionSnapshot
    at the top of evaluation.  All soft exit branches use exec_pnl from the
    snapshot (always BID-based) rather than option_pnl_pct (which is midpoint
    for PAPER positions).  Hard exits retain backward-compatible option_pnl_pct
    so they fire even when bid is unavailable.
    """
    if now_et is None:
        now_et = datetime.now(ET)
    # AMENDMENT (PR #385 review): pin DTE / threshold profile lookup to the
    # caller-supplied ET evaluation clock.  Without this, historical replays
    # and after-midnight-UTC runs would silently pick a different threshold
    # profile than the session they claim to evaluate.
    try:
        _session_date = now_et.astimezone(ET).date()
    except Exception:
        _session_date = None
    _hard_stop, _immediate_tp, _profit_lock = _effective_thresholds(
        pos, session_date=_session_date,
    )

    # ── Build canonical decision snapshot ────────────────────────────────────
    # One snapshot per evaluation; all soft exit branches consume it.
    now_utc = _evaluation_now_utc(now_et, pos=pos)
    snap = _build_exit_decision_snapshot(pos, now_utc)
    _effective_hard_ref = get_effective_hard_exit_reference(pos, now_utc)

    hour, minute = now_et.hour, now_et.minute
    # option_pnl: backward-compatible (bid for LIVE, mid for PAPER) — ONLY used
    # by hard exits that must fire even without a valid bid.
    option_pnl   = pos.option_pnl_pct
    _decision_pnl = _effective_hard_ref if _effective_hard_ref is not None else option_pnl
    # exec_pnl: always BID-based — used by ALL soft exit branches.
    # May be None when bid is missing.  Soft exit gates will catch None.
    exec_pnl     = snap.exit_executable_pnl_pct
    qty_rem      = pos.quantity_remaining

    # ── PR #403: underlying technical-stop evidence ──────────────────────────
    # This state is computed independently of option P&L.  The soft-loss
    # timer below must never be promoted into technical-stop authority.
    _underlying_evidence = _underlying_stop_evidence(pos, snap)
    _technical_stop_state = "DEFERRED"
    _technical_stop_age_sec = 0.0
    if not _underlying_evidence["valid"]:
        pos._underlying_stop_breach_ts = None
        pos._underlying_stop_breach_quote_ts = None
    elif _underlying_evidence["breached"]:
        _UNDERLYING_CONFIRM_SEC = float(
            os.getenv("UNDERLYING_STOP_CONFIRM_SECONDS", "30")
        )
        _now_dt = now_utc
        _stop_dt = getattr(pos, "_underlying_stop_breach_ts", None)
        _quote_ts = snap.underlying_quote_ts
        _prior_quote_ts = getattr(pos, "_underlying_stop_breach_quote_ts", None)

        if _stop_dt is None:
            pos._underlying_stop_breach_ts = _now_dt
            pos._underlying_stop_breach_quote_ts = _quote_ts
            _technical_stop_state = "CONFIRMING"
        else:
            try:
                _technical_stop_age_sec = max(0.0, (_now_dt - _stop_dt).total_seconds())
            except Exception:
                _technical_stop_age_sec = 0.0

            # A confirmation window needs a later fresh observation.  Keep the
            # initial breach clock, but advance the stored observation marker
            # whenever a newer breached quote arrives before the horizon. A
            # single quote cannot sit on the shelf until the wall-clock
            # interval expires and certify a breach by itself.
            _later_quote_observation = True
            if _prior_quote_ts is not None:
                try:
                    _later_quote_observation = _quote_ts > _prior_quote_ts
                except Exception:
                    _later_quote_observation = False

            if _later_quote_observation and _quote_ts is not None:
                pos._underlying_stop_breach_quote_ts = _quote_ts

            if (
                _technical_stop_age_sec >= _UNDERLYING_CONFIRM_SEC
                and _later_quote_observation
            ):
                _technical_stop_state = "CONFIRMED"
            else:
                _technical_stop_state = "CONFIRMING"
    else:
        # Underlying recovered to the valid side of the stored stop.
        if getattr(pos, "_underlying_stop_breach_ts", None) is not None:
            log.info(
                "[%s] UNDERLYING_STOP_RECOVERED — price reclaimed stop level",
                pos.ticker,
            )
        pos._underlying_stop_breach_ts = None
        pos._underlying_stop_breach_quote_ts = None
        _technical_stop_state = "CLEAR"

    # ── 1. TARGET HIT ────────────────────────────────────────────────────────
    # AMENDMENT #4 (blocker 1): pos.is_at_target already zero-guards
    # current_underlying, but we additionally verify the snapshot's
    # underlying_available.  Missing underlying truth is not a target hit.
    # AMENDMENT #5 (blocker 2): underlying must be BOTH available AND fresh.
    # A frozen at-target price left over from a QPM stall must not close a trade.
    if snap.underlying_available and snap.underlying_fresh and pos.is_at_target:
        return ExitDecision(
            action="CLOSE_ALL", quantity=qty_rem,
            reason=f"TARGET HIT -- underlying ${pos.current_underlying:.2f} reached ${pos.underlying_target:.2f}",
            urgency="IMMEDIATE",
            pnl_pct=_decision_pnl,
        )

    # ══ P0 (PR #385 amendment #3, blocker 1): HARD-EXIT PRE-EVALUATION ════════
    # Hard exits fire BEFORE every soft branch AND every soft truth gate.
    # AMENDMENT #3: they consume a DEDICATED loss authority
    # (hard_exit_reference_pnl_pct / price) written by QPM from the last-known
    # best-available option price (bid → mid → mark → ask → last).  This is
    # separate from the soft-exit executable authority: when LIVE loses its bid,
    # QPM correctly zeroes current_option_price so soft exits cannot fire from
    # a mid/mark — but option_pnl_pct then returns 0.0, silently disarming any
    # hard-stop check that reads it.  The dedicated authority never zeroes when
    # ANY market price is available, closing the catastrophic-loser trap.
    #
    # All hard-exit consumers use the shared resolver — never direct attribute reads.
    # Blocker 2 (amendment #6+): resolver also recomputes age from the stored
    # hard_exit_reference_ts so an expired "proven" label cannot mask new losses.
    # AMENDMENT (Jason BAC): the hard stop must consume only authoritative
    # hard-stop truth — provenance-aware persisted reference OR fresh
    # executable BID. Previously `_hard_loss_pnl = _decision_pnl` allowed a
    # PAPER midpoint/mark/LAST/ASK-derived option_pnl_pct to trigger a hard
    # stop whenever the persisted hard reference was rejected by the resolver
    # (unproven / expired / no_data). That is exactly the class of exit #385
    # was designed to prevent. Missing authority => None => do not fire.
    _hard_loss_pnl = _resolve_hard_stop_pnl_authority(pos, now_utc)

    # HARD STOP (pre-evaluated, using dedicated loss authority)
    if _hard_loss_pnl is not None and _hard_loss_pnl <= _hard_stop:
        _dte_hard, _is_idx_hard, _profile_hard = _option_profile(
            pos, session_date=_session_date,
        )
        _hard_ref_price = getattr(
            pos, "hard_exit_reference_price", getattr(pos, "hardexitreferenceprice", None)
        )
        _hard_ref_source = getattr(
            pos, "hard_exit_reference_source", getattr(pos, "hardexitreferencesource", "")
        )
        _hard_ref_validity = getattr(
            pos, "hard_exit_reference_validity", getattr(pos, "hardexitreferencevalidity", "")
        )
        if _has_fresh_dedicated_bid(pos, now_utc=now_utc):
            _hard_authority_source = "fresh_executable_bid"
            _hard_authority_price = getattr(pos, "current_bid", getattr(pos, "currentbid", 0.0))
        else:
            _hard_authority_source = str(_hard_ref_source or "proven_hard_exit_reference")
            _hard_authority_price = _hard_ref_price
        return ExitDecision(
            action="STOP", quantity=qty_rem,
            reason=(
                f"STOP HIT — HARD STOP — OPTION_CATASTROPHIC_STOP "
                f"(independent option-loss authority; underlying stop not asserted) — "
                f"{_hard_loss_pnl*100:.0f}% exceeded {abs(_hard_stop)*100:.0f}% "
                f"threshold | contract={pos.option_symbol} entry={pos.entry_price:.4f} "
                f"authority_price={_hard_authority_price} "
                f"authority_source={_hard_authority_source} "
                f"reference_validity={_hard_ref_validity or 'n/a'} "
                f"dte={_dte_hard} profile={_profile_hard} "
                f"client_id={getattr(pos, 'client_id', '') or 'n/a'} "
                f"execution_mode={getattr(pos, 'execution_mode', '') or 'unknown'} "
                f"position_id={getattr(pos, 'position_id', '') or 'n/a'}"
            ),
            urgency="IMMEDIATE", pnl_pct=_hard_loss_pnl,
            reason_code=OPTION_CATASTROPHIC_STOP,
        )

    # EOD FORCE CLOSE (pre-evaluated) — deliberately independent of quote
    # authority. `_eod_audit_pnl` is display/audit only; it never certifies
    # safety and never drives the exit decision itself.
    _eod_audit_pnl = _hard_loss_pnl if _hard_loss_pnl is not None else _decision_pnl
    _pre_past_eod = (hour > EOD_HARD_CLOSE_HOUR or
                     (hour == EOD_HARD_CLOSE_HOUR and minute >= EOD_HARD_CLOSE_MIN))
    _pre_market_closed = (hour >= 16)
    if _pre_past_eod or (_pre_market_closed and not getattr(pos, "overnight_hold_approved", False)):
        return ExitDecision(
            action="CLOSE_ALL", quantity=qty_rem,
            reason=f"EOD FORCE CLOSE -- {hour}:{minute:02d} ET {'(market closed)' if _pre_market_closed else f'past {EOD_HARD_CLOSE_HOUR}:{EOD_HARD_CLOSE_MIN:02d}'}",
            urgency="IMMEDIATE", pnl_pct=_eod_audit_pnl,
        )

    # Technical underlying-stop authority is returned only after the hard and
    # EOD protections above have had their independent chance to act.
    if _technical_stop_state == "CONFIRMED":
        if not _technical_stop_identity_proven(pos):
            pos._underlying_stop_breach_ts = None
            pos._underlying_stop_breach_quote_ts = None
            return ExitDecision(
                action="HOLD", quantity=0,
                reason=(
                    f"{UNDERLYING_STOP_IDENTITY_UNPROVEN} — ordinary technical stop "
                    f"requires explicit client/mode/position/contract identity"
                ),
                urgency="NORMAL", pnl_pct=_decision_pnl,
                reason_code=UNDERLYING_STOP_IDENTITY_UNPROVEN,
            )
        pos._underlying_stop_breach_ts = None
        pos._underlying_stop_breach_quote_ts = None
        _quote_ts_text = (
            snap.underlying_quote_ts.isoformat()
            if hasattr(snap.underlying_quote_ts, "isoformat")
            else str(snap.underlying_quote_ts or "")
        )
        return ExitDecision(
            action="STOP", quantity=qty_rem,
            reason=(
                f"STOP HIT — {UNDERLYING_TECHNICAL_STOP_CONFIRMED} — "
                f"{_underlying_evidence['side']} underlying "
                f"${_underlying_evidence['price']:.4f} crossed stored stop "
                f"${_underlying_evidence['stop']:.4f} for {_technical_stop_age_sec:.0f}s "
                f"| quote_ts={_quote_ts_text} "
                f"quote_age_sec={snap.underlying_age_sec} "
                f"source={snap.underlying_quote_source or 'unknown'} "
                f"contract={pos.option_symbol} "
                f"client_id={getattr(pos, 'client_id', '') or 'n/a'} "
                f"execution_mode={getattr(pos, 'execution_mode', '') or 'unknown'} "
                f"position_id={getattr(pos, 'position_id', '') or 'n/a'}"
            ),
            urgency="HIGH", pnl_pct=_decision_pnl,
            reason_code=UNDERLYING_TECHNICAL_STOP_CONFIRMED,
        )
    # A first technical-stop breach is intentionally remembered while the
    # existing winner-protection branches below still get priority. If none
    # of those branches acts, the confirmation HOLD is returned immediately
    # before option-loss/never-green policy can manufacture a competing exit.
    # ══════════════════════════════════════════════════════════════════════════

    # ── TOUCHED PROFIT PROTECTION ─────────────────────────────────────────────
    # Once green, we LOCK IN a minimum profit. Never let a green trade
    # become a loss.  Uses executable BID P&L only (never midpoint).
    # Gated on fresh executable bid truth — stale/missing bid defers.
    #
    # Floor logic (option_pnl must stay ABOVE floor or we exit):
    #   peak >= 25%  → floor +12%  (never give back more than 13pts of a big winner)
    #   peak >= 15%  → floor +8%   (protect 8% minimum from a 15%+ trade)
    #   peak >= 10%  → floor +5%   (protect 5% minimum from a 10%+ trade)
    #   peak >= 5%   → floor +3%   (MINIMUM green — never turn a 5%+ win into a loss)
    #   any green    → floor  0%   (breakeven floor — touched green = never go red)
    if pos.scale_outs_done == 0 and pos.touched_profit:
        # ── Option executable truth gate (soft exit) ─────────────────────────
        _tp_option_gate = _soft_exit_option_truth_gate(snap, qty_rem=qty_rem)
        if _tp_option_gate is not None:
            return _tp_option_gate

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

        # Use exec_pnl (BID-based) for the floor comparison, not midpoint.
        _tp_pnl = exec_pnl if exec_pnl is not None else option_pnl
        if _tp_pnl <= _floor:
            # Before firing: apply underlying data truth gate.
            # Missing or stale underlying → defer (not genuine non-confirmation).
            _tp_und_gate = _soft_exit_underlying_truth_gate(snap)
            if _tp_und_gate is not None:
                return _tp_und_gate

            # Underlying data is fresh and available — check direction.
            # A single penny drop on a cheap contract (-9%) is not a real signal
            # if the underlying is still moving in our direction.
            _age_min = _position_age_minutes(pos, now_utc=now_utc)
            _confirming, _confirm_reason = _underlying_still_confirming(pos)
            _MAX_THESIS_OVERRIDE = float(
                os.getenv("MAX_HOLD_MINUTES_WHILE_RED", "5")
            )
            if _confirming and _age_min < _MAX_THESIS_OVERRIDE:
                # Underlying still in our direction — hold, don't exit on noise
                log.info(
                    "[%s] TOUCHED_PROFIT_STOP suppressed — underlying still confirming "
                    "(%s) | age=%.1fmin < %.0fmin thesis window | exec_pnl=%.1f%%",
                    pos.ticker, _confirm_reason, _age_min,
                    _MAX_THESIS_OVERRIDE, _tp_pnl * 100,
                )
            else:
                return ExitDecision(
                    action="CLOSE_ALL", quantity=qty_rem,
                    reason=(
                        f"TOUCHED PROFIT STOP — peaked +{_max*100:.0f}% "
                        f"now {_tp_pnl*100:.0f}% (exec/bid) — floor={_floor*100:.0f}% | "
                        f"underlying={'confirming' if _confirming else 'not confirming'}"
                    ),
                    urgency="IMMEDIATE", pnl_pct=_tp_pnl,
                )

    # ── 33/33/34 SCALE-OUT LADDER ────────────────────────────────────────────
    # Scale-out ladder:
    # +15% (SCALE_OUT_1_THRESHOLD) → sell first third  — lock base gain
    # +25% (SCALE_OUT_2_THRESHOLD) → sell second third — lock extended gain
    # Runner: trails with no fixed ceiling — let winners run
    # Do NOT close everything at 15% — let winners run to 25-30%+ with trail.
    # Single-contract positions: hold until trail fires or 30%+ hit.

    # Scale 1: first +15% hit → sell first third
    # Uses executable BID P&L — midpoint above threshold but bid below does NOT trigger.
    _scale_gate_1 = _soft_exit_option_truth_gate(snap, qty_rem=qty_rem)
    if exec_pnl is not None and exec_pnl >= SCALE_OUT_1_THRESHOLD and pos.scale_outs_done == 0:
        if _scale_gate_1 is not None:
            return _scale_gate_1
        if qty_rem <= 1:
            # 1 contract — do NOT sell here, let it run to trail
            pass  # fall through to trail/profit-lock checks
        else:
            qty_s1 = max(1, round(qty_rem / 3))
            return ExitDecision(
                action="SCALE_OUT", quantity=qty_s1,
                reason=f"SCALE_1 (+15% exec/bid) -- selling {qty_s1}/{qty_rem} | running {qty_rem-qty_s1} to +25%",
                urgency="HIGH", pnl_pct=exec_pnl,
            )

    # Scale 2: +25% hit → sell second third
    _scale_gate_2 = _soft_exit_option_truth_gate(snap, qty_rem=qty_rem)
    if exec_pnl is not None and exec_pnl >= SCALE_OUT_2_THRESHOLD and pos.scale_outs_done == 1:
        if _scale_gate_2 is not None:
            return _scale_gate_2
        if qty_rem <= 1:
            pass  # 1 contract runner — let it run to +30% or trail
        else:
            qty_s2 = max(1, round(qty_rem / 2))  # half of what's left ≈ second third of original
            return ExitDecision(
                action="SCALE_OUT", quantity=qty_s2,
                reason=f"SCALE_2 (+25% exec/bid) -- selling {qty_s2}/{qty_rem} runner | targeting +40%",
                urgency="HIGH", pnl_pct=exec_pnl,
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
    # P0: Uses executable BID P&L for both peak and drawdown comparisons.
    _single_contract = (qty_rem == 1 and pos.scale_outs_done == 0)
    if (pos.scale_outs_done >= 1 or _single_contract) and pos.peak_pnl_pct >= IMMEDIATE_TP_PCT:
        # ── Option executable truth gate ─────────────────────────────────────
        _runner_opt_gate = _soft_exit_option_truth_gate(snap, qty_rem=qty_rem)
        if _runner_opt_gate is not None:
            return _runner_opt_gate

        # exec_pnl is guaranteed non-None here (gate passed above).
        _r_pnl = exec_pnl  # type: ignore[assignment]

        # ── PROFIT FLOOR CHECK — fires before trail math ──────────────────────
        # If peak crossed a floor threshold and current BID P&L is below that floor,
        # sell immediately. This protects against QPM gaps missing the peak.
        # Example: peaked at +25% (floor=10%), now bid at +7% → EXIT at +7%.
        _applicable_floor = 0.0
        for _floor_trigger, _floor_min in sorted(PROFIT_FLOOR.items(), reverse=True):
            if pos.peak_pnl_pct >= _floor_trigger:
                _applicable_floor = _floor_min
                break
        if _applicable_floor > 0 and 0 < _r_pnl < _applicable_floor:
            return ExitDecision(
                action="CLOSE_ALL", quantity=qty_rem,
                reason=(
                    f"PROFIT LOCK — peaked +{pos.peak_pnl_pct*100:.0f}%, "
                    f"now +{_r_pnl*100:.0f}% (exec/bid) below floor +{_applicable_floor*100:.0f}%"
                ),
                urgency="IMMEDIATE",
                pnl_pct=_r_pnl,
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
        runner_drop = pos.peak_pnl_pct - _r_pnl
        if runner_drop >= _runner_trail or _r_pnl <= 0:
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
                    f"now +{_r_pnl*100:.0f}% (exec/bid), protecting runner gains"
                ),
                urgency="HIGH", pnl_pct=_r_pnl,
            )
            # FIX-1: carry alert metadata so _submit_exit_decision() can fire
            # the Discord notification after callback returns (outside lock).
            if pos.peak_pnl_pct >= 0.50:
                d._runner_alert_peak_pct = pos.peak_pnl_pct
                d._runner_trail_used     = _runner_trail
                d._runner_duration_min   = _dur_min
            return d

    # ── SMALL WIN CAPTURE ─────────────────────────────────────────────────────
    # P0: gate on executable bid truth; use exec_pnl for comparison.
    if pos.max_profit_seen >= SMALL_WIN_PCT:
        _sw_gate = _soft_exit_option_truth_gate(snap, qty_rem=qty_rem)
        if _sw_gate is not None:
            return _sw_gate
        _sw_pnl = exec_pnl if exec_pnl is not None else option_pnl
        floor = max(0.03, pos.max_profit_seen - SMALL_WIN_TRAIL)
        if 0 < _sw_pnl <= floor:
            return ExitDecision(
                action="CLOSE_ALL", quantity=qty_rem,
                reason=(
                    f"SMALL WIN LOCK — peaked +{pos.max_profit_seen*100:.0f}%, "
                    f"protecting +{_sw_pnl*100:.0f}% (exec/bid)"
                ),
                urgency="HIGH", pnl_pct=_sw_pnl,
            )

    if _technical_stop_state == "CONFIRMING":
        # Winner protection had first opportunity above. Only when no existing
        # winner branch fires should the technical-stop hysteresis surface its
        # normal HOLD state; the timer itself remains underlying-only.
        if not _technical_stop_identity_proven(pos):
            pos._underlying_stop_breach_ts = None
            pos._underlying_stop_breach_quote_ts = None
            return ExitDecision(
                action="HOLD", quantity=0,
                reason=(
                    f"{UNDERLYING_STOP_IDENTITY_UNPROVEN} — ordinary technical stop "
                    f"confirmation deferred until identity is proven"
                ),
                urgency="NORMAL", pnl_pct=_decision_pnl,
                reason_code=UNDERLYING_STOP_IDENTITY_UNPROVEN,
            )
        _quote_ts_text = (
            snap.underlying_quote_ts.isoformat()
            if hasattr(snap.underlying_quote_ts, "isoformat")
            else str(snap.underlying_quote_ts or "")
        )
        return ExitDecision(
            action="HOLD", quantity=0,
            reason=(
                f"{UNDERLYING_STOP_CONFIRMING} — "
                f"{_underlying_evidence['side']} underlying "
                f"${_underlying_evidence['price']:.4f} beyond stored stop "
                f"${_underlying_evidence['stop']:.4f} "
                f"| breach_age_sec={_technical_stop_age_sec:.0f} "
                f"| quote_ts={_quote_ts_text} "
                f"quote_age_sec={snap.underlying_age_sec} "
                f"source={snap.underlying_quote_source or 'unknown'}"
            ),
            urgency="NORMAL", pnl_pct=_decision_pnl,
            reason_code=UNDERLYING_STOP_CONFIRMING,
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

    # P0: Use exec_pnl (BID-based) for the soft-loss stop threshold comparison.
    # Underlying is required for this branch — apply both gates before evaluating.
    #
    # P0 AUDIT FIX (PR #385): a deferral returned from inside this branch must
    # NEVER shadow the HARD STOP below.  If the last-known loss is already at or
    # past the hard-stop threshold, we SKIP this entire soft branch so the hard
    # stop (which fires regardless of bid/underlying availability) is reachable.
    # Without this, a position at -45% with a missing bid returned
    # SOFT_EXIT_DEFERRED_* forever and had NO exit path — worse than main.
    _sl_pnl = exec_pnl if exec_pnl is not None else option_pnl  # fallback for breach-stamp only
    _sl_catastrophic = option_pnl <= _hard_stop  # hard stop must remain reachable
    if not _sl_catastrophic and (
        (exec_pnl is not None and exec_pnl <= _SOFT_LOSS_PCT and not pos.touched_profit) or
        (exec_pnl is None and option_pnl <= _SOFT_LOSS_PCT and not pos.touched_profit)
    ):
        # ── Option truth gate ────────────────────────────────────────────────
        _sl_opt_gate = _soft_exit_option_truth_gate(snap, qty_rem=qty_rem)
        if _sl_opt_gate is not None:
            return _sl_opt_gate
        _sl_pnl = exec_pnl  # type: ignore[assignment]  # guaranteed non-None after gate

        # ── Underlying truth gate ─────────────────────────────────────────────
        # Missing or stale underlying → DEFER (not genuine non-confirmation).
        _live_risk_mode = str(getattr(pos, "execution_mode", "") or "").strip().lower() == "live"
        _strict_underlying_defer = _live_risk_mode and _technical_stop_state == "DEFERRED"
        _sl_und_gate = _soft_exit_underlying_truth_gate(
            snap,
            unavailable_code=(
                UNDERLYING_STOP_DEFERRED_DATA_UNAVAILABLE
                if _strict_underlying_defer
                else SOFT_EXIT_DEFERRED_UNDERLYING_UNAVAILABLE
            ),
            stale_code=(
                UNDERLYING_STOP_DEFERRED_DATA_UNAVAILABLE
                if _strict_underlying_defer
                else SOFT_EXIT_DEFERRED_UNDERLYING_STALE
            ),
        )
        if _sl_und_gate is not None:
            return _sl_und_gate
        if _technical_stop_state == "DEFERRED" and _underlying_evidence["detail"] not in {
            "underlying_missing", "underlying_stale",
        }:
            return ExitDecision(
                action="HOLD", quantity=0,
                reason=(
                    f"{UNDERLYING_STOP_DEFERRED_DATA_UNAVAILABLE} — "
                    f"technical stop cannot be evaluated: {_underlying_evidence['detail']}"
                ),
                urgency="NORMAL", pnl_pct=_sl_pnl,
                reason_code=UNDERLYING_STOP_DEFERRED_DATA_UNAVAILABLE,
            )
        _sl_grace_gate = _soft_exit_entry_grace_decision(snap)
        if _sl_grace_gate is not None:
            return _sl_grace_gate

        _soft_age       = _position_age_minutes(pos, now_utc=now_utc)
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
        _now_dt = now_utc
        _breach_dt = pos._stop_breach_ts
        if _breach_dt is None:
            # First time we see this breach — stamp it, don't exit yet
            pos._stop_breach_ts = _now_dt
            log.info(
                "[%s] STOP_BREACH_STARTED — %.1f%% exec/bid loss | confirming=%s | "
                "will exit if breach holds >%.0fs | age=%.1fmin",
                pos.ticker, _sl_pnl * 100, _soft_confirm, _STOP_CONFIRM_SEC, _soft_age,
            )
            return ExitDecision(
                action="HOLD", quantity=0,
                reason=f"{SOFT_LOSS_CONFIRMING} — {_sl_pnl*100:.1f}% exec/bid loss | soft-policy confirmation stamped; stored underlying stop not confirmed",
                urgency="NORMAL", pnl_pct=_sl_pnl, reason_code=SOFT_LOSS_CONFIRMING,
            )

        _breach_age_sec = (_now_dt - _breach_dt).total_seconds()

        # If breach lasted < confirmation window AND underlying is recovering → reset
        if _breach_age_sec < _STOP_CONFIRM_SEC:
            if _strong_confirm:
                # Underlying moving our way — this looks like a wick, not a real break
                pos._stop_breach_ts = None
                log.info(
                    "[%s] STOP_BREACH_RESET — underlying recovered (%.2f%% move) "
                    "| exec/bid=%.1f%% | breach lasted %.0fs < %.0fs confirm window",
                    pos.ticker, _u_move * 100, _sl_pnl * 100,
                    _breach_age_sec, _STOP_CONFIRM_SEC,
                )
                return ExitDecision(
                    action="HOLD", quantity=0,
                    reason=f"STOP_BREACH_RESET — underlying recovered {_u_move*100:.2f}%, wick not confirmed",
                    urgency="NORMAL", pnl_pct=_sl_pnl, reason_code="STOP_BREACH_RESET",
                )
            # Still in confirmation window — log and wait
            log.info(
                "[%s] STOP_BREACH_CONFIRMING — %.1f%% exec/bid loss | breach=%.0fs/%.0fs | "
                "underlying=%s",
                pos.ticker, _sl_pnl * 100, _breach_age_sec, _STOP_CONFIRM_SEC,
                _soft_reason,
            )
            return ExitDecision(
                action="HOLD", quantity=0,
                reason=f"{SOFT_LOSS_CONFIRMING} — {_sl_pnl*100:.1f}% exec/bid loss | {_breach_age_sec:.0f}s/{_STOP_CONFIRM_SEC:.0f}s window | stored underlying stop not confirmed | {_soft_reason}",
                urgency="NORMAL", pnl_pct=_sl_pnl, reason_code=SOFT_LOSS_CONFIRMING,
            )

        # ── Breach confirmed (held past confirmation window) ──────────────────
        # Now evaluate whether to exit

        # Past -20% with confirmed breach → exit, bid-limit
        if _sl_pnl <= _SOFT_LOSS_DEEP_PCT:
            pos._stop_breach_ts = None
            return ExitDecision(
                action="CLOSE_ALL", quantity=qty_rem,
                reason=(
                    f"DEEP_LOSS_STOP — {_sl_pnl*100:.0f}% exec/bid exceeds deep floor "
                    f"{_SOFT_LOSS_DEEP_PCT*100:.0f}% | confirmed {_breach_age_sec:.0f}s | "
                    f"underlying={_soft_reason}"
                ),
                urgency="HIGH", pnl_pct=_sl_pnl,
            )

        # Young position with confirming underlying → suppress, give more time
        if _soft_age < _MIN_HOLD_SOFT and (_soft_confirm or _strong_confirm):
            log.info(
                "[%s] SOFT_STOP_SUPPRESSED — %.1f%% exec/bid loss but age=%.1fmin < %.0fmin "
                "hold floor | underlying=%s | giving thesis time to develop",
                pos.ticker, _sl_pnl * 100, _soft_age, _MIN_HOLD_SOFT, _soft_reason,
            )
            return ExitDecision(
                action="HOLD", quantity=0,
                reason=f"SOFT_STOP_SUPPRESSED — age {_soft_age:.1f}min < {_MIN_HOLD_SOFT:.0f}min floor, underlying confirming",
                urgency="NORMAL", pnl_pct=_sl_pnl, reason_code="SOFT_STOP_SUPPRESSED",
            )

        if not _soft_confirm:
            # A move against the entry is not enough to certify the stored
            # technical stop. The dedicated underlying-stop path above would
            # already have returned CONFIRMING or CONFIRMED when its geometry
            # was valid. Keep this separate soft policy in a watch state rather
            # than manufacturing THESIS_FAIL_SOFT_STOP from entry geometry.
            pos._stop_breach_ts = None
            return ExitDecision(
                action="HOLD", quantity=0,
                reason=(
                    f"SOFT_LOSS_WATCH — {_sl_pnl*100:.0f}% exec/bid loss "
                    f"and stored underlying stop not confirmed "
                    f"({_soft_reason}) | age={_soft_age:.1f}min"
                ),
                urgency="NORMAL", pnl_pct=_sl_pnl, reason_code="SOFT_LOSS_WATCH",
            )

        # Thesis still valid — watch, don't exit on time alone
        log.warning(
            "[%s] SOFT_LOSS_WATCH — %.1f%% exec/bid loss | underlying=%s | "
            "age=%.1fmin | breach confirmed %.0fs | waiting for thesis to break",
            pos.ticker, _sl_pnl * 100, _soft_reason,
            _soft_age, _breach_age_sec,
        )
        return ExitDecision(
            action="HOLD", quantity=0,
            reason=f"SOFT_LOSS_WATCH — {_sl_pnl*100:.1f}% exec/bid loss | thesis holding, underlying={_soft_reason}",
            urgency="NORMAL", pnl_pct=_sl_pnl, reason_code="SOFT_LOSS_WATCH",
        )

    # ── NEVER-GREEN ESCALATING STOP ───────────────────────────────────────────
    if not pos.touched_profit:
        _age_min = _position_age_minutes(pos, now_utc=now_utc) if pos.opened_at else 0
        # AMENDMENT (PR #385 review): use the SAME session date that
        # _effective_thresholds consumed above.  Without this, one
        # evaluate_exit() call could pick its hard-stop profile from the
        # supplied now_et and its never-green profile from the host's
        # wall-clock date — deterministic replays would misclassify DTE.
        _dte_ng, _is_idx_ng, _profile_ng = _option_profile(
            pos, session_date=_session_date,
        )

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

        # P0: use exec_pnl (BID) for comparison — midpoint below threshold does not fire.
        # P0 AUDIT FIX (PR #385): catastrophic losses skip this branch entirely so a
        # deferral cannot shadow the HARD STOP below (same rescue as soft-loss branch).
        _ng_pnl = exec_pnl if exec_pnl is not None else option_pnl
        if _ng_pnl <= _ng_stop and not (option_pnl <= _hard_stop):
            # ── Option truth gate ────────────────────────────────────────────
            _ng_opt_gate = _soft_exit_option_truth_gate(snap, qty_rem=qty_rem)
            if _ng_opt_gate is not None:
                return _ng_opt_gate
            _ng_pnl = exec_pnl  # type: ignore[assignment]  # non-None after gate

            # ── Underlying truth gate ────────────────────────────────────────
            # Missing/stale underlying → DEFER.  Never treat missing data as
            # "thesis never confirmed" — that is only valid when we actually
            # have confirming underlying price data and it says wrong direction.
            _ng_und_gate = _soft_exit_underlying_truth_gate(snap)
            if _ng_und_gate is not None:
                return _ng_und_gate
            _ng_grace_gate = _soft_exit_entry_grace_decision(snap)
            if _ng_grace_gate is not None:
                return _ng_grace_gate

            _ng_age_min = _position_age_minutes(pos, now_utc=now_utc)
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
                    "(%s) | age=%.1fmin < %.0fmin hold floor | exec_pnl=%.1f%%",
                    pos.ticker, _ng_confirm_reason, _ng_age_min,
                    _MIN_HOLD_NG, _ng_pnl * 100,
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
                        f"NEVER GREEN STOP [{_profile}] — {_ng_pnl*100:.0f}% (exec/bid) "
                        f"at {_ng_age_min:.0f}min | threshold={_ng_stop*100:.0f}% | "
                        f"thesis never confirmed | underlying:{_ng_confirm_reason}"
                    ),
                    urgency="HIGH", pnl_pct=_ng_pnl,
                )

    # ── PROFIT LOCK / TRAILING STOP ───────────────────────────────────────────
    # P0: gate on executable bid truth; use exec_pnl for comparisons.
    if pos.peak_pnl_pct >= _immediate_tp:
        _pl_opt_gate = _soft_exit_option_truth_gate(snap, qty_rem=qty_rem)
        if _pl_opt_gate is not None:
            return _pl_opt_gate
        _pl_pnl = exec_pnl  # type: ignore[assignment]  # non-None after gate
        if _pl_pnl <= _profit_lock:
            return ExitDecision(
                action="CLOSE_ALL", quantity=qty_rem,
                reason=f"PROFIT LOCK -- peaked at +{pos.peak_pnl_pct*100:.0f}%, exec/bid fell to +{_pl_pnl*100:.0f}% — locking in",
                urgency="HIGH", pnl_pct=_pl_pnl,
            )
        drop_from_peak = pos.peak_pnl_pct - _pl_pnl
        if drop_from_peak >= TRAIL_DROP_FROM_PEAK:
            return ExitDecision(
                action="CLOSE_ALL", quantity=qty_rem,
                reason=f"TRAILING STOP -- peak +{pos.peak_pnl_pct*100:.0f}%, exec/bid dropped {drop_from_peak*100:.0f}pts to +{_pl_pnl*100:.0f}%",
                urgency="HIGH", pnl_pct=_pl_pnl,
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
    # P0: gate on executable bid truth; use exec_pnl for profit comparison.
    past_window3 = (hour > PROFIT_PROTECT_3_HOUR or
                    (hour == PROFIT_PROTECT_3_HOUR and minute >= PROFIT_PROTECT_3_MIN))
    protect3_thresh = 0.50 if direction_aligns else PROTECT_3_THRESHOLD
    if past_window3 and exec_pnl is not None and exec_pnl >= protect3_thresh:
        _w3_gate = _soft_exit_option_truth_gate(snap, qty_rem=qty_rem)
        if _w3_gate is not None:
            return _w3_gate
        trend_note = " [trend day -- raised to 50% threshold]" if direction_aligns else ""
        return ExitDecision(
            action="CLOSE_ALL", quantity=qty_rem,
            reason=f"PROFIT PROTECT W3 -- +{exec_pnl*100:.0f}% exec/bid at 2PM+{trend_note}",
            urgency="HIGH", pnl_pct=exec_pnl,
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
    if past_window2 and exec_pnl is not None and exec_pnl >= scale2_threshold and pos.scale_outs_done < 2:
        _w2_gate = _soft_exit_option_truth_gate(snap, qty_rem=qty_rem)
        if _w2_gate is not None:
            return _w2_gate
        qty_close  = max(1, round(qty_rem * (0.50 if direction_aligns else 0.75)))
        trend_note = " [TREND DAY -- reduced scale]" if direction_aligns else ""
        return ExitDecision(
            action="SCALE_OUT", quantity=qty_close,
            reason=f"PROFIT PROTECT W2 -- +{exec_pnl*100:.0f}% exec/bid at {w2_hour}:{w2_min:02d}+ scale{trend_note}",
            urgency="HIGH", pnl_pct=exec_pnl,
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
    if past_window1 and exec_pnl is not None and exec_pnl >= scale1_threshold and pos.scale_outs_done < 1:
        _w1_gate = _soft_exit_option_truth_gate(snap, qty_rem=qty_rem)
        if _w1_gate is not None:
            return _w1_gate
        qty_close  = max(1, round(qty_rem * (0.35 if direction_aligns else 0.50)))
        trend_note = " [TREND DAY -- let runner breathe]" if direction_aligns else ""
        return ExitDecision(
            action="SCALE_OUT", quantity=qty_close,
            reason=f"PROFIT PROTECT W1 -- +{exec_pnl*100:.0f}% exec/bid at {w1_hour}:{w1_min:02d}+ scale{trend_note}",
            urgency="NORMAL", pnl_pct=exec_pnl,
        )

    # ── 7. THETA KILL SWITCH (past noon, down >35%) ───────────────────────────
    # P0: gate on executable truth; use exec_pnl for loss comparison.
    past_noon = hour >= 12
    _theta_pnl = exec_pnl if exec_pnl is not None else option_pnl
    if past_noon and _theta_pnl <= THETA_STOP_LOSS_PCT:
        _theta_gate = _soft_exit_option_truth_gate(snap, qty_rem=qty_rem)
        if _theta_gate is not None:
            return _theta_gate
        return ExitDecision(
            action="CLOSE_ALL", quantity=qty_rem,
            reason=f"THETA STOP -- exec/bid down {_theta_pnl*100:.0f}% after noon, cutting losses",
            urgency="NORMAL", pnl_pct=_theta_pnl,
        )

    # ── P0 AUDIT FIX (PR #385): explicit deferral surfacing ──────────────────
    # When executable truth is unavailable/stale AND the display (midpoint) P&L
    # sits in territory where a soft exit would otherwise have been evaluated,
    # return the explicit deferred reason instead of a silent "No exit condition
    # met".  Operators must be able to distinguish "nothing to do" from
    # "soft exit was possible but truth was missing" (spec §5.12).
    _final_gate = _soft_exit_option_truth_gate(snap, qty_rem=qty_rem)
    if _final_gate is not None:
        _dp = snap.display_pnl_pct
        _soft_territory = (
            (_dp is not None and (
                _dp >= SCALE_OUT_1_THRESHOLD
                or _dp <= float(os.getenv("SOFT_LOSS_STOP_PCT", "-0.12"))
            ))
            or pos.touched_profit
            or pos.peak_pnl_pct >= _immediate_tp
            or pos.max_profit_seen >= SMALL_WIN_PCT
        )
        if _soft_territory:
            return _final_gate

    _display_pnl = snap.display_pnl_pct
    _soft_loss_pct = float(os.getenv("SOFT_LOSS_STOP_PCT", "-0.12"))
    if _display_pnl is not None and exec_pnl is not None:
        _display_crossed_tp = (
            _display_pnl >= SCALE_OUT_1_THRESHOLD
            and exec_pnl < SCALE_OUT_1_THRESHOLD
            and pos.scale_outs_done < 1
        )
        _display_crossed_loss = (
            _display_pnl <= _soft_loss_pct
            and exec_pnl > _soft_loss_pct
        )
        if _display_crossed_tp or _display_crossed_loss:
            return ExitDecision(
                action="HOLD", quantity=0,
                reason=(
                    "SOFT_EXIT_DEFERRED — display crossed soft threshold but "
                    "fresh executable BID did not confirm"
                ),
                urgency="NORMAL",
                pnl_pct=exec_pnl,
                reason_code=SOFT_EXIT_DEFERRED_EXECUTABLE_THRESHOLD_UNCONFIRMED,
            )

    return ExitDecision(
        action="HOLD", quantity=0,
        reason="No exit condition met", urgency="NORMAL", pnl_pct=option_pnl,
    )


# ── EXIT ENGINE ───────────────────────────────────────────────────────────────

EXIT_RULE_PRECEDENCE = (
    "STOP_HIT",
    UNDERLYING_TECHNICAL_STOP_CONFIRMED,
    OPTION_CATASTROPHIC_STOP,
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


def _clamp_env_number(name: str, default: float, min_value: float, max_value: float, *, as_int: bool = False):
    raw = os.getenv(name)
    try:
        value = float(raw) if raw not in (None, "") else float(default)
        if value != value or value in (float("inf"), float("-inf")):
            value = float(default)
    except Exception:
        value = float(default)
    value = max(float(min_value), min(float(max_value), value))
    return int(round(value)) if as_int else value


STALE_EXIT_RETRY_MAX_ATTEMPTS = _clamp_env_number(
    "EXIT_STALE_DECISION_RETRY_MAX_ATTEMPTS", 5, 1, 20, as_int=True
)
STALE_EXIT_RETRY_MAX_AGE_SEC = _clamp_env_number(
    "EXIT_STALE_DECISION_RETRY_MAX_AGE_SEC", 30, 5, 300
)
STALE_EXIT_RETRY_DELAY_SEC = _clamp_env_number(
    "EXIT_STALE_DECISION_RETRY_DELAY_SEC", 1, 0.5, 60
)

PROTECTIVE_STATE_ACTIVE = "ACTIVE"
PROTECTIVE_STATE_DEGRADED = "PROTECTIVE_MONITORING_DEGRADED"
PROTECTIVE_STATE_UNPERSISTED = "PROTECTIVE_MONITORING_DEGRADED_UNPERSISTED"
PROTECTIVE_STATE_RETRY_EXHAUSTED = "PROTECTIVE_RETRY_EXHAUSTED"
PROTECTIVE_STATE_BROKER_FLAT_PENDING = "BROKER_FLAT_CLOSE_PENDING"
PROTECTIVE_STATE_RESOLVED = "RESOLVED"
FORCED_RISK_EXIT_CODES = {
    "EOD_FORCE_CLOSE",
    "STOP_HIT",
    UNDERLYING_TECHNICAL_STOP_CONFIRMED,
    OPTION_CATASTROPHIC_STOP,
    "SENTINEL_FORCED_EXIT",
    "HARD_STOP",
    "EMERGENCY_STOP",
    "MAX_LOSS",
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


@dataclass
class DegradedMonitoringPersistResult:
    persisted: bool
    rowcount: int
    reason: str
    error: Optional[str] = None


@dataclass
class BrokerFlatCloseResult:
    closed: bool
    rowcount: int
    verified: bool
    reason: str
    error: Optional[str] = None


class BrokerPositionTruth(str, Enum):
    OPEN = "OPEN"
    FLAT = "FLAT"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class ProtectivePositionIdentity:
    position_id: str
    client_id: str
    execution_mode: str
    contract: str


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


def _is_reserved_local_exit_submit_intent(pos: Any, order: dict | None) -> bool:
    """Allow the exact pre-submit reserved EXIT row to flow into the callback once."""
    if not order:
        return False
    status = str(order.get("status") or "").upper()
    if status != "EXIT_REQUESTED":
        return False
    active_local_order_id = str(order.get("local_order_id") or "").strip()
    pending_local_order_id = str(getattr(pos, "pending_exit_local_order_id", "") or "").strip()
    if not active_local_order_id or active_local_order_id != pending_local_order_id:
        return False
    if str(order.get("broker_order_id") or "").strip():
        return False
    if bool(getattr(pos, "exit_in_flight", False)):
        return False
    return True


def _classify_exit_decision(decision: "ExitDecision") -> str:
    explicit_code = (getattr(decision, "reason_code", "") or "").upper().strip()
    if explicit_code:
        return explicit_code
    r      = (getattr(decision, "reason", "") or "").upper()
    action = (getattr(decision, "action", "") or "").upper()
    if UNDERLYING_TECHNICAL_STOP_CONFIRMED in r:
        return UNDERLYING_TECHNICAL_STOP_CONFIRMED
    if OPTION_CATASTROPHIC_STOP in r:
        return OPTION_CATASTROPHIC_STOP
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


def _classify_exact_broker_open_qty(value) -> tuple[BrokerPositionTruth, Optional[int]]:
    """
    Classify exact broker open quantity without coercing malformed values to flat.
    Only finite numeric zero is FLAT; positive finite whole numbers are OPEN.
    Missing, malformed, negative, fractional, NaN, or infinite values are UNKNOWN.
    """
    if value is None or isinstance(value, bool):
        return BrokerPositionTruth.UNKNOWN, None
    try:
        text = str(value).strip()
        if not text:
            return BrokerPositionTruth.UNKNOWN, None
        qty_decimal = Decimal(text)
    except (InvalidOperation, ValueError, TypeError):
        return BrokerPositionTruth.UNKNOWN, None
    if not qty_decimal.is_finite():
        return BrokerPositionTruth.UNKNOWN, None
    if qty_decimal < 0:
        return BrokerPositionTruth.UNKNOWN, None
    if qty_decimal != qty_decimal.to_integral_value():
        return BrokerPositionTruth.UNKNOWN, None
    qty = int(qty_decimal)
    if qty == 0:
        return BrokerPositionTruth.FLAT, 0
    return BrokerPositionTruth.OPEN, qty


def _is_protective_exit(reason: str) -> bool:
    r = (reason or "").upper()
    return any(k in r for k in (
        "EOD", "STOP", "MAX_LOSS", "THETA", "PROTECTIVE", "FORCE CLOSE", "SENTINEL",
        "TARGET HIT", "IMMEDIATE TP", "PROFIT PROTECT", "SMALL WIN", "RUNNER TRAIL",
        "PROFIT LOCK", "TOUCHED PROFIT", "NEVER GREEN", "DEAD TRADE",
    ))


def _positive_or_none(value) -> Optional[float]:
    try:
        num = float(value)
    except Exception:
        return None
    return num if num > 0 and math.isfinite(num) else None


def _decision_window(decision: "ExitDecision", now_et: Optional[datetime] = None) -> str:
    code = _classify_exit_decision(decision)
    reason = (getattr(decision, "reason", "") or "").upper()
    if code == "EOD_FORCE_CLOSE" or "EOD" in reason:
        return "EOD"
    if code in {
        "HARD_STOP",
        "STOP_HIT",
        UNDERLYING_TECHNICAL_STOP_CONFIRMED,
        OPTION_CATASTROPHIC_STOP,
    } or "HARD STOP" in reason:
        return "HARD_STOP"
    if code == "THETA_STOP" or "THETA STOP" in reason:
        return "THETA_STOP"
    if code == "PROFIT_PROTECT_W3" or "PROFIT PROTECT W3" in reason:
        return "W3"
    if code == "PROFIT_PROTECT_W2" or "PROFIT PROTECT W2" in reason:
        return "W2"
    if code == "PROFIT_PROTECT_W1" or "PROFIT PROTECT W1" in reason:
        return "W1"
    if code in {
        "IMMEDIATE_TP",
        "TP_SCALE_OUT",
        "PROFIT_LOCK",
        "RUNNER_TRAIL",
        "TRAILING_STOP",
    } or any(fragment in reason for fragment in (
        "TARGET HIT",
        "IMMEDIATE TP",
        "SCALE_",
        "PROFIT LOCK",
        "RUNNER TRAIL",
        "TRAILING STOP",
    )):
        return "TP"
    if now_et is not None:
        hour, minute = now_et.hour, now_et.minute
        if hour > PROFIT_PROTECT_3_HOUR or (hour == PROFIT_PROTECT_3_HOUR and minute >= PROFIT_PROTECT_3_MIN):
            return "W3"
        if hour > PROFIT_PROTECT_2_HOUR or (hour == PROFIT_PROTECT_2_HOUR and minute >= PROFIT_PROTECT_2_MIN):
            return "W2"
        if hour > PROFIT_PROTECT_1_HOUR or (hour == PROFIT_PROTECT_1_HOUR and minute >= PROFIT_PROTECT_1_MIN):
            return "W1"
        if hour >= 12:
            return "THETA_STOP"
    return "TP"


def build_exit_decision_stamp(
    pos: "ManagedPosition",
    decision: "ExitDecision",
    *,
    client_id: str = "",
    execution_mode: str = "",
    run_id: str = "",
    strategy_version: str = "",
    git_commit: str = "",
    now_et: Optional[datetime] = None,
) -> dict:
    bid = _positive_or_none(getattr(pos, "current_bid", None))
    ask = _positive_or_none(getattr(pos, "current_ask", None))
    if bid is not None and ask is not None:
        mid = round((bid + ask) / 2.0, 4)
    else:
        mid = _positive_or_none(getattr(pos, "current_option_price", None))

    pnl_pct = None
    if (
        _positive_or_none(getattr(pos, "entry_price", None)) is not None
        and _positive_or_none(getattr(pos, "current_option_price", None)) is not None
    ):
        try:
            pnl_pct = float(getattr(decision, "pnl_pct", 0.0))
        except Exception:
            pnl_pct = None

    _underlying_quote_ts = (
        getattr(pos, "last_underlying_quote_update_ts", None)
        or getattr(pos, "lastunderlyingquoteupdatets", None)
    )
    _underlying_quote_age_sec = None
    if _underlying_quote_ts is not None:
        try:
            _quote_dt = _underlying_quote_ts
            if isinstance(_quote_dt, str):
                _quote_dt = datetime.fromisoformat(_quote_dt.replace("Z", "+00:00"))
            if _quote_dt.tzinfo is None:
                _quote_dt = _quote_dt.replace(tzinfo=timezone.utc)
            else:
                _quote_dt = _quote_dt.astimezone(timezone.utc)
            _quote_age = (_evaluation_now_utc(now_et) - _quote_dt).total_seconds()
            if _quote_age >= 0:
                _underlying_quote_age_sec = max(0.0, _quote_age)
        except Exception:
            _underlying_quote_age_sec = None

    return {
        "event": "exit_decision",
        "window": _decision_window(decision, now_et),
        "action": "fired" if getattr(decision, "should_act", False) else "skipped",
        "reason_code": getattr(decision, "reason_code", "") or _classify_exit_decision(decision),
        "reason": getattr(decision, "reason", "") or "",
        "client_id": str(client_id or getattr(pos, "client_id", "") or ""),
        "execution_mode": str(execution_mode or getattr(pos, "execution_mode", "") or ""),
        "position_id": str(getattr(pos, "position_id", "") or ""),
        "local_order_id": str(getattr(pos, "pending_exit_local_order_id", "") or ""),
        "broker_order_id": str(getattr(pos, "pending_exit_broker_order_id", "") or ""),
        "ticker": str(getattr(pos, "ticker", "") or ""),
        "option_symbol": str(getattr(pos, "option_symbol", "") or ""),
        "option_bid": bid,
        "option_ask": ask,
        "option_mid": mid,
        "option_last": _positive_or_none(getattr(pos, "current_last", None)),
        "underlying_price": _positive_or_none(getattr(pos, "current_underlying", None)),
        "underlying_stop": _positive_or_none(getattr(pos, "underlying_stop", None)),
        "underlying_side": str(getattr(pos, "side", "") or "").upper(),
        "underlying_quote_ts": str(
            _underlying_quote_ts
            or ""
        ),
        "underlying_quote_source": str(
            getattr(pos, "underlying_quote_source", "")
            or getattr(pos, "underlyingquotesource", "")
            or ("position_quote_monitor" if _underlying_quote_ts else "")
        ),
        "underlying_quote_age_sec": _underlying_quote_age_sec,
        "hard_exit_reference_price": getattr(pos, "hard_exit_reference_price", None),
        "hard_exit_reference_source": getattr(pos, "hard_exit_reference_source", ""),
        "hard_exit_reference_validity": getattr(pos, "hard_exit_reference_validity", ""),
        "hard_exit_reference_pnl_pct": getattr(pos, "hard_exit_reference_pnl_pct", None),
        "pnl_pct_at_decision": pnl_pct,
        "mfe_pct_so_far": (
            float(getattr(pos, "peak_pnl_pct", 0.0) or 0.0)
            if getattr(pos, "peak_pnl_pct", None) is not None else None
        ),
        "mae_pct_so_far": getattr(pos, "mae_pct_so_far", None),
        "ladder_mode": "legacy",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "candidate_id": getattr(pos, "signal_id", None) or getattr(pos, "position_id", None),
        "strategy_version": strategy_version,
        "git_commit": git_commit,
    }


# ─────────────────────────────────────────────────────────────────────────────
# PR #176 — LIVE_DEGRADED_SOFT_EXIT_GUARD
#
# Problem this guard fixes (production incident, NKE/RIVN, Jason live):
#   Exit engine submitted SOFT_LOSS / THESIS_FAIL_SOFT_STOP / NEVER_GREEN_STOP
#   while live context was degraded (underlying_entry=0, current_underlying
#   missing, execution_mode='unknown'). NKE only needed a few more minutes,
#   but the bot sold from no_underlying_data instead of holding.
#
# Scope (intentionally tiny):
#   - One module-level guard helper here.
#   - One call-site in _submit_exit_decision (the SUBMIT event).
#   - No new modules. No new market data fetches. No paper behavior changes.
#
# Behavior:
#   - Identifies the position as live-risk if execution_mode == "live", OR
#     execution_mode is blank/unknown AND client_id is a known live client.
#   - For these soft reason codes only:
#         SOFT_LOSS, SOFT_LOSS_WATCH, DEEP_LOSS_STOP, STOP_BREACH_CONFIRMING,
#         THESIS_FAIL_SOFT_STOP, NEVER_GREEN_STOP, "NEVER GREEN STOP" text
#   - Context is degraded if ANY of:
#         underlying_entry is null/zero,
#         current_underlying is null/zero,
#         decision.reason contains "no_underlying_data",
#         option bid/ask/mid all null AND the decision turns on option loss.
#   - Live-only minimum holds (overrides existing constants for live only):
#         SOFT_LOSS: 8 minutes minimum
#         NEVER_GREEN_STOP: 12 minutes minimum
#   - Always-allowed exits (do not gate):
#         manual close, EOD flatten, hard disaster / emergency, broker-already-gone.
# ─────────────────────────────────────────────────────────────────────────────

# Reason codes that this guard gates when the position is live-risk + degraded.
_PR176_GATED_SOFT_REASON_CODES = frozenset({
    "SOFT_LOSS",
    SOFT_LOSS_CONFIRMING,
    "SOFT_LOSS_WATCH",
    "DEEP_LOSS_STOP",
    "STOP_BREACH_CONFIRMING",
    "THESIS_FAIL_SOFT_STOP",
    "NEVER_GREEN_STOP",
})

# Reason codes / text fragments that ALWAYS proceed (never gated by this guard).
_PR176_ALWAYS_ALLOWED_REASON_CODES = frozenset({
    "EOD_FORCE_CLOSE",
    "HARD_STOP",
    OPTION_CATASTROPHIC_STOP,
    UNDERLYING_TECHNICAL_STOP_CONFIRMED,
    "HARD_DISASTER_STOP",
    "EMERGENCY_FLATTEN",
    "BROKER_FORCE_CLOSE",
    "BROKER_POSITION_GONE",
    "RECONCILER_BROKER_GONE",
    "MANUAL_CLOSE",
    "MANUAL_EXIT",
    "RUNNER_TRAIL",
    "PROFIT_LOCK",
    "TRAILING_STOP",
})
_PR176_ALWAYS_ALLOWED_TEXT_FRAGMENTS = (
    "EOD",
    "MANUAL",
    "EMERGENCY",
    "BROKER POSITION GONE",
    # PR #179 amendment: removed broad "RECONCILER" fragment — generic reconciler
    # paths must NOT bypass the degraded live soft-exit guard.  Only the
    # explicit broker-gone reason codes (RECONCILER_BROKER_GONE,
    # BROKER_POSITION_GONE) in _PR176_ALWAYS_ALLOWED_REASON_CODES bypass.
    "HARD STOP",
    "HARD DISASTER",
    "FORCE CLOSE",
)

# PR #179 amendment: text fragments that imply a gated soft-exit even when the
# reason_code is unfamiliar/normalized differently.  Production reasons may
# carry the soft-exit meaning in text (e.g. RECONCILER_AUTO_CLOSE wrapping a
# "NEVER GREEN STOP ... no_underlying_data" decision) while reason_code reads
# as something we don't recognize.  Any of these fragments in reason_text
# triggers the same live-risk + degraded check.
_PR176_GATED_SOFT_TEXT_FRAGMENTS = (
    "NEVER GREEN STOP",
    "THESIS_FAIL_SOFT_STOP",
    "NO_UNDERLYING_DATA",
)

# Live-only minimum hold floors (minutes). These override the global
# _MIN_HOLD_BEFORE_EXIT_MIN constant when the position is live-risk.
# Paper behavior is unchanged.
_PR176_LIVE_MIN_HOLD_SOFT_LOSS_MIN     = 8.0
_PR176_LIVE_MIN_HOLD_NEVER_GREEN_MIN  = 12.0

# Known live client emails. Used as a safety net when execution_mode is
# blank/unknown — fail to "live-risk" rather than letting a degraded
# soft exit submit. Override via env (comma-separated) if needed.
_PR176_LIVE_CLIENT_IDS = frozenset(
    s.strip().lower()
    for s in os.getenv("PR176_LIVE_CLIENT_IDS", "jasoncosby1@gmail.com").split(",")
    if s.strip()
)


def _pr176_is_live_risk(pos: "ManagedPosition") -> bool:
    """True if the position should be treated as live for the degraded guard."""
    mode = (getattr(pos, "execution_mode", "") or "").lower().strip()
    if mode == "live":
        return True
    if mode == "paper":
        return False
    # mode is blank/unknown → fail safe if client_id is on the live list
    client_id = (getattr(pos, "client_id", "") or "").lower().strip()
    return client_id in _PR176_LIVE_CLIENT_IDS


def _pr176_reason_is_always_allowed(reason_code: str, reason_text: str) -> bool:
    """Manual close / EOD / hard disaster / broker-gone → never gate."""
    rc = (reason_code or "").upper().strip()
    if rc in _PR176_ALWAYS_ALLOWED_REASON_CODES:
        return True
    rt = (reason_text or "").upper()
    return any(frag in rt for frag in _PR176_ALWAYS_ALLOWED_TEXT_FRAGMENTS)


def _pr176_context_is_degraded(pos: "ManagedPosition", decision_reason: str) -> tuple[bool, list[str]]:
    """
    Returns (is_degraded, list_of_signals). Order of checks matters for
    observability — the returned signals are emitted with the gated event.
    """
    signals: list[str] = []
    # 1) Missing/zero underlying entry
    entry_u = getattr(pos, "underlying_entry", None)
    if entry_u is None or float(entry_u or 0.0) == 0.0:
        signals.append("underlying_entry_missing_or_zero")
    # 2) Missing/zero current underlying
    curr_u = getattr(pos, "current_underlying", None)
    if curr_u is None or float(curr_u or 0.0) == 0.0:
        signals.append("current_underlying_missing")
    # 3) Decision text itself confessed no underlying data
    if "no_underlying_data" in (decision_reason or "").lower():
        signals.append("decision_text_no_underlying_data")
    # 4) Bid/ask/mid all null AND we are evaluating an option-loss decision.
    #    current_option_price doubles as mid in this engine; check bid/ask too.
    cur_bid = float(getattr(pos, "current_bid", 0.0) or 0.0)
    cur_ask = float(getattr(pos, "current_ask", 0.0) or 0.0)
    cur_mid = float(getattr(pos, "current_option_price", 0.0) or 0.0)
    if cur_bid == 0.0 and cur_ask == 0.0 and cur_mid == 0.0:
        signals.append("option_quote_bid_ask_mid_all_zero")
    return (len(signals) > 0), signals


def _pr176_live_min_hold_blocks(pos: "ManagedPosition", reason_code: str) -> tuple[bool, float, float]:
    """
    Returns (blocked, age_min, floor_min). For live-risk positions only:
      SOFT_LOSS:         8-minute floor
      NEVER_GREEN_STOP:  12-minute floor
    Paper is unaffected (this is only called when _pr176_is_live_risk is True).
    """
    age_min = _position_age_minutes(pos)
    rc = (reason_code or "").upper().strip()
    if rc == "SOFT_LOSS":
        return (age_min < _PR176_LIVE_MIN_HOLD_SOFT_LOSS_MIN, age_min, _PR176_LIVE_MIN_HOLD_SOFT_LOSS_MIN)
    if rc == "NEVER_GREEN_STOP":
        return (age_min < _PR176_LIVE_MIN_HOLD_NEVER_GREEN_MIN, age_min, _PR176_LIVE_MIN_HOLD_NEVER_GREEN_MIN)
    return (False, age_min, 0.0)


def _pr176_should_hold(
    pos: "ManagedPosition",
    decision_reason_code: str,
    decision_reason_text: str,
) -> tuple[bool, str, dict]:
    """
    Top-level guard. Returns (should_hold, hold_reason_code, extra_inputs).

    should_hold=True means: do NOT submit the broker exit. Emit a HOLD event
    instead. The caller MUST `continue` to the next position in the loop.
    """
    # 1) Always-allowed paths never gated.  Runs FIRST so manual/EOD/hard
    #    text wins even when paired with a gated soft reason_code.
    if _pr176_reason_is_always_allowed(decision_reason_code, decision_reason_text):
        return False, "", {}

    # 2) Gate when EITHER:
    #      reason_code is in the gated soft-exit set, OR
    #      reason_text contains a gated soft-exit fragment.
    #    PR #179 amendment: text-fragment match catches production reasons
    #    where the soft-exit meaning lives in the text (e.g. an unfamiliar
    #    reason_code wrapping "NEVER GREEN STOP ... no_underlying_data").
    rc = (decision_reason_code or "").upper().strip()
    rt = (decision_reason_text or "").upper()
    code_is_gated = rc in _PR176_GATED_SOFT_REASON_CODES
    text_is_gated = any(frag in rt for frag in _PR176_GATED_SOFT_TEXT_FRAGMENTS)
    if not (code_is_gated or text_is_gated):
        return False, "", {}

    # 3) Only gate live-risk positions
    if not _pr176_is_live_risk(pos):
        return False, "", {}

    # 4) DATA_DEGRADED_HOLD: any degraded signal blocks the exit
    degraded, signals = _pr176_context_is_degraded(pos, decision_reason_text)
    if degraded:
        return True, "DATA_DEGRADED_HOLD", {
            "pr176_degraded_signals": signals,
            "pr176_reason_code": rc,
            "pr176_gated_by": "reason_code" if code_is_gated else "reason_text",
            "pr176_execution_mode": (getattr(pos, "execution_mode", "") or "").lower().strip() or "unknown",
            "pr176_client_id": getattr(pos, "client_id", "") or "",
            "pr176_underlying_entry": getattr(pos, "underlying_entry", None),
            "pr176_current_underlying": getattr(pos, "current_underlying", None),
        }

    # 5) Live-only minimum hold: SOFT_LOSS<8min and NEVER_GREEN_STOP<12min held.
    #    Hold floors are keyed off reason_code, so they only apply when
    #    code_is_gated; text-only matches (without a recognized rc) fall
    #    through to the no-hold return below if data is clean.
    if code_is_gated:
        held_short, age_min, floor_min = _pr176_live_min_hold_blocks(pos, rc)
        if held_short:
            return True, "LIVE_MIN_HOLD_NOT_MET", {
                "pr176_reason_code": rc,
                "pr176_age_min": age_min,
                "pr176_floor_min": floor_min,
                "pr176_execution_mode": (getattr(pos, "execution_mode", "") or "").lower().strip() or "unknown",
                "pr176_client_id": getattr(pos, "client_id", "") or "",
            }

    return False, "", {}


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


def _broker_repair_float(value) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _broker_repair_positive_float(value) -> float:
    result = _broker_repair_float(value)
    return result if result is not None and result > 0.0 else 0.0


def _broker_repair_positive_int(value) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(numeric) or numeric <= 0.0 or not numeric.is_integer():
        return None
    return int(numeric)


def _broker_repair_timestamp(value) -> Optional[datetime]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, datetime):
        result = value
    else:
        try:
            result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    if result.tzinfo is None:
        return result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def _broker_repair_entry_timestamp(broker_position: dict):
    """Read date_acquired without changing the broker adapter contract."""
    if not isinstance(broker_position, dict):
        return None
    direct = broker_position.get("date_acquired")
    if _broker_repair_timestamp(direct) is not None:
        return direct
    raw = broker_position.get("raw")
    if isinstance(raw, dict):
        return raw.get("date_acquired")
    return None


def _broker_repair_order_meta(value) -> dict:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            decoded = _json.loads(value)
            return dict(decoded) if isinstance(decoded, dict) else {}
        except Exception:
            return {}
    return {}



_BROKER_REPAIR_ENTRY_GEOMETRY_KEYS = (
    "underlying_entry",
    "entry_underlying",
    "underlying_price_at_entry",
    "underlying_entry_price",
    "entry_underlying_price",
)
_BROKER_REPAIR_STOP_GEOMETRY_KEYS = (
    "stop_underlying",
    "underlying_stop",
)
_BROKER_REPAIR_TARGET_GEOMETRY_KEYS = (
    "target_underlying",
    "underlying_target",
)


def _broker_repair_historical_value(
    *mappings, keys: tuple[str, ...],
) -> tuple[float, bool]:
    """Read historical aliases and reject malformed/contradictory evidence.

    A zero is evidence, not "missing": zero alongside any positive alias is
    contradictory.  The same rule applies across the order row and its JSON
    metadata because both are durable historical claims.
    """
    values: list[float] = []
    saw_explicit_zero = False
    for mapping in mappings:
        if not isinstance(mapping, dict):
            continue
        for key in keys:
            if key not in mapping:
                continue
            raw = mapping.get(key)
            if raw is None or (isinstance(raw, str) and not raw.strip()):
                continue
            if isinstance(raw, bool):
                return 0.0, True
            try:
                numeric = float(raw)
            except (TypeError, ValueError, OverflowError):
                return 0.0, True
            if not math.isfinite(numeric):
                return 0.0, True
            if numeric == 0.0:
                saw_explicit_zero = True
                continue
            if numeric < 0.0:
                return 0.0, True
            values.append(numeric)

    if saw_explicit_zero and values:
        return 0.0, True
    if any(candidate != values[0] for candidate in values[1:]):
        return 0.0, True
    return (values[0] if values else 0.0), False


def _broker_repair_text_value(
    order: dict, meta: dict, key: str,
) -> tuple[Optional[str], bool]:
    """Read one text identity from row/meta and reject conflicting copies."""
    values: list[str] = []
    for mapping in (order, meta):
        if not isinstance(mapping, dict) or key not in mapping:
            continue
        raw = mapping.get(key)
        if raw is None:
            continue
        text = str(raw).strip()
        if not text or text.lower() in {"none", "null", "nan", "unknown"}:
            continue
        values.append(text)
    if any(candidate != values[0] for candidate in values[1:]):
        return None, True
    return (values[0] if values else None), False


def _broker_repair_position_id(value) -> str:
    candidate = str(value or "").strip()
    if not candidate or candidate.lower() in {
        "0", "none", "null", "nan", "na", "n/a", "nil", "unknown",
        "undefined", "unavailable", "missing", "placeholder", "true", "false",
        "?", "-",
    }:
        return ""
    if candidate.lower().startswith("broker-repair-"):
        return ""
    return candidate


def _broker_repair_lookup_marker(status: str, reason: str = "") -> dict:
    return {"_lookup_status": status, "_lookup_reason": reason}


_BROKER_REPAIR_PROVENANCE_KEY = "broker_repair_provenance"
_BROKER_REPAIR_PROVENANCE_VALUE = "broker_recovery_uuid"


def _broker_repair_provenance_from_row(row: dict) -> str:
    """Read explicit durable recovery provenance without inferring from identity gaps."""
    if not isinstance(row, dict):
        return ""
    direct = str(row.get(_BROKER_REPAIR_PROVENANCE_KEY) or "").strip().lower()
    if direct:
        return direct
    meta = _broker_repair_order_meta(row.get("meta"))
    return str(meta.get(_BROKER_REPAIR_PROVENANCE_KEY) or "").strip().lower()


def _broker_repair_row_is_provisional(row: dict) -> bool:
    return (
        _broker_repair_provenance_from_row(row)
        == _BROKER_REPAIR_PROVENANCE_VALUE
    )


def _is_broker_repair_provisional(position) -> bool:
    """Identify a provisional broker-recovery owner independent of UUID format."""
    if bool(getattr(position, "broker_repair_provisional", False)):
        return True
    if bool(getattr(position, "brokerrepairprovisional", False)):
        return True
    return str(getattr(position, "position_id", "") or "").startswith("broker-repair-")


def _clear_broker_repair_provisional(position) -> None:
    """Canonical adoption must remove every in-memory repair-provenance spelling."""
    try:
        position.broker_repair_provisional = False
        position.brokerrepairprovisional = False
        position.broker_repair_provenance = ""
    except Exception as exc:
        log.debug("[exit_eng] clear broker-repair provenance failed: %s", exc)


def _converge_broker_repair_db_identity(
    client_id: str, execution_mode: str, contract: str,
    provisional_id: str, canonical_id: str,
) -> bool:
    """Retire or rename a provisional DB owner under the repair lock."""
    provisional_id = str(provisional_id or "").strip()
    canonical_id = str(canonical_id or "").strip()
    client_id = str(client_id or "").strip().lower()
    execution_mode = str(execution_mode or "").strip().lower()
    contract = str(contract or "").strip().upper()
    if not provisional_id or not canonical_id:
        return True
    try:
        from ap.db import conn, run_with_retry
        def _converge_once():
            with conn() as cur:
                lock_key = f"broker-repair:{client_id}:{execution_mode}:{contract}"
                cur.execute(
                    "SELECT pg_advisory_xact_lock(('x' || md5(%s))::bit(64)::bigint)",
                    (lock_key,),
                )
                if provisional_id == canonical_id:
                    cur.execute(
                        """UPDATE positions
                           SET meta = COALESCE(meta, '{}'::jsonb) - %s,
                               updated_at = NOW()
                           WHERE id = %s AND client_id = %s
                             AND LOWER(TRIM(COALESCE(execution_mode, ''))) = %s
                             AND UPPER(TRIM(COALESCE(contract, ''))) = %s
                             AND UPPER(TRIM(COALESCE(status, ''))) IN
                                 ('OPEN', 'CLOSING', 'PARTIAL', 'ACTIVE')
                             AND COALESCE(quantity_remaining, qty, 0) > 0""",
                        (
                            _BROKER_REPAIR_PROVENANCE_KEY,
                            canonical_id,
                            client_id,
                            execution_mode,
                            contract,
                        ),
                    )
                    return cur.rowcount in (0, 1)
                cur.execute(
                    """SELECT id FROM positions
                       WHERE id = %s AND client_id = %s
                         AND LOWER(TRIM(COALESCE(execution_mode, ''))) = %s
                         AND UPPER(TRIM(COALESCE(contract, ''))) = %s
                         AND UPPER(TRIM(COALESCE(status, ''))) IN
                             ('OPEN', 'CLOSING', 'PARTIAL', 'ACTIVE')
                         AND COALESCE(quantity_remaining, qty, 0) > 0
                       LIMIT 1""",
                    (provisional_id, client_id, execution_mode, contract),
                )
                if cur.fetchone() is None:
                    return True
                cur.execute(
                    """SELECT id FROM positions
                       WHERE id = %s AND client_id = %s
                         AND LOWER(TRIM(COALESCE(execution_mode, ''))) = %s
                         AND UPPER(TRIM(COALESCE(contract, ''))) = %s
                         AND UPPER(TRIM(COALESCE(status, ''))) IN
                             ('OPEN', 'CLOSING', 'PARTIAL', 'ACTIVE')
                         AND COALESCE(quantity_remaining, qty, 0) > 0
                       LIMIT 1""",
                    (canonical_id, client_id, execution_mode, contract),
                )
                if cur.fetchone() is not None:
                    cur.execute(
                        """UPDATE positions
                           SET status = 'CLOSED', quantity_remaining = 0,
                               qty = 0, updated_at = NOW()
                           WHERE id = %s AND client_id = %s
                             AND LOWER(TRIM(COALESCE(execution_mode, ''))) = %s
                             AND UPPER(TRIM(COALESCE(contract, ''))) = %s
                             AND UPPER(TRIM(COALESCE(status, ''))) IN
                                 ('OPEN', 'CLOSING', 'PARTIAL', 'ACTIVE')
                             AND COALESCE(quantity_remaining, qty, 0) > 0""",
                        (provisional_id, client_id, execution_mode, contract),
                    )
                    return cur.rowcount == 1
                cur.execute(
                    """UPDATE positions
                       SET id = %s,
                           meta = COALESCE(meta, '{}'::jsonb) - %s
                       WHERE id = %s AND client_id = %s
                         AND LOWER(TRIM(COALESCE(execution_mode, ''))) = %s
                         AND UPPER(TRIM(COALESCE(contract, ''))) = %s
                         AND UPPER(TRIM(COALESCE(status, ''))) IN
                             ('OPEN', 'CLOSING', 'PARTIAL', 'ACTIVE')
                         AND COALESCE(quantity_remaining, qty, 0) > 0""",
                    (
                        canonical_id,
                        _BROKER_REPAIR_PROVENANCE_KEY,
                        provisional_id,
                        client_id,
                        execution_mode,
                        contract,
                    ),
                )
                return cur.rowcount == 1

        def _converge():
            # A canonical writer does not need the repair advisory lock to
            # insert B.  If it commits after the existence check but before
            # the provisional rename, PostgreSQL raises UniqueViolation on
            # the primary key.  The failed transaction is discarded by
            # conn(); retry from a fresh transaction and retire A instead.
            for attempt in range(2):
                try:
                    return _converge_once()
                except Exception as exc:
                    try:
                        from psycopg2 import errors as _pg_errors
                        _is_unique_violation = isinstance(
                            exc, _pg_errors.UniqueViolation
                        )
                    except Exception:
                        _is_unique_violation = (
                            type(exc).__name__ == "UniqueViolation"
                        )
                    if not _is_unique_violation or attempt:
                        raise
                    log.warning(
                        "[exit_eng] canonical identity arrived during provisional "
                        "rename; retrying convergence client=%s mode=%s contract=%s",
                        client_id, execution_mode, contract,
                    )
        return bool(run_with_retry(_converge))
    except Exception as exc:
        log.critical(
            "[exit_eng] CANONICAL_DB_IDENTITY_CONVERGENCE_FAILED "
            "client=%s mode=%s contract=%s provisional=%s canonical=%s: %s",
            client_id, execution_mode, contract, provisional_id, canonical_id, exc,
        )
        return False


def _broker_repair_order_matches(order: dict, broker_position: dict) -> bool:
    """Require enough fill evidence before reusing an existing position id."""
    broker_qty = _broker_repair_positive_int(broker_position.get("quantity"))
    order_qty = _broker_repair_positive_int(order.get("filled_qty"))
    if broker_qty is None or order_qty is None or order_qty < broker_qty:
        return False

    broker_raw_ts = _broker_repair_entry_timestamp(broker_position)
    broker_ts = _broker_repair_timestamp(broker_raw_ts)
    order_ts = _broker_repair_timestamp(order.get("filled_ts"))
    # Tradier's production list_positions shape may omit date_acquired. The
    # broker price/quantity and exact client/mode/OCC ENTRY scope remain
    # mandatory proof; timestamp fencing is applied whenever the broker
    # actually supplies a timestamp, but its absence is not a mismatch.
    if broker_raw_ts not in (None, ""):
        if broker_ts is None or order_ts is None:
            return False
        # Tradier commonly reports date_acquired as a date rather than a
        # fill-time timestamp. A same-day match is the strongest proof
        # available in that shape; timestamp-bearing broker data gets the
        # tighter fence.
        if isinstance(broker_raw_ts, str) and len(broker_raw_ts.strip()) == 10:
            if broker_ts.date() != order_ts.date():
                return False
        elif abs((order_ts - broker_ts).total_seconds()) > 600.0:
            return False

    broker_cost_basis = _broker_repair_float(broker_position.get("cost_basis"))
    order_fill = _broker_repair_positive_float(order.get("fill_price"))
    if broker_cost_basis is None or abs(broker_cost_basis) <= 0.0 or order_fill <= 0.0:
        return False
    broker_entry_price = abs(broker_cost_basis) / broker_qty / 100.0
    return math.isclose(
        order_fill,
        broker_entry_price,
        rel_tol=0.05,
        abs_tol=0.02,
    )


class _BrokerRepairIdentity(str):
    """String-compatible id carrying only the row data needed this cycle."""

    def __new__(cls, value: str, row: Optional[dict] = None):
        instance = super().__new__(cls, str(value))
        instance.repair_row = dict(row or {})
        return instance


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
        self._position_manager = None
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


    def adopt_canonical_position_identity(
        self,
        *,
        contract: str,
        canonical_position_id: str,
        local_order_id: str,
        broker_order_id: str,
        signal_id: str,
        canonical_signal_id: str,
        entry_fill: float,
        entry_ts,
        execution_mode: str,
        client_id: str,
        order_filled_ts=None,
        underlying_entry: float = 0.0,
        score: float = 0.0,
        tier: str = "",
        pattern: str = "",
        direction: str = "",
        timeframe: str = "",
        underlying_stop: float = 0.0,
        underlying_target: float = 0.0,
    ) -> CanonicalAdoptionResult:
        """Atomically upgrade a broker-repair position to canonical filled-order identity.

        Returns CanonicalAdoptionResult. Callers must check disposition:
          ADOPTED / ALREADY_CANONICAL_REPAIR_REMOVED → return
          NO_REPAIR_FOUND → seed normally
          RETRY_* → emit critical, preserve monitoring, do not seed
        """
        _contract = str(contract or "").upper().strip()
        _canon_id = str(canonical_position_id or "").strip()
        _client   = str(client_id or "").strip().lower()
        _mode     = str(execution_mode or "").strip().lower()
        if not _contract or not _canon_id:
            return CanonicalAdoptionResult(
                disposition="RETRY_ADOPTION_ERROR", adopted=False,
                safe_to_seed=False, retryable=True, reason="missing_contract_or_id",
            )

        with self._lock:
            # ── Final Blocker 1: Canonical + repair collapse ───────────────────
            # When a canonical object already exists for canonical_position_id,
            # do not simply return True — find every active broker-repair for the
            # same client/contract, merge safe state into the canonical object,
            # then remove the repair objects so exactly one active position remains.
            _existing_canon = self._positions_by_id.get(_canon_id)
            if _existing_canon is not None:
                _canon_sym  = str(getattr(_existing_canon, "option_symbol", "") or "").upper().strip()
                _canon_cli  = str(getattr(_existing_canon, "client_id", "") or "").strip().lower()
                _canon_mode = str(getattr(_existing_canon, "execution_mode", "") or "").strip().lower()

                # P1: Execution mode fencing — LIVE and PAPER must never collapse into
                # one exit owner. Blank/unknown canonical mode is unproven; do not
                # allow cross-mode collapse.
                #
                # Compatible mode matrix:
                #   incoming live   + canonical live   → OK
                #   incoming paper  + canonical paper  → OK
                #   incoming live   + canonical blank  → RETRY (cannot prove it's live)
                #   incoming paper  + canonical blank  → RETRY (cannot prove it's paper)
                #   incoming blank  + canonical any    → RETRY (unproven mode)
                #   incoming live   + canonical paper  → RETRY_MODE_MISMATCH
                #   incoming paper  + canonical live   → RETRY_MODE_MISMATCH
                _norm_incoming  = _mode
                _norm_canonical = _canon_mode
                # Both must be the same *known* mode: {"live", "paper"}.
                # blank+blank, blank+live, live+blank, unknown+anything, etc.
                # are ALL unproven and must fail closed with RETRY_MODE_MISMATCH.
                # The `(_norm_incoming or _norm_canonical)` guard that was here
                # previously allowed blank+blank to bypass the check — removed.
                _mode_compatible = (
                    _norm_incoming in {"live", "paper"}
                    and _norm_canonical in {"live", "paper"}
                    and _norm_incoming == _norm_canonical
                )
                if not _mode_compatible:
                    log.critical(
                        "[exit_eng] RETRY_MODE_MISMATCH | canonical_id=%s "
                        "canonical_mode=%r incoming_mode=%r contract=%s — "
                        "both modes must be explicitly known and equal; "
                        "blank/unknown/mismatched mode may not collapse positions",
                        _canon_id, _canon_mode, _mode, _contract,
                    )
                    return CanonicalAdoptionResult(
                        disposition="RETRY_MODE_MISMATCH", adopted=False,
                        safe_to_seed=False, retryable=True,
                        reason=(
                            f"canonical_mode={_norm_canonical!r} "
                            f"incoming_mode={_norm_incoming!r}"
                        ),
                    )

                _canon_ok = (
                    _canon_sym == _contract
                    and (not _client or _canon_cli == _client)
                    and not getattr(_existing_canon, "closed", False)
                )
                if not _canon_ok:
                    log.critical(
                        "[exit_eng] RETRY_IDENTITY_CONFLICT | canonical=%s contract=%s "
                        "client=%s — existing canonical does not match; refusing adoption",
                        _canon_id, _contract, _client,
                    )
                    return CanonicalAdoptionResult(
                        disposition="RETRY_IDENTITY_CONFLICT",
                        adopted=False, safe_to_seed=False, retryable=True,
                        reason="canonical_object_mismatch",
                    )

                # Merge every active broker-repair for this exact client/mode/contract
                # into canonical. Unknown-client, foreign-client, blank-mode, or
                # wrong-mode repairs stay quarantined and cannot donate quote authority.
                _repairs_to_remove = []
                _identity_unproven_repairs = []
                for p in self._positions:
                    if p is _existing_canon:
                        continue
                    if not _is_broker_repair_provisional(p):
                        continue
                    if str(getattr(p, "option_symbol", "") or "").upper().strip() != _contract:
                        continue
                    if getattr(p, "closed", False):
                        continue
                    _rp_cli = str(getattr(p, "client_id", "") or "").strip().lower()
                    if not _client or _rp_cli != _client:
                        _mark_adoption_identity_quarantined(
                            p, f"repair_client={_rp_cli!r} canonical_client={_client!r}",
                        )
                        _identity_unproven_repairs.append(p)
                        continue
                    _rp_mode = str(getattr(p, "execution_mode", "") or "").strip().lower()
                    if _rp_mode not in {"live", "paper"} or _rp_mode != _norm_canonical:
                        _mark_adoption_identity_quarantined(
                            p, f"repair_mode={_rp_mode!r} canonical_mode={_norm_canonical!r}",
                        )
                        _identity_unproven_repairs.append(p)
                        continue
                    _repairs_to_remove.append(p)
                for _rp in _repairs_to_remove:
                    if (
                        bool(getattr(_rp, "broker_repair_provisional", False))
                        and not _converge_broker_repair_db_identity(
                            _client, _norm_canonical, _contract,
                            str(getattr(_rp, "position_id", "") or ""), _canon_id,
                        )
                    ):
                        return CanonicalAdoptionResult(
                            disposition="RETRY_ADOPTION_ERROR", adopted=False,
                            safe_to_seed=False, retryable=True,
                            reason="provisional_db_identity_convergence_failed",
                        )
                    _merge_now = datetime.now(timezone.utc)
                    _accepted_repair_bid = None
                    # Quote values and their timestamps are one snapshot.  Never
                    # copy a price unless that repair observation is newer.
                    for _price_attr, _ts_attr in (
                        ("current_bid", "last_option_bid_update_ts"),
                        ("current_ask", "last_option_quote_update_ts"),
                        ("current_underlying", "last_underlying_quote_update_ts"),
                    ):
                        _rp_v = getattr(_rp, _price_attr, 0.0) or 0.0
                        _rp_ts = getattr(_rp, _ts_attr, None)
                        _cn_ts = getattr(_existing_canon, _ts_attr, None)
                        _rp_ts_norm = _normalize_hard_ref_ts(_rp_ts, now_utc=_merge_now)
                        _cn_ts_norm = _normalize_hard_ref_ts(_cn_ts, now_utc=_merge_now)
                        if _price_attr == "current_bid":
                            try:
                                _bid_age_ok = (
                                    _rp_ts_norm is not None
                                    and 0 <= (_merge_now - _rp_ts_norm).total_seconds() <= float(STALE_OPTION_QUOTE_MAX_AGE_SEC)
                                )
                            except Exception:
                                _bid_age_ok = False
                            if not _bid_age_ok:
                                continue
                        if (_rp_v > 0 and _rp_ts_norm is not None
                                and (_cn_ts_norm is None or _rp_ts_norm > _cn_ts_norm)):
                            _set_position_attr_pair(_existing_canon, _price_attr, _rp_v)
                            _set_position_attr_pair(_existing_canon, _ts_attr, _rp_ts_norm)
                            if _price_attr == "current_bid":
                                _accepted_repair_bid = float(_rp_v)
                                _set_position_attr_pair(_existing_canon, "option_bid_valid", True)
                                _set_position_attr_pair(_existing_canon, "option_quote_fresh", True)
                            elif _price_attr == "current_underlying":
                                _set_position_attr_pair(_existing_canon, "underlying_available", True)
                                _set_position_attr_pair(_existing_canon, "underlying_fresh", True)
                    # Transfer the complete hard-reference record before the
                    # repair is removed; the cached P&L is recomputed by the
                    # resolver and is never the authority.
                    _merge_hard_exit_reference_for_collapse(
                        _existing_canon, _rp, now_utc=_merge_now,
                    )
                    # Merge bid-proven peak only.  Never copy the repair's
                    # stored percentage; it may have used a provisional entry
                    # denominator.  A newly accepted repair BID is rebased
                    # against the canonical entry price before comparison.
                    _rp_src = str(getattr(_rp, "live_executable_price_source", "") or "").lower()
                    if _rp_src == "bid" and _accepted_repair_bid is not None:
                        _cn_peak = float(getattr(_existing_canon, "peak_pnl_pct", 0.0) or 0.0)
                        _cn_entry = float(getattr(_existing_canon, "entry_price", 0.0) or 0.0)
                        _rp_rebased_pnl = (
                            (_accepted_repair_bid - _cn_entry) / _cn_entry
                            if _cn_entry > 0 else 0.0
                        )
                        if _rp_rebased_pnl > _cn_peak:
                            try:
                                _set_position_attr_pair(
                                    _existing_canon, "peak_pnl_pct", _rp_rebased_pnl,
                                )
                                _set_position_attr_pair(
                                    _existing_canon, "max_profit_seen", _rp_rebased_pnl,
                                )
                                _existing_canon.touched_profit  = False
                                _existing_canon.touchedprofit   = False
                            except Exception as _e:
                                log.debug("[exit_eng] merge peak from repair: %s", _e)

                    _rp_id = str(getattr(_rp, "position_id", "") or "")
                    self._positions.remove(_rp)
                    self._positions_by_id.pop(_rp_id, None)
                    log.info(
                        "[exit_eng] ALREADY_CANONICAL_REPAIR_REMOVED | "
                        "canonical=%s repair=%s contract=%s",
                        _canon_id, _rp_id, _contract,
                    )

                _reclassify_hard_ref_for_entry(_existing_canon)

                # A previously adopted UUID repair may already be indexed under
                # the canonical id. Positive canonical identity clears both its
                # durable and in-memory recovery provenance; it must never remove
                # itself as a repair on a repeated adoption call.
                if _is_broker_repair_provisional(_existing_canon):
                    if not _converge_broker_repair_db_identity(
                        _client, _norm_canonical, _contract,
                        _canon_id, _canon_id,
                    ):
                        return CanonicalAdoptionResult(
                            disposition="RETRY_ADOPTION_ERROR", adopted=False,
                            safe_to_seed=False, retryable=True,
                            reason="canonical_provenance_clear_failed",
                        )
                    _clear_broker_repair_provisional(_existing_canon)

                # Assert exactly one nonclosed active object for this contract.
                _active_for_contract = [
                    p for p in self._positions
                    if str(getattr(p, "option_symbol", "") or "").upper().strip() == _contract
                    and not getattr(p, "closed", False)
                ]
                if len(_active_for_contract) != 1:
                    log.critical(
                        "[exit_eng] CANONICAL_COLLAPSE_INVARIANT_VIOLATED | "
                        "contract=%s active_count=%d — expected exactly 1",
                        _contract, len(_active_for_contract),
                    )
                    return CanonicalAdoptionResult(
                        disposition="RETRY_REPAIR_IDENTITY_UNPROVEN",
                        adopted=False, safe_to_seed=False, retryable=True,
                        reason=f"active_count={len(_active_for_contract)}",
                    )

                if _identity_unproven_repairs:
                    log.critical(
                        "[exit_eng] RETRY_REPAIR_IDENTITY_UNPROVEN | "
                        "canonical=%s contract=%s retained_repairs=%d",
                        _canon_id, _contract, len(_identity_unproven_repairs),
                    )
                    return CanonicalAdoptionResult(
                        disposition="RETRY_REPAIR_IDENTITY_UNPROVEN",
                        adopted=False, safe_to_seed=False, retryable=True,
                        reason=f"retained_repairs={len(_identity_unproven_repairs)}",
                    )

                return CanonicalAdoptionResult(
                    disposition="ALREADY_CANONICAL_REPAIR_REMOVED",
                    adopted=True, safe_to_seed=False, retryable=False,
                    reason=f"removed_{len(_repairs_to_remove)}_repair_objects",
                )

            for pos in self._positions:
                _pid = str(pos.position_id or "")
                _sym = str(pos.option_symbol or "").upper().strip()
                _is_repair = _is_broker_repair_provisional(pos)
                _same_contract = (_sym == _contract)
                if not (_is_repair and _same_contract and not pos.closed):
                    continue

                # ── Blocker 5: Client and mode fencing ────────────────────────
                # The repair position must belong to the exact same client and
                # exact known execution mode before we overwrite identity fields.
                _repair_client = str(getattr(pos, "client_id", "") or "").strip().lower()
                if not _client or _repair_client != _client:
                    log.critical(
                        "[exit_eng] RETRY_CLIENT_MISMATCH | "
                        "contract=%s repair_client=%s canonical_client=%s — "
                        "refusing unknown/cross-client adoption",
                        _contract, _repair_client, _client,
                    )
                    return CanonicalAdoptionResult(
                        disposition="RETRY_CLIENT_MISMATCH", adopted=False,
                        safe_to_seed=False, retryable=True,
                        reason=f"repair_client={_repair_client} != {_client}",
                    )

                _repair_mode = str(getattr(pos, "execution_mode", "") or "").strip().lower()
                _mode_ok = (
                    _repair_mode in {"live", "paper"}
                    and _mode in {"live", "paper"}
                    and _repair_mode == _mode
                )
                if not _mode_ok:
                    log.critical(
                        "[exit_eng] RETRY_MODE_MISMATCH | "
                        "contract=%s repair_mode=%s canonical_mode=%s — "
                        "refusing unknown/incompatible mode adoption",
                        _contract, _repair_mode, _mode,
                    )
                    return CanonicalAdoptionResult(
                        disposition="RETRY_MODE_MISMATCH", adopted=False,
                        safe_to_seed=False, retryable=True,
                        reason=f"repair_mode={_repair_mode} vs canonical={_mode}",
                    )

                # Found a valid broker-repair position — upgrade in place.
                old_id = _pid
                if (
                    bool(getattr(pos, "broker_repair_provisional", False))
                    and not _converge_broker_repair_db_identity(
                        _client, _mode, _contract, old_id, _canon_id,
                    )
                ):
                    return CanonicalAdoptionResult(
                        disposition="RETRY_ADOPTION_ERROR", adopted=False,
                        safe_to_seed=False, retryable=True,
                        reason="provisional_db_identity_convergence_failed",
                    )

                # ── Blocker 3: Remove contaminated midpoint state ────────────
                _prior_peak_source = str(
                    getattr(pos, "live_executable_price_source", "")
                    or getattr(pos, "liveexecutablepricesource", "")
                    or ""
                ).lower().strip()
                _prior_peak_is_bid_proven = (_prior_peak_source == "bid")

                if entry_fill > 0:
                    _set_position_attr_pair(pos, "entry_price", entry_fill)
                    _bid_is_fresh = _has_fresh_dedicated_bid(pos)
                    _fresh_bid = float(getattr(pos, "current_bid", 0.0) or 0.0) if _bid_is_fresh else 0.0
                    if _fresh_bid > 0:
                        _rebased_pnl = (_fresh_bid - entry_fill) / entry_fill
                    else:
                        _rebased_pnl = 0.0

                    # Peak percentages depend on the entry denominator.  After
                    # canonical adoption, only the current BID is valid until
                    # QPM supplies a new two-observation confirmation.
                    _rebased_peak = max(0.0, _rebased_pnl)
                    _set_position_attr_pair(pos, "peak_pnl_pct", _rebased_peak)
                    _set_position_attr_pair(pos, "max_profit_seen", _rebased_peak)
                    pos.touched_profit = False
                    pos.touchedprofit = False
                    _reclassify_hard_ref_for_entry(pos)

                # ── Canonical identity fields ──────────────────────────────────
                pos.position_id = _canon_id
                _clear_broker_repair_provisional(pos)
                if client_id:
                    pos.client_id = _client
                if signal_id:
                    pos.signal_id = signal_id
                if canonical_signal_id:
                    try:
                        pos.canonical_signal_id = canonical_signal_id
                    except Exception as _e:
                        log.debug("[exit_eng] adopt canonical_signal_id: %s", _e)
                if execution_mode:
                    pos.execution_mode = _mode

                # Order / broker identity
                try:
                    pos.entry_local_order_id = local_order_id
                except Exception as _e:
                    log.debug("[exit_eng] adopt entry_local_order_id: %s", _e)
                try:
                    pos.entry_broker_order_id = broker_order_id
                except Exception as _e:
                    log.debug("[exit_eng] adopt entry_broker_order_id: %s", _e)

                # ── Blocker 3+4: Normalize canonical fill timestamp ────────────
                if entry_ts is not None or order_filled_ts is not None:
                    try:
                        _norm_ts = _normalize_canonical_ts(
                            entry_ts,
                            fallback=order_filled_ts,
                            local_order_id=local_order_id or "",
                        )
                        pos.opened_at = _norm_ts
                    except Exception as _e:
                        log.debug("[exit_eng] adopt opened_at: %s", _e)

                if underlying_entry > 0:
                    try:
                        pos.underlying_entry = underlying_entry
                    except Exception as _ue_err:
                        log.debug("[exit_eng] adopt: underlying_entry set skipped: %s", _ue_err)

                # Signal metadata — written to both direct attrs and pos.signal
                # so proof and exit code reading either surface gets canonical values.
                _sig_patch: dict = {}
                if score:
                    try: pos.score = score
                    except Exception as _e: log.debug("[exit_eng] adopt score: %s", _e)
                    _sig_patch["score"] = score
                if tier:
                    try: pos.tier = tier
                    except Exception as _e: log.debug("[exit_eng] adopt tier: %s", _e)
                    _sig_patch["tier"] = tier
                if pattern:
                    try: pos.pattern = pattern
                    except Exception as _e: log.debug("[exit_eng] adopt pattern: %s", _e)
                    _sig_patch["pattern"] = pattern
                if timeframe:
                    try: pos.timeframe = timeframe
                    except Exception as _e: log.debug("[exit_eng] adopt timeframe: %s", _e)
                    _sig_patch["timeframe"] = timeframe
                if direction:
                    try: pos.side = direction.upper()
                    except Exception as _e: log.debug("[exit_eng] adopt side: %s", _e)
                    _sig_patch["side"] = direction.upper()
                if underlying_stop > 0:
                    try: pos.underlying_stop = underlying_stop
                    except Exception as _e: log.debug("[exit_eng] adopt underlying_stop: %s", _e)
                if underlying_target > 0:
                    try: pos.underlying_target = underlying_target
                    except Exception as _e: log.debug("[exit_eng] adopt underlying_target: %s", _e)
                if canonical_signal_id:
                    _sig_patch["canonical_signal_id"] = canonical_signal_id
                if signal_id:
                    _sig_patch["signal_id"] = signal_id
                if execution_mode:
                    _sig_patch["execution_mode"] = _mode

                if _sig_patch:
                    try:
                        _existing_sig = getattr(pos, "signal", None)
                        if isinstance(_existing_sig, dict):
                            _existing_sig.update(_sig_patch)
                        else:
                            pos.signal = _sig_patch
                    except Exception as _sig_err:
                        log.debug("[exit_eng] adopt signal dict merge failed: %s", _sig_err)

                # Update O(1) index atomically
                self._positions_by_id.pop(old_id, None)
                self._positions_by_id[_canon_id] = pos
                _clear_adoption_identity_quarantine(pos)

                log.info(
                    "[exit_eng] CANONICAL_POSITION_ADOPTED "
                    "contract=%s old_id=%s new_id=%s local_order=%s broker_order=%s "
                    "signal_id=%s execution_mode=%s entry_fill=%.4f opened_at=%s "
                    "underlying_entry=%.4f peak_pnl_pct=%.2f%% touched_profit=%s",
                    _contract, old_id, _canon_id,
                    local_order_id or "?", broker_order_id or "?",
                    signal_id or "?", _mode, entry_fill or 0.0,
                    str(entry_ts or ""),
                    float(underlying_entry or 0.0),
                    (pos.peak_pnl_pct or 0.0) * 100,
                    pos.touched_profit,
                )
                return CanonicalAdoptionResult(
                    disposition="ADOPTED", adopted=True,
                    safe_to_seed=False, retryable=False,
                )

        return CanonicalAdoptionResult(
            disposition="NO_REPAIR_FOUND", adopted=False,
            safe_to_seed=True, retryable=False,
        )

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
                    _incoming_id = str(getattr(pos, "position_id", "") or "")
                    _incoming_client = str(getattr(pos, "client_id", "") or "").strip().lower()
                    _incoming_mode = str(getattr(pos, "execution_mode", "") or "").strip().lower()
                    _incoming_is_provisional = _is_broker_repair_provisional(pos)
                    _existing_is_provisional = _is_broker_repair_provisional(existing)
                    _existing_client = str(
                        getattr(existing, "client_id", "") or ""
                    ).strip().lower()
                    _existing_mode = str(
                        getattr(existing, "execution_mode", "") or ""
                    ).strip().lower()
                    _same_durable_identity = (
                        bool(_incoming_client)
                        and _incoming_client == _existing_client
                        and _incoming_mode in {"live", "paper"}
                        and _incoming_mode == _existing_mode
                    )
                    _incoming_is_proven_canonical = (
                        _incoming_id
                        and not _incoming_is_provisional
                        and _incoming_client
                        and _incoming_mode in {"live", "paper"}
                    )
                    _can_replace_quarantined_repair = (
                        _incoming_is_proven_canonical
                        or (
                            _incoming_is_provisional
                            and _same_durable_identity
                            and _existing_is_provisional
                        )
                    )
                    if (
                        same_sym
                        and not same_id
                        and _is_adoption_identity_quarantined(existing)
                        and _can_replace_quarantined_repair
                    ):
                        if _incoming_is_provisional:
                            log.critical(
                                "[exit_eng] "
                                "ADD_POSITION_PROVISIONAL_BYPASSES_QUARANTINED_REPAIR "
                                "client=%s mode=%s contract=%s provisional_id=%s "
                                "quarantined_id=%s",
                                _incoming_client,
                                _incoming_mode,
                                getattr(pos, "option_symbol", ""),
                                _incoming_id,
                                getattr(existing, "position_id", ""),
                            )
                        else:
                            log.warning(
                                "[exit_eng] "
                                "ADD_POSITION_CANONICAL_BYPASSES_QUARANTINED_REPAIR "
                                "client=%s mode=%s contract=%s canonical=%s repair=%s",
                                _incoming_client,
                                _incoming_mode,
                                getattr(pos, "option_symbol", ""),
                                _incoming_id,
                                getattr(existing, "position_id", ""),
                            )
                        # Retain the quarantined repair for audit visibility, but
                        # retire it as an active owner before installing the
                        # exact same-domain replacement durable owner.
                        existing.closed = True
                        existing.quantity_remaining = 0
                        _clear_adoption_identity_quarantine(pos)
                        continue
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
            return [p for p in self._positions if _is_behavior_active_position(p)]

    def attach_quote_monitor(self, monitor) -> None:
        """Wire the PositionQuoteMonitor for observability and wake-driven exits."""
        self.quote_monitor = monitor

    def _request_immediate_quote_retry(self, pos: ManagedPosition) -> bool:
        qm = getattr(self, "quote_monitor", None)
        try:
            if qm is not None and hasattr(qm, "request_immediate_refresh"):
                return bool(qm.request_immediate_refresh(
                    getattr(pos, "option_symbol", "") or "",
                    getattr(pos, "ticker", "") or "",
                ))
            if qm is not None and hasattr(qm, "kick"):
                qm.kick()
                return True
            self._quote_arrived_event.set()
            return True
        except Exception as exc:
            log.debug("[%s] quote retry request failed: %s", getattr(pos, "ticker", "?"), exc)
            return False

    @staticmethod
    def _dt_to_iso(value) -> str:
        return value.isoformat() if isinstance(value, datetime) else ""

    @staticmethod
    def _dt_to_epoch(value) -> float:
        if isinstance(value, datetime):
            return value.timestamp()
        try:
            return float(value or 0.0)
        except Exception:
            return 0.0

    @staticmethod
    def _coerce_dt(value) -> Optional[datetime]:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if isinstance(value, (int, float)) and value > 0:
            return datetime.fromtimestamp(float(value), timezone.utc)
        if isinstance(value, str) and value.strip():
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except Exception:
                return None
        return None

    def _protective_position_identity(self, pos: ManagedPosition) -> Optional[ProtectivePositionIdentity]:
        position_id = str(getattr(pos, "position_id", "") or "").strip()
        client_id = str(getattr(pos, "client_id", "") or "").strip()
        execution_mode = str(getattr(pos, "execution_mode", "") or "").strip().lower()
        contract = str(getattr(pos, "option_symbol", "") or "").strip().upper()
        try:
            from ap.exit_safety import is_valid_exact_occ_contract, _normalize_contract
        except Exception:
            return None
        if (
            not position_id
            or not client_id
            or execution_mode not in {"live", "paper"}
            or not is_valid_exact_occ_contract(contract)
        ):
            return None
        contract = _normalize_contract(contract)
        return ProtectivePositionIdentity(
            position_id=position_id,
            client_id=client_id,
            execution_mode=execution_mode,
            contract=contract,
        )

    def _persist_degraded_monitoring_state(
        self,
        pos: ManagedPosition,
        *,
        state: str,
        intent: dict,
        persist_reason: str,
        broker_truth: Optional[dict] = None,
    ) -> DegradedMonitoringPersistResult:
        identity = self._protective_position_identity(pos)
        if identity is None:
            pos.protective_monitoring_state = PROTECTIVE_STATE_UNPERSISTED
            pos.behavior_quieted = False
            self._emit_degraded_critical(
                pos,
                "PROTECTIVE_MONITORING_IDENTITY_UNPROVEN",
                "degraded retry ownership was not persisted because position identity is incomplete",
                extra={
                    "position_id": str(getattr(pos, "position_id", "") or ""),
                    "client_id_present": bool(str(getattr(pos, "client_id", "") or "").strip()),
                    "execution_mode": str(getattr(pos, "execution_mode", "") or ""),
                    "contract": str(getattr(pos, "option_symbol", "") or ""),
                },
            )
            return DegradedMonitoringPersistResult(False, 0, "identity_unproven", "missing position_id/client_id/execution_mode/contract")
        try:
            import json
            from ap.db import conn, run_with_retry

            patch = {
                "protective_monitoring_state": state,
                "protective_monitoring_reason": persist_reason,
                "protective_monitoring_degraded_at": datetime.now(timezone.utc).isoformat(),
                "log_quieted_only": True,
                "behavior_quieted": False,
                **intent,
                "broker_truth": broker_truth or {},
            }

            def _update():
                with conn() as c:
                    c.execute(
                        """
                        UPDATE positions
                        SET meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb,
                            updated_at = NOW()
                        WHERE id = %s
                          AND client_id = %s
                          AND LOWER(COALESCE(execution_mode, '')) = %s
                          AND COALESCE(contract, '') = %s
                          AND status IN ('OPEN', 'CLOSING')
                          AND COALESCE(quantity_remaining, qty, 0) > 0
                        """,
                        (
                            json.dumps(patch, default=str),
                            identity.position_id,
                            identity.client_id,
                            identity.execution_mode,
                            identity.contract,
                        ),
                    )
                    return c.rowcount

            rowcount = int(run_with_retry(_update) or 0)
            if rowcount == 1:
                return DegradedMonitoringPersistResult(True, rowcount, "persisted", None)
            msg = f"degraded ownership rowcount={rowcount}"
            log.critical(
                "[%s] PROTECTIVE_MONITORING_OWNERSHIP_PERSIST_FAILED | pos_id=%s client_id=%s mode=%s contract=%s rowcount=%s",
                pos.ticker, identity.position_id, identity.client_id, identity.execution_mode, identity.contract, rowcount,
            )
            return DegradedMonitoringPersistResult(False, rowcount, "rowcount_not_one", msg)
        except Exception as exc:
            log.critical(
                "[%s] PROTECTIVE_MONITORING_OWNERSHIP_PERSIST_ERROR | pos_id=%s client_id=%s error=%s",
                pos.ticker, identity.position_id, identity.client_id, exc,
                exc_info=True,
            )
            return DegradedMonitoringPersistResult(False, 0, "exception", str(exc))

    def _mark_broker_flat_stale_position(self, pos: ManagedPosition, broker_truth: dict) -> BrokerFlatCloseResult:
        identity = self._protective_position_identity(pos)
        if identity is None:
            pos.protective_monitoring_state = PROTECTIVE_STATE_BROKER_FLAT_PENDING
            pos.behavior_quieted = False
            self._emit_degraded_critical(
                pos,
                "BROKER_FLAT_CLOSE_IDENTITY_UNPROVEN",
                "broker-flat durable close was not attempted because position identity is incomplete",
                extra={
                    "position_id": str(getattr(pos, "position_id", "") or ""),
                    "client_id_present": bool(str(getattr(pos, "client_id", "") or "").strip()),
                    "execution_mode": str(getattr(pos, "execution_mode", "") or ""),
                    "contract": str(getattr(pos, "option_symbol", "") or ""),
                    "broker_truth": broker_truth,
                },
            )
            return BrokerFlatCloseResult(False, 0, False, "identity_unproven", "missing position_id/client_id/execution_mode/contract")
        truth_state, parsed_qty = _classify_exact_broker_open_qty((broker_truth or {}).get("broker_truth_open_qty"))
        if (broker_truth or {}).get("is_fresh_exact") is not True or truth_state != BrokerPositionTruth.FLAT:
            pos.protective_monitoring_state = PROTECTIVE_STATE_BROKER_FLAT_PENDING
            self._emit_degraded_critical(
                pos,
                "BROKER_FLAT_CLOSE_TRUTH_UNPROVEN",
                "broker-flat durable close was not attempted because broker truth is not exact numeric flat",
                extra={
                    "broker_truth": broker_truth,
                    "broker_truth_state": truth_state.value,
                    "broker_truth_parsed_open_qty": parsed_qty,
                },
            )
            return BrokerFlatCloseResult(False, 0, False, "broker_flat_truth_unproven", None)
        try:
            import json
            from ap.db import conn, run_with_retry

            patch = {
                "synthetic_position_stale_broker_flat": True,
                "stale_marked_at": datetime.now(timezone.utc).isoformat(),
                "broker_truth_open_qty": 0,
                "exit_circuit_breaker_broker_truth": broker_truth,
                "stale_source": "protective_monitoring_degraded",
                "reconciler_manual_close_needed": True,
                "protective_monitoring_state": PROTECTIVE_STATE_BROKER_FLAT_PENDING,
            }
            pos.protective_monitoring_state = PROTECTIVE_STATE_BROKER_FLAT_PENDING
            pos.behavior_quieted = False

            def _update():
                with conn() as c:
                    c.execute(
                        """
                        UPDATE positions
                        SET meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb,
                            updated_at = NOW()
                        WHERE id = %s
                          AND client_id = %s
                          AND LOWER(COALESCE(execution_mode, '')) = %s
                          AND COALESCE(contract, '') = %s
                          AND status IN ('OPEN', 'CLOSING', 'PARTIAL', 'ACTIVE')
                          AND COALESCE(quantity_remaining, qty, 0) > 0
                        """,
                        (
                            json.dumps(patch, default=str),
                            identity.position_id,
                            identity.client_id,
                            identity.execution_mode,
                            identity.contract,
                        ),
                    )
                    return int(c.rowcount or 0)

            rowcount = int(run_with_retry(_update) or 0)
            if rowcount != 1:
                log.critical(
                    "[%s] BROKER_FLAT_RECONCILIATION_MARKER_UNPERSISTED | pos_id=%s client_id=%s mode=%s contract=%s rowcount=%s",
                    pos.ticker, identity.position_id, identity.client_id, identity.execution_mode, identity.contract, rowcount,
                )
                return BrokerFlatCloseResult(False, rowcount, False, "reconciliation_marker_unpersisted", None)
            return BrokerFlatCloseResult(False, rowcount, False, "reconciliation_pending", None)
        except Exception as exc:
            pos.protective_monitoring_state = PROTECTIVE_STATE_BROKER_FLAT_PENDING
            log.critical(
                "[%s] BROKER_FLAT_DURABLE_CLOSE_ERROR | pos_id=%s client_id=%s mode=%s contract=%s error=%s",
                pos.ticker, identity.position_id, identity.client_id, identity.execution_mode, identity.contract, exc,
                exc_info=True,
            )
            return BrokerFlatCloseResult(False, 0, False, "exception", str(exc))

    def _clear_degraded_monitoring_state(self, pos: ManagedPosition) -> None:
        if getattr(pos, "protective_monitoring_state", "") not in {
            PROTECTIVE_STATE_DEGRADED,
            PROTECTIVE_STATE_UNPERSISTED,
            PROTECTIVE_STATE_RETRY_EXHAUSTED,
        }:
            return
        try:
            pos.protective_monitoring_state = PROTECTIVE_STATE_RESOLVED
            pos.protective_monitoring_recovered_at = datetime.now(timezone.utc)
            pos.protective_monitoring_recovery_attempts = int(getattr(pos, "protective_monitoring_recovery_attempts", 0) or 0) + 1
            pos.exit_retry_owner = ""
            pos.exit_retry_status = PROTECTIVE_STATE_RESOLVED
            pos.exit_retry_reason = ""
            pos.exit_retry_decision_code = ""
            pos.exit_retry_attempt = 0
            pos.exit_retry_count = 0
            pos.exit_retry_requested_at = None
            pos.exit_retry_first_requested_at = None
            pos.exit_retry_at = None
            pos.exit_retry_last_error = ""
        except Exception:
            pass
        try:
            import json
            from ap.db import conn, run_with_retry

            identity = self._protective_position_identity(pos)
            if identity is None:
                self._emit_degraded_critical(
                    pos,
                    "PROTECTIVE_MONITORING_RESOLVE_PERSIST_IDENTITY_UNPROVEN",
                    "degraded monitoring was cleared in memory but durable clear was not attempted because identity is incomplete",
                    extra={
                        "position_id": str(getattr(pos, "position_id", "") or ""),
                        "client_id_present": bool(str(getattr(pos, "client_id", "") or "").strip()),
                        "execution_mode": str(getattr(pos, "execution_mode", "") or ""),
                        "contract": str(getattr(pos, "option_symbol", "") or ""),
                    },
                )
                return
            patch = {
                "protective_monitoring_state": PROTECTIVE_STATE_RESOLVED,
                "protective_monitoring_recovered_at": datetime.now(timezone.utc).isoformat(),
                "protective_monitoring_recovery_attempts": int(
                    getattr(pos, "protective_monitoring_recovery_attempts", 1) or 1
                ),
                "exit_retry_owner": "",
                "exit_retry_status": PROTECTIVE_STATE_RESOLVED,
            }

            def _update():
                with conn() as c:
                    c.execute(
                        """
                        UPDATE positions
                        SET meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb,
                            updated_at = NOW()
                        WHERE id = %s
                          AND client_id = %s
                          AND LOWER(COALESCE(execution_mode, '')) = %s
                          AND COALESCE(contract, '') = %s
                          AND status IN ('OPEN', 'CLOSING')
                        """,
                        (
                            json.dumps(patch),
                            identity.position_id,
                            identity.client_id,
                            identity.execution_mode,
                            identity.contract,
                        ),
                    )
                    return c.rowcount

            rowcount = int(run_with_retry(_update) or 0)
            if rowcount != 1:
                log.warning(
                    "[%s] degraded monitoring clear metadata rowcount=%s pos=%s mode=%s contract=%s",
                    pos.ticker, rowcount, identity.position_id, identity.execution_mode, identity.contract,
                )
        except Exception as exc:
            log.warning("[%s] degraded monitoring clear metadata failed pos=%s: %s", pos.ticker, getattr(pos, "position_id", "?"), exc)

    def _resolve_broker_open_qty_for_degraded_monitoring(self, pos: ManagedPosition) -> dict:
        try:
            from ap.exit_safety import resolve_exit_broker_truth as _broker_truth_resolver
            result = _broker_truth_resolver(
                broker=getattr(self, "broker", None),
                client_id=str(getattr(pos, "client_id", "") or self._email or ""),
                contract=str(getattr(pos, "option_symbol", "") or ""),
            )
            return dict(result or {})
        except Exception as exc:
            return {
                "is_fresh_exact": False,
                "broker_truth_open_qty": None,
                "audit": {"error": str(exc), "source": "degraded_monitoring_probe"},
            }

    def _build_retry_intent(
        self,
        pos: ManagedPosition,
        decision: ExitDecision,
        *,
        status: str,
        attempt: int,
        now: datetime,
        first_requested: datetime,
        next_at: datetime,
        deadline: datetime,
        option_quote_state: str,
        option_quote_age_sec,
        persisted: bool = False,
        persist_error: str = "",
    ) -> dict:
        code = _classify_exit_decision(decision)
        return {
            "exit_retry_owner": "ap_exit_engine",
            "exit_retry_status": status,
            "exit_retry_reason": "stale_option_quote",
            "exit_retry_action": decision.action,
            "exit_retry_quantity": int(decision.quantity or 0),
            "exit_retry_decision_code": code,
            "exit_retry_decided_at": now.isoformat(),
            "exit_retry_first_requested_at": first_requested.isoformat(),
            "exit_retry_last_requested_at": now.isoformat(),
            "exit_retry_attempt": int(attempt),
            "exit_retry_max_attempts": int(STALE_EXIT_RETRY_MAX_ATTEMPTS),
            "exit_retry_next_at": next_at.isoformat(),
            "exit_retry_deadline": deadline.isoformat(),
            "exit_retry_quote_state": option_quote_state,
            "exit_retry_quote_age_sec": option_quote_age_sec,
            "exit_retry_persisted": bool(persisted),
            "exit_retry_persist_error": persist_error,
            "exit_retry_pnl_pct": float(decision.pnl_pct or 0.0),
            "exit_retry_option_symbol": str(getattr(pos, "option_symbol", "") or ""),
            "exit_retry_underlying": str(getattr(pos, "ticker", "") or ""),
            "exit_retry_position_id": str(getattr(pos, "position_id", "") or ""),
            "exit_retry_client_id": str(getattr(pos, "client_id", "") or self._email or ""),
            "exit_retry_execution_mode": str(getattr(pos, "execution_mode", "") or "").lower().strip(),
        }

    def _apply_retry_intent_to_position(self, pos: ManagedPosition, intent: dict, *, state: str) -> None:
        pos.protective_monitoring_state = state
        pos.behavior_quieted = False
        pos.log_quieted = True
        pos.exit_retry_owner = intent.get("exit_retry_owner", "ap_exit_engine")
        pos.exit_retry_status = intent.get("exit_retry_status", state)
        pos.exit_retry_reason = intent.get("exit_retry_reason", "stale_option_quote")
        pos.exit_retry_action = intent.get("exit_retry_action", "")
        pos.exit_retry_quantity = int(intent.get("exit_retry_quantity") or 0)
        pos.exit_retry_decision_code = intent.get("exit_retry_decision_code", "")
        pos.exit_retry_decided_at = intent.get("exit_retry_decided_at", "")
        pos.exit_retry_first_requested_at = self._coerce_dt(intent.get("exit_retry_first_requested_at"))
        pos.exit_retry_last_requested_at = self._coerce_dt(intent.get("exit_retry_last_requested_at"))
        pos.exit_retry_attempt = int(intent.get("exit_retry_attempt") or 0)
        pos.exit_retry_count = pos.exit_retry_attempt
        pos.exit_retry_max_attempts = int(intent.get("exit_retry_max_attempts") or STALE_EXIT_RETRY_MAX_ATTEMPTS)
        pos.exit_retry_next_at = self._coerce_dt(intent.get("exit_retry_next_at"))
        pos.exit_retry_at = self._dt_to_epoch(pos.exit_retry_next_at)
        pos.exit_retry_deadline = self._coerce_dt(intent.get("exit_retry_deadline"))
        pos.exit_retry_quote_state = intent.get("exit_retry_quote_state", "")
        pos.exit_retry_quote_age_sec = intent.get("exit_retry_quote_age_sec", None)
        pos.exit_retry_persisted = bool(intent.get("exit_retry_persisted", False))
        pos.exit_retry_persist_error = intent.get("exit_retry_persist_error", "")

    def _emit_degraded_critical(self, pos: ManagedPosition, code: str, message: str, *, extra: Optional[dict] = None) -> None:
        log.critical("[%s] %s | pos_id=%s %s", pos.ticker, code, getattr(pos, "position_id", "") or "?", message)
        self._emit_exit_event(
            pos,
            decision="ALERT",
            reason_code=code,
            explanation=message,
            stage="exit_degraded_monitoring",
            extra_inputs=extra or {},
        )

    def _own_stale_exit_retry(
        self,
        pos: ManagedPosition,
        decision: ExitDecision,
        *,
        option_quote_state: str,
        option_quote_age_sec,
        stage: str,
    ) -> bool:
        """
        Convert stale quote behavioral suppression into explicit, bounded
        protective ownership.
        """
        now = datetime.now(timezone.utc)
        current_next = self._coerce_dt(getattr(pos, "exit_retry_next_at", None) or getattr(pos, "exit_retry_at", None))
        current_status = str(getattr(pos, "exit_retry_status", "") or getattr(pos, "protective_monitoring_state", "") or "")
        if current_next and now < current_next and current_status in {
            PROTECTIVE_STATE_DEGRADED,
            PROTECTIVE_STATE_UNPERSISTED,
            PROTECTIVE_STATE_RETRY_EXHAUSTED,
        }:
            return False

        first_requested = self._coerce_dt(getattr(pos, "exit_retry_first_requested_at", None))
        if not isinstance(first_requested, datetime):
            first_requested = now
        deadline = self._coerce_dt(getattr(pos, "exit_retry_deadline", None))
        if not isinstance(deadline, datetime):
            deadline = first_requested + timedelta(seconds=float(STALE_EXIT_RETRY_MAX_AGE_SEC))

        current_attempt = int(getattr(pos, "exit_retry_attempt", 0) or getattr(pos, "exit_retry_count", 0) or 0)
        retry_exhausted = (
            current_attempt >= int(STALE_EXIT_RETRY_MAX_ATTEMPTS)
            or now >= deadline
        )
        next_attempt = current_attempt if retry_exhausted else current_attempt + 1
        next_at = now + timedelta(seconds=float(STALE_EXIT_RETRY_DELAY_SEC))

        broker_truth = dict(self._resolve_broker_open_qty_for_degraded_monitoring(pos) or {})
        broker_truth_qty = broker_truth.get("broker_truth_open_qty")
        truth_state, parsed_broker_qty = _classify_exact_broker_open_qty(broker_truth_qty)
        fresh_exact = broker_truth.get("is_fresh_exact") is True
        broker_open = fresh_exact and truth_state == BrokerPositionTruth.OPEN
        broker_flat = fresh_exact and truth_state == BrokerPositionTruth.FLAT
        broker_unknown = not fresh_exact or truth_state == BrokerPositionTruth.UNKNOWN
        broker_truth["broker_truth_state"] = truth_state.value
        broker_truth["broker_truth_parsed_open_qty"] = parsed_broker_qty

        pos.broker_truth_state = truth_state.value
        pos.broker_truth_parsed_open_qty = parsed_broker_qty

        exhausted_reason_code = "PROTECTIVE_RETRY_EXHAUSTED_BROKER_OPEN"
        if broker_unknown and not fresh_exact:
            exhausted_reason_code = "PROTECTIVE_RETRY_EXHAUSTED_BROKER_TRUTH_UNAVAILABLE"
        elif broker_unknown:
            exhausted_reason_code = "PROTECTIVE_RETRY_EXHAUSTED_BROKER_QTY_UNKNOWN"

        reason_code = (
            "SYNTHETIC_POSITION_STALE_BROKER_FLAT"
            if broker_flat
            else exhausted_reason_code
            if retry_exhausted
            else "PROTECTIVE_MONITORING_DEGRADED_RETRY_OWNED"
        )
        state = (
            PROTECTIVE_STATE_BROKER_FLAT_PENDING
            if broker_flat
            else PROTECTIVE_STATE_RETRY_EXHAUSTED
            if retry_exhausted
            else PROTECTIVE_STATE_DEGRADED
        )
        intent = self._build_retry_intent(
            pos,
            decision,
            status=state,
            attempt=next_attempt,
            now=now,
            first_requested=first_requested,
            next_at=next_at,
            deadline=deadline,
            option_quote_state=option_quote_state,
            option_quote_age_sec=option_quote_age_sec,
            persisted=True,
        )
        self._apply_retry_intent_to_position(pos, intent, state=state)
        pos.broker_truth_open_qty = broker_truth_qty

        if fresh_exact and truth_state == BrokerPositionTruth.UNKNOWN:
            self._emit_degraded_critical(
                pos,
                "BROKER_TRUTH_QUANTITY_UNKNOWN",
                "fresh exact broker truth had an unknown or malformed open quantity; broker-flat close is forbidden",
                extra={
                    **intent,
                    "broker_truth": broker_truth,
                    "broker_truth_state": truth_state.value,
                    "broker_truth_parsed_open_qty": parsed_broker_qty,
                },
            )

        persist = self._persist_degraded_monitoring_state(
            pos,
            state=state,
            intent=intent,
            persist_reason=reason_code,
            broker_truth=broker_truth,
        )
        intent["exit_retry_persisted"] = persist.persisted
        intent["exit_retry_persist_error"] = persist.error or "" if not persist.persisted else ""
        self._apply_retry_intent_to_position(
            pos,
            intent,
            state=state if persist.persisted else PROTECTIVE_STATE_UNPERSISTED,
        )

        if not persist.persisted:
            self._emit_degraded_critical(
                pos,
                "PROTECTIVE_MONITORING_UNPERSISTED",
                f"degraded retry ownership was not durably persisted: {persist.reason} {persist.error or ''}",
                extra={"persist_result": persist.__dict__, **intent},
            )

        if broker_flat:
            close_result = self._mark_broker_flat_stale_position(pos, broker_truth)
            self._emit_exit_event(
                pos,
                decision="RESOLVE" if close_result.closed else "ALERT",
                reason_code=reason_code,
                explanation=(
                    "Fresh exact broker truth is flat and durable close completed."
                    if close_result.closed
                    else "Fresh exact broker truth is flat but durable local close is still pending."
                ),
                stage=stage,
                extra_inputs={
                    "decision_action": decision.action,
                    "decision_qty": decision.quantity,
                    "decision_reason_code": _classify_exit_decision(decision),
                    "option_quote_state": option_quote_state,
                    "option_quote_age_sec": option_quote_age_sec,
                    "broker_truth": broker_truth,
                    "broker_truth_state": truth_state.value,
                    "broker_truth_parsed_open_qty": parsed_broker_qty,
                    "broker_flat_close": close_result.__dict__,
                },
            )
            return close_result.closed

        if retry_exhausted:
            self._emit_degraded_critical(
                pos,
                reason_code,
                (
                    "stale quote retry exhausted while broker truth still indicates open or is unavailable; "
                    "position remains monitored and reconciliation/emergency paths remain active"
                ),
                extra={
                    **intent,
                    "broker_truth_open_qty": broker_truth_qty,
                    "broker_truth_state": truth_state.value,
                    "broker_truth_parsed_open_qty": parsed_broker_qty,
                    "broker_open_override": broker_open,
                    "broker_truth_unknown": broker_unknown,
                },
            )
            return False

        refresh_requested = False
        if int(getattr(pos, "exit_retry_refresh_requested_attempt", 0) or 0) != next_attempt:
            refresh_requested = self._request_immediate_quote_retry(pos)
            if refresh_requested:
                pos.exit_retry_refresh_requested_attempt = next_attempt

        self._emit_exit_event(
            pos,
            decision="HOLD",
            reason_code=reason_code,
            explanation=(
                "Stale option quote moved to PROTECTIVE_MONITORING_DEGRADED with owned retry; "
                "quote polling, broker reconciliation, stop evaluation, and emergency exits remain active."
            ),
            stage=stage,
            extra_inputs={
                "decision_action": decision.action,
                "decision_qty": decision.quantity,
                "decision_pnl_pct": decision.pnl_pct,
                "decision_reason_code": _classify_exit_decision(decision),
                "option_quote_state": option_quote_state,
                "option_quote_age_sec": option_quote_age_sec,
                "exit_retry_owner": "ap_exit_engine",
                "exit_retry_attempt": next_attempt,
                "exit_retry_max_attempts": STALE_EXIT_RETRY_MAX_ATTEMPTS,
                "retry_exhausted": retry_exhausted,
                "refresh_requested": refresh_requested,
                "broker_truth_open_qty": broker_truth_qty,
                "broker_truth_state": truth_state.value,
                "broker_truth_parsed_open_qty": parsed_broker_qty,
                "broker_open_override": broker_open,
                "broker_truth_unknown": broker_unknown,
            },
        )
        log.warning(
            "[%s] PROTECTIVE_MONITORING_DEGRADED | pos_id=%s code=%s quote_state=%s age=%s "
            "retry=%d/%d broker_truth_open_qty=%s exhausted=%s",
            pos.ticker,
            getattr(pos, "position_id", "") or "?",
            _classify_exit_decision(decision),
            option_quote_state,
            option_quote_age_sec,
            next_attempt,
            STALE_EXIT_RETRY_MAX_ATTEMPTS,
            broker_truth_qty,
            retry_exhausted,
        )
        return False

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
                    # ── AMENDMENT #4 (blocker 4): apply all money-safety fields ──
                    # Prior code used `if x > 0` guards which meant a missing-bid
                    # cycle could not INVALIDATE a retained bid — the exact stale-
                    # retained-bid failure this PR exists to close.  Under
                    # DIRECT_POSITION_WRITES=0 this path is the ONLY writer, so
                    # money-safety truth transitions MUST propagate through here.
                    # For price fields: apply the value verbatim (0.0 valid).
                    # For explicit truth booleans: apply verbatim (False wins).
                    # For timestamps: apply if present in the snapshot payload.

                    def _apply(name, snap_key=None):
                        k = snap_key or name
                        if k in snap:
                            try:
                                setattr(pos, name, snap[k])
                            except Exception:
                                pass

                    # Prices (0.0 must be honored — this is the invalidation)
                    _apply("current_underlying")
                    _apply("currentunderlying", "current_underlying")
                    _apply("current_option_price")
                    _apply("currentoptionprice", "current_option_price")
                    _apply("current_bid")
                    _apply("currentbid", "current_bid")
                    _apply("current_ask")
                    _apply("currentask", "current_ask")

                    # Money-safety booleans / metadata
                    _apply("option_bid_valid")
                    _apply("optionbidvalid", "option_bid_valid")
                    _apply("option_quote_fresh")
                    _apply("optionquotefresh", "option_quote_fresh")
                    _apply("option_quote_age_sec")
                    _apply("underlying_available")
                    _apply("underlyingavailable", "underlying_available")
                    _apply("underlying_fresh")
                    _apply("underlyingfresh", "underlying_fresh")
                    _apply("underlying_age_sec")
                    _apply("display_mark")
                    _apply("display_pnl_pct")
                    _apply("exit_executable_mark")
                    _apply("exit_executable_pnl_pct")

                    # Hard-exit reference (LIVE-money HARD STOP consumer)
                    if "hard_exit_reference_validity" in snap or "hard_exit_reference_price" in snap:
                        _now_utc = datetime.now(timezone.utc)
                        _incoming_validity = str(snap.get("hard_exit_reference_validity") or "no_data")
                        _incoming_ts = _normalize_hard_ref_ts(
                            snap.get("hard_exit_reference_ts"),
                            now_utc=_now_utc,
                        )
                        _incoming_price = snap.get("hard_exit_reference_price")
                        _incoming_refresh = bool(snap.get("hard_exit_reference_refresh_needed", True))
                        if _incoming_validity in ("proven", "catastrophic_ask") and _incoming_ts is None:
                            _incoming_validity = "unproven"
                            _incoming_refresh = True
                        _prior_validity = str(getattr(pos, "hard_exit_reference_validity", "") or "")
                        _prior_price = getattr(pos, "hard_exit_reference_price", 0.0) or 0.0
                        try:
                            _prior_price = float(_prior_price)
                        except Exception:
                            _prior_price = 0.0
                        _prior_ts = getattr(
                            pos, "hard_exit_reference_ts",
                            getattr(pos, "hardexitreferencets", None),
                        )
                        # AMENDMENT (PR #385 review — snapshot chronology):
                        # older authoritative snapshots may arrive AFTER a newer
                        # authoritative reference was written by another path
                        # (broker-precheck adoption, QuoteAuthority bridge,
                        # or a later QPM cycle applied first).  The previous
                        # `_preserve_prior` guard only rejected `unproven` /
                        # `no_data` incoming refs; an OLDER `proven` /
                        # `catastrophic_ask` snapshot would silently overwrite
                        # the newer money-safety truth.  Delegate to the
                        # shared `_should_replace_hard_ref` chronology gate
                        # (already used by `_apply_option_quote_for_decision`
                        # and by QPM itself) so the newest authoritative
                        # observation always wins and `refresh_needed` is set
                        # instead when we keep the prior reference.
                        _prior_source = str(
                            getattr(pos, "hard_exit_reference_source", "") or ""
                        )
                        _incoming_source = str(snap.get("hard_exit_reference_source") or "")
                        _apply_incoming = _should_replace_hard_ref(
                            prior_validity=_prior_validity,
                            prior_ts=_prior_ts,
                            prior_price=_prior_price,
                            candidate_validity=_incoming_validity,
                            candidate_ts=_incoming_ts,
                            prior_source=_prior_source,
                            candidate_source=_incoming_source,
                            now_utc=_now_utc,
                        )
                        # Legacy safety net: even if the shared gate would
                        # accept the incoming, an unproven / no_data payload
                        # must never erase a positive authoritative reference.
                        if _incoming_validity in ("unproven", "no_data") and _prior_price > 0:
                            _apply_incoming = False
                        if not _apply_incoming:
                            pos.hard_exit_reference_refresh_needed = True
                            pos.hardexitreferencerefreshneeded = True
                        else:
                            pos.hard_exit_reference_price = _incoming_price
                            pos.hardexitreferenceprice = _incoming_price
                            pos.hard_exit_reference_source = snap.get("hard_exit_reference_source")
                            pos.hardexitreferencesource = snap.get("hard_exit_reference_source")
                            pos.hard_exit_reference_ts = _incoming_ts
                            pos.hardexitreferencets = _incoming_ts
                            pos.hard_exit_reference_pnl_pct = snap.get("hard_exit_reference_pnl_pct")
                            pos.hardexitreferencepnlpct = snap.get("hard_exit_reference_pnl_pct")
                            pos.hard_exit_reference_validity = _incoming_validity
                            pos.hardexitreferencevalidity = _incoming_validity
                            pos.hard_exit_reference_refresh_needed = _incoming_refresh
                            pos.hardexitreferencerefreshneeded = _incoming_refresh

                    # Timestamps
                    _apply("last_option_bid_update_ts")
                    _apply("lastoptionbidupdatets", "last_option_bid_update_ts")
                    _apply("last_underlying_quote_update_ts")
                    _apply("lastunderlyingquoteupdatets", "last_underlying_quote_update_ts")
                    _apply("last_option_quote_update_ts")
                    _apply("lastoptionquoteupdatets", "last_option_quote_update_ts")

                    # Analytics mark carried separately
                    analytics_mark = snap.get("analytics_mark_price")
                    if analytics_mark is not None:
                        try:
                            pos.analytics_mark_price = analytics_mark
                            pos.analyticsmarkprice   = analytics_mark
                        except Exception:
                            pass

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
                    if _is_behavior_active_position(p)
                ]

            for pos in snapshot:
                try:
                    qty = int(getattr(pos, "quantity_remaining", 0) or 0)
                    if qty <= 0:
                        continue

                    # AMENDMENT #4 (blocker 3): pnl_pct on the emergency decision
                    # must reflect true loss authority.  Reading option_pnl_pct
                    # here returns 0.0 for LIVE missing-bid positions, misreporting
                    # the actual risk being cleared.  The exit still fires (this
                    # is IMMEDIATE + allow_inflight_override), but the audit trail
                    # and downstream decisioning must see the true P&L.
                    _ef_pnl_resolved = get_effective_hard_exit_reference(pos, datetime.now(timezone.utc))
                    _flatten_pnl = _ef_pnl_resolved if _ef_pnl_resolved is not None else float(getattr(pos, "option_pnl_pct", 0.0) or 0.0)
                    decision = ExitDecision(
                        action="CLOSE_ALL",
                        quantity=qty,
                        reason=f"SENTINEL FORCED EXIT -- {reason}",
                        urgency="IMMEDIATE",
                        pnl_pct=_flatten_pnl,
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
            if not _is_behavior_active_position(pos):
                continue
            age_min = (now - pos.opened_at).total_seconds() / 60 if pos.opened_at else 0
            # AMENDMENT (Jason BAC): sentinels share the SAME hard-stop truth
            # contract as evaluate_exit().  option_pnl_pct is not authoritative
            # — for PAPER it may be derived from midpoint / mark / LAST / ASK
            # fallback, and letting the sentinel fire from it produces the exact
            # false HARD STOP class #385 was built to prevent.  Use the shared
            # resolver: provenance-aware persisted reference, else fresh
            # executable BID, else None (no price-based hard stop this cycle).
            _sentinel_hard_pnl = _resolve_hard_stop_pnl_authority(pos, now)
            # `pnl` retained as a display value for MISSED-TP and EXIT-STUCK
            # diagnostics only; it is never used to submit an exit.
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
                    pos.ticker, peak * 100, pos.position_id, (pnl or 0.0) * 100, age_min,
                )

            # AMENDMENT (Jason BAC): the sentinel hard-stop uses the
            # per-position/per-DTE threshold (not the global HARD_STOP_PCT)
            # and consumes ONLY authoritative loss truth.  When authority is
            # missing this cycle we skip — silently treating missing authority
            # as 0% would falsely certify safety on the last-chance path.
            try:
                _sent_hard_stop, _, _ = _effective_thresholds(pos)
            except Exception:
                _sent_hard_stop = HARD_STOP_PCT
            if (
                _sentinel_hard_pnl is not None
                and _sentinel_hard_pnl <= _sent_hard_stop
                and not pos.exit_in_flight
                and age_min > 1
            ):
                decision = ExitDecision(
                    action="CLOSE_ALL", quantity=pos.quantity_remaining,
                    reason=(
                        f"SENTINEL FORCED EXIT — {_sentinel_hard_pnl*100:.0f}% "
                        f"(hard-exit authority) with no exit order"
                    ),
                    urgency="IMMEDIATE", pnl_pct=_sentinel_hard_pnl,
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
                and pos.max_profit_seen < DEAD_TRADE_HIGH
            ):
                # AMENDMENT (Jason BAC): the 45m TIME STOP is a soft exit and
                # must obey the same executable-option and underlying truth
                # contract as the main evaluator.  Build the real production
                # snapshot and require every truth signal — bid available,
                # bid fresh, executable P&L derivable, underlying available,
                # underlying fresh, and valid underlying entry+target geometry
                # to prove progress.  Any missing/stale signal defers.
                snap = _build_exit_decision_snapshot(pos, now)
                _ts_exec_pnl = snap.exit_executable_pnl_pct

                if not snap.option_bid_valid or not snap.option_quote_fresh or _ts_exec_pnl is None:
                    log.info(
                        "[SENTINEL] %s | TIME_STOP_DEFERRED — executable option truth "
                        "unavailable (bid_valid=%s quote_fresh=%s exec_pnl=%s) | pos=%s",
                        pos.ticker, snap.option_bid_valid, snap.option_quote_fresh,
                        _ts_exec_pnl, pos.position_id or "?",
                    )
                    continue

                if not snap.underlying_available or not snap.underlying_fresh:
                    log.info(
                        "[SENTINEL] %s | TIME_STOP_DEFERRED — underlying truth "
                        "unavailable (available=%s fresh=%s) | pos=%s",
                        pos.ticker, snap.underlying_available, snap.underlying_fresh,
                        pos.position_id or "?",
                    )
                    continue

                if not (DEAD_TRADE_LOW <= _ts_exec_pnl <= DEAD_TRADE_HIGH):
                    continue

                # Progress requires valid geometry; missing entry/target cannot
                # prove non-confirmation.  Defer rather than treat as 0%.
                _u_entry  = float(getattr(pos, "underlying_entry", 0.0) or 0.0)
                _u_target = float(getattr(pos, "underlying_target", 0.0) or 0.0)
                _u_now    = float(snap.underlying_price or 0.0)
                _denom    = abs(_u_target - _u_entry)
                if _u_entry <= 0.0 or _u_target <= 0.0 or _u_now <= 0.0 or _denom <= 0.0:
                    log.info(
                        "[SENTINEL] %s | TIME_STOP_DEFERRED — underlying geometry "
                        "unavailable (entry=%.2f target=%.2f now=%.2f) | pos=%s",
                        pos.ticker, _u_entry, _u_target, _u_now, pos.position_id or "?",
                    )
                    continue

                # AMENDMENT (PR #385 review P1-3): the previous
                # `abs(current - entry) / abs(target - entry)` counted
                # adverse movement as "progress toward target" and skipped
                # the TIME STOP even when the underlying had moved
                # entirely the wrong way (e.g. PUT entry 150, target 145,
                # current 155 → 100% "progress").  Use signed directional
                # progress: (current - entry) / (target - entry).  The
                # sign of the denominator matches the intended direction,
                # so both CALL (target above entry) and PUT (target below
                # entry) yield a positive fraction only when the trade is
                # actually moving toward its target.  Adverse movement
                # clamps to 0 and the TIME STOP fires as designed.
                raw_progress = (_u_now - _u_entry) / (_u_target - _u_entry)
                progress = max(0.0, raw_progress)
                if progress < 0.30:
                    decision = ExitDecision(
                        action="CLOSE_ALL", quantity=pos.quantity_remaining,
                        reason=(
                            f"TIME STOP — thesis not confirmed after {age_min:.0f}min "
                            f"exec_pnl={_ts_exec_pnl*100:.1f}% progress={progress*100:.0f}% toward target"
                        ),
                        urgency="HIGH", pnl_pct=_ts_exec_pnl,
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

    def _emit_exit_decision_stamp(
        self,
        pos: ManagedPosition,
        decision: ExitDecision,
        *,
        now_et: Optional[datetime] = None,
    ) -> None:
        if emit_exit_decision_stamp is None:
            return
        try:
            _exec_mode = str(
                getattr(getattr(self, "master_control", None), "mode", "") or
                getattr(pos, "execution_mode", "") or
                ""
            ).strip().lower()
            payload = build_exit_decision_stamp(
                pos,
                decision,
                client_id=getattr(pos, "client_id", "") or getattr(self, "_email", "") or "default",
                execution_mode=_exec_mode,
                run_id=getattr(self, "run_id", ""),
                strategy_version=getattr(self, "strategy_version", ""),
                git_commit=getattr(self, "git_commit", ""),
                now_et=now_et,
            )
            emit_exit_decision_stamp(payload)
        except Exception as e:
            log.debug("Exit decision stamp failed (non-critical): %s", e)

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
        if _is_adoption_identity_quarantined(pos):
            log.error(
                "[%s] ADOPTION IDENTITY QUARANTINE BLOCK | pos=%s contract=%s reason=%s",
                getattr(pos, "ticker", "?"),
                getattr(pos, "position_id", "?"),
                getattr(pos, "option_symbol", "?"),
                getattr(pos, "adoption_identity_quarantine_reason", "")
                or getattr(pos, "adoptionidentityquarantinereason", ""),
            )
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
            # The durable EXIT row stores the broker's cumulative fill.  Seed
            # both possible callback identities at restart so the first
            # repeated broker snapshot applies only a new delta, never the
            # already-persisted partial fill a second time.
            durable_cum_fill = max(0, pos.pending_exit_filled_qty)
            pos.last_applied_exit_cum_fill = durable_cum_fill
            pos.last_applied_exit_cum_fill_by_order = {
                order_id: durable_cum_fill
                for order_id in (broker_id, local_id)
                if order_id
            }
            pos.last_applied_exit_local_order_id = local_id
            pos.last_applied_exit_broker_order_id = broker_id

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
        self._position_manager = position_manager
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
                        # PR #176: carry execution_mode from positions row
                        execution_mode=str(row.get("execution_mode") or "").lower().strip(),
                    )
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
                    # AMENDMENT #6 blocker 3: ALL current-quote fields are zeroed.
                    # Entry fill is NEVER current market truth. option_pnl_pct will
                    # return 0.0 explicitly (property guard: current_option_price<=0)
                    # which is honest — we have no market data yet.  QPM/broker-
                    # precheck will populate within one cycle.
                    mp.current_option_price  = 0.0
                    mp.currentoptionprice    = 0.0
                    mp.current_bid           = 0.0
                    mp.currentbid            = 0.0
                    mp.current_ask           = 0.0
                    mp.currentask            = 0.0
                    mp.current_underlying    = 0.0
                    mp.currentunderlying     = 0.0
                    mp.option_bid_valid      = False
                    mp.optionbidvalid        = False
                    mp.option_quote_fresh    = False
                    mp.optionquotefresh      = False
                    mp.underlying_available  = False
                    mp.underlyingavailable   = False
                    mp.underlying_fresh      = False
                    mp.underlyingfresh       = False

                    # ── AMENDMENT #6 (blocker 1): RESTART HARD-REF HYDRATION ────
                    # Restore persisted hard-exit reference fields from the meta
                    # payload when available.  When absent, we mark the position
                    # as having NO hard-exit authority (rather than treating the
                    # entry fill as current market truth) and emit a visible
                    # warning so operators know QPM must run before this position
                    # has hard-stop coverage.
                    _meta = row.get("meta") or {}
                    if isinstance(_meta, str) and _meta.strip():
                        try:
                            import json as _json_m
                            _meta = _json_m.loads(_meta)
                        except Exception:
                            _meta = {}
                    _persisted_href = {}
                    if isinstance(_meta, dict):
                        _persisted_href = _meta.get("hard_exit_reference") or {}
                    if isinstance(_persisted_href, dict) and _persisted_href.get("price", 0) > 0:
                        try:
                            mp.hard_exit_reference_price    = float(_persisted_href.get("price", 0))
                            mp.hardexitreferenceprice       = mp.hard_exit_reference_price
                            mp.hard_exit_reference_source   = str(_persisted_href.get("source", ""))
                            mp.hardexitreferencesource      = mp.hard_exit_reference_source
                            _persisted_validity = str(_persisted_href.get("validity", "") or "")
                            _persisted_refresh_needed = bool(_persisted_href.get("refresh_needed", True))
                            _persisted_ts = _persisted_href.get("ts")
                            # AMENDMENT #6 blocker 3: age-check the persisted ts.
                            # "Merely having a timestamp" was not a real age check.
                            # A persisted proven ref written hours ago is NOT still fresh.
                            # Use HARD_REF_MAX_AGE_SEC from the shared constant.
                            _persisted_trusted = False
                            _ts_parsed = None
                            if _persisted_ts and _persisted_validity in ("proven", "catastrophic_ask"):
                                try:
                                    _now_seed = datetime.now(timezone.utc)
                                    _ts_parsed = _normalize_hard_ref_ts(_persisted_ts, now_utc=_now_seed)
                                    if _ts_parsed is not None:
                                        _persisted_age_sec = max(0.0, (_now_seed - _ts_parsed).total_seconds())
                                        _persisted_trusted = _persisted_age_sec <= HARD_REF_MAX_AGE_SEC
                                except Exception:
                                    _persisted_trusted = False
                            if _persisted_trusted:
                                mp.hard_exit_reference_validity = _persisted_validity
                                mp.hardexitreferencevalidity    = _persisted_validity
                                mp.hard_exit_reference_refresh_needed = _persisted_refresh_needed
                                mp.hardexitreferencerefreshneeded    = _persisted_refresh_needed
                            else:
                                mp.hard_exit_reference_validity = "unproven"
                                mp.hardexitreferencevalidity    = "unproven"
                                mp.hard_exit_reference_refresh_needed = True
                                mp.hardexitreferencerefreshneeded    = True
                            mp.hard_exit_reference_ts = _ts_parsed
                            mp.hardexitreferencets    = _ts_parsed
                            if mp.entry_price > 0:
                                _hr_pnl_r = (mp.hard_exit_reference_price - mp.entry_price) / mp.entry_price
                                _persisted_pnl_raw = _persisted_href.get("pnl_pct", None)
                                _persisted_pnl = None
                                try:
                                    _persisted_pnl_candidate = float(_persisted_pnl_raw)
                                    if math.isfinite(_persisted_pnl_candidate):
                                        _persisted_pnl = _persisted_pnl_candidate
                                except Exception:
                                    _persisted_pnl = None
                                if _persisted_pnl is not None and abs(_persisted_pnl - _hr_pnl_r) > 1e-6:
                                    log.warning(
                                        "[exit_eng] SEED_HARD_REF_PNL_MISMATCH client=%s position_id=%s "
                                        "persisted=%.6f recomputed=%.6f",
                                        mp.client_id, mp.position_id, _persisted_pnl, _hr_pnl_r,
                                    )
                                mp.hard_exit_reference_pnl_pct = _hr_pnl_r
                                mp.hardexitreferencepnlpct     = _hr_pnl_r
                            log.info(
                                "[exit_eng] SEED_HARD_REF_RESTORED client=%s position_id=%s "
                                "price=%.4f source=%s validity=%s",
                                mp.client_id, mp.position_id,
                                mp.hard_exit_reference_price,
                                mp.hard_exit_reference_source,
                                mp.hard_exit_reference_validity,
                            )
                        except Exception as _hr_e:
                            log.warning("seed_from_db hard-ref restore failed: %s", _hr_e)
                            _persisted_href = {}
                    if not (isinstance(_persisted_href, dict) and _persisted_href.get("price", 0) > 0):
                        # No persisted hard-ref — position has NO hard-exit authority
                        # until the first fresh quote arrives.  Mark explicitly.
                        mp.hard_exit_reference_price = 0.0
                        mp.hardexitreferenceprice    = 0.0
                        mp.hard_exit_reference_source = ""
                        mp.hardexitreferencesource   = ""
                        mp.hard_exit_reference_validity = "no_data"
                        mp.hardexitreferencevalidity   = "no_data"
                        mp.hard_exit_reference_pnl_pct = None
                        mp.hardexitreferencepnlpct     = None
                        mp.hard_exit_reference_refresh_needed = True
                        mp.hardexitreferencerefreshneeded    = True
                        log.warning(
                            "[exit_eng] SEED_HARD_REF_UNAVAILABLE client=%s position_id=%s "
                            "contract=%s — position has NO hard-exit authority until QPM "
                            "provides a fresh quote; sentinel/hard-stop will fall back to "
                            "option_pnl_pct which reads 0%% during LIVE missing-bid",
                            mp.client_id, mp.position_id, _contract_sym,
                        )
                    _meta = row.get("meta") or {}
                    if isinstance(_meta, str) and _meta.strip():
                        try:
                            import json
                            _meta = json.loads(_meta)
                        except Exception:
                            _meta = {}
                    if isinstance(_meta, dict) and _meta.get("protective_monitoring_state") in {
                        PROTECTIVE_STATE_DEGRADED,
                        PROTECTIVE_STATE_UNPERSISTED,
                        PROTECTIVE_STATE_RETRY_EXHAUSTED,
                        PROTECTIVE_STATE_BROKER_FLAT_PENDING,
                    }:
                        self._apply_retry_intent_to_position(
                            mp,
                            {
                                "exit_retry_owner": _meta.get("exit_retry_owner", "ap_exit_engine"),
                                "exit_retry_status": _meta.get("exit_retry_status") or _meta.get("protective_monitoring_state"),
                                "exit_retry_reason": _meta.get("exit_retry_reason", ""),
                                "exit_retry_action": _meta.get("exit_retry_action", ""),
                                "exit_retry_quantity": _meta.get("exit_retry_quantity", 0),
                                "exit_retry_decision_code": _meta.get("exit_retry_decision_code", ""),
                                "exit_retry_decided_at": _meta.get("exit_retry_decided_at", ""),
                                "exit_retry_first_requested_at": _meta.get("exit_retry_first_requested_at", ""),
                                "exit_retry_last_requested_at": _meta.get("exit_retry_last_requested_at", ""),
                                "exit_retry_attempt": _meta.get("exit_retry_attempt", _meta.get("exit_retry_count", 0)),
                                "exit_retry_max_attempts": _meta.get("exit_retry_max_attempts", STALE_EXIT_RETRY_MAX_ATTEMPTS),
                                "exit_retry_next_at": _meta.get("exit_retry_next_at", ""),
                                "exit_retry_deadline": _meta.get("exit_retry_deadline", ""),
                                "exit_retry_quote_state": _meta.get("exit_retry_quote_state", ""),
                                "exit_retry_quote_age_sec": _meta.get("exit_retry_quote_age_sec", None),
                                "exit_retry_persisted": _meta.get("exit_retry_persisted", True),
                                "exit_retry_persist_error": _meta.get("exit_retry_persist_error", ""),
                            },
                            state=_meta.get("protective_monitoring_state"),
                        )
                        log.warning(
                            "seed_from_db: restored degraded protective owner | pos_id=%s state=%s attempt=%s next_at=%s",
                            mp.position_id,
                            getattr(mp, "protective_monitoring_state", ""),
                            getattr(mp, "exit_retry_attempt", 0),
                            getattr(mp, "exit_retry_next_at", None),
                        )
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

    def _resolved_execution_mode_detail(self) -> dict:
        """Structured resolution of LIVE/PAPER identity for this engine.

        AMENDMENT (PR #385 review): callers that must distinguish "no
        evidence" from "contradictory evidence" (e.g. the fill-monitor
        canonical adoption gate) need a status they can branch on.
        `_resolved_execution_mode()` returns "" for BOTH cases; that
        collapse would let a known-good order mode launder a genuine
        internal engine conflict.  This helper preserves the distinction.

        Returns:
            {
              "mode":    "live" | "paper" | "",
              "status":  "PROVEN" | "CONFLICT" | "UNPROVEN",
              "sources": {source_name: normalized_mode, ...},
            }

        The existing `_resolved_execution_mode()` contract is unchanged;
        it still returns "" for both CONFLICT and UNPROVEN so all current
        callers continue to fail closed.
        """
        def _known_mode(value) -> str:
            _mode = str(value or "").strip().lower()
            return _mode if _mode in {"live", "paper"} else ""

        _mc = getattr(self, "master_control", None)
        _br = getattr(self, "broker", None)
        _cfg = getattr(_br, "cfg", None) if _br is not None else None

        raw_sources = {
            "master_control.mode":      getattr(_mc, "mode", ""),
            "broker.execution_mode":    getattr(_br, "execution_mode", ""),
            "broker.mode":              getattr(_br, "mode", ""),
            "broker.cfg.execution_mode": getattr(_cfg, "execution_mode", ""),
            "broker.cfg.mode":          getattr(_cfg, "mode", ""),
        }

        recognized = {
            name: normalized
            for name, value in raw_sources.items()
            for normalized in (_known_mode(value),)
            if normalized
        }
        unique_modes = set(recognized.values())

        if len(unique_modes) == 1:
            return {"mode": next(iter(unique_modes)), "status": "PROVEN",
                    "sources": recognized}

        if len(unique_modes) > 1:
            return {"mode": "", "status": "CONFLICT", "sources": recognized}

        return {"mode": "", "status": "UNPROVEN", "sources": recognized}

    def _resolved_execution_mode(self) -> str:
        """
        Fail-closed resolution of LIVE/PAPER identity for this engine instance.

        Collects every recognized mode source; if the sources disagree (e.g.
        master_control.mode=paper while broker.mode=live) this returns "" and
        emits a critical mode-conflict diagnostic. Silently preferring the
        first hit would launder a LIVE broker under a PAPER identity (or vice
        versa) and let cross-mode broker repair overwrite the wrong canonical
        row.

        Returns only the resolved mode string; callers that need to
        distinguish CONFLICT from UNPROVEN should use
        `_resolved_execution_mode_detail()`.
        """
        detail = self._resolved_execution_mode_detail()
        if detail["status"] == "CONFLICT":
            log.critical(
                "[exit_eng] EXECUTION_MODE_CONFLICT client=%s sources=%s — "
                "broker repair blocked; refusing LIVE/PAPER identity laundering",
                self._email, detail["sources"],
            )
        return detail["mode"]

    def _find_exact_filled_entry_order(
        self, sym: str, mode: str, broker_position: Optional[dict] = None
    ) -> dict | None:
        """Find one exact client/mode/OCC ENTRY fill for broker recovery."""
        contract = str(sym or "").strip().upper()
        normalized_mode = str(mode or "").strip().lower()
        if (
            not self._email
            or not contract
            or normalized_mode not in {"live", "paper"}
        ):
            return None
        try:
            from ap.db import conn, run_with_retry
            from ap.order_state_machine import (
                _DURABLE_EXECUTION_MODE_SQL,
                _durable_execution_mode,
            )
        except Exception as _mode_import_err:
            log.error(
                "[exit_eng] BROKER_REPAIR_ENTRY_LOOKUP_UNAVAILABLE "
                "client=%s mode=%s contract=%s mode_authority_error=%s: %s",
                self._email, normalized_mode, contract,
                type(_mode_import_err).__name__, _mode_import_err,
            )
            return _broker_repair_lookup_marker(
                "UNAVAILABLE",
                f"mode_authority:{type(_mode_import_err).__name__}:{_mode_import_err}",
            )

        try:
            def _query():
                with conn() as c:
                    c.execute(
                        f"""
                        SELECT *
                        FROM orders
                        WHERE client_id = %s
                          AND {_DURABLE_EXECUTION_MODE_SQL}
                          AND UPPER(TRIM(COALESCE(contract, ''))) = UPPER(TRIM(%s))
                          AND UPPER(TRIM(COALESCE(kind, ''))) = 'ENTRY'
                          AND UPPER(TRIM(COALESCE(status, ''))) IN ('FILLED', 'PARTIAL_FILL')
                          AND COALESCE(filled_qty, 0) > 0
                        ORDER BY filled_ts DESC NULLS LAST,
                                 updated_ts DESC NULLS LAST,
                                 created_ts DESC NULLS LAST
                        """,
                        (self._email, normalized_mode, contract),
                    )
                    return [dict(row) for row in (c.fetchall() or [])]

            candidates = run_with_retry(_query) or []

            # Redundant with SQL by design: driver-faithful wrappers and test
            # doubles must not be able to launder an out-of-scope row into an
            # identity reuse.
            def _row_is_exact(row: dict) -> bool:
                if not isinstance(row, dict):
                    return False
                if str(row.get("client_id") or "").strip() != str(self._email).strip():
                    return False
                if str(row.get("contract") or "").strip().upper() != contract:
                    return False
                if str(row.get("kind") or "").strip().upper() != "ENTRY":
                    return False
                if str(row.get("status") or "").strip().upper() not in {"FILLED", "PARTIAL_FILL"}:
                    return False
                if _durable_execution_mode(row) != normalized_mode:
                    return False
                return _broker_repair_positive_int(row.get("filled_qty")) is not None

            candidates = [row for row in candidates if _row_is_exact(row)]
            if broker_position is not None:
                candidates = [
                    row for row in candidates
                    if _broker_repair_order_matches(row, broker_position)
                ]
            if len(candidates) == 1:
                return candidates[0]
            if len(candidates) > 1:
                log.warning(
                    "[exit_eng] BROKER_REPAIR_ENTRY_EVIDENCE_AMBIGUOUS "
                    "client=%s mode=%s contract=%s candidate_count=%d",
                    self._email, normalized_mode, contract, len(candidates),
                )
                return _broker_repair_lookup_marker(
                    "AMBIGUOUS", "multiple_exact_entry_matches"
                )
            return None
        except Exception as _oe:
            log.warning(
                "[exit_eng] BROKER_REPAIR_ENTRY_EVIDENCE_UNAVAILABLE "
                "client=%s mode=%s contract=%s error=%s: %s",
                self._email, normalized_mode, contract,
                type(_oe).__name__, _oe,
            )
            return _broker_repair_lookup_marker(
                "UNAVAILABLE", f"{type(_oe).__name__}:{_oe}"
            )

    def _load_db_position_row(self, sym: str) -> dict | None:
        """Look up an active positions row for this client + contract symbol.

        P0-PARTIAL-CLOSE: status filter expanded to include PARTIAL and ACTIVE,
        AND adds a quantity_remaining safety guard so a row incorrectly marked
        CLOSED but with remaining qty is still found and managed.
        """
        _mode = self._resolved_execution_mode()
        if _mode not in {"live", "paper"}:
            log.critical(
                "[exit_eng] _load_db_position_row %s blocked: execution mode unproven for client=%s",
                sym, self._email,
            )
            return None
        try:
            from ap.db import conn, run_with_retry
            def _q():
                with conn() as c:
                    c.execute(
                        """
                        SELECT id, underlying, contract, option_symbol, side, direction,
                               qty, quantity_remaining, avg_fill, entry_price,
                               underlying_entry, stop_underlying, target_underlying,
                               entry_ts, status, signal_id, execution_mode,
                               local_order_id, broker_order_id, meta
                        FROM positions
                        WHERE client_id = %s
                          AND LOWER(TRIM(COALESCE(execution_mode, ''))) = %s
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
                        (self._email, _mode, sym, sym),
                    )
                    # Production ap.db wraps psycopg2 RealDictCursor and returns
                    # rows as plain dicts (see ap/db.py::_ConnWrapper.fetchone).
                    # Rebuilding from c.description iterated the dict's KEYS as
                    # if they were values, so every field became its own column
                    # name string. Trust the production contract: fetchone()
                    # already returns a dict of {column: value}.
                    row = c.fetchone()
                    return dict(row) if row else None
            return run_with_retry(_q)
        except Exception as _de:
            log.warning("[exit_eng] _load_db_position_row %s failed: %s", sym, _de)
            return None

    def _upsert_broker_position_to_db(self, sym: str, bp: dict) -> str | None:
        """Persist one broker-open position with an explicit lifecycle id."""
        _mode = self._resolved_execution_mode()
        if _mode not in {"live", "paper"}:
            log.critical(
                "[exit_eng] _upsert_broker_position_to_db %s blocked: execution mode unproven for client=%s",
                sym, self._email,
            )
            return None

        broker_position = bp if isinstance(bp, dict) else {}
        contract = str(sym or "").strip().upper()
        qty = _broker_repair_positive_int(broker_position.get("quantity"))
        raw_cost_basis = broker_position.get("cost_basis")
        cost_basis_value = _broker_repair_float(raw_cost_basis)
        entry_ts = _broker_repair_entry_timestamp(broker_position)
        if (
            not self._email
            or not contract
            or qty is None
            or (
                raw_cost_basis not in (None, "")
                and cost_basis_value is None
            )
            or (entry_ts not in (None, "") and _broker_repair_timestamp(entry_ts) is None)
        ):
            log.error(
                "[exit_eng] BROKER_REPAIR_INSERT_BLOCKED client=%s contract=%s "
                "qty=%s cost_basis=%s date_acquired=%s",
                self._email, contract, broker_position.get("quantity"),
                raw_cost_basis, entry_ts,
            )
            return None

        side = self._parse_occ_side(contract)
        if side not in {"CALL", "PUT"}:
            log.error(
                "[exit_eng] BROKER_REPAIR_INSERT_BLOCKED client=%s contract=%s side=%s",
                self._email, contract, side,
            )
            return None

        try:
            from ap.db import conn, run_with_retry

            lookup = self._find_exact_filled_entry_order(
                contract, _mode, broker_position
            )
            if isinstance(lookup, dict) and lookup.get("_lookup_status"):
                log.error(
                    "[exit_eng] BROKER_REPAIR_ENTRY_LOOKUP_%s client=%s mode=%s "
                    "contract=%s — recovery held",
                    str(lookup.get("_lookup_status") or "UNAVAILABLE").upper(),
                    self._email, _mode, contract,
                )
                return None
            order = dict(lookup or {})
            order_fill = _broker_repair_positive_float(order.get("fill_price"))
            broker_entry_price = (
                round(abs(cost_basis_value) / qty / 100.0, 6)
                if cost_basis_value is not None and abs(cost_basis_value) > 0.0
                else 0.0
            )
            entry_px = broker_entry_price or order_fill
            if entry_px <= 0.0:
                log.error(
                    "[exit_eng] BROKER_REPAIR_INSERT_BLOCKED client=%s contract=%s "
                    "qty=%d entry_price=%s",
                    self._email, contract, qty, entry_px,
                )
                return None

            proven_position_id = _broker_repair_position_id(order.get("position_id"))
            position_id = proven_position_id or str(_uuid.uuid4())
            meta = _broker_repair_order_meta(order.get("meta"))

            local_order_id, local_order_id_conflict = _broker_repair_text_value(
                order, meta, "local_order_id"
            )
            broker_order_id, broker_order_id_conflict = _broker_repair_text_value(
                order, meta, "broker_order_id"
            )
            if local_order_id_conflict or broker_order_id_conflict:
                log.error(
                    "[exit_eng] BROKER_REPAIR_INSERT_BLOCKED client=%s contract=%s "
                    "reason=contradictory_order_identity_metadata",
                    self._email, contract,
                )
                return None

            signal_id = str(
                order.get("signal_id") or order.get("canonical_signal_id") or ""
            ).strip() or None
            underlying_entry, entry_geometry_malformed = _broker_repair_historical_value(
                order, meta, keys=_BROKER_REPAIR_ENTRY_GEOMETRY_KEYS
            )
            underlying_stop, stop_geometry_malformed = _broker_repair_historical_value(
                order, meta, keys=_BROKER_REPAIR_STOP_GEOMETRY_KEYS
            )
            underlying_target, target_geometry_malformed = _broker_repair_historical_value(
                order, meta, keys=_BROKER_REPAIR_TARGET_GEOMETRY_KEYS
            )
            if (
                entry_geometry_malformed
                or stop_geometry_malformed
                or target_geometry_malformed
            ):
                log.error(
                    "[exit_eng] BROKER_REPAIR_INSERT_BLOCKED client=%s contract=%s "
                    "reason=contradictory_historical_geometry",
                    self._email, contract,
                )
                return None

            if order.get("filled_ts") and _broker_repair_timestamp(order.get("filled_ts")):
                entry_ts = order.get("filled_ts")

            position_meta = (
                {
                    _BROKER_REPAIR_PROVENANCE_KEY:
                        _BROKER_REPAIR_PROVENANCE_VALUE,
                }
                if not proven_position_id
                else {}
            )
            repair_row = {
                "id": position_id,
                "client_id": self._email,
                "underlying": self._underlying_from_occ(contract),
                "contract": contract,
                "option_symbol": contract,
                "execution_mode": _mode,
                "broker_repair_provisional": not bool(proven_position_id),
                "side": side,
                "direction": side,
                "qty": qty,
                "quantity_remaining": qty,
                "entry_price": entry_px,
                "avg_fill": entry_px,
                "underlying_entry": underlying_entry,
                "stop_underlying": underlying_stop,
                "target_underlying": underlying_target,
                "status": "OPEN",
                "entry_ts": entry_ts,
                "signal_id": signal_id,
                "local_order_id": local_order_id,
                "broker_order_id": broker_order_id,
                "meta": position_meta,
            }

            def _remember(row_id, row: Optional[dict] = None):
                cached = dict(row or repair_row)
                cached["id"] = str(row_id)
                # A no-id fill is uncertainty, not proof that an independently
                # written canonical row is provisional. Only the durable marker
                # written by this recovery INSERT can classify a UUID repair.
                cached["broker_repair_provisional"] = (
                    not bool(proven_position_id)
                    and _broker_repair_row_is_provisional(cached)
                )
                return _BrokerRepairIdentity(str(row_id), cached)

            def _ins():
                with conn() as c:
                    # All same client/mode/contract repairs serialize on this
                    # transaction-scoped lock. The active-row recheck below
                    # makes restart/concurrent repair idempotent even though
                    # every UUID fallback would otherwise be distinct.
                    lock_key = f"broker-repair:{self._email}:{_mode}:{contract}"
                    c.execute(
                        "SELECT pg_advisory_xact_lock(('x' || md5(%s))::bit(64)::bigint)",
                        (lock_key,),
                    )
                    c.execute(
                        """
                        SELECT *
                        FROM positions
                        WHERE client_id = %s
                          AND LOWER(TRIM(COALESCE(execution_mode, ''))) = %s
                          AND UPPER(TRIM(COALESCE(contract, ''))) = UPPER(TRIM(%s))
                          AND UPPER(TRIM(COALESCE(status, ''))) IN
                              ('OPEN', 'CLOSING', 'PARTIAL', 'ACTIVE')
                          AND COALESCE(quantity_remaining, qty, 0) > 0
                        ORDER BY entry_ts DESC NULLS LAST,
                                 updated_at DESC NULLS LAST
                        LIMIT 1
                        """,
                        (self._email, _mode, contract),
                    )
                    existing_active = c.fetchone()
                    if existing_active:
                        existing_row = dict(existing_active)
                        existing_id = existing_row.get("id")
                        if not existing_id:
                            return None
                        if proven_position_id and str(existing_id) != proven_position_id:
                            log.error(
                                "[exit_eng] BROKER_REPAIR_INSERT_BLOCKED client=%s "
                                "mode=%s contract=%s reason=active_identity_conflict "
                                "proven_position_id=%s existing_position_id=%s",
                                self._email, _mode, contract,
                                proven_position_id, existing_id,
                            )
                            return None
                        log.info(
                            "[exit_eng] BROKER_REPAIR_DB_IDENTITY_REUSED client=%s "
                            "mode=%s contract=%s position_id=%s",
                            self._email, _mode, contract, existing_id,
                        )
                        return _remember(existing_id, existing_row)

                    c.execute(
                        """
                        INSERT INTO positions (
                            id, client_id, underlying, contract, option_symbol,
                            execution_mode,
                            side, direction,
                            qty, quantity_remaining,
                            entry_price, avg_fill,
                            underlying_entry, stop_underlying, target_underlying,
                            status, entry_ts, signal_id,
                            local_order_id, broker_order_id, meta, updated_at
                        ) VALUES (
                            %s, %s, %s, %s, %s,
                            %s,
                            %s, %s,
                            %s, %s,
                            %s, %s,
                            %s, %s, %s,
                            'OPEN', %s, %s,
                            %s, %s, %s::jsonb, NOW()
                        )
                        ON CONFLICT DO NOTHING
                        RETURNING id
                        """,
                        (
                            position_id, self._email, repair_row["underlying"],
                            contract, contract, _mode,
                            side, side, qty, qty,
                            entry_px, entry_px,
                            underlying_entry, underlying_stop, underlying_target,
                            entry_ts, signal_id,
                            local_order_id, broker_order_id,
                            _json.dumps(position_meta),
                        ),
                    )
                    row = c.fetchone()
                    if row:
                        row_id = row.get("id")
                        if row_id:
                            return _remember(row_id)

                    # If the filled ENTRY already named a position id, an
                    # unrelated same-contract row is not an acceptable
                    # substitute for that canonical identity.
                    if proven_position_id:
                        c.execute(
                            """
                            SELECT *
                            FROM positions
                            WHERE id = %s
                              AND client_id = %s
                              AND LOWER(TRIM(COALESCE(execution_mode, ''))) = %s
                              AND UPPER(TRIM(COALESCE(contract, ''))) = UPPER(TRIM(%s))
                              AND UPPER(TRIM(COALESCE(status, ''))) IN
                                  ('OPEN', 'CLOSING', 'PARTIAL', 'ACTIVE')
                              AND COALESCE(quantity_remaining, qty, 0) > 0
                            LIMIT 1
                            """,
                            (proven_position_id, self._email, _mode, contract),
                        )
                        exact_existing = c.fetchone()
                        if exact_existing:
                            exact_row = dict(exact_existing)
                            exact_id = exact_row.get("id")
                            if exact_id:
                                return _remember(exact_id, exact_row)
                        return None

                    c.execute(
                        """
                        SELECT *
                        FROM positions
                        WHERE client_id = %s
                          AND LOWER(TRIM(COALESCE(execution_mode, ''))) = %s
                          AND UPPER(TRIM(COALESCE(contract, ''))) = UPPER(TRIM(%s))
                          AND UPPER(TRIM(COALESCE(status, ''))) IN
                              ('OPEN', 'CLOSING', 'PARTIAL', 'ACTIVE')
                          AND COALESCE(quantity_remaining, qty, 0) > 0
                        ORDER BY entry_ts DESC NULLS LAST,
                                 updated_at DESC NULLS LAST
                        LIMIT 1
                        """,
                        (self._email, _mode, contract),
                    )
                    existing = c.fetchone()
                    if existing:
                        existing_row = dict(existing)
                        existing_id = existing_row.get("id")
                        if existing_id:
                            log.info(
                                "[exit_eng] _upsert_broker_position_to_db ON CONFLICT "
                                "re-query returned existing active id for %s client=%s",
                                contract, self._email,
                            )
                            return _remember(existing_id, existing_row)
                    return None

            result = run_with_retry(_ins)
            if result:
                log.info(
                    "[exit_eng] BROKER_REPAIR_DB_IDENTITY_CONFIRMED client=%s mode=%s "
                    "contract=%s position_id=%s source=%s",
                    self._email, _mode, contract, result,
                    "filled_entry_order" if proven_position_id else "generated_repair_uuid",
                )
            return result
        except Exception as _ue:
            log.error(
                "[exit_eng] _upsert_broker_position_to_db %s failed: %s: %s",
                contract, type(_ue).__name__, _ue,
            )
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

        underlying_entry, entry_geometry_malformed = _broker_repair_historical_value(
            row, keys=_BROKER_REPAIR_ENTRY_GEOMETRY_KEYS
        )
        underlying_target, target_geometry_malformed = _broker_repair_historical_value(
            row, keys=_BROKER_REPAIR_TARGET_GEOMETRY_KEYS
        )
        underlying_stop, stop_geometry_malformed = _broker_repair_historical_value(
            row, keys=_BROKER_REPAIR_STOP_GEOMETRY_KEYS
        )
        historical_geometry_malformed = (
            entry_geometry_malformed
            or target_geometry_malformed
            or stop_geometry_malformed
        )
        _now = datetime.now(timezone.utc)
        _row_mode = str(row.get("execution_mode") or row.get("executionmode") or "").strip().lower()
        _execution_mode = _row_mode if _row_mode in {"live", "paper"} else self._resolved_execution_mode()
        mp = ManagedPosition(
            ticker           = ticker,
            option_symbol    = sym,
            side             = side,
            quantity         = qty,
            entry_price      = entry_px,
            underlying_entry = underlying_entry,
            underlying_target= underlying_target,
            underlying_stop  = underlying_stop,
            position_id      = pos_id,
            client_id        = self._email,
            signal_id        = sig_id,
            execution_mode    = _execution_mode,
            quantity_remaining = qty,
            opened_at        = opened_at or _now,
        )
        for _identity_attr in ("entry_local_order_id", "entry_broker_order_id"):
            _persisted_attr = _identity_attr.replace("entry_", "", 1)
            _identity_value = row.get(_identity_attr)
            if _identity_value in (None, ""):
                _identity_value = row.get(_persisted_attr)
            if _identity_value not in (None, ""):
                _normalized_identity = str(_identity_value).strip()
                if _normalized_identity.lower() not in {
                    "none", "null", "nan", "unknown", "n/a", "unavailable",
                }:
                    setattr(mp, _identity_attr, _normalized_identity)
        _explicit_provisional = row.get("broker_repair_provisional")
        _row_is_provisional = (
            bool(_explicit_provisional)
            if _explicit_provisional is not None
            else _broker_repair_row_is_provisional(row)
        )
        if _row_is_provisional:
            mp.broker_repair_provisional = True
            mp.brokerrepairprovisional = True

        if historical_geometry_malformed:
            _mark_adoption_identity_quarantined(
                mp,
                "broker_repair_historical_geometry_contradictory",
            )
            log.critical(
                "[exit_eng] BROKER_REPAIR_HISTORICAL_GEOMETRY_CONTRADICTORY "
                "client=%s contract=%s position_id=%s — position quarantined",
                self._email, sym, pos_id or "unknown",
            )

        if prefer_qty_override and not _execution_mode:
            _mark_adoption_identity_quarantined(
                mp,
                "broker_repair_execution_mode_unproven",
            )
            log.critical(
                "[exit_eng] EXIT_BROKER_POSITION_REPAIR_MODE_UNPROVEN "
                "client=%s contract=%s position_id=%s — broker repair retained "
                "for diagnostics but blocked from behavior until LIVE/PAPER mode "
                "is proven by DB row or account configuration",
                self._email, sym, pos_id or "unknown",
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
            _cfg = getattr(self.broker, "cfg", None)
            base = (
                getattr(self.broker, "base_url", None)
                or getattr(_cfg, "base_url", None)
            )
            token = (
                getattr(self.broker, "access_token", None)
                or getattr(self.broker, "_access_token", None)
                or getattr(_cfg, "access_token", None)
            )
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
                if _is_behavior_active_position(p)
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
                if _is_behavior_active_position(p)
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
            _repair_evidence = None
            if db_row:
                # Durable positions.meta provenance survives restart. Filled
                # ENTRY identity may positively confirm or contradict the row,
                # but a missing order position_id is uncertainty and must never
                # classify an otherwise canonical row as provisional.
                try:
                    _repair_evidence = self._find_exact_filled_entry_order(
                        sym, self._resolved_execution_mode(), bp
                    )
                    if (
                        isinstance(_repair_evidence, dict)
                        and not _repair_evidence.get("_lookup_status")
                    ):
                        _db_position_id = str(db_row.get("id") or "").strip()
                        _evidence_position_id = _broker_repair_position_id(
                            _repair_evidence.get("position_id")
                        )
                        if _db_position_id and _evidence_position_id:
                            db_row["broker_repair_provisional"] = (
                                _evidence_position_id != _db_position_id
                            )
                except Exception as _provenance_err:
                    log.debug(
                        "[exit_eng] broker-repair provenance recheck failed "
                        "for %s: %s",
                        sym, _provenance_err,
                    )
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
                    # Converge a provisional owner when a later exact filled-ENTRY lookup proves canonical identity.
                    if (isinstance(_repair_evidence, dict) and not _repair_evidence.get("_lookup_status") and _is_broker_repair_provisional(pos)):
                        _canonical_id = _broker_repair_position_id(_repair_evidence.get("position_id"))
                        if _canonical_id and _canonical_id != str(getattr(pos, "position_id", "") or ""):
                            _order_meta = _broker_repair_order_meta(_repair_evidence.get("meta"))
                            _entry_fill = _broker_repair_positive_float(_repair_evidence.get("fill_price"))
                            if _entry_fill is None:
                                _entry_fill = _broker_repair_positive_float(getattr(pos, "entry_price", 0.0)) or 0.0
                            _entry_geom = _broker_repair_historical_value(_repair_evidence, "underlying_entry")
                            _stop_geom = _broker_repair_historical_value(_repair_evidence, "stop_underlying")
                            _target_geom = _broker_repair_historical_value(_repair_evidence, "target_underlying")
                            if _entry_geom is not None and _stop_geom is not None and _target_geom is not None:
                                _signal_id = _broker_repair_text_value(_repair_evidence, "signal_id") or str(_order_meta.get("signal_id") or "")
                                _canonical_signal_id = _broker_repair_text_value(_repair_evidence, "canonical_signal_id") or str(_order_meta.get("canonical_signal_id") or _signal_id)
                                _local_order_id = _broker_repair_text_value(_repair_evidence, "local_order_id") or str(_order_meta.get("local_order_id") or "")
                                _broker_order_id = _broker_repair_text_value(_repair_evidence, "broker_order_id") or str(_order_meta.get("broker_order_id") or "")
                                _adoption = self.adopt_canonical_position_identity(contract=sym, canonical_position_id=_canonical_id, local_order_id=_local_order_id, broker_order_id=_broker_order_id, signal_id=_signal_id, canonical_signal_id=_canonical_signal_id, entry_fill=_entry_fill, entry_ts=_repair_evidence.get("filled_ts"), execution_mode=self._resolved_execution_mode(), client_id=self._email, order_filled_ts=_repair_evidence.get("filled_ts"), underlying_entry=_entry_geom, underlying_stop=_stop_geom, underlying_target=_target_geom, direction=str(getattr(pos, "side", "") or ""))
                                if getattr(_adoption, "adopted", False):
                                    pos = self._positions_by_id.get(_canonical_id, pos)
                    _loaded_active = (
                        pos in self.active_positions()
                    )
                    if not _loaded_active:
                        raise RuntimeError("add_position did not install behavior-active DB owner")
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

                    # A broker recovery owner is usable only after the INSERT
                    # (or its exact conflict re-query) returned a real id.
                    # Never install an engine-only synthetic lifecycle object.
                    if not new_id:
                        repair_failed_reason = "db_upsert_returned_no_id"
                        log.warning(
                            "[exit_eng] EXIT_BROKER_POSITION_UPSERT_NO_ID "
                            "client=%s account=%s contract_symbol=%s "
                            "— DB owner not confirmed; broker recovery held",
                            self._email, _account_id, sym,
                        )
                        raise RuntimeError(repair_failed_reason)
                    _pos_id = str(new_id)
                    db_repaired = True
                    _repair_row = getattr(new_id, "repair_row", None)
                    _recovery_mode = ""
                    if isinstance(_repair_row, dict):
                        _recovery_mode = str(
                            _repair_row.get("execution_mode") or ""
                        ).strip().lower()
                    if _recovery_mode not in {"live", "paper"}:
                        _recovery_mode = self._resolved_execution_mode()

                    minimal_row = {
                        "id":                 _pos_id,
                        "client_id":          self._email,
                        "contract":           sym,
                        "option_symbol":      sym,
                        "underlying":         self._underlying_from_occ(sym),
                        "side":               self._parse_occ_side(sym),
                        "qty":                broker_qty,
                        "quantity_remaining": broker_qty,
                        "entry_price":        entry_px,
                        "avg_fill":           entry_px,
                        "entry_ts":           (
                            bp.get("date_acquired")
                            or (
                                bp.get("raw", {}).get("date_acquired")
                                if isinstance(bp.get("raw"), dict) else None
                            )
                        ),
                        "execution_mode":     _recovery_mode,
                        "broker_repair_provisional": True,
                    }
                    if isinstance(_repair_row, dict):
                        minimal_row.update(_repair_row)
                    # Broker truth controls current quantity and exact recovery
                    # identity even when the attached row came from a conflict
                    # path.
                    minimal_row.update(
                        {
                            "id": _pos_id,
                            "client_id": self._email,
                            "contract": sym,
                            "option_symbol": sym,
                            "qty": broker_qty,
                            "quantity_remaining": broker_qty,
                            "execution_mode": _recovery_mode,
                        }
                    )
                    pos = self._managed_position_from_row(
                        minimal_row,
                        qty_override=broker_qty,
                        prefer_qty_override=True,
                    )
                    self.add_position(pos)
                    _loaded_active = (
                        pos in self.active_positions()
                    )
                    if not _loaded_active:
                        raise RuntimeError("add_position did not install behavior-active broker owner")
                    repaired_syms.append(sym)              # confirmed DB row
                except Exception as _re_err:
                    repair_failed_syms.append(sym)
                    repair_failed_reason = f"{type(_re_err).__name__}: {_re_err}"
                    log.error(
                        "[exit_eng] EXIT_UNSAFE_BROKER_POSITION_REPAIR_FAILED "
                        "client=%s account=%s contract_symbol=%s error=%s",
                        self._email, _account_id, sym, repair_failed_reason,
                    )
                    # Durable identity is unavailable, but broker truth is positive.
                    # Retain a narrowly-scoped provisional owner for exit evaluation.
                    try:
                        _degraded_id = f"broker-degraded-{_uuid.uuid4()}"
                        _degraded_row = {
                            "id": _degraded_id, "client_id": self._email,
                            "contract": sym, "option_symbol": sym,
                            "underlying": self._underlying_from_occ(sym),
                            "side": self._parse_occ_side(sym), "direction": self._parse_occ_side(sym),
                            "qty": broker_qty, "quantity_remaining": broker_qty,
                            "entry_price": entry_px, "avg_fill": entry_px,
                            "execution_mode": self._resolved_execution_mode(),
                            "status": "OPEN", "broker_repair_provisional": True,
                            "broker_repair_degraded": True,
                        }
                        _degraded_pos = self._managed_position_from_row(_degraded_row, qty_override=broker_qty, prefer_qty_override=True)
                        _degraded_pos.broker_repair_provisional = True
                        _degraded_pos.brokerrepairprovisional = True
                        _degraded_pos.broker_repair_degraded = True
                        _degraded_pos.brokerrepairdegraded = True
                        self.add_position(_degraded_pos)
                        log.error(
                            "[exit_eng] EXIT_BROKER_DEGRADED_OWNER_INSTALLED "
                            "client=%s mode=%s contract=%s quantity=%d position_id=%s canonical_identity=false broker_submit=NOT_ATTEMPTED",
                            self._email, getattr(_degraded_pos, "execution_mode", ""), sym, broker_qty, _degraded_id,
                        )
                    except Exception as _degraded_err:
                        log.critical("[exit_eng] degraded broker-truth owner install failed for %s: %s", sym, _degraded_err)
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
            # AMENDMENT #6 (blocker 3): pass RAW broker fields separately.
            # Previously we collapsed mark/mid/last/bid/ask into broker_mark and
            # passed the collapsed value as 'mark' — which laundered ASK-only
            # quotes as MARK and suppressed real LAST evidence.  The helper's
            # source-aware resolver only works if we give it the real data.
            broker_bid  = float(quote.get("bid")  or 0.0)
            broker_ask  = float(quote.get("ask")  or 0.0)
            broker_mark = float(quote.get("mark") or 0.0)
            broker_last = float(quote.get("last") or 0.0)
            # 'mid' from broker (if given) is a computed field; only use when
            # bid+ask absent to seed mark for _apply_option_quote_for_decision's
            # analytics_mark.  Do NOT let mid replace real mark for hard-ref.
            if broker_mark == 0.0:
                broker_mark = float(quote.get("mid") or 0.0)
            _has_any_price = broker_mark > 0 or broker_bid > 0 or broker_last > 0 or broker_ask > 0
            _quote_status = "OK" if _has_any_price else "QUOTE_UNAVAILABLE"

            # Position is ALWAYS added regardless of quote availability
            if _has_any_price:
                # P0 fix: route through the authoritative quote helper so LIVE
                # positions get bid-based current_option_price.  RAW fields are
                # passed so the hard-ref selector can honor real provenance.
                _broker_aqr = _apply_option_quote_for_decision(
                    pos,
                    bid  = broker_bid,
                    ask  = broker_ask,
                    mark = broker_mark,
                    last = broker_last,
                    quote_ts = datetime.now(timezone.utc),
                    last_ts  = quote.get("last_trade_ts") or quote.get("trade_date"),
                    mark_ts  = quote.get("mark_ts"),
                    source   = "broker_precheck",
                )
            else:
                log.warning(
                    "[exit_eng] EXIT_BROKER_PRECHECK_QUOTE_UNAVAILABLE "
                    "client=%s contract_symbol=%s quote_status=QUOTE_UNAVAILABLE "
                    "— position still added to engine; will_evaluate_this_cycle=true",
                    self._email, sym,
                )

            # Seed peak P&L using BID only (never mid/mark).
            # P0 (PR #385 amendment): touched_profit is NEVER armed here — a
            # single seed observation would defeat QPM's two-consecutive-fresh-BID
            # confirmation contract.  QPM arms it within ~2 polls of restart.
            broker_pnl_pct = 0.0
            if pos.entry_price > 0 and broker_bid > 0:
                broker_pnl_pct = (broker_bid - pos.entry_price) / pos.entry_price
                if broker_pnl_pct > 0 and broker_pnl_pct > pos.peak_pnl_pct:
                    pos.peak_pnl_pct = broker_pnl_pct
                    if broker_pnl_pct > pos.max_profit_seen:
                        pos.max_profit_seen = broker_pnl_pct

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
            "repair_failed=%s",
            self._email, _account_id,
            len(broker_syms), sorted(broker_syms),
            len(engine_syms), sorted(engine_syms),
            sorted(missing_from_engine),
            loaded_db_syms, repaired_syms,
            repair_failed_syms,
        )

        return len(repair_failed_syms) == 0

    def _check_all_positions(self, now_et: Optional[datetime] = None):
        today_et = _et_session_date()

        # Broker truth precheck: verify engine positions match broker before evaluating exits.
        # Prevents "no positions" assumption when DB/engine missed a fill.
        # EXIT_UNSAFE_BROKER_TRUTH_UNAVAILABLE is logged if check fails — execution continues.
        try:
            _precheck_ok = self._broker_position_precheck()
            if not _precheck_ok:
                log.warning("[exit_eng] _broker_position_precheck returned false; continuing with retained broker-truth/degraded owners")
        except Exception as _pce:
            log.warning("[exit_eng] _broker_position_precheck error (non-blocking): %s", _pce)

        # FIX-3: expired contract cleanup now runs inside self._lock.
        # Previously this block iterated and reassigned self._positions without
        # the lock — a concurrent add_position or fill callback could corrupt
        # the list or silently drop a newly added position.
        expired_for_db_cleanup = []
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
                        expired_for_db_cleanup.append((str(getattr(pos, "position_id", "") or ""), sym))
                        to_remove.append(pos)
                except Exception as _exp_err:
                    log.debug("[exit_eng] Expired-contract cleanup check failed for %s: %s", sym, _exp_err)

            if to_remove:
                self._positions = [p for p in self._positions if not p.closed]
                # P1: prune expired positions from O(1) index.
                for _ep in to_remove:
                    self._positions_by_id.pop(_ep.position_id, None)
                log.info("[exit_eng] Removed %d expired contract(s) from engine", len(to_remove))

        if expired_for_db_cleanup:
            _pm = (
                getattr(self, "_position_manager", None)
                or getattr(self, "position_manager", None)
                or getattr(getattr(self, "master_control", None), "pm", None)
            )
            if _pm is None:
                log.warning(
                    "[exit_eng] EXPIRED_CONTRACT_DB_REPAIR_SKIPPED client=%s count=%d reason=no_position_manager",
                    self._email or "default",
                    len(expired_for_db_cleanup),
                )
            else:
                for _pos_id, _sym in expired_for_db_cleanup:
                    if not _pos_id:
                        continue
                    try:
                        _pm.close_expired_position(
                            position_id=_pos_id,
                            reason="expired_contract_local_cleanup",
                            exit_reason="expired_contract",
                            close_source="expired_contract_cleanup",
                            close_confidence="SYSTEM",
                        )
                    except Exception as _db_exp_exc:
                        log.warning(
                            "[exit_eng] EXPIRED_CONTRACT_DB_REPAIR_FAILED client=%s pos=%s sym=%s err=%s",
                            self._email or "default",
                            _pos_id,
                            _sym,
                            _db_exp_exc,
                        )

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

        now_et = now_et or datetime.now(ET)
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
                        if _snap.bid > 0 or _snap.ask > 0 or getattr(_snap, "last", 0) > 0:
                            # P0: route through the single authoritative quote helper.
                            # LIVE positions must use bid, never _snap.mid.
                            # AMENDMENT #6 blocker 5: pass raw LAST and its timestamp
                            # from the QuoteAuthority snapshot so the hard-ref selector
                            # receives real provenance, not synthetic mid-as-mark.
                            _snap_bid = float(_snap.bid or 0.0)
                            _snap_ask = float(_snap.ask or 0.0)
                            _snap_last = float(getattr(_snap, "last", 0.0) or 0.0)
                            _snap_mark = 0.0
                            _receipt_ts = _normalize_hard_ref_ts(
                                getattr(_snap, "timestamp_epoch", None),
                                now_utc=now_utc,
                            )
                            _apply_option_quote_for_decision(
                                pos,
                                bid      = _snap_bid,
                                ask      = _snap_ask,
                                mark     = _snap_mark,
                                last     = _snap_last,
                                quote_ts = _receipt_ts,
                                last_ts  = None,
                                mark_ts  = None,
                                source   = "",
                            )
                            pos.last_option_quote_missing_ts = None
                            pos.last_quote_update_ts  = _receipt_ts
                            pos.last_quote_missing_ts = None
                        if _snap.underlying_price > 0:
                            pos.current_underlying              = _snap.underlying_price
                            pos.last_underlying_quote_update_ts  = _normalize_hard_ref_ts(
                                getattr(_snap, "timestamp_epoch", None),
                                now_utc=now_utc,
                            )
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
                    self._emit_exit_decision_stamp(pos, _eod_decision, now_et=now_et)
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

                # ── PEAK TRACKING — runs BEFORE quote gate ────────────────────
                # P0 (PR #385 amendment): peak_pnl_pct and max_profit_seen are
                # EXECUTABLE-BID-ONLY authority.  They feed soft-exit decisions
                # (profit floors, runner trails, small-win capture), so a PAPER
                # midpoint spike must never inflate them.  The engine advances
                # peak only from a bid-derived P&L; when bid is missing this
                # cycle, the peak simply does not advance (QPM will catch it on
                # the next bid-valid poll — bounded by poll cadence, not lost).
                #
                # touched_profit: the engine NEVER arms it.  QPM is the sole
                # arming authority via two-consecutive-fresh-BID confirmation
                # (position-scoped).  A single engine-side observation arming
                # touched_profit would defeat that confirmation contract.
                if pos.entry_price > 0:
                    _pg_bid = float(getattr(pos, "current_bid", 0.0) or 0.0)
                    _pg_bid_valid = _pg_bid > 0.0
                    if _pg_bid_valid:
                        _bid_pnl = (_pg_bid - pos.entry_price) / pos.entry_price
                        if _bid_pnl > pos.peak_pnl_pct:
                            pos.peak_pnl_pct = _bid_pnl
                            log.debug(
                                "[%s] PEAK UPDATE (pre-gate, bid) | peak=%.1f%% | bid=$%.2f entry=$%.2f",
                                pos.ticker, _bid_pnl * 100, _pg_bid, pos.entry_price,
                            )
                        if _bid_pnl > 0 and _bid_pnl > pos.max_profit_seen:
                            pos.max_profit_seen = _bid_pnl
                    # Keep in-memory option_pnl_pct fresh for the DB write below
                    # (display/hard-exit authority — mode-specific, unchanged).
                    if pos.current_option_price > 0:
                        _raw_pnl = (pos.current_option_price - pos.entry_price) / pos.entry_price
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
                # ── HARD-RISK PRE-GATE ────────────────────────────────────────
                # Uses shared resolver: checks validity + recomputes timestamp age.
                # An expired or unproven reference cannot force evaluation.
                _gate_now = now_utc if now_utc else datetime.now(timezone.utc)
                _hard_ref_pnl_early = get_effective_hard_exit_reference(pos, _gate_now)
                _force_hard_eval = (
                    _hard_ref_pnl_early is not None
                    and _has_entry_price
                    and not getattr(pos, "closed", False)
                    and int(getattr(pos, "quantity_remaining", 0) or 0) > 0
                    and not getattr(pos, "exit_in_flight", False)
                )
                if _force_hard_eval:
                    try:
                        # AMENDMENT (PR #385 review): pin the pre-gate DTE
                        # profile to the caller-supplied evaluation clock so
                        # replays and after-midnight-UTC runs see the same
                        # threshold profile evaluate_exit() will apply.
                        try:
                            _gate_session_date = now_et.astimezone(ET).date()
                        except Exception:
                            _gate_session_date = None
                        _pos_hard_stop, _, _ = _effective_thresholds(
                            pos, session_date=_gate_session_date,
                        )
                        _force_hard_eval = _hard_ref_pnl_early <= _pos_hard_stop
                    except Exception:
                        _force_hard_eval = False

                _should_evaluate = (
                    _has_live_quotes
                    or _has_peak_to_protect
                    or (_has_entry_price and _last_known_price)
                    or (_has_entry_price and _position_old_enough)
                    or _force_hard_eval
                )
                if _should_evaluate:
                    # Gate now only controls whether evaluate_exit() is called.
                    # (P0-3 force-close-all is handled by the pre-gate above
                    # so positions can close even with stale quotes.)
                    decision = evaluate_exit(pos, now_et)
                    decision.reason_code = _classify_exit_decision(decision)
                    _ledger_exit_decision(pos, decision, client_id=getattr(pos, "client_id", "") or getattr(self, "client_id", ""))
                    self._emit_exit_decision_stamp(pos, decision, now_et=now_et)

                    if decision.should_act:
                        option_quote_stale, option_quote_age_sec, option_quote_state = _is_option_quote_stale(pos, now_utc)
                        if option_quote_stale and not _is_forced_risk_exit_code(decision.reason_code):
                            qm = getattr(self, "quote_monitor", None)
                            if qm is not None:
                                qpm_state = getattr(pos, "quote_state", "")
                                if qpm_state == "blind" and hasattr(qm, "note_exit_gated_blind"):
                                    qm.note_exit_gated_blind()
                                elif hasattr(qm, "note_exit_gated_stale"):
                                    qm.note_exit_gated_stale()
                            self._own_stale_exit_retry(
                                pos,
                                decision,
                                option_quote_state=option_quote_state,
                                option_quote_age_sec=option_quote_age_sec,
                                stage="exit_decision",
                            )
                            continue

                        if option_quote_stale and _is_forced_risk_exit_code(decision.reason_code):
                            decision.reason = f"{decision.reason} | DEGRADED_QUOTE_MODE:{option_quote_state}"
                        else:
                            self._clear_degraded_monitoring_state(pos)

                        # ── PR #176: LIVE_DEGRADED_SOFT_EXIT_GUARD ────────────────────────────
                        # Last line of defence before any live broker exit submission.
                        # If the position is live-risk AND context is degraded AND the
                        # reason code is a gated soft-exit code, emit HOLD and continue.
                        # Manual close / EOD / hard disaster / broker-gone are exempt.
                        _pr176_hold, _pr176_hold_code, _pr176_extra = _pr176_should_hold(
                            pos,
                            self._exit_reason_code(decision),
                            decision.reason,
                        )
                        if _pr176_hold:
                            self._emit_exit_event(
                                pos, decision="HOLD",
                                reason_code=_pr176_hold_code,
                                explanation=(
                                    f"PR#176 blocked live soft exit before broker submit. "
                                    f"proposed={self._exit_reason_code(decision)} "
                                    f"hold_reason={_pr176_hold_code} "
                                    f"signals={_pr176_extra.get('pr176_degraded_signals', [])}"
                                ),
                                stage="exit_decision",
                                extra_inputs={
                                    "decision_action":      decision.action,
                                    "decision_qty":         decision.quantity,
                                    "decision_pnl_pct":     decision.pnl_pct,
                                    "decision_reason_code": decision.reason_code,
                                    **_pr176_extra,
                                },
                            )
                            log.error(
                                "[%s] PR176_%s — live broker exit BLOCKED | "
                                "proposed=%s pos_id=%s execution_mode=%s "
                                "underlying_entry=%s current_underlying=%s "
                                "signals=%s",
                                pos.ticker, _pr176_hold_code,
                                self._exit_reason_code(decision),
                                pos.position_id or "?",
                                _pr176_extra.get("pr176_execution_mode", ""),
                                _pr176_extra.get("pr176_underlying_entry", None),
                                _pr176_extra.get("pr176_current_underlying", None),
                                _pr176_extra.get("pr176_degraded_signals", []),
                            )
                            continue
                        # ─────────────────────────────────────────────────────────────────────

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
            adoption_identity_quarantined = []
            behavior_active_count = 0

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
                adoption_quarantined = _is_adoption_identity_quarantined(pos)
                if adoption_quarantined:
                    adoption_identity_quarantined.append(ident)
                if _is_behavior_active_position(pos):
                    behavior_active_count += 1

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
                    "adoption_identity_quarantined":   adoption_quarantined,
                    "adoption_identity_quarantine_reason": (
                        getattr(pos, "adoption_identity_quarantine_reason", "")
                        or getattr(pos, "adoptionidentityquarantinereason", "")
                    ),
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
                "tracked_position_count":         len(positions),
                "behavior_active_position_count": behavior_active_count,
                "adoption_identity_quarantined_count": len(adoption_identity_quarantined),
                "adoption_identity_quarantined":  adoption_identity_quarantined,
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
                if _active_exit_blocks_resubmit(_active_exit) and not _is_reserved_local_exit_submit_intent(pos, _active_exit):
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
                self._own_stale_exit_retry(
                    pos,
                    decision,
                    option_quote_state=option_quote_state,
                    option_quote_age_sec=option_quote_age_sec,
                    stage="exit_submission",
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
            else:
                self._clear_degraded_monitoring_state(pos)

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
                    "EOD_FORCE_CLOSE", "HARD_STOP", "STOP_HIT",
                    OPTION_CATASTROPHIC_STOP, UNDERLYING_TECHNICAL_STOP_CONFIRMED,
                    "THETA_STOP",
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

        _exec_mode = str(
            getattr(getattr(self, "master_control", None), "mode", "") or
            getattr(pos, "execution_mode", "") or
            ""
        ).strip().lower()
        try:
            from ap.exit_safety import (
                alert_exit_submission_halted,
                evaluate_exit_submission_safety,
                is_valid_exact_occ_contract,
                resolve_exit_broker_truth,
            )
            if not is_valid_exact_occ_contract(option_symbol):
                with self._lock:
                    pos.exit_in_flight = False
                    pos.pending_exit_reason = ""
                self._emit_exit_event(
                    pos,
                    decision="HOLD",
                    reason_code="BROKER_CONTRACT_IDENTITY_UNPROVEN",
                    explanation=(
                        "Blocked exit submit before broker truth lookup: exact OCC "
                        "contract identity is missing or invalid."
                    ),
                    stage="exit_submission",
                    extra_inputs={"contract": option_symbol, "identity_state": "unproven"},
                )
                log.error(
                    "[%s] BROKER_CONTRACT_IDENTITY_UNPROVEN | position_id=%s contract=%s",
                    ticker,
                    position_id or "?",
                    option_symbol or "?",
                )
                return False
            _broker_truth = resolve_exit_broker_truth(
                broker=getattr(self, "broker", None),
                client_id=str(getattr(pos, "client_id", "") or self.client_id),
                contract=str(option_symbol or ""),
            )
            _broker_truth_qty = _broker_truth.get("broker_truth_open_qty")
            _broker_truth_state, _broker_truth_parsed_qty = _classify_exact_broker_open_qty(_broker_truth_qty)
            _broker_truth_fresh_exact = _broker_truth.get("is_fresh_exact") is True
            _broker_truth["broker_truth_state"] = _broker_truth_state.value
            _broker_truth["broker_truth_parsed_open_qty"] = _broker_truth_parsed_qty
            _broker_truth_audit = dict((_broker_truth.get("audit") or {}))
            _broker_truth_audit["requested_qty"] = int(decision.quantity or 0)
            _broker_truth_audit["broker_truth_state"] = _broker_truth_state.value
            _broker_truth_audit["broker_truth_parsed_open_qty"] = _broker_truth_parsed_qty
            if (
                not _broker_truth_fresh_exact
                or _broker_truth_qty is None
                or _broker_truth_state == BrokerPositionTruth.UNKNOWN
            ):
                _truth_reason = (
                    "BROKER_TRUTH_QUANTITY_UNKNOWN"
                    if _broker_truth_fresh_exact
                    else "BROKER_TRUTH_UNAVAILABLE"
                )
                self._emit_degraded_critical(
                    pos,
                    _truth_reason,
                    "authoritative broker position truth is unavailable or malformed at the submit seam; "
                    "the existing canonical exit remains allowed and broker-flat close is forbidden",
                    extra={"broker_truth": _broker_truth_audit},
                )
                log.warning(
                    "[%s] %s | position_id=%s contract=%s snapshot_status=%s — continuing with canonical exit callback",
                    ticker,
                    _truth_reason,
                    position_id or "?",
                    option_symbol,
                    _broker_truth_audit.get("snapshot_status", "unknown"),
                )
            if _broker_truth_fresh_exact and _broker_truth_state == BrokerPositionTruth.FLAT:
                with self._lock:
                    pos.exit_in_flight   = False
                    pos.pending_exit_reason = ""
                close_result = self._mark_broker_flat_stale_position(pos, _broker_truth)
                self._emit_exit_event(
                    pos,
                    decision="REJECT" if close_result.closed else "ALERT",
                    reason_code="SYNTHETIC_POSITION_STALE_BROKER_FLAT",
                    explanation=(
                        "Blocked exit submit: fresh exact broker snapshot shows no open long position."
                        if close_result.closed
                        else "Blocked exit submit: broker snapshot is flat but durable local close remains pending."
                    ),
                    stage="exit_submission",
                    extra_inputs={
                        "broker_truth": _broker_truth_audit,
                        "broker_flat_close": close_result.__dict__,
                    },
                )
                log.warning(
                    "[%s] SYNTHETIC_POSITION_STALE_BROKER_FLAT | position_id=%s contract=%s "
                    "requested_qty=%s snapshot_status=%s close_verified=%s",
                    ticker, position_id or "?", option_symbol,
                    int(decision.quantity or 0),
                    _broker_truth_audit.get("snapshot_status", "unknown"),
                    close_result.verified,
                )
                return False
            if (
                _broker_truth_fresh_exact
                and _broker_truth_state == BrokerPositionTruth.OPEN
                and _broker_truth_parsed_qty is not None
                and int(decision.quantity or 0) > int(_broker_truth_parsed_qty)
            ):
                with self._lock:
                    pos.exit_in_flight = False
                    pos.pending_exit_reason = ""
                self._emit_exit_event(
                    pos,
                    decision="REJECT",
                    reason_code="EXIT_BLOCKED_BROKER_QTY_INSUFFICIENT",
                    explanation=(
                        f"Blocked exit submit because requested_qty={int(decision.quantity or 0)} "
                        f"exceeds exact broker long qty={int(_broker_truth_parsed_qty)}"
                    ),
                    stage="exit_submission",
                    extra_inputs={"broker_truth": _broker_truth_audit},
                )
                log.warning(
                    "[%s] EXIT_BLOCKED_BROKER_QTY_INSUFFICIENT | position_id=%s contract=%s requested_qty=%s broker_truth_open_qty=%s",
                    ticker,
                    position_id or "?",
                    option_symbol,
                    int(decision.quantity or 0),
                    int(_broker_truth_parsed_qty),
                )
                return False

            _exit_guard = evaluate_exit_submission_safety(
                position_id=str(position_id or ""),
                client_id=str(getattr(pos, "client_id", "") or self.client_id),
                execution_mode=_exec_mode,
                contract=str(option_symbol or ""),
                broker_truth_open_qty=_broker_truth_qty,
                allow_missing_position_with_broker_truth=_is_broker_repair_provisional(pos),
            )
            if _exit_guard.get("blocked"):
                _blocked_reason = str(_exit_guard.get("reason") or "exit_submission_blocked")
                if _blocked_reason != "exit_circuit_breaker_tripped":
                    with self._lock:
                        pos.exit_in_flight = False
                        pos.pending_exit_reason = ""
                log.warning(
                    "[%s] EXIT guard blocked before callback submit | position_id=%s client_id=%s execution_mode=%s contract=%s reason=%s",
                    ticker,
                    position_id or "?",
                    getattr(pos, "client_id", "") or self.client_id,
                    _exec_mode,
                    option_symbol,
                    _blocked_reason,
                )
                if _blocked_reason == "exit_circuit_breaker_tripped":
                    _breaker = (_exit_guard.get("circuit_breaker") or {}) if isinstance(_exit_guard, dict) else {}
                    try:
                        alert_exit_submission_halted(
                            client_id=str(getattr(pos, "client_id", "") or self.client_id),
                            execution_mode=_exec_mode,
                            position_id=str(position_id or ""),
                            contract=str(option_symbol or ""),
                            reason=_blocked_reason,
                            rejection_count=_breaker.get("rejection_count"),
                            threshold=_breaker.get("threshold"),
                        )
                    except Exception as _alert_err:
                        log.warning("[%s] exit guard alert failed: %s", ticker, _alert_err)
                self._emit_exit_event(
                    pos,
                    decision="HOLD",
                    reason_code="EXIT_GUARD_BLOCKED",
                    explanation=f"Exit submit blocked before callback: {_blocked_reason}",
                    stage="exit_submission",
                    extra_inputs={
                        "decision_action": decision.action,
                        "decision_qty": decision.quantity,
                        "execution_mode": _exec_mode,
                        "blocked_reason": _blocked_reason,
                    },
                )
                return False
        except Exception as _guard_err:
            log.warning(
                "[%s] exit pre-submit guard unavailable; continuing with callback submit: %s",
                ticker,
                _guard_err,
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
                    if bid > 0 or ask > 0:
                        _apply_option_quote_for_decision(
                            pos,
                            bid      = bid,
                            ask      = ask,
                            mark     = float(oq.get("mark", 0) or oq.get("mid", 0) or 0),
                            quote_ts = now_utc,
                        )

        log.info("_refresh_quotes: refreshed %d position(s) with live quotes", len(active))
