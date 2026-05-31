# ap_entry_watcher.py — Angel Precision Real-Time Entry Watcher
# =============================================================================
# OSM / queue / execution-core compatible watcher.
#
# Responsibilities:
#   1) Hold pending plan-based entry signals until the underlying breaches trigger.
#   2) Confirm breach momentum over multiple polls before firing on_trigger(watched).
#   3) Hold post-session and pre-market setups overnight, then revalidate at open.
#   4) Protect OSM from duplicate, conflicting, stale, or chaotic open-trigger events.
#
# Non-responsibilities:
#   - This file does NOT submit broker orders.
#   - This file does NOT transition OSM order states.
#   - ExecutionCore must wire on_trigger -> _on_entry_trigger -> OSM.submit_existing_entry(...).
#
# Production notes:
#   - Based on the newer OSM-integrated watcher, with production-safe pieces kept
#     from the older simpler watcher: debug visibility, explicit watcher ref for
#     dedup cleanup, clearer comments, and safer fallback behavior.
#   - Daily overnight signals use ap.overnight_daily_validator when available.
#   - If the validator import is unavailable, the watcher still imports/runs, but
#     daily-specific validation is disabled and a warning is emitted.
# =============================================================================

from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional
from zoneinfo import ZoneInfo

# ── Lifecycle + health wiring (defensive — watcher runs standalone if missing) ──
try:
    from ap_lifecycle import (
        LEDGER as _EW_LEDGER,
        SignalState as _EW_SS,
        LifecycleOwner as _EW_LO,
        signal_watching,
        signal_invalidated,
        signal_cancelled,
        signal_expired,
        signal_triggered,
    )
    _EW_LIFECYCLE_OK = True
except Exception:
    _EW_LIFECYCLE_OK = False

def _ew_record(signal_id: str, ticker: str, to_state_name: str, reason: str, **meta) -> None:
    """Safe lifecycle wrapper — never raises, never blocks watcher logic."""
    if not _EW_LIFECYCLE_OK:
        return
    try:
        fn_map = {
            "WATCHING":     signal_watching,
            "INVALIDATED":  signal_invalidated,
            "CANCELLED":    signal_cancelled,
            "EXPIRED":      signal_expired,
            "TRIGGER_READY": signal_triggered,
        }
        fn = fn_map.get(to_state_name)
        if fn:
            fn(signal_id, ticker, _EW_LO.WATCHER, reason, **meta)
    except Exception:
        pass

log = logging.getLogger("ap.entry_watcher")

# Module-level ET zoneinfo: declared BEFORE any helper that uses it.
ET = ZoneInfo("America/New_York")

# FUNNEL FIX (2026-05-20, hardened 2026-05-21) + PR-C / BUG-EW-1:
# Pre-open helper for the stop-touch invalidation guard. Returns True from
# midnight ET through 9:30 ET (pre-market) AND through the 5-min open-protect
# window (9:30–9:35 ET) — spreads remain wide during the open protect window,
# transient bid-below-stop ticks should not invalidate overnight/daily setups.
#
# PR-C / BUG-EW-1: removed lazy `from datetime import` and
# `from zoneinfo import` calls inside the function body. They added a
# sys.modules lookup on every poll tick (this helper is called for
# every overnight/daily signal on every 15s cycle) AND constructed a
# new ZoneInfo("America/New_York") object every call, shadowing the
# module-level ET constant. Now we use the module-level ET directly.
def _is_pre_market_now() -> bool:
    try:
        et = datetime.now(ET)
        # Pre-9:30 → pre-market
        if et.hour < 9 or (et.hour == 9 and et.minute < 30):
            return True
        # 9:30–9:35 → open-protect window: still wide spreads, treat as pre-open
        if et.hour == 9 and 30 <= et.minute < 35:
            return True
        return False
    except Exception:
        return False

POLL_INTERVAL_SEC = 15
MAX_WATCH_MINUTES = 4320  # 72 hours — covers weekend holds

# PR-C / Section 1 documentation note:
# EOD_CUTOFF (15:30 ET) intentionally fires BEFORE the exit engine's
# EOD_HARD_CLOSE (15:50 ET) in ap_exit_engine.py. This 20-minute gap
# ensures no new entries are opened while the exit engine is in its
# final forced-close window. Do not align these times — the gap is a
# deliberate safety margin.
EOD_CUTOFF_HOUR = 15
EOD_CUTOFF_MIN = 30
WRONG_DIR_BUFFER_PCT = 0.001
OVERNIGHT_THRESHOLD_HOUR = 15
OVERNIGHT_THRESHOLD_MIN = 30

MAX_INTRADAY_WATCH_MIN = int(os.getenv("MAX_INTRADAY_WATCH_MIN", "45"))
# Default 45 min: watcher stays armed for 45 minutes after breach.
# Was hardcoded 5 — too short for slow-moving setups. Env-tunable.

# PR-C: MAX_INTRADAY_DRIFT_PCT is now env-tunable for consistency with
# every other arm/watch threshold. Default 0.015 (1.5%) preserved.
MAX_INTRADAY_DRIFT_PCT = float(os.getenv("MAX_INTRADAY_DRIFT_PCT", "0.015"))

# PR-C: MAX_OPTION_PREMIUM_DRIFT_PCT elevated from inline os.getenv()
# in watch() to a module-level constant. Default 0.25 (25%) preserved.
# If the option bid is already this far above the signal's reference
# entry_option_price at arm time, the move has likely happened without us
# and entering would be catching the reversal. Module-level so the dep
# is visible to static analysis and the read isn't repeated per arm.
MAX_OPTION_PREMIUM_DRIFT_PCT = float(os.getenv("MAX_OPTION_PREMIUM_DRIFT_PCT", "0.25"))

# ============================================================
# FUNNEL FIX (2026-05-20, hardened 2026-05-21) — dedicated arm-time tolerance.
#
# Production observed 12/29 orders today rejected with
# 'watch_arm_failed:stale_price_-0.X%_from_trigger' at deltas of 0.0–0.4%.
# Two root causes possible:
#   (a) MAX_INTRADAY_DRIFT_PCT is hard-coded 1.5% — NOT env tunable.
#   (b) The 'stop guard' below conflates 'price below stop' with stale, and the
#       reject reason string only captures pct_from_trigger — making it look
#       like a drift reject even when it's actually a too-close stop reject.
#
# Hardened logic:
#   - WATCH_ARM_STALE_TOLERANCE_PCT (default 0.010 = 1.0%) is env-tunable.
#   - Env actually controls the threshold (replaces the previous max() floor
#     which prevented the env from ever loosening below 1.5%).
#   - Safety bounds: minimum 0.001 (0.1%), maximum 0.05 (5%) — protects
#     against fat-finger env values that would either kill all signals (too
#     tight) or admit garbage (too loose).
#   - Reject reason now distinguishes 'arm_drift_X%' from 'arm_below_stop'
#     so we can tell which gate fired in post-mortem analysis.
# ============================================================
try:
    _raw_tol = float(os.getenv("WATCH_ARM_STALE_TOLERANCE_PCT", "0.010"))
    # Safety bounds — env can loosen but cannot become absurd.
    WATCH_ARM_STALE_TOLERANCE_PCT = max(0.001, min(0.05, _raw_tol))
    if _raw_tol != WATCH_ARM_STALE_TOLERANCE_PCT:
        # Will be visible at startup so operator knows env was clamped.
        _clamp_warn = True
    else:
        _clamp_warn = False
except (TypeError, ValueError):
    WATCH_ARM_STALE_TOLERANCE_PCT = 0.010
    _clamp_warn = False

# Effective threshold used at arm-time. The env value IS the threshold.
# (Previously this was max(env, MAX_INTRADAY_DRIFT_PCT) which prevented env
# from loosening below 1.5% — defeating the purpose of the env knob.)
WATCH_ARM_EFFECTIVE_THRESHOLD_PCT = WATCH_ARM_STALE_TOLERANCE_PCT

if _clamp_warn:
    log.warning(
        "[entry-watcher] WATCH_ARM_STALE_TOLERANCE_PCT was clamped to safety "
        "bounds [0.001, 0.05]; effective=%.4f",
        WATCH_ARM_EFFECTIVE_THRESHOLD_PCT,
    )
log.info(
    "[entry-watcher] thresholds loaded: "
    "MAX_INTRADAY_DRIFT_PCT=%.4f WATCH_ARM_STALE_TOLERANCE_PCT=%.4f effective=%.4f",
    MAX_INTRADAY_DRIFT_PCT,
    WATCH_ARM_STALE_TOLERANCE_PCT,
    WATCH_ARM_EFFECTIVE_THRESHOLD_PCT,
)
OVERNIGHT_MAX_DRIFT_PCT = 0.020  # generic/non-daily overnight drift guard

OPEN_PROTECT_MINUTES = 5
MAX_OPEN_TRIGGERS = 1

# ── P0-W2: Strong-signal re-arm after temporary wrong-side-of-stop ───────────
#
# When arm is rejected because price is temporarily on the wrong side of stop
# (below_stop gate, NOT drift_stale), high-conviction signals enter a
# DISARMED_WAITING_FOR_RECLAIM hold instead of being permanently killed.
# If price reclaims the valid side within WATCHER_REARM_WINDOW_SEC, the signal
# arms normally with the same signal_id/plan_id. If it does not, it expires.
#
# Only applies to: score >= WATCHER_REARM_MIN_SCORE OR tier A, AND
# (if WATCHER_REARM_ONLY_DAILY_OR_OVERNIGHT) the signal must be daily/overnight.
# Drift-stale rejects are always permanent — those represent a decisive miss.
#
# WATCHER_REARM_TOLERANCE_PCT: the underlying must clear past the stop level
# by this fraction before re-arming. Prevents oscillation at the stop line.
# Default 0.001 = 0.1% of stop price. For NVDA stop=212.71, reclaim fires when
# mid <= 212.71 * 0.999 = 212.497 (PUT) or mid >= stop * 1.001 (CALL).
#
# WATCHER_REARM_MAX_ATTEMPTS: after this many rearm cycles (disarm → reclaim),
# the next disarm is permanent. Default 1 — allows one recovery per signal.
#
# Overnight rearm expiry is market-open-aware: if the signal enters rearm mode
# pre-market, the window starts at 9:30 ET open, not at arm time, so the signal
# is not silently expired before quotes are even available.
# ─────────────────────────────────────────────────────────────────────────────
WATCHER_REARM_ENABLED: bool = (
    os.getenv("WATCHER_REARM_ENABLED", "0").strip().lower() not in {"0", "false", "no"}
)
WATCHER_REARM_MIN_SCORE: float = float(os.getenv("WATCHER_REARM_MIN_SCORE", "75"))
_raw_rearm_window = int(os.getenv("WATCHER_REARM_WINDOW_SEC", "600"))
WATCHER_REARM_WINDOW_SEC: int = max(60, min(3600, _raw_rearm_window))  # clamp 1 min – 60 min
WATCHER_REARM_ONLY_DAILY_OR_OVERNIGHT: bool = (
    os.getenv("WATCHER_REARM_ONLY_DAILY_OR_OVERNIGHT", "1").strip().lower() not in {"0", "false", "no"}
)
_raw_rearm_tol = float(os.getenv("WATCHER_REARM_TOLERANCE_PCT", "0.001"))
WATCHER_REARM_TOLERANCE_PCT: float = max(0.0, min(0.02, _raw_rearm_tol))  # clamp 0 – 2%
WATCHER_REARM_MAX_ATTEMPTS: int = max(1, int(os.getenv("WATCHER_REARM_MAX_ATTEMPTS", "1")))

# Opposite-side conflict smart-eligibility (P0 PR):
# Friday's audit: 419/871 (~48%) of canceled entries were
# watcher_block:opposite_side_conflict. Many were against stale or already-
# canceled opposite watchers. New rule set below; env-overridable.
#
# Decision matrix:
#   1. If opposite watcher is STALE (older than OPPOSITE_CONFLICT_MAX_AGE_SEC)
#      AND has lower-or-equal score, ignore it. New signal admits.
#   2. If opposite watcher score > new score AND fresh, BLOCK new (current
#      behavior preserved — protect the better setup).
#   3. If new score > opposite score, FLIP (cancel opposite, admit new —
#      current behavior preserved).
#   4. Equal scores within OPPOSITE_CONFLICT_SCORE_TIE_PCT of each other,
#      prefer the higher-tier timeframe (1d/4h beats 1h/15m). Daily beats
#      intraday on tie.
#   5. New signal can override if it is non-rearm/active + opposite is rearm-only
#      (rearm signals are weaker by design).
OPPOSITE_CONFLICT_MAX_AGE_SEC      = int(os.getenv("OPPOSITE_CONFLICT_MAX_AGE_SEC", "600"))   # 10 min default
OPPOSITE_CONFLICT_SCORE_TIE_PCT    = float(os.getenv("OPPOSITE_CONFLICT_SCORE_TIE_PCT", "0.03"))  # within 3pts = tie

# Timeframe tier preference (higher wins on ties). Daily/4h/overnight
# structurally stronger than intraday for failed-directional setups.
_OPPOSITE_TF_TIER = {
    "1d": 5, "d": 5, "daily": 5,
    "4h": 4, "240m": 4, "4hour": 4,
    "1h": 3, "60m": 3, "hourly": 3,
    "30m": 2,
    "15m": 1, "5m": 1, "1m": 1,
}

def _opposite_tf_rank(tf: str) -> int:
    return _OPPOSITE_TF_TIER.get(str(tf or "").lower().strip(), 0)

log.info(
    "[entry-watcher] rearm config: enabled=%s min_score=%.0f window=%ds "
    "daily_only=%s tolerance=%.4f max_attempts=%d",
    WATCHER_REARM_ENABLED,
    WATCHER_REARM_MIN_SCORE,
    WATCHER_REARM_WINDOW_SEC,
    WATCHER_REARM_ONLY_DAILY_OR_OVERNIGHT,
    WATCHER_REARM_TOLERANCE_PCT,
    WATCHER_REARM_MAX_ATTEMPTS,
)


# ── Optional daily overnight validator integration ───────────────────────────
try:
    from ap.overnight_daily_validator import (  # type: ignore
        OvernightWatchState,
        _is_daily_signal as _validator_is_daily_signal,
        recheck_overnight_daily as _validator_recheck_overnight_daily,
    )
    _DAILY_VALIDATOR_AVAILABLE = True
except Exception as _import_err:  # pragma: no cover - defensive import fallback
    _DAILY_VALIDATOR_AVAILABLE = False
    log.warning(
        "ap.overnight_daily_validator unavailable; daily overnight signals will "
        "use generic overnight validation only: %s",
        _import_err,
    )

    class OvernightWatchState:  # type: ignore[no-redef]
        OVERNIGHT_QUEUED = "OVERNIGHT_QUEUED"
        OPEN_RECHECK_PENDING = "OPEN_RECHECK_PENDING"
        VALID_AWAITING_BREACH = "VALID_AWAITING_BREACH"
        INVALIDATED = "INVALIDATED"

    def _validator_is_daily_signal(_obj) -> bool:
        return False

    def _validator_recheck_overnight_daily(_obj, _broker):
        class _Result:
            valid = True
            reason_code = "VALIDATOR_UNAVAILABLE"
            reason_text = "Daily validator unavailable; generic watcher fallback used."

        return _Result()


def _safe_is_daily_signal(obj) -> bool:
    """Safely identify daily signals whether validator expects object or dict."""
    try:
        return bool(_validator_is_daily_signal(obj))
    except Exception:
        try:
            return bool(_validator_is_daily_signal(getattr(obj, "signal", obj)))
        except Exception as exc:
            log.debug("daily-signal classification failed; treating as non-daily: %s", exc)
            return False


class WatchState:
    PENDING = "PENDING"
    TRIGGERED = "TRIGGERED"
    EXPIRED = "EXPIRED"
    INVALIDATED = "INVALIDATED"
    CANCELLED = "CANCELLED"


class WatchedSignal:
    MOMENTUM_POLLS_REQUIRED = 2

    def __init__(self, signal: dict, overnight: bool = False):
        self.signal = signal
        self.ticker = str(signal.get("ticker") or "").upper().strip()
        self.side = str(signal.get("side") or "CALL").upper().strip()
        self.overnight = bool(overnight)

        if not self.ticker:
            raise ValueError("WatchedSignal requires ticker")
        if self.side not in {"CALL", "PUT"}:
            raise ValueError(f"[{self.ticker}] invalid side={self.side!r}; expected CALL or PUT")

        # Normalize payload so downstream execution core sees clean fields.
        self.signal["ticker"] = self.ticker
        self.signal["side"] = self.side

        trigger = signal.get("trigger") or {}
        entry = signal.get("entry_price") or trigger.get("entry")
        stop = signal.get("stop_price") or trigger.get("stop")
        target = signal.get("target_price") or trigger.get("pt1") or trigger.get("pt2")

        # PR-C / BUG-EW-2: explicit None-or-empty check on entry.
        # Previously \`float(entry or 0) or None\` silently coerced a literal
        # 0.0 to None, leaving the subsequent guard to raise ValueError
        # anyway. The pattern was confusing and conflated "missing" with
        # "zero-valued" — raise loudly for either case.
        def _coerce_or_none(v):
            if v is None or v == "" or v == 0 or v == 0.0:
                return None
            try:
                f = float(v)
            except (TypeError, ValueError):
                return None
            return f if f != 0.0 else None

        self.entry_trigger = _coerce_or_none(entry)
        self.stop_level    = _coerce_or_none(stop)
        self.target_price  = _coerce_or_none(target)

        if not self.entry_trigger:
            raise ValueError(
                f"[{self.ticker}] entry_price/trigger is None or zero — "
                f"signal payload incomplete: entry={entry} stop={stop} target={target}"
            )

        self.score = float(signal.get("score") or 0)
        self.grade = signal.get("grade", "B")
        self.signal_id = str(signal.get("signal_id") or uuid.uuid4())
        self.signal["signal_id"] = self.signal_id

        self.state = WatchState.PENDING
        self.created_at = datetime.now(timezone.utc)
        self.triggered_at: Optional[datetime] = None
        self.trigger_price: Optional[float] = None
        self.expire_at = self.created_at + timedelta(minutes=MAX_WATCH_MINUTES)

        self.breach_count = 0
        self.breach_price = 0.0
        self.last_quote_bid = 0.0
        self.last_quote_ask = 0.0
        self._watcher_ref = None
        self._pending_audit: Optional[dict] = None  # audit payload staged inside check(), persisted by poll loop

        # P0-W2: re-arm state — set by add_signal() when watch() marks the signal rearm-eligible.
        self.rearm_mode: bool = False                   # True while waiting for price reclaim
        self.rearm_expires_at: Optional[datetime] = None  # deadline for reclaim (None = no window set)
        self.rearm_reason: str = ""                     # original arm_below_stop raw_reason
        self.rearm_count: int = 0                       # how many disarm→reclaim cycles completed

        if self.overnight and _safe_is_daily_signal(self):
            self.signal["queue_status"] = OvernightWatchState.OVERNIGHT_QUEUED

        if self.overnight:
            log.info(
                "[%s] OVERNIGHT signal queued | %s | trigger=$%s | stop=$%s | "
                "target=$%s | activates next session open",
                self.ticker,
                self.side,
                self.entry_trigger,
                self.stop_level,
                self.target_price,
            )
        else:
            log.info(
                "[%s] Watching %s | trigger=$%s | stop=$%s | target=$%s | expires %s",
                self.ticker,
                self.side,
                self.entry_trigger,
                self.stop_level,
                self.target_price,
                self.expire_at.strftime("%H:%M UTC"),
            )

    @property
    def is_active(self) -> bool:
        # rearm_mode signals are PENDING but NOT active — they are waiting for
        # price to reclaim the valid side of stop and must not enter normal
        # breach/trigger/stale-drift poll logic until reclaimed.
        return self.state == WatchState.PENDING and not self.rearm_mode

    @property
    def minutes_watching(self) -> float:
        return (datetime.now(timezone.utc) - self.created_at).total_seconds() / 60.0

    def _release_dedup_key(self) -> None:
        try:
            watcher = getattr(self, "_watcher_ref", None)
            dedup_set = getattr(watcher, "_dedup_set", None) if watcher else None
            if dedup_set is not None and self.signal_id:
                dedup_set.discard(str(self.signal_id))
                log.debug("[%s] Dedup key released | signal=%s", self.ticker, self.signal_id)
        except Exception:
            pass

    def check(self, bid: float, ask: float) -> str:
        # PR-C precedence note: when ask >= trigger AND bid <= stop on the
        # SAME poll tick, the breach check runs FIRST (may set
        # state=TRIGGERED), then the stop check runs and OVERWRITES with
        # state=INVALIDATED. This is "last-write wins" and the chosen
        # behavior is safer-by-design: a single tick where the underlying
        # is whipsawing both directions should NOT fire an entry. Do not
        # add an early-return after TRIGGERED — the current precedence is
        # intentional. See tests/test_entry_watcher_audit.py
        # TestPrecedenceTriggerVsStop for the structural guarantee.
        now = datetime.now(timezone.utc)
        self.last_quote_bid = bid
        self.last_quote_ask = ask

        if now >= self.expire_at:
            self.state = WatchState.EXPIRED
            self._release_dedup_key()
            log.info("[%s] EXPIRED — no breach in %smin", self.ticker, MAX_WATCH_MINUTES)
            return self.state

        # Intraday stale-move invalidation. Daily overnight signals get their
        # own structural validator, not generic drift logic.
        if (
            not self.overnight
            and not _safe_is_daily_signal(self)
            and self.minutes_watching >= MAX_INTRADAY_WATCH_MIN
        ):
            # PR-C / BUG-EW-3: explicit zero-quote guard BEFORE drift
            # calculation. Previously a quote outage (bid=0, ask=0)
            # produced drift = (0 - trigger) / trigger = -1.0, which for
            # a PUT signal was below the negative threshold and silently
            # expired the signal. Now: if both bid and ask are zero we
            # never enter the drift branch — we wait for a real quote.
            if bid <= 0 and ask <= 0:
                mid = 0.0  # explicit: no quote available, skip drift check
            else:
                mid = (bid + ask) / 2 if bid > 0 and ask > 0 else max(bid, ask)
            if mid > 0 and self.entry_trigger:
                drift = (mid - self.entry_trigger) / self.entry_trigger
                stale = (self.side == "CALL" and drift > MAX_INTRADAY_DRIFT_PCT) or (
                    self.side == "PUT" and drift < -MAX_INTRADAY_DRIFT_PCT
                )
                if stale:
                    self.state = WatchState.EXPIRED
                    self._release_dedup_key()
                    log.info(
                        "[%s] STALE ENTRY — watching %.1fmin, price drifted %.2f%% "
                        "from trigger $%.2f. Move missed — expiring.",
                        self.ticker,
                        self.minutes_watching,
                        drift * 100.0,
                        self.entry_trigger,
                    )
                    return self.state

        if self.side == "CALL":
            if ask >= self.entry_trigger:
                if self.breach_count == 0:
                    self.breach_price = ask
                    log.debug(
                        "[%s] CALL breach candidate — ask=$%.2f >= trigger=$%.2f",
                        self.ticker,
                        ask,
                        self.entry_trigger,
                    )
                self.breach_count += 1
                if self.breach_count >= self.MOMENTUM_POLLS_REQUIRED:
                    self.state = WatchState.TRIGGERED
                    self.triggered_at = now
                    self.trigger_price = ask
                    log.info(
                        "[%s] CALL CONFIRMED — ask=$%.2f held above $%.2f for %d polls",
                        self.ticker,
                        ask,
                        self.entry_trigger,
                        self.breach_count,
                    )
            else:
                if self.breach_count > 0:
                    log.debug("[%s] CALL breach reset — ask=$%.2f pulled back", self.ticker, ask)
                self.breach_count = 0

            # FUNNEL FIX (2026-05-20): for overnight + daily setups, do NOT
            # invalidate on a pre-market stop touch. Pre-open spreads are wide
            # and thinly-traded extended-hours quotes can spike below stop
            # transiently without representing a real thesis break.
            # The validated overnight revalidation at 9:30 ET (which runs the
            # full structural daily validator) will catch genuine breaks.
            _pre_open_skip = (
                (self.overnight or _safe_is_daily_signal(self))
                and _is_pre_market_now()
            )
            if (
                self.stop_level
                and bid <= self.stop_level * (1 - WRONG_DIR_BUFFER_PCT)
                and not _pre_open_skip
            ):
                _call_stop_mid = (bid + ask) / 2.0 if (bid and ask) else max(bid, ask)
                _call_wref = getattr(self, "_watcher_ref", None)
                if _call_wref is not None:
                    self._pending_audit = _call_wref._build_watcher_audit_payload(
                        self,
                        trigger_type="intraday_check",
                        current_bid=bid,
                        current_ask=ask,
                        current_mid=_call_stop_mid,
                        arm_condition=f"trigger_{self.entry_trigger:.4f}",
                        stop_condition=f"bid_{bid:.4f}_le_call_stop_{self.stop_level:.4f}",
                        reason_code="stop_bid_below_call_stop",
                        raw_reason=f"bid_{bid:.4f}_broke_call_stop_{self.stop_level:.4f}",
                        extra={
                            "overnight": self.overnight,
                            "is_daily": _safe_is_daily_signal(self),
                            "pre_open_skip": _pre_open_skip,
                        },
                    )
                self.state = WatchState.INVALIDATED
                self.breach_count = 0
                self._release_dedup_key()
                log.info(
                    "[%s] INVALIDATED — bid=$%.2f broke stop=$%.2f before trigger",
                    self.ticker,
                    bid,
                    self.stop_level,
                )
            elif _pre_open_skip and self.stop_level and bid <= self.stop_level * (1 - WRONG_DIR_BUFFER_PCT):
                log.debug(
                    "[%s] pre-open stop touch ignored for daily/overnight setup "
                    "(bid=$%.2f stop=$%.2f) — will revalidate at 9:30 ET",
                    self.ticker, bid, self.stop_level,
                )

        else:  # PUT
            if bid <= self.entry_trigger:
                if self.breach_count == 0:
                    self.breach_price = bid
                    log.debug(
                        "[%s] PUT breach candidate — bid=$%.2f <= trigger=$%.2f",
                        self.ticker,
                        bid,
                        self.entry_trigger,
                    )
                self.breach_count += 1
                if self.breach_count >= self.MOMENTUM_POLLS_REQUIRED:
                    self.state = WatchState.TRIGGERED
                    self.triggered_at = now
                    self.trigger_price = bid
                    log.info(
                        "[%s] PUT CONFIRMED — bid=$%.2f held below $%.2f for %d polls",
                        self.ticker,
                        bid,
                        self.entry_trigger,
                        self.breach_count,
                    )
            else:
                if self.breach_count > 0:
                    log.debug("[%s] PUT breach reset — bid=$%.2f pulled back", self.ticker, bid)
                self.breach_count = 0

            # FUNNEL FIX (2026-05-20): same pre-open guard for PUT setups.
            _pre_open_skip = (
                (self.overnight or _safe_is_daily_signal(self))
                and _is_pre_market_now()
            )
            if (
                self.stop_level
                and ask >= self.stop_level * (1 + WRONG_DIR_BUFFER_PCT)
                and not _pre_open_skip
            ):
                _put_stop_mid = (bid + ask) / 2.0 if (bid and ask) else max(bid, ask)
                _put_wref = getattr(self, "_watcher_ref", None)
                if _put_wref is not None:
                    self._pending_audit = _put_wref._build_watcher_audit_payload(
                        self,
                        trigger_type="intraday_check",
                        current_bid=bid,
                        current_ask=ask,
                        current_mid=_put_stop_mid,
                        arm_condition=f"trigger_{self.entry_trigger:.4f}",
                        stop_condition=f"ask_{ask:.4f}_ge_put_stop_{self.stop_level:.4f}",
                        reason_code="stop_ask_above_put_stop",
                        raw_reason=f"ask_{ask:.4f}_broke_put_stop_{self.stop_level:.4f}",
                        extra={
                            "overnight": self.overnight,
                            "is_daily": _safe_is_daily_signal(self),
                            "pre_open_skip": _pre_open_skip,
                        },
                    )
                self.state = WatchState.INVALIDATED
                self.breach_count = 0
                self._release_dedup_key()
                log.info(
                    "[%s] INVALIDATED — ask=$%.2f broke stop=$%.2f before trigger",
                    self.ticker,
                    ask,
                    self.stop_level,
                )

        return self.state


class APEntryWatcher:
    """Background watcher for queue-created entry plans."""

    def __init__(self, broker, order_state_machine=None,
                 require_on_trigger: Optional[bool] = None,
                 mode: str = "PAPER"):
        self.broker = broker
        self.order_state_machine = order_state_machine
        if require_on_trigger is None:
            require_on_trigger = os.getenv("AP_WATCHER_REQUIRE_ON_TRIGGER", "1").strip().lower() not in {"0", "false", "no"}
        self.require_on_trigger = bool(require_on_trigger)

        # PR-C / BUG-EW-5: canonical mode wired from APExecutionCore at
        # construct time. Previously self.mode was never set anywhere,
        # so getattr(self, "mode", "PAPER") in _revalidate_overnight_at_open
        # always returned "PAPER" — silently arming live overnight setups
        # on pre-market quote outages instead of invalidating them. Default
        # is PAPER for back-compat with any callers that omit the kwarg.
        self.mode = (mode or "PAPER").upper()
        self._pending: list[WatchedSignal] = []
        self._lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None

        self.on_trigger: Optional[Callable] = None
        self.on_expire: Optional[Callable] = None
        self.on_invalidate: Optional[Callable] = None

        self._open_trigger_count = 0
        self._open_protect_date = None
        self._open_trigger_tickers: set = set()   # per-ticker open protection

        # Real watcher-level duplicate barrier. Cleanup alone is not enough;
        # the key must be initialized and enforced before a signal is armed.
        self._dedup_set: set[str] = set()

    def _dedup_key_for_signal(self, signal: dict) -> str:
        return str(signal.get("signal_id") or "").strip()

    def _validate_local_order_id(self, local_order_id: Optional[str]) -> bool:
        """Best-effort OSM pre-arm validation.

        If an OSM instance is supplied, prove the queue-created local order exists
        before arming. If no OSM is supplied, stay backward compatible and let
        ExecutionCore perform breach-time recovery.
        """
        if not local_order_id:
            return True
        osm = getattr(self, "order_state_machine", None)
        if osm is None:
            return True

        probes = ("has_order", "exists", "contains", "get_order", "get", "get_order_by_local_id")
        for name in probes:
            fn = getattr(osm, name, None)
            if not callable(fn):
                continue
            try:
                result = fn(local_order_id)
                if isinstance(result, bool):
                    return result
                if result is not None:
                    return True
            except TypeError:
                continue
            except Exception as exc:
                log.warning("OSM local_order_id validation failed via %s(%s): %s", name, local_order_id, exc)
                return False

        for attr in ("orders", "_orders", "pending_orders", "_pending_orders", "local_orders", "_local_orders"):
            obj = getattr(osm, attr, None)
            try:
                if isinstance(obj, dict):
                    return local_order_id in obj
                if obj is not None and local_order_id in obj:
                    return True
            except Exception:
                continue

        log.warning(
            "[WATCHER_VALIDATION_FALLBACK] OSM object supplied but no recognized "
            "local-order lookup contract exists; allowing watcher arm for "
            "local_order_id=%s and relying on ExecutionCore recovery. "
            "A misconfigured or mock OSM will silently validate every signal. "
            "Set AP_WATCHER_STRICT_OSM_VALIDATION=1 to make this path reject instead.",
            local_order_id,
        )
        return True

    # ── Watcher Audit Helpers ────────────────────────────────────────────────
    # _build_watcher_audit_payload: pure dict construction — safe to call inside
    #   any lock or poll tick. No I/O.
    # _persist_watcher_audit: best-effort OSM meta merge. Never raises.
    #   Logs with persisted=false when local_order_id is absent or order not found.

    def _watcher_quote_identity(self) -> dict:
        """Return the ACTUAL quote source/base_url/sandbox flag the watcher uses.

        QUOTE-DOMAIN AUDIT: _fetch_quotes() reads from self.broker.session with
        self.broker(.cfg).base_url. We surface that real base_url here — never
        assume — so watcher_audit rows prove which Tradier environment the
        watcher evaluated against (sandbox = delayed paper, live = current).
        Keys are namespaced watcher_* so they don't collide with the order's
        selector_*/submit_* quote evidence.

        POINT 3 (review): the fallback chain MUST be byte-identical to
        _fetch_quotes — same getattr order, same final default of
        'https://sandbox.tradier.com'. If they diverged, the audit could label
        a quote 'unknown' while _fetch_quotes actually queried sandbox, which
        would mislead the whole investigation. The default IS sandbox because
        that is exactly what _fetch_quotes hits when base_url can't be read.
        """
        base_url = (
            getattr(self.broker, "base_url", None)
            or getattr(getattr(self.broker, "cfg", None), "base_url", None)
            or "https://sandbox.tradier.com"
        )
        base_url = str(base_url)
        sandbox = "sandbox" in base_url.lower()
        # base_url is now never empty (matches _fetch_quotes default), so
        # source is always sandbox or live — never 'unknown' for the watcher,
        # because _fetch_quotes would never silently query an unknown host.
        source = "tradier_sandbox" if sandbox else "tradier_live"
        return {
            "watcher_quote_source":   source,
            "watcher_quote_base_url": base_url,
            "watcher_sandbox_mode":   bool(sandbox),
        }

    def _build_watcher_audit_payload(
        self,
        w=None,
        *,
        symbol: str = "",
        score: float = 0.0,
        tier: str = "",
        direction: str = "",
        timeframe: str = "",
        pattern: str = "",
        signal_id: str = "",
        plan_id: str = "",
        trigger_type: str = "",
        signal_entry_price: Optional[float] = None,
        trigger_price: Optional[float] = None,
        stop_price: Optional[float] = None,
        current_bid: float = 0.0,
        current_ask: float = 0.0,
        current_mid: float = 0.0,
        quote_age_ms: Optional[int] = None,
        arm_price: Optional[float] = None,
        arm_condition: str = "",
        stop_condition: str = "",
        reason_code: str = "",
        raw_reason: str = "",
        extra: Optional[dict] = None,
    ) -> dict:
        """Build a structured watcher_audit dict for any block/expire/invalidate path.
        If a WatchedSignal (w) is provided, missing keyword fields are pulled from it.
        Explicit keyword args always override w-derived values.
        Pure dict construction — no I/O, safe to call inside any lock."""
        if w is not None:
            _sig = getattr(w, "signal", {}) or {}
            symbol      = symbol      or getattr(w, "ticker", "")
            score       = score       or float(getattr(w, "score", 0) or 0)
            tier        = tier        or str(getattr(w, "grade", "") or "")
            direction   = direction   or getattr(w, "side", "")
            timeframe   = timeframe   or str(_sig.get("timeframe") or "")
            pattern     = pattern     or str(_sig.get("pattern") or "")
            signal_id   = signal_id   or str(getattr(w, "signal_id", "") or "")
            plan_id     = plan_id     or str(_sig.get("plan_id") or "")
            if signal_entry_price is None:
                signal_entry_price = getattr(w, "entry_trigger", None)
            if stop_price is None:
                stop_price = getattr(w, "stop_level", None)
            if not current_bid and not current_ask:
                current_bid = float(getattr(w, "last_quote_bid", 0) or 0)
                current_ask = float(getattr(w, "last_quote_ask", 0) or 0)

        if not current_mid and (current_bid or current_ask):
            current_mid = (
                (current_bid + current_ask) / 2.0
                if (current_bid and current_ask)
                else max(current_bid, current_ask)
            )

        entry_ref = signal_entry_price if signal_entry_price is not None else trigger_price
        dist_trigger_pct: Optional[float] = None
        if entry_ref and current_mid:
            try:
                dist_trigger_pct = round((current_mid - entry_ref) / entry_ref * 100.0, 4)
            except ZeroDivisionError:
                pass
        dist_stop_pct: Optional[float] = None
        if stop_price and current_mid:
            try:
                dist_stop_pct = round((current_mid - stop_price) / stop_price * 100.0, 4)
            except ZeroDivisionError:
                pass

        payload: dict = {
            "symbol":              symbol,
            "score":               score,
            "tier":                tier,
            "direction":           direction,
            "timeframe":           timeframe,
            "pattern":             pattern,
            "signal_id":           signal_id,
            "plan_id":             plan_id,
            "trigger_type":        trigger_type,
            "signal_entry_price":  signal_entry_price,
            "trigger_price":       trigger_price if trigger_price is not None else signal_entry_price,
            "stop_price":          stop_price,
            "current_underlying":  current_mid,
            "current_bid":         current_bid,
            "current_ask":         current_ask,
            "current_mid":         current_mid,
            "quote_age_ms":        quote_age_ms,
            "arm_price":           arm_price,
            "arm_condition":       arm_condition,
            "stop_condition":      stop_condition,
            "distance_to_trigger_pct": dist_trigger_pct,
            "distance_to_stop_pct":    dist_stop_pct,
            "reason_code":         reason_code,
            "raw_reason":          raw_reason,
            "evaluated_at":        datetime.now(timezone.utc).isoformat(),
            # QUOTE-DOMAIN AUDIT: record the ACTUAL quote source the watcher
            # used (read off self.broker, never assumed). This lets us prove
            # whether the watcher evaluated against sandbox or live quotes —
            # the core question behind the paper no-fill investigation.
            **self._watcher_quote_identity(),
        }
        if extra:
            # Never allow extra to overwrite protected order fields
            _protected = {"retry_status", "retry_payload"}
            for k, v in extra.items():
                if k not in _protected:
                    payload[k] = v
        return payload

    def _insert_watcher_audit_row(
        self, payload: dict, local_order_id: Optional[str] = None
    ) -> None:
        """Best-effort INSERT into public.watcher_audit for analytics queries.

        Provides a queryable row per watcher event alongside the JSONB snapshot
        in orders.meta.  If the table does not exist yet (migration pending) or
        ap.db is unavailable (test / CI), the insert fails silently.

        Key contract: uses the snake_case key names that _build_watcher_audit_payload
        emits — signal_id, trigger_price, reason_code, etc.  Never swaps to
        camelCase or no-separator forms.
        """
        import json as _json_local  # local import keeps watcher importable without ap.db
        try:
            from ap.db import conn as _ap_conn, run_with_retry as _ap_retry  # type: ignore
        except ImportError:
            return

        # Rearm config snapshot at evaluation time
        _rearm_enabled  = WATCHER_REARM_ENABLED
        _rearm_daily    = WATCHER_REARM_ONLY_DAILY_OR_OVERNIGHT
        _rearm_window   = WATCHER_REARM_WINDOW_SEC
        _rearm_min_sc   = WATCHER_REARM_MIN_SCORE
        _rearm_tol      = WATCHER_REARM_TOLERANCE_PCT
        _rearm_max      = WATCHER_REARM_MAX_ATTEMPTS

        # Eligibility breakdown — use payload fields, not method call, to avoid
        # re-evaluating against current env if env changed mid-session.
        _score       = float(payload.get("score") or 0)
        _tier        = str(payload.get("tier") or "").upper()
        _score_ok    = _score >= _rearm_min_sc
        _tier_ok     = _tier == "A"
        # is_rearm_eligible is already in payload for arm_time_rearm_queued events;
        # fall back to evaluating it for other trigger types.
        _eligible    = bool(payload.get("rearm_eligible") or (
            _rearm_enabled and (_score_ok or _tier_ok)
        ))

        # Lifecycle boolean derivation from reason_code
        _reason      = str(payload.get("reason_code") or "")
        _rearmed     = _reason == "rearm_reclaimed"
        _expired     = _reason in {
            "rearm_window_expired",
            "rearm_max_attempts_expired",
            "overnight_too_far_from_trigger",
            "overnight_premarket_breached",
            "overnight_live_quote_unavailable",
        }
        _perm_reject = _reason in {
            "arm_drift",
            "arm_below_stop",          # only when not rearm-eligible
            "osm_validation_failed",
            "dedup_block",
            "opposite_side_conflict",
            "same_side_block",
        } and not _eligible

        # Fetch client_id from OSM if available — not in payload
        _client_id = getattr(
            getattr(self, "order_state_machine", None), "client_id", None
        )

        _sql = """
            INSERT INTO public.watcher_audit (
                local_order_id, signal_id, client_id,
                symbol, underlying_price,
                score, tier, direction, timeframe, pattern,
                trigger_price, stop_price,
                current_bid, current_ask, current_mid,
                arm_price,
                distance_to_trigger_pct, distance_to_stop_pct,
                trigger_type, reason_code, raw_reason,
                arm_condition, stop_condition,
                rearm_enabled, rearm_only_daily_or_overnight,
                rearm_window_sec, rearm_min_score,
                rearm_tolerance_pct, rearm_max_attempts,
                score_ok, tier_ok, is_rearm_eligible,
                rearmed, expired, permanently_rejected,
                full_payload
            ) VALUES (
                %s, %s, %s,
                %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s::jsonb
            )
        """
        _params = (
            local_order_id or None,
            payload.get("signal_id") or None,
            _client_id,
            # instrument
            payload.get("symbol"),
            payload.get("current_underlying") or payload.get("current_mid"),
            # signal quality
            _score,
            payload.get("tier"),
            payload.get("direction"),
            payload.get("timeframe"),
            payload.get("pattern"),
            # prices
            payload.get("trigger_price") or payload.get("signal_entry_price"),
            payload.get("stop_price"),
            payload.get("current_bid"),
            payload.get("current_ask"),
            payload.get("current_mid"),
            payload.get("arm_price"),
            payload.get("distance_to_trigger_pct"),
            payload.get("distance_to_stop_pct"),
            # watcher decision
            payload.get("trigger_type"),
            payload.get("reason_code"),
            payload.get("raw_reason"),
            payload.get("arm_condition"),
            payload.get("stop_condition"),
            # rearm config snapshot
            _rearm_enabled,
            _rearm_daily,
            _rearm_window,
            _rearm_min_sc,
            _rearm_tol,
            _rearm_max,
            # eligibility
            _score_ok,
            _tier_ok,
            _eligible,
            # lifecycle outcome
            _rearmed,
            _expired,
            _perm_reject,
            # full payload JSONB — evaluated_at uses column DEFAULT NOW()
            _json_local.dumps(payload, default=str),
        )

        try:
            def _write():
                with _ap_conn() as _c:
                    _cur = _c.execute(_sql, _params)
                    return getattr(_cur, "rowcount", getattr(_c, "rowcount", None))
            _ap_retry(_write)
        except Exception as _exc:
            log.warning("[watcher_audit_table] insert failed (non-critical): %s", _exc)

    def _persist_watcher_audit(
        self, local_order_id: Optional[str], payload: dict
    ) -> None:
        """Best-effort merge of watcher_audit into orders.meta.
        Logs with persisted=false when local_order_id is absent or order row not found.
        Does NOT overwrite retry_status, retry_payload, or any existing order meta key
        other than watcher_audit / watcher_audit_history.
        Never raises — audit must never disrupt watcher flow."""
        try:
            # Always attempt a table row — works even without local_order_id.
            # Fails silently if the migration hasn't run yet or ap.db is unavailable.
            self._insert_watcher_audit_row(payload, local_order_id=local_order_id)

            if not local_order_id:
                log.info(
                    "[watcher_audit] no local_order_id — orders.meta skipped | reason=%s | %s",
                    payload.get("reason_code"),
                    json.dumps({**payload, "persisted": False}, default=str),
                )
                return

            osm = getattr(self, "order_state_machine", None)
            if osm is None:
                log.info(
                    "[watcher_audit] no OSM — orders.meta skipped | local_order_id=%s | reason=%s | %s",
                    local_order_id,
                    payload.get("reason_code"),
                    json.dumps({**payload, "persisted": False}, default=str),
                )
                return

            # Retrieve order — try every plausible OSM accessor
            order = None
            for _mname in ("get_order", "get_order_by_local_id", "get", "get_by_local_id"):
                _fn = getattr(osm, _mname, None)
                if callable(_fn):
                    try:
                        order = _fn(local_order_id)
                        if order is not None:
                            break
                    except Exception:
                        continue

            if order is None:
                log.info(
                    "[watcher_audit] order not found in OSM — orders.meta skipped | "
                    "local_order_id=%s | reason=%s",
                    local_order_id,
                    payload.get("reason_code"),
                )
                return

            # Build the patch — ONLY watcher_audit and watcher_audit_history.
            # Never read the full existing meta and pass it back into DB.
            # That pattern risks overwriting concurrent fields (retry_status,
            # retry_payload, submit_refresh evidence) with a stale snapshot.
            if isinstance(order, dict):
                _existing_meta = order.get("meta") or {}
            else:
                _existing_meta = getattr(order, "meta", None) or {}
            _existing_meta = _existing_meta if isinstance(_existing_meta, dict) else {}
            _history = list(_existing_meta.get("watcher_audit_history") or [])
            _history.append(payload)
            _patch = {
                "watcher_audit":         payload,
                "watcher_audit_history": _history[-5:],
            }

            # Try every plausible OSM meta-update method (patch dict only)
            _persisted = False
            for _mname in ("update_order_meta", "patch_meta", "set_meta", "update_meta"):
                _fn = getattr(osm, _mname, None)
                if callable(_fn):
                    try:
                        _result = _fn(local_order_id, _patch)
                        # update_order_meta returns bool; older shims may return None
                        _persisted = bool(_result) if _result is not None else True
                        break
                    except Exception as _me:
                        log.debug(
                            "[watcher_audit] %s(%s) failed: %s", _mname, local_order_id, _me
                        )

            # Direct SQL fallback — for deployments where the OSM predates
            # update_order_meta.  Conditional import keeps ap_entry_watcher
            # importable in test / CI environments without ap.db.
            # Writes only _patch (not full meta). Uses COALESCE so NULL meta
            # rows are handled safely.  Rowcount is read from the cursor, not
            # from a fallback that would turn 0 into 1.
            if not _persisted:
                try:
                    from ap.db import conn as _ap_conn, run_with_retry as _ap_retry  # type: ignore
                    _client_id = getattr(osm, "client_id", "")
                    _patch_json = json.dumps(_patch, default=str)
                    if _client_id:
                        _fb_sql = (
                            "UPDATE orders "
                            "SET meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb, "
                            "    updated_ts = NOW() "
                            "WHERE local_order_id = %s AND client_id = %s"
                        )
                        _fb_params = (_patch_json, local_order_id, _client_id)
                    else:
                        _fb_sql = (
                            "UPDATE orders "
                            "SET meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb, "
                            "    updated_ts = NOW() "
                            "WHERE local_order_id = %s"
                        )
                        _fb_params = (_patch_json, local_order_id)

                    def _update_fn():
                        with _ap_conn() as _c:
                            _cur = _c.execute(_fb_sql, _fb_params)
                            return getattr(_cur, "rowcount", getattr(_c, "rowcount", None))

                    _rowcount = _ap_retry(_update_fn)
                    _persisted = bool(_rowcount and _rowcount > 0)
                except ImportError:
                    pass  # ap.db not available (test env / CI) — fall through to log
                except Exception as _sql_exc:
                    log.debug("[watcher_audit] direct SQL fallback failed: %s", _sql_exc)

            if _persisted:
                log.debug(
                    "[watcher_audit] persisted | local_order_id=%s reason=%s",
                    local_order_id,
                    payload.get("reason_code"),
                )
            else:
                log.info(
                    "[watcher_audit] OSM meta update not available — logging payload | "
                    "local_order_id=%s | %s",
                    local_order_id,
                    json.dumps({**payload, "persisted": False}, default=str),
                )
        except Exception as _exc:
            log.warning("[watcher_audit] persist failed (non-critical): %s", _exc)

    # ── P0-W2: Re-arm eligibility + reclaim poll ─────────────────────────────

    def _is_rearm_eligible(
        self,
        score: float,
        tier: str,
        is_overnight_or_daily: bool,
    ) -> bool:
        """Return True when a wrong-side-of-stop arm failure should enter DISARMED
        wait mode instead of permanently rejecting the signal.

        Eligibility requires ALL of:
          - WATCHER_REARM_ENABLED=1 (env default: off — must be explicitly enabled)
          - score >= WATCHER_REARM_MIN_SCORE OR tier is 'A'
          - if WATCHER_REARM_ONLY_DAILY_OR_OVERNIGHT: signal must be overnight or daily

        Drift-stale rejects are NEVER eligible — those mean price has moved
        decisively away from trigger, not temporarily beyond the stop.
        """
        if not WATCHER_REARM_ENABLED:
            return False
        score_ok = score >= WATCHER_REARM_MIN_SCORE
        tier_ok = str(tier or "").upper() == "A"
        if not (score_ok or tier_ok):
            return False
        if WATCHER_REARM_ONLY_DAILY_OR_OVERNIGHT and not is_overnight_or_daily:
            return False
        return True

    def _check_rearm_signals(self) -> None:
        """Poll all DISARMED_WAITING_FOR_RECLAIM signals. Called from _check_all()
        during market hours (after the pre-market gate, so never before 9:30 ET).

        For each rearm signal:
          - Timeout elapsed → expire, fire on_expire, clean from pending.
          - Price reclaimed valid side + tolerance → flip rearm_mode=False so normal
            poll picks it up next cycle. Log + audit the reclaim.
          - Otherwise → log debug, continue waiting.

        All state changes are written to watcher_audit via _persist_watcher_audit."""
        with self._lock:
            rearm_signals = [
                w for w in self._pending
                if getattr(w, "rearm_mode", False) and w.state == WatchState.PENDING
            ]
        if not rearm_signals:
            return

        tickers = list({w.ticker for w in rearm_signals})
        try:
            quotes = self._fetch_quotes(tickers)
        except Exception as exc:
            log.warning("[rearm] Quote fetch failed: %s", exc)
            return

        now = datetime.now(timezone.utc)
        to_expire: list = []
        to_rearm: list = []

        for w in rearm_signals:
            # ── Timeout check ──────────────────────────────────────────────
            if w.rearm_expires_at and now >= w.rearm_expires_at:
                to_expire.append(w)
                continue

            quote = quotes.get(w.ticker)
            if not quote:
                continue

            bid = float(quote.get("bid", 0) or 0)
            ask = float(quote.get("ask", 0) or 0)
            if bid == 0 and ask == 0:
                last = float(quote.get("last", 0) or 0)
                bid = ask = last
            mid = (bid + ask) / 2.0 if (bid and ask) else max(bid, ask)
            if mid <= 0:
                continue

            # ── Direction-aware reclaim check ──────────────────────────────
            # CALL: was disarmed because mid < call_stop (dropped below support).
            #   Reclaim: mid rises back above stop + tolerance buffer.
            # PUT:  was disarmed because mid > put_stop (rallied above resistance).
            #   Reclaim: mid falls back below stop - tolerance buffer.
            reclaimed = False
            reclaim_threshold = 0.0
            if w.stop_level:
                if w.side == "CALL":
                    reclaim_threshold = w.stop_level * (1.0 + WATCHER_REARM_TOLERANCE_PCT)
                    reclaimed = mid >= reclaim_threshold
                else:  # PUT
                    reclaim_threshold = w.stop_level * (1.0 - WATCHER_REARM_TOLERANCE_PCT)
                    reclaimed = mid <= reclaim_threshold

            if reclaimed:
                to_rearm.append((w, bid, ask, mid, reclaim_threshold))
            else:
                remaining = (
                    max(0.0, (w.rearm_expires_at - now).total_seconds())
                    if w.rearm_expires_at else 0.0
                )
                log.debug(
                    "[%s] REARM_WAIT | side=%s mid=%.4f stop=%.4f "
                    "reclaim_threshold=%.4f window_remaining=%.0fs",
                    w.ticker, w.side, mid,
                    w.stop_level or 0.0,
                    reclaim_threshold,
                    remaining,
                )

        # ── Process expirations — state change inside lock, I/O outside ───
        expired_signals: list = []
        with self._lock:
            for w in to_expire:
                if getattr(w, "rearm_mode", False) and w.state == WatchState.PENDING:
                    w.rearm_mode = False
                    w.state = WatchState.EXPIRED
                    w._release_dedup_key()
                    expired_signals.append(w)

        for w in expired_signals:
            _exp_audit = self._build_watcher_audit_payload(
                w,
                trigger_type="rearm_check",
                reason_code="rearm_window_expired",
                raw_reason=(
                    f"rearm_window_{WATCHER_REARM_WINDOW_SEC}s_elapsed_no_reclaim"
                    f"_side_{w.side}_stop_{w.stop_level:.4f}"
                    if w.stop_level else f"rearm_window_{WATCHER_REARM_WINDOW_SEC}s_elapsed"
                ),
                extra={
                    "rearm_count":       w.rearm_count,
                    "rearm_reason":      w.rearm_reason,
                    "rearm_window_sec":  WATCHER_REARM_WINDOW_SEC,
                },
            )
            self._persist_watcher_audit(w.signal.get("local_order_id"), _exp_audit)
            _sig_id = str(w.signal.get("signal_id", ""))
            if _sig_id and w.ticker:
                _ew_record(_sig_id, w.ticker, "EXPIRED", "rearm_window_expired")
            log.info(
                "[%s] REARM_EXPIRED — no reclaim within %ds | side=%s stop=%s | expiring",
                w.ticker, WATCHER_REARM_WINDOW_SEC, w.side,
                f"{w.stop_level:.4f}" if w.stop_level else "none",
            )
            if self.on_expire:
                try:
                    self.on_expire(w)
                except Exception as _exc:
                    log.error(
                        "[%s] on_expire failed during rearm expiry: %s", w.ticker, _exc, exc_info=True
                    )

        if expired_signals:
            with self._lock:
                _exp_ids = {id(w) for w in expired_signals}
                self._pending = [w for w in self._pending if id(w) not in _exp_ids]

        # ── Process reclaims — flip inside lock, audit+log outside ────────
        rearmed_signals: list = []
        _max_attempt_expired: list = []  # signals that hit limit during reclaim evaluation
        with self._lock:
            for w, bid, ask, mid, reclaim_threshold in to_rearm:
                if getattr(w, "rearm_mode", False) and w.state == WatchState.PENDING:
                    # Guard: if already at max rearm attempts, expire instead of re-arming
                    if w.rearm_count >= WATCHER_REARM_MAX_ATTEMPTS:
                        w.rearm_mode = False
                        w.state = WatchState.EXPIRED
                        w._release_dedup_key()
                        _max_attempt_expired.append(w)   # separate list — cleanup runs below
                        log.info(
                            "[%s] REARM_MAX_ATTEMPTS reached (%d) — expiring on reclaim",
                            w.ticker, WATCHER_REARM_MAX_ATTEMPTS,
                        )
                        continue
                    w.rearm_mode = False
                    w.rearm_count += 1
                    w.last_quote_bid = bid
                    w.last_quote_ask = ask
                    rearmed_signals.append((w, bid, ask, mid, reclaim_threshold))

        # Max-attempts expiry: callbacks + pending cleanup.
        # Must run after the lock is released; separate from the timeout expiry path
        # because expired_signals cleanup already ran above.
        for w in _max_attempt_expired:
            _max_audit = self._build_watcher_audit_payload(
                w,
                trigger_type="rearm_check",
                reason_code="rearm_max_attempts_expired",
                raw_reason=(
                    f"rearm_count_{w.rearm_count}_reached_max_{WATCHER_REARM_MAX_ATTEMPTS}"
                    f"_on_reclaim_side_{w.side}"
                ),
                extra={
                    "rearm_count":        w.rearm_count,
                    "rearm_max_attempts": WATCHER_REARM_MAX_ATTEMPTS,
                    "rearm_reason":       w.rearm_reason,
                },
            )
            self._persist_watcher_audit(w.signal.get("local_order_id"), _max_audit)
            _sig_id = str(w.signal.get("signal_id", ""))
            if _sig_id and w.ticker:
                _ew_record(_sig_id, w.ticker, "EXPIRED", "rearm_max_attempts_expired")
            log.info(
                "[%s] REARM_MAX_ATTEMPTS_EXPIRED | side=%s stop=%s | removing from pending",
                w.ticker, w.side,
                f"{w.stop_level:.4f}" if w.stop_level else "none",
            )
            if self.on_expire:
                try:
                    self.on_expire(w)
                except Exception as _exc:
                    log.error(
                        "[%s] on_expire failed on max-attempts expiry: %s",
                        w.ticker, _exc, exc_info=True,
                    )

        if _max_attempt_expired:
            with self._lock:
                _max_ids = {id(w) for w in _max_attempt_expired}
                self._pending = [w for w in self._pending if id(w) not in _max_ids]

        for w, bid, ask, mid, reclaim_threshold in rearmed_signals:
            _reclaim_dir = "above" if w.side == "CALL" else "below"
            _rearm_audit = self._build_watcher_audit_payload(
                w,
                trigger_type="rearm_check",
                current_bid=bid,
                current_ask=ask,
                current_mid=mid,
                arm_condition=f"trigger_{w.entry_trigger:.4f}" if w.entry_trigger else "",
                stop_condition=(
                    f"mid_{mid:.4f}_{_reclaim_dir}_reclaim_threshold_{reclaim_threshold:.4f}"
                ),
                reason_code="rearm_reclaimed",
                raw_reason=(
                    f"mid_{mid:.4f}_reclaimed_{_reclaim_dir}"
                    f"_stop_{w.stop_level:.4f}_tol_{WATCHER_REARM_TOLERANCE_PCT:.4f}"
                    if w.stop_level else f"mid_{mid:.4f}_reclaimed"
                ),
                extra={
                    "rearm_count":       w.rearm_count,
                    "rearm_reason":      w.rearm_reason,
                    "reclaim_mid":       mid,
                    "reclaim_threshold": reclaim_threshold,
                    "stop_level":        w.stop_level,
                    "rearm_tolerance":   WATCHER_REARM_TOLERANCE_PCT,
                },
            )
            self._persist_watcher_audit(w.signal.get("local_order_id"), _rearm_audit)
            log.info(
                "[%s] REARM_RECLAIMED — price returned to valid side | side=%s "
                "mid=%.4f stop=%.4f threshold=%.4f rearm_count=%d | "
                "arming for breach detection",
                w.ticker, w.side, mid,
                w.stop_level or 0.0,
                reclaim_threshold,
                w.rearm_count,
            )

    def add_signal(self, signal: dict) -> bool:
        now_et = datetime.now(ET)
        post_session = (
            now_et.hour > OVERNIGHT_THRESHOLD_HOUR
            or (now_et.hour == OVERNIGHT_THRESHOLD_HOUR and now_et.minute >= OVERNIGHT_THRESHOLD_MIN)
        )
        pre_market = now_et.hour < 9 or (now_et.hour == 9 and now_et.minute < 30)
        overnight = post_session or pre_market

        if overnight:
            session_label = "Pre-market" if pre_market else "Post-session"
            log.info(
                "[%s] %s signal accepted — holding for next regular session open. trigger=$%s",
                signal.get("ticker"),
                session_label,
                signal.get("entry_price") or (signal.get("trigger") or {}).get("entry", "?"),
            )

        watched = WatchedSignal(signal, overnight=overnight)
        watched._watcher_ref = self

        local_order_id = watched.signal.get("local_order_id")
        if not self._validate_local_order_id(local_order_id):
            log.error(
                "[%s] WATCH_ARM_BLOCKED — local_order_id=%s does not exist in OSM",
                watched.ticker,
                local_order_id,
            )
            _osm_audit = self._build_watcher_audit_payload(
                watched,
                trigger_type="add_signal_block",
                reason_code="osm_validation_failed",
                raw_reason=f"local_order_id={local_order_id}_not_in_osm",
            )
            self._persist_watcher_audit(local_order_id, _osm_audit)
            # PR-C: every self._last_reject_reason write goes under the
            # watcher lock so concurrent add_signal() callers cannot race
            # on the diagnostic field.
            with self._lock:
                self._last_reject_reason = "osm_validation_failed"
            return False

        dedup_key = self._dedup_key_for_signal(watched.signal)
        if not dedup_key:
            log.warning(
                "[%s] add_signal: signal_id missing — dedup disabled for this signal. "
                "Duplicate arms possible.", watched.ticker
            )

        with self._lock:
            if dedup_key and dedup_key in self._dedup_set:
                log.info(
                    "[%s] DEDUP_BLOCK — signal_id=%s is already armed in watcher",
                    watched.ticker,
                    dedup_key,
                )
                _dedup_audit = self._build_watcher_audit_payload(
                    watched,
                    trigger_type="add_signal_block",
                    reason_code="dedup_block",
                    raw_reason=f"signal_id_{dedup_key}_already_armed",
                    extra={"dedup_key": dedup_key, "persisted": False},
                )
                log.info(
                    "[watcher_audit] dedup_block | local_order_id=%s | %s",
                    watched.signal.get("local_order_id"),
                    json.dumps(_dedup_audit, default=str),
                )
                self._last_reject_reason = "dedup_block"
                return False

            # Conflict detection must include rearm_mode signals.
            # A DISARMED_WAITING_FOR_RECLAIM signal is still a live position attempt
            # with an open OSM order. Treating it as invisible (is_active=False)
            # would allow a new opposite-side signal to arm alongside it, and if
            # the disarmed signal later reclaims, both would be active simultaneously.
            # Using (w.is_active or w.rearm_mode) ensures rearm signals participate
            # in the same scoring and cancellation logic as normal signals.
            same_side = [
                w
                for w in self._pending
                if (w.is_active or getattr(w, "rearm_mode", False))
                and w.ticker == watched.ticker
                and w.side == watched.side
            ]
            opposite_side = [
                w
                for w in self._pending
                if (w.is_active or getattr(w, "rearm_mode", False))
                and w.ticker == watched.ticker
                and w.side != watched.side
            ]

            # Never keep both CALL and PUT armed for the same ticker. Stronger
            # score wins. Equal/lower score gets blocked to avoid OSM conflict.
            #
            # P0 PR smart-eligibility: prune the opposite_side candidate set
            # before the conflict decision. A stale or non-active opposite
            # should NOT block a fresh signal. We keep the legacy stronger-
            # score-wins / flip semantics; we only refine what counts as a
            # valid opposite to compare against.
            if opposite_side:
                # Same-symbol opposite filter — already guaranteed by the
                # ticker check above, kept as a documented invariant.
                _now_dt = datetime.now(timezone.utc)
                _fresh_opps = []
                _stale_opps_ignored = []
                for _opp in opposite_side:
                    _opp_age_sec = max(0.0, (_now_dt - _opp.created_at).total_seconds())
                    _opp_is_terminal = _opp.state in (
                        WatchState.CANCELLED,
                        WatchState.EXPIRED,
                        WatchState.INVALIDATED,
                    )
                    _opp_is_rearm_only = (
                        getattr(_opp, "rearm_mode", False) and not _opp.is_active
                    )
                    if _opp_is_terminal:
                        _stale_opps_ignored.append(
                            (_opp, "opp_in_terminal_state", _opp_age_sec)
                        )
                        continue
                    if (_opp_age_sec > OPPOSITE_CONFLICT_MAX_AGE_SEC
                            and _opp.score <= watched.score):
                        # Stale opposite that is not stronger — ignore.
                        _stale_opps_ignored.append(
                            (_opp, "opp_stale_not_stronger", _opp_age_sec)
                        )
                        continue
                    if (_opp_is_rearm_only and not getattr(watched, "rearm_mode", False)
                            and watched.score >= _opp.score):
                        # Fresh active beats rearm-only at equal/higher score.
                        _stale_opps_ignored.append(
                            (_opp, "opp_rearm_only_weaker", _opp_age_sec)
                        )
                        continue
                    _fresh_opps.append(_opp)

                if _stale_opps_ignored:
                    for _opp, _why, _age in _stale_opps_ignored:
                        log.info(
                            "[%s] OPPOSITE_IGNORED %s | opp_side=%s opp_score=%.1f "
                            "opp_age=%.0fs new_side=%s new_score=%.1f",
                            watched.ticker, _why, _opp.side, _opp.score, _age,
                            watched.side, watched.score,
                        )

                # If pruning eliminated all opposites, admit normally.
                if not _fresh_opps:
                    opposite_side = []

            if opposite_side:
                best_opp = max(_fresh_opps if '_fresh_opps' in dir() and _fresh_opps else opposite_side,
                               key=lambda w: w.score)
                # Equal-score tie-break: prefer higher-tier timeframe.
                _opp_score = best_opp.score
                _new_score = watched.score
                _scores_tied = abs(_new_score - _opp_score) <= OPPOSITE_CONFLICT_SCORE_TIE_PCT * max(1.0, _opp_score)
                if _scores_tied:
                    _opp_tf = (best_opp.signal or {}).get("timeframe", "")
                    _new_tf = (watched.signal or {}).get("timeframe", "")
                    _opp_tier = _opposite_tf_rank(_opp_tf)
                    _new_tier = _opposite_tf_rank(_new_tf)
                    # If new has strictly higher tier, treat as winner.
                    if _new_tier > _opp_tier:
                        _new_score = _opp_score + 0.01  # tip the scales for the > check below
                if _new_score > best_opp.score:
                    for w in opposite_side:
                        w.state = WatchState.CANCELLED
                        w._release_dedup_key()
                        log.info(
                            "[%s] SAFE_MODE_DIRECTION_FLIP — cancelling %s score=%.1f "
                            "for stronger %s score=%.1f",
                            watched.ticker,
                            w.side,
                            w.score,
                            watched.side,
                            watched.score,
                        )
                        _local_oid = w.signal.get("local_order_id")
                        if _local_oid and self.order_state_machine and hasattr(self.order_state_machine, "cancel_pending_entry"):
                            try:
                                _cancel_ok = self.order_state_machine.cancel_pending_entry(
                                    _local_oid, reason="direction_flip_watcher_cancel"
                                )
                                if not _cancel_ok:
                                    log.error(
                                        "[%s] direction_flip: cancel_pending_entry returned False "
                                        "for local_order_id=%s — OSM row may be stuck in "
                                        "PENDING_TRIGGER. Investigate immediately.",
                                        w.ticker, _local_oid,
                                    )
                            except Exception as _exc:
                                log.warning("[%s] OSM cancel failed for direction_flip: %s", w.ticker, _exc)
                    self._pending = [w for w in self._pending if w not in opposite_side]
                else:
                    # P0 PR — structured opposite_side_conflict audit.
                    # The block is the SAFE choice; we preserve the legacy
                    # "weaker setup loses to stronger opposite" semantic but
                    # add the conflicting-signal fields so the post-mortem
                    # can answer: was this correct? was the opposite stale?
                    # was it a tie that timeframe-tier should have flipped?
                    _opp_age_sec = max(
                        0.0,
                        (datetime.now(timezone.utc) - best_opp.created_at).total_seconds(),
                    )
                    _opp_signal = best_opp.signal or {}
                    _new_signal = watched.signal or {}
                    _decision_rule = (
                        "opp_stronger_fresh" if best_opp.score > watched.score
                        else (
                            "opp_higher_tf_tier_on_tie"
                            if _scores_tied
                            else "opp_equal_or_higher_score_blocks"
                        )
                    )
                    _opp_audit = self._build_watcher_audit_payload(
                        watched,
                        trigger_type="add_signal_block",
                        reason_code="opposite_side_conflict",
                        raw_reason=(
                            f"blocked_{watched.side}_{watched.score:.1f}"
                            f"_existing_{best_opp.side}_{best_opp.score:.1f}"
                        ),
                        extra={
                            # Required structured fields (P0 PR spec):
                            "block_stage":              "watcher",
                            "block_reason":             "opposite_side_conflict",
                            "symbol":                   watched.ticker,
                            "current_local_order_id":   _new_signal.get("local_order_id"),
                            "current_direction":        watched.side,
                            "current_score":            watched.score,
                            "current_timeframe":        _new_signal.get("timeframe"),
                            "current_pattern":          _new_signal.get("pattern"),
                            "current_signal_id":        watched.signal_id,
                            "conflicting_local_order_id": _opp_signal.get("local_order_id"),
                            "conflicting_direction":    best_opp.side,
                            "conflicting_score":        best_opp.score,
                            "conflicting_timeframe":    _opp_signal.get("timeframe"),
                            "conflicting_pattern":      _opp_signal.get("pattern"),
                            "conflicting_signal_id":    best_opp.signal_id,
                            "conflicting_state":        str(best_opp.state),
                            "conflict_age_seconds":     _opp_age_sec,
                            "conflict_scope":           "same_symbol_opposite_direction",
                            "decision_rule":            _decision_rule,
                            "score_tie_window_pct":     OPPOSITE_CONFLICT_SCORE_TIE_PCT,
                            "max_age_threshold_sec":    OPPOSITE_CONFLICT_MAX_AGE_SEC,
                            "stale_opps_ignored_count": len(_stale_opps_ignored) if '_stale_opps_ignored' in dir() else 0,
                            # Back-compat keys (old dashboard / replay scripts):
                            "blocked_side":    watched.side,
                            "blocked_score":   watched.score,
                            "existing_side":   best_opp.side,
                            "existing_score":  best_opp.score,
                            "persisted":       False,
                        },
                    )
                    log.info(
                        "[watcher_audit] opposite_side_conflict | local_order_id=%s | %s",
                        watched.signal.get("local_order_id"),
                        json.dumps(_opp_audit, default=str),
                    )
                    self._last_reject_reason = "opposite_side_conflict"
                    log.info(
                        "[%s] SAFE_MODE_BLOCK_OPPOSITE — keeping existing %s score=%.1f, "
                        "blocking new %s score=%.1f",
                        watched.ticker,
                        best_opp.side,
                        best_opp.score,
                        watched.side,
                        watched.score,
                    )
                    return False

            # Same-side dedup/replacement by score.
            if same_side:
                best_same = max(same_side, key=lambda w: w.score)
                if watched.score > best_same.score:
                    for w in same_side:
                        w.state = WatchState.CANCELLED
                        w._release_dedup_key()
                        log.info(
                            "[%s] SAME_SIDE_REPLACE — cancelling %s score=%.1f "
                            "for stronger same-side score=%.1f",
                            watched.ticker,
                            w.side,
                            w.score,
                            watched.score,
                        )
                        _local_oid = w.signal.get("local_order_id")
                        if _local_oid and self.order_state_machine and hasattr(self.order_state_machine, "cancel_pending_entry"):
                            try:
                                _cancel_ok = self.order_state_machine.cancel_pending_entry(
                                    _local_oid, reason="same_side_replace_watcher_cancel"
                                )
                                if not _cancel_ok:
                                    log.error(
                                        "[%s] same_side_replace: cancel_pending_entry returned False "
                                        "for local_order_id=%s — OSM row may be stuck in "
                                        "PENDING_TRIGGER. Investigate immediately.",
                                        w.ticker, _local_oid,
                                    )
                            except Exception as _exc:
                                log.warning("[%s] OSM cancel failed for same_side_replace: %s", w.ticker, _exc)
                    self._pending = [w for w in self._pending if w not in same_side]
                else:
                    _ss_audit = self._build_watcher_audit_payload(
                        watched,
                        trigger_type="add_signal_block",
                        reason_code="same_side_block",
                        raw_reason=(
                            f"blocked_{watched.side}_{watched.score:.1f}"
                            f"_existing_same_side_{best_same.score:.1f}"
                        ),
                        extra={
                            "blocked_score":   watched.score,
                            "existing_score":  best_same.score,
                            "side":            watched.side,
                            "persisted":       False,
                        },
                    )
                    log.info(
                        "[watcher_audit] same_side_block | local_order_id=%s | %s",
                        watched.signal.get("local_order_id"),
                        json.dumps(_ss_audit, default=str),
                    )
                    self._last_reject_reason = "same_side_block"
                    log.info(
                        "[%s] SAME_SIDE_BLOCK — keeping %s score=%.1f, "
                        "blocking weaker same-side score=%.1f",
                        watched.ticker,
                        watched.side,
                        best_same.score,
                        watched.score,
                    )
                    return False

            if dedup_key:
                self._dedup_set.add(dedup_key)

            self._pending.append(watched)

            # P0-W2: consume rearm marker placed by watch() arm-time path.
            # The marker is a private key in watched.signal (which IS signal_dict
            # by reference). Pop it so it never propagates downstream.
            _rearm_at_arm = bool(watched.signal.pop("__watcher_rearm_pending", False))
            _rearm_arm_reason = str(watched.signal.pop("__watcher_rearm_reason", "") or "")
            if _rearm_at_arm:
                watched.rearm_mode = True
                watched.rearm_reason = _rearm_arm_reason
                watched.signal["queue_status"] = "DISARMED_WAITING_FOR_RECLAIM"
                # Overnight-aware expiry: if signal is overnight/daily and we are
                # pre-market, start the window from 9:30 ET open, not right now,
                # so the signal is not silently expired before quotes are available.
                _rearm_window = timedelta(seconds=WATCHER_REARM_WINDOW_SEC)
                _now_utc = datetime.now(timezone.utc)
                if watched.overnight or _safe_is_daily_signal(watched):
                    try:
                        _market_open_et = datetime.now(ET).replace(
                            hour=9, minute=30, second=0, microsecond=0
                        ).astimezone(timezone.utc)
                        if _now_utc < _market_open_et:
                            watched.rearm_expires_at = _market_open_et + _rearm_window
                        else:
                            watched.rearm_expires_at = _now_utc + _rearm_window
                    except Exception:
                        watched.rearm_expires_at = _now_utc + _rearm_window
                else:
                    watched.rearm_expires_at = _now_utc + _rearm_window
                log.info(
                    "[%s] DISARMED_WAITING_FOR_RECLAIM | side=%s score=%.1f tier=%s | "
                    "reason=%s | rearm_window=%ds | expires=%s UTC",
                    watched.ticker,
                    watched.side,
                    watched.score,
                    watched.grade,
                    _rearm_arm_reason,
                    WATCHER_REARM_WINDOW_SEC,
                    watched.rearm_expires_at.strftime("%H:%M:%S") if watched.rearm_expires_at else "?",
                )

            overnight_count = sum(1 for w in self._pending if w.overnight and w.is_active)
            same_day_count = sum(1 for w in self._pending if not w.overnight and w.is_active)
            active_total = sum(1 for w in self._pending if w.is_active)
            rearm_total = sum(1 for w in self._pending if getattr(w, "rearm_mode", False))

        log.info(
            "[%s] Added to watch queue — %d same-day + %d overnight = %d active | %d rearm-wait",
            watched.ticker,
            same_day_count,
            overnight_count,
            active_total,
            rearm_total,
        )
        return True

    def watch(self, plan, local_order_id: str) -> bool:
        """Plan-aware entrypoint called by queue/execution orchestration."""
        if plan is None:
            log.warning("watch() called with None plan -- skipping")
            return False

        signal_dict = {
            "signal_id": getattr(plan, "signal_id", str(uuid.uuid4())),
            "ticker": getattr(plan, "ticker", ""),
            "side": getattr(plan, "side", "CALL"),
            "score": getattr(plan, "score", 65.0),
            "grade": getattr(plan, "tier", "B"),
            "entry_price": getattr(plan, "trigger_price", None),
            "stop_price": getattr(plan, "stop_underlying", None),
            "target_price": getattr(plan, "target_underlying", None),
            "plan_id": getattr(plan, "plan_id", ""),
            "local_order_id": local_order_id,
            "contract_symbol": getattr(plan, "contract_symbol", ""),
            "pattern": getattr(plan, "pattern", ""),
            "prior_day_high": getattr(plan, "prior_day_high", None),
            "prior_day_low": getattr(plan, "prior_day_low", None),
            "timeframe": getattr(plan, "timeframe", "1d"),
            "strategy_type": getattr(plan, "strategy_type", ""),
            "trigger": {
                "entry": getattr(plan, "trigger_price", None),
                "stop": getattr(plan, "stop_underlying", None),
                "pt1": getattr(plan, "target_underlying", None),
            },
        }

        now_et = datetime.now(ET)
        post_session = (
            now_et.hour > OVERNIGHT_THRESHOLD_HOUR
            or (now_et.hour == OVERNIGHT_THRESHOLD_HOUR and now_et.minute >= OVERNIGHT_THRESHOLD_MIN)
        )
        pre_market = now_et.hour < 9 or (now_et.hour == 9 and now_et.minute < 30)
        trigger = signal_dict.get("entry_price")
        side = str(signal_dict.get("side", "CALL")).upper()
        ticker = str(signal_dict.get("ticker", "")).upper()
        stop = signal_dict.get("stop_price")

        # Queue-time staleness check is skipped for outside-session setups.
        # Those are revalidated at the regular-session open instead.
        # Also skip for overnight/daily signals — their trigger is a prior-day
        # level, not a same-day intraday price. A 1.5% move from a prior-day
        # high/low is normal and should not invalidate the signal at arm time.
        _is_overnight_signal = bool(
            signal_dict.get("prior_day_high") or
            signal_dict.get("prior_day_low") or
            str(signal_dict.get("timeframe", "")).lower() in ("1d", "daily", "overnight")
        )

        # ── OPTION PREMIUM STALENESS CHECK ────────────────────────────────
        # Separate from the underlying check. Even if the underlying is at
        # the trigger level, the option premium may have already moved 20-40%
        # because other participants front-ran the entry. Entering now means
        # overpaying on premium that already priced in the move — catching the
        # reversal instead of the setup.
        #
        # Only runs when we have a reference option price from the signal and
        # live option quotes are available. If quotes unavailable, pass through
        # (fail open — a missed quote check is better than a missed trade).
        #
        # MAX_OPTION_PREMIUM_DRIFT: if option bid is already >25% above the
        # signal's entry_option_price, the move happened without us.
        # PR-C: read from the module-level constant; was previously read
        # via os.getenv() inline on every signal arm.
        _MAX_OPTION_PREMIUM_DRIFT = MAX_OPTION_PREMIUM_DRIFT_PCT
        _signal_option_price = float(
            getattr(plan, "entry_option_price", 0)
            or signal_dict.get("entry_option_price", 0)
            or 0
        )
        if _signal_option_price > 0 and not post_session and not pre_market:
            try:
                _opt_quote = self._get_option_quote(
                    str(signal_dict.get("contract_symbol", "")
                    or getattr(plan, "contract_symbol", ""))
                )
                _opt_bid = float((_opt_quote or {}).get("bid", 0) or 0)
                if _opt_bid > 0:
                    _opt_drift = (_opt_bid - _signal_option_price) / _signal_option_price
                    if _opt_drift > _MAX_OPTION_PREMIUM_DRIFT:
                        log.warning(
                            "[%s] OPTION_PREMIUM_STALE | signal_price=$%.2f "
                            "current_bid=$%.2f drift=+%.1f%% > %.0f%% max | "
                            "move already happened — rejecting late entry",
                            ticker, _signal_option_price, _opt_bid,
                            _opt_drift * 100, _MAX_OPTION_PREMIUM_DRIFT * 100,
                        )
                        self._last_reject_reason = (
                            f"option_premium_stale_{_opt_drift*100:+.1f}pct_above_signal"
                        )
                        return False
                    log.debug(
                        "[%s] Option premium OK | signal=$%.2f bid=$%.2f drift=%.1f%%",
                        ticker, _signal_option_price, _opt_bid, _opt_drift * 100,
                    )
            except Exception as _oq_exc:
                log.debug(
                    "[%s] Option premium check unavailable (non-blocking): %s",
                    ticker, _oq_exc,
                )

        if post_session or pre_market or _is_overnight_signal:
            log.info(
                "[%s] Outside-session queue — skipping underlying staleness check "
                "(trigger=$%.2f side=%s)",
                ticker,
                float(trigger or 0),
                side,
            )
        elif trigger and trigger > 0:
            try:
                quote = self._get_quote(ticker)
                bid = float(quote.get("bid") or 0)
                ask = float(quote.get("ask") or 0)
                mid = (bid + ask) / 2 if bid > 0 and ask > 0 else max(bid, ask)
                if mid > 0:
                    pct_from_trigger = (mid - trigger) / trigger
                    # FUNNEL FIX (2026-05-20):
                    # 1) Use WATCH_ARM_EFFECTIVE_THRESHOLD_PCT (env-tunable, env
                    #    can only loosen, default = 1.0% or hard-coded 1.5%
                    #    whichever is larger).
                    # 2) Distinguish drift-stale from below-stop-stale so the
                    #    reject reason actually tells operators which gate fired.
                    drift_stale = (
                        (side == "CALL" and pct_from_trigger >  WATCH_ARM_EFFECTIVE_THRESHOLD_PCT) or
                        (side == "PUT"  and pct_from_trigger < -WATCH_ARM_EFFECTIVE_THRESHOLD_PCT)
                    )
                    below_stop = False
                    if stop and stop > 0:
                        if side == "CALL" and mid < stop:
                            below_stop = True
                        elif side == "PUT" and mid > stop:
                            below_stop = True
                    if drift_stale:
                        # Drift stale: price has moved decisively away from trigger.
                        # Structurally missed — no re-arm opportunity.
                        reason_code = (
                            f"arm_drift_{pct_from_trigger*100:+.2f}pct"
                            f"_thr_{WATCH_ARM_EFFECTIVE_THRESHOLD_PCT*100:.2f}pct"
                        )
                        log.warning(
                            "[%s] STALE_ARM_REJECT gate=drift mid=$%.2f trigger=$%.2f "
                            "drift=%.3f%% effective_threshold=%.3f%% side=%s",
                            ticker, mid, trigger,
                            pct_from_trigger * 100.0,
                            WATCH_ARM_EFFECTIVE_THRESHOLD_PCT * 100.0,
                            side,
                        )
                        _drift_audit = self._build_watcher_audit_payload(
                            None,
                            symbol=ticker,
                            score=float(signal_dict.get("score") or 0),
                            tier=str(signal_dict.get("grade") or ""),
                            direction=side,
                            timeframe=str(signal_dict.get("timeframe") or ""),
                            pattern=str(signal_dict.get("pattern") or ""),
                            signal_id=str(signal_dict.get("signal_id") or ""),
                            plan_id=str(signal_dict.get("plan_id") or ""),
                            trigger_type="arm_time",
                            signal_entry_price=trigger,
                            trigger_price=trigger,
                            stop_price=stop,
                            current_bid=bid,
                            current_ask=ask,
                            current_mid=mid,
                            arm_price=mid,
                            arm_condition=(
                                f"drift_{pct_from_trigger*100:+.2f}pct"
                                f"_vs_threshold_{WATCH_ARM_EFFECTIVE_THRESHOLD_PCT*100:.2f}pct"
                            ),
                            stop_condition=f"stop_{stop:.4f}" if stop and stop > 0 else "",
                            reason_code="arm_drift",
                            raw_reason=reason_code,
                        )
                        self._persist_watcher_audit(local_order_id, _drift_audit)
                        self._last_reject_reason = reason_code
                        return False

                    elif below_stop:
                        # Wrong-side-of-stop: price is temporarily on the wrong side.
                        # For strong daily/overnight signals this may be transient —
                        # check rearm eligibility before permanently rejecting.
                        reason_code = f"arm_below_stop_mid_{mid:.2f}_stop_{stop:.2f}"
                        log.warning(
                            "[%s] STALE_ARM_REJECT gate=below_stop mid=$%.2f trigger=$%.2f "
                            "drift=%.3f%% side=%s stop=%.2f",
                            ticker, mid, trigger,
                            pct_from_trigger * 100.0,
                            side, stop,
                        )
                        _score_val = float(signal_dict.get("score") or 0)
                        _tier_val = str(signal_dict.get("grade") or "")
                        _is_daily_or_overnight = post_session or pre_market or _is_overnight_signal
                        _stop_audit = self._build_watcher_audit_payload(
                            None,
                            symbol=ticker,
                            score=_score_val,
                            tier=_tier_val,
                            direction=side,
                            timeframe=str(signal_dict.get("timeframe") or ""),
                            pattern=str(signal_dict.get("pattern") or ""),
                            signal_id=str(signal_dict.get("signal_id") or ""),
                            plan_id=str(signal_dict.get("plan_id") or ""),
                            trigger_type="arm_time",
                            signal_entry_price=trigger,
                            trigger_price=trigger,
                            stop_price=stop,
                            current_bid=bid,
                            current_ask=ask,
                            current_mid=mid,
                            arm_price=mid,
                            arm_condition=f"mid_{mid:.4f}_wrong_side_of_stop_{stop:.4f}",
                            stop_condition=f"stop_{stop:.4f}" if stop and stop > 0 else "",
                            reason_code="arm_below_stop",
                            raw_reason=reason_code,
                        )
                        if self._is_rearm_eligible(_score_val, _tier_val, _is_daily_or_overnight):
                            # Signal accepted into watcher in DISARMED state.
                            # add_signal() will flip rearm_mode=True via __watcher_rearm_pending.
                            # OSM order stays alive — on_expire fires on timeout if no reclaim.
                            _stop_audit["trigger_type"] = "arm_time_rearm_queued"
                            _stop_audit["rearm_eligible"] = True
                            _stop_audit["rearm_window_sec"] = WATCHER_REARM_WINDOW_SEC
                            _stop_audit["is_daily_or_overnight"] = _is_daily_or_overnight
                            log.info(
                                "[%s] ARM_BELOW_STOP → REARM_ELIGIBLE | "
                                "score=%.1f tier=%s daily_overnight=%s | "
                                "entering disarm window=%ds",
                                ticker, _score_val, _tier_val,
                                _is_daily_or_overnight,
                                WATCHER_REARM_WINDOW_SEC,
                            )
                            self._persist_watcher_audit(local_order_id, _stop_audit)
                            # Mark for rearm mode — consumed and cleaned by add_signal()
                            signal_dict["__watcher_rearm_pending"] = True
                            signal_dict["__watcher_rearm_reason"] = reason_code
                            # Do NOT set _last_reject_reason — this is not a rejection
                            # Do NOT return False — fall through to add_signal()
                        else:
                            # Low-score / intraday / not eligible — permanent reject
                            self._persist_watcher_audit(local_order_id, _stop_audit)
                            self._last_reject_reason = reason_code
                            return False
                    log.debug(
                        "[%s] Price check OK — $%.2f vs trigger $%.2f (%.1f%%)",
                        ticker,
                        mid,
                        trigger,
                        pct_from_trigger * 100.0,
                    )
            except Exception as exc:
                log.debug("[%s] Price staleness check failed; continuing: %s", ticker, exc)

        log.info(
            "[%s] watch() | plan=%s order=%s trigger=$%s side=%s",
            ticker,
            getattr(plan, "plan_id", ""),
            local_order_id,
            signal_dict["entry_price"],
            signal_dict["side"],
        )
        # PR-C / BUG-EW-4: when add_signal blocks (dedup, opposite-side
        # weaker score, etc.), the OSM entry order created earlier by the
        # queue/worker stays as a ghost CREATED row with no watcher
        # attached. Same failure mode as the watcher_expired /
        # watcher_invalidated cases that execution_core already cleans up
        # via _cleanup_pending_entry_order. Here we proactively cancel
        # the pending entry order through the existing OSM helper.
        try:
            ok = self.add_signal(signal_dict)
        finally:
            signal_dict.pop("__watcher_rearm_pending", None)
            signal_dict.pop("__watcher_rearm_reason", None)
        if not ok:
            # add_signal already logged the audit for locked-path blocks (dedup/opposite/same-side).
            # Attempt a best-effort DB persist here using the full signal context available in watch().
            _blk_reject = getattr(self, "_last_reject_reason", "") or "watcher_add_signal_blocked"
            _blk_audit = self._build_watcher_audit_payload(
                None,
                symbol=ticker,
                score=float(signal_dict.get("score") or 0),
                tier=str(signal_dict.get("grade") or ""),
                direction=side,
                timeframe=str(signal_dict.get("timeframe") or ""),
                pattern=str(signal_dict.get("pattern") or ""),
                signal_id=str(signal_dict.get("signal_id") or ""),
                plan_id=str(signal_dict.get("plan_id") or ""),
                trigger_type="add_signal_block",
                signal_entry_price=trigger,
                trigger_price=trigger,
                stop_price=stop,
                reason_code=_blk_reject,
                raw_reason=_blk_reject,
            )
            self._persist_watcher_audit(local_order_id, _blk_audit)
        if not ok and local_order_id and self.order_state_machine is not None:
            cancel_fn = getattr(self.order_state_machine, "cancel_pending_entry", None)
            if callable(cancel_fn):
                _reject = getattr(self, "_last_reject_reason", "") or "watcher_add_signal_blocked"
                try:
                    cancel_fn(local_order_id, reason=f"watcher_block:{_reject}")
                    log.info(
                        "[%s] OSM cancel_pending_entry called after watcher block | "
                        "local_order_id=%s reason=%s",
                        ticker, local_order_id, _reject,
                    )
                except Exception as _osm_exc:
                    log.error(
                        "[%s] OSM cancel_pending_entry failed after watcher block | "
                        "local_order_id=%s error=%s",
                        ticker, local_order_id, _osm_exc,
                    )
        return ok

    def start(self):
        if self._running:
            return
        if not self.on_trigger:
            msg = (
                "APEntryWatcher cannot start without on_trigger callback; "
                "triggered entries would not submit to execution core/OSM."
            )
            if self.require_on_trigger:
                raise RuntimeError(msg)
            log.warning(msg)
        self._running = True
        self._thread = threading.Thread(
            target=self._poll_loop,
            daemon=True,
            name="ap-entry-watcher",
        )
        self._thread.start()
        log.info("APEntryWatcher started")

    def stop(self):
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
        log.info("APEntryWatcher stopped")

    def status(self) -> list[dict]:
        with self._lock:
            return [
                {
                    "ticker": w.ticker,
                    "side": w.side,
                    "trigger": w.entry_trigger,
                    "target": w.target_price,
                    "stop": w.stop_level,
                    "state": w.state,
                    "mins_watching": round(w.minutes_watching, 1),
                    "score": w.score,
                    "grade": w.grade,
                    "overnight": w.overnight,
                    "queue_status": w.signal.get("queue_status", ""),
                    "strategy_type": w.signal.get("strategy_type", ""),
                    "timeframe": w.signal.get("timeframe", ""),
                    "plan_id": w.signal.get("plan_id", ""),
                    "local_order_id": w.signal.get("local_order_id", ""),
                    "contract_symbol": w.signal.get("contract_symbol", ""),
                    "last_bid": w.last_quote_bid,
                    "last_ask": w.last_quote_ask,
                    "breach_count": w.breach_count,
                    "rearm_mode": getattr(w, "rearm_mode", False),
                    "rearm_count": getattr(w, "rearm_count", 0),
                    "rearm_expires_at": (
                        w.rearm_expires_at.isoformat()
                        if getattr(w, "rearm_expires_at", None) else None
                    ),
                    "rearm_reason": getattr(w, "rearm_reason", ""),
                }
                for w in self._pending
            ]

    def _poll_loop(self):
        # Register with health registry once at thread start.
        try:
            from ap_health_registry import HEALTH as _WH, Criticality as _WC
            _WH.ensure_registered("ap_entry_watcher", _WC.HIGH, stale_after_s=45.0)
            _watcher_health_ok = True
        except Exception:
            _watcher_health_ok = False

        while self._running:
            try:
                self._check_all()
                if _watcher_health_ok:
                    try:
                        # PR-C: _WH was already captured at thread start;
                        # the inner `from ap_health_registry import HEALTH`
                        # was redundant and added a sys.modules lookup per
                        # heartbeat. Removed.
                        with self._lock:
                            _watching_count = sum(1 for w in self._pending if w.is_active)
                        _WH.heartbeat(
                            "ap_entry_watcher",
                            metrics={"watching": _watching_count},
                        )
                    except Exception:
                        pass
            except Exception as exc:
                log.error("Watcher poll error: %s", exc, exc_info=True)
            time.sleep(POLL_INTERVAL_SEC)

    def _check_all(self):
        now_et = datetime.now(ET)

        today_et = now_et.date()
        if self._open_protect_date != today_et:
            self._open_protect_date = today_et
            self._open_trigger_count = 0
            self._open_trigger_tickers = set()

        open_protect_active = (
            now_et.hour == 9 and 30 <= now_et.minute < 30 + OPEN_PROTECT_MINUTES
        )

        # EOD force-expire same-day signals only. Overnight setups survive.
        if now_et.hour > EOD_CUTOFF_HOUR or (now_et.hour == EOD_CUTOFF_HOUR and now_et.minute >= EOD_CUTOFF_MIN):
            with self._lock:
                expired = []
                surviving = []
                for w in self._pending:
                    # Expire both actively-watching and rearm-wait same-day signals.
                    # rearm_mode signals have is_active=False so they were previously
                    # invisible to this gate and would ghost as PENDING overnight.
                    _is_eod_target = (
                        (w.is_active and not w.overnight)
                        or (getattr(w, "rearm_mode", False) and not w.overnight)
                    )
                    if _is_eod_target:
                        if getattr(w, "rearm_mode", False):
                            w.rearm_mode = False  # clear before state change
                        w.state = WatchState.EXPIRED
                        w._release_dedup_key()
                        # BUG TRAP: WATCHING → EXPIRED logged here.
                        # If signals vanish before market open, this log reveals if
                        # the EOD cron is incorrectly expiring overnight signals.
                        _ew_record(
                            str(w.signal.get("signal_id", "")),
                            w.ticker, "EXPIRED",
                            "eod_force_expire_same_day_signal",
                            overnight=False,
                        )
                        expired.append(w)
                        log.info("[%s] Force-expired at market close (same-day signal)", w.ticker)
                    else:
                        surviving.append(w)
                self._pending = surviving
                if expired:
                    log.info(
                        "EOD force-expire: %d same-day signals expired, %d overnight signals held",
                        len(expired),
                        len(surviving),
                    )
            for w in expired:
                if self.on_expire:
                    try:
                        self.on_expire(w)
                    except Exception as _exc:
                        log.error("[%s] on_expire callback failed on EOD expire: %s", w.ticker, _exc)
            return

        # Pre-market hold — no regular trigger polling before 9:30 ET.
        if now_et.hour < 9 or (now_et.hour == 9 and now_et.minute < 30):
            return

        self._revalidate_overnight_at_open()
        self._poll_active_signals(open_protect_active=open_protect_active)
        self._check_rearm_signals()

    def _revalidate_overnight_at_open(self) -> None:
        with self._lock:
            overnight_active = [w for w in self._pending if w.is_active and w.overnight]

        if not overnight_active:
            return

        to_remove = []
        for w in overnight_active:
            if _safe_is_daily_signal(w) and _DAILY_VALIDATOR_AVAILABLE:
                w.signal["queue_status"] = OvernightWatchState.OPEN_RECHECK_PENDING
                try:
                    result = _validator_recheck_overnight_daily(w, self.broker)
                except Exception as exc:
                    _ov_exc_audit = self._build_watcher_audit_payload(
                        w,
                        trigger_type="overnight_revalidation",
                        reason_code="overnight_daily_validator_error",
                        raw_reason=f"validator_exception_{type(exc).__name__}",
                        extra={
                            "queue_status": str(w.signal.get("queue_status") or ""),
                            "is_daily": True,
                        },
                    )
                    self._persist_watcher_audit(w.signal.get("local_order_id"), _ov_exc_audit)
                    w.state = WatchState.INVALIDATED
                    w.signal["queue_status"] = OvernightWatchState.INVALIDATED
                    w._release_dedup_key()
                    log.error("[%s] OVERNIGHT_DAILY_VALIDATOR_ERROR — %s", w.ticker, exc, exc_info=True)
                    to_remove.append(w)
                    continue

                if not getattr(result, "valid", False):
                    _ov_inv_audit = self._build_watcher_audit_payload(
                        w,
                        trigger_type="overnight_revalidation",
                        reason_code="overnight_daily_invalidated",
                        raw_reason=(
                            f"{getattr(result, 'reason_code', 'UNKNOWN')}"
                            f"_{getattr(result, 'reason_text', '')}"
                        ),
                        extra={
                            "queue_status":     str(w.signal.get("queue_status") or ""),
                            "validator_reason": getattr(result, "reason_code", "UNKNOWN"),
                            "validator_text":   getattr(result, "reason_text", ""),
                            "is_daily":         True,
                        },
                    )
                    self._persist_watcher_audit(w.signal.get("local_order_id"), _ov_inv_audit)
                    w.state = WatchState.INVALIDATED
                    w.signal["queue_status"] = OvernightWatchState.INVALIDATED
                    w._release_dedup_key()
                    log.info(
                        "[%s] OVERNIGHT_DAILY_INVALIDATED | side=%s | %s | %s",
                        w.ticker,
                        w.side,
                        getattr(result, "reason_code", "UNKNOWN"),
                        getattr(result, "reason_text", ""),
                    )
                    to_remove.append(w)
                else:
                    w.overnight = False
                    w.signal["queue_status"] = OvernightWatchState.VALID_AWAITING_BREACH
                    log.info(
                        "[%s] OVERNIGHT_DAILY_ARMED | side=%s | queue_status=%s | %s",
                        w.ticker,
                        w.side,
                        OvernightWatchState.VALID_AWAITING_BREACH,
                        getattr(result, "reason_text", "valid"),
                    )
                continue

            # Generic/non-daily overnight revalidation from older stable watcher.
            try:
                quote = self._get_quote(w.ticker)
            except Exception as exc:
                log.warning("[%s] Overnight quote fetch failed: %s", w.ticker, exc)
                quote = {}

            bid = float(quote.get("bid") or 0)
            ask = float(quote.get("ask") or 0)
            if bid == 0 and ask == 0:
                last = float(quote.get("last") or 0)
                bid = ask = last

            if not (bid or ask) or not w.entry_trigger:
                # Mode-aware failure policy:
                # LIVE: quote outage = invalidate. Never arm with stale/zero quotes.
                #       Premium clients cannot have positions opened without verified price.
                # PAPER: fail open (arm watcher) — sandbox is for learning, not money protection.
                _is_live_watcher = str(getattr(self, "mode", "PAPER")).upper() == "LIVE"
                if _is_live_watcher:
                    _ov_quot_audit = self._build_watcher_audit_payload(
                        w,
                        trigger_type="overnight_revalidation",
                        current_bid=0.0,
                        current_ask=0.0,
                        current_mid=0.0,
                        reason_code="overnight_live_quote_unavailable",
                        raw_reason="live_overnight_recheck_quote_zero_invalidated",
                        extra={
                            "mode": "LIVE",
                            "quote_available": False,
                        },
                    )
                    self._persist_watcher_audit(w.signal.get("local_order_id"), _ov_quot_audit)
                    w.state = WatchState.INVALIDATED
                    w._release_dedup_key()
                    log.warning(
                        "[%s] LIVE overnight recheck: quote unavailable — INVALIDATING setup "
                        "(fail closed). Will need fresh signal at market open.",
                        w.ticker,
                    )
                else:
                    w.overnight = False
                    log.warning("[%s] PAPER overnight recheck: quote unavailable — arming fail-open", w.ticker)
                continue

            mid = (bid + ask) / 2.0 if bid and ask else max(bid, ask)
            if not mid:
                w.overnight = False
                continue

            drift = (mid - w.entry_trigger) / w.entry_trigger
            premarket_breached = (w.side == "CALL" and mid >= w.entry_trigger * 1.005) or (
                w.side == "PUT" and mid <= w.entry_trigger * 0.995
            )
            too_far = (w.side == "CALL" and drift > OVERNIGHT_MAX_DRIFT_PCT) or (
                w.side == "PUT" and drift < -OVERNIGHT_MAX_DRIFT_PCT
            )

            if premarket_breached:
                _ov_pre_audit = self._build_watcher_audit_payload(
                    w,
                    trigger_type="overnight_revalidation",
                    current_bid=bid,
                    current_ask=ask,
                    current_mid=mid,
                    arm_condition=f"trigger_{w.entry_trigger:.4f}",
                    stop_condition=f"stop_{w.stop_level:.4f}" if w.stop_level else "",
                    reason_code="overnight_premarket_breached",
                    raw_reason=f"mid_{mid:.4f}_already_through_trigger_{w.entry_trigger:.4f}",
                    extra={"drift_pct": round(drift * 100.0, 4)},
                )
                self._persist_watcher_audit(w.signal.get("local_order_id"), _ov_pre_audit)
                w.state = WatchState.EXPIRED
                w._release_dedup_key()
                log.info(
                    "[%s] OVERNIGHT INVALIDATED — pre-market breach detected. "
                    "Price $%.2f already through trigger $%.2f. Move done; expiring.",
                    w.ticker,
                    mid,
                    w.entry_trigger,
                )
                to_remove.append(w)
            elif too_far:
                _ov_drift_audit = self._build_watcher_audit_payload(
                    w,
                    trigger_type="overnight_revalidation",
                    current_bid=bid,
                    current_ask=ask,
                    current_mid=mid,
                    arm_condition=f"trigger_{w.entry_trigger:.4f}",
                    stop_condition=f"stop_{w.stop_level:.4f}" if w.stop_level else "",
                    reason_code="overnight_too_far_from_trigger",
                    raw_reason=(
                        f"mid_{mid:.4f}_drifted_{drift*100.0:+.2f}pct"
                        f"_from_trigger_{w.entry_trigger:.4f}"
                        f"_max_{OVERNIGHT_MAX_DRIFT_PCT*100:.1f}pct"
                    ),
                    extra={"drift_pct": round(drift * 100.0, 4)},
                )
                self._persist_watcher_audit(w.signal.get("local_order_id"), _ov_drift_audit)
                w.state = WatchState.EXPIRED
                w._release_dedup_key()
                log.info(
                    "[%s] OVERNIGHT INVALIDATED — price $%.2f drifted %.2f%% "
                    "from trigger $%.2f overnight. Expiring stale setup.",
                    w.ticker,
                    mid,
                    drift * 100.0,
                    w.entry_trigger,
                )
                to_remove.append(w)
            else:
                w.overnight = False
                log.info(
                    "[%s] OVERNIGHT VALIDATED at open — price $%.2f within %.2f%% "
                    "of trigger $%.2f. Arming for breach detection.",
                    w.ticker,
                    mid,
                    drift * 100.0,
                    w.entry_trigger,
                )

        # Fire expire/invalidate callbacks BEFORE removing from pending.
        # Without this, OSM orders for rejected overnight signals stay as
        # phantom PENDING_TRIGGER orders until the next startup cleanup.
        for w in to_remove:
            # ── BUG TRAP: log the exact reason this signal left WATCHING ──────
            # If a signal vanishes before market open, this log + SIGNAL_TRACE
            # will show exactly which overnight revalidation branch killed it.
            _sig_id = str(w.signal.get("signal_id", ""))
            _ticker = str(w.ticker or "")
            if _sig_id and _ticker:
                _reason = (
                    "overnight_revalidation_invalidated"
                    if w.state == WatchState.INVALIDATED
                    else "overnight_revalidation_expired"
                )
                _ew_record(_sig_id, _ticker,
                           "INVALIDATED" if w.state == WatchState.INVALIDATED else "EXPIRED",
                           _reason,
                           queue_status=str(w.signal.get("queue_status", "")))

            if w.state == WatchState.INVALIDATED and self.on_invalidate:
                try:
                    self.on_invalidate(w)
                except Exception as _exc:
                    log.error("[%s] on_invalidate failed during overnight revalidation: %s", w.ticker, _exc)
            elif w.state == WatchState.EXPIRED and self.on_expire:
                try:
                    self.on_expire(w)
                except Exception as _exc:
                    log.error("[%s] on_expire failed during overnight revalidation: %s", w.ticker, _exc)

        if to_remove:
            with self._lock:
                remove_ids = {id(w) for w in to_remove}
                self._pending = [w for w in self._pending if id(w) not in remove_ids]
            log.info("[WATCHER] Overnight revalidation: %d removed", len(to_remove))

    def _poll_active_signals(self, open_protect_active: bool) -> None:
        with self._lock:
            active = [w for w in self._pending if w.is_active]

        if not active:
            return

        tickers = list({w.ticker for w in active})
        try:
            quotes = self._fetch_quotes(tickers)
        except Exception as exc:
            log.warning("Quote fetch failed: %s", exc)
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
                    if open_protect_active and w.ticker in self._open_trigger_tickers:
                        # Per-ticker open protection: this ticker already triggered once
                        # at open. Block duplicate triggers for the same ticker within
                        # the open protection window (first 5 minutes).
                        w.state = WatchState.EXPIRED
                        w._release_dedup_key()
                        completed.append(("done", w))
                        log.info(
                            "[%s] OPEN_PROTECTION_BLOCK — ticker already triggered at open",
                            w.ticker,
                        )
                    else:
                        self._open_trigger_count += 1
                        if open_protect_active:
                            self._open_trigger_tickers.add(w.ticker)
                        completed.append(("trigger", w))
                elif new_state in (WatchState.EXPIRED, WatchState.INVALIDATED):
                    completed.append(("done", w))

            done_ids = {id(w) for _, w in completed}
            self._pending = [w for w in self._pending if id(w) not in done_ids]

        for action, w in completed:
            _sig_id = str(w.signal.get("signal_id", ""))
            _ticker = str(w.ticker or "")
            if action == "trigger":
                # Signal breached — record TRIGGER_READY before firing callback.
                if _sig_id and _ticker:
                    _ew_record(_sig_id, _ticker, "TRIGGER_READY",
                               "trigger_breached_entry_submitted",
                               contract=str(w.signal.get("contract_symbol", "")),
                               entry_trigger=str(w.entry_trigger or ""))
                if self.on_trigger:
                    # Retry on_trigger up to 3 times before expiring.
                    # A transient Tradier timeout or DB hiccup at breach time
                    # must not permanently kill a valid setup. On all 3 failures
                    # the signal is expired with a clear reason — not silently lost.
                    _trigger_attempts = getattr(w, "_trigger_attempts", 0)
                    try:
                        self.on_trigger(w)
                        w._trigger_attempts = 0   # reset on success
                    except Exception as exc:
                        _trigger_attempts += 1
                        w._trigger_attempts = _trigger_attempts
                        log.error(
                            "[%s] on_trigger callback failed (attempt %d/3): %s",
                            w.ticker, _trigger_attempts, exc, exc_info=True,
                        )
                        if _trigger_attempts < 3:
                            # Do NOT release dedup key — keep watcher armed for next poll
                            log.warning(
                                "[%s] TRIGGER_RETRY — will retry on next poll cycle (attempt %d/3)",
                                w.ticker, _trigger_attempts,
                            )
                            continue  # stay in poll loop, retry on next tick
                        else:
                            # 3 failures — expire cleanly with reason
                            log.error(
                                "[%s] TRIGGER_EXHAUSTED — 3 on_trigger failures, expiring signal | "
                                "last_error=%s",
                                w.ticker, exc,
                            )
                            w.state = WatchState.EXPIRED
                            w._release_dedup_key()
                            if _sig_id and _ticker:
                                _ew_record(_sig_id, _ticker, "EXPIRED",
                                           "on_trigger_exhausted_3_attempts")
                    finally:
                        if getattr(w, "_trigger_attempts", 0) == 0 or getattr(w, "_trigger_attempts", 0) >= 3:
                            w._release_dedup_key()
                else:
                    log.error("[%s] TRIGGERED but no on_trigger callback is wired", w.ticker)
                    w._release_dedup_key()
            elif w.state == WatchState.EXPIRED:
                # BUG TRAP: log every expiry with the reason it was in.
                if _sig_id and _ticker:
                    _ew_record(_sig_id, _ticker, "EXPIRED",
                               "signal_expired_in_poll_loop",
                               minutes_watching=str(getattr(w, "minutes_watching", "?")))
                if self.on_expire:
                    try:
                        self.on_expire(w)
                    except Exception as exc:
                        log.error("[%s] on_expire callback failed: %s", w.ticker, exc, exc_info=True)
            elif w.state == WatchState.INVALIDATED:
                _pending_audit = getattr(w, "_pending_audit", None)
                if _pending_audit:
                    self._persist_watcher_audit(
                        w.signal.get("local_order_id"), _pending_audit
                    )
                if _sig_id and _ticker:
                    _ew_record(_sig_id, _ticker, "INVALIDATED",
                               "signal_invalidated_in_poll_loop")
                if self.on_invalidate:
                    try:
                        self.on_invalidate(w)
                    except Exception as exc:
                        log.error("[%s] on_invalidate callback failed: %s", w.ticker, exc, exc_info=True)

    def _get_quote(self, ticker: str) -> dict:
        try:
            quotes = self._fetch_quotes([ticker])
            return quotes.get(str(ticker).upper(), {})
        except Exception:
            return {}

    def _get_option_quote(self, option_symbol: str) -> dict:
        """Fetch a single option contract quote (bid/ask/last).
        Uses the same Tradier quotes endpoint as _get_quote.
        Returns empty dict on any failure — caller treats as unavailable."""
        if not option_symbol or not option_symbol.strip():
            return {}
        try:
            quotes = self._fetch_quotes([option_symbol.strip().upper()])
            return quotes.get(option_symbol.strip().upper(), {})
        except Exception:
            return {}

    def _fetch_quotes(self, tickers: list[str]) -> dict:
        clean_tickers = [str(t).upper().strip() for t in tickers if str(t).strip()]
        if not clean_tickers:
            return {}

        symbols = ",".join(sorted(set(clean_tickers)))
        try:
            base_url = (
                getattr(self.broker, "base_url", None)
                or getattr(getattr(self.broker, "cfg", None), "base_url", None)
                or "https://sandbox.tradier.com"
            )
            resp = self.broker.session.get(
                f"{base_url}/v1/markets/quotes",
                params={"symbols": symbols, "greeks": "false"},
                headers={"Accept": "application/json"},
                timeout=5,
            )
            data = resp.json()
            quotes_raw = data.get("quotes", {}).get("quote", [])
            if isinstance(quotes_raw, dict):
                quotes_raw = [quotes_raw]
            return {
                str(q.get("symbol", "")).upper(): q
                for q in quotes_raw
                if q.get("symbol")
            }
        except Exception as exc:
            log.warning("Tradier quote fetch failed: %s", exc)
            return {}
