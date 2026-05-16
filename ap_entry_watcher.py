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
ET = ZoneInfo("America/New_York")

POLL_INTERVAL_SEC = 15
MAX_WATCH_MINUTES = 4320  # 72 hours — covers weekend holds
EOD_CUTOFF_HOUR = 15
EOD_CUTOFF_MIN = 30
WRONG_DIR_BUFFER_PCT = 0.001
OVERNIGHT_THRESHOLD_HOUR = 15
OVERNIGHT_THRESHOLD_MIN = 30

MAX_INTRADAY_WATCH_MIN = 5
MAX_INTRADAY_DRIFT_PCT = 0.015
OVERNIGHT_MAX_DRIFT_PCT = 0.020  # generic/non-daily overnight drift guard

OPEN_PROTECT_MINUTES = 5
MAX_OPEN_TRIGGERS = 1


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

        self.entry_trigger = float(entry or 0) or None
        self.stop_level = float(stop or 0) or None
        self.target_price = float(target or 0) or None

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
        return self.state == WatchState.PENDING

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

            if self.stop_level and bid <= self.stop_level * (1 - WRONG_DIR_BUFFER_PCT):
                self.state = WatchState.INVALIDATED
                self.breach_count = 0
                self._release_dedup_key()
                log.info(
                    "[%s] INVALIDATED — bid=$%.2f broke stop=$%.2f before trigger",
                    self.ticker,
                    bid,
                    self.stop_level,
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

            if self.stop_level and ask >= self.stop_level * (1 + WRONG_DIR_BUFFER_PCT):
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

    def __init__(self, broker, order_state_machine=None, require_on_trigger: Optional[bool] = None):
        self.broker = broker
        self.order_state_machine = order_state_machine
        if require_on_trigger is None:
            require_on_trigger = os.getenv("AP_WATCHER_REQUIRE_ON_TRIGGER", "1").strip().lower() not in {"0", "false", "no"}
        self.require_on_trigger = bool(require_on_trigger)
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
            "OSM object supplied but no recognized local-order lookup contract exists; "
            "allowing watcher arm for local_order_id=%s and relying on ExecutionCore recovery",
            local_order_id,
        )
        return True

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
                self._last_reject_reason = "dedup_block"
                return False

            same_side = [
                w
                for w in self._pending
                if w.is_active and w.ticker == watched.ticker and w.side == watched.side
            ]
            opposite_side = [
                w
                for w in self._pending
                if w.is_active and w.ticker == watched.ticker and w.side != watched.side
            ]

            # Never keep both CALL and PUT armed for the same ticker. Stronger
            # score wins. Equal/lower score gets blocked to avoid OSM conflict.
            if opposite_side:
                best_opp = max(opposite_side, key=lambda w: w.score)
                if watched.score > best_opp.score:
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
                                self.order_state_machine.cancel_pending_entry(
                                    _local_oid, reason="direction_flip_watcher_cancel"
                                )
                            except Exception as _exc:
                                log.warning("[%s] OSM cancel failed for direction_flip: %s", w.ticker, _exc)
                    self._pending = [w for w in self._pending if w not in opposite_side]
                else:
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
                                self.order_state_machine.cancel_pending_entry(
                                    _local_oid, reason="same_side_replace_watcher_cancel"
                                )
                            except Exception as _exc:
                                log.warning("[%s] OSM cancel failed for same_side_replace: %s", w.ticker, _exc)
                    self._pending = [w for w in self._pending if w not in same_side]
                else:
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
            overnight_count = sum(1 for w in self._pending if w.overnight and w.is_active)
            same_day_count = sum(1 for w in self._pending if not w.overnight and w.is_active)
            active_total = sum(1 for w in self._pending if w.is_active)

        log.info(
            "[%s] Added to watch queue — %d same-day + %d overnight = %d active",
            watched.ticker,
            same_day_count,
            overnight_count,
            active_total,
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

        if post_session or pre_market or _is_overnight_signal:
            log.info(
                "[%s] Outside-session queue — skipping queue-time staleness check "
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
                    stale = (side == "CALL" and pct_from_trigger > MAX_INTRADAY_DRIFT_PCT) or (
                        side == "PUT" and pct_from_trigger < -MAX_INTRADAY_DRIFT_PCT
                    )
                    if stop and stop > 0:
                        if side == "CALL" and mid < stop:
                            stale = True
                        elif side == "PUT" and mid > stop:
                            stale = True
                    if stale:
                        log.warning(
                            "[%s] STALE SIGNAL — price $%.2f is %.1f%% from trigger $%.2f "
                            "(side=%s) — skipping stale entry",
                            ticker,
                            mid,
                            pct_from_trigger * 100.0,
                            trigger,
                            side,
                        )
                        self._last_reject_reason = f"stale_price_{pct_from_trigger*100:+.1f}pct_from_trigger"
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
        return self.add_signal(signal_dict)

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
                        from ap_health_registry import HEALTH as _WH
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
                    if w.is_active and not w.overnight:
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
                    w.state = WatchState.INVALIDATED
                    w.signal["queue_status"] = OvernightWatchState.INVALIDATED
                    w._release_dedup_key()
                    log.error("[%s] OVERNIGHT_DAILY_VALIDATOR_ERROR — %s", w.ticker, exc, exc_info=True)
                    to_remove.append(w)
                    continue

                if not getattr(result, "valid", False):
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
                    try:
                        self.on_trigger(w)
                    except Exception as exc:
                        log.error("[%s] on_trigger callback failed: %s", w.ticker, exc, exc_info=True)
                    finally:
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
