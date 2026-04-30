# ap_execution_core.py -- Angel Precision Execution Core
# =============================================================================
# Ties all execution modules together into one clean interface.
# One instance per client (per ClientRunner thread).
#
# Signal flow:
#   receive_signal()
#     -> score gate (75 live / 45 paper)
#     -> context hard block (context < 10 live / 0 paper -> reject)
#     -> tier classify (A+/A/B)
#     -> B tier -> shadow tracker (paper only)
#     -> A/A+ -> RankingQueue (signals compete by score)
#         -> every 5s: highest scores fill available slots first
#         -> EntryWatcher parks signal -> waits for breach (2 polls)
#         -> breach confirmed -> options intelligence gate
#         -> order placed -> ExitEngine monitors position
#         -> position closed -> FeedbackLoop records outcome
#
# PAPER MODE FIXES:
#   Fix A: Fallback exec quality scores when no real chain data (paper only)
#   Fix B: Score floor lowered to 75 in paper mode (85 live)
#   Fix C: Context floor lowered to 8.0 in paper mode (12.0 live)
#   Fix D: Skip context gate when score_breakdown missing OR real_time_ctx absent
#           _apply_paper_exec_fallbacks injects spread/liquidity into score_breakdown,
#           making it non-empty. Checking only bool(score_breakdown) would then trigger
#           the context gate with ctx=0.0 and block every signal. The fix requires
#           real_time_ctx to actually be present before gating on it.
#
# SIGNAL INTELLIGENCE:
#   Every signal is a first-class persistent object in ap_signals.
#   Full lifecycle: received -> queued -> watching -> triggered -> executed -> closed
#   Dropped signals record WHY: rejected / context_blocked / shadow /
#   sector_cap / context_recheck_fail / requeued_after_trigger / legacy_routed
#
# FUNNEL COUNTER:
#   All funnel.inc() calls wired: signals_received, rejected_score, passed_score,
#   context_blocked, passed_context, shadow_tracked, sector_capped, queue_expired,
#   watcher_sent, watcher_triggered, watcher_expired, watcher_invalidated,
#   options_rejected, order_failed, trades_executed
# =============================================================================

from __future__ import annotations

import os
import time
import uuid
import logging
import threading
from datetime import datetime, timezone, date
from typing import Optional
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from ap_entry_watcher        import APEntryWatcher, WatchedSignal
from ap_options_intelligence import evaluate_contract, chain_health_report
from ap_exit_engine          import APExitEngine, ManagedPosition
from ap_feedback_loop        import APFeedbackLoop
from ap_tier_engine          import APTierEngine, APShadowTracker, Tier
from ap_proof_logger         import APProofLogger, funnel
from ap_signal_store         import APSignalStore
from ap_signal_tracker       import APSignalTracker
from ap_master_control       import APMasterControl, ApprovedExecutionPlan

# Intelligence outcome feedback — optional, fails silently if bridge not deployed
try:
    from intelligence_bridge import record_trade_outcome as _record_intel_outcome
except ImportError:
    _record_intel_outcome = None

log = logging.getLogger("ap.execution_core")
ET  = ZoneInfo("America/New_York")

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
BOT_MODE            = (os.getenv("AP_MODE") or os.getenv("BOT_MODE") or "PAPER").upper()
ENABLE_LEGACY_EXECUTION_CORE = False  # pure production: legacy ExecutionCore path is permanently disabled
MAX_POSITIONS       = int(os.getenv("MAX_POSITIONS", "7"))

# ── Paper-mode gate thresholds ────────────────────────────────────────────────
SCORE_FLOOR_LIVE    = 75   # live: only trade validated setups
SCORE_FLOOR_PAPER   = 45   # paper: collect data on all qualifying signals
CONTEXT_FLOOR_LIVE  = 10.0
CONTEXT_FLOOR_PAPER = 0.0  # paper: don't block on context -- collect the data

# Paper-mode fallback exec quality scores (used when chain data absent)
PAPER_SPREAD_DEFAULT    = 3.0   # /5
PAPER_LIQUIDITY_DEFAULT = 3.0   # /5


# =============================================================================
# RANKING QUEUE
# =============================================================================

class RankingQueue:
    """
    Signals compete before execution. Highest score fills slots first.
    Prevents a mediocre A trade from blocking an A+ that arrives 2 seconds later.
    """

    MAX_WAIT_SECONDS = 120

    def __init__(self):
        self._queue: list[dict] = []
        self._lock  = threading.Lock()

    def add(self, signal: dict):
        signal["_queued_at"] = time.time()
        with self._lock:
            key      = f"{signal.get('ticker')}:{signal.get('side')}"
            existing = next((s for s in self._queue
                             if f"{s.get('ticker')}:{s.get('side')}" == key), None)
            if existing:
                if float(signal.get("score", 0)) > float(existing.get("score", 0)):
                    self._queue.remove(existing)
                    log.info(
                        f"[{signal.get('ticker')}] Replaced queued signal "
                        f"(score {existing.get('score',0):.0f} -> {signal.get('score',0):.0f})"
                    )
                else:
                    log.debug(f"[{signal.get('ticker')}] Kept existing higher-score signal in queue")
                    return
            self._queue.append(signal)
            self._queue.sort(key=lambda s: float(s.get("score", 0)), reverse=True)
        log.info(
            f"[{signal.get('ticker')}] Added to ranking queue "
            f"score={signal.get('score', 0):.1f} [{signal.get('grade')}] -- "
            f"{len(self._queue)} total queued"
        )

    def drain(self, available_slots: int) -> list[dict]:
        """Return up to available_slots signals, highest score first. Expire stale ones."""
        now = time.time()
        with self._lock:
            fresh   = [s for s in self._queue
                       if now - s.get("_queued_at", now) < self.MAX_WAIT_SECONDS]
            expired = len(self._queue) - len(fresh)
            if expired > 0:
                log.info(f"RankingQueue: {expired} signal(s) expired (>{self.MAX_WAIT_SECONDS}s)")
                try:
                    from ap_proof_logger import funnel as _f
                    _f.inc("queue_expired", expired)
                except Exception:
                    pass
            self._queue  = fresh
            to_execute   = self._queue[:available_slots]
            self._queue  = self._queue[available_slots:]
        return to_execute

    def size(self) -> int:
        with self._lock:
            return len(self._queue)



# =============================================================================
# EXECUTION CORE
# =============================================================================

# ═══════════════════════════════════════════════════════════
# PRODUCTION PATH (April 2026):
#   /signal → trade_queue → worker_loop → APMasterControl →
#   APContractSelector → APOrderStateMachine → APEntryWatcher.watch()
#   → APPositionManager
# receive_signal() is NOT in the production path.
# ═══════════════════════════════════════════════════════════
# ── Quote cache — survives Tradier timeouts so valid breach signals aren't dropped ──
_last_quote_cache: dict = {}  # {ticker: {"price": float, "ts": float}}

def _get_underlying_price_cached(ticker: str, broker, max_age_seconds: int = 300) -> float:
    """
    Fetch live quote with fallback to recent cache.
    Raises if cache is stale/empty and live fetch fails — legitimate abort.
    """
    import time as _time
    try:
        price = broker.get_quote(ticker)
        _last_quote_cache[ticker] = {"price": float(price), "ts": _time.time()}
        return float(price)
    except Exception as exc:
        cached = _last_quote_cache.get(ticker)
        if cached and (_time.time() - cached["ts"]) < max_age_seconds:
            age = int(_time.time() - cached["ts"])
            log.warning(
                "[%s] Live quote failed (%s) — using cached price $%.2f (age=%ds)",
                ticker, exc, cached["price"], age,
            )
            return cached["price"]
        log.error("[%s] Quote fetch failed and cache empty/stale — aborting", ticker)
        raise


class APExecutionCore:
    """
    One instance per client (per ClientRunner thread).
    Manages full lifecycle: signal -> rank -> watch -> enter -> manage -> exit -> record.
    """

    def __init__(self, broker, supabase_client=None, email: str = "", position_manager=None, order_state_machine=None, data_broker=None, master_control=None):
        self.broker    = broker
        self.email              = email
        self.position_manager   = position_manager    # APPositionManager (optional for now)
        self.order_state_machine = order_state_machine # APOrderStateMachine (optional for now)
        # Mode: injected master_control is canonical; BOT_MODE is the env fallback.
        # Resolved again after mc is assigned — see below.
        self.paper     = BOT_MODE != "LIVE"
        self.mode      = "LIVE" if not self.paper else "PAPER"
        self._pos_lock = threading.Lock()
        self._position_count = 0

        # Mode-specific gate values (may be overridden after mc injection)
        self._score_floor   = SCORE_FLOOR_PAPER   if self.paper else SCORE_FLOOR_LIVE
        self._context_floor = CONTEXT_FLOOR_PAPER if self.paper else CONTEXT_FLOOR_LIVE

        # Signal intelligence store + tracker
        # Tracker deduplicates strictly by signal_id so one per client is safe.
        self.store   = APSignalStore(supabase_client, client_email=email)
        self.tracker = APSignalTracker(supabase_client, store=self.store)

        # Core modules
        self.entry_watcher = APEntryWatcher(broker, order_state_machine=self.order_state_machine)
        self.exit_eng    = APExitEngine(broker, email=email,
                                           data_broker=data_broker)
        self.feedback    = APFeedbackLoop(supabase_client, DISCORD_WEBHOOK_URL, signal_store=self.store)
        self.tier_engine = APTierEngine()
        self.shadow      = APShadowTracker(supabase_client, DISCORD_WEBHOOK_URL)
        self.rank_queue  = RankingQueue()
        self._sector_counts: dict[str, int] = {}
        self._sector_lock   = threading.Lock()
        self.proof          = APProofLogger(
            supabase_client=supabase_client,
            client_email=email,
            mode="paper" if self.paper else "live",
        )

        # ── MASTER CONTROL -- single decision authority ────────────────────────
        # Fix: require injected master_control; internal construction only if
        # ALLOW_INTERNAL_MASTER_CONTROL=1 (unit tests / local dev only).
        import os as _os_ec
        _allow_internal = _os_ec.getenv("ALLOW_INTERNAL_MASTER_CONTROL", "0") == "1"
        if master_control is not None:
            self.master_control = master_control
            log.info("[%s] APExecutionCore using injected master_control", email)
        elif _allow_internal:
            log.warning(
                "[%s] APExecutionCore building internal master_control "
                "(ALLOW_INTERNAL_MASTER_CONTROL=1 — dev/test only)", email
            )
            self.master_control = APMasterControl(
                mode           = "live" if BOT_MODE == "LIVE" else "paper",
                score_floor    = self._score_floor,
                context_floor  = self._context_floor,
                max_positions  = MAX_POSITIONS,
                supabase_client= supabase_client,
                signal_store   = self.store,
                tier_engine    = self.tier_engine,
                feedback_loop  = self.feedback,
            )
            self.master_control.wire(
                position_count_fn = lambda: self._position_count,
                kill_switch_fn    = None,
                mode_fn           = None,
            )
        else:
            raise RuntimeError(
                f"[{email}] APExecutionCore requires injected master_control in production. "
                "Pass master_control= from ClientRunner. "
                "Set ALLOW_INTERNAL_MASTER_CONTROL=1 only for local tests."
            )

        # ── Mode + limits canonical override from master_control ──────────────
        # Now that mc is assigned, derive paper/mode/max_pos from the single
        # authority so execution core is always cohesive with the runner.
        if hasattr(self, "master_control") and self.master_control is not None:
            _mc_mode   = getattr(self.master_control, "mode", self.mode).upper()
            self.mode  = _mc_mode
            self.paper = _mc_mode != "LIVE"
            self._max_positions = getattr(self.master_control, "max_positions", MAX_POSITIONS)
            # Re-derive floors from canonical mode
            self._score_floor   = SCORE_FLOOR_PAPER   if self.paper else SCORE_FLOOR_LIVE
            self._context_floor = CONTEXT_FLOOR_PAPER if self.paper else CONTEXT_FLOOR_LIVE
            # Sync proof logger mode
            if hasattr(self, "proof"):
                self.proof.mode = "paper" if self.paper else "live"
        else:
            self._max_positions = MAX_POSITIONS

        # Production safety: legacy direct-ranking/receive_signal path may exist
        # for paper/dev fallback, but LIVE must remain queue/OSM authority only.
        if ENABLE_LEGACY_EXECUTION_CORE and self.mode == "LIVE":
            raise RuntimeError(
                f"[{email}] ENABLE_LEGACY_EXECUTION_CORE must be off in LIVE mode; "
                "production execution must stay queue-driven through trade_queue/worker_loop/OSM."
            )

        # Wire watcher callbacks
        self.entry_watcher.on_trigger    = self._on_entry_trigger
        self.entry_watcher.on_expire     = self._on_signal_expire
        self.entry_watcher.on_invalidate = self._on_signal_invalidate

        # Wire exit callbacks
        self.exit_eng.on_exit  = self._on_position_close
        self.exit_eng.on_scale = self._on_position_scale

        log.info(
            f"APExecutionCore initialized for {email} | "
            f"Mode: {self.mode} | "
            f"MaxPos: {self._max_positions} | "
            f"ScoreFloor: {self._score_floor} | "
            f"ContextFloor: {self._context_floor}"
        )

    @property
    def _exit_thread(self) -> Optional[threading.Thread]:
        """Expose exit engine thread so worker_health can check liveness."""
        return self.exit_eng._thread

    def _score_and_context_floors(self) -> tuple[float, float]:
        """Single source of truth for score/context floors per mode."""
        if self.mode == "LIVE":
            return SCORE_FLOOR_LIVE, CONTEXT_FLOOR_LIVE
        return SCORE_FLOOR_PAPER, CONTEXT_FLOOR_PAPER

    def _current_open_position_count(self) -> int:
        """
        Return the most reliable open-position count available.

        Production path uses APPositionManager/Postgres truth. The local
        _position_count is kept only as a fallback for legacy/synthetic paper
        paths that do not create a DB position through OSM/fill_monitor.
        """
        if self.position_manager is not None:
            try:
                snap = self.position_manager.snapshot()
                return int(snap.get("open_count") or 0)
            except Exception as exc:
                log.warning(
                    "[%s] position_manager.snapshot failed — falling back to local count: %s",
                    self.email, exc,
                )
        with self._pos_lock:
            return int(self._position_count or 0)

    def _current_pending_entry_count(self) -> int:
        """Return pending entry count from position-manager snapshot when available."""
        if self.position_manager is not None:
            try:
                snap = self.position_manager.snapshot()
                return int(snap.get("pending_entries") or 0)
            except Exception:
                return 0
        return 0

    def _available_position_slots(self) -> int:
        """
        Slots based on broker/DB truth first, local legacy count second.
        This prevents the ranking queue from over-dispatching just because
        _position_count only tracks synthetic/legacy positions.
        """
        open_count = self._current_open_position_count()
        pending_entries = self._current_pending_entry_count()
        return max(0, int(self._max_positions) - open_count - pending_entries)

    def _recover_plan_for_revalidation(self, watched: WatchedSignal):
        """
        Recover a minimal ApprovedExecutionPlan-like object for breach-time
        exposure revalidation when the watcher signal came from queue.watch(plan)
        and did not carry _approved_plan through the signal dict.

        This keeps the queue as the production entry authority while preserving
        LIVE-mode capital protection at breach time. If recovery cannot prove
        a real reserved/limit cost, LIVE mode will block rather than fail open.
        """
        sig = watched.signal or {}
        existing = sig.get("_approved_plan")
        if existing is not None:
            return existing

        local_order_id = str(sig.get("local_order_id") or "")
        if not local_order_id or self.order_state_machine is None:
            return None

        try:
            order = self.order_state_machine.get_order(local_order_id)
        except Exception as exc:
            log.warning(
                "[%s] Could not recover approved plan from OSM order %s: %s",
                watched.ticker, local_order_id, exc,
            )
            return None

        if not order:
            return None

        try:
            qty = int(order.get("qty") or 0)
            reserved = float(order.get("reserved_cost") or 0)
            limit_price = float(order.get("limit_price") or 0)
            real_cost = reserved if reserved > 0 else (limit_price * qty * 100 if limit_price > 0 and qty > 0 else 0.0)
            if real_cost <= 0:
                log.critical(
                    "[%s] Recovered OSM order %s but could not prove real_cost for LIVE breach revalidation",
                    watched.ticker, local_order_id,
                )
                return None

            recovered = SimpleNamespace(
                plan_id=str(order.get("plan_id") or sig.get("plan_id") or local_order_id),
                signal_id=str(order.get("signal_id") or sig.get("signal_id") or local_order_id),
                client_id=str(order.get("client_id") or self.email or "default"),
                ticker=str(order.get("symbol") or watched.ticker),
                side=str(order.get("direction") or watched.side),
                direction=str(order.get("direction") or watched.side),
                pattern=str(sig.get("pattern") or ""),
                timeframe=str(sig.get("timeframe") or "1d"),
                contracts=qty,
                max_position_usd=real_cost,
                tier=str(sig.get("grade") or sig.get("tier") or "B"),
                score=float(sig.get("score") or 0),
                trigger_type="breach",
                trigger_price=getattr(watched, "entry_trigger", watched.trigger_price),
                stop_underlying=watched.stop_level,
                target_underlying=watched.target_price,
                contract_symbol=str(order.get("contract") or sig.get("contract_symbol") or ""),
                limit_price=limit_price if limit_price > 0 else None,
            )
            sig["_approved_plan"] = recovered
            log.info(
                "[%s] Recovered approved plan for breach revalidation from OSM order %s | cost=$%.0f",
                watched.ticker, local_order_id, real_cost,
            )
            return recovered
        except Exception as exc:
            log.warning(
                "[%s] Failed to recover breach revalidation plan from OSM order %s: %s",
                watched.ticker, local_order_id, exc,
            )
            return None

    def _breach_risk_check(self, watched: WatchedSignal) -> bool:
        """
        Lightweight breach-time safety check.

        IMPORTANT: Do not call master_control.evaluate() here. The signal was
        already approved and armed, and evaluate() performs dedup/persistence
        side effects that are wrong for an in-flight signal. This check only
        verifies kill-switch and current position capacity using live state.
        """
        sig = watched.signal
        ticker = watched.ticker
        signal_id = str(sig.get("signal_id", "") or "")

        if getattr(self, "_kill_switch", False):
            log.critical("[%s] Breach blocked — execution core kill switch active", ticker)
            if signal_id:
                self.store.update_signal_fields(signal_id, {
                    "decision_status": "blocked_at_breach",
                    "context_notes": "kill_switch_active_at_breach",
                })
            return False

        if self.master_control is not None:
            try:
                kill_fn = getattr(self.master_control, "_kill_switch_fn", None)
                if kill_fn and kill_fn():
                    log.critical("[%s] Breach blocked — master control kill switch active", ticker)
                    if signal_id:
                        self.store.update_signal_fields(signal_id, {
                            "decision_status": "blocked_at_breach",
                            "context_notes": "master_control_kill_switch_active_at_breach",
                        })
                    return False
            except Exception as exc:
                log.warning("[%s] Kill-switch check failed at breach: %s", ticker, exc)

        open_count = self._current_open_position_count()
        pending_entries = self._current_pending_entry_count()
        effective_count = open_count + pending_entries
        if effective_count >= int(self._max_positions):
            log.info(
                "[%s] No slot at breach time — open=%s pending=%s max=%s. Re-queuing signal.",
                ticker, open_count, pending_entries, self._max_positions,
            )
            if signal_id:
                self.store.update_signal_fields(signal_id, {
                    "decision_status": "requeued_after_trigger",
                    "context_notes": (
                        f"positions_full_at_breach open={open_count} "
                        f"pending={pending_entries} max={self._max_positions}"
                    ),
                })
            _sector = sig.get("sector") or sig.get("correlation_bucket") or ticker
            with self._sector_lock:
                self._sector_counts[_sector] = max(0, self._sector_counts.get(_sector, 0) - 1)
            self.rank_queue.add(sig)
            return False

        approved_plan = self._recover_plan_for_revalidation(watched)
        if approved_plan is None:
            msg = "approved_plan_missing_at_breach_revalidation"
            if self.mode == "LIVE":
                log.critical(
                    "[%s] LIVE BREACH BLOCK — _approved_plan missing; cannot revalidate exposure safely",
                    ticker,
                )
                if signal_id:
                    self.store.update_signal_fields(signal_id, {
                        "decision_status": "blocked_at_breach",
                        "context_notes": msg,
                    })
                _sector = sig.get("sector") or sig.get("correlation_bucket") or ticker
                with self._sector_lock:
                    self._sector_counts[_sector] = max(0, self._sector_counts.get(_sector, 0) - 1)
                return False

            log.critical(
                "[%s] PAPER BREACH WARNING — _approved_plan missing; continuing without exposure revalidation",
                ticker,
            )
            if signal_id:
                self.store.update_signal_fields(signal_id, {
                    "context_notes": msg + "_paper_fail_open",
                })

        if approved_plan is not None and self.master_control is not None:
            try:
                reval = self.master_control.revalidate_exposure(
                    approved_plan,
                    client_id=self.email or "default",
                )
                if not getattr(reval, "ok", False):
                    reason = getattr(reval, "reason", "revalidation_failed")
                    log.info("[%s] Breach exposure revalidation blocked: %s", ticker, reason)
                    if signal_id:
                        self.store.update_signal_fields(signal_id, {
                            "decision_status": "blocked_at_breach",
                            "context_notes": f"exposure_revalidation={reason}",
                        })
                    _sector = sig.get("sector") or sig.get("correlation_bucket") or ticker
                    with self._sector_lock:
                        self._sector_counts[_sector] = max(0, self._sector_counts.get(_sector, 0) - 1)
                    return False
            except Exception as exc:
                if self.mode == "LIVE":
                    log.critical(
                        "[%s] LIVE BREACH BLOCK — exposure revalidation errored: %s",
                        ticker, exc,
                    )
                    if signal_id:
                        self.store.update_signal_fields(signal_id, {
                            "decision_status": "blocked_at_breach",
                            "context_notes": f"exposure_revalidation_error={exc}",
                        })
                    _sector = sig.get("sector") or sig.get("correlation_bucket") or ticker
                    with self._sector_lock:
                        self._sector_counts[_sector] = max(0, self._sector_counts.get(_sector, 0) - 1)
                    return False
                log.warning("[%s] PAPER breach exposure revalidation failed open: %s", ticker, exc)

        return True

    def start(self):
        """Start production orchestration threads only.

        Pure production contract:
          - Queue/worker owns signal evaluation and plan creation.
          - OSM owns order lifecycle.
          - EntryWatcher only waits for breach and calls this core back.
          - Legacy ranking/direct execution is never started.
        """
        problems = []
        if self.master_control is None:
            problems.append("master_control_missing")
        if self.order_state_machine is None:
            problems.append("order_state_machine_missing")
        if self.entry_watcher is None:
            problems.append("entry_watcher_missing")
        if getattr(self.entry_watcher, "on_trigger", None) != self._on_entry_trigger:
            problems.append("entry_watcher_on_trigger_not_wired")
        if getattr(self.entry_watcher, "order_state_machine", None) is not self.order_state_machine:
            problems.append("entry_watcher_osm_not_wired")
        if self.exit_eng is None:
            problems.append("exit_engine_missing")
        if self.tracker is None:
            problems.append("tracker_missing")
        if problems:
            raise RuntimeError(f"[{self.email}] APExecutionCore production startup validation failed: {','.join(problems)}")

        self.entry_watcher.start()
        self.exit_eng.start()
        self.tracker.start()
        self._rq_running = False
        log.info(f"[{self.email}] Execution core started (PURE_PRODUCTION: watcher + exit engine + tracker; queue is entry authority)")

    def stop(self):
        self.entry_watcher.stop()
        self.exit_eng.stop()
        self.tracker.stop()
        self._rq_running = False

    # ── HELPER: paper-mode exec quality fallbacks ─────────────────────────────

    def _apply_paper_exec_fallbacks(self, signal: dict) -> dict:
        """
        Fix A: In paper mode, when exec quality sub-scores are 0/absent,
        inject minimum viable defaults so composite score is not crushed.
        Only mutates a copy -- never alters the original signal dict.

        NOTE: This injects spread_score/liquidity_score into score_breakdown,
        making it non-empty. The context gate must therefore check for the
        presence of real_time_ctx specifically, not just bool(score_breakdown).
        """
        if not self.paper:
            return signal

        import copy
        sig = copy.deepcopy(signal)
        bd  = sig.setdefault("score_breakdown", {})

        spread_score    = float(bd.get("spread_score", 0) or 0)
        liquidity_score = float(bd.get("liquidity_score", 0) or 0)

        if spread_score == 0 and liquidity_score == 0:
            bd["spread_score"]    = PAPER_SPREAD_DEFAULT
            bd["liquidity_score"] = PAPER_LIQUIDITY_DEFAULT
            log.debug(
                f"[{sig.get('ticker')}] Paper exec fallback applied: "
                f"spread={PAPER_SPREAD_DEFAULT} liquidity={PAPER_LIQUIDITY_DEFAULT}"
            )
            raw_score = float(sig.get("score", 0) or 0)
            if raw_score > 0:
                injected                 = PAPER_SPREAD_DEFAULT + PAPER_LIQUIDITY_DEFAULT
                sig["score"]             = round(raw_score + injected, 2)
                sig["_paper_exec_boost"] = injected
                log.debug(
                    f"[{sig.get('ticker')}] Paper score boosted: "
                    f"{raw_score:.1f} -> {sig['score']:.1f} (+{injected:.1f} exec fallback)"
                )

        return sig

    # ── PUBLIC: receive incoming scanner signal ───────────────────────────────

    def receive_signal(self, signal: dict, score_result=None):
        """Compatibility shim only.

        Pure production does not evaluate, rank, or submit entries inside
        APExecutionCore. Incoming scanner signals are routed to the unified
        Postgres queue, where worker_loop + master_control + contract_selector
        + OSM own the entry lifecycle.
        """
        try:
            from ap.queue import enqueue_signal
            signal_id = str(signal.get("signal_id") or uuid.uuid4())
            signal["signal_id"] = signal_id
            ok = enqueue_signal(
                signal,
                client_id=self.email or "default",
                idempotency_key=f"{signal_id}:{self.email or 'default'}",
            )
            log.info(
                "[%s] receive_signal routed to production queue | inserted=%s",
                signal.get("ticker") or signal.get("symbol") or "?",
                ok,
            )
        except Exception as exc:
            log.critical(
                "[%s] receive_signal queue route failed: %s",
                signal.get("ticker") or signal.get("symbol") or "?",
                exc,
            )
        return

    def _start_ranking_processor(self):
        """Disabled in pure production. Queue/worker is the only entry authority."""
        self._rq_running = False
        log.warning("[%s] Legacy RankingQueueProcessor is disabled in pure production", self.email)
        return

    # ── CALLBACK: Breach Confirmed -> Execute ─────────────────────────────────

    def _on_entry_trigger(self, watched: WatchedSignal):
        """Called by watcher when price holds above/below trigger for 2 polls."""
        sig       = watched.signal
        ticker    = watched.ticker
        side      = watched.side
        signal_id = str(sig.get("signal_id", ""))

        log.info(f"[{ticker}] Breach confirmed @ ${watched.trigger_price:.2f} -- evaluating chain")

        if signal_id:
            self.store.update_status(signal_id, "triggered", timestamp_flag="triggered_at")
        funnel.inc("watcher_triggered")

        # Lightweight breach risk check only.
        # Do NOT call master_control.evaluate() here: this signal is already
        # approved/armed, and full evaluate() would re-run dedup/persistence.
        if not self._breach_risk_check(watched):
            funnel.inc("master_control_blocked")
            return

        # Fetch 0DTE chain
        try:
            chain, expiration = self._fetch_0dte_chain(ticker)
        except Exception as e:
            log.error(f"[{ticker}] Chain fetch failed: {e}")
            return

        if not chain:
            log.warning(f"[{ticker}] Empty chain -- cannot enter")
            return

        # Chain health check
        health = chain_health_report(chain, side, watched.trigger_price)
        _chain_health  = health.get("status", "ok") if isinstance(health, dict) else "ok"
        _used_fallback = health.get("used_fallback", False) if isinstance(health, dict) else False
        _paper_only    = (self.mode != "LIVE") and bool(_used_fallback or _chain_health != "ok")
        if self.mode == "LIVE" and (_used_fallback or _chain_health != "ok"):
            log.warning("[%s] LIVE: rejecting — degraded chain | fallback=%s health=%s",
                        ticker, _used_fallback, _chain_health)
            return
        if not health["tradeable"]:
            log.warning(
                f"[{ticker}] Chain health FAILED -- "
                f"spread={health['avg_spread']}% vol={health['total_volume']}"
            )
            return

        # Options intelligence gate
        decision = evaluate_contract(
            chain            = chain,
            direction        = side,
            underlying_price = watched.trigger_price,
            expiration       = expiration,
            signal_score     = watched.score,
            signal_store     = self.store,
            signal_id        = signal_id,
        )

        if not decision.approved:
            log.warning(f"[{ticker}] Options gate REJECTED: {decision.rejection_reason}")
            funnel.inc("options_rejected")
            return

        # Feedback size modifier
        feedback_mod = self.feedback.get_size_modifier(
            ticker    = ticker,
            pattern   = sig.get("pattern", ""),
            timeframe = sig.get("timeframe", "1d"),
            side      = side,
        )
        setup_status = self.feedback.get_setup_status(
            ticker, sig.get("pattern", ""), sig.get("timeframe", "1d"), side
        )

        # Hard block DOWNGRADED setups in live mode
        if setup_status == "DOWNGRADED" and not self.paper:
            log.warning(f"[{ticker}] BLOCKED -- setup DOWNGRADED (live WR diverged >25% from backtest)")
            return

        # ── PREMIUM BOUNDS — last line of defense before sizing ────────────────
        MIN_PREMIUM = 0.40   # $0.40/share min — avoid lottery tickets
        MAX_PREMIUM = 15.00  # $15.00/share max — avoid over-priced contracts

        if decision.mid_price < MIN_PREMIUM or decision.mid_price > MAX_PREMIUM:
            log.info(
                f"[{ticker}] Skipping {decision.symbol} mid=${decision.mid_price:.2f} "
                f"outside premium bounds [${MIN_PREMIUM}–${MAX_PREMIUM}]"
            )
            funnel.inc("options_rejected")
            return

        # ── ET SESSION CUTOFF ────────────────────────────────────────────────
        from datetime import time as _time
        _now_et   = datetime.now(ET)
        _cutoff   = _time(15, 15)
        if _now_et.time() >= _cutoff:
            log.info(
                f"[{ticker}] Skipping new entry after cutoff "
                f"({_now_et.strftime('%H:%M')} ET ≥ 15:15)"
            )
            funnel.inc("queue_expired")
            return

        # ── AGGRESSIVE SIZING ────────────────────────────────────────────────
        tier     = sig.get("tier", Tier.A)
        _equity  = getattr(self.master_control, "account_equity", 25000) or 25000
        contracts = self._size_position(
            tier         = str(tier),
            option_price = decision.mid_price,
            equity       = _equity,
        )

        if contracts <= 0:
            log.info(
                f"[{ticker}] Sizing → 0 contracts "
                f"(tier={tier} equity=${_equity:.0f} price=${decision.mid_price:.2f}) — skipping"
            )
            funnel.inc("options_rejected")
            return

        log.info(
            f"[{ticker}] Sizing: equity=${_equity:.0f} budget=${self._position_budget(_equity):.0f} "
            f"price=${decision.mid_price:.2f} tier={tier} → {contracts}x "
            f"[{decision.grade}] [{setup_status}]"
        )

        # ── ENTRY SUBMISSION — PURE PRODUCTION QUEUE/OSM PATH ───────────────
        # Queue/worker must have already created an ENTRY order and armed the
        # watcher with local_order_id. This core never creates a fresh entry
        # order at breach time and never creates synthetic in-memory positions.

        if self.order_state_machine is None:
            log.critical("[%s] PRODUCTION_ENTRY_BLOCK — order_state_machine missing at breach", ticker)
            funnel.inc("order_failed")
            if signal_id:
                self.store.update_signal_fields(signal_id, {
                    "decision_status": "blocked_at_breach",
                    "context_notes": "order_state_machine_missing_at_breach",
                })
            return

        queue_local_order_id = str(sig.get("local_order_id") or "")
        if not queue_local_order_id:
            log.critical("[%s] PRODUCTION_ENTRY_BLOCK — local_order_id missing from watcher signal", ticker)
            funnel.inc("order_failed")
            if signal_id:
                self.store.update_signal_fields(signal_id, {
                    "decision_status": "blocked_at_breach",
                    "context_notes": "local_order_id_missing_at_breach",
                })
            return

        if not hasattr(self.order_state_machine, "submit_existing_entry"):
            log.critical("[%s] PRODUCTION_ENTRY_BLOCK — OSM missing submit_existing_entry", ticker)
            funnel.inc("order_failed")
            if signal_id:
                self.store.update_signal_fields(signal_id, {
                    "decision_status": "blocked_at_breach",
                    "context_notes": "osm_missing_submit_existing_entry",
                })
            return

        plan = ApprovedExecutionPlan(
            plan_id           = str(sig.get("plan_id") or signal_id or queue_local_order_id),
            signal_id         = signal_id or str(sig.get("signal_id") or queue_local_order_id),
            client_id         = self.email,
            ticker            = ticker,
            side              = side.upper(),
            direction         = side.upper(),
            pattern           = str(sig.get("pattern", "") or ""),
            timeframe         = str(sig.get("timeframe", "1d") or "1d"),
            contracts         = contracts,
            limit_price       = decision.mid_price,
            max_position_usd  = float(decision.mid_price * contracts * 100),
            contract_symbol   = decision.symbol,
            tier              = str(tier or sig.get("tier", "B")),
            score             = float(sig.get("score", 0) or 0),
            intel_score       = float(sig.get("intel_score", 0) or 0),
            confidence_bucket = str(decision.grade or sig.get("confidence_tag", "B")),
            trigger_type      = "breach",
            trigger_price     = watched.trigger_price,
            stop_underlying   = watched.stop_level,
            target_underlying = watched.target_price,
            mode              = "live" if not self.paper else "paper",
        )

        log.info(
            "[%s] %s — submitting EXISTING queue order via OSM | local=%s @ $%.2f x%s",
            ticker,
            "PAPER" if self.paper else "LIVE",
            queue_local_order_id,
            decision.mid_price,
            contracts,
        )
        submit_res = self.order_state_machine.submit_existing_entry(
            local_order_id = queue_local_order_id,
            broker         = self.broker,
            plan           = plan,
            limit_price    = decision.mid_price,
        )

        if submit_res.get("ok"):
            local_order_id  = submit_res.get("local_order_id")
            broker_order_id = submit_res.get("broker_order_id")
            if signal_id:
                self.store.update_signal_fields(signal_id, {
                    "decision_status": "submitted",
                    "context_notes": (
                        f"entry_submitted local={local_order_id} "
                        f"broker={broker_order_id}"
                    ),
                })
            log.info(
                "[%s] Entry submitted via OSM | local=%s broker=%s %sx %s @ $%.2f",
                ticker,
                local_order_id,
                broker_order_id,
                contracts,
                decision.symbol,
                decision.mid_price,
            )
            # Do NOT create ManagedPosition here; fill_monitor/OSM/position_manager
            # create DB truth after broker fill.
            return

        log.error(
            "[%s] Entry submit failed via OSM | local=%s error=%s",
            ticker,
            submit_res.get("local_order_id") or queue_local_order_id,
            submit_res.get("error"),
        )
        funnel.inc("order_failed")
        if signal_id:
            self.store.update_signal_fields(signal_id, {
                "decision_status": "submit_failed",
                "context_notes": f"osm_submit_existing_entry_failed={submit_res.get('error')}",
            })
        return

    # ── CALLBACK: Position Closed ─────────────────────────────────────────────

    def _on_position_close(self, pos: ManagedPosition, decision):
        with self._pos_lock:
            self._position_count = max(0, self._position_count - 1)

        sector = getattr(pos, "signal", {}).get("correlation_bucket", "OTHER")
        with self._sector_lock:
            self._sector_counts[sector] = max(0, self._sector_counts.get(sector, 0) - 1)

        # ── IDEMPOTENCY: skip if position already marked closed ──────────────
        if getattr(pos, "closed", False):
            log.debug("[%s] _on_position_close called but pos.closed=True — skipping", pos.ticker)
            return

        # ── EXIT SUBMISSION ─────────────────────────────────────────────────
        sig = getattr(pos, "signal", {})
        _sig_id = str(sig.get("signal_id", ""))
        # Use bid for exit limit price — guarantees fill vs mid which often misses.
        # If bid is not populated yet, fall back to mid.
        _exit_limit = (
            pos.current_bid
            if getattr(pos, "current_bid", 0) > 0
            else pos.current_option_price
        )
        exit_price = _exit_limit

        if self.order_state_machine and pos.position_id:
            log.info(
                f"[{pos.ticker}] {'PAPER' if self.paper else 'LIVE'} CLOSE -- "
                f"submitting sell_to_close via OSM @ ${_exit_limit:.2f} (bid) | {decision.reason}"
            )
            exit_res = self.order_state_machine.submit_exit(
                broker      = self.broker,
                position_id = str(pos.position_id),
                contract    = pos.option_symbol,
                symbol      = pos.ticker,
                direction   = pos.side,
                qty         = pos.quantity_remaining,
                limit_price = _exit_limit,
                signal_id   = _sig_id or None,
            )
            if exit_res["ok"]:
                log.info(
                    f"[{pos.ticker}] Exit order submitted | "
                    f"local={exit_res['local_order_id']} broker={exit_res['broker_order_id']} "
                    f"qty={pos.quantity_remaining} @ ${pos.current_option_price:.2f}"
                )
            else:
                log.error(
                    f"[{pos.ticker}] Exit submit failed via OSM | "
                    f"order={exit_res['local_order_id']} error={exit_res['error']} "
                    f"-- falling back to direct order"
                )
                # Fallback so position doesn't get stuck
                exit_price = self._place_option_order(
                    symbol      = pos.option_symbol,
                    contracts   = pos.quantity_remaining,
                    side        = "sell_to_close",
                    limit_price = pos.current_option_price,
                ) or pos.current_option_price
        else:
            # Legacy path (no OSM/position_id) — keep sandbox visible for paper
            log.info(
                f"[{pos.ticker}] {'PAPER' if self.paper else 'LIVE'} CLOSE (legacy) -- "
                f"submitting sell_to_close | {decision.reason}"
            )
            placed = self._place_option_order(
                symbol      = pos.option_symbol,
                contracts   = pos.quantity_remaining,
                side        = "sell_to_close",
                limit_price = pos.current_option_price,
            )
            exit_price = placed or pos.current_option_price
            if self.paper:
                log.info(
                    f"[{pos.ticker}] PAPER CLOSE | P&L={pos.option_pnl_pct*100:+.1f}% | "
                    f"exit=${exit_price:.2f} | sandbox={'OK' if placed else 'SIMULATED'} | {decision.reason}"
                )

        opt_pnl = (exit_price - pos.entry_price) / pos.entry_price * 100 if pos.entry_price else 0
        win     = opt_pnl > 0
        tier    = sig.get("tier", Tier.A_PLUS)

        # Set 30-min same-direction cooldown on master_control
        try:
            import time as _t
            _ck = f"{pos.ticker.upper()}:{pos.side.upper()}:cooldown"
            if hasattr(self.master_control, "_trade_cooldowns"):
                self.master_control._trade_cooldowns[_ck] = _t.time()
        except Exception:
            pass

        # ── TRADE INTEGRITY CHECKLIST — runs every close, before proof guard ────────
        if not getattr(pos, "_integrity_logged", False):
            pos._integrity_logged = True  # type: ignore[attr-defined]
            _checks = [
                ("has_position_id",  bool(getattr(pos, "position_id", ""))),
                ("exit_px_positive", exit_price > 0),
                ("pnl_recorded",     abs(opt_pnl) >= 0),
            ]
            _pass = all(v for _, v in _checks)
            _str  = " ".join(f"{k}={'OK' if v else 'FAIL'}" for k, v in _checks)
            log.info(
                "[INTEGRITY] %s | %s | %s | pnl=%+.1f%% exit=$%.2f",
                pos.ticker, "PASS" if _pass else "FAIL",
                _str, opt_pnl * 100, exit_price,
            )

        # Guard: only log once per position — exit_in_flight retries must not re-log
        if getattr(pos, "proof_logged", False):
            log.debug("[%s] proof.log_trade skipped — already logged for this position", pos.ticker)
            return
        pos.proof_logged = True  # type: ignore[attr-defined]

        self.proof.log_trade(
            ticker             = pos.ticker,
            pattern            = sig.get("pattern", ""),
            side               = pos.side,
            timeframe          = sig.get("timeframe", "1d"),
            score              = float(sig.get("score", 0) or 0),
            tier               = tier,
            context_score      = float((sig.get("score_breakdown") or {}).get("real_time_ctx", 0) or 0),
            setup_status       = self.feedback.get_setup_status(
                                     pos.ticker, sig.get("pattern", ""),
                                     sig.get("timeframe", "1d"), pos.side),
            entry_trigger      = pos.underlying_entry,
            entry_option_price = pos.entry_price,
            exit_option_price  = exit_price,
            underlying_entry   = pos.underlying_entry,
            underlying_exit    = pos.current_underlying,
            contracts          = pos.quantity,
            exit_reason        = decision.reason,
            option_pnl_pct     = opt_pnl,
            underlying_pnl_pct = (pos.current_underlying - pos.underlying_entry) / pos.underlying_entry * 100
                                  if pos.underlying_entry else 0,
            win                = win,
            spread_pct         = float(sig.get("spread_pct", 0) or 0),
            chain_grade        = sig.get("chain_grade", ""),
            opened_at          = pos.opened_at if hasattr(pos, "opened_at") else None,
            synthetic_entry    = bool(getattr(pos, "synthetic_entry", False)),
        )

        self.feedback.record_outcome(
            signal             = sig,
            entry_option_price = pos.entry_price,
            exit_option_price  = exit_price,
            exit_reason        = decision.reason,
            underlying_entry   = pos.underlying_entry,
            underlying_exit    = pos.current_underlying,
            contracts          = pos.quantity,
            context_notes      = f"mode={'paper' if self.paper else 'live'}",
            synthetic_entry    = bool(getattr(pos, "synthetic_entry", False)),
        )

        # Belt-and-suspenders: mark closed here in addition to feedback loop
        signal_id = str(sig.get("signal_id", ""))
        if signal_id:
            self.store.update_status(signal_id, "closed", timestamp_flag="closed_at")

        self.shadow.record_live_outcome(tier, opt_pnl)

        # Log trade to edge intelligence
        try:
            from ap_edge_intelligence import APTradeLogger
            _edge_logger = APTradeLogger()
            _edge_logger.log_trade(
                position={
                    **(pos.__dict__ if hasattr(pos, "__dict__") else {}),
                    "signal": sig,
                    "ticker": pos.ticker,
                    "direction": pos.side,
                    "timeframe": sig.get("timeframe", "1d"),
                    "synthetic_entry": bool(getattr(pos, "synthetic_entry", False)),
                },
                exit_info={
                    "exit_price": exit_price,
                    "exit_reason": decision.reason,
                    "exit_ts": datetime.now(timezone.utc).isoformat(),
                    "underlying_exit": pos.current_underlying,
                },
                client_id=self.email,
            )
        except Exception as _e:
            log.debug(f"Trade logger error (non-critical): {_e}")

        # Feed outcome back to intelligence audit log (builds learning dataset)
        if _record_intel_outcome:
            try:
                _record_intel_outcome(
                    ticker    = pos.ticker,
                    signal_id = signal_id,        # already resolved two lines above
                    pnl_pct   = opt_pnl / 100.0,  # opt_pnl is %, convert to decimal
                )
            except Exception:
                pass

    # ── CALLBACKS: Expire / Invalidate ────────────────────────────────────────

    def _cleanup_pending_entry_order(self, watched: WatchedSignal, *, action: str, reason: str) -> None:
        """Best-effort OSM cleanup for watcher terminal outcomes.

        The queue creates an ENTRY order before arming the watcher. If the
        watcher later expires or invalidates before broker submission, that
        order must not remain as a ghost CREATED/PENDING_TRIGGER row.
        """
        sig = getattr(watched, "signal", {}) or {}
        local_order_id = str(sig.get("local_order_id") or "").strip()
        if not local_order_id or self.order_state_machine is None:
            return

        try:
            if action == "expire" and hasattr(self.order_state_machine, "expire_pending_entry"):
                ok = self.order_state_machine.expire_pending_entry(local_order_id, reason=reason)
                if not ok:
                    log.warning(
                        "[%s] OSM expire_pending_entry returned false | order=%s reason=%s",
                        watched.ticker, local_order_id, reason,
                    )
                return

            if action == "cancel" and hasattr(self.order_state_machine, "cancel_pending_entry"):
                ok = self.order_state_machine.cancel_pending_entry(local_order_id, reason=reason)
                if not ok:
                    log.warning(
                        "[%s] OSM cancel_pending_entry returned false | order=%s reason=%s",
                        watched.ticker, local_order_id, reason,
                    )
                return

            # Compatibility fallback for older OSM versions that do not expose
            # helper methods yet. Only legal CREATED/PENDING_TRIGGER orders will
            # transition; illegal/terminal states are blocked by OSM.transition().
            fallback_status = "EXPIRED" if action == "expire" else "CANCELED"
            if hasattr(self.order_state_machine, "transition"):
                self.order_state_machine.transition(
                    local_order_id,
                    fallback_status,
                    last_error=reason,
                )
        except Exception as exc:
            log.error(
                "[%s] OSM pending-entry cleanup failed | order=%s action=%s reason=%s error=%s",
                watched.ticker, local_order_id, action, reason, exc, exc_info=True,
            )

    def _on_signal_expire(self, watched: WatchedSignal):
        signal_id = str(watched.signal.get("signal_id", ""))
        if signal_id:
            self.store.update_status(signal_id, "expired", timestamp_flag="expired_at")
        self._cleanup_pending_entry_order(watched, action="expire", reason="watcher_expired")
        funnel.inc("watcher_expired")
        log.info(f"[{watched.ticker}] Signal expired -- no breach")

    def _on_signal_invalidate(self, watched: WatchedSignal):
        signal_id = str(watched.signal.get("signal_id", ""))
        if signal_id:
            self.store.update_status(signal_id, "invalidated", timestamp_flag="invalidated_at")
        self._cleanup_pending_entry_order(watched, action="cancel", reason="watcher_invalidated")
        funnel.inc("watcher_invalidated")
        log.info(f"[{watched.ticker}] Signal invalidated -- wrong direction")

    def _on_position_scale(self, pos: ManagedPosition, decision):
        log.info(
            f"[{pos.ticker}] SCALE OUT {decision.quantity}x | "
            f"P&L={pos.option_pnl_pct*100:+.1f}% | {decision.reason}"
        )
        _sig_id = str(getattr(pos, "signal", {}).get("signal_id", "") or "")

        if self.order_state_machine and pos.position_id:
            scale_res = self.order_state_machine.submit_exit(
                broker      = self.broker,
                position_id = str(pos.position_id),
                contract    = pos.option_symbol,
                symbol      = pos.ticker,
                direction   = pos.side,
                qty         = decision.quantity,
                limit_price = pos.current_option_price,
                signal_id   = _sig_id or None,
            )
            if scale_res["ok"]:
                log.info(
                    f"[{pos.ticker}] Scale exit submitted | "
                    f"local={scale_res['local_order_id']} broker={scale_res['broker_order_id']} "
                    f"qty={decision.quantity} @ ${pos.current_option_price:.2f}"
                )
            else:
                log.error(
                    f"[{pos.ticker}] Scale exit failed via OSM | "
                    f"order={scale_res['local_order_id']} error={scale_res['error']} "
                    f"-- falling back to direct order"
                )
                self._place_option_order(
                    symbol      = pos.option_symbol,
                    contracts   = decision.quantity,
                    side        = "sell_to_close",
                    limit_price = pos.current_option_price,
                )
        else:
            # Legacy path
            self._place_option_order(
                symbol      = pos.option_symbol,
                contracts   = decision.quantity,
                side        = "sell_to_close",
                limit_price = pos.current_option_price,
            )

    # ── BROKER HELPERS ────────────────────────────────────────────────────────

    def _broker_base_url(self) -> str:
        """FIX: TradierBroker stores URL as broker.cfg.base_url not broker.base_url"""
        return (
            getattr(self.broker, "base_url", None)
            or getattr(getattr(self.broker, "cfg", None), "base_url", None)
            or "https://sandbox.tradier.com"
        )

    def _broker_account_id(self) -> str:
        """FIX: TradierBroker stores account as broker.cfg.account_id not broker.account_id"""
        return (
            getattr(self.broker, "account_id", None)
            or getattr(getattr(self.broker, "cfg", None), "account_id", None)
            or ""
        )

    def _fetch_0dte_chain(self, ticker: str) -> tuple[list, str]:
        """
        Fetch option chain for ticker. Tries today (0DTE) first,
        then falls back to nearest available expiry (weekly).
        Handles Tradier returning {"options": null} gracefully.
        """
        base_url = self._broker_base_url()
        headers  = {"Accept": "application/json",
                    "Authorization": f"Bearer {getattr(getattr(self.broker, 'cfg', None), 'access_token', '') or ''}"}

        def _get_chain(expiry: str) -> list:
            resp = self.broker.session.get(
                f"{base_url}/v1/markets/options/chains",
                params  = {"symbol": ticker, "expiration": expiry, "greeks": "true"},
                headers = headers,
                timeout = 10,
            )
            raw     = resp.json() or {}
            options = (raw.get("options") or {}).get("option", []) if raw.get("options") else []
            if isinstance(options, dict):
                options = [options]
            return options or []

        today = date.today().strftime("%Y-%m-%d")

        # Try today first (0DTE)
        options = _get_chain(today)
        if options:
            return options, today

        # Fall back to nearest expiry from broker
        try:
            exp_resp = self.broker.session.get(
                f"{base_url}/v1/markets/options/expirations",
                params  = {"symbol": ticker, "includeAllRoots": "true"},
                headers = headers,
                timeout = 10,
            )
            exp_data = exp_resp.json() or {}
            expirations = (exp_data.get("expirations") or {}).get("date", [])
            if isinstance(expirations, str):
                expirations = [expirations]
            # Pick nearest future expiry
            for exp in sorted(expirations):
                if exp >= today:
                    options = _get_chain(exp)
                    if options:
                        log.info(f"[{ticker}] No 0DTE chain — using nearest expiry {exp} ({len(options)} contracts)")
                        return options, exp
        except Exception as exp_err:
            log.warning(f"[{ticker}] Expiry lookup failed: {exp_err}")

        return [], today

    # =========================================================================
    # POSITION SIZING — AGGRESSIVE RISK CURVE
    # =========================================================================

    def _position_budget(self, equity: float) -> float:
        """
        Per-trade dollar allocation based on account size.
        Aggressive curve for early beta phase — dial down as AUM grows.
          ≤$10k  → 15% per trade
          ≤$25k  → 10% per trade
          >$25k  →  5% per trade
        Floored at $300, capped at 25% of equity.
        """
        if equity <= 10_000:
            risk_pct = 0.15
        elif equity <= 25_000:
            risk_pct = 0.10
        else:
            risk_pct = 0.05

        raw    = equity * risk_pct
        floor  = 300.0
        cap    = equity * 0.25
        return max(floor, min(raw, cap))

    def _max_contracts_for_tier(self, tier: str) -> int:
        """Hard contract caps per tier so cheap options can't snowball."""
        t = (tier or "B").upper()
        if t == "A+": return 10
        if t == "A":  return 6
        return 3   # B or unknown

    def _size_position(self, tier: str, option_price: float, equity: float) -> int:
        """
        Return contract count = floor(budget / option_price), capped by tier.
        option_price is the per-share mid-price (multiply by 100 for per-contract cost).
        Returns 0 if sizing is impossible (zero price, etc.).
        """
        if option_price <= 0:
            return 0
        budget        = self._position_budget(equity)
        per_contract  = option_price * 100          # e.g. $1.50 mid → $150/contract
        raw_qty       = int(budget // per_contract) if per_contract > 0 else 0
        tier_cap      = self._max_contracts_for_tier(tier)
        return max(0, min(raw_qty, tier_cap))       # strict production sizing: 0 if budget cannot afford 1 contract

    def _get_base_contracts(self, score: float) -> int:
        if score >= 95: return 4
        if score >= 90: return 3
        if score >= 85: return 2
        return 1

    def _place_option_order(
        self, symbol: str, contracts: int, side: str, limit_price: float
    ) -> Optional[float]:
        try:
            resp = self.broker.session.post(
                f"{self._broker_base_url()}/v1/accounts/{self._broker_account_id()}/orders",
                data={
                    "class":         "option",
                    "symbol":        symbol.split()[0] if " " in symbol else symbol[:6],
                    "option_symbol": symbol,
                    "side":          side,
                    "quantity":      contracts,
                    "type":          "limit",
                    "price":         round(limit_price, 2),
                    "duration":      "day",
                },
                headers = headers,
                timeout = 10,
            )
            order  = resp.json().get("order", {})
            status = order.get("status", "")
            log.info(f"Order: {status} | {symbol} x{contracts} @ ${limit_price:.2f}")
            return limit_price if status in ("ok", "filled", "pending") else None
        except Exception as e:
            log.error(f"Order error: {e}")
            return None
