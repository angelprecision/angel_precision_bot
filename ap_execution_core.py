# ap_execution_core.py -- Angel Precision Execution Core
# =============================================================================
# Ties all execution modules together into one clean interface.
# One instance per client (per ClientRunner thread).
#
# Signal flow:
#   receive_signal()
#     -> score gate (85 live / 75 paper)
#     -> context hard block (context < 12/20 live / 8/20 paper -> reject)
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
        self.entry_watcher = APEntryWatcher(broker)
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

    def start(self):
        """Start all background threads."""
        self.entry_watcher.start()
        self.exit_eng.start()
        self.tracker.start()
        self._start_ranking_processor()
        log.info(f"[{self.email}] Execution core started (watcher + exit engine + tracker + ranking queue)")

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
        """
        Entry point for all scanner signals.
        Routes through APMasterControl -- the single decision authority.
        Master control handles: gates, intelligence, tier, sizing, plan creation.
        Execution core only dispatches the approved plan.
        """
        ticker = signal.get("ticker", "")
        score  = float(signal.get("score", 0) or 0)

        # Assign signal_id immediately
        signal_id = str(signal.get("signal_id") or uuid.uuid4())
        signal["signal_id"] = signal_id

        log.info(
            f"[{ticker}] Signal received | "
            f"{signal.get('pattern')} {signal.get('side')} [{signal.get('timeframe')}] | "
            f"Score={score:.1f}"
        )

        funnel.inc("signals_received")

        # ── LEGACY FALLBACK ───────────────────────────────────────────────────
        # Old scanners send symbol/direction/pattern_id but no score or ev_score.
        # Route to legacy queue. Insert ap_signals row so traffic is visible.
        # Master control does NOT run on legacy signals.
        is_legacy = (
            score == 0 and (
                signal.get("signal_id")
                or signal.get("symbol")
                or signal.get("pattern_id")
            ) and not signal.get("ev_score")
        )
        if is_legacy:
            legacy_ticker = signal.get("symbol") or signal.get("ticker", "?")
            log.info(f"[{legacy_ticker}] LEGACY SIGNAL -- routing to legacy queue (no score)")
            self.store.insert_signal(signal_id, signal, decision_status="legacy_routed")
            try:
                from ap.queue import enqueue_signal
                from ap.models import Signal
                legacy_sig = Signal(
                    signal_id      = signal.get("signal_id", str(uuid.uuid4())),
                    symbol         = signal.get("symbol", signal.get("ticker", "")),
                    direction      = signal.get("direction", signal.get("side", "CALL")),
                    pattern_id     = signal.get("pattern_id", "SCANNER_V1"),
                    confidence_tag = signal.get("confidence_tag", "standard_pool"),
                    timestamp_iso  = signal.get("timestamp_iso", datetime.now(timezone.utc).isoformat()),
                    trigger        = signal.get("trigger", signal),
                )
                enqueue_signal(legacy_sig, client_id="default")
                log.info(f"[{legacy_ticker}] Legacy signal queued successfully")
            except Exception as e:
                log.warning(f"[{legacy_ticker}] Legacy queue failed: {e} -- signal dropped")
            return

        # Apply paper exec fallbacks before master control evaluates
        signal    = self._apply_paper_exec_fallbacks(signal)
        score     = float(signal.get("score", 0) or 0)
        signal_id = str(signal.get("signal_id", signal_id))  # preserve after deepcopy

        # Insert into ap_signals immediately so every signal is visible
        self.store.insert_signal(signal_id, signal, decision_status="received")

        # ── MASTER CONTROL -- single decision authority ────────────────────────
        decision = self.master_control.evaluate(signal, client_id=self.email or "default")

        if not decision.ok:
            if decision.stage == "shadow":
                if score_result:
                    td = self.tier_engine.classify(score_result)
                    self.shadow.log_shadow(signal, td)
                funnel.inc("shadow_tracked")
                log.info(f"[{ticker}] SHADOW-TIER | score={score:.1f}")
            else:
                funnel.inc("rejected_score")
            return

        # Plan approved -- attach tier and dispatch to ranking queue
        plan = decision.plan
        signal["tier"]         = plan.tier
        signal["auto_execute"] = (plan.tier == "A+")
        funnel.inc("passed_score")
        funnel.inc("passed_context")
        self.rank_queue.add(signal)
        self.store.update_status(signal_id, "queued", timestamp_flag="queued_at")
        log.info(
            f"[{ticker}] {plan.tier}-TIER queued | score={score:.1f} "
            f"contracts={plan.contracts} | "
            f"{'1 contract probation' if plan.tier == 'B' else 'full execution'}"
        )
        return

        # ── LEGACY GATE CODE -- now owned by APMasterControl (kept for reference) ──
        score_floor, _ = self._score_and_context_floors()
        if score < score_floor:
            log.info(
                f"[{ticker}] REJECTED -- score {score:.1f} below "
                f"{'paper' if self.paper else 'live'} floor ({score_floor})"
            )
            funnel.inc("rejected_score")
            self.store.update_status(signal_id, "rejected",
                context_notes=f"score {score:.1f} below floor {score_floor}")
            return
        funnel.inc("passed_score")

        # ── Gate 2: Context hard block ────────────────────────────────────────
        # Fix D (corrected): check that real_time_ctx is actually present in
        # score_breakdown before attempting to gate on it.
        #
        # Why this check is required:
        #   _apply_paper_exec_fallbacks injects spread_score + liquidity_score
        #   into score_breakdown, making it non-empty even when the scanner did
        #   not send real_time_ctx. Checking only bool(score_breakdown) would
        #   enter the gate with ctx=0.0 and block every signal from a scanner
        #   that does not emit a context score. This check ensures the gate only
        #   fires when the scanner explicitly computed and sent real_time_ctx.
        score_breakdown = signal.get("score_breakdown")
        has_breakdown   = bool(score_breakdown and "real_time_ctx" in score_breakdown)

        if not has_breakdown:
            log.info(
                f"[{ticker}] Context gate SKIPPED -- real_time_ctx not in score_breakdown"
            )
        else:
            context_score = float(score_breakdown.get("real_time_ctx", 0) or 0)
            context_floor = self._context_floor
            if context_score < context_floor:
                log.info(
                    f"[{ticker}] CONTEXT BLOCKED -- context={context_score:.1f}/20 "
                    f"(floor={context_floor} {'paper' if self.paper else 'live'} | "
                    f"score={score:.1f} but tape is wrong)"
                )
                funnel.inc("context_blocked")
                self.store.update_status(signal_id, "context_blocked",
                    context_notes=f"ctx={context_score:.1f} below floor {context_floor}")
                return
            log.debug(f"[{ticker}] Context OK: {context_score:.1f}/20 (floor={context_floor})")
        funnel.inc("passed_context")

        # ── Tier classify ─────────────────────────────────────────────────────
        tier = Tier.from_score(score)

        # ── SHADOW tier: paper track only, no capital ────────────────────────
        # FIX: B-tier (78-84) now EXECUTES at 1 contract in paper mode.
        #      Only SHADOW tier (75-77) is parked without live capital.
        if tier == Tier.SHADOW:
            if score_result:
                td = self.tier_engine.classify(score_result)
                self.shadow.log_shadow(signal, td)
            funnel.inc("shadow_tracked")
            self.store.update_status(signal_id, "shadow")
            log.info(f"[{ticker}] SHADOW-TIER -- paper tracked only (score={score:.1f})")
            return

        # ── Hard reject ───────────────────────────────────────────────────────
        if tier == Tier.REJECT:
            log.info(f"[{ticker}] REJECTED -- score {score:.1f}")
            self.store.update_status(signal_id, "rejected",
                context_notes=f"tier=REJECT score={score:.1f}")
            return

        # ── A+ / A / B: add to ranking queue ─────────────────────────────────
        # B-tier executes at 1 contract (30% size) -- probation tier
        signal["tier"]         = tier
        signal["auto_execute"] = (tier == Tier.A_PLUS)
        self.rank_queue.add(signal)
        self.store.update_status(signal_id, "queued", timestamp_flag="queued_at")
        log.info(f"[{ticker}] {tier}-TIER queued (score={score:.1f}) -- {'1 contract probation' if tier == Tier.B else 'full execution'}")

    # ── RANKING QUEUE PROCESSOR ───────────────────────────────────────────────

    def _start_ranking_processor(self):
        """Background thread: drains ranking queue into watcher every 5 seconds."""
        self._rq_running = True

        def _loop():
            while self._rq_running:
                try:
                    slots = max(0, self._max_positions - self._position_count)
                    if slots > 0 and self.rank_queue.size() > 0:
                        signals = self.rank_queue.drain(slots)
                        for sig in signals:
                            tkr       = sig.get("ticker", "")
                            score     = float(sig.get("score", 0))
                            signal_id = str(sig.get("signal_id", ""))

                            if slots <= 0:
                                log.info(f"[{tkr}] No slots left -- re-queuing remaining signals")
                                self.rank_queue.add(sig)
                                continue
                            slots -= 1

                            # Sector correlation cap
                            sector = sig.get("correlation_bucket", "OTHER")
                            with self._sector_lock:
                                sector_count = self._sector_counts.get(sector, 0)
                            # FIX: raised OTHER cap to 7 -- all scanner tickers land in OTHER
                            SECTOR_MAX = {"SEMI": 3, "MEGACAP": 3, "INDEX": 3,
                                          "FINANCIAL": 3, "BIO": 2, "CLOUD": 3}.get(sector, 7)
                            if sector_count >= SECTOR_MAX:
                                log.info(
                                    f"[{tkr}] SECTOR CAP -- {sector} already has "
                                    f"{sector_count}/{SECTOR_MAX} active. Skipping."
                                )
                                funnel.inc("sector_capped")
                                if signal_id:
                                    self.store.update_signal_fields(signal_id, {
                                        "decision_status": "dropped",
                                        "context_notes":   f"sector_cap: {sector} at {sector_count}/{SECTOR_MAX}",
                                    })
                                continue

                            # Re-check context before dispatching to watcher.
                            # Signal may have sat in queue for up to 120s.
                            # Only re-check if real_time_ctx is actually present.
                            stale_breakdown = sig.get("score_breakdown")
                            if stale_breakdown and "real_time_ctx" in stale_breakdown:
                                stale_ctx = float(stale_breakdown.get("real_time_ctx", 0) or 0)
                                if stale_ctx < self._context_floor and stale_ctx > 0:
                                    log.info(
                                        f"[{tkr}] Context re-check failed after queue wait "
                                        f"(ctx={stale_ctx:.1f} < floor={self._context_floor}) -- skipping"
                                    )
                                    if signal_id:
                                        self.store.update_signal_fields(signal_id, {
                                            "decision_status": "dropped",
                                            "context_notes":   f"context_recheck_fail: ctx={stale_ctx:.1f} floor={self._context_floor}",
                                        })
                                    continue

                            added = self.entry_watcher.add_signal(sig)
                            if added:
                                funnel.inc("watcher_sent")
                                self.store.update_status(
                                    signal_id, "watching",
                                    timestamp_flag="watcher_started_at",
                                )
                                with self._sector_lock:
                                    self._sector_counts[sector] = self._sector_counts.get(sector, 0) + 1
                                log.info(
                                    f"[{tkr}] Dispatched from ranking queue -> watcher | "
                                    f"score={score:.1f} [{sig.get('grade')}] | "
                                    f"sector={sector} ({sector_count+1}/{SECTOR_MAX}) | "
                                    f"slots left: {slots}"
                                )
                            else:
                                slots += 1
                                log.info(f"[{tkr}] Watcher rejected (EOD or duplicate) -- slot returned")
                except Exception as e:
                    log.error(f"Ranking queue processor error: {e}")
                time.sleep(5)

        t = threading.Thread(target=_loop, daemon=True, name=f"rq-processor-{self.email}")
        t.start()
        log.info(f"[{self.email}] RankingQueueProcessor started (5s interval)")

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

        # Re-check position count at breach time
        if self._position_count >= self._max_positions:
            log.info(
                f"[{ticker}] No slot at breach time -- positions full "
                f"({self._position_count}/{self._max_positions}). Re-queuing signal."
            )
            if signal_id:
                self.store.update_signal_fields(signal_id, {
                    "decision_status": "requeued_after_trigger",
                    "context_notes":   f"positions_full={self._position_count}/{self._max_positions} at breach",
                })
            self.rank_queue.add(sig)
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

        # Size: base x spread modifier x feedback modifier x tier multiplier
        tier      = sig.get("tier", Tier.A)
        if tier == Tier.B:
            contracts = 1   # B-tier: always 1 contract, no scaling
            base      = 1
            tier_mult = 0.30
        else:
            tier_mult = 1.0 if tier == Tier.A_PLUS else 0.6
            base      = self._get_base_contracts(watched.score)
            contracts = max(1, round(base * decision.size_modifier * feedback_mod * tier_mult))

        log.info(
            f"[{ticker}] Sizing: base={base} x spread={decision.size_modifier:.2f} "
            f"x feedback={feedback_mod:.2f} x tier={tier_mult:.1f} -> {contracts}x "
            f"[{decision.grade}] [{setup_status}]"
        )

        # ── ENTRY SUBMISSION ─────────────────────────────────────────────────
        # Primary path: route through OSM. This creates DB row + transitions
        # lifecycle. We do NOT mint a position here on success; that belongs
        # to the FILLED transition path via fill monitor / reconciler.

        if self.order_state_machine:
            plan = ApprovedExecutionPlan(
                plan_id           = signal_id,
                signal_id         = signal_id,
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
                f"[{ticker}] {'PAPER' if self.paper else 'LIVE'} -- "
                f"submitting entry via OSM @ ${decision.mid_price:.2f} x{contracts}"
            )
            submit_res = self.order_state_machine.submit_entry(
                broker      = self.broker,
                plan        = plan,
                limit_price = decision.mid_price,
            )

            if submit_res["ok"]:
                local_order_id  = submit_res["local_order_id"]
                broker_order_id = submit_res["broker_order_id"]

                if signal_id:
                    self.store.update_signal_fields(signal_id, {
                        "decision_status": "submitted",
                        "context_notes": (
                            f"entry_submitted local={local_order_id} "
                            f"broker={broker_order_id}"
                        ),
                    })

                log.info(
                    f"[{ticker}] Entry submitted via OSM | "
                    f"local={local_order_id} broker={broker_order_id} "
                    f"{contracts}x {decision.symbol} @ ${decision.mid_price:.2f}"
                )
                # Do NOT create ManagedPosition here; wait for FILLED.
                return

            # OSM / broker submission failed
            if self.paper:
                # Paper continuity: sandbox may be unavailable — simulate fill
                log.warning(
                    f"[{ticker}] OSM/sandbox order failed — using simulated fill "
                    f"@ ${decision.mid_price:.2f} (paper continuity) | "
                    f"error={submit_res['error']}"
                )
                fill_price      = decision.mid_price
                local_order_id  = submit_res.get("local_order_id")
                broker_order_id = submit_res.get("broker_order_id")
                synthetic       = True
            else:
                log.error(
                    f"[{ticker}] Entry submit failed via OSM | "
                    f"order={submit_res['local_order_id']} error={submit_res['error']}"
                )
                funnel.inc("order_failed")
                return
        else:
            # Legacy path (no OSM wired) — paper keeps sandbox continuity
            log.info(
                f"[{ticker}] {'PAPER' if self.paper else 'LIVE'} -- "
                f"submitting entry (legacy) @ ${decision.mid_price:.2f}"
            )
            fill_price = self._place_option_order(
                symbol      = decision.symbol,
                contracts   = contracts,
                side        = "buy_to_open",
                limit_price = decision.mid_price,
            )
            synthetic       = False
            local_order_id  = None
            broker_order_id = None

            if not fill_price:
                if self.paper:
                    log.warning(
                        f"[{ticker}] Tradier sandbox order failed — falling back to simulated fill "
                        f"@ ${decision.mid_price:.2f} (paper continuity)"
                    )
                    fill_price = decision.mid_price
                    synthetic  = True
                else:
                    log.error(f"[{ticker}] Order failed -- no fill price returned")
                    funnel.inc("order_failed")
                    return

        # If we reach here, we are in a simulated or legacy fill path only.
        if not fill_price:
            log.error(f"[{ticker}] Order failed -- no fill price")
            funnel.inc("order_failed")
            return

        # Register with exit engine using simulated/legacy fill
        pos = ManagedPosition(
            ticker            = ticker,
            option_symbol     = decision.symbol,
            side              = side,
            quantity          = contracts,
            entry_price       = fill_price,
            underlying_entry  = watched.trigger_price,
            underlying_target = watched.target_price,
            underlying_stop   = watched.stop_level,
            is_trend_day      = bool(sig.get("is_trend_day", False)),
            trend_direction   = sig.get("spy_trend", "neutral"),
        )
        pos.current_option_price = fill_price
        pos.current_underlying   = watched.trigger_price
        pos.signal               = sig  # type: ignore[attr-defined]
        pos.synthetic_entry      = synthetic  # type: ignore[attr-defined]
        if local_order_id:
            pos.local_order_id  = local_order_id   # type: ignore[attr-defined]
        if broker_order_id:
            pos.broker_order_id = broker_order_id  # type: ignore[attr-defined]

        self.exit_eng.add_position(pos)

        with self._pos_lock:
            self._position_count += 1

        if signal_id:
            _status = "executed_synthetic" if synthetic else "executed"
            self.store.update_status(signal_id, _status, timestamp_flag="executed_at")

        self.proof.log_position_opened(
            ticker          = ticker,
            side            = side,
            tier            = sig.get("tier", "A"),
            score           = watched.score,
            contracts       = contracts,
            synthetic_entry = bool(getattr(pos, "synthetic_entry", False)),
        )

        log.info(
            f"[{ticker}] {'PAPER' if self.paper else 'LIVE'} OPEN "
            f"(synthetic={synthetic}) | "
            f"{contracts}x {decision.symbol} @ ${fill_price:.2f} | "
            f"target=${watched.target_price} stop=${watched.stop_level} | "
            f"tier={tier} score={watched.score:.0f}"
            + (f" | local={local_order_id} broker={broker_order_id}" if local_order_id else "")
        )

    # ── CALLBACK: Position Closed ─────────────────────────────────────────────

    def _on_position_close(self, pos: ManagedPosition, decision):
        with self._pos_lock:
            self._position_count = max(0, self._position_count - 1)

        sector = getattr(pos, "signal", {}).get("correlation_bucket", "OTHER")
        with self._sector_lock:
            self._sector_counts[sector] = max(0, self._sector_counts.get(sector, 0) - 1)

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
                client_id=self._email,
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

    def _on_signal_expire(self, watched: WatchedSignal):
        signal_id = str(watched.signal.get("signal_id", ""))
        if signal_id:
            self.store.update_status(signal_id, "expired", timestamp_flag="expired_at")
        funnel.inc("watcher_expired")
        log.info(f"[{watched.ticker}] Signal expired -- no breach")

    def _on_signal_invalidate(self, watched: WatchedSignal):
        signal_id = str(watched.signal.get("signal_id", ""))
        if signal_id:
            self.store.update_status(signal_id, "invalidated", timestamp_flag="invalidated_at")
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
                headers = {"Accept": "application/json"},
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
                headers = {"Accept": "application/json"},
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
                headers = {"Accept": "application/json"},
                timeout = 10,
            )
            order  = resp.json().get("order", {})
            status = order.get("status", "")
            log.info(f"Order: {status} | {symbol} x{contracts} @ ${limit_price:.2f}")
            return limit_price if status in ("ok", "filled", "pending") else None
        except Exception as e:
            log.error(f"Order error: {e}")
            return None
