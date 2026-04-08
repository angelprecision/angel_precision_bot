# ap_execution_core.py — Angel Precision Execution Core
# =============================================================================
# Ties all execution modules together into one clean interface.
# One instance per client (per ClientRunner thread).
#
# Signal flow:
#   receive_signal()
#     → score gate (85 live / 75 paper)
#     → context hard block (context < 12/20 live / 8/20 paper → reject)
#     → tier classify (A+/A/B)
#     → B tier → shadow tracker (paper only)
#     → A/A+ → RankingQueue (signals compete by score)
#         → every 5s: highest scores fill available slots first
#         → EntryWatcher parks signal → waits for breach (2 polls)
#         → breach confirmed → options intelligence gate
#         → order placed → ExitEngine monitors position
#         → position closed → FeedbackLoop records outcome
#
# PAPER MODE FIXES (2025-01 patch):
#   Fix A: Fallback exec quality scores when no real chain data (paper only)
#   Fix B: Score floor lowered to 75 in paper mode (85 live)
#   Fix C: Context floor lowered to 8.0 in paper mode (12.0 live)
#   Fix D: Skip context gate entirely when score_breakdown is missing/empty
#
# SIGNAL INTELLIGENCE (2025-01 patch):
#   Every signal is now a first-class persistent object in ap_signals.
#   Full lifecycle tracked: received → queued → watching → triggered → executed → closed
#   Dropped signals record WHY: rejected / context_blocked / shadow /
#   sector_cap / context_recheck_fail / requeued_after_trigger / legacy_routed
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

log = logging.getLogger("ap.execution_core")
ET  = ZoneInfo("America/New_York")

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
BOT_MODE            = os.getenv("BOT_MODE", "PAPER").upper()
MAX_POSITIONS       = int(os.getenv("MAX_POSITIONS", "7"))

# ── Paper-mode gate thresholds ────────────────────────────────────────────────
SCORE_FLOOR_LIVE    = 85
SCORE_FLOOR_PAPER   = 75
CONTEXT_FLOOR_LIVE  = 12.0
CONTEXT_FLOOR_PAPER = 8.0

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
                        f"(score {existing.get('score',0):.0f} → {signal.get('score',0):.0f})"
                    )
                else:
                    log.debug(f"[{signal.get('ticker')}] Kept existing higher-score signal in queue")
                    return
            self._queue.append(signal)
            self._queue.sort(key=lambda s: float(s.get("score", 0)), reverse=True)
        log.info(
            f"[{signal.get('ticker')}] Added to ranking queue "
            f"score={signal.get('score', 0):.1f} [{signal.get('grade')}] — "
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

    def available_slots(self, position_count: int) -> int:
        return max(0, MAX_POSITIONS - position_count)


# =============================================================================
# EXECUTION CORE
# =============================================================================

class APExecutionCore:
    """
    One instance per client (per ClientRunner thread).
    Manages full lifecycle: signal → rank → watch → enter → manage → exit → record.
    """

    def __init__(self, broker, supabase_client=None, email: str = ""):
        self.broker    = broker
        self.email     = email
        self.paper     = BOT_MODE != "LIVE"
        self._pos_lock = threading.Lock()
        self._position_count = 0

        # Mode-specific gate values
        self._score_floor   = SCORE_FLOOR_PAPER   if self.paper else SCORE_FLOOR_LIVE
        self._context_floor = CONTEXT_FLOOR_PAPER if self.paper else CONTEXT_FLOOR_LIVE

        # Signal intelligence store + tracker
        self.store   = APSignalStore(supabase_client, client_email=email)
        self.tracker = APSignalTracker(supabase_client, store=self.store)

        # Core modules
        self.watcher     = APEntryWatcher(broker)
        self.exit_eng    = APExitEngine(broker)
        self.feedback    = APFeedbackLoop(supabase_client, DISCORD_WEBHOOK_URL, signal_store=self.store)
        self.tier_engine = APTierEngine()
        self.shadow      = APShadowTracker(supabase_client, DISCORD_WEBHOOK_URL)
        self.rank_queue  = RankingQueue()
        self._sector_counts: dict[str, int] = {}
        self._sector_lock   = threading.Lock()
        self.proof          = APProofLogger(
            supabase_client=supabase_client,
            client_email=email,
            mode="paper" if BOT_MODE != "LIVE" else "live",
        )

        # Wire watcher callbacks
        self.watcher.on_trigger    = self._on_entry_trigger
        self.watcher.on_expire     = self._on_signal_expire
        self.watcher.on_invalidate = self._on_signal_invalidate

        # Wire exit callbacks
        self.exit_eng.on_exit  = self._on_position_close
        self.exit_eng.on_scale = self._on_position_scale

        log.info(
            f"APExecutionCore initialized for {email} | "
            f"Mode: {'PAPER' if self.paper else 'LIVE'} | "
            f"MaxPos: {MAX_POSITIONS} | "
            f"ScoreFloor: {self._score_floor} | "
            f"ContextFloor: {self._context_floor}"
        )

    def start(self):
        """Start all background threads."""
        self.watcher.start()
        self.exit_eng.start()
        self.tracker.start()
        self._start_ranking_processor()
        log.info(f"[{self.email}] Execution core started (watcher + exit engine + tracker + ranking queue)")

    def stop(self):
        self.watcher.stop()
        self.exit_eng.stop()
        self.tracker.stop()
        self._rq_running = False

    # ── HELPER: apply paper-mode exec quality fallbacks ───────────────────────

    def _apply_paper_exec_fallbacks(self, signal: dict) -> dict:
        """
        Fix A: In paper mode, when exec quality sub-scores are 0/absent,
        inject minimum viable defaults so composite score isn't crushed.
        Only mutates a copy — never alters the original signal dict.
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
                injected = PAPER_SPREAD_DEFAULT + PAPER_LIQUIDITY_DEFAULT
                sig["score"] = round(raw_score + injected, 2)
                sig["_paper_exec_boost"] = injected
                log.debug(
                    f"[{sig.get('ticker')}] Paper score boosted: "
                    f"{raw_score:.1f} → {sig['score']:.1f} (+{injected:.1f} exec fallback)"
                )

        return sig

    # ── PUBLIC: receive incoming scanner signal ───────────────────────────────

    def receive_signal(self, signal: dict, score_result=None):
        """
        Entry point for all scanner signals.
        Gates → tier classify → shadow or rank queue.
        Every signal — regardless of outcome — gets a row in ap_signals.
        """
        ticker = signal.get("ticker", "")
        score  = float(signal.get("score", 0) or 0)

        # Assign signal_id immediately — flows through the entire lifecycle
        signal_id = str(signal.get("signal_id") or uuid.uuid4())
        signal["signal_id"] = signal_id

        log.info(
            f"[{ticker}] Signal received | "
            f"{signal.get('pattern')} {signal.get('side')} [{signal.get('timeframe')}] | "
            f"Score={score:.1f}"
        )

        funnel.inc("signals_received")

        # ── LEGACY FALLBACK ───────────────────────────────────────────────────
        is_legacy = (
            score == 0 and (
                signal.get("signal_id")
                or signal.get("symbol")
                or signal.get("pattern_id")
            ) and not signal.get("ev_score")
        )
        if is_legacy:
            legacy_ticker = signal.get("symbol") or signal.get("ticker", "?")
            log.info(f"[{legacy_ticker}] LEGACY SIGNAL — routing to legacy queue (no score)")
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
                log.warning(f"[{legacy_ticker}] Legacy queue failed: {e} — signal dropped")
            return

        # Apply paper exec fallbacks before score gate
        signal    = self._apply_paper_exec_fallbacks(signal)
        score     = float(signal.get("score", 0) or 0)
        signal_id = str(signal.get("signal_id", signal_id))

        # Insert into ap_signals with the final post-boost score
        self.store.insert_signal(signal_id, signal, decision_status="received")

        # ── Gate 1: Score floor ───────────────────────────────────────────────
        score_floor = self._score_floor
        if score < score_floor:
            log.info(
                f"[{ticker}] REJECTED — score {score:.1f} below "
                f"{'paper' if self.paper else 'live'} floor ({score_floor})"
            )
            funnel.inc("rejected_score")
            self.store.update_status(signal_id, "rejected",
                context_notes=f"score {score:.1f} below floor {score_floor}")
            return
        funnel.inc("passed_score")

        # ── Gate 2: Context hard block ────────────────────────────────────────
        score_breakdown = signal.get("score_breakdown")
        has_breakdown   = bool(score_breakdown)

        if not has_breakdown:
            log.info(
                f"[{ticker}] Context gate SKIPPED — no score_breakdown in signal "
                f"(legacy signal path)"
            )
        else:
            context_score = float(score_breakdown.get("real_time_ctx", 0) or 0)
            context_floor = self._context_floor
            if context_score < context_floor:
                log.info(
                    f"[{ticker}] CONTEXT BLOCKED — context={context_score:.1f}/20 "
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

        # ── B Tier: shadow track only, never live ─────────────────────────────
        if tier == Tier.B:
            if score_result:
                td = self.tier_engine.classify(score_result)
                self.shadow.log_shadow(signal, td)
            funnel.inc("shadow_tracked")
            self.store.update_status(signal_id, "shadow")
            log.info(f"[{ticker}] B-TIER — shadow tracked (score={score:.1f}), no live capital")
            return

        # ── Hard reject ───────────────────────────────────────────────────────
        if tier == Tier.REJECT:
            log.info(f"[{ticker}] REJECTED — score {score:.1f}")
            self.store.update_status(signal_id, "rejected",
                context_notes=f"tier=REJECT score={score:.1f}")
            return

        # ── A / A+: add to ranking queue ──────────────────────────────────────
        signal["tier"]         = tier
        signal["auto_execute"] = (tier == Tier.A_PLUS)
        self.rank_queue.add(signal)
        self.store.update_status(signal_id, "queued", timestamp_flag="queued_at")

    # ── RANKING QUEUE PROCESSOR ───────────────────────────────────────────────

    def _start_ranking_processor(self):
        """Background thread: drains ranking queue into watcher every 5 seconds."""
        self._rq_running = True

        def _loop():
            while self._rq_running:
                try:
                    slots = self.rank_queue.available_slots(self._position_count)
                    if slots > 0 and self.rank_queue.size() > 0:
                        signals = self.rank_queue.drain(slots)
                        for sig in signals:
                            tkr       = sig.get("ticker", "")
                            score     = float(sig.get("score", 0))
                            signal_id = str(sig.get("signal_id", ""))

                            if slots <= 0:
                                log.info(f"[{tkr}] No slots left — re-queuing remaining signals")
                                self.rank_queue.add(sig)
                                continue
                            slots -= 1

                            # Sector correlation cap
                            sector = sig.get("correlation_bucket", "OTHER")
                            with self._sector_lock:
                                sector_count = self._sector_counts.get(sector, 0)
                            SECTOR_MAX = {"SEMI": 2, "MEGACAP": 2, "INDEX": 2,
                                          "FINANCIAL": 2, "BIO": 1, "CLOUD": 2}.get(sector, 2)
                            if sector_count >= SECTOR_MAX:
                                log.info(
                                    f"[{tkr}] SECTOR CAP — {sector} already has "
                                    f"{sector_count}/{SECTOR_MAX} active. Skipping."
                                )
                                funnel.inc("sector_capped")
                                if signal_id:
                                    self.store.update_signal_fields(signal_id, {
                                        "decision_status": "dropped",
                                        "context_notes":   f"sector_cap: {sector} at {sector_count}/{SECTOR_MAX}",
                                    })
                                continue

                            # Re-check context before dispatching to watcher
                            stale_breakdown = sig.get("score_breakdown")
                            if stale_breakdown:
                                stale_ctx = float(stale_breakdown.get("real_time_ctx", 0) or 0)
                                if stale_ctx < self._context_floor and stale_ctx > 0:
                                    log.info(
                                        f"[{tkr}] Context re-check failed after queue wait "
                                        f"(ctx={stale_ctx:.1f} < floor={self._context_floor}) — skipping"
                                    )
                                    if signal_id:
                                        self.store.update_signal_fields(signal_id, {
                                            "decision_status": "dropped",
                                            "context_notes":   f"context_recheck_fail: ctx={stale_ctx:.1f} floor={self._context_floor}",
                                        })
                                    continue

                            added = self.watcher.add_signal(sig)
                            if added:
                                funnel.inc("watcher_sent")                          # ← ADDED
                                self.store.update_status(
                                    signal_id, "watching",
                                    timestamp_flag="watcher_started_at",
                                )
                                with self._sector_lock:
                                    self._sector_counts[sector] = self._sector_counts.get(sector, 0) + 1
                                log.info(
                                    f"[{tkr}] Dispatched from ranking queue → watcher | "
                                    f"score={score:.1f} [{sig.get('grade')}] | "
                                    f"sector={sector} ({sector_count+1}/{SECTOR_MAX}) | "
                                    f"slots left: {slots}"
                                )
                            else:
                                slots += 1
                                log.info(f"[{tkr}] Watcher rejected (EOD or duplicate) — slot returned")
                except Exception as e:
                    log.error(f"Ranking queue processor error: {e}")
                time.sleep(5)

        t = threading.Thread(target=_loop, daemon=True, name=f"rq-processor-{self.email}")
        t.start()
        log.info(f"[{self.email}] RankingQueueProcessor started (5s interval)")

    # ── CALLBACK: Breach Confirmed → Execute ──────────────────────────────────

    def _on_entry_trigger(self, watched: WatchedSignal):
        """Called by watcher when price holds above/below trigger for 2 polls."""
        sig       = watched.signal
        ticker    = watched.ticker
        side      = watched.side
        signal_id = str(sig.get("signal_id", ""))

        log.info(f"[{ticker}] Breach confirmed @ ${watched.trigger_price:.2f} — evaluating chain")

        if signal_id:
            self.store.update_status(signal_id, "triggered", timestamp_flag="triggered_at")
        funnel.inc("watcher_triggered")                                             # ← ADDED

        # Re-check position count at breach time
        if self._position_count >= MAX_POSITIONS:
            log.info(
                f"[{ticker}] No slot at breach time — positions full "
                f"({self._position_count}/{MAX_POSITIONS}). Re-queuing signal."
            )
            if signal_id:
                self.store.update_signal_fields(signal_id, {
                    "decision_status": "requeued_after_trigger",
                    "context_notes":   f"positions_full={self._position_count}/{MAX_POSITIONS} at breach",
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
            log.warning(f"[{ticker}] Empty chain — cannot enter")
            return

        # Chain health check
        health = chain_health_report(chain, side, watched.trigger_price)
        if not health["tradeable"]:
            log.warning(
                f"[{ticker}] Chain health FAILED — "
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
            log.warning(f"[{ticker}] BLOCKED — setup DOWNGRADED (live WR diverged >25% from backtest)")
            return

        # Size: base × spread modifier × feedback modifier × tier multiplier
        tier      = sig.get("tier", Tier.A)
        tier_mult = 1.0 if tier == Tier.A_PLUS else 0.6
        base      = self._get_base_contracts(watched.score)
        contracts = max(1, round(base * decision.size_modifier * feedback_mod * tier_mult))

        log.info(
            f"[{ticker}] Sizing: base={base} × spread={decision.size_modifier:.2f} "
            f"× feedback={feedback_mod:.2f} × tier={tier_mult:.1f} → {contracts}x "
            f"[{decision.grade}] [{setup_status}]"
        )

        # Place order
        if self.paper:
            log.info(f"[{ticker}] PAPER — simulating fill @ ${decision.mid_price:.2f}")
            fill_price = decision.mid_price
        else:
            fill_price = self._place_option_order(
                symbol      = decision.symbol,
                contracts   = contracts,
                side        = "buy_to_open",
                limit_price = decision.mid_price,
            )

        if not fill_price:
            log.error(f"[{ticker}] Order failed — no fill price returned")
            funnel.inc("order_failed")                                              # ← ADDED
            return

        # Register with exit engine
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

        self.exit_eng.add_position(pos)

        with self._pos_lock:
            self._position_count += 1

        if signal_id:
            self.store.update_status(signal_id, "executed", timestamp_flag="executed_at")

        self.proof.log_position_opened(
            ticker    = ticker,
            side      = side,
            tier      = sig.get("tier", "A"),
            score     = watched.score,
            contracts = contracts,
        )

        log.info(
            f"[{ticker}] {'PAPER' if self.paper else 'LIVE'} OPEN | "
            f"{contracts}x {decision.symbol} @ ${fill_price:.2f} | "
            f"target=${watched.target_price} stop=${watched.stop_level} | "
            f"tier={tier} score={watched.score:.0f}"
        )

    # ── CALLBACK: Position Closed ─────────────────────────────────────────────

    def _on_position_close(self, pos: ManagedPosition, decision):
        with self._pos_lock:
            self._position_count = max(0, self._position_count - 1)

        sector = getattr(pos, "signal", {}).get("correlation_bucket", "OTHER")
        with self._sector_lock:
            self._sector_counts[sector] = max(0, self._sector_counts.get(sector, 0) - 1)

        if self.paper:
            exit_price = pos.current_option_price
            log.info(f"[{pos.ticker}] PAPER CLOSE | P&L={pos.option_pnl_pct*100:+.1f}% | {decision.reason}")
        else:
            exit_price = self._place_option_order(
                symbol      = pos.option_symbol,
                contracts   = pos.quantity_remaining,
                side        = "sell_to_close",
                limit_price = pos.current_option_price,
            ) or pos.current_option_price

        sig     = getattr(pos, "signal", {})
        opt_pnl = (exit_price - pos.entry_price) / pos.entry_price * 100 if pos.entry_price else 0
        win     = opt_pnl > 0
        tier    = sig.get("tier", Tier.A_PLUS)

        self.proof.log_trade(
            ticker             = pos.ticker,
            pattern            = sig.get("pattern", ""),
            side               = pos.side,
            timeframe          = sig.get("timeframe", "1d"),
            score              = float(sig.get("score", 0) or 0),
            tier               = tier,
            context_score      = float(sig.get("score_breakdown", {}).get("real_time_ctx", 0) or 0),
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
        )

        signal_id = str(sig.get("signal_id", ""))
        if signal_id:
            self.store.update_status(signal_id, "closed", timestamp_flag="closed_at")

        self.shadow.record_live_outcome(tier, opt_pnl)

    # ── CALLBACKS: Expire / Invalidate ────────────────────────────────────────

    def _on_signal_expire(self, watched: WatchedSignal):
        signal_id = str(watched.signal.get("signal_id", ""))
        if signal_id:
            self.store.update_status(signal_id, "expired", timestamp_flag="expired_at")
        funnel.inc("watcher_expired")                                               # ← ADDED
        log.info(f"[{watched.ticker}] Signal expired — no breach")

    def _on_signal_invalidate(self, watched: WatchedSignal):
        signal_id = str(watched.signal.get("signal_id", ""))
        if signal_id:
            self.store.update_status(signal_id, "invalidated", timestamp_flag="invalidated_at")
        funnel.inc("watcher_invalidated")                                           # ← ADDED
        log.info(f"[{watched.ticker}] Signal invalidated — wrong direction")

    def _on_position_scale(self, pos: ManagedPosition, decision):
        log.info(f"[{pos.ticker}] SCALE OUT {decision.quantity}x | P&L={pos.option_pnl_pct*100:+.1f}% | {decision.reason}")
        if not self.paper:
            self._place_option_order(
                symbol      = pos.option_symbol,
                contracts   = decision.quantity,
                side        = "sell_to_close",
                limit_price = pos.current_option_price,
            )

    # ── BROKER HELPERS ────────────────────────────────────────────────────────

    def _fetch_0dte_chain(self, ticker: str) -> tuple[list, str]:
        today = date.today().strftime("%Y-%m-%d")
        resp  = self.broker.session.get(
            f"{self.broker.base_url}/v1/markets/options/chains",
            params  = {"symbol": ticker, "expiration": today, "greeks": "true"},
            headers = {"Accept": "application/json"},
            timeout = 10,
        )
        options = resp.json().get("options", {}).get("option", [])
        if isinstance(options, dict):
            options = [options]
        return options or [], today

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
                f"{self.broker.base_url}/v1/accounts/{self.broker.account_id}/orders",
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
