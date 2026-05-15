# ap_execution_core.py -- Angel Precision Pure Production Execution Core
# =============================================================================
# Production contract:
#   /signal -> trade_queue -> worker_loop -> APMasterControl
#   -> APContractSelector -> APOrderStateMachine.create_entry_order()
#   -> APEntryWatcher.watch(plan, local_order_id)
#   -> breach -> APOrderStateMachine.submit_existing_entry()
#   -> fill_monitor / position_manager / exit_engine.
#
# ExecutionCore does not select contracts, size entries, create entry orders at
# breach time, or create synthetic positions. It only owns watcher callbacks,
# breach-time risk revalidation, OSM submission of existing orders, exit-engine
# callbacks, signal tracking, and proof/feedback logging.
# =============================================================================

from __future__ import annotations

import os
import time
import uuid
import logging
import threading
from datetime import datetime, timezone
from typing import Optional
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from ap_entry_watcher        import APEntryWatcher, WatchedSignal
from ap_exit_engine          import APExitEngine, ManagedPosition
from ap_feedback_loop        import APFeedbackLoop
from ap_tier_engine          import APShadowTracker
from ap_proof_logger         import APProofLogger, funnel
from ap_signal_store         import APSignalStore
from ap_signal_tracker       import APSignalTracker

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

# ── Mode metadata thresholds ────────────────────────────────────────────────
SCORE_FLOOR_LIVE    = 75   # live: only trade validated setups
SCORE_FLOOR_PAPER   = 45
CONTEXT_FLOOR_LIVE  = 10.0
CONTEXT_FLOOR_PAPER = 0.0

# =============================================================================
# EXECUTION CORE
# =============================================================================

class APExecutionCore:
    """
    One instance per client. Orchestrates watcher callbacks, OSM submit of
    already-created entry orders, exit-engine callbacks, and signal logging.
    """

    def __init__(self, broker, supabase_client=None, email: str = "", position_manager=None, order_state_machine=None, data_broker=None, master_control=None, contract_selector=None):
        self.broker            = broker
        self.contract_selector = contract_selector  # wired for breach-time selection of deferred overnight signals
        self.email              = email
        self.position_manager   = position_manager
        self.order_state_machine = order_state_machine
        # Mode: injected master_control is canonical; BOT_MODE is the env fallback.
        self.paper     = BOT_MODE != "LIVE"
        self.mode      = "LIVE" if not self.paper else "PAPER"
        self._pos_lock = threading.Lock()
        self._position_count = 0

        # Mode-specific metadata values.
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
        self.shadow      = APShadowTracker(supabase_client, DISCORD_WEBHOOK_URL)
        self._sector_counts: dict[str, int] = {}
        self._sector_lock   = threading.Lock()
        self.proof          = APProofLogger(
            supabase_client=supabase_client,
            client_email=email,
            mode="paper" if self.paper else "live",
        )
        try:
            from ap_edge_intelligence import APTradeLogger as _ATL
            self._edge_logger = _ATL()
        except Exception:
            self._edge_logger = None

        # ── MASTER CONTROL -- single production decision authority ─────────────
        if master_control is None:
            raise RuntimeError(
                f"[{email}] APExecutionCore requires injected master_control. "
                "Production decisions must come from ClientRunner/worker_loop."
            )
        self.master_control = master_control
        log.info("[%s] APExecutionCore using injected master_control", email)

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

    def _current_open_position_count(self) -> int:
        """
        Return the most reliable open-position count available.

        Production path uses APPositionManager/Postgres truth. The local
        counter is only a defensive fallback if snapshot() is unavailable.
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
        This prevents over-dispatch if DB truth reports active/pending exposure.
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
                "[%s] No slot at breach time — open=%s pending=%s max=%s. Blocking queued entry.",
                ticker, open_count, pending_entries, self._max_positions,
            )
            if signal_id:
                self.store.update_signal_fields(signal_id, {
                    "decision_status": "blocked_at_breach",
                    "context_notes": (
                        f"positions_full_at_breach open={open_count} "
                        f"pending={pending_entries} max={self._max_positions}"
                    ),
                })
            _sector = sig.get("sector") or sig.get("correlation_bucket") or ticker
            try:
                self._cleanup_pending_entry_order(watched, action="cancel", reason="positions_full_at_breach")
            except Exception as _clean_err:
                log.error("[%s] Failed to cleanup pending entry order: %s", ticker, _clean_err)
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
                    return False
                log.warning("[%s] PAPER breach exposure revalidation failed open: %s", ticker, exc)

        return True

    def start(self):
        """Start production orchestration threads only.

        Pure production contract:
          - Queue/worker owns signal evaluation and plan creation.
          - OSM owns order lifecycle.
          - EntryWatcher only waits for breach and calls this core back.
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
        log.info(f"[{self.email}] Execution core started (PURE_PRODUCTION: watcher + exit engine + tracker; queue is entry authority)")

    def stop(self):
        self.entry_watcher.stop()
        self.exit_eng.stop()
        self.tracker.stop()

    # ── HELPER: paper-mode exec quality fallbacks ─────────────────────────────

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

    def _mark_breach_failure(
        self,
        watched: WatchedSignal,
        *,
        decision_status: str,
        context_note: str,
        funnel_key: Optional[str] = None,
        cleanup_action: Optional[str] = None,
    ) -> None:
        """Record a breach-time non-entry in every local truth surface.

        Once the watcher fires, the watch has already left the pending queue.
        Therefore every early return must be explicit: signal-store status,
        funnel counter, and OSM cleanup for unsubmitted queue-created orders
        when applicable.
        """
        sig = getattr(watched, "signal", {}) or {}
        signal_id = str(sig.get("signal_id") or "")
        ticker = getattr(watched, "ticker", sig.get("ticker", "?"))

        if signal_id:
            try:
                self.store.update_signal_fields(signal_id, {
                    "decision_status": decision_status,
                    "context_notes": context_note,
                })
            except Exception as exc:
                log.warning("[%s] breach failure signal-store update failed: %s", ticker, exc)

        if funnel_key:
            try:
                funnel.inc(funnel_key)
            except Exception:
                pass

        if cleanup_action in {"expire", "cancel"}:
            try:
                self._cleanup_pending_entry_order(
                    watched,
                    action=cleanup_action,
                    reason=context_note,
                )
            except Exception as exc:
                log.error(
                    "[%s] breach failure OSM cleanup failed | action=%s reason=%s error=%s",
                    ticker, cleanup_action, context_note, exc, exc_info=True,
                )

    # ── CALLBACK: Breach Confirmed -> Execute ─────────────────────────────────

    def _on_entry_trigger(self, watched: WatchedSignal):
        """Called by watcher when price holds above/below trigger for 2 polls.

        Production cohesion rule:
          - Approved at queue time.
          - Revalidated at breach time.
          - Submitted unchanged.

        This method intentionally does NOT re-run contract selection, options
        intelligence, premium-bound checks, sizing, or fresh plan creation.
        Queue/worker + master_control + contract_selector own those decisions.
        """
        sig = watched.signal or {}
        ticker = watched.ticker
        signal_id = str(sig.get("signal_id", "") or "")

        trigger_price = getattr(watched, "trigger_price", None)
        try:
            trigger_price_for_log = float(trigger_price or 0)
        except Exception:
            trigger_price_for_log = 0.0

        log.info(
            "[%s] Breach confirmed @ $%.2f -- submitting approved queued plan",
            ticker,
            trigger_price_for_log,
        )

        if signal_id:
            self.store.update_status(signal_id, "triggered", timestamp_flag="triggered_at")
        funnel.inc("watcher_triggered")

        # 1) Revalidate only. Never re-run selection/sizing logic here.
        if not self._breach_risk_check(watched):
            funnel.inc("master_control_blocked")
            return

        # 2) Require OSM + existing queue-created local order id.
        if self.order_state_machine is None:
            log.critical("[%s] PRODUCTION_ENTRY_BLOCK — order_state_machine missing at breach", ticker)
            funnel.inc("order_failed")
            if signal_id:
                self.store.update_signal_fields(signal_id, {
                    "decision_status": "blocked_at_breach",
                    "context_notes": "order_state_machine_missing_at_breach",
                })
            return

        queue_local_order_id = str(sig.get("local_order_id") or "").strip()
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

        # 3) Recover the already-approved queue/OSM plan.
        approved_plan = self._recover_plan_for_revalidation(watched)
        if approved_plan is None:
            log.critical("[%s] PRODUCTION_ENTRY_BLOCK — approved plan missing after breach revalidation", ticker)
            funnel.inc("order_failed")
            if signal_id:
                self.store.update_signal_fields(signal_id, {
                    "decision_status": "blocked_at_breach",
                    "context_notes": "approved_plan_missing_after_revalidation",
                })
            return

        # 3b) Breach-time contract selection for overnight deferred signals.
        # Pre-market option chains have zero bids — overnight_reeval cannot select
        # contracts before 9:30 AM ET. Those signals are armed with
        # contract_deferred=True. Here at breach (market open, live quotes) we
        # select the contract before the limit_price and contract_symbol checks.
        _sig_meta   = getattr(approved_plan, "metadata", {}) or {}
        _sig_dict   = sig or {}
        _deferred   = (
            bool(_sig_meta.get("contract_deferred"))
            or bool(_sig_dict.get("contract_deferred"))
            or not str(getattr(approved_plan, "contract_symbol", "") or "").strip()
        )
        if _deferred:
            if self.contract_selector is None:
                log.critical(
                    "[%s] PRODUCTION_ENTRY_BLOCK — contract_deferred=True but no "
                    "contract_selector wired into execution core",
                    ticker,
                )
                funnel.inc("order_failed")
                if signal_id:
                    self.store.update_signal_fields(signal_id, {
                        "decision_status": "blocked_at_breach",
                        "context_notes": "contract_deferred_no_selector",
                    })
                return
            try:
                log.info(
                    "[%s] Overnight deferred signal — selecting contract at breach "
                    "with live quotes (trigger=%.4f side=%s)",
                    ticker,
                    float(getattr(approved_plan, "trigger_price", 0) or 0),
                    getattr(approved_plan, "side", "?"),
                )
                _sel = self.contract_selector.select(approved_plan)
                _live_contract = str(getattr(approved_plan, "contract_symbol", "") or "").strip()
                if not _sel or not _live_contract:
                    log.critical(
                        "[%s] PRODUCTION_ENTRY_BLOCK — breach-time contract selection "
                        "returned no contract",
                        ticker,
                    )
                    funnel.inc("order_failed")
                    if signal_id:
                        self.store.update_signal_fields(signal_id, {
                            "decision_status": "blocked_at_breach",
                            "context_notes": "breach_time_contract_selection_no_result",
                        })
                    return
                log.info(
                    "[%s] Breach-time contract selected: %s @ $%.2f x%s",
                    ticker, _live_contract,
                    float(getattr(approved_plan, "limit_price", 0) or 0),
                    int(getattr(approved_plan, "contracts", 1) or 1),
                )
            except Exception as _cs_err:
                log.critical(
                    "[%s] PRODUCTION_ENTRY_BLOCK — breach-time contract selection "
                    "error: %s",
                    ticker, _cs_err,
                )
                funnel.inc("order_failed")
                if signal_id:
                    self.store.update_signal_fields(signal_id, {
                        "decision_status": "blocked_at_breach",
                        "context_notes": f"breach_time_contract_selection_error:{_cs_err}",
                    })
                return

        # 4) Require the approved plan to carry a valid limit price.
        submit_limit = getattr(approved_plan, "limit_price", None)
        try:
            submit_limit = float(submit_limit)
        except Exception:
            submit_limit = 0.0

        if submit_limit <= 0:
            log.critical("[%s] PRODUCTION_ENTRY_BLOCK — approved plan missing valid limit_price", ticker)
            funnel.inc("order_failed")
            if signal_id:
                self.store.update_signal_fields(signal_id, {
                    "decision_status": "blocked_at_breach",
                    "context_notes": "approved_plan_missing_limit_price",
                })
            return

        # Optional hard guards: require contract + nonzero qty from the approved plan.
        approved_contract = str(getattr(approved_plan, "contract_symbol", "") or "").strip()
        try:
            approved_qty = int(getattr(approved_plan, "contracts", 0) or 0)
        except Exception:
            approved_qty = 0

        if not approved_contract:
            log.critical("[%s] PRODUCTION_ENTRY_BLOCK — approved plan missing contract_symbol", ticker)
            funnel.inc("order_failed")
            if signal_id:
                self.store.update_signal_fields(signal_id, {
                    "decision_status": "blocked_at_breach",
                    "context_notes": "approved_plan_missing_contract_symbol",
                })
            return

        if approved_qty <= 0:
            log.critical("[%s] PRODUCTION_ENTRY_BLOCK — approved plan has invalid contracts=%s", ticker, approved_qty)
            funnel.inc("order_failed")
            if signal_id:
                self.store.update_signal_fields(signal_id, {
                    "decision_status": "blocked_at_breach",
                    "context_notes": f"approved_plan_invalid_contracts={approved_qty}",
                })
            return

        # 5) Submit unchanged.
        log.info(
            "[%s] %s — submitting EXISTING queue order via approved plan | local=%s contract=%s @ $%.2f x%s",
            ticker,
            "PAPER" if self.paper else "LIVE",
            queue_local_order_id,
            approved_contract,
            submit_limit,
            approved_qty,
        )

        submit_res = self.order_state_machine.submit_existing_entry(
            local_order_id=queue_local_order_id,
            broker=self.broker,
            plan=approved_plan,
            limit_price=submit_limit,
        )

        if submit_res.get("ok"):
            local_order_id = submit_res.get("local_order_id")
            broker_order_id = submit_res.get("broker_order_id")
            if signal_id:
                self.store.update_signal_fields(signal_id, {
                    "decision_status": "submitted",
                    "context_notes": f"entry_submitted local={local_order_id} broker={broker_order_id}",
                })
            log.info(
                "[%s] Entry submitted via OSM | local=%s broker=%s %sx %s @ $%.2f",
                ticker,
                local_order_id,
                broker_order_id,
                approved_qty,
                approved_contract,
                submit_limit,
            )
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
        try:
            if hasattr(self.order_state_machine, "cancel_pending_entry"):
                self.order_state_machine.cancel_pending_entry(
                    queue_local_order_id,
                    reason=f"submit_failed:{submit_res.get('error')}",
                )
        except Exception as exc:
            log.warning("[%s] OSM cancel after submit failure failed | order=%s error=%s", ticker, queue_local_order_id, exc)
        return

    # ── CALLBACK: Position Closed ─────────────────────────────────────────────

    def _on_position_close(self, pos: ManagedPosition, decision):
        # ── IDEMPOTENCY: skip if position already marked closed ──────────────
        if getattr(pos, "closed", False):
            log.debug("[%s] _on_position_close called but pos.closed=True — skipping", pos.ticker)
            return

        with self._pos_lock:
            self._position_count = max(0, self._position_count - 1)

        sector = getattr(pos, "signal", {}).get("correlation_bucket", "OTHER")
        with self._sector_lock:
            self._sector_counts[sector] = max(0, self._sector_counts.get(sector, 0) - 1)

        # ── EXIT SUBMISSION ─────────────────────────────────────────────────
        sig = getattr(pos, "signal", {})
        _sig_id = str(sig.get("signal_id", ""))
        _urgency = str(getattr(decision, "urgency", "") or "").upper()
        _bid = getattr(pos, "current_bid", 0) or 0
        _ask = getattr(pos, "current_ask", 0) or 0
        _mid = getattr(pos, "current_option_price", 0) or 0

        # EXIT PRICING POLICY
        # ─────────────────────────────────────────────────────────────────
        # IMMEDIATE urgency (hard stop -33%, never-green, EOD) → MARKET ORDER
        #   Must fill. Hard stops and EOD closes cannot miss. No slippage risk.
        #
        # HIGH urgency — PROTECTIVE exits (RUNNER_TRAIL, PROFIT_LOCK, etc.) → BID
        #   Start at bid immediately. These exits protect earned profit; sitting at
        #   mid while price slides back costs more than the bid/mid spread.
        #
        # HIGH urgency — SCALE exits (scale-out W1/W2/W3) → MID
        #   Scale-outs are non-urgent partial closes; mid pricing is fine.
        #   Still escalates to bid at 90s and market at 150s if unfilled.
        #
        # No quote available for IMMEDIATE → BLOCK (never submit zero-price stop)
        # No quote available for HIGH → retry on next quote cycle (not blocking)

        _PROTECTIVE_EXIT_CODES = {
            "RUNNER_TRAIL", "TRAILING_STOP", "PROFIT_LOCK", "TOUCHED_PROFIT_STOP",
            "SMALL_WIN_LOCK", "EOD_FORCE_CLOSE", "HARD_STOP", "STOP_HIT",
            "SENTINEL_FORCED_EXIT", "NEVER_GREEN_STOP", "THETA_STOP", "TIME_STOP",
        }
        _reason_code = str(getattr(decision, "reason_code", "") or "").upper()
        _reason_text = str(getattr(decision, "reason", "") or "").upper()

        _PROTECTIVE_REASON_TEXT_MARKERS = {
            "RUNNER TRAIL", "TRAILING STOP", "PROFIT LOCK", "TOUCHED PROFIT",
            "SMALL WIN", "EOD FORCE CLOSE", "HARD STOP", "STOP HIT",
            "NEVER GREEN", "THETA", "TIME STOP", "SENTINEL",
        }

        _is_protective = (
            _reason_code in _PROTECTIVE_EXIT_CODES
            or any(marker in _reason_text for marker in _PROTECTIVE_REASON_TEXT_MARKERS)
        )

        _use_market = _urgency == "IMMEDIATE"

        # CHEAP CONTRACT MARKET EXIT:
        # If option is worth < $0.25/share, limit orders will not fill reliably.
        # The spread is typically $0.01–$0.05 wide which means limit orders at
        # $0.12, $0.13, $0.14 etc. bounce around without filling, causing the
        # cascade of canceled exits seen with NVDA $0.11 contract.
        # Go straight to market — the slippage on a $0.10 fill vs $0.11 is $0.01/share
        # ($1/contract), which is far less damage than 15 failed limit attempts.
        _CHEAP_EXIT_MARKET_THRESHOLD = float(
            os.getenv("CHEAP_EXIT_MARKET_THRESHOLD", "0.25")
        )
        _current_opt_price = float(getattr(pos, "current_option_price", 0) or _mid or _bid)
        if not _use_market and _current_opt_price > 0 and _current_opt_price < _CHEAP_EXIT_MARKET_THRESHOLD:
            _use_market = True
            log.warning(
                "[%s] CHEAP_CONTRACT_MARKET_EXIT — option price $%.2f < threshold $%.2f "
                "— using market order to avoid limit cascade | %s",
                pos.ticker, _current_opt_price, _CHEAP_EXIT_MARKET_THRESHOLD, decision.reason,
            )

        # LIMIT-TO-MARKET ESCALATION for HIGH urgency exits:
        # If a limit order was placed and hasn't filled within the escalation window,
        # step down the price and eventually go market. Prevents sitting on a 22% gain
        # while the price slides back. Tracked via pos._exit_submit_ts and _exit_attempts.
        _exit_submit_ts = getattr(pos, "_exit_submit_ts", 0) or 0
        _exit_attempts  = getattr(pos, "_exit_attempts",  0) or 0
        _age_since_submit = time.time() - _exit_submit_ts if _exit_submit_ts else 999

        # If HIGH urgency limit is in-flight > 90s → escalate to bid
        # If HIGH urgency limit is in-flight > 150s → escalate to market
        if not _use_market and _urgency == "HIGH" and getattr(pos, "exit_in_flight", False):
            if _age_since_submit > 150:
                _use_market = True
                log.warning("[%s] EXIT ESCALATED TO MARKET — limit unfilled >150s | %s", pos.ticker, decision.reason)
            elif _age_since_submit > 90 and _bid > 0:
                # Step down to bid
                _exit_limit = round(_bid, 2)
                exit_price = _exit_limit
                log.warning("[%s] EXIT STEPPED DOWN to bid $%.2f — limit unfilled >90s | %s", pos.ticker, _exit_limit, decision.reason)

        if _use_market:
            _exit_limit = None
            exit_price = _mid if _mid > 0 else _bid
            if exit_price <= 0:
                log.critical("[%s] CLOSE BLOCKED — no quote for IMMEDIATE exit | %s", pos.ticker, decision.reason)
                return
            log.info("[%s] MARKET EXIT @ est.$%.2f | urgency=%s | %s", pos.ticker, exit_price, _urgency, decision.reason)
        elif _is_protective and _bid > 0:
            # Protective exits: start at bid immediately.
            # Sitting at mid while profit erodes costs more than the spread.
            _exit_limit = round(_bid, 2)
            exit_price  = _exit_limit
            pos._exit_submit_ts = time.time()  # type: ignore[attr-defined]
            pos._exit_attempts  = _exit_attempts + 1  # type: ignore[attr-defined]
            log.info("[%s] PROTECTIVE EXIT @ $%.2f (bid) attempt=%d | %s | %s",
                     pos.ticker, _exit_limit, pos._exit_attempts, _reason_code, decision.reason)
        elif _mid > 0:
            _exit_limit = round(_mid, 2)
            exit_price = _exit_limit
            # Track submission time for escalation
            pos._exit_submit_ts = time.time()  # type: ignore[attr-defined]
            pos._exit_attempts  = _exit_attempts + 1  # type: ignore[attr-defined]
            log.info("[%s] LIMIT EXIT @ $%.2f (mid) attempt=%d | urgency=HIGH | %s",
                     pos.ticker, _exit_limit, pos._exit_attempts, decision.reason)
        elif _bid > 0:
            _exit_limit = round(_bid, 2)
            exit_price = _exit_limit
            pos._exit_submit_ts = time.time()  # type: ignore[attr-defined]
            pos._exit_attempts  = _exit_attempts + 1  # type: ignore[attr-defined]
            log.info("[%s] LIMIT EXIT @ $%.2f (bid) attempt=%d | urgency=HIGH | %s",
                     pos.ticker, _exit_limit, pos._exit_attempts, decision.reason)
        else:
            if _urgency == "IMMEDIATE":
                log.critical("[%s] CLOSE BLOCKED — no quote for IMMEDIATE exit | %s", pos.ticker, decision.reason)
                return
            log.warning("[%s] Exit skipped — no quote | will retry | %s", pos.ticker, decision.reason)
            return

        if self.order_state_machine and pos.position_id:
            _price_str = f"${_exit_limit:.2f}" if _exit_limit is not None else "MARKET"
            log.info(
                f"[{pos.ticker}] {'PAPER' if self.paper else 'LIVE'} CLOSE -- "
                f"submitting sell_to_close via OSM @ {_price_str} | {decision.reason}"
            )
            exit_res = self.order_state_machine.submit_exit(
                broker      = self.broker,
                position_id = str(pos.position_id),
                contract    = pos.option_symbol,
                symbol      = pos.ticker,
                direction   = pos.side,
                qty         = pos.quantity_remaining,
                limit_price = _exit_limit,  # None = market order for IMMEDIATE exits
                signal_id   = _sig_id or None,
                order_type  = "market" if _exit_limit is None else "limit",
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
                    f"order={exit_res['local_order_id']} error={exit_res['error']}"
                )
                return
        else:
            log.critical(
                f"[{pos.ticker}] CLOSE BLOCKED — OSM or position_id missing; "
                f"cannot submit sell_to_close through production authority | {decision.reason}"
            )
            return

        # Always compute option P&L from option prices — never from pos.entry_price
        # which can be seeded from avg_fill (which sometimes stored underlying price).
        _entry_opt = float(pos.entry_price or 0)
        _exit_opt  = float(exit_price or 0)
        if _entry_opt > 0 and _exit_opt > 0:
            opt_pnl = (_exit_opt - _entry_opt) / _entry_opt * 100  # e.g. 15.3 = 15.3%
        else:
            opt_pnl = 0.0
        win  = opt_pnl > 0
        tier = sig.get("tier", "A+")

        if _entry_opt > 0:
            log.info(
                "[%s] P&L CALC | entry=$%.4f exit=$%.4f → option_pnl=%.1f%% win=%s",
                pos.ticker, _entry_opt, _exit_opt, opt_pnl, win,
            )

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
                ("pnl_recorded",     abs(opt_pnl) > 0.001),
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
            option_pnl_pct     = opt_pnl / 100.0,
            underlying_pnl_pct = (pos.current_underlying - pos.underlying_entry) / pos.underlying_entry * 100
                                  if pos.underlying_entry else 0,
            win                = win,
            spread_pct         = float(sig.get("spread_pct", 0) or 0),
            chain_grade        = sig.get("chain_grade", ""),
            opened_at          = pos.opened_at if hasattr(pos, "opened_at") else None,
            synthetic_entry    = bool(getattr(pos, "synthetic_entry", False)),
            position_id        = str(getattr(pos, "position_id", "") or ""),
            local_order_id     = str(getattr(pos, "local_order_id", "") or ""),
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

        self.shadow.record_live_outcome(tier, opt_pnl / 100.0)

        # Log trade to edge intelligence (logger instantiated once in __init__ to avoid resource leaks)
        try:
            _edge_logger = self._edge_logger
            if _edge_logger:
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
            except Exception as _alpha_err:
                log.warning("Alpha tracker update failed: %s", _alpha_err)

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
            _scale_bid   = getattr(pos, "current_bid", 0) or 0
            _scale_mid   = getattr(pos, "current_option_price", 0) or 0
            if _scale_bid <= 0 and _scale_mid <= 0:
                log.critical("[%s] SCALE BLOCKED — no valid bid or mid for scale-out", pos.ticker)
                return
            _scale_limit = _scale_bid if _scale_bid > 0 else max(round(_scale_mid - 0.01, 2), 0.01)
            scale_res = self.order_state_machine.submit_exit(
                broker      = self.broker,
                position_id = str(pos.position_id),
                contract    = pos.option_symbol,
                symbol      = pos.ticker,
                direction   = pos.side,
                qty         = decision.quantity,
                limit_price = _scale_limit,
                signal_id   = _sig_id or None,
            )
            if scale_res["ok"]:
                log.info(
                    f"[{pos.ticker}] Scale exit submitted | "
                    f"local={scale_res['local_order_id']} broker={scale_res['broker_order_id']} "
                    f"qty={decision.quantity} @ ${_scale_limit:.2f}"
                )
            else:
                log.error(
                    f"[{pos.ticker}] Scale exit failed via OSM | "
                    f"order={scale_res['local_order_id']} error={scale_res['error']}"
                )
                return
        else:
            log.critical(
                f"[{pos.ticker}] SCALE BLOCKED — OSM or position_id missing; "
                "cannot submit scale-out through production authority"
            )
            return

    # ── BROKER HELPERS ────────────────────────────────────────────────────────

    # =========================================================================
    # POSITION SIZING — AGGRESSIVE RISK CURVE
    # =========================================================================

