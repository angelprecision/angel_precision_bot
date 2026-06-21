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
# PR-B / FIX-8: floors are now env-overridable so they can be tuned
# on Render without a code deploy (essential for proof-week response time).
SCORE_FLOOR_LIVE    = int(os.getenv("SCORE_FLOOR_LIVE",    "75"))   # live: only trade validated setups
SCORE_FLOOR_PAPER   = int(os.getenv("SCORE_FLOOR_PAPER",   "45"))
CONTEXT_FLOOR_LIVE  = float(os.getenv("CONTEXT_FLOOR_LIVE",  "10.0"))
CONTEXT_FLOOR_PAPER = float(os.getenv("CONTEXT_FLOOR_PAPER", "0.0"))

# PR-B / FIX-8: single-source-of-truth for the breakeven band. Previously
# read inline via os.getenv() in both _on_position_close and _finalize_proof
# — same env, two reads, easy drift. Now read once at module load.
# Stored as percent (e.g. -2.0 means -2%); callers may need /100.0 when
# comparing against decimal pnl.
BREAKEVEN_BAND_PCT  = float(os.getenv("BREAKEVEN_BAND_PCT", "-2.0"))

# ── P0: Breach-time entry pricing controls ────────────────────────────────────
# These match the existing process_signal() path in ap/execution.py and bring
# the watcher-breach path to parity.
# ENTRY_PAPER_ASK_CROSS_CENTS: how far above current ask the paper limit sits.
# ENTRY_LIVE_ASK_CROSS_CENTS:  how far above current ask the live limit sits.
# ENTRY_MAX_PRICE_DRIFT_PCT_FROM_PLAN: cancel if ask > plan * (1 + this).
#   Default 0.25 = 25%. Prevents chasing if the option has already run hard
#   between queue time and breach time.
ENTRY_PAPER_ASK_CROSS_CENTS        = float(os.getenv("ENTRY_PAPER_ASK_CROSS_CENTS",        "0.02"))
ENTRY_LIVE_ASK_CROSS_CENTS         = float(os.getenv("ENTRY_LIVE_ASK_CROSS_CENTS",         "0.01"))
ENTRY_MAX_PRICE_DRIFT_PCT_FROM_PLAN = float(os.getenv("ENTRY_MAX_PRICE_DRIFT_PCT_FROM_PLAN", "0.25"))

# =============================================================================
# EXECUTION CORE
# =============================================================================

class APExecutionCore:
    """
    One instance per client. Orchestrates watcher callbacks, OSM submit of
    already-created entry orders, exit-engine callbacks, and signal logging.
    """

    def __init__(self, broker, supabase_client=None, email: str = "", position_manager=None, order_state_machine=None, data_broker=None, master_control=None, contract_selector=None):
        # PR-B / FIX-8: AP_MODE vs BOT_MODE conflict check at __init__
        # (not at module import — import-time asserts break tests/scripts).
        _ap_mode_env  = (os.environ.get("AP_MODE")  or "").upper()
        _bot_mode_env = (os.environ.get("BOT_MODE") or "").upper()
        if _ap_mode_env and _bot_mode_env and _ap_mode_env != _bot_mode_env:
            raise RuntimeError(
                f"[{email}] AP_MODE={_ap_mode_env!r} conflicts with "
                f"BOT_MODE={_bot_mode_env!r}. Both env vars are set and "
                f"disagree — unsafe before live trading. Set only one, or "
                f"set both to the same value."
            )

        # PR-B / FIX-3: validate master_control FIRST and derive canonical
        # mode BEFORE constructing any submodule. Previously, mode was
        # derived from BOT_MODE, submodules were constructed, then mode
        # was re-derived from master_control — a window where APProofLogger
        # / APExitEngine could see the pre-canonical mode.
        if master_control is None:
            raise RuntimeError(
                f"[{email}] APExecutionCore requires injected master_control. "
                "Production decisions must come from ClientRunner/worker_loop."
            )
        self.master_control = master_control
        log.info("[%s] APExecutionCore using injected master_control", email)

        # PR-B / FIX-3: derive canonical mode from master_control NOW.
        # BOT_MODE is only a last-resort fallback if mc has no .mode attr.
        _mc_mode    = (getattr(master_control, "mode", BOT_MODE) or BOT_MODE).upper()
        self.mode   = _mc_mode
        self.paper  = _mc_mode != "LIVE"
        self._max_positions = getattr(master_control, "max_positions", MAX_POSITIONS)

        # Mode-specific metadata values (derived from canonical mode).
        self._score_floor   = SCORE_FLOOR_PAPER   if self.paper else SCORE_FLOOR_LIVE
        self._context_floor = CONTEXT_FLOOR_PAPER if self.paper else CONTEXT_FLOOR_LIVE

        # Plain attributes (no mode dependency).
        self.broker            = broker
        # P0B paper-quote-truth: attach data_broker onto self.broker so that
        # process_signal (which receives only `broker`) can resolve the live
        # market-data source via getattr(broker, "data_broker", None).
        # data_broker is the live Tradier instance when TRADIER_DATA_TOKEN is
        # set; it equals broker itself when the token is absent (client_runner
        # sets data_broker=broker as a fallback in that case — acceptable
        # because both are then the same object and no silent sandbox leak
        # occurs). This attribute is used only for quote reads, never for
        # order submission.
        if data_broker is not None and data_broker is not broker:
            self.broker.data_broker = data_broker
        self.contract_selector = contract_selector  # wired for breach-time selection of deferred overnight signals
        self.email              = email

        # P0 hotfix — APExecutionCore.client_id was never set, causing
        # AttributeError at breach-time contract selection log calls.
        # `email` IS the canonical client identity — it is the client_id
        # (e.g. "jasoncosby1@gmail.com") passed from ClientRunner.
        # We also set execution_mode as a stable alias of self.mode for
        # use in breach-time logs that interpolate both attributes.
        self.client_id    = email or os.getenv("SINGLE_CLIENT_EMAIL") or ""
        # client_email: prefer the dedicated field if a subclass has set it
        # separately; otherwise alias to client_id (same email address).
        self.client_email = (
            email
            or os.getenv("SINGLE_CLIENT_EMAIL")
            or ""
        )
        self.execution_mode = self.mode  # canonical alias for breach-time log fields
        self.position_manager   = position_manager
        self.order_state_machine = order_state_machine
        self._pos_lock = threading.Lock()
        self._position_count = 0

        # Signal intelligence store + tracker
        # Tracker deduplicates strictly by signal_id so one per client is safe.
        self.store   = APSignalStore(supabase_client, client_email=email)
        self.tracker = APSignalTracker(supabase_client, store=self.store)

        # PR-B / FIX-3 + FIX-4: Submodules constructed AFTER canonical
        # mode + master_control are known. APExitEngine receives
        # master_control at construction time (FIX-4) so the engine
        # never exists without a risk-control reference. APProofLogger
        # receives the canonical mode the first time.
        # PR-C / BUG-EW-5: pass canonical mode to the watcher at
        # construct time so its overnight-revalidation fail-closed-on-LIVE
        # branch actually fires for LIVE clients. Previously the watcher
        # had no self.mode attribute and silently used PAPER fail-open
        # for every client — a live quote outage would arm setups that
        # should have been invalidated.
        self.entry_watcher = APEntryWatcher(
            broker,
            order_state_machine=self.order_state_machine,
            mode=self.mode,  # canonical mode from master_control
        )
        self.exit_eng    = APExitEngine(
            broker,
            email=email,
            data_broker=data_broker,
            master_control=self.master_control,  # FIX-4: construct-time wiring
        )
        self.feedback    = APFeedbackLoop(supabase_client, DISCORD_WEBHOOK_URL, signal_store=self.store)
        self.shadow      = APShadowTracker(supabase_client, DISCORD_WEBHOOK_URL)
        self._sector_counts: dict[str, int] = {}
        self._sector_lock   = threading.Lock()
        self.proof          = APProofLogger(
            supabase_client=supabase_client,
            client_email=email,
            mode="paper" if self.paper else "live",  # canonical from the start
        )
        try:
            from ap_edge_intelligence import APTradeLogger as _ATL
            self._edge_logger = _ATL()
        except Exception:
            self._edge_logger = None

        # PR-B / FIX-3: master_control validation, canonical mode
        # derivation, and submodule construction (above) all happen
        # BEFORE this point. The old "MASTER CONTROL" + "Mode override"
        # blocks that used to live here are gone — work is done upfront.

        # Wire watcher callbacks
        self.entry_watcher.on_trigger    = self._on_entry_trigger
        self.entry_watcher.on_expire     = self._on_signal_expire
        self.entry_watcher.on_invalidate = self._on_signal_invalidate

        # Wire exit callbacks
        self.exit_eng.on_exit  = self._on_position_close
        self.exit_eng.on_scale = self._on_position_scale
        # FIX 2: broker-confirmed fill callback — writes proof with actual fill price
        self.exit_eng.on_exit_fill_confirmed = self._finalize_proof
        # PR-B / FIX-4 belt-and-suspenders: master_control was wired at
        # exit-engine construction time above; re-assign here only if
        # somehow missing (idempotent no-op in normal path). One-way
        # reference: exit engine READS the flag, never sets it.
        try:
            if getattr(self.exit_eng, "master_control", None) is None:
                self.exit_eng.master_control = self.master_control
        except Exception as _e:
            log.warning("exit_eng_master_control_wire_failed: %s", _e)

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

    # ── Diagnostic-only helper (PR hotfix/breach-block-diagnostics) ──────────
    # Emits a single structured log line for every silent block / exception
    # path in _breach_risk_check and _on_entry_trigger. The bot is working;
    # this PR adds zero behavior changes. Operators grep Render logs for
    # BREACH_RISK_CHECK_BLOCKED / BREACH_RISK_CHECK_EXCEPTION /
    # WATCHER_ON_TRIGGER_RETURNED / WATCHER_ON_TRIGGER_EXCEPTION /
    # ENTRY_TRIGGER_BLOCKED_RETURN to diagnose stuck PENDING_TRIGGER rows.
    def _emit_breach_diag(
        self,
        event: str,
        *,
        watched: "WatchedSignal",
        reason: str,
        positions_open: object = "n/a",
        pending_entries: object = "n/a",
        max_positions: object = "n/a",
        current_total_exposure: object = "n/a",
        remaining_total_cap: object = "n/a",
        mc_block_reason: str = "",
        exception_type: str = "",
        exception_message: str = "",
        level: str = "warning",
    ) -> None:
        """Emit one structured diagnostic line. Never raises.

        Field set is fixed across emissions so log-grep stays stable:
        client_id, local_order_id, signal_id, symbol, contract, reason,
        positions_open, pending_entries, max_positions, current_total_exposure,
        remaining_total_cap, mc_block_reason, exception_type, exception_message,
        execution_mode.
        """
        try:
            sig             = getattr(watched, "signal", {}) or {}
            client_id_val   = (
                getattr(self, "client_id", None)
                or getattr(self, "email", None)
                or sig.get("client_email")
                or "n/a"
            )
            local_order_id  = sig.get("local_order_id") or "n/a"
            signal_id       = sig.get("signal_id") or "n/a"
            symbol          = getattr(watched, "ticker", None) or sig.get("ticker") or "n/a"
            contract        = (
                (sig.get("plan") or {}).get("contract_symbol")
                or sig.get("contract_symbol")
                or sig.get("contract")
                or "n/a"
            )
            execution_mode  = getattr(self, "mode", "n/a")

            msg = (
                f"{event} client_id={client_id_val} local_order_id={local_order_id} "
                f"signal_id={signal_id} symbol={symbol} contract={contract} "
                f"reason={reason} execution_mode={execution_mode} "
                f"positions_open={positions_open} pending_entries={pending_entries} "
                f"max_positions={max_positions} "
                f"current_total_exposure={current_total_exposure} "
                f"remaining_total_cap={remaining_total_cap} "
                f"mc_block_reason={mc_block_reason or 'n/a'} "
                f"exception_type={exception_type or 'n/a'} "
                f"exception_message={(exception_message or 'n/a')[:200]}"
            )
            if level == "critical":
                log.critical(msg)
            elif level == "error":
                log.error(msg)
            elif level == "info":
                log.info(msg)
            else:
                log.warning(msg)
        except Exception:  # pragma: no cover — never let diagnostics break flow
            try:
                log.warning("BREACH_DIAG_EMIT_FAILED event=%s", event)
            except Exception:
                pass

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
            self._emit_breach_diag(
                "BREACH_RISK_CHECK_BLOCKED",
                watched=watched,
                reason="kill_switch_active",
                max_positions=getattr(self, "_max_positions", "n/a"),
                level="critical",
            )
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
                    self._emit_breach_diag(
                        "BREACH_RISK_CHECK_BLOCKED",
                        watched=watched,
                        reason="master_control_kill_switch_active",
                        max_positions=getattr(self, "_max_positions", "n/a"),
                        level="critical",
                    )
                    return False
            except Exception as exc:
                log.warning("[%s] Kill-switch check failed at breach: %s", ticker, exc)
                self._emit_breach_diag(
                    "BREACH_RISK_CHECK_EXCEPTION",
                    watched=watched,
                    reason="kill_switch_check_exception",
                    max_positions=getattr(self, "_max_positions", "n/a"),
                    exception_type=type(exc).__name__,
                    exception_message=str(exc),
                    level="warning",
                )

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
            self._emit_breach_diag(
                "BREACH_RISK_CHECK_BLOCKED",
                watched=watched,
                reason="positions_full_at_breach",
                positions_open=open_count,
                pending_entries=pending_entries,
                max_positions=self._max_positions,
                level="info",
            )
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
                self._emit_breach_diag(
                    "BREACH_RISK_CHECK_BLOCKED",
                    watched=watched,
                    reason="approved_plan_missing_at_breach_revalidation",
                    positions_open=open_count,
                    pending_entries=pending_entries,
                    max_positions=self._max_positions,
                    level="critical",
                )
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
                    # Best-effort capacity numbers for diagnostics
                    _cur_total_exp = (
                        getattr(reval, "current_total_exposure", None)
                        if hasattr(reval, "current_total_exposure") else "n/a"
                    )
                    _rem_total_cap = (
                        getattr(reval, "remaining_total_cap", None)
                        if hasattr(reval, "remaining_total_cap") else "n/a"
                    )
                    self._emit_breach_diag(
                        "BREACH_RISK_CHECK_BLOCKED",
                        watched=watched,
                        reason="exposure_revalidation_blocked",
                        positions_open=open_count,
                        pending_entries=pending_entries,
                        max_positions=self._max_positions,
                        current_total_exposure=(
                            _cur_total_exp if _cur_total_exp is not None else "n/a"
                        ),
                        remaining_total_cap=(
                            _rem_total_cap if _rem_total_cap is not None else "n/a"
                        ),
                        mc_block_reason=str(reason),
                        level="warning",
                    )
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
                    self._emit_breach_diag(
                        "BREACH_RISK_CHECK_EXCEPTION",
                        watched=watched,
                        reason="exposure_revalidation_error_live",
                        positions_open=open_count,
                        pending_entries=pending_entries,
                        max_positions=self._max_positions,
                        exception_type=type(exc).__name__,
                        exception_message=str(exc),
                        level="critical",
                    )
                    return False
                log.warning("[%s] PAPER breach exposure revalidation failed open: %s", ticker, exc)
                self._emit_breach_diag(
                    "BREACH_RISK_CHECK_EXCEPTION",
                    watched=watched,
                    reason="exposure_revalidation_error_paper_fail_open",
                    positions_open=open_count,
                    pending_entries=pending_entries,
                    max_positions=self._max_positions,
                    exception_type=type(exc).__name__,
                    exception_message=str(exc),
                    level="warning",
                )

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
        # PR-B / FIX-5: exit-engine callback + master_control preflight.
        # If these are missing, positions accumulate with no exits or
        # no risk-control reference — the worst-possible failure mode.
        if self.exit_eng is not None:
            if getattr(self.exit_eng, "on_exit", None) is None:
                problems.append("exit_eng_on_exit_not_wired")
            if getattr(self.exit_eng, "on_scale", None) is None:
                problems.append("exit_eng_on_scale_not_wired")
            if getattr(self.exit_eng, "master_control", None) is None:
                problems.append("exit_eng_master_control_not_wired")
        # PR-B / FIX-5: LIVE mode must not start without a position_manager.
        # In paper, position_manager is optional (local fallback counter
        # is acceptable). In LIVE, broker/DB truth is required.
        if not self.paper and self.position_manager is None:
            problems.append("position_manager_missing")
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

        # P0 hotfix — resolve client identity from the signal dict first, then
        # fall back to self.client_id (set in __init__), then self.email.
        # This ensures breach-time logs never throw AttributeError and that
        # the most-specific client identity (from the row/signal) is used even
        # if self.client_id is somehow stale or missing.
        _breach_client_id = (
            str(sig.get("client_id") or sig.get("client_email") or "").strip()
            or getattr(self, "client_id", None)
            or self.email
            or ""
        )
        if not _breach_client_id:
            # Hard guard: log with ticker and write a sentinel last_error so
            # the failure is diagnosable rather than an opaque AttributeError.
            log.warning(
                "[%s] BREACH_TIME_CONTRACT_SELECTION_FAILED "
                "reason=missing_client_id — client identity cannot be resolved; "
                "breach-time selection will continue but logs will be incomplete",
                ticker,
            )

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
            self._emit_breach_diag(
                "ENTRY_TRIGGER_BLOCKED_RETURN",
                watched=watched,
                reason="breach_risk_check_false",
                max_positions=getattr(self, "_max_positions", "n/a"),
                level="info",
            )
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
            _reason = "osm_missing_submit_existing_entry"
            funnel.inc("order_failed")
            if signal_id:
                self.store.update_signal_fields(signal_id, {
                    "decision_status": "blocked_at_breach",
                    "context_notes": _reason,
                })
            self._cleanup_pending_entry_order(watched, action="expire", reason=_reason)
            return

        def _terminalize_breach_failure(
            reason: str,
            *,
            cleanup_action: str = "expire",
            meta_patch: dict | None = None,
            decision_status: str = "blocked_at_breach",
            context_notes: str | None = None,
            funnel_key: str = "order_failed",
        ) -> None:
            if funnel_key:
                funnel.inc(funnel_key)
            if signal_id:
                self.store.update_signal_fields(signal_id, {
                    "decision_status": decision_status,
                    "context_notes": context_notes or reason,
                })
            if queue_local_order_id and self.order_state_machine is not None and meta_patch:
                try:
                    update_meta = getattr(self.order_state_machine, "update_order_meta", None)
                    if callable(update_meta):
                        update_meta(queue_local_order_id, meta_patch)
                except Exception as _meta_exc:
                    log.warning("[%s] breach failure meta persist failed: %s", ticker, _meta_exc)
            self._cleanup_pending_entry_order(watched, action=cleanup_action, reason=reason)
            return

        def _terminalize_deferred_breach_failure(reason: str, *, extra_meta: dict | None = None) -> None:
            """Best-effort cleanup for deferred breach failures.

            Acceptance contract:
              a trigger_ready deferred order must either submit with a real
              contract, or end terminal with last_error populated.
            """
            meta_patch = {
                "deferred_breach_failure": True,
                "deferred_breach_reason": reason,
                "local_order_id": queue_local_order_id,
            }
            if extra_meta:
                meta_patch.update(extra_meta)
            _terminalize_breach_failure(
                reason,
                cleanup_action="expire",
                meta_patch=meta_patch,
                context_notes=reason,
            )
            return

        # ── PR3 (no-silent-deferred-trigger-exits): canonical terminal outcome ──
        # Every triggered deferred row MUST leave exactly one explicit terminal
        # outcome from this taxonomy so no trigger returns silently. This is
        # observability only — it records the outcome that the existing code
        # paths already produce; it does not change any decision or order action.
        #
        # Outcomes:
        #   BREACH_CONTRACT_SELECTED       real OCC contract chosen, proceeding
        #   BREACH_RISK_CHECK_BLOCKED      _breach_risk_check returned False
        #   BREACH_SELECTOR_RETURNED_NONE  selector.select() returned None
        #   BREACH_SELECTOR_EXCEPTION      selector.select() raised
        #   BREACH_SUBMISSION_SKIPPED      passed selection but submit not attempted
        #   BREACH_BROKER_SUBMITTED        order handed to broker submit path
        #   NO_VALID_PLAYBOOK_DTE_CONTRACT no survivor in any evaluated DTE bucket
        #   UNTRADEABLE_FOR_ACCOUNT_SIZE   quality contract exists but exceeds budget
        #   DATA_MISSING_OI_VOLUME         chain returned with zero OI/volume fields
        #
        # _deferred_outcome["emitted"] is the sentinel the post-trigger guard checks.
        _deferred_outcome = {"emitted": False, "outcome": None, "is_deferred": False}

        def _emit_deferred_outcome(
            outcome: str,
            *,
            reason: str = "",
            contract: str = "",
            broker_order_id: str = "",
            extra: dict | None = None,
        ) -> None:
            """Emit EXACTLY ONE canonical terminal outcome for a triggered
            DEFERRED row. Never raises. Always includes local_order_id +
            signal_id so the event joins back to the order row.

            Two guards (per review amendment):
              1. Deferred-only: does nothing unless this trigger is a deferred
                 entry (_deferred_outcome["is_deferred"] set True once known).
                 Non-deferred triggers never emit a deferred outcome.
              2. Exactly-once: the first emission wins; later calls are ignored
                 so a row can never carry two terminal outcomes.
            """
            if not _deferred_outcome.get("is_deferred"):
                return
            if _deferred_outcome.get("emitted"):
                return
            _deferred_outcome["emitted"] = True
            _deferred_outcome["outcome"] = outcome
            try:
                payload = {
                    "outcome": outcome,
                    "local_order_id": queue_local_order_id or "",
                    "signal_id": signal_id or "",
                    "symbol": ticker,
                    "execution_mode": getattr(self, "mode", "n/a"),
                    "reason": reason or "",
                    "contract": contract or "",
                    "broker_order_id": broker_order_id or "",
                }
                if extra:
                    for _k, _v in extra.items():
                        payload[_k] = _v
                _fields = " ".join(f"{k}={v}" for k, v in payload.items())
                if outcome in ("BREACH_CONTRACT_SELECTED", "BREACH_BROKER_SUBMITTED"):
                    log.info("DEFERRED_TRIGGER_OUTCOME %s", _fields)
                else:
                    log.warning("DEFERRED_TRIGGER_OUTCOME %s", _fields)
            except Exception:
                try:
                    log.warning("DEFERRED_TRIGGER_OUTCOME_EMIT_FAILED outcome=%s", outcome)
                except Exception:
                    pass

        # 3) Recover the already-approved queue/OSM plan.
        approved_plan = self._recover_plan_for_revalidation(watched)
        if approved_plan is None:
            log.critical("[%s] PRODUCTION_ENTRY_BLOCK — approved plan missing after breach revalidation", ticker)
            # NOTE: _deferred is not yet known here, and an invalid/missing plan
            # is not a deferred-selection outcome — do not emit a deferred
            # outcome. _terminalize_breach_failure records this terminal state.
            _terminalize_breach_failure("approved_plan_missing_after_revalidation")
            return

        # 3b) Breach-time contract selection for overnight deferred signals.
        # Pre-market option chains have zero bids — overnight_reeval cannot select
        # contracts before 9:30 AM ET. Those signals are armed with
        # contract_deferred=True. Here at breach (market open, live quotes) we
        # select the contract before the limit_price and contract_symbol checks.
        _sig_meta   = getattr(approved_plan, "metadata", {}) or {}
        _sig_dict   = sig or {}
        _candidate_audit = None  # Item 3 — set if breach-time selection runs
        _contract_sym_raw = str(getattr(approved_plan, "contract_symbol", "") or "").strip()
        _deferred   = (
            bool(_sig_meta.get("contract_deferred"))
            or bool(_sig_dict.get("contract_deferred"))
            or not _contract_sym_raw
            or _contract_sym_raw.upper().startswith("DEFERRED:")  # safety: never submit placeholder
        )
        # Enable deferred-outcome emission only for deferred triggers (amendment:
        # guard deferred logs with _deferred). Non-deferred entries never emit a
        # deferred terminal outcome.
        _deferred_outcome["is_deferred"] = bool(_deferred)
        if _deferred:
            if self.contract_selector is None:
                _reason = "contract_deferred_no_selector"
                log.critical(
                    "[%s] DEFERRED_BREACH_CONTRACT_FAILED — %s",
                    ticker, _reason,
                )
                _emit_deferred_outcome(
                    "BREACH_SELECTOR_RETURNED_NONE",
                    reason=_reason,
                )
                _terminalize_deferred_breach_failure(
                    _reason,
                    extra_meta={"failure_stage": "deferred_contract_selection"},
                )
                log.critical(
                    "[%s] PRODUCTION_ENTRY_BLOCK — contract_deferred=True but no "
                    "contract_selector wired into execution core",
                    ticker,
                )
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
                _sel_contract = str(getattr(_sel, "contract_symbol", "") or "").strip()
                _live_contract = str(getattr(approved_plan, "contract_symbol", "") or "").strip()
                _plan_is_placeholder = (not _live_contract) or _live_contract.upper().startswith("DEFERRED:")
                _sel_is_real = bool(_sel_contract) and not _sel_contract.upper().startswith("DEFERRED:")

                if _sel_is_real and _plan_is_placeholder:
                    try:
                        approved_plan.contract_symbol = _sel_contract
                        _sel_price = (
                            getattr(_sel, "execution_price_per_share", None)
                            or getattr(_sel, "ask", None)
                            or getattr(_sel, "mid", None)
                        )
                        if _sel_price:
                            approved_plan.limit_price = float(_sel_price)
                        _sel_qty = int(getattr(_sel, "affordable_contracts", 0) or 0)
                        if _sel_qty > 0:
                            approved_plan.contracts = _sel_qty
                            _prem_per_contract = float(getattr(_sel, "premium_per_contract", 0) or 0)
                            if _prem_per_contract > 0:
                                approved_plan.max_position_usd = _sel_qty * _prem_per_contract
                        _live_contract = str(getattr(approved_plan, "contract_symbol", "") or "").strip()
                    except Exception as _copy_exc:
                        log.warning("[%s] deferred breach selected contract copy failed: %s", ticker, _copy_exc)

                # ── P0 (hotfix/deferred-breach-selector-reasons, amended): ────
                # Shared helper — builds the selector audit dict and extracts
                # the canonical reason_code/stage from get_last_failure() for
                # use by BOTH deferred-breach failure paths below.
                # get_last_failure() is observability-only and never raises.
                # After a failed select() it holds the last REJECT emitted.
                # After a successful select() that failed to copy (unresolved
                # DEFERRED: placeholder) it is None — the caller must supply
                # an override_reason_code in that case.
                def _build_deferred_selector_audit(
                    *,
                    override_reason_code: str | None = None,
                    override_stage: str | None = None,
                ) -> tuple[str, dict]:
                    """
                    Returns (last_error_string, deferred_selector_audit_dict).
                    Reads selector.get_last_failure() and merges with any
                    caller-supplied override values.
                    override_reason_code is used when the selector succeeded
                    but post-selection validation failed (DEFERRED unresolved).
                    """
                    _sf = None
                    _rc = None
                    _st = None
                    _ex = None
                    try:
                        if hasattr(self.contract_selector, "get_last_failure"):
                            _sf = self.contract_selector.get_last_failure()
                        if isinstance(_sf, dict):
                            _rc = _sf.get("reason_code") or None
                            _st = _sf.get("stage") or None
                            _ex = _sf.get("explanation") or None
                    except Exception as _gf_exc:
                        log.debug(
                            "[%s] get_last_failure() read failed (non-fatal): %s",
                            ticker, _gf_exc,
                        )
                    # Override takes precedence when the selector itself succeeded
                    # (no REJECT emitted) but downstream validation failed.
                    if override_reason_code:
                        _rc = override_reason_code
                    if override_stage:
                        _st = override_stage

                    _error = (
                        f"breach_time_contract_selection:{_rc}"
                        if _rc
                        else "breach_time_contract_selection_no_result"
                    )
                    import datetime as _dt
                    _audit: dict = {
                        "reason_code":         _rc,
                        "stage":               _st,
                        "explanation":         _ex,
                        "budget":              float(getattr(approved_plan, "max_position_usd", 0) or 0),
                        "ticker":              ticker,
                        "side":                str(getattr(approved_plan, "side", "") or ""),
                        "execution_mode":      str(getattr(approved_plan, "execution_mode", "") or ""),
                        "contract_before":     _contract_sym_raw or None,
                        "selected_contract":   _sel_contract or None,
                        "timestamp":           _dt.datetime.now(_dt.timezone.utc).isoformat(),
                        "raw_selector_reason": (
                            _sf.get("raw_reason") if isinstance(_sf, dict) else None
                        ),
                    }
                    return _error, _audit

                # Path A: selector returned None OR live contract is still empty
                # after copy-back. The selector's REJECT is the blocker.
                if not _sel or not _live_contract:
                    _reason, _deferred_selector_audit = _build_deferred_selector_audit()
                    log.critical(
                        "DEFERRED_BREACH_CONTRACT_SELECTION_FAILED "
                        "client=%s symbol=%s side=%s execution_mode=%s "
                        "budget=%.2f reason=%s stage=%s order_id=%s signal_id=%s",
                        _breach_client_id,
                        ticker,
                        str(getattr(approved_plan, "side", "") or ""),
                        str(getattr(approved_plan, "execution_mode", "") or ""),
                        float(getattr(approved_plan, "max_position_usd", 0) or 0),
                        _reason,
                        _deferred_selector_audit.get("stage") or "unknown",
                        queue_local_order_id or "",
                        str(getattr(approved_plan, "signal_id", "") or ""),
                    )
                    log.critical(
                        "BREACH_TIME_CONTRACT_SELECTION_FAILED "
                        "client=%s ticker=%s reason=%s",
                        _breach_client_id, ticker, _reason,
                    )
                    _emit_deferred_outcome(
                        (
                            "DATA_MISSING_OI_VOLUME"
                            if "vol0_oi0" in str(_reason)
                            else "BREACH_SELECTOR_RETURNED_NONE"
                        ),
                        reason=_reason,
                        extra={"stage": _deferred_selector_audit.get("stage") or "unknown"},
                    )
                    _terminalize_deferred_breach_failure(
                        _reason,
                        extra_meta={
                            "failure_stage":           "deferred_contract_selection",
                            "selected_contract":       _sel_contract or None,
                            "deferred_selector_audit": _deferred_selector_audit,
                        },
                    )
                    log.critical(
                        "[%s] PRODUCTION_ENTRY_BLOCK — breach-time contract selection "
                        "returned no contract (reason=%s stage=%s budget=%.2f)",
                        ticker, _reason,
                        _deferred_selector_audit.get("stage") or "unknown",
                        float(getattr(approved_plan, "max_position_usd", 0) or 0),
                    )
                    return

                # Path B: selector returned a result but the plan copy-back failed
                # or the selector wrote a DEFERRED: placeholder — unresolved.
                # The selector itself did not emit a REJECT (it returned a value),
                # so get_last_failure() is None; supply override reason code.
                if _live_contract.upper().startswith("DEFERRED:"):
                    _reason, _deferred_selector_audit = _build_deferred_selector_audit(
                        override_reason_code="DEFERRED_UNRESOLVED_AT_BREACH",
                        override_stage="deferred_copy_back",
                    )
                    # Embed the unresolved placeholder in the reason string so
                    # the order row records which contract was stuck.
                    _reason = f"breach_time_contract_selection:DEFERRED_UNRESOLVED_AT_BREACH:{_live_contract}"
                    _deferred_selector_audit["unresolved_placeholder"] = _live_contract
                    log.critical(
                        "DEFERRED_BREACH_CONTRACT_SELECTION_FAILED "
                        "client=%s symbol=%s side=%s execution_mode=%s "
                        "budget=%.2f reason=%s stage=%s order_id=%s signal_id=%s",
                        _breach_client_id,
                        ticker,
                        str(getattr(approved_plan, "side", "") or ""),
                        str(getattr(approved_plan, "execution_mode", "") or ""),
                        float(getattr(approved_plan, "max_position_usd", 0) or 0),
                        _reason,
                        _deferred_selector_audit.get("stage") or "unknown",
                        queue_local_order_id or "",
                        str(getattr(approved_plan, "signal_id", "") or ""),
                    )
                    _emit_deferred_outcome(
                        "BREACH_SELECTOR_RETURNED_NONE",
                        reason=_reason,
                        contract=_live_contract,
                        extra={"stage": "deferred_copy_back"},
                    )
                    _terminalize_deferred_breach_failure(
                        _reason,
                        extra_meta={
                            "failure_stage":           "deferred_contract_selection",
                            "selected_contract":       _sel_contract or None,
                            "approved_contract":       _live_contract,
                            "deferred_selector_audit": _deferred_selector_audit,
                        },
                    )
                    log.critical(
                        "BREACH_TIME_CONTRACT_SELECTION_FAILED "
                        "client=%s ticker=%s reason=%s",
                        _breach_client_id, ticker, _reason,
                    )
                    log.critical(
                        "[%s] PRODUCTION_ENTRY_BLOCK — contract selector left placeholder "
                        "unresolved: %s (reason=%s)",
                        ticker, _live_contract, _reason,
                    )
                    return
                _emit_deferred_outcome(
                    "BREACH_CONTRACT_SELECTED",
                    contract=_live_contract,
                    extra={
                        "limit_price": float(getattr(approved_plan, "limit_price", 0) or 0),
                        "qty": int(getattr(approved_plan, "contracts", 0) or 0),
                    },
                )
                log.info(
                    "[%s] DEFERRED_BREACH_CONTRACT_SELECTED — contract=%s limit=%.2f qty=%s",
                    ticker,
                    _live_contract,
                    float(getattr(approved_plan, "limit_price", 0) or 0),
                    int(getattr(approved_plan, "contracts", 0) or 0),
                )
                log.info(
                    "[%s] Breach-time contract selected: %s @ $%.2f x%s",
                    ticker, _live_contract,
                    float(getattr(approved_plan, "limit_price", 0) or 0),
                    int(getattr(approved_plan, "contracts", 1) or 1),
                )
                # Required structured log for ops confirmation that the existing
                # PENDING_TRIGGER row was finalized in-place (not a new order).
                log.info(
                    "BREACH_TIME_CONTRACT_FINALIZED "
                    "client=%s ticker=%s local_order_id=%s "
                    "old_contract=%s new_contract=%s limit=%.4f qty=%s",
                    _breach_client_id,
                    ticker,
                    queue_local_order_id or "",
                    _contract_sym_raw or "DEFERRED:?",
                    _live_contract,
                    float(getattr(approved_plan, "limit_price", 0) or 0),
                    int(getattr(approved_plan, "contracts", 1) or 1),
                )
                # Item 3 — capture the selector candidate audit (EVIDENCE ONLY).
                # Persisted into orders.meta after a successful submit below.
                try:
                    _candidate_audit = getattr(_sel, "candidate_audit", None)
                except Exception:
                    _candidate_audit = None
            except Exception as _cs_err:
                _reason = f"breach_time_contract_selection_error:{_cs_err}"
                log.critical(
                    "[%s] DEFERRED_BREACH_CONTRACT_FAILED — %s",
                    ticker, _reason,
                )
                _emit_deferred_outcome(
                    "BREACH_SELECTOR_EXCEPTION",
                    reason=_reason,
                    extra={"exception_type": type(_cs_err).__name__},
                )
                _terminalize_deferred_breach_failure(
                    _reason,
                    extra_meta={"failure_stage": "deferred_contract_selection"},
                )
                log.critical(
                    "[%s] PRODUCTION_ENTRY_BLOCK — breach-time contract selection "
                    "error: %s",
                    ticker, _cs_err,
                )
                return

        # 4) Require the approved plan to carry a valid limit price (used as
        #    the drift baseline — the actual submit limit is re-anchored to the
        #    current option ask in step 4b below).
        _plan_limit = getattr(approved_plan, "limit_price", None)
        try:
            _plan_limit = float(_plan_limit)
        except Exception:
            _plan_limit = 0.0

        if _plan_limit <= 0:
            log.critical("[%s] PRODUCTION_ENTRY_BLOCK — approved plan missing valid limit_price", ticker)
            _terminalize_breach_failure("approved_plan_missing_limit_price")
            return

        # Optional hard guards: require contract + nonzero qty from the approved plan.
        approved_contract = str(getattr(approved_plan, "contract_symbol", "") or "").strip()
        try:
            approved_qty = int(getattr(approved_plan, "contracts", 0) or 0)
        except Exception:
            approved_qty = 0

        if not approved_contract:
            log.critical("[%s] PRODUCTION_ENTRY_BLOCK — approved plan missing contract_symbol", ticker)
            _terminalize_breach_failure("approved_plan_missing_contract_symbol")
            return

        if approved_qty <= 0:
            log.critical("[%s] PRODUCTION_ENTRY_BLOCK — approved plan has invalid contracts=%s", ticker, approved_qty)
            _terminalize_breach_failure(f"approved_plan_invalid_contracts={approved_qty}")
            return

        # 4b) P0 FIX: refresh the option contract ask immediately before submit.
        #
        # ROOT CAUSE FIXED HERE: the prior code submitted at approved_plan.limit_price
        # which was set at contract-selection time. For overnight/queued signals that
        # can be hours old. The option ask has moved. Tradier receives a limit far below
        # the current ask. The order sits unfilled. This block brings the watcher-breach
        # path to parity with process_signal() in ap/execution.py which already does
        # this correctly via _refresh_ask_at_submit().
        try:
            from ap.execution import _refresh_ask_at_submit as _breach_refresh_ask
        except ImportError:
            _breach_refresh_ask = None

        _entry_pricing_decision = "ASK_CROSSED"
        _submit_ask = 0.0
        _refresh_ok = False
        _refresh_reason = "import_failed"
        _submit_quote_fields = {
            "submit_bid": None, "submit_ask": None,
            "submit_last": None, "submit_mid": None, "spread_pct": None,
        }

        if _breach_refresh_ask is not None:
            _submit_ask, _quote_age_ms, _refresh_ok, _refresh_reason, _submit_quote_fields =                 _breach_refresh_ask(self.broker, approved_contract)
        else:
            # _refresh_ask_at_submit unavailable — treat as refresh failure
            _refresh_ok = False
            _refresh_reason = "import_failed"
            _quote_age_ms = 0

        if not _refresh_ok or _submit_ask <= 0:
            # Fail closed: never submit at a stale plan price when we can't get
            # a fresh ask. The retry engine can re-arm after a delay.
            _entry_pricing_decision = "QUOTE_REFRESH_FAILED" if not _refresh_ok else "MISSING_ASK"
            log.critical(
                "[%s] ENTRY_PRICING_BLOCK — breach-time quote refresh failed "
                "contract=%s reason=%s refresh_ok=%s submit_ask=%s | blocking submit",
                ticker, approved_contract, _refresh_reason, _refresh_ok, _submit_ask,
            )
            _terminalize_breach_failure(f"breach_quote_refresh_failed:{_refresh_reason}")
            return

        # Spread sanity guard (wide spread = illiquid contract, skip).
        _spread_pct = _submit_quote_fields.get("spread_pct") or 0.0
        _max_spread = float(os.getenv("ENTRY_MAX_SPREAD_PCT", "0.50"))
        if _spread_pct > _max_spread:
            _entry_pricing_decision = "SPREAD_TOO_WIDE"
            log.warning(
                "[%s] ENTRY_PRICING_BLOCK — spread too wide at breach "
                "contract=%s spread_pct=%.3f max=%.3f | blocking submit",
                ticker, approved_contract, _spread_pct, _max_spread,
            )
            _terminalize_breach_failure(f"breach_spread_too_wide:{_spread_pct:.3f}")
            return

        # Drift guard: if the current ask has run more than ENTRY_MAX_PRICE_DRIFT_PCT_FROM_PLAN
        # above the original plan price, the move has already happened — don't chase.
        _drift_pct = (_submit_ask / _plan_limit) - 1.0 if _plan_limit > 0 else 0.0
        if _drift_pct > ENTRY_MAX_PRICE_DRIFT_PCT_FROM_PLAN:
            _entry_pricing_decision = "ENTRY_PRICE_DRIFT_TOO_HIGH"
            log.warning(
                "[%s] ENTRY_PRICING_BLOCK — ask drifted too far from plan at breach "
                "contract=%s plan_limit=%.2f submit_ask=%.2f drift=%.1f%% max=%.1f%% | blocking",
                ticker, approved_contract, _plan_limit, _submit_ask,
                _drift_pct * 100, ENTRY_MAX_PRICE_DRIFT_PCT_FROM_PLAN * 100,
            )
            _terminalize_breach_failure(
                (
                    f"breach_entry_price_drift_too_high:"
                    f"plan={_plan_limit:.2f} ask={_submit_ask:.2f} "
                    f"drift={_drift_pct*100:.1f}%"
                )
            )
            return

        # Compute the broker-submitted limit: ask + mode-appropriate crossing pennies.
        _ask_cross = ENTRY_PAPER_ASK_CROSS_CENTS if self.paper else ENTRY_LIVE_ASK_CROSS_CENTS
        submit_limit = round(_submit_ask + _ask_cross, 2)

        # Keep approved_plan in sync so OSM and DB record the correct price.
        try:
            approved_plan.limit_price = submit_limit
        except Exception:
            pass  # plan is a namespace; attribute assignment is always valid

        # Build the entry pricing audit to persist in orders.meta post-submit.
        _entry_pricing_audit = {
            "entry_pricing_audit":         True,
            "selected_contract":           approved_contract,
            "original_limit_price":        _plan_limit,
            "original_selector_ask":       _plan_limit,  # plan_limit = selector ask at selection time
            "submit_bid":                  _submit_quote_fields.get("submit_bid"),
            "submit_mid":                  _submit_quote_fields.get("submit_mid"),
            "submit_ask":                  float(_submit_ask),
            "submit_last":                 _submit_quote_fields.get("submit_last"),
            "submitted_limit_price":       float(submit_limit),
            "limit_vs_submit_ask_pct":     round((_ask_cross / _submit_ask) * 100, 4) if _submit_ask > 0 else None,
            "quote_refreshed_at_submit":   True,
            "quote_age_ms":                int(_quote_age_ms),
            "spread_pct_at_submit":        _submit_quote_fields.get("spread_pct"),
            "sandbox_mode":                self.paper,
            "broker_base_url":             getattr(getattr(self.broker, "cfg", None), "base_url", None),
            "pricing_rule":                "PAPER_ASK_CROSS" if self.paper else "LIVE_ASK_CROSS",
            "ask_cross_cents":             _ask_cross,
            "drift_from_plan_pct":         round(_drift_pct * 100, 4),
            "entry_price_decision":        _entry_pricing_decision,
            "attempt_number":              0,
            "retry_reprice_count":         0,
        }

        # 5) Submit with the refreshed, ask-anchored limit price.
        log.info(
            "[%s] %s — submitting EXISTING queue order | local=%s contract=%s "
            "plan_ask=%.2f submit_ask=%.2f cross=+%.2f limit=%.2f drift=%.1f%% x%s",
            ticker,
            "PAPER" if self.paper else "LIVE",
            queue_local_order_id,
            approved_contract,
            _plan_limit,
            _submit_ask,
            _ask_cross,
            submit_limit,
            _drift_pct * 100,
            approved_qty,
        )
        if _deferred:
            log.info(
                "[%s] DEFERRED_BREACH_SUBMIT_ATTEMPT — local=%s contract=%s qty=%s limit=%.2f",
                ticker,
                queue_local_order_id,
                approved_contract,
                approved_qty,
                submit_limit,
            )

        # ── P0 follow-up: Entry Confirmation Preflight ──────────────────────────
        # Runs AFTER live ask refresh (live quote available), BEFORE broker submit.
        # For plans with confirmation_required=True (set by PR74 Hybrid Gate),
        # checks: quote age, spread, option fade, underlying reversal.
        # Non-blocking for non-client plans — fast-path if confirmation_required=False.
        _confirm_meta = {}
        try:
            from ap_entry_confirmation import check_entry_confirmation
            _sig_for_confirm = watched.signal or {}
            _plan_meta_for_confirm = getattr(approved_plan, "metadata", {}) or {}
            _sandbox = bool(
                _plan_meta_for_confirm.get("sandbox_mode")
                or getattr(self.broker, "sandbox", False)
            )
            _underlying_last = None
            try:
                # Re-use the latest watcher underlying quote if available
                _ul_ask = getattr(watched, "last_quote_ask", None)
                _ul_bid = getattr(watched, "last_quote_bid", None)
                if _ul_ask and _ul_bid and _ul_ask > 0 and _ul_bid > 0:
                    _underlying_last = (_ul_ask + _ul_bid) / 2
                elif _ul_ask and _ul_ask > 0:
                    _underlying_last = _ul_ask
            except Exception:
                pass

            _confirm_result = check_entry_confirmation(
                plan             = approved_plan,
                direction        = str(getattr(approved_plan, "side", "CALL") or "CALL").upper(),
                trigger_price    = float(getattr(approved_plan, "trigger_price", 0) or 0) or None,
                live_bid         = _submit_quote_fields.get("submit_bid"),
                live_ask         = _submit_quote_fields.get("submit_ask"),
                live_quote_age_ms= _quote_age_ms if "_quote_age_ms" in dir() else None,
                underlying_last  = _underlying_last,
                decision_option_price = float(_plan_limit or 0) or None,  # use saved pre-overwrite decision price
                score     = float(_sig_for_confirm.get("score") or 0) or None,
                tier      = str(getattr(approved_plan, "tier", "") or ""),
                timeframe = str(_sig_for_confirm.get("timeframe") or "1d"),
                sandbox_mode = _sandbox,
            )
            _confirm_meta = _confirm_result.to_meta(
                started_at   = _confirm_result.metadata.get("live_entry_ts", ""),
                completed_at = __import__("datetime").datetime.now(
                    __import__("datetime").timezone.utc).isoformat(),
            )
            if not _confirm_result.passed:
                _fail_reason = _confirm_result.fail_reason or "entry_confirm_failed"
                log.info(
                    "[%s] ENTRY_CONFIRM_BLOCK — %s | client=%s | "
                    "spread=%.3f fade=%.2f reversal=%.3f age=%.1fs",
                    ticker, _fail_reason,
                    str(watched.signal.get("client_id", "?") if watched.signal else "?"),
                    float(_confirm_meta.get("spread_pct") or 0),
                    float(_confirm_meta.get("option_move_pct") or 0),
                    float(_confirm_meta.get("underlying_move_pct") or 0),
                    float(_confirm_meta.get("quote_age_seconds") or 0),
                )
                funnel.inc("entry_confirm_blocked")
                if signal_id:
                    # Only write to known ap_signals columns — no unknown fields
                    self.store.update_signal_fields(signal_id, {
                        "decision_status": "blocked_at_breach",
                        "context_notes":   _fail_reason,
                    })
                if queue_local_order_id and hasattr(self.order_state_machine, "update_order_meta"):
                    try:
                        self.order_state_machine.update_order_meta(
                            queue_local_order_id, {"entry_confirmation": _confirm_meta})
                    except Exception:
                        pass
                self._cleanup_pending_entry_order(
                    watched,
                    action="expire",
                    reason=_fail_reason,
                )
                # PR81 Final Amendment v2 §3: ENTRY_CONFIRMATION_FAILED ledger write.
                try:
                    from ap.opportunity_ledger import (
                        update_opportunity, STAGE_ENTRY_CONFIRMATION,
                    )
                    _client_id_for_ledger = str(
                        watched.signal.get("client_id") if watched.signal else ""
                    ) or getattr(self, "client_id", "")
                    _canon = str(
                        (watched.signal or {}).get("canonical_signal_id") or signal_id
                    )
                    update_opportunity(
                        signal_id or _canon, _client_id_for_ledger,
                        "ENTRY_CONFIRMATION_FAILED",
                        canonical_signal_id=_canon,
                        miss_stage=STAGE_ENTRY_CONFIRMATION,
                        miss_reason=_fail_reason,
                        order_local_id=str(queue_local_order_id) if queue_local_order_id else None,
                        entry_confirmation_result=_fail_reason,
                    )
                except Exception:
                    pass
                return   # NO BROKER SUBMIT
        except ImportError:
            # When confirmation_required=True, a missing module is NOT safe to skip.
            # Client-eligible trades must not bypass confirmation — fail closed.
            _gate_meta_imp = (getattr(approved_plan, "metadata", {}) or {})
            _hcqg_imp      = _gate_meta_imp.get("hybrid_client_quality_gate") or {}
            if _hcqg_imp.get("confirmation_required"):
                log.critical(
                    "[%s] ENTRY_CONFIRM_MODULE_MISSING — confirmation_required=True "
                    "but ap_entry_confirmation is not deployed. "
                    "Blocking client submit to preserve gate integrity.",
                    ticker,
                )
                self._alert_degraded(
                    "ENTRY_CONFIRM_MODULE_MISSING",
                    severity="CRITICAL",
                    client_id=str(
                        watched.signal.get("client_id", "?") if watched.signal else "?"
                    ),
                    ticker=ticker,
                    signal_id=signal_id,
                    details={"reason": "entry_confirm_module_missing",
                             "confirmation_required": True},
                )
                funnel.inc("entry_confirm_blocked")
                if signal_id:
                    self.store.update_signal_fields(signal_id, {
                        "decision_status": "blocked_at_breach",
                        "context_notes":   "entry_confirm_module_missing",
                    })
                if queue_local_order_id and hasattr(self.order_state_machine,
                                                    "update_order_meta"):
                    try:
                        self.order_state_machine.update_order_meta(
                            queue_local_order_id,
                            {"entry_confirmation": {
                                "confirmation_required": True,
                                "confirmation_passed":   False,
                                "confirmation_fail_reason": "entry_confirm_module_missing",
                            }},
                        )
                    except Exception:
                        pass
                self._cleanup_pending_entry_order(
                    watched,
                    action="expire",
                    reason="entry_confirm_module_missing",
                )
                # PR81 Final Amendment v2 §3: ENTRY_CONFIRMATION_FAILED ledger write.
                try:
                    from ap.opportunity_ledger import (
                        update_opportunity, STAGE_ENTRY_CONFIRMATION,
                    )
                    _client_id_for_ledger = str(
                        watched.signal.get("client_id") if watched.signal else ""
                    ) or getattr(self, "client_id", "")
                    _canon = str(
                        (watched.signal or {}).get("canonical_signal_id") or signal_id
                    )
                    update_opportunity(
                        signal_id or _canon, _client_id_for_ledger,
                        "ENTRY_CONFIRMATION_FAILED",
                        canonical_signal_id=_canon,
                        miss_stage=STAGE_ENTRY_CONFIRMATION,
                        miss_reason="entry_confirm_module_missing",
                        order_local_id=str(queue_local_order_id) if queue_local_order_id else None,
                        entry_confirmation_result="entry_confirm_module_missing",
                    )
                except Exception:
                    pass
                return   # NO BROKER SUBMIT
            # confirmation not required — module absence is safe to skip
            log.debug(
                "ap_entry_confirmation not found — preflight skipped "
                "(confirmation_required=False for this signal)"
            )
        except Exception as _ec_err:
            # Fail-closed for confirmation errors — block the submit
            log.error("[%s] ENTRY_CONFIRM_ERROR — failing closed: %s", ticker, _ec_err)
            _terminalize_breach_failure(
                f"entry_confirm_error:{_ec_err}",
                cleanup_action="expire",
                funnel_key="entry_confirm_blocked",
            )
            # PR81 Final Amendment v2 §3: ENTRY_CONFIRMATION_FAILED ledger write.
            try:
                from ap.opportunity_ledger import (
                    update_opportunity, STAGE_ENTRY_CONFIRMATION,
                )
                _client_id_for_ledger = str(
                    watched.signal.get("client_id") if watched.signal else ""
                ) or getattr(self, "client_id", "")
                _canon = str(
                    (watched.signal or {}).get("canonical_signal_id") or signal_id
                )
                update_opportunity(
                    signal_id or _canon, _client_id_for_ledger,
                    "ENTRY_CONFIRMATION_FAILED",
                    canonical_signal_id=_canon,
                    miss_stage=STAGE_ENTRY_CONFIRMATION,
                    miss_reason=f"entry_confirm_error:{_ec_err}",
                    order_local_id=str(queue_local_order_id) if queue_local_order_id else None,
                    entry_confirmation_result=f"entry_confirm_error:{_ec_err}",
                )
            except Exception:
                pass
            return

        submit_res = self.order_state_machine.submit_existing_entry(
            local_order_id=queue_local_order_id,
            broker=self.broker,
            plan=approved_plan,
            limit_price=submit_limit,
        )

        if submit_res.get("ok"):
            local_order_id = submit_res.get("local_order_id")
            broker_order_id = submit_res.get("broker_order_id")
            # P0: persist entry pricing audit into orders.meta (best-effort).
            if local_order_id and hasattr(self.order_state_machine, "update_order_meta"):
                try:
                    self.order_state_machine.update_order_meta(
                        local_order_id, _entry_pricing_audit
                    )
                except Exception as _audit_exc:
                    log.warning("[%s] entry_pricing_audit persist failed: %s", ticker, _audit_exc)
                # P0 follow-up: persist confirmation preflight metadata on success
                if _confirm_meta:
                    try:
                        self.order_state_machine.update_order_meta(
                            local_order_id, {"entry_confirmation": _confirm_meta})
                    except Exception:
                        pass
            # Item 3 — persist selector candidate audit into orders.meta
            # (EVIDENCE ONLY, best-effort, non-destructive JSONB merge).
            # _candidate_audit is set ONLY when breach-time deferred selection
            # ran above. For preselected (non-deferred) orders the queue stashed
            # the audit on approved_plan.metadata at selection time — fall back
            # to that source so both paths produce orders.meta.selector_candidate_audit.
            try:
                _persist_ca = _candidate_audit
                if not _persist_ca and approved_plan is not None:
                    _pmeta = getattr(approved_plan, "metadata", None) or {}
                    if isinstance(_pmeta, dict):
                        _persist_ca = _pmeta.get("selector_candidate_audit")
                if _persist_ca and local_order_id and hasattr(
                    self.order_state_machine, "update_order_meta"
                ):
                    self.order_state_machine.update_order_meta(
                        local_order_id,
                        {"selector_candidate_audit": _persist_ca},
                    )
            except Exception as _ca_exc:
                log.warning("[%s] candidate_audit persist failed: %s", ticker, _ca_exc)
            if signal_id:
                self.store.update_signal_fields(signal_id, {
                    "decision_status": "submitted",
                    "context_notes": f"entry_submitted local={local_order_id} broker={broker_order_id}",
                })
            # P1 ENTRY FIX (2026-05-21): tag the original ask submission as
            # entry_attempt=0 so the dashboard log-parser can bucket attempts.
            # entry_attempt=0 = original ask submit (this line)
            # entry_attempt=1 = ask+0.01 repeg (emitted by retry_engine.apply_repeg)
            # entry_attempt=2 = ask+0.02 repeg
            log.info(
                "[%s] Entry submitted via OSM | entry_attempt=0 local=%s broker=%s %sx %s @ $%.2f",
                ticker,
                local_order_id,
                broker_order_id,
                approved_qty,
                approved_contract,
                submit_limit,
            )
            _emit_deferred_outcome(
                "BREACH_BROKER_SUBMITTED",
                contract=str(approved_contract or ""),
                broker_order_id=str(broker_order_id or ""),
            )
            return

        log.error(
            "[%s] Entry submit failed via OSM | local=%s error=%s",
            ticker,
            submit_res.get("local_order_id") or queue_local_order_id,
            submit_res.get("error"),
        )
        _emit_deferred_outcome(
            "BREACH_SUBMISSION_SKIPPED",
            reason=f"osm_submit_existing_entry_failed:{submit_res.get('error')}",
            contract=str(approved_contract or ""),
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

        # PR-B / FIX-2: decision.suggested_limit is the FIRST authority.
        # The exit engine already prices the exit (urgency tier-aware,
        # bid/mid/(mid+bid)/2). When suggested_limit > 0 we honor it
        # verbatim and skip the duplicate execution-core ladder below.
        # The existing ladder logic is preserved as a fallback for
        # decisions that did not produce a suggested_limit (legacy
        # callers, paper sim, etc.).
        #
        # TODO(post-proof-week): delete the duplicate execution-core
        # pricing ladder entirely and make the exit engine the sole
        # pricing authority. Left in place for this PR to avoid
        # fill-side-effects right before proof week.
        _suggested_limit = float(getattr(decision, "suggested_limit", 0) or 0)
        _suggested_limit_locked = False  # True once a valid suggested_limit is locked in

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
            # Soft stops — thesis failed or went stale. Must use bid, not mid.
            # These were missing and would get mid pricing (wrong for a stop).
            "THESIS_FAIL_SOFT_STOP", "THESIS_STALE_SOFT_STOP", "SOFT_STOP",
        }

        _is_protective = (
            _reason_code in _PROTECTIVE_EXIT_CODES
            or any(marker in _reason_text for marker in _PROTECTIVE_REASON_TEXT_MARKERS)
        )

        # IMMEDIATE splits into two tiers:
        #   TRUE EMERGENCY → market (HARD_STOP -33%, EOD, SENTINEL/kill-switch)
        #   PROFIT EXIT → aggressive bid-limit, fast step-down (TARGET, TOUCHED
        #     PROFIT, PROFIT LOCK, IMMEDIATE_TP) — never blind-market a winner
        _reason_code_u = str(getattr(decision, "reason_code", "") or "").upper()
        _reason_text_u = str(getattr(decision, "reason", "") or "").upper()
        _TRUE_EMERGENCY_CODES = {
            "HARD_STOP", "EOD_FORCE_CLOSE", "SENTINEL_FORCED_EXIT",
        }
        _TRUE_EMERGENCY_MARKERS = {
            "HARD STOP", "EOD FORCE CLOSE", "EOD FORCED", "SENTINEL FORCED",
            "KILL SWITCH", "EMERGENCY",
        }
        _is_true_emergency = (
            _reason_code_u in _TRUE_EMERGENCY_CODES
            or any(m in _reason_text_u for m in _TRUE_EMERGENCY_MARKERS)
        )
        _use_market = (_urgency == "IMMEDIATE") and _is_true_emergency
        _is_fast_profit_exit = (_urgency == "IMMEDIATE") and not _is_true_emergency

        # Exit attempt tracking — needed by cheap-contract logic below
        _exit_submit_ts   = getattr(pos, "_exit_submit_ts", 0) or 0
        _exit_attempts    = getattr(pos, "_exit_attempts",  0) or 0
        _age_since_submit = time.time() - _exit_submit_ts if _exit_submit_ts else 0
        _ladder_price_set = False  # True once a step-down price is locked in

        # CHEAP CONTRACT HANDLING:
        # Sub-$0.25 options have wide relative spreads and limits can bounce.
        # OLD behavior blind-marketed them on the FIRST exit attempt — that
        # violates no-market-except-emergency and gave away the spread.
        #
        # NEW behavior: cheap contracts still use bid-limit FIRST. Only escalate
        # to market after multiple failed attempts (the limit cascade is real,
        # but one clean bid-limit attempt almost always fills). Default threshold
        # is 0 (disabled) — cheap-market only kicks in if explicitly enabled AND
        # the position has already failed 3+ exit attempts.
        _CHEAP_EXIT_MARKET_THRESHOLD = float(
            os.getenv("CHEAP_EXIT_MARKET_THRESHOLD", "0")  # default OFF
        )
        _cheap_exit_min_attempts = int(
            os.getenv("CHEAP_EXIT_MARKET_MIN_ATTEMPTS", "3")
        )
        _current_opt_price = float(getattr(pos, "current_option_price", 0) or _mid or _bid)
        if (
            not _use_market
            and _CHEAP_EXIT_MARKET_THRESHOLD > 0
            and _current_opt_price > 0
            and _current_opt_price < _CHEAP_EXIT_MARKET_THRESHOLD
            and _exit_attempts >= _cheap_exit_min_attempts
        ):
            _use_market = True
            log.warning(
                "[%s] CHEAP_CONTRACT_MARKET_EXIT — $%.2f < $%.2f AND %d failed "
                "limit attempts — escalating to market | %s",
                pos.ticker, _current_opt_price, _CHEAP_EXIT_MARKET_THRESHOLD,
                _exit_attempts, decision.reason,
            )
        elif (
            _CHEAP_EXIT_MARKET_THRESHOLD > 0
            and _current_opt_price > 0
            and _current_opt_price < _CHEAP_EXIT_MARKET_THRESHOLD
            and _exit_attempts < _cheap_exit_min_attempts
        ):
            log.info(
                "[%s] CHEAP_CONTRACT_BID_LIMIT — $%.2f cheap but attempt %d < %d "
                "— using bid-limit, not market yet | %s",
                pos.ticker, _current_opt_price, _exit_attempts,
                _cheap_exit_min_attempts, decision.reason,
            )

        # LIMIT-TO-MARKET ESCALATION for HIGH urgency exits:
        # EXIT PRICING — step-down bid ladder. Never market except true emergency.
        # Market orders on options fill at ASK. On an intraday dip this means
        # selling at the absolute worst price (BA case: $0.44 worse than bid).
        #
        # Step-down ladder priced from LIVE BID:
        #   First attempt:  current_bid
        #   15–30s unfilled: current_bid - $0.01
        #   30–60s unfilled: current_bid - $0.02
        #   60s+ unfilled:  market ONLY for HARD_STOP / EOD, else hold at bid-0.02
        # (_exit_submit_ts / _exit_attempts / _ladder_price_set defined above)

        # In-flight escalation for HIGH urgency exits
        if not _use_market and _urgency == "HIGH" and getattr(pos, "exit_in_flight", False) and _exit_submit_ts > 0:
            if _age_since_submit >= 60 and _is_protective:
                # 60s+ unfilled on a hard stop/EOD → market (must exit)
                _use_market = True
                log.warning("[%s] EXIT ESCALATED TO MARKET — bid-limit unfilled >60s | %s", pos.ticker, decision.reason)
            elif _age_since_submit >= 30 and _bid > 0:
                # 30–60s → bid - $0.02
                _exit_limit = max(round(_bid - 0.02, 2), 0.01)
                exit_price  = _exit_limit
                _ladder_price_set = True
                log.warning("[%s] EXIT STEP-DOWN bid-$0.02 = $%.2f — unfilled >30s | %s", pos.ticker, _exit_limit, decision.reason)
            elif _age_since_submit >= 15 and _bid > 0:
                # 15–30s → bid - $0.01
                _exit_limit = max(round(_bid - 0.01, 2), 0.01)
                exit_price  = _exit_limit
                _ladder_price_set = True
                log.warning("[%s] EXIT STEP-DOWN bid-$0.01 = $%.2f — unfilled >15s | %s", pos.ticker, _exit_limit, decision.reason)

        # FAST PROFIT-EXIT step-down — tighter than HIGH (profit exits want speed
        # but should never blind-market and give away the spread on a winner).
        # 0–10s: bid | 10–20s: bid-0.01 | 20–40s: bid-0.02 | 40s+: market (take it)
        if not _use_market and _is_fast_profit_exit and getattr(pos, "exit_in_flight", False) and _exit_submit_ts > 0:
            if _age_since_submit >= 40:
                # 40s+ — profit exit truly stuck, take market to lock the gain
                _use_market = True
                log.warning("[%s] PROFIT EXIT → MARKET — bid-limit unfilled >40s, locking gain | %s", pos.ticker, decision.reason)
            elif _age_since_submit >= 20 and _bid > 0:
                _exit_limit = max(round(_bid - 0.02, 2), 0.01)
                exit_price  = _exit_limit
                _ladder_price_set = True
                log.warning("[%s] PROFIT EXIT STEP-DOWN bid-$0.02 = $%.2f — unfilled >20s | %s", pos.ticker, _exit_limit, decision.reason)
            elif _age_since_submit >= 10 and _bid > 0:
                _exit_limit = max(round(_bid - 0.01, 2), 0.01)
                exit_price  = _exit_limit
                _ladder_price_set = True
                log.warning("[%s] PROFIT EXIT STEP-DOWN bid-$0.01 = $%.2f — unfilled >10s | %s", pos.ticker, _exit_limit, decision.reason)

        # PR-B / FIX-2: First-authority check. If the exit engine produced
        # a suggested_limit > 0 AND this is not a true emergency (which
        # must go to market regardless), lock the exit engine's price and
        # skip the ladder. This is the trail-exit spread fix: the engine
        # prices TRAIL at (mid+bid)/2; the execution-core ladder used to
        # override that to bid.
        if not _use_market and _suggested_limit > 0:
            _exit_limit = round(_suggested_limit, 2)
            exit_price  = _exit_limit
            _suggested_limit_locked = True
            # Stamp submit timestamp on FIRST submit so OSM/reconciler aging
            # paths see the same behavior they did under the legacy ladder.
            if not getattr(pos, "exit_in_flight", False) or not _exit_submit_ts:
                pos._exit_submit_ts = time.time()
            pos._exit_attempts = _exit_attempts + 1
            log.info(
                "[%s] EXIT @ suggested_limit=$%.2f (exit-engine authority) "
                "attempt=%d | urgency=%s | %s",
                pos.ticker, _exit_limit, pos._exit_attempts, _urgency, decision.reason,
            )

        if _use_market:
            _exit_limit = None
            exit_price = _bid if _bid > 0 else _mid
            if exit_price <= 0:
                log.critical("[%s] CLOSE BLOCKED — no quote for IMMEDIATE exit | %s", pos.ticker, decision.reason)
                return
            log.info("[%s] MARKET EXIT @ est.$%.2f (bid) | urgency=%s | %s", pos.ticker, exit_price, _urgency, decision.reason)
        elif _suggested_limit_locked:
            # Already priced via decision.suggested_limit — ladder skipped.
            pass
        elif _ladder_price_set and _exit_limit is not None and _exit_limit > 0:
            # Step-down already locked an aggressive price — DO NOT overwrite it
            exit_price = _exit_limit
            log.info("[%s] LADDER PRICE LOCKED @ $%.2f | %s", pos.ticker, _exit_limit, decision.reason)
        elif _bid > 0:
            # First attempt — all exits start at current bid
            _exit_limit = round(_bid, 2)
            exit_price  = _exit_limit
            # Only stamp timer on FIRST submit — never reset while in-flight, or
            # the step-down ladder never ages to 15s/30s/60s (reviewer bug #2)
            if not getattr(pos, "exit_in_flight", False) or not _exit_submit_ts:
                pos._exit_submit_ts = time.time()  # type: ignore[attr-defined]
            pos._exit_attempts  = _exit_attempts + 1  # type: ignore[attr-defined]
            log.info("[%s] LIMIT EXIT @ $%.2f (bid) attempt=%d | urgency=%s | %s",
                     pos.ticker, _exit_limit, pos._exit_attempts, _urgency, decision.reason)
        elif _mid > 0:
            # Bid unavailable — use mid as fallback
            _exit_limit = round(_mid, 2)
            exit_price  = _exit_limit
            if not getattr(pos, "exit_in_flight", False) or not _exit_submit_ts:
                pos._exit_submit_ts = time.time()  # type: ignore[attr-defined]
            pos._exit_attempts  = _exit_attempts + 1  # type: ignore[attr-defined]
            log.info("[%s] LIMIT EXIT @ $%.2f (mid fallback) attempt=%d | %s",
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
        # Breakeven band: trades within BREAKEVEN_BAND_PCT of entry count as
        # breakeven wins — not losses. Prevents $1-$2 slippage from showing
        # as a loss when the position was effectively flat. PR-B / FIX-8:
        # reads from the module-level constant, not an inline getenv.
        win  = opt_pnl >= BREAKEVEN_BAND_PCT
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

        # ── FIX 2: STAGE proof data on position — DO NOT write proof yet ─────
        # submit_exit() means the exit order was SUBMITTED, not FILLED.
        # Proof / P&L / feedback must only be written after broker fill
        # is confirmed through the fill_monitor → mark_position_closed path.
        # Stage all the data now (while we have decision/sig context) and
        # finalize it in _finalize_proof() called from mark_position_closed.
        #
        # exit_price here is the limit price we placed (estimated fill).
        # The actual broker fill price overwrites it in _finalize_proof().
        pos._proof_staged = {                                   # type: ignore[attr-defined]
            "ticker":             pos.ticker,
            "pattern":            sig.get("pattern", ""),
            "side":               pos.side,
            "timeframe":          sig.get("timeframe", "1d"),
            "score":              float(sig.get("score", 0) or 0),
            "tier":               tier,
            "context_score":      float((sig.get("score_breakdown") or {}).get("real_time_ctx", 0) or 0),
            "setup_status":       self.feedback.get_setup_status(
                                      pos.ticker, sig.get("pattern", ""),
                                      sig.get("timeframe", "1d"), pos.side),
            "entry_trigger":      pos.underlying_entry,
            "entry_option_price": pos.entry_price,
            "exit_option_price":  exit_price,   # estimated; overwritten at fill
            "underlying_entry":   pos.underlying_entry,
            "underlying_exit":    pos.current_underlying,
            "contracts":          pos.quantity,
            "exit_reason":        decision.reason,
            "opt_pnl":            opt_pnl,       # re-calculated at fill with actual fill price
            "win":                win,
            "spread_pct":         float(sig.get("spread_pct", 0) or 0),
            "chain_grade":        sig.get("chain_grade", ""),
            "opened_at":          pos.opened_at if hasattr(pos, "opened_at") else None,
            "synthetic_entry":    bool(getattr(pos, "synthetic_entry", False)),
            "position_id":        str(getattr(pos, "position_id", "") or ""),
            "local_order_id":     str(getattr(pos, "local_order_id", "") or ""),
            "signal":             sig,
            "paper":              self.paper,
        }
        pos.proof_logged = True  # type: ignore[attr-defined]
        log.info(
            "[EXIT_SUBMITTED_PROOF_STAGED] %s | est_exit=$%.2f pnl=%.1f%% | "
            "proof HELD — will finalize at broker-confirmed fill",
            pos.ticker, exit_price, opt_pnl,
        )
        # All P&L-bearing records (proof, feedback, signal-closed, shadow)
        # are deferred to _finalize_proof() called from mark_position_closed()
        # after broker fill confirmation. Nothing is written here.

        # Log trade to edge intelligence (logger instantiated once in __init__ to avoid resource leaks)
        try:
            _edge_logger = self._edge_logger
            if _edge_logger:
                # PR-B / FIX-7: edge-logger payload uses an explicit
                # allowlist of trade-relevant fields. The earlier
                # pos.__dict__ pattern leaked every ManagedPosition
                # internal (including ghost fields, _submit_generation,
                # pending_exit_*, exit_identity_quarantine, etc.) into
                # the edge intelligence schema, creating an unstable
                # contract that broke on every code release.
                _edge_logger.log_trade(
                    position={
                        "ticker":             pos.ticker,
                        "side":               pos.side,
                        "direction":          pos.side,
                        "entry_price":        getattr(pos, "entry_price", 0.0),
                        "quantity":           getattr(pos, "quantity", 0),
                        "quantity_remaining": getattr(pos, "quantity_remaining", 0),
                        "opened_at":          getattr(pos, "opened_at", None),
                        "position_id":        getattr(pos, "position_id", ""),
                        "client_id":          getattr(pos, "client_id", ""),
                        "signal_id":          getattr(pos, "signal_id", ""),
                        "signal":             sig,
                        "timeframe":          sig.get("timeframe", "1d"),
                        "synthetic_entry":    bool(getattr(pos, "synthetic_entry", False)),
                        # CODEX-1 (PR B follow-up): APTradeLogger.log_trade in
                        # ap_edge_intelligence.py derives contract_symbol from
                        # `option_symbol` or `contract` (line ~110), and uses
                        # underlying_entry / underlying_stop / underlying_target
                        # for r-multiple and risk metrics. These are all
                        # trade-relevant identity/level fields (not internals).
                        # Omitting them was a real regression that broke trade
                        # analytics and the proof_trades ↔ trades_intel join.
                        "option_symbol":      getattr(pos, "option_symbol", ""),
                        "contract":           getattr(pos, "option_symbol", ""),  # alias
                        "underlying_entry":   getattr(pos, "underlying_entry", 0.0),
                        "underlying_stop":    getattr(pos, "underlying_stop", 0.0),
                        "underlying_target":  getattr(pos, "underlying_target", 0.0),
                        # Score / tier come from the signal dict (not pos);
                        # the logger checks both top-level and signal nested.
                        "score":              sig.get("score", 0),
                        "tier":               sig.get("tier", ""),
                        # contracts alias — logger accepts contracts | quantity | qty
                        "contracts":          getattr(pos, "quantity", 0),
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

        # PR-B / FIX-6: _record_intel_outcome moved to _finalize_proof()
        # so the intelligence dataset receives the ACTUAL broker fill P/L,
        # not the estimated submit-time P/L. The signal_id is resolved
        # at finalize time from the staged dict.

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
        plan = watched.signal.get("plan") or {}
        contract = (
            plan.get("contract_symbol")
            or watched.signal.get("contract_symbol")
            or watched.signal.get("contract")
            or ""
        )

        # ── P1 FIX (2026-05-21): DEFERRED contract guard ───────────────────
        # If the contract starts with 'DEFERRED:', the contract was not yet
        # selected at watcher-arm time. Watcher invalidation on a DEFERRED
        # contract is NOT a true thesis invalidation — it means contract
        # selection at breach time hasn't been attempted yet. Keep the
        # watcher in PENDING_TRIGGER and let breach-time contract selection
        # do its job. Only log the event; do NOT permanently cancel.
        #
        # SAFETY (post-review): by the time _on_signal_invalidate is called,
        # the watcher has ALREADY set self.state = WatchState.INVALIDATED.
        # Returning early without restoring the state would zombie the
        # watcher in INVALIDATED — it would never check() the price again
        # and breach-time contract selection would never run. Restore the
        # state to PENDING explicitly so the next poll re-enters check().
        is_deferred_contract = isinstance(contract, str) and contract.startswith("DEFERRED:")
        if is_deferred_contract:
            try:
                from ap_entry_watcher import WatchState
                _prior_state = getattr(watched, "state", None)
                watched.state = WatchState.PENDING
                # Reset breach counter so a stop touch doesn't immediately
                # re-invalidate on the very next poll.
                if hasattr(watched, "breach_count"):
                    watched.breach_count = 0
                log.info(
                    "[%s] DEFERRED_CONTRACT_INVALIDATED ignored | signal_id=%s contract=%s "
                    "— state restored %s -> PENDING; awaiting breach-time contract selection",
                    watched.ticker, signal_id or "?", contract, _prior_state,
                )
            except Exception as _e:
                # If we can't restore state, the safest thing is to still NOT cancel
                # the order — log the failure so it can be investigated.
                log.error(
                    "[%s] DEFERRED_CONTRACT_INVALIDATED state-restore failed: %s — "
                    "order NOT canceled, but watcher may be stuck in INVALIDATED",
                    watched.ticker, _e,
                )
            funnel.inc("deferred_contract_invalidated")
            return  # Do NOT cancel the order or write 'invalidated' to signal store.

        # ── Full forensic context for legitimate invalidations ─────────────────
        # P1 FIX (2026-05-21): every watcher_invalidated must log:
        # signal_id, plan_id, local_order_id, client_id, symbol, contract,
        # CALL/PUT, trigger, current underlying, option bid/mid/ask, stop, target,
        # setup age, exact invalidation formula, and reason_code.
        try:
            plan_id = (plan.get("plan_id") if isinstance(plan, dict) else None) or "?"
            local_order_id = (
                (plan.get("local_order_id") if isinstance(plan, dict) else None)
                or watched.signal.get("local_order_id") or "?"
            )
            side = (
                (plan.get("direction") if isinstance(plan, dict) else None)
                or watched.signal.get("direction")
                or watched.signal.get("side") or "?"
            )
            trigger = getattr(watched, "trigger_price", None) or watched.signal.get("entry_trigger")
            stop    = (plan.get("stop_level") if isinstance(plan, dict) else None) or watched.signal.get("stop")
            target  = (plan.get("target")     if isinstance(plan, dict) else None) or watched.signal.get("target")
            age_secs = None
            try:
                if getattr(watched, "armed_at", None):
                    import time as _t
                    age_secs = _t.time() - watched.armed_at
            except Exception:
                pass
            # Best-effort current-market snapshot.
            current_underlying = None
            opt_bid = opt_ask = opt_mid = None
            try:
                if hasattr(self, "broker") and hasattr(self.broker, "get_quote"):
                    if watched.ticker:
                        uq = self.broker.get_quote(watched.ticker)
                        if isinstance(uq, dict):
                            current_underlying = uq.get("last") or uq.get("close") or uq.get("price")
                    if contract and not is_deferred_contract:
                        oq = self.broker.get_quote(contract)
                        if isinstance(oq, dict):
                            opt_bid = oq.get("bid")
                            opt_ask = oq.get("ask")
                            if opt_bid and opt_ask:
                                try:
                                    opt_mid = (float(opt_bid) + float(opt_ask)) / 2
                                except Exception:
                                    opt_mid = None
            except Exception:
                pass

            log.warning(
                "[%s] WATCHER_INVALIDATED | signal_id=%s plan_id=%s local_order_id=%s "
                "client_id=%s symbol=%s contract=%s side=%s trigger=%s underlying=%s "
                "opt_bid=%s opt_ask=%s opt_mid=%s stop=%s target=%s age=%s",
                watched.ticker, signal_id or "?", plan_id, local_order_id,
                getattr(self, "client_id", "?"),
                watched.ticker, contract or "?", side,
                trigger if trigger is not None else "?",
                current_underlying if current_underlying is not None else "?",
                opt_bid if opt_bid is not None else "?",
                opt_ask if opt_ask is not None else "?",
                opt_mid if opt_mid is not None else "?",
                stop if stop is not None else "?",
                target if target is not None else "?",
                f"{age_secs:.0f}s" if age_secs is not None else "?",
            )
        except Exception as e:
            log.debug("watcher_invalidated forensic log failed: %s", e)

        if signal_id:
            self.store.update_status(signal_id, "invalidated", timestamp_flag="invalidated_at")
        self._cleanup_pending_entry_order(watched, action="cancel", reason="watcher_invalidated")
        funnel.inc("watcher_invalidated")

    def _finalize_proof(self, pos: "ManagedPosition", actual_fill_price: float = 0.0) -> None:
        """Write proof/P&L/feedback using the ACTUAL broker fill price.

        Called from mark_position_closed via the exit engine's
        on_exit_fill_confirmed callback — the only point where we have
        broker-confirmed fill data. Reads the staged proof dict written
        at submit time in _on_position_close and finalises with real numbers.

        If actual_fill_price is 0 or None (can happen in paper sandbox),
        falls back to the estimated exit_price staged at submission.
        """
        staged = getattr(pos, "_proof_staged", None)
        if not staged:
            return
        if getattr(pos, "_proof_finalized", False):
            log.info(
                "[EXIT_PROOF_FINALIZE_SKIPPED_ALREADY_LOGGED] %s | "
                "position already finalized — skipping duplicate",
                getattr(pos, "ticker", "?"),
            )
            return
        pos._proof_finalized = True  # type: ignore[attr-defined]

        # Use actual fill price; fall back to estimated if broker returns 0/None
        fill = float(actual_fill_price or 0)
        est  = float(staged.get("exit_option_price") or 0)
        final_exit_price = fill if fill > 0 else est

        entry_px = float(staged.get("entry_option_price") or 0)
        if entry_px > 0 and final_exit_price > 0:
            opt_pnl_pct = (final_exit_price - entry_px) / entry_px
        else:
            opt_pnl_pct = staged.get("opt_pnl", 0.0) / 100.0

        # PR-B / FIX-8: single-source-of-truth via module-level constant.
        # BREAKEVEN_BAND_PCT is stored as percent (e.g. -2.0 = -2%);
        # opt_pnl_pct here is decimal, so divide by 100.
        win = opt_pnl_pct >= (BREAKEVEN_BAND_PCT / 100.0)

        slippage_vs_est = round(final_exit_price - est, 4) if est > 0 else None
        if fill > 0 and est > 0:
            log.info(
                "[EXIT_FILL_CONFIRMED_PROOF_FINALIZED] %s | "
                "est_exit=$%.2f actual_fill=$%.2f slip=%.4f pnl=%.1f%%",
                staged["ticker"], est, fill,
                slippage_vs_est if slippage_vs_est is not None else 0.0,
                opt_pnl_pct * 100,
            )
        else:
            log.info(
                "[EXIT_FILL_CONFIRMED_PROOF_FINALIZED] %s | "
                "no broker fill price — using staged estimate $%.2f pnl=%.1f%%",
                staged["ticker"], final_exit_price, opt_pnl_pct * 100,
            )

        underlying_entry = staged.get("underlying_entry") or 0
        underlying_exit  = staged.get("underlying_exit")  or pos.current_underlying or 0
        u_pnl_pct = (
            (underlying_exit - underlying_entry) / underlying_entry * 100
            if underlying_entry else 0
        )

        # proof_trades.option_pnl_pct is stored as PERCENTAGE (e.g. -25.0 = -25%)
        # not decimal (e.g. -0.25). Convert before passing to log_trade.
        opt_pnl_pct_for_proof = round(opt_pnl_pct * 100, 2)

        # CODEX-2 (PR B follow-up): proof.log_trade failure must NOT short-
        # circuit the rest of _finalize_proof. Feedback, shadow, and the
        # intel outcome callback are all independent of proof DB success —
        # they're computational over the staged dict + actual fill price.
        # Dropping them when proof errors creates silent data loss exactly
        # in degraded DB conditions (when intel matters most for diagnosis).
        # Track proof outcome with a flag; do NOT early-return.
        proof_ok = True
        try:
            self.proof.log_trade(
                ticker             = staged["ticker"],
                pattern            = staged.get("pattern", ""),
                side               = staged.get("side", ""),
                timeframe          = staged.get("timeframe", "1d"),
                score              = staged.get("score", 0),
                tier               = staged.get("tier", ""),
                context_score      = staged.get("context_score", 0),
                setup_status       = staged.get("setup_status", ""),
                entry_trigger      = underlying_entry,
                entry_option_price = entry_px,
                exit_option_price  = final_exit_price,
                underlying_entry   = underlying_entry,
                underlying_exit    = underlying_exit,
                contracts          = staged.get("contracts", 1),
                exit_reason        = staged.get("exit_reason", ""),
                option_pnl_pct     = opt_pnl_pct_for_proof,
                underlying_pnl_pct = u_pnl_pct,
                win                = win,
                spread_pct         = staged.get("spread_pct", 0),
                chain_grade        = staged.get("chain_grade", ""),
                opened_at          = staged.get("opened_at"),
                synthetic_entry    = staged.get("synthetic_entry", False),
                position_id        = staged.get("position_id", ""),
                local_order_id     = staged.get("local_order_id", ""),
                # Slippage vs staged estimate
                exit_fill_price    = fill if fill > 0 else None,
                exit_limit_placed  = est if est > 0 else None,
                slippage_vs_bid    = slippage_vs_est,
            )
        except Exception as proof_err:
            proof_ok = False
            log.error(
                "[%s] _finalize_proof: proof.log_trade failed: %s — "
                "continuing to feedback/shadow/intel (proof-independent)",
                staged["ticker"], proof_err,
            )

        # Feedback + shadow safe to finalize here with real P&L
        try:
            sig = staged.get("signal", {})
            paper = staged.get("paper", True)
            self.feedback.record_outcome(
                signal             = sig,
                entry_option_price = entry_px,
                exit_option_price  = final_exit_price,
                exit_reason        = staged.get("exit_reason", ""),
                underlying_entry   = underlying_entry,
                underlying_exit    = underlying_exit,
                contracts          = staged.get("contracts", 1),
                context_notes      = f"mode={'paper' if paper else 'live'} fill_confirmed=True",
                synthetic_entry    = staged.get("synthetic_entry", False),
            )
            # Mark signal closed only after broker-confirmed fill
            signal_id = str(sig.get("signal_id", "") or "")
            if signal_id:
                self.store.update_status(signal_id, "closed", timestamp_flag="closed_at")
        except Exception as fb_err:
            log.error("[%s] _finalize_proof: feedback/store failed: %s", staged["ticker"], fb_err)

        try:
            self.shadow.record_live_outcome(staged.get("tier", ""), opt_pnl_pct)
        except Exception as _e:
            log.warning("shadow_record_live_outcome_failed: %s", _e)

        # PR-B / FIX-6: record_intel_outcome with the ACTUAL broker fill
        # P/L (opt_pnl_pct is decimal here, e.g. 0.18 = 18%). Previously
        # called from _on_position_close with the estimated submit-time
        # P/L — the intelligence dataset received pre-fill estimates.
        if _record_intel_outcome:
            try:
                _intel_sig    = staged.get("signal", {}) or {}
                _intel_sig_id = str(_intel_sig.get("signal_id", "") or "")
                _record_intel_outcome(
                    ticker    = staged.get("ticker", ""),
                    signal_id = _intel_sig_id,
                    pnl_pct   = opt_pnl_pct,  # already decimal; actual fill-based
                )
            except Exception as _alpha_err:
                log.warning("Alpha tracker update failed: %s", _alpha_err)

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
