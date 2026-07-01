# ap/order_monitor.py — APOrderMonitor
# =============================================================================
# Stale order detection and automated response.
# Runs as a background thread per client.
#
# Timeout policy:
#   CREATED      > 2 min  → cancel (never submitted to broker)
#   SUBMITTED    > 5 min  → query broker status, then cancel if unresponsive
#   ACKNOWLEDGED > 10 min → query broker, log alert, cancel if stalled
#   PARTIAL_FILL > 15 min → log alert, leave open (partial fill is real risk)
#   EXIT_REQUESTED/
#   EXIT_SUBMITTED > 5 min → ESCALATE immediately (exit failures are account risk)
#   EXIT_ACKNOWLEDGED > 10 min → query broker, escalate
#
# Actions:
#   cancel  — transition order to CANCELED via order_state_machine
#             + mark position back to OPEN if exit was canceled
#   alert   — log.error + optionally send email/webhook
#   query   — ask broker for current order status, advance state machine
#
# Design:
#   - Runs every POLL_INTERVAL seconds (default 60s)
#   - Uses order_state_machine for all transitions
#   - Uses position_manager to fix position state after exit cancellation
#   - Never raises — all exceptions logged, loop continues
# =============================================================================

from __future__ import annotations

import logging
import os
import re
import threading
import time
from datetime import datetime, timezone
from typing import Optional

from ap.db import conn, run_with_retry
from ap.observability import emit_decision_event, get_git_commit, make_config_hash
from ap.utils import now_utc_iso

log = logging.getLogger("ap.order_monitor")

_OCC_SIDE_RE = re.compile(r"\d{6}([CP])")

# ── Shared broker order-status cache ─────────────────────────────────────────
# fill_monitor and order_monitor both call broker.get_order(broker_order_id)
# independently. With 10 clients and active orders both components fire
# concurrently — same broker_order_id queried twice within seconds.
# This module-level cache (shared across all client threads in one process)
# deduplicates those calls. TTL=8s: short enough to not delay stale detection,
# long enough to absorb fill_monitor + order_monitor firing in the same window.
_BROKER_STATUS_CACHE: dict = {}       # broker_order_id -> (status_str, expires_ts)
_BROKER_STATUS_CACHE_LOCK = threading.Lock()
_BROKER_STATUS_CACHE_TTL  = float(os.getenv("BROKER_STATUS_CACHE_TTL", "8.0"))


def _cached_broker_order_status(broker_order_id: str, fetch_fn) -> Optional[str]:
    """Return cached broker status or call fetch_fn() and cache the result.

    fetch_fn must be a zero-arg callable that returns the raw status string.
    Returns None if fetch_fn returns None (query failed or order not found).
    """
    now = time.monotonic()
    with _BROKER_STATUS_CACHE_LOCK:
        entry = _BROKER_STATUS_CACHE.get(broker_order_id)
        if entry and entry[1] > now:
            return entry[0]
    # Cache miss — call broker
    status = fetch_fn()
    if status is not None:
        with _BROKER_STATUS_CACHE_LOCK:
            _BROKER_STATUS_CACHE[broker_order_id] = (status, now + _BROKER_STATUS_CACHE_TTL)
        # Prune stale entries (keep cache small)
        if len(_BROKER_STATUS_CACHE) > 500:
            with _BROKER_STATUS_CACHE_LOCK:
                dead = [k for k, v in _BROKER_STATUS_CACHE.items() if v[1] <= now]
                for k in dead:
                    _BROKER_STATUS_CACHE.pop(k, None)
    return status

# Timeout thresholds (seconds)
# P1 ENTRY FIX (2026-05-21): tightened from 120s -> 30s. Production data
# showed orders sitting in CREATED for 120s+ without ever submitting (the
# LOST_HANDOFF failure mode). 30s is enough for a legitimate OSM handoff;
# anything beyond that is a watcher fall-off.
# PR #141: bumped 30→90 to absorb startup recovery bursts and overnight-rescue
# batches. The watchdog now performs a single ownership re-arm BEFORE cancel,
# so 90s is the time until LOST_HANDOFF_90S is declared after re-arm failure.
TIMEOUT_CREATED       = int(os.getenv("ORDER_TIMEOUT_CREATED",       "90"))    # 90s — CREATED = never reached broker
TIMEOUT_CREATED_NO_BROKER_WARN = int(os.getenv("ORDER_TIMEOUT_CREATED_NO_BROKER_WARN", "10"))  # fast diagnostic — alert at 10s, cancel at 30s
TIMEOUT_SUBMITTED     = int(os.getenv("ORDER_TIMEOUT_SUBMITTED",     "300"))   # 5 min
# Entry ACKNOWLEDGED timeout: an options scalp limit that sits acknowledged-
# unfilled for 10 minutes is a stale entry — the move already happened. If it
# fills now we enter late into a weaker setup (the SMCI bug). 180s is the
# correct ceiling for scalp entries. Exits are handled separately and faster.
TIMEOUT_ACKNOWLEDGED  = int(os.getenv("ORDER_TIMEOUT_ACKNOWLEDGED",  "180"))   # 3 min — scalp entry ceiling
TIMEOUT_PARTIAL_FILL  = int(os.getenv("ORDER_TIMEOUT_PARTIAL_FILL",  "900"))   # 15 min
# DEPRECATED CONSTANT: ENTRY_LIMIT_MAX_AGE_SECONDS
# This was the pre-Phase-2 hard cancel ceiling at 25s. Phase 2 (2026-05-23)
# replaced it with the two-step adaptive autocancel:
#   25s  = RE-EVALUATION trigger (see ENTRY_REEVAL_AGE_SECONDS below)
#   90s  = normal max active entry window (ENTRY_MAX_AGE_NORMAL)
#   120s = A/A+ scored max active entry window (ENTRY_MAX_AGE_APLUS)
# The constant is RETAINED only so legacy deployments setting the env var
# do not break startup; it is no longer read by any decision path. Setting
# it has NO effect on cancel behavior.
ENTRY_LIMIT_MAX_AGE_SECONDS = int(os.getenv("ENTRY_LIMIT_MAX_AGE_SECONDS", "25"))  # deprecated; see ENTRY_REEVAL_AGE_SECONDS

# PHASE 2 ADAPTIVE AUTOCANCEL (2026-05-23):
# 25s is the RE-EVALUATION trigger, not a hard kill. At 25s we run the
# alignment gates (decide_repeg). If still aligned, the order may continue
# up to ENTRY_MAX_AGE_NORMAL (90s) for a typical setup, or
# ENTRY_MAX_AGE_APLUS (120s) for A/A+ scored setups.
#
# Immediate cancel happens ONLY for:
#   thesis_invalid, spread_wide, runaway_quote, positions_full,
#   lost_handoff_systemic, risk_gate_blocked.
#
# Every cancel writes an exact reason_code so the dashboard can bucket.
ENTRY_REEVAL_AGE_SECONDS = int(os.getenv("ENTRY_REEVAL_AGE_SECONDS", "25"))
ENTRY_MAX_AGE_NORMAL     = int(os.getenv("ENTRY_MAX_AGE_NORMAL",     "90"))
ENTRY_MAX_AGE_APLUS      = int(os.getenv("ENTRY_MAX_AGE_APLUS",      "120"))
ENTRY_APLUS_SCORE_THRESHOLD = float(os.getenv("ENTRY_APLUS_SCORE_THRESHOLD", "85"))

# PR66: Paper/live split for entry max-age.
# Paper (sandbox) Tradier fills and quotes can lag vs live — valid test
# orders were canceling before they could fill. LIVE keeps the strict 90s
# ceiling; PAPER gets 2x breathing room.
# PR #68 — PAPER default bumped from 180s to 300s so the new reprice ladder
# (10s/25s/40s) + market fallback (45s) have room to run before the age
# ceiling fires. LIVE behavior intentionally unchanged at 90s.
PAPER_ENTRY_MAX_AGE_NORMAL = int(os.getenv("PAPER_ENTRY_MAX_AGE_SECONDS", "300"))
LIVE_ENTRY_MAX_AGE_NORMAL  = int(os.getenv("LIVE_ENTRY_MAX_AGE_SECONDS",   "90"))

# PR #68 — PAPER-mode entry retry + market fallback.
# In sandbox mode, marketable limit orders frequently sit unfilled because
# Tradier sandbox's matching engine uses a lagged quote feed. We add a
# reprice ladder + bounded market fallback so paper-proof sessions actually
# get fills when the underlying moves in the bot's favor. LIVE mode is
# strictly unaffected by all of the following.
def _parse_int_list(raw: str, default: list[int]) -> list[int]:
    try:
        out = [int(x.strip()) for x in (raw or "").split(",") if x.strip()]
        return sorted(set(out)) if out else default
    except Exception:
        return default

PAPER_ENTRY_REPEG_LADDER_SECONDS   = _parse_int_list(
    os.getenv("PAPER_ENTRY_REPEG_SECONDS", "10,25,40"),
    default=[10, 25, 40],
)
ENTRY_PAPER_ASK_CROSS_CENTS        = float(os.getenv("ENTRY_PAPER_ASK_CROSS_CENTS", "0.05"))
ENTRY_LIVE_ASK_CROSS_CENTS         = float(os.getenv("ENTRY_LIVE_ASK_CROSS_CENTS",  "0.01"))
PAPER_ENTRY_MARKET_FALLBACK_ENABLED       = os.getenv("PAPER_ENTRY_MARKET_FALLBACK_ENABLED", "true").strip().lower() in ("1", "true", "yes")
PAPER_ENTRY_MARKET_FALLBACK_AFTER_SECONDS = int(os.getenv("PAPER_ENTRY_MARKET_FALLBACK_AFTER_SECONDS", "45"))
# Spread guard: PAPER market fallback only fires when the spread is not
# pathologically wide. Default mirrors selector's max-spread check.
PAPER_ENTRY_MARKET_FALLBACK_MAX_SPREAD_PCT = float(os.getenv("PAPER_ENTRY_MARKET_FALLBACK_MAX_SPREAD_PCT", "0.25"))
# H4: exit reliability. An unfilled exit on a fast-moving option is direct
# account risk — a +25% green trade can round-trip to breakeven or a loss
# while a mispriced limit exit sits unfilled. 5 min was far too slow. At 45s
# the existing safety path (broker-fill-check -> cancel -> clear_exit_in_flight
# -> exit engine re-evaluates & re-prices within its 8s loop) becomes
# genuinely protective. Mechanism is UNCHANGED; only the trigger is faster.
# Broker status is always checked first, so an exit that is mid-fill is never
# wrongly canceled. Env-tunable for per-deployment calibration.
TIMEOUT_EXIT_PENDING  = int(os.getenv("ORDER_TIMEOUT_EXIT_PENDING",  "45"))    # 45s — exit = account risk
TIMEOUT_EXIT_ACK      = int(os.getenv("ORDER_TIMEOUT_EXIT_ACK",      "90"))    # 90s — acked but no fill

POLL_INTERVAL = int(os.getenv("ORDER_MONITOR_POLL", "60"))  # seconds (entry checks)
# H4: exits run on this faster cadence (account risk). Keep >= a few seconds
# to avoid hammering the broker; 15s + 45s timeout => hung exit caught fast.
EXIT_CHECK_INTERVAL = int(os.getenv("ORDER_MONITOR_EXIT_POLL", "15"))  # seconds
PENDING_TRIGGER_CLEANUP_ENABLED = os.getenv(
    "PENDING_TRIGGER_CLEANUP_ENABLED", "1"
).strip().lower() in ("1", "true", "yes")
PENDING_TRIGGER_MAX_AGE_SECONDS = int(os.getenv("PENDING_TRIGGER_MAX_AGE_SECONDS", "5400"))
PENDING_TRIGGER_CLEANUP_DRY_RUN = os.getenv(
    "PENDING_TRIGGER_CLEANUP_DRY_RUN", "0"
).strip().lower() in ("1", "true", "yes")

# EOD entry cutoff for PENDING_TRIGGER watcher-held rows (ET).
# Mirrors ap_entry_watcher.EOD_CUTOFF_HOUR / EOD_CUTOFF_MIN so the orphan
# guard uses the same boundary the watcher uses when disarming at EOD.
# After 15:30 ET a watcher-held row that still hasn't breached is legitimately
# expired — it missed its trading window. Before that, it must be kept alive.
_PT_ORPHAN_EOD_CUTOFF_HOUR = int(os.getenv("PT_ORPHAN_EOD_CUTOFF_HOUR", "15"))
_PT_ORPHAN_EOD_CUTOFF_MIN  = int(os.getenv("PT_ORPHAN_EOD_CUTOFF_MIN",  "30"))

# Watchdog ownership controls.
# Default is intentionally passive for POSITION lifecycle (that authority
# belongs to fill monitor / reconciler / OSM / exit engine). BUT a stale
# unfilled ENTRY is not position lifecycle — letting it fill late creates a
# bad trade. Entry cancels are therefore always permitted (see _handle_stale_entry).
ORDER_MONITOR_MODE = os.getenv("ORDER_MONITOR_MODE", "watchdog").strip().lower()
# Missed-move cancel ENABLED by default. The whole point is to not chase a
# setup after the move already happened. Disabling it (the old default) is
# exactly why the SMCI stale entry came back.
ENABLE_MISSED_MOVE_CANCEL = os.getenv("ENABLE_MISSED_MOVE_CANCEL", "1").strip() == "1"
ALLOW_ORDER_MONITOR_POSITION_REOPEN = os.getenv("ALLOW_ORDER_MONITOR_POSITION_REOPEN", "0").strip() == "1"
ORDER_MONITOR_CAN_ACT = ORDER_MONITOR_MODE in {"active", "actor", "enforce", "enforced"}
# Stale ENTRY cancels are always allowed even in watchdog mode. An unfilled
# buy-to-open is not a position — leaving it to fill late is the actual risk.
ALLOW_ENTRY_CANCEL_IN_WATCHDOG = os.getenv("ALLOW_ENTRY_CANCEL_IN_WATCHDOG", "1").strip() == "1"

# PHASE 5 WIRE-IN (2026-05-23):
# After an ENTRY cancel is broker-confirmed, the monitor consults
# ap.post_cancel_retry.evaluate_retry. If the decision returns ARM, the
# retry intent is persisted into the canceled order's meta JSONB and the
# monitor's _check_armed_retries tick re-submits via execution.process_signal
# after the jittered wait elapses.
#
# The wire-in is gated by ENTRY_RETRY_ENABLED (default on) so it can be
# disabled in production via env without a code change. Wire-in itself is
# additive — disabling it returns the monitor to pre-PR-22 behavior.
ENTRY_RETRY_ENABLED = os.getenv("ENTRY_RETRY_ENABLED", "1").strip().lower() in ("1", "true", "yes")


class APOrderMonitor:
    """
    Background stale-order monitor per client.

    Usage:
        monitor = APOrderMonitor(
            client_id=member["email"],  # always pass the authenticated client_id
            broker=broker,
            order_state_machine=osm,
            position_manager=pm,
            client_mode=runner.mode,    # PR66: always pass "PAPER" or "LIVE" explicitly
        )
        monitor.start()   # starts daemon thread
        monitor.stop()    # signals thread to exit
    """

    def __init__(
        self,
        client_id:         str,
        broker,
        order_state_machine,
        position_manager,
        exit_engine=None,
        entry_watcher=None,
        alert_fn=None,
        # PR66: "PAPER" or "LIVE". Default is "LIVE" so any call site that
        # forgets to pass client_mode uses the strict 90s ceiling rather than
        # the relaxed 180s paper ceiling. Missing wiring fails safe, not relaxed.
        client_mode: str = "LIVE",
        data_broker=None,
    ):
        self.client_id   = client_id
        self.broker      = broker
        self.osm         = order_state_machine
        self.pm          = position_manager
        self.exit_engine = exit_engine
        self.entry_watcher = entry_watcher
        self.alert_fn    = alert_fn
        self.data_broker = data_broker or getattr(broker, "data_broker", None)
        # PR66: store mode for per-mode max-age selection.
        # "or LIVE" guards against explicit None/empty being passed — fail safe.
        self.client_mode = str(client_mode or "LIVE").strip().upper()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

        self.run_id           = os.getenv("AP_RUN_ID", "unknown")
        self.strategy_version = os.getenv("AP_STRATEGY_VERSION", "ap_live_beta")
        self.git_commit       = get_git_commit()
        self.config_hash      = make_config_hash({
            "timeout_created":        TIMEOUT_CREATED,
            "timeout_submitted":      TIMEOUT_SUBMITTED,
            "timeout_acknowledged":   TIMEOUT_ACKNOWLEDGED,
            "timeout_partial_fill":   TIMEOUT_PARTIAL_FILL,
            "timeout_exit_pending":   TIMEOUT_EXIT_PENDING,
            "timeout_exit_ack":       TIMEOUT_EXIT_ACK,
            "poll_interval":          POLL_INTERVAL,
            "order_monitor_mode":     ORDER_MONITOR_MODE,
            "enable_missed_move_cancel": ENABLE_MISSED_MOVE_CANCEL,
            "allow_position_reopen":  ALLOW_ORDER_MONITOR_POSITION_REOPEN,
            "missed_move_min_secs":   int(os.getenv("MISSED_MOVE_MIN_SECS",    "6")),
            "missed_move_price_mult": float(os.getenv("MISSED_MOVE_PRICE_MULT", "1.07")),
            "entry_limit_max_age_seconds": ENTRY_LIMIT_MAX_AGE_SECONDS,
            "allow_entry_cancel_in_watchdog": ALLOW_ENTRY_CANCEL_IN_WATCHDOG,
            "pending_trigger_cleanup_enabled": PENDING_TRIGGER_CLEANUP_ENABLED,
            "pending_trigger_max_age_seconds": PENDING_TRIGGER_MAX_AGE_SECONDS,
            "pending_trigger_cleanup_dry_run": PENDING_TRIGGER_CLEANUP_DRY_RUN,
        })

    def _quote_broker(self):
        return self.data_broker or getattr(self.broker, "data_broker", None) or self.broker

    def _normalize_order_direction(self, value) -> str:
        raw = str(value or "").strip().upper()
        if raw in {"CALL", "PUT"}:
            return raw
        if raw in {"BUY", "LONG", "CALLS", "BULLISH"}:
            return "CALL"
        if raw in {"SELL", "SHORT", "PUTS", "BEARISH"}:
            return "PUT"
        return ""

    def _direction_from_occ_contract(self, contract) -> str:
        symbol = str(contract or "").strip().upper()
        match = _OCC_SIDE_RE.search(symbol)
        if not match:
            return ""
        return "CALL" if match.group(1) == "C" else "PUT"

    def _emit_order_event(
        self,
        *,
        local_order_id: str,
        stage: str,
        decision: str,
        reason_code: str | None = None,
        explanation: str = "",
        contract: str | None = None,
        position_id: str | None = None,
        inputs: dict | None = None,
        thresholds: dict | None = None,
        context: dict | None = None,
    ):
        try:
            emit_decision_event(
                run_id=self.run_id,
                candidate_id=local_order_id,
                trade_id=local_order_id,
                position_id=position_id,
                client_id=self.client_id,
                stage=stage,
                decision=decision,
                reason_code=reason_code,
                explanation=explanation,
                symbol=contract,
                contract=contract,
                strategy_version=self.strategy_version,
                config_hash=self.config_hash,
                git_commit=self.git_commit,
                inputs=inputs or {},
                thresholds=thresholds or {},
                context=context or {},
            )
        except Exception as e:
            log.debug(f"Order monitor observability emit failed (non-critical): {e}")

    def start(self):
        if self._thread and self._thread.is_alive():
            log.debug("[%s] APOrderMonitor already running", self.client_id)
            return
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name=f"order-monitor-{self.client_id}",
        )
        self._thread.start()
        log.info(
            f"[{self.client_id}] APOrderMonitor started "
            f"(poll={POLL_INTERVAL}s)"
        )

    def stop(self):
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=min(POLL_INTERVAL, 5))
        log.info(f"[{self.client_id}] APOrderMonitor stopping")

    def _run(self):
        # H4: exits are account risk and must be checked far more often than
        # entries. Entry checks stay at POLL_INTERVAL (a stuck entry just
        # doesn't fill — no open position bleeding). Exit checks run every
        # EXIT_CHECK_INTERVAL so a hung exit is caught in seconds, not a full
        # minute. This does NOT multiply entry/broker API load — only the
        # cheaper exit query runs on the fast cadence.
        _last_entry_check = 0.0
        while not self._stop_event.wait(EXIT_CHECK_INTERVAL):
            _now = time.monotonic()
            try:
                self._check_exit_orders()
            except Exception as e:
                log.error(f"[{self.client_id}] OrderMonitor exit-check error: {e}")
            if _now - _last_entry_check >= POLL_INTERVAL:
                _last_entry_check = _now
                try:
                    self._check_entry_orders()
                except Exception as e:
                    log.error(f"[{self.client_id}] OrderMonitor entry-check error: {e}")
            # PHASE 5 WIRE-IN: armed retries are polled on the FAST cadence
            # (every EXIT_CHECK_INTERVAL = 15s by default) so a retry whose
            # ready_at falls inside the 15-30s wait window is submitted within
            # one tick of its target. Putting this on the slow POLL_INTERVAL
            # (60s) would push worst-case submit latency to ~90s after cancel
            # — outside the spec. The check itself is cheap (one DB SELECT).
            # Wrapped in try/except: a retry-submit failure must NEVER prevent
            # the next stale-order tick.
            try:
                self._check_armed_retries()
            except Exception as e:
                log.error(
                    f"[{self.client_id}] OrderMonitor armed-retry-check error: {e}",
                    exc_info=True,
                )
            # PR #30 LIVE-SAFETY: reconciler staleness watchdog.
            # Alert-only. Does NOT change position state, does NOT block exits.
            try:
                from ap import reconciler_heartbeat as _hb
                _stale = _hb.check_staleness(self.client_id)
                if _stale.get("should_alert"):
                    log.critical(
                        "[%s] RECONCILER_STALE age_secs=%.1f sla_secs=%s cycles=%s",
                        self.client_id,
                        float(_stale.get("age_secs") or 0),
                        _stale.get("sla_secs"),
                        _stale.get("cycles"),
                    )
                    try:
                        self._emit_order_event(
                            local_order_id=None,
                            stage="reconciler_watchdog",
                            decision="CRITICAL",
                            reason_code="RECONCILER_STALE",
                            explanation=(
                                f"reconciler heartbeat age "
                                f"{_stale.get('age_secs')}s >= SLA "
                                f"{_stale.get('sla_secs')}s"
                            ),
                            contract=None,
                            inputs=_stale,
                        )
                    except Exception:
                        # _emit_order_event may not accept these args in all
                        # branches; the log.critical above is the authoritative
                        # alert.
                        pass
            except Exception as _hbe:
                log.debug(
                    "[%s] reconciler_heartbeat watchdog non-fatal: %s",
                    self.client_id, _hbe,
                )
            try:
                from ap.self_healing import get_healer as _gh
                _h = _gh()
                if _h:
                    _h.heartbeat(self.client_id, "order_monitor")
            except Exception:
                pass

    def _check_entry_orders(self):
        orders = self._get_active_entry_orders()
        now = datetime.now(timezone.utc)

        for order in orders:
            status = order.get("status", "")
            local_id = order.get("local_order_id", "")
            broker_oid = order.get("broker_order_id")
            created_ts = self._parse_ts(order.get("created_ts"))
            submitted_ts = self._parse_ts(order.get("submitted_ts"))
            contract = order.get("contract") or order.get("symbol", "?")

            if not created_ts:
                continue

            age_secs = (now - created_ts).total_seconds()

            if status == "CREATED":
                # Fast diagnostic watchdog: surface CREATED/no broker_id early.
                #
                # Overnight watcher-held entries should move CREATED → PENDING_TRIGGER
                # immediately after watcher arms (ap_overnight_reeval.py).
                #
                # Broker-submitted entries should move CREATED → SUBMITTED
                # immediately after Tradier returns broker_order_id.
                #
                # If still CREATED after 10s with no broker_id, something is wrong.
                # Log it fast — but keep the hard 120s cancel as final safety.
                if not broker_oid and age_secs > TIMEOUT_CREATED_NO_BROKER_WARN:
                    self._emit_order_event(
                        local_order_id=local_id,
                        stage="order_monitor",
                        decision="ERROR",
                        reason_code="CREATED_NO_BROKER_ID_WATCHDOG",
                        explanation=(
                            f"CREATED/no broker_order_id for {age_secs:.0f}s > "
                            f"{TIMEOUT_CREATED_NO_BROKER_WARN}s — expected PENDING_TRIGGER "
                            "if watcher-held, or SUBMITTED if broker-submitted"
                        ),
                        contract=contract,
                        inputs={
                            "status": status,
                            "age_secs": age_secs,
                            "broker_order_id": broker_oid,
                        },
                        thresholds={
                            "created_no_broker_warn": TIMEOUT_CREATED_NO_BROKER_WARN,
                            "timeout_created_cancel": TIMEOUT_CREATED,
                        },
                    )
                    log.warning(
                        "[%s] CREATED_NO_BROKER_ID_WATCHDOG | %s | %s | age=%.0fs — "
                        "check overnight_reeval PENDING_TRIGGER or broker submit path",
                        self.client_id, contract, local_id, age_secs,
                    )

                if age_secs > TIMEOUT_CREATED:
                    # P0 PR #141 — ENTRY HANDOFF RELIABILITY.
                    # Before declaring LOST_HANDOFF, perform ONE ownership
                    # re-arm attempt. Sequence:
                    #   1. probe watcher: does it already own this local_order_id?
                    #      (race resolved itself; just keep waiting one more cycle)
                    #   2. if not owned, rebuild a plan from the order row and
                    #      call entry_watcher.watch(plan, local_order_id) once
                    #   3. on success, leave the order in CREATED; the very
                    #      next watcher tick will transition it to PENDING_TRIGGER
                    #   4. on failure, fall through to LOST_HANDOFF_90S cancel
                    # This protects valid trades from being killed by transient
                    # startup/restart races where the order existed but no
                    # watcher had attached yet.
                    _sig_id = order.get("signal_id") or "?"
                    _plan_id = order.get("plan_id") or "?"

                    _already_owned, _probe_ok, _probe_err = self._pending_trigger_watcher_owner_state(local_id)
                    if _already_owned is True:
                        # Watcher claims ownership — the race resolved on its own.
                        # Skip cancel this cycle; let the watcher transition it.
                        log.info(
                            "[%s] LOST_HANDOFF_OWNERSHIP_CONFIRMED | local=%s "
                            "contract=%s age=%.0fs — watcher already owns; "
                            "skipping cancel",
                            self.client_id, local_id, contract, age_secs,
                        )
                        continue

                    # Attempt single auto re-arm before cancel.
                    _rearm_attempted = False
                    _rearm_succeeded = False
                    _rearm_reason = None
                    # Read prior re-arm attempts from orders.meta (JSONB).
                    # meta may arrive as dict (psycopg2 JSONB) or str (raw JSON);
                    # handle both shapes defensively.
                    _meta_raw = order.get("meta")
                    if isinstance(_meta_raw, dict):
                        _already_attempted = bool(_meta_raw.get("auto_rearm_attempted"))
                    elif isinstance(_meta_raw, str) and _meta_raw:
                        try:
                            import json as _json_check
                            _already_attempted = bool(
                                _json_check.loads(_meta_raw).get("auto_rearm_attempted")
                            )
                        except Exception:
                            _already_attempted = False
                    else:
                        _already_attempted = False
                    if _already_attempted:
                        _rearm_reason = "already_attempted_in_prior_cycle"
                        log.info(
                            "[%s] LOST_HANDOFF_REARM_SKIP | local=%s contract=%s — "
                            "auto re-arm already attempted previously",
                            self.client_id, local_id, contract,
                        )
                    else:
                        _rearm_attempted, _rearm_succeeded, _rearm_reason = (
                            self._attempt_lost_handoff_rearm(order, local_id, contract)
                        )

                    if _rearm_succeeded:
                        # Trade survives. Persist the attempt so we never
                        # re-arm the same order twice, and skip cancel.
                        log.info(
                            "[%s] LOST_HANDOFF_REARM_OK | local=%s contract=%s "
                            "age=%.0fs signal_id=%s — watcher re-armed, keeping order",
                            self.client_id, local_id, contract, age_secs, _sig_id,
                        )
                        self._record_lost_handoff_rearm(
                            local_id, attempted=True, succeeded=True,
                            reason=_rearm_reason or "lost_handoff_recovery",
                        )
                        continue

                    # Re-arm not attempted, or attempted and failed.
                    # Now we honestly declare LOST_HANDOFF and cancel.
                    _enriched_reason = (
                        f"LOST_HANDOFF_90S: CREATED for {age_secs:.0f}s > {TIMEOUT_CREATED}s — "
                        f"never submitted (signal_id={_sig_id} plan_id={_plan_id} "
                        f"broker_order_id={broker_oid or 'null'}); "
                        f"auto_rearm_attempted={_rearm_attempted} reason={_rearm_reason}"
                    )
                    log.warning(
                        "[%s] LOST_HANDOFF_90S | local=%s contract=%s age=%.0fs "
                        "signal_id=%s plan_id=%s broker_oid=%s rearm_attempted=%s "
                        "rearm_reason=%s",
                        self.client_id, local_id, contract, age_secs,
                        _sig_id, _plan_id, broker_oid or "null",
                        _rearm_attempted, _rearm_reason,
                    )
                    if _rearm_attempted:
                        self._record_lost_handoff_rearm(
                            local_id, attempted=True, succeeded=False,
                            reason=_rearm_reason or "lost_handoff_recovery",
                        )
                    self._handle_stale_entry(
                        local_id, status, contract, age_secs,
                        action="cancel",
                        reason=_enriched_reason,
                    )
                    # Systemic-rate guard — detect bot-wide handoff failure.
                    try:
                        self._record_lost_handoff_and_check_systemic()
                    except Exception as _e:
                        log.debug("[%s] lost-handoff systemic check failed: %s", self.client_id, _e)

            elif status == "PENDING_TRIGGER":
                self._check_pending_trigger_order(
                    order=order,
                    local_id=local_id,
                    contract=contract,
                    age_secs=age_secs,
                    broker_oid=broker_oid,
                    submitted_ts=submitted_ts,
                )

            elif status == "SUBMITTED":
                ref_ts = submitted_ts or created_ts
                age_secs = (now - ref_ts).total_seconds()

                # Missed-move + hard entry-age cancel (applies to SUBMITTED)
                if self._check_stale_entry_cancel(
                    order, local_id, status, contract, age_secs
                ):
                    continue

                if age_secs > TIMEOUT_SUBMITTED:
                    if not broker_oid:
                        log.debug(
                            f"[{self.client_id}] Watcher-held order {local_id} "
                            f"({contract}) SUBMITTED for {age_secs:.0f}s — skipping stale cancel"
                        )
                        self._emit_order_event(
                            local_order_id=local_id,
                            stage="order_monitor",
                            decision="ALERT",
                            reason_code="NO_BROKER_ACK",
                            explanation=f"Order SUBMITTED for {age_secs:.0f}s but has no broker_order_id — watcher-held, skipping cancel",
                            contract=contract,
                            inputs={"status": status, "age_secs": age_secs},
                        )
                    else:
                        broker_status = self._query_broker_order(broker_oid)
                        if broker_status:
                            self._advance_from_broker_status(local_id, broker_status, contract)
                        else:
                            self._handle_stale_entry(
                                local_id, status, contract, age_secs,
                                action="cancel",
                                reason=(
                                    f"SUBMITTED for {age_secs:.0f}s > {TIMEOUT_SUBMITTED}s "
                                    f"— no broker response"
                                ),
                            )

            elif status == "ACKNOWLEDGED":
                ref_ts = submitted_ts or created_ts
                age_secs = (now - ref_ts).total_seconds()

                # MISSED-MOVE + HARD ENTRY-AGE CANCEL — also applies to ACKNOWLEDGED.
                # The SMCI bug: an acknowledged-unfilled buy-to-open sat for 1hr+
                # because missed-move only checked SUBMITTED and the ACK timeout
                # was 10min. Now the same stale-entry protection runs here.
                if self._check_stale_entry_cancel(
                    order, local_id, status, contract, age_secs
                ):
                    continue

                if age_secs > TIMEOUT_ACKNOWLEDGED:
                    broker_status = self._query_broker_order(broker_oid)
                    if broker_status:
                        self._advance_from_broker_status(local_id, broker_status, contract)
                    else:
                        self._handle_stale_entry(
                            local_id, status, contract, age_secs,
                            action="alert_and_cancel",
                            reason=(
                                f"ACKNOWLEDGED for {age_secs:.0f}s > {TIMEOUT_ACKNOWLEDGED}s "
                                f"— no fill, scalp entry stale"
                            ),
                        )

            elif status == "PARTIAL_FILL":
                ref_ts = submitted_ts or created_ts
                age_secs = (now - ref_ts).total_seconds()
                if age_secs > TIMEOUT_PARTIAL_FILL:
                    # INTENTIONALLY PASSIVE — partial fills are not auto-canceled.
                    # A partial fill = real contracts at real cost. Auto-canceling
                    # the remainder risks leaving an unhedged position.
                    # Mitigation: exit engine manages the filled portion independently.
                    self._emit_order_event(
                        local_order_id=local_id,
                        stage="order_monitor",
                        decision="ALERT",
                        reason_code="PARTIAL_FILL_STALLED",
                        explanation=f"PARTIAL_FILL stalled for {age_secs:.0f}s > {TIMEOUT_PARTIAL_FILL}s",
                        contract=contract,
                        position_id=order.get("position_id"),
                        inputs={"status": status, "age_secs": age_secs},
                        thresholds={"timeout_partial_fill": TIMEOUT_PARTIAL_FILL},
                    )
                    self._alert(
                        f"⚠️ PARTIAL_FILL stalled | {self.client_id} | {contract} "
                        f"| {local_id} | {age_secs:.0f}s > {TIMEOUT_PARTIAL_FILL}s "
                        f"| Manual review required"
                    )

    def _is_after_pt_eod_cutoff(self) -> bool:
        """Return True if the current UTC time is past the entry-cutoff ET boundary.

        Uses _PT_ORPHAN_EOD_CUTOFF_HOUR / _PT_ORPHAN_EOD_CUTOFF_MIN (default
        15:30 ET), matching ap_entry_watcher.EOD_CUTOFF_HOUR / EOD_CUTOFF_MIN.
        After this time, watcher-held rows that still haven't breached are
        legitimately terminal for the session.
        """
        try:
            import zoneinfo
            _et = zoneinfo.ZoneInfo("America/New_York")
        except ImportError:
            try:
                from datetime import timezone as _tz
                import pytz as _pytz
                _et = _pytz.timezone("America/New_York")
            except ImportError:
                # Cannot determine ET — default conservative: assume NOT past cutoff
                # so watcher-held rows are preserved rather than wrongly expired.
                return False
        now_et = datetime.now(_et)
        return (
            now_et.hour > _PT_ORPHAN_EOD_CUTOFF_HOUR
            or (now_et.hour == _PT_ORPHAN_EOD_CUTOFF_HOUR
                and now_et.minute >= _PT_ORPHAN_EOD_CUTOFF_MIN)
        )

    def _is_overnight_or_deferred_row(self, order: dict) -> tuple[bool, str]:
        """Return (is_overnight_or_deferred, evidence_description).

        Overnight and deferred PENDING_TRIGGER rows must NOT be EOD-expired
        at the 15:30 ET wall-clock cutoff. They are designed to survive across
        sessions and will be re-evaluated at the next market open by
        overnight_reeval or the morning handoff audit.

        Evidence that a row is overnight/deferred (any single item is sufficient):
          1. contract starts with DEFERRED: — explicitly deferred to breach time
          2. meta.contract_deferred = True  — explicit deferred flag from MC
          3. meta.overnight = True          — set by overnight signal path
          4. meta.queue_status indicates overnight/open recheck
             (after_hours_deferred, awaiting_overnight_reeval, open_recheck)
          5. timeframe in daily/overnight/1d variants — The Strat daily timeframe
          6. meta.prior_day_high or meta.prior_day_low present — cross-session
             level, not an intraday signal
          7. order.prior_day_high or order.prior_day_low present (top-level)

        Same-day intraday signals (e.g. timeframe=5m, no deferred evidence)
        MAY be EOD-expired after cutoff — they missed the session window.
        """
        meta_raw = order.get("meta")
        if isinstance(meta_raw, str):
            try:
                import json as _json
                meta = _json.loads(meta_raw) if meta_raw else {}
            except Exception:
                meta = {}
        elif isinstance(meta_raw, dict):
            meta = meta_raw
        else:
            meta = {}

        contract = str(order.get("contract") or "").strip()

        # Evidence 1: DEFERRED:* contract
        if contract.upper().startswith("DEFERRED:"):
            return True, f"deferred_contract={contract}"

        # Evidence 2: meta.contract_deferred
        if meta.get("contract_deferred"):
            return True, "meta.contract_deferred=True"

        # Evidence 3: meta.overnight
        if meta.get("overnight"):
            return True, "meta.overnight=True"

        # Evidence 4: meta.queue_status overnight/open-recheck variants
        _qs = str(meta.get("queue_status") or "").lower().strip()
        if _qs in (
            "after_hours_deferred",
            "awaiting_overnight_reeval",
            "open_recheck",
            "overnight_deferred",
            "deferred",
        ):
            return True, f"meta.queue_status={_qs}"

        # Evidence 5: timeframe indicates daily/overnight signal
        _tf = str(
            order.get("timeframe")
            or meta.get("timeframe")
            or ""
        ).strip().lower()
        if _tf in ("1d", "daily", "overnight", "d", "day"):
            return True, f"timeframe={_tf}"

        # Evidence 6: prior_day_high/low in meta (cross-session level)
        if meta.get("prior_day_high") is not None or meta.get("prior_day_low") is not None:
            return True, "meta.prior_day_high_or_low_present"

        # Evidence 7: prior_day_high/low at top level (stamped by plan builder)
        if (order.get("prior_day_high") is not None
                or order.get("prior_day_low") is not None):
            return True, "prior_day_high_or_low_present"

        return False, "no_overnight_deferred_evidence"

    def _is_valid_watcher_held_pending_trigger(self, order: dict) -> tuple[bool, str]:
        """Return (is_valid_watcher_row, evidence_description).

        A PENDING_TRIGGER ENTRY row is watcher-held when it carries evidence
        that it is legitimately waiting for a breach trigger before broker
        submission. Such rows MUST NOT be expired as broker orphans solely
        because broker_order_id/submitted_ts are NULL — that is the expected
        pre-submit state for every overnight/deferred/watchlisted entry.

        Evidence checked (any single item is sufficient):
          1. trigger_price field is non-null/non-zero — set by MC when arming
          2. meta.trigger_type present — set by OSM on watcher entries
          3. meta.watcher_audit present — APEntryWatcher audit of the arm
          4. meta.watcher_audit_history present — multi-cycle audit trail
          5. contract is a real option symbol (TICKER + date + C/P + strike)
             OR starts with DEFERRED: (overnight deferred — watcher will
             resolve the contract at breach time)
          6. meta.contract_deferred = True — explicit deferred flag from MC

        NOT valid watcher evidence:
          - status=PENDING_TRIGGER alone (could be broken CREATED→PT handoff)
          - arbitrary meta fields like 'score' or 'tier'

        Returns (False, reason) if no watcher evidence found. The caller
        should then fall through to the normal orphan expiry logic, which
        preserves additional guards (watcher ownership check, dry-run mode,
        cleanup_enabled flag).
        """
        meta_raw = order.get("meta")
        if isinstance(meta_raw, str):
            try:
                import json as _json
                meta = _json.loads(meta_raw) if meta_raw else {}
            except Exception:
                meta = {}
        elif isinstance(meta_raw, dict):
            meta = meta_raw
        else:
            meta = {}

        contract = str(order.get("contract") or "").strip()

        # Evidence 1: trigger_price set
        _trigger_price = order.get("trigger_price")
        if _trigger_price is not None:
            try:
                if float(_trigger_price) > 0:
                    return True, f"trigger_price={_trigger_price}"
            except (TypeError, ValueError):
                pass

        # Evidence 2: meta.trigger_type present (set by OSM on watcher entries)
        _trigger_type = meta.get("trigger_type") or meta.get("trigger_type_label")
        if _trigger_type and str(_trigger_type).strip():
            return True, f"trigger_type={_trigger_type}"

        # Evidence 3: meta.watcher_audit present
        if meta.get("watcher_audit"):
            return True, "watcher_audit_present"

        # Evidence 4: meta.watcher_audit_history present
        if meta.get("watcher_audit_history"):
            return True, "watcher_audit_history_present"

        # Evidence 5a: contract is a real option symbol (heuristic: 8+ chars,
        #   contains digits and C or P surrounded by digits — not a raw ticker)
        if contract and len(contract) >= 8:
            import re as _re
            if _re.search(r'\d{6}[CP]\d{5,8}', contract):
                return True, f"real_option_contract={contract}"

        # Evidence 5b: DEFERRED:* overnight signal waiting for breach
        if contract.upper().startswith("DEFERRED:"):
            return True, f"deferred_contract={contract}"

        # Evidence 6: meta.contract_deferred = True
        if meta.get("contract_deferred"):
            return True, "contract_deferred=True"

        return False, "no_watcher_evidence_found"

    def _check_pending_trigger_order(
        self,
        *,
        order: dict,
        local_id: str,
        contract: str,
        age_secs: float,
        broker_oid,
        submitted_ts,
    ) -> None:
        if age_secs <= PENDING_TRIGGER_MAX_AGE_SECONDS:
            return

        _submitted_repr = submitted_ts.isoformat() if submitted_ts else "None"
        (
            _watcher_owner_state,
            _ownership_check_available,
            _ownership_check_error,
        ) = self._pending_trigger_watcher_owner_state(local_id)
        _owner_state_repr = (
            "unknown" if _watcher_owner_state is None else str(_watcher_owner_state)
        )
        _ownership_error_repr = _ownership_check_error or "None"

        if broker_oid:
            self._log_pending_trigger_watchdog_seen(
                level="critical",
                contract=contract,
                local_id=local_id,
                age_secs=age_secs,
                broker_order_id=broker_oid,
                submitted_ts_repr=_submitted_repr,
                watcher_owner_state=_owner_state_repr,
                ownership_check_available=_ownership_check_available,
                ownership_check_error=_ownership_error_repr,
                cleanup_action="skip_broker_order_id_present",
            )
            return

        if submitted_ts:
            self._log_pending_trigger_watchdog_seen(
                level="info",
                contract=contract,
                local_id=local_id,
                age_secs=age_secs,
                broker_order_id="None",
                submitted_ts_repr=_submitted_repr,
                watcher_owner_state=_owner_state_repr,
                ownership_check_available=_ownership_check_available,
                ownership_check_error=_ownership_error_repr,
                cleanup_action="skip_submitted_ts_present",
            )
            return

        if _watcher_owner_state is True:
            self._log_pending_trigger_watchdog_seen(
                level="info",
                contract=contract,
                local_id=local_id,
                age_secs=age_secs,
                broker_order_id="None",
                submitted_ts_repr=_submitted_repr,
                watcher_owner_state=_owner_state_repr,
                ownership_check_available=_ownership_check_available,
                ownership_check_error=_ownership_error_repr,
                cleanup_action="preserve_watcher_owned",
            )
            return

        if _watcher_owner_state is None:
            _unknown_action = (
                "dry_run_only" if PENDING_TRIGGER_CLEANUP_DRY_RUN
                else "preserve_ownership_unknown"
            )
            self._log_pending_trigger_watchdog_seen(
                level="warning",
                contract=contract,
                local_id=local_id,
                age_secs=age_secs,
                broker_order_id="None",
                submitted_ts_repr=_submitted_repr,
                watcher_owner_state=_owner_state_repr,
                ownership_check_available=_ownership_check_available,
                ownership_check_error=_ownership_error_repr,
                cleanup_action=_unknown_action,
            )
            log.warning(
                "[%s] PENDING_TRIGGER_ORPHAN_OWNERSHIP_UNKNOWN | %s | %s | age=%.0fs "
                "| watcher_owner_state=%s | ownership_check_available=%s "
                "| ownership_check_error=%s | cleanup_action=%s",
                self.client_id,
                contract,
                local_id,
                age_secs,
                _owner_state_repr,
                _ownership_check_available,
                _ownership_error_repr,
                _unknown_action,
            )
            return

        # ── P0 (hotfix/pending-trigger-orphan-guard): watcher-evidence guard ──
        #
        # The watcher owner check (_watcher_owner_state is False) means this
        # process's in-memory watcher does not currently hold this order.
        # That happens legitimately on every watcher restart, pod restart, or
        # recovery cycle — the OSM row persists across restarts but the in-
        # memory watcher set does not.
        #
        # Before declaring the row a broker orphan and expiring it, check
        # whether the row itself carries evidence that it is a valid watcher-
        # held entry waiting for a breach trigger:
        #   - trigger_price set            (MC wires this at plan creation)
        #   - meta.trigger_type present    (OSM writes this from plan.trigger_type)
        #   - meta.watcher_audit present   (watcher writes this on arm)
        #   - meta.watcher_audit_history   (watcher multi-cycle audit trail)
        #   - real option contract symbol  (C260626C00150000, not a raw ticker)
        #   - DEFERRED:* contract          (overnight deferred pre-breach)
        #   - meta.contract_deferred=True  (explicit deferred flag from MC)
        #
        # Production regression (Jason order 22514):
        #   contract=C260626C00150000, trigger_price=155.0, reserved_cost=105.0
        #   broker_order_id=NULL, submitted_ts=NULL, age=5452s > 5400s
        #   → was expired as PENDING_TRIGGER_ORPHAN_EXPIRED
        #   → WRONG: it was a valid breach-waiting entry; NULL broker fields
        #     are EXPECTED for pre-submit watcher-held rows.
        #
        # After this guard:
        #   If watcher evidence found AND entry cutoff has NOT passed:
        #     → preserve the row and attempt a re-arm recovery.
        #     → log PENDING_TRIGGER_WATCHER_EVIDENCE_PRESERVED.
        #   If watcher evidence found AND entry cutoff HAS passed:
        #     → expire with a cutoff reason (not orphan reason).
        #     → log PENDING_TRIGGER_WATCHER_EVIDENCE_EOD_EXPIRED.
        #   If no watcher evidence:
        #     → fall through to normal orphan expiry (existing behavior).

        _has_watcher_evidence, _watcher_evidence_desc = (
            self._is_valid_watcher_held_pending_trigger(order)
        )

        if _has_watcher_evidence:
            _after_cutoff = self._is_after_pt_eod_cutoff()

            if not _after_cutoff:
                # Valid watcher-held row, market session still open — PRESERVE.
                # Attempt a re-arm in case the watcher lost its in-memory state
                # after a restart. If re-arm fails, still preserve the row;
                # the next cycle will retry.
                _rearm_attempted, _rearm_succeeded, _rearm_reason = (
                    self._attempt_lost_handoff_rearm(order, local_id, contract)
                )
                log.info(
                    "[%s] PENDING_TRIGGER_WATCHER_EVIDENCE_PRESERVED "
                    "| %s | %s | age=%.0fs | evidence=%s "
                    "| rearm_attempted=%s | rearm_succeeded=%s | rearm_reason=%s "
                    "| cleanup_action=preserve_watcher_evidence",
                    self.client_id,
                    contract,
                    local_id,
                    age_secs,
                    _watcher_evidence_desc,
                    _rearm_attempted,
                    _rearm_succeeded,
                    _rearm_reason,
                )
                self._log_pending_trigger_watchdog_seen(
                    level="info",
                    contract=contract,
                    local_id=local_id,
                    age_secs=age_secs,
                    broker_order_id="None",
                    submitted_ts_repr=_submitted_repr,
                    watcher_owner_state=_owner_state_repr,
                    ownership_check_available=_ownership_check_available,
                    ownership_check_error=_ownership_error_repr,
                    cleanup_action="preserve_watcher_evidence",
                )
                return

            else:
                # Wall-clock is past 15:30 ET.
                # Before expiring, check whether this is an overnight or deferred
                # row. Those survive across sessions — the 15:30 cutoff applies
                # only to same-day intraday signals that missed the trading window.
                _is_overnight, _overnight_evidence = (
                    self._is_overnight_or_deferred_row(order)
                )
                if _is_overnight:
                    # Overnight/deferred row: PRESERVE even after cutoff.
                    # overnight_reeval or the morning handoff audit will handle it
                    # at the next session open. Do NOT write EOD_EXPIRED or
                    # ORPHAN_EXPIRED — this row is working as designed.
                    _rearm_attempted, _rearm_succeeded, _rearm_reason = (
                        self._attempt_lost_handoff_rearm(order, local_id, contract)
                    )
                    log.info(
                        "[%s] PENDING_TRIGGER_OVERNIGHT_PRESERVED "
                        "| %s | %s | age=%.0fs | watcher_evidence=%s "
                        "| overnight_evidence=%s "
                        "| rearm_attempted=%s | rearm_succeeded=%s | rearm_reason=%s "
                        "| cleanup_action=preserve_overnight_deferred",
                        self.client_id,
                        contract,
                        local_id,
                        age_secs,
                        _watcher_evidence_desc,
                        _overnight_evidence,
                        _rearm_attempted,
                        _rearm_succeeded,
                        _rearm_reason,
                    )
                    self._log_pending_trigger_watchdog_seen(
                        level="info",
                        contract=contract,
                        local_id=local_id,
                        age_secs=age_secs,
                        broker_order_id="None",
                        submitted_ts_repr=_submitted_repr,
                        watcher_owner_state=_owner_state_repr,
                        ownership_check_available=_ownership_check_available,
                        ownership_check_error=_ownership_error_repr,
                        cleanup_action="preserve_overnight_deferred",
                    )
                    return

                # Same-day watcher row, entry cutoff passed — this trade missed
                # its window. Expire with a cutoff reason (NOT orphan reason).
                _eod_reason = (
                    f"PENDING_TRIGGER_EOD_EXPIRED: watcher_evidence={_watcher_evidence_desc} "
                    f"age={age_secs:.0f}s entry_cutoff_passed=True"
                )
                log.warning(
                    "[%s] PENDING_TRIGGER_WATCHER_EVIDENCE_EOD_EXPIRED "
                    "| %s | %s | age=%.0fs | evidence=%s "
                    "| cleanup_action=expire_eod_cutoff | reason=%s",
                    self.client_id,
                    contract,
                    local_id,
                    age_secs,
                    _watcher_evidence_desc,
                    _eod_reason,
                )
                self._log_pending_trigger_watchdog_seen(
                    level="warning",
                    contract=contract,
                    local_id=local_id,
                    age_secs=age_secs,
                    broker_order_id="None",
                    submitted_ts_repr=_submitted_repr,
                    watcher_owner_state=_owner_state_repr,
                    ownership_check_available=_ownership_check_available,
                    ownership_check_error=_ownership_error_repr,
                    cleanup_action="expire_eod_cutoff",
                )
                if not PENDING_TRIGGER_CLEANUP_ENABLED or PENDING_TRIGGER_CLEANUP_DRY_RUN:
                    return
                try:
                    if hasattr(self.osm, "expire_pending_entry"):
                        self.osm.expire_pending_entry(local_id, reason=_eod_reason)
                    elif hasattr(self.osm, "transition"):
                        self.osm.transition(local_id, "EXPIRED", last_error=_eod_reason)
                except Exception as _eod_exc:
                    log.error(
                        "[%s] PENDING_TRIGGER_EOD_EXPIRE_FAILED | %s | %s | error=%s",
                        self.client_id, contract, local_id, _eod_exc,
                    )
                return
        # ── End watcher-evidence guard ─────────────────────────────────────

        self._log_pending_trigger_watchdog_seen(
            level="info",
            contract=contract,
            local_id=local_id,
            age_secs=age_secs,
            broker_order_id="None",
            submitted_ts_repr=_submitted_repr,
            watcher_owner_state=_owner_state_repr,
            ownership_check_available=_ownership_check_available,
            ownership_check_error=_ownership_error_repr,
            cleanup_action=(
                "cleanup_disabled"
                if not PENDING_TRIGGER_CLEANUP_ENABLED
                else (
                    "dry_run_only"
                    if PENDING_TRIGGER_CLEANUP_DRY_RUN
                    else "expire_orphan_candidate"
                )
            ),
        )

        if not PENDING_TRIGGER_CLEANUP_ENABLED:
            return

        reason = (
            "PENDING_TRIGGER_ORPHAN_EXPIRED: no_broker_order_id "
            f"no_submitted_ts age={age_secs:.0f}s"
        )

        if PENDING_TRIGGER_CLEANUP_DRY_RUN:
            log.warning(
                "[%s] PENDING_TRIGGER_ORPHAN_DRY_RUN | %s | %s | age=%.0fs "
                "| watcher_owner_state=%s | ownership_check_available=%s "
                "| ownership_check_error=%s | cleanup_action=%s "
                "| cleanup_method=dry_run | cleanup_success=%s | reason=%s",
                self.client_id,
                contract,
                local_id,
                age_secs,
                _owner_state_repr,
                _ownership_check_available,
                _ownership_error_repr,
                "dry_run_only",
                False,
                reason,
            )
            return

        cleanup_method = "expire_pending_entry"
        cleanup_action = "expire_pending_entry"
        cleanup_success = False

        if hasattr(self.osm, "expire_pending_entry"):
            try:
                cleanup_success = bool(
                    self.osm.expire_pending_entry(local_id, reason=reason)
                )
            except Exception as exc:
                log.error(
                    "[%s] PENDING_TRIGGER expire_pending_entry failed | %s | %s | error=%s",
                    self.client_id,
                    contract,
                    local_id,
                    exc,
                )
                cleanup_success = False

        if not cleanup_success and hasattr(self.osm, "transition"):
            cleanup_method = "transition:EXPIRED"
            cleanup_action = "transition_expired"
            try:
                cleanup_success = bool(
                    self.osm.transition(local_id, "EXPIRED", last_error=reason)
                )
            except Exception as exc:
                log.error(
                    "[%s] PENDING_TRIGGER transition(EXPIRED) failed | %s | %s | error=%s",
                    self.client_id,
                    contract,
                    local_id,
                    exc,
                )
                cleanup_success = False

        getattr(log, "info" if cleanup_success else "error")(
            "[%s] PENDING_TRIGGER_ORPHAN_EXPIRED | %s | %s | age=%.0fs "
            "| watcher_owner_state=%s | ownership_check_available=%s "
            "| ownership_check_error=%s | cleanup_action=%s "
            "| cleanup_method=%s | cleanup_success=%s | reason=%s",
            self.client_id,
            contract,
            local_id,
            age_secs,
            _owner_state_repr,
            _ownership_check_available,
            _ownership_error_repr,
            cleanup_action,
            cleanup_method,
            cleanup_success,
            reason,
        )

    def _log_pending_trigger_watchdog_seen(
        self,
        *,
        level: str,
        contract: str,
        local_id: str,
        age_secs: float,
        broker_order_id: str,
        submitted_ts_repr: str,
        watcher_owner_state: str,
        ownership_check_available: bool,
        ownership_check_error: str,
        cleanup_action: str,
    ) -> None:
        getattr(log, level)(
            "[%s] PENDING_TRIGGER_WATCHDOG_SEEN | %s | %s | age=%.0fs "
            "| broker_order_id=%s | submitted_ts=%s | watcher_owner_state=%s "
            "| ownership_check_available=%s | ownership_check_error=%s "
            "| cleanup_enabled=%s | dry_run=%s | cleanup_action=%s",
            self.client_id,
            contract,
            local_id,
            age_secs,
            broker_order_id,
            submitted_ts_repr,
            watcher_owner_state,
            ownership_check_available,
            ownership_check_error,
            PENDING_TRIGGER_CLEANUP_ENABLED,
            PENDING_TRIGGER_CLEANUP_DRY_RUN,
            cleanup_action,
        )

    def _pending_trigger_watcher_owner_state(
        self,
        local_order_id: str,
    ) -> tuple[Optional[bool], bool, Optional[str]]:
        watcher = getattr(self, "entry_watcher", None)
        if watcher is None:
            return None, False, "entry_watcher_missing"

        has_order = getattr(watcher, "has_order", None)
        if not callable(has_order):
            return None, False, "has_order_unavailable"

        try:
            return bool(has_order(local_order_id)), True, None
        except Exception as exc:
            return None, True, f"{type(exc).__name__}: {exc}"

    def _attempt_lost_handoff_rearm(
        self,
        order: dict,
        local_order_id: str,
        contract: str,
    ) -> tuple[bool, bool, Optional[str]]:
        """Single watcher re-arm attempt for a CREATED order whose ownership was lost.

        Returns (attempted, succeeded, reason).

        Sequence:
          1. resolve entry_watcher (None → not attempted)
          2. rebuild a minimal plan from the order row (same path as recovery)
          3. call entry_watcher.watch(plan, local_order_id) exactly once
          4. on success, the next watcher poll will transition the order to
             PENDING_TRIGGER; we leave the order row alone.

        This NEVER bypasses the watcher's own quality gates — watch() applies
        the same stop-above-mid, contract-validity, and quote-availability
        checks it would for a fresh arm.
        """
        watcher = getattr(self, "entry_watcher", None)
        if watcher is None:
            return (False, False, "entry_watcher_missing")
        if not callable(getattr(watcher, "watch", None)):
            return (False, False, "watcher_watch_unavailable")

        plan = self._build_lost_handoff_plan_from_order(order)
        if plan is None:
            return (False, False, "plan_rebuild_failed")

        try:
            armed = bool(watcher.watch(plan, local_order_id))
        except Exception as exc:
            log.error(
                "[%s] LOST_HANDOFF_REARM_EXCEPTION | local=%s contract=%s error=%s",
                self.client_id, local_order_id, contract, exc,
            )
            return (True, False, f"watch_exception:{type(exc).__name__}")

        if not armed:
            return (True, False, getattr(watcher, "_last_reject_reason", "watch_returned_false"))
        return (True, True, "lost_handoff_recovery")

    def _build_lost_handoff_plan_from_order(self, order: dict):
        """Rebuild a minimal watcher plan from an orders row.

        Mirrors APStartupRecovery._build_recovery_plan_from_order so that
        the in-flight handoff re-arm uses the exact same plan shape the
        startup-reseed path produces. If the order lacks ticker or
        trigger_price, returns None — caller will fall through to cancel.
        """
        try:
            import types as _types
            meta_raw = order.get("meta")
            if isinstance(meta_raw, dict):
                meta = meta_raw
            elif isinstance(meta_raw, str):
                try:
                    import json as _json
                    meta = _json.loads(meta_raw) if meta_raw else {}
                except Exception:
                    meta = {}
            else:
                meta = {}

            ticker = str(
                order.get("symbol")
                or meta.get("symbol")
                or meta.get("ticker")
                or ""
            ).upper()
            if not ticker:
                return None

            trigger = (
                order.get("trigger_price")
                if order.get("trigger_price") is not None
                else meta.get("signal_entry_price")
            )
            try:
                trigger = float(trigger) if trigger is not None else 0.0
            except (TypeError, ValueError):
                trigger = 0.0
            if trigger <= 0:
                return None

            contract = (
                order.get("contract")
                or meta.get("selected_contract")
                or meta.get("contract_symbol")
                or ""
            )
            direction = (
                self._normalize_order_direction(order.get("direction"))
                or self._normalize_order_direction(meta.get("direction"))
                or self._normalize_order_direction(meta.get("side"))
                or self._direction_from_occ_contract(contract)
            )
            if direction not in {"CALL", "PUT"}:
                self._emit_order_event(
                    local_order_id=order.get("local_order_id") or order.get("id"),
                    stage="order_monitor",
                    decision="BLOCK",
                    reason_code="LOST_HANDOFF_REARM_FAILED_INVALID_OR_MISSING_SIDE",
                    explanation=(
                        "Lost-handoff watcher rearm blocked because direction/side was "
                        "missing or invalid and could not be safely derived from OCC C/P."
                    ),
                    contract=str(contract or ""),
                    inputs={
                        "order_direction": order.get("direction"),
                        "meta_direction": meta.get("direction"),
                        "meta_side": meta.get("side"),
                        "contract": contract,
                    },
                )
                return None
            stop = (
                order.get("stop_underlying")
                if order.get("stop_underlying") is not None
                else meta.get("stop_underlying")
            )
            target = (
                order.get("target_underlying")
                if order.get("target_underlying") is not None
                else meta.get("target_underlying")
            )

            plan = _types.SimpleNamespace(
                signal_id=str(
                    order.get("signal_id")
                    or meta.get("signal_id")
                    or order.get("local_order_id")
                    or ""
                ),
                plan_id=str(order.get("plan_id") or meta.get("plan_id") or ""),
                ticker=ticker,
                symbol=ticker,
                direction=direction,
                side=direction,
                trigger_price=trigger,
                stop_underlying=stop,
                target_underlying=target,
                contract_symbol=str(contract or ""),
                contracts=int(order.get("qty") or meta.get("contracts") or 0) or 1,
                tier=str(order.get("tier") or meta.get("tier") or "B"),
                score=float(order.get("score") or meta.get("score") or 0) or 0.0,
                timeframe=str(meta.get("timeframe") or "1d"),
                pattern=str(meta.get("pattern") or ""),
                limit_price=float(order.get("limit_price") or 0) or None,
                metadata=dict(meta) if isinstance(meta, dict) else {},
            )
            # Preserve deferred-contract behavior — see PR #140.
            if str(plan.contract_symbol).upper().startswith("DEFERRED:"):
                try:
                    if not isinstance(plan.metadata, dict):
                        plan.metadata = {}
                    plan.metadata["contract_deferred"] = True
                except Exception:
                    pass
            return plan
        except Exception as exc:
            log.warning(
                "[%s] LOST_HANDOFF_PLAN_REBUILD_FAILED | local=%s error=%s",
                self.client_id, order.get("local_order_id"), exc,
            )
            return None

    def _record_lost_handoff_rearm(
        self,
        local_order_id: str,
        *,
        attempted: bool,
        succeeded: bool,
        reason: str,
    ) -> None:
        """Persist auto-rearm attempt on the orders row's meta (JSONB) so we
        never re-arm the same order twice across watchdog cycles."""
        if not local_order_id:
            return
        try:
            from ap.db import run_with_retry, conn
            payload = {
                "auto_rearm_attempted": attempted,
                "auto_rearm_succeeded": succeeded,
                "auto_rearm_reason": reason,
            }

            def _persist():
                with conn() as c:
                    c.execute(
                        """
                        UPDATE orders
                        SET meta       = COALESCE(meta, '{}'::jsonb)
                                          || %s::jsonb,
                            updated_ts = NOW()
                        WHERE local_order_id = %s
                          AND client_id = %s
                        """,
                        (
                            __import__("json").dumps(payload),
                            local_order_id,
                            self.client_id,
                        ),
                    )
            run_with_retry(_persist)
        except Exception as exc:
            log.warning(
                "[%s] LOST_HANDOFF_REARM_PERSIST_FAILED | local=%s error=%s",
                self.client_id, local_order_id, exc,
            )

    def _check_exit_orders(self):
        orders = self._get_active_exit_orders()
        now = datetime.now(timezone.utc)

        for order in orders:
            status = order.get("status", "")
            local_id = order.get("local_order_id", "")
            broker_oid = order.get("broker_order_id")
            position_id = order.get("position_id")
            created_ts = self._parse_ts(order.get("created_ts"))
            submitted_ts = self._parse_ts(order.get("submitted_ts"))
            contract = order.get("contract") or order.get("symbol", "?")

            if not created_ts:
                continue

            ref_ts = submitted_ts or created_ts
            age_secs = (now - ref_ts).total_seconds()

            if status in ("EXIT_REQUESTED", "EXIT_SUBMITTED"):
                if age_secs > TIMEOUT_EXIT_PENDING:
                    broker_status = self._query_broker_order(broker_oid)
                    if self._is_executed_status(broker_status) or self._is_terminal_failure_status(broker_status):
                        self._advance_from_broker_status(local_id, broker_status, contract)
                    else:
                        self._handle_stale_exit(
                            local_id, status, contract, age_secs,
                            position_id=position_id,
                            reason=(
                                f"EXIT {status} for {age_secs:.0f}s > {TIMEOUT_EXIT_PENDING}s "
                                f"— ESCALATING"
                            ),
                        )

            elif status == "EXIT_ACKNOWLEDGED":
                if age_secs > TIMEOUT_EXIT_ACK:
                    broker_status = self._query_broker_order(broker_oid)
                    if broker_status:
                        self._advance_from_broker_status(local_id, broker_status, contract)
                    else:
                        self._handle_stale_exit(
                            local_id, status, contract, age_secs,
                            position_id=position_id,
                            reason=(
                                f"EXIT_ACKNOWLEDGED for {age_secs:.0f}s > {TIMEOUT_EXIT_ACK}s "
                                f"— no fill confirmation — ESCALATING"
                            ),
                        )

            elif status == "EXIT_PARTIAL_FILL":
                if age_secs > TIMEOUT_PARTIAL_FILL:
                    # INTENTIONALLY PASSIVE — exit partial fills are NOT auto-retried.
                    # The filled portion is closed; auto-retrying the remainder risks
                    # double-exit on already-closed contracts.
                    # Policy: alert at CRITICAL level, require manual review.
                    self._emit_order_event(
                        local_order_id=local_id,
                        stage="order_monitor",
                        decision="ALERT",
                        reason_code="PARTIAL_FILL_STALLED",
                        explanation=f"EXIT_PARTIAL_FILL stalled for {age_secs:.0f}s > {TIMEOUT_PARTIAL_FILL}s",
                        contract=contract,
                        position_id=position_id,
                        inputs={"status": status, "age_secs": age_secs},
                        thresholds={"timeout_partial_fill": TIMEOUT_PARTIAL_FILL},
                    )
                    self._alert(
                        f"🚨 EXIT PARTIAL_FILL STALLED | {self.client_id} | {contract} "
                        f"| {local_id} | {age_secs:.0f}s | pos={position_id} "
                        f"| MANUAL INTERVENTION REQUIRED"
                    )

    def _check_stale_entry_cancel(
        self,
        order: dict,
        local_id: str,
        status: str,
        contract: str,
        age_secs: float,
    ) -> bool:
        """Cancel a stale unfilled buy-to-open entry limit.

        Returns True if the order was cancelled (caller should `continue`).

        Two independent triggers, EITHER fires a cancel:
          (a) MISSED MOVE: current option price has run far enough above the
              limit that filling now means entering late into a setup the
              move already left behind.
          (b) HARD AGE: the entry limit has simply been unfilled too long for
              a scalp entry, regardless of price.

        Applies to SUBMITTED and ACKNOWLEDGED (the SMCI bug was an
        ACKNOWLEDGED order the old SUBMITTED-only check never touched).
        """
        try:
            broker_oid = self._get_broker_order_id(local_id)

            # Tightened defaults — old 600s/1.20x was effectively "never" for
            # a scalp. 75s / 1.07x catches a stale entry before it fills late.
            _missed_min_secs  = int(os.getenv("MISSED_MOVE_MIN_SECS", "6"))
            _missed_price_mult = float(os.getenv("MISSED_MOVE_PRICE_MULT", "1.07"))

            _limit_price = (
                order.get("limit_price")
                or order.get("price")
                or (self.osm.get_order(local_id) or {}).get("limit_price")
                or (self.osm.get_order(local_id) or {}).get("price")
            )
            _sym = order.get("contract") or order.get("symbol", "") or contract

            # ── Trigger (a): RE-PEG-FIRST, then MISSED-MOVE-CANCEL ────────
            #
            # BUG-A FIX (PR #29, 2026-05-23): the prior shape gated _try_repeg
            # behind `current > limit * 1.07`. That meant a $3.08 limit with
            # a $3.10 current ask (the exact case where bumping the limit by
            # one tick gets us filled) was IGNORED — we waited for the option
            # to run 7% (to $3.30) before even considering a re-peg, by which
            # point the move was already over.
            #
            # New shape:
            #   Step 1 — if minimal preconditions are met (broker_oid +
            #            age >= MISSED_MOVE_MIN_SECS + limit + sym + we got a
            #            current option quote), ALWAYS attempt _try_repeg.
            #            retry_engine.decide_repeg owns the alignment /
            #            proximity / attempt-count gating; we don't second-
            #            guess it with a 7% pre-filter.
            #   Step 2 — only if _try_repeg DECLINED *and* the current ask is
            #            truly above limit * MISSED_MOVE_PRICE_MULT (the
            #            runaway threshold) do we run MISSED_MOVE_CANCEL.
            #
            # Net effect: bangers that move 2% from our limit get re-pegged
            # and filled; only orders that truly ran past us get canceled.
            if (
                broker_oid
                and age_secs >= _missed_min_secs
                and _limit_price
                and float(_limit_price) > 0
                and _sym
            ):
                _current = self._get_option_price(_sym)
                if _current and _current > 0:
                    # Step 1: attempt re-peg unconditionally. The repeg engine
                    # owns alignment/proximity/attempt gating.
                    if self._try_repeg(
                        order, local_id, contract, _sym,
                        float(_limit_price), _current, status, age_secs,
                    ):
                        # Repeg applied; next tick observes the new limit.
                        return True

                    # Step 2: repeg declined. Only cancel if current price is
                    # ACTUALLY runaway past limit * MISSED_MOVE_PRICE_MULT.
                    # A repeg-decline at 2% above limit must NOT trigger
                    # cancel — we keep working the original limit and let
                    # the Phase 2 adaptive autocancel ceiling decide.
                    _runaway = _current > float(_limit_price) * _missed_price_mult
                    if ENABLE_MISSED_MOVE_CANCEL and _runaway:
                        _pct_above = (_current / float(_limit_price) - 1) * 100
                        log.warning(
                            "[%s] MISSED_MOVE_CANCEL | %s | limit=$%.2f current=$%.2f "
                            "(%.0f%% above) status=%s age=%.0fs — move happened without us",
                            self.client_id, _sym, float(_limit_price), _current,
                            _pct_above, status, age_secs,
                        )
                        self._emit_order_event(
                            local_order_id=local_id,
                            stage="order_monitor",
                            decision="REJECT",
                            reason_code="MISSED_MOVE_ENTRY_CANCEL",
                            explanation=(
                                f"Canceling stale entry — option ran {_pct_above:.0f}% "
                                f"above limit before fill (status={status}, age={age_secs:.0f}s)"
                            ),
                            contract=contract,
                            inputs={
                                "limit_price": float(_limit_price),
                                "current_option_price": _current,
                                "percent_above_limit": round(_pct_above, 2),
                                "age_secs": round(age_secs, 1),
                                "broker_order_id": broker_oid,
                                "status": status,
                            },
                        )
                        self._handle_stale_entry(
                            local_id, status, contract, age_secs,
                            action="cancel",
                            reason=(
                                f"STALE_ENTRY_CANCEL MISSED_MOVE — limit=${float(_limit_price):.2f} "
                                f"current=${_current:.2f} ({_pct_above:.0f}% above) "
                                f"status={status} age={age_secs:.0f}s"
                            ),
                        )
                        return True

            # ── Trigger (b): HARD ENTRY AGE ───────────────────────────────
            # A buy-to-open that has not filled within the hard ceiling is a
            # PHASE 2 ADAPTIVE AUTOCANCEL (2026-05-23):
            # At ENTRY_REEVAL_AGE_SECONDS (25s default), re-evaluate alignment
            # instead of hard-canceling. If still aligned, the order may live
            # to ENTRY_MAX_AGE_NORMAL (90s) or ENTRY_MAX_AGE_APLUS (120s) for
            # A/A+ setups. Hard cancel only on thesis break or hitting the
            # absolute ceiling.
            #
            # Score lookup for A/A+ tier:
            _order_meta = order.get("meta") if isinstance(order.get("meta"), dict) else {}
            # PR fix/entry-retry-context-and-submit-refresh:
            # Score priority is:
            #   1) order.score      (top-level column — PR #44 ground truth)
            #   2) order.meta.score (older paths / retry-engine mirror)
            #   3) 0                (only if genuinely missing)
            # Same priority for tier. The PRIOR expression
            # (`_order_meta.get("score") or order.get("score")`) put meta FIRST,
            # which is wrong for two reasons:
            #   - meta.score is `None` on rows where retry_engine wrote
            #     retry_status/retry_abort_ts but didn't carry the original meta
            #     forward.
            #   - top-level score is authoritative (written by OSM
            #     create_entry_order in PR #44).
            try:
                _score = float(
                    order.get("score")
                    or _order_meta.get("score")
                    or 0
                )
            except (TypeError, ValueError):
                _score = 0.0
            _tier_str = (
                str(order.get("tier") or _order_meta.get("tier") or "").strip()
            )
            _is_aplus = _score >= ENTRY_APLUS_SCORE_THRESHOLD
            # PR66: PAPER entries get more breathing room (sandbox fills lag).
            # LIVE keeps the strict ceiling.  A+ tier always uses ENTRY_MAX_AGE_APLUS
            # regardless of mode — the wider A+ window already provides relief.
            _is_paper = (self.client_mode == "PAPER")
            _normal_ceiling = PAPER_ENTRY_MAX_AGE_NORMAL if _is_paper else LIVE_ENTRY_MAX_AGE_NORMAL
            _max_age = ENTRY_MAX_AGE_APLUS if _is_aplus else _normal_ceiling

            # PR #68 — PAPER entry retry ladder + market fallback.
            # Strict scope: this block only runs when client_mode='PAPER' AND
            # broker_oid is set AND the order is still ENTRY/SUBMITTED/ACK.
            # LIVE mode falls through unchanged.
            #
            # The ladder reprices at 10s/25s/40s using the current ask plus
            # ENTRY_PAPER_ASK_CROSS_CENTS, and at 45s (if enabled) submits a
            # bounded market order. The order is NEVER cancelled by the age
            # ceiling before the ladder has had a chance to run.
            if _is_paper and broker_oid:
                try:
                    self._paper_entry_retry_and_fallback(
                        order=order,
                        local_id=local_id,
                        broker_oid=broker_oid,
                        contract=contract,
                        sym=_sym,
                        score=_score,
                        tier_str=_tier_display if '_tier_display' in dir() else _tier_str,
                        is_aplus=_is_aplus,
                        age_secs=age_secs,
                        order_meta=_order_meta,
                    )
                except Exception as _exc:
                    log.warning(
                        "[%s] paper_entry_retry_and_fallback raised — "
                        "falling through to legacy age-cancel: %s",
                        self.client_id, _exc,
                    )

            if broker_oid and age_secs >= ENTRY_REEVAL_AGE_SECONDS:
                # Step 1: if past absolute ceiling, hard-cancel with explicit reason.
                if age_secs >= _max_age:
                    _ceiling_reason = (
                        "ENTRY_MAX_AGE_APLUS_REACHED" if _is_aplus
                        else "ENTRY_MAX_AGE_NORMAL_REACHED"
                    )
                    # PR fix/entry-retry-context-and-submit-refresh:
                    # Display the REAL tier label (A+/A/B/etc.) from the row
                    # when available, not the binary aplus/normal bucket. The
                    # bucket is still used for the max-age decision but the
                    # log text now matches the row contents — fixes the
                    # "score=0.0 tier=normal" log noise on rows that actually
                    # had score=70 tier=B.
                    _tier_display = (
                        _tier_str
                        or ("A+" if _is_aplus else "normal")
                    )
                    log.warning(
                        "[%s] %s | %s | status=%s age=%.0fs >= %ds (score=%.1f tier=%s mode=%s)",
                        self.client_id, _ceiling_reason, _sym or contract, status,
                        age_secs, _max_age, _score, _tier_display, self.client_mode,
                    )
                    # Capture live quote for metadata (best-effort — never blocks cancel).
                    _bid, _ask, _mid = None, None, None
                    try:
                        _quote = self._quote_broker().get_quote(_sym)
                        if _quote:
                            _bid = _quote.get("bid")
                            _ask = _quote.get("ask")
                            if _bid is not None and _ask is not None:
                                try:
                                    _mid = round((float(_bid) + float(_ask)) / 2, 4)
                                except Exception:
                                    pass
                    except Exception:
                        pass
                    _retries = int(_order_meta.get("retry_count") or 0)
                    self._emit_order_event(
                        local_order_id=local_id,
                        stage="order_monitor",
                        decision="REJECT",
                        reason_code=_ceiling_reason,
                        explanation=(
                            f"Canceling stale entry at absolute ceiling — unfilled "
                            f"{age_secs:.0f}s >= {_max_age}s "
                            f"({'A+' if _is_aplus else 'normal'} tier, mode={self.client_mode})"
                        ),
                        contract=contract,
                        inputs={
                            # PR66 required metadata fields.
                            # cancel_reason mirrors _ceiling_reason so A+ orders
                            # correctly record ENTRY_MAX_AGE_APLUS_REACHED, not NORMAL.
                            "cancel_reason":  _ceiling_reason,
                            "mode":           self.client_mode,
                            "max_age_seconds": _max_age,
                            "actual_age_seconds": round(age_secs, 1),
                            "symbol":         order.get("symbol") or "",
                            "direction":      _order_meta.get("direction") or _order_meta.get("side") or "",
                            "contract":       contract or "",
                            "score":          _score,
                            "tier":           _tier_display,
                            "limit_price":    float(_limit_price) if _limit_price else 0.0,
                            "bid":            _bid,
                            "ask":            _ask,
                            "mid":            _mid,
                            "retries":        _retries,
                            # Existing fields kept for backward compat
                            "age_secs":       round(age_secs, 1),
                            "max_age":        _max_age,
                            "is_aplus":       _is_aplus,
                            "broker_order_id": broker_oid,
                            "status":         status,
                        },
                    )
                    self._handle_stale_entry(
                        local_id, status, contract, age_secs,
                        action="cancel",
                        reason=(
                            f"{_ceiling_reason} unfilled {age_secs:.0f}s >= {_max_age}s "
                            f"(score={_score:.1f} tier={_tier_display} mode={self.client_mode})"
                        ),
                    )
                    return True

                # Step 2: between 25s and the ceiling, run alignment re-eval.
                # We DON'T cancel here — we just emit a re-eval telemetry event.
                # The repeg engine (called separately at MISSED_MOVE_MIN_SECS)
                # handles actually adjusting the limit. This step exists so
                # operators can SEE the re-eval happening in dashboard logs.
                #
                # Throttle: only log re-eval once per 15s to avoid log spam.
                _last_reeval = (_order_meta.get("last_reeval_ts") or 0)
                try:
                    _last_reeval = float(_last_reeval)
                except (TypeError, ValueError):
                    _last_reeval = 0.0
                _import_time = __import__("time")
                if _import_time.time() - _last_reeval >= 15:
                    log.info(
                        "[%s] ENTRY_REEVAL | %s | status=%s age=%.0fs (max=%ds tier=%s score=%.1f) — "
                        "order continues if aligned",
                        self.client_id, _sym or contract, status, age_secs, _max_age,
                        "A+" if _is_aplus else "normal", _score,
                    )
                    self._emit_order_event(
                        local_order_id=local_id,
                        stage="order_monitor",
                        decision="CONTINUE",
                        reason_code="ENTRY_REEVAL",
                        explanation=(
                            f"Re-evaluating at {age_secs:.0f}s — ceiling={_max_age}s "
                            f"({'A+' if _is_aplus else 'normal'} tier, score={_score:.1f})"
                        ),
                        contract=contract,
                        inputs={
                            "age_secs":    round(age_secs, 1),
                            "max_age":     _max_age,
                            "reeval_threshold": ENTRY_REEVAL_AGE_SECONDS,
                            "score":       _score,
                            "is_aplus":    _is_aplus,
                            "status":      status,
                        },
                    )
                    # Best-effort: persist last_reeval_ts in order meta to throttle.
                    try:
                        from ap.db import update_order
                        new_meta = dict(_order_meta)
                        new_meta["last_reeval_ts"] = _import_time.time()
                        update_order(local_id, meta=new_meta)
                    except Exception:
                        pass
                # Re-eval done; do NOT cancel. Order continues.
                return False

        except Exception as _se:
            log.debug(
                "[%s] _check_stale_entry_cancel non-fatal error: %s",
                self.client_id, _se,
            )
        return False

    # ─── PHASE 5 WIRE-IN: post-cancel retry orchestration ──────────────────────
    #
    # Two methods compose the retry orchestrator:
    #
    #   _maybe_arm_post_cancel_retry(local_order_id, contract, reason)
    #       Called once, immediately after a successful CANCELED transition.
    #       Consults ap.post_cancel_retry.evaluate_retry. On ARM, persists the
    #       retry intent into the canceled order's meta JSONB and emits
    #       ENTRY_RETRY_ARMED. On ABORT, emits ENTRY_RETRY_ABORTED.
    #
    #   _check_armed_retries()
    #       Called every EXIT_CHECK_INTERVAL by the main loop. Selects this
    #       client's CANCELED orders whose meta carries retry_status='ARMED'
    #       AND retry_ready_at <= NOW(). For each, builds the signal payload
    #       and hands it to ap.execution.process_signal, then transitions the
    #       meta to retry_status='SUBMITTED' (or 'FAILED' if process_signal
    #       rejected). Emits ENTRY_RETRY_SUBMITTED on success.
    #
    # Storage choice: the retry intent is stored in the existing orders.meta
    # JSONB column (added by 20260519_phase2_orders_meta.sql). No new table
    # is required — the canceled order row is durable, and a JSON object on
    # it captures everything we need. retry_status is one of:
    #   ARMED      — evaluate_retry returned ARM; submit pending until ready_at
    #   SUBMITTED  — process_signal accepted the retry; a new order exists
    #   ABORTED    — evaluate_retry returned ABORT (or retry was canceled)
    #   FAILED     — process_signal returned ok=False at retry submit
    #
    # Duplicate-position guard: process_signal already calls
    # acquire_symbol_lock and _count_active_entry_orders_today. Those gates
    # prevent us from arming a position while one is already open. We also
    # short-circuit if a FILLED position on the same symbol exists.

    def _release_symbol_lock_for_canceled(
        self,
        local_order_id: str,
        contract: Optional[str],
    ) -> None:
        """BUG-B FIX (PR #29): release the symbol lock for a just-canceled
        ENTRY order so the 15-30s post-cancel retry can submit.

        The symbol lock is acquired in execution.process_signal with a 90s
        TTL. When the order is canceled within that window, the lock is
        still live and the retry returns {error: 'symbol_locked'}.

        We derive the symbol from (in priority order):
          1. order.symbol             — the canonical underlying ticker
          2. order.meta.ticker        — set by process_signal during admission
          3. OCC root from contract   — fallback when neither is set

        Best-effort: a release failure must never crash the monitor.
        """
        try:
            from ap.execution import release_symbol_lock
        except Exception as e:
            log.debug(
                "[%s] release_symbol_lock unavailable: %s",
                self.client_id, e,
            )
            return

        symbol = None
        try:
            order = self.osm.get_order(local_order_id) or {}
            meta = order.get("meta") if isinstance(order.get("meta"), dict) else {}
            symbol = (
                (order.get("symbol") or "").strip().upper()
                or (meta or {}).get("ticker")
                or self._get_underlying_symbol_from_contract(contract)
            )
            if isinstance(symbol, str):
                symbol = symbol.strip().upper()
        except Exception as e:
            log.debug(
                "[%s] _release_symbol_lock_for_canceled: symbol lookup failed: %s",
                self.client_id, e,
            )

        if not symbol:
            log.debug(
                "[%s] _release_symbol_lock_for_canceled: no symbol derivable for %s (contract=%s) — skipping",
                self.client_id, local_order_id, contract,
            )
            return

        try:
            release_symbol_lock(self.client_id, symbol)
            log.info(
                "[%s] SYMBOL_LOCK_RELEASED_POST_CANCEL order=%s symbol=%s",
                self.client_id, local_order_id, symbol,
            )
        except Exception as e:
            log.debug(
                "[%s] release_symbol_lock failed for %s/%s: %s",
                self.client_id, local_order_id, symbol, e,
            )

    def _maybe_arm_post_cancel_retry(
        self,
        local_order_id: str,
        contract: str,
        cancel_reason: str,
    ) -> None:
        if not ENTRY_RETRY_ENABLED:
            return
        try:
            from ap.post_cancel_retry import evaluate_retry
        except Exception as e:
            log.debug("[%s] post_cancel_retry unavailable: %s", self.client_id, e)
            return

        order = self.osm.get_order(local_order_id) or {}
        if not order:
            log.warning(
                "[%s] _maybe_arm_post_cancel_retry: order not found %s",
                self.client_id, local_order_id,
            )
            return

        # Resolve underlying spot for the alignment gate. Re-uses the same
        # logic as _try_repeg — stocks live in get_quote on the underlying
        # symbol, not on the OCC option contract.
        meta = order.get("meta") if isinstance(order.get("meta"), dict) else {}
        underlying = (
            self._get_underlying_symbol_from_contract(contract)
            or order.get("symbol")
            or (meta or {}).get("ticker")
        )
        spot: Optional[float] = None
        if underlying:
            try:
                if hasattr(self.broker, "get_quote"):
                    q = self._quote_broker().get_quote(underlying) or {}
                    last = q.get("last") or q.get("close") or q.get("price")
                    if last:
                        spot = float(last)
                    elif q.get("bid") and q.get("ask"):
                        spot = (float(q["bid"]) + float(q["ask"])) / 2
            except Exception as e:
                log.debug(
                    "[%s] retry: underlying spot lookup failed for %s: %s",
                    self.client_id, underlying, e,
                )

        decision = evaluate_retry(
            canceled_order=order,
            cancel_reason=cancel_reason,
            underlying_spot=spot,
        )

        # Build inputs/context once; emitted on both ARM and ABORT paths.
        evt_inputs = {
            "cancel_reason":           cancel_reason,
            "cancel_reason_normalized": decision.cancel_reason_normalized,
            "attempt_number":          decision.attempt_number,
            "max_attempts":            decision.max_attempts,
            "alignment_ok":            decision.alignment_ok,
            "underlying_spot":         spot,
            "signal_entry_price":      decision.signal_entry_price,
            "direction":               decision.direction,
        }

        if decision.action == "ABORT":
            log.info(
                "[%s] ENTRY_RETRY_ABORTED order=%s reason=%s detail=%s",
                self.client_id, local_order_id,
                decision.reason_code, decision.explanation,
            )
            self._emit_order_event(
                local_order_id=local_order_id,
                stage="post_cancel_retry",
                decision="ABORT",
                reason_code=decision.reason_code,
                explanation=decision.explanation,
                contract=contract,
                inputs=evt_inputs,
            )
            # Best-effort: stamp meta so the dashboard can show the abort
            # alongside the cancel.
            try:
                from ap.db import update_order
                _meta = dict(meta or {})
                _meta["retry_status"] = "ABORTED"
                _meta["retry_abort_reason"] = decision.reason_code
                _meta["retry_abort_ts"] = now_utc_iso()
                update_order(local_order_id, meta=_meta)
            except Exception as e:
                log.debug(
                    "[%s] retry: failed to stamp ABORTED meta on %s: %s",
                    self.client_id, local_order_id, e,
                )
            return

        # action == 'ARM'
        ready_at_epoch = time.time() + float(decision.wait_secs)
        try:
            from ap.db import update_order
            _meta = dict(meta or {})
            _meta["retry_status"]      = "ARMED"
            _meta["retry_attempt"]     = int(decision.attempt_number)
            _meta["retry_armed_ts"]    = now_utc_iso()
            _meta["retry_ready_at"]    = float(ready_at_epoch)
            _meta["retry_wait_secs"]   = float(decision.wait_secs)
            _meta["retry_payload"]     = decision.retry_payload
            _meta["retry_cancel_reason"] = decision.cancel_reason_normalized
            update_order(local_order_id, meta=_meta)
        except Exception as e:
            log.error(
                "[%s] retry: failed to persist ARMED meta on %s: %s — SKIPPING retry",
                self.client_id, local_order_id, e,
                exc_info=True,
            )
            return

        log.info(
            "[%s] ENTRY_RETRY_ARMED order=%s attempt=%d/%d wait=%.1fs "
            "cancel_reason=%s direction=%s alignment_ok=%s",
            self.client_id, local_order_id,
            decision.attempt_number, decision.max_attempts,
            decision.wait_secs,
            decision.cancel_reason_normalized,
            decision.direction, decision.alignment_ok,
        )
        self._emit_order_event(
            local_order_id=local_order_id,
            stage="post_cancel_retry",
            decision="ARM",
            reason_code=decision.reason_code,
            explanation=decision.explanation,
            contract=contract,
            inputs={**evt_inputs, "wait_secs": decision.wait_secs,
                    "ready_at_epoch": ready_at_epoch},
        )

    def _check_armed_retries(self) -> None:
        """Look for this client's ARMED retries whose ready_at has passed,
        submit each via process_signal, and update meta to SUBMITTED/FAILED.
        """
        if not ENTRY_RETRY_ENABLED:
            return

        # Query: this client's CANCELED orders with meta.retry_status='ARMED'
        # and retry_ready_at <= now. Cap at 16 per tick to avoid burst submits.
        now_epoch = time.time()
        rows = []
        try:
            def _fn():
                with conn() as c:
                    c.execute(
                        """
                        SELECT local_order_id, contract, symbol, direction, meta
                        FROM   orders
                        WHERE  client_id = %s
                          AND  kind = 'ENTRY'
                          AND  status = 'CANCELED'
                          AND  meta ->> 'retry_status' = 'ARMED'
                          AND  COALESCE((meta ->> 'retry_ready_at')::float, 0) <= %s
                        ORDER BY updated_ts ASC
                        LIMIT 16
                        """,
                        (self.client_id, now_epoch),
                    )
                    return [dict(r) for r in c.fetchall()]
            rows = run_with_retry(_fn) or []
        except Exception as e:
            log.error(
                "[%s] _check_armed_retries: DB select failed: %s",
                self.client_id, e, exc_info=True,
            )
            return

        if not rows:
            return

        log.info(
            "[%s] _check_armed_retries: processing %d ready retr%s",
            self.client_id, len(rows), "y" if len(rows) == 1 else "ies",
        )

        for row in rows:
            local_oid = row.get("local_order_id")
            contract  = row.get("contract")
            meta      = row.get("meta") if isinstance(row.get("meta"), dict) else {}
            payload   = (meta or {}).get("retry_payload") or {}
            self._submit_armed_retry(local_oid, contract, payload, meta)

    def _submit_armed_retry(
        self,
        local_order_id: str,
        contract: Optional[str],
        retry_payload: dict,
        prior_meta: dict,
    ) -> None:
        # Defensive: refuse to submit if the payload is empty or malformed.
        if not isinstance(retry_payload, dict) or not retry_payload.get("ticker"):
            log.warning(
                "[%s] _submit_armed_retry: malformed payload for %s — marking FAILED",
                self.client_id, local_order_id,
            )
            self._stamp_retry_status(local_order_id, prior_meta,
                                     status="FAILED", detail="malformed_payload")
            return

        # Hand to the same admission path a fresh signal uses. process_signal
        # enforces symbol lock, equity reserve, daily cap, kill-switch, trend
        # gate, and chase-band guard. We do NOT re-implement any of that here.
        try:
            from ap.execution import process_signal
        except Exception as e:
            log.error(
                "[%s] _submit_armed_retry: cannot import process_signal: %s",
                self.client_id, e,
            )
            self._stamp_retry_status(local_order_id, prior_meta,
                                     status="FAILED", detail=f"import_error:{e}")
            return

        try:
            result = process_signal(self.broker, self.client_id, retry_payload)
        except Exception as e:
            log.error(
                "[%s] _submit_armed_retry: process_signal raised for retry of %s: %s",
                self.client_id, local_order_id, e, exc_info=True,
            )
            self._stamp_retry_status(local_order_id, prior_meta,
                                     status="FAILED", detail=f"exception:{e}")
            self._emit_order_event(
                local_order_id=local_order_id,
                stage="post_cancel_retry",
                decision="ERROR",
                reason_code="RETRY_SUBMIT_EXCEPTION",
                explanation=str(e),
                contract=contract,
            )
            return

        if not isinstance(result, dict) or not result.get("ok"):
            err = (result or {}).get("error", "unknown")
            # PR fix/entry-retry-context-and-submit-refresh:
            # Classify quote-related submit rejects with explicit codes so
            # the audit trail distinguishes runaway-quote (price moved out of
            # chase band) from a missing refresh (broker quote API failed).
            # Generic gates (time_gate, trend_gate, symbol_locked, ...) keep
            # the generic submit_reject:<err> shape they had before.
            _r = result or {}
            if err == "runaway_quote_at_submit":
                _detail = (
                    f"RUNAWAY_QUOTE_AT_RETRY:gap={_r.get('gap_pct'):.4f} "
                    f"selector_ask={_r.get('selector_ask')} "
                    f"submit_ask={_r.get('submit_ask')}"
                    if _r.get("gap_pct") is not None
                    else "RUNAWAY_QUOTE_AT_RETRY"
                )
            elif err in ("quote_refresh_failed", "submit_quote_unavailable"):
                # If the quote refresh outright failed (broker API error),
                # surface that distinctly so operators can disambiguate from
                # "price moved" runaways. process_signal already falls back
                # to selector_ask on refresh failure unless explicitly
                # configured otherwise; this branch is mainly defensive.
                _detail = f"QUOTE_REFRESH_FAILED_AT_RETRY:{err}"
            else:
                _detail = f"submit_reject:{err}"
            # Persist quote-refresh evidence in meta on the FAILURE path too
            # (previously only the SUCCESS path recorded it via _emit). The
            # dashboard / post-mortem can now answer "what was the quote at
            # retry submit?" for every failed retry, not just successes.
            log.warning(
                "[%s] ENTRY_RETRY_ABORTED order=%s submit-time reject: %s",
                self.client_id, local_order_id, err,
            )
            self._stamp_retry_status(
                local_order_id, prior_meta,
                status="FAILED",
                detail=_detail,
                extra={
                    "retry_submit_err":        err,
                    "retry_selector_ask":      _r.get("selector_ask"),
                    "retry_submit_ask":        _r.get("submit_ask"),
                    "retry_submit_limit":      _r.get("submit_limit"),
                    "retry_quote_age_ms":      _r.get("quote_age_ms"),
                    "retry_gap_pct":           _r.get("gap_pct"),
                    "retry_refresh_ok":        _r.get("refresh_ok"),
                    "retry_refresh_reason":    _r.get("refresh_reason"),
                },
            )
            # Also emit a post-cancel-retry abort event so the dashboard
            # records the full lifecycle (ARM → submit-time reject).
            self._emit_order_event(
                local_order_id=local_order_id,
                stage="post_cancel_retry",
                decision="ABORT",
                reason_code="SUBMIT_REJECT",
                explanation=str(err),
                contract=contract,
                inputs={"submit_result": result},
            )
            return

        # Success: process_signal accepted the retry. A NEW local_order_id
        # exists for the retry order; reference it back to the canceled one.
        new_local_oid = result.get("local_order_id")
        new_broker_oid = result.get("broker_order_id")
        log.info(
            "[%s] ENTRY_RETRY_SUBMITTED prev=%s new=%s broker=%s contract=%s",
            self.client_id, local_order_id, new_local_oid, new_broker_oid,
            result.get("contract"),
        )
        self._stamp_retry_status(
            local_order_id, prior_meta,
            status="SUBMITTED",
            detail="",
            extra={
                "retry_new_local_order_id":  new_local_oid,
                "retry_new_broker_order_id": new_broker_oid,
                "retry_submitted_ts":        now_utc_iso(),
                # PR fix/entry-retry-context-and-submit-refresh:
                # Persist the full submit-time quote evidence on success too
                # so success and failure rows have a consistent shape.
                "retry_selector_ask":        result.get("selector_ask"),
                "retry_submit_ask":          result.get("submit_ask"),
                "retry_submit_limit":        result.get("submit_limit"),
                "retry_quote_age_ms":        result.get("quote_age_ms"),
                "retry_refresh_ok":          True if result.get("submit_ask") is not None else None,
                "retry_submit_refresh_ok":   True if result.get("submit_ask") is not None else None,
            },
        )
        self._emit_order_event(
            local_order_id=local_order_id,
            stage="post_cancel_retry",
            decision="SUBMIT",
            reason_code="RETRY_SUBMITTED",
            explanation=f"retry submitted as {new_local_oid}",
            contract=result.get("contract") or contract,
            inputs={
                "new_local_order_id":  new_local_oid,
                "new_broker_order_id": new_broker_oid,
                "qty":                 result.get("qty"),
                "submit_limit":        result.get("submit_limit"),
                "selector_ask":        result.get("selector_ask"),
                "submit_ask":          result.get("submit_ask"),
                "quote_age_ms":        result.get("quote_age_ms"),
            },
        )

    def _stamp_retry_status(
        self,
        local_order_id: str,
        prior_meta: dict,
        *,
        status: str,
        detail: str = "",
        extra: Optional[dict] = None,
    ) -> None:
        try:
            from ap.db import update_order
            _meta = dict(prior_meta or {})
            _meta["retry_status"]    = status
            _meta["retry_status_ts"] = now_utc_iso()
            if detail:
                _meta["retry_status_detail"] = detail
            if extra:
                _meta.update(extra)
            update_order(local_order_id, meta=_meta)
        except Exception as e:
            log.error(
                "[%s] _stamp_retry_status failed for %s status=%s: %s",
                self.client_id, local_order_id, status, e,
            )

    def _handle_stale_entry(
        self,
        local_order_id: str,
        status: str,
        contract: str,
        age_secs: float,
        action: str,
        reason: str,
    ):
        self._emit_order_event(
            local_order_id=local_order_id,
            stage="order_monitor",
            decision="ESCALATE",
            reason_code="STALE_ENTRY_TIMEOUT",
            explanation=reason,
            contract=contract,
            inputs={"status": status, "age_secs": age_secs},
            thresholds={
                "timeout_created": TIMEOUT_CREATED,
                "timeout_submitted": TIMEOUT_SUBMITTED,
                "timeout_acknowledged": TIMEOUT_ACKNOWLEDGED,
            },
        )

        log.warning(
            f"[{self.client_id}] STALE ENTRY | {contract} | {local_order_id} "
            f"| status={status} | {reason}"
        )

        if "alert" in action:
            self._alert(
                f"⚠️ STALE ENTRY ORDER | {self.client_id} | {contract} "
                f"| {local_order_id} | {reason}"
            )

        # WATCHDOG GATE — with ENTRY exception.
        # Position lifecycle authority stays with fill monitor/reconciler/OSM/
        # exit engine, so watchdog mode normally suppresses action. BUT an
        # unfilled buy-to-open ENTRY is NOT a position — leaving it to fill
        # late is the actual risk (the SMCI bug). Entry cancels are therefore
        # permitted even in watchdog mode.
        _is_entry_cancel = ("cancel" in action) and status in (
            "CREATED", "SUBMITTED", "ACKNOWLEDGED",
        )
        _entry_cancel_allowed = ALLOW_ENTRY_CANCEL_IN_WATCHDOG and _is_entry_cancel

        if not ORDER_MONITOR_CAN_ACT and not _entry_cancel_allowed:
            self._alert(
                f"[WATCHDOG ONLY] Stale entry detected; no cancel attempted | "
                f"{self.client_id} | {contract} | {local_order_id} | {reason}"
            )
            log.warning(
                "[%s] WATCHDOG MODE — stale entry action suppressed | order=%s status=%s action=%s",
                self.client_id, local_order_id, status, action,
            )
            return

        if not ORDER_MONITOR_CAN_ACT and _entry_cancel_allowed:
            log.warning(
                "[%s] WATCHDOG MODE but ENTRY CANCEL PERMITTED — unfilled %s "
                "entry is not position lifecycle | order=%s | %s",
                self.client_id, status, local_order_id, reason,
            )

        if "cancel" in action:
            broker_oid = self._get_broker_order_id(local_order_id)

            if status == "CREATED" and not broker_oid:
                ok = self.osm.transition(local_order_id, "CANCELED", last_error=reason)
                if ok:
                    log.info(
                        f"[{self.client_id}] Entry order CANCELED locally | "
                        f"{contract} | {local_order_id} | CREATED/no broker_id"
                    )
                    # BUG-B FIX (PR #29): release the symbol lock held by the
                    # original process_signal call before retry submits.
                    # Without this, the 15-30s retry hits symbol_locked
                    # because the 90s TTL lock from the canceled order is
                    # still in place. Best-effort only — a failed release must
                    # never crash the monitor.
                    self._release_symbol_lock_for_canceled(local_order_id, contract)
                    # PHASE 5 WIRE-IN: consider arming a post-cancel retry.
                    # CREATED/no-broker-id cancels almost never come from a
                    # retryable reason, so the engine will typically ABORT;
                    # call it anyway so every cancel goes through the same gate.
                    self._maybe_arm_post_cancel_retry(local_order_id, contract, reason)
                else:
                    log.error(
                        f"[{self.client_id}] Failed local cancel for CREATED entry: {local_order_id}"
                    )
                return

            cancel_result = None
            try:
                cancel_result = self._cancel_broker_order(broker_oid)
            except Exception as e:
                log.warning(f"[{self.client_id}] Broker cancel failed: {e}")

            confirmed_status = self._extract_broker_status(cancel_result)

            if self._is_terminal_cancel_status(confirmed_status):
                ok = self.osm.transition(local_order_id, "CANCELED", last_error=reason)
                if ok:
                    log.info(
                        f"[{self.client_id}] Entry order CANCELED (broker-confirmed) | "
                        f"{contract} | {local_order_id}"
                    )
                    # BUG-B FIX (PR #29): release the symbol lock held by the
                    # original process_signal call before retry submits.
                    # Without this, the 15-30s retry hits symbol_locked
                    # because the 90s TTL lock from the canceled order is
                    # still in place. Best-effort only — a failed release must
                    # never crash the monitor.
                    self._release_symbol_lock_for_canceled(local_order_id, contract)
                    # PHASE 5 WIRE-IN: this is the primary retry hook.
                    # After a broker-confirmed cancel we ask post_cancel_retry
                    # whether the signal is still actionable. If ARM, the
                    # retry intent lands in meta and _check_armed_retries
                    # re-submits after the jittered wait. If ABORT, we emit
                    # ENTRY_RETRY_ABORTED and move on.
                    self._maybe_arm_post_cancel_retry(local_order_id, contract, reason)
                else:
                    log.error(
                        f"[{self.client_id}] Failed to transition entry order to CANCELED: {local_order_id}"
                    )
            else:
                # Cancel request sent but broker has not yet confirmed terminal state.
                # Risk: order may fill after this check. Mitigation: next poll will
                # call _query_broker_order → _advance_from_broker_status and catch it.
                # This is eventually consistent — not a bug, a documented system property.
                log.warning(
                    "[%s] Cancel sent but NOT yet broker-confirmed | %s | %s | "
                    "broker_status=%s — next poll will re-query",
                    self.client_id, contract, local_order_id,
                    confirmed_status or "unknown",
                )
                # reason_code intentionally None here — "cancel sent but not confirmed"
                # is a distinct condition from NO_BROKER_ACK (no broker_order_id at all).
                # Reusing NO_BROKER_ACK would blur analytics. The explanation text is
                # precise enough; a new canonical code can be added centrally if needed.
                self._emit_order_event(
                    local_order_id=local_order_id,
                    stage="order_monitor",
                    decision="ALERT",
                    reason_code=None,
                    explanation=(
                        f"Cancel sent but not yet broker-confirmed for {contract} "
                        f"| broker_status={confirmed_status or 'unknown'} "
                        "| next poll will re-query and advance state if canceled"
                    ),
                    contract=contract,
                    inputs={"confirmed_status": confirmed_status, "age_secs": age_secs},
                )
                self._alert(
                    f"Cancel sent but NOT broker-confirmed | {self.client_id} | "
                    f"{contract} | {local_order_id} | broker_status={confirmed_status or 'unknown'}"
                )

    def _handle_stale_exit(
        self,
        local_order_id: str,
        status: str,
        contract: str,
        age_secs: float,
        position_id: Optional[str],
        reason: str,
    ):
        self._emit_order_event(
            local_order_id=local_order_id,
            stage="order_monitor",
            decision="ESCALATE",
            reason_code="STALE_EXIT_TIMEOUT",
            explanation=reason,
            contract=contract,
            position_id=position_id,
            inputs={"status": status, "age_secs": age_secs},
            thresholds={
                "timeout_exit_pending": TIMEOUT_EXIT_PENDING,
                "timeout_exit_ack": TIMEOUT_EXIT_ACK,
            },
        )

        log.error(
            f"[{self.client_id}] STALE EXIT | {contract} | {local_order_id} "
            f"| status={status} | pos={position_id} | {reason}"
        )
        self._alert(
            f"STALE EXIT ORDER — ACCOUNT RISK | {self.client_id} | {contract} "
            f"| {local_order_id} | pos={position_id} | {reason}"
        )

        if not ORDER_MONITOR_CAN_ACT:
            self._alert(
                f"[WATCHDOG ONLY] Stale exit detected; no cancel/clear/reopen attempted | "
                f"{self.client_id} | {contract} | {local_order_id} | pos={position_id} | {reason}"
            )
            log.warning(
                "[%s] WATCHDOG MODE — stale exit action suppressed | order=%s status=%s pos=%s",
                self.client_id, local_order_id, status, position_id,
            )
            return

        broker_oid = self._get_broker_order_id(local_order_id)
        broker_status = self._query_broker_order(broker_oid)

        if self._is_executed_status(broker_status):
            log.info(
                f"[{self.client_id}] Exit actually filled at broker — "
                f"advancing state machine: {local_order_id}"
            )
            self._advance_from_broker_status(local_order_id, broker_status, contract)
            return

        cancel_result = None
        try:
            cancel_result = self._cancel_broker_order(broker_oid)
        except Exception as e:
            log.warning(f"[{self.client_id}] Exit broker cancel failed: {e}")

        confirmed_status = self._extract_broker_status(cancel_result)
        is_confirmed_canceled = self._is_terminal_cancel_status(confirmed_status)

        if not is_confirmed_canceled:
            self._emit_order_event(
                local_order_id=local_order_id,
                stage="order_monitor",
                decision="ALERT",
                reason_code="MANUAL_INTERVENTION_REQUIRED",
                explanation="Exit order stuck and cancel not broker-confirmed",
                contract=contract,
                position_id=position_id,
                inputs={"confirmed_status": confirmed_status},
            )
            self._alert(
                f"Exit cancel sent but NOT broker-confirmed | {self.client_id} | "
                f"{contract} | {local_order_id} | broker_status={confirmed_status or 'unknown'}"
            )

        if is_confirmed_canceled:
            self.osm.transition(local_order_id, "CANCELED", last_error=reason)

            if position_id and self.exit_engine:
                try:
                    self.exit_engine.clear_exit_in_flight(position_id)
                    log.warning(
                        "[%s] clear_exit_in_flight(%s) called — "
                        "exit engine will retry within 8s",
                        self.client_id, position_id,
                    )
                except Exception as _cef:
                    log.error(
                        "[%s] clear_exit_in_flight failed for pos=%s: %s",
                        self.client_id, position_id, _cef,
                    )

            if position_id and self.pm:
                self._guarded_revert_position_open_after_exit_cancel(
                    position_id=position_id,
                    canceled_exit_order_id=local_order_id,
                    contract=contract,
                    reason=reason,
                )
        else:
            log.error(
                f"[{self.client_id}] Exit order could not be canceled — "
                f"MANUAL INTERVENTION REQUIRED | {local_order_id}"
            )
            self._emit_order_event(
                local_order_id=local_order_id,
                stage="order_monitor",
                decision="ALERT",
                reason_code="MANUAL_INTERVENTION_REQUIRED",
                explanation="Exit order stuck — cancel not broker-confirmed, manual action required",
                contract=contract,
                position_id=position_id,
                inputs={"confirmed_status": confirmed_status, "age_secs": age_secs},
            )
            self._alert(
                f"MANUAL INTERVENTION REQUIRED | {self.client_id} | {contract} "
                f"| Exit order {local_order_id} stuck and cannot be canceled"
            )

    def _guarded_revert_position_open_after_exit_cancel(
        self,
        *,
        position_id: str,
        canceled_exit_order_id: str,
        contract: str,
        reason: str,
    ) -> bool:
        """Safely reopen a position after a stale EXIT cancel is broker-confirmed."""
        if not ALLOW_ORDER_MONITOR_POSITION_REOPEN:
            log.warning(
                "[%s] Position reopen skipped — ALLOW_ORDER_MONITOR_POSITION_REOPEN=0 | pos=%s old_exit=%s",
                self.client_id, position_id, canceled_exit_order_id,
            )
            self._emit_order_event(
                local_order_id=canceled_exit_order_id,
                stage="order_monitor",
                decision="BLOCK",
                reason_code="POSITION_REOPEN_DISABLED",
                explanation="Order monitor position reopen is disabled by configuration",
                contract=contract,
                position_id=position_id,
                inputs={"allow_position_reopen": ALLOW_ORDER_MONITOR_POSITION_REOPEN},
            )
            return False

        try:
            replacement = self._get_newer_active_exit_order(
                position_id=position_id,
                canceled_exit_order_id=canceled_exit_order_id,
            )
            if replacement:
                repl_id = replacement.get("local_order_id")
                repl_status = replacement.get("status")
                log.warning(
                    "[%s] Position reopen skipped after stale exit cancel — newer active "
                    "replacement exit exists | pos=%s old_exit=%s new_exit=%s status=%s",
                    self.client_id, position_id, canceled_exit_order_id, repl_id, repl_status,
                )
                self._emit_order_event(
                    local_order_id=canceled_exit_order_id,
                    stage="order_monitor",
                    decision="BLOCK",
                    reason_code="POSITION_REOPEN_SKIPPED_REPLACEMENT_EXIT_ACTIVE",
                    explanation=(
                        "Skipped reverting position to OPEN after stale exit cancel because "
                        "a newer active replacement exit already exists"
                    ),
                    contract=contract,
                    position_id=position_id,
                    inputs={
                        "canceled_exit_order_id": canceled_exit_order_id,
                        "replacement_exit_order_id": repl_id,
                        "replacement_status": repl_status,
                    },
                )
                return False

            for method_name in (
                "revert_position_to_open",
                "revertpositiontoopen",
                "revert_to_open_after_exit_cancel",
            ):
                method = getattr(self.pm, method_name, None)
                if not callable(method):
                    continue
                try:
                    ok = method(
                        position_id,
                        canceled_exit_order_id=canceled_exit_order_id,
                        reason=reason,
                    )
                except TypeError:
                    try:
                        ok = method(position_id, reason=reason)
                    except TypeError:
                        ok = method(position_id)
                if ok is False:
                    log.warning(
                        "[%s] %s declined position reopen | pos=%s old_exit=%s",
                        self.client_id, method_name, position_id, canceled_exit_order_id,
                    )
                    return False
                log.warning(
                    "[%s] Position reverted to OPEN via %s after broker-confirmed "
                    "exit cancel — RETRY EXIT REQUIRED | pos=%s old_exit=%s",
                    self.client_id, method_name, position_id, canceled_exit_order_id,
                )
                self._alert(
                    f"Position reverted to OPEN — exit must be retried | "
                    f"{self.client_id} | {contract} | pos={position_id}"
                )
                return True

            self.pm.update_position(position_id, status="OPEN")
            log.warning(
                "[%s] Position reverted to OPEN after broker-confirmed exit cancel "
                "using guarded update_position fallback — RETRY EXIT REQUIRED | pos=%s old_exit=%s",
                self.client_id, position_id, canceled_exit_order_id,
            )
            self._alert(
                f"Position reverted to OPEN — exit must be retried | "
                f"{self.client_id} | {contract} | pos={position_id}"
            )
            return True
        except Exception as e:
            log.error(
                "[%s] Failed guarded position reopen after exit cancel | pos=%s old_exit=%s: %s",
                self.client_id, position_id, canceled_exit_order_id, e,
            )
            return False

    def _get_newer_active_exit_order(
        self,
        *,
        position_id: str,
        canceled_exit_order_id: str,
    ) -> Optional[dict]:
        """Return a newer active replacement EXIT order for this position, if any."""
        if not position_id or not canceled_exit_order_id:
            return None

        def _fn():
            with conn() as c:
                c.execute(
                    """
                    WITH canceled AS (
                        SELECT created_ts
                        FROM orders
                        WHERE client_id=%s
                          AND local_order_id=%s
                          AND kind='EXIT'
                        LIMIT 1
                    )
                    SELECT local_order_id, broker_order_id, status, created_ts, submitted_ts
                    FROM orders
                    WHERE client_id=%s
                      AND position_id=%s
                      AND kind='EXIT'
                      AND local_order_id<>%s
                      AND status IN (
                          'EXIT_REQUESTED','EXIT_SUBMITTED',
                          'EXIT_ACKNOWLEDGED','EXIT_PARTIAL_FILL'
                      )
                      AND (
                          (SELECT created_ts FROM canceled) IS NULL
                          OR created_ts >= (SELECT created_ts FROM canceled)
                      )
                    ORDER BY created_ts DESC
                    LIMIT 1
                    """,
                    (
                        self.client_id, canceled_exit_order_id,
                        self.client_id, position_id, canceled_exit_order_id,
                    ),
                )
                return c.fetchone()

        try:
            row = run_with_retry(_fn)
            return dict(row) if row else None
        except Exception as e:
            log.error(
                "[%s] Failed checking newer replacement exit | pos=%s old_exit=%s: %s — "
                "assuming no replacement; proceeding with position reopen",
                self.client_id, position_id, canceled_exit_order_id, e,
            )
            return None  # safe: caller proceeds with position reopen on None

    def _query_broker_order(self, broker_order_id: Optional[str]) -> Optional[str]:
        if not broker_order_id or not self.broker:
            return None
        def _fetch():
            try:
                if hasattr(self.broker, "get_order"):
                    result = self.broker.get_order(broker_order_id)
                    if isinstance(result, dict):
                        return str(
                            result.get("status") or result.get("order_status") or ""
                        ).lower()
                if hasattr(self.broker, "order_status"):
                    result = self.broker.order_status(broker_order_id)
                    return str(result).lower() if result else None
            except Exception as e:
                log.debug(f"[{self.client_id}] Broker order query failed: {e}")
                # PR #30 LIVE-SAFETY: feed status-query failures into the
                # circuit breaker for this client. Threshold-burst opens
                # the breaker and blocks new entries (exits unaffected).
                try:
                    from ap import safety_circuit as _sc
                    _sc.record_broker_error(self.client_id, e, op_kind="status")
                except Exception:
                    pass
            return None
        return _cached_broker_order_status(broker_order_id, _fetch)

    def _cancel_broker_order(self, broker_order_id: Optional[str]):
        if not broker_order_id or not self.broker:
            return None
        try:
            result = None
            if hasattr(self.broker, "cancel_order"):
                result = self.broker.cancel_order(broker_order_id)
                log.info(f"[{self.client_id}] Broker cancel requested: {broker_order_id} → {result}")

            if isinstance(result, dict):
                status = str(result.get("status") or result.get("order_status") or "").strip().lower()
                if status:
                    return result

            if isinstance(result, str) and result.strip():
                return result.strip().lower()

            queried = self._query_broker_order(broker_order_id)
            if queried:
                return queried

        except Exception as e:
            log.warning(f"[{self.client_id}] Broker cancel error: {e}")
            # PR #30 LIVE-SAFETY: feed cancel failures into the circuit
            # breaker. Threshold-burst opens the breaker.
            try:
                from ap import safety_circuit as _sc
                _sc.record_broker_error(self.client_id, e, op_kind="cancel")
            except Exception:
                pass
        return None

    def _normalize_broker_status(self, raw_status) -> str:
        if raw_status is None:
            return ""
        if isinstance(raw_status, dict):
            raw_status = raw_status.get("status") or raw_status.get("order_status") or ""
        s = str(raw_status).strip().lower()
        if not s:
            return ""
        aliases = {
            "cancelled": "canceled",
            "partial_fill": "partially_filled",
            "partial_filled": "partially_filled",
        }
        return aliases.get(s, s)

    def _is_terminal_cancel_status(self, raw_status) -> bool:
        return self._normalize_broker_status(raw_status) in {"canceled", "expired"}

    def _is_terminal_failure_status(self, raw_status) -> bool:
        return self._normalize_broker_status(raw_status) in {
            "canceled",
            "expired",
            "rejected",
        }

    def _is_filled_status(self, raw_status) -> bool:
        return self._normalize_broker_status(raw_status) == "filled"

    def _is_executed_status(self, raw_status) -> bool:
        return self._normalize_broker_status(raw_status) in {"filled", "partially_filled"}

    def _extract_broker_status(self, raw_result) -> str:
        return self._normalize_broker_status(raw_result)

    def _advance_from_broker_status(self, local_order_id: str, broker_status, contract: str):
        s = self._normalize_broker_status(broker_status)
        order = self.osm.get_order(local_order_id) or {}
        kind = str(order.get("kind") or "").upper()
        if kind == "EXIT":
            mapping = {
                "filled": "EXIT_FILLED",
                "partially_filled": "EXIT_PARTIAL_FILL",
                "canceled": "CANCELED",
                "rejected": "REJECTED",
                "expired": "EXPIRED",
                "pending": "EXIT_SUBMITTED",
                "open": "EXIT_ACKNOWLEDGED",
            }
        else:
            mapping = {
                "filled": "FILLED",
                "partially_filled": "PARTIAL_FILL",
                "canceled": "CANCELED",
                "rejected": "REJECTED",
                "expired": "EXPIRED",
                "pending": "SUBMITTED",
                "open": "ACKNOWLEDGED",
            }
        new_status = mapping.get(s)
        if not new_status:
            log.debug(f"[{self.client_id}] Unknown broker status '{s}' — no transition")
            return
        kwargs = {}
        if s in {"filled", "partially_filled"}:
            fill_status = (
                "EXIT_FILLED" if kind == "EXIT" and s == "filled" else
                "EXIT_PARTIAL_FILL" if kind == "EXIT" else
                "FILLED" if s == "filled" else
                "PARTIAL_FILL"
            )
            log.warning(
                "[%s] BROKER_FILL_SEEN_DEFER_TO_FILL_MONITOR | local=%s contract=%s kind=%s broker_status=%s blocked_status=%s",
                self.client_id,
                local_order_id,
                contract,
                kind or "ENTRY",
                s,
                fill_status,
            )
            self._emit_order_event(
                local_order_id=local_order_id,
                stage="order_monitor",
                decision="ALERT",
                reason_code="BROKER_FILL_SEEN_DEFER_TO_FILL_MONITOR",
                explanation=(
                    "Broker reported fill/partial fill, but order_monitor only has "
                    "status-level data. Fill monitor owns fill finalization because it "
                    "hydrates filled_qty/avg_fill and creates/syncs positions."
                ),
                contract=contract,
                position_id=(order or {}).get("position_id"),
                inputs={
                    "broker_status": s,
                    "kind": kind or "ENTRY",
                    "blocked_status": fill_status,
                    "requires_fill_monitor": True,
                },
            )
            return
        ok = self.osm.transition(local_order_id, new_status, **kwargs)
        if ok:
            log.info(f"[{self.client_id}] Advanced | {contract} | {local_order_id} → {new_status}")
            self._emit_order_event(
                local_order_id=local_order_id,
                stage="order_monitor",
                decision="FILL" if "FILLED" in new_status else "ACKNOWLEDGE",
                reason_code=(
                    "BROKER_REJECTED" if new_status == "REJECTED" else
                    "BROKER_CANCELED" if new_status == "CANCELED" else
                    "BROKER_EXPIRED" if new_status == "EXPIRED" else None
                ),
                explanation=f"Broker status {s} advanced local order to {new_status}",
                contract=contract,
                position_id=(order or {}).get("position_id"),
                inputs={"broker_status": s, "kind": kind, "new_status": new_status},
            )

        if ok and kind == "EXIT" and new_status == "EXIT_FILLED":
            _pos_id = (order or {}).get("position_id")
            _avg_fill = (order or {}).get("avg_fill")
            if _avg_fill is None:
                _avg_fill = (order or {}).get("fill_price")
            _entry = (order or {}).get("entry_price")
            if _entry is None and _pos_id and self.pm:
                try:
                    _pos = self.pm.get_position(_pos_id) or {}
                    _entry = _pos.get("entry_option_price") or _pos.get("entry_price")
                except Exception:
                    _entry = None
            _tick = (order or {}).get("ticker") or contract
            if _pos_id and _avg_fill is not None:
                try:
                    from ap.exit_price_sync import sync_exit_price_to_dashboard
                    sync_exit_price_to_dashboard(
                        position_id=_pos_id,
                        exit_avg_fill=float(_avg_fill),
                        entry_price=float(_entry) if _entry is not None else None,
                        ticker=_tick,
                    )
                except Exception as _esp:
                    log.debug("[%s] exit_price_sync failed (non-critical): %s", self.client_id, _esp)

        if ok and kind == "EXIT" and s in {"canceled", "expired", "rejected"}:
            _pos_id = (order or {}).get("position_id")
            if _pos_id and self.exit_engine:
                try:
                    self.exit_engine.clear_exit_in_flight(_pos_id)
                    log.warning(
                        "[%s] clear_exit_in_flight(%s) from broker status=%s — "
                        "exit engine will retry",
                        self.client_id, _pos_id, s,
                    )
                except Exception as _e2:
                    log.error(
                        "[%s] clear_exit_in_flight failed for pos=%s status=%s: %s",
                        self.client_id, _pos_id, s, _e2,
                    )
            # Revert position to OPEN so the exit engine can re-submit.
            # Without this, the position stays CLOSING and the exit engine
            # never re-submits even after clearing in-flight.
            if _pos_id and self.pm:
                try:
                    self._guarded_revert_position_open_after_exit_cancel(
                        position_id=_pos_id,
                        canceled_exit_order_id=local_order_id,
                        contract=contract,
                        reason=f"broker_terminal_status={s}",
                    )
                except Exception as _e3:
                    log.error(
                        "[%s] _guarded_revert failed after broker terminal exit status=%s pos=%s: %s",
                        self.client_id, s, _pos_id, _e3,
                    )

    def _get_broker_order_id(self, local_order_id: str) -> Optional[str]:
        try:
            order = self.osm.get_order(local_order_id)
            return (order or {}).get("broker_order_id")
        except Exception:
            return None

    def _get_active_entry_orders(self) -> list[dict]:
        def _fn():
            with conn() as c:
                c.execute(
                    """
                    SELECT local_order_id, broker_order_id, status, symbol,
                           contract, position_id, signal_id, plan_id,
                           created_ts, submitted_ts,
                           qty, direction, execution_mode, reserved_cost,
                           limit_price,
                           limit_price AS price,
                           fill_price,
                           -- PR fix/entry-retry-context-and-submit-refresh:
                           -- Include score/tier/trigger_price/meta so the A+/normal
                           -- tier decision in _check_entry_age_and_cancel uses real
                           -- row values instead of always falling back to 0/normal.
                           -- Previously the SELECT omitted these and order.get("score")
                           -- returned None → _score=0 → every order treated as normal
                           -- tier (90s) even when score=70 (A/B tier) qualified for the
                           -- longer window. This was the source of "score=0.0 tier=normal"
                           -- log noise on rows that actually had score=70 tier=B.
                           score, tier, trigger_price, stop_underlying,
                           target_underlying, meta
                    FROM orders
                    WHERE client_id=%s
                      AND kind='ENTRY'
                      AND status IN ('CREATED','PENDING_TRIGGER','SUBMITTED','ACKNOWLEDGED','PARTIAL_FILL')
                    ORDER BY created_ts ASC
                    """,
                    (self.client_id,),
                )
                return c.fetchall()
        try:
            return run_with_retry(_fn)
        except Exception as e:
            log.error(f"[{self.client_id}] Failed to fetch entry orders: {e}")
            return []

    def _get_active_exit_orders(self) -> list[dict]:
        def _fn():
            with conn() as c:
                c.execute(
                    """
                    SELECT local_order_id, broker_order_id, status, symbol,
                           contract, position_id, created_ts, submitted_ts,
                           fill_price,
                           fill_price AS avg_fill,
                           limit_price AS entry_price,
                           symbol AS ticker
                    FROM orders
                    WHERE client_id=%s
                      AND kind='EXIT'
                      AND status IN (
                          'EXIT_REQUESTED','EXIT_SUBMITTED',
                          'EXIT_ACKNOWLEDGED','EXIT_PARTIAL_FILL'
                      )
                    ORDER BY created_ts ASC
                    """,
                    (self.client_id,),
                )
                return c.fetchall()
        try:
            return run_with_retry(_fn)
        except Exception as e:
            log.error(f"[{self.client_id}] Failed to fetch exit orders: {e}")
            return []

    def _parse_ts(self, ts) -> Optional[datetime]:
        if ts is None:
            return None
        if isinstance(ts, datetime):
            return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
        try:
            dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None

    # ─────────────────────────────────────────────────────────────────────
    # P1 ENTRY FIX (2026-05-21): systemic LOST_HANDOFF rate guard.
    #
    # If 3+ LOST_HANDOFF_30S events fire within a 5-minute rolling window for
    # the same client_id, the system has a structural handoff failure (watcher
    # loop crashed, OSM submit-path broken, etc). Continuing to arm new entries
    # will produce the same outcome. Halt new entry arms for this client until
    # an operator clears the flag, and emit a CRITICAL audit event.
    #
    # The flag lives in a process-local set; on restart it's cleared. The
    # systemic event is also written to client_state.context_notes so the
    # dashboard surfaces it. One self-heal attempt is allowed per event window
    # (we DON'T loop forever).
    # ─────────────────────────────────────────────────────────────────────
    _LOST_HANDOFF_WINDOW_SEC      = 300  # 5 minutes
    _LOST_HANDOFF_SYSTEMIC_THRESH = 3

    def _record_lost_handoff_and_check_systemic(self) -> None:
        import time as _t
        # Lazy per-instance ring buffer of recent LOST_HANDOFF timestamps.
        ring = getattr(self, "_lost_handoff_ring", None)
        if ring is None:
            ring = []
            self._lost_handoff_ring = ring
        now = _t.time()
        ring.append(now)
        # Prune entries older than the window.
        cutoff = now - self._LOST_HANDOFF_WINDOW_SEC
        ring[:] = [t for t in ring if t >= cutoff]
        if len(ring) < self._LOST_HANDOFF_SYSTEMIC_THRESH:
            return

        # Systemic event. Don't re-fire if already halted (one-shot per window).
        if getattr(self, "_lost_handoff_systemic_active", False):
            return
        self._lost_handoff_systemic_active = True

        log.critical(
            "[%s] LOST_HANDOFF_SYSTEMIC | %d LOST_HANDOFF_30S events in %ds window — "
            "halting new entry arms; check entry-watcher loop and OSM submit path",
            self.client_id, len(ring), self._LOST_HANDOFF_WINDOW_SEC,
        )

        # Best-effort: write halt flag to client_state so the entry gate sees it.
        try:
            from ap.db import update_client_state
            update_client_state(
                self.client_id,
                lost_handoff_systemic_halt=True,
                context_notes=f"LOST_HANDOFF_SYSTEMIC at {now:.0f} — {len(ring)} in {self._LOST_HANDOFF_WINDOW_SEC}s",
            )
        except Exception as e:
            log.warning("[%s] could not persist LOST_HANDOFF_SYSTEMIC flag: %s", self.client_id, e)

        # Best-effort: emit a structured decision_event for the dashboard.
        try:
            from ap.db import insert_decision_event
            insert_decision_event(
                client_id=self.client_id,
                stage="entry_watch",
                decision="BLOCK",
                reason_code="LOST_HANDOFF_SYSTEMIC",
                explanation=(
                    f"{len(ring)} LOST_HANDOFF_30S events in "
                    f"{self._LOST_HANDOFF_WINDOW_SEC}s window. New entries halted."
                ),
                ctx={"event_count": len(ring),
                     "window_sec":  self._LOST_HANDOFF_WINDOW_SEC},
            )
        except Exception as e:
            log.debug("[%s] could not insert decision_event: %s", self.client_id, e)

    def _get_underlying_symbol_from_contract(self, contract_symbol: str) -> Optional[str]:
        """Extract the underlying ticker from an OCC option contract symbol.
        e.g. 'MSFT260522C00427500' -> 'MSFT'.  Falls back to None on parse failure."""
        if not contract_symbol:
            return None
        s = str(contract_symbol).strip().upper()
        # OCC: <ROOT>[1-6 chars]<YYMMDD><C|P><STRIKE*1000 padded to 8>
        # Strike block is 8 digits at the end. Date is the 6 digits before C/P.
        # Simpler heuristic: take chars before the first digit.
        out = []
        for ch in s:
            if ch.isdigit():
                break
            out.append(ch)
        root = "".join(out)
        return root or None

    def _paper_entry_retry_and_fallback(
        self,
        *,
        order: dict,
        local_id: str,
        broker_oid: str,
        contract: str,
        sym: str,
        score: float,
        tier_str: str,
        is_aplus: bool,
        age_secs: float,
        order_meta: dict,
    ) -> None:
        """PR #68 — PAPER-mode entry retry ladder + market fallback.

        Strict scope: invoked only when client_mode='PAPER' and the order
        has a broker_order_id. LIVE mode never reaches this helper.

        Ladder semantics (env-overridable):
          - PAPER_ENTRY_REPEG_SECONDS    = '10,25,40' — reprice checkpoints
          - ENTRY_PAPER_ASK_CROSS_CENTS  = 0.05      — added to ask at reprice
          - PAPER_ENTRY_MARKET_FALLBACK_AFTER_SECONDS = 45 — market fallback
          - PAPER_ENTRY_MARKET_FALLBACK_ENABLED       = true (default)
          - PAPER_ENTRY_MARKET_FALLBACK_MAX_SPREAD_PCT = 0.25 (safety)

        We track per-order how many reprices have fired in meta so we don't
        thrash on every monitor tick:
          meta.paper_entry_retry_enabled  bool
          meta.reprice_attempt_count       int
          meta.last_reprice_age_seconds    float (seconds at last reprice)
          meta.market_fallback_used        bool
          meta.marketable_unfilled_seen    bool

        On every fire we persist:
          old_limit_price / new_limit_price (or 'MARKET')
          current_bid / current_ask / current_mid / current_last
          spread_pct, final_cancel_reason
        plus the existing PR-A submit-time quote fields stay untouched.

        Logs (structured, one per event):
          PAPER_ENTRY_MARKETABLE_UNFILLED
          PAPER_ENTRY_REPEG_ATTEMPT
          PAPER_ENTRY_REPEG_OK
          PAPER_ENTRY_REPEG_FAILED
          PAPER_ENTRY_MARKET_FALLBACK
          PAPER_ENTRY_CANCEL_AFTER_RETRIES (set by caller after this returns)
        """
        # Defensive: refuse to run if anything indicates LIVE.
        if self.client_mode != "PAPER":
            return

        ladder = PAPER_ENTRY_REPEG_LADDER_SECONDS or [10, 25, 40]
        attempts_done = int(order_meta.get("reprice_attempt_count") or 0)
        market_used   = bool(order_meta.get("market_fallback_used") or False)

        # Already used market fallback — nothing more to do, let age win.
        if market_used:
            return

        # Determine which step we're at. Each ladder rung fires at most once.
        next_rung_idx = attempts_done
        rung_age = ladder[next_rung_idx] if next_rung_idx < len(ladder) else None

        # Fetch fresh option quote (single call, used for ladder + fallback).
        live_bid = live_ask = live_mid = live_last = None
        spread_pct = None
        try:
            _qb = self._quote_broker()
            q = _qb.get_quote(contract) if hasattr(_qb, "get_quote") else None
            if isinstance(q, dict):
                _b = q.get("bid"); _a = q.get("ask"); _l = q.get("last")
                try: live_bid = float(_b) if _b is not None else None
                except (TypeError, ValueError): pass
                try: live_ask = float(_a) if _a is not None else None
                except (TypeError, ValueError): pass
                try: live_last = float(_l) if _l is not None else None
                except (TypeError, ValueError): pass
                if live_bid is not None and live_ask is not None and live_bid > 0 and live_ask > 0:
                    live_mid = round((live_bid + live_ask) / 2.0, 4)
                    spread_pct = (live_ask - live_bid) / live_mid if live_mid else None
        except Exception as _e:
            log.debug("[%s] paper_retry quote fetch failed for %s: %s",
                      self.client_id, contract, _e)

        # Marketable-unfilled detection (audit only — doesn't block anything).
        try:
            current_limit = float(order.get("limit_price") or 0)
        except (TypeError, ValueError):
            current_limit = 0.0
        is_marketable = (
            current_limit > 0 and live_ask is not None and live_ask > 0
            and current_limit >= live_ask
        )
        if is_marketable and not order_meta.get("marketable_unfilled_seen"):
            log.info(
                "[%s] PAPER_ENTRY_MARKETABLE_UNFILLED | %s contract=%s "
                "limit=%.2f ask=%.2f age=%.0fs",
                self.client_id, sym, contract, current_limit, live_ask, age_secs,
            )

        # ----- Reprice ladder -----
        if rung_age is not None and age_secs >= rung_age and next_rung_idx < len(ladder):
            log.info(
                "[%s] PAPER_ENTRY_REPEG_ATTEMPT | %s contract=%s attempt=%d/%d "
                "age=%.0fs limit=%.2f bid=%s ask=%s",
                self.client_id, sym, contract,
                next_rung_idx + 1, len(ladder), age_secs, current_limit,
                live_bid, live_ask,
            )
            # Sanity: need a usable ask to reprice at.
            if live_ask is None or live_ask <= 0:
                log.warning(
                    "[%s] PAPER_ENTRY_REPEG_FAILED | %s no_quote contract=%s",
                    self.client_id, sym, contract,
                )
                self._stamp_paper_retry_meta(
                    local_id,
                    {
                        "paper_entry_retry_enabled": True,
                        "reprice_attempt_count":     attempts_done + 1,
                        "last_reprice_age_seconds":  float(age_secs),
                        "last_reprice_outcome":      "no_quote",
                        "marketable_unfilled_seen":  bool(is_marketable),
                        "current_bid":   live_bid,
                        "current_ask":   live_ask,
                        "current_mid":   live_mid,
                        "current_last": live_last,
                        "spread_pct":    spread_pct,
                    },
                )
                return
            new_limit = round(live_ask + ENTRY_PAPER_ASK_CROSS_CENTS, 2)
            ok, outcome = self._paper_replace_at_new_limit(
                order=order, broker_oid=broker_oid, contract=contract,
                sym=sym, new_limit=new_limit,
            )
            if ok:
                log.info(
                    "[%s] PAPER_ENTRY_REPEG_OK | %s contract=%s new_limit=%.2f",
                    self.client_id, sym, contract, new_limit,
                )
            else:
                log.warning(
                    "[%s] PAPER_ENTRY_REPEG_FAILED | %s contract=%s new_limit=%.2f outcome=%s",
                    self.client_id, sym, contract, new_limit, outcome,
                )
            _meta_patch = {
                "paper_entry_retry_enabled":  True,
                "reprice_attempt_count":      attempts_done + 1,
                "last_reprice_age_seconds":   float(age_secs),
                "last_reprice_outcome":       outcome,
                "marketable_unfilled_seen":   bool(is_marketable),
                "old_limit_price":            current_limit,
                "new_limit_price":            new_limit,
                "attempted_new_limit":        new_limit,
                "current_bid":                live_bid,
                "current_ask":                live_ask,
                "current_mid":                live_mid,
                "current_last":               live_last,
                "spread_pct":                 spread_pct,
            }
            # If the cancel succeeded but the replacement did NOT, surface
            # that as a top-level meta flag so the dashboard / post-mortem
            # never assumes the order is still open. Acceptance criteria
            # require the canonical outcome string + the canceled broker_order_id
            # + the attempted_new_limit so the audit trail is unambiguous.
            if outcome.startswith("replace_failed_after_cancel") or outcome == "replace_bad_response_after_cancel":
                _meta_patch["replace_failed_after_cancel"] = True
                _meta_patch["final_cancel_reason"] = outcome
                # Normalize the canonical outcome key per PR #69 acceptance.
                # Granular subtype is preserved in final_cancel_reason above.
                _meta_patch["last_reprice_outcome"] = "replace_failed_after_cancel"
                _meta_patch["canceled_broker_order_id"] = broker_oid
            self._stamp_paper_retry_meta(local_id, _meta_patch)
            return

        # ----- Market fallback at 45s -----
        if (
            PAPER_ENTRY_MARKET_FALLBACK_ENABLED
            and age_secs >= PAPER_ENTRY_MARKET_FALLBACK_AFTER_SECONDS
        ):
            # Sanity gates: valid quote + sane spread.
            if live_ask is None or live_ask <= 0 or live_bid is None or live_bid <= 0:
                log.warning(
                    "[%s] PAPER_ENTRY_MARKET_FALLBACK skipped — missing quote "
                    "%s contract=%s bid=%s ask=%s",
                    self.client_id, sym, contract, live_bid, live_ask,
                )
                return
            if spread_pct is not None and spread_pct > PAPER_ENTRY_MARKET_FALLBACK_MAX_SPREAD_PCT:
                log.warning(
                    "[%s] PAPER_ENTRY_MARKET_FALLBACK skipped — spread %.3f > %.3f "
                    "%s contract=%s",
                    self.client_id, spread_pct,
                    PAPER_ENTRY_MARKET_FALLBACK_MAX_SPREAD_PCT,
                    sym, contract,
                )
                return

            log.warning(
                "[%s] PAPER_ENTRY_MARKET_FALLBACK | %s contract=%s age=%.0fs "
                "bid=%.2f ask=%.2f spread_pct=%.3f",
                self.client_id, sym, contract, age_secs,
                live_bid, live_ask, spread_pct or 0.0,
            )
            ok, outcome = self._paper_replace_to_market(
                order=order, broker_oid=broker_oid, contract=contract, sym=sym,
            )
            _meta_patch = {
                "paper_entry_retry_enabled":   True,
                "market_fallback_used":        True,
                "market_fallback_outcome":     outcome,
                "market_fallback_age_seconds": float(age_secs),
                "marketable_unfilled_seen":    bool(is_marketable),
                "old_limit_price":             current_limit,
                "new_limit_price":             "MARKET",
                "attempted_market_fallback":   True,
                "current_bid":                 live_bid,
                "current_ask":                 live_ask,
                "current_mid":                 live_mid,
                "current_last":                live_last,
                "spread_pct":                  spread_pct,
            }
            if outcome.startswith("replace_failed_after_cancel") or outcome == "replace_bad_response_after_cancel":
                _meta_patch["replace_failed_after_cancel"] = True
                _meta_patch["final_cancel_reason"] = outcome
                # Canonical outcome key per PR #69 acceptance.
                _meta_patch["last_reprice_outcome"] = "replace_failed_after_cancel"
                _meta_patch["canceled_broker_order_id"] = broker_oid
            self._stamp_paper_retry_meta(local_id, _meta_patch)

    # ------------------------------------------------------------------
    # PR #68 + Codex P1 patch (2026-06-01):
    # The canonical broker submit method across this codebase is
    # broker.place_order(...) — NOT submit_option. Used by
    # ap/execution.py:749, ap/exit_manager.py:205, ap/retry_engine.py:360,
    # and the Tradier adapter at ap/brokers/tradier.py:179. The previous
    # PR-69 draft called a non-existent submit_option, which would have
    # cancelled the open broker order and then raised AttributeError on
    # the replacement, leaving the local row pointing at a cancelled
    # broker_order_id with no live replacement.
    #
    # Both _paper_replace_* helpers below now:
    #   1. Verify hasattr(self.broker, 'place_order') BEFORE any cancel.
    #      If absent, log PAPER_ENTRY_*_UNSUPPORTED and return False with
    #      no destructive action.
    #   2. Cancel the existing broker order.
    #   3. Call broker.place_order(symbol, contract, qty, limit_price,
    #      side='buy_to_open', tag=local_oid).
    #   4. Parse the dict-or-object response the same way
    #      retry_engine.apply_repeg does (broker_order_id / order_id / id;
    #      status in ACK/ACKED/FILLED/SUBMITTED/OK/ACCEPTED/PENDING/OPEN).
    #   5. On success: write update_order with the new broker_order_id
    #      and limit_price (or keep limit_price for the market path).
    #   6. On post-cancel submit failure: write update_order with
    #      status=CANCELED + last_error="replace_failed_after_cancel:<...>"
    #      AND have the caller stamp meta.replace_failed_after_cancel=True
    #      so it never lies that the order is still open.
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_broker_place_response(resp) -> tuple[Optional[str], str, Optional[str]]:
        """Extract (broker_order_id, status_upper, error) from a place_order
        response. Mirrors retry_engine.apply_repeg parsing exactly."""
        if isinstance(resp, dict):
            oid    = resp.get("broker_order_id") or resp.get("order_id") or resp.get("id")
            status = str(resp.get("status") or "").upper()
            error  = resp.get("error")
        else:
            oid    = getattr(resp, "broker_order_id", None) or getattr(resp, "order_id", None)
            status = str(getattr(resp, "status", "") or "").upper()
            error  = getattr(resp, "error", None)
        return (str(oid) if oid else None), status, (str(error) if error else None)

    @staticmethod
    def _broker_place_ok(status: str) -> bool:
        return status in ("ACK", "ACKED", "FILLED", "SUBMITTED", "OK",
                          "ACCEPTED", "PENDING", "OPEN")

    def _paper_replace(
        self,
        *,
        order: dict,
        broker_oid: str,
        contract: str,
        sym: str,
        new_limit: Optional[float],   # None => market
        unsupported_log_tag: str,     # e.g. 'PAPER_ENTRY_REPEG' / 'PAPER_ENTRY_MARKET_FALLBACK'
    ) -> tuple[bool, str]:
        """Cancel + replace a broker order. Returns (ok, outcome_str).

        outcome_str values (for meta stamping by caller):
          'replaced_ok'
          'broker_missing_place_order'        — NO cancel performed
          'invalid_qty'                       — NO cancel performed
          'cancel_failed'                     — NO replacement submitted
          'replace_failed_after_cancel'       — cancel succeeded, replace did NOT
          'replace_failed_after_cancel_exception'
          'replace_bad_response_after_cancel'
        """
        local_oid = order.get("local_order_id") or order.get("id")

        # 1) Pre-flight: never cancel if we can't replace.
        if not hasattr(self.broker, "place_order"):
            log.error(
                "[%s] %s_UNSUPPORTED | broker %s has no place_order — "
                "skipping replace; original order remains.",
                self.client_id, unsupported_log_tag,
                type(self.broker).__name__,
            )
            return False, "broker_missing_place_order"

        # 2) Pre-flight: build the replacement request.
        qty = order.get("qty") or order.get("quantity") or 0
        try:
            qty = int(qty)
        except (TypeError, ValueError):
            qty = 0
        if qty <= 0:
            log.warning("[%s] %s_UNSUPPORTED | invalid qty=%r local=%s",
                        self.client_id, unsupported_log_tag, qty, local_oid)
            return False, "invalid_qty"

        # 3) Cancel the existing broker order.
        try:
            cancel_resp = self.broker.cancel_order(broker_oid)
        except Exception as e:
            log.warning(
                "[%s] %s | cancel raised broker=%s err=%s — replacement aborted, "
                "original order unchanged.",
                self.client_id, unsupported_log_tag, broker_oid, e,
            )
            return False, "cancel_failed"
        _cancel_err = None
        if isinstance(cancel_resp, dict):
            _cancel_err = cancel_resp.get("error")
        elif cancel_resp is None:
            _cancel_err = "no_response"
        if _cancel_err:
            log.warning(
                "[%s] %s | cancel failed broker=%s err=%s — replacement aborted, "
                "original order may still be live.",
                self.client_id, unsupported_log_tag, broker_oid, _cancel_err,
            )
            return False, "cancel_failed"

        # 4) Cancel succeeded — from here we MUST either land a replacement
        #    or transition the local row to CANCELED with a clear reason.
        try:
            resp = self.broker.place_order(
                symbol=sym,
                contract=contract,
                qty=qty,
                limit_price=(float(new_limit) if new_limit is not None else None),
                side="buy_to_open",
                tag=str(local_oid) if local_oid else None,
            )
        except Exception as e:
            log.error(
                "[%s] %s | place_order raised after cancel local=%s err=%s",
                self.client_id, unsupported_log_tag, local_oid, e,
            )
            self._mark_local_cancelled_after_failed_replace(
                local_oid, reason=f"replace_failed_after_cancel_exception:{e}",
            )
            return False, "replace_failed_after_cancel_exception"

        new_oid, status, err = self._parse_broker_place_response(resp)
        if err or not new_oid or not self._broker_place_ok(status):
            log.error(
                "[%s] %s | replace bad response after cancel local=%s "
                "new_oid=%s status=%s err=%s",
                self.client_id, unsupported_log_tag, local_oid,
                new_oid, status, err,
            )
            self._mark_local_cancelled_after_failed_replace(
                local_oid,
                reason=f"replace_bad_response_after_cancel:{err or status or 'no_oid'}",
            )
            return False, "replace_bad_response_after_cancel"

        # 5) Success — write back the new broker id + (new limit if any).
        try:
            from ap.db import update_order
            if new_limit is not None:
                update_order(
                    local_order_id=local_oid,
                    broker_order_id=new_oid,
                    limit_price=float(new_limit),
                    status="SUBMITTED",
                )
            else:
                # Market path: keep limit_price column unchanged — we don't
                # have a real limit anymore. update_order(limit_price=None)
                # would null the column on some implementations; safer to
                # omit the kwarg entirely.
                update_order(
                    local_order_id=local_oid,
                    broker_order_id=new_oid,
                    status="SUBMITTED",
                )
        except Exception as e:
            log.error(
                "[%s] %s | replace succeeded at broker but local update_order "
                "raised — broker now has %s but local still references %s. "
                "err=%s",
                self.client_id, unsupported_log_tag, new_oid, broker_oid, e,
            )
            # Replacement is live at broker. Don't mark local cancelled — the
            # reconciler will resolve on its next pass.
            return True, "replaced_ok_but_local_write_failed"

        return True, "replaced_ok"

    def _mark_local_cancelled_after_failed_replace(self, local_oid, reason: str) -> None:
        """After a successful broker cancel + failed replace, transition the
        local row to CANCELED so nothing in the system keeps treating the
        order as open. Best-effort — a failure here is logged but doesn't
        re-raise."""
        if not local_oid:
            return
        try:
            from ap.db import update_order
            update_order(
                local_order_id=local_oid,
                status="CANCELED",
                last_error=reason[:240],
            )
        except Exception as e:
            log.error(
                "[%s] failed to mark local CANCELED after failed replace "
                "local=%s reason=%s err=%s",
                self.client_id, local_oid, reason, e,
            )

    def _paper_replace_at_new_limit(
        self, *, order: dict, broker_oid: str, contract: str, sym: str,
        new_limit: float,
    ) -> tuple[bool, str]:
        """Reprice path: cancel current + place new limit. Returns (ok, outcome)."""
        return self._paper_replace(
            order=order, broker_oid=broker_oid, contract=contract, sym=sym,
            new_limit=float(new_limit),
            unsupported_log_tag="PAPER_ENTRY_REPEG",
        )

    def _paper_replace_to_market(
        self, *, order: dict, broker_oid: str, contract: str, sym: str,
    ) -> tuple[bool, str]:
        """Market fallback: cancel current + place market (limit_price=None).
        Returns (ok, outcome)."""
        return self._paper_replace(
            order=order, broker_oid=broker_oid, contract=contract, sym=sym,
            new_limit=None,
            unsupported_log_tag="PAPER_ENTRY_MARKET_FALLBACK",
        )

    def _stamp_paper_retry_meta(self, local_order_id: str, extra: dict) -> None:
        """Merge PR #68 retry fields into orders.meta without overwriting prior keys."""
        try:
            from ap.db import conn as _conn
            import json as _json
            patch = _json.dumps({k: v for k, v in (extra or {}).items()}, default=str)
            with _conn() as c:
                c.execute(
                    """
                    UPDATE orders
                    SET meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb,
                        updated_ts = NOW()
                    WHERE local_order_id = %s AND client_id = %s
                    """,
                    (patch, local_order_id, self.client_id),
                )
        except Exception as e:
            log.debug("[%s] _stamp_paper_retry_meta failed for %s: %s",
                      self.client_id, local_order_id, e)

    def _try_repeg(
        self,
        order: dict,
        local_id: str,
        contract: str,
        sym: str,
        limit_price: float,
        current_option_price: float,
        status: str,
        age_secs: float,
    ) -> bool:
        """Attempt an alignment-aware re-peg before falling through to cancel.
        Returns True iff the re-peg was applied (caller should exit early).

        Three-gate decision in ap/retry_engine.decide_repeg:
          1. TIME       — attempts cap + min interval between re-pegs
          2. PROXIMITY  — option must be within REPEG_PROXIMITY_PCT of limit
          3. ALIGNMENT  — underlying still in favor of the original thesis

        If ANY gate fails, returns False and the caller proceeds with its
        normal MISSED_MOVE_CANCEL path. We never re-peg into a broken thesis.
        """
        try:
            from ap.retry_engine import decide_repeg, apply_repeg
        except Exception as e:
            log.debug("[%s] retry_engine unavailable: %s", self.client_id, e)
            return False

        # Resolve underlying spot price for alignment gate.
        underlying = self._get_underlying_symbol_from_contract(sym) \
                     or (order.get("underlying") or order.get("symbol"))
        spot = None
        if underlying:
            try:
                _quote_broker = self._quote_broker()
                if hasattr(_quote_broker, "get_quote"):
                    q = _quote_broker.get_quote(underlying)
                    if isinstance(q, dict):
                        last = q.get("last") or q.get("close") or q.get("price")
                        if last:
                            spot = float(last)
                        else:
                            bid = float(q.get("bid") or 0)
                            ask = float(q.get("ask") or 0)
                            if bid > 0 and ask > 0:
                                spot = (bid + ask) / 2
            except Exception as e:
                log.debug("[%s] _try_repeg spot lookup failed: %s", self.client_id, e)

        # Build the order_row the decision + apply functions need.
        # BLOCKER-2 FIX (post-review): apply_repeg actually re-submits to the
        # broker, so it needs symbol, contract, and qty too. Without these the
        # resubmit can't happen and the slot would die in CREATED forever.
        # P1 FIX (2026-05-21): pass kind + current_ask so the ENTRY ladder
        # (ask + 0.01, ask + 0.02) is applied instead of the legacy gap-close.
        meta = dict(order.get("meta") or {})

        # ── Req 1: Qty hydration ──────────────────────────────────────────────
        # orders.qty may be 0 (falsy) even when meta.contracts / orders.quantity
        # carry the real value.  Must check explicit zero vs None separately.
        def _resolve_qty(order: dict, meta: dict):
            sources = [
                ("orders.qty",        order.get("qty")),
                ("orders.quantity",   order.get("quantity")),
                ("orders.contracts",  order.get("contracts")),
                ("meta.contracts",    meta.get("contracts")),
                ("meta.qty",          meta.get("qty")),
                ("meta.original_qty", meta.get("original_qty")),
            ]
            for src_name, val in sources:
                try:
                    n = int(val)
                    if n > 0:
                        return n, src_name
                except (TypeError, ValueError):
                    pass
            return 0, "no_valid_source"

        _resolved_qty, _qty_src = _resolve_qty(order, meta)
        log.debug(
            "[%s] _try_repeg qty resolution: resolved=%s source=%s "
            "raw={qty=%r, quantity=%r, contracts=%r, meta.contracts=%r} "
            "local=%s contract=%s",
            self.client_id, _resolved_qty, _qty_src,
            order.get("qty"), order.get("quantity"),
            order.get("contracts"), meta.get("contracts"),
            local_id, contract,
        )
        if _resolved_qty <= 0:
            log.warning(
                "[%s] HYDRATION_FAILED local=%s contract=%s client=%s "
                "reason=invalid_qty resolved=%s qty_sources=%r "
                "— skipping repeg, marking invalid_entry_qty",
                self.client_id, local_id, contract, self.client_id,
                _resolved_qty,
                {s: v for s, v in [
                    ("orders.qty", order.get("qty")),
                    ("orders.quantity", order.get("quantity")),
                    ("meta.contracts", meta.get("contracts")),
                ]},
            )
            self._emit_order_event(
                local_order_id=local_id,
                stage="order_monitor",
                decision="REJECT",
                reason_code="invalid_entry_qty",
                explanation=(
                    f"Re-peg blocked: qty resolved to {_resolved_qty} "
                    f"from all sources (orders.qty={order.get('qty')!r}, "
                    f"meta.contracts={meta.get('contracts')!r}). "
                    "Fix: ensure plan.contracts > 0 before create_entry_order."
                ),
                contract=contract,
                inputs={
                    "local_order_id":   local_id,
                    "client_id":        self.client_id,
                    "contract":         contract,
                    "orders_qty":       order.get("qty"),
                    "orders_quantity":  order.get("quantity"),
                    "orders_contracts": order.get("contracts"),
                    "meta_contracts":   meta.get("contracts"),
                    "qty_source":       _qty_src,
                },
            )
            return False

        # ── Req 3: Direction hydration ────────────────────────────────────────
        # Infer direction from orders.direction → orders.side → meta.direction
        # → meta.side → OCC contract symbol (C/P between digits = last resort).
        import re as _re
        def _resolve_direction(order: dict, meta: dict, contract_sym: str):
            for src_name, val in [
                ("orders.direction", order.get("direction")),
                ("orders.side",      order.get("side")),
                ("meta.direction",   meta.get("direction")),
                ("meta.side",        meta.get("side")),
            ]:
                if val and str(val).upper().strip() in ("CALL", "PUT"):
                    return str(val).upper().strip(), src_name
            _m = _re.search(r"\d([CP])\d", str(contract_sym or "").upper())
            if _m:
                return ("CALL" if _m.group(1) == "C" else "PUT"), "contract_inferred"
            return "", "unknown"

        _resolved_dir, _dir_src = _resolve_direction(order, meta, contract or _sym)
        log.debug(
            "[%s] _try_repeg direction resolution: resolved=%r source=%s "
            "raw={direction=%r, side=%r, meta.dir=%r} local=%s contract=%s",
            self.client_id, _resolved_dir, _dir_src,
            order.get("direction"), order.get("side"), meta.get("direction"),
            local_id, contract,
        )

        # Resolve current OPTION-CONTRACT ask for the ladder anchor.
        # SAFETY (post-review): sym MUST be the OCC option contract symbol,
        # NOT the underlying ticker. An OCC option symbol has the structure
        # <ROOT><6 digits date><C|P><8 digit strike>, so it contains digits.
        # Bare underlying tickers ('MSFT', 'SPY') are all alpha. If sym looks
        # like an underlying, refuse the fetch — we'd otherwise quote the
        # stock instead of the option and ladder against the wrong price.
        _is_option_contract = isinstance(sym, str) and any(ch.isdigit() for ch in sym)
        _current_ask = None
        if _is_option_contract:
            try:
                _quote_broker = self._quote_broker()
                if hasattr(_quote_broker, "get_quote"):
                    _opt_q = _quote_broker.get_quote(sym)
                    if isinstance(_opt_q, dict):
                        _ask_raw = _opt_q.get("ask")
                        if _ask_raw is not None:
                            _current_ask = float(_ask_raw) or None
            except Exception:
                _current_ask = None
        else:
            log.warning(
                "[%s] _try_repeg: sym=%r is not an OCC option contract — "
                "refusing to fetch ask. Ladder will fall back to current_option_price.",
                self.client_id, sym,
            )

        order_row = {
            "id":                 local_id,
            "broker_order_id":    order.get("broker_order_id") or self._get_broker_order_id(local_id),
            "symbol":             order.get("symbol") or meta.get("ticker"),
            "contract":            contract or order.get("contract"),
            "qty":                 _resolved_qty,       # hydrated above; always > 0 at this point
            "limit_price":        limit_price,
            "direction":          _resolved_dir,        # hydrated above; source logged
            "signal_entry_price": (order.get("signal_entry_price")
                                   or meta.get("signal_entry_price")
                                   or meta.get("entry_price")),
            "repeg_attempts":     int(meta.get("repeg_attempts") or 0),
            "last_repeg_ts":      float(meta.get("last_repeg_ts") or 0),
            "meta":               meta,
            # P1 ladder inputs:
            "kind":               (order.get("kind") or meta.get("kind") or "ENTRY"),
            "current_ask":        _current_ask,
            # Observability (Req 4):
            "resolved_qty_source":       _qty_src,
            "resolved_direction_source": _dir_src,
        }

        decision = decide_repeg(
            order_row=order_row,
            current_option_price=current_option_price,
            underlying_spot=spot,
        )
        if not decision.ok:
            log.info(
                "[%s] REPEG_DECLINED order=%s sym=%s reason=%s detail=%s",
                self.client_id, local_id, sym, decision.reason, decision.detail,
            )
            return False

        applied = apply_repeg(
            broker=self.broker,
            osm=self.osm,
            order_row=order_row,
            decision=decision,
            client_id=self.client_id,
        )
        if applied:
            log.info(
                "[%s] REPEG_APPLIED order=%s sym=%s new_limit=%.2f attempt=%d",
                self.client_id, local_id, sym, decision.new_limit_price,
                decision.attempts_used,
            )
        return applied

    def _get_option_price(self, symbol: str) -> Optional[float]:
        quote_broker = self._quote_broker()
        if not symbol or not quote_broker:
            return None
        try:
            if hasattr(quote_broker, "get_quote"):
                q = quote_broker.get_quote(symbol)
                if isinstance(q, dict):
                    bid = float(q.get("bid") or 0)
                    ask = float(q.get("ask") or 0)
                    if bid > 0 and ask > 0:
                        return (bid + ask) / 2
            if hasattr(quote_broker, "session") and hasattr(quote_broker, "cfg"):
                cfg = quote_broker.cfg
                base = getattr(cfg, "base_url", None)
                if not base or "sandbox" in str(base).lower():
                    log.warning(
                        "[%s] ORDER_MONITOR_MARKET_DATA_BASE_BLOCKED | symbol=%s base_url=%s",
                        self.client_id,
                        symbol,
                        base or "missing",
                    )
                    return None
                token = getattr(cfg, "access_token", None) or getattr(cfg, "token", "")
                headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
                resp = quote_broker.session.get(
                    f"{base}/v1/markets/quotes",
                    params={"symbols": symbol, "greeks": "false"},
                    headers=headers, timeout=(3.05, 5),
                )
                if resp.status_code == 200:
                    q = resp.json().get("quotes", {}).get("quote", {})
                    if isinstance(q, dict):
                        bid = float(q.get("bid") or 0)
                        ask = float(q.get("ask") or 0)
                        if bid > 0 and ask > 0:
                            return (bid + ask) / 2
        except Exception as e:
            log.debug("[%s] _get_option_price(%s) failed: %s", self.client_id, symbol, e)
        return None

    def _alert(self, msg: str):
        log.error(msg)
        if self.alert_fn:
            try:
                self.alert_fn(msg)
            except Exception as e:
                log.debug(f"Alert fn failed: {e}")
