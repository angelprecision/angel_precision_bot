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
import threading
import time
from datetime import datetime, timezone
from typing import Optional

from ap.db import conn, run_with_retry
from ap.observability import emit_decision_event, get_git_commit, make_config_hash
from ap.utils import now_utc_iso

log = logging.getLogger("ap.order_monitor")

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
TIMEOUT_CREATED       = int(os.getenv("ORDER_TIMEOUT_CREATED",       "30"))    # 30s — CREATED = never reached broker
TIMEOUT_CREATED_NO_BROKER_WARN = int(os.getenv("ORDER_TIMEOUT_CREATED_NO_BROKER_WARN", "10"))  # fast diagnostic — alert at 10s, cancel at 30s
TIMEOUT_SUBMITTED     = int(os.getenv("ORDER_TIMEOUT_SUBMITTED",     "300"))   # 5 min
# Entry ACKNOWLEDGED timeout: an options scalp limit that sits acknowledged-
# unfilled for 10 minutes is a stale entry — the move already happened. If it
# fills now we enter late into a weaker setup (the SMCI bug). 180s is the
# correct ceiling for scalp entries. Exits are handled separately and faster.
TIMEOUT_ACKNOWLEDGED  = int(os.getenv("ORDER_TIMEOUT_ACKNOWLEDGED",  "180"))   # 3 min — scalp entry ceiling
TIMEOUT_PARTIAL_FILL  = int(os.getenv("ORDER_TIMEOUT_PARTIAL_FILL",  "900"))   # 15 min
# Hard ceiling on ANY unfilled buy-to-open entry limit regardless of status.
# If a scalp entry has not filled in this window, the setup is stale — cancel.
# P1 ENTRY FIX (2026-05-21): 150s -> 25s. With the ask-based ladder and 6s
# repeg interval, orders that haven't filled within 25s aren't going to fill
# at any reasonable price. Earlier cancellation frees the slot for the next
# signal and prevents capital being parked on dead limit orders.
ENTRY_LIMIT_MAX_AGE_SECONDS = int(os.getenv("ENTRY_LIMIT_MAX_AGE_SECONDS", "25"))  # 25s
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



class APOrderMonitor:
    """
    Background stale-order monitor per client.

    Usage:
        monitor = APOrderMonitor(
            client_id=member["email"],  # always pass the authenticated client_id
            broker=broker,
            order_state_machine=osm,
            position_manager=pm,
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
        alert_fn=None,
    ):
        self.client_id   = client_id
        self.broker      = broker
        self.osm         = order_state_machine
        self.pm          = position_manager
        self.exit_engine = exit_engine
        self.alert_fn    = alert_fn
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
        })

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
                    # P1 ENTRY FIX (2026-05-21): TIMEOUT_CREATED tightened from
                    # 120s -> 30s. Reason code is now LOST_HANDOFF_30S.
                    # Systemic-rate guard: 3 in 5 minutes for the same client_id
                    # halts new entry arms until the operator clears it.
                    _sig_id = order.get("signal_id") or "?"
                    _plan_id = order.get("plan_id") or "?"
                    _enriched_reason = (
                        f"LOST_HANDOFF_30S: CREATED for {age_secs:.0f}s > {TIMEOUT_CREATED}s — "
                        f"never submitted (signal_id={_sig_id} plan_id={_plan_id} "
                        f"broker_order_id={broker_oid or 'null'}); "
                        f"watcher likely fell off or on_trigger never fired"
                    )
                    log.warning(
                        "[%s] LOST_HANDOFF_30S | local=%s contract=%s age=%.0fs "
                        "signal_id=%s plan_id=%s broker_oid=%s",
                        self.client_id, local_id, contract, age_secs,
                        _sig_id, _plan_id, broker_oid or "null",
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

            # ── Trigger (a): MISSED MOVE ──────────────────────────────────
            if (
                ENABLE_MISSED_MOVE_CANCEL
                and broker_oid
                and age_secs >= _missed_min_secs
                and _limit_price
                and float(_limit_price) > 0
                and _sym
            ):
                _current = self._get_option_price(_sym)
                if _current and _current > float(_limit_price) * _missed_price_mult:
                    # AUDIT PHASE-2: before MISSED_MOVE_CANCEL, try an alignment-
                    # gated re-peg. Three gates (time, proximity, underlying
                    # alignment) must ALL pass. If alignment broke (the MSFT
                    # 2026-05-19 case), this returns ok=False and we fall through
                    # to the existing cancel — which is the correct behavior, the
                    # cancel was right today, we just couldn't chase safely.
                    if self._try_repeg(order, local_id, contract, _sym, float(_limit_price), _current, status, age_secs):
                        return True  # re-peg applied; next tick will observe new limit
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
            # stale scalp entry — cancel regardless of price.
            if broker_oid and age_secs >= ENTRY_LIMIT_MAX_AGE_SECONDS:
                log.warning(
                    "[%s] ENTRY_ACK_TIMEOUT_CANCEL | %s | status=%s age=%.0fs "
                    ">= %ds hard entry ceiling — scalp entry stale",
                    self.client_id, _sym or contract, status, age_secs,
                    ENTRY_LIMIT_MAX_AGE_SECONDS,
                )
                self._emit_order_event(
                    local_order_id=local_id,
                    stage="order_monitor",
                    decision="REJECT",
                    reason_code="MISSED_MOVE_ENTRY_CANCEL",
                    explanation=(
                        f"Canceling stale entry — unfilled {age_secs:.0f}s "
                        f">= {ENTRY_LIMIT_MAX_AGE_SECONDS}s hard ceiling (status={status})"
                    ),
                    contract=contract,
                    inputs={
                        "limit_price": float(_limit_price) if _limit_price else 0.0,
                        "current_option_price": 0.0,
                        "percent_above_limit": 0.0,
                        "age_secs": round(age_secs, 1),
                        "broker_order_id": broker_oid,
                        "status": status,
                    },
                )
                self._handle_stale_entry(
                    local_id, status, contract, age_secs,
                    action="cancel",
                    reason=(
                        f"STALE_ENTRY_CANCEL ENTRY_ACK_TIMEOUT — unfilled {age_secs:.0f}s "
                        f">= {ENTRY_LIMIT_MAX_AGE_SECONDS}s ceiling status={status}"
                    ),
                )
                return True

        except Exception as _se:
            log.debug(
                "[%s] _check_stale_entry_cancel non-fatal error: %s",
                self.client_id, _se,
            )
        return False

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
        ok = self.osm.transition(local_order_id, new_status)
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
                           limit_price,
                           limit_price AS price,
                           fill_price
                    FROM orders
                    WHERE client_id=%s
                      AND kind='ENTRY'
                      AND status IN ('CREATED','SUBMITTED','ACKNOWLEDGED','PARTIAL_FILL')
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
                if hasattr(self.broker, "get_quote"):
                    q = self.broker.get_quote(underlying)
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

        # Resolve current option ask for the ladder anchor.
        _current_ask = None
        try:
            if hasattr(self.broker, "get_quote"):
                _opt_q = self.broker.get_quote(sym)
                if isinstance(_opt_q, dict):
                    _ask_raw = _opt_q.get("ask")
                    if _ask_raw is not None:
                        _current_ask = float(_ask_raw) or None
        except Exception:
            _current_ask = None

        order_row = {
            "id":                 local_id,
            "broker_order_id":    order.get("broker_order_id") or self._get_broker_order_id(local_id),
            "symbol":             order.get("symbol") or meta.get("ticker"),
            "contract":            contract or order.get("contract"),
            "qty":                 order.get("qty") or order.get("quantity"),
            "limit_price":        limit_price,
            "direction":          (order.get("direction")
                                   or order.get("side")
                                   or meta.get("direction")),
            "signal_entry_price": (order.get("signal_entry_price")
                                   or meta.get("signal_entry_price")
                                   or meta.get("entry_price")),
            "repeg_attempts":     int(meta.get("repeg_attempts") or 0),
            "last_repeg_ts":      float(meta.get("last_repeg_ts") or 0),
            "meta":               meta,
            # P1 ladder inputs:
            "kind":               (order.get("kind") or meta.get("kind") or "ENTRY"),
            "current_ask":        _current_ask,
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
        if not symbol or not self.broker:
            return None
        try:
            if hasattr(self.broker, "get_quote"):
                q = self.broker.get_quote(symbol)
                if isinstance(q, dict):
                    bid = float(q.get("bid") or 0)
                    ask = float(q.get("ask") or 0)
                    if bid > 0 and ask > 0:
                        return (bid + ask) / 2
            if hasattr(self.broker, "session") and hasattr(self.broker, "cfg"):
                cfg = self.broker.cfg
                base = getattr(cfg, "base_url", "https://sandbox.tradier.com")
                token = getattr(cfg, "access_token", None) or getattr(cfg, "token", "")
                headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
                resp = self.broker.session.get(
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
