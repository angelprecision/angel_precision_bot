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

# Timeout thresholds (seconds)
TIMEOUT_CREATED       = int(os.getenv("ORDER_TIMEOUT_CREATED",       "120"))    # 2 min — CREATED = never reached broker
TIMEOUT_SUBMITTED     = int(os.getenv("ORDER_TIMEOUT_SUBMITTED",     "300"))   # 5 min
TIMEOUT_ACKNOWLEDGED  = int(os.getenv("ORDER_TIMEOUT_ACKNOWLEDGED",  "600"))   # 10 min
TIMEOUT_PARTIAL_FILL  = int(os.getenv("ORDER_TIMEOUT_PARTIAL_FILL",  "900"))   # 15 min
TIMEOUT_EXIT_PENDING  = int(os.getenv("ORDER_TIMEOUT_EXIT_PENDING",  "300"))   # 5 min — stricter
TIMEOUT_EXIT_ACK      = int(os.getenv("ORDER_TIMEOUT_EXIT_ACK",      "600"))   # 10 min

POLL_INTERVAL = int(os.getenv("ORDER_MONITOR_POLL", "60"))  # seconds

# Watchdog ownership controls.
# Default is intentionally passive: this monitor alerts only and does not become
# a second lifecycle authority alongside fill monitor, reconciler, OSM, and exit engine.
ORDER_MONITOR_MODE = os.getenv("ORDER_MONITOR_MODE", "watchdog").strip().lower()
ENABLE_MISSED_MOVE_CANCEL = os.getenv("ENABLE_MISSED_MOVE_CANCEL", "0").strip() == "1"
ALLOW_ORDER_MONITOR_POSITION_REOPEN = os.getenv("ALLOW_ORDER_MONITOR_POSITION_REOPEN", "0").strip() == "1"
ORDER_MONITOR_CAN_ACT = ORDER_MONITOR_MODE in {"active", "actor", "enforce", "enforced"}



class APOrderMonitor:
    """
    Background stale-order monitor per client.

    Usage:
        monitor = APOrderMonitor(
            client_id="tradefluencehq@gmail.com",
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
            "missed_move_min_secs":   int(os.getenv("MISSED_MOVE_MIN_SECS",    "600")),
            "missed_move_price_mult": float(os.getenv("MISSED_MOVE_PRICE_MULT", "1.20")),
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
        while not self._stop_event.wait(POLL_INTERVAL):
            try:
                self._check_entry_orders()
                self._check_exit_orders()
            except Exception as e:
                log.error(f"[{self.client_id}] OrderMonitor loop error: {e}")
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
                if age_secs > TIMEOUT_CREATED:
                    self._handle_stale_entry(
                        local_id, status, contract, age_secs,
                        action="cancel",
                        reason=f"CREATED for {age_secs:.0f}s > {TIMEOUT_CREATED}s — never submitted",
                    )

            elif status == "SUBMITTED":
                ref_ts = submitted_ts or created_ts
                age_secs = (now - ref_ts).total_seconds()

                # MISSED-MOVE CANCEL — strategy decision with documented tradeoff:
                # RISK: Cancels valid trades in fast markets where price runs 20%+
                #       then pulls back to our level — reduces win rate in volatile sessions.
                # BENEFIT: Prevents chasing stale setups where the move already happened.
                # TUNING: MISSED_MOVE_PRICE_MULT via env (default 1.20 = 20% above limit).
                #         Increase to 1.30+ to reduce false cancels in high-volatility markets.
                _MISSED_MOVE_MIN_SECS = int(os.getenv("MISSED_MOVE_MIN_SECS", "600"))
                _MISSED_MOVE_PRICE_MULT = float(os.getenv("MISSED_MOVE_PRICE_MULT", "1.20"))
                if ENABLE_MISSED_MOVE_CANCEL and broker_oid and age_secs >= _MISSED_MOVE_MIN_SECS and status == "SUBMITTED":
                    try:
                        _limit_price = (
                            order.get("limit_price")
                            or order.get("price")
                            or (self.osm.get_order(local_id) or {}).get("limit_price")
                            or (self.osm.get_order(local_id) or {}).get("price")
                        )
                        _sym = order.get("contract") or order.get("symbol", "")
                        if _limit_price and float(_limit_price) > 0 and _sym:
                            _current = self._get_option_price(_sym)
                            if _current and _current > float(_limit_price) * _MISSED_MOVE_PRICE_MULT:
                                log.warning(
                                    "[%s] MISSED MOVE CANCEL | %s | limit=$%.2f current=$%.2f "
                                    "(%.0f%% above limit) after %.0fs — move happened without us",
                                    self.client_id, _sym,
                                    float(_limit_price), _current,
                                    (_current / float(_limit_price) - 1) * 100,
                                    age_secs,
                                )
                                self._handle_stale_entry(
                                    local_id, status, contract, age_secs,
                                    action="cancel",
                                    reason=(
                                        f"MISSED_MOVE — limit=${float(_limit_price):.2f} "
                                        f"current=${_current:.2f} ({(_current/float(_limit_price)-1)*100:.0f}% above) "
                                        f"after {age_secs:.0f}s — canceling stale entry"
                                    ),
                                )
                                continue
                    except Exception as _mme:
                        log.debug("[%s] Missed-move check failed (non-critical): %s", self.client_id, _mme)

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
                                f"— no fill"
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

        if not ORDER_MONITOR_CAN_ACT:
            self._alert(
                f"[WATCHDOG ONLY] Stale entry detected; no cancel attempted | "
                f"{self.client_id} | {contract} | {local_order_id} | {reason}"
            )
            log.warning(
                "[%s] WATCHDOG MODE — stale entry action suppressed | order=%s status=%s action=%s",
                self.client_id, local_order_id, status, action,
            )
            return

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
                "[%s] Failed checking newer replacement exit | pos=%s old_exit=%s: %s",
                self.client_id, position_id, canceled_exit_order_id, e,
            )
            return {"local_order_id": "UNKNOWN_REPLACEMENT_CHECK_FAILED", "status": "UNKNOWN"}

    def _query_broker_order(self, broker_order_id: Optional[str]) -> Optional[str]:
        if not broker_order_id or not self.broker:
            return None
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
                           contract, position_id, created_ts, submitted_ts,
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
                import requests as _req
                cfg = self.broker.cfg
                base = getattr(cfg, "base_url", "https://sandbox.tradier.com")
                token = getattr(cfg, "access_token", None) or getattr(cfg, "token", "")
                headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
                resp = _req.get(
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
