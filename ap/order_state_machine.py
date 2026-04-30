# ap/order_state_machine.py — APOrderStateMachine
# =============================================================================
# Enforced order lifecycle controller.
#
# This module is the canonical order-state authority:
#   - validates legal state transitions
#   - persists order status updates
#   - coordinates exit-engine hooks after broker-confirmed exit events
#   - emits structured observability for transition success/failure
#
# It should remain small, deterministic, and conservative. Fill monitor and
# order monitor may observe broker reality, but this state machine decides
# whether the requested lifecycle move is legal.
# =============================================================================

# CONTRACT: Any caller that passes filled_qty MUST pass the broker cumulative filled quantity for that order, not an incremental fill delta.
from __future__ import annotations

import logging
import os
import threading
import uuid
from typing import Optional

from ap.db import conn, run_with_retry
from ap.utils import now_utc_iso

try:
    from ap.observability import emit_decision_event, get_git_commit, make_config_hash
except Exception:  # pragma: no cover - observability must never block order truth
    emit_decision_event = None

    def get_git_commit(default: str = "unknown") -> str:
        return default

    def make_config_hash(config: dict) -> str:
        return "unknown"


log = logging.getLogger("ap.order_state_machine")

_exit_engine_registry: dict[str, object] = {}
_registry_lock = threading.Lock()


def register_exit_engine(client_id: str, exit_engine) -> None:
    """Register a per-client exit engine for broker-confirmed exit hooks."""
    with _registry_lock:
        _exit_engine_registry[client_id] = exit_engine


def unregister_exit_engine(client_id: str) -> None:
    with _registry_lock:
        _exit_engine_registry.pop(client_id, None)


def _get_exit_engine_for_client(client_id: str):
    with _registry_lock:
        return _exit_engine_registry.get(client_id)


class OrderStatus:
    # Entry states
    CREATED = "CREATED"
    PENDING_TRIGGER = "PENDING_TRIGGER"  # watcher armed, no broker order yet
    SUBMITTED = "SUBMITTED"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    PARTIAL_FILL = "PARTIAL_FILL"
    FILLED = "FILLED"

    # Exit states
    EXIT_REQUESTED = "EXIT_REQUESTED"
    EXIT_SUBMITTED = "EXIT_SUBMITTED"
    EXIT_ACKNOWLEDGED = "EXIT_ACKNOWLEDGED"
    EXIT_PARTIAL_FILL = "EXIT_PARTIAL_FILL"
    EXIT_FILLED = "EXIT_FILLED"

    # Terminal failure states
    REJECTED = "REJECTED"
    CANCELED = "CANCELED"
    EXPIRED = "EXPIRED"
    ERROR = "ERROR"

    # State sets
    ENTRY_ACTIVE = {CREATED, PENDING_TRIGGER, SUBMITTED, ACKNOWLEDGED, PARTIAL_FILL}
    EXIT_ACTIVE = {EXIT_REQUESTED, EXIT_SUBMITTED, EXIT_ACKNOWLEDGED, EXIT_PARTIAL_FILL}
    TERMINAL = {FILLED, EXIT_FILLED, REJECTED, CANCELED, EXPIRED, ERROR}
    ACTIVE = ENTRY_ACTIVE | EXIT_ACTIVE

    # Legal transitions.
    # Conservative rule: terminal states are final; active states can resolve to
    # broker terminal failures when broker/reconciler confirms reality.
    TRANSITIONS: dict[str, set[str]] = {
        CREATED: {PENDING_TRIGGER, SUBMITTED, ERROR, CANCELED, EXPIRED, REJECTED},
        PENDING_TRIGGER: {SUBMITTED, ERROR, CANCELED, EXPIRED, REJECTED},
        SUBMITTED: {ACKNOWLEDGED, PARTIAL_FILL, FILLED, REJECTED, EXPIRED, ERROR, CANCELED},
        ACKNOWLEDGED: {PARTIAL_FILL, FILLED, REJECTED, EXPIRED, ERROR, CANCELED},
        PARTIAL_FILL: {FILLED, REJECTED, EXPIRED, ERROR, CANCELED},
        EXIT_REQUESTED: {EXIT_SUBMITTED, REJECTED, CANCELED, EXPIRED, ERROR},
        EXIT_SUBMITTED: {EXIT_ACKNOWLEDGED, EXIT_PARTIAL_FILL, EXIT_FILLED, REJECTED, EXPIRED, ERROR, CANCELED},
        EXIT_ACKNOWLEDGED: {EXIT_PARTIAL_FILL, EXIT_FILLED, REJECTED, EXPIRED, ERROR, CANCELED},
        EXIT_PARTIAL_FILL: {EXIT_FILLED, REJECTED, EXPIRED, ERROR, CANCELED},
    }

    @classmethod
    def can_transition(cls, from_status: str, to_status: str) -> bool:
        return to_status in cls.TRANSITIONS.get(from_status, set())

    @classmethod
    def is_terminal(cls, status: str) -> bool:
        return status in cls.TERMINAL

    @classmethod
    def is_active(cls, status: str) -> bool:
        return status in cls.ACTIVE


PENDING_ENTRY_STATUSES = (
    OrderStatus.CREATED,
    OrderStatus.PENDING_TRIGGER,
    OrderStatus.SUBMITTED,
    OrderStatus.ACKNOWLEDGED,
    OrderStatus.PARTIAL_FILL,
)
PENDING_EXIT_STATUSES = (
    OrderStatus.EXIT_REQUESTED,
    OrderStatus.EXIT_SUBMITTED,
    OrderStatus.EXIT_ACKNOWLEDGED,
    OrderStatus.EXIT_PARTIAL_FILL,
)


class APOrderStateMachine:
    def __init__(self, client_id: str):
        self.client_id = client_id
        self.run_id = os.getenv("AP_RUN_ID", "unknown")
        self.strategy_version = os.getenv("AP_STRATEGY_VERSION", "ap_live_beta")
        self.git_commit = get_git_commit()
        self.config_hash = make_config_hash(
            {
                "entry_active": sorted(PENDING_ENTRY_STATUSES),
                "exit_active": sorted(PENDING_EXIT_STATUSES),
                "terminal": sorted(OrderStatus.TERMINAL),
                "transitions": {k: sorted(v) for k, v in OrderStatus.TRANSITIONS.items()},
            }
        )
        log.info("[%s] APOrderStateMachine initialized", client_id)

    # ---------------------------------------------------------------------
    # Observability
    # ---------------------------------------------------------------------

    def _reason_code_for_transition(self, old_status: str, new_status: str, kind: str = "") -> str:
        if new_status == OrderStatus.FILLED:
            return "ENTRY_FILLED"
        if new_status == OrderStatus.EXIT_FILLED:
            return "EXIT_FILLED"
        if new_status in (OrderStatus.PARTIAL_FILL, OrderStatus.EXIT_PARTIAL_FILL):
            return "PARTIAL_FILL"
        if new_status == OrderStatus.REJECTED:
            return "ORDER_REJECTED"
        if new_status == OrderStatus.CANCELED:
            return "ORDER_CANCELED"
        if new_status == OrderStatus.EXPIRED:
            return "ORDER_EXPIRED"
        if new_status == OrderStatus.ERROR:
            return "ORDER_ERROR"
        if new_status in (OrderStatus.SUBMITTED, OrderStatus.EXIT_SUBMITTED):
            return "ORDER_SUBMITTED"
        if new_status in (OrderStatus.ACKNOWLEDGED, OrderStatus.EXIT_ACKNOWLEDGED):
            return "BROKER_ACKNOWLEDGED"
        if new_status == OrderStatus.PENDING_TRIGGER:
            return "PENDING_TRIGGER"
        return "ORDER_TRANSITION"

    def _emit_transition_event(
        self,
        *,
        local_order_id: str,
        old_status: str,
        new_status: str,
        order: Optional[dict] = None,
        decision: str = "CONFIRMED",
        reason_code: Optional[str] = None,
        explanation: str = "",
        broker_order_id=None,
        filled_qty=None,
        fill_price=None,
        last_error=None,
        extra_inputs: Optional[dict] = None,
        extra_context: Optional[dict] = None,
    ) -> None:
        if emit_decision_event is None:
            return
        try:
            order = dict(order or {})
            kind = str(order.get("kind") or "")
            emit_decision_event(
                run_id=self.run_id,
                candidate_id=str(order.get("signal_id") or local_order_id or ""),
                trade_id=str(local_order_id or ""),
                position_id=str(order.get("position_id") or ""),
                client_id=self.client_id,
                stage="order_state_machine",
                decision=decision,
                reason_code=reason_code or self._reason_code_for_transition(old_status, new_status, kind),
                explanation=explanation or f"Order transition {old_status} -> {new_status}",
                symbol=order.get("symbol"),
                contract=order.get("contract"),
                setup_type=order.get("pattern"),
                timeframe=order.get("timeframe"),
                strategy_version=self.strategy_version,
                config_hash=self.config_hash,
                git_commit=self.git_commit,
                inputs={
                    "kind": kind,
                    "old_status": old_status,
                    "new_status": new_status,
                    "local_order_id": local_order_id,
                    "broker_order_id": broker_order_id or order.get("broker_order_id"),
                    "filled_qty": filled_qty,
                    "fill_price": fill_price,
                    "last_error": last_error,
                    **(extra_inputs or {}),
                },
                context=extra_context or {},
            )
        except Exception as e:
            log.debug("OSM observability emit failed (non-critical): %s", e)

    # ---------------------------------------------------------------------
    # Order creation
    # ---------------------------------------------------------------------

    def create_entry_order(self, plan, *, limit_price=None, reserved_cost=None) -> str:
        existing = self._get_order_by_plan(plan.plan_id, kind="ENTRY")
        if existing:
            log.warning(
                "[%s] create_entry_order SKIPPED — plan %s already has order %s",
                self.client_id,
                plan.plan_id,
                existing["local_order_id"],
            )
            return existing["local_order_id"]

        local_order_id = str(uuid.uuid4())
        contract = getattr(plan, "contract_symbol", None) or plan.ticker
        lp = float(limit_price) if limit_price else (float(plan.limit_price) if getattr(plan, "limit_price", None) else None)
        rc = float(reserved_cost) if reserved_cost else (float(plan.max_position_usd) if getattr(plan, "max_position_usd", None) else None)
        ts = now_utc_iso()

        def _fn():
            with conn() as c:
                c.execute(
                    """
                    INSERT INTO orders (
                        local_order_id, client_id, plan_id, signal_id,
                        kind, status,
                        symbol, contract, direction,
                        qty, limit_price, reserved_cost,
                        filled_qty,
                        created_ts, updated_ts
                    ) VALUES (
                        %s,%s,%s,%s,
                        'ENTRY','CREATED',
                        %s,%s,%s,
                        %s,%s,%s,
                        0,
                        %s,%s
                    )
                    ON CONFLICT (local_order_id) DO NOTHING
                    """,
                    (
                        local_order_id,
                        self.client_id,
                        plan.plan_id,
                        plan.signal_id,
                        plan.ticker,
                        contract,
                        plan.side.upper(),
                        int(plan.contracts),
                        lp,
                        rc,
                        ts,
                        ts,
                    ),
                )

        run_with_retry(_fn)
        log.info(
            "[%s] ORDER CREATED (entry) | %s x%s | local_order_id=%s",
            self.client_id,
            contract,
            plan.contracts,
            local_order_id,
        )
        return local_order_id

    def create_exit_order(
        self,
        *,
        position_id,
        contract,
        symbol,
        direction,
        qty,
        local_order_id=None,
        plan_id=None,
        signal_id=None,
        limit_price=None,
        reserved_cost=None,
    ) -> str:
        existing = self._get_active_exit_order(position_id)
        if existing:
            log.warning(
                "[%s] create_exit_order SKIPPED — position %s already has exit order %s",
                self.client_id,
                position_id,
                existing["local_order_id"],
            )
            return existing["local_order_id"]

        local_id = local_order_id or str(uuid.uuid4())
        ts = now_utc_iso()

        def _fn():
            with conn() as c:
                c.execute(
                    """
                    INSERT INTO orders (
                        local_order_id, client_id, position_id, plan_id, signal_id,
                        kind, status,
                        symbol, contract, direction,
                        qty, limit_price, reserved_cost,
                        filled_qty,
                        created_ts, updated_ts
                    ) VALUES (
                        %s,%s,%s,%s,%s,
                        'EXIT','EXIT_REQUESTED',
                        %s,%s,%s,
                        %s,%s,%s,
                        0,
                        %s,%s
                    )
                    ON CONFLICT (local_order_id) DO NOTHING
                    """,
                    (
                        local_id,
                        self.client_id,
                        position_id,
                        plan_id,
                        signal_id,
                        symbol.upper(),
                        contract,
                        direction.upper(),
                        int(qty),
                        float(limit_price) if limit_price else None,
                        float(reserved_cost) if reserved_cost else None,
                        ts,
                        ts,
                    ),
                )

        run_with_retry(_fn)
        log.info(
            "[%s] ORDER CREATED (exit) | %s x%s pos=%s | local_order_id=%s",
            self.client_id,
            contract,
            qty,
            position_id,
            local_id,
        )
        return local_id

    # ---------------------------------------------------------------------
    # State transition authority
    # ---------------------------------------------------------------------

    def transition(
        self,
        local_order_id: str,
        new_status: str,
        *,
        broker_order_id=None,
        filled_qty=None,
        fill_price=None,
        last_error=None,
        submitted_ts=None,
        filled_ts=None,
        position_id=None,
    ) -> bool:
        current = self._get_order(local_order_id)
        if not current:
            log.error("[%s] transition: order %s not found", self.client_id, local_order_id)
            self._emit_transition_event(
                local_order_id=local_order_id,
                old_status="UNKNOWN",
                new_status=new_status,
                decision="REJECT",
                reason_code="ORDER_NOT_FOUND",
                explanation=f"Order {local_order_id} not found for transition to {new_status}",
            )
            return False

        current = dict(current)
        old_status = current.get("status", "")
        kind = current.get("kind", "")

        prev_filled = self._safe_int(current.get("filled_qty"), 0)
        same_state_fill_update = False

        # filled_qty contract: broker cumulative quantity only, never incremental.
        if filled_qty is not None:
            incoming_filled = self._safe_int(filled_qty, None)
            if incoming_filled is None:
                log.critical("[%s] INVALID FILL QTY | order=%s filled_qty=%r", self.client_id, local_order_id, filled_qty)
                return False
            if incoming_filled < prev_filled:
                log.critical("[%s] INVALID CUMULATIVE FILL REGRESSION | order=%s new=%s prev=%s", self.client_id, local_order_id, incoming_filled, prev_filled)
                self._emit_transition_event(
                    local_order_id=local_order_id, old_status=old_status, new_status=new_status,
                    order=current, decision="REJECT", reason_code="FILL_QTY_REGRESSION",
                    explanation=f"filled_qty must be cumulative: new={incoming_filled} prev={prev_filled}",
                    broker_order_id=broker_order_id, filled_qty=filled_qty, fill_price=fill_price, last_error=last_error,
                )
                return False

        if old_status == new_status:
            # Same-state fill updates are not no-ops.
            # Brokers can report EXIT_PARTIAL_FILL cum=1 then EXIT_PARTIAL_FILL cum=2.
            if new_status in (OrderStatus.PARTIAL_FILL, OrderStatus.EXIT_PARTIAL_FILL):
                same_state_fill_update = True
            else:
                log.debug("[%s] %s already %s — no-op", self.client_id, local_order_id, new_status)
                return True

        if OrderStatus.is_terminal(old_status):
            reason = f"terminal_transition_blocked:{old_status}->{new_status}"
            log.warning(
                "[%s] TRANSITION BLOCKED — %s already terminal (%s), cannot move to %s",
                self.client_id,
                local_order_id,
                old_status,
                new_status,
            )
            self._record_error(local_order_id, reason)
            self._emit_transition_event(
                local_order_id=local_order_id,
                old_status=old_status,
                new_status=new_status,
                order=current,
                decision="REJECT",
                reason_code="TERMINAL_STATE_BLOCK",
                explanation=reason,
                last_error=last_error,
            )
            return False

        if not same_state_fill_update and not OrderStatus.can_transition(old_status, new_status):
            reason = f"illegal_transition:{old_status}->{new_status}"
            log.critical(
                "[%s] ILLEGAL TRANSITION — %s: %s -> %s | kind=%s broker=%s filled=%s price=%s",
                self.client_id,
                local_order_id,
                old_status,
                new_status,
                kind,
                broker_order_id or current.get("broker_order_id"),
                filled_qty,
                fill_price,
            )
            self._record_error(local_order_id, reason)
            self._emit_transition_event(
                local_order_id=local_order_id,
                old_status=old_status,
                new_status=new_status,
                order=current,
                decision="REJECT",
                reason_code="ILLEGAL_TRANSITION",
                explanation=reason,
                broker_order_id=broker_order_id,
                filled_qty=filled_qty,
                fill_price=fill_price,
                last_error=last_error,
            )
            return False

        updates = ["status=%s", "updated_ts=NOW()"]
        params = [new_status]

        if broker_order_id:
            updates.append("broker_order_id=%s")
            params.append(broker_order_id)
        if filled_qty is not None:
            updates.append("filled_qty=%s")
            params.append(int(filled_qty))
        if fill_price is not None:
            updates.append("fill_price=%s")
            params.append(float(fill_price))
        if last_error:
            updates.append("last_error=%s")
            params.append(last_error)
        if submitted_ts:
            updates.append("submitted_ts=%s")
            params.append(submitted_ts)
        if position_id:
            updates.append("position_id=%s")
            params.append(position_id)
        if new_status in (OrderStatus.FILLED, OrderStatus.EXIT_FILLED):
            updates.append("filled_ts=%s")
            params.append(filled_ts or now_utc_iso())

        params.extend([local_order_id, self.client_id])
        sql = f"UPDATE orders SET {', '.join(updates)} WHERE local_order_id=%s AND client_id=%s"

        def _fn():
            with conn() as c:
                cur = c.execute(sql, tuple(params))
                # psycopg2 returns cursor; psycopg3 may return cursor-like object.
                rowcount = getattr(cur, "rowcount", getattr(c, "rowcount", None))
                return rowcount

        rowcount = run_with_retry(_fn)
        if rowcount == 0:
            reason = f"transition_update_no_rows:{old_status}->{new_status}"
            log.critical("[%s] OSM UPDATE TOUCHED ZERO ROWS | %s | %s", self.client_id, local_order_id, reason)
            self._record_error(local_order_id, reason)
            self._emit_transition_event(
                local_order_id=local_order_id,
                old_status=old_status,
                new_status=new_status,
                order=current,
                decision="ERROR",
                reason_code="DB_UPDATE_MISSED",
                explanation=reason,
            )
            return False

        log.info(
            "[%s] ORDER %s -> %s | %s%s%s",
            self.client_id,
            old_status,
            new_status,
            local_order_id,
            f" broker={broker_order_id}" if broker_order_id else "",
            f" fill={filled_qty}@{fill_price}" if fill_price is not None else "",
        )

        self._emit_transition_event(
            local_order_id=local_order_id,
            old_status=old_status,
            new_status=new_status,
            order=current,
            decision="CONFIRMED" if new_status not in {OrderStatus.REJECTED, OrderStatus.CANCELED, OrderStatus.EXPIRED, OrderStatus.ERROR} else "TERMINAL",
            broker_order_id=broker_order_id,
            filled_qty=filled_qty,
            fill_price=fill_price,
            last_error=last_error,
        )

        self._handle_exit_engine_hooks(
            current=current,
            new_status=new_status,
            position_id=position_id,
            filled_qty=filled_qty,
            fill_price=fill_price,
            broker_order_id=broker_order_id or current.get("broker_order_id"),
            local_order_id=local_order_id,
        )

        return True

    def _handle_exit_engine_hooks(
        self,
        *,
        current: dict,
        new_status: str,
        position_id=None,
        filled_qty=None,
        fill_price=None,
        broker_order_id=None,
        local_order_id=None,
    ) -> None:
        """Coordinate broker-confirmed EXIT events with the exit engine.

        Critical invariant:
            EXIT_FILLED means "this EXIT order fully filled".
            It does NOT always mean "the whole position is flat".

        Therefore, when a scale-out order reaches EXIT_FILLED, we must apply
        only that order's filled quantity to the ManagedPosition and keep the
        runner alive unless quantity_remaining reaches zero.
        """
        if new_status not in (
            OrderStatus.EXIT_SUBMITTED,
            OrderStatus.EXIT_FILLED,
            OrderStatus.EXIT_PARTIAL_FILL,
            OrderStatus.CANCELED,
            OrderStatus.EXPIRED,
            OrderStatus.REJECTED,
        ):
            return

        _pos_id = position_id or current.get("position_id")
        _kind = str(current.get("kind") or "ENTRY").upper()
        if not _pos_id or _kind != "EXIT":
            return

        try:
            _ee = _get_exit_engine_for_client(self.client_id)
            if not _ee:
                log.warning(
                    "[%s] EXIT hook skipped — no exit engine registered | order=%s pos=%s status=%s",
                    self.client_id, local_order_id or current.get("local_order_id"), _pos_id, new_status,
                )
                return

            _local_id = local_order_id or current.get("local_order_id")
            _broker_id = broker_order_id or current.get("broker_order_id")
            _order_qty = self._safe_int(current.get("qty"), 0)
            _prev_filled = self._safe_int(current.get("filled_qty"), 0)
            _cum_filled = self._safe_int(filled_qty, None)

            if _cum_filled is not None and _cum_filled < _prev_filled:
                log.critical(
                    "[%s] INVALID EXIT CUMULATIVE FILL | order=%s pos=%s new=%s prev=%s",
                    self.client_id, _local_id, _pos_id, _cum_filled, _prev_filled,
                )
                return

            if new_status == OrderStatus.EXIT_SUBMITTED:
                if not _broker_id:
                    log.critical(
                        "[%s] EXIT_SUBMITTED hook missing broker_order_id | order=%s pos=%s",
                        self.client_id, _local_id, _pos_id,
                    )
                    return
                self._call_exit_engine(
                    _ee,
                    "set_pending_exit_order",
                    _pos_id,
                    local_order_id=_local_id,
                    broker_order_id=_broker_id,
                    qty=_order_qty,
                    reason=str(current.get("last_error") or ""),
                )
                return

            if new_status == OrderStatus.EXIT_PARTIAL_FILL:
                if _cum_filled is None:
                    log.warning(
                        "[%s] EXIT_PARTIAL_FILL missing filled_qty | order=%s pos=%s — not applying engine fill",
                        self.client_id, _local_id, _pos_id,
                    )
                    return
                _delta = max(0, _cum_filled - _prev_filled)
                if _delta > 0:
                    self._call_exit_engine(
                        _ee,
                        "note_partial_exit_fill",
                        _pos_id,
                        _delta,
                        fill_price=fill_price,
                        local_order_id=_local_id,
                        broker_order_id=_broker_id,
                        cumulative_filled=_cum_filled,
                    )
                return

            if new_status == OrderStatus.EXIT_FILLED:
                # For a fully-filled EXIT order, apply only the unaccounted delta.
                # If broker did not provide cumulative filled_qty, the order is fully
                # filled, so use order qty as the cumulative amount for this order.
                if _cum_filled is None or _cum_filled <= 0:
                    _cum_filled = _order_qty
                _delta = max(0, _cum_filled - _prev_filled)

                # Determine whether this filled EXIT order flattens the whole position.
                _pos = self._get_exit_engine_position(_ee, _pos_id)
                _remaining_before = self._safe_int(getattr(_pos, "quantity_remaining", None), None) if _pos else None
                if _remaining_before is None:
                    _remaining_before = self._get_position_remaining_from_db(_pos_id)

                if _delta <= 0:
                    log.info(
                        "[%s] EXIT_FILLED hook no-op — no new filled qty | order=%s pos=%s prev=%s cum=%s",
                        self.client_id, _local_id, _pos_id, _prev_filled, _cum_filled,
                    )
                    self._call_exit_engine(
                        _ee,
                        "clear_exit_in_flight",
                        _pos_id,
                        local_order_id=_local_id,
                        broker_order_id=_broker_id,
                    )
                    return

                # If this exit order sold less than the current remaining position,
                # it is a completed scale-out order, not a full close.
                if _remaining_before is not None and _delta < int(_remaining_before):
                    log.info(
                        "[%s] EXIT_FILLED treated as completed scale-out | order=%s pos=%s delta=%s remaining_before=%s",
                        self.client_id, _local_id, _pos_id, _delta, _remaining_before,
                    )
                    self._call_exit_engine(
                        _ee,
                        "note_partial_exit_fill",
                        _pos_id,
                        _delta,
                        fill_price=fill_price,
                        local_order_id=_local_id,
                        broker_order_id=_broker_id,
                        cumulative_filled=_cum_filled,
                    )
                    return

                # Full flatten: order fill quantity equals/exceeds remaining size.
                log.info(
                    "[%s] EXIT_FILLED treated as full close | order=%s pos=%s delta=%s remaining_before=%s",
                    self.client_id, _local_id, _pos_id, _delta, _remaining_before,
                )
                self._call_exit_engine(
                    _ee,
                    "mark_position_closed",
                    _pos_id,
                    reason="EXIT_FILLED",
                    qty_filled=_delta,
                    fill_price=fill_price,
                    local_order_id=_local_id,
                    broker_order_id=_broker_id,
                    cumulative_filled=_cum_filled,
                )
                return

            if new_status in (OrderStatus.CANCELED, OrderStatus.EXPIRED, OrderStatus.REJECTED):
                if hasattr(_ee, "on_exit_failure"):
                    _ee.on_exit_failure(
                        _pos_id,
                        local_order_id=_local_id,
                        broker_order_id=_broker_id,
                        status=new_status,
                    )
                elif hasattr(_ee, "clear_exit_in_flight"):
                    _ee.clear_exit_in_flight(_pos_id)
                else:
                    log.critical(
                        "[%s] Exit failure hook missing on exit engine | order=%s pos=%s status=%s",
                        self.client_id, _local_id, _pos_id, new_status,
                    )
                if new_status == OrderStatus.REJECTED:
                    try:
                        import time as _time

                        for _p in getattr(_ee, "_positions", []):
                            if str(getattr(_p, "position_id", "")) == str(_pos_id):
                                _p.last_exit_rejected = True
                                _p.last_rejection_ts = _time.time()
                                log.info(
                                    "[%s] Exit REJECTED — 30s cooldown started",
                                    getattr(_p, "ticker", _pos_id),
                                )
                                break
                    except Exception:
                        pass
        except Exception as _ee_err:
            log.exception("[%s] exit_eng hook failed: %s", self.client_id, _ee_err)

    @staticmethod
    def _safe_int(value, default=0):
        try:
            if value is None:
                return default
            return int(value)
        except Exception:
            return default

    @staticmethod
    def _call_exit_engine(exit_engine, method_name: str, *args, **kwargs):
        """Strictly call an exit-engine hook.

        Do not filter kwargs or fall back to positional-only calls. Signature
        mismatches must surface because they can hide lost order identity or
        lost cumulative-fill attribution.
        """
        method = getattr(exit_engine, method_name, None)
        if not method:
            return None
        return method(*args, **kwargs)

    @staticmethod
    def _get_exit_engine_position(exit_engine, position_id: str):
        if not exit_engine or not position_id:
            return None
        getter = getattr(exit_engine, "get_position", None)
        if callable(getter):
            try:
                return getter(position_id)
            except Exception:
                pass
        for pos in getattr(exit_engine, "_positions", []) or []:
            if str(getattr(pos, "position_id", "")) == str(position_id):
                return pos
        return None

    def _get_position_remaining_from_db(self, position_id: str):
        """Best-effort fallback when exit engine is not seeded.

        Supports both older schema (qty only) and hardened schema
        (quantity_remaining). Returns None if unavailable.
        """
        if not position_id:
            return None
        try:
            def _fn():
                with conn() as c:
                    c.execute(
                        "SELECT * FROM positions WHERE client_id=%s AND id=%s LIMIT 1",
                        (self.client_id, position_id),
                    )
                    return c.fetchone()
            row = run_with_retry(_fn) or {}
            if not row:
                return None
            if row.get("quantity_remaining") is not None:
                return int(row.get("quantity_remaining") or 0)
            return int(row.get("qty") or 0)
        except Exception:
            return None

    def apply_fill_update(
        self,
        local_order_id: str,
        *,
        cumulative_filled: int,
        fill_price=None,
        broker_order_id=None,
    ) -> bool:
        """Apply a broker cumulative fill update without requiring a state change.

        Use this when an order is already PARTIAL_FILL / EXIT_PARTIAL_FILL and
        the broker reports a larger cumulative filled quantity for the same
        lifecycle state. The cumulative_filled value MUST be broker cumulative
        quantity for this order, not an incremental delta.
        """
        order = self._get_order(local_order_id)
        if not order:
            log.error("[%s] apply_fill_update: order %s not found", self.client_id, local_order_id)
            return False

        order = dict(order)
        status = str(order.get("status") or "")
        kind = str(order.get("kind") or "").upper()

        if status not in (OrderStatus.PARTIAL_FILL, OrderStatus.EXIT_PARTIAL_FILL):
            log.warning(
                "[%s] apply_fill_update blocked — order=%s status=%s is not partial-fill",
                self.client_id, local_order_id, status,
            )
            return False

        if status == OrderStatus.EXIT_PARTIAL_FILL and kind != "EXIT":
            log.critical(
                "[%s] apply_fill_update blocked — EXIT_PARTIAL_FILL on non-EXIT order=%s kind=%s",
                self.client_id, local_order_id, kind,
            )
            return False

        return self.transition(
            local_order_id,
            status,
            broker_order_id=broker_order_id or order.get("broker_order_id"),
            filled_qty=cumulative_filled,
            fill_price=fill_price,
        )

    # ---------------------------------------------------------------------
    # Public readers / mutators
    # ---------------------------------------------------------------------

    def increment_retry(self, local_order_id: str):
        def _fn():
            with conn() as c:
                c.execute(
                    "UPDATE orders SET retries=retries+1, updated_ts=NOW() WHERE local_order_id=%s AND client_id=%s",
                    (local_order_id, self.client_id),
                )

        run_with_retry(_fn)

    def get_order(self, local_order_id: str):
        return self._get_order(local_order_id)

    def get_orders_for_position(self, position_id: str) -> list:
        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT * FROM orders WHERE client_id=%s AND position_id=%s ORDER BY created_ts DESC",
                    (self.client_id, position_id),
                )
                return c.fetchall()

        return run_with_retry(_fn)

    def get_active_entry_orders(self) -> list:
        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT * FROM orders WHERE client_id=%s AND kind='ENTRY' "
                    "AND status IN ('CREATED','PENDING_TRIGGER','SUBMITTED','ACKNOWLEDGED','PARTIAL_FILL') "
                    "ORDER BY created_ts DESC",
                    (self.client_id,),
                )
                return c.fetchall()

        return run_with_retry(_fn)

    def get_active_exit_orders(self) -> list:
        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT * FROM orders WHERE client_id=%s AND kind='EXIT' "
                    "AND status IN ('EXIT_REQUESTED','EXIT_SUBMITTED','EXIT_ACKNOWLEDGED','EXIT_PARTIAL_FILL') "
                    "ORDER BY created_ts DESC",
                    (self.client_id,),
                )
                return c.fetchall()

        return run_with_retry(_fn)

    # ---------------------------------------------------------------------
    # Broker submit helpers
    # ---------------------------------------------------------------------

    def submit_existing_entry(self, *, local_order_id: str, broker, plan=None, limit_price=None) -> dict:
        """Submit an already-created ENTRY order after watcher breach.

        Money-safe invariant: broker accepts and returns a real broker_order_id
        first; only then does OSM transition CREATED/PENDING_TRIGGER -> SUBMITTED.
        """
        current = self._get_order(local_order_id)
        if not current:
            error_msg = "existing_entry_order_not_found"
            log.critical("[%s] submit_existing_entry failed — %s | %s", self.client_id, error_msg, local_order_id)
            return {"ok": False, "local_order_id": local_order_id, "broker_order_id": None, "status": OrderStatus.ERROR, "error": error_msg}

        current = dict(current)
        kind = str(current.get("kind") or "").upper()
        status = str(current.get("status") or "").upper()

        if kind != "ENTRY":
            error_msg = f"submit_existing_entry_wrong_kind:{kind}"
            self.transition(local_order_id, OrderStatus.ERROR, last_error=error_msg)
            return {"ok": False, "local_order_id": local_order_id, "broker_order_id": None, "status": OrderStatus.ERROR, "error": error_msg}

        if status in (OrderStatus.SUBMITTED, OrderStatus.ACKNOWLEDGED, OrderStatus.PARTIAL_FILL, OrderStatus.FILLED):
            return {"ok": True, "local_order_id": local_order_id, "broker_order_id": current.get("broker_order_id"), "status": status, "error": None}

        if status not in (OrderStatus.CREATED, OrderStatus.PENDING_TRIGGER):
            error_msg = f"submit_existing_entry_invalid_status:{status}"
            log.critical("[%s] submit_existing_entry blocked — %s | %s", self.client_id, error_msg, local_order_id)
            self.transition(local_order_id, OrderStatus.ERROR, last_error=error_msg)
            return {"ok": False, "local_order_id": local_order_id, "broker_order_id": None, "status": OrderStatus.ERROR, "error": error_msg}

        lp = float(limit_price or current.get("limit_price") or getattr(plan, "limit_price", 0) or 0)
        contract = current.get("contract") or getattr(plan, "contract_symbol", None) or current.get("symbol") or getattr(plan, "ticker", "")
        ticker = current.get("symbol") or getattr(plan, "ticker", "")
        qty = int(current.get("qty") or getattr(plan, "contracts", 0) or 0)

        if lp <= 0:
            error_msg = "invalid_existing_entry_limit_price"
            self.transition(local_order_id, OrderStatus.ERROR, last_error=error_msg)
            return {"ok": False, "local_order_id": local_order_id, "broker_order_id": None, "status": OrderStatus.ERROR, "error": error_msg}
        if qty <= 0:
            error_msg = "invalid_existing_entry_qty"
            self.transition(local_order_id, OrderStatus.ERROR, last_error=error_msg)
            return {"ok": False, "local_order_id": local_order_id, "broker_order_id": None, "status": OrderStatus.ERROR, "error": error_msg}
        if not contract:
            error_msg = "missing_existing_entry_contract"
            self.transition(local_order_id, OrderStatus.ERROR, last_error=error_msg)
            return {"ok": False, "local_order_id": local_order_id, "broker_order_id": None, "status": OrderStatus.ERROR, "error": error_msg}

        base_url = getattr(broker, "base_url", None) or getattr(getattr(broker, "cfg", None), "base_url", None) or "https://sandbox.tradier.com"
        account_id = getattr(broker, "account_id", None) or getattr(getattr(broker, "cfg", None), "account_id", None) or ""

        error_msg = broker_order_id = None
        try:
            resp = broker.session.post(
                f"{base_url}/v1/accounts/{account_id}/orders",
                data={
                    "class": "option",
                    "symbol": ticker,
                    "option_symbol": contract,
                    "side": "buy_to_open",
                    "quantity": qty,
                    "type": "limit",
                    "price": round(lp, 2),
                    "duration": "day",
                },
                headers={"Accept": "application/json"},
                timeout=10,
            )
            order = (resp.json() or {}).get("order") or {}
            broker_status = str(order.get("status") or "").lower().strip()
            broker_order_id = order.get("id") or order.get("order_id")

            if broker_status in ("ok", "pending", "open", "accepted", "filled") and broker_order_id:
                ok = self.transition(local_order_id, OrderStatus.SUBMITTED, broker_order_id=broker_order_id, submitted_ts=now_utc_iso())
                if ok:
                    return {"ok": True, "local_order_id": local_order_id, "broker_order_id": broker_order_id, "status": OrderStatus.SUBMITTED, "error": None}
                error_msg = "submitted_transition_failed_after_broker_accept"
            else:
                error_msg = f"broker_status:{broker_status or 'unknown'} broker_order_id_missing:{not bool(broker_order_id)}"
        except Exception as e:
            error_msg = f"broker_error:{e}"

        self.transition(local_order_id, OrderStatus.ERROR, last_error=error_msg or "unknown_error")
        return {"ok": False, "local_order_id": local_order_id, "broker_order_id": broker_order_id, "status": OrderStatus.ERROR, "error": error_msg}

    def submit_entry(self, *, broker, plan, limit_price=None, reserved_cost=None) -> dict:
        local_id = self.create_entry_order(plan, limit_price=limit_price, reserved_cost=reserved_cost)
        lp = float(limit_price or getattr(plan, "limit_price", 0) or 0)
        symbol = getattr(plan, "contract_symbol", None) or plan.ticker

        if lp <= 0:
            error_msg = "invalid_entry_limit_price"
            self.transition(local_id, OrderStatus.ERROR, last_error=error_msg)
            return {"ok": False, "local_order_id": local_id, "broker_order_id": None, "status": OrderStatus.ERROR, "error": error_msg}

        base_url = getattr(broker, "base_url", None) or getattr(getattr(broker, "cfg", None), "base_url", None) or "https://sandbox.tradier.com"
        account_id = getattr(broker, "account_id", None) or getattr(getattr(broker, "cfg", None), "account_id", None) or ""

        error_msg = broker_order_id = None
        try:
            resp = broker.session.post(
                f"{base_url}/v1/accounts/{account_id}/orders",
                data={
                    "class": "option",
                    "symbol": plan.ticker,
                    "option_symbol": symbol,
                    "side": "buy_to_open",
                    "quantity": int(plan.contracts),
                    "type": "limit",
                    "price": round(lp, 2),
                    "duration": "day",
                },
                headers={"Accept": "application/json"},
                timeout=10,
            )
            order = (resp.json() or {}).get("order") or {}
            status = (order.get("status") or "").lower()
            broker_order_id = order.get("id") or order.get("order_id")
            if status in ("ok", "pending", "open", "accepted", "filled") and broker_order_id:
                ok = self.transition(local_id, OrderStatus.SUBMITTED, broker_order_id=broker_order_id, submitted_ts=now_utc_iso())
                if ok:
                    return {"ok": True, "local_order_id": local_id, "broker_order_id": broker_order_id, "status": OrderStatus.SUBMITTED, "error": None}
                error_msg = "submitted_transition_failed_after_broker_accept"
            else:
                error_msg = f"broker_status:{status or 'unknown'} broker_order_id_missing:{not bool(broker_order_id)}"
        except Exception as e:
            error_msg = f"broker_error:{e}"

        self.transition(local_id, OrderStatus.ERROR, last_error=error_msg or "unknown_error")
        return {"ok": False, "local_order_id": local_id, "broker_order_id": broker_order_id, "status": OrderStatus.ERROR, "error": error_msg}

    def submit_exit(
        self,
        *,
        broker,
        position_id,
        contract,
        symbol,
        direction,
        qty,
        limit_price,
        plan_id=None,
        signal_id=None,
    ) -> dict:
        local_id = self.create_exit_order(
            position_id=position_id,
            contract=contract,
            symbol=symbol,
            direction=direction,
            qty=qty,
            plan_id=plan_id,
            signal_id=signal_id,
            limit_price=limit_price,
        )
        lp = float(limit_price or 0)
        if lp <= 0:
            error_msg = "invalid_exit_limit_price"
            self.transition(local_id, OrderStatus.ERROR, last_error=error_msg)
            return {"ok": False, "local_order_id": local_id, "broker_order_id": None, "status": OrderStatus.ERROR, "error": error_msg}

        base_url = getattr(broker, "base_url", None) or getattr(getattr(broker, "cfg", None), "base_url", None) or "https://sandbox.tradier.com"
        account_id = getattr(broker, "account_id", None) or getattr(getattr(broker, "cfg", None), "account_id", None) or ""
        underlying = self._resolve_underlying_symbol(symbol=symbol, contract=contract)

        error_msg = broker_order_id = None
        try:
            resp = broker.session.post(
                f"{base_url}/v1/accounts/{account_id}/orders",
                data={
                    "class": "option",
                    "symbol": underlying,
                    "option_symbol": contract,
                    "side": "sell_to_close",
                    "quantity": int(qty),
                    "type": "limit",
                    "price": round(lp, 2),
                    "duration": "day",
                },
                headers={"Accept": "application/json"},
                timeout=10,
            )
            order = (resp.json() or {}).get("order") or {}
            status = (order.get("status") or "").lower()
            broker_order_id = order.get("id") or order.get("order_id")
            if status in ("ok", "pending", "open", "accepted", "filled") and broker_order_id:
                ok = self.transition(local_id, OrderStatus.EXIT_SUBMITTED, broker_order_id=broker_order_id, submitted_ts=now_utc_iso())
                if ok:
                    return {"ok": True, "local_order_id": local_id, "broker_order_id": broker_order_id, "status": OrderStatus.EXIT_SUBMITTED, "error": None}
                error_msg = "exit_submitted_transition_failed_after_broker_accept"
            else:
                error_msg = f"broker_status:{status or 'unknown'} broker_order_id_missing:{not bool(broker_order_id)}"
        except Exception as e:
            error_msg = f"broker_error:{e}"

        self.transition(local_id, OrderStatus.ERROR, last_error=error_msg or "unknown_error")
        return {"ok": False, "local_order_id": local_id, "broker_order_id": broker_order_id, "status": OrderStatus.ERROR, "error": error_msg}

    @staticmethod
    def _resolve_underlying_symbol(*, symbol: str, contract: str) -> str:
        """Resolve Tradier order `symbol` safely from underlying/contract fields."""
        raw_symbol = (symbol or "").strip().upper()
        raw_contract = (contract or "").strip().upper()
        if raw_symbol and not any(ch.isdigit() for ch in raw_symbol):
            return raw_symbol
        # OCC style option symbols start with underlying letters before YYMMDD.
        import re

        m = re.match(r"^([A-Z]{1,10})\d{6}[CP]", raw_contract or raw_symbol)
        if m:
            return m.group(1)
        return raw_symbol if raw_symbol else raw_contract

    # ---------------------------------------------------------------------
    # Internal DB helpers
    # ---------------------------------------------------------------------

    def _get_order(self, local_order_id: str):
        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT * FROM orders WHERE local_order_id=%s AND client_id=%s",
                    (local_order_id, self.client_id),
                )
                return c.fetchone()

        return run_with_retry(_fn)

    def _get_order_by_plan(self, plan_id: str, kind: str = "ENTRY"):
        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT * FROM orders WHERE client_id=%s AND plan_id=%s AND kind=%s "
                    "AND status NOT IN ( 'FILLED', 'EXIT_FILLED', 'REJECTED', 'CANCELED', 'EXPIRED', 'ERROR' ) "
                    "ORDER BY created_ts DESC LIMIT 1",
                    (self.client_id, plan_id, kind),
                )
                return c.fetchone()

        return run_with_retry(_fn)

    def _get_active_exit_order(self, position_id: str):
        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT * FROM orders WHERE client_id=%s AND position_id=%s AND kind='EXIT' "
                    "AND status NOT IN ('EXIT_FILLED', 'REJECTED', 'CANCELED', 'EXPIRED', 'ERROR') "
                    "ORDER BY created_ts DESC LIMIT 1",
                    (self.client_id, position_id),
                )
                return c.fetchone()

        return run_with_retry(_fn)

    def _record_error(self, local_order_id: str, error_msg: str):
        def _fn():
            with conn() as c:
                c.execute(
                    "UPDATE orders SET last_error=%s, updated_ts=NOW() WHERE local_order_id=%s AND client_id=%s",
                    (error_msg, local_order_id, self.client_id),
                )

        try:
            run_with_retry(_fn)
        except Exception:
            pass
