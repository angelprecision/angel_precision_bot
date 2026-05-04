# ap/order_state_machine.py — APOrderStateMachine
# =============================================================================
# Enforced order lifecycle controller.
# =============================================================================

from __future__ import annotations

import threading
import uuid
import logging
from typing import Optional

from ap.db import conn, run_with_retry
from ap.utils import now_utc_iso

log = logging.getLogger("ap.order_state_machine")

_exit_engine_registry: dict[str, object] = {}
_registry_lock = threading.Lock()

def register_exit_engine(client_id: str, exit_engine) -> None:
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
    CREATED         = "CREATED"
    PENDING_TRIGGER = "PENDING_TRIGGER"   # ← NEW: watcher armed, no broker order yet
    SUBMITTED       = "SUBMITTED"
    ACKNOWLEDGED    = "ACKNOWLEDGED"
    PARTIAL_FILL    = "PARTIAL_FILL"
    FILLED          = "FILLED"

    # Exit states
    EXIT_REQUESTED    = "EXIT_REQUESTED"
    EXIT_SUBMITTED    = "EXIT_SUBMITTED"
    EXIT_ACKNOWLEDGED = "EXIT_ACKNOWLEDGED"
    EXIT_PARTIAL_FILL = "EXIT_PARTIAL_FILL"
    EXIT_FILLED       = "EXIT_FILLED"

    # Terminal failure states
    REJECTED = "REJECTED"
    CANCELED = "CANCELED"
    EXPIRED  = "EXPIRED"
    ERROR    = "ERROR"

    # State sets
    ENTRY_ACTIVE = {CREATED, PENDING_TRIGGER, SUBMITTED, ACKNOWLEDGED, PARTIAL_FILL}
    EXIT_ACTIVE  = {EXIT_REQUESTED, EXIT_SUBMITTED, EXIT_ACKNOWLEDGED, EXIT_PARTIAL_FILL}
    TERMINAL     = {FILLED, EXIT_FILLED, REJECTED, CANCELED, EXPIRED, ERROR}
    ACTIVE       = ENTRY_ACTIVE | EXIT_ACTIVE

    # Legal transitions
    TRANSITIONS: dict[str, set[str]] = {
        CREATED:          {PENDING_TRIGGER, SUBMITTED, ERROR, CANCELED},
        PENDING_TRIGGER:  {SUBMITTED, ERROR, CANCELED},
        SUBMITTED:        {ACKNOWLEDGED, PARTIAL_FILL, FILLED,
                           REJECTED, EXPIRED, ERROR, CANCELED},
        ACKNOWLEDGED:     {PARTIAL_FILL, FILLED,
                           REJECTED, EXPIRED, ERROR, CANCELED},
        PARTIAL_FILL:     {FILLED, REJECTED, EXPIRED, ERROR, CANCELED},
        EXIT_REQUESTED:   {EXIT_SUBMITTED, ERROR, CANCELED},
        EXIT_SUBMITTED:   {EXIT_ACKNOWLEDGED, EXIT_PARTIAL_FILL, EXIT_FILLED,
                           REJECTED, ERROR, CANCELED},
        EXIT_ACKNOWLEDGED:{EXIT_PARTIAL_FILL, EXIT_FILLED,
                           REJECTED, ERROR, CANCELED},
        EXIT_PARTIAL_FILL:{EXIT_FILLED, REJECTED, ERROR, CANCELED},
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


PENDING_ENTRY_STATUSES = ("CREATED", "PENDING_TRIGGER", "SUBMITTED", "ACKNOWLEDGED", "PARTIAL_FILL")
PENDING_EXIT_STATUSES  = ("EXIT_REQUESTED", "EXIT_SUBMITTED",
                           "EXIT_ACKNOWLEDGED", "EXIT_PARTIAL_FILL")


class APOrderStateMachine:

    def __init__(self, client_id: str):
        self.client_id = client_id
        log.info(f"[{client_id}] APOrderStateMachine initialized")

    def create_entry_order(self, plan, *, limit_price=None, reserved_cost=None) -> str:
        existing = self._get_order_by_plan(plan.plan_id, kind="ENTRY")
        if existing:
            log.warning(f"[{self.client_id}] create_entry_order SKIPPED — plan {plan.plan_id} already has order {existing['local_order_id']}")
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
                    (local_order_id, self.client_id, plan.plan_id, plan.signal_id,
                     plan.ticker, contract, plan.side.upper(),
                     int(plan.contracts), lp, rc, ts, ts),
                )
        run_with_retry(_fn)
        log.info(f"[{self.client_id}] ORDER CREATED (entry) | {contract} x{plan.contracts} | local_order_id={local_order_id}")
        return local_order_id

    def create_exit_order(self, *, position_id, contract, symbol, direction, qty,
                          local_order_id=None, plan_id=None, signal_id=None,
                          limit_price=None, reserved_cost=None) -> str:
        existing = self._get_active_exit_order(position_id)
        if existing:
            log.warning(f"[{self.client_id}] create_exit_order SKIPPED — position {position_id} already has exit order {existing['local_order_id']}")
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
                    (local_id, self.client_id, position_id, plan_id, signal_id,
                     symbol.upper(), contract, direction.upper(),
                     int(qty),
                     float(limit_price) if limit_price else None,
                     float(reserved_cost) if reserved_cost else None,
                     ts, ts),
                )
        run_with_retry(_fn)
        log.info(f"[{self.client_id}] ORDER CREATED (exit) | {contract} x{qty} pos={position_id} | local_order_id={local_id}")
        return local_id

    def transition(self, local_order_id: str, new_status: str, *,
                   broker_order_id=None, filled_qty=None, fill_price=None,
                   last_error=None, submitted_ts=None, filled_ts=None,
                   position_id=None) -> bool:
        current = self._get_order(local_order_id)
        if not current:
            log.error(f"[{self.client_id}] transition: order {local_order_id} not found")
            return False

        old_status = current.get("status", "")

        if old_status == new_status:
            log.debug(f"[{self.client_id}] {local_order_id} already {new_status} — no-op")
            return True

        if OrderStatus.is_terminal(old_status):
            log.warning(f"[{self.client_id}] TRANSITION BLOCKED — {local_order_id} already terminal ({old_status}), cannot move to {new_status}")
            return False

        if not OrderStatus.can_transition(old_status, new_status):
            log.error(f"[{self.client_id}] ILLEGAL TRANSITION — {local_order_id}: {old_status} → {new_status}")
            self._record_error(local_order_id, f"illegal_transition:{old_status}→{new_status}")
            return False

        updates = ["status=%s", "updated_ts=NOW()"]
        params  = [new_status]

        if broker_order_id:
            updates.append("broker_order_id=%s"); params.append(broker_order_id)
        if filled_qty is not None:
            updates.append("filled_qty=%s");      params.append(int(filled_qty))
        if fill_price is not None:
            updates.append("fill_price=%s");      params.append(float(fill_price))
        if last_error:
            updates.append("last_error=%s");      params.append(last_error)
        if submitted_ts:
            updates.append("submitted_ts=%s");    params.append(submitted_ts)
        if position_id:
            updates.append("position_id=%s");     params.append(position_id)
        if new_status in (OrderStatus.FILLED, OrderStatus.EXIT_FILLED):
            updates.append("filled_ts=%s")
            params.append(filled_ts or now_utc_iso())

        params.extend([local_order_id, self.client_id])
        sql = f"UPDATE orders SET {', '.join(updates)} WHERE local_order_id=%s AND client_id=%s"

        def _fn():
            with conn() as c:
                c.execute(sql, tuple(params))
        run_with_retry(_fn)

        log.info(
            f"[{self.client_id}] ORDER {old_status} → {new_status} | {local_order_id}"
            + (f" broker={broker_order_id}" if broker_order_id else "")
            + (f" fill={filled_qty}@{fill_price}" if fill_price else "")
        )

        if new_status in (OrderStatus.EXIT_FILLED, OrderStatus.EXIT_PARTIAL_FILL,
                          OrderStatus.CANCELED, OrderStatus.EXPIRED, OrderStatus.REJECTED):
            _pos_id = position_id or current.get("position_id")
            _kind   = current.get("kind", "ENTRY")
            if _pos_id and _kind == "EXIT":
                try:
                    _ee = _get_exit_engine_for_client(self.client_id)
                    if _ee:
                        if new_status == OrderStatus.EXIT_FILLED:
                            _ee.mark_position_closed(_pos_id, reason="EXIT_FILLED")
                        elif new_status == OrderStatus.EXIT_PARTIAL_FILL and filled_qty is not None:
                            prev_filled_qty = int(current.get("filled_qty") or 0)
                            delta = max(0, int(filled_qty) - prev_filled_qty)
                            if delta > 0:
                                _ee.note_partial_exit_fill(_pos_id, delta)
                        elif new_status in (OrderStatus.CANCELED, OrderStatus.EXPIRED,
                                            OrderStatus.REJECTED):
                            _ee.clear_exit_in_flight(_pos_id)
                            if new_status == OrderStatus.REJECTED:
                                try:
                                    import time as _time
                                    for _p in _ee._positions:
                                        if str(getattr(_p, "position_id", "")) == str(_pos_id):
                                            _p.last_exit_rejected = True
                                            _p.last_rejection_ts  = _time.time()
                                            log.info("[%s] Exit REJECTED — 30s cooldown started", getattr(_p, "ticker", _pos_id))
                                            break
                                except Exception:
                                    pass
                except Exception as _ee_err:
                    log.debug("[%s] exit_eng hook (non-critical): %s", self.client_id, _ee_err)

        return True

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

    def submit_entry(self, *, broker, plan, limit_price=None, reserved_cost=None) -> dict:
        local_id = self.create_entry_order(plan, limit_price=limit_price, reserved_cost=reserved_cost)
        lp = float(limit_price or getattr(plan, "limit_price", 0) or 0)
        symbol = getattr(plan, "contract_symbol", None) or plan.ticker
        base_url   = (getattr(broker, "base_url", None) or getattr(getattr(broker, "cfg", None), "base_url", None) or "https://sandbox.tradier.com")
        account_id = (getattr(broker, "account_id", None) or getattr(getattr(broker, "cfg", None), "account_id", None) or "")

        error_msg = broker_order_id = None
        try:
            resp = broker.session.post(
                f"{base_url}/v1/accounts/{account_id}/orders",
                data={"class": "option", "symbol": plan.ticker, "option_symbol": symbol,
                      "side": "buy_to_open", "quantity": int(plan.contracts),
                      "type": "limit", "price": round(lp, 2), "duration": "day"},
                headers={"Accept": "application/json"}, timeout=10,
            )
            order = (resp.json() or {}).get("order") or {}
            status = (order.get("status") or "").lower()
            broker_order_id = order.get("id") or order.get("order_id")
            if status in ("ok", "pending", "open", "accepted"):
                self.transition(local_id, OrderStatus.SUBMITTED, broker_order_id=broker_order_id, submitted_ts=now_utc_iso())
                return {"ok": True, "local_order_id": local_id, "broker_order_id": broker_order_id, "status": OrderStatus.SUBMITTED, "error": None}
            else:
                error_msg = f"broker_status:{status or 'unknown'}"
        except Exception as e:
            error_msg = f"broker_error:{e}"

        self.transition(local_id, OrderStatus.ERROR, last_error=error_msg or "unknown_error")
        return {"ok": False, "local_order_id": local_id, "broker_order_id": broker_order_id, "status": OrderStatus.ERROR, "error": error_msg}

    def submit_exit(self, *, broker, position_id, contract, symbol, direction, qty,
                    limit_price, plan_id=None, signal_id=None) -> dict:
        local_id = self.create_exit_order(position_id=position_id, contract=contract,
                                           symbol=symbol, direction=direction, qty=qty,
                                           plan_id=plan_id, signal_id=signal_id, limit_price=limit_price)
        base_url   = (getattr(broker, "base_url", None) or getattr(getattr(broker, "cfg", None), "base_url", None) or "https://sandbox.tradier.com")
        account_id = (getattr(broker, "account_id", None) or getattr(getattr(broker, "cfg", None), "account_id", None) or "")
        underlying = symbol.split()[0] if " " in symbol else symbol[:6]

        error_msg = broker_order_id = None
        try:
            resp = broker.session.post(
                f"{base_url}/v1/accounts/{account_id}/orders",
                data={"class": "option", "symbol": underlying, "option_symbol": contract,
                      "side": "sell_to_close", "quantity": int(qty),
                      "type": "limit", "price": round(limit_price, 2), "duration": "day"},
                headers={"Accept": "application/json"}, timeout=10,
            )
            order = (resp.json() or {}).get("order") or {}
            status = (order.get("status") or "").lower()
            broker_order_id = order.get("id") or order.get("order_id")
            if status in ("ok", "pending", "open", "accepted"):
                self.transition(local_id, OrderStatus.EXIT_SUBMITTED, broker_order_id=broker_order_id, submitted_ts=now_utc_iso())
                return {"ok": True, "local_order_id": local_id, "broker_order_id": broker_order_id, "status": OrderStatus.EXIT_SUBMITTED, "error": None}
            else:
                error_msg = f"broker_status:{status or 'unknown'}"
        except Exception as e:
            error_msg = f"broker_error:{e}"

        self.transition(local_id, OrderStatus.ERROR, last_error=error_msg or "unknown_error")
        return {"ok": False, "local_order_id": local_id, "broker_order_id": broker_order_id, "status": OrderStatus.ERROR, "error": error_msg}

    def _get_order(self, local_order_id: str):
        def _fn():
            with conn() as c:
                c.execute("SELECT * FROM orders WHERE local_order_id=%s AND client_id=%s", (local_order_id, self.client_id))
                return c.fetchone()
        return run_with_retry(_fn)

    def _get_order_by_plan(self, plan_id: str, kind: str = "ENTRY"):
        def _fn():
            with conn() as c:
                c.execute("SELECT * FROM orders WHERE client_id=%s AND plan_id=%s AND kind=%s LIMIT 1", (self.client_id, plan_id, kind))
                return c.fetchone()
        return run_with_retry(_fn)

    def _get_active_exit_order(self, position_id: str):
        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT * FROM orders WHERE client_id=%s AND position_id=%s AND kind='EXIT' "
                    "AND status NOT IN ('EXIT_FILLED','REJECTED','CANCELED','EXPIRED','ERROR') LIMIT 1",
                    (self.client_id, position_id),
                )
                return c.fetchone()
        return run_with_retry(_fn)

    def _record_error(self, local_order_id: str, error_msg: str):
        def _fn():
            with conn() as c:
                c.execute("UPDATE orders SET last_error=%s, updated_ts=NOW() WHERE local_order_id=%s AND client_id=%s",
                          (error_msg, local_order_id, self.client_id))
        try:
            run_with_retry(_fn)
        except Exception:
            pass
