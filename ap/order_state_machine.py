# ap/order_state_machine.py — APOrderStateMachine
# =============================================================================
# Enforced order lifecycle controller.
# Reconciled to EXACT orders table schema:
#
#   id              BIGSERIAL PRIMARY KEY        ← internal DB id (integer)
#   local_order_id  TEXT UNIQUE                  ← our UUID, used for lookups
#   broker_order_id TEXT                         ← Tradier order id
#   client_id       TEXT
#   position_id     TEXT                         ← set after position opens
#   plan_id         TEXT
#   signal_id       TEXT
#   kind            TEXT  ('ENTRY' | 'EXIT')
#   status          TEXT
#   symbol          TEXT  (underlying ticker)
#   contract        TEXT  (option symbol)
#   direction       TEXT  ('CALL' | 'PUT')
#   reserved_cost   NUMERIC
#   qty             INTEGER
#   limit_price     NUMERIC
#   filled_qty      INTEGER DEFAULT 0
#   fill_price      NUMERIC                      ← avg fill (NOT avg_fill_price)
#   retries         INTEGER DEFAULT 0
#   last_error      TEXT
#   created_ts      TIMESTAMPTZ                  ← NOT created_at
#   updated_ts      TIMESTAMPTZ                  ← NOT updated_at
#   submitted_ts    TIMESTAMPTZ
#   filled_ts       TIMESTAMPTZ
#
# Entry lifecycle:
#   CREATED → SUBMITTED → ACKNOWLEDGED → PARTIAL_FILL → FILLED
#   Any active → REJECTED | EXPIRED | CANCELED | ERROR
#
# Exit lifecycle:
#   EXIT_REQUESTED → EXIT_SUBMITTED → EXIT_ACKNOWLEDGED
#   → EXIT_PARTIAL_FILL → EXIT_FILLED
# =============================================================================

from __future__ import annotations

import threading
import uuid
import logging
from typing import Optional

from ap.db import conn, run_with_retry
from ap.utils import now_utc_iso

log = logging.getLogger("ap.order_state_machine")

# Registry so OSM can notify the right exit engine per client
_exit_engine_registry: dict[str, object] = {}
_registry_lock = threading.Lock()

def register_exit_engine(client_id: str, exit_engine) -> None:
    """Called by ClientRunner to register the exit engine for this client."""
    with _registry_lock:
        _exit_engine_registry[client_id] = exit_engine

def unregister_exit_engine(client_id: str) -> None:
    """Called in ClientRunner.finally to prevent stale registry references."""
    with _registry_lock:
        _exit_engine_registry.pop(client_id, None)

def _get_exit_engine_for_client(client_id: str):
    with _registry_lock:
        return _exit_engine_registry.get(client_id)


# =============================================================================
# STATUS CONSTANTS + LEGAL TRANSITIONS
# =============================================================================

class OrderStatus:
    # Entry states
    CREATED       = "CREATED"
    SUBMITTED     = "SUBMITTED"
    ACKNOWLEDGED  = "ACKNOWLEDGED"
    PARTIAL_FILL  = "PARTIAL_FILL"
    FILLED        = "FILLED"

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
    ENTRY_ACTIVE = {CREATED, SUBMITTED, ACKNOWLEDGED, PARTIAL_FILL}
    EXIT_ACTIVE  = {EXIT_REQUESTED, EXIT_SUBMITTED, EXIT_ACKNOWLEDGED, EXIT_PARTIAL_FILL}
    TERMINAL     = {FILLED, EXIT_FILLED, REJECTED, CANCELED, EXPIRED, ERROR}
    ACTIVE       = ENTRY_ACTIVE | EXIT_ACTIVE

    # Legal transitions — anything not in this map is ILLEGAL
    TRANSITIONS: dict[str, set[str]] = {
        CREATED:          {SUBMITTED, ERROR, CANCELED},
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


# Pending entry statuses for position manager queries
PENDING_ENTRY_STATUSES = ("CREATED", "SUBMITTED", "ACKNOWLEDGED", "PARTIAL_FILL")
PENDING_EXIT_STATUSES  = ("EXIT_REQUESTED", "EXIT_SUBMITTED",
                           "EXIT_ACKNOWLEDGED", "EXIT_PARTIAL_FILL")


# =============================================================================
# ORDER STATE MACHINE
# =============================================================================

class APOrderStateMachine:
    """
    Single controller for all order lifecycle transitions.
    All reads/writes use local_order_id (UUID) as the external key.
    The DB id (BIGSERIAL) is internal only.

    Usage:
        osm = APOrderStateMachine(client_id="tradefluencehq@gmail.com")

        # Create entry order
        local_id = osm.create_entry_order(plan)

        # Advance as broker events arrive
        osm.transition(local_id, OrderStatus.SUBMITTED,
                       broker_order_id="TDR-12345")
        osm.transition(local_id, OrderStatus.FILLED,
                       filled_qty=1, fill_price=3.50)

        # Create exit order
        exit_id = osm.create_exit_order(
            local_order_id=str(uuid.uuid4()),
            position_id="...", contract="AAPL...",
            symbol="AAPL", direction="CALL", qty=1
        )
        osm.transition(exit_id, OrderStatus.EXIT_SUBMITTED,
                       broker_order_id="TDR-99999")
        osm.transition(exit_id, OrderStatus.EXIT_FILLED,
                       filled_qty=1, fill_price=4.20)
    """

    def __init__(self, client_id: str):
        self.client_id = client_id
        log.info(f"[{client_id}] APOrderStateMachine initialized")

    # =========================================================================
    # CREATE ORDERS
    # =========================================================================

    def create_entry_order(
        self,
        plan,
        *,
        limit_price:   Optional[float] = None,
        reserved_cost: Optional[float] = None,
    ) -> str:
        """
        Insert a CREATED entry order from an ApprovedExecutionPlan.
        Returns local_order_id (UUID string).

        Dedup guard: if plan_id already has an ENTRY order, return existing
        local_order_id — do NOT create a duplicate.
        """
        existing = self._get_order_by_plan(plan.plan_id, kind="ENTRY")
        if existing:
            log.warning(
                f"[{self.client_id}] create_entry_order SKIPPED — "
                f"plan {plan.plan_id} already has order {existing['local_order_id']}"
            )
            return existing["local_order_id"]

        local_order_id = str(uuid.uuid4())
        contract = getattr(plan, "contract_symbol", None) or plan.ticker
        lp = float(limit_price) if limit_price else (
             float(plan.limit_price) if getattr(plan, "limit_price", None) else None)
        rc = float(reserved_cost) if reserved_cost else (
             float(plan.max_position_usd) if getattr(plan, "max_position_usd", None) else None)
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
                        local_order_id, self.client_id, plan.plan_id, plan.signal_id,
                        plan.ticker, contract, plan.side.upper(),
                        int(plan.contracts), lp, rc,
                        ts, ts,
                    ),
                )
        run_with_retry(_fn)
        log.info(
            f"[{self.client_id}] ORDER CREATED (entry) | {contract} "
            f"x{plan.contracts} | local_order_id={local_order_id}"
        )
        return local_order_id

    def create_exit_order(
        self,
        *,
        position_id:     str,
        contract:        str,
        symbol:          str,
        direction:       str,
        qty:             int,
        local_order_id:  Optional[str] = None,
        plan_id:         Optional[str] = None,
        signal_id:       Optional[str] = None,
        limit_price:     Optional[float] = None,
        reserved_cost:   Optional[float] = None,
    ) -> str:
        """
        Insert an EXIT_REQUESTED exit order.
        Dedup guard: if position already has an active exit order,
        return existing local_order_id.
        Returns local_order_id.
        """
        existing = self._get_active_exit_order(position_id)
        if existing:
            log.warning(
                f"[{self.client_id}] create_exit_order SKIPPED — "
                f"position {position_id} already has exit order "
                f"{existing['local_order_id']}"
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
                        local_id, self.client_id, position_id, plan_id, signal_id,
                        symbol.upper(), contract, direction.upper(),
                        int(qty),
                        float(limit_price) if limit_price else None,
                        float(reserved_cost) if reserved_cost else None,
                        ts, ts,
                    ),
                )
        run_with_retry(_fn)
        log.info(
            f"[{self.client_id}] ORDER CREATED (exit) | {contract} "
            f"x{qty} pos={position_id} | local_order_id={local_id}"
        )
        return local_id

    # =========================================================================
    # TRANSITION — THE ONLY WAY TO CHANGE ORDER STATUS
    # =========================================================================

    def transition(
        self,
        local_order_id:  str,
        new_status:      str,
        *,
        broker_order_id: Optional[str]   = None,
        filled_qty:      Optional[int]   = None,
        fill_price:      Optional[float] = None,   # schema: fill_price not avg_fill_price
        last_error:      Optional[str]   = None,
        submitted_ts:    Optional[str]   = None,
        filled_ts:       Optional[str]   = None,
        position_id:     Optional[str]   = None,   # set on FILLED to link position
    ) -> bool:
        """
        Advance an order to new_status.
        Returns True if applied, False if rejected (illegal or terminal).

        Rules:
        - Illegal transition → rejected, error recorded
        - Already terminal → blocked
        - Same status → no-op (idempotent, returns True)
        """
        current = self._get_order(local_order_id)
        if not current:
            log.error(
                f"[{self.client_id}] transition: order {local_order_id} not found"
            )
            return False

        old_status = current.get("status", "")

        # Idempotent — already there
        if old_status == new_status:
            log.debug(f"[{self.client_id}] {local_order_id} already {new_status} — no-op")
            return True

        # Already terminal
        if OrderStatus.is_terminal(old_status):
            log.warning(
                f"[{self.client_id}] TRANSITION BLOCKED — "
                f"{local_order_id} already terminal ({old_status}), "
                f"cannot move to {new_status}"
            )
            return False

        # Illegal transition
        if not OrderStatus.can_transition(old_status, new_status):
            log.error(
                f"[{self.client_id}] ILLEGAL TRANSITION — "
                f"{local_order_id}: {old_status} → {new_status}"
            )
            self._record_error(
                local_order_id,
                f"illegal_transition:{old_status}→{new_status}"
            )
            return False

        # Build update — use exact schema column names
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
        sql = (
            f"UPDATE orders SET {', '.join(updates)} "
            f"WHERE local_order_id=%s AND client_id=%s"
        )

        def _fn():
            with conn() as c:
                c.execute(sql, tuple(params))

        run_with_retry(_fn)
        log.info(
            f"[{self.client_id}] ORDER {old_status} → {new_status} "
            f"| {local_order_id}"
            + (f" broker={broker_order_id}" if broker_order_id else "")
            + (f" fill={filled_qty}@{fill_price}" if fill_price else "")
        )

        # ── Notify exit engine of confirmed order state changes ───────────────
        # This is the glue that makes exit engine lifecycle honest:
        # it learns about fills/cancels from here, not from its own assumptions.
        if new_status in (
            OrderStatus.EXIT_FILLED, OrderStatus.EXIT_PARTIAL_FILL,
            OrderStatus.CANCELED, OrderStatus.EXPIRED, OrderStatus.REJECTED
        ):
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
                            # Flag rejected exits so exit engine won't blindly retry
                            if new_status == OrderStatus.REJECTED:
                                try:
                                    import time as _time
                                    for _p in _ee._positions:
                                        if str(getattr(_p, "position_id", "")) == str(_pos_id):
                                            _p.last_exit_rejected = True
                                            _p.last_rejection_ts  = _time.time()  # start 30s cooldown
                                            log.info(
                                                "[%s] Exit REJECTED — 30s cooldown started",
                                                getattr(_p, "ticker", _pos_id)
                                            )
                                            break
                                except Exception:
                                    pass
                except Exception as _ee_err:
                    log.debug("[%s] exit_eng hook (non-critical): %s", self.client_id, _ee_err)

        return True

    # =========================================================================
    # INCREMENT RETRY
    # =========================================================================

    def increment_retry(self, local_order_id: str):
        """Increment retries counter — called when resubmitting after failure."""
        def _fn():
            with conn() as c:
                c.execute(
                    "UPDATE orders SET retries=retries+1, updated_ts=NOW() "
                    "WHERE local_order_id=%s AND client_id=%s",
                    (local_order_id, self.client_id),
                )
        run_with_retry(_fn)

    # =========================================================================
    # READ HELPERS
    # =========================================================================

    def get_order(self, local_order_id: str) -> Optional[dict]:
        return self._get_order(local_order_id)

    def get_orders_for_position(self, position_id: str) -> list[dict]:
        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT * FROM orders "
                    "WHERE client_id=%s AND position_id=%s "
                    "ORDER BY created_ts DESC",
                    (self.client_id, position_id),
                )
                return c.fetchall()
        return run_with_retry(_fn)

    def get_active_entry_orders(self) -> list[dict]:
        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT * FROM orders WHERE client_id=%s AND kind='ENTRY' "
                    "AND status IN ('CREATED','SUBMITTED','ACKNOWLEDGED','PARTIAL_FILL') "
                    "ORDER BY created_ts DESC",
                    (self.client_id,),
                )
                return c.fetchall()
        return run_with_retry(_fn)

    def get_active_exit_orders(self) -> list[dict]:
        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT * FROM orders WHERE client_id=%s AND kind='EXIT' "
                    "AND status IN ('EXIT_REQUESTED','EXIT_SUBMITTED',"
                    "'EXIT_ACKNOWLEDGED','EXIT_PARTIAL_FILL') "
                    "ORDER BY created_ts DESC",
                    (self.client_id,),
                )
                return c.fetchall()
        return run_with_retry(_fn)

    # =========================================================================
    # HIGH-LEVEL SUBMISSION HELPERS (BROKER + TRANSITION)
    # =========================================================================

    def submit_entry(
        self,
        *,
        broker,
        plan,
        limit_price:   Optional[float] = None,
        reserved_cost: Optional[float] = None,
    ) -> dict:
        """
        Create an ENTRY order row for plan and submit it to broker.
        Returns:
            {
              "ok": bool,
              "local_order_id": str,
              "broker_order_id": Optional[str],
              "status": str,          # SUBMITTED or ERROR
              "error": Optional[str]
            }

        Semantics:
        - If an order for this plan already exists, reuse it (idempotent).
        - On broker accepted, transition CREATED -> SUBMITTED + broker_order_id.
        - On broker failure, transition CREATED -> ERROR + last_error.
        - "ok/pending/open/accepted" from Tradier = accepted, NOT filled.
        """
        local_id = self.create_entry_order(
            plan,
            limit_price=limit_price,
            reserved_cost=reserved_cost,
        )

        lp = float(limit_price or getattr(plan, "limit_price", 0) or 0)
        symbol = getattr(plan, "contract_symbol", None) or plan.ticker

        # Resolve broker URL + account the same way execution_core does
        base_url   = (
            getattr(broker, "base_url", None)
            or getattr(getattr(broker, "cfg", None), "base_url", None)
            or "https://sandbox.tradier.com"
        )
        account_id = (
            getattr(broker, "account_id", None)
            or getattr(getattr(broker, "cfg", None), "account_id", None)
            or ""
        )

        error_msg       = None
        broker_order_id = None
        try:
            resp = broker.session.post(
                f"{base_url}/v1/accounts/{account_id}/orders",
                data={
                    "class":         "option",
                    "symbol":        plan.ticker,
                    "option_symbol": symbol,
                    "side":          "buy_to_open",
                    "quantity":      int(plan.contracts),
                    "type":          "limit",
                    "price":         round(lp, 2),
                    "duration":      "day",
                },
                headers={"Accept": "application/json"},
                timeout=10,
            )
            order_json      = resp.json() or {}
            order           = order_json.get("order") or {}
            status          = (order.get("status") or "").lower()
            broker_order_id = order.get("id") or order.get("order_id")

            if status in ("ok", "pending", "open", "accepted"):
                self.transition(
                    local_id,
                    OrderStatus.SUBMITTED,
                    broker_order_id=broker_order_id,
                    submitted_ts=now_utc_iso(),
                )
                return {
                    "ok":             True,
                    "local_order_id": local_id,
                    "broker_order_id": broker_order_id,
                    "status":         OrderStatus.SUBMITTED,
                    "error":          None,
                }
            else:
                error_msg = f"broker_status:{status or 'unknown'}"
        except Exception as e:
            error_msg = f"broker_error:{e}"

        self.transition(
            local_id,
            OrderStatus.ERROR,
            last_error=error_msg or "unknown_error",
        )
        return {
            "ok":             False,
            "local_order_id": local_id,
            "broker_order_id": broker_order_id,
            "status":         OrderStatus.ERROR,
            "error":          error_msg,
        }

    def submit_exit(
        self,
        *,
        broker,
        position_id: str,
        contract:    str,
        symbol:      str,
        direction:   str,
        qty:         int,
        limit_price: float,
        plan_id:     Optional[str] = None,
        signal_id:   Optional[str] = None,
    ) -> dict:
        """
        Create an EXIT_REQUESTED order for a position and submit it to broker.
        Returns same shape dict as submit_entry().
        """
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

        base_url   = (
            getattr(broker, "base_url", None)
            or getattr(getattr(broker, "cfg", None), "base_url", None)
            or "https://sandbox.tradier.com"
        )
        account_id = (
            getattr(broker, "account_id", None)
            or getattr(getattr(broker, "cfg", None), "account_id", None)
            or ""
        )

        # Underlying ticker: strip suffixes (e.g. "AAPL 250117C00200000" -> "AAPL")
        underlying = symbol.split()[0] if " " in symbol else symbol[:6]

        error_msg       = None
        broker_order_id = None
        try:
            resp = broker.session.post(
                f"{base_url}/v1/accounts/{account_id}/orders",
                data={
                    "class":         "option",
                    "symbol":        underlying,
                    "option_symbol": contract,
                    "side":          "sell_to_close",
                    "quantity":      int(qty),
                    "type":          "limit",
                    "price":         round(limit_price, 2),
                    "duration":      "day",
                },
                headers={"Accept": "application/json"},
                timeout=10,
            )
            order_json      = resp.json() or {}
            order           = order_json.get("order") or {}
            status          = (order.get("status") or "").lower()
            broker_order_id = order.get("id") or order.get("order_id")

            if status in ("ok", "pending", "open", "accepted"):
                self.transition(
                    local_id,
                    OrderStatus.EXIT_SUBMITTED,
                    broker_order_id=broker_order_id,
                    submitted_ts=now_utc_iso(),
                )
                return {
                    "ok":              True,
                    "local_order_id":  local_id,
                    "broker_order_id": broker_order_id,
                    "status":          OrderStatus.EXIT_SUBMITTED,
                    "error":           None,
                }
            else:
                error_msg = f"broker_status:{status or 'unknown'}"
        except Exception as e:
            error_msg = f"broker_error:{e}"

        self.transition(
            local_id,
            OrderStatus.ERROR,
            last_error=error_msg or "unknown_error",
        )
        return {
            "ok":              False,
            "local_order_id":  local_id,
            "broker_order_id": broker_order_id,
            "status":          OrderStatus.ERROR,
            "error":           error_msg,
        }

    # =========================================================================
    # PRIVATE
    # =========================================================================

    def _get_order(self, local_order_id: str) -> Optional[dict]:
        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT * FROM orders "
                    "WHERE local_order_id=%s AND client_id=%s",
                    (local_order_id, self.client_id),
                )
                return c.fetchone()
        return run_with_retry(_fn)

    def _get_order_by_plan(
        self, plan_id: str, kind: str = "ENTRY"
    ) -> Optional[dict]:
        """Dedup: check if plan already has an order of this kind."""
        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT * FROM orders "
                    "WHERE client_id=%s AND plan_id=%s AND kind=%s LIMIT 1",
                    (self.client_id, plan_id, kind),
                )
                return c.fetchone()
        return run_with_retry(_fn)

    def _get_active_exit_order(self, position_id: str) -> Optional[dict]:
        """Dedup: check if position already has an active exit order."""
        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT * FROM orders "
                    "WHERE client_id=%s AND position_id=%s AND kind='EXIT' "
                    "AND status NOT IN ('EXIT_FILLED','REJECTED','CANCELED','EXPIRED','ERROR') "
                    "LIMIT 1",
                    (self.client_id, position_id),
                )
                return c.fetchone()
        return run_with_retry(_fn)

    def _record_error(self, local_order_id: str, error_msg: str):
        def _fn():
            with conn() as c:
                c.execute(
                    "UPDATE orders SET last_error=%s, updated_ts=NOW() "
                    "WHERE local_order_id=%s AND client_id=%s",
                    (error_msg, local_order_id, self.client_id),
                )
        try:
            run_with_retry(_fn)
        except Exception:
            pass
