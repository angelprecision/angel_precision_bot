# ap/order_state_machine.py — APOrderStateMachine
# =============================================================================
# Enforced order lifecycle controller.
#
# Bug-fix history:
#   FIX-A  EXIT_SUBMITTED missing broker_order_id → quarantine via set_pending_exit_order
#   FIX-B  rowcount None bypass closed (OSM_ROWCOUNT_NONE_IS_FATAL default True)
#   FIX-C  _call_exit_engine signature mismatch fallback
#   FIX-D  REJECTED cooldown: _positions.values() when dict
#   FIX-E  _safe_int None-safe default
#   FIX-F  record_trade_outcome_from_position native on full-close path
#   FIX-G  Reconciler idempotency: no-op shim (native in ap_reconciler.py)
#   FIX-H  EXIT_FILLED zero-qty → quarantine not clear
#   FIX-I  CANCELED/EXPIRED/REJECTED routed through _call_exit_engine
#   FIX-J  Split-brain freeze: SPLIT_BRAIN: prefix + broker_order_id preserved
#   FIX-K  probe_db_rowcount() module-level startup probe
#
# Post-audit fixes (see inline AUDIT-N tags):
#   AUDIT-1  last_rejection_ts standardized to datetime.now(timezone.utc) — was
#            _time_module.time() (float epoch), which raised TypeError in
#            ap_exit_engine._can_submit_exit() after FIX-8 made the field
#            Optional[datetime]. Cooldown was silently broken for all OSM-sourced
#            rejections.
#   AUDIT-2  _get_position_remaining_from_db separated into its own properly-
#            indented method. The body was previously dead code appended inside
#            get_split_brain_orders after its return statements — the method
#            always returned None, making every EXIT_FILLED go to conservative
#            partial handling instead of mark_position_closed. The OSM full-close
#            path was structurally broken.
#   AUDIT-3  submit_entry() split-brain path now calls _flag_split_brain_order
#            and returns split_brain=True, matching submit_existing_entry().
#            Previously a direct entry split-brain left the broker with a live
#            order, the DB with ERROR status, and nothing for reconciler to act on.
#   AUDIT-4  self.client_id stripped in __init__ — DB whitespace artifacts caused
#            log lines to show dirty IDs and could cause registry key mismatches.
#   AUDIT-7  Broker rejection reason included in _emit_transition_event
#            extra_inputs on ERROR transitions so structured observability captures
#            why Tradier rejected the order, not just that it did.
#   AUDIT-8  probe_db_rowcount handles -1 (psycopg2 "unknown affected rows"
#            sentinel). -1 is truthy in Python so the probe previously passed
#            incorrectly when the driver returned -1.
# =============================================================================
from __future__ import annotations

import logging
import os
import re
import threading
import time as _time_module
import uuid
from datetime import datetime, timezone
from typing import Optional

from ap.db import conn, run_with_retry
try:
    from psycopg2 import errors as pg_errors
except ImportError:
    pg_errors = None
from ap.utils import now_utc_iso

try:
    from ap.observability import emit_decision_event, get_git_commit, make_config_hash
except Exception:  # pragma: no cover
    emit_decision_event = None

    def get_git_commit(default: str = "unknown") -> str:
        return default

    def make_config_hash(config: dict) -> str:
        return "unknown"


log = logging.getLogger("ap.order_state_machine")

_ROWCOUNT_NONE_IS_FATAL: bool = (
    os.getenv("OSM_ROWCOUNT_NONE_IS_FATAL", "1").strip().lower()
    not in {"0", "false", "no", "off"}
)


def probe_db_rowcount() -> Optional[int]:
    """
    Test whether the active DB driver exposes rowcount on UPDATE statements.
    Call once at runner startup before any APOrderStateMachine is created.

    AUDIT-8: now treats -1 the same as None. psycopg2 returns -1 for rowcount
    when it executed successfully but cannot report the number of affected rows.
    -1 is truthy in Python so the previous version passed the probe incorrectly
    when the driver returned -1.

    Returns:
        int >= 0  — driver exposes rowcount (0 expected for no-op UPDATE)
        None      — driver does not expose rowcount; transitions stall in strict mode
    """
    try:
        def _probe():
            with conn() as c:
                cur = c.execute("UPDATE orders SET updated_ts=updated_ts WHERE 1=0")
                return getattr(cur, "rowcount", getattr(c, "rowcount", None))

        result = run_with_retry(_probe)

        # AUDIT-8: treat -1 (psycopg2 "unknown") the same as None.
        if result is None or result == -1:
            log.critical(
                "DB ROWCOUNT PROBE -> %r | OSM_ROWCOUNT_NONE_IS_FATAL=%s | "
                "strict mode will refuse ALL transitions until resolved; "
                "verify: cur = c.execute('UPDATE orders SET updated_ts=updated_ts WHERE 1=0'); "
                "print(cur.rowcount) -- fix driver or set OSM_ROWCOUNT_NONE_IS_FATAL=0",
                result,
                "1 (strict)" if _ROWCOUNT_NONE_IS_FATAL else "0 (permissive)",
            )
            return None  # normalise -1 → None so callers only check `is None`
        else:
            log.info(
                "DB rowcount probe -> %r (int) -- OSM strict rowcount check is safe",
                result,
            )
        return result
    except Exception as exc:
        log.error("DB rowcount probe failed with exception: %s", exc)
        return None


_exit_engine_registry: dict[str, object] = {}
_registry_lock = threading.Lock()


def _normalize_client_key(client_id: str) -> str:
    """Lowercase + strip for consistent registry key lookups."""
    return str(client_id or "").strip().lower()


def register_exit_engine(*args, **kwargs) -> None:
    """
    Register a per-client exit engine. Supports:
        register_exit_engine(client_id, exit_engine)
        register_exit_engine(exit_engine)   # engine must carry client_id/.email/._email
    """
    if len(args) == 2:
        client_id, exit_engine = args
    elif len(args) == 1:
        exit_engine = args[0]
        client_id = (
            kwargs.get("client_id")
            or getattr(exit_engine, "client_id", None)
            or getattr(exit_engine, "email", None)
            or getattr(exit_engine, "_email", None)
        )
    elif len(args) == 0:
        client_id   = kwargs.get("client_id")
        exit_engine = kwargs.get("exit_engine")
    else:
        raise TypeError(f"register_exit_engine() takes 1-2 positional arguments, got {len(args)}")
    if not client_id or exit_engine is None:
        raise ValueError(
            "register_exit_engine requires both client_id and exit_engine."
        )
    key = _normalize_client_key(client_id)
    with _registry_lock:
        _exit_engine_registry[key] = exit_engine
    log.info("[%s] Exit engine registered (key=%s)", client_id, key)


def unregister_exit_engine(client_id: str) -> None:
    key = _normalize_client_key(client_id)
    with _registry_lock:
        _exit_engine_registry.pop(key, None)


def _get_exit_engine_for_client(client_id: str):
    key = _normalize_client_key(client_id)
    with _registry_lock:
        return _exit_engine_registry.get(key)


class OrderStatus:
    CREATED           = "CREATED"
    PENDING_TRIGGER   = "PENDING_TRIGGER"
    SUBMITTED         = "SUBMITTED"
    ACKNOWLEDGED      = "ACKNOWLEDGED"
    PARTIAL_FILL      = "PARTIAL_FILL"
    FILLED            = "FILLED"
    EXIT_REQUESTED    = "EXIT_REQUESTED"
    EXIT_SUBMITTED    = "EXIT_SUBMITTED"
    EXIT_ACKNOWLEDGED = "EXIT_ACKNOWLEDGED"
    EXIT_PARTIAL_FILL = "EXIT_PARTIAL_FILL"
    EXIT_FILLED       = "EXIT_FILLED"
    REJECTED  = "REJECTED"
    CANCELED  = "CANCELED"
    EXPIRED   = "EXPIRED"
    ERROR     = "ERROR"

    ENTRY_ACTIVE = {CREATED, PENDING_TRIGGER, SUBMITTED, ACKNOWLEDGED, PARTIAL_FILL}
    EXIT_ACTIVE  = {EXIT_REQUESTED, EXIT_SUBMITTED, EXIT_ACKNOWLEDGED, EXIT_PARTIAL_FILL}
    TERMINAL     = {FILLED, EXIT_FILLED, REJECTED, CANCELED, EXPIRED, ERROR}
    ACTIVE       = ENTRY_ACTIVE | EXIT_ACTIVE

    TRANSITIONS: dict[str, set[str]] = {
        CREATED:           {PENDING_TRIGGER, SUBMITTED, ERROR, CANCELED, EXPIRED, REJECTED},
        PENDING_TRIGGER:   {SUBMITTED, ERROR, CANCELED, EXPIRED, REJECTED},
        SUBMITTED:         {ACKNOWLEDGED, PARTIAL_FILL, FILLED, REJECTED, EXPIRED, ERROR, CANCELED},
        ACKNOWLEDGED:      {PARTIAL_FILL, FILLED, REJECTED, EXPIRED, ERROR, CANCELED},
        PARTIAL_FILL:      {FILLED, REJECTED, EXPIRED, ERROR, CANCELED},
        # DESIGN: FILLED → EXIT_REQUESTED is allowed to support direct re-use of
        # the entry order row. In practice, exit lifecycle uses a separate EXIT order
        # row (kind='EXIT') starting at EXIT_REQUESTED — see create_exit_order().
        # EXIT_FILLED is intentionally absent (terminal, no further transitions).
        FILLED:            {EXIT_REQUESTED},
        EXIT_REQUESTED:    {EXIT_SUBMITTED, REJECTED, CANCELED, EXPIRED, ERROR},
        EXIT_SUBMITTED:    {EXIT_ACKNOWLEDGED, EXIT_PARTIAL_FILL, EXIT_FILLED, REJECTED, EXPIRED, ERROR, CANCELED},
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
        # AUDIT-4: strip client_id — DB whitespace artifacts caused log noise and
        # potential registry mismatches when self.client_id was passed forward as a
        # lookup key. _normalize_client_key normalizes for registry lookups but
        # self.client_id itself was never cleaned.
        self.client_id        = str(client_id or "").strip()
        self.run_id           = os.getenv("AP_RUN_ID", "unknown")
        self.strategy_version = os.getenv("AP_STRATEGY_VERSION", "ap_live_beta")
        self.git_commit       = get_git_commit()
        self.config_hash      = make_config_hash(
            {
                "entry_active": sorted(PENDING_ENTRY_STATUSES),
                "exit_active":  sorted(PENDING_EXIT_STATUSES),
                "terminal":     sorted(OrderStatus.TERMINAL),
                "transitions":  {k: sorted(v) for k, v in OrderStatus.TRANSITIONS.items()},
            }
        )
        log.info("[%s] APOrderStateMachine initialized", self.client_id)

    # ------------------------------------------------------------------
    # Internal utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _safe_int(value, default=0):
        """
        Convert value to int, returning default on failure.
        FIX-E: default is intentionally untyped so callers can pass None to
        distinguish "not provided" from zero.
        """
        try:
            if value is None or value == "":
                return default
            return int(float(value))
        except Exception:
            return default

    @staticmethod
    def _call_exit_engine(exit_engine, method_name: str, *args, **kwargs):
        """
        Call an exit engine hook method with graceful signature fallback.
        FIX-C: strips optional extension kwargs and retries once on TypeError.
        """
        method = getattr(exit_engine, method_name, None)
        if not method:
            return None
        try:
            return method(*args, **kwargs)
        except TypeError:
            for drop_key in ("identity_quarantine", "reconciled", "status"):
                kwargs.pop(drop_key, None)
            try:
                return method(*args, **kwargs)
            except Exception:
                log.exception("[OSM] exit engine hook %s failed after filtered retry", method_name)
                return None
        except Exception:
            log.exception("[OSM] exit engine hook %s failed", method_name)
            return None

    @staticmethod
    def _get_exit_engine_position(exit_engine, position_id: str):
        """Locate a ManagedPosition by position_id in any container the engine uses."""
        if not exit_engine or not position_id:
            return None
        getter = getattr(exit_engine, "get_position", None)
        if callable(getter):
            try:
                return getter(position_id)
            except Exception:
                pass
        raw = getattr(exit_engine, "_positions", None)
        if raw is None:
            return None
        if isinstance(raw, dict):
            pos = raw.get(position_id)
            if pos is not None:
                return pos
            items = raw.values()
        else:
            items = raw
        for pos in items:
            if str(getattr(pos, "position_id", getattr(pos, "id", "")) or "") == str(position_id):
                return pos
        return None

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------

    def _reason_code_for_transition(self, old_status: str, new_status: str, kind: str = "") -> str:
        if new_status == OrderStatus.FILLED:            return "ENTRY_FILLED"
        if new_status == OrderStatus.EXIT_FILLED:       return "EXIT_FILLED"
        if new_status in (OrderStatus.PARTIAL_FILL, OrderStatus.EXIT_PARTIAL_FILL):
            return "PARTIAL_FILL"
        if new_status == OrderStatus.REJECTED:          return "ORDER_REJECTED"
        if new_status == OrderStatus.CANCELED:          return "ORDER_CANCELED"
        if new_status == OrderStatus.EXPIRED:           return "ORDER_EXPIRED"
        if new_status == OrderStatus.ERROR:             return "ORDER_ERROR"
        if new_status in (OrderStatus.SUBMITTED, OrderStatus.EXIT_SUBMITTED):
            return "ORDER_SUBMITTED"
        if new_status in (OrderStatus.ACKNOWLEDGED, OrderStatus.EXIT_ACKNOWLEDGED):
            return "BROKER_ACKNOWLEDGED"
        if new_status == OrderStatus.PENDING_TRIGGER:   return "PENDING_TRIGGER"
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
            kind  = str(order.get("kind") or "")
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
                    "kind":             kind,
                    "old_status":       old_status,
                    "new_status":       new_status,
                    "local_order_id":   local_order_id,
                    "broker_order_id":  broker_order_id or order.get("broker_order_id"),
                    "filled_qty":       filled_qty,
                    "fill_price":       fill_price,
                    "last_error":       last_error,
                    **(extra_inputs or {}),
                },
                context=extra_context or {},
            )
        except Exception as e:
            log.debug("OSM observability emit failed (non-critical): %s", e)

    # ------------------------------------------------------------------
    # Order creation
    # ------------------------------------------------------------------

    def create_entry_order(self, plan, *, limit_price=None, reserved_cost=None) -> str:
        existing = self._get_order_by_plan(plan.plan_id, kind="ENTRY")
        if existing:
            log.warning(
                "[%s] create_entry_order SKIPPED -- plan %s already has order %s",
                self.client_id, plan.plan_id, existing["local_order_id"],
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
                        local_order_id, self.client_id,
                        plan.plan_id, plan.signal_id,
                        plan.ticker, contract, plan.side.upper(),
                        int(plan.contracts), lp, rc,
                        ts, ts,
                    ),
                )

        try:
            run_with_retry(_fn)
        except Exception as _db_err:
            if pg_errors and isinstance(_db_err, pg_errors.UniqueViolation):
                log.critical(
                    "[%s] DUPLICATE ORDER BLOCKED by DB constraint — ENTRY plan=%s",
                    self.client_id, plan.plan_id,
                )
                # Re-fetch: another thread may have inserted between our check and INSERT
                refetched = self._get_order_by_plan(plan.plan_id, kind="ENTRY")
                if refetched:
                    return refetched["local_order_id"]
                return existing["local_order_id"] if existing else local_order_id
            raise
        log.info(
            "[%s] ORDER CREATED (entry) | %s x%s | local_order_id=%s",
            self.client_id, contract, plan.contracts, local_order_id,
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
                "[%s] create_exit_order SKIPPED -- position %s already has exit order %s",
                self.client_id, position_id, existing["local_order_id"],
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
                        local_id, self.client_id,
                        position_id, plan_id, signal_id,
                        symbol.upper(), contract, direction.upper(),
                        int(qty),
                        float(limit_price) if limit_price else None,
                        float(reserved_cost) if reserved_cost else None,
                        ts, ts,
                    ),
                )

        try:
            run_with_retry(_fn)
        except Exception as _db_err:
            if pg_errors and isinstance(_db_err, pg_errors.UniqueViolation):
                log.critical(
                    "[%s] DUPLICATE ORDER BLOCKED by DB constraint — EXIT pos=%s",
                    self.client_id, position_id,
                )
                # Re-fetch using position_id identity (same as the pre-check above),
                # NOT _get_order_by_plan which queries plan_id — a different column.
                # Using the wrong identity here can return None or a wrong row when
                # two threads race to create an exit for the same position.
                refetched = self._get_active_exit_order(position_id)
                if refetched:
                    return refetched["local_order_id"]
                return existing["local_order_id"] if existing else local_id
            raise
        log.info(
            "[%s] ORDER CREATED (exit) | %s x%s pos=%s | local_order_id=%s",
            self.client_id, contract, qty, position_id, local_id,
        )
        return local_id

    # ------------------------------------------------------------------
    # Watcher/queue entry-state helpers
    # ------------------------------------------------------------------

    def mark_entry_pending_trigger(self, local_order_id: str) -> bool:
        current = self._get_order(local_order_id)
        if not current:
            log.error("[%s] mark_entry_pending_trigger: order %s not found",
                      self.client_id, local_order_id)
            return False
        current = dict(current)
        kind   = str(current.get("kind") or "").upper()
        status = str(current.get("status") or "").upper()
        if kind != "ENTRY":
            log.critical("[%s] mark_entry_pending_trigger blocked -- wrong kind %s for order=%s",
                         self.client_id, kind, local_order_id)
            return False
        if status == OrderStatus.PENDING_TRIGGER:
            return True
        if status != OrderStatus.CREATED:
            log.warning("[%s] mark_entry_pending_trigger blocked -- invalid status %s for order=%s",
                        self.client_id, status, local_order_id)
            return False
        return self.transition(local_order_id, OrderStatus.PENDING_TRIGGER)

    def expire_pending_entry(self, local_order_id: str, *, reason: str = "watcher_expired") -> bool:
        current = self._get_order(local_order_id)
        if not current:
            log.error("[%s] expire_pending_entry: order %s not found",
                      self.client_id, local_order_id)
            return False
        current = dict(current)
        kind   = str(current.get("kind") or "").upper()
        status = str(current.get("status") or "").upper()
        if kind != "ENTRY":
            log.critical("[%s] expire_pending_entry blocked -- wrong kind %s | %s",
                         self.client_id, kind, local_order_id)
            return False
        if status == OrderStatus.EXPIRED:
            return True
        if status not in (OrderStatus.CREATED, OrderStatus.PENDING_TRIGGER):
            log.warning("[%s] expire_pending_entry blocked -- status=%s order=%s",
                        self.client_id, status, local_order_id)
            return False
        return self.transition(local_order_id, OrderStatus.EXPIRED, last_error=reason)

    def cancel_pending_entry(self, local_order_id: str, *, reason: str = "watcher_invalidated") -> bool:
        current = self._get_order(local_order_id)
        if not current:
            log.error("[%s] cancel_pending_entry: order %s not found",
                      self.client_id, local_order_id)
            return False
        current = dict(current)
        kind   = str(current.get("kind") or "").upper()
        status = str(current.get("status") or "").upper()
        if kind != "ENTRY":
            log.critical("[%s] cancel_pending_entry blocked -- wrong kind %s | %s",
                         self.client_id, kind, local_order_id)
            return False
        if status == OrderStatus.CANCELED:
            return True
        if status not in (OrderStatus.CREATED, OrderStatus.PENDING_TRIGGER):
            log.warning("[%s] cancel_pending_entry blocked -- status=%s order=%s",
                        self.client_id, status, local_order_id)
            return False
        return self.transition(local_order_id, OrderStatus.CANCELED, last_error=reason)

    # ------------------------------------------------------------------
    # State transition authority
    # ------------------------------------------------------------------

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

        current     = dict(current)
        old_status  = str(current.get("status") or "")
        kind        = str(current.get("kind") or "")
        prev_filled = self._safe_int(current.get("filled_qty"), 0)
        same_state_fill_update = False

        if filled_qty is not None:
            incoming_filled = self._safe_int(filled_qty, None)
            if incoming_filled is None:
                log.critical("[%s] INVALID FILL QTY | order=%s filled_qty=%r",
                             self.client_id, local_order_id, filled_qty)
                return False
            if incoming_filled < prev_filled:
                log.critical("[%s] INVALID CUMULATIVE FILL REGRESSION | order=%s new=%s prev=%s",
                             self.client_id, local_order_id, incoming_filled, prev_filled)
                self._emit_transition_event(
                    local_order_id=local_order_id, old_status=old_status, new_status=new_status,
                    order=current, decision="REJECT", reason_code="FILL_QTY_REGRESSION",
                    explanation=f"filled_qty must be cumulative: new={incoming_filled} prev={prev_filled}",
                    broker_order_id=broker_order_id, filled_qty=filled_qty,
                    fill_price=fill_price, last_error=last_error,
                )
                return False

        if old_status == new_status:
            if new_status in (OrderStatus.PARTIAL_FILL, OrderStatus.EXIT_PARTIAL_FILL):
                same_state_fill_update = True
            else:
                log.debug("[%s] %s already %s -- no-op", self.client_id, local_order_id, new_status)
                return True

        if OrderStatus.is_terminal(old_status):
            reason = f"terminal_transition_blocked:{old_status}->{new_status}"
            log.warning("[%s] TRANSITION BLOCKED -- %s already terminal (%s), cannot move to %s",
                        self.client_id, local_order_id, old_status, new_status)
            self._record_error(local_order_id, reason)
            self._emit_transition_event(
                local_order_id=local_order_id, old_status=old_status, new_status=new_status,
                order=current, decision="REJECT", reason_code="TERMINAL_STATE_BLOCK",
                explanation=reason, last_error=last_error,
            )
            return False

        if not same_state_fill_update and not OrderStatus.can_transition(old_status, new_status):
            reason = f"illegal_transition:{old_status}->{new_status}"
            log.critical(
                "[%s] ILLEGAL TRANSITION -- %s: %s -> %s | kind=%s broker=%s filled=%s price=%s",
                self.client_id, local_order_id, old_status, new_status, kind,
                broker_order_id or current.get("broker_order_id"), filled_qty, fill_price,
            )
            self._record_error(local_order_id, reason)
            self._emit_transition_event(
                local_order_id=local_order_id, old_status=old_status, new_status=new_status,
                order=current, decision="REJECT", reason_code="ILLEGAL_TRANSITION",
                explanation=reason, broker_order_id=broker_order_id,
                filled_qty=filled_qty, fill_price=fill_price, last_error=last_error,
            )
            return False

        updates = ["status=%s", "updated_ts=NOW()"]
        params  = [new_status]
        if broker_order_id:
            updates.append("broker_order_id=%s"); params.append(broker_order_id)
        if filled_qty is not None:
            updates.append("filled_qty=%s"); params.append(int(filled_qty))
        if fill_price is not None:
            updates.append("fill_price=%s"); params.append(float(fill_price))
        if last_error:
            updates.append("last_error=%s"); params.append(last_error)
        if submitted_ts:
            updates.append("submitted_ts=%s"); params.append(submitted_ts)
        if position_id:
            updates.append("position_id=%s"); params.append(position_id)
        if new_status in (OrderStatus.FILLED, OrderStatus.EXIT_FILLED):
            updates.append("filled_ts=%s"); params.append(filled_ts or now_utc_iso())
        # COMPARE-AND-SWAP: guard the UPDATE on the status we read above.
        # Without this, two concurrent callers (fill_monitor / order_monitor /
        # reconciler all run in separate threads) can both pass the Python-side
        # can_transition() check and both UPDATE the row — the second silently
        # overwriting the first's fill_qty/fill_price. The rowcount==1 check
        # cannot detect that because both UPDATEs legitimately touch one row.
        # With "AND status=<old_status>" the DB serializes concurrent writers:
        # exactly one wins, the losers get rowcount=0 and are handled below.
        # same_state_fill_update (PARTIAL_FILL->PARTIAL_FILL) is safe here too
        # because old_status == new_status, so the guard still matches the row.
        params.extend([local_order_id, self.client_id, old_status])
        sql = (
            f"UPDATE orders SET {', '.join(updates)} "
            f"WHERE local_order_id=%s AND client_id=%s AND status=%s"
        )

        def _fn():
            with conn() as c:
                cur = c.execute(sql, tuple(params))
                return getattr(cur, "rowcount", getattr(c, "rowcount", None))

        rowcount = run_with_retry(_fn)

        if rowcount == 0:
            # CAS miss. Re-read to determine WHY the guarded UPDATE matched no row:
            #   (a) another thread already advanced it to new_status -> idempotent OK
            #   (b) another thread moved it somewhere else            -> real conflict
            #   (c) row genuinely missing                             -> real error
            latest = self._get_order(local_order_id)
            if latest:
                latest_status = str(dict(latest).get("status") or "")
                if latest_status == new_status:
                    log.info(
                        "[%s] transition CAS no-op -- %s already advanced to %s by "
                        "a concurrent worker; treating as success",
                        self.client_id, local_order_id, new_status,
                    )
                    return True
                reason = (
                    f"transition_cas_conflict:{old_status}->{new_status}"
                    f" (actual_now={latest_status})"
                )
                log.critical(
                    "[%s] OSM CAS CONFLICT | %s | expected_from=%s wanted=%s actual=%s "
                    "-- concurrent writer changed status; refusing to overwrite",
                    self.client_id, local_order_id, old_status, new_status, latest_status,
                )
            else:
                reason = f"transition_update_no_rows:{old_status}->{new_status}"
                log.critical("[%s] OSM UPDATE TOUCHED ZERO ROWS (row missing) | %s | %s",
                             self.client_id, local_order_id, reason)
            self._record_error(local_order_id, reason)
            self._emit_transition_event(
                local_order_id=local_order_id, old_status=old_status, new_status=new_status,
                order=current, decision="ERROR", reason_code="DB_UPDATE_MISSED", explanation=reason,
            )
            return False

        if rowcount is None:
            if _ROWCOUNT_NONE_IS_FATAL:
                reason = f"transition_rowcount_unconfirmed:{old_status}->{new_status}"
                log.critical(
                    "[%s] OSM ROWCOUNT UNCONFIRMED -- refusing state advance | "
                    "order=%s %s->%s | set OSM_ROWCOUNT_NONE_IS_FATAL=0 only if "
                    "your DB driver is proven to never silently fail UPDATE statements",
                    self.client_id, local_order_id, old_status, new_status,
                )
                self._record_error(local_order_id, reason)
                self._emit_transition_event(
                    local_order_id=local_order_id, old_status=old_status, new_status=new_status,
                    order=current, decision="ERROR", reason_code="DB_ROWCOUNT_UNCONFIRMED",
                    explanation=reason,
                )
                return False
            else:
                log.warning(
                    "[%s] OSM rowcount unavailable (OSM_ROWCOUNT_NONE_IS_FATAL=0) | "
                    "order=%s %s->%s -- proceeding without row confirmation",
                    self.client_id, local_order_id, old_status, new_status,
                )

        log.info(
            "[%s] ORDER %s -> %s | %s%s%s",
            self.client_id, old_status, new_status, local_order_id,
            f" broker={broker_order_id}" if broker_order_id else "",
            f" fill={filled_qty}@{fill_price}" if fill_price is not None else "",
        )
        self._emit_transition_event(
            local_order_id=local_order_id, old_status=old_status, new_status=new_status,
            order=current,
            decision="CONFIRMED" if new_status not in {
                OrderStatus.REJECTED, OrderStatus.CANCELED, OrderStatus.EXPIRED, OrderStatus.ERROR
            } else "TERMINAL",
            broker_order_id=broker_order_id, filled_qty=filled_qty,
            fill_price=fill_price, last_error=last_error,
        )
        self._handle_exit_engine_hooks(
            current=current, new_status=new_status, position_id=position_id,
            filled_qty=filled_qty, fill_price=fill_price,
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
        if new_status not in (
            OrderStatus.EXIT_SUBMITTED, OrderStatus.EXIT_FILLED,
            OrderStatus.EXIT_PARTIAL_FILL, OrderStatus.CANCELED,
            OrderStatus.EXPIRED, OrderStatus.REJECTED,
        ):
            return

        _pos_id = position_id or current.get("position_id")
        _kind   = str(current.get("kind") or "ENTRY").upper()
        if not _pos_id or _kind != "EXIT":
            return

        try:
            _ee = _get_exit_engine_for_client(self.client_id)
            if not _ee:
                log.warning(
                    "[%s] EXIT hook skipped -- no exit engine registered | order=%s pos=%s status=%s",
                    self.client_id, local_order_id or current.get("local_order_id"),
                    _pos_id, new_status,
                )
                return

            _local_id    = local_order_id or current.get("local_order_id")
            _broker_id   = broker_order_id or current.get("broker_order_id")
            _order_qty   = self._safe_int(current.get("qty"), 0)
            _prev_filled = self._safe_int(current.get("filled_qty"), 0)
            _cum_filled  = self._safe_int(filled_qty, None)  # FIX-E: None means "not provided"

            if _cum_filled is not None and _cum_filled < _prev_filled:
                log.critical(
                    "[%s] INVALID EXIT CUMULATIVE FILL | order=%s pos=%s new=%s prev=%s",
                    self.client_id, _local_id, _pos_id, _cum_filled, _prev_filled,
                )
                return

            # ── EXIT_SUBMITTED ─────────────────────────────────────────────
            if new_status == OrderStatus.EXIT_SUBMITTED:
                if _broker_id:
                    self._call_exit_engine(
                        _ee, "set_pending_exit_order", _pos_id,
                        local_order_id=str(_local_id or ""),
                        broker_order_id=str(_broker_id),
                        qty=_order_qty,
                        reason=str(current.get("last_error") or ""),
                    )
                else:
                    # FIX-A: quarantine on missing broker identity
                    log.critical(
                        "[%s] EXIT_SUBMITTED missing broker_order_id -- quarantining exit | "
                        "order=%s pos=%s; reconciler will recover identity",
                        self.client_id, _local_id, _pos_id,
                    )
                    self._call_exit_engine(
                        _ee, "set_pending_exit_order", _pos_id,
                        local_order_id=str(_local_id or ""),
                        broker_order_id="",
                        qty=_order_qty,
                        reason=str(current.get("last_error") or "broker_accepted_missing_order_id_quarantine"),
                        identity_quarantine=True,
                    )
                return

            # ── EXIT_PARTIAL_FILL ───────────────────────────────────────────
            if new_status == OrderStatus.EXIT_PARTIAL_FILL:
                if _cum_filled is None:
                    log.warning(
                        "[%s] EXIT_PARTIAL_FILL missing filled_qty | order=%s pos=%s -- not applying",
                        self.client_id, _local_id, _pos_id,
                    )
                    return
                _delta = max(0, _cum_filled - _prev_filled)
                if _delta > 0:
                    self._call_exit_engine(
                        _ee, "note_partial_exit_fill", _pos_id, _delta,
                        fill_price=fill_price,
                        local_order_id=str(_local_id or ""),
                        broker_order_id=str(_broker_id or ""),
                        cumulative_filled=_cum_filled,
                    )
                return

            # ── EXIT_FILLED ─────────────────────────────────────────────────
            if new_status == OrderStatus.EXIT_FILLED:
                if _cum_filled is None or _cum_filled <= 0:
                    _cum_filled = _order_qty

                # FIX-H: zero qty → quarantine, not clear
                if _cum_filled <= 0:
                    log.critical(
                        "[%s] EXIT_FILLED with zero/unknown quantity -- QUARANTINING | "
                        "order=%s pos=%s | broker fill data unreliable",
                        self.client_id, _local_id, _pos_id,
                    )
                    self._call_exit_engine(
                        _ee, "set_pending_exit_order", _pos_id,
                        local_order_id=str(_local_id or ""),
                        broker_order_id=str(_broker_id or ""),
                        qty=0,
                        reason="EXIT_FILLED_ZERO_QTY_QUARANTINE",
                        identity_quarantine=True,
                    )
                    return

                _delta = max(0, _cum_filled - _prev_filled)
                # AUDIT-2: _get_position_remaining_from_db is now a proper method
                # (not dead code). This call correctly returns the DB quantity.
                _remaining_before = self._get_position_remaining_from_db(_pos_id)
                if _remaining_before is None:
                    _pos_obj = self._get_exit_engine_position(_ee, str(_pos_id))
                    _remaining_before = (
                        self._safe_int(getattr(_pos_obj, "quantity_remaining", None), None)
                        if _pos_obj else None
                    )

                if _delta <= 0:
                    log.info(
                        "[%s] EXIT_FILLED duplicate callback -- no new qty | "
                        "order=%s pos=%s prev_filled=%s cum=%s; clearing in-flight (safe: prev>0)",
                        self.client_id, _local_id, _pos_id, _prev_filled, _cum_filled,
                    )
                    self._call_exit_engine(
                        _ee, "clear_exit_in_flight", _pos_id,
                        local_order_id=str(_local_id or ""),
                        broker_order_id=str(_broker_id or ""),
                    )
                    return

                if _remaining_before is None:
                    log.critical(
                        "[%s] EXIT_FILLED remaining size unknown -- conservative partial handling | "
                        "order=%s pos=%s delta=%s",
                        self.client_id, _local_id, _pos_id, _delta,
                    )
                    self._call_exit_engine(
                        _ee, "note_partial_exit_fill", _pos_id, _delta,
                        fill_price=fill_price,
                        local_order_id=str(_local_id or ""),
                        broker_order_id=str(_broker_id or ""),
                        cumulative_filled=_cum_filled,
                    )
                    return

                if _delta < int(_remaining_before):
                    log.info(
                        "[%s] EXIT_FILLED treated as completed scale-out | order=%s pos=%s "
                        "delta=%s remaining_before=%s",
                        self.client_id, _local_id, _pos_id, _delta, _remaining_before,
                    )
                    self._call_exit_engine(
                        _ee, "note_partial_exit_fill", _pos_id, _delta,
                        fill_price=fill_price,
                        local_order_id=str(_local_id or ""),
                        broker_order_id=str(_broker_id or ""),
                        cumulative_filled=_cum_filled,
                    )
                    return

                # Full close: delta >= remaining
                log.info(
                    "[%s] EXIT_FILLED treated as full close | order=%s pos=%s "
                    "delta=%s remaining_before=%s",
                    self.client_id, _local_id, _pos_id, _delta, _remaining_before,
                )
                # FIX-F: record performance before removing from engine
                _pos_for_perf = self._get_exit_engine_position(_ee, str(_pos_id))
                if _pos_for_perf is not None:
                    try:
                        from ap.performance_tracker import record_trade_outcome_from_position
                        record_trade_outcome_from_position(
                            _pos_for_perf,
                            qty_filled=_delta,
                            fill_price=fill_price,
                            reason="EXIT_FILLED",
                            supabase_client=getattr(_ee, "sb", None) or getattr(_ee, "supabase", None),
                        )
                    except Exception as _perf_err:
                        log.debug("[%s] performance outcome hook failed (non-critical): %s",
                                  self.client_id, _perf_err)
                self._call_exit_engine(
                    _ee, "mark_position_closed", str(_pos_id),
                    reason="EXIT_FILLED",
                    qty_filled=_delta,
                    fill_price=fill_price,
                    local_order_id=str(_local_id or ""),
                    broker_order_id=str(_broker_id or ""),
                    cumulative_filled=_cum_filled,
                )
                # Finalize position row from confirmed broker fill truth.
                # This is the canonical write path: orders → positions → dashboard.
                # Runs after mark_position_closed so exit engine state is updated first.
                self._finalize_position_from_exit_order(str(_local_id or ""), fill_price=fill_price, filled_qty=_delta)
                return

            # ── CANCELED / EXPIRED / REJECTED ───────────────────────────────
            if new_status in (OrderStatus.CANCELED, OrderStatus.EXPIRED, OrderStatus.REJECTED):
                # FIX-I: routed through _call_exit_engine for TypeError fallback
                if hasattr(_ee, "on_exit_failure"):
                    self._call_exit_engine(
                        _ee, "on_exit_failure", _pos_id,
                        local_order_id=str(_local_id or ""),
                        broker_order_id=str(_broker_id or ""),
                        status=new_status,
                    )
                elif hasattr(_ee, "clear_exit_in_flight"):
                    self._call_exit_engine(
                        _ee, "clear_exit_in_flight", _pos_id,
                        local_order_id=str(_local_id or ""),
                        broker_order_id=str(_broker_id or ""),
                    )
                else:
                    log.critical(
                        "[%s] Exit failure hook missing on exit engine | order=%s pos=%s status=%s",
                        self.client_id, _local_id, _pos_id, new_status,
                    )

                if new_status == OrderStatus.REJECTED:
                    try:
                        _positions_raw = getattr(_ee, "_positions", None)
                        if _positions_raw is not None:
                            # FIX-D: use .values() when _positions is a dict
                            _iter = (
                                _positions_raw.values()
                                if isinstance(_positions_raw, dict)
                                else _positions_raw
                            )
                            for _p in _iter:
                                if str(getattr(_p, "position_id", getattr(_p, "id", "")) or "") == str(_pos_id):
                                    _p.last_exit_rejected = True
                                    # AUDIT-1: FIX-8 standardized last_rejection_ts to
                                    # Optional[datetime]. Using _time_module.time() (float)
                                    # caused TypeError in _can_submit_exit's cooldown
                                    # calculation: (datetime.now(utc) - float) → TypeError.
                                    _p.last_rejection_ts = datetime.now(timezone.utc)
                                    log.info(
                                        "[%s] Exit REJECTED -- 30s cooldown started | pos=%s ticker=%s",
                                        self.client_id, _pos_id, getattr(_p, "ticker", "?"),
                                    )
                                    break
                    except Exception as _cd_err:
                        log.debug("[%s] rejection cooldown update failed (non-critical): %s",
                                  self.client_id, _cd_err)

        except Exception as _ee_err:
            log.exception("[%s] exit_eng hook failed: %s", self.client_id, _ee_err)

    # ------------------------------------------------------------------
    # Readers / mutators
    # ------------------------------------------------------------------

    def apply_fill_update(
        self,
        local_order_id: str,
        *,
        cumulative_filled: int,
        fill_price=None,
        broker_order_id=None,
    ) -> bool:
        order = self._get_order(local_order_id)
        if not order:
            log.error("[%s] apply_fill_update: order %s not found",
                      self.client_id, local_order_id)
            return False
        order  = dict(order)
        status = str(order.get("status") or "")
        kind   = str(order.get("kind") or "").upper()
        if status not in (OrderStatus.PARTIAL_FILL, OrderStatus.EXIT_PARTIAL_FILL):
            log.warning("[%s] apply_fill_update blocked -- order=%s status=%s is not partial-fill",
                        self.client_id, local_order_id, status)
            return False
        if status == OrderStatus.EXIT_PARTIAL_FILL and kind != "EXIT":
            log.critical("[%s] apply_fill_update blocked -- EXIT_PARTIAL_FILL on non-EXIT order=%s kind=%s",
                         self.client_id, local_order_id, kind)
            return False
        return self.transition(
            local_order_id, status,
            broker_order_id=broker_order_id or order.get("broker_order_id"),
            filled_qty=cumulative_filled,
            fill_price=fill_price,
        )

    def increment_retry(self, local_order_id: str):
        def _fn():
            with conn() as c:
                c.execute(
                    "UPDATE orders SET retries=retries+1, updated_ts=NOW() "
                    "WHERE local_order_id=%s AND client_id=%s",
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

    # ------------------------------------------------------------------
    # Broker submit helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _is_broker_accept_status(status: str) -> bool:
        normalized = str(status or "").lower().strip().replace("-", "_").replace(" ", "_")
        return normalized in {
            "ok", "pending", "open", "accepted", "filled", "submitted",
            "queued", "ack", "acked", "acknowledged", "received", "working",
        }

    def submit_existing_entry(
        self,
        *,
        local_order_id: str,
        broker,
        plan=None,
        limit_price=None,
    ) -> dict:
        """Submit an already-created ENTRY order after watcher breach."""
        current = self._get_order(local_order_id)
        if not current:
            error_msg = "existing_entry_order_not_found"
            log.critical("[%s] submit_existing_entry failed -- %s | %s",
                         self.client_id, error_msg, local_order_id)
            return {"ok": False, "local_order_id": local_order_id, "broker_order_id": None,
                    "status": OrderStatus.ERROR, "error": error_msg}
        current = dict(current)
        kind   = str(current.get("kind") or "").upper()
        status = str(current.get("status") or "").upper()
        if kind != "ENTRY":
            error_msg = f"submit_existing_entry_wrong_kind:{kind}"
            self.transition(local_order_id, OrderStatus.ERROR, last_error=error_msg)
            return {"ok": False, "local_order_id": local_order_id,
                    "broker_order_id": current.get("broker_order_id"),
                    "status": OrderStatus.ERROR, "error": error_msg}
        if status in (OrderStatus.SUBMITTED, OrderStatus.ACKNOWLEDGED,
                      OrderStatus.PARTIAL_FILL, OrderStatus.FILLED):
            return {"ok": True, "local_order_id": local_order_id,
                    "broker_order_id": current.get("broker_order_id"),
                    "status": status, "error": None}
        if status in (OrderStatus.REJECTED, OrderStatus.CANCELED,
                      OrderStatus.EXPIRED, OrderStatus.ERROR):
            error_msg = f"submit_existing_entry_terminal_status:{status}"
            log.warning("[%s] submit_existing_entry blocked -- terminal status %s | %s",
                        self.client_id, status, local_order_id)
            return {"ok": False, "local_order_id": local_order_id,
                    "broker_order_id": current.get("broker_order_id"),
                    "status": status, "error": error_msg}
        if status not in (OrderStatus.CREATED, OrderStatus.PENDING_TRIGGER):
            error_msg = f"submit_existing_entry_invalid_status:{status}"
            log.critical("[%s] submit_existing_entry blocked -- %s | %s",
                         self.client_id, error_msg, local_order_id)
            return {"ok": False, "local_order_id": local_order_id,
                    "broker_order_id": current.get("broker_order_id"),
                    "status": status, "error": error_msg}

        lp       = float(limit_price or current.get("limit_price") or getattr(plan, "limit_price", 0) or 0)
        # ── CONTRACT RESOLUTION — prefer live plan over stale DB row ──────────
        # DB order row may contain DEFERRED:TICKER (set during overnight premarket
        # when no chain was available). At breach time, execution_core runs live
        # contract selection and updates plan.contract_symbol with the real OCC symbol.
        # We MUST use the plan's live contract, not the stale placeholder.
        _db_contract   = str(current.get("contract") or "").strip()
        _plan_contract = str(getattr(plan, "contract_symbol", "") or "").strip()
        _ticker        = current.get("symbol") or getattr(plan, "ticker", "")

        _db_is_deferred = (
            not _db_contract
            or _db_contract.upper().startswith("DEFERRED:")
            or _db_contract.upper() == str(_ticker).upper()
        )

        if _db_is_deferred and _plan_contract and not _plan_contract.upper().startswith("DEFERRED:"):
            # Use live plan contract — breach-time selection resolved it
            contract = _plan_contract
            log.info(
                "[%s] CONTRACT_RESOLVED: DB had placeholder %r → using plan %r",
                _ticker, _db_contract, contract,
            )
            # Update the order row so DB is consistent before broker POST
            try:
                self._db.table("orders").update({
                    "contract":    contract,
                    "limit_price": round(lp, 2),
                    "qty":         int(getattr(plan, "contracts", 0) or current.get("qty") or 0),
                    "updated_ts":  now_utc_iso(),
                }).eq("local_order_id", local_order_id).execute()
            except Exception as _upd_exc:
                log.warning("[%s] Failed to update order contract pre-submit: %s", _ticker, _upd_exc)
        else:
            contract = _db_contract or _plan_contract or _ticker

        # HARD BLOCK — never submit a DEFERRED placeholder to Tradier
        if not contract or contract.upper().startswith("DEFERRED:") or contract.upper() == str(_ticker).upper():
            error_msg = f"DEFERRED_CONTRACT_BLOCKED:{contract}"
            log.critical(
                "[%s] CRITICAL — submit_existing_entry blocked: contract=%r is still a placeholder. "
                "Breach-time contract selection may have failed. Blocking broker POST.",
                _ticker, contract,
            )
            self.transition(local_order_id, OrderStatus.ERROR, last_error=error_msg)
            return {"ok": False, "local_order_id": local_order_id,
                    "broker_order_id": current.get("broker_order_id"),
                    "status": OrderStatus.ERROR, "error": error_msg}

        ticker   = _ticker
        qty      = int(current.get("qty") or getattr(plan, "contracts", 0) or 0)

        if lp <= 0:
            error_msg = "invalid_existing_entry_limit_price"
            self.transition(local_order_id, OrderStatus.ERROR, last_error=error_msg)
            return {"ok": False, "local_order_id": local_order_id,
                    "broker_order_id": current.get("broker_order_id"),
                    "status": OrderStatus.ERROR, "error": error_msg}
        if qty <= 0:
            error_msg = "invalid_existing_entry_qty"
            self.transition(local_order_id, OrderStatus.ERROR, last_error=error_msg)
            return {"ok": False, "local_order_id": local_order_id,
                    "broker_order_id": current.get("broker_order_id"),
                    "status": OrderStatus.ERROR, "error": error_msg}
        if not contract:
            error_msg = "missing_existing_entry_contract"
            self.transition(local_order_id, OrderStatus.ERROR, last_error=error_msg)
            return {"ok": False, "local_order_id": local_order_id,
                    "broker_order_id": current.get("broker_order_id"),
                    "status": OrderStatus.ERROR, "error": error_msg}

        # Stale-read protection before broker POST
        latest = self._get_order(local_order_id)
        if not latest:
            return {"ok": False, "local_order_id": local_order_id, "broker_order_id": None,
                    "status": OrderStatus.ERROR,
                    "error": "existing_entry_order_disappeared_before_submit"}
        latest        = dict(latest)
        latest_status = str(latest.get("status") or "").upper()
        if latest_status in (OrderStatus.REJECTED, OrderStatus.CANCELED,
                              OrderStatus.EXPIRED, OrderStatus.ERROR):
            return {"ok": False, "local_order_id": local_order_id,
                    "broker_order_id": latest.get("broker_order_id"),
                    "status": latest_status,
                    "error": f"submit_existing_entry_terminal_status:{latest_status}"}
        if latest_status not in (OrderStatus.CREATED, OrderStatus.PENDING_TRIGGER):
            return {"ok": False, "local_order_id": local_order_id,
                    "broker_order_id": latest.get("broker_order_id"),
                    "status": latest_status,
                    "error": f"submit_existing_entry_invalid_status:{latest_status}"}

        base_url   = (getattr(broker, "base_url", None)
                      or getattr(getattr(broker, "cfg", None), "base_url", None)
                      or "https://sandbox.tradier.com")
        account_id = (getattr(broker, "account_id", None)
                      or getattr(getattr(broker, "cfg", None), "account_id", None)
                      or "")
        error_msg = broker_order_id = None
        try:
            resp = broker.session.post(
                f"{base_url}/v1/accounts/{account_id}/orders",
                data={
                    "class": "option", "symbol": ticker, "option_symbol": contract,
                    "side": "buy_to_open", "quantity": qty,
                    "type": "limit", "price": round(lp, 2), "duration": "day",
                },
                headers={"Accept": "application/json"},
                timeout=10,
            )
            order          = (resp.json() or {}).get("order") or {}
            broker_status  = str(order.get("status") or "").lower().strip()
            broker_order_id = order.get("id") or order.get("order_id")
            if self._is_broker_accept_status(broker_status) and broker_order_id:
                ok = self.transition(local_order_id, OrderStatus.SUBMITTED,
                                     broker_order_id=broker_order_id, submitted_ts=now_utc_iso())
                if ok:
                    return {"ok": True, "local_order_id": local_order_id,
                            "broker_order_id": broker_order_id,
                            "status": OrderStatus.SUBMITTED, "error": None}
                error_msg = "submitted_transition_failed_after_broker_accept"
                self._flag_split_brain_order(local_order_id, broker_order_id=broker_order_id,
                                             error_msg=error_msg)
                return {"ok": False, "local_order_id": local_order_id,
                        "broker_order_id": broker_order_id, "status": OrderStatus.ERROR,
                        "error": error_msg, "split_brain": True}
            else:
                error_msg = (f"broker_status:{broker_status or 'unknown'} "
                             f"broker_order_id_missing:{not bool(broker_order_id)}")
        except Exception as e:
            error_msg = f"broker_error:{e}"

        # AUDIT-7: include broker rejection reason in observability event so
        # structured audit trail captures WHY Tradier rejected the order.
        self.transition(local_order_id, OrderStatus.ERROR,
                        last_error=error_msg or "unknown_error")
        self._emit_transition_event(
            local_order_id=local_order_id,
            old_status=latest_status,
            new_status=OrderStatus.ERROR,
            order=current,
            decision="ERROR",
            reason_code="BROKER_REJECTED_ENTRY",
            explanation=error_msg or "unknown_error",
            broker_order_id=broker_order_id,
            extra_inputs={"broker_rejection_reason": error_msg or "unknown_error"},
        )
        return {"ok": False, "local_order_id": local_order_id, "broker_order_id": broker_order_id,
                "status": OrderStatus.ERROR, "error": error_msg}

    def submit_entry(
        self,
        *,
        broker,
        plan,
        limit_price=None,
        reserved_cost=None,
    ) -> dict:
        local_id = self.create_entry_order(plan, limit_price=limit_price, reserved_cost=reserved_cost)
        lp       = float(limit_price or getattr(plan, "limit_price", 0) or 0)
        symbol   = getattr(plan, "contract_symbol", None) or plan.ticker
        if lp <= 0:
            error_msg = "invalid_entry_limit_price"
            self.transition(local_id, OrderStatus.ERROR, last_error=error_msg)
            return {"ok": False, "local_order_id": local_id, "broker_order_id": None,
                    "status": OrderStatus.ERROR, "error": error_msg}

        base_url   = (getattr(broker, "base_url", None)
                      or getattr(getattr(broker, "cfg", None), "base_url", None)
                      or "https://sandbox.tradier.com")
        account_id = (getattr(broker, "account_id", None)
                      or getattr(getattr(broker, "cfg", None), "account_id", None)
                      or "")
        error_msg = broker_order_id = None
        try:
            resp = broker.session.post(
                f"{base_url}/v1/accounts/{account_id}/orders",
                data={
                    "class": "option", "symbol": plan.ticker, "option_symbol": symbol,
                    "side": "buy_to_open", "quantity": int(plan.contracts),
                    "type": "limit", "price": round(lp, 2), "duration": "day",
                },
                headers={"Accept": "application/json"},
                timeout=10,
            )
            order           = (resp.json() or {}).get("order") or {}
            status          = str(order.get("status") or "").lower().strip()
            broker_order_id = order.get("id") or order.get("order_id")
            if self._is_broker_accept_status(status) and broker_order_id:
                ok = self.transition(local_id, OrderStatus.SUBMITTED,
                                     broker_order_id=broker_order_id, submitted_ts=now_utc_iso())
                if ok:
                    return {"ok": True, "local_order_id": local_id,
                            "broker_order_id": broker_order_id,
                            "status": OrderStatus.SUBMITTED, "error": None}
                # AUDIT-3: split-brain path now matches submit_existing_entry.
                # Previously: moved to ERROR with no flag, no split_brain key.
                # Now: flags the order, preserves broker_order_id, returns split_brain=True
                # so the runner can freeze the client and the reconciler can recover.
                error_msg = "submitted_transition_failed_after_broker_accept"
                self._flag_split_brain_order(
                    local_id,
                    broker_order_id=broker_order_id,
                    error_msg=error_msg,
                )
                return {
                    "ok":             False,
                    "local_order_id": local_id,
                    "broker_order_id": broker_order_id,
                    "status":         OrderStatus.ERROR,
                    "error":          error_msg,
                    "split_brain":    True,
                }
            else:
                error_msg = (f"broker_status:{status or 'unknown'} "
                             f"broker_order_id_missing:{not bool(broker_order_id)}")
        except Exception as e:
            error_msg = f"broker_error:{e}"

        # AUDIT-7: broker rejection reason in observability
        self.transition(local_id, OrderStatus.ERROR, last_error=error_msg or "unknown_error")
        self._emit_transition_event(
            local_order_id=local_id,
            old_status=OrderStatus.CREATED,
            new_status=OrderStatus.ERROR,
            decision="ERROR",
            reason_code="BROKER_REJECTED_ENTRY",
            explanation=error_msg or "unknown_error",
            broker_order_id=broker_order_id,
            extra_inputs={"broker_rejection_reason": error_msg or "unknown_error"},
        )
        return {"ok": False, "local_order_id": local_id, "broker_order_id": broker_order_id,
                "status": OrderStatus.ERROR, "error": error_msg}

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
        order_type: str = "limit",
    ) -> dict:
        existing = self._get_active_exit_order(position_id)
        if existing:
            existing  = dict(existing)
            error_msg = (f"active_exit_already_exists:"
                         f"{existing.get('local_order_id')}:{existing.get('status')}")
            log.critical(
                "[%s] submit_exit BLOCKED -- active exit already exists | "
                "pos=%s existing=%s status=%s broker=%s",
                self.client_id, position_id,
                existing.get("local_order_id"), existing.get("status"),
                existing.get("broker_order_id"),
            )
            return {"ok": False, "local_order_id": existing.get("local_order_id"),
                    "broker_order_id": existing.get("broker_order_id"),
                    "status": existing.get("status"), "error": error_msg}

        local_id = self.create_exit_order(
            position_id=position_id, contract=contract, symbol=symbol,
            direction=direction, qty=qty, plan_id=plan_id, signal_id=signal_id,
            limit_price=limit_price,
        )
        lp = float(limit_price or 0)
        _is_market = (order_type == "market") or (lp <= 0 and order_type != "limit")
        if lp <= 0 and not _is_market:
            error_msg = "invalid_exit_limit_price"
            self.transition(local_id, OrderStatus.ERROR, last_error=error_msg)
            return {"ok": False, "local_order_id": local_id, "broker_order_id": None,
                    "status": OrderStatus.ERROR, "error": error_msg}

        base_url   = (getattr(broker, "base_url", None)
                      or getattr(getattr(broker, "cfg", None), "base_url", None)
                      or "https://sandbox.tradier.com")
        account_id = (getattr(broker, "account_id", None)
                      or getattr(getattr(broker, "cfg", None), "account_id", None)
                      or "")
        underlying    = self._resolve_underlying_symbol(symbol=symbol, contract=contract)
        error_msg     = broker_order_id = None
        try:
            _is_market = (order_type == "market") or (lp <= 0 and order_type != "limit")
            _order_data = {
                "class": "option", "symbol": underlying, "option_symbol": contract,
                "side": "sell_to_close", "quantity": int(qty),
                "type": "market" if _is_market else "limit",
                "duration": "day",
            }
            if not _is_market:
                _order_data["price"] = round(lp, 2)
            resp = broker.session.post(
                f"{base_url}/v1/accounts/{account_id}/orders",
                data=_order_data,
                headers={"Accept": "application/json"},
                timeout=10,
            )
            order           = (resp.json() or {}).get("order") or {}
            status          = str(order.get("status") or "").lower().strip()
            broker_order_id = order.get("id") or order.get("order_id")
            if self._is_broker_accept_status(status) and broker_order_id:
                ok = self.transition(local_id, OrderStatus.EXIT_SUBMITTED,
                                     broker_order_id=broker_order_id, submitted_ts=now_utc_iso())
                if ok:
                    return {"ok": True, "local_order_id": local_id,
                            "broker_order_id": broker_order_id,
                            "status": OrderStatus.EXIT_SUBMITTED, "error": None}
                error_msg = "exit_submitted_transition_failed_after_broker_accept"
                self._flag_split_brain_order(local_id, broker_order_id=broker_order_id,
                                             error_msg=error_msg)
                return {"ok": False, "local_order_id": local_id,
                        "broker_order_id": broker_order_id, "status": OrderStatus.ERROR,
                        "error": error_msg, "split_brain": True}
            elif self._is_broker_accept_status(status) and not broker_order_id:
                error_msg = f"broker_accepted_missing_order_id_quarantine:status={status or 'unknown'}"
                ok = self.transition(local_id, OrderStatus.EXIT_SUBMITTED,
                                     submitted_ts=now_utc_iso(), last_error=error_msg)
                return {"ok": False, "local_order_id": local_id, "broker_order_id": None,
                        "status": OrderStatus.EXIT_SUBMITTED if ok else OrderStatus.ERROR,
                        "error": error_msg, "identity_quarantine": True}
            else:
                error_msg = (f"broker_status:{status or 'unknown'} "
                             f"broker_order_id_missing:{not bool(broker_order_id)}")
        except Exception as e:
            error_msg = f"broker_error:{e}"

        self.transition(local_id, OrderStatus.ERROR, last_error=error_msg or "unknown_error")
        self._emit_transition_event(
            local_order_id=local_id,
            old_status=OrderStatus.EXIT_REQUESTED,
            new_status=OrderStatus.ERROR,
            decision="ERROR",
            reason_code="BROKER_REJECTED_EXIT",
            explanation=error_msg or "unknown_error",
            broker_order_id=broker_order_id,
            extra_inputs={"broker_rejection_reason": error_msg or "unknown_error"},
        )
        return {"ok": False, "local_order_id": local_id, "broker_order_id": broker_order_id,
                "status": OrderStatus.ERROR, "error": error_msg}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _flag_split_brain_order(
        self,
        local_order_id: str,
        *,
        broker_order_id: str,
        error_msg: str,
    ) -> None:
        """
        FIX-J: Write split-brain marker when broker accepted but DB transition failed.
        Preserves broker_order_id via COALESCE and prefixes last_error with SPLIT_BRAIN:.
        """
        try:
            def _mark():
                with conn() as c:
                    c.execute(
                        """
                        UPDATE orders
                        SET broker_order_id = COALESCE(NULLIF(broker_order_id, ''), %s),
                            last_error      = %s,
                            updated_ts      = NOW()
                        WHERE local_order_id = %s AND client_id = %s
                        """,
                        (broker_order_id, f"SPLIT_BRAIN:{error_msg}",
                         local_order_id, self.client_id),
                    )
            run_with_retry(_mark)
            log.critical(
                "[%s] SPLIT_BRAIN FLAGGED | order=%s broker=%s | "
                "broker accepted but DB transition failed; reconciler will recover on next pass",
                self.client_id, local_order_id, broker_order_id,
            )
        except Exception as flag_err:
            log.critical(
                "[%s] SPLIT_BRAIN FLAG WRITE FAILED | order=%s broker=%s | "
                "MANUAL INTERVENTION REQUIRED -- broker has live order with no DB record | "
                "flag_error=%s",
                self.client_id, local_order_id, broker_order_id, flag_err,
            )

    def get_split_brain_orders(self) -> list:
        """Return all orders flagged as split-brain for this client."""
        def _fn():
            with conn() as c:
                c.execute(
                    """
                    SELECT *
                    FROM orders
                    WHERE client_id = %s
                      AND last_error LIKE 'SPLIT_BRAIN:%%'
                    ORDER BY created_ts DESC
                    """,
                    (self.client_id,),
                )
                return c.fetchall()
        try:
            rows = run_with_retry(_fn) or []
            return [dict(r) for r in rows]
        except Exception as e:
            log.error("[%s] get_split_brain_orders failed: %s", self.client_id, e)
            return []

    def _get_position_remaining_from_db(self, position_id: str) -> Optional[int]:
        """
        AUDIT-2: This method is now a proper separate method with its own indentation.

        Previously its body was dead code appended after get_split_brain_orders's
        return statements — Python didn't error but the code was unreachable, so
        the method always returned None. That caused every EXIT_FILLED event to
        take the "remaining size unknown" path (conservative partial handling via
        note_partial_exit_fill) instead of mark_position_closed. The OSM full-close
        path was structurally broken.

        Returns the current quantity_remaining from the positions table, or None
        if the position cannot be found or the query fails.
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
            # Fallback: quantity_remaining is NULL in DB.
            # This is correct on first fill (before any scale-out), but if it fires
            # after a scale-out it means DB is not being updated — partial close will
            # be silently treated as full close on the next fill.
            log.critical(
                "[%s] _get_position_remaining_from_db: quantity_remaining is NULL for pos=%s "
                "falling back to qty=%s — if this fires after a scale-out, "
                "quantity_remaining is not being updated in DB",
                self.client_id, position_id, row.get("qty"),
            )
            return int(row.get("qty") or 0)
        except Exception:
            return None

    @staticmethod
    def _resolve_underlying_symbol(*, symbol: str, contract: str) -> str:
        raw_symbol   = (symbol or "").strip().upper()
        raw_contract = (contract or "").strip().upper()
        if raw_symbol and not any(ch.isdigit() for ch in raw_symbol):
            return raw_symbol
        m = re.match(r"^([A-Z]{1,10})\d{6}[CP]", raw_contract or raw_symbol)
        if m:
            return m.group(1)
        return raw_symbol if raw_symbol else raw_contract

    def _finalize_position_from_exit_order(
        self,
        local_order_id: str,
        fill_price: float | None = None,
        filled_qty: int | None = None,
    ) -> None:
        """
        Copy confirmed broker fill truth from orders → positions after EXIT_FILLED.

        Production rule: broker fill > order row > position row > dashboard.
        Called immediately after mark_position_closed so the position row is
        finalized with real fill price, P&L, and close metadata.
        Non-fatal — any error is logged and swallowed; it never blocks the exit path.
        """
        try:
            order = self._get_order(local_order_id)
            if not order:
                log.warning("[%s] _finalize_position_from_exit_order: order not found %s",
                            self.client_id, local_order_id)
                return

            position_id    = str(order.get("position_id") or "")
            _fill_price    = fill_price if fill_price is not None else order.get("fill_price")
            _filled_qty    = filled_qty if filled_qty is not None else order.get("filled_qty")
            _filled_ts     = order.get("filled_ts")
            _broker_id     = str(order.get("broker_order_id") or "")

            if not position_id or _fill_price is None or int(_filled_qty or 0) <= 0:
                log.warning(
                    "[%s] _finalize_position_from_exit_order skipped — incomplete fill data | "
                    "order=%s pos=%s price=%r qty=%r",
                    self.client_id, local_order_id, position_id, _fill_price, _filled_qty,
                )
                return

            from ap.position_manager import APPositionManager
            pm = APPositionManager(self.client_id)
            pm.close_position_from_exit_fill(
                position_id=position_id,
                exit_price=float(_fill_price),
                filled_qty=int(_filled_qty),
                filled_ts=str(_filled_ts) if _filled_ts else now_utc_iso(),
                local_order_id=local_order_id,
                broker_order_id=_broker_id,
                close_source="broker_exit_fill",
                close_confidence="HIGH",
                exit_reason="exit_filled",
            )
        except Exception as exc:
            log.error(
                "[%s] _finalize_position_from_exit_order failed | order=%s | %s",
                self.client_id, local_order_id, exc, exc_info=True,
            )

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
                    "AND status NOT IN ('FILLED','EXIT_FILLED','REJECTED','CANCELED','EXPIRED','ERROR') "
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
                    "AND status NOT IN ('EXIT_FILLED','REJECTED','CANCELED','EXPIRED','ERROR') "
                    "ORDER BY created_ts DESC LIMIT 1",
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


# =============================================================================
# Backward-compatible shims
# =============================================================================

def install_exit_quarantine_patch(osm_cls):
    """No-op shim — all protections are now native to APOrderStateMachine."""
    return osm_cls


def _install_reconciler_idempotency_patch() -> bool:
    """No-op shim — ap_reconciler.py has native RLock + monotonic-clock idempotency."""
    return True


def _install_exit_performance_patch() -> bool:
    """No-op shim — performance tracking is native in _handle_exit_engine_hooks (FIX-F)."""
    return True


__all__ = [
    "install_exit_quarantine_patch",
    "APOrderStateMachine",
    "OrderStatus",
    "register_exit_engine",
    "unregister_exit_engine",
    "probe_db_rowcount",
    "PENDING_ENTRY_STATUSES",
    "PENDING_EXIT_STATUSES",
]
