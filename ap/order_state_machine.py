# ap/order_state_machine.py — APOrderStateMachine
# =============================================================================
# Enforced order lifecycle controller.
#
# Canonical order-state authority:
#   - validates legal state transitions
#   - persists order status updates
#   - supports watcher-armed queue entries via PENDING_TRIGGER
#   - submits queue-created entries only after watcher breach
#   - coordinates exit-engine hooks after broker-confirmed exit events
#   - emits structured observability for transition success/failure
#
# CONTRACT: Any caller that passes filled_qty MUST pass broker cumulative filled
# quantity for that order, not an incremental fill delta.
#
# Merged from order_state_machine_exit_quarantine_patch.py
# --------------------------------------------------------
# All protections previously installed as runtime monkey-patches are now native
# to this class. The external patch file and install_exit_quarantine_patch()
# are retained as backward-compatible no-op shims so client_runner.py call
# sites do not need to be changed.
#
# Bug-fix history (see inline FIX-N tags):
#   FIX-A  _handle_exit_engine_hooks: EXIT_SUBMITTED with missing broker_order_id
#          now quarantines the exit engine via set_pending_exit_order(...,
#          identity_quarantine=True) instead of only logging a critical warning.
#          Prevents duplicate exit submissions while broker identity is recovered.
#   FIX-B  transition(): rowcount None bypass — `None == 0` is False in Python,
#          so when the DB driver returns None for rowcount a failed UPDATE
#          silently returned True. Now a HARD FAILURE by default. Controlled by
#          OSM_ROWCOUNT_NONE_IS_FATAL (default "1"). Set to "0" only for DB
#          driver wrappers that are known never to expose rowcount and are
#          separately proven correct by integration tests. The previous version
#          of this fix overstated the result — "log and proceed" still allowed
#          state to advance without confirmed persistence, which is exactly the
#          split-brain class of bug this fix is supposed to close.
#   FIX-C  _call_exit_engine(): strict call raised on any exit engine signature
#          mismatch. Now retries with optional/extended kwargs stripped so version
#          skew between OSM and exit engine does not kill the entire hook chain.
#   FIX-D  REJECTED cooldown loop: iterated `_positions` directly; if the exit
#          engine stores positions in a dict (keyed by position_id), the loop
#          yielded string keys, not position objects — cooldown never fired.
#          Now uses .values() when _positions is a dict.
#   FIX-E  _safe_int(): added None-safe default so callers can distinguish "not
#          provided" from zero without abusing a sentinel value. Cumulative fill
#          checks throughout depend on this distinction.
#   FIX-F  Performance tracking: record_trade_outcome_from_position() is now
#          called natively on the full-close path inside _handle_exit_engine_hooks
#          immediately before mark_position_closed(). The exit engine patch that
#          previously monkey-patched mark_position_closed is replaced by this.
#   FIX-G  Reconciler idempotency patch: ap_reconciler.py has native RLock +
#          monotonic timestamp idempotency. The external patch used threading.Lock
#          and wall clock on different attribute keys and is now a no-op shim.
#   FIX-H  cum_filled zero-order-qty edge case: when both cum_filled and order_qty
#          are zero, the handler was silently clearing exit-in-flight — releasing
#          engine protection on ambiguous fill data. A prior version of this fix
#          still called clear_exit_in_flight() which is operationally identical to
#          the original bug in a bad-broker-data scenario. Now calls
#          set_pending_exit_order(..., identity_quarantine=True) so the engine stays
#          locked and requires reconciler resolution. No engine protection is released
#          on an EXIT_FILLED event with zero quantity data.
#   FIX-I  CANCELED/EXPIRED/REJECTED direct engine calls: on_exit_failure() and
#          clear_exit_in_flight() were called directly on the exit engine, bypassing
#          _call_exit_engine()'s TypeError fallback. Now routed through the wrapper
#          so version-skew / signature mismatch is handled consistently.
#   FIX-J  Split-brain freeze: when the broker accepts a submission but the OSM DB
#          transition fails, the order is immediately flagged with a SPLIT_BRAIN:
#          prefix in last_error and the broker_order_id is persisted via a direct
#          COALESCE UPDATE. This prevents the watcher/runner from re-submitting the
#          same order while the reconciler recovers the identity. The return dict
#          includes split_brain=True so the runner can freeze the client.
#          get_split_brain_orders() allows startup audit of prior-session residue.
#   FIX-K  probe_db_rowcount(): module-level function that tests actual rowcount
#          behavior of the active DB driver before any APOrderStateMachine instance
#          is created. Runners should call this at boot and fail-fast if the result
#          is None and OSM_ROWCOUNT_NONE_IS_FATAL=1.
# =============================================================================

from __future__ import annotations

import logging
import os
import re
import threading
import time as _time_module
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

# FIX-B: Controls whether a None rowcount from the DB driver is treated as a hard
# failure. Default is True (strict). Set OSM_ROWCOUNT_NONE_IS_FATAL=0 ONLY for DB
# driver wrappers that are verified by integration tests never to expose rowcount
# AND that never silently fail UPDATE statements. In any other environment, None
# rowcount means the OSM cannot confirm the state change was persisted and must
# refuse to advance state to avoid split-brain between DB and in-process truth.
_ROWCOUNT_NONE_IS_FATAL: bool = (
    os.getenv("OSM_ROWCOUNT_NONE_IS_FATAL", "1").strip().lower()
    not in {"0", "false", "no", "off"}
)

def probe_db_rowcount() -> Optional[int]:
    """
    Test whether the active DB driver exposes rowcount on UPDATE statements.

    Call this ONCE at runner startup, before creating any APOrderStateMachine
    instances. If the probe returns None and OSM_ROWCOUNT_NONE_IS_FATAL=1
    (the default), every call to transition() will refuse to advance state,
    stalling order lifecycle updates across the entire system.

    Expected call:
        result = probe_db_rowcount()
        if result is None and _ROWCOUNT_NONE_IS_FATAL:
            # Either fix the DB wrapper or set OSM_ROWCOUNT_NONE_IS_FATAL=0
            # only after proving your wrapper never silently drops writes.
            raise RuntimeError("DB driver does not expose rowcount — OSM will stall")

    Returns:
        int  — rowcount value from the driver (0 expected for the no-op UPDATE)
        None — driver does not expose rowcount; transitions will fail in strict mode
    """
    try:
        def _probe():
            with conn() as c:
                cur = c.execute("UPDATE orders SET updated_ts=updated_ts WHERE 1=0")
                return getattr(cur, "rowcount", getattr(c, "rowcount", None))

        result = run_with_retry(_probe)
        if result is None:
            log.critical(
                "DB ROWCOUNT PROBE → None | OSM_ROWCOUNT_NONE_IS_FATAL=%s | "
                "strict mode will refuse ALL transitions until this is resolved; "
                "run: cur = c.execute('UPDATE orders SET updated_ts=updated_ts WHERE 1=0'); "
                "print(cur.rowcount) — if None, fix driver or set OSM_ROWCOUNT_NONE_IS_FATAL=0",
                "1 (strict — transitions will stall)" if _ROWCOUNT_NONE_IS_FATAL else "0 (permissive)",
            )
        else:
            log.info(
                "DB rowcount probe → %r (int) — OSM strict rowcount check is safe",
                result,
            )
        return result
    except Exception as exc:
        log.error("DB rowcount probe failed with exception: %s", exc)
        return None


_exit_engine_registry: dict[str, object] = {}
_registry_lock = threading.Lock()


def _normalize_client_key(client_id: str) -> str:
    """
    Normalize a client_id/email for use as a registry key.

    Lowercase + strip so that caller casing differences between the runner that
    registers the engine and the OSM instance that looks it up cannot cause a
    silent miss.  A miss means all exit hooks are skipped — no quarantine, no
    pending exit tracking, no full-close, no performance recording.
    """
    return str(client_id or "").strip().lower()


def register_exit_engine(*args, **kwargs) -> None:
    """
    Register a per-client exit engine for broker-confirmed exit hooks.

    Supports both call signatures for backward compatibility:

        register_exit_engine(client_id, exit_engine)   # explicit — preferred
        register_exit_engine(exit_engine)              # engine carries client_id

    In the single-arg form, client_id is resolved from keyword arg or from
    ``exit_engine.client_id`` / ``.email`` / ``._email`` attributes.
    Raises ValueError if client_id cannot be determined.

    Keys are normalized (lowercase + strip) so casing differences between the
    runner that registers and the OSM that looks up cannot cause a silent miss.
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
            "register_exit_engine requires both client_id and exit_engine. "
            "In single-arg form the engine must expose a client_id, email, or _email attribute."
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
    # Entry states
    CREATED          = "CREATED"
    PENDING_TRIGGER  = "PENDING_TRIGGER"  # watcher armed, no broker order yet
    SUBMITTED        = "SUBMITTED"
    ACKNOWLEDGED     = "ACKNOWLEDGED"
    PARTIAL_FILL     = "PARTIAL_FILL"
    FILLED           = "FILLED"

    # Exit states
    EXIT_REQUESTED   = "EXIT_REQUESTED"
    EXIT_SUBMITTED   = "EXIT_SUBMITTED"
    EXIT_ACKNOWLEDGED = "EXIT_ACKNOWLEDGED"
    EXIT_PARTIAL_FILL = "EXIT_PARTIAL_FILL"
    EXIT_FILLED      = "EXIT_FILLED"

    # Terminal failure states
    REJECTED = "REJECTED"
    CANCELED = "CANCELED"
    EXPIRED  = "EXPIRED"
    ERROR    = "ERROR"

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
        self.client_id       = client_id
        self.run_id          = os.getenv("AP_RUN_ID", "unknown")
        self.strategy_version = os.getenv("AP_STRATEGY_VERSION", "ap_live_beta")
        self.git_commit      = get_git_commit()
        self.config_hash     = make_config_hash(
            {
                "entry_active": sorted(PENDING_ENTRY_STATUSES),
                "exit_active":  sorted(PENDING_EXIT_STATUSES),
                "terminal":     sorted(OrderStatus.TERMINAL),
                "transitions":  {k: sorted(v) for k, v in OrderStatus.TRANSITIONS.items()},
            }
        )
        log.info("[%s] APOrderStateMachine initialized", client_id)

    # ------------------------------------------------------------------
    # Internal utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _safe_int(value, default=0):
        """
        Convert value to int, returning default on failure.

        FIX-E: default is intentionally untyped so callers can pass None to
        distinguish "not provided" from zero. Cumulative fill checks rely on
        this — passing default=None lets callers write `if x is None` rather
        than `if x == 0` which conflates a real zero-fill with missing data.
        Also uses int(float(value)) rather than int(value) so string
        representations like "1.0" parse correctly.
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

        FIX-C: the original implementation was a strict pass-through that let
        any TypeError from unknown kwargs propagate and kill the entire hook
        chain. Exit engine interface evolves independently; unknown optional
        kwargs (identity_quarantine, reconciled, status) are now stripped and
        retried once before the error is surfaced. This keeps the hook chain
        alive across version skew without hiding genuine errors.
        """
        method = getattr(exit_engine, method_name, None)
        if not method:
            return None
        try:
            return method(*args, **kwargs)
        except TypeError:
            # Strip optional extension kwargs and retry once.
            for drop_key in ("identity_quarantine", "reconciled", "status"):
                kwargs.pop(drop_key, None)
            try:
                return method(*args, **kwargs)
            except Exception:
                log.exception(
                    "[OSM] exit engine hook %s failed after filtered retry", method_name
                )
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
        # FIX-D (partial): normalise dict vs list for the get-by-id case.
        if isinstance(raw, dict):
            pos = raw.get(position_id)
            if pos is not None:
                return pos
            # dict keyed by something other than position_id — fall through to scan
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
                "[%s] create_entry_order SKIPPED — plan %s already has order %s",
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

        run_with_retry(_fn)
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
                "[%s] create_exit_order SKIPPED — position %s already has exit order %s",
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

        run_with_retry(_fn)
        log.info(
            "[%s] ORDER CREATED (exit) | %s x%s pos=%s | local_order_id=%s",
            self.client_id, contract, qty, position_id, local_id,
        )
        return local_id

    # ------------------------------------------------------------------
    # Watcher/queue entry-state helpers
    # ------------------------------------------------------------------

    def mark_entry_pending_trigger(self, local_order_id: str) -> bool:
        """Mark a queue-created ENTRY as watcher-armed and waiting for breach."""
        current = self._get_order(local_order_id)
        if not current:
            log.error("[%s] mark_entry_pending_trigger: order %s not found",
                      self.client_id, local_order_id)
            return False

        current = dict(current)
        kind   = str(current.get("kind") or "").upper()
        status = str(current.get("status") or "").upper()

        if kind != "ENTRY":
            log.critical(
                "[%s] mark_entry_pending_trigger blocked — wrong kind %s for order=%s",
                self.client_id, kind, local_order_id,
            )
            return False
        if status == OrderStatus.PENDING_TRIGGER:
            return True
        if status != OrderStatus.CREATED:
            log.warning(
                "[%s] mark_entry_pending_trigger blocked — invalid status %s for order=%s",
                self.client_id, status, local_order_id,
            )
            return False

        return self.transition(local_order_id, OrderStatus.PENDING_TRIGGER)

    def expire_pending_entry(self, local_order_id: str, *, reason: str = "watcher_expired") -> bool:
        """Move a non-submitted watcher entry to EXPIRED after watcher expiry."""
        current = self._get_order(local_order_id)
        if not current:
            log.error("[%s] expire_pending_entry: order %s not found",
                      self.client_id, local_order_id)
            return False

        current = dict(current)
        kind   = str(current.get("kind") or "").upper()
        status = str(current.get("status") or "").upper()

        if kind != "ENTRY":
            log.critical("[%s] expire_pending_entry blocked — wrong kind %s | %s",
                         self.client_id, kind, local_order_id)
            return False
        if status == OrderStatus.EXPIRED:
            return True
        if status not in (OrderStatus.CREATED, OrderStatus.PENDING_TRIGGER):
            log.warning("[%s] expire_pending_entry blocked — status=%s order=%s",
                        self.client_id, status, local_order_id)
            return False

        return self.transition(local_order_id, OrderStatus.EXPIRED, last_error=reason)

    def cancel_pending_entry(self, local_order_id: str, *, reason: str = "watcher_invalidated") -> bool:
        """Move a non-submitted watcher entry to CANCELED after invalidation."""
        current = self._get_order(local_order_id)
        if not current:
            log.error("[%s] cancel_pending_entry: order %s not found",
                      self.client_id, local_order_id)
            return False

        current = dict(current)
        kind   = str(current.get("kind") or "").upper()
        status = str(current.get("status") or "").upper()

        if kind != "ENTRY":
            log.critical("[%s] cancel_pending_entry blocked — wrong kind %s | %s",
                         self.client_id, kind, local_order_id)
            return False
        if status == OrderStatus.CANCELED:
            return True
        if status not in (OrderStatus.CREATED, OrderStatus.PENDING_TRIGGER):
            log.warning("[%s] cancel_pending_entry blocked — status=%s order=%s",
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
                log.critical(
                    "[%s] INVALID FILL QTY | order=%s filled_qty=%r",
                    self.client_id, local_order_id, filled_qty,
                )
                return False
            if incoming_filled < prev_filled:
                log.critical(
                    "[%s] INVALID CUMULATIVE FILL REGRESSION | order=%s new=%s prev=%s",
                    self.client_id, local_order_id, incoming_filled, prev_filled,
                )
                self._emit_transition_event(
                    local_order_id=local_order_id,
                    old_status=old_status,
                    new_status=new_status,
                    order=current,
                    decision="REJECT",
                    reason_code="FILL_QTY_REGRESSION",
                    explanation=f"filled_qty must be cumulative: new={incoming_filled} prev={prev_filled}",
                    broker_order_id=broker_order_id,
                    filled_qty=filled_qty,
                    fill_price=fill_price,
                    last_error=last_error,
                )
                return False

        if old_status == new_status:
            if new_status in (OrderStatus.PARTIAL_FILL, OrderStatus.EXIT_PARTIAL_FILL):
                same_state_fill_update = True
            else:
                log.debug("[%s] %s already %s — no-op", self.client_id, local_order_id, new_status)
                return True

        if OrderStatus.is_terminal(old_status):
            reason = f"terminal_transition_blocked:{old_status}->{new_status}"
            log.warning(
                "[%s] TRANSITION BLOCKED — %s already terminal (%s), cannot move to %s",
                self.client_id, local_order_id, old_status, new_status,
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
        params  = [new_status]
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
                return getattr(cur, "rowcount", getattr(c, "rowcount", None))

        rowcount = run_with_retry(_fn)

        # FIX-B: Both `rowcount == 0` (zero rows updated) and `rowcount is None`
        # (driver does not expose rowcount) are treated as hard failures by default.
        # `None == 0` is False in Python, so the original code let None silently
        # return True — the DB update was unconfirmed but state advanced anyway.
        # That is the exact split-brain class of bug this fix closes.
        #
        # Controlled by _ROWCOUNT_NONE_IS_FATAL (env: OSM_ROWCOUNT_NONE_IS_FATAL,
        # default True). Set to False ONLY for DB drivers proven by integration tests
        # to never expose rowcount yet never silently fail UPDATE statements.
        if rowcount == 0:
            reason = f"transition_update_no_rows:{old_status}->{new_status}"
            log.critical(
                "[%s] OSM UPDATE TOUCHED ZERO ROWS | %s | %s",
                self.client_id, local_order_id, reason,
            )
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

        if rowcount is None:
            if _ROWCOUNT_NONE_IS_FATAL:
                # Cannot confirm the row was updated. Refusing to advance state
                # prevents the OSM from asserting truth it has not confirmed.
                reason = f"transition_rowcount_unconfirmed:{old_status}->{new_status}"
                log.critical(
                    "[%s] OSM ROWCOUNT UNCONFIRMED — refusing state advance | "
                    "order=%s %s->%s | set OSM_ROWCOUNT_NONE_IS_FATAL=0 only if "
                    "your DB driver is proven to never silently fail UPDATE statements",
                    self.client_id, local_order_id, old_status, new_status,
                )
                self._record_error(local_order_id, reason)
                self._emit_transition_event(
                    local_order_id=local_order_id,
                    old_status=old_status,
                    new_status=new_status,
                    order=current,
                    decision="ERROR",
                    reason_code="DB_ROWCOUNT_UNCONFIRMED",
                    explanation=reason,
                )
                return False
            else:
                # Opt-out mode: driver is known not to expose rowcount but is
                # verified correct. Log at WARNING so it stays visible.
                log.warning(
                    "[%s] OSM rowcount unavailable (OSM_ROWCOUNT_NONE_IS_FATAL=0) | "
                    "order=%s %s->%s — proceeding without row confirmation",
                    self.client_id, local_order_id, old_status, new_status,
                )

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
            decision="CONFIRMED"
            if new_status not in {OrderStatus.REJECTED, OrderStatus.CANCELED, OrderStatus.EXPIRED, OrderStatus.ERROR}
            else "TERMINAL",
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
        """
        Coordinate broker-confirmed EXIT events with the exit engine.

        EXIT_FILLED means this EXIT order fully filled, not necessarily that
        the whole position is flat. Scale-outs must update remaining quantity
        without closing runners.

        Merged protections (all previously external runtime patches):
        - FIX-A: EXIT_SUBMITTED with missing broker_order_id → quarantine.
        - FIX-C: _call_exit_engine tolerance for unknown kwargs.
        - FIX-D: REJECTED cooldown handles dict-keyed _positions.
        - FIX-F: performance outcome recorded on full-close path.
        - FIX-H: zero-order-qty edge case guarded with a critical log.
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
        _kind   = str(current.get("kind") or "ENTRY").upper()
        if not _pos_id or _kind != "EXIT":
            return

        try:
            _ee = _get_exit_engine_for_client(self.client_id)
            if not _ee:
                log.warning(
                    "[%s] EXIT hook skipped — no exit engine registered | order=%s pos=%s status=%s",
                    self.client_id,
                    local_order_id or current.get("local_order_id"),
                    _pos_id,
                    new_status,
                )
                return

            _local_id    = local_order_id or current.get("local_order_id")
            _broker_id   = broker_order_id or current.get("broker_order_id")
            _order_qty   = self._safe_int(current.get("qty"), 0)
            _prev_filled = self._safe_int(current.get("filled_qty"), 0)
            # FIX-E: None default so we can distinguish "broker sent no qty" from "zero fill"
            _cum_filled  = self._safe_int(filled_qty, None)

            if _cum_filled is not None and _cum_filled < _prev_filled:
                log.critical(
                    "[%s] INVALID EXIT CUMULATIVE FILL | order=%s pos=%s new=%s prev=%s",
                    self.client_id, _local_id, _pos_id, _cum_filled, _prev_filled,
                )
                return

            # ── EXIT_SUBMITTED ─────────────────────────────────────────────
            if new_status == OrderStatus.EXIT_SUBMITTED:
                if _broker_id:
                    # Normal path: broker returned an order ID; register with engine.
                    self._call_exit_engine(
                        _ee,
                        "set_pending_exit_order",
                        _pos_id,
                        local_order_id=str(_local_id or ""),
                        broker_order_id=str(_broker_id),
                        qty=_order_qty,
                        reason=str(current.get("last_error") or ""),
                    )
                else:
                    # FIX-A: Broker accepted but returned no order identity.
                    # Quarantine the exit engine so no duplicate exit can be
                    # submitted while reconciler performs identity recovery.
                    # This is the primary gap the original OSM had — it only
                    # logged critical here and returned without registering the
                    # pending exit, leaving the engine unaware and unlocked.
                    log.critical(
                        "[%s] EXIT_SUBMITTED missing broker_order_id — quarantining exit | "
                        "order=%s pos=%s; reconciler will recover identity",
                        self.client_id, _local_id, _pos_id,
                    )
                    self._call_exit_engine(
                        _ee,
                        "set_pending_exit_order",
                        _pos_id,
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
                        local_order_id=str(_local_id or ""),
                        broker_order_id=str(_broker_id or ""),
                        cumulative_filled=_cum_filled,
                    )
                return

            # ── EXIT_FILLED ─────────────────────────────────────────────────
            if new_status == OrderStatus.EXIT_FILLED:
                if _cum_filled is None or _cum_filled <= 0:
                    _cum_filled = _order_qty

                # FIX-H: if order_qty is also zero, qty data is completely unreliable.
                # The previous version of this fix still called clear_exit_in_flight(),
                # which releases engine protection — operationally identical to the
                # original silent bug when broker data is bad. Quarantine instead:
                # lock the engine on this position and require reconciler resolution
                # rather than unblocking re-entry on ambiguous fill data.
                if _cum_filled <= 0:
                    log.critical(
                        "[%s] EXIT_FILLED with zero/unknown quantity — QUARANTINING | "
                        "order=%s pos=%s | broker fill data unreliable; "
                        "reconciler must resolve before re-exit is permitted",
                        self.client_id, _local_id, _pos_id,
                    )
                    self._call_exit_engine(
                        _ee,
                        "set_pending_exit_order",
                        _pos_id,
                        local_order_id=str(_local_id or ""),
                        broker_order_id=str(_broker_id or ""),
                        qty=0,
                        reason="EXIT_FILLED_ZERO_QTY_QUARANTINE",
                        identity_quarantine=True,
                    )
                    return

                _delta = max(0, _cum_filled - _prev_filled)

                # Prefer DB-backed remaining quantity for scale-out classification
                # because in-memory exit-engine state can lag under concurrent
                # callback/reconciler timing. Only fall back to engine memory if
                # DB cannot answer.
                _remaining_before = self._get_position_remaining_from_db(_pos_id)
                if _remaining_before is None:
                    _pos_obj = self._get_exit_engine_position(_ee, str(_pos_id))
                    _remaining_before = (
                        self._safe_int(getattr(_pos_obj, "quantity_remaining", None), None)
                        if _pos_obj else None
                    )

                if _delta <= 0:
                    # At this point the FIX-H guard above has already confirmed
                    # _cum_filled > 0. The regression guard at the top of this
                    # method confirmed _cum_filled >= _prev_filled. Therefore
                    # _delta == 0 means _cum_filled == _prev_filled, which means
                    # _prev_filled >= _cum_filled > 0, i.e. _prev_filled > 0.
                    #
                    # This is a DUPLICATE CALLBACK for fills already processed in a
                    # prior EXIT_PARTIAL_FILL or EXIT_FILLED event. The exit engine
                    # hook for those prior events already called note_partial_exit_fill
                    # or mark_position_closed. Clearing in-flight here is safe and
                    # correct: it unblocks a lock that is no longer protecting anything
                    # new, and the prior events have already advanced truth.
                    #
                    # This is NOT the ambiguous-zero-fill case (handled by FIX-H above)
                    # and NOT a stale callback from a different order — the regression
                    # guard proves cum >= prev, so the fill data is internally consistent.
                    log.info(
                        "[%s] EXIT_FILLED duplicate callback — no new qty | "
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
                        "[%s] EXIT_FILLED remaining size unknown — conservative partial handling | "
                        "order=%s pos=%s delta=%s",
                        self.client_id, _local_id, _pos_id, _delta,
                    )
                    self._call_exit_engine(
                        _ee,
                        "note_partial_exit_fill",
                        _pos_id,
                        _delta,
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
                        _ee,
                        "note_partial_exit_fill",
                        _pos_id,
                        _delta,
                        fill_price=fill_price,
                        local_order_id=str(_local_id or ""),
                        broker_order_id=str(_broker_id or ""),
                        cumulative_filled=_cum_filled,
                    )
                    return

                # Full close: delta >= remaining — position is flat.
                log.info(
                    "[%s] EXIT_FILLED treated as full close | order=%s pos=%s "
                    "delta=%s remaining_before=%s",
                    self.client_id, _local_id, _pos_id, _delta, _remaining_before,
                )

                # FIX-F: record trade performance outcome before removing the position
                # from the exit engine. The position object must be retrieved BEFORE
                # mark_position_closed() removes it from engine state. Previously this
                # was a runtime monkey-patch on APExitEngine.mark_position_closed that
                # could miss calls from reconciler and watcher paths. Now native here.
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
                        log.debug(
                            "[%s] performance outcome hook failed (non-critical): %s",
                            self.client_id, _perf_err,
                        )

                self._call_exit_engine(
                    _ee,
                    "mark_position_closed",
                    str(_pos_id),
                    reason="EXIT_FILLED",
                    qty_filled=_delta,
                    fill_price=fill_price,
                    local_order_id=str(_local_id or ""),
                    broker_order_id=str(_broker_id or ""),
                    cumulative_filled=_cum_filled,
                )
                return

            # ── CANCELED / EXPIRED / REJECTED ───────────────────────────────
            if new_status in (OrderStatus.CANCELED, OrderStatus.EXPIRED, OrderStatus.REJECTED):
                # FIX-I: previously called _ee.on_exit_failure() and
                # _ee.clear_exit_in_flight() directly, bypassing _call_exit_engine()'s
                # TypeError fallback. Version-skew and signature mismatches on these
                # calls were as exposed as the original FIX-C problem. Now routed
                # through the wrapper consistently.
                if hasattr(_ee, "on_exit_failure"):
                    self._call_exit_engine(
                        _ee,
                        "on_exit_failure",
                        _pos_id,
                        local_order_id=str(_local_id or ""),
                        broker_order_id=str(_broker_id or ""),
                        status=new_status,
                    )
                elif hasattr(_ee, "clear_exit_in_flight"):
                    self._call_exit_engine(
                        _ee,
                        "clear_exit_in_flight",
                        _pos_id,
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
                            # FIX-D: when _positions is a dict the original code iterated
                            # keys (strings), not values. getattr on a string key always
                            # returns the default, so the position_id match never fired and
                            # the rejection cooldown was silently never applied.
                            _iter = (
                                _positions_raw.values()
                                if isinstance(_positions_raw, dict)
                                else _positions_raw
                            )
                            for _p in _iter:
                                if str(getattr(_p, "position_id", getattr(_p, "id", "")) or "") == str(_pos_id):
                                    _p.last_exit_rejected = True
                                    _p.last_rejection_ts  = _time_module.time()
                                    log.info(
                                        "[%s] Exit REJECTED — 30s cooldown started | pos=%s ticker=%s",
                                        self.client_id, _pos_id, getattr(_p, "ticker", "?"),
                                    )
                                    break
                    except Exception as _cd_err:
                        log.debug(
                            "[%s] rejection cooldown update failed (non-critical): %s",
                            self.client_id, _cd_err,
                        )

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
        """Apply broker cumulative fill update without requiring a state change."""
        order = self._get_order(local_order_id)
        if not order:
            log.error("[%s] apply_fill_update: order %s not found",
                      self.client_id, local_order_id)
            return False
        order  = dict(order)
        status = str(order.get("status") or "")
        kind   = str(order.get("kind") or "").upper()
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
        """Return True for broker statuses that mean the order was accepted."""
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
        """Submit an already-created ENTRY order after watcher breach.

        Money-safe invariant: broker accepts and returns a real broker_order_id
        first; only then does OSM transition CREATED/PENDING_TRIGGER -> SUBMITTED.
        """
        current = self._get_order(local_order_id)
        if not current:
            error_msg = "existing_entry_order_not_found"
            log.critical("[%s] submit_existing_entry failed — %s | %s",
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
            log.warning("[%s] submit_existing_entry blocked — terminal status %s | %s",
                        self.client_id, status, local_order_id)
            return {"ok": False, "local_order_id": local_order_id,
                    "broker_order_id": current.get("broker_order_id"),
                    "status": status, "error": error_msg}

        if status not in (OrderStatus.CREATED, OrderStatus.PENDING_TRIGGER):
            error_msg = f"submit_existing_entry_invalid_status:{status}"
            log.critical("[%s] submit_existing_entry blocked — %s | %s",
                         self.client_id, error_msg, local_order_id)
            return {"ok": False, "local_order_id": local_order_id,
                    "broker_order_id": current.get("broker_order_id"),
                    "status": status, "error": error_msg}

        lp       = float(limit_price or current.get("limit_price") or getattr(plan, "limit_price", 0) or 0)
        contract = (current.get("contract") or getattr(plan, "contract_symbol", None)
                    or current.get("symbol") or getattr(plan, "ticker", ""))
        ticker   = current.get("symbol") or getattr(plan, "ticker", "")
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

        # Stale-read protection: watcher can expire/invalidate between the first
        # read and the broker POST. Never submit after a terminal watcher outcome.
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
                # SPLIT-BRAIN: broker has a live order but DB state did not advance.
                # Flag the order immediately so the reconciler can recover identity on
                # next pass and prevent the watcher from re-submitting this order.
                error_msg = "submitted_transition_failed_after_broker_accept"
                self._flag_split_brain_order(
                    local_order_id,
                    broker_order_id=broker_order_id,
                    error_msg=error_msg,
                )
                return {
                    "ok":           False,
                    "local_order_id": local_order_id,
                    "broker_order_id": broker_order_id,
                    "status":       OrderStatus.ERROR,
                    "error":        error_msg,
                    "split_brain":  True,
                }
            else:
                error_msg = (f"broker_status:{broker_status or 'unknown'} "
                             f"broker_order_id_missing:{not bool(broker_order_id)}")
        except Exception as e:
            error_msg = f"broker_error:{e}"

        self.transition(local_order_id, OrderStatus.ERROR, last_error=error_msg or "unknown_error")
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
            order          = (resp.json() or {}).get("order") or {}
            status         = str(order.get("status") or "").lower().strip()
            broker_order_id = order.get("id") or order.get("order_id")
            if self._is_broker_accept_status(status) and broker_order_id:
                ok = self.transition(local_id, OrderStatus.SUBMITTED,
                                     broker_order_id=broker_order_id, submitted_ts=now_utc_iso())
                if ok:
                    return {"ok": True, "local_order_id": local_id, "broker_order_id": broker_order_id,
                            "status": OrderStatus.SUBMITTED, "error": None}
                error_msg = "submitted_transition_failed_after_broker_accept"
            else:
                error_msg = (f"broker_status:{status or 'unknown'} "
                             f"broker_order_id_missing:{not bool(broker_order_id)}")
        except Exception as e:
            error_msg = f"broker_error:{e}"

        self.transition(local_id, OrderStatus.ERROR, last_error=error_msg or "unknown_error")
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
    ) -> dict:
        """Submit an EXIT order through OSM only.

        Hard safety rules:
        - If any active EXIT order already exists for the position, do NOT post
          another broker order from this path. The prior order must be reconciled
          to EXIT_FILLED/CANCELED/EXPIRED/REJECTED/ERROR first.
        - If broker appears to accept the order but returns no broker_order_id,
          park the local order in active EXIT_SUBMITTED quarantine instead of
          ERROR. That blocks duplicate exits until broker reconciliation/manual
          identity recovery resolves truth.  _handle_exit_engine_hooks will
          quarantine the exit engine for the same reason via FIX-A.
        """
        existing = self._get_active_exit_order(position_id)
        if existing:
            existing  = dict(existing)
            error_msg = (f"active_exit_already_exists:"
                         f"{existing.get('local_order_id')}:{existing.get('status')}")
            log.critical(
                "[%s] submit_exit BLOCKED — active exit already exists | "
                "pos=%s existing=%s status=%s broker=%s",
                self.client_id, position_id,
                existing.get("local_order_id"), existing.get("status"),
                existing.get("broker_order_id"),
            )
            return {
                "ok":               False,
                "local_order_id":   existing.get("local_order_id"),
                "broker_order_id":  existing.get("broker_order_id"),
                "status":           existing.get("status"),
                "error":            error_msg,
            }

        local_id = self.create_exit_order(
            position_id=position_id, contract=contract, symbol=symbol,
            direction=direction, qty=qty, plan_id=plan_id, signal_id=signal_id,
            limit_price=limit_price,
        )
        lp = float(limit_price or 0)
        if lp <= 0:
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
            resp = broker.session.post(
                f"{base_url}/v1/accounts/{account_id}/orders",
                data={
                    "class": "option", "symbol": underlying, "option_symbol": contract,
                    "side": "sell_to_close", "quantity": int(qty),
                    "type": "limit", "price": round(lp, 2), "duration": "day",
                },
                headers={"Accept": "application/json"},
                timeout=10,
            )
            order          = (resp.json() or {}).get("order") or {}
            status         = str(order.get("status") or "").lower().strip()
            broker_order_id = order.get("id") or order.get("order_id")

            if self._is_broker_accept_status(status) and broker_order_id:
                ok = self.transition(local_id, OrderStatus.EXIT_SUBMITTED,
                                     broker_order_id=broker_order_id, submitted_ts=now_utc_iso())
                if ok:
                    return {"ok": True, "local_order_id": local_id,
                            "broker_order_id": broker_order_id,
                            "status": OrderStatus.EXIT_SUBMITTED, "error": None}
                # SPLIT-BRAIN: broker has a live exit order but DB state did not advance.
                # Flag immediately so reconciler can recover and prevent duplicate exits.
                error_msg = "exit_submitted_transition_failed_after_broker_accept"
                self._flag_split_brain_order(
                    local_id,
                    broker_order_id=broker_order_id,
                    error_msg=error_msg,
                )
                return {
                    "ok":           False,
                    "local_order_id": local_id,
                    "broker_order_id": broker_order_id,
                    "status":       OrderStatus.ERROR,
                    "error":        error_msg,
                    "split_brain":  True,
                }

            elif self._is_broker_accept_status(status) and not broker_order_id:
                # Broker may have accepted a live order but failed to return identity.
                # Keep the local order ACTIVE so no second exit can be submitted.
                # FIX-A in _handle_exit_engine_hooks will quarantine the exit engine.
                error_msg = f"broker_accepted_missing_order_id_quarantine:status={status or 'unknown'}"
                ok = self.transition(
                    local_id,
                    OrderStatus.EXIT_SUBMITTED,
                    submitted_ts=now_utc_iso(),
                    last_error=error_msg,
                )
                return {
                    "ok":                False,
                    "local_order_id":    local_id,
                    "broker_order_id":   None,
                    "status":            OrderStatus.EXIT_SUBMITTED if ok else OrderStatus.ERROR,
                    "error":             error_msg,
                    "identity_quarantine": True,
                }
            else:
                error_msg = (f"broker_status:{status or 'unknown'} "
                             f"broker_order_id_missing:{not bool(broker_order_id)}")
        except Exception as e:
            error_msg = f"broker_error:{e}"

        self.transition(local_id, OrderStatus.ERROR, last_error=error_msg or "unknown_error")
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
        Write a split-brain marker when broker accepted but OSM DB transition failed.

        Stores broker_order_id in the DB row (via COALESCE so it is never
        overwritten with empty) and prefixes last_error with "SPLIT_BRAIN:" so
        the reconciler and get_split_brain_orders() can find these orders on next
        pass. Does NOT attempt another status transition — that already failed.

        If this write also fails, logs a CRITICAL with manual-intervention language
        because the system is now in a state where broker has a live order but no
        DB record at all. The runner must freeze that client until reconciled.
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
                        (
                            broker_order_id,
                            f"SPLIT_BRAIN:{error_msg}",
                            local_order_id,
                            self.client_id,
                        ),
                    )

            run_with_retry(_mark)
            log.critical(
                "[%s] SPLIT_BRAIN FLAGGED | order=%s broker=%s | "
                "broker accepted but DB transition failed; reconciler will recover on next pass",
                self.client_id, local_order_id, broker_order_id,
            )
        except Exception as flag_err:
            # Double DB failure: transition failed AND the flag write failed.
            # The broker has a live order with no DB record of it at all.
            log.critical(
                "[%s] SPLIT_BRAIN FLAG WRITE FAILED | order=%s broker=%s | "
                "MANUAL INTERVENTION REQUIRED — broker has live order with no DB record | "
                "flag_error=%s",
                self.client_id, local_order_id, broker_order_id, flag_err,
            )

    def get_split_brain_orders(self) -> list:
        """
        Return all orders flagged as split-brain for this client.

        Split-brain orders are those where the broker accepted the submission but
        the OSM DB transition failed, leaving the broker with a live order that
        the DB does not reflect in a SUBMITTED/EXIT_SUBMITTED state.

        The reconciler's order-reconciliation pass will recover these on the next
        cycle by matching broker_order_id → advancing OSM to SUBMITTED/ACKNOWLEDGED.
        Call this at runner startup to detect any orders left in split-brain state
        from a prior session and log/alert before resuming normal operation.
        """
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
# client_runner.py calls install_exit_quarantine_patch(APOrderStateMachine) at
# startup. All protections previously installed as runtime monkey-patches are
# now native inside APOrderStateMachine. These shims keep the call sites working
# without modification.

def install_exit_quarantine_patch(osm_cls):
    """
    No-op shim — all protections are now native to APOrderStateMachine.

    Previously installed:
      1. OSM EXIT_SUBMITTED with missing broker_order_id → exit engine quarantine.  (FIX-A native)
      2. EXIT_FILLED scale-out/full-close fill-aware classification.                (native)
      3. APBrokerReconciler.run_once() idempotency/throttle.                        (FIX-G, native in ap_reconciler.py)
      4. APExitEngine.mark_position_closed() → ap.performance_tracker recording.   (FIX-F native)

    Retained for backward compatibility so client_runner.py call sites need
    no change.
    """
    return osm_cls


def _install_reconciler_idempotency_patch() -> bool:
    """
    No-op shim — ap_reconciler.py now has native RLock + monotonic-clock
    idempotency built directly into APBrokerReconciler.run_once().

    FIX-G: the previous external patch used threading.Lock (non-reentrant) and
    wall-clock time.time() on different attribute keys (_last_run_ts vs the
    native _last_run_once_ts), meaning the two guards were completely independent
    and the external one never actually fired. Now removed.
    """
    return True


def _install_exit_performance_patch() -> bool:
    """
    No-op shim — performance tracking is now called natively inside
    APOrderStateMachine._handle_exit_engine_hooks on the full-close path
    (FIX-F) before mark_position_closed() is dispatched to the exit engine.

    Monkey-patching APExitEngine.mark_position_closed at runtime risked missing
    calls from reconciler and watcher paths that called it directly. The native
    OSM hook covers all paths through the single authoritative hook method.
    """
    return True


__all__ = ["install_exit_quarantine_patch", "APOrderStateMachine", "OrderStatus",
           "register_exit_engine", "unregister_exit_engine", "probe_db_rowcount",
           "PENDING_ENTRY_STATUSES", "PENDING_EXIT_STATUSES"]
