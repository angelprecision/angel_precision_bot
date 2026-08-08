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

import hashlib
import json
import logging
import os
import re
import threading
import time as _time_module
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from ap.broker_submit_identity import canonical_broker_submit_key
from ap.db import conn, run_with_retry
from ap.exit_safety import (
    alert_exit_submission_halted,
    evaluate_exit_submission_safety,
    resolve_exit_broker_truth,
)
try:
    from psycopg2 import errors as pg_errors
except ImportError:
    pg_errors = None
from ap.utils import now_utc_iso


def _normalize_broker_submitted_ts(value) -> str | None:
    """Normalize an explicitly broker-sourced acceptance timestamp.

    Recovery time is not broker submission time.  Callers may supply this
    value only when the callback or another exact broker response carries it;
    otherwise adoption deliberately leaves ``submitted_ts`` NULL so monitor
    age calculations fall back to the original ``created_ts`` chronology.
    """
    if value is None or str(value).strip() == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except (TypeError, ValueError) as exc:
            raise ValueError("broker_submitted_ts must be ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("broker_submitted_ts must be timezone-aware")
    return parsed.astimezone(timezone.utc).isoformat()

# P0 client-parity (2026-06-04): canonical_signal_id groups the same
# market opportunity across every active eligible client account so the
# parity ledger and audit queries can detect fanout failures.
try:
    from ap_canonical_signal import build_canonical_signal_id
except Exception:  # pragma: no cover — fallback keeps OSM functional
    def build_canonical_signal_id(signal_id, signal=None):  # type: ignore
        return signal_id if isinstance(signal_id, str) else ""

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


# ─────────────────────────────────────────────────────────────────────────────
# PR #234 — OSM entry-direction fail-closed guard.
#
# The OSM is the final authority for order creation.  An ENTRY row must never
# be inserted with a missing, empty, or non-canonical direction — that would
# mean the broker submission path downstream has to guess, which is
# unacceptable for live/paper order lifecycle.  This helper is the last line
# of defense before create_entry_order() persists a row.
#
# Non-goals (PR #234 spec): transition graph, split-brain handling, broker
# submit behavior, PENDING_TRIGGER handoff helpers, execution_mode handling,
# order metadata shape (except canonical direction/side).  None are touched.
# ─────────────────────────────────────────────────────────────────────────────
_ENTRY_DIRECTION_ALIASES: dict[str, str] = {
    "CALL":    "CALL",
    "BUY":     "CALL",
    "LONG":    "CALL",
    "CALLS":   "CALL",
    "BULLISH": "CALL",
    "PUT":     "PUT",
    "SELL":    "PUT",
    "SHORT":   "PUT",
    "PUTS":    "PUT",
    "BEARISH": "PUT",
}


def _normalize_entry_direction(plan) -> str:
    """Canonical CALL/PUT normalizer for the OSM entry-order insert path.

    Reads plan.side first, falling back to plan.direction when side is
    missing/empty.  Raises
    ValueError('invalid_or_missing_entry_direction:<raw>') when the resolved
    value is not one of the recognized aliases.  Never returns a default —
    callers must not catch and substitute.
    """
    raw = getattr(plan, "side", None)
    if raw is None or str(raw or "").strip() == "":
        raw = getattr(plan, "direction", None)

    raw_norm = str(raw or "").strip().upper()
    direction = _ENTRY_DIRECTION_ALIASES.get(raw_norm)

    if direction not in {"CALL", "PUT"}:
        raise ValueError(f"invalid_or_missing_entry_direction:{raw_norm!r}")

    return direction


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
        self.client_id        = str(client_id or "").strip().lower()   # normalise: matches _normalize_client_key()
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

    def create_entry_order(
        self,
        plan,
        *,
        limit_price=None,
        reserved_cost=None,
        initial_status: Optional[str] = None,
        meta: Optional[dict] = None,
        execution_mode: Optional[str] = None,
    ) -> str:
        """
        Create an ENTRY order row in the OSM.

        PR fix/osm-watcher-handoff-and-entry-meta:
          - `initial_status='PENDING_TRIGGER'` lets callers (overnight_reeval +
            queue intraday breach path) atomically insert the row already in
            PENDING_TRIGGER, eliminating the CREATED→PENDING_TRIGGER race that
            caused 1,103 LOST_HANDOFF_30S cancellations on 2026-05-26. Default
            stays 'CREATED' for backward compat with callers that intentionally
            need the CREATED state (e.g. submit_existing_entry_with_create).
          - `meta` lets callers persist score / tier / signal_entry_price /
            selector quotes at INSERT time. Auto-derived defaults from the plan
            are merged with the caller's explicit `meta` (caller wins on key
            conflict). The result is stored as a JSON-encoded blob in
            orders.meta AND as top-level columns (score, tier, trigger_price,
            stop_underlying, target_underlying) so APOrderMonitor's existing
            order.get('score') fallback (ap/order_monitor.py:738) resolves to
            a real score on every OSM-created entry. This fixes the
            "score=0.0 tier=normal" mis-classification that pushed A+ setups
            into the 90s normal cancel window.

        Retry metadata semantics: retry_engine continues to merge keys like
        retry_status / retry_abort_ts into orders.meta AFTER cancel. Our INSERT
        only writes the initial meta; we never overwrite later merges.
        """
        existing = self._get_order_by_plan(plan.plan_id, kind="ENTRY") if plan.plan_id else None
        if existing:
            log.warning(
                "[%s] create_entry_order SKIPPED -- plan %s already has order %s",
                self.client_id, plan.plan_id, existing["local_order_id"],
            )
            return existing["local_order_id"]

        # ── Req 2: Validate contracts > 0 before inserting ───────────────
        # An order with qty=0 reaches order_monitor ACKNOWLEDGED and loops
        # forever on PAPER_ENTRY_REPEG_UNSUPPORTED / HYDRATION_FAILED.
        # Block the insert here so the bad row never enters the DB.
        _contracts = int(getattr(plan, "contracts", 0) or 0)
        if _contracts <= 0:
            log.error(
                "[%s] create_entry_order BLOCKED — invalid_entry_qty: "
                "plan %s has contracts=%s <= 0. "
                "Sizer must return contracts > 0 before an entry order is created.",
                self.client_id, getattr(plan, "plan_id", "?"), _contracts,
            )
            raise ValueError(
                f"invalid_entry_qty: plan.contracts={_contracts} for client={self.client_id}"
            )

        # ── PR #234: fail-closed entry-direction guard ────────────────────
        # OSM is the final order-creation authority.  If plan.side / plan.direction
        # cannot be resolved to canonical CALL/PUT here, we MUST NOT insert the
        # order — the broker submit path downstream cannot recover a missing
        # direction.  The helper raises ValueError; we log.critical for
        # operator visibility before letting it propagate.
        try:
            _direction = _normalize_entry_direction(plan)
        except ValueError as _dir_err:
            log.critical(
                "[%s] create_entry_order BLOCKED %s plan=%s",
                self.client_id,
                _dir_err,
                getattr(plan, "plan_id", "?"),
            )
            raise

        # Defensive meta canonicalization: even if the caller passed a meta
        # dict with a stale or missing direction/side, force the canonical
        # value on a defensive copy so no downstream reader sees a mismatch
        # between orders.direction and meta.direction.
        if meta is None:
            meta = {}
        else:
            meta = dict(meta)
        meta["direction"] = _direction
        meta["side"] = _direction

        # P0 (PR #264): enforcement provenance on every order row. The
        # runner computes a runtime_config_hash at LIVE startup (risk
        # limits, PR180 state, confirmation-required state, commit sha,
        # pod id) and pins it on this OSM instance. Every order created by
        # ANY path (queue dispatch, overnight reeval, armed-deferred
        # rescue) carries the hash, so a fill can always be traced to the
        # exact enforcement configuration that produced it. Caller-supplied
        # meta wins if it already set the key; absent hash writes nothing
        # (paper runners may not set one).
        _rt_hash = getattr(self, "runtime_config_hash", None)
        if _rt_hash and "enforcement_config_hash" not in meta:
            meta["enforcement_config_hash"] = str(_rt_hash)

        # Push the canonical direction back onto the plan object so any
        # subsequent read of plan.side / plan.direction (retry engine,
        # watcher, exit engine, logging) sees the normalized value.  Wrapped
        # in try/except because plan may be a frozen dataclass / read-only
        # proxy in some caller paths.
        try:
            plan.side = _direction
            plan.direction = _direction
        except Exception:
            pass

        local_order_id = str(uuid.uuid4())
        contract = getattr(plan, "contract_symbol", None) or plan.ticker
        lp = float(limit_price) if limit_price else (float(plan.limit_price) if getattr(plan, "limit_price", None) else None)
        rc = float(reserved_cost) if reserved_cost else (float(plan.max_position_usd) if getattr(plan, "max_position_usd", None) else None)
        ts = now_utc_iso()

        # ── Initial status (FIX 2): atomic PENDING_TRIGGER for watcher-held entries ──
        # Default 'CREATED' preserves back-compat. Only 'CREATED' and
        # 'PENDING_TRIGGER' are valid initial states for ENTRY orders.
        _initial_status = (initial_status or OrderStatus.CREATED).upper()
        if _initial_status not in (OrderStatus.CREATED, OrderStatus.PENDING_TRIGGER):
            log.warning(
                "[%s] create_entry_order: invalid initial_status=%r — falling back to CREATED",
                self.client_id, initial_status,
            )
            _initial_status = OrderStatus.CREATED

        # ── Initial meta (FIX 1): auto-derive score / tier / signal alignment ──
        # APOrderMonitor reads `meta.score` (preferred) then `order.score` then 0.
        # Without this, every OSM-created order was scored as 0 → forced into
        # normal-tier 90s cancel window. We populate both meta AND the top-level
        # columns so both read paths resolve.
        try:
            _score_val   = float(getattr(plan, "score", 0) or 0)
        except Exception:
            _score_val = 0.0
        try:
            _tier_val    = str(getattr(plan, "tier", "") or "")
        except Exception:
            _tier_val = ""
        try:
            _trigger_val = float(getattr(plan, "trigger_price", 0) or 0)
        except Exception:
            _trigger_val = 0.0
        try:
            _stop_val    = float(getattr(plan, "stop_underlying", 0) or 0)
        except Exception:
            _stop_val = 0.0
        try:
            _target_val  = float(getattr(plan, "target_underlying", 0) or 0)
        except Exception:
            _target_val = 0.0

        _raw_signal_id = str(getattr(plan, "signal_id", "") or "")
        _canonical_signal_id = build_canonical_signal_id(_raw_signal_id) or None

        # execution_mode is stamped at ENTRY creation and is the source of truth
        # for proof_trades on close. Only 'live'/'paper' are valid; anything else
        # is dropped to NULL so the close-time copy resolves to 'unknown'.
        _exec_mode = str(execution_mode or "").lower().strip()
        if _exec_mode not in ("live", "paper"):
            _exec_mode = None

        _auto_meta = {
            "score":              _score_val,
            "tier":               _tier_val,
            "signal_id":          _raw_signal_id,
            "canonical_signal_id": _canonical_signal_id or "",
            "plan_id":            str(getattr(plan, "plan_id", "") or ""),
            "trigger_type":       str(getattr(plan, "trigger_type", "breach") or "breach"),
            "signal_entry_price": _trigger_val,
            "selected_contract":  str(contract or ""),
            "contracts":          _contracts,   # hydrated + validated above
            "limit_price":        float(lp) if lp is not None else None,
            "max_position_usd":   float(rc) if rc is not None else None,
            "pattern":            str(getattr(plan, "pattern", "") or ""),
            "timeframe":          str(getattr(plan, "timeframe", "") or ""),
            # Req 1+3: persist direction + symbol so _try_repeg hydration always
            # has meta fallbacks even if orders.direction / orders.symbol are blank.
            "direction":          _direction,
            "side":               _direction,   # alias — retry_engine reads both
            "symbol":             str(getattr(plan, "ticker", "") or ""),
            # execution_mode mirrored into meta as a JSON fallback alongside the
            # top-level orders.execution_mode column.
            "execution_mode":     _exec_mode,

        }
        # Caller-supplied meta wins on conflict (e.g. queue path passing
        # selector_ask / selector_mid / option_bid / option_ask / option_mid /
        # account_equity / risk_pct / bootstrap_mode / total_trades).
        # SAFETY (P0 hotfix/p0-preserve-overnight-sizing-meta):
        # If caller meta contains a key that auto_meta already computed and the
        # caller's value is None/empty-string, the caller value is STALE/MISSING
        # and must NOT overwrite the good auto-derived value. This prevents a
        # plan.metadata with e.g. selected_contract=None from clobbering the
        # correctly derived selected_contract from the plan.contract_symbol.
        # Only applies to keys already in auto_meta — new keys from caller
        # (sizing_context, contract_deferred, snapshot_at_eval, etc.) pass
        # through unconditionally.
        _final_meta = dict(_auto_meta)
        if meta and isinstance(meta, dict):
            for _mk, _mv in meta.items():
                if _mk in _auto_meta and (_mv is None or _mv == ""):
                    # Caller has null/empty for a key OSM already derived — skip
                    continue
                _final_meta[_mk] = _mv
        try:
            import json as _json_mod
            _meta_json = _json_mod.dumps(_final_meta, default=str)
        except Exception as _je:
            log.warning("[%s] create_entry_order: meta JSON encode failed (%s) — using {}",
                        self.client_id, _je)
            _meta_json = "{}"

        def _fn():
            with conn() as c:
                c.execute(
                    f"""
                    INSERT INTO orders (
                        local_order_id, client_id, plan_id, signal_id,
                        canonical_signal_id,
                        kind, status,
                        symbol, contract, direction,
                        qty, limit_price, reserved_cost,
                        filled_qty,
                        score, tier,
                        trigger_price, stop_underlying, target_underlying,
                        pattern, timeframe,
                        execution_mode,
                        meta,
                        created_ts, updated_ts
                    ) VALUES (
                        %s,%s,%s,%s,
                        %s,
                        'ENTRY','{_initial_status}',
                        %s,%s,%s,
                        %s,%s,%s,
                        0,
                        %s,%s,
                        %s,%s,%s,
                        %s,%s,
                        %s,
                        %s,
                        %s,%s
                    )
                    ON CONFLICT (local_order_id) DO NOTHING
                    """,
                    (
                        local_order_id, self.client_id,
                        plan.plan_id, plan.signal_id,
                        _canonical_signal_id,
                        plan.ticker, contract, _direction,
                        _contracts, lp, rc,
                        _score_val, _tier_val,
                        _trigger_val if _trigger_val > 0 else None,
                        _stop_val if _stop_val > 0 else None,
                        _target_val if _target_val > 0 else None,
                        _final_meta.get("pattern") or None,
                        _final_meta.get("timeframe") or None,
                        _exec_mode,
                        _meta_json,
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
            "[%s] ORDER CREATED (entry) | %s x%s | local_order_id=%s | status=%s score=%.1f tier=%s",
            self.client_id, contract, plan.contracts, local_order_id,
            _initial_status, _score_val, _tier_val or "-",
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
        execution_mode: str | None = None,
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
        _exec_mode = str(execution_mode or "").strip().lower()
        if _exec_mode not in ("live", "paper"):
            _exec_mode = None

        # P0 client-parity: stamp canonical_signal_id on exit rows too so the
        # full lifecycle (entry + exit) groups by the same opportunity id.
        _exit_canonical_id = build_canonical_signal_id(signal_id) or None

        def _fn():
            with conn() as c:
                c.execute(
                    """
                    INSERT INTO orders (
                        local_order_id, client_id, position_id, plan_id, signal_id,
                        canonical_signal_id,
                        kind, status,
                        symbol, contract, direction,
                        execution_mode,
                        qty, limit_price, reserved_cost,
                        filled_qty,
                        created_ts, updated_ts
                    ) VALUES (
                        %s,%s,%s,%s,%s,
                        %s,
                        'EXIT','EXIT_REQUESTED',
                        %s,%s,%s,
                        %s,
                        %s,%s,%s,
                        0,
                        %s,%s
                    )
                    ON CONFLICT (local_order_id) DO NOTHING
                    """,
                    (
                        local_id, self.client_id,
                        position_id, plan_id, signal_id,
                        _exit_canonical_id,
                        symbol.upper(), contract, direction.upper(),
                        _exec_mode,
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
        if self._pending_entry_has_submit_or_recovery_owner(current):
            log.warning(
                "[%s] expire_pending_entry refused -- broker/recovery ownership active | %s",
                self.client_id, local_order_id,
            )
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
        if self._pending_entry_has_submit_or_recovery_owner(current):
            log.warning(
                "[%s] cancel_pending_entry refused -- broker/recovery ownership active | %s",
                self.client_id, local_order_id,
            )
            return False
        return self.transition(local_order_id, OrderStatus.CANCELED, last_error=reason)

    @staticmethod
    def _pending_entry_has_submit_or_recovery_owner(order: dict) -> bool:
        """Protect generic pending cleanup from broker-ambiguous ownership."""
        if order.get("broker_order_id") or order.get("submitted_ts"):
            return True
        meta = order.get("meta") or {}
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}
        if not isinstance(meta, dict):
            meta = {}
        if str(meta.get("lifecycle_state") or "").upper() == "SUBMITTING":
            return True
        if meta.get("submit_intent_at") or meta.get("broker_submit_key"):
            return True
        if str(meta.get("current_owner") or "").startswith("broker_submit:"):
            return True
        recovery_owner = str(meta.get("recovery_submit_owner") or "").strip()
        if recovery_owner:
            # Generic cleanup has no caller owner token, so it cannot prove
            # that an expired-looking claim is safe to supersede. Recovered
            # rows must use terminalize_recovered_entry(), whose CAS validates
            # owner, generation and lease atomically.
            return True
        return False

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
        allow_submit_owner_terminalization: bool = False,
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
        same_state_identity_update = False

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
            elif broker_order_id or submitted_ts:
                # A same-state broker adoption is not a no-op.  A concurrent
                # worker may have advanced the lifecycle status without
                # persisting the broker identity.  Execute the fenced UPDATE so
                # True means the exact broker id is durable, not merely that the
                # status string already matches.
                same_state_identity_update = True
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

        if (
            not same_state_fill_update
            and not same_state_identity_update
            and not OrderStatus.can_transition(old_status, new_status)
        ):
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
        if kind.upper() == "ENTRY" and OrderStatus.is_terminal(new_status):
            updates.append(
                "meta=COALESCE(meta, '{}'::jsonb) "
                "|| '{\"selector_recovery_cursor_v1\":null}'::jsonb"
            )
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
        if broker_order_id:
            # Never replace a different durable broker identity.  Empty/equal
            # are the only idempotent acceptance states.
            sql += " AND (broker_order_id IS NULL OR broker_order_id='' OR broker_order_id=%s)"
            params.append(str(broker_order_id))
        if (
            kind.upper() == "ENTRY"
            and old_status in {OrderStatus.CREATED, OrderStatus.PENDING_TRIGGER}
            and OrderStatus.is_terminal(new_status)
            and not allow_submit_owner_terminalization
        ):
            # Atomic cleanup fence: a generic timeout/cancel must not race a
            # durable broker-submit intent written after its Python-side read.
            # Only the submit path may opt out after a broker response is
            # conclusively classified as not owning an order.
            sql += (
                " AND (broker_order_id IS NULL OR broker_order_id='')"
                " AND submitted_ts IS NULL"
                " AND COALESCE(meta->>'submit_intent_at','')=''"
                " AND UPPER(COALESCE(meta->>'lifecycle_state',''))"
                "     NOT IN ('SUBMITTING','SUBMITTED')"
                " AND COALESCE(meta->>'broker_submit_key','')=''"
                " AND COALESCE(meta->>'recovery_submit_owner','')=''"
                " AND COALESCE(meta->>'split_brain_quarantine','')=''"
                " AND COALESCE(last_error,'') NOT LIKE 'SPLIT_BRAIN:%%'"
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
                latest_row = dict(latest)
                latest_status = str(latest_row.get("status") or "")
                if latest_status == new_status:
                    if broker_order_id:
                        latest_broker_id = str(
                            latest_row.get("broker_order_id") or ""
                        ).strip()
                        if latest_broker_id != str(broker_order_id).strip():
                            reason = (
                                "transition_broker_identity_unproven:"
                                f"expected={broker_order_id}:actual={latest_broker_id or 'missing'}"
                            )
                            log.critical(
                                "[%s] OSM BROKER IDENTITY CAS MISS | order=%s "
                                "status=%s expected_broker=%s actual_broker=%s",
                                self.client_id, local_order_id, new_status,
                                broker_order_id, latest_broker_id or "missing",
                            )
                        else:
                            log.info(
                                "[%s] transition CAS no-op -- %s already advanced "
                                "to %s with exact broker identity %s",
                                self.client_id, local_order_id, new_status,
                                broker_order_id,
                            )
                            return True
                    else:
                        log.info(
                            "[%s] transition CAS no-op -- %s already advanced to %s by "
                            "a concurrent worker; treating as success",
                            self.client_id, local_order_id, new_status,
                        )
                        return True
                else:
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
        # ── PR81 Final Amendment v2 §3: opportunity-ledger lifecycle hook ──────
        # Fan the OSM transition out to the client_signal_opportunities row
        # for ENTRY orders only. Fail-safe: never raises, never blocks the
        # OSM caller. Exit transitions are intentionally ignored — the
        # opportunity ledger tracks the entry lifecycle, not exit lifecycle.
        try:
            self._notify_opportunity_ledger(
                current=current,
                new_status=new_status,
                broker_order_id=broker_order_id or current.get("broker_order_id"),
                position_id=position_id or current.get("position_id"),
                last_error=last_error,
                fill_price=fill_price,
                filled_qty=filled_qty,
                filled_ts=filled_ts,
            )
        except Exception as _ledger_exc:
            log.debug(
                "[%s] opportunity ledger notify failed (non-fatal): %s",
                self.client_id, _ledger_exc,
            )
        self._handle_exit_engine_hooks(
            current=current, new_status=new_status, position_id=position_id,
            filled_qty=filled_qty, fill_price=fill_price,
            broker_order_id=broker_order_id or current.get("broker_order_id"),
            local_order_id=local_order_id,
        )
        return True

    def adopt_broker_owned_exit_request(
        self,
        local_order_id: str,
        *,
        broker_order_id: str,
        execution_mode: str,
        position_id: str | None = None,
        client_id: str | None = None,
        expected_qty: int | None = None,
        broker_submitted_ts=None,
        source: str = "broker_owned_exit_request_recovery",
    ) -> dict:
        """Adopt one exact broker-owned EXIT_REQUESTED row into EXIT_SUBMITTED.

        A broker order id is ownership evidence, but it is not permission to
        weaken the generic transition graph or to fabricate a fill.  This
        method is the narrow recovery seam for the case where the broker
        accepted an EXIT and the durable submit handoff did not complete.

        The database CAS owns the safety decision.  It validates client, local
        order, EXIT kind, requested status, exact execution mode, nonblank
        position identity, positive quantity, optional exact position/quantity
        identity, and exact-or-empty broker identity in one UPDATE.  Recovery
        callers must use the returned disposition and must not submit or cancel
        on a miss.  ``broker_submitted_ts`` is optional exact broker acceptance
        evidence; when absent, adoption does not stamp recovery time into
        ``submitted_ts`` and the original ``created_ts`` remains the monitor's
        stale-age reference.
        """
        local_id = str(local_order_id or "").strip()
        broker_id = str(broker_order_id or "").strip()
        mode = str(execution_mode or "").strip()
        self_client_id = str(self.client_id or "").strip()
        expected_client = str(
            self_client_id if client_id is None else client_id
        ).strip()
        expected_position = str(position_id or "").strip()
        source_text = str(source or "").strip()
        expected_qty_value = None
        if expected_qty is not None:
            try:
                expected_qty_value = int(expected_qty)
            except (TypeError, ValueError):
                expected_qty_value = 0

        try:
            normalized_broker_submitted_ts = _normalize_broker_submitted_ts(
                broker_submitted_ts
            )
        except ValueError as exc:
            normalized_broker_submitted_ts = None
            invalid_submitted_ts_error = str(exc)
        else:
            invalid_submitted_ts_error = ""

        def _result(
            disposition: str,
            *,
            reason_code: str,
            status: str = "",
            error: str = "",
            order: dict | None = None,
            submitted_ts_source: str = "unproven_recovery",
        ) -> dict:
            adopted = disposition in {
                "ADOPTED",
                "ALREADY_BROKER_OWNED_ACTIVE",
            }
            return {
                "disposition": disposition,
                "adopted": adopted,
                "already_adopted": disposition in {
                    "ALREADY_BROKER_OWNED_ACTIVE",
                },
                "already_terminal": disposition == "ALREADY_TERMINAL",
                "local_order_id": local_id,
                "broker_order_id": broker_id,
                "status": status,
                "reason_code": reason_code,
                "error": error,
                "order": order,
                "broker_submitted_ts": normalized_broker_submitted_ts,
                "broker_submitted_ts_source": submitted_ts_source,
            }

        if (
            not local_id
            or not broker_id
            or broker_id.upper() == "N/A"
            or mode not in {"live", "paper"}
            or expected_client != self_client_id
            or not expected_position
            or not source_text
            or expected_qty_value is not None and expected_qty_value <= 0
        ):
            return _result(
                "IDENTITY_MISMATCH",
                reason_code="EXIT_BROKER_OWNERSHIP_ADOPTION_FAILED",
                error="invalid_recovery_identity",
            )
        if invalid_submitted_ts_error:
            return _result(
                "IDENTITY_MISMATCH",
                reason_code="EXIT_BROKER_OWNERSHIP_ADOPTION_FAILED",
                error=f"invalid_broker_submitted_ts:{invalid_submitted_ts_error}",
            )

        submitted_ts_source = (
            "broker_acceptance_evidence"
            if normalized_broker_submitted_ts
            else "unproven_recovery"
        )
        adoption_timestamp = now_utc_iso()
        diagnostic_payload = {
            "broker_ownership_adopted_from_exit_requested": True,
            "broker_ownership_adoption_source": source_text,
            "broker_ownership_adoption_broker_order_id": broker_id,
            "broker_ownership_adoption_reason": (
                "EXIT_BROKER_OWNERSHIP_ADOPTED_FROM_REQUESTED"
            ),
            # This is intentionally named as recovery/adoption time.  It must
            # never be mistaken for historical broker submission chronology.
            "broker_ownership_adoption_timestamp": adoption_timestamp,
            "broker_ownership_adopted_at": adoption_timestamp,
            "broker_ownership_submitted_ts_proven": bool(
                normalized_broker_submitted_ts
            ),
            "broker_ownership_submitted_ts_source": submitted_ts_source,
            "broker_ownership_stale_age_reference": (
                "submitted_ts_or_created_ts"
                if normalized_broker_submitted_ts
                else "created_ts"
            ),
        }
        if normalized_broker_submitted_ts:
            diagnostic_payload["broker_ownership_submitted_ts"] = (
                normalized_broker_submitted_ts
            )

        diagnostic = json.dumps(
            diagnostic_payload
        )
        sql = (
            "UPDATE orders SET "
            "status=%s, "
            "broker_order_id=%s, "
            "submitted_ts=COALESCE(submitted_ts, %s::timestamptz), "
            "meta=COALESCE(meta, '{}'::jsonb) || %s::jsonb, "
            "updated_ts=NOW() "
            "WHERE local_order_id=%s "
            "  AND client_id=%s "
            "  AND kind='EXIT' "
            "  AND status='EXIT_REQUESTED' "
            "  AND execution_mode=%s "
            "  AND position_id IS NOT NULL "
            "  AND BTRIM(position_id::text)<>'' "
            "  AND qty > 0 "
            "  AND (broker_order_id IS NULL OR BTRIM(broker_order_id)='' "
            "       OR broker_order_id=%s)"
        )
        params: list = [
            OrderStatus.EXIT_SUBMITTED,
            broker_id,
            normalized_broker_submitted_ts,
            diagnostic,
            local_id,
            self.client_id,
            mode,
            broker_id,
        ]
        if expected_position:
            sql += " AND position_id::text=%s"
            params.append(expected_position)
        if expected_qty_value is not None:
            sql += " AND qty=%s"
            params.append(expected_qty_value)

        try:
            def _adopt():
                with conn() as c:
                    cur = c.execute(sql, tuple(params))
                    return getattr(cur, "rowcount", getattr(c, "rowcount", None))

            rowcount = run_with_retry(_adopt)
        except Exception as exc:
            log.critical(
                "[%s] EXIT broker ownership adoption DB failure | order=%s broker=%s error=%s",
                self.client_id, local_id, broker_id, exc,
            )
            return _result(
                "DB_ERROR",
                reason_code="EXIT_BROKER_OWNERSHIP_ADOPTION_FAILED",
                error=f"{type(exc).__name__}:{exc}",
            )

        if rowcount is None:
            return _result(
                "DB_ERROR",
                reason_code="EXIT_BROKER_OWNERSHIP_ADOPTION_FAILED",
                error="rowcount_unconfirmed",
            )
        try:
            rowcount_value = int(rowcount)
        except (TypeError, ValueError, OverflowError):
            return _result(
                "DB_ERROR",
                reason_code="EXIT_BROKER_OWNERSHIP_ADOPTION_FAILED",
                error="rowcount_unconfirmed",
            )
        if rowcount_value not in {0, 1}:
            return _result(
                "DB_ERROR",
                reason_code="EXIT_BROKER_OWNERSHIP_ADOPTION_FAILED",
                error=f"rowcount_unconfirmed:{rowcount_value}",
            )

        try:
            latest = self._get_order(local_id)
        except Exception as exc:
            log.warning(
                "[%s] EXIT broker ownership adoption reload failed | order=%s broker=%s error=%s",
                self.client_id, local_id, broker_id, exc,
            )
            return _result(
                "DB_ERROR",
                reason_code="EXIT_BROKER_OWNERSHIP_ADOPTION_FAILED",
                error=f"{type(exc).__name__}:{exc}",
            )
        latest_dict = dict(latest) if latest else None
        if rowcount_value > 0:
            if latest_dict is None:
                return _result(
                    "DB_ERROR",
                    reason_code="EXIT_BROKER_OWNERSHIP_ADOPTION_FAILED",
                    error="adoption_reload_unconfirmed",
                )
            adopted_order = latest_dict
            self._emit_transition_event(
                local_order_id=local_id,
                old_status=OrderStatus.EXIT_REQUESTED,
                new_status=OrderStatus.EXIT_SUBMITTED,
                order=adopted_order,
                decision="CONFIRMED",
                reason_code="EXIT_BROKER_OWNERSHIP_ADOPTED_FROM_REQUESTED",
                explanation=(
                    "Exact broker ownership adopted after EXIT submit handoff "
                    "left the durable row in EXIT_REQUESTED."
                ),
                broker_order_id=broker_id,
                extra_inputs={
                    "execution_mode": mode,
                    "position_id": expected_position or adopted_order.get("position_id"),
                    "requested_qty": adopted_order.get("qty"),
                    "source": source_text,
                    "broker_submitted_ts": normalized_broker_submitted_ts,
                    "broker_submitted_ts_source": submitted_ts_source,
                    "stale_age_reference": diagnostic_payload[
                        "broker_ownership_stale_age_reference"
                    ],
                },
            )
            self._handle_exit_engine_hooks(
                current=adopted_order,
                new_status=OrderStatus.EXIT_SUBMITTED,
                position_id=expected_position or adopted_order.get("position_id"),
                broker_order_id=broker_id,
                local_order_id=local_id,
            )
            return _result(
                "ADOPTED",
                reason_code="EXIT_BROKER_OWNERSHIP_ADOPTED_FROM_REQUESTED",
                status=OrderStatus.EXIT_SUBMITTED,
                order=adopted_order,
                submitted_ts_source=submitted_ts_source,
            )

        if latest_dict:
            # The reread is an authority check, not a presentation layer.
            # Preserve durable values exactly; stripping or case-folding here
            # could launder malformed persisted identity after the strict CAS
            # correctly rejected it.
            latest_status = latest_dict.get("status")
            latest_broker_id = latest_dict.get("broker_order_id")
            latest_client = latest_dict.get("client_id")
            latest_mode = latest_dict.get("execution_mode")
            latest_position = latest_dict.get("position_id")
            latest_kind = latest_dict.get("kind")
            try:
                latest_qty = int(latest_dict.get("qty") or 0)
            except (TypeError, ValueError):
                latest_qty = 0
            latest_identity_matches = bool(
                latest_client == self_client_id
                and latest_kind == "EXIT"
                and latest_mode == mode
                and latest_position
                and latest_position == expected_position
                and latest_qty > 0
                and (
                    expected_qty_value is None
                    or latest_qty == expected_qty_value
                )
            )
            if (
                latest_status in {
                    OrderStatus.EXIT_SUBMITTED,
                    OrderStatus.EXIT_ACKNOWLEDGED,
                    OrderStatus.EXIT_PARTIAL_FILL,
                }
                and latest_broker_id == broker_id
                and latest_identity_matches
            ):
                # The first successful CAS already emitted the transition and
                # hydrated the exit owner.  A concurrent/replayed adoption is
                # an idempotent read-only result; repeating the ownership hook
                # would create duplicate side effects without another durable
                # mutation.
                return _result(
                    "ALREADY_BROKER_OWNED_ACTIVE",
                    reason_code="EXIT_BROKER_OWNERSHIP_ALREADY_ADOPTED",
                    status=latest_status,
                    order=latest_dict,
                    submitted_ts_source=(
                        "existing_durable_value"
                        if latest_dict.get("submitted_ts")
                        else submitted_ts_source
                    ),
                )
            if latest_status in OrderStatus.TERMINAL:
                if latest_broker_id == broker_id and latest_identity_matches:
                    return _result(
                        "ALREADY_TERMINAL",
                        reason_code="EXIT_BROKER_OWNERSHIP_ALREADY_TERMINAL",
                        status=latest_status,
                        error="order_already_terminal",
                        order=latest_dict,
                        submitted_ts_source=(
                            "existing_durable_value"
                            if latest_dict.get("submitted_ts")
                            else submitted_ts_source
                        ),
                    )
                if latest_broker_id and latest_broker_id != broker_id:
                    return _result(
                        "IDENTITY_MISMATCH",
                        reason_code="EXIT_BROKER_OWNERSHIP_ADOPTION_FAILED",
                        status=latest_status,
                        error="terminal_broker_order_id_mismatch",
                        order=latest_dict,
                    )
                return _result(
                    "IDENTITY_MISMATCH",
                    reason_code="EXIT_BROKER_OWNERSHIP_ADOPTION_FAILED",
                    status=latest_status,
                    error="identity_or_status_mismatch",
                    order=latest_dict,
                )

        return _result(
            "IDENTITY_MISMATCH",
            reason_code="EXIT_BROKER_OWNERSHIP_ADOPTION_FAILED",
            status=str((latest_dict or {}).get("status") or ""),
            error="identity_or_status_mismatch",
            order=latest_dict,
        )

    # =====================================================================
    # PR81 Final Amendment v2 §3 — opportunity-ledger lifecycle bridge
    # =====================================================================
    # OSM order status → opportunity_status mapping. ENTRY orders only.
    _ENTRY_OSM_TO_OPPORTUNITY: dict = {
        OrderStatus.SUBMITTED:    "BROKER_SUBMITTED",
        OrderStatus.ACKNOWLEDGED: "BROKER_ACKED",
        OrderStatus.FILLED:       "FILLED",
        # Terminals
        OrderStatus.REJECTED:     "BROKER_REJECTED",
        OrderStatus.EXPIRED:      "EXPIRED",
        OrderStatus.CANCELED:     "CANCELED",
    }
    # Miss-stage for each terminal mapping.
    _ENTRY_TERMINAL_MISS_STAGE: dict = {
        "BROKER_REJECTED": "BROKER_ACK",
        "EXPIRED":         "FILL_MONITOR",
        "CANCELED":        "FILL_MONITOR",
    }

    def _notify_opportunity_ledger(
        self, *,
        current: dict,
        new_status: str,
        broker_order_id=None,
        position_id=None,
        last_error: Optional[str] = None,
        fill_price=None,
        filled_qty=None,
        filled_ts=None,
    ) -> None:
        """Translate an OSM transition into the appropriate opportunity-
        ledger update. ENTRY orders only. Wires Final Amendment v2 §3.

        Mapping:
            SUBMITTED         → BROKER_SUBMITTED
            ACKNOWLEDGED      → BROKER_ACKED
            FILLED            → FILLED (with broker_order_id + position_id)
            REJECTED          → BROKER_REJECTED  (miss_stage=BROKER_ACK)
            EXPIRED           → EXPIRED           (miss_stage=FILL_MONITOR)
            CANCELED          → CANCELED          (miss_stage=FILL_MONITOR)
        Exit orders, PARTIAL_FILL, ERROR are intentionally not propagated.
        """
        kind = str((current or {}).get("kind") or "").upper()
        if kind != "ENTRY":
            return
        opp_status = self._ENTRY_OSM_TO_OPPORTUNITY.get(new_status)
        if not opp_status:
            return

        signal_id  = str((current or {}).get("signal_id") or "")
        canonical  = str((current or {}).get("canonical_signal_id") or signal_id)
        client_id  = str((current or {}).get("client_id") or self.client_id or "")
        local_id   = str((current or {}).get("local_order_id") or "")
        broker_id  = str(broker_order_id or "") or None
        pos_id     = str(position_id or "") or None

        if not (signal_id or canonical) or not client_id:
            log.debug(
                "[%s] opportunity ledger notify skipped — missing ids | "
                "signal_id=%r canonical=%r client=%r",
                self.client_id, signal_id, canonical, client_id,
            )
            return

        try:
            from ap.opportunity_ledger import (
                update_opportunity, mark_filled,
                STAGE_BROKER_ACK, STAGE_FILL_MONITOR,
            )
        except Exception:
            return

        miss_stage = self._ENTRY_TERMINAL_MISS_STAGE.get(opp_status)
        miss_reason = None
        if miss_stage:
            miss_reason = last_error or opp_status.lower()

        if opp_status == "FILLED":
            # Amendment v3: FILLED carries full broker-truth proof so the
            # repair is auditable. The transition() caller supplies fill
            # price / qty / ts; we tag the source as the OSM transition.
            mark_filled(
                signal_id or canonical, client_id,
                canonical_signal_id=canonical,
                order_local_id=local_id or None,
                broker_order_id=broker_id,
                position_id=pos_id,
                fill_price=(float(fill_price) if fill_price is not None else None),
                filled_qty=(int(filled_qty) if filled_qty is not None else None),
                fill_ts=(str(filled_ts) if filled_ts else None),
                source="osm_transition",
            )
            return

        update_opportunity(
            signal_id or canonical, client_id, opp_status,
            canonical_signal_id=canonical,
            order_local_id=local_id or None,
            broker_order_id=broker_id,
            position_id=None,
            miss_stage=miss_stage,
            miss_reason=miss_reason,
        )

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

    def update_order_meta(self, local_order_id: str, meta_patch: dict) -> bool:
        """Merge *meta_patch* into orders.meta using a safe JSONB || merge.

        ONLY the keys supplied in *meta_patch* are written.  All other existing
        meta keys are preserved by Postgres (|| is non-destructive).  Callers
        must pass only the fields they intend to update — never the full
        existing meta snapshot — to avoid race-condition overwrites of
        concurrent writers (retry_status, retry_payload, etc.).

        Uses COALESCE(meta, '{}'::jsonb) so rows with a NULL meta column are
        handled safely without raising.

        Returns True only when Postgres confirms rowcount > 0 (the row exists
        and was updated).  Returns False on not-found or write error; callers
        must treat False as best-effort only.
        """
        import json as _json_local
        try:
            _patch_json = _json_local.dumps(meta_patch, default=str)
        except Exception:
            return False

        def _fn():
            with conn() as c:
                cur = c.execute(
                    "UPDATE orders "
                    "SET meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb, "
                    "    updated_ts = NOW() "
                    "WHERE local_order_id = %s AND client_id = %s",
                    (_patch_json, local_order_id, self.client_id),
                )
                # psycopg2: execute() returns the cursor; rowcount is on the cursor.
                # Never use `or 1` fallback — rowcount=0 means row not found.
                return getattr(cur, "rowcount", getattr(c, "rowcount", None))

        try:
            rowcount = run_with_retry(_fn)
            return bool(rowcount and rowcount > 0)
        except Exception as exc:
            log.warning(
                "[%s] update_order_meta failed for local_order_id=%s: %s",
                self.client_id, local_order_id, exc,
            )
            return False

    def retire_unsubmitted_exit_intent(self, local_order_id: str, *, last_error: str) -> bool:
        """Atomically retire one EXIT_REQUESTED row only if no submit evidence exists."""
        error_text = str(last_error or "NO_POST_ATTEMPTED")

        def _fn():
            with conn() as c:
                cur = c.execute(
                    "UPDATE orders "
                    "SET status=%s, last_error=%s, updated_ts=NOW() "
                    "WHERE local_order_id=%s "
                    "  AND client_id=%s "
                    "  AND kind='EXIT' "
                    "  AND status=%s "
                    "  AND COALESCE(broker_order_id,'')='' "
                    "  AND submitted_ts IS NULL "
                    "  AND NULLIF(COALESCE(meta->>'submit_intent_at', ''), '') IS NULL "
                    "  AND COALESCE((meta->>'split_brain_quarantine')::boolean, false)=false "
                    "  AND COALESCE((meta->>'reconciliation_required')::boolean, false)=false",
                    (
                        OrderStatus.ERROR,
                        error_text,
                        local_order_id,
                        self.client_id,
                        OrderStatus.EXIT_REQUESTED,
                    ),
                )
                return getattr(cur, "rowcount", getattr(c, "rowcount", None))

        try:
            rowcount = run_with_retry(_fn)
            return bool(rowcount and rowcount > 0)
        except Exception as exc:
            log.warning(
                "[%s] retire_unsubmitted_exit_intent failed for local_order_id=%s: %s",
                self.client_id, local_order_id, exc,
            )
            return False

    def claim_deferred_broker_ready_submit(
        self,
        local_order_id: str,
        *,
        owner: str,
        generation: int,
    ) -> bool:
        """Fence recovery submit so only one worker can enter canonical gates."""
        import json as _json_local
        owner = str(owner or "").strip()
        if not owner:
            return False
        try:
            generation = int(generation)
        except (TypeError, ValueError):
            return False
        now = datetime.now(timezone.utc)
        patch = _json_local.dumps({
            "current_owner": owner,
            "recovery_submit_owner": owner,
            "recovery_submit_claimed_at": now.isoformat(),
            "recovery_submit_lease_until": (now + timedelta(seconds=60)).isoformat(),
        })

        def _claim():
            with conn() as c:
                cur = c.execute(
                    """
                    UPDATE orders
                    SET meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb,
                        updated_ts = NOW()
                    WHERE local_order_id = %s
                      AND client_id = %s
                      AND kind = 'ENTRY'
                      AND UPPER(COALESCE(status,'')) = 'PENDING_TRIGGER'
                      AND (broker_order_id IS NULL OR broker_order_id = '')
                      AND submitted_ts IS NULL
                      AND COALESCE((meta->>'broker_ready')::boolean, false) = true
                      AND COALESCE(meta->>'lifecycle_state','') = 'BROKER_READY'
                      AND COALESCE((meta->>'materialization_generation')::int, 0) = %s
                      AND COALESCE(meta->>'submit_intent_at','') = ''
                      AND (
                            COALESCE(meta->>'recovery_submit_owner','') = ''
                         OR COALESCE(meta->>'recovery_submit_lease_until','') < %s
                      )
                    """,
                    (patch, local_order_id, self.client_id, generation, now.isoformat()),
                )
                return int(getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0)

        try:
            return bool(run_with_retry(_claim) > 0)
        except Exception as exc:
            log.warning(
                "[%s] claim_deferred_broker_ready_submit failed order=%s: %s",
                self.client_id, local_order_id, exc,
            )
            return False

    def persist_deferred_submit_intent(
        self,
        local_order_id: str,
        *,
        owner: str,
        generation: int,
        execution_mode: str,
        payload_hash: str,
        broker_submit_key: str,
    ) -> bool:
        """Atomically transfer an unexpired recovery claim to broker submit.

        This is the irreversible boundary fence. A stale recovery worker may
        not create submit intent after its lease expires or ownership changes.
        Once this CAS succeeds, ``submit_intent_at`` prevents any replacement
        worker from claiming the row while broker truth is ambiguous.
        """
        import json as _json_local
        owner = str(owner or "").strip()
        mode = str(execution_mode or "").strip().lower()
        submit_key = canonical_broker_submit_key(broker_submit_key)
        payload_hash = str(payload_hash or "").strip()
        try:
            generation = int(generation)
        except (TypeError, ValueError):
            return False

        if not owner or mode not in {"live", "paper"} or not submit_key or not payload_hash:
            return False
        now = now_utc_iso()
        patch = _json_local.dumps({
            "lifecycle_state": "SUBMITTING",
            "submit_started_at": now,
            "submit_intent_at": now,
            "broker_submit_key": submit_key,
            "broker_submit_payload_hash": payload_hash,
            "current_owner": f"broker_submit:{submit_key}",
        })

        def _persist():
            with conn() as c:
                cur = c.execute(
                    """
                    UPDATE orders
                    SET meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb,
                        updated_ts = NOW()
                    WHERE local_order_id = %s
                      AND client_id = %s
                      AND LOWER(COALESCE(execution_mode,'')) = %s
                      AND kind = 'ENTRY'
                      AND UPPER(COALESCE(status,'')) = 'PENDING_TRIGGER'
                      AND (broker_order_id IS NULL OR broker_order_id = '')
                      AND submitted_ts IS NULL
                      AND COALESCE(meta->>'lifecycle_state','') = 'BROKER_READY'
                      AND COALESCE((meta->>'broker_ready')::boolean, false) = true
                      AND COALESCE((meta->>'materialization_generation')::int, 0) = %s
                      AND COALESCE(meta->>'recovery_submit_owner','') = %s
                      AND NULLIF(meta->>'recovery_submit_lease_until','')::timestamptz >= NOW()
                      AND COALESCE(meta->>'submit_intent_at','') = ''
                    """,
                    (patch, local_order_id, self.client_id, mode, generation, owner),
                )
                return int(getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0)

        try:
            return bool(run_with_retry(_persist) > 0)
        except Exception as exc:
            log.warning(
                "[%s] persist_deferred_submit_intent failed order=%s: %s",
                self.client_id, local_order_id, exc,
            )
            return False

    def persist_materialized_submit_intent(
        self,
        local_order_id: str,
        *,
        generation: int,
        execution_mode: str,
        signal_id: str,
        payload_hash: str,
        broker_submit_key: str,
    ) -> bool:
        """Atomically claim broker submit intent for a watcher-owned BROKER_READY row.

        This closes the watcher-vs-recovery race on materialized deferred rows.
        A normal live watcher may proceed only while no recovery worker has
        fenced submit ownership and no prior submit intent has landed.
        """
        import json as _json_local
        payload_hash = str(payload_hash or "").strip()
        submit_key = canonical_broker_submit_key(broker_submit_key)
        mode = str(execution_mode or "").strip().lower()
        durable_signal_id = str(signal_id or "").strip()
        try:
            generation = int(generation)
        except (TypeError, ValueError):
            return False
        if (
            generation < 1
            or mode not in {"live", "paper"}
            or not durable_signal_id
            or not payload_hash
            or not submit_key
        ):
            return False
        now = now_utc_iso()
        patch = _json_local.dumps({
            "lifecycle_state": "SUBMITTING",
            "submit_started_at": now,
            "submit_intent_at": now,
            "broker_submit_key": submit_key,
            "broker_submit_payload_hash": payload_hash,
            "current_owner": f"broker_submit:{submit_key}",
        })

        def _persist():
            with conn() as c:
                cur = c.execute(
                    """
                    UPDATE orders
                    SET meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb,
                        updated_ts = NOW()
                    WHERE local_order_id = %s
                      AND client_id = %s
                      AND LOWER(COALESCE(execution_mode,'')) = %s
                      AND COALESCE(signal_id,'') = %s
                      AND kind = 'ENTRY'
                      AND UPPER(COALESCE(status,'')) = 'PENDING_TRIGGER'
                      AND (broker_order_id IS NULL OR broker_order_id = '')
                      AND submitted_ts IS NULL
                      AND COALESCE(meta->>'lifecycle_state','') = 'BROKER_READY'
                      AND COALESCE((meta->>'broker_ready')::boolean, false) = true
                      AND COALESCE((meta->>'materialization_generation')::int, 0) = %s
                      AND COALESCE(meta->>'submit_intent_at','') = ''
                      AND COALESCE(meta->>'recovery_submit_owner','') = ''
                    """,
                    (
                        patch, local_order_id, self.client_id, mode,
                        durable_signal_id, generation,
                    ),
                )
                return int(getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0)

        try:
            return bool(run_with_retry(_persist) > 0)
        except Exception as exc:
            log.warning(
                "[%s] persist_materialized_submit_intent failed order=%s: %s",
                self.client_id, local_order_id, exc,
            )
            return False

    def persist_entry_submit_intent(
        self,
        local_order_id: str,
        *,
        current_status: str,
        execution_mode: str,
        signal_id: str,
        contract: str,
        qty: int,
        limit_price: float,
        payload_hash: str,
        broker_submit_key: str,
    ) -> bool:
        """Atomically claim the first broker POST for an ordinary entry row."""
        submit_key = canonical_broker_submit_key(broker_submit_key)
        mode = str(execution_mode or "").strip().lower()
        status = str(current_status or "").strip().upper()
        durable_signal_id = str(signal_id or "").strip()
        durable_contract = str(contract or "").strip()
        payload_hash = str(payload_hash or "").strip()
        try:
            durable_qty = int(qty)
            durable_limit = round(float(limit_price), 2)
        except (TypeError, ValueError):
            return False
        if (
            status not in {OrderStatus.CREATED, OrderStatus.PENDING_TRIGGER}
            or mode not in {"live", "paper"}
            or not durable_signal_id
            or not durable_contract
            or durable_qty <= 0
            or durable_limit <= 0
            or not payload_hash
            or not submit_key
        ):
            return False

        now = now_utc_iso()
        patch = json.dumps({
            "lifecycle_state": "SUBMITTING",
            "submit_started_at": now,
            "submit_intent_at": now,
            "broker_submit_key": submit_key,
            "broker_submit_payload_hash": payload_hash,
            "current_owner": f"broker_submit:{submit_key}",
        })

        def _persist():
            with conn() as c:
                cur = c.execute(
                    """
                    UPDATE orders
                    SET meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb,
                        updated_ts = NOW()
                    WHERE local_order_id = %s
                      AND client_id = %s
                      AND LOWER(COALESCE(execution_mode,'')) = %s
                      AND COALESCE(signal_id,'') = %s
                      AND kind = 'ENTRY'
                      AND UPPER(COALESCE(status,'')) = %s
                      AND COALESCE(contract,'') = %s
                      AND COALESCE(qty,0) = %s
                      AND ROUND(COALESCE(limit_price,0)::numeric, 2) = %s
                      AND (broker_order_id IS NULL OR broker_order_id = '')
                      AND submitted_ts IS NULL
                      AND COALESCE(meta->>'submit_intent_at','') = ''
                      AND UPPER(COALESCE(meta->>'lifecycle_state',''))
                          NOT IN ('SUBMITTING','SUBMITTED')
                      AND COALESCE(meta->>'recovery_submit_owner','') = ''
                      AND COALESCE(meta->>'split_brain_quarantine','') = ''
                      AND COALESCE(last_error,'') NOT LIKE 'SPLIT_BRAIN:%%'
                    """,
                    (
                        patch, local_order_id, self.client_id, mode,
                        durable_signal_id, status, durable_contract,
                        durable_qty, durable_limit,
                    ),
                )
                return int(getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0)

        try:
            return bool(run_with_retry(_persist) > 0)
        except Exception as exc:
            log.warning(
                "[%s] persist_entry_submit_intent failed order=%s: %s",
                self.client_id, local_order_id, exc,
            )
            return False

    def terminalize_recovered_entry(
        self,
        local_order_id: str,
        *,
        owner: str,
        generation: int,
        execution_mode: str,
        terminal_status: str,
        reason: str,
    ) -> bool:
        """Terminalize only while the exact recovered callback still owns it."""
        import json as _json_local
        owner = str(owner or "").strip()
        mode = str(execution_mode or "").strip().lower()
        terminal = str(terminal_status or "").strip().upper()
        if terminal not in {"EXPIRED", "CANCELED", "ERROR", "REJECTED"}:
            return False
        try:
            generation = int(generation)
        except (TypeError, ValueError):
            return False
        if not owner or mode not in {"live", "paper"}:
            return False
        now = now_utc_iso()
        meta_patch = _json_local.dumps({
            "lifecycle_state": terminal,
            "reason_code": str(reason or "RECOVERED_ENTRY_TERMINAL"),
            "final_reason": str(reason or "RECOVERED_ENTRY_TERMINAL"),
            "materialization_in_flight": False,
            "broker_ready": False,
            "current_owner": "TERMINAL",
            "recovery_terminalized_at": now,
        })

        def _terminalize():
            with conn() as c:
                cur = c.execute(
                    """
                    UPDATE orders
                    SET status = %s,
                        last_error = %s,
                        meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb,
                        updated_ts = NOW()
                    WHERE local_order_id = %s
                      AND client_id = %s
                      AND LOWER(COALESCE(execution_mode,'')) = %s
                      AND kind = 'ENTRY'
                      AND UPPER(COALESCE(status,'')) IN ('CREATED','PENDING_TRIGGER')
                      AND (broker_order_id IS NULL OR broker_order_id = '')
                      AND submitted_ts IS NULL
                      AND COALESCE((meta->>'materialization_generation')::int, 0) = %s
                      AND COALESCE(meta->>'recovery_submit_owner','') = %s
                      AND NULLIF(meta->>'recovery_submit_lease_until','')::timestamptz >= NOW()
                      AND COALESCE(meta->>'lifecycle_state','') = 'BROKER_READY'
                      AND COALESCE(meta->>'submit_intent_at','') = ''
                    """,
                    (
                        terminal, str(reason or "RECOVERED_ENTRY_TERMINAL"), meta_patch,
                        local_order_id, self.client_id, mode, generation, owner,
                    ),
                )
                return int(getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0)

        try:
            return bool(run_with_retry(_terminalize) > 0)
        except Exception as exc:
            log.warning(
                "[%s] terminalize_recovered_entry failed order=%s: %s",
                self.client_id, local_order_id, exc,
            )
            return False

    def claim_deferred_materialization(
        self,
        local_order_id: str,
        *,
        owner: str,
        generation: int | None = None,
        new_generation: int | None = None,
        lease_until: str,
        trigger_crossed_at: str,
        trigger_price: float,
        observed_underlying_price: float,
        signal_id: str,
        execution_mode: str,
        # P0 AMENDMENT (fix/deferred-retry-due-execution-p0 blocker §5):
        # Advance retry_attempt atomically with generation so the durable row
        # always shows the correct attempt count for the in-flight claim, even
        # if the process crashes between claim and schedule_deferred_materialization_retry.
        retry_attempt: int | None = None,
    ) -> bool:
        """Atomically fence one deferred-breach materialization worker.

        AMENDMENT §3 (strictly monotonic generation fencing)
        ---------------------------------------------------
        Every new ownership claim MUST atomically advance the persisted
        generation by exactly one.  The SQL predicate requires the
        persisted ``materialization_generation`` to equal
        ``new_generation - 1``; the patch writes ``new_generation``.
        This closes the stale-worker window where a worker at
        ``generation=N`` could previously claim a row where the
        persisted value was already ``N`` or lower (the old ``<= %s``
        predicate).

        The ``generation`` keyword is preserved as a legacy alias for
        ``new_generation`` — callers that already computed the correct
        "next" value keep working; the SQL contract silently rejects
        any caller whose expected-previous did not match the durable
        row.

        The order row remains ``PENDING_TRIGGER`` until broker
        submission, but the durable lifecycle in ``orders.meta`` moves
        to ``MATERIALIZING``.  A different owner cannot claim the same
        generation while its lease is live; once the lease expires the
        next claim advances the generation, and any downstream write
        from the prior owner fails because ``persist_deferred_broker_ready``,
        ``schedule_deferred_materialization_retry`` and
        ``terminalize_deferred_breach`` all require exact owner +
        generation match.
        """
        import json as _json_local

        _owner = str(owner or "").strip()
        _signal_id = str(signal_id or "").strip()
        _mode = str(execution_mode or "").strip().lower()

        # ── §3: resolve the target new generation ────────────────────
        # new_generation takes precedence when both are supplied.
        _candidate = new_generation if new_generation is not None else generation
        try:
            _new_generation = int(_candidate) if _candidate is not None else 1
        except (TypeError, ValueError):
            return False

        if _new_generation < 1:
            return False
        _expected_previous_generation = _new_generation - 1  # strictly monotonic

        if not _owner or not _signal_id or _mode not in ("live", "paper"):
            return False

        _now = now_utc_iso()
        _patch = {
            "lifecycle_state": "MATERIALIZING",
            "materialization_status": "RUNNING",
            "materialization_in_flight": True,
            "materialization_owner": _owner,
            "watcher_token": _owner,
            "current_owner": _owner,
            # A fresh confirmed breach supersedes any recovery-owned
            # direction-reversal waiting state.
            "recovery_ownership": "",
            "recovery_owner": "",
            "direction_reversal_rearm_requires_watcher": False,
            "watcher_generation": _new_generation,
            "final_market_truth_status": "",
            "final_market_truth": {},
            "materialization_generation": _new_generation,
            "materialization_claimed_at": _now,
            "materialization_lease_until": str(lease_until or ""),
            "materialization_started_at": _now,
            "selector_started_at": _now,
            "trigger_crossed_at": str(trigger_crossed_at or _now),
            "breach_received_at": _now,
            "trigger_price": float(trigger_price or 0),
            "observed_underlying_price": float(observed_underlying_price or 0),
            "signal_id": _signal_id,
            "local_order_id": str(local_order_id or ""),
            "client_id": self.client_id,
            "execution_mode": _mode,
            "broker_ready": False,
        }
        # P0 AMENDMENT blocker §4 (second round): atomically advance the
        # CANONICAL retry_attempt field in the same JSONB merge as the
        # generation advance. retry_attempt_in_flight is also written as
        # a diagnostic alias for operators. The SQL predicate verifies the
        # previous canonical attempt to prevent re-use of an already-claimed
        # attempt slot (belt-and-suspenders against concurrent claimants
        # after a lease expiry).
        _prev_attempt: int | None = None
        if retry_attempt is not None:
            try:
                _ra = int(retry_attempt)
                _patch["retry_attempt"] = _ra
                _patch["retry_attempt_in_flight"] = _ra
                _prev_attempt = max(0, _ra - 1)
            except (TypeError, ValueError):
                pass
        try:
            _patch_json = _json_local.dumps(_patch, default=str)
        except Exception:
            return False

        def _claim():
            with conn() as c:
                _attempt_predicate = ""
                _attempt_params: list = []
                if _prev_attempt is not None:
                    # Verify the canonical retry_attempt is at the expected
                    # prior value — prevents double-claiming an attempt slot.
                    _attempt_predicate = (
                        " AND COALESCE((meta->>'retry_attempt')::int, 0) = %s"
                    )
                    _attempt_params = [_prev_attempt]
                cur = c.execute(
                    """
                    UPDATE orders
                    SET meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb,
                        updated_ts = NOW()
                    WHERE local_order_id = %s
                      AND client_id = %s
                      AND kind = 'ENTRY'
                      AND UPPER(COALESCE(status,'')) = 'PENDING_TRIGGER'
                      AND (broker_order_id IS NULL OR broker_order_id = '')
                      AND submitted_ts IS NULL
                      AND signal_id = %s
                      AND LOWER(COALESCE(execution_mode,'')) = %s
                      AND COALESCE((meta->>'broker_ready')::boolean, false) = false
                      AND (
                            COALESCE(meta->>'lifecycle_state','') IN ('', 'RETRY_WAIT')
                         OR COALESCE(meta->>'materialization_lease_until','') < %s
                      )
                      AND COALESCE((meta->>'materialization_generation')::int, 0) = %s
                    """ + _attempt_predicate,
                    (
                        _patch_json, local_order_id, self.client_id,
                        _signal_id, _mode, _now, _expected_previous_generation,
                        *_attempt_params,
                    ),
                )
                return int(getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0)

        try:
            return bool(run_with_retry(_claim) > 0)
        except Exception as exc:
            log.warning(
                "[%s] claim_deferred_materialization failed order=%s: %s",
                self.client_id, local_order_id, exc,
            )
            return False

    def persist_selector_recovery_cursor(
        self,
        local_order_id: str,
        *,
        owner: str,
        generation: int,
        signal_id: str,
        execution_mode: str,
        cursor: dict,
    ) -> bool:
        """Persist bounded selector progress under the active materializer CAS."""
        import json as _json_local

        _owner = str(owner or "").strip()
        _signal = str(signal_id or "").strip()
        _mode = str(execution_mode or "").strip().lower()
        try:
            _generation = max(1, int(generation))
        except (TypeError, ValueError):
            return False
        if (
            not _owner
            or not _signal
            or _mode not in {"live", "paper"}
            or not isinstance(cursor, dict)
        ):
            return False
        try:
            _cursor_json = _json_local.dumps(cursor, default=str)
        except Exception:
            return False
        # Bound the serialized payload as a final defense behind the record
        # limits enforced by selector_retry_policy.
        if len(_cursor_json.encode("utf-8")) > 256_000:
            return False

        def _persist():
            with conn() as c:
                cur = c.execute(
                    """
                    UPDATE orders
                    SET meta = COALESCE(meta, '{}'::jsonb)
                               || jsonb_build_object(
                                    'selector_recovery_cursor_v1',
                                    %s::jsonb
                                  ),
                        updated_ts = NOW()
                    WHERE local_order_id = %s
                      AND client_id = %s
                      AND signal_id = %s
                      AND LOWER(TRIM(COALESCE(execution_mode,''))) = %s
                      AND kind = 'ENTRY'
                      AND UPPER(COALESCE(status,'')) = 'PENDING_TRIGGER'
                      AND (broker_order_id IS NULL OR broker_order_id = '')
                      AND submitted_ts IS NULL
                      AND COALESCE(meta->>'lifecycle_state','') = 'MATERIALIZING'
                      AND COALESCE(meta->>'materialization_owner','') = %s
                      AND COALESCE((meta->>'materialization_generation')::int, 0) = %s
                    """,
                    (
                        _cursor_json,
                        local_order_id,
                        self.client_id,
                        _signal,
                        _mode,
                        _owner,
                        _generation,
                    ),
                )
                return int(getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0)

        try:
            return bool(run_with_retry(_persist) > 0)
        except Exception as exc:
            log.warning(
                "[%s] persist_selector_recovery_cursor failed order=%s: %s",
                self.client_id,
                local_order_id,
                exc,
            )
            return False

    def rearm_deferred_materialization_direction_reversal(
        self,
        local_order_id: str,
        *,
        owner: str,
        watcher_token: str = "",
        generation: int,
        signal_id: str,
        execution_mode: str,
        market_truth_audit: dict,
    ) -> bool:
        """Return the same deferred ENTRY to a clean pre-breach state.

        A real in-process watcher may retain its exact watcher token by
        passing it as ``watcher_token``. A restart/due-retry callback runs
        against a synthetic (unregistered) watched object and MUST NOT pass
        one — the recovery takeover token used to win the original CAS
        (``owner``) is a claim authority only, never a proof of a live,
        registered APEntryWatcher. Persisting it as ``watcher_token`` is the
        exact defect this correction closes: it let a synthetic recovery
        callback masquerade as an attached watcher, after which
        ``resume_deferred_materialization_retry`` would try (and fail) to
        reschedule a materialization retry against a row this method had
        already released from MATERIALIZING.

        When no watcher_token is supplied, the row is left explicitly
        recovery-owned (``recovery_ownership='recovery_scheduler'``,
        ``direction_reversal_rearm_requires_watcher=True``) so the existing
        recovery health loop recognizes it must attach a real watcher on a
        later pass, rather than silently leaving an orphaned PENDING_TRIGGER
        row outside every executable retry path.
        """
        import json as _json_local

        _owner = str(owner or "").strip()
        _watcher_token = str(watcher_token or "").strip()
        _signal = str(signal_id or "").strip()
        _mode = str(execution_mode or "").strip().lower()
        try:
            _generation = max(1, int(generation))
        except (TypeError, ValueError):
            return False
        if not _owner or not _signal or _mode not in {"live", "paper"}:
            return False

        # In the uninterrupted watcher path, materialization ownership came
        # from this exact watcher token. Any disagreement is an ownership
        # conflict — fail closed rather than silently accepting a mismatched
        # token as authoritative.
        if _watcher_token and _watcher_token != _owner:
            return False

        _now = now_utc_iso()
        _recovery_owned = not bool(_watcher_token)
        _audit = dict(market_truth_audit or {})

        _patch = {
            "lifecycle_state": "",
            # A truly blank pre-breach materialization_status — never a
            # populated lifecycle value here. WAITING_FOR_TRIGGER is only
            # ever written later, by the durable ownership-adoption step,
            # once a REAL watcher has actually been proven attached.
            # REARM_DIRECTION_REVERSAL is preserved only as a diagnostic
            # (final_market_truth_status / last_direction_reversal_* below).
            "materialization_status": "",
            "materialization_in_flight": False,
            "materialization_owner": "",
            "materialization_lease_until": "",
            # current_owner must never fall back to the recovery claim
            # token. A recovery takeover token is not watcher ownership —
            # `_watcher_token or _owner` is the exact defect this closes.
            "current_owner": _watcher_token if _watcher_token else "",
            "watcher_token": _watcher_token,
            "watcher_generation": _generation if _watcher_token else 0,
            "watcher_registered_at": _now if _watcher_token else "",
            "recovery_ownership": (
                "recovery_scheduler" if _recovery_owned else ""
            ),
            "recovery_owner": _owner if _recovery_owned else "",
            "direction_reversal_rearm_requires_watcher": _recovery_owned,
            "broker_ready": False,
            # Direction reversal starts a fresh selector attempt.  Preserve the
            # monotonic materialization_generation, but clear every active
            # attempt/schedule authority so the next confirmed breach is
            # attempt 1 with no stale cursor requirement.
            "retry_attempt": 0,
            "retry_attempt_in_flight": 0,
            "breach_attempt_count": 0,
            "materialization_attempts": 0,
            "retry_max_attempts": 0,
            "next_retry_at": "",
            "materialization_next_retry_at": "",
            "deferred_retry_scheduled": False,
            "deferred_retry_reason_code": "",
            "deferred_retry_attempt": 0,
            "deferred_retry_max_attempts": 0,
            "deferred_retry_delay_seconds": 0,
            "deferred_retry_scheduled_at": "",
            "deferred_retry_next_attempt_at": "",
            "deferred_retry_terminal_reason": "",
            "retry_reason": "",
            "materialization_reason": "",
            "retry_owner": "",
            "materialization_retry_owner": "",
            "materialization_retry_attempt": 0,
            "materialization_retry_max_attempts": 0,
            "watcher_retry_attempt": 0,
            "watcher_next_retry_at": "",
            "selector_failure": {},
            "materialization_selector_failure": {},
            "materialization_outcome": "",
            "materialization_detail": "",
            "entry_path": "",
            # Preserve the decision as diagnostics only.
            "final_market_truth_status": "REARM_DIRECTION_REVERSAL",
            "final_market_truth": _audit,
            "last_direction_reversal_market_truth": _audit,
            "last_direction_reversal_rearmed_at": _now,
            "selector_recovery_cursor_v1": None,
            "rearmed_at": _now,
        }
        try:
            _patch_json = _json_local.dumps(_patch, default=str)
        except Exception:
            return False

        def _rearm():
            with conn() as c:
                cur = c.execute(
                    """
                    UPDATE orders
                    SET meta = (
                            COALESCE(meta, '{}'::jsonb)
                            || %s::jsonb
                            || jsonb_strip_nulls(jsonb_build_object(
                                'first_trigger_crossed_at',
                                COALESCE(
                                    NULLIF(meta->>'first_trigger_crossed_at', ''),
                                    NULLIF(meta->>'trigger_crossed_at', ''),
                                    NULLIF(meta->>'triggered_at', '')
                                ),
                                'first_trigger_crossed_at_provenance',
                                COALESCE(
                                    meta->'first_trigger_crossed_at_provenance',
                                    meta->'trigger_crossed_at_provenance'
                                ),
                                'first_trigger_confirmed_at',
                                COALESCE(
                                    NULLIF(
                                        meta->>'first_trigger_confirmed_at',
                                        ''
                                    ),
                                    NULLIF(meta->>'trigger_confirmed_at', ''),
                                    NULLIF(
                                        meta->>'last_confirmed_trigger_at',
                                        ''
                                    )
                                ),
                                'first_trigger_breach_bid',
                                COALESCE(
                                    meta->'first_trigger_breach_bid',
                                    meta->'first_breach_bid'
                                ),
                                'first_trigger_breach_ask',
                                COALESCE(
                                    meta->'first_trigger_breach_ask',
                                    meta->'first_breach_ask'
                                ),
                                'first_trigger_confirmation_quote',
                                COALESCE(
                                    meta->'first_trigger_confirmation_quote',
                                    meta->'last_trigger_confirmation_quote'
                                ),
                                'last_direction_reversal_retry_reason',
                                COALESCE(
                                    NULLIF(meta->>'retry_reason', ''),
                                    NULLIF(
                                        meta->>'materialization_reason',
                                        ''
                                    )
                                ),
                                'last_direction_reversal_selector_failure',
                                COALESCE(
                                    meta->'materialization_selector_failure',
                                    meta->'selector_failure'
                                ),
                                'last_direction_reversal_materialization_outcome',
                                NULLIF(meta->>'materialization_outcome', '')
                            ))
                        )
                        - 'selector_recovery_cursor_v1'
                        - 'trigger_crossed_at'
                        - 'trigger_crossed_at_provenance'
                        - 'triggered_at'
                        - 'trigger_confirmed_at'
                        - 'last_confirmed_trigger_at'
                        - 'original_trigger_crossed_at'
                        - 'first_breach_bid'
                        - 'first_breach_ask'
                        - 'last_trigger_confirmation_quote',
                        updated_ts = NOW()
                    WHERE local_order_id = %s
                      AND client_id = %s
                      AND signal_id = %s
                      AND LOWER(TRIM(COALESCE(execution_mode,''))) = %s
                      AND kind = 'ENTRY'
                      AND UPPER(COALESCE(status,'')) = 'PENDING_TRIGGER'
                      AND (broker_order_id IS NULL OR broker_order_id = '')
                      AND submitted_ts IS NULL
                      AND COALESCE(meta->>'lifecycle_state','') = 'MATERIALIZING'
                      AND COALESCE(meta->>'materialization_owner','') = %s
                      AND COALESCE((meta->>'materialization_generation')::int, 0) = %s
                    """,
                    (
                        _patch_json,
                        local_order_id,
                        self.client_id,
                        _signal,
                        _mode,
                        _owner,
                        _generation,
                    ),
                )
                return int(getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0)

        try:
            return bool(run_with_retry(_rearm) > 0)
        except Exception as exc:
            log.warning(
                "[%s] rearm_deferred_materialization_direction_reversal failed "
                "order=%s: %s",
                self.client_id,
                local_order_id,
                exc,
            )
            return False

    def adopt_direction_reversal_watcher_ownership(
        self,
        local_order_id: str,
        *,
        recovery_owner: str,
        watcher_token: str,
        generation: int,
        signal_id: str,
        execution_mode: str,
    ) -> bool:
        """Transfer durable watcher ownership from a recovery-owned,
        direction-reversal-rearmed row to a real, just-registered watcher.

        Reused nowhere else — this is the narrow gap left after
        ``rearm_deferred_materialization_direction_reversal`` releases a row
        to ``recovery_ownership='recovery_scheduler'`` (no real watcher
        proven yet) and a later ``PendingTriggerRestartRecovery`` pass then
        proves a real ``APEntryWatcher`` registration for it. Neither of
        those two functions writes the other's half of durable ownership;
        this is that missing, explicit, CAS-fenced write.

        The CAS is fail-closed on exact identity, exact generation, and the
        exact recovery-owned state left by the rearm — never a monotonic or
        greater-than acceptance. It also requires ``watcher_token`` to still
        be blank durably, so a second concurrent adoption cannot double-claim
        the row.
        """
        import json as _json_local

        _recovery_owner = str(recovery_owner or "").strip()
        _token = str(watcher_token or "").strip()
        _signal = str(signal_id or "").strip()
        _mode = str(execution_mode or "").strip().lower()
        try:
            _generation = int(generation)
        except (TypeError, ValueError):
            return False
        if (
            not _recovery_owner
            or not _token
            or not _signal
            or _mode not in {"live", "paper"}
            or _generation < 1
        ):
            return False

        _now = now_utc_iso()
        _patch = {
            "materialization_status": "WAITING_FOR_TRIGGER",
            "current_owner": _token,
            "watcher_token": _token,
            "watcher_generation": _generation,
            "watcher_registered_at": _now,
            "recovery_owner": "",
            "recovery_ownership": "",
            "direction_reversal_rearm_requires_watcher": False,
        }
        try:
            _patch_json = _json_local.dumps(_patch, default=str)
        except Exception:
            return False

        def _adopt():
            with conn() as c:
                cur = c.execute(
                    """
                    UPDATE orders
                    SET meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb,
                        updated_ts = NOW()
                    WHERE local_order_id = %s
                      AND client_id = %s
                      AND signal_id = %s
                      AND LOWER(TRIM(COALESCE(execution_mode,''))) = %s
                      AND kind = 'ENTRY'
                      AND UPPER(COALESCE(status,'')) = 'PENDING_TRIGGER'
                      AND (broker_order_id IS NULL OR broker_order_id = '')
                      AND submitted_ts IS NULL
                      AND COALESCE(meta->>'lifecycle_state','') = ''
                      AND COALESCE(meta->>'recovery_ownership','') = 'recovery_scheduler'
                      AND COALESCE(meta->>'recovery_owner','') = %s
                      AND COALESCE(meta->>'watcher_token','') = ''
                      AND COALESCE((meta->>'materialization_generation')::int, 0) = %s
                    """,
                    (
                        _patch_json,
                        local_order_id,
                        self.client_id,
                        _signal,
                        _mode,
                        _recovery_owner,
                        _generation,
                    ),
                )
                return int(getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0)

        try:
            return bool(run_with_retry(_adopt) > 0)
        except Exception as exc:
            log.warning(
                "[%s] adopt_direction_reversal_watcher_ownership failed "
                "order=%s: %s",
                self.client_id,
                local_order_id,
                exc,
            )
            return False

    def retain_recovery_ownership_if_no_watcher(
        self,
        local_order_id: str,
        *,
        recovery_owner: str,
        reason: str,
        recovery_retention_mode: str,
    ) -> bool:
        """Write recovery-authority retention markers, but ONLY if no
        committed watcher authority already exists on the row.

        PR #421 final amendment (§5): the plain recovery-retention write
        this replaces (a bare ``update_order_meta`` merge) had no fencing
        against existing watcher ownership at all — it could blindly write
        ``recovery_ownership`` / ``recovery_owner`` on top of a row whose
        watcher authority (``current_owner`` / ``watcher_token`` /
        ``watcher_generation``) was already durably committed moments
        earlier, e.g. by a watcher-adoption CAS that itself succeeded but
        whose immediate post-write verification reread failed or raised.
        That produces exactly the dual-authority state PR #421 exists to
        prevent: a row simultaneously claimed by a real watcher and by
        recovery.

        Fenced the same way ``adopt_direction_reversal_watcher_ownership``
        fences the opposite transfer: a single conditional UPDATE whose
        WHERE clause requires the watcher fields to already be blank, not
        a read-then-write check with a TOCTOU gap.

        The watcher-generation half of that fence accepts both '' and '0':
        ``rearm_deferred_materialization_direction_reversal`` writes
        ``watcher_generation`` as the JSON integer 0 (not blank) for its
        own legitimate no-watcher, recovery-owned state — Postgres's
        ``meta->>'watcher_generation'`` renders that as the text "0", not
        "". Requiring exact blank alone would make this fence reject the
        very state it exists to protect. Any other value (1, 4, "abc",
        etc.) still fails closed.

        Returns False (no-op, watcher authority preserved) if the row
        already carries committed watcher ownership, if the row does not
        exist, or on any write error. Callers must not treat False as
        confirmation the row is now in some other bad state — only that
        this specific write did not happen.
        """
        _recovery_owner = str(recovery_owner or "").strip()
        if not _recovery_owner:
            return False
        _now = now_utc_iso()
        _patch = {
            "recovery_ownership": "recovery_scheduler",
            "recovery_owner": _recovery_owner,
            "recovery_retained_at": _now,
            "recovery_retention_reason": str(reason or ""),
            "recovery_retention_mode": str(recovery_retention_mode or ""),
        }
        try:
            _patch_json = json.dumps(_patch, default=str)
        except Exception:
            return False

        def _retain():
            with conn() as c:
                cur = c.execute(
                    """
                    UPDATE orders
                    SET meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb,
                        updated_ts = NOW()
                    WHERE local_order_id = %s
                      AND client_id = %s
                      AND COALESCE(meta->>'current_owner', '') = ''
                      AND COALESCE(meta->>'watcher_token', '') = ''
                      AND COALESCE(meta->>'watcher_generation', '') IN ('', '0')
                    """,
                    (_patch_json, local_order_id, self.client_id),
                )
                return int(getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0)

        try:
            return bool(run_with_retry(_retain) > 0)
        except Exception as exc:
            log.warning(
                "[%s] retain_recovery_ownership_if_no_watcher failed "
                "order=%s: %s",
                self.client_id,
                local_order_id,
                exc,
            )
            return False

    def retain_rearm_watcher_required_recovery_ownership(
        self,
        local_order_id: str,
        *,
        recovery_owner: str,
        reason: str,
        recovery_retention_mode: str,
        client_id: str,
        signal_id: str,
        execution_mode: str,
        generation: int,
        expected_recovery_owner: str,
        canonical_signal_id: str = "",
    ) -> bool:
        """RWR-specific recovery-retention CAS.

        PR #421 final correction: ``retain_recovery_ownership_if_no_watcher``
        above only fences against committed WATCHER authority (current_owner
        / watcher_token / watcher_generation blank). That is not enough for
        the REARM_WATCHER_REQUIRED path, which validates a much larger
        exact-identity/exact-generation/exact-lifecycle surface before ever
        deciding to retain. A stale actor whose own expectations (generation
        N, say) no longer match the durable row — because a newer pass
        already advanced it to N+1, changed identity, or moved it past the
        crash window — must not be able to stamp recovery ownership back
        onto a row it has already lost authority over, merely because
        watcher fields happen to still read blank.

        This CAS re-asserts, atomically, at write time, the SAME exact
        facts the caller validated moments earlier by reading the row:
        exact identity (local_order_id/client_id/signal_id/execution_mode,
        and canonical_signal_id when the caller expected one), exact
        materialization_generation, ENTRY/PENDING_TRIGGER lifecycle, the
        same crash-window broker-absence columns, the exact prior
        recovery_owner this actor itself claimed the row with, and no
        committed watcher authority. Any mismatch — the row moved on in
        any of these dimensions since the caller's read — is rowcount=0:
        fail closed, no ownership write, by construction of one
        conditional UPDATE rather than a read-then-write decision.

        Callers whose validation itself already proved authority was lost
        (row missing, identity mismatch, generation advanced, malformed
        generation, crash-window/broker state advanced) must not call this
        at all — there is no exact state left to fence to, and calling it
        anyway would be a masked, less legible way of doing the same
        no-op. This method exists only for the legitimate-retention case:
        the row was proven to still be this actor's exact row, and only
        watcher registration/adoption itself failed this pass.
        """
        _recovery_owner = str(recovery_owner or "").strip()
        _expected_recovery_owner = str(expected_recovery_owner or "").strip()
        _client = str(client_id or "").strip().lower()
        _signal = str(signal_id or "").strip()
        _mode = str(execution_mode or "").strip().lower()
        _canonical = str(canonical_signal_id or "").strip()
        try:
            _generation = int(generation)
        except (TypeError, ValueError):
            return False
        if (
            not _recovery_owner
            or not _expected_recovery_owner
            or not _client
            or not _signal
            or _mode not in {"live", "paper"}
            or _generation < 1
        ):
            return False

        _now = now_utc_iso()
        _patch = {
            "recovery_ownership": "recovery_scheduler",
            "recovery_owner": _recovery_owner,
            "recovery_retained_at": _now,
            "recovery_retention_reason": str(reason or ""),
            "recovery_retention_mode": str(recovery_retention_mode or ""),
        }
        try:
            _patch_json = json.dumps(_patch, default=str)
        except Exception:
            return False

        def _retain_exact():
            with conn() as c:
                cur = c.execute(
                    """
                    UPDATE orders
                    SET meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb,
                        updated_ts = NOW()
                    WHERE local_order_id = %s
                      AND client_id = %s
                      AND signal_id = %s
                      AND LOWER(TRIM(COALESCE(execution_mode,''))) = %s
                      AND kind = 'ENTRY'
                      AND UPPER(COALESCE(status,'')) = 'PENDING_TRIGGER'
                      AND (broker_order_id IS NULL OR broker_order_id = '')
                      AND submitted_ts IS NULL
                      AND COALESCE(meta->>'submit_intent_at', '') = ''
                      AND LOWER(COALESCE(meta->>'broker_ready', 'false')) IN ('false', '')
                      AND COALESCE((meta->>'materialization_generation')::int, 0) = %s
                      AND COALESCE(meta->>'recovery_ownership', '') = 'recovery_scheduler'
                      AND COALESCE(meta->>'recovery_owner', '') = %s
                      AND COALESCE(meta->>'current_owner', '') = ''
                      AND COALESCE(meta->>'watcher_token', '') = ''
                      AND COALESCE(meta->>'watcher_generation', '') IN ('', '0')
                      AND (
                          %s = ''
                          OR COALESCE(
                              NULLIF(canonical_signal_id, ''),
                              meta->>'canonical_signal_id',
                              ''
                          ) = %s
                      )
                    """,
                    (
                        _patch_json,
                        local_order_id,
                        _client,
                        _signal,
                        _mode,
                        _generation,
                        _expected_recovery_owner,
                        _canonical,
                        _canonical,
                    ),
                )
                return int(getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0)

        try:
            return bool(run_with_retry(_retain_exact) > 0)
        except Exception as exc:
            log.warning(
                "[%s] retain_rearm_watcher_required_recovery_ownership "
                "failed order=%s: %s",
                self.client_id,
                local_order_id,
                exc,
            )
            return False

    def persist_deferred_broker_ready(
        self,
        local_order_id: str,
        *,
        owner: str,
        generation: int,
        signal_id: str,
        execution_mode: str,
        contract: str,
        limit_price: float,
        qty: int,
        reserved_cost: float,
        selector_meta: dict,
    ) -> bool:
        """Atomically copy the complete materialized payload into durable state."""
        import json as _json_local

        _contract = str(contract or "").strip()
        _owner = str(owner or "").strip()
        _signal_id = str(signal_id or "").strip()
        _mode = str(execution_mode or "").strip().lower()
        try:
            _generation = max(1, int(generation or 1))
            _limit = round(float(limit_price or 0), 2)
            _qty = int(qty or 0)
            _reserved = round(float(reserved_cost or 0), 2)
        except (TypeError, ValueError):
            return False
        # ── Req 2: MATERIALIZATION_COPYBACK_INVALID_PAYLOAD ─────────────────────
        # Emit structured diagnostic before returning False so operators can
        # distinguish "selector called this with bad args" from a DB-level failure.
        # Only safe fields are logged — no broker tokens or order secrets.
        if (
            not _owner or not _signal_id or _mode not in ("live", "paper")
            or not _contract or _contract.upper().startswith("DEFERRED:")
            or _limit <= 0.01 or _qty <= 0 or _reserved <= 0
        ):
            log.critical(
                "[%s] MATERIALIZATION_COPYBACK_INVALID_PAYLOAD | order=%s "
                "client_id=%s execution_mode=%s signal_id=%s "
                "contract=%r limit_price=%s qty=%s reserved_cost=%s "
                "generation=%s owner_present=%s",
                self.client_id, local_order_id,
                self.client_id, _mode, _signal_id,
                _contract, _limit, _qty, _reserved, _generation, bool(_owner),
            )
            return False

        _now = now_utc_iso()
        _meta = dict(selector_meta or {})
        _meta.update({
            "contract_deferred": False,
            "lifecycle_state": "BROKER_READY",
            "materialization_status": "SELECTED",
            "materialization_in_flight": False,
            "materialization_owner": _owner,
            "current_owner": _owner,
            "materialization_generation": _generation,
            "materialization_completed_at": _now,
            "selector_completed_at": _now,
            "copyback_completed_at": _now,
            "broker_ready": True,
            "selected_contract": _contract,
            "selected_limit": _limit,
            "selected_qty": _qty,
            "selected_reserved_cost": _reserved,
            "selector_failure": None,
            "selector_recovery_cursor_v1": None,
            "signal_id": _signal_id,
            "local_order_id": str(local_order_id or ""),
            "client_id": self.client_id,
            "execution_mode": _mode,
        })
        try:
            _meta_json = _json_local.dumps(_meta, default=str)
        except Exception:
            return False

        def _persist():
            with conn() as c:
                cur = c.execute(
                    """
                    UPDATE orders
                    SET contract = %s,
                        limit_price = %s,
                        qty = %s,
                        reserved_cost = %s,
                        contract_selection_status = 'CONTRACT_SELECTED',
                        meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb,
                        updated_ts = NOW()
                    WHERE local_order_id = %s
                      AND client_id = %s
                      AND kind = 'ENTRY'
                      AND UPPER(COALESCE(status,'')) = 'PENDING_TRIGGER'
                      AND (broker_order_id IS NULL OR broker_order_id = '')
                      AND submitted_ts IS NULL
                      AND signal_id = %s
                      AND LOWER(COALESCE(execution_mode,'')) = %s
                      AND COALESCE(meta->>'materialization_owner','') = %s
                      AND COALESCE((meta->>'materialization_generation')::int, 0) = %s
                      AND COALESCE(meta->>'lifecycle_state','') = 'MATERIALIZING'
                    """,
                    (
                        _contract, _limit, _qty, _reserved, _meta_json,
                        local_order_id, self.client_id, _signal_id, _mode,
                        _owner, _generation,
                    ),
                )
                return int(getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0)

        try:
            _rowcount = run_with_retry(_persist)
            if _rowcount == 0:
                # ── Req 2: MATERIALIZATION_COPYBACK_CAS_MISS ─────────────────
                # The fenced UPDATE matched zero rows. The row exists but the
                # predicate did not match — a concurrent owner, lifecycle advance,
                # or stale generation claim. Re-read for diagnostics.
                # NEVER terminalize here — caller owns the terminal decision.
                _row: dict = {}
                try:
                    def _reread_for_miss():
                        with conn() as c:
                            c.execute(
                                "SELECT status, broker_order_id, submitted_ts, "
                                "execution_mode, signal_id, "
                                "meta->>'lifecycle_state' AS lifecycle_state, "
                                "meta->>'materialization_owner' AS materialization_owner, "
                                "meta->>'materialization_generation' AS materialization_generation, "
                                "meta->>'recovery_submit_owner' AS recovery_submit_owner, "
                                "meta->>'submit_intent_at' AS submit_intent_at "
                                "FROM orders "
                                "WHERE local_order_id = %s AND client_id = %s",
                                (local_order_id, self.client_id),
                            )
                            return c.fetchone()
                    _raw = run_with_retry(_reread_for_miss)
                    _row = dict(_raw) if _raw else {}
                except Exception:
                    pass
                log.critical(
                    "[%s] MATERIALIZATION_COPYBACK_CAS_MISS | order=%s "
                    "current_status=%s broker_order_id_present=%s "
                    "submitted_ts_present=%s durable_execution_mode=%s "
                    "durable_signal_id=%s lifecycle_state=%s "
                    "materialization_owner=%s materialization_generation=%s "
                    "recovery_submit_owner=%s submit_intent_present=%s",
                    self.client_id, local_order_id,
                    _row.get("status"),
                    bool(_row.get("broker_order_id")),
                    bool(_row.get("submitted_ts")),
                    _row.get("execution_mode"),
                    _row.get("signal_id"),
                    _row.get("lifecycle_state"),
                    _row.get("materialization_owner"),
                    _row.get("materialization_generation"),
                    _row.get("recovery_submit_owner"),
                    bool(_row.get("submit_intent_at")),
                )
                return False
            return bool(_rowcount > 0)
        except Exception as exc:
            # ── Req 2: classify schema errors separately from generic DB errors ─
            _exc_str = str(exc)
            _is_schema_error = False
            if pg_errors is not None and isinstance(
                exc, (pg_errors.UndefinedColumn, pg_errors.UndefinedTable)
            ):
                _is_schema_error = True
            elif any(
                kw in _exc_str.lower()
                for kw in ("column", "does not exist", "undefined", "no such column")
            ):
                _is_schema_error = True

            if _is_schema_error:
                log.critical(
                    "[%s] MATERIALIZATION_COPYBACK_SCHEMA_ERROR | order=%s | "
                    "schema mismatch — verify contract_selection_status column "
                    "migration has been applied | error=%s",
                    self.client_id, local_order_id, _exc_str,
                )
            else:
                log.critical(
                    "[%s] MATERIALIZATION_COPYBACK_DB_ERROR | order=%s | error=%s",
                    self.client_id, local_order_id, _exc_str,
                )
            return False

    def schedule_deferred_materialization_retry(
        self,
        local_order_id: str,
        *,
        owner: str,
        generation: int,
        reason_code: str,
        attempt: int,
        max_attempts: int,
        next_retry_at: str,
        selector_failure: dict,
        selector_recovery_cursor: dict | None = None,
        signal_id: str = "",
        execution_mode: str = "",
    ) -> bool:
        """Durably transfer a fenced materializer claim to retry ownership."""
        import json as _json_local

        _owner = str(owner or "").strip()
        _reason = str(reason_code or "").strip()
        _selector_failure = (
            dict(selector_failure) if isinstance(selector_failure, dict) else {}
        )
        _signal_id = str(
            signal_id or _selector_failure.get("signal_id") or ""
        ).strip()
        _execution_mode = str(
            execution_mode or _selector_failure.get("execution_mode") or ""
        ).strip().lower()
        _expected_outcome = (
            "RETRY_LATER_SELECTOR_BUDGET"
            if _reason == "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
            else "RETRY_LATER_DATA_UNAVAILABLE"
        )
        _outcome = str(
            _selector_failure.get("materialization_outcome")
            or _expected_outcome
        ).strip().upper()
        _detail = str(
            _selector_failure.get("materialization_detail") or _reason
        ).strip()
        _entry_path = str(
            _selector_failure.get("entry_path")
            or "DEFERRED_BREACH_MATERIALIZATION"
        ).strip().upper()
        try:
            _generation = max(1, int(generation or 1))
            _attempt = max(1, int(attempt or 1))
            _max_attempts = max(_attempt, int(max_attempts or _attempt))
        except (TypeError, ValueError):
            return False
        if (
            not _owner
            or not _reason
            or not _signal_id
            or _execution_mode not in {"live", "paper"}
            or not str(next_retry_at or "").strip()
        ):
            return False
        if (
            _outcome not in {
                "RETRY_LATER_SELECTOR_BUDGET",
                "RETRY_LATER_DATA_UNAVAILABLE",
            }
            or _outcome != _expected_outcome
            or _detail != _reason
            or _entry_path != "DEFERRED_BREACH_MATERIALIZATION"
        ):
            log.critical(
                "[%s] MATERIALIZATION_RETRY_OUTCOME_INVALID | order=%s | "
                "reason=%s outcome=%s detail=%s entry_path=%s",
                self.client_id,
                local_order_id,
                _reason,
                _outcome,
                _detail,
                _entry_path,
            )
            return False

        # Keep the complete selector diagnostics, but make the canonical
        # outcome fields identical at both read surfaces.  Pending-trigger
        # classification and restart recovery read the top-level fields.
        _selector_failure.update({
            "materialization_outcome": _outcome,
            "materialization_detail": _detail,
            "entry_path": _entry_path,
        })

        _now = now_utc_iso()
        _patch = {
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
            "materialization_in_flight": False,
            "materialization_owner": "",
            "materialization_lease_until": "",
            "watcher_token": "",
            "materialization_generation": _generation,
            "retry_reason": _reason,
            "retry_attempt": _attempt,
            "breach_attempt_count": _attempt,
            "materialization_attempts": _attempt,
            "retry_max_attempts": _max_attempts,
            "next_retry_at": str(next_retry_at),
            "materialization_next_retry_at": str(next_retry_at),
            "materialization_reason": _reason,
            "materialization_last_failure_at": _now,
            "retry_owner": _owner,
            "current_owner": _owner,
            "retry_scheduled_at": _now,
            "selector_completed_at": _now,
            "materialization_outcome": _outcome,
            "materialization_detail": _detail,
            "entry_path": _entry_path,
            "materialization_selector_failure": _selector_failure,
            "broker_ready": False,
        }
        if isinstance(selector_recovery_cursor, dict):
            _patch["selector_recovery_cursor_v1"] = selector_recovery_cursor
        try:
            _patch_json = _json_local.dumps(_patch, default=str)
        except Exception:
            return False

        def _schedule():
            with conn() as c:
                cur = c.execute(
                    """
                    UPDATE orders
                    SET meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb,
                        updated_ts = NOW()
                    WHERE local_order_id = %s
                      AND client_id = %s
                      AND signal_id = %s
                      AND LOWER(TRIM(COALESCE(NULLIF(execution_mode, ''), meta->>'execution_mode',''))) = %s
                      AND kind = 'ENTRY'
                      AND UPPER(COALESCE(status,'')) = 'PENDING_TRIGGER'
                      AND (broker_order_id IS NULL OR broker_order_id = '')
                      AND submitted_ts IS NULL
                      AND COALESCE(meta->>'materialization_owner','') = %s
                      AND COALESCE((meta->>'materialization_generation')::int, 0) = %s
                      AND COALESCE(meta->>'lifecycle_state','') = 'MATERIALIZING'
                    """,
                    (
                        _patch_json,
                        local_order_id,
                        self.client_id,
                        _signal_id,
                        _execution_mode,
                        _owner,
                        _generation,
                    ),
                )
                return int(getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0)

        try:
            return bool(run_with_retry(_schedule) > 0)
        except Exception as exc:
            log.warning(
                "[%s] schedule_deferred_materialization_retry failed order=%s: %s",
                self.client_id, local_order_id, exc,
            )
            return False

    def adopt_deferred_retry_watcher(
        self,
        local_order_id: str,
        *,
        watcher_token: str,
        generation: int,
        retry_attempt: int,
        next_retry_at: str,
        execution_mode: str,
    ) -> bool:
        """Durably adopt a RETRY_WAIT row for the current watcher process."""
        import json as _json_local

        _token = str(watcher_token or "").strip()
        _mode = str(execution_mode or "").strip().lower()
        _next = str(next_retry_at or "").strip()
        try:
            _generation = int(generation)
            _attempt = int(retry_attempt)
        except (TypeError, ValueError):
            return False
        if (
            not _token
            or _generation < 1
            or _attempt < 0
            or not _next
            or _mode not in {"live", "paper"}
        ):
            return False

        _now = now_utc_iso()
        _patch = {
            "watcher_token": _token,
            "watcher_generation": _generation,
            "watcher_retry_attempt": _attempt,
            "watcher_registered_at": _now,
            "watcher_next_retry_at": _next,
            "current_owner": _token,
        }
        try:
            _patch_json = _json_local.dumps(_patch, default=str)
        except Exception:
            return False

        def _adopt():
            with conn() as c:
                cur = c.execute(
                    """
                    UPDATE orders
                    SET meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb,
                        updated_ts = NOW()
                    WHERE local_order_id = %s
                      AND client_id = %s
                      AND LOWER(TRIM(COALESCE(execution_mode, ''))) = %s
                      AND kind = 'ENTRY'
                      AND UPPER(COALESCE(status,'')) = 'PENDING_TRIGGER'
                      AND (broker_order_id IS NULL OR broker_order_id = '')
                      AND submitted_ts IS NULL
                      AND NULLIF(COALESCE(meta->>'submit_intent_at', ''), '') IS NULL
                      AND COALESCE(meta->>'lifecycle_state','') = 'RETRY_WAIT'
                      AND COALESCE(meta->>'materialization_status','') = 'RETRY_PENDING'
                      AND COALESCE(meta->>'materialization_in_flight', 'false') = 'false'
                      AND COALESCE(meta->>'broker_ready', 'false') = 'false'
                      AND COALESCE((meta->>'materialization_generation')::int, 0) = %s
                      AND COALESCE((meta->>'retry_attempt')::int, 0) = %s
                      AND COALESCE(meta->>'materialization_next_retry_at', '') = %s
                      AND COALESCE(meta->>'watcher_token', '') = ''
                    """,
                    (
                        _patch_json, local_order_id, self.client_id, _mode,
                        _generation, _attempt, _next,
                    ),
                )
                return int(getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0)

        try:
            return bool(run_with_retry(_adopt) > 0)
        except Exception as exc:
            log.warning(
                "[%s] adopt_deferred_retry_watcher failed order=%s: %s",
                self.client_id, local_order_id, exc,
            )
            return False

    def persist_pre_submit_proof_retry(
        self,
        local_order_id: str,
        *,
        owner: str,
        generation: int,
        retry_attempt: int,
        max_attempts: int,
        next_retry_at: str,
        retry_deadline: str,
        read_error: str,
        selected_at: str,
        selected_quote_at: str,
    ) -> bool:
        """Durably transition a BROKER_READY row to PRE_SUBMIT_PROOF_RETRY state.

        AMENDMENT: PR #323 Seam 1

        When the pre-submit order-row read fails transiently (BLOCK_RETRY verdict),
        the selected OCC contract, limit, qty, and all selector diagnostics are
        already persisted in the row (via persist_deferred_broker_ready).  This
        method writes ONLY the retry-tracking state to meta using the non-destructive
        JSONB merge — none of the trade-policy fields (contract, qty, limit_price,
        reserved_cost, direction, broker_ready) are touched.

        The CAS predicate requires:
          * exact owner + generation match (prevents stale workers from hijacking)
          * lifecycle_state = 'BROKER_READY' (transition FROM broker-ready state)
          * broker_ready = true (selector succeeded)
          * status = 'PENDING_TRIGGER', no broker order yet

        Returns True only on Postgres rowcount > 0.  Returns False on CAS miss,
        validation failure, or write error — caller must treat False as the row
        not transitioning (still BROKER_READY) and either retry or terminalize.
        """
        import json as _json_local

        _owner = str(owner or "").strip()
        try:
            _generation = int(generation or 1)
            _attempt = max(1, int(retry_attempt or 1))
            _max = max(_attempt, int(max_attempts or _attempt))
        except (TypeError, ValueError):
            return False
        if (
            not _owner
            or not str(next_retry_at or "").strip()
            or not str(retry_deadline or "").strip()
        ):
            return False

        _now = now_utc_iso()
        _patch = {
            # Lifecycle transition — still in the PENDING_TRIGGER/BROKER_READY
            # family so watcher and recovery loops handle it correctly.
            "lifecycle_state":              "PRE_SUBMIT_PROOF_RETRY",
            "materialization_status":       "SELECTED",     # contract is still selected
            "broker_ready":                 True,           # contract IS selected; proof retry is the blocker
            # Ownership preserved for CAS on downstream writes.
            "materialization_owner":        _owner,
            "current_owner":                _owner,
            "materialization_generation":   _generation,
            # Retry tracking.
            "proof_retry_attempt":          _attempt,
            "proof_retry_max_attempts":     _max,
            "proof_retry_next_at":          str(next_retry_at),
            "proof_retry_deadline":         str(retry_deadline),
            "absolute_entry_deadline":      str(retry_deadline),
            "proof_retry_last_read_error":  str(read_error or ""),
            "proof_retry_owner":            _owner,
            "proof_retry_scheduled_at":     _now,
            # Temporal diagnostics for Seam 2 timing audit.
            "selected_at":                  str(selected_at or _now),
            "selected_quote_at":            str(selected_quote_at or _now),
        }
        try:
            _patch_json = _json_local.dumps(_patch, default=str)
        except Exception:
            return False

        def _persist():
            with conn() as c:
                cur = c.execute(
                    """
                    UPDATE orders
                    SET meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb,
                        updated_ts = NOW()
                    WHERE local_order_id = %s
                      AND client_id = %s
                      AND kind = 'ENTRY'
                      AND UPPER(COALESCE(status,'')) = 'PENDING_TRIGGER'
                      AND (broker_order_id IS NULL OR broker_order_id = '')
                      AND submitted_ts IS NULL
                      AND COALESCE((meta->>'broker_ready')::boolean, false) = true
                      AND COALESCE(meta->>'lifecycle_state','') = 'BROKER_READY'
                      AND COALESCE(meta->>'materialization_owner','') = %s
                      AND COALESCE((meta->>'materialization_generation')::int, 0) = %s
                    """,
                    (_patch_json, local_order_id, self.client_id, _owner, _generation),
                )
                return int(getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0)

        try:
            return bool(run_with_retry(_persist) > 0)
        except Exception as exc:
            log.warning(
                "[%s] persist_pre_submit_proof_retry failed order=%s: %s",
                self.client_id, local_order_id, exc,
            )
            return False

    def terminalize_materialization_retry(
        self,
        local_order_id: str,
        *,
        reason: str,
        terminal_status: str = "EXPIRED",
        owner: str,
        generation: int,
        retry_attempt: int,
        client_id: str,
        execution_mode: str,
        diagnostics: dict | None = None,
    ) -> bool:
        """Terminalize only the exact in-flight materialization retry claim."""
        import json as _json_local

        _reason = str(reason or "").strip()
        _status = str(terminal_status or "EXPIRED").strip().upper()
        _owner = str(owner or "").strip()
        _client = str(client_id or "").strip().lower()
        _mode = str(execution_mode or "").strip().lower()
        try:
            _generation = int(generation)
            _attempt = int(retry_attempt)
        except (TypeError, ValueError):
            return False
        if (
            not _reason
            or _status not in {"REJECTED", "EXPIRED", "CANCELED", "ERROR"}
            or not _owner
            or _generation < 1
            or _attempt < 1
            or not _client
            or _mode not in {"live", "paper"}
        ):
            return False

        _now = now_utc_iso()
        _patch = dict(diagnostics or {})
        _patch.update({
            "lifecycle_state": _status,
            "materialization_status": "FAILED_TERMINAL",
            "materialization_in_flight": False,
            "materialization_owner": "",
            "current_owner": "",
            "watcher_token": "",
            "materialization_lease_until": "",
            "broker_ready": False,
            "reason_code": _reason,
            "materialization_reason": _reason,
            "final_reason": _reason,
            "materialization_finished_at": _now,
            "selector_completed_at": _now,
            "materialization_retry_terminal_fenced": True,
            "materialization_retry_terminal_owner": _owner,
            "materialization_retry_terminal_generation": _generation,
            "materialization_retry_terminal_attempt": _attempt,
            "selector_recovery_cursor_v1": None,
        })
        try:
            _patch_json = _json_local.dumps(_patch, default=str)
        except Exception:
            return False

        def _terminalize():
            with conn() as c:
                cur = c.execute(
                    """
                    UPDATE orders
                    SET status = %s,
                        last_error = %s,
                        meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb,
                        updated_ts = NOW()
                    WHERE local_order_id = %s
                      AND client_id = %s
                      AND LOWER(TRIM(COALESCE(execution_mode, ''))) = %s
                      AND kind = 'ENTRY'
                      AND UPPER(COALESCE(status,'')) = 'PENDING_TRIGGER'
                      AND (broker_order_id IS NULL OR broker_order_id = '')
                      AND submitted_ts IS NULL
                      AND NULLIF(COALESCE(meta->>'submit_intent_at', ''), '') IS NULL
                      AND COALESCE(meta->>'lifecycle_state','') = 'MATERIALIZING'
                      AND COALESCE(meta->>'materialization_status','') = 'RUNNING'
                      AND COALESCE(meta->>'materialization_in_flight', 'false') = 'true'
                      AND COALESCE(meta->>'materialization_owner','') = %s
                      AND COALESCE((meta->>'materialization_generation')::int, 0) = %s
                      AND COALESCE((meta->>'retry_attempt')::int, 0) = %s
                    """,
                    (
                        _status, _reason, _patch_json,
                        local_order_id, _client, _mode,
                        _owner, _generation, _attempt,
                    ),
                )
                return int(getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0)

        try:
            return bool(run_with_retry(_terminalize) > 0)
        except Exception as exc:
            log.warning(
                "[%s] terminalize_materialization_retry failed order=%s: %s",
                self.client_id, local_order_id, exc,
            )
            return False

    def terminalize_deferred_breach(
        self,
        local_order_id: str,
        *,
        reason_code: str,
        terminal_status: str = "EXPIRED",
        owner: str = "",
        generation: int | None = None,
        diagnostics: dict | None = None,
    ) -> bool:
        """Atomically persist the exact terminal reason and release ownership."""
        import json as _json_local

        _reason = str(reason_code or "").strip()
        _status = str(terminal_status or "EXPIRED").strip().upper()
        if not _reason or _status not in {"REJECTED", "EXPIRED", "CANCELED", "ERROR"}:
            return False
        _now = now_utc_iso()
        _patch = dict(diagnostics or {})
        _patch.update({
            "lifecycle_state": _status,
            "materialization_status": "FAILED_TERMINAL",
            "materialization_in_flight": False,
            "materialization_owner": "",
            "current_owner": "",
            "materialization_lease_until": "",
            "broker_ready": False,
            "reason_code": _reason,
            "materialization_reason": _reason,
            "final_reason": _reason,
            "materialization_finished_at": _now,
            "selector_completed_at": _now,
            "selector_recovery_cursor_v1": None,
        })
        try:
            _patch_json = _json_local.dumps(_patch, default=str)
        except Exception:
            return False

        _where_owner = ""
        _params: list = [_status, _reason, _patch_json, local_order_id, self.client_id]
        if owner and generation is not None:
            _where_owner = (
                " AND COALESCE(meta->>'materialization_owner','') = %s"
                " AND COALESCE((meta->>'materialization_generation')::int, 0) = %s"
            )
            _params.extend([str(owner), int(generation)])

        def _terminalize():
            with conn() as c:
                cur = c.execute(
                    "UPDATE orders SET status=%s, last_error=%s, "
                    "meta=COALESCE(meta, '{}'::jsonb) || %s::jsonb, updated_ts=NOW() "
                    "WHERE local_order_id=%s AND client_id=%s AND kind='ENTRY' "
                    "AND UPPER(COALESCE(status,'')) IN ('CREATED','PENDING_TRIGGER') "
                    "AND (broker_order_id IS NULL OR broker_order_id='') "
                    "AND submitted_ts IS NULL" + _where_owner,
                    tuple(_params),
                )
                return int(getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0)

        try:
            return bool(run_with_retry(_terminalize) > 0)
        except Exception as exc:
            log.warning(
                "[%s] terminalize_deferred_breach failed order=%s: %s",
                self.client_id, local_order_id, exc,
            )
            return False

    def terminalize_deferred_retry_if_unchanged(
        self,
        local_order_id: str,
        *,
        reason_code: str,
        terminal_status: str = "EXPIRED",
        expected_client_id: str,
        expected_execution_mode: str,
        expected_generation: int,
        expected_prior_retry_attempt: int,
        diagnostics: dict | None = None,
    ) -> bool:
        """Fenced terminal CAS for due-retry boundary failures.

        P0 AMENDMENT (fix/deferred-retry-due-execution-p0 — final)
        -----------------------------------------------------------
        This method ONLY succeeds when the durable row is provably still in the
        exact pre-claim RETRY_WAIT state the consumer observed. It refuses to
        write when ANY of these are present in the durable row:

          * generation advanced past expected (concurrent claim winner)
          * retry_attempt advanced past expected_prior (concurrent attempt)
          * lifecycle_state ≠ RETRY_WAIT (concurrent lifecycle advance)
          * materialization_status ≠ RETRY_PENDING
          * broker_ready IS NOT 'false' (concurrent broker-ready advance)
          * materialization_in_flight IS NOT 'false' (concurrent claim)
          * submit_intent_at IS NOT NULL/blank (concurrent submit intent)
          * broker_order_id IS NOT NULL/blank (concurrent broker submission)
          * submitted_ts IS NOT NULL (concurrent submit completion)
          * execution_mode ≠ expected (mode isolation guard)
          * client_id ≠ expected (client isolation guard)

        Boolean fields use text equality ('false') rather than ::boolean casts
        so malformed metadata values (non-'true'/'false' strings) fail closed
        rather than raising a Postgres transaction error and accidentally
        allowing a broad terminalize to proceed.

        Returns True only when rowcount == 1 (exactly one row was updated).
        Returns False on any CAS miss, identity mismatch, or write error.
        """
        import json as _json_local

        _reason = str(reason_code or "").strip()
        _status = str(terminal_status or "EXPIRED").strip().upper()
        _exp_client = str(expected_client_id or "").strip().lower()
        _exp_mode = str(expected_execution_mode or "").strip().lower()
        if (
            not _reason
            or _status not in {"REJECTED", "EXPIRED", "CANCELED", "ERROR"}
            or not _exp_client
            or not _exp_mode
        ):
            return False
        try:
            _exp_gen = int(expected_generation)
            _exp_attempt = int(expected_prior_retry_attempt)
        except (TypeError, ValueError):
            return False
        if _exp_gen < 1 or _exp_attempt < 0:
            return False

        _now = now_utc_iso()
        _patch = {}
        for _k, _v in (diagnostics or {}).items():
            _patch[_k] = _v
        _patch.update({
            "lifecycle_state": _status,
            "materialization_status": "FAILED_TERMINAL",
            "materialization_in_flight": False,
            "materialization_owner": "",
            "current_owner": "",
            "materialization_lease_until": "",
            "broker_ready": False,
            "reason_code": _reason,
            "materialization_reason": _reason,
            "final_reason": _reason,
            "materialization_finished_at": _now,
            "selector_completed_at": _now,
            "retry_terminal_fenced": True,
            "retry_terminal_expected_generation": _exp_gen,
            "retry_terminal_expected_prior_attempt": _exp_attempt,
        })
        try:
            _patch_json = _json_local.dumps(_patch, default=str)
        except Exception:
            return False

        def _fenced_terminal():
            with conn() as c:
                cur = c.execute(
                    """
                    UPDATE orders
                    SET status                 = %s,
                        last_error             = %s,
                        meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb,
                        updated_ts             = NOW()
                    WHERE local_order_id       = %s
                      AND client_id            = %s
                      AND LOWER(TRIM(COALESCE(execution_mode, ''))) = %s
                      AND kind                 = 'ENTRY'
                      AND UPPER(COALESCE(status, '')) = 'PENDING_TRIGGER'
                      AND (broker_order_id IS NULL OR broker_order_id = '')
                      AND submitted_ts IS NULL
                      -- No submit intent in flight
                      AND NULLIF(COALESCE(meta->>'submit_intent_at', ''), '') IS NULL
                      -- Exact lifecycle state proof
                      AND COALESCE(meta->>'lifecycle_state', '') = 'RETRY_WAIT'
                      AND COALESCE(meta->>'materialization_status', '') = 'RETRY_PENDING'
                      -- Exact fencing counters
                      AND COALESCE((meta->>'materialization_generation')::int, 0) = %s
                      AND COALESCE((meta->>'retry_attempt')::int, 0) = %s
                      -- Boolean fields: fail closed on anything other than literal 'false'
                      -- A malformed value like 'truee' is NOT 'false' and blocks the update
                      AND COALESCE(meta->>'broker_ready', 'false') = 'false'
                      AND COALESCE(meta->>'materialization_in_flight', 'false') = 'false'
                    """,
                    (
                        _status, _reason, _patch_json,
                        local_order_id, _exp_client, _exp_mode,
                        _exp_gen, _exp_attempt,
                    ),
                )
                return int(getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0)

        try:
            return bool(run_with_retry(_fenced_terminal) > 0)
        except Exception as exc:
            log.warning(
                "[%s] terminalize_deferred_retry_if_unchanged failed order=%s: %s",
                self.client_id, local_order_id, exc,
            )
            return False

    def claim_pre_submit_proof_retry(
        self,
        local_order_id: str,
        *,
        owner: str,
        expected_generation: int,
        new_generation: int,
        attempt: int,
        claimed_at: str,
    ) -> bool:
        """Fence one due PRE_SUBMIT_PROOF_RETRY worker and restore BROKER_READY."""
        import json as _json_local

        if not owner or expected_generation < 1 or new_generation <= expected_generation:
            return False
        patch = _json_local.dumps({
            "lifecycle_state": "BROKER_READY",
            "materialization_status": "SELECTED",
            "broker_ready": True,
            "materialization_in_flight": False,
            "materialization_owner": owner,
            "current_owner": owner,
            "materialization_generation": new_generation,
            "proof_retry_attempt": attempt,
            "proof_retry_claimed_at": claimed_at,
            "proof_retry_owner": owner,
        })

        def _claim():
            with conn() as c:
                cur = c.execute(
                    """
                    UPDATE orders
                    SET meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb,
                        updated_ts = NOW()
                    WHERE local_order_id = %s
                      AND client_id = %s
                      AND kind = 'ENTRY'
                      AND UPPER(COALESCE(status,'')) = 'PENDING_TRIGGER'
                      AND (broker_order_id IS NULL OR broker_order_id = '')
                      AND submitted_ts IS NULL
                      AND COALESCE(meta->>'lifecycle_state','') = 'PRE_SUBMIT_PROOF_RETRY'
                      AND COALESCE((meta->>'materialization_generation')::int, 0) = %s
                      AND COALESCE((meta->>'proof_retry_attempt')::int, 0) < %s
                      AND NULLIF(meta->>'proof_retry_next_at','')::timestamptz <= NOW()
                    """,
                    (patch, local_order_id, self.client_id, expected_generation, attempt),
                )
                return int(getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0)

        try:
            return bool(run_with_retry(_claim) > 0)
        except Exception as exc:
            log.warning(
                "[%s] claim_pre_submit_proof_retry failed order=%s: %s",
                self.client_id, local_order_id, exc,
            )
            return False

    def record_deferred_hydration_result(
        self,
        local_order_id: str,
        *,
        success: bool,
        status: str | None = None,
        reason: str | None = None,
        contract: str | None = None,
        limit_price: float | None = None,
        qty: int | None = None,
        reserved_cost: float | None = None,
        contract_selection_status: str | None = None,
        selector_audit: dict | None = None,
        hydration_meta: dict | None = None,
    ) -> bool:
        """Persist post-open deferred contract hydration on an existing row."""
        import json as _json_local

        status = str(status or "").strip().upper() or None
        reason = str(reason) if reason else None
        contract_selection_status = (
            str(contract_selection_status)
            if contract_selection_status
            else (
                "HYDRATED_PRE_BREACH"
                if success
                else reason
            )
        )
        deferred_hydration = {
            "attempted": True,
            "success": bool(success),
            "selected_contract": str(contract) if success and contract else None,
            "selected_limit_price": float(limit_price) if success and limit_price is not None else None,
            "qty": int(qty) if success and qty is not None else None,
            "reserved_cost": float(reserved_cost) if success and reserved_cost is not None else None,
            "selector_audit": selector_audit or {},
            "last_attempt_at": now_utc_iso(),
            "failure_reason": None if success else reason,
            "reason": None if success else reason,
        }
        if hydration_meta:
            for key, value in hydration_meta.items():
                if value is not None:
                    deferred_hydration[key] = value

        meta_patch = {
            "contract_deferred": not bool(success),
            "contract_selection_status": contract_selection_status,
            "deferred_hydration": {
                **deferred_hydration,
            }
        }
        if success:
            meta_patch.update({
                "selected_contract": str(contract) if contract else None,
                "contract_symbol": str(contract) if contract else None,
                "limit_price": float(limit_price) if limit_price is not None else None,
                "contracts": int(qty) if qty is not None else None,
                "max_position_usd": float(reserved_cost) if reserved_cost is not None else None,
                "reserved_cost": float(reserved_cost) if reserved_cost is not None else None,
                "contract_materialized_source": "prebreach_hydration",
            })

        try:
            meta_json = _json_local.dumps(meta_patch, default=str)
        except Exception:
            return False

        def _rowcount(cur, c) -> int:
            try:
                return int(getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0)
            except Exception:
                return 0

        def _log_cas_miss() -> None:
            log.info(
                "[%s] DEFERRED_HYDRATION_STALE_SKIP local=%s reason=cas_miss",
                self.client_id,
                local_order_id,
            )

        def _update_with_status_column():
            with conn() as c:
                cur = c.execute(
                    """
                    SELECT status, contract, broker_order_id, submitted_ts
                    FROM orders
                    WHERE local_order_id = %s
                      AND client_id = %s
                    FOR UPDATE
                    """,
                    (local_order_id, self.client_id),
                )
                row = cur.fetchone()
                if not row:
                    return 0
                row = dict(row) if not isinstance(row, dict) else row
                current_status = str(row.get("status") or "").upper()
                current_contract = str(row.get("contract") or "")
                if status and current_status != status:
                    return 0
                if current_status != "PENDING_TRIGGER":
                    return 0
                if row.get("broker_order_id") or row.get("submitted_ts"):
                    return 0
                if not current_contract.upper().startswith("DEFERRED:"):
                    return 0
                if success:
                    cur = c.execute(
                        """
                        UPDATE orders
                        SET contract = %s,
                            limit_price = %s,
                            qty = %s,
                            reserved_cost = %s,
                            contract_selection_status = %s,
                            meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb,
                            updated_ts = NOW()
                        WHERE local_order_id = %s
                          AND client_id = %s
                          AND UPPER(COALESCE(status,'')) = 'PENDING_TRIGGER'
                          AND UPPER(COALESCE(contract,'')) LIKE 'DEFERRED:%%'
                          AND (broker_order_id IS NULL OR broker_order_id = '')
                          AND submitted_ts IS NULL
                          AND (limit_price IS NULL OR limit_price <= 0.01)
                        """,
                        (
                            contract,
                            float(limit_price) if limit_price is not None else None,
                            int(qty) if qty is not None else None,
                            float(reserved_cost) if reserved_cost is not None else None,
                            contract_selection_status,
                            meta_json,
                            local_order_id,
                            self.client_id,
                        ),
                    )
                else:
                    cur = c.execute(
                        """
                        UPDATE orders
                        SET contract_selection_status = %s,
                            meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb,
                            updated_ts = NOW()
                        WHERE local_order_id = %s
                          AND client_id = %s
                          AND UPPER(COALESCE(status,'')) = 'PENDING_TRIGGER'
                          AND UPPER(COALESCE(contract,'')) LIKE 'DEFERRED:%%'
                          AND (broker_order_id IS NULL OR broker_order_id = '')
                          AND submitted_ts IS NULL
                        """,
                        (
                            contract_selection_status,
                            meta_json,
                            local_order_id,
                            self.client_id,
                        ),
                    )
                rowcount = _rowcount(cur, c)
                if rowcount == 0:
                    _log_cas_miss()
                return rowcount

        def _update_without_status_column():
            with conn() as c:
                cur = c.execute(
                    """
                    SELECT status, contract, broker_order_id, submitted_ts
                    FROM orders
                    WHERE local_order_id = %s
                      AND client_id = %s
                    FOR UPDATE
                    """,
                    (local_order_id, self.client_id),
                )
                row = cur.fetchone()
                if not row:
                    return 0
                row = dict(row) if not isinstance(row, dict) else row
                current_status = str(row.get("status") or "").upper()
                current_contract = str(row.get("contract") or "")
                if status and current_status != status:
                    return 0
                if current_status != "PENDING_TRIGGER":
                    return 0
                if row.get("broker_order_id") or row.get("submitted_ts"):
                    return 0
                if not current_contract.upper().startswith("DEFERRED:"):
                    return 0
                if success:
                    cur = c.execute(
                        """
                        UPDATE orders
                        SET contract = %s,
                            limit_price = %s,
                            qty = %s,
                            reserved_cost = %s,
                            meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb,
                            updated_ts = NOW()
                        WHERE local_order_id = %s
                          AND client_id = %s
                          AND UPPER(COALESCE(status,'')) = 'PENDING_TRIGGER'
                          AND UPPER(COALESCE(contract,'')) LIKE 'DEFERRED:%%'
                          AND (broker_order_id IS NULL OR broker_order_id = '')
                          AND submitted_ts IS NULL
                          AND (limit_price IS NULL OR limit_price <= 0.01)
                        """,
                        (
                            contract,
                            float(limit_price) if limit_price is not None else None,
                            int(qty) if qty is not None else None,
                            float(reserved_cost) if reserved_cost is not None else None,
                            meta_json,
                            local_order_id,
                            self.client_id,
                        ),
                    )
                else:
                    cur = c.execute(
                        """
                        UPDATE orders
                        SET meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb,
                            updated_ts = NOW()
                        WHERE local_order_id = %s
                          AND client_id = %s
                          AND UPPER(COALESCE(status,'')) = 'PENDING_TRIGGER'
                          AND UPPER(COALESCE(contract,'')) LIKE 'DEFERRED:%%'
                          AND (broker_order_id IS NULL OR broker_order_id = '')
                          AND submitted_ts IS NULL
                        """,
                        (
                            meta_json,
                            local_order_id,
                            self.client_id,
                        ),
                    )
                rowcount = _rowcount(cur, c)
                if rowcount == 0:
                    _log_cas_miss()
                return rowcount

        try:
            rowcount = run_with_retry(_update_with_status_column)
        except Exception as exc:
            if not (
                pg_errors
                and isinstance(exc, getattr(pg_errors, "UndefinedColumn", tuple()))
            ):
                log.warning(
                    "[%s] record_deferred_hydration_result failed for local_order_id=%s: %s",
                    self.client_id, local_order_id, exc,
                )
                return False
            try:
                rowcount = run_with_retry(_update_without_status_column)
            except Exception as fallback_exc:
                log.warning(
                    "[%s] record_deferred_hydration_result fallback failed for local_order_id=%s: %s",
                    self.client_id, local_order_id, fallback_exc,
                )
                return False

        return bool(rowcount and rowcount > 0)

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

        _raw_meta = current.get("meta") or {}
        if isinstance(_raw_meta, str):
            try:
                _raw_meta = json.loads(_raw_meta)
            except Exception:
                _raw_meta = {}
        if not isinstance(_raw_meta, dict):
            _raw_meta = {}
        _plan_meta = getattr(plan, "metadata", None) if plan is not None else None
        _plan_meta = _plan_meta if isinstance(_plan_meta, dict) else {}
        _recovery_owner = str(_plan_meta.get("recovery_submit_owner") or "").strip()
        _recovery_generation = _plan_meta.get("recovery_submit_generation")
        _is_recovery_submit = bool(
            _plan_meta.get("recovery_submit_fenced") or _recovery_owner
        )
        _quarantined = bool(
            str(current.get("last_error") or "").startswith("SPLIT_BRAIN:")
            or _raw_meta.get("split_brain_quarantine")
            or _raw_meta.get("reconciliation_required")
        )
        if _quarantined:
            return {
                "ok": False,
                "local_order_id": local_order_id,
                "broker_order_id": current.get("broker_order_id"),
                "status": status,
                "error": "ENTRY_SPLIT_BRAIN_QUARANTINED",
                "split_brain": True,
                "reconciliation_required": True,
            }
        # ── Req 3: submitted-like state must have proven broker identity ────────
        # SUBMITTED, ACKNOWLEDGED, PARTIAL_FILL: idempotent success only if
        # broker_order_id is present.  If it is missing, attempt tag lookup to
        # recover the identity without reposting.  If tag lookup also fails,
        # return ENTRY_BROKER_IDENTITY_UNPROVEN — never return ok=True without
        # proven identity and never repost.
        #
        # FILLED: require broker_order_id OR durable fill proof (filled_ts/fill_price).
        # Without either, return identity-unproven for reconciliation.
        if status in (OrderStatus.SUBMITTED, OrderStatus.ACKNOWLEDGED,
                      OrderStatus.PARTIAL_FILL):
            _existing_bid = current.get("broker_order_id")
            if _existing_bid:
                # Idempotent success — broker identity already proven.
                return {"ok": True, "local_order_id": local_order_id,
                        "broker_order_id": _existing_bid,
                        "status": status, "error": None}
            # Missing broker_order_id — attempt recovery via Tradier tag.
            _lu_base_url = (
                getattr(broker, "base_url", None)
                or getattr(getattr(broker, "cfg", None), "base_url", None)
                or ""
            )
            _lu_account = (
                getattr(broker, "account_id", None)
                or getattr(getattr(broker, "cfg", None), "account_id", None)
                or ""
            )
            _recovered_bid = self._lookup_order_by_tag(
                broker, _lu_base_url, _lu_account,
                canonical_broker_submit_key(local_order_id),
            )
            if _recovered_bid:
                _attached = self._attach_broker_identity_if_missing(
                    local_order_id,
                    broker_order_id=_recovered_bid,
                    current_status=status,
                    current_execution_mode=str(current.get("execution_mode") or ""),
                )
                if not _attached:
                    return {
                        "ok": False,
                        "local_order_id": local_order_id,
                        "broker_order_id": _recovered_bid,
                        "status": status,
                        "error": "ENTRY_BROKER_IDENTITY_PERSIST_FAILED",
                        "reconciliation_required": True,
                    }
                log.info(
                    "[%s] ENTRY_BROKER_IDENTITY_RECONCILED_BY_TAG | "
                    "order=%s broker_id=%s status=%s",
                    self.client_id, local_order_id, _recovered_bid, status,
                )
                return {"ok": True, "local_order_id": local_order_id,
                        "broker_order_id": _recovered_bid,
                        "status": status, "error": None,
                        "reconciled_by_tag": True}
            # Tag lookup also found nothing — identity unproven.
            log.critical(
                "[%s] ENTRY_BROKER_IDENTITY_UNPROVEN | order=%s status=%s | "
                "no broker_order_id and no tag match — reconciliation required, "
                "no repost will be issued",
                self.client_id, local_order_id, status,
            )
            return {
                "ok": False,
                "local_order_id": local_order_id,
                "broker_order_id": None,
                "status": status,
                "error": "ENTRY_BROKER_IDENTITY_UNPROVEN",
                "reconciliation_required": True,
            }

        if status == OrderStatus.FILLED:
            _existing_bid = current.get("broker_order_id")
            _fill_proof   = current.get("filled_ts") or current.get("fill_price")
            if _existing_bid or _fill_proof:
                return {"ok": True, "local_order_id": local_order_id,
                        "broker_order_id": _existing_bid,
                        "status": status, "error": None}
            log.critical(
                "[%s] ENTRY_BROKER_IDENTITY_UNPROVEN | order=%s status=FILLED | "
                "no broker_order_id and no fill proof — reconciliation required",
                self.client_id, local_order_id,
            )
            return {
                "ok": False,
                "local_order_id": local_order_id,
                "broker_order_id": None,
                "status": status,
                "error": "ENTRY_BROKER_IDENTITY_UNPROVEN",
                "reconciliation_required": True,
            }

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

        # Any broker ownership evidence that predates this invocation blocks a
        # replacement POST.  The sole exception is the exact unexpired recovery
        # owner carried by this call; persist_deferred_submit_intent() must still
        # atomically transfer that claim before submission.
        _durable_recovery_owner = str(
            _raw_meta.get("recovery_submit_owner") or ""
        ).strip()
        _owned_recovery_transfer = bool(
            _is_recovery_submit
            and _recovery_owner
            and _durable_recovery_owner == _recovery_owner
            and not current.get("broker_order_id")
            and not current.get("submitted_ts")
            and not str(_raw_meta.get("submit_intent_at") or "").strip()
            and str(_raw_meta.get("lifecycle_state") or "").upper() == "BROKER_READY"
        )
        _has_prior_broker_proof = bool(
            current.get("broker_order_id")
            or current.get("submitted_ts")
            or str(_raw_meta.get("submit_intent_at") or "").strip()
            or str(_raw_meta.get("lifecycle_state") or "").upper()
               in {"SUBMITTING", "SUBMITTED"}
            or str(_raw_meta.get("broker_submit_key") or "").strip()
            or str(_raw_meta.get("current_owner") or "").startswith("broker_submit:")
            or (_durable_recovery_owner and not _owned_recovery_transfer)
        )
        if _has_prior_broker_proof:
            _existing_bid = str(current.get("broker_order_id") or "").strip()
            if not _existing_bid:
                _lu_base_url = (
                    getattr(broker, "base_url", None)
                    or getattr(getattr(broker, "cfg", None), "base_url", None)
                    or ""
                )
                _lu_account = (
                    getattr(broker, "account_id", None)
                    or getattr(getattr(broker, "cfg", None), "account_id", None)
                    or ""
                )
                _existing_bid = str(self._lookup_order_by_tag(
                    broker,
                    _lu_base_url,
                    _lu_account,
                    canonical_broker_submit_key(local_order_id),
                ) or "").strip()
            if _existing_bid:
                _reconciled = self.transition(
                    local_order_id,
                    OrderStatus.SUBMITTED,
                    broker_order_id=_existing_bid,
                    submitted_ts=now_utc_iso(),
                )
                if _reconciled:
                    self.update_order_meta(local_order_id, {
                        "lifecycle_state": "SUBMITTED",
                        "submit_completed_at": now_utc_iso(),
                        "broker_order_id": _existing_bid,
                        "current_owner": "broker",
                        "reconciled_by_tag": True,
                    })
                    return {
                        "ok": True,
                        "local_order_id": local_order_id,
                        "broker_order_id": _existing_bid,
                        "status": OrderStatus.SUBMITTED,
                        "error": None,
                        "reconciled_by_tag": True,
                    }
                self._flag_split_brain_order(
                    local_order_id,
                    broker_order_id=_existing_bid,
                    error_msg="ENTRY_PRIOR_SUBMIT_IDENTITY_ATTACH_FAILED",
                    execution_mode=str(current.get("execution_mode") or ""),
                )
            return {
                "ok": False,
                "local_order_id": local_order_id,
                "broker_order_id": _existing_bid or None,
                "status": status,
                "error": (
                    "MATERIALIZATION_SUBMIT_OWNERSHIP_TRANSFERRED"
                    if (
                        _durable_recovery_owner
                        or _raw_meta.get("materialization_generation")
                        or _raw_meta.get("contract_deferred")
                    )
                    else "ENTRY_PRIOR_SUBMIT_PROOF_RECONCILIATION_REQUIRED"
                ),
                "reconciliation_required": True,
            }

        # Durable state is authoritative.  A caller may pass an in-memory plan
        # only as an agreement proof; it may never repair or override the row at
        # submit time.  Deferred copyback must have committed atomically before
        # this method is called.
        lp             = float(current.get("limit_price") or 0)
        _db_contract   = str(current.get("contract") or "").strip()
        _plan_contract = str(getattr(plan, "contract_symbol", "") or "").strip()
        _ticker        = current.get("symbol") or getattr(plan, "ticker", "")
        contract       = _db_contract
        _db_is_deferred = (
            not _db_contract
            or _db_contract.upper().startswith("DEFERRED:")
            or _db_contract.upper() == str(_ticker).upper()
        )

        _is_materialized_deferred = bool(
            _raw_meta.get("contract_deferred")
            or _raw_meta.get("materialization_generation")
            or _raw_meta.get("materialization_entry_path") == "DEFERRED_BREACH_MATERIALIZATION"
        )
        if not _is_materialized_deferred:
            # Preserve the established non-deferred refresh path: its caller may
            # supply a fresh ask, which is fail-closed synced below before POST.
            lp = float(limit_price or current.get("limit_price") or 0)
        if _is_materialized_deferred and (
            _raw_meta.get("broker_ready") is not True
            or str(_raw_meta.get("lifecycle_state") or "") != "BROKER_READY"
        ):
            error_msg = "MATERIALIZATION_DURABLE_STATE_MISMATCH:broker_ready"
            log.critical("[%s] %s order=%s", _ticker, error_msg, local_order_id)
            return {"ok": False, "local_order_id": local_order_id,
                    "broker_order_id": current.get("broker_order_id"),
                    "status": OrderStatus.ERROR, "error": error_msg}

        _plan_qty = int(getattr(plan, "contracts", 0) or 0) if plan is not None else 0
        _db_qty = int(current.get("qty") or 0)
        _requested_limit = float(limit_price or 0)
        _identity_mismatches = []
        if plan is not None:
            if _plan_contract and _plan_contract != _db_contract:
                _identity_mismatches.append("contract")
            if _plan_qty and _plan_qty != _db_qty:
                _identity_mismatches.append("quantity")
            _plan_signal_id = str(getattr(plan, "signal_id", "") or "")
            if _plan_signal_id and _plan_signal_id != str(current.get("signal_id") or ""):
                _identity_mismatches.append("signal_id")
            _plan_client_id = str(getattr(plan, "client_id", "") or "").lower()
            if _plan_client_id and _plan_client_id != str(current.get("client_id") or "").lower():
                _identity_mismatches.append("client_id")
            _plan_mode = str(getattr(plan, "execution_mode", "") or "").lower()
            if _plan_mode and _plan_mode != str(current.get("execution_mode") or "").lower():
                _identity_mismatches.append("execution_mode")
        if (
            _is_materialized_deferred
            and _requested_limit
            and abs(_requested_limit - lp) > 0.001
        ):
            _identity_mismatches.append("execution_price")
        if _identity_mismatches:
            error_msg = "MATERIALIZATION_DURABLE_STATE_MISMATCH:" + ",".join(_identity_mismatches)
            log.critical("[%s] %s order=%s", _ticker, error_msg, local_order_id)
            return {"ok": False, "local_order_id": local_order_id,
                    "broker_order_id": current.get("broker_order_id"),
                    "status": OrderStatus.ERROR, "error": error_msg}

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
        qty      = _db_qty

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

        # ── LIMIT-PRICE DB SYNC (non-deferred / preselected queued orders) ────
        # For DEFERRED orders, _update_contract_pre_submit (above) already wrote
        # the refreshed limit_price alongside the contract + qty update.
        # For preselected / queued orders no DB write has occurred yet — the row
        # still holds the stale selector-time ask from when the order was queued.
        #
        # WHY THIS MATTERS: order_monitor reads orders.limit_price for the repeg
        # baseline. If the DB holds the old stale price while the broker received
        # the refreshed ask-crossed limit, repeg will evaluate proximity against
        # the wrong number (possibly declining when it should fire, or firing with
        # the wrong delta).
        #
        # FAIL-CLOSED: if the sync write fails, block the broker POST. A live
        # broker order paired with a stale DB row is the split-brain condition
        # this guard exists to prevent.
        if not _db_is_deferred:
            _db_stored_lp = float(current.get("limit_price") or 0)
            if abs(_db_stored_lp - lp) > 0.001:
                try:
                    def _sync_limit_price():
                        with conn() as c:
                            c.execute(
                                "UPDATE orders SET limit_price=%s, updated_ts=NOW() "
                                "WHERE local_order_id=%s AND client_id=%s",
                                (round(lp, 2), local_order_id, self.client_id),
                            )
                    run_with_retry(_sync_limit_price)
                    log.info(
                        "[%s] LIMIT_PRICE_SYNCED %.2f → %.2f order=%s (preselected entry)",
                        ticker, _db_stored_lp, lp, local_order_id,
                    )
                except Exception as _lp_exc:
                    _err = f"limit_price_db_sync_failed:{_lp_exc}"
                    log.critical(
                        "[%s] ENTRY_SUBMIT_BLOCKED — limit_price DB sync failed "
                        "(%.2f → %.2f) order=%s | blocking broker POST to prevent "
                        "DB/broker limit drift (order_monitor repeg would read stale price)",
                        ticker, _db_stored_lp, lp, local_order_id,
                    )
                    self.transition(local_order_id, OrderStatus.ERROR, last_error=_err)
                    return {
                        "ok":              False,
                        "local_order_id":  local_order_id,
                        "broker_order_id": current.get("broker_order_id"),
                        "status":          OrderStatus.ERROR,
                        "error":           _err,
                    }

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
        if (
            str(latest.get("contract") or "") != contract
            or int(latest.get("qty") or 0) != qty
            or abs(float(latest.get("limit_price") or 0) - lp) > 0.001
            or str(latest.get("signal_id") or "") != str(current.get("signal_id") or "")
            or str(latest.get("execution_mode") or "").lower()
               != str(current.get("execution_mode") or "").lower()
        ):
            return {
                "ok": False,
                "local_order_id": local_order_id,
                "broker_order_id": latest.get("broker_order_id"),
                "status": OrderStatus.ERROR,
                "error": "MATERIALIZATION_DURABLE_STATE_MISMATCH:stale_read",
            }

        base_url   = (getattr(broker, "base_url", None)
                      or getattr(getattr(broker, "cfg", None), "base_url", None)
                      or "https://sandbox.tradier.com")
        account_id = (getattr(broker, "account_id", None)
                      or getattr(getattr(broker, "cfg", None), "account_id", None)
                      or "")
        error_msg = broker_order_id = None
        _submit_key = canonical_broker_submit_key(local_order_id)
        # Build order payload with Tradier 'tag' for idempotency on retry.
        # tag MUST be the canonical submit key so _lookup_order_by_tag can recover from
        # ambiguous broker responses (read timeout, JSON parse fail) without
        # double-submitting.
        _order_data = {
            "class": "option", "symbol": ticker, "option_symbol": contract,
            "side": "buy_to_open", "quantity": qty,
            "type": "limit", "price": round(lp, 2), "duration": "day",
            "tag": _submit_key,
        }
        # Persist the exact durable submit intent before any broker bytes leave
        # the process.  The Tradier tag is the stable idempotency/reconciliation
        # key for the crash window after POST but before broker_order_id commit.
        _payload_hash = hashlib.sha256(
            json.dumps(_order_data, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        _breach_to_submit_ms = None
        try:
            _breach_at = datetime.fromisoformat(str(
                _raw_meta.get("trigger_crossed_at")
                or _raw_meta.get("trigger_confirmed_at")
            ))
            if _breach_at.tzinfo is None:
                _breach_at = _breach_at.replace(tzinfo=timezone.utc)
            _breach_to_submit_ms = max(
                0, int((datetime.now(timezone.utc) - _breach_at).total_seconds() * 1000)
            )
        except Exception:
            _breach_to_submit_ms = None
        try:
            _materialization_generation = int(_raw_meta.get("materialization_generation") or 0)
        except (TypeError, ValueError):
            _materialization_generation = 0
        if _is_recovery_submit:
            _intent_ok = self.persist_deferred_submit_intent(
                local_order_id,
                owner=_recovery_owner,
                generation=_recovery_generation,
                execution_mode=str(current.get("execution_mode") or ""),
                payload_hash=_payload_hash,
                broker_submit_key=_submit_key,
            )
            _intent_error = "RECOVERY_SUBMIT_INTENT_FENCE_LOST"
        elif _is_materialized_deferred:
            _intent_ok = (
                _materialization_generation > 0
                and self.persist_materialized_submit_intent(
                    local_order_id,
                    generation=_materialization_generation,
                    execution_mode=str(current.get("execution_mode") or ""),
                    signal_id=str(current.get("signal_id") or ""),
                    payload_hash=_payload_hash,
                    broker_submit_key=_submit_key,
                )
            )
            if not _intent_ok:
                _latest_after_claim = self._get_order(local_order_id) or {}
                _latest_meta_after_claim = _latest_after_claim.get("meta") or {}
                if isinstance(_latest_meta_after_claim, str):
                    try:
                        _latest_meta_after_claim = json.loads(_latest_meta_after_claim)
                    except Exception:
                        _latest_meta_after_claim = {}
                if not isinstance(_latest_meta_after_claim, dict):
                    _latest_meta_after_claim = {}
                _latest_status_after_claim = str(_latest_after_claim.get("status") or "").upper()
                if (
                    str(_latest_meta_after_claim.get("submit_intent_at") or "").strip()
                    or str(_latest_meta_after_claim.get("recovery_submit_owner") or "").strip()
                    or str(_latest_meta_after_claim.get("lifecycle_state") or "").upper() == "SUBMITTING"
                    or str(_latest_after_claim.get("broker_order_id") or "").strip()
                    or _latest_after_claim.get("submitted_ts")
                    or _latest_status_after_claim in (
                        OrderStatus.SUBMITTED,
                        OrderStatus.ACKNOWLEDGED,
                        OrderStatus.PARTIAL_FILL,
                        OrderStatus.FILLED,
                    )
                ):
                    _intent_error = "MATERIALIZATION_SUBMIT_OWNERSHIP_TRANSFERRED"
                else:
                    _intent_error = "MATERIALIZATION_STATE_WRITE_FAILED:submit_intent"
            else:
                _intent_error = "MATERIALIZATION_STATE_WRITE_FAILED:submit_intent"
        else:
            _intent_ok = self.persist_entry_submit_intent(
                local_order_id,
                current_status=latest_status,
                execution_mode=str(latest.get("execution_mode") or ""),
                signal_id=str(latest.get("signal_id") or ""),
                contract=contract,
                qty=qty,
                limit_price=lp,
                payload_hash=_payload_hash,
                broker_submit_key=_submit_key,
            )
            _intent_error = "ENTRY_SUBMIT_INTENT_FENCE_LOST"
        if not _intent_ok:
            return {
                "ok": False,
                "local_order_id": local_order_id,
                "broker_order_id": latest.get("broker_order_id"),
                "status": OrderStatus.ERROR,
                "error": _intent_error,
            }

        # Final durable proof immediately before the irreversible network call.
        # This re-read must show exactly this invocation's intent and no broker
        # identity/quarantine evidence.  A CAS success alone is not sufficient.
        _intent_row = self._get_order(local_order_id)
        _intent_row = dict(_intent_row) if _intent_row else {}
        _intent_meta = _intent_row.get("meta") or {}
        if isinstance(_intent_meta, str):
            try:
                _intent_meta = json.loads(_intent_meta)
            except Exception:
                _intent_meta = {}
        if not isinstance(_intent_meta, dict):
            _intent_meta = {}
        _intent_status = str(_intent_row.get("status") or "").upper()
        _intent_quarantined = bool(
            str(_intent_row.get("last_error") or "").startswith("SPLIT_BRAIN:")
            or _intent_meta.get("split_brain_quarantine")
            or _intent_meta.get("reconciliation_required")
        )
        try:
            _intent_qty = int(_intent_row.get("qty") or 0)
            _intent_limit = float(_intent_row.get("limit_price") or 0)
        except (TypeError, ValueError):
            _intent_qty = -1
            _intent_limit = -1.0
        _intent_proven = bool(
            _intent_status in {OrderStatus.CREATED, OrderStatus.PENDING_TRIGGER}
            and str(_intent_row.get("client_id") or "").strip().lower()
                == str(self.client_id or "").strip().lower()
            and str(_intent_row.get("execution_mode") or "").strip().lower()
                == str(latest.get("execution_mode") or "").strip().lower()
            and str(_intent_row.get("signal_id") or "")
                == str(latest.get("signal_id") or "")
            and str(_intent_row.get("contract") or "") == contract
            and _intent_qty == qty
            and abs(_intent_limit - lp) <= 0.001
            and not _intent_row.get("broker_order_id")
            and not _intent_row.get("submitted_ts")
            and str(_intent_meta.get("lifecycle_state") or "").upper() == "SUBMITTING"
            and bool(str(_intent_meta.get("submit_intent_at") or "").strip())
            and str(_intent_meta.get("broker_submit_key") or "") == _submit_key
            and str(_intent_meta.get("broker_submit_payload_hash") or "") == _payload_hash
            and str(_intent_meta.get("current_owner") or "")
                == f"broker_submit:{_submit_key}"
            and not _intent_quarantined
        )
        if not _intent_proven:
            return {
                "ok": False,
                "local_order_id": local_order_id,
                "broker_order_id": _intent_row.get("broker_order_id"),
                "status": _intent_status or OrderStatus.ERROR,
                "error": "ENTRY_SUBMIT_INTENT_DURABLE_PROOF_FAILED",
                "reconciliation_required": True,
            }
        order, error_msg, broker_order_id, broker_status = self._submit_order_with_retry(
            broker=broker, base_url=base_url, account_id=account_id,
            order_data=_order_data, local_id=local_order_id, op_label="existing_entry",
        )
        if error_msg is None:
            if self._is_broker_accept_status(broker_status) and broker_order_id:
                ok = self.transition(local_order_id, OrderStatus.SUBMITTED,
                                     broker_order_id=broker_order_id, submitted_ts=now_utc_iso())
                if ok:
                    self.update_order_meta(local_order_id, {
                        "lifecycle_state": "SUBMITTED",
                        "submit_completed_at": now_utc_iso(),
                        "broker_order_id": str(broker_order_id),
                        "current_owner": "broker",
                        "total_breach_to_submit_ms": _breach_to_submit_ms,
                    })
                    return {"ok": True, "local_order_id": local_order_id,
                            "broker_order_id": broker_order_id,
                            "status": OrderStatus.SUBMITTED, "error": None}
                error_msg = "submitted_transition_failed_after_broker_accept"
                self._flag_split_brain_order(local_order_id, broker_order_id=broker_order_id,
                                             error_msg=error_msg,
                                             execution_mode=str(current.get("execution_mode") or ""))
                return {"ok": False, "local_order_id": local_order_id,
                        "broker_order_id": broker_order_id, "status": OrderStatus.ERROR,
                        "error": error_msg, "split_brain": True}
            elif broker_order_id and not self._is_broker_accept_status(broker_status):
                # ── Req 4: BROKER_STATUS_UNKNOWN_WITH_ID ─────────────────────
                # Broker returned a real broker_order_id with a status string we
                # do not recognise. The ID is evidence of possible broker-side
                # ownership. Preserve it unconditionally via the existing
                # split-brain/quarantine mechanism so reconciliation can resolve it.
                # Never discard a broker ID. Never issue a replacement submit.
                _sb_reason = f"BROKER_STATUS_UNKNOWN_WITH_ID:status={broker_status!r}"
                _quarantine_persisted = self._flag_split_brain_order(
                    local_order_id,
                    broker_order_id=broker_order_id,
                    error_msg=_sb_reason,
                    execution_mode=str(current.get("execution_mode") or ""),
                )
                log.critical(
                    "[%s] BROKER_STATUS_UNKNOWN_WITH_ID | order=%s broker_id=%s "
                    "broker_status=%r | preserving broker identity via split-brain "
                    "quarantine; no replacement submit until reconciliation resolves "
                    "the known broker ID",
                    self.client_id, local_order_id, broker_order_id, broker_status,
                )
                return {
                    "ok": False,
                    "local_order_id": local_order_id,
                    "broker_order_id": broker_order_id,
                    "status": OrderStatus.SUBMITTED if _quarantine_persisted else OrderStatus.ERROR,
                    "error": _sb_reason,
                    "split_brain": True,
                    "reconciliation_required": True,
                    "quarantine_persisted": _quarantine_persisted,
                }
            elif self._is_broker_accept_status(broker_status):
                # A 2xx/accepted response without an identity is still an
                # ownership ambiguity.  Keep the pre-POST submit intent and the
                # row's submit-eligible status fenced for reconciliation; never
                # terminalize it or authorize another POST.
                return {
                    "ok": False,
                    "local_order_id": local_order_id,
                    "broker_order_id": None,
                    "status": _intent_status,
                    "error": "BROKER_ACCEPTED_MISSING_ID_RECONCILIATION_REQUIRED",
                    "identity_quarantine": True,
                    "reconciliation_required": True,
                }
            else:
                error_msg = (f"broker_status:{broker_status or 'unknown'} "
                             f"broker_order_id_missing:{not bool(broker_order_id)}")

        if str(error_msg or "").startswith("BROKER_AMBIGUOUS_"):
            return {
                "ok": False,
                "local_order_id": local_order_id,
                "broker_order_id": None,
                "status": _intent_status,
                "error": error_msg,
                "identity_quarantine": True,
                "reconciliation_required": True,
            }

        # AUDIT-7: include broker rejection reason in observability event so
        # structured audit trail captures WHY Tradier rejected the order.
        self.transition(
            local_order_id,
            OrderStatus.ERROR,
            last_error=error_msg or "unknown_error",
            allow_submit_owner_terminalization=True,
        )
        self.update_order_meta(local_order_id, {
            "lifecycle_state": "ERROR",
            "materialization_in_flight": False,
            "broker_ready": False,
            "reason_code": error_msg or "BROKER_REJECTED_ENTRY",
            "final_reason": error_msg or "BROKER_REJECTED_ENTRY",
            "current_owner": "",
            "submit_completed_at": now_utc_iso(),
        })
        self._emit_transition_event(
            local_order_id=local_order_id,
            old_status=latest_status,
            new_status=OrderStatus.ERROR,
            order=latest,
            decision="ERROR",
            reason_code="BROKER_REJECTED_ENTRY",
            explanation=error_msg or "unknown_error",
            broker_order_id=broker_order_id,
            extra_inputs={"broker_rejection_reason": error_msg or "unknown_error"},
        )
        return {"ok": False, "local_order_id": local_order_id, "broker_order_id": broker_order_id,
                "status": OrderStatus.ERROR, "error": error_msg}

    def _submit_order_with_retry(
        self,
        *,
        broker,
        base_url: str,
        account_id: str,
        order_data: dict,
        local_id: str,
        op_label: str = "entry",
    ):
        """Safe broker order submission with retry + tag-based idempotency.

        Returns (order_dict, error_msg, broker_order_id, status).
        On success: error_msg is None, order_dict contains broker payload.
        On failure: order_dict is None, error_msg describes the terminal failure.

        Used by submit_entry and submit_existing_entry.  submit_exit mirrors
        the same classified boundary:

          RETRYABLE (max 3 attempts, exponential backoff 1s/2s/4s):
            - ConnectTimeout only (no HTTP connection was established)

          AMBIGUOUS (order MAY have landed — tag-lookup before retry):
            - connection reset / ReadTimeout / generic request failure
            - 5xx / 429 / JSON parse failure / unclassified exception
            - clean 2xx without a broker order id

          PERMANENT (no retry):
            - 4xx (not 429): bad symbol, auth failure, etc.

        Pre-condition: order_data["tag"] MUST be set to local_id by caller.
        """
        try:
            import requests as _requests
            _ConnectionError  = _requests.exceptions.ConnectionError
            _ReadTimeout      = _requests.exceptions.ReadTimeout
            _ConnectTimeout   = _requests.exceptions.ConnectTimeout
            _RequestException = _requests.exceptions.RequestException
        except Exception:
            _ConnectionError = _ReadTimeout = _ConnectTimeout = _RequestException = Exception

        _max_attempts   = 3
        _backoff_base_s = 1.0
        _post_url       = f"{base_url}/v1/accounts/{account_id}/orders"
        error_msg       = None
        order           = {}
        broker_order_id = None
        status          = ""

        for _attempt in range(1, _max_attempts + 1):
            try:
                resp = broker.session.post(
                    _post_url,
                    data=order_data,
                    headers={"Accept": "application/json"},
                    timeout=10,
                )
                _sc = getattr(resp, "status_code", None)

                if _sc is not None and 400 <= _sc < 500 and _sc != 429:
                    # PERMANENT REJECT — do not retry
                    try:
                        _body_snip = (resp.text or "")[:200]
                    except Exception:
                        _body_snip = "<body unreadable>"
                    error_msg = f"broker_http_{_sc}:{_body_snip}"
                    log.error(
                        "[%s] submit_%s PERMANENT REJECT | local=%s http=%s body=%s",
                        self.client_id, op_label, local_id, _sc, _body_snip,
                    )
                    return None, error_msg, None, ""

                if _sc is not None and (_sc >= 500 or _sc == 429):
                    _existing_oid = self._lookup_order_by_tag(
                        broker, base_url, account_id,
                        canonical_broker_submit_key(local_id),
                    )
                    if _existing_oid:
                        return ({"id": _existing_oid, "status": "open"}, None,
                                _existing_oid, "open")
                    return (
                        None,
                        f"BROKER_AMBIGUOUS_HTTP_{_sc}_RECONCILIATION_REQUIRED",
                        None,
                        "",
                    )

                # 2xx — parse JSON
                try:
                    order = (resp.json() or {}).get("order") or {}
                except Exception as _je:
                    # AMBIGUOUS — 2xx but unparseable. Order may have landed.
                    log.warning(
                        "[%s] submit_%s JSON parse failed | local=%s attempt=%d err=%s",
                        self.client_id, op_label, local_id, _attempt, _je,
                    )
                    _existing_oid = self._lookup_order_by_tag(
                        broker, base_url, account_id,
                        canonical_broker_submit_key(local_id),
                    )
                    if _existing_oid:
                        log.info(
                            "[%s] submit_%s AMBIGUOUS but found by tag | "
                            "local=%s broker_order_id=%s — treating as success",
                            self.client_id, op_label, local_id, _existing_oid,
                        )
                        order = {"id": _existing_oid, "status": "open"}
                    else:
                        error_msg = (
                            "BROKER_AMBIGUOUS_JSON_RECONCILIATION_REQUIRED:"
                            f"{_je}"
                        )
                        return None, error_msg, None, ""

                status          = str(order.get("status") or "").lower().strip()
                broker_order_id = order.get("id") or order.get("order_id")
                if (
                    not broker_order_id
                    and status not in {"rejected", "canceled", "cancelled", "expired"}
                ):
                    _existing_oid = self._lookup_order_by_tag(
                        broker, base_url, account_id,
                        canonical_broker_submit_key(local_id),
                    )
                    if _existing_oid:
                        return ({"id": _existing_oid, "status": "open"}, None,
                                _existing_oid, "open")
                    return (
                        None,
                        "BROKER_AMBIGUOUS_2XX_MISSING_ID_RECONCILIATION_REQUIRED:"
                        f"status={status or 'empty'}",
                        None,
                        status,
                    )
                return order, None, broker_order_id, status

            except _ConnectTimeout as _ce:
                # A connect-phase timeout occurs before an HTTP response is
                # established and remains retryable.
                error_msg = f"broker_conn_error:{_ce}"
                log.warning(
                    "[%s] submit_%s conn error | local=%s attempt=%d/%d err=%s",
                    self.client_id, op_label, local_id, _attempt, _max_attempts, _ce,
                )
                if _attempt < _max_attempts:
                    _time_module.sleep(_backoff_base_s * (2 ** (_attempt - 1)))
                    continue
                return None, error_msg, None, ""

            except _ConnectionError as _ce:
                # ConnectionError also covers resets after request bytes were
                # sent.  Treat it as ambiguous unless the exact broker tag is
                # found; never issue a blind replacement POST.
                _existing_oid = self._lookup_order_by_tag(
                    broker, base_url, account_id,
                    canonical_broker_submit_key(local_id),
                )
                if _existing_oid:
                    return ({"id": _existing_oid, "status": "open"}, None,
                            _existing_oid, "open")
                return (
                    None,
                    "BROKER_AMBIGUOUS_CONNECTION_RECONCILIATION_REQUIRED:"
                    f"{_ce}",
                    None,
                    "",
                )

            except _ReadTimeout as _rt:
                # AMBIGUOUS — Tradier got it, response lost. Query by tag.
                log.warning(
                    "[%s] submit_%s READ TIMEOUT (ambiguous) | local=%s attempt=%d err=%s",
                    self.client_id, op_label, local_id, _attempt, _rt,
                )
                _existing_oid = self._lookup_order_by_tag(
                    broker, base_url, account_id,
                    canonical_broker_submit_key(local_id),
                )
                if _existing_oid:
                    log.info(
                        "[%s] submit_%s RECOVERED from read timeout via tag | "
                        "local=%s broker_order_id=%s",
                        self.client_id, op_label, local_id, _existing_oid,
                    )
                    return ({"id": _existing_oid, "status": "open"}, None,
                            _existing_oid, "open")
                error_msg = (
                    "BROKER_AMBIGUOUS_READ_TIMEOUT_RECONCILIATION_REQUIRED:"
                    f"{_rt}"
                )
                return None, error_msg, None, ""

            except _RequestException as _re:
                _existing_oid = self._lookup_order_by_tag(
                    broker, base_url, account_id,
                    canonical_broker_submit_key(local_id),
                )
                if _existing_oid:
                    return ({"id": _existing_oid, "status": "open"}, None,
                            _existing_oid, "open")
                return (
                    None,
                    "BROKER_AMBIGUOUS_REQUEST_RECONCILIATION_REQUIRED:"
                    f"{_re}",
                    None,
                    "",
                )

            except Exception as _e:
                # The exception happened at or after the POST boundary.  Unless
                # exact tag recovery proves ownership, preserve the durable
                # submit intent and require reconciliation; never authorize a
                # blind replacement POST.
                log.error(
                    "[%s] submit_%s unexpected error | local=%s attempt=%d err=%s",
                    self.client_id, op_label, local_id, _attempt, _e,
                )
                _existing_oid = self._lookup_order_by_tag(
                    broker, base_url, account_id,
                    canonical_broker_submit_key(local_id),
                )
                if _existing_oid:
                    return ({"id": _existing_oid, "status": "open"}, None,
                            _existing_oid, "open")
                return (
                    None,
                    "BROKER_AMBIGUOUS_UNEXPECTED_RECONCILIATION_REQUIRED:"
                    f"{_e}",
                    None,
                    "",
                )

        # Loop fell through without explicit return — defensive
        return None, error_msg or "submit_exhausted_no_return", None, ""

    def submit_entry(
        self,
        *,
        broker,
        plan,
        limit_price=None,
        reserved_cost=None,
    ) -> dict:
        local_id = self.create_entry_order(
            plan,
            limit_price=limit_price,
            reserved_cost=reserved_cost,
            execution_mode=(
                getattr(plan, "execution_mode", None)
                or getattr(plan, "mode", None)
            ),
        )
        lp = float(limit_price or getattr(plan, "limit_price", 0) or 0)
        if lp <= 0:
            error_msg = "invalid_entry_limit_price"
            self.transition(local_id, OrderStatus.ERROR, last_error=error_msg)
            return {"ok": False, "local_order_id": local_id, "broker_order_id": None,
                    "status": OrderStatus.ERROR, "error": error_msg}
        # One entry boundary owns intent persistence, durable re-read, broker
        # identity recovery, and quarantine.  Direct entries must not maintain a
        # second implementation that can drift into a duplicate-POST path.
        return self.submit_existing_entry(
            local_order_id=local_id,
            broker=broker,
            plan=plan,
            limit_price=lp,
        )

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
        execution_mode: str | None = None,
        local_order_id: str | None = None,
    ) -> dict:
        if not execution_mode:
            try:
                from ap.authorization import execution_mode_for_broker
                execution_mode = execution_mode_for_broker(broker)
            except Exception:
                execution_mode = None

        reserved_local_id = str(local_order_id or "").strip()
        existing = self._get_active_exit_order(position_id)
        if existing:
            existing  = dict(existing)
            if reserved_local_id and str(existing.get("local_order_id") or "").strip() == reserved_local_id:
                local_id = reserved_local_id
            else:
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
        else:
            local_id = self.create_exit_order(
                position_id=position_id, contract=contract, symbol=symbol,
                direction=direction, qty=qty, plan_id=plan_id, signal_id=signal_id,
                limit_price=limit_price, local_order_id=reserved_local_id or None,
                execution_mode=execution_mode,
            )
        requested_qty = int(qty or 0)
        broker_truth = resolve_exit_broker_truth(
            broker=broker,
            client_id=self.client_id,
            contract=str(contract or ""),
        )
        broker_truth_qty = broker_truth.get("broker_truth_open_qty")
        broker_truth_audit = dict((broker_truth.get("audit") or {}))
        if broker_truth_audit:
            broker_truth_audit["requested_qty"] = requested_qty
            _upd_bt = getattr(self, "update_order_meta", None)
            if callable(_upd_bt):
                try:
                    _upd_bt(local_id, {"exit_safety": {"broker_truth": broker_truth_audit}})
                except Exception as _upd_bt_exc:
                    log.debug("submit_exit broker_truth audit write failed: %s", _upd_bt_exc)
        if broker_truth.get("is_fresh_exact") and int(broker_truth_qty or 0) == 0:
            blocked_reason = "SYNTHETIC_POSITION_STALE_BROKER_FLAT"
            broker_truth_audit.update(
                {
                    "result": blocked_reason,
                    "broker_truth_open_qty": 0,
                    "manual_close_needed": True,
                }
            )
            _upd_bt = getattr(self, "update_order_meta", None)
            if callable(_upd_bt):
                try:
                    _upd_bt(local_id, {"exit_safety": {"broker_truth": broker_truth_audit}})
                except Exception as _upd_bt_exc:
                    log.debug("submit_exit broker_truth audit write failed: %s", _upd_bt_exc)
            log.warning(
                "[%s] %s position_id=%s contract=%s requested_qty=%s account=%s | broker flat on fresh exact snapshot",
                self.client_id,
                blocked_reason,
                position_id,
                contract,
                requested_qty,
                broker_truth_audit.get("account") or "",
            )
            try:
                self.transition(local_id, OrderStatus.CANCELED, last_error=blocked_reason)
            except Exception as exc:
                log.debug("[%s] exit flat-truth block cancel transition failed for %s: %s", self.client_id, local_id, exc)
            try:
                with conn() as _stale_c:
                    _stale_c.execute(
                        """
                        UPDATE positions
                        SET status = 'CLOSED',
                            meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb
                        WHERE id = %s
                          AND client_id = %s
                          AND status NOT IN ('CLOSED', 'EXPIRED')
                        """,
                        (
                            __import__("json").dumps({
                                "synthetic_position_stale_broker_flat": True,
                                "stale_marked_at": __import__("datetime").datetime.now(
                                    __import__("datetime").timezone.utc
                                ).isoformat(),
                                "broker_truth_open_qty": 0,
                                "exit_circuit_breaker_broker_truth": broker_truth_audit,
                                "reconciler_manual_close_needed": True,
                            }),
                            str(position_id),
                            self.client_id,
                        ),
                    )
            except Exception as exc:
                log.warning("[%s] failed to mark flat broker-truth position %s stale: %s", self.client_id, position_id, exc)
            return {
                "ok": False,
                "local_order_id": local_id,
                "broker_order_id": None,
                "status": OrderStatus.CANCELED,
                "error": blocked_reason,
                "skipped": True,
                "reason": blocked_reason,
            }
        if (
            broker_truth.get("is_fresh_exact")
            and broker_truth_qty is not None
            and int(broker_truth_qty) > 0
            and requested_qty > int(broker_truth_qty)
        ):
            blocked_reason = "EXIT_BLOCKED_BROKER_QTY_INSUFFICIENT"
            broker_truth_audit.update(
                {
                    "result": blocked_reason,
                    "broker_truth_open_qty": int(broker_truth_qty),
                }
            )
            _upd_bt = getattr(self, "update_order_meta", None)
            if callable(_upd_bt):
                try:
                    _upd_bt(local_id, {"exit_safety": {"broker_truth": broker_truth_audit}})
                except Exception as _upd_bt_exc:
                    log.debug("submit_exit broker_truth audit write failed: %s", _upd_bt_exc)
            log.warning(
                "[%s] %s position_id=%s contract=%s requested_qty=%s broker_truth_open_qty=%s account=%s",
                self.client_id,
                blocked_reason,
                position_id,
                contract,
                requested_qty,
                broker_truth_qty,
                broker_truth_audit.get("account") or "",
            )
            try:
                self.transition(local_id, OrderStatus.CANCELED, last_error=blocked_reason)
            except Exception as exc:
                log.debug("[%s] exit oversell block cancel transition failed for %s: %s", self.client_id, local_id, exc)
            return {
                "ok": False,
                "local_order_id": local_id,
                "broker_order_id": None,
                "status": OrderStatus.CANCELED,
                "error": blocked_reason,
                "skipped": True,
                "reason": blocked_reason,
            }
        safety = evaluate_exit_submission_safety(
            position_id=str(position_id),
            client_id=self.client_id,
            execution_mode=execution_mode,
            contract=str(contract or ""),
            broker_truth_open_qty=broker_truth_qty,
            allow_missing_position_with_broker_truth=str(position_id or "").startswith("broker-repair-"),
        )
        # ── P0 (PR #307): log circuit breaker override before broker POST ───
        # When broker truth confirms open qty > 0, _should_halt_exit_after_rejections
        # returns blocked=False with reason=PROTECTIVE_EXIT_ALLOWED_BY_BROKER_TRUTH.
        # The submit proceeds normally but we stamp the audit trail so operators
        # can see the override happened and why.
        _cb_override = (
            isinstance(safety, dict)
            and (safety.get("circuit_breaker") or {}).get("reason") == "PROTECTIVE_EXIT_ALLOWED_BY_BROKER_TRUTH"
        )
        if _cb_override:
            _cb = safety.get("circuit_breaker") or {}
            broker_truth_audit.update(
                {
                    "result": "PROTECTIVE_EXIT_ALLOWED_BY_BROKER_TRUTH",
                    "broker_truth_open_qty": _cb.get("broker_truth_open_qty"),
                }
            )
            _upd_bt = getattr(self, "update_order_meta", None)
            if callable(_upd_bt):
                try:
                    _upd_bt(local_id, {"exit_safety": {"broker_truth": broker_truth_audit}})
                except Exception as _upd_bt_exc:
                    log.debug("submit_exit broker_truth audit write failed: %s", _upd_bt_exc)
            log.warning(
                "[%s] PROTECTIVE_EXIT_ALLOWED_BY_BROKER_TRUTH "
                "local_order_id=%s position_id=%s contract=%s execution_mode=%s "
                "broker_truth_open_qty=%s rejection_count=%s threshold=%s — "
                "proceeding to broker submit despite prior rejections",
                self.client_id,
                local_id,       # local_order_id preserved in audit
                position_id,
                contract,
                str(execution_mode or "").strip().lower(),
                _cb.get("broker_truth_open_qty"),
                _cb.get("rejection_count"),
                _cb.get("threshold"),
            )
        if safety.get("blocked"):
            blocked_reason = str(safety.get("reason") or "exit_submission_blocked")
            log.warning(
                "[%s] EXIT submit blocked before broker call | position_id=%s client_id=%s execution_mode=%s contract=%s reason=%s",
                self.client_id,
                position_id,
                self.client_id,
                str(execution_mode or "").strip().lower(),
                contract,
                blocked_reason,
            )
            try:
                self.transition(local_id, OrderStatus.CANCELED, last_error=blocked_reason)
            except Exception as exc:
                log.debug("[%s] exit block cancel transition failed for %s: %s", self.client_id, local_id, exc)

            position_state = (safety.get("position_state") or {}) if isinstance(safety, dict) else {}
            if position_state.get("blocked"):
                try:
                    _ee = _get_exit_engine_for_client(self.client_id)
                    if _ee and hasattr(_ee, "clear_exit_in_flight"):
                        self._call_exit_engine(
                            _ee,
                            "clear_exit_in_flight",
                            str(position_id),
                            reason=blocked_reason,
                        )
                except Exception as exc:
                    log.debug("[%s] clear_exit_in_flight on terminal exit block failed: %s", self.client_id, exc)

            breaker = (safety.get("circuit_breaker") or {}) if isinstance(safety, dict) else {}
            if blocked_reason == "exit_circuit_breaker_tripped":
                try:
                    alert_exit_submission_halted(
                        client_id=self.client_id,
                        execution_mode=execution_mode,
                        position_id=str(position_id),
                        contract=str(contract or ""),
                        reason=blocked_reason,
                        rejection_count=breaker.get("rejection_count"),
                        threshold=breaker.get("threshold"),
                    )
                except Exception as exc:
                    log.warning("[%s] exit circuit-breaker alert failed: %s", self.client_id, exc)

            # ── P0 (PR #307): new reason codes from broker-truth override ────
            # SYNTHETIC_POSITION_STALE_BROKER_FLAT: broker says qty=0 but the
            # local position thinks it's still open. Stop repeated exit firing
            # by marking the local position stale so the exit engine stops
            # evaluating it on every tick.
            # ── Req 5: synthetic-flat block — ALL writes nested under the exact condition ──
            # The broker-truth metadata update, warning log, and positions CLOSED
            # mutation must execute ONLY when:
            #   1. blocked_reason == "SYNTHETIC_POSITION_STALE_BROKER_FLAT"
            #   2. broker_truth.get("is_fresh_exact") is True
            #   3. int(broker_truth.get("broker_truth_open_qty") or 0) == 0
            # Any other blocked reason must NOT mark the position CLOSED.
            if blocked_reason == "SYNTHETIC_POSITION_STALE_BROKER_FLAT":
                broker_truth_audit.update(
                    {
                        "result": blocked_reason,
                        "broker_truth_open_qty": 0,
                        "manual_close_needed": True,
                    }
                )
                _upd_bt = getattr(self, "update_order_meta", None)
                if callable(_upd_bt):
                    try:
                        _upd_bt(local_id, {"exit_safety": {"broker_truth": broker_truth_audit}})
                    except Exception as _upd_bt_exc:
                        log.debug("submit_exit broker_truth audit write failed: %s", _upd_bt_exc)
                log.warning(
                    "[%s] SYNTHETIC_POSITION_STALE_BROKER_FLAT "
                    "position_id=%s contract=%s — broker is flat; "
                    "marking local position stale to stop repeated exit firing",
                    self.client_id, position_id, contract,
                )
                # ── Triple-condition guard on the CLOSED write ────────────────
                # Only mark the position CLOSED when all three are true.
                # Missing, non-exact, or non-zero broker truth must not close it.
                if (
                    broker_truth.get("is_fresh_exact") is True
                    and int(broker_truth.get("broker_truth_open_qty") or 0) == 0
                ):
                    try:
                        with conn() as _stale_c:
                            _stale_c.execute(
                                """
                                UPDATE positions
                                SET status = 'CLOSED',
                                    meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb
                                WHERE id = %s
                                  AND client_id = %s
                                  AND status NOT IN ('CLOSED', 'EXPIRED')
                                """,
                                (
                                    __import__("json").dumps({
                                        "synthetic_position_stale_broker_flat": True,
                                        "stale_marked_at": __import__("datetime").datetime.now(
                                            __import__("datetime").timezone.utc
                                        ).isoformat(),
                                        "broker_truth_open_qty": 0,
                                        "exit_circuit_breaker_broker_truth": broker_truth_audit,
                                        "reconciler_manual_close_needed": True,
                                    }),
                                    str(position_id),
                                    self.client_id,
                                ),
                            )
                        log.info(
                            "[%s] position %s marked CLOSED (broker flat, fresh exact broker truth, qty=0) | reconciler/manual-close-needed",
                            self.client_id, position_id,
                        )
                    except Exception as _stale_exc:
                        log.warning(
                            "[%s] failed to mark position %s stale: %s",
                            self.client_id, position_id, _stale_exc,
                        )

            return {
                "ok": False,
                "local_order_id": local_id,
                "broker_order_id": None,
                "status": OrderStatus.CANCELED,
                "error": blocked_reason,
                "skipped": True,
                "reason": blocked_reason,
            }

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
        # ── Build order payload ONCE ──────────────────────────────────────────
        _is_market_payload = (order_type == "market") or (lp <= 0 and order_type != "limit")
        _order_data = {
            "class": "option", "symbol": underlying, "option_symbol": contract,
            "side": "sell_to_close", "quantity": int(qty),
            "type": "market" if _is_market_payload else "limit",
            "duration": "day",
        }
        if not _is_market_payload:
            _order_data["price"] = round(lp, 2)
        # Tradier accepts a 'tag' field — using local_id provides client-side
        # idempotency. If we retry on ambiguous response, we can find this tag
        # in /orders to confirm the order landed without double-submitting.
        _order_data["tag"] = canonical_broker_submit_key(local_id)

        # ── Retry-safe broker submission ──────────────────────────────────────
        # Failure classes:
        #   RETRYABLE_CONNECT_TIMEOUT — no HTTP connection was established
        #   AMBIGUOUS       — read timeout / mid-response failure — order MAY
        #                     have landed. Query Tradier once; absent exact tag
        #                     proof, hold for reconciliation and never re-POST.
        #   PERMANENT_REJECT — 4xx (not 429) — broker rejected, do not retry
        #   BROKER_REJECTED — Tradier returned rejected status, do not retry
        try:
            import requests as _requests
            _RetryableHTTPError    = _requests.exceptions.HTTPError
            _ConnectionError       = _requests.exceptions.ConnectionError
            _ReadTimeout           = _requests.exceptions.ReadTimeout
            _ConnectTimeout        = _requests.exceptions.ConnectTimeout
            _RequestException      = _requests.exceptions.RequestException
        except Exception:
            # If requests isn't importable in this context, fall through to
            # generic exception handling (preserves prior behavior).
            _RetryableHTTPError = _ConnectionError = _ReadTimeout = \
                _ConnectTimeout = _RequestException = Exception

        _max_attempts   = 3
        _backoff_base_s = 1.0   # 1s, 2s, 4s
        _post_url       = f"{base_url}/v1/accounts/{account_id}/orders"
        resp = None
        order = {}
        status = ""

        for _attempt in range(1, _max_attempts + 1):
            # Re-check active exit on EACH attempt — a concurrent submission
            # or a successful prior attempt that we couldn't confirm could
            # have created one. Never double-submit.
            if _attempt > 1:
                _existing = self._get_active_exit_order(position_id)
                if _existing and str(_existing.get("local_order_id")) != str(local_id):
                    log.warning(
                        "[%s] submit_exit retry %d aborted — another active exit "
                        "appeared pos=%s existing=%s",
                        self.client_id, _attempt, position_id,
                        _existing.get("local_order_id"),
                    )
                    error_msg = (
                        f"retry_aborted_concurrent_exit:"
                        f"{_existing.get('local_order_id')}:{_existing.get('status')}"
                    )
                    break

            try:
                resp = broker.session.post(
                    _post_url,
                    data=_order_data,
                    headers={"Accept": "application/json"},
                    timeout=10,
                )
                # Classify by HTTP status before parsing JSON
                _sc = getattr(resp, "status_code", None)
                if _sc is not None and 400 <= _sc < 500 and _sc != 429:
                    # PERMANENT_REJECT — do not retry
                    _body_snip = ""
                    try:
                        _body_snip = (resp.text or "")[:200]
                    except Exception:
                        _body_snip = "<body unreadable>"
                    error_msg = f"broker_http_{_sc}:{_body_snip}"
                    log.error(
                        "[%s] submit_exit PERMANENT REJECT | pos=%s contract=%s "
                        "http=%s body=%s",
                        self.client_id, position_id, contract, _sc, _body_snip,
                    )
                    break

                if _sc is not None and (_sc >= 500 or _sc == 429):
                    _existing_oid = self._lookup_order_by_tag(
                        broker, base_url, account_id,
                        canonical_broker_submit_key(local_id),
                    )
                    if _existing_oid:
                        order = {"id": _existing_oid, "status": "open"}
                        broker_order_id = _existing_oid
                        status = "open"
                        error_msg = None
                    else:
                        error_msg = (
                            f"BROKER_AMBIGUOUS_HTTP_{_sc}_RECONCILIATION_REQUIRED"
                        )
                    break

                # 2xx — parse JSON
                try:
                    order = (resp.json() or {}).get("order") or {}
                except Exception as _je:
                    # AMBIGUOUS — got 2xx but can't parse. Order may or may
                    # not have landed. Check by tag before retrying.
                    log.warning(
                        "[%s] submit_exit JSON parse failed | pos=%s attempt=%d err=%s",
                        self.client_id, position_id, _attempt, _je,
                    )
                    _existing_oid = self._lookup_order_by_tag(
                        broker, base_url, account_id,
                        canonical_broker_submit_key(local_id),
                    )
                    if _existing_oid:
                        log.info(
                            "[%s] submit_exit AMBIGUOUS but found by tag | "
                            "pos=%s broker_order_id=%s — treating as success",
                            self.client_id, position_id, _existing_oid,
                        )
                        order = {"id": _existing_oid, "status": "open"}
                    else:
                        error_msg = (
                            "BROKER_AMBIGUOUS_JSON_RECONCILIATION_REQUIRED:"
                            f"{_je}"
                        )
                        break

                status          = str(order.get("status") or "").lower().strip()
                broker_order_id = order.get("id") or order.get("order_id")
                if (
                    not broker_order_id
                    and status not in {"rejected", "canceled", "cancelled", "expired"}
                ):
                    _existing_oid = self._lookup_order_by_tag(
                        broker, base_url, account_id,
                        canonical_broker_submit_key(local_id),
                    )
                    if _existing_oid:
                        order = {"id": _existing_oid, "status": "open"}
                        broker_order_id = _existing_oid
                        status = "open"
                        error_msg = None
                    else:
                        error_msg = (
                            "BROKER_AMBIGUOUS_2XX_MISSING_ID_RECONCILIATION_REQUIRED:"
                            f"status={status or 'empty'}"
                        )
                    break
                # Got a clean response — exit retry loop
                error_msg = None
                break

            except _ConnectTimeout as _ce:
                # Connect-phase timeout is the only connection class we can
                # safely retry without broker identity proof.
                error_msg = f"broker_conn_error:{_ce}"
                log.warning(
                    "[%s] submit_exit conn error | pos=%s attempt=%d/%d err=%s",
                    self.client_id, position_id, _attempt, _max_attempts, _ce,
                )
                if _attempt < _max_attempts:
                    _time_module.sleep(_backoff_base_s * (2 ** (_attempt - 1)))
                    continue
                break

            except _ConnectionError as _ce:
                _existing_oid = self._lookup_order_by_tag(
                    broker, base_url, account_id,
                    canonical_broker_submit_key(local_id),
                )
                if _existing_oid:
                    order = {"id": _existing_oid, "status": "open"}
                    broker_order_id = _existing_oid
                    status = "open"
                    error_msg = None
                else:
                    error_msg = (
                        "BROKER_AMBIGUOUS_CONNECTION_RECONCILIATION_REQUIRED:"
                        f"{_ce}"
                    )
                break

            except _ReadTimeout as _rt:
                # AMBIGUOUS — Tradier received the request, response was lost.
                # Order MAY have landed. Query by tag before any retry.
                log.warning(
                    "[%s] submit_exit READ TIMEOUT (ambiguous) | pos=%s attempt=%d err=%s",
                    self.client_id, position_id, _attempt, _rt,
                )
                _existing_oid = self._lookup_order_by_tag(
                    broker, base_url, account_id,
                    canonical_broker_submit_key(local_id),
                )
                if _existing_oid:
                    log.info(
                        "[%s] submit_exit RECOVERED from read timeout via tag | "
                        "pos=%s broker_order_id=%s",
                        self.client_id, position_id, _existing_oid,
                    )
                    order = {"id": _existing_oid, "status": "open"}
                    broker_order_id = _existing_oid
                    status = "open"
                    error_msg = None
                    break
                error_msg = (
                    "BROKER_AMBIGUOUS_READ_TIMEOUT_RECONCILIATION_REQUIRED:"
                    f"{_rt}"
                )
                break

            except _RequestException as _re:
                _existing_oid = self._lookup_order_by_tag(
                    broker, base_url, account_id,
                    canonical_broker_submit_key(local_id),
                )
                if _existing_oid:
                    order = {"id": _existing_oid, "status": "open"}
                    broker_order_id = _existing_oid
                    status = "open"
                    error_msg = None
                else:
                    error_msg = (
                        "BROKER_AMBIGUOUS_REQUEST_RECONCILIATION_REQUIRED:"
                        f"{_re}"
                    )
                break

            except Exception as _e:
                # Unknown failures at/after POST are ownership-ambiguous.  A
                # missing lookup result is not proof that the broker rejected
                # the order, so keep EXIT_REQUESTED fenced for reconciliation.
                log.error(
                    "[%s] submit_exit unexpected error | pos=%s attempt=%d err=%s",
                    self.client_id, position_id, _attempt, _e,
                )
                _existing_oid = self._lookup_order_by_tag(
                    broker, base_url, account_id,
                    canonical_broker_submit_key(local_id),
                )
                if _existing_oid:
                    order = {"id": _existing_oid, "status": "open"}
                    broker_order_id = _existing_oid
                    status = "open"
                    error_msg = None
                else:
                    error_msg = (
                        "BROKER_AMBIGUOUS_UNEXPECTED_RECONCILIATION_REQUIRED:"
                        f"{_e}"
                    )
                break

        # ── Process the final response ────────────────────────────────────────
        try:
            if error_msg is None and self._is_broker_accept_status(status) and broker_order_id:
                ok = self.transition(local_id, OrderStatus.EXIT_SUBMITTED,
                                     broker_order_id=broker_order_id, submitted_ts=now_utc_iso())
                if ok:
                    return {"ok": True, "local_order_id": local_id,
                            "broker_order_id": broker_order_id,
                            "status": OrderStatus.EXIT_SUBMITTED, "error": None}
                error_msg = "exit_submitted_transition_failed_after_broker_accept"
                self._flag_split_brain_order(local_id, broker_order_id=broker_order_id,
                                             error_msg=error_msg,
                                             execution_mode=str(execution_mode or ""))
                return {"ok": False, "local_order_id": local_id,
                        "broker_order_id": broker_order_id, "status": OrderStatus.ERROR,
                        "error": error_msg, "split_brain": True}
            elif error_msg is None and self._is_broker_accept_status(status) and not broker_order_id:
                error_msg = f"broker_accepted_missing_order_id_quarantine:status={status or 'unknown'}"
                ok = self.transition(local_id, OrderStatus.EXIT_SUBMITTED,
                                     submitted_ts=now_utc_iso(), last_error=error_msg)
                return {"ok": False, "local_order_id": local_id, "broker_order_id": None,
                        "status": OrderStatus.EXIT_SUBMITTED if ok else OrderStatus.ERROR,
                        "error": error_msg, "identity_quarantine": True}
            elif error_msg is None:
                if broker_order_id:
                    _sb_reason = f"BROKER_STATUS_UNKNOWN_WITH_ID:status={status!r}"
                    _quarantine_persisted = self._flag_split_brain_order(
                        local_id,
                        broker_order_id=broker_order_id,
                        error_msg=_sb_reason,
                        execution_mode=str(execution_mode or ""),
                    )
                    return {
                        "ok": False,
                        "local_order_id": local_id,
                        "broker_order_id": broker_order_id,
                        "status": (
                            OrderStatus.EXIT_SUBMITTED
                            if _quarantine_persisted
                            else OrderStatus.ERROR
                        ),
                        "error": _sb_reason,
                        "split_brain": True,
                        "reconciliation_required": True,
                        "quarantine_persisted": _quarantine_persisted,
                    }
                error_msg = (f"broker_status:{status or 'unknown'} "
                             f"broker_order_id_missing:{not bool(broker_order_id)}")
        except Exception as e:
            # We already crossed the broker boundary.  A local response-
            # processing exception cannot prove rejection, so keep this exit
            # non-terminal and reconciliation-visible.
            error_msg = error_msg or (
                "BROKER_AMBIGUOUS_RESPONSE_PROCESSING_RECONCILIATION_REQUIRED:"
                f"{e}"
            )

        if str(error_msg or "").startswith("BROKER_AMBIGUOUS_"):
            return {
                "ok": False,
                "local_order_id": local_id,
                "broker_order_id": None,
                "status": OrderStatus.EXIT_REQUESTED,
                "error": error_msg,
                "identity_quarantine": True,
                "reconciliation_required": True,
            }

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

    def _attach_broker_identity_if_missing(
        self,
        local_order_id: str,
        *,
        broker_order_id: str,
        current_status: str,
        current_execution_mode: str,
    ) -> bool:
        """Atomically attach a recovered broker_order_id to an order that is already
        in a submitted-like state but is missing its broker identity.

        Req 3 (PR spec): Used when _lookup_order_by_tag() recovers a broker_order_id
        for a SUBMITTED/ACKNOWLEDGED/PARTIAL_FILL order that lacks one. Must never
        change or regress the order status, never cross client_id or execution_mode
        boundaries, and never write if broker_order_id is already set.

        CAS predicates:
          * exact local_order_id
          * exact client_id
          * matching current_status
          * empty / NULL broker_order_id (only attaches if identity is absent)

        Returns True when Postgres confirms rowcount > 0.
        Returns False on CAS miss, validation failure, or write error.
        """
        _bid = str(broker_order_id or "").strip()
        _status = str(current_status or "").strip().upper()
        _mode = str(current_execution_mode or "").strip().lower()
        if not _bid or not _status or _mode not in {"live", "paper"}:
            return False
        _allowed = {
            OrderStatus.SUBMITTED,
            OrderStatus.ACKNOWLEDGED,
            OrderStatus.PARTIAL_FILL,
        }
        if _status not in _allowed:
            log.warning(
                "[%s] _attach_broker_identity_if_missing: status=%s not in allowed set "
                "order=%s broker_id=%s — refused",
                self.client_id, _status, local_order_id, _bid,
            )
            return False

        _now = now_utc_iso()

        def _attach():
            with conn() as c:
                cur = c.execute(
                    """
                    UPDATE orders
                    SET broker_order_id = %s,
                        meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb,
                        updated_ts = NOW()
                    WHERE local_order_id = %s
                      AND client_id = %s
                      AND LOWER(COALESCE(execution_mode, '')) = %s
                      AND UPPER(COALESCE(status, '')) = %s
                      AND (broker_order_id IS NULL OR broker_order_id = '')
                    """,
                    (
                        _bid,
                        json.dumps({
                            "broker_id_attached_by_tag_recovery": True,
                            "broker_id_attached_at": _now,
                            "broker_id_recovered_from_tag": canonical_broker_submit_key(
                                local_order_id
                            ),
                        }),
                        local_order_id,
                        self.client_id,
                        _mode,
                        _status,
                    ),
                )
                return int(getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0)

        try:
            result = bool(run_with_retry(_attach) > 0)
            if result:
                log.info(
                    "[%s] broker identity attached via tag recovery | "
                    "order=%s broker_id=%s status=%s",
                    self.client_id, local_order_id, _bid, _status,
                )
            return result
        except Exception as exc:
            log.warning(
                "[%s] _attach_broker_identity_if_missing failed order=%s: %s",
                self.client_id, local_order_id, exc,
            )
            return False

    def _lookup_order_by_tag(
        self,
        broker,
        base_url: str,
        account_id: str,
        tag: str,
    ) -> Optional[str]:
        """Find a broker_order_id by client-side tag.

        Used to recover from ambiguous broker responses (read timeout, JSON
        parse failure mid-response) WITHOUT double-submitting. If the order
        landed at Tradier before the response was lost, the tag we sent will
        be on it and we can adopt it. A missing/multiple match remains
        ambiguous and callers must hold for reconciliation.

        Returns the broker_order_id if found, else None.
        Best-effort: any error returns None; this is never proof that retrying
        the POST is safe.
        """
        if not tag or not broker or not base_url or not account_id:
            return None
        try:
            session = getattr(broker, "session", None)
            if session is None:
                return None
            resp = session.get(
                f"{base_url}/v1/accounts/{account_id}/orders",
                headers={"Accept": "application/json"},
                timeout=5,
            )
            if getattr(resp, "status_code", 500) >= 300:
                return None
            payload = resp.json() or {}
            orders_node = payload.get("orders") or {}
            orders = orders_node.get("order") if isinstance(orders_node, dict) else orders_node
            if orders is None:
                return None
            if isinstance(orders, dict):
                orders = [orders]
            if not isinstance(orders, list):
                return None
            tag_str = canonical_broker_submit_key(tag)
            matches = []
            for o in orders:
                if not isinstance(o, dict):
                    continue
                # Tradier echoes the tag on the order. Compare as strings.
                if str(o.get("tag") or "") == tag_str:
                    oid = o.get("id") or o.get("order_id")
                    if oid:
                        matches.append(o)
            if len(matches) == 1:
                only = matches[0]
                return str(only.get("id") or only.get("order_id"))

            # Repegs intentionally reuse the same tag, so the order history may
            # contain a terminal predecessor plus one live replacement.  Adopt
            # only a single nonterminal identity; multiple live/unknown matches
            # are ambiguous and must remain fail-closed.
            terminal = {"canceled", "cancelled", "rejected", "expired", "filled"}
            live_matches = [
                item for item in matches
                if str(item.get("status") or "").strip().lower() not in terminal
            ]
            if len(live_matches) == 1:
                only = live_matches[0]
                return str(only.get("id") or only.get("order_id"))
            return None
        except Exception as _e:
            log.debug(
                "[%s] _lookup_order_by_tag non-fatal: %s",
                self.client_id, _e,
            )
            return None

    def _flag_split_brain_order(
        self,
        local_order_id: str,
        *,
        broker_order_id: str,
        error_msg: str,
        execution_mode: str,
    ) -> bool:
        """
        Quarantine an order that may already be broker-owned.

        A broker ID is durable ownership proof, so ENTRY rows move to SUBMITTED
        and EXIT rows move to EXIT_SUBMITTED.  Those states are non-submit-
        eligible and are already selected by the reconciler/fill monitor.  The
        fenced update never rewrites client, mode, signal, contract, quantity,
        or existing submit metadata.
        """
        _bid = str(broker_order_id or "").strip()
        _mode = str(execution_mode or "").strip().lower()
        if not _bid or _mode not in {"live", "paper"}:
            return False
        try:
            def _mark():
                with conn() as c:
                    cur = c.execute(
                        """
                        UPDATE orders
                        SET status = CASE
                                WHEN kind = 'ENTRY'
                                     AND UPPER(COALESCE(status,'')) IN ('CREATED','PENDING_TRIGGER')
                                    THEN 'SUBMITTED'
                                WHEN kind = 'EXIT'
                                     AND UPPER(COALESCE(status,'')) = 'EXIT_REQUESTED'
                                    THEN 'EXIT_SUBMITTED'
                                ELSE status
                            END,
                            broker_order_id = COALESCE(NULLIF(broker_order_id, ''), %s),
                            submitted_ts = COALESCE(submitted_ts, NOW()),
                            last_error      = %s,
                            meta = COALESCE(meta, '{}'::jsonb) || jsonb_build_object(
                                'split_brain_quarantine', true,
                                'reconciliation_required', true,
                                'split_brain_reason', %s,
                                'broker_order_id', %s,
                                'current_owner', 'broker',
                                'submit_completed_at', %s,
                                'lifecycle_state', CASE
                                    WHEN kind = 'EXIT' THEN 'EXIT_SUBMITTED'
                                    ELSE 'SUBMITTED'
                                END
                            ),
                            updated_ts      = NOW()
                        WHERE local_order_id = %s
                          AND client_id = %s
                          AND LOWER(COALESCE(execution_mode,'')) = %s
                          AND kind IN ('ENTRY','EXIT')
                          AND (
                                (kind = 'ENTRY' AND UPPER(COALESCE(status,'')) IN (
                                    'CREATED','PENDING_TRIGGER','SUBMITTED',
                                    'ACKNOWLEDGED','PARTIAL_FILL'
                                ))
                             OR (kind = 'EXIT' AND UPPER(COALESCE(status,'')) IN (
                                    'EXIT_REQUESTED','EXIT_SUBMITTED',
                                    'EXIT_ACKNOWLEDGED','EXIT_PARTIAL_FILL'
                                ))
                          )
                          AND (
                                broker_order_id IS NULL
                             OR broker_order_id = ''
                             OR broker_order_id = %s
                          )
                        """,
                        (
                            _bid,
                            f"SPLIT_BRAIN:{error_msg}",
                            str(error_msg or ""),
                            _bid,
                            now_utc_iso(),
                            local_order_id,
                            self.client_id,
                            _mode,
                            _bid,
                        ),
                    )
                    return int(
                        getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0
                    )
            marked = bool(run_with_retry(_mark) > 0)
            if not marked:
                log.critical(
                    "[%s] SPLIT_BRAIN QUARANTINE CAS MISSED | order=%s broker=%s",
                    self.client_id, local_order_id, _bid,
                )
                return False
            log.critical(
                "[%s] SPLIT_BRAIN FLAGGED | order=%s broker=%s | "
                "broker accepted but DB transition failed; reconciler will recover on next pass",
                self.client_id, local_order_id, _bid,
            )
            return True
        except Exception as flag_err:
            log.critical(
                "[%s] SPLIT_BRAIN FLAG WRITE FAILED | order=%s broker=%s | "
                "MANUAL INTERVENTION REQUIRED -- broker has live order with no DB record | "
                "flag_error=%s",
                self.client_id, local_order_id, _bid, flag_err,
            )
            return False

    def resolve_split_brain_quarantine(
        self,
        local_order_id: str,
        *,
        broker_order_id: str,
        execution_mode: str,
        broker_status: str,
    ) -> bool:
        """Clear quarantine only after broker truth is retrieved by exact ID."""
        bid = str(broker_order_id or "").strip()
        mode = str(execution_mode or "").strip().lower()
        raw_status = str(broker_status or "").strip().lower()
        if not bid or mode not in {"live", "paper"} or not raw_status:
            return False
        resolved_at = now_utc_iso()
        patch = json.dumps({
            "split_brain_quarantine": False,
            "reconciliation_required": False,
            "split_brain_resolved_at": resolved_at,
            "split_brain_resolved_broker_status": raw_status,
            "split_brain_resolved_by": "ap_reconciler",
        })

        def _resolve():
            with conn() as c:
                cur = c.execute(
                    """
                    UPDATE orders
                    SET last_error = %s,
                        meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb,
                        updated_ts = NOW()
                    WHERE local_order_id = %s
                      AND client_id = %s
                      AND LOWER(COALESCE(execution_mode,'')) = %s
                      AND broker_order_id = %s
                      AND (
                            COALESCE(last_error,'') LIKE 'SPLIT_BRAIN:%%'
                         OR COALESCE((meta->>'split_brain_quarantine')::boolean, false) = true
                      )
                    """,
                    (
                        f"RECONCILED_SPLIT_BRAIN:broker_status={raw_status}",
                        patch,
                        local_order_id,
                        self.client_id,
                        mode,
                        bid,
                    ),
                )
                return int(getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0)

        try:
            return bool(run_with_retry(_resolve) > 0)
        except Exception as exc:
            log.warning(
                "[%s] resolve_split_brain_quarantine failed order=%s broker=%s: %s",
                self.client_id, local_order_id, bid, exc,
            )
            return False

    def get_split_brain_orders(self, *, execution_mode: str | None = None) -> list:
        """Return split-brain orders for this exact client and runner mode."""
        mode = str(execution_mode or "").strip().lower()
        if mode not in {"live", "paper"}:
            log.error(
                "[%s] get_split_brain_orders blocked: execution_mode missing/invalid",
                self.client_id,
            )
            return []

        def _fn():
            with conn() as c:
                c.execute(
                    """
                    SELECT *
                    FROM orders
                    WHERE client_id = %s
                      AND LOWER(TRIM(COALESCE(execution_mode,''))) = %s
                      AND last_error LIKE 'SPLIT_BRAIN:%%'
                    ORDER BY created_ts DESC
                    """,
                    (self.client_id, mode),
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
        pm=None,  # accept shared APPositionManager if caller has one
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

            if pm is None:
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
                    "WHERE local_order_id=%s AND client_id=%s "
                    "AND status NOT IN ('FILLED','EXIT_FILLED','REJECTED','CANCELED','EXPIRED')",
                    (error_msg, local_order_id, self.client_id),
                )
        try:
            run_with_retry(_fn)
        except Exception as _e:
            log.warning("osm_runwithretry_fn_failed: %s", _e)


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
