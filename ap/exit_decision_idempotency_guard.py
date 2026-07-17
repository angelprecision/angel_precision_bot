"""Exit-decision idempotency and broker-flat closed-position fencing.

The exit engine already checks active EXIT orders at the final submit seam, but
it records the decision before that seam and marks the in-memory position
in-flight only after the external callback returns. Production evidence showed a
closed QCOM position producing 2,905 identical SCALE_OUT decisions and a SPY
position producing 602.

This guard adds three narrow protections without changing exit policy:

* an in-process claim around the callback seam so concurrent engine paths cannot
  both call the broker-submit callback for one position;
* an active-EXIT-order and terminal broker-flat fence in the existing broker
  precheck seam, which runs before the engine decision lock;
* rate-limited decision-ledger writes so a persistent failure remains visible
  without inserting the same row every eight seconds.

Broker-truth failures fail open for exits. Durable-claim infrastructure failures
fail closed in LIVE so separate processes cannot double-submit; PAPER may
continue fail-open with a loud diagnostic. Stale unresolved claims are promoted
to AMBIGUOUS rather than reclaimed by age alone.
"""
from __future__ import annotations

import os
import math
import threading
import time
import uuid
from typing import Any, Callable

from ap.db import conn, run_with_retry
from ap.logger import get_logger

log = get_logger("ap.exit_decision_idempotency_guard")

_PATCHED_ATTR = "_AP_EXIT_DECISION_IDEMPOTENCY_PATCHED"
_ORIGINAL_LEDGER_ATTR = "_AP_EXIT_LEDGER_ORIGINAL"
_ORIGINAL_PRECHECK_ATTR = "_AP_EXIT_PRECHECK_ORIGINAL"
_ORIGINAL_SUBMIT_ATTR = "_AP_EXIT_SUBMIT_ORIGINAL"

_ACTIVE_EXIT_STATUSES = {
    "EXIT_REQUESTED",
    "EXIT_SUBMITTED",
    "EXIT_ACKNOWLEDGED",
    "EXIT_PARTIAL_FILL",
}
_TERMINAL_POSITION_STATUSES = {
    "CLOSED",
    "CLOSED_REPAIR",
    "EXPIRED",
    "STOPPED",
    "TAKEN_PROFIT",
    "ERROR",
    "CANCELED",
    "CANCELLED",
}

_CLAIM_STATE_CLAIMED = "CLAIMED"
_CLAIM_STATE_BROKER_OWNED = "BROKER_OWNED"
_CLAIM_STATE_RELEASED_NO_SUBMIT = "RELEASED_NO_SUBMIT"
_CLAIM_STATE_AMBIGUOUS = "AMBIGUOUS"
_STALE_CLAIM_RECONCILIATION_REQUIRED = "STALE_CLAIM_RECONCILIATION_REQUIRED"
_STALE_CLAIM_RECONCILING = "STALE_CLAIM_RECONCILING"
_CONCLUSIVE_NO_SUBMIT_STATUSES = {"ERROR", "REJECTED", "CANCELED", "CANCELLED", "EXPIRED"}
_CONCLUSIVE_PRE_SUBMIT_FAILURE_MARKERS = (
    "PRE_SUBMIT_VALIDATION_FAILED",
    "PRE_BROKER_CALLBACK_REJECTED",
    "NO_POST_ATTEMPTED",
)


def _bounded_float_env(name: str, default: float, minimum: float, maximum: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        log.error("invalid %s=%r; using conservative default=%s", name, raw, default)
        return default
    if not math.isfinite(value) or value < 0:
        log.error("unsafe %s=%r; using conservative default=%s", name, raw, default)
        return default
    return min(max(value, minimum), maximum)


def _bounded_int_env(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError, OverflowError):
        log.error("invalid %s=%r; using conservative default=%s", name, raw, default)
        return default
    if value < 0:
        log.error("unsafe %s=%r; using conservative default=%s", name, raw, default)
        return default
    return min(max(value, minimum), maximum)


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


# Documented deployment bounds: TTLs are at least one second and at most one
# day; caches retain at least 128 entries and never exceed 100k entries.
_ACTION_LEDGER_TTL = _bounded_float_env(
    "EXIT_DECISION_LEDGER_ACTION_DEDUPE_SECONDS", 60.0, 1.0, 86400.0
)
_HOLD_LEDGER_TTL = _bounded_float_env(
    "EXIT_DECISION_LEDGER_HOLD_DEDUPE_SECONDS", 300.0, 1.0, 86400.0
)
_LEDGER_CACHE_MAX = _bounded_int_env(
    "EXIT_DECISION_LEDGER_DEDUPE_CACHE_MAX", 4096, 128, 100000
)
_EXTERNAL_PRECHECK_TTL = _bounded_float_env(
    "EXIT_DECISION_EXTERNAL_PRECHECK_SECONDS", 30.0, 1.0, 86400.0
)
_PRECHECK_CACHE_MAX = _bounded_int_env(
    "EXIT_DECISION_PRECHECK_CACHE_MAX", 4096, 128, 100000
)
_CLAIM_LEASE_SECONDS = _bounded_float_env(
    "EXIT_DECISION_CLAIM_LEASE_SECONDS", 300.0, 1.0, 86400.0
)
_TERMINAL_EXIT_STATUSES = (
    "EXIT_FILLED", "REJECTED", "CANCELED", "CANCELLED", "EXPIRED", "ERROR",
)

_LEDGER_LOCK = threading.Lock()
_LEDGER_LAST_WRITTEN: dict[tuple[str, ...], float] = {}
_PRECHECK_LOCK = threading.Lock()
_PRECHECK_LAST_RUN: dict[str, float] = {}


def _int(value: Any, default: int | None = 0) -> int | None:
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _position_key(pos: Any) -> str:
    return str(
        getattr(pos, "position_id", "")
        or getattr(pos, "option_symbol", "")
        or getattr(pos, "ticker", "")
        or id(pos)
    )


def active_exit_order_blocks(order: Any) -> bool:
    if not isinstance(order, dict):
        return False
    return str(order.get("status") or "").strip().upper() in _ACTIVE_EXIT_STATUSES


def decision_fingerprint(pos: Any, decision: Any, client_id: str = "") -> tuple[str, ...]:
    """Stable fingerprint for one economically identical decision state."""
    return (
        str(client_id or getattr(pos, "client_id", "") or ""),
        _position_key(pos),
        str(getattr(pos, "option_symbol", "") or ""),
        str(getattr(decision, "action", "") or ""),
        str(_int(getattr(decision, "quantity", 0), 0) or 0),
        str(getattr(decision, "reason_code", "") or ""),
        str(getattr(decision, "reason", "") or ""),
        str(_int(getattr(pos, "quantity_remaining", 0), 0) or 0),
        str(getattr(pos, "pending_exit_local_order_id", "") or ""),
        str(getattr(pos, "pending_exit_broker_order_id", "") or ""),
    )


def _decision_should_act(decision: Any) -> bool:
    value = getattr(decision, "should_act", None)
    if value is not None:
        return bool(value)
    return str(getattr(decision, "action", "") or "").upper() in {"CLOSE_ALL", "SCALE_OUT"}


def _execution_mode(engine: Any, pos: Any) -> str:
    return str(
        getattr(getattr(engine, "master_control", None), "mode", "")
        or getattr(pos, "execution_mode", "")
        or ""
    ).strip().upper()


def _durable_claim_outage_blocks_submit(engine: Any, pos: Any) -> bool:
    if _execution_mode(engine, pos) != "LIVE":
        return False
    return getattr(engine, "order_state_machine", None) is not None or getattr(engine, "osm", None) is not None


def _durable_exit_generation(pos: Any, client_id: str) -> tuple[str, int] | None:
    """Resolve the durable generation from terminal EXIT order history.

    Generation advances only after the prior broker-owned EXIT order reaches a
    terminal state.  Combined with remaining quantity, this produces the audit
    contract: client + real position + remaining qty + exit generation.
    """
    client_id = str(client_id or getattr(pos, "client_id", "") or "").strip()
    position_id = str(getattr(pos, "position_id", "") or "").strip()
    remaining_qty = _int(getattr(pos, "quantity_remaining", 0), 0) or 0
    if (
        not client_id
        or not position_id
        or position_id.lower().startswith("broker-repair-")
        or remaining_qty <= 0
    ):
        return None

    def _read_generation() -> int:
        with conn() as c:
            row = c.execute(
                "SELECT COUNT(DISTINCT local_order_id) AS terminal_exit_count "
                "FROM orders WHERE client_id=%s AND position_id::text=%s "
                "AND kind='EXIT' AND status IN %s",
                (client_id, position_id, _TERMINAL_EXIT_STATUSES),
            ).fetchone()
            row = dict(row) if row else {}
            return max(0, _int(row.get("terminal_exit_count"), 0) or 0) + 1

    generation = int(run_with_retry(_read_generation) or 1)
    key = f"{client_id}|{position_id}|{remaining_qty}|{generation}"
    return key, generation


def _claim_local_order_id(pos: Any) -> str:
    return str(getattr(pos, "pending_exit_local_order_id", "") or "").strip()


def _is_exact_reserved_exit_intent(pos: Any, active_order: dict | None) -> bool:
    if not isinstance(active_order, dict):
        return False
    if str(active_order.get("status") or "").strip().upper() != "EXIT_REQUESTED":
        return False
    active_local_order_id = str(active_order.get("local_order_id") or "").strip()
    if not active_local_order_id or active_local_order_id != _claim_local_order_id(pos):
        return False
    if str(active_order.get("broker_order_id") or "").strip():
        return False
    if bool(getattr(pos, "exit_in_flight", False)):
        return False
    return True


def _ensure_local_exit_intent_row(
    engine: Any,
    pos: Any,
    *,
    generation_key: str,
    exit_generation: int,
) -> str:
    local_order_id = _claim_local_order_id(pos)
    if local_order_id:
        return local_order_id

    osm = getattr(engine, "order_state_machine", None) or getattr(engine, "osm", None)
    if osm is None or not callable(getattr(osm, "create_exit_order", None)):
        return ""

    position_id = str(getattr(pos, "position_id", "") or "").strip()
    contract = str(getattr(pos, "option_symbol", "") or "").strip()
    symbol = str(getattr(pos, "ticker", "") or "").strip()
    direction = str(getattr(pos, "side", "") or "").strip()
    qty = _int(getattr(pos, "quantity_remaining", 0), 0) or 0
    execution_mode = _execution_mode(engine, pos).lower()
    if (
        not position_id
        or not contract
        or not symbol
        or not direction
        or qty <= 0
        or execution_mode not in {"live", "paper"}
    ):
        return ""

    local_order_id = str(uuid.uuid4())
    created = str(osm.create_exit_order(
        position_id=position_id,
        contract=contract,
        symbol=symbol,
        direction=direction,
        qty=qty,
        local_order_id=local_order_id,
        limit_price=None,
        execution_mode=execution_mode,
    ) or "").strip()
    if not created:
        return ""
    local_order_id = created

    update_meta = getattr(osm, "update_order_meta", None)
    if callable(update_meta):
        try:
            persisted = update_meta(local_order_id, {
                "exit_generation_claim_key": generation_key,
                "exit_generation_claim": int(exit_generation),
            })
            if persisted is False:
                _retire_local_exit_intent_after_no_submit(
                    engine,
                    local_order_id,
                    error_text="EXIT_DECISION_LOCAL_EXIT_META_PERSIST_FAILED",
                )
                return ""
        except Exception:
            _retire_local_exit_intent_after_no_submit(
                engine,
                local_order_id,
                error_text="EXIT_DECISION_LOCAL_EXIT_META_PERSIST_FAILED",
            )
            return ""

    with engine._lock:
        if not bool(getattr(pos, "closed", False)):
            pos.pending_exit_local_order_id = local_order_id
    return local_order_id


def _claim_durable_decision_generation(
    *,
    generation_key: str,
    client_id: str,
    position_id: str,
    remaining_qty: int,
    exit_generation: int,
    decision: Any,
    local_order_id: str = "",
) -> dict:
    """Atomically claim one actionable decision for this durable generation."""
    def _claim() -> dict:
        with conn() as c:
            row = c.execute(
                "UPDATE exit_decision_generation_claims SET "
                "client_id=%s, "
                "position_id=%s, "
                "remaining_qty=%s, "
                "exit_generation=%s, "
                "decision_action=%s, "
                "decision_reason_code=%s, "
                "claim_state=%s, "
                "last_error=%s "
                "WHERE generation_key=%s AND claim_state=%s "
                "AND claimed_at <= NOW() - (%s * INTERVAL '1 second') "
                "RETURNING generation_key, client_id, position_id, remaining_qty, "
                "exit_generation, decision_action, decision_reason_code, claim_state, "
                "local_order_id, broker_order_id, last_error, claimed_at, released_at",
                (
                    client_id,
                    position_id,
                    remaining_qty,
                    exit_generation,
                    str(getattr(decision, "action", "") or ""),
                    str(getattr(decision, "reason_code", "") or ""),
                    _CLAIM_STATE_AMBIGUOUS,
                    _STALE_CLAIM_RECONCILIATION_REQUIRED,
                    generation_key,
                    _CLAIM_STATE_CLAIMED,
                    _CLAIM_LEASE_SECONDS,
                ),
            ).fetchone()
            if row:
                stale = dict(row)
                stale["claimed"] = False
                log.critical(
                    "EXIT_DECISION_STALE_CLAIM_AMBIGUOUS client=%s position=%s generation_key=%s lease_seconds=%s",
                    client_id,
                    position_id,
                    generation_key,
                    _CLAIM_LEASE_SECONDS,
                )
                return stale

            row = c.execute(
                "UPDATE exit_decision_generation_claims SET "
                "client_id=%s, "
                "position_id=%s, "
                "remaining_qty=%s, "
                "exit_generation=%s, "
                "decision_action=%s, "
                "decision_reason_code=%s, "
                "claim_state=%s, "
                "claimed_at=NOW(), "
                "released_at=NULL, "
                "local_order_id=%s, "
                "broker_order_id=NULL, "
                "last_error=NULL "
                "WHERE generation_key=%s AND claim_state=%s "
                "RETURNING generation_key, client_id, position_id, remaining_qty, "
                "exit_generation, decision_action, decision_reason_code, claim_state, "
                "local_order_id, broker_order_id, last_error, claimed_at, released_at",
                (
                    client_id,
                    position_id,
                    remaining_qty,
                    exit_generation,
                    str(getattr(decision, "action", "") or ""),
                    str(getattr(decision, "reason_code", "") or ""),
                    _CLAIM_STATE_CLAIMED,
                    local_order_id,
                    generation_key,
                    _CLAIM_STATE_RELEASED_NO_SUBMIT,
                ),
            ).fetchone()
            if row:
                claimed = dict(row)
                claimed["claimed"] = True
                return claimed

            row = c.execute(
                "INSERT INTO exit_decision_generation_claims ("
                "generation_key, client_id, position_id, remaining_qty, "
                "exit_generation, decision_action, decision_reason_code, "
                "claim_state, claimed_at, released_at, local_order_id, broker_order_id, last_error"
                ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NULL,%s,NULL,NULL) "
                "ON CONFLICT (generation_key) DO NOTHING "
                "RETURNING generation_key, client_id, position_id, remaining_qty, "
                "exit_generation, decision_action, decision_reason_code, claim_state, "
                "local_order_id, broker_order_id, last_error, claimed_at, released_at",
                (
                    generation_key,
                    client_id,
                    position_id,
                    remaining_qty,
                    exit_generation,
                    str(getattr(decision, "action", "") or ""),
                    str(getattr(decision, "reason_code", "") or ""),
                    _CLAIM_STATE_CLAIMED,
                    local_order_id,
                ),
            ).fetchone()
            if row:
                claimed = dict(row)
                claimed["claimed"] = True
                return claimed

            row = c.execute(
                "SELECT generation_key, client_id, position_id, remaining_qty, "
                "exit_generation, decision_action, decision_reason_code, claim_state, "
                "local_order_id, broker_order_id, last_error, claimed_at, released_at "
                "FROM exit_decision_generation_claims WHERE generation_key=%s LIMIT 1",
                (generation_key,),
            ).fetchone()
            existing = dict(row) if row else {"generation_key": generation_key}
            existing["claimed"] = False
            return existing

    return dict(run_with_retry(_claim) or {"generation_key": generation_key, "claimed": False})


def _load_durable_decision_generation(generation_key: str) -> dict:
    def _read() -> dict:
        with conn() as c:
            row = c.execute(
                "SELECT generation_key, client_id, position_id, remaining_qty, "
                "exit_generation, decision_action, decision_reason_code, claim_state, "
                "local_order_id, broker_order_id, last_error, claimed_at, released_at "
                "FROM exit_decision_generation_claims WHERE generation_key=%s LIMIT 1",
                (generation_key,),
            ).fetchone()
            return dict(row) if row else {}

    return dict(run_with_retry(_read) or {})


def _stale_claim_error(reason: str) -> str:
    text = str(reason or "").strip()
    if not text:
        return _STALE_CLAIM_RECONCILIATION_REQUIRED
    return f"{_STALE_CLAIM_RECONCILIATION_REQUIRED}:{text}"


def _claim_meta_dict(order: dict) -> dict:
    meta = order.get("meta") or {}
    if isinstance(meta, str):
        try:
            import json as _json_local

            meta = _json_local.loads(meta)
        except Exception:
            meta = {}
    return meta if isinstance(meta, dict) else {}


def _acquire_stale_claim_reconciliation(generation_key: str) -> tuple[dict, str]:
    token = f"{_STALE_CLAIM_RECONCILING}:{uuid.uuid4()}"

    def _acquire() -> dict:
        with conn() as c:
            row = c.execute(
                "UPDATE exit_decision_generation_claims "
                "SET last_error=%s, claimed_at=NOW() "
                "WHERE generation_key=%s "
                "  AND claim_state=%s "
                "  AND ("
                "       COALESCE(last_error,'') = '' "
                "    OR COALESCE(last_error,'') LIKE %s "
                "    OR (COALESCE(last_error,'') LIKE %s AND claimed_at <= NOW() - (%s * INTERVAL '1 second'))"
                "  ) "
                "RETURNING generation_key, client_id, position_id, remaining_qty, "
                "exit_generation, decision_action, decision_reason_code, claim_state, "
                "local_order_id, broker_order_id, last_error, claimed_at, released_at",
                (
                    token,
                    generation_key,
                    _CLAIM_STATE_AMBIGUOUS,
                    f"{_STALE_CLAIM_RECONCILIATION_REQUIRED}%",
                    f"{_STALE_CLAIM_RECONCILING}:%",
                    _CLAIM_LEASE_SECONDS,
                ),
            ).fetchone()
            return dict(row) if row else {}

    return dict(run_with_retry(_acquire) or {}), token


def _finish_stale_claim_reconciliation(
    generation_key: str,
    *,
    reconciliation_token: str,
    claim_state: str,
    reason: str = "",
    local_order_id: str = "",
    broker_order_id: str = "",
) -> dict:
    final_error = ""
    if claim_state == _CLAIM_STATE_AMBIGUOUS:
        final_error = _stale_claim_error(reason)

    def _finish() -> dict:
        with conn() as c:
            row = c.execute(
                "UPDATE exit_decision_generation_claims "
                "SET claim_state=%s, "
                "released_at=CASE WHEN %s=%s THEN NOW() ELSE released_at END, "
                "local_order_id=CASE WHEN %s<>'' THEN %s ELSE local_order_id END, "
                "broker_order_id=CASE WHEN %s<>'' THEN %s ELSE broker_order_id END, "
                "last_error=CASE WHEN %s<>'' THEN %s ELSE NULL END "
                "WHERE generation_key=%s "
                "  AND claim_state=%s "
                "  AND COALESCE(last_error,'')=%s "
                "RETURNING generation_key, client_id, position_id, remaining_qty, "
                "exit_generation, decision_action, decision_reason_code, claim_state, "
                "local_order_id, broker_order_id, last_error, claimed_at, released_at",
                (
                    claim_state,
                    claim_state,
                    _CLAIM_STATE_RELEASED_NO_SUBMIT,
                    local_order_id,
                    local_order_id,
                    broker_order_id,
                    broker_order_id,
                    final_error,
                    final_error,
                    generation_key,
                    _CLAIM_STATE_AMBIGUOUS,
                    reconciliation_token,
                ),
            ).fetchone()
            return dict(row) if row else {}

    return dict(run_with_retry(_finish) or {})


def reconcile_stale_exit_generation_claim(
    generation_key: str,
    *,
    execution_core: Any | None = None,
    osm: Any | None = None,
) -> dict:
    """Reconcile one stale ambiguous EXIT decision claim without submit/cancel authority."""
    claim, token = _acquire_stale_claim_reconciliation(generation_key)
    if not claim:
        return _load_durable_decision_generation(generation_key)

    local_order_id = str(claim.get("local_order_id") or "").strip()
    broker_order_id = str(claim.get("broker_order_id") or "").strip()
    position_id = str(claim.get("position_id") or "").strip()
    client_id = str(claim.get("client_id") or "").strip().lower()
    expected_qty = _int(claim.get("remaining_qty"), 0) or 0
    runtime_osm = osm or getattr(execution_core, "order_state_machine", None) or getattr(execution_core, "osm", None)
    if runtime_osm is None:
        return _finish_stale_claim_reconciliation(
            generation_key,
            reconciliation_token=token,
            claim_state=_CLAIM_STATE_AMBIGUOUS,
            reason="RECONCILE_OSM_UNAVAILABLE",
            local_order_id=local_order_id,
            broker_order_id=broker_order_id,
        )
    if not local_order_id:
        return _finish_stale_claim_reconciliation(
            generation_key,
            reconciliation_token=token,
            claim_state=_CLAIM_STATE_AMBIGUOUS,
            reason="RECONCILE_LOCAL_ORDER_ID_MISSING",
        )

    try:
        order = runtime_osm.get_order(local_order_id)
    except Exception as exc:
        return _finish_stale_claim_reconciliation(
            generation_key,
            reconciliation_token=token,
            claim_state=_CLAIM_STATE_AMBIGUOUS,
            reason=f"RECONCILE_ROW_READ_ERROR:{type(exc).__name__}",
            local_order_id=local_order_id,
            broker_order_id=broker_order_id,
        )
    if not isinstance(order, dict):
        return _finish_stale_claim_reconciliation(
            generation_key,
            reconciliation_token=token,
            claim_state=_CLAIM_STATE_AMBIGUOUS,
            reason="RECONCILE_ROW_MISSING",
            local_order_id=local_order_id,
            broker_order_id=broker_order_id,
        )

    if str(order.get("client_id") or "").strip().lower() != client_id:
        return _finish_stale_claim_reconciliation(
            generation_key,
            reconciliation_token=token,
            claim_state=_CLAIM_STATE_AMBIGUOUS,
            reason="RECONCILE_CLIENT_ID_MISMATCH",
            local_order_id=local_order_id,
            broker_order_id=broker_order_id,
        )
    if str(order.get("kind") or "").strip().upper() != "EXIT":
        return _finish_stale_claim_reconciliation(
            generation_key,
            reconciliation_token=token,
            claim_state=_CLAIM_STATE_AMBIGUOUS,
            reason="RECONCILE_KIND_MISMATCH",
            local_order_id=local_order_id,
            broker_order_id=broker_order_id,
        )
    if position_id and str(order.get("position_id") or "").strip() != position_id:
        return _finish_stale_claim_reconciliation(
            generation_key,
            reconciliation_token=token,
            claim_state=_CLAIM_STATE_AMBIGUOUS,
            reason="RECONCILE_POSITION_ID_MISMATCH",
            local_order_id=local_order_id,
            broker_order_id=broker_order_id,
        )
    if expected_qty > 0 and (_int(order.get("qty"), 0) or 0) != expected_qty:
        return _finish_stale_claim_reconciliation(
            generation_key,
            reconciliation_token=token,
            claim_state=_CLAIM_STATE_AMBIGUOUS,
            reason="RECONCILE_QTY_MISMATCH",
            local_order_id=local_order_id,
            broker_order_id=broker_order_id,
        )

    order_meta = _claim_meta_dict(order)
    if _int(order_meta.get("exit_generation_claim"), 0) != (_int(claim.get("exit_generation"), 0) or 0):
        return _finish_stale_claim_reconciliation(
            generation_key,
            reconciliation_token=token,
            claim_state=_CLAIM_STATE_AMBIGUOUS,
            reason="RECONCILE_EXIT_GENERATION_MISMATCH",
            local_order_id=local_order_id,
            broker_order_id=broker_order_id,
        )
    status = str(order.get("status") or "").strip().upper()
    broker_order_id = str(order.get("broker_order_id") or broker_order_id or "").strip()
    filled_qty = _int(order.get("filled_qty"), 0) or 0
    fill_price = _float(order.get("fill_price"), 0.0) or 0.0

    no_submit_proven = (
        status in {"EXIT_REQUESTED", "ERROR", "REJECTED", "CANCELED", "CANCELLED", "EXPIRED"}
        and not broker_order_id
        and not order.get("submitted_ts")
        and not str(order_meta.get("submit_intent_at") or "").strip()
        and not str(order_meta.get("broker_submit_key") or "").strip()
    )
    if no_submit_proven:
        return _finish_stale_claim_reconciliation(
            generation_key,
            reconciliation_token=token,
            claim_state=_CLAIM_STATE_RELEASED_NO_SUBMIT,
            local_order_id=local_order_id,
        )

    if (
        status == "EXIT_REQUESTED"
        and str(order_meta.get("submit_intent_at") or "").strip()
        and execution_core is not None
        and callable(getattr(execution_core, "reconcile_exit_broker_intent", None))
    ):
        adapter_result = execution_core.reconcile_exit_broker_intent(local_order_id=local_order_id) or {}
        disposition = str(adapter_result.get("disposition") or "").strip().upper()
        if disposition == "RECONCILE_PENDING":
            return _finish_stale_claim_reconciliation(
                generation_key,
                reconciliation_token=token,
                claim_state=_CLAIM_STATE_AMBIGUOUS,
                reason=str(adapter_result.get("reason_code") or "RECONCILE_PENDING"),
                local_order_id=local_order_id,
                broker_order_id=broker_order_id,
            )
        try:
            order = runtime_osm.get_order(local_order_id)
        except Exception:
            order = None
        if not isinstance(order, dict):
            return _finish_stale_claim_reconciliation(
                generation_key,
                reconciliation_token=token,
                claim_state=_CLAIM_STATE_AMBIGUOUS,
                reason="RECONCILE_ROW_MISSING_POST_ADOPTION",
                local_order_id=local_order_id,
                broker_order_id=broker_order_id,
            )
        order_meta = _claim_meta_dict(order)
        status = str(order.get("status") or "").strip().upper()
        broker_order_id = str(order.get("broker_order_id") or broker_order_id or "").strip()
        filled_qty = _int(order.get("filled_qty"), 0) or 0
        fill_price = _float(order.get("fill_price"), 0.0) or 0.0

    if status in {"EXIT_FILLED", "EXIT_PARTIAL_FILL"} and broker_order_id and filled_qty > 0 and fill_price > 0:
        from ap.exit_fill_truth_guard import reconcile_confirmed_exit_fill

        canonical_result = {
            "status": order.get("status"),
            "broker_order_id": broker_order_id,
            "filled_qty": order.get("filled_qty"),
            "fill_price": order.get("fill_price"),
            "filled_ts": order.get("filled_ts"),
        }
        try:
            reconcile_confirmed_exit_fill(order, canonical_result)
        except Exception as exc:
            return _finish_stale_claim_reconciliation(
                generation_key,
                reconciliation_token=token,
                claim_state=_CLAIM_STATE_AMBIGUOUS,
                reason=f"EXIT_FILL_RECONCILE_FAILED:{type(exc).__name__}",
                local_order_id=local_order_id,
                broker_order_id=broker_order_id,
            )
        return _finish_stale_claim_reconciliation(
            generation_key,
            reconciliation_token=token,
            claim_state=_CLAIM_STATE_BROKER_OWNED,
            local_order_id=local_order_id,
            broker_order_id=broker_order_id,
        )

    active_evidence = bool(
        broker_order_id
        or status in {"EXIT_SUBMITTED", "EXIT_ACKNOWLEDGED", "EXIT_PARTIAL_FILL", "EXIT_FILLED"}
        or str(order_meta.get("submit_intent_at") or "").strip()
        or bool(order_meta.get("split_brain_quarantine"))
        or bool(order_meta.get("reconciliation_required"))
    )
    if active_evidence:
        return _finish_stale_claim_reconciliation(
            generation_key,
            reconciliation_token=token,
            claim_state=_CLAIM_STATE_BROKER_OWNED,
            local_order_id=local_order_id,
            broker_order_id=broker_order_id,
        )

    return _finish_stale_claim_reconciliation(
        generation_key,
        reconciliation_token=token,
        claim_state=_CLAIM_STATE_AMBIGUOUS,
        reason="RECONCILE_TRUTH_UNAVAILABLE",
        local_order_id=local_order_id,
        broker_order_id=broker_order_id,
    )


def _update_durable_decision_generation(
    generation_key: str,
    *,
    claim_state: str,
    local_order_id: str = "",
    broker_order_id: str = "",
    error_text: str = "",
) -> None:
    def _update() -> None:
        with conn() as c:
            c.execute(
                "UPDATE exit_decision_generation_claims "
                "SET claim_state=%s, "
                "released_at=CASE WHEN %s=%s THEN NOW() ELSE released_at END, "
                "local_order_id=CASE WHEN %s<>'' THEN %s ELSE local_order_id END, "
                "broker_order_id=CASE WHEN %s<>'' THEN %s ELSE broker_order_id END, "
                "last_error=CASE WHEN %s<>'' THEN %s ELSE NULL END "
                "WHERE generation_key=%s",
                (
                    claim_state,
                    claim_state,
                    _CLAIM_STATE_RELEASED_NO_SUBMIT,
                    local_order_id,
                    local_order_id,
                    broker_order_id,
                    broker_order_id,
                    error_text,
                    error_text,
                    generation_key,
                ),
            )

    run_with_retry(_update)


def _retire_local_exit_intent_after_no_submit(engine: Any, local_order_id: str, error_text: str = "") -> None:
    """Retire one reserved EXIT_REQUESTED row after conclusive no-submit proof."""
    local_order_id = str(local_order_id or "").strip()
    if not local_order_id:
        return
    osm = getattr(engine, "order_state_machine", None) or getattr(engine, "osm", None)
    if osm is None:
        return
    retire_intent = getattr(osm, "retire_unsubmitted_exit_intent", None)
    if not callable(retire_intent):
        return
    try:
        retire_intent(local_order_id, last_error=error_text or "NO_POST_ATTEMPTED")
    except Exception:
        return


def _extract_callback_trace_identity(callback_trace: dict) -> dict:
    identity = dict(callback_trace.get("identity") or {})
    result = callback_trace.get("result")
    if not isinstance(result, dict):
        return identity

    for source_key, target_key in (
        ("local_order_id", "local_order_id"),
        ("exit_local_order_id", "local_order_id"),
        ("broker_order_id", "broker_order_id"),
        ("order_id", "broker_order_id"),
        ("id", "broker_order_id"),
    ):
        value = result.get(source_key)
        if value is not None and not identity.get(target_key):
            identity[target_key] = str(value)
    if "accepted" not in identity and "accepted" in result:
        identity["accepted"] = bool(result.get("accepted"))
    if not identity.get("raw_status"):
        raw_status = result.get("status") or result.get("raw_status") or result.get("state")
        if raw_status is not None:
            identity["raw_status"] = str(raw_status)
    return identity


def _classify_submit_claim_outcome(
    engine: Any,
    pos: Any,
    callback_trace: dict,
    callback_returned: bool,
) -> tuple[str, str, str, str]:
    position_id = str(getattr(pos, "position_id", "") or "")
    try:
        active_order = _active_exit_order(engine, position_id)
    except Exception as exc:
        log.warning(
            "[%s] EXIT_DECISION_GENERATION_POST_SUBMIT_LOOKUP_FAILED position=%s error=%s",
            getattr(pos, "ticker", ""),
            position_id,
            exc,
        )
        active_order = None

    active_meta = _claim_meta_dict(active_order or {})
    active_order_status = str((active_order or {}).get("status") or "").strip().upper()
    active_order_has_broker_ownership = bool(
        str((active_order or {}).get("broker_order_id") or "").strip()
        or (active_order or {}).get("submitted_ts")
        or str(active_meta.get("submit_intent_at") or "").strip()
        or bool(active_meta.get("split_brain_quarantine"))
        or bool(active_meta.get("reconciliation_required"))
        or active_order_status in {"EXIT_SUBMITTED", "EXIT_ACKNOWLEDGED", "EXIT_PARTIAL_FILL", "EXIT_FILLED"}
    )
    identity = _extract_callback_trace_identity(callback_trace)
    local_order_id = str(
        identity.get("local_order_id")
        or getattr(pos, "pending_exit_local_order_id", "")
        or (active_order or {}).get("local_order_id")
        or ""
    ).strip()
    broker_order_id = str(
        identity.get("broker_order_id")
        or getattr(pos, "pending_exit_broker_order_id", "")
        or (active_order or {}).get("broker_order_id")
        or ""
    ).strip()
    raw_status = str(identity.get("raw_status") or callback_trace.get("status") or "").strip().upper()
    error_text = str(callback_trace.get("error") or "")
    if not error_text and callback_trace.get("exception") is not None:
        error_text = str(callback_trace["exception"])

    if active_exit_order_blocks(active_order) and active_order_has_broker_ownership:
        return _CLAIM_STATE_BROKER_OWNED, local_order_id, broker_order_id, error_text
    if broker_order_id or (raw_status == "EXIT_SUBMITTED" and local_order_id):
        return _CLAIM_STATE_BROKER_OWNED, local_order_id, broker_order_id, error_text
    if bool(getattr(pos, "exit_in_flight", False)) and (local_order_id or broker_order_id):
        return _CLAIM_STATE_BROKER_OWNED, local_order_id, broker_order_id, error_text

    if not callback_trace.get("entered"):
        return _CLAIM_STATE_RELEASED_NO_SUBMIT, local_order_id, broker_order_id, error_text
    if callback_trace.get("exception") is not None:
        return _CLAIM_STATE_AMBIGUOUS, local_order_id, broker_order_id, error_text
    if error_text.startswith("BROKER_AMBIGUOUS_"):
        return _CLAIM_STATE_AMBIGUOUS, local_order_id, broker_order_id, error_text

    result = callback_trace.get("result")
    if isinstance(result, dict):
        if result.get("reconciliation_required") or result.get("split_brain"):
            return _CLAIM_STATE_AMBIGUOUS, local_order_id, broker_order_id, error_text
        if bool(result.get("identity_quarantine")) and raw_status not in {"", "EXIT_SUBMITTED"}:
            return _CLAIM_STATE_AMBIGUOUS, local_order_id, broker_order_id, error_text

    accepted = identity.get("accepted")
    if error_text.startswith("broker_conn_error:"):
        return _CLAIM_STATE_AMBIGUOUS, local_order_id, broker_order_id, error_text
    if any(error_text.startswith(marker) for marker in _CONCLUSIVE_PRE_SUBMIT_FAILURE_MARKERS):
        return _CLAIM_STATE_RELEASED_NO_SUBMIT, local_order_id, broker_order_id, error_text
    if raw_status in _CONCLUSIVE_NO_SUBMIT_STATUSES:
        return _CLAIM_STATE_RELEASED_NO_SUBMIT, local_order_id, broker_order_id, error_text
    if accepted is False:
        return _CLAIM_STATE_RELEASED_NO_SUBMIT, local_order_id, broker_order_id, error_text
    if callback_returned:
        return _CLAIM_STATE_AMBIGUOUS, local_order_id, broker_order_id, error_text
    return _CLAIM_STATE_RELEASED_NO_SUBMIT, local_order_id, broker_order_id, error_text


def should_write_ledger(
    pos: Any,
    decision: Any,
    *,
    client_id: str = "",
    now_monotonic: float | None = None,
) -> bool:
    """Return True once per fingerprint/TTL; terminal and in-flight repeats skip."""
    if bool(getattr(pos, "closed", False)):
        return False
    if (_int(getattr(pos, "quantity_remaining", 0), 0) or 0) <= 0:
        return False
    if bool(getattr(pos, "exit_in_flight", False)):
        return False

    now_value = time.monotonic() if now_monotonic is None else float(now_monotonic)
    fingerprint = decision_fingerprint(pos, decision, client_id)
    ttl = _ACTION_LEDGER_TTL if _decision_should_act(decision) else _HOLD_LEDGER_TTL

    with _LEDGER_LOCK:
        previous = _LEDGER_LAST_WRITTEN.get(fingerprint)
        if previous is not None and now_value - previous < max(0.0, ttl):
            return False
        _LEDGER_LAST_WRITTEN[fingerprint] = now_value

        if len(_LEDGER_LAST_WRITTEN) > max(128, _LEDGER_CACHE_MAX):
            cutoff = now_value - max(_ACTION_LEDGER_TTL, _HOLD_LEDGER_TTL, 60.0) * 2
            stale = [key for key, seen_at in _LEDGER_LAST_WRITTEN.items() if seen_at < cutoff]
            for key in stale:
                _LEDGER_LAST_WRITTEN.pop(key, None)
            while len(_LEDGER_LAST_WRITTEN) > max(128, _LEDGER_CACHE_MAX):
                _LEDGER_LAST_WRITTEN.pop(next(iter(_LEDGER_LAST_WRITTEN)), None)
    return True


def should_run_external_precheck(pos: Any, *, now_monotonic: float | None = None) -> bool:
    """Bound DB/OSM reconciliation reads; the final submit fence stays immediate."""
    if bool(getattr(pos, "closed", False)) or bool(getattr(pos, "exit_in_flight", False)):
        return False
    now_value = time.monotonic() if now_monotonic is None else float(now_monotonic)
    key = _position_key(pos)
    with _PRECHECK_LOCK:
        previous = _PRECHECK_LAST_RUN.get(key)
        if previous is not None and now_value - previous < max(0.0, _EXTERNAL_PRECHECK_TTL):
            return False
        _PRECHECK_LAST_RUN[key] = now_value
        if len(_PRECHECK_LAST_RUN) > max(128, _PRECHECK_CACHE_MAX):
            cutoff = now_value - max(_EXTERNAL_PRECHECK_TTL, 30.0) * 4
            stale = [cache_key for cache_key, seen_at in _PRECHECK_LAST_RUN.items() if seen_at < cutoff]
            for cache_key in stale:
                _PRECHECK_LAST_RUN.pop(cache_key, None)
            while len(_PRECHECK_LAST_RUN) > max(128, _PRECHECK_CACHE_MAX):
                _PRECHECK_LAST_RUN.pop(next(iter(_PRECHECK_LAST_RUN)), None)
    return True


def _active_exit_order(engine: Any, position_id: str) -> dict | None:
    osm = getattr(engine, "order_state_machine", None) or getattr(engine, "osm", None)
    if osm is None or not position_id:
        return None
    getter = getattr(osm, "_get_active_exit_order", None) or getattr(osm, "get_active_exit_order", None)
    if not callable(getter):
        return None
    row = getter(position_id)
    return row if isinstance(row, dict) else None


def _terminal_position_snapshot(pos: Any, engine: Any) -> dict | None:
    client_id = str(
        getattr(pos, "client_id", "")
        or getattr(engine, "client_id", "")
        or getattr(engine, "_email", "")
        or ""
    )
    position_id = str(getattr(pos, "position_id", "") or "")
    contract = str(getattr(pos, "option_symbol", "") or "").strip().upper()
    if not client_id:
        return None

    def _read() -> dict | None:
        with conn() as c:
            row = None
            if position_id and not position_id.lower().startswith("broker-repair-"):
                row = c.execute(
                    "SELECT id, status, qty, quantity_remaining, contract, client_id "
                    "FROM positions WHERE client_id=%s AND id::text=%s LIMIT 1",
                    (client_id, position_id),
                ).fetchone()
            if not row and contract and (
                not position_id or position_id.lower().startswith("broker-repair-")
            ):
                rows = c.execute(
                    "SELECT id, status, qty, quantity_remaining, contract, client_id "
                    "FROM positions WHERE client_id=%s AND UPPER(contract)=UPPER(%s) "
                    "ORDER BY COALESCE(entry_ts, created_at) DESC LIMIT 2",
                    (client_id, contract),
                ).fetchall()
                row = rows[0] if len(rows) == 1 else None
            return dict(row) if row else None

    try:
        snapshot = run_with_retry(_read)
    except Exception as exc:
        log.debug(
            "terminal position snapshot unavailable position=%s contract=%s error=%s",
            position_id,
            contract,
            exc,
        )
        return None
    if not snapshot:
        return None

    status = str(snapshot.get("status") or "").strip().upper()
    remaining = _int(snapshot.get("quantity_remaining"), None)
    if status in _TERMINAL_POSITION_STATUSES or remaining == 0:
        return snapshot
    return None


def _fresh_exact_broker_flat(pos: Any, engine: Any) -> bool:
    try:
        from ap.exit_safety import resolve_exit_broker_truth

        truth = resolve_exit_broker_truth(
            broker=getattr(engine, "broker", None),
            client_id=str(getattr(pos, "client_id", "") or getattr(engine, "client_id", "") or ""),
            contract=str(getattr(pos, "option_symbol", "") or ""),
        )
    except Exception as exc:
        log.debug("broker-flat confirmation unavailable position=%s error=%s", _position_key(pos), exc)
        return False

    if not isinstance(truth, dict) or truth.get("is_fresh_exact") is not True:
        return False
    open_qty = _int(truth.get("broker_truth_open_qty"), None)
    return open_qty == 0


def _mark_active_exit_owned(engine: Any, pos: Any, active_order: dict) -> None:
    with engine._lock:
        if bool(getattr(pos, "closed", False)):
            return
        pos.exit_in_flight = True
        pos.pending_exit_local_order_id = str(active_order.get("local_order_id") or "")
        pos.pending_exit_broker_order_id = str(active_order.get("broker_order_id") or "")


def _remove_terminal_broker_flat(engine: Any, pos: Any, terminal: dict) -> None:
    key = _position_key(pos)
    with engine._lock:
        pos.closed = True
        pos.quantity_remaining = 0
        pos.exit_in_flight = False
        pos.pending_exit_action = ""
        pos.pending_exit_reason = ""
        pos.pending_exit_qty = 0
        pos.pending_exit_local_order_id = ""
        pos.pending_exit_broker_order_id = ""
        engine._positions = [tracked for tracked in engine._positions if tracked is not pos]
        positions_by_id = getattr(engine, "_positions_by_id", None)
        if isinstance(positions_by_id, dict):
            positions_by_id.pop(str(getattr(pos, "position_id", "") or ""), None)
    with _PRECHECK_LOCK:
        _PRECHECK_LAST_RUN.pop(key, None)
    log.warning(
        "[%s] EXIT_EVALUATION_SUPPRESSED_TERMINAL_BROKER_FLAT position=%s contract=%s db_status=%s",
        getattr(pos, "ticker", ""),
        terminal.get("id"),
        terminal.get("contract"),
        terminal.get("status"),
    )


def wrap_ledger(original: Callable[..., Any]) -> Callable[..., Any]:
    def guarded(pos, decision, *, client_id: str = ""):
        if not should_write_ledger(pos, decision, client_id=client_id):
            return None
        return original(pos, decision, client_id=client_id)

    return guarded


def wrap_precheck(original: Callable[..., bool]) -> Callable[..., bool]:
    """Run bounded external lookups outside the engine decision lock."""
    def guarded(self, *args, **kwargs) -> bool:
        result = bool(original(self, *args, **kwargs))
        try:
            active_positions = list(self.active_positions())
        except Exception as exc:
            log.debug("exit precheck active snapshot unavailable: %s", exc)
            return result

        for pos in active_positions:
            if not should_run_external_precheck(pos):
                continue
            position_id = str(getattr(pos, "position_id", "") or "")
            try:
                active_order = _active_exit_order(self, position_id)
            except Exception as exc:
                log.debug("early active-exit lookup unavailable position=%s error=%s", position_id, exc)
                active_order = None
            if active_exit_order_blocks(active_order) and not _is_exact_reserved_exit_intent(pos, active_order):
                try:
                    _mark_active_exit_owned(self, pos, active_order)
                except Exception as exc:
                    log.debug("active exit ownership hydration failed position=%s error=%s", position_id, exc)
                continue

            terminal = _terminal_position_snapshot(pos, self)
            if terminal and _fresh_exact_broker_flat(pos, self):
                try:
                    _remove_terminal_broker_flat(self, pos, terminal)
                except Exception as exc:
                    log.warning("terminal broker-flat suppression failed position=%s error=%s", position_id, exc)

        return result

    return guarded


def wrap_submit(original: Callable[..., bool]) -> Callable[..., bool]:
    def guarded(self, pos, decision, *args, **kwargs) -> bool:
        key = _position_key(pos)
        resolved_client = str(
            getattr(pos, "client_id", "")
            or getattr(self, "client_id", "")
            or getattr(self, "_email", "")
            or ""
        ).strip()
        position_id = str(getattr(pos, "position_id", "") or "")
        remaining_qty = _int(getattr(pos, "quantity_remaining", 0), 0) or 0
        with self._lock:
            claims = getattr(self, "_ap_exit_submit_claims", None)
            if claims is None:
                claims = set()
                self._ap_exit_submit_claims = claims
            if key in claims:
                log.warning(
                    "[%s] EXIT_PROCESS_CLAIM_BLOCKED_DUPLICATE position=%s action=%s qty=%s",
                    getattr(pos, "ticker", ""),
                    key,
                    getattr(decision, "action", ""),
                    getattr(decision, "quantity", 0),
                )
                return False
            claims.add(key)

        try:
            try:
                active_order = _active_exit_order(self, position_id)
            except Exception as exc:
                log.warning(
                    "[%s] EXIT_DECISION_ACTIVE_EXIT_FENCE_LOOKUP_FAILED position=%s error=%s",
                    getattr(pos, "ticker", ""),
                    position_id,
                    exc,
                )
                active_order = None
            if active_exit_order_blocks(active_order) and not _is_exact_reserved_exit_intent(pos, active_order):
                try:
                    _mark_active_exit_owned(self, pos, active_order)
                except Exception as exc:
                    log.debug("active exit ownership hydration failed position=%s error=%s", position_id, exc)
                log.warning(
                    "[%s] EXIT_DECISION_ACTIVE_EXIT_FENCE_BLOCKED position=%s local_order_id=%s broker_order_id=%s status=%s",
                    getattr(pos, "ticker", ""),
                    position_id,
                    active_order.get("local_order_id"),
                    active_order.get("broker_order_id"),
                    active_order.get("status"),
                )
                return False

            generation_key = ""
            exit_generation = 0
            if _decision_should_act(decision):
                try:
                    durable = _durable_exit_generation(pos, resolved_client)
                except Exception as exc:
                    mode = _execution_mode(self, pos)
                    log.critical(
                        "[%s] EXIT_DECISION_GENERATION_READ_UNAVAILABLE mode=%s client_id=%s position_id=%s action=%s reason_code=%s error=%s",
                        getattr(pos, "ticker", ""),
                        mode,
                        resolved_client,
                        position_id,
                        getattr(decision, "action", ""),
                        getattr(decision, "reason_code", ""),
                        exc,
                    )
                    if _durable_claim_outage_blocks_submit(self, pos):
                        return False
                    durable = None
                if durable is not None:
                    generation_key, exit_generation = durable
                    local_order_id = _ensure_local_exit_intent_row(
                        self,
                        pos,
                        generation_key=generation_key,
                        exit_generation=exit_generation,
                    )
                    if not local_order_id and _durable_claim_outage_blocks_submit(self, pos):
                        log.critical(
                            "[%s] EXIT_DECISION_LOCAL_EXIT_IDENTITY_UNAVAILABLE client_id=%s position_id=%s key=%s exit_generation=%s",
                            getattr(pos, "ticker", ""),
                            resolved_client,
                            position_id,
                            generation_key,
                            exit_generation,
                        )
                        return False
                    try:
                        claim = _claim_durable_decision_generation(
                            generation_key=generation_key,
                            client_id=resolved_client,
                            position_id=position_id,
                            remaining_qty=remaining_qty,
                            exit_generation=exit_generation,
                            decision=decision,
                            local_order_id=local_order_id,
                        )
                    except Exception as exc:
                        mode = _execution_mode(self, pos)
                        log.critical(
                            "[%s] EXIT_DECISION_GENERATION_CLAIM_FAILED mode=%s client_id=%s position_id=%s key=%s qty_remaining=%s exit_generation=%s action=%s reason_code=%s error=%s",
                            getattr(pos, "ticker", ""),
                            mode,
                            resolved_client,
                            position_id,
                            generation_key,
                            remaining_qty,
                            exit_generation,
                            getattr(decision, "action", ""),
                            getattr(decision, "reason_code", ""),
                            exc,
                        )
                        if _durable_claim_outage_blocks_submit(self, pos):
                            _retire_local_exit_intent_after_no_submit(
                                self,
                                local_order_id,
                                error_text=f"EXIT_DECISION_GENERATION_CLAIM_FAILED:{exc}",
                            )
                            return False
                        generation_key = ""
                        exit_generation = 0
                        claim = {"claimed": True}
                    if not claim.get("claimed"):
                        winning_local_id = str(claim.get("local_order_id") or "").strip()
                        reserved_local_id = str(local_order_id or "").strip()
                        if reserved_local_id and winning_local_id != reserved_local_id:
                            _retire_local_exit_intent_after_no_submit(
                                self,
                                reserved_local_id,
                                error_text="EXIT_DECISION_GENERATION_DUPLICATE_SUPPRESSED",
                            )
                        log.critical(
                            "[%s] EXIT_DECISION_GENERATION_DUPLICATE_SUPPRESSED client_id=%s position_id=%s key=%s qty_remaining=%s exit_generation=%s action=%s reason_code=%s existing_state=%s",
                            getattr(pos, "ticker", ""),
                            resolved_client,
                            position_id,
                            generation_key,
                            remaining_qty,
                            exit_generation,
                            getattr(decision, "action", ""),
                            getattr(decision, "reason_code", ""),
                            claim.get("claim_state", ""),
                        )
                        return False

            callback_attr = (
                "on_scale"
                if str(getattr(decision, "action", "") or "").upper() == "SCALE_OUT"
                else "on_exit"
            )
            original_callback = getattr(self, callback_attr, None)
            callback_trace = {
                "entered": False,
                "result": None,
                "exception": None,
                "identity": {},
                "status": "",
                "error": "",
            }
            if callable(original_callback):
                with self._lock:
                    callback_lock = getattr(self, "_ap_exit_submit_callback_lock", None)
                    if callback_lock is None:
                        callback_lock = threading.Lock()
                        self._ap_exit_submit_callback_lock = callback_lock

                def traced_callback(*cb_args, **cb_kwargs):
                    callback_trace["entered"] = True
                    try:
                        result = original_callback(*cb_args, **cb_kwargs)
                    except Exception as exc:
                        callback_trace["exception"] = exc
                        raise
                    callback_trace["result"] = result
                    if isinstance(result, dict):
                        callback_trace["status"] = str(
                            result.get("status") or result.get("raw_status") or result.get("state") or ""
                        )
                        callback_trace["error"] = str(result.get("error") or "")
                    extractor = getattr(self, "_extract_exit_order_identity", None)
                    if callable(extractor):
                        try:
                            callback_trace["identity"] = extractor(result) or {}
                        except Exception:
                            callback_trace["identity"] = {}
                    return result

                with callback_lock:
                    setattr(self, callback_attr, traced_callback)
                    try:
                        callback_returned = bool(original(self, pos, decision, *args, **kwargs))
                    finally:
                        setattr(self, callback_attr, original_callback)
            else:
                callback_returned = bool(original(self, pos, decision, *args, **kwargs))

            if generation_key:
                claim_state, local_order_id, broker_order_id, error_text = _classify_submit_claim_outcome(
                    self,
                    pos,
                    callback_trace,
                    callback_returned,
                )
                try:
                    _update_durable_decision_generation(
                        generation_key,
                        claim_state=claim_state,
                        local_order_id=local_order_id,
                        broker_order_id=broker_order_id,
                        error_text=error_text,
                    )
                except Exception as exc:
                    log.critical(
                        "[%s] EXIT_DECISION_GENERATION_POST_SUBMIT_UPDATE_FAILED client_id=%s position_id=%s key=%s claim_state=%s callback_returned=%s error=%s",
                        getattr(pos, "ticker", ""),
                        resolved_client,
                        position_id,
                        generation_key,
                        claim_state,
                        callback_returned,
                        exc,
                    )
                if claim_state == _CLAIM_STATE_RELEASED_NO_SUBMIT:
                    _retire_local_exit_intent_after_no_submit(
                        self,
                        local_order_id,
                        error_text=error_text,
                    )
            return callback_returned
        finally:
            with self._lock:
                claims = getattr(self, "_ap_exit_submit_claims", None)
                if isinstance(claims, set):
                    claims.discard(key)

    return guarded


def install_exit_decision_idempotency_guard() -> None:
    """Install idempotent wrappers on the actual production exit engine."""
    import ap_exit_engine as engine_module

    if getattr(engine_module, _PATCHED_ATTR, False):
        return

    engine_cls = engine_module.APExitEngine
    original_ledger = engine_module._ledger_exit_decision
    original_precheck = engine_cls._broker_position_precheck
    original_submit = engine_cls._submit_exit_decision

    setattr(engine_module, _ORIGINAL_LEDGER_ATTR, original_ledger)
    setattr(engine_cls, _ORIGINAL_PRECHECK_ATTR, original_precheck)
    setattr(engine_cls, _ORIGINAL_SUBMIT_ATTR, original_submit)

    engine_module._ledger_exit_decision = wrap_ledger(original_ledger)
    engine_cls._broker_position_precheck = wrap_precheck(original_precheck)
    engine_cls._submit_exit_decision = wrap_submit(original_submit)
    setattr(engine_module, _PATCHED_ATTR, True)
