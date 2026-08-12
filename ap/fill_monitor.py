# ap/fill_monitor.py — Fill Monitor (OSM-routed, rich final)
"""
Fill Monitor — broker reconciliation poller + side-effect orchestrator.

This file preserves the production behaviors from the larger legacy monitor:
- audit_log writes
- structured observability events
- entry equity/symbol-lock release
- 1-1 pair cancel after confirmed entry fill
- optional broker-side standing stop after local position persistence
- exit price/dashboard sync
- legacy fallback helpers, but disabled in production unless explicitly allowed

Architecture:
- APOrderStateMachine owns order lifecycle truth.
- APPositionManager owns position truth.
- APExitEngine owns exit behavior, scale-outs, runner state, and full-close logic.
- Fill monitor polls broker reality, maps broker status to canonical OSM states,
  enforces cumulative fill sanity, calls OSM, then runs side effects only after
  successful OSM confirmation.

Critical safety rules:
- MUST filter by client_id.
- MUST support stop_event so old runners do not become zombie reconcilers.
- MUST NOT poll non-broker PENDING_TRIGGER watch plans.
- MUST NOT use legacy direct DB lifecycle writes in production unless
  ALLOW_LEGACY_FILL_MONITOR=1.
- MUST persist local position before optional broker-side standing stop.
- MUST cancel broker order before marking pair-opposite local order CANCELED.
- filled_qty MUST be cumulative broker fill quantity, not incremental.
"""

from __future__ import annotations

import importlib
import inspect
import math
import os
import re
import time
from contextlib import nullcontext
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Optional

from ap.trace import trace_gate
from ap.db import conn, run_with_retry
from ap.utils import now_utc_iso, json_dumps, json_loads
from ap.logger import get_logger
from ap.config import Config
from ap.state import release_equity, release_symbol_lock
from ap.broker import BrokerAdapter
from ap.observability import emit_decision_event, get_git_commit

log = get_logger("ap.fill_monitor")
cfg = Config()

OPT_MULTIPLIER = 100
RUN_ID = os.getenv("AP_RUN_ID", "unknown")
STRATEGY_VERSION = os.getenv("AP_STRATEGY_VERSION", "ap_live_beta")
GIT_COMMIT = get_git_commit()

_CANONICAL_OWNER_HANDOFF_RETRY_ERROR_PREFIXES = (
    "FILLED_ENTRY_POSITION_IDENTITY_BIND_FAILED",
    "FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN",
)
_CANONICAL_OWNER_HANDOFF_STANDING_STOP_STATE_KEY = (
    "canonical_owner_handoff_standing_stop_state"
)
_CANONICAL_OWNER_HANDOFF_STANDING_STOP_ID_KEY = (
    "canonical_owner_handoff_standing_stop_id"
)
_CANONICAL_OWNER_HANDOFF_STANDING_STOP_ATTEMPTED_KEY = (
    "canonical_owner_handoff_standing_stop_attempted"
)
_CANONICAL_OWNER_HANDOFF_ENTRY_PROVEN_KEY = (
    "canonical_owner_handoff_entry_handoff_proven"
)
_CANONICAL_OWNER_HANDOFF_STANDING_STOP_STATES = frozenset(
    {"SUBMITTING", "SUBMITTED", "FAILED", "OUTCOME_UNPROVEN"}
)

def _strict_positive_finite_float(value) -> float | None:
    """Return a positive finite scalar without allowing bool coercion."""
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(parsed) or parsed <= 0:
        return None
    return parsed


def _strict_positive_whole_number(value) -> int | None:
    """Return a positive whole-number scalar without allowing bool coercion."""
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(parsed) or parsed <= 0 or not parsed.is_integer():
        return None
    return int(parsed)


def _strict_nonnegative_whole_number(value) -> int | None:
    """Return a non-negative whole-number scalar without allowing bool coercion."""
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(parsed) or parsed < 0 or not parsed.is_integer():
        return None
    return int(parsed)

ALLOW_LEGACY_FILL_MONITOR = (
    os.getenv("ALLOW_LEGACY_FILL_MONITOR", "0").strip().lower()
    in {"1", "true", "yes", "on"}
)

AP_ENV = os.getenv("AP_ENV", os.getenv("ENV", "production")).strip().lower()
PRODUCTION_MODE = AP_ENV in {"prod", "production", "live"} or os.getenv("AP_LIVE_TRADING", "0").strip().lower() in {"1", "true", "yes", "on"}

if PRODUCTION_MODE and ALLOW_LEGACY_FILL_MONITOR:
    raise RuntimeError(
        "ALLOW_LEGACY_FILL_MONITOR=1 is forbidden in production/live mode; "
        "fill_monitor must use OSM/PM as the source-of-truth path."
    )

ACTIVE_BROKER_STATUSES = {
    "OPEN",
    "PENDING",
    "ACCEPTED",
    "WORKING",
    "LIVE",
    "QUEUED",
    "HELD",
    "ROUTED",
    "NEW",
    "PENDING_REVIEW",
}

TERMINAL_FAILURE_STATUSES = {"REJECTED", "CANCELED", "EXPIRED"}

UNKNOWN_ERROR_ESCALATE_AFTER = int(os.getenv("FILL_MONITOR_UNKNOWN_ERROR_ESCALATE_AFTER", "3"))
FILL_ANOMALY_STATUS = os.getenv("FILL_MONITOR_ANOMALY_STATUS", "BROKER_FILL_ANOMALY").strip().upper()

_BROKER_STATE_ANOMALY_COUNTS: dict[str, int] = {}


def _order_count_key(client_id: str, local_order_id: str, broker_order_id: str | None = None) -> str:
    return f"{client_id}:{local_order_id}:{broker_order_id or ''}"


def _reset_broker_anomaly_count(client_id: str, local_order_id: str, broker_order_id: str | None = None) -> None:
    _BROKER_STATE_ANOMALY_COUNTS.pop(_order_count_key(client_id, local_order_id, broker_order_id), None)


def _increment_broker_anomaly_count(client_id: str, local_order_id: str, broker_order_id: str | None = None) -> int:
    key = _order_count_key(client_id, local_order_id, broker_order_id)
    count = int(_BROKER_STATE_ANOMALY_COUNTS.get(key, 0)) + 1
    _BROKER_STATE_ANOMALY_COUNTS[key] = count
    return count


# =============================================================================
# AUDIT / OBSERVABILITY
# =============================================================================

def audit(client_id: str, level: str, event: str, payload: dict):
    """Best-effort audit write. Audit failures must never block reconciliation."""
    def _fn():
        with conn() as c:
            c.execute(
                "INSERT INTO audit_log (ts, level, event, payload, client_id) "
                "VALUES (%s,%s,%s,%s,%s)",
                (now_utc_iso(), level, event, json_dumps(payload), client_id),
            )
    try:
        run_with_retry(_fn)
    except Exception as exc:
        log.warning(
            "Audit write failed non-fatal | client=%s event=%s error=%s",
            client_id,
            event,
            exc,
        )



def _safe_alert(alert_fn, msg: str) -> None:
    """Best-effort operator alert. Never block reconciliation."""
    if not alert_fn:
        return
    try:
        alert_fn(msg)
    except Exception as exc:
        log.debug("Fill monitor alert_fn failed non-critical: %s", exc)

def emit_fill_event(
    order: dict,
    *,
    decision: str,
    reason_code: str,
    explanation: str,
    result: dict | None = None,
    stage: str = "fill_monitor",
    extra_inputs: dict | None = None,
    extra_context: dict | None = None,
) -> bool:
    """Emit structured fill-monitor observability without blocking reconciliation.

    Returns True on successful emit, False if the underlying observability
    write failed.  Failures are logged at WARNING level (not debug) so
    identity-critical events like CANONICAL_ADOPTION_MODE_CONFLICT stay
    visible even when the downstream ledger is degraded.  The function
    remains non-fatal and never re-raises.
    """
    try:
        result = result or {}
        emit_decision_event(
            run_id=RUN_ID,
            candidate_id=str(order.get("signal_id") or order.get("local_order_id") or ""),
            trade_id=str(order.get("local_order_id") or ""),
            position_id=str(order.get("position_id") or ""),
            client_id=str(order.get("client_id") or "default"),
            stage=stage,
            decision=decision,
            reason_code=reason_code,
            explanation=explanation,
            symbol=order.get("symbol"),
            contract=order.get("contract"),
            setup_type=order.get("pattern"),
            timeframe=order.get("timeframe"),
            strategy_version=STRATEGY_VERSION,
            git_commit=GIT_COMMIT,
            inputs={
                "kind": order.get("kind"),
                "broker_order_id": order.get("broker_order_id"),
                "local_order_id": order.get("local_order_id"),
                "filled_qty": result.get("filled_qty"),
                "avg_fill": result.get("avg_fill"),
                "broker_reason": result.get("reason"),
                **(extra_inputs or {}),
            },
            context=extra_context or {},
        )
        return True
    except Exception as exc:
        # AMENDMENT (PR #385 review — observability contract): promote
        # from log.debug to log.warning and return False.  A silent
        # debug-only failure hid critical identity events; callers now
        # get a real signal without needing (or getting fooled by) an
        # outer try/except that never runs.
        log.warning(
            "Fill monitor observability emit failed | "
            "reason_code=%s client=%s local_order_id=%s error=%s",
            reason_code,
            order.get("client_id"),
            order.get("local_order_id"),
            exc,
        )
        return False


# =============================================================================
# DB QUERY — CLIENT-SAFE BROKER-BACKED ORDERS ONLY
# =============================================================================

def get_pending_orders(client_id: str) -> list[dict]:
    """
    Return broker-backed orders for one client.

    PENDING_TRIGGER is intentionally excluded because those are watcher plans,
    not live broker orders. They should only enter this monitor after the watcher
    submits to broker and OSM moves them to SUBMITTED.
    """
    if not client_id:
        raise ValueError("get_pending_orders requires client_id")

    def _fn():
        with conn() as c:
            rows = c.execute(
                """
                SELECT
                    client_id,
                    local_order_id,
                    broker_order_id,
                    position_id,
                    kind,
                    symbol,
                    contract,
                    direction,
                    qty,
                    limit_price,
                    reserved_cost,
                    status,
                    created_ts,
                    plan_id,
                    signal_id,
                    tier,
                    score,
                    pattern,
                    stop_underlying,
                    target_underlying,
                    trigger_price,
                    timeframe,
                    filled_qty,
                    fill_price,
                    execution_mode,
                    filled_ts,
                    meta,
                    last_error,
                    meta->>'canonical_signal_id' AS canonical_signal_id,
                    meta->>'underlying_entry'    AS underlying_entry_meta,
                    meta->>'entry_underlying'    AS entry_underlying_meta,
                    meta->>'last_underlying_price' AS last_underlying_price_meta
                FROM orders
                WHERE client_id = %s
                  AND kind IN ('ENTRY','EXIT')
                  AND (
                    status IN (
                      'SUBMITTED',
                      'ACKNOWLEDGED',
                      'PARTIAL_FILL',
                      'EXIT_SUBMITTED',
                      'EXIT_ACKNOWLEDGED',
                      'EXIT_PARTIAL_FILL'
                    )
                    OR (
                      kind = 'ENTRY'
                      AND status = 'FILLED'
                      AND (
                        position_id IS NULL
                        OR BTRIM(position_id) = ''
                        OR LOWER(BTRIM(COALESCE(
                            meta->>'canonical_owner_handoff_retry_required', ''
                        ))) = 'true'
                        OR (
                            meta ? 'canonical_owner_handoff_entry_handoff_proven'
                            AND LOWER(BTRIM(COALESCE(
                                meta->>'canonical_owner_handoff_entry_handoff_proven', ''
                            ))) <> 'true'
                        )
                        OR last_error LIKE %s
                        OR last_error LIKE %s
                      )
                    )
                  )
                  AND broker_order_id IS NOT NULL
                  AND broker_order_id != ''
                  AND UPPER(BTRIM(broker_order_id)) NOT IN (
                    'N/A', 'NA', 'NONE', 'NULL', 'UNKNOWN', '?', 'TRUE', 'FALSE', '0'
                  )
                ORDER BY created_ts ASC
                """,
                (
                    client_id,
                    *(
                        f"{prefix}%"
                        for prefix in _CANONICAL_OWNER_HANDOFF_RETRY_ERROR_PREFIXES
                    ),
                ),
            ).fetchall()
            return [dict(r) for r in rows]
    return run_with_retry(_fn)


def _has_proven_broker_order_id(value) -> bool:
    if isinstance(value, bool):
        return False
    broker_id = str(value or "").strip()
    return bool(
        broker_id
        and broker_id.upper() not in {
            "N/A", "NA", "NONE", "NULL", "UNKNOWN", "?",
            "TRUE", "FALSE", "0",
        }
    )


def _has_proven_standing_stop_order_id(value) -> bool:
    """Require a concrete broker identity before calling protection proven."""
    if isinstance(value, bool):
        return False
    if isinstance(value, (int, float)) and value <= 0:
        return False
    return _has_proven_broker_order_id(value)


def _is_canonical_owner_handoff_recovery(order: dict) -> bool:
    """Return the one restart predicate for a durably FILLED ENTRY handoff."""
    if not isinstance(order, dict):
        return False
    if str(order.get("kind") or "").strip().upper() != "ENTRY":
        return False
    if str(order.get("status") or "").strip().upper() != "FILLED":
        return False
    if not _has_proven_broker_order_id(order.get("broker_order_id")):
        return False

    position_id_missing = not str(order.get("position_id") or "").strip()
    retry_marker_value = _canonical_handoff_meta(order).get(
        "canonical_owner_handoff_retry_required"
    )
    retry_marker = retry_marker_value is True or str(
        retry_marker_value or ""
    ).strip().lower() == "true"
    last_error = str(order.get("last_error") or "").strip()
    retry_error = any(
        last_error.startswith(prefix)
        for prefix in _CANONICAL_OWNER_HANDOFF_RETRY_ERROR_PREFIXES
    )
    handoff_proven_value = _canonical_handoff_meta(order).get(
        _CANONICAL_OWNER_HANDOFF_ENTRY_PROVEN_KEY
    )
    handoff_proven = handoff_proven_value is True or str(
        handoff_proven_value or ""
    ).strip().lower() == "true"
    handoff_pending = (
        _CANONICAL_OWNER_HANDOFF_ENTRY_PROVEN_KEY
        in _canonical_handoff_meta(order)
        and not handoff_proven
    )
    return position_id_missing or retry_marker or retry_error or handoff_pending


def _durable_filled_entry_recovery_result(
    order: dict,
    *,
    runtime_execution_mode=None,
) -> dict | None:
    """Return a DB-only FILLED result only after exact runtime admission.

    ``runtime_execution_mode`` is the already-resolved per-iteration runtime
    authority from ``run_fill_monitor``.  A direct caller that cannot provide
    that authority must hold rather than deriving it from the durable row.
    """
    if not _is_canonical_owner_handoff_recovery(order):
        return None
    if _canonical_handoff_identity_where(order) is None:
        return None

    row_mode_state, row_mode = _mode_source_state(order.get("execution_mode"))
    runtime_mode_state, resolved_runtime_mode = _mode_source_state(
        runtime_execution_mode
    )
    if (
        row_mode_state != "valid"
        or runtime_mode_state != "valid"
        or row_mode != resolved_runtime_mode
    ):
        return None

    raw_qty = order.get("filled_qty")
    if isinstance(raw_qty, bool):
        return None
    try:
        filled_qty = int(raw_qty or 0)
    except (TypeError, ValueError, OverflowError):
        return None
    if filled_qty <= 0:
        return None

    raw_price = order.get("fill_price")
    avg_fill = _finite_float_or_none(raw_price)
    if avg_fill is None or avg_fill <= 0:
        return None

    return {
        "status": "FILLED",
        "filled_qty": filled_qty,
        "avg_fill": avg_fill,
        "reason": "DURABLE_ORDERS_FILLED_ENTRY_HANDOFF_RECOVERY",
        "raw": {"source": "durable_orders_fill"},
        "durable_db_filled_recovery": True,
    }


def _broker_poll_unavailable_for_durable_filled_recovery(result: dict) -> bool:
    """Allow DB-only repair only for an unavailable, not contradictory, poll."""
    mapped = str(result.get("status") or "").strip().upper()
    raw = result.get("raw") or {}
    raw_status = str(raw.get("status") or "").strip().upper() if isinstance(raw, dict) else ""
    reason = str(result.get("reason") or "").strip().upper()
    if mapped == "UNKNOWN":
        return not raw_status or raw_status in {"UNKNOWN", "UNAVAILABLE", "ERROR"}
    if mapped == "ERROR":
        return not raw and reason not in {
            "BROKER_FILLED_ZERO_QTY",
            "FILLED_ORDER_SIDE_UNRESOLVED",
        }
    return False


def _mode_source_state(value) -> tuple[str, str]:
    """Classify one runtime mode source: ('absent'|'malformed'|'valid', mode).

    ``absent`` covers ``None`` and unset; ``malformed`` covers explicitly
    present but non-canonical (e.g. ``"LIVE"``, ``"  live  "``, unrelated
    strings); ``valid`` returns the canonical ``live``/``paper``.
    """
    if value is None:
        return ("absent", "")
    raw = str(value)
    stripped = raw.strip().lower()
    if stripped in {"live", "paper"} and raw == stripped:
        return ("valid", stripped)
    if stripped == "":
        return ("absent", "")
    return ("malformed", "")


def _finite_float_or_none(value) -> float | None:
    """Return a finite float, rejecting booleans and non-finite values."""
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) else None


_MAX_BROKER_QUANTITY = 2**63 - 1


def _validated_broker_quantity(value) -> int | None:
    """Return a non-negative integral broker quantity, or hold on invalid data."""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, str) and value != value.strip():
        return None
    try:
        parsed = Decimal(str(value))
        if (
            not parsed.is_finite()
            or parsed < 0
            or parsed > _MAX_BROKER_QUANTITY
        ):
            return None
        integral = parsed.to_integral_value()
        if parsed != integral:
            return None
        return int(integral)
    except (InvalidOperation, TypeError, ValueError, OverflowError):
        return None


def _broker_quantity_error_result(
    order: dict,
    broker_order_id,
    raw: dict,
    *,
    quantity_field: str,
    quantity_value,
) -> dict:
    """Return contradictory broker evidence without laundering it as unavailable."""
    reason = "BROKER_QUANTITY_INVALID"
    response_evidence = dict(raw)
    response_evidence.update(
        {
            "_malformed_broker_quantity": True,
            "quantity_field": quantity_field,
            "quantity_type": type(quantity_value).__name__,
        }
    )
    audit(
        str(order.get("client_id") or "default"),
        "ERROR",
        reason,
        {
            "broker_order_id": broker_order_id,
            "local_order_id": order.get("local_order_id"),
            "quantity_field": quantity_field,
            "quantity_type": type(quantity_value).__name__,
        },
    )
    return {
        "status": "ERROR",
        "filled_qty": 0,
        "avg_fill": 0.0,
        "reason": reason,
        "raw": response_evidence,
    }


def _resolve_runtime_execution_mode(
    *,
    runtime_execution_mode=None,
    exit_engine=None,
) -> str:
    """Resolve runtime execution-mode authority with CONFLICT detection.

    Binding audit correction: runtime authority is independent per source.
    Conflicts and malformed-but-present values must HOLD, never resolve by
    precedence.  Behavior:

    - zero valid sources -> ``""`` (UNPROVEN / HOLD)
    - one or more valid sources agreeing -> canonical ``live``/``paper``
    - two or more valid sources disagreeing -> ``""`` (CONFLICT / HOLD)
    - any explicitly-present malformed source -> ``""`` (HOLD)

    Durable row mode is not part of this resolution.  Callers apply the
    exact-equality gate between this result and the row's own
    ``execution_mode``.
    """
    # ``fill_monitor_loop`` passes the already-resolved value into each
    # per-order call. Treat an explicitly supplied but unproven value as a
    # hard HOLD; do not let a second source launder a prior conflict by
    # resolving it back to a valid engine mode.
    if runtime_execution_mode is not None:
        explicit_state, _ = _mode_source_state(runtime_execution_mode)
        if explicit_state != "valid":
            return ""

    master_control = getattr(exit_engine, "master_control", None)
    if master_control is not None and hasattr(
        master_control, "runtime_execution_mode"
    ):
        master_control_mode = getattr(
            master_control, "runtime_execution_mode", None
        )
    else:
        master_control_mode = getattr(master_control, "mode", None)
    candidates = (
        runtime_execution_mode,
        master_control_mode,
        getattr(exit_engine, "execution_mode", None),
    )
    proven: set[str] = set()
    for candidate in candidates:
        state, mode = _mode_source_state(candidate)
        if state == "malformed":
            return ""
        if state == "valid":
            proven.add(mode)
    if len(proven) != 1:
        return ""
    return next(iter(proven))


# Backwards-compatible shim: some call sites and tests import this name.
def _normalize_runtime_execution_mode(value) -> str:
    state, mode = _mode_source_state(value)
    return mode if state == "valid" else ""


def get_broker_owned_exit_requests(client_id: str) -> list[dict]:
    """Return only EXIT_REQUESTED rows with exact broker ownership proof.

    These rows are intentionally separate from ``get_pending_orders``: an
    ordinary EXIT_REQUESTED row is a local pre-submit intent and must never be
    polled at the broker.  The dedicated predicate is the recovery boundary
    for a row whose broker handoff returned an exact order id but whose local
    status remained EXIT_REQUESTED.
    """
    if not client_id:
        raise ValueError("get_broker_owned_exit_requests requires client_id")

    def _fn():
        with conn() as c:
            rows = c.execute(
                """
                SELECT
                    client_id,
                    local_order_id,
                    broker_order_id,
                    position_id,
                    kind,
                    symbol,
                    contract,
                    direction,
                    qty,
                    limit_price,
                    reserved_cost,
                    status,
                    created_ts,
                    plan_id,
                    signal_id,
                    tier,
                    score,
                    pattern,
                    stop_underlying,
                    target_underlying,
                    trigger_price,
                    timeframe,
                    filled_qty,
                    fill_price,
                    execution_mode,
                    filled_ts,
                    meta,
                    meta->>'broker_submitted_ts' AS broker_submitted_ts,
                    meta->>'canonical_signal_id' AS canonical_signal_id,
                    meta->>'underlying_entry'    AS underlying_entry_meta,
                    meta->>'entry_underlying'    AS entry_underlying_meta,
                    meta->>'last_underlying_price' AS last_underlying_price_meta
                FROM orders
                WHERE client_id = %s
                  AND kind = 'EXIT'
                  AND status = 'EXIT_REQUESTED'
                  AND execution_mode IN ('live','paper')
                  AND broker_order_id IS NOT NULL
                  AND BTRIM(broker_order_id) <> ''
                  AND UPPER(BTRIM(broker_order_id)) <> 'N/A'
                  AND qty > 0
                ORDER BY created_ts ASC
                """,
                (client_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    return run_with_retry(_fn)


def _adopt_broker_owned_exit_request(
    osm,
    order: dict,
    *,
    source: str,
    runtime_execution_mode: str = "",
) -> tuple[bool, dict, dict]:
    """Adopt a broker-owned EXIT_REQUESTED row and reload durable truth."""
    local_id = str(order.get("local_order_id") or "").strip()
    broker_id = str(order.get("broker_order_id") or "").strip()
    client_id = str(order.get("client_id") or "").strip()
    position_id = str(order.get("position_id") or "").strip()
    execution_mode = str(order.get("execution_mode") or "").strip()
    runtime_mode = _normalize_runtime_execution_mode(runtime_execution_mode)
    try:
        expected_qty = int(order.get("qty") or 0)
    except (TypeError, ValueError):
        expected_qty = 0

    if not _has_proven_broker_order_id(broker_id):
        result = {
            "disposition": "IDENTITY_MISMATCH",
            "adopted": False,
            "reason_code": "BROKER_OWNED_EXIT_REQUEST_RECOVERY_HOLD",
            "error": "broker_order_id_missing_or_placeholder",
        }
        return False, dict(order), result

    adopt = getattr(osm, "adopt_broker_owned_exit_request", None) if osm else None
    if not local_id or not position_id or expected_qty <= 0:
        result = {
            "disposition": "IDENTITY_MISMATCH",
            "adopted": False,
            "reason_code": "BROKER_OWNED_EXIT_REQUEST_RECOVERY_HOLD",
            "error": "local_or_position_identity_missing_or_qty_invalid",
        }
    elif execution_mode not in {"live", "paper"}:
        result = {
            "disposition": "IDENTITY_MISMATCH",
            "adopted": False,
            "reason_code": "BROKER_OWNED_EXIT_REQUEST_RECOVERY_HOLD",
            "error": "durable_execution_mode_missing_or_noncanonical",
        }
    elif not runtime_mode or runtime_mode != execution_mode:
        result = {
            "disposition": "IDENTITY_MISMATCH",
            "adopted": False,
            "reason_code": "BROKER_OWNED_EXIT_REQUEST_RECOVERY_HOLD",
            "error": (
                "runtime_execution_mode_unproven_or_conflict:"
                f"runtime={runtime_mode or 'unknown'}:durable={execution_mode}"
            ),
        }
    elif not callable(adopt):
        result = {
            "disposition": "DB_ERROR",
            "adopted": False,
            "reason_code": "BROKER_OWNED_EXIT_REQUEST_RECOVERY_HOLD",
            "error": "osm_adoption_method_unavailable",
        }
    else:
        try:
            broker_submitted_ts = order.get("broker_submitted_ts")
            result = adopt(
                local_id,
                broker_order_id=broker_id,
                execution_mode=execution_mode,
                client_id=client_id,
                position_id=position_id,
                expected_qty=expected_qty,
                broker_submitted_ts=broker_submitted_ts,
                source=source,
            )
        except Exception as exc:
            result = {
                "disposition": "DB_ERROR",
                "adopted": False,
                "reason_code": "BROKER_OWNED_EXIT_REQUEST_RECOVERY_HOLD",
                "error": f"{type(exc).__name__}:{exc}",
            }

    if not isinstance(result, dict):
        result = {
            "disposition": "ADOPTED" if bool(result) else "IDENTITY_MISMATCH",
            "adopted": bool(result),
            "reason_code": (
                "EXIT_BROKER_OWNERSHIP_ADOPTED_FROM_REQUESTED"
                if bool(result)
                else "BROKER_OWNED_EXIT_REQUEST_RECOVERY_HOLD"
            ),
        }
    result = dict(result)
    adopted = bool(
        result.get("adopted")
        or result.get("disposition") in {
            "ADOPTED",
            "ALREADY_BROKER_OWNED_ACTIVE",
        }
    )
    result["adopted"] = adopted

    if result.get("already_terminal") or result.get("disposition") == "ALREADY_TERMINAL":
        result["already_terminal"] = True
        emit_fill_event(
            order,
            decision="CONFIRMED",
            reason_code="EXIT_BROKER_OWNERSHIP_ALREADY_TERMINAL",
            explanation=(
                "The exact broker-owned EXIT row is already terminal; stopping "
                "recovery without replaying broker polling or fill side effects."
            ),
            result=result,
            extra_context={"source": source, "previous_status": "EXIT_REQUESTED"},
        )
        return False, dict(order), result

    if adopted:
        try:
            refreshed = osm.get_order(local_id) if callable(getattr(osm, "get_order", None)) else None
        except Exception:
            refreshed = None
        merged = dict(order)
        if isinstance(refreshed, dict):
            merged.update(refreshed)
        merged["broker_order_id"] = broker_id
        merged_status = str(merged.get("status") or "").strip().upper()
        if not merged_status or merged_status == "EXIT_REQUESTED":
            merged_status = str(result.get("status") or "EXIT_SUBMITTED").strip().upper()
        merged["status"] = merged_status
        emit_fill_event(
            merged,
            decision="CONFIRMED",
            reason_code=str(
                result.get("reason_code")
                or "EXIT_BROKER_OWNERSHIP_ADOPTED_FROM_REQUESTED"
            ),
            explanation="Exact broker ownership normalized before canonical broker polling.",
            result=result,
            extra_context={"source": source, "previous_status": "EXIT_REQUESTED"},
        )
        return True, merged, result

    result.setdefault("reason_code", "BROKER_OWNED_EXIT_REQUEST_RECOVERY_HOLD")
    result.setdefault("error", "adoption_not_confirmed")
    emit_fill_event(
        order,
        decision="ALERT",
        reason_code="BROKER_OWNED_EXIT_REQUEST_RECOVERY_HOLD",
        explanation=(
            "Broker ownership is present but durable EXIT lifecycle adoption "
            "could not be proven; holding without submit/cancel/fill mutation."
        ),
        result=result,
        extra_context={
            "source": source,
            "previous_status": "EXIT_REQUESTED",
            "execution_mode": execution_mode,
            "position_id": position_id,
        },
    )
    audit(
        client_id,
        "CRITICAL",
        "BROKER_OWNED_EXIT_REQUEST_RECOVERY_HOLD",
        {
            "local_order_id": local_id,
            "broker_order_id": broker_id,
            "position_id": position_id,
            "execution_mode": execution_mode,
            "reason": result.get("error"),
            "source": source,
        },
    )
    return False, dict(order), result


# =============================================================================
# PR #235 — Fill monitor safety helpers (inlined from prior shim).
#
# All logic here supports the eight hardening pieces:
#   1. Confirmed broker fills must not default a missing side to CALL.
#   2. Side is resolved from orders.direction first, then OCC C/P marker.
#   3. Broker FILLED / EXIT_FILLED with cumulative filled_qty <= 0 must NOT
#      transition to filled.
#   4. underlying-at-fill is read from an explicit data broker when provided.
#   5. Terminal ENTRY cleanup always releases the symbol lock, even when
#      reserved_cost cannot be reconstructed.
#   6. Position-creation failure after confirmed fill persists a dashboard-
#      visible orders.last_error.
#   7. data_broker is an optional argument on process_pending_order and
#      fill_monitor_loop.
#   8. BUY/SELL are NEVER mapped to CALL/PUT in the fill monitor — they are
#      execution actions, not thesis direction.
# =============================================================================

# OCC option symbol format: <root><YYMMDD><C|P><strike>.  We only need the
# side marker; strike/root normalization stays in the OCC parser used elsewhere.
_OCC_SIDE_RE = re.compile(r"\d{6}([CP])")


def _resolve_order_option_side(order: dict) -> tuple[Optional[str], str]:
    """Resolve option thesis side without guessing.

    Priority:
      1. orders.direction / order.side when already canonical CALL or PUT.
      2. OCC option symbol C/P marker.
      3. unresolved — caller MUST fail closed / quarantine.  Never default
         to CALL, and never map BUY/SELL → CALL/PUT (those are execution
         actions on option orders, not thesis direction).

    Returns (side, source) where source is one of:
      "order_direction" | "occ_contract" | "missing_or_unparseable"
    """
    raw = str(order.get("direction") or order.get("side") or "").strip().upper()
    if raw in {"CALL", "PUT"}:
        return raw, "order_direction"

    contract = str(
        order.get("contract")
        or order.get("option_symbol")
        or order.get("symbol")
        or ""
    ).strip().upper()
    m = _OCC_SIDE_RE.search(contract)
    if m:
        return ("CALL" if m.group(1) == "C" else "PUT"), "occ_contract"

    return None, "missing_or_unparseable"


def _with_resolved_direction(order: dict, side: str, source: str) -> dict:
    """Return a shallow copy of the order with canonical direction + provenance."""
    patched = dict(order)
    patched["direction"] = side
    patched["_fill_monitor_side_source"] = source
    return patched


def _broker_base_url(broker) -> Optional[str]:
    """Best-effort broker base URL extraction for audit context."""
    cfg_obj = getattr(broker, "cfg", None)
    return (
        getattr(broker, "base_url", None)
        or getattr(cfg_obj, "base_url", None)
        or getattr(cfg_obj, "baseurl", None)
        or getattr(broker, "_base_url", None)
    )


def _select_quote_broker(execution_broker, data_broker=None):
    """Prefer an explicit data broker over the execution broker for quote reads.

    Priority:
      1. explicit data_broker argument
      2. execution_broker.data_broker attribute (attached via fill_monitor_loop)
      3. execution_broker itself (fallback — matches legacy behavior)
    """
    return data_broker or getattr(execution_broker, "data_broker", None) or execution_broker


def _emit_side_unresolved(order: dict, *, reason_code: str, alert_fn=None) -> None:
    """Emit critical audit + fill event when option side cannot be resolved.

    Callers MUST also skip whatever side-effect they were about to run
    (position creation, exit engine seed, pair-opposite cancel, etc.).
    """
    client_id = str(order.get("client_id") or "default")
    payload = {
        "local_order_id":  order.get("local_order_id"),
        "broker_order_id": order.get("broker_order_id"),
        "symbol":          order.get("symbol"),
        "contract":        order.get("contract"),
        "direction":       order.get("direction"),
        "side":            order.get("side"),
        "reason":          "missing_or_unparseable_side",
    }
    log.critical("[%s] %s | %s", client_id, reason_code, payload)
    audit(client_id, "CRITICAL", reason_code, payload)
    emit_fill_event(
        order,
        decision="ERROR",
        reason_code=reason_code,
        explanation=(
            "Confirmed broker fill could not be mapped to CALL/PUT without "
            "guessing; normal position/exit seeding blocked."
        ),
        result={},
        extra_context=payload,
    )
    _safe_alert(
        alert_fn,
        f"[fill_monitor:{client_id}] {reason_code} | "
        f"order={order.get('local_order_id')} broker={order.get('broker_order_id')}",
    )


def _record_position_create_failure(order: dict, reason: str = "FILLED_ORDER_POSITION_CREATE_FAILED") -> None:
    """Persist dashboard-visible orders.last_error when a confirmed fill's
    downstream side effect (position create, exit engine seed, etc.) fails.

    Never raises — this is observability, not a control-flow gate.
    """
    client_id = str(order.get("client_id") or "default")
    local_order_id = order.get("local_order_id")
    if not local_order_id:
        return

    def _write():
        with conn() as c:
            c.execute(
                """
                UPDATE orders
                   SET last_error = %s,
                       updated_ts = NOW()
                 WHERE client_id = %s
                   AND local_order_id = %s
                """,
                (reason, client_id, local_order_id),
            )

    try:
        run_with_retry(_write)
    except Exception as exc:
        log.debug(
            "[%s] failed to persist %s for %s: %s",
            client_id, reason, local_order_id, exc,
        )

    audit(
        client_id,
        "CRITICAL",
        reason,
        {
            "local_order_id":  local_order_id,
            "broker_order_id": order.get("broker_order_id"),
            "symbol":          order.get("symbol"),
            "contract":        order.get("contract"),
        },
    )


# =============================================================================
# BROKER CHECK
# =============================================================================

def check_order_with_broker(broker: BrokerAdapter, order: dict) -> dict:
    """
    Query broker for actual order status.

    Contract:
    - returned filled_qty is broker cumulative filled quantity, not incremental
    - raw order quantity is used as fallback only when broker status is truly FILLED
    - active broker statuses map to ACKNOWLEDGED / EXIT_ACKNOWLEDGED
    """
    broker_order_id = order.get("broker_order_id")
    if not _has_proven_broker_order_id(broker_order_id):
        return {
            "status": "UNKNOWN",
            "filled_qty": 0,
            "avg_fill": 0.0,
            "reason": "NO_BROKER_ID",
            "raw": {},
        }

    kind = (order.get("kind") or "ENTRY").upper()

    try:
        raw = broker.get_order(broker_order_id)
        raw_status = raw.get("status") if isinstance(raw, dict) else None
        if (
            not isinstance(raw, dict)
            or not raw
            or not isinstance(raw_status, str)
            or not raw_status.strip()
        ):
            reason = "BROKER_RESPONSE_MALFORMED"
            response_evidence = (
                dict(raw) if isinstance(raw, dict) else {}
            )
            response_evidence.update(
                {
                    "_malformed_broker_response": True,
                    "response_type": type(raw).__name__,
                }
            )
            audit(
                str(order.get("client_id") or "default"),
                "ERROR",
                reason,
                {
                    "broker_order_id": broker_order_id,
                    "local_order_id": order.get("local_order_id"),
                    "response_type": type(raw).__name__,
                },
            )
            return {
                "status": "ERROR",
                "filled_qty": 0,
                "avg_fill": 0.0,
                "reason": reason,
                "raw": response_evidence,
            }
        status = raw_status.upper()

        if kind == "EXIT":
            status_map = {
                "FILLED": "EXIT_FILLED",
                "PARTIALLY_FILLED": "EXIT_PARTIAL_FILL",
                "PARTIAL": "EXIT_PARTIAL_FILL",
                "CANCELED": "CANCELED",
                "CANCELLED": "CANCELED",
                "REJECTED": "REJECTED",
                "EXPIRED": "EXPIRED",
            }
            our = "EXIT_ACKNOWLEDGED" if status in ACTIVE_BROKER_STATUSES else status_map.get(status, "UNKNOWN")
        else:
            status_map = {
                "FILLED": "FILLED",
                "PARTIALLY_FILLED": "PARTIAL_FILL",
                "PARTIAL": "PARTIAL_FILL",
                "CANCELED": "CANCELED",
                "CANCELLED": "CANCELED",
                "REJECTED": "REJECTED",
                "EXPIRED": "EXPIRED",
            }
            our = "ACKNOWLEDGED" if status in ACTIVE_BROKER_STATUSES else status_map.get(status, "UNKNOWN")

        explicit_filled_qty = None
        for _qty_key in (
            "exec_quantity",
            "filled_quantity",
            "filled_qty",
            "cumulative_filled_qty",
        ):
            if _qty_key in raw:
                explicit_filled_qty = raw.get(_qty_key)
                break
        if explicit_filled_qty is not None:
            filled_qty = _strict_positive_whole_number(explicit_filled_qty) or 0
        elif our in ("FILLED", "EXIT_FILLED"):
            # Requested order quantity is not execution truth. A terminal
            # broker status without an explicit executed quantity remains a
            # fill-truth hold; do not synthesize it from ``quantity``.
            filled_qty = 0
        else:
            filled_qty = 0

        raw_fill_price = None
        for _price_key in (
            "avg_fill_price",
            "average_fill_price",
            "fill_price",
            "filled_avg_price",
        ):
            if _price_key in raw:
                raw_fill_price = raw.get(_price_key)
                break
        avg_fill = _strict_positive_finite_float(raw_fill_price) or 0.0

        result = {
            "status": our,
            "filled_qty": filled_qty,
            "avg_fill": avg_fill,
            "reason": raw.get("reason") or status,
            "raw": raw,
        }

        # A broker fill with a non-finite or otherwise invalid price is not
        # executable truth. Hold before OSM/PM/exit-engine/broker side effects
        # and preserve the raw broker response as contradictory evidence.
        if our in {"FILLED", "PARTIAL_FILL", "EXIT_FILLED", "EXIT_PARTIAL_FILL"}:
            finite_avg_fill = _finite_float_or_none(avg_fill)
            if finite_avg_fill is None or finite_avg_fill <= 0:
                reason = (
                    "BROKER_FILL_NONFINITE_PRICE"
                    if avg_fill is not None and not math.isfinite(avg_fill)
                    else "BROKER_FILL_INVALID_PRICE"
                )
                client_id = str(order.get("client_id") or "default")
                payload = {
                    "local_order_id": order.get("local_order_id"),
                    "broker_order_id": broker_order_id,
                    "kind": kind,
                    "mapped_status": our,
                    "avg_fill": avg_fill,
                    "broker_reason": raw.get("reason") or status,
                }
                log.critical("[%s] %s | %s", client_id, reason, payload)
                audit(client_id, "CRITICAL", reason, payload)
                emit_fill_event(
                    order,
                    decision="ERROR",
                    reason_code=reason,
                    explanation=(
                        "Broker reported a fill/partial fill without a finite, "
                        "positive fill price; OSM transition and downstream "
                        "side effects are blocked pending a new broker poll."
                    ),
                    result=result,
                    extra_context=payload,
                )
                return {
                    **result,
                    "status": "ERROR",
                    "filled_qty": 0,
                    "reason": reason,
                }

        # PR #235 (hardening #3): broker FILLED / EXIT_FILLED with cumulative
        # filled_qty <= 0 is impossible truth for filled or partial-fill states.  Block the OSM transition and
        # emit a critical audit + fill event so operators see it.  The
        # reconciler/next broker poll will re-check on the next tick.
        if our in {"FILLED", "PARTIAL_FILL", "EXIT_FILLED", "EXIT_PARTIAL_FILL"} and (
            _strict_positive_whole_number(explicit_filled_qty)
            if explicit_filled_qty is not None
            else None
        ) is None:
            reason = "BROKER_FILLED_ZERO_QTY"
            client_id = str(order.get("client_id") or "default")
            payload = {
                "local_order_id":  order.get("local_order_id"),
                "broker_order_id": broker_order_id,
                "kind":            kind,
                "mapped_status":   our,
                "filled_qty":      filled_qty,
                "broker_reason":   raw.get("reason") or status,
            }
            log.critical("[%s] %s | %s", client_id, reason, payload)
            audit(client_id, "CRITICAL", reason, payload)
            emit_fill_event(
                order,
                decision="ERROR",
                reason_code=reason,
                explanation=(
                    "Broker reported a fill/partial fill but no positive "
                    "cumulative filled quantity; OSM transition and side "
                    "effects blocked pending next broker/reconciler pass."
                ),
                result=result,
                extra_context=payload,
            )
            return {
                **result,
                "status": "ERROR",
                "filled_qty": 0,
                "reason": reason,
            }

        if our in {"FILLED", "PARTIAL_FILL", "EXIT_FILLED", "EXIT_PARTIAL_FILL"} and (
            _strict_positive_finite_float(raw_fill_price) is None
        ):
            reason = "BROKER_FILLED_INVALID_PRICE"
            client_id = str(order.get("client_id") or "default")
            payload = {
                "local_order_id": order.get("local_order_id"),
                "broker_order_id": broker_order_id,
                "kind": kind,
                "mapped_status": our,
                "filled_qty": filled_qty,
                "avg_fill": raw_fill_price,
                "broker_reason": raw.get("reason") or status,
            }
            log.critical("[%s] %s | %s", client_id, reason, payload)
            audit(client_id, "CRITICAL", reason, payload)
            emit_fill_event(
                order,
                decision="ERROR",
                reason_code=reason,
                explanation=(
                    "Broker reported a fill/partial fill without a finite-positive "
                    "execution price; OSM transition and side effects blocked."
                ),
                result=result,
                extra_context=payload,
            )
            return {
                **result,
                "status": "ERROR",
                "filled_qty": 0,
                "reason": reason,
            }

        # PR #235 (hardening #1 + #2): on confirmed ENTRY FILLED, resolve
        # option side from orders.direction (canonical) or the OCC C/P
        # marker on the contract symbol.  If neither resolves, emit critical
        # and return ERROR — do NOT silently default to CALL and do NOT map
        # BUY/SELL.  This mutates `order` in-place so callers downstream of
        # check_order_with_broker see the canonical direction.
        if our == "FILLED" and kind == "ENTRY":
            side, source = _resolve_order_option_side(order)
            if not side:
                _emit_side_unresolved(order, reason_code="FILLED_ORDER_SIDE_UNRESOLVED")
                return {
                    **result,
                    "status": "ERROR",
                    "reason": "FILLED_ORDER_SIDE_UNRESOLVED",
                }
            order["direction"] = side
            order["_fill_monitor_side_source"] = source

        return result

    except Exception as e:
        audit(
            str(order.get("client_id") or "default"),
            "ERROR",
            "FILL_CHECK_FAILED",
            {
                "error": str(e),
                "broker_order_id": broker_order_id,
                "local_order_id": order.get("local_order_id"),
            },
        )
        return {"status": "ERROR", "filled_qty": 0, "avg_fill": 0.0, "reason": str(e), "raw": {}}


# =============================================================================
# ENTRY GUARDS / LOCK RELEASE
# =============================================================================

def _release_entry_guards(order: dict):
    """Release reserved equity and symbol lock for ENTRY orders."""
    client_id = order["client_id"]
    symbol = order.get("symbol")

    # PR #235 (hardening #5): compute reserved cost defensively but do NOT
    # let a missing cost skip the symbol lock release.  Pre-#235 an
    # early-return here would leak the entry symbol lock forever if
    # reserved_cost was None and limit_price × qty was also unavailable
    # (e.g. broker-repair rows).
    cost: Optional[float] = None
    if order.get("reserved_cost") is not None:
        try:
            cost = float(order["reserved_cost"])
        except Exception:
            cost = None

    if cost is None:
        try:
            cost = (
                float(order.get("limit_price") or 0.0)
                * int(order.get("qty") or 0)
                * OPT_MULTIPLIER
            )
        except Exception:
            cost = None

    if cost and cost > 0:
        release_equity(client_id, cost)
    else:
        log.warning(
            "[%s] _release_entry_guards: cost is zero/unknown for order=%s — equity may not be fully released",
            order.get("client_id"), order.get("local_order_id"),
        )

    # Always release the symbol lock, regardless of cost resolution.
    if symbol:
        release_symbol_lock(client_id, symbol)
    else:
        log.warning(
            "[%s] _release_entry_guards: symbol missing for order=%s — symbol lock could not be released",
            order.get("client_id"), order.get("local_order_id"),
        )


# =============================================================================
# PAIR MANAGER HELPER — BROKER CANCEL FIRST
# =============================================================================

def _cancel_pair_opposite(order: dict, broker: BrokerAdapter, osm, alert_fn=None) -> None:
    """
    On ENTRY fill: cancel the opposite side of a 1-1 pair.

    Broker cancel is attempted before local CANCELED transition.
    If broker id cannot be resolved, local order is not marked canceled.
    """
    if not osm:
        return

    try:
        from ap.signal_pair_manager import get_pair_manager

        # PR #235 (hardening #2): resolve side from order/OCC — do NOT default
        # to CALL when direction is missing.  If side is unresolvable, we
        # cannot safely reason about which pair-opposite to cancel.
        _pair_side, _pair_side_source = _resolve_order_option_side(order)
        if not _pair_side:
            _emit_side_unresolved(order, reason_code="PAIR_CANCEL_SIDE_UNRESOLVED", alert_fn=alert_fn)
            return

        pair_manager = get_pair_manager()
        ticker = (order.get("symbol") or "").upper()
        side = _pair_side
        filled_local_id = order.get("local_order_id", "")

        cancel_local_id = pair_manager.on_fill(
            ticker=ticker,
            side=side,
            local_order_id=filled_local_id,
        )

        if not cancel_local_id:
            return

        log.warning(
            "[%s] 1-1 PAIR FILL — canceling opposite local_order_id=%s",
            ticker,
            cancel_local_id,
        )

        resolved_broker_id = None
        try:
            existing = osm.get_order(cancel_local_id)
            resolved_broker_id = (existing or {}).get("broker_order_id")
        except Exception as exc:
            log.warning("[%s] Could not resolve opposite broker id: %s", ticker, exc)

        if not resolved_broker_id:
            msg = f"PAIR_CANCEL_SKIPPED_NO_BROKER_ID | {ticker} | opposite_local={cancel_local_id} | filled_local={filled_local_id}"
            log.warning("[%s] %s", ticker, msg)
            audit(
                str(order.get("client_id") or "default"),
                "CRITICAL",
                "PAIR_CANCEL_SKIPPED_NO_BROKER_ID",
                {
                    "filled_local_order_id": filled_local_id,
                    "opposite_local_order_id": cancel_local_id,
                    "symbol": ticker,
                    "side": side,
                    "reason": "opposite order has no broker_order_id",
                },
            )
            emit_fill_event(
                order,
                decision="ERROR",
                reason_code="PAIR_CANCEL_SKIPPED_NO_BROKER_ID",
                explanation=msg,
                result={},
                extra_context={
                    "filled_local_order_id": filled_local_id,
                    "opposite_local_order_id": cancel_local_id,
                },
            )
            _safe_alert(alert_fn, f"[fill_monitor:{order.get('client_id')}] {msg}")
            return

        try:
            if hasattr(broker, "cancel_order"):
                broker.cancel_order(resolved_broker_id)
            else:
                _cancel_with_session(broker, resolved_broker_id)
        except Exception as exc:
            msg = (
                f"PAIR_CANCEL_BROKER_FAILED | {ticker} | opposite_local={cancel_local_id} "
                f"broker={resolved_broker_id} error={exc}"
            )
            log.warning("[%s] %s", ticker, msg)
            audit(
                str(order.get("client_id") or "default"),
                "CRITICAL",
                "PAIR_CANCEL_BROKER_FAILED",
                {
                    "filled_local_order_id": filled_local_id,
                    "opposite_local_order_id": cancel_local_id,
                    "opposite_broker_order_id": resolved_broker_id,
                    "symbol": ticker,
                    "side": side,
                    "error": str(exc),
                },
            )
            emit_fill_event(
                order,
                decision="ERROR",
                reason_code="PAIR_CANCEL_BROKER_FAILED",
                explanation=msg,
                result={},
                extra_context={
                    "filled_local_order_id": filled_local_id,
                    "opposite_local_order_id": cancel_local_id,
                    "opposite_broker_order_id": resolved_broker_id,
                },
            )
            _safe_alert(alert_fn, f"[fill_monitor:{order.get('client_id')}] {msg}")
            return

        ok = osm.transition(
            cancel_local_id,
            "CANCELED",
            broker_order_id=resolved_broker_id,
            last_error="pair_fill_cancel_broker_confirmed",
        )
        if ok:
            log.info(
                "[%s] Opposite side CANCELED | local=%s broker=%s",
                ticker,
                cancel_local_id,
                resolved_broker_id,
            )
            audit(
                str(order.get("client_id") or "default"),
                "INFO",
                "PAIR_CANCEL_CONFIRMED",
                {
                    "filled_local_order_id": filled_local_id,
                    "opposite_local_order_id": cancel_local_id,
                    "opposite_broker_order_id": resolved_broker_id,
                    "symbol": ticker,
                    "side": side,
                },
            )

    except ImportError:
        pass
    except Exception as exc:
        msg = f"PAIR_CANCEL_MANAGER_FAILED | {order.get('symbol','?')} | local={order.get('local_order_id')} error={exc}"
        log.warning(msg)
        audit(
            str(order.get("client_id") or "default"),
            "WARNING",
            "PAIR_CANCEL_MANAGER_FAILED",
            {
                "local_order_id": order.get("local_order_id"),
                "broker_order_id": order.get("broker_order_id"),
                "symbol": order.get("symbol"),
                "error": str(exc),
            },
        )
        _safe_alert(alert_fn, f"[fill_monitor:{order.get('client_id')}] {msg}")


def _cancel_with_session(broker: BrokerAdapter, broker_order_id: str):
    base_url = (
        getattr(broker, "base_url", None)
        or getattr(getattr(broker, "cfg", None), "base_url", None)
        or getattr(broker, "_base_url", None)
    )
    account_id = (
        getattr(broker, "account_id", None)
        or getattr(getattr(broker, "cfg", None), "account_id", None)
        or getattr(broker, "_account_id", None)
    )
    if not base_url or not account_id or not getattr(broker, "session", None):
        raise RuntimeError("broker cancel not available: missing base_url/account_id/session")

    resp = broker.session.delete(
        f"{base_url}/v1/accounts/{account_id}/orders/{broker_order_id}",
        headers={"Accept": "application/json"},
        timeout=10,
    )
    if getattr(resp, "status_code", 500) >= 300:
        raise RuntimeError(f"broker cancel failed HTTP {resp.status_code}: {getattr(resp, 'text', '')[:200]}")


# =============================================================================
# SAFE HELPERS
# =============================================================================


def _extract_underlying_from_contract(contract: str, fallback: str = "") -> str:
    """
    Extract the underlying root from an OCC option symbol.

    Example:
        META260515C00615000 -> META

    This prevents the corrupted ticker bug where slicing an OCC symbol produced
    invalid underlyings such as META26, causing quote monitors to fetch a ticker
    that does not exist.
    """
    import re as _re
    sym = str(contract or "").upper().strip()
    fallback = str(fallback or "").upper().strip()
    if sym:
        m = _re.search(r"\d{6}[CP]", sym)
        if m:
            root = sym[:m.start()].strip()
            if root:
                return root
        root = _re.sub(r"\d+$", "", sym).strip()
        if root:
            return root
    return fallback


def _get_underlying_price_at_fill(broker: BrokerAdapter, ticker: str) -> float:
    """
    Capture underlying price at the moment an entry fill is confirmed.

    FIX: the original implementation only tried abstract method names that
    TradierBroker does not expose, silently returning 0.0 every time.
    This caused underlying_entry = NULL in the positions table, disabling
    all underlying-based exit logic (stop_hit, target_hit, progress exit).

    Strategy (in priority order):
      1. Abstract broker helper methods (forwards-compatible with any broker)
      2. Tradier REST API via broker.session (guaranteed path for TradierBroker)
      3. Hard fallback: 0.0 — now logged as WARNING so failures are never silent
    """
    if not broker or not ticker:
        return 0.0

    ticker = str(ticker).upper().strip()

    # ── 1. Abstract broker helpers ────────────────────────────────────────────
    for method_name in ("get_quote", "get_underlying_price", "get_last_price", "quote"):
        method = getattr(broker, method_name, None)
        if not callable(method):
            continue
        try:
            result = method(ticker)
            if isinstance(result, (int, float)) and float(result) > 0:
                return float(result)
            if isinstance(result, dict):
                for key in ("last", "last_price", "price", "mark", "bid", "close"):
                    val = result.get(key)
                    if val is not None:
                        try:
                            fv = float(val)
                            if fv > 0:
                                return fv
                        except Exception:
                            pass
        except Exception:
            continue

    # ── 2. Tradier REST API via broker session (primary path for TradierBroker) ─
    # TradierBroker exposes .session (requests.Session) and .cfg.base_url.
    # Using the broker's existing authenticated session avoids credential duplication.
    try:
        session = getattr(broker, "session", None)
        cfg = getattr(broker, "cfg", None)
        base_url = (
            getattr(cfg, "base_url", None)
            or getattr(cfg, "baseurl", None)
            or getattr(broker, "base_url", None)
            or getattr(broker, "_base_url", None)
        )
        if session and base_url:
            resp = session.get(
                f"{base_url}/v1/markets/quotes",
                params={"symbols": ticker, "greeks": "false"},
                headers={"Accept": "application/json"},
                timeout=4,
            )
            if getattr(resp, "status_code", 500) < 300:
                data = resp.json() or {}
                raw = data.get("quotes", {}).get("quote", {})
                if isinstance(raw, list):
                    raw = raw[0] if raw else {}
                if isinstance(raw, dict):
                    for key in ("last", "ask", "bid", "close", "prevclose"):
                        val = raw.get(key)
                        if val is not None:
                            try:
                                fv = float(val)
                                if fv > 0:
                                    log.debug(
                                        "[fill_monitor] underlying_entry=%s for %s "
                                        "via Tradier REST (%s)",
                                        fv, ticker, key,
                                    )
                                    return fv
                            except Exception:
                                pass
    except Exception as exc:
        log.debug(
            "[fill_monitor] Tradier REST underlying price fetch failed for %s: %s",
            ticker, exc,
        )

    # ── 3. Hard fallback ───────────────────────────────────────────────────────
    log.warning(
        "[fill_monitor] _get_underlying_price_at_fill: could not fetch price for '%s' "
        "— underlying_entry will be NULL. "
        "Check broker session/base_url are accessible at fill time.",
        ticker,
    )
    return 0.0

def _call_with_supported_kwargs(fn, **kwargs):
    """Call a function with only supported keyword args for compatibility."""
    sig = inspect.signature(fn)
    supported = {k: v for k, v in kwargs.items() if k in sig.parameters}
    return fn(**supported)


def _get_existing_position_by_order(pm, local_order_id: str, broker_order_id: Optional[str]):
    for method_name, arg in (
        ("get_position_by_local_order", local_order_id),
        ("get_position_by_broker_order", broker_order_id),
    ):
        if not arg:
            continue
        method = getattr(pm, method_name, None)
        if callable(method):
            try:
                found = method(arg)
                if found:
                    return found
            except Exception as _resolve_err:
                log.debug("Method %s failed during resolution: %s", method.__name__ if hasattr(method, "__name__") else method, _resolve_err)
    return None


_TERMINAL_POSITION_STATUSES = frozenset({
    "CLOSED",
    "EXPIRED",
    "STOPPED",
    "TAKEN_PROFIT",
    "ERROR",
})


def _position_value(position, name: str, default=None):
    if isinstance(position, dict):
        return position.get(name, default)
    return getattr(position, name, default)


def _existing_position_is_terminal(position) -> bool:
    status = str(_position_value(position, "status", "") or "").strip().upper()
    if status in _TERMINAL_POSITION_STATUSES:
        return True
    if bool(_position_value(position, "closed", False)):
        return True
    remaining = _position_value(position, "quantity_remaining", None)
    if remaining is not None:
        try:
            return int(remaining or 0) <= 0
        except (TypeError, ValueError):
            return True
    return False


def _open_position_safe(
    pm,
    *,
    order: dict,
    result: dict,
    plan_id: str,
    signal_id: str,
    local_id: str,
    broker: BrokerAdapter = None,
    quote_broker=None,
) -> Optional[str]:
    """Open position with idempotency keys when the PM supports them.

    PR #235 (hardening #1 + #2 + #4 + #6):
      - Resolve side without guessing; fail closed on unresolved.
      - Use quote_broker (or broker.data_broker) for the underlying-at-fill
        quote read.
      - Persist orders.last_error = FILLED_ORDER_POSITION_CREATE_FAILED when
        position creation raises or returns falsy.
    """
    entry_price = _finite_float_or_none(result.get("avg_fill"))
    if entry_price is None or entry_price <= 0:
        # Do not invoke the PM or persist a downstream failure for an
        # unexecutable broker price. The caller's admission fence normally
        # catches this first; keep the helper safe for direct callers too.
        return None

    existing = _get_existing_position_by_order(pm, local_id, order.get("broker_order_id"))
    if existing:
        if _existing_position_is_terminal(existing):
            log.critical(
                "[%s] FILLED_ENTRY_CANONICAL_OWNER_RETRY_TERMINAL_POSITION | "
                "local=%s broker=%s position=%s status=%s",
                order.get("client_id"),
                local_id,
                order.get("broker_order_id"),
                _position_value(existing, "id", "") or _position_value(existing, "position_id", ""),
                _position_value(existing, "status", ""),
            )
            return None
        return _position_value(existing, "id") or _position_value(existing, "position_id")

    # PR #235: resolve canonical CALL/PUT — never default to CALL.
    _side, _side_source = _resolve_order_option_side(order)
    if not _side:
        _emit_side_unresolved(order, reason_code="POSITION_OPEN_SIDE_UNRESOLVED")
        _record_position_create_failure(order, "FILLED_ORDER_POSITION_CREATE_FAILED")
        return None

    # PR #235: pick an explicit data broker for the underlying-at-fill quote
    # when the caller supplied one (or attached one to the execution broker).
    _quote_broker = _select_quote_broker(broker, quote_broker)
    if _quote_broker is not broker:
        audit(
            str(order.get("client_id") or "default"),
            "INFO",
            "FILL_MONITOR_UNDERLYING_ENTRY_QUOTE_BROKER_SELECTED",
            {
                "local_order_id":            order.get("local_order_id"),
                "broker_order_id":           order.get("broker_order_id"),
                "symbol":                    order.get("symbol"),
                "contract":                  order.get("contract"),
                "underlying_entry_source":   "data_broker",
                "quote_broker_base_url":     _broker_base_url(_quote_broker),
                "execution_broker_base_url": _broker_base_url(broker),
                "quote_broker_is_data_broker": True,
                "side":                      _side,
                "side_source":               _side_source,
            },
        )

    contract = order.get("contract") or order.get("symbol") or ""
    ticker = _extract_underlying_from_contract(contract, fallback=order.get("symbol") or "")
    underlying_entry = _safe_float(
        order.get("underlying_entry")
        or order.get("entry_underlying")
        or order.get("last_underlying_price")
        or order.get("trigger_price")
        or 0.0
    )
    if underlying_entry <= 0:
        underlying_entry = _get_underlying_price_at_fill(_quote_broker, ticker)

    kwargs = {
        "plan_id": plan_id,
        "signal_id": signal_id,
        "ticker": ticker,
        "contract": contract,
        "side": _side,
        "qty": int(result.get("filled_qty") or order.get("qty") or 0),
        "entry_price": entry_price,
        "underlying_entry": underlying_entry if underlying_entry > 0 else None,
        "tier": str(order.get("tier") or "B"),
        "score": float(order.get("score") or 0),
        "pattern": str(order.get("pattern") or ""),
        "stop_underlying": float(order.get("stop_underlying")) if order.get("stop_underlying") is not None else None,
        "target_underlying": float(order.get("target_underlying")) if order.get("target_underlying") is not None else None,
        "local_order_id": local_id,
        "broker_order_id": order.get("broker_order_id"),
        "execution_mode": order.get("execution_mode"),
    }

    try:
        position_id = pm.open_position(**kwargs)
    except TypeError:
        # Compatibility with older PM signature that does not yet accept every field.
        try:
            position_id = _call_with_supported_kwargs(pm.open_position, **kwargs)
        except Exception:
            _record_position_create_failure(order, "FILLED_ORDER_POSITION_CREATE_FAILED")
            raise
    except Exception:
        _record_position_create_failure(order, "FILLED_ORDER_POSITION_CREATE_FAILED")
        raise

    if not position_id:
        _record_position_create_failure(order, "FILLED_ORDER_POSITION_CREATE_FAILED")
    return position_id


def _bind_filled_entry_position_id(order: dict, canonical_position_id: str) -> dict:
    """Bind and prove the canonical position id on one filled ENTRY row.

    The fill monitor must prove this durable link before the exit engine is
    allowed to treat the in-memory owner as canonical.  The UPDATE is fenced
    by exact client/local/broker/contract/kind/mode identity and never
    overwrites a different existing position id.  The durable position row is
    checked first, and an order readback is mandatory because a zero-row
    update can mean either an idempotent already-bound row or an identity
    conflict.
    """
    client_id = str(order.get("client_id") or "").strip()
    local_order_id = str(order.get("local_order_id") or "").strip()
    broker_order_id = str(order.get("broker_order_id") or "").strip()
    contract = str(order.get("contract") or "").strip().upper()
    position_id = str(canonical_position_id or "").strip()
    kind = str(order.get("kind") or "").strip().upper()
    mode_state, execution_mode = _mode_source_state(order.get("execution_mode"))

    def _failure(detail_reason: str, **extra) -> dict:
        return {
            "ok": False,
            "disposition": "BIND_FAILED",
            "reason_code": "FILLED_ENTRY_POSITION_IDENTITY_BIND_FAILED",
            "detail_reason": detail_reason,
            "client_id": client_id,
            "local_order_id": local_order_id,
            "broker_order_id": broker_order_id,
            "contract": contract,
            "position_id": position_id,
            "execution_mode": execution_mode,
            **extra,
        }

    if not client_id:
        return _failure("client_id_missing")
    if not local_order_id:
        return _failure("local_order_id_missing")
    if not _has_proven_broker_order_id(broker_order_id):
        return _failure("broker_order_id_missing_or_invalid")
    if not contract:
        return _failure("contract_missing")
    if not position_id:
        return _failure("canonical_position_id_missing")
    if kind != "ENTRY":
        return _failure("kind_not_ENTRY", kind=kind)
    if mode_state != "valid":
        return _failure("execution_mode_missing_or_invalid")

    def _bind_row():
        with conn() as c:
            position_row = c.execute(
                "SELECT id, client_id, contract, execution_mode "
                "FROM positions WHERE id=%s AND client_id=%s LIMIT 1",
                (position_id, client_id),
            ).fetchone()
            if not position_row:
                return {
                    "ok": False,
                    "detail_reason": "canonical_position_row_missing",
                }

            position_client = str(position_row.get("client_id") or "").strip()
            position_contract = str(position_row.get("contract") or "").strip().upper()
            position_mode_state, position_mode = _mode_source_state(
                position_row.get("execution_mode")
            )
            position_row_id = str(position_row.get("id") or "").strip()
            if (
                position_row_id != position_id
                or position_client != client_id
                or position_contract != contract
                or position_mode_state != "valid"
                or position_mode != execution_mode
            ):
                return {
                    "ok": False,
                    "detail_reason": "canonical_position_identity_mismatch",
                    "position_row_id": position_row_id,
                    "position_row_client_id": position_client,
                    "position_row_contract": position_contract,
                    "position_row_execution_mode": position_mode,
                }

            result = c.execute(
                "UPDATE orders SET position_id=%s, "
                "meta=COALESCE(meta, '{}'::jsonb) || %s::jsonb, "
                "updated_ts=NOW() "
                "WHERE client_id=%s AND local_order_id=%s "
                "AND broker_order_id=%s AND contract=%s "
                "AND kind='ENTRY' "
                "AND LOWER(BTRIM(COALESCE(execution_mode,'')))=%s "
                "AND (position_id IS NULL OR BTRIM(position_id)='')",
                (
                    position_id,
                    json_dumps({_CANONICAL_OWNER_HANDOFF_ENTRY_PROVEN_KEY: False}),
                    client_id,
                    local_order_id,
                    broker_order_id,
                    contract,
                    execution_mode,
                ),
            )
            return {
                "ok": True,
                "rowcount": int(
                    getattr(result, "rowcount", getattr(c, "rowcount", 0)) or 0
                ),
            }

    def _read_row():
        with conn() as c:
            return c.execute(
                "SELECT client_id, local_order_id, broker_order_id, contract, "
                "kind, execution_mode, position_id, meta "
                "FROM orders WHERE client_id=%s AND local_order_id=%s "
                "AND broker_order_id=%s AND contract=%s AND kind='ENTRY' LIMIT 1",
                (client_id, local_order_id, broker_order_id, contract),
            ).fetchone()

    try:
        bind_outcome = run_with_retry(_bind_row)
        if not bind_outcome.get("ok"):
            return _failure(
                bind_outcome.get("detail_reason") or "canonical_position_not_proven",
                **{
                    key: value
                    for key, value in bind_outcome.items()
                    if key not in {"ok", "detail_reason"}
                },
            )
        rowcount = int(bind_outcome.get("rowcount") or 0)
        row = run_with_retry(_read_row)
    except Exception as exc:
        return _failure(
            "database_error",
            exception_type=type(exc).__name__,
            exception=str(exc),
        )

    if not row:
        return _failure("order_row_missing_after_bind", rowcount=rowcount)

    row_client = str(row.get("client_id") or "").strip()
    row_local = str(row.get("local_order_id") or "").strip()
    row_broker = str(row.get("broker_order_id") or "").strip()
    row_contract = str(row.get("contract") or "").strip().upper()
    row_kind = str(row.get("kind") or "").strip().upper()
    row_mode_state, row_mode = _mode_source_state(row.get("execution_mode"))
    row_position = str(row.get("position_id") or "").strip()
    row_meta = row.get("meta") if isinstance(row.get("meta"), dict) else {}
    row_handoff_proven = row_meta.get(_CANONICAL_OWNER_HANDOFF_ENTRY_PROVEN_KEY)
    row_handoff_proven_is_false = row_handoff_proven is False or str(
        row_handoff_proven or ""
    ).strip().lower() == "false"

    if (
        row_client != client_id
        or row_local != local_order_id
        or row_broker != broker_order_id
        or row_contract != contract
        or row_kind != "ENTRY"
        or row_mode_state != "valid"
        or row_mode != execution_mode
    ):
        return _failure(
            "stored_order_identity_mismatch",
            row_client_id=row_client,
            row_local_order_id=row_local,
            row_broker_order_id=row_broker,
            row_contract=row_contract,
            row_kind=row_kind,
            row_execution_mode=row_mode,
            rowcount=rowcount,
        )

    if rowcount > 0 and not row_handoff_proven_is_false:
        return _failure(
            "handoff_pending_marker_not_proven_after_bind",
            rowcount=rowcount,
            row_meta=row_meta,
        )

    if row_position == position_id:
        return {
            "ok": True,
            "disposition": "BOUND" if rowcount else "ALREADY_BOUND",
            "reason_code": "FILLED_ENTRY_POSITION_IDENTITY_BOUND",
            "client_id": client_id,
            "local_order_id": local_order_id,
            "broker_order_id": broker_order_id,
            "contract": contract,
            "position_id": position_id,
            "execution_mode": execution_mode,
            "rowcount": rowcount,
        }

    if row_position:
        return _failure(
            "POSITION_IDENTITY_CONFLICT",
            existing_position_id=row_position,
            rowcount=rowcount,
        )
    return _failure("position_id_not_proven_after_readback", rowcount=rowcount)


def _place_standing_stop_best_effort(
    *,
    broker: BrokerAdapter,
    order: dict,
    qty: int,
    entry_price: float,
):
    """Submit the optional broker stop and classify the broker outcome.

    This helper intentionally does not decide whether a retry may submit.  The
    caller must first claim the durable standing-stop intent.  Exceptions after
    a broker call are ``OUTCOME_UNPROVEN`` because the broker may have accepted
    the order before the process lost its response.
    """
    def _outcome(outcome: str, *, detail_reason: str = "", **extra) -> dict:
        return {
            "ok": outcome == "SUBMITTED",
            "outcome": outcome,
            "detail_reason": detail_reason,
            **extra,
        }

    try:
        entry_price = _finite_float_or_none(entry_price)
        if qty <= 0 or entry_price is None or entry_price <= 0:
            return _outcome("FAILED", detail_reason="invalid_qty_or_entry_price")

        try:
            stop_pct = float(os.getenv("BROKER_STANDING_STOP_PCT", "0.30"))
        except (TypeError, ValueError):
            return _outcome("FAILED", detail_reason="invalid_stop_percentage")
        stop_px = round(entry_price * (1 - stop_pct), 2)
        contract = order.get("contract", "")
        ticker = (order.get("symbol") or "").upper()

        def _classify_helper_response(stop_resp):
            if stop_resp is True:
                return _outcome(
                    "OUTCOME_UNPROVEN",
                    detail_reason="broker_helper_success_without_order_id",
                )
            if stop_resp is False:
                return _outcome("FAILED", detail_reason="broker_helper_rejected")
            if not isinstance(stop_resp, dict):
                return _outcome(
                    "OUTCOME_UNPROVEN",
                    detail_reason="broker_helper_response_unrecognized",
                )

            stop_id = (
                stop_resp.get("id")
                or stop_resp.get("order_id")
                or stop_resp.get("broker_order_id")
            )
            stop_stat = str(
                stop_resp.get("status") or stop_resp.get("state") or ""
            ).strip().lower()
            if stop_resp.get("success") is False or stop_stat in {
                "rejected",
                "reject",
                "failed",
                "failure",
                "error",
                "canceled",
                "cancelled",
            }:
                return _outcome(
                    "FAILED",
                    detail_reason="broker_helper_rejected",
                    broker_stop_status=stop_stat,
                )
            if _has_proven_standing_stop_order_id(stop_id):
                return _outcome(
                    "SUBMITTED",
                    broker_stop_id=str(stop_id).strip(),
                    broker_stop_status=stop_stat or "unknown",
                )
            return _outcome(
                "OUTCOME_UNPROVEN",
                detail_reason="broker_helper_response_missing_order_id",
                broker_stop_status=stop_stat or "unknown",
            )

        if hasattr(broker, "place_stop_order"):
            try:
                stop_resp = broker.place_stop_order(
                    symbol=contract, qty=qty, stop_price=stop_px
                )
            except Exception as exc:
                return _outcome(
                    "OUTCOME_UNPROVEN",
                    detail_reason="broker_helper_exception",
                    exception_type=type(exc).__name__,
                    exception=str(exc),
                )
            result = _classify_helper_response(stop_resp)
            if result.get("outcome") == "SUBMITTED":
                log.info(
                    "[%s] Standing stop placed via broker helper @ $%.2f | "
                    "broker_stop=%s status=%s",
                    ticker,
                    stop_px,
                    result.get("broker_stop_id") or "?",
                    result.get("broker_stop_status") or "unknown",
                )
                audit(
                    order["client_id"],
                    "INFO",
                    "STOP_ORDER_PLACED",
                    {
                        "local_order_id": order.get("local_order_id"),
                        "ticker": ticker,
                        "contract": contract,
                        "qty": int(qty),
                        "stop_px": stop_px,
                        "broker_stop_order_id": result.get("broker_stop_id"),
                        "broker_stop_status": result.get(
                            "broker_stop_status", "unknown"
                        ),
                        "source": "broker_helper",
                    },
                )
            return result

        base_url = (
            getattr(broker, "base_url", None)
            or getattr(getattr(broker, "cfg", None), "base_url", None)
            or getattr(broker, "_base_url", None)
        )
        account_id = (
            getattr(broker, "account_id", None)
            or getattr(getattr(broker, "cfg", None), "account_id", None)
            or getattr(broker, "_account_id", None)
        )

        if not base_url or not account_id or not getattr(broker, "session", None):
            log.warning("[%s] Standing stop skipped — broker stop interface unavailable", ticker)
            return _outcome("FAILED", detail_reason="broker_stop_interface_unavailable")

        try:
            resp = broker.session.post(
                f"{base_url}/v1/accounts/{account_id}/orders",
                data={
                    "class": "option",
                    "option_symbol": contract,
                    "side": "sell_to_close",
                    "quantity": qty,
                    "type": "stop",
                    "stop": stop_px,
                    "duration": "gtc",
                },
                headers={"Accept": "application/json"},
                timeout=10,
            )
        except Exception as exc:
            return _outcome(
                "OUTCOME_UNPROVEN",
                detail_reason="broker_rest_exception",
                exception_type=type(exc).__name__,
                exception=str(exc),
            )

        if resp.status_code < 300:
            try:
                payload = resp.json() or {}
                stop_data = payload.get("order", {}) or {}
                if not isinstance(stop_data, dict):
                    return _outcome(
                        "OUTCOME_UNPROVEN",
                        detail_reason="broker_rest_response_unrecognized",
                    )
                stop_id = (
                    stop_data.get("id")
                    or stop_data.get("order_id")
                    or stop_data.get("broker_order_id")
                    or ""
                )
                stop_stat = stop_data.get("status", "unknown")
            except Exception as exc:
                return _outcome(
                    "OUTCOME_UNPROVEN",
                    detail_reason="broker_rest_response_parse_failed",
                    exception_type=type(exc).__name__,
                    exception=str(exc),
                )
            if not _has_proven_standing_stop_order_id(stop_id):
                return _outcome(
                    "OUTCOME_UNPROVEN",
                    detail_reason="broker_rest_response_missing_order_id",
                    broker_stop_status=str(stop_stat or "unknown"),
                )
            log.info(
                "[%s] Standing stop placed @ $%.2f | broker_stop=%s status=%s",
                ticker,
                stop_px,
                stop_id,
                stop_stat,
            )
            audit(
                order["client_id"],
                "INFO",
                "STOP_ORDER_PLACED",
                {
                    "local_order_id": order.get("local_order_id"),
                    "ticker": ticker,
                    "contract": contract,
                    "qty": int(qty),
                    "stop_px": stop_px,
                    "broker_stop_order_id": stop_id,
                    "broker_stop_status": stop_stat,
                    "source": "rest",
                },
            )
            return _outcome(
                "SUBMITTED",
                broker_stop_id=str(stop_id or "").strip(),
                broker_stop_status=str(stop_stat or "unknown"),
            )
        else:
            err_body = getattr(resp, "text", "")[:200]
            log.warning("[%s] Standing stop FAILED — exit engine sole protection | %s", ticker, err_body)
            audit(
                order["client_id"],
                "WARNING",
                "STOP_ORDER_FAILED",
                {
                    "local_order_id": order.get("local_order_id"),
                    "ticker": ticker,
                    "stop_px": stop_px,
                    "body": err_body,
                },
            )
            return _outcome(
                "FAILED",
                detail_reason="broker_rest_rejected",
                broker_http_status=getattr(resp, "status_code", None),
            )

    except Exception as exc:
        log.warning("[%s] Standing stop placement error: %s", order.get("symbol", "?"), exc)
        return {
            "ok": False,
            "outcome": "OUTCOME_UNPROVEN",
            "detail_reason": "standing_stop_unexpected_exception",
            "exception_type": type(exc).__name__,
            "exception": str(exc),
        }


def _load_managed_position_class():
    """Resolve ManagedPosition across legacy/hardened exit-engine module paths."""
    configured = os.getenv("AP_MANAGED_POSITION_MODULE", "").strip()
    candidates = []
    if configured:
        candidates.append(configured)

    # Keep all known paths so fill-monitor seeding does not break during repo renames.
    candidates.extend([
        "ap_exit_engine",
        "ap.exit_engine",
        "ap.exitengine",
        "ap.exit_engine_hardened",
    ])

    seen = set()
    last_error = None
    for module_name in candidates:
        if not module_name or module_name in seen:
            continue
        seen.add(module_name)
        try:
            module = importlib.import_module(module_name)
            cls = getattr(module, "ManagedPosition", None)
            if cls is not None:
                return cls
            last_error = RuntimeError(f"{module_name}.ManagedPosition missing")
        except Exception as exc:
            last_error = exc

    raise ImportError(f"Could not import ManagedPosition from known exit-engine paths: {last_error}")


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _normalize_execution_mode_token(value) -> str:
    """Recognize only lowercase "live" / "paper".  Never infer, never default."""
    _tok = str(value or "").strip().lower()
    return _tok if _tok in {"live", "paper"} else ""


def _resolve_canonical_adoption_execution_mode(exit_engine, order: dict):
    """Resolve the exact execution_mode to pass into
    adopt_canonical_position_identity, or fail closed.

    AMENDMENT (PR #385 review — Fix A): consult the ENGINE's structured
    resolution status, not a bare mode string.  The bare string is ""
    for BOTH "no evidence" and "contradictory evidence"; treating them
    the same lets a known-good order mode launder a genuine engine
    conflict.  We now branch on `_resolved_execution_mode_detail()`:

      engine PROVEN   → reconcile against order mode; disagree ⇒ CONFLICT
      engine CONFLICT → fail closed regardless of order mode
      engine UNPROVEN → a normalized known order mode may be used;
                        otherwise UNPROVEN

    The existing `_resolved_execution_mode()` contract remains unchanged
    for all its other callers.

    Returns a 3-tuple (mode, disposition, diagnostics):
      mode        — "live" | "paper" | ""  ("" means "do not adopt")
      disposition — "OK" | "CONFLICT" | "UNPROVEN"
      diagnostics — dict with order_mode / engine_mode / engine_status
                    / engine_sources for logs and audit records.
    """
    order_mode = _normalize_execution_mode_token(order.get("execution_mode"))

    engine_status = "UNPROVEN"
    engine_mode = ""
    engine_sources: dict = {}
    _detail_fn = getattr(exit_engine, "_resolved_execution_mode_detail", None)
    if callable(_detail_fn):
        try:
            _d = _detail_fn() or {}
            engine_mode = _normalize_execution_mode_token(_d.get("mode"))
            _s = str(_d.get("status") or "").upper()
            if _s in {"PROVEN", "CONFLICT", "UNPROVEN"}:
                engine_status = _s
            engine_sources = dict(_d.get("sources") or {})
        except Exception:
            # Structured detail unavailable — do NOT silently downgrade
            # to plain-mode inference; treat as UNPROVEN.
            engine_status = "UNPROVEN"
            engine_mode = ""
    else:
        # Older engine without the detail helper: fall back to the plain
        # resolver.  Its "" cannot distinguish CONFLICT from UNPROVEN, so
        # we conservatively treat "" as UNPROVEN here.  Real production
        # engines always expose the detail helper.
        _plain = getattr(exit_engine, "_resolved_execution_mode", None)
        if callable(_plain):
            try:
                engine_mode = _normalize_execution_mode_token(_plain())
            except Exception:
                engine_mode = ""
            engine_status = "PROVEN" if engine_mode else "UNPROVEN"

    diag = {
        "order_mode": order_mode,
        "engine_mode": engine_mode,
        "engine_status": engine_status,
        "engine_sources": engine_sources,
    }

    # Engine CONFLICT must ALWAYS fail closed — a known-good order mode
    # cannot mask contradictory engine evidence.
    if engine_status == "CONFLICT":
        return "", "CONFLICT", diag

    if engine_status == "PROVEN":
        if order_mode and order_mode != engine_mode:
            return "", "CONFLICT", diag
        return engine_mode, "OK", diag

    # Engine UNPROVEN.
    if order_mode:
        return order_mode, "OK", diag
    return "", "UNPROVEN", diag


def _classify_existing_protective_owner(
    *,
    exit_engine,
    contract: str,
    client_id: str,
    expected_mode: str,
) -> tuple[str, int, int]:
    """Classify whether an exact protective owner exists for this order.

    AMENDMENT (PR #385 review — every-failure-path owner truthfulness):
    used by mode-fail-closed, RETRY_* dispositions, and adoption
    exception paths so every "protective monitoring retained" claim is
    actually verified.

    A protective owner is PROVEN only when a behavior-active position
    matches:
      - contract EXACTLY (already canonical uppercase);
      - normalized nonblank client_id equal to the order's client_id;
      - execution_mode is exactly "live" or "paper";
      - AND, when `expected_mode` is proven ("live"/"paper"), the
        position's execution_mode equals it (mode-scoped ownership).

    Otherwise:
      - OWNER_IDENTITY_UNPROVEN: candidates exist (same client+contract)
        but their execution_mode is blank / malformed / mismatched.
      - NO_OWNER: zero same-client same-contract candidates.
      - OWNER_LOOKUP_FAILED: active_positions() raised.

    Returns (state, proven_count, unproven_count).
    """
    _contract = str(contract or "").upper().strip()
    _expected_client = str(client_id or "").strip().lower()
    _expected_mode = str(expected_mode or "").strip().lower()
    _expected_mode_proven = _expected_mode in {"live", "paper"}
    if not _expected_mode_proven:
        _expected_mode = ""

    if not _contract or not _expected_client:
        return "NO_OWNER", 0, 0

    _active_fn = getattr(exit_engine, "active_positions", None)
    if not callable(_active_fn):
        return "NO_OWNER", 0, 0

    try:
        _actives = _active_fn() or []
    except Exception as _err:
        log.warning(
            "[%s] owner-existence check failed for %s: %s",
            client_id, _contract, _err,
        )
        return "OWNER_LOOKUP_FAILED", 0, 0

    try:
        from ap_exit_engine import _classify_canonical_repair_owner_domain
    except Exception:
        _classify_canonical_repair_owner_domain = None

    _proven = 0
    _unproven = 0
    for _p in _actives:
        _sym = str(getattr(_p, "option_symbol", "") or "").upper().strip()
        if _sym != _contract:
            continue
        _pid = str(getattr(_p, "position_id", "") or "").strip()
        if _classify_canonical_repair_owner_domain and _pid.startswith(
            "broker-repair-"
        ):
            _repair_domain = _classify_canonical_repair_owner_domain(
                _p,
                client_id=_expected_client,
                execution_mode=_expected_mode,
                contract=_contract,
            )
            if _repair_domain == "IDENTITY_UNPROVEN":
                _unproven += 1
                continue
            if _repair_domain == "PROVEN_FOREIGN_DOMAIN":
                continue
        _p_client = str(getattr(_p, "client_id", "") or "").strip().lower()
        if not _p_client or _p_client != _expected_client:
            # Blank / different-client contract match is not exact
            # ownership — do NOT count in either bucket.
            continue
        _p_mode = str(getattr(_p, "execution_mode", "") or "").strip().lower()
        # AMENDMENT (PR #385 review — final owner classification):
        #
        # (1) Without a PROVEN expected_mode, we cannot say that any
        #     same-client/same-contract candidate is THIS fill's owner
        #     — even a valid-looking live/paper mode is not a proof of
        #     match.  All such candidates go into the unproven bucket.
        #
        # (2) With a proven expected_mode, a candidate with blank /
        #     malformed / different mode is still unproven; only an
        #     exact live/paper match on the same mode is proven.
        if _p_mode not in {"live", "paper"}:
            _unproven += 1
            continue
        if not _expected_mode_proven:
            _unproven += 1
            continue
        if _p_mode != _expected_mode:
            _unproven += 1
            continue
        _proven += 1

    # AMENDMENT (PR #385 review — final owner classification):
    # PROVEN_OWNER is only valid when a SINGLE uncontested exact owner
    # exists.  Duplicate proven owners or mixed proven/unproven states
    # are ownership ambiguity, not proof; they must escalate through
    # OWNER_IDENTITY_UNPROVEN.  A protective-monitoring claim requires
    # exactly one verified responsible party, not "at least one adult in
    # the room."
    if _proven == 1 and _unproven == 0:
        return "PROVEN_OWNER", 1, 0
    if _proven > 0 or _unproven > 0:
        return "OWNER_IDENTITY_UNPROVEN", _proven, _unproven
    return "NO_OWNER", 0, 0


def _owner_value(owner, name: str, default=None):
    if isinstance(owner, dict):
        return owner.get(name, default)
    return getattr(owner, name, default)


def _canonical_handoff_meta(order: dict) -> dict:
    raw_meta = order.get("meta")
    if isinstance(raw_meta, dict):
        return dict(raw_meta)
    if isinstance(raw_meta, str) and raw_meta.strip():
        try:
            parsed = json_loads(raw_meta)
            return dict(parsed) if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _canonical_handoff_meta_flag(order: dict, key: str) -> bool:
    value = _canonical_handoff_meta(order).get(key)
    return value is True or str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _canonical_owner_handoff_lock(exit_engine):
    lock = getattr(exit_engine, "_lock", None)
    return lock if hasattr(lock, "__enter__") and hasattr(lock, "__exit__") else nullcontext()


def _canonical_handoff_identity_where(
    order: dict,
) -> tuple[list[str], list] | None:
    client_id = str(order.get("client_id") or "").strip()
    local_order_id = str(order.get("local_order_id") or "").strip()
    broker_order_id = str(order.get("broker_order_id") or "").strip()
    contract = str(order.get("contract") or "").strip().upper()
    mode_state, execution_mode = _mode_source_state(order.get("execution_mode"))
    kind = str(order.get("kind") or "").strip().upper()

    # Retry, stop-claim, and retry-clear writes must never degrade to a
    # client/local-only update when one identity component is unavailable.
    if (
        not client_id
        or not local_order_id
        or not _has_proven_broker_order_id(broker_order_id)
        or not contract
        or mode_state != "valid"
        or kind != "ENTRY"
    ):
        return None

    clauses = [
        "client_id=%s",
        "local_order_id=%s",
        "broker_order_id=%s",
        "contract=%s",
        "kind='ENTRY'",
        "LOWER(BTRIM(COALESCE(execution_mode,'')))=%s",
    ]
    values = [client_id, local_order_id, broker_order_id, contract, execution_mode]
    return clauses, values


def _canonical_handoff_standing_stop_state(order: dict) -> str:
    """Read the durable standing-stop state without authorizing a resubmit."""
    meta = _canonical_handoff_meta(order)
    raw_state = str(
        meta.get(_CANONICAL_OWNER_HANDOFF_STANDING_STOP_STATE_KEY) or ""
    ).strip().upper()
    if raw_state in _CANONICAL_OWNER_HANDOFF_STANDING_STOP_STATES:
        return raw_state
    if raw_state or _canonical_handoff_meta_flag(
        order, _CANONICAL_OWNER_HANDOFF_STANDING_STOP_ATTEMPTED_KEY
    ):
        # The pre-amendment boolean only proved that a call was attempted; it
        # never proved the broker outcome.  It therefore cannot authorize a
        # second submission.
        return "OUTCOME_UNPROVEN"
    return ""


def _apply_canonical_handoff_meta_patch(order: dict, meta_patch: dict) -> None:
    meta = _canonical_handoff_meta(order)
    meta.update(meta_patch)
    order["meta"] = meta


def _persist_canonical_handoff_standing_stop_state(
    order: dict,
    state: str,
    *,
    broker_stop_id: str = "",
    detail_reason: str = "",
) -> bool:
    if state not in _CANONICAL_OWNER_HANDOFF_STANDING_STOP_STATES:
        return False
    if state == "SUBMITTED" and not _has_proven_standing_stop_order_id(broker_stop_id):
        log.critical(
            "[%s] refusing SUBMITTED standing-stop marker without broker order id | local=%s",
            order.get("client_id"),
            order.get("local_order_id"),
        )
        return False
    meta_patch = {
        _CANONICAL_OWNER_HANDOFF_STANDING_STOP_STATE_KEY: state,
        _CANONICAL_OWNER_HANDOFF_STANDING_STOP_ATTEMPTED_KEY: True,
    }
    if broker_stop_id:
        meta_patch[_CANONICAL_OWNER_HANDOFF_STANDING_STOP_ID_KEY] = str(
            broker_stop_id
        ).strip()
    if detail_reason:
        meta_patch["canonical_owner_handoff_standing_stop_detail"] = str(
            detail_reason
        )
    persisted = _update_canonical_handoff_order(
        order,
        operation=f"standing-stop {state.lower()} marker",
        meta_patch=meta_patch,
    )
    if persisted:
        _apply_canonical_handoff_meta_patch(order, meta_patch)
    return persisted


def _claim_canonical_handoff_standing_stop(order: dict) -> dict:
    """CAS-claim the only durable right to call the standing-stop broker API."""
    client_id = str(order.get("client_id") or "").strip()
    local_order_id = str(order.get("local_order_id") or "").strip()
    broker_order_id = str(order.get("broker_order_id") or "").strip()
    contract = str(order.get("contract") or "").strip().upper()
    kind = str(order.get("kind") or "").strip().upper()
    mode_state, execution_mode = _mode_source_state(order.get("execution_mode"))

    def _failure(detail_reason: str, *, state: str = "OUTCOME_UNPROVEN") -> dict:
        log.critical(
            "[%s] standing-stop durable claim failed | local=%s contract=%s "
            "mode=%s state=%s detail=%s",
            client_id,
            local_order_id,
            contract,
            execution_mode,
            state,
            detail_reason,
        )
        return {
            "ok": False,
            "claimant": False,
            "state": state,
            "outcome": "OUTCOME_UNPROVEN",
            "protection_proven": False,
            "standing_stop_attempted": bool(state),
            "detail_reason": detail_reason,
        }

    if not client_id:
        return _failure("client_id_missing")
    if not local_order_id:
        return _failure("local_order_id_missing")
    if not _has_proven_broker_order_id(broker_order_id):
        return _failure("broker_order_id_missing_or_invalid")
    if not contract:
        return _failure("contract_missing")
    if kind != "ENTRY":
        return _failure("kind_not_ENTRY")
    if mode_state != "valid":
        return _failure("execution_mode_missing_or_invalid")

    identity = _canonical_handoff_identity_where(order)
    if identity is None:
        return _failure("canonical_handoff_identity_missing_or_invalid")
    where_clauses, where_params = identity

    current_state = _canonical_handoff_standing_stop_state(order)
    meta = _canonical_handoff_meta(order)
    broker_stop_id = str(
        meta.get(_CANONICAL_OWNER_HANDOFF_STANDING_STOP_ID_KEY) or ""
    ).strip()
    if current_state == "SUBMITTED":
        if not _has_proven_standing_stop_order_id(broker_stop_id):
            persisted = _persist_canonical_handoff_standing_stop_state(
                order,
                "OUTCOME_UNPROVEN",
                detail_reason="submitted_state_missing_broker_stop_id",
            )
            if not persisted:
                return _failure(
                    "submitted_state_missing_broker_stop_id_persistence_failed",
                    state="SUBMITTED",
                )
            return {
                "ok": False,
                "claimant": False,
                "state": "OUTCOME_UNPROVEN",
                "outcome": "OUTCOME_UNPROVEN",
                "protection_proven": False,
                "standing_stop_attempted": True,
                "broker_stop_id": "",
                "detail_reason": "submitted_state_missing_broker_stop_id",
            }
        return {
            "ok": True,
            "claimant": False,
            "state": "SUBMITTED",
            "outcome": "SUBMITTED",
            "protection_proven": True,
            "standing_stop_attempted": True,
            "broker_stop_id": broker_stop_id,
            "detail_reason": "already_submitted",
        }
    if current_state:
        if current_state == "SUBMITTING":
            # A prior process may have died after broker acceptance and before
            # writing SUBMITTED.  Convert only the local interpretation; the
            # durable row remains SUBMITTING if this marker cannot commit.
            persisted = _persist_canonical_handoff_standing_stop_state(
                order,
                "OUTCOME_UNPROVEN",
                detail_reason="prior_claim_outcome_unproven",
            )
            if not persisted:
                return _failure(
                    "prior_submitting_state_outcome_unproven_persistence_failed",
                    state="SUBMITTING",
                )
            current_state = "OUTCOME_UNPROVEN"
        log.critical(
            "[%s] standing-stop resubmit blocked | local=%s state=%s",
            client_id,
            local_order_id,
            current_state,
        )
        return {
            "ok": False,
            "claimant": False,
            "state": current_state,
            "outcome": current_state,
            "protection_proven": current_state == "SUBMITTED",
            "standing_stop_attempted": True,
            "broker_stop_id": broker_stop_id,
            "detail_reason": "durable_state_blocks_automatic_resubmit",
        }

    def _claim():
        with conn() as c:
            updated = c.execute(
                "UPDATE orders SET "
                "meta=COALESCE(meta, '{}'::jsonb) || %s::jsonb, "
                "updated_ts=NOW() "
                "WHERE " + " AND ".join(where_clauses) + " "
                "AND (meta->>'canonical_owner_handoff_standing_stop_state' "
                "IS NULL OR BTRIM(meta->>'canonical_owner_handoff_standing_stop_state')='')",
                [
                    json_dumps(
                        {
                            _CANONICAL_OWNER_HANDOFF_STANDING_STOP_STATE_KEY: "SUBMITTING",
                            _CANONICAL_OWNER_HANDOFF_STANDING_STOP_ATTEMPTED_KEY: True,
                        }
                    ),
                    *where_params,
                ],
            )
            return int(
                getattr(updated, "rowcount", getattr(c, "rowcount", 0)) or 0
            )

    try:
        rowcount = run_with_retry(_claim)
    except Exception as exc:
        return _failure(
            "standing_stop_claim_database_error",
            state="OUTCOME_UNPROVEN",
        ) | {"exception_type": type(exc).__name__, "exception": str(exc)}
    if int(rowcount or 0) != 1:
        return _failure("standing_stop_claim_lost_or_order_missing")

    _apply_canonical_handoff_meta_patch(
        order,
        {
            _CANONICAL_OWNER_HANDOFF_STANDING_STOP_STATE_KEY: "SUBMITTING",
            _CANONICAL_OWNER_HANDOFF_STANDING_STOP_ATTEMPTED_KEY: True,
        },
    )
    return {
        "ok": True,
        "claimant": True,
        "state": "SUBMITTING",
        "outcome": "SUBMITTING",
        "protection_proven": False,
        "standing_stop_attempted": True,
        "detail_reason": "durable_claim_acquired",
    }


def _normalize_standing_stop_call_result(raw_result) -> dict:
    if isinstance(raw_result, dict):
        outcome = str(raw_result.get("outcome") or "").strip().upper()
        if outcome in {"SUBMITTED", "FAILED", "OUTCOME_UNPROVEN"}:
            if outcome == "SUBMITTED":
                broker_stop_id = str(raw_result.get("broker_stop_id") or "").strip()
                if not _has_proven_standing_stop_order_id(broker_stop_id):
                    return dict(
                        raw_result,
                        ok=False,
                        outcome="OUTCOME_UNPROVEN",
                        broker_stop_id="",
                        detail_reason=(
                            raw_result.get("detail_reason")
                            or "standing_stop_submitted_without_broker_order_id"
                        ),
                    )
                return dict(raw_result, ok=True, outcome=outcome, broker_stop_id=broker_stop_id)
            return dict(raw_result, outcome=outcome)
        if raw_result.get("ok") is True:
            broker_stop_id = str(raw_result.get("broker_stop_id") or "").strip()
            if not _has_proven_standing_stop_order_id(broker_stop_id):
                return dict(
                    raw_result,
                    ok=False,
                    outcome="OUTCOME_UNPROVEN",
                    broker_stop_id="",
                    detail_reason="standing_stop_success_without_broker_order_id",
                )
            return dict(
                raw_result,
                ok=True,
                outcome="SUBMITTED",
                broker_stop_id=broker_stop_id,
            )
        if raw_result.get("ok") is False:
            return dict(raw_result, ok=False, outcome="FAILED")
    if raw_result is True:
        return {
            "ok": False,
            "outcome": "OUTCOME_UNPROVEN",
            "detail_reason": "standing_stop_success_without_broker_order_id",
        }
    if raw_result is False:
        return {"ok": False, "outcome": "FAILED"}
    return {
        "ok": False,
        "outcome": "OUTCOME_UNPROVEN",
        "detail_reason": "standing_stop_call_result_unrecognized",
    }


def _establish_canonical_handoff_standing_stop(
    *,
    broker: BrokerAdapter,
    order: dict,
    qty: int,
    entry_price: float,
) -> dict:
    """Resolve standing-stop protection before identity bind, at most once."""
    claim = _claim_canonical_handoff_standing_stop(order)
    if not claim.get("claimant"):
        return claim

    call_result = _normalize_standing_stop_call_result(
        _place_standing_stop_best_effort(
            broker=broker,
            order=order,
            qty=qty,
            entry_price=entry_price,
        )
    )
    outcome = call_result.get("outcome")
    if outcome == "SUBMITTED" and not _has_proven_standing_stop_order_id(
        call_result.get("broker_stop_id")
    ):
        call_result = {
            **call_result,
            "ok": False,
            "outcome": "OUTCOME_UNPROVEN",
            "broker_stop_id": "",
            "detail_reason": "standing_stop_submitted_without_broker_order_id",
        }
        outcome = call_result.get("outcome")
    if outcome == "SUBMITTED":
        persisted = _persist_canonical_handoff_standing_stop_state(
            order,
            "SUBMITTED",
            broker_stop_id=str(call_result.get("broker_stop_id") or "").strip(),
            detail_reason=call_result.get("detail_reason", ""),
        )
        if persisted:
            return {
                **call_result,
                "state": "SUBMITTED",
                "protection_proven": True,
                "standing_stop_attempted": True,
            }
        log.critical(
            "[%s] standing-stop success marker failed; outcome remains unproven | local=%s",
            order.get("client_id"),
            order.get("local_order_id"),
        )
        return {
            **call_result,
            "state": "SUBMITTING",
            "outcome": "OUTCOME_UNPROVEN",
            "protection_proven": False,
            "standing_stop_attempted": True,
            "detail_reason": "submitted_marker_persistence_failed",
        }

    desired_state = (
        "FAILED" if outcome == "FAILED" else "OUTCOME_UNPROVEN"
    )
    persisted = _persist_canonical_handoff_standing_stop_state(
        order,
        desired_state,
        detail_reason=call_result.get("detail_reason", ""),
    )
    if not persisted:
        log.critical(
            "[%s] standing-stop %s marker failed; no automatic retry is allowed | local=%s",
            order.get("client_id"),
            desired_state,
            order.get("local_order_id"),
        )
        return {
            **call_result,
            "state": "SUBMITTING",
            "outcome": "OUTCOME_UNPROVEN",
            "protection_proven": False,
            "standing_stop_attempted": True,
            "detail_reason": "standing_stop_outcome_marker_persistence_failed",
        }
    return {
        **call_result,
        "state": desired_state,
        "protection_proven": False,
        "standing_stop_attempted": True,
    }


def _update_canonical_handoff_order(
    order: dict,
    *,
    meta_patch: dict,
    last_error: str | None = None,
    clear_handoff_error: bool = False,
    operation: str,
) -> bool:
    client_id = str(order.get("client_id") or "").strip()
    local_order_id = str(order.get("local_order_id") or "").strip()
    if not client_id or not local_order_id:
        return False

    identity = _canonical_handoff_identity_where(order)
    if identity is None:
        log.critical(
            "[%s] canonical owner handoff %s rejected incomplete identity | local=%s",
            client_id,
            operation,
            local_order_id,
        )
        return False
    where_clauses, where_params = identity

    def _fn():
        with conn() as c:
            assignments = []
            params = []
            if clear_handoff_error:
                assignments.append(
                    "last_error=CASE "
                    "WHEN last_error LIKE 'FILLED_ENTRY_POSITION_IDENTITY_BIND_FAILED%' "
                    "OR last_error LIKE 'FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN%' "
                    "THEN NULL ELSE last_error END"
                )
            elif last_error is not None:
                assignments.append("last_error=%s")
                params.append(last_error)
            assignments.extend([
                "meta=COALESCE(meta, '{}'::jsonb) || %s::jsonb",
                "updated_ts=NOW()",
            ])
            params.append(json_dumps(meta_patch))
            params.extend(where_params)
            updated = c.execute(
                "UPDATE orders SET " + ", ".join(assignments) + " "
                "WHERE " + " AND ".join(where_clauses),
                params,
            )
            return int(
                getattr(updated, "rowcount", getattr(c, "rowcount", 0)) or 0
            )

    try:
        rowcount = run_with_retry(_fn)
        if int(rowcount or 0) != 1:
            log.critical(
                "[%s] canonical owner handoff %s persistence did not match exactly one order row | local=%s rowcount=%s",
                client_id,
                operation,
                local_order_id,
                rowcount,
            )
            return False
        return True
    except Exception as exc:
        log.critical(
            "[%s] canonical owner handoff %s persistence failed | local=%s error=%s",
            client_id,
            operation,
            local_order_id,
            exc,
        )
        return False


def _persist_canonical_handoff_last_error_fallback(
    order: dict,
    reason_code: str,
) -> bool:
    """Keep a retry-visible error when the JSONB marker write cannot commit."""
    identity = _canonical_handoff_identity_where(order)
    if identity is None:
        log.critical(
            "[%s] canonical owner handoff fallback rejected incomplete identity | local=%s",
            order.get("client_id"),
            order.get("local_order_id"),
        )
        return False
    where_clauses, where_params = identity

    def _fn():
        with conn() as c:
            updated = c.execute(
                "UPDATE orders SET last_error=%s, updated_ts=NOW() "
                "WHERE " + " AND ".join(where_clauses),
                [reason_code, *where_params],
            )
            return int(
                getattr(updated, "rowcount", getattr(c, "rowcount", 0)) or 0
            )

    try:
        rowcount = run_with_retry(_fn)
        if int(rowcount or 0) != 1:
            log.critical(
                "[%s] canonical owner handoff fallback persistence did not match exactly one order row | local=%s rowcount=%s",
                order.get("client_id"),
                order.get("local_order_id"),
                rowcount,
            )
            return False
        return True
    except Exception as exc:
        log.critical(
            "[%s] canonical owner handoff fallback persistence failed | local=%s error=%s",
            order.get("client_id"),
            order.get("local_order_id"),
            exc,
        )
        return False


def _persist_canonical_owner_handoff_retry(
    order: dict,
    handoff_result: dict,
) -> bool:
    """Persist a retryable FILLED-entry handoff failure on the order row."""
    reason_code = str(
        handoff_result.get("reason_code")
        or "FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN"
    )
    persisted = _update_canonical_handoff_order(
        order,
        last_error=reason_code,
        operation="retry marker",
        meta_patch={
            "canonical_owner_handoff_retry_required": True,
            "canonical_owner_handoff_reason_code": reason_code,
            "canonical_owner_handoff_position_id": str(
                handoff_result.get("position_id") or ""
            ),
            "canonical_owner_handoff_standing_stop_attempted": bool(
                handoff_result.get("standing_stop_attempted", False)
            ),
            "canonical_owner_handoff_failed_at": now_utc_iso(),
        },
    )
    if not persisted:
        persisted = _persist_canonical_handoff_last_error_fallback(order, reason_code)
    return bool(persisted)


def _mark_canonical_owner_handoff_stop_attempted(order: dict) -> bool:
    """Compatibility wrapper for callers that already proved submission."""
    return _persist_canonical_handoff_standing_stop_state(order, "SUBMITTED")


def _clear_canonical_owner_handoff_retry(order: dict) -> bool:
    """Clear only the handoff retry marker after exact owner proof succeeds."""
    persisted = _update_canonical_handoff_order(
        order,
        clear_handoff_error=True,
        operation="retry clear",
        meta_patch={
            "canonical_owner_handoff_retry_required": False,
            "canonical_owner_handoff_reason_code": "",
            "canonical_owner_handoff_entry_handoff_proven": True,
            "canonical_owner_handoff_resolved_at": now_utc_iso(),
        },
    )
    if not persisted:
        log.critical(
            "[%s] canonical owner handoff success could not clear retry state | local=%s",
            order.get("client_id"),
            order.get("local_order_id"),
        )
    return persisted


def _verify_canonical_entry_owner(
    exit_engine,
    order: dict,
    canonical_position_id: str,
) -> dict:
    """Prove one exact behavior-active canonical owner after ENTRY handoff."""
    client_id = str(order.get("client_id") or "").strip().lower()
    contract = str(order.get("contract") or order.get("symbol") or "").strip().upper()
    execution_mode = _normalize_execution_mode_token(order.get("execution_mode"))
    position_id = str(canonical_position_id or "").strip()

    def _failure(detail_reason: str, **extra) -> dict:
        return {
            "ok": False,
            "disposition": "OWNER_UNPROVEN",
            "reason_code": "FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN",
            "detail_reason": detail_reason,
            "client_id": client_id,
            "contract": contract,
            "execution_mode": execution_mode,
            "position_id": position_id,
            **extra,
        }

    try:
        from ap_exit_engine import (
            _classify_canonical_repair_owner_domain,
            _is_proven_owner_client_id,
        )
    except Exception as exc:
        return _failure(
            "owner_domain_classifier_unavailable",
            exception_type=type(exc).__name__,
            exception=str(exc),
        )

    if (
        not _is_proven_owner_client_id(client_id)
        or not contract
        or execution_mode not in {"live", "paper"}
        or not position_id
    ):
        return _failure("canonical_owner_identity_missing")

    active_fn = getattr(exit_engine, "active_positions", None)
    if not callable(active_fn):
        return _failure("active_owner_lookup_unavailable")

    try:
        active_positions = list(active_fn() or [])
    except Exception as exc:
        return _failure(
            "active_owner_lookup_failed",
            exception_type=type(exc).__name__,
            exception=str(exc),
        )

    identity_matches = []
    repair_matches = []
    ambiguous_repair_matches = []
    for owner in active_positions:
        if bool(_owner_value(owner, "closed", False)):
            continue
        owner_contract = str(
            _owner_value(owner, "option_symbol", _owner_value(owner, "contract", ""))
            or ""
        ).strip().upper()
        owner_client = str(_owner_value(owner, "client_id", "") or "").strip().lower()
        owner_mode = _normalize_execution_mode_token(
            _owner_value(owner, "execution_mode", "")
        )
        owner_id = str(_owner_value(owner, "position_id", "") or "").strip()

        if owner_id.startswith("broker-repair-"):
            repair_domain = _classify_canonical_repair_owner_domain(
                owner,
                client_id=client_id,
                execution_mode=execution_mode,
                contract=contract,
            )
            if repair_domain == "IDENTITY_UNPROVEN":
                ambiguous_repair_matches.append(owner_id)
                continue
            if repair_domain == "PROVEN_FOREIGN_DOMAIN":
                continue
        if (
            owner_contract != contract
            or owner_client != client_id
            or owner_mode != execution_mode
        ):
            continue
        identity_matches.append(owner_id)
        if owner_id.startswith("broker-repair-"):
            repair_matches.append(owner_id)

    if (
        len(identity_matches) == 1
        and identity_matches[0] == position_id
        and not repair_matches
        and not ambiguous_repair_matches
    ):
        return {
            "ok": True,
            "disposition": "CANONICAL_OWNER_PROVEN",
            "reason_code": "FILLED_ENTRY_CANONICAL_OWNER_PROVEN",
            "client_id": client_id,
            "contract": contract,
            "execution_mode": execution_mode,
            "position_id": position_id,
            "owner_ids": identity_matches,
            "behavior_active_canonical_owner_count": 1,
            "behavior_active_ambiguous_same_contract_repair_count": 0,
            "ambiguous_repair_ids": [],
        }

    return _failure(
        "owner_cardinality_or_identity_unproven",
        owner_ids=identity_matches,
        repair_ids=repair_matches,
        ambiguous_repair_ids=ambiguous_repair_matches,
        behavior_active_canonical_owner_count=sum(
            owner_id == position_id for owner_id in identity_matches
        ),
        behavior_active_ambiguous_same_contract_repair_count=(
            len(ambiguous_repair_matches)
        ),
    )


def _emit_canonical_owner_handoff_failure(
    order: dict,
    canonical_position_id: str,
    handoff_result: dict,
) -> None:
    """Make an ENTRY identity failure durable and visible without broker mutation."""
    client_id = str(order.get("client_id") or "default")
    reason_code = str(
        handoff_result.get("reason_code")
        or "FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN"
    )
    payload = {
        "client_id": client_id,
        "local_order_id": order.get("local_order_id"),
        "broker_order_id": order.get("broker_order_id"),
        "contract": order.get("contract") or order.get("symbol"),
        "execution_mode": order.get("execution_mode"),
        "position_id": str(canonical_position_id or ""),
        **handoff_result,
    }
    retry_persisted = _persist_canonical_owner_handoff_retry(order, handoff_result)
    payload["canonical_owner_handoff_retry_persisted"] = retry_persisted
    if not retry_persisted:
        log.critical(
            "[%s] canonical owner handoff retry state was not persisted | local=%s",
            client_id,
            order.get("local_order_id"),
        )
    log.critical(
        "[%s] %s | local=%s contract=%s position_id=%s detail=%s",
        client_id,
        reason_code,
        order.get("local_order_id"),
        order.get("contract") or order.get("symbol"),
        canonical_position_id,
        handoff_result.get("detail_reason") or "unspecified",
    )
    audit(client_id, "ERROR", reason_code, payload)
    emit_fill_event(
        order,
        decision="ERROR",
        reason_code=reason_code,
        explanation=(
            "Canonical ENTRY ownership was not proven; no ambiguous exit "
            "owner may be declared active."
        ),
        result=handoff_result,
        extra_context=payload,
    )


def _quarantine_canonical_owner_handoff(
    *,
    exit_engine,
    order: dict,
    canonical_position_id: str,
    failure: dict,
) -> dict:
    """Disable only the exact client/mode/contract owners until retry proof."""
    quarantine_fn = getattr(
        exit_engine, "quarantine_canonical_owner_handoff", None
    )
    if not callable(quarantine_fn):
        log.critical(
            "[%s] canonical owner quarantine unavailable | local=%s contract=%s",
            order.get("client_id"),
            order.get("local_order_id"),
            order.get("contract") or order.get("symbol"),
        )
        return {"ok": False, "detail_reason": "quarantine_unavailable"}
    try:
        result = quarantine_fn(
            canonical_position_id=str(canonical_position_id or ""),
            contract=str(
                order.get("contract") or order.get("symbol") or ""
            ).strip().upper(),
            client_id=str(order.get("client_id") or "").strip(),
            execution_mode=_normalize_execution_mode_token(
                order.get("execution_mode")
            ),
            reason=str(
                failure.get("detail_reason")
                or failure.get("reason_code")
                or "canonical_owner_handoff_failed"
            ),
        )
        if isinstance(result, dict):
            if result.get("ok") is False:
                log.critical(
                    "[%s] canonical owner quarantine returned failure | local=%s detail=%s",
                    order.get("client_id"),
                    order.get("local_order_id"),
                    result.get("detail_reason") or "unspecified",
                )
            return result
        if result is False:
            log.critical(
                "[%s] canonical owner quarantine returned false | local=%s",
                order.get("client_id"),
                order.get("local_order_id"),
            )
            return {"ok": False, "detail_reason": "quarantine_returned_false"}
        return {"ok": True}
    except Exception as exc:
        # Durable retry persistence still occurs in the failure emitter.  A
        # quarantine failure is separately surfaced because an existing owner
        # must never remain silently submit-capable after proof loss.
        log.critical(
            "[%s] canonical owner quarantine failed | local=%s contract=%s error=%s",
            order.get("client_id"),
            order.get("local_order_id"),
            order.get("contract") or order.get("symbol"),
            exc,
        )
        return {
            "ok": False,
            "detail_reason": "quarantine_call_failed",
            "exception_type": type(exc).__name__,
            "exception": str(exc),
        }


def _emit_seed_failure_diagnostic(
    *,
    order: dict,
    result: dict,
    contract: str,
    position_id: str,
    signal_id: str,
    base_reason: str,
    mode_diag: dict,
    resolved_mode: str,
    owner_state: str,
    proven_count: int,
    unproven_count: int,
    tail_explanation_for_owner: str,
    extra_payload: dict | None = None,
) -> None:
    """Emit a truthful CRITICAL log + audit + fill event on any seed
    failure path.  Reason code carries the owner classification suffix
    so operators can distinguish real "retained" states from
    unproven-identity and no-owner cases.
    """
    _client_id = str(order.get("client_id") or "")
    _reason_code = base_reason
    if owner_state == "PROVEN_OWNER":
        pass  # base reason means monitoring truly is retained
    elif owner_state == "OWNER_IDENTITY_UNPROVEN":
        _reason_code = f"{base_reason}_OWNER_IDENTITY_UNPROVEN"
    elif owner_state == "OWNER_LOOKUP_FAILED":
        _reason_code = f"{base_reason}_OWNER_LOOKUP_FAILED"
    else:
        _reason_code = f"{base_reason}_NO_OWNER"

    _payload = {
        "contract": contract,
        "position_id": position_id,
        "local_order_id": order.get("local_order_id"),
        "broker_order_id": order.get("broker_order_id"),
        "signal_id": signal_id or order.get("signal_id"),
        "order_mode": mode_diag.get("order_mode"),
        "engine_mode": mode_diag.get("engine_mode"),
        "engine_status": mode_diag.get("engine_status"),
        "engine_sources": mode_diag.get("engine_sources"),
        "resolved_mode": resolved_mode,
        "owner_state": owner_state,
        "proven_owner_count": proven_count,
        "unproven_owner_count": unproven_count,
    }
    if extra_payload:
        _payload.update(extra_payload)

    _tail = {
        "PROVEN_OWNER": (
            "protective monitoring retained; no new exit owner created"
        ),
        "OWNER_IDENTITY_UNPROVEN": (
            "same-client contract candidates exist but their identity is unproven; "
            "reconciler must resolve"
        ),
        "OWNER_LOOKUP_FAILED": (
            "owner lookup failed; treat as unknown, do not create new owner"
        ),
        "NO_OWNER": (
            "NO protective owner exists; escalation required"
        ),
    }[owner_state]
    log.critical(
        "[%s] %s | contract=%s position_id=%s local=%s broker=%s "
        "resolved_mode=%s engine_status=%s owner_state=%s "
        "proven=%d unproven=%d — %s (%s)",
        _client_id, _reason_code,
        contract, position_id,
        order.get("local_order_id"), order.get("broker_order_id"),
        resolved_mode or "", mode_diag.get("engine_status") or "",
        owner_state, proven_count, unproven_count,
        _tail, tail_explanation_for_owner,
    )
    try:
        audit(_client_id, "CRITICAL", _reason_code, _payload)
    except Exception as _audit_err:
        log.warning(
            "[%s] audit write failed for %s: %s",
            _client_id, _reason_code, _audit_err,
        )
    emit_fill_event(
        order,
        decision="ERROR",
        reason_code=_reason_code,
        explanation=(
            f"{tail_explanation_for_owner}; owner_state={owner_state}; {_tail}."
        ),
        result=result or {},
        extra_context=_payload,
    )


def _seed_exit_engine(exit_engine, position_id: str, order: dict, result: dict, signal_id: str):
    """Seed the exit engine after confirmed ENTRY fill without faking live quote state.

    PR #235 (hardening #2): resolve side from order/OCC — fail-closed on
    unresolved, do NOT default to CALL.

    Repair 4: If the exit engine already holds a broker-repair position for
    the same contract, upgrade it to canonical identity instead of creating
    a second in-memory position.  This closes the BA production incident:
    the broker-repair position held synthetic ID and unknown execution_mode;
    exits and proof rows were never attributed to the canonical fill.
    """
    def _seed_result(ok: bool, disposition: str, detail_reason: str = "", **extra) -> dict:
        return {
            "ok": bool(ok),
            "disposition": disposition,
            "reason_code": (
                "EXIT_ENGINE_HANDOFF_SUCCEEDED"
                if ok
                else "FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN"
            ),
            "detail_reason": detail_reason,
            **extra,
        }

    if not exit_engine or not position_id:
        return _seed_result(False, "SEED_SKIPPED", "missing_exit_engine_or_position")

    # Resolve canonical side up front — used by both the ManagedPosition
    # constructor and the mp.signal dict below.  Emit critical + skip if
    # unresolvable rather than silently seeding a wrong-direction position.
    _side, _side_source = _resolve_order_option_side(order)
    if not _side:
        _emit_side_unresolved(order, reason_code="EXIT_ENGINE_SEED_SIDE_UNRESOLVED")
        return _seed_result(False, "SEED_SKIPPED", "side_unresolved")

    # ── Repair 4: canonical adoption of broker-repair position ───────────────
    # Before creating a new position, check whether the exit engine is already
    # tracking a broker-repair position for the same contract. If yes, upgrade
    # it atomically so we never have two in-memory positions for the same open
    # trade and all subsequent exits use canonical identity.
    _contract_for_adopt = str(order.get("contract") or order.get("symbol") or "").upper().strip()

    # AMENDMENT (PR #385 review — Fix 1 + NO_REPAIR_FOUND fallthrough):
    # resolve execution_mode ONCE at the top of _seed_exit_engine so the
    # exact lowercase live/paper flows through every downstream branch:
    # adoption, seed_position, and ManagedPosition construction.  The
    # previous placement inside the adoption block meant the fall-through
    # NO_REPAIR_FOUND path silently re-read the raw nullable order value
    # and produced a canonical position with blank execution_mode.
    _resolved_mode, _mode_disp, _mode_diag = _resolve_canonical_adoption_execution_mode(
        exit_engine, order,
    )

    # AMENDMENT (PR #385 review — mode gate must govern ALL seeding,
    # not just repair adoption): the mode-fail-closed block previously
    # lived inside `if callable(_adopt_fn) and contract and position_id`.
    # An engine without the optional adoption API could then bypass
    # validation and reach seed_position()/add_position() with a blank
    # canonical mode.  Hoist the gate here so every seeding path — with
    # or without adoption support — enforces exact live/paper before
    # any writer sees the position.
    if _mode_disp != "OK":
        _owner_state, _proven_owner, _unproven_owner = (
            _classify_existing_protective_owner(
                exit_engine=exit_engine,
                contract=_contract_for_adopt,
                client_id=str(order.get("client_id") or ""),
                expected_mode=_resolved_mode,
            )
        )
        _base_reason = (
            "CANONICAL_ADOPTION_MODE_CONFLICT"
            if _mode_disp == "CONFLICT"
            else "CANONICAL_ADOPTION_MODE_UNPROVEN"
        )
        _emit_seed_failure_diagnostic(
            order=order,
            result=result or {},
            contract=_contract_for_adopt,
            position_id=position_id,
            signal_id=signal_id or "",
            base_reason=_base_reason,
            mode_diag=_mode_diag,
            resolved_mode=_resolved_mode,
            owner_state=_owner_state,
            proven_count=_proven_owner,
            unproven_count=_unproven_owner,
            tail_explanation_for_owner=(
                "Canonical adoption execution mode could not be proven"
            ),
        )
        return _seed_result(
            False,
            "ADOPTION_BLOCKED",
            f"canonical_adoption_mode_{_mode_disp.lower()}",
        )

    _adopt_fn = getattr(exit_engine, "adopt_canonical_position_identity", None)
    if callable(_adopt_fn) and _contract_for_adopt and position_id:
        try:
            _entry_fill_for_adopt = _safe_float(
                result.get("avg_fill") or order.get("fill_price") or 0.0
            )
            # Blocker 4: canonical fill timestamp (broker > order fallback > now).
            from datetime import datetime, timezone as _tz
            _now_utc = datetime.now(_tz.utc)
            _entry_ts_for_adopt = (
                result.get("filled_ts")
                or result.get("filled_at")
                or result.get("timestamp")
                or order.get("filled_ts")
                or _now_utc
            )
            # Blocker 4: underlying entry with same precedence as normal seeding.
            _underlying_entry_for_adopt = _safe_float(
                order.get("underlying_entry")
                or order.get("entry_underlying")
                or order.get("last_underlying_price")
                or order.get("last_underlying_price_meta")
                or order.get("entry_underlying_meta")
                or order.get("underlying_entry_meta")
                or order.get("trigger_price")
                or 0.0
            )
            _adopted = _adopt_fn(
                contract              = _contract_for_adopt,
                canonical_position_id = position_id,
                local_order_id        = str(order.get("local_order_id") or ""),
                broker_order_id       = str(order.get("broker_order_id") or ""),
                signal_id             = signal_id or str(order.get("signal_id") or ""),
                canonical_signal_id   = str(order.get("canonical_signal_id") or ""),
                entry_fill            = _entry_fill_for_adopt,
                entry_ts              = _entry_ts_for_adopt,
                order_filled_ts       = order.get("filled_ts"),
                execution_mode        = _resolved_mode,
                client_id             = str(order.get("client_id") or ""),
                underlying_entry      = _underlying_entry_for_adopt,
                score                 = _safe_float(order.get("score") or 0.0),
                tier                  = str(order.get("tier") or ""),
                pattern               = str(order.get("pattern") or ""),
                direction             = str(order.get("direction") or _side or ""),
                timeframe             = str(order.get("timeframe") or ""),
                underlying_stop       = _safe_float(order.get("stop_underlying") or order.get("underlying_stop") or 0.0),
                underlying_target     = _safe_float(order.get("target_underlying") or order.get("underlying_target") or 0.0),
            )

            # Handle CanonicalAdoptionResult (structured) or legacy bool.
            _disposition = getattr(_adopted, "disposition", None)
            if _disposition in ("ADOPTED", "ALREADY_CANONICAL_REPAIR_REMOVED"):
                log.info(
                    "[%s] _seed_exit_engine: %s contract=%s position_id=%s",
                    order.get("client_id"), _disposition, _contract_for_adopt, position_id,
                )
                return _seed_result(True, _disposition)
            elif _disposition == "NO_REPAIR_FOUND":
                pass  # fall through to normal seed
            elif _disposition and _disposition.startswith("RETRY_"):
                # AMENDMENT (PR #385 review): a RETRY_* disposition means
                # the adoption target is untrusted; do NOT falsely claim
                # protective monitoring is retained.  Verify whether any
                # exact protective owner (contract + client + live/paper)
                # exists on the engine right now, then emit a truthful
                # reason code that reflects the actual owner state.
                _owner_state, _proven_owner, _unproven_owner = (
                    _classify_existing_protective_owner(
                        exit_engine=exit_engine,
                        contract=_contract_for_adopt,
                        client_id=str(order.get("client_id") or ""),
                        expected_mode=_resolved_mode,
                    )
                )
                _emit_seed_failure_diagnostic(
                    order=order,
                    result=result or {},
                    contract=_contract_for_adopt,
                    position_id=position_id,
                    signal_id=signal_id or "",
                    base_reason=f"CANONICAL_ADOPTION_{_disposition}",
                    mode_diag=_mode_diag,
                    resolved_mode=_resolved_mode,
                    owner_state=_owner_state,
                    proven_count=_proven_owner,
                    unproven_count=_unproven_owner,
                    tail_explanation_for_owner=(
                        f"Adoption returned {_disposition}: "
                        f"{getattr(_adopted, 'reason', '')}"
                    ),
                    extra_payload={"adoption_disposition": _disposition},
                )
                return _seed_result(
                    False,
                    "ADOPTION_BLOCKED",
                    f"adoption_{_disposition.lower()}",
                    adoption_disposition=_disposition,
                )
            elif _adopted is True:  # legacy bool path
                return _seed_result(True, "ADOPTED_LEGACY")
            elif _adopted is False:  # legacy bool — no repair found, seed normally
                pass
        except Exception as _adopt_err:
            # AMENDMENT (PR #385 review): adoption raised.  Route through
            # the same owner classifier so the diagnostic reflects the
            # real owner state instead of claiming monitoring is retained.
            _owner_state, _proven_owner, _unproven_owner = (
                _classify_existing_protective_owner(
                    exit_engine=exit_engine,
                    contract=_contract_for_adopt,
                    client_id=str(order.get("client_id") or ""),
                    expected_mode=_resolved_mode,
                )
            )
            _emit_seed_failure_diagnostic(
                order=order,
                result=result or {},
                contract=_contract_for_adopt,
                position_id=position_id,
                signal_id=signal_id or "",
                base_reason="CANONICAL_ADOPTION_RETRY_ADOPTION_ERROR",
                mode_diag=_mode_diag,
                resolved_mode=_resolved_mode,
                owner_state=_owner_state,
                proven_count=_proven_owner,
                unproven_count=_unproven_owner,
                tail_explanation_for_owner=(
                    f"Canonical adoption raised: {type(_adopt_err).__name__}: "
                    f"{_adopt_err}"
                ),
                extra_payload={
                    "adoption_disposition": "RETRY_ADOPTION_ERROR",
                    "exception_type": type(_adopt_err).__name__,
                },
            )
            return _seed_result(
                False,
                "ADOPTION_ERROR",
                "canonical_adoption_exception",
                exception_type=type(_adopt_err).__name__,
            )

    try:
        getter = getattr(exit_engine, "get_position", None)
        if callable(getter) and getter(position_id):
            return _seed_result(True, "ALREADY_SEEDED")
    except Exception as _ee_err:
        log.warning("Exit engine position check failed for %s: %s", position_id, _ee_err)

    # AMENDMENT (PR #385 review): both fall-through seeding paths
    # (seed_position and the ManagedPosition/add_position path below)
    # must consume the SAME _resolved_mode the adoption call would
    # have used.  Whenever a canonical mode is proven, ALWAYS pass a
    # normalized copy — including cases like "LIVE" / " LIVE " /
    # "Paper" that would normalize to _resolved_mode but still hand
    # non-canonical text into any downstream seed_position payload
    # that inspects the raw string.  The original order dict is never
    # mutated.
    if _resolved_mode:
        _seed_order = dict(order)
        _seed_order["execution_mode"] = _resolved_mode
    else:
        _seed_order = order

    try:
        if hasattr(exit_engine, "seed_position"):
            exit_engine.seed_position(position_id, _seed_order, result)
            return _seed_result(True, "SEEDED")
    except Exception as exc:
        log.debug("exit_engine.seed_position failed; trying add_position path: %s", exc)

    try:
        _MP = _load_managed_position_class()
        contract = order.get("contract") or order.get("symbol") or ""
        ticker = _extract_underlying_from_contract(contract, fallback=order.get("symbol") or "")
        underlying_entry = _safe_float(
            order.get("underlying_entry")
            or order.get("entry_underlying")
            or order.get("last_underlying_price")
            or order.get("trigger_price")
            or 0.0
        )
        entry_option_price = _safe_float(result.get("avg_fill") or order.get("fill_price") or 0.0)

        mp = _MP(
            ticker=ticker,
            option_symbol=contract,
            side=_side,
            quantity=int(result.get("filled_qty") or order.get("qty") or 0),
            entry_price=entry_option_price,
            underlying_entry=underlying_entry,
            underlying_target=_safe_float(order.get("target_underlying") or order.get("underlying_target") or 0.0),
            underlying_stop=_safe_float(order.get("stop_underlying") or order.get("underlying_stop") or 0.0),
        )
        mp.position_id = position_id
        mp.client_id = str(order.get("client_id") or "")
        mp.signal_id = signal_id
        # AMENDMENT (PR #385 review): use _resolved_mode (proven exact
        # lowercase live/paper), not the raw nullable order value.  A
        # blank order.execution_mode is exactly the historical /
        # recovery-created shape the resolver is designed to repair;
        # storing "" here would break exact-mode protective persistence
        # and LIVE/PAPER isolation for the canonical position.
        try:
            mp.execution_mode = _resolved_mode
        except Exception:
            pass

        try:
            mp.current_underlying = _safe_float(order.get("last_underlying_price") or 0.0)
            mp.current_option_price = entry_option_price
            mp.current_bid = 0.0
            mp.current_ask = 0.0
            mp.quote_fresh = False
            mp.quote_source = "fill_monitor_seed_unhydrated"
            mp.quote_ts = None
            mp.last_quote_ts = None
            mp.needs_quote_refresh = True
        except Exception:
            pass

        mp.signal = {
            "signal_id": signal_id,
            "pattern": str(order.get("pattern") or ""),
            "tier": str(order.get("tier") or "B"),
            "score": float(order.get("score") or 0),
            "timeframe": str(order.get("timeframe") or "1d"),
            "side": _side,
            "quote_fresh": False,
            "seed_source": "fill_monitor",
        }
        if not getattr(mp, "client_id", ""):
            mp.client_id = getattr(exit_engine, "_email", "") or getattr(exit_engine, "client_id", "")
        exit_engine.add_position(mp)

        refresher = getattr(exit_engine, "request_quote_refresh", None)
        if callable(refresher):
            try:
                refresher(position_id)
            except Exception as exc:
                log.debug("[%s] exit_engine.request_quote_refresh failed non-critical for %s: %s", order.get("client_id"), position_id, exc)

        log.info(
            "[%s] Exit engine seeded for pos=%s ticker=%s quote_fresh=False current_underlying=%s",
            order.get("client_id"),
            position_id,
            ticker,
            getattr(mp, "current_underlying", None),
        )
        return _seed_result(True, "SEEDED")
    except Exception as exc:
        log.error("[%s] exit_engine.add_position failed: %s", order.get("client_id"), exc)
        return _seed_result(
            False,
            "SEED_FAILED",
            "exit_engine_add_position_failed",
            exception_type=type(exc).__name__,
        )


# =============================================================================
# BROKER FILL SANITY / ANOMALY ESCALATION
# =============================================================================

def _sanitize_cumulative_filled(order: dict, result: dict) -> tuple[int, bool]:
    """Clamp broker cumulative fill to local order qty and audit impossible broker values."""
    client_id = str(order.get("client_id") or "default")
    local_id = str(order.get("local_order_id") or "")
    broker_id = order.get("broker_order_id")

    raw_filled = _strict_nonnegative_whole_number(result.get("filled_qty"))
    order_qty = _strict_nonnegative_whole_number(order.get("qty"))
    if raw_filled is None:
        raw_filled = 0
    if order_qty is None:
        order_qty = 0

    if order_qty > 0 and raw_filled > order_qty:
        clamped = order_qty
        log.critical(
            "[%s] Broker overfill anomaly clamped | order=%s broker=%s raw_filled=%s order_qty=%s",
            client_id,
            local_id,
            broker_id,
            raw_filled,
            order_qty,
        )
        result["filled_qty_raw"] = raw_filled
        result["filled_qty"] = clamped
        audit(
            client_id,
            "CRITICAL",
            "BROKER_OVERFILL_CLAMPED",
            {
                "local_order_id": local_id,
                "broker_order_id": broker_id,
                "raw_filled_qty": raw_filled,
                "clamped_filled_qty": clamped,
                "order_qty": order_qty,
                "broker_reason": result.get("reason"),
            },
        )
        emit_fill_event(
            order,
            decision="ERROR",
            reason_code="BROKER_OVERFILL_CLAMPED",
            explanation="Broker returned cumulative filled quantity greater than local order quantity; clamped before OSM/PM side effects.",
            result=result,
            extra_context={
                "raw_filled_qty": raw_filled,
                "clamped_filled_qty": clamped,
                "order_qty": order_qty,
            },
        )
        return clamped, True

    return raw_filled, False


def _mark_broker_fill_anomaly(osm, order: dict, *, reason: str, mapped: str | None = None) -> bool:
    """Best-effort special OSM quarantine transition for impossible broker/fill data."""
    if not osm:
        return False

    local_id  = order.get("local_order_id")
    broker_id = order.get("broker_order_id")
    client_id = order.get("client_id", "?")

    # BROKER_FILL_ANOMALY is a diagnostic alert condition, not a legal OSM
    # lifecycle state. Attempting to transition to it always produces:
    #   CRITICAL: ILLEGAL TRANSITION -- EXIT_ACKNOWLEDGED -> BROKER_FILL_ANOMALY
    # Guard here so the alert fires but OSM is never touched.
    if FILL_ANOMALY_STATUS == "BROKER_FILL_ANOMALY":
        log.critical(
            "[%s] Broker fill anomaly retained as alert only | order=%s broker=%s reason=%s "
            "| NOT transitioning OSM — BROKER_FILL_ANOMALY is not a legal lifecycle state",
            client_id, local_id, broker_id, reason,
        )
        return False

    try:
        return bool(
            osm.transition(
                local_id,
                FILL_ANOMALY_STATUS,
                broker_order_id=broker_id,
                last_error=reason,
            )
        )
    except Exception as exc:
        log.error(
            "[%s] OSM anomaly transition failed | order=%s status=%s mapped=%s error=%s",
            client_id, local_id, FILL_ANOMALY_STATUS, mapped, exc,
        )
        return False

# =============================================================================
# CORE — PROCESS ONE PENDING ORDER
# =============================================================================

def process_pending_order(
    broker: BrokerAdapter,
    order: dict,
    osm=None,
    pm=None,
    exit_engine=None,
    alert_fn=None,
    data_broker=None,
    runtime_execution_mode=None,
):
    if PRODUCTION_MODE and ALLOW_LEGACY_FILL_MONITOR:
        raise RuntimeError("ALLOW_LEGACY_FILL_MONITOR=1 is forbidden in production/live mode")
    if osm is None and not ALLOW_LEGACY_FILL_MONITOR:
        raise RuntimeError("fill_monitor requires OSM unless ALLOW_LEGACY_FILL_MONITOR=1")

    client_id = order["client_id"]
    local_id = order["local_order_id"]
    broker_id = order.get("broker_order_id")
    kind = (order.get("kind") or "ENTRY").upper()
    canonical_owner_handoff_recovery = _is_canonical_owner_handoff_recovery(order)
    entry_handoff_proven = kind != "ENTRY"
    resolved_runtime_execution_mode = _resolve_runtime_execution_mode(
        runtime_execution_mode=runtime_execution_mode,
        exit_engine=exit_engine,
    )

    requested_exit_recovery = (
        kind == "EXIT"
        and str(order.get("status") or "").strip().upper() == "EXIT_REQUESTED"
    )
    if requested_exit_recovery:
        if not _has_proven_broker_order_id(broker_id):
            emit_fill_event(
                order,
                decision="ALERT",
                reason_code="BROKER_OWNED_EXIT_REQUEST_RECOVERY_HOLD",
                explanation=(
                    "Broker-less EXIT_REQUESTED remains a local intent and is "
                    "excluded from broker polling."
                ),
                result={"reason": "NO_BROKER_ID"},
            )
            return
        adopted, order, _adoption_result = _adopt_broker_owned_exit_request(
            osm,
            order,
            source="fill_monitor",
            runtime_execution_mode=resolved_runtime_execution_mode,
        )
        if not adopted:
            return
        broker_id = order.get("broker_order_id")
        kind = (order.get("kind") or "EXIT").upper()

    result = check_order_with_broker(broker, order)
    mapped = result.get("status", "UNKNOWN")
    durable_db_filled_recovery = False
    if (
        canonical_owner_handoff_recovery
        and mapped in ("UNKNOWN", "ERROR")
        and _broker_poll_unavailable_for_durable_filled_recovery(result)
    ):
        broker_poll_status = mapped
        durable_result = _durable_filled_entry_recovery_result(
            order,
            runtime_execution_mode=resolved_runtime_execution_mode,
        )
        if durable_result is not None:
            result = durable_result
            mapped = "FILLED"
            durable_db_filled_recovery = True
            audit(
                client_id,
                "WARNING",
                "FILLED_ENTRY_DURABLE_DB_HANDOFF_RECOVERY",
                {
                    "local_order_id": local_id,
                    "broker_order_id": broker_id,
                    "filled_qty": durable_result["filled_qty"],
                    "fill_price": durable_result["avg_fill"],
                    "broker_poll_status": broker_poll_status,
                },
            )

    # Ordinary broker-confirmed fills must satisfy the same runtime-mode
    # admission boundary as DB-only recovery.  This check is deliberately
    # before cumulative-fill handling and before every OSM/PM/pair-cancel/
    # standing-stop/exit-owner side effect.  A client-scoped pending-order
    # query is not execution-mode proof.
    if mapped in {"FILLED", "PARTIAL_FILL", "EXIT_FILLED", "EXIT_PARTIAL_FILL"}:
        row_mode_state, row_mode = _mode_source_state(order.get("execution_mode"))
        runtime_mode_state, resolved_mode = _mode_source_state(
            resolved_runtime_execution_mode
        )
        mode_hold = (
            row_mode_state != "valid"
            or runtime_mode_state != "valid"
            or row_mode != resolved_mode
        )
        finite_avg_fill = _finite_float_or_none(result.get("avg_fill"))
        price_hold = finite_avg_fill is None or finite_avg_fill <= 0
        if mode_hold or price_hold:
            if mode_hold:
                reason = (
                    "FILL_EXECUTION_MODE_UNPROVEN"
                    if row_mode_state != "valid" or runtime_mode_state != "valid"
                    else "FILL_EXECUTION_MODE_MISMATCH"
                )
            else:
                price_is_nonfinite = False
                if result.get("avg_fill") is not None:
                    try:
                        price_is_nonfinite = not math.isfinite(
                            float(result.get("avg_fill"))
                        )
                    except (TypeError, ValueError, OverflowError):
                        price_is_nonfinite = False
                reason = (
                    "BROKER_FILL_NONFINITE_PRICE"
                    if price_is_nonfinite
                    else "BROKER_FILL_INVALID_PRICE"
                )
            payload = {
                "local_order_id": local_id,
                "broker_order_id": broker_id,
                "kind": kind,
                "mapped_status": mapped,
                "row_execution_mode": order.get("execution_mode"),
                "runtime_execution_mode": resolved_runtime_execution_mode,
                "avg_fill": result.get("avg_fill"),
                "mode_hold": mode_hold,
                "price_hold": price_hold,
            }
            log.critical("[%s] %s | %s", client_id, reason, payload)
            audit(client_id, "CRITICAL", reason, payload)
            emit_fill_event(
                order,
                decision="ERROR",
                reason_code=reason,
                explanation=(
                    "Broker fill admission failed before OSM, position, "
                    "exit-engine, or broker side effects."
                ),
                result={**result, "status": "ERROR", "reason": reason},
                extra_context=payload,
            )
            return

    prev_filled = _strict_nonnegative_whole_number(order.get("filled_qty"))
    if prev_filled is None:
        log.critical(
            "[%s] Durable order filled_qty malformed | order=%s value=%r — holding",
            client_id,
            local_id,
            order.get("filled_qty"),
        )
        return
    new_filled, overfill_clamped = _sanitize_cumulative_filled(order, result)

    if mapped not in ("UNKNOWN", "ERROR"):
        _reset_broker_anomaly_count(client_id, local_id, broker_id)

    if new_filled < prev_filled:
        log.critical(
            "[%s] Fill regression blocked | order=%s broker=%s prev=%s new=%s",
            client_id,
            local_id,
            broker_id,
            prev_filled,
            new_filled,
        )
        if osm:
            osm.increment_retry(local_id)
        audit(
            client_id,
            "ERROR",
            "FILL_REGRESSION_BLOCKED",
            {"local_order_id": local_id, "broker_order_id": broker_id, "prev": prev_filled, "new": new_filled},
        )
        _mark_broker_fill_anomaly(
            osm,
            order,
            reason=f"broker fill regression prev={prev_filled} new={new_filled}",
            mapped=mapped,
        )
        return

    # ── FILLED / EXIT_FILLED ────────────────────────────────────────────────
    if mapped in ("FILLED", "EXIT_FILLED"):
        emit_fill_event(
            order,
            decision="CONFIRMED",
            # F7: the canonical terminal reason code must not fork.  Emitting
            # a recovery-specific code here made post-adoption fills invisible
            # to every existing consumer that matches on EXIT_FILLED.
            # Provenance belongs in context, not in the taxonomy.
            reason_code="ORDER_FILLED" if kind == "ENTRY" else "EXIT_FILLED",
            explanation=f"{kind} filled via broker",
            result=result,
            extra_context={
                "osm_status": mapped,
                "broker_owned_exit_request_recovered": bool(
                    requested_exit_recovery
                ),
                "canonical_owner_handoff_recovery": bool(
                    canonical_owner_handoff_recovery
                ),
            },
        )

        ok = False
        if osm:
            try:
                if canonical_owner_handoff_recovery:
                    # The row is already durably FILLED.  A terminal OSM
                    # transition is not a retry mechanism and can reject the
                    # replay before PM/exit ownership is repaired.
                    ok = True
                    audit(
                        client_id,
                        "WARNING",
                        "FILLED_ENTRY_CANONICAL_OWNER_HANDOFF_RETRY",
                        {
                            "local_order_id": local_id,
                            "broker_order_id": broker_id,
                            "position_id": order.get("position_id"),
                        },
                    )
                else:
                    ok = osm.transition(
                        local_id,
                        mapped,
                        filled_qty=new_filled,
                        fill_price=result.get("avg_fill"),
                        broker_order_id=broker_id,
                    )
            except Exception as exc:
                log.error("[%s] OSM transition %s failed for %s: %s", client_id, mapped, local_id, exc)
        else:
            _legacy_update_order_status(local_id, "FILLED", filled_qty=new_filled)
            ok = True

        if ok and kind == "ENTRY" and pm:
            position_id = None
            try:
                plan_id = order.get("plan_id") or order.get("signal_id") or local_id
                signal_id = order.get("signal_id") or local_id
                ticker = (order.get("symbol") or "").upper()
                qty = int(new_filled or order.get("qty") or 0)
                price = float(result.get("avg_fill") or 0.0)

                trace_gate(
                    str(signal_id),
                    ticker,
                    "ORDER_FILLED",
                    "PASS",
                    reason="entry_filled",
                    trigger_price=price,
                    contracts=qty,
                )

                # Pair cancel is broker-first/local-second.  A DB-only
                # recovery has durable fill proof but no current broker truth;
                # do not create a new broker mutation from an unavailable poll.
                if not durable_db_filled_recovery:
                    _cancel_pair_opposite(order, broker, osm, alert_fn=alert_fn)
                else:
                    audit(
                        client_id,
                        "WARNING",
                        "FILLED_ENTRY_DB_RECOVERY_BROKER_MUTATION_SKIPPED",
                        {"local_order_id": local_id, "broker_order_id": broker_id},
                    )

                # Persist local position BEFORE placing optional standing stop.
                position_id = _open_position_safe(
                    pm,
                    order=order,
                    result=result,
                    plan_id=plan_id,
                    signal_id=signal_id,
                    local_id=local_id,
                    broker=broker,
                    quote_broker=data_broker,
                )

                log.info(
                    "[%s] order_filled_detected order=%s contract=%s qty=%d "
                    "position_link_result=%s",
                    client_id, local_id,
                    order.get("contract") or order.get("symbol"),
                    int(new_filled or 0),
                    "position_opened" if position_id else "MISSING",
                )
                if position_id:
                    # A broker-side standing stop is temporary protection, not
                    # canonical ownership.  Fence it before the DB identity
                    # bind so a bind failure cannot leave a confirmed LIVE fill
                    # with no automated protective path.
                    if durable_db_filled_recovery:
                        durable_stop_state = _canonical_handoff_standing_stop_state(order)
                        durable_stop_id = str(
                            _canonical_handoff_meta(order).get(
                                _CANONICAL_OWNER_HANDOFF_STANDING_STOP_ID_KEY
                            )
                            or ""
                        ).strip()
                        standing_stop_result = {
                            "state": durable_stop_state or "OUTCOME_UNPROVEN",
                            "standing_stop_attempted": bool(
                                durable_stop_state or durable_stop_id
                            ),
                            "protection_proven": (
                                durable_stop_state == "SUBMITTED"
                                and _has_proven_standing_stop_order_id(durable_stop_id)
                            ),
                            "detail_reason": "broker_status_unavailable_db_fill_recovery",
                        }
                    else:
                        standing_stop_result = _establish_canonical_handoff_standing_stop(
                            broker=broker,
                            order=order,
                            qty=qty,
                            entry_price=price,
                        )
                    standing_stop_attempted = bool(
                        standing_stop_result.get("standing_stop_attempted")
                    )
                    standing_stop_state = str(
                        standing_stop_result.get("state") or ""
                    ).strip().upper()
                    standing_stop_proven = bool(
                        standing_stop_result.get("protection_proven")
                    )

                    # Durable canonical identity must be proven before any
                    # exit-engine adoption/seed decision is treated as live.
                    bind_result = _bind_filled_entry_position_id(order, position_id)
                    if not bind_result.get("ok"):
                        bind_failure = dict(bind_result)
                        bind_failure.update(
                            {
                                "standing_stop_attempted": standing_stop_attempted,
                                "standing_stop_state": standing_stop_state,
                                "standing_stop_proven": standing_stop_proven,
                            }
                        )
                        owner_state, proven_count, unproven_count = (
                            _classify_existing_protective_owner(
                                exit_engine=exit_engine,
                                contract=str(
                                    order.get("contract") or order.get("symbol") or ""
                                ).strip().upper(),
                                client_id=str(order.get("client_id") or "").strip(),
                                expected_mode=_normalize_execution_mode_token(
                                    order.get("execution_mode")
                                ),
                            )
                        )
                        bind_failure.update(
                            {
                                "protective_owner_state": owner_state,
                                "protective_owner_proven_count": proven_count,
                                "protective_owner_unproven_count": unproven_count,
                            }
                        )
                        if owner_state == "OWNER_IDENTITY_UNPROVEN":
                            quarantine_result = _quarantine_canonical_owner_handoff(
                                exit_engine=exit_engine,
                                order=order,
                                canonical_position_id=position_id,
                                failure=bind_failure,
                            )
                            if not quarantine_result.get("ok"):
                                bind_failure["quarantine_failure"] = quarantine_result
                        elif owner_state == "NO_OWNER" and not standing_stop_proven:
                            bind_failure.update(
                                {
                                    "reason_code": "FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN_NO_PROTECTION",
                                    "detail_reason": "bind_failed_no_owner_no_proven_standing_stop",
                                }
                            )
                        _emit_canonical_owner_handoff_failure(
                            order, position_id, bind_failure
                        )
                    else:
                        log.info(
                            "[%s] order_position_link_success order=%s position=%s "
                            "disposition=%s contract=%s",
                            client_id, local_id, position_id,
                            bind_result.get("disposition"),
                            order.get("contract") or order.get("symbol"),
                        )

                        seed_failure = None
                        owner_failure = None
                        clear_failure = None
                        with _canonical_owner_handoff_lock(exit_engine):
                            clear_fn = getattr(
                                exit_engine,
                                "clear_canonical_owner_handoff_quarantine",
                                None,
                            )
                            if callable(clear_fn):
                                try:
                                    clear_result = clear_fn(
                                        canonical_position_id=position_id,
                                        contract=str(
                                            order.get("contract") or order.get("symbol") or ""
                                        ).strip().upper(),
                                        client_id=str(order.get("client_id") or "").strip(),
                                        execution_mode=_normalize_execution_mode_token(
                                            order.get("execution_mode")
                                        ),
                                    )
                                    if not isinstance(clear_result, dict) or not clear_result.get(
                                        "ok"
                                    ):
                                        clear_failure = {
                                            "reason_code": "FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN",
                                            "detail_reason": "canonical_owner_quarantine_clear_failed",
                                            "clear_result": clear_result,
                                        }
                                except Exception as clear_err:
                                    clear_failure = {
                                        "reason_code": "FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN",
                                        "detail_reason": "canonical_owner_quarantine_clear_failed",
                                        "exception_type": type(clear_err).__name__,
                                        "exception": str(clear_err),
                                    }

                            if clear_failure is None:
                                seed_result = _seed_exit_engine(
                                    exit_engine, position_id, order, result, signal_id
                                )
                                if not isinstance(seed_result, dict) or not seed_result.get("ok"):
                                    seed_failure = (
                                        dict(seed_result)
                                        if isinstance(seed_result, dict)
                                        else {
                                            "reason_code": "FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN",
                                            "detail_reason": "seed_result_missing_or_invalid",
                                        }
                                    )
                                    seed_failure.setdefault(
                                        "standing_stop_attempted", standing_stop_attempted
                                    )
                                    seed_failure.setdefault(
                                        "standing_stop_state", standing_stop_state
                                    )
                                    quarantine_result = _quarantine_canonical_owner_handoff(
                                        exit_engine=exit_engine,
                                        order=order,
                                        canonical_position_id=position_id,
                                        failure=seed_failure,
                                    )
                                    if not quarantine_result.get("ok"):
                                        seed_failure["quarantine_failure"] = quarantine_result
                                else:
                                    owner_result = _verify_canonical_entry_owner(
                                        exit_engine, order, position_id
                                    )
                                    if not owner_result.get("ok"):
                                        owner_failure = dict(owner_result)
                                        owner_failure.setdefault(
                                            "standing_stop_attempted", standing_stop_attempted
                                        )
                                        owner_failure.setdefault(
                                            "standing_stop_state", standing_stop_state
                                        )
                                        quarantine_result = _quarantine_canonical_owner_handoff(
                                            exit_engine=exit_engine,
                                            order=order,
                                            canonical_position_id=position_id,
                                            failure=owner_failure,
                                        )
                                        if not quarantine_result.get("ok"):
                                            owner_failure["quarantine_failure"] = quarantine_result

                        if clear_failure is not None:
                            _emit_canonical_owner_handoff_failure(
                                order, position_id, clear_failure
                            )
                        elif seed_failure is not None:
                            _emit_canonical_owner_handoff_failure(
                                order, position_id, seed_failure
                            )
                        elif owner_failure is not None:
                            _emit_canonical_owner_handoff_failure(
                                order, position_id, owner_failure
                            )
                        elif _clear_canonical_owner_handoff_retry(order):
                            entry_handoff_proven = True
                        else:
                            _emit_canonical_owner_handoff_failure(
                                order,
                                position_id,
                                {
                                    "reason_code": "FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN",
                                    "detail_reason": "retry_state_clear_failed",
                                    "standing_stop_attempted": standing_stop_attempted,
                                    "standing_stop_state": standing_stop_state,
                                },
                            )

                if not position_id:
                    log.critical(
                        "[%s] filled_order_missing_position_p0 order=%s contract=%s "
                        "(fill confirmed but no position record created — "
                        "exit engine BLIND to this position)",
                        client_id, local_id,
                        order.get("contract") or order.get("symbol"),
                    )
                    _emit_canonical_owner_handoff_failure(
                        order,
                        "",
                        {
                            "reason_code": "FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN",
                            "detail_reason": "canonical_position_create_failed",
                        },
                    )

            except Exception as exc:
                log.critical(
                    "[%s] ENTRY fill side-effects FAILED for %s — position NOT created, "
                    "exit engine BLIND to this position. Error: %s",
                    client_id, local_id, exc, exc_info=True,
                )
                _emit_canonical_owner_handoff_failure(
                    order,
                    str(position_id or ""),
                    {
                        "reason_code": "FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN",
                        "detail_reason": "entry_fill_side_effects_exception",
                        "exception_type": type(exc).__name__,
                        "exception": str(exc),
                        "standing_stop_attempted": False,
                    },
                )

        elif ok and kind == "EXIT":
            _sync_exit_price(order, result)

        # Release equity/symbol lock only after OSM and canonical owner proof.
        # A durable FILLED row with unresolved ownership remains reserved for
        # the retry path instead of allowing new capital to race the repair.
        if ok and kind == "ENTRY" and entry_handoff_proven:
            _release_entry_guards(order)

        audit(
            client_id,
            "INFO",
            "ORDER_FILLED",
            {
                "local_order_id": local_id,
                "broker_order_id": broker_id,
                "kind": kind,
                "osm_status": mapped,
                "filled_qty": new_filled,
                "avg_fill": result.get("avg_fill"),
            },
        )
        return

    # ── PARTIAL FILL / EXIT_PARTIAL_FILL ─────────────────────────────────
    if mapped in ("PARTIAL_FILL", "EXIT_PARTIAL_FILL"):
        emit_fill_event(
            order,
            decision="PARTIAL_FILL",
            reason_code="ORDER_PARTIAL_FILL" if kind == "ENTRY" else "EXIT_PARTIAL_FILL",
            explanation=f"{kind} partially filled via broker",
            result=result,
            extra_context={"osm_status": mapped},
        )

        partial_applied = False
        if osm:
            try:
                current_status = str(order.get("status") or "").upper()
                if current_status == mapped and current_status in ("PARTIAL_FILL", "EXIT_PARTIAL_FILL"):
                    osm.apply_fill_update(
                        local_order_id=local_id,
                        cumulative_filled=new_filled,
                        fill_price=result.get("avg_fill"),
                        broker_order_id=broker_id,
                    )
                else:
                    osm.transition(
                        local_id,
                        mapped,
                        filled_qty=new_filled,
                        fill_price=result.get("avg_fill"),
                        broker_order_id=broker_id,
                    )
                partial_applied = True
            except Exception as exc:
                log.error("[%s] OSM partial update %s failed for %s: %s", client_id, mapped, local_id, exc)
        else:
            _legacy_update_order_status(local_id, "PARTIAL_FILL", filled_qty=new_filled)
            partial_applied = True

        # Canonical accounting must advance on every confirmed EXIT fill, not
        # only when the broker order becomes terminal.  Stamp the normalized
        # OSM status into the result so the reducer preserves durable in-flight
        # ownership while the broker still owns the remainder.
        if partial_applied and kind == "EXIT":
            partial_result = dict(result)
            partial_result["status"] = "EXIT_PARTIAL_FILL"
            partial_result["filled_qty"] = new_filled
            partial_result["broker_order_id"] = broker_id
            _sync_exit_price(order, partial_result)

        audit(
            client_id,
            "INFO",
            "ORDER_PARTIAL",
            {
                "local_order_id": local_id,
                "broker_order_id": broker_id,
                "kind": kind,
                "osm_status": mapped,
                "filled_qty": new_filled,
                "total_qty": int(order.get("qty") or 0),
            },
        )
        return

    # ── ACKNOWLEDGED / BROKER ACTIVE ─────────────────────────────────────
    if mapped in ("ACKNOWLEDGED", "EXIT_ACKNOWLEDGED"):
        if osm:
            try:
                current_status = str(order.get("status") or "").upper()
                if current_status != mapped:
                    osm.transition(local_id, mapped, broker_order_id=broker_id)
            except Exception as exc:
                log.error("[%s] OSM ack transition %s failed for %s: %s", client_id, mapped, local_id, exc)

        _audit_long_pending(order, kind, client_id, local_id, broker_id)
        return

    # ── TERMINAL FAILURES ────────────────────────────────────────────────
    if mapped in TERMINAL_FAILURE_STATUSES:
        emit_fill_event(
            order,
            decision="REJECT",
            reason_code=f"ORDER_{mapped}",
            explanation=f"{kind} terminal broker status: {mapped} — {result.get('reason')}",
            result=result,
            extra_context={"terminal_status": mapped},
        )

        if osm:
            try:
                osm.transition(
                    local_id,
                    mapped,
                    broker_order_id=broker_id,
                    last_error=result.get("reason"),
                )
            except Exception as exc:
                log.error("[%s] OSM terminal transition %s failed for %s: %s", client_id, mapped, local_id, exc)
        else:
            _legacy_update_order_status(local_id, mapped, error=result.get("reason"))

        # IMPORTANT: no direct positions table mutation here.
        # EXIT failure repair is handled by OSM + exit-engine hooks.

        if kind == "ENTRY":
            _release_entry_guards(order)

        audit(
            client_id,
            "WARNING",
            f"ORDER_{mapped}",
            {"local_order_id": local_id, "broker_order_id": broker_id, "kind": kind, "reason": result.get("reason")},
        )
        return

    # ── UNKNOWN / ERROR ──────────────────────────────────────────────────
    if mapped in ("UNKNOWN", "ERROR"):
        emit_fill_event(
            order,
            decision="ERROR" if mapped == "ERROR" else "ALERT",
            reason_code="BROKER_FILL_CHECK_ERROR" if mapped == "ERROR" else "BROKER_STATUS_UNKNOWN",
            explanation=f"Broker fill check unresolved: {result.get('reason')}",
            result=result,
        )
        log.warning(
            "[%s] Unresolved broker state | order=%s broker=%s status=%s reason=%s",
            client_id,
            local_id,
            broker_id,
            mapped,
            result.get("reason"),
        )
        anomaly_count = _increment_broker_anomaly_count(client_id, local_id, broker_id)

        if osm:
            osm.increment_retry(local_id)

        audit(
            client_id,
            "ERROR" if mapped == "ERROR" else "WARNING",
            "ORDER_CHECK_ERROR" if mapped == "ERROR" else "ORDER_STATUS_UNKNOWN",
            {
                "local_order_id": local_id,
                "broker_order_id": broker_id,
                "reason": result.get("reason"),
                "consecutive_count": anomaly_count,
                "escalate_after": UNKNOWN_ERROR_ESCALATE_AFTER,
            },
        )

        if anomaly_count >= UNKNOWN_ERROR_ESCALATE_AFTER:
            reason = (
                f"consecutive broker {mapped} states reached {anomaly_count}/"
                f"{UNKNOWN_ERROR_ESCALATE_AFTER}: {result.get('reason')}"
            )
            log.critical(
                "[%s] Broker state anomaly escalation | order=%s broker=%s status=%s count=%s reason=%s",
                client_id,
                local_id,
                broker_id,
                mapped,
                anomaly_count,
                result.get("reason"),
            )
            emit_fill_event(
                order,
                decision="ERROR",
                reason_code="BROKER_FILL_ANOMALY",
                explanation=reason,
                result=result,
                extra_context={
                    "mapped_status": mapped,
                    "consecutive_count": anomaly_count,
                    "escalate_after": UNKNOWN_ERROR_ESCALATE_AFTER,
                    "anomaly_status": FILL_ANOMALY_STATUS,
                },
            )
            audit(
                client_id,
                "CRITICAL",
                "BROKER_FILL_ANOMALY_ESCALATED",
                {
                    "local_order_id": local_id,
                    "broker_order_id": broker_id,
                    "mapped_status": mapped,
                    "reason": result.get("reason"),
                    "consecutive_count": anomaly_count,
                    "anomaly_status": FILL_ANOMALY_STATUS,
                },
            )
            _safe_alert(
                alert_fn,
                f"[fill_monitor:{client_id}] BROKER_FILL_ANOMALY_ESCALATED | order={local_id} broker={broker_id} status={mapped} count={anomaly_count} reason={result.get('reason')}",
            )
            _mark_broker_fill_anomaly(osm, order, reason=reason, mapped=mapped)

        return

    # Fallback guard
    log.warning(
        "[%s] Unhandled broker mapped status | order=%s broker=%s mapped=%s",
        client_id,
        local_id,
        broker_id,
        mapped,
    )
    if osm:
        osm.increment_retry(local_id)


def _audit_long_pending(order: dict, kind: str, client_id: str, local_id: str, broker_id: str):
    try:
        created_raw = order.get("created_ts")
        if isinstance(created_raw, str):
            created = datetime.fromisoformat(created_raw)
        else:
            created = created_raw
        if not created:
            return
        age = (datetime.now(timezone.utc) - created).total_seconds()
        if age > 300:
            audit(
                client_id,
                "WARNING",
                "ORDER_PENDING_LONG",
                {"local_order_id": local_id, "broker_order_id": broker_id, "kind": kind, "age_seconds": age},
            )
    except Exception:
        pass


def _sync_exit_price(order: dict, result: dict):
    """
    Update proof_trades with the REAL Tradier avg_fill price.
    The execution core logs exit_option_price = limit_price at close time.
    This corrects it to the actual broker fill price and recalculates PnL.
    Runs directly against Supabase proof_trades — no dashboard API hop needed.
    """
    try:
        pos_id      = order.get("position_id")
        avg_fill    = result.get("avg_fill")
        entry_price = float(order.get("entry_price") or order.get("fill_price") or 0)
        client_id   = order.get("client_id", "")
        ticker      = (order.get("symbol") or "").upper()

        if not pos_id or avg_fill is None:
            return

        exit_px = float(avg_fill)

        # Recalculate PnL from broker fill price
        opt_pnl_pct = None
        win = None
        if entry_price > 0:
            opt_pnl_pct = round((exit_px - entry_price) / entry_price * 100, 2)
            win = opt_pnl_pct > 0

        # 1. Update proof_trades directly in Supabase
        try:
            from ap.db import conn, run_with_retry
            def _update_proof():
                with conn() as c:
                    params = [exit_px]
                    set_clause = "exit_option_price = %s"
                    if opt_pnl_pct is not None:
                        set_clause += ", option_pnl_pct = %s, win = %s"
                        params += [opt_pnl_pct, win]
                    params += [str(pos_id)]
                    cur = c.execute(
                        f"UPDATE proof_trades SET {set_clause} WHERE position_id = %s",
                        params,
                    )
                    primary_rowcount = getattr(cur, "rowcount", getattr(c, "rowcount", 0))
                    # Fallback: repair ONE unresolved orphan row only.
                    # Primary position_id match remains the source of truth.
                    if primary_rowcount == 0 and client_id and ticker:
                        params2 = [exit_px]
                        set2 = "exit_option_price = %s"
                        if opt_pnl_pct is not None:
                            set2 += ", option_pnl_pct = %s, win = %s"
                            params2 += [opt_pnl_pct, win]
                        params2 += [client_id, ticker]
                        cur2 = c.execute(
                            f"UPDATE proof_trades SET {set2} "
                            "WHERE id = ("
                            "  SELECT id FROM proof_trades "
                            "  WHERE client_email = %s "
                            "    AND ticker = %s "
                            "    AND closed_at >= NOW() - INTERVAL '60 minutes' "
                            "    AND (position_id IS NULL OR position_id = '') "
                            "    AND exit_option_price IS NULL "
                            "  ORDER BY closed_at DESC "
                            "  LIMIT 1"
                            ")",
                            params2,
                        )
                        return getattr(cur2, "rowcount", getattr(c, "rowcount", 0))
                    return primary_rowcount
            updated = run_with_retry(_update_proof) or 0
            if updated:
                log.info(
                    "[%s] EXIT PRICE SYNCED | %s broker_fill=$%.4f entry=$%.4f pnl=%.1f%% win=%s",
                    client_id, ticker, exit_px, entry_price,
                    opt_pnl_pct if opt_pnl_pct is not None else 0,
                    win,
                )
            else:
                log.warning(
                    "[%s] EXIT PRICE SYNC: no eligible row found "
                    "(primary by position_id=%s and narrowed fallback both empty)",
                    client_id,
                    pos_id,
                )
        except Exception as db_exc:
            log.debug("[%s] proof_trades exit sync DB error (non-critical): %s", client_id, db_exc)

        # 2. Also notify dashboard API if configured (belt-and-suspenders)
        try:
            from ap.exit_price_sync import sync_exit_price_to_dashboard
            sync_exit_price_to_dashboard(
                position_id=str(pos_id),
                exit_avg_fill=exit_px,
                entry_price=entry_price if entry_price else None,
                ticker=ticker,
            )
        except ImportError:
            pass
        except Exception:
            pass

    except Exception as exc:
        log.debug("[%s] _sync_exit_price failed (non-critical): %s", order.get("client_id"), exc)


# =============================================================================
# MAIN LOOP — STOP-EVENT SAFE
# =============================================================================

def fill_monitor_loop(
    broker: BrokerAdapter,
    poll_seconds: float = 10.0,
    osm=None,
    pm=None,
    exit_engine=None,
    stop_event=None,
    client_id: str | None = None,
    alert_fn=None,
    data_broker=None,
    runtime_execution_mode=None,
):
    """Fill monitor must never pause on kill switch — it reconciles reality.

    PR #235 (hardening #4 + #7): data_broker is an optional argument.  When
    supplied, it becomes the quote source for underlying-at-fill reads
    (see _select_quote_broker), keeping the execution broker (paper) and
    the data broker (Polygon/prod-quotes) separated.
    """
    if PRODUCTION_MODE and ALLOW_LEGACY_FILL_MONITOR:
        raise RuntimeError("ALLOW_LEGACY_FILL_MONITOR=1 is forbidden in production/live mode")
    if osm is None and not ALLOW_LEGACY_FILL_MONITOR:
        raise RuntimeError("fill_monitor_loop requires OSM unless ALLOW_LEGACY_FILL_MONITOR=1")

    if not client_id:
        client_id = getattr(osm, "client_id", None) or getattr(pm, "client_id", None)
    if not client_id:
        raise ValueError("fill_monitor_loop requires client_id or osm/pm with client_id")

    log.info(
        "Fill monitor started | client_id=%s osm=%s pm=%s ee=%s",
        client_id,
        "wired" if osm else "legacy-fallback",
        "wired" if pm else "none",
        "wired" if exit_engine else "none",
    )
    while not (stop_event and stop_event.is_set()):
        try:
            # F3: resolve per iteration.  Resolving once before the loop meant
            # an exit_engine/master_control whose mode was not yet hydrated at
            # startup pinned the fence to "" for the entire process lifetime,
            # holding every broker-owned recovery forever — reproducing the
            # exact stranded-row condition this PR exists to close.
            resolved_runtime_execution_mode = _resolve_runtime_execution_mode(
                runtime_execution_mode=runtime_execution_mode,
                exit_engine=exit_engine,
            )
            normal_pending = get_pending_orders(client_id)
            recovery_pending = get_broker_owned_exit_requests(client_id)
            pending = []
            processed_local_order_ids = set()
            # Prefer the dedicated recovery snapshot when a concurrent query
            # returns the same local order in both result sets.  It is the
            # only path allowed to normalize EXIT_REQUESTED ownership.
            for order in recovery_pending + normal_pending:
                local_order_id = str(order.get("local_order_id") or "").strip()
                if local_order_id and local_order_id in processed_local_order_ids:
                    continue
                if local_order_id:
                    processed_local_order_ids.add(local_order_id)
                pending.append(order)
            for order in pending:
                try:
                    process_pending_order(
                        broker,
                        order,
                        osm=osm,
                        pm=pm,
                        exit_engine=exit_engine,
                        alert_fn=alert_fn,
                        data_broker=data_broker,
                        runtime_execution_mode=resolved_runtime_execution_mode,
                    )
                except Exception as exc:
                    log.exception("Failed to process order %s: %s", order.get("local_order_id"), exc)

            # Idle gate: when no orders are in flight, poll slowly to reduce
            # Tradier API calls across 10 concurrent clients. 10 clients × 6
            # calls/min = 60 wasted calls/min hitting broker for nothing.
            # When active orders exist, use the normal fast poll cadence.
            _sleep = poll_seconds if pending else min(poll_seconds * 3, 30.0)
            if stop_event:
                stop_event.wait(_sleep)
            else:
                time.sleep(_sleep)

        except Exception as exc:
            log.exception("Fill monitor loop error: %s", exc)
            if stop_event:
                stop_event.wait(poll_seconds * 2)
            else:
                time.sleep(poll_seconds * 2)


# =============================================================================
# LEGACY FALLBACK HELPERS (deprecated — used only when ALLOW_LEGACY_FILL_MONITOR=1)
# =============================================================================

def _legacy_update_order_status(
    local_order_id: str,
    status: str,
    filled_qty: int | None = None,
    error: str | None = None,
):
    """DEPRECATED: Direct DB write. Use osm.transition() instead."""
    if not ALLOW_LEGACY_FILL_MONITOR:
        raise RuntimeError("legacy fill monitor path disabled")

    updates = ["status=%s", "updated_ts=%s"]
    params = [status, now_utc_iso()]
    if filled_qty is not None:
        updates.append("filled_qty=%s")
        params.append(int(filled_qty))
    if error is not None:
        updates.append("last_error=%s")
        params.append(error)
    params.append(local_order_id)
    sql = f"UPDATE orders SET {', '.join(updates)} WHERE local_order_id=%s"
    def _fn():
        with conn() as c:
            c.execute(sql, params)
    run_with_retry(_fn)


def _legacy_create_position_from_fill(order: dict, avg_fill_price: float, filled_qty: int):
    """DEPRECATED: Direct DB write. OSM/PM path should handle position opening."""
    if not ALLOW_LEGACY_FILL_MONITOR:
        raise RuntimeError("legacy fill monitor path disabled")

    import uuid

    pos_id = str(uuid.uuid4())
    client_id = order["client_id"]

    # PR #235 (hardening #2): resolve side without guessing.  This legacy
    # fallback path is guarded by ALLOW_LEGACY_FILL_MONITOR=1 already, but
    # even in that mode we must not persist a fabricated direction.
    _leg_side, _leg_side_source = _resolve_order_option_side(order)
    if not _leg_side:
        _emit_side_unresolved(order, reason_code="LEGACY_POSITION_SIDE_UNRESOLVED")
        raise RuntimeError("LEGACY_POSITION_SIDE_UNRESOLVED")
    direction = _leg_side

    def _insert_pos():
        with conn() as c:
            c.execute(
                """
                INSERT INTO positions (
                    id, client_id, underlying, contract, direction, qty, avg_fill,
                    entry_ts, tp_pct, sl_pct, status
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    pos_id,
                    client_id,
                    order["symbol"],
                    order["contract"],
                    direction,
                    int(filled_qty),
                    float(avg_fill_price),
                    now_utc_iso(),
                    float(cfg.TAKE_PROFIT_PCT),
                    float(cfg.STOP_LOSS_PCT),
                    "OPEN",
                ),
            )
    run_with_retry(_insert_pos)

    def _link_order():
        with conn() as c:
            c.execute(
                "UPDATE orders SET position_id=%s WHERE local_order_id=%s",
                (pos_id, order["local_order_id"]),
            )
    run_with_retry(_link_order)

    audit(
        client_id,
        "INFO",
        "POSITION_CREATED_FROM_FILL_LEGACY",
        {
            "position_id": pos_id,
            "local_order_id": order["local_order_id"],
            "contract": order["contract"],
            "qty": int(filled_qty),
            "avg_fill": float(avg_fill_price),
        },
    )
    return pos_id


def _legacy_close_position_from_exit_fill(order: dict, avg_fill_price: float):
    """DEPRECATED: Direct DB write. OSM.transition(EXIT_FILLED) via exit engine should handle this."""
    if not ALLOW_LEGACY_FILL_MONITOR:
        raise RuntimeError("legacy fill monitor path disabled")

    client_id = order["client_id"]
    position_id = order.get("position_id")

    if not position_id:
        log.error("Exit order has no position_id: %s", order.get("local_order_id"))
        return

    def _fetch_pos():
        with conn() as c:
            return c.execute(
                "SELECT * FROM positions WHERE id=%s AND client_id=%s",
                (position_id, client_id),
            ).fetchone()
    pos_row = run_with_retry(_fetch_pos)

    if not pos_row:
        log.error("Position not found: %s", position_id)
        return

    pos = dict(pos_row)
    entry_price = float(pos["avg_fill"])
    qty = int(pos["qty"])
    exit_px = float(avg_fill_price)
    realized_pnl = (exit_px - entry_price) * qty * OPT_MULTIPLIER
    realized_pnl_pct = round(((exit_px - entry_price) / entry_price) * 100, 2) if entry_price > 0 else 0.0

    def _close_pos():
        with conn() as c:
            c.execute(
                """
                UPDATE positions
                SET status='CLOSED', exit_ts=%s, exit_price=%s,
                    realized_pnl=%s, realized_pnl_pct=%s,
                    close_source=%s, close_confidence=%s
                WHERE id=%s AND client_id=%s
                """,
                (
                    now_utc_iso(),
                    exit_px,
                    float(realized_pnl),
                    realized_pnl_pct,
                    "FILL_MONITOR",
                    "HIGH",
                    position_id,
                    client_id,
                ),
            )
    run_with_retry(_close_pos)

    def _update_pnl():
        with conn() as c:
            c.execute(
                "UPDATE client_state "
                "SET realized_pnl_today = COALESCE(realized_pnl_today, 0.0) + %s "
                "WHERE client_id=%s",
                (float(realized_pnl), client_id),
            )
    run_with_retry(_update_pnl)

    audit(
        client_id,
        "INFO",
        "FINALIZED_TRADE_FROM_FILL_MONITOR",
        {
            "position_id": position_id,
            "contract": pos["contract"],
            "entry_price": entry_price,
            "exit_price": exit_px,
            "qty": qty,
            "realized_pnl": float(realized_pnl),
            "realized_pnl_pct": realized_pnl_pct,
            "close_source": "FILL_MONITOR",
            "close_confidence": "HIGH",
        },
    )

    try:
        epx = exit_px
        pct = realized_pnl_pct
        win = realized_pnl_pct > 0
        cid = client_id

        def _update_proof():
            with conn() as c2:
                c2.execute(
                    """
                    UPDATE proof_trades
                    SET exit_option_price = %s,
                        option_pnl_pct    = %s,
                        win               = %s
                    WHERE client_email = %s
                      AND closed_at >= NOW() - INTERVAL '4 hours'
                      AND ABS(COALESCE(exit_option_price,0) - %s) > 0.05
                    """,
                    (epx, pct, win, cid, epx),
                )
                return c2.rowcount

        updated = run_with_retry(_update_proof) or 0
        if updated:
            log.info("[%s] proof_trades corrected with actual fill $%.4f pnl=%.1f%%", client_id, exit_px, realized_pnl_pct)
    except Exception as exc:
        log.debug("[%s] proof_trades correction (non-critical): %s", client_id, exc)
