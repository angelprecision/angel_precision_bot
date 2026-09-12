"""Narrow durable convergence for terminal ENTRY order truth.

This module is intentionally limited to the bookkeeping projection from an
exact terminal ``orders`` ENTRY row to its exact ``trade_queue`` row.  It does
not read a broker, execute retries, evict watchers, create positions, or write
proof rows.  Those remain owned by their existing lifecycle components.
"""
from __future__ import annotations

import copy
import json
import logging
from dataclasses import dataclass, replace
from typing import Any

from ap_canonical_signal import build_canonical_signal_id
from ap.utils import now_utc_iso

log = logging.getLogger("ap.trade_queue_lifecycle_reconciler")

KEEP_NONTERMINAL = "KEEP_NONTERMINAL"
TERMINALIZE = "TERMINALIZE"

_QUEUE_TERMINAL_STATUSES = frozenset({
    "REJECTED", "ERROR", "SUBMITTED", "FILLED", "CANCELED", "CANCELLED",
    "EXPIRED", "DONE", "ARCHIVED",
})
_ENTRY_TERMINAL_STATUSES = frozenset({"FILLED", "REJECTED", "CANCELED", "CANCELLED", "EXPIRED", "ERROR"})
_QUEUE_ACTIVE_STATUSES = frozenset({"NEW", "PROCESSING", "WATCHING", "ARMED", "TRIGGERED", "PENDING_TRIGGER"})


@dataclass(frozen=True)
class QueueLifecycleDecision:
    """Pure classification result; persistence is reported separately."""

    action: str
    target_status: str | None
    reason_code: str
    authority_source: str
    evidence: dict[str, Any]
    mutated: bool = False
    outcome: str = "CLASSIFIED"


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _object(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return copy.deepcopy(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return None
        return copy.deepcopy(parsed) if isinstance(parsed, dict) else None
    if value is None:
        return {}
    return None


def _mode_from_sources(sources: tuple[tuple[str, Any], ...]) -> tuple[str | None, str | None]:
    modes: list[tuple[str, str]] = []
    for name, raw in sources:
        if raw is None or not _text(raw):
            continue
        mode = _text(raw).lower()
        if mode not in {"live", "paper"}:
            return None, f"{name}_invalid"
        modes.append((name, mode))
    if not modes:
        return None, "missing"
    distinct = {mode for _, mode in modes}
    if len(distinct) != 1:
        return None, "conflict"
    return modes[0][1], None


def _canonical_from_sources(
    *,
    signal_id: str,
    explicit_sources: tuple[tuple[str, Any], ...],
) -> tuple[str | None, str | None]:
    explicit: list[tuple[str, str]] = []
    for name, raw in explicit_sources:
        if raw is None or not _text(raw):
            continue
        explicit.append((name, _text(raw)))
    if explicit and len({value for _, value in explicit}) != 1:
        return None, "conflict"
    canonical = explicit[0][1] if explicit else build_canonical_signal_id(signal_id)
    if not canonical:
        return None, "missing"
    return canonical, None


def _optional_positive_id_sources(
    sources: tuple[tuple[str, Any], ...],
) -> tuple[int | None, str | None]:
    values: list[tuple[str, int]] = []
    for name, raw in sources:
        if raw is None or not _text(raw):
            continue
        if isinstance(raw, bool):
            return None, f"{name}_invalid"
        if isinstance(raw, int):
            parsed = raw
        elif isinstance(raw, str) and raw.strip() == raw and raw.isdigit():
            parsed = int(raw)
        else:
            return None, f"{name}_invalid"
        if parsed <= 0 or str(parsed) != _text(raw):
            return None, f"{name}_invalid"
        values.append((name, parsed))
    if not values:
        return None, None
    if len({value for _, value in values}) != 1:
        return None, "conflict"
    return values[0][1], None


def _proven_broker_id(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    token = _text(value)
    return bool(token) and token.lower() not in {"0", "n/a", "na", "none", "unknown", "null"}


def _positive_filled_qty(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    try:
        return int(value) > 0
    except (TypeError, ValueError):
        return False


def _protected_broker_submit_state(order: dict[str, Any], meta: dict[str, Any]) -> bool:
    if bool(meta.get("reconciliation_required")) or bool(meta.get("split_brain_quarantine")):
        return True
    if _text(order.get("last_error")).startswith("SPLIT_BRAIN:"):
        return True
    if _text(meta.get("lifecycle_state")).upper() in {"SUBMITTING", "BROKER_UNKNOWN", "AMBIGUOUS"}:
        return True
    for key in ("submit_intent_at", "broker_submit_key", "recovery_submit_owner"):
        if _text(meta.get(key)):
            return True
    return False


def _hold(reason_code: str, evidence: dict[str, Any] | None = None) -> QueueLifecycleDecision:
    return QueueLifecycleDecision(
        action=KEEP_NONTERMINAL,
        target_status=None,
        reason_code=reason_code,
        authority_source="none",
        evidence=evidence or {},
    )


def classify_queue_lifecycle(
    *,
    queue_row: dict[str, Any],
    durable_state: dict[str, Any],
    expected_client_id: str | None = None,
) -> QueueLifecycleDecision:
    """Classify one queue row against exact durable ENTRY truth.

    The function is side-effect free.  Missing, malformed, or conflicting
    identity is always a hold; no ticker-only or age-only inference exists.
    """
    queue = dict(queue_row or {})
    order = dict(durable_state or {})
    queue_payload = _object(queue.get("payload"))
    queue_result = _object(queue.get("result_json"))
    order_meta = _object(order.get("meta"))
    if queue_payload is None or queue_result is None or order_meta is None:
        return _hold("IDENTITY_UNPROVEN_MALFORMED_JSON")

    queue_status = _text(queue.get("status")).upper()
    order_status = _text(order.get("status")).upper()
    local_order_id = _text(order.get("local_order_id"))
    order_client = _text(order.get("client_id")).lower()
    queue_client = _text(queue.get("client_id")).lower()
    expected_client = _text(expected_client_id).lower()
    signal_id = _text(order.get("signal_id"))
    queue_signal_id = _text(queue.get("signal_id"))

    if not local_order_id or not order_client or not queue_client or not signal_id or not queue_signal_id:
        return _hold("IDENTITY_UNPROVEN_MISSING_REQUIRED_IDENTITY")
    if expected_client and (order_client != expected_client or queue_client != expected_client):
        return _hold("IDENTITY_UNPROVEN_CLIENT_MISMATCH")
    if order_client != queue_client:
        return _hold("IDENTITY_UNPROVEN_CLIENT_CONFLICT")
    if _text(order.get("kind")).upper() != "ENTRY":
        return _hold("IDENTITY_UNPROVEN_NOT_ENTRY")

    order_mode, order_mode_error = _mode_from_sources((
        ("order.execution_mode", order.get("execution_mode")),
        ("order.meta.execution_mode", order_meta.get("execution_mode")),
    ))
    queue_mode, queue_mode_error = _mode_from_sources((
        ("queue.execution_mode", queue.get("execution_mode")),
        ("queue.result_json.execution_mode", queue_result.get("execution_mode")),
        ("queue.payload.execution_mode", queue_payload.get("execution_mode")),
        ("queue.payload.mode", queue_payload.get("mode")),
    ))
    if order_mode_error:
        return _hold(f"IDENTITY_UNPROVEN_ORDER_MODE_{order_mode_error.upper()}")
    if queue_mode_error:
        return _hold(f"IDENTITY_UNPROVEN_QUEUE_MODE_{queue_mode_error.upper()}")
    if order_mode != queue_mode:
        return _hold("IDENTITY_UNPROVEN_EXECUTION_MODE_MISMATCH")

    order_canonical, order_canonical_error = _canonical_from_sources(
        signal_id=signal_id,
        explicit_sources=(
            ("order.canonical_signal_id", order.get("canonical_signal_id")),
            ("order.meta.canonical_signal_id", order_meta.get("canonical_signal_id")),
        ),
    )
    queue_canonical, queue_canonical_error = _canonical_from_sources(
        signal_id=queue_signal_id,
        explicit_sources=(
            ("queue.result_json.canonical_signal_id", queue_result.get("canonical_signal_id")),
            ("queue.payload.canonical_signal_id", queue_payload.get("canonical_signal_id")),
        ),
    )
    if order_canonical_error or queue_canonical_error:
        return _hold("IDENTITY_UNPROVEN_CANONICAL_SIGNAL_ID")
    if order_canonical != queue_canonical:
        return _hold("IDENTITY_UNPROVEN_CANONICAL_SIGNAL_MISMATCH")

    stored_local_id = _text(queue_result.get("local_order_id"))
    if stored_local_id != local_order_id:
        return _hold("IDENTITY_UNPROVEN_LOCAL_ORDER_ID_MISMATCH")
    queue_id = queue.get("id")
    linked_queue_id, queue_id_error = _optional_positive_id_sources((
        ("order.meta.queue_id", order_meta.get("queue_id")),
        ("order.meta.trade_queue_id", order_meta.get("trade_queue_id")),
    ))
    if queue_id_error:
        return _hold("IDENTITY_UNPROVEN_QUEUE_ID_" + queue_id_error.upper())
    if linked_queue_id is None and not stored_local_id:
        return _hold("IDENTITY_UNPROVEN_QUEUE_LINK")
    if linked_queue_id is not None and str(queue_id) != str(linked_queue_id):
        return _hold("IDENTITY_UNPROVEN_QUEUE_ID_MISMATCH")

    evidence = {
        "client_id": order_client,
        "execution_mode": order_mode,
        "signal_id": signal_id,
        "canonical_signal_id": order_canonical,
        "local_order_id": local_order_id,
        "queue_id": queue_id,
        "order_status": order_status,
        "queue_status": queue_status,
    }

    if queue_status in _QUEUE_TERMINAL_STATUSES:
        return QueueLifecycleDecision(
            action=KEEP_NONTERMINAL,
            target_status=queue_status,
            reason_code="QUEUE_ALREADY_TERMINAL",
            authority_source="trade_queue",
            evidence=evidence,
        )
    if queue_status not in _QUEUE_ACTIVE_STATUSES:
        return _hold("QUEUE_STATUS_UNEXPECTED", evidence)
    if order_status not in _ENTRY_TERMINAL_STATUSES:
        return QueueLifecycleDecision(
            action=KEEP_NONTERMINAL,
            target_status=None,
            reason_code="ENTRY_LIFECYCLE_ACTIVE",
            authority_source="orders",
            evidence=evidence,
        )
    if _protected_broker_submit_state(order, order_meta):
        return _hold("BROKER_TRUTH_UNRESOLVED", evidence)

    if order_status == "FILLED":
        if not (
            _proven_broker_id(order.get("broker_order_id"))
            or (
                _positive_filled_qty(order.get("filled_qty"))
                and order.get("filled_ts") is not None
                and order.get("fill_price") is not None
            )
        ):
            return _hold("ENTRY_FILL_IDENTITY_UNPROVEN", evidence)
        reason_code = "ENTRY_BROKER_FILL_CONFIRMED"
        target_status = "FILLED"
    elif order_status == "CANCELLED":
        reason_code = "ENTRY_TERMINAL_NO_FILL:CANCELED"
        target_status = "CANCELED"
    else:
        reason_code = f"ENTRY_TERMINAL_NO_FILL:{order_status}"
        target_status = order_status

    evidence = {
        **evidence,
        "broker_order_id": order.get("broker_order_id"),
        "position_id": order.get("position_id"),
    }
    return QueueLifecycleDecision(
        action=TERMINALIZE,
        target_status=target_status,
        reason_code=reason_code,
        authority_source="orders",
        evidence=evidence,
    )


def _conn_factory():
    from ap.db import conn
    return conn


def _run_with_retry(fn):
    from ap.db import run_with_retry
    # Queue projection is non-blocking bookkeeping attached to an already
    # durable order transition.  Keep its retry budget short so a DB outage
    # cannot stall the order-state worker or cause a retry storm.
    return run_with_retry(fn, retries=2, base_sleep=0.05, max_sleep=0.25)


def _queue_rows_for_order(order: dict[str, Any], *, queue_id: int | None) -> list[dict[str, Any]]:
    local_order_id = _text(order.get("local_order_id"))
    client_id = _text(order.get("client_id"))
    signal_id = _text(order.get("signal_id"))

    def _fetch():
        with _conn_factory()() as c:
            if queue_id is not None:
                c.execute(
                    "SELECT id, client_id, signal_id, status, payload, result_json, last_error "
                    "FROM trade_queue WHERE id=%s AND client_id=%s",
                    (queue_id, client_id),
                )
            else:
                c.execute(
                    "SELECT id, client_id, signal_id, status, payload, result_json, last_error "
                    "FROM trade_queue WHERE client_id=%s AND signal_id=%s "
                    "AND result_json->>'local_order_id'=%s",
                    (client_id, signal_id, local_order_id),
                )
            return [dict(row) for row in (c.fetchall() or [])]

    return _run_with_retry(_fetch) or []


def _queue_row_by_id(queue_row: dict[str, Any]) -> dict[str, Any] | None:
    queue_id = queue_row.get("id")
    client_id = _text(queue_row.get("client_id"))

    def _fetch():
        with _conn_factory()() as c:
            c.execute(
                "SELECT id, client_id, signal_id, status, payload, result_json, last_error "
                "FROM trade_queue WHERE id=%s AND client_id=%s",
                (queue_id, client_id),
            )
            row = c.fetchone()
            return dict(row) if row else None

    return _run_with_retry(_fetch)


def _terminalize_queue_row(
    *,
    queue_row: dict[str, Any],
    order: dict[str, Any],
    decision: QueueLifecycleDecision,
) -> QueueLifecycleDecision:
    queue_status = _text(queue_row.get("status")).upper()
    mode = _text(decision.evidence.get("execution_mode")).lower()
    local_order_id = _text(order.get("local_order_id"))
    client_id = _text(order.get("client_id"))
    signal_id = _text(order.get("signal_id"))
    diagnostics = {
        "queue_lifecycle_reconciled": True,
        "queue_lifecycle_previous_status": queue_status,
        "queue_lifecycle_target_status": decision.target_status,
        "queue_lifecycle_reason": decision.reason_code,
        "queue_lifecycle_authority": decision.authority_source,
        "queue_lifecycle_execution_mode": mode,
        "queue_lifecycle_local_order_id": local_order_id,
        "queue_lifecycle_broker_order_id": order.get("broker_order_id"),
        "queue_lifecycle_position_id": order.get("position_id"),
        "queue_lifecycle_reconciled_at": now_utc_iso(),
    }
    last_error = _text(order.get("last_error"))
    if not last_error and decision.target_status != "FILLED":
        last_error = decision.reason_code
    stored_local_id = _text((_object(queue_row.get("result_json")) or {}).get("local_order_id"))

    def _update():
        with _conn_factory()() as c:
            c.execute(
                """
                UPDATE trade_queue
                   SET status=%s,
                       finished_ts=NOW(),
                       result_json=COALESCE(result_json, '{}'::jsonb) || %s::jsonb,
                       last_error=%s
                 WHERE id=%s
                   AND client_id=%s
                   AND signal_id=%s
                   AND status=%s
                   AND result_json->>'local_order_id'=%s
                   AND LOWER(TRIM(COALESCE(
                       NULLIF(result_json->>'execution_mode',''),
                       NULLIF(payload->>'execution_mode',''),
                       NULLIF(payload->>'mode',''),
                       '')))=%s
                """,
                (
                    decision.target_status,
                    json.dumps(diagnostics, separators=(",", ":"), default=str),
                    last_error,
                    queue_row.get("id"),
                    client_id,
                    signal_id,
                    queue_status,
                    stored_local_id,
                    mode,
                ),
            )
            return getattr(c, "rowcount", None)

    rowcount = _run_with_retry(_update)
    if rowcount == 1:
        return replace(decision, mutated=True, outcome="MUTATED")
    if rowcount not in (0, None):
        return replace(decision, outcome="CAS_ROWCOUNT_UNEXPECTED")
    if rowcount is None:
        return replace(decision, outcome="CAS_ROWCOUNT_UNCONFIRMED")

    latest = _queue_row_by_id(queue_row)
    if latest is None:
        return replace(decision, outcome="CAS_LOST_ROW_MISSING")
    latest_status = _text(latest.get("status")).upper()
    if latest_status == decision.target_status:
        return replace(decision, mutated=False, outcome="ALREADY_TERMINAL")
    return replace(decision, mutated=False, outcome=f"CAS_LOST_TO_{latest_status or 'UNKNOWN'}")


def reconcile_entry_order_transition(
    *,
    order: dict[str, Any],
    expected_client_id: str | None = None,
) -> QueueLifecycleDecision:
    """Project a terminal ENTRY transition into one exact queue row.

    Database failures and identity ambiguity become a non-mutating hold.  The
    caller's order transition has already succeeded and must not be rolled
    back by this bookkeeping projection.
    """
    normalized_order = dict(order or {})
    status = _text(normalized_order.get("status")).upper()
    if status not in _ENTRY_TERMINAL_STATUSES:
        return QueueLifecycleDecision(
            action=KEEP_NONTERMINAL,
            target_status=None,
            reason_code="ENTRY_LIFECYCLE_ACTIVE",
            authority_source="orders",
            evidence={"order_status": status},
        )

    order_meta = _object(normalized_order.get("meta"))
    if order_meta is None:
        return _hold("IDENTITY_UNPROVEN_MALFORMED_ORDER_META")
    queue_id, queue_id_error = _optional_positive_id_sources((
        ("order.meta.queue_id", order_meta.get("queue_id")),
        ("order.meta.trade_queue_id", order_meta.get("trade_queue_id")),
    ))
    if queue_id_error:
        return _hold("IDENTITY_UNPROVEN_QUEUE_ID_" + queue_id_error.upper())

    try:
        rows = _queue_rows_for_order(normalized_order, queue_id=queue_id)
    except Exception as exc:
        log.error(
            "queue lifecycle lookup held | client=%s local_order_id=%s error=%s",
            normalized_order.get("client_id"), normalized_order.get("local_order_id"), exc,
        )
        return _hold("QUEUE_LOOKUP_FAILED")
    if not rows:
        return _hold("QUEUE_ROW_NOT_FOUND")
    if len(rows) != 1:
        return _hold("QUEUE_ROW_IDENTITY_AMBIGUOUS", {"matches": len(rows)})

    decision = classify_queue_lifecycle(
        queue_row=rows[0],
        durable_state=normalized_order,
        expected_client_id=expected_client_id,
    )
    if decision.action != TERMINALIZE:
        return decision
    try:
        return _terminalize_queue_row(queue_row=rows[0], order=normalized_order, decision=decision)
    except Exception as exc:
        log.error(
            "queue lifecycle mutation held | client=%s local_order_id=%s error=%s",
            normalized_order.get("client_id"), normalized_order.get("local_order_id"), exc,
        )
        return replace(decision, mutated=False, outcome="QUEUE_UPDATE_FAILED")


__all__ = [
    "KEEP_NONTERMINAL",
    "TERMINALIZE",
    "QueueLifecycleDecision",
    "classify_queue_lifecycle",
    "reconcile_entry_order_transition",
]
