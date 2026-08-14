"""Canonical broker-fill reconciliation for EXIT orders.

This guard replaces the legacy proof-only EXIT sync with one transactionally
consistent projection from broker-confirmed EXIT order fills. It never submits
or cancels orders; it runs only after OSM has accepted an EXIT fill.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from typing import Any, Iterable

from ap.db import conn, run_with_retry
from ap.logger import get_logger
from ap.operator.live_execution_journal import (
    PRICE_SOURCE_PAPER_BROKER,
    PRICE_SOURCE_TRADIER_ENTRY,
    PRICE_SOURCE_TRADIER_EXIT,
    classify_official,
)

log = get_logger("ap.exit_fill_truth_guard")

_PATCHED_ATTR = "_AP_CANONICAL_EXIT_FILL_TRUTH_PATCHED"
_ORIGINAL_ATTR = "_AP_CANONICAL_EXIT_FILL_TRUTH_ORIGINAL"
_TERMINAL_POSITION_STATUSES = {
    "CLOSED", "CLOSED_REPAIR", "EXPIRED", "STOPPED", "TAKEN_PROFIT",
    "ERROR", "CANCELED", "CANCELLED",
}
_EXIT_FILL_STATUSES = ("EXIT_FILLED", "EXIT_PARTIAL_FILL")
_PARTIAL_RESULT_STATUSES = {
    "PARTIAL_FILL", "PARTIALLY_FILLED", "PARTIAL", "EXIT_PARTIAL_FILL",
}
_RECONCILIATION_STALE_ATTEMPT_LEASE = timedelta(minutes=5)


class LifecycleProjectionError(ValueError):
    """Broker fill rows cannot describe exactly one valid position lifecycle."""


class ReconciliationIdentityError(LifecycleProjectionError):
    def __init__(self, reason_code: str, candidate_position_ids: Iterable[Any] = ()) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code
        self.candidate_position_ids = [
            str(value) for value in candidate_position_ids if str(value or "").strip()
        ]


class PositionUpdateCardinalityError(LifecycleProjectionError):
    def __init__(self, row_count: int) -> None:
        self.row_count = int(row_count)
        reason = (
            "POSITION_UPDATE_ZERO_ROWS"
            if self.row_count == 0
            else "POSITION_UPDATE_MULTIPLE_ROWS"
        )
        super().__init__(reason)


@dataclass(frozen=True)
class LifecycleProjection:
    exited_qty: int
    remaining_qty: int
    weighted_exit_price: float
    realized_pnl: float
    realized_pnl_pct: float
    final_fill_ts: Any
    closed: bool


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def project_position_from_exit_fills(position: dict, fills: Iterable[dict]) -> LifecycleProjection:
    """Pure cumulative fill reducer using one row per EXIT order."""
    qty = _int(position.get("qty") or position.get("contracts"))
    entry_price = _float(position.get("avg_fill") or position.get("entry_price"))
    if qty <= 0:
        raise LifecycleProjectionError("position_qty_missing_or_invalid")
    if entry_price <= 0:
        raise LifecycleProjectionError("position_entry_price_missing_or_invalid")

    normalized: list[tuple[Any, int, float]] = []
    for row in fills:
        fill_qty = _int(row.get("filled_qty"))
        fill_price = _float(row.get("fill_price"))
        if fill_qty > 0 and fill_price > 0:
            normalized.append((row.get("filled_ts"), fill_qty, fill_price))
    normalized.sort(key=lambda item: str(item[0] or ""))

    exited_qty = sum(item[1] for item in normalized)
    if exited_qty <= 0:
        raise LifecycleProjectionError("no_positive_exit_fills")
    if exited_qty > qty:
        raise LifecycleProjectionError(f"exit_overfill:{exited_qty}>{qty}")

    proceeds = sum(fill_qty * fill_price * 100.0 for _, fill_qty, fill_price in normalized)
    cost = entry_price * exited_qty * 100.0
    realized_pnl = proceeds - cost
    remaining_qty = qty - exited_qty
    return LifecycleProjection(
        exited_qty=exited_qty,
        remaining_qty=remaining_qty,
        weighted_exit_price=round(proceeds / (exited_qty * 100.0), 6),
        realized_pnl=round(realized_pnl, 2),
        realized_pnl_pct=round((realized_pnl / cost * 100.0) if cost > 0 else 0.0, 4),
        final_fill_ts=normalized[-1][0],
        closed=remaining_qty == 0,
    )


def official_live_eligibility(
    *,
    execution_mode: str,
    closed: bool,
    entry_broker_order_id: str,
    exit_broker_order_id: str,
    all_exit_fills_broker_backed: bool,
    entry_filled_qty: int = 1,
    exit_filled_qty: int = 1,
    entry_fill_price: float = 1.0,
    exit_fill_price: float = 1.0,
    synthetic_entry: bool = False,
) -> bool:
    """Apply the repository's existing Tradier Exit Proof Lock exactly."""
    if not closed or not all_exit_fills_broker_backed:
        return False
    mode = str(execution_mode or "").strip().lower()
    row = {
        "execution_mode": mode,
        "broker_reconciled": bool(entry_broker_order_id and exit_broker_order_id),
        "synthetic_entry": bool(synthetic_entry),
        "entry_price_source": (
            PRICE_SOURCE_TRADIER_ENTRY if mode == "live" else PRICE_SOURCE_PAPER_BROKER
        ),
        "exit_price_source": (
            PRICE_SOURCE_TRADIER_EXIT if mode == "live" else PRICE_SOURCE_PAPER_BROKER
        ),
        "broker_entry_order_id": str(entry_broker_order_id or ""),
        "broker_exit_order_id": str(exit_broker_order_id or ""),
        "broker_entry_filled_qty": entry_filled_qty,
        "broker_exit_filled_qty": exit_filled_qty,
        "entry_option_price": entry_fill_price,
        "exit_fill_price": exit_fill_price,
    }
    return classify_official(row).is_official


def _table_columns(c, table_name: str) -> set[str]:
    rows = c.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema='public' AND table_name=%s",
        (table_name,),
    ).fetchall()
    return {str(dict(row).get("column_name") or "") for row in rows}


def _synthetic_position_id(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return not text or text.startswith("broker-repair-")


def _resolve_position(c, order: dict, fill_ts: Any) -> dict | None:
    client_id = str(order.get("client_id") or "").strip()
    contract = str(order.get("contract") or order.get("symbol") or "").strip().upper()
    requested_id = str(order.get("position_id") or "").strip()
    if not client_id or not contract:
        raise ReconciliationIdentityError("CANONICAL_POSITION_UNRESOLVED")

    if requested_id and not _synthetic_position_id(requested_id):
        row = c.execute(
            "SELECT * FROM positions WHERE client_id=%s AND id::text=%s "
            "LIMIT 1 FOR UPDATE",
            (client_id, requested_id),
        ).fetchone()
        if row:
            resolved = dict(row)
            if str(resolved.get("contract") or "").strip().upper() == contract:
                return resolved
        # A supplied real position identity is authoritative.  Missing or
        # contract-mismatched identity is corruption, not permission to guess
        # from another same-contract lifecycle.
        raise ReconciliationIdentityError("CANONICAL_POSITION_UNRESOLVED")

    # Synthetic/null identity is recoverable only when the broker fill can be
    # bound to exactly one contemporaneous lifecycle.  Never choose the newest
    # same-contract row: multiple positions may legitimately share an OCC
    # contract, and a newest-row guess can move fills/P&L across trades.
    rows = c.execute(
        "SELECT * FROM positions WHERE client_id=%s AND UPPER(contract)=UPPER(%s) "
        "AND COALESCE(entry_ts, created_at, NOW()) <= COALESCE(%s, NOW()) "
        "AND ("
        "  UPPER(COALESCE(status,'')) NOT IN %s "
        "  OR COALESCE(exit_ts, updated_at, created_at) >= "
        "     COALESCE(%s, NOW()) - INTERVAL '10 minutes'"
        ") "
        "ORDER BY COALESCE(entry_ts, created_at) DESC LIMIT 3 FOR UPDATE",
        (client_id, contract, fill_ts, tuple(_TERMINAL_POSITION_STATUSES), fill_ts),
    ).fetchall()
    candidates = [dict(row) for row in rows]
    if len(candidates) == 1:
        return candidates[0]
    candidate_ids = [candidate.get("id") for candidate in candidates]
    if candidates:
        raise ReconciliationIdentityError(
            "CANONICAL_POSITION_AMBIGUOUS", candidate_ids
        )
    raise ReconciliationIdentityError("CANONICAL_POSITION_UNRESOLVED")


def _load_exit_fills(c, position: dict, order: dict) -> list[dict]:
    client_id = str(order.get("client_id") or "").strip()
    contract = str(position.get("contract") or order.get("contract") or "").strip().upper()
    position_id = str(position.get("id") or "").strip()
    current_local_order_id = str(order.get("local_order_id") or "").strip()
    entry_ts = position.get("entry_ts") or position.get("created_at")
    rows = c.execute(
        "SELECT local_order_id, broker_order_id, position_id, filled_qty, fill_price, filled_ts, status "
        "FROM orders WHERE client_id=%s AND kind='EXIT' AND UPPER(contract)=UPPER(%s) "
        "AND (status IN %s OR status IN ('CANCELED','REJECTED','EXPIRED','ERROR')) "
        "AND COALESCE(filled_qty,0)>0 AND fill_price IS NOT NULL "
        "AND (%s IS NULL OR filled_ts >= %s OR (%s <> '' AND local_order_id=%s)) "
        "AND (position_id::text=%s OR (%s<>'' AND local_order_id=%s)) "
        "ORDER BY filled_ts ASC NULLS LAST, created_ts ASC",
        (
            client_id, contract, _EXIT_FILL_STATUSES, entry_ts, entry_ts,
            current_local_order_id, current_local_order_id,
            position_id, current_local_order_id, current_local_order_id,
        ),
    ).fetchall()
    return [dict(row) for row in rows]


def _is_partial_result(order: dict, result: dict) -> bool:
    status = str(
        result.get("status") or result.get("state") or order.get("status") or ""
    ).strip().upper()
    return status in _PARTIAL_RESULT_STATUSES


def _load_entry_order(c, position: dict, order: dict) -> dict:
    client_id = str(order.get("client_id") or "").strip()
    position_id = str(position.get("id") or "").strip()
    local_order_id = str(position.get("local_order_id") or "").strip()
    if local_order_id:
        row = c.execute(
            "SELECT * FROM orders WHERE client_id=%s AND local_order_id=%s AND kind='ENTRY' LIMIT 1",
            (client_id, local_order_id),
        ).fetchone()
        if row:
            return dict(row)

    row = c.execute(
        "SELECT * FROM orders WHERE client_id=%s AND kind='ENTRY' AND position_id::text=%s "
        "ORDER BY filled_ts DESC NULLS LAST LIMIT 1",
        (client_id, position_id),
    ).fetchone()
    if row:
        return dict(row)

    entry_ts = position.get("entry_ts") or position.get("created_at")
    contract = str(position.get("contract") or order.get("contract") or "").strip().upper()
    if not entry_ts or not contract:
        return {}
    rows = c.execute(
        "SELECT * FROM orders WHERE client_id=%s AND kind='ENTRY' AND status='FILLED' "
        "AND UPPER(contract)=UPPER(%s) "
        "AND filled_ts BETWEEN %s - INTERVAL '10 minutes' AND %s + INTERVAL '10 minutes' "
        "ORDER BY ABS(EXTRACT(EPOCH FROM (filled_ts - %s))) ASC LIMIT 2",
        (client_id, contract, entry_ts, entry_ts, entry_ts),
    ).fetchall()
    candidates = [dict(candidate) for candidate in rows]
    return candidates[0] if len(candidates) == 1 else {}


def _dynamic_update(c, table: str, updates: dict[str, Any], where_sql: str, params: tuple[Any, ...]) -> int:
    if not updates:
        return 0
    set_sql = ", ".join(f"{column}=%s" for column in updates)
    cur = c.execute(
        f"UPDATE {table} SET {set_sql} WHERE {where_sql}",
        tuple(updates.values()) + tuple(params),
    )
    return int(getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0)


def _proof_quarantine(
    reason_code: str,
    *,
    candidate_proof_ids: Iterable[Any] = (),
    error: str = "",
) -> dict[str, Any]:
    return {
        "status": "QUARANTINED",
        "reason_code": str(reason_code),
        "error": str(error),
        "candidate_proof_ids": [
            str(value) for value in candidate_proof_ids if str(value or "").strip()
        ],
        "rows_updated": 0,
    }


def _select_canonical_proof_row(
    c,
    *,
    proof_columns: set[str],
    client_id: str,
    position_id: str,
    entry_local_order_id: str,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Lock exactly one originating proof row or return a durable quarantine."""
    if not {"id", "client_email"}.issubset(proof_columns):
        return None, _proof_quarantine(
            "PROOF_IDENTITY_UNRESOLVED",
            error="proof_trades.id/client_email unavailable",
        )

    def _rows(where_sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        selected = c.execute(
            "SELECT id, local_order_id, position_id FROM proof_trades WHERE "
            + where_sql
            + " ORDER BY id FOR UPDATE",
            params,
        ).fetchall()
        return [dict(row) for row in selected]

    entry_local_order_id = str(entry_local_order_id or "").strip()
    if entry_local_order_id and "local_order_id" in proof_columns:
        local_matches = _rows(
            "client_email=%s AND local_order_id=%s",
            (client_id, entry_local_order_id),
        )
        if len(local_matches) == 1:
            return local_matches[0], None
        if len(local_matches) > 1:
            return None, _proof_quarantine(
                "PROOF_IDENTITY_AMBIGUOUS",
                candidate_proof_ids=(row.get("id") for row in local_matches),
                error="multiple proof rows match originating ENTRY local_order_id",
            )

    if "position_id" not in proof_columns:
        return None, _proof_quarantine(
            "PROOF_IDENTITY_UNRESOLVED",
            error="proof_trades.position_id unavailable",
        )
    position_matches = _rows(
        "client_email=%s AND position_id::text=%s",
        (client_id, position_id),
    )
    if len(position_matches) == 1:
        return position_matches[0], None
    reason_code = (
        "PROOF_IDENTITY_AMBIGUOUS"
        if position_matches
        else "PROOF_IDENTITY_UNRESOLVED"
    )
    return None, _proof_quarantine(
        reason_code,
        candidate_proof_ids=(row.get("id") for row in position_matches),
        error=(
            "multiple proof rows match canonical position"
            if position_matches
            else "no proof row matches originating ENTRY or canonical position"
        ),
    )


def _update_canonical_proof_row(
    c,
    *,
    proof_columns: set[str],
    client_id: str,
    position_id: str,
    entry_local_order_id: str,
    proof_updates: dict[str, Any],
) -> tuple[int, dict[str, Any]]:
    proof_row, quarantine = _select_canonical_proof_row(
        c,
        proof_columns=proof_columns,
        client_id=client_id,
        position_id=position_id,
        entry_local_order_id=entry_local_order_id,
    )
    if quarantine is not None:
        log.critical(
            "[%s] EXIT_PROOF_RECONCILIATION_QUARANTINED position=%s entry_order=%s "
            "reason=%s candidates=%s",
            client_id,
            position_id,
            entry_local_order_id,
            quarantine.get("reason_code"),
            quarantine.get("candidate_proof_ids"),
        )
        return 0, quarantine

    proof_id = str((proof_row or {}).get("id") or "").strip()
    changed = _dynamic_update(
        c,
        "proof_trades",
        proof_updates,
        "id::text=%s AND client_email=%s",
        (proof_id, client_id),
    )
    if changed != 1:
        reason_code = (
            "PROOF_UPDATE_ZERO_ROWS"
            if changed == 0
            else "PROOF_UPDATE_MULTIPLE_ROWS"
        )
        quarantine = _proof_quarantine(
            reason_code,
            candidate_proof_ids=[proof_id],
            error=f"canonical proof update affected {changed} rows",
        )
        log.critical(
            "[%s] EXIT_PROOF_UPDATE_CARDINALITY_QUARANTINED position=%s proof=%s rows=%s",
            client_id,
            position_id,
            proof_id,
            changed,
        )
        return 0, quarantine
    return 1, {
        "status": "RECONCILED",
        "reason_code": "CANONICAL_EXIT_PROOF_RECONCILED",
        "error": "",
        "proof_id": proof_id,
        "candidate_proof_ids": [],
        "rows_updated": 1,
    }


def _reconciliation_order_identity(order: dict) -> tuple[str, str]:
    return (
        str(order.get("client_id") or "").strip(),
        str(order.get("local_order_id") or "").strip(),
    )


def _diagnostic_context(order: dict, result: dict) -> dict[str, Any]:
    return {
        "client_id": str(order.get("client_id") or ""),
        "execution_mode": str(order.get("execution_mode") or "unknown").lower(),
        "contract": str(order.get("contract") or order.get("symbol") or "").upper(),
        "supplied_position_id": str(order.get("position_id") or ""),
        "local_order_id": str(order.get("local_order_id") or ""),
        "broker_order_id": str(
            result.get("broker_order_id") or order.get("broker_order_id") or ""
        ),
        "filled_qty": _int(result.get("filled_qty"), _int(order.get("filled_qty"))),
        "fill_price": _float(result.get("fill_price"), _float(order.get("fill_price"))),
        "filled_ts": str(result.get("filled_ts") or order.get("filled_ts") or ""),
        "recorded_by": "canonical_exit_fill_reconciler",
    }


def _read_marker(c, client_id: str, local_order_id: str) -> tuple[dict, str]:
    row = c.execute(
        "SELECT meta, status FROM orders WHERE client_id=%s AND local_order_id=%s "
        "AND kind='EXIT' LIMIT 1 FOR UPDATE",
        (client_id, local_order_id),
    ).fetchone()
    if not row:
        raise LifecycleProjectionError("EXIT_ORDER_IDENTITY_UNRESOLVED")
    data = dict(row)
    meta = data.get("meta") or {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except Exception:
            meta = {}
    marker = dict(meta.get("exit_fill_reconciliation") or {}) if isinstance(meta, dict) else {}
    return marker, str(data.get("status") or "")


def _write_marker(c, client_id: str, local_order_id: str, marker: dict) -> None:
    cur = c.execute(
        "UPDATE orders SET meta=COALESCE(meta,'{}'::jsonb) || "
        "jsonb_build_object('exit_fill_reconciliation', %s::jsonb), updated_ts=NOW() "
        "WHERE client_id=%s AND local_order_id=%s AND kind='EXIT'",
        (json.dumps(marker, default=str), client_id, local_order_id),
    )
    changed = int(getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0)
    if changed != 1:
        raise LifecycleProjectionError("EXIT_RECONCILIATION_MARKER_WRITE_FAILED")


def _begin_reconciliation_attempt(order: dict, result: dict) -> int:
    client_id, local_order_id = _reconciliation_order_identity(order)
    if not client_id or not local_order_id:
        raise LifecycleProjectionError("EXIT_ORDER_IDENTITY_UNRESOLVED")

    def _tx() -> int:
        with conn() as c:
            previous, _ = _read_marker(c, client_id, local_order_id)
            attempt_count = max(0, _int(previous.get("attempt_count"))) + 1
            marker = {
                **previous,
                **_diagnostic_context(order, result),
                "status": "IN_PROGRESS",
                "reason_code": "RECONCILIATION_ATTEMPT_STARTED",
                "error": "",
                "candidate_position_ids": list(previous.get("candidate_position_ids") or []),
                "attempt_count": attempt_count,
                "last_attempt_at": datetime.now(timezone.utc).isoformat(),
            }
            _write_marker(c, client_id, local_order_id, marker)
            return attempt_count

    return int(run_with_retry(_tx))


def _parse_marker_timestamp(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _reconciliation_marker_retryable(
    marker: dict,
    now: datetime,
    *,
    stale_attempt_lease: timedelta = _RECONCILIATION_STALE_ATTEMPT_LEASE,
) -> bool:
    status = str((marker or {}).get("status") or "").strip().upper()
    if status in {"RETRY_REQUIRED", "QUARANTINED"}:
        return True
    if status != "IN_PROGRESS":
        return False
    last_attempt_at = _parse_marker_timestamp((marker or {}).get("last_attempt_at"))
    if last_attempt_at is None:
        return True
    now_utc = now if now.tzinfo is not None else now.replace(tzinfo=timezone.utc)
    return now_utc.astimezone(timezone.utc) - last_attempt_at >= stale_attempt_lease


def _claim_reconciliation_retry(
    order: dict,
    result: dict,
    *,
    now: datetime | None = None,
) -> int | None:
    """Atomically reclaim one retryable marker under the order row lock."""
    client_id, local_order_id = _reconciliation_order_identity(order)
    if not client_id or not local_order_id:
        return None
    claimed_at = now or datetime.now(timezone.utc)

    def _tx() -> int | None:
        with conn() as c:
            previous, _ = _read_marker(c, client_id, local_order_id)
            if not _reconciliation_marker_retryable(previous, claimed_at):
                return None
            attempt_count = max(0, _int(previous.get("attempt_count"))) + 1
            marker = {
                **previous,
                **_diagnostic_context(order, result),
                "status": "IN_PROGRESS",
                "reason_code": "RECONCILIATION_RETRY_CLAIMED",
                "error": "",
                "attempt_count": attempt_count,
                "last_attempt_at": claimed_at.astimezone(timezone.utc).isoformat(),
            }
            _write_marker(c, client_id, local_order_id, marker)
            return attempt_count

    claimed = run_with_retry(_tx)
    return int(claimed) if claimed is not None else None


def _failure_reason(exc: Exception) -> tuple[str, str, list[str]]:
    candidates = list(getattr(exc, "candidate_position_ids", []) or [])
    if isinstance(exc, ReconciliationIdentityError):
        return "QUARANTINED", exc.reason_code, candidates
    if isinstance(exc, PositionUpdateCardinalityError):
        status = "RETRY_REQUIRED" if exc.row_count == 0 else "QUARANTINED"
        return status, str(exc), candidates
    if isinstance(exc, LifecycleProjectionError):
        text = str(exc).upper()
        if text.startswith("EXIT_OVERFILL"):
            return "QUARANTINED", "EXIT_OVERFILL", candidates
        if text == "NO_POSITIVE_EXIT_FILLS":
            return "QUARANTINED", "NO_POSITIVE_EXIT_FILLS", candidates
        if "ENTRY" in text and "UNRESOLVED" in text:
            return "QUARANTINED", "ENTRY_IDENTITY_UNRESOLVED", candidates
    return "RETRY_REQUIRED", "RECONCILIATION_SQL_FAILURE", candidates


def _finish_reconciliation_attempt(
    order: dict,
    result: dict,
    *,
    attempt_count: int,
    reconciled_position_id: str = "",
    proof_reconciliation: dict[str, Any] | None = None,
    error: Exception | None = None,
) -> None:
    client_id, local_order_id = _reconciliation_order_identity(order)

    def _tx() -> None:
        with conn() as c:
            previous, _ = _read_marker(c, client_id, local_order_id)
            durable_attempt = _int(previous.get("attempt_count"))
            if durable_attempt != attempt_count:
                log.critical(
                    "[%s] EXIT_RECONCILIATION_STALE_FINISH_BLOCKED order=%s "
                    "worker_attempt=%s durable_attempt=%s",
                    client_id,
                    local_order_id,
                    attempt_count,
                    durable_attempt,
                )
                return
            preserved_attempts = attempt_count
            if error is None:
                proof_diagnostic = dict(proof_reconciliation or {})
                proof_quarantined = (
                    str(proof_diagnostic.get("status") or "").upper()
                    == "QUARANTINED"
                )
                now = datetime.now(timezone.utc).isoformat()
                marker = {
                    **previous,
                    **_diagnostic_context(order, result),
                    "status": "QUARANTINED" if proof_quarantined else "RECONCILED",
                    "reason_code": (
                        str(proof_diagnostic.get("reason_code") or "PROOF_IDENTITY_UNRESOLVED")
                        if proof_quarantined
                        else "CANONICAL_EXIT_FILL_RECONCILED"
                    ),
                    "error": (
                        str(proof_diagnostic.get("error") or "")
                        if proof_quarantined
                        else ""
                    ),
                    "position_id": str(reconciled_position_id or ""),
                    "position_reconciled": True,
                    "candidate_position_ids": [],
                    "candidate_proof_ids": list(
                        proof_diagnostic.get("candidate_proof_ids") or []
                    ),
                    "proof_reconciliation": proof_diagnostic,
                    "attempt_count": preserved_attempts,
                    "position_reconciled_at": now,
                    "reconciled_at": "" if proof_quarantined else now,
                    "last_attempt_at": now,
                }
            else:
                status, reason_code, candidates = _failure_reason(error)
                marker = {
                    **previous,
                    **_diagnostic_context(order, result),
                    "status": status,
                    "reason_code": reason_code,
                    "error": str(error),
                    "candidate_position_ids": candidates,
                    "attempt_count": preserved_attempts,
                    "last_attempt_at": datetime.now(timezone.utc).isoformat(),
                }
            _write_marker(c, client_id, local_order_id, marker)

    run_with_retry(_tx)


def _run_reconciliation_attempt(
    order: dict,
    result: dict,
    *,
    attempt_count: int,
) -> dict:
    client_id = str(order.get("client_id") or "").strip()
    fill_ts = result.get("filled_ts") or order.get("filled_ts") or datetime.now(timezone.utc)

    def _tx() -> dict:
        with conn() as c:
            current_status = str(order.get("status") or "")
            current_filled_qty = _int(
                result.get("filled_qty"), _int(order.get("filled_qty"))
            )
            current_fill_price = _float(result.get("avg_fill"))
            if current_fill_price <= 0:
                current_fill_price = _float(result.get("fill_price"))
            if (
                current_status.upper() in {"CANCELED", "REJECTED", "EXPIRED", "ERROR"}
                and current_filled_qty > 0
                and current_fill_price > 0
            ):
                c.execute(
                    "UPDATE orders SET filled_qty=GREATEST(COALESCE(filled_qty,0),%s), "
                    "fill_price=%s, filled_ts=COALESCE(filled_ts,%s,NOW()), updated_ts=NOW() "
                    "WHERE client_id=%s AND local_order_id=%s AND kind='EXIT' "
                    "AND status=%s AND broker_order_id=%s "
                    "AND COALESCE(filled_qty,0)<=%s",
                    (
                        current_filled_qty,
                        current_fill_price,
                        result.get("filled_ts"),
                        str(order.get("client_id") or "").strip(),
                        str(order.get("local_order_id") or "").strip(),
                        current_status,
                        str(
                            result.get("broker_order_id")
                            or order.get("broker_order_id")
                            or ""
                        ).strip(),
                        current_filled_qty,
                    ),
                )
            position = _resolve_position(c, order, fill_ts)
            if not position:
                raise ReconciliationIdentityError("CANONICAL_POSITION_UNRESOLVED")
            position_id = str(position.get("id") or "").strip()
            if not position_id:
                raise LifecycleProjectionError("canonical_position_id_missing")

            fills = _load_exit_fills(c, position, order)
            projection = project_position_from_exit_fills(position, fills)
            entry_order = _load_entry_order(c, position, order)
            entry_broker_id = str(entry_order.get("broker_order_id") or position.get("broker_order_id") or "").strip()
            exit_ids = [str(row.get("broker_order_id") or "").strip() for row in fills]
            all_exit_broker_backed = bool(fills) and all(exit_ids)
            exit_ids = [value for value in exit_ids if value]
            final_exit_broker_id = exit_ids[-1] if exit_ids else str(order.get("broker_order_id") or "").strip()
            mode = str(
                position.get("execution_mode") or entry_order.get("execution_mode")
                or order.get("execution_mode") or "unknown"
            ).strip().lower()
            signal_id = str(
                position.get("signal_id") or entry_order.get("signal_id")
                or order.get("signal_id") or ""
            ).strip()
            canonical_signal_id = str(
                entry_order.get("canonical_signal_id") or order.get("canonical_signal_id") or ""
            ).strip()
            entry_fill_price = _float(entry_order.get("fill_price") or position.get("avg_fill") or position.get("entry_price"))
            entry_filled_qty = _int(entry_order.get("filled_qty") or position.get("qty"))
            synthetic_entry = bool(position.get("synthetic_entry") or entry_order.get("synthetic_entry"))
            partial_active = _is_partial_result(order, result) and not projection.closed
            current_filled_qty = _int(
                result.get("filled_qty"), _int(order.get("filled_qty"))
            )
            current_order_qty = _int(order.get("qty"))
            remaining_on_current_order = max(0, current_order_qty - current_filled_qty)

            position_columns = _table_columns(c, "positions")
            ownership_updates = (
                {
                    "exit_in_flight": True,
                    "pending_exit_qty": remaining_on_current_order or None,
                    "pending_exit_local_order_id": str(order.get("local_order_id") or "") or None,
                    "pending_exit_broker_order_id": str(
                        result.get("broker_order_id") or order.get("broker_order_id") or ""
                    ) or None,
                }
                if partial_active
                else {
                    "exit_in_flight": False,
                    "pending_exit_action": None,
                    "pending_exit_reason": None,
                    "pending_exit_qty": None,
                    "pending_exit_local_order_id": None,
                    "pending_exit_broker_order_id": None,
                }
            )
            position_updates = {
                key: value
                for key, value in {
                    "contracts_exited": projection.exited_qty,
                    "quantity_remaining": projection.remaining_qty,
                    "exit_price": projection.weighted_exit_price,
                    "realized_pnl": projection.realized_pnl,
                    "realized_pnl_pct": projection.realized_pnl_pct,
                    **ownership_updates,
                    "updated_at": datetime.now(timezone.utc),
                    **({"status": "CLOSED", "exit_ts": projection.final_fill_ts or fill_ts} if projection.closed else {}),
                }.items()
                if key in position_columns
            }
            position_rows_updated = _dynamic_update(
                c,
                "positions",
                position_updates,
                "client_id=%s AND id::text=%s",
                (client_id, position_id),
            )
            if position_rows_updated != 1:
                raise PositionUpdateCardinalityError(position_rows_updated)

            local_ids = [str(row.get("local_order_id") or "").strip() for row in fills]
            local_ids = [value for value in local_ids if value]
            if local_ids:
                c.execute(
                    "UPDATE orders SET position_id=%s, updated_ts=NOW() WHERE client_id=%s "
                    "AND local_order_id IN %s AND (position_id IS NULL OR position_id::text='' "
                    "OR position_id::text=%s OR position_id::text LIKE 'broker-repair-%%')",
                    (position_id, client_id, tuple(local_ids), position_id),
                )
            if entry_order.get("local_order_id"):
                c.execute(
                    "UPDATE orders SET position_id=%s, updated_ts=NOW() WHERE client_id=%s "
                    "AND local_order_id=%s AND (position_id IS NULL OR position_id::text='')",
                    (position_id, client_id, entry_order.get("local_order_id")),
                )

            proof_updated = 0
            eligible = False
            proof_reconciliation: dict[str, Any] | None = None
            if projection.closed:
                proof_columns = _table_columns(c, "proof_trades")
                eligible = official_live_eligibility(
                    execution_mode=mode,
                    closed=True,
                    entry_broker_order_id=entry_broker_id,
                    exit_broker_order_id=final_exit_broker_id,
                    all_exit_fills_broker_backed=all_exit_broker_backed,
                    entry_filled_qty=entry_filled_qty,
                    exit_filled_qty=projection.exited_qty,
                    entry_fill_price=entry_fill_price,
                    exit_fill_price=projection.weighted_exit_price,
                    synthetic_entry=synthetic_entry,
                )
                live_mode = mode == "live"
                proof_updates = {
                    key: value
                    for key, value in {
                        "position_id": position_id,
                        "execution_mode": mode if mode in {"live", "paper"} else "unknown",
                        "signal_id": signal_id or None,
                        "canonical_signal_id": canonical_signal_id or None,
                        "broker_entry_order_id": entry_broker_id or None,
                        "broker_exit_order_id": final_exit_broker_id or None,
                        "broker_entry_fill_ts": entry_order.get("filled_ts"),
                        "broker_exit_fill_ts": projection.final_fill_ts,
                        "broker_entry_filled_qty": entry_filled_qty or None,
                        "broker_exit_filled_qty": projection.exited_qty,
                        "entry_price_source": (
                            PRICE_SOURCE_TRADIER_ENTRY if live_mode else PRICE_SOURCE_PAPER_BROKER
                        ),
                        "exit_price_source": (
                            PRICE_SOURCE_TRADIER_EXIT if live_mode else PRICE_SOURCE_PAPER_BROKER
                        ),
                        "exit_option_price": projection.weighted_exit_price,
                        "exit_fill_price": projection.weighted_exit_price,
                        "option_pnl_pct": projection.realized_pnl_pct,
                        "win": projection.realized_pnl > 0,
                        "broker_reconciled": bool(
                            entry_broker_id and all_exit_broker_backed
                            and entry_filled_qty > 0 and entry_fill_price > 0
                        ),
                        "official_live_performance_eligible": eligible,
                    }.items()
                    if key in proof_columns
                }
                entry_local_id = str(entry_order.get("local_order_id") or "")
                proof_updated, proof_reconciliation = _update_canonical_proof_row(
                    c,
                    proof_columns=proof_columns,
                    client_id=client_id,
                    position_id=position_id,
                    entry_local_order_id=entry_local_id,
                    proof_updates=proof_updates,
                )
                if proof_updated != 1:
                    eligible = False

            return {
                "position_id": position_id,
                "projection": projection,
                "exit_ownership": {
                    key: position_updates.get(key)
                    for key in (
                        "exit_in_flight",
                        "pending_exit_local_order_id",
                        "pending_exit_broker_order_id",
                        "pending_exit_qty",
                    )
                },
                "execution_mode": mode,
                "proof_rows_updated": proof_updated,
                "proof_reconciliation": proof_reconciliation,
                "official_live_performance_eligible": eligible,
            }

    try:
        reconciled = run_with_retry(_tx)
    except Exception as exc:
        _finish_reconciliation_attempt(
            order,
            result,
            attempt_count=attempt_count,
            error=exc,
        )
        raise
    _finish_reconciliation_attempt(
        order,
        result,
        attempt_count=attempt_count,
        reconciled_position_id=str(reconciled.get("position_id") or ""),
        proof_reconciliation=reconciled.get("proof_reconciliation"),
    )
    return reconciled


def _reconcile_exit_fill(order: dict, result: dict) -> dict:
    attempt_count = _begin_reconciliation_attempt(order, result)
    return _run_reconciliation_attempt(
        order,
        result,
        attempt_count=attempt_count,
    )


def reconcile_confirmed_exit_fill(order: dict, result: dict) -> dict:
    """Reconcile one already broker-confirmed EXIT fill.

    This function has no broker adapter and cannot submit or cancel orders.
    It delegates to the one canonical EXIT-fill reducer.
    """
    return _reconcile_exit_fill(order, result)


def retry_exit_fill_reconciliation(*, client_id: str, local_order_id: str) -> dict | None:
    """Retry durable post-fill accounting without broker submit/cancel authority."""
    client_id = str(client_id or "").strip()
    local_order_id = str(local_order_id or "").strip()
    if not client_id or not local_order_id:
        return None

    def _load() -> dict | None:
        with conn() as c:
            row = c.execute(
                "SELECT * FROM orders WHERE client_id=%s AND local_order_id=%s "
                "AND kind='EXIT' AND status IN ('EXIT_FILLED','EXIT_PARTIAL_FILL') LIMIT 1",
                (client_id, local_order_id),
            ).fetchone()
            return dict(row) if row else None

    order = run_with_retry(_load)
    if not order:
        return None
    meta = order.get("meta") or {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except Exception:
            meta = {}
    marker = dict(meta.get("exit_fill_reconciliation") or {}) if isinstance(meta, dict) else {}
    status = str(marker.get("status") or "").upper()
    if status == "RECONCILED":
        return {
            "position_id": str(marker.get("position_id") or ""),
            "already_reconciled": True,
        }
    result = {
        "status": order.get("status"),
        "broker_order_id": order.get("broker_order_id"),
        "filled_qty": order.get("filled_qty"),
        "fill_price": order.get("fill_price"),
        "filled_ts": order.get("filled_ts"),
    }
    attempt_count = _claim_reconciliation_retry(order, result)
    if attempt_count is None:
        return None
    return _run_reconciliation_attempt(
        order,
        result,
        attempt_count=attempt_count,
    )


def retry_pending_exit_fill_reconciliations(
    *,
    client_id: str | None = None,
    limit: int = 100,
) -> list[dict]:
    """Discover and retry stranded broker-confirmed EXIT accounting work."""
    normalized_client = str(client_id or "").strip()
    try:
        bounded_limit = max(1, min(int(limit), 500))
    except (TypeError, ValueError):
        bounded_limit = 100
    fetch_limit = min(500, max(bounded_limit, bounded_limit * 4))

    def _load_candidates() -> list[dict]:
        with conn() as c:
            client_sql = " AND client_id=%s" if normalized_client else ""
            params: list[Any] = []
            if normalized_client:
                params.append(normalized_client)
            params.append(fetch_limit)
            rows = c.execute(
                "SELECT client_id, local_order_id, meta FROM orders "
                "WHERE kind='EXIT' "
                "AND status IN ('EXIT_FILLED','EXIT_PARTIAL_FILL') "
                "AND UPPER(COALESCE(meta->'exit_fill_reconciliation'->>'status','')) "
                "IN ('RETRY_REQUIRED','QUARANTINED','IN_PROGRESS')"
                + client_sql
                + " ORDER BY COALESCE(meta->'exit_fill_reconciliation'->>'last_attempt_at','') ASC "
                "LIMIT %s",
                tuple(params),
            ).fetchall()
            return [dict(row) for row in rows]

    now = datetime.now(timezone.utc)
    candidates = run_with_retry(_load_candidates)
    retryable: list[dict] = []
    for row in candidates:
        meta = row.get("meta") or {}
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}
        marker = dict(meta.get("exit_fill_reconciliation") or {}) if isinstance(meta, dict) else {}
        if _reconciliation_marker_retryable(marker, now):
            retryable.append(row)
        if len(retryable) >= bounded_limit:
            break

    outcomes: list[dict] = []
    for row in retryable:
        row_client = str(row.get("client_id") or "")
        row_local = str(row.get("local_order_id") or "")
        try:
            reconciled = retry_exit_fill_reconciliation(
                client_id=row_client,
                local_order_id=row_local,
            )
            proof_status = str(
                ((reconciled or {}).get("proof_reconciliation") or {}).get("status")
                or ""
            ).strip().upper()
            resolved = reconciled is not None and proof_status != "QUARANTINED"
            outcomes.append({
                "client_id": row_client,
                "local_order_id": row_local,
                "status": (
                    "RECONCILED"
                    if resolved
                    else "QUARANTINED" if proof_status == "QUARANTINED"
                    else "NOT_CLAIMED"
                ),
                "reconciled": resolved,
                "result": reconciled,
            })
        except Exception as exc:
            status, reason_code, candidates = _failure_reason(exc)
            outcomes.append({
                "client_id": row_client,
                "local_order_id": row_local,
                "status": "QUARANTINED" if status == "QUARANTINED" else "FAILED",
                "reconciled": False,
                "result": {
                    "reason_code": reason_code,
                    "candidate_position_ids": candidates,
                    "error": str(exc),
                } if status == "QUARANTINED" else None,
                "error": str(exc),
            })
    return outcomes


def install_exit_fill_truth_guard() -> None:
    from ap import fill_monitor

    if getattr(fill_monitor, _PATCHED_ATTR, False):
        return
    setattr(fill_monitor, _ORIGINAL_ATTR, fill_monitor._sync_exit_price)

    def guarded_sync_exit_price(order: dict, result: dict):
        try:
            reconciled = _reconcile_exit_fill(order, result)
            projection = reconciled["projection"]
            log.info(
                "[%s] CANONICAL_EXIT_FILL_RECONCILED position=%s exited=%s remaining=%s "
                "exit_price=%.4f realized_pnl=%.2f proof_rows=%s official_live=%s",
                order.get("client_id"), reconciled.get("position_id"),
                projection.exited_qty, projection.remaining_qty,
                projection.weighted_exit_price, projection.realized_pnl,
                reconciled.get("proof_rows_updated"),
                reconciled.get("official_live_performance_eligible"),
            )
            try:
                from ap.exit_price_sync import sync_exit_price_to_dashboard
                sync_exit_price_to_dashboard(
                    position_id=str(reconciled.get("position_id") or ""),
                    exit_avg_fill=projection.weighted_exit_price,
                    entry_price=None,
                    ticker=str(order.get("symbol") or "").upper(),
                )
            except Exception:
                pass
            return reconciled
        except Exception as exc:
            log.critical(
                "[%s] CANONICAL_EXIT_FILL_RECONCILE_FAILED order=%s broker=%s position=%s contract=%s error=%s",
                order.get("client_id"), order.get("local_order_id"),
                order.get("broker_order_id"), order.get("position_id"),
                order.get("contract") or order.get("symbol"), exc,
                exc_info=True,
            )
            return None

    fill_monitor._sync_exit_price = guarded_sync_exit_price
    setattr(fill_monitor, _PATCHED_ATTR, True)
