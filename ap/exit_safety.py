from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Optional

from ap.db import conn, run_with_retry
from ap.notify import post_discord
from ap.utils import now_utc_iso

log = logging.getLogger("ap.exit_safety")

_SCHEMA_CACHE_LOCK = threading.Lock()
_SCHEMA_CACHE: dict[str, set[str]] = {}
_ALERT_CACHE_LOCK = threading.Lock()
_ALERT_CACHE: dict[tuple[str, str, str, str, str], float] = {}

_DEFAULT_LOOKBACK_MINUTES = 60
_DEFAULT_THRESHOLD = 5
_ALERT_COOLDOWN_SECONDS = 900

_TERMINAL_CLOSE_SOURCES = {
    "reconciler_auto_close",
    "operator_manual_close",
    "expired",
    "expiry",
    "manual_close",
    "broker_position_missing",
}

_BROKER_REJECT_LAST_ERROR_PATTERNS = (
    "%broker_rejected_exit%",
    "%broker_http_4%",
    "%broker_status:rejected%",
    "%permanent_broker_reject%",
)


def _normalize_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _normalize_mode(value: Any) -> Optional[str]:
    normalized = _normalize_text(value)
    return normalized or None


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value in (None, ""):
            return None
        return int(float(value))
    except Exception:
        return None


def _table_columns(table_name: str) -> set[str]:
    with _SCHEMA_CACHE_LOCK:
        cached = _SCHEMA_CACHE.get(table_name)
        if cached is not None:
            return cached

    def _fn():
        with conn() as c:
            c.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                """,
                (table_name,),
            )
            return {str(r["column_name"]) for r in c.fetchall()}

    try:
        columns = run_with_retry(_fn) or set()
    except Exception as exc:
        log.debug("schema inspection failed for %s: %s", table_name, exc)
        columns = set()

    with _SCHEMA_CACHE_LOCK:
        _SCHEMA_CACHE[table_name] = columns
    return columns


def refresh_schema_cache() -> None:
    with _SCHEMA_CACHE_LOCK:
        _SCHEMA_CACHE.clear()


def has_orders_column(name: str) -> bool:
    return name in _table_columns("orders")


def has_positions_column(name: str) -> bool:
    return name in _table_columns("positions")


def _parse_entry_ts(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _parse_max_exit_rejections_threshold() -> int:
    raw = os.getenv("MAX_EXIT_REJECTIONS_BEFORE_HALT")
    if raw is None or str(raw).strip() == "":
        return _DEFAULT_THRESHOLD
    try:
        threshold = int(str(raw).strip())
    except Exception:
        log.warning(
            "MAX_EXIT_REJECTIONS_BEFORE_HALT=%r invalid; using default=%d",
            raw,
            _DEFAULT_THRESHOLD,
        )
        return _DEFAULT_THRESHOLD
    if threshold <= 0:
        log.warning(
            "MAX_EXIT_REJECTIONS_BEFORE_HALT=%d disables exit rejection circuit breaker",
            threshold,
        )
    return threshold


def resolve_broker_exit_truth(
    *,
    broker,
    client_id: str,
    execution_mode: Optional[str],
    contract: str,
    requested_exit_qty: Optional[int] = None,
) -> dict:
    checked_at = now_utc_iso()
    account_id = str(
        getattr(broker, "account_id", None)
        or getattr(getattr(broker, "cfg", None), "account_id", None)
        or ""
    ).strip()
    audit = {
        "client_id": str(client_id or ""),
        "execution_mode": str(execution_mode or ""),
        "contract": str(contract or ""),
        "requested_qty": _safe_int(requested_exit_qty),
        "account": account_id,
        "checked_at": checked_at,
        "snapshot_available": False,
        "snapshot_fresh": False,
        "exact_contract_match": False,
        "broker_truth_open_qty": None,
        "position_count": None,
        "reason": None,
    }
    list_positions = getattr(broker, "list_positions", None)
    if not callable(list_positions):
        audit["reason"] = "broker_positions_snapshot_unavailable"
        return audit

    try:
        positions = list_positions() or []
    except Exception as exc:
        audit["reason"] = f"broker_positions_snapshot_error:{type(exc).__name__}"
        return audit
    if not isinstance(positions, (list, tuple)):
        audit["reason"] = "broker_positions_snapshot_unavailable"
        return audit

    target = str(contract or "").strip().upper()
    audit["snapshot_available"] = True
    audit["snapshot_fresh"] = True
    audit["position_count"] = len(positions)
    audit["reason"] = "fresh_snapshot"

    exact_rows = []
    for row in positions:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or row.get("option_symbol") or "").strip().upper()
        if symbol == target:
            exact_rows.append(row)

    if exact_rows:
        qty = 0
        for row in exact_rows:
            row_qty = _safe_int(row.get("quantity"))
            if row_qty and row_qty > 0:
                qty += row_qty
        audit["exact_contract_match"] = True
        audit["broker_truth_open_qty"] = max(int(qty or 0), 0)
        audit["matched_rows"] = len(exact_rows)
        audit["reason"] = (
            "fresh_exact_occ_long_qty"
            if audit["broker_truth_open_qty"] > 0
            else "fresh_exact_occ_not_long"
        )
        return audit

    audit["broker_truth_open_qty"] = 0
    audit["reason"] = "fresh_snapshot_exact_occ_flat"
    return audit


def mark_synthetic_position_stale_broker_flat(
    *,
    position_id: str,
    client_id: str,
    execution_mode: Optional[str],
    broker_truth_audit: Optional[dict] = None,
) -> bool:
    position_columns = _table_columns("positions")
    if "status" not in position_columns:
        return False

    normalized_mode = _normalize_mode(execution_mode)
    metadata_col = "metadata" if "metadata" in position_columns else ("meta" if "meta" in position_columns else "")
    audit_payload = {
        "reason": "synthetic_position_stale_broker_flat",
        "broker_truth": dict(broker_truth_audit or {}),
        "manual_close_needed": True,
        "reconciler_manual_close_needed": True,
        "closed_at": now_utc_iso(),
    }

    def _fn():
        with conn() as c:
            updates = ["status=%s"]
            params: list[Any] = ["CLOSED"]
            if "quantity_remaining" in position_columns:
                updates.append("quantity_remaining=%s")
                params.append(0)
            if "close_source" in position_columns:
                updates.append("close_source=%s")
                params.append("synthetic_position_stale_broker_flat")
            if "exit_ts" in position_columns:
                updates.append("exit_ts=%s")
                params.append(now_utc_iso())
            if metadata_col:
                updates.append(f"{metadata_col} = COALESCE({metadata_col}, '{{}}'::jsonb) || %s::jsonb")
                params.append(json.dumps({"exit_circuit_breaker_broker_truth": audit_payload}, default=str))
            updates.append("updated_ts=NOW()")
            sql = (
                f"UPDATE positions SET {', '.join(updates)} "
                "WHERE id=%s AND client_id=%s"
            )
            params.extend([position_id, client_id])
            if normalized_mode and "execution_mode" in position_columns:
                sql += " AND LOWER(COALESCE(execution_mode, '')) = %s"
                params.append(normalized_mode)
            cur = c.execute(sql, tuple(params))
            return getattr(cur, "rowcount", getattr(c, "rowcount", 0))

    try:
        rowcount = run_with_retry(_fn)
        return bool(rowcount)
    except Exception as exc:
        log.warning(
            "synthetic broker-flat close persistence failed | client_id=%s execution_mode=%s position_id=%s err=%s",
            client_id,
            normalized_mode or "",
            position_id,
            exc,
        )
        return False


def _persist_circuit_breaker_marker(
    db_conn,
    *,
    position_id: str,
    client_id: str,
    rejection_count: int,
) -> bool:
    position_columns = _table_columns("positions")
    metadata_col = "metadata" if "metadata" in position_columns else ("meta" if "meta" in position_columns else "")
    if not metadata_col:
        return False

    updates = [f"{metadata_col} = COALESCE({metadata_col}, '{{}}'::jsonb) || %s::jsonb"]
    params: list[Any] = [
        json.dumps(
            {
                "exit_circuit_breaker_tripped": True,
                "exit_circuit_breaker_tripped_at": now_utc_iso(),
                "exit_circuit_breaker_rejection_count": int(rejection_count),
            }
        )
    ]

    if "updated_ts" in position_columns:
        updates.append("updated_ts = NOW()")
    elif "updated_at" in position_columns:
        updates.append("updated_at = NOW()")

    params.extend([position_id, client_id])
    db_conn.execute(
        f"""
        UPDATE positions
        SET {", ".join(updates)}
        WHERE id = %s
          AND client_id = %s
        """,
        tuple(params),
    )
    return True


def _rate_limit_key(*, client_id: str, execution_mode: Optional[str], position_id: str, contract: str, reason: str) -> tuple[str, str, str, str, str]:
    return (
        str(client_id or "").strip().lower(),
        str(execution_mode or "").strip().lower(),
        str(position_id or "").strip(),
        str(contract or "").strip().upper(),
        str(reason or "").strip().lower(),
    )


def alert_exit_submission_halted(
    *,
    client_id: str,
    execution_mode: Optional[str],
    position_id: str,
    contract: str,
    reason: str,
    rejection_count: Optional[int] = None,
    threshold: Optional[int] = None,
) -> bool:
    cache_key = _rate_limit_key(
        client_id=client_id,
        execution_mode=execution_mode,
        position_id=position_id,
        contract=contract,
        reason=reason,
    )
    now = time.time()
    with _ALERT_CACHE_LOCK:
        last_ts = _ALERT_CACHE.get(cache_key, 0.0)
        if now - last_ts < _ALERT_COOLDOWN_SECONDS:
            return False
        _ALERT_CACHE[cache_key] = now

    details = [
        "EXIT HALT",
        f"client={client_id}",
        f"mode={execution_mode or 'unknown'}",
        f"position_id={position_id}",
        f"contract={contract}",
        f"reason={reason}",
    ]
    if rejection_count is not None:
        details.append(f"rejections={rejection_count}")
    if threshold is not None:
        details.append(f"threshold={threshold}")
    try:
        post_discord(" | ".join(details))
    except Exception as exc:
        log.warning("exit halt alert failed: %s", exc)
    return True


def _exit_position_terminal_state(
    db_conn,
    *,
    position_id,
    client_id,
    execution_mode=None,
    contract=None,
    broker_truth_open_qty=None,
    broker_truth_audit: Optional[dict] = None,
    allow_missing_position_with_broker_truth: bool = False,
) -> dict:
    position_columns = _table_columns("positions")
    selected_columns = ["status"]
    for optional_col in ("quantity_remaining", "close_source", "entry_ts"):
        if optional_col in position_columns:
            selected_columns.append(optional_col)

    params: list[Any] = [position_id, client_id]
    sql = (
        f"SELECT {', '.join(selected_columns)} "
        "FROM positions "
        "WHERE id = %s AND client_id = %s"
    )
    normalized_mode = _normalize_mode(execution_mode)
    broker_truth_qty = _safe_int(broker_truth_open_qty)
    broker_truth_fresh = bool((broker_truth_audit or {}).get("snapshot_fresh"))
    if normalized_mode and "execution_mode" in position_columns:
        sql += " AND LOWER(COALESCE(execution_mode, '')) = %s"
        params.append(normalized_mode)
    sql += " LIMIT 1"

    db_conn.execute(sql, tuple(params))
    row = db_conn.fetchone()
    if not row:
        synthetic_repair_id = str(position_id or "").startswith("broker-repair-")
        if broker_truth_qty is not None and broker_truth_qty > 0 and (
            allow_missing_position_with_broker_truth or synthetic_repair_id
        ):
            log.info(
                "exit guard allowing missing position from broker truth | client_id=%s execution_mode=%s position_id=%s contract=%s broker_truth_open_qty=%s synthetic_repair=%s",
                client_id,
                normalized_mode or "",
                position_id,
                contract or "",
                broker_truth_qty,
                synthetic_repair_id,
            )
            return {
                "blocked": False,
                "reason": None,
                "status": None,
                "quantity_remaining": broker_truth_qty,
                "close_source": None,
                "entry_ts": None,
            }
        log.warning(
            "exit guard blocked missing position | client_id=%s execution_mode=%s position_id=%s contract=%s",
            client_id,
            normalized_mode or "",
            position_id,
            contract or "",
        )
        return {
            "blocked": True,
            "reason": "position_missing",
            "status": None,
            "quantity_remaining": None,
            "close_source": None,
            "entry_ts": None,
        }

    status = row.get("status")
    quantity_remaining = row.get("quantity_remaining") if "quantity_remaining" in row else None
    close_source = row.get("close_source") if "close_source" in row else None
    entry_ts = row.get("entry_ts") if "entry_ts" in row else None

    status_norm = _normalize_text(status)
    close_source_norm = _normalize_text(close_source)
    qty_remaining_int = _safe_int(quantity_remaining)

    if broker_truth_qty is not None and broker_truth_qty > 0 and broker_truth_fresh:
        return {
            "blocked": False,
            "reason": None,
            "status": status,
            "quantity_remaining": broker_truth_qty,
            "close_source": close_source,
            "entry_ts": entry_ts,
            "broker_truth_override": True,
            "local_block_reason": (
                "position_already_closed"
                if status_norm == "closed"
                else "position_quantity_depleted"
                if qty_remaining_int is not None and qty_remaining_int <= 0
                else f"terminal_close_source:{close_source_norm}"
                if close_source_norm in _TERMINAL_CLOSE_SOURCES
                else None
            ),
        }

    if status_norm == "closed":
        return {
            "blocked": True,
            "reason": "position_already_closed",
            "status": status,
            "quantity_remaining": qty_remaining_int,
            "close_source": close_source,
            "entry_ts": entry_ts,
        }

    if qty_remaining_int is not None and qty_remaining_int <= 0:
        return {
            "blocked": True,
            "reason": "position_quantity_depleted",
            "status": status,
            "quantity_remaining": qty_remaining_int,
            "close_source": close_source,
            "entry_ts": entry_ts,
        }

    if close_source_norm in _TERMINAL_CLOSE_SOURCES:
        return {
            "blocked": True,
            "reason": f"terminal_close_source:{close_source_norm}",
            "status": status,
            "quantity_remaining": qty_remaining_int,
            "close_source": close_source,
            "entry_ts": entry_ts,
        }

    return {
        "blocked": False,
        "reason": None,
        "status": status,
        "quantity_remaining": qty_remaining_int,
        "close_source": close_source,
        "entry_ts": entry_ts,
    }


def _should_halt_exit_after_rejections(
    db_conn,
    *,
    position_id,
    client_id,
    execution_mode=None,
    contract,
    entry_ts=None,
) -> dict:
    threshold = _parse_max_exit_rejections_threshold()
    if threshold <= 0:
        return {
            "blocked": False,
            "reason": None,
            "rejection_count": 0,
            "threshold": threshold,
        }

    order_columns = _table_columns("orders")
    normalized_mode = _normalize_mode(execution_mode)
    params: list[Any] = [client_id, contract]
    sql = [
        "SELECT COUNT(*) AS rejection_count",
        "FROM orders",
        "WHERE client_id = %s",
        "  AND kind = 'EXIT'",
        "  AND contract = %s",
        "  AND (",
        "        status = 'REJECTED'",
    ]

    if "last_error" in order_columns:
        sql.extend(
            [
                "        OR (",
                "            status = 'ERROR'",
                "            AND (",
            ]
        )
        for idx, pattern in enumerate(_BROKER_REJECT_LAST_ERROR_PATTERNS):
            sql.append("                COALESCE(last_error, '') ILIKE %s")
            if idx < len(_BROKER_REJECT_LAST_ERROR_PATTERNS) - 1:
                sql.append("                OR")
            params.append(pattern)
        sql.extend(
            [
                "            )",
                "        )",
            ]
        )
    sql.append("      )")

    if normalized_mode and "execution_mode" in order_columns:
        sql.append("  AND LOWER(COALESCE(execution_mode, '')) = %s")
        params.append(normalized_mode)

    timestamp_expr = None
    if "updated_ts" in order_columns and "created_ts" in order_columns:
        timestamp_expr = "COALESCE(updated_ts, created_ts)"
    elif "updated_ts" in order_columns:
        timestamp_expr = "updated_ts"
    elif "created_ts" in order_columns:
        timestamp_expr = "created_ts"

    parsed_entry_ts = _parse_entry_ts(entry_ts)
    if timestamp_expr and parsed_entry_ts is not None:
        sql.append(f"  AND {timestamp_expr} >= %s")
        params.append(parsed_entry_ts)
    elif timestamp_expr:
        sql.append(f"  AND {timestamp_expr} >= NOW() - (%s * INTERVAL '1 minute')")
        params.append(_DEFAULT_LOOKBACK_MINUTES)

    db_conn.execute("\n".join(sql), tuple(params))
    row = db_conn.fetchone() or {}
    rejection_count = int(row.get("rejection_count") or 0)
    blocked = rejection_count >= threshold

    if blocked and position_id:
        try:
            _persist_circuit_breaker_marker(
                db_conn,
                position_id=str(position_id),
                client_id=str(client_id),
                rejection_count=rejection_count,
            )
        except Exception as exc:
            log.debug("exit circuit breaker marker persistence failed: %s", exc)

    return {
        "blocked": blocked,
        "reason": "exit_circuit_breaker_tripped" if blocked else None,
        "rejection_count": rejection_count,
        "threshold": threshold,
    }


def evaluate_exit_submission_safety(
    *,
    position_id: str,
    client_id: str,
    execution_mode: Optional[str],
    contract: str,
    broker_truth_open_qty: Optional[int] = None,
    requested_exit_qty: Optional[int] = None,
    broker_truth_audit: Optional[dict] = None,
    allow_missing_position_with_broker_truth: bool = False,
) -> dict:
    def _fn():
        with conn() as c:
            position_state = _exit_position_terminal_state(
                c,
                position_id=position_id,
                client_id=client_id,
                execution_mode=execution_mode,
                contract=contract,
                broker_truth_open_qty=broker_truth_open_qty,
                broker_truth_audit=broker_truth_audit,
                allow_missing_position_with_broker_truth=allow_missing_position_with_broker_truth,
            )
            if position_state.get("blocked"):
                return {
                    "blocked": True,
                    "reason": position_state.get("reason"),
                    "position_state": position_state,
                    "circuit_breaker": None,
                    "broker_truth": broker_truth_audit,
                }

            broker_truth_qty = _safe_int(broker_truth_open_qty)
            requested_qty_int = _safe_int(requested_exit_qty)
            broker_truth_fresh = bool((broker_truth_audit or {}).get("snapshot_fresh"))
            if broker_truth_fresh and broker_truth_qty == 0:
                return {
                    "blocked": True,
                    "reason": "synthetic_position_stale_broker_flat",
                    "position_state": position_state,
                    "circuit_breaker": None,
                    "broker_truth": broker_truth_audit,
                }

            if (
                broker_truth_fresh
                and broker_truth_qty is not None
                and broker_truth_qty > 0
                and requested_qty_int is not None
                and requested_qty_int > broker_truth_qty
            ):
                return {
                    "blocked": True,
                    "reason": "EXIT_BLOCKED_BROKER_QTY_INSUFFICIENT",
                    "position_state": position_state,
                    "circuit_breaker": None,
                    "broker_truth": broker_truth_audit,
                }

            circuit_breaker = _should_halt_exit_after_rejections(
                c,
                position_id=position_id,
                client_id=client_id,
                execution_mode=execution_mode,
                contract=contract,
                entry_ts=position_state.get("entry_ts"),
            )
            if (
                circuit_breaker.get("blocked")
                and broker_truth_fresh
                and broker_truth_qty is not None
                and broker_truth_qty > 0
            ):
                return {
                    "blocked": False,
                    "reason": None,
                    "position_state": position_state,
                    "circuit_breaker": {
                        **circuit_breaker,
                        "blocked": False,
                        "override_reason": "exit_circuit_breaker_broker_truth",
                    },
                    "broker_truth": broker_truth_audit,
                }
            return {
                "blocked": bool(circuit_breaker.get("blocked")),
                "reason": circuit_breaker.get("reason"),
                "position_state": position_state,
                "circuit_breaker": circuit_breaker,
                "broker_truth": broker_truth_audit,
            }

    try:
        return run_with_retry(_fn)
    except Exception as exc:
        log.warning(
            "exit safety preflight unavailable; allowing submit to preserve existing flow | "
            "client_id=%s execution_mode=%s position_id=%s contract=%s err=%s",
            client_id,
            execution_mode or "",
            position_id,
            contract,
            exc,
        )
        return {
            "blocked": False,
            "reason": None,
            "position_state": None,
            "circuit_breaker": None,
            "broker_truth": broker_truth_audit,
        }
