"""Read-only release preflight for PR #423 exit-retry liveness authority.

The command proves the deployed database can support the exact durable
replacement/fill lifecycle.  It never calls a broker and never writes to
PostgreSQL.  A missing migration, missing JSONB authority, missing active-EXIT
uniqueness evidence, malformed lifecycle, or ambiguous active partial-fill
row is a HOLD.

Run from the exact artifact under review::

    python -m ap.exit_retry_liveness_preflight
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ap.db import conn, run_with_retry
from ap.schema_attestation import attest_schema

from ap_exit_engine import (
    _parse_exit_retry_liveness_namespace,
    _restore_exit_fill_consumption_from_meta,
)

MIGRATION_FILENAME = "20260811_positions_exit_retry_liveness_authority.sql"
_ACTIVE_EXIT_STATUSES = (
    "EXIT_REQUESTED",
    "EXIT_SUBMITTED",
    "EXIT_ACKNOWLEDGED",
    "EXIT_PARTIAL_FILL",
)


def _row_dict(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return dict(row)
    if hasattr(row, "keys"):
        return {key: row[key] for key in row.keys()}
    return {}


def _row_value(row: Any, key: str, position: int = 0) -> Any:
    if isinstance(row, dict):
        return row.get(key)
    if hasattr(row, "keys"):
        return row[key]
    try:
        return row[position]
    except (IndexError, KeyError, TypeError):
        return None


def _exact_text(value: Any) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        return ""
    return value


def _positive_int(value: Any) -> bool:
    return type(value) is int and value > 0


def _meta_object(raw: Any) -> tuple[dict[str, Any], str | None]:
    if raw in (None, ""):
        return {}, None
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, ValueError):
            return {}, "META_UNPARSEABLE"
    if not isinstance(raw, dict):
        return {}, "META_UNPARSEABLE"
    return dict(raw), None


def _migration_checksum() -> str:
    path = Path(__file__).resolve().parents[1] / "migrations" / MIGRATION_FILENAME
    if not path.is_file():
        raise FileNotFoundError(f"required migration missing from artifact: {MIGRATION_FILENAME}")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _index_is_active_exit_unique(row: dict[str, Any]) -> bool:
    definition = re.sub(r"\s+", " ", str(row.get("indexdef") or "").lower()).strip()
    if "create unique index" not in definition:
        return False
    if not re.search(r"\(\s*client_id\s*,\s*position_id\s*\)", definition):
        return False
    if not re.search(r"\bkind\s*=\s*'exit'(?:\s*::\s*[a-z0-9_]+)?", definition):
        return False
    status_predicates = re.findall(r"\bstatus\s+in\s*\(([^)]*)\)", definition)
    status_predicates.extend(
        re.findall(
            r"\bstatus\s*=\s*any\s*\(\s*array\s*\[([^\]]*)\]",
            definition,
        )
    )
    if not status_predicates:
        return False
    status_literals = set()
    for predicate in status_predicates:
        status_literals.update(re.findall(r"'(exit_[a-z_]+)'", predicate))
    return status_literals == {status.lower() for status in _ACTIVE_EXIT_STATUSES}


def _read_snapshot() -> dict[str, Any]:
    expected_checksum = _migration_checksum()

    def _read() -> dict[str, Any]:
        with conn() as c:
            meta_column = _row_dict(
                c.execute(
                    "SELECT data_type, udt_name, is_nullable, column_default "
                    "FROM information_schema.columns "
                    "WHERE table_schema='public' AND table_name='positions' "
                    "AND column_name='meta'"
                ).fetchone()
            )
            ledger_exists = _row_value(
                c.execute("SELECT to_regclass('public.schema_migrations')").fetchone(),
                "to_regclass",
            )
            ledger_rows = []
            if ledger_exists:
                ledger_rows = [
                    _row_dict(row)
                    for row in c.execute(
                        "SELECT filename, checksum, applied_at, baselined "
                        "FROM public.schema_migrations WHERE filename=%s",
                        (MIGRATION_FILENAME,),
                    ).fetchall()
                ]
            index_rows = [
                _row_dict(row)
                for row in c.execute(
                    "SELECT indexname, indexdef FROM pg_indexes "
                    "WHERE schemaname='public' AND tablename='orders'"
                ).fetchall()
            ]
            active_positions = [
                _row_dict(row)
                for row in c.execute(
                    "SELECT id, client_id, execution_mode, status, "
                    "quantity_remaining, meta FROM public.positions "
                    "WHERE UPPER(COALESCE(status,'')) IN "
                    "('OPEN','CLOSING','PARTIAL','ACTIVE') "
                    "OR COALESCE(quantity_remaining,0)>0"
                ).fetchall()
            ]
            active_partial_fills = [
                _row_dict(row)
                for row in c.execute(
                    "SELECT p.id AS position_id, p.client_id, "
                    "p.execution_mode, p.meta, "
                    "o.local_order_id, o.broker_order_id, o.position_id AS order_position_id, "
                    "o.kind, o.status AS order_status, o.execution_mode AS order_execution_mode, "
                    "o.filled_qty, o.qty "
                    "FROM public.positions p "
                    "JOIN public.orders o ON o.client_id=p.client_id "
                    "AND o.position_id=p.id AND o.kind='EXIT' "
                    "WHERE o.status = ANY(%s) AND COALESCE(o.filled_qty,0)>0 "
                    "ORDER BY p.client_id, p.id, o.created_ts",
                    (list(_ACTIVE_EXIT_STATUSES),),
                ).fetchall()
            ]
            duplicate_rows = [
                _row_dict(row)
                for row in c.execute(
                    "SELECT client_id, position_id, COUNT(*) AS active_exit_count "
                    "FROM public.orders WHERE kind='EXIT' AND status=ANY(%s) "
                    "GROUP BY client_id, position_id HAVING COUNT(*)>1",
                    (list(_ACTIVE_EXIT_STATUSES),),
                ).fetchall()
            ]
        return {
            "meta_column": meta_column,
            "ledger_exists": bool(ledger_exists),
            "ledger_rows": ledger_rows,
            "index_rows": index_rows,
            "active_positions": active_positions,
            "active_partial_fills": active_partial_fills,
            "duplicate_rows": duplicate_rows,
            "expected_migration_checksum": expected_checksum,
        }

    return dict(run_with_retry(_read) or {})


def _classify_lifecycle(row: dict[str, Any]) -> dict[str, Any]:
    meta, meta_error = _meta_object(row.get("meta"))
    findings: list[str] = []
    if meta_error:
        findings.append(meta_error)
        return {"position_id": row.get("id"), "findings": findings, "safe": False}

    namespace = meta.get("exit_retry_liveness")
    if namespace is None:
        return {"position_id": row.get("id"), "findings": [], "safe": True}
    if not isinstance(namespace, dict) or "state" not in namespace:
        findings.append("MALFORMED_EXIT_RETRY_LIVENESS")
        return {"position_id": row.get("id"), "findings": findings, "safe": False}

    parsed, reason = _parse_exit_retry_liveness_namespace(namespace)
    if parsed is None:
        findings.append(f"MALFORMED_EXIT_RETRY_LIVENESS:{reason}")
    elif parsed.get("state") != "NONE":
        if parsed.get("position_id") != _exact_text(row.get("id")):
            findings.append("LIFECYCLE_POSITION_ID_MISMATCH")
        if parsed.get("client_id") != _exact_text(row.get("client_id")):
            findings.append("LIFECYCLE_CLIENT_ID_MISMATCH")
        row_mode = str(row.get("execution_mode") or "").strip().lower()
        if parsed.get("execution_mode") != row_mode:
            findings.append("LIFECYCLE_EXECUTION_MODE_MISMATCH")

    return {
        "position_id": row.get("id"),
        "state": parsed.get("state") if parsed else None,
        "findings": findings,
        "safe": not findings,
    }


def _classify_partial_fill(row: dict[str, Any]) -> dict[str, Any]:
    meta, meta_error = _meta_object(row.get("meta"))
    findings: list[str] = []
    filled_qty = row.get("filled_qty")
    if not _positive_int(filled_qty):
        findings.append("ACTIVE_PARTIAL_FILLED_QTY_INVALID")
    if meta_error:
        findings.append(meta_error)
        meta = {}
    marker, valid, reason = _restore_exit_fill_consumption_from_meta(meta)
    if not valid or not marker:
        findings.append(
            "ACTIVE_PARTIAL_FILL_WATERMARK_UNAVAILABLE"
            if valid
            else f"ACTIVE_PARTIAL_FILL_WATERMARK_MALFORMED:{reason}"
        )
    else:
        if marker.get("position_id") != _exact_text(row.get("position_id")):
            findings.append("PARTIAL_FILL_POSITION_ID_MISMATCH")
        if marker.get("client_id") != _exact_text(row.get("client_id")):
            findings.append("PARTIAL_FILL_CLIENT_ID_MISMATCH")
        if marker.get("local_order_id") != _exact_text(row.get("local_order_id")):
            findings.append("PARTIAL_FILL_LOCAL_ORDER_ID_MISMATCH")
        if marker.get("broker_order_id") != _exact_text(row.get("broker_order_id")):
            findings.append("PARTIAL_FILL_BROKER_ORDER_ID_MISMATCH")
        if marker.get("execution_mode") != str(row.get("execution_mode") or "").strip().lower():
            findings.append("PARTIAL_FILL_EXECUTION_MODE_MISMATCH")
        position_mode = str(row.get("execution_mode") or "").strip().lower()
        order_mode = str(row.get("order_execution_mode") or "").strip().lower()
        if not order_mode or order_mode != position_mode:
            findings.append("PARTIAL_FILL_ORDER_EXECUTION_MODE_MISMATCH")
        if _positive_int(filled_qty) and marker.get("applied_cumulative_qty") != filled_qty:
            findings.append("PARTIAL_FILL_WATERMARK_QTY_MISMATCH")
    return {
        "position_id": row.get("position_id"),
        "local_order_id": row.get("local_order_id"),
        "filled_qty": filled_qty,
        "findings": findings,
        "safe": not findings,
    }


def run_preflight() -> dict[str, Any]:
    findings: list[str] = []
    try:
        schema = attest_schema(strict=True)
    except Exception as exc:  # noqa: BLE001 - release proof must report HOLD
        schema = {"ok": False, "skipped": False, "error": f"{type(exc).__name__}:{exc}"}
        findings.append("SCHEMA_ATTESTATION_UNPROVEN")

    try:
        snapshot = _read_snapshot()
    except Exception as exc:  # noqa: BLE001 - no fail-open on unavailable proof
        return {
            "tool": "ap.exit_retry_liveness_preflight",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "status": "HOLD",
            "safe": False,
            "findings": [*findings, f"PREFLIGHT_READ_FAILED:{type(exc).__name__}"],
            "schema": schema,
            "broker_calls": 0,
            "writes": 0,
        }

    if not schema.get("ok") or schema.get("skipped"):
        findings.append("SCHEMA_ATTESTATION_NOT_PROVEN")

    meta_column = snapshot.get("meta_column") or {}
    meta_ok = (
        meta_column.get("data_type") == "jsonb"
        and meta_column.get("udt_name") == "jsonb"
        and meta_column.get("is_nullable") == "NO"
        and bool(meta_column.get("column_default"))
    )
    if not meta_ok:
        findings.append("POSITIONS_META_JSONB_NOT_PROVEN")

    ledger_rows = snapshot.get("ledger_rows") or []
    ledger = ledger_rows[0] if ledger_rows else None
    expected_checksum = snapshot.get("expected_migration_checksum")
    migration = {
        "filename": MIGRATION_FILENAME,
        "present": bool(expected_checksum),
        "applied": bool(ledger),
        "checksum_match": bool(
            ledger and expected_checksum and ledger.get("checksum") == expected_checksum
        ),
    }
    if not migration["present"]:
        findings.append("MIGRATION_MISSING_FROM_ARTIFACT")
    if not migration["applied"]:
        findings.append("MIGRATION_NOT_LEDGERED")
    elif not migration["checksum_match"]:
        findings.append("MIGRATION_CHECKSUM_DRIFT")

    indexes = [
        row for row in snapshot.get("index_rows", [])
        if _index_is_active_exit_unique(row)
    ]
    if not indexes:
        findings.append("ACTIVE_EXIT_UNIQUE_INDEX_NOT_PROVEN")

    lifecycle = [
        _classify_lifecycle(row)
        for row in snapshot.get("active_positions", [])
    ]
    unsafe_lifecycle = [row for row in lifecycle if not row["safe"]]
    if unsafe_lifecycle:
        findings.append("MALFORMED_OR_MISMATCHED_EXIT_RETRY_LIFECYCLE")

    partial_fills = [
        _classify_partial_fill(row)
        for row in snapshot.get("active_partial_fills", [])
    ]
    unsafe_partial_fills = [row for row in partial_fills if not row["safe"]]
    if unsafe_partial_fills:
        findings.append("AMBIGUOUS_ACTIVE_PARTIAL_FILL_ROWS")
    duplicate_rows = snapshot.get("duplicate_rows", [])
    if duplicate_rows:
        findings.append("MULTIPLE_ACTIVE_EXITS_PER_POSITION")

    safe = not findings
    return {
        "tool": "ap.exit_retry_liveness_preflight",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "PASS" if safe else "HOLD",
        "safe": safe,
        "findings": findings,
        "schema": schema,
        "migration": migration,
        "positions_meta": meta_column,
        "active_exit_unique_indexes": indexes,
        "lifecycle_rows": lifecycle,
        "active_partial_fill_rows": partial_fills,
        "duplicate_active_exits": duplicate_rows,
        "broker_calls": 0,
        "writes": 0,
    }


def main() -> int:
    result = run_preflight()
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0 if result.get("safe") else 2


if __name__ == "__main__":
    sys.exit(main())
