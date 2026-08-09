"""Read-only production preflight for PR #425 broker-owned EXIT recovery.

This command is deliberately narrower than a broker reconciler.  It performs
database/schema reads only; it never calls a broker, submits, cancels, or
mutates an order.  Exit 0 is the release-evidence result.  Exit 2 means the
database or durable lifecycle contains an unresolved identity/configuration
finding.  Exit 1 means the preflight itself could not complete.

Run it from the exact deployed artifact after the two #425 migrations have
been applied and before enabling normal trading traffic::

    python -m ap.broker_owned_exit_recovery_preflight
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ap.db import conn, run_with_retry
from ap.schema_attestation import attest_schema

_MIGRATIONS = (
    "20260717_exit_decision_generation_claims.sql",
    "20260809_exit_decision_generation_requested_qty.sql",
)
_ACTIVE_CLAIM_STATES = (
    "CLAIMED",
    "BROKER_OWNED",
    "AMBIGUOUS",
    "STALE_CLAIM_RECONCILING",
)
_ACTIVE_EXIT_STATUSES = (
    "EXIT_REQUESTED",
    "EXIT_SUBMITTED",
    "EXIT_ACKNOWLEDGED",
    "EXIT_PARTIAL_FILL",
)


def _runtime_mode() -> tuple[str, str | None]:
    """Return canonical mode only for an exact, independently supplied value."""
    raw = os.getenv("BOT_MODE")
    if raw is None:
        raw = os.getenv("MODE")
    if raw is None:
        return "", "runtime_mode_missing"
    if raw == "LIVE":
        return "live", None
    if raw == "PAPER":
        return "paper", None
    return "", "runtime_mode_unproven"


def _migration_checksums() -> dict[str, str]:
    directory = Path(__file__).resolve().parents[1] / "migrations"
    checksums: dict[str, str] = {}
    for filename in _MIGRATIONS:
        path = directory / filename
        if not path.is_file():
            raise FileNotFoundError(f"required migration missing from artifact: {filename}")
        checksums[filename] = hashlib.sha256(path.read_bytes()).hexdigest()
    return checksums


def _read_snapshot(runtime_mode: str) -> dict[str, Any]:
    checksums = _migration_checksums()

    def _read() -> dict[str, Any]:
        with conn() as c:
            ledger_rows = c.execute(
                "SELECT filename, checksum, applied_at, baselined "
                "FROM schema_migrations WHERE filename = ANY(%s)",
                (list(_MIGRATIONS),),
            ).fetchall()
            claim_rows = c.execute(
                """
                SELECT
                    c.generation_key, c.client_id, c.position_id,
                    c.requested_qty, c.claim_state, c.local_order_id,
                    c.broker_order_id AS claim_broker_order_id,
                    o.local_order_id AS order_local_order_id,
                    o.client_id AS order_client_id,
                    o.position_id AS order_position_id,
                    o.kind AS order_kind, o.status AS order_status,
                    o.execution_mode AS order_execution_mode,
                    o.qty AS order_qty,
                    o.broker_order_id AS order_broker_order_id,
                    o.meta AS order_meta
                FROM exit_decision_generation_claims c
                LEFT JOIN orders o
                  ON o.local_order_id = c.local_order_id
                 AND o.client_id = c.client_id
                WHERE c.claim_state = ANY(%s)
                ORDER BY c.claimed_at ASC
                """,
                (list(_ACTIVE_CLAIM_STATES),),
            ).fetchall()
            duplicate_rows = c.execute(
                """
                SELECT client_id, position_id, COUNT(*) AS active_exit_count
                FROM orders
                WHERE kind = 'EXIT' AND status = ANY(%s)
                GROUP BY client_id, position_id
                HAVING COUNT(*) > 1
                ORDER BY client_id, position_id
                """,
                (list(_ACTIVE_EXIT_STATUSES),),
            ).fetchall()
        return {
            "ledger_rows": [dict(row) for row in ledger_rows],
            "claim_rows": [dict(row) for row in claim_rows],
            "duplicate_rows": [dict(row) for row in duplicate_rows],
            "migration_checksums": checksums,
        }

    return dict(run_with_retry(_read) or {})


def _text(value: Any) -> str:
    return str(value or "").strip()


def _exact_identity(value: Any) -> str:
    """Return an identity only when it is a canonical, nonblank text value."""
    if value is None:
        return ""
    text = str(value)
    if not text or text != text.strip():
        return ""
    return text


def _positive_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _classify_claim(row: dict[str, Any], runtime_mode: str) -> dict[str, Any]:
    findings: list[str] = []
    requested_qty = row.get("requested_qty")
    if not _positive_int(requested_qty):
        findings.append("REQUESTED_QTY_MISSING_OR_INVALID")

    local_id = _exact_identity(row.get("local_order_id"))
    order_local_id = _exact_identity(row.get("order_local_order_id"))
    if not local_id or local_id != order_local_id:
        findings.append("LOCAL_ORDER_ID_UNPROVEN")

    if not order_local_id:
        findings.append("DURABLE_EXIT_ROW_MISSING")
    if row.get("order_kind") != "EXIT":
        findings.append("EXIT_KIND_MISMATCH")
    if _exact_identity(row.get("order_client_id")) != _exact_identity(row.get("client_id")):
        findings.append("CLIENT_ID_MISMATCH")
    if _exact_identity(row.get("order_position_id")) != _exact_identity(row.get("position_id")):
        findings.append("POSITION_ID_MISMATCH")

    durable_mode = row.get("order_execution_mode")
    if durable_mode not in {"live", "paper"}:
        findings.append("DURABLE_MODE_INVALID")
    elif durable_mode != runtime_mode:
        findings.append("RUNTIME_DURABLE_MODE_MISMATCH")

    if _positive_int(requested_qty) and row.get("order_qty") != requested_qty:
        findings.append("REQUESTED_QTY_ORDER_MISMATCH")

    status = _text(row.get("order_status")).upper()
    claim_state = _text(row.get("claim_state")).upper()
    if claim_state == "CLAIMED":
        findings.append("CLAIM_STILL_CLAIMED")
    elif claim_state in {"AMBIGUOUS", "STALE_CLAIM_RECONCILING"}:
        findings.append("CLAIM_RECONCILIATION_UNRESOLVED")
    broker_id = _exact_identity(row.get("claim_broker_order_id")) or _exact_identity(
        row.get("order_broker_order_id")
    )
    meta = row.get("order_meta") or {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except (TypeError, ValueError):
            meta = {}
            findings.append("ORDER_META_UNPARSEABLE")
    if not isinstance(meta, dict):
        findings.append("ORDER_META_UNPARSEABLE")
        meta = {}

    if claim_state in {"BROKER_OWNED", "AMBIGUOUS", "STALE_CLAIM_RECONCILING"} and not broker_id:
        # Submit-intent-only rows can be broker-owned without an id, but they
        # are still unresolved for a deployment gate and must be reconciled.
        findings.append("BROKER_OWNERSHIP_UNRESOLVED")
    if status == "EXIT_REQUESTED" and (
        broker_id
        or _text(meta.get("submit_intent_at"))
        or _text(meta.get("broker_submit_key"))
    ):
        findings.append("EXIT_REQUESTED_SUBMIT_EVIDENCE_UNRESOLVED")

    return {
        "generation_key": _text(row.get("generation_key")),
        "claim_state": claim_state,
        "local_order_id": local_id,
        "requested_qty": requested_qty,
        "status": status,
        "broker_order_id_present": bool(broker_id),
        "findings": findings,
        "safe": not findings,
    }


def run_preflight() -> dict[str, Any]:
    runtime_mode, runtime_mode_error = _runtime_mode()
    schema = attest_schema(strict=True)
    snapshot = _read_snapshot(runtime_mode) if runtime_mode else {
        "ledger_rows": [],
        "claim_rows": [],
        "duplicate_rows": [],
        "migration_checksums": {},
    }

    findings: list[str] = []
    if runtime_mode_error:
        findings.append(runtime_mode_error)
    if not schema.get("ok") or schema.get("skipped"):
        findings.append("SCHEMA_ATTESTATION_NOT_PROVEN")

    ledger_by_name = {
        _text(row.get("filename")): row for row in snapshot["ledger_rows"]
    }
    migrations: list[dict[str, Any]] = []
    for filename in _MIGRATIONS:
        row = ledger_by_name.get(filename)
        expected = snapshot["migration_checksums"].get(filename)
        actual = _text(row.get("checksum")) if row else ""
        migration_result = {
            "filename": filename,
            "applied": row is not None,
            "checksum_match": bool(row and expected and actual == expected),
            "applied_at": row.get("applied_at") if row else None,
            "baselined": row.get("baselined") if row else None,
        }
        migrations.append(migration_result)
        if not migration_result["applied"]:
            findings.append(f"MIGRATION_NOT_LEDGERED:{filename}")
        elif not migration_result["checksum_match"]:
            findings.append(f"MIGRATION_CHECKSUM_DRIFT:{filename}")

    claims = [_classify_claim(row, runtime_mode) for row in snapshot["claim_rows"]]
    unsafe_claims = [claim for claim in claims if not claim["safe"]]
    if unsafe_claims:
        findings.append("UNSAFE_ACTIVE_CLAIM_ROWS")
    duplicate_rows = snapshot["duplicate_rows"]
    if duplicate_rows:
        findings.append("MULTIPLE_ACTIVE_EXITS_PER_POSITION")

    return {
        "tool": "ap.broker_owned_exit_recovery_preflight",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "runtime_mode": runtime_mode or None,
        "schema": schema,
        "migrations": migrations,
        "active_claim_count": len(claims),
        "unsafe_claim_count": len(unsafe_claims),
        "duplicate_active_exit_count": len(duplicate_rows),
        "claims": claims,
        "duplicate_active_exits": duplicate_rows,
        "findings": findings,
        "safe": not findings,
        "broker_calls": 0,
        "writes": 0,
    }


def main() -> int:
    try:
        result = run_preflight()
    except Exception as exc:  # noqa: BLE001 — command must report tool failure
        print(json.dumps({
            "tool": "ap.broker_owned_exit_recovery_preflight",
            "tool_error": f"{type(exc).__name__}:{exc}",
            "broker_calls": 0,
            "writes": 0,
        }, indent=2, sort_keys=True, default=str))
        return 1
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0 if result["safe"] else 2


if __name__ == "__main__":
    sys.exit(main())
