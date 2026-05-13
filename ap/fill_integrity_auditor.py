# ap/fill_integrity_auditor.py
"""
Supabase Fill Integrity Auditor
===============================

Purpose
-------
Compare Supabase order/position fill data against broker truth and report/repair
bad fill rows safely.

This module is intentionally side-effect safe by default:
- dry_run=True reports mismatches only
- dry_run=False updates only columns that can be proven from broker order truth
- broker truth wins for order status, cumulative filled_qty, and average fill price
- no synthetic fill quantity is invented when broker does not expose one

Typical usage from a live ClientRunner / reconciler context:

    from ap.fill_integrity_auditor import audit_client_fills
    report = audit_client_fills(broker=self.broker, client_id=self.email, dry_run=True)

To actually repair proven mismatches:

    report = audit_client_fills(broker=self.broker, client_id=self.email, dry_run=False)

Required:
- DATABASE_URL already configured for ap.db
- broker object must expose get_order(broker_order_id)
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Optional

from ap.db import conn, run_with_retry
from ap.utils import now_utc_iso

log = logging.getLogger("ap.fill_integrity_auditor")

ENTRY_FILLED_STATUSES = {"FILLED", "PARTIAL_FILL"}
EXIT_FILLED_STATUSES = {"EXIT_FILLED", "EXIT_PARTIAL_FILL"}
DB_AUDITABLE_STATUSES = ENTRY_FILLED_STATUSES | EXIT_FILLED_STATUSES | {
    "SUBMITTED",
    "ACKNOWLEDGED",
    "EXIT_SUBMITTED",
    "EXIT_ACKNOWLEDGED",
}

BROKER_FULL_FILLED = {"filled"}
BROKER_PARTIAL_FILLED = {"partially_filled", "partial"}
BROKER_TERMINAL_CANCEL = {"canceled", "cancelled", "rejected", "expired"}

PRICE_TOLERANCE = float(os.getenv("FILL_AUDIT_PRICE_TOLERANCE", "0.0001"))


@dataclass
class FillAuditMismatch:
    client_id: str
    local_order_id: str
    broker_order_id: str
    kind: str
    db_status: str
    broker_status: str
    field: str
    db_value: Any
    broker_value: Any
    severity: str
    repaired: bool = False
    note: str = ""


@dataclass
class FillAuditReport:
    client_id: str
    checked: int = 0
    mismatches: int = 0
    repaired: int = 0
    errors: int = 0
    dry_run: bool = True
    started_at: str = ""
    finished_at: str = ""
    items: list[FillAuditMismatch] | None = None

    def to_dict(self) -> dict:
        d = asdict(self)
        d["items"] = [asdict(x) for x in (self.items or [])]
        return d


def _safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    if value is None or value == "":
        return default
    try:
        return int(float(value))
    except Exception:
        return default


def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except Exception:
        return default


def _norm_status(value: Any) -> str:
    return str(value or "").strip().lower()


def _extract_broker_filled_qty(raw: dict) -> Optional[int]:
    """Return explicit cumulative broker fill qty, never invented qty."""
    for key in (
        "filled_qty",
        "filled_quantity",
        "cumulative_filled_qty",
        "cumulative_filled_quantity",
        "exec_quantity",
        "executed_quantity",
        "filled",
    ):
        qty = _safe_int(raw.get(key), None)
        if qty is not None and qty >= 0:
            return qty
    return None


def _extract_broker_avg_fill(raw: dict) -> Optional[float]:
    for key in (
        "avg_fill_price",
        "average_fill_price",
        "fill_price",
        "filled_avg_price",
        "avg_price",
        "price",
    ):
        px = _safe_float(raw.get(key), None)
        if px is not None and px > 0:
            return px
    return None


def _expected_db_status(kind: str, broker_status: str, filled_qty: Optional[int], order_qty: int) -> Optional[str]:
    kind_u = str(kind or "ENTRY").upper()
    b = _norm_status(broker_status)
    if b in BROKER_FULL_FILLED:
        return "EXIT_FILLED" if kind_u == "EXIT" else "FILLED"
    if b in BROKER_PARTIAL_FILLED:
        if filled_qty is not None and order_qty > 0 and filled_qty >= order_qty:
            return "EXIT_FILLED" if kind_u == "EXIT" else "FILLED"
        return "EXIT_PARTIAL_FILL" if kind_u == "EXIT" else "PARTIAL_FILL"
    if b in BROKER_TERMINAL_CANCEL:
        return "CANCELED" if b in {"canceled", "cancelled"} else b.upper()
    return None


def ensure_fill_audit_table() -> None:
    """Create a simple audit table if it does not exist."""
    def _fn():
        with conn() as c:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS fill_integrity_audit (
                    id BIGSERIAL PRIMARY KEY,
                    ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    client_id TEXT NOT NULL,
                    local_order_id TEXT,
                    broker_order_id TEXT,
                    severity TEXT NOT NULL,
                    field TEXT NOT NULL,
                    db_value TEXT,
                    broker_value TEXT,
                    repaired BOOLEAN NOT NULL DEFAULT FALSE,
                    payload JSONB
                )
                """
            )
    run_with_retry(_fn)


def _load_candidate_orders(client_id: str, lookback_limit: int) -> list[dict]:
    def _fn():
        with conn() as c:
            c.execute(
                """
                SELECT
                    client_id,
                    local_order_id,
                    broker_order_id,
                    position_id,
                    kind,
                    status,
                    symbol,
                    contract,
                    qty,
                    filled_qty,
                    fill_price,
                    created_ts,
                    updated_ts
                FROM orders
                WHERE client_id = %s
                  AND broker_order_id IS NOT NULL
                  AND broker_order_id != ''
                  AND broker_order_id != 'N/A'
                  AND status = ANY(%s)
                ORDER BY COALESCE(updated_ts, created_ts) DESC
                LIMIT %s
                """,
                (client_id, list(DB_AUDITABLE_STATUSES), int(lookback_limit)),
            )
            return c.fetchall()
    return run_with_retry(_fn) or []


def _write_audit_row(item: FillAuditMismatch) -> None:
    payload = asdict(item)
    def _fn():
        with conn() as c:
            c.execute(
                """
                INSERT INTO fill_integrity_audit (
                    client_id, local_order_id, broker_order_id,
                    severity, field, db_value, broker_value, repaired, payload
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                """,
                (
                    item.client_id,
                    item.local_order_id,
                    item.broker_order_id,
                    item.severity,
                    item.field,
                    str(item.db_value),
                    str(item.broker_value),
                    bool(item.repaired),
                    json.dumps(payload, default=str),
                ),
            )
    run_with_retry(_fn)


def _repair_order_from_broker(
    *,
    order: dict,
    expected_status: Optional[str],
    broker_filled_qty: Optional[int],
    broker_avg_fill: Optional[float],
) -> int:
    """Repair only proven broker-backed fields on orders table."""
    updates: list[str] = []
    params: list[Any] = []

    if expected_status:
        updates.append("status=%s")
        params.append(expected_status)
    if broker_filled_qty is not None:
        updates.append("filled_qty=%s")
        params.append(int(broker_filled_qty))
    if broker_avg_fill is not None:
        updates.append("fill_price=%s")
        params.append(float(broker_avg_fill))

    if not updates:
        return 0

    updates.append("updated_ts=%s")
    params.append(now_utc_iso())
    params.append(order["local_order_id"])
    params.append(order["client_id"])

    def _fn():
        with conn() as c:
            c.execute(
                f"""
                UPDATE orders
                SET {', '.join(updates)}
                WHERE local_order_id=%s
                  AND client_id=%s
                """,
                tuple(params),
            )
            return c.rowcount
    return int(run_with_retry(_fn) or 0)


def _sync_position_entry_fill(order: dict, broker_avg_fill: Optional[float], broker_filled_qty: Optional[int]) -> int:
    """For ENTRY fills, sync the attached OPEN position entry price/qty when safe."""
    if str(order.get("kind") or "ENTRY").upper() != "ENTRY":
        return 0
    position_id = order.get("position_id")
    if not position_id:
        return 0
    updates: list[str] = []
    params: list[Any] = []
    if broker_avg_fill is not None:
        updates.append("avg_fill=%s")
        params.append(float(broker_avg_fill))
    if broker_filled_qty is not None and broker_filled_qty > 0:
        updates.append("qty=%s")
        params.append(int(broker_filled_qty))
    if not updates:
        return 0
    updates.append("updated_ts=%s")
    params.append(now_utc_iso())
    params.append(position_id)
    params.append(order["client_id"])

    def _fn():
        with conn() as c:
            c.execute(
                f"""
                UPDATE positions
                SET {', '.join(updates)}
                WHERE id=%s
                  AND client_id=%s
                  AND status IN ('OPEN','CLOSING')
                """,
                tuple(params),
            )
            return c.rowcount
    return int(run_with_retry(_fn) or 0)


def audit_client_fills(
    *,
    broker,
    client_id: str,
    dry_run: bool = True,
    lookback_limit: int = 250,
    write_audit_rows: bool = True,
) -> FillAuditReport:
    """
    Compare recent Supabase fill rows against broker truth.

    dry_run=True: report only.
    dry_run=False: repair orders table + safe position entry fields.
    """
    ensure_fill_audit_table()
    started = datetime.now(timezone.utc).isoformat()
    report = FillAuditReport(
        client_id=client_id,
        dry_run=dry_run,
        started_at=started,
        items=[],
    )

    rows = _load_candidate_orders(client_id, lookback_limit)
    report.checked = len(rows)

    for order in rows:
        local_id = str(order.get("local_order_id") or "")
        broker_oid = str(order.get("broker_order_id") or "")
        kind = str(order.get("kind") or "ENTRY").upper()
        db_status = str(order.get("status") or "").upper()
        db_filled_qty = _safe_int(order.get("filled_qty"), 0) or 0
        db_fill_price = _safe_float(order.get("fill_price"), None)
        order_qty = _safe_int(order.get("qty"), 0) or 0

        try:
            raw = broker.get_order(broker_oid) or {}
        except Exception as exc:
            report.errors += 1
            item = FillAuditMismatch(
                client_id=client_id,
                local_order_id=local_id,
                broker_order_id=broker_oid,
                kind=kind,
                db_status=db_status,
                broker_status="ERROR",
                field="broker_get_order",
                db_value="",
                broker_value=str(exc),
                severity="ERROR",
                note="Could not fetch broker order; no repair attempted.",
            )
            report.items.append(item)
            if write_audit_rows:
                _write_audit_row(item)
            continue

        broker_status = str(raw.get("status") or raw.get("Status") or "").strip()
        broker_filled_qty = _extract_broker_filled_qty(raw)
        broker_avg_fill = _extract_broker_avg_fill(raw)
        expected_status = _expected_db_status(kind, broker_status, broker_filled_qty, order_qty)

        order_needs_repair = False
        local_items: list[FillAuditMismatch] = []

        if expected_status and db_status != expected_status:
            severity = "HIGH" if expected_status in {"FILLED", "EXIT_FILLED"} else "MEDIUM"
            local_items.append(FillAuditMismatch(
                client_id=client_id,
                local_order_id=local_id,
                broker_order_id=broker_oid,
                kind=kind,
                db_status=db_status,
                broker_status=broker_status,
                field="status",
                db_value=db_status,
                broker_value=expected_status,
                severity=severity,
            ))
            order_needs_repair = True

        if broker_filled_qty is not None and db_filled_qty != broker_filled_qty:
            local_items.append(FillAuditMismatch(
                client_id=client_id,
                local_order_id=local_id,
                broker_order_id=broker_oid,
                kind=kind,
                db_status=db_status,
                broker_status=broker_status,
                field="filled_qty",
                db_value=db_filled_qty,
                broker_value=broker_filled_qty,
                severity="HIGH",
            ))
            order_needs_repair = True

        if broker_avg_fill is not None:
            if db_fill_price is None or abs(float(db_fill_price) - float(broker_avg_fill)) > PRICE_TOLERANCE:
                local_items.append(FillAuditMismatch(
                    client_id=client_id,
                    local_order_id=local_id,
                    broker_order_id=broker_oid,
                    kind=kind,
                    db_status=db_status,
                    broker_status=broker_status,
                    field="fill_price",
                    db_value=db_fill_price,
                    broker_value=broker_avg_fill,
                    severity="HIGH",
                ))
                order_needs_repair = True

        repaired_count = 0
        if order_needs_repair and not dry_run:
            try:
                repaired_count += _repair_order_from_broker(
                    order=order,
                    expected_status=expected_status,
                    broker_filled_qty=broker_filled_qty,
                    broker_avg_fill=broker_avg_fill,
                )
                repaired_count += _sync_position_entry_fill(order, broker_avg_fill, broker_filled_qty)
            except Exception as exc:
                report.errors += 1
                local_items.append(FillAuditMismatch(
                    client_id=client_id,
                    local_order_id=local_id,
                    broker_order_id=broker_oid,
                    kind=kind,
                    db_status=db_status,
                    broker_status=broker_status,
                    field="repair_error",
                    db_value="",
                    broker_value=str(exc),
                    severity="ERROR",
                    note="Repair failed after mismatch detection.",
                ))

        for item in local_items:
            if repaired_count > 0 and item.severity != "ERROR":
                item.repaired = not dry_run
            report.items.append(item)
            report.mismatches += 1
            if item.repaired:
                report.repaired += 1
            if write_audit_rows:
                _write_audit_row(item)

    report.finished_at = datetime.now(timezone.utc).isoformat()
    return report


def audit_all_runner_fills(runners: dict, *, dry_run: bool = True, lookback_limit: int = 250) -> dict:
    """
    Helper for dashboard/supervisor code: pass active runner registry.
    Each runner must expose .broker and .email.
    """
    reports = {}
    for client_id, runner in list((runners or {}).items()):
        broker = getattr(runner, "broker", None)
        email = getattr(runner, "email", client_id)
        if broker is None:
            reports[email] = {"error": "runner has no broker"}
            continue
        reports[email] = audit_client_fills(
            broker=broker,
            client_id=email,
            dry_run=dry_run,
            lookback_limit=lookback_limit,
        ).to_dict()
    return reports
