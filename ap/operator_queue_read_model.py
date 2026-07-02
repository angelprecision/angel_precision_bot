"""
ap.operator_queue_read_model
────────────────────────────
Production-safe read model for the Operator Console Trade Queue card.

This module intentionally does not mutate anything. It only translates the bot's
current queue/order statuses into the dashboard buckets the operator page
already renders:

    NEW / WATCHING / TRIGGERED / REJECTED / EXPIRED

Compatibility after the recent hardening stack:
- orders.PENDING_TRIGGER is active watcher flow and displays as WATCHING.
- orders.SUBMITTED/ACK/ACKNOWLEDGED/WORKING displays as TRIGGERED.

Schema guard:
- trade_queue does not have updated_ts. Use created_ts/started_ts/finished_ts.
- orders does have updated_ts.
"""
from __future__ import annotations

from typing import Any


DASHBOARD_BUCKETS = ("NEW", "WATCHING", "TRIGGERED", "REJECTED", "EXPIRED")


def dashboard_queue_bucket(status: str | None) -> str:
    s = str(status or "").strip().upper()
    if s == "NEW":
        return "NEW"
    if s in {"WATCHING", "PENDING_TRIGGER"}:
        return "WATCHING"
    if s in {"TRIGGERED", "SUBMITTED", "ACK", "ACKNOWLEDGED", "WORKING"}:
        return "TRIGGERED"
    if s == "REJECTED":
        return "REJECTED"
    if s == "EXPIRED":
        return "EXPIRED"
    return "UNKNOWN"


def empty_queue_counts() -> dict[str, int]:
    return {bucket: 0 for bucket in DASHBOARD_BUCKETS}


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _dict(row: Any) -> dict:
    if isinstance(row, dict):
        return row
    try:
        return dict(row)
    except Exception:
        return {}


def _trade_queue_row(row: Any) -> dict:
    r = _dict(row)
    payload = r.get("payload") or {}
    if not isinstance(payload, dict):
        payload = {}
    trigger = payload.get("trigger") or {}
    if not isinstance(trigger, dict):
        trigger = {}
    status = str(r.get("status") or "").upper()
    bucket = dashboard_queue_bucket(status)
    return {
        "source": "trade_queue",
        "id": r.get("id"),
        "client_id": r.get("client_id"),
        "signal_id": r.get("signal_id") or payload.get("signal_id"),
        "created_ts": r.get("created_ts"),
        "status": status,
        "dashboard_status": bucket,
        "ticker": payload.get("ticker") or payload.get("symbol"),
        "side": payload.get("side") or payload.get("direction"),
        "score": payload.get("score") or payload.get("ev_score"),
        "trigger_price": payload.get("entry_trigger") or payload.get("trigger_price") or trigger.get("entry"),
        "contract": payload.get("contract_symbol") or payload.get("contract"),
        "last_error": r.get("last_error"),
    }


def _order_row(row: Any) -> dict:
    r = _dict(row)
    meta = r.get("meta") or {}
    if not isinstance(meta, dict):
        meta = {}
    status = str(r.get("status") or "").upper()
    bucket = dashboard_queue_bucket(status)
    return {
        "source": "orders",
        "id": r.get("local_order_id") or r.get("id"),
        "client_id": r.get("client_id"),
        "signal_id": r.get("signal_id") or meta.get("signal_id"),
        "created_ts": r.get("created_ts"),
        "status": status,
        "dashboard_status": bucket,
        "ticker": r.get("symbol") or meta.get("ticker") or meta.get("symbol"),
        "side": r.get("side") or meta.get("side") or meta.get("direction"),
        "score": r.get("score") or meta.get("score"),
        "trigger_price": meta.get("trigger_price") or meta.get("entry_trigger"),
        "contract": r.get("contract") or meta.get("contract_symbol"),
        "last_error": r.get("last_error"),
    }


def build_operator_queue_read_model(*, client_id: str | None = None, hours: int = 24, limit: int = 200) -> dict:
    from ap.db import conn, run_with_retry

    hours = max(1, min(int(hours or 24), 168))
    limit = max(1, min(int(limit or 200), 1000))

    tq_where = ["created_ts >= NOW() - (%s::text || ' hours')::interval"]
    tq_params: list[Any] = [str(hours)]
    order_where = ["created_ts >= NOW() - (%s::text || ' hours')::interval"]
    order_params: list[Any] = [str(hours)]
    if client_id:
        tq_where.append("client_id = %s")
        tq_params.append(client_id)
        order_where.append("client_id = %s")
        order_params.append(client_id)

    tq_params.append(limit)
    order_params.append(limit)

    # IMPORTANT: trade_queue has no updated_ts column.
    tq_sql = (
        "SELECT id, client_id, signal_id, status, created_ts, started_ts, finished_ts, payload, last_error "
        "FROM trade_queue "
        "WHERE " + " AND ".join(tq_where) + " "
        "ORDER BY created_ts DESC LIMIT %s"
    )
    order_sql = (
        "SELECT local_order_id, client_id, kind, status, broker_order_id, symbol, side, contract, "
        "       score, signal_id, created_ts, updated_ts, last_error, meta "
        "FROM orders "
        "WHERE " + " AND ".join(order_where) + " "
        "  AND kind = 'ENTRY' "
        "  AND UPPER(COALESCE(status,'')) IN "
        "      ('PENDING_TRIGGER','SUBMITTED','ACK','ACKNOWLEDGED','WORKING','REJECTED','EXPIRED') "
        "ORDER BY updated_ts DESC LIMIT %s"
    )

    def _fetch():
        with conn() as c:
            return c.execute(tq_sql, tuple(tq_params)).fetchall(), c.execute(order_sql, tuple(order_params)).fetchall()

    trade_queue_rows, order_rows = run_with_retry(_fetch)

    rows = [_order_row(r) for r in (order_rows or [])] + [_trade_queue_row(r) for r in (trade_queue_rows or [])]
    counts = empty_queue_counts()
    for row in rows:
        bucket = row.get("dashboard_status")
        if bucket in counts:
            counts[bucket] += 1

    active = counts["NEW"] + counts["WATCHING"] + counts["TRIGGERED"]
    return {
        "ok": True,
        "client_id": client_id,
        "hours": hours,
        "counts": counts,
        "NEW": counts["NEW"],
        "WATCHING": counts["WATCHING"],
        "TRIGGERED": counts["TRIGGERED"],
        "REJECTED": counts["REJECTED"],
        "EXPIRED": counts["EXPIRED"],
        "active_queue_signals": active,
        "pending_signals": active,
        "rows": rows[:limit],
    }
