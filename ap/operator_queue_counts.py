"""
ap.operator_queue_counts
────────────────────────
Production-safe operator Trade Queue read model.

This restores compatibility between the operator console buckets and the newer
post-hardening state vocabulary:

- trade_queue.PENDING_TRIGGER / orders.PENDING_TRIGGER are active watcher flow
  and must display under the dashboard WATCHING bucket.
- broker-inflight entry states display under TRIGGERED.

Read-only. No queue/order mutation. No broker interaction.
"""
from __future__ import annotations

import os
import hmac
from typing import Any

from flask import jsonify, request


WATCHING_STATUSES = {"WATCHING", "PENDING_TRIGGER"}
TRIGGERED_STATUSES = {"TRIGGERED", "SUBMITTED", "ACK", "ACKNOWLEDGED", "WORKING"}
TERMINAL_STATUSES = {"REJECTED", "EXPIRED"}


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _dashboard_bucket(status: str | None) -> str:
    s = str(status or "").strip().upper()
    if s == "NEW":
        return "NEW"
    if s in WATCHING_STATUSES:
        return "WATCHING"
    if s in TRIGGERED_STATUSES:
        return "TRIGGERED"
    if s == "REJECTED":
        return "REJECTED"
    if s == "EXPIRED":
        return "EXPIRED"
    return s or "UNKNOWN"


def _authorized_admin_request() -> bool:
    expected = (os.getenv("ADMIN_API_KEY") or os.getenv("ADMIN_KEY") or "").strip()
    if not expected:
        return False
    supplied = (request.headers.get("X-Admin-Key") or request.headers.get("X-API-Key") or "").strip()
    return hmac.compare_digest(supplied, expected)


def _row_value(row: dict, *names: str):
    for name in names:
        if name in row and row.get(name) is not None:
            return row.get(name)
    return None


def _extract_order_meta(row: dict) -> dict:
    meta = row.get("meta") or {}
    return meta if isinstance(meta, dict) else {}


def _format_trade_queue_row(row: dict) -> dict:
    payload = row.get("payload") or {}
    if not isinstance(payload, dict):
        payload = {}
    trigger = payload.get("trigger") or {}
    if not isinstance(trigger, dict):
        trigger = {}
    status = str(row.get("status") or "").upper()
    return {
        "source": "trade_queue",
        "id": row.get("id"),
        "created_ts": row.get("created_ts"),
        "client_id": row.get("client_id"),
        "ticker": payload.get("ticker") or payload.get("symbol"),
        "side": payload.get("side") or payload.get("direction"),
        "status": status,
        "dashboard_status": _dashboard_bucket(status),
        "score": payload.get("score") or payload.get("ev_score"),
        "trigger_price": (
            payload.get("entry_trigger")
            or payload.get("trigger_price")
            or trigger.get("entry")
            or trigger.get("trigger")
        ),
        "contract": payload.get("contract_symbol") or payload.get("contract"),
        "last_error": row.get("last_error"),
        "signal_id": row.get("signal_id"),
    }


def _format_order_row(row: dict) -> dict:
    meta = _extract_order_meta(row)
    status = str(row.get("status") or "").upper()
    return {
        "source": "orders",
        "id": row.get("local_order_id") or row.get("id"),
        "created_ts": row.get("created_ts"),
        "client_id": row.get("client_id"),
        "ticker": row.get("symbol") or meta.get("ticker") or meta.get("symbol"),
        "side": row.get("side") or meta.get("side") or meta.get("direction"),
        "status": status,
        "dashboard_status": _dashboard_bucket(status),
        "score": row.get("score") or meta.get("score"),
        "trigger_price": meta.get("trigger_price") or meta.get("entry_trigger"),
        "contract": row.get("contract") or meta.get("contract_symbol"),
        "last_error": row.get("last_error"),
        "signal_id": row.get("signal_id") or meta.get("signal_id"),
    }


def _empty_counts() -> dict[str, int]:
    return {"NEW": 0, "WATCHING": 0, "TRIGGERED": 0, "REJECTED": 0, "EXPIRED": 0}


def build_operator_queue_counts(*, client_id: str | None = None, hours: int = 24, limit: int = 200) -> dict:
    """Build the operator Trade Queue read model from trade_queue + orders.

    Uses correct schemas:
    - trade_queue timing: created_ts / started_ts / finished_ts
    - orders timing: created_ts / updated_ts
    """
    from ap.db import conn, run_with_retry

    hours = max(1, min(int(hours or 24), 168))
    limit = max(1, min(int(limit or 200), 1000))

    tq_params: list[Any] = [str(hours)]
    tq_where = ["created_ts >= NOW() - (%s::text || ' hours')::interval"]
    if client_id:
        tq_where.append("client_id = %s")
        tq_params.append(client_id)
    tq_params.append(limit)

    order_params: list[Any] = [str(hours)]
    order_where = ["created_ts >= NOW() - (%s::text || ' hours')::interval"]
    if client_id:
        order_where.append("client_id = %s")
        order_params.append(client_id)
    order_params.append(limit)

    tq_sql = (
        "SELECT id, client_id, signal_id, status, created_ts, started_ts, finished_ts, "
        "       payload, last_error "
        "FROM trade_queue "
        "WHERE " + " AND ".join(tq_where) + " "
        "ORDER BY created_ts DESC LIMIT %s"
    )
    order_sql = (
        "SELECT local_order_id, client_id, kind, status, broker_order_id, symbol, side, "
        "       contract, score, signal_id, created_ts, updated_ts, last_error, meta "
        "FROM orders "
        "WHERE " + " AND ".join(order_where) + " "
        "  AND kind = 'ENTRY' "
        "  AND UPPER(COALESCE(status,'')) IN "
        "      ('PENDING_TRIGGER','SUBMITTED','ACK','ACKNOWLEDGED','WORKING','REJECTED','EXPIRED') "
        "ORDER BY created_ts DESC LIMIT %s"
    )

    def _fetch():
        with conn() as c:
            tq_rows = c.execute(tq_sql, tuple(tq_params)).fetchall()
            order_rows = c.execute(order_sql, tuple(order_params)).fetchall()
            return tq_rows, order_rows

    tq_rows, order_rows = run_with_retry(_fetch)
    rows: list[dict] = []
    seen_order_ids: set[str] = set()

    for row in order_rows or []:
        formatted = _format_order_row(row)
        oid = str(formatted.get("id") or "")
        if oid:
            seen_order_ids.add(oid)
        rows.append(formatted)

    for row in tq_rows or []:
        formatted = _format_trade_queue_row(row)
        # Avoid showing the same watcher twice when an OSM order already owns it.
        rows.append(formatted)

    counts = _empty_counts()
    for row in rows:
        bucket = row.get("dashboard_status") or "UNKNOWN"
        if bucket in counts:
            counts[bucket] += 1

    active_queue_signals = counts["NEW"] + counts["WATCHING"] + counts["TRIGGERED"]
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
        "active_queue_signals": active_queue_signals,
        "active_order_flow": counts["WATCHING"] + counts["TRIGGERED"],
        "rows": rows[:limit],
        "schema_contract": {
            "trade_queue_timestamp_fields": ["created_ts", "started_ts", "finished_ts"],
            "orders_timestamp_fields": ["created_ts", "updated_ts"],
            "status_compatibility": {
                "PENDING_TRIGGER": "WATCHING",
                "SUBMITTED/ACK/ACKNOWLEDGED/WORKING": "TRIGGERED",
            },
        },
    }


def operator_queue_counts_view():
    if not _authorized_admin_request():
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    try:
        client_id = (request.args.get("client_id") or "").strip() or None
        hours = int(request.args.get("hours") or 24)
        limit = int(request.args.get("limit") or 200)
        return jsonify(build_operator_queue_counts(client_id=client_id, hours=hours, limit=limit))
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc), "rows": [], "counts": _empty_counts()}), 500


def install_operator_queue_counts_route() -> None:
    """Install endpoint without editing app.py's large inline route block.

    app.py imports the ap package before create_app() calls Flask(...). Patching
    Flask.__init__ here lets this read-only endpoint register on the app during
    construction while keeping trading code untouched.
    """
    try:
        import flask
    except Exception:
        return

    flask_cls = flask.Flask
    if getattr(flask_cls, "_ap_operator_queue_counts_patched", False):
        return

    original_init = flask_cls.__init__

    def patched_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        try:
            if "ap_operator_queue_counts" not in self.view_functions:
                self.add_url_rule(
                    "/admin/operator/queue_counts",
                    endpoint="ap_operator_queue_counts",
                    view_func=operator_queue_counts_view,
                    methods=["GET"],
                )
                # Backward-compatible alias for older operator panels that still
                # call the queue debug route. This alias is admin-key protected.
                self.add_url_rule(
                    "/debug/queue_counts",
                    endpoint="ap_operator_queue_counts_legacy_debug_alias",
                    view_func=operator_queue_counts_view,
                    methods=["GET"],
                )
        except Exception:
            # Route visibility must never block app boot.
            pass

    flask_cls.__init__ = patched_init
    flask_cls._ap_operator_queue_counts_patched = True
