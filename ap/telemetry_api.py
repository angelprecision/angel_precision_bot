"""
ap/telemetry_api.py — PR #27 follow-up to PR #23

Flask Blueprint exposing the Phase 6 telemetry projection over HTTP so the
dashboard backend can consume it without re-implementing the
order.meta -> 23-field projection in JavaScript.

Routes
------
  GET /telemetry/entries
        Query params:
          client_id      (required)
          limit          (optional, default 50, max 500)
          status         (optional CSV: FILLED,CANCELED,...)
          since_hours    (optional, default 24, max 168)
        Response:
          {
            "ok": true,
            "client_id": "...",
            "count": N,
            "entries": [ <telemetry dict per row>, ... ]
          }

  GET /telemetry/entries/<local_order_id>
        Response:
          {"ok": true, "entry": <telemetry dict>}
        404 if order not found.

  GET /telemetry/schema
        Returns the canonical 23-field schema so dashboard JS can render
        a column list without round-tripping a SELECT first. Static.

Authentication
--------------
Both endpoints require either:
  - X-Admin-Key header (re-uses ADMIN_KEY / ADMIN_API_KEY)
  - Or X-Telemetry-Key header (TELEMETRY_API_KEY env var)

The telemetry key is a SEPARATE secret from the admin key so the dashboard
backend can be granted read-only access without admin privileges.

Read-only
---------
This Blueprint NEVER writes to the DB. All queries are SELECT.
"""

from __future__ import annotations

import os
from functools import wraps
from typing import Any

from flask import Blueprint, jsonify, request

from ap.logger import get_logger
from ap.db import conn, run_with_retry
from ap.entry_telemetry import compute_entry_telemetry, RESULT_BUCKETS

log = get_logger("ap.telemetry_api")

telemetry_bp = Blueprint("telemetry", __name__, url_prefix="/telemetry")


# ----------------------------------------------------------------------
# Authentication
# ----------------------------------------------------------------------

def _admin_key() -> str:
    return (os.getenv("ADMIN_KEY") or os.getenv("ADMIN_API_KEY") or "").strip()


def _telemetry_key() -> str:
    return (os.getenv("TELEMETRY_API_KEY") or "").strip()


def _check_telemetry_auth(req) -> bool:
    admin = _admin_key()
    tele = _telemetry_key()
    if not admin and not tele:
        log.warning(
            "telemetry_api: neither ADMIN_KEY nor TELEMETRY_API_KEY set "
            "— denying all requests"
        )
        return False
    admin_in = (req.headers.get("X-Admin-Key", "") or "").strip()
    tele_in  = (req.headers.get("X-Telemetry-Key", "") or "").strip()
    if admin and admin_in and admin_in == admin:
        return True
    if tele and tele_in and tele_in == tele:
        return True
    return False


def require_telemetry_auth(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not _check_telemetry_auth(request):
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        return fn(*args, **kwargs)
    return wrapper


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def _clamp_int(raw: Any, default: int, lo: int, hi: int) -> int:
    try:
        v = int(raw) if raw not in (None, "") else default
    except (TypeError, ValueError):
        v = default
    if v < lo:
        v = lo
    if v > hi:
        v = hi
    return v


def _parse_status_csv(raw: str | None) -> list[str]:
    if not raw:
        return []
    out = []
    for s in raw.split(","):
        s = s.strip().upper()
        if s and s.replace("_", "").isalpha():
            out.append(s)
    return out


# ----------------------------------------------------------------------
# DB queries (read-only)
# ----------------------------------------------------------------------

def _select_entries(
    *,
    client_id: str,
    limit: int,
    since_hours: int,
    statuses: list[str],
) -> list[dict]:
    """SELECT entry orders + LEFT JOIN to positions to get fill_price.

    Returns raw dict rows. compute_entry_telemetry() projects each into
    the canonical 23-field dict.
    """
    # Build the optional status filter safely (no string interpolation).
    status_clause = ""
    params: list[Any] = [client_id, since_hours]
    if statuses:
        placeholders = ",".join(["%s"] * len(statuses))
        status_clause = f"AND UPPER(o.status) IN ({placeholders})"
        params.extend(statuses)
    params.append(limit)

    sql = f"""
        SELECT o.client_id,
               o.local_order_id,
               o.broker_order_id,
               o.position_id,
               o.kind,
               o.status,
               o.symbol,
               o.contract,
               o.direction,
               o.qty,
               o.limit_price,
               o.last_error,
               o.created_ts,
               o.updated_ts,
               o.meta,
               p.avg_fill         AS p_avg_fill,
               p.opened_ts        AS p_opened_ts
        FROM   orders o
        LEFT   JOIN positions p
               ON  p.contract  = o.contract
               AND p.client_id = o.client_id
               AND p.status IN ('OPEN','CLOSED','CLOSING','PENDING')
        WHERE  o.client_id = %s
          AND  o.kind = 'ENTRY'
          AND  o.created_ts >= NOW() - (%s || ' hours')::interval
          {status_clause}
        ORDER  BY o.created_ts DESC
        LIMIT  %s
    """

    def _fn():
        with conn() as c:
            c.execute(sql, tuple(params))
            return [dict(r) for r in c.fetchall()]

    return run_with_retry(_fn) or []


def _select_one_entry(local_order_id: str) -> dict | None:
    sql = """
        SELECT o.client_id,
               o.local_order_id,
               o.broker_order_id,
               o.position_id,
               o.kind,
               o.status,
               o.symbol,
               o.contract,
               o.direction,
               o.qty,
               o.limit_price,
               o.last_error,
               o.created_ts,
               o.updated_ts,
               o.meta,
               p.avg_fill         AS p_avg_fill,
               p.opened_ts        AS p_opened_ts
        FROM   orders o
        LEFT   JOIN positions p
               ON  p.contract  = o.contract
               AND p.client_id = o.client_id
               AND p.status IN ('OPEN','CLOSED','CLOSING','PENDING')
        WHERE  o.local_order_id = %s
        LIMIT  1
    """
    def _fn():
        with conn() as c:
            c.execute(sql, (local_order_id,))
            row = c.fetchone()
            return dict(row) if row else None
    return run_with_retry(_fn)


def _project_row(row: dict) -> dict:
    """Shape a DB row into the (order_row, position_row) pair the projection
    helper expects, then return the projection."""
    position_row = None
    if row.get("p_avg_fill") not in (None, "", 0) or row.get("p_opened_ts"):
        position_row = {
            "avg_fill":  row.get("p_avg_fill"),
            "opened_ts": row.get("p_opened_ts"),
        }
    # The order_row mirrors the orders columns plus meta.
    order_row = {k: v for k, v in row.items()
                 if k not in ("p_avg_fill", "p_opened_ts")}
    return compute_entry_telemetry(order_row=order_row,
                                   position_row=position_row)


# ----------------------------------------------------------------------
# Routes
# ----------------------------------------------------------------------

@telemetry_bp.get("/entries")
@require_telemetry_auth
def list_entries():
    client_id = (request.args.get("client_id") or "").strip()
    if not client_id:
        return jsonify({"ok": False, "error": "client_id required"}), 400

    limit       = _clamp_int(request.args.get("limit"),       default=50,  lo=1, hi=500)
    since_hours = _clamp_int(request.args.get("since_hours"), default=24,  lo=1, hi=168)
    statuses    = _parse_status_csv(request.args.get("status"))

    try:
        rows = _select_entries(
            client_id=client_id,
            limit=limit,
            since_hours=since_hours,
            statuses=statuses,
        )
    except Exception as e:
        log.error("telemetry list_entries failed: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": "internal"}), 500

    entries = [_project_row(r) for r in rows]

    return jsonify({
        "ok":        True,
        "client_id": client_id,
        "count":     len(entries),
        "limit":     limit,
        "since_hours": since_hours,
        "statuses":  statuses,
        "entries":   entries,
    })


@telemetry_bp.get("/entries/<local_order_id>")
@require_telemetry_auth
def get_one_entry(local_order_id: str):
    if not local_order_id:
        return jsonify({"ok": False, "error": "local_order_id required"}), 400
    try:
        row = _select_one_entry(local_order_id)
    except Exception as e:
        log.error("telemetry get_one_entry failed: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": "internal"}), 500
    if not row:
        return jsonify({"ok": False, "error": "not_found"}), 404
    return jsonify({"ok": True, "entry": _project_row(row)})


# Schema endpoint: returns the canonical key list + bucket enum so the
# dashboard can render columns / legends without calling /entries first.
SCHEMA = {
    "version": "1.0.0",
    "keys": [
        "client_id", "local_order_id", "broker_order_id",
        "symbol", "contract", "direction", "status", "score",
        "entry_attempt", "repeg_attempt", "retry_attempt",
        "reason_bucket", "cancel_reason_detail",
        "selector_ask", "submit_ask", "submit_limit", "fill_price",
        "quote_age_ms", "seconds_to_fill",
        "account_equity", "position_budget", "final_qty", "sizing_reason_code",
    ],
    "reason_buckets": list(RESULT_BUCKETS),
}


@telemetry_bp.get("/schema")
@require_telemetry_auth
def get_schema():
    return jsonify({"ok": True, **SCHEMA})
