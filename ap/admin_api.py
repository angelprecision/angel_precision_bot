# ap/admin_api.py - COMPLETE ADMIN API (FIXED)
# Client management + reporting + monitoring + control
# Multi-client ready, drop-in replacement
#
# ✅ FIXES INCLUDED
# - Admin key auth reads ADMIN_KEY (and ADMIN_API_KEY as fallback)
# - /admin/clients now ALWAYS passes broker_type
# - broker_token is ALWAYS encrypted at create time
# - PATCH supports updating broker_token (will auto-encrypt if plaintext)
# - Safer error messages + consistent JSON

from __future__ import annotations

import os
from functools import wraps
from typing import Any, Dict

from flask import Blueprint, jsonify, request

from ap.crypto import encrypt_token
from ap.logger import get_logger
from ap.db import (
    conn,
    run_with_retry,
    get_client,
    get_all_clients,
    create_client,
    update_client,
    get_client_state,
    update_client_state,
    list_orders,
    list_positions,
    list_audit,
    get_open_orders_for_reconcile,
)

log = get_logger("ap.admin_api")

admin_bp = Blueprint("admin", __name__, url_prefix="/admin")


# ===================================================================
# SECURITY
# ===================================================================

def _admin_key_value() -> str:
    # Support both env var names to avoid confusion
    return (os.getenv("ADMIN_KEY") or os.getenv("ADMIN_API_KEY") or "").strip()


def _check_admin_key(req) -> bool:
    """
    Check X-Admin-Key header.
    HIGH-004: If no admin key set, deny all and log warning.
    """
    admin_key = _admin_key_value()
    if not admin_key:
        log.warning("ADMIN_KEY / ADMIN_API_KEY not set -- denying all admin requests")
        return False

    provided = (req.headers.get("X-Admin-Key", "") or "").strip()
    return provided == admin_key


def require_admin_key(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not _check_admin_key(request):
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        return fn(*args, **kwargs)
    return wrapper


# ===================================================================
# HELPERS
# ===================================================================

def _json_error(msg: str, status: int = 400, **extra):
    payload = {"ok": False, "error": msg}
    if extra:
        payload.update(extra)
    return jsonify(payload), status


def _as_float(val, default: float) -> float:
    try:
        return float(val)
    except Exception:
        return float(default)


def _as_int(val, default: int) -> int:
    try:
        return int(val)
    except Exception:
        return int(default)


def _encrypt_maybe(token: str) -> str:
    """
    Always encrypt.
    If encryption key is missing, encrypt_token will raise and we return 500.
    """
    return encrypt_token(token)


# ===================================================================
# CLIENT MANAGEMENT
# ===================================================================

@admin_bp.post("/clients")
@require_admin_key
def create_client_endpoint():
    """
    Create a new client (stores encrypted broker token)

    Body:
    {
        "client_id": "default_v4",
        "name": "Angel Default",
        "broker_type": "tradier",
        "broker_account_id": "VA12345678",
        "broker_token": "TRADIER_ACCESS_TOKEN",
        "broker_base_url": "https://sandbox.tradier.com",
        "initial_equity": 5000,
        # AUDIT PHASE-2: raised from 5 -> 12 and 3 -> 4.
        "max_trades_per_day": 12,
        "max_concurrent_positions": 4,
        "daily_max_loss_pct": 0.05,
        "base_position_pct": 0.10
    }
    """
    try:
        body: Dict[str, Any] = request.get_json(force=True) or {}

        client_id = (body.get("client_id") or "").strip()
        name = (body.get("name") or "").strip()
        broker_type = (body.get("broker_type") or "tradier").strip()
        broker_account_id = (body.get("broker_account_id") or "").strip()
        broker_token = (body.get("broker_token") or "").strip()
        broker_base_url = (body.get("broker_base_url") or "https://sandbox.tradier.com").strip()

        if not client_id:
            return _json_error("missing_field: client_id", 400)
        if not name:
            return _json_error("missing_field: name", 400)
        if not broker_type:
            return _json_error("missing_field: broker_type", 400)
        if not broker_account_id:
            return _json_error("missing_field: broker_account_id", 400)
        if not broker_token:
            return _json_error("missing_field: broker_token", 400)
        if not broker_base_url:
            return _json_error("missing_field: broker_base_url", 400)

        initial_equity = _as_float(body.get("initial_equity", 100000.0), 100000.0)

        # Risk knobs
        # AUDIT PHASE-2: default raised from 5 -> 12 and 3 -> 4.
        max_trades_per_day = _as_int(body.get("max_trades_per_day", 12), 12)
        max_concurrent_positions = _as_int(body.get("max_concurrent_positions", 4), 4)
        daily_max_loss_pct = _as_float(body.get("daily_max_loss_pct", 0.05), 0.05)
        base_position_pct = _as_float(body.get("base_position_pct", 0.10), 0.10)

        enc = _encrypt_maybe(broker_token)

        client = create_client(
            client_id=client_id,
            name=name,
            broker_type=broker_type,                # ✅ REQUIRED
            broker_account_id=broker_account_id,
            broker_token=enc,                       # ✅ ENCRYPTED
            broker_base_url=broker_base_url,
            initial_equity=initial_equity,
            max_trades_per_day=max_trades_per_day,
            max_concurrent_positions=max_concurrent_positions,
            daily_max_loss_pct=daily_max_loss_pct,
            base_position_pct=base_position_pct,
        )

        log.info(f"Admin created client: {client_id}")
        return jsonify({"ok": True, "client": client}), 201

    except Exception as e:
        log.error(f"Create client failed: {e}", exc_info=True)
        return _json_error("create_client_failed", 500, details=str(e))


@admin_bp.get("/clients")
@require_admin_key
def list_clients_endpoint():
    """List all clients (optionally filtered by status)"""
    try:
        status = request.args.get("status")
        clients = get_all_clients(status)

        return jsonify({"ok": True, "count": len(clients), "clients": clients}), 200

    except Exception as e:
        log.error(f"List clients failed: {e}", exc_info=True)
        return _json_error("list_clients_failed", 500, details=str(e))


@admin_bp.get("/clients/<client_id>")
@require_admin_key
def get_client_endpoint(client_id):
    """Get a specific client + state"""
    try:
        client = get_client(client_id)
        state = get_client_state(client_id)

        return jsonify({"ok": True, "client": client, "state": state}), 200

    except Exception as e:
        log.error(f"Get client failed: {e}")
        return _json_error("client_not_found", 404, details=str(e))


@admin_bp.patch("/clients/<client_id>")
@require_admin_key
def update_client_endpoint(client_id):
    """
    Update client configuration.
    Supports updating broker_token: if provided, we encrypt it before save.
    """
    try:
        body: Dict[str, Any] = request.get_json(force=True) or {}

        # If broker_token is provided in PATCH, encrypt it before updating.
        if "broker_token" in body and body["broker_token"]:
            body["broker_token"] = _encrypt_maybe(str(body["broker_token"]).strip())

        client = update_client(client_id, **body)

        return jsonify({"ok": True, "client": client}), 200

    except Exception as e:
        log.error(f"Update client failed: {e}", exc_info=True)
        return _json_error("update_client_failed", 500, details=str(e))


# ===================================================================
# REPORTING (Orders, Positions, Audit)
# ===================================================================

@admin_bp.get("/report/orders")
@require_admin_key
def report_orders():
    """
    List orders (for specific client or all)
    Query params: ?client_id=default_v3&status=FILLED&limit=100
    """
    try:
        client_id = request.args.get("client_id")
        status = request.args.get("status")
        limit = _as_int(request.args.get("limit", "200"), 200)

        orders = list_orders(client_id=client_id, limit=limit, status=status)

        return jsonify({"ok": True, "count": len(orders), "orders": orders}), 200

    except Exception as e:
        log.error(f"Report orders failed: {e}", exc_info=True)
        return _json_error("report_orders_failed", 500, details=str(e))


@admin_bp.get("/report/positions")
@require_admin_key
def report_positions():
    """
    List positions (for specific client or all)
    Query params: ?client_id=default_v3&status=OPEN&limit=100
    """
    try:
        client_id = request.args.get("client_id")
        status = request.args.get("status", "OPEN")
        limit = _as_int(request.args.get("limit", "200"), 200)

        positions = list_positions(client_id=client_id, limit=limit, status=status)

        return jsonify({"ok": True, "count": len(positions), "positions": positions}), 200

    except Exception as e:
        log.error(f"Report positions failed: {e}", exc_info=True)
        return _json_error("report_positions_failed", 500, details=str(e))


@admin_bp.get("/report/audit")
@require_admin_key
def report_audit():
    """
    List audit log (for specific client or all)
    Query params: ?client_id=default_v3&limit=200
    """
    try:
        client_id = request.args.get("client_id")
        limit = _as_int(request.args.get("limit", "200"), 200)

        events = list_audit(client_id=client_id, limit=limit)

        return jsonify({"ok": True, "count": len(events), "events": events}), 200

    except Exception as e:
        log.error(f"Report audit failed: {e}", exc_info=True)
        return _json_error("report_audit_failed", 500, details=str(e))


@admin_bp.get("/report/summary")
@require_admin_key
def report_summary():
    """
    Summary stats (all clients or specific)
    Query params: ?client_id=default_v3
    """
    try:
        client_id = request.args.get("client_id")

        with conn() as c:
            if client_id:
                order_stats = run_with_retry(lambda: c.execute(
                    "SELECT status, COUNT(*) as count FROM orders WHERE client_id=? GROUP BY status",
                    (client_id,),
                ).fetchall())

                pos_stats = run_with_retry(lambda: c.execute(
                    "SELECT status, COUNT(*) as count FROM positions WHERE client_id=? GROUP BY status",
                    (client_id,),
                ).fetchall())

                queue_stats = run_with_retry(lambda: c.execute(
                    "SELECT status, COUNT(*) as count FROM trade_queue WHERE client_id=? GROUP BY status",
                    (client_id,),
                ).fetchall())
            else:
                order_stats = run_with_retry(lambda: c.execute(
                    "SELECT status, COUNT(*) as count FROM orders GROUP BY status"
                ).fetchall())

                pos_stats = run_with_retry(lambda: c.execute(
                    "SELECT status, COUNT(*) as count FROM positions GROUP BY status"
                ).fetchall())

                queue_stats = run_with_retry(lambda: c.execute(
                    "SELECT status, COUNT(*) as count FROM trade_queue GROUP BY status"
                ).fetchall())

        return jsonify({
            "ok": True,
            "client_id": client_id,
            "orders": {row["status"]: row["count"] for row in order_stats},
            "positions": {row["status"]: row["count"] for row in pos_stats},
            "queue": {row["status"]: row["count"] for row in queue_stats},
        }), 200

    except Exception as e:
        log.error(f"Report summary failed: {e}", exc_info=True)
        return _json_error("report_summary_failed", 500, details=str(e))


# ===================================================================
# MONITORING (Live positions, open orders)
# ===================================================================

@admin_bp.get("/monitor/positions")
@require_admin_key
def monitor_positions():
    """
    Monitor active positions (for specific client or all).
    Query params: ?client_id=default_v3

    P0-PARTIAL-CLOSE: query expanded to include PARTIAL/ACTIVE statuses and a
    quantity_remaining>0 safety guard so broker exposure is never hidden by an
    incorrect CLOSED status.  Display fields normalized:
      client_email    = positions.client_id  (client_id stores the email)
      option_contract = COALESCE(option_symbol, contract)
    """
    try:
        client_id = request.args.get("client_id")

        # P0-PARTIAL-CLOSE active-exposure query:
        # Include any row whose status is managed OR has remaining contracts.
        _ACTIVE_STATUS_CLAUSE = (
            "UPPER(COALESCE(status,'')) IN ('OPEN','CLOSING','PARTIAL','ACTIVE') "
            "OR COALESCE(quantity_remaining, 0) > 0"
        )

        with conn() as c:
            if client_id:
                rows = run_with_retry(lambda: c.execute(
                    f"SELECT *, client_id AS client_email, "
                    f"COALESCE(option_symbol, contract) AS option_contract "
                    f"FROM positions WHERE client_id=? AND ({_ACTIVE_STATUS_CLAUSE}) "
                    f"ORDER BY entry_ts DESC",
                    (client_id,),
                ).fetchall())
            else:
                rows = run_with_retry(lambda: c.execute(
                    f"SELECT *, client_id AS client_email, "
                    f"COALESCE(option_symbol, contract) AS option_contract "
                    f"FROM positions WHERE ({_ACTIVE_STATUS_CLAUSE}) "
                    f"ORDER BY entry_ts DESC"
                ).fetchall())

        positions = [dict(r) for r in rows]
        return jsonify({"ok": True, "client_id": client_id, "count": len(positions), "positions": positions}), 200

    except Exception as e:
        log.error(f"Monitor positions failed: {e}", exc_info=True)
        return _json_error("monitor_positions_failed", 500, details=str(e))


@admin_bp.get("/monitor/orders")
@require_admin_key
def monitor_orders():
    """
    Monitor open orders (for specific client or all)
    Query params: ?client_id=default_v3
    """
    try:
        client_id = request.args.get("client_id")
        open_orders = get_open_orders_for_reconcile(client_id=client_id, limit=200)

        return jsonify({"ok": True, "client_id": client_id, "count": len(open_orders), "orders": open_orders}), 200

    except Exception as e:
        log.error(f"Monitor orders failed: {e}", exc_info=True)
        return _json_error("monitor_orders_failed", 500, details=str(e))


# ===================================================================
# CONTROL (Manual interventions)
# ===================================================================

@admin_bp.post("/control/force_exit/<position_id>")
@require_admin_key
def force_exit_position(position_id):
    """Force exit a position (mark as CLOSING)"""
    try:
        client_id = request.args.get("client_id")

        with conn() as c:
            # P0-PARTIAL-CLOSE: include PARTIAL/ACTIVE and qty_remaining guard
            _ACTIVE_CLAUSE_ID = (
                "UPPER(COALESCE(status,'')) IN ('OPEN','CLOSING','PARTIAL','ACTIVE') "
                "OR COALESCE(quantity_remaining, 0) > 0"
            )
            if client_id:
                pos_row = run_with_retry(lambda: c.execute(
                    f"SELECT * FROM positions WHERE id=? AND client_id=? AND ({_ACTIVE_CLAUSE_ID})",
                    (position_id, client_id),
                ).fetchone())
            else:
                pos_row = run_with_retry(lambda: c.execute(
                    f"SELECT * FROM positions WHERE id=? AND ({_ACTIVE_CLAUSE_ID})",
                    (position_id,),
                ).fetchone())

            if not pos_row:
                return _json_error("position_not_found", 404)

            pos = dict(pos_row)

            run_with_retry(lambda: c.execute(
                "UPDATE positions SET status='CLOSING', exit_reason='MANUAL_EXIT' WHERE id=?",
                (position_id,),
            ))

        log.warning(f"🚨 Manual exit: position={position_id} {pos.get('contract')}")
        return jsonify({"ok": True, "position_id": position_id, "contract": pos.get("contract"), "status": "marked_for_exit"}), 200

    except Exception as e:
        log.error(f"Force exit failed: {e}", exc_info=True)
        return _json_error("force_exit_failed", 500, details=str(e))


@admin_bp.post("/control/flatten_all")
@require_admin_key
def flatten_all_positions():
    """Force close all open positions for a client"""
    try:
        client_id = request.args.get("client_id")
        if not client_id:
            return _json_error("client_id_required", 400)

        with conn() as c:
            # P0-PARTIAL-CLOSE: include all live statuses and qty_remaining guard
            rows = run_with_retry(lambda: c.execute(
                "SELECT * FROM positions WHERE client_id=? AND ("
                "UPPER(COALESCE(status,'')) IN ('OPEN','CLOSING','PARTIAL','ACTIVE') "
                "OR COALESCE(quantity_remaining, 0) > 0)",
                (client_id,),
            ).fetchall())

            positions = [dict(r) for r in rows]
            if not positions:
                return jsonify({"ok": True, "message": "no_open_positions", "closed": 0}), 200

            run_with_retry(lambda: c.execute(
                "UPDATE positions SET status='CLOSING', exit_reason='FLATTEN_ALL' WHERE client_id=? AND status='OPEN'",
                (client_id,),
            ))

        log.warning(f"🚨 FLATTEN ALL: client={client_id} closing {len(positions)} positions")
        return jsonify({"ok": True, "client_id": client_id, "closed": len(positions), "positions": [p["id"] for p in positions]}), 200

    except Exception as e:
        log.error(f"Flatten all failed: {e}", exc_info=True)
        return _json_error("flatten_all_failed", 500, details=str(e))


@admin_bp.post("/control/reset_equity")
@require_admin_key
def reset_equity():
    """Reset equity tracking for a client"""
    try:
        client_id = request.args.get("client_id")
        if not client_id:
            return _json_error("client_id_required", 400)

        client = get_client(client_id)
        initial_equity = _as_float(client.get("initial_equity", 0), 0)

        if initial_equity <= 0:
            return _json_error("invalid_initial_equity", 400)

        update_client_state(client_id, {
            "current_equity": initial_equity,
            "starting_equity_today": initial_equity,
            "realized_pnl_today": 0.0,
            "trades_taken_today": 0,
            "daily_stop_hit": 0,
        })

        log.info(f"Reset equity for {client_id}: ${initial_equity}")
        return jsonify({"ok": True, "client_id": client_id, "equity": initial_equity}), 200

    except Exception as e:
        log.error(f"Reset equity failed: {e}", exc_info=True)
        return _json_error("reset_equity_failed", 500, details=str(e))


# ===================================================================
# KILL SWITCH PER CLIENT
# ===================================================================

@admin_bp.post("/control/kill_switch/<client_id>")
@require_admin_key
def kill_switch_client(client_id):
    """Enable/disable kill switch for a client"""
    try:
        body = request.get_json(force=True) or {}
        enabled = bool(body.get("enabled", True))

        update_client_state(client_id, {
            "kill_switch": 1 if enabled else 0,
            "mode": "READ_ONLY" if enabled else "PAPER",
        })

        log.warning(f"Kill switch {'ON' if enabled else 'OFF'} for {client_id}")
        return jsonify({"ok": True, "client_id": client_id, "kill_switch": enabled}), 200

    except Exception as e:
        log.error(f"Kill switch failed: {e}", exc_info=True)
        return _json_error("kill_switch_failed", 500, details=str(e))


# ===================================================================
# GROWTH/RENTAL STATUS (optional)
# ===================================================================

@admin_bp.get("/clients/<client_id>/growth")
@require_admin_key
def get_client_growth(client_id):
    """Get growth/profit tracking for a client (if using fee-rental model)"""
    try:
        from ap.account_growth import get_growth_metrics, check_growth_status

        metrics = get_growth_metrics(client_id)
        status = check_growth_status(client_id)

        return jsonify({"ok": True, "client_id": client_id, "metrics": metrics, "status": status}), 200

    except Exception as e:
        log.error(f"Get growth failed: {e}", exc_info=True)
        return _json_error("get_growth_failed", 500, details=str(e))

@admin_bp.post("/reconcile/<client_id>")
def reconcile_client(client_id):
    """
    Reconcile broker positions vs DB positions for a client.
    Finds mismatches between what Tradier shows and what DB has.
    Call this at EOD or after any suspected state drift.
    """
    try:
        from ap.reconcile import run_reconciliation
        result = run_reconciliation(client_id)
        return jsonify({"ok": True, "client_id": client_id, "result": result}), 200
    except Exception as e:
        log.error(f"Reconcile error for {client_id}: {e}")
        return _json_error(f"reconcile_failed: {e}", 500)


@admin_bp.get("/health/clients")
def client_health_check():
    """
    Returns health status of all active client runners.
    Shows which clients are running, stalled, or dead.
    """
    try:
        from ap.db import run_with_retry, conn
        def _fn():
            with conn() as c:
                c.execute("""
                    SELECT client_id, component, status, last_heartbeat_at,
                           restarts, last_error
                    FROM client_health
                    ORDER BY client_id, component
                """)
                return c.fetchall()
        rows = run_with_retry(_fn)
        return jsonify({"ok": True, "clients": rows}), 200
    except Exception as e:
        return _json_error(f"health_check_failed: {e}", 500)

