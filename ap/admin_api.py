# ap/admin_api.py - COMPLETE ADMIN API
# Client management + reporting + monitoring + control
# Multi-client ready, drop-in replacement

from ap.crypto import encrypt_token
from flask import Blueprint, jsonify, request
from ap.logger import get_logger
from ap.db import (
    conn, run_with_retry, get_client, get_all_clients, create_client, update_client,
    get_client_state, update_client_state,
    list_orders, list_positions, list_audit,
    get_open_orders_for_reconcile
)
from ap.utils import json_dumps

log = get_logger("ap.admin_api")

# Create blueprint
admin_bp = Blueprint("admin", __name__, url_prefix="/admin")


# ===================================================================
# SECURITY: Simple admin key check (can replace with require_admin_auth)
# ===================================================================

def _check_admin_key(req):
    """Check X-Admin-Key header"""
    import os
    admin_key = os.getenv("ADMIN_KEY", "").strip()
    if not admin_key:
        return True  # No key set, allow all (dev mode)
    
    provided = (req.headers.get("X-Admin-Key", "") or "").strip()
    return provided == admin_key


def require_admin_key(fn):
    """Decorator: require admin key"""
    from functools import wraps
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not _check_admin_key(request):
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        return fn(*args, **kwargs)
    return wrapper


# ===================================================================
# CLIENT MANAGEMENT
# ===================================================================

@admin_bp.post("/clients")
@require_admin_key
def create_client_endpoint():
    """
    Create a new client
    
    Body:
    {
        "client_id": "john@example.com",
        "name": "John Smith",
        "broker_type": "tradier",
        "broker_account_id": "VA12345678",
        "broker_token": "abc123...",
        "broker_base_url": "https://sandbox.tradier.com",
        "initial_equity": 100000.0,
        "max_trades_per_day": 5,
        "max_concurrent_positions": 3
    }
    """
    try:
        body = request.get_json(force=True) or {}
        
        required = ["client_id", "name", "broker_type", "broker_account_id", "broker_token", "broker_base_url"]
        for field in required:
            if not body.get(field):
                return jsonify({"ok": False, "error": f"missing_field: {field}"}), 400
        
        client = create_client(
            client_id=body["client_id"],
            name=body["name"],
            broker_token=encrypt_token(body["broker_token"]),
            broker_account_id=body["broker_account_id"],
            broker_base_url=body["broker_base_url"],
            initial_equity=float(body.get("initial_equity", 100000.0)),
            max_trades_per_day=int(body.get("max_trades_per_day", 5)),
            max_concurrent_positions=int(body.get("max_concurrent_positions", 3)),
            daily_max_loss_pct=float(body.get("daily_max_loss_pct", 0.05)),
            base_position_pct=float(body.get("base_position_pct", 0.10))
        )
        
        log.info(f"Admin created client: {client['client_id']}")
        
        return jsonify({"ok": True, "client": client}), 201
        
    except Exception as e:
        log.error(f"Create client failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@admin_bp.get("/clients")
@require_admin_key
def list_clients_endpoint():
    """List all clients (optionally filtered by status)"""
    try:
        status = request.args.get("status")
        clients = get_all_clients(status)
        
        return jsonify({
            "ok": True,
            "count": len(clients),
            "clients": clients
        })
        
    except Exception as e:
        log.error(f"List clients failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@admin_bp.get("/clients/<client_id>")
@require_admin_key
def get_client_endpoint(client_id):
    """Get a specific client"""
    try:
        client = get_client(client_id)
        state = get_client_state(client_id)
        
        return jsonify({
            "ok": True,
            "client": client,
            "state": state
        })
        
    except Exception as e:
        log.error(f"Get client failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 404


@admin_bp.patch("/clients/<client_id>")
@require_admin_key
def update_client_endpoint(client_id):
    """Update client configuration"""
    try:
        body = request.get_json(force=True) or {}
        
        client = update_client(client_id, **body)
        
        return jsonify({
            "ok": True,
            "client": client
        })
        
    except Exception as e:
        log.error(f"Update client failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


# ===================================================================
# REPORTING (Orders, Positions, Audit)
# ===================================================================

@admin_bp.get("/report/orders")
@require_admin_key
def report_orders():
    """
    List orders (for specific client or all)
    Query params: ?client_id=john@example.com&status=FILLED&limit=100
    """
    try:
        client_id = request.args.get("client_id")  # Optional - filter by client
        status = request.args.get("status")  # Optional - filter by status
        limit = int(request.args.get("limit", "200"))
        
        orders = list_orders(client_id=client_id, limit=limit, status=status)
        
        return jsonify({
            "ok": True,
            "count": len(orders),
            "orders": orders
        })
        
    except Exception as e:
        log.error(f"Report orders failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@admin_bp.get("/report/positions")
@require_admin_key
def report_positions():
    """
    List positions (for specific client or all)
    Query params: ?client_id=john@example.com&status=OPEN&limit=100
    """
    try:
        client_id = request.args.get("client_id")  # Optional - filter by client
        status = request.args.get("status", "OPEN")  # Default: OPEN
        limit = int(request.args.get("limit", "200"))
        
        positions = list_positions(client_id=client_id, limit=limit, status=status)
        
        return jsonify({
            "ok": True,
            "count": len(positions),
            "positions": positions
        })
        
    except Exception as e:
        log.error(f"Report positions failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@admin_bp.get("/report/audit")
@require_admin_key
def report_audit():
    """
    List audit log (for specific client or all)
    Query params: ?client_id=john@example.com&limit=200
    """
    try:
        client_id = request.args.get("client_id")  # Optional - filter by client
        limit = int(request.args.get("limit", "200"))
        
        events = list_audit(client_id=client_id, limit=limit)
        
        return jsonify({
            "ok": True,
            "count": len(events),
            "events": events
        })
        
    except Exception as e:
        log.error(f"Report audit failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@admin_bp.get("/report/summary")
@require_admin_key
def report_summary():
    """
    Get summary statistics (all clients or specific)
    Query params: ?client_id=john@example.com
    """
    try:
        client_id = request.args.get("client_id")
        
        with conn() as c:
            if client_id:
                # Single client summary
                order_stats = run_with_retry(lambda: c.execute(
                    "SELECT status, COUNT(*) as count FROM orders WHERE client_id=? GROUP BY status",
                    (client_id,)
                ).fetchall())
                
                pos_stats = run_with_retry(lambda: c.execute(
                    "SELECT status, COUNT(*) as count FROM positions WHERE client_id=? GROUP BY status",
                    (client_id,)
                ).fetchall())
                
                queue_stats = run_with_retry(lambda: c.execute(
                    "SELECT status, COUNT(*) as count FROM trade_queue WHERE client_id=? GROUP BY status",
                    (client_id,)
                ).fetchall())
            else:
                # All clients summary
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
        })
        
    except Exception as e:
        log.error(f"Report summary failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


# ===================================================================
# MONITORING (Live position pricing, open orders)
# ===================================================================

@admin_bp.get("/monitor/positions")
@require_admin_key
def monitor_positions():
    """
    Monitor open positions (for specific client or all)
    Shows current status and any errors
    Query params: ?client_id=john@example.com
    """
    try:
        client_id = request.args.get("client_id")
        
        with conn() as c:
            if client_id:
                rows = run_with_retry(lambda: c.execute(
                    "SELECT * FROM positions WHERE client_id=? AND status IN ('OPEN', 'CLOSING') ORDER BY entry_ts DESC",
                    (client_id,)
                ).fetchall())
            else:
                rows = run_with_retry(lambda: c.execute(
                    "SELECT * FROM positions WHERE status IN ('OPEN', 'CLOSING') ORDER BY entry_ts DESC"
                ).fetchall())
        
        positions = [dict(r) for r in rows]
        
        return jsonify({
            "ok": True,
            "client_id": client_id,
            "count": len(positions),
            "positions": positions
        })
        
    except Exception as e:
        log.error(f"Monitor positions failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@admin_bp.get("/monitor/orders")
@require_admin_key
def monitor_orders():
    """
    Monitor open orders (for specific client or all)
    Query params: ?client_id=john@example.com
    """
    try:
        client_id = request.args.get("client_id")
        
        open_orders = get_open_orders_for_reconcile(client_id=client_id, limit=200)
        
        return jsonify({
            "ok": True,
            "client_id": client_id,
            "count": len(open_orders),
            "orders": open_orders
        })
        
    except Exception as e:
        log.error(f"Monitor orders failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


# ===================================================================
# CONTROL (Manual interventions)
# ===================================================================

@admin_bp.post("/control/force_exit/<position_id>")
@require_admin_key
def force_exit_position(position_id):
    """Force exit a position (mark as CLOSING)"""
    try:
        client_id = request.args.get("client_id")  # Optional - verify ownership
        
        with conn() as c:
            if client_id:
                pos_row = run_with_retry(lambda: c.execute(
                    "SELECT * FROM positions WHERE id=? AND client_id=? AND status IN ('OPEN', 'CLOSING')",
                    (position_id, client_id)
                ).fetchone())
            else:
                pos_row = run_with_retry(lambda: c.execute(
                    "SELECT * FROM positions WHERE id=? AND status IN ('OPEN', 'CLOSING')",
                    (position_id,)
                ).fetchone())
            
            if not pos_row:
                return jsonify({"ok": False, "error": "position_not_found"}), 404
            
            pos = dict(pos_row)
            
            # Mark position for exit
            run_with_retry(lambda: c.execute(
                "UPDATE positions SET status='CLOSING', exit_reason='MANUAL_EXIT' WHERE id=?",
                (position_id,)
            ))
        
        log.warning(f"🚨 Manual exit: position={position_id} {pos['contract']}")
        
        return jsonify({
            "ok": True,
            "position_id": position_id,
            "contract": pos["contract"],
            "status": "marked_for_exit"
        })
        
    except Exception as e:
        log.error(f"Force exit failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@admin_bp.post("/control/flatten_all")
@require_admin_key
def flatten_all_positions():
    """Force close all open positions for a client"""
    try:
        client_id = request.args.get("client_id")
        if not client_id:
            return jsonify({"ok": False, "error": "client_id_required"}), 400
        
        with conn() as c:
            rows = run_with_retry(lambda: c.execute(
                "SELECT * FROM positions WHERE client_id=? AND status='OPEN'",
                (client_id,)
            ).fetchall())
            
            positions = [dict(r) for r in rows]
            if not positions:
                return jsonify({"ok": True, "message": "no_open_positions", "closed": 0})
            
            # Mark all for exit
            run_with_retry(lambda: c.execute(
                "UPDATE positions SET status='CLOSING', exit_reason='FLATTEN_ALL' WHERE client_id=? AND status='OPEN'",
                (client_id,)
            ))
        
        log.warning(f"🚨 FLATTEN ALL: client={client_id} closing {len(positions)} positions")
        
        return jsonify({
            "ok": True,
            "client_id": client_id,
            "closed": len(positions),
            "positions": [p["id"] for p in positions]
        })
        
    except Exception as e:
        log.error(f"Flatten all failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@admin_bp.post("/control/reset_equity")
@require_admin_key
def reset_equity():
    """Reset equity tracking for a client"""
    try:
        client_id = request.args.get("client_id")
        if not client_id:
            return jsonify({"ok": False, "error": "client_id_required"}), 400
        
        client = get_client(client_id)
        initial_equity = float(client.get("initial_equity", 0))
        
        if initial_equity <= 0:
            return jsonify({"ok": False, "error": "invalid_initial_equity"}), 400
        
        update_client_state(client_id, {
            "current_equity": initial_equity,
            "starting_equity_today": initial_equity,
            "realized_pnl_today": 0.0,
            "trades_taken_today": 0,
            "daily_stop_hit": 0
        })
        
        log.info(f"Reset equity for {client_id}: ${initial_equity}")
        
        return jsonify({
            "ok": True,
            "client_id": client_id,
            "equity": initial_equity
        })
        
    except Exception as e:
        log.error(f"Reset equity failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


# ===================================================================
# KILL SWITCH PER CLIENT
# ===================================================================

@admin_bp.post("/control/kill_switch/<client_id>")
@require_admin_key
def kill_switch_client(client_id):
    """Enable/disable kill switch for a client"""
    try:
        body = request.get_json(force=True) or {}
        enabled = body.get("enabled", True)
        
        update_client_state(client_id, {
            "kill_switch": 1 if enabled else 0,
            "mode": "READ_ONLY" if enabled else "PAPER"
        })
        
        log.warning(f"Kill switch {'ON' if enabled else 'OFF'} for {client_id}")
        
        return jsonify({
            "ok": True,
            "client_id": client_id,
            "kill_switch": enabled
        })
        
    except Exception as e:
        log.error(f"Kill switch failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


# ===================================================================
# GROWTH/RENTAL STATUS (bonus endpoints)
# ===================================================================

@admin_bp.get("/clients/<client_id>/growth")
@require_admin_key
def get_client_growth(client_id):
    """Get growth/profit tracking for a client (if using fee-rental model)"""
    try:
        from ap.account_growth import get_growth_metrics, check_growth_status
        
        metrics = get_growth_metrics(client_id)
        status = check_growth_status(client_id)
        
        return jsonify({
            "ok": True,
            "client_id": client_id,
            "metrics": metrics,
            "status": status
        })
        
    except Exception as e:
        log.error(f"Get growth failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500
