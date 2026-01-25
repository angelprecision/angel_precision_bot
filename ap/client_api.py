# ap/client_api.py
"""
Client-facing API endpoints
These are called by clients using their API keys
"""
from flask import Blueprint, jsonify, request
from ap.auth import require_client_auth
from ap.db import conn, run_with_retry, get_client_state
from ap.logger import get_logger

log = get_logger("ap.client_api")

# Create blueprint
client_bp = Blueprint("client", __name__, url_prefix="/client")


@client_bp.get("/me")
@require_client_auth
def get_my_info(client_info):
    """
    Get current client info (without sensitive data)
    """
    try:
        from ap.db import get_client
        
        client = get_client(client_info["client_id"])
        state = get_client_state(client_info["client_id"])
        
        return jsonify({
            "ok": True,
            "client": {
                "client_id": client["client_id"],
                "name": client["name"],
                "status": client["status"],
                "broker_type": client["broker_type"],
                "broker_account_id": client["broker_account_id"],
                "initial_equity": client["initial_equity"],
                "max_trades_per_day": client["max_trades_per_day"],
                "max_concurrent_positions": client["max_concurrent_positions"]
            },
            "state": {
                "current_equity": state["current_equity"],
                "starting_equity_today": state["starting_equity_today"],
                "realized_pnl_today": state["realized_pnl_today"],
                "trades_taken_today": state["trades_taken_today"],
                "mode": state["mode"],
                "kill_switch": bool(state["kill_switch"])
            }
        })
    except Exception as e:
        log.error(f"Get client info failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@client_bp.get("/me/positions")
@require_client_auth
def get_my_positions(client_info):
    """
    Get positions for authenticated client only
    """
    try:
        status = request.args.get("status", "OPEN").upper()
        limit = int(request.args.get("limit", "100"))
        
        client_id = client_info["client_id"]
        
        with conn() as c:
            if status == "ALL":
                rows = run_with_retry(lambda: c.execute("""
                    SELECT * FROM positions
                    WHERE client_id=?
                    ORDER BY entry_ts DESC
                    LIMIT ?
                """, (client_id, limit)).fetchall())
            else:
                rows = run_with_retry(lambda: c.execute("""
                    SELECT * FROM positions
                    WHERE client_id=? AND status=?
                    ORDER BY entry_ts DESC
                    LIMIT ?
                """, (client_id, status, limit)).fetchall())
        
        positions = [dict(r) for r in rows]
        
        return jsonify({
            "ok": True,
            "count": len(positions),
            "positions": positions
        })
        
    except Exception as e:
        log.error(f"Get positions failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@client_bp.get("/me/orders")
@require_client_auth
def get_my_orders(client_info):
    """
    Get orders for authenticated client only
    """
    try:
        status = request.args.get("status")
        limit = int(request.args.get("limit", "100"))
        
        client_id = client_info["client_id"]
        
        with conn() as c:
            if status:
                rows = run_with_retry(lambda: c.execute("""
                    SELECT * FROM orders
                    WHERE client_id=? AND status=?
                    ORDER BY created_ts DESC
                    LIMIT ?
                """, (client_id, status, limit)).fetchall())
            else:
                rows = run_with_retry(lambda: c.execute("""
                    SELECT * FROM orders
                    WHERE client_id=?
                    ORDER BY created_ts DESC
                    LIMIT ?
                """, (client_id, limit)).fetchall())
        
        orders = [dict(r) for r in rows]
        
        return jsonify({
            "ok": True,
            "count": len(orders),
            "orders": orders
        })
        
    except Exception as e:
        log.error(f"Get orders failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@client_bp.get("/me/performance")
@require_client_auth
def get_my_performance(client_info):
    """
    Get performance metrics for authenticated client
    """
    try:
        client_id = client_info["client_id"]
        
        with conn() as c:
            # Get all closed positions
            closed = run_with_retry(lambda: c.execute("""
                SELECT * FROM positions
                WHERE client_id=? AND status='CLOSED'
                ORDER BY exit_ts DESC
            """, (client_id,)).fetchall())
            
            # Calculate metrics
            total_trades = len(closed)
            winning_trades = sum(1 for p in closed if (p["realized_pnl"] or 0) > 0)
            losing_trades = sum(1 for p in closed if (p["realized_pnl"] or 0) < 0)
            
            total_pnl = sum(p["realized_pnl"] or 0 for p in closed)
            win_pnl = sum(p["realized_pnl"] for p in closed if (p["realized_pnl"] or 0) > 0)
            loss_pnl = sum(p["realized_pnl"] for p in closed if (p["realized_pnl"] or 0) < 0)
            
            win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
            avg_win = (win_pnl / winning_trades) if winning_trades > 0 else 0
            avg_loss = (loss_pnl / losing_trades) if losing_trades > 0 else 0
            
            # Get state
            state = get_client_state(client_id)
        
        return jsonify({
            "ok": True,
            "performance": {
                "total_trades": total_trades,
                "winning_trades": winning_trades,
                "losing_trades": losing_trades,
                "win_rate": round(win_rate, 2),
                "total_pnl": round(total_pnl, 2),
                "avg_win": round(avg_win, 2),
                "avg_loss": round(avg_loss, 2),
                "current_equity": state["current_equity"],
                "realized_pnl_today": state["realized_pnl_today"],
                "trades_today": state["trades_taken_today"]
            }
        })
        
    except Exception as e:
        log.error(f"Get performance failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500
    
