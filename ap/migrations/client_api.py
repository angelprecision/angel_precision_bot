# ap/client_api.py
from flask import Blueprint, jsonify, request
from ap.auth import require_client_auth
from ap.db import conn, run_with_retry, get_client_state, get_client
from ap.logger import get_logger

log = get_logger("ap.client_api")

client_bp = Blueprint("client", __name__, url_prefix="/client")


@client_bp.get("/me")
@require_client_auth
def get_my_info(client_info):
    try:
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
                "broker_base_url": client["broker_base_url"],
                "initial_equity": client["initial_equity"],
                "max_trades_per_day": client["max_trades_per_day"],
                "max_concurrent_positions": client["max_concurrent_positions"],
            },
            "state": {
                "current_equity": state["current_equity"],
                "starting_equity_today": state["starting_equity_today"],
                "realized_pnl_today": state["realized_pnl_today"],
                "trades_taken_today": state["trades_taken_today"],
                "mode": state["mode"],
                "kill_switch": bool(state["kill_switch"]),
            }
        })
    except Exception as e:
        log.error(f"Get client info failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@client_bp.get("/me/positions")
@require_client_auth
def get_my_positions(client_info):
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
                """, (
