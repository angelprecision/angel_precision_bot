# ap/admin_api.py
from flask import Blueprint, jsonify, request
from ap.auth import require_admin_auth
from ap.client_manager import create_new_client, list_all_clients, update_client_status, regenerate_api_key
from ap.logger import get_logger

log = get_logger("ap.admin_api")

admin_bp = Blueprint("admin", __name__, url_prefix="/admin")


@admin_bp.post("/clients")
@require_admin_auth
def create_client_endpoint():
    try:
        body = request.get_json(force=True) or {}
        required = ["name", "tradier_account_id", "tradier_access_token"]
        for f in required:
            if not body.get(f):
                return jsonify({"ok": False, "error": f"missing_field:{f}"}), 400

        client = create_new_client(
            name=body["name"],
            tradier_account_id=body["tradier_account_id"],
            tradier_access_token=body["tradier_access_token"],
            tradier_base_url=body.get("tradier_base_url", "https://sandbox.tradier.com"),
            initial_equity=float(body.get("initial_equity", 100000.0)),
            max_trades_per_day=int(body.get("max_trades_per_day", 5)),
            max_concurrent_positions=int(body.get("max_concurrent_positions", 3)),
            daily_max_loss_pct=float(body.get("daily_max_loss_pct", 0.05)),
            base_position_pct=float(body.get("base_position_pct", 0.10)),
        )

        log.info(f"Admin created client: {client['client_id']}")
        return jsonify({"ok": True, "client": client}), 201

    except Exception as e:
        log.error(f"Create client failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@admin_bp.get("/clients")
@require_admin_auth
def list_clients_endpoint():
    try:
        status = request.args.get("status")
        clients = list_all_clients(status)
        return jsonify({"ok": True, "count": len(clients), "clients": clients})
    except Exception as e:
        log.error(f"List clients failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@admin_bp.patch("/clients/<client_id>/status")
@require_admin_auth
def update_client_status_endpoint(client_id):
    try:
        body = request.get_json(force=True) or {}
        status = body.get("status")
        if not status:
            return jsonify({"ok": False, "error": "missing_status"}), 400
        update_client_status(client_id, status)
        return jsonify({"ok": True, "client_id": client_id, "status": status})
    except Exception as e:
        log.error(f"Update client status failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@admin_bp.post("/clients/<client_id>/regenerate-key")
@require_admin_auth
def regenerate_key_endpoint(client_id):
    try:
        new_key = regenerate_api_key(client_id)
        return jsonify({"ok": True, "client_id": client_id, "new_api_key": new_key})
    except Exception as e:
        log.error(f"Regenerate key failed: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500
