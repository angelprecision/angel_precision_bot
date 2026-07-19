"""Authenticated read/admin API for the frozen daily intelligence feed."""

from __future__ import annotations

from flask import Blueprint, jsonify, request

from ap.auth import require_admin_auth, require_client_auth
from ap.intelligence_daily_rankings import (
    DEFAULT_POLICY_FROZEN_AT,
    fetch_daily_feed,
    fetch_daily_performance,
    freeze_daily_rankings,
    reconcile_official_outcomes,
)


intelligence_bp = Blueprint("intelligence", __name__, url_prefix="/intelligence")


def _mode() -> str:
    mode = str(request.args.get("execution_mode") or request.args.get("mode") or "PAPER").upper()
    if mode not in {"PAPER", "LIVE"}:
        raise ValueError("execution_mode must be PAPER or LIVE")
    return mode


@intelligence_bp.get("/daily-feed")
@require_client_auth
def daily_feed(client_info):
    try:
        from ap.db import conn
        with conn() as cursor:
            result = fetch_daily_feed(
                cursor,
                client_id=str(client_info["client_id"]),
                execution_mode=_mode(),
                session_date=request.args.get("session_date"),
            )
        return jsonify(result)
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400


@intelligence_bp.get("/daily-performance")
@require_client_auth
def daily_performance(client_info):
    try:
        from ap.db import conn
        with conn() as cursor:
            result = fetch_daily_performance(
                cursor,
                client_id=str(client_info["client_id"]),
                session_date=request.args.get("session_date"),
            )
        return jsonify(result)
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400


@intelligence_bp.post("/admin/freeze-daily")
@require_admin_auth
def admin_freeze_daily():
    body = request.get_json(silent=True) or {}
    client_id = str(body.get("client_id") or "").strip()
    if not client_id:
        return jsonify({"ok": False, "error": "client_id is required"}), 400
    try:
        from ap.db import conn
        with conn() as cursor:
            result = freeze_daily_rankings(
                cursor,
                client_id=client_id,
                execution_mode=str(body.get("execution_mode") or "PAPER"),
                session_date=body.get("session_date"),
                policy_version=body.get("policy_version"),
                policy_frozen_at=body.get("policy_frozen_at") or DEFAULT_POLICY_FROZEN_AT,
            )
        return jsonify(result)
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400


@intelligence_bp.post("/admin/reconcile-outcomes")
@require_admin_auth
def admin_reconcile_outcomes():
    body = request.get_json(silent=True) or {}
    client_id = str(body.get("client_id") or "").strip()
    if not client_id:
        return jsonify({"ok": False, "error": "client_id is required"}), 400
    try:
        from ap.db import conn
        with conn() as cursor:
            result = reconcile_official_outcomes(
                cursor,
                client_id=client_id,
                lookback_days=int(body.get("lookback_days") or 10),
            )
        return jsonify(result)
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
