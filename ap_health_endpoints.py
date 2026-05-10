"""
ap_health_endpoints.py
======================
Flask Blueprint exposing health, lifecycle trace, and kill switch endpoints.

Register in app.py:
    from ap_health_endpoints import health_bp
    app.register_blueprint(health_bp)

Endpoints:
    GET  /health/status          — full organ health snapshot + system state
    GET  /health/compact         — lightweight name→status map
    GET  /health/quotes          — quote authority snapshot (all tracked contracts)
    GET  /health/lifecycle       — lifecycle ledger summary (state counts)
    GET  /health/signal/<id>     — full trace for a specific signal_id
    GET  /health/rejections      — all rejection records (last N)
    POST /health/kill            — trigger local kill switch {"reason": "..."}
    POST /health/reset           — reset local kill switch (manual operator action)
"""
from __future__ import annotations

import logging

from flask import Blueprint, jsonify, request

log = logging.getLogger("ap.health_endpoints")

health_bp = Blueprint("ap_health", __name__, url_prefix="/health")


@health_bp.route("/status")
def status():
    """Full organ health snapshot + kill switch state."""
    from ap_health_registry import HEALTH
    from ap_kill_switch import KILL
    from ap_quote_authority import QUOTES

    return jsonify({
        "system_healthy":  HEALTH.is_system_healthy(),
        "killed":          KILL.is_killed(),
        "kill_reason":     KILL.reason,
        "quote_rejects":   QUOTES.reject_count,
        "unhealthy_critical": HEALTH.unhealthy_critical_organs(),
        "organs":          HEALTH.snapshot(),
    })


@health_bp.route("/compact")
def compact():
    """Lightweight name→status map for dashboard polling."""
    from ap_health_registry import HEALTH
    from ap_kill_switch import KILL
    return jsonify({
        "system_healthy": HEALTH.is_system_healthy(),
        "killed":         KILL.is_killed(),
        "organs":         HEALTH.compact_snapshot(),
    })


@health_bp.route("/quotes")
def quotes():
    """All contracts currently tracked by the quote authority."""
    from ap_quote_authority import QUOTES
    return jsonify(QUOTES.snapshot_all())


@health_bp.route("/lifecycle")
def lifecycle():
    """Lifecycle ledger summary — state counts and rejection counts."""
    from ap_lifecycle import LEDGER
    return jsonify(LEDGER.snapshot())


@health_bp.route("/signal/<signal_id>/trace")
def signal_trace(signal_id: str):
    """Full lifecycle + rejection trace for a specific signal_id."""
    from ap_lifecycle import LEDGER
    entries    = LEDGER.history(signal_id)
    rejections = LEDGER.rejection_history(signal_id)
    current    = LEDGER.current_state(signal_id)
    return jsonify({
        "signal_id":     signal_id,
        "current_state": current.value if current else None,
        "trace": [
            {
                "ts":     e.timestamp_iso,
                "from":   e.from_state,
                "to":     e.to_state,
                "owner":  e.owner,
                "reason": e.reason,
            }
            for e in entries
        ],
        "rejections": [
            {
                "ts":         r.timestamp_iso,
                "category":   r.category,
                "severity":   r.severity,
                "code":       r.reason_code,
                "reason":     r.human_reason,
                "owner":      r.owner,
            }
            for r in rejections
        ],
    })


@health_bp.route("/rejections")
def rejections():
    """All rejection records. Optional ?limit=N query param."""
    from ap_lifecycle import LEDGER
    limit = int(request.args.get("limit", 200))
    all_r = LEDGER.all_rejections()
    recent = all_r[-limit:] if len(all_r) > limit else all_r
    return jsonify({
        "total":      len(all_r),
        "returned":   len(recent),
        "rejections": [
            {
                "ts":       r.timestamp_iso,
                "signal":   r.signal_id,
                "ticker":   r.ticker,
                "category": r.category,
                "severity": r.severity,
                "code":     r.reason_code,
                "reason":   r.human_reason,
                "owner":    r.owner,
            }
            for r in recent
        ],
    })


@health_bp.route("/kill", methods=["POST"])
def kill():
    """Trigger local kill switch. Body: {"reason": "manual operator halt"}"""
    from ap_kill_switch import KILL
    reason = (request.json or {}).get("reason", "manual_operator_halt")
    KILL.kill(reason)
    log.critical("[HEALTH_ENDPOINT] Kill switch triggered via API: %s", reason)
    return jsonify({"killed": True, "reason": reason})


@health_bp.route("/reset", methods=["POST"])
def reset():
    """Reset local kill switch. Requires manual operator action."""
    from ap_kill_switch import KILL
    KILL.reset()
    log.info("[HEALTH_ENDPOINT] Kill switch reset via API")
    return jsonify({"killed": False})
