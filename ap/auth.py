# ap/auth.py
import time
import secrets
from functools import wraps
from collections import defaultdict, deque
from flask import request, jsonify
from ap.db import conn, run_with_retry
from ap.logger import get_logger

log = get_logger("ap.auth")

_RATE_LIMITS = defaultdict(lambda: deque())
RATE_LIMIT_REQUESTS = 100
RATE_LIMIT_WINDOW = 60  # seconds


def generate_api_key(prefix: str = "ak") -> str:
    random_part = secrets.token_urlsafe(24)
    return f"{prefix}_live_{random_part}"


def validate_api_key(api_key: str) -> dict | None:
    if not api_key or not api_key.startswith("ak_"):
        return None

    try:
        with conn() as c:
            row = run_with_retry(lambda: c.execute(
                "SELECT client_id, name, status FROM clients WHERE api_key=?",
                (api_key,)
            ).fetchone())
            if not row:
                return None

            client = dict(row)
            if client["status"] != "ACTIVE":
                return None
            return client

    except Exception as e:
        log.error(f"API key validation error: {e}")
        return None


def check_rate_limit(key: str) -> bool:
    now = time.time()
    q = _RATE_LIMITS[key]
    while q and (now - q[0] > RATE_LIMIT_WINDOW):
        q.popleft()
    if len(q) >= RATE_LIMIT_REQUESTS:
        return True
    q.append(now)
    return False


def require_client_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        api_key = request.headers.get("X-API-Key", "").strip()
        if not api_key:
            return jsonify({"ok": False, "error": "missing_api_key", "message": "X-API-Key required"}), 401

        client_info = validate_api_key(api_key)
        if not client_info:
            return jsonify({"ok": False, "error": "invalid_api_key"}), 401

        if check_rate_limit(api_key):
            return jsonify({"ok": False, "error": "rate_limited", "message": f"{RATE_LIMIT_REQUESTS}/min"}), 429

        kwargs["client_info"] = client_info
        return f(*args, **kwargs)
    return decorated


def require_admin_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        import os
        admin_key = request.headers.get("X-Admin-Key", "").strip()
        expected = os.getenv("ADMIN_API_KEY", "").strip()
        if not expected:
            return jsonify({"ok": False, "error": "admin_auth_not_configured"}), 500
        if not admin_key or admin_key != expected:
            return jsonify({"ok": False, "error": "invalid_admin_key"}), 401
        return f(*args, **kwargs)
    return decorated
