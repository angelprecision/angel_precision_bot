# ap/auth.py
"""
Authentication and authorization middleware for API endpoints
"""
import time
import secrets
from functools import wraps
from collections import defaultdict, deque
from flask import request, jsonify
from ap.db import conn, run_with_retry
from ap.logger import get_logger

log = get_logger("ap.auth")

# In-memory rate limiting (per-process)
# TODO: Upgrade to Redis for multi-worker deployments
_RATE_LIMITS = defaultdict(lambda: deque())

# Rate limit: 100 requests per minute per API key
RATE_LIMIT_REQUESTS = 100
RATE_LIMIT_WINDOW = 60  # seconds


def generate_api_key(prefix: str = "ak") -> str:
    """
    Generate a secure API key
    Format: ak_live_<32_random_chars> or ak_test_<32_random_chars>
    """
    random_part = secrets.token_urlsafe(24)  # 32 chars when base64 encoded
    return f"{prefix}_live_{random_part}"


def validate_api_key(api_key: str) -> dict | None:
    """
    Validate API key and return client info
    Returns: {"client_id": "...", "name": "...", "status": "..."} or None
    """
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
            
            # Check if client is active
            if client["status"] != "ACTIVE":
                log.warning(f"Inactive client attempted access: {client['client_id']}")
                return None
            
            return client
            
    except Exception as e:
        log.error(f"API key validation error: {e}")
        return None


def check_rate_limit(api_key: str) -> bool:
    """
    Check if API key has exceeded rate limit
    Returns: True if rate limited, False if ok
    """
    now = time.time()
    
    # Clean old entries
    queue = _RATE_LIMITS[api_key]
    while queue and (now - queue[0] > RATE_LIMIT_WINDOW):
        queue.popleft()
    
    # Check limit
    if len(queue) >= RATE_LIMIT_REQUESTS:
        return True
    
    # Record this request
    queue.append(now)
    return False


def require_client_auth(f):
    """
    Decorator for endpoints requiring client authentication
    Validates X-API-Key header and injects client_info into kwargs
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        api_key = request.headers.get("X-API-Key", "").strip()
        
        if not api_key:
            return jsonify({
                "ok": False,
                "error": "missing_api_key",
                "message": "X-API-Key header required"
            }), 401
        
        # Validate API key
        client_info = validate_api_key(api_key)
        if not client_info:
            return jsonify({
                "ok": False,
                "error": "invalid_api_key"
            }), 401
        
        # Check rate limit
        if check_rate_limit(api_key):
            return jsonify({
                "ok": False,
                "error": "rate_limited",
                "message": f"Rate limit: {RATE_LIMIT_REQUESTS} requests per minute"
            }), 429
        
        # Inject client_info into kwargs
        kwargs["client_info"] = client_info
        
        return f(*args, **kwargs)
    
    return decorated_function


def require_admin_auth(f):
    """
    Decorator for admin endpoints requiring master admin key
    Validates X-Admin-Key header
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        import os
        
        admin_key = request.headers.get("X-Admin-Key", "").strip()
        expected_key = os.getenv("ADMIN_API_KEY", "").strip()
        
        if not expected_key:
            log.error("ADMIN_API_KEY not set in environment!")
            return jsonify({
                "ok": False,
                "error": "admin_auth_not_configured"
            }), 500
        
        if not admin_key or admin_key != expected_key:
            log.warning(f"Invalid admin auth attempt from {request.remote_addr}")
            return jsonify({
                "ok": False,
                "error": "invalid_admin_key"
            }), 401
        
        return f(*args, **kwargs)
    
    return decorated_function
