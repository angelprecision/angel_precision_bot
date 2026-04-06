
# app.py - Angel Precision Hybrid Dashboard Backend (BETA PRODUCTION+)
# Architecture: Supabase Auth + members table (subscriptions) + Trading Bot API (live)
# Adds: trade_fills persistence (Supabase) + /api/user/bootstrap to fix RLS signup failure

import os
import jwt
import requests
from datetime import datetime, timezone, date, timedelta
from functools import wraps
from flask import Flask, request, jsonify
from flask_cors import CORS
from dotenv import load_dotenv
from supabase import create_client, Client
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from cryptography.fernet import Fernet
import base64
import hashlib

load_dotenv()

app = Flask(__name__)

# ============================================================
# CONFIG
# ============================================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")  # service_role (backend only)
TRADING_API_URL = os.getenv("TRADING_API_URL", "https://angel-precision-bot-official-1.onrender.com")
TRADING_API_KEY = os.getenv("TRADING_API_KEY")            # shared bot API key (beta)
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY")                # admin-only endpoints
JWT_SECRET      = os.getenv("JWT_SECRET", "angel-precision-secret-2026")  # signs client session tokens
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY", "")  # SendGrid for transactional email
FROM_EMAIL       = "angel@angelprecision.com"          # verified sender in SendGrid
ZELLE_NAME       = "Aamiyah Wilson"
ZELLE_CONTACT    = "209-486-1334"
DASHBOARD_URL    = "https://www.angelprecision.com/login.html"

# ── Encryption (for Tradier tokens at rest) ──────────────────────────────────
_raw_key = os.getenv("ENCRYPTION_KEY", "angel-precision-encrypt-2026")
# Fernet requires a 32-byte URL-safe base64 key — derive one from the raw string
_key_bytes = hashlib.sha256(_raw_key.encode()).digest()
FERNET_KEY = base64.urlsafe_b64encode(_key_bytes)
_fernet = Fernet(FERNET_KEY)

def encrypt_token(plaintext: str) -> str:
    return _fernet.encrypt(plaintext.encode()).decode()

def decrypt_token(ciphertext: str) -> str:
    return _fernet.decrypt(ciphertext.encode()).decode()

# ============================================================
# EMAIL HELPERS
# ============================================================
def send_email(to_email: str, subject: str, html_body: str) -> bool:
    """Send transactional email via SendGrid. Returns True on success."""
    if not SENDGRID_API_KEY:
        print(f"[EMAIL SKIP] No SENDGRID_API_KEY set. Would send to {to_email}: {subject}")
        return False
    try:
        msg = Mail(
            from_email=FROM_EMAIL,
            to_emails=to_email,
            subject=subject,
            html_content=html_body
        )
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        resp = sg.send(msg)
        print(f"[EMAIL OK] {to_email} — {subject} — status {resp.status_code}")
        return True
    except Exception as e:
        print(f"[EMAIL ERROR] {to_email} — {e}")
        return False


def email_payment_instructions(name: str, to_email: str, tier: str) -> bool:
    """Send Zelle payment instructions after apply form submit."""
    price = "$5,000/month"
    html = f"""
    <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;background:#080b0f;color:#e2eaf5;padding:40px;border-radius:12px;">
      <img src="https://www.angelprecision.com/logo.png" width="120" style="margin-bottom:24px;" />
      <h2 style="color:#00e5a0;font-size:22px;margin-bottom:8px;">Application Received</h2>
      <p style="color:#7a95b0;margin-bottom:24px;">Hi {name}, your application has been received. To secure your spot, please send payment via Zelle.</p>

      <div style="background:#0d1117;border:1px solid #1e2a35;border-radius:8px;padding:24px;margin-bottom:24px;">
        <p style="margin:0 0 8px;font-size:12px;text-transform:uppercase;letter-spacing:0.1em;color:#3d5470;">Payment Details</p>
        <p style="margin:0 0 6px;"><strong>Amount:</strong> {price} ({tier} tier)</p>
        <p style="margin:0 0 6px;"><strong>Zelle Name:</strong> {ZELLE_NAME}</p>
        <p style="margin:0 0 6px;"><strong>Zelle:</strong> {ZELLE_CONTACT}</p>
        <p style="margin:0;font-size:12px;color:#7a95b0;">Please include your email in the Zelle memo so we can match your payment.</p>
      </div>

      <p style="color:#7a95b0;font-size:13px;">Once payment is confirmed, you will receive your access code and dashboard login within 24 hours.</p>
      <p style="color:#3d5470;font-size:11px;margin-top:32px;">Questions? Reply to this email or contact <a href="mailto:angel@angelprecision.com" style="color:#00e5a0;">angel@angelprecision.com</a></p>
    </div>
    """
    return send_email(to_email, "Angel Precision — Payment Instructions", html)


def email_access_granted(name: str, to_email: str, access_code: str, tier: str) -> bool:
    """Send access code + login link when admin approves a member."""
    html = f"""
    <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;background:#080b0f;color:#e2eaf5;padding:40px;border-radius:12px;">
      <h2 style="color:#00e5a0;font-size:22px;margin-bottom:8px;">You're Approved ✓</h2>
      <p style="color:#7a95b0;margin-bottom:24px;">Hi {name}, your Angel Precision account is active. Here are your login credentials.</p>

      <div style="background:#0d1117;border:1px solid #00e5a0;border-radius:8px;padding:24px;margin-bottom:24px;">
        <p style="margin:0 0 8px;font-size:12px;text-transform:uppercase;letter-spacing:0.1em;color:#3d5470;">Your Credentials</p>
        <p style="margin:0 0 6px;"><strong>Email:</strong> {to_email}</p>
        <p style="margin:0 0 6px;"><strong>Access Code:</strong> <span style="color:#00e5a0;font-size:18px;letter-spacing:0.1em;font-family:monospace;">{access_code}</span></p>
        <p style="margin:0 0 6px;"><strong>Tier:</strong> {tier}</p>
      </div>

      <a href="{DASHBOARD_URL}" style="display:inline-block;background:#00e5a0;color:#080b0f;font-weight:700;padding:14px 32px;border-radius:8px;text-decoration:none;font-size:14px;letter-spacing:0.05em;">ACCESS DASHBOARD →</a>

      <p style="color:#3d5470;font-size:11px;margin-top:32px;">Keep your access code private. Questions? <a href="mailto:angel@angelprecision.com" style="color:#00e5a0;">angel@angelprecision.com</a></p>
    </div>
    """
    return send_email(to_email, "Angel Precision — Your Access Code & Dashboard Login", html)


if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY in environment")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

# ============================================================
# CORS (lock to your domain)
# ============================================================
CORS(app, resources={
    r"/*": {
        "origins": [
            "https://www.angelprecision.com",
            "https://angelprecision.com",
            "http://localhost:3000",
            "http://localhost:5173",
        ],
        "methods": ["GET", "POST", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization", "X-Admin-Key"],
    }
})
@app.post("/api/public/apply")
def public_apply():
    data = request.get_json() or {}

    name = (data.get("name") or "").strip()
    email = (data.get("email") or "").strip().lower()
    experience = (data.get("experience") or "").strip()
    tier = (data.get("tier") or "").strip()
    account_size = (data.get("account_size") or "").strip()
    reason = (data.get("reason") or "").strip()

    if not name or not email:
        return jsonify({"ok": False, "error": "missing_fields"}), 400

    # Write application into members as a PENDING row (no approval, no subscription)
    # Only insert columns that actually exist in the members table
    try:
        res = supabase.table("members").upsert([{
            "name":                name,
            "email":               email,
            "tier":                tier or "ENTERPRISE",
            "approved":            False,
            "subscription_active": False,
            "access_code":         None,
            "auto_renew":          False,
            # Store extra fields in a notes/metadata column if it exists, else drop them
        }], on_conflict="email").execute()
    except Exception as e:
        return jsonify({"ok": False, "error": "db_error", "detail": str(e)}), 500

    # Send Zelle payment instructions email
    inserted = res.data[0] if res.data else {}
    email_payment_instructions(
        name=name,
        to_email=email,
        tier=tier or "ENTERPRISE"
    )

    return jsonify({"ok": True, "member": inserted})

# ============================================================
# TIME HELPERS
# ============================================================
def parse_dt(value):
    """
    Accepts:
      - ISO timestamps like '2026-01-27T14:35:00Z'
      - ISO without Z
      - DATE like '2026-01-27'
      - python date/datetime
    Returns aware datetime in UTC, or None.
    """
    if not value:
        return None
    try:
        if isinstance(value, datetime):
            dt = value
        elif isinstance(value, date):
            dt = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
        elif isinstance(value, str):
            s = value.strip()
            # date-only
            if len(s) == 10 and s[4] == "-" and s[7] == "-":
                y, m, d = s.split("-")
                dt = datetime(int(y), int(m), int(d), tzinfo=timezone.utc)
            else:
                s = s.replace("Z", "+00:00")
                dt = datetime.fromisoformat(s)
        else:
            return None

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

# ============================================================
# SUBSCRIPTION CHECK
# ============================================================
def check_subscription_active(member):
    """
    Returns: (is_active: bool, error_message: str | None)
    Enforces BOTH:
      - subscription_active == true
      - subscription_end exists and is in the future
      - approved == true handled elsewhere
    """
    if not member.get("subscription_active", False):
        return False, "Subscription is inactive"

    sub_end = member.get("subscription_end")
    end_dt = parse_dt(sub_end)

    if end_dt:
        now_utc = datetime.now(timezone.utc)
        if now_utc > end_dt:
            # best-effort flip the flag off
            try:
                supabase.table("members").update({
                    "subscription_active": False
                }).eq("id", member["id"]).execute()
            except Exception:
                pass
            return False, f"Subscription expired on {end_dt.strftime('%Y-%m-%d')}"
    # null subscription_end + subscription_active=true = lifetime/manual subscription, allow it

    return True, None

# ============================================================
# AUTH HELPERS + DECORATORS
# ============================================================
def get_user_from_bearer():
    """
    Accept either:
      - AP custom JWT (issued by /api/auth/login) — verified with JWT_SECRET
      - Legacy Supabase JWT — verified via supabase.auth.get_user()
    Returns (user_obj_or_dict, uid, email) or raises.
    """
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise ValueError("Missing authorization token")

    token = auth_header[7:].strip()

    # Try AP custom JWT — decode without verification first to check issuer
    try:
        unverified = jwt.decode(token, options={"verify_signature": False}, algorithms=["HS256"])
    except Exception:
        unverified = {}

    if unverified.get("iss") == "angel-precision":
        # This is our token — verify signature strictly
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
            return payload, payload["sub"], payload["email"]
        except jwt.ExpiredSignatureError:
            raise ValueError("Session expired — please log in again")
        except jwt.InvalidTokenError as e:
            raise ValueError(f"Invalid token — {e}")

    # Fallback: Supabase JWT (admin login only)
    try:
        user_resp = supabase.auth.get_user(token)
        if user_resp and user_resp.user:
            user = user_resp.user
            return user, user.id, user.email
    except Exception:
        pass
    raise ValueError("Invalid or expired token")

def find_or_link_member(uid: str, email: str):
    """
    For AP JWT users, uid is the string row ID (e.g. '5').
    Look up by email first (reliable), then try user_id as UUID fallback.
    Returns member row dict or None.
    """
    # Primary: look up by email
    by_email = supabase.table("members").select("*").eq("email", email).execute()
    if by_email.data:
        return by_email.data[0]

    # Fallback: try user_id match (for legacy Supabase auth sessions)
    try:
        member_res = supabase.table("members").select("*").eq("user_id", uid).execute()
        if member_res.data:
            return member_res.data[0]
    except Exception:
        pass

    return None

def auth_required(f):
    """Require valid Supabase session only (no subscription check)."""
    @wraps(f)
    def decorated(*args, **kwargs):
        try:
            user, uid, email = get_user_from_bearer()
            request.user = user
            request.uid = uid
            request.email = email
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 401
        return f(*args, **kwargs)
    return decorated

def token_required(f):
    """Require valid Supabase session + approved + active subscription."""
    @wraps(f)
    def decorated(*args, **kwargs):
        try:
            user, uid, email = get_user_from_bearer()

            member = find_or_link_member(uid, email)
            if not member:
                return jsonify({"ok": False, "error": "Member not found"}), 404

            if not member.get("approved", False):
                return jsonify({"ok": False, "error": "Account pending approval"}), 403

            is_active, error_msg = check_subscription_active(member)
            if not is_active:
                return jsonify({
                    "ok": False,
                    "error": "subscription_expired",
                    "message": error_msg,
                    "subscription_end": member.get("subscription_end")
                }), 403

            request.user = user
            request.uid = uid
            request.email = email
            request.member = member

        except Exception as e:
            print(f"Auth error: {e}")
            return jsonify({"ok": False, "error": "Authentication failed"}), 401

        return f(*args, **kwargs)
    return decorated

def admin_required(f):
    """Admin-only endpoints."""
    @wraps(f)
    def decorated(*args, **kwargs):
        admin_key = request.headers.get("X-Admin-Key")
        if not admin_key or admin_key != ADMIN_API_KEY:
            return jsonify({"ok": False, "error": "Unauthorized - Admin access required"}), 401
        return f(*args, **kwargs)
    return decorated

# ============================================================
# TRADIER DIRECT HELPERS
# ============================================================
def get_member_tradier(member: dict):
    """
    Returns (account_id, access_token, base_url) for a member, decrypting the token.
    Raises ValueError if the member has no Tradier credentials connected.
    """
    account_id = member.get("tradier_account_id")
    enc_token  = member.get("tradier_access_token")
    base_url   = member.get("tradier_base_url") or "https://sandbox.tradier.com"
    if not account_id or not enc_token:
        raise ValueError("no_tradier")
    return account_id, decrypt_token(enc_token), base_url

def tradier_get(member: dict, path: str, params: dict = None):
    """Make an authenticated GET to this member's Tradier account."""
    account_id, token, base_url = get_member_tradier(member)
    return requests.get(
        f"{base_url}{path}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        params=params or {},
        timeout=10
    ), account_id

# ============================================================
# BOT PROXY HELPERS
# ============================================================
def bot_get(path: str):
    if not TRADING_API_KEY:
        raise RuntimeError("Missing TRADING_API_KEY in environment")
    return requests.get(
        f"{TRADING_API_URL}{path}",
        headers={"X-API-Key": TRADING_API_KEY},
        timeout=10
    )

def bot_post(path: str, payload: dict = None):
    if not TRADING_API_KEY:
        raise RuntimeError("Missing TRADING_API_KEY in environment")
    return requests.post(
        f"{TRADING_API_URL}{path}",
        json=payload or {},
        headers={"X-API-Key": TRADING_API_KEY},
        timeout=10
    )

# ============================================================
# PERSISTENCE (Supabase) - Trade Fills Ledger
# ============================================================
def persist_fills_from_orders(member, orders):
    """
    For beta: treat FILLED/PARTIALLY_FILLED orders as fills.
    Requires Supabase table public.trade_fills with UNIQUE (member_id, order_id)
    """
    if not orders:
        return

    rows = []
    for o in orders:
        status = (o.get("status") or "").upper()
        if status not in ("FILLED", "PARTIALLY_FILLED"):
            continue

        rows.append({
            "member_id": member["id"],
            "order_id": o.get("order_id"),
            "symbol": o.get("symbol"),
            "contract": o.get("contract"),
            "side": o.get("kind"),  # OPEN/CLOSE etc.
            "qty": o.get("qty"),
            "filled_qty": o.get("filled_qty"),
            "avg_fill": o.get("avg_fill"),
            "status": status,
            "created_ts": o.get("created_ts"),
            "source": "tradier",
            "raw": o
        })

    if not rows:
        return

    supabase.table("trade_fills").upsert(rows, on_conflict="member_id,order_id").execute()

# ============================================================
# PUBLIC
# ============================================================
@app.get("/")
def root():
    return jsonify({
        "service": "Angel Precision Dashboard Backend",
        "version": "3.1 - Beta Ledger + Bootstrap",
        "status": "online"
    })

@app.get("/health")
def health():
    return jsonify({
        "ok": True,
        "systems": {
            "supabase_url_set": bool(SUPABASE_URL),
            "supabase_key_set": bool(SUPABASE_SERVICE_KEY),
            "trading_api_url": TRADING_API_URL,
            "trading_api_key_set": bool(TRADING_API_KEY),
        }
    })

# ============================================================
# AUTH ENDPOINT — email + access_code → AP JWT (no OTP/email needed)
# ============================================================
@app.post("/api/auth/login")
def auth_login():
    """
    Client posts { email, access_code }.
    Backend validates against members table.
    Returns a signed JWT valid for 7 days — no email magic link needed.
    """
    data = request.get_json() or {}
    email = (data.get("email") or "").strip().lower()
    code  = (data.get("access_code") or "").strip().upper()

    if not email or not code:
        return jsonify({"ok": False, "error": "missing_fields"}), 400

    # Look up member — must be approved + active + code match
    try:
        res = supabase.table("members") \
            .select("id,email,name,approved,subscription_active,subscription_end,access_code,client_id,tier,auto_renew") \
            .eq("email", email) \
            .eq("access_code", code) \
            .eq("approved", True) \
            .execute()
    except Exception as e:
        return jsonify({"ok": False, "error": "db_error", "detail": str(e)}), 500

    rows = res.data if res else []
    member = rows[0] if rows else None
    if not member:
        return jsonify({"ok": False, "error": "invalid_credentials"}), 401

    # Check subscription active
    if not member.get("subscription_active", False):
        return jsonify({"ok": False, "error": "subscription_inactive"}), 403

    # Issue AP JWT — 7 day expiry
    now = datetime.now(timezone.utc)
    payload = {
        "iss":   "angel-precision",
        "sub":   str(member["id"]),  # JWT spec requires sub to be a string
        "email": member["email"],
        "name":  member.get("name") or "",
        "tier":  member.get("tier") or "standard",
        "client_id": member.get("client_id") or member["id"],
        "iat":   int(now.timestamp()),
        "exp":   int((now + timedelta(days=7)).timestamp()),
    }
    token = jwt.encode(payload, JWT_SECRET, algorithm="HS256")

    return jsonify({
        "ok":    True,
        "token": token,
        "member": {
            "id":           member["id"],
            "email":        member["email"],
            "name":         member.get("name") or "",
            "tier":         member.get("tier") or "standard",
            "client_id":    member.get("client_id") or member["id"],
            "auto_renew":   member.get("auto_renew", False),
            "subscription_end": member.get("subscription_end"),
        }
    })


# ============================================================
# USER ENDPOINTS
# ============================================================
@app.post("/api/user/bootstrap")
@auth_required
def bootstrap_user():
    """
    Fixes: "new row violates row-level security policy for table members"
    Flow:
      1) Frontend signs up / signs in (Supabase Auth)
      2) Frontend calls POST /api/user/bootstrap with Bearer token
      3) Backend creates/links members row using service_role key (bypasses RLS safely)
    """
    user = request.user
    uid = request.uid
    email = request.email

    # If member exists, return it
    existing = supabase.table("members").select("*").eq("user_id", uid).execute()
    if existing.data:
        return jsonify({"ok": True, "created": False, "member": existing.data[0]})

    # If old row exists by email, link it
    by_email = supabase.table("members").select("*").eq("email", email).execute()
    if by_email.data:
        m = by_email.data[0]
        try:
            updated = supabase.table("members").update({"user_id": uid}).eq("id", m["id"]).execute()
            return jsonify({"ok": True, "created": False, "member": updated.data[0]})
        except Exception:
            return jsonify({"ok": True, "created": False, "member": m})

    # Otherwise create new row (default: not approved, no subscription)
    name = None
    try:
        meta = user.user_metadata or {}
        name = meta.get("name")
    except Exception:
        pass
    if not name:
        name = email.split("@")[0]

    payload = {
        "user_id": uid,
        "email": email,
        "name": name,
        "approved": False,
        "subscription_active": False,
        "tier": "ENTERPRISE",
        "created_at": datetime.now(timezone.utc).isoformat()
    }

    created = supabase.table("members").insert(payload).execute()
    return jsonify({"ok": True, "created": True, "member": created.data[0]})

@app.get("/api/user/me")
@auth_required
def user_me():
    """Returns auth user + member (if exists). No subscription gate."""
    uid = request.uid
    email = request.email
    member = find_or_link_member(uid, email)
    return jsonify({
        "ok": True,
        "user": {"id": uid, "email": email},
        "member": member
    })

@app.get("/api/user/profile")
@token_required
def get_user_profile():
    m = request.member
    return jsonify({
        "ok": True,
        "profile": {
            "name": m.get("name"),
            "email": m.get("email"),
            "tier": m.get("tier"),
            "member_since": m.get("created_at"),
            "client_id": m.get("client_id"),
            "user_id": m.get("user_id"),
        }
    })

@app.get("/api/user/subscription")
@token_required
def get_user_subscription():
    m = request.member
    is_active, _ = check_subscription_active(m)
    return jsonify({
        "ok": True,
        "subscription": {
            "active": is_active,
            "tier": m.get("tier"),
            "start_date": m.get("subscription_start"),
            "end_date": m.get("subscription_end"),
            "payment_method": m.get("payment_method"),
            "auto_renew": m.get("auto_renew"),
        }
    })

# ============================================================
# TRADING (LIVE) - Bot API proxy
# ============================================================
@app.get("/api/trading/account")
@token_required
def trading_account():
    """Returns account balances directly from Tradier."""
    member = request.member
    try:
        r, account_id = tradier_get(member, f"/v1/accounts/{member['tradier_account_id']}/balances")
        if r.status_code != 200:
            return jsonify({"ok": False, "error": f"Tradier returned {r.status_code}"}), r.status_code
        balances = r.json().get("balances", {})
        equity = balances.get("equity") or balances.get("total_equity") or 0
        return jsonify({"ok": True, "account_id": account_id, "balances": balances, "current_equity": equity})
    except ValueError:
        return jsonify({"ok": False, "error": "no_tradier", "message": "No Tradier account connected"}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/api/trading/positions")
@token_required
def trading_positions():
    """Returns open positions directly from Tradier."""
    member = request.member
    try:
        account_id, token, base_url = get_member_tradier(member)
        r = requests.get(
            f"{base_url}/v1/accounts/{account_id}/positions",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            timeout=10
        )
        raw = r.json()
        positions_data = raw.get("positions", {})
        # Tradier returns "null" string or dict
        if not positions_data or positions_data == "null":
            positions = []
        else:
            pos = positions_data.get("position", [])
            positions = pos if isinstance(pos, list) else [pos]
        # Normalize to dashboard format
        normalized = []
        for p in positions:
            normalized.append({
                "symbol":       p.get("symbol"),
                "qty":          p.get("quantity"),
                "cost_basis":   p.get("cost_basis"),
                "date_acquired":p.get("date_acquired"),
            })
        return jsonify({"ok": True, "count": len(normalized), "positions": normalized})
    except ValueError:
        return jsonify({"ok": False, "error": "no_tradier", "message": "No Tradier account connected"}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/api/trading/orders")
@token_required
def trading_orders():
    """Returns orders directly from Tradier."""
    member = request.member
    try:
        account_id, token, base_url = get_member_tradier(member)
        r = requests.get(
            f"{base_url}/v1/accounts/{account_id}/orders",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            timeout=10
        )
        raw = r.json()
        orders_data = raw.get("orders", {})
        if not orders_data or orders_data == "null":
            orders = []
        else:
            o = orders_data.get("order", [])
            orders = o if isinstance(o, list) else [o]
        # Normalize to dashboard format
        normalized = []
        for o in orders:
            normalized.append({
                "order_id":   o.get("id"),
                "symbol":     o.get("symbol"),
                "side":       o.get("side"),
                "qty":        o.get("quantity"),
                "filled_qty": o.get("exec_quantity"),
                "avg_fill":   o.get("avg_fill_price"),
                "status":     o.get("status"),
                "type":       o.get("type"),
                "created_at": o.get("transaction_date"),
            })
        return jsonify({"ok": True, "count": len(normalized), "orders": normalized})
    except ValueError:
        return jsonify({"ok": False, "error": "no_tradier", "message": "No Tradier account connected"}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/api/trading/performance")
@token_required
def trading_performance():
    """Builds performance metrics from Tradier balances + order history."""
    member = request.member
    try:
        account_id, token, base_url = get_member_tradier(member)
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

        # Balances for equity
        bal_r = requests.get(f"{base_url}/v1/accounts/{account_id}/balances",
                             headers=headers, timeout=10)
        balances = bal_r.json().get("balances", {}) if bal_r.status_code == 200 else {}
        equity = float(balances.get("equity") or balances.get("total_equity") or 0)

        # Today's P&L from gain_loss
        gl_r = requests.get(f"{base_url}/v1/accounts/{account_id}/gainloss",
                            headers=headers, timeout=10)
        gl_data = gl_r.json().get("gainloss", {}) if gl_r.status_code == 200 else {}
        closed_positions = gl_data.get("closed_position", []) if gl_data else []
        if isinstance(closed_positions, dict):
            closed_positions = [closed_positions]

        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        total_pnl = 0.0
        today_pnl = 0.0
        wins = 0
        losses = 0
        for cp in closed_positions:
            pnl = float(cp.get("gain_loss") or 0)
            total_pnl += pnl
            if pnl > 0: wins += 1
            else: losses += 1
            close_date = (cp.get("close_date") or "")[:10]
            if close_date == today_str:
                today_pnl += pnl

        total_trades = wins + losses
        win_rate = round(wins / total_trades * 100, 2) if total_trades > 0 else 0.0

        return jsonify({
            "ok": True,
            "performance": {
                "current_equity":     equity,
                "total_pnl":          round(total_pnl, 2),
                "realized_pnl_today": round(today_pnl, 2),
                "trades_today":       0,   # Tradier doesn't expose intraday count separately
                "total_trades":       total_trades,
                "winning_trades":     wins,
                "losing_trades":      losses,
                "win_rate":           win_rate,
            }
        })
    except ValueError:
        return jsonify({"ok": False, "error": "no_tradier", "message": "No Tradier account connected"}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# ============================================================
# FILLS + LEDGER (Supabase)
# ============================================================
@app.get("/api/trading/fills")
@token_required
def trading_fills():
    """Persistent "every trade taken" feed from Supabase trade_fills."""
    member = request.member
    try:
        res = supabase.table("trade_fills") \
            .select("order_id,symbol,contract,side,qty,filled_qty,avg_fill,status,created_ts,source") \
            .eq("member_id", member["id"]) \
            .order("created_ts", desc=True) \
            .limit(200) \
            .execute()
        return jsonify({"ok": True, "fills": res.data})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/api/trading/ledger")
@token_required
def trading_ledger():
    """Live orders + persistent fills (audit/debug)."""
    member = request.member
    try:
        live = bot_get("/client/me/orders").json()
        if isinstance(live, dict) and live.get("ok") and isinstance(live.get("orders"), list):
            persist_fills_from_orders(member, live["orders"])

        fills = supabase.table("trade_fills") \
            .select("order_id,symbol,contract,side,qty,filled_qty,avg_fill,status,created_ts,source") \
            .eq("member_id", member["id"]) \
            .order("created_ts", desc=True) \
            .limit(200) \
            .execute()

        return jsonify({
            "ok": True,
            "live_orders": live.get("orders", []) if isinstance(live, dict) else [],
            "fills": fills.data
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# ============================================================
# TRADIER CONNECT (client self-service)
# ============================================================
@app.post("/api/account/connect-tradier")
@auth_required
def connect_tradier():
    """
    Client posts { account_id, access_token, sandbox } from dashboard.
    Token is encrypted before storing in Supabase.
    """
    uid   = request.uid
    email = request.email
    data         = request.get_json() or {}
    account_id   = (data.get("account_id") or "").strip()
    access_token = (data.get("access_token") or "").strip()
    sandbox      = data.get("sandbox", True)  # default sandbox

    if not account_id or not access_token:
        return jsonify({"ok": False, "error": "account_id and access_token required"}), 400

    # Validate against Tradier before saving
    base_url = "https://sandbox.tradier.com" if sandbox else "https://api.tradier.com"
    try:
        verify = requests.get(
            f"{base_url}/v1/accounts/{account_id}/balances",
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
            timeout=8
        )
        if verify.status_code == 401:
            return jsonify({"ok": False, "error": "invalid_credentials",
                            "message": "Tradier rejected these credentials. Check your account ID and token."}), 401
        if verify.status_code not in (200, 201):
            return jsonify({"ok": False, "error": "tradier_error",
                            "message": f"Tradier returned {verify.status_code}"}), 400
    except requests.exceptions.Timeout:
        return jsonify({"ok": False, "error": "timeout",
                        "message": "Tradier did not respond in time. Try again."}), 504
    except Exception as e:
        return jsonify({"ok": False, "error": "network_error", "message": str(e)}), 500

    # Encrypt token before storing
    encrypted = encrypt_token(access_token)

    try:
        supabase.table("members").update({
            "tradier_account_id":   account_id,
            "tradier_access_token": encrypted,
            "tradier_base_url":     base_url,
        }).eq("email", email).execute()
    except Exception as e:
        return jsonify({"ok": False, "error": "db_error", "message": str(e)}), 500

    return jsonify({"ok": True, "message": "Tradier account connected successfully",
                    "sandbox": sandbox, "account_id": account_id})


@app.get("/api/account/tradier-status")
@auth_required
def tradier_status():
    """Returns whether this member has a Tradier account connected."""
    uid   = request.uid
    email = request.email
    try:
        res = supabase.table("members") \
            .select("tradier_account_id,tradier_base_url") \
            .eq("email", email).execute()
        if not res.data:
            return jsonify({"ok": True, "connected": False})
        m = res.data[0]
        connected = bool(m.get("tradier_account_id"))
        return jsonify({
            "ok":        True,
            "connected": connected,
            "account_id": m.get("tradier_account_id") or None,
            "sandbox":    "sandbox" in (m.get("tradier_base_url") or "sandbox"),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.delete("/api/account/disconnect-tradier")
@auth_required
def disconnect_tradier():
    """Remove Tradier credentials for this member."""
    uid   = request.uid
    email = request.email
    try:
        supabase.table("members").update({
            "tradier_account_id":   None,
            "tradier_access_token": None,
        }).eq("email", email).execute()
        return jsonify({"ok": True, "message": "Tradier account disconnected"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ============================================================
# ADMIN (Wire/Zelle workflow)
# ============================================================
@app.post("/admin/subscription/activate")
@admin_required
def admin_activate_subscription():
    data = request.get_json() or {}
    email       = data.get("email")
    tier        = data.get("tier", "ENTERPRISE")
    end_date    = data.get("end_date")   # ISO or YYYY-MM-DD
    access_code = (data.get("access_code") or "").strip().upper()

    if not email or not end_date or not access_code:
        return jsonify({"ok": False, "error": "email, end_date, and access_code required"}), 400

    end_dt = parse_dt(end_date)
    if not end_dt:
        return jsonify({"ok": False, "error": "Invalid end_date format"}), 400

    try:
        result = supabase.table("members").update({
            "approved":            True,
            "subscription_active": True,
            "subscription_start":  datetime.now(timezone.utc).isoformat(),
            "subscription_end":    end_dt.isoformat(),
            "tier":                tier,
            "access_code":         access_code,
        }).eq("email", email).execute()

        if not result.data:
            return jsonify({"ok": False, "error": "Member not found"}), 404

        member = result.data[0]

        # Send approval email with access code
        email_access_granted(
            name=member.get("name") or email,
            to_email=email,
            access_code=access_code,
            tier=tier
        )

        return jsonify({"ok": True, "message": f"Activated for {email}", "member": member, "email_sent": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.post("/admin/subscription/deactivate")
@admin_required
def admin_deactivate_subscription():
    data = request.get_json() or {}
    email = data.get("email")
    if not email:
        return jsonify({"ok": False, "error": "email required"}), 400

    try:
        result = supabase.table("members").update({
            "subscription_active": False
        }).eq("email", email).execute()

        if not result.data:
            return jsonify({"ok": False, "error": "Member not found"}), 404

        return jsonify({"ok": True, "message": f"Deactivated for {email}"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/admin/members")
@admin_required
def admin_list_members():
    try:
        result = supabase.table("members").select(
            "id,user_id,email,name,tier,subscription_active,subscription_end,approved,created_at"
        ).order("created_at", desc=True).execute()
        return jsonify({"ok": True, "members": result.data, "count": len(result.data)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# ============================================================
# TRADING — STATE (gauges: daily loss %, trades used, queue)
# ============================================================
@app.get("/api/trading/state")
@token_required
def trading_state():
    """
    Returns gauge data sourced directly from Tradier balances.
    Falls back to zeroes gracefully if Tradier creds not connected yet.
    """
    member = request.member
    _fallback = {
        "ok": True,
        "bot_online": False,
        "state": {
            "daily_loss_pct":     0.0,
            "max_daily_loss_pct": 6,
            "trades_today":       0,
            "max_trades_per_day": 3,
            "current_equity":     0.0,
            "realized_pnl_today": 0.0,
            "kill_switch":        False,
            "mode":               "PAPER" if "sandbox" in (member.get("tradier_base_url") or "sandbox") else "LIVE",
        }
    }
    try:
        account_id, token, base_url = get_member_tradier(member)
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

        bal_r = requests.get(f"{base_url}/v1/accounts/{account_id}/balances",
                             headers=headers, timeout=8)
        balances = bal_r.json().get("balances", {}) if bal_r.status_code == 200 else {}
        equity = float(balances.get("equity") or balances.get("total_equity") or 0)

        # Today P&L from gainloss
        gl_r = requests.get(f"{base_url}/v1/accounts/{account_id}/gainloss",
                            headers=headers, timeout=8)
        gl_data = gl_r.json().get("gainloss", {}) if gl_r.status_code == 200 else {}
        closed = gl_data.get("closed_position", []) if gl_data else []
        if isinstance(closed, dict):
            closed = [closed]
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        today_pnl = sum(
            float(cp.get("gain_loss") or 0) for cp in closed
            if (cp.get("close_date") or "")[:10] == today_str
        )

        mode = "PAPER" if "sandbox" in base_url else "LIVE"
        daily_loss_pct = round(abs(today_pnl) / equity * 100, 2) if equity > 0 and today_pnl < 0 else 0.0

        return jsonify({
            "ok": True,
            "bot_online": True,
            "state": {
                "daily_loss_pct":     daily_loss_pct,
                "max_daily_loss_pct": 6,
                "trades_today":       0,
                "max_trades_per_day": 3,
                "current_equity":     equity,
                "realized_pnl_today": round(today_pnl, 2),
                "kill_switch":        False,
                "mode":               mode,
            }
        })
    except ValueError:
        return jsonify({**_fallback, "error": "no_tradier"}), 200
    except requests.exceptions.Timeout:
        return jsonify({**_fallback, "error": "tradier_timeout"}), 200
    except Exception as e:
        return jsonify({**_fallback, "error": str(e)}), 200


@app.get("/api/trading/queue")
@token_required
def trading_queue():
    """Returns empty queue — bot queue not yet wired to multi-client."""
    return jsonify({"ok": True, "queues": {"scan": 0, "exec": 0, "exit": 0}})


# ============================================================
# SIGNALS (Supabase) — live scanner output for signals tab
# ============================================================
@app.get("/api/signals")
@token_required
def get_signals():
    """
    Returns active signals from Supabase signals table.
    Filtered to last 24 hours so stale signals don't show.
    """
    try:
        from datetime import timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        # Try signals table first, fall back to signal_outcomes if it doesn't exist
        try:
            res = supabase.table("signals") \
                .select("*") \
                .gte("signal_time", cutoff) \
                .order("signal_time", desc=True) \
                .limit(50) \
                .execute()
            return jsonify({"ok": True, "signals": res.data or [], "source": "signals"})
        except Exception:
            # Fall back to signal_outcomes table
            res2 = supabase.table("signal_outcomes") \
                .select("*") \
                .gte("created_at", cutoff) \
                .order("created_at", desc=True) \
                .limit(50) \
                .execute()
            return jsonify({"ok": True, "signals": res2.data or [], "source": "signal_outcomes"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ============================================================
# PERFORMANCE — daily_performance table for logged-in member
# ============================================================
@app.get("/api/trading/daily_performance")
@token_required
def trading_daily_performance():
    """
    Returns last 30 days of daily_performance rows for the
    logged-in member. Dashboard uses this for the Performance tab.
    """
    member = request.member
    try:
        res = supabase.table("daily_performance") \
            .select("*") \
            .eq("member_id", member["id"]) \
            .order("trade_date", desc=True) \
            .limit(30) \
            .execute()
        return jsonify({"ok": True, "performance": res.data or []})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ============================================================
# KILL SWITCH
# ============================================================
@app.post("/api/trading/killswitch")
@token_required
def trading_killswitch():
    """
    Pause or resume the trading bot kill switch.
    Body: { "action": "pause" | "resume" }
    """
    data = request.get_json() or {}
    action = (data.get("action") or "").strip().lower()

    if action not in ("pause", "resume"):
        return jsonify({"ok": False, "error": "action must be 'pause' or 'resume'"}), 400

    try:
        if action == "pause":
            # Bot route: POST /kill_switch/on (defined in bot app.py)
            r = bot_post("/kill_switch/on")
            kill_switch = True
        else:
            # Bot route: POST /kill_switch/off (defined in bot app.py)
            r = bot_post("/kill_switch/off")
            kill_switch = False

        if r.status_code == 404:
            return jsonify({
                "ok": False,
                "error": "bot_route_not_found",
                "hint": "Bot does not have /kill_switch/on or /off routes. Check bot app.py."
            }), 502

        if not (200 <= r.status_code < 300):
            return jsonify({
                "ok": False,
                "error": f"bot_returned_{r.status_code}",
                "detail": r.text[:200]
            }), 502

        return jsonify({"ok": True, "kill_switch": kill_switch, "action": action})

    except requests.exceptions.Timeout:
        return jsonify({"ok": False, "error": "bot_timeout", "hint": "Bot is sleeping on Render free tier"}), 504
    except requests.exceptions.ConnectionError:
        return jsonify({"ok": False, "error": "bot_offline"}), 503
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ============================================================
# NOTIFICATIONS
# ============================================================
@app.get("/api/trading/notifications")
@token_required
def get_notifications():
    """
    Returns the most recent 20 notifications for the logged-in member.
    Table columns: id, member_id, type, title, body, read, created_at
    """
    member = request.member
    try:
        res = supabase.table("notifications") \
            .select("id,member_id,type,title,body,read,created_at") \
            .eq("member_id", member["id"]) \
            .order("created_at", desc=True) \
            .limit(20) \
            .execute()
        return jsonify({"ok": True, "notifications": res.data or []})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/api/trading/notifications/read")
@token_required
def mark_notifications_read():
    """
    Mark one or all notifications as read.
    Body: { "notification_id": "uuid" }  OR  { "all": true }
    """
    member = request.member
    data = request.get_json() or {}

    mark_all = data.get("all", False)
    notification_id = data.get("notification_id")

    if not mark_all and not notification_id:
        return jsonify({"ok": False, "error": "Provide 'notification_id' or 'all': true"}), 400

    try:
        q = supabase.table("notifications").update({"read": True}).eq("member_id", member["id"])
        if not mark_all:
            q = q.eq("id", notification_id)
        q.execute()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ============================================================
# EOD SUMMARY
# ============================================================
@app.get("/api/trading/eod_summary")
@token_required
def trading_eod_summary():
    """
    Builds an end-of-day summary for the logged-in member by combining:
      - today's daily_performance row (Supabase)
      - today's closed positions from the bot (/client/me/performance)
    """
    member = request.member
    today_str = date.today().isoformat()  # YYYY-MM-DD

    # --- Supabase daily_performance row ---
    perf_row = {}
    try:
        res = supabase.table("daily_performance") \
            .select("*") \
            .eq("member_id", member["id"]) \
            .eq("trade_date", today_str) \
            .limit(1) \
            .execute()
        if res.data:
            perf_row = res.data[0]
    except Exception:
        pass

    # --- Bot performance endpoint ---
    bot_perf = {}
    try:
        r = bot_get("/client/me/performance")
        bot_perf = r.json() if r.ok else {}
    except Exception:
        pass

    # Build summary — prefer Supabase row, fall back to bot data
    daily_pnl   = perf_row.get("daily_pnl")   or bot_perf.get("daily_pnl")   or 0
    trades_count = perf_row.get("trades_count") or bot_perf.get("trades_count") or 0
    wins        = perf_row.get("wins")         or bot_perf.get("wins")         or 0
    losses      = perf_row.get("losses")       or bot_perf.get("losses")       or 0
    best_trade  = perf_row.get("best_trade")   or bot_perf.get("best_trade")
    worst_trade = perf_row.get("worst_trade")  or bot_perf.get("worst_trade")
    equity      = perf_row.get("equity")       or bot_perf.get("equity")

    total_decided = (wins or 0) + (losses or 0)
    win_rate = round((wins / total_decided) * 100, 2) if total_decided else 0.0

    summary = {
        "date":         today_str,
        "daily_pnl":    daily_pnl,
        "trades_count": trades_count,
        "wins":         wins,
        "losses":       losses,
        "win_rate":     win_rate,
        "best_trade":   best_trade,
        "worst_trade":  worst_trade,
        "equity":       equity,
    }

    return jsonify({"ok": True, "summary": summary})


# ============================================================
# INTERNAL NOTIFY (no auth — protected by INTERNAL_SECRET)
# ============================================================
@app.post("/internal/notify")
def internal_notify():
    """
    Used by the bot/scanner to push notifications to members.
    Protected by INTERNAL_SECRET header check (not Supabase auth).
    Body: { "member_email": str, "type": str, "title": str, "body": str, "secret": str }
    """
    INTERNAL_SECRET = os.getenv("INTERNAL_SECRET")
    data = request.get_json() or {}

    secret = data.get("secret", "")
    if not INTERNAL_SECRET or secret != INTERNAL_SECRET:
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    member_email = (data.get("member_email") or "").strip().lower()
    notif_type   = (data.get("type")         or "").strip()
    title        = (data.get("title")        or "").strip()
    body         = (data.get("body")         or "").strip()

    if not member_email or not notif_type or not title:
        return jsonify({"ok": False, "error": "member_email, type, and title are required"}), 400

    # Look up member by email
    try:
        member_res = supabase.table("members").select("id").eq("email", member_email).limit(1).execute()
        if not member_res.data:
            return jsonify({"ok": False, "error": "Member not found"}), 404
        member_id = member_res.data[0]["id"]
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

    # Insert notification
    try:
        supabase.table("notifications").insert({
            "member_id":  member_id,
            "type":       notif_type,
            "title":      title,
            "body":       body,
            "read":       False,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }).execute()
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

    return jsonify({"ok": True})



# ============================================================
# PROOF SYSTEM API ROUTES
# Powers the client dashboard proof pages (v2 performance only).
# All queries filter to system_version = 'v2' automatically.
# ============================================================
# ============================================================
# PROOF SYSTEM API ROUTES
# These power the new client dashboard proof pages.
# All queries filter to system_version = 'v2' automatically.
# ============================================================

@app.route("/api/proof/summary")
@auth_required
def proof_summary():
    """V2 performance summary for the logged-in client."""
    try:
        res = supabase.table("proof_trades") \
            .select("win,option_pnl_pct,tier,closed_at") \
            .eq("client_email", request.email) \
            .eq("system_version", "v2") \
            .execute()
        trades = res.data or []
        if not trades:
            return jsonify({"total_trades": 0, "win_rate": None,
                            "avg_return": None, "max_drawdown_single": None,
                            "a_plus_win_rate": None, "a_win_rate": None})

        wins      = [t for t in trades if t.get("win")]
        pnls      = [float(t.get("option_pnl_pct", 0)) for t in trades]
        a_plus    = [t for t in trades if t.get("tier") == "A+"]
        a_tier    = [t for t in trades if t.get("tier") == "A"]

        return jsonify({
            "total_trades":        len(trades),
            "wins":                len(wins),
            "win_rate":            round(len(wins)/len(trades)*100, 1) if trades else None,
            "avg_return":          round(sum(pnls)/len(pnls), 1) if pnls else None,
            "max_drawdown_single": round(min(pnls), 1) if pnls else None,
            "a_plus_win_rate":     round(sum(1 for t in a_plus if t.get("win"))/len(a_plus)*100,1) if a_plus else None,
            "a_win_rate":          round(sum(1 for t in a_tier  if t.get("win"))/len(a_tier)*100, 1) if a_tier else None,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/proof/trades")
@auth_required
def proof_trades_api():
    """Last N closed trades for the logged-in client (v2 only)."""
    limit = min(int(request.args.get("limit", 20)), 100)
    try:
        res = supabase.table("proof_trades") \
            .select("*") \
            .eq("client_email", request.email) \
            .eq("system_version", "v2") \
            .order("closed_at", desc=True) \
            .limit(limit) \
            .execute()
        return jsonify({"data": res.data or []})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/proof/equity_curve")
@auth_required
def proof_equity_curve():
    """Cumulative daily P&L for equity curve chart."""
    try:
        res = supabase.table("proof_daily_summary") \
            .select("date,gross_pnl_pct") \
            .eq("client_email", request.email) \
            .eq("system_version", "v2") \
            .order("date", desc=False) \
            .execute()
        rows = res.data or []
        # Build cumulative
        running = 0.0
        curve   = []
        for r in rows:
            running += float(r.get("gross_pnl_pct", 0) or 0)
            curve.append({"date": r["date"], "cumulative_pnl": round(running, 2)})
        return jsonify({"data": curve})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/proof/daily")
@auth_required
def proof_daily():
    """Daily summaries for the performance tab."""
    limit = min(int(request.args.get("limit", 14)), 60)
    try:
        res = supabase.table("proof_daily_summary") \
            .select("*") \
            .eq("client_email", request.email) \
            .eq("system_version", "v2") \
            .order("date", desc=True) \
            .limit(limit) \
            .execute()
        return jsonify({"data": list(reversed(res.data or []))})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/proof/today")
@auth_required
def proof_today():
    """Today's signal funnel stats + regime."""
    from datetime import date
    today = str(date.today())
    try:
        res = supabase.table("proof_daily_summary") \
            .select("*") \
            .eq("client_email", request.email) \
            .eq("date", today) \
            .limit(1) \
            .execute()
        rows = res.data or []
        if rows:
            return jsonify(rows[0])
        return jsonify({"signals_received": 0, "trades_executed": 0,
                        "context_blocked": 0, "options_rejected": 0,
                        "passed_score_filter": 0})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ============================================================
# RUN
# ============================================================
if __name__ == "__main__":
    port = int(os.getenv("PORT", 5001))
    app.run(host="0.0.0.0", port=port, debug=False)
