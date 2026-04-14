# ap/onboarding.py
# =============================================================================
# Angel Precision -- Automated Member Onboarding
#
# Called from /admin/subscription/activate endpoint.
# Handles three things atomically:
#   1. DB provisioning  -- clients + client_state rows via ensure_client_exists
#   2. Welcome email    -- SendGrid (access code + dashboard link + Tradier setup guide)
#   3. Discord alert    -- post to operator channel confirming new member is live
#
# Required env vars:
#   SENDGRID_API_KEY    -- SendGrid API key
#   DISCORD_WEBHOOK_URL -- Discord webhook URL for operator alerts
#   DASHBOARD_URL       -- https://www.angelprecision.com/login.html
# =============================================================================

from __future__ import annotations

import os
import logging
from datetime import datetime, timezone

log = logging.getLogger("ap.onboarding")

# ── Config ────────────────────────────────────────────────────────────────────
SENDGRID_API_KEY    = os.getenv("SENDGRID_API_KEY", "")
FROM_EMAIL          = "angel@angelprecision.com"
DASHBOARD_URL       = os.getenv("DASHBOARD_URL", "https://www.angelprecision.com/login.html")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
ZELLE_NAME          = "Aamiyah Wilson"
ZELLE_CONTACT       = "209-486-1334"


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def run_onboarding(
    *,
    email:       str,
    name:        str,
    access_code: str,
    tier:        str,
    equity:      float = 25000.0,
) -> dict:
    """
    Run full onboarding flow for a newly approved member.
    Returns a dict with results for each step.
    Safe to call multiple times -- DB steps are idempotent.
    """
    results = {
        "db_provisioned": False,
        "email_sent":     False,
        "discord_posted": False,
        "errors":         [],
    }

    # ── 1. DB provision ───────────────────────────────────────────────────────
    try:
        from ap.db import ensure_client_exists
        ensure_client_exists(email, equity=equity)
        results["db_provisioned"] = True
        log.info(f"[onboarding] DB provisioned for {email}")
    except Exception as e:
        msg = f"DB provision failed for {email}: {e}"
        log.error(f"[onboarding] {msg}")
        results["errors"].append(msg)

    # ── 2. Welcome email ──────────────────────────────────────────────────────
    try:
        sent = _send_welcome_email(
            name=name,
            email=email,
            access_code=access_code,
            tier=tier,
        )
        results["email_sent"] = sent
        if not sent:
            results["errors"].append("Email not sent (SENDGRID_API_KEY may not be set)")
    except Exception as e:
        msg = f"Email failed for {email}: {e}"
        log.error(f"[onboarding] {msg}")
        results["errors"].append(msg)

    # ── 3. Discord alert ──────────────────────────────────────────────────────
    try:
        posted = _post_discord_alert(
            name=name,
            email=email,
            tier=tier,
            equity=equity,
        )
        results["discord_posted"] = posted
        if not posted:
            results["errors"].append("Discord not posted (DISCORD_WEBHOOK_URL may not be set)")
    except Exception as e:
        msg = f"Discord post failed: {e}"
        log.error(f"[onboarding] {msg}")
        results["errors"].append(msg)

    status = "✅" if results["db_provisioned"] else "⚠️"
    log.info(
        f"[onboarding] {status} {email} | "
        f"db={results['db_provisioned']} "
        f"email={results['email_sent']} "
        f"discord={results['discord_posted']}"
    )
    return results


# =============================================================================
# WELCOME EMAIL
# =============================================================================

def _send_welcome_email(
    *,
    name:        str,
    email:       str,
    access_code: str,
    tier:        str,
) -> bool:
    if not SENDGRID_API_KEY:
        log.warning("[onboarding] SENDGRID_API_KEY not set -- skipping email")
        return False

    try:
        from sendgrid import SendGridAPIClient
        from sendgrid.helpers.mail import Mail
    except ImportError:
        log.error("[onboarding] sendgrid package not installed")
        return False

    html = f"""
<!DOCTYPE html>
<html>
<body style="margin:0;padding:0;background:#080b0f;font-family:Arial,sans-serif;">
<div style="max-width:600px;margin:0 auto;padding:40px 24px;">

  <!-- Header -->
  <div style="text-align:center;margin-bottom:32px;">
    <div style="font-size:28px;font-weight:900;color:#00e5a0;letter-spacing:0.05em;">ANGEL PRECISION</div>
    <div style="font-size:13px;color:#3d5470;letter-spacing:0.12em;margin-top:4px;">AUTONOMOUS TRADING SYSTEM</div>
  </div>

  <!-- Hero -->
  <div style="background:#0d1117;border:1px solid #00e5a0;border-radius:12px;padding:32px;margin-bottom:24px;text-align:center;">
    <div style="font-size:36px;margin-bottom:8px;">✓</div>
    <h2 style="color:#00e5a0;font-size:22px;margin:0 0 8px;">You're In, {name.split()[0]}.</h2>
    <p style="color:#7a95b0;margin:0;font-size:14px;">Your Angel Precision account is live and trading on your behalf.</p>
  </div>

  <!-- Credentials -->
  <div style="background:#0d1117;border:1px solid #1e2a35;border-radius:8px;padding:24px;margin-bottom:24px;">
    <p style="margin:0 0 16px;font-size:11px;font-weight:700;letter-spacing:0.14em;text-transform:uppercase;color:#3d5470;">Your Credentials</p>
    <table style="width:100%;border-collapse:collapse;">
      <tr>
        <td style="padding:8px 0;color:#7a95b0;font-size:13px;width:140px;">Email</td>
        <td style="padding:8px 0;color:#e2eaf5;font-size:13px;">{email}</td>
      </tr>
      <tr style="border-top:1px solid #1e2a35;">
        <td style="padding:8px 0;color:#7a95b0;font-size:13px;">Access Code</td>
        <td style="padding:8px 0;">
          <span style="color:#00e5a0;font-size:20px;font-weight:700;font-family:monospace;letter-spacing:0.15em;">{access_code}</span>
        </td>
      </tr>
      <tr style="border-top:1px solid #1e2a35;">
        <td style="padding:8px 0;color:#7a95b0;font-size:13px;">Tier</td>
        <td style="padding:8px 0;color:#e2eaf5;font-size:13px;">{tier}</td>
      </tr>
    </table>
  </div>

  <!-- Dashboard CTA -->
  <div style="text-align:center;margin-bottom:32px;">
    <a href="{DASHBOARD_URL}"
       style="display:inline-block;background:#00e5a0;color:#080b0f;font-weight:700;
              padding:16px 40px;border-radius:8px;text-decoration:none;
              font-size:14px;letter-spacing:0.08em;">
      VIEW YOUR DASHBOARD →
    </a>
  </div>

  <!-- Tradier Setup -->
  <div style="background:#0d1117;border:1px solid #1e2a35;border-radius:8px;padding:24px;margin-bottom:24px;">
    <p style="margin:0 0 16px;font-size:11px;font-weight:700;letter-spacing:0.14em;text-transform:uppercase;color:#3d5470;">Connect Your Tradier Account</p>
    <p style="color:#7a95b0;font-size:13px;margin:0 0 16px;">To allow the bot to trade on your behalf, connect your Tradier brokerage account in 3 steps:</p>

    <div style="margin-bottom:12px;display:flex;align-items:flex-start;">
      <div style="background:#00e5a0;color:#080b0f;font-weight:700;border-radius:50%;width:24px;height:24px;min-width:24px;
                  display:flex;align-items:center;justify-content:center;font-size:12px;margin-right:12px;margin-top:2px;">1</div>
      <div>
        <p style="color:#e2eaf5;font-size:13px;margin:0 0 2px;font-weight:600;">Open a Tradier account</p>
        <p style="color:#7a95b0;font-size:12px;margin:0;">Visit <a href="https://brokerage.tradier.com" style="color:#00e5a0;">brokerage.tradier.com</a> and open a brokerage account. Options approval (Level 2 or higher) is required.</p>
      </div>
    </div>

    <div style="margin-bottom:12px;display:flex;align-items:flex-start;">
      <div style="background:#00e5a0;color:#080b0f;font-weight:700;border-radius:50%;width:24px;height:24px;min-width:24px;
                  display:flex;align-items:center;justify-content:center;font-size:12px;margin-right:12px;margin-top:2px;">2</div>
      <div>
        <p style="color:#e2eaf5;font-size:13px;margin:0 0 2px;font-weight:600;">Generate your API key</p>
        <p style="color:#7a95b0;font-size:12px;margin:0;">Log in to Tradier → Account → API Access → Generate a new token. Copy the Bearer token (starts with the long alphanumeric string after "Bearer ").</p>
      </div>
    </div>

    <div style="display:flex;align-items:flex-start;">
      <div style="background:#00e5a0;color:#080b0f;font-weight:700;border-radius:50%;width:24px;height:24px;min-width:24px;
                  display:flex;align-items:center;justify-content:center;font-size:12px;margin-right:12px;margin-top:2px;">3</div>
      <div>
        <p style="color:#e2eaf5;font-size:13px;margin:0 0 2px;font-weight:600;">Add it to your dashboard</p>
        <p style="color:#7a95b0;font-size:12px;margin:0;">Log into your Angel Precision dashboard → Settings → paste your Tradier account ID and API token. The bot activates automatically within 5 minutes.</p>
      </div>
    </div>
  </div>

  <!-- What happens next -->
  <div style="background:#0d1117;border:1px solid #1e2a35;border-radius:8px;padding:24px;margin-bottom:32px;">
    <p style="margin:0 0 12px;font-size:11px;font-weight:700;letter-spacing:0.14em;text-transform:uppercase;color:#3d5470;">What Happens Next</p>
    <p style="color:#7a95b0;font-size:13px;margin:0 0 8px;">• The bot scans the market every morning at 9:05am and 10:15am ET</p>
    <p style="color:#7a95b0;font-size:13px;margin:0 0 8px;">• Qualifying setups are routed through our risk engine and executed automatically</p>
    <p style="color:#7a95b0;font-size:13px;margin:0 0 8px;">• All trades are visible in your dashboard in real time</p>
    <p style="color:#7a95b0;font-size:13px;margin:0;">• Weekly performance reports are emailed every Friday</p>
  </div>

  <!-- Footer -->
  <div style="text-align:center;border-top:1px solid #1e2a35;padding-top:24px;">
    <p style="color:#3d5470;font-size:12px;margin:0 0 8px;">Keep your access code private. Do not share it with anyone.</p>
    <p style="color:#3d5470;font-size:11px;margin:0;">
      Questions? <a href="mailto:angel@angelprecision.com" style="color:#00e5a0;">angel@angelprecision.com</a>
      &nbsp;·&nbsp; <a href="https://www.angelprecision.com" style="color:#00e5a0;">angelprecision.com</a>
    </p>
  </div>

</div>
</body>
</html>
"""

    try:
        msg = Mail(
            from_email=FROM_EMAIL,
            to_emails=email,
            subject="Angel Precision — You're Approved. Here's Your Access.",
            html_content=html,
        )
        sg   = SendGridAPIClient(SENDGRID_API_KEY)
        resp = sg.send(msg)
        log.info(f"[onboarding] Welcome email sent to {email} (status {resp.status_code})")
        return True
    except Exception as e:
        log.error(f"[onboarding] SendGrid error: {e}")
        return False


# =============================================================================
# DISCORD ALERT
# =============================================================================

def _post_discord_alert(
    *,
    name:   str,
    email:  str,
    tier:   str,
    equity: float,
) -> bool:
    if not DISCORD_WEBHOOK_URL:
        log.warning("[onboarding] DISCORD_WEBHOOK_URL not set -- skipping Discord")
        return False

    import requests as _requests

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    payload = {
        "username":   "Angel Precision",
        "avatar_url": "https://www.angelprecision.com/logo.png",
        "embeds": [{
            "title":       "🟢 New Client Onboarded",
            "color":       0x00e5a0,
            "description": f"**{name}** is live and trading.",
            "fields": [
                {"name": "Email",          "value": email,           "inline": True},
                {"name": "Tier",           "value": tier,            "inline": True},
                {"name": "Starting Equity","value": f"${equity:,.0f}","inline": True},
            ],
            "footer": {"text": f"Angel Precision Bot  ·  {now_str}"},
        }]
    }

    try:
        r = _requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
        if r.status_code in (200, 204):
            log.info(f"[onboarding] Discord alert posted for {email}")
            return True
        else:
            log.warning(f"[onboarding] Discord returned {r.status_code}: {r.text[:100]}")
            return False
    except Exception as e:
        log.error(f"[onboarding] Discord post error: {e}")
        return False


# =============================================================================
# DEACTIVATION ALERT  (call when subscription cancelled)
# =============================================================================

def post_discord_deactivation(name: str, email: str, reason: str = "subscription ended") -> bool:
    if not DISCORD_WEBHOOK_URL:
        return False
    import requests as _requests
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    payload = {
        "username": "Angel Precision",
        "embeds": [{
            "title":       "🔴 Client Deactivated",
            "color":       0xff4444,
            "description": f"**{name}** ({email}) has been deactivated.",
            "fields": [{"name": "Reason", "value": reason, "inline": False}],
            "footer": {"text": f"Angel Precision Bot  ·  {now_str}"},
        }]
    }
    try:
        r = _requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
        return r.status_code in (200, 204)
    except Exception:
        return False
