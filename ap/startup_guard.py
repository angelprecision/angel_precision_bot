"""
ap/startup_guard.py
Angel Precision — Pre-flight Configuration Validator

Run this at the TOP of app.py before any thread or broker is initialized.
In LIVE mode, any failure here CRASHES the process intentionally.
Better to fail loudly on startup than silently with client money.

Usage in app.py:
    from ap.startup_guard import run_preflight
    run_preflight()   # call before create_app()
"""

from __future__ import annotations

import logging
import os
import sys

log = logging.getLogger("ap.startup_guard")

SANDBOX_URLS = {
    "https://sandbox.tradier.com",
    "http://sandbox.tradier.com",
    "sandbox.tradier.com",
}

LIVE_URL = "https://api.tradier.com"


def _check_tradier_url():
    """CRITICAL: block LIVE mode from using sandbox URL."""
    mode = os.getenv("BOT_MODE", os.getenv("AP_MODE", "PAPER")).upper()
    url  = os.getenv("TRADIER_BASE_URL", "https://sandbox.tradier.com").strip().rstrip("/")

    normalized = url.lower().replace("https://", "").replace("http://", "")
    is_sandbox = any(normalized.startswith(s.replace("https://", "").replace("http://", "")) for s in SANDBOX_URLS)

    if mode == "LIVE" and is_sandbox:
        msg = (
            f"\n{'='*60}\n"
            f"STARTUP BLOCKED — LIVE mode with sandbox broker URL!\n"
            f"  BOT_MODE       = {mode}\n"
            f"  TRADIER_BASE_URL = {url}\n"
            f"\nSet TRADIER_BASE_URL=https://api.tradier.com for live trading.\n"
            f"Set BOT_MODE=PAPER to continue with sandbox.\n"
            f"{'='*60}\n"
        )
        log.critical(msg)
        sys.exit(1)

    if mode == "LIVE" and not url:
        log.critical("STARTUP BLOCKED — LIVE mode requires explicit TRADIER_BASE_URL")
        sys.exit(1)

    if mode == "LIVE":
        log.info("LIVE mode verified: broker URL=%s", url)
    else:
        log.info("Mode=%s | broker URL=%s", mode, url)


def _check_required_env_vars():
    """Warn loudly on missing critical env vars."""
    required_live = ["TRADIER_ACCESS_TOKEN", "TRADIER_ACCOUNT_ID", "DATABASE_URL"]
    required_all  = ["DATABASE_URL", "AP_HMAC_SECRET"]
    mode = os.getenv("BOT_MODE", "PAPER").upper()

    missing_all = [k for k in required_all if not os.getenv(k, "").strip()]
    if missing_all:
        log.critical("MISSING REQUIRED ENV VARS: %s", missing_all)
        if mode == "LIVE":
            sys.exit(1)

    if mode == "LIVE":
        missing_live = [k for k in required_live if not os.getenv(k, "").strip()]
        if missing_live:
            log.critical("LIVE mode missing env vars: %s", missing_live)
            sys.exit(1)


def _check_position_limit_consistency():
    """
    CRITICAL: MAX_CONCURRENT_POSITIONS (risk gate) and MAX_POSITIONS (execution core)
    must agree. If they differ, clamp to the lower value and warn loudly.

    The execution core reads MAX_POSITIONS; the risk gate reads MAX_CONCURRENT_POSITIONS.
    They MUST be the same number.
    """
    # AUDIT PHASE-2: raised defaults from 2/7 -> 4/4 so the two caps default to
    # the same value. They're meant to be synchronized; previous defaults diverged.
    risk_limit = int(os.getenv("MAX_CONCURRENT_POSITIONS", "4"))
    exec_limit = int(os.getenv("MAX_POSITIONS", "4"))

    if risk_limit != exec_limit:
        # Clamp to the lower (safer) value
        canonical = min(risk_limit, exec_limit)
        log.critical(
            "POSITION LIMIT CONFLICT: MAX_CONCURRENT_POSITIONS=%d vs MAX_POSITIONS=%d "
            "— clamping both to %d. Fix your env vars!",
            risk_limit, exec_limit, canonical,
        )
        os.environ["MAX_CONCURRENT_POSITIONS"] = str(canonical)
        os.environ["MAX_POSITIONS"]            = str(canonical)
    else:
        log.info("Position limits consistent: %d", risk_limit)


def _check_discord_webhook():
    """Warn if Discord webhook is missing — no alerts will fire."""
    url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not url:
        log.warning(
            "DISCORD_WEBHOOK_URL not set — all Discord alerts will silently fail. "
            "Set this env var to receive trade notifications."
        )


def run_preflight():
    """
    Run all pre-flight checks.
    Call this at the very top of app.py before anything else starts.
    LIVE mode failures are fatal (sys.exit). PAPER mode failures are logged.
    """
    log.info("Running Angel Precision pre-flight checks...")

    _check_required_env_vars()
    _check_tradier_url()
    _check_position_limit_consistency()
    _check_discord_webhook()

    log.info("Pre-flight complete ✅")
