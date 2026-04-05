# client_runner.py — Multi-client trading loop for Angel Precision Bot
# Each active member with Tradier credentials gets their own isolated trading thread.
#
# How it works:
#   1. On startup, fetch all approved + subscription_active members from Supabase
#   2. For each member with tradier_account_id + tradier_access_token → spawn a ClientRunner thread
#   3. Each thread runs its own exit poller and trade loop independently
#   4. Every 5 minutes, re-check Supabase for new clients or disconnected ones
#
# The bot's existing single-client logic stays intact — ClientRunner wraps it per client.

import os
import time
import threading
import logging
import hashlib
import base64
from datetime import datetime, timezone

from cryptography.fernet import Fernet
from supabase import create_client, Client

logger = logging.getLogger("client_runner")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")

# ── Supabase ──────────────────────────────────────────────────────────────────
SUPABASE_URL         = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")

# ── Encryption (must match dashboard backend) ─────────────────────────────────
_raw_key   = os.getenv("ENCRYPTION_KEY", "angel-precision-encrypt-2026")
_key_bytes = hashlib.sha256(_raw_key.encode()).digest()
_fernet    = Fernet(base64.urlsafe_b64encode(_key_bytes))

def decrypt_token(ciphertext: str) -> str:
    return _fernet.decrypt(ciphertext.encode()).decode()


# ── Active runner registry ────────────────────────────────────────────────────
_active_runners: dict[str, "ClientRunner"] = {}   # keyed by member email
_registry_lock  = threading.Lock()


class ClientRunner(threading.Thread):
    """
    One thread per client. Owns its own broker connection and trading loop.
    Mirrors the single-client logic but isolated per account.
    """
    def __init__(self, member: dict):
        super().__init__(daemon=True, name=f"runner-{member['email']}")
        self.member       = member
        self.email        = member["email"]
        self.account_id   = member["tradier_account_id"]
        self.base_url     = member.get("tradier_base_url", "https://sandbox.tradier.com")
        self.stopped      = threading.Event()
        self._access_token: str | None = None

    def _get_token(self) -> str | None:
        """Decrypt the stored token."""
        try:
            raw = self.member.get("tradier_access_token", "")
            if not raw:
                return None
            return decrypt_token(raw)
        except Exception as e:
            logger.error(f"[{self.email}] Token decrypt failed: {e}")
            return None

    def _import_bot(self):
        """
        Lazy-import the bot modules to avoid circular imports at module load.
        Returns (TradierBroker, TradierConfig, exit_manager_loop) tuple or raises.
        """
        from ap.brokers.tradier  import TradierBroker, TradierConfig
        from ap.exit_manager     import exit_manager_loop
        return TradierBroker, TradierConfig, exit_manager_loop

    def run(self):
        logger.info(f"[{self.email}] ClientRunner starting — account {self.account_id} @ {self.base_url}")
        token = self._get_token()
        if not token:
            logger.error(f"[{self.email}] No token — aborting runner")
            return

        try:
            TradierBroker, TradierConfig, exit_manager_loop = self._import_bot()
        except Exception as e:
            logger.error(f"[{self.email}] Failed to import bot modules: {e}")
            return

        try:
            # Pass credentials directly — never mutate os.environ (not thread-safe)
            broker_cfg = TradierConfig(
                base_url=self.base_url,
                access_token=token,
                account_id=self.account_id,
            )
            broker = TradierBroker(broker_cfg)
            logger.info(f"[{self.email}] Broker initialized. Starting exit poll loop.")

            # exit_manager_loop runs its own internal loop — blocking call
            # It will run until the thread is stopped
            exit_manager_loop(broker)

        except Exception as e:
            logger.error(f"[{self.email}] Runner crashed: {e}", exc_info=True)
        finally:
            logger.info(f"[{self.email}] ClientRunner stopped.")

    def stop(self):
        self.stopped.set()


# ── Supervisor loop ───────────────────────────────────────────────────────────

def _fetch_active_members(sb: Client) -> list[dict]:
    """Pull all approved + active members with Tradier credentials from Supabase."""
    try:
        res = sb.table("members") \
            .select("id,email,name,tradier_account_id,tradier_access_token,tradier_base_url,subscription_active,approved") \
            .eq("approved", True) \
            .eq("subscription_active", True) \
            .not_.is_("tradier_account_id", "null") \
            .not_.is_("tradier_access_token", "null") \
            .execute()
        return res.data or []
    except Exception as e:
        logger.error(f"Supabase fetch failed: {e}")
        return []


def _sync_runners(sb: Client):
    """
    Compare active members in Supabase with running threads.
    Start new runners, stop removed ones.
    """
    members = _fetch_active_members(sb)
    active_emails = {m["email"] for m in members}

    with _registry_lock:
        # Stop runners for members that are no longer active
        to_stop = [email for email in _active_runners if email not in active_emails]
        for email in to_stop:
            logger.info(f"Stopping runner for {email} — no longer active")
            _active_runners[email].stop()
            del _active_runners[email]

        # Start runners for new members
        for member in members:
            email = member["email"]
            if email not in _active_runners or not _active_runners[email].is_alive():
                logger.info(f"Starting runner for {email}")
                runner = ClientRunner(member)
                runner.start()
                _active_runners[email] = runner


def start_multi_client_supervisor():
    """
    Call this once from bot app.py on startup.
    Runs a background thread that syncs client runners every 5 minutes.
    """
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        logger.warning("No Supabase credentials — multi-client supervisor not starting")
        return

    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    def _supervisor():
        logger.info("Multi-client supervisor started")
        while True:
            try:
                _sync_runners(sb)
            except Exception as e:
                logger.error(f"Supervisor sync error: {e}")
            time.sleep(300)   # re-sync every 5 minutes

    t = threading.Thread(target=_supervisor, daemon=True, name="client-supervisor")
    t.start()
    logger.info("Client supervisor thread launched")


def get_runner_status() -> list[dict]:
    """Return status of all active runners — for /debug/clients endpoint."""
    with _registry_lock:
        return [
            {
                "email":      email,
                "account_id": r.account_id,
                "base_url":   r.base_url,
                "alive":      r.is_alive(),
            }
            for email, r in _active_runners.items()
        ]
