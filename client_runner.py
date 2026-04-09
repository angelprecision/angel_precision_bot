# client_runner.py — Multi-client trading loop for Angel Precision Bot
# Each active member with Tradier credentials gets their own isolated trading thread.
#
# How it works:
#   1. On startup, fetch all approved + subscription_active members from Supabase
#   2. For each member with tradier_account_id + tradier_access_token → spawn a ClientRunner thread
#   3. Each thread runs its own APExecutionCore (entry watcher, options gate, exit engine, feedback)
#   4. Every 5 minutes, re-check Supabase for new clients or disconnected ones
#   5. app.py routes /signal calls to each active runner's core via runner.core.receive_signal()
#
# QUEUE SUBSCRIBER FIX:
#   gunicorn --workers 2 creates 2 separate Python processes, each with their own
#   _active_runners dict. Signals hit worker 1 OR worker 2 randomly via round-robin.
#   Fix: each ClientRunner now subscribes to the SQLite trade_queue directly.
#   Signals written by ANY gunicorn worker to the shared SQLite DB are picked up
#   by the queue subscriber thread and routed to receive_signal(). This works
#   correctly with any number of gunicorn workers.

import os
import time
import json
import sqlite3
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
    One thread per client. Owns its own broker connection and APExecutionCore.
    The core handles: entry watching, options intelligence, exit engine, feedback loop.
    Also runs a queue subscriber thread that polls trade_queue and routes signals
    to receive_signal() — works correctly across all gunicorn worker processes.
    """
    def __init__(self, member: dict):
        super().__init__(daemon=True, name=f"runner-{member['email']}")
        self.member       = member
        self.email        = member["email"]
        self.account_id   = member["tradier_account_id"]
        self.base_url     = member.get("tradier_base_url", "https://sandbox.tradier.com")
        self.stopped      = threading.Event()
        self.core         = None

    def _get_token(self) -> str | None:
        try:
            raw = self.member.get("tradier_access_token", "")
            if not raw:
                return None
            return decrypt_token(raw)
        except Exception as e:
            logger.error(f"[{self.email}] Token decrypt failed: {e}")
            return None

    def run(self):
        logger.info(f"[{self.email}] ClientRunner starting — account {self.account_id} @ {self.base_url}")
        token = self._get_token()
        if not token:
            logger.error(f"[{self.email}] No token — aborting runner")
            return

        try:
            from ap.brokers.tradier import TradierBroker, TradierConfig
            from ap_execution_core  import APExecutionCore
        except Exception as e:
            logger.error(f"[{self.email}] Failed to import bot modules: {e}")
            return

        try:
            broker_cfg = TradierConfig(
                base_url=self.base_url,
                access_token=token,
                account_id=self.account_id,
            )
            broker = TradierBroker(broker_cfg)
            logger.info(f"[{self.email}] Broker initialized. Starting execution core.")

            sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY) if SUPABASE_URL else None

            self.core = APExecutionCore(
                broker=broker,
                supabase_client=sb,
                email=self.email,
            )
            self.core.start()

            # Start queue subscriber — routes signals from SQLite to receive_signal()
            # This is the fix for the gunicorn multi-worker routing problem.
            self._start_queue_subscriber()

            while not self.stopped.wait(60):
                pass

        except Exception as e:
            logger.error(f"[{self.email}] Runner crashed: {e}", exc_info=True)
        finally:
            if self.core:
                self.core.stop()
            logger.info(f"[{self.email}] ClientRunner stopped.")

    def stop(self):
        self.stopped.set()

    # ── QUEUE SUBSCRIBER ──────────────────────────────────────────────────────

    def _start_queue_subscriber(self):
        """
        Poll trade_queue for NEW signals and route them to receive_signal().

        Race condition fix:
            The old worker_loop (started by app.py) also reads from trade_queue.
            To prevent it from stealing new-format signals before the subscriber
            can process them, this subscriber bulk-claims ALL NEW rows in a single
            UPDATE before reading their payloads. The old worker uses the same
            atomic claim pattern so whichever process runs the UPDATE first wins
            all rows in that batch.

            Poll interval is 0.1s (10x faster than old worker at 1.0s) to ensure
            the subscriber almost always gets first pick.
        """
        def _poll():
            from ap.config import Config
            db_path = Config().DB_FILE

            while not self.stopped.is_set():
                try:
                    with sqlite3.connect(db_path, timeout=10,
                                         isolation_level=None) as c:
                        c.row_factory = sqlite3.Row
                        c.execute("PRAGMA journal_mode=WAL;")
                        c.execute("PRAGMA busy_timeout=5000;")

                        # Bulk-claim ALL new signals in one atomic UPDATE.
                        # This beats the old worker which only claims one at a time.
                        claimed = c.execute("""
                            UPDATE trade_queue
                            SET status = 'PROCESSING',
                                started_ts = datetime('now')
                            WHERE status = 'NEW'
                        """).rowcount

                        if claimed == 0:
                            time.sleep(0.1)
                            continue

                        # Fetch everything we just claimed
                        rows = c.execute("""
                            SELECT id, payload, signal_id
                            FROM trade_queue
                            WHERE status = 'PROCESSING'
                              AND started_ts >= datetime('now', '-3 seconds')
                            ORDER BY created_ts ASC
                        """).fetchall()

                    # Process each claimed row outside the DB connection
                    for row in rows:
                        job_id    = row["id"]
                        signal_id = row["signal_id"]

                        try:
                            payload = json.loads(row["payload"])
                        except Exception as e:
                            with sqlite3.connect(db_path, timeout=10,
                                                 isolation_level=None) as c:
                                c.execute(
                                    "UPDATE trade_queue SET status='REJECTED', last_error=? WHERE id=?",
                                    (str(e), job_id)
                                )
                            continue

                        if self.core:
                            try:
                                self.core.receive_signal(payload)
                                with sqlite3.connect(db_path, timeout=10,
                                                     isolation_level=None) as c:
                                    c.execute(
                                        "UPDATE trade_queue SET status='DONE', "
                                        "finished_ts=datetime('now') WHERE id=?",
                                        (job_id,)
                                    )
                            except Exception as e:
                                logger.error(
                                    f"[{self.email}] receive_signal failed for "
                                    f"{signal_id}: {e}"
                                )
                                with sqlite3.connect(db_path, timeout=10,
                                                     isolation_level=None) as c:
                                    c.execute(
                                        "UPDATE trade_queue SET status='ERROR', "
                                        "last_error=? WHERE id=?",
                                        (str(e), job_id)
                                    )
                        else:
                            # Core not ready — release back to NEW
                            with sqlite3.connect(db_path, timeout=10,
                                                 isolation_level=None) as c:
                                c.execute(
                                    "UPDATE trade_queue SET status='NEW', "
                                    "started_ts=NULL WHERE id=?",
                                    (job_id,)
                                )

                except Exception as e:
                    logger.warning(f"[{self.email}] Queue subscriber error: {e}")
                    time.sleep(1)

        t = threading.Thread(
            target=_poll,
            daemon=True,
            name=f"queue-sub-{self.email}",
        )
        t.start()
        logger.info(f"[{self.email}] Queue subscriber started")


# ── Supervisor loop ───────────────────────────────────────────────────────────

def _fetch_active_members(sb: Client) -> list[dict]:
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
    members = _fetch_active_members(sb)
    active_emails = {m["email"] for m in members}

    with _registry_lock:
        to_stop = [email for email in _active_runners if email not in active_emails]
        for email in to_stop:
            logger.info(f"Stopping runner for {email} — no longer active")
            _active_runners[email].stop()
            del _active_runners[email]

        for member in members:
            email = member["email"]
            if email not in _active_runners or not _active_runners[email].is_alive():
                logger.info(f"Starting runner for {email}")
                runner = ClientRunner(member)
                runner.start()
                _active_runners[email] = runner


def start_multi_client_supervisor():
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
            time.sleep(300)

    t = threading.Thread(target=_supervisor, daemon=True, name="client-supervisor")
    t.start()
    logger.info("Client supervisor thread launched")


def get_runner_status() -> list[dict]:
    with _registry_lock:
        return [
            {
                "email":       email,
                "account_id":  r.account_id,
                "base_url":    r.base_url,
                "alive":       r.is_alive(),
                "core_active": r.core is not None,
            }
            for email, r in _active_runners.items()
        ]


def route_signal_to_all_clients(signal: dict):
    """
    Called by app.py /signal endpoint.
    With the queue subscriber in place, this is now a best-effort fast path
    for the worker that happens to have runners. The queue subscriber handles
    the case where this worker has no runners.
    """
    with _registry_lock:
        runners = list(_active_runners.values())

    if not runners:
        # Not a warning — queue subscriber in the other worker handles it
        logger.debug(
            "route_signal_to_all_clients: no runners in this worker — "
            "queue subscriber will handle via trade_queue"
        )
        return

    for runner in runners:
        if runner.core and runner.is_alive():
            try:
                runner.core.receive_signal(signal)
            except Exception as e:
                logger.error(f"[{runner.email}] Signal routing failed: {e}")
