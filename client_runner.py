# client_runner.py — Multi-client trading loop for Angel Precision Bot
# =============================================================================
# Each active member with Tradier credentials gets their own isolated trading thread.
#
# SIGNAL ROUTING FIX (gunicorn multi-worker):
#   gunicorn --workers 2 creates 2 separate Python processes, each with their own
#   _active_runners dict. Signals hit worker 1 OR worker 2 randomly via round-robin.
#
#   OLD (broken): route_signal_to_all_clients() called runner.core.receive_signal()
#   directly — only works if the signal lands on the SAME worker that has the runner.
#
#   FIX: route_signal_to_all_clients() now writes to SQLite trade_queue.
#   Each ClientRunner has a queue subscriber thread that polls trade_queue every 1s.
#   Signals written by ANY gunicorn worker are picked up by the subscriber in the
#   worker that has the runner — works correctly regardless of worker count.
# =============================================================================

import os
import time
import json
import threading
import logging
import hashlib
import base64
import uuid
from datetime import datetime, timezone

from cryptography.fernet import Fernet
from supabase import create_client, Client
from ap.db import conn as ap_conn, run_with_retry
from ap.utils import now_utc_iso

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


# ── Queue writer (called by route_signal_to_all_clients) ─────────────────────

def _write_signal_to_queue(signal: dict, client_id: str = "default"):
    """
    Write a signal to the SQLite trade_queue using ap/db.py's connection pool.
    Uses run_with_retry to handle Render's disk contention gracefully.
    """
    signal_id       = str(signal.get("signal_id") or uuid.uuid4())
    idempotency_key = f"{signal_id}:{client_id}"

    def _do_write():
        with ap_conn() as c:
            existing = c.execute(
                "SELECT id FROM trade_queue WHERE idempotency_key = ?",
                (idempotency_key,)
            ).fetchone()
            if existing:
                logger.debug(f"Signal {signal_id} already in queue — skipping duplicate")
                return
            c.execute(
                """INSERT INTO trade_queue
                   (client_id, signal_id, created_ts, status, payload, idempotency_key)
                   VALUES (?, ?, ?, 'NEW', ?, ?)""",
                (
                    client_id,
                    signal_id,
                    now_utc_iso(),
                    json.dumps(signal),
                    idempotency_key,
                )
            )

    try:
        run_with_retry(_do_write)
        logger.info(f"Signal {signal_id} written to trade_queue for client={client_id}")
    except Exception as e:
        logger.error(f"Failed to write signal to trade_queue: {e}")


class ClientRunner(threading.Thread):
    """
    One thread per client. Owns its own broker connection and APExecutionCore.
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

            # Start queue subscriber — this is the fix for gunicorn multi-worker
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

    def _start_queue_subscriber(self):
        """
        Poll trade_queue every 1s for NEW signals.
        Uses ap/db.py conn pool + run_with_retry — no raw sqlite3, no lock errors.
        """
        def _poll():
            logger.info(f"[{self.email}] Queue subscriber started")
            while not self.stopped.is_set():
                try:
                    def _fetch():
                        with ap_conn() as c:
                            return c.execute(
                                """SELECT id, signal_id, payload FROM trade_queue
                                   WHERE status = 'NEW'
                                   ORDER BY created_ts ASC LIMIT 3"""
                            ).fetchall()

                    rows = run_with_retry(_fetch)

                    for row in rows:
                        row_id    = row["id"]
                        signal_id = row["signal_id"]

                        # Claim atomically
                        def _claim(rid=row_id):
                            with ap_conn() as c:
                                c.execute(
                                    "UPDATE trade_queue SET status='PROCESSING', started_ts=? "
                                    "WHERE id=? AND status='NEW'",
                                    (now_utc_iso(), rid)
                                )
                        run_with_retry(_claim)

                        # Route to execution core
                        try:
                            signal = json.loads(row["payload"])
                            if self.core:
                                self.core.receive_signal(signal)

                            def _done(rid=row_id):
                                with ap_conn() as c:
                                    c.execute(
                                        "UPDATE trade_queue SET status='DONE', finished_ts=? WHERE id=?",
                                        (now_utc_iso(), rid)
                                    )
                            run_with_retry(_done)

                        except Exception as e:
                            logger.error(f"[{self.email}] Queue signal failed {signal_id}: {e}")
                            def _err(rid=row_id, err=str(e)):
                                with ap_conn() as c:
                                    c.execute(
                                        "UPDATE trade_queue SET status='ERROR', last_error=?, finished_ts=? WHERE id=?",
                                        (err, now_utc_iso(), rid)
                                    )
                            run_with_retry(_err)

                except Exception as e:
                    logger.error(f"[{self.email}] Queue subscriber error: {e}")

                time.sleep(1)

        t = threading.Thread(target=_poll, daemon=True, name=f"queue-sub-{self.email}")
        t.start()


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
    FIX: Writes to SQLite trade_queue instead of calling receive_signal() directly.
    This ensures delivery regardless of which gunicorn worker receives the HTTP request.
    The queue subscriber in each ClientRunner picks it up within 1 second.
    """
    signal_id = str(signal.get("signal_id") or uuid.uuid4())
    signal["signal_id"] = signal_id

    # Write to queue — subscriber in the correct worker process picks it up
    _write_signal_to_queue(signal, client_id="default")

    logger.info(f"Signal {signal_id} [{signal.get('ticker')}] written to queue for all clients")
