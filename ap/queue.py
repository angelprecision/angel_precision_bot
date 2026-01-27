# ap/queue.py
# ✅ DB-backed queue (trade_queue)
# ✅ Per-client broker cache
# ✅ Idempotency: already_processed_signal / mark_signal_processed
# ✅ Best-effort reconcile tick
# ✅ Throttle handling (requeues job cleanly)

import time
from typing import Dict

from ap.db import (
    conn,
    run_with_retry,
    already_processed_signal,
    mark_signal_processed,
)
from ap.utils import now_utc_iso, json_dumps, json_loads
from ap.logger import get_logger
from ap.models import Signal
from ap.execution import process_signal
from ap.state import update_state  # global heartbeat only
from ap.reconcile import reconcile_once

log = get_logger("ap.queue")

DEFAULT_CLIENT_ID = "default"
_BROKERS: Dict[str, object] = {}  # populated lazily by _broker_for()


def _broker_for(client_id: str, default_broker):
    """
    MVP: one broker instance shared per worker.
    If you later support client-specific broker creds, build them here.
    """
    if client_id in _BROKERS:
        return _BROKERS[client_id]
    _BROKERS[client_id] = default_broker
    return default_broker


def enqueue_signal(signal: Signal, client_id: str = DEFAULT_CLIENT_ID):
    """
    Enqueue a signal for a specific client.
    """
    with conn() as c:
        run_with_retry(lambda: c.execute(
            "INSERT INTO trade_queue (client_id, signal_id, created_ts, status, payload) VALUES (?,?,?,?,?)",
            (client_id, signal.signal_id, now_utc_iso(), "NEW", json_dumps(signal.model_dump())),
        ))


def fetch_next_job():
    """
    Atomically claim the next NEW job.
    """
    with conn() as c:
        row = run_with_retry(lambda: c.execute("""
            SELECT * FROM trade_queue
            WHERE status='NEW'
            ORDER BY id ASC
            LIMIT 1
        """).fetchone())

        if not row:
            return None

        cur = run_with_retry(lambda: c.execute(
            "UPDATE trade_queue SET status='PROCESSING' WHERE id=? AND status='NEW'",
            (row["id"],)
        ))

        if cur.rowcount != 1:
            return None

        return dict(row)


def complete_job(job_id: int, ok: bool, decision: str, reason: str = "", details=None):
    with conn() as c:
        run_with_retry(lambda: c.execute("""
            UPDATE trade_queue
            SET status=?, decision=?, reason=?, details=?
            WHERE id=?
        """, (
            "DONE" if ok else "REJECTED",
            decision,
            reason,
            json_dumps(details) if details is not None else None,
            job_id
        )))


def requeue_job(job_id: int, reason: str, details=None):
    """
    Put a job back to NEW (simple MVP throttle/retry).
    """
    with conn() as c:
        run_with_retry(lambda: c.execute("""
            UPDATE trade_queue
            SET status='NEW', decision=?, reason=?, details=?
            WHERE id=?
        """, (
            "REQUEUED",
            reason,
            json_dumps(details) if details is not None else None,
            job_id
        )))


def worker_loop(_unused_broker=None, poll_seconds: float = 0.5):
    """
    Gunicorn worker loop. Uses per-client brokers based on job.client_id.
    """
    log.info("Worker loop running")
    last_tick = 0.0

    while True:
        now = time.time()

        # heartbeat + reconcile tick
        if now - last_tick > 5.0:
            update_state({"last_heartbeat_ts": now_utc_iso()})

            for cid, b in list(_BROKERS.items()):
                try:
                    reconcile_once(b, limit=50)
                except Exception:
                    log.exception(f"Reconcile loop error (client_id={cid})")

            last_tick = now

        job = fetch_next_job()
        if not job:
            time.sleep(poll_seconds)
            continue

        job_id = int(job["id"])
        client_id = (job.get("client_id") or DEFAULT_CLIENT_ID).strip()
        signal_id = (job.get("signal_id") or "").strip()

        try:
            # Idempotency (DB-backed)
            if signal_id and already_processed_signal(signal_id, client_id=client_id):
                complete_job(job_id, ok=True, decision="SKIP_DUP", reason="already_processed")
                continue

            payload = json_loads(job.get("payload") or "{}")
            sig = Signal(**payload)

            broker = _broker_for(client_id, _unused_broker)

            result = process_signal(sig, broker, client_id=client_id)

            if result.get("ok"):
                if signal_id:
                    mark_signal_processed(signal_id, client_id=client_id, decision="EXECUTED")
                complete_job(job_id, ok=True, decision="EXECUTED", reason="", details=result)
            else:
                reason = result.get("reason", "REJECTED")
                # If throttled, requeue instead of rejecting permanently
                if reason == "THROTTLED":
                    requeue_job(job_id, reason="THROTTLED", details=result)
                    time.sleep(2.0)
                else:
                    if signal_id:
                        mark_signal_processed(signal_id, client_id=client_id, decision="REJECTED", reason=reason)
                    complete_job(job_id, ok=False, decision="REJECTED", reason=reason, details=result)

        except Exception as e:
            log.exception(f"Job failed (id={job_id}, client_id={client_id}): {e}")
            complete_job(job_id, ok=False, decision="ERROR", reason="exception", details={"err": str(e)})
            time.sleep(0.25)
