# ap/queue.py
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
from ap.client_manager import get_client_broker

log = get_logger("ap.queue")

DEFAULT_CLIENT_ID = "default"

# Per-worker broker cache (good enough for now)
_BROKERS: Dict[str, object] = {}


def _broker_for(client_id: str):
    b = _BROKERS.get(client_id)
    if b is not None:
        return b
    b = get_client_broker(client_id)
    _BROKERS[client_id] = b
    return b


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


def worker_loop(_unused_broker=None, poll_seconds: float = 0.5):
    """
    Gunicorn worker loop. Uses per-client brokers based on job.client_id.
    NOTE: signature kept compatible with your current app.py thread start.
    """
    log.info("Worker started (multi-client)")
    last_tick = 0.0

    while True:
        now = time.time()

        # Every 10 seconds: global heartbeat + reconcile best-effort
        if now - last_tick >= 10.0:
            try:
                # Global heartbeat for /health
                update_state({"last_heartbeat_ts": now_utc_iso()})
            except Exception:
                log.exception("Heartbeat update failed")

            # Reconcile each cached broker (best-effort)
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

        client_id = (job.get("client_id") or DEFAULT_CLIENT_ID)

        try:
            payload = json_loads(job["payload"])
            signal = Signal(**payload)

            # Dedupe
            if already_processed_signal(signal.signal_id):
                res = {"ok": True, "reason": "DEDUPED", "signal_id": signal.signal_id, "client_id": client_id}
                complete_job(job["id"], True, decision="DEDUPED", reason="DEDUPED", details=res)
                log.info(f"Processed signal={signal.signal_id} client_id={client_id} ok=True reason=DEDUPED")
                continue

            mark_signal_processed(signal.signal_id)

            broker = _broker_for(client_id)

            res = process_signal(signal, broker, client_id=client_id)

            complete_job(
                job["id"],
                bool(res.get("ok")),
                decision="EXECUTED" if res.get("ok") else "REJECTED",
                reason=res.get("reason", ""),
                details=res,
            )
            log.info(f"Processed signal={signal.signal_id} client_id={client_id} ok={res.get('ok')} reason={res.get('reason')}")

        except Exception as e:
            complete_job(job["id"], False, decision="ERROR", reason=str(e), details={"error": str(e), "client_id": client_id})
            log.exception("Job failed")
