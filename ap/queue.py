# ap/queue.py
import time

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
from ap.state import update_state
from ap.reconcile import reconcile_once

log = get_logger("ap.queue")


def enqueue_signal(signal: Signal):
    with conn() as c:
        run_with_retry(lambda: c.execute(
            "INSERT INTO trade_queue (signal_id, created_ts, status, payload) VALUES (?,?,?,?)",
            (signal.signal_id, now_utc_iso(), "NEW", json_dumps(signal.model_dump())),
        ))


def fetch_next_job():
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


def worker_loop(broker, poll_seconds: float = 0.5):
    log.info("Worker started")
    last_tick = 0.0  # heartbeat + reconcile timer

    while True:
        now = time.time()

        # Every 10 seconds: heartbeat + reconcile
        if now - last_tick >= 10.0:
            try:
                update_state({"last_heartbeat_ts": now_utc_iso()})
            except Exception:
                log.exception("Heartbeat update failed")

            try:
                reconcile_once(broker, limit=50)
            except Exception:
                log.exception("Reconcile loop error")

            last_tick = now

        job = fetch_next_job()
        if not job:
            time.sleep(poll_seconds)
            continue

        try:
            payload = json_loads(job["payload"])
            signal = Signal(**payload)

            # Dedupe
            if already_processed_signal(signal.signal_id):
                res = {"ok": True, "reason": "DEDUPED", "signal_id": signal.signal_id}
                complete_job(job["id"], True, decision="DEDUPED", reason="DEDUPED", details=res)
                log.info(f"Processed signal={signal.signal_id} ok=True reason=DEDUPED")
                continue

            mark_signal_processed(signal.signal_id)

            res = process_signal(signal, broker)

            complete_job(
                job["id"],
                bool(res.get("ok")),
                decision="EXECUTED" if res.get("ok") else "REJECTED",
                reason=res.get("reason", ""),
                details=res,
            )
            log.info(f"Processed signal={signal.signal_id} ok={res.get('ok')} reason={res.get('reason')}")

        except Exception as e:
            complete_job(job["id"], False, decision="ERROR", reason=str(e), details={"error": str(e)})
            log.exception("Job failed")
