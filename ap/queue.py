import time
from ap.db import conn
from ap.utils import now_utc_iso, json_dumps, json_loads
from ap.logger import get_logger
from ap.models import Signal
from ap.execution import process_signal
from ap.state import update_state

log = get_logger("ap.queue")

def enqueue_signal(signal: Signal):
    with conn() as c:
        c.execute(
            "INSERT INTO trade_queue (signal_id, created_ts, status, payload) VALUES (?,?,?,?)",
            (signal.signal_id, now_utc_iso(), "NEW", json_dumps(signal.model_dump())),
        )

def fetch_next_job():
    with conn() as c:
        c.execute("BEGIN IMMEDIATE;")
        row = c.execute("""
            SELECT * FROM trade_queue
            WHERE status='NEW'
            ORDER BY id ASC
            LIMIT 1
        """).fetchone()
        if not row:
            c.execute("COMMIT;")
            return None
        c.execute("UPDATE trade_queue SET status='PROCESSING' WHERE id=?", (row["id"],))
        c.execute("COMMIT;")
        return dict(row)

def complete_job(job_id: int, ok: bool, decision: str, reason: str = "", details=None):
    with conn() as c:
        c.execute("""
            UPDATE trade_queue
            SET status=?, decision=?, reason=?
            WHERE id=?
        """, ("DONE" if ok else "REJECTED", decision, reason, job_id))

def worker_loop(broker, poll_seconds: float = 0.5):
    log.info("Worker started")
    while True:
        update_state({"last_heartbeat_ts": now_utc_iso()})
        job = fetch_next_job()
        if not job:
            time.sleep(poll_seconds)
            continue
        try:
            payload = json_loads(job["payload"])
            signal = Signal(**payload)
            res = process_signal(signal, broker)
            complete_job(job["id"], bool(res.get("ok")), decision="EXECUTED" if res.get("ok") else "REJECTED", reason=res.get("reason", ""), details=res)
            log.info(f"Processed signal={signal.signal_id} ok={res.get('ok')} reason={res.get('reason')}")
        except Exception as e:
            complete_job(job["id"], False, decision="ERROR", reason=str(e))
            log.exception("Job failed")

