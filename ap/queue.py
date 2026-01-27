# ap/queue.py - FIXED VERSION
# Multi-client signal queue processing
# FIXED: uses 'payload' column (not payload_json)

import time
import json
from datetime import datetime, timezone

from ap.db import conn, run_with_retry, get_client_state, update_client_state
from ap.logger import get_logger
from ap.execution import process_signal

log = get_logger("ap.queue")

# Safety pacing (override via env)
MAX_TRADES_PER_HOUR = int(__import__("os").getenv("MAX_TRADES_PER_HOUR", "12"))


def _now_iso():
    """Get current time in ISO format"""
    return datetime.now(timezone.utc).isoformat()


def _json_dumps(x) -> str:
    """Serialize to compact JSON"""
    return json.dumps(x, separators=(",", ":"), ensure_ascii=False)


def enqueue_signal(sig, client_id: str, idempotency_key: str | None = None):
    """
    Insert signal into trade_queue for async processing.
    
    Args:
        sig: Signal model (has .model_dump() or .dict())
        client_id: Which client this signal is for
        idempotency_key: Unique key for deduping
    """
    payload = sig.model_dump() if hasattr(sig, "model_dump") else sig.dict()

    with conn() as c:
        def _ins():
            c.execute("""
                INSERT INTO trade_queue (
                    client_id, signal_id, payload, status, created_ts, idempotency_key
                )
                VALUES (?, ?, ?, 'NEW', ?, ?)
            """, (client_id, payload.get("signal_id"), _json_dumps(payload), _now_iso(), idempotency_key))
        
        try:
            run_with_retry(_ins)
        except Exception as e:
            # If unique constraint violation (duplicate), ignore safely
            msg = str(e).lower()
            if "unique" in msg or "constraint" in msg:
                log.info(f"Idempotent enqueue ignored: client_id={client_id} signal_id={payload.get('signal_id')}")
                return
            raise


def _too_many_recent_trades(client_id: str) -> bool:
    """Check if client has exceeded MAX_TRADES_PER_HOUR"""
    with conn() as c:
        row = run_with_retry(lambda: c.execute("""
            SELECT COUNT(*) AS n
            FROM positions
            WHERE client_id=?
              AND entry_ts >= datetime('now', '-1 hour')
        """, (client_id,)).fetchone())
        
        count = int(row["n"] or 0)
        if count >= MAX_TRADES_PER_HOUR:
            log.warning(f"Client {client_id} exceeded max trades per hour: {count} >= {MAX_TRADES_PER_HOUR}")
            return True
        return False


def worker_loop(broker):
    """
    Main worker loop: poll trade_queue and execute signals.
    Runs in background daemon thread.
    One signal at a time, per client.
    
    Args:
        broker: Broker instance
    """
    log.info("Worker loop started")
    
    while True:
        try:
            job = None
            
            # GET NEXT JOB FROM QUEUE
            with conn() as c:
                job = run_with_retry(lambda: c.execute("""
                    SELECT id, client_id, signal_id, payload
                    FROM trade_queue
                    WHERE status='NEW'
                    ORDER BY id ASC
                    LIMIT 1
                """).fetchone())

                if job:
                    # MARK AS PROCESSING
                    run_with_retry(lambda: c.execute("""
                        UPDATE trade_queue
                        SET status='PROCESSING', started_ts=?
                        WHERE id=?
                    """, (_now_iso(), job["id"])))

            if not job:
                # No work - sleep and retry
                time.sleep(0.25)
                continue

            job_id = job["id"]
            client_id = job["client_id"]
            signal_id = job["signal_id"]

            log.info(f"Processing job: id={job_id} client={client_id} signal={signal_id}")

            # PACING GATE: don't let one client spam
            if _too_many_recent_trades(client_id):
                log.warning(f"Pacing gate: client {client_id} too many recent trades")
                with conn() as c:
                    run_with_retry(lambda: c.execute("""
                        UPDATE trade_queue
                        SET status='NEW', started_ts=NULL, last_error=?
                        WHERE id=?
                    """, (f"pacing:max_trades_per_hour={MAX_TRADES_PER_HOUR}", job_id)))
                time.sleep(0.5)
                continue

            # PARSE PAYLOAD
            try:
                payload = json.loads(job["payload"] or "{}")
            except Exception as e:
                log.error(f"Failed to parse payload: {e}")
                with conn() as c:
                    run_with_retry(lambda: c.execute("""
                        UPDATE trade_queue
                        SET status='REJECTED', finished_ts=?, last_error=?
                        WHERE id=?
                    """, (_now_iso(), f"parse_error: {str(e)}", job_id)))
                continue

            # EXECUTE SIGNAL
            result = process_signal(broker=broker, client_id=client_id, signal_payload=payload)

            # UPDATE QUEUE WITH RESULT
            with conn() as c:
                run_with_retry(lambda: c.execute("""
                    UPDATE trade_queue
                    SET status=?, finished_ts=?, result_json=?, last_error=?
                    WHERE id=?
                """, (
                    "DONE" if result.get("ok") else "REJECTED",
                    _now_iso(),
                    _json_dumps(result),
                    None if result.get("ok") else result.get("error") or result.get("reason"),
                    job_id
                )))

            if result.get("ok"):
                log.info(f"✅ Signal executed: client={client_id} signal={signal_id}")
            else:
                log.warning(f"⚠️  Signal rejected: client={client_id} reason={result.get('error')}")

        except Exception as e:
            log.error(f"Worker loop error: {e}")
            
            # Try to mark job as error if we have one
            try:
                if "job_id" in locals() and job_id:
                    with conn() as c:
                        run_with_retry(lambda: c.execute("""
                            UPDATE trade_queue
                            SET status='ERROR', finished_ts=?, last_error=?
                            WHERE id=?
                        """, (_now_iso(), str(e), job_id)))
            except Exception as inner_e:
                log.error(f"Failed to mark job as error: {inner_e}")
            
            time.sleep(0.5)
