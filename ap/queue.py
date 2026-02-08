# ap/queue.py - PRODUCTION READY
# Fixes:
# ✅ init_db() at startup (prevents "no such table: trade_queue")
# ✅ Safe job claiming (avoids double-processing with multiple workers)
# ✅ Anti-starvation: requeue with updated created_ts when waiting/price/rate-limited
# ✅ Correct entry=0 handling (_extract_entry_price uses is not None)
# ✅ Uses your existing trade_queue schema/columns from ap/db.py

import time
import json
import os
import sqlite3
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from ap.db import conn, run_with_retry, init_db
from ap.logger import get_logger
from ap.execution import process_signal
from ap.state import update_state
from ap.utils import now_utc_iso


log = get_logger("ap.queue")

MAX_TRADES_PER_HOUR = int(os.getenv("MAX_TRADES_PER_HOUR", "12"))
POLL_INTERVAL = float(os.getenv("QUEUE_POLL_INTERVAL", "1.0"))
PRICE_CHECK_INTERVAL = float(os.getenv("PRICE_CHECK_INTERVAL", "0.5"))
ENTRY_TOLERANCE = float(os.getenv("ENTRY_TOLERANCE", "0.05"))

# If a job is stuck in PROCESSING too long (worker crashed), reclaim it
PROCESSING_STALE_SECS = int(os.getenv("PROCESSING_STALE_SECS", "900"))  # 15 min


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_dumps(data: Any) -> str:
    return json.dumps(data, separators=(",", ":"), ensure_ascii=False)


def _normalize_direction(direction: str) -> str:
    if not direction:
        return ""
    d = direction.upper().strip()
    if d in ("BUY", "LONG", "CALLS", "CALL"):
        return "CALL"
    if d in ("SELL", "SHORT", "PUTS", "PUT"):
        return "PUT"
    return d


def _is_triggered(direction: str, entry_price: float, current_price: float, tolerance: float = ENTRY_TOLERANCE) -> bool:
    d = _normalize_direction(direction)
    if d == "CALL":
        return current_price + tolerance >= entry_price
    if d == "PUT":
        return current_price - tolerance <= entry_price
    return False


def _get_stock_price(broker, symbol: str) -> Optional[float]:
    if not symbol:
        return None
    try:
        if hasattr(broker, "get_quote"):
            q = broker.get_quote(symbol)
            if isinstance(q, dict):
                last = q.get("last") or q.get("lastPrice") or q.get("mark")
                if last is not None and float(last) > 0:
                    return float(last)

        if hasattr(broker, "get_last_price"):
            p = broker.get_last_price(symbol)
            if p is not None and float(p) > 0:
                return float(p)

        if hasattr(broker, "quote"):
            q = broker.quote(symbol)
            if isinstance(q, dict):
                last = q.get("last") or q.get("lastPrice") or q.get("mark")
                if last is not None and float(last) > 0:
                    return float(last)

        return None
    except Exception as e:
        log.warning(f"Price fetch error for {symbol}: {e}")
        return None


def _extract_entry_price(payload: Dict[str, Any]) -> Optional[float]:
    """
    Correctly handle entry=0.0 (0 is a valid value so we check is not None)
    """
    if isinstance(payload.get("trigger"), dict):
        entry = payload["trigger"].get("entry")
        if entry is not None:
            try:
                return float(entry)
            except Exception:
                pass

    entry = payload.get("entry_price")
    if entry is not None:
        try:
            return float(entry)
        except Exception:
            pass

    entry = payload.get("entry")
    if entry is not None:
        try:
            return float(entry)
        except Exception:
            pass

    return None


def _check_rate_limit(client_id: str) -> bool:
    with conn() as c:
        row = run_with_retry(lambda: c.execute(
            """
            SELECT COUNT(*) AS n
            FROM positions
            WHERE client_id = ?
              AND entry_ts >= datetime('now', '-1 hour')
            """,
            (client_id,),
        ).fetchone())
        count = int(row["n"] or 0)
        if count >= MAX_TRADES_PER_HOUR:
            log.warning(f"Rate limit: {client_id} has {count}/{MAX_TRADES_PER_HOUR} trades")
            return True
        return False


def enqueue_signal(sig, client_id: str = "default", idempotency_key: str | None = None) -> bool:
    """
    Insert signal into trade_queue for async processing.
    Accepts dict OR pydantic model.
    Matches ap/db.py trade_queue columns.
    """
    if isinstance(sig, dict):
        payload = sig
    elif hasattr(sig, "model_dump"):
        payload = sig.model_dump()
    elif hasattr(sig, "dict"):
        payload = sig.dict()
    else:
        raise TypeError(f"Unsupported signal type: {type(sig)}")

    signal_id = payload.get("signal_id") or f"signal_{_now_iso()}"

    if not idempotency_key:
        idempotency_key = f"{client_id}:{signal_id}"

    with conn() as c:
        def _ins():
            c.execute(
                """
                INSERT INTO trade_queue (
                    client_id, signal_id, created_ts, status, payload, idempotency_key
                )
                VALUES (?, ?, ?, 'NEW', ?, ?)
                """,
                (client_id, signal_id, _now_iso(), _json_dumps(payload), idempotency_key),
            )

        try:
            run_with_retry(_ins)
            log.info(f"✅ Enqueued: {signal_id}")
            return True
        except Exception as e:
            msg = str(e).lower()
            if "unique" in msg or "constraint" in msg:
                log.debug(f"Duplicate ignored: {signal_id}")
                return False
            raise


def _requeue(job_id: int, reason: str):
    """
    Anti-starvation: put back to NEW, clear started_ts, bump created_ts so we don't hammer the same job.
    """
    with conn() as c:
        run_with_retry(lambda: c.execute(
            """
            UPDATE trade_queue
            SET status='NEW',
                started_ts=NULL,
                last_error=?,
                created_ts=?
            WHERE id=?
            """,
            (reason, _now_iso(), job_id),
        ))


def _mark_job(job_id: int, status: str, *, result: dict | None = None, error: str | None = None):
    with conn() as c:
        run_with_retry(lambda: c.execute(
            """
            UPDATE trade_queue
            SET status=?,
                finished_ts=?,
                result_json=?,
                last_error=?
            WHERE id=?
            """,
            (status, _now_iso(), _json_dumps(result) if result is not None else None, error, job_id),
        ))


def _claim_one_job() -> Optional[sqlite3.Row]:
    """
    Claim exactly one job safely.
    - Prefer NEW jobs.
    - Also reclaim stale PROCESSING jobs (worker crash) older than PROCESSING_STALE_SECS.
    """
    with conn() as c:
        # 1) Reclaim stale PROCESSING jobs
        run_with_retry(lambda: c.execute(
            """
            UPDATE trade_queue
            SET status='NEW',
                started_ts=NULL,
                last_error=COALESCE(last_error,'') || ' | reclaimed_stale',
                created_ts=?
            WHERE status='PROCESSING'
              AND started_ts IS NOT NULL
              AND started_ts < datetime('now', ?)
            """,
            (_now_iso(), f"-{PROCESSING_STALE_SECS} seconds"),
        ))

        # 2) Find next NEW job
        job = run_with_retry(lambda: c.execute(
            """
            SELECT id, client_id, signal_id, payload
            FROM trade_queue
            WHERE status='NEW'
            ORDER BY created_ts ASC
            LIMIT 1
            """
        ).fetchone())

        if not job:
            return None

        job_id = int(job["id"])

        # 3) Claim it (atomic-ish): only claim if still NEW
        def _claim():
            cur = c.execute(
                """
                UPDATE trade_queue
                SET status='PROCESSING',
                    started_ts=?
                WHERE id=?
                  AND status='NEW'
                """,
                (_now_iso(), job_id),
            )
            return cur.rowcount

        claimed = run_with_retry(_claim)
        if claimed != 1:
            return None

        # Re-read claimed row (payload etc)
        return run_with_retry(lambda: c.execute(
            "SELECT id, client_id, signal_id, payload FROM trade_queue WHERE id=?",
            (job_id,),
        ).fetchone())


def worker_loop(broker, poll_seconds: float = POLL_INTERVAL):
    # Heartbeat (proves worker is alive)
    try:
    update_state({"last_heartbeat_ts": now_utc_iso()}, client_id="default")
        except Exception:
            pass
    
    # ✅ Critical: ensures trade_queue exists in the DB file the worker is using
    init_db()

    log.info(f"🤖 Worker started (poll={poll_seconds}s)")

    while True:
        job = None
        job_id = None

        try:
            job = _claim_one_job()
            if not job:
                time.sleep(poll_seconds)
                continue

            job_id = int(job["id"])
            client_id = job["client_id"]
            signal_id = job["signal_id"]

            try:
                payload = json.loads(job["payload"])
            except Exception as e:
                log.error(f"Parse error: {e}")
                _mark_job(job_id, "REJECTED", error=f"parse_error: {e}")
                continue

            # Rate-limit anti-starvation
            if _check_rate_limit(client_id):
                _requeue(job_id, f"rate_limited: {MAX_TRADES_PER_HOUR}/hour")
                time.sleep(poll_seconds)
                continue

            symbol = payload.get("symbol") or payload.get("underlying")
            direction = payload.get("direction") or payload.get("side")
            entry_price = _extract_entry_price(payload)

            # If entry price is supplied, wait for trigger
            if entry_price is not None and symbol and direction:
                current_price = _get_stock_price(broker, symbol)

                if current_price is None:
                    _requeue(job_id, "price_unavailable")
                    time.sleep(PRICE_CHECK_INTERVAL)
                    continue

                if not _is_triggered(direction, entry_price, current_price):
                    log.debug(f"⏳ {symbol}: ${current_price:.2f} waiting entry=${entry_price}")
                    _requeue(job_id, f"waiting: current=${current_price:.2f} entry={entry_price}")
                    time.sleep(PRICE_CHECK_INTERVAL)
                    continue

                log.info(f"🎯 {symbol} TRIGGERED: ${current_price:.2f} → entry={entry_price}")

            log.info(f"Executing: {signal_id}")

            # ✅ Call execution with keyword args (matches your execution.py)
            result = process_signal(
                broker=broker,
                client_id=client_id,
                signal_payload=payload,
            )

            ok = bool(result.get("ok"))
            status = "DONE" if ok else "REJECTED"
            error = None if ok else (result.get("error") or result.get("reason") or "unknown_error")

            _mark_job(job_id, status, result=result, error=error)

            if ok:
                log.info(f"✅ {signal_id}: {result.get('contract')}")
            else:
                log.warning(f"❌ {signal_id}: {error}")

        except Exception as e:
            log.error(f"Worker error: {e}", exc_info=True)
            if job_id is not None:
                try:
                    _mark_job(job_id, "ERROR", error=str(e))
                except Exception:
                    pass
            time.sleep(poll_seconds)

