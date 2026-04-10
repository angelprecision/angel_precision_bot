# ap/queue.py - POSTGRES PRODUCTION VERSION
# ✅ All SQLite datetime() replaced with Postgres NOW() / INTERVAL
# ✅ Removed unused sqlite3 import
# ✅ JSONB payload handled correctly (Postgres returns dict, not string)
# ✅ ? placeholders throughout (wrapper converts to %s)
# ✅ INTERVAL parameterized safely via f-string (avoids Postgres version issues)
# ✅ Legacy worker skips signals with ev_score — those route to APExecutionCore

import time
import json
import os
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from ap.db import conn, run_with_retry, init_db
from ap.logger import get_logger
from ap.execution import process_signal
from ap.state import update_state
from ap.utils import now_utc_iso

log = get_logger("ap.queue")

MAX_TRADES_PER_HOUR    = int(os.getenv("MAX_TRADES_PER_HOUR", "12"))
POLL_INTERVAL          = float(os.getenv("QUEUE_POLL_INTERVAL", "1.0"))
PRICE_CHECK_INTERVAL   = float(os.getenv("PRICE_CHECK_INTERVAL", "0.5"))
ENTRY_TOLERANCE        = float(os.getenv("ENTRY_TOLERANCE", "0.05"))
PROCESSING_STALE_SECS  = int(os.getenv("PROCESSING_STALE_SECS", "900"))


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


def _is_triggered(direction: str, entry_price: float, current_price: float,
                  tolerance: float = ENTRY_TOLERANCE) -> bool:
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
    if isinstance(payload.get("trigger"), dict):
        entry = payload["trigger"].get("entry")
        if entry is not None:
            try:
                return float(entry)
            except Exception:
                pass
    for key in ("entry_price", "entry"):
        entry = payload.get(key)
        if entry is not None:
            try:
                return float(entry)
            except Exception:
                pass
    return None


def _trigger_source(payload: Dict[str, Any]) -> str:
    trg = payload.get("trigger")
    if isinstance(trg, dict):
        return str(trg.get("source") or "").lower().strip()
    return ""


def _parse_payload(raw) -> dict:
    """Postgres JSONB returns dict. SQLite returned string. Handle both."""
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        return json.loads(raw)
    return {}


def _check_rate_limit(client_id: str) -> bool:
    def _fn():
        with conn() as c:
            c.execute(
                """
                SELECT COUNT(*) AS n
                FROM positions
                WHERE client_id = ?
                  AND entry_ts >= NOW() - INTERVAL '1 hour'
                """,
                (client_id,),
            )
            row = c.fetchone()
            return int((row or {}).get("n") or 0)
    count = run_with_retry(_fn)
    if count >= MAX_TRADES_PER_HOUR:
        log.warning(f"Rate limit: {client_id} has {count}/{MAX_TRADES_PER_HOUR} trades")
        return True
    return False


def enqueue_signal(sig, client_id: str = "default",
                   idempotency_key: str | None = None) -> bool:
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

    def _ins():
        with conn() as c:
            c.execute(
                """
                INSERT INTO trade_queue (
                    client_id, signal_id, created_ts, status, payload, idempotency_key
                )
                VALUES (?, ?, ?, 'NEW', ?, ?)
                ON CONFLICT (idempotency_key) DO NOTHING
                """,
                (client_id, signal_id, _now_iso(),
                 json.dumps(payload), idempotency_key),
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
    def _fn():
        with conn() as c:
            c.execute(
                """
                UPDATE trade_queue
                SET status='NEW',
                    started_ts=NULL,
                    last_error=?,
                    created_ts=NOW()
                WHERE id=?
                """,
                (reason, job_id),
            )
    run_with_retry(_fn)


def _mark_job(job_id: int, status: str, *,
              result: dict | None = None, error: str | None = None):
    def _fn():
        with conn() as c:
            c.execute(
                """
                UPDATE trade_queue
                SET status=?,
                    finished_ts=NOW(),
                    result_json=?,
                    last_error=?
                WHERE id=?
                """,
                (status,
                 json.dumps(result) if result is not None else None,
                 error, job_id),
            )
    run_with_retry(_fn)


def _claim_one_job() -> Optional[dict]:
    """
    Claim exactly one LEGACY job safely.
    Skips signals with ev_score — those route to APExecutionCore.
    Uses f-string for INTERVAL to avoid Postgres parameterized INTERVAL issues.
    """
    # 1) Reclaim stale PROCESSING jobs — f-string INTERVAL is safe here (integer constant)
    def _reclaim():
        with conn() as c:
            c.execute(
                f"""
                UPDATE trade_queue
                SET status='NEW',
                    started_ts=NULL,
                    last_error=COALESCE(last_error,'') || ' | reclaimed_stale',
                    created_ts=NOW()
                WHERE status='PROCESSING'
                  AND started_ts IS NOT NULL
                  AND started_ts < NOW() - INTERVAL '{PROCESSING_STALE_SECS} seconds'
                  AND (payload->>'ev_score') IS NULL
                """,
            )
    run_with_retry(_reclaim)

    # 2) Find next NEW legacy job
    def _find():
        with conn() as c:
            c.execute(
                """
                SELECT id, client_id, signal_id, payload
                FROM trade_queue
                WHERE status='NEW'
                  AND (payload->>'ev_score') IS NULL
                ORDER BY created_ts ASC
                LIMIT 1
                """
            )
            return c.fetchone()
    job = run_with_retry(_find)
    if not job:
        return None

    job_id = int(job["id"])

    # 3) Claim atomically
    def _claim():
        with conn() as c:
            c.execute(
                """
                UPDATE trade_queue
                SET status='PROCESSING',
                    started_ts=NOW()
                WHERE id=?
                  AND status='NEW'
                """,
                (job_id,),
            )
            return c.rowcount
    claimed = run_with_retry(_claim)
    if claimed != 1:
        return None

    # 4) Re-read claimed row
    def _reread():
        with conn() as c:
            c.execute(
                "SELECT id, client_id, signal_id, payload FROM trade_queue WHERE id=?",
                (job_id,),
            )
            return c.fetchone()
    return run_with_retry(_reread)


def worker_loop(broker, poll_seconds: float = POLL_INTERVAL):
    init_db()
    log.info(f"🤖 Worker started (poll={poll_seconds}s)")

    while True:
        try:
            update_state({"last_heartbeat_ts": now_utc_iso()}, client_id="default")
        except Exception:
            pass

        job = None
        job_id = None

        try:
            job = _claim_one_job()
            if not job:
                time.sleep(poll_seconds)
                continue

            job_id    = int(job["id"])
            client_id = job["client_id"]
            signal_id = job["signal_id"]

            try:
                payload = _parse_payload(job["payload"])
            except Exception as e:
                log.error(f"Parse error: {e}")
                _mark_job(job_id, "REJECTED", error=f"parse_error: {e}")
                continue

            if _check_rate_limit(client_id):
                _requeue(job_id, f"rate_limited: {MAX_TRADES_PER_HOUR}/hour")
                time.sleep(poll_seconds)
                continue

            symbol      = payload.get("symbol") or payload.get("underlying")
            direction   = payload.get("direction") or payload.get("side")
            entry_price = _extract_entry_price(payload)
            source      = _trigger_source(payload)

            if source == "discord":
                log.info(f"🔔 Discord signal — skipping stock-price wait: {symbol} {direction}")
            else:
                if entry_price is not None and symbol and direction:
                    current_price = _get_stock_price(broker, symbol)
                    if current_price is None:
                        _requeue(job_id, "price_unavailable")
                        time.sleep(PRICE_CHECK_INTERVAL)
                        continue
                    if not _is_triggered(direction, entry_price, current_price):
                        log.debug(f"⏳ {symbol}: ${current_price:.2f} waiting entry={entry_price}")
                        _requeue(job_id, f"waiting: current={current_price:.2f} entry={entry_price}")
                        time.sleep(PRICE_CHECK_INTERVAL)
                        continue
                    log.info(f"🎯 {symbol} TRIGGERED: ${current_price:.2f} → entry={entry_price}")

            log.info(f"Executing: {signal_id}")
            result = process_signal(
                broker=broker,
                client_id=client_id,
                signal_payload=payload,
            )

            ok     = bool(result.get("ok"))
            status = "DONE" if ok else "REJECTED"
            error  = None if ok else (result.get("error") or result.get("reason") or "unknown_error")
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
