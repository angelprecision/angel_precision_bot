cat > ap/queue.py << 'EOF'
# ap/queue.py - PRODUCTION READY (no processed_ts, fixed idempotency)
import time
import json
import os
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from ap.db import conn, run_with_retry
from ap.logger import get_logger
from ap.execution import process_signal

log = get_logger("ap.queue")

# Configuration
MAX_TRADES_PER_HOUR = int(os.getenv("MAX_TRADES_PER_HOUR", "12"))
POLL_INTERVAL = float(os.getenv("QUEUE_POLL_INTERVAL", "1.0"))
PRICE_CHECK_INTERVAL = float(os.getenv("PRICE_CHECK_INTERVAL", "0.5"))
ENTRY_TOLERANCE = float(os.getenv("ENTRY_TOLERANCE", "0.05"))


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_dumps(data: Any) -> str:
    return json.dumps(data, separators=(",", ":"), ensure_ascii=False)


def _normalize_direction(direction: str) -> str:
    if not direction:
        return ""
    direction = direction.upper().strip()
    if direction in ("BUY", "LONG", "CALLS", "CALL"):
        return "CALL"
    if direction in ("SELL", "SHORT", "PUTS", "PUT"):
        return "PUT"
    return direction


def _is_triggered(direction: str, entry_price: float, current_price: float, 
                  tolerance: float = ENTRY_TOLERANCE) -> bool:
    direction = _normalize_direction(direction)
    if direction == "CALL":
        return current_price + tolerance >= entry_price
    if direction == "PUT":
        return current_price - tolerance <= entry_price
    return False


def _get_stock_price(broker, symbol: str) -> Optional[float]:
    if not symbol:
        return None
    
    try:
        if hasattr(broker, "get_quote"):
            quote_data = broker.get_quote(symbol)
            if isinstance(quote_data, dict):
                last = (quote_data.get("last") or 
                       quote_data.get("lastPrice") or 
                       quote_data.get("mark"))
                if last and float(last) > 0:
                    return float(last)
        
        if hasattr(broker, "get_last_price"):
            price = broker.get_last_price(symbol)
            if price and float(price) > 0:
                return float(price)
        
        if hasattr(broker, "quote"):
            quote_data = broker.quote(symbol)
            if isinstance(quote_data, dict):
                last = quote_data.get("last") or quote_data.get("lastPrice")
                if last and float(last) > 0:
                    return float(last)
        
        return None
        
    except Exception as e:
        log.warning(f"Price fetch error for {symbol}: {e}")
        return None


def _extract_entry_price(payload: Dict[str, Any]) -> Optional[float]:
    # Check trigger.entry first
    if "trigger" in payload and isinstance(payload["trigger"], dict):
        entry = payload["trigger"].get("entry")
        if entry:
            try:
                return float(entry)
            except:
                pass
    
    # Check top-level
    entry = payload.get("entry_price") or payload.get("entry")
    if entry:
        try:
            return float(entry)
        except:
            pass
    
    return None


def _check_rate_limit(client_id: str) -> bool:
    with conn() as c:
        row = run_with_retry(lambda: c.execute("""
            SELECT COUNT(*) AS n
            FROM positions
            WHERE client_id = ?
              AND entry_ts >= datetime('now', '-1 hour')
        """, (client_id,)).fetchone())
        
        count = int(row["n"] or 0)
        if count >= MAX_TRADES_PER_HOUR:
            log.warning(f"Rate limit: {client_id} has {count}/{MAX_TRADES_PER_HOUR} trades")
            return True
        return False


def enqueue_signal(signal: Dict[str, Any], client_id: str = "default", 
                   idempotency_key: Optional[str] = None) -> bool:
    # Handle model objects
    if hasattr(signal, "model_dump"):
        payload = signal.model_dump()
    elif hasattr(signal, "dict"):
        payload = signal.dict()
    else:
        payload = signal
    
    signal_id = payload.get("signal_id", f"signal_{_now_iso()}")
    
    # FIX: Auto-generate idempotency_key if not provided
    if not idempotency_key:
        idempotency_key = f"{client_id}:{signal_id}"
    
    try:
        with conn() as c:
            run_with_retry(lambda: c.execute("""
                INSERT INTO trade_queue (
                    client_id,
                    signal_id,
                    payload,
                    status,
                    created_ts,
                    idempotency_key
                )
                VALUES (?, ?, ?, 'NEW', ?, ?)
            """, (
                client_id,
                signal_id,
                _json_dumps(payload),
                _now_iso(),
                idempotency_key
            )))
        
        log.info(f"✅ Enqueued: {signal_id}")
        return True
        
    except Exception as e:
        error_msg = str(e).lower()
        if "unique" in error_msg or "constraint" in error_msg:
            log.debug(f"Duplicate ignored: {signal_id}")
            return False
        log.error(f"Enqueue failed: {e}")
        raise


def worker_loop(broker, poll_seconds: float = POLL_INTERVAL):
    log.info(f"🤖 Worker started (poll={poll_seconds}s)")
    
    while True:
        job = None
        job_id = None
        
        try:
            # GET NEXT JOB
            with conn() as c:
                job = run_with_retry(lambda: c.execute("""
                    SELECT id, client_id, signal_id, payload
                    FROM trade_queue
                    WHERE status = 'NEW'
                    ORDER BY created_ts ASC
                    LIMIT 1
                """).fetchone())
            
            if not job:
                time.sleep(poll_seconds)
                continue
            
            job_id = job["id"]
            client_id = job["client_id"]
            signal_id = job["signal_id"]
            
            # MARK PROCESSING
            with conn() as c:
                run_with_retry(lambda: c.execute("""
                    UPDATE trade_queue
                    SET status = 'PROCESSING', started_ts = ?
                    WHERE id = ?
                """, (_now_iso(), job_id)))
            
            # PARSE PAYLOAD
            try:
                payload = json.loads(job["payload"])
            except Exception as e:
                log.error(f"Parse error: {e}")
                with conn() as c:
                    run_with_retry(lambda: c.execute("""
                        UPDATE trade_queue
                        SET status = 'REJECTED', 
                            finished_ts = ?,
                            last_error = ?
                        WHERE id = ?
                    """, (_now_iso(), f"parse_error: {str(e)}", job_id)))
                continue
            
            # RATE LIMIT
            if _check_rate_limit(client_id):
                with conn() as c:
                    run_with_retry(lambda: c.execute("""
                        UPDATE trade_queue
                        SET status = 'NEW',
                            started_ts = NULL,
                            last_error = ?
                        WHERE id = ?
                    """, (f"rate_limited: {MAX_TRADES_PER_HOUR}/hour", job_id)))
                time.sleep(poll_seconds)
                continue
            
            # EXTRACT DATA
            symbol = payload.get("symbol") or payload.get("underlying")
            direction = payload.get("direction") or payload.get("side")
            entry_price = _extract_entry_price(payload)
            
            # PRICE TRIGGER CHECK
            if entry_price and symbol and direction:
                current_price = _get_stock_price(broker, symbol)
                
                if current_price is None:
                    with conn() as c:
                        run_with_retry(lambda: c.execute("""
                            UPDATE trade_queue
                            SET status = 'NEW',
                                started_ts = NULL,
                                last_error = ?
                            WHERE id = ?
                        """, ("price_unavailable", job_id)))
                    time.sleep(PRICE_CHECK_INTERVAL)
                    continue
                
                triggered = _is_triggered(direction, entry_price, current_price)
                
                if not triggered:
                    log.debug(f"⏳ {symbol}: ${current_price:.2f} waiting ${entry_price}")
                    with conn() as c:
                        run_with_retry(lambda: c.execute("""
                            UPDATE trade_queue
                            SET status = 'NEW',
                                started_ts = NULL,
                                last_error = ?
                            WHERE id = ?
                        """, (f"waiting: current=${current_price:.2f}", job_id)))
                    time.sleep(PRICE_CHECK_INTERVAL)
                    continue
                
                log.info(f"🎯 {symbol} TRIGGERED: ${current_price:.2f} → ${entry_price}")
            
            # EXECUTE
            log.info(f"Executing: {signal_id}")
            result = process_signal(broker, client_id, payload)
            
            # UPDATE RESULT (NO processed_ts - only finished_ts)
            status = "DONE" if result.get("ok") else "REJECTED"
            error = None if result.get("ok") else (result.get("error") or result.get("reason"))
            
            with conn() as c:
                run_with_retry(lambda: c.execute("""
                    UPDATE trade_queue
                    SET status = ?,
                        finished_ts = ?,
                        result_json = ?,
                        last_error = ?
                    WHERE id = ?
                """, (
                    status,
                    _now_iso(),
                    _json_dumps(result),
                    error,
                    job_id
                )))
            
            # LOG
            if result.get("ok"):
                log.info(f"✅ {signal_id}: {result.get('contract')}")
            else:
                log.warning(f"❌ {signal_id}: {error}")
        
        except Exception as e:
            log.error(f"Worker error: {e}", exc_info=True)
            if job_id:
                try:
                    with conn() as c:
                        run_with_retry(lambda: c.execute("""
                            UPDATE trade_queue
                            SET status = 'ERROR',
                                finished_ts = ?,
                                last_error = ?
                            WHERE id = ?
                        """, (_now_iso(), str(e), job_id)))
                except:
                    pass
            time.sleep(poll_seconds)
EOF

echo "✅ Production queue installed"
