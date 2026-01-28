cd /Users/azmareyawilson/Desktop/latest_bot

cat > ap/queue.py << 'EOF'
# ap/queue.py - PRODUCTION READY (anti-starvation, correct entry=0 handling)
import time
import json
import os
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from ap.db import conn, run_with_retry
from ap.logger import get_logger
from ap.execution import process_signal

log = get_logger("ap.queue")

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
    """FIX 1: Correctly handle entry=0.0"""
    # Check trigger.entry first
    if "trigger" in payload and isinstance(payload["trigger"], dict):
        entry = payload["trigger"].get("entry")
        if entry is not None:  # Changed from 'if entry'
            try:
                return float(entry)
            except:
                pass
    
    # Check top-level entry_price
    entry = payload.get("entry_price")
    if entry is not None:  # Changed from 'if entry'
        try:
            return float(entry)
        except:
            pass
    
    # Check top-level entry
    entry = payload.get("entry")
    if entry is not None:  # Changed from 'if entry'
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


def enqueue_signal(sig, client_id: str = "default", idempotency_key: str | None = None):
    """
    Insert signal into trade_queue for async processing.
    Accepts dict OR pydantic model.
    """

    # ✅ FIX: handle dict safely
    if isinstance(sig, dict):
        payload = sig
    elif hasattr(sig, "model_dump"):
        payload = sig.model_dump()
    elif hasattr(sig, "dict"):
        payload = sig.dict()
    else:
        raise TypeError(f"Unsupported signal type: {type(sig)}")

    # Always have a signal_id
    signal_id = payload.get("signal_id") or f"signal_{_now_iso()}"

    # ✅ FIX: default idempotency key if missing
    if not idempotency_key:
        idempotency_key = f"{client_id}:{signal_id}"

    with conn() as c:
        def _ins():
            c.execute("""
                INSERT INTO trade_queue (
                    client_id, signal_id, payload, status, created_ts, idempotency_key
                )
                VALUES (?, ?, ?, 'NEW', ?, ?)
            """, (client_id, signal_id, _json_dumps(payload), _now_iso(), idempotency_key))

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


def worker_loop(broker, poll_seconds: float = POLL_INTERVAL):
    log.info(f"🤖 Worker started (poll={poll_seconds}s)")
    
    while True:
        job = None
        job_id = None
        
        try:
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
            
            with conn() as c:
                run_with_retry(lambda: c.execute("""
                    UPDATE trade_queue
                    SET status = 'PROCESSING', started_ts = ?
                    WHERE id = ?
                """, (_now_iso(), job_id)))
            
            try:
                payload = json.loads(job["payload"])
            except Exception as e:
                log.error(f"Parse error: {e}")
                with conn() as c:
                    run_with_retry(lambda: c.execute("""
                        UPDATE trade_queue
                        SET status = 'REJECTED', finished_ts = ?, last_error = ?
                        WHERE id = ?
                    """, (_now_iso(), f"parse_error: {str(e)}", job_id)))
                continue
            
            # FIX 2: Anti-starvation for rate limit
            if _check_rate_limit(client_id):
                with conn() as c:
                    run_with_retry(lambda: c.execute("""
                        UPDATE trade_queue
                        SET status = 'NEW', started_ts = NULL, last_error = ?, created_ts = ?
                        WHERE id = ?
                    """, (f"rate_limited: {MAX_TRADES_PER_HOUR}/hour", _now_iso(), job_id)))
                time.sleep(poll_seconds)
                continue
            
            symbol = payload.get("symbol") or payload.get("underlying")
            direction = payload.get("direction") or payload.get("side")
            entry_price = _extract_entry_price(payload)
            
            if entry_price is not None and symbol and direction:
                current_price = _get_stock_price(broker, symbol)
                
                # FIX 2: Anti-starvation for price unavailable
                if current_price is None:
                    with conn() as c:
                        run_with_retry(lambda: c.execute("""
                            UPDATE trade_queue
                            SET status = 'NEW', started_ts = NULL, last_error = ?, created_ts = ?
                            WHERE id = ?
                        """, ("price_unavailable", _now_iso(), job_id)))
                    time.sleep(PRICE_CHECK_INTERVAL)
                    continue
                
                triggered = _is_triggered(direction, entry_price, current_price)
                
                # FIX 2: Anti-starvation for waiting trigger
                if not triggered:
                    log.debug(f"⏳ {symbol}: ${current_price:.2f} waiting ${entry_price}")
                    with conn() as c:
                        run_with_retry(lambda: c.execute("""
                            UPDATE trade_queue
                            SET status = 'NEW', started_ts = NULL, last_error = ?, created_ts = ?
                            WHERE id = ?
                        """, (f"waiting: current=${current_price:.2f}", _now_iso(), job_id)))
                    time.sleep(PRICE_CHECK_INTERVAL)
                    continue
                
                log.info(f"🎯 {symbol} TRIGGERED: ${current_price:.2f} → ${entry_price}")
            
            # FIX 3: Call with keyword args
            log.info(f"Executing: {signal_id}")
            result = process_signal(
                broker=broker,
                client_id=client_id,
                signal_payload=payload
            )
            
            status = "DONE" if result.get("ok") else "REJECTED"
            error = None if result.get("ok") else (result.get("error") or result.get("reason"))
            
            with conn() as c:
                run_with_retry(lambda: c.execute("""
                    UPDATE trade_queue
                    SET status = ?, finished_ts = ?, result_json = ?, last_error = ?
                    WHERE id = ?
                """, (status, _now_iso(), _json_dumps(result), error, job_id)))
            
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
                            SET status = 'ERROR', finished_ts = ?, last_error = ?
                            WHERE id = ?
                        """, (_now_iso(), str(e), job_id)))
                except:
                    pass
            time.sleep(poll_seconds)
EOF

