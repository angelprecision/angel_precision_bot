# ap/state.py - ATOMIC STATE MANAGEMENT
"""
Equity reservation & symbol locking for safe concurrency.
Prevents over-allocation when multiple signals arrive simultaneously.
"""

import json
from datetime import datetime, timezone

from ap.db import conn, run_with_retry
from ap.utils import now_utc_iso, json_dumps, json_loads
from ap.logger import get_logger

log = get_logger("ap.state")


# =====================================================================
# EQUITY RESERVATION (Prevents over-allocation)
# =====================================================================

def reserve_equity_if_available(
    client_id: str,
    amount: float,
    current_equity: float
) -> bool:
    """
    Atomically reserve equity only if available.
    
    CRITICAL: This prevents 88 signals from each allocating $5K when
    you only have $100K total. First 20 get reserved, rest fail gracefully.
    
    Args:
        client_id: Client identifier
        amount: Amount to reserve (e.g., $5000)
        current_equity: Current account equity
        
    Returns:
        True if reservation succeeded, False if insufficient available
    """
    amount = float(amount)
    current_equity = float(current_equity)
    
    if amount <= 0:
        return True
    
    key = f"reserved_equity:{client_id}"
    ok = False
    
    def _txn():
        nonlocal ok
        with conn() as c:
            c.execute("BEGIN IMMEDIATE")
            
            # Get current reserved amount
            row = c.execute(
                "SELECT v FROM kv WHERE k=?",
                (key,)
            ).fetchone()
            
            reserved = float(json_loads(row["v"])) if row else 0.0
            
            # Calculate available
            available = max(0.0, current_equity - reserved)
            
            if amount > available:
                log.warning(
                    f"❌ Insufficient equity: need ${amount:,.2f}, "
                    f"available ${available:,.2f} "
                    f"(equity=${current_equity:,.2f}, reserved=${reserved:,.2f})"
                )
                c.execute("ROLLBACK")
                ok = False
                return
            
            # Reserve it
            reserved_new = reserved + amount
            c.execute(
                "INSERT OR REPLACE INTO kv (k, v, updated_at) VALUES (?, ?, ?)",
                (key, json_dumps(reserved_new), now_utc_iso())
            )
            c.execute("COMMIT")
            
            log.info(
                f"✅ Reserved ${amount:,.2f} for {client_id} "
                f"(total reserved: ${reserved_new:,.2f})"
            )
            ok = True
    
    run_with_retry(_txn)
    return ok


def release_equity(client_id: str, amount: float) -> None:
    """
    Release previously reserved equity.
    
    Call this when:
    - Order gets rejected by broker
    - Order fills (position now exists, no longer reserved)
    - Position closes (realized P&L applied)
    
    Args:
        client_id: Client identifier
        amount: Amount to release
    """
    amount = float(amount)
    if amount <= 0:
        return
    
    key = f"reserved_equity:{client_id}"
    
    def _txn():
        with conn() as c:
            c.execute("BEGIN IMMEDIATE")
            
            row = c.execute("SELECT v FROM kv WHERE k=?", (key,)).fetchone()
            reserved = float(json_loads(row["v"])) if row else 0.0
            
            reserved_new = max(0.0, reserved - amount)
            
            c.execute(
                "INSERT OR REPLACE INTO kv (k, v, updated_at) VALUES (?, ?, ?)",
                (key, json_dumps(reserved_new), now_utc_iso())
            )
            c.execute("COMMIT")
            
            log.info(
                f"✅ Released ${amount:,.2f} for {client_id} "
                f"(remaining reserved: ${reserved_new:,.2f})"
            )
    
    run_with_retry(_txn)


def get_reserved_equity(client_id: str) -> float:
    """Get currently reserved equity for client"""
    key = f"reserved_equity:{client_id}"
    
    with conn() as c:
        row = run_with_retry(
            lambda: c.execute("SELECT v FROM kv WHERE k=?", (key,)).fetchone()
        )
        
        if row:
            return float(json_loads(row["v"]))
        return 0.0


# =====================================================================
# SYMBOL LOCKS (Prevents duplicate trades)
# =====================================================================

def acquire_symbol_lock(
    client_id: str,
    symbol: str,
    ttl_seconds: int = 60
) -> bool:
    """
    Atomically acquire lock on a symbol.
    
    CRITICAL: This prevents FAST trading twice simultaneously.
    Uses BEGIN IMMEDIATE for true atomicity.
    
    Args:
        client_id: Client identifier
        symbol: Stock symbol (e.g., "AAPL")
        ttl_seconds: Lock expires after this time (default 60s)
        
    Returns:
        True if lock acquired, False if already locked
    """
    client_id = (client_id or "default").strip()
    symbol = (symbol or "").strip().upper()
    
    if not symbol:
        return False
    
    key = f"lock:{client_id}:{symbol}"
    now = datetime.now(timezone.utc).timestamp()
    
    ok = False
    
    def _txn():
        nonlocal ok
        with conn() as c:
            c.execute("BEGIN IMMEDIATE")
            
            # Check if lock exists and is still valid
            row = c.execute("SELECT v FROM kv WHERE k=?", (key,)).fetchone()
            
            if row:
                payload = json_loads(row["v"])
                lock_ts = float(payload.get("ts", 0))
                
                # Lock still valid?
                if (now - lock_ts) < ttl_seconds:
                    log.warning(f"❌ Symbol locked: {symbol} (locked {now - lock_ts:.0f}s ago)")
                    c.execute("ROLLBACK")
                    ok = False
                    return
            
            # Acquire lock
            c.execute(
                "INSERT OR REPLACE INTO kv (k, v, updated_at) VALUES (?, ?, ?)",
                (key, json_dumps({"ts": now}), now_utc_iso())
            )
            c.execute("COMMIT")
            
            log.info(f"🔒 Acquired symbol lock: {symbol}")
            ok = True
    
    run_with_retry(_txn)
    return ok


def release_symbol_lock(client_id: str, symbol: str) -> None:
    """
    Release symbol lock.
    
    Call this when:
    - Order fills (position created, lock no longer needed)
    - Order rejected (allow retry on different signal)
    
    Args:
        client_id: Client identifier
        symbol: Stock symbol
    """
    client_id = (client_id or "default").strip()
    symbol = (symbol or "").strip().upper()
    
    if not symbol:
        return
    
    key = f"lock:{client_id}:{symbol}"
    
    with conn() as c:
        run_with_retry(
            lambda: c.execute("DELETE FROM kv WHERE k=?", (key,))
        )
    
    log.info(f"🔓 Released symbol lock: {symbol}")


def is_symbol_locked(client_id: str, symbol: str, ttl_seconds: int = 60) -> bool:
    """
    Check if symbol is currently locked (without acquiring).
    
    Useful for read-only checks without blocking.
    """
    client_id = (client_id or "default").strip()
    symbol = (symbol or "").strip().upper()
    
    if not symbol:
        return False
    
    key = f"lock:{client_id}:{symbol}"
    now = datetime.now(timezone.utc).timestamp()
    
    with conn() as c:
        row = run_with_retry(
            lambda: c.execute("SELECT v FROM kv WHERE k=?", (key,)).fetchone()
        )
        
        if row:
            payload = json_loads(row["v"])
            lock_ts = float(payload.get("ts", 0))
            return (now - lock_ts) < ttl_seconds
        
        return False


# =====================================================================
# STATE HELPERS (Legacy compatibility)
# =====================================================================

def load_state(client_id: str = "default") -> dict:
    """Load client state from database"""
    from ap.db import get_client_state
    return get_client_state(client_id)


def update_state(updates: dict, client_id: str = "default") -> None:
    """Update client state"""
    from ap.db import update_client_state
    update_client_state(client_id, updates)
