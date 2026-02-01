# ap/state.py
"""
Equity reservation & symbol locking for safe concurrency.
Uses SQLite kv table with BEGIN IMMEDIATE for atomicity.

- reserved_equity:{client_id} tracks approved-but-not-filled exposure
- lock:{client_id}:{SYMBOL} prevents duplicate symbol trades
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ap.db import conn, run_with_retry
from ap.utils import now_utc_iso, json_dumps, json_loads
from ap.logger import get_logger

log = get_logger("ap.state")


# =====================================================================
# EQUITY RESERVATION (per-client)
# =====================================================================

def reserve_equity_if_available(client_id: str, amount: float, current_equity: float) -> bool:
    """
    Atomically reserve 'amount' only if (current_equity - reserved_equity) >= amount.
    """
    client_id = (client_id or "default").strip()
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

            row = c.execute("SELECT v FROM kv WHERE k=?", (key,)).fetchone()
            reserved = float(json_loads(row["v"])) if row else 0.0

            available = max(0.0, current_equity - reserved)
            if amount > available:
                c.execute("ROLLBACK")
                ok = False
                return

            reserved_new = reserved + amount
            c.execute(
                "INSERT OR REPLACE INTO kv (k, v, updated_at) VALUES (?,?,?)",
                (key, json_dumps(reserved_new), now_utc_iso()),
            )
            c.execute("COMMIT")
            ok = True

    run_with_retry(_txn)
    if ok:
        log.info(f"✅ Reserved ${amount:,.2f} for {client_id}")
    else:
        log.warning(f"❌ Reserve failed for {client_id}: ${amount:,.2f} (equity=${current_equity:,.2f})")
    return ok


def release_equity(client_id: str, amount: float) -> None:
    """
    Atomically release reserved equity. Floors at 0.
    """
    client_id = (client_id or "default").strip()
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
                "INSERT OR REPLACE INTO kv (k, v, updated_at) VALUES (?,?,?)",
                (key, json_dumps(reserved_new), now_utc_iso()),
            )
            c.execute("COMMIT")

    run_with_retry(_txn)
    log.info(f"✅ Released ${amount:,.2f} for {client_id}")


def get_reserved_equity(client_id: str) -> float:
    client_id = (client_id or "default").strip()
    key = f"reserved_equity:{client_id}"
    with conn() as c:
        row = run_with_retry(lambda: c.execute("SELECT v FROM kv WHERE k=?", (key,)).fetchone())
        return float(json_loads(row["v"])) if row else 0.0


# =====================================================================
# SYMBOL LOCKS
# =====================================================================

def acquire_symbol_lock(client_id: str, symbol: str, ttl_seconds: int = 90) -> bool:
    """
    Atomically acquire lock for symbol. Lock auto-expires after ttl_seconds.
    Stores epoch seconds in payload["ts"].
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

            row = c.execute("SELECT v FROM kv WHERE k=?", (key,)).fetchone()
            if row:
                payload = json_loads(row["v"])
                lock_ts = float(payload.get("ts") or 0.0)
                if (now - lock_ts) < ttl_seconds:
                    c.execute("ROLLBACK")
                    ok = False
                    return

            c.execute(
                "INSERT OR REPLACE INTO kv (k, v, updated_at) VALUES (?,?,?)",
                (key, json_dumps({"ts": now}), now_utc_iso()),
            )
            c.execute("COMMIT")
            ok = True

    run_with_retry(_txn)
    if ok:
        log.info(f"🔒 Acquired lock: {client_id}:{symbol}")
    else:
        log.warning(f"❌ Lock busy: {client_id}:{symbol}")
    return ok


def release_symbol_lock(client_id: str, symbol: str) -> None:
    client_id = (client_id or "default").strip()
    symbol = (symbol or "").strip().upper()
    if not symbol:
        return
    key = f"lock:{client_id}:{symbol}"
    with conn() as c:
        run_with_retry(lambda: c.execute("DELETE FROM kv WHERE k=?", (key,)))
    log.info(f"🔓 Released lock: {client_id}:{symbol}")


def is_symbol_locked(client_id: str, symbol: str, ttl_seconds: int = 90) -> bool:
    client_id = (client_id or "default").strip()
    symbol = (symbol or "").strip().upper()
    if not symbol:
        return False
    key = f"lock:{client_id}:{symbol}"
    now = datetime.now(timezone.utc).timestamp()
    with conn() as c:
        row = run_with_retry(lambda: c.execute("SELECT v FROM kv WHERE k=?", (key,)).fetchone())
        if not row:
            return False
        payload = json_loads(row["v"])
        lock_ts = float(payload.get("ts") or 0.0)
        return (now - lock_ts) < ttl_seconds

