# ap/state.py
"""
Equity reservation & symbol locking for safe concurrency.
Postgres-native -- no SQLite syntax anywhere.

Locking strategy:
  - pg_try_advisory_xact_lock(hashtext(key)) -- lightweight, no table needed,
    releases automatically at transaction end. Best for trading signals.
  - kv table stores reserved equity and symbol lock timestamps.
  - All mutations use INSERT ... ON CONFLICT DO UPDATE (upsert) -- no manual COMMIT/ROLLBACK.
  - All queries use %s placeholders (psycopg2, not sqlite3).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

# ap.db imports are deferred to inside functions to avoid circular import:
# ap/db.py -> ap/state.py -> ap/db.py at module level = ImportError
from ap.utils import now_utc_iso, json_dumps, json_loads
from ap.logger import get_logger

log = get_logger("ap.state")


def _conn():
    """Deferred import of conn to break circular dependency."""
    from ap.db import conn
    return conn


def _run_with_retry(fn, **kwargs):
    """Deferred import of run_with_retry to break circular dependency."""
    from ap.db import run_with_retry
    return run_with_retry(fn, **kwargs)


# =====================================================================
# EQUITY RESERVATION (per-client)
# =====================================================================

def reserve_equity_if_available(
    client_id: str, amount: float, current_equity: float
) -> bool:
    """
    Atomically reserve 'amount' only if (current_equity - reserved_equity) >= amount.
    Uses pg_try_advisory_xact_lock so only one process can mutate this key at a time.
    """
    client_id = (client_id or "default").strip()
    amount     = float(amount)
    current_equity = float(current_equity)

    if amount <= 0:
        return True

    key    = f"reserved_equity:{client_id}"
    ok     = False

    def _txn():
        nonlocal ok
        with _conn()() as c:
            # Acquire advisory lock for this key -- non-blocking
            # hashtext() is a stable Postgres function: same string -> same int
            c.execute(
                "SELECT pg_try_advisory_xact_lock(hashtext(%s))",
                (key,)
            )
            acquired = c.fetchone()['pg_try_advisory_xact_lock']
            if not acquired:
                ok = False
                return

            # Read current reservation
            c.execute("SELECT v FROM kv WHERE k = %s", (key,))
            row = c.fetchone()
            reserved = float(json_loads(row['v'])) if row else 0.0

            available = max(0.0, current_equity - reserved)
            if amount > available:
                ok = False
                return

            # Write new reservation
            reserved_new = reserved + amount
            c.execute(
                """
                INSERT INTO kv (k, v, updated_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (k) DO UPDATE
                    SET v = EXCLUDED.v, updated_at = EXCLUDED.updated_at
                """,
                (key, json_dumps(reserved_new), now_utc_iso()),
            )
            ok = True

    _run_with_retry(_txn)
    if ok:
        log.info(f"Reserved ${amount:,.2f} for {client_id}")
    else:
        log.warning(
            f"Reserve failed for {client_id}: ${amount:,.2f} "
            f"(equity=${current_equity:,.2f})"
        )
    return ok


def release_equity(client_id: str, amount: float) -> None:
    """
    Atomically release reserved equity. Floors at 0.
    """
    client_id = (client_id or "default").strip()
    amount     = float(amount)
    if amount <= 0:
        return

    key = f"reserved_equity:{client_id}"

    def _txn():
        with _conn()() as c:
            c.execute(
                "SELECT pg_try_advisory_xact_lock(hashtext(%s))",
                (key,)
            )
            acquired = c.fetchone()['pg_try_advisory_xact_lock']
            if not acquired:
                log.warning(f"Release skipped (lock busy): {client_id}")
                return

            c.execute("SELECT v FROM kv WHERE k = %s", (key,))
            row = c.fetchone()
            reserved     = float(json_loads(row['v'])) if row else 0.0
            reserved_new = max(0.0, reserved - amount)
            c.execute(
                """
                INSERT INTO kv (k, v, updated_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (k) DO UPDATE
                    SET v = EXCLUDED.v, updated_at = EXCLUDED.updated_at
                """,
                (key, json_dumps(reserved_new), now_utc_iso()),
            )

    _run_with_retry(_txn)
    log.info(f"Released ${amount:,.2f} for {client_id}")


def get_reserved_equity(client_id: str) -> float:
    client_id = (client_id or "default").strip()
    key = f"reserved_equity:{client_id}"

    def _read():
        with _conn()() as c:
            c.execute("SELECT v FROM kv WHERE k = %s", (key,))
            row = c.fetchone()
            return float(json_loads(row['v'])) if row else 0.0

    return _run_with_retry(_read)


# =====================================================================
# SYMBOL LOCKS
# =====================================================================

def acquire_symbol_lock(
    client_id: str, symbol: str, ttl_seconds: int = 90
) -> bool:
    """
    Atomically acquire a per-symbol lock with TTL.
    Uses pg_try_advisory_xact_lock for mutual exclusion, kv for TTL tracking.
    Returns True if lock acquired, False if already held by another process.
    """
    client_id = (client_id or "default").strip()
    symbol    = (symbol or "").strip().upper()
    if not symbol:
        return False

    key = f"lock:{client_id}:{symbol}"
    now = datetime.now(timezone.utc).timestamp()
    ok  = False

    def _txn():
        nonlocal ok
        with _conn()() as c:
            # Advisory lock ensures only one process enters this block at a time
            c.execute(
                "SELECT pg_try_advisory_xact_lock(hashtext(%s))",
                (key,)
            )
            acquired = c.fetchone()['pg_try_advisory_xact_lock']
            if not acquired:
                ok = False
                return

            # Check if existing lock is still within TTL
            c.execute("SELECT v FROM kv WHERE k = %s", (key,))
            row = c.fetchone()
            if row:
                payload  = json_loads(row['v'])
                lock_ts  = float(payload.get("ts") or 0.0)
                if (now - lock_ts) < ttl_seconds:
                    ok = False
                    return

            # Write new lock timestamp
            c.execute(
                """
                INSERT INTO kv (k, v, updated_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (k) DO UPDATE
                    SET v = EXCLUDED.v, updated_at = EXCLUDED.updated_at
                """,
                (key, json_dumps({"ts": now}), now_utc_iso()),
            )
            ok = True

    _run_with_retry(_txn)
    if ok:
        log.info(f"Acquired lock: {client_id}:{symbol}")
    else:
        log.warning(f"Lock busy: {client_id}:{symbol}")
    return ok


def release_symbol_lock(client_id: str, symbol: str) -> None:
    client_id = (client_id or "default").strip()
    symbol    = (symbol or "").strip().upper()
    if not symbol:
        return
    key = f"lock:{client_id}:{symbol}"

    def _delete():
        with _conn()() as c:
            c.execute("DELETE FROM kv WHERE k = %s", (key,))

    _run_with_retry(_delete)
    log.info(f"Released lock: {client_id}:{symbol}")


def is_symbol_locked(
    client_id: str, symbol: str, ttl_seconds: int = 90
) -> bool:
    client_id = (client_id or "default").strip()
    symbol    = (symbol or "").strip().upper()
    if not symbol:
        return False
    key = f"lock:{client_id}:{symbol}"
    now = datetime.now(timezone.utc).timestamp()

    def _read():
        with _conn()() as c:
            c.execute("SELECT v FROM kv WHERE k = %s", (key,))
            row = c.fetchone()
            if not row:
                return False
            payload = json_loads(row['v'])
            lock_ts = float(payload.get("ts") or 0.0)
            return (now - lock_ts) < ttl_seconds

    return _run_with_retry(_read)


# =====================================================================
# LEGACY COMPATIBILITY
# =====================================================================

def load_state(client_id: str = "default") -> dict:
    """Load client state from database (legacy compatibility)."""
    from ap.db import get_client_state
    return get_client_state(client_id)


def update_state(updates: dict, client_id: str = "default") -> None:
    """Update client state (legacy compatibility)."""
    from ap.db import update_client_state
    update_client_state(client_id, updates)
