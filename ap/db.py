# ap/db.py — PRODUCTION POSTGRES VERSION
# =============================================================================
# Replaces SQLite with Supabase Postgres via psycopg2.
# ThreadedConnectionPool handles concurrent gunicorn workers + threads safely.
# Interface is identical to the SQLite version — nothing else needs to change.
#
# Required env vars:
#   DATABASE_URL  — Supabase Postgres connection string
#                   Format: postgresql://postgres:[password]@db.[ref].supabase.co:5432/postgres
#                   Found in: Supabase → Settings → Database → Connection String → URI
# =============================================================================

from __future__ import annotations

import os
import time
import uuid
import logging
from contextlib import contextmanager
from typing import Any, Callable

import psycopg2
import psycopg2.extras
import psycopg2.pool
from psycopg2 import errors as pg_errors

from ap.utils import now_utc_iso

log = logging.getLogger("ap.db")

# ── Connection pool ───────────────────────────────────────────────────────────
DATABASE_URL = os.getenv("DATABASE_URL", "")

if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL env var is required. "
        "Get it from Supabase → Settings → Database → Connection String → URI"
    )

_pool: psycopg2.pool.ThreadedConnectionPool | None = None


def _get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    global _pool
    if _pool is None or _pool.closed:
        _pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=20,
            dsn=DATABASE_URL,
            connect_timeout=10,
        )
        log.info("Postgres connection pool initialized (min=2 max=20)")
    return _pool


# ── Retry wrapper ─────────────────────────────────────────────────────────────

def run_with_retry(fn: Callable[[], Any], retries: int = 10,
                   base_sleep: float = 0.1, max_sleep: float = 2.0):
    delay = base_sleep
    last_err = None
    for _ in range(retries):
        try:
            return fn()
        except (psycopg2.OperationalError,
                pg_errors.DeadlockDetected,
                pg_errors.SerializationFailure) as e:
            last_err = e
            log.warning(f"DB transient error (retrying): {e}")
            time.sleep(delay)
            delay = min(delay * 2, max_sleep)
    try:
        return fn()
    except Exception:
        if last_err:
            raise last_err
        raise


# ── Connection context manager ────────────────────────────────────────────────

@contextmanager
def conn():
    """
    Yields a _ConnWrapper from the pool.
    Auto-commits on success, rolls back on exception.
    Callers use: `with conn() as c: c.execute(sql, params)`
    """
    pool = _get_pool()
    db_conn = pool.getconn()
    try:
        db_conn.autocommit = False
        cursor = db_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        wrapper = _ConnWrapper(db_conn, cursor)
        yield wrapper
        db_conn.commit()
    except Exception:
        db_conn.rollback()
        raise
    finally:
        cursor.close()
        pool.putconn(db_conn)


class _ConnWrapper:
    """
    Drop-in replacement for sqlite3 connection.
    Converts ? → %s placeholders automatically.
    Converts RealDictRow results to plain dicts.
    Also supports .rowcount for UPDATE/DELETE checks.
    """
    def __init__(self, connection, cursor):
        self._conn = connection
        self._cur  = cursor

    def execute(self, sql: str, params: tuple | list = ()):
        pg_sql = sql.replace("?", "%s")
        self._cur.execute(pg_sql, params)
        return self

    def fetchone(self):
        row = self._cur.fetchone()
        return dict(row) if row else None

    def fetchall(self):
        return [dict(r) for r in (self._cur.fetchall() or [])]

    @property
    def rowcount(self):
        return self._cur.rowcount

    @property
    def lastrowid(self):
        # Postgres uses RETURNING — fallback to None if not used
        return None

    def __getattr__(self, name):
        return getattr(self._cur, name)


# ── Schema init ───────────────────────────────────────────────────────────────

def init_db():
    """
    No-op for Postgres — schema managed via Supabase SQL editor.
    Verifies connection is healthy on startup.
    """
    try:
        with conn() as c:
            c.execute("SELECT 1")
        log.info("✅ Postgres connection verified")
    except Exception as e:
        log.error(f"❌ Postgres connection failed: {e}")
        raise


# =========================================================================
# DEDUPE HELPERS
# =========================================================================

def already_processed_signal(signal_id: str) -> bool:
    def _fn():
        with conn() as c:
            c.execute("SELECT 1 FROM processed_signals WHERE signal_id = %s", (signal_id,))
            return c.fetchone() is not None
    return run_with_retry(_fn)


def mark_signal_processed(signal_id: str):
    def _fn():
        with conn() as c:
            c.execute(
                "INSERT INTO processed_signals(signal_id, first_seen_ts) "
                "VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (signal_id, now_utc_iso()),
            )
    return run_with_retry(_fn)


# =========================================================================
# ORDER HELPERS
# =========================================================================

def new_local_order_id() -> str:
    return str(uuid.uuid4())


def insert_order(
    *,
    client_id: str = "default",
    local_order_id: str,
    position_id: str | None,
    kind: str,
    status: str,
    symbol: str,
    contract: str,
    qty: int,
    limit_price: float | None = None,
    broker_order_id: str | None = None,
    direction: str | None = None,
    reserved_cost: float | None = None,
):
    ts = now_utc_iso()
    def _fn():
        with conn() as c:
            c.execute(
                """
                INSERT INTO orders (
                    client_id, local_order_id, broker_order_id, position_id,
                    kind, status, symbol, contract, direction, reserved_cost,
                    qty, limit_price, filled_qty, retries, last_error,
                    created_ts, updated_ts
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (local_order_id) DO NOTHING
                """,
                (client_id, local_order_id, broker_order_id, position_id,
                 kind, status, symbol, contract, direction, reserved_cost,
                 int(qty), limit_price, 0, 0, None, ts, ts),
            )
    return run_with_retry(_fn)


def update_order(
    local_order_id: str,
    *,
    status: str | None = None,
    broker_order_id: str | None = None,
    last_error: str | None = None,
    filled_qty: int | None = None,
):
    updates = []; params: list[Any] = []
    if status is not None:           updates.append("status=%s");           params.append(status)
    if broker_order_id is not None:  updates.append("broker_order_id=%s");  params.append(broker_order_id)
    if last_error is not None:       updates.append("last_error=%s");       params.append(last_error)
    if filled_qty is not None:       updates.append("filled_qty=%s");       params.append(int(filled_qty))
    updates.append("updated_ts=%s"); params.append(now_utc_iso())
    params.append(local_order_id)
    sql = f"UPDATE orders SET {', '.join(updates)} WHERE local_order_id=%s"
    def _fn():
        with conn() as c:
            c.execute(sql, tuple(params))
    return run_with_retry(_fn)


# =========================================================================
# CLIENT MANAGEMENT
# =========================================================================

def get_client(client_id: str) -> dict:
    def _fn():
        with conn() as c:
            c.execute("SELECT * FROM clients WHERE client_id = %s", (client_id,))
            row = c.fetchone()
            if not row:
                raise ValueError(f"Client not found: {client_id}")
            return row
    return run_with_retry(_fn)


def get_client_state(client_id: str = "default") -> dict:
    def _fn():
        with conn() as c:
            c.execute("SELECT * FROM client_state WHERE client_id = %s", (client_id,))
            row = c.fetchone()
            if not row:
                return {
                    "client_id": client_id, "current_equity": 0.0,
                    "starting_equity_today": 0.0, "realized_pnl_today": 0.0,
                    "trades_taken_today": 0, "daily_stop_hit": 0,
                    "kill_switch": 0, "mode": "PAPER", "day_key": None,
                }
            return row
    return run_with_retry(_fn)


def update_client_state(client_id: str = "default", updates: dict | None = None):
    if not updates:
        return
    ALLOWED = {
        "current_equity", "starting_equity_today", "realized_pnl_today",
        "trades_taken_today", "daily_stop_hit", "kill_switch", "mode",
        "last_heartbeat_ts", "day_key", "client_capital", "rental_fee",
        "working_capital", "profit_target_min", "profit_target_max",
        "subscription_end_date", "growth_tracking_enabled", "account_type",
        "updated_at",
    }
    safe = {k: v for k, v in updates.items() if k in ALLOWED}
    if not safe:
        return
    safe["updated_at"] = now_utc_iso()
    set_clause = ", ".join(f"{k}=%s" for k in safe.keys())
    values = list(safe.values()) + [client_id]
    def _fn():
        with conn() as c:
            c.execute(
                f"""
                INSERT INTO client_state (client_id, current_equity, starting_equity_today,
                    realized_pnl_today, trades_taken_today, mode, updated_at)
                VALUES (%s, 100000, 100000, 0, 0, 'PAPER', NOW())
                ON CONFLICT (client_id) DO UPDATE SET {set_clause}
                """,
                [client_id] + values,
            )
    return run_with_retry(_fn)


def create_client(
    client_id: str, name: str, broker_type: str,
    broker_account_id: str, broker_token: str, broker_base_url: str,
    initial_equity: float, max_trades_per_day: int = 5,
    max_concurrent_positions: int = 7, daily_max_loss_pct: float = 0.05,
    base_position_pct: float = 0.10, api_key: str | None = None,
) -> str:
    now = now_utc_iso()
    def _fn():
        with conn() as c:
            c.execute(
                """
                INSERT INTO clients (
                    client_id, api_key, name, broker_type, broker_account_id,
                    broker_token, broker_base_url, initial_equity, status,
                    created_at, max_trades_per_day, max_concurrent_positions,
                    daily_max_loss_pct, base_position_pct
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (client_id) DO NOTHING
                """,
                (client_id, api_key, name, broker_type, broker_account_id,
                 broker_token, broker_base_url, float(initial_equity), "ACTIVE",
                 now, int(max_trades_per_day), int(max_concurrent_positions),
                 float(daily_max_loss_pct), float(base_position_pct)),
            )
            c.execute(
                """
                INSERT INTO client_state (
                    client_id, current_equity, starting_equity_today,
                    realized_pnl_today, trades_taken_today, daily_stop_hit,
                    kill_switch, mode, day_key
                ) VALUES (%s,%s,%s,0.0,0,0,0,'PAPER',NULL)
                ON CONFLICT (client_id) DO NOTHING
                """,
                (client_id, float(initial_equity), float(initial_equity)),
            )
    run_with_retry(_fn)
    return client_id


def upsert_client(client_id: str, **kwargs):
    def _fn():
        with conn() as c:
            c.execute("SELECT client_id FROM clients WHERE client_id=%s", (client_id,))
            existing = c.fetchone()
            now = now_utc_iso()
            if existing:
                updates = []; params = []
                for k, v in kwargs.items():
                    updates.append(f"{k}=%s"); params.append(v)
                params.append(client_id)
                c.execute(f"UPDATE clients SET {', '.join(updates)} WHERE client_id=%s", params)
            else:
                kwargs.setdefault("status", "ACTIVE")
                kwargs.setdefault("created_at", now)
                kwargs["client_id"] = client_id
                cols = ", ".join(kwargs.keys())
                phs  = ", ".join(["%s"] * len(kwargs))
                c.execute(f"INSERT INTO clients ({cols}) VALUES ({phs})", list(kwargs.values()))
    return run_with_retry(_fn)


def get_all_clients(status: str | None = None) -> list[dict]:
    def _fn():
        with conn() as c:
            if status:
                c.execute("SELECT * FROM clients WHERE status=%s ORDER BY created_at DESC", (status,))
            else:
                c.execute("SELECT * FROM clients ORDER BY created_at DESC")
            return c.fetchall()
    return run_with_retry(_fn)


def update_client(client_id: str, **kwargs) -> dict:
    if not kwargs:
        raise ValueError("No fields to update")
    def _fn():
        with conn() as c:
            updates = []; params = []
            for k, v in kwargs.items():
                updates.append(f"{k}=%s"); params.append(v)
            params.append(client_id)
            c.execute(f"UPDATE clients SET {', '.join(updates)} WHERE client_id=%s", params)
    run_with_retry(_fn)
    return get_client(client_id)


def delete_client(client_id: str):
    def _fn():
        with conn() as c:
            for table in ["audit_log", "trade_queue", "orders", "positions", "client_state", "clients"]:
                c.execute(f"DELETE FROM {table} WHERE client_id=%s", (client_id,))
    return run_with_retry(_fn)


# =========================================================================
# POSITION HELPERS
# =========================================================================

def get_all_positions(client_id: str | None = None) -> list[dict]:
    def _fn():
        with conn() as c:
            if client_id:
                c.execute("SELECT * FROM positions WHERE client_id=%s ORDER BY entry_ts DESC", (client_id,))
            else:
                c.execute("SELECT * FROM positions ORDER BY entry_ts DESC")
            return c.fetchall()
    return run_with_retry(_fn)


def get_position_by_id(position_id: str) -> dict | None:
    def _fn():
        with conn() as c:
            c.execute("SELECT * FROM positions WHERE id=%s", (position_id,))
            return c.fetchone()
    return run_with_retry(_fn)


def list_positions(client_id: str | None = None, limit: int = 200,
                   status: str | None = None) -> list[dict]:
    def _fn():
        with conn() as c:
            if client_id and status:
                c.execute(
                    "SELECT * FROM positions WHERE client_id=%s AND status=%s "
                    "ORDER BY entry_ts DESC LIMIT %s", (client_id, status, limit))
            elif client_id:
                c.execute(
                    "SELECT * FROM positions WHERE client_id=%s "
                    "ORDER BY entry_ts DESC LIMIT %s", (client_id, limit))
            elif status:
                c.execute(
                    "SELECT * FROM positions WHERE status=%s "
                    "ORDER BY entry_ts DESC LIMIT %s", (status, limit))
            else:
                c.execute("SELECT * FROM positions ORDER BY entry_ts DESC LIMIT %s", (limit,))
            return c.fetchall()
    return run_with_retry(_fn)


# =========================================================================
# ORDER QUERY HELPERS
# =========================================================================

def get_orders_for_client(client_id: str) -> list[dict]:
    def _fn():
        with conn() as c:
            c.execute("SELECT * FROM orders WHERE client_id=%s ORDER BY created_ts DESC", (client_id,))
            return c.fetchall()
    return run_with_retry(_fn)


def get_all_orders() -> list[dict]:
    def _fn():
        with conn() as c:
            c.execute("SELECT * FROM orders ORDER BY created_ts DESC")
            return c.fetchall()
    return run_with_retry(_fn)


def get_order_by_id(local_order_id: str) -> dict | None:
    def _fn():
        with conn() as c:
            c.execute("SELECT * FROM orders WHERE local_order_id=%s", (local_order_id,))
            return c.fetchone()
    return run_with_retry(_fn)


def list_orders(client_id: str | None = None, limit: int = 200,
                status: str | None = None) -> list[dict]:
    def _fn():
        with conn() as c:
            if client_id and status:
                c.execute(
                    "SELECT * FROM orders WHERE client_id=%s AND status=%s "
                    "ORDER BY created_ts DESC LIMIT %s", (client_id, status, limit))
            elif client_id:
                c.execute(
                    "SELECT * FROM orders WHERE client_id=%s "
                    "ORDER BY created_ts DESC LIMIT %s", (client_id, limit))
            elif status:
                c.execute(
                    "SELECT * FROM orders WHERE status=%s "
                    "ORDER BY created_ts DESC LIMIT %s", (status, limit))
            else:
                c.execute("SELECT * FROM orders ORDER BY created_ts DESC LIMIT %s", (limit,))
            return c.fetchall()
    return run_with_retry(_fn)


def get_open_orders_for_reconcile(client_id: str | None = None,
                                   limit: int = 200) -> list[dict]:
    def _fn():
        with conn() as c:
            if client_id:
                c.execute(
                    "SELECT * FROM orders WHERE client_id=%s "
                    "AND status IN ('NEW','ACK','PARTIAL') "
                    "ORDER BY created_ts DESC LIMIT %s", (client_id, limit))
            else:
                c.execute(
                    "SELECT * FROM orders WHERE status IN ('NEW','ACK','PARTIAL') "
                    "ORDER BY created_ts DESC LIMIT %s", (limit,))
            return c.fetchall()
    return run_with_retry(_fn)


# =========================================================================
# AUDIT LOG HELPERS
# =========================================================================

def get_audit_logs(client_id: str | None = None, limit: int = 100) -> list[dict]:
    def _fn():
        with conn() as c:
            if client_id:
                c.execute(
                    "SELECT * FROM audit_log WHERE client_id=%s ORDER BY ts DESC LIMIT %s",
                    (client_id, limit))
            else:
                c.execute("SELECT * FROM audit_log ORDER BY ts DESC LIMIT %s", (limit,))
            return c.fetchall()
    return run_with_retry(_fn)


def list_audit(client_id: str | None = None, limit: int = 200) -> list[dict]:
    return get_audit_logs(client_id=client_id, limit=limit)
