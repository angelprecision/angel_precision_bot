# ap/db.py -- PRODUCTION POSTGRES VERSION
# =============================================================================
# Replaces SQLite with Supabase Postgres via psycopg2.
# ThreadedConnectionPool handles concurrent gunicorn workers + threads safely.
# Interface is identical to the SQLite version -- nothing else needs to change.
#
# Required env vars:
#   DATABASE_URL  -- Supabase Postgres connection string
#                   Format: postgresql://postgres:[password]@db.[ref].supabase.co:5432/postgres
#                   Found in: Supabase → Settings → Database → Connection String → URI
# =============================================================================

from __future__ import annotations

import os
import time
import uuid
import logging
import warnings
from contextlib import contextmanager
from typing import Any, Callable

import threading

import psycopg2
import psycopg2.extras
import psycopg2.pool
from psycopg2 import errors as pg_errors

from ap.utils import now_utc_iso

log = logging.getLogger("ap.db")

# ---------------------------------------------------------------------------
# Retry profile constants — callers can import these for consistent backoff.
# Do not pass these to run_with_retry() yet; a future PR will wire callers.
# ---------------------------------------------------------------------------
DB_RETRY_FAST = dict(retries=3,  base_sleep=0.05, max_sleep=0.5)
DB_RETRY_SLOW = dict(retries=10, base_sleep=0.1,  max_sleep=2.0)

# HIGH-010: thread-safe pool rebuild lock
_pool_lock = threading.Lock()

# ── Connection pool ───────────────────────────────────────────────────────────
DATABASE_URL = (
    os.getenv("DATABASE_URL")
    or os.getenv("DATABASE_URI")
    or os.getenv("SUPABASE_DB_URL")
    or os.getenv("POSTGRES_URL")
    or ""
)

if not DATABASE_URL:
    raise RuntimeError(
        "No database URL configured. Set DATABASE_URL (or DATABASE_URI / "
        "SUPABASE_DB_URL / POSTGRES_URL) to the Supabase connection string. "
        "Found in: Supabase → Settings → Database → Connection String → URI"
    )

_pool: psycopg2.pool.ThreadedConnectionPool | None = None


def _get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    global _pool
    if _pool is not None and not _pool.closed:
        return _pool
    with _pool_lock:  # HIGH-010: thread-safe pool rebuild
        if _pool is not None and not _pool.closed:
            return _pool
        try:
            # Append sslmode=require if not already in URL
            dsn = DATABASE_URL
            if "sslmode" not in dsn:
                dsn += "?sslmode=require" if "?" not in dsn else "&sslmode=require"
            _max_conn = int(os.getenv("DB_POOL_MAX", "20"))  # HIGH-009: configurable
            _pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=_max_conn,
                dsn=dsn,
                connect_timeout=10,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5,
            )
            log.info(f"Postgres connection pool initialized (min=1 max={_max_conn})")
        except Exception as e:
            log.error(f"Pool creation failed: {e}")
            raise
        return _pool


# ── Retry wrapper ─────────────────────────────────────────────────────────────

def run_with_retry(fn: Callable[[], Any], retries: int = 10,
                   base_sleep: float = 0.1, max_sleep: float = 2.0):
    delay = base_sleep
    for attempt in range(retries + 1):
        try:
            return fn()
        except (
            psycopg2.OperationalError,
            psycopg2.InterfaceError,
            psycopg2.pool.PoolError,       # pool exhausted -- wait and retry
            pg_errors.DeadlockDetected,
            pg_errors.SerializationFailure,
            pg_errors.LockNotAvailable,    # advisory lock contention
        ) as e:
            # Supabase pooler circuit-breaker — fail fast, do NOT retry.
            # Retrying hammers the DB and extends the outage.
            _msg = str(e).lower()
            if "ecircuitbreaker" in _msg or "too many authentication failures" in _msg:
                log.error(
                    "Supabase pooler circuit breaker active — failing fast "
                    "to avoid hammering DB. Error: %s", e
                )
                raise
            if attempt < retries:
                log.warning(f"DB transient error (attempt {attempt+1}/{retries}): {e}")
                time.sleep(delay)
                delay = min(delay * 2, max_sleep)
            else:
                log.error(f"DB error after {retries} retries: {e}")
                raise
        except psycopg2.Error as e:
            # Non-retryable Postgres errors (schema mismatch, constraint violation, etc.)
            # Log with full context so we know exactly which query failed
            log.error(f"Non-retryable DB error [{type(e).__name__}]: {e}")
            raise  # always re-raise -- caller decides whether to mark job ERROR


# ── Connection context manager ────────────────────────────────────────────────

@contextmanager
def conn():
    """
    Yields a _ConnWrapper from the pool.
    Auto-commits on success, rolls back on exception.
    Validates connection health before use -- discards and reopens stale
    connections (handles SSL drop / transient TCP errors).
    Callers use: `with conn() as c: c.execute(sql, params)`
    """
    pool = _get_pool()

    # SELECT 1 probe on every acquisition — keeps connections alive and detects
    # SSL EOF before the real query fires. Required for Supabase which kills
    # idle connections after ~60s. Performance cost acceptable for correctness.
    for _attempt in range(2):
        db_conn = pool.getconn()
        try:
            _probe_cur = db_conn.cursor()
            _probe_cur.execute("SELECT 1")
            _probe_cur.close()
            db_conn.rollback()
            break
        except Exception as _probe_err:
            log.warning("Stale connection detected (%s), discarding and rebuilding pool", _probe_err)
            try:
                pool.putconn(db_conn, close=True)
            except Exception:
                pass
            global _pool
            with _pool_lock:
                _pool = None
            pool = _get_pool()
    else:
        raise psycopg2.OperationalError("Could not obtain a live DB connection after pool rebuild")

    cursor = None
    try:
        db_conn.autocommit = False
        cursor = db_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        wrapper = _ConnWrapper(db_conn, cursor)
        yield wrapper
        db_conn.commit()
    except Exception:
        try:
            db_conn.rollback()
        except Exception:
            pass
        raise
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
        try:
            pool.putconn(db_conn)
        except Exception:
            pass


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
        """Execute a SQL statement and return self for chaining.

        Usage::
            rows = c.execute("SELECT ...").fetchall()
            c.execute("UPDATE ..."); affected = c.rowcount
        """
        # Do NOT replace ? with %s here. The queue SQL uses the Postgres
        # JSONB key-existence operator (payload ? 'score') which the
        # naive replacement converts to payload %s 'score', giving psycopg2
        # a phantom bind placeholder and raising IndexError on every claim.
        # All SQL in this codebase already uses %s placeholders natively.
        self._cur.execute(sql, params)
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
        # Postgres uses RETURNING -- fallback to None if not used
        return None

    def __getattr__(self, name):
        return getattr(self._cur, name)


# ── Schema init ───────────────────────────────────────────────────────────────

def init_db():
    """
    Verifies Postgres connection on startup.
    LIVE mode: DB failure is fatal -- raises RuntimeError to abort startup.
    PAPER/SIM mode: logs error but does not crash (UptimeRobot keeps service awake).
    """
    import os as _os_db
    _bot_mode = _os_db.getenv("BOT_MODE", _os_db.getenv("MODE", "PAPER")).upper()
    try:
        def _probe():
            with conn() as c:
                c.execute("SELECT 1")
        run_with_retry(_probe)
        log.info("✅ Postgres connection verified")
    except Exception as e:
        log.error(f"❌ Postgres connection failed: {e}")
        if _bot_mode == "LIVE":
            raise RuntimeError(
                f"LIVE mode startup aborted -- Postgres unavailable: {e}"
            )
        log.error("Bot will continue in PAPER/SIM mode -- DB writes will fail until restored.")


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
    meta: dict | None = None,
):
    """AUDIT PHASE-2: added `meta` JSONB for signal_entry_price, score,
    and re-peg counters. See migrations/20260519_phase2_orders_meta.sql."""
    warnings.warn(
        "insert_order() is deprecated. Use APOrderStateMachine.create_entry_order() instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    ts = now_utc_iso()
    import json as _json
    meta_json = _json.dumps(meta or {})
    def _fn():
        with conn() as c:
            c.execute(
                """
                INSERT INTO orders (
                    client_id, local_order_id, broker_order_id, position_id,
                    kind, status, symbol, contract, direction, reserved_cost,
                    qty, limit_price, filled_qty, retries, last_error,
                    created_ts, updated_ts, meta
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                ON CONFLICT (local_order_id) DO NOTHING
                """,
                (client_id, local_order_id, broker_order_id, position_id,
                 kind, status, symbol, contract, direction, reserved_cost,
                 int(qty), limit_price, 0, 0, None, ts, ts, meta_json),
            )
    return run_with_retry(_fn)


# TODO(future-PR): update_order() is legacy.
# Status updates should go through APOrderStateMachine.transition() which
# enforces the CAS guard (WHERE status=%s) and rowcount validation.
# A future PR should require client_id and optional expected_status here
# after all callers are audited.
def update_order(
    local_order_id: str,
    *,
    status: str | None = None,
    broker_order_id: str | None = None,
    last_error: str | None = None,
    filled_qty: int | None = None,
    limit_price: float | None = None,
    meta: dict | None = None,
):
    """AUDIT PHASE-2: added `limit_price` (for re-pegs) and `meta` (re-peg counters,
    signal entry price, score). Both are optional and backward-compatible."""
    import json as _json
    updates = []; params: list[Any] = []
    if status is not None:           updates.append("status=%s");           params.append(status)
    if broker_order_id is not None:  updates.append("broker_order_id=%s");  params.append(broker_order_id)
    if last_error is not None:       updates.append("last_error=%s");       params.append(last_error)
    if filled_qty is not None:       updates.append("filled_qty=%s");       params.append(int(filled_qty))
    if limit_price is not None:      updates.append("limit_price=%s");      params.append(float(limit_price))
    if meta is not None:             updates.append("meta=%s::jsonb");      params.append(_json.dumps(meta))
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
                    "kill_switch": False, "mode": "PAPER", "day_key": None,
                }
            return row
    return run_with_retry(_fn)


def ensure_client_exists(client_id: str, equity: float = 25000.0) -> None:
    """
    Auto-provision a client row + client_state row if they don't exist yet.
    Called automatically before any write that has a FK dependency on clients.
    This means adding a new member never requires a manual DB insert.

    Safe to call on every heartbeat -- ON CONFLICT DO NOTHING makes it a no-op
    if the rows already exist.
    """
    now = now_utc_iso()
    def _fn():
        with conn() as c:
            # 1. clients row -- required by FK on client_state, positions, orders, trade_queue
            c.execute(
                """
                INSERT INTO clients (
                    client_id, name, broker_type, broker_account_id,
                    broker_token, broker_base_url, initial_equity, status,
                    created_at, max_trades_per_day, max_concurrent_positions,
                    daily_max_loss_pct, base_position_pct
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (client_id) DO NOTHING
                """,
                (client_id, client_id, 'tradier', '', '', 'https://sandbox.tradier.com',
                 float(equity), 'ACTIVE', now, 10, 7, 0.05, 0.10),
            )
            # 2. client_state row -- required by heartbeat / mode reads
            c.execute(
                """
                INSERT INTO client_state (
                    client_id, current_equity, starting_equity_today,
                    realized_pnl_today, trades_taken_today, daily_stop_hit,
                    kill_switch, mode, day_key, updated_at
                ) VALUES (%s,%s,%s,0.0,0,0,False,'PAPER',NULL,%s)
                ON CONFLICT (client_id) DO NOTHING
                """,
                (client_id, float(equity), float(equity), now),
            )
    run_with_retry(_fn)


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
    set_values = list(safe.values())
    def _fn():
        with conn() as c:
            # Auto-provision client + state rows before writing.
            # SQL kept in sync with ensure_client_exists() — both use ON CONFLICT DO NOTHING.
            _now = now_utc_iso()
            c.execute(
                """
                INSERT INTO clients (
                    client_id, name, broker_type, broker_account_id,
                    broker_token, broker_base_url, initial_equity, status,
                    created_at, max_trades_per_day, max_concurrent_positions,
                    daily_max_loss_pct, base_position_pct
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (client_id) DO NOTHING
                """,
                (client_id, client_id, 'tradier', '', '', 'https://sandbox.tradier.com',
                 25000.0, 'ACTIVE', _now, 10, 7, 0.05, 0.10),
            )
            c.execute(
                """
                INSERT INTO client_state (
                    client_id, current_equity, starting_equity_today,
                    realized_pnl_today, trades_taken_today, daily_stop_hit,
                    kill_switch, mode, day_key, updated_at
                ) VALUES (%s,%s,%s,0.0,0,0,False,'PAPER',NULL,%s)
                ON CONFLICT (client_id) DO NOTHING
                """,
                (client_id, 25000.0, 25000.0, _now),
            )
            # Apply the actual updates
            c.execute(
                f"UPDATE client_state SET {set_clause} WHERE client_id=%s",
                set_values + [client_id],
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
                ) VALUES (%s,%s,%s,0.0,0,0,False,'PAPER',NULL)
                ON CONFLICT (client_id) DO NOTHING
                """,
                (client_id, float(initial_equity), float(initial_equity)),
            )
    run_with_retry(_fn)
    return client_id


# HIGH-011: allowlist of valid column names to prevent SQL injection
_UPSERT_ALLOWED_COLUMNS = {
    "client_id", "api_key", "name", "broker_type", "broker_account_id",
    "broker_token", "broker_base_url", "initial_equity", "status",
    "created_at", "max_trades_per_day", "max_concurrent_positions",
    "daily_max_loss_pct", "base_position_pct",
}


def upsert_client(client_id: str, **kwargs):
    # HIGH-011: validate column names against allowlist before interpolating into SQL
    bad_keys = set(kwargs.keys()) - _UPSERT_ALLOWED_COLUMNS
    if bad_keys:
        raise ValueError(f"upsert_client: invalid column names: {bad_keys}")

    def _fn():
        with conn() as c:
            now = now_utc_iso()
            insert_kwargs = dict(kwargs)
            insert_kwargs.setdefault("status", "ACTIVE")
            insert_kwargs.setdefault("created_at", now)
            insert_kwargs["client_id"] = client_id
            cols    = ", ".join(insert_kwargs.keys())
            phs     = ", ".join(["%s"] * len(insert_kwargs))
            updates = [f"{k}=%s" for k in kwargs.keys()]
            # ON CONFLICT DO UPDATE is atomic — no SELECT-then-branch race condition.
            # Only update keys explicitly passed; client_id/created_at are stable.
            if updates:
                c.execute(
                    f"INSERT INTO clients ({cols}) VALUES ({phs}) "
                    f"ON CONFLICT (client_id) DO UPDATE SET {', '.join(updates)}",
                    list(insert_kwargs.values()) + list(kwargs.values()),
                )
            else:
                c.execute(
                    f"INSERT INTO clients ({cols}) VALUES ({phs}) ON CONFLICT (client_id) DO NOTHING",
                    list(insert_kwargs.values()),
                )
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
    # HIGH-011: same allowlist as upsert_client — prevent SQL injection via admin API
    bad_keys = set(kwargs.keys()) - _UPSERT_ALLOWED_COLUMNS
    if bad_keys:
        raise ValueError(f"update_client: invalid column names: {bad_keys}")
    def _fn():
        with conn() as c:
            updates = []; params = []
            for k, v in kwargs.items():
                updates.append(f"{k}=%s"); params.append(v)
            params.append(client_id)
            c.execute(f"UPDATE clients SET {', '.join(updates)} WHERE client_id=%s", params)
    run_with_retry(_fn)
    return get_client(client_id)


def delete_client(client_id: str, *, confirm: bool = False):
    if not confirm:
        raise ValueError(
            "delete_client requires confirm=True. "
            "This permanently deletes ALL positions, orders, and audit history for this client."
        )
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


def get_position_by_id(position_id: str, client_id: str | None = None) -> dict | None:
    """Fetch a position row by id.

    Args:
      position_id: row primary key.
      client_id:   OPTIONAL ownership filter. When provided, the SELECT
                   adds 'AND client_id=%s' so a row belonging to a
                   different tenant is NEVER returned. Callers that hit
                   this from a client-facing endpoint MUST pass the
                   authenticated client_id.

    Returns the row dict or None. When client_id is omitted the behavior
    is unchanged (global lookup, suitable for internal/admin use).
    """
    def _fn():
        with conn() as c:
            if client_id:
                c.execute(
                    "SELECT * FROM positions WHERE id=%s AND client_id=%s",
                    (position_id, client_id),
                )
            else:
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


def get_order_by_id(local_order_id: str, client_id: str | None = None) -> dict | None:
    """Fetch an order row by local_order_id.

    Args:
      local_order_id: the AP-side order id.
      client_id:      OPTIONAL ownership filter. When provided, the
                      SELECT adds 'AND client_id=%s' so a row belonging
                      to a different tenant is NEVER returned. Callers
                      that hit this from a client-facing endpoint MUST
                      pass the authenticated client_id.

    Returns the row dict or None. When client_id is omitted the behavior
    is unchanged (global lookup, suitable for internal/admin use).
    """
    def _fn():
        with conn() as c:
            if client_id:
                c.execute(
                    "SELECT * FROM orders WHERE local_order_id=%s AND client_id=%s",
                    (local_order_id, client_id),
                )
            else:
                c.execute(
                    "SELECT * FROM orders WHERE local_order_id=%s",
                    (local_order_id,),
                )
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
                    "AND status IN ("
                    "  'CREATED','SUBMITTED','ACKNOWLEDGED','PARTIAL_FILL',"
                    "  'EXIT_REQUESTED','EXIT_SUBMITTED','EXIT_ACKNOWLEDGED','EXIT_PARTIAL_FILL'"
                    ") "
                    "ORDER BY created_ts DESC LIMIT %s", (client_id, limit))
            else:
                c.execute(
                    "SELECT * FROM orders WHERE status IN ("
                    "  'CREATED','SUBMITTED','ACKNOWLEDGED','PARTIAL_FILL',"
                    "  'EXIT_REQUESTED','EXIT_SUBMITTED','EXIT_ACKNOWLEDGED','EXIT_PARTIAL_FILL'"
                    ") "
                    "ORDER BY created_ts DESC LIMIT %s", (limit,))
            return c.fetchall()
    return run_with_retry(_fn)


def get_stale_pending_trigger_orders(client_id: str, older_than_hours: int = 8) -> list[dict]:
    """Find ENTRY orders stuck in PENDING_TRIGGER longer than threshold.

    PENDING_TRIGGER orders have not reached the broker and normally have no
    broker_order_id, so they should NOT be included in broker polling via
    get_open_orders_for_reconcile().

    This helper exists so reconciler/health/admin tooling can detect leaked
    watcher/queue orders that may reserve capital forever.
    """
    def _fn():
        with conn() as c:
            c.execute(
                """
                SELECT *
                FROM orders
                WHERE client_id = %s
                  AND kind = 'ENTRY'
                  AND status = 'PENDING_TRIGGER'
                  AND created_ts < NOW() - (%s || ' hours')::interval
                ORDER BY created_ts ASC
                """,
                (client_id, str(older_than_hours)),
            )
            rows = c.fetchall()
            return [dict(r) for r in rows] if rows else []
    try:
        return run_with_retry(_fn) or []
    except Exception as e:
        log.error("get_stale_pending_trigger_orders failed for %s: %s", client_id, e)
        raise


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
