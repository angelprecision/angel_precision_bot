# ap/db.py - COMPLETE MULTI-CLIENT VERSION (PRODUCTION / RENDER SAFE)
# - Uses /data disk when BOT_DB_FILE is set to /data/ap_state.db
# - Safe to run repeatedly (migrations included)
# - Multi-client tables: clients, client_state, trade_queue, orders, positions, audit_log
# - Adds orders.direction + orders.reserved_cost to support safe release logic

from __future__ import annotations

import os
import time
import uuid
import sqlite3
from contextlib import contextmanager
from typing import Any, Callable

from ap.config import Config
from ap.utils import now_utc_iso

cfg = Config()


# ============================================================
# SQLite retry wrapper
# ============================================================
def run_with_retry(fn: Callable[[], Any], retries: int = 12, base_sleep: float = 0.05, max_sleep: float = 1.0):
    delay = base_sleep
    last_err = None

    for _ in range(retries):
        try:
            return fn()
        except sqlite3.OperationalError as e:
            last_err = e
            msg = str(e).lower()
            if ("database is locked" in msg) or ("database is busy" in msg) or ("locked" in msg):
                time.sleep(delay)
                delay = min(delay * 2, max_sleep)
                continue
            raise

    try:
        return fn()
    except Exception:
        if last_err:
            raise last_err
        raise


# ============================================================
# Connection context manager (Render disk-safe)
# ============================================================
@contextmanager
def conn():
    db_dir = os.path.dirname(cfg.DB_FILE)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)

    c = sqlite3.connect(
        cfg.DB_FILE,
        timeout=30,
        isolation_level=None,  # autocommit
        check_same_thread=False,
    )
    c.row_factory = sqlite3.Row

    c.execute("PRAGMA journal_mode=WAL;")
    c.execute("PRAGMA busy_timeout=30000;")
    c.execute("PRAGMA foreign_keys=ON;")

    try:
        yield c
    finally:
        c.close()


# ============================================================
# Schema init + migrations
# ============================================================
def init_db():
    with conn() as c:

        def ex(sql: str, params: tuple = ()):
            return run_with_retry(lambda: c.execute(sql, params))

        # ------------------------
        # KV STORE
        # ------------------------
        ex(
            """
            CREATE TABLE IF NOT EXISTS kv (
                k TEXT PRIMARY KEY,
                v TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )

        # ------------------------
        # AUDIT LOG
        # ------------------------
        ex(
            """
            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                level TEXT NOT NULL,
                event TEXT NOT NULL,
                payload TEXT NOT NULL,
                client_id TEXT
            );
            """
        )
        try:
            ex("ALTER TABLE audit_log ADD COLUMN client_id TEXT;")
        except Exception:
            pass
        try:
            ex("CREATE INDEX IF NOT EXISTS idx_audit_log_client_id ON audit_log(client_id);")
        except Exception:
            pass

        # ------------------------
        # PROCESSED SIGNALS (dedupe)
        # ------------------------
        ex(
            """
            CREATE TABLE IF NOT EXISTS processed_signals (
                signal_id TEXT PRIMARY KEY,
                first_seen_ts TEXT NOT NULL
            );
            """
        )

        # ------------------------
        # TRADE QUEUE
        # ------------------------
        ex(
            """
            CREATE TABLE IF NOT EXISTS trade_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id TEXT NOT NULL DEFAULT 'default',
                signal_id TEXT NOT NULL,
                created_ts TEXT NOT NULL,
                status TEXT NOT NULL,     -- NEW | PROCESSING | DONE | REJECTED | ERROR
                payload TEXT NOT NULL,
                started_ts TEXT,
                finished_ts TEXT,
                result_json TEXT,
                last_error TEXT,
                idempotency_key TEXT
            );
            """
        )
        for sql in [
            "ALTER TABLE trade_queue ADD COLUMN client_id TEXT NOT NULL DEFAULT 'default';",
            "ALTER TABLE trade_queue ADD COLUMN started_ts TEXT;",
            "ALTER TABLE trade_queue ADD COLUMN finished_ts TEXT;",
            "ALTER TABLE trade_queue ADD COLUMN result_json TEXT;",
            "ALTER TABLE trade_queue ADD COLUMN last_error TEXT;",
            "ALTER TABLE trade_queue ADD COLUMN idempotency_key TEXT;",
        ]:
            try:
                ex(sql)
            except Exception:
                pass

        try:
            ex("CREATE INDEX IF NOT EXISTS idx_trade_queue_client_status ON trade_queue(client_id, status);")
            ex("CREATE INDEX IF NOT EXISTS idx_trade_queue_status_id ON trade_queue(status, id);")
            ex("CREATE INDEX IF NOT EXISTS idx_trade_queue_created_ts ON trade_queue(created_ts);")
            ex("CREATE INDEX IF NOT EXISTS idx_trade_queue_signal_id ON trade_queue(signal_id);")
        except Exception:
            pass

        try:
            ex("CREATE UNIQUE INDEX IF NOT EXISTS idx_trade_queue_idempotency ON trade_queue(client_id, idempotency_key);")
        except Exception:
            pass

        # ------------------------
        # ORDERS
        # ------------------------
        ex(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id TEXT NOT NULL DEFAULT 'default',
                local_order_id TEXT NOT NULL,
                broker_order_id TEXT,
                position_id TEXT,
                kind TEXT NOT NULL,       -- ENTRY | EXIT | FLATTEN
                status TEXT NOT NULL,     -- NEW | ACK | PARTIAL | FILLED | REJECTED | CANCELED
                symbol TEXT NOT NULL,
                contract TEXT NOT NULL,

                -- NEW: direction + reserved_cost (migrations below)
                direction TEXT,
                reserved_cost REAL,

                qty INTEGER NOT NULL,
                limit_price REAL,
                filled_qty INTEGER NOT NULL DEFAULT 0,
                retries INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                created_ts TEXT NOT NULL,
                updated_ts TEXT NOT NULL
            );
            """
        )

        # migrations for older DBs that created orders without these columns
        for sql in [
            "ALTER TABLE orders ADD COLUMN client_id TEXT NOT NULL DEFAULT 'default';",
            "ALTER TABLE orders ADD COLUMN direction TEXT;",
            "ALTER TABLE orders ADD COLUMN reserved_cost REAL;",
        ]:
            try:
                ex(sql)
            except Exception:
                pass

        try:
            ex("CREATE INDEX IF NOT EXISTS idx_orders_client_local_order_id ON orders(client_id, local_order_id);")
            ex("CREATE INDEX IF NOT EXISTS idx_orders_client_status ON orders(client_id, status);")
            ex("CREATE INDEX IF NOT EXISTS idx_orders_local_order_id ON orders(local_order_id);")
            ex("CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);")
            ex("CREATE INDEX IF NOT EXISTS idx_orders_created_ts ON orders(created_ts);")
            ex("CREATE INDEX IF NOT EXISTS idx_orders_broker_order_id ON orders(broker_order_id);")
            ex("CREATE INDEX IF NOT EXISTS idx_orders_position_id ON orders(position_id);")
        except Exception:
            pass

        # ------------------------
        # POSITIONS
        # ------------------------
        ex(
            """
            CREATE TABLE IF NOT EXISTS positions (
                id TEXT PRIMARY KEY,
                client_id TEXT NOT NULL DEFAULT 'default',
                underlying TEXT NOT NULL,
                contract TEXT NOT NULL,
                direction TEXT NOT NULL,  -- CALL | PUT
                qty INTEGER NOT NULL,
                avg_fill REAL NOT NULL,
                entry_ts TEXT NOT NULL,
                tp_pct REAL NOT NULL,
                sl_pct REAL NOT NULL,
                status TEXT NOT NULL,     -- OPEN | CLOSING | CLOSED
                exit_ts TEXT,
                exit_reason TEXT,
                realized_pnl REAL
            );
            """
        )
        try:
            ex("ALTER TABLE positions ADD COLUMN client_id TEXT NOT NULL DEFAULT 'default';")
        except Exception:
            pass

        try:
            ex("CREATE INDEX IF NOT EXISTS idx_positions_client_status ON positions(client_id, status);")
            ex("CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);")
            ex("CREATE INDEX IF NOT EXISTS idx_positions_entry_ts ON positions(entry_ts);")
            ex("CREATE INDEX IF NOT EXISTS idx_positions_contract ON positions(contract);")
        except Exception:
            pass

        # ------------------------
        # CLIENTS
        # ------------------------
        ex(
            """
            CREATE TABLE IF NOT EXISTS clients (
                client_id TEXT PRIMARY KEY,
                api_key TEXT UNIQUE,

                name TEXT NOT NULL,
                broker_type TEXT NOT NULL,           -- 'tradier' or 'ibkr'
                broker_account_id TEXT NOT NULL,
                broker_token TEXT NOT NULL,
                broker_base_url TEXT NOT NULL,
                initial_equity REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'ACTIVE',
                created_at TEXT NOT NULL,

                max_trades_per_day INTEGER NOT NULL DEFAULT 5,
                max_concurrent_positions INTEGER NOT NULL DEFAULT 3,
                daily_max_loss_pct REAL NOT NULL DEFAULT 0.05,
                base_position_pct REAL NOT NULL DEFAULT 0.10
            );
            """
        )
        try:
            ex("CREATE INDEX IF NOT EXISTS idx_clients_status ON clients(status);")
            ex("CREATE INDEX IF NOT EXISTS idx_clients_api_key ON clients(api_key);")
        except Exception:
            pass

        # ------------------------
        # CLIENT STATE
        # ------------------------
        ex(
            """
            CREATE TABLE IF NOT EXISTS client_state (
                client_id TEXT PRIMARY KEY,
                current_equity REAL NOT NULL,
                starting_equity_today REAL NOT NULL,
                realized_pnl_today REAL NOT NULL DEFAULT 0.0,
                trades_taken_today INTEGER NOT NULL DEFAULT 0,
                daily_stop_hit INTEGER NOT NULL DEFAULT 0,
                kill_switch INTEGER NOT NULL DEFAULT 0,
                mode TEXT NOT NULL DEFAULT 'PAPER',
                last_heartbeat_ts TEXT,
                day_key TEXT,

                client_capital REAL,
                rental_fee REAL,
                working_capital REAL,
                profit_target_min REAL,
                profit_target_max REAL,
                subscription_end_date TEXT,
                growth_tracking_enabled INTEGER DEFAULT 0,
                account_type TEXT,

                FOREIGN KEY (client_id) REFERENCES clients(client_id)
            );
            """
        )

        for sql in [
            "ALTER TABLE client_state ADD COLUMN day_key TEXT;",
            "ALTER TABLE client_state ADD COLUMN client_capital REAL;",
            "ALTER TABLE client_state ADD COLUMN rental_fee REAL;",
            "ALTER TABLE client_state ADD COLUMN working_capital REAL;",
            "ALTER TABLE client_state ADD COLUMN profit_target_min REAL;",
            "ALTER TABLE client_state ADD COLUMN profit_target_max REAL;",
            "ALTER TABLE client_state ADD COLUMN subscription_end_date TEXT;",
            "ALTER TABLE client_state ADD COLUMN growth_tracking_enabled INTEGER DEFAULT 0;",
            "ALTER TABLE client_state ADD COLUMN account_type TEXT;",
        ]:
            try:
                ex(sql)
            except Exception:
                pass

        try:
            ex("CREATE INDEX IF NOT EXISTS idx_client_state_day_key ON client_state(day_key);")
        except Exception:
            pass


# =========================================================================
# DEDUPE HELPERS
# =========================================================================
def already_processed_signal(signal_id: str) -> bool:
    def _fn():
        with conn() as c:
            row = c.execute("SELECT 1 FROM processed_signals WHERE signal_id=?", (signal_id,)).fetchone()
            return row is not None
    return run_with_retry(_fn)


def mark_signal_processed(signal_id: str):
    def _fn():
        with conn() as c:
            c.execute(
                "INSERT OR IGNORE INTO processed_signals(signal_id, first_seen_ts) VALUES (?,?)",
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
                    client_id, local_order_id, broker_order_id, position_id, kind, status,
                    symbol, contract, direction, reserved_cost,
                    qty, limit_price, filled_qty, retries, last_error,
                    created_ts, updated_ts
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    client_id,
                    local_order_id,
                    broker_order_id,
                    position_id,
                    kind,
                    status,
                    symbol,
                    contract,
                    direction,
                    reserved_cost,
                    int(qty),
                    limit_price,
                    0,
                    0,
                    None,
                    ts,
                    ts,
                ),
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
    updates = []
    params: list[Any] = []

    if status is not None:
        updates.append("status=?")
        params.append(status)

    if broker_order_id is not None:
        updates.append("broker_order_id=?")
        params.append(broker_order_id)

    if last_error is not None:
        updates.append("last_error=?")
        params.append(last_error)

    if filled_qty is not None:
        updates.append("filled_qty=?")
        params.append(int(filled_qty))

    updates.append("updated_ts=?")
    params.append(now_utc_iso())
    params.append(local_order_id)

    sql = f"UPDATE orders SET {', '.join(updates)} WHERE local_order_id=?"

    def _fn():
        with conn() as c:
            c.execute(sql, tuple(params))

    return run_with_retry(_fn)


# =========================================================================
# CLIENT MANAGEMENT
# =========================================================================
def get_client(client_id: str) -> dict:
    with conn() as c:
        row = run_with_retry(lambda: c.execute("SELECT * FROM clients WHERE client_id=?", (client_id,)).fetchone())
        if not row:
            raise ValueError(f"Client not found: {client_id}")
        return dict(row)


def get_client_state(client_id: str = "default") -> dict:
    with conn() as c:
        row = run_with_retry(lambda: c.execute("SELECT * FROM client_state WHERE client_id=?", (client_id,)).fetchone())
        if not row:
            return {
                "client_id": client_id,
                "current_equity": 0.0,
                "starting_equity_today": 0.0,
                "realized_pnl_today": 0.0,
                "trades_taken_today": 0,
                "daily_stop_hit": 0,
                "kill_switch": 0,
                "mode": "PAPER",
                "day_key": None,
            }
        return dict(row)


def update_client_state(client_id: str = "default", updates: dict | None = None):
    if not updates:
        return

    # Ensure row exists
    with conn() as c:
        existing = run_with_retry(lambda: c.execute(
            "SELECT 1 FROM client_state WHERE client_id=?",
            (client_id,),
        ).fetchone())

        if not existing:
            run_with_retry(lambda: c.execute(
                """
                INSERT INTO client_state (
                    client_id,
                    current_equity,
                    starting_equity_today,
                    realized_pnl_today,
                    trades_taken_today,
                    daily_stop_hit,
                    kill_switch,
                    mode,
                    last_heartbeat_ts,
                    day_key
                )
                VALUES (?, 0, 0, 0.0, 0, 0, 0, 'PAPER', NULL, NULL)
                """,
                (client_id,),
            ))

    # Discover real columns
    with conn() as c:
        cols = run_with_retry(lambda: c.execute("PRAGMA table_info(client_state);").fetchall())

    allowed = set()
    for r in cols:
        try:
            allowed.add(r["name"])
        except Exception:
            allowed.add(r[1])

    safe_updates = {k: v for k, v in updates.items() if k in allowed}
    if not safe_updates:
        return

    set_clause = ", ".join(f"{k}=?" for k in safe_updates.keys())
    values = list(safe_updates.values()) + [client_id]

    with conn() as c:
        run_with_retry(lambda: c.execute(
            f"UPDATE client_state SET {set_clause} WHERE client_id=?",
            values,
        ))


# =========================================================================
# ADMIN & QUERY HELPERS (for admin_api.py)
# =========================================================================

def get_all_clients(status: str | None = None) -> list[dict]:
    """Get all clients, optionally filtered by status"""
    with conn() as c:
        if status:
            rows = run_with_retry(
                lambda: c.execute(
                    "SELECT * FROM clients WHERE status=? ORDER BY created_at DESC",
                    (status,)
                ).fetchall()
            )
        else:
            rows = run_with_retry(
                lambda: c.execute("SELECT * FROM clients ORDER BY created_at DESC").fetchall()
            )
        return [dict(r) for r in rows]


def create_client(
    client_id: str,
    name: str,
    broker_type: str,
    broker_account_id: str,
    broker_token: str,
    broker_base_url: str,
    initial_equity: float,
    max_trades_per_day: int = 5,
    max_concurrent_positions: int = 3,
    daily_max_loss_pct: float = 0.05,
    base_position_pct: float = 0.10,
    api_key: str | None = None,
) -> str:
    """Create a new client"""
    now = now_utc_iso()
    
    with conn() as c:
        run_with_retry(
            lambda: c.execute(
                """
                INSERT INTO clients (
                    client_id, api_key, name, broker_type, broker_account_id,
                    broker_token, broker_base_url, initial_equity, status,
                    created_at, max_trades_per_day, max_concurrent_positions,
                    daily_max_loss_pct, base_position_pct
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    client_id,
                    api_key,
                    name,
                    broker_type,
                    broker_account_id,
                    broker_token,
                    broker_base_url,
                    float(initial_equity),
                    "ACTIVE",
                    now,
                    int(max_trades_per_day),
                    int(max_concurrent_positions),
                    float(daily_max_loss_pct),
                    float(base_position_pct),
                ),
            )
        )
    
    # Create initial client_state
    with conn() as c:
        run_with_retry(
            lambda: c.execute(
                """
                INSERT INTO client_state (
                    client_id, current_equity, starting_equity_today,
                    realized_pnl_today, trades_taken_today, daily_stop_hit,
                    kill_switch, mode, day_key
                )
                VALUES (?, ?, ?, 0.0, 0, 0, 0, 'PAPER', NULL)
                """,
                (client_id, float(initial_equity), float(initial_equity)),
            )
        )
    
    return client_id


def upsert_client(client_id: str, **kwargs):
    """Insert or update client"""
    with conn() as c:
        existing = c.execute(
            "SELECT client_id FROM clients WHERE client_id=?",
            (client_id,)
        ).fetchone()

        now = now_utc_iso()

        if existing:
            # Update
            updates = []
            params = []
            for k, v in kwargs.items():
                updates.append(f"{k}=?")
                params.append(v)
            params.append(client_id)

            sql = f"UPDATE clients SET {', '.join(updates)} WHERE client_id=?"
            run_with_retry(lambda: c.execute(sql, params))
        else:
            # Insert
            kwargs.setdefault("status", "ACTIVE")
            kwargs.setdefault("created_at", now)
            kwargs["client_id"] = client_id

            cols = ", ".join(kwargs.keys())
            placeholders = ", ".join(["?"] * len(kwargs))
            sql = f"INSERT INTO clients ({cols}) VALUES ({placeholders})"
            run_with_retry(lambda: c.execute(sql, list(kwargs.values())))


def get_all_positions(client_id: str | None = None) -> list[dict]:
    """Get all positions, optionally filtered by client"""
    with conn() as c:
        if client_id:
            rows = run_with_retry(
                lambda: c.execute(
                    "SELECT * FROM positions WHERE client_id=? ORDER BY entry_ts DESC",
                    (client_id,)
                ).fetchall()
            )
        else:
            rows = run_with_retry(
                lambda: c.execute("SELECT * FROM positions ORDER BY entry_ts DESC").fetchall()
            )
        return [dict(r) for r in rows]


def get_orders_for_client(client_id: str) -> list[dict]:
    """Get all orders for a client"""
    with conn() as c:
        rows = run_with_retry(
            lambda: c.execute(
                "SELECT * FROM orders WHERE client_id=? ORDER BY created_ts DESC",
                (client_id,)
            ).fetchall()
        )
        return [dict(r) for r in rows]


def get_all_orders() -> list[dict]:
    """Get all orders"""
    with conn() as c:
        rows = run_with_retry(
            lambda: c.execute("SELECT * FROM orders ORDER BY created_ts DESC").fetchall()
        )
        return [dict(r) for r in rows]


def get_audit_logs(client_id: str | None = None, limit: int = 100) -> list[dict]:
    """Get audit logs, optionally filtered by client"""
    with conn() as c:
        if client_id:
            rows = run_with_retry(
                lambda: c.execute(
                    "SELECT * FROM audit_log WHERE client_id=? ORDER BY ts DESC LIMIT ?",
                    (client_id, limit)
                ).fetchall()
            )
        else:
            rows = run_with_retry(
                lambda: c.execute(
                    "SELECT * FROM audit_log ORDER BY ts DESC LIMIT ?",
                    (limit,)
                ).fetchall()
            )
        return [dict(r) for r in rows]


def get_position_by_id(position_id: str) -> dict | None:
    """Get position by ID"""
    with conn() as c:
        row = run_with_retry(
            lambda: c.execute(
                "SELECT * FROM positions WHERE id=?",
                (position_id,)
            ).fetchone()
        )
        return dict(row) if row else None


def get_order_by_id(local_order_id: str) -> dict | None:
    """Get order by local order ID"""
    with conn() as c:
        row = run_with_retry(
            lambda: c.execute(
                "SELECT * FROM orders WHERE local_order_id=?",
                (local_order_id,)
            ).fetchone()
        )
        return dict(row) if row else None


def delete_client(client_id: str):
    """Delete client and all related data"""
    with conn() as c:
        # Delete in reverse order of foreign keys
        run_with_retry(lambda: c.execute("DELETE FROM audit_log WHERE client_id=?", (client_id,)))
        run_with_retry(lambda: c.execute("DELETE FROM trade_queue WHERE client_id=?", (client_id,)))
        run_with_retry(lambda: c.execute("DELETE FROM orders WHERE client_id=?", (client_id,)))
        run_with_retry(lambda: c.execute("DELETE FROM positions WHERE client_id=?", (client_id,)))
        run_with_retry(lambda: c.execute("DELETE FROM client_state WHERE client_id=?", (client_id,)))
        run_with_retry(lambda: c.execute("DELETE FROM clients WHERE client_id=?", (client_id,)))


def update_client(client_id: str, **kwargs) -> dict:
    """Update client fields and return updated client"""
    if not kwargs:
        raise ValueError("No fields to update")
    
    with conn() as c:
        # Build UPDATE query
        updates = []
        params = []
        for k, v in kwargs.items():
            updates.append(f"{k}=?")
            params.append(v)
        params.append(client_id)
        
        sql = f"UPDATE clients SET {', '.join(updates)} WHERE client_id=?"
        run_with_retry(lambda: c.execute(sql, params))
        
        # Return updated client
        return get_client(client_id)


def list_orders(client_id: str | None = None, limit: int = 200, status: str | None = None) -> list[dict]:
    """List orders with optional filters"""
    with conn() as c:
        if client_id and status:
            rows = run_with_retry(
                lambda: c.execute(
                    "SELECT * FROM orders WHERE client_id=? AND status=? ORDER BY created_ts DESC LIMIT ?",
                    (client_id, status, limit)
                ).fetchall()
            )
        elif client_id:
            rows = run_with_retry(
                lambda: c.execute(
                    "SELECT * FROM orders WHERE client_id=? ORDER BY created_ts DESC LIMIT ?",
                    (client_id, limit)
                ).fetchall()
            )
        elif status:
            rows = run_with_retry(
                lambda: c.execute(
                    "SELECT * FROM orders WHERE status=? ORDER BY created_ts DESC LIMIT ?",
                    (status, limit)
                ).fetchall()
            )
        else:
            rows = run_with_retry(
                lambda: c.execute(
                    "SELECT * FROM orders ORDER BY created_ts DESC LIMIT ?",
                    (limit,)
                ).fetchall()
            )
        return [dict(r) for r in rows]


def list_positions(client_id: str | None = None, limit: int = 200, status: str | None = None) -> list[dict]:
    """List positions with optional filters"""
    with conn() as c:
        if client_id and status:
            rows = run_with_retry(
                lambda: c.execute(
                    "SELECT * FROM positions WHERE client_id=? AND status=? ORDER BY entry_ts DESC LIMIT ?",
                    (client_id, status, limit)
                ).fetchall()
            )
        elif client_id:
            rows = run_with_retry(
                lambda: c.execute(
                    "SELECT * FROM positions WHERE client_id=? ORDER BY entry_ts DESC LIMIT ?",
                    (client_id, limit)
                ).fetchall()
            )
        elif status:
            rows = run_with_retry(
                lambda: c.execute(
                    "SELECT * FROM positions WHERE status=? ORDER BY entry_ts DESC LIMIT ?",
                    (status, limit)
                ).fetchall()
            )
        else:
            rows = run_with_retry(
                lambda: c.execute(
                    "SELECT * FROM positions ORDER BY entry_ts DESC LIMIT ?",
                    (limit,)
                ).fetchall()
            )
        return [dict(r) for r in rows]


def list_audit(client_id: str | None = None, limit: int = 200) -> list[dict]:
    """List audit log events"""
    with conn() as c:
        if client_id:
            rows = run_with_retry(
                lambda: c.execute(
                    "SELECT * FROM audit_log WHERE client_id=? ORDER BY ts DESC LIMIT ?",
                    (client_id, limit)
                ).fetchall()
            )
        else:
            rows = run_with_retry(
                lambda: c.execute(
                    "SELECT * FROM audit_log ORDER BY ts DESC LIMIT ?",
                    (limit,)
                ).fetchall()
            )
        return [dict(r) for r in rows]


def get_open_orders_for_reconcile(client_id: str | None = None, limit: int = 200) -> list[dict]:
    """Get open/pending orders for reconciliation"""
    with conn() as c:
        if client_id:
            rows = run_with_retry(
                lambda: c.execute(
                    """
                    SELECT * FROM orders 
                    WHERE client_id=? 
                      AND status IN ('NEW', 'ACK', 'PARTIAL')
                    ORDER BY created_ts DESC 
                    LIMIT ?
                    """,
                    (client_id, limit)
                ).fetchall()
            )
        else:
            rows = run_with_retry(
                lambda: c.execute(
                    """
                    SELECT * FROM orders 
                    WHERE status IN ('NEW', 'ACK', 'PARTIAL')
                    ORDER BY created_ts DESC 
                    LIMIT ?
                    """,
                    (limit,)
                ).fetchall()
            )
        return [dict(r) for r in rows]
