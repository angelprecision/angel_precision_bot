# ap/db.py
import time
import sqlite3
from contextlib import contextmanager
from ap.config import Config

cfg = Config()

def run_with_retry(fn, retries: int = 12, base_sleep: float = 0.05, max_sleep: float = 1.0):
    """
    Retry SQLite operations that fail with 'database is locked/busy'.
    Exponential backoff.
    """
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

    # final attempt (or re-raise last locked error)
    try:
        return fn()
    except Exception:
        if last_err:
            raise last_err
        raise


@contextmanager
def conn():
    c = sqlite3.connect(
        cfg.DB_FILE,
        timeout=30,
        isolation_level=None,          # autocommit
        check_same_thread=False
    )
    c.row_factory = sqlite3.Row

    # DB pragmas
    c.execute("PRAGMA journal_mode=WAL;")
    c.execute("PRAGMA busy_timeout=30000;")
    c.execute("PRAGMA foreign_keys=ON;")

    try:
        yield c
    finally:
        c.close()


def init_db():
    with conn() as c:
        # -------------------------
        # Core KV store
        # -------------------------
        c.execute("""
        CREATE TABLE IF NOT EXISTS kv (
            k TEXT PRIMARY KEY,
            v TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """)

        # -------------------------
        # Audit log
        # -------------------------
        c.execute("""
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            level TEXT NOT NULL,
            event TEXT NOT NULL,
            payload TEXT NOT NULL
        );
        """)

        # -------------------------
        # Trade queue (NOW includes details)
        # -------------------------
        c.execute("""
        CREATE TABLE IF NOT EXISTS trade_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id TEXT NOT NULL,
            created_ts TEXT NOT NULL,
            status TEXT NOT NULL,    -- NEW | PROCESSING | DONE | REJECTED
            payload TEXT NOT NULL,
            decision TEXT,
            reason TEXT,
            details TEXT
        );
        """)

        # Migration: add details column if table already existed before
        try:
            c.execute("ALTER TABLE trade_queue ADD COLUMN details TEXT;")
        except Exception:
            pass

        # Helpful indexes
        try:
            c.execute("CREATE INDEX IF NOT EXISTS idx_trade_queue_status_id ON trade_queue(status, id);")
        except Exception:
            pass

        # -------------------------
        # Orders
        # -------------------------
        c.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            local_order_id TEXT NOT NULL,
            broker_order_id TEXT,
            position_id TEXT,
            kind TEXT NOT NULL,      -- ENTRY | EXIT | FLATTEN
            status TEXT NOT NULL,    -- NEW | ACK | PARTIAL | FILLED | REJECTED | CANCELED
            symbol TEXT NOT NULL,
            contract TEXT NOT NULL,
            qty INTEGER NOT NULL,
            limit_price REAL,
            filled_qty INTEGER NOT NULL DEFAULT 0,
            retries INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            created_ts TEXT NOT NULL,
            updated_ts TEXT NOT NULL
        );
        """)

        try:
            c.execute("CREATE INDEX IF NOT EXISTS idx_orders_local_order_id ON orders(local_order_id);")
            c.execute("CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);")
        except Exception:
            pass

        # -------------------------
        # Positions
        # -------------------------
        c.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            id TEXT PRIMARY KEY,
            underlying TEXT NOT NULL,
            contract TEXT NOT NULL,
            direction TEXT NOT NULL, -- CALL | PUT
            qty INTEGER NOT NULL,
            avg_fill REAL NOT NULL,
            entry_ts TEXT NOT NULL,
            tp_pct REAL NOT NULL,
            sl_pct REAL NOT NULL,
            status TEXT NOT NULL,    -- OPEN | CLOSING | CLOSED
            exit_ts TEXT,
            exit_reason TEXT,
            realized_pnl REAL
        );
        """)

        try:
            c.execute("CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);")
        except Exception:
            pass
