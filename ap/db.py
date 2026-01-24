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
        isolation_level=None,  # autocommit
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
    """
    Single source of truth schema for beta.
    Safe to run repeatedly.
    """
    with conn() as c:
        # KV
        c.execute("""
        CREATE TABLE IF NOT EXISTS kv (
            k TEXT PRIMARY KEY,
            v TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """)

        # Audit log
        c.execute("""
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            level TEXT NOT NULL,
            event TEXT NOT NULL,
            payload TEXT NOT NULL
        );
        """)

        # Dedupe table (idempotency)
        c.execute("""
        CREATE TABLE IF NOT EXISTS processed_signals (
            signal_id TEXT PRIMARY KEY,
            first_seen_ts TEXT NOT NULL
        );
        """)

        # Trade queue
        c.execute("""
        CREATE TABLE IF NOT EXISTS trade_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id TEXT NOT NULL,
            created_ts TEXT NOT NULL,
            status TEXT NOT NULL,     -- NEW | PROCESSING | DONE | REJECTED
            payload TEXT NOT NULL,
            decision TEXT,
            reason TEXT,
            details TEXT
        );
        """)
        # migration
        try:
            c.execute("ALTER TABLE trade_queue ADD COLUMN details TEXT;")
        except Exception:
            pass

        # Orders (execution truth)
        c.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            local_order_id TEXT NOT NULL,
            broker_order_id TEXT,
            position_id TEXT,
            kind TEXT NOT NULL,       -- ENTRY | EXIT | FLATTEN
            status TEXT NOT NULL,     -- NEW | ACK | PARTIAL | FILLED | REJECTED | CANCELED
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

        # Positions
        c.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            id TEXT PRIMARY KEY,
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
        """)

        # Indexes
        try:
            c.execute("CREATE INDEX IF NOT EXISTS idx_processed_signals_ts ON processed_signals(first_seen_ts);")
            c.execute("CREATE INDEX IF NOT EXISTS idx_trade_queue_status_id ON trade_queue(status, id);")
            c.execute("CREATE INDEX IF NOT EXISTS idx_trade_queue_created_ts ON trade_queue(created_ts);")
            c.execute("CREATE INDEX IF NOT EXISTS idx_trade_queue_signal_id ON trade_queue(signal_id);")

            c.execute("CREATE INDEX IF NOT EXISTS idx_orders_local_order_id ON orders(local_order_id);")
            c.execute("CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);")
            c.execute("CREATE INDEX IF NOT EXISTS idx_orders_created_ts ON orders(created_ts);")
            c.execute("CREATE INDEX IF NOT EXISTS idx_orders_broker_order_id ON orders(broker_order_id);")

            c.execute("CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);")
            c.execute("CREATE INDEX IF NOT EXISTS idx_positions_entry_ts ON positions(entry_ts);")
            c.execute("CREATE INDEX IF NOT EXISTS idx_positions_contract ON positions(contract);")
        except Exception:
            pass


# --- helpers used by ap/queue.py ---
def already_processed_signal(signal_id: str) -> bool:
    def _fn():
        with conn() as c:
            row = c.execute("SELECT 1 FROM processed_signals WHERE signal_id=?", (signal_id,)).fetchone()
            return row is not None
    return run_with_retry(_fn)


def mark_signal_processed(signal_id: str):
    from ap.utils import now_utc_iso
    def _fn():
        with conn() as c:
            c.execute(
                "INSERT OR IGNORE INTO processed_signals(signal_id, first_seen_ts) VALUES (?,?)",
                (signal_id, now_utc_iso())
            )
    return run_with_retry(_fn)
