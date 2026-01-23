import sqlite3
from contextlib import contextmanager
from ap.config import Config

cfg = Config()

@contextmanager
@contextmanager
def conn():
    c = sqlite3.connect(cfg.DB_FILE, timeout=30, isolation_level=None, check_same_thread=False)  # ✅ Added parameter
    c.row_factory = sqlite3.Row
    
    # ✅ Enable WAL mode and busy timeout
    c.execute("PRAGMA journal_mode=WAL;")
    c.execute("PRAGMA busy_timeout=30000;")
    
    try:
        yield c
    finally:
        c.close()

def init_db():
    with conn() as c:
        c.execute("PRAGMA journal_mode=WAL;")
        c.execute("PRAGMA foreign_keys=ON;")

        c.execute("""
        CREATE TABLE IF NOT EXISTS kv (
            k TEXT PRIMARY KEY,
            v TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            level TEXT NOT NULL,
            event TEXT NOT NULL,
            payload TEXT NOT NULL
        );
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS trade_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id TEXT NOT NULL,
            created_ts TEXT NOT NULL,
            status TEXT NOT NULL, -- NEW | PROCESSING | DONE | REJECTED
            payload TEXT NOT NULL,
            decision TEXT,
            reason TEXT
        );
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            local_order_id TEXT NOT NULL,
            broker_order_id TEXT,
            position_id TEXT,
            kind TEXT NOT NULL, -- ENTRY | EXIT | FLATTEN
            status TEXT NOT NULL, -- NEW | ACK | PARTIAL | FILLED | REJECTED | CANCELED
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
            status TEXT NOT NULL, -- OPEN | CLOSING | CLOSED
            exit_ts TEXT,
            exit_reason TEXT,
            realized_pnl REAL
        );
        """)

