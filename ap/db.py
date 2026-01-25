# ap/db.py
import time
import uuid
import sqlite3
from contextlib import contextmanager

from ap.config import Config
from ap.utils import now_utc_iso

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


def init_db():
    """
    Single source of truth schema for beta (single-client) + safe multi-client tables.
    Safe to run repeatedly.
    """
    with conn() as c:
        # -------------------------
        # KV
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
        # Dedupe (idempotency)
        # -------------------------
        c.execute("""
        CREATE TABLE IF NOT EXISTS processed_signals (
            signal_id TEXT PRIMARY KEY,
            first_seen_ts TEXT NOT NULL
        );
        """)

        # -------------------------
        # Trade queue
        # -------------------------
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
        # Migration for older DBs
        try:
            c.execute("ALTER TABLE trade_queue ADD COLUMN details TEXT;")
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

        # -------------------------
        # Positions
        # -------------------------
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

        # -------------------------
        # Multi-client tables (safe to exist even if unused)
        # -------------------------
        c.execute("""
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
        """)

        c.execute("""
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
            FOREIGN KEY (client_id) REFERENCES clients(client_id)
        );
        """)

        # -------------------------
        # Indexes
        # -------------------------
        try:
            c.execute("CREATE INDEX IF NOT EXISTS idx_processed_signals_ts ON processed_signals(first_seen_ts);")

            c.execute("CREATE INDEX IF NOT EXISTS idx_trade_queue_status_id ON trade_queue(status, id);")
            c.execute("CREATE INDEX IF NOT EXISTS idx_trade_queue_created_ts ON trade_queue(created_ts);")
            c.execute("CREATE INDEX IF NOT EXISTS idx_trade_queue_signal_id ON trade_queue(signal_id);")

            c.execute("CREATE INDEX IF NOT EXISTS idx_orders_local_order_id ON orders(local_order_id);")
            c.execute("CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);")
            c.execute("CREATE INDEX IF NOT EXISTS idx_orders_created_ts ON orders(created_ts);")
            c.execute("CREATE INDEX IF NOT EXISTS idx_orders_broker_order_id ON orders(broker_order_id);")
            c.execute("CREATE INDEX IF NOT EXISTS idx_orders_position_id ON orders(position_id);")

            c.execute("CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);")
            c.execute("CREATE INDEX IF NOT EXISTS idx_positions_entry_ts ON positions(entry_ts);")
            c.execute("CREATE INDEX IF NOT EXISTS idx_positions_contract ON positions(contract);")

            c.execute("CREATE INDEX IF NOT EXISTS idx_clients_status ON clients(status);")
            c.execute("CREATE INDEX IF NOT EXISTS idx_clients_api_key ON clients(api_key);")
        except Exception:
            pass


# =========================================================================
# DEDUPE HELPERS (used by ap/queue.py)
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
# ORDER HELPERS (used by exit_manager + reconciliation)
# =========================================================================

def new_local_order_id() -> str:
    return str(uuid.uuid4())


def insert_order(
    *,
    local_order_id: str,
    position_id: str | None,
    kind: str,
    status: str,
    symbol: str,
    contract: str,
    qty: int,
    limit_price: float | None = None,
    broker_order_id: str | None = None,
):
    ts = now_utc_iso()

    def _fn():
        with conn() as c:
            c.execute(
                """
                INSERT INTO orders (
                    local_order_id, broker_order_id, position_id, kind, status,
                    symbol, contract, qty, limit_price, filled_qty, retries, last_error,
                    created_ts, updated_ts
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    local_order_id,
                    broker_order_id,
                    position_id,
                    kind,
                    status,
                    symbol,
                    contract,
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
    params = []

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
# REPORTING + RECONCILIATION READS
# =========================================================================

def list_orders(limit: int = 200):
    def _fn():
        with conn() as c:
            rows = c.execute("SELECT * FROM orders ORDER BY created_ts DESC LIMIT ?", (limit,)).fetchall()
            return [dict(r) for r in rows]
    return run_with_retry(_fn)


def list_positions(limit: int = 200, status: str = "ALL"):
    status = (status or "ALL").upper()

    def _fn():
        with conn() as c:
            if status != "ALL":
                rows = c.execute(
                    "SELECT * FROM positions WHERE status=? ORDER BY entry_ts DESC LIMIT ?",
                    (status, limit),
                ).fetchall()
            else:
                rows = c.execute(
                    "SELECT * FROM positions ORDER BY entry_ts DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            return [dict(r) for r in rows]
    return run_with_retry(_fn)


def list_audit(limit: int = 200):
    def _fn():
        with conn() as c:
            rows = c.execute(
                "SELECT ts, level, event, payload FROM audit_log ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]
    return run_with_retry(_fn)


def get_open_orders_for_reconcile(limit: int = 200):
    def _fn():
        with conn() as c:
            rows = c.execute(
                """
                SELECT * FROM orders
                WHERE broker_order_id IS NOT NULL
                  AND broker_order_id != 'N/A'
                  AND status IN ('NEW','ACK','PARTIAL')
                ORDER BY updated_ts ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]
    return run_with_retry(_fn)


# =========================================================================
# CLIENT MANAGEMENT (ONLY for admin endpoints; safe because schema exists)
# =========================================================================

def get_client(client_id: str) -> dict:
    with conn() as c:
        row = run_with_retry(lambda: c.execute(
            "SELECT * FROM clients WHERE client_id=?",
            (client_id,),
        ).fetchone())
        if not row:
            raise ValueError(f"Client not found: {client_id}")
        return dict(row)


def get_all_clients(status: str | None = None) -> list:
    with conn() as c:
        if status:
            rows = run_with_retry(lambda: c.execute(
                "SELECT * FROM clients WHERE status=? ORDER BY created_at DESC",
                (status,),
            ).fetchall())
        else:
            rows = run_with_retry(lambda: c.execute(
                "SELECT * FROM clients ORDER BY created_at DESC",
            ).fetchall())
        return [dict(r) for r in rows]


def create_client(
    client_id: str,
    name: str,
    broker_type: str,
    broker_account_id: str,
    broker_token: str,
    broker_base_url: str,
    initial_equity: float,
    **kwargs,
) -> dict:
    with conn() as c:
        run_with_retry(lambda: c.execute(
            """
            INSERT INTO clients (
                client_id, name, broker_type, broker_account_id, broker_token,
                broker_base_url, initial_equity, status, created_at,
                max_trades_per_day, max_concurrent_positions,
                daily_max_loss_pct, base_position_pct
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?, ?, ?, ?, ?)
            """,
            (
                client_id,
                name,
                broker_type,
                broker_account_id,
                broker_token,
                broker_base_url,
                float(initial_equity),
                now_utc_iso(),
                int(kwargs.get("max_trades_per_day", 5)),
                int(kwargs.get("max_concurrent_positions", 3)),
                float(kwargs.get("daily_max_loss_pct", 0.05)),
                float(kwargs.get("base_position_pct", 0.10)),
            ),
        ))

        run_with_retry(lambda: c.execute(
            """
            INSERT INTO client_state (
                client_id, current_equity, starting_equity_today,
                realized_pnl_today, trades_taken_today, mode
            )
            VALUES (?, ?, ?, 0.0, 0, 'PAPER')
            """,
            (client_id, float(initial_equity), float(initial_equity)),
        ))

    return get_client(client_id)


def update_client(client_id: str, **updates) -> dict:
    allowed = {
        "name", "broker_account_id", "broker_token", "broker_base_url",
        "status", "max_trades_per_day", "max_concurrent_positions",
        "daily_max_loss_pct", "base_position_pct",
    }
    updates = {k: v for k, v in updates.items() if k in allowed}
    if not updates:
        return get_client(client_id)

    set_clause = ", ".join(f"{k}=?" for k in updates.keys())
    values = list(updates.values()) + [client_id]

    with conn() as c:
        run_with_retry(lambda: c.execute(
            f"UPDATE clients SET {set_clause} WHERE client_id=?",
            values,
        ))
    return get_client(client_id)


def get_client_state(client_id: str) -> dict:
    with conn() as c:
        row = run_with_retry(lambda: c.execute(
            "SELECT * FROM client_state WHERE client_id=?",
            (client_id,),
        ).fetchone())
        if not row:
            raise ValueError(f"Client state not found: {client_id}")
        return dict(row)


def update_client_state(client_id: str, updates: dict):
    if not updates:
        return

    set_clause = ", ".join(f"{k}=?" for k in updates.keys())
    values = list(updates.values()) + [client_id]

    with conn() as c:
        run_with_retry(lambda: c.execute(
            f"UPDATE client_state SET {set_clause} WHERE client_id=?",
            values,
        ))
