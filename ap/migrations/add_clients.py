# ap/migrations/add_clients.py
"""
HARDENED Migration: Add multi-client support + client_id columns across trading tables.

Goals:
- Safe to run multiple times (idempotent).
- Works even if TRADIER env vars are missing (creates PAUSED default client).
- Ensures consistent schema with ap/db.py expectations (kv includes updated_at).
- Adds client_id columns with NOT NULL + DEFAULT 'default' where possible.
- Backfills NULL/empty client_id values to 'default'.
- Creates indexes for per-client queries.

Run:
  python -m ap.migrations.add_clients
"""
import sqlite3
import json

from ap.config import Config
from ap.utils import now_utc_iso
from ap.auth import generate_api_key
from ap.crypto import encrypt_token

cfg = Config()

DEFAULT_CLIENT_ID = "default"
DEFAULT_BROKER_BASE = "https://sandbox.tradier.com"


def _connect():
    conn = sqlite3.connect(cfg.DB_FILE, timeout=30)
    conn.row_factory = sqlite3.Row
    # Reasonable pragmas for migrations
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def _table_exists(conn, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def _has_column(conn, table: str, col: str) -> bool:
    try:
        rows = conn.execute(f"PRAGMA table_info({table});").fetchall()
        return any(r["name"] == col for r in rows)
    except Exception:
        return False


def _add_column(conn, table: str, col_def: str):
    """
    Best-effort ALTER TABLE ADD COLUMN.
    SQLite supports ADD COLUMN but not IF NOT EXISTS; tolerate duplicates.
    """
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_def};")
    except sqlite3.OperationalError as e:
        msg = str(e).lower()
        if "duplicate column" in msg or "already exists" in msg:
            return
        # If the table doesn't exist, ignore (some deployments may not have all tables yet)
        if "no such table" in msg:
            return
        raise


def _ensure_kv(conn):
    # Create kv with updated_at to match ap/db.py
    conn.execute("""
    CREATE TABLE IF NOT EXISTS kv (
        k TEXT PRIMARY KEY,
        v TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    """)
    # Older kv may lack updated_at
    if not _has_column(conn, "kv", "updated_at"):
        _add_column(conn, "kv", "updated_at TEXT")
    # Backfill updated_at
    try:
        conn.execute("UPDATE kv SET updated_at=? WHERE updated_at IS NULL OR updated_at='';", (now_utc_iso(),))
    except Exception:
        pass


def _ensure_clients_tables(conn):
    # clients table (matches ap/db.py)
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS clients (
        client_id TEXT PRIMARY KEY,
        api_key TEXT UNIQUE,

        name TEXT NOT NULL,
        broker_type TEXT NOT NULL,
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

    # client_state table (matches ap/db.py)
    conn.execute("""
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


def _ensure_client_id_columns(conn):
    """
    Add client_id to trading tables if they exist.
    Use NOT NULL + DEFAULT for new rows and safe queries.
    """
    for table in ("positions", "orders", "trade_queue", "audit_log"):
        if not _table_exists(conn, table):
            continue

        # Prefer NOT NULL DEFAULT. SQLite supports this in ADD COLUMN.
        _add_column(conn, table, f"client_id TEXT NOT NULL DEFAULT '{DEFAULT_CLIENT_ID}'")

        # Backfill NULL/empty just in case
        try:
            conn.execute(
                f"UPDATE {table} SET client_id=? WHERE client_id IS NULL OR client_id='';",
                (DEFAULT_CLIENT_ID,),
            )
        except Exception:
            pass


def _create_indexes(conn):
    idx = [
        # client lookup
        "CREATE INDEX IF NOT EXISTS idx_clients_status ON clients(status);",
        "CREATE INDEX IF NOT EXISTS idx_clients_api_key ON clients(api_key);",

        # per-client query speed
        "CREATE INDEX IF NOT EXISTS idx_positions_client_id ON positions(client_id);",
        "CREATE INDEX IF NOT EXISTS idx_orders_client_id ON orders(client_id);",
        "CREATE INDEX IF NOT EXISTS idx_trade_queue_client_id ON trade_queue(client_id);",
        "CREATE INDEX IF NOT EXISTS idx_audit_client_id ON audit_log(client_id);",

        # helpful composites (ignore if columns missing)
        "CREATE INDEX IF NOT EXISTS idx_orders_client_status_created ON orders(client_id, status, created_ts);",
        "CREATE INDEX IF NOT EXISTS idx_positions_client_status_entry ON positions(client_id, status, entry_ts);",
    ]
    for sql in idx:
        try:
            conn.execute(sql)
        except Exception:
            # Some indexes reference columns that might not exist yet on older tables.
            # Don't fail the migration for that.
            pass


def _infer_existing_equity(conn) -> float:
    """
    Try to infer current equity from existing kv state. Falls back to 100000.
    """
    try:
        row = conn.execute("SELECT v FROM kv WHERE k='bot_state'").fetchone()
        if not row:
            return 100000.0
        state = json.loads(row["v"])
        return float(state.get("current_equity_last", 100000.0))
    except Exception:
        return 100000.0


def _ensure_default_client(conn):
    """
    Ensure default client exists for backward compatibility.
    Creates an API key ONCE and prints it.
    If Tradier env vars are missing, sets status=PAUSED and stores placeholder token.
    """
    row = conn.execute("SELECT client_id, api_key FROM clients WHERE client_id=?", (DEFAULT_CLIENT_ID,)).fetchone()
    if row:
        print("  ↳ Default client already exists")
        return

    current_equity = _infer_existing_equity(conn)

    # Determine broker config
    tradier_account_id = (getattr(cfg, "TRADIER_ACCOUNT_ID", "") or "").strip()
    tradier_access_token = (getattr(cfg, "TRADIER_ACCESS_TOKEN", "") or "").strip()
    tradier_base_url = (getattr(cfg, "TRADIER_BASE_URL", "") or "").strip() or DEFAULT_BROKER_BASE

    # Generate API key
    default_api_key = generate_api_key("ak")

    if not tradier_account_id or not tradier_access_token:
        # Create PAUSED default client until creds are provided
        status = "PAUSED"
        broker_type = "tradier"
        broker_account_id = tradier_account_id or "MISSING"
        broker_token_enc = encrypt_token("MISSING")
        broker_base = tradier_base_url
        print("  ⚠️ Tradier env vars missing; creating default client as PAUSED.")
    else:
        status = "ACTIVE"
        broker_type = "tradier"
        broker_account_id = tradier_account_id
        broker_token_enc = encrypt_token(tradier_access_token)
        broker_base = tradier_base_url

    conn.execute("""
    INSERT INTO clients (
        client_id, api_key, name, broker_type, broker_account_id, broker_token,
        broker_base_url, initial_equity, status, created_at,
        max_trades_per_day, max_concurrent_positions, daily_max_loss_pct, base_position_pct
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        DEFAULT_CLIENT_ID,
        default_api_key,
        "Default Account",
        broker_type,
        broker_account_id,
        broker_token_enc,
        broker_base,
        float(current_equity),
        status,
        now_utc_iso(),
        5,     # max_trades_per_day
        3,     # max_concurrent_positions
        0.05,  # daily_max_loss_pct
        0.10,  # base_position_pct
    ))

    conn.execute("""
    INSERT INTO client_state (
        client_id, current_equity, starting_equity_today, realized_pnl_today, trades_taken_today,
        daily_stop_hit, kill_switch, mode, last_heartbeat_ts
    ) VALUES (?, ?, ?, 0.0, 0, 0, 0, 'PAPER', NULL)
    """, (
        DEFAULT_CLIENT_ID,
        float(current_equity),
        float(current_equity),
    ))

    print(f"  ✅ Default client created with API key: {default_api_key}")
    print("  ⚠️  SAVE THIS API KEY - you won't see it again!")


def migrate():
    conn = _connect()
    try:
        print("🔄 Starting HARDENED multi-client migration...")

        # 0) Ensure kv matches expected schema
        _ensure_kv(conn)

        # 1) Create multi-client tables
        print("Creating clients/client_state tables...")
        _ensure_clients_tables(conn)

        # 2) Add client_id columns across existing tables
        print("Adding client_id columns to trading tables (if present)...")
        _ensure_client_id_columns(conn)

        # 3) Indexes
        print("Creating indexes...")
        _create_indexes(conn)

        # 4) Default client (backward compatibility)
        print("Ensuring default client exists...")
        _ensure_default_client(conn)

        conn.commit()
        print("✅ Migration complete!")

    except Exception as e:
        conn.rollback()
        print(f"❌ Migration failed: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    migrate()
