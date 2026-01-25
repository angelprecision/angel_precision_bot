# ap/migrations/add_clients.py
"""
Migration: Add multi-client support tables
Safe to run multiple times (uses IF NOT EXISTS / tolerate duplicate columns)
"""
import sqlite3
import json
from ap.config import Config
from ap.utils import now_utc_iso
from ap.auth import generate_api_key
from ap.crypto import encrypt_token

cfg = Config()


def migrate():
    conn = sqlite3.connect(cfg.DB_FILE)
    conn.row_factory = sqlite3.Row

    try:
        print("🔄 Starting multi-client migration...")

        # 0) Ensure kv exists (older DBs)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS kv (
            k TEXT PRIMARY KEY,
            v TEXT NOT NULL
        );
        """)

        # 1) CLIENTS TABLE (includes api_key!)
        print("Creating clients table...")
        conn.execute("""
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

            -- Risk limits
            max_trades_per_day INTEGER NOT NULL DEFAULT 5,
            max_concurrent_positions INTEGER NOT NULL DEFAULT 3,
            daily_max_loss_pct REAL NOT NULL DEFAULT 0.05,
            base_position_pct REAL NOT NULL DEFAULT 0.10
        );
        """)

        print("Creating client_state table...")
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

        # 2) ADD client_id TO EXISTING TABLES
        def add_col(table: str, col_def: str):
            try:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_def};")
            except sqlite3.OperationalError as e:
                if "duplicate column" in str(e).lower():
                    print(f"  ↳ {table}.{col_def.split()[0]} already exists")
                    return
                raise

        print("Adding client_id columns...")
        add_col("positions", "client_id TEXT DEFAULT 'default'")
        add_col("orders", "client_id TEXT DEFAULT 'default'")
        add_col("trade_queue", "client_id TEXT DEFAULT 'default'")
        add_col("audit_log", "client_id TEXT DEFAULT 'default'")

        # 3) INDEXES
        print("Creating indexes...")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_clients_status ON clients(status);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_clients_api_key ON clients(api_key);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_positions_client_id ON positions(client_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_client_id ON orders(client_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trade_queue_client_id ON trade_queue(client_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_client_id ON audit_log(client_id);")

        # 4) DEFAULT CLIENT (backward compatibility)
        print("Ensuring default client exists...")
        row = conn.execute("SELECT 1 FROM clients WHERE client_id='default'").fetchone()

        if not row:
            # Try pull existing bot state for equity
            state_row = conn.execute("SELECT v FROM kv WHERE k='bot_state'").fetchone()
            if state_row:
                try:
                    state = json.loads(state_row["v"])
                    current_equity = float(state.get("current_equity_last", 100000.0))
                except Exception:
                    current_equity = 100000.0
            else:
                current_equity = 100000.0

            # Generate API key for default client
            default_api_key = generate_api_key("ak")
            
            # Encrypt the Tradier token
            encrypted_token = encrypt_token(cfg.TRADIER_ACCESS_TOKEN)

            conn.execute("""
            INSERT INTO clients (
                client_id, api_key, name, broker_type, broker_account_id, broker_token,
                broker_base_url, initial_equity, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                "default",
                default_api_key,  # Generated API key
                "Default Account",
                "tradier",
                cfg.TRADIER_ACCOUNT_ID,  # Real account ID from env
                encrypted_token,  # Encrypted token
                cfg.TRADIER_BASE_URL or "https://sandbox.tradier.com",
                current_equity,
                "ACTIVE",
                now_utc_iso()
            ))

            conn.execute("""
            INSERT INTO client_state (
                client_id, current_equity, starting_equity_today, realized_pnl_today, trades_taken_today, mode
            ) VALUES (?, ?, ?, ?, ?, ?)
            """, (
                "default",
                current_equity,
                current_equity,
                0.0,
                0,
                "PAPER"
            ))

            print(f"  ✅ Default client created with API key: {default_api_key}")
            print(f"  ⚠️  SAVE THIS API KEY - you won't see it again!")
        else:
            print("  ↳ Default client already exists")

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
