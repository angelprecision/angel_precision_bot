
# ap/migrations/add_clients.py
"""
Migration: Add multi-client support tables
Safe to run multiple times (uses IF NOT EXISTS)
"""
import sqlite3
from ap.config import Config
from ap.utils import now_utc_iso

cfg = Config()


def migrate():
    """Add client tables and update existing tables"""
    conn = sqlite3.connect(cfg.DB_FILE)
    conn.row_factory = sqlite3.Row
    
    try:
        print("🔄 Starting multi-client migration...")
        
        # =====================
        # 1. CREATE CLIENT TABLES
        # =====================
        
        print("Creating clients table...")
        conn.execute("""
        CREATE TABLE IF NOT EXISTS clients (
            client_id TEXT PRIMARY KEY,
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
        
        # =====================
        # 2. ADD client_id TO EXISTING TABLES
        # =====================
        
        print("Adding client_id to positions...")
        try:
            conn.execute("ALTER TABLE positions ADD COLUMN client_id TEXT DEFAULT 'default';")
        except sqlite3.OperationalError as e:
            if "duplicate column" not in str(e).lower():
                raise
            print("  ↳ Column already exists")
        
        print("Adding client_id to orders...")
        try:
            conn.execute("ALTER TABLE orders ADD COLUMN client_id TEXT DEFAULT 'default';")
        except sqlite3.OperationalError as e:
            if "duplicate column" not in str(e).lower():
                raise
            print("  ↳ Column already exists")
        
        print("Adding client_id to trade_queue...")
        try:
            conn.execute("ALTER TABLE trade_queue ADD COLUMN client_id TEXT DEFAULT 'default';")
        except sqlite3.OperationalError as e:
            if "duplicate column" not in str(e).lower():
                raise
            print("  ↳ Column already exists")
        
        # =====================
        # 3. CREATE INDEXES
        # =====================
        
        print("Creating indexes...")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_clients_status ON clients(status);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_positions_client_id ON positions(client_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_client_id ON orders(client_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trade_queue_client_id ON trade_queue(client_id);")
        
        # =====================
        # 4. CREATE DEFAULT CLIENT (backward compatibility)
        # =====================
        
        print("Checking for default client...")
        row = conn.execute("SELECT 1 FROM clients WHERE client_id='default'").fetchone()
        
        if not row:
            print("Creating default client from current state...")
            
            # Get current state from kv table
            state_row = conn.execute("SELECT v FROM kv WHERE k='bot_state'").fetchone()
            
            import json
            if state_row:
                state = json.loads(state_row[0])
                current_equity = state.get("current_equity_last", 100000.0)
            else:
                current_equity = 100000.0
            
            # Create default client
            conn.execute("""
            INSERT INTO clients (
                client_id, name, broker_type, broker_account_id, broker_token,
                broker_base_url, initial_equity, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                "default",
                "Default Account",
                "tradier",
                "VA12345678",  # Placeholder - update via API
                "REPLACE_ME",  # Placeholder - update via API
                "https://sandbox.tradier.com",
                current_equity,
                "ACTIVE",
                now_utc_iso()
            ))
            
            # Create default client state
            conn.execute("""
            INSERT INTO client_state (
                client_id, current_equity, starting_equity_today,
                realized_pnl_today, trades_taken_today, mode
            ) VALUES (?, ?, ?, ?, ?, ?)
            """, (
                "default",
                current_equity,
                current_equity,
                0.0,
                0,
                "PAPER"
            ))
            
            print("  ✅ Default client created")
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
