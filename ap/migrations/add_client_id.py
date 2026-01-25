# ap/migrations/add_client_id.py
"""
Migration: Add client_id to existing tables for multi-client support
Safe to run multiple times (idempotent)
"""
import sqlite3
from ap.config import Config
from ap.logger import get_logger

cfg = Config()
log = get_logger("ap.migrations")


def migrate():
    """Add client_id columns to existing tables"""
    conn = sqlite3.connect(cfg.DB_FILE, timeout=30)
    conn.row_factory = sqlite3.Row
    
    try:
        cursor = conn.cursor()
        
        # Add client_id to orders table
        try:
            cursor.execute("ALTER TABLE orders ADD COLUMN client_id TEXT")
            log.info("✅ Added client_id to orders table")
        except sqlite3.OperationalError as e:
            if "duplicate column" in str(e).lower():
                log.info("⏭️  client_id already exists in orders table")
            else:
                raise
        
        # Add client_id to positions table
        try:
            cursor.execute("ALTER TABLE positions ADD COLUMN client_id TEXT")
            log.info("✅ Added client_id to positions table")
        except sqlite3.OperationalError as e:
            if "duplicate column" in str(e).lower():
                log.info("⏭️  client_id already exists in positions table")
            else:
                raise
        
        # Add client_id to trade_queue table
        try:
            cursor.execute("ALTER TABLE trade_queue ADD COLUMN client_id TEXT")
            log.info("✅ Added client_id to trade_queue table")
        except sqlite3.OperationalError as e:
            if "duplicate column" in str(e).lower():
                log.info("⏭️  client_id already exists in trade_queue table")
            else:
                raise
        
        # Create indexes for client_id
        try:
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_client_id ON orders(client_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_positions_client_id ON positions(client_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_queue_client_id ON trade_queue(client_id)")
            log.info("✅ Created client_id indexes")
        except Exception as e:
            log.warning(f"Index creation warning: {e}")
        
        conn.commit()
        log.info("✅ Multi-client migration completed successfully")
        
    except Exception as e:
        conn.rollback()
        log.error(f"❌ Migration failed: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    migrate()
