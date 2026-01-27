# initialize_client.py - Set up your client for trading
"""
Run this script to initialize or fix your client configuration.

Usage:
    python initialize_client.py
"""

import os
import sys
from ap.db import conn, run_with_retry, update_client_state, update_client
from ap.client_manager import create_new_client
from ap.logger import get_logger

log = get_logger("initialize")


def get_or_create_client(client_id: str = "default") -> dict:
    """Get existing client or create new one"""
    
    with conn() as c:
        row = run_with_retry(lambda: c.execute(
            "SELECT * FROM clients WHERE client_id=?",
            (client_id,)
        ).fetchone())
        
        if row:
            log.info(f"✅ Found existing client: {client_id}")
            return dict(row)
    
    # Create new client
    log.info(f"Creating new client: {client_id}")
    
    tradier_account_id = os.getenv("TRADIER_ACCOUNT_ID", "").strip()
    tradier_token = os.getenv("TRADIER_ACCESS_TOKEN", "").strip()
    tradier_base_url = os.getenv("TRADIER_BASE_URL", "https://sandbox.tradier.com").strip()
    
    if not tradier_account_id or not tradier_token:
        print("\n❌ ERROR: Missing Tradier credentials!")
        print("Set these environment variables:")
        print("  - TRADIER_ACCOUNT_ID")
        print("  - TRADIER_ACCESS_TOKEN")
        print("  - TRADIER_BASE_URL (optional, defaults to sandbox)")
        sys.exit(1)
    
    client = create_new_client(
        name=f"Trading Client {client_id}",
        tradier_account_id=tradier_account_id,
        tradier_access_token=tradier_token,
        tradier_base_url=tradier_base_url,
        initial_equity=100000.0,
        max_trades_per_day=5,
        max_concurrent_positions=2,
        daily_max_loss_pct=0.05,
        base_position_pct=0.15
    )
    
    print(f"\n🔑 API Key: {client['api_key']}")
    print("⚠️  SAVE THIS - you won't see it again!\n")
    
    return client


def initialize_client_state(client_id: str, equity: float = 100000.0):
    """Initialize or reset client state"""
    
    log.info(f"Initializing state for {client_id}")
    
    # Check if state exists
    with conn() as c:
        row = run_with_retry(lambda: c.execute(
            "SELECT * FROM client_state WHERE client_id=?",
            (client_id,)
        ).fetchone())
    
    if not row:
        # Create initial state
        with conn() as c:
            run_with_retry(lambda: c.execute("""
                INSERT INTO client_state (
                    client_id, current_equity, starting_equity_today,
                    realized_pnl_today, trades_taken_today, daily_stop_hit,
                    kill_switch, mode
                ) VALUES (?, ?, ?, 0.0, 0, 0, 0, 'PAPER')
            """, (client_id, equity, equity)))
        log.info("✅ Created new client state")
    else:
        # Reset state
        update_client_state(client_id, {
            "kill_switch": 0,
            "mode": "PAPER",
            "trades_taken_today": 0,
            "realized_pnl_today": 0.0,
            "daily_stop_hit": 0,
            "current_equity": equity,
            "starting_equity_today": equity
        })
        log.info("✅ Reset client state")


def verify_broker_connection(client_id: str) -> bool:
    """Test broker connection"""
    
    log.info(f"Testing broker connection for {client_id}")
    
    try:
        from ap.client_manager import get_client_broker
        
        broker = get_client_broker(client_id)
        equity = broker.get_account_equity()
        
        log.info(f"✅ Broker connected: ${equity:,.2f}")
        
        # Update equity in state
        update_client_state(client_id, {
            "current_equity": equity,
            "starting_equity_today": equity
        })
        
        return True
        
    except Exception as e:
        log.error(f"❌ Broker connection failed: {e}")
        return False


def verify_trading_ready(client_id: str) -> bool:
    """Verify client is ready to trade"""
    
    log.info("Running pre-flight checks...")
    
    checks = []
    
    # 1. Client status
    with conn() as c:
        client = run_with_retry(lambda: c.execute(
            "SELECT * FROM clients WHERE client_id=?",
            (client_id,)
        ).fetchone())
    
    if not client:
        log.error("❌ Client not found")
        return False
    
    client = dict(client)
    
    if client['status'] != 'ACTIVE':
        log.warning(f"⚠️  Client status: {client['status']} (should be ACTIVE)")
        checks.append(False)
    else:
        log.info("✅ Client is ACTIVE")
        checks.append(True)
    
    # 2. Client state
    with conn() as c:
        state = run_with_retry(lambda: c.execute(
            "SELECT * FROM client_state WHERE client_id=?",
            (client_id,)
        ).fetchone())
    
    if not state:
        log.error("❌ Client state not found")
        return False
    
    state = dict(state)
    
    if state.get('kill_switch'):
        log.warning("⚠️  Kill switch is ON")
        checks.append(False)
    else:
        log.info("✅ Kill switch is OFF")
        checks.append(True)
    
    mode = state.get('mode', '').upper()
    if mode not in ('PAPER', 'LIVE'):
        log.warning(f"⚠️  Invalid mode: {mode}")
        checks.append(False)
    else:
        log.info(f"✅ Mode: {mode}")
        checks.append(True)
    
    # 3. Broker credentials
    if not client.get('broker_token'):
        log.error("❌ No broker token")
        checks.append(False)
    else:
        log.info("✅ Broker token configured")
        checks.append(True)
    
    # 4. Trading limits
    log.info(f"Trading limits:")
    log.info(f"  - Max trades/day: {client.get('max_trades_per_day')}")
    log.info(f"  - Max positions: {client.get('max_concurrent_positions')}")
    log.info(f"  - Trades today: {state.get('trades_taken_today', 0)}")
    
    # 5. Environment
    required_env = ['ENCRYPTION_KEY']
    for var in required_env:
        if not os.getenv(var):
            log.warning(f"⚠️  {var} not set")
            checks.append(False)
        else:
            log.info(f"✅ {var} is set")
            checks.append(True)
    
    all_passed = all(checks)
    
    if all_passed:
        print("\n" + "="*60)
        print("🎉 ALL CHECKS PASSED - READY TO TRADE")
        print("="*60)
    else:
        print("\n" + "="*60)
        print("⚠️  SOME CHECKS FAILED - SEE ABOVE")
        print("="*60)
    
    return all_passed


def main():
    print("="*60)
    print("🚀 CLIENT INITIALIZATION")
    print("="*60)
    
    client_id = input("\nEnter client ID (default: 'default'): ").strip() or "default"
    
    # Step 1: Get or create client
    print("\n1️⃣ Setting up client...")
    client = get_or_create_client(client_id)
    
    # Step 2: Activate client
    print("\n2️⃣ Activating client...")
    update_client(client_id, status="ACTIVE")
    
    # Step 3: Initialize state
    print("\n3️⃣ Initializing state...")
    initialize_client_state(client_id)
    
    # Step 4: Test broker
    print("\n4️⃣ Testing broker connection...")
    broker_ok = verify_broker_connection(client_id)
    
    if not broker_ok:
        print("\n⚠️  Broker connection failed - check credentials")
        print("But you can still continue with setup")
    
    # Step 5: Verify ready
    print("\n5️⃣ Running pre-flight checks...")
    ready = verify_trading_ready(client_id)
    
    # Summary
    print("\n" + "="*60)
    print("📊 CONFIGURATION SUMMARY")
    print("="*60)
    print(f"Client ID: {client_id}")
    print(f"Name: {client['name']}")
    print(f"Status: {client['status']}")
    print(f"Broker: {client['broker_type']}")
    print(f"Account: {client['broker_account_id']}")
    
    if not broker_ok:
        print("\n⚠️  Next steps:")
        print("1. Check your TRADIER_ACCOUNT_ID and TRADIER_ACCESS_TOKEN")
        print("2. Make sure your Tradier account is funded")
        print("3. Re-run this script to verify connection")
    elif not ready:
        print("\n⚠️  Next steps:")
        print("1. Fix the issues shown above")
        print("2. Re-run this script to verify")
    else:
        print("\n✅ All set! Your bot is ready to receive signals.")
        print("\nTo test, send a signal to your webhook endpoint:")
        print("""
curl -X POST http://your-server/signals/webhook \\
  -H "Content-Type: application/json" \\
  -d '{
    "symbol": "SPY",
    "direction": "CALL",
    "trigger": {"strike": 580.0},
    "reason": "test"
  }'
        """)


if __name__ == "__main__":
    main()
