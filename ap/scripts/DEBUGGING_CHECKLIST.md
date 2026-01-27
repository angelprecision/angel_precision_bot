# 🔍 DEBUGGING CHECKLIST - Why Trades Aren't Executing

## Run This Diagnostic Script

```python
# diagnostic.py - Run this to debug why trades won't execute
import os
import sys
from ap.db import get_client, get_client_state, conn, run_with_retry
from ap.logger import get_logger

log = get_logger("diagnostic")

def run_diagnostics(client_id: str = "default"):
    """Check all conditions that could block trades"""
    
    print("=" * 60)
    print(f"🔍 DIAGNOSTICS FOR CLIENT: {client_id}")
    print("=" * 60)
    
    # 1. Check client exists and is active
    print("\n1️⃣ CLIENT STATUS:")
    try:
        client = get_client(client_id)
        print(f"   ✅ Client found: {client['name']}")
        print(f"   Status: {client['status']}")
        
        if client['status'] != 'ACTIVE':
            print(f"   ❌ CLIENT IS NOT ACTIVE - THIS BLOCKS TRADES")
            print(f"   Fix: Update client status to ACTIVE")
        else:
            print(f"   ✅ Client is ACTIVE")
    except Exception as e:
        print(f"   ❌ Client not found: {e}")
        print(f"   Fix: Create client with create_new_client()")
        return
    
    # 2. Check client state
    print("\n2️⃣ CLIENT STATE:")
    try:
        state = get_client_state(client_id)
        print(f"   Mode: {state.get('mode', 'NOT SET')}")
        print(f"   Kill Switch: {bool(state.get('kill_switch'))}")
        print(f"   Trades Today: {state.get('trades_taken_today', 0)}")
        print(f"   Current Equity: ${state.get('current_equity', 0):,.2f}")
        
        if state.get('kill_switch'):
            print(f"   ❌ KILL SWITCH IS ON - THIS BLOCKS TRADES")
            print(f"   Fix: Run update_client_state('{client_id}', {{'kill_switch': 0}})")
        
        if state.get('mode') == 'READ_ONLY':
            print(f"   ❌ MODE IS READ_ONLY - THIS BLOCKS TRADES")
            print(f"   Fix: Run update_client_state('{client_id}', {{'mode': 'PAPER'}})")
        
        if not state.get('mode'):
            print(f"   ⚠️  Mode not set - defaulting to PAPER")
            
    except Exception as e:
        print(f"   ❌ State error: {e}")
        print(f"   Fix: Initialize state with update_client_state()")
    
    # 3. Check broker configuration
    print("\n3️⃣ BROKER CONFIG:")
    print(f"   Broker Type: {client.get('broker_type')}")
    print(f"   Account ID: {client.get('broker_account_id')}")
    print(f"   Base URL: {client.get('broker_base_url')}")
    
    if not client.get('broker_token'):
        print(f"   ❌ NO BROKER TOKEN - THIS BLOCKS TRADES")
        print(f"   Fix: Set broker token via update_client()")
    else:
        print(f"   ✅ Broker token is set (encrypted)")
    
    # 4. Check trading limits
    print("\n4️⃣ TRADING LIMITS:")
    max_trades = client.get('max_trades_per_day', 25)
    trades_today = state.get('trades_taken_today', 0)
    max_positions = client.get('max_concurrent_positions', 2)
    
    print(f"   Max Trades/Day: {max_trades}")
    print(f"   Trades Today: {trades_today}")
    
    if trades_today >= max_trades:
        print(f"   ❌ DAILY TRADE CAP HIT - THIS BLOCKS TRADES")
        print(f"   Fix: Wait for next day or increase max_trades_per_day")
    else:
        print(f"   ✅ Under daily limit ({trades_today}/{max_trades})")
    
    # Check open positions
    with conn() as c:
        open_count = run_with_retry(lambda: c.execute(
            "SELECT COUNT(*) as n FROM positions WHERE client_id=? AND status IN ('OPEN','CLOSING')",
            (client_id,)
        ).fetchone())['n']
    
    print(f"   Max Open Positions: {max_positions}")
    print(f"   Current Open: {open_count}")
    
    if open_count >= max_positions:
        print(f"   ❌ MAX OPEN POSITIONS - THIS BLOCKS TRADES")
        print(f"   Fix: Close positions or increase max_concurrent_positions")
    else:
        print(f"   ✅ Can open more positions ({open_count}/{max_positions})")
    
    # 5. Check growth tracking
    print("\n5️⃣ GROWTH TRACKING:")
    try:
        from ap.account_growth import get_growth_metrics, check_growth_status
        
        metrics = get_growth_metrics(client_id)
        if not metrics.get('ok'):
            print(f"   ⚠️  Growth tracking not initialized (this is OK for testing)")
            print(f"   Error: {metrics.get('error')}")
        else:
            status = check_growth_status(client_id)
            print(f"   Enabled: Yes")
            print(f"   Profit: ${metrics['profit']:,.2f}")
            print(f"   Target Max: ${metrics['profit_target_max']:,.2f}")
            print(f"   Should Stop: {status.get('should_stop')}")
            
            if status.get('should_stop'):
                print(f"   ❌ GROWTH TARGET HIT - THIS BLOCKS TRADES")
                print(f"   Reason: {status.get('reason')}")
                print(f"   Fix: Reset growth tracking or increase targets")
            else:
                print(f"   ✅ Under growth limits")
                
    except Exception as e:
        print(f"   ⚠️  Growth check failed: {e}")
        print(f"   This might block LIVE trades for safety")
    
    # 6. Check environment variables
    print("\n6️⃣ ENVIRONMENT VARIABLES:")
    required_vars = [
        'ENCRYPTION_KEY',
        'ADMIN_API_KEY',
    ]
    
    for var in required_vars:
        val = os.getenv(var)
        if val:
            print(f"   ✅ {var}: Set ({len(val)} chars)")
        else:
            print(f"   ❌ {var}: NOT SET")
    
    # 7. Test signal processing
    print("\n7️⃣ SIGNAL PROCESSING TEST:")
    print("   Testing with dummy signal...")
    
    test_signal = {
        "symbol": "SPY",
        "direction": "CALL",
        "trigger": {"strike": 580.0},
        "reason": "test"
    }
    
    try:
        from ap.execution import process_signal
        from ap.client_manager import get_client_broker
        
        broker = get_client_broker(client_id)
        result = process_signal(broker, client_id, test_signal)
        
        if result.get('ok'):
            print(f"   ✅ TEST PASSED - Trade would execute")
            print(f"   Would trade: {result.get('contract')} x{result.get('qty')}")
        else:
            print(f"   ❌ TEST FAILED - {result.get('error')}")
            print(f"   Details: {result}")
            
    except Exception as e:
        print(f"   ❌ TEST ERROR: {e}")
    
    print("\n" + "=" * 60)
    print("DIAGNOSTICS COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    client_id = sys.argv[1] if len(sys.argv) > 1 else "default"
    run_diagnostics(client_id)
```

## Quick Fixes

### Fix 1: Enable Trading
```python
from ap.db import update_client_state, update_client

# Enable client
update_client("your_client_id", status="ACTIVE")

# Enable trading mode
update_client_state("your_client_id", {
    "mode": "PAPER",  # or "LIVE"
    "kill_switch": 0,
    "trades_taken_today": 0
})
```

### Fix 2: Check Database State
```python
from ap.db import conn

with conn() as c:
    # Check client
    client = c.execute("SELECT * FROM clients WHERE client_id=?", ("your_client_id",)).fetchone()
    print(dict(client))
    
    # Check state
    state = c.execute("SELECT * FROM client_state WHERE client_id=?", ("your_client_id",)).fetchone()
    print(dict(state))
```

### Fix 3: Test Broker Connection
```python
from ap.client_manager import get_client_broker

try:
    broker = get_client_broker("your_client_id")
    equity = broker.get_account_equity()
    print(f"✅ Broker connected: ${equity:,.2f}")
except Exception as e:
    print(f"❌ Broker error: {e}")
```

### Fix 4: Reset Growth Tracking
```python
from ap.account_growth import disable_growth_tracking

# Temporarily disable for testing
disable_growth_tracking("your_client_id")
```

### Fix 5: Check Logs
```python
from ap.db import conn

# Get recent audit logs
with conn() as c:
    logs = c.execute("""
        SELECT * FROM audit_log
        WHERE client_id=?
        ORDER BY ts DESC
        LIMIT 20
    """, ("your_client_id",)).fetchall()
    
    for log in logs:
        print(f"{log['ts']} - {log['event']}: {log['payload']}")
```

## Common Issues & Solutions

### Issue: "client_inactive"
**Cause:** Client status is not ACTIVE
**Fix:**
```python
update_client("client_id", status="ACTIVE")
```

### Issue: "kill_switch_active"
**Cause:** Kill switch was triggered (growth target hit, error, etc)
**Fix:**
```python
update_client_state("client_id", {"kill_switch": 0})
```

### Issue: "contract_resolution_failed"
**Cause:** Can't get option chain from broker
**Fix:**
1. Check broker credentials
2. Verify broker API is working
3. Check if market is open
4. Ensure symbol exists

### Issue: "broker_error"
**Cause:** Broker rejected order
**Fix:**
1. Check broker account has funds
2. Verify trading permissions
3. Check market hours
4. Review broker error message

### Issue: "growth_tracking_disabled"
**Cause:** In LIVE mode but growth not initialized
**Fix:**
```python
from ap.account_growth import init_fee_rental_account

init_fee_rental_account(
    client_id="your_client_id",
    client_capital=10000.0,
    rental_fee=10000.0,
    profit_target_max=25000.0
)
```

### Issue: No error but no trades
**Cause:** Signal not reaching execution
**Fix:**
1. Check scanner is running
2. Verify webhook endpoint
3. Check signal format matches expected schema
4. Review audit logs for incoming signals
