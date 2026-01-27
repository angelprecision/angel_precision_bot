cat > ap/subscription_tiers.py << 'EOF'
# ap/subscription_tiers.py - SIMPLE FIX VERSION
from datetime import datetime, timedelta, timezone
from ap.db import get_client_state, update_client_state
from ap.logger import get_logger

log = get_logger("ap.subscription_tiers")

class CorrectRentalTiers:
    TIERS = {
        "rental_5k": {
            "name": "Angel Precision - $5K Tier",
            "description": "Pay $5K, invest $5K, need $5-20K profit",
            "rental_fee": 5000.0,
            "client_capital": 5000.0,
            "working_capital": 10000.0,
            "profit_target_min": 5000.0,
            "profit_target_max": 20000.0,
            "end_balance_min": 15000.0,
            "end_balance_max": 30000.0,
        },
        "rental_10k": {
            "name": "Angel Precision - $10K Tier",
            "description": "Pay $10K, invest $10K, need $10-25K profit",
            "rental_fee": 10000.0,
            "client_capital": 10000.0,
            "working_capital": 20000.0,
            "profit_target_min": 10000.0,
            "profit_target_max": 25000.0,
            "end_balance_min": 30000.0,
            "end_balance_max": 45000.0,
        },
        "rental_25k": {
            "name": "Angel Precision - $25K Tier",
            "description": "Pay $25K, invest $25K, need $25-40K profit",
            "rental_fee": 25000.0,
            "client_capital": 25000.0,
            "working_capital": 50000.0,
            "profit_target_min": 25000.0,
            "profit_target_max": 40000.0,
            "end_balance_min": 75000.0,
            "end_balance_max": 90000.0,
        },
    }

def create_rental_subscription(client_id: str, tier: str, start_date: str = None) -> dict:
    try:
        if tier not in CorrectRentalTiers.TIERS:
            return {"ok": False, "error": "invalid_tier"}
        
        tier_def = CorrectRentalTiers.TIERS[tier]
        
        if start_date:
            start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        else:
            start_dt = datetime.now(timezone.utc)
        
        end_dt = start_dt + timedelta(days=30)
        
        # Initialize client_state directly with all growth fields
        update_client_state(client_id, {
            "rental_fee": tier_def["rental_fee"],
            "client_capital": tier_def["client_capital"],
            "working_capital": tier_def["working_capital"],
            "current_equity": tier_def["working_capital"],
            "profit_target_min": tier_def["profit_target_min"],
            "profit_target_max": tier_def["profit_target_max"],
            "subscription_start_date": start_dt.isoformat(),
            "subscription_end_date": end_dt.isoformat(),
            "rental_tier": tier,
        })
        
        log.info(f"✅ Subscription created: {client_id} {tier}")
        
        return {
            "ok": True,
            "client_id": client_id,
            "tier": tier,
            "tier_name": tier_def["name"],
            "rental_fee": tier_def["rental_fee"],
            "client_capital": tier_def["client_capital"],
            "working_capital": tier_def["working_capital"],
            "profit_target_min": tier_def["profit_target_min"],
            "profit_target_max": tier_def["profit_target_max"],
            "subscription_start": start_dt.isoformat(),
            "subscription_end": end_dt.isoformat(),
            "subscription_days": 30,
        }
    except Exception as e:
        log.error(f"Create subscription failed: {e}")
        return {"ok": False, "error": str(e)}

def get_rental_status(client_id: str) -> dict:
    try:
        state = get_client_state(client_id)
        if not state:
            return {"ok": False, "error": "client_not_found"}
        
        current = state.get("current_equity", state.get("working_capital", 0))
        working = state.get("working_capital", 0)
        profit = current - working if working > 0 else 0
        
        return {
            "ok": True,
            "client_id": client_id,
            "current_balance": current,
            "profit": profit,
            "profit_target_min": state.get("profit_target_min", 0),
            "profit_target_max": state.get("profit_target_max", 0),
        }
    except Exception as e:
        log.error(f"Get rental status failed: {e}")
        return {"ok": False, "error": str(e)}
EOF
