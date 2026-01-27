 cat > ap/subscription_tiers.py << 'EOF'
# ap/subscription_tiers.py - CORRECTED VERSION
# Properly initializes client_state when subscription is created
# Ready to deploy - no manual edits needed

from datetime import datetime, timedelta, timezone
from ap.db import conn, run_with_retry, get_client_state, update_client_state
from ap.logger import get_logger

log = get_logger("ap.subscription_tiers")


class CorrectRentalTiers:
    """Define the 3 rental tiers"""
    
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
    """
    Create a rental subscription for a client.
    
    This INITIALIZES client_state with all growth tracking fields.
    
    Args:
        client_id: Client identifier
        tier: One of: rental_5k, rental_10k, rental_25k
        start_date: ISO format start date (optional, defaults to now)
    
    Returns:
        Dict with subscription details
    """
    try:
        # Validate tier
        if tier not in CorrectRentalTiers.TIERS:
            return {
                "ok": False,
                "error": "invalid_tier",
                "available_tiers": list(CorrectRentalTiers.TIERS.keys())
            }
        
        tier_def = CorrectRentalTiers.TIERS[tier]
        
        # Parse dates
        if start_date:
            try:
                start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            except Exception as e:
                return {"ok": False, "error": f"invalid_start_date: {str(e)}"}
        else:
            start_dt = datetime.now(timezone.utc)
        
        end_dt = start_dt + timedelta(days=30)
        
        # CRITICAL: Initialize client_state with growth tracking
        # This is what was missing before
        try:
            from ap.account_growth import init_fee_rental_account
            
            init_result = init_fee_rental_account(
                client_id=client_id,
                rental_fee=tier_def["rental_fee"],
                client_capital=tier_def["client_capital"],
                working_capital=tier_def["working_capital"],
                profit_target_min=tier_def["profit_target_min"],
                profit_target_max=tier_def["profit_target_max"]
            )
            
            if not init_result.get("ok"):
                return {"ok": False, "error": "failed_to_init_growth_tracking"}
        except Exception as e:
            log.error(f"Failed to initialize growth tracking: {e}")
            return {"ok": False, "error": f"growth_init_error: {str(e)}"}
        
        # Also update client_state with subscription dates
        try:
            update_client_state(client_id, {
                "subscription_start_date": start_dt.isoformat(),
                "subscription_end_date": end_dt.isoformat(),
                "rental_tier": tier,
            })
        except Exception as e:
            log.error(f"Failed to update subscription dates: {e}")
            return {"ok": False, "error": f"subscription_date_error: {str(e)}"}
        
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
            "end_balance_if_min": tier_def["end_balance_min"],
            "end_balance_if_max": tier_def["end_balance_max"],
            "subscription_start": start_dt.isoformat(),
            "subscription_end": end_dt.isoformat(),
            "subscription_days": 30,
            "description": tier_def["description"],
        }
    
    except Exception as e:
        log.error(f"Create subscription failed: {e}")
        return {"ok": False, "error": str(e)}


def get_rental_status(client_id: str) -> dict:
    """
    Get current rental subscription status for a client.
    Shows progress toward profit targets.
    
    Returns:
        Dict with current balance, profit, targets, time remaining
    """
    try:
        from ap.account_growth import get_growth_metrics, check_growth_status
        
        state = get_client_state(client_id)
        if not state:
            return {"ok": False, "error": "client_not_found"}
        
        metrics = get_growth_metrics(client_id)
        status = check_growth_status(client_id)
        
        return {
            "ok": True,
            "client_id": client_id,
            "tier": state.get("rental_tier"),
            "current_balance": metrics.get("current_balance", 0),
            "starting_balance": metrics.get("starting_balance", 0),
            "profit": metrics.get("profit", 0),
            "profit_pct": metrics.get("profit_pct", 0),
            "profit_target_min": state.get("profit_target_min", 0),
            "profit_target_max": state.get("profit_target_max", 0),
            "should_stop": status.get("should_stop", False),
            "days_remaining": status.get("days_remaining", 0),
            "subscription_end": state.get("subscription_end_date"),
        }
    
    except Exception as e:
        log.error(f"Get rental status failed: {e}")
        return {"ok": False, "error": str(e)}


def calculate_settlement(client_id: str) -> dict:
    """
    Calculate final settlement when subscription ends or targets hit.
    
    Returns how much profit went to Angel (fee already paid)
    and how much client earned/lost.
    """
    try:
        from ap.account_growth import get_growth_metrics
        
        state = get_client_state(client_id)
        if not state:
            return {"ok": False, "error": "client_not_found"}
        
        metrics = get_growth_metrics(client_id)
        
        rental_fee = state.get("rental_fee", 0)
        working_capital = state.get("working_capital", 0)
        current_balance = metrics.get("current_balance", working_capital)
        profit = current_balance - working_capital
        
        return {
            "ok": True,
            "client_id": client_id,
            "rental_fee_paid": rental_fee,
            "working_capital": working_capital,
            "final_balance": current_balance,
            "total_profit": profit,
            "profit_pct": round(100.0 * profit / working_capital, 2) if working_capital > 0 else 0,
            "angel_keeps": rental_fee,  # Fee was upfront
            "client_receives": current_balance - rental_fee,  # Their original capital +/- profit
        }
    
    except Exception as e:
        log.error(f"Calculate settlement failed: {e}")
        return {"ok": False, "error": str(e)}


def project_subscription_revenue(num_subscriptions: int, tiers: list = None) -> dict:
    """
    Project monthly revenue based on subscription tiers.
    
    Args:
        num_subscriptions: Number of clients
        tiers: List of tier names (default: all 3)
    
    Returns:
        Monthly and annual revenue projections
    """
    if not tiers:
        tiers = ["rental_5k", "rental_10k", "rental_25k"]
    
    total_monthly = 0
    breakdown = {}
    
    for tier in tiers:
        if tier not in CorrectRentalTiers.TIERS:
            continue
        
        fee = CorrectRentalTiers.TIERS[tier]["rental_fee"]
        monthly = fee  # Each client pays fee once per month
        breakdown[tier] = {
            "fee_per_client": fee,
            "clients": num_subscriptions,
            "monthly_revenue": fee * num_subscriptions,
        }
        total_monthly += fee * num_subscriptions
    
    return {
        "ok": True,
        "total_clients": num_subscriptions,
        "monthly_revenue": total_monthly,
        "annual_revenue": total_monthly * 12,
        "by_tier": breakdown,
    }
EOF
