# ap/account_growth.py (CORRECTED - Fee + Capital Model)
"""
Account growth tracker for fee-based bot rental.

CORRECT MODEL:
- Client pays Angel: $10K (rental fee upfront)
- Client puts in: $10K (their own capital)
- Total working capital: $20K
- Minimum profit needed: $10K (to recover rental fee)
- Ideal profit target: $17-25K additional (fee + bonus)

Usage:
    from ap.account_growth import init_fee_rental_account
    
    init_fee_rental_account(
        client_id="user123",
        client_capital=10000.0,      # What they invest
        rental_fee=10000.0,          # What they pay Angel
        profit_target_min=10000.0,   # Cover fee minimum
        profit_target_max=25000.0,   # Fee + profit ideal
        subscription_end_date="2026-02-26"
    )
"""

from datetime import datetime, timezone
from ap.logger import get_logger
from ap.db import conn, run_with_retry, get_client_state, update_client_state

log = get_logger("ap.account_growth")


def get_growth_metrics(client_id: str = "default") -> dict:
    """
    Get current growth metrics for a fee-rental account.
    
    Working capital = client_capital + rental_fee (both in account)
    Profit = current_balance - working_capital
    
    Returns:
        {
            "client_capital": 10000.0,
            "rental_fee": 10000.0,
            "working_capital": 20000.0,
            "current_balance": 25500.0,
            "profit": 5500.0,
            "profit_pct": 27.5,
            "profit_target_min": 10000.0,
            "profit_target_max": 25000.0,
            "min_hit": True,
            "max_hit": False,
            ...
        }
    """
    with conn() as c:
        state = run_with_retry(lambda: c.execute(
            "SELECT * FROM client_state WHERE client_id=?",
            (client_id,)
        ).fetchone())
    
    if not state:
        return {"error": "Client not found", "client_id": client_id}
    
    state = dict(state)
    
    client_capital = float(state.get("client_capital", 0.0)) or 10000.0
    rental_fee = float(state.get("rental_fee", 0.0)) or 10000.0
    working_capital = client_capital + rental_fee  # Total in account
    
    current_balance = float(state.get("current_equity", 0.0)) or working_capital
    
    profit_target_min = float(state.get("profit_target_min", 0.0)) or rental_fee  # Default: cover fee
    profit_target_max = float(state.get("profit_target_max", 0.0)) or (rental_fee + 15000.0)  # Default: fee + $15K
    
    profit = current_balance - working_capital
    profit_pct = ((profit / working_capital) * 100) if working_capital > 0 else 0
    
    return {
        "client_id": client_id,
        "client_capital": client_capital,
        "rental_fee": rental_fee,
        "working_capital": working_capital,
        "current_balance": current_balance,
        "profit": profit,
        "profit_pct": profit_pct,
        "profit_target_min": profit_target_min,
        "profit_target_max": profit_target_max,
        "min_hit": profit >= profit_target_min,
        "max_hit": profit >= profit_target_max,
        "remaining_to_min": max(0, profit_target_min - profit),
        "remaining_to_max": max(0, profit_target_max - profit),
        "end_balance_if_min": working_capital + profit_target_min,
        "end_balance_if_max": working_capital + profit_target_max,
        "progress_to_max_pct": min((profit / profit_target_max * 100) if profit_target_max > 0 else 0, 100)
    }


def check_growth_status(
    client_id: str = "default",
    subscription_end_date: str = None,
    stop_at_min: bool = False,
) -> dict:
    """
    Check if rental account should stop trading.
    
    Stops when:
    1. Minimum profit target hit (covers rental fee), OR
    2. Maximum profit target hit (fee + bonus), OR
    3. Subscription period ends
    
    Args:
        client_id: Client identifier
        subscription_end_date: When rental ends (ISO format)
        stop_at_min: If True, stop at minimum. If False, stop at maximum.
    
    Returns:
        {"should_stop": bool, "reason": str, "message": str, "metrics": dict}
    """
    metrics = get_growth_metrics(client_id)
    
    if "error" in metrics:
        return {
            "should_stop": False,
            "reason": "account_not_found",
            "error": metrics.get("error")
        }
    
    # Check minimum profit (fee coverage)
    if stop_at_min and metrics["min_hit"]:
        return {
            "should_stop": True,
            "reason": "min_profit_hit",
            "message": f"Minimum profit hit (rental fee covered): ${metrics['profit']:,.2f} / ${metrics['profit_target_min']:,.2f}",
            "metrics": metrics
        }
    
    # Check maximum profit
    if metrics["max_hit"]:
        return {
            "should_stop": True,
            "reason": "max_profit_hit",
            "message": f"Maximum profit target hit: ${metrics['profit']:,.2f} / ${metrics['profit_target_max']:,.2f}",
            "metrics": metrics
        }
    
    # Check subscription end
    if subscription_end_date:
        try:
            if subscription_end_date.endswith('Z') or 'T' in subscription_end_date:
                end_dt = datetime.fromisoformat(subscription_end_date.replace('Z', '+00:00'))
            else:
                end_dt = datetime.fromisoformat(subscription_end_date + "T23:59:59Z").replace(tzinfo=timezone.utc)
            
            now = datetime.now(timezone.utc)
            
            if now >= end_dt:
                return {
                    "should_stop": True,
                    "reason": "subscription_ended",
                    "message": f"Subscription period ended on {subscription_end_date}",
                    "metrics": metrics
                }
        except Exception as e:
            log.warning(f"Failed to parse subscription end date: {e}")
    
    # Still growing
    remaining = metrics["remaining_to_max"]
    return {
        "should_stop": False,
        "reason": "still_growing",
        "message": f"Growing: ${metrics['current_balance']:,.2f} → Target: ${metrics['end_balance_if_max']:,.2f} (${remaining:,.2f} remaining)",
        "metrics": metrics
    }


def init_fee_rental_account(
    client_id: str = "default",
    client_capital: float = 10000.0,
    rental_fee: float = 10000.0,
    profit_target_min: float = None,
    profit_target_max: float = None,
    subscription_end_date: str = None,
) -> bool:
    """
    Initialize a fee-based rental account.
    
    Args:
        client_id: Client identifier
        client_capital: Client's own investment ($10K, $25K, etc.)
        rental_fee: What client paid Angel upfront ($10K, $25K, etc.)
        profit_target_min: Minimum profit (default: equal to rental_fee)
        profit_target_max: Ideal profit (default: rental_fee + $15K)
        subscription_end_date: When rental ends
    
    Returns:
        True if successful
    
    Example:
        init_fee_rental_account(
            client_id="john@email.com",
            client_capital=10000.0,
            rental_fee=10000.0,
            profit_target_min=10000.0,  # Need $10K profit
            profit_target_max=25000.0,  # Want $25K profit
            subscription_end_date="2026-02-26"
        )
    """
    try:
        # Default targets
        if profit_target_min is None:
            profit_target_min = rental_fee  # Minimum = cover the fee
        if profit_target_max is None:
            profit_target_max = rental_fee + 15000.0  # Ideal = fee + $15K bonus
        
        working_capital = client_capital + rental_fee
        
        state = {
            "client_capital": client_capital,
            "rental_fee": rental_fee,
            "working_capital": working_capital,
            "profit_target_min": profit_target_min,
            "profit_target_max": profit_target_max,
            "subscription_end_date": subscription_end_date,
            "current_equity": working_capital,
            "growth_tracking_enabled": 1,
            "account_type": "fee_rental"
        }
        
        update_client_state(client_id, state)
        log.info(
            f"Fee-rental account initialized: {client_id} "
            f"capital=${client_capital:,.2f} fee=${rental_fee:,.2f} "
            f"total=${working_capital:,.2f} "
            f"profit_target=${profit_target_max:,.2f}"
        )
        return True
    except Exception as e:
        log.error(f"Failed to initialize fee-rental account: {e}")
        return False


def get_progress_bar(client_id: str = "default", width: int = 20) -> str:
    """Get ASCII progress bar for Discord."""
    metrics = get_growth_metrics(client_id)
    
    if "error" in metrics:
        return "Account not initialized"
    
    progress = metrics.get("progress_to_max_pct", 0) / 100.0
    filled = int(progress * width)
    bar = "█" * filled + "░" * (width - filled)
    
    return f"[{bar}] ${metrics['profit']:,.2f} / ${metrics['profit_target_max']:,.2f} ({metrics['progress_to_max_pct']:.1f}%)"


def format_growth_summary(client_id: str = "default") -> str:
    """Format metrics for Discord display."""
    metrics = get_growth_metrics(client_id)
    
    if "error" in metrics:
        return "Account not initialized"
    
    status = check_growth_status(client_id)
    
    summary = f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
💰 FEE-RENTAL ACCOUNT PROGRESS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Your Capital:       ${metrics['client_capital']:>12,.2f}
Rental Fee (paid):  ${metrics['rental_fee']:>12,.2f}
Working Capital:    ${metrics['working_capital']:>12,.2f}
Current Balance:    ${metrics['current_balance']:>12,.2f}

📈 Profit:          ${metrics['profit']:>12,.2f} ({metrics['profit_pct']:>5.1f}%)

🎯 Profit Targets:
   Min (cover fee): ${metrics['profit_target_min']:>12,.2f} {'✓' if metrics['min_hit'] else ''}
   Max (+ bonus):   ${metrics['profit_target_max']:>12,.2f} {'✓' if metrics['max_hit'] else ''}

🔄 End Balances:
   If Min Hit:      ${metrics['end_balance_if_min']:>12,.2f}
   If Max Hit:      ${metrics['end_balance_if_max']:>12,.2f}

📊 Progress:        {get_progress_bar(client_id)}

Status:             {status['message']}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
    return summary.strip()


def get_rental_summary(client_id: str = "default") -> dict:
    """Get complete rental summary for reporting/billing."""
    metrics = get_growth_metrics(client_id)
    status = check_growth_status(client_id)
    
    if "error" in metrics:
        return {"error": metrics.get("error")}
    
    return {
        "client_id": client_id,
        "client_capital": metrics["client_capital"],
        "rental_fee": metrics["rental_fee"],
        "working_capital": metrics["working_capital"],
        "current_balance": metrics["current_balance"],
        "profit": metrics["profit"],
        "profit_pct": metrics["profit_pct"],
        "min_profit_target": metrics["profit_target_min"],
        "max_profit_target": metrics["profit_target_max"],
        "min_profit_hit": metrics["min_hit"],
        "max_profit_hit": metrics["max_hit"],
        "end_balance_if_min": metrics["end_balance_if_min"],
        "end_balance_if_max": metrics["end_balance_if_max"],
        "progress_pct": metrics["progress_to_max_pct"],
        "should_stop": status.get("should_stop"),
        "stop_reason": status.get("reason"),
    }
