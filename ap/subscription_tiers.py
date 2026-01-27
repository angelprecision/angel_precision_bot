# CORRECTED SUBSCRIPTION TIERS
# Client pays rental fee + puts in their own capital
# =====================================================================

"""
ANGEL PRECISION RENTAL MODEL (CORRECT)

Each tier follows same rule:
- Client pays Angel: $X (rental fee)
- Client invests: $X (their capital)
- Total working capital: $2X
- Minimum profit needed: $X (to cover fee)
- Ideal profit: $X + $7-15K (fee + bonus)

Example: $10K Tier
- Client pays you: $10K
- Client puts in: $10K
- Total account: $20K
- Min profit: $10K (covers fee, breaks even)
- Max profit: $10K + $7-15K = $17-25K
- Min end: $30K
- Max end: $37-45K
"""


class CorrectRentalTiers:
    """Fee + Capital rental subscription tiers"""
    
    TIERS = {
        "rental_5k": {
            "name": "Angel Precision - $5K Tier",
            "client_capital": 5000.0,
            "rental_fee": 5000.0,
            "working_capital": 10000.0,
            "profit_target_min": 5000.0,      # Must make $5K to cover fee
            "profit_target_max": 20000.0,     # Ideal: $5K fee + $15K profit = $20K total profit
            "end_balance_min": 15000.0,       # $10K working + $5K profit
            "end_balance_max": 30000.0,       # $10K working + $20K profit
            "subscription_days": 30,
            "description": "Pay $5K, invest $5K, need $5-20K profit"
        },
        "rental_10k": {
            "name": "Angel Precision - $10K Tier",
            "client_capital": 10000.0,
            "rental_fee": 10000.0,
            "working_capital": 20000.0,
            "profit_target_min": 10000.0,     # Must make $10K to cover fee
            "profit_target_max": 25000.0,     # Ideal: $10K fee + $15K profit = $25K total profit
            "end_balance_min": 30000.0,       # $20K working + $10K profit
            "end_balance_max": 45000.0,       # $20K working + $25K profit
            "subscription_days": 30,
            "description": "Pay $10K, invest $10K, need $10-25K profit"
        },
        "rental_25k": {
            "name": "Angel Precision - $25K Tier",
            "client_capital": 25000.0,
            "rental_fee": 25000.0,
            "working_capital": 50000.0,
            "profit_target_min": 25000.0,     # Must make $25K to cover fee
            "profit_target_max": 40000.0,     # Ideal: $25K fee + $15K profit = $40K total profit
            "end_balance_min": 75000.0,       # $50K working + $25K profit
            "end_balance_max": 90000.0,       # $50K working + $40K profit
            "subscription_days": 30,
            "description": "Pay $25K, invest $25K, need $25-40K profit"
        },
    }


def create_rental_subscription(
    client_id: str,
    tier: str,  # "rental_5k", "rental_10k", "rental_25k"
    start_date: str = None,
):
    """
    Create a new fee-rental subscription.
    
    Args:
        client_id: Unique client ID
        tier: Which tier
        start_date: When subscription starts (ISO format)
    
    Example:
        create_rental_subscription(
            client_id="john@email.com",
            tier="rental_10k",
            start_date="2026-01-26T09:30:00Z"
        )
    """
    from datetime import datetime, timedelta, timezone
    from ap.account_growth import init_fee_rental_account
    
    if tier not in CorrectRentalTiers.TIERS:
        return {"ok": False, "error": f"Unknown tier: {tier}"}
    
    tier_def = CorrectRentalTiers.TIERS[tier]
    
    # Calculate dates
    if start_date:
        start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
    else:
        start_dt = datetime.now(timezone.utc)
    
    end_dt = start_dt + timedelta(days=tier_def["subscription_days"])
    
    # Initialize rental account
    try:
        init_fee_rental_account(
            client_id=client_id,
            client_capital=tier_def["client_capital"],
            rental_fee=tier_def["rental_fee"],
            profit_target_min=tier_def["profit_target_min"],
            profit_target_max=tier_def["profit_target_max"],
            subscription_end_date=end_dt.isoformat()
        )
        
        return {
            "ok": True,
            "client_id": client_id,
            "tier": tier,
            "tier_name": tier_def["name"],
            "subscription_start": start_dt.isoformat(),
            "subscription_end": end_dt.isoformat(),
            "subscription_days": tier_def["subscription_days"],
            "client_capital": tier_def["client_capital"],
            "rental_fee": tier_def["rental_fee"],
            "working_capital": tier_def["working_capital"],
            "profit_target_min": tier_def["profit_target_min"],
            "profit_target_max": tier_def["profit_target_max"],
            "end_balance_if_min": tier_def["end_balance_min"],
            "end_balance_if_max": tier_def["end_balance_max"],
            "description": tier_def["description"],
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def get_rental_status(client_id: str) -> dict:
    """Get complete rental subscription status."""
    from ap.account_growth import get_rental_summary, check_growth_status
    
    try:
        summary = get_rental_summary(client_id)
        
        if "error" in summary:
            return {"ok": False, "error": summary.get("error")}
        
        status = check_growth_status(client_id)
        
        return {
            "ok": True,
            **summary,
            "status": status.get("reason"),
            "message": status.get("message"),
            "subscription_active": not status.get("should_stop"),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


# =====================================================================
# TIER COMPARISON CHART
# =====================================================================

def print_tier_comparison():
    """Print all tiers side by side."""
    print("\n" + "="*100)
    print("ANGEL PRECISION - FEE-RENTAL SUBSCRIPTION TIERS")
    print("="*100)
    
    for tier_key, tier_def in CorrectRentalTiers.TIERS.items():
        print(f"\n{'='*100}")
        print(f"TIER: {tier_def['name']}")
        print(f"{'='*100}")
        print(f"Description:        {tier_def['description']}")
        print(f"\nClient Investment:")
        print(f"  Pay to Angel:     ${tier_def['rental_fee']:>12,.2f}")
        print(f"  Own Capital:      ${tier_def['client_capital']:>12,.2f}")
        print(f"  Total Working:    ${tier_def['working_capital']:>12,.2f}")
        print(f"\nProfit Targets:")
        print(f"  Minimum (cover fee):  ${tier_def['profit_target_min']:>12,.2f}")
        print(f"  Ideal (fee + $15K):   ${tier_def['profit_target_max']:>12,.2f}")
        print(f"\nEnd Balance:")
        print(f"  If Min Hit:       ${tier_def['end_balance_min']:>12,.2f}")
        print(f"  If Max Hit:       ${tier_def['end_balance_max']:>12,.2f}")
        print(f"\nSubscription:")
        print(f"  Duration:         {tier_def['subscription_days']} days")
        print(f"  Position Sizing:  15% of current balance (auto-compounds)")


# =====================================================================
# POSITION SIZING EXAMPLES
# =====================================================================

def show_position_sizing_example(tier: str):
    """Show how position sizes compound for a tier."""
    if tier not in CorrectRentalTiers.TIERS:
        print(f"Unknown tier: {tier}")
        return
    
    tier_def = CorrectRentalTiers.TIERS[tier]
    
    print(f"\n{'='*80}")
    print(f"POSITION SIZING EXAMPLE - {tier_def['name']}")
    print(f"{'='*80}")
    print(f"Starting Capital: ${tier_def['working_capital']:,.2f}")
    print(f"Target Profit: ${tier_def['profit_target_min']:,.2f} - ${tier_def['profit_target_max']:,.2f}")
    print(f"{'='*80}\n")
    
    print(f"{'Trade':<6} {'Balance':<15} {'15% Position':<15} {'Profit (23%)':<15} {'New Balance':<15}")
    print("-" * 80)
    
    balance = tier_def["working_capital"]
    trade_num = 0
    
    while balance < tier_def["end_balance_max"] and trade_num < 20:
        trade_num += 1
        position = balance * 0.15
        profit_if_win = position * 0.23
        new_balance = balance + profit_if_win
        
        print(f"{trade_num:<6} ${balance:>13,.2f} ${position:>13,.2f} ${profit_if_win:>13,.2f} ${new_balance:>13,.2f}")
        
        balance = new_balance
    
    print(f"\n✓ Reached target end balance of ${tier_def['end_balance_max']:,.2f} in {trade_num} trades")
    print(f"  (Assuming consistent 23% wins, 15% position sizing)")


# =====================================================================
# API ENDPOINTS
# =====================================================================

"""
Add these to app.py:

@app.post("/rental/subscribe")
@require_hmac
def create_rental():
    body = request.get_json(force=True) or {}
    
    result = create_rental_subscription(
        client_id=body.get("client_id"),
        tier=body.get("tier"),  # "rental_5k", "rental_10k", "rental_25k"
        start_date=body.get("start_date")
    )
    
    if result.get("ok"):
        log.info(f"Rental subscription created: {result}")
        return jsonify(result), 201
    else:
        return jsonify(result), 400


@app.get("/rental/<client_id>/status")
@require_hmac
def rental_status(client_id: str):
    result = get_rental_status(client_id)
    return jsonify(result), 200 if result.get("ok") else 404


@app.get("/rental/tiers")
def list_tiers():
    tiers = {}
    for tier_key, tier_def in CorrectRentalTiers.TIERS.items():
        tiers[tier_key] = {
            "name": tier_def["name"],
            "client_capital": tier_def["client_capital"],
            "rental_fee": tier_def["rental_fee"],
            "working_capital": tier_def["working_capital"],
            "profit_target_min": tier_def["profit_target_min"],
            "profit_target_max": tier_def["profit_target_max"],
            "description": tier_def["description"],
        }
    return jsonify({"ok": True, "tiers": tiers})
"""


# =====================================================================
# BILLING & REVENUE CALCULATION
# =====================================================================

def calculate_settlement(
    client_id: str,
    final_balance: float
) -> dict:
    """
    Calculate settlement when subscription ends.
    
    Returns what client gets and what Angel keeps.
    """
    from ap.account_growth import get_growth_metrics
    
    metrics = get_growth_metrics(client_id)
    
    if "error" in metrics:
        return {"error": metrics.get("error")}
    
    # Client gets their profit
    working_capital = metrics["working_capital"]
    profit = final_balance - working_capital
    client_profit = final_balance - metrics["client_capital"]  # Original capital + profits minus rental fee already paid
    
    return {
        "client_id": client_id,
        "starting_capital": metrics["client_capital"],
        "rental_fee_paid": metrics["rental_fee"],
        "final_balance": final_balance,
        "working_capital_start": working_capital,
        "total_profit": profit,
        "client_net": client_profit,  # What they walk away with after you get rental fee
        "angel_keeps": metrics["rental_fee"],  # Rental fee is yours
    }


if __name__ == "__main__":
    # Show all tiers
    print_tier_comparison()
    
    # Show position sizing examples
    show_position_sizing_example("rental_5k")
    show_position_sizing_example("rental_10k")
    show_position_sizing_example("rental_25k")
