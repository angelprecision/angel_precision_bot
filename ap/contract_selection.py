# ap/contract_selection.py - STRICT CONTRACT SELECTION
from datetime import datetime
from zoneinfo import ZoneInfo

from ap.logger import get_logger

log = get_logger("ap.contract_selection")

ET = ZoneInfo("America/New_York")


def pick_expiration(expirations: list[str], hint: str | None) -> str:
    """
    Pick the best expiration date based on hint.
    
    CRITICAL SAFETY RULES:
    - 0DTE means TODAY ONLY (strict enforcement)
    - Rejects stale data (all dates in past)
    - Caps weeklies/monthlies at 30 days max
    
    Args:
        expirations: List of dates in "YYYY-MM-DD" format
        hint: "0DTE", "Weekly", "Monthly", or None
        
    Returns:
        Selected expiration date as "YYYY-MM-DD"
        
    Raises:
        ValueError: If no valid expiration found
    """
    if not expirations:
        raise ValueError("No expirations available")
    
    today = datetime.now(ET).date()
    
    # Parse and sort dates
    exp_dates = sorted(datetime.strptime(d, "%Y-%m-%d").date() for d in expirations)
    
    # SAFETY CHECK: Reject stale data
    if exp_dates[-1] < today:
        log.error(f"Stale option chain detected: latest={exp_dates[-1]}, today={today}")
        raise ValueError(
            f"All expirations are in the past (today={today}). "
            f"Latest={exp_dates[-1]}. Raw={expirations}"
        )
    
    # =========================================================================
    # 0DTE: TODAY ONLY (STRICT!)
    # =========================================================================
    if hint and hint.upper() == "0DTE":
        if today in exp_dates:
            log.info(f"✅ 0DTE: Using today's expiration: {today}")
            return today.strftime("%Y-%m-%d")
        
        # TODAY NOT AVAILABLE - REJECT TRADE!
        log.warning(f"❌ 0DTE rejected: today ({today}) not in {expirations}")
        raise ValueError(
            f"0DTE requested but today ({today}) not in expirations. "
            f"Available: {expirations}"
        )
    
    # =========================================================================
    # WEEKLY/MONTHLY: Next expiration within 30 days
    # =========================================================================
    candidates = [d for d in exp_dates if d >= today]
    
    if not candidates:
        raise ValueError(f"No future expirations available. Raw={expirations}")
    
    # Prefer expirations within 30 days
    for d in candidates:
        days_out = (d - today).days
        if days_out <= 30:
            log.info(f"✅ Selected expiration: {d} ({days_out} days out)")
            return d.strftime("%Y-%m-%d")
    
    # All expirations >30 days out - use nearest future
    nearest_future = candidates[0]
    days_out = (nearest_future - today).days
    
    log.warning(
        f"⚠️  All expirations >{30}d out, using nearest: {nearest_future} "
        f"({days_out} days from {today})"
    )
    
    return nearest_future.strftime("%Y-%m-%d")


def resolve_contract_symbol(chain: list[dict], strike: float, direction: str) -> str:
    """
    Find the option contract symbol closest to the requested strike.
    
    Args:
        chain: List of option contracts from broker
        strike: Desired strike price
        direction: "CALL" or "PUT"
        
    Returns:
        Option symbol (e.g., "SPY260203C00580000")
        
    Raises:
        ValueError: If no matching contract found
    """
    want_type = "call" if direction.upper() == "CALL" else "put"
    
    # Filter by option type
    filtered = [
        o for o in chain 
        if str(o.get("option_type", "")).lower() == want_type
    ]
    
    if not filtered:
        raise ValueError(f"No {direction} options in chain")
    
    # Find closest strike
    def strike_distance(option):
        return abs(float(option.get("strike", 0)) - float(strike))
    
    best = min(filtered, key=strike_distance)
    
    symbol = best.get("symbol")
    if not symbol:
        raise ValueError("No symbol in selected option contract")
    
    actual_strike = float(best.get("strike", 0))
    log.info(f"✅ Matched strike ${strike:.2f} → ${actual_strike:.2f} ({symbol})")
    
    return symbol
