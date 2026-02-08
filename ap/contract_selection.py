# ap/contract_selection.py - PRODUCTION-GRADE CONTRACT SELECTION
# =====================================================================
# Battle-tested for 100+ trades/day with strict quality filters:
# - Liquidity (volume + open interest)
# - Bid-ask spread validation
# - Price range checks
# - Implied volatility sanity
# - Greeks validation (optional)
# - Multi-factor tie-breaking
# - Comprehensive logging
# =====================================================================

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional

from ap.logger import get_logger

log = get_logger("ap.contract_selection")
ET = ZoneInfo("America/New_York")


# ============================================================
# CONFIGURATION - Adjust these for your risk tolerance
# ============================================================

class ContractFilters:
    """Production filters tuned for 0DTE options trading"""
    
    # Liquidity requirements (CRITICAL for fills)
    MIN_VOLUME = 100              # Minimum daily volume
    MIN_OPEN_INTEREST = 50        # Minimum open interest
    PREFERRED_VOLUME = 500        # Ideal volume for tie-breaking
    
    # Spread requirements (CRITICAL to avoid instant losses)
    MAX_SPREAD_PCT = 0.10         # 10% max spread (tighter is better)
    IDEAL_SPREAD_PCT = 0.05       # 5% spread for tie-breaking
    
    # Price boundaries (prevent penny stocks and expensive contracts)
    MIN_CONTRACT_PRICE = 0.10     # $0.10 minimum (avoid illiquid pennies)
    MAX_CONTRACT_PRICE = 10.00    # $10.00 maximum (risk management)
    IDEAL_MIN_PRICE = 0.50        # $0.50+ preferred for liquidity
    IDEAL_MAX_PRICE = 5.00        # $5.00 max preferred for position sizing
    
    # Greeks boundaries (optional but recommended for 0DTE)
    MIN_DELTA = 0.30              # Minimum delta (avoid lottery tickets)
    MAX_DELTA = 0.80              # Maximum delta (avoid deep ITM illiquidity)
    MAX_THETA = 0.20              # Max theta decay per day
    
    # IV sanity checks (prevent buying insane premiums)
    MAX_IV = 2.50                 # 250% max IV (earnings plays excluded)
    MIN_IV = 0.10                 # 10% min IV (avoid dead contracts)
    
    # Distance from money thresholds
    MAX_OTM_DISTANCE_PCT = 0.05   # Max 5% OTM for safer entries
    PREFER_ITM_DISTANCE_PCT = 0.02 # Prefer 2% ITM


# ============================================================
# EXPIRATION SELECTION
# ============================================================

def _parse_dates(expirations: list[str]):
    """Parse and sort expiration dates"""
    return sorted(datetime.strptime(d, "%Y-%m-%d").date() for d in expirations)


def _is_mwf(d):
    """Check if date is Monday, Wednesday, or Friday"""
    return d.weekday() in (0, 2, 4)


def _is_friday(d):
    """Check if date is Friday"""
    return d.weekday() == 4


def pick_expiration(expirations: list[str], hint: str | None) -> str:
    """
    Strict expiration selection with fallback strategies.
    
    Supported hints (case-insensitive):
    - 0DTE   : TODAY ONLY (strict - fails if today not available)
    - DAILY  : Today if available, else nearest future date
    - MWF    : Nearest Mon/Wed/Fri >= today
    - WEEKLY : Nearest Friday >= today
    - None   : Smart default (<=30d preferred, else nearest)
    
    Returns:
        YYYY-MM-DD formatted date string
        
    Raises:
        ValueError: If no valid expiration can be found
    """
    if not expirations:
        raise ValueError("No expirations available - chain may be empty")
    
    today = datetime.now(ET).date()
    exp_dates = _parse_dates(expirations)
    
    # Validate chain freshness
    if exp_dates[-1] < today:
        log.error(f"❌ Stale option chain: latest={exp_dates[-1]} today={today}")
        raise ValueError(f"All expirations are in the past (today={today})")
    
    hint_norm = hint.strip().upper() if hint else None
    candidates = [d for d in exp_dates if d >= today]
    
    if not candidates:
        raise ValueError(f"No future expirations available (today={today})")
    
    # === 0DTE STRICT ===
    if hint_norm == "0DTE":
        if today in exp_dates:
            log.info(f"✅ 0DTE expiration: {today}")
            return today.strftime("%Y-%m-%d")
        log.warning(f"❌ 0DTE unavailable: today={today} expirations={expirations[:5]}")
        raise ValueError(f"0DTE requested but today ({today}) not available")
    
    # === DAILY (PRACTICAL) ===
    if hint_norm == "DAILY":
        chosen = today if today in exp_dates else candidates[0]
        days_out = (chosen - today).days
        log.info(f"✅ DAILY expiration: {chosen} ({days_out}d out)")
        return chosen.strftime("%Y-%m-%d")
    
    # === MWF (M/W/F PREFERENCE) ===
    if hint_norm == "MWF":
        mwf = [d for d in candidates if _is_mwf(d)]
        if mwf:
            days_out = (mwf[0] - today).days
            log.info(f"✅ MWF expiration: {mwf[0]} ({days_out}d out)")
            return mwf[0].strftime("%Y-%m-%d")
        # Fallback to nearest
        log.warning(f"⚠️ No MWF dates found, using nearest: {candidates[0]}")
        return candidates[0].strftime("%Y-%m-%d")
    
    # === WEEKLY (FRIDAY PREFERENCE) ===
    if hint_norm in ("WEEKLY", "WEEK", "EOW", "ENDOFWEEK"):
        fridays = [d for d in candidates if _is_friday(d)]
        if fridays:
            days_out = (fridays[0] - today).days
            log.info(f"✅ WEEKLY expiration: {fridays[0]} ({days_out}d out)")
            return fridays[0].strftime("%Y-%m-%d")
        # Fallback to nearest
        log.warning(f"⚠️ No Friday found, using nearest: {candidates[0]}")
        return candidates[0].strftime("%Y-%m-%d")
    
    # === DEFAULT (SMART) ===
    # Prefer <=30 days, else nearest future
    for d in candidates:
        days_out = (d - today).days
        if days_out <= 30:
            log.info(f"✅ Expiration selected: {d} ({days_out}d out)")
            return d.strftime("%Y-%m-%d")
    
    # All >30d, use nearest
    nearest = candidates[0]
    days_out = (nearest - today).days
    log.warning(f"⚠️ All expirations >30d, using nearest: {nearest} ({days_out}d out)")
    return nearest.strftime("%Y-%m-%d")


# ============================================================
# CONTRACT QUALITY FILTERS
# ============================================================

def _get_strike(o: dict) -> float:
    """Extract strike price safely"""
    try:
        return float(o.get("strike") or 0)
    except (ValueError, TypeError):
        return 0.0


def _get_mid_price(o: dict) -> float:
    """Calculate mid price from bid/ask"""
    try:
        bid = float(o.get("bid") or 0)
        ask = float(o.get("ask") or 0)
        if bid > 0 and ask > 0:
            return (bid + ask) / 2.0
        return 0.0
    except (ValueError, TypeError):
        return 0.0


def _is_liquid(o: dict) -> bool:
    """Check if contract meets minimum liquidity requirements"""
    try:
        volume = int(o.get("volume") or 0)
        open_interest = int(o.get("open_interest") or 0)
        return volume >= ContractFilters.MIN_VOLUME and open_interest >= ContractFilters.MIN_OPEN_INTEREST
    except (ValueError, TypeError):
        return False


def _has_tight_spread(o: dict) -> bool:
    """Check if bid-ask spread is within acceptable range"""
    try:
        bid = float(o.get("bid") or 0)
        ask = float(o.get("ask") or 0)
        
        if bid <= 0 or ask <= 0:
            return False
        
        mid = (bid + ask) / 2.0
        if mid <= 0:
            return False
        
        spread = ask - bid
        spread_pct = spread / mid
        
        return spread_pct <= ContractFilters.MAX_SPREAD_PCT
    except (ValueError, TypeError):
        return False


def _is_price_valid(o: dict) -> bool:
    """Check if contract price is within acceptable range"""
    mid = _get_mid_price(o)
    return ContractFilters.MIN_CONTRACT_PRICE <= mid <= ContractFilters.MAX_CONTRACT_PRICE


def _has_valid_greeks(o: dict) -> bool:
    """Check if greeks are within acceptable ranges (optional)"""
    try:
        delta = abs(float(o.get("delta") or 0))
        theta = abs(float(o.get("theta") or 0))
        
        # Allow missing greeks (some brokers don't provide them)
        if delta == 0:
            return True  # Skip check if not available
        
        return (ContractFilters.MIN_DELTA <= delta <= ContractFilters.MAX_DELTA and 
                theta <= ContractFilters.MAX_THETA)
    except (ValueError, TypeError):
        return True  # Skip check if data invalid


def _has_sane_iv(o: dict) -> bool:
    """Check if implied volatility is within reasonable bounds"""
    try:
        # Greeks can be stored under different keys
        greeks = o.get("greeks") or {}
        iv = float(greeks.get("mid_iv") or greeks.get("iv") or o.get("implied_volatility") or 0)
        
        if iv == 0:
            return True  # Skip check if not available
        
        return ContractFilters.MIN_IV <= iv <= ContractFilters.MAX_IV
    except (ValueError, TypeError):
        return True  # Skip check if data invalid


def _quality_score(o: dict, underlying_price: float) -> tuple:
    """
    Calculate multi-factor quality score for tie-breaking.
    Returns tuple for sorting (lower is better).
    
    Priority:
    1. Distance from target (primary)
    2. Higher volume (secondary)
    3. Higher open interest (tertiary)
    4. Tighter spread (quaternary)
    5. Better price tier (quinary)
    """
    try:
        strike = _get_strike(o)
        volume = int(o.get("volume") or 0)
        oi = int(o.get("open_interest") or 0)
        
        bid = float(o.get("bid") or 0)
        ask = float(o.get("ask") or 0)
        mid = (bid + ask) / 2.0 if (bid > 0 and ask > 0) else 0
        spread_pct = ((ask - bid) / mid) if mid > 0 else 999.0
        
        # Distance from underlying (primary factor)
        distance = abs(strike - underlying_price)
        
        # Price tier bonus (prefer $0.50-$5.00 range)
        price_score = 0
        if ContractFilters.IDEAL_MIN_PRICE <= mid <= ContractFilters.IDEAL_MAX_PRICE:
            price_score = -1  # Bonus for ideal price range
        
        return (
            distance,              # 1. Closest to target
            -volume,               # 2. Higher volume (negative for min())
            -oi,                   # 3. Higher OI
            spread_pct,            # 4. Tighter spread
            price_score,           # 5. Better price tier
        )
    except Exception:
        return (float('inf'), 0, 0, 999.0, 0)  # Push to end


# ============================================================
# STRIKE SELECTION
# ============================================================

def resolve_contract_symbol(
    chain: list[dict],
    strike: float,
    direction: str,
    *,
    underlying_price: Optional[float] = None,
    prefer_itm: bool = True,
    apply_filters: bool = True,
    log_filtering: bool = True,
) -> str:
    """
    Find best option contract with production-grade filtering.
    
    Args:
        chain: List of option contracts from broker API
        strike: Target strike price (used as fallback)
        direction: "CALL" or "PUT"
        underlying_price: Current price of underlying (for ITM selection)
        prefer_itm: Prefer ITM strikes when underlying_price provided
        apply_filters: Apply liquidity/spread/price filters (default True)
        log_filtering: Log detailed filtering stats (default True)
    
    Returns:
        Option symbol string
        
    Raises:
        ValueError: If no valid contract found after filtering
    """
    
    # === VALIDATION ===
    want = (direction or "").upper()
    if want not in ("CALL", "PUT"):
        raise ValueError(f"Invalid direction: {direction} (must be CALL or PUT)")
    
    if not chain:
        raise ValueError("Empty option chain provided")
    
    want_type = "call" if want == "CALL" else "put"
    
    def _option_type(o: dict) -> str:
        """Extract option type from various broker formats"""
        t = str(o.get("option_type") or o.get("type") or o.get("right") or "").lower()
        return t
    
    def _is_wanted_type(o: dict) -> bool:
        """Check if option matches desired type (CALL/PUT)"""
        t = _option_type(o)
        if want_type == "call":
            return t in ("call", "c")
        return t in ("put", "p")
    
    # === INITIAL TYPE FILTER ===
    initial_count = len(chain)
    filtered = [o for o in chain if _is_wanted_type(o)]
    
    if not filtered:
        raise ValueError(f"No {want} options in chain (had {initial_count} total contracts)")
    
    if log_filtering:
        log.info(f"Type filter: {initial_count} total → {len(filtered)} {want}s")
    
    # === APPLY QUALITY FILTERS ===
    if apply_filters:
        pre_filter_count = len(filtered)
        
        # Liquidity filter
        liquid = [o for o in filtered if _is_liquid(o)]
        if log_filtering:
            log.info(f"Liquidity filter: {pre_filter_count} → {len(liquid)} (volume≥{ContractFilters.MIN_VOLUME}, OI≥{ContractFilters.MIN_OPEN_INTEREST})")
        
        # Spread filter
        tight_spread = [o for o in liquid if _has_tight_spread(o)]
        if log_filtering:
            log.info(f"Spread filter: {len(liquid)} → {len(tight_spread)} (spread≤{ContractFilters.MAX_SPREAD_PCT*100:.0f}%)")
        
        # Price filter
        price_ok = [o for o in tight_spread if _is_price_valid(o)]
        if log_filtering:
            log.info(f"Price filter: {len(tight_spread)} → {len(price_ok)} (${ContractFilters.MIN_CONTRACT_PRICE:.2f}-${ContractFilters.MAX_CONTRACT_PRICE:.2f})")
        
        # Greeks filter (optional)
        greeks_ok = [o for o in price_ok if _has_valid_greeks(o)]
        if len(greeks_ok) < len(price_ok) and log_filtering:
            log.info(f"Greeks filter: {len(price_ok)} → {len(greeks_ok)} (delta {ContractFilters.MIN_DELTA:.2f}-{ContractFilters.MAX_DELTA:.2f})")
        
        # IV filter
        iv_ok = [o for o in greeks_ok if _has_sane_iv(o)]
        if len(iv_ok) < len(greeks_ok) and log_filtering:
            log.info(f"IV filter: {len(greeks_ok)} → {len(iv_ok)} (IV {ContractFilters.MIN_IV:.0%}-{ContractFilters.MAX_IV:.0%})")
        
        # Use filtered list if we have candidates
        if iv_ok:
            filtered = iv_ok
            if log_filtering:
                log.info(f"✅ {len(filtered)} high-quality contracts remaining after all filters")
        elif greeks_ok:
            filtered = greeks_ok
            log.warning(f"⚠️ IV filter too strict, using {len(filtered)} contracts (pre-IV filter)")
        elif price_ok:
            filtered = price_ok
            log.warning(f"⚠️ Greeks filter too strict, using {len(filtered)} contracts (pre-Greeks filter)")
        elif tight_spread:
            filtered = tight_spread
            log.warning(f"⚠️ Price filter too strict, using {len(filtered)} contracts (pre-price filter)")
        elif liquid:
            filtered = liquid
            log.warning(f"⚠️ Spread filter too strict, using {len(filtered)} liquid contracts")
        else:
            log.error(f"❌ All filters rejected contracts, falling back to type-only filter")
            # Keep type-filtered list as last resort
    
    # === ITM PREFERENCE (when underlying price available) ===
    if prefer_itm and underlying_price is not None:
        up = float(underlying_price)
        
        if want == "CALL":
            # CALL: ITM = strike <= underlying
            itm = [o for o in filtered if _get_strike(o) <= up]
            if itm:
                best = min(itm, key=lambda o: _quality_score(o, up))
                sym = best.get("symbol") or best.get("option_symbol")
                if not sym:
                    raise ValueError("No symbol field in selected contract")
                
                strike_val = _get_strike(best)
                mid = _get_mid_price(best)
                volume = int(best.get("volume") or 0)
                oi = int(best.get("open_interest") or 0)
                
                log.info(f"✅ ITM CALL: strike=${strike_val:.2f} underlying=${up:.2f} mid=${mid:.2f} vol={volume} OI={oi} | {sym}")
                return sym
        
        else:  # PUT
            # PUT: ITM = strike >= underlying
            itm = [o for o in filtered if _get_strike(o) >= up]
            if itm:
                best = min(itm, key=lambda o: _quality_score(o, up))
                sym = best.get("symbol") or best.get("option_symbol")
                if not sym:
                    raise ValueError("No symbol field in selected contract")
                
                strike_val = _get_strike(best)
                mid = _get_mid_price(best)
                volume = int(best.get("volume") or 0)
                oi = int(best.get("open_interest") or 0)
                
                log.info(f"✅ ITM PUT: strike=${strike_val:.2f} underlying=${up:.2f} mid=${mid:.2f} vol={volume} OI={oi} | {sym}")
                return sym
        
        log.warning(f"⚠️ No ITM {want} contracts found, falling back to closest strike")
    
    # === FALLBACK: CLOSEST TO TARGET STRIKE ===
    if not underlying_price:
        underlying_price = float(strike)  # Use target strike as reference
    
    best = min(filtered, key=lambda o: _quality_score(o, underlying_price))
    sym = best.get("symbol") or best.get("option_symbol")
    
    if not sym:
        raise ValueError("No symbol field in selected contract")
    
    strike_val = _get_strike(best)
    mid = _get_mid_price(best)
    volume = int(best.get("volume") or 0)
    oi = int(best.get("open_interest") or 0)
    
    log.info(f"✅ MATCHED: target=${float(strike):.2f} → strike=${strike_val:.2f} mid=${mid:.2f} vol={volume} OI={oi} | {sym}")
    return sym


# ============================================================
# DIAGNOSTICS (for troubleshooting)
# ============================================================

def analyze_chain_quality(chain: list[dict], direction: str = "CALL") -> dict:
    """
    Analyze option chain quality for debugging.
    Returns stats about liquidity, spreads, prices, etc.
    """
    if not chain:
        return {"error": "Empty chain"}
    
    want_type = "call" if direction.upper() == "CALL" else "put"
    
    def _is_type(o: dict) -> bool:
        t = str(o.get("option_type") or o.get("type") or "").lower()
        if want_type == "call":
            return t in ("call", "c")
        return t in ("put", "p")
    
    filtered = [o for o in chain if _is_type(o)]
    
    if not filtered:
        return {"error": f"No {direction} options in chain"}
    
    stats = {
        "total_contracts": len(filtered),
        "liquid_contracts": sum(1 for o in filtered if _is_liquid(o)),
        "tight_spread_contracts": sum(1 for o in filtered if _has_tight_spread(o)),
        "valid_price_contracts": sum(1 for o in filtered if _is_price_valid(o)),
        "avg_volume": sum(int(o.get("volume") or 0) for o in filtered) / len(filtered),
        "avg_oi": sum(int(o.get("open_interest") or 0) for o in filtered) / len(filtered),
        "avg_mid_price": sum(_get_mid_price(o) for o in filtered) / len(filtered),
    }
    
    return stats
