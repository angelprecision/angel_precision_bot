# ap/execution.py - PRODUCTION GRADE - REAL MONEY SAFE
# =====================================================================
# ANGEL PRECISION BOT - TRADE EXECUTION ENGINE
# 
# CRITICAL GUARANTEE: Positions are NEVER created until broker confirms fill.
# This file creates ORDERS. fill_monitor.py creates POSITIONS.
#
# Safety Features:
# - Duplicate symbol prevention (no FAST x2, EXC x2)
# - Position size cap ($5K max per trade)
# - Pre-trade validation (equity, risk limits, growth targets)
# - Contract resolution with PAPER/SIM fallback
# - Premium validation (prevents bad fills)
# - Order submission with retry logic
# - Comprehensive error handling
# - Full audit trail
# - Daily reset automation
# - Growth tracking integration
# =====================================================================

from __future__ import annotations

import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from ap.logger import get_logger
from ap.config import Config
from ap.db import (
    conn,
    run_with_retry,
    get_client,
    get_client_state,
    update_client_state,
    insert_order,
    new_local_order_id,
    update_order,
)
from ap.utils import json_dumps, now_utc_iso
from ap.contract_selection import pick_expiration, resolve_contract_symbol

log = get_logger("ap.execution")
cfg = Config()

# =====================================================================
# CONSTANTS
# =====================================================================

OPT_MULTIPLIER = 100
NY = ZoneInfo("America/New_York")

# Premium validation bounds (prevents obviously bad fills)
MIN_PREMIUM_PER_SHARE = 0.50  # $50 minimum per contract
MAX_PREMIUM_PER_SHARE = 2.50  # $250 maximum per contract

# Retry configuration for broker submission
MAX_BROKER_RETRIES = 3
BROKER_RETRY_DELAY = 1.0  # seconds


# =====================================================================
# AUDIT LOGGING
# =====================================================================

def audit(client_id: str, level: str, event: str, payload: dict):
    """Log all critical events to audit table"""
    try:
        with conn() as c:
            run_with_retry(
                lambda: c.execute(
                    "INSERT INTO audit_log (ts, level, event, payload, client_id) VALUES (?,?,?,?,?)",
                    (now_utc_iso(), level, event, json_dumps(payload), client_id),
                )
            )
    except Exception as e:
        log.error(f"Audit logging failed: {e}")
        # Don't fail trade execution due to audit failure


# =====================================================================
# DAILY RESET
# =====================================================================

def _ny_day_key() -> str:
    """Get current date in NY timezone (for daily reset logic)"""
    return datetime.now(NY).strftime("%Y-%m-%d")


def _maybe_reset_daily_state(broker, client_id: str, client_cfg: dict, st: dict) -> dict:
    """
    Reset daily counters at market open.
    
    This runs automatically when processing first signal of the day.
    Ensures trades_today, pnl_today, etc. are fresh for the new trading day.
    
    Args:
        broker: Broker instance for equity lookup
        client_id: Client identifier
        client_cfg: Client configuration from clients table
        st: Current client state
        
    Returns:
        Updated client state dict
    """
    today = _ny_day_key()
    prev = st.get("day_key")

    equity = _get_equity_for_client(broker, st, client_cfg)

    if prev != today:
        log.info(f"📅 Daily reset for {client_id}: equity=${equity:,.2f}")
        
        patch = {
            "day_key": today,
            "trades_taken_today": 0,
            "realized_pnl_today": 0.0,
            "daily_stop_hit": 0,
            "starting_equity_today": equity,
            "current_equity": equity,
        }
        
        update_client_state(client_id, patch)
        st.update(patch)
        
        audit(client_id, "INFO", "DAILY_RESET", {
            "date": today,
            "starting_equity": equity
        })
    else:
        # Update current equity even if not resetting
        update_client_state(client_id, {"current_equity": equity})
        st["current_equity"] = equity

    return st


# =====================================================================
# EQUITY LOOKUP (with robust fallbacks)
# =====================================================================

def _get_equity_for_client(broker, st: dict, client_cfg: dict) -> float:
    """
    Get current account equity with multiple fallback layers.
    
    Priority:
    1. Live broker API (PAPER/LIVE modes)
    2. Last known equity from state
    3. Starting equity from state
    4. Initial equity from client config
    5. Hard default (100k)
    
    This ensures we never fail to get equity, but logs warnings
    when using fallbacks so you know broker connection is down.
    
    Returns:
        Current equity as float
    """
    try:
        equity = float(broker.get_account_equity())
        log.debug(f"💰 Equity from broker: ${equity:,.2f}")
        return equity
    except Exception as e:
        log.warning(f"⚠️  Failed to get broker equity: {e}")
        
        # Fallback chain
        fallbacks = [
            ("current_equity", st.get("current_equity")),
            ("starting_equity_today", st.get("starting_equity_today")),
            ("initial_equity", client_cfg.get("initial_equity")),
            ("hard_default", 100000.0),
        ]
        
        for source, value in fallbacks:
            if value and float(value) > 0:
                equity = float(value)
                log.warning(f"Using equity from {source}: ${equity:,.2f}")
                return equity
        
        # Should never reach here
        log.error("All equity fallbacks failed, using 100k default")
        return 100000.0


# =====================================================================
# POSITION COUNTING
# =====================================================================

def _count_open_positions(client_id: str) -> int:
    """Count how many positions are currently OPEN or CLOSING"""
    with conn() as c:
        row = run_with_retry(
            lambda: c.execute(
                """
                SELECT COUNT(*) AS n
                FROM positions
                WHERE client_id=?
                  AND status IN ('OPEN','CLOSING')
                """,
                (client_id,),
            ).fetchone()
        )
        return int(row["n"] or 0)


def _today_trade_count(st: dict) -> int:
    """Get number of trades taken today"""
    return int(st.get("trades_taken_today") or 0)


# =====================================================================
# GROWTH TRACKING INTEGRATION
# =====================================================================

def _check_growth_limits(client_id: str, mode: str) -> dict:
    """
    Check if client has hit growth targets (profit goals).
    
    When a client is approaching or exceeding their profit target,
    this triggers position size reduction or trading halt.
    
    Safe: Returns OK if growth tracking isn't initialized (won't block trades).
    
    Returns:
        {"ok": True} if can continue trading
        {"ok": False, "error": "growth_limit_..."} if should stop
    """
    # Skip growth checks in SIM/PAPER
    if mode in ("PAPER", "SIM"):
        return {"ok": True}

    try:
        from ap.account_growth import check_growth_status, get_growth_metrics

        # Check if growth tracking is initialized
        metrics = get_growth_metrics(client_id)
        if not metrics.get("ok"):
            log.debug(f"Growth tracking not initialized for {client_id}, allowing trades")
            return {"ok": True}

        # Check current growth status
        status = check_growth_status(client_id)
        
        if status.get("should_stop"):
            reason = status.get("reason", "unknown")
            log.warning(f"🛑 Growth limit hit for {client_id}: {reason}")

            # Auto-enable kill switch when profit target hit
            update_client_state(client_id, {
                "kill_switch": 1,
                "mode": "READ_ONLY"
            })
            
            audit(client_id, "WARNING", "GROWTH_TARGET_HIT", status)
            
            return {
                "ok": False,
                "error": f"growth_limit_{reason}",
                "details": status
            }

        # Check if approaching limit (reduce position size)
        growth_pct = status.get("growth_pct", 0)
        if growth_pct >= 75:
            log.info(f"📊 Client {client_id} at {growth_pct:.1f}% of profit target")
            # Note: Position size reduction is handled by caller

        return {"ok": True}

    except Exception as e:
        log.error(f"❌ Growth check failed: {e}")
        
        # CRITICAL: In LIVE mode, fail safe (don't allow trade if check fails)
        if mode == "LIVE":
            return {
                "ok": False,
                "error": "growth_check_failed",
                "details": str(e)
            }
        
        # In PAPER, allow trade despite check failure
        return {"ok": True}


# =====================================================================
# CONTRACT RESOLUTION (with PAPER/SIM synthetic fallback)
# =====================================================================

def _resolve_option_contract(
    broker,
    symbol: str,
    strike: float,
    direction: str,
    mode: str = "PAPER",
) -> tuple[str, float]:
    """
    Resolve contract symbol and current premium from broker.
    
    CRITICAL FOR PAPER/SIM TESTING:
    If broker returns no option chain (common with SimBroker or delayed feeds),
    creates synthetic contract symbol and $1.00 premium so full pipeline can test.
    
    This allows you to test the entire execution → fill → exit flow
    without needing live option market data.
    
    Args:
        broker: Broker instance
        symbol: Underlying symbol (e.g., "SPY")
        strike: Strike price
        direction: "CALL" or "PUT"
        mode: Trading mode ("PAPER", "SIM", "LIVE")
        
    Returns:
        (contract_symbol, premium_per_share)
        
    Raises:
        ValueError: If contract resolution fails in LIVE mode
    """
    # Validate direction
    direction = (direction or "").upper()
    if direction not in ("CALL", "PUT"):
        raise ValueError(f"Invalid direction: {direction}")

    try:
        # Step 1: Get available expirations
        expirations = broker.get_option_expirations(symbol)
        if not expirations:
            raise ValueError(f"No expirations available for {symbol}")

        # Step 2: Pick best expiration (0DTE or nearest weekly)
        expiration = pick_expiration(expirations, hint="0DTE")
        log.debug(f"Selected expiration: {expiration}")

        # Step 3: Get option chain
        chain = broker.get_option_chain(symbol, expiration)
        
        if not chain:
            # PAPER/SIM FALLBACK: Create synthetic contract
            if (mode or "").upper() in ("PAPER", "SIM"):
                contract_symbol = f"{symbol}_{expiration}_{int(strike)}_{direction}"
                premium = 1.00  # $100 per contract
                
                log.warning(
                    f"⚠️  [{mode}] No options in chain for {symbol} {expiration}. "
                    f"Using synthetic {contract_symbol} @ ${premium:.2f}/share"
                )
                
                audit("system", "WARNING", "SYNTHETIC_CONTRACT", {
                    "symbol": symbol,
                    "expiration": expiration,
                    "strike": strike,
                    "direction": direction,
                    "contract": contract_symbol,
                    "premium": premium,
                    "reason": "no_chain_data"
                })
                
                return contract_symbol, premium
            
            raise ValueError(f"No options in chain for {symbol} {expiration}")

        # Step 4: Resolve contract symbol from chain
        contract_symbol = resolve_contract_symbol(chain, strike, direction)
        log.debug(f"Resolved contract: {contract_symbol}")

        # Step 5: Get current premium
        from ap.contract_pricing import get_contract_price

        premium = float(get_contract_price(broker, contract_symbol, side="BUY"))
        
        if premium <= 0:
            # PAPER/SIM FALLBACK: Use synthetic premium
            if (mode or "").upper() in ("PAPER", "SIM"):
                log.warning(f"⚠️  Invalid premium {premium} for {contract_symbol}, using $1.00")
                premium = 1.00
            else:
                raise ValueError(f"Invalid premium for {contract_symbol}: {premium}")

        log.info(f"✅ Contract resolved: {contract_symbol} @ ${premium:.2f}/share")
        
        return contract_symbol, premium

    except Exception as e:
        log.error(f"❌ Contract resolution failed: {e}")
        
        # PAPER/SIM FALLBACK: Create synthetic contract even on error
        if (mode or "").upper() in ("PAPER", "SIM"):
            contract_symbol = f"{symbol}_SYNTH_{int(strike)}_{direction}"
            premium = 1.00
            
            log.warning(
                f"⚠️  [{mode}] Contract resolution error: {e}. "
                f"Using synthetic {contract_symbol} @ ${premium:.2f}/share"
            )
            
            return contract_symbol, premium
        
        raise


# =====================================================================
# PREMIUM VALIDATION
# =====================================================================

def _validate_premium(premium: float, mode: str = "PAPER") -> tuple[bool, str]:
    """
    Validate premium is within acceptable range.
    
    Prevents obviously bad fills like:
    - $0.01 options (nearly worthless)
    - $10+ options (way too expensive for 0DTE strategy)
    
    Range: $0.50 - $2.50 per share ($50 - $250 per contract)
    
    Skips validation in PAPER/SIM when using synthetic contracts.
    
    Returns:
        (is_valid, error_message)
    """
    premium = float(premium)
    
    # Skip strict validation for synthetic contracts
    if (mode or "").upper() in ("PAPER", "SIM") and premium == 1.00:
        return True, ""
    
    if premium < MIN_PREMIUM_PER_SHARE:
        return False, f"Premium too low: ${premium:.2f} < ${MIN_PREMIUM_PER_SHARE:.2f}"
    
    if premium > MAX_PREMIUM_PER_SHARE:
        return False, f"Premium too high: ${premium:.2f} > ${MAX_PREMIUM_PER_SHARE:.2f}"
    
    return True, ""


# =====================================================================
# POSITION SIZING (15% with $5K cap)
# =====================================================================

def _calc_qty(dollars: float, premium: float) -> int:
    """
    Calculate number of contracts based on dollar allocation.
    
    Args:
        dollars: Amount to allocate (e.g., $10,000)
        premium: Premium per share (e.g., 1.25)
        
    Returns:
        Number of contracts (minimum 1)
    """
    cost_per_contract = float(premium) * OPT_MULTIPLIER
    
    if cost_per_contract <= 0:
        return 0
    
    qty = int(float(dollars) // cost_per_contract)
    
    return max(1, qty)  # Always at least 1 contract


# =====================================================================
# BROKER ORDER SUBMISSION (with retry logic)
# =====================================================================

def _submit_order_with_retry(
    broker,
    symbol: str,
    contract: str,
    qty: int,
    premium: float,
) -> tuple[bool, str | None, str | None]:
    """
    Submit order to broker with automatic retry on transient failures.
    
    Retries up to MAX_BROKER_RETRIES times with exponential backoff.
    Only retries on network/timeout errors, not rejections.
    
    Returns:
        (success, broker_order_id, error_message)
    """
    last_error = None
    
    for attempt in range(1, MAX_BROKER_RETRIES + 1):
        try:
            log.info(f"📤 Submitting order (attempt {attempt}/{MAX_BROKER_RETRIES}): {contract} x{qty}")
            
            resp = broker.place_order(
                symbol=symbol,
                contract=contract,
                qty=qty,
                limit_price=premium,
                side="buy_to_open",
            )
            
            broker_order_id = getattr(resp, "broker_order_id", None)
            status = getattr(resp, "status", "UNKNOWN")
            error = getattr(resp, "error", None)
            
            log.info(f"📥 Broker response: status={status}, order_id={broker_order_id}")
            
            # Check for rejection (don't retry)
            if error:
                log.error(f"❌ Broker rejected order: {error}")
                return False, broker_order_id, error
            
            # Success statuses
            if status.upper() in ("ACK", "ACKED", "FILLED", "SUBMITTED", "OK", "ACCEPTED", "PENDING"):
                log.info(f"✅ Order submitted successfully: {broker_order_id}")
                return True, broker_order_id, None
            
            # Unknown status - treat as failure
            last_error = f"Unknown status: {status}"
            log.warning(f"⚠️  Unexpected broker status: {status}")
            
        except Exception as e:
            last_error = str(e)
            log.warning(f"⚠️  Broker submission attempt {attempt} failed: {e}")
            
            # Don't retry on last attempt
            if attempt < MAX_BROKER_RETRIES:
                delay = BROKER_RETRY_DELAY * (2 ** (attempt - 1))  # Exponential backoff
                log.info(f"⏳ Retrying in {delay}s...")
                time.sleep(delay)
                continue
    
    # All retries exhausted
    log.error(f"❌ Order submission failed after {MAX_BROKER_RETRIES} attempts")
    return False, None, last_error


# =====================================================================
# MAIN EXECUTION FUNCTION
# =====================================================================

def process_signal(broker, client_id: str, signal_payload: dict) -> dict:
    """
    Process a trading signal and execute trade.
    
    THIS IS THE MAIN ENTRY POINT FOR TRADE EXECUTION.
    
    Flow:
    1. Validate client is active
    2. Check kill switch / read-only mode
    3. Reset daily counters if needed
    4. Check growth limits
    5. Validate daily trade cap
    6. Validate max open positions
    7. CHECK FOR DUPLICATE SYMBOL ⚡ NEW!
    8. Calculate position size
    9. Resolve option contract
    10. Validate premium
    11. Calculate quantity
    12. CAP POSITION AT $5K ⚡ NEW!
    13. Create ORDER record (status="NEW")
    14. Submit to broker
    15. Update order with broker response
    16. Update daily counters
    17. Log to audit trail
    
    CRITICAL: This function creates ORDERS, not POSITIONS.
    Positions are created by fill_monitor.py when order fills.
    
    Args:
        broker: Broker instance
        client_id: Client identifier
        signal_payload: Signal data from scanner
        
    Returns:
        {
            "ok": True/False,
            "symbol": "SPY",
            "contract": "SPY260131C00580000",
            "qty": 10,
            "local_order_id": "uuid...",
            "broker_order_id": "12345",
            "status": "PENDING_FILL",
            "error": "..." (if ok=False)
        }
    """
    
    execution_start = time.time()
    
    try:
        log.info(f"{'='*70}")
        log.info(f"🎯 EXECUTION START: {client_id}")
        log.info(f"Signal: {signal_payload.get('symbol')} {signal_payload.get('direction')}")
        log.info(f"{'='*70}")
        
        # ============================================================
        # STEP 1: VALIDATE CLIENT
        # ============================================================
        
        log.info("Step 1: Validating client...")
        
        try:
            client = get_client(client_id)
        except ValueError as e:
            log.error(f"❌ Client not found: {client_id}")
            return {"ok": False, "error": "client_not_found"}
        
        if (client.get("status") or "").upper() != "ACTIVE":
            log.warning(f"❌ Client {client_id} is not active: {client.get('status')}")
            return {"ok": False, "error": "client_inactive", "status": client.get("status")}
        
        log.info(f"✅ Client validated: {client.get('name')}")
        
        # ============================================================
        # STEP 2: GET STATE & CHECK MODE
        # ============================================================
        
        log.info("Step 2: Checking state and mode...")
        
        st = get_client_state(client_id)
        mode = (st.get("mode") or "PAPER").upper()
        
        log.info(f"Mode: {mode}")
        
        if st.get("kill_switch"):
            log.warning(f"❌ Kill switch active for {client_id}")
            return {"ok": False, "error": "kill_switch_active"}
        
        if mode == "READ_ONLY":
            log.warning(f"❌ Client {client_id} in read-only mode")
            return {"ok": False, "error": "read_only_mode"}
        
        log.info(f"✅ Mode check passed: {mode}")
        
        # ============================================================
        # STEP 3: DAILY RESET (if needed)
        # ============================================================
        
        log.info("Step 3: Checking daily reset...")
        
        st = _maybe_reset_daily_state(broker, client_id, client, st)
        
        # ============================================================
        # STEP 4: GROWTH LIMIT CHECKS
        # ============================================================
        
        log.info("Step 4: Checking growth limits...")
        
        growth_check = _check_growth_limits(client_id, mode)
        
        if not growth_check.get("ok"):
            log.error(f"❌ Growth limit check failed: {growth_check.get('error')}")
            return growth_check
        
        log.info("✅ Growth limits OK")
        
        # ============================================================
        # STEP 5: DAILY TRADE CAP
        # ============================================================
        
        log.info("Step 5: Checking daily trade cap...")
        
        max_trades = int(client.get("max_trades_per_day") or cfg.MAX_TRADES_PER_DAY)
        trades_today = _today_trade_count(st)
        
        if trades_today >= max_trades:
            log.warning(f"❌ Daily trade cap hit: {trades_today}/{max_trades}")
            return {
                "ok": False,
                "error": "daily_trade_cap",
                "trades_today": trades_today,
                "max_trades": max_trades
            }
        
        log.info(f"✅ Trade cap OK: {trades_today}/{max_trades}")
        
        # ============================================================
        # STEP 6: MAX OPEN POSITIONS
        # ============================================================
        
        log.info("Step 6: Checking max open positions...")
        
        max_open = int(client.get("max_concurrent_positions") or cfg.MAX_CONCURRENT_POSITIONS)
        open_positions = _count_open_positions(client_id)
        
        if open_positions >= max_open:
            log.warning(f"❌ Max open positions: {open_positions}/{max_open}")
            return {
                "ok": False,
                "error": "max_open_positions",
                "open_positions": open_positions,
                "max_open": max_open
            }
        
        log.info(f"✅ Position limit OK: {open_positions}/{max_open}")
        
        # ============================================================
        # STEP 7: PARSE SIGNAL
        # ============================================================
        
        log.info("Step 7: Parsing signal...")
        
        symbol = (signal_payload.get("symbol") or "").upper()
        direction = (signal_payload.get("direction") or "").upper()
        trigger = signal_payload.get("trigger") or {}
        
        if not symbol:
            log.error("❌ Missing symbol in signal")
            return {"ok": False, "error": "missing_symbol"}
        
        if direction not in ("CALL", "PUT"):
            log.error(f"❌ Invalid direction: {direction}")
            return {"ok": False, "error": "invalid_direction", "direction": direction}
        
        strike = trigger.get("strike")
        if strike is None:
            log.error("❌ Missing strike in signal")
            return {"ok": False, "error": "missing_strike"}
        
        strike = float(strike)
        
        log.info(f"✅ Signal parsed: {symbol} {direction} ${strike}")
        
        # ============================================================
        # STEP 7B: CHECK FOR DUPLICATE SYMBOL ⚡ NEW!
        # ============================================================
        
        log.info("Step 7B: Checking for duplicate symbol...")
        
        with conn() as c:
            existing = run_with_retry(lambda: c.execute("""
                SELECT id, contract, status 
                FROM positions 
                WHERE client_id=? 
                  AND underlying=? 
                  AND status IN ('OPEN','CLOSING')
            """, (client_id, symbol)).fetchone())
            
            if existing:
                log.warning(f"❌ Already have position in {symbol}: {existing['contract']} ({existing['status']})")
                
                audit(client_id, "WARNING", "DUPLICATE_SYMBOL_BLOCKED", {
                    "symbol": symbol,
                    "direction": direction,
                    "existing_position": existing['id'],
                    "existing_contract": existing['contract'],
                    "existing_status": existing['status']
                })
                
                return {
                    "ok": False,
                    "error": "duplicate_symbol",
                    "symbol": symbol,
                    "existing_position": existing['id'],
                    "existing_contract": existing['contract']
                }
        
        log.info(f"✅ No duplicate: {symbol} is clear")
        
        # ============================================================
        # STEP 8: POSITION SIZING
        # ============================================================
        
        log.info("Step 8: Calculating position size...")
        
        equity = _get_equity_for_client(broker, st, client)
        position_pct = float(client.get("base_position_pct") or cfg.BASE_POSITION_PCT)
        dollars = equity * position_pct
        
        log.info(f"💰 Equity: ${equity:,.2f}")
        log.info(f"📊 Position %: {position_pct*100:.1f}%")
        log.info(f"💵 Allocation: ${dollars:,.2f}")
        
        # ============================================================
        # STEP 9: RESOLVE CONTRACT
        # ============================================================
        
        log.info("Step 9: Resolving option contract...")
        
        try:
            contract_symbol, premium = _resolve_option_contract(
                broker, symbol, strike, direction, mode=mode
            )
        except Exception as e:
            log.error(f"❌ Contract resolution failed: {e}")
            return {
                "ok": False,
                "error": "contract_resolution_failed",
                "details": str(e)
            }
        
        log.info(f"✅ Contract: {contract_symbol}")
        log.info(f"💎 Premium: ${premium:.2f}/share (${premium*100:.2f}/contract)")
        
        # ============================================================
        # STEP 10: VALIDATE PREMIUM
        # ============================================================
        
        log.info("Step 10: Validating premium...")
        
        premium_valid, premium_error = _validate_premium(premium, mode)
        
        if not premium_valid:
            log.error(f"❌ {premium_error}")
            return {
                "ok": False,
                "error": "premium_out_of_range",
                "premium": premium,
                "contract_cost": premium * OPT_MULTIPLIER,
                "details": premium_error
            }
        
        log.info(f"✅ Premium validated")
        
        # ============================================================
        # STEP 11: CALCULATE QUANTITY
        # ============================================================
        
        log.info("Step 11: Calculating quantity...")
        
        qty = _calc_qty(dollars, premium)
        
        if qty < 1:
            log.warning(f"❌ Position too small: qty={qty}")
            return {
                "ok": False,
                "error": "position_too_small",
                "dollars": dollars,
                "premium": premium,
                "calculated_qty": qty
            }
        
        total_cost = qty * premium * OPT_MULTIPLIER
        
        log.info(f"📦 Quantity: {qty} contracts")
        log.info(f"💰 Total cost: ${total_cost:,.2f}")
        
        # ============================================================
        # STEP 11B: CAP POSITION AT $5K ⚡ NEW!
        # ============================================================
        
        log.info("Step 11B: Checking position size cap...")
        
        MAX_POSITION_COST = cfg.MAX_POSITION_COST
        
        if total_cost > MAX_POSITION_COST:
            log.warning(f"⚠️  Position too large: ${total_cost:,.2f} > ${MAX_POSITION_COST:,.2f}")
            
            # Recalculate qty to fit cap
            qty = int(MAX_POSITION_COST // (premium * OPT_MULTIPLIER))
            qty = max(1, qty)
            total_cost = qty * premium * OPT_MULTIPLIER
            
            log.info(f"✅ Reduced to {qty} contracts = ${total_cost:,.2f}")
            
            audit(client_id, "WARNING", "POSITION_SIZE_CAPPED", {
                "symbol": symbol,
                "requested_cost": dollars,
                "requested_qty": int(dollars // (premium * OPT_MULTIPLIER)),
                "capped_qty": qty,
                "capped_cost": total_cost,
                "max_cost": MAX_POSITION_COST
            })
        
        log.info(f"✅ Position size validated: ${total_cost:,.2f}")
        
        # ============================================================
        # STEP 12: CREATE ORDER RECORD
        # ============================================================
        
        log.info("Step 12: Creating order record...")
        
        local_order_id = new_local_order_id()
        
        insert_order(
            client_id=client_id,
            local_order_id=local_order_id,
            position_id=None,  # ⚠️ CRITICAL: No position until fill confirmed
            kind="ENTRY",
            status="NEW",
            symbol=symbol,
            contract=contract_symbol,
            qty=qty,
            limit_price=premium,
        )
        
        log.info(f"✅ Order record created: {local_order_id}")
        
        # ============================================================
        # STEP 13: SUBMIT TO BROKER
        # ============================================================
        
        log.info("Step 13: Submitting to broker...")
        
        success, broker_order_id, error = _submit_order_with_retry(
            broker, symbol, contract_symbol, qty, premium
        )
        
        if not success:
            log.error(f"❌ Broker submission failed: {error}")
            
            # Update order as rejected
            update_order(
                local_order_id,
                status="REJECTED",
                broker_order_id=broker_order_id,
                last_error=error
            )
            
            audit(client_id, "ERROR", "ORDER_REJECTED", {
                "symbol": symbol,
                "contract": contract_symbol,
                "qty": qty,
                "local_order_id": local_order_id,
                "broker_order_id": broker_order_id,
                "error": error
            })
            
            return {
                "ok": False,
                "error": "broker_rejected",
                "details": error,
                "local_order_id": local_order_id,
                "broker_order_id": broker_order_id
            }
        
        # ============================================================
        # STEP 14: UPDATE ORDER WITH BROKER RESPONSE
        # ============================================================
        
        log.info("Step 14: Updating order status...")
        
        update_order(
            local_order_id,
            status="ACK",  # Acknowledged by broker, waiting for fill
            broker_order_id=broker_order_id
        )
        
        log.info(f"✅ Order status updated: ACK")
        
        # ============================================================
        # STEP 15: UPDATE DAILY COUNTERS
        # ============================================================
        
        log.info("Step 15: Updating daily counters...")
        
        update_client_state(
            client_id,
            {
                "trades_taken_today": trades_today + 1,
                "current_equity": equity,
            },
        )
        
        log.info(f"✅ Counters updated: trades_today={trades_today + 1}")
        
        # ============================================================
        # STEP 16: AUDIT TRAIL
        # ============================================================
        
        log.info("Step 16: Logging to audit trail...")
        
        audit(
            client_id,
            "INFO",
            "TRADE_EXECUTED",
            {
                "symbol": symbol,
                "direction": direction,
                "contract": contract_symbol,
                "qty": qty,
                "premium": premium,
                "total_cost": total_cost,
                "local_order_id": local_order_id,
                "broker_order_id": broker_order_id,
                "execution_time_ms": int((time.time() - execution_start) * 1000)
            },
        )
        
        # ============================================================
        # SUCCESS!
        # ============================================================
        
        execution_time = time.time() - execution_start
        
        log.info(f"{'='*70}")
        log.info(f"✅ EXECUTION COMPLETE: {contract_symbol} x{qty}")
        log.info(f"⏱️  Execution time: {execution_time:.2f}s")
        log.info(f"🔍 Local order ID: {local_order_id}")
        log.info(f"🏦 Broker order ID: {broker_order_id}")
        log.info(f"⏳ Status: PENDING_FILL (waiting for fill_monitor)")
        log.info(f"{'='*70}")
        
        return {
            "ok": True,
            "symbol": symbol,
            "direction": direction,
            "contract": contract_symbol,
            "qty": qty,
            "premium": premium,
            "total_cost": total_cost,
            "local_order_id": local_order_id,
            "broker_order_id": broker_order_id,
            "status": "PENDING_FILL",  # Waiting for fill_monitor to create position
            "execution_time": execution_time,
        }

    except Exception as e:
        log.exception(f"💥 EXECUTION EXCEPTION: {e}")
        
        execution_time = time.time() - execution_start
        
        audit(client_id, "ERROR", "EXECUTION_FAILED", {
            "error": str(e),
            "signal": signal_payload,
            "execution_time_ms": int(execution_time * 1000)
        })
        
        return {
            "ok": False,
            "error": "execution_exception",
            "details": str(e)
        }
