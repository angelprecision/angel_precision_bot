# ap/manual_trades.py (NEW)
"""
Manual trade entry function.
Allows you to place trades directly via code or API without waiting for scanner signals.

Usage:
    place_manual_trade(
        symbol="SMCI",
        direction="CALL",
        strike=34,
        expiry_hint="Weekly",
        account_equity=10000.0,
        client_id="default"
    )
"""

import uuid
from datetime import datetime, timezone

from ap.logger import get_logger
from ap.models import Signal
from ap.execution import process_signal
from ap.client_manager import get_client_broker

log = get_logger("ap.manual_trades")


def place_manual_trade(
    symbol: str,
    direction: str,
    strike: float,
    expiry_hint: str = "Weekly",
    account_equity: float = 10000.0,
    entry_price: float = 0.0,
    stop_loss_price: float = 0.0,
    pt1_price: float = 0.0,
    pt2_price: float = 0.0,
    pt3_price: float = 0.0,
    client_id: str = "default",
) -> dict:
    """
    Place a manual trade (same execution logic as scanner signals).
    
    Args:
        symbol: Stock ticker (e.g., "SMCI", "SPY")
        direction: "CALL" or "PUT"
        strike: Strike price (e.g., 34.0)
        expiry_hint: "Weekly" or "0DTE"
        account_equity: Current account equity (for 15% position sizing)
        entry_price: Entry price from your analysis (optional)
        stop_loss_price: Stop loss price (optional)
        pt1_price: Profit target 1 (optional)
        pt2_price: Profit target 2 (optional)
        pt3_price: Profit target 3 (optional)
        client_id: Client identifier (default: "default")
    
    Returns:
        dict: Response with ok, reason, plan details
        
    Example:
        response = place_manual_trade(
            symbol="SMCI",
            direction="CALL",
            strike=34,
            expiry_hint="Weekly",
            account_equity=10000.0,
            entry_price=33.63,
            stop_loss_price=32.48,
            pt1_price=34.79,
            pt2_price=35.50,
            client_id="default"
        )
        
        if response["ok"]:
            print(f"Trade placed! Order ID: {response['broker_order_id']}")
        else:
            print(f"Trade failed: {response['reason']}")
    """
    
    # Validate inputs
    if not symbol or not symbol.isalpha():
        return {"ok": False, "reason": "INVALID_SYMBOL", "error": "Symbol must be alphabetic"}
    
    direction = direction.upper()
    if direction not in ("CALL", "PUT"):
        return {"ok": False, "reason": "INVALID_DIRECTION", "error": "Direction must be CALL or PUT"}
    
    try:
        strike = float(strike)
    except (ValueError, TypeError):
        return {"ok": False, "reason": "INVALID_STRIKE", "error": "Strike must be a number"}
    
    if strike <= 0:
        return {"ok": False, "reason": "INVALID_STRIKE", "error": "Strike must be positive"}
    
    if account_equity <= 0:
        return {"ok": False, "reason": "INVALID_EQUITY", "error": "Account equity must be positive"}
    
    # Create a Signal object as if it came from the scanner
    signal = Signal(
        signal_id=str(uuid.uuid4()),
        symbol=symbol.upper(),
        direction=direction,
        pattern_id="MANUAL_ENTRY",
        confidence_tag="manual",
        timestamp_iso=datetime.now(timezone.utc).isoformat(),
        trigger={
            "source": "manual",
            "strike": strike,
            "expiry_hint": expiry_hint,
            "entry": entry_price,
            "stop": stop_loss_price,
            "pt1": pt1_price,
            "pt2": pt2_price,
            "pt3": pt3_price,
            "raw_strike_line": f"{symbol} {strike} {direction} {expiry_hint}",
            "scanned_at": datetime.now(timezone.utc).isoformat(),
            "current": 0.0,
        }
    )
    
    log.info(f"Manual trade: {symbol} {direction} {strike} {expiry_hint} (equity=${account_equity:.2f})")
    
    # Get broker for this client
    try:
        broker = get_client_broker(client_id)
    except Exception as e:
        log.error(f"Failed to get broker for client {client_id}: {e}")
        return {
            "ok": False,
            "reason": "BROKER_ERROR",
            "error": str(e),
            "client_id": client_id
        }
    
    # Execute using the standard process_signal pipeline
    try:
        result = process_signal(signal, broker, client_id=client_id)
        return result
    except Exception as e:
        log.exception(f"Manual trade execution failed: {e}")
        return {
            "ok": False,
            "reason": "EXECUTION_ERROR",
            "error": str(e),
            "client_id": client_id
        }
