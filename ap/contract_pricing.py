# ap/contract_pricing.py
from ap.logger import get_logger
from ap.broker import BrokerAdapter

log = get_logger("ap.pricing")

def get_contract_price(broker: BrokerAdapter, contract_symbol: str) -> float:
    """
    Get current bid price for an option contract.
    Returns the bid (what you can sell for).
    """
    try:
        quote = broker.get_quote(contract_symbol)
        
        # Tradier returns quote dict with 'bid', 'ask', 'last'
        bid = quote.get("bid")
        if bid is not None and float(bid) > 0:
            return float(bid)
        
        # Fallback to last if bid is 0 or None
        last = quote.get("last")
        if last is not None and float(last) > 0:
            return float(last)
        
        # Fallback to midpoint
        ask = quote.get("ask")
        if bid is not None and ask is not None:
            return (float(bid) + float(ask)) / 2.0
        
        log.warning(f"No valid price for {contract_symbol}, quote: {quote}")
        return 0.0
        
    except Exception as e:
        log.error(f"Failed to get price for {contract_symbol}: {e}")
        return 0.0
