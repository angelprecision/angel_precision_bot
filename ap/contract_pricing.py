# ap/contract_pricing.py
from ap.logger import get_logger
from ap.broker import BrokerAdapter

log = get_logger("ap.pricing")

def _to_float(x):
    try:
        if x is None:
            return None
        v = float(x)
        return v
    except Exception:
        return None

def get_contract_price(broker: BrokerAdapter, contract_symbol: str, side: str = "SELL") -> float:
    """
    Get a usable price for an option contract.

    side:
      - "SELL": use bid first (what you can sell for)
      - "BUY" : use ask first (what you would pay)
    """
    try:
        quote = broker.get_quote(contract_symbol) or {}

        bid = _to_float(quote.get("bid"))
        ask = _to_float(quote.get("ask"))
        last = _to_float(quote.get("last"))

        # Choose best primary based on side
        primary = bid if side.upper() == "SELL" else ask
        if primary is not None and primary > 0:
            return primary

        # Fallbacks
        if last is not None and last > 0:
            return last

        if bid is not None and ask is not None and bid > 0 and ask > 0:
            return (bid + ask) / 2.0

        log.warning(f"No valid price for {contract_symbol}, quote={quote}")
        return 0.0

    except Exception as e:
        log.error(f"Failed to get price for {contract_symbol}: {e}")
        return 0.0
