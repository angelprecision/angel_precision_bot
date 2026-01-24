# ap/broker.py
import random
from dataclasses import dataclass
from typing import Optional, Dict, Any, List

@dataclass
class BrokerOrderResponse:
    broker_order_id: str
    status: str  # NEW | ACK | FILLED | REJECTED | PARTIAL | CANCELED
    filled_qty: int
    avg_fill_price: float
    error: Optional[str] = None
    raw: Optional[Dict[str, Any]] = None  # keep raw broker payload (debug)


class BrokerAdapter:
    def get_account_equity(self) -> float:
        raise NotImplementedError

    def get_order(self, order_id: str) -> Dict[str, Any]:
        """
        Return the broker's raw order payload as dict.
        Required for reconciliation.
        """
        raise NotImplementedError

    def get_quote(self, symbol: str) -> Dict[str, Any]:
        """
        Quote dict with bid/ask/last where possible.
        Needed for contract pricing.
        """
        raise NotImplementedError

    def get_option_expirations(self, symbol: str) -> List[str]:
        raise NotImplementedError

    def get_option_chain(self, symbol: str, expiration: str) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def place_order(
        self,
        symbol: str,
        contract: str,
        qty: int,
        limit_price: Optional[float],
        side: str = "buy_to_open",
    ) -> BrokerOrderResponse:
        """
        side: buy_to_open | buy_to_close | sell_to_open | sell_to_close
        SIM ignores side; Tradier uses it.
        """
        raise NotImplementedError

    def close_position(self, position_id: str) -> BrokerOrderResponse:
        raise NotImplementedError


class SimBroker(BrokerAdapter):
    """
    MVP sim broker: fills immediately around limit_price or random mid.
    """

    def __init__(self, starting_equity: float = 10000.0):
        self.equity = float(starting_equity)
        self._orders: Dict[str, Dict[str, Any]] = {}

    def get_account_equity(self) -> float:
        return self.equity

    def get_order(self, order_id: str) -> Dict[str, Any]:
        o = self._orders.get(order_id)
        if not o:
            raise RuntimeError(f"SIM: order not found: {order_id}")
        return o

    def get_quote(self, symbol: str) -> Dict[str, Any]:
        bid = round(random.uniform(0.8, 1.1), 2)
        ask = round(bid + random.uniform(0.05, 0.15), 2)
        last = round((bid + ask) / 2.0, 2)
        return {"symbol": symbol, "bid": bid, "ask": ask, "last": last}

    def get_option_expirations(self, symbol: str) -> List[str]:
        return ["2026-01-30", "2026-02-06"]

    def get_option_chain(self, symbol: str, expiration: str) -> List[Dict[str, Any]]:
        return []

    def place_order(
        self,
        symbol: str,
        contract: str,
        qty: int,
        limit_price: Optional[float],
        side: str = "buy_to_open",
    ) -> BrokerOrderResponse:
        broker_order_id = f"SIM-{symbol}-{random.randint(100000,999999)}"
        fill_price = float(limit_price) if limit_price is not None else round(random.uniform(0.8, 1.2), 2)

        raw = {
            "id": broker_order_id,
            "status": "filled",
            "symbol": symbol,
            "option_symbol": contract,
            "side": side,
            "quantity": qty,
            "avg_fill_price": fill_price,
            "exec_quantity": qty,
        }
        self._orders[broker_order_id] = raw

        return BrokerOrderResponse(
            broker_order_id=broker_order_id,
            status="FILLED",
            filled_qty=qty,
            avg_fill_price=fill_price,
            error=None,
            raw=raw,
        )

    def close_position(self, position_id: str) -> BrokerOrderResponse:
        broker_order_id = f"SIM-CLOSE-{random.randint(100000,999999)}"
        raw = {"id": broker_order_id, "status": "filled", "position_id": position_id}
        self._orders[broker_order_id] = raw
        return BrokerOrderResponse(
            broker_order_id=broker_order_id,
            status="FILLED",
            filled_qty=0,
            avg_fill_price=0.0,
            error=None,
            raw=raw,
        )
