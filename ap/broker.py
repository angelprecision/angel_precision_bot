import random
from dataclasses import dataclass
from typing import Optional
from ap.utils import now_utc_iso

@dataclass
class BrokerOrderResponse:
    broker_order_id: str
    status: str  # ACK | FILLED | REJECTED | PARTIAL
    filled_qty: int
    avg_fill_price: float
    error: Optional[str] = None

class BrokerAdapter:
    def get_account_equity(self) -> float:
        raise NotImplementedError

    def place_order(self, symbol: str, contract: str, qty: int, limit_price: Optional[float]) -> BrokerOrderResponse:
        raise NotImplementedError

    def close_position(self, position_id: str) -> BrokerOrderResponse:
        raise NotImplementedError

class SimBroker(BrokerAdapter):
    """
    MVP sim broker: fills immediately around limit_price or random mid.
    Replace later with Alpaca/Tradier/IBKR adapter.
    """
    def __init__(self, starting_equity: float = 10000.0):
        self.equity = float(starting_equity)

    def get_account_equity(self) -> float:
        return self.equity

    def place_order(self, symbol: str, contract: str, qty: int, limit_price: Optional[float]) -> BrokerOrderResponse:
        broker_order_id = f"SIM-{symbol}-{random.randint(100000,999999)}"
        fill_price = float(limit_price) if limit_price is not None else round(random.uniform(0.8, 1.2), 2)
        return BrokerOrderResponse(
            broker_order_id=broker_order_id,
            status="FILLED",
            filled_qty=qty,
            avg_fill_price=fill_price
        )

    def close_position(self, position_id: str) -> BrokerOrderResponse:
        broker_order_id = f"SIM-CLOSE-{random.randint(100000,999999)}"
        return BrokerOrderResponse(
            broker_order_id=broker_order_id,
            status="FILLED",
            filled_qty=0,
            avg_fill_price=0.0
        )

