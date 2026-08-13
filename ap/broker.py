# ap/broker.py - BROKER ADAPTERS (PRODUCTION SAFE)
from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Optional, Dict, Any, List


# ============================================================
# Unified response model
# ============================================================
@dataclass
class BrokerOrderResponse:
    broker_order_id: str
    status: str  # NEW | ACK | FILLED | REJECTED | PARTIAL | CANCELED | EXPIRED | UNKNOWN
    filled_qty: int = 0
    avg_fill_price: float = 0.0
    error: Optional[str] = None
    raw: Optional[Dict[str, Any]] = None


# ============================================================
# Normalization helpers (make debugging consistent)
# ============================================================
def normalize_status(s: Any) -> str:
    s = str(s or "").strip().upper()

    mapping = {
        "FILLED": "FILLED",
        "FILL": "FILLED",

        "PARTIALLY_FILLED": "PARTIAL",
        "PARTIAL": "PARTIAL",

        "ACK": "ACK",
        "ACKED": "ACK",
        "OPEN": "ACK",
        "PENDING": "ACK",
        "SUBMITTED": "ACK",
        "ACCEPTED": "ACK",
        "OK": "ACK",
        "NEW": "NEW",

        "REJECTED": "REJECTED",
        "CANCELED": "CANCELED",
        "CANCELLED": "CANCELED",

        "EXPIRED": "EXPIRED",
    }

    # common lowercase payloads
    if s == "FILLED".lower().upper():  # no-op, just clarity
        pass

    return mapping.get(s, "UNKNOWN")


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        v = float(x)
        return v
    except Exception:
        return None


def normalize_quote(q: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize quote dict to {bid, ask, last} floats when possible.
    """
    if not isinstance(q, dict):
        return {"bid": None, "ask": None, "last": None}

    bid = _to_float(q.get("bid"))
    ask = _to_float(q.get("ask"))
    last = _to_float(q.get("last") or q.get("lastPrice") or q.get("mark"))

    return {
        "bid": bid if (bid is not None and bid > 0) else None,
        "ask": ask if (ask is not None and ask > 0) else None,
        "last": last if (last is not None and last > 0) else None,
        "raw": q,
    }


# ============================================================
# Broker adapter interface
# ============================================================
class BrokerAdapter:
    def get_account_equity(self) -> float:
        raise NotImplementedError

    def get_order(self, order_id: str) -> Dict[str, Any]:
        """
        Return broker order payload as dict.
        REQUIRED KEYS (normalized preferred):
          - status (any form, fill_monitor normalizes)
          - exec_quantity or filled_quantity or quantity
          - avg_fill_price or price
          - filled_ts / broker_fill_timestamp only when the adapter has an
            actual execution-time authority; lifecycle update timestamps such
            as ``transaction_date`` must remain non-authoritative
        """
        raise NotImplementedError

    def get_quote(self, symbol: str) -> Dict[str, Any]:
        """
        Return quote dict; pricing will normalize.
        Prefer keys: bid, ask, last.
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
        *,
        tag: Optional[str] = None,
    ) -> BrokerOrderResponse:
        raise NotImplementedError

    def close_position(self, position_id: str) -> BrokerOrderResponse:
        raise NotImplementedError

    def cancel_order(self, broker_order_id: str) -> dict:
        """
        Cancel a live broker order. Must query broker after cancel to confirm.
        Returns: {"ok": bool, "status": str, "broker_order_id": str, "error": str|None}
        """
        raise NotImplementedError


# ============================================================
# SIM BROKER (for pipeline testing)
# ============================================================
class SimBroker(BrokerAdapter):
    """
    MVP sim broker: fills immediately around limit_price or random mid.

    Note:
      - option chain defaults empty -> forces synthetic contract branch
      - set synthetic_chain=True to test resolve_contract_symbol path
    """

    def __init__(self, starting_equity: float = 10000.0, synthetic_chain: bool = False):
        self.equity = float(starting_equity)
        self.synthetic_chain = bool(synthetic_chain)
        self._orders: Dict[str, Dict[str, Any]] = {}

    def get_account_equity(self) -> float:
        return self.equity

    def get_order(self, order_id: str) -> Dict[str, Any]:
        o = self._orders.get(order_id)
        if not o:
            raise RuntimeError(f"SIM: order not found: {order_id}")

        # normalize status for downstream
        o2 = dict(o)
        o2["status"] = normalize_status(o2.get("status"))
        return o2

    def get_quote(self, symbol: str) -> Dict[str, Any]:
        bid = round(random.uniform(0.8, 1.1), 2)
        ask = round(bid + random.uniform(0.05, 0.15), 2)
        last = round((bid + ask) / 2.0, 2)
        return {"symbol": symbol, "bid": bid, "ask": ask, "last": last}

    def get_option_expirations(self, symbol: str) -> List[str]:
        # include "today" occasionally? you can replace with real date logic in tests
        return ["2026-01-30", "2026-02-06"]

    def get_option_chain(self, symbol: str, expiration: str) -> List[Dict[str, Any]]:
        if not self.synthetic_chain:
            return []

        # minimal synthetic chain (CALL/PUT) at a few strikes
        strikes = [100, 105, 110, 115, 120]
        chain: List[Dict[str, Any]] = []
        for k in strikes:
            chain.append({
                "symbol": f"{symbol}{expiration.replace('-','')[2:]}C{int(k):08d}",
                "option_type": "call",
                "strike": float(k),
            })
            chain.append({
                "symbol": f"{symbol}{expiration.replace('-','')[2:]}P{int(k):08d}",
                "option_type": "put",
                "strike": float(k),
            })
        return chain

    def place_order(
        self,
        symbol: str,
        contract: str,
        qty: int,
        limit_price: Optional[float],
        side: str = "buy_to_open",
        *,
        tag: Optional[str] = None,
    ) -> BrokerOrderResponse:
        broker_order_id = f"SIM-{symbol}-{random.randint(100000,999999)}"
        fill_price = float(limit_price) if limit_price is not None else round(random.uniform(0.8, 1.2), 2)

        raw = {
            "id": broker_order_id,
            "status": "FILLED",
            "symbol": symbol,
            "option_symbol": contract,
            "side": side,
            "quantity": int(qty),
            "avg_fill_price": float(fill_price),
            "exec_quantity": int(qty),
        }
        self._orders[broker_order_id] = raw

        return BrokerOrderResponse(
            broker_order_id=broker_order_id,
            status="FILLED",
            filled_qty=int(qty),
            avg_fill_price=float(fill_price),
            error=None,
            raw=raw,
        )

    def close_position(self, position_id: str) -> BrokerOrderResponse:
        broker_order_id = f"SIM-CLOSE-{random.randint(100000,999999)}"
        raw = {"id": broker_order_id, "status": "FILLED", "position_id": position_id}
        self._orders[broker_order_id] = raw
        return BrokerOrderResponse(
            broker_order_id=broker_order_id,
            status="FILLED",
            filled_qty=0,
            avg_fill_price=0.0,
            error=None,
            raw=raw,
        )
