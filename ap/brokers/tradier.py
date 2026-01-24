# ap/brokers/tradier.py
import requests
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from ap.broker import BrokerAdapter, BrokerOrderResponse

@dataclass
class TradierConfig:
    base_url: str
    access_token: str
    account_id: str

class TradierBroker(BrokerAdapter):
    def __init__(self, cfg: TradierConfig):
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {cfg.access_token}",
            "Accept": "application/json"
        })

    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        url = f"{self.cfg.base_url}{path}"
        # connect timeout, read timeout
        r = self.session.get(url, params=params, timeout=(3.05, 15))
        r.raise_for_status()
        return r.json() if r.content else {}

    def _post(self, path: str, data: dict) -> dict:
        url = f"{self.cfg.base_url}{path}"
        r = self.session.post(url, data=data, timeout=(3.05, 15))
        r.raise_for_status()
        return r.json() if r.content else {}

    def get_account_equity(self) -> float:
        j = self._get(f"/v1/accounts/{self.cfg.account_id}/balances")
        bal = j.get("balances") or {}
        for k in ("total_equity", "equity", "total_cash", "cash"):
            v = bal.get(k)
            if v is not None:
                return float(v)
        raise RuntimeError(f"Unexpected balances payload: {j}")

    def get_quote(self, symbol: str) -> Dict[str, Any]:
        j = self._get("/v1/markets/quotes", params={"symbols": symbol, "greeks": "false"})
        q = (j.get("quotes") or {}).get("quote")
        if isinstance(q, list):
            q = q[0]
        if not q:
            raise RuntimeError(f"No quote for {symbol}: {j}")
        return q

    def get_option_chain(self, symbol: str, expiration: str) -> List[Dict[str, Any]]:
        j = self._get("/v1/markets/options/chains", params={
            "symbol": symbol,
            "expiration": expiration,
            "greeks": "false"
        })
        opts = (j.get("options") or {}).get("option")
        if not opts:
            return []
        return opts if isinstance(opts, list) else [opts]

    def get_option_expirations(self, symbol: str) -> List[str]:
        j = self._get("/v1/markets/options/expirations", params={
            "symbol": symbol,
            "includeAllRoots": "true",
            "strikes": "false"
        })
        dates = (j.get("expirations") or {}).get("date") or []
        return dates if isinstance(dates, list) else [dates]

    def place_order(
        self,
        symbol: str,
        contract: str,
        qty: int,
        limit_price: Optional[float],
        side: str = "buy_to_open",   # ✅ NEW: default keeps old behavior
    ) -> BrokerOrderResponse:
        """
        side values Tradier accepts for options:
          buy_to_open, buy_to_close, sell_to_open, sell_to_close
        """
        side = (side or "buy_to_open").lower().strip()

        data = {
            "class": "option",
            "symbol": symbol,
            "option_symbol": contract,
            "side": side,
            "quantity": str(qty),
            "type": "limit" if limit_price is not None else "market",
            "duration": "day",
        }
        if limit_price is not None:
            data["price"] = str(limit_price)

        j = self._post(f"/v1/accounts/{self.cfg.account_id}/orders", data=data)

        order = j.get("order") or {}
        oid = str(order.get("id") or "")
        status = "ACK" if oid else "REJECTED"

        return BrokerOrderResponse(
            broker_order_id=oid or "N/A",
            status=status,
            filled_qty=0,
            avg_fill_price=0.0,
            error=None if oid else str(j)
        )

    def get_order(self, order_id: str) -> Dict[str, Any]:
        j = self._get(f"/v1/accounts/{self.cfg.account_id}/orders/{order_id}")
        return j.get("order") or j

    def close_position(self, position_id: str) -> BrokerOrderResponse:
        return BrokerOrderResponse(
            broker_order_id="N/A",
            status="REJECTED",
            filled_qty=0,
            avg_fill_price=0.0,
            error="close_position not implemented (use exit orders)"
        )

