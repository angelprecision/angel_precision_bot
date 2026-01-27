# ap/brokers/tradier.py - COMPLETE FIXED VERSION
import requests
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from ap.broker import BrokerAdapter, BrokerOrderResponse
from ap.logger import get_logger

log = get_logger("ap.brokers.tradier")

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
        try:
            r = self.session.get(url, params=params, timeout=(3.05, 15))
            r.raise_for_status()
            return r.json() if r.content else {}
        except Exception as e:
            log.error(f"GET {path} failed: {e}")
            raise

    def _post(self, path: str, data: dict) -> dict:
        url = f"{self.cfg.base_url}{path}"
        try:
            r = self.session.post(url, data=data, timeout=(3.05, 15))
            r.raise_for_status()
            return r.json() if r.content else {}
        except Exception as e:
            log.error(f"POST {path} failed: {e}")
            raise

    def get_account_equity(self) -> float:
        """Get current account equity"""
        try:
            j = self._get(f"/v1/accounts/{self.cfg.account_id}/balances")
            bal = j.get("balances") or {}
            
            # Try multiple balance fields in order of preference
            for k in ("total_equity", "equity", "option_buying_power", "total_cash", "cash"):
                v = bal.get(k)
                if v is not None:
                    equity = float(v)
                    log.debug(f"Got equity from '{k}': ${equity:,.2f}")
                    return equity
            
            log.error(f"No equity field found in balances: {bal}")
            raise RuntimeError(f"Unexpected balances payload: {j}")
            
        except Exception as e:
            log.error(f"Failed to get account equity: {e}")
            raise

    def get_quote(self, symbol: str) -> Dict[str, Any]:
        """
        Get quote for a symbol (stock or option).
        Returns dict with: bid, ask, last, etc.
        """
        try:
            j = self._get("/v1/markets/quotes", params={
                "symbols": symbol,
                "greeks": "false"
            })
            
            quotes = j.get("quotes") or {}
            q = quotes.get("quote")
            
            # Handle single quote or list
            if isinstance(q, list):
                q = q[0] if q else None
            
            if not q:
                log.warning(f"No quote found for {symbol}")
                return {}
            
            return q
            
        except Exception as e:
            log.error(f"Failed to get quote for {symbol}: {e}")
            raise

    def get_contract_price(self, contract_symbol: str) -> float:
        """
        Get current price for an option contract.
        This is the method execution.py needs!
        
        Returns: Premium per share (e.g., 1.25 = $125/contract)
        """
        try:
            quote = self.get_quote(contract_symbol)
            
            if not quote:
                log.warning(f"Empty quote for {contract_symbol}")
                return 0.0
            
            # Try to get a valid price
            bid = float(quote.get("bid") or 0)
            ask = float(quote.get("ask") or 0)
            last = float(quote.get("last") or 0)
            
            # Prefer mid-market for entry pricing
            if bid > 0 and ask > 0:
                price = (bid + ask) / 2.0
                log.debug(f"{contract_symbol}: bid={bid}, ask={ask}, mid={price}")
                return price
            
            # Fallback to last
            if last > 0:
                log.debug(f"{contract_symbol}: using last={last}")
                return last
            
            # If ask is available, use it (worst case for buying)
            if ask > 0:
                log.debug(f"{contract_symbol}: using ask={ask}")
                return ask
            
            log.warning(f"No valid price for {contract_symbol}: {quote}")
            return 0.0
            
        except Exception as e:
            log.error(f"Failed to get contract price for {contract_symbol}: {e}")
            return 0.0

    def get_option_chain(self, symbol: str, expiration: str) -> List[Dict[str, Any]]:
        """
        Get option chain for a symbol and expiration.
        Returns list of option contracts.
        """
        try:
            j = self._get("/v1/markets/options/chains", params={
                "symbol": symbol,
                "expiration": expiration,
                "greeks": "false"
            })
            
            opts = (j.get("options") or {}).get("option")
            
            if not opts:
                log.warning(f"No options in chain for {symbol} {expiration}")
                return []
            
            # Normalize to list
            if not isinstance(opts, list):
                opts = [opts]
            
            log.debug(f"Got {len(opts)} options for {symbol} {expiration}")
            return opts
            
        except Exception as e:
            log.error(f"Failed to get option chain for {symbol} {expiration}: {e}")
            return []

    def get_option_expirations(self, symbol: str) -> List[str]:
        """
        Get available expiration dates for a symbol.
        Returns list of dates in YYYY-MM-DD format.
        """
        try:
            j = self._get("/v1/markets/options/expirations", params={
                "symbol": symbol,
                "includeAllRoots": "true",
                "strikes": "false"
            })
            
            dates = (j.get("expirations") or {}).get("date") or []
            
            # Normalize to list
            if not isinstance(dates, list):
                dates = [dates] if dates else []
            
            log.debug(f"Got {len(dates)} expirations for {symbol}")
            return dates
            
        except Exception as e:
            log.error(f"Failed to get expirations for {symbol}: {e}")
            return []

    def place_order(
        self,
        symbol: str,
        contract: str,
        qty: int,
        limit_price: Optional[float],
        side: str = "buy_to_open",
    ) -> BrokerOrderResponse:
        """
        Place an option order.
        
        Args:
            symbol: Underlying symbol (e.g., "SPY")
            contract: Option symbol (e.g., "SPY250131C00580000")
            qty: Number of contracts
            limit_price: Limit price per share (None for market order)
            side: buy_to_open | buy_to_close | sell_to_open | sell_to_close
        
        Returns:
            BrokerOrderResponse with order details
        """
        try:
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
                data["price"] = f"{limit_price:.2f}"
            
            log.info(f"Placing order: {contract} {side} x{qty} @ ${limit_price}")
            
            j = self._post(f"/v1/accounts/{self.cfg.account_id}/orders", data=data)
            
            order = j.get("order") or {}
            oid = str(order.get("id") or "")
            status_str = str(order.get("status") or "").upper()
            
            # Map Tradier statuses to our statuses
            if oid and status_str in ("OK", "ACCEPTED", "PENDING"):
                status = "ACK"
            elif status_str == "REJECTED":
                status = "REJECTED"
            else:
                status = "ACK" if oid else "REJECTED"
            
            log.info(f"Order response: id={oid}, status={status}")
            
            return BrokerOrderResponse(
                broker_order_id=oid or "N/A",
                status=status,
                filled_qty=0,
                avg_fill_price=0.0,
                error=None if oid else str(j),
                raw=order
            )
            
        except Exception as e:
            log.error(f"Failed to place order: {e}")
            return BrokerOrderResponse(
                broker_order_id="N/A",
                status="REJECTED",
                filled_qty=0,
                avg_fill_price=0.0,
                error=str(e),
                raw=None
            )

    def get_order(self, order_id: str) -> Dict[str, Any]:
        """Get order details by ID"""
        try:
            j = self._get(f"/v1/accounts/{self.cfg.account_id}/orders/{order_id}")
            return j.get("order") or j
        except Exception as e:
            log.error(f"Failed to get order {order_id}: {e}")
            return {"error": str(e)}

    def close_position(self, position_id: str) -> BrokerOrderResponse:
        """
        Deprecated: Use exit orders instead.
        This method exists for backward compatibility.
        """
        log.warning("close_position called but not implemented - use exit orders")
        return BrokerOrderResponse(
            broker_order_id="N/A",
            status="REJECTED",
            filled_qty=0,
            avg_fill_price=0.0,
            error="close_position not implemented (use exit orders)"
        )
