# ap/brokers/tradier.py - PRODUCTION SAFE (CONSISTENT CONTRACTS)

from __future__ import annotations

import os
import requests
from dataclasses import dataclass
from typing import Optional, Dict, Any, List

from ap.broker import BrokerAdapter, BrokerOrderResponse, normalize_status
from ap.logger import get_logger

log = get_logger("ap.brokers.tradier")


@dataclass(frozen=True)
class TradierConfig:
    base_url: str
    access_token: str
    account_id: str


class TradierMarketDataError(RuntimeError):
    """Structured market-data failure raised by TradierBroker data endpoints."""

    def __init__(self, reason_code: str, message: str, *, endpoint: str = "", symbol: str = "", expiration: str = "", status_code: int | None = None, base_url: str = "", retryable: bool = False, payload_shape: dict | None = None):
        super().__init__(message)
        self.reason_code = str(reason_code or "MARKET_DATA_ERROR")
        self.endpoint = str(endpoint or "")
        self.symbol = str(symbol or "")
        self.expiration = str(expiration or "")
        self.status_code = status_code
        self.base_url = str(base_url or "")
        self.retryable = bool(retryable)
        self.payload_shape = dict(payload_shape or {})

    def to_meta(self) -> dict:
        meta = {"reason_code": self.reason_code, "endpoint": self.endpoint, "symbol": self.symbol, "expiration": self.expiration, "status_code": self.status_code, "base_url": self.base_url, "retryable": self.retryable}
        if self.payload_shape:
            meta["payload_shape"] = dict(self.payload_shape)
        return meta


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


class TradierBroker(BrokerAdapter):
    def __init__(self, cfg: TradierConfig):
        self.cfg = cfg
        self._last_market_data_error: dict[str, Any] = {}
        self._last_market_data_call: dict[str, Any] = {}
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {cfg.access_token}",
            "Accept": "application/json",
        })

    @property
    def base_url(self) -> str:
        return self.cfg.base_url

    def get_last_market_data_error(self) -> dict[str, Any]:
        try:
            return dict(self._last_market_data_error or {})
        except Exception:
            return {}

    def get_last_market_data_call(self) -> dict[str, Any]:
        try:
            return dict(self._last_market_data_call or {})
        except Exception:
            return {}

    def _record_market_data_call(self, *, endpoint: str, symbol: str = "", expiration: str = "") -> None:
        self._last_market_data_call = {"endpoint": endpoint, "symbol": symbol, "expiration": expiration, "base_url": self.cfg.base_url}

    def _clear_market_data_error(self) -> None:
        self._last_market_data_error = {}

    def _record_market_data_error(self, *, reason_code: str, endpoint: str, symbol: str = "", expiration: str = "", status_code: int | None = None, retryable: bool = False, payload_shape: dict | None = None) -> dict[str, Any]:
        meta = {"reason_code": str(reason_code or "MARKET_DATA_ERROR"), "endpoint": endpoint, "symbol": symbol, "expiration": expiration, "status_code": status_code, "base_url": self.cfg.base_url, "retryable": bool(retryable)}
        if payload_shape:
            meta["payload_shape"] = dict(payload_shape)
        self._last_market_data_error = meta
        return meta

    def _classify_market_data_exception(self, prefix: str, exc: Exception) -> tuple[str, int | None, bool]:
        status = getattr(getattr(exc, "response", None), "status_code", None)
        if isinstance(exc, requests.exceptions.Timeout):
            return f"{prefix}_FETCH_TIMEOUT", status, True
        if isinstance(exc, requests.exceptions.ConnectionError):
            return f"{prefix}_FETCH_NETWORK_ERROR", status, True
        if isinstance(exc, requests.exceptions.HTTPError):
            if status == 429:
                return f"{prefix}_FETCH_RATE_LIMITED", status, True
            if status in (401, 403):
                return f"{prefix}_FETCH_AUTH_FAILED", status, False
            if status is not None and 500 <= int(status) < 600:
                return f"{prefix}_FETCH_SERVER_ERROR", status, True
            return f"{prefix}_FETCH_HTTP_ERROR", status, False
        return f"{prefix}_PAYLOAD_MALFORMED", status, True

    def _raise_market_data_error(self, prefix: str, exc: Exception, *, endpoint: str, symbol: str = "", expiration: str = "") -> None:
        reason, status, retryable = self._classify_market_data_exception(prefix, exc)
        self._record_market_data_error(reason_code=reason, endpoint=endpoint, symbol=symbol, expiration=expiration, status_code=status, retryable=retryable)
        raise TradierMarketDataError(reason, str(exc), endpoint=endpoint, symbol=symbol, expiration=expiration, status_code=status, base_url=self.cfg.base_url, retryable=retryable) from exc

    # -------------------------
    # HTTP helpers
    # -------------------------
    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        url = f"{self.cfg.base_url}{path}"
        r = self.session.get(url, params=params, timeout=(3.05, 15))
        r.raise_for_status()
        return r.json() if r.content else {}

    def _post(self, path: str, data: dict) -> dict:
        url = f"{self.cfg.base_url}{path}"
        r = self.session.post(url, data=data, timeout=(3.05, 15))
        r.raise_for_status()
        return r.json() if r.content else {}

    # -------------------------
    # Account
    # -------------------------
    def get_account_equity(self) -> float:
        """
        Tradier balances endpoint has multiple fields.
        Prefer total_equity if present.
        """
        j = self._get(f"/v1/accounts/{self.cfg.account_id}/balances")
        bal = j.get("balances") or {}

        for k in ("total_equity", "equity", "total_cash", "cash"):
            v = bal.get(k)
            if v is not None:
                equity = float(v)
                return equity

        raise RuntimeError(f"Unexpected balances payload (no equity field): {j}")

    # -------------------------
    # Quotes
    # -------------------------
    def get_quote(self, symbol: str) -> Dict[str, Any]:
        """
        Returns a dict with bid/ask/last if available.
        contract_pricing.py will handle spread sanity etc.
        """
        endpoint = "/v1/markets/quotes"
        self._record_market_data_call(endpoint=endpoint, symbol=symbol)
        try:
            j = self._get(endpoint, params={"symbols": symbol, "greeks": "false"})
        except Exception as e:
            self._raise_market_data_error("QUOTE", e, endpoint=endpoint, symbol=symbol)
        q = (j.get("quotes") or {}).get("quote")

        # Normalize list vs single
        if isinstance(q, list):
            q = q[0] if q else None

        if not isinstance(q, dict):
            self._record_market_data_error(reason_code="QUOTE_EMPTY", endpoint=endpoint, symbol=symbol, retryable=True, payload_shape={"has_quotes": bool(j.get("quotes")), "quote_type": type(q).__name__})
            return {}

        # Ensure bid/ask/last are numeric if present
        bid = _to_float(q.get("bid"))
        ask = _to_float(q.get("ask"))
        last = _to_float(q.get("last"))

        out = dict(q)
        out["bid"] = bid
        out["ask"] = ask
        out["last"] = last
        if bid is None or ask is None:
            self._record_market_data_error(reason_code="QUOTE_MISSING_BID_ASK", endpoint=endpoint, symbol=symbol, retryable=True)
        elif bid <= 0 or ask <= 0:
            self._record_market_data_error(reason_code="QUOTE_ZERO_BID_ASK", endpoint=endpoint, symbol=symbol, retryable=True)
        elif ask < bid:
            self._record_market_data_error(reason_code="QUOTE_INVERTED_BID_ASK", endpoint=endpoint, symbol=symbol, retryable=True)
        else:
            self._clear_market_data_error()
        return out

    # -------------------------
    # Options
    # -------------------------
    def get_prior_day_levels(self, symbol: str) -> dict:
        """Fetch prior trading day OHLC from Tradier history endpoint.
        Returns {"prior_day_high": float, "prior_day_low": float, "prior_day_close": float}
        or empty dict on failure.

        Used by the overnight reeval loop at 9:15 AM ET to validate daily setups
        before arming the entry watcher.
        """
        from datetime import datetime, timedelta, timezone
        try:
            # Fetch 5 days of daily history — take the most recent completed day
            # (not today, since today's bar is still open)
            j = self._get("/v1/markets/history", params={
                "symbol": symbol,
                "interval": "daily",
                "start": (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d"),
                "end": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            })
            days = (j.get("history") or {}).get("day") or []
            if not isinstance(days, list):
                days = [days] if days else []

            # Sort by date descending, skip today's partial bar if present
            today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            completed = [d for d in days if isinstance(d, dict) and d.get("date", "") < today_str]
            completed.sort(key=lambda d: d.get("date", ""), reverse=True)

            if not completed:
                return {}

            prior = completed[0]
            return {
                "prior_day_high":  float(prior.get("high")  or 0) or None,
                "prior_day_low":   float(prior.get("low")   or 0) or None,
                "prior_day_close": float(prior.get("close") or 0) or None,
                "prior_day_date":  prior.get("date"),
            }
        except Exception as e:
            import logging
            logging.getLogger("ap.broker").warning("[%s] get_prior_day_levels failed: %s", symbol, e)
            return {}

    def get_option_expirations(self, symbol: str) -> List[str]:
        endpoint = "/v1/markets/options/expirations"
        self._record_market_data_call(endpoint=endpoint, symbol=symbol)
        try:
            j = self._get(endpoint, params={"symbol": symbol, "includeAllRoots": "true", "strikes": "false"})
        except Exception as e:
            self._raise_market_data_error("EXPIRATIONS", e, endpoint=endpoint, symbol=symbol)
        expirations = j.get("expirations") or {}
        if not isinstance(expirations, dict):
            payload_shape = {"expirations_type": type(expirations).__name__, "has_expirations": bool(j.get("expirations"))}
            self._record_market_data_error(reason_code="EXPIRATIONS_PAYLOAD_MALFORMED", endpoint=endpoint, symbol=symbol, retryable=True, payload_shape=payload_shape)
            raise TradierMarketDataError("EXPIRATIONS_PAYLOAD_MALFORMED", "Tradier expirations payload is malformed", endpoint=endpoint, symbol=symbol, base_url=self.cfg.base_url, retryable=True, payload_shape=payload_shape)
        dates = expirations.get("date") or []
        if not isinstance(dates, list):
            dates = [dates] if dates else []
        if not dates:
            self._record_market_data_error(reason_code="EXPIRATIONS_EMPTY", endpoint=endpoint, symbol=symbol, retryable=False, payload_shape={"has_expirations": bool(j.get("expirations")), "has_date": bool(expirations.get("date"))})
            return []
        self._clear_market_data_error()
        return dates

    def get_option_chain(self, symbol: str, expiration: str) -> List[Dict[str, Any]]:
        endpoint = "/v1/markets/options/chains"
        self._record_market_data_call(endpoint=endpoint, symbol=symbol, expiration=expiration)
        try:
            j = self._get(endpoint, params={"symbol": symbol, "expiration": expiration, "greeks": "false"})
        except Exception as e:
            self._raise_market_data_error("CHAIN", e, endpoint=endpoint, symbol=symbol, expiration=expiration)
        options = j.get("options") or {}
        if not isinstance(options, dict):
            payload_shape = {"options_type": type(options).__name__, "has_options": bool(j.get("options"))}
            self._record_market_data_error(reason_code="CHAIN_PAYLOAD_MALFORMED", endpoint=endpoint, symbol=symbol, expiration=expiration, retryable=True, payload_shape=payload_shape)
            raise TradierMarketDataError("CHAIN_PAYLOAD_MALFORMED", "Tradier chain payload is malformed", endpoint=endpoint, symbol=symbol, expiration=expiration, base_url=self.cfg.base_url, retryable=True, payload_shape=payload_shape)
        opts = options.get("option")
        if not opts:
            self._record_market_data_error(reason_code="CHAIN_EMPTY", endpoint=endpoint, symbol=symbol, expiration=expiration, retryable=False, payload_shape={"has_options": bool(j.get("options")), "has_option": bool(options.get("option"))})
            return []

        if not isinstance(opts, list):
            opts = [opts]
        self._clear_market_data_error()
        return opts

    # -------------------------
    # Orders
    # -------------------------
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
        """
        Places an option order.
        Returns BrokerOrderResponse (ACK/REJECTED with broker order id).

        `tag` is forwarded to Tradier for client-side idempotency. On retry,
        the caller can query orders by tag to detect whether a prior attempt
        landed before the response was lost (avoiding double-submit).
        """
        side = (side or "buy_to_open").lower().strip()

        data = {
            "class": "option",
            "symbol": symbol,
            "option_symbol": contract,
            "side": side,
            "quantity": str(int(qty)),
            "type": "limit" if limit_price is not None else "market",
            "duration": "day",
        }
        if limit_price is not None:
            data["price"] = f"{float(limit_price):.2f}"
        if tag:
            # Tradier accepts tag (max 32 chars). Trim defensively.
            data["tag"] = str(tag)[:32]

        if os.getenv("TRADIER_DEBUG_ORDERS", "0") == "1":
            log.info(
                f"TRADIER_ORDER_PAYLOAD | symbol={symbol} contract={contract} "
                f"side={side} qty={qty} price={data.get('price', 'market')} "
                f"full_payload={data}"
            )

        try:
            j = self._post(f"/v1/accounts/{self.cfg.account_id}/orders", data=data)

            if os.getenv("TRADIER_DEBUG_ORDERS", "0") == "1":
                log.info(f"TRADIER_ORDER_RESPONSE | {j}")

            # Tradier returns {"order":{"id": "...", "status":"ok"}} or similar
            order = j.get("order") or {}
            oid = str(order.get("id") or "") or "N/A"
            raw_status = order.get("status") or j.get("status") or ""

            status = normalize_status(raw_status)
            # When Tradier returns OK/ACCEPTED/PENDING, treat as ACK
            if status in ("NEW", "UNKNOWN"):
                s = str(raw_status).upper()
                if s in ("OK", "ACCEPTED", "PENDING"):
                    status = "ACK"

            # If no id, treat as rejected
            if oid == "N/A":
                log.error(f"TRADIER_ORDER_REJECTED | no order id in response: {j}")
                return BrokerOrderResponse(
                    broker_order_id="N/A",
                    status="REJECTED",
                    error=f"no_order_id:{j}",
                    raw=j,
                )

            # For submissions, we expect ACK (not FILLED immediately)
            if status == "FILLED":
                status = "ACK"

            log.info(
                f"TRADIER_ORDER_ACK | symbol={symbol} contract={contract} "
                f"side={side} broker_order_id={oid} status={status}"
            )

            return BrokerOrderResponse(
                broker_order_id=oid,
                status=status if status != "UNKNOWN" else "ACK",
                filled_qty=0,
                avg_fill_price=0.0,
                error=None,
                raw=order if isinstance(order, dict) else j,
            )

        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.HTTPError) as e:
            # HIGH-006: transient errors — re-raise so caller can retry
            status_code = getattr(getattr(e, 'response', None), 'status_code', None)
            if isinstance(e, requests.exceptions.HTTPError) and status_code and 400 <= status_code < 500 and status_code != 429:
                # Permanent client error (400, 401, 403, etc.) — treat as REJECTED
                log.error(f"place_order REJECTED (HTTP {status_code}) | symbol={symbol} contract={contract} side={side} error={e}")
                return BrokerOrderResponse(
                    broker_order_id="N/A",
                    status="REJECTED",
                    error=str(e),
                    raw=None,
                )
            log.warning(f"place_order TRANSIENT error | symbol={symbol} contract={contract} side={side} error={e}")
            raise
        except Exception as e:
            log.error(f"place_order EXCEPTION | symbol={symbol} contract={contract} side={side} error={e}")
            return BrokerOrderResponse(
                broker_order_id="N/A",
                status="REJECTED",
                error=str(e),
                raw=None,
            )

    def get_order(self, order_id: str) -> Dict[str, Any]:
        """
        Fetch order details. Return a dict with keys used by fill_monitor:
          - status
          - exec_quantity
          - avg_fill_price
          - price (fallback)
        """
        try:
            j = self._get(f"/v1/accounts/{self.cfg.account_id}/orders/{order_id}")
            order = j.get("order") if isinstance(j, dict) else None
            if isinstance(order, dict):
                return order
            return j if isinstance(j, dict) else {"status": "UNKNOWN", "raw": j}
        except Exception as e:
            return {"status": "ERROR", "reason": str(e)}

    def close_position(self, position_id: str) -> BrokerOrderResponse:
        # Not used in current architecture
        return BrokerOrderResponse(
            broker_order_id="N/A",
            status="REJECTED",
            error="close_position not implemented (use EXIT orders)",
            raw=None,
        )



    def cancel_order(self, broker_order_id: str) -> dict:
        """Cancel a live order via Tradier DELETE + confirm with re-query."""
        try:
            resp = self.session.delete(
                f"{self.cfg.base_url}/v1/accounts/{self.cfg.account_id}/orders/{broker_order_id}",
                headers={"Accept": "application/json"},
                timeout=(3.05, 10),
            )
            raw = resp.json() if resp.content else {}
            status = str((raw.get("order") or raw).get("status", "")).lower()
            ok = resp.status_code in (200, 204) or status in ("ok", "canceled", "cancelled")
            confirmed_status = status
            try:
                confirmed = self.get_order(broker_order_id)
                confirmed_status = confirmed.get("status", status)
            except Exception:
                pass
            log.info("TRADIER_CANCEL | order=%s http=%d confirmed=%s",
                     broker_order_id, resp.status_code, confirmed_status)
            return {"ok": ok, "status": confirmed_status,
                    "broker_order_id": broker_order_id, "raw": raw,
                    "error": None if ok else f"cancel_http_{resp.status_code}"}
        except Exception as e:
            log.error("TRADIER_CANCEL_FAILED | order=%s error=%s", broker_order_id, e)
            return {"ok": False, "status": "unknown",
                    "broker_order_id": broker_order_id, "raw": {}, "error": str(e)}

    def list_positions(self) -> list:
        """
        Return open positions from Tradier account.
        Returns list of dicts with: symbol, quantity, cost_basis, side
        Returns [] if no positions or on error.
        """
        try:
            resp = self._get(f"/v1/accounts/{self.cfg.account_id}/positions")
            positions = resp.get("positions", {})
            if not positions or positions == "null":
                return []
            pos_list = positions.get("position", [])
            if isinstance(pos_list, dict):
                pos_list = [pos_list]
            result = []
            for p in pos_list:
                result.append({
                    "symbol":     p.get("symbol", ""),
                    "quantity":   float(p.get("quantity", 0)),
                    "cost_basis": float(p.get("cost_basis", 0)),
                    "side":       (lambda sym: (
                        "CALL" if (len(sym) >= 15 and sym[-9] == "C") else
                        "PUT"  if (len(sym) >= 15 and sym[-9] == "P") else
                        "CALL" if "C" in sym else "PUT"
                    ))(str(p.get("symbol", ""))),
                    "raw":        p,
                })
            return result
        except Exception as e:
            log.error("TRADIER_LIST_POSITIONS_FAILED | error=%s", e)
            return []
