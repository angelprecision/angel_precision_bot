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
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {cfg.access_token}",
            "Accept": "application/json",
        })

    # -------------------------
    # HTTP helpers — rate-limit aware
    # -------------------------
    # Tradier published rate limits (per their docs):
    #   Market-data endpoints   (quotes, history, options chains): 120 req/min
    #   Trading endpoints       (orders, account, positions):       60 req/min
    # Tradier returns these response headers we honor:
    #   X-Ratelimit-Available     remaining tokens
    #   X-Ratelimit-Used          tokens consumed in current window
    #   X-Ratelimit-Expiry        epoch when bucket resets (seconds)
    # We log when below TRADIER_RATELIMIT_WARN_THRESHOLD (default 10) and
    # back off proactively when at or below TRADIER_RATELIMIT_BLOCK_THRESHOLD
    # (default 2). On 429 we honor Retry-After and exponential backoff up to
    # TRADIER_RATELIMIT_MAX_RETRIES (default 3).

    def _record_ratelimit_headers(self, path: str, resp) -> None:
        """Log Tradier rate-limit headers when remaining is low."""
        try:
            avail_raw = resp.headers.get("X-Ratelimit-Available")
            used_raw  = resp.headers.get("X-Ratelimit-Used")
            exp_raw   = resp.headers.get("X-Ratelimit-Expiry")
            if avail_raw is None:
                return
            avail = int(avail_raw)
            warn_thr = int(os.getenv("TRADIER_RATELIMIT_WARN_THRESHOLD", "10"))
            if avail <= warn_thr:
                log.warning(
                    "TRADIER_RATELIMIT_LOW path=%s available=%s used=%s expiry=%s",
                    path, avail_raw, used_raw, exp_raw,
                )
        except Exception:
            # Header parsing failure is non-fatal — never block a request on this
            pass

    def _request_with_retry(self, method: str, path: str,
                             *, params=None, data=None) -> "requests.Response":
        """
        Send a request honoring Tradier rate limits.

        On 429:
          - Read Retry-After header (default 2s)
          - Sleep + retry up to TRADIER_RATELIMIT_MAX_RETRIES (default 3)
          - Exponential backoff: retry-after * 2^attempt, capped at 30s
          - After max retries, re-raise the HTTPError
        On other 5xx: re-raise immediately (transient — caller decides).
        """
        import time as _t
        url = f"{self.cfg.base_url}{path}"
        max_retries = int(os.getenv("TRADIER_RATELIMIT_MAX_RETRIES", "3"))
        max_sleep   = float(os.getenv("TRADIER_RATELIMIT_MAX_SLEEP_SEC", "30"))

        attempt = 0
        while True:
            if method == "GET":
                resp = self.session.get(url, params=params, timeout=(3.05, 15))
            elif method == "POST":
                resp = self.session.post(url, data=data, timeout=(3.05, 15))
            else:
                raise ValueError(f"unsupported method: {method}")

            self._record_ratelimit_headers(path, resp)

            # 429 — Tradier rate limited
            if resp.status_code == 429:
                if attempt >= max_retries:
                    log.error(
                        "TRADIER_RATELIMIT_EXHAUSTED path=%s method=%s "
                        "retries=%s — re-raising",
                        path, method, attempt,
                    )
                    resp.raise_for_status()  # raises HTTPError(429)
                # Honor Retry-After header if present
                retry_after_hdr = resp.headers.get("Retry-After")
                try:
                    base_sleep = float(retry_after_hdr) if retry_after_hdr else 2.0
                except (TypeError, ValueError):
                    base_sleep = 2.0
                sleep_sec = min(base_sleep * (2 ** attempt), max_sleep)
                log.warning(
                    "TRADIER_RATELIMIT_429 path=%s method=%s attempt=%s "
                    "retry_after=%s sleeping=%.2fs",
                    path, method, attempt + 1, retry_after_hdr, sleep_sec,
                )
                _t.sleep(sleep_sec)
                attempt += 1
                continue

            # Non-429: raise on any other error (4xx/5xx)
            resp.raise_for_status()
            return resp

    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        r = self._request_with_retry("GET", path, params=params)
        return r.json() if r.content else {}

    def _post(self, path: str, data: dict) -> dict:
        r = self._request_with_retry("POST", path, data=data)
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
        j = self._get("/v1/markets/quotes", params={"symbols": symbol, "greeks": "false"})
        q = (j.get("quotes") or {}).get("quote")

        # Normalize list vs single
        if isinstance(q, list):
            q = q[0] if q else None

        if not isinstance(q, dict):
            return {}

        # Ensure bid/ask/last are numeric if present
        bid = _to_float(q.get("bid"))
        ask = _to_float(q.get("ask"))
        last = _to_float(q.get("last"))

        out = dict(q)
        out["bid"] = bid
        out["ask"] = ask
        out["last"] = last
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
        j = self._get("/v1/markets/options/expirations", params={
            "symbol": symbol,
            "includeAllRoots": "true",
            "strikes": "false",
        })

        dates = (j.get("expirations") or {}).get("date") or []
        if not isinstance(dates, list):
            dates = [dates] if dates else []
        return dates

    def get_option_chain(self, symbol: str, expiration: str) -> List[Dict[str, Any]]:
        j = self._get("/v1/markets/options/chains", params={
            "symbol": symbol,
            "expiration": expiration,
            "greeks": "false",
        })

        opts = (j.get("options") or {}).get("option")
        if not opts:
            return []

        if not isinstance(opts, list):
            opts = [opts]
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

