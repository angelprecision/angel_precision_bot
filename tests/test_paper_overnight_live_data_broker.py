import requests

from ap.brokers.tradier import TradierBroker, TradierConfig


class _FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code
        self.content = b"{}"

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


def test_paper_market_data_gets_live_url_and_live_token(monkeypatch):
    monkeypatch.setenv("TRADIER_MARKET_DATA_TOKEN", "live-md-token")
    monkeypatch.setenv("TRADIER_MARKET_DATA_BASE_URL", "https://api.tradier.com")

    calls = []

    def fake_parent_request(self, method, url, **kwargs):
        calls.append({"method": method, "url": url, "headers": kwargs.get("headers") or {}})
        return _FakeResponse({
            "quotes": {
                "quote": {
                    "symbol": "AAPL",
                    "bid": "1.00",
                    "ask": "1.02",
                    "last": "1.01",
                }
            }
        })

    monkeypatch.setattr(requests.Session, "request", fake_parent_request)

    broker = TradierBroker(TradierConfig(
        base_url="https://sandbox.tradier.com",
        access_token="paper-exec-token",
        account_id="paper-acct",
    ))

    quote = broker.get_quote("AAPL")

    assert quote["bid"] == 1.0
    assert calls[0]["url"] == "https://api.tradier.com/v1/markets/quotes"
    assert calls[0]["headers"]["Authorization"] == "Bearer live-md-token"


def test_live_snapshot_url_on_paper_broker_uses_live_market_data_token(monkeypatch):
    monkeypatch.setenv("TRADIER_MARKET_DATA_TOKEN", "live-md-token")

    calls = []

    def fake_parent_request(self, method, url, **kwargs):
        calls.append({"method": method, "url": url, "headers": kwargs.get("headers") or {}})
        return _FakeResponse({"series": {"data": {"item": []}}})

    monkeypatch.setattr(requests.Session, "request", fake_parent_request)

    broker = TradierBroker(TradierConfig(
        base_url="https://sandbox.tradier.com",
        access_token="paper-exec-token",
        account_id="paper-acct",
    ))

    broker.session.get(
        "https://api.tradier.com/v1/markets/timesales",
        params={"symbol": "AAPL"},
        headers={"Accept": "application/json"},
    )

    assert calls[0]["url"] == "https://api.tradier.com/v1/markets/timesales"
    assert calls[0]["headers"]["Authorization"] == "Bearer live-md-token"


def test_paper_order_submit_stays_on_sandbox_execution_token(monkeypatch):
    monkeypatch.setenv("TRADIER_MARKET_DATA_TOKEN", "live-md-token")
    monkeypatch.setenv("TRADIER_MARKET_DATA_BASE_URL", "https://api.tradier.com")

    calls = []

    def fake_parent_request(self, method, url, **kwargs):
        calls.append({
            "method": method,
            "url": url,
            "headers": kwargs.get("headers") or {},
            "data": kwargs.get("data"),
        })
        return _FakeResponse({"order": {"id": "123", "status": "ok"}})

    monkeypatch.setattr(requests.Session, "request", fake_parent_request)

    broker = TradierBroker(TradierConfig(
        base_url="https://sandbox.tradier.com",
        access_token="paper-exec-token",
        account_id="paper-acct",
    ))

    response = broker.place_order(
        symbol="AAPL",
        contract="AAPL260717C00200000",
        qty=1,
        limit_price=1.23,
    )

    assert response.broker_order_id == "123"
    assert calls[0]["url"] == "https://sandbox.tradier.com/v1/accounts/paper-acct/orders"
    assert calls[0]["headers"]["Authorization"] == "Bearer paper-exec-token"
    assert calls[0]["data"]["option_symbol"] == "AAPL260717C00200000"
