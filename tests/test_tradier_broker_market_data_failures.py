from types import SimpleNamespace

import pytest
import requests

from ap.brokers.tradier import TradierBroker, TradierConfig, TradierMarketDataError


class FakeResponse:
    def __init__(self, status_code=200, payload=None, content=True):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.content = b"{}" if content else b""

    def raise_for_status(self):
        if self.status_code >= 400:
            err = requests.exceptions.HTTPError(f"HTTP {self.status_code}")
            err.response = self
            raise err

    def json(self):
        return self._payload


def _broker(payload=None, *, status_code=200, exc=None):
    b = TradierBroker(TradierConfig(
        base_url="https://api.tradier.com",
        access_token="redacted-test-token",
        account_id="acct",
    ))

    def fake_get(url, params=None, timeout=None):
        if exc is not None:
            raise exc
        return FakeResponse(status_code=status_code, payload=payload)

    b.session = SimpleNamespace(get=fake_get)
    return b


def test_get_option_chain_records_chain_empty():
    b = _broker({"options": {}})

    assert b.get_option_chain("AAPL", "2026-07-17") == []

    meta = b.get_last_market_data_error()
    assert meta["reason_code"] == "CHAIN_EMPTY"
    assert meta["endpoint"] == "/v1/markets/options/chains"
    assert meta["symbol"] == "AAPL"
    assert meta["expiration"] == "2026-07-17"
    assert meta["retryable"] is False
    assert "redacted-test-token" not in str(meta)


def test_get_option_chain_raises_structured_timeout():
    b = _broker(exc=requests.exceptions.Timeout("timed out"))

    with pytest.raises(TradierMarketDataError) as err:
        b.get_option_chain("AAPL", "2026-07-17")

    assert err.value.reason_code == "CHAIN_FETCH_TIMEOUT"
    assert err.value.retryable is True
    assert b.get_last_market_data_error()["reason_code"] == "CHAIN_FETCH_TIMEOUT"


def test_get_option_chain_raises_structured_429():
    b = _broker(status_code=429, payload={"error": "rate limited"})

    with pytest.raises(TradierMarketDataError) as err:
        b.get_option_chain("AAPL", "2026-07-17")

    assert err.value.reason_code == "CHAIN_FETCH_RATE_LIMITED"
    assert err.value.status_code == 429
    assert err.value.retryable is True


def test_get_option_chain_raises_structured_auth_failure():
    b = _broker(status_code=401, payload={"error": "unauthorized"})

    with pytest.raises(TradierMarketDataError) as err:
        b.get_option_chain("AAPL", "2026-07-17")

    assert err.value.reason_code == "CHAIN_FETCH_AUTH_FAILED"
    assert err.value.status_code == 401
    assert err.value.retryable is False


def test_get_option_chain_normalizes_single_option_to_list_and_clears_stale_error():
    b = _broker({
        "options": {
            "option": {
                "symbol": "AAPL260717C00200000",
                "bid": 1.0,
                "ask": 1.1,
            }
        }
    })

    rows = b.get_option_chain("AAPL", "2026-07-17")

    assert isinstance(rows, list)
    assert rows[0]["symbol"] == "AAPL260717C00200000"
    assert b.get_last_market_data_error() == {}


def test_get_option_expirations_records_empty():
    b = _broker({"expirations": {}})

    assert b.get_option_expirations("AAPL") == []

    meta = b.get_last_market_data_error()
    assert meta["reason_code"] == "EXPIRATIONS_EMPTY"
    assert meta["endpoint"] == "/v1/markets/options/expirations"
    assert meta["symbol"] == "AAPL"


def test_get_option_expirations_single_date_normalizes_to_list():
    b = _broker({"expirations": {"date": "2026-07-17"}})

    assert b.get_option_expirations("AAPL") == ["2026-07-17"]
    assert b.get_last_market_data_error() == {}


def test_get_quote_records_empty_quote_without_raising():
    b = _broker({"quotes": {}})

    assert b.get_quote("AAPL") == {}

    meta = b.get_last_market_data_error()
    assert meta["reason_code"] == "QUOTE_EMPTY"
    assert meta["endpoint"] == "/v1/markets/quotes"


def test_get_quote_records_zero_bid_ask_without_rejecting():
    b = _broker({"quotes": {"quote": {"symbol": "AAPL", "bid": 0, "ask": 0, "last": 1.23}}})

    quote = b.get_quote("AAPL")

    assert quote["bid"] == 0.0
    assert quote["ask"] == 0.0
    assert quote["last"] == 1.23
    assert b.get_last_market_data_error()["reason_code"] == "QUOTE_ZERO_BID_ASK"
