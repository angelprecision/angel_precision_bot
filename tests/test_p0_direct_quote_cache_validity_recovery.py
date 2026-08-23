from __future__ import annotations

import pytest

from ap.contract_quote_revalidator import (
    _QUOTE_CACHE,
    _cache_key_for,
    clear_quote_cache,
    direct_quote_is_valid,
    fetch_direct_option_quote,
    fetch_direct_option_quote_with_meta,
)

OCC = "SPY301231C00500000"


class SeqBroker:
    def __init__(self, quotes):
        self.quotes = list(quotes)
        self.calls = 0

    def get_quote(self, symbol):
        quote = self.quotes[min(self.calls, len(self.quotes) - 1)]
        self.calls += 1
        return quote


@pytest.fixture(autouse=True)
def clean_cache():
    clear_quote_cache()
    yield
    clear_quote_cache()


def cached(broker):
    return _QUOTE_CACHE.get(_cache_key_for(broker, OCC))


@pytest.mark.parametrize(
    "invalid",
    [
        {"bid": 0, "ask": 1.5},
        {"bid": 1.0, "ask": 0},
        {"bid": -1.0, "ask": 1.5},
        {"bid": float("nan"), "ask": 1.5},
        {"bid": 1.0, "ask": float("inf")},
        {"bid": True, "ask": 1.5},
        {"bid": "bad", "ask": 1.5},
        {"bid": 2.0, "ask": 1.0},
    ],
)
def test_invalid_observation_is_not_cached(invalid):
    broker = SeqBroker([invalid])
    fetch_direct_option_quote(broker, OCC)
    assert cached(broker) is None
    assert direct_quote_is_valid(invalid) is False


def test_invalid_then_valid_rereads_provider_and_recovers_legacy_path():
    broker = SeqBroker([{"bid": 0, "ask": 1.5}, {"bid": 1.0, "ask": 1.5}])
    fetch_direct_option_quote(broker, OCC)
    recovered = fetch_direct_option_quote(broker, OCC)
    assert broker.calls == 2
    assert recovered["bid"] == 1.0
    assert cached(broker) is not None


def test_invalid_then_valid_rereads_provider_and_recovers_meta_path():
    broker = SeqBroker([
        {"bid": float("nan"), "ask": 1.5},
        {"bid": 1.0, "ask": 1.5},
    ])
    first = fetch_direct_option_quote_with_meta(broker, OCC)
    assert first["ok"] is True
    assert direct_quote_is_valid(first["quote"]) is False
    assert cached(broker) is None
    recovered = fetch_direct_option_quote_with_meta(broker, OCC)
    assert broker.calls == 2
    assert recovered["ok"] is True
    assert recovered["quote"]["bid"] == 1.0
    assert cached(broker) is not None


@pytest.mark.parametrize(
    "fetch",
    [fetch_direct_option_quote, fetch_direct_option_quote_with_meta],
)
def test_valid_first_uses_cache_without_unnecessary_reread(fetch):
    broker = SeqBroker([{"bid": 1.1, "ask": 1.2}])
    fetch(broker, OCC)
    fetch(broker, OCC)
    assert broker.calls == 1
    assert cached(broker) is not None


def test_invalid_forced_refresh_evicts_older_valid_observation():
    broker = SeqBroker([{"bid": 1.1, "ask": 1.2}, {"bid": 0, "ask": 1.2}])
    fetch_direct_option_quote_with_meta(broker, OCC)
    assert cached(broker) is not None
    refreshed = fetch_direct_option_quote_with_meta(broker, OCC, cache_ttl_s=0.0)
    assert direct_quote_is_valid(refreshed["quote"]) is False
    assert broker.calls == 2
    assert cached(broker) is None
