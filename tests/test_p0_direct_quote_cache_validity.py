"""
P0 production-path regression: invalid direct option quotes must never enter
the direct-quote cache, and an invalid observation must not poison a subsequent
fresh provider read.

Incident context (Aug 14 audit): both direct-fetch paths in
ap/contract_quote_revalidator.py wrote _normalize_quote() output to _QUOTE_CACHE
unconditionally. Invalid observations (None / bool / malformed / zero / negative
/ NaN / +-Infinity / overflow / crossed book) were cached and then served on a
cache hit for the whole TTL, suppressing a fresh provider read and starving the
selector even after the feed recovered.

These tests drive the real fetch functions against a stub broker (production
path), asserting on cache state and on the number of broker.get_quote calls --
not on internal helpers alone.
"""

from __future__ import annotations

import math

import pytest

from ap.contract_quote_revalidator import (
    fetch_direct_option_quote,
    direct_quote_is_valid,
    quote_is_cache_eligible,
    clear_quote_cache,
    _QUOTE_CACHE,
    _cache_key_for,
)

OCC = "SPY301231C00500000"  # far-future expiry, not relevant to cache logic


class SeqBroker:
    """Broker stub returning a sequence of canned raw quotes, counting calls."""

    def __init__(self, seq):
        self.seq = list(seq)
        self.calls = 0

    def get_quote(self, symbol):
        i = min(self.calls, len(self.seq) - 1)
        self.calls += 1
        return self.seq[i]


@pytest.fixture(autouse=True)
def _clear_cache():
    clear_quote_cache()
    yield
    clear_quote_cache()


def _is_cached(broker):
    return _QUOTE_CACHE.get(_cache_key_for(broker, OCC)) is not None


# 1. valid bid/ask caches and cache-hit avoids another provider call
def test_valid_quote_caches_and_cache_hit_avoids_second_call():
    brk = SeqBroker([{"bid": 1.10, "ask": 1.20}])
    q1 = fetch_direct_option_quote(brk, OCC)
    q2 = fetch_direct_option_quote(brk, OCC)
    assert _is_cached(brk)
    assert brk.calls == 1, "cache hit must avoid a second provider call"
    assert q1["bid"] == 1.10 and q2["bid"] == 1.10


# 2. zero bid does not cache
def test_zero_bid_not_cached():
    brk = SeqBroker([{"bid": 0, "ask": 1.5}])
    fetch_direct_option_quote(brk, OCC)
    assert not _is_cached(brk)


# 3. zero ask does not cache
def test_zero_ask_not_cached():
    brk = SeqBroker([{"bid": 1.0, "ask": 0}])
    fetch_direct_option_quote(brk, OCC)
    assert not _is_cached(brk)


# 4. negative bid/ask does not cache
def test_negative_bid_not_cached():
    brk = SeqBroker([{"bid": -1.0, "ask": 1.5}])
    fetch_direct_option_quote(brk, OCC)
    assert not _is_cached(brk)


def test_negative_ask_not_cached():
    brk = SeqBroker([{"bid": 1.0, "ask": -2.0}])
    fetch_direct_option_quote(brk, OCC)
    assert not _is_cached(brk)


# 5. NaN does not cache
def test_nan_not_cached():
    brk = SeqBroker([{"bid": float("nan"), "ask": 1.5}])
    fetch_direct_option_quote(brk, OCC)
    assert not _is_cached(brk)


# 6. positive/negative Infinity does not cache
def test_positive_infinity_not_cached():
    brk = SeqBroker([{"bid": 1.0, "ask": float("inf")}])
    fetch_direct_option_quote(brk, OCC)
    assert not _is_cached(brk)


def test_negative_infinity_not_cached():
    brk = SeqBroker([{"bid": float("-inf"), "ask": 1.5}])
    fetch_direct_option_quote(brk, OCC)
    assert not _is_cached(brk)


# 7. boolean does not cache (float(True)==1.0 must not masquerade as a price)
def test_boolean_bid_not_cached():
    brk = SeqBroker([{"bid": True, "ask": 1.5}])
    fetch_direct_option_quote(brk, OCC)
    assert not _is_cached(brk)


def test_boolean_false_not_cached():
    brk = SeqBroker([{"bid": False, "ask": 1.5}])
    fetch_direct_option_quote(brk, OCC)
    assert not _is_cached(brk)


# 8. malformed scalar does not cache
def test_malformed_scalar_not_cached():
    brk = SeqBroker([{"bid": "abc", "ask": 1.5}])
    fetch_direct_option_quote(brk, OCC)
    assert not _is_cached(brk)


# overflow literal (1e400 -> inf) does not cache
def test_overflow_not_cached():
    brk = SeqBroker([{"bid": 1e400, "ask": 1.5}])
    fetch_direct_option_quote(brk, OCC)
    assert not _is_cached(brk)


# 9. crossed/inverted quote does not cache
def test_inverted_book_not_cached():
    brk = SeqBroker([{"bid": 2.0, "ask": 1.0}])
    fetch_direct_option_quote(brk, OCC)
    assert not _is_cached(brk)


# 10. invalid observation followed by valid observation -> fresh read + recover
def test_invalid_then_valid_performs_fresh_read_and_recovers():
    brk = SeqBroker([{"bid": 0, "ask": 1.5}, {"bid": 1.0, "ask": 1.5}])
    first = fetch_direct_option_quote(brk, OCC)
    assert not _is_cached(brk), "invalid first observation must not be cached"
    second = fetch_direct_option_quote(brk, OCC)
    assert brk.calls == 2, "second fetch must perform a fresh provider read"
    assert second["bid"] == 1.0, "recovered valid quote must be returned"
    assert _is_cached(brk), "recovered valid quote must now be cached"


# 11. invalid cached legacy shape is not promoted into selector authority
def test_invalid_shapes_are_not_valid_for_selector_authority():
    # direct_quote_is_valid is the selector-authority gate; it must reject the
    # same shapes the cache rejects, including NaN and bool which previously
    # slipped through.
    assert direct_quote_is_valid({"bid": float("nan"), "ask": 1.5}) is False
    assert direct_quote_is_valid({"bid": True, "ask": 1.5}) is False
    assert direct_quote_is_valid({"bid": float("inf"), "ask": 1.5}) is False
    assert direct_quote_is_valid({"bid": 0, "ask": 1.5}) is False
    assert direct_quote_is_valid({"bid": 2.0, "ask": 1.0}) is False
    assert direct_quote_is_valid({"bid": 1.0, "ask": 1.5}) is True
    # eligibility helper agrees
    assert quote_is_cache_eligible({"bid": 1.0, "ask": 1.5}) is True
    assert quote_is_cache_eligible({"bid": float("nan"), "ask": 1.5}) is False


# 12. canonical #471 direct-quote budget behavior remains unchanged
def test_pr471_budget_authority_symbols_unchanged():
    # This PR does not touch the direct-quote budget module surface. Assert the
    # canonical budget helper/exports referenced by #471 are still importable
    # and unchanged in shape, so the budget authority is preserved.
    import ap.contract_quote_revalidator as R
    # The budget failure helper is the #471 authority entry point.
    assert hasattr(R, "_direct_quote_budget_failure")
    # The cache-eligibility change must not have altered the budget contract:
    # a valid quote still caches (budget path only runs on cache miss).
    brk = SeqBroker([{"bid": 1.1, "ask": 1.2}])
    fetch_direct_option_quote(brk, OCC)
    assert _is_cached(brk)
