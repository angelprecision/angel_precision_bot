"""Production-path proof that invalid direct quotes never poison the cache."""

from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@127.0.0.1:5432/test")

from ap.contract_quote_revalidator import (  # noqa: E402
    _QUOTE_CACHE,
    _cache_key_for,
    clear_quote_cache,
    direct_quote_is_valid,
    fetch_direct_option_quote,
    fetch_direct_option_quote_with_meta,
    quote_is_cache_eligible,
)


OCC = "SPY301231C00500000"
VALID = {"bid": 1.0, "ask": 1.5}


class SeqBroker:
    """Production-shaped get_quote stub with deterministic sequential output."""

    def __init__(self, sequence):
        self.sequence = list(sequence)
        self.calls = 0

    def get_quote(self, symbol):
        index = min(self.calls, len(self.sequence) - 1)
        self.calls += 1
        return self.sequence[index]


@pytest.fixture(autouse=True)
def _clear_cache():
    clear_quote_cache()
    yield
    clear_quote_cache()


def _is_cached(broker):
    return _QUOTE_CACHE.get(_cache_key_for(broker, OCC)) is not None


@pytest.mark.parametrize(
    "invalid",
    [
        {},
        {"bid": None, "ask": 1.5},
        {"bid": 1.0, "ask": None},
        {"bid": 0, "ask": 1.5},
        {"bid": 1.0, "ask": 0},
        {"bid": -1.0, "ask": 1.5},
        {"bid": 1.0, "ask": -1.5},
        {"bid": float("nan"), "ask": 1.5},
        {"bid": 1.0, "ask": float("nan")},
        {"bid": float("inf"), "ask": 1.5},
        {"bid": 1.0, "ask": float("inf")},
        {"bid": float("-inf"), "ask": 1.5},
        {"bid": True, "ask": 1.5},
        {"bid": False, "ask": 1.5},
        {"bid": "not-a-price", "ask": 1.5},
        {"bid": 1e400, "ask": 1.5},
        {"bid": 10**1000, "ask": 1.5},
        {"bid": 2.0, "ask": 1.0},
    ],
)
def test_invalid_normal_quote_is_not_cached(invalid):
    broker = SeqBroker([invalid, VALID])

    first = fetch_direct_option_quote(broker, OCC)

    assert first is not None
    assert not _is_cached(broker)


def test_normal_path_invalid_then_valid_reads_again_and_recovers():
    broker = SeqBroker([{"bid": 0, "ask": 1.5}, VALID])

    first = fetch_direct_option_quote(broker, OCC)
    second = fetch_direct_option_quote(broker, OCC)

    assert first["bid"] == 0.0
    assert second["bid"] == 1.0
    assert broker.calls == 2
    assert _is_cached(broker)


def test_normal_path_valid_first_is_cached_and_second_call_is_suppressed():
    broker = SeqBroker([VALID])

    first = fetch_direct_option_quote(broker, OCC)
    second = fetch_direct_option_quote(broker, OCC)

    assert first["bid"] == second["bid"] == 1.0
    assert broker.calls == 1
    assert _is_cached(broker)


def test_selector_validity_rejects_the_same_invalid_shapes_as_cache():
    assert direct_quote_is_valid({"bid": float("nan"), "ask": 1.5}) is False
    assert direct_quote_is_valid({"bid": True, "ask": 1.5}) is False
    assert direct_quote_is_valid({"bid": float("inf"), "ask": 1.5}) is False
    assert direct_quote_is_valid({"bid": 0, "ask": 1.5}) is False
    assert direct_quote_is_valid({"bid": 2.0, "ask": 1.0}) is False
    assert direct_quote_is_valid(VALID) is True
    assert quote_is_cache_eligible(VALID) is True
    assert quote_is_cache_eligible({"bid": float("nan"), "ask": 1.5}) is False


def test_with_meta_valid_first_is_cached_and_second_call_is_suppressed():
    broker = SeqBroker([VALID])

    first = fetch_direct_option_quote_with_meta(broker, OCC)
    second = fetch_direct_option_quote_with_meta(broker, OCC)

    assert first["ok"] is True and second["ok"] is True
    assert first["quote"]["bid"] == second["quote"]["bid"] == 1.0
    assert broker.calls == 1
    assert _is_cached(broker)


@pytest.mark.parametrize(
    "invalid",
    [
        {"bid": float("nan"), "ask": 1.5},
        {"bid": True, "ask": 1.5},
        {"bid": 0, "ask": 1.5},
        {"bid": 2.0, "ask": 1.0},
    ],
)
def test_with_meta_invalid_then_valid_reads_again_and_recovers(invalid):
    broker = SeqBroker([invalid, VALID])

    first = fetch_direct_option_quote_with_meta(broker, OCC)

    assert first["ok"] is True
    assert not _is_cached(broker)

    second = fetch_direct_option_quote_with_meta(broker, OCC)

    assert second["ok"] is True
    assert second["quote"]["bid"] == 1.0
    assert broker.calls == 2
    assert _is_cached(broker)


def _request_context(mode):
    return SimpleNamespace(
        execution_mode=mode,
        started_at_monotonic=0.0,
        max_total_elapsed_ms=15_000,
        max_direct_quote_calls=5,
        provider_call_counts={},
        direct_quote_attempted_symbols=[],
        diagnostics_sink={},
    )


@pytest.mark.parametrize("mode", ["live", "paper"])
def test_with_meta_live_and_paper_have_identical_cache_recovery(mode):
    """The same quote tape has the same cache/recovery result in both modes."""
    broker = SeqBroker([{"bid": 0, "ask": 1.5}, VALID])
    context = _request_context(mode)

    first = fetch_direct_option_quote_with_meta(
        broker, OCC, request_context=context
    )
    second = fetch_direct_option_quote_with_meta(
        broker, OCC, request_context=context
    )

    assert first["quote"]["bid"] == 0.0
    assert second["quote"]["bid"] == 1.0
    assert broker.calls == 2
    assert context.provider_call_counts["direct_quote_calls"] == 2
    assert _is_cached(broker)

