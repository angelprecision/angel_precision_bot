"""
Regression tests for contract quote revalidator hardening.

These tests cover the surgical MVP hardening from the contract quote
revalidator audit:
  - structured direct quote fetch failure reasons
  - explicit fetch-latency semantics
  - transport-aware direct quote cache key
  - LIVE final premium cap uses ask execution basis
"""
from __future__ import annotations

import pytest

from ap.contract_quote_revalidator import (
    clear_quote_cache,
    fetch_direct_option_quote,
    fetch_direct_option_quote_with_meta,
    final_quote_check_before_submit,
    revalidate_with_direct_quote,
    direct_quote_is_valid,
    REASON_DIRECT_QUOTE_AUTH_FAILED,
    REASON_DIRECT_QUOTE_FETCH_TIMEOUT,
    REASON_DIRECT_QUOTE_RATE_LIMITED,
    REASON_DIRECT_QUOTE_SERVER_ERROR,
    REASON_FINAL_CONTRACT_QUOTE_INVALID,
    REASON_FINAL_CONTRACT_UNAFFORDABLE,
)

OCC = "NVDA260620C00500000"


class Cfg:
    def __init__(self, base_url: str):
        self.base_url = base_url


class StubBroker:
    def __init__(self, quote=None, *, base_url="https://api.tradier.com", exc=None):
        self.cfg = Cfg(base_url)
        self.quote = quote if quote is not None else {"bid": 1.20, "ask": 1.25}
        self.exc = exc
        self.calls = []

    def get_quote(self, symbol: str):
        self.calls.append(symbol)
        if self.exc is not None:
            raise self.exc
        return dict(self.quote)


class ResponseBackedError(RuntimeError):
    def __init__(self, status_code: int):
        super().__init__(f"HTTP {status_code}")
        self.response = type("Response", (), {"status_code": status_code})()


class MetadataError(RuntimeError):
    reason_code = REASON_DIRECT_QUOTE_AUTH_FAILED
    status_code = 403
    endpoint = "/v1/markets/quotes"
    retryable = False


class TimeoutLikeError(RuntimeError):
    pass


@pytest.fixture(autouse=True)
def _clear_cache():
    clear_quote_cache()
    yield
    clear_quote_cache()


def _chain_opt_zero(symbol=OCC):
    return {
        "symbol": symbol,
        "strike": 500.0,
        "bid": 0.0,
        "ask": 0.0,
        "last": 0.0,
        "volume": 0,
        "open_interest": 0,
        "option_type": "call",
        "expiration_date": "2026-06-20",
    }


@pytest.mark.parametrize(
    ("exc", "expected_reason", "expected_retryable"),
    [
        (
            TimeoutLikeError("request timeout while fetching quote"),
            REASON_DIRECT_QUOTE_FETCH_TIMEOUT,
            True,
        ),
        (ResponseBackedError(429), REASON_DIRECT_QUOTE_RATE_LIMITED, True),
        (ResponseBackedError(500), REASON_DIRECT_QUOTE_SERVER_ERROR, True),
        (MetadataError("forbidden"), REASON_DIRECT_QUOTE_AUTH_FAILED, False),
    ],
)
def test_revalidate_preserves_structured_fetch_failure_reason(
    exc,
    expected_reason,
    expected_retryable,
):
    broker = StubBroker(exc=exc)
    result = revalidate_with_direct_quote(
        broker,
        _chain_opt_zero(),
        "zero_bid_or_ask",
        market_open_override=True,
    )
    assert result["action"] == "REJECT_UNAVAILABLE"
    assert result["reason_code"] == expected_reason
    assert result["audit"]["contract_quote_source"] == "none"
    assert result["audit"]["direct_quote_retryable"] is expected_retryable


def test_empty_payload_is_structured_unavailable_for_decision_callers():
    broker = StubBroker(quote={})
    meta = fetch_direct_option_quote_with_meta(broker, OCC)
    assert meta["ok"] is False
    assert meta["reason_code"] == "DIRECT_QUOTE_UNAVAILABLE"
    assert meta["error"] == "empty quote payload"


def test_backward_compatible_fetch_still_returns_normalized_empty_quote():
    broker = StubBroker(quote={})
    quote = fetch_direct_option_quote(broker, OCC)
    assert quote is not None
    assert quote["bid"] is None
    assert quote["ask"] is None
    assert quote["quote_age_semantics"] == "fetch_latency_not_exchange_age"


def test_invalid_direct_quote_is_not_cached_over_a_recovered_feed():
    class SequencedBroker(StubBroker):
        def __init__(self):
            super().__init__(quote={})
            self.responses = [
                {"bid": 0.0, "ask": 0.0},
                {"bid": 1.20, "ask": 1.25},
            ]

        def get_quote(self, symbol: str):
            self.calls.append(symbol)
            return dict(self.responses.pop(0))

    broker = SequencedBroker()
    first = fetch_direct_option_quote_with_meta(broker, OCC)
    second = fetch_direct_option_quote_with_meta(broker, OCC)

    assert first["ok"] is True
    assert first["quote"]["bid"] == 0.0
    assert second["quote"]["bid"] == 1.20
    assert broker.calls == [OCC, OCC]


@pytest.mark.parametrize(
    "invalid_quote",
    [
        {"bid": float("nan"), "ask": 1.25},
        {"bid": 1.20, "ask": float("nan")},
        {"bid": float("inf"), "ask": float("inf")},
        {"bid": True, "ask": True},
        {"bid": 10**10000, "ask": 10**10000},
    ],
)
def test_nonfinite_boolean_and_overflow_quotes_are_invalid(invalid_quote):
    assert direct_quote_is_valid(invalid_quote) is False


@pytest.mark.parametrize(
    "invalid_quote",
    [
        {"bid": float("nan"), "ask": 1.25},
        {"bid": 1.20, "ask": float("nan")},
        {"bid": float("inf"), "ask": float("inf")},
        {"bid": True, "ask": True},
        {"bid": 10**10000, "ask": 10**10000},
    ],
)
def test_malformed_direct_quote_is_not_cached(invalid_quote):
    class SequencedBroker(StubBroker):
        def __init__(self):
            super().__init__(quote={})
            self.responses = [invalid_quote, {"bid": 1.20, "ask": 1.25}]

        def get_quote(self, symbol: str):
            self.calls.append(symbol)
            return dict(self.responses.pop(0))

    broker = SequencedBroker()
    first = fetch_direct_option_quote_with_meta(broker, OCC)
    second = fetch_direct_option_quote_with_meta(broker, OCC)

    assert first["ok"] is True
    assert direct_quote_is_valid(first["quote"]) is False
    assert second["quote"]["bid"] == 1.20
    assert broker.calls == [OCC, OCC]


def test_legacy_fetch_does_not_cache_malformed_direct_quote():
    class SequencedBroker(StubBroker):
        def __init__(self):
            super().__init__(quote={})
            self.responses = [
                {"bid": float("nan"), "ask": 1.25},
                {"bid": 1.20, "ask": 1.25},
            ]

        def get_quote(self, symbol: str):
            self.calls.append(symbol)
            return dict(self.responses.pop(0))

    broker = SequencedBroker()
    first = fetch_direct_option_quote(broker, OCC)
    second = fetch_direct_option_quote(broker, OCC)

    assert first["bid"] is None
    assert second["bid"] == 1.20
    assert broker.calls == [OCC, OCC]


@pytest.mark.parametrize(
    "invalid_quote",
    [
        {"bid": float("nan"), "ask": 1.25},
        {"bid": 1.20, "ask": float("nan")},
        {"bid": True, "ask": True},
    ],
)
def test_final_quote_gate_rejects_malformed_direct_quote(invalid_quote):
    broker = StubBroker(quote=invalid_quote)
    result = final_quote_check_before_submit(
        broker,
        OCC,
        max_spread_pct=0.50,
        min_premium=10.0,
        max_premium=350.0,
        budget_usd=1000.0,
        qty=1,
        is_live=True,
    )

    assert result["ok"] is False
    assert result["reason_code"] == REASON_FINAL_CONTRACT_QUOTE_INVALID


def test_direct_quote_metadata_names_latency_not_exchange_age():
    broker = StubBroker(quote={"bid": 1.20, "ask": 1.25, "last": 1.22})
    quote = fetch_direct_option_quote(broker, OCC)
    assert quote["quote_age_ms"] >= 0
    assert quote["quote_fetch_latency_ms"] == quote["quote_age_ms"]
    assert quote["quote_fetched_at"] == quote["fetched_at"]
    assert quote["quote_age_semantics"] == "fetch_latency_not_exchange_age"


def test_direct_quote_cache_is_transport_aware():
    live = StubBroker(
        quote={"bid": 1.20, "ask": 1.25},
        base_url="https://api.tradier.com",
    )
    sandbox = StubBroker(
        quote={"bid": 0.10, "ask": 0.15},
        base_url="https://sandbox.tradier.com",
    )
    q_live = fetch_direct_option_quote(live, OCC)
    q_sandbox = fetch_direct_option_quote(sandbox, OCC)
    assert q_live["bid"] == 1.20
    assert q_sandbox["bid"] == 0.10
    assert len(live.calls) == 1
    assert len(sandbox.calls) == 1


def test_live_final_premium_cap_uses_ask_execution_basis():
    # mid = 3.50 ($350) would fit max_premium, but LIVE executes at ask = 3.60
    # ($360), so the final gate must block before broker submit.
    broker = StubBroker(quote={"bid": 3.40, "ask": 3.60})
    result = final_quote_check_before_submit(
        broker,
        OCC,
        max_spread_pct=0.50,
        min_premium=10.0,
        max_premium=350.0,
        budget_usd=1000.0,
        qty=1,
        is_live=True,
    )
    assert result["ok"] is False
    assert result["reason_code"] == REASON_FINAL_CONTRACT_UNAFFORDABLE
    assert result["pricing_basis"] == "ASK_EXECUTION"
    assert result["execution_price"] == 3.60
    assert result["execution_cost"] == 360.0


def test_paper_final_premium_cap_keeps_mid_simulation_basis():
    # Same quote as the live test. PAPER remains mid-based, so $350 is within
    # max_premium and the quote passes when budget allows.
    broker = StubBroker(quote={"bid": 3.40, "ask": 3.60})
    result = final_quote_check_before_submit(
        broker,
        OCC,
        max_spread_pct=0.50,
        min_premium=10.0,
        max_premium=350.0,
        budget_usd=1000.0,
        qty=1,
        is_live=False,
    )
    assert result["ok"] is True
    assert result["pricing_basis"] == "MID_SIMULATION"
    assert result["execution_price"] == pytest.approx(3.50)
    assert result["execution_cost"] == pytest.approx(350.0)
