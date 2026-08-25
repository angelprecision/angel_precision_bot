from __future__ import annotations

import math
import os
import time
from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://fake")

from ap.contract_quote_revalidator import (  # noqa: E402
    _QUOTE_CACHE,
    _cache_key_for,
    clear_quote_cache,
    direct_quote_is_valid,
    fetch_direct_option_quote,
    fetch_direct_option_quote_with_meta,
    revalidate_with_direct_quote,
)
from ap.contract_selector import (  # noqa: E402
    APContractSelectionEngine,
    _classify_selector_failure,
)
from ap.brokers.tradier import TradierBroker, TradierConfig  # noqa: E402


OCC = "SPY260901C00101000"
VALID_QUOTE = {
    "bid": 1.60,
    "ask": 1.70,
    "volume": 500,
    "open_interest": 2000,
}


class SequenceBroker:
    base_url = "https://api.tradier.com"

    def __init__(self, quotes, *, base_url=None, chain=None, underlying=100.0):
        self.cfg = SimpleNamespace(
            base_url=base_url or self.base_url,
            access_token="token",
        )
        self.base_url = self.cfg.base_url
        self._quotes = list(quotes)
        self._quote_index = 0
        self.calls: list[str] = []
        self.chain = list(chain or [])
        self.underlying = float(underlying)
        self.submit_order = MagicMock()
        self.cancel_order = MagicMock()
        self.session = MagicMock()
        self.session.get.side_effect = self._session_get

    def get_quote(self, symbol: str):
        self.calls.append(symbol)
        index = min(self._quote_index, len(self._quotes) - 1)
        self._quote_index += 1
        quote = self._quotes[index]
        return None if quote is None else dict(quote)

    def _session_get(self, url, *, params=None, headers=None, timeout=None):
        response = MagicMock()
        response.status_code = 200
        if "options/expirations" in url:
            response.json.return_value = {
                "expirations": {"date": [_selector_expiry().isoformat()]}
            }
        elif "options/chains" in url:
            response.json.return_value = {"options": {"option": self.chain}}
        else:
            response.json.return_value = {
                "quotes": {"quote": {"last": self.underlying}}
            }
        return response


@pytest.fixture(autouse=True)
def _clear_cache_and_disable_throttle(monkeypatch):
    clear_quote_cache()
    monkeypatch.setenv("TRADIER_MD_THROTTLE_ENABLED", "0")
    yield
    clear_quote_cache()


def _cached(broker, symbol=OCC):
    return _QUOTE_CACHE.get(_cache_key_for(broker, symbol))


INVALID_QUOTES = [
    pytest.param(None, id="none"),
    pytest.param({"bid": "not-a-number", "ask": 1.5}, id="malformed-bid"),
    pytest.param({"bid": 1.0, "ask": "not-a-number"}, id="malformed-ask"),
    pytest.param({"bid": True, "ask": 1.5}, id="bool-bid"),
    pytest.param({"bid": 1.0, "ask": False}, id="bool-ask"),
    pytest.param({"bid": math.nan, "ask": 1.5}, id="nan-bid"),
    pytest.param({"bid": 1.0, "ask": math.nan}, id="nan-ask"),
    pytest.param({"bid": math.inf, "ask": 2.0}, id="positive-inf-bid"),
    pytest.param({"bid": 1.0, "ask": math.inf}, id="positive-inf-ask"),
    pytest.param({"bid": -math.inf, "ask": 2.0}, id="negative-inf-bid"),
    pytest.param({"bid": 1.0, "ask": -math.inf}, id="negative-inf-ask"),
    pytest.param({"bid": 0.0, "ask": 1.5}, id="zero-bid"),
    pytest.param({"bid": 1.0, "ask": 0.0}, id="zero-ask"),
    pytest.param({"bid": -1.0, "ask": 1.5}, id="negative-bid"),
    pytest.param({"bid": 1.0, "ask": -1.0}, id="negative-ask"),
    pytest.param({"bid": 2.0, "ask": 1.0}, id="crossed"),
]


@pytest.mark.parametrize(
    "fetch",
    [fetch_direct_option_quote, fetch_direct_option_quote_with_meta],
    ids=["legacy-fetch", "metadata-fetch"],
)
@pytest.mark.parametrize("invalid", INVALID_QUOTES)
def test_unusable_direct_quote_never_becomes_cache_authority(fetch, invalid):
    broker = SequenceBroker([invalid])

    observed = fetch(broker, OCC)
    quote = observed.get("quote") if fetch is fetch_direct_option_quote_with_meta else observed

    assert direct_quote_is_valid(invalid) is False
    assert direct_quote_is_valid(quote) is False
    assert _cached(broker) is None


def test_finite_positive_uncrossed_quote_caches_and_reuses():
    broker = SequenceBroker([VALID_QUOTE])

    first = fetch_direct_option_quote_with_meta(broker, OCC)
    second = fetch_direct_option_quote(broker, OCC)

    assert first["ok"] is True
    assert direct_quote_is_valid(first["quote"]) is True
    assert second["bid"] == VALID_QUOTE["bid"]
    assert second["ask"] == VALID_QUOTE["ask"]
    assert _cached(broker) is not None
    assert broker.calls == [OCC]


@pytest.mark.parametrize(
    ("bid", "ask", "invalid_field"),
    [
        (True, 1.70, "bid"),
        (1.60, False, "ask"),
    ],
)
def test_tradier_boolean_quote_scalars_cannot_become_cache_authority(
    bid, ask, invalid_field
):
    """Provider booleans must stay invalid through the real adapter boundary."""
    broker = TradierBroker(
        TradierConfig(
            base_url="https://api.tradier.com",
            access_token="token",
            account_id="acct",
        )
    )
    broker._get = MagicMock(
        side_effect=[
            {"quotes": {"quote": {"bid": 1.60, "ask": 1.70, "last": 1.65}}},
            {"quotes": {"quote": {"bid": bid, "ask": ask, "last": 1.65}}},
        ]
    )

    first = fetch_direct_option_quote_with_meta(broker, OCC)
    assert first["ok"] is True
    assert _cached(broker) is not None

    refreshed = fetch_direct_option_quote_with_meta(broker, OCC, cache_ttl_s=0.0)
    assert refreshed["ok"] is True
    assert refreshed["quote"][invalid_field] is None
    assert direct_quote_is_valid(refreshed["quote"]) is False
    assert _cached(broker) is None
    broker._get.assert_has_calls(
        [
            (("/v1/markets/quotes",), {"params": {"symbols": OCC, "greeks": "false"}}),
            (("/v1/markets/quotes",), {"params": {"symbols": OCC, "greeks": "false"}}),
        ]
    )


@pytest.mark.parametrize(
    "fetch",
    [fetch_direct_option_quote, fetch_direct_option_quote_with_meta],
    ids=["legacy-fetch", "metadata-fetch"],
)
def test_invalid_forced_refresh_evicts_old_authority_and_valid_refresh_recovers(fetch):
    invalid = {"bid": 0.0, "ask": 1.70}
    broker = SequenceBroker([VALID_QUOTE, invalid, VALID_QUOTE])

    fetch(broker, OCC)
    assert _cached(broker) is not None

    refreshed = fetch(broker, OCC, cache_ttl_s=0.0)
    invalid_quote = (
        refreshed.get("quote")
        if fetch is fetch_direct_option_quote_with_meta
        else refreshed
    )
    assert direct_quote_is_valid(invalid_quote) is False
    assert _cached(broker) is None
    assert broker.calls == [OCC, OCC]

    recovered = fetch(broker, OCC)
    recovered_quote = (
        recovered.get("quote")
        if fetch is fetch_direct_option_quote_with_meta
        else recovered
    )
    assert direct_quote_is_valid(recovered_quote) is True
    assert recovered_quote["bid"] == VALID_QUOTE["bid"]
    assert _cached(broker) is not None
    assert broker.calls == [OCC, OCC, OCC]


def test_legacy_invalid_cache_entry_is_evicted_before_provider_reuse():
    broker = SequenceBroker([VALID_QUOTE])
    _QUOTE_CACHE[_cache_key_for(broker, OCC)] = (time.time(), {"bid": 0.0, "ask": 1.5})

    observed = fetch_direct_option_quote_with_meta(broker, OCC)

    assert observed["ok"] is True
    assert observed["quote"]["bid"] == VALID_QUOTE["bid"]
    assert broker.calls == [OCC]
    assert _cached(broker) is not None


@pytest.mark.parametrize(
    ("mode", "base_url"),
    [
        ("LIVE", "https://api.tradier.com"),
        ("PAPER", "https://sandbox.tradier.com"),
    ],
)
def test_live_and_paper_share_validity_semantics_without_identity_mutation(mode, base_url):
    broker = SequenceBroker([{"bid": True, "ask": 1.5}], base_url=base_url)
    context = SimpleNamespace(
        client_id=f"client-{mode.lower()}",
        signal_id=f"signal-{mode.lower()}",
        execution_mode=mode,
        started_at_monotonic=time.monotonic(),
        provider_call_counts={},
        effective_direct_quote_limit=5,
        max_direct_quote_calls=5,
        diagnostics_sink={},
    )

    result = revalidate_with_direct_quote(
        broker,
        {
            "symbol": OCC,
            "bid": 0.0,
            "ask": 0.0,
        },
        "zero_bid_or_ask",
        market_open_override=True,
        request_context=context,
    )

    assert result["action"] == "REJECT_DIRECT_ZERO"
    assert result["reason_code"] == "DIRECT_QUOTE_ZERO_BID_ASK"
    assert _classify_selector_failure(result["reason_code"]) == (
        "data_quality_zero_quotes",
        True,
        False,
    )
    assert context.client_id == f"client-{mode.lower()}"
    assert context.signal_id == f"signal-{mode.lower()}"
    assert context.execution_mode == mode
    assert _cached(broker) is None


def _selector_expiry() -> date:
    target = date.today() + timedelta(days=2)
    while target.weekday() >= 5:
        target += timedelta(days=1)
    return target


def _selector_row(expiry: date) -> dict:
    return {
        "symbol": f"SPY{expiry:%y%m%d}C00101000",
        "expiration_date": expiry.isoformat(),
        "option_type": "call",
        "strike": 101.0,
        "bid": 0.0,
        "ask": 0.0,
        "last": 0.0,
        "greeks": {"delta": 0.40},
        "open_interest": 2000,
        "volume": 500,
    }


def _selector_plan():
    return {
        "signal_id": "signal-cache-truth",
        "client_id": "client-cache-truth",
        "execution_mode": "LIVE",
        "ticker": "SPY",
        "side": "CALL",
        "target_underlying": 100.0,
        "wick_targets": [{"distance_pct": 0.5, "confidence": 0.75}],
        "trigger_price": 100.0,
        "tier": "A",
        "score": 85.0,
        "pattern": "3-1-2",
        "timeframe": "5m",
        "metadata": {
            "sizing_context": {
                "budget": 1000.0,
                "account_equity": 10000.0,
                "risk_pct": 0.10,
                "max_affordable_premium": 1000.0,
            }
        },
        "max_position_usd": 1000.0,
    }


def test_selector_invalid_current_quote_is_data_unavailable_then_recovers(monkeypatch):
    """A stale valid cache must not hide current invalid data or terminalize quality."""
    monkeypatch.setenv("PRO_CONTRACT_QUALITY", "true")
    monkeypatch.setenv("SELECTOR_MAX_DIRECT_QUOTE_CALLS", "5")
    monkeypatch.setattr(
        "ap.contract_quote_revalidator.is_market_open",
        lambda *args, **kwargs: True,
    )
    monkeypatch.setattr(
        APContractSelectionEngine,
        "_emit_selector_event",
        lambda *args, **kwargs: None,
    )

    expiry = _selector_expiry()
    row = _selector_row(expiry)
    occ = row["symbol"]
    invalid = {
        "bid": 0.0,
        "ask": 1.70,
        "volume": 500,
        "open_interest": 2000,
    }
    broker = SequenceBroker(
        [VALID_QUOTE, invalid, invalid, VALID_QUOTE],
        chain=[row],
        underlying=100.0,
    )
    selector = APContractSelectionEngine(
        broker,
        mode="LIVE",
        data_broker=broker,
        min_premium=1.0,
        max_premium=1000.0,
        min_oi=1,
        min_volume=0,
    )

    first_plan = _selector_plan()
    first_selected = selector.select(first_plan)
    assert first_selected is not None
    assert first_selected.contract_symbol == occ
    assert _cached(broker, occ) is not None
    assert first_plan["metadata"]["selector_request_diagnostics"]["limits"][
        "max_direct_quote_calls"
    ] == 5

    forced = fetch_direct_option_quote_with_meta(broker, occ, cache_ttl_s=0.0)
    assert forced["ok"] is True
    assert direct_quote_is_valid(forced["quote"]) is False
    assert _cached(broker, occ) is None
    assert broker.calls == [occ, occ]

    unavailable_plan = _selector_plan()
    unavailable_selected = selector.select(unavailable_plan)
    assert unavailable_selected is None
    assert _cached(broker, occ) is None
    unavailable_failure = unavailable_plan["metadata"]["selector_failure"]
    assert unavailable_failure["selector_failure_class"] == "data_quality_zero_quotes"
    assert unavailable_failure["data_failure"] is True
    assert unavailable_failure["quality_failure"] is False
    assert unavailable_failure["reason_code"] in {
        "DIRECT_QUOTE_ZERO_BID_ASK",
        "CHAIN_ROW_ZERO_BID_ASK",
    }

    recovered_plan = _selector_plan()
    recovered_selected = selector.select(recovered_plan)
    assert recovered_selected is not None
    assert recovered_selected.contract_symbol == occ
    assert direct_quote_is_valid(_cached(broker, occ)[1]) is True

    # The exact direct-quote request envelope remains one OCC string per
    # selector/provider attempt; no submit/cancel authority is invented.
    assert broker.calls == [occ, occ, occ, occ]
    assert broker.submit_order.call_count == 0
    assert broker.cancel_order.call_count == 0
    assert recovered_plan["metadata"]["selector_request_diagnostics"]["limits"][
        "max_direct_quote_calls"
    ] == 5
