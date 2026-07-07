from __future__ import annotations

from types import SimpleNamespace

from ap.live_submit_integration_guard import _final_market_validity_for_submit, _patch_exit_safety_binding


class FakeResponse:
    def __init__(self, quote):
        self._quote = quote

    def raise_for_status(self):
        return None

    def json(self):
        return {"quotes": {"quote": self._quote}}


class FakeSession:
    def __init__(self, quote):
        self.quote = quote
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return FakeResponse(self.quote)


class FakeBroker:
    def __init__(self, quote):
        self.base_url = "https://api.tradier.com"
        self.session = FakeSession(quote)


def test_final_market_validity_blocks_consumed_call_opportunity():
    current = {
        "symbol": "META",
        "direction": "CALL",
        "execution_mode": "live",
        "trigger_price": 100.0,
        "target_underlying": 110.0,
    }
    broker = FakeBroker({"symbol": "META", "bid": 108.0, "ask": 108.5, "last": 108.2})

    result = _final_market_validity_for_submit(current=current, plan=None, broker=broker)

    assert result["ok"] is False
    assert result["reason"] == "REMAINING_OPPORTUNITY_TOO_SMALL"


def test_final_market_validity_allows_remaining_call_opportunity():
    current = {
        "symbol": "META",
        "direction": "CALL",
        "execution_mode": "live",
        "trigger_price": 100.0,
        "target_underlying": 120.0,
    }
    broker = FakeBroker({"symbol": "META", "bid": 105.0, "ask": 105.5, "last": 105.2})

    result = _final_market_validity_for_submit(current=current, plan=None, broker=broker)

    assert result["ok"] is True
    assert result["reason"] == "FINAL_MARKET_VALIDITY_OK"


def test_osm_local_exit_safety_binding_is_post_processed():
    module = SimpleNamespace()

    def original(**kwargs):
        return {"blocked": True, "reason": "exit_circuit_breaker_tripped"}

    module.evaluate_exit_submission_safety = original
    _patch_exit_safety_binding(module)

    result = module.evaluate_exit_submission_safety(
        position_id="repair-client-META260717C00100000",
        broker_truth_open_qty=1,
        allow_missing_position_with_broker_truth=True,
    )

    assert result["blocked"] is False
    assert result["p0_broker_truth_circuit_breaker_bypass"] is True
