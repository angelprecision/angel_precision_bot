"""P1 — Selector identity assertion must use the capture-side fallback chain.

Production incident: 74 trade_queue ERROR rows in 7 days with
``contract_selector_error: Selector mutation changed execution identity``.

Root cause: identity CAPTURE resolved ``execution_mode`` with a legacy
``mode`` fallback, while the post-mutation ASSERTION read only
``execution_mode`` with no fallback. Any plan carrying legacy ``mode``
(but no ``execution_mode`` attribute) captured a non-empty mode, compared
it against ``None``, and raised a false-positive AssertionError — killing
viable signals at contract selection.

These tests pin both directions:
  1. legacy-``mode``-only plans (dict and object shape) select successfully
     and their identity fields are untouched (false positive eliminated);
  2. a genuine identity mutation during selection still raises
     AssertionError (true-positive detection preserved).
"""
from __future__ import annotations

import sys
from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

sys.modules.setdefault(
    "ap.observability",
    SimpleNamespace(
        emit_decision_event=lambda *args, **kwargs: None,
        get_git_commit=lambda: "test",
        make_config_hash=lambda payload: "hash",
    ),
)
sys.modules.setdefault("psycopg2", MagicMock())
sys.modules.setdefault("psycopg2.extras", MagicMock())
sys.modules.setdefault("psycopg2.pool", MagicMock())

from ap.contract_selector import APContractSelectionEngine


def _option(*, bid=2.48, ask=2.50, symbol="SPY260717C00500000"):
    return {
        "symbol": symbol,
        "expiration_date": (date.today() + timedelta(days=1)).isoformat(),
        "strike": 500.0,
        "option_type": "call",
        "bid": bid,
        "ask": ask,
        "open_interest": 2000,
        "volume": 500,
        "bid_size": 20,
        "ask_size": 20,
        "greeks": {"delta": "0.40"},
    }


class FakeSelector(APContractSelectionEngine):
    def __init__(self, *, mode="PAPER", chain=None):
        super().__init__(
            broker=SimpleNamespace(base_url="https://api.tradier.com"),
            mode=mode,
            iv_filter=None,
            min_premium=1.0,
            max_premium=1000.0,
        )
        self.chain = chain or [_option()]

    def _fetch_chain_with_price(self, ticker, direction, *, expiration_override=None, request_context=None):
        return list(self.chain), 500.0


class MutatingSelector(FakeSelector):
    """Simulates a genuine identity mutation mid-selection."""

    def _fetch_chain_with_price(self, ticker, direction, *, expiration_override=None, request_context=None):
        # Corrupt identity after capture, before the post-mutation assertion.
        if isinstance(self._plan_under_test, dict):
            self._plan_under_test["client_id"] = "client-EVIL"
        else:
            self._plan_under_test.client_id = "client-EVIL"
        return list(self.chain), 500.0


def _legacy_mode_plan_dict(**overrides):
    """Plan shaped like legacy producers: carries ``mode``, NO ``execution_mode``."""
    plan = {
        "signal_id": "sig-legacy-1",
        "client_id": "client-A",
        "mode": "PAPER",  # legacy field — intentionally no execution_mode key
        "ticker": "SPY",
        "side": "CALL",
        "target_underlying": 505.0,
        "wick_targets": [{"distance_pct": 1.0, "confidence": 0.7}],
        "trigger_price": 500.0,
        "tier": "A",
        "score": 80.0,
        "pattern": "3-1-2",
        "timeframe": "5m",
        "metadata": {"sizing_context": {"budget": 500.0, "account_equity": 5000.0}},
        "max_position_usd": 999.0,
    }
    plan.update(overrides)
    return plan


def test_legacy_mode_only_dict_plan_selects_without_false_positive():
    selector = FakeSelector(mode="PAPER")
    plan = _legacy_mode_plan_dict()
    result = selector.select(plan)  # must NOT raise AssertionError
    assert result is not None
    # Identity fields untouched; execution_mode must not be fabricated.
    assert plan["client_id"] == "client-A"
    assert plan["signal_id"] == "sig-legacy-1"
    assert plan["mode"] == "PAPER"


def test_legacy_mode_only_object_plan_selects_without_false_positive():
    selector = FakeSelector(mode="PAPER")
    plan = SimpleNamespace(**_legacy_mode_plan_dict(signal_id="sig-legacy-2"))
    result = selector.select(plan)  # must NOT raise AssertionError
    assert result is not None
    assert plan.client_id == "client-A"
    assert plan.signal_id == "sig-legacy-2"
    assert plan.mode == "PAPER"


def test_genuine_identity_mutation_still_raises():
    selector = MutatingSelector(mode="PAPER")
    plan = _legacy_mode_plan_dict(signal_id="sig-legacy-3")
    selector._plan_under_test = plan
    with pytest.raises(AssertionError, match="Selector mutation changed execution identity"):
        selector.select(plan)
