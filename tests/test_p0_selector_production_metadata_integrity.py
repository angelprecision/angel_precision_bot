from __future__ import annotations

import contextvars
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


class FakeSelector(APContractSelectionEngine):
    def __init__(self, *, mode="PAPER", chain=None, iv_filter=None):
        super().__init__(
            broker=SimpleNamespace(base_url="https://api.tradier.com"),
            mode=mode,
            iv_filter=iv_filter,
            min_premium=1.0,
            max_premium=1000.0,
        )
        self.fetch_calls = 0
        self.chain = chain or [_option()]

    def _fetch_chain_with_price(self, ticker, direction, *, expiration_override=None, request_context=None):
        self.fetch_calls += 1
        return list(self.chain), 500.0


class MomentumIVFilter:
    def check(self, *args, **kwargs):
        return {
            "blocked": False,
            "requires_momentum": True,
            "iv_zone": "soft",
            "momentum_min_score": 65.0,
            "iv_rank": 70.0,
        }


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


def _plan_dict(**overrides):
    plan = {
        "signal_id": "sig-1",
        "client_id": "client-A",
        "execution_mode": "LIVE",
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


def _plan_object(**overrides):
    return SimpleNamespace(**_plan_dict(**overrides))


def test_blank_mode_rejects_before_market_data():
    selector = FakeSelector(mode="")
    result = selector.select(_plan_dict())
    assert result is None
    assert selector.fetch_calls == 0
    assert selector.get_last_failure()["reason_code"] == "INVALID_EXECUTION_MODE"


def test_unknown_mode_rejects_before_market_data():
    selector = FakeSelector(mode="unknown")
    result = selector.select(_plan_dict())
    assert result is None
    assert selector.fetch_calls == 0
    assert selector.get_last_failure()["reason_code"] == "INVALID_EXECUTION_MODE"


def test_live_uses_ask_execution_basis_and_preserves_identity():
    selector = FakeSelector(mode="LIVE")
    plan = _plan_dict()
    result = selector.select(plan)
    assert result is not None
    assert result.pricing_basis == "ASK_EXECUTION"
    assert result.execution_price_per_share == pytest.approx(2.50)
    assert plan["client_id"] == "client-A"
    assert plan["execution_mode"] == "LIVE"
    assert plan["signal_id"] == "sig-1"


def test_paper_uses_paper_simulation_basis():
    selector = FakeSelector(mode="PAPER")
    result = selector.select(_plan_dict(execution_mode="PAPER"))
    assert result is not None
    assert result.pricing_basis == "MID_SIMULATION"
    assert result.execution_price_per_share == pytest.approx(2.49)


def test_selector_budget_beats_generic_budget():
    selector = FakeSelector(mode="LIVE")
    plan = _plan_dict(
        metadata={"sizing_context": {"budget": 500.0, "selector_budget": 260.0}},
        max_position_usd=999.0,
    )
    result = selector.select(plan)
    assert result is not None
    assert result.affordable_contracts == 1
    assert plan["selector_metadata"]["budget_conflict"] is True
    assert plan["selector_metadata"]["budget_source"] == "selector_budget"
    assert plan["selector_metadata"]["effective_budget"] == pytest.approx(260.0)
    assert plan["selector_metadata"]["premium_per_contract_usd"] == pytest.approx(250.0)
    assert plan["selector_metadata"]["projected_reserved_cost_usd"] == pytest.approx(250.0)


def test_remaining_capacity_beats_generic_budget():
    selector = FakeSelector(mode="LIVE")
    plan = _plan_dict(
        metadata={"sizing_context": {"budget": 500.0, "remaining_capacity": 220.0}},
        max_position_usd=999.0,
    )
    result = selector.select(plan)
    assert result is None
    assert selector.get_last_failure()["reason_code"] == "UNTRADEABLE_FOR_ACCOUNT_SIZE"
    assert isinstance(plan["metadata"]["selector_failure"]["selection_diagnostics"], dict)
    assert selector.get_last_failure()["tradeability_diag"]["budget"] == pytest.approx(220.0)


def test_minimum_positive_budget_constraint_wins():
    selector = FakeSelector(mode="LIVE")
    plan = _plan_dict(
        metadata={
            "sizing_context": {
                "budget": 500.0,
                "selector_budget": 400.0,
                "remaining_capacity": 250.0,
            }
        },
        max_position_usd=999.0,
    )
    result = selector.select(plan)
    assert result is not None
    assert result.affordable_contracts == 1
    assert plan["selector_metadata"]["budget_source"] == "remaining_capacity"
    assert plan["selector_metadata"]["effective_budget"] == pytest.approx(250.0)


def test_remaining_budget_ranking_selects_cheaper_valid_contract():
    expensive = _option(bid=4.48, ask=4.50, symbol="SPY260717C00501000")
    cheap = _option(bid=2.48, ask=2.50, symbol="SPY260717C00500000")
    selector = FakeSelector(mode="LIVE", chain=[expensive, cheap])
    plan = _plan_dict(
        metadata={"sizing_context": {"budget": 500.0, "selector_budget": 260.0}},
        max_position_usd=999.0,
    )
    result = selector.select(plan)
    assert result is not None
    assert result.contract_symbol == "SPY260717C00500000"
    assert result.premium_per_contract == pytest.approx(250.0)
    assert result.affordable_contracts == 1


def test_dict_and_object_plans_select_equivalently():
    dict_result = FakeSelector(mode="LIVE").select(_plan_dict())
    obj_result = FakeSelector(mode="LIVE").select(_plan_object())
    assert dict_result is not None and obj_result is not None
    assert dict_result.contract_symbol == obj_result.contract_symbol
    assert dict_result.affordable_contracts == obj_result.affordable_contracts
    assert dict_result.pricing_basis == obj_result.pricing_basis


def test_dict_plan_score_passes_iv_momentum_gate():
    selector = FakeSelector(mode="LIVE", iv_filter=MomentumIVFilter())
    result = selector.select(_plan_dict(score=80.0))
    assert result is not None
    assert selector.get_last_failure() is None


def test_selector_live_plan_paper_rejects_before_market_data():
    selector = FakeSelector(mode="LIVE")
    result = selector.select(_plan_dict(execution_mode="PAPER"))
    assert result is None
    assert selector.fetch_calls == 0
    assert selector.get_last_failure()["reason_code"] == "EXECUTION_MODE_MISMATCH"


def test_selector_paper_plan_live_rejects_before_market_data():
    selector = FakeSelector(mode="PAPER")
    result = selector.select(_plan_dict(execution_mode="LIVE"))
    assert result is None
    assert selector.fetch_calls == 0
    assert selector.get_last_failure()["reason_code"] == "EXECUTION_MODE_MISMATCH"


def test_invalid_plan_execution_mode_rejects_before_market_data():
    selector = FakeSelector(mode="LIVE")
    for bad_mode in ("", "unknown"):
        selector.fetch_calls = 0
        result = selector.select(_plan_dict(execution_mode=bad_mode))
        assert result is None
        assert selector.fetch_calls == 0
        assert selector.get_last_failure()["reason_code"] == "INVALID_EXECUTION_MODE"


def test_execution_mode_case_normalization_preserves_plan_identity():
    selector = FakeSelector(mode=" live ")
    plan = _plan_dict(execution_mode=" Live ")
    result = selector.select(plan)
    assert result is not None
    assert result.pricing_basis == "ASK_EXECUTION"
    assert plan["execution_mode"] == " Live "


def test_cost_diagnostics_do_not_double_multiply_contract_cost():
    selector = FakeSelector(mode="LIVE", chain=[_option(bid=2.48, ask=2.50)])
    plan = _plan_dict(metadata={"sizing_context": {"budget": 200.0, "account_equity": 2000.0}})
    result = selector.select(plan)
    assert result is None
    failure = plan["metadata"]["selector_failure"]
    diag = failure["selection_diagnostics"] if "selection_diagnostics" in failure else {}
    last = selector.get_last_failure()
    tradeability = last["tradeability_diag"]
    assert tradeability["premium_per_contract_usd"] == pytest.approx(250.0)
    assert tradeability["ask_cost"] == pytest.approx(250.0)
    assert tradeability["projected_reserved_cost"] == pytest.approx(250.0)
    assert isinstance(diag, dict)


def test_two_contexts_on_one_selector_do_not_cross_contaminate_getters():
    selector = FakeSelector(mode="LIVE")
    ctx_a = contextvars.copy_context()
    ctx_b = contextvars.copy_context()
    seen = {}

    def set_failure(name, reason):
        selector._set_last_failure({"stage": name, "reason_code": reason, "explanation": name})
        seen[name] = selector.get_last_failure()["reason_code"]

    ctx_a.run(set_failure, "a", "FAIL_A")
    ctx_b.run(set_failure, "b", "FAIL_B")
    assert ctx_a.run(selector.get_last_failure)["reason_code"] == "FAIL_A"
    assert ctx_b.run(selector.get_last_failure)["reason_code"] == "FAIL_B"
    assert seen == {"a": "FAIL_A", "b": "FAIL_B"}


def test_malformed_budget_rejects_before_chain_fetch():
    selector = FakeSelector(mode="LIVE")
    result = selector.select(
        _plan_dict(metadata={"sizing_context": {"budget": "bad"}}, max_position_usd=None)
    )
    assert result is None
    assert selector.fetch_calls == 0
    assert selector.get_last_failure()["reason_code"] == "INVALID_POSITION_BUDGET"
