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


def _force_rank(monkeypatch, scores):
    def fake_rank(self, opt, budget, **kwargs):
        return scores[opt["symbol"]]

    monkeypatch.setattr(APContractSelectionEngine, "_rank_score", fake_rank)


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


def test_selector_budget_zero_blocks_before_market_data():
    selector = FakeSelector(mode="LIVE")
    plan = _plan_dict(
        metadata={"sizing_context": {"budget": 500.0, "selector_budget": 0.0}},
        max_position_usd=None,
    )
    result = selector.select(plan)
    assert result is None
    assert selector.fetch_calls == 0
    assert selector.get_last_failure()["reason_code"] == "CAPITAL_NO_REMAINING"


def test_remaining_capacity_zero_blocks_before_market_data():
    selector = FakeSelector(mode="LIVE")
    plan = _plan_dict(
        metadata={"sizing_context": {"budget": 500.0, "remaining_capacity": 0.0}},
        max_position_usd=500.0,
    )
    result = selector.select(plan)
    assert result is None
    assert selector.fetch_calls == 0
    assert selector.get_last_failure()["reason_code"] == "CAPITAL_NO_REMAINING"


def test_zero_authoritative_budget_beats_positive_constraints():
    selector = FakeSelector(mode="LIVE")
    plan = _plan_dict(
        metadata={"sizing_context": {"selector_budget": 260.0, "remaining_capacity": 0.0}},
        max_position_usd=500.0,
    )
    result = selector.select(plan)
    assert result is None
    assert selector.fetch_calls == 0
    assert selector.get_last_failure()["reason_code"] == "CAPITAL_NO_REMAINING"


def test_generic_budget_is_only_fallback_when_authoritative_constraints_missing():
    selector = FakeSelector(mode="LIVE")
    plan = _plan_dict(
        metadata={"sizing_context": {"budget": 500.0}},
        max_position_usd=None,
    )
    result = selector.select(plan)
    assert result is not None
    assert result.affordable_contracts == 2
    assert plan["selector_metadata"]["budget_source"] == "budget"


def test_negative_selector_budget_rejects_without_falling_back():
    selector = FakeSelector(mode="LIVE")
    plan = _plan_dict(
        metadata={"sizing_context": {"budget": 500.0, "selector_budget": -10.0}},
        max_position_usd=None,
    )
    result = selector.select(plan)
    assert result is None
    assert selector.fetch_calls == 0
    assert selector.get_last_failure()["reason_code"] == "INVALID_POSITION_BUDGET"


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
        _plan_dict(
            metadata={"sizing_context": {"budget": 500.0, "selector_budget": "bad"}},
            max_position_usd=None,
        )
    )
    assert result is None
    assert selector.fetch_calls == 0
    assert selector.get_last_failure()["reason_code"] == "INVALID_POSITION_BUDGET"


def test_rank_one_unaffordable_falls_back_to_rank_two(monkeypatch):
    expensive = _option(bid=4.48, ask=4.50, symbol="SPY260717C00501000")
    affordable = _option(bid=2.48, ask=2.50, symbol="SPY260717C00500000")
    _force_rank(monkeypatch, {expensive["symbol"]: 100.0, affordable["symbol"]: 90.0})
    selector = FakeSelector(mode="LIVE", chain=[expensive, affordable])
    plan = _plan_dict(metadata={"sizing_context": {"selector_budget": 260.0, "budget": 500.0}})
    result = selector.select(plan)
    assert result is not None
    assert result.contract_symbol == affordable["symbol"]
    assert selector.fetch_calls == 1
    assert plan["selector_metadata"]["premium_per_contract_usd"] == pytest.approx(250.0)


def test_cheap_contract_rejection_falls_back_to_next_candidate(monkeypatch):
    cheap = _option(bid=0.48, ask=0.49, symbol="SPY260717C00495000")
    valid = _option(bid=2.48, ask=2.50, symbol="SPY260717C00500000")
    _force_rank(monkeypatch, {cheap["symbol"]: 100.0, valid["symbol"]: 90.0})
    selector = FakeSelector(mode="LIVE", chain=[cheap, valid])
    result = selector.select(_plan_dict())
    assert result is not None
    assert result.contract_symbol == valid["symbol"]
    assert result.candidate_audit["final_candidate_rejections"][0]["reason_code"] == "CHEAP_CONTRACT_NO_UPGRADE"


def test_rank_one_final_delta_fail_falls_back(monkeypatch):
    low_delta = _option(symbol="SPY260717C00499000")
    low_delta["greeks"] = {"delta": "0.06"}
    valid = _option(symbol="SPY260717C00500000")
    _force_rank(monkeypatch, {low_delta["symbol"]: 100.0, valid["symbol"]: 90.0})
    selector = FakeSelector(mode="LIVE", chain=[low_delta, valid])
    selector.target_delta = 0.2
    selector.delta_band = 0.2
    result = selector.select(_plan_dict())
    assert result is not None
    assert result.contract_symbol == valid["symbol"]
    assert result.candidate_audit["final_candidate_rejections"][0]["reason_code"] == "DELTA_OUT_OF_RANGE"


def test_rank_one_final_moneyness_fail_falls_back(monkeypatch):
    far_otm = _option(symbol="SPY260717C00580000")
    far_otm["strike"] = 580.0
    valid = _option(symbol="SPY260717C00500000")
    _force_rank(monkeypatch, {far_otm["symbol"]: 100.0, valid["symbol"]: 90.0})
    selector = FakeSelector(mode="LIVE", chain=[far_otm, valid])
    result = selector.select(_plan_dict())
    assert result is not None
    assert result.contract_symbol == valid["symbol"]
    assert result.candidate_audit["final_candidate_rejections"][0]["contract"] == far_otm["symbol"]


def test_rank_one_final_premium_cap_fail_falls_back(monkeypatch):
    over_cap = _option(bid=4.48, ask=4.50, symbol="SPY260717C00501000")
    valid = _option(bid=3.48, ask=3.50, symbol="SPY260717C00500000")
    _force_rank(monkeypatch, {over_cap["symbol"]: 100.0, valid["symbol"]: 90.0})
    selector = FakeSelector(mode="LIVE", chain=[over_cap, valid])
    result = selector.select(_plan_dict(metadata={"sizing_context": {"budget": 800.0}}))
    assert result is not None
    assert result.contract_symbol == valid["symbol"]
    assert result.candidate_audit["final_candidate_rejections"][0]["reason_code"] == "PREMIUM_CAP_EXCEEDED"


def test_all_candidates_unaffordable_return_truthful_reason_and_no_preselection_mutation(monkeypatch):
    first = _option(bid=4.48, ask=4.50, symbol="SPY260717C00501000")
    second = _option(bid=3.48, ask=3.50, symbol="SPY260717C00500000")
    _force_rank(monkeypatch, {first["symbol"]: 100.0, second["symbol"]: 90.0})
    selector = FakeSelector(mode="LIVE", chain=[first, second])
    plan = _plan_dict(metadata={"sizing_context": {"selector_budget": 200.0, "budget": 500.0}})
    result = selector.select(plan)
    assert result is None
    assert selector.fetch_calls == 1
    assert selector.get_last_failure()["reason_code"] == "UNTRADEABLE_FOR_ACCOUNT_SIZE"
    assert plan.get("contract_symbol") is None
    audit = plan["metadata"]["selector_failure"]["selection_diagnostics"]["final_candidate_rejections"]
    assert [row["reason_code"] for row in audit] == [
        "UNTRADEABLE_FOR_ACCOUNT_SIZE",
        "UNTRADEABLE_FOR_ACCOUNT_SIZE",
    ]


def test_failure_audit_stays_deterministic_for_mixed_final_rejections(monkeypatch):
    low_delta = _option(symbol="SPY260717C00499000")
    low_delta["greeks"] = {"delta": "0.06"}
    over_cap = _option(bid=4.48, ask=4.50, symbol="SPY260717C00501000")
    _force_rank(monkeypatch, {low_delta["symbol"]: 100.0, over_cap["symbol"]: 90.0})
    selector = FakeSelector(mode="LIVE", chain=[low_delta, over_cap])
    selector.target_delta = 0.2
    selector.delta_band = 0.2
    plan = _plan_dict(metadata={"sizing_context": {"budget": 800.0}})
    result = selector.select(plan)
    assert result is None
    failure = plan["metadata"]["selector_failure"]
    assert failure["reason_code"] == "DELTA_OUT_OF_RANGE"
    rejections = failure["selection_diagnostics"]["final_candidate_rejections"]
    assert [row["reason_code"] for row in rejections] == [
        "DELTA_OUT_OF_RANGE",
        "PREMIUM_CAP_EXCEEDED",
    ]


# =============================================================================
# Amendment 1 — clear authoritative last_failure after successful fallback
# =============================================================================
# For each gate type that can cause rank-1 rejection: verify that when a
# later candidate passes, get_last_failure() is None, plan.metadata has no
# selector_failure, and the rank-1 rejection survives in candidate_audit.

def test_rank_one_unaffordable_fallback_clears_last_failure(monkeypatch):
    """Rank-1 unaffordable, rank-2 selected → get_last_failure() is None."""
    expensive = _option(bid=4.48, ask=4.50, symbol="SPY260717C00501000")
    affordable = _option(bid=2.48, ask=2.50, symbol="SPY260717C00500000")
    _force_rank(monkeypatch, {expensive["symbol"]: 100.0, affordable["symbol"]: 90.0})
    selector = FakeSelector(mode="LIVE", chain=[expensive, affordable])
    plan = _plan_dict(metadata={"sizing_context": {"selector_budget": 260.0, "budget": 500.0}})

    result = selector.select(plan)

    assert result is not None, "rank-2 should be selected"
    assert result.contract_symbol == affordable["symbol"]
    # Amendment 1: authoritative failure cleared after fallback selection
    assert selector.get_last_failure() is None, (
        "get_last_failure() must be None after a valid fallback candidate is selected"
    )
    # plan.metadata must not carry a selector_failure from the candidate loop
    assert plan["metadata"].get("selector_failure") is None, (
        "plan.metadata.selector_failure must be absent after successful fallback"
    )
    # Rank-1 rejection is preserved in the winner's candidate_audit
    rejections = result.candidate_audit["final_candidate_rejections"]
    assert len(rejections) >= 1
    assert rejections[0]["reason_code"] == "UNTRADEABLE_FOR_ACCOUNT_SIZE"


def test_cheap_contract_fallback_clears_last_failure(monkeypatch):
    """Rank-1 cheap (flag=false), rank-2 selected → get_last_failure() is None."""
    cheap = _option(bid=0.48, ask=0.49, symbol="SPY260717C00495000")
    valid = _option(bid=2.48, ask=2.50, symbol="SPY260717C00500000")
    _force_rank(monkeypatch, {cheap["symbol"]: 100.0, valid["symbol"]: 90.0})
    selector = FakeSelector(mode="LIVE", chain=[cheap, valid])

    result = selector.select(_plan_dict())

    assert result is not None
    assert result.contract_symbol == valid["symbol"]
    assert selector.get_last_failure() is None, (
        "get_last_failure() must be None after cheap-fallback succeeds"
    )
    assert _plan_dict()["metadata"].get("selector_failure") is None
    rejections = result.candidate_audit["final_candidate_rejections"]
    assert rejections[0]["reason_code"] == "CHEAP_CONTRACT_NO_UPGRADE"


def test_delta_fallback_clears_last_failure(monkeypatch):
    """Rank-1 deep-OTM delta fail, rank-2 selected → get_last_failure() is None."""
    low_delta = _option(symbol="SPY260717C00499000")
    low_delta["greeks"] = {"delta": "0.06"}
    valid = _option(symbol="SPY260717C00500000")
    _force_rank(monkeypatch, {low_delta["symbol"]: 100.0, valid["symbol"]: 90.0})
    selector = FakeSelector(mode="LIVE", chain=[low_delta, valid])
    selector.target_delta = 0.2
    selector.delta_band = 0.2
    plan = _plan_dict()

    result = selector.select(plan)

    assert result is not None
    assert result.contract_symbol == valid["symbol"]
    assert selector.get_last_failure() is None, (
        "get_last_failure() must be None after delta-fallback succeeds"
    )
    assert plan["metadata"].get("selector_failure") is None
    rejections = result.candidate_audit["final_candidate_rejections"]
    assert rejections[0]["reason_code"] == "DELTA_OUT_OF_RANGE"


def test_moneyness_fallback_clears_last_failure(monkeypatch):
    """Rank-1 far-OTM moneyness fail, rank-2 selected → get_last_failure() is None."""
    far_otm = _option(symbol="SPY260717C00580000")
    far_otm["strike"] = 580.0
    valid = _option(symbol="SPY260717C00500000")
    _force_rank(monkeypatch, {far_otm["symbol"]: 100.0, valid["symbol"]: 90.0})
    selector = FakeSelector(mode="LIVE", chain=[far_otm, valid])
    plan = _plan_dict()

    result = selector.select(plan)

    assert result is not None
    assert result.contract_symbol == valid["symbol"]
    assert selector.get_last_failure() is None, (
        "get_last_failure() must be None after moneyness-fallback succeeds"
    )
    assert plan["metadata"].get("selector_failure") is None
    rejections = result.candidate_audit["final_candidate_rejections"]
    assert rejections[0]["contract"] == far_otm["symbol"]


def test_premium_cap_fallback_clears_last_failure(monkeypatch):
    """Rank-1 over per-ticker premium cap, rank-2 selected → get_last_failure() is None."""
    over_cap = _option(bid=4.48, ask=4.50, symbol="SPY260717C00501000")
    valid = _option(bid=3.48, ask=3.50, symbol="SPY260717C00500000")
    _force_rank(monkeypatch, {over_cap["symbol"]: 100.0, valid["symbol"]: 90.0})
    selector = FakeSelector(mode="LIVE", chain=[over_cap, valid])
    plan = _plan_dict(metadata={"sizing_context": {"budget": 800.0}})

    result = selector.select(plan)

    assert result is not None
    assert result.contract_symbol == valid["symbol"]
    assert selector.get_last_failure() is None, (
        "get_last_failure() must be None after premium-cap-fallback succeeds"
    )
    assert plan["metadata"].get("selector_failure") is None
    rejections = result.candidate_audit["final_candidate_rejections"]
    assert rejections[0]["reason_code"] == "PREMIUM_CAP_EXCEEDED"


# =============================================================================
# Amendment 2 — true "cheap contract only choice" semantics
# =============================================================================

def test_cheap_rank1_normal_rank2_flag_true_rank2_wins(monkeypatch, monkeypatch_env=None):
    """
    A: rank-1 cheap, rank-2 normal premium, ALLOW_CHEAP=true → rank-2 wins.
    The cheap candidate must NOT be selected when a normal-premium candidate
    passes all gates.
    """
    cheap = _option(bid=0.48, ask=0.49, symbol="SPY260717C00495000")
    valid = _option(bid=2.48, ask=2.50, symbol="SPY260717C00500000")
    _force_rank(monkeypatch, {cheap["symbol"]: 100.0, valid["symbol"]: 90.0})
    selector = FakeSelector(mode="LIVE", chain=[cheap, valid])
    plan = _plan_dict()

    import os
    original = os.environ.get("ALLOW_CHEAP_CONTRACT_IF_ONLY_CHOICE")
    os.environ["ALLOW_CHEAP_CONTRACT_IF_ONLY_CHOICE"] = "true"
    try:
        result = selector.select(plan)
    finally:
        if original is None:
            os.environ.pop("ALLOW_CHEAP_CONTRACT_IF_ONLY_CHOICE", None)
        else:
            os.environ["ALLOW_CHEAP_CONTRACT_IF_ONLY_CHOICE"] = original

    assert result is not None, "rank-2 should be selected"
    assert result.contract_symbol == valid["symbol"], (
        "rank-2 normal-premium candidate must win over rank-1 cheap candidate"
    )
    assert result.selection_reason != "cheap_contract_only_choice", (
        "selection_reason must NOT be cheap_contract_only_choice when normal candidate exists"
    )
    assert selector.get_last_failure() is None


def test_cheap_rank1_all_later_fail_flag_true_cheap_selected(monkeypatch):
    """
    B: rank-1 cheap and otherwise valid, every later candidate fails,
    flag=true → cheap rank-1 may be selected with selection_reason=cheap_contract_only_choice.
    """
    cheap = _option(bid=0.48, ask=0.49, symbol="SPY260717C00495000")
    # rank-2 fails: way too far OTM (moneyness gate)
    far_otm = _option(symbol="SPY260717C00580000")
    far_otm["strike"] = 580.0
    _force_rank(monkeypatch, {cheap["symbol"]: 100.0, far_otm["symbol"]: 90.0})
    selector = FakeSelector(mode="LIVE", chain=[cheap, far_otm])
    plan = _plan_dict()

    import os
    original = os.environ.get("ALLOW_CHEAP_CONTRACT_IF_ONLY_CHOICE")
    os.environ["ALLOW_CHEAP_CONTRACT_IF_ONLY_CHOICE"] = "true"
    try:
        result = selector.select(plan)
    finally:
        if original is None:
            os.environ.pop("ALLOW_CHEAP_CONTRACT_IF_ONLY_CHOICE", None)
        else:
            os.environ["ALLOW_CHEAP_CONTRACT_IF_ONLY_CHOICE"] = original

    assert result is not None, (
        "cheap candidate should be selected when it is the only valid choice"
    )
    assert result.contract_symbol == cheap["symbol"]
    assert result.selection_reason == "cheap_contract_only_choice", (
        "selection_reason must be cheap_contract_only_choice"
    )
    assert selector.get_last_failure() is None
    assert plan["metadata"].get("selector_failure") is None


def test_cheap_rank1_all_later_fail_flag_false_no_selection(monkeypatch):
    """
    C: same setup as B but ALLOW_CHEAP_CONTRACT_IF_ONLY_CHOICE=false →
    no selection; cheap candidate is rejected outright.
    """
    cheap = _option(bid=0.48, ask=0.49, symbol="SPY260717C00495000")
    far_otm = _option(symbol="SPY260717C00580000")
    far_otm["strike"] = 580.0
    _force_rank(monkeypatch, {cheap["symbol"]: 100.0, far_otm["symbol"]: 90.0})
    selector = FakeSelector(mode="LIVE", chain=[cheap, far_otm])
    plan = _plan_dict()

    import os
    original = os.environ.get("ALLOW_CHEAP_CONTRACT_IF_ONLY_CHOICE")
    os.environ["ALLOW_CHEAP_CONTRACT_IF_ONLY_CHOICE"] = "false"
    try:
        result = selector.select(plan)
    finally:
        if original is None:
            os.environ.pop("ALLOW_CHEAP_CONTRACT_IF_ONLY_CHOICE", None)
        else:
            os.environ["ALLOW_CHEAP_CONTRACT_IF_ONLY_CHOICE"] = original

    assert result is None, (
        "no selection expected when ALLOW_CHEAP_CONTRACT_IF_ONLY_CHOICE=false"
    )
    # Cheap candidate must appear in rejections with CHEAP_CONTRACT_NO_UPGRADE
    failure = plan["metadata"]["selector_failure"]
    rejections = failure["selection_diagnostics"]["final_candidate_rejections"]
    assert any(r["reason_code"] == "CHEAP_CONTRACT_NO_UPGRADE" for r in rejections)


# =============================================================================
# Amendment 3 — final failure is authoritative and deterministic
# =============================================================================

def test_final_rejection_counts_present_in_failure_diagnostics(monkeypatch):
    """All-fail scenario must include final_rejection_counts in selection_diagnostics."""
    first = _option(bid=4.48, ask=4.50, symbol="SPY260717C00501000")
    second = _option(bid=3.48, ask=3.50, symbol="SPY260717C00500000")
    _force_rank(monkeypatch, {first["symbol"]: 100.0, second["symbol"]: 90.0})
    selector = FakeSelector(mode="LIVE", chain=[first, second])
    plan = _plan_dict(metadata={"sizing_context": {"selector_budget": 200.0, "budget": 500.0}})

    result = selector.select(plan)

    assert result is None
    diag = plan["metadata"]["selector_failure"]["selection_diagnostics"]
    assert "final_rejection_counts" in diag, (
        "final_rejection_counts must be present in selection_diagnostics"
    )
    counts = diag["final_rejection_counts"]
    assert isinstance(counts, dict)
    assert counts.get("UNTRADEABLE_FOR_ACCOUNT_SIZE", 0) == 2


def test_final_reason_uses_precedence_not_last_evaluated(monkeypatch):
    """
    Deterministic precedence: DELTA_OUT_OF_RANGE (rank-1) vs PREMIUM_CAP_EXCEEDED
    (rank-2), each appearing once — DELTA_OUT_OF_RANGE wins by fixed precedence,
    not by which candidate was evaluated last.
    """
    low_delta = _option(symbol="SPY260717C00499000")
    low_delta["greeks"] = {"delta": "0.06"}
    over_cap = _option(bid=4.48, ask=4.50, symbol="SPY260717C00501000")
    _force_rank(monkeypatch, {low_delta["symbol"]: 100.0, over_cap["symbol"]: 90.0})
    selector = FakeSelector(mode="LIVE", chain=[low_delta, over_cap])
    selector.target_delta = 0.2
    selector.delta_band = 0.2
    plan = _plan_dict(metadata={"sizing_context": {"budget": 800.0}})

    result = selector.select(plan)

    assert result is None
    failure = plan["metadata"]["selector_failure"]
    # DELTA_OUT_OF_RANGE must win over PREMIUM_CAP_EXCEEDED by fixed precedence
    assert failure["reason_code"] == "DELTA_OUT_OF_RANGE"
    diag = failure["selection_diagnostics"]
    counts = diag["final_rejection_counts"]
    assert counts == {"DELTA_OUT_OF_RANGE": 1, "PREMIUM_CAP_EXCEEDED": 1}


def test_final_reason_uses_most_common_when_counts_differ(monkeypatch):
    """
    Most-common-count policy: if one reason appears more than any other it wins
    regardless of its position in the precedence list.
    Two UNTRADEABLE + one DELTA_OUT_OF_RANGE → UNTRADEABLE wins by count.
    """
    unaffordable_a = _option(bid=4.48, ask=4.50, symbol="SPY260717C00501000")
    unaffordable_b = _option(bid=4.0, ask=4.2, symbol="SPY260717C00502000")
    low_delta = _option(symbol="SPY260717C00499000")
    low_delta["greeks"] = {"delta": "0.06"}
    _force_rank(monkeypatch, {
        unaffordable_a["symbol"]: 100.0,
        unaffordable_b["symbol"]: 95.0,
        low_delta["symbol"]: 90.0,
    })
    selector = FakeSelector(mode="LIVE", chain=[unaffordable_a, unaffordable_b, low_delta])
    selector.target_delta = 0.2
    selector.delta_band = 0.2
    # selector_budget=350: unaffordable_a ($450) and unaffordable_b ($420) each
    # floor(350 / cost) = 0  → UNTRADEABLE_FOR_ACCOUNT_SIZE (×2).
    # low_delta ($250) is affordable at 1 contract but fails the delta gate → DELTA_OUT_OF_RANGE (×1).
    # Count policy: UNTRADEABLE (2) > DELTA (1) → UNTRADEABLE wins.
    plan = _plan_dict(metadata={"sizing_context": {"selector_budget": 350.0, "budget": 500.0}})

    result = selector.select(plan)

    assert result is None
    failure = plan["metadata"]["selector_failure"]
    assert failure["reason_code"] == "UNTRADEABLE_FOR_ACCOUNT_SIZE"
    counts = failure["selection_diagnostics"]["final_rejection_counts"]
    assert counts.get("UNTRADEABLE_FOR_ACCOUNT_SIZE", 0) == 2
    assert counts.get("DELTA_OUT_OF_RANGE", 0) == 1
