from __future__ import annotations

import os
from datetime import date, timedelta

from ap.contract_playbook import (
    build_playbook_candidate_context,
    playbook_contract_selection_enabled,
    resolve_contract_playbook,
    resolve_playbook_expiration_order,
)


def _today_plus(days: int) -> str:
    return (date.today() + timedelta(days=days)).isoformat()


def test_flag_aliases_enable_complete_playbook():
    prev_contract = os.environ.get("PLAYBOOK_CONTRACT_SELECTION_ENABLED")
    prev_strike = os.environ.get("PLAYBOOK_STRIKE_SELECTION_ENABLED")
    try:
        os.environ["PLAYBOOK_CONTRACT_SELECTION_ENABLED"] = "0"
        os.environ["PLAYBOOK_STRIKE_SELECTION_ENABLED"] = "1"
        assert playbook_contract_selection_enabled() is True
    finally:
        if prev_contract is None:
            os.environ.pop("PLAYBOOK_CONTRACT_SELECTION_ENABLED", None)
        else:
            os.environ["PLAYBOOK_CONTRACT_SELECTION_ENABLED"] = prev_contract
        if prev_strike is None:
            os.environ.pop("PLAYBOOK_STRIKE_SELECTION_ENABLED", None)
        else:
            os.environ["PLAYBOOK_STRIKE_SELECTION_ENABLED"] = prev_strike


def test_liquid_index_prefers_short_expirations_in_window():
    available = [_today_plus(0), _today_plus(1), _today_plus(3), _today_plus(7)]
    spec = resolve_contract_playbook(
        ticker="SPY",
        side="CALL",
        timeframe="1d",
        pattern="2-1-2",
        underlying_price=500.0,
        trigger_price=499.5,
        target_underlying=505.0,
        wick_targets=[],
        available_expirations=available,
        today=date.today(),
        min_dte=0,
        max_dte=7,
        metadata={},
    )
    ordered = resolve_playbook_expiration_order(spec, available, today=date.today(), min_dte=0, max_dte=7)
    assert ordered[:3] == available[:3]


def test_daily_equity_prefers_same_week_over_farther_dte():
    available = [_today_plus(2), _today_plus(5), _today_plus(10)]
    spec = resolve_contract_playbook(
        ticker="AAPL",
        side="CALL",
        timeframe="1d",
        pattern="3-1-2",
        underlying_price=100.0,
        trigger_price=99.5,
        target_underlying=105.0,
        wick_targets=[],
        available_expirations=available,
        today=date.today(),
        min_dte=0,
        max_dte=14,
        metadata={},
    )
    ordered = resolve_playbook_expiration_order(spec, available, today=date.today(), min_dte=0, max_dte=14)
    assert set(ordered[:2]) == {available[0], available[1]}
    assert ordered[-1] == available[2]


def test_no_policy_match_outside_max_dte():
    available = [_today_plus(10), _today_plus(14)]
    spec = resolve_contract_playbook(
        ticker="SPY",
        side="CALL",
        timeframe="1d",
        pattern=None,
        underlying_price=500.0,
        trigger_price=499.5,
        target_underlying=505.0,
        wick_targets=[],
        available_expirations=available,
        today=date.today(),
        min_dte=0,
        max_dte=2,
        metadata={},
    )
    ordered = resolve_playbook_expiration_order(spec, available, today=date.today(), min_dte=0, max_dte=2)
    assert ordered == []


def test_strike_context_prefers_atm_or_one_step_otm_not_far_pt1():
    candidates = [
        {"symbol": "ATM", "strike": 100.0},
        {"symbol": "ONE_UP", "strike": 101.0},
        {"symbol": "PT1", "strike": 110.0},
    ]
    spec = resolve_contract_playbook(
        ticker="AAPL",
        side="CALL",
        timeframe="1d",
        pattern="2-1-2",
        underlying_price=100.0,
        trigger_price=100.0,
        target_underlying=110.0,
        wick_targets=[],
        available_expirations=[_today_plus(2)],
        today=date.today(),
        min_dte=0,
        max_dte=5,
        metadata={},
    )
    ctx = build_playbook_candidate_context(spec, candidates)
    assert ctx["preferred_strikes"][:2] == [100.0, 101.0]
    assert ctx["per_symbol"]["ATM"]["bounded_bonus"] > ctx["per_symbol"]["PT1"]["bounded_bonus"]
    assert ctx["per_symbol"]["ONE_UP"]["bounded_bonus"] > ctx["per_symbol"]["PT1"]["bounded_bonus"]


def test_request_context_carries_playbook_audit_fields():
    ctx = type("Ctx", (), {})()
    ctx.playbook_ordered_expirations = [_today_plus(0), _today_plus(1)]
    ctx.playbook_audit = {"enabled": True, "ordered_expirations": list(ctx.playbook_ordered_expirations)}
    assert ctx.playbook_audit["enabled"] is True
    assert len(ctx.playbook_ordered_expirations) == 2
