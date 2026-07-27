from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from ap import touched_profit_confirmation_guard as guard


@dataclass
class FakeDecision:
    action: str
    quantity: int
    reason: str
    urgency: str
    pnl_pct: float
    reason_code: str = ""


def _position(**overrides):
    now = datetime.now(timezone.utc)
    values = {
        "ticker": "NVDA",
        "execution_mode": "live",
        "quantity_remaining": 2,
        "scale_outs_done": 0,
        "current_bid": 0.72,
        "peak_pnl_pct": 0.0588,
        "max_profit_seen": 0.0588,
        "last_option_bid_update_ts": now,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _decision(code: str, *, action: str = "CLOSE_ALL", pnl: float = -0.1529):
    return FakeDecision(
        action=action,
        quantity=2 if action != "HOLD" else 0,
        reason=code,
        urgency="IMMEDIATE",
        pnl_pct=pnl,
        reason_code=code,
    )


def _wrapped(sequence):
    decisions = iter(sequence)

    def original(_pos, now_et=None):
        return next(decisions)

    return guard.wrap_evaluate_exit(
        original,
        exit_decision_cls=FakeDecision,
        classify_decision=lambda decision: decision.reason_code,
    )


@pytest.fixture(autouse=True)
def _policy_on(monkeypatch):
    monkeypatch.setenv("TOUCHED_PROFIT_BREACH_CONFIRMATION_ENABLED", "1")
    monkeypatch.setenv("TOUCHED_PROFIT_BREACH_CONFIRMATIONS", "2")


def test_nvda_shape_first_transient_bid_is_held():
    pos = _position()
    wrapped = _wrapped([_decision("TOUCHED_PROFIT_STOP")])

    result = wrapped(pos)

    assert result.action == "HOLD"
    assert result.quantity == 0
    assert result.reason_code == "TOUCHED_PROFIT_STOP_CONFIRMING"
    assert pos._touched_profit_floor_breach_count == 1


def test_repeated_evaluation_of_same_bid_does_not_count_twice():
    pos = _position()
    wrapped = _wrapped([
        _decision("TOUCHED_PROFIT_STOP"),
        _decision("TOUCHED_PROFIT_STOP"),
    ])

    first = wrapped(pos)
    second = wrapped(pos)

    assert first.reason_code == "TOUCHED_PROFIT_STOP_CONFIRMING"
    assert second.reason_code == "TOUCHED_PROFIT_STOP_CONFIRMING"
    assert pos._touched_profit_floor_breach_count == 1


def test_second_distinct_bid_confirms_sustained_floor_breach():
    first_ts = datetime.now(timezone.utc)
    pos = _position(last_option_bid_update_ts=first_ts)
    original_decision = _decision("TOUCHED_PROFIT_STOP")
    wrapped = _wrapped([original_decision, original_decision])

    held = wrapped(pos)
    pos.last_option_bid_update_ts = first_ts + timedelta(seconds=2)
    pos.current_bid = 0.71
    confirmed = wrapped(pos)

    assert held.reason_code == "TOUCHED_PROFIT_STOP_CONFIRMING"
    assert confirmed is original_decision
    assert confirmed.action == "CLOSE_ALL"
    assert confirmed.reason_code == "TOUCHED_PROFIT_STOP"
    assert pos._touched_profit_floor_breach_count == 0


def test_quote_recovery_resets_confirmation_sequence():
    first_ts = datetime.now(timezone.utc)
    pos = _position(last_option_bid_update_ts=first_ts)
    wrapped = _wrapped([
        _decision("TOUCHED_PROFIT_STOP"),
        _decision("UNKNOWN_EXIT", action="HOLD", pnl=0.0471),
        _decision("TOUCHED_PROFIT_STOP"),
    ])

    first = wrapped(pos)
    pos.last_option_bid_update_ts = first_ts + timedelta(seconds=2)
    recovered = wrapped(pos)
    pos.last_option_bid_update_ts = first_ts + timedelta(seconds=4)
    later = wrapped(pos)

    assert first.reason_code == "TOUCHED_PROFIT_STOP_CONFIRMING"
    assert recovered.reason_code == "UNKNOWN_EXIT"
    assert later.reason_code == "TOUCHED_PROFIT_STOP_CONFIRMING"
    assert pos._touched_profit_floor_breach_count == 1


def test_qqq_runner_trail_passes_through_unchanged():
    pos = _position(
        ticker="QQQ",
        quantity_remaining=1,
        current_bid=1.67,
        peak_pnl_pct=0.525,
        max_profit_seen=0.525,
    )
    runner = _decision("RUNNER_TRAIL", pnl=0.3917)
    wrapped = _wrapped([runner])

    result = wrapped(pos)

    assert result is runner
    assert result.action == "CLOSE_ALL"
    assert pos._touched_profit_floor_breach_count == 0


@pytest.mark.parametrize(
    "code",
    ["HARD_STOP", "STOP_HIT", "EOD_FORCE_CLOSE", "TARGET_HIT"],
)
def test_immediate_money_safety_exits_are_never_delayed(code):
    pos = _position()
    decision = _decision(code, pnl=-0.40)
    wrapped = _wrapped([decision])

    result = wrapped(pos)

    assert result is decision
    assert result.action == "CLOSE_ALL"


def test_paper_touched_profit_behavior_is_unchanged():
    pos = _position(execution_mode="paper")
    decision = _decision("TOUCHED_PROFIT_STOP")
    wrapped = _wrapped([decision])

    result = wrapped(pos)

    assert result is decision


def test_feature_off_returns_original_decision(monkeypatch):
    monkeypatch.setenv("TOUCHED_PROFIT_BREACH_CONFIRMATION_ENABLED", "0")
    pos = _position()
    decision = _decision("TOUCHED_PROFIT_STOP")
    wrapped = _wrapped([decision])

    result = wrapped(pos)

    assert result is decision


@pytest.mark.parametrize(
    "raw, expected",
    [("bad", 2), ("1", 2), ("2", 2), ("3", 3), ("99", 4)],
)
def test_confirmation_count_is_bounded(monkeypatch, raw, expected):
    monkeypatch.setenv("TOUCHED_PROFIT_BREACH_CONFIRMATIONS", raw)
    assert guard._required_confirmations() == expected


def test_lifecycle_manifest_marks_guard_required():
    from ap.trade_lifecycle_guards import _GUARDS

    rows = {name: (module, installer, required) for name, module, installer, required in _GUARDS}
    assert rows["touched_profit_bid_confirmation"] == (
        "ap.touched_profit_confirmation_guard",
        "install_touched_profit_confirmation_guard",
        True,
    )
