from __future__ import annotations

from types import SimpleNamespace

import ap.one_contract_exit_guard as guard


class Decision:
    def __init__(
        self,
        *,
        action: str,
        quantity: int,
        reason: str,
        urgency: str = "NORMAL",
        pnl_pct: float = 0.0,
        reason_code: str = "",
    ):
        self.action = action
        self.quantity = quantity
        self.reason = reason
        self.urgency = urgency
        self.pnl_pct = pnl_pct
        self.reason_code = reason_code


def _pos(**overrides):
    values = {
        "execution_mode": "live",
        "ticker": "SPY",
        "quantity_remaining": 1,
        "scale_outs_done": 0,
        "option_pnl_pct": 0.02,
        "peak_pnl_pct": 0.06,
        "max_profit_seen": 0.06,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _touched_stop(*, pnl: float = 0.02) -> Decision:
    return Decision(
        action="CLOSE_ALL",
        quantity=1,
        reason="TOUCHED PROFIT STOP — peaked +6% now +2% — floor=3%",
        urgency="IMMEDIATE",
        pnl_pct=pnl,
        reason_code="TOUCHED_PROFIT_STOP",
    )


def test_jason_spy_shape_holds_early_green_single_contract() -> None:
    assert guard.should_hold_early_green_one_contract(
        _pos(),
        _touched_stop(),
        decision_code="TOUCHED_PROFIT_STOP",
        runner_arm_pct=0.12,
    ) is True


def test_breakeven_or_red_touched_profit_close_is_preserved() -> None:
    for current_pnl in (0.0, -0.01):
        assert guard.should_hold_early_green_one_contract(
            _pos(option_pnl_pct=current_pnl),
            _touched_stop(pnl=current_pnl),
            decision_code="TOUCHED_PROFIT_STOP",
            runner_arm_pct=0.12,
        ) is False


def test_runner_armed_peak_uses_original_exit_logic() -> None:
    assert guard.should_hold_early_green_one_contract(
        _pos(peak_pnl_pct=0.12, max_profit_seen=0.12),
        _touched_stop(),
        decision_code="TOUCHED_PROFIT_STOP",
        runner_arm_pct=0.12,
    ) is False


def test_paper_and_multi_contract_paths_are_unchanged() -> None:
    assert guard.should_hold_early_green_one_contract(
        _pos(execution_mode="paper"),
        _touched_stop(),
        decision_code="TOUCHED_PROFIT_STOP",
        runner_arm_pct=0.12,
    ) is False
    assert guard.should_hold_early_green_one_contract(
        _pos(quantity_remaining=14),
        _touched_stop(),
        decision_code="TOUCHED_PROFIT_STOP",
        runner_arm_pct=0.12,
    ) is False


def test_stop_target_and_eod_decisions_are_never_suppressed() -> None:
    for code, reason in (
        ("STOP_HIT", "STOP HIT — underlying breached"),
        ("HARD_STOP", "HARD STOP — option down 33%"),
        ("EOD_FORCE_CLOSE", "EOD FORCE CLOSE — past 15:50"),
        ("TARGET_HIT", "TARGET HIT — underlying reached target"),
    ):
        decision = Decision(
            action="CLOSE_ALL",
            quantity=1,
            reason=reason,
            urgency="IMMEDIATE",
            pnl_pct=0.02,
            reason_code=code,
        )
        assert guard.should_hold_early_green_one_contract(
            _pos(),
            decision,
            decision_code=code,
            runner_arm_pct=0.12,
        ) is False


def test_wrapper_converts_only_jason_shape_to_hold(monkeypatch) -> None:
    monkeypatch.setenv("LIVE_SINGLE_CONTRACT_RUNNER_PRECEDENCE_ENABLED", "1")
    wrapped = guard.wrap_evaluate_exit(
        lambda pos, now_et=None: _touched_stop(),
        exit_decision_cls=Decision,
        classify_decision=lambda decision: decision.reason_code,
    )

    result = wrapped(_pos())
    assert result.action == "HOLD"
    assert result.quantity == 0
    assert result.reason_code == "SINGLE_CONTRACT_EARLY_GREEN_HOLD"
    assert "waiting for +12% runner arm" in result.reason


def test_wrapper_preserves_original_stop_object(monkeypatch) -> None:
    monkeypatch.setenv("LIVE_SINGLE_CONTRACT_RUNNER_PRECEDENCE_ENABLED", "1")
    stop = Decision(
        action="CLOSE_ALL",
        quantity=1,
        reason="EOD FORCE CLOSE",
        urgency="IMMEDIATE",
        pnl_pct=0.02,
        reason_code="EOD_FORCE_CLOSE",
    )
    wrapped = guard.wrap_evaluate_exit(
        lambda pos, now_et=None: stop,
        exit_decision_cls=Decision,
        classify_decision=lambda decision: decision.reason_code,
    )

    assert wrapped(_pos()) is stop


def test_wrapper_preserves_touched_profit_close_at_breakeven(monkeypatch) -> None:
    monkeypatch.setenv("LIVE_SINGLE_CONTRACT_RUNNER_PRECEDENCE_ENABLED", "1")
    close = _touched_stop(pnl=0.0)
    wrapped = guard.wrap_evaluate_exit(
        lambda pos, now_et=None: close,
        exit_decision_cls=Decision,
        classify_decision=lambda decision: decision.reason_code,
    )

    assert wrapped(_pos(option_pnl_pct=0.0)) is close


def test_live_policy_change_is_default_off(monkeypatch) -> None:
    monkeypatch.delenv("LIVE_SINGLE_CONTRACT_RUNNER_PRECEDENCE_ENABLED", raising=False)
    close = _touched_stop()
    wrapped = guard.wrap_evaluate_exit(
        lambda pos, now_et=None: close,
        exit_decision_cls=Decision,
        classify_decision=lambda decision: decision.reason_code,
    )

    assert guard._enabled() is False
    assert wrapped(_pos()) is close


def test_runner_arm_env_is_bounded(monkeypatch) -> None:
    monkeypatch.setenv("SINGLE_CONTRACT_RUNNER_ARM_PCT", "0.01")
    assert guard._runner_arm_pct() == 0.05
    monkeypatch.setenv("SINGLE_CONTRACT_RUNNER_ARM_PCT", "0.90")
    assert guard._runner_arm_pct() == 0.30
    monkeypatch.setenv("SINGLE_CONTRACT_RUNNER_ARM_PCT", "bad")
    assert guard._runner_arm_pct() == 0.12
