"""P0 — LIVE one-contract runner-precedence guard.

Tests for ``ap.one_contract_exit_guard``.

Coverage requirements (from PR spec):
 1. SPY/index 0DTE: peak +14%, current positive → HOLD (canonical arm = +20%).
 2. Equity 0DTE: below +22% → HOLD.
 3. 1–2 DTE: below +25% → HOLD.
 4. Standard DTE: at or above +12% → original decision preserved.
 5. Exact threshold boundary (peak == arm) → original decision preserved.
 6. PAPER, unknown mode, multi-contract, scaled, red, breakeven, hard-stop,
    target, EOD, underlying-stop → original decision object returned unchanged.
 7. Real ``ap_exit_engine.ExitDecision`` class used throughout.
 8. Real ``_effective_thresholds()`` resolver exercised, not hard-coded values.
 9. Replacement HOLD is non-actionable via real ``_classify_exit_decision``.
10. Disabled / malformed / unset / false flag preserves behavior object-for-object.
"""
from __future__ import annotations

import os
from types import SimpleNamespace
from typing import Any

import pytest

import ap.one_contract_exit_guard as guard
import ap_exit_engine as engine_module
from ap_exit_engine import ExitDecision, _classify_exit_decision, _effective_thresholds


# ── Dynamic OCC date strings (date-stable across CI calendar days) ─────────────
# _TEST_TODAY is captured once at import time using the production trading-day
# timezone (America/New_York) so tests behave identically to the live bot and
# are immune to midnight rollovers during test collection.
from datetime import datetime as _datetime_cls, timedelta as _timedelta
from zoneinfo import ZoneInfo as _ZoneInfo

_TEST_TODAY = _datetime_cls.now(_ZoneInfo("America/New_York")).date()

def _expiry(days: int) -> str:
    """Return a YYMMDD OCC expiry date string relative to today in ET."""
    return (_TEST_TODAY + _timedelta(days=days)).strftime("%y%m%d")

_D_0DTE = _expiry(0)   # 0DTE  → arm 0.20 (index) or 0.22 (equity)
_D_1DTE = _expiry(1)   # 1DTE  → arm 0.25
_D_2DTE = _expiry(2)   # 2DTE  → arm 0.25
_D_STD  = _expiry(14)  # standard (14d) → arm 0.12

_SPY_0DTE  = f"SPY{_D_0DTE}C00600000"
_AAPL_0DTE = f"AAPL{_D_0DTE}C00200000"
_SPY_1DTE  = f"SPY{_D_1DTE}C00600000"
_AAPL_2DTE = f"AAPL{_D_2DTE}C00200000"
_SPY_STD   = f"SPY{_D_STD}C00600000"
# ──────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# Position factories using real option symbols so _effective_thresholds resolves
# the canonical DTE/instrument threshold, not a test-hard-coded value.
# ─────────────────────────────────────────────────────────────────────────────

def _pos(
    *,
    execution_mode: str = "live",
    option_symbol: str = _SPY_0DTE,  # 0DTE SPY → arm=0.20
    ticker: str = "SPY",
    quantity_remaining: int = 1,
    scale_outs_done: int = 0,
    option_pnl_pct: float = 0.05,
    peak_pnl_pct: float = 0.14,
    max_profit_seen: float = 0.14,
    **extra: Any,
) -> SimpleNamespace:
    return SimpleNamespace(
        execution_mode=execution_mode,
        option_symbol=option_symbol,
        ticker=ticker,
        quantity_remaining=quantity_remaining,
        scale_outs_done=scale_outs_done,
        option_pnl_pct=option_pnl_pct,
        peak_pnl_pct=peak_pnl_pct,
        max_profit_seen=max_profit_seen,
        **extra,
    )


def _touched_stop(pnl: float = 0.05) -> ExitDecision:
    return ExitDecision(
        action="CLOSE_ALL",
        quantity=1,
        reason="TOUCHED PROFIT STOP — peaked +14% now +5% — floor=3%",
        urgency="IMMEDIATE",
        pnl_pct=pnl,
        reason_code="TOUCHED_PROFIT_STOP",
    )


def _resolved_arm(pos: SimpleNamespace) -> float:
    """Return the canonical runner-arm pct from the real exit engine."""
    _, arm, _ = _effective_thresholds(pos)
    return float(arm)


# ─────────────────────────────────────────────────────────────────────────────
# 1. SPY / index 0DTE — peak +14%, arm = +20% → HOLD
# ─────────────────────────────────────────────────────────────────────────────

def test_spy_0dte_peak_14_pct_holds_with_canonical_20pct_arm() -> None:
    """Requirement 1: SPY 0DTE → canonical arm +20%; peak +14% → HOLD."""
    pos = _pos(option_symbol=_SPY_0DTE, ticker="SPY",
               peak_pnl_pct=0.14, max_profit_seen=0.14, option_pnl_pct=0.05)
    arm = _resolved_arm(pos)
    assert arm == pytest.approx(0.20), f"Expected 0DTE SPY arm=0.20, got {arm}"
    assert guard.should_hold_early_green_one_contract(
        pos, _touched_stop(), decision_code="TOUCHED_PROFIT_STOP", runner_arm_pct=arm
    ) is True


def test_spy_0dte_canonical_arm_is_exactly_20_pct() -> None:
    """Regression: canonical threshold resolver must return 0.20 for 0DTE SPY."""
    pos = _pos(option_symbol=_SPY_0DTE, ticker="SPY")
    hard, arm, lock = _effective_thresholds(pos)
    assert arm == pytest.approx(0.20)
    assert hard == pytest.approx(-0.18)


# ─────────────────────────────────────────────────────────────────────────────
# 2. Equity 0DTE — below +22% → HOLD
# ─────────────────────────────────────────────────────────────────────────────

def test_equity_0dte_below_22pct_holds() -> None:
    """Requirement 2: AAPL 0DTE → canonical arm +22%; peak +18% → HOLD."""
    pos = _pos(option_symbol=_AAPL_0DTE, ticker="AAPL",
               peak_pnl_pct=0.18, max_profit_seen=0.18, option_pnl_pct=0.06)
    arm = _resolved_arm(pos)
    assert arm == pytest.approx(0.22), f"Expected 0DTE equity arm=0.22, got {arm}"
    assert guard.should_hold_early_green_one_contract(
        pos, _touched_stop(), decision_code="TOUCHED_PROFIT_STOP", runner_arm_pct=arm
    ) is True


# ─────────────────────────────────────────────────────────────────────────────
# 3. 1–2 DTE — below +25% → HOLD
# ─────────────────────────────────────────────────────────────────────────────

def test_one_dte_spy_below_25pct_holds() -> None:
    """Requirement 3: SPY 1DTE → canonical arm +25%; peak +20% → HOLD."""
    pos = _pos(option_symbol=_SPY_1DTE, ticker="SPY",
               peak_pnl_pct=0.20, max_profit_seen=0.20, option_pnl_pct=0.07)
    arm = _resolved_arm(pos)
    assert arm == pytest.approx(0.25), f"Expected 1DTE arm=0.25, got {arm}"
    assert guard.should_hold_early_green_one_contract(
        pos, _touched_stop(), decision_code="TOUCHED_PROFIT_STOP", runner_arm_pct=arm
    ) is True


def test_two_dte_equity_below_25pct_holds() -> None:
    """Requirement 3 (DTE=2): canonical arm +25%; peak +18% → HOLD."""
    pos = _pos(option_symbol=_AAPL_2DTE, ticker="AAPL",
               peak_pnl_pct=0.18, max_profit_seen=0.18, option_pnl_pct=0.05)
    arm = _resolved_arm(pos)
    assert arm == pytest.approx(0.25)
    assert guard.should_hold_early_green_one_contract(
        pos, _touched_stop(), decision_code="TOUCHED_PROFIT_STOP", runner_arm_pct=arm
    ) is True


# ─────────────────────────────────────────────────────────────────────────────
# 4. Standard DTE — at or above +12% → original decision preserved
# ─────────────────────────────────────────────────────────────────────────────

def test_standard_dte_at_12pct_preserves_original() -> None:
    """Requirement 4: standard DTE arm=0.12; peak at/above 0.12 → no hold."""
    pos = _pos(option_symbol=_SPY_STD, ticker="SPY",
               peak_pnl_pct=0.12, max_profit_seen=0.12, option_pnl_pct=0.04)
    arm = _resolved_arm(pos)
    assert arm == pytest.approx(0.12)
    assert guard.should_hold_early_green_one_contract(
        pos, _touched_stop(), decision_code="TOUCHED_PROFIT_STOP", runner_arm_pct=arm
    ) is False


# ─────────────────────────────────────────────────────────────────────────────
# 5. Exact threshold boundary — peak == arm → original decision preserved
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("symbol,ticker,expected_arm", [
    (_SPY_0DTE, "SPY",  0.20),   # 0DTE index
    (_AAPL_0DTE, "AAPL", 0.22),  # 0DTE equity
    (_SPY_1DTE, "SPY",  0.25),   # 1DTE
    (_SPY_STD, "SPY",  0.12),   # standard
])
def test_exact_boundary_preserves_original(symbol: str, ticker: str, expected_arm: float) -> None:
    """Requirement 5: peak exactly at the canonical arm → NOT held (runner fires)."""
    pos = _pos(option_symbol=symbol, ticker=ticker,
               peak_pnl_pct=expected_arm, max_profit_seen=expected_arm,
               option_pnl_pct=0.03)
    arm = _resolved_arm(pos)
    assert arm == pytest.approx(expected_arm), f"Resolver returned {arm}, expected {expected_arm}"
    assert guard.should_hold_early_green_one_contract(
        pos, _touched_stop(), decision_code="TOUCHED_PROFIT_STOP", runner_arm_pct=arm
    ) is False, f"Peak exactly at arm={arm} should NOT be suppressed"


# ─────────────────────────────────────────────────────────────────────────────
# 6. All "never suppress" paths return the original decision object unchanged
# ─────────────────────────────────────────────────────────────────────────────

def _never_suppress_decisions() -> list[tuple[str, ExitDecision]]:
    return [
        ("hard_stop", ExitDecision(
            action="CLOSE_ALL", quantity=1,
            reason="HARD STOP HIT", urgency="IMMEDIATE",
            pnl_pct=-0.18, reason_code="HARD_STOP")),
        ("underlying_stop", ExitDecision(
            action="CLOSE_ALL", quantity=1,
            reason="STOP HIT underlying fell below stop", urgency="IMMEDIATE",
            pnl_pct=-0.05, reason_code="STOP_HIT")),
        ("eod_close", ExitDecision(
            action="CLOSE_ALL", quantity=1,
            reason="EOD FORCE CLOSE", urgency="IMMEDIATE",
            pnl_pct=0.03, reason_code="EOD_FORCE_CLOSE")),
        ("target_exit", ExitDecision(
            action="CLOSE_ALL", quantity=1,
            reason="TARGET HIT", urgency="IMMEDIATE",
            pnl_pct=0.25, reason_code="TARGET_HIT")),
        ("theta_stop", ExitDecision(
            action="CLOSE_ALL", quantity=1,
            reason="THETA STOP", urgency="HIGH",
            pnl_pct=-0.10, reason_code="THETA_STOP")),
        ("runner_trail", ExitDecision(
            action="CLOSE_ALL", quantity=1,
            reason="RUNNER TRAIL STOP", urgency="IMMEDIATE",
            pnl_pct=0.15, reason_code="RUNNER_TRAIL")),
        ("profit_lock", ExitDecision(
            action="CLOSE_ALL", quantity=1,
            reason="PROFIT LOCK triggered", urgency="NORMAL",
            pnl_pct=0.09, reason_code="PROFIT_LOCK")),
        ("hold_do_nothing", ExitDecision(
            action="HOLD", quantity=0,
            reason="holding", urgency="NORMAL",
            pnl_pct=0.04, reason_code="")),
    ]


@pytest.mark.parametrize("label,decision", _never_suppress_decisions())
def test_non_targeted_decisions_never_suppressed(label: str, decision: ExitDecision) -> None:
    """Requirement 6: non-TOUCHED_PROFIT_STOP decisions pass through untouched."""
    pos = _pos(option_symbol=_SPY_0DTE, ticker="SPY",
               peak_pnl_pct=0.10, max_profit_seen=0.10, option_pnl_pct=0.05)
    arm = _resolved_arm(pos)
    assert not guard.should_hold_early_green_one_contract(
        pos, decision,
        decision_code=_classify_exit_decision(decision),
        runner_arm_pct=arm,
    ), f"Non-targeted decision '{label}' should never be suppressed"


def test_paper_mode_never_suppressed() -> None:
    """Requirement 6: PAPER mode → original decision unchanged."""
    pos = _pos(execution_mode="paper", option_symbol=_SPY_0DTE,
               peak_pnl_pct=0.10, option_pnl_pct=0.05)
    arm = _resolved_arm(pos)
    assert guard.should_hold_early_green_one_contract(
        pos, _touched_stop(), decision_code="TOUCHED_PROFIT_STOP", runner_arm_pct=arm
    ) is False


def test_unknown_mode_never_suppressed() -> None:
    """Requirement 6: unknown / blank mode → original decision unchanged."""
    for mode in ("", "unknown", "sandbox", "broker_repair", "None", "PAPER"):
        pos = _pos(execution_mode=mode, peak_pnl_pct=0.10, option_pnl_pct=0.05)
        arm = _resolved_arm(pos)
        result = guard.should_hold_early_green_one_contract(
            pos, _touched_stop(), decision_code="TOUCHED_PROFIT_STOP", runner_arm_pct=arm
        )
        assert result is False, f"mode={mode!r} should never be suppressed"


def test_multi_contract_never_suppressed() -> None:
    """Requirement 6: qty_remaining > 1 → original decision unchanged."""
    pos = _pos(quantity_remaining=2, peak_pnl_pct=0.10, option_pnl_pct=0.05)
    arm = _resolved_arm(pos)
    assert guard.should_hold_early_green_one_contract(
        pos, _touched_stop(), decision_code="TOUCHED_PROFIT_STOP", runner_arm_pct=arm
    ) is False


def test_scaled_position_never_suppressed() -> None:
    """Requirement 6: scale_outs_done > 0 → original decision unchanged."""
    pos = _pos(scale_outs_done=1, peak_pnl_pct=0.10, option_pnl_pct=0.05)
    arm = _resolved_arm(pos)
    assert guard.should_hold_early_green_one_contract(
        pos, _touched_stop(), decision_code="TOUCHED_PROFIT_STOP", runner_arm_pct=arm
    ) is False


def test_red_pnl_touched_profit_preserved() -> None:
    """Requirement 6: current P&L negative → original decision unchanged."""
    pos = _pos(option_pnl_pct=-0.01, peak_pnl_pct=0.10)
    arm = _resolved_arm(pos)
    assert guard.should_hold_early_green_one_contract(
        pos, _touched_stop(pnl=-0.01), decision_code="TOUCHED_PROFIT_STOP", runner_arm_pct=arm
    ) is False


def test_breakeven_touched_profit_preserved() -> None:
    """Requirement 6: current P&L exactly zero → original decision unchanged."""
    pos = _pos(option_pnl_pct=0.0, peak_pnl_pct=0.10)
    arm = _resolved_arm(pos)
    assert guard.should_hold_early_green_one_contract(
        pos, _touched_stop(pnl=0.0), decision_code="TOUCHED_PROFIT_STOP", runner_arm_pct=arm
    ) is False


# ─────────────────────────────────────────────────────────────────────────────
# 7. Real ExitDecision class is used, not a test stub
# ─────────────────────────────────────────────────────────────────────────────

def test_uses_real_exit_decision_class() -> None:
    """Requirement 7: ExitDecision in tests is the real engine class."""
    d = _touched_stop()
    assert type(d).__name__ == "ExitDecision"
    assert d.__class__.__module__ == "ap_exit_engine"


# ─────────────────────────────────────────────────────────────────────────────
# 8. Real threshold resolver exercised — not hard-coded test values
# ─────────────────────────────────────────────────────────────────────────────

def test_real_resolver_used_not_hardcoded_values() -> None:
    """Requirement 8: _resolved_arm() calls the real _effective_thresholds."""
    spy_0dte = _pos(option_symbol=_SPY_0DTE, ticker="SPY")
    spy_std  = _pos(option_symbol=_SPY_STD, ticker="SPY")
    arm_0dte = _resolved_arm(spy_0dte)
    arm_std  = _resolved_arm(spy_std)
    assert arm_0dte != arm_std, "Threshold resolver must return DTE-dependent values"
    assert arm_0dte == pytest.approx(0.20)
    assert arm_std  == pytest.approx(0.12)


def test_no_single_contract_runner_arm_pct_attribute_in_guard() -> None:
    """Guard must not expose SINGLE_CONTRACT_RUNNER_ARM_PCT as a policy source."""
    assert not hasattr(guard, "SINGLE_CONTRACT_RUNNER_ARM_PCT"), (
        "SINGLE_CONTRACT_RUNNER_ARM_PCT must not exist — single source of truth "
        "is ap_exit_engine._effective_thresholds"
    )
    assert not hasattr(guard, "_runner_arm_pct"), (
        "_runner_arm_pct() must not exist — threshold comes from canonical engine"
    )


def test_startup_diagnostic_does_not_log_single_configured_threshold() -> None:
    """Requirement startup-diagnostic: diagnostic returns policy map, not a scalar."""
    result = guard.startup_policy_diagnostic()
    assert "canonical_threshold_policy" in result, (
        "Diagnostic must describe DTE-dependent policy, not a single configured number"
    )
    policy = result["canonical_threshold_policy"]
    assert policy["0DTE_index"]  == pytest.approx(0.20)
    assert policy["0DTE_equity"] == pytest.approx(0.22)
    assert policy["DTE_lte_2"]   == pytest.approx(0.25)
    assert policy["standard"]    == pytest.approx(0.12)
    # Must NOT have a flat "runner_arm_pct" key implying a single threshold
    assert "runner_arm_pct" not in result, (
        "Diagnostic must not expose a single runner_arm_pct — that would imply "
        "one threshold when the policy is DTE/instrument-dependent"
    )


# ─────────────────────────────────────────────────────────────────────────────
# 9. Replacement HOLD is non-actionable through real decision classifier
# ─────────────────────────────────────────────────────────────────────────────

def test_replacement_hold_classified_as_hold(monkeypatch: pytest.MonkeyPatch) -> None:
    """Requirement 9: suppressed decision becomes ExitDecision with action=HOLD."""
    monkeypatch.setenv("LIVE_SINGLE_CONTRACT_RUNNER_PRECEDENCE_ENABLED", "1")

    pos = _pos(option_symbol=_SPY_0DTE, ticker="SPY",
               peak_pnl_pct=0.14, option_pnl_pct=0.05)
    original = _touched_stop()

    wrapped = guard.wrap_evaluate_exit(
        lambda p, now_et=None: original,
        exit_decision_cls=ExitDecision,
        classify_decision=_classify_exit_decision,
        engine_module=engine_module,
    )
    result = wrapped(pos)

    # Action must be HOLD
    assert result.action == "HOLD", f"Expected HOLD, got {result.action}"
    assert result.quantity == 0
    assert result.reason_code == "SINGLE_CONTRACT_EARLY_GREEN_HOLD"

    # Classifier must not recognize it as a CLOSE decision
    code = _classify_exit_decision(result)
    assert "CLOSE" not in code, f"HOLD should not classify as close: {code}"
    assert "STOP" not in code or "HOLD" in code or code == "SINGLE_CONTRACT_EARLY_GREEN_HOLD"


def test_replacement_hold_is_real_exit_decision_instance(monkeypatch: pytest.MonkeyPatch) -> None:
    """Requirement 9: the replacement is a real ExitDecision, not a stub."""
    monkeypatch.setenv("LIVE_SINGLE_CONTRACT_RUNNER_PRECEDENCE_ENABLED", "1")
    pos = _pos(option_symbol=_SPY_0DTE, ticker="SPY",
               peak_pnl_pct=0.14, option_pnl_pct=0.05)
    wrapped = guard.wrap_evaluate_exit(
        lambda p, now_et=None: _touched_stop(),
        exit_decision_cls=ExitDecision,
        classify_decision=_classify_exit_decision,
        engine_module=engine_module,
    )
    result = wrapped(pos)
    assert isinstance(result, ExitDecision), (
        f"Expected ExitDecision, got {type(result)}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# 10. Disabled / malformed / unset / false flag preserves object-for-object
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("value", [
    "0", "false", "False", "FALSE", "no", "NO", "off", "OFF",
    "", "  ", "none", "null", "disabled",
    None,  # unset
])
def test_disabled_flag_returns_original_object(
    monkeypatch: pytest.MonkeyPatch, value: str | None
) -> None:
    """Requirement 10: any non-truthy flag value returns the exact original object."""
    if value is None:
        monkeypatch.delenv("LIVE_SINGLE_CONTRACT_RUNNER_PRECEDENCE_ENABLED", raising=False)
    else:
        monkeypatch.setenv("LIVE_SINGLE_CONTRACT_RUNNER_PRECEDENCE_ENABLED", value)

    pos = _pos(option_symbol=_SPY_0DTE, peak_pnl_pct=0.14, option_pnl_pct=0.05)
    original = _touched_stop()

    wrapped = guard.wrap_evaluate_exit(
        lambda p, now_et=None: original,
        exit_decision_cls=ExitDecision,
        classify_decision=_classify_exit_decision,
        engine_module=engine_module,
    )
    result = wrapped(pos)
    assert result is original, (
        f"Flag value {value!r}: expected exact original object, got a different object"
    )


def test_unset_flag_is_default_off() -> None:
    """Requirement 10: without any env var the guard is off by default."""
    with pytest.MonkeyPatch().context() as mp:
        mp.delenv("LIVE_SINGLE_CONTRACT_RUNNER_PRECEDENCE_ENABLED", raising=False)
        assert guard._enabled() is False


def test_flag_default_not_set_in_guard_module() -> None:
    """Guard must not hard-code a truthy default for the feature flag."""
    import inspect
    src = inspect.getsource(guard._enabled)
    assert '"0"' in src or "'0'" in src, (
        "_enabled() must default to '0' (off)"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Wrapper integration tests (feature flag on)
# ─────────────────────────────────────────────────────────────────────────────

def test_wrapper_with_canonical_threshold_holds_spy_0dte(monkeypatch: pytest.MonkeyPatch) -> None:
    """Integration: wrapper resolves canonical 0DTE SPY arm and holds pre-runner."""
    monkeypatch.setenv("LIVE_SINGLE_CONTRACT_RUNNER_PRECEDENCE_ENABLED", "1")

    pos = _pos(option_symbol=_SPY_0DTE, ticker="SPY",
               peak_pnl_pct=0.14, option_pnl_pct=0.05)
    original = _touched_stop()

    wrapped = guard.wrap_evaluate_exit(
        lambda p, now_et=None: original,
        exit_decision_cls=ExitDecision,
        classify_decision=_classify_exit_decision,
        engine_module=engine_module,
    )
    result = wrapped(pos)
    assert result is not original
    assert result.action == "HOLD"
    # Reason must mention canonical +20% arm
    assert "20%" in result.reason or "20" in result.reason, (
        f"Reason should mention 20% canonical arm; got: {result.reason}"
    )


def test_wrapper_preserves_hard_stop_object_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """Enabled guard must pass hard-stop through as the exact original object."""
    monkeypatch.setenv("LIVE_SINGLE_CONTRACT_RUNNER_PRECEDENCE_ENABLED", "1")
    pos = _pos(option_symbol=_SPY_0DTE, ticker="SPY")
    hard_stop = ExitDecision(
        action="CLOSE_ALL", quantity=1,
        reason="HARD STOP HIT", urgency="IMMEDIATE",
        pnl_pct=-0.20, reason_code="HARD_STOP",
    )
    wrapped = guard.wrap_evaluate_exit(
        lambda p, now_et=None: hard_stop,
        exit_decision_cls=ExitDecision,
        classify_decision=_classify_exit_decision,
        engine_module=engine_module,
    )
    assert wrapped(pos) is hard_stop


def test_wrapper_preserves_touched_stop_at_standard_arm(monkeypatch: pytest.MonkeyPatch) -> None:
    """Requirement 4 (integration): standard DTE peak at arm → original returned."""
    monkeypatch.setenv("LIVE_SINGLE_CONTRACT_RUNNER_PRECEDENCE_ENABLED", "1")
    pos = _pos(option_symbol=_SPY_STD, ticker="SPY",
               peak_pnl_pct=0.12, option_pnl_pct=0.04)
    original = _touched_stop()
    wrapped = guard.wrap_evaluate_exit(
        lambda p, now_et=None: original,
        exit_decision_cls=ExitDecision,
        classify_decision=_classify_exit_decision,
        engine_module=engine_module,
    )
    assert wrapped(pos) is original


def test_wrapper_preserves_touched_stop_at_breakeven(monkeypatch: pytest.MonkeyPatch) -> None:
    """Enabled guard: breakeven touched-stop → original object unchanged."""
    monkeypatch.setenv("LIVE_SINGLE_CONTRACT_RUNNER_PRECEDENCE_ENABLED", "1")
    pos = _pos(option_symbol=_SPY_0DTE, ticker="SPY",
               peak_pnl_pct=0.10, option_pnl_pct=0.0)
    original = _touched_stop(pnl=0.0)
    wrapped = guard.wrap_evaluate_exit(
        lambda p, now_et=None: original,
        exit_decision_cls=ExitDecision,
        classify_decision=_classify_exit_decision,
        engine_module=engine_module,
    )
    assert wrapped(pos) is original


# ─────────────────────────────────────────────────────────────────────────────
# Installer and lifecycle wiring
# ─────────────────────────────────────────────────────────────────────────────

def test_installer_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Calling install_one_contract_exit_guard() twice must not double-wrap."""
    import ap_exit_engine as eng

    # Clean any previous patch
    for attr in (guard._PATCHED_ATTR, guard._ORIGINAL_ATTR):
        if hasattr(eng, attr):
            delattr(eng, attr)
    original_fn = eng.evaluate_exit

    guard.install_one_contract_exit_guard()
    first_patched = eng.evaluate_exit

    guard.install_one_contract_exit_guard()
    second_patched = eng.evaluate_exit

    assert first_patched is second_patched, "Double install must not double-wrap"

    # Restore
    eng.evaluate_exit = original_fn
    for attr in (guard._PATCHED_ATTR, guard._ORIGINAL_ATTR):
        if hasattr(eng, attr):
            delattr(eng, attr)


def test_install_wraps_without_enabling_policy(monkeypatch: pytest.MonkeyPatch) -> None:
    """install() changes the function pointer but doesn't enable the flag."""
    monkeypatch.delenv("LIVE_SINGLE_CONTRACT_RUNNER_PRECEDENCE_ENABLED", raising=False)
    import ap_exit_engine as eng

    for attr in (guard._PATCHED_ATTR, guard._ORIGINAL_ATTR):
        if hasattr(eng, attr):
            delattr(eng, attr)
    original_fn = eng.evaluate_exit

    guard.install_one_contract_exit_guard()
    assert eng.evaluate_exit is not original_fn, "evaluate_exit should be wrapped"
    assert guard._enabled() is False, "Flag must remain off after install"

    # Restore
    eng.evaluate_exit = original_fn
    for attr in (guard._PATCHED_ATTR, guard._ORIGINAL_ATTR):
        if hasattr(eng, attr):
            delattr(eng, attr)


# ─────────────────────────────────────────────────────────────────────────────
# ManagedPosition execution_mode identity proof (requirement: runtime identity)
# ─────────────────────────────────────────────────────────────────────────────

def test_managed_position_has_execution_mode_field() -> None:
    """ManagedPosition must declare execution_mode for the guard to read it."""
    import ap_exit_engine as eng
    import dataclasses
    fields = {f.name for f in dataclasses.fields(eng.ManagedPosition)}
    assert "execution_mode" in fields, (
        "ManagedPosition.execution_mode must exist for the guard to distinguish "
        "LIVE from PAPER positions"
    )


def test_live_fill_position_mode_is_authoritative() -> None:
    """LIVE fill populates execution_mode='live' — guard reads it directly."""
    pos = _pos(execution_mode="live")
    assert pos.execution_mode.strip().lower() == "live"
    arm = _resolved_arm(pos)
    result = guard.should_hold_early_green_one_contract(
        pos, _touched_stop(), decision_code="TOUCHED_PROFIT_STOP", runner_arm_pct=arm
    )
    # With peak=0.14 and arm=0.20, LIVE should hold
    assert result is True


def test_paper_position_mode_is_never_live() -> None:
    """PAPER fill populates execution_mode='paper' — guard must not suppress."""
    pos = _pos(execution_mode="paper", peak_pnl_pct=0.14, option_pnl_pct=0.05)
    arm = _resolved_arm(pos)
    assert guard.should_hold_early_green_one_contract(
        pos, _touched_stop(), decision_code="TOUCHED_PROFIT_STOP", runner_arm_pct=arm
    ) is False


def test_missing_mode_not_suppressed() -> None:
    """Startup/recovery hydration with no mode → guard must preserve original."""
    pos = _pos(execution_mode="", peak_pnl_pct=0.14, option_pnl_pct=0.05)
    arm = _resolved_arm(pos)
    assert guard.should_hold_early_green_one_contract(
        pos, _touched_stop(), decision_code="TOUCHED_PROFIT_STOP", runner_arm_pct=arm
    ) is False


def test_broker_repair_unresolved_mode_not_suppressed() -> None:
    """Unresolved broker-repair mode must not suppress exit."""
    for mode in ("broker_repair", "UNKNOWN", "unresolved", "None"):
        pos = _pos(execution_mode=mode, peak_pnl_pct=0.14, option_pnl_pct=0.05)
        arm = _resolved_arm(pos)
        assert guard.should_hold_early_green_one_contract(
            pos, _touched_stop(), decision_code="TOUCHED_PROFIT_STOP", runner_arm_pct=arm
        ) is False, f"mode={mode!r} must not suppress"


# ─────────────────────────────────────────────────────────────────────────────
# 17. No deployment or environment file enables the flag (default-off attestation)
# ─────────────────────────────────────────────────────────────────────────────

def test_flag_absent_from_render_config() -> None:
    """Requirement 17: LIVE_SINGLE_CONTRACT_RUNNER_PRECEDENCE_ENABLED must not
    appear in render.yaml or any environment file. Enabling it is a separate
    operational rollout decision, not part of this PR install."""
    import pathlib
    import re

    repo_root = pathlib.Path(__file__).parent.parent
    candidate_files = list(repo_root.glob("render.yaml")) + \
                      list(repo_root.glob("render.yml")) + \
                      list(repo_root.glob(".env")) + \
                      list(repo_root.glob(".env.*")) + \
                      list(repo_root.glob("*.env"))

    pattern = re.compile(
        r"LIVE_SINGLE_CONTRACT_RUNNER_PRECEDENCE_ENABLED\s*=\s*(?!0\b|false|False|FALSE|\"0\"|'0')"
    )
    for f in candidate_files:
        try:
            text = f.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        m = pattern.search(text)
        assert m is None, (
            f"{f.name} enables LIVE_SINGLE_CONTRACT_RUNNER_PRECEDENCE_ENABLED "
            f"(found {m.group()!r}); enabling the flag must be a separate "
            "operational rollout decision, not part of this PR"
        )


def test_flag_absent_from_github_workflow() -> None:
    """Requirement 17: the flag must not be set to a truthy value in CI config."""
    import pathlib
    import re

    workflows_dir = pathlib.Path(__file__).parent.parent / ".github" / "workflows"
    if not workflows_dir.exists():
        return

    pattern = re.compile(
        r"LIVE_SINGLE_CONTRACT_RUNNER_PRECEDENCE_ENABLED\s*[:=]\s*(?!0\b|false|\"0\"|'0'|\"false\"|'false')\S"
    )
    for f in workflows_dir.glob("*.yml"):
        text = f.read_text(encoding="utf-8", errors="ignore")
        m = pattern.search(text)
        assert m is None, (
            f"{f.name} enables LIVE_SINGLE_CONTRACT_RUNNER_PRECEDENCE_ENABLED "
            f"in CI (found {m.group()!r}); this PR installs default-off only"
        )


def test_one_contract_guard_listed_in_lifecycle_guards_as_not_required() -> None:
    """Lifecycle wiring: one_contract_policy must be registered with required=False.

    The guard is installed by ``install_trade_lifecycle_guards()`` but because
    the feature flag is off, it never changes live exit behaviour until
    LIVE_SINGLE_CONTRACT_RUNNER_PRECEDENCE_ENABLED is explicitly set.
    ``required=False`` means the preflight check does not block deployment
    if the module is absent on a branch where this PR hasn't merged yet.
    """
    import importlib
    import types

    # Read ap/trade_lifecycle_guards.py as source to avoid triggering ap.db
    # which requires DATABASE_URL at import time.
    import pathlib, ast

    guards_src = (
        pathlib.Path(__file__).parent.parent / "ap" / "trade_lifecycle_guards.py"
    ).read_text(encoding="utf-8")

    # Extract _GUARDS via AST — no module execution needed.
    tree = ast.parse(guards_src)
    guards_value = None
    for node in ast.walk(tree):
        # _GUARDS may be a plain Assign or an AnnAssign (annotated assignment)
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id == "_GUARDS":
                    guards_value = node.value
        elif isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name) and node.target.id == "_GUARDS":
                guards_value = node.value

    assert guards_value is not None, "_GUARDS not found in trade_lifecycle_guards.py"
    elts = guards_value.elts  # type: ignore[attr-defined]
    guard_map: dict[str, bool] = {}
    for elt in elts:
        if isinstance(elt, ast.Tuple) and len(elt.elts) == 4:
            name_node, _, _, required_node = elt.elts
            if isinstance(name_node, ast.Constant) and isinstance(required_node, ast.Constant):
                guard_map[name_node.value] = bool(required_node.value)

    assert "one_contract_policy" in guard_map, (
        "one_contract_policy must be registered in ap.trade_lifecycle_guards._GUARDS"
    )
    assert guard_map["one_contract_policy"] is False, (
        "one_contract_policy must be registered with required=False "
        "so that deployment is never blocked while the flag is disabled"
    )
