"""
RED-ON-MAIN CLEANUP (PR #265): rewritten against the post-#243 tri-state
daily-continuation contract.

These tests were written when daily continuation ran (and blocked) by
default. PR #243 deliberately introduced ENABLE_DAILY_CONTINUATION_MODE
(off | observe | enforce) with DEFAULT OFF, and PR #219 Fix A made observe
mode diagnostic-only (would_block recorded, submit continues). The old
tests asserted the pre-#243 metadata schema (daily_continuation_allowed /
daily_continuation_reason) which no longer exists.

Invariants enforced here (same protections, current contract):
  - ENFORCE mode: fade-after-touch and missing-context BLOCK before submit
  - ENFORCE mode: fresh directional extension is ALLOWED
  - OBSERVE mode: a would-block condition is RECORDED but does NOT block
    (PR #219 Fix A — observe-only must never terminalize)
  - OFF mode (the emergency rollback): continuation never blocks and
    never fabricates a pass/fail verdict
"""
from __future__ import annotations

from types import SimpleNamespace

import ap_entry_confirmation as entry_confirmation
from ap_entry_confirmation import check_entry_confirmation


import pytest


@pytest.fixture(autouse=True)
def _enforce_mode(monkeypatch):
    """Default for this module: ENFORCE — the mode these invariants target.
    Individual tests override to observe/off to test those contracts."""
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_MODE", "enforce")
    monkeypatch.delenv("ENABLE_DAILY_CONTINUATION_VALIDATION", raising=False)
    yield


def _plan(*, candles=None, confirmation_required=False, side="CALL"):
    metadata = {
        "ticker": "AAPL",
        "client_id": "jasoncosby1@gmail.com",
        "execution_mode": "live",
        "canonical_signal_id": "sig-daily-1",
        "hybrid_client_quality_gate": {"confirmation_required": confirmation_required},
    }
    if candles is not None:
        metadata["intraday_candles"] = candles
    return SimpleNamespace(
        ticker="AAPL",
        symbol="AAPL",
        side=side,
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        canonical_signal_id="sig-daily-1",
        signal_id="sig-daily-1",
        stop_underlying=95.0 if side == "CALL" else 105.0,
        metadata=metadata,
    )


def _confirm(**overrides):
    candles = overrides.pop("candles", None)
    kwargs = {
        "plan": overrides.pop("plan", _plan(candles=candles, side=overrides.get("direction", "CALL"))),
        "direction": "CALL",
        "trigger_price": 100.0,
        "live_bid": 1.00,
        "live_ask": 1.04,
        "live_quote_age_ms": 500,
        "underlying_last": 100.80,
        "decision_option_price": 1.00,
        "score": 78,
        "tier": "A",
        "timeframe": "1d",
        "sandbox_mode": False,
    }
    kwargs.update(overrides)
    return check_entry_confirmation(**kwargs)


def test_daily_call_fresh_high_allowed_before_submit_even_without_legacy_confirmation():
    candles = [
        {"open": 99.80, "high": 100.20, "low": 99.70, "close": 100.05, "time": "09:31"},
        {"open": 100.05, "high": 101.00, "low": 100.00, "close": 100.80, "time": "09:36"},
    ]
    result = _confirm(candles=candles)
    assert result.passed is True
    assert result.fail_reason is None
    assert result.metadata["confirmation_required"] is False
    assert result.metadata["daily_continuation_passed"] is True
    assert result.metadata["daily_continuation_would_block"] is False


def test_daily_call_touch_then_fade_blocks_before_submit():
    candles = [
        {"open": 99.80, "high": 100.20, "low": 99.70, "close": 99.90, "time": "09:31"},
        {"open": 99.90, "high": 99.95, "low": 99.40, "close": 99.60, "time": "09:36"},
    ]
    result = _confirm(candles=candles, underlying_last=99.60)
    assert result.passed is False
    assert str(result.fail_reason).startswith("daily_continuation_failed:")
    assert result.metadata["daily_continuation_would_block"] is True
    assert result.metadata["daily_continuation_passed"] is False


def test_daily_put_fresh_low_allowed_before_submit():
    candles = [
        {"open": 100.20, "high": 100.30, "low": 99.80, "close": 99.95, "time": "09:31"},
        {"open": 99.95, "high": 100.00, "low": 99.00, "close": 99.10, "time": "09:36"},
    ]
    plan = _plan(candles=candles, side="PUT")
    result = _confirm(plan=plan, direction="PUT", underlying_last=99.10)
    assert result.passed is True
    assert result.metadata["daily_continuation_passed"] is True
    assert result.metadata["daily_continuation_would_block"] is False


def test_daily_missing_context_blocks_before_legacy_fast_path():
    # Post-#243 there is no fetch fallback — candles come exclusively from
    # plan metadata, so absent candles ARE missing context.
    result = _confirm(candles=None, underlying_last=101.00)
    assert result.passed is False
    assert result.fail_reason == "daily_continuation_failed:missing_intraday_context"
    assert result.metadata["confirmation_required"] is False
    assert result.metadata["daily_continuation_would_block"] is True


def test_daily_opening_breach_no_extension_blocks():
    candles = [
        {"open": 99.90, "high": 100.10, "low": 99.70, "close": 100.02, "time": "09:31"},
        {"open": 100.02, "high": 100.06, "low": 99.90, "close": 100.01, "time": "09:32"},
    ]
    result = _confirm(candles=candles, underlying_last=100.02)
    assert result.passed is False
    assert result.fail_reason == "daily_continuation_failed:opening_move_exhausted"
    assert result.metadata["daily_continuation_would_block"] is True


def test_non_daily_preserves_legacy_fast_path_without_continuation_context():
    result = _confirm(candles=None, timeframe="60min", underlying_last=100.50)
    assert result.passed is True
    assert result.fail_reason is None
    assert "daily_continuation_allowed" not in result.metadata


def test_daily_continuation_can_be_disabled_for_emergency_rollback(monkeypatch):
    # Post-#243 the emergency rollback is ENABLE_DAILY_CONTINUATION_MODE=off
    # (the MODE env outranks the legacy VALIDATION flag). Off mode is a pure
    # pass-through: no block, no fabricated verdict.
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_MODE", "off")
    result = _confirm(candles=None, underlying_last=99.00)
    assert result.passed is True
    assert result.metadata["daily_continuation_passed"] is None
    assert result.metadata["daily_continuation_would_block"] is False


def test_missing_context_blocks_even_when_legacy_confirmation_required():
    plan = _plan(candles=None, confirmation_required=True)
    result = _confirm(plan=plan, underlying_last=101.00)
    assert result.passed is False
    assert result.fail_reason == "daily_continuation_failed:missing_intraday_context"
    assert result.metadata["confirmation_required"] is True


def test_observe_mode_records_would_block_but_never_terminalizes(monkeypatch):
    """NEW invariant fence (PR #219 Fix A, added by PR #265): in OBSERVE
    mode a failing continuation is RECORDED (would_block=True, fail reason
    in metadata) but the confirmation result must still pass — observe-only
    must never terminalize a submit."""
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_MODE", "observe")
    candles = [
        {"open": 99.80, "high": 100.20, "low": 99.70, "close": 99.90, "time": "09:31"},
        {"open": 99.90, "high": 99.95, "low": 99.40, "close": 99.60, "time": "09:36"},
    ]
    result = _confirm(candles=candles, underlying_last=99.60)
    assert result.passed is True
    assert result.metadata["daily_continuation_would_block"] is True
    assert str(result.metadata["daily_continuation_fail_reason"]).startswith(
        "daily_continuation_failed:"
    )
