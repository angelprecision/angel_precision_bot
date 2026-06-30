from __future__ import annotations

from types import SimpleNamespace

from ap_entry_confirmation import ConfirmationResult, check_entry_confirmation


def _plan(*, confirmation_required: bool = False, candles=None, side: str = "CALL"):
    metadata = {
        "hybrid_client_quality_gate": {
            "confirmation_required": confirmation_required,
            "confirmation_seconds": 45,
        },
    }
    if candles is not None:
        metadata["intraday_candles"] = candles
    return SimpleNamespace(
        side=side,
        trigger_price=100.0,
        limit_price=1.00,
        contracts=1,
        tier="A",
        metadata=metadata,
    )


def _confirm(plan=None, **overrides):
    kwargs = {
        "plan": plan or _plan(),
        "direction": "CALL",
        "trigger_price": 100.0,
        "live_bid": 1.00,
        "live_ask": 1.04,
        "live_quote_age_ms": 1000,
        "underlying_last": 100.8,
        "decision_option_price": 1.00,
        "score": 78,
        "tier": "A",
        "timeframe": "1d",
        "sandbox_mode": False,
    }
    kwargs.update(overrides)
    return check_entry_confirmation(**kwargs)


def _failing_candles():
    return [
        {"open": 99.80, "high": 100.20, "low": 99.70, "close": 99.90},
        {"open": 99.90, "high": 99.95, "low": 99.40, "close": 99.60},
    ]


def _passing_candles():
    return [
        {"open": 99.80, "high": 100.20, "low": 99.70, "close": 100.05},
        {"open": 100.05, "high": 101.00, "low": 100.00, "close": 100.80},
    ]


def test_daily_missing_continuation_observe_is_diagnostic_only(monkeypatch):
    monkeypatch.delenv("ENABLE_DAILY_CONTINUATION_VALIDATION", raising=False)
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_MODE", "observe")

    result = _confirm(plan=_plan(confirmation_required=False, candles=None), underlying_last=100.9)

    assert result.passed is True
    assert result.fail_reason is None
    assert result.metadata["daily_continuation_mode"] == "observe"
    assert result.metadata["daily_continuation_passed"] is None
    assert result.metadata["daily_continuation_fail_reason"] == "daily_continuation_failed:missing_intraday_context"
    assert result.metadata["daily_continuation_would_block"] is True
    assert result.metadata["confirmation_required"] is False


def test_daily_missing_continuation_enforce_blocks(monkeypatch):
    monkeypatch.delenv("ENABLE_DAILY_CONTINUATION_VALIDATION", raising=False)
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_MODE", "enforce")

    result = _confirm(plan=_plan(confirmation_required=False, candles=None), underlying_last=100.9)

    assert result.passed is False
    assert result.fail_reason == "daily_continuation_failed:missing_intraday_context"
    assert result.metadata["daily_continuation_mode"] == "enforce"
    assert result.metadata["daily_continuation_passed"] is None
    assert result.metadata["daily_continuation_would_block"] is True


def test_daily_continuation_failure_observe_does_not_block(monkeypatch):
    monkeypatch.delenv("ENABLE_DAILY_CONTINUATION_VALIDATION", raising=False)
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_MODE", "observe")

    result = _confirm(
        plan=_plan(confirmation_required=False, candles=_failing_candles()),
        underlying_last=99.60,
    )

    assert result.passed is True
    assert result.fail_reason is None
    assert result.metadata["daily_continuation_mode"] == "observe"
    assert result.metadata["daily_continuation_passed"] is False
    assert result.metadata["daily_continuation_would_block"] is True


def test_daily_continuation_failure_enforce_blocks(monkeypatch):
    monkeypatch.delenv("ENABLE_DAILY_CONTINUATION_VALIDATION", raising=False)
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_MODE", "enforce")

    result = _confirm(
        plan=_plan(confirmation_required=False, candles=_failing_candles()),
        underlying_last=99.60,
    )

    assert result.passed is False
    assert result.fail_reason == "daily_continuation_failed:trigger_touch_only"
    assert result.metadata["daily_continuation_mode"] == "enforce"
    assert result.metadata["daily_continuation_passed"] is False
    assert result.metadata["daily_continuation_would_block"] is True


def test_confirmation_required_spread_still_blocks_in_observe_mode(monkeypatch):
    monkeypatch.delenv("ENABLE_DAILY_CONTINUATION_VALIDATION", raising=False)
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_MODE", "observe")

    result = _confirm(
        plan=_plan(confirmation_required=True, candles=_passing_candles()),
        live_bid=2.00,
        live_ask=3.00,
        underlying_last=100.80,
    )

    assert result.passed is False
    assert result.fail_reason == "entry_confirm_failed_spread"
    assert result.metadata["daily_continuation_mode"] == "observe"
    assert result.metadata["daily_continuation_passed"] is True


def test_to_meta_preserves_confirmation_required_false_for_observe_only_daily(monkeypatch):
    monkeypatch.delenv("ENABLE_DAILY_CONTINUATION_VALIDATION", raising=False)
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_MODE", "observe")

    result = _confirm(plan=_plan(confirmation_required=False, candles=None), underlying_last=100.9)
    meta = result.to_meta("t1", "t2")

    assert meta["confirmation_required"] is False
    assert meta["confirmation_passed"] is True
    assert meta["daily_continuation_mode"] == "observe"
    assert meta["daily_continuation_would_block"] is True


def test_legacy_validation_env_maps_to_observe_not_enforce(monkeypatch):
    monkeypatch.delenv("ENABLE_DAILY_CONTINUATION_MODE", raising=False)
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_VALIDATION", "1")

    result = _confirm(plan=_plan(confirmation_required=False, candles=None), underlying_last=100.9)

    assert result.passed is True
    assert result.metadata["daily_continuation_mode"] == "observe"


def test_confirmation_result_to_meta_uses_hybrid_confirmation_required():
    result = ConfirmationResult(
        passed=True,
        fail_reason=None,
        metadata={"confirmation_required": False, "hybrid_confirmation_required": True},
    )

    meta = result.to_meta("t1", "t2")

    assert meta["confirmation_required"] is True
