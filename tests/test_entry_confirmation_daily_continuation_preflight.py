from __future__ import annotations

from types import SimpleNamespace

from ap_entry_confirmation import check_entry_confirmation


def _plan(*, candles=None, confirmation_required=False, side="CALL"):
    metadata = {
        "ticker": "AAPL",
        "client_id": "jasoncosby1@gmail.com",
        "execution_mode": "live",
        "canonical_signal_id": "sig-daily-1",
        "hybrid_client_quality_gate": {
            "confirmation_required": confirmation_required,
        },
    }
    if candles is not None:
        metadata["intraday_candles"] = candles
    return SimpleNamespace(
        ticker="AAPL",
        symbol="AAPL",
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        canonical_signal_id="sig-daily-1",
        signal_id="sig-daily-1",
        stop_underlying=95.0 if side == "CALL" else 105.0,
        metadata=metadata,
    )


def _call_confirmation(**overrides):
    kwargs = {
        "plan": _plan(candles=overrides.pop("candles", None), side=overrides.get("direction", "CALL")),
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


def test_daily_call_fresh_high_after_trigger_allowed_even_without_legacy_confirmation():
    candles = [
        {"open": 99.80, "high": 100.20, "low": 99.70, "close": 100.05, "time": "09:31"},
        {"open": 100.05, "high": 101.00, "low": 100.00, "close": 100.80, "time": "09:36"},
    ]

    result = _call_confirmation(candles=candles)

    assert result.passed is True
    assert result.fail_reason is None
    assert result.metadata["confirmation_required"] is False
    assert result.metadata["daily_continuation"]["continuation_allowed"] is True
    assert result.metadata["daily_continuation"]["fresh_intraday_high"] is True
    assert result.metadata["daily_continuation"]["client_id"] == "jasoncosby1@gmail.com"
    assert result.metadata["daily_continuation"]["execution_mode"] == "live"


def test_daily_call_missing_intraday_context_blocks_before_legacy_fast_path():
    result = _call_confirmation(candles=None, underlying_last=101.00)

    assert result.passed is False
    assert result.fail_reason == "daily_continuation_failed:missing_intraday_context"
    assert result.metadata["confirmation_required"] is False
    assert result.metadata["daily_continuation"]["continuation_allowed"] is False
    assert result.metadata["daily_continuation"]["reason"] == "daily_continuation_failed:missing_intraday_context"


def test_daily_call_trigger_touch_then_fade_blocks():
    candles = [
        {"open": 99.80, "high": 100.20, "low": 99.70, "close": 99.90, "time": "09:31"},
        {"open": 99.90, "high": 99.95, "low": 99.40, "close": 99.60, "time": "09:36"},
    ]

    result = _call_confirmation(candles=candles, underlying_last=99.60)

    assert result.passed is False
    assert result.fail_reason in {
        "daily_continuation_failed:trigger_touch_only",
        "daily_continuation_failed:price_back_through_trigger",
    }
    assert result.metadata["daily_continuation"]["price_back_through_trigger"] is True


def test_daily_put_fresh_low_after_trigger_allowed():
    candles = [
        {"open": 100.20, "high": 100.30, "low": 99.80, "close": 99.95, "time": "09:31"},
        {"open": 99.95, "high": 100.00, "low": 99.00, "close": 99.10, "time": "09:36"},
    ]
    plan = _plan(candles=candles, side="PUT")

    result = _call_confirmation(
        plan=plan,
        direction="PUT",
        candles=candles,
        underlying_last=99.10,
    )

    assert result.passed is True
    assert result.metadata["daily_continuation"]["continuation_allowed"] is True
    assert result.metadata["daily_continuation"]["fresh_intraday_low"] is True


def test_non_daily_timeframe_preserves_legacy_fast_path_without_continuation_context():
    result = _call_confirmation(candles=None, timeframe="60min", underlying_last=100.50)

    assert result.passed is True
    assert result.fail_reason is None
    assert "daily_continuation" not in result.metadata


def test_daily_continuation_can_be_disabled_for_emergency_rollback(monkeypatch):
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_VALIDATION", "0")

    result = _call_confirmation(candles=None, underlying_last=99.00)

    assert result.passed is True
    assert result.metadata["daily_continuation"]["continuation_allowed"] is True
    assert result.metadata["daily_continuation"]["reason"] == "daily_continuation_skipped:disabled"
