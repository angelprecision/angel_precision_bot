from __future__ import annotations

from types import SimpleNamespace

from ap_entry_confirmation import check_entry_confirmation


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


def test_daily_call_fresh_high_allowed_before_submit_even_without_legacy_confirmation(monkeypatch):
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_MODE", "enforce")
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


def test_daily_call_touch_then_fade_blocks_before_submit(monkeypatch):
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_MODE", "enforce")
    candles = [
        {"open": 99.80, "high": 100.20, "low": 99.70, "close": 99.90, "time": "09:31"},
        {"open": 99.90, "high": 99.95, "low": 99.40, "close": 99.60, "time": "09:36"},
    ]
    result = _confirm(candles=candles, underlying_last=99.60)
    assert result.passed is False
    assert result.fail_reason == "DAILY_CONTINUATION_TRIGGER_TOUCH_ONLY"
    assert result.metadata["daily_continuation_price_back_through_trigger"] is True


def test_daily_put_fresh_low_allowed_before_submit(monkeypatch):
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_MODE", "enforce")
    candles = [
        {"open": 100.20, "high": 100.30, "low": 99.80, "close": 99.95, "time": "09:31"},
        {"open": 99.95, "high": 100.00, "low": 99.00, "close": 99.10, "time": "09:36"},
    ]
    plan = _plan(candles=candles, side="PUT")
    result = _confirm(plan=plan, direction="PUT", underlying_last=99.10)
    assert result.passed is True
    assert result.metadata["daily_continuation_passed"] is True
    assert result.metadata["daily_continuation_fresh_extension"] is True


def test_daily_missing_context_blocks_before_legacy_fast_path(monkeypatch):
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_MODE", "enforce")
    result = _confirm(candles=None, underlying_last=101.00)
    assert result.passed is False
    assert result.fail_reason == "DAILY_CONTINUATION_MISSING_INTRADAY_CANDLES"
    assert result.metadata["confirmation_required"] is False
    assert result.metadata["daily_continuation_would_block"] is True


def test_daily_no_breach_context_has_distinct_reason(monkeypatch):
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_MODE", "enforce")
    candles = [
        {"open": 99.50, "high": 99.80, "low": 99.20, "close": 99.70, "time": "09:31"},
        {"open": 99.70, "high": 99.90, "low": 99.40, "close": 99.80, "time": "09:36"},
    ]
    result = _confirm(candles=candles, underlying_last=100.50)
    assert result.passed is False
    assert result.fail_reason == "DAILY_CONTINUATION_NO_BREACH_CANDLE"


def test_daily_opening_breach_no_extension_blocks(monkeypatch):
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_MODE", "enforce")
    candles = [
        {"open": 99.90, "high": 100.10, "low": 99.70, "close": 100.02, "time": "09:31"},
        {"open": 100.02, "high": 100.06, "low": 99.90, "close": 100.01, "time": "09:32"},
    ]
    result = _confirm(candles=candles, underlying_last=100.02)
    assert result.passed is False
    assert result.fail_reason == "DAILY_CONTINUATION_OPENING_MOVE_EXHAUSTED"


def test_non_daily_preserves_legacy_fast_path_without_continuation_context(monkeypatch):
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_MODE", "enforce")
    result = _confirm(candles=None, timeframe="60min", underlying_last=100.50)
    assert result.passed is True
    assert result.fail_reason is None
    assert result.metadata["daily_continuation_mode"] == "enforce"
    assert result.metadata["daily_continuation_would_block"] is False


def test_daily_continuation_can_be_disabled_for_emergency_rollback(monkeypatch):
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_MODE", "off")
    result = _confirm(candles=None, underlying_last=99.00)
    assert result.passed is True
    assert result.metadata["daily_continuation_mode"] == "off"
    assert result.metadata["daily_continuation_would_block"] is False


def test_missing_context_blocks_even_when_legacy_confirmation_required(monkeypatch):
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_MODE", "enforce")
    plan = _plan(candles=None, confirmation_required=True)
    result = _confirm(plan=plan, underlying_last=101.00)
    assert result.passed is False
    assert result.fail_reason == "DAILY_CONTINUATION_MISSING_INTRADAY_CANDLES"
    assert result.metadata["confirmation_required"] is True
