from __future__ import annotations

from datetime import datetime

from ap.daily_continuation_validator import (
    REASON_ALLOWED,
    REASON_BACK_THROUGH,
    REASON_MISSING_CONTEXT,
    REASON_TOUCH_ONLY,
    validate_daily_intraday_continuation,
)


def candle(open_, high, low, close, ts=None):
    return {"open": open_, "high": high, "low": low, "close": close, "ts": ts}


def test_call_daily_above_trigger_with_fresh_intraday_high_allowed(monkeypatch):
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_VALIDATION", "true")

    decision = validate_daily_intraday_continuation(
        ticker="AAPL",
        timeframe="1d",
        direction="CALL",
        trigger_price=100.0,
        current_underlying_price=101.25,
        intraday_candles=[
            candle(99.50, 100.10, 99.40, 100.05),
            candle(100.05, 100.80, 99.95, 100.70),
            candle(100.70, 101.30, 100.60, 101.20),
        ],
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        canonical_signal_id="REEVAL:abc",
    )

    assert decision.allowed is True
    assert decision.reason == REASON_ALLOWED
    assert decision.diagnostics["fresh_intraday_high"] is True
    assert decision.diagnostics["continuation_allowed"] is True
    assert decision.diagnostics["client_id"] == "jasoncosby1@gmail.com"
    assert decision.diagnostics["execution_mode"] == "live"
    assert decision.diagnostics["canonical_signal_id"] == "REEVAL:abc"


def test_call_daily_touched_trigger_at_open_now_below_trigger_blocked(monkeypatch):
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_VALIDATION", "true")

    decision = validate_daily_intraday_continuation(
        ticker="AAPL",
        timeframe="daily",
        direction="CALL",
        trigger_price=100.0,
        current_underlying_price=99.40,
        intraday_candles=[
            candle(99.70, 100.05, 99.50, 99.80),
            candle(99.80, 99.90, 99.20, 99.30),
            candle(99.30, 99.50, 99.10, 99.40),
        ],
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        canonical_signal_id="REEVAL:def",
    )

    assert decision.allowed is False
    assert decision.reason == REASON_TOUCH_ONLY
    assert decision.diagnostics["price_back_through_trigger"] is True
    assert decision.diagnostics["client_id"] == "jasoncosby1@gmail.com"
    assert decision.diagnostics["execution_mode"] == "live"
    assert decision.diagnostics["canonical_signal_id"] == "REEVAL:def"


def test_put_daily_below_trigger_with_fresh_intraday_low_allowed(monkeypatch):
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_VALIDATION", "true")

    decision = validate_daily_intraday_continuation(
        ticker="TSLA",
        timeframe="1d",
        direction="PUT",
        trigger_price=200.0,
        current_underlying_price=197.85,
        intraday_candles=[
            candle(201.00, 201.20, 199.90, 199.95),
            candle(199.95, 200.05, 198.80, 198.90),
            candle(198.90, 199.00, 197.70, 197.90),
        ],
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        canonical_signal_id="REEVAL:ghi",
    )

    assert decision.allowed is True
    assert decision.reason == REASON_ALLOWED
    assert decision.diagnostics["fresh_intraday_low"] is True
    assert decision.diagnostics["continuation_allowed"] is True
    assert decision.diagnostics["client_id"] == "jasoncosby1@gmail.com"
    assert decision.diagnostics["execution_mode"] == "live"
    assert decision.diagnostics["canonical_signal_id"] == "REEVAL:ghi"


def test_put_daily_broke_down_at_open_then_reclaimed_above_trigger_blocked(monkeypatch):
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_VALIDATION", "true")

    decision = validate_daily_intraday_continuation(
        ticker="TSLA",
        timeframe="daily",
        direction="PUT",
        trigger_price=200.0,
        current_underlying_price=201.10,
        intraday_candles=[
            candle(200.30, 200.50, 199.80, 199.90),
            candle(199.90, 200.40, 199.70, 200.20),
            candle(200.20, 201.20, 200.10, 201.10),
        ],
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        canonical_signal_id="REEVAL:jkl",
    )

    assert decision.allowed is False
    assert decision.reason == REASON_BACK_THROUGH
    assert decision.diagnostics["price_back_through_trigger"] is True
    assert decision.diagnostics["client_id"] == "jasoncosby1@gmail.com"
    assert decision.diagnostics["execution_mode"] == "live"
    assert decision.diagnostics["canonical_signal_id"] == "REEVAL:jkl"


def test_missing_intraday_context_fails_safe(monkeypatch):
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_VALIDATION", "true")

    decision = validate_daily_intraday_continuation(
        ticker="NVDA",
        timeframe="1d",
        direction="CALL",
        trigger_price=150.0,
        current_underlying_price=151.0,
        intraday_candles=[],
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        canonical_signal_id="REEVAL:missing",
    )

    assert decision.allowed is False
    assert decision.reason == REASON_MISSING_CONTEXT
    assert decision.diagnostics["reason"] == REASON_MISSING_CONTEXT
    assert decision.diagnostics["client_id"] == "jasoncosby1@gmail.com"
    assert decision.diagnostics["execution_mode"] == "live"
    assert decision.diagnostics["canonical_signal_id"] == "REEVAL:missing"


def test_client_id_execution_mode_and_canonical_signal_id_preserved_on_block(monkeypatch):
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_VALIDATION", "true")

    decision = validate_daily_intraday_continuation(
        ticker="MSFT",
        timeframe="1d",
        direction="CALL",
        trigger_price=300.0,
        current_underlying_price=299.0,
        intraday_candles=[
            candle(299.50, 300.05, 299.20, 299.70),
            candle(299.70, 299.80, 298.90, 299.00),
        ],
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        canonical_signal_id="REEVAL:preserve-me",
    )

    assert decision.allowed is False
    assert decision.diagnostics["client_id"] == "jasoncosby1@gmail.com"
    assert decision.diagnostics["execution_mode"] == "live"
    assert decision.diagnostics["canonical_signal_id"] == "REEVAL:preserve-me"


def test_non_daily_timeframe_skips_validation(monkeypatch):
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_VALIDATION", "true")

    decision = validate_daily_intraday_continuation(
        ticker="SPY",
        timeframe="30m",
        direction="CALL",
        trigger_price=500.0,
        current_underlying_price=499.0,
        intraday_candles=[],
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        canonical_signal_id="INTRADAY:abc",
    )

    assert decision.allowed is True
    assert decision.reason == "daily_continuation_skipped:not_daily"


def test_flag_disabled_allows_without_metadata_rewrite(monkeypatch):
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_VALIDATION", "false")

    decision = validate_daily_intraday_continuation(
        ticker="SPY",
        timeframe="1d",
        direction="CALL",
        trigger_price=500.0,
        current_underlying_price=490.0,
        intraday_candles=[],
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        canonical_signal_id="REEVAL:flag-off",
    )

    assert decision.allowed is True
    assert decision.reason == "daily_continuation_skipped:disabled"
    assert decision.diagnostics["client_id"] == "jasoncosby1@gmail.com"
    assert decision.diagnostics["execution_mode"] == "live"
    assert decision.diagnostics["canonical_signal_id"] == "REEVAL:flag-off"


def test_polygon_style_candle_aliases_are_supported(monkeypatch):
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_VALIDATION", "true")

    decision = validate_daily_intraday_continuation(
        ticker="AAPL",
        timeframe="1d",
        direction="CALL",
        trigger_price=100.0,
        current_underlying_price=101.1,
        intraday_candles=[
            {"o": 99.8, "h": 100.2, "l": 99.7, "c": 100.1},
            {"o": 100.1, "h": 101.2, "l": 100.0, "c": 101.1},
        ],
        client_id="paper-client@example.com",
        execution_mode="paper",
        canonical_signal_id="REEVAL:aliases",
    )

    assert decision.allowed is True
    assert decision.reason == REASON_ALLOWED
    assert decision.diagnostics["execution_mode"] == "paper"


def test_late_day_without_fresh_extension_blocks(monkeypatch):
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_VALIDATION", "true")

    decision = validate_daily_intraday_continuation(
        ticker="AAPL",
        timeframe="1d",
        direction="CALL",
        trigger_price=100.0,
        current_underlying_price=100.5,
        intraday_candles=[
            candle(99.8, 100.2, 99.7, 100.1),
            candle(100.1, 100.15, 99.9, 100.05),
            candle(100.05, 100.12, 99.95, 100.06),
        ],
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        canonical_signal_id="REEVAL:late",
        now=datetime(2026, 6, 26, 14, 45),
    )

    assert decision.allowed is False
    assert decision.reason == "daily_continuation_failed:late_day_no_followthrough"
