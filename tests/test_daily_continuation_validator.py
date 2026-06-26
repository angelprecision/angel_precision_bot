from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

from ap.daily_continuation_validator import (
    REASON_ALLOWED,
    REASON_BACK_THROUGH,
    REASON_MISSING_CONTEXT,
    REASON_OPENING_EXHAUSTED,
    REASON_TOUCH_ONLY,
    validate_daily_intraday_continuation,
    validate_daily_intraday_continuation_for_watched_signal,
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


def test_opening_breach_with_no_later_extension_blocks(monkeypatch):
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_VALIDATION", "true")

    decision = validate_daily_intraday_continuation(
        ticker="WFC",
        timeframe="1d",
        direction="CALL",
        trigger_price=77.62,
        current_underlying_price=77.80,
        intraday_candles=[
            candle(77.30, 77.70, 77.20, 77.55, "09:30"),
            candle(77.55, 77.66, 77.40, 77.61, "09:31"),
            candle(77.61, 77.65, 77.50, 77.62, "09:32"),
            candle(77.62, 77.64, 77.56, 77.60, "09:33"),
            candle(77.60, 77.63, 77.55, 77.61, "09:34"),
            candle(77.61, 77.64, 77.58, 77.63, "09:45"),
        ],
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        canonical_signal_id="REEVAL:open",
    )

    assert decision.allowed is False
    assert decision.reason == REASON_OPENING_EXHAUSTED
    assert decision.diagnostics["opening_breach"] is True


def test_current_price_above_trigger_buffer_allows_when_not_open_exhausted(monkeypatch):
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_VALIDATION", "true")

    decision = validate_daily_intraday_continuation(
        ticker="AAPL",
        timeframe="1d",
        direction="CALL",
        trigger_price=100.0,
        current_underlying_price=100.35,
        intraday_candles=[
            candle(99.80, 99.90, 99.60, 99.70),
            candle(99.70, 99.95, 99.65, 99.90),
            candle(99.90, 100.10, 99.80, 100.02),
            candle(100.02, 100.12, 99.98, 100.08),
            candle(100.08, 100.36, 100.05, 100.35),
        ],
        client_id="paper-client@example.com",
        execution_mode="paper",
        canonical_signal_id="REEVAL:buffer",
    )

    assert decision.allowed is True
    assert decision.reason == REASON_ALLOWED
    assert decision.diagnostics["holding_with_buffer"] is True


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
            candle(99.50, 99.80, 99.40, 99.70),
            candle(99.70, 99.85, 99.60, 99.75),
            candle(99.75, 99.90, 99.65, 99.80),
            candle(99.80, 99.95, 99.70, 99.90),
            candle(99.90, 99.98, 99.80, 99.95),
            candle(99.95, 100.2, 99.7, 100.1),
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


class _FakeResponse:
    status_code = 200

    def json(self):
        return {
            "series": {
                "data": {
                    "item": [
                        {"time": "09:30", "open": 99.5, "high": 100.1, "low": 99.4, "close": 100.05},
                        {"time": "09:36", "open": 100.05, "high": 100.9, "low": 100.0, "close": 100.8},
                    ]
                }
            }
        }

    def raise_for_status(self):
        return None


class _FakeSession:
    def get(self, *args, **kwargs):
        return _FakeResponse()


class _FakeBroker:
    session = _FakeSession()


def test_watched_signal_adapter_uses_real_timesales_shape_and_preserves_diagnostics(monkeypatch):
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_VALIDATION", "true")
    watched = SimpleNamespace(
        ticker="AAPL",
        side="CALL",
        entry_trigger=100.0,
        stop_level=98.0,
        signal={
            "signal_id": "sig-1",
            "canonical_signal_id": "canon-1",
            "client_id": "jasoncosby1@gmail.com",
            "execution_mode": "live",
            "timeframe": "1d",
            "local_order_id": "ord-1",
        },
    )

    decision = validate_daily_intraday_continuation_for_watched_signal(
        watched,
        _FakeBroker(),
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        canonical_signal_id="canon-1",
        now=datetime(2026, 6, 26, 10, 0),
    )

    assert decision.allowed is True
    assert decision.reason == REASON_ALLOWED
    assert decision.diagnostics["adapter"] == "watched_signal"
    assert decision.diagnostics["candles_count"] == 2
    assert decision.diagnostics["local_order_id"] == "ord-1"
    assert decision.diagnostics["client_id"] == "jasoncosby1@gmail.com"
    assert decision.diagnostics["execution_mode"] == "live"
