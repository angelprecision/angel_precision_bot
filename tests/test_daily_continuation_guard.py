from ap_daily_continuation_guard import daily_signal_has_intraday_continuation


def test_non_daily_timeframe_passes_without_live_behavior_change():
    result = daily_signal_has_intraday_continuation(
        timeframe="30m",
        side="CALL",
        trigger_price=100,
        current_underlying_price=90,
    )
    assert result.allowed is True
    assert result.reason == "not_daily_timeframe"


def test_daily_call_blocks_when_back_below_trigger():
    result = daily_signal_has_intraday_continuation(
        timeframe="1d",
        side="CALL",
        trigger_price=100,
        current_underlying_price=99.99,
        intraday_candles=[{"open": 101, "high": 102, "low": 99.5, "close": 100.5}],
    )
    assert result.allowed is False
    assert result.reason == "daily_call_back_below_trigger"


def test_daily_call_allows_current_above_buffer():
    result = daily_signal_has_intraday_continuation(
        timeframe="daily",
        side="CALL",
        trigger_price=100,
        current_underlying_price=100.20,
    )
    assert result.allowed is True
    assert result.reason == "daily_call_continuation_confirmed"
    assert result.diagnostics["still_above_with_buffer"] is True


def test_daily_call_allows_reclaim_after_retest():
    result = daily_signal_has_intraday_continuation(
        timeframe="1d",
        side="CALL",
        trigger_price=100,
        current_underlying_price=100.05,
        buffer_bps=2,
        intraday_candles=[
            {"open": 100.5, "high": 101, "low": 99.80, "close": 100.04},
        ],
    )
    assert result.allowed is True
    assert result.diagnostics["reclaimed_after_retest"] is True


def test_daily_call_blocks_wick_touch_without_close_confirmation():
    result = daily_signal_has_intraday_continuation(
        timeframe="1d",
        side="CALL",
        trigger_price=100,
        current_underlying_price=100.01,
        intraday_candles=[
            {"open": 99.80, "high": 100.20, "low": 99.70, "close": 99.95},
            {"open": 99.95, "high": 100.15, "low": 99.90, "close": 100.01},
        ],
    )
    assert result.allowed is False
    assert result.reason == "daily_call_no_intraday_continuation"


def test_daily_put_blocks_when_back_above_trigger():
    result = daily_signal_has_intraday_continuation(
        timeframe="1d",
        side="PUT",
        trigger_price=100,
        current_underlying_price=100.01,
        intraday_candles=[{"open": 99, "high": 100.5, "low": 98.5, "close": 99.5}],
    )
    assert result.allowed is False
    assert result.reason == "daily_put_back_above_trigger"


def test_daily_put_allows_current_below_buffer():
    result = daily_signal_has_intraday_continuation(
        timeframe="daily",
        side="PUT",
        trigger_price=100,
        current_underlying_price=99.80,
    )
    assert result.allowed is True
    assert result.reason == "daily_put_continuation_confirmed"
    assert result.diagnostics["still_below_with_buffer"] is True


def test_daily_put_allows_reclaim_after_retest():
    result = daily_signal_has_intraday_continuation(
        timeframe="1d",
        side="PUT",
        trigger_price=100,
        current_underlying_price=99.95,
        buffer_bps=2,
        intraday_candles=[
            {"open": 99.5, "high": 100.10, "low": 99.0, "close": 99.96},
        ],
    )
    assert result.allowed is True
    assert result.diagnostics["reclaimed_after_retest"] is True


def test_missing_shape_fails_closed_with_diagnostics():
    result = daily_signal_has_intraday_continuation(
        timeframe="1d",
        side="CALL",
        trigger_price=None,
        current_underlying_price=100,
    )
    assert result.allowed is False
    assert result.reason == "daily_continuation_missing_trigger"
    assert result.diagnostics["trigger_price"] is None


def test_daily_call_allows_fresh_intraday_high_path():
    result = daily_signal_has_intraday_continuation(
        timeframe="1d",
        side="CALL",
        trigger_price=100,
        current_underlying_price=100.09,
        buffer_bps=8,
        intraday_candles=[
            {"open": 99.70, "high": 99.90, "low": 99.50, "close": 99.80},
            {"open": 99.90, "high": 100.09, "low": 99.85, "close": 100.00},
        ],
    )

    assert result.allowed is True
    assert result.reason == "daily_call_continuation_confirmed"
    assert result.diagnostics["fresh_intraday_high"] is True


def test_daily_put_allows_fresh_intraday_low_path():
    result = daily_signal_has_intraday_continuation(
        timeframe="1d",
        side="PUT",
        trigger_price=100,
        current_underlying_price=99.91,
        buffer_bps=8,
        intraday_candles=[
            {"open": 100.30, "high": 100.50, "low": 100.10, "close": 100.20},
            {"open": 100.10, "high": 100.20, "low": 99.91, "close": 100.00},
        ],
    )

    assert result.allowed is True
    assert result.reason == "daily_put_continuation_confirmed"
    assert result.diagnostics["fresh_intraday_low"] is True


def test_daily_call_allows_recent_body_confirmation_path():
    result = daily_signal_has_intraday_continuation(
        timeframe="1d",
        side="CALL",
        trigger_price=100,
        current_underlying_price=100.01,
        buffer_bps=8,
        intraday_candles=[
            {"open": 100.00, "high": 100.20, "low": 99.95, "close": 100.09},
        ],
    )

    assert result.allowed is True
    assert result.reason == "daily_call_continuation_confirmed"
    assert result.diagnostics["recent_body_confirms"] is True


def test_daily_put_allows_recent_body_confirmation_path():
    result = daily_signal_has_intraday_continuation(
        timeframe="1d",
        side="PUT",
        trigger_price=100,
        current_underlying_price=99.99,
        buffer_bps=8,
        intraday_candles=[
            {"open": 100.00, "high": 100.05, "low": 99.80, "close": 99.91},
        ],
    )

    assert result.allowed is True
    assert result.reason == "daily_put_continuation_confirmed"
    assert result.diagnostics["recent_body_confirms"] is True


def test_malformed_intraday_candles_do_not_create_false_confirmation():
    result = daily_signal_has_intraday_continuation(
        timeframe="1d",
        side="CALL",
        trigger_price=100,
        current_underlying_price=100.01,
        buffer_bps=8,
        intraday_candles=[
            {"open": 99.90, "high": None, "low": 99.80, "close": 100.50},
            {"o": 99.90, "h": "bad", "l": 99.80, "c": 100.50},
        ],
    )

    assert result.allowed is False
    assert result.reason == "daily_call_no_intraday_continuation"
    assert result.diagnostics["candles_seen"] == 0
