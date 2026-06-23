from ap.models import Signal


BASE = {
    "signal_id": "sig-1",
    "symbol": "AAPL",
    "direction": "CALL",
    "pattern_id": "2-3",
    "timestamp_iso": "2026-06-21T12:00:00Z",
}


def test_signal_without_timeframe_defaults_to_1d():
    sig = Signal(**BASE)
    payload = sig.model_dump()

    assert sig.timeframe == "1d"
    assert payload["timeframe"] == "1d"


def test_signal_with_daily_timeframe_preserves_it():
    sig = Signal(**{**BASE, "timeframe": "1d"})

    assert sig.timeframe == "1d"


def test_signal_with_intraday_or_weekly_timeframe_preserves_it():
    intraday = Signal(**{**BASE, "timeframe": "15m"})
    weekly = Signal(**{**BASE, "timeframe": "1wk"})

    assert intraday.timeframe == "15m"
    assert weekly.timeframe == "1wk"


def test_entry_fields_and_trigger_round_trip():
    sig = Signal(
        **{
            **BASE,
            "timeframe": "1d",
            "entry_price": 210.25,
            "stop_price": 206.10,
            "target_price": 218.40,
        }
    )
    payload = sig.model_dump()

    assert payload["entry_price"] == 210.25
    assert payload["entry_trigger"] == 210.25
    assert payload["trigger"]["entry"] == 210.25
    assert payload["trigger"]["stop"] == 206.10
    assert payload["trigger"]["target"] == 218.40
    assert payload["trigger"]["pt1"] == 218.40
