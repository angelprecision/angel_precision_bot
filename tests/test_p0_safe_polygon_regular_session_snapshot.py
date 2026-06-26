from __future__ import annotations

import importlib.util
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_validator():
    spec = importlib.util.spec_from_file_location(
        "ap.overnight_daily_validator",
        REPO_ROOT / "ap" / "overnight_daily_validator.py",
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["ap.overnight_daily_validator"] = mod
    spec.loader.exec_module(mod)
    return mod


def _et_dt(year, month, day, hour, minute):
    return datetime(year, month, day, hour, minute, tzinfo=ZoneInfo("America/New_York"))


def _bar_ts_ms(dt_et: datetime) -> int:
    return int(dt_et.astimezone(timezone.utc).timestamp() * 1000)


class _ExplodingSession:
    def get(self, *args, **kwargs):
        raise AssertionError("Tradier session should not be used for overnight snapshot")


class _Broker:
    def __init__(self):
        self.session = _ExplodingSession()
        self.market_data_base_url = "https://api.tradier.com"
        self.quote_base_url = "https://api.tradier.com"


def test_preopen_returns_retry_later_not_invalidated(monkeypatch):
    mod = _load_validator()
    monkeypatch.setattr(mod, "_et_now", lambda: _et_dt(2026, 6, 26, 9, 15))
    result = mod._fetch_market_snapshot_result("AAPL")
    assert result.ok is False
    assert result.retry_later is True
    assert result.reason_code == "REGULAR_SESSION_NOT_OPEN"
    assert result.snapshot is None


def test_preopen_polygon_day_values_are_ignored(monkeypatch):
    mod = _load_validator()
    monkeypatch.setattr(mod, "_et_now", lambda: _et_dt(2026, 6, 26, 9, 15))
    called = {"n": 0}

    def _boom(*args, **kwargs):
        called["n"] += 1
        raise AssertionError("Polygon should not be queried before regular session open")

    monkeypatch.setattr(mod.requests, "get", _boom)
    result = mod._fetch_market_snapshot_result("AAPL")
    assert result.retry_later is True
    assert result.reason_code == "REGULAR_SESSION_NOT_OPEN"
    assert called["n"] == 0


def test_after_open_filters_out_premarket_bars(monkeypatch):
    mod = _load_validator()
    monkeypatch.setenv("POLYGON_API_KEY", "test-key")
    monkeypatch.setattr(mod, "_et_now", lambda: _et_dt(2026, 6, 26, 9, 32))

    bars = [
        {"t": _bar_ts_ms(_et_dt(2026, 6, 26, 8, 0)), "h": 999.0, "l": 1.0, "c": 50.0},
        {"t": _bar_ts_ms(_et_dt(2026, 6, 26, 9, 10)), "h": 998.0, "l": 2.0, "c": 51.0},
        {"t": _bar_ts_ms(_et_dt(2026, 6, 26, 9, 31)), "h": 102.0, "l": 97.0, "c": 101.0},
        {"t": _bar_ts_ms(_et_dt(2026, 6, 26, 9, 32)), "h": 103.0, "l": 96.0, "c": 102.0},
    ]
    monkeypatch.setattr(mod, "_fetch_polygon_regular_session_aggs", lambda *args, **kwargs: bars)

    snap = mod.fetch_market_snapshot("AAPL")
    assert snap is not None
    assert snap.session_high_so_far == 103.0
    assert snap.session_low_so_far == 96.0
    assert snap.last_price == 102.0
    assert snap.source == "polygon_regular_session_aggs"


def test_after_open_no_regular_bars_retry_later(monkeypatch):
    mod = _load_validator()
    monkeypatch.setenv("POLYGON_API_KEY", "test-key")
    monkeypatch.setattr(mod, "_et_now", lambda: _et_dt(2026, 6, 26, 9, 32))
    bars = [
        {"t": _bar_ts_ms(_et_dt(2026, 6, 26, 8, 0)), "h": 999.0, "l": 1.0, "c": 50.0},
        {"t": _bar_ts_ms(_et_dt(2026, 6, 26, 9, 10)), "h": 998.0, "l": 2.0, "c": 51.0},
    ]
    monkeypatch.setattr(mod, "_fetch_polygon_regular_session_aggs", lambda *args, **kwargs: bars)
    result = mod._fetch_market_snapshot_result("AAPL")
    assert result.ok is False
    assert result.retry_later is True
    assert result.reason_code == "REGULAR_SESSION_BARS_UNAVAILABLE"
    assert result.snapshot is None


def test_paper_does_not_use_tradier_timesales(monkeypatch):
    mod = _load_validator()
    monkeypatch.setenv("POLYGON_API_KEY", "test-key")
    monkeypatch.setattr(mod, "_et_now", lambda: _et_dt(2026, 6, 26, 9, 35))
    bars = [{"t": _bar_ts_ms(_et_dt(2026, 6, 26, 9, 31)), "h": 102.0, "l": 97.0, "c": 101.0}]
    monkeypatch.setattr(mod, "_fetch_polygon_regular_session_aggs", lambda *args, **kwargs: bars)
    snap = mod.fetch_market_snapshot("AAPL", _Broker())
    assert snap is not None
    assert snap.source == "polygon_regular_session_aggs"


def test_live_does_not_use_tradier_timesales_for_snapshot(monkeypatch):
    mod = _load_validator()
    monkeypatch.setenv("POLYGON_API_KEY", "test-key")
    monkeypatch.setattr(mod, "_et_now", lambda: _et_dt(2026, 6, 26, 9, 35))
    bars = [{"t": _bar_ts_ms(_et_dt(2026, 6, 26, 9, 31)), "h": 202.0, "l": 197.0, "c": 201.0}]
    monkeypatch.setattr(mod, "_fetch_polygon_regular_session_aggs", lambda *args, **kwargs: bars)
    snap = mod.fetch_market_snapshot("MSFT", _Broker())
    assert snap is not None
    assert snap.session_high_so_far == 202.0


def test_polygon_regular_session_snapshot_success(monkeypatch):
    mod = _load_validator()
    monkeypatch.setenv("POLYGON_API_KEY", "test-key")
    monkeypatch.setattr(mod, "_et_now", lambda: _et_dt(2026, 6, 26, 10, 5))
    bars = [
        {"t": _bar_ts_ms(_et_dt(2026, 6, 26, 9, 31)), "h": 301.0, "l": 299.0, "c": 300.0},
        {"t": _bar_ts_ms(_et_dt(2026, 6, 26, 9, 40)), "h": 304.0, "l": 298.5, "c": 303.0},
        {"t": _bar_ts_ms(_et_dt(2026, 6, 26, 10, 5)), "h": 305.0, "l": 300.0, "c": 304.0},
    ]
    monkeypatch.setattr(mod, "_fetch_polygon_regular_session_aggs", lambda *args, **kwargs: bars)
    result = mod._fetch_market_snapshot_result("NVDA")
    assert result.ok is True
    assert result.retry_later is False
    assert result.snapshot is not None
    assert result.snapshot.session_high_so_far == 305.0
    assert result.snapshot.session_low_so_far == 298.5
    assert result.snapshot.last_price == 304.0
    assert result.snapshot.source == "polygon_regular_session_aggs"
