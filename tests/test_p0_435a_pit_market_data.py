"""P0 fail-first coverage for #435-A point-in-time market data.

These tests intentionally describe the surgical recut contract rather than the
historical #435 implementation.  The pre-fix base is expected to fail the
BREACH-specific assertions.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import math
from types import SimpleNamespace

import pytest

import ap.fvg_telemetry as fvg
import ap.intelligence_market_data as imd


AS_OF = datetime(2026, 9, 11, 17, 30, tzinfo=timezone.utc)


def _bar(open_time: datetime, *, price: float = 100.0) -> dict:
    return {
        "time": open_time.isoformat(),
        "open": price,
        "high": price + 1.0,
        "low": price - 1.0,
        "close": price + 0.25,
        "volume": 1000.0,
    }


def _series(start: datetime, *, count: int, minutes: int) -> list[dict]:
    return [
        _bar(start + timedelta(minutes=minutes * idx), price=100.0 + idx)
        for idx in range(count)
    ]


def _breach_signal(**overrides) -> dict:
    signal = {
        "ticker": "SPY",
        "symbol": "SPY",
        "side": "CALL",
        "trigger_crossed_at": AS_OF.isoformat(),
        "breach_price": 501.25,
        "underlying_price": 501.25,
        "market_symbol": "QQQ",
        "sector_etf": "XLK",
    }
    signal.update(overrides)
    return signal


class _Response:
    def __init__(self, rows: list[dict]):
        self._rows = rows

    def raise_for_status(self):
        return None

    def json(self):
        return {"series": {"data": self._rows}}


class _Session:
    def __init__(self, responder):
        self.responder = responder
        self.calls: list[dict] = []

    def get(self, _url, *, params, headers=None, timeout=None):
        self.calls.append(dict(params))
        return _Response(self.responder(dict(params), len(self.calls)))


class _Broker:
    def __init__(self, responder):
        self.session = _Session(responder)
        self.cfg = SimpleNamespace(base_url="https://api.tradier.com")


@pytest.fixture(autouse=True)
def _clear_fvg_cache():
    # Both the historical and recut implementations use these process caches.
    getattr(fvg, "_candle_cache", {}).clear()
    getattr(fvg, "_inflight", {}).clear()
    yield
    getattr(fvg, "_candle_cache", {}).clear()
    getattr(fvg, "_inflight", {}).clear()


def test_fail_first_breach_uses_exact_as_of_and_skips_current_quotes(monkeypatch):
    quote_calls: list[str] = []
    history_calls: list[datetime | None] = []
    fifteen_calls: list[datetime | None] = []
    five_calls: list[datetime | None] = []

    def fake_quote(symbol, _broker):
        quote_calls.append(symbol)
        return {"last": 999.0, "trade_timestamp": "2026-09-11T19:00:00+00:00"}

    def fake_history(_symbol, _broker, *, days=400, now=None):
        history_calls.append(now)
        return []

    def fake_15m(_ticker, _broker, *, now=None):
        fifteen_calls.append(now)
        return []

    def fake_5m(_ticker, _broker, *, now=None):
        five_calls.append(now)
        return []

    monkeypatch.setattr(imd, "_quote", fake_quote)
    monkeypatch.setattr(imd, "_history", fake_history)
    monkeypatch.setattr(fvg, "fetch_15m_bars", fake_15m)
    monkeypatch.setattr(fvg, "fetch_5m_bars", fake_5m, raising=False)

    result = imd.collect_point_in_time_context(
        _breach_signal(), broker=object(), phase="BREACH"
    )

    assert quote_calls == [], "BREACH must never read a later current quote"
    assert history_calls == [AS_OF]
    assert fifteen_calls == [AS_OF]
    assert five_calls == [AS_OF]
    assert result.get("as_of") == AS_OF.isoformat()
    observation = result.get("underlying_observation") or {}
    assert observation.get("price") == pytest.approx(501.25)
    assert observation.get("source") not in {None, "tradier_quote"}


def test_generic_prices_cannot_become_breach_truth():
    observation = imd.resolve_frozen_breach_observation(
        _breach_signal(
            breach_price=None,
            frozen_underlying_price=None,
            underlying_at_breach=None,
            price_at_breach=None,
            underlying_price=999,
            current_price=998,
        ),
        AS_OF,
    )

    assert observation["price"] is None
    assert observation["source"] is None


def test_signal_time_price_cannot_become_breach_truth():
    observation = imd.resolve_frozen_breach_observation(
        _breach_signal(
            breach_price=None,
            frozen_underlying_price=None,
            underlying_at_breach=None,
            price_at_breach=None,
            underlying_at_signal=130.50,
        ),
        AS_OF,
    )

    assert observation["price"] is None


def test_explicit_breach_price_wins_over_contradictory_generic_prices():
    observation = imd.resolve_frozen_breach_observation(
        _breach_signal(breach_price=130.50, underlying_price=999, current_price=998),
        AS_OF,
    )

    assert observation["price"] == pytest.approx(130.50)
    assert observation["source"] == "signal.breach_price"


@pytest.mark.parametrize("value", [True, False, math.nan, math.inf, -math.inf])
def test_non_finite_or_boolean_breach_prices_are_rejected(value):
    observation = imd.resolve_frozen_breach_observation(
        _breach_signal(
            breach_price=value,
            frozen_underlying_price=None,
            underlying_at_breach=None,
            price_at_breach=None,
            underlying_price=999,
            current_price=998,
        ),
        AS_OF,
    )

    assert observation["price"] is None
    assert observation["source"] is None


def test_future_breach_price_companion_timestamp_is_rejected():
    observation = imd.resolve_frozen_breach_observation(
        _breach_signal(
            breach_price=130.50,
            breach_price_at=(AS_OF + timedelta(minutes=1)).isoformat(),
            frozen_underlying_price=None,
            underlying_at_breach=None,
            price_at_breach=None,
            underlying_price=999,
            current_price=998,
        ),
        AS_OF,
    )

    assert observation["price"] is None
    assert observation["source"] is None


@pytest.mark.parametrize("timestamp", ["2026-09-11T17:30:00", "not-a-timestamp"])
def test_naive_or_malformed_breach_price_companion_timestamp_is_rejected(timestamp):
    observation = imd.resolve_frozen_breach_observation(
        _breach_signal(
            breach_price=130.50,
            breach_price_at=timestamp,
            frozen_underlying_price=None,
            underlying_at_breach=None,
            price_at_breach=None,
            underlying_price=999,
            current_price=998,
        ),
        AS_OF,
    )

    assert observation["price"] is None


def test_valid_mapping_breach_evidence_preserves_source_timestamp():
    timestamp = "2026-09-11T17:29:00+00:00"
    observation = imd.resolve_frozen_breach_observation(
        _breach_signal(
            breach_price=None,
            frozen_underlying_price=None,
            underlying_at_breach=None,
            price_at_breach=None,
            breach_evidence={"price": 130.50, "timestamp": timestamp},
        ),
        AS_OF,
    )

    assert observation["price"] == pytest.approx(130.50)
    assert observation["source"] == "signal.breach_evidence"
    assert observation["source_timestamp"] == timestamp


@pytest.mark.parametrize(
    "evidence",
    [
        {"price": 130.50},
        {"price": 130.50, "timestamp": "2026-09-11T17:29:00"},
        {"price": 130.50, "timestamp": "not-a-timestamp"},
        {
            "price": 130.50,
            "timestamp": (AS_OF + timedelta(minutes=1)).isoformat(),
        },
    ],
)
def test_invalid_mapping_breach_evidence_timestamp_is_rejected(evidence):
    observation = imd.resolve_frozen_breach_observation(
        _breach_signal(
            breach_price=None,
            frozen_underlying_price=None,
            underlying_at_breach=None,
            price_at_breach=None,
            breach_evidence=evidence,
            underlying_price=999,
            current_price=998,
        ),
        AS_OF,
    )

    assert observation["price"] is None
    assert observation["source"] is None


def test_production_shaped_breach_result_uses_only_frozen_breach_authority(monkeypatch):
    quote_calls: list[str] = []

    def fail_quote(symbol, _broker):
        quote_calls.append(symbol)
        raise AssertionError("BREACH must not read current quotes")

    monkeypatch.setattr(imd, "_quote", fail_quote)
    monkeypatch.setattr(imd, "_history", lambda *_a, **_k: [])
    monkeypatch.setattr(fvg, "fetch_15m_bars", lambda *_a, **_k: [])
    monkeypatch.setattr(fvg, "fetch_5m_bars", lambda *_a, **_k: [], raising=False)

    result = imd.collect_point_in_time_context(
        _breach_signal(
            breach_price=130.50,
            underlying_price=999,
            current_price=998,
        ),
        broker=object(),
        phase="BREACH",
    )

    assert result["as_of"] == AS_OF.isoformat()
    candles = result["data_sources"]["candles"]
    assert all(timeframe in candles for timeframe in ("5m", "15m", "1h", "4h"))
    observation = result["underlying_observation"]
    assert observation["price"] == pytest.approx(130.50)
    assert observation["source"] == "signal.breach_price"
    assert result["provenance"]["quote"] is None
    assert quote_calls == []


def test_breach_snapshot_excludes_candle_completed_after_trigger():
    trigger = datetime(2026, 9, 11, 10, 2, tzinfo=timezone.utc)
    confirmation_as_of = datetime(2026, 9, 11, 10, 5, tzinfo=timezone.utc)
    candle = _bar(datetime(2026, 9, 11, 10, 0, tzinfo=timezone.utc), price=130.50)
    signal = _breach_signal(
        trigger_crossed_at=trigger.isoformat(),
        candles_5m=[candle],
    )

    breach_snapshot = imd.collect_point_in_time_context(
        signal, broker=None, phase="BREACH"
    )

    assert breach_snapshot["as_of"] == trigger.isoformat()
    assert breach_snapshot["data_sources"]["candles"]["5m"] == []
    assert imd._filter_completed_bars(
        [candle], interval_minutes=5, as_of=confirmation_as_of
    ) == [candle]


@pytest.mark.parametrize(
    "bad_as_of",
    [None, "", "not-a-timestamp", "2026-09-11T17:30:00"],
)
def test_breach_invalid_as_of_touches_no_market_data(monkeypatch, bad_as_of):
    calls: list[str] = []

    def touched(*_args, **_kwargs):
        calls.append("touched")
        return []

    monkeypatch.setattr(imd, "_quote", touched)
    monkeypatch.setattr(imd, "_history", touched)
    monkeypatch.setattr(fvg, "fetch_15m_bars", touched)
    monkeypatch.setattr(fvg, "fetch_5m_bars", touched, raising=False)

    result = imd.collect_point_in_time_context(
        _breach_signal(trigger_crossed_at=bad_as_of),
        broker=object(),
        phase="BREACH",
    )

    assert calls == []
    assert result.get("as_of") is None
    assert any(
        "breach" in str(item).lower() and "as_of" in str(item).lower()
        for item in (result.get("errors") or [])
    )
    candles = ((result.get("data_sources") or {}).get("candles") or {})
    assert candles.get("5m", []) == []
    assert candles.get("15m", []) == []
    assert candles.get("1h", []) == []
    assert candles.get("4h", []) == []


def test_breach_frozen_rows_exclude_incomplete_5m_and_15m_and_derive_htf():
    # 09:30-13:30 ET (13:30-17:30 UTC) supplies one complete session-anchored
    # 4H bar and four complete 1H bars at AS_OF.  The bar opening exactly at
    # AS_OF is future/incomplete and must not enter any derived timeframe.
    fifteen = _series(
        datetime(2026, 9, 11, 13, 30, tzinfo=timezone.utc),
        count=17,
        minutes=15,
    )
    five = _series(
        datetime(2026, 9, 11, 17, 10, tzinfo=timezone.utc),
        count=5,
        minutes=5,
    )
    result = imd.collect_point_in_time_context(
        _breach_signal(candles_15m=fifteen, candles_5m=five),
        broker=None,
        phase="BREACH",
    )

    candles = result["data_sources"]["candles"]
    assert len(candles["15m"]) == 16
    assert len(candles["5m"]) == 4
    assert len(candles["1h"]) == 4
    assert len(candles["4h"]) == 1
    assert candles["15m"][-1]["time"] == "2026-09-11T17:15:00+00:00"
    assert candles["5m"][-1]["time"] == "2026-09-11T17:25:00+00:00"
    assert all(row["time"] != AS_OF.isoformat() for row in candles["15m"])
    assert all(row["time"] != AS_OF.isoformat() for row in candles["5m"])


def test_breach_network_rows_are_refiltered_even_if_transport_returns_partial(monkeypatch):
    fifteen = [
        _bar(datetime(2026, 9, 11, 17, 0, tzinfo=timezone.utc)),
        _bar(datetime(2026, 9, 11, 17, 15, tzinfo=timezone.utc)),
        _bar(datetime(2026, 9, 11, 17, 30, tzinfo=timezone.utc)),
    ]
    five = [
        _bar(datetime(2026, 9, 11, 17, 20, tzinfo=timezone.utc)),
        _bar(datetime(2026, 9, 11, 17, 25, tzinfo=timezone.utc)),
        _bar(datetime(2026, 9, 11, 17, 30, tzinfo=timezone.utc)),
    ]

    monkeypatch.setattr(imd, "_quote", lambda *_a, **_k: (_ for _ in ()).throw(
        AssertionError("BREACH current quote must not be called")
    ))
    monkeypatch.setattr(imd, "_history", lambda *_a, **_k: [])
    monkeypatch.setattr(fvg, "fetch_15m_bars", lambda *_a, **_k: list(fifteen))
    monkeypatch.setattr(fvg, "fetch_5m_bars", lambda *_a, **_k: list(five), raising=False)

    result = imd.collect_point_in_time_context(
        _breach_signal(), broker=object(), phase="BREACH"
    )
    candles = result["data_sources"]["candles"]
    assert [row["time"] for row in candles["15m"]] == [
        "2026-09-11T17:00:00+00:00",
        "2026-09-11T17:15:00+00:00",
    ]
    assert [row["time"] for row in candles["5m"]] == [
        "2026-09-11T17:20:00+00:00",
        "2026-09-11T17:25:00+00:00",
    ]


def test_history_request_can_be_bounded_to_breach_as_of_date():
    seen: list[dict] = []

    class Source:
        def _get(self, _path, *, params):
            seen.append(dict(params))
            return {"history": {"day": []}}

    broker = SimpleNamespace(data_broker=Source())
    rows = imd._history("SPY", broker, now=AS_OF)
    assert rows == []
    assert seen[0]["end"] == "2026-09-11"


def test_fail_first_15m_cache_crossing_completed_boundary_refetches(monkeypatch):
    first_rows = [
        _bar(datetime(2026, 9, 11, 13, 30, tzinfo=timezone.utc)),
        _bar(datetime(2026, 9, 11, 13, 45, tzinfo=timezone.utc)),
    ]
    second_rows = first_rows + [
        _bar(datetime(2026, 9, 11, 14, 0, tzinfo=timezone.utc)),
    ]

    def responder(params, call_number):
        assert params["interval"] == "15min"
        return first_rows if call_number == 1 else second_rows

    broker = _Broker(responder)
    monkeypatch.setattr(fvg, "_resolve_base_url", lambda _broker: "https://api.tradier.com")

    early = datetime(2026, 9, 11, 14, 10, tzinfo=timezone.utc)
    later = datetime(2026, 9, 11, 14, 20, tzinfo=timezone.utc)
    first = fvg.fetch_15m_bars("SPY", broker, now=early)
    second = fvg.fetch_15m_bars("SPY", broker, now=later)

    assert len(broker.session.calls) == 2, (
        "a later completed 15m boundary must not reuse earlier ticker-only cache"
    )
    assert first[-1]["time"] == "2026-09-11T13:45:00+00:00"
    assert second[-1]["time"] == "2026-09-11T14:00:00+00:00"


def test_tradier_naive_local_time_is_normalized_before_pit_filter(monkeypatch):
    # Tradier timesales uses exchange-local ISO timestamps without an offset.
    # At 13:30 ET, the 13:15 bar is complete and the 13:30 bar is not.
    rows = [
        {"time": "2026-09-11T13:15:00", "open": 100, "high": 101, "low": 99, "close": 100.5},
        {"time": "2026-09-11T13:30:00", "open": 100.5, "high": 102, "low": 100, "close": 101.5},
    ]
    broker = _Broker(lambda _params, _call_number: rows)
    monkeypatch.setattr(fvg, "_resolve_base_url", lambda _broker: "https://api.tradier.com")

    bars = fvg.fetch_15m_bars("SPY", broker, now=AS_OF)

    assert [row["time"] for row in bars] == ["2026-09-11T13:15:00-04:00"]


def test_5m_and_15m_cache_namespaces_cannot_collide(monkeypatch):
    fetch_5m = getattr(fvg, "fetch_5m_bars", None)
    assert callable(fetch_5m), "#435-A must expose the bounded 5m compatibility wrapper"

    def responder(params, _call_number):
        if params["interval"] == "5min":
            return [_bar(datetime(2026, 9, 11, 14, 5, tzinfo=timezone.utc))]
        if params["interval"] == "15min":
            return [_bar(datetime(2026, 9, 11, 13, 45, tzinfo=timezone.utc))]
        raise AssertionError(params["interval"])

    broker = _Broker(responder)
    monkeypatch.setattr(fvg, "_resolve_base_url", lambda _broker: "https://api.tradier.com")
    as_of = datetime(2026, 9, 11, 14, 15, tzinfo=timezone.utc)

    five = fetch_5m("SPY", broker, now=as_of)
    fifteen = fvg.fetch_15m_bars("SPY", broker, now=as_of)

    assert five and fifteen
    assert [call["interval"] for call in broker.session.calls] == ["5min", "15min"]


def test_live_15m_same_ticker_retains_existing_ttl_coalescing(monkeypatch):
    rows = [_bar(datetime(2026, 9, 11, 14, 0, tzinfo=timezone.utc))]
    broker = _Broker(lambda _params, _call_number: rows)
    monkeypatch.setattr(fvg, "_resolve_base_url", lambda _broker: "https://api.tradier.com")

    first = fvg.fetch_15m_bars("SPY", broker)
    second = fvg.fetch_15m_bars("SPY", broker)

    assert first == second
    assert len(broker.session.calls) == 1


def test_pretrigger_current_context_behavior_is_preserved(monkeypatch):
    quote_calls: list[str] = []
    fifteen_calls: list[datetime | None] = []
    five_calls: list[datetime | None] = []

    def fake_quote(symbol, _broker):
        quote_calls.append(symbol)
        return {"last": 500.0, "change_percentage": 0.25}

    monkeypatch.setattr(imd, "_quote", fake_quote)
    monkeypatch.setattr(imd, "_history", lambda *_a, **_k: [])
    monkeypatch.setattr(
        fvg,
        "fetch_15m_bars",
        lambda _ticker, _broker, *, now=None: fifteen_calls.append(now) or [],
    )
    monkeypatch.setattr(
        fvg,
        "fetch_5m_bars",
        lambda _ticker, _broker, *, now=None: five_calls.append(now) or [],
        raising=False,
    )

    result = imd.collect_point_in_time_context(
        {
            "ticker": "SPY",
            "market_symbol": "QQQ",
            "sector_etf": "XLK",
        },
        broker=object(),
        phase="PRETRIGGER",
    )

    assert quote_calls == ["SPY", "QQQ", "XLK"]
    assert fifteen_calls == [None]
    assert five_calls == [], "#435-A must not add a 5m transport cost to PRETRIGGER"
    assert result["underlying_observation"]["price"] == pytest.approx(500.0)


def test_market_data_failures_remain_fail_soft(monkeypatch):
    monkeypatch.setattr(imd, "_quote", lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("quote down")))
    monkeypatch.setattr(imd, "_history", lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("history down")))
    monkeypatch.setattr(fvg, "fetch_15m_bars", lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("timesales down")))

    result = imd.collect_point_in_time_context(
        {"ticker": "SPY"}, broker=object(), phase="PRETRIGGER"
    )

    assert result["phase"] == "PRETRIGGER"
    assert result["errors"]
    assert result["data_sources"]["candles"]["1h"] == []
    assert result["data_sources"]["candles"]["4h"] == []
