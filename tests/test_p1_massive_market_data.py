"""Fail-closed transport, geometry, and cache proof for Massive 4H data."""

from __future__ import annotations

import ast
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
import threading
import time

import pytest
import requests

import ap.massive_market_data as mm
from ap.massive_market_data import (
    MassiveAggregate,
    MassiveClient,
    MassiveError,
    build_native_4h_bars,
    build_rth_4h_bars,
    clear_massive_caches_for_tests,
    compare_native_to_rth,
    deserialize_canonical_4h_bar_set,
    get_canonical_4h_bars,
    resolve_massive_api_key,
    serialize_canonical_4h_bar_set,
    session_window,
)


UTC = timezone.utc


def _millis(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def _raw_bar(started_at: datetime, *, price: float = 100.0, volume: float = 100.0) -> dict:
    return {
        "t": _millis(started_at),
        "o": price,
        "h": price + 1.0,
        "l": price - 1.0,
        "c": price + 0.5,
        "v": volume,
        "vw": price + 0.25,
        "n": 10,
    }


def _payload(rows: list[dict], *, adjusted: bool = True, status: str = "OK") -> dict:
    return {
        "adjusted": adjusted,
        "queryCount": 1,
        "resultsCount": len(rows),
        "results": rows,
        "status": status,
    }


class _Response:
    def __init__(self, payload: dict, *, status_code: int = 200, headers: dict | None = None):
        self._payload = payload
        self.status_code = status_code
        self.headers = headers or {}

    def json(self):
        return self._payload


class _Session:
    def __init__(self, responder, *, delay: float = 0.0):
        self.responder = responder
        self.delay = delay
        self.calls: list[dict] = []
        self._lock = threading.Lock()

    def get(self, url, *, params, headers=None, timeout=None):
        with self._lock:
            self.calls.append(
                {
                    "url": url,
                    "params": dict(params or {}),
                    "headers": dict(headers or {}),
                    "timeout": timeout,
                }
            )
            call_number = len(self.calls)
        if self.delay:
            time.sleep(self.delay)
        return self.responder(url, params, headers, call_number)


def _provider_session(rows: list[dict], *, status_code: int = 200, headers: dict | None = None) -> _Session:
    return _Session(lambda *_args: _Response(_payload(rows), status_code=status_code, headers=headers))


def _aggregate(
    started_at: datetime,
    *,
    price: float = 100.0,
    volume: float = 100.0,
    interval_minutes: int = 15,
) -> MassiveAggregate:
    return MassiveAggregate(
        ticker="SPY",
        started_at=started_at.astimezone(UTC),
        open=price,
        high=price + 1.0,
        low=price - 1.0,
        close=price + 0.5,
        volume=volume,
        vwap=price + 0.25,
        transactions=10,
        adjusted=True,
        source_bar_id=f"source-{_millis(started_at)}-{price}",
        interval_minutes=interval_minutes,
    )


def _rth_day(day: date, *, price: float = 100.0, count: int | None = None) -> list[MassiveAggregate]:
    window = session_window(day)
    assert window is not None
    total = count or int((window.close_at - window.open_at).total_seconds() // 900)
    return [
        _aggregate(window.open_at + timedelta(minutes=15 * index), price=price + index)
        for index in range(total)
    ]


@pytest.fixture(autouse=True)
def _clear_cache(monkeypatch):
    clear_massive_caches_for_tests()
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    monkeypatch.delenv("POLYGON_API_KEY", raising=False)
    monkeypatch.delenv("MASSIVE_4H_ALIGNMENT", raising=False)
    yield
    clear_massive_caches_for_tests()


def test_valid_response_is_strictly_normalized_and_header_authenticated():
    started = datetime(2026, 3, 9, 13, 30, tzinfo=UTC)
    session = _provider_session([_raw_bar(started), _raw_bar(started + timedelta(hours=4), price=101.0)])
    client = MassiveClient(api_key="unit-test-secret", session=session)

    result = client.fetch_aggregates(
        "spy",
        multiplier=4,
        timespan="hour",
        from_date=date(2026, 3, 9),
        to_date=date(2026, 3, 10),
    )

    assert [bar.ticker for bar in result.bars] == ["SPY", "SPY"]
    assert result.bars[0].started_at == started
    assert result.bars[0].adjusted is True
    assert result.diagnostics["schema_keys"] == ["c", "h", "l", "n", "o", "t", "v", "vw"]
    assert session.calls[0]["headers"]["Authorization"] == "Bearer unit-test-secret"
    assert "apiKey" not in session.calls[0]["params"]
    assert "unit-test-secret" not in repr(result.diagnostics)

    reordered = dict(reversed(list(_raw_bar(started).items())))
    left = client._normalize_row("SPY", _raw_bar(started), adjusted=True)
    right = client._normalize_row("SPY", reordered, adjusted=True)
    assert left.source_bar_id == right.source_bar_id


def test_pagination_cursor_cannot_move_auth_into_a_url():
    started = datetime(2026, 3, 9, 13, 30, tzinfo=UTC)

    def responder(_url, _params, _headers, call_number):
        if call_number == 1:
            payload = _payload([_raw_bar(started)])
            payload["next_url"] = (
                "https://api.massive.com/v2/aggs/ticker/SPY/range/4/hour/2026-03-09/2026-03-10"
                "?cursor=next&apiKey=provider-key-not-url"
            )
            return _Response(payload)
        return _Response(_payload([_raw_bar(started + timedelta(hours=4), price=101.0)]))

    session = _Session(responder)
    result = MassiveClient(api_key="provider-key-not-url", session=session).fetch_aggregates(
        "SPY", multiplier=4, timespan="hour", from_date=date(2026, 3, 9), to_date=date(2026, 3, 10)
    )
    assert len(result.bars) == 2
    assert "apiKey" not in session.calls[1]["params"]
    assert "provider-key-not-url" not in session.calls[1]["url"]


def test_identical_duplicates_are_idempotent_but_conflicts_and_unordered_rows_hold():
    started = datetime(2026, 3, 9, 13, 30, tzinfo=UTC)

    duplicate_session = _provider_session([_raw_bar(started), _raw_bar(started)])
    duplicate_client = MassiveClient(api_key="secret", session=duplicate_session)
    duplicate = duplicate_client.fetch_aggregates(
        "SPY", multiplier=4, timespan="hour", from_date=date(2026, 3, 9), to_date=date(2026, 3, 9)
    )
    assert len(duplicate.bars) == 1

    conflict_session = _provider_session([_raw_bar(started), _raw_bar(started, price=101.0)])
    with pytest.raises(MassiveError, match="massive_duplicate_conflict"):
        MassiveClient(api_key="secret", session=conflict_session).fetch_aggregates(
            "SPY", multiplier=4, timespan="hour", from_date=date(2026, 3, 9), to_date=date(2026, 3, 9)
        )

    unordered_session = _provider_session([_raw_bar(started + timedelta(hours=4)), _raw_bar(started)])
    with pytest.raises(MassiveError, match="massive_unordered_results"):
        MassiveClient(api_key="secret", session=unordered_session).fetch_aggregates(
            "SPY", multiplier=4, timespan="hour", from_date=date(2026, 3, 9), to_date=date(2026, 3, 9)
        )


@pytest.mark.parametrize(
    ("row_update", "reason"),
    [
        ({"o": "nan"}, "ohlcv_invalid"),
        ({"h": 99.0}, "ohlc_geometry_invalid"),
        ({"t": 1_700_000_000}, "timestamp_invalid"),
        ({"n": -1}, "transactions_invalid"),
    ],
)
def test_malformed_provider_rows_fail_closed(row_update, reason):
    started = datetime(2026, 3, 9, 13, 30, tzinfo=UTC)
    row = _raw_bar(started)
    row.update(row_update)
    client = MassiveClient(api_key="secret", session=_provider_session([row]))

    with pytest.raises(MassiveError, match=f"massive_{reason}"):
        client.fetch_aggregates(
            "SPY", multiplier=4, timespan="hour", from_date=date(2026, 3, 9), to_date=date(2026, 3, 9)
        )


@pytest.mark.parametrize(
    ("status_code", "reason"),
    [(401, "authentication_failed"), (403, "authentication_failed"), (429, "rate_limited"), (503, "provider_5xx")],
)
def test_provider_status_failures_are_sanitized(status_code, reason):
    session = _provider_session([], status_code=status_code, headers={"Retry-After": "7", "X-RateLimit-Limit": "5"})
    with pytest.raises(MassiveError) as caught:
        MassiveClient(api_key="secret-not-for-output", session=session).fetch_aggregates(
            "SPY", multiplier=4, timespan="hour", from_date=date(2026, 3, 9), to_date=date(2026, 3, 9)
        )
    error = caught.value
    assert error.reason_code == reason
    assert error.status_code == status_code
    if status_code == 429:
        assert error.diagnostic()["rate_limits"]["retry_after"] == "7"
    assert "secret-not-for-output" not in str(error)
    assert "secret-not-for-output" not in repr(error.diagnostic())


def test_timeout_and_malformed_json_are_safe():
    timeout_session = _Session(lambda *_args: (_ for _ in ()).throw(requests.exceptions.Timeout("secret")))
    with pytest.raises(MassiveError, match="massive_timeout"):
        MassiveClient(api_key="secret", session=timeout_session).fetch_aggregates(
            "SPY", multiplier=4, timespan="hour", from_date=date(2026, 3, 9), to_date=date(2026, 3, 9)
        )

    malformed_session = _Session(lambda *_args: _Response([], status_code=200))
    with pytest.raises(MassiveError, match="massive_malformed_payload"):
        MassiveClient(api_key="secret", session=malformed_session).fetch_aggregates(
            "SPY", multiplier=4, timespan="hour", from_date=date(2026, 3, 9), to_date=date(2026, 3, 9)
        )


def test_empty_response_is_explicit_and_adjustment_semantics_are_required():
    empty = MassiveClient(api_key="secret", session=_provider_session([])).fetch_aggregates(
        "SPY", multiplier=4, timespan="hour", from_date=date(2026, 3, 9), to_date=date(2026, 3, 9)
    )
    assert empty.bars == ()
    assert empty.diagnostics["result_count"] == 0

    unadjusted_session = _Session(lambda *_args: _Response(_payload([], adjusted=False)))
    with pytest.raises(MassiveError, match="massive_unadjusted_response"):
        MassiveClient(api_key="secret", session=unadjusted_session).fetch_aggregates(
            "SPY", multiplier=4, timespan="hour", from_date=date(2026, 3, 9), to_date=date(2026, 3, 9)
        )


def test_credentials_are_environment_only_and_ambiguous_aliases_hold():
    with pytest.raises(MassiveError, match="massive_credential_missing"):
        resolve_massive_api_key({})
    assert resolve_massive_api_key({"MASSIVE_API_KEY": "same"}) == ("same", "MASSIVE_API_KEY")
    assert resolve_massive_api_key({"POLYGON_API_KEY": "same"}) == ("same", "POLYGON_API_KEY")
    with pytest.raises(MassiveError, match="massive_ambiguous_credentials"):
        resolve_massive_api_key({"MASSIVE_API_KEY": "one", "POLYGON_API_KEY": "two"})


def test_rth_fallback_excludes_extended_hours_and_builds_session_anchored_buckets():
    day = date(2026, 7, 1)
    window = session_window(day)
    assert window is not None and window.early_close is False
    source = [_aggregate(window.open_at - timedelta(minutes=15), price=90.0)]
    source.extend(_rth_day(day))
    source.append(_aggregate(window.close_at, price=200.0))

    bars = build_rth_4h_bars(
        source,
        ticker="SPY",
        as_of=datetime(2026, 7, 1, 21, 0, tzinfo=UTC),
    )

    assert len(bars) == 2
    assert bars[0].started_at == datetime(2026, 7, 1, 13, 30, tzinfo=UTC)
    assert bars[0].completed_at == datetime(2026, 7, 1, 17, 30, tzinfo=UTC)
    assert bars[0].constituent_count == bars[0].expected_constituent_count == 16
    assert bars[1].constituent_count == bars[1].expected_constituent_count == 10
    assert bars[0].open == pytest.approx(100.0)
    assert bars[0].close == pytest.approx(115.5)


def test_missing_rth_interval_does_not_become_a_partial_canonical_bar():
    rows = _rth_day(date(2026, 7, 1))
    del rows[5]
    bars = build_rth_4h_bars(rows, ticker="SPY", as_of=datetime(2026, 7, 1, 21, tzinfo=UTC))
    assert len(bars) == 1
    assert bars[0].started_at == datetime(2026, 7, 1, 17, 30, tzinfo=UTC)


def test_early_close_holiday_and_dst_geometry_are_explicit():
    early_day = date(2026, 7, 2)
    early_window = session_window(early_day)
    assert early_window is not None and early_window.early_close is True
    early_bars = build_rth_4h_bars(
        _rth_day(early_day), ticker="SPY", as_of=datetime(2026, 7, 2, 21, tzinfo=UTC)
    )
    assert len(early_bars) == 1
    assert early_bars[0].completed_at == datetime(2026, 7, 2, 17, tzinfo=UTC)
    assert early_bars[0].constituent_count == early_bars[0].expected_constituent_count == 14
    assert session_window(date(2026, 7, 3)) is None

    before_dst = session_window(date(2026, 3, 6))
    after_dst = session_window(date(2026, 3, 9))
    assert before_dst is not None and after_dst is not None
    assert before_dst.open_at.astimezone(UTC).hour == 14
    assert after_dst.open_at.astimezone(UTC).hour == 13


def test_native_alignment_is_comparable_but_misaligned_native_bars_hold():
    fallback = build_rth_4h_bars(
        _rth_day(date(2026, 7, 1)), ticker="SPY", as_of=datetime(2026, 7, 1, 21, tzinfo=UTC)
    )
    native_source = [
        _aggregate(bar.started_at, price=bar.open, volume=bar.volume, interval_minutes=240)
        for bar in fallback
    ]
    native = build_native_4h_bars(
        native_source, ticker="SPY", as_of=datetime(2026, 7, 1, 21, tzinfo=UTC)
    )
    # The compact native fixture above uses the same OHLC shape only for the
    # first field; force exact comparison through dataclass replacement.
    native = tuple(
        bar.__class__(
            ticker=bar.ticker,
            timeframe=bar.timeframe,
            open=source.open,
            high=source.high,
            low=source.low,
            close=source.close,
            volume=source.volume,
            vwap=source.vwap,
            transactions=source.transactions,
            started_at=bar.started_at,
            completed_at=bar.completed_at,
            source=bar.source,
            adjusted=bar.adjusted,
            provider_schema=bar.provider_schema,
            alignment_version=bar.alignment_version,
            source_bar_id=bar.source_bar_id,
            constituent_count=bar.constituent_count,
            expected_constituent_count=bar.expected_constituent_count,
        )
        for bar, source in zip(native, fallback)
    )
    comparison = compare_native_to_rth(native, fallback)
    assert comparison["aligned"] is True
    assert comparison["mismatch_count"] == 0

    with pytest.raises(MassiveError, match="massive_native_alignment_mismatch"):
        build_native_4h_bars(
            [_aggregate(datetime(2026, 7, 1, 16, tzinfo=UTC))],
            ticker="SPY",
            as_of=datetime(2026, 7, 1, 21, tzinfo=UTC),
        )


def test_same_ticker_refresh_is_singleflight_and_different_tickers_are_isolated(monkeypatch):
    rows = [_raw_bar(datetime(2026, 7, 1, 13, 30, tzinfo=UTC) + timedelta(minutes=15 * i)) for i in range(26)]

    def responder(_url, _params, _headers, _call_number):
        return _Response(_payload(rows))

    session = _Session(responder, delay=0.05)
    client = MassiveClient(api_key="shared-secret", session=session)
    cutoff = datetime(2026, 7, 1, 21, tzinfo=UTC)
    barrier = threading.Barrier(8)

    def read_same_ticker():
        barrier.wait()
        return get_canonical_4h_bars("SPY", as_of=cutoff, lookback_days=5, client=client)

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _index: read_same_ticker(), range(8)))
    assert len(session.calls) == 1
    assert all(result.status == "AVAILABLE" for result in results)
    assert all(result.bars == results[0].bars for result in results)

    get_canonical_4h_bars("QQQ", as_of=cutoff, lookback_days=5, client=client)
    assert len(session.calls) == 2


def test_refresh_failure_keeps_last_good_as_stale_without_claiming_adverse_truth(monkeypatch):
    rows = [_raw_bar(datetime(2026, 7, 1, 13, 30, tzinfo=UTC) + timedelta(minutes=15 * i)) for i in range(26)]
    state = {"fail": False}

    def responder(_url, _params, _headers, _call_number):
        if state["fail"]:
            return _Response({}, status_code=503)
        return _Response(_payload(rows))

    session = _Session(responder)
    client = MassiveClient(api_key="shared-secret", session=session)
    cutoff = datetime(2026, 7, 1, 21, tzinfo=UTC)
    good = get_canonical_4h_bars("SPY", as_of=cutoff, lookback_days=5, client=client)
    assert good.status == "AVAILABLE"

    state["fail"] = True
    stale = get_canonical_4h_bars("SPY", as_of=cutoff, lookback_days=5, client=client, force_refresh=True)
    assert stale.status == "STALE"
    assert stale.reason == "provider_5xx"
    assert stale.bars == good.bars
    assert stale.diagnostics["status"] == "ERROR"
    assert stale.diagnostics["status_code"] == 503

    restored = deserialize_canonical_4h_bar_set(serialize_canonical_4h_bar_set(good))
    assert restored.bars == good.bars
    assert restored.to_dict() == good.to_dict()
    tampered = serialize_canonical_4h_bar_set(good)
    tampered["alignment_version"] = "wrong"
    with pytest.raises(MassiveError, match="massive_cache_alignment_invalid"):
        deserialize_canonical_4h_bar_set(tampered)


def test_cache_namespace_isolates_credentials_and_missing_live_key_is_unavailable(monkeypatch):
    rows = [_raw_bar(datetime(2026, 7, 1, 13, 30, tzinfo=UTC) + timedelta(minutes=15 * i)) for i in range(26)]
    session = _provider_session(rows)
    cutoff = datetime(2026, 7, 1, 21, tzinfo=UTC)
    first = get_canonical_4h_bars(
        "SPY", as_of=cutoff, lookback_days=5, client=MassiveClient(api_key="one", session=session)
    )
    second = get_canonical_4h_bars(
        "SPY", as_of=cutoff, lookback_days=5, client=MassiveClient(api_key="two", session=session)
    )
    assert first.status == second.status == "AVAILABLE"
    assert len(session.calls) == 2

    clear_massive_caches_for_tests()
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    monkeypatch.delenv("POLYGON_API_KEY", raising=False)
    missing = get_canonical_4h_bars("SPY", as_of=cutoff, lookback_days=5)
    assert missing.status == "UNAVAILABLE"
    assert missing.reason == "credential_missing"
    assert missing.provider_status == "NOT_REQUESTED"


def test_market_data_module_has_no_money_path_imports():
    tree = ast.parse(open(mm.__file__, encoding="utf-8").read())
    imported = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.append(node.module or "")
    forbidden = ("watcher", "selector", "broker", "order", "position", "queue", "proof")
    assert not any(any(word in module.lower() for word in forbidden) for module in imported)
