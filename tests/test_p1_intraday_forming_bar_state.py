"""Focused acceptance matrix for PR #617's transport-agnostic bar state."""

from __future__ import annotations

import copy
import importlib.util
import sys
from datetime import date, datetime, time, timezone
from pathlib import Path

import pytest


_MODULE_NAME = "_ap_intraday_bar_state_under_test"
_MODULE_PATH = Path(__file__).parents[1] / "ap" / "intraday_bar_state.py"
_SPEC = importlib.util.spec_from_file_location(_MODULE_NAME, _MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
bar_state = importlib.util.module_from_spec(_SPEC)
sys.modules[_MODULE_NAME] = bar_state
_SPEC.loader.exec_module(bar_state)


ET = bar_state.ET
DAY = date(2026, 9, 11)


class StaticCalendar:
    """Test exchange authority with explicit full-closure and early-close truth."""

    def __init__(self, *, closed=(), early_closes=None):
        self.closed = set(closed)
        self.early_closes = dict(early_closes or {})

    def session_for(self, session_date):
        if session_date.weekday() >= 5 or session_date in self.closed:
            return None
        close = self.early_closes.get(session_date, time(16, 0))
        return bar_state.TradingSession(
            session_date=session_date,
            open=datetime.combine(session_date, time(9, 30), tzinfo=ET),
            close=datetime.combine(session_date, close, tzinfo=ET),
            source="test_exchange_authority",
        )


def _ts(hour: int, minute: int, second: int = 0, *, day: date = DAY) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, second, tzinfo=ET)


def _obs(
    timestamp: datetime,
    price: float = 100.0,
    *,
    source_id: str | None = None,
    source_sequence: int | None = None,
    timeframe: str | None = None,
    volume: float | None = None,
    **extra,
):
    row = {"ticker": "NVDA", "timestamp": timestamp, "price": price}
    if source_id is not None:
        row["source_observation_id"] = source_id
    if source_sequence is not None:
        row["source_sequence"] = source_sequence
    if timeframe is not None:
        row["timeframe"] = timeframe
    if volume is not None:
        row["volume"] = volume
        row["volume_kind"] = "incremental"
    row.update(extra)
    return row


def _state(**kwargs):
    return bar_state.IntradayBarState(calendar=StaticCalendar(**kwargs))


def test_5m_basic_incremental_ohlc_and_exact_finalization():
    state = _state()
    first = _obs(_ts(9, 30), 100.0, source_id="p1")
    second = _obs(_ts(9, 32), 103.0, source_id="p2")
    third = _obs(_ts(9, 33), 98.0, source_id="p3")
    assert state.ingest(first).status == bar_state.ACCEPTED
    state.ingest(second)
    state.ingest(third)

    forming = state.forming_bar("NVDA", "5m")
    assert forming is not None
    assert forming.status == bar_state.FORMING
    assert (forming.bucket_start.hour, forming.bucket_start.minute) == (9, 30)
    assert (forming.bucket_end.hour, forming.bucket_end.minute) == (9, 35)
    assert (forming.open, forming.high, forming.low, forming.close) == (100.0, 103.0, 98.0, 98.0)

    result = state.ingest(
        _obs(_ts(9, 35), 101.0, source_id="boundary", source_sequence=4),
    )
    assert result.status == bar_state.ACCEPTED
    completed = state.completed_bars("NVDA", "5m")
    assert len(completed) == 1
    assert completed[0].status == bar_state.COMPLETED
    assert completed[0].bar_id == "NVDA|5m|2026-09-11|09:30:00"
    assert state.ingest(
        _obs(_ts(9, 35), 102.0, source_id="boundary-2", source_sequence=5),
    ).status == bar_state.ACCEPTED
    assert len(state.completed_bars("NVDA", "5m")) == 1
    assert state.forming_bar("NVDA", "5m").bar_id == "NVDA|5m|2026-09-11|09:35:00"


@pytest.mark.parametrize(
    ("timeframe", "boundary", "expected_end"),
    [
        ("15m", (9, 45), (9, 45)),
        ("30m", (10, 0), (10, 0)),
        ("60m", (10, 30), (10, 30)),
    ],
)
def test_session_anchored_boundaries(timeframe, boundary, expected_end):
    state = _state()
    state.ingest(_obs(_ts(9, 30), source_id=f"start-{timeframe}", timeframe=timeframe))
    state.ingest(_obs(_ts(*boundary), 101.0, source_id=f"end-{timeframe}", timeframe=timeframe))
    completed = state.completed_bars("NVDA", timeframe)
    assert len(completed) == 1
    assert (completed[0].bucket_start.hour, completed[0].bucket_start.minute) == (9, 30)
    assert (completed[0].bucket_end.hour, completed[0].bucket_end.minute) == expected_end
    forming = state.forming_bar("NVDA", timeframe)
    assert forming is not None
    assert forming.status == bar_state.FORMING
    assert (forming.bucket_start.hour, forming.bucket_start.minute) == boundary


def test_60m_developing_bar_is_queryable_before_close():
    state = _state()
    for index, (hour, minute, price) in enumerate(
        [(9, 30, 100.0), (10, 30, 101.0), (11, 30, 102.0), (11, 43, 104.0)]
    ):
        state.ingest(_obs(_ts(hour, minute), price, source_id=f"develop-{index}"))
    forming = state.forming_bar("NVDA", "60m")
    assert forming is not None
    assert forming.status == bar_state.FORMING
    assert (forming.bucket_start.hour, forming.bucket_start.minute) == (11, 30)
    assert (forming.bucket_end.hour, forming.bucket_end.minute) == (12, 30)
    assert (forming.open, forming.high, forming.low, forming.close) == (102.0, 104.0, 102.0, 104.0)
    assert [bar.bar_id for bar in state.completed_bars("NVDA", "60m")] == [
        "NVDA|60m|2026-09-11|09:30:00",
        "NVDA|60m|2026-09-11|10:30:00",
    ]


def test_one_observation_updates_all_timeframes_but_lifecycles_are_independent():
    state = _state()
    state.ingest(_obs(_ts(9, 30), source_id="open"))
    state.ingest(_obs(_ts(9, 35), 101.0, source_id="five-boundary"))
    assert len(state.completed_bars("NVDA", "5m")) == 1
    for timeframe in ("15m", "30m", "60m"):
        forming = state.forming_bar("NVDA", timeframe)
        assert forming is not None and forming.status == bar_state.FORMING
        assert len(state.completed_bars("NVDA", timeframe)) == 0


def test_exact_boundary_is_half_open_and_final_close_has_no_next_shell():
    state = _state()
    state.ingest(_obs(_ts(10, 29, 59), 100.0, source_id="before"))
    state.ingest(_obs(_ts(10, 30), 101.0, source_id="exact"))
    assert state.completed_bars("NVDA", "60m")[0].bar_id == "NVDA|60m|2026-09-11|09:30:00"
    assert state.forming_bar("NVDA", "60m").bar_id == "NVDA|60m|2026-09-11|10:30:00"

    final_state = _state()
    final_state.ingest(_obs(_ts(15, 59), 105.0, source_id="last-minute"))
    advanced = final_state.advance(_ts(16, 0))
    assert advanced.status == bar_state.ACCEPTED
    final_bars = final_state.completed_bars("NVDA", "60m")
    assert [bar.bar_id for bar in final_bars] == ["NVDA|60m|2026-09-11|15:30:00"]
    assert final_bars[0].bucket_end.astimezone(ET).time() == time(16, 0)
    assert final_state.forming_bar("NVDA", "60m") is None
    assert final_state.advance(_ts(16, 0)).status == bar_state.NOOP


def test_early_close_clips_final_bucket_and_rejects_close_timestamp():
    early_day = date(2026, 11, 27)
    state = _state(early_closes={early_day: time(13, 0)})
    source = _ts(12, 59, day=early_day)
    assert state.ingest(_obs(source, 105.0, source_id="early-last")).status == bar_state.ACCEPTED
    forming = state.forming_bar("NVDA", "60m")
    assert forming is not None
    assert forming.bucket_start.astimezone(ET).time() == time(12, 30)
    assert forming.bucket_end.astimezone(ET).time() == time(13, 0)
    assert state.advance(_ts(13, 0, day=early_day)).status == bar_state.ACCEPTED
    assert state.ingest(_obs(_ts(13, 0, day=early_day), 106.0, source_id="after-close")).status == bar_state.REJECTED
    assert state.forming_bar("NVDA", "60m") is None


@pytest.mark.parametrize(
    "timestamp",
    [_ts(9, 29), _ts(16, 0), _ts(16, 1)],
)
def test_premarket_and_postmarket_are_not_admitted(timestamp):
    state = _state()
    before = state.snapshot()
    result = state.ingest(_obs(timestamp, 100.0, source_id=f"outside-{timestamp.isoformat()}"))
    assert result.status == bar_state.REJECTED
    assert result.reason == "outside_rth"
    assert state.snapshot() == before


def test_weekend_and_holiday_are_not_admitted():
    holiday = date(2026, 9, 14)
    state = _state(closed={holiday})
    weekend = datetime(2026, 9, 12, 10, 0, tzinfo=ET)
    holiday_open = datetime.combine(holiday, time(10, 0), tzinfo=ET)
    assert state.ingest(_obs(weekend, source_id="weekend")).reason == "non_trading_session"
    assert state.ingest(_obs(holiday_open, source_id="holiday")).reason == "non_trading_session"
    assert state.snapshot()["forming"] == []


@pytest.mark.parametrize(
    "bad",
    [
        {"ticker": "NVDA", "timestamp": _ts(9, 30), "price": float("nan")},
        {"ticker": "NVDA", "timestamp": _ts(9, 30), "price": float("inf")},
        {"ticker": "NVDA", "timestamp": _ts(9, 30), "price": 0},
        {"ticker": "NVDA", "timestamp": _ts(9, 30), "open": 10, "high": 9, "low": 8, "close": 8},
        {"ticker": "NVDA", "timestamp": datetime(2026, 9, 11, 9, 30), "price": 100},
        {"ticker": "NVDA", "timestamp": _ts(9, 30), "price": 100, "timeframe": "1m"},
        {"ticker": "TSLA", "timestamp": _ts(9, 30), "price": 100},
        {"timestamp": _ts(9, 30), "price": 100},
    ],
)
def test_malformed_or_out_of_scope_input_cannot_mutate_state(bad):
    state = _state()
    before = state.snapshot()
    result = state.ingest(bad)
    assert result.status == bar_state.REJECTED
    assert state.snapshot() == before


def test_duplicate_is_idempotent_and_does_not_double_incremental_volume():
    state = _state()
    row = _obs(_ts(9, 30), 100.0, source_id="volume-1", volume=7.0)
    assert state.ingest(row).status == bar_state.ACCEPTED
    assert state.ingest(copy.deepcopy(row)).status == bar_state.DUPLICATE
    for timeframe in bar_state.SUPPORTED_TIMEFRAMES:
        assert state.forming_bar("NVDA", timeframe).volume == 7.0


def test_same_price_distinct_source_updates_are_not_price_deduped():
    state = _state()
    state.ingest(_obs(_ts(9, 30), 100.0, source_id="same-price-1"))
    state.ingest(_obs(_ts(9, 30, 1), 100.0, source_id="same-price-2"))
    forming = state.forming_bar("NVDA", "5m")
    assert forming is not None
    assert forming.last_source_timestamp == _ts(9, 30, 1).astimezone(timezone.utc)


def test_ohlc_snapshot_merging_is_explicit_and_deterministic():
    state = _state()
    first = {
        "ticker": "NVDA",
        "timestamp": _ts(9, 30),
        "open": 100.0,
        "high": 103.0,
        "low": 99.0,
        "close": 101.0,
        "volume": 5.0,
        "volume_kind": "incremental",
        "source_observation_id": "ohlc-1",
    }
    second = {
        **first,
        "timestamp": _ts(9, 31),
        "open": 101.0,
        "high": 105.0,
        "low": 98.0,
        "close": 104.0,
        "volume": 7.0,
        "source_observation_id": "ohlc-2",
    }
    state.ingest(first)
    state.ingest(second)
    bar = state.forming_bar("NVDA", "5m")
    assert bar is not None
    assert (bar.open, bar.high, bar.low, bar.close, bar.volume) == (100.0, 105.0, 98.0, 104.0, 12.0)


def test_late_completed_bucket_cannot_rewrite_frozen_truth():
    state = _state()
    state.ingest(_obs(_ts(9, 30), 100.0, source_id="late-start", timeframe="5m"))
    state.ingest(_obs(_ts(9, 35), 101.0, source_id="late-boundary", timeframe="5m"))
    frozen_before = state.completed_bars("NVDA", "5m")[0].to_dict()
    result = state.ingest(_obs(_ts(9, 34), 999.0, source_id="late-arrival", timeframe="5m"))
    assert result.status == bar_state.LATE_COMPLETED_BUCKET
    assert state.completed_bars("NVDA", "5m")[0].to_dict() == frozen_before


def test_backward_timestamp_in_same_forming_bucket_is_rejected():
    state = _state()
    state.ingest(_obs(_ts(10, 0), 100.0, source_id="ordered", timeframe="60m"))
    before = state.forming_bar("NVDA", "60m").to_dict()
    result = state.ingest(_obs(_ts(9, 59), 999.0, source_id="backward", timeframe="60m"))
    assert result.status == bar_state.OUT_OF_ORDER_FORMING
    assert state.forming_bar("NVDA", "60m").to_dict() == before


def test_explicit_as_of_rejects_future_source_timestamp_without_wall_clock():
    state = _state()
    result = state.ingest(
        _obs(_ts(11, 0), 100.0, source_id="future"),
        as_of=_ts(10, 59),
    )
    assert result.status == bar_state.REJECTED
    assert result.reason == "future_observation"
    assert state.snapshot()["forming"] == []


def test_restart_snapshot_reconstruction_is_byte_for_byte_equivalent():
    state = _state()
    rows = [
        _obs(_ts(9, 30), 100.0, source_id="r1"),
        _obs(_ts(9, 31), 102.0, source_id="r2"),
        _obs(_ts(10, 30), 101.0, source_id="r3"),
        _obs(_ts(11, 43), 105.0, source_id="r4"),
    ]
    for row in rows:
        state.ingest(row)
    restarted = bar_state.IntradayBarState.from_snapshot(state.snapshot(), calendar=StaticCalendar())
    next_row = _obs(_ts(11, 45), 104.0, source_id="r5")
    assert state.ingest(next_row).status == bar_state.ACCEPTED
    assert restarted.ingest(copy.deepcopy(next_row)).status == bar_state.ACCEPTED
    assert state.advance(_ts(16, 0)).status == bar_state.ACCEPTED
    assert restarted.advance(_ts(16, 0)).status == bar_state.ACCEPTED
    assert state.snapshot() == restarted.snapshot()


def test_multi_ticker_state_and_duplicate_identity_are_isolated():
    state = _state()
    state.ingest(_obs(_ts(9, 30), 100.0, source_id="nvda-1"))
    qqq = _obs(_ts(9, 30), 100.0, source_id="qqq-1")
    qqq["ticker"] = "QQQ"
    state.ingest(qqq)
    assert state.forming_bar("NVDA", "60m").close == 100.0
    assert state.forming_bar("QQQ", "60m").close == 100.0
    update = _obs(_ts(9, 31), 105.0, source_id="nvda-2")
    state.ingest(update)
    assert state.forming_bar("NVDA", "60m").close == 105.0
    assert state.forming_bar("QQQ", "60m").close == 100.0


def test_seed_provider_is_called_once_and_incremental_updates_do_not_refresh_history():
    state = _state()
    calls = []

    def provider():
        calls.append("seed")
        return [_obs(_ts(9, 30), 100.0, source_id="seed")]

    seeded = state.seed_once(provider)
    assert seeded.status == bar_state.ACCEPTED
    assert calls == ["seed"]
    assert state.seed_once(provider).status == bar_state.NOOP
    for index in range(20):
        result = state.ingest(
            _obs(_ts(9, 30, index + 1), 100.0 + index, source_id=f"live-{index}")
        )
        assert result.accepted
    assert calls == ["seed"]


def test_seeded_completed_bar_can_be_reconstructed_without_refetching():
    source = _state()
    source.ingest(_obs(_ts(9, 30), 100.0, source_id="seed-bar-start", timeframe="5m"))
    source.ingest(_obs(_ts(9, 35), 101.0, source_id="seed-bar-end", timeframe="5m"))
    completed = source.completed_bars("NVDA", "5m")[0].to_dict()

    restored = _state()
    result = restored.seed_once([completed])
    assert result.accepted_rows == 1
    assert restored.completed_bars("NVDA", "5m")[0].to_dict() == completed


def test_input_mapping_is_not_mutated_and_state_has_no_setup_or_money_authority():
    state = _state()
    row = _obs(_ts(9, 30), 100.0, source_id="immutable")
    before = copy.deepcopy(row)
    result = state.ingest(row)
    assert row == before
    assert result.to_dict().keys() == {"status", "ticker", "source_timestamp", "accepted", "reason", "timeframes"}
    assert not hasattr(bar_state, "classify_setup")
    assert not any(name.startswith("broker") or name.startswith("watcher") for name in dir(bar_state))


def test_default_nyse_adapter_uses_existing_full_day_authority_and_early_close_schedule(monkeypatch):
    import types

    fake_ap = types.ModuleType("ap")
    fake_ap.__path__ = []
    fake_flatline = types.ModuleType("ap.flatline_alarm")
    fake_flatline.NYSE_HOLIDAYS = {2026: frozenset()}
    fake_flatline.is_trading_day = lambda value: value != date(2026, 9, 14)
    monkeypatch.setitem(sys.modules, "ap", fake_ap)
    monkeypatch.setitem(sys.modules, "ap.flatline_alarm", fake_flatline)

    calendar = bar_state.NYSESessionCalendar(
        early_closes={date(2026, 9, 11): time(13, 0)}
    )
    assert calendar.session_for(date(2026, 9, 14)) is None
    session = calendar.session_for(DAY)
    assert session is not None
    assert session.close.time() == time(13, 0)


def test_equal_timestamp_authoritative_sequence_is_delivery_order_independent_and_survives_restart():
    first = _obs(
        _ts(9, 30),
        100.0,
        source_id="equal-a",
        source_sequence=1,
        timeframe="5m",
        volume=3.0,
    )
    second = _obs(
        _ts(9, 30),
        102.0,
        source_id="equal-b",
        source_sequence=2,
        timeframe="5m",
        volume=4.0,
    )

    forward = _state()
    reverse = _state()
    assert forward.ingest(first).status == bar_state.ACCEPTED
    assert forward.ingest(second).status == bar_state.ACCEPTED
    assert reverse.ingest(second).status == bar_state.ACCEPTED
    assert reverse.ingest(first).status == bar_state.ACCEPTED

    forward_bar = forward.forming_bar("NVDA", "5m")
    reverse_bar = reverse.forming_bar("NVDA", "5m")
    assert forward_bar is not None and reverse_bar is not None
    assert forward_bar.to_dict() == reverse_bar.to_dict()
    assert (forward_bar.open, forward_bar.high, forward_bar.low, forward_bar.close) == (
        100.0,
        102.0,
        100.0,
        102.0,
    )
    assert (forward_bar.first_source_sequence, forward_bar.last_source_sequence) == (1, 2)
    assert forward_bar.volume == 7.0

    restarted = bar_state.IntradayBarState.from_snapshot(
        forward.snapshot(), calendar=StaticCalendar()
    )
    assert restarted.snapshot() == forward.snapshot()
    assert restarted.ingest(copy.deepcopy(first)).status == bar_state.DUPLICATE
    assert restarted.ingest(copy.deepcopy(second)).status == bar_state.DUPLICATE
    third = _obs(
        _ts(9, 30),
        101.0,
        source_id="equal-c",
        source_sequence=3,
        timeframe="5m",
    )
    assert forward.ingest(third).status == bar_state.ACCEPTED
    assert restarted.ingest(copy.deepcopy(third)).status == bar_state.ACCEPTED
    assert restarted.snapshot() == forward.snapshot()


def test_equal_timestamp_without_authoritative_sequence_fails_closed_without_mutation():
    state = _state()
    first = _obs(_ts(9, 30), 100.0, source_id="ambiguous-a", timeframe="5m")
    second = _obs(_ts(9, 30), 102.0, source_id="ambiguous-b", timeframe="5m")
    assert state.ingest(first).status == bar_state.ACCEPTED
    before = state.snapshot()

    result = state.ingest(second)
    assert result.status == bar_state.AMBIGUOUS_EQUAL_TIMESTAMP
    assert result.reason == "equal_source_timestamp_requires_authoritative_order"
    assert result.timeframes[0].status == bar_state.AMBIGUOUS_EQUAL_TIMESTAMP
    assert state.snapshot() == before


@pytest.mark.parametrize("bad_sequence", [True, 1.0, -1, "1.5", {"n": 1}])
def test_source_sequence_is_strictly_typed_ordering_evidence(bad_sequence):
    state = _state()
    row = _obs(_ts(9, 30), source_id="bad-sequence", timeframe="5m")
    row["source_sequence"] = bad_sequence
    before = state.snapshot()
    result = state.ingest(row)
    assert result.status == bar_state.REJECTED
    assert result.reason == "source_sequence_malformed"
    assert state.snapshot() == before


def test_one_source_event_can_be_consumed_by_5m_then_15m_without_global_replay_loss():
    state = _state()
    event = _obs(
        _ts(9, 30),
        100.0,
        source_id="route-once",
        timeframe="5m",
        volume=7.0,
    )
    assert state.ingest(event).status == bar_state.ACCEPTED
    routed = {**event, "timeframe": "15m"}
    assert state.ingest(routed).status == bar_state.ACCEPTED
    assert state.forming_bar("NVDA", "5m").volume == 7.0
    assert state.forming_bar("NVDA", "15m").volume == 7.0


def test_all_timeframes_then_5m_replay_is_per_timeframe_duplicate_without_double_volume():
    state = _state()
    event = _obs(_ts(9, 30), source_id="route-all", volume=4.0)
    assert state.ingest(event).status == bar_state.ACCEPTED
    replay = {**event, "timeframe": "5m"}
    result = state.ingest(replay)
    assert result.status == bar_state.DUPLICATE
    assert result.timeframes[0].status == bar_state.DUPLICATE
    for timeframe in bar_state.SUPPORTED_TIMEFRAMES:
        assert state.forming_bar("NVDA", timeframe).volume == 4.0


def test_fallback_event_identity_excludes_timeframe_routing_and_volume_is_not_doubled():
    state = _state()
    event = _obs(_ts(9, 30), source_id=None, volume=6.0)
    assert state.ingest(event).status == bar_state.ACCEPTED
    replay = {**event, "timeframe": "5m"}
    result = state.ingest(replay)
    assert result.status == bar_state.DUPLICATE
    assert state.forming_bar("NVDA", "5m").volume == 6.0


def test_same_source_id_with_conflicting_payload_is_rejected_before_new_routing():
    state = _state()
    first = _obs(_ts(9, 30), 100.0, source_id="reused", timeframe="5m")
    conflicting = _obs(_ts(9, 31), 101.0, source_id="reused", timeframe="15m")
    assert state.ingest(first).status == bar_state.ACCEPTED
    before = state.snapshot()
    result = state.ingest(conflicting)
    assert result.status == bar_state.CONFLICTING_DUPLICATE
    assert result.reason == "source_observation_id_reused_with_different_payload"
    assert state.snapshot() == before


def test_provider_namespace_and_version_are_part_of_source_event_identity():
    state = _state()
    first = _obs(
        _ts(9, 30),
        100.0,
        source_id="shared-id",
        source_identity="provider-a",
        source_version="v1",
        timeframe="5m",
    )
    second = _obs(
        _ts(9, 31),
        102.0,
        source_id="shared-id",
        source_identity="provider-b",
        source_version="v1",
        timeframe="5m",
    )
    assert state.ingest(first).status == bar_state.ACCEPTED
    assert state.ingest(second).status == bar_state.ACCEPTED
    bar = state.forming_bar("NVDA", "5m")
    assert bar is not None
    assert (bar.open, bar.high, bar.low, bar.close) == (100.0, 102.0, 100.0, 102.0)
    assert bar.source_identity == "MIXED"


def test_seed_rows_are_canonically_sorted_and_permutations_reconstruct_identically():
    rows = [
        _obs(_ts(9, 33), 103.0, source_id="seed-3", timeframe="5m"),
        _obs(_ts(9, 30), 100.0, source_id="seed-0", timeframe="5m"),
        _obs(_ts(9, 32), 102.0, source_id="seed-2", timeframe="5m"),
        _obs(_ts(9, 31), 101.0, source_id="seed-1", timeframe="5m"),
    ]
    snapshots = []
    for ordered in (rows, list(reversed(rows)), [rows[2], rows[0], rows[3], rows[1]]):
        state = _state()
        result = state.seed_once(ordered)
        assert result.status == bar_state.ACCEPTED
        snapshots.append(state.snapshot())
    assert snapshots[0] == snapshots[1] == snapshots[2]


def test_seed_equal_timestamp_sequence_permutations_reconstruct_identically():
    rows = [
        _obs(_ts(9, 30), 100.0, source_id="seed-equal-a", source_sequence=1, timeframe="5m"),
        _obs(_ts(9, 30), 102.0, source_id="seed-equal-b", source_sequence=2, timeframe="5m"),
    ]
    forward = _state()
    reverse = _state()
    assert forward.seed_once(rows).status == bar_state.ACCEPTED
    assert reverse.seed_once(list(reversed(rows))).status == bar_state.ACCEPTED
    assert forward.snapshot() == reverse.snapshot()


def test_seed_equal_timestamp_without_sequence_is_rejected_before_any_mutation():
    state = _state()
    rows = [
        _obs(_ts(9, 30), 100.0, source_id="seed-ambiguous-a", timeframe="5m"),
        _obs(_ts(9, 30), 102.0, source_id="seed-ambiguous-b", timeframe="5m"),
    ]
    result = state.seed_once(rows)
    assert result.status == bar_state.REJECTED
    assert result.reason.startswith("seed_equal_source_timestamp_requires_source_sequence:")
    assert state.forming_bar("NVDA", "5m") is None


def test_completed_bar_seed_rows_are_canonically_sorted():
    source = _state()
    for index, minute in enumerate((30, 35, 40, 45)):
        assert source.ingest(
            _obs(
                _ts(9, minute),
                100.0 + index,
                source_id=f"completed-seed-{index}",
                timeframe="5m",
            )
        ).accepted
    rows = [bar.to_dict() for bar in source.completed_bars("NVDA", "5m")]
    forward = _state()
    reverse = _state()
    assert forward.seed_once(rows).status == bar_state.ACCEPTED
    assert reverse.seed_once(list(reversed(rows))).status == bar_state.ACCEPTED
    assert forward.snapshot() == reverse.snapshot()


def test_default_calendar_rejects_unknown_year_as_calendar_unavailable(monkeypatch):
    import types

    fake_ap = types.ModuleType("ap")
    fake_ap.__path__ = []
    fake_flatline = types.ModuleType("ap.flatline_alarm")
    fake_flatline.NYSE_HOLIDAYS = {2026: frozenset()}
    fake_flatline.is_trading_day = lambda value: True
    monkeypatch.setitem(sys.modules, "ap", fake_ap)
    monkeypatch.setitem(sys.modules, "ap.flatline_alarm", fake_flatline)

    calendar = bar_state.NYSESessionCalendar()
    unknown_day = date(2028, 1, 3)
    assert calendar.is_supported(unknown_day) is False
    assert calendar.session_for(unknown_day) is None

    state = bar_state.IntradayBarState()
    unknown_timestamp = datetime(2028, 1, 3, 10, 0, tzinfo=ET)
    result = state.ingest(_obs(unknown_timestamp, source_id="unknown-year"))
    assert result.status == bar_state.REJECTED
    assert result.reason == "calendar_unavailable"
    assert state.snapshot()["forming"] == []
