"""Bounded pre-confirmation breach continuity for recovery PR #494.

The timer is process-local and measured from the most recent valid canonical
breach observation. Missing/unusable required-side truth can HOLD only while
that anchor is fresh; valid contradictory truth resets immediately.
"""
from __future__ import annotations

import datetime as dt
from unittest.mock import MagicMock, patch

import pytest

import ap_entry_watcher
from ap_entry_watcher import (
    APEntryWatcher,
    WatchState,
    WatchedSignal,
    WATCHER_BREACH_CONTINUITY_MAX_GAP_SEC as MAX_GAP,
)


T0 = dt.datetime(2026, 1, 1, 12, 0, tzinfo=dt.timezone.utc)


def _signal(side: str, *, execution_mode: str = "paper", client_id: str = "client@example.com") -> dict:
    return {
        "ticker": "SPY",
        "side": side,
        "entry_price": 100.0,
        "stop_price": 95.0 if side == "CALL" else 105.0,
        "target_price": 105.0 if side == "CALL" else 95.0,
        "signal_id": f"sig-494-{side.lower()}",
        "local_order_id": f"local-494-{side.lower()}",
        "client_id": client_id,
        "execution_mode": execution_mode,
    }


def _at(watched: WatchedSignal, when: dt.datetime, bid, ask) -> str:
    with patch.object(watched, "_get_watcher_now", return_value=when):
        return watched.check(bid=bid, ask=ask)


@pytest.mark.parametrize(
    ("side", "first", "missing", "second"),
    [
        ("CALL", (99.0, 100.05), (99.0, None), (99.0, 100.06)),
        ("PUT", (100.0, 101.0), (None, 101.0), (99.94, 101.0)),
    ],
)
def test_short_missing_gap_holds_and_confirms(side, first, missing, second):
    watched = WatchedSignal(_signal(side), overnight=False)

    assert _at(watched, T0, *first) == WatchState.PENDING
    assert watched.breach_count == 1
    anchor = watched._last_valid_breach_observation_at

    assert _at(watched, T0 + dt.timedelta(seconds=20), *missing) == WatchState.PENDING
    assert watched.breach_count == 1
    assert watched._last_valid_breach_observation_at == anchor
    assert watched.trigger_price is None

    assert _at(watched, T0 + dt.timedelta(seconds=30), *second) == WatchState.TRIGGERED
    assert watched.breach_count == 2
    assert watched.trigger_crossed_at == anchor


@pytest.mark.parametrize(
    ("side", "first", "missing", "second"),
    [
        ("CALL", (99.0, 100.05), (99.0, None), (99.0, 100.06)),
        ("PUT", (100.0, 101.0), (None, 101.0), (99.94, 101.0)),
    ],
)
def test_gap_beyond_45_seconds_discards_partial_streak(side, first, missing, second):
    watched = WatchedSignal(_signal(side), overnight=False)

    _at(watched, T0, *first)
    stale_at = T0 + dt.timedelta(seconds=MAX_GAP + 1)
    assert _at(watched, stale_at, *missing) == WatchState.PENDING
    assert watched.breach_count == 0
    assert watched._last_valid_breach_observation_at is None

    assert _at(watched, stale_at + dt.timedelta(seconds=1), *second) == WatchState.PENDING
    assert watched.breach_count == 1
    assert watched.trigger_crossed_at is None


@pytest.mark.parametrize(
    ("side", "first", "second"),
    [
        ("CALL", (99.0, 100.05), (99.0, 100.06)),
        ("PUT", (100.0, 101.0), (99.94, 101.0)),
    ],
)
def test_delayed_valid_observation_cannot_revive_stale_continuity(side, first, second):
    watched = WatchedSignal(_signal(side), overnight=False)

    _at(watched, T0, *first)
    delayed = T0 + dt.timedelta(seconds=MAX_GAP + 10)
    assert _at(watched, delayed, *second) == WatchState.PENDING
    assert watched.breach_count == 1
    assert watched._pending_first_breach_at == delayed
    assert watched._last_valid_breach_observation_at == delayed
    assert watched.trigger_crossed_at is None


@pytest.mark.parametrize(
    ("side", "first", "missing", "contradiction"),
    [
        ("CALL", (99.0, 100.10), (99.0, None), (99.0, 99.80)),
        ("PUT", (100.0, 101.0), (None, 101.0), (100.20, 101.0)),
    ],
)
def test_valid_contradiction_resets_immediately(side, first, missing, contradiction):
    watched = WatchedSignal(_signal(side), overnight=False)

    _at(watched, T0, *first)
    _at(watched, T0 + dt.timedelta(seconds=5), *missing)
    assert watched.breach_count == 1

    assert _at(watched, T0 + dt.timedelta(seconds=10), *contradiction) == WatchState.PENDING
    assert watched.breach_count == 0
    assert watched._last_valid_breach_observation_at is None
    assert watched.trigger_crossed_at is None


def test_exact_45_second_boundary_is_fresh_but_epsilon_is_stale():
    fresh = WatchedSignal(_signal("CALL"), overnight=False)
    _at(fresh, T0, 99.0, 100.05)
    assert _at(fresh, T0 + dt.timedelta(seconds=MAX_GAP), 99.0, 100.06) == WatchState.TRIGGERED

    stale = WatchedSignal(_signal("CALL"), overnight=False)
    _at(stale, T0, 99.0, 100.05)
    assert _at(
        stale, T0 + dt.timedelta(seconds=MAX_GAP, milliseconds=1), 99.0, 100.06
    ) == WatchState.PENDING
    assert stale.breach_count == 1
    assert stale.trigger_crossed_at is None


@pytest.mark.parametrize("side", ["CALL", "PUT"])
def test_backward_clock_movement_fails_closed_for_valid_observation(side):
    watched = WatchedSignal(_signal(side), overnight=False)
    first = (99.0, 100.05) if side == "CALL" else (100.0, 101.0)
    second = (99.0, 100.06) if side == "CALL" else (99.94, 101.0)

    _at(watched, T0, *first)
    assert _at(watched, T0 - dt.timedelta(seconds=1), *second) == WatchState.PENDING
    assert watched.breach_count == 1
    assert watched._pending_first_breach_at == T0 - dt.timedelta(seconds=1)
    assert watched.trigger_crossed_at is None


@pytest.mark.parametrize("side", ["CALL", "PUT"])
def test_backward_clock_movement_fails_closed_for_missing_observation(side):
    watched = WatchedSignal(_signal(side), overnight=False)
    first = (99.0, 100.05) if side == "CALL" else (100.0, 101.0)
    missing = (99.0, None) if side == "CALL" else (None, 101.0)

    _at(watched, T0, *first)
    assert _at(watched, T0 - dt.timedelta(seconds=1), *missing) == WatchState.PENDING
    assert watched.breach_count == 0
    assert watched._last_valid_breach_observation_at is None


@pytest.mark.parametrize("side", ["CALL", "PUT"])
def test_restart_does_not_fabricate_unconfirmed_continuity(side):
    signal = _signal(side)
    original = WatchedSignal(signal, overnight=False)
    first = (99.0, 100.05) if side == "CALL" else (100.0, 101.0)
    second = (99.0, 100.06) if side == "CALL" else (99.94, 101.0)
    _at(original, T0, *first)
    assert original._last_valid_breach_observation_at == T0

    restarted = WatchedSignal(dict(signal), overnight=False)
    assert restarted._last_valid_breach_observation_at is None
    assert _at(restarted, T0 + dt.timedelta(seconds=15), *second) == WatchState.PENDING
    assert restarted.breach_count == 1
    assert restarted.trigger_crossed_at is None
    assert "_last_valid_breach_observation_at" not in signal


@pytest.mark.parametrize(
    ("side", "first", "last_only", "second"),
    [
        ("CALL", {"bid": 99.0, "ask": 100.05}, {"bid": 0, "ask": 0, "last": 105.0}, {"bid": 99.0, "ask": 100.06}),
        ("PUT", {"bid": 100.0, "ask": 101.0}, {"bid": 0, "ask": 0, "last": 95.0}, {"bid": 99.94, "ask": 101.0}),
    ],
)
def test_exported_poll_loop_processes_last_only_as_missing(side, first, last_only, second):
    class QuoteWatcher(APEntryWatcher):
        def __init__(self, quotes):
            self._quotes = iter(quotes)
            self.broker = MagicMock()
            super().__init__(self.broker, order_state_machine=MagicMock(), require_on_trigger=False)
            self._persist_watcher_audit = MagicMock()

        def _fetch_quotes(self, tickers):
            return {tickers[0]: next(self._quotes)}

    watcher = QuoteWatcher([first, last_only, second])
    watched = WatchedSignal(_signal(side), overnight=False)
    watched._watcher_ref = watcher
    watcher._pending.append(watched)
    watcher._dedup_set.add(watched.signal_id)
    watcher.on_trigger = MagicMock(return_value={"disposition": "KEEP_WATCHER"})
    calls = []
    original_check = watched.check

    def spy_check(bid, ask, quote_age_ms=None):
        calls.append((bid, ask))
        return original_check(bid, ask, quote_age_ms=quote_age_ms)

    watched.check = spy_check
    for when in (T0, T0 + dt.timedelta(seconds=20), T0 + dt.timedelta(seconds=30)):
        with patch.object(watched, "_get_watcher_now", return_value=when):
            watcher._poll_active_signals(open_protect_active=False)

    assert calls[1] == (None, None)
    assert watcher.on_trigger.call_count == 1
    assert watched.breach_count == 2
    assert watched.triggered_at is not None
    assert watcher.broker.submit_order.call_count == 0
    assert watcher.broker.cancel_order.call_count == 0


def test_confirmed_lifecycle_ignores_preconfirmation_timer_but_keeps_stop_safety():
    watched = WatchedSignal(_signal("CALL"), overnight=False)
    _at(watched, T0, 99.0, 100.0)
    assert _at(watched, T0 + dt.timedelta(seconds=15), 99.0, 100.0) == WatchState.TRIGGERED
    confirmed_at = watched.trigger_crossed_at
    watched.state = WatchState.PENDING

    assert _at(watched, T0 + dt.timedelta(seconds=300), 99.0, None) == WatchState.PENDING
    assert watched.trigger_crossed_at == confirmed_at

    assert _at(watched, T0 + dt.timedelta(seconds=301), 10.0, None) == WatchState.INVALIDATED
    assert watched.trigger_crossed_at == confirmed_at


def test_continuity_anchor_is_fixed_safety_constant():
    assert MAX_GAP == 45
    assert not hasattr(ap_entry_watcher, "WATCHER_BREACH_CONTINUITY_MAX_GAP_SEC_ENV")
