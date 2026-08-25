"""Fail-closed confirmation when the newly active scanner stop is unknown."""
from __future__ import annotations

import datetime as dt
from unittest.mock import MagicMock, patch

import pytest

from ap_entry_watcher import APEntryWatcher, WatchState, WatchedSignal


T0 = dt.datetime(2026, 1, 1, 12, 0, tzinfo=dt.timezone.utc)


def _signal(side: str) -> dict:
    return {
        "ticker": "SPY",
        "side": side,
        "entry_price": 100.0,
        "stop_price": 95.0 if side == "CALL" else 105.0,
        "target_price": 105.0 if side == "CALL" else 95.0,
        "signal_id": f"sig-same-poll-{side.lower()}",
        "local_order_id": f"local-same-poll-{side.lower()}",
        "client_id": "client@example.com",
        "execution_mode": "paper",
    }


def _at(watched: WatchedSignal, when: dt.datetime, bid, ask) -> str:
    with patch.object(watched, "_get_watcher_now", return_value=when):
        return watched.check(bid=bid, ask=ask)


@pytest.mark.parametrize(
    ("side", "first", "confirmation_unknown", "stop_side"),
    [
        ("CALL", (99.0, 100.05), (None, 100.06), "BID"),
        ("PUT", (99.95, 101.0), (99.94, None), "ASK"),
    ],
)
def test_same_poll_confirmation_with_unknown_active_stop_holds_and_preserves_proof(
    side, first, confirmation_unknown, stop_side
):
    signal = _signal(side)
    watched = WatchedSignal(signal, overnight=False)

    assert _at(watched, T0, *first) == WatchState.PENDING
    assert watched.breach_count == 1

    assert _at(
        watched,
        T0 + dt.timedelta(seconds=15),
        *confirmation_unknown,
    ) == WatchState.PENDING
    assert watched.state == WatchState.PENDING
    assert watched.trigger_crossed_at == T0
    assert watched.last_trigger_evidence_reason == (
        f"ACTIVE_STOP_TRUTH_UNAVAILABLE_{stop_side}"
    )
    assert signal["client_id"] == "client@example.com"
    assert signal["execution_mode"] == "paper"
    assert signal["signal_id"] == f"sig-same-poll-{side.lower()}"


@pytest.mark.parametrize(
    ("side", "first", "confirmation_unknown", "intact"),
    [
        ("CALL", (99.0, 100.05), (None, 100.06), (99.0, 100.06)),
        ("PUT", (99.95, 101.0), (99.94, None), (99.94, 101.0)),
    ],
)
def test_confirmed_retry_resumes_when_stop_truth_returns_intact(
    side, first, confirmation_unknown, intact
):
    watched = WatchedSignal(_signal(side), overnight=False)

    _at(watched, T0, *first)
    assert _at(watched, T0 + dt.timedelta(seconds=15), *confirmation_unknown) == WatchState.PENDING
    assert watched.trigger_crossed_at == T0

    assert _at(watched, T0 + dt.timedelta(seconds=30), *intact) == WatchState.TRIGGERED
    assert watched.trigger_crossed_at == T0


@pytest.mark.parametrize(
    ("side", "first", "confirmation_unknown", "broken"),
    [
        ("CALL", (99.0, 100.05), (None, 100.06), (90.0, 100.06)),
        ("PUT", (99.95, 101.0), (99.94, None), (99.94, 110.0)),
    ],
)
def test_confirmed_retry_invalidates_when_stop_truth_returns_broken(
    side, first, confirmation_unknown, broken
):
    watched = WatchedSignal(_signal(side), overnight=False)

    _at(watched, T0, *first)
    assert _at(watched, T0 + dt.timedelta(seconds=15), *confirmation_unknown) == WatchState.PENDING
    assert watched.trigger_crossed_at == T0

    assert _at(watched, T0 + dt.timedelta(seconds=30), *broken) == WatchState.INVALIDATED
    assert watched.trigger_crossed_at == T0


class _QuoteWatcher(APEntryWatcher):
    def __init__(self, quotes):
        self._quotes = iter(quotes)
        self.broker = MagicMock()
        super().__init__(self.broker, order_state_machine=MagicMock(), require_on_trigger=False)
        self._persist_watcher_audit = MagicMock()

    def _fetch_quotes(self, tickers):
        return {tickers[0]: next(self._quotes)}


@pytest.mark.parametrize(
    ("side", "first", "confirmation_unknown", "stop_side"),
    [
        ("CALL", {"bid": 99.0, "ask": 100.05}, {"bid": None, "ask": 100.06}, "BID"),
        ("PUT", {"bid": 99.95, "ask": 101.0}, {"bid": 99.94, "ask": None}, "ASK"),
    ],
)
def test_production_poll_route_suppresses_callback_on_same_poll_unknown_stop(
    side, first, confirmation_unknown, stop_side
):
    watcher = _QuoteWatcher([first, confirmation_unknown])
    watched = WatchedSignal(_signal(side), overnight=False)
    watched._watcher_ref = watcher
    watcher._pending.append(watched)
    watcher._dedup_set.add(watched.signal_id)
    watcher.on_trigger = MagicMock(return_value={"disposition": "KEEP_WATCHER"})

    watcher._poll_active_signals()
    watcher._poll_active_signals()

    assert watched.state == WatchState.PENDING
    assert watched.trigger_crossed_at is not None
    assert watched.last_trigger_evidence_reason == (
        f"ACTIVE_STOP_TRUTH_UNAVAILABLE_{stop_side}"
    )
    assert watcher.on_trigger.call_count == 0
    assert watcher.broker.submit_order.call_count == 0
    assert watcher.broker.cancel_order.call_count == 0
