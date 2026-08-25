"""Missing/unusable canonical quote HOLD semantics for watcher recovery #494."""
from __future__ import annotations

import math

import pytest

from ap_entry_watcher import WatchState, WatchedSignal


UNUSABLE = [
    pytest.param(None, id="missing"),
    pytest.param(0, id="zero"),
    pytest.param(0.0, id="zero-float"),
    pytest.param(float("nan"), id="nan"),
    pytest.param(float("inf"), id="positive-infinity"),
    pytest.param(float("-inf"), id="negative-infinity"),
    pytest.param("not-a-price", id="malformed"),
    pytest.param(True, id="boolean"),
]


def _signal(side: str) -> dict:
    return {
        "ticker": "SPY",
        "side": side,
        "entry_price": 100.0,
        "stop_price": 95.0 if side == "CALL" else 105.0,
        "target_price": 105.0 if side == "CALL" else 95.0,
        "signal_id": f"sig-missing-{side.lower()}",
        "local_order_id": f"local-missing-{side.lower()}",
        "client_id": "client@example.com",
        "execution_mode": "paper",
    }


@pytest.mark.parametrize("bad_ask", UNUSABLE)
def test_call_unusable_ask_holds_partial_breach(bad_ask):
    watched = WatchedSignal(_signal("CALL"), overnight=False)
    watched.check(99.0, 100.05)
    first_pending_at = watched._pending_first_breach_at

    assert watched.check(99.0, bad_ask) == WatchState.PENDING
    assert watched.breach_count == 1
    assert watched._pending_first_breach_at == first_pending_at
    assert watched.trigger_price is None
    assert watched.last_trigger_evidence_reason == "TRIGGER_EVIDENCE_UNAVAILABLE_ASK"


@pytest.mark.parametrize("bad_bid", UNUSABLE)
def test_put_unusable_bid_holds_partial_breach(bad_bid):
    watched = WatchedSignal(_signal("PUT"), overnight=False)
    watched.check(100.0, 101.0)
    first_pending_at = watched._pending_first_breach_at

    assert watched.check(bad_bid, 101.0) == WatchState.PENDING
    assert watched.breach_count == 1
    assert watched._pending_first_breach_at == first_pending_at
    assert watched.trigger_price is None
    assert watched.last_trigger_evidence_reason == "TRIGGER_EVIDENCE_UNAVAILABLE_BID"


@pytest.mark.parametrize("side", ["CALL", "PUT"])
def test_unusable_required_quote_without_prior_breach_stays_pending(side):
    watched = WatchedSignal(_signal(side), overnight=False)
    quote = (99.0, None) if side == "CALL" else (None, 101.0)

    assert watched.check(*quote) == WatchState.PENDING
    assert watched.breach_count == 0
    assert watched._pending_first_breach_at is None
    assert watched._last_valid_breach_observation_at is None
    assert watched.trigger_price is None
    assert watched.trigger_crossed_at is None


@pytest.mark.parametrize("side", ["CALL", "PUT"])
def test_opposite_side_crossing_is_not_trigger_evidence(side):
    watched = WatchedSignal(_signal(side), overnight=False)
    quote = (105.0, None) if side == "CALL" else (None, 90.0)

    assert watched.check(*quote) == WatchState.PENDING
    assert watched.breach_count == 0
    assert watched.trigger_crossed_at is None


@pytest.mark.parametrize(
    ("side", "first", "missing", "second"),
    [
        ("CALL", (99.0, 100.05), (99.0, None), (99.0, 100.06)),
        ("PUT", (100.0, 101.0), (None, 101.0), (99.94, 101.0)),
    ],
)
def test_missing_observation_confirms_only_after_next_valid_canonical_breach(
    side, first, missing, second
):
    watched = WatchedSignal(_signal(side), overnight=False)
    assert watched.check(*first) == WatchState.PENDING
    assert watched.check(*missing) == WatchState.PENDING
    assert watched.breach_count == 1
    assert watched.check(*second) == WatchState.TRIGGERED
    assert watched.breach_count == 2
    assert watched.trigger_price is not None


@pytest.mark.parametrize("side", ["CALL", "PUT"])
def test_zero_and_last_are_not_canonical_trigger_authority(side):
    watched = WatchedSignal(_signal(side), overnight=False)
    first = (99.0, 100.05) if side == "CALL" else (100.0, 101.0)
    missing = (0.0, 0.0)

    watched.check(*first)
    assert watched.check(*missing) == WatchState.PENDING
    assert watched.breach_count == 1
    assert watched.trigger_crossed_at is None


def test_confirmed_entry_missing_does_not_replay_trigger_when_stop_truth_is_intact():
    watched = WatchedSignal(_signal("CALL"), overnight=False)
    assert watched.check(99.0, 100.0) == WatchState.PENDING
    assert watched.check(99.0, 100.0) == WatchState.TRIGGERED
    confirmed_at = watched.trigger_crossed_at
    watched.state = WatchState.PENDING

    assert watched.check(99.0, None) == WatchState.PENDING
    assert watched.trigger_crossed_at == confirmed_at
    assert watched.state == WatchState.PENDING


def test_confirmed_missing_stop_truth_holds_without_retrigger():
    watched = WatchedSignal(_signal("CALL"), overnight=False)
    watched.check(99.0, 100.0)
    watched.check(99.0, 100.0)
    confirmed_at = watched.trigger_crossed_at
    watched.state = WatchState.PENDING

    assert watched.check(None, 100.5) == WatchState.PENDING
    assert watched.trigger_crossed_at == confirmed_at
    assert watched.last_trigger_evidence_reason == "ACTIVE_STOP_TRUTH_UNAVAILABLE_BID"


def test_non_finite_entry_trigger_is_unknown_not_trigger_truth():
    watched = WatchedSignal(_signal("CALL"), overnight=False)
    watched.check(99.0, 100.05)
    watched.entry_trigger = math.nan

    assert watched.check(99.0, 100.10) == WatchState.PENDING
    assert watched.breach_count == 1
    assert watched.trigger_crossed_at is None
