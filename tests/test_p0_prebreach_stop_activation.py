"""PR #407 — scanner stop stays dormant until entry-direction breach."""

from datetime import datetime, timedelta, timezone

import pytest

import ap_entry_watcher as ew
from ap.pending_trigger_classifier import (
    STOP_ALREADY_BROKEN_TERMINAL,
    TRIGGER_TRUTH_UNAVAILABLE_RETRY,
    classify_late_attachment,
)


@pytest.mark.parametrize(
    "side,trigger,stop,bid,ask,source,stop_source",
    [
        ("CALL", 100.0, 95.0, 94.90, 95.10, "ask", "bid"),
        ("PUT", 61.90, 62.49, 62.45, 62.49, "bid", "ask"),
    ],
)
def test_pretrigger_stop_geometry_is_dormant(
    side, trigger, stop, bid, ask, source, stop_source
):
    decision = classify_late_attachment(
        side=side,
        trigger_price=trigger,
        bid=bid,
        ask=ask,
        stop=stop,
    )

    assert decision.classification == TRIGGER_TRUTH_UNAVAILABLE_RETRY
    assert decision.quote_source == source
    assert decision.quote is not None
    assert f"stop_broken_at_{stop_source}" not in decision.detail


@pytest.mark.parametrize(
    "side,trigger,stop,bid,ask,stop_source,stop_value",
    [
        ("CALL", 100.0, 95.0, 94.90, 95.10, "bid", "94.9"),
        ("PUT", 61.90, 62.49, 62.45, 62.49, "ask", "62.49"),
    ],
)
def test_activated_stop_uses_the_authoritative_stop_side(
    side, trigger, stop, bid, ask, stop_source, stop_value
):
    decision = classify_late_attachment(
        side=side,
        trigger_price=trigger,
        bid=bid,
        ask=ask,
        stop=stop,
        trigger_previously_breached=True,
    )

    assert decision.classification == STOP_ALREADY_BROKEN_TERMINAL
    assert decision.detail == f"stop_broken_at_{stop_source}={stop_value}"


def _signal(*, side, trigger, stop, trigger_crossed_at=None, seed=None):
    signal = {
        "signal_id": f"sig-{side.lower()}",
        "ticker": "BAC" if side == "PUT" else "AAPL",
        "side": side,
        "entry_price": trigger,
        "stop_price": stop,
        "target_price": 150.0 if side == "CALL" else 30.0,
        "score": 80.0,
        "grade": "A",
    }
    if trigger_crossed_at is not None:
        signal["trigger_crossed_at"] = trigger_crossed_at
    if seed is not None:
        signal["_late_attachment_seed"] = seed
    return signal


@pytest.mark.parametrize(
    "side,trigger,stop,pre_bid,pre_ask,breach_bid,breach_ask",
    [
        ("CALL", 100.0, 95.0, 94.90, 95.10, 100.00, 100.05),
        ("PUT", 61.90, 62.49, 62.45, 62.49, 61.90, 61.95),
    ],
)
def test_pretrigger_stop_touch_keeps_ordinary_trade_flow_open(
    side, trigger, stop, pre_bid, pre_ask, breach_bid, breach_ask
):
    watcher = ew.WatchedSignal(
        _signal(side=side, trigger=trigger, stop=stop),
        overnight=False,
    )

    assert watcher.check(bid=pre_bid, ask=pre_ask) == ew.WatchState.PENDING
    assert watcher.trigger_crossed_at is None
    assert watcher.state != ew.WatchState.INVALIDATED

    assert watcher.check(bid=breach_bid, ask=breach_ask) == ew.WatchState.PENDING
    assert watcher.trigger_crossed_at is not None
    assert watcher.check(bid=breach_bid, ask=breach_ask) == ew.WatchState.TRIGGERED


def test_restart_hydrates_prior_breach_and_keeps_stop_active():
    crossed_at = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
    watcher = ew.WatchedSignal(
        _signal(
            side="PUT",
            trigger=61.90,
            stop=62.49,
            trigger_crossed_at=crossed_at,
        ),
        overnight=False,
    )

    assert watcher.trigger_crossed_at is not None
    assert watcher.check(bid=62.45, ask=62.60) == ew.WatchState.INVALIDATED


def test_late_gate_uses_hydrated_breach_evidence_after_restart():
    crossed_at = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
    seed = {
        "state": "LATE_ATTACHMENT_WITHIN_CONTINUATION",
        "seen_at": crossed_at,
        "quote": 61.85,
        "quote_source": "bid",
    }
    watcher = ew.WatchedSignal(
        _signal(
            side="PUT",
            trigger=61.90,
            stop=62.49,
            trigger_crossed_at=crossed_at,
            seed=seed,
        ),
        overnight=False,
    )

    assert watcher.check(bid=62.45, ask=62.49) == ew.WatchState.INVALIDATED
