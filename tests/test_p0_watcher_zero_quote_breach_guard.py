"""Zero/unavailable quote guard for the #494 watcher continuity seam."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from ap_entry_watcher import MAX_INTRADAY_WATCH_MIN, WatchState, WatchedSignal


def _signal(side: str) -> dict:
    return {
        "ticker": "SPY",
        "side": side,
        "entry_price": 100.0,
        "stop_price": 95.0 if side == "CALL" else 105.0,
        "target_price": 105.0 if side == "CALL" else 95.0,
        "signal_id": f"sig-zero-{side.lower()}",
        "local_order_id": f"local-zero-{side.lower()}",
        "client_id": "client@example.com",
        "execution_mode": "paper",
    }


def test_call_zero_quote_does_not_erase_partial_breach():
    watched = WatchedSignal(_signal("CALL"), overnight=False)
    assert watched.check(99.0, 100.05) == WatchState.PENDING
    assert watched.check(0.0, 0.0) == WatchState.PENDING
    assert watched.breach_count == 1
    assert watched.trigger_crossed_at is None


def test_put_zero_quote_does_not_become_false_breach():
    watched = WatchedSignal(_signal("PUT"), overnight=False)
    assert watched.check(100.0, 101.0) == WatchState.PENDING
    assert watched.check(0.0, 0.0) == WatchState.PENDING
    assert watched.breach_count == 1
    assert watched.trigger_crossed_at is None


def test_last_only_values_are_ignored_when_canonical_sides_are_missing():
    watched = WatchedSignal(_signal("CALL"), overnight=False)
    assert watched.check(0.0, 0.0) == WatchState.PENDING
    assert watched.breach_count == 0
    assert watched.trigger_price is None


def test_invalid_zero_quote_does_not_expire_aged_put():
    watched = WatchedSignal(_signal("PUT"), overnight=False)
    watched.created_at = datetime.now(timezone.utc) - timedelta(
        minutes=MAX_INTRADAY_WATCH_MIN + 1
    )
    assert watched.check(0.0, 0.0) != WatchState.EXPIRED
