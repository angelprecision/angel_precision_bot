from __future__ import annotations

from types import SimpleNamespace

import pytest

from ap_entry_watcher import APEntryWatcher, WatchState, WatchedSignal


class DummyBroker:
    session = None


class TestWatcher(APEntryWatcher):
    def __init__(self, *args, quotes=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._quotes = quotes or {}
        self.audit_rows = []

    def _fetch_quotes(self, tickers):
        return {str(k).upper(): dict(v) for k, v in self._quotes.items()}

    def _persist_watcher_audit(self, local_order_id, payload):
        self.audit_rows.append((local_order_id, dict(payload)))


def _signal(**overrides):
    base = {
        "signal_id": "sig-1",
        "ticker": "AAPL",
        "side": "CALL",
        "score": 82,
        "grade": "A",
        "entry_price": 100.0,
        "stop_price": 95.0,
        "target_price": 105.0,
        "local_order_id": "lo-1",
        "client_id": "paper@example.com",
        "execution_mode": "paper",
        "contract_symbol": "AAPL260717C00100000",
        "timeframe": "1d",
        "trigger": {"entry": 100.0, "stop": 95.0, "pt1": 105.0},
    }
    base.update(overrides)
    return base


def _add_intraday_signal(watcher: TestWatcher, **overrides):
    watched = WatchedSignal(_signal(**overrides), overnight=False)
    watched._watcher_ref = watcher
    watcher._pending.append(watched)
    watcher._dedup_set.add(watched.signal_id)
    return watched


def _plan(**overrides):
    base = {
        "signal_id": "sig-plan-1",
        "ticker": "AAPL",
        "side": "CALL",
        "score": 82,
        "tier": "A",
        "trigger_price": 100.0,
        "stop_underlying": 95.0,
        "target_underlying": 105.0,
        "plan_id": "plan-1",
        "contract_symbol": "AAPL260717C00100000",
        "pattern": "1-2-2U",
        "timeframe": "1d",
        "strategy_type": "daily_continuation",
        "metadata": {},
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def _prime_trigger(watcher: TestWatcher):
    _add_intraday_signal(watcher)
    watcher._quotes = {
        "AAPL": {"bid": 100.0, "ask": 101.0, "last": 101.0, "quote_age_ms": 1}
    }


def test_watched_signal_rejects_missing_side_without_defaulting_call():
    with pytest.raises(ValueError, match="invalid_or_missing_side"):
        WatchedSignal(_signal(side=None), overnight=False)


def test_watch_rejects_missing_side_without_defaulting_call():
    watcher = TestWatcher(DummyBroker())
    plan = _plan(side=None)

    assert watcher.watch(plan, "lo-missing-side") is False
    assert watcher._pending == []
    assert getattr(watcher, "_last_reject_reason", None) == "invalid_or_missing_side"
    assert any(row[1]["reason_code"] == "invalid_or_missing_side" for row in watcher.audit_rows)


def test_watch_normalizes_bullish_and_bearish_aliases():
    call_watcher = TestWatcher(DummyBroker())
    assert call_watcher.watch(_plan(side="bullish"), "lo-bullish") is True
    assert call_watcher._pending[-1].side == "CALL"

    put_watcher = TestWatcher(DummyBroker())
    assert put_watcher.watch(
        _plan(
            signal_id="sig-plan-put",
            side="bearish",
            trigger_price=100.0,
            stop_underlying=105.0,
            target_underlying=95.0,
        ),
        "lo-bearish",
    ) is True
    assert put_watcher._pending[-1].side == "PUT"


def test_active_watcher_does_not_trigger_from_last_only_quote():
    watcher = TestWatcher(
        DummyBroker(),
        quotes={"AAPL": {"bid": 0.0, "ask": 0.0, "last": 150.0, "quote_age_ms": 4}},
    )
    _add_intraday_signal(watcher)
    triggered = []
    watcher.on_trigger = lambda watched: triggered.append(watched)

    watcher._poll_active_signals()
    watcher._poll_active_signals()

    assert triggered == []
    assert len(watcher._pending) == 1
    assert watcher._pending[0].state == WatchState.PENDING
    assert watcher._pending[0].breach_count == 0


def test_on_trigger_failure_keeps_watcher_pending_for_retry_then_durable_success_removes():
    watcher = TestWatcher(DummyBroker())
    _prime_trigger(watcher)
    calls = {"count": 0}

    def flaky_on_trigger(watched):
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("transient db hiccup")
        return {"disposition": "SUBMITTED"}

    watcher.on_trigger = flaky_on_trigger

    watcher._poll_active_signals()  # first momentum poll
    watcher._poll_active_signals()  # trigger, callback fails

    assert calls["count"] == 1
    assert len(watcher._pending) == 1
    assert watcher._pending[0].state == WatchState.PENDING
    # First-breach evidence is deliberately preserved across callback retries.
    assert watcher._pending[0].triggered_at is not None
    assert watcher._pending[0].trigger_price == 101.0
    assert getattr(watcher._pending[0], "_trigger_attempts") == 1
    assert "sig-1" in watcher._dedup_set

    watcher._poll_active_signals()  # retained watcher retries and hands off

    assert calls["count"] == 2
    assert watcher._pending == []
    assert "sig-1" not in watcher._dedup_set


def test_on_trigger_three_failures_enters_ownership_quarantine_without_release():
    watcher = TestWatcher(DummyBroker())
    _prime_trigger(watcher)
    expired = []

    def always_fails(watched):
        raise RuntimeError("tradier timeout")

    watcher.on_trigger = always_fails
    watcher.on_expire = lambda watched: expired.append(watched)

    watcher._poll_active_signals()  # first momentum poll
    watcher._poll_active_signals()  # attempt 1
    watcher._poll_active_signals()  # attempt 2
    watcher._poll_active_signals()  # attempt 3 -> no durable terminalization available

    assert len(watcher._pending) == 1
    retained = watcher._pending[0]
    assert retained.state == WatchState.PENDING
    assert getattr(retained, "_ownership_quarantine", False) is True
    assert getattr(retained, "_trigger_attempts") == 3
    assert "sig-1" in watcher._dedup_set
    assert expired == []
    assert str(getattr(retained, "_quarantine_reason", "")).startswith(
        "trigger_exhaustion_terminal_write_failed:"
    )
