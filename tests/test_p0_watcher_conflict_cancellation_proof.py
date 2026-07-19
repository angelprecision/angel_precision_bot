from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from ap_entry_watcher import APEntryWatcher, WatchState, WatchedSignal


class DummyBroker:
    session = None


class FakeOSM:
    def __init__(self, rows, cancel_results=None, unreadable=None, observer=None):
        self.rows = {key: dict(value) for key, value in rows.items()}
        self.cancel_results = dict(cancel_results or {})
        self.unreadable = set(unreadable or ())
        self.observer = observer
        self.cancel_calls = []

    def has_order(self, local_order_id):
        return local_order_id in self.rows

    def cancel_pending_entry(self, local_order_id, reason=None):
        self.cancel_calls.append((local_order_id, reason))
        if self.observer:
            self.observer("cancel", local_order_id)
        result = self.cancel_results.get(local_order_id, True)
        if isinstance(result, BaseException):
            raise result
        if result:
            self.rows[local_order_id]["status"] = "CANCELED"
        return bool(result)

    def get_order(self, local_order_id):
        if self.observer:
            self.observer("read", local_order_id)
        if local_order_id in self.unreadable:
            raise RuntimeError("durable row unavailable")
        row = self.rows.get(local_order_id)
        return dict(row) if isinstance(row, dict) else row


class AuditWatcher(APEntryWatcher):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.audits = []

    def _persist_watcher_audit(self, local_order_id, payload):
        self.audits.append((local_order_id, dict(payload)))


def signal(
    *,
    signal_id,
    local_order_id,
    side,
    score,
    timeframe="1d",
    client_id="client@example.com",
    execution_mode="paper",
    trigger=None,
):
    entry = float(trigger if trigger is not None else (100 if side == "CALL" else 90))
    stop = entry - 5 if side == "CALL" else entry + 5
    target = entry + 5 if side == "CALL" else entry - 5
    return {
        "signal_id": signal_id,
        "ticker": "AAPL",
        "side": side,
        "score": score,
        "grade": "A",
        "entry_price": entry,
        "stop_price": stop,
        "target_price": target,
        "timeframe": timeframe,
        "local_order_id": local_order_id,
        "client_id": client_id,
        "execution_mode": execution_mode,
        "contract_symbol": "AAPL260717C00100000",
        "trigger": {"entry": entry, "stop": stop, "pt1": target},
    }


def row_for(sig, *, status="PENDING_TRIGGER", **overrides):
    row = {
        "local_order_id": sig["local_order_id"],
        "client_id": sig["client_id"],
        "execution_mode": sig["execution_mode"],
        "signal_id": sig["signal_id"],
        "status": status,
        "kind": "ENTRY",
        "meta": {
            "client_id": sig["client_id"],
            "execution_mode": sig["execution_mode"],
            "signal_id": sig["signal_id"],
        },
    }
    row.update(overrides)
    return row


def seed(watcher, sig, *, stale=False, rearm_only=False):
    watched = WatchedSignal(dict(sig), overnight=False)
    watched._watcher_ref = watcher
    if stale:
        watched.created_at = datetime.now(timezone.utc) - timedelta(hours=1)
    if rearm_only:
        watched.rearm_mode = True
    with watcher._lock:
        watcher._pending.append(watched)
        watcher._dedup_set.add(sig["signal_id"])
    return watched


def active_directions(watcher):
    with watcher._lock:
        return {
            watched.side
            for watched in watcher._pending
            if watched.is_active or getattr(watched, "rearm_mode", False)
        }


def assert_retained(watcher, watched, sig):
    with watcher._lock:
        assert watched in watcher._pending
        assert watched.state == WatchState.PENDING
        assert sig["signal_id"] in watcher._dedup_set
    assert active_directions(watcher) == {sig["side"]}


def test_stale_opposite_stays_owned_until_cancel_true_then_replacement_admitted():
    old = signal(signal_id="old", local_order_id="old-lo", side="PUT", score=70)
    new = signal(signal_id="new", local_order_id="new-lo", side="CALL", score=80)
    watcher_ref = {}

    def observer(stage, local_order_id):
        if stage == "cancel" and local_order_id == "old-lo":
            watcher = watcher_ref["watcher"]
            locked = watcher._lock.acquire(blocking=False)
            assert locked, "watcher lock must not be held during OSM cancellation"
            watcher._lock.release()
            with watcher._lock:
                assert existing in watcher._pending
                assert existing.state == WatchState.PENDING
                assert "old" in watcher._dedup_set

    osm = FakeOSM(
        {"old-lo": row_for(old), "new-lo": row_for(new)},
        cancel_results={"old-lo": True},
        observer=observer,
    )
    watcher = AuditWatcher(DummyBroker(), order_state_machine=osm)
    watcher_ref["watcher"] = watcher
    existing = seed(watcher, old, stale=True)

    assert watcher.add_signal(dict(new)) is True
    with watcher._lock:
        assert existing not in watcher._pending
        assert existing.state == WatchState.CANCELLED
        assert "old" not in watcher._dedup_set
        assert any(item.signal_id == "new" for item in watcher._pending)
    assert active_directions(watcher) == {"CALL"}


def test_false_cancel_pending_row_retains_owner_and_blocks_incoming():
    old = signal(signal_id="old", local_order_id="old-lo", side="PUT", score=70)
    new = signal(signal_id="new", local_order_id="new-lo", side="CALL", score=80)
    osm = FakeOSM(
        {"old-lo": row_for(old), "new-lo": row_for(new)},
        cancel_results={"old-lo": False},
    )
    watcher = AuditWatcher(DummyBroker(), order_state_machine=osm)
    existing = seed(watcher, old, stale=True)

    assert watcher.add_signal(dict(new)) is False
    assert_retained(watcher, existing, old)
    assert "new" not in watcher._dedup_set
    assert watcher._last_reject_reason == "conflict_cancel_unproven"
    assert any(
        payload.get("reason_code") == "conflict_cancel_unproven"
        and "cancel_returned_false_row_pending" in str(payload.get("raw_reason"))
        for _, payload in watcher.audits
    )


def test_cancel_raises_and_row_unreadable_retains_owner():
    old = signal(signal_id="old", local_order_id="old-lo", side="PUT", score=70)
    new = signal(signal_id="new", local_order_id="new-lo", side="CALL", score=80)
    osm = FakeOSM(
        {"old-lo": row_for(old), "new-lo": row_for(new)},
        cancel_results={"old-lo": RuntimeError("db write failed")},
        unreadable={"old-lo"},
    )
    watcher = AuditWatcher(DummyBroker(), order_state_machine=osm)
    existing = seed(watcher, old, stale=True)

    assert watcher.add_signal(dict(new)) is False
    assert_retained(watcher, existing, old)
    assert any(
        "cancel_raised_row_unreadable" in str(payload.get("raw_reason"))
        for _, payload in watcher.audits
    )


def test_false_cancel_exact_terminal_reread_allows_replacement():
    old = signal(signal_id="old", local_order_id="old-lo", side="PUT", score=70)
    new = signal(signal_id="new", local_order_id="new-lo", side="CALL", score=80)
    osm = FakeOSM(
        {"old-lo": row_for(old, status="EXPIRED"), "new-lo": row_for(new)},
        cancel_results={"old-lo": False},
    )
    watcher = AuditWatcher(DummyBroker(), order_state_machine=osm)
    existing = seed(watcher, old, stale=True)

    assert watcher.add_signal(dict(new)) is True
    with watcher._lock:
        assert existing not in watcher._pending
        assert "old" not in watcher._dedup_set
        assert any(item.signal_id == "new" for item in watcher._pending)


def test_terminal_reread_identity_mismatch_fails_closed():
    old = signal(signal_id="old", local_order_id="old-lo", side="PUT", score=70)
    new = signal(signal_id="new", local_order_id="new-lo", side="CALL", score=80)
    mismatched = row_for(old, status="CANCELED")
    mismatched["client_id"] = "other@example.com"
    osm = FakeOSM(
        {"old-lo": mismatched, "new-lo": row_for(new)},
        cancel_results={"old-lo": False},
    )
    watcher = AuditWatcher(DummyBroker(), order_state_machine=osm)
    existing = seed(watcher, old, stale=True)

    assert watcher.add_signal(dict(new)) is False
    assert_retained(watcher, existing, old)
    assert any(
        "durable_identity_mismatch:client_id" in str(payload.get("raw_reason"))
        for _, payload in watcher.audits
    )


def test_cancel_method_unavailable_retains_owner():
    old = signal(signal_id="old", local_order_id="old-lo", side="PUT", score=70)
    new = signal(signal_id="new", local_order_id="new-lo", side="CALL", score=80)

    class NoCancelOSM:
        def has_order(self, local_order_id):
            return True

        def get_order(self, local_order_id):
            return row_for(old)

    watcher = AuditWatcher(DummyBroker(), order_state_machine=NoCancelOSM())
    existing = seed(watcher, old, stale=True)

    assert watcher.add_signal(dict(new)) is False
    assert_retained(watcher, existing, old)
    assert any(
        "osm_cancel_unavailable" in str(payload.get("raw_reason"))
        for _, payload in watcher.audits
    )


@pytest.mark.parametrize("cancel_result,expected", [(True, True), (False, False)])
def test_stronger_direction_flip_uses_same_proof_invariant(cancel_result, expected):
    old = signal(signal_id="old", local_order_id="old-lo", side="PUT", score=70)
    new = signal(signal_id="new", local_order_id="new-lo", side="CALL", score=95)
    osm = FakeOSM(
        {"old-lo": row_for(old), "new-lo": row_for(new)},
        cancel_results={"old-lo": cancel_result},
    )
    watcher = AuditWatcher(DummyBroker(), order_state_machine=osm)
    existing = seed(watcher, old)

    assert watcher.add_signal(dict(new)) is expected
    if expected:
        assert active_directions(watcher) == {"CALL"}
        assert "old" not in watcher._dedup_set
    else:
        assert_retained(watcher, existing, old)


def test_stronger_same_side_false_cancel_retains_owner_and_blocks_replacement():
    old = signal(signal_id="old", local_order_id="old-lo", side="CALL", score=70)
    new = signal(signal_id="new", local_order_id="new-lo", side="CALL", score=95)
    osm = FakeOSM(
        {"old-lo": row_for(old), "new-lo": row_for(new)},
        cancel_results={"old-lo": False},
    )
    watcher = AuditWatcher(DummyBroker(), order_state_machine=osm)
    existing = seed(watcher, old)

    assert watcher.add_signal(dict(new)) is False
    assert_retained(watcher, existing, old)
    assert "new" not in watcher._dedup_set
    assert osm.rows["old-lo"]["status"] == "PENDING_TRIGGER"
    assert any(
        payload.get("reason_code") == "conflict_cancel_unproven"
        and "cancel_returned_false_row_pending" in str(payload.get("raw_reason"))
        for _, payload in watcher.audits
    )


def test_rearm_only_and_higher_timeframe_tie_use_proven_cancellation():
    old = signal(
        signal_id="old", local_order_id="old-lo", side="PUT", score=80, timeframe="15m"
    )
    new = signal(
        signal_id="new", local_order_id="new-lo", side="CALL", score=80, timeframe="1d"
    )
    osm = FakeOSM(
        {"old-lo": row_for(old), "new-lo": row_for(new)},
        cancel_results={"old-lo": True},
    )
    watcher = AuditWatcher(DummyBroker(), order_state_machine=osm)
    seed(watcher, old, rearm_only=True)

    assert watcher.add_signal(dict(new)) is True
    assert active_directions(watcher) == {"CALL"}


def plan_from(sig):
    return SimpleNamespace(
        signal_id=sig["signal_id"],
        ticker=sig["ticker"],
        side=sig["side"],
        score=sig["score"],
        tier=sig["grade"],
        trigger_price=sig["entry_price"],
        stop_underlying=sig["stop_price"],
        target_underlying=sig["target_price"],
        plan_id=f"plan-{sig['signal_id']}",
        contract_symbol=sig["contract_symbol"],
        pattern="test",
        timeframe=sig["timeframe"],
        strategy_type="daily",
        prior_day_high=sig["entry_price"],
        prior_day_low=None,
        entry_option_price=0,
        client_id=sig["client_id"],
        execution_mode=sig["execution_mode"],
        metadata={
            "client_id": sig["client_id"],
            "execution_mode": sig["execution_mode"],
        },
    )


def test_concurrent_watch_results_are_invocation_local_and_no_pending_row_is_ownerless():
    call = signal(signal_id="call", local_order_id="call-lo", side="CALL", score=80)
    put = signal(signal_id="put", local_order_id="put-lo", side="PUT", score=95)
    call_armed = threading.Event()
    release_call = threading.Event()

    class OverlapWatcher(AuditWatcher):
        def add_signal(self, payload):
            result = super().add_signal(payload)
            if payload.get("signal_id") == "call" and result:
                call_armed.set()
                assert release_call.wait(5)
            return result

    osm = FakeOSM(
        {"call-lo": row_for(call), "put-lo": row_for(put)},
        cancel_results={"call-lo": False, "put-lo": True},
    )
    watcher = OverlapWatcher(DummyBroker(), order_state_machine=osm)
    results = {}

    def run(name, sig):
        results[name] = watcher.watch_with_result(plan_from(sig), sig["local_order_id"])

    thread_a = threading.Thread(target=run, args=("A", call))
    thread_b = threading.Thread(target=run, args=("B", put))
    thread_a.start()
    assert call_armed.wait(5)
    thread_b.start()
    thread_b.join(5)
    release_call.set()
    thread_a.join(5)

    assert not thread_a.is_alive() and not thread_b.is_alive()
    assert results["A"].accepted is True
    assert results["A"].reason_code == "accepted"
    assert results["A"].local_order_id == "call-lo"
    assert results["A"].has_order_after is True
    assert results["B"].accepted is False
    assert results["B"].reason_code == "conflict_cancel_unproven"
    assert "cancel_returned_false_row_pending" in results["B"].detail
    assert results["B"].conflict_local_order_id == "call-lo"
    assert results["B"].has_order_after is False
    assert active_directions(watcher) == {"CALL"}
    assert osm.rows["call-lo"]["status"] == "PENDING_TRIGGER"
    assert watcher.has_order("call-lo") is True
    assert osm.rows["put-lo"]["status"] == "CANCELED"


def test_runtime_package_watch_forwards_recovery_compatibility_flags(monkeypatch):
    captured = {}
    legacy_class = APEntryWatcher.__mro__[1]

    def fake_watch(self, plan, local_order_id, **kwargs):
        captured.update(kwargs)
        return True

    monkeypatch.setattr(legacy_class, "watch", fake_watch)
    watcher = APEntryWatcher(DummyBroker(), order_state_machine=None)
    plan = SimpleNamespace(side="CALL", ticker="AAPL")

    assert watcher.watch(
        plan,
        "lo",
        recovery_rearm=True,
        no_cancel_on_reject=True,
        materialization_resume=True,
    ) is True
    assert captured == {
        "recovery_rearm": True,
        "no_cancel_on_reject": True,
        "materialization_resume": True,
    }
