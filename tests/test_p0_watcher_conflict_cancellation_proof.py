from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from ap_entry_watcher import APEntryWatcher, WatchState, WatchedSignal


class DummyBroker:
    session = None


class FakeOSM:
    def __init__(
        self,
        rows,
        cancel_results=None,
        unreadable=None,
        observer=None,
        meta_results=None,
    ):
        self.rows = {key: dict(value) for key, value in rows.items()}
        self.cancel_results = dict(cancel_results or {})
        self.meta_results = dict(meta_results or {})
        self.unreadable = set(unreadable or ())
        self.observer = observer
        self.cancel_calls = []
        self.meta_calls = []
        self.meta_expected_statuses = []
        self.meta_expected_execution_modes = []
        self.meta_expected_signal_ids = []

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

    def update_order_meta(
        self,
        local_order_id,
        patch,
        *,
        expected_status=None,
        expected_execution_mode=None,
        expected_signal_id=None,
    ):
        self.meta_calls.append((local_order_id, dict(patch)))
        self.meta_expected_statuses.append(expected_status)
        self.meta_expected_execution_modes.append(expected_execution_mode)
        self.meta_expected_signal_ids.append(expected_signal_id)
        if self.observer:
            self.observer("meta", local_order_id)
        result = self.meta_results.get(local_order_id, True)
        if isinstance(result, BaseException):
            raise result
        if not result or local_order_id not in self.rows:
            return False
        if (
            expected_status is not None
            and str(self.rows[local_order_id].get("status") or "").upper()
            != str(expected_status).upper()
        ):
            return False
        if (
            expected_execution_mode is not None
            and str(self.rows[local_order_id].get("execution_mode") or "").strip().lower()
            != str(expected_execution_mode).strip().lower()
        ):
            return False
        if (
            expected_signal_id is not None
            and str(self.rows[local_order_id].get("signal_id") or "").strip()
            != str(expected_signal_id).strip()
        ):
            return False
        self.rows[local_order_id].setdefault("meta", {}).update(dict(patch))
        return True


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
            "canonical_signal_id": sig.get("canonical_signal_id") or sig["signal_id"],
            "symbol": sig["ticker"],
            "direction": sig["side"],
        },
        "canonical_signal_id": sig.get("canonical_signal_id") or sig["signal_id"],
        "symbol": sig["ticker"],
        "direction": sig["side"],
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


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("symbol", "MSFT", "ticker"),
        ("direction", "CALL", "side"),
        ("canonical_signal_id", "other-canonical", "canonical_signal_id"),
    ],
)
def test_terminal_reread_complete_identity_mismatch_fails_closed(
    field, value, reason
):
    old = signal(signal_id="old", local_order_id="old-lo", side="PUT", score=70)
    new = signal(signal_id="new", local_order_id="new-lo", side="CALL", score=80)
    mismatched = row_for(old, status="CANCELED")
    mismatched[field] = value
    osm = FakeOSM(
        {"old-lo": mismatched, "new-lo": row_for(new)},
        cancel_results={"old-lo": False},
    )
    watcher = AuditWatcher(DummyBroker(), order_state_machine=osm)
    existing = seed(watcher, old, stale=True)

    assert watcher.add_signal(dict(new)) is False
    assert_retained(watcher, existing, old)
    assert any(
        f"durable_identity_mismatch:{reason}"
        in str(payload.get("raw_reason"))
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


def test_prebreach_direction_does_not_flip_or_cancel_by_score():
    old = signal(signal_id="old", local_order_id="old-lo", side="PUT", score=70)
    new = signal(signal_id="new", local_order_id="new-lo", side="CALL", score=95)
    osm = FakeOSM(
        {"old-lo": row_for(old), "new-lo": row_for(new)},
        cancel_results={"old-lo": False},
    )
    watcher = AuditWatcher(DummyBroker(), order_state_machine=osm)
    existing = seed(watcher, old)

    assert watcher.add_signal(dict(new)) is True
    assert active_directions(watcher) == {"CALL", "PUT"}
    assert existing in watcher._pending
    assert "old" in watcher._dedup_set
    assert osm.cancel_calls == []


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
        def add_signal(self, payload, *, registration_provenance_out=None):
            result = super().add_signal(
                payload, registration_provenance_out=registration_provenance_out,
            )
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
    assert results["B"].accepted is True
    assert results["B"].reason_code == "accepted"
    assert results["B"].has_order_after is True
    assert active_directions(watcher) == {"CALL", "PUT"}
    assert osm.rows["call-lo"]["status"] == "PENDING_TRIGGER"
    assert osm.rows["put-lo"]["status"] == "PENDING_TRIGGER"
    assert watcher.has_order("call-lo") is True
    assert watcher.has_order("put-lo") is True
    assert osm.cancel_calls == []


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
        "registration_provenance_out": None,
    }


def _poll_quote(watcher, *, bid, ask, open_protect_active=False):
    # Direct admission tests must exercise the poll arbitration regardless of
    # the wall-clock session when the suite is run.
    with watcher._lock:
        for watched in watcher._pending:
            watched.overnight = False
    watcher._fetch_quotes = lambda tickers: {
        ticker: {"bid": bid, "ask": ask}
        for ticker in tickers
    }
    watcher._poll_active_signals(open_protect_active)


@pytest.mark.parametrize(
    "registration_order",
    [("CALL", "PUT"), ("PUT", "CALL")],
)
def test_market_open_protection_arbitrates_same_poll_before_duplicate_barrier(
    registration_order,
):
    call = signal(
        signal_id="open-call",
        local_order_id="open-call-lo",
        side="CALL",
        score=70,
        trigger=100,
    )
    put = signal(
        signal_id="open-put",
        local_order_id="open-put-lo",
        side="PUT",
        score=70,
        trigger=105,
    )
    signals = {"CALL": call, "PUT": put}
    osm = FakeOSM(
        {sig["local_order_id"]: row_for(sig) for sig in signals.values()}
    )
    watcher = AuditWatcher(DummyBroker(), order_state_machine=osm)
    callbacks = []
    watcher.on_trigger = lambda watched: callbacks.append(watched.signal_id)

    for side in registration_order:
        assert watcher.add_signal(dict(signals[side])) is True

    # Both sides confirm in one poll.  The package arbitration hook must see
    # both candidates before market-open protection can apply a duplicate rule.
    _poll_quote(watcher, bid=98, ask=106, open_protect_active=True)
    _poll_quote(watcher, bid=98, ask=106, open_protect_active=True)

    assert callbacks == []
    assert osm.cancel_calls == []
    assert active_directions(watcher) == {"CALL", "PUT"}
    assert watcher._open_trigger_keys == set()


@pytest.mark.parametrize("registration_order", [("CALL", "PUT"), ("PUT", "CALL")])
@pytest.mark.parametrize(
    "call_identity,put_identity",
    [
        (("call-client@example.com", "paper"), ("put-client@example.com", "paper")),
        (("same-client@example.com", "paper"), ("same-client@example.com", "live")),
    ],
)
def test_market_open_protection_uses_directional_ownership_identity(
    registration_order, call_identity, put_identity
):
    call = signal(
        signal_id="identity-call",
        local_order_id="identity-call-lo",
        side="CALL",
        score=70,
        trigger=100,
        client_id=call_identity[0],
        execution_mode=call_identity[1],
    )
    put = signal(
        signal_id="identity-put",
        local_order_id="identity-put-lo",
        side="PUT",
        score=70,
        trigger=105,
        client_id=put_identity[0],
        execution_mode=put_identity[1],
    )
    signals = {"CALL": call, "PUT": put}
    osm = FakeOSM(
        {sig["local_order_id"]: row_for(sig) for sig in signals.values()}
    )
    watcher = AuditWatcher(DummyBroker(), order_state_machine=osm)
    callbacks = []
    watcher.on_trigger = lambda watched: (
        callbacks.append(watched.signal_id) or {"disposition": "TERMINAL_DURABLE"}
    )

    for side in registration_order:
        assert watcher.add_signal(dict(signals[side])) is True
    _poll_quote(watcher, bid=98, ask=106, open_protect_active=True)
    _poll_quote(watcher, bid=98, ask=106, open_protect_active=True)

    assert set(callbacks) == {"identity-call", "identity-put"}
    assert osm.cancel_calls == []
    assert watcher._open_trigger_keys == {
        ("direction", call_identity[0], call_identity[1], "AAPL"),
        ("direction", put_identity[0], put_identity[1], "AAPL"),
    }


def test_market_open_protection_preserves_same_watcher_callback_retry():
    call = signal(
        signal_id="open-retry",
        local_order_id="open-retry-lo",
        side="CALL",
        score=70,
        trigger=100,
    )
    osm = FakeOSM({"open-retry-lo": row_for(call)})
    watcher = AuditWatcher(DummyBroker(), order_state_machine=osm)
    callbacks = []

    def callback(watched):
        callbacks.append(watched.signal_id)
        if len(callbacks) == 1:
            return {"disposition": "RETRY_WAIT", "retry_after_seconds": 1}
        return {"disposition": "TERMINAL_DURABLE"}

    watcher.on_trigger = callback
    assert watcher.add_signal(dict(call)) is True
    _poll_quote(watcher, bid=98, ask=106, open_protect_active=True)
    _poll_quote(watcher, bid=98, ask=106, open_protect_active=True)

    assert callbacks == ["open-retry"]
    with watcher._lock:
        assert watcher._pending[0].state == WatchState.PENDING
        watcher._pending[0].deferred_retry_not_before = (
            datetime.now(timezone.utc) - timedelta(seconds=1)
        )

    _poll_quote(watcher, bid=98, ask=106, open_protect_active=True)

    assert callbacks == ["open-retry", "open-retry"]
    assert watcher.has_order("open-retry-lo") is False


def test_winner_authority_failure_holds_before_loser_cancellation():
    call = signal(
        signal_id="call-live",
        local_order_id="call-live-lo",
        side="CALL",
        score=70,
        trigger=100,
        execution_mode="live",
    )
    put = signal(
        signal_id="put-live",
        local_order_id="put-live-lo",
        side="PUT",
        score=70,
        trigger=90,
        execution_mode="live",
    )
    events = []
    osm = FakeOSM(
        {"call-live-lo": row_for(call), "put-live-lo": row_for(put)},
        meta_results={"call-live-lo": False},
        observer=lambda stage, local_order_id: events.append((stage, local_order_id)),
    )
    watcher = AuditWatcher(DummyBroker(), order_state_machine=osm, mode="LIVE")
    callbacks = []
    watcher.on_trigger = lambda watched: callbacks.append(watched.signal_id)

    assert watcher.add_signal(dict(call)) is True
    assert watcher.add_signal(dict(put)) is True
    _poll_quote(watcher, bid=98, ask=101)
    _poll_quote(watcher, bid=98, ask=101)

    assert callbacks == []
    assert osm.cancel_calls == []
    assert events == [
        ("read", "call-live-lo"),
        ("meta", "call-live-lo"),
    ]
    assert osm.rows["call-live-lo"]["status"] == "PENDING_TRIGGER"
    assert osm.rows["put-live-lo"]["status"] == "PENDING_TRIGGER"
    assert "trigger_crossed_at" not in osm.rows["call-live-lo"]["meta"]
    assert watcher._direction_claims[
        ("client@example.com", "live", "AAPL")
    ]["reason"] == "winner_authority_unproven"
    assert active_directions(watcher) == {"CALL", "PUT"}

    # Once the durable write becomes available, the held winner may proceed and
    # only then is the opposite allowed to be canceled.
    osm.meta_results["call-live-lo"] = True
    with watcher._lock:
        for watched in watcher._pending:
            watched.deferred_retry_not_before = None
    _poll_quote(watcher, bid=98, ask=101)

    assert callbacks == ["call-live"]
    assert osm.cancel_calls == [
        ("put-live-lo", "confirmed_breach_direction_claim_lost")
    ]


@pytest.mark.parametrize(
    ("identity_field", "replacement"),
    [("signal_id", "MUTATED_SIGNAL"), ("execution_mode", "paper")],
)
def test_winner_authority_identity_cas_holds_after_proven_row_changes(
    identity_field, replacement
):
    call = signal(
        signal_id="CALL_SIGNAL",
        local_order_id="A",
        side="CALL",
        score=70,
        trigger=100,
        client_id="Jason",
        execution_mode="live",
    )
    put = signal(
        signal_id="PUT_SIGNAL",
        local_order_id="B",
        side="PUT",
        score=70,
        trigger=90,
        client_id="Jason",
        execution_mode="live",
    )
    events = []
    holder = {}

    def observer(stage, local_order_id):
        events.append((stage, local_order_id))
        if stage == "meta" and local_order_id == "A":
            row = holder["osm"].rows[local_order_id]
            row[identity_field] = replacement
            row["meta"][identity_field] = replacement

    osm = FakeOSM(
        {"A": row_for(call), "B": row_for(put)},
        observer=observer,
    )
    holder["osm"] = osm
    watcher = AuditWatcher(DummyBroker(), order_state_machine=osm, mode="LIVE")
    callbacks = []
    broker_submits = []

    def callback(watched):
        callbacks.append(watched.signal_id)
        broker_submits.append(watched.signal_id)

    watcher.on_trigger = callback

    assert watcher.add_signal(dict(call)) is True
    assert watcher.add_signal(dict(put)) is True
    _poll_quote(watcher, bid=98, ask=101)
    _poll_quote(watcher, bid=98, ask=101)

    assert events == [("read", "A"), ("meta", "A")]
    assert osm.meta_expected_statuses == ["PENDING_TRIGGER"]
    assert osm.meta_expected_execution_modes == ["live"]
    assert osm.meta_expected_signal_ids == ["CALL_SIGNAL"]
    assert callbacks == []
    assert broker_submits == []
    assert osm.cancel_calls == []
    assert osm.rows["A"]["status"] == "PENDING_TRIGGER"
    assert osm.rows["B"]["status"] == "PENDING_TRIGGER"
    claim = watcher._direction_claims[("jason", "live", "AAPL")]
    assert claim == {"status": "ambiguous_hold", "reason": "winner_authority_unproven"}
    assert any(
        payload.get("reason_code") == "direction_claim_authority_unproven_hold"
        for _, payload in watcher.audits
    )
    assert active_directions(watcher) == {"CALL", "PUT"}


def test_winner_authority_identity_cas_rechecks_on_retry_before_cancel():
    call = signal(
        signal_id="call-retry-cas",
        local_order_id="call-retry-cas-lo",
        side="CALL",
        score=70,
        trigger=100,
    )
    put = signal(
        signal_id="put-retry-cas",
        local_order_id="put-retry-cas-lo",
        side="PUT",
        score=70,
        trigger=90,
    )
    holder = {}
    meta_attempts = {"n": 0}

    def observer(stage, local_order_id):
        if stage == "meta" and local_order_id == call["local_order_id"]:
            meta_attempts["n"] += 1
            if meta_attempts["n"] == 2:
                row = holder["osm"].rows[local_order_id]
                row["signal_id"] = "mutated-on-retry"
                row["meta"]["signal_id"] = "mutated-on-retry"

    osm = FakeOSM(
        {call["local_order_id"]: row_for(call), put["local_order_id"]: row_for(put)},
        cancel_results={put["local_order_id"]: False},
        observer=observer,
    )
    holder["osm"] = osm
    watcher = AuditWatcher(DummyBroker(), order_state_machine=osm)
    callbacks = []
    watcher.on_trigger = lambda watched: callbacks.append(watched.signal_id)

    assert watcher.add_signal(dict(call)) is True
    assert watcher.add_signal(dict(put)) is True
    _poll_quote(watcher, bid=98, ask=101)
    _poll_quote(watcher, bid=98, ask=101)
    assert osm.cancel_calls == [(put["local_order_id"], "confirmed_breach_direction_claim_lost")]

    with watcher._lock:
        for watched in watcher._pending:
            watched.deferred_retry_not_before = None
    _poll_quote(watcher, bid=98, ask=101)

    assert meta_attempts["n"] == 2
    assert osm.cancel_calls == [(put["local_order_id"], "confirmed_breach_direction_claim_lost")]
    assert callbacks == []
    assert active_directions(watcher) == {"CALL", "PUT"}


@pytest.mark.parametrize(
    "first_side,second_side",
    [("CALL", "PUT"), ("PUT", "CALL")],
)
def test_equal_score_opposites_coarm_in_either_registration_order(first_side, second_side):
    first = signal(
        signal_id=f"{first_side.lower()}-first",
        local_order_id=f"{first_side.lower()}-first-lo",
        side=first_side,
        score=70,
        timeframe="1d",
    )
    second = signal(
        signal_id=f"{second_side.lower()}-second",
        local_order_id=f"{second_side.lower()}-second-lo",
        side=second_side,
        score=70,
        timeframe="1d",
    )
    osm = FakeOSM({
        first["local_order_id"]: row_for(first),
        second["local_order_id"]: row_for(second),
    })
    watcher = AuditWatcher(DummyBroker(), order_state_machine=osm)

    assert watcher.add_signal(dict(first)) is True
    assert watcher.add_signal(dict(second)) is True
    assert active_directions(watcher) == {"CALL", "PUT"}
    assert osm.cancel_calls == []
    assert any(
        payload.get("reason_code") == "opposite_side_coarmed"
        for _, payload in watcher.audits
    )


def test_opposite_direction_isolated_by_client_and_execution_mode():
    old = signal(
        signal_id="other-client-put",
        local_order_id="other-client-put-lo",
        side="PUT",
        score=95,
        client_id="other@example.com",
        execution_mode="live",
    )
    new = signal(
        signal_id="paper-call",
        local_order_id="paper-call-lo",
        side="CALL",
        score=70,
        client_id="client@example.com",
        execution_mode="paper",
    )
    osm = FakeOSM({
        old["local_order_id"]: row_for(old),
        new["local_order_id"]: row_for(new),
    })
    watcher = AuditWatcher(DummyBroker(), order_state_machine=osm)

    assert watcher.add_signal(dict(old)) is True
    assert watcher.add_signal(dict(new)) is True
    assert active_directions(watcher) == {"CALL", "PUT"}
    assert osm.cancel_calls == []


@pytest.mark.parametrize(
    "mutation", ["terminal", "signal_id", "direction", "canonical_signal_id"]
)
def test_confirmed_direction_requires_exact_pending_winner_row(mutation):
    call = signal(
        signal_id="call",
        local_order_id="call-lo",
        side="CALL",
        score=70,
        trigger=100,
    )
    put = signal(
        signal_id="put",
        local_order_id="put-lo",
        side="PUT",
        score=70,
        trigger=90,
    )
    call_row = row_for(call)
    if mutation == "terminal":
        call_row["status"] = "CANCELED"
    elif mutation == "signal_id":
        call_row["signal_id"] = "other-signal"
        call_row["meta"]["signal_id"] = "other-signal"
    elif mutation == "direction":
        call_row["direction"] = "PUT"
        call_row["meta"]["direction"] = "PUT"
    else:
        call_row["canonical_signal_id"] = "other-canonical"
        call_row["meta"]["canonical_signal_id"] = "other-canonical"
    osm = FakeOSM({"call-lo": call_row, "put-lo": row_for(put)})
    watcher = AuditWatcher(DummyBroker(), order_state_machine=osm)
    callbacks = []
    watcher.on_trigger = lambda watched: callbacks.append(watched.signal_id)

    assert watcher.add_signal(dict(call)) is True
    assert watcher.add_signal(dict(put)) is True
    _poll_quote(watcher, bid=98, ask=101)
    _poll_quote(watcher, bid=98, ask=101)

    assert callbacks == []
    assert osm.cancel_calls == []
    assert osm.meta_calls == []
    assert osm.meta_expected_statuses == []
    assert active_directions(watcher) == {"CALL", "PUT"}
    assert watcher._direction_claims[
        ("client@example.com", "paper", "AAPL")
    ]["reason"] == "winner_authority_unproven"


def test_confirmed_direction_winner_write_is_pending_status_cas():
    call = signal(
        signal_id="call-cas",
        local_order_id="call-cas-lo",
        side="CALL",
        score=70,
        trigger=100,
    )
    put = signal(
        signal_id="put-cas",
        local_order_id="put-cas-lo",
        side="PUT",
        score=70,
        trigger=90,
    )
    holder = {}

    def observer(stage, local_order_id):
        if stage == "meta" and local_order_id == call["local_order_id"]:
            holder["osm"].rows[local_order_id]["status"] = "CANCELED"

    osm = FakeOSM(
        {"call-cas-lo": row_for(call), "put-cas-lo": row_for(put)},
        observer=observer,
    )
    holder["osm"] = osm
    watcher = AuditWatcher(DummyBroker(), order_state_machine=osm)
    callbacks = []
    watcher.on_trigger = lambda watched: callbacks.append(watched.signal_id)

    assert watcher.add_signal(dict(call)) is True
    assert watcher.add_signal(dict(put)) is True
    _poll_quote(watcher, bid=98, ask=101)
    _poll_quote(watcher, bid=98, ask=101)

    assert callbacks == []
    assert osm.cancel_calls == []
    assert osm.meta_expected_statuses == ["PENDING_TRIGGER"]
    assert active_directions(watcher) == {"CALL", "PUT"}


def test_terminal_cancellation_requires_durable_signal_id():
    old = signal(signal_id="old", local_order_id="old-lo", side="PUT", score=70)
    new = signal(signal_id="new", local_order_id="new-lo", side="CALL", score=80)
    missing_signal_id = row_for(old, status="CANCELED")
    missing_signal_id.pop("signal_id")
    missing_signal_id["meta"].pop("signal_id")
    osm = FakeOSM(
        {"old-lo": missing_signal_id, "new-lo": row_for(new)},
        cancel_results={"old-lo": False},
    )
    watcher = AuditWatcher(DummyBroker(), order_state_machine=osm)
    existing = seed(watcher, old, stale=True)

    assert watcher.add_signal(dict(new)) is False
    assert_retained(watcher, existing, old)
    assert any(
        "durable_identity_mismatch:signal_id_missing" in str(payload.get("raw_reason"))
        for _, payload in watcher.audits
    )


def test_confirmed_call_wins_and_cancels_prebreach_put_before_callback():
    call = signal(
        signal_id="call",
        local_order_id="call-lo",
        side="CALL",
        score=70,
        trigger=100,
    )
    put = signal(
        signal_id="put",
        local_order_id="put-lo",
        side="PUT",
        score=70,
        trigger=90,
    )
    events = []
    osm = FakeOSM(
        {"call-lo": row_for(call), "put-lo": row_for(put)},
        observer=lambda stage, local_order_id: events.append((stage, local_order_id)),
    )
    watcher = AuditWatcher(DummyBroker(), order_state_machine=osm)
    callbacks = []
    watcher.on_trigger = lambda watched: (
        callbacks.append(watched.signal_id) or {"disposition": "TERMINAL_DURABLE"}
    )

    assert watcher.add_signal(dict(call)) is True
    assert watcher.add_signal(dict(put)) is True
    _poll_quote(watcher, bid=98, ask=101)
    _poll_quote(watcher, bid=98, ask=101)

    assert callbacks == ["call"]
    assert osm.cancel_calls == [
        ("put-lo", "confirmed_breach_direction_claim_lost")
    ]
    assert osm.meta_expected_statuses == ["PENDING_TRIGGER"]
    assert osm.meta_expected_execution_modes == ["paper"]
    assert osm.meta_expected_signal_ids == ["call"]
    assert osm.rows["put-lo"]["status"] == "CANCELED"
    assert watcher.has_order("put-lo") is False
    assert watcher.has_order("call-lo") is False
    assert events.index(("meta", "call-lo")) < events.index(("cancel", "put-lo"))
    reasons = [payload.get("reason_code") for _, payload in watcher.audits]
    assert "direction_claim_won" in reasons
    assert "direction_claim_lost" in reasons


def test_confirmed_put_wins_and_cancels_prebreach_call_before_callback():
    call = signal(
        signal_id="call",
        local_order_id="call-lo",
        side="CALL",
        score=70,
        trigger=120,
    )
    put = signal(
        signal_id="put",
        local_order_id="put-lo",
        side="PUT",
        score=70,
        trigger=110,
    )
    osm = FakeOSM({"call-lo": row_for(call), "put-lo": row_for(put)})
    watcher = AuditWatcher(DummyBroker(), order_state_machine=osm)
    callbacks = []
    watcher.on_trigger = lambda watched: (
        callbacks.append(watched.signal_id) or {"disposition": "TERMINAL_DURABLE"}
    )

    assert watcher.add_signal(dict(call)) is True
    assert watcher.add_signal(dict(put)) is True
    _poll_quote(watcher, bid=109, ask=114)
    _poll_quote(watcher, bid=109, ask=114)

    assert callbacks == ["put"]
    assert osm.cancel_calls == [
        ("call-lo", "confirmed_breach_direction_claim_lost")
    ]
    assert osm.rows["call-lo"]["status"] == "CANCELED"
    assert watcher.has_order("call-lo") is False
    assert watcher.has_order("put-lo") is False


def test_same_poll_confirmed_directions_hold_without_callback_or_cancellation():
    call = signal(
        signal_id="call",
        local_order_id="call-lo",
        side="CALL",
        score=70,
        trigger=100,
    )
    put = signal(
        signal_id="put",
        local_order_id="put-lo",
        side="PUT",
        score=70,
        trigger=105,
    )
    osm = FakeOSM({"call-lo": row_for(call), "put-lo": row_for(put)})
    watcher = AuditWatcher(DummyBroker(), order_state_machine=osm)
    callbacks = []
    watcher.on_trigger = lambda watched: callbacks.append(watched.signal_id)

    assert watcher.add_signal(dict(call)) is True
    assert watcher.add_signal(dict(put)) is True
    _poll_quote(watcher, bid=98, ask=106)
    _poll_quote(watcher, bid=98, ask=106)

    assert callbacks == []
    assert osm.cancel_calls == []
    assert active_directions(watcher) == {"CALL", "PUT"}
    assert all(
        watched.state == WatchState.PENDING
        for watched in watcher._pending
    )
    assert any(
        payload.get("reason_code") == "direction_claim_ambiguous_hold"
        for _, payload in watcher.audits
    )


def test_confirmed_direction_holds_when_loser_cancellation_is_unproven():
    call = signal(
        signal_id="call",
        local_order_id="call-lo",
        side="CALL",
        score=70,
        trigger=100,
    )
    put = signal(
        signal_id="put",
        local_order_id="put-lo",
        side="PUT",
        score=70,
        trigger=90,
    )
    osm = FakeOSM(
        {"call-lo": row_for(call), "put-lo": row_for(put)},
        cancel_results={"put-lo": False},
    )
    watcher = AuditWatcher(DummyBroker(), order_state_machine=osm)
    callbacks = []
    watcher.on_trigger = lambda watched: callbacks.append(watched.signal_id)

    assert watcher.add_signal(dict(call)) is True
    assert watcher.add_signal(dict(put)) is True
    _poll_quote(watcher, bid=98, ask=101)
    _poll_quote(watcher, bid=98, ask=101)

    assert callbacks == []
    assert osm.cancel_calls == [
        ("put-lo", "confirmed_breach_direction_claim_lost")
    ]
    assert active_directions(watcher) == {"CALL", "PUT"}
    assert watcher._direction_claims[("client@example.com", "paper", "AAPL")]["status"] == "ambiguous_hold"
    assert any(
        payload.get("reason_code") == "direction_claim_cancel_unproven_hold"
        for _, payload in watcher.audits
    )


def test_won_direction_claim_blocks_opposite_replacement_after_callback_failure():
    call = signal(
        signal_id="call",
        local_order_id="call-lo",
        side="CALL",
        score=70,
        trigger=100,
    )
    put = signal(
        signal_id="put",
        local_order_id="put-lo",
        side="PUT",
        score=70,
        trigger=90,
    )
    osm = FakeOSM({"call-lo": row_for(call), "put-lo": row_for(put)})
    watcher = AuditWatcher(DummyBroker(), order_state_machine=osm)

    def callback_failure(_watched):
        raise RuntimeError("callback unavailable")

    watcher.on_trigger = callback_failure
    assert watcher.add_signal(dict(call)) is True
    assert watcher.add_signal(dict(put)) is True
    _poll_quote(watcher, bid=98, ask=101)
    _poll_quote(watcher, bid=98, ask=101)

    replacement = signal(
        signal_id="put-replacement",
        local_order_id="put-replacement-lo",
        side="PUT",
        score=95,
        trigger=90,
    )
    osm.rows["put-replacement-lo"] = row_for(replacement)

    assert watcher.add_signal(dict(replacement)) is False
    assert watcher._last_reject_reason == "direction_claim_active"
    assert osm.cancel_calls == [
        ("put-lo", "confirmed_breach_direction_claim_lost")
    ]
    assert osm.rows["call-lo"]["status"] == "PENDING_TRIGGER"
    assert watcher.has_order("call-lo") is True
    assert watcher.has_order("put-replacement-lo") is False


def test_rehydrated_confirmed_winner_blocks_score_reversal_after_restart():
    call = signal(
        signal_id="call",
        local_order_id="call-lo",
        side="CALL",
        score=70,
        trigger=100,
    )
    confirmed_at = datetime.now(timezone.utc).isoformat()
    call["metadata"] = {"trigger_crossed_at": confirmed_at}
    put = signal(
        signal_id="put-replacement",
        local_order_id="put-replacement-lo",
        side="PUT",
        score=95,
        trigger=90,
    )
    call_row = row_for(call)
    call_row["meta"]["trigger_crossed_at"] = confirmed_at
    osm = FakeOSM(
        {"call-lo": call_row, "put-replacement-lo": row_for(put)},
    )
    watcher = AuditWatcher(DummyBroker(), order_state_machine=osm)
    winner = seed(watcher, call)

    # This watcher was rehydrated into a new process: durable trigger evidence
    # exists, but no process-local direction claim has been reconstructed.
    assert winner.trigger_crossed_at is not None
    assert watcher._direction_claims == {}

    assert watcher.add_signal(dict(put)) is False
    assert watcher._last_reject_reason == "direction_claim_active"
    assert osm.cancel_calls == []
    assert watcher.has_order("call-lo") is True
    assert watcher.has_order("put-replacement-lo") is False


def test_rehydrated_confirmed_winner_blocks_stale_score_reversal_after_restart():
    call = signal(
        signal_id="call-stale",
        local_order_id="call-stale-lo",
        side="CALL",
        score=70,
        trigger=100,
    )
    confirmed_at = datetime.now(timezone.utc).isoformat()
    call["metadata"] = {"trigger_crossed_at": confirmed_at}
    put = signal(
        signal_id="put-stale-replacement",
        local_order_id="put-stale-replacement-lo",
        side="PUT",
        score=95,
        trigger=90,
    )
    call_row = row_for(call)
    call_row["meta"]["trigger_crossed_at"] = confirmed_at
    osm = FakeOSM(
        {
            "call-stale-lo": call_row,
            "put-stale-replacement-lo": row_for(put),
        },
    )
    watcher = AuditWatcher(DummyBroker(), order_state_machine=osm)
    winner = seed(watcher, call, stale=True)

    # A restart must not make an aged confirmed winner eligible for legacy
    # stale/score replacement before durable direction ownership is checked.
    assert winner.trigger_crossed_at is not None
    assert watcher._direction_claims == {}

    assert watcher.add_signal(dict(put)) is False
    assert watcher._last_reject_reason == "direction_claim_active"
    assert osm.cancel_calls == []
    assert watcher.has_order("call-stale-lo") is True
    assert watcher.has_order("put-stale-replacement-lo") is False


def test_unrelated_incomplete_identity_opposite_does_not_hold_winner():
    call = signal(
        signal_id="call",
        local_order_id="call-lo",
        side="CALL",
        score=70,
        trigger=100,
    )
    orphan = signal(
        signal_id="orphan",
        local_order_id="orphan-lo",
        side="PUT",
        score=70,
        trigger=90,
    )
    orphan["ticker"] = "MSFT"
    orphan_row = row_for(orphan)
    orphan.pop("client_id")
    orphan.pop("execution_mode")
    osm = FakeOSM({"call-lo": row_for(call), "orphan-lo": orphan_row})
    watcher = AuditWatcher(DummyBroker(), order_state_machine=osm)
    callbacks = []
    watcher.on_trigger = lambda watched: (
        callbacks.append(watched.signal_id) or {"disposition": "TERMINAL_DURABLE"}
    )

    assert watcher.add_signal(dict(call)) is True
    assert watcher.add_signal(dict(orphan)) is True
    _poll_quote(watcher, bid=98, ask=101)
    _poll_quote(watcher, bid=98, ask=101)

    assert callbacks == ["call"]
    assert osm.cancel_calls == []
    assert watcher.has_order("call-lo") is False
    assert watcher.has_order("orphan-lo") is True


def test_unproven_loser_cancellation_retries_after_deferred_hold():
    call = signal(
        signal_id="call",
        local_order_id="call-lo",
        side="CALL",
        score=70,
        trigger=100,
    )
    put = signal(
        signal_id="put",
        local_order_id="put-lo",
        side="PUT",
        score=70,
        trigger=90,
    )
    osm = FakeOSM(
        {"call-lo": row_for(call), "put-lo": row_for(put)},
        cancel_results={"put-lo": False},
    )
    watcher = AuditWatcher(DummyBroker(), order_state_machine=osm)
    callbacks = []
    watcher.on_trigger = lambda watched: (
        callbacks.append(watched.signal_id) or {"disposition": "TERMINAL_DURABLE"}
    )

    assert watcher.add_signal(dict(call)) is True
    assert watcher.add_signal(dict(put)) is True
    _poll_quote(watcher, bid=98, ask=101)
    _poll_quote(watcher, bid=98, ask=101)
    assert callbacks == []
    assert osm.cancel_calls == [
        ("put-lo", "confirmed_breach_direction_claim_lost")
    ]

    osm.cancel_results["put-lo"] = True
    with watcher._lock:
        for watched in watcher._pending:
            watched.deferred_retry_not_before = None
    _poll_quote(watcher, bid=98, ask=101)

    assert callbacks == ["call"]
    assert osm.cancel_calls == [
        ("put-lo", "confirmed_breach_direction_claim_lost"),
        ("put-lo", "confirmed_breach_direction_claim_lost"),
    ]
    assert osm.rows["put-lo"]["status"] == "CANCELED"
    assert watcher.has_order("put-lo") is False
    assert watcher.has_order("call-lo") is False


def test_recovery_rearm_coarms_healthy_prebreach_opposite_without_cancel():
    old = signal(signal_id="old", local_order_id="old-lo", side="PUT", score=70)
    new = signal(signal_id="new", local_order_id="new-lo", side="CALL", score=95)
    # PR #580 recovery admission requires the exact canonical identity.
    new["canonical_signal_id"] = "new"
    new["__recovery_rearm"] = True
    osm = FakeOSM(
        {"old-lo": row_for(old), "new-lo": row_for(new)},
        cancel_results={"old-lo": False},
    )
    watcher = AuditWatcher(DummyBroker(), order_state_machine=osm)
    watcher._test_only_allow_recovery_without_row_lock = True
    existing = seed(watcher, old)

    assert watcher.add_signal(dict(new)) is True
    assert active_directions(watcher) == {"CALL", "PUT"}
    assert existing in watcher._pending
    assert "old" in watcher._dedup_set
    assert osm.cancel_calls == []


def test_recovery_rearm_still_blocks_on_durable_confirmed_breach():
    old = signal(signal_id="old", local_order_id="old-lo", side="PUT", score=70)
    new = signal(signal_id="new", local_order_id="new-lo", side="CALL", score=60)
    new["__recovery_rearm"] = True
    osm = FakeOSM(
        {"old-lo": row_for(old), "new-lo": row_for(new)},
        cancel_results={"old-lo": False},
    )
    watcher = AuditWatcher(DummyBroker(), order_state_machine=osm)
    existing = seed(watcher, old)
    existing.trigger_crossed_at = "2026-08-28T13:44:55+00:00"

    assert watcher.add_signal(dict(new)) is False
    assert_retained(watcher, existing, old)
    assert "new" not in watcher._dedup_set
    assert osm.cancel_calls == []


def test_recovery_materialization_resume_does_not_coarm_opposite():
    old = signal(signal_id="old", local_order_id="old-lo", side="PUT", score=70)
    new = signal(signal_id="new", local_order_id="new-lo", side="CALL", score=60)
    new["__recovery_rearm"] = True
    new["__materialization_resume"] = True
    osm = FakeOSM(
        {"old-lo": row_for(old), "new-lo": row_for(new)},
        cancel_results={"old-lo": False},
    )
    watcher = AuditWatcher(DummyBroker(), order_state_machine=osm)
    existing = seed(watcher, old)

    assert watcher.add_signal(dict(new)) is False
    assert_retained(watcher, existing, old)
    assert "new" not in watcher._dedup_set
    assert osm.cancel_calls == []
