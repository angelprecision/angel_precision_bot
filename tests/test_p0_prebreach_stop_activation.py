"""
tests/test_p0_prebreach_stop_activation.py

PR #407 — P0: The scanner stop is DORMANT before the first CONFIRMED
entry-trigger breach and ACTIVE after it, including after a process restart.

Invariants under test:
    Before confirmation:
        - trigger_crossed_at is None
        - _pending_first_breach_at records the first breach poll timestamp
        - CALL stop is dormant even if bid <= scanner stop
        - PUT stop is dormant even if ask >= scanner stop
    Confirmation (breach_count reaches MOMENTUM_POLLS_REQUIRED):
        - trigger_crossed_at <- _pending_first_breach_at (FIRST breach time)
        - _pending_first_breach_at is cleared
    Persistence:
        - Before on_trigger runs, meta JSONB carries trigger_crossed_at,
          trigger_confirmed_at, first_breach_bid, first_breach_ask
    After restart via production hydration path:
        - CALL stop authority = bid
        - PUT stop authority = ask
        - Hydration rejects any input that isn't tz-aware datetime/ISO string

Geometry: BAC - trigger 61.90, stop 62.49 (PUT).
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Optional
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import ap_entry_watcher as aew_module
from ap_entry_watcher import (
    APEntryWatcher,
    ET,
    RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN,
    WatchedSignal,
    WatchState,
    recovery_trigger_evidence_identity_is_proven,
    _trigger_crossed_at_provenance_matches,
)
from ap.pending_trigger_restart_recovery import (
    PendingTriggerRestartRecovery,
    _build_plan,
)
from ap_recovery import APStartupRecovery


# -- Local minimal mock OSM --------------------------------------------------
#
# Deliberately lightweight -- this file must not import test infrastructure
# from other test modules, per scope discipline.

class _MockOSM:
    def __init__(self) -> None:
        self._row_status: dict[str, str] = {}
        self._row_meta: dict[str, dict] = {}
        self.meta_writes: list[tuple[str, dict]] = []
        self.cancel_calls: list[tuple[str, str]] = []
        self.expire_calls: list[tuple[str, str]] = []
        self.transition_calls: list[tuple[str, str]] = []

    def _ensure(self, oid: str) -> None:
        if oid not in self._row_status:
            self._row_status[oid] = "PENDING_TRIGGER"
            self._row_meta[oid] = {}

    def get_order(self, local_order_id: str) -> dict:
        self._ensure(local_order_id)
        _meta = dict(self._row_meta.get(local_order_id, {}))
        return {
            "local_order_id": local_order_id,
            "status": self._row_status[local_order_id],
            "meta": _meta,
            "client_id": "client@test.com",
            "execution_mode": "paper",
        }

    def update_order_meta(self, local_order_id: str, meta_patch: dict) -> bool:
        self.meta_writes.append((local_order_id, dict(meta_patch)))
        self._ensure(local_order_id)
        self._row_meta[local_order_id].update(meta_patch)
        return True

    def cancel_pending_entry(self, local_order_id: str, *, reason: str = "") -> bool:
        self.cancel_calls.append((local_order_id, reason))
        self._ensure(local_order_id)
        self._row_status[local_order_id] = "CANCELED"
        return True

    def expire_pending_entry(self, local_order_id: str, *, reason: str = "") -> bool:
        self.expire_calls.append((local_order_id, reason))
        return True

    def transition(self, local_order_id: str, status: str, **_kwargs) -> bool:
        self.transition_calls.append((local_order_id, status))
        return True

    def terminalize_deferred_breach(self, *a, **kw) -> bool:
        return False


# -- Geometry constants ------------------------------------------------------
BAC_PUT_TRIGGER = 61.90
BAC_PUT_STOP = 62.49


def _bac_put_signal(local_order_id: Optional[str] = None,
                    trigger_crossed_at=None) -> dict:
    sig = {
        "signal_id": str(uuid.uuid4()),
        "local_order_id": local_order_id or str(uuid.uuid4()),
        "client_id": "client@test.com",
        "client_email": "client@test.com",
        "execution_mode": "paper",
        "timeframe": "1h",
        "ticker": "BAC",
        "side": "PUT",
        "entry_price": BAC_PUT_TRIGGER,
        "stop_price": BAC_PUT_STOP,
        "target_price": 60.50,
        "contract_symbol": "BAC240101P00062000",
        "contracts": 1,
        "limit_price": 1.20,
        "score": 0.8,
        "tier": "A",
        "queue_status": "QUEUED",
    }
    sig["canonical_signal_id"] = sig["signal_id"]
    sig["metadata"] = {
        "canonical_signal_id": sig["canonical_signal_id"],
        "client_id": sig["client_id"],
        "execution_mode": sig["execution_mode"],
    }
    if trigger_crossed_at is not None:
        sig["trigger_crossed_at"] = trigger_crossed_at
    return sig


def _spy_call_signal(local_order_id: Optional[str] = None) -> dict:
    sig = {
        "signal_id": str(uuid.uuid4()),
        "local_order_id": local_order_id or str(uuid.uuid4()),
        "client_id": "client@test.com",
        "client_email": "client@test.com",
        "execution_mode": "paper",
        "timeframe": "1h",
        "ticker": "SPY",
        "side": "CALL",
        "entry_price": 450.0,
        "stop_price": 447.0,
        "target_price": 455.0,
        "contract_symbol": "SPY240101C00450000",
        "contracts": 1,
        "limit_price": 1.50,
        "score": 0.8,
        "tier": "A",
        "queue_status": "QUEUED",
    }
    sig["canonical_signal_id"] = sig["signal_id"]
    sig["metadata"] = {
        "canonical_signal_id": sig["canonical_signal_id"],
        "client_id": sig["client_id"],
        "execution_mode": sig["execution_mode"],
    }
    return sig


# ===========================================================================
# Class 1 -- Pre-confirmation and confirmation semantics (PUT / BAC geometry)
# ===========================================================================

class TestPreBreachStopActivationPut:
    """BAC PUT: trigger=61.90, stop=62.49."""

    def test_first_breach_stays_pending_and_populates_pending_slot(self):
        """Step 6 (parts 1-2): first qualifying breach poll leaves state
        PENDING, breach_count=1, trigger_crossed_at STILL None (durable
        proof not yet issued), and _pending_first_breach_at holds the
        tz-aware timestamp of THIS poll."""
        sig = _bac_put_signal()
        watched = WatchedSignal(sig, overnight=False)
        watched.entry_trigger = BAC_PUT_TRIGGER
        watched.stop_level = BAC_PUT_STOP

        # PUT breach: bid <= trigger. Ask well above trigger (not relevant
        # to PUT trigger direction).
        state = watched.check(bid=61.85, ask=61.95)

        assert state == WatchState.PENDING
        assert watched.breach_count == 1
        assert watched.trigger_crossed_at is None, (
            "Durable trigger_crossed_at must NOT be issued before "
            "MOMENTUM_POLLS_REQUIRED breaches confirm."
        )
        pending = watched._pending_first_breach_at
        assert isinstance(pending, datetime)
        assert pending.tzinfo is not None, (
            "Pending first-breach timestamp must be tz-aware."
        )

    def test_confirmation_promotes_pending_to_trigger_crossed_at(self):
        """Step 6 (parts 3-4): once breach_count reaches
        MOMENTUM_POLLS_REQUIRED, trigger_crossed_at MUST equal the FIRST
        breach poll timestamp (NOT the confirmation-poll timestamp)."""
        sig = _bac_put_signal()
        watched = WatchedSignal(sig, overnight=False)
        watched.entry_trigger = BAC_PUT_TRIGGER
        watched.stop_level = BAC_PUT_STOP

        # First breach poll -- capture the pending timestamp for later
        # comparison.
        state_first = watched.check(bid=61.85, ask=61.95)
        assert state_first == WatchState.PENDING
        first_poll_ts = watched._pending_first_breach_at
        assert first_poll_ts is not None

        # Feed additional qualifying breach polls until MOMENTUM_POLLS_REQUIRED
        # is reached.  The confirming poll MUST assign trigger_crossed_at
        # to the FIRST poll's timestamp, not this poll's.
        polls_remaining = watched.MOMENTUM_POLLS_REQUIRED - 1
        assert polls_remaining >= 1, (
            "This test presumes MOMENTUM_POLLS_REQUIRED >= 2."
        )
        state = None
        for _ in range(polls_remaining):
            state = watched.check(bid=61.80, ask=61.90)

        assert state == WatchState.TRIGGERED
        assert isinstance(watched.trigger_crossed_at, datetime)
        assert watched.trigger_crossed_at.tzinfo is not None
        assert watched.trigger_crossed_at == first_poll_ts, (
            "trigger_crossed_at must be promoted from the FIRST breach "
            "poll's timestamp, not from the confirmation poll."
        )
        # Pending slot must be cleared post-promotion.
        assert watched._pending_first_breach_at is None

    def test_dispatch_persists_four_meta_keys_before_on_trigger(self):
        """Step 6 (parts 5-6): the existing dispatch path must call
        update_order_meta with all four keys -- trigger_crossed_at,
        trigger_confirmed_at, first_breach_bid, first_breach_ask --
        BEFORE on_trigger fires."""
        osm = _MockOSM()
        broker = MagicMock()
        w = APEntryWatcher(broker, order_state_machine=osm, mode="PAPER")

        # Sequence order between meta persistence and on_trigger call.
        events: list[str] = []
        osm_write_orig = osm.update_order_meta

        def _tracing_meta_write(oid: str, patch: dict) -> bool:
            # Only mark ordering on the trigger-timestamp write; later
            # audit writes are noise for this ordering assertion.
            if "trigger_crossed_at" in patch:
                events.append("meta_write")
            return osm_write_orig(oid, patch)
        osm.update_order_meta = _tracing_meta_write  # type: ignore[assignment]

        def _on_trigger(ws: WatchedSignal):
            events.append("on_trigger")
            return None  # None -> default disposition
        w.on_trigger = _on_trigger

        sig = _bac_put_signal()
        watched = WatchedSignal(sig, overnight=False)
        watched.entry_trigger = BAC_PUT_TRIGGER
        watched.stop_level = BAC_PUT_STOP
        watched._watcher_ref = w
        w._pending.append(watched)
        w._dedup_set.add(sig["signal_id"])

        # Seed one prior breach poll so the next poll confirms.
        prior_ts = datetime.now(timezone.utc)
        watched.breach_count = watched.MOMENTUM_POLLS_REQUIRED - 1
        watched._pending_first_breach_at = prior_ts
        watched.first_breach_bid = 61.85
        watched.first_breach_ask = 61.95
        # The staged partial breach must retain its valid-observation anchor
        # so bounded continuity can prove this poll is a confirmation.
        watched._last_valid_breach_observation_at = prior_ts

        # Drive the confirming poll through the production dispatch path.
        # The quote leaves bid strictly above scanner stop (no collision).
        with patch.object(w, "_fetch_quotes", return_value={
            "BAC": {"bid": 61.80, "ask": 61.90}
        }):
            w._poll_active_signals(open_protect_active=False)

        # This is a real confirmation dispatch, so the callback must be
        # reached exactly once after the timestamp write.
        assert "meta_write" in events, (
            "Dispatch path did not call update_order_meta before on_trigger."
        )
        assert events.count("on_trigger") == 1, (
            "A confirmed watcher must invoke on_trigger exactly once in this poll."
        )
        assert events.index("meta_write") < events.index("on_trigger"), (
            "update_order_meta MUST be persisted before on_trigger."
        )

        # Capture the exact meta patch and assert the four required keys.
        assert osm.meta_writes, "No meta writes recorded."
        ts_writes = [
            patch for (_, patch) in osm.meta_writes
            if "trigger_crossed_at" in patch
        ]
        assert ts_writes, (
            "No update_order_meta call carried trigger_crossed_at."
        )
        first_ts_patch = ts_writes[0]
        for required_key in (
            "trigger_crossed_at",
            "trigger_confirmed_at",
            "first_breach_bid",
            "first_breach_ask",
            "trigger_crossed_at_provenance",
        ):
            assert required_key in first_ts_patch, (
                f"meta patch missing required key: {required_key}"
            )
        assert first_ts_patch["trigger_crossed_at_provenance"] == {
            "canonical_signal_id": sig["canonical_signal_id"],
            "client_id": "client@test.com",
            "execution_mode": "paper",
            "local_order_id": sig["local_order_id"],
        }
        assert "materialization_generation" not in first_ts_patch[
            "trigger_crossed_at_provenance"
        ], "ordinary queue evidence must not invent a generation"

    def test_restart_hydration_reactivates_stop_via_ask(self):
        """Step 6 (parts 7-11): end-to-end restart proof.

        This test does NOT fabricate the persisted metadata.  It drives a
        real dispatch through _poll_active_signals so the production path
        emits update_order_meta(local_order_id, patch); captures the exact
        patch; JSON-round-trips the dict to simulate the Supabase JSONB
        boundary; then reconstructs a second watcher via the production
        hydration entry point (WatchedSignal.__init__).  A post-restart
        quote whose ASK breaks the PUT stop must invalidate the second
        watcher with no selector, broker submit, or broker cancel side
        effects.

        Geometry note: WRONG_DIR_BUFFER_PCT (0.001) requires
        ask >= stop_level * 1.001 to fire.  For stop=62.49, threshold is
        62.5525; test feeds ask=62.60 which clears the buffer while
        preserving the spec's "post-restart quote above trigger, breaks
        PUT stop via ASK" invariant.
        """
        # -- Step A: drive the first watcher through a real confirmation
        #    dispatch so update_order_meta captures the production metadata.
        first_osm = _MockOSM()
        broker = MagicMock()
        selector = MagicMock()
        w1 = APEntryWatcher(broker, order_state_machine=first_osm, mode="PAPER")

        first_local_oid = str(uuid.uuid4())
        first_sig = _bac_put_signal(local_order_id=first_local_oid)
        first_watched = WatchedSignal(first_sig, overnight=False)
        first_watched.entry_trigger = BAC_PUT_TRIGGER
        first_watched.stop_level = BAC_PUT_STOP
        first_watched._watcher_ref = w1
        w1._pending.append(first_watched)
        w1._dedup_set.add(first_sig["signal_id"])

        # on_trigger records that dispatch reached it (i.e., production path
        # ran end-to-end); it does not call selector or broker.
        on_trigger_calls: list[str] = []
        def _on_trigger(ws: WatchedSignal):
            on_trigger_calls.append(ws.signal_id)
            return None
        w1.on_trigger = _on_trigger

        # First qualifying breach — bid below trigger; ask well above the
        # buffered stop so no collision.  Leaves the watcher PENDING with
        # breach_count == 1 through the public check() path.
        with patch.object(w1, "_fetch_quotes", return_value={
            "BAC": {"bid": 61.85, "ask": 61.95}
        }):
            w1._poll_active_signals(open_protect_active=False)
        assert first_watched.state == WatchState.PENDING
        assert first_watched.breach_count == 1
        assert first_watched.trigger_crossed_at is None

        # Confirming poll — bid still below trigger, ask still below the
        # buffered stop.  Promotes _pending_first_breach_at into
        # trigger_crossed_at and drives update_order_meta.
        with patch.object(w1, "_fetch_quotes", return_value={
            "BAC": {"bid": 61.80, "ask": 61.90}
        }):
            w1._poll_active_signals(open_protect_active=False)

        # -- Step B: extract the exact meta patch persisted before on_trigger.
        ts_writes = [
            patch for (oid, patch) in first_osm.meta_writes
            if oid == first_local_oid and "trigger_crossed_at" in patch
        ]
        assert ts_writes, (
            "Production dispatch did not persist trigger metadata "
            "before on_trigger."
        )
        captured_meta = ts_writes[0]
        for required_key in (
            "trigger_crossed_at",
            "trigger_confirmed_at",
            "first_breach_bid",
            "first_breach_ask",
        ):
            assert required_key in captured_meta, (
                f"Captured meta missing production key: {required_key}"
            )

        # -- Step C: JSONB round-trip simulates the Supabase boundary on
        #    the captured production metadata (not fabricated data).
        round_tripped_meta = json.loads(json.dumps(captured_meta))
        assert round_tripped_meta == captured_meta

        # -- Step D: reconstruct a second watcher through the production
        #    hydration entry point (WatchedSignal.__init__).  The signal
        #    carries the round-tripped trigger_crossed_at exactly as a
        #    restart-recovery module would read it from orders.meta.
        second_sig = _bac_put_signal(
            trigger_crossed_at=round_tripped_meta["trigger_crossed_at"]
        )
        second_watcher = WatchedSignal(second_sig, overnight=False)
        second_watcher.entry_trigger = BAC_PUT_TRIGGER
        second_watcher.stop_level = BAC_PUT_STOP

        assert isinstance(second_watcher.trigger_crossed_at, datetime)
        assert second_watcher.trigger_crossed_at.tzinfo is not None

        # -- Step E: post-restart quote breaks the PUT stop via ASK.
        state = second_watcher.check(bid=62.60, ask=62.60)

        assert state == WatchState.INVALIDATED, (
            "Post-restart quote whose ask breaks the PUT stop must "
            "invalidate given hydrated (round-tripped) trigger_crossed_at."
        )
        # No selector work, no broker submit, no broker cancel.  The
        # second watcher never had a broker order; check() cannot reach
        # the dispatch loop's broker code from a standalone WatchedSignal.
        assert selector.mock_calls == [], (
            "Selector must not be called during stop invalidation."
        )
        # The dispatch path may read broker attributes for gating (e.g.
        # broker.live_access_token during LIVE-mode checks); those reads
        # are not the invariant under test.  What must NOT happen is any
        # submit or cancel call originating from the invalidation path.
        forbidden_broker_methods = {
            "submit_order",
            "submit_option_order",
            "cancel_order",
            "cancel_option_order",
        }
        for call_ in broker.mock_calls:
            called_name = str(call_).split("(", 1)[0]
            for forbidden in forbidden_broker_methods:
                assert forbidden not in called_name, (
                    f"Broker.{forbidden} must not be called during "
                    f"pre-broker-order invalidation; observed {call_}"
                )


    def test_prebreach_ask_touch_leaves_stop_dormant(self):
        """Explicit dormancy check: with NO prior trigger evidence and
        NO breach in this poll, an ask above the PUT stop must NOT
        invalidate.  Guarantees clause (e) of the invariant."""
        sig = _bac_put_signal()
        watched = WatchedSignal(sig, overnight=False)
        watched.entry_trigger = BAC_PUT_TRIGGER
        watched.stop_level = BAC_PUT_STOP

        # Bid well above trigger (no breach). Ask breaks stop-level with
        # buffer clearance.
        state = watched.check(bid=62.60, ask=62.62)

        assert state == WatchState.PENDING, (
            "Pre-breach PUT stop must be dormant even if ask >= scanner stop."
        )
        assert watched.trigger_crossed_at is None
        assert watched._pending_first_breach_at is None

    def test_prebreach_original_defect_geometry_stays_pending(self):
        """Original defect proof at the exact spec geometry:
        BAC PUT, trigger=61.90, stop=62.49, quote bid=62.45/ask=62.49.

        Before PR #407, a single trigger-side or stop-side poll could
        activate scanner-stop invalidation with no confirmed breach on
        record.  With PR #407 the invariant is: no confirmed breach ->
        stop is dormant.  This test drives the exact spec quote through
        the public check() path and asserts the setup remains PENDING.
        The WRONG_DIR_BUFFER_PCT (0.001) is intentionally untouched;
        the value of this test is the defect proof at the exact numbers.
        """
        sig = _bac_put_signal()
        watched = WatchedSignal(sig, overnight=False)
        watched.entry_trigger = BAC_PUT_TRIGGER   # 61.90
        watched.stop_level = BAC_PUT_STOP         # 62.49

        # The exact BAC quote from the original spec.  No breach (bid
        # above trigger); no confirmed evidence.  Stop MUST be dormant.
        state = watched.check(bid=62.45, ask=62.49)

        assert state == WatchState.PENDING, (
            "Original defect: pre-confirmed BAC PUT at bid=62.45/ask=62.49 "
            f"must remain PENDING; got {state}."
        )
        assert watched.trigger_crossed_at is None
        assert watched._pending_first_breach_at is None
        assert watched.breach_count == 0


@pytest.mark.parametrize(
    ("side", "signal_factory", "safe_quote", "broken_quote"),
    [
        (
            "CALL",
            _spy_call_signal,
            {"SPY": {"bid": 449.0, "ask": 451.0}},
            {"SPY": {"bid": 446.0, "ask": 451.0}},
        ),
        (
            "PUT",
            _bac_put_signal,
            {"BAC": {"bid": 61.80, "ask": 61.90}},
            {"BAC": {"bid": 61.80, "ask": 62.60}},
        ),
    ],
)
@pytest.mark.parametrize("retry_path", ["callback_exception", "keep_watcher"])
def test_confirmed_evidence_survives_real_poll_retry_and_active_stop(
    side, signal_factory, safe_quote, broken_quote, retry_path
):
    """A real poll confirmation must survive both existing retry branches.

    The second poll invokes the callback and returns the watcher to PENDING
    while retaining breach_count.  The next qualifying poll is deliberately
    a broken active scanner stop: it must terminalize the watcher without
    erasing the original confirmed timestamp or invoking on_trigger again.
    """
    osm = _MockOSM()
    watcher = APEntryWatcher(MagicMock(), order_state_machine=osm, mode="LIVE")
    signal = signal_factory()
    watched = WatchedSignal(signal, overnight=False)
    if side == "CALL":
        watched.entry_trigger = 450.0
        watched.stop_level = 447.0
    else:
        watched.entry_trigger = BAC_PUT_TRIGGER
        watched.stop_level = BAC_PUT_STOP
    watched._watcher_ref = watcher
    watcher._pending.append(watched)
    watcher._dedup_set.add(signal["signal_id"])
    watcher._persist_watcher_audit = lambda *args, **kwargs: None

    callback_calls: list[str] = []

    def _on_trigger(_watched):
        callback_calls.append(side)
        if retry_path == "callback_exception":
            raise RuntimeError("simulated transient callback failure")
        return {"disposition": "KEEP_WATCHER", "retry_after_seconds": 1}

    watcher.on_trigger = _on_trigger
    quote_sequence = [safe_quote, safe_quote, broken_quote]

    def _fetch_quotes(_tickers):
        return quote_sequence.pop(0)

    watcher._fetch_quotes = _fetch_quotes

    # First breach: real _poll_active_signals -> real WatchedSignal.check().
    watcher._poll_active_signals(open_protect_active=False)
    assert watched.breach_count == 1
    assert watched._pending_first_breach_at is not None
    assert watched.trigger_crossed_at is None

    # Confirmation: callback retry/KEEP_WATCHER resets only state, not the
    # already confirmed evidence or the momentum count.
    watcher._poll_active_signals(open_protect_active=False)
    original_confirmed_at = watched.trigger_crossed_at
    preserved_breach_count = watched.breach_count
    assert original_confirmed_at is not None
    assert watched._pending_first_breach_at is None
    assert watched.state == WatchState.PENDING
    assert preserved_breach_count >= watched.MOMENTUM_POLLS_REQUIRED
    assert len(callback_calls) == 1

    # Simulate process restart from the exact orders.meta JSON boundary.  The
    # callback exception and KEEP_WATCHER paths both leave a durable timestamp;
    # the newly hydrated watcher must retain active-stop authority.
    persisted_signal = dict(signal)
    persisted_signal["metadata"] = json.loads(
        json.dumps(osm._row_meta[signal["local_order_id"]])
    )
    restarted = WatchedSignal(persisted_signal, overnight=False)
    if side == "CALL":
        restarted.entry_trigger = 450.0
        restarted.stop_level = 447.0
    else:
        restarted.entry_trigger = BAC_PUT_TRIGGER
        restarted.stop_level = BAC_PUT_STOP
    restarted._watcher_ref = watcher
    watcher._pending = [restarted]
    watcher._dedup_set = {signal["signal_id"]}

    # Make the retry due immediately; this models the scheduler's next due
    # poll without waiting for the wall-clock retry interval.
    restarted.deferred_retry_not_before = None
    watcher._poll_active_signals(open_protect_active=False)

    assert restarted.state == WatchState.INVALIDATED, (
        f"{side} active scanner stop must terminalize after confirmation"
    )
    assert restarted.trigger_crossed_at == original_confirmed_at, (
        f"{side} retry must not erase or replace confirmed trigger evidence"
    )
    assert restarted.trigger_crossed_at is not None
    assert len(callback_calls) == 1, (
        "A stop terminalization after a callback retry must not invoke "
        "on_trigger a second time."
    )


@pytest.mark.parametrize("deferred", [False, True], ids=["ordinary-live", "deferred-live"])
def test_live_timestamp_persistence_retry_requires_durable_write_before_callback(
    deferred,
):
    """Every LIVE watcher must persist trigger evidence before callback work.

    The ordinary queue path is intentionally included alongside the deferred
    path: the persistence fence is a LIVE safety invariant, not a deferred-only
    special case.  A later successful write permits exactly one KEEP_WATCHER
    callback, after which a broken active stop terminalizes without a second
    callback.
    """
    osm = _MockOSM()
    watcher = APEntryWatcher(MagicMock(), order_state_machine=osm, mode="LIVE")
    signal = _bac_put_signal()
    if deferred:
        signal["contract_symbol"] = "DEFERRED:BAC240101P00062000"
        signal["contract_deferred"] = True
    watched = WatchedSignal(signal, overnight=False)
    watched.entry_trigger = BAC_PUT_TRIGGER
    watched.stop_level = BAC_PUT_STOP
    watched._watcher_ref = watcher
    watcher._pending.append(watched)
    watcher._dedup_set.add(signal["signal_id"])
    watcher._persist_watcher_audit = lambda *args, **kwargs: None
    watcher._is_live_runtime = lambda: True

    persist_attempts: list[dict] = []
    fail_persistence = True

    def _meta_write(oid, patch):
        nonlocal fail_persistence
        osm.meta_writes.append((oid, dict(patch)))
        persist_attempts.append(dict(patch))
        if fail_persistence:
            return False
        return True

    osm.update_order_meta = _meta_write  # type: ignore[assignment]
    callback_calls: list[str] = []

    def _on_trigger(_watched):
        callback_calls.append("called")
        return {"disposition": "KEEP_WATCHER", "retry_after_seconds": 1}

    watcher.on_trigger = _on_trigger
    quote_sequence = [
        {"BAC": {"bid": 61.85, "ask": 61.95}},
        {"BAC": {"bid": 61.80, "ask": 61.90}},
        {"BAC": {"bid": 61.80, "ask": 61.90}},
        {"BAC": {"bid": 61.80, "ask": 62.60}},
    ]
    watcher._fetch_quotes = lambda _tickers: quote_sequence.pop(0)

    watcher._poll_active_signals(open_protect_active=False)
    assert watched._pending_first_breach_at is not None
    assert watched.trigger_crossed_at is None

    watcher._poll_active_signals(open_protect_active=False)
    original_confirmed_at = watched.trigger_crossed_at
    preserved_breach_count = watched.breach_count
    assert original_confirmed_at is not None
    assert watched.state == WatchState.PENDING
    assert watched.deferred_retry_not_before is not None
    assert preserved_breach_count >= watched.MOMENTUM_POLLS_REQUIRED
    assert callback_calls == [], (
        "LIVE timestamp persistence failure must not enter callback work."
    )

    # The next due poll gets a successful durable write.  Callback work may
    # begin only after that write and must run once.
    fail_persistence = False
    watched.deferred_retry_not_before = None
    watcher._poll_active_signals(open_protect_active=False)
    assert len(callback_calls) == 1
    assert watched.state == WatchState.PENDING
    assert watched.trigger_crossed_at == original_confirmed_at
    assert len(persist_attempts) >= 2

    # A later qualifying poll with a broken active stop must terminalize and
    # must not invoke the callback a second time.
    watched.deferred_retry_not_before = None
    watcher._fetch_quotes = lambda _tickers: {
        "BAC": {"bid": 61.80, "ask": 62.60}
    }
    watcher._poll_active_signals(open_protect_active=False)

    assert watched.state == WatchState.INVALIDATED
    assert watched.trigger_crossed_at == original_confirmed_at
    assert watched.trigger_crossed_at is not None
    assert callback_calls == ["called"]


def _restart_row_with_confirmed_evidence() -> dict:
    local_order_id = "lo-restart-407"
    canonical_signal_id = "canonical-restart-407"
    crossed_at = "2026-08-03T16:00:00+00:00"
    provenance = {
        "canonical_signal_id": canonical_signal_id,
        "client_id": "client@test.com",
        "execution_mode": "paper",
        "local_order_id": local_order_id,
    }
    return {
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "submitted_ts": None,
        "local_order_id": local_order_id,
        "signal_id": "signal-restart-407",
        "canonical_signal_id": canonical_signal_id,
        "client_id": "client@test.com",
        "execution_mode": "paper",
        "symbol": "BAC",
        "side": "PUT",
        "trigger_price": BAC_PUT_TRIGGER,
        "stop_underlying": BAC_PUT_STOP,
        "target_underlying": 60.50,
        "contract": "BAC240101P00062000",
        "qty": 1,
        "limit_price": 1.20,
        "meta": {
            "signal_id": "signal-restart-407",
            "canonical_signal_id": canonical_signal_id,
            "client_id": "client@test.com",
            "execution_mode": "paper",
            "ticker": "BAC",
            "side": "PUT",
            "signal_entry_price": BAC_PUT_TRIGGER,
            "stop_underlying": BAC_PUT_STOP,
            "target_underlying": 60.50,
            "selected_contract": "BAC240101P00062000",
            "selected_qty": 1,
            "selected_limit": 1.20,
            "materialization_generation": 7,
            "trigger_crossed_at": crossed_at,
            "trigger_crossed_at_provenance": provenance,
        },
    }


def test_restart_plan_json_roundtrip_reaches_watcher_with_active_stop():
    """Production recovery builders must pass an attribute plan into watch()."""
    persisted_row = _restart_row_with_confirmed_evidence()
    row_after_json = json.loads(json.dumps(persisted_row))

    # Exercise both generic and startup recovery builders.  The generic
    # builder is the formerly-dict-shaped path; startup recovery is the
    # ap_recovery -> PendingTriggerRestartRecovery -> watch() path.
    generic_plan = _build_plan(row_after_json)
    assert isinstance(generic_plan, SimpleNamespace)
    assert generic_plan.signal_id == "signal-restart-407"
    assert generic_plan.canonical_signal_id == "canonical-restart-407"
    assert generic_plan.entry_trigger == BAC_PUT_TRIGGER
    assert generic_plan.stop_underlying == BAC_PUT_STOP
    assert generic_plan.target_underlying == 60.50
    assert generic_plan.materialization_generation == 7
    assert generic_plan.trigger_crossed_at == "2026-08-03T16:00:00+00:00"
    assert isinstance(generic_plan.metadata, dict)

    startup_recovery = APStartupRecovery.__new__(APStartupRecovery)
    startup_recovery.client_id = "client@test.com"
    production_plan = startup_recovery._build_recovery_plan_from_order(
        row_after_json
    )
    assert isinstance(production_plan, SimpleNamespace)

    osm = _MockOSM()
    osm._row_status[persisted_row["local_order_id"]] = "PENDING_TRIGGER"
    osm._row_meta[persisted_row["local_order_id"]] = dict(
        row_after_json["meta"]
    )
    watcher = APEntryWatcher(MagicMock(), order_state_machine=osm, mode="PAPER")
    watcher._persist_watcher_audit = lambda *args, **kwargs: None

    # Keep the test independent of wall-clock market/session state while
    # using the real recovery classifier and real add_signal/watch path.
    with patch.object(watcher, "_is_regular_session_now", return_value=False), \
         patch.object(watcher, "_is_past_entry_cutoff_now", return_value=False), \
         patch.object(watcher, "_get_quote", return_value={"bid": 62.45, "ask": 62.49}):
        assert watcher.watch(
            production_plan,
            persisted_row["local_order_id"],
            recovery_rearm=True,
        ) is True

    assert len(watcher._pending) == 1
    armed = watcher._pending[0]
    assert armed.signal["ticker"] == "BAC"
    assert armed.signal["side"] == "PUT"
    assert armed.entry_trigger == BAC_PUT_TRIGGER
    assert armed.stop_level == BAC_PUT_STOP
    assert armed.signal["client_id"] == "client@test.com"
    assert armed.signal["execution_mode"] == "paper"
    assert armed.signal["local_order_id"] == "lo-restart-407"
    assert armed.signal["canonical_signal_id"] == "canonical-restart-407"
    assert armed.signal["materialization_generation"] == 7
    assert armed.trigger_crossed_at == datetime.fromisoformat(
        "2026-08-03T16:00:00+00:00"
    )

    # The recovered confirmed evidence must make the scanner stop live.
    assert armed.check(bid=62.60, ask=62.60) == WatchState.INVALIDATED


@pytest.mark.parametrize(
    ("field", "bad_value"),
    [
        pytest.param("canonical_signal_id", "stale-canonical", id="wrong-canonical"),
        pytest.param("client_id", "other@test.com", id="wrong-client"),
        pytest.param("execution_mode", "live", id="wrong-mode"),
        pytest.param("local_order_id", "stale-order", id="wrong-order"),
    ],
)
def test_recovery_trigger_evidence_rejects_mismatched_lifecycle_identity(
    field, bad_value
):
    row = json.loads(json.dumps(_restart_row_with_confirmed_evidence()))
    row["meta"]["trigger_crossed_at_provenance"][field] = bad_value
    signal = {
        "signal_id": row["signal_id"],
        "canonical_signal_id": row["canonical_signal_id"],
        "client_id": row["client_id"],
        "execution_mode": row["execution_mode"],
        "local_order_id": row["local_order_id"],
    }
    provenance = dict(row["meta"]["trigger_crossed_at_provenance"])

    assert not _trigger_crossed_at_provenance_matches(
        provenance, signal, row["local_order_id"]
    ), f"mismatched {field} must fail closed"

    startup_recovery = APStartupRecovery.__new__(APStartupRecovery)
    startup_recovery.client_id = "client@test.com"
    production_plan = startup_recovery._build_recovery_plan_from_order(row)
    assert isinstance(production_plan, SimpleNamespace)
    osm = _MockOSM()
    osm._row_status[row["local_order_id"]] = "PENDING_TRIGGER"
    osm._row_meta[row["local_order_id"]] = dict(row["meta"])
    watcher = APEntryWatcher(MagicMock(), order_state_machine=osm, mode="PAPER")
    watcher._persist_watcher_audit = lambda *args, **kwargs: None
    with patch.object(watcher, "_is_regular_session_now", return_value=False), \
         patch.object(watcher, "_is_past_entry_cutoff_now", return_value=False), \
         patch.object(watcher, "_get_quote", return_value={"bid": 62.45, "ask": 62.49}):
        assert watcher.watch(
            production_plan, row["local_order_id"], recovery_rearm=True
        ) is False
    assert watcher._last_reject_reason == RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN
    assert watcher._pending == [], f"mismatched {field} must not register a watcher"
    assert osm.meta_writes == [], f"mismatched {field} must not write order metadata"
    assert osm.cancel_calls == [], f"mismatched {field} must not cancel the order"
    assert osm.expire_calls == [], f"mismatched {field} must not expire the order"
    assert osm.transition_calls == [], f"mismatched {field} must not transition the order"


def test_recovery_trigger_evidence_rejects_incomplete_provenance():
    for missing_field in (
        "canonical_signal_id", "client_id", "execution_mode", "local_order_id"
    ):
        row = _restart_row_with_confirmed_evidence()
        incomplete = dict(row["meta"]["trigger_crossed_at_provenance"])
        incomplete.pop(missing_field)
        row["meta"]["trigger_crossed_at_provenance"] = incomplete
        startup_recovery = APStartupRecovery.__new__(APStartupRecovery)
        startup_recovery.client_id = "client@test.com"
        production_plan = startup_recovery._build_recovery_plan_from_order(row)
        osm = _MockOSM()
        osm._row_status[row["local_order_id"]] = "PENDING_TRIGGER"
        osm._row_meta[row["local_order_id"]] = dict(row["meta"])
        watcher = APEntryWatcher(MagicMock(), order_state_machine=osm, mode="PAPER")
        with patch.object(watcher, "_is_regular_session_now", return_value=False), \
             patch.object(watcher, "_is_past_entry_cutoff_now", return_value=False), \
             patch.object(watcher, "_get_quote", return_value={"bid": 62.45, "ask": 62.49}):
            assert watcher.watch(
                production_plan, row["local_order_id"], recovery_rearm=True
            ) is False
        assert watcher._last_reject_reason == RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN
        assert watcher._pending == []
        assert osm.meta_writes == []
        assert osm.cancel_calls == []


def test_legacy_confirmed_row_is_blocked_until_rollout_backfill():
    """Pre-#407 rows stay fail-closed until the data migration binds them."""
    row = json.loads(json.dumps(_restart_row_with_confirmed_evidence()))
    row["meta"].pop("trigger_crossed_at_provenance")

    assert not recovery_trigger_evidence_identity_is_proven(
        row, row["local_order_id"]
    )

    osm = _MockOSM()
    watcher = MagicMock()
    quote_check = MagicMock(return_value=False)
    recovery = PendingTriggerRestartRecovery(
        client_id="client@test.com",
        execution_mode="paper",
        osm=osm,
        entry_watcher=watcher,
        broker=MagicMock(),
        quote_check_fn=quote_check,
    )

    outcome = recovery.recover_one_row(row)
    assert outcome == "UNRESOLVED"
    assert recovery._row_failure_reasons[row["local_order_id"]] == (
        RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN
    )
    quote_check.assert_not_called()
    watcher.watch.assert_not_called()
    assert osm.meta_writes == []
    assert osm.cancel_calls == []
    assert osm.expire_calls == []
    assert osm.transition_calls == []

    # This is the exact four-field value produced by the rollout migration
    # when the durable row identity is complete and internally consistent.
    row["meta"]["trigger_crossed_at_provenance"] = {
        "canonical_signal_id": row["canonical_signal_id"],
        "client_id": row["client_id"],
        "execution_mode": row["execution_mode"],
        "local_order_id": row["local_order_id"],
    }
    assert recovery_trigger_evidence_identity_is_proven(
        row, row["local_order_id"]
    )


def test_trigger_provenance_rollout_migration_is_bounded_and_generation_free():
    """The migration is the explicit pre-deploy repair, not a new runtime path."""
    from pathlib import Path

    migration = Path(__file__).resolve().parents[1] / (
        "migrations/20260804_trigger_crossed_at_provenance_backfill.sql"
    )
    sql = migration.read_text(encoding="utf-8")
    normalized = " ".join(sql.lower().split())

    assert "update orders as o" in normalized
    assert "o.meta ? 'trigger_crossed_at'" in normalized
    assert "not (o.meta ? 'trigger_crossed_at_provenance')" in normalized
    for field in (
        "canonical_signal_id",
        "client_id",
        "execution_mode",
        "local_order_id",
    ):
        assert field in normalized
    assert "does not add a column" in normalized
    assert "delete from orders" not in normalized
    update_body = normalized.split("update orders as o", 1)[1].split(";", 1)[0]
    assert "broker" not in update_body
    assert "materialization_generation" not in update_body


def test_trigger_provenance_rollout_migration_executes_against_postgres_rows():
    """Execute the shipped backfill against production-shaped JSONB rows."""
    import os

    psycopg2 = pytest.importorskip("psycopg2")
    dsn = next(
        (
            os.getenv(name)
            for name in (
                "DATABASE_URL",
                "DATABASE_URI",
                "POSTGRES_URL",
                "SUPABASE_DB_URL",
            )
            if os.getenv(name)
        ),
        None,
    )
    if not dsn:
        pytest.skip("no PostgreSQL DSN configured for migration integration test")

    try:
        connection = psycopg2.connect(dsn)
    except psycopg2.OperationalError as exc:
        if os.getenv("GITHUB_ACTIONS", "").strip().lower() == "true":
            raise
        pytest.skip(f"PostgreSQL unavailable for migration integration test: {exc}")

    from pathlib import Path

    migration = Path(__file__).resolve().parents[1] / (
        "migrations/20260804_trigger_crossed_at_provenance_backfill.sql"
    )
    sql = migration.read_text(encoding="utf-8")
    crossed_at = "2026-08-03T16:00:00+00:00"
    reeval_signal = (
        "REEVAL:603bce5b-352e-44e0-b00b-6baa826ca2c9:f4dc44"
    )

    rows = [
        (
            "lo-eligible-407",
            "client@test.com",
            "paper",
            "signal-eligible-407",
            "signal-eligible-407",
            "ENTRY",
            {
                "trigger_crossed_at": crossed_at,
                "canonical_signal_id": "signal-eligible-407",
            },
        ),
        (
            "lo-reeval-407",
            "client@test.com",
            "paper",
            reeval_signal,
            "REEVAL:603bce5b-352e-44e0-b00b-6baa826ca2c9",
            "ENTRY",
            {"trigger_crossed_at": crossed_at},
        ),
        (
            "lo-conflicting-canonical-407",
            "client@test.com",
            "paper",
            "signal-conflicting-canonical-407",
            "stale-canonical-407",
            "ENTRY",
            {"trigger_crossed_at": crossed_at},
        ),
        (
            "lo-conflicting-client-407",
            "client@test.com",
            "paper",
            "signal-conflicting-client-407",
            "signal-conflicting-client-407",
            "ENTRY",
            {
                "trigger_crossed_at": crossed_at,
                "client_id": "other@test.com",
            },
        ),
        (
            "lo-existing-provenance-407",
            "client@test.com",
            "paper",
            "signal-existing-407",
            "signal-existing-407",
            "ENTRY",
            {
                "trigger_crossed_at": crossed_at,
                "trigger_crossed_at_provenance": {
                    "canonical_signal_id": "preserve-existing-407",
                    "client_id": "client@test.com",
                    "execution_mode": "paper",
                    "local_order_id": "lo-existing-provenance-407",
                },
            },
        ),
        (
            "lo-incomplete-mode-407",
            "client@test.com",
            "",
            "signal-incomplete-mode-407",
            "signal-incomplete-mode-407",
            "ENTRY",
            {"trigger_crossed_at": crossed_at},
        ),
    ]

    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TEMP TABLE orders (
                        local_order_id TEXT PRIMARY KEY,
                        client_id TEXT,
                        execution_mode TEXT,
                        signal_id TEXT,
                        canonical_signal_id TEXT,
                        kind TEXT,
                        meta JSONB
                    )
                    """
                )
                for row in rows:
                    cursor.execute(
                        """
                        INSERT INTO orders (
                            local_order_id, client_id, execution_mode,
                            signal_id, canonical_signal_id, kind, meta
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                        """,
                        (*row[:6], json.dumps(row[6])),
                    )

                cursor.execute(sql)
                cursor.execute(
                    "SELECT local_order_id, meta::text FROM orders "
                    "ORDER BY local_order_id"
                )
                first_pass = {
                    local_order_id: json.loads(meta_text)
                    for local_order_id, meta_text in cursor.fetchall()
                }

                assert first_pass["lo-eligible-407"][
                    "trigger_crossed_at_provenance"
                ] == {
                    "canonical_signal_id": "signal-eligible-407",
                    "client_id": "client@test.com",
                    "execution_mode": "paper",
                    "local_order_id": "lo-eligible-407",
                }
                assert first_pass["lo-reeval-407"][
                    "trigger_crossed_at_provenance"
                ]["canonical_signal_id"] == (
                    "REEVAL:603bce5b-352e-44e0-b00b-6baa826ca2c9"
                )
                for refused_id in (
                    "lo-conflicting-canonical-407",
                    "lo-conflicting-client-407",
                    "lo-incomplete-mode-407",
                ):
                    assert "trigger_crossed_at_provenance" not in first_pass[refused_id]
                assert first_pass["lo-existing-provenance-407"][
                    "trigger_crossed_at_provenance"
                ] == rows[4][6]["trigger_crossed_at_provenance"]

                cursor.execute(sql)
                cursor.execute(
                    "SELECT local_order_id, meta::text FROM orders "
                    "ORDER BY local_order_id"
                )
                second_pass = {
                    local_order_id: json.loads(meta_text)
                    for local_order_id, meta_text in cursor.fetchall()
                }
                assert second_pass == first_pass, "backfill must be idempotent"
    finally:
        connection.close()


def test_ordinary_queue_evidence_identity_does_not_require_generation():
    """Ordinary queue plans have no durable materialization generation.

    This is the production contract audit result: generation remains available
    to deferred materialization CAS, but it is not fabricated or required for
    ordinary confirmed-trigger evidence.
    """
    signal = _spy_call_signal(local_order_id="lo-ordinary-queue-407")
    assert "materialization_generation" not in signal
    provenance = {
        "canonical_signal_id": signal["canonical_signal_id"],
        "client_id": signal["client_id"],
        "execution_mode": signal["execution_mode"],
        "local_order_id": signal["local_order_id"],
    }
    signal["trigger_crossed_at"] = "2026-08-03T16:00:00+00:00"
    signal["metadata"]["trigger_crossed_at_provenance"] = provenance
    assert recovery_trigger_evidence_identity_is_proven(
        signal, signal["local_order_id"]
    )
    persisted_row = {
        "local_order_id": signal["local_order_id"],
        "meta": {
            "canonical_signal_id": signal["canonical_signal_id"],
            "client_id": signal["client_id"],
            "execution_mode": signal["execution_mode"],
            "trigger_crossed_at": signal["trigger_crossed_at"],
            "trigger_crossed_at_provenance": provenance,
        },
    }
    assert recovery_trigger_evidence_identity_is_proven(
        persisted_row, signal["local_order_id"]
    )


def test_ordinary_queue_dispatch_passes_real_plan_without_generation(monkeypatch):
    """The real queue arm seam does not inject a fake generation.

    This uses the production ``ApprovedExecutionPlan`` and production
    ``ap.queue._dispatch`` call chain, capturing the exact plan handed to
    ``entry_watcher.watch``.  Deferred materialization generation remains a
    separate lifecycle field and is absent here by contract.
    """
    import os

    os.environ.setdefault("DATABASE_URL", "postgresql://test:test@127.0.0.1:5432/test")
    import ap.queue as queue
    import ap.authorization as authorization
    from ap_master_control import ApprovedExecutionPlan

    plan = ApprovedExecutionPlan(
        plan_id="plan-ordinary-407",
        signal_id="signal-ordinary-407",
        client_id="client@test.com",
        ticker="SPY",
        side="CALL",
        direction="CALL",
        pattern="3-2U",
        timeframe="1h",
        contracts=1,
        max_position_usd=150.0,
        tier="A",
        score=80.0,
        intel_score=80.0,
        confidence_bucket="standard_pool",
        trigger_type="breach",
        trigger_price=450.0,
        stop_underlying=447.0,
        target_underlying=455.0,
        contract_symbol="SPY240101C00450000",
        limit_price=1.50,
        mode="PAPER",
        metadata={
            "canonical_signal_id": "canonical-ordinary-407",
            "client_id": "client@test.com",
            "execution_mode": "paper",
        },
    )

    class _QueueMC:
        mode = "PAPER"
        _equity_cache_ts = 10**12

        def evaluate(self, _payload, client_id=None):
            return SimpleNamespace(ok=True, stage="approved", reason="", plan=plan)

        def revalidate_exposure(self, _plan, client_id=None):
            return SimpleNamespace(ok=True, reason="")

    selector = MagicMock()
    selector.select.return_value = SimpleNamespace(
        contract_symbol=plan.contract_symbol,
        mid=plan.limit_price,
        candidate_audit=None,
    )
    osm = MagicMock()
    osm.create_entry_order.return_value = "lo-ordinary-queue-407"
    osm.mark_entry_pending_trigger.return_value = True
    watcher = MagicMock()
    watcher.watch.return_value = True

    monkeypatch.setattr(queue, "_mark_job", MagicMock())
    monkeypatch.setattr(queue, "_log_signal_to_db", MagicMock(return_value=True))
    monkeypatch.setattr(queue, "trace_gate", MagicMock())
    monkeypatch.setattr(
        authorization, "execution_mode_for_broker", lambda _broker: "paper"
    )
    monkeypatch.setattr(
        queue,
        "_now_et",
        lambda: datetime(2026, 8, 3, 10, 0, tzinfo=timezone.utc),
    )
    monkeypatch.setattr(queue, "_is_regular_session_et", lambda _now: True)

    queue._dispatch(
        40701,
        "client@test.com",
        "signal-ordinary-407",
        {
            "signal_id": "signal-ordinary-407",
            "ticker": "SPY",
            "side": "CALL",
            "score": 80.0,
            "timeframe": "1h",
            "execution_mode": "paper",
        },
        master_control=_QueueMC(),
        contract_selector=selector,
        order_state_machine=osm,
        entry_watcher=watcher,
        broker=MagicMock(),
    )

    osm.create_entry_order.assert_called_once()
    watcher.watch.assert_called_once()
    passed_plan = watcher.watch.call_args.kwargs["plan"]
    assert passed_plan is plan
    assert not hasattr(passed_plan, "materialization_generation")
    assert "materialization_generation" not in passed_plan.metadata


def test_queue_and_rescue_plans_share_canonical_id_authority_without_generation():
    """Real queue/rescue plan shapes normalize REEVAL IDs at watcher ingress."""
    from ap_canonical_signal import build_canonical_signal_id
    from ap_armed_deferred_rescue import _plan_from_payload
    from ap_master_control import ApprovedExecutionPlan

    raw_queue_id = "REEVAL:603bce5b-352e-44e0-b00b-6baa826ca2c9:f4dc44"
    raw_rescue_id = "REEVAL:8d9338d0-5dde-4b7b-81ea-208039999b72:a1b2c3"
    expected_queue_id = build_canonical_signal_id(raw_queue_id)
    expected_rescue_id = build_canonical_signal_id(raw_rescue_id)

    queue_plan = ApprovedExecutionPlan(
        plan_id="plan-ordinary-canonical-407",
        signal_id=raw_queue_id,
        client_id="client@test.com",
        ticker="SPY",
        side="CALL",
        direction="CALL",
        pattern="3-2U",
        timeframe="1h",
        contracts=1,
        max_position_usd=150.0,
        tier="A",
        score=80.0,
        intel_score=80.0,
        confidence_bucket="standard_pool",
        trigger_type="breach",
        trigger_price=450.0,
        stop_underlying=447.0,
        target_underlying=455.0,
        contract_symbol="SPY240101C00450000",
        limit_price=1.50,
        mode="PAPER",
        metadata={
            "client_id": "client@test.com",
            "execution_mode": "paper",
        },
    )
    rescue_plan = _plan_from_payload(
        {
            "signal_id": raw_rescue_id,
            "ticker": "QQQ",
            "side": "PUT",
            "trigger": {"entry": 450.0, "stop": 453.0, "pt1": 440.0},
            "metadata": {
                "client_id": "client@test.com",
                "execution_mode": "paper",
            },
        },
        signal_id=raw_rescue_id,
        client_id="client@test.com",
        execution_mode="paper",
        queue_id=40702,
    )
    assert rescue_plan is not None
    assert not hasattr(queue_plan, "materialization_generation")
    assert not hasattr(rescue_plan, "materialization_generation")

    osm = _MockOSM()
    watcher = APEntryWatcher(MagicMock(), order_state_machine=osm, mode="PAPER")
    watcher._persist_watcher_audit = lambda *args, **kwargs: None
    # Fixed pre-market ET time (2 AM), matching this test's already-explicit
    # intent (via the two session-method patches below) that real-world
    # session/session-boundary state must not gate this test — it exists to
    # prove canonical-ID normalization, not arm-time trigger-through checks.
    # Without this, add_signal()'s own inline `datetime.now(ET)` read (a
    # separate mechanism from the two patched methods) makes this test
    # wall-clock-dependent: it silently passes before 9:30 ET and fails
    # after, since the CALL/PUT arm-time already-through-trigger check
    # (PR #304 "Bug C") only activates during real regular session and the
    # queue_plan (CALL, trigger=450) and rescue_plan (PUT, trigger=450)
    # share one quote that cannot simultaneously satisfy both sides'
    # not-yet-through conditions once that check is live.
    _fixed_pre_market_et = datetime(2026, 8, 5, 2, 0, 0, tzinfo=ET)
    with patch.object(aew_module._base, "datetime") as mock_dt, \
         patch.object(watcher, "_is_regular_session_now", return_value=False), \
         patch.object(watcher, "_is_past_entry_cutoff_now", return_value=False), \
         patch.object(watcher, "_get_quote", return_value={"bid": 450.0, "ask": 450.0}):
        mock_dt.now.return_value = _fixed_pre_market_et
        mock_dt.fromisoformat.side_effect = datetime.fromisoformat
        mock_dt.side_effect = lambda *a, **k: datetime(*a, **k)
        assert watcher.watch(queue_plan, "lo-queue-canonical-407") is True
        assert watcher.watch(rescue_plan, "lo-rescue-canonical-407") is True

    assert watcher._pending[-2].signal["canonical_signal_id"] == expected_queue_id
    assert watcher._pending[-1].signal["canonical_signal_id"] == expected_rescue_id


def test_startup_and_morning_builders_preserve_top_level_trigger_timestamp():
    """Both production restart builders retain legacy top-level evidence."""
    from ap_morning_handoff_audit import _build_audit_plan

    row = _restart_row_with_confirmed_evidence()
    crossed_at = row["meta"].pop("trigger_crossed_at")
    row["trigger_crossed_at"] = crossed_at

    startup_recovery = APStartupRecovery.__new__(APStartupRecovery)
    startup_recovery.client_id = "client@test.com"
    startup_plan = startup_recovery._build_recovery_plan_from_order(row)
    audit_plan = _build_audit_plan(row)

    assert startup_plan.trigger_crossed_at == crossed_at
    assert audit_plan.trigger_crossed_at == crossed_at


@pytest.mark.parametrize("bad_metadata", [
    "not-json",
    "[1, 2]",
    ["malformed"],
    7,
])
def test_restart_recovery_rejects_malformed_metadata_without_side_effects(
    bad_metadata,
):
    """Malformed metadata cannot be reinterpreted as a pre-breach row."""
    row = _restart_row_with_confirmed_evidence()
    row["meta"] = bad_metadata
    osm = _MockOSM()
    watcher = MagicMock()
    quote_check = MagicMock(return_value=False)
    recovery = PendingTriggerRestartRecovery(
        client_id="client@test.com",
        execution_mode="paper",
        osm=osm,
        entry_watcher=watcher,
        broker=MagicMock(),
        quote_check_fn=quote_check,
    )

    outcome = recovery.recover_one_row(row)

    assert outcome == "UNRESOLVED"
    assert recovery._row_failure_reasons[row["local_order_id"]] == (
        RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN
    )
    quote_check.assert_not_called()
    watcher.watch.assert_not_called()
    assert osm.meta_writes == []
    assert osm.cancel_calls == []
    assert osm.expire_calls == []
    assert osm.transition_calls == []


def test_restart_recovery_identity_refusal_has_no_side_effects():
    """Production restart recovery refuses stale evidence before any action."""
    row = json.loads(json.dumps(_restart_row_with_confirmed_evidence()))
    row["meta"]["trigger_crossed_at_provenance"]["canonical_signal_id"] = (
        "stale-canonical"
    )
    osm = _MockOSM()
    osm._row_status[row["local_order_id"]] = "PENDING_TRIGGER"
    osm._row_meta[row["local_order_id"]] = dict(row["meta"])
    watcher = MagicMock()
    quote_check = MagicMock(return_value=False)
    recovery = PendingTriggerRestartRecovery(
        client_id="client@test.com",
        execution_mode="paper",
        osm=osm,
        entry_watcher=watcher,
        broker=MagicMock(),
        quote_check_fn=quote_check,
    )

    outcome = recovery.recover_one_row(row)

    assert outcome == "UNRESOLVED"
    assert recovery._row_failure_reasons[row["local_order_id"]] == (
        RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN
    )
    quote_check.assert_not_called()
    watcher.watch.assert_not_called()
    assert osm.meta_writes == []
    assert osm.cancel_calls == []
    assert osm.expire_calls == []
    assert osm.transition_calls == []

# ===========================================================================
# Class 1b -- Lifecycle identity binding through existing recovery path
# ===========================================================================
#
# The binding specification requires trigger evidence to remain bound to the
# canonical signal ID, client ID, normalized execution mode, and local order
# ID.  No new architecture is introduced here: the existing production
# recovery module (ap.pending_trigger_restart_recovery.PendingTriggerRestartRecovery)
# already enforces this fence and returns UNRESOLVED with a specific reason
# code for each mismatched case.  The tests below prove that a persisted row
# carrying valid trigger_crossed_at metadata cannot be routed into a watcher
# by the recovery path unless every identity field belongs to the same
# lifecycle.

class TestLifecycleIdentityBinding:
    """Cross-client, cross-mode, and missing-identity rows carrying valid
    durable trigger evidence must be rejected by the production recovery
    entry point before any watcher can hydrate."""

    def _row_with_valid_trigger_evidence(
        self,
        *,
        client_id: str,
        execution_mode: str,
        local_order_id=None,
    ) -> dict:
        """A row shaped like the durable Supabase pending_trigger record,
        with all four post-confirmation meta keys populated.  A watcher
        hydrated from this row's meta would treat the trigger as
        confirmed — which is exactly why the recovery entry point must
        reject mismatched lifecycles BEFORE hydration.

        local_order_id=None auto-generates; pass "" explicitly to test
        the missing-oid rejection path (do NOT collapse with `or`).
        """
        oid = str(uuid.uuid4()) if local_order_id is None else local_order_id
        return {
            "local_order_id": oid,
            "signal_id": str(uuid.uuid4()),
            "client_id": client_id,
            "execution_mode": execution_mode,
            "ticker": "BAC",
            "side": "PUT",
            "entry_price": BAC_PUT_TRIGGER,
            "stop_price": BAC_PUT_STOP,
            "meta": {
                "trigger_crossed_at":   datetime.now(timezone.utc).isoformat(),
                "trigger_confirmed_at": datetime.now(timezone.utc).isoformat(),
                "first_breach_bid":     61.85,
                "first_breach_ask":     61.95,
            },
        }

    def test_cross_client_row_rejected_before_hydration(self):
        """Row belongs to another client — recovery MUST fail with
        identity:client_id_mismatch and never construct a watcher."""
        from ap.pending_trigger_restart_recovery import (
            PendingTriggerRestartRecovery,
        )
        recovery = PendingTriggerRestartRecovery(
            client_id="owner@example.com",
            execution_mode="paper",
            osm=_MockOSM(),
        )
        row = self._row_with_valid_trigger_evidence(
            client_id="other@example.com",
            execution_mode="paper",
        )
        outcome = recovery.recover_one_row(row)

        # Recovery does not resolve; failure reason names the mismatch.
        failure_reason = recovery._row_failure_reasons.get(
            row["local_order_id"], ""
        )
        assert failure_reason == "identity:client_id_mismatch", (
            f"Cross-client row must fail with client_id_mismatch; "
            f"got {failure_reason!r}, outcome={outcome!r}."
        )

    def test_cross_mode_row_rejected_before_hydration(self):
        """Row execution_mode does not match the recovery owner's mode
        — MUST fail with identity:execution_mode_mismatch."""
        from ap.pending_trigger_restart_recovery import (
            PendingTriggerRestartRecovery,
        )
        recovery = PendingTriggerRestartRecovery(
            client_id="owner@example.com",
            execution_mode="live",     # owner is LIVE
            osm=_MockOSM(),
        )
        row = self._row_with_valid_trigger_evidence(
            client_id="owner@example.com",
            execution_mode="paper",    # durable row is PAPER
        )
        outcome = recovery.recover_one_row(row)

        failure_reason = recovery._row_failure_reasons.get(
            row["local_order_id"], ""
        )
        assert failure_reason == "identity:execution_mode_mismatch", (
            f"Cross-mode row must fail with execution_mode_mismatch; "
            f"got {failure_reason!r}, outcome={outcome!r}."
        )

    def test_row_missing_local_order_id_rejected(self):
        """A row without a durable local_order_id cannot bind to a
        lifecycle at all — durable trigger evidence must never be reused
        without an owner."""
        import ap.pending_trigger_restart_recovery as restart_recovery
        from ap.pending_trigger_restart_recovery import (
            PendingTriggerRestartRecovery,
        )
        recovery = PendingTriggerRestartRecovery(
            client_id="owner@example.com",
            execution_mode="paper",
            osm=_MockOSM(),
        )
        row = self._row_with_valid_trigger_evidence(
            client_id="owner@example.com",
            execution_mode="paper",
            local_order_id="",   # explicitly missing (helper honors "")
        )
        with patch.object(restart_recovery.log, "critical") as critical:
            outcome = recovery.recover_one_row(row)

        # Recovery does not resolve.  The rejection reason is emitted as a
        # CRITICAL identity-failure log marker (empty local_oid is not
        # storable in _row_failure_reasons keyed by local_oid).
        assert outcome != "RESOLVED", (
            f"Row without local_order_id must not resolve; got {outcome!r}."
        )
        assert critical.called
        assert "RESTART_RECOVERY_MISSING_LOCAL_ORDER_ID" in str(
            critical.call_args
        ), (
            "Missing local_order_id must emit the identity-failure marker."
        )


# ===========================================================================
# Class 2 -- Hydration rejection matrix (Step 7)
# ===========================================================================

@pytest.mark.parametrize("bad_value", [
    None,
    "",
    "not-a-date",
    "2026-07-29T13:32:00",                # naive ISO string (no tz info)
    datetime(2026, 7, 29, 13, 32),        # naive datetime
])
class TestHydrationRejectionMatrix:
    """Step 7: invalid trigger_crossed_at values MUST be treated as
    absence-of-proof.  Hydration parses to None; the scanner stop stays
    dormant; a pre-trigger quote that would break the stop must leave
    state PENDING with no invalidation, selector, or broker call."""

    def test_invalid_input_leaves_stop_dormant_via_production_hydration(
        self, bad_value
    ):
        sig = _bac_put_signal()
        # Direct injection into the production hydration slot (top-level
        # signal[trigger_crossed_at]) -- WatchedSignal.__init__ pipes this
        # through _parse_trigger_crossed_at().  Direct attribute assignment
        # on the watcher is explicitly not used.
        sig["trigger_crossed_at"] = bad_value

        watched = WatchedSignal(sig, overnight=False)
        watched.entry_trigger = BAC_PUT_TRIGGER
        watched.stop_level = BAC_PUT_STOP

        # Hydration rejected the invalid input.
        assert watched.trigger_crossed_at is None

        # WatchedSignal.check() doesn't call selectors or brokers itself;
        # the dispatch loop does.  We assert no state-change and no
        # _pending_audit is produced.
        state = watched.check(bid=62.60, ask=62.62)

        assert state == WatchState.PENDING, (
            f"Bad hydration input {bad_value!r} must leave state PENDING; "
            f"got {state}."
        )
        assert watched._pending_audit is None, (
            "No invalidation audit should have been staged."
        )


# ===========================================================================
# Class 3 -- CALL symmetry (invariant applies equally to both sides)
# ===========================================================================

class TestPreBreachStopActivationCall:
    """SPY CALL: trigger=450, stop=447.  Mirror of the PUT invariant --
    verifies clauses (e) and (f) for CALL: BID authority, dormant before
    confirmation, active after."""

    def test_call_pre_confirmation_stop_dormant_even_if_bid_below_stop(self):
        sig = _spy_call_signal()
        watched = WatchedSignal(sig, overnight=False)
        watched.entry_trigger = 450.0
        watched.stop_level = 447.0

        # Ask below trigger (no breach). Bid clearly below stop with buffer.
        state = watched.check(bid=445.0, ask=449.0)

        assert state == WatchState.PENDING, (
            "Pre-breach CALL stop must be dormant even if bid <= scanner stop."
        )
        assert watched.trigger_crossed_at is None
        assert watched._pending_first_breach_at is None

    def test_call_confirmation_activates_bid_stop_authority(self):
        """CALL: after confirmation, if bid subsequently breaks stop, the
        watcher invalidates on BID authority (spec clause f)."""
        sig = _spy_call_signal()
        watched = WatchedSignal(sig, overnight=False)
        watched.entry_trigger = 450.0
        watched.stop_level = 447.0

        # Drive MOMENTUM_POLLS_REQUIRED breach polls to confirm.
        for _ in range(watched.MOMENTUM_POLLS_REQUIRED):
            _ = watched.check(bid=449.0, ask=451.0)
        assert watched.state == WatchState.TRIGGERED

        # Reset only state to simulate downstream watcher continuing after
        # the confirmed trigger.  Preserve breach_count exactly as the real
        # callback retry/KEEP_WATCHER paths do.
        watched.state = WatchState.PENDING
        preserved_breach_count = watched.breach_count
        watched._trigger_stop_collision = False
        assert preserved_breach_count >= watched.MOMENTUM_POLLS_REQUIRED

        # Now feed a quote where bid breaks stop with buffer clearance.
        # stop=447 * 0.999 = 446.553; bid=446 clears.
        state = watched.check(bid=446.0, ask=449.0)

        assert state == WatchState.INVALIDATED, (
            "Confirmed CALL: bid <= stop*(1-buffer) must invalidate."
        )


def test_confirmed_call_stop_stays_active_through_preopen_window():
    """HOTFIX regression: a confirmed-breach CALL pending-entry watcher must
    not go dormant overnight/pre-market. The pre-open skip exists to protect
    setups that have NOT yet triggered from wide-spread false stops; once
    trigger_crossed_at is confirmed, PR #407's own invariant ("stop stays
    active for the remainder of the pending-entry lifecycle") must hold
    regardless of session, or a confirmed-breach setup remains eligible for
    later recovery, retry, contract selection, or materialization when it
    should already be invalidated.

    The "zero callback on invalidation" property is already proven for both
    sides by test_confirmed_evidence_survives_real_poll_retry_and_active_stop
    via the real _poll_active_signals path; this test is scoped specifically
    to the pre-market-window interaction, using a MagicMock broker to prove
    check() itself never reaches broker submit/cancel.
    """
    broker = MagicMock()
    osm = _MockOSM()
    watcher = APEntryWatcher(broker, order_state_machine=osm, mode="PAPER")
    watcher._persist_watcher_audit = lambda *args, **kwargs: None
    sig = _spy_call_signal()
    sig["overnight"] = True
    watched = WatchedSignal(sig, overnight=True)
    watched.entry_trigger = 450.0
    watched.stop_level = 447.0
    watched._watcher_ref = watcher

    with patch("ap_entry_watcher._is_pre_market_now", return_value=False):
        for _ in range(watched.MOMENTUM_POLLS_REQUIRED):
            _ = watched.check(bid=449.0, ask=451.0)
    assert watched.state == WatchState.TRIGGERED
    assert watched.trigger_crossed_at is not None
    original_confirmed_at = watched.trigger_crossed_at

    # Simulate downstream watcher continuing after the confirmed trigger,
    # then entering the overnight/pre-market window with BID breaking stop.
    watched.state = WatchState.PENDING
    watched._trigger_stop_collision = False

    with patch("ap_entry_watcher._is_pre_market_now", return_value=True):
        state = watched.check(bid=446.0, ask=449.0)

    assert state == WatchState.INVALIDATED, (
        "Confirmed CALL stop must fire even during the pre-market window."
    )
    assert watched.trigger_crossed_at == original_confirmed_at, (
        "Invalidation must not erase or replace confirmed trigger evidence."
    )
    assert watched._pending_audit is not None
    assert watched._pending_audit.get("reason_code") == "stop_bid_below_call_stop", (
        "CALL invalidation must be authorized by BID breaking the CALL stop."
    )
    forbidden_broker_methods = {
        "submit_order", "submit_option_order",
        "cancel_order", "cancel_option_order",
    }
    for call_ in broker.mock_calls:
        called_name = str(call_).split("(", 1)[0]
        for forbidden in forbidden_broker_methods:
            assert forbidden not in called_name, (
                f"Broker.{forbidden} must not be called during "
                f"pre-broker-order invalidation; observed {call_}"
            )


def test_confirmed_put_stop_stays_active_through_preopen_window():
    """HOTFIX regression: PUT mirror of the CALL case above, exercised
    through the actual production restart-recovery path (APStartupRecovery
    -> PendingTriggerRestartRecovery-shaped plan -> watch(recovery_rearm)),
    asserting the real #407 identity and lifecycle fields, not just state.
    """
    persisted_row = _restart_row_with_confirmed_evidence()
    row_after_json = json.loads(json.dumps(persisted_row))
    startup_recovery = APStartupRecovery.__new__(APStartupRecovery)
    startup_recovery.client_id = "client@test.com"
    production_plan = startup_recovery._build_recovery_plan_from_order(
        row_after_json
    )
    osm = _MockOSM()
    osm._row_status[persisted_row["local_order_id"]] = "PENDING_TRIGGER"
    osm._row_meta[persisted_row["local_order_id"]] = dict(row_after_json["meta"])
    broker = MagicMock()
    watcher = APEntryWatcher(broker, order_state_machine=osm, mode="PAPER")
    watcher._persist_watcher_audit = lambda *args, **kwargs: None

    with patch.object(watcher, "_is_regular_session_now", return_value=False), \
         patch.object(watcher, "_is_past_entry_cutoff_now", return_value=False), \
         patch.object(watcher, "_get_quote", return_value={"bid": 62.45, "ask": 62.49}):
        assert watcher.watch(
            production_plan, persisted_row["local_order_id"], recovery_rearm=True
        ) is True

    armed = watcher._pending[0]
    # #407 identity/lifecycle fields — the actual production contract, not
    # just "some watcher exists".
    assert armed.signal["canonical_signal_id"] == "canonical-restart-407"
    assert armed.signal["client_id"] == "client@test.com"
    assert armed.signal["execution_mode"] == "paper"
    assert armed.signal["local_order_id"] == "lo-restart-407"
    assert armed.trigger_crossed_at == datetime.fromisoformat(
        "2026-08-03T16:00:00+00:00"
    )

    # Simulate the overnight/pre-market window explicitly rather than
    # depending on the wall clock at test-run time.
    with patch("ap_entry_watcher._is_pre_market_now", return_value=True):
        state = armed.check(bid=62.60, ask=62.60)

    assert state == WatchState.INVALIDATED, (
        "Confirmed PUT stop must fire even during the pre-market window."
    )
    assert armed._pending_audit is not None
    assert armed._pending_audit.get("reason_code") == "stop_ask_above_put_stop", (
        "PUT invalidation must be authorized by ASK breaking the PUT stop."
    )
    forbidden_broker_methods = {
        "submit_order", "submit_option_order",
        "cancel_order", "cancel_option_order",
    }
    for call_ in broker.mock_calls:
        called_name = str(call_).split("(", 1)[0]
        for forbidden in forbidden_broker_methods:
            assert forbidden not in called_name, (
                f"Broker.{forbidden} must not be called during "
                f"pre-broker-order invalidation; observed {call_}"
            )


def test_prebreach_overnight_call_stop_touch_during_premarket_remains_ignored():
    """Preservation: the pre-2026-08-05 behavior must still hold for a
    setup that has NOT triggered yet — pre-market spread protection is
    still active pre-breach, exactly as FUNNEL FIX (2026-05-20) intended.
    """
    sig = _spy_call_signal()
    sig["overnight"] = True
    watched = WatchedSignal(sig, overnight=True)
    watched.entry_trigger = 450.0
    watched.stop_level = 447.0
    assert watched.trigger_crossed_at is None

    with patch("ap_entry_watcher._is_pre_market_now", return_value=True):
        state = watched.check(bid=446.0, ask=449.0)

    assert state == WatchState.PENDING, (
        "Pre-breach overnight CALL stop touch during pre-market must "
        "still be ignored — this is the original FUNNEL FIX behavior "
        "and must not regress."
    )
    assert watched.trigger_crossed_at is None


def test_prebreach_overnight_put_stop_touch_during_premarket_remains_ignored():
    """Preservation: PUT mirror of the CALL case above."""
    sig = {
        "signal_id": str(uuid.uuid4()),
        "local_order_id": str(uuid.uuid4()),
        "client_id": "client@test.com",
        "client_email": "client@test.com",
        "execution_mode": "paper",
        "timeframe": "1d",
        "ticker": "BAC",
        "side": "PUT",
        "entry_price": BAC_PUT_TRIGGER,
        "stop_price": BAC_PUT_STOP,
        "target_price": 60.50,
        "contract_symbol": "BAC240101P00062000",
        "contracts": 1,
        "limit_price": 1.20,
        "score": 0.8,
        "tier": "A",
        "queue_status": "QUEUED",
    }
    sig["canonical_signal_id"] = sig["signal_id"]
    sig["metadata"] = {
        "canonical_signal_id": sig["canonical_signal_id"],
        "client_id": sig["client_id"],
        "execution_mode": sig["execution_mode"],
    }
    watched = WatchedSignal(sig, overnight=True)
    watched.entry_trigger = BAC_PUT_TRIGGER
    watched.stop_level = BAC_PUT_STOP
    assert watched.trigger_crossed_at is None

    with patch("ap_entry_watcher._is_pre_market_now", return_value=True):
        state = watched.check(bid=62.45, ask=62.60)

    assert state == WatchState.PENDING, (
        "Pre-breach overnight PUT stop touch during pre-market must "
        "still be ignored — must not regress."
    )
    assert watched.trigger_crossed_at is None


def test_ordinary_nonovernight_preopen_behavior_unchanged():
    """Preservation: a non-overnight, non-daily setup never engages
    _pre_open_skip in the first place (self.overnight is False and the
    signal is not daily), so this fix must not change its behavior at all,
    regardless of confirmation state or wall-clock session.
    """
    sig = _spy_call_signal()
    watched = WatchedSignal(sig, overnight=False)
    watched.entry_trigger = 450.0
    watched.stop_level = 447.0

    for _ in range(watched.MOMENTUM_POLLS_REQUIRED):
        _ = watched.check(bid=449.0, ask=451.0)
    assert watched.state == WatchState.TRIGGERED
    watched.state = WatchState.PENDING
    watched._trigger_stop_collision = False

    with patch("ap_entry_watcher._is_pre_market_now", return_value=True):
        state = watched.check(bid=446.0, ask=449.0)

    assert state == WatchState.INVALIDATED, (
        "Non-overnight setups were never gated by _pre_open_skip and must "
        "invalidate identically before and after this fix."
    )


def test_call_invalidation_preserves_independent_put_on_same_ticker():
    """Opposite-side preservation: a confirmed CALL breaking its CALL stop
    must invalidate ONLY the CALL watcher. The independent PUT setup on the
    same underlying (distinct signal_id/local_order_id by construction --
    see app.py's _discord_signal_id(symbol, "CALL"/"PUT", ...)) must remain
    untouched: not deleted, not terminalized, not consumed, still eligible
    to activate later on its own PUT trigger confirmation.
    """
    broker = MagicMock()
    osm = _MockOSM()
    watcher = APEntryWatcher(broker, order_state_machine=osm, mode="PAPER")
    watcher._persist_watcher_audit = lambda *args, **kwargs: None

    call_sig = _spy_call_signal()
    put_sig = _spy_call_signal()  # fresh signal_id/local_order_id
    put_sig["side"] = "PUT"
    put_sig["entry_price"] = 445.0
    put_sig["stop_price"] = 448.0
    put_sig["target_price"] = 440.0
    put_sig["canonical_signal_id"] = put_sig["signal_id"]
    put_sig["metadata"] = {
        "canonical_signal_id": put_sig["canonical_signal_id"],
        "client_id": put_sig["client_id"],
        "execution_mode": put_sig["execution_mode"],
    }
    assert call_sig["signal_id"] != put_sig["signal_id"]
    assert call_sig["local_order_id"] != put_sig["local_order_id"]

    call_watched = WatchedSignal(call_sig, overnight=True)
    call_watched.entry_trigger = 450.0
    call_watched.stop_level = 447.0
    call_watched._watcher_ref = watcher

    put_watched = WatchedSignal(put_sig, overnight=True)
    put_watched.entry_trigger = 445.0
    put_watched.stop_level = 448.0
    put_watched._watcher_ref = watcher

    watcher._pending = [call_watched, put_watched]
    watcher._dedup_set = {call_sig["signal_id"], put_sig["signal_id"]}

    # Confirm the CALL breach (regular hours), independent of PUT entirely.
    with patch("ap_entry_watcher._is_pre_market_now", return_value=False):
        for _ in range(call_watched.MOMENTUM_POLLS_REQUIRED):
            _ = call_watched.check(bid=449.0, ask=451.0)
    assert call_watched.state == WatchState.TRIGGERED
    assert call_watched.trigger_crossed_at is not None
    call_watched.state = WatchState.PENDING
    call_watched._trigger_stop_collision = False

    # PUT has NEVER been touched — still pristine, pre-breach.
    assert put_watched.state == WatchState.PENDING
    assert put_watched.trigger_crossed_at is None

    # CALL stop breaks during pre-market (the exact #414 scenario).
    with patch("ap_entry_watcher._is_pre_market_now", return_value=True):
        call_state = call_watched.check(bid=446.0, ask=449.0)
    assert call_state == WatchState.INVALIDATED
    assert call_watched._pending_audit.get("reason_code") == "stop_bid_below_call_stop"

    # PUT must be completely unaffected by the CALL invalidation: same
    # object identity, same signal_id, still PENDING, still un-triggered,
    # still present in the watcher's own bookkeeping.
    assert put_watched.state == WatchState.PENDING, (
        "CALL invalidation must not alter the independent PUT setup's state."
    )
    assert put_watched.trigger_crossed_at is None, (
        "CALL invalidation must not fabricate PUT trigger evidence."
    )
    assert put_watched in watcher._pending, (
        "The independent PUT watcher must not be removed/deleted/consumed "
        "as a side effect of the CALL invalidating."
    )
    assert put_sig["signal_id"] in watcher._dedup_set, (
        "The PUT's dedup entry must not be cleared by the CALL invalidating."
    )

    # PUT can still activate normally afterward, on its OWN trigger only —
    # the CALL's stop break does not auto-activate the opposite side.
    with patch("ap_entry_watcher._is_pre_market_now", return_value=False):
        for _ in range(put_watched.MOMENTUM_POLLS_REQUIRED):
            put_state = put_watched.check(bid=444.0, ask=446.0)
    assert put_state == WatchState.TRIGGERED, (
        "The independent PUT must still be able to confirm its own trigger "
        "after the CALL side invalidated."
    )
    assert put_watched.trigger_crossed_at is not None

    forbidden_broker_methods = {
        "submit_order", "submit_option_order",
        "cancel_order", "cancel_option_order",
    }
    for call_ in broker.mock_calls:
        called_name = str(call_).split("(", 1)[0]
        for forbidden in forbidden_broker_methods:
            assert forbidden not in called_name, (
                f"Broker.{forbidden} must not be called by either the CALL "
                f"invalidation or the PUT confirmation; observed {call_}"
            )
    assert osm.cancel_calls == [] and osm.expire_calls == [], (
        "Neither the CALL invalidation nor the PUT confirmation reaching "
        "trigger state should cancel or expire any order — trigger "
        "confirmation alone is not a broker submission or order mutation."
    )


def test_put_invalidation_preserves_independent_call_on_same_ticker():
    """Opposite-side preservation, mirrored: a confirmed PUT breaking its
    PUT stop must invalidate ONLY the PUT watcher; the independent CALL
    setup remains eligible and activates only on its own trigger.
    """
    broker = MagicMock()
    osm = _MockOSM()
    watcher = APEntryWatcher(broker, order_state_machine=osm, mode="PAPER")
    watcher._persist_watcher_audit = lambda *args, **kwargs: None

    put_sig = _spy_call_signal()
    put_sig["side"] = "PUT"
    put_sig["entry_price"] = 445.0
    put_sig["stop_price"] = 448.0
    put_sig["target_price"] = 440.0
    put_sig["canonical_signal_id"] = put_sig["signal_id"]
    put_sig["metadata"] = {
        "canonical_signal_id": put_sig["canonical_signal_id"],
        "client_id": put_sig["client_id"],
        "execution_mode": put_sig["execution_mode"],
    }
    call_sig = _spy_call_signal()
    assert call_sig["signal_id"] != put_sig["signal_id"]
    assert call_sig["local_order_id"] != put_sig["local_order_id"]

    put_watched = WatchedSignal(put_sig, overnight=True)
    put_watched.entry_trigger = 445.0
    put_watched.stop_level = 448.0
    put_watched._watcher_ref = watcher

    call_watched = WatchedSignal(call_sig, overnight=True)
    call_watched.entry_trigger = 450.0
    call_watched.stop_level = 447.0
    call_watched._watcher_ref = watcher

    watcher._pending = [put_watched, call_watched]
    watcher._dedup_set = {put_sig["signal_id"], call_sig["signal_id"]}

    with patch("ap_entry_watcher._is_pre_market_now", return_value=False):
        for _ in range(put_watched.MOMENTUM_POLLS_REQUIRED):
            _ = put_watched.check(bid=444.0, ask=446.0)
    assert put_watched.state == WatchState.TRIGGERED
    assert put_watched.trigger_crossed_at is not None
    put_watched.state = WatchState.PENDING
    put_watched._trigger_stop_collision = False

    assert call_watched.state == WatchState.PENDING
    assert call_watched.trigger_crossed_at is None

    with patch("ap_entry_watcher._is_pre_market_now", return_value=True):
        put_state = put_watched.check(bid=447.0, ask=449.0)
    assert put_state == WatchState.INVALIDATED
    assert put_watched._pending_audit.get("reason_code") == "stop_ask_above_put_stop"

    assert call_watched.state == WatchState.PENDING, (
        "PUT invalidation must not alter the independent CALL setup's state."
    )
    assert call_watched.trigger_crossed_at is None, (
        "PUT invalidation must not fabricate CALL trigger evidence."
    )
    assert call_watched in watcher._pending
    assert call_sig["signal_id"] in watcher._dedup_set

    with patch("ap_entry_watcher._is_pre_market_now", return_value=False):
        for _ in range(call_watched.MOMENTUM_POLLS_REQUIRED):
            call_state = call_watched.check(bid=449.0, ask=451.0)
    assert call_state == WatchState.TRIGGERED, (
        "The independent CALL must still be able to confirm its own "
        "trigger after the PUT side invalidated."
    )
    assert call_watched.trigger_crossed_at is not None

    forbidden_broker_methods = {
        "submit_order", "submit_option_order",
        "cancel_order", "cancel_option_order",
    }
    for call_ in broker.mock_calls:
        called_name = str(call_).split("(", 1)[0]
        for forbidden in forbidden_broker_methods:
            assert forbidden not in called_name, (
                f"Broker.{forbidden} must not be called by either the PUT "
                f"invalidation or the CALL confirmation; observed {call_}"
            )
    assert osm.cancel_calls == [] and osm.expire_calls == [], (
        "Neither the PUT invalidation nor the CALL confirmation reaching "
        "trigger state should cancel or expire any order."
    )
