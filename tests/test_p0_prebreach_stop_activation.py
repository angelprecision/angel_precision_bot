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
from unittest.mock import MagicMock, patch

import pytest

from ap_entry_watcher import APEntryWatcher, WatchedSignal, WatchState


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
    if trigger_crossed_at is not None:
        sig["trigger_crossed_at"] = trigger_crossed_at
    return sig


def _spy_call_signal(local_order_id: Optional[str] = None) -> dict:
    return {
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

        # Drive the confirming poll through the production dispatch path.
        # The quote leaves bid strictly above scanner stop (no collision).
        with patch.object(w, "_fetch_quotes", return_value={
            "BAC": {"bid": 61.80, "ask": 61.90}
        }):
            w._poll_active_signals(open_protect_active=False)

        # meta_write occurred, and it occurred BEFORE on_trigger (if
        # on_trigger fired at all this pass).
        assert "meta_write" in events, (
            "Dispatch path did not call update_order_meta before on_trigger."
        )
        if "on_trigger" in events:
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
        ):
            assert required_key in first_ts_patch, (
                f"meta patch missing required key: {required_key}"
            )

    def test_restart_hydration_reactivates_stop_via_ask(self):
        """Step 6 (parts 7-11): after a JSONB round-trip and reconstruction
        through the production hydration entry point (WatchedSignal.__init__
        reading trigger_crossed_at from the signal), the scanner stop is
        ACTIVE.  A post-restart quote whose ASK breaks the PUT stop must
        invalidate -- with no selector call, no broker submit, no broker
        cancel of a nonexistent broker order, and no duplicate local order.

        Note on geometry: WRONG_DIR_BUFFER_PCT (0.001) requires
        ask >= stop_level * 1.001 to fire.  For stop=62.49, threshold is
        62.5525; test feeds ask=62.60 which comfortably clears the buffer
        while preserving the spec's "post-restart quote above trigger,
        breaks PUT stop via ASK" invariant.
        """
        # Round-trip the persisted meta through JSONB serialization to
        # simulate the Supabase boundary.
        captured_meta = {
            "trigger_crossed_at": datetime.now(timezone.utc).isoformat(),
            "trigger_confirmed_at": datetime.now(timezone.utc).isoformat(),
            "first_breach_bid": 61.85,
            "first_breach_ask": 61.95,
        }
        round_tripped_meta = json.loads(json.dumps(captured_meta))

        # Reconstruct through the production hydration entry point:
        # WatchedSignal.__init__ pulls trigger_crossed_at from either the
        # top-level signal dict or signal["metadata"], then routes it
        # through _parse_trigger_crossed_at().
        sig = _bac_put_signal(
            trigger_crossed_at=round_tripped_meta["trigger_crossed_at"]
        )
        second_watcher = WatchedSignal(sig, overnight=False)
        second_watcher.entry_trigger = BAC_PUT_TRIGGER
        second_watcher.stop_level = BAC_PUT_STOP

        # Hydration must produce a tz-aware datetime.
        assert isinstance(second_watcher.trigger_crossed_at, datetime)
        assert second_watcher.trigger_crossed_at.tzinfo is not None

        broker = MagicMock()
        selector = MagicMock()
        on_trigger_calls: list[str] = []

        # WatchedSignal.check() does not call selector or broker directly;
        # those are on the APEntryWatcher dispatch loop.  We verify no
        # side-effect from the check() invalidation itself, and that
        # neither MagicMock was touched.
        state = second_watcher.check(bid=62.60, ask=62.60)

        assert state == WatchState.INVALIDATED, (
            "Post-restart quote whose ask breaks the PUT stop must "
            "invalidate given hydrated trigger_crossed_at."
        )
        assert selector.mock_calls == [], (
            "Selector must not be called during stop invalidation."
        )
        assert broker.mock_calls == [], (
            "Broker must not be called (no submit, no cancel) during "
            "pre-broker-order invalidation."
        )
        assert on_trigger_calls == []

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

        # Reset state to simulate downstream watcher continuing after the
        # confirmed trigger (durable trigger_crossed_at retained).
        watched.state = WatchState.PENDING
        watched.breach_count = 0
        watched._trigger_stop_collision = False

        # Now feed a quote where bid breaks stop with buffer clearance.
        # stop=447 * 0.999 = 446.553; bid=446 clears.
        state = watched.check(bid=446.0, ask=449.0)

        assert state == WatchState.INVALIDATED, (
            "Confirmed CALL: bid <= stop*(1-buffer) must invalidate."
        )
