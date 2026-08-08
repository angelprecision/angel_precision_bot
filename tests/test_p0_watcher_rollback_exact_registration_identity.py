"""
tests/test_p0_watcher_rollback_exact_registration_identity.py

P0 regression for PR #421 (fix/p0-direction-reversal-rearm), amendment:
"watcher rollback ownership".

Bug: APStartupRecovery._evict_just_registered_watcher() fenced eviction on
LOGICAL identity only (local_order_id / client_id / signal_id /
execution_mode). Logical identity can legitimately be shared by two
DIFFERENT watcher registrations across an ownership transition:

    loser registers watcher A
    -> loser identifies A for rollback
    -> A disappears and releases its dedup key (via some other, legitimate
       path -- e.g. normal cleanup, expiry, or another actor's own fenced
       eviction)
    -> legitimate winner registers watcher B for the same signal_id
    -> loser resumes its now-stale rollback
    -> loser matches "A" by logical fields, actually finds B
    -> loser evicts B and releases B's dedup key
    -> B remains logically re-derivable in _pending's absence but the
       signal_id dedup protection is gone -- a duplicate could now be
       armed for the same signal.

Fix: rollback is fenced to the EXACT watcher object identity
(``id()``) captured by ``_capture_just_registered_watcher_id()``
immediately after a WATCHER_OWNED outcome -- not logical fields alone.
A stale loser may only remove the precise registration it created, and
may only release dedup ownership when that exact registration is still
the one present.

This test drives the REAL production ``APEntryWatcher`` (not a fake) and
the REAL ``APStartupRecovery._capture_just_registered_watcher_id`` /
``_evict_just_registered_watcher`` methods end-to-end, forcing the exact
adversarial sequence above.
"""
from __future__ import annotations

import os
from unittest.mock import MagicMock

os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost/db")

from ap_entry_watcher import APEntryWatcher
from ap_recovery import APStartupRecovery


LOCAL_ORDER_ID = "order-rollback-race-001"
SIGNAL_ID = "sig-rollback-race-001"
CLIENT_ID = "jason@example.com"
EXECUTION_MODE = "paper"


def _signal(*, entry_price=100.0) -> dict:
    return {
        "ticker": "SPY",
        "side": "CALL",
        "entry_price": entry_price,
        "score": 75.0,
        "grade": "A",
        "signal_id": SIGNAL_ID,
        "local_order_id": LOCAL_ORDER_ID,
        "client_id": CLIENT_ID,
        "execution_mode": EXECUTION_MODE,
    }


def _build_real_watcher() -> APEntryWatcher:
    # order_state_machine=None makes _validate_local_order_id a documented
    # no-op passthrough (see APEntryWatcher._validate_local_order_id), so
    # add_signal() exercises the real _pending / _dedup_set / _lock
    # machinery without requiring an unrelated OSM double.
    return APEntryWatcher(broker=MagicMock(), order_state_machine=None, mode="PAPER")


def _build_recovery(entry_watcher: APEntryWatcher) -> APStartupRecovery:
    return APStartupRecovery(
        client_id=CLIENT_ID,
        broker=MagicMock(),
        osm=MagicMock(),
        pm=MagicMock(),
        master_control=MagicMock(),
        entry_watcher=entry_watcher,
    )


def test_stale_loser_cannot_evict_legitimate_winners_replacement_registration():
    """Force the exact adversarial sequence from the PR #421 watcher
    rollback amendment and prove the stale loser can neither remove the
    winner's registration nor release its dedup authority."""
    watcher = _build_real_watcher()
    recovery = _build_recovery(watcher)

    # ── Step 1: losing recovery actor creates watcher A ─────────────────
    armed_a = watcher.add_signal(_signal())
    assert armed_a is True, "setup: watcher A must arm successfully"
    assert watcher.has_order(LOCAL_ORDER_ID) is True
    assert SIGNAL_ID in watcher._dedup_set

    # ── Step 2: pause the loser's rollback before final cleanup ─────────
    # The loser captures the exact identity of ITS registration now --
    # this is the production call site's ordering: capture happens
    # immediately after WATCHER_OWNED, before the durable CAS attempt
    # that will (in the failure branch) trigger rollback later.
    loser_expected_watcher_id = recovery._capture_just_registered_watcher_id(
        watcher,
        local_order_id=LOCAL_ORDER_ID,
        client_id=CLIENT_ID,
        signal_id=SIGNAL_ID,
        execution_mode=EXECUTION_MODE,
    )
    assert loser_expected_watcher_id is not None, (
        "setup: loser must have captured a real exact-registration id for A"
    )

    # ── Step 3: A disappears / its original dedup ownership is released ─
    # Simulates a legitimate concurrent path independently retiring A
    # (e.g. expiry, a different actor's own correctly-fenced cleanup).
    # This uses the same production idiom _evict_just_registered_watcher
    # itself uses (exact-identity _pending filter + _release_dedup_key),
    # applied directly here to isolate step 3 from the code under test.
    with watcher._lock:
        watcher._pending = [
            w for w in watcher._pending
            if recovery._exact_identity_of(w) != loser_expected_watcher_id
        ]
        watcher._dedup_set.discard(SIGNAL_ID)
    assert watcher.has_order(LOCAL_ORDER_ID) is False
    assert SIGNAL_ID not in watcher._dedup_set, (
        "setup: A's dedup key must be fully released before the winner arms"
    )

    # ── Step 4: legitimate winner creates watcher B for the same signal_id ─
    armed_b = watcher.add_signal(_signal())
    assert armed_b is True, (
        "setup: winner B must be able to arm now that A's dedup key is clear"
    )
    assert watcher.has_order(LOCAL_ORDER_ID) is True
    assert SIGNAL_ID in watcher._dedup_set
    winner_watcher_id = recovery._exact_identity_of(
        recovery._find_pending_watcher_by_logical_identity(
            watcher,
            local_order_id=LOCAL_ORDER_ID,
            client_id=CLIENT_ID,
            signal_id=SIGNAL_ID,
            execution_mode=EXECUTION_MODE,
        )
    )
    assert winner_watcher_id != loser_expected_watcher_id, (
        "setup: B must be a distinct in-memory registration from A -- this "
        "is the whole precondition for the race"
    )

    # ── Step 5: resume the loser's stale cleanup ─────────────────────────
    # The loser still believes it owns the logical identity and now
    # executes its rollback, fenced only to the id it captured in step 2.
    evicted = recovery._evict_just_registered_watcher(
        watcher,
        local_order_id=LOCAL_ORDER_ID,
        client_id=CLIENT_ID,
        signal_id=SIGNAL_ID,
        execution_mode=EXECUTION_MODE,
        expected_watcher_id=loser_expected_watcher_id,
    )
    assert evicted is True, (
        "rollback must report clean completion (nothing it owns remains to "
        "evict), not an error"
    )

    # ── Step 6: watcher B is still present ────────────────────────────────
    assert watcher.has_order(LOCAL_ORDER_ID) is True, (
        "the stale loser's rollback must never remove the legitimate "
        "winner's replacement registration"
    )

    # ── Step 7: signal_id is still present in the real dedup set ─────────
    assert SIGNAL_ID in watcher._dedup_set, (
        "the stale loser's rollback must never release dedup ownership "
        "belonging to a registration it did not create"
    )

    # ── Step 8: another add_signal() for the same signal is rejected ─────
    duplicate_armed = watcher.add_signal(_signal())
    assert duplicate_armed is False, (
        "dedup protection must still be intact -- a duplicate arm for the "
        "same signal_id must be rejected"
    )
    assert watcher._last_reject_reason == "dedup_block"

    # Exactly one registration remains: B, untouched.
    assert len(watcher._pending) == 1
    assert recovery._exact_identity_of(watcher._pending[0]) == winner_watcher_id


def test_rollback_still_evicts_and_releases_when_registration_is_truly_its_own():
    """Sanity counterpart: when no ownership transition occurred, rollback
    must still work exactly as before -- evict the exact registration and
    release its dedup key."""
    watcher = _build_real_watcher()
    recovery = _build_recovery(watcher)

    armed = watcher.add_signal(_signal())
    assert armed is True

    expected_id = recovery._capture_just_registered_watcher_id(
        watcher,
        local_order_id=LOCAL_ORDER_ID,
        client_id=CLIENT_ID,
        signal_id=SIGNAL_ID,
        execution_mode=EXECUTION_MODE,
    )
    assert expected_id is not None

    evicted = recovery._evict_just_registered_watcher(
        watcher,
        local_order_id=LOCAL_ORDER_ID,
        client_id=CLIENT_ID,
        signal_id=SIGNAL_ID,
        execution_mode=EXECUTION_MODE,
        expected_watcher_id=expected_id,
    )
    assert evicted is True
    assert watcher.has_order(LOCAL_ORDER_ID) is False
    assert SIGNAL_ID not in watcher._dedup_set
    assert len(watcher._pending) == 0


def test_rollback_with_no_captured_id_is_a_safe_noop():
    """If capture ever yields None (nothing provably this actor's own at
    registration time), rollback must not guess -- it must do nothing and
    must never release a dedup key it cannot prove ownership of."""
    watcher = _build_real_watcher()
    recovery = _build_recovery(watcher)

    armed = watcher.add_signal(_signal())
    assert armed is True

    evicted = recovery._evict_just_registered_watcher(
        watcher,
        local_order_id=LOCAL_ORDER_ID,
        client_id=CLIENT_ID,
        signal_id=SIGNAL_ID,
        execution_mode=EXECUTION_MODE,
        expected_watcher_id=None,
    )
    assert evicted is True
    # Nothing touched -- the real registration is exactly as it was.
    assert watcher.has_order(LOCAL_ORDER_ID) is True
    assert SIGNAL_ID in watcher._dedup_set
    assert len(watcher._pending) == 1
