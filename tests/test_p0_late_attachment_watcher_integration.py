"""
tests/test_p0_late_attachment_watcher_integration.py
=====================================================
PR #388 Block-2 — integration tests for the WatchedSignal.check() gate
that consumes ap.pending_trigger_classifier.classify_late_attachment.

Exercises the state machine directly on a WatchedSignal (no APEntryWatcher
wiring needed). Covers: CALL/PUT confirm, WAITING_RESET, reset→rebreach,
STOP_BROKEN mid-flight, restart identity, one-poll spike rejection.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

import ap_entry_watcher as ew
from ap.pending_trigger_classifier import (
    LATE_ATTACHMENT_AWAITING_FIRST_TRUTH as _AWAITING,
    LATE_ATTACHMENT_WITHIN_CONTINUATION as _WITHIN,
    MISSED_LATE_WATCHER_ATTACHMENT_WAITING_RESET as _WAITING,
)


def _signal(**overrides):
    base = {
        "signal_id": "sig-test",
        "ticker":    "AAPL",
        "side":      "CALL",
        "entry_price": 100.0,
        "stop_price":  95.0,
        "target_price": 110.0,
        "score":     80.0,
        "grade":     "A",
    }
    base.update(overrides)
    return base


def _make_watcher(*, side="CALL", trigger=100.0, stop=95.0, state=None):
    seed = None
    if state is not None:
        seed = {
            "state":                state,
            "seen_at":              datetime.now(timezone.utc).isoformat(),
            "quote":                float(trigger) + 0.05,
            "quote_source":         "ask" if side == "CALL" else "bid",
            "allowed_continuation": 0.15,
            "raw_bid":              float(trigger) - 0.05,
            "raw_ask":              float(trigger) + 0.05,
        }
    signal = _signal(side=side, entry_price=trigger, stop_price=stop)
    if seed is not None:
        signal["_late_attachment_seed"] = seed
    return ew.WatchedSignal(signal, overnight=False)


# ── CALL confirm path ────────────────────────────────────────────────────────

def test_call_within_continuation_first_poll_does_not_trigger():
    w = _make_watcher(side="CALL", trigger=100, state=_WITHIN)
    # Fresh poll inside continuation zone.
    st = w.check(bid=99.98, ask=100.05)
    assert st == ew.WatchState.PENDING
    assert w.late_attachment_state == _WITHIN
    assert w.late_confirm_polls == 1


def test_call_within_continuation_second_poll_clears_gate_and_ordinary_path_fires():
    w = _make_watcher(side="CALL", trigger=100, state=_WITHIN)
    # First confirming poll — still gated.
    w.check(bid=99.98, ask=100.05)
    assert w.late_attachment_state == _WITHIN
    # Second confirming poll — gate clears. Ordinary breach path is entered
    # with the same poll (ask >= trigger), so breach_count = 1.
    w.check(bid=99.98, ask=100.05)
    assert w.late_attachment_state is None
    # Now the third poll runs through the ordinary path only.
    st = w.check(bid=99.98, ask=100.05)
    assert st == ew.WatchState.TRIGGERED


# ── PUT confirm path ─────────────────────────────────────────────────────────

def test_put_within_continuation_two_polls_clears_gate():
    w = _make_watcher(side="PUT", trigger=100, stop=105, state=_WITHIN)
    w.check(bid=99.95, ask=100.02)
    assert w.late_attachment_state == _WITHIN
    w.check(bid=99.95, ask=100.02)
    assert w.late_attachment_state is None


# ── WAITING_RESET path ──────────────────────────────────────────────────────

def test_waiting_reset_does_not_terminalize_on_continued_price_past_zone():
    w = _make_watcher(side="CALL", trigger=100, state=_WAITING)
    for _ in range(5):
        st = w.check(bid=100.30, ask=100.35)  # well past continuation
        assert st == ew.WatchState.PENDING
        assert w.late_attachment_state == _WAITING


def test_waiting_reset_two_polls_below_threshold_clears_state():
    w = _make_watcher(side="CALL", trigger=100, state=_WAITING)
    # reset_tolerance = max(0.01, 0.075) = 0.075; CALL reset needs ask <= 99.925
    w.check(bid=99.90, ask=99.925)
    assert w.late_attachment_state == _WAITING
    assert w.late_reset_polls == 1
    w.check(bid=99.90, ask=99.925)
    assert w.late_attachment_state is None
    assert w.late_reset_polls == 0
    assert w.breach_count == 0  # cleared so a new ordinary breach is required


def test_waiting_reset_single_poll_below_then_above_does_not_confirm():
    w = _make_watcher(side="CALL", trigger=100, state=_WAITING)
    w.check(bid=99.90, ask=99.90)  # below threshold, count=1
    assert w.late_reset_polls == 1
    w.check(bid=100.20, ask=100.30)  # bounced back above
    assert w.late_reset_polls == 0
    assert w.late_attachment_state == _WAITING


# ── STOP_BROKEN mid-flight ──────────────────────────────────────────────────

def test_stop_broken_during_within_continuation_terminalizes():
    w = _make_watcher(side="CALL", trigger=100, stop=95, state=_WITHIN)
    st = w.check(bid=94.90, ask=94.95)
    assert st == ew.WatchState.INVALIDATED


# ── Missing canonical quote is retryable, not terminal ─────────────────────

def test_missing_canonical_quote_during_gate_does_not_terminalize():
    w = _make_watcher(side="CALL", trigger=100, state=_WITHIN)
    # ask=0 → TRIGGER_TRUTH_UNAVAILABLE_RETRY; state must be preserved.
    st = w.check(bid=100.05, ask=0)
    assert st == ew.WatchState.PENDING
    assert w.late_attachment_state == _WITHIN


# ── Restart identity: same seed produces same initial state ────────────────

def test_restart_identity_same_seed_produces_same_state():
    """A restart re-runs WatchedSignal.__init__ with the seeded signal payload.
    Pure classifier + explicit seed consumption means the reconstructed watcher
    starts at the same state as the original."""
    seed = {
        "state":                _WITHIN,
        "seen_at":              datetime.now(timezone.utc).isoformat(),
        "quote":                100.05,
        "quote_source":         "ask",
        "allowed_continuation": 0.15,
        "raw_bid":              99.98,
        "raw_ask":              100.05,
    }
    payload_a = _signal(side="CALL", entry_price=100.0, stop_price=95.0)
    payload_a["_late_attachment_seed"] = dict(seed)
    payload_b = _signal(side="CALL", entry_price=100.0, stop_price=95.0)
    payload_b["_late_attachment_seed"] = dict(seed)
    a = ew.WatchedSignal(payload_a)
    b = ew.WatchedSignal(payload_b)
    assert a.late_attachment_state == b.late_attachment_state == _WITHIN
    assert a.late_attachment_generation == b.late_attachment_generation == 1


def test_seed_is_consumed_off_signal_payload():
    """Once a WatchedSignal is constructed, the seed is removed from the
    signal dict so a subsequent construction (e.g. duplicate register)
    does not re-seed."""
    payload = _signal(side="CALL", entry_price=100.0, stop_price=95.0)
    payload["_late_attachment_seed"] = {
        "state":                _WITHIN,
        "seen_at":              datetime.now(timezone.utc).isoformat(),
        "quote":                100.05,
        "quote_source":         "ask",
        "allowed_continuation": 0.15,
        "raw_bid":              99.98,
        "raw_ask":              100.05,
    }
    ew.WatchedSignal(payload)
    assert "_late_attachment_seed" not in payload


# ── Original signal identity preserved through the gate ────────────────────

# ── AWAITING_FIRST_TRUTH: arm-time missing canonical quote must NOT let the
# ordinary breach path fire on the first available quote without the
# continuation policy being applied. Regression tests for the reviewer-found
# defect where a missing arm-time ask followed by a far-past-trigger ask
# recreated the July-23 arm_already_through_trigger failure through the
# missing-quote branch.

def _make_awaiting_call(trigger=200.0, stop=180.0):
    return _make_watcher(side="CALL", trigger=trigger, stop=stop, state=_AWAITING)


def _make_awaiting_put(trigger=200.0, stop=220.0):
    return _make_watcher(side="PUT", trigger=trigger, stop=stop, state=_AWAITING)


def test_call_arm_time_ask_missing_then_far_above_transitions_to_waiting_reset():
    """CALL arm-time ask missing → next ask far above continuation → WAITING_RESET.
    Must NOT enter the ordinary breach path; no trigger submission."""
    w = _make_awaiting_call(trigger=200.0)
    # Sanity: seeded AWAITING.
    assert w.late_attachment_state == _AWAITING
    # First truthful quote: ask=200.40, far above the 200.15 upper bound.
    st = w.check(bid=200.35, ask=200.40)
    assert st == ew.WatchState.PENDING
    assert w.late_attachment_state == _WAITING
    assert w.state != ew.WatchState.TRIGGERED
    # Subsequent polls at the same far-past-trigger price must stay PENDING
    # in WAITING_RESET and must never trigger.
    for _ in range(3):
        st = w.check(bid=200.35, ask=200.40)
        assert st == ew.WatchState.PENDING
        assert w.late_attachment_state == _WAITING


def test_put_arm_time_bid_missing_then_far_below_transitions_to_waiting_reset():
    """PUT arm-time bid missing → next bid far below continuation → WAITING_RESET.
    Must NOT enter the ordinary breach path; no trigger submission."""
    w = _make_awaiting_put(trigger=200.0)
    assert w.late_attachment_state == _AWAITING
    # First truthful quote: bid=199.60, far below the 199.85 lower bound.
    st = w.check(bid=199.60, ask=199.65)
    assert st == ew.WatchState.PENDING
    assert w.late_attachment_state == _WAITING
    assert w.state != ew.WatchState.TRIGGERED
    for _ in range(3):
        st = w.check(bid=199.60, ask=199.65)
        assert st == ew.WatchState.PENDING
        assert w.late_attachment_state == _WAITING


def test_arm_time_missing_quote_preserved_across_multiple_missing_polls():
    """Arm-time canonical quote missing for several polls → state preserved
    → no terminalization → no ordinary breach."""
    w = _make_awaiting_call(trigger=200.0)
    for _ in range(5):
        # CALL canonical = ask; ask=0 means canonical unavailable.
        st = w.check(bid=200.30, ask=0)
        assert st == ew.WatchState.PENDING
        assert w.late_attachment_state == _AWAITING
        assert w.state != ew.WatchState.TRIGGERED
        assert w.state != ew.WatchState.INVALIDATED
    # PUT variant: canonical = bid; bid=0 means canonical unavailable.
    p = _make_awaiting_put(trigger=200.0)
    for _ in range(5):
        st = p.check(bid=0, ask=199.60)
        assert st == ew.WatchState.PENDING
        assert p.late_attachment_state == _AWAITING


def test_arm_time_missing_then_first_quote_inside_zone_enters_within_and_requires_two_confirms():
    """Arm-time quote missing → first available quote is inside continuation
    → enters WITHIN_CONTINUATION → requires two late-confirmation polls
    before the ordinary breach path may fire."""
    w = _make_awaiting_call(trigger=200.0)
    # First truthful quote inside continuation zone (ask=200.10, upper=200.15).
    w.check(bid=200.05, ask=200.10)
    assert w.late_attachment_state == _WITHIN
    assert w.late_confirm_polls == 0  # transition itself doesn't count
    # First confirming poll — still gated, no trigger.
    w.check(bid=200.05, ask=200.10)
    assert w.late_attachment_state == _WITHIN
    assert w.late_confirm_polls == 1
    assert w.state != ew.WatchState.TRIGGERED
    # Second confirming poll — gate clears; ordinary breach path takes over
    # on this same poll (breach_count reaches 1).
    w.check(bid=200.05, ask=200.10)
    assert w.late_attachment_state is None
    # Third poll runs ordinary breach path only; confirmed.
    st = w.check(bid=200.05, ask=200.10)
    assert st == ew.WatchState.TRIGGERED


def test_arm_time_missing_then_first_quote_pre_trigger_clears_gate_and_ordinary_breach_permitted():
    """Arm-time quote missing → first available quote is still pre-trigger
    (below trigger for CALL) → clears unresolved state → ordinary future
    breach remains permitted (two normal confirmation polls at/above
    trigger produce TRIGGERED)."""
    w = _make_awaiting_call(trigger=200.0)
    # First truthful quote: ask=199.50 — below trigger. Setup hasn't
    # breached; gate must release and let ordinary breach path own it.
    w.check(bid=199.45, ask=199.50)
    assert w.late_attachment_state is None
    assert w.state == ew.WatchState.PENDING
    # Ordinary future breach at trigger — two confirming polls fire normally.
    w.check(bid=200.00, ask=200.05)
    assert w.state == ew.WatchState.PENDING
    assert w.breach_count == 1
    w.check(bid=200.00, ask=200.05)
    assert w.state == ew.WatchState.TRIGGERED


# ── Both-bid-and-ask-zero arm-time path must still seed AWAITING ────────────
# Structural + composed proof for the outer-guard removal.
#
# The prior amendment wrapped the arm-time classifier call in
#   `if _bug_c_bid > 0 or _bug_c_ask > 0:`
# which meant a fully-missing quote payload silently skipped the entire
# late-attachment gate and armed the watcher with state=None. We prove:
#
#   1. STRUCTURAL — the guard is gone from the source and the classifier
#      call is entered unconditionally at the arm-time site.
#   2. CLASSIFIER — classify_late_attachment(bid=0, ask=0) yields TRUTH_RETRY
#      with quote=None (i.e. the arm-time gate's seed condition).
#   3. WIRING — seeding a WatchedSignal with that state and driving check()
#      with a next-poll quote far above trigger transitions to WAITING_RESET
#      and never enters the ordinary breach path.
#
# Together these prove the both-zero arm-time path is fixed end-to-end
# without needing to drive the full APEntryWatcher.watch() call graph.

import pathlib as _pathlib


def test_arm_time_outer_guard_removed_from_watcher_source():
    """Regression: the bypass wrapper must not return."""
    src = (_pathlib.Path(__file__).parent.parent / "ap_entry_watcher.py").read_text()
    assert "if _bug_c_bid > 0 or _bug_c_ask > 0" not in src, (
        "The both-zero arm-time bypass has returned. The late-attachment "
        "classifier must run unconditionally at arm time — even when both "
        "bid and ask come back as 0 — so AWAITING_FIRST_TRUTH is seeded and "
        "the poll loop's gate stays engaged."
    )


def test_arm_time_classifier_unconditional_call_shape_present_in_source():
    """The arm-time site must call classify_late_attachment with the raw
    _bug_c_bid / _bug_c_ask values — including when both are 0."""
    src = (_pathlib.Path(__file__).parent.parent / "ap_entry_watcher.py").read_text()
    # Both these markers exist inside the arm-time block only.
    assert "AWAITING_FIRST_TRUTH — " in src
    assert "bid=_bug_c_bid" in src and "ask=_bug_c_ask" in src


def test_classifier_returns_truth_retry_when_both_bid_and_ask_are_zero_call():
    """This is the classifier-side proof: (bid=0, ask=0) → TRUTH_RETRY with
    quote=None, which is exactly the shape the arm-time gate now seeds on."""
    from ap.pending_trigger_classifier import (
        classify_late_attachment,
        TRIGGER_TRUTH_UNAVAILABLE_RETRY,
    )
    d = classify_late_attachment(
        side="CALL", trigger_price=200.0, bid=0, ask=0,
    )
    assert d.classification == TRIGGER_TRUTH_UNAVAILABLE_RETRY
    assert d.quote is None


def test_classifier_returns_truth_retry_when_both_bid_and_ask_are_zero_put():
    from ap.pending_trigger_classifier import (
        classify_late_attachment,
        TRIGGER_TRUTH_UNAVAILABLE_RETRY,
    )
    d = classify_late_attachment(
        side="PUT", trigger_price=200.0, bid=0, ask=0,
    )
    assert d.classification == TRIGGER_TRUTH_UNAVAILABLE_RETRY
    assert d.quote is None


def test_both_zero_arm_time_wired_end_to_end_call():
    """CALL arm-time bid=0 AND ask=0 → seeded AWAITING → next poll far above
    → WAITING_RESET, never TRIGGERED. Combines the arm-time seed (produced
    directly from the classifier's TRUTH_RETRY result) with WatchedSignal
    gate behavior. This is the exact production shape the reviewer flagged."""
    w = _make_watcher(side="CALL", trigger=200.0, stop=180.0, state=_AWAITING)
    assert w.late_attachment_state == _AWAITING
    # First truthful poll: far above continuation (ask=200.40, upper=200.15).
    st = w.check(bid=200.35, ask=200.40)
    assert st == ew.WatchState.PENDING
    assert w.late_attachment_state == _WAITING
    assert w.state != ew.WatchState.TRIGGERED
    # Continued far-past-trigger price must stay in WAITING_RESET.
    for _ in range(3):
        st = w.check(bid=200.35, ask=200.40)
        assert st == ew.WatchState.PENDING
        assert w.late_attachment_state == _WAITING
        assert w.state != ew.WatchState.TRIGGERED


def test_both_zero_arm_time_wired_end_to_end_put():
    """PUT symmetric proof."""
    w = _make_watcher(side="PUT", trigger=200.0, stop=220.0, state=_AWAITING)
    assert w.late_attachment_state == _AWAITING
    st = w.check(bid=199.60, ask=199.65)   # far below 199.85 lower bound
    assert st == ew.WatchState.PENDING
    assert w.late_attachment_state == _WAITING
    assert w.state != ew.WatchState.TRIGGERED


# ── TRUTH_RETRY with a valid quote is NOT a late attachment ─────────────────
# Regression: the prior amendment seeded AWAITING_FIRST_TRUTH for every
# TRUTH_RETRY result, including the case where the classifier returned a
# valid pre-trigger canonical quote (CALL: ask<trigger; PUT: bid>trigger).
# That killed normal trade flow — a CALL armed at ask=199.90 for
# trigger=200 was placed in AWAITING and then transitioned to WAITING_RESET
# when the next poll arrived at ask=200.40, even though this was the
# ordinary "arm slightly early, breach on next tick" path.


def test_classifier_call_pre_trigger_quote_returns_truth_retry_with_quote_populated():
    """Classifier-side proof: a valid CALL ask just below trigger classifies
    as TRUTH_RETRY, but with .quote populated — distinguishing it from the
    canonical-quote-unavailable case (quote=None)."""
    from ap.pending_trigger_classifier import (
        classify_late_attachment,
        TRIGGER_TRUTH_UNAVAILABLE_RETRY,
    )
    d = classify_late_attachment(
        side="CALL", trigger_price=200.0, bid=199.85, ask=199.90,
    )
    assert d.classification == TRIGGER_TRUTH_UNAVAILABLE_RETRY
    assert d.quote is not None
    from decimal import Decimal
    assert d.quote == Decimal("199.90")


def test_classifier_put_pre_trigger_quote_returns_truth_retry_with_quote_populated():
    """PUT symmetric proof — bid above trigger."""
    from ap.pending_trigger_classifier import (
        classify_late_attachment,
        TRIGGER_TRUTH_UNAVAILABLE_RETRY,
    )
    d = classify_late_attachment(
        side="PUT", trigger_price=200.0, bid=200.10, ask=200.15,
    )
    assert d.classification == TRIGGER_TRUTH_UNAVAILABLE_RETRY
    assert d.quote is not None
    from decimal import Decimal
    assert d.quote == Decimal("200.10")


def test_arm_time_pre_trigger_quote_does_not_seed_late_attachment_state_source():
    """Structural: the arm-time site's AWAITING seed branch must be gated on
    `_late_decision.quote is None`. A valid pre-trigger quote must fall
    through to normal arming — no _late_attachment_seed written."""
    src = (_pathlib.Path(__file__).parent.parent / "ap_entry_watcher.py").read_text()
    assert "elif _late_cls == _PT_TRUTH_RETRY and _late_decision.quote is None:" in src, (
        "The AWAITING_FIRST_TRUTH seed must be gated on quote=None. "
        "A TRUTH_RETRY with a valid pre-trigger quote is the ordinary arm "
        "path and must NOT seed any late-attachment gate."
    )
    # And the pre-trigger branch must exist as its own elif.
    assert "elif _late_cls == _PT_TRUTH_RETRY:" in src


def test_call_arm_time_pre_trigger_then_first_poll_breach_uses_ordinary_path():
    """Wiring: watcher armed with no late seed (as the fixed arm-time gate
    would leave a valid pre-trigger case). First poll arrives at ask=200.40
    — the ordinary breach path must own it. On the second confirming poll
    the watcher must TRIGGER via the normal path, NOT enter WAITING_RESET."""
    # Simulate the fixed arm-time behavior: NO seed on the signal payload.
    w = ew.WatchedSignal(_signal(side="CALL", entry_price=100.0, stop_price=90.0), overnight=False)
    # Confirm the watcher was armed with no late state.
    assert w.late_attachment_state is None
    # First poll: canonical ask > trigger. Ordinary breach path starts.
    st1 = w.check(bid=100.35, ask=100.40)
    assert st1 == ew.WatchState.PENDING
    assert w.late_attachment_state is None
    assert w.breach_count == 1
    # Second poll: normal momentum confirmation fires — NOT WAITING_RESET.
    st2 = w.check(bid=100.35, ask=100.40)
    assert st2 == ew.WatchState.TRIGGERED
    assert w.late_attachment_state is None


def test_put_arm_time_pre_trigger_then_first_poll_breach_uses_ordinary_path():
    """PUT symmetric."""
    w = ew.WatchedSignal(_signal(side="PUT", entry_price=100.0, stop_price=110.0), overnight=False)
    assert w.late_attachment_state is None
    st1 = w.check(bid=99.60, ask=99.65)
    assert st1 == ew.WatchState.PENDING
    assert w.breach_count == 1
    st2 = w.check(bid=99.60, ask=99.65)
    assert st2 == ew.WatchState.TRIGGERED
    assert w.late_attachment_state is None


# ── Terminal short-circuits still apply when in AWAITING (defensive) ────────

def test_stop_broken_during_awaiting_terminalizes():
    w = _make_awaiting_call(trigger=200.0, stop=180.0)
    # First truthful quote: ask=180.00, at stop — CALL stop broken when ask<=stop.
    st = w.check(bid=179.95, ask=180.00)
    assert st == ew.WatchState.INVALIDATED


def test_signal_id_and_side_survive_gate_transitions():
    w = _make_watcher(side="CALL", trigger=100, state=_WITHIN)
    original_sid = w.signal_id
    original_side = w.side
    w.check(bid=99.98, ask=100.05)
    w.check(bid=99.98, ask=100.05)
    w.check(bid=99.98, ask=100.05)
    assert w.signal_id == original_sid
    assert w.side == original_side
