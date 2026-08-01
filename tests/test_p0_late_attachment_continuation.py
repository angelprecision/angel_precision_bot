"""
tests/test_p0_late_attachment_continuation.py
==============================================
PR #388 Block-2 — canonical trigger-continuation helper + late-attachment
classifier. Pure-function tests for ap/pending_trigger_classifier helpers.

Watcher-integration tests live in
tests/test_p0_late_attachment_watcher_integration.py (Commit B).
"""
from __future__ import annotations

import importlib
import os
from decimal import Decimal

import pytest

import ap.pending_trigger_classifier as ptc


# ── compute_allowed_continuation ─────────────────────────────────────────────

@pytest.mark.parametrize(
    "trigger,expected",
    [
        (Decimal("200.00"), Decimal("0.15")),   # ABS cap dominates
        (Decimal("100.00"), Decimal("0.075")),  # BPS  cap dominates
        (Decimal("50.00"),  Decimal("0.0375")),
        (Decimal("400.00"), Decimal("0.15")),   # still ABS-capped
    ],
)
def test_compute_allowed_continuation_matches_spec_examples(trigger, expected):
    assert ptc.compute_allowed_continuation(trigger) == expected


def test_compute_allowed_continuation_returns_none_for_invalid_inputs():
    assert ptc.compute_allowed_continuation(None) is None
    assert ptc.compute_allowed_continuation(0) is None
    assert ptc.compute_allowed_continuation(-1) is None
    assert ptc.compute_allowed_continuation("nan") is None
    assert ptc.compute_allowed_continuation("abc") is None


def test_compute_allowed_continuation_accepts_float_and_string():
    assert ptc.compute_allowed_continuation(100.0) == Decimal("0.075")
    assert ptc.compute_allowed_continuation("100") == Decimal("0.075")


def test_env_override_applies_at_import(monkeypatch):
    monkeypatch.setenv("ENTRY_TRIGGER_CONTINUATION_MAX_ABS", "0.50")
    monkeypatch.setenv("ENTRY_TRIGGER_CONTINUATION_MAX_BPS", "20")
    reloaded = importlib.reload(ptc)
    try:
        assert reloaded.ENTRY_TRIGGER_CONTINUATION_MAX_ABS == Decimal("0.50")
        assert reloaded.compute_allowed_continuation(Decimal("200")) == Decimal("0.40")
    finally:
        monkeypatch.delenv("ENTRY_TRIGGER_CONTINUATION_MAX_ABS", raising=False)
        monkeypatch.delenv("ENTRY_TRIGGER_CONTINUATION_MAX_BPS", raising=False)
        importlib.reload(ptc)


# ── compute_reset_tolerance ──────────────────────────────────────────────────

@pytest.mark.parametrize(
    "trigger,expected",
    [
        (Decimal("200.00"), Decimal("0.075")),  # 0.15 * 0.5
        (Decimal("100.00"), Decimal("0.0375")), # 0.075 * 0.5
        (Decimal("2.00"),   Decimal("0.01")),   # floor at 0.01
        (Decimal("1.00"),   Decimal("0.01")),   # tiny trigger clamps to floor
    ],
)
def test_compute_reset_tolerance(trigger, expected):
    assert ptc.compute_reset_tolerance(trigger) == expected


def test_compute_reset_tolerance_returns_none_for_invalid():
    assert ptc.compute_reset_tolerance(None) is None
    assert ptc.compute_reset_tolerance(0) is None


# ── _canonical_trigger_quote ─────────────────────────────────────────────────

def test_canonical_trigger_quote_call_uses_ask():
    r = ptc._canonical_trigger_quote(side="CALL", bid=99.90, ask=100.10)
    assert r.available is True
    assert r.value == Decimal("100.10")
    assert r.source == "ask"
    assert r.reason_code == ptc.TRIGGER_QUOTE_AVAILABLE


def test_canonical_trigger_quote_put_uses_bid():
    r = ptc._canonical_trigger_quote(side="PUT", bid=99.80, ask=100.20)
    assert r.available is True
    assert r.value == Decimal("99.80")
    assert r.source == "bid"


def test_canonical_trigger_quote_call_missing_ask_is_unavailable_no_fallback():
    r = ptc._canonical_trigger_quote(side="CALL", bid=100.50, ask=0)
    assert r.available is False
    assert r.value is None
    assert r.source == "ask"
    assert r.reason_code == ptc.TRIGGER_QUOTE_UNAVAILABLE


def test_canonical_trigger_quote_put_missing_bid_is_unavailable_no_fallback():
    r = ptc._canonical_trigger_quote(side="PUT", bid=None, ask=99.50)
    assert r.available is False
    assert r.value is None
    assert r.source == "bid"


def test_canonical_trigger_quote_invalid_side():
    r = ptc._canonical_trigger_quote(side="STRADDLE", bid=1, ask=2)
    assert r.available is False
    assert r.reason_code == ptc.TRIGGER_SIDE_INVALID


# ── classify_late_attachment: 16 required cases ──────────────────────────────

def _decide(**kwargs):
    return ptc.classify_late_attachment(**kwargs)


# Case 1: CALL trigger 200.00, attachment at 200.10 — within continuation.
def test_case1_call_within_continuation_zone():
    d = _decide(side="CALL", trigger_price=200, bid=200.05, ask=200.10)
    assert d.classification == ptc.LATE_ATTACHMENT_WITHIN_CONTINUATION
    assert d.quote_source == "ask"
    assert d.allowed_continuation == Decimal("0.15")


# Case 2: CALL trigger 200.00, attachment at 200.15 — accepted at boundary.
def test_case2_call_at_upper_boundary_still_within():
    d = _decide(side="CALL", trigger_price=200, bid=200.10, ask=200.15)
    assert d.classification == ptc.LATE_ATTACHMENT_WITHIN_CONTINUATION


# Case 3: CALL trigger 200.00, attachment at 200.16 — outside zone.
def test_case3_call_one_cent_past_boundary_is_waiting_reset():
    d = _decide(side="CALL", trigger_price=200, bid=200.11, ask=200.16)
    assert d.classification == ptc.MISSED_LATE_WATCHER_ATTACHMENT_WAITING_RESET


def test_case3b_call_decisive_drift_exceeded_is_terminal():
    d = _decide(
        side="CALL", trigger_price=200, bid=200.11, ask=200.16,
        decisive_drift_exceeded=True,
    )
    assert d.classification == ptc.LATE_ATTACHMENT_MOVE_MISSED_TERMINAL


# Case 4: PUT trigger 200.00, attachment at 199.90 — accepted after confirmation.
def test_case4_put_within_continuation_zone():
    d = _decide(side="PUT", trigger_price=200, bid=199.90, ask=199.95)
    assert d.classification == ptc.LATE_ATTACHMENT_WITHIN_CONTINUATION
    assert d.quote_source == "bid"


# Case 5: PUT trigger 200.00, attachment at 199.85 — accepted at boundary.
def test_case5_put_at_lower_boundary_still_within():
    d = _decide(side="PUT", trigger_price=200, bid=199.85, ask=199.90)
    assert d.classification == ptc.LATE_ATTACHMENT_WITHIN_CONTINUATION


# Case 6: PUT trigger 200.00, attachment at 199.84 — outside zone.
def test_case6_put_one_cent_past_boundary_is_waiting_reset():
    d = _decide(side="PUT", trigger_price=200, bid=199.84, ask=199.89)
    assert d.classification == ptc.MISSED_LATE_WATCHER_ATTACHMENT_WAITING_RESET


# Case 7: one-poll quote spike — a single call gives only ONE decision;
# the two-poll requirement is enforced by the watcher, not the classifier.
# What we assert here: the classifier itself never returns "confirmed" from a
# single observation — only WITHIN or WAITING_RESET.
def test_case7_single_poll_does_not_confirm():
    for _ in range(3):
        d = _decide(side="CALL", trigger_price=200, bid=200.05, ask=200.10)
        assert d.classification == ptc.LATE_ATTACHMENT_WITHIN_CONTINUATION
        # Confirmed states are watcher-controlled; helper never emits them.
        assert d.classification != ptc.LATE_CONTINUATION_CONFIRMED
        assert d.classification != ptc.LATE_ATTACHMENT_REBREACH_CONFIRMED


# Case 8: stale quote inside tolerance — classifier is pure and quote-freshness
# is the caller's contract. We assert that a *missing* quote never yields a
# non-terminal classification (retry only).
def test_case8_missing_canonical_quote_is_retryable_no_terminalization():
    d = _decide(side="CALL", trigger_price=200, bid=200.10, ask=0)
    assert d.classification == ptc.TRIGGER_TRUTH_UNAVAILABLE_RETRY


# Case 9: stop broken — CALL stop uses BID (not the canonical trigger quote).
def test_case9_call_stop_broken_is_terminal():
    # CALL stop broken when bid <= stop. Trigger side (ask) is irrelevant.
    d = _decide(side="CALL", trigger_price=200, bid=194.90, ask=195.10,
                stop=195.00, trigger_previously_breached=True)
    assert d.classification == ptc.STOP_ALREADY_BROKEN_TERMINAL


def test_case9b_put_stop_broken_is_terminal():
    # PUT stop broken when ask >= stop. Trigger side (bid) is irrelevant.
    d = _decide(side="PUT", trigger_price=200, bid=204.90, ask=205.10,
                stop=205.00, trigger_previously_breached=True)
    assert d.classification == ptc.STOP_ALREADY_BROKEN_TERMINAL


# ── Spread-shaped stop cases: bid and ask disagree ─────────────────────────
# The prior implementation used the canonical trigger quote (ask for CALL,
# bid for PUT) to evaluate stop. That silently hid real stop breaks in every
# spread where the stop side had crossed but the trigger side had not.

def test_call_stop_broken_when_bid_at_or_below_stop_even_if_ask_above():
    """CALL stop=195, bid=194.90 (broken), ask=195.10 (not broken by ask).
    Must terminalize on bid-side break — the trigger-side ask must not
    rescue the setup from a real stop break."""
    d = _decide(side="CALL", trigger_price=200, bid=194.90, ask=195.10,
                stop=195.00, trigger_previously_breached=True)
    assert d.classification == ptc.STOP_ALREADY_BROKEN_TERMINAL


def test_call_stop_not_broken_when_bid_above_stop_even_if_ask_below():
    """Symmetric: ask below stop but bid above. Stop is NOT broken because
    the actionable exit price (bid) is still above stop."""
    d = _decide(side="CALL", trigger_price=200, bid=195.10, ask=194.90,
                stop=195.00)
    assert d.classification != ptc.STOP_ALREADY_BROKEN_TERMINAL


def test_put_stop_broken_when_ask_at_or_above_stop_even_if_bid_below():
    """PUT stop=205, ask=205.10 (broken), bid=204.90 (not broken by bid).
    Must terminalize on ask-side break."""
    d = _decide(side="PUT", trigger_price=200, bid=204.90, ask=205.10,
                stop=205.00, trigger_previously_breached=True)
    assert d.classification == ptc.STOP_ALREADY_BROKEN_TERMINAL


def test_put_stop_not_broken_when_ask_below_stop_even_if_bid_above():
    d = _decide(side="PUT", trigger_price=200, bid=205.10, ask=204.90,
                stop=205.00)
    assert d.classification != ptc.STOP_ALREADY_BROKEN_TERMINAL


def test_call_stop_missing_stop_side_quote_returns_truth_retry_with_quote_none():
    """Missing bid (stop side for CALL) must NOT claim a stop break AND
    must NOT let the classifier continue into WITHIN/WAITING/pre-trigger.
    Return TRIGGER_TRUTH_UNAVAILABLE_RETRY with quote=None so the watcher
    seeds/preserves AWAITING_FIRST_TRUTH."""
    # Ask is inside continuation zone (would otherwise be WITHIN); the
    # missing stop-side bid must override into retry.
    d = _decide(side="CALL", trigger_price=200, bid=0, ask=200.10,
                stop=195.00)
    assert d.classification == ptc.TRIGGER_TRUTH_UNAVAILABLE_RETRY
    assert d.quote is None, (
        "Missing stop-side quote must yield quote=None so the watcher "
        "seeds AWAITING_FIRST_TRUTH; a non-None quote would allow the "
        "arm-time gate to fall through to normal arming."
    )


def test_put_stop_missing_stop_side_quote_returns_truth_retry_with_quote_none():
    d = _decide(side="PUT", trigger_price=200, bid=199.90, ask=0,
                stop=205.00)
    assert d.classification == ptc.TRIGGER_TRUTH_UNAVAILABLE_RETRY
    assert d.quote is None


def test_missing_stop_side_quote_with_no_stop_configured_does_not_retry():
    """Sanity: if no stop is configured, a missing stop-side quote is not a
    problem — the classifier can proceed with just the trigger side."""
    d = _decide(side="CALL", trigger_price=200, bid=0, ask=200.10)  # no stop
    assert d.classification == ptc.LATE_ATTACHMENT_WITHIN_CONTINUATION


def test_call_stop_missing_stop_side_quote_when_would_have_been_waiting_reset():
    """A CALL past the continuation zone with missing bid must also seed
    retry (not silently transition to WAITING_RESET without stop truth)."""
    d = _decide(side="CALL", trigger_price=200, bid=0, ask=200.50,
                stop=195.00)
    assert d.classification == ptc.TRIGGER_TRUTH_UNAVAILABLE_RETRY
    assert d.quote is None


# Case 10: target complete.
def test_case10_target_complete_is_terminal_regardless_of_quote():
    d = _decide(side="CALL", trigger_price=200, bid=200.05, ask=200.10,
                target_complete=True)
    assert d.classification == ptc.TARGET_ALREADY_COMPLETE_TERMINAL


# Case 11: late attachment resets for two polls and rebreaches — this is a
# watcher-orchestrated flow. Here we assert the primitive: is_reset_confirmed
# returns True when the quote satisfies the reset condition.
def test_case11_is_reset_confirmed_call_returns_true_at_or_below_threshold():
    # trigger=200 → allowed=0.15 → reset_tolerance=max(0.01, 0.075)=0.075
    # CALL reset: ask <= 200 - 0.075 = 199.925
    assert ptc.is_reset_confirmed(
        side="CALL", trigger_price=200, canonical_quote=Decimal("199.925")
    ) is True
    assert ptc.is_reset_confirmed(
        side="CALL", trigger_price=200, canonical_quote=Decimal("199.93")
    ) is False


def test_case11b_is_reset_confirmed_put():
    # PUT reset: bid >= 200 + 0.075 = 200.075
    assert ptc.is_reset_confirmed(
        side="PUT", trigger_price=200, canonical_quote=Decimal("200.075")
    ) is True
    assert ptc.is_reset_confirmed(
        side="PUT", trigger_price=200, canonical_quote=Decimal("200.07")
    ) is False


# Case 12: restart during WAITING_RESET — restart-recovery is a watcher/
# persistence concern; here we assert the classifier is pure (same inputs
# yield same output) so any recovered state re-evaluates identically.
def test_case12_classifier_is_pure_same_input_same_output():
    kwargs = dict(side="CALL", trigger_price=200, bid=200.11, ask=200.16)
    first = _decide(**kwargs)
    second = _decide(**kwargs)
    assert first.classification == second.classification
    assert first.allowed_continuation == second.allowed_continuation


# Case 13: one ticker waiting for reset does not block other tickers — the
# helper has no cross-ticker state, so this is a watcher-loop invariant.
# We assert the helper carries no module-level mutable state that could leak.
def test_case13_classifier_has_no_module_level_mutable_state():
    # Interleave two tickers with different classifications; the WAITING_RESET
    # of one must not influence the WITHIN_CONTINUATION of the other.
    a = _decide(side="CALL", trigger_price=200, bid=200.05, ask=200.10)
    b = _decide(side="PUT",  trigger_price=100, bid=99.80, ask=99.85)  # past-boundary
    a2 = _decide(side="CALL", trigger_price=200, bid=200.05, ask=200.10)
    b2 = _decide(side="PUT",  trigger_price=100, bid=99.80, ask=99.85)
    assert a.classification == ptc.LATE_ATTACHMENT_WITHIN_CONTINUATION
    assert b.classification == ptc.MISSED_LATE_WATCHER_ATTACHMENT_WAITING_RESET
    assert a2.classification == a.classification
    assert b2.classification == b.classification


# Case 14: exact PAPER/LIVE isolation — the classifier has no mode concept
# (mode isolation belongs to the disposition resolver, not the trigger math).
# Assert the helper doesn't read env/globals that could leak between modes.
def test_case14_helper_does_not_read_execution_mode():
    # Set noise env vars; result must be identical.
    prior = os.environ.get("EXECUTION_MODE")
    os.environ["EXECUTION_MODE"] = "LIVE"
    try:
        live = _decide(side="CALL", trigger_price=200, bid=200.05, ask=200.10)
    finally:
        os.environ["EXECUTION_MODE"] = "PAPER"
        paper = _decide(side="CALL", trigger_price=200, bid=200.05, ask=200.10)
        if prior is None:
            del os.environ["EXECUTION_MODE"]
        else:
            os.environ["EXECUTION_MODE"] = prior
    assert live.classification == paper.classification


# Case 15: existing LOOKUP_FAILED behavior remains fail-closed — the
# classifier has no lookup, but assert TRIGGER_TRUTH_UNAVAILABLE_RETRY is in
# the RETRYABLE taxonomy so downstream is_safe_to_recovery_rearm treats it
# safely (never terminal).
def test_case15_trigger_truth_unavailable_maps_to_retryable_taxonomy():
    assert ptc.classify_watcher_reason("trigger_truth_unavailable_retry") == \
        ptc.WatcherInvalidationClass.RETRYABLE


# Case 16: existing REATTACH_WATCHER behavior remains idempotent — the new
# terminal reason codes are in the TERMINAL taxonomy so callers of
# _reason_is_invalidation still treat them correctly.
def test_case16_new_terminal_reasons_are_in_terminal_taxonomy():
    for reason in (
        "late_attachment_move_missed_terminal",
        "stop_already_broken_terminal",
        "target_already_complete_terminal",
    ):
        assert ptc.classify_watcher_reason(reason) == \
            ptc.WatcherInvalidationClass.TERMINAL
        assert ptc._reason_is_invalidation(reason) is True


# ── Extra safety: canonical helper is the single source of truth ─────────────

def test_helper_and_reset_tolerance_derive_from_same_constant():
    """compute_reset_tolerance is derived from compute_allowed_continuation.
    Changing MAX_ABS via env must move both, not just one."""
    allowed = ptc.compute_allowed_continuation(Decimal("200"))
    reset = ptc.compute_reset_tolerance(Decimal("200"))
    assert reset == max(Decimal("0.01"), allowed * Decimal("0.5"))


def test_ordinary_call_breach_and_continuation_share_canonical_quote():
    """Both the ordinary breach check and the continuation check use the same
    canonical trigger quote (ask for CALL). Assert the helper returns the
    same value regardless of who asks."""
    r = ptc._canonical_trigger_quote(side="CALL", bid=100.05, ask=100.20)
    d = _decide(side="CALL", trigger_price=100, bid=100.05, ask=100.20)
    assert d.quote == r.value
    assert d.quote_source == r.source
