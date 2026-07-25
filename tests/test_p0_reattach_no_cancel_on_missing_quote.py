"""
tests/test_p0_reattach_no_cancel_on_missing_quote.py
=====================================================
PR #388 Blocker 2 — the REATTACH_WATCHER path calls
    watch(recovery_rearm=True, no_cancel_on_reject=True)
precisely so a temporary LIVE quote outage cannot destroy the exact
PENDING_TRIGGER order the amendment was designed to recover.

The prior amendment's `no_cancel_on_reject` flag protected only the later
add_signal() rejection cleanup. Before add_signal() was ever reached, the
LIVE regular-session recovery classifier at bid=ask=0 called
_terminalize_recovery_rearm_candidate → cancel_pending_entry — destroying
the exact order the caller claimed was preserved.

Fix: at the RECOVERY_REARM_QUOTE_UNAVAILABLE branch, when
_no_cancel_on_reject is True, log + return False WITHOUT any
_terminalize_recovery_rearm_candidate call.
"""
from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest

import ap_entry_watcher as ew


def _force_regular_live_session(monkeypatch):
    from datetime import datetime as _real_dt
    _fixed = _real_dt(2026, 7, 23, 10, 15, tzinfo=ZoneInfo("America/New_York"))

    class _FrozenDT(_real_dt):
        @classmethod
        def now(cls, tz=None):
            if tz is not None:
                return _fixed.astimezone(tz)
            return _fixed.replace(tzinfo=None)

    monkeypatch.setattr(ew, "datetime", _FrozenDT)


def _make_watcher_for_reattach(monkeypatch, *, quote_bid=0, quote_ask=0):
    import threading
    w = object.__new__(ew.APEntryWatcher)
    w._pending = []
    w._dedup_set = set()
    w._lock = threading.RLock()
    w._last_reject_reason = ""
    w.on_invalidate = None
    w.mode = "LIVE"           # regular-session LIVE recovery classifier
    w.broker = SimpleNamespace(session=None)
    w._last_quote_fetch_proof = {}
    w.owner_token = "test-owner"
    w.core = None

    # Spy on the OSM cancel_pending_entry call — must be 0 for the fix.
    cancel_spy = MagicMock()
    w.order_state_machine = SimpleNamespace(
        cancel_pending_entry=cancel_spy,
    )
    # Return the fake PENDING_TRIGGER order shape for recovery classifier.
    def _get_order(oid):
        return {
            "local_order_id":     oid,
            "status":             "PENDING_TRIGGER",
            "meta":               {"watcher_audit": {}},
            "client_id":          "jason@example.com",
            "execution_mode":     "live",
            "watcher_token":      "test-owner",
            "trigger_generation": 1,
        }
    w.order_state_machine.get_order = _get_order

    # Force the quote fetch to the requested (bid, ask).
    monkeypatch.setattr(w, "_get_quote", lambda _t: {"bid": quote_bid, "ask": quote_ask})
    monkeypatch.setattr(w, "_persist_watcher_audit", lambda *_a, **_kw: None)
    monkeypatch.setattr(w, "_is_live_runtime", lambda: True)
    # add_signal must never be called on the recovery-quote-unavailable path.
    add_spy = MagicMock(return_value=True)
    monkeypatch.setattr(w, "add_signal", add_spy)

    _force_regular_live_session(monkeypatch)

    return w, cancel_spy, add_spy


def _reattach_plan(*, local_order_id="local-existing-1"):
    return SimpleNamespace(
        signal_id="sig-reattach-1",
        canonical_signal_id="sig-reattach-1",
        ticker="SPY",
        side="CALL",
        direction="CALL",
        score=85.0,
        tier="A",
        timeframe="1d",
        entry_trigger=500.0,
        trigger_price=500.0,
        stop_underlying=490.0,
        target_underlying=520.0,
        pattern="2-1-2",
        contract_symbol=f"DEFERRED:SPY",
        contracts=2,
        limit_price=0.01,
        plan_id="plan-reattach-1",
        client_id="jason@example.com",
        execution_mode="live",
        prior_day_high=502.0,
        prior_day_low=498.0,
        late_attachment_policy_eligible=True,   # PR#388 seam
        metadata={
            "reattach_watcher":                 True,
            "overnight":                        True,
            "contract_deferred":                True,
            "materialization_generation":       1,
            "late_attachment_policy_eligible":  True,
            "execution_mode":                   "live",
            "client_id":                        "jason@example.com",
        },
    )


def test_reattach_with_missing_live_quote_does_not_cancel_existing_order(monkeypatch):
    """The reviewer's exact required test.

    Real APEntryWatcher, LIVE regular session, bid=0 AND ask=0,
    recovery_rearm=True + no_cancel_on_reject=True (the REATTACH contract).

    Contract:
      * cancel_pending_entry.call_count == 0
      * add_signal.call_count == 0
      * watch() returns False
    """
    w, cancel_spy, add_spy = _make_watcher_for_reattach(
        monkeypatch, quote_bid=0, quote_ask=0,
    )

    result = w.watch(
        _reattach_plan(),
        local_order_id="local-existing-1",
        recovery_rearm=True,
        no_cancel_on_reject=True,
    )

    assert result is False
    assert cancel_spy.call_count == 0, (
        "cancel_pending_entry was called despite no_cancel_on_reject=True "
        f"— the exact PENDING_TRIGGER order the REATTACH contract promised "
        f"to preserve has been destroyed. Called with: {cancel_spy.call_args_list}"
    )
    assert add_spy.call_count == 0, (
        "add_signal was called on the recovery-quote-unavailable path; "
        "the LIVE recovery classifier must return before add_signal."
    )


def test_reattach_with_valid_quotes_still_proceeds(monkeypatch):
    """Sanity: when quotes ARE available, the reattach path continues past
    the quote-unavailable branch. (add_signal will be reached; whether it
    ultimately succeeds is not the concern of THIS test — the concern is
    that we did not short-circuit at the quote-unavailable branch.)"""
    # Quotes safely below trigger — pre-trigger arm, WATCHING classification.
    w, cancel_spy, add_spy = _make_watcher_for_reattach(
        monkeypatch, quote_bid=499.80, quote_ask=499.90,
    )

    w.watch(
        _reattach_plan(),
        local_order_id="local-existing-2",
        recovery_rearm=True,
        no_cancel_on_reject=True,
    )

    # We don't require True/False here — only that the quote-unavailable
    # early return was NOT taken, i.e. add_signal was invoked.
    assert add_spy.call_count == 1, (
        "Reattach with valid quotes must proceed past the quote-unavailable "
        "branch and invoke add_signal."
    )
    # And cancel_pending_entry must still not fire.
    assert cancel_spy.call_count == 0


def test_non_recovery_arm_does_not_enter_recovery_classifier_at_all(monkeypatch):
    """Scope documentation: with recovery_rearm=False the LIVE recovery
    classifier branch (which is where _terminalize_recovery_rearm_candidate
    lives) is never entered. This test does NOT prove anything about legacy
    recovery callers — it only documents that the fix is confined to the
    explicit no-cancel contract path. See
    test_reattach_with_missing_live_quote_does_not_cancel_existing_order
    for the actual runtime proof of the fix."""
    w, cancel_spy, add_spy = _make_watcher_for_reattach(
        monkeypatch, quote_bid=0, quote_ask=0,
    )

    plan = _reattach_plan()
    plan.metadata["reattach_watcher"] = False
    result = w.watch(
        plan,
        local_order_id="local-non-recovery-1",
        recovery_rearm=False,
        no_cancel_on_reject=False,
    )
    # recovery_rearm=False → the whole LIVE recovery classifier branch is
    # unreachable; _terminalize_recovery_rearm_candidate cannot be called.
    assert cancel_spy.call_count == 0
