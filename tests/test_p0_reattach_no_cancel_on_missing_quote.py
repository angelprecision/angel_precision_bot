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

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest

import ap_entry_watcher as ew

# Dynamic signal date — keeps signal age below OVERNIGHT_SIGNAL_MAX_AGE_DAYS (4).
_SIG_DATE = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
_SIG_TS   = (datetime.now(timezone.utc) - timedelta(days=1)).strftime(
    "%Y-%m-%dT20:00:00+00:00"
)


def _force_regular_live_session(monkeypatch, watcher=None):
    """Force the runtime session predicates to think we're in the middle of
    regular session (well before EOD cutoff). Patches the module-level
    `datetime` AND stubs the two instance methods that gate on wall clock
    (belt-and-suspenders — different callsites resolve LOAD_GLOBAL in
    slightly different ways depending on whether the module has been
    reloaded in the test session)."""
    from datetime import datetime as _real_dt
    _fixed = _real_dt(2026, 7, 23, 10, 15, tzinfo=ZoneInfo("America/New_York"))

    class _FrozenDT(_real_dt):
        @classmethod
        def now(cls, tz=None):
            if tz is not None:
                return _fixed.astimezone(tz)
            return _fixed.replace(tzinfo=None)

    # Patch the exact base-module globals that super().watch() reads. The
    # production import is a shim package; patching only the shim's copied
    # datetime binding does not affect the arm-time gate in the base module.
    base_watch_globals = ew._BaseAPEntryWatcher.watch.__globals__
    monkeypatch.setitem(base_watch_globals, "datetime", _FrozenDT)
    monkeypatch.setattr(ew, "_datetime", _FrozenDT)
    if watcher is not None:
        monkeypatch.setattr(watcher, "_is_regular_session_now", lambda: True)
        monkeypatch.setattr(watcher, "_is_past_entry_cutoff_now", lambda: False)


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
    durable_authority = {}

    def _read_trigger_confirmation_authority(
        local_order_id,
        *,
        client_id,
        execution_mode,
        signal_id,
        canonical_signal_id,
        expected_materialization_generation=None,
    ):
        authority = durable_authority.get(local_order_id)
        if not authority:
            return None
        if (
            authority["client_id"].lower() != str(client_id or "").strip().lower()
            or authority["execution_mode"].lower()
            != str(execution_mode or "").strip().lower()
            or authority["signal_id"] != str(signal_id or "").strip()
            or authority["canonical_signal_id"] != str(canonical_signal_id or "").strip()
        ):
            return None
        if (
            expected_materialization_generation is not None
            and authority.get("materialization_generation")
            != expected_materialization_generation
        ):
            return None
        return dict(authority)

    w.order_state_machine = SimpleNamespace(
        cancel_pending_entry=cancel_spy,
        read_trigger_confirmation_authority=_read_trigger_confirmation_authority,
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

    _orig_watch = w.watch

    def _watch_with_durable_fixture(plan, local_order_id, **kwargs):
        meta = getattr(plan, "metadata", {}) or {}
        provenance = meta.get("trigger_crossed_at_provenance")
        if isinstance(provenance, dict) and meta.get("trigger_crossed_at"):
            durable_authority[local_order_id] = {
                "proven": True,
                "trigger_crossed_at": meta["trigger_crossed_at"],
                "trigger_crossed_at_provenance": dict(provenance),
                "materialization_generation": meta.get("materialization_generation"),
                "client_id": provenance.get("client_id", ""),
                "execution_mode": provenance.get("execution_mode", ""),
                "signal_id": str(getattr(plan, "signal_id", "") or "").strip(),
                "canonical_signal_id": provenance.get("canonical_signal_id", ""),
            }
        return _orig_watch(plan, local_order_id, **kwargs)

    w.watch = _watch_with_durable_fixture

    # Force the quote fetch to the requested (bid, ask).
    monkeypatch.setattr(w, "_get_quote", lambda _t: {"bid": quote_bid, "ask": quote_ask})
    monkeypatch.setattr(w, "_persist_watcher_audit", lambda *_a, **_kw: None)
    monkeypatch.setattr(w, "_is_live_runtime", lambda: True)
    # add_signal must never be called on the recovery-quote-unavailable path.
    add_spy = MagicMock(return_value=True)
    monkeypatch.setattr(w, "add_signal", add_spy)

    _force_regular_live_session(monkeypatch, watcher=w)

    return w, cancel_spy, add_spy


def _reattach_plan(*, local_order_id="local-existing-1", confirmed=False):
    """Build a SimpleNamespace plan for reattach tests.

    confirmed=True adds durable trigger_crossed_at evidence to the plan
    metadata.  PR #407 requires this for the scanner stop to be active;
    a real rearmed recovery restores it from the persisted order meta.
    Use confirmed=True for terminal-truth test cases (stop/target already
    broken) and the default False for pre-breach or quote-unavailable cases.
    """
    meta = {
        "reattach_watcher":                 True,
        "overnight":                        True,
        "contract_deferred":                True,
        "materialization_generation":       1,
        "late_attachment_policy_eligible":  True,
        "execution_mode":                   "live",
        "client_id":                        "jason@example.com",
    }
    if confirmed:
        # Durable first-breach evidence — always present in a real rearmed
        # recovery row that reached PENDING_TRIGGER after confirmation.
        meta["trigger_crossed_at"] = (
            datetime.now(timezone.utc) - timedelta(minutes=10)
        ).isoformat()
        meta["trigger_crossed_at_provenance"] = {
            "canonical_signal_id": "sig-reattach-1",
            "client_id": "jason@example.com",
            "execution_mode": "live",
            "local_order_id": local_order_id,
            "materialization_generation": 1,
        }
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
        contract_symbol="DEFERRED:SPY",
        contracts=2,
        limit_price=0.01,
        plan_id="plan-reattach-1",
        client_id="jason@example.com",
        execution_mode="live",
        prior_day_high=502.0,
        prior_day_low=498.0,
        late_attachment_policy_eligible=True,
        metadata=meta,
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


@pytest.mark.parametrize(
    ("bid", "ask", "expected_reason"),
    [
        (489.90, 500.05, "stop_already_broken_terminal"),
        (519.90, 520.00, "target_already_complete_terminal"),
        (508.00, 508.00, "late_attachment_move_missed_terminal"),
    ],
)
def test_reattach_with_proven_terminal_truth_terminalizes_existing_order_once(
    monkeypatch, bid, ask, expected_reason,
):
    """Proven STOP / TARGET / decisive-missed truth is not protected by
    no_cancel_on_reject. The exact recovered PENDING_TRIGGER row must be
    terminalized once so the outer no-filter reread can persist suppression."""
    w, cancel_spy, add_spy = _make_watcher_for_reattach(
        monkeypatch, quote_bid=bid, quote_ask=ask,
    )

    result = w.watch(
        _reattach_plan(
            local_order_id="local-existing-terminal", confirmed=True
        ),   # real rearmed recovery has trigger_crossed_at + provenance
        local_order_id="local-existing-terminal",
        recovery_rearm=True,
        no_cancel_on_reject=True,
    )

    assert result is False
    add_spy.assert_not_called()
    cancel_spy.assert_called_once()
    assert cancel_spy.call_args.args[0] == "local-existing-terminal"
    assert expected_reason in cancel_spy.call_args.kwargs["reason"]


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
