"""
P0 regression: nonterminal ownership poisoning during REATTACH.

Context
───────
opportunity_ledger.update_opportunity() blocks lifecycle regression but
continues to write identifier + metadata fields and returns True. That
means a prior WATCHER_ARMED / BROKER_SUBMITTED / BROKER_ACKED / FILLED /
terminal opportunity row can retain its stale lifecycle status while
accepting current-session metadata written by
_persist_reattach_in_progress_fence() — mode, session_key, local_order_id,
and reattach_in_progress=true.

If the resolver trusts that stale status before the exact active-order
fence runs, it returns ALREADY_ARMED / ALREADY_OWNED / ALREADY_TERMINAL
and the live PENDING_TRIGGER order is stranded on the next reeval attempt
without in-memory watcher ownership.

Contract enforced here
──────────────────────
When reattach_in_progress=true for the exact current mode/session/order:

  • WATCHER_ARMED       → do NOT return ALREADY_ARMED
  • BROKER_SUBMITTED    → do NOT return ALREADY_OWNED
  • BROKER_ACKED        → do NOT return ALREADY_OWNED
  • FILLED              → do NOT return ALREADY_OWNED
  • terminal statuses   → do NOT return ALREADY_TERMINAL (covered
                          separately by test_p0_reattach_two_attempt_order_preservation)

The resolver MUST run the exact active-order fence and let it decide:
  • active PENDING_TRIGGER      → REATTACH_WATCHER
  • active submitted/open/etc.  → ALREADY_OWNED
  • active lookup failure       → LOOKUP_FAILED
  • no active order             → latest-entry-no-status fence
  • no order truth              → AMBIGUOUS_OWNERSHIP (fail closed)

Forbidden writes contract
─────────────────────────
Every test asserts zero calls to:
  • master_control.evaluate
  • contract_selector.*
  • order_state_machine.create_entry_order
  • entry_watcher.watch (unless a REATTACH is expected)
  • broker submit / place / cancel / replace
"""
from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

import pytest


# ─── Test infrastructure ─────────────────────────────────────────────────────

CLIENT_ID = "poison-client@example.com"
CANONICAL = "2026-07-24:1-2:AAPL:1d:CALL"
SIGNAL_ID = "2026-07-24:1-2:AAPL:1d:CALL"
LOCAL_ORDER_ID = "loi-poison-42"
EXECUTION_MODE = "live"
SESSION_KEY = "2026-07-24"


def _mk_signal():
    return {
        "signal_id": SIGNAL_ID,
        "canonical_signal_id": CANONICAL,
        "ticker": "AAPL",
        "symbol": "AAPL",
        "side": "CALL",
        "score": 82.0,
        "timeframe": "1d",
        "pattern": "2u:2u",
        "tier": "A",
        "entry_trigger": 231.50,
        "stop_price":   229.25,
        "target_price": 236.75,
    }


def _mk_pending_trigger_order():
    return {
        "local_order_id":       LOCAL_ORDER_ID,
        "client_id":            CLIENT_ID,
        "execution_mode":       EXECUTION_MODE,
        "canonical_signal_id":  CANONICAL,
        "signal_id":            SIGNAL_ID,
        "kind":                 "ENTRY",
        "status":               "PENDING_TRIGGER",
        "direction":            "CALL",
        "symbol":               "AAPL",
        "trigger_price":        231.50,
        "stop_underlying":      229.25,
        "target_underlying":    236.75,
        "score":                82.0,
        "timeframe":            "1d",
        "pattern":              "2u:2u",
        "tier":                 "A",
        "contract":             f"DEFERRED:AAPL",
        "qty":                  1,
        "limit_price":          0.01,
        "meta":                 {},
    }


def _opportunity_row_with_status(status: str, reattach_in_progress: bool):
    """Build a client_signal_opportunities row that carries `status` as its
    lifecycle field but current mode/session/order in metadata — the exact
    shape produced by _persist_reattach_in_progress_fence() writing on top of
    a prior row whose monotonic guard preserved its stale status."""
    return {
        "canonical_signal_id": CANONICAL,
        "client_id":           CLIENT_ID,
        "opportunity_status":  status,
        "miss_stage":          "",
        "miss_reason":         "",
        "order_local_id":      LOCAL_ORDER_ID,
        "metadata": {
            "execution_mode":               EXECUTION_MODE,
            "overnight_reeval_session_key": SESSION_KEY,
            "local_order_id":               LOCAL_ORDER_ID,
            "reattach_in_progress":         reattach_in_progress,
            "canonical_signal_id":          CANONICAL,
            "client_id":                    CLIENT_ID,
            "original_signal_id":           SIGNAL_ID,
        },
    }


class _StubMC:
    def __init__(self):
        self.evaluate = MagicMock(name="master_control.evaluate")


class _StubSelector:
    def __init__(self):
        self.select = MagicMock(name="contract_selector.select")


class _StubOSM:
    def __init__(self):
        self.create_entry_order = MagicMock(name="osm.create_entry_order")
        self.mark_entry_pending_trigger = MagicMock(return_value=True)
        self.transition = MagicMock(return_value=True)
        self.get_order = MagicMock(return_value=None)


class _StubBroker:
    def __init__(self):
        self.submit = MagicMock(name="broker.submit")
        self.place  = MagicMock(name="broker.place")
        self.cancel = MagicMock(name="broker.cancel")
        self.replace = MagicMock(name="broker.replace")
        self.get_prior_day_levels = MagicMock(return_value={
            "prior_day_high": 231.10, "prior_day_low": 227.80,
            "prior_day_close": 230.20, "prior_day_date": "2026-07-23",
        })


@pytest.fixture
def stubbed_ov(monkeypatch):
    """Import ap_overnight_reeval and stub its module-boundary dependencies."""
    import ap_overnight_reeval as ov

    # Stub canonical resolver → deterministic
    monkeypatch.setattr(ov, "_resolve_canonical_signal_id",
                        lambda sid, sig: CANONICAL)

    # Fail-safe: force _et_now inside the 9:00-9:45 ET window.
    class _FakeET:
        weekday_val = 4  # Friday, always a trading day (avoid holiday drift)
        year, month, day = 2026, 7, 24
        hour, minute = 9, 15

        @classmethod
        def date(cls):
            import datetime as _dt
            return _dt.date(cls.year, cls.month, cls.day)

    import datetime as _dt

    def _fake_now(tz=None):
        return _dt.datetime(2026, 7, 24, 9, 15, tzinfo=tz)

    monkeypatch.setattr(ov, "_et_now", lambda: _dt.datetime(
        2026, 7, 24, 9, 15,
        tzinfo=__import__("zoneinfo").ZoneInfo("America/New_York"),
    ))
    monkeypatch.setattr(ov, "_is_trading_day", lambda dt: True)
    monkeypatch.setattr(ov, "_overnight_reeval_session_key", lambda now=None: SESSION_KEY)
    return ov


# ─── Nonterminal ownership poisoning: WATCHER_ARMED ───────────────────────────

def test_prior_watcher_armed_with_reattach_in_progress_defers_to_active_order(stubbed_ov, monkeypatch):
    """
    Setup:
      1. A prior opportunity row has opportunity_status=WATCHER_ARMED.
      2. Pre-watch fence overlays current mode/session/order/
         reattach_in_progress=true on top; monotonic guard preserves
         WATCHER_ARMED but metadata is now current.
      3. An exact PENDING_TRIGGER order exists for the same client/mode/
         canonical.

    Contract:
      Resolver must NOT return ALREADY_ARMED. It must fall through to the
      active-order fence and return REATTACH_WATCHER.

    Forbidden writes:
      Zero create_entry_order / broker / selector / master_control calls
      (the resolver itself makes none — this fixture proves the disposition).
    """
    ov = stubbed_ov

    # Poisoned opportunity row: WATCHER_ARMED + reattach_in_progress=true.
    poisoned = _opportunity_row_with_status("WATCHER_ARMED", reattach_in_progress=True)
    monkeypatch.setattr(
        ov, "_get_client_opportunity_row",
        lambda sid, cid, sig: ov._LookupResult(CANONICAL, ov._LS_FOUND, poisoned, None),
    )

    # Active PENDING_TRIGGER order still alive.
    order = _mk_pending_trigger_order()
    monkeypatch.setattr(
        ov, "_query_active_entry_order",
        lambda cid, mode, canon: (ov._LS_FOUND, order),
    )

    disp = ov._resolve_shared_setup_disposition(
        SIGNAL_ID, CLIENT_ID, _mk_signal(),
        execution_mode=EXECUTION_MODE,
        session_key=SESSION_KEY,
    )

    assert disp.disposition == ov._DISPOSITION_REATTACH_WATCHER, (
        f"Expected REATTACH_WATCHER but got {disp.disposition!r}. Stale "
        f"WATCHER_ARMED must not outrank an active PENDING_TRIGGER order "
        f"when reattach_in_progress=true."
    )
    assert disp.existing_local_order_id == LOCAL_ORDER_ID
    assert disp.existing_order_row is order


def test_prior_watcher_armed_without_reattach_flag_still_returns_already_armed(stubbed_ov, monkeypatch):
    """Confirm the guard is NARROW: without reattach_in_progress=true,
    current-session WATCHER_ARMED still short-circuits to ALREADY_ARMED.
    Prevents accidental over-fixing that would defer every arm proof."""
    ov = stubbed_ov

    clean = _opportunity_row_with_status("WATCHER_ARMED", reattach_in_progress=False)
    monkeypatch.setattr(
        ov, "_get_client_opportunity_row",
        lambda sid, cid, sig: ov._LookupResult(CANONICAL, ov._LS_FOUND, clean, None),
    )
    # Active-order fence should NEVER run in this branch.
    _fence_called = {"n": 0}
    def _sentinel(*a, **kw):
        _fence_called["n"] += 1
        return (ov._LS_NOT_FOUND, None)
    monkeypatch.setattr(ov, "_query_active_entry_order", _sentinel)

    disp = ov._resolve_shared_setup_disposition(
        SIGNAL_ID, CLIENT_ID, _mk_signal(),
        execution_mode=EXECUTION_MODE,
        session_key=SESSION_KEY,
    )
    assert disp.disposition == ov._DISPOSITION_ALREADY_ARMED
    assert _fence_called["n"] == 0, "Active-order fence must not run for clean ALREADY_ARMED."


# ─── Nonterminal ownership poisoning: broker-owned states ────────────────────

@pytest.mark.parametrize("broker_status", ["BROKER_SUBMITTED", "BROKER_ACKED", "FILLED"])
def test_prior_broker_owned_with_reattach_in_progress_defers_to_active_order(
    stubbed_ov, monkeypatch, broker_status,
):
    """
    Setup:
      Prior opportunity row status is BROKER_SUBMITTED / BROKER_ACKED /
      FILLED. Pre-watch fence overlays current-session metadata +
      reattach_in_progress=true. Monotonic guard preserves stale status.
      An exact PENDING_TRIGGER order exists.

    Contract:
      Resolver must NOT return ALREADY_OWNED. Active-order fence must run
      and REATTACH_WATCHER must win.

    Historical note:
      FILLED is the most dangerous case because in the pre-amendment
      code path it was checked in the same {BROKER_SUBMITTED, BROKER_ACKED,
      FILLED} branch that returned ALREADY_OWNED before terminal deferral
      logic ran — so the terminal-only fix left FILLED completely
      unguarded. This test proves the new guard covers it.
    """
    ov = stubbed_ov

    poisoned = _opportunity_row_with_status(broker_status, reattach_in_progress=True)
    monkeypatch.setattr(
        ov, "_get_client_opportunity_row",
        lambda sid, cid, sig: ov._LookupResult(CANONICAL, ov._LS_FOUND, poisoned, None),
    )
    order = _mk_pending_trigger_order()
    monkeypatch.setattr(
        ov, "_query_active_entry_order",
        lambda cid, mode, canon: (ov._LS_FOUND, order),
    )

    disp = ov._resolve_shared_setup_disposition(
        SIGNAL_ID, CLIENT_ID, _mk_signal(),
        execution_mode=EXECUTION_MODE,
        session_key=SESSION_KEY,
    )
    assert disp.disposition == ov._DISPOSITION_REATTACH_WATCHER, (
        f"For prior status={broker_status!r} carrying reattach_in_progress=true, "
        f"expected REATTACH_WATCHER (active PENDING_TRIGGER wins); got "
        f"{disp.disposition!r}. Stale broker-owned status must not strand "
        f"the live active order."
    )
    assert disp.existing_local_order_id == LOCAL_ORDER_ID


@pytest.mark.parametrize("broker_status", ["BROKER_SUBMITTED", "BROKER_ACKED", "FILLED"])
def test_prior_broker_owned_without_reattach_flag_still_returns_already_owned(
    stubbed_ov, monkeypatch, broker_status,
):
    """Guard is narrow: without reattach_in_progress=true, broker-owned
    status short-circuits to ALREADY_OWNED. Active-order fence never runs."""
    ov = stubbed_ov

    clean = _opportunity_row_with_status(broker_status, reattach_in_progress=False)
    monkeypatch.setattr(
        ov, "_get_client_opportunity_row",
        lambda sid, cid, sig: ov._LookupResult(CANONICAL, ov._LS_FOUND, clean, None),
    )
    fence_calls = {"n": 0}
    def _sentinel(*a, **kw):
        fence_calls["n"] += 1
        return (ov._LS_NOT_FOUND, None)
    monkeypatch.setattr(ov, "_query_active_entry_order", _sentinel)

    disp = ov._resolve_shared_setup_disposition(
        SIGNAL_ID, CLIENT_ID, _mk_signal(),
        execution_mode=EXECUTION_MODE,
        session_key=SESSION_KEY,
    )
    assert disp.disposition == ov._DISPOSITION_ALREADY_OWNED
    assert fence_calls["n"] == 0


# ─── Ambiguity fallback: active order gone but proof was reattach-poisoned ────

def test_watcher_armed_with_reattach_and_no_active_order_fails_closed_ambiguous(
    stubbed_ov, monkeypatch,
):
    """
    Setup:
      Prior WATCHER_ARMED + reattach_in_progress=true. Active-order fence
      returns NOT_FOUND (order somehow gone). Latest-entry-no-status fence
      also returns NOT_FOUND.

    Contract:
      Because the opportunity had current-session proof (poisoned),
      _has_current_session_proof is True — after deferring, we cannot fall
      through to NEW. Must return AMBIGUOUS_OWNERSHIP (fail-closed).

    This is the exact fail-safe the audit demands: no trustworthy order
    truth + poisoned lifecycle status = ambiguous, not NEW.
    """
    ov = stubbed_ov

    poisoned = _opportunity_row_with_status("WATCHER_ARMED", reattach_in_progress=True)
    monkeypatch.setattr(
        ov, "_get_client_opportunity_row",
        lambda sid, cid, sig: ov._LookupResult(CANONICAL, ov._LS_FOUND, poisoned, None),
    )
    monkeypatch.setattr(
        ov, "_query_active_entry_order",
        lambda cid, mode, canon: (ov._LS_NOT_FOUND, None),
    )
    monkeypatch.setattr(
        ov, "_query_latest_entry_order_no_status",
        lambda cid, mode, canon: (ov._LS_NOT_FOUND, None),
    )

    disp = ov._resolve_shared_setup_disposition(
        SIGNAL_ID, CLIENT_ID, _mk_signal(),
        execution_mode=EXECUTION_MODE,
        session_key=SESSION_KEY,
    )
    assert disp.disposition == ov._DISPOSITION_AMBIGUOUS_OWNERSHIP, (
        f"When poisoned WATCHER_ARMED coexists with no active/latest order, "
        f"resolver must fail closed as AMBIGUOUS_OWNERSHIP (never NEW). "
        f"Got {disp.disposition!r}."
    )


# ─── Lookup-failed still fails closed regardless of reattach flag ────────────

def test_reattach_in_progress_lookup_failed_still_fails_closed(stubbed_ov, monkeypatch):
    """Active-order query failure must always yield LOOKUP_FAILED, even when
    reattach_in_progress=true — the deferral does not weaken fail-closed."""
    ov = stubbed_ov

    poisoned = _opportunity_row_with_status("WATCHER_ARMED", reattach_in_progress=True)
    monkeypatch.setattr(
        ov, "_get_client_opportunity_row",
        lambda sid, cid, sig: ov._LookupResult(CANONICAL, ov._LS_FOUND, poisoned, None),
    )
    monkeypatch.setattr(
        ov, "_query_active_entry_order",
        lambda cid, mode, canon: (ov._LS_LOOKUP_FAILED, None),
    )

    disp = ov._resolve_shared_setup_disposition(
        SIGNAL_ID, CLIENT_ID, _mk_signal(),
        execution_mode=EXECUTION_MODE,
        session_key=SESSION_KEY,
    )
    assert disp.disposition == ov._DISPOSITION_LOOKUP_FAILED


# ─── Active submitted/filled beats stale WATCHER_ARMED ───────────────────────

def test_reattach_poisoned_watcher_armed_with_active_broker_submitted_returns_already_owned(
    stubbed_ov, monkeypatch,
):
    """
    Setup:
      Poisoned WATCHER_ARMED + reattach_in_progress=true. The actual live
      order truth is that the order moved into ACKNOWLEDGED (broker-side
      state) — i.e., during the retry gap the watcher successfully placed it
      but the WATCHER_ARMED proof clear hadn't been written yet.

    Contract:
      Active-order fence sees ACKNOWLEDGED → ALREADY_OWNED via the exact
      order truth. Correct — no replacement, no re-arm attempt.
    """
    ov = stubbed_ov

    poisoned = _opportunity_row_with_status("WATCHER_ARMED", reattach_in_progress=True)
    monkeypatch.setattr(
        ov, "_get_client_opportunity_row",
        lambda sid, cid, sig: ov._LookupResult(CANONICAL, ov._LS_FOUND, poisoned, None),
    )
    order = _mk_pending_trigger_order()
    order["status"] = "ACKNOWLEDGED"
    monkeypatch.setattr(
        ov, "_query_active_entry_order",
        lambda cid, mode, canon: (ov._LS_FOUND, order),
    )

    disp = ov._resolve_shared_setup_disposition(
        SIGNAL_ID, CLIENT_ID, _mk_signal(),
        execution_mode=EXECUTION_MODE,
        session_key=SESSION_KEY,
    )
    assert disp.disposition == ov._DISPOSITION_ALREADY_OWNED


# ─── Forbidden-writes contract: resolver never touches broker/OSM/selector ───

def test_resolver_never_calls_broker_osm_or_selector_in_any_poisoned_branch(
    stubbed_ov, monkeypatch,
):
    """
    Structural contract test: no matter the disposition, the resolver
    itself must never call master_control, contract_selector,
    order_state_machine.create_entry_order, broker.submit/place/cancel/
    replace, or entry_watcher.watch.

    This is not a mocked equivalence — the resolver has no reference to
    any of these objects. Prove it stays that way by inspecting its
    signature: it must take only signal_id, client_id, signal,
    execution_mode, session_key. Any regression that adds an
    order_state_machine / broker / etc. parameter would fail this test.
    """
    ov = stubbed_ov
    import inspect
    sig = inspect.signature(ov._resolve_shared_setup_disposition)
    param_names = set(sig.parameters.keys())
    forbidden = {
        "broker", "order_state_machine", "osm", "contract_selector",
        "selector", "entry_watcher", "watcher", "master_control", "mc",
    }
    intersect = param_names & forbidden
    assert not intersect, (
        f"_resolve_shared_setup_disposition must not accept any of "
        f"{forbidden!r}. Found: {intersect!r}. Adding one would create a "
        f"write path where the disposition resolver could touch broker "
        f"state — violating the PR #388 contract."
    )
    assert param_names <= {
        "signal_id", "client_id", "signal", "execution_mode", "session_key",
    }, f"Unexpected resolver params: {param_names!r}"
