"""
tests/test_p0_active_order_fence_full_family.py
==================================================
PR #388 amendment — the exact active-ENTRY order fence must recognize the
FULL nonterminal status family the repository's existing durable duplicate
logic uses:

    CREATED, PENDING_TRIGGER, SUBMITTED, ACCEPTED, ACKNOWLEDGED, OPEN,
    PARTIAL_FILL, PARTIALLY_FILLED, FILLED

Prior amendment only recognized {PENDING_TRIGGER, SUBMITTED, ACKNOWLEDGED,
PARTIAL_FILL, FILLED}. Any ENTRY row in ACCEPTED / OPEN / CREATED /
PARTIALLY_FILLED slipped through, letting the resolver reach NEW and
allowing a duplicate create_entry_order() call on retry.

Disposition rules:
  * PENDING_TRIGGER                                     → REATTACH_WATCHER
  * CREATED                                             → RETRYABLE (no new order)
  * SUBMITTED/ACCEPTED/ACKNOWLEDGED/OPEN/PARTIAL_FILL/
    PARTIALLY_FILLED/FILLED                             → ALREADY_OWNED
"""
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

import ap_overnight_reeval as ov


NONTERMINAL_STATUSES = [
    "CREATED",
    "PENDING_TRIGGER",
    "SUBMITTED",
    "ACCEPTED",
    "ACKNOWLEDGED",
    "OPEN",
    "PARTIAL_FILL",
    "PARTIALLY_FILLED",
    "FILLED",
]


@pytest.fixture(autouse=True)
def _fixed_reeval_clock(monkeypatch):
    """Keep fixed July 27 inventory fresh regardless of the CI wall clock."""
    monkeypatch.setattr(
        ov,
        "_et_now",
        lambda: datetime(2026, 7, 28, 9, 35, tzinfo=ZoneInfo("America/New_York")),
    )


def test_full_nonterminal_family_present_in_active_status_set():
    """Every status in the durable duplicate family must be in the fence."""
    for st in NONTERMINAL_STATUSES:
        assert st in ov._ACTIVE_ENTRY_OWN_STATUSES, (
            f"status {st!r} missing from _ACTIVE_ENTRY_OWN_STATUSES — "
            f"the fence still lets duplicates through"
        )


@pytest.mark.parametrize("status", NONTERMINAL_STATUSES)
def test_disposition_never_returns_new_for_any_nonterminal_status(monkeypatch, status):
    """For every nonterminal status the resolver MUST return a disposition
    that PREVENTS a duplicate create_entry_order call — never NEW, never
    LOOKUP_FAILED. PENDING_TRIGGER→REATTACH_WATCHER; CREATED→RETRYABLE;
    everything else in the family→ALREADY_OWNED."""
    # Patch the opportunity lookup so the resolver falls through to the
    # active-order check.
    monkeypatch.setattr(
        ov, "_get_client_opportunity_row",
        lambda *_a, **_kw: ov._LookupResult(
            canonical_signal_id="canon-sig-1",
            lookup_status=ov._LS_NOT_FOUND,
            row=None,
            error=None,
        ),
    )
    # Patch the active-order query to return a row in the parametrized status.
    order_row = {
        "local_order_id":       "local-1",
        "status":               status,
        "canonical_signal_id":  "canon-sig-1",
        "execution_mode":       "paper",
        "client_id":            "jose@example.com",
        "kind":                 "ENTRY",
    }
    monkeypatch.setattr(
        ov, "_query_active_entry_order",
        lambda *_a, **_kw: (ov._LS_FOUND, order_row),
    )

    result = ov._resolve_shared_setup_disposition(
        signal_id="canon-sig-1",
        client_id="jose@example.com",
        signal={"signal_id": "canon-sig-1"},
        execution_mode="paper",
        session_key="2026-07-27",
    )

    # Never NEW (would trigger create_entry_order).
    assert result.disposition != ov._DISPOSITION_NEW, (
        f"status {status!r} produced NEW — duplicate create_entry_order path is open"
    )
    # Never LOOKUP_FAILED (test setup provided FOUND).
    assert result.disposition != ov._DISPOSITION_LOOKUP_FAILED

    if status == "PENDING_TRIGGER":
        assert result.disposition == ov._DISPOSITION_REATTACH_WATCHER
    elif status == "CREATED":
        # Dedicated disposition — NOT the generic RETRYABLE — because the
        # caller's RETRYABLE branch would fall through to master_control
        # and create_entry_order (blocker #6).
        assert result.disposition == ov._DISPOSITION_ACTIVE_CREATED_RETRY
    else:
        assert result.disposition == ov._DISPOSITION_ALREADY_OWNED, (
            f"status {status!r}: expected ALREADY_OWNED, got {result.disposition!r}"
        )


# ── End-to-end: real run_overnight_reeval with spies on MC/OSM/selector ────

def test_end_to_end_created_row_never_reaches_master_control_or_create_entry_order(monkeypatch):
    """The reviewer's exact test spec for blocker #6.

    Drive the REAL run_overnight_reeval outer control flow with a single
    shared ap_signals row and an exact ACTIVE CREATED order fence hit.
    Assert:
      * master_control.evaluate is NEVER called
      * order_state_machine.create_entry_order is NEVER called
      * contract_selector.select is NEVER called
      * broker.submit_order / place_order / cancel_order NEVER called
      * result["retryable_deferred"] == 1
      * result["completed"] is False
    """
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    import sys, types

    # Stub the sub-modules the reeval loop imports (mode auth, intelligence,
    # daily validator) so the row can reach the disposition resolver in a
    # test environment with no real Supabase/Postgres.
    _mod = types.ModuleType
    auth = _mod("ap.authorization")
    auth.is_live_broker = lambda _b: False
    auth.broker_live_mode_known = lambda _b: True
    auth.check_live_authorization = lambda _c: None
    auth.authorization_gate_enforced = lambda: False
    auth.LIVE_AUTHORIZATION_GATE_UNAVAILABLE = "LIVE_AUTHORIZATION_GATE_UNAVAILABLE"
    auth.execution_mode_for_broker = lambda _b: "PAPER"
    monkeypatch.setitem(sys.modules, "ap.authorization", auth)
    intel = _mod("ap.intelligence_context_handoff")
    intel.enqueue_preopen_context_best_effort = lambda *_a, **_kw: None
    monkeypatch.setitem(sys.modules, "ap.intelligence_context_handoff", intel)
    validator = _mod("ap.overnight_daily_validator")
    validator.fetch_market_snapshot = lambda *_a, **_kw: {"last": 100.0}
    validator.validate_overnight_daily_signal = lambda **_kw: SimpleNamespace(
        valid=True, reason_code="", reason_text="",
    )
    validator.InvalidationReason = object
    monkeypatch.setitem(sys.modules, "ap.overnight_daily_validator", validator)

    # Force a single shared ap_signals row into the loop.
    row = {
        "id":         "sup:sig-created-fence",
        "signal_id":  "sig-created-fence",
        "payload":    {
            "signal_id":     "sig-created-fence",
            "canonical_signal_id": "sig-created-fence",
            "ticker":        "SPY",
            "side":          "CALL",
            "timeframe":     "1d",
            "entry_trigger": 500.0,
            "stop_price":    490.0,
            "target_price":  520.0,
            "score":         85.0,
            "tier":          "A",
            "created_at":    "2026-07-27T20:00:00+00:00",
        },
        "created_ts": "2026-07-27T20:00:00+00:00",
        "_source":    "ap_signals",
    }
    monkeypatch.setattr(
        ov, "_fetch_watching_signals_with_status_impl",
        lambda _c: ov._FetchWatchingSignalsResult(
            rows=[row],
            trade_queue_status=ov._SOURCE_STATUS_SUCCESS,
            ap_signals_status=ov._SOURCE_STATUS_SUCCESS,
            trade_queue_error=None,
            ap_signals_error=None,
        ),
    )

    # Opportunity lookup returns NOT_FOUND so the resolver reaches the
    # active-order query.
    monkeypatch.setattr(
        ov, "_get_client_opportunity_row",
        lambda *_a, **_kw: ov._LookupResult(
            canonical_signal_id="sig-created-fence",
            lookup_status=ov._LS_NOT_FOUND,
            row=None, error=None,
        ),
    )
    # Active-order query returns an exact CREATED row for this client/mode/canonical.
    monkeypatch.setattr(
        ov, "_query_active_entry_order",
        lambda *_a, **_kw: (ov._LS_FOUND, {
            "local_order_id":       "local-existing-created-1",
            "status":               "CREATED",
            "canonical_signal_id":  "sig-created-fence",
            "execution_mode":       "paper",
            "client_id":            "jose@example.com",
            "kind":                 "ENTRY",
            "symbol":               "SPY",
            "direction":            "CALL",
            "trigger_price":        500.0,
        }),
    )
    # Trap _mark_job_* so they don't try to hit the DB.
    monkeypatch.setattr(ov, "_mark_job_rejected", lambda *_a, **_kw: None)
    monkeypatch.setattr(ov, "_mark_job_error",   lambda *_a, **_kw: None)
    monkeypatch.setattr(ov, "_mark_job_watching_reason", lambda *_a, **_kw: None)
    monkeypatch.setattr(ov, "_mark_job_watching_armed",  lambda *_a, **_kw: None)

    # Real spies on the fence-critical objects.
    mc_evaluate = MagicMock()
    osm_create = MagicMock()
    selector_select = MagicMock()

    broker = SimpleNamespace(
        submit_order=MagicMock(), place_order=MagicMock(),
        cancel_order=MagicMock(), replace_order=MagicMock(),
        get_prior_day_levels=lambda _t: {"prior_day_high": 502, "prior_day_low": 498},
    )
    master_control = SimpleNamespace(evaluate=mc_evaluate)
    selector = SimpleNamespace(select=selector_select)
    osm = SimpleNamespace(
        create_entry_order=osm_create,
        get_order=lambda _oid: {"local_order_id": _oid, "status": "PENDING_TRIGGER"},
    )
    entry_watcher = SimpleNamespace(watch=MagicMock(return_value=True))

    result = ov.run_overnight_reeval(
        client_id="jose@example.com",
        broker=broker,
        data_broker=broker,
        master_control=master_control,
        contract_selector=selector,
        order_state_machine=osm,
        entry_watcher=entry_watcher,
        force=True,
    )

    # THE FENCE ASSERTIONS.
    mc_evaluate.assert_not_called()
    osm_create.assert_not_called()
    selector_select.assert_not_called()
    broker.submit_order.assert_not_called()
    broker.place_order.assert_not_called()
    broker.cancel_order.assert_not_called()
    entry_watcher.watch.assert_not_called()

    # Run classification.
    assert result["retryable_deferred"] == 1
    assert result["completed"] is False
    assert result["retryable"] is True
    # No armed row, no terminal reject.
    assert result["armed"] == 0
    assert result["terminal_rejected"] == 0
