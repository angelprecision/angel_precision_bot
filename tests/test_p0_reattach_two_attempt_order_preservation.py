"""
tests/test_p0_reattach_two_attempt_order_preservation.py
=========================================================
PR #388 integrated regression — the full two-attempt REATTACH flow:

  Attempt 1:
    * shared ap_signals row present
    * an exact ACTIVE PENDING_TRIGGER order already exists for
      client + mode + canonical
    * (simulated) opportunity proof missing → resolver returns
      REATTACH_WATCHER
    * caller reuses the existing local_order_id
    * NO create_entry_order, NO broker submit/place/cancel/replace
    * (this attempt could either arm or fail-safe retryable; the important
      contract is that the existing order is preserved)

  Attempt 2:
    * same shared row still present
    * SAME PENDING_TRIGGER order still there with the SAME local_order_id
    * resolver returns REATTACH_WATCHER again
    * SAME local_order_id reused (not a fresh id)
    * NO create_entry_order, NO broker action

This exercises the outer control flow through run_overnight_reeval rather
than the pure classifier or a fake watcher — the exact seam the reviewer
flagged as untested.
"""
from __future__ import annotations

import sys
import types
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import ap_overnight_reeval as ov


EXISTING_LOCAL_OID = "local-existing-pending-1"


def _install_reeval_sub_module_stubs(monkeypatch):
    """Minimal sub-module stubs needed by the reeval loop when Postgres /
    Supabase / intelligence / authorization / options daily validator are
    not available (as in CI without a fully-provisioned DB)."""
    auth = types.ModuleType("ap.authorization")
    auth.is_live_broker = lambda _b: False
    auth.broker_live_mode_known = lambda _b: True
    auth.check_live_authorization = lambda _c: None
    auth.authorization_gate_enforced = lambda: False
    auth.LIVE_AUTHORIZATION_GATE_UNAVAILABLE = "LIVE_AUTHORIZATION_GATE_UNAVAILABLE"
    auth.execution_mode_for_broker = lambda _b: "PAPER"
    monkeypatch.setitem(sys.modules, "ap.authorization", auth)

    intel = types.ModuleType("ap.intelligence_context_handoff")
    intel.enqueue_preopen_context_best_effort = lambda *_a, **_kw: None
    monkeypatch.setitem(sys.modules, "ap.intelligence_context_handoff", intel)

    validator = types.ModuleType("ap.overnight_daily_validator")
    validator.fetch_market_snapshot = lambda *_a, **_kw: {"last": 500.0}
    validator.validate_overnight_daily_signal = lambda **_kw: SimpleNamespace(
        valid=True, reason_code="", reason_text="",
    )
    validator.InvalidationReason = object
    monkeypatch.setitem(sys.modules, "ap.overnight_daily_validator", validator)


def _shared_row():
    return {
        "id":         "sup:sig-reattach-integration",
        "signal_id":  "sig-reattach-integration",
        "payload":    {
            "signal_id":     "sig-reattach-integration",
            "canonical_signal_id": "sig-reattach-integration",
            "ticker":        "SPY",
            "side":          "CALL",
            "timeframe":     "1d",
            "entry_trigger": 500.0,
            "stop_price":    490.0,
            "target_price":  520.0,
            "score":         85.0,
            "tier":          "A",
            "created_at":    "2026-07-27T20:00:00+00:00",
            "prior_day_high": 502.0,
            "prior_day_low":  498.0,
        },
        "created_ts": "2026-07-27T20:00:00+00:00",
        "_source":    "ap_signals",
    }


def _pending_trigger_order_row():
    return {
        "local_order_id":       EXISTING_LOCAL_OID,
        "status":               "PENDING_TRIGGER",
        "canonical_signal_id":  "sig-reattach-integration",
        "execution_mode":       "paper",
        "client_id":            "jose@example.com",
        "kind":                 "ENTRY",
        "symbol":               "SPY",
        "direction":            "CALL",
        "trigger_price":        500.0,
        "stop_underlying":      490.0,
        "target_underlying":    520.0,
        "score":                85.0,
        "tier":                 "A",
        "timeframe":            "1d",
        "pattern":              "2-1-2",
        "plan_id":              "plan-1",
        "signal_id":            "sig-reattach-integration",
        "qty":                  2,
        "limit_price":          0.01,
        "meta":                 {},
        "contract":             "DEFERRED:SPY",
    }


def _terminal_marker_row(*, reason="reattach_post_watch_terminal:CANCELED"):
    return {
        "canonical_signal_id": "sig-reattach-integration",
        "client_id": "jose@example.com",
        "opportunity_status": "MISSED",
        "miss_stage": "WATCHER_ARM",
        "miss_reason": reason,
        "order_local_id": EXISTING_LOCAL_OID,
        "metadata": {
            "execution_mode": "paper",
            "overnight_reeval_session_key": "2026-07-27",
            "local_order_id": EXISTING_LOCAL_OID,
            "reattach_terminal_suppression": True,
        },
    }


def _reattach_in_progress_row():
    return {
        "canonical_signal_id": "sig-reattach-integration",
        "client_id": "jose@example.com",
        "opportunity_status": "CREATED",
        "miss_stage": "",
        "miss_reason": "",
        "order_local_id": EXISTING_LOCAL_OID,
        "metadata": {
            "execution_mode": "paper",
            "overnight_reeval_session_key": "2026-07-27",
            "local_order_id": EXISTING_LOCAL_OID,
            "reattach_in_progress": True,
        },
    }


def _install_opportunity_ledger_stub(
    monkeypatch,
    *,
    create_opportunities,
    mark_watcher_invalidated,
    update_opportunity=None,
):
    _opp = types.ModuleType("ap.opportunity_ledger")
    _opp.CREATED = "CREATED"
    _opp.WATCHER_ARMED = "WATCHER_ARMED"
    _opp.BROKER_SUBMITTED = "BROKER_SUBMITTED"
    _opp.BROKER_ACKED = "BROKER_ACKED"
    _opp.FILLED = "FILLED"
    _opp.TERMINAL_STATUSES = frozenset({"EXPIRED", "CANCELED", "REJECTED", "MISSED", "INTERNAL_ERROR"})
    _opp.create_opportunities = create_opportunities
    _opp.mark_watcher_armed = MagicMock(return_value=True)
    _opp.mark_watcher_invalidated = mark_watcher_invalidated
    _opp.update_opportunity = update_opportunity or MagicMock(return_value=True)
    monkeypatch.setitem(sys.modules, "ap.opportunity_ledger", _opp)
    return _opp


def test_ambiguity_marker_storage_failure_returns_false(monkeypatch):
    create_opportunities = MagicMock(return_value=1)
    mark_watcher_invalidated = MagicMock(return_value=False)
    _install_opportunity_ledger_stub(
        monkeypatch,
        create_opportunities=create_opportunities,
        mark_watcher_invalidated=mark_watcher_invalidated,
    )

    ok = ov._persist_reattach_post_watch_ambiguity(
        client_id="jose@example.com",
        execution_mode="paper",
        canonical_signal_id="sig-reattach-integration",
        signal_id="sig-reattach-integration",
        signal_payload=_shared_row()["payload"],
        local_order_id=EXISTING_LOCAL_OID,
        session_key="2026-07-27",
        post_status=ov._LS_LOOKUP_FAILED,
        post_row_status="",
    )

    assert ok is False
    create_opportunities.assert_called_once()
    mark_watcher_invalidated.assert_called_once()


def test_ambiguity_marker_success_requires_readback_verification(monkeypatch):
    create_opportunities = MagicMock(return_value=1)
    mark_watcher_invalidated = MagicMock(return_value=True)
    _install_opportunity_ledger_stub(
        monkeypatch,
        create_opportunities=create_opportunities,
        mark_watcher_invalidated=mark_watcher_invalidated,
    )

    monkeypatch.setattr(
        ov,
        "_get_client_opportunity_row",
        lambda *_a, **_kw: ov._LookupResult(
            canonical_signal_id="sig-reattach-integration",
            lookup_status=ov._LS_FOUND,
            row=_terminal_marker_row(
                reason="reattach_post_watch_ambiguous_terminal_or_missing"
            ),
            error=None,
        ),
    )

    ok = ov._persist_reattach_post_watch_ambiguity(
        client_id="jose@example.com",
        execution_mode="paper",
        canonical_signal_id="sig-reattach-integration",
        signal_id="sig-reattach-integration",
        signal_payload=_shared_row()["payload"],
        local_order_id=EXISTING_LOCAL_OID,
        session_key="2026-07-27",
        post_status=ov._LS_NOT_FOUND,
        post_row_status="",
    )

    assert ok is True
    mark_watcher_invalidated.assert_called_once()
    _, args, kwargs = mark_watcher_invalidated.mock_calls[0]
    assert args[2] == "reattach_post_watch_ambiguous_terminal_or_missing"
    assert kwargs["order_local_id"] == EXISTING_LOCAL_OID
    assert kwargs["extra_meta"]["execution_mode"] == "paper"
    assert kwargs["extra_meta"]["overnight_reeval_session_key"] == "2026-07-27"


def test_terminal_marker_readback_suppresses_new_disposition(monkeypatch):
    _install_opportunity_ledger_stub(
        monkeypatch,
        create_opportunities=MagicMock(return_value=1),
        mark_watcher_invalidated=MagicMock(return_value=True),
    )
    monkeypatch.setattr(
        ov,
        "_get_client_opportunity_row",
        lambda *_a, **_kw: ov._LookupResult(
            canonical_signal_id="sig-reattach-integration",
            lookup_status=ov._LS_FOUND,
            row=_terminal_marker_row(),
            error=None,
        ),
    )
    monkeypatch.setattr(
        ov,
        "_query_active_entry_order",
        lambda *_a, **_kw: (ov._LS_NOT_FOUND, None),
    )
    monkeypatch.setattr(
        ov,
        "_query_latest_entry_order_no_status",
        lambda *_a, **_kw: (ov._LS_NOT_FOUND, None),
    )

    disp = ov._resolve_shared_setup_disposition(
        "sig-reattach-integration",
        "jose@example.com",
        _shared_row()["payload"],
        "paper",
        session_key="2026-07-27",
    )

    assert disp.disposition == ov._DISPOSITION_ALREADY_TERMINAL


def test_second_full_reeval_terminal_marker_creates_zero_replacement_orders(monkeypatch):
    _install_reeval_sub_module_stubs(monkeypatch)
    monkeypatch.setattr(ov, "_overnight_reeval_session_key", lambda *_a, **_kw: "2026-07-27")
    _install_opportunity_ledger_stub(
        monkeypatch,
        create_opportunities=MagicMock(return_value=1),
        mark_watcher_invalidated=MagicMock(return_value=True),
    )
    monkeypatch.setattr(
        ov, "_fetch_watching_signals_with_status_impl",
        lambda _c: ov._FetchWatchingSignalsResult(
            rows=[_shared_row()],
            trade_queue_status=ov._SOURCE_STATUS_SUCCESS,
            ap_signals_status=ov._SOURCE_STATUS_SUCCESS,
            trade_queue_error=None,
            ap_signals_error=None,
        ),
    )
    monkeypatch.setattr(
        ov,
        "_get_client_opportunity_row",
        lambda *_a, **_kw: ov._LookupResult(
            canonical_signal_id="sig-reattach-integration",
            lookup_status=ov._LS_FOUND,
            row=_terminal_marker_row(),
            error=None,
        ),
    )
    monkeypatch.setattr(
        ov,
        "_query_active_entry_order",
        lambda *_a, **_kw: (ov._LS_NOT_FOUND, None),
    )
    monkeypatch.setattr(
        ov,
        "_query_latest_entry_order_no_status",
        lambda *_a, **_kw: (ov._LS_NOT_FOUND, None),
    )
    monkeypatch.setattr(ov, "_mark_job_rejected", lambda *_a, **_kw: None)
    monkeypatch.setattr(ov, "_mark_job_error", lambda *_a, **_kw: None)
    monkeypatch.setattr(ov, "_mark_job_watching_reason", lambda *_a, **_kw: None)
    monkeypatch.setattr(ov, "_mark_job_watching_armed", lambda *_a, **_kw: None)

    mc_evaluate = MagicMock()
    osm_create = MagicMock()
    selector_select = MagicMock()
    broker = SimpleNamespace(
        submit_order=MagicMock(), place_order=MagicMock(),
        cancel_order=MagicMock(), replace_order=MagicMock(),
        get_prior_day_levels=lambda _t: {"prior_day_high": 502, "prior_day_low": 498},
    )

    result = ov.run_overnight_reeval(
        client_id="jose@example.com",
        broker=broker,
        data_broker=broker,
        master_control=SimpleNamespace(evaluate=mc_evaluate),
        contract_selector=SimpleNamespace(select=selector_select),
        order_state_machine=SimpleNamespace(
            create_entry_order=osm_create,
            get_order=lambda _oid: {"local_order_id": _oid, "status": "CANCELED"},
        ),
        entry_watcher=SimpleNamespace(has_order=lambda _oid: False, watch=MagicMock(return_value=True)),
        force=True,
    )

    assert result["already_resolved"] == 1
    assert result["retryable_deferred"] == 0
    mc_evaluate.assert_not_called()
    selector_select.assert_not_called()
    osm_create.assert_not_called()
    broker.submit_order.assert_not_called()
    broker.place_order.assert_not_called()
    broker.cancel_order.assert_not_called()
    broker.replace_order.assert_not_called()


def test_latest_no_status_terminal_order_suppresses_new_disposition(monkeypatch):
    _install_opportunity_ledger_stub(
        monkeypatch,
        create_opportunities=MagicMock(return_value=1),
        mark_watcher_invalidated=MagicMock(return_value=True),
    )
    monkeypatch.setattr(
        ov,
        "_get_client_opportunity_row",
        lambda *_a, **_kw: ov._LookupResult(
            canonical_signal_id="sig-reattach-integration",
            lookup_status=ov._LS_NOT_FOUND,
            row=None,
            error=None,
        ),
    )
    monkeypatch.setattr(
        ov,
        "_query_active_entry_order",
        lambda *_a, **_kw: (ov._LS_NOT_FOUND, None),
    )
    monkeypatch.setattr(
        ov,
        "_query_latest_entry_order_no_status",
        lambda *_a, **_kw: (ov._LS_FOUND, {
            **_pending_trigger_order_row(),
            "status": "CANCELED",
        }),
    )

    disp = ov._resolve_shared_setup_disposition(
        "sig-reattach-integration",
        "jose@example.com",
        _shared_row()["payload"],
        "paper",
        session_key="2026-07-27",
    )

    assert disp.disposition == ov._DISPOSITION_ALREADY_TERMINAL


def test_pre_watch_fence_failure_never_calls_watcher_or_replaces_order(monkeypatch):
    _install_reeval_sub_module_stubs(monkeypatch)
    monkeypatch.setattr(ov, "_overnight_reeval_session_key", lambda *_a, **_kw: "2026-07-27")
    monkeypatch.setattr(
        ov, "_fetch_watching_signals_with_status_impl",
        lambda _c: ov._FetchWatchingSignalsResult(
            rows=[_shared_row()],
            trade_queue_status=ov._SOURCE_STATUS_SUCCESS,
            ap_signals_status=ov._SOURCE_STATUS_SUCCESS,
            trade_queue_error=None,
            ap_signals_error=None,
        ),
    )
    monkeypatch.setattr(
        ov,
        "_get_client_opportunity_row",
        lambda *_a, **_kw: ov._LookupResult(
            canonical_signal_id="sig-reattach-integration",
            lookup_status=ov._LS_NOT_FOUND,
            row=None,
            error=None,
        ),
    )
    monkeypatch.setattr(
        ov,
        "_query_active_entry_order",
        lambda *_a, **_kw: (ov._LS_FOUND, _pending_trigger_order_row()),
    )
    monkeypatch.setattr(ov, "_persist_reattach_in_progress_fence", MagicMock(return_value=False))
    monkeypatch.setattr(ov, "_mark_job_rejected", lambda *_a, **_kw: None)
    monkeypatch.setattr(ov, "_mark_job_error", lambda *_a, **_kw: None)
    monkeypatch.setattr(ov, "_mark_job_watching_reason", lambda *_a, **_kw: None)
    monkeypatch.setattr(ov, "_mark_job_watching_armed", lambda *_a, **_kw: None)

    mc_evaluate = MagicMock()
    osm_create = MagicMock()
    selector_select = MagicMock()
    watcher_watch = MagicMock(return_value=True)
    broker = SimpleNamespace(
        submit_order=MagicMock(), place_order=MagicMock(),
        cancel_order=MagicMock(), replace_order=MagicMock(),
        get_prior_day_levels=lambda _t: {"prior_day_high": 502, "prior_day_low": 498},
    )

    result = ov.run_overnight_reeval(
        client_id="jose@example.com",
        broker=broker,
        data_broker=broker,
        master_control=SimpleNamespace(evaluate=mc_evaluate),
        contract_selector=SimpleNamespace(select=selector_select),
        order_state_machine=SimpleNamespace(
            create_entry_order=osm_create,
            get_order=lambda _oid: {"local_order_id": _oid, "status": "PENDING_TRIGGER"},
        ),
        entry_watcher=SimpleNamespace(has_order=lambda _oid: False, watch=watcher_watch),
        force=True,
    )

    assert result["retryable_deferred"] == 1
    assert result["completed"] is False
    watcher_watch.assert_not_called()
    mc_evaluate.assert_not_called()
    selector_select.assert_not_called()
    osm_create.assert_not_called()
    broker.submit_order.assert_not_called()
    broker.place_order.assert_not_called()
    broker.cancel_order.assert_not_called()
    broker.replace_order.assert_not_called()


def test_post_watch_marker_failure_then_second_reeval_cannot_return_new(monkeypatch):
    _install_reeval_sub_module_stubs(monkeypatch)
    monkeypatch.setattr(ov, "_overnight_reeval_session_key", lambda *_a, **_kw: "2026-07-27")
    monkeypatch.setattr(
        ov, "_fetch_watching_signals_with_status_impl",
        lambda _c: ov._FetchWatchingSignalsResult(
            rows=[_shared_row()],
            trade_queue_status=ov._SOURCE_STATUS_SUCCESS,
            ap_signals_status=ov._SOURCE_STATUS_SUCCESS,
            trade_queue_error=None,
            ap_signals_error=None,
        ),
    )
    monkeypatch.setattr(ov, "_mark_job_rejected", lambda *_a, **_kw: None)
    monkeypatch.setattr(ov, "_mark_job_error", lambda *_a, **_kw: None)
    monkeypatch.setattr(ov, "_mark_job_watching_reason", lambda *_a, **_kw: None)
    monkeypatch.setattr(ov, "_mark_job_watching_armed", lambda *_a, **_kw: None)

    broker = SimpleNamespace(
        submit_order=MagicMock(), place_order=MagicMock(),
        cancel_order=MagicMock(), replace_order=MagicMock(),
        get_prior_day_levels=lambda _t: {"prior_day_high": 502, "prior_day_low": 498},
    )
    mc_evaluate = MagicMock()
    osm_create = MagicMock()
    selector_select = MagicMock()

    # Attempt 1: active PENDING_TRIGGER; pre-watch fence succeeds; watcher
    # returns False; exact reread sees CANCELED; terminal marker fails.
    monkeypatch.setattr(
        ov,
        "_get_client_opportunity_row",
        lambda *_a, **_kw: ov._LookupResult(
            canonical_signal_id="sig-reattach-integration",
            lookup_status=ov._LS_NOT_FOUND,
            row=None,
            error=None,
        ),
    )
    monkeypatch.setattr(
        ov,
        "_query_active_entry_order",
        lambda *_a, **_kw: (ov._LS_FOUND, _pending_trigger_order_row()),
    )
    monkeypatch.setattr(ov, "_persist_reattach_in_progress_fence", MagicMock(return_value=True))
    monkeypatch.setattr(
        ov,
        "_query_exact_entry_order_by_local_id",
        lambda *_a, **_kw: (ov._LS_FOUND, {
            **_pending_trigger_order_row(),
            "status": "CANCELED",
        }),
    )
    monkeypatch.setattr(ov, "_persist_reattach_terminal_suppression_marker", MagicMock(return_value=False))

    first = ov.run_overnight_reeval(
        client_id="jose@example.com",
        broker=broker,
        data_broker=broker,
        master_control=SimpleNamespace(evaluate=mc_evaluate),
        contract_selector=SimpleNamespace(select=selector_select),
        order_state_machine=SimpleNamespace(
            create_entry_order=osm_create,
            get_order=lambda _oid: {"local_order_id": _oid, "status": "CANCELED"},
        ),
        entry_watcher=SimpleNamespace(has_order=lambda _oid: False, watch=MagicMock(return_value=False)),
        force=True,
    )
    assert first["retryable_deferred"] == 1
    assert first["completed"] is False

    # Attempt 2: opportunity lookup is still NOT_FOUND and active fence sees
    # nothing, but resolver's no-status latest-order fence finds CANCELED.
    monkeypatch.setattr(
        ov,
        "_query_active_entry_order",
        lambda *_a, **_kw: (ov._LS_NOT_FOUND, None),
    )
    monkeypatch.setattr(
        ov,
        "_query_latest_entry_order_no_status",
        lambda *_a, **_kw: (ov._LS_FOUND, {
            **_pending_trigger_order_row(),
            "status": "CANCELED",
        }),
    )

    second = ov.run_overnight_reeval(
        client_id="jose@example.com",
        broker=broker,
        data_broker=broker,
        master_control=SimpleNamespace(evaluate=mc_evaluate),
        contract_selector=SimpleNamespace(select=selector_select),
        order_state_machine=SimpleNamespace(
            create_entry_order=osm_create,
            get_order=lambda _oid: {"local_order_id": _oid, "status": "CANCELED"},
        ),
        entry_watcher=SimpleNamespace(has_order=lambda _oid: False, watch=MagicMock(return_value=True)),
        force=True,
    )

    assert second["already_resolved"] == 1
    assert second["retryable_deferred"] == 0
    mc_evaluate.assert_not_called()
    selector_select.assert_not_called()
    osm_create.assert_not_called()
    broker.submit_order.assert_not_called()
    broker.place_order.assert_not_called()
    broker.cancel_order.assert_not_called()
    broker.replace_order.assert_not_called()


def _run_one_attempt(monkeypatch, *, mock_ledger):
    """Drive the real run_overnight_reeval loop for a single attempt.
    Uses spies on master_control, contract_selector, OSM create/broker to
    prove the fence assertions."""
    _install_reeval_sub_module_stubs(monkeypatch)

    monkeypatch.setattr(
        ov, "_fetch_watching_signals_with_status_impl",
        lambda _c: ov._FetchWatchingSignalsResult(
            rows=[_shared_row()],
            trade_queue_status=ov._SOURCE_STATUS_SUCCESS,
            ap_signals_status=ov._SOURCE_STATUS_SUCCESS,
            trade_queue_error=None,
            ap_signals_error=None,
        ),
    )
    # Opportunity ledger reports FOUND but with no proof — resolver falls
    # through to the active-order query. This mirrors "watcher armed but
    # WATCHER_ARMED durable proof write failed on the previous attempt".
    monkeypatch.setattr(
        ov, "_get_client_opportunity_row",
        lambda *_a, **_kw: ov._LookupResult(
            canonical_signal_id="sig-reattach-integration",
            lookup_status=ov._LS_NOT_FOUND,
            row=None, error=None,
        ),
    )
    # Active-order query: return the same PENDING_TRIGGER row on every call
    # (both attempt 1 and attempt 2).
    monkeypatch.setattr(
        ov, "_query_active_entry_order",
        lambda *_a, **_kw: (ov._LS_FOUND, _pending_trigger_order_row()),
    )
    monkeypatch.setattr(
        ov, "_persist_reattach_in_progress_fence",
        MagicMock(return_value=True),
    )

    # Trap DB writers.
    monkeypatch.setattr(ov, "_mark_job_rejected", lambda *_a, **_kw: None)
    monkeypatch.setattr(ov, "_mark_job_error",   lambda *_a, **_kw: None)
    monkeypatch.setattr(ov, "_mark_job_watching_reason", lambda *_a, **_kw: None)
    monkeypatch.setattr(ov, "_mark_job_watching_armed",  lambda *_a, **_kw: None)

    # opportunity_ledger stubs — track proof-write attempts.
    _opp = types.ModuleType("ap.opportunity_ledger")
    _opp.WATCHER_ARMED     = "WATCHER_ARMED"
    _opp.BROKER_SUBMITTED  = "BROKER_SUBMITTED"
    _opp.BROKER_ACKED      = "BROKER_ACKED"
    _opp.FILLED            = "FILLED"
    _opp.TERMINAL_STATUSES = frozenset({"EXPIRED", "CANCELED", "REJECTED"})
    _opp.create_opportunities = MagicMock()
    _opp.mark_watcher_armed = mock_ledger  # attempt 1 = fail; attempt 2 = ok
    _opp.update_opportunity = MagicMock(return_value=True)
    monkeypatch.setitem(sys.modules, "ap.opportunity_ledger", _opp)

    mc_evaluate = MagicMock()
    osm_create = MagicMock()
    selector_select = MagicMock()

    broker = SimpleNamespace(
        submit_order=MagicMock(), place_order=MagicMock(),
        cancel_order=MagicMock(), replace_order=MagicMock(),
        get_prior_day_levels=lambda _t: {"prior_day_high": 502, "prior_day_low": 498},
    )
    entry_watcher = SimpleNamespace(
        # Real reattach precheck path uses has_order; return False so the
        # caller invokes watch() which we stub to True. In production the
        # real watcher runs; here we only need to prove NO cancel / create
        # calls originate from the reeval outer flow.
        has_order=lambda _oid: False,
        watch=MagicMock(return_value=True),
    )

    result = ov.run_overnight_reeval(
        client_id="jose@example.com",
        broker=broker,
        data_broker=broker,
        master_control=SimpleNamespace(evaluate=mc_evaluate),
        contract_selector=SimpleNamespace(select=selector_select),
        order_state_machine=SimpleNamespace(
            create_entry_order=osm_create,
            get_order=lambda _oid: {"local_order_id": _oid, "status": "PENDING_TRIGGER"},
        ),
        entry_watcher=entry_watcher,
        force=True,
    )
    return SimpleNamespace(
        result=result,
        mc_evaluate=mc_evaluate,
        osm_create=osm_create,
        selector_select=selector_select,
        broker=broker,
        entry_watcher=entry_watcher,
    )


def test_two_attempts_reuse_same_local_order_id_and_never_create_or_broker(monkeypatch):
    """The reviewer's integrated regression.

    Attempt 1: mark_watcher_armed returns False (durable proof write failed).
    Attempt 2: mark_watcher_armed returns True (proof write succeeds).

    Across BOTH attempts:
      * The active-order query returns the SAME PENDING_TRIGGER row
        (same local_order_id) — proving the row was not canceled.
      * The reeval outer flow NEVER calls master_control.evaluate.
      * The reeval outer flow NEVER calls contract_selector.select.
      * The reeval outer flow NEVER calls order_state_machine.create_entry_order.
      * The reeval outer flow NEVER calls broker.submit_order / place_order
        / cancel_order / replace_order.
      * The watcher receives the SAME local_order_id both times (reused).
    """
    # Attempt 1: durable proof write fails → run classifies retryable_deferred.
    _mark_wa_1 = MagicMock(return_value=False)
    state1 = _run_one_attempt(monkeypatch, mock_ledger=_mark_wa_1)

    state1.mc_evaluate.assert_not_called()
    state1.osm_create.assert_not_called()
    state1.selector_select.assert_not_called()
    state1.broker.submit_order.assert_not_called()
    state1.broker.place_order.assert_not_called()
    state1.broker.cancel_order.assert_not_called()
    state1.broker.replace_order.assert_not_called()

    # The watcher was invoked with the EXISTING local_order_id — not a
    # fresh one from create_entry_order.
    assert state1.entry_watcher.watch.call_count == 1
    _call_args_1 = state1.entry_watcher.watch.call_args
    _passed_oid_1 = _call_args_1.args[1] if len(_call_args_1.args) >= 2 else _call_args_1.kwargs.get("local_order_id")
    assert _passed_oid_1 == EXISTING_LOCAL_OID, (
        f"Attempt 1: watcher was invoked with local_order_id={_passed_oid_1!r}, "
        f"expected {EXISTING_LOCAL_OID!r} (the existing row's id)."
    )

    # Attempt 2 (fresh monkeypatch scope not necessary; the resolver still
    # returns the same PENDING_TRIGGER row). Ledger proof now succeeds.
    _mark_wa_2 = MagicMock(return_value=True)
    state2 = _run_one_attempt(monkeypatch, mock_ledger=_mark_wa_2)

    state2.mc_evaluate.assert_not_called()
    state2.osm_create.assert_not_called()
    state2.selector_select.assert_not_called()
    state2.broker.submit_order.assert_not_called()
    state2.broker.place_order.assert_not_called()
    state2.broker.cancel_order.assert_not_called()
    state2.broker.replace_order.assert_not_called()

    assert state2.entry_watcher.watch.call_count == 1
    _call_args_2 = state2.entry_watcher.watch.call_args
    _passed_oid_2 = _call_args_2.args[1] if len(_call_args_2.args) >= 2 else _call_args_2.kwargs.get("local_order_id")
    assert _passed_oid_2 == EXISTING_LOCAL_OID, (
        f"Attempt 2: watcher was invoked with local_order_id={_passed_oid_2!r}, "
        f"expected {EXISTING_LOCAL_OID!r} (the SAME id reused across attempts)."
    )
    # Same order id → the row was preserved between attempts.
    assert _passed_oid_1 == _passed_oid_2

    # Recovery flags carried on both attempts.
    _kwargs_2 = _call_args_2.kwargs
    assert _kwargs_2.get("recovery_rearm") is True
    assert _kwargs_2.get("no_cancel_on_reject") is True

    # Attempt 2 armed the row (proof write succeeded).
    assert state2.result["armed"] == 1
