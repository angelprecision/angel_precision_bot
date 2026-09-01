"""P0 coverage for the LIVE deferred-entry submit-intent crash window.

These tests bind the production contract at the point where a durable
``submit_intent_at`` and ``broker_submit:*`` owner already exist.  The only
safe outcomes are exact-tag adoption, a complete-query no-match observation,
or retention of the exact owner with no broker POST.
"""

from __future__ import annotations

import os
import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@127.0.0.1:1/test")

import ap_execution_core
import ap.fill_monitor as fill_monitor
from ap.broker_submit_identity import (
    build_entry_submit_payload,
    canonical_broker_submit_key,
    entry_submit_payload_hash,
)
from ap.brokers.tradier import TradierBroker, TradierConfig
from ap_entry_watcher import APEntryWatcher
from ap_recovery import APStartupRecovery
from ap.order_state_machine import APOrderStateMachine
from ap.order_monitor import APOrderMonitor


_CLIENT = "jason@example.com"
_AAPL_ID = "33121850-ce16-43c0-a1e0-964c8bc608f1"
_CSCO_ID = "386a608d-6282-4a08-a97f-2a6413f93064"


def _intent_row(
    *,
    local_order_id: str = _AAPL_ID,
    symbol: str = "AAPL",
    contract: str = "AAPL260904C00230000",
    execution_mode: str = "live",
    intent_at: str | None = None,
    generation: int = 4,
    **overrides,
) -> dict:
    key = canonical_broker_submit_key(local_order_id)
    meta = {
        "execution_mode": execution_mode,
        "lifecycle_state": "SUBMITTING",
        "materialization_generation": generation,
        "submit_intent_at": intent_at or datetime.now(timezone.utc).isoformat(),
        "broker_submit_key": key,
        "current_owner": f"broker_submit:{key}",
        "trigger_crossed_at": "2026-08-31T16:00:00+00:00",
        "trigger_crossed_at_provenance": {
            "canonical_signal_id": f"sig-{local_order_id}",
            "client_id": _CLIENT,
            "execution_mode": execution_mode,
            "local_order_id": local_order_id,
        },
        "canonical_signal_id": f"sig-{local_order_id}",
    }
    meta["broker_submit_payload_hash"] = entry_submit_payload_hash(
        build_entry_submit_payload(
            symbol=symbol,
            contract=contract,
            qty=1,
            limit_price=2.25,
            broker_submit_key=key,
        )
    )
    row = {
        "local_order_id": local_order_id,
        "client_id": _CLIENT,
        "execution_mode": execution_mode,
        "signal_id": f"sig-{local_order_id}",
        "kind": "ENTRY",
        "status": "PENDING_TRIGGER",
        "symbol": symbol,
        "contract": contract,
        "qty": 1,
        "limit_price": 2.25,
        "broker_order_id": None,
        "submitted_ts": None,
        "created_ts": datetime.now(timezone.utc),
        "meta": meta,
    }
    row.update(overrides)
    return row


def _core_for(row: dict, *, mode: str | None = None):
    osm = MagicMock()
    osm.get_order.return_value = row
    osm.retain_broker_submit_owner_for_reconciliation.return_value = True

    broker = MagicMock()
    broker.list_orders.return_value = []
    core = SimpleNamespace(
        client_id=_CLIENT,
        email=_CLIENT,
        execution_mode=mode or str(row.get("execution_mode") or "live"),
        mode=(mode or str(row.get("execution_mode") or "live")).upper(),
        order_state_machine=osm,
        broker=broker,
        submit_existing_entry=MagicMock(),
    )
    core._is_real_occ_contract = ap_execution_core.APExecutionCore._is_real_occ_contract
    core.resume_deferred_broker_ready_order = MagicMock()
    core.reconcile_deferred_broker_intent = (
        ap_execution_core.APExecutionCore.reconcile_deferred_broker_intent.__get__(
            core, type(core)
        )
    )
    return core, osm, broker


def test_osm_runtime_mode_is_explicit_and_never_defaults_from_malformed_input():
    live_osm = APOrderStateMachine(
        client_id=_CLIENT,
        execution_mode=" LIVE ",
    )
    assert live_osm.execution_mode == "live"
    assert APOrderStateMachine(client_id=_CLIENT).execution_mode is None

    with pytest.raises(ValueError, match="execution_mode"):
        APOrderStateMachine(client_id=_CLIENT, execution_mode="staging")


def test_client_runner_wires_canonical_mode_into_production_osm_constructor():
    import inspect
    import client_runner

    captured = {}

    class _CaptureOSM:
        def __init__(self, *, client_id, execution_mode):
            captured.update(
                client_id=client_id,
                execution_mode=execution_mode,
            )

    runner = object.__new__(client_runner.ClientRunner)
    runner.email = _CLIENT
    runner.mode = "LIVE"

    result = runner._build_order_state_machine(_CaptureOSM)

    assert isinstance(result, _CaptureOSM)
    assert captured == {"client_id": _CLIENT, "execution_mode": "live"}
    run_inner_source = inspect.getsource(client_runner.ClientRunner._run_inner)
    assert "self._build_order_state_machine(" in run_inner_source
    assert "APOrderStateMachine" in run_inner_source

    runner.mode = "UNKNOWN"
    with pytest.raises(RuntimeError, match="unproven execution mode"):
        runner._build_order_state_machine(_CaptureOSM)


class _StatefulAdoptionOSM:
    """Small durable-state double that honors the production metadata CAS."""

    def __init__(self, row: dict):
        self.row = dict(row)
        self.row["meta"] = dict(row.get("meta") or {})
        self.client_id = row["client_id"]
        self.transition_calls = []
        self.meta_calls = []
        self.advance_to_filled_before_meta = False

    def get_order(self, local_order_id: str):
        if local_order_id != self.row["local_order_id"]:
            return None
        current = dict(self.row)
        current["meta"] = dict(self.row.get("meta") or {})
        return current

    def transition(self, local_order_id: str, new_status: str, **kwargs):
        self.transition_calls.append((local_order_id, new_status, dict(kwargs)))
        if local_order_id != self.row["local_order_id"]:
            return False
        self.row["status"] = new_status
        if kwargs.get("broker_order_id"):
            self.row["broker_order_id"] = kwargs["broker_order_id"]
        if kwargs.get("submitted_ts"):
            self.row["submitted_ts"] = kwargs["submitted_ts"]
        if kwargs.get("filled_qty") is not None:
            self.row["filled_qty"] = kwargs["filled_qty"]
        if kwargs.get("fill_price") is not None:
            self.row["fill_price"] = kwargs["fill_price"]
        return True

    def update_order_meta(self, local_order_id: str, meta_patch: dict, **kwargs):
        self.meta_calls.append((local_order_id, dict(meta_patch), dict(kwargs)))
        if local_order_id != self.row["local_order_id"]:
            return False
        if self.advance_to_filled_before_meta:
            self.row["status"] = "FILLED"
            self.advance_to_filled_before_meta = False
        expected_status = kwargs.get("expected_status")
        if expected_status and self.row.get("status") != expected_status:
            return False
        expected_mode = kwargs.get("expected_execution_mode")
        if expected_mode and self.row.get("execution_mode") != expected_mode:
            return False
        expected_signal_id = kwargs.get("expected_signal_id")
        if expected_signal_id and self.row.get("signal_id") != expected_signal_id:
            return False
        self.row["meta"].update(meta_patch)
        return True


def _stateful_adoption_core(row: dict, *, broker_status: str = "filled"):
    osm = _StatefulAdoptionOSM(row)
    broker = MagicMock()
    broker.list_orders.return_value = [_remote(row, status=broker_status)]
    core, _, _ = _core_for(row)
    core.order_state_machine = osm
    core.broker = broker
    return core, osm, broker


def _recovery_ready_row(*, lease_until: str, local_order_id: str = _AAPL_ID) -> dict:
    row = _intent_row(local_order_id=local_order_id)
    owner = f"recovery_submit:{local_order_id}:lease"
    row.update({"reserved_cost": 225.0})
    row["meta"] = {
        "execution_mode": "live",
        "lifecycle_state": "BROKER_READY",
        "materialization_status": "SELECTED",
        "broker_ready": True,
        "materialization_generation": 4,
        "recovery_submit_owner": owner,
        "recovery_submit_lease_until": lease_until,
        "current_owner": owner,
        "trigger_crossed_at": (
            datetime.now(timezone.utc) - timedelta(seconds=1)
        ).isoformat(),
        "selected_contract": row["contract"],
        "selected_limit": row["limit_price"],
        "selected_qty": row["qty"],
        "trigger_price": 230.0,
        "direction": "CALL",
        "signal_id": row["signal_id"],
    }
    return row


def _recovery_watched(row: dict):
    meta = dict(row["meta"])
    plan = SimpleNamespace(
        contract_symbol=row["contract"],
        limit_price=row["limit_price"],
        contracts=row["qty"],
        max_position_usd=row["reserved_cost"],
        side="CALL",
        direction="CALL",
        execution_mode="live",
        client_id=_CLIENT,
        signal_id=row["signal_id"],
        trigger_price=230.0,
        stop_underlying=225.0,
        target_underlying=240.0,
        metadata=meta,
        ticker=row["symbol"],
    )
    owner = meta["recovery_submit_owner"]
    return SimpleNamespace(
        ticker=row["symbol"],
        trigger_price=230.0,
        signal={
            "_approved_plan": plan,
            "local_order_id": row["local_order_id"],
            "signal_id": row["signal_id"],
            "client_id": _CLIENT,
            "execution_mode": "live",
            "contract_symbol": row["contract"],
            "contract_deferred": False,
            "materialization_generation": row["meta"]["materialization_generation"],
            "recovery_submit_owner": owner,
            "metadata": meta,
        },
    )


def _recovery_watcher_core(row: dict):
    osm = MagicMock()
    osm.get_order.return_value = row
    core = SimpleNamespace(
        client_id=_CLIENT,
        email=_CLIENT,
        execution_mode="live",
        mode="LIVE",
        order_state_machine=osm,
        broker=MagicMock(),
        store=MagicMock(),
    )
    core._is_real_occ_contract = ap_execution_core.APExecutionCore._is_real_occ_contract
    core._plan_is_deferred = ap_execution_core.APExecutionCore._plan_is_deferred
    core._breach_risk_check = MagicMock(return_value=False)
    core._cleanup_pending_entry_order = MagicMock(return_value=False)
    core._emit_breach_diag = MagicMock()
    core._on_entry_trigger = ap_execution_core.APExecutionCore._on_entry_trigger.__get__(
        core, type(core)
    )
    return core, osm


def _remote(row: dict, *, broker_id: str = "TR-ACTIVE", status: str = "open", **extra):
    value = {
        "id": broker_id,
        "tag": row["meta"]["broker_submit_key"],
        "option_symbol": row["contract"],
        "side": "buy_to_open",
        "quantity": "1",
        "status": status,
    }
    value.update(extra)
    return value


def test_adoption_transition_failure_retains_owner_and_never_posts():
    row = _intent_row()
    core, osm, broker = _core_for(row)
    osm.get_order.side_effect = [row, row]
    osm.transition.return_value = False
    broker.list_orders.return_value = [_remote(row, status="filled")]

    result = core.reconcile_deferred_broker_intent(local_order_id=_AAPL_ID)

    assert result["disposition"] == "RECONCILE_PENDING"
    assert result["reason_code"] == "RECONCILE_ADOPTION_TRANSITION_FAILED"
    assert result["broker_submit_owner_retained"] is True
    osm.retain_broker_submit_owner_for_reconciliation.assert_called_once()
    osm.update_order_meta.assert_not_called()
    broker.place_order.assert_not_called()


def test_adoption_metadata_cas_cannot_rewrite_a_concurrent_filled_row():
    row = _intent_row()
    core, osm, broker = _stateful_adoption_core(row)
    osm.advance_to_filled_before_meta = True

    result = core.reconcile_deferred_broker_intent(local_order_id=_AAPL_ID)

    assert result["disposition"] == "ALREADY_RECONCILED"
    assert osm.row["status"] == "FILLED"
    assert osm.row["meta"]["current_owner"] == (
        f"broker_submit:{row['meta']['broker_submit_key']}"
    )
    assert osm.meta_calls[0][2]["expected_status"] == "SUBMITTED"
    assert osm.meta_calls[0][2]["expected_execution_mode"] == "live"
    assert osm.meta_calls[0][2]["expected_signal_id"] == row["signal_id"]
    broker.place_order.assert_not_called()


def test_found_filled_flows_through_real_fill_monitor_side_effects(monkeypatch):
    row = _intent_row()
    core, osm, broker = _stateful_adoption_core(row, broker_status="filled")
    result = core.reconcile_deferred_broker_intent(local_order_id=_AAPL_ID)

    assert result["status"] == "SUBMITTED"
    broker.get_order.return_value = {
        "status": "FILLED",
        "exec_quantity": 1,
        "avg_fill_price": 2.40,
    }

    open_position = MagicMock(return_value="position-1")
    bind_identity = MagicMock(return_value=(True, "bound"))
    seed_exit = MagicMock(return_value=(True, "seeded"))
    release_guards = MagicMock()
    monkeypatch.setattr(fill_monitor, "emit_fill_event", MagicMock())
    monkeypatch.setattr(fill_monitor, "audit", MagicMock())
    monkeypatch.setattr(fill_monitor, "trace_gate", MagicMock())
    monkeypatch.setattr(fill_monitor, "_cancel_pair_opposite", MagicMock())
    monkeypatch.setattr(fill_monitor, "_open_position_safe", open_position)
    monkeypatch.setattr(
        fill_monitor,
        "_bind_filled_entry_durable_identity",
        bind_identity,
    )
    monkeypatch.setattr(fill_monitor, "_seed_exit_engine", seed_exit)
    monkeypatch.setattr(fill_monitor, "_release_entry_guards", release_guards)

    fill_monitor.process_pending_order(
        broker,
        osm.get_order(_AAPL_ID),
        osm=osm,
        pm=object(),
        exit_engine=object(),
        runtime_execution_mode="live",
    )

    assert [call[1] for call in osm.transition_calls] == ["SUBMITTED", "FILLED"]
    open_position.assert_called_once()
    bind_identity.assert_called_once()
    seed_exit.assert_called_once()
    release_guards.assert_called_once()
    broker.place_order.assert_not_called()


@pytest.mark.parametrize(
    ("broker_status", "expected_status", "filled_qty"),
    [
        ("partially_filled", "PARTIAL_FILL", 1),
        ("rejected", "REJECTED", 0),
        ("canceled", "CANCELED", 0),
        ("expired", "EXPIRED", 0),
    ],
)
def test_found_nonfilled_statuses_wait_for_fill_monitor_transition(
    monkeypatch,
    broker_status,
    expected_status,
    filled_qty,
):
    row = _intent_row()
    core, osm, broker = _stateful_adoption_core(
        row,
        broker_status=broker_status,
    )
    result = core.reconcile_deferred_broker_intent(local_order_id=_AAPL_ID)

    assert result["status"] == "SUBMITTED"
    broker.get_order.return_value = {
        "status": broker_status.upper(),
        "exec_quantity": filled_qty,
        "avg_fill_price": 2.40,
        "reason": broker_status,
    }
    open_position = MagicMock(return_value="position-should-not-exist")
    release_guards = MagicMock()
    monkeypatch.setattr(fill_monitor, "emit_fill_event", MagicMock())
    monkeypatch.setattr(fill_monitor, "audit", MagicMock())
    monkeypatch.setattr(fill_monitor, "trace_gate", MagicMock())
    monkeypatch.setattr(fill_monitor, "_cancel_pair_opposite", MagicMock())
    monkeypatch.setattr(fill_monitor, "_open_position_safe", open_position)
    monkeypatch.setattr(
        fill_monitor,
        "_bind_filled_entry_durable_identity",
        MagicMock(return_value=(True, "bound")),
    )
    monkeypatch.setattr(
        fill_monitor,
        "_seed_exit_engine",
        MagicMock(return_value=(True, "seeded")),
    )
    monkeypatch.setattr(fill_monitor, "_release_entry_guards", release_guards)

    fill_monitor.process_pending_order(
        broker,
        osm.get_order(_AAPL_ID),
        osm=osm,
        pm=object(),
        exit_engine=object(),
        runtime_execution_mode="live",
    )

    assert [call[1] for call in osm.transition_calls] == [
        "SUBMITTED",
        expected_status,
    ]
    open_position.assert_not_called()
    if expected_status in {"REJECTED", "CANCELED", "EXPIRED"}:
        release_guards.assert_called_once()
    else:
        release_guards.assert_not_called()
    broker.place_order.assert_not_called()


class _TradierResponse:
    def __init__(self, payload, *, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code
        self.content = b"{}"

    def raise_for_status(self):
        if self.status_code >= 400:
            raise ValueError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


def _transport_broker(get_side_effect) -> TradierBroker:
    broker = TradierBroker(
        TradierConfig(
            base_url="https://api.tradier.com",
            access_token="test-token",
            account_id="acct",
        )
    )
    broker.session.get = MagicMock(side_effect=get_side_effect)
    broker.session.post = MagicMock()
    return broker


def test_tradier_list_orders_requests_tagged_page_one():
    """The exact-tag reconciler must request tags and the full first page."""
    broker = _transport_broker(
        [_TradierResponse({"orders": {"order": []}})]
    )

    assert broker.list_orders() == []

    broker.session.get.assert_called_once()
    call = broker.session.get.call_args
    assert call.args[0] == "https://api.tradier.com/v1/accounts/acct/orders"
    assert call.kwargs["params"] == {
        "includeTags": "true",
        "limit": 1500,
        "page": 1,
    }


def test_actual_tradier_page_two_tag_is_adopted_without_post():
    """A tag found on page 2 must reach the real reconciler as FOUND."""
    import ap.brokers.tradier as tradier_module

    row = _intent_row()
    page_one = [
        {"id": f"TR-OTHER-{index}", "tag": f"other-{index}"}
        for index in range(tradier_module.TRADIER_ORDERS_PAGE_SIZE)
    ]
    page_two = [_remote(row, broker_id="TR-AAPL-PAGE-2")]
    broker = _transport_broker(
        [
            _TradierResponse({"orders": {"order": page_one}}),
            _TradierResponse({"orders": {"order": page_two}}),
        ]
    )
    core, osm, _unused_mock_broker = _core_for(row)
    core.broker = broker

    result = core.reconcile_deferred_broker_intent(
        local_order_id=row["local_order_id"]
    )

    assert result["broker_truth"] == "FOUND"
    assert result["broker_order_id"] == "TR-AAPL-PAGE-2"
    osm.transition.assert_called_once()
    broker.session.post.assert_not_called()
    assert broker.session.get.call_count == 2
    for expected_page, call in enumerate(broker.session.get.call_args_list, 1):
        assert call.args[0] == "https://api.tradier.com/v1/accounts/acct/orders"
        assert call.kwargs["params"] == {
            "includeTags": "true",
            "limit": 1500,
            "page": expected_page,
        }


def test_actual_tradier_complete_pages_without_tag_retain_owner_after_page_two():
    """A complete paginated no-tag result is only a no-match observation."""
    import ap.brokers.tradier as tradier_module

    row = _intent_row(
        intent_at=(datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
    )
    page_one = [
        {"id": f"TR-OTHER-{index}", "tag": f"other-{index}"}
        for index in range(tradier_module.TRADIER_ORDERS_PAGE_SIZE)
    ]
    page_two = [{"id": "TR-OTHER-1500", "tag": "other-1500"}]
    broker = _transport_broker(
        [
            _TradierResponse({"orders": {"order": page_one}}),
            _TradierResponse({"orders": {"order": page_two}}),
        ]
    )
    core, osm, _unused_mock_broker = _core_for(row)
    core.broker = broker

    result = core.reconcile_deferred_broker_intent(
        local_order_id=row["local_order_id"]
    )

    assert result["disposition"] == "RECONCILE_PENDING"
    assert result["reason_code"] == "RECONCILE_BROKER_NO_MATCH_OBSERVED"
    assert result["broker_truth"] == "NO_MATCH_OBSERVED"
    assert result["broker_submit_owner_retained"] is True
    osm.retain_broker_submit_owner_for_reconciliation.assert_called_once()
    core.resume_deferred_broker_ready_order.assert_not_called()
    broker.session.post.assert_not_called()
    assert [
        call.kwargs["params"]["page"]
        for call in broker.session.get.call_args_list
    ] == [1, 2]


def test_actual_tradier_query_error_is_unknown_and_never_not_found():
    """Transport/auth failure must retain broker ownership, not release it."""
    row = _intent_row()
    broker = _transport_broker(RuntimeError("Tradier auth unavailable"))
    core, osm, _unused_mock_broker = _core_for(row)
    core.broker = broker

    result = core.reconcile_deferred_broker_intent(
        local_order_id=row["local_order_id"]
    )

    assert result["broker_truth"] == "UNKNOWN"
    assert result["disposition"] == "RECONCILE_PENDING"
    assert result["reason_code"].startswith("RECONCILE_BROKER_QUERY_FAILED")
    osm.retain_broker_submit_owner_for_reconciliation.assert_called_once()
    core.resume_deferred_broker_ready_order.assert_not_called()
    broker.session.post.assert_not_called()


def test_actual_tradier_malformed_page_is_unknown_and_never_not_found():
    """A malformed page must fail closed before bounded NOT_FOUND logic."""
    row = _intent_row()
    broker = _transport_broker(
        [_TradierResponse({"orders": {"order": [{"id": "ok"}, "bad"]}})]
    )
    core, osm, _unused_mock_broker = _core_for(row)
    core.broker = broker

    result = core.reconcile_deferred_broker_intent(
        local_order_id=row["local_order_id"]
    )

    assert result["broker_truth"] == "UNKNOWN"
    assert result["reason_code"].startswith("RECONCILE_BROKER_QUERY_FAILED")
    osm.retain_broker_submit_owner_for_reconciliation.assert_called_once()
    core.resume_deferred_broker_ready_order.assert_not_called()
    broker.session.post.assert_not_called()


def test_actual_tradier_pagination_ceiling_is_unknown_and_never_not_found(
    monkeypatch,
):
    """A full page at the hard ceiling cannot be treated as complete."""
    # Patch the function's actual globals so this remains correct even when
    # the full suite has loaded the broker module through another package
    # alias.
    page_size = TradierBroker.list_orders.__globals__["TRADIER_ORDERS_PAGE_SIZE"]
    monkeypatch.setitem(
        TradierBroker.list_orders.__globals__, "TRADIER_ORDERS_MAX_PAGES", 2
    )
    row = _intent_row()
    full_page = [
        {"id": f"TR-OTHER-{index}", "tag": f"other-{index}"}
        for index in range(page_size)
    ]
    broker = _transport_broker(
        [
            _TradierResponse({"orders": {"order": full_page}}),
            _TradierResponse({"orders": {"order": full_page}}),
        ]
    )
    core, osm, _unused_mock_broker = _core_for(row)
    core.broker = broker

    result = core.reconcile_deferred_broker_intent(
        local_order_id=row["local_order_id"]
    )

    assert result["broker_truth"] == "UNKNOWN"
    assert result["reason_code"].startswith("RECONCILE_BROKER_QUERY_FAILED")
    osm.retain_broker_submit_owner_for_reconciliation.assert_called_once()
    core.resume_deferred_broker_ready_order.assert_not_called()
    broker.session.post.assert_not_called()
    assert [
        call.kwargs["params"]["page"]
        for call in broker.session.get.call_args_list
    ] == [1, 2]


@pytest.mark.parametrize(
    "intent_age",
    [timedelta(seconds=31), timedelta(minutes=10)],
    ids=["31_seconds_old", "10_minutes_old"],
)
def test_actual_tradier_complete_empty_result_retains_and_never_posts(intent_age):
    """Age never turns a clean empty query into permission for another POST."""
    row = _intent_row(
        intent_at=(datetime.now(timezone.utc) - intent_age).isoformat()
    )
    broker = _transport_broker(
        [_TradierResponse({"orders": {"order": []}})]
    )
    core, osm, _unused_mock_broker = _core_for(row)
    core.broker = broker
    result = core.reconcile_deferred_broker_intent(
        local_order_id=row["local_order_id"]
    )

    assert result["disposition"] == "RECONCILE_PENDING"
    assert result["reason_code"] == "RECONCILE_BROKER_NO_MATCH_OBSERVED"
    assert result["broker_truth"] == "NO_MATCH_OBSERVED"
    assert result["broker_submit_owner_retained"] is True
    osm.retain_broker_submit_owner_for_reconciliation.assert_called_once()
    core.resume_deferred_broker_ready_order.assert_not_called()
    broker.session.post.assert_not_called()
    broker.session.get.assert_called_once()
    assert broker.session.get.call_args.kwargs["params"] == {
        "includeTags": "true",
        "limit": 1500,
        "page": 1,
    }


def test_repeated_empty_queries_retain_owner_until_late_page_two_tag():
    """Repeated clean misses stay fenced until a later exact tag is found."""
    import ap.brokers.tradier as tradier_module

    row = _intent_row(
        intent_at=(datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
    )
    page_one = [
        {"id": f"TR-OTHER-{index}", "tag": f"other-{index}"}
        for index in range(tradier_module.TRADIER_ORDERS_PAGE_SIZE)
    ]
    broker = _transport_broker(
        [
            _TradierResponse({"orders": {"order": []}}),
            _TradierResponse({"orders": {"order": []}}),
            _TradierResponse({"orders": {"order": page_one}}),
            _TradierResponse(
                {"orders": {"order": [_remote(row, broker_id="TR-LATE-PAGE-2")]}}
            ),
        ]
    )
    core, osm, _unused_mock_broker = _core_for(row)
    core.broker = broker

    first = core.reconcile_deferred_broker_intent(local_order_id=row["local_order_id"])
    second = core.reconcile_deferred_broker_intent(local_order_id=row["local_order_id"])
    found = core.reconcile_deferred_broker_intent(local_order_id=row["local_order_id"])

    for result in (first, second):
        assert result["disposition"] == "RECONCILE_PENDING"
        assert result["reason_code"] == "RECONCILE_BROKER_NO_MATCH_OBSERVED"
        assert result["broker_truth"] == "NO_MATCH_OBSERVED"
        assert result["broker_submit_owner_retained"] is True
    assert found["disposition"] == "ALREADY_RECONCILED"
    assert found["broker_truth"] == "FOUND"
    assert found["broker_order_id"] == "TR-LATE-PAGE-2"
    assert osm.retain_broker_submit_owner_for_reconciliation.call_count == 2
    core.resume_deferred_broker_ready_order.assert_not_called()
    osm.transition.assert_called_once()
    broker.session.post.assert_not_called()
    assert [
        call.kwargs["params"]["page"]
        for call in broker.session.get.call_args_list
    ] == [1, 1, 1, 2]


@pytest.mark.parametrize(
    ("local_order_id", "symbol", "contract"),
    [
        (_AAPL_ID, "AAPL", "AAPL260904C00230000"),
        (_CSCO_ID, "CSCO", "CSCO260904C00075000"),
    ],
)
def test_production_jason_shapes_retain_exact_owner_on_query_error(
    local_order_id, symbol, contract
):
    row = _intent_row(
        local_order_id=local_order_id,
        symbol=symbol,
        contract=contract,
    )
    core, osm, broker = _core_for(row)
    broker.list_orders.side_effect = ValueError("Tradier order node malformed")

    result = core.reconcile_deferred_broker_intent(local_order_id=local_order_id)

    assert result["disposition"] == "RECONCILE_PENDING"
    assert result["broker_truth"] == "UNKNOWN"
    assert result["broker_submit_key"] == row["meta"]["broker_submit_key"]
    assert result["exception_class"] == "ValueError"
    assert result["exception_message"] == "Tradier order node malformed"
    assert result["reconciliation_stage"] == "broker_order_query"
    osm.retain_broker_submit_owner_for_reconciliation.assert_called_once()
    assert osm.retain_broker_submit_owner_for_reconciliation.call_args.kwargs[
        "broker_submit_key"
    ] == row["meta"]["broker_submit_key"]
    broker.place_order.assert_not_called()


def test_exact_tag_found_is_adopted_once_and_never_posted():
    row = _intent_row()
    after_adoption = {
        **row,
        "status": "SUBMITTED",
        "broker_order_id": "TR-AAPL-1",
        "submitted_ts": datetime.now(timezone.utc).isoformat(),
    }
    core, osm, broker = _core_for(row)
    osm.get_order.side_effect = [row, after_adoption]
    broker.list_orders.return_value = [_remote(row)]
    osm.transition.return_value = True

    first = core.reconcile_deferred_broker_intent(local_order_id=_AAPL_ID)
    second = core.reconcile_deferred_broker_intent(local_order_id=_AAPL_ID)

    assert first["disposition"] == "ALREADY_RECONCILED"
    assert first["reason_code"] == "BROKER_ORDER_ADOPTED"
    assert first["broker_order_id"] == "TR-ACTIVE"
    assert second["disposition"] == "ALREADY_RECONCILED"
    assert second["reason_code"] == "RECONCILE_BROKER_ORDER_PRESENT"
    assert osm.transition.call_count == 1
    assert osm.update_order_meta.call_args.args[1]["current_owner"] == "ORDER_MONITOR"
    broker.place_order.assert_not_called()
    assert broker.list_orders.call_count == 1


def test_no_match_observed_retains_owner_without_resume_or_post():
    row = _intent_row(intent_at=datetime.now(timezone.utc).isoformat())
    core, osm, broker = _core_for(row)
    broker.list_orders.return_value = []

    result = core.reconcile_deferred_broker_intent(local_order_id=_AAPL_ID)

    assert result["disposition"] == "RECONCILE_PENDING"
    assert result["reason_code"] == "RECONCILE_BROKER_NO_MATCH_OBSERVED"
    assert result["broker_truth"] == "NO_MATCH_OBSERVED"
    assert result["broker_submit_owner_retained"] is True
    assert result["next_retry_at"]
    osm.retain_broker_submit_owner_for_reconciliation.assert_called_once()
    core.resume_deferred_broker_ready_order.assert_not_called()
    broker.place_order.assert_not_called()


def test_no_match_observed_after_any_age_remains_fail_closed_without_resume():
    row = _intent_row(
        intent_at=(datetime.now(timezone.utc) - timedelta(minutes=2)).isoformat()
    )
    core, osm, broker = _core_for(row)
    broker.list_orders.return_value = []

    result = core.reconcile_deferred_broker_intent(local_order_id=_AAPL_ID)

    assert result["disposition"] == "RECONCILE_PENDING"
    assert result["reason_code"] == "RECONCILE_BROKER_NO_MATCH_OBSERVED"
    assert result["broker_truth"] == "NO_MATCH_OBSERVED"
    assert result["broker_submit_owner_retained"] is True
    assert result["next_retry_at"]
    osm.retain_broker_submit_owner_for_reconciliation.assert_called_once()
    core.resume_deferred_broker_ready_order.assert_not_called()
    core.submit_existing_entry.assert_not_called()
    broker.place_order.assert_not_called()


@pytest.mark.parametrize(
    ("broker_result", "expected_class", "expected_message"),
    [
        (TimeoutError("Tradier timeout"), "TimeoutError", "Tradier timeout"),
        (ValueError("HTTP 502"), "ValueError", "HTTP 502"),
        (["not-an-order-object"], "ValueError", "broker order response contains a non-object"),
    ],
)
def test_unknown_or_malformed_broker_truth_is_retryable_and_fail_closed(
    broker_result, expected_class, expected_message
):
    row = _intent_row()
    core, osm, broker = _core_for(row)
    if isinstance(broker_result, Exception):
        broker.list_orders.side_effect = broker_result
    else:
        broker.list_orders.return_value = broker_result

    result = core.reconcile_deferred_broker_intent(local_order_id=_AAPL_ID)

    assert result["disposition"] == "RECONCILE_PENDING"
    assert result["broker_truth"] == "UNKNOWN"
    assert result["exception_class"] == expected_class
    assert result["exception_message"] == expected_message
    assert result["reconciliation_stage"] == "broker_order_query"
    assert result["next_retry_at"]
    assert osm.retain_broker_submit_owner_for_reconciliation.call_count == 1
    broker.place_order.assert_not_called()


def test_matching_tag_with_malformed_broker_identity_is_not_adopted():
    row = _intent_row()
    core, osm, broker = _core_for(row)
    broker.list_orders.return_value = [_remote(row, broker_id={"unexpected": "object"})]

    result = core.reconcile_deferred_broker_intent(local_order_id=_AAPL_ID)

    assert result["disposition"] == "RECONCILE_PENDING"
    assert result["reason_code"] == "RECONCILE_MATCH_MISSING_ORDER_ID"
    osm.transition.assert_not_called()
    broker.place_order.assert_not_called()


def test_malformed_unrelated_tag_is_unknown_not_proven_no_match():
    row = _intent_row(
        intent_at=(datetime.now(timezone.utc) - timedelta(minutes=2)).isoformat()
    )
    core, osm, broker = _core_for(row)
    broker.list_orders.return_value = [{"id": "TR-OTHER", "tag": ["unexpected"]}]

    result = core.reconcile_deferred_broker_intent(local_order_id=_AAPL_ID)

    assert result["disposition"] == "RECONCILE_PENDING"
    assert result["reason_code"] == (
        "RECONCILE_BROKER_RESPONSE_MALFORMED:ValueError:invalid tag"
    )
    assert result["broker_truth"] == "UNKNOWN"
    core.resume_deferred_broker_ready_order.assert_not_called()
    broker.place_order.assert_not_called()


@pytest.mark.parametrize("field", ["current_owner", "broker_submit_key", "broker_submit_payload_hash"])
def test_submit_identity_mismatch_fails_closed_before_broker_query(field):
    row = _intent_row()
    if field == "current_owner":
        row["meta"][field] = "broker_submit:other"
    elif field == "broker_submit_key":
        row["meta"][field] = "other-key"
    else:
        row["meta"][field] = "wrong-hash"
    core, osm, broker = _core_for(row)

    result = core.reconcile_deferred_broker_intent(local_order_id=_AAPL_ID)

    assert result["disposition"] == "KEEP_WATCHER"
    assert result["reason_code"].startswith(
        "RECONCILE_SUBMIT_INTENT_IDENTITY_UNPROVEN"
    )
    broker.list_orders.assert_not_called()
    osm.transition.assert_not_called()


def test_paper_row_cannot_be_adopted_from_live_tradier():
    row = _intent_row(execution_mode="paper")
    core, osm, broker = _core_for(row, mode="paper")
    broker.list_orders.return_value = [_remote(row)]

    result = core.reconcile_deferred_broker_intent(local_order_id=_AAPL_ID)

    assert result["disposition"] == "KEEP_WATCHER"
    assert result["reason_code"] == "RECONCILE_LIVE_ONLY_EXECUTION_MODE_REQUIRED"
    broker.list_orders.assert_not_called()
    broker.place_order.assert_not_called()
    osm.transition.assert_not_called()


@pytest.mark.parametrize(
    "payload",
    [
        None,
        {"orders": "malformed"},
        {"orders": {"order": "malformed"}},
        {"orders": {"order": [None]}},
    ],
)
def test_tradier_order_adapter_propagates_malformed_truth(payload):
    from ap.brokers.tradier import TradierBroker

    adapter = SimpleNamespace(
        cfg=SimpleNamespace(account_id="acct"),
        _get=MagicMock(return_value=payload),
    )

    with pytest.raises(ValueError, match="TRADIER_ORDERS_PAYLOAD_MALFORMED"):
        TradierBroker.list_orders(adapter)


def test_concurrent_reconcilers_retain_owner_without_duplicate_post():
    """Concurrent clean misses retain one durable owner and make zero POSTs."""
    row = _intent_row(
        intent_at=(datetime.now(timezone.utc) - timedelta(minutes=2)).isoformat()
    )
    core, osm, broker = _core_for(row)
    responses = [
        _TradierResponse({"orders": {"order": []}}),
        _TradierResponse({"orders": {"order": []}}),
    ]
    response_lock = threading.Lock()
    query_barrier = threading.Barrier(2)

    def _concurrent_get(*_args, **_kwargs):
        query_barrier.wait(timeout=5)
        with response_lock:
            return responses.pop()

    broker = _transport_broker([])
    broker.session.get = MagicMock(side_effect=_concurrent_get)
    core.broker = broker
    results = []
    errors = []
    result_lock = threading.Lock()

    def _reconcile():
        try:
            result = core.reconcile_deferred_broker_intent(
                local_order_id=row["local_order_id"]
            )
            with result_lock:
                results.append(result)
        except Exception as exc:  # pragma: no cover - assertion reports it
            with result_lock:
                errors.append(exc)

    threads = [threading.Thread(target=_reconcile) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert errors == []
    assert len(results) == 2
    for result in results:
        assert result["disposition"] == "RECONCILE_PENDING"
        assert result["reason_code"] == "RECONCILE_BROKER_NO_MATCH_OBSERVED"
        assert result["broker_truth"] == "NO_MATCH_OBSERVED"
        assert result["broker_submit_owner_retained"] is True
    assert osm.retain_broker_submit_owner_for_reconciliation.call_count == 2
    core.resume_deferred_broker_ready_order.assert_not_called()
    broker.session.post.assert_not_called()
    assert broker.session.get.call_count == 2


def test_watcher_routes_durable_submitting_row_directly_to_reconciler():
    row = _intent_row()
    osm = MagicMock()
    osm.get_order.return_value = row
    reconcile = MagicMock(
        return_value={
            "disposition": "RECONCILE_PENDING",
            "reason_code": "RECONCILE_BROKER_QUERY_FAILED:ValueError:bad",
            "next_retry_at": "2026-08-31T16:01:00+00:00",
        }
    )
    core = SimpleNamespace(
        client_id=_CLIENT,
        email=_CLIENT,
        execution_mode="live",
        mode="LIVE",
        order_state_machine=osm,
        reconcile_deferred_broker_intent=reconcile,
        submit_existing_entry=MagicMock(),
    )
    core._on_entry_trigger = ap_execution_core.APExecutionCore._on_entry_trigger.__get__(
        core, type(core)
    )
    watched = SimpleNamespace(
        ticker="AAPL",
        trigger_price=230.0,
        signal={
            "local_order_id": _AAPL_ID,
            "signal_id": row["signal_id"],
            "execution_mode": "live",
            "materialization_generation": row["meta"]["materialization_generation"],
        },
    )

    result = core._on_entry_trigger(watched)

    assert result["disposition"] == "RECONCILE_BROKER_INTENT"
    assert result["reason_code"].startswith("RECONCILE_BROKER_QUERY_FAILED")
    reconcile.assert_called_once_with(local_order_id=_AAPL_ID)
    core.submit_existing_entry.assert_not_called()


@pytest.mark.parametrize(
    ("local_order_id", "symbol", "contract"),
    [
        (_AAPL_ID, "AAPL", "AAPL260904C00230000"),
        (_CSCO_ID, "CSCO", "CSCO260904C00075000"),
    ],
)
def test_order_monitor_stale_pending_trigger_routes_broker_handoff_end_to_end(
    monkeypatch, caplog, local_order_id, symbol, contract
):
    """The actual stale order-monitor caller must stay out of watcher rearm.

    This is the production-shaped path that previously reached
    ``_canonical_pending_trigger_rearm`` and could emit
    ``RESTART_RECOVERY_MISSING_DURABLE_CLIENT_ID`` before the broker-submit
    reconciler saw the row.  Run the same stale row through UNKNOWN, FOUND,
    and complete empty broker-query outcomes to bind the caller to the #562 contract.
    """
    import ap.order_monitor as order_monitor_module
    import ap.pending_trigger_restart_recovery as restart_recovery_module

    monkeypatch.setattr(order_monitor_module, "PENDING_TRIGGER_MAX_AGE_SECONDS", 1)
    row = _intent_row(
        local_order_id=local_order_id,
        symbol=symbol,
        contract=contract,
    )
    row["created_ts"] = datetime.now(timezone.utc) - timedelta(minutes=10)
    core, osm, broker = _core_for(row)
    reconcile = MagicMock(wraps=core.reconcile_deferred_broker_intent)
    core.reconcile_deferred_broker_intent = reconcile
    watcher = MagicMock()
    watcher.has_order.return_value = False
    watcher.watch.return_value = True
    monitor = APOrderMonitor(
        client_id=_CLIENT,
        broker=broker,
        order_state_machine=osm,
        position_manager=MagicMock(),
        entry_watcher=watcher,
        client_mode="LIVE",
        execution_core=core,
    )
    monitor._get_active_entry_orders = MagicMock(return_value=[row])
    monitor._maybe_hydrate_deferred_order = MagicMock(
        side_effect=AssertionError("broker-owned handoff must bypass hydration")
    )
    monitor._canonical_pending_trigger_rearm = MagicMock(
        side_effect=AssertionError("broker-owned handoff must bypass watcher recovery")
    )
    pending_recovery = MagicMock(
        side_effect=AssertionError("broker-owned handoff must bypass restart recovery")
    )
    monkeypatch.setattr(
        restart_recovery_module.PendingTriggerRestartRecovery,
        "recover_one_row",
        pending_recovery,
    )
    caplog.set_level("INFO", logger="ap.order_monitor")

    # UNKNOWN broker truth: retain the exact broker-submit owner and do not
    # probe/rearm the watcher or attempt a replacement POST.
    broker.list_orders.side_effect = ValueError("broker unavailable")
    monitor._check_entry_orders()

    reconcile.assert_called_once_with(local_order_id=local_order_id)
    osm.retain_broker_submit_owner_for_reconciliation.assert_called_once()
    retained_kwargs = osm.retain_broker_submit_owner_for_reconciliation.call_args.kwargs
    assert retained_kwargs["broker_submit_key"] == row["meta"]["broker_submit_key"]
    watcher.has_order.assert_not_called()
    watcher.watch.assert_not_called()
    monitor._canonical_pending_trigger_rearm.assert_not_called()
    pending_recovery.assert_not_called()
    osm.retain_recovery_ownership_if_no_watcher.assert_not_called()
    broker.place_order.assert_not_called()
    assert "RESTART_RECOVERY_MISSING_DURABLE_CLIENT_ID" not in caplog.text

    # FOUND broker truth: the canonical reconciler adopts exactly once, still
    # without watcher rearm or a new broker POST.
    osm.retain_broker_submit_owner_for_reconciliation.reset_mock()
    osm.transition.reset_mock()
    osm.update_order_meta.reset_mock()
    broker.list_orders.side_effect = None
    broker.list_orders.return_value = [
        _remote(row, broker_id=f"TR-{symbol}-FOUND")
    ]
    monitor._check_entry_orders()

    assert reconcile.call_count == 2
    osm.transition.assert_called_once()
    assert osm.transition.call_args.args[:2] == (local_order_id, "SUBMITTED")
    broker.place_order.assert_not_called()
    watcher.has_order.assert_not_called()
    watcher.watch.assert_not_called()
    monitor._canonical_pending_trigger_rearm.assert_not_called()
    pending_recovery.assert_not_called()
    osm.retain_recovery_ownership_if_no_watcher.assert_not_called()
    assert "RESTART_RECOVERY_MISSING_DURABLE_CLIENT_ID" not in caplog.text

    # A clean empty result remains only a no-match observation even after the
    # intent is old.  The order-monitor caller must keep the exact fence and
    # never enter canonical resume from this ambiguous-POST recovery path.
    row["meta"]["submit_intent_at"] = (
        datetime.now(timezone.utc) - timedelta(minutes=2)
    ).isoformat()
    broker.list_orders.return_value = []
    core.resume_deferred_broker_ready_order.reset_mock()
    osm.retain_broker_submit_owner_for_reconciliation.reset_mock()
    monitor._check_entry_orders()

    assert reconcile.call_count == 3
    osm.retain_broker_submit_owner_for_reconciliation.assert_called_once()
    core.resume_deferred_broker_ready_order.assert_not_called()
    broker.place_order.assert_not_called()
    core.submit_existing_entry.assert_not_called()
    watcher.has_order.assert_not_called()
    watcher.watch.assert_not_called()
    monitor._canonical_pending_trigger_rearm.assert_not_called()
    pending_recovery.assert_not_called()
    osm.retain_recovery_ownership_if_no_watcher.assert_not_called()
    assert "RESTART_RECOVERY_MISSING_DURABLE_CLIENT_ID" not in caplog.text


def test_order_monitor_paper_handoff_never_enters_live_reconciler(
    monkeypatch, caplog
):
    """A PAPER row cannot be routed into the LIVE broker reconciler."""
    import ap.order_monitor as order_monitor_module

    monkeypatch.setattr(order_monitor_module, "PENDING_TRIGGER_MAX_AGE_SECONDS", 1)
    row = _intent_row(
        local_order_id=_AAPL_ID,
        symbol="AAPL",
        contract="AAPL260904C00230000",
        execution_mode="paper",
    )
    row["created_ts"] = datetime.now(timezone.utc) - timedelta(minutes=10)
    core, osm, broker = _core_for(row)
    reconcile = MagicMock(
        side_effect=AssertionError("PAPER must not enter live reconciliation")
    )
    core.reconcile_deferred_broker_intent = reconcile
    watcher = MagicMock()
    monitor = APOrderMonitor(
        client_id=_CLIENT,
        broker=broker,
        order_state_machine=osm,
        position_manager=MagicMock(),
        entry_watcher=watcher,
        client_mode="PAPER",
        execution_core=core,
    )
    monitor._get_active_entry_orders = MagicMock(return_value=[row])
    monitor._maybe_hydrate_deferred_order = MagicMock(
        side_effect=AssertionError("broker-owned handoff must bypass hydration")
    )
    monitor._canonical_pending_trigger_rearm = MagicMock(
        side_effect=AssertionError("PAPER handoff must not rearm watcher")
    )
    caplog.set_level("INFO", logger="ap.order_monitor")

    monitor._check_entry_orders()

    reconcile.assert_not_called()
    watcher.has_order.assert_not_called()
    watcher.watch.assert_not_called()
    monitor._canonical_pending_trigger_rearm.assert_not_called()
    osm.retain_broker_submit_owner_for_reconciliation.assert_not_called()
    osm.retain_recovery_ownership_if_no_watcher.assert_not_called()
    broker.list_orders.assert_not_called()
    broker.place_order.assert_not_called()
    assert "PENDING_TRIGGER_BROKER_HANDOFF_NON_LIVE_MODE" in caplog.text


def test_recovery_submit_owner_with_future_lease_keeps_watcher():
    row = _recovery_ready_row(
        lease_until=(datetime.now(timezone.utc) + timedelta(seconds=60)).isoformat()
    )
    core, _osm = _recovery_watcher_core(row)

    result = core._on_entry_trigger(_recovery_watched(row))

    assert result == {
        "disposition": "KEEP_WATCHER",
        "reason_code": "RECOVERY_SUBMIT_OWNER_ACTIVE",
        "retry_after_seconds": 5,
    }
    core._breach_risk_check.assert_not_called()


def test_expired_recovery_submit_owner_reaches_existing_recovery_path():
    row = _recovery_ready_row(
        lease_until=(datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
    )
    core, _osm = _recovery_watcher_core(row)
    watched = _recovery_watched(row)

    result = core._on_entry_trigger(watched)

    assert result["reason_code"] == "breach_risk_check_false"
    assert result["reason_code"] != "RECOVERY_SUBMIT_OWNER_ACTIVE"
    core._breach_risk_check.assert_called_once_with(watched)


def test_expired_recovery_submit_owner_can_be_reclaimed_by_canonical_recovery():
    row = _recovery_ready_row(
        lease_until=(datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
    )
    after = {
        **row,
        "status": "SUBMITTED",
        "broker_order_id": "TR-RECOVERY-1",
        "submitted_ts": datetime.now(timezone.utc).isoformat(),
    }
    osm = MagicMock()
    osm.get_order.side_effect = [row, after]
    osm.claim_deferred_broker_ready_submit.return_value = True
    core = SimpleNamespace(
        client_id=_CLIENT,
        email=_CLIENT,
        execution_mode="live",
        order_state_machine=osm,
    )
    core._is_real_occ_contract = ap_execution_core.APExecutionCore._is_real_occ_contract
    core._on_entry_trigger = MagicMock()
    core.resume_deferred_broker_ready_order = (
        ap_execution_core.APExecutionCore.resume_deferred_broker_ready_order.__get__(
            core, type(core)
        )
    )

    result = core.resume_deferred_broker_ready_order(local_order_id=_AAPL_ID)

    assert result["disposition"] == "SUBMITTED"
    osm.claim_deferred_broker_ready_submit.assert_called_once()
    core._on_entry_trigger.assert_called_once()


def test_watcher_resolver_preserves_reconciler_retry():
    row = _intent_row()
    watcher = APEntryWatcher(None, order_state_machine=MagicMock(), mode="LIVE")
    watcher.order_state_machine.get_order.return_value = row
    next_retry = (datetime.now(timezone.utc) + timedelta(seconds=25)).isoformat()
    watched = SimpleNamespace(
        signal={"local_order_id": _AAPL_ID, "contract_symbol": "DEFERRED:AAPL"}
    )

    disposition, claimed_retry = watcher._resolve_trigger_callback_disposition(
        watched,
        {"disposition": "RECONCILE_BROKER_INTENT", "next_retry_at": next_retry},
    )

    assert disposition == "RECONCILE_BROKER_INTENT"
    assert claimed_retry == next_retry


class _RowsCursor:
    def __init__(self, rows):
        self.rows = rows
        self.rowcount = 1

    def execute(self, *_args, **_kwargs):
        return self

    def fetchall(self):
        return self.rows


class _RowsConnection:
    def __init__(self, rows):
        self.cursor = _RowsCursor(rows)

    def __enter__(self):
        return self.cursor

    def __exit__(self, *_args):
        return False


def test_recovery_retains_exact_broker_owner_without_generic_recovery_write(monkeypatch):
    row = _intent_row(
        local_order_id="recovery-live-1",
        symbol="CSCO",
        contract="CSCO260904C00075000",
    )
    row["meta"]["trigger_crossed_at"] = "2026-08-31T16:00:00+00:00"
    row["meta"]["trigger_crossed_at_provenance"]["local_order_id"] = "recovery-live-1"
    row["meta"]["canonical_signal_id"] = row["signal_id"]
    rows = [row]
    import ap.db as ap_db

    monkeypatch.setattr(ap_db, "conn", lambda: _RowsConnection(rows))
    monkeypatch.setattr(ap_db, "run_with_retry", lambda fn, *args, **kwargs: fn())
    osm = MagicMock()
    osm.client_id = _CLIENT
    osm.get_order.return_value = row
    osm.retain_broker_submit_owner_for_reconciliation.return_value = True
    osm.retain_recovery_ownership_if_no_watcher.return_value = False
    osm.update_order_meta.return_value = True
    ec = SimpleNamespace(
        reconcile_deferred_broker_intent=MagicMock(
            return_value={
                "disposition": "RECONCILE_PENDING",
                "reason_code": "RECONCILE_BROKER_QUERY_FAILED:ValueError:bad",
                "reconciliation_stage": "broker_order_query",
                "next_retry_at": "2026-08-31T16:01:00+00:00",
                "exception_class": "ValueError",
                "exception_message": "bad",
            }
        )
    )
    watcher = SimpleNamespace(has_order=lambda _loid: False, watch=MagicMock())
    recovery = APStartupRecovery(
        client_id=_CLIENT,
        broker=object(),
        osm=osm,
        pm=None,
        master_control=SimpleNamespace(mode="LIVE"),
        entry_watcher=watcher,
        execution_core=ec,
    )
    result = {"deferred_lifecycles_recovered": 0}

    recovery._recover_deferred_breach_lifecycles(result)

    osm.retain_broker_submit_owner_for_reconciliation.assert_called_once()
    osm.retain_recovery_ownership_if_no_watcher.assert_not_called()
    osm.update_order_meta.assert_not_called()
    assert not any(
        error == "recovery_retention_write_failed"
        for error in result.get("errors", [])
    )


def test_recovery_malformed_meta_does_not_fall_through_to_resume(monkeypatch):
    row = _intent_row(local_order_id="malformed-meta-1")
    row["meta"] = "{"  # malformed JSON must be held as unknown truth
    import ap.db as ap_db

    monkeypatch.setattr(ap_db, "conn", lambda: _RowsConnection([row]))
    monkeypatch.setattr(ap_db, "run_with_retry", lambda fn, *args, **kwargs: fn())
    osm = MagicMock()
    osm.client_id = _CLIENT
    osm.update_order_meta.return_value = True
    ec = SimpleNamespace(resume_deferred_broker_ready_order=MagicMock())
    recovery = APStartupRecovery(
        client_id=_CLIENT,
        broker=object(),
        osm=osm,
        pm=None,
        master_control=SimpleNamespace(mode="LIVE"),
        entry_watcher=SimpleNamespace(has_order=lambda _loid: False),
        execution_core=ec,
    )
    result = {"deferred_lifecycles_recovered": 0}

    recovery._recover_deferred_breach_lifecycles(result)

    ec.resume_deferred_broker_ready_order.assert_not_called()
    osm.update_order_meta.assert_not_called()
    assert "recovery_submit_meta_unreadable:malformed-meta-1" in result["errors"]


def test_no_match_release_mutation_is_not_exposed():
    assert not hasattr(
        APOrderStateMachine,
        "release_broker_submit_intent_after_no_match",
    )
