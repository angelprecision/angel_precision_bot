from __future__ import annotations

import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")


def _make_order(**overrides):
    base = {
        "local_order_id": "local-123",
        "client_id": "client@example.com",
        "signal_id": "sig-123",
        "status": "PENDING_TRIGGER",
        "kind": "ENTRY",
        "symbol": "AAPL",
        "contract": "DEFERRED:AAPL",
        "qty": 2,
        "limit_price": 0.01,
        "reserved_cost": 0.0,
        "broker_order_id": None,
        "submitted_ts": None,
        "created_ts": datetime.now(timezone.utc).isoformat(),
        "execution_mode": "paper",
        "direction": "CALL",
        "score": 88.0,
        "tier": "A",
        "trigger_price": 201.5,
        "meta": {
            "contracts": 2,
            "max_position_usd": 400.0,
            "timeframe": "1d",
            "pattern": "2-1-2",
            "execution_mode": "paper",
        },
    }
    base.update(overrides)
    return base


def _make_monitor(contract_selector=None):
    from ap.order_monitor import APOrderMonitor

    osm = MagicMock()
    osm.record_deferred_hydration_result.return_value = True
    broker = MagicMock()
    broker.data_broker = MagicMock()
    broker.data_broker.get_quote.return_value = {"last": 201.75, "quote_time": "2026-07-02T13:37:00Z"}
    return APOrderMonitor(
        client_id="client@example.com",
        broker=broker,
        order_state_machine=osm,
        position_manager=MagicMock(),
        contract_selector=contract_selector,
        client_mode="PAPER",
    )


def _enable_window(monkeypatch):
    monkeypatch.setattr("ap.contract_quote_revalidator.is_market_open", lambda now=None: True)
    monkeypatch.setattr(
        "ap.order_monitor.APOrderMonitor._is_within_deferred_hydration_window",
        lambda self: True,
    )
    monkeypatch.setattr("ap.order_monitor.DEFERRED_PREBREACH_HYDRATION_ENABLED", True)
    monkeypatch.setattr("ap.order_monitor.DEFERRED_HYDRATION_MAX_PER_CYCLE", 3)


def test_disabled_flag_prevents_hydration_and_selector_call(monkeypatch):
    selector = MagicMock()
    monitor = _make_monitor(contract_selector=selector)
    monkeypatch.setattr("ap.order_monitor.DEFERRED_PREBREACH_HYDRATION_ENABLED", False)

    result = monitor._maybe_hydrate_deferred_order(_make_order())

    assert result == {"attempted": False, "reason": "disabled"}
    selector.select.assert_not_called()
    monitor.osm.record_deferred_hydration_result.assert_not_called()


def test_active_materializer_blocks_prebreach_selector_and_copyback(monkeypatch):
    """The poll-loop hydration consumer must preserve a live owner read-only."""
    selector = MagicMock()
    monitor = _make_monitor(contract_selector=selector)
    _enable_window(monkeypatch)
    active_meta = {
        "watcher_audit": {"reason_code": "trigger_ready"},
        "lifecycle_state": "MATERIALIZING",
        "materialization_status": "RUNNING",
        "materialization_in_flight": True,
        "materialization_owner": "materializer:client@example.com:paper:local-123",
        "materialization_generation": 1,
        "materialization_lease_until": (
            datetime.now(timezone.utc) + timedelta(minutes=5)
        ).isoformat(),
        "broker_ready": False,
        "submit_intent_at": "",
        "broker_submit_key": "",
    }
    result = monitor._maybe_hydrate_deferred_order(_make_order(meta=active_meta))

    assert result == {"attempted": False, "reason": "materialization_in_flight"}
    selector.select.assert_not_called()
    monitor.osm.record_deferred_hydration_result.assert_not_called()


@pytest.mark.parametrize(
    "partial_meta,label",
    [
        ({"materialization_status": "RUNNING"}, "running_only"),
        ({"materialization_status": "QUEUED"}, "queued_only"),
        ({"lifecycle_state": "MATERIALIZING"}, "materializing_only"),
        ({"materialization_in_flight": True}, "in_flight_only"),
        ({"materialization_in_flight": "true"}, "in_flight_string_true"),
        ({"broker_ready": True}, "broker_ready_only"),
        (
            # Owner + generation but no lease at all
            {
                "lifecycle_state": "MATERIALIZING",
                "materialization_status": "RUNNING",
                "materialization_in_flight": True,
                "materialization_owner": "materializer:worker-a",
                "materialization_generation": 1,
            },
            "no_lease",
        ),
        (
            # Owner + generation but expired lease
            {
                "lifecycle_state": "MATERIALIZING",
                "materialization_status": "RUNNING",
                "materialization_in_flight": True,
                "materialization_owner": "materializer:worker-a",
                "materialization_generation": 1,
                "materialization_lease_until": (
                    datetime.now(timezone.utc) - timedelta(minutes=5)
                ).isoformat(),
            },
            "expired_lease",
        ),
        (
            # Full-looking proof but generation=0 (invalid)
            {
                "lifecycle_state": "MATERIALIZING",
                "materialization_status": "RUNNING",
                "materialization_in_flight": True,
                "materialization_owner": "materializer:worker-a",
                "materialization_generation": 0,
                "materialization_lease_until": (
                    datetime.now(timezone.utc) + timedelta(minutes=5)
                ).isoformat(),
            },
            "generation_zero",
        ),
    ],
)
def test_partial_materialization_markers_do_not_block_hydration(monkeypatch, partial_meta, label):
    """
    Amendment negative-control gate: the retained monitor hydration guard must NOT
    protect rows on partial lifecycle markers. Only the canonical full-proof shape
    (owner + generation positive int + future timezone-aware lease + RUNNING +
    MATERIALIZING + in_flight is bool True + no broker handoff) may skip.

    Any of these partial-marker shapes must reach the normal hydration path
    (reason != 'materialization_in_flight'). Otherwise a crashed row leaves a
    permanent HOLD and tradeflow quietly dies.
    """
    selector = MagicMock()
    monitor = _make_monitor(contract_selector=selector)
    _enable_window(monkeypatch)
    meta = {"watcher_audit": {"reason_code": "trigger_ready"}}
    meta.update(partial_meta)

    result = monitor._maybe_hydrate_deferred_order(_make_order(meta=meta))

    # The guard must NOT own the skip decision here. Either hydration
    # proceeds normally, or it fails for a reason UNRELATED to the
    # materialization guard.
    assert result.get("reason") != "materialization_in_flight", (
        f"partial marker shape {label!r} incorrectly triggered the "
        f"materialization guard; got {result!r}"
    )


def _full_active_meta():
    """Full canonical #524 owner-proof shape — MATERIALIZING/RUNNING/in_flight
    is bool True/valid owner/positive-int generation/future tz-aware lease,
    no broker handoff. Every downstream check should treat this as owned.
    """
    return {
        "watcher_audit": {"reason_code": "trigger_ready"},
        "lifecycle_state": "MATERIALIZING",
        "materialization_status": "RUNNING",
        "materialization_in_flight": True,
        "materialization_owner": "materializer:client@example.com:paper:local-123",
        "materialization_generation": 1,
        "materialization_lease_until": (
            datetime.now(timezone.utc) + timedelta(minutes=5)
        ).isoformat(),
        "broker_ready": False,
        "submit_intent_at": "",
        "broker_submit_key": "",
    }


@pytest.mark.parametrize(
    "durable_mode,expected_reason,label",
    [
        (None, "execution_mode_missing_or_invalid", "mode_none"),
        ("", "execution_mode_missing_or_invalid", "mode_blank"),
        ("   ", "execution_mode_missing_or_invalid", "mode_whitespace"),
        ("banana", "execution_mode_missing_or_invalid", "mode_banana"),
        ("LIVE_OR_PAPER", "execution_mode_missing_or_invalid", "mode_junk_string"),
    ],
)
def test_hydration_refuses_to_infer_missing_or_malformed_execution_mode(
    monkeypatch, durable_mode, expected_reason, label
):
    """
    Amendment r2 — retained hydration guard must NOT substitute the runner's
    self.client_mode for a missing/blank/malformed durable execution_mode.

    A row whose durable execution_mode is absent, blank, whitespace, or an
    unknown token like "banana" MUST fail closed BEFORE the materialization
    guard is consulted, with reason=execution_mode_missing_or_invalid.
    Otherwise a malformed row with full-looking active-owner metadata could
    receive the #521 no-hydration fence despite failing every other #521
    consumer's identity gate (classifier + PTR + ap_recovery all reject
    missing/malformed durable mode). Inconsistency is not tolerable in a
    consumer that could reselect and overwrite a live materializer attempt.
    """
    selector = MagicMock()
    monitor = _make_monitor(contract_selector=selector)
    _enable_window(monkeypatch)

    order = _make_order(meta=_full_active_meta())
    order["execution_mode"] = durable_mode  # deliberately malformed

    result = monitor._maybe_hydrate_deferred_order(order)

    assert result == {"attempted": False, "reason": expected_reason}, (
        f"malformed durable mode {label!r} was inferred from runner context; "
        f"got {result!r}"
    )
    # And crucially, the materialization-in-flight fence must NOT be the
    # reason — the row is malformed and belongs to the durable-identity
    # authority, not the materialization-owner authority.
    assert result.get("reason") != "materialization_in_flight"
    selector.select.assert_not_called()
    monitor.osm.record_deferred_hydration_result.assert_not_called()


def test_hydration_execution_mode_mismatch_still_wins_over_materialization_guard(monkeypatch):
    """
    Amendment r2 (companion) — a durable mode present but different from
    the runner (e.g. PAPER row on a LIVE runner) must fail closed with
    reason=execution_mode_mismatch, NOT materialization_in_flight. Even if
    the meta shape passes every #524 canonical proof gate, cross-mode
    protection would be wrong: the LIVE runner has no business touching a
    PAPER row's materializer, and vice versa.
    """
    selector = MagicMock()
    monitor = _make_monitor(contract_selector=selector)  # runner is PAPER
    monitor.client_mode = "live"  # flip runner to LIVE
    _enable_window(monkeypatch)

    order = _make_order(meta=_full_active_meta())
    order["execution_mode"] = "paper"  # durable is PAPER, runner is LIVE

    result = monitor._maybe_hydrate_deferred_order(order)

    assert result == {"attempted": False, "reason": "execution_mode_mismatch"}
    assert result.get("reason") != "materialization_in_flight"
    selector.select.assert_not_called()
    monitor.osm.record_deferred_hydration_result.assert_not_called()


def test_pending_trigger_deferred_row_hydrates_to_occ_contract_and_limit_gt_point_01(monkeypatch):
    selector = MagicMock()

    def _select(plan):
        plan.contract_symbol = "AAPL260717C00200000"
        plan.limit_price = 1.23
        plan.contracts = 2
        return SimpleNamespace(
            contract_symbol="AAPL260717C00200000",
            execution_price_per_share=1.23,
            affordable_contracts=2,
        )

    selector.select.side_effect = _select
    selector.get_last_failure.return_value = None

    monitor = _make_monitor(contract_selector=selector)
    _enable_window(monkeypatch)

    result = monitor._maybe_hydrate_deferred_order(_make_order())

    assert result["attempted"] is True
    assert result["success"] is True
    assert result["contract"] == "AAPL260717C00200000"
    assert result["limit_price"] > 0.01
    monitor.osm.record_deferred_hydration_result.assert_called_once()
    _, kwargs = monitor.osm.record_deferred_hydration_result.call_args
    assert kwargs["success"] is True
    assert kwargs["contract_selection_status"] == "HYDRATED_PRE_BREACH"
    assert kwargs["qty"] == 2
    assert monitor.broker.place_order.call_count == 0
    assert monitor.osm.submit_existing_entry.call_count == 0


def test_reserved_cost_updates_to_qty_times_limit_times_100(monkeypatch):
    selector = MagicMock()

    def _select(plan):
        plan.contract_symbol = "AAPL260717C00200000"
        plan.limit_price = 1.37
        plan.contracts = 3
        return SimpleNamespace(
            contract_symbol="AAPL260717C00200000",
            execution_price_per_share=1.37,
            affordable_contracts=3,
        )

    selector.select.side_effect = _select
    selector.get_last_failure.return_value = None

    monitor = _make_monitor(contract_selector=selector)
    _enable_window(monkeypatch)

    monitor._maybe_hydrate_deferred_order(_make_order(qty=1))

    _, kwargs = monitor.osm.record_deferred_hydration_result.call_args
    assert kwargs["reserved_cost"] == 411.0


def test_status_remains_pending_trigger_after_successful_hydration(monkeypatch):
    selector = MagicMock()
    selector.select.side_effect = lambda plan: SimpleNamespace(
        contract_symbol="MSFT260717P00400000",
        execution_price_per_share=2.05,
        affordable_contracts=1,
    )
    selector.get_last_failure.return_value = None

    monitor = _make_monitor(contract_selector=selector)
    _enable_window(monkeypatch)
    order = _make_order(symbol="MSFT", contract="DEFERRED:MSFT", qty=1, status="PENDING_TRIGGER")

    monitor._maybe_hydrate_deferred_order(order)

    assert order["status"] == "PENDING_TRIGGER"
    _, kwargs = monitor.osm.record_deferred_hydration_result.call_args
    assert kwargs["status"] == "PENDING_TRIGGER"


def test_local_order_id_client_id_execution_mode_signal_id_preserved(monkeypatch):
    selector = MagicMock()

    def _select(plan):
        assert plan.client_id == "client@example.com"
        assert plan.signal_id == "sig-keep"
        assert plan.execution_mode == "live"
        plan.contract_symbol = "MSFT260717P00400000"
        plan.limit_price = 2.05
        plan.contracts = 1
        return SimpleNamespace(
            contract_symbol="MSFT260717P00400000",
            execution_price_per_share=2.05,
            affordable_contracts=1,
        )

    selector.select.side_effect = _select
    selector.get_last_failure.return_value = None

    monitor = _make_monitor(contract_selector=selector)
    _enable_window(monkeypatch)
    order = _make_order(
        local_order_id="local-keep",
        signal_id="sig-keep",
        symbol="MSFT",
        contract="DEFERRED:MSFT",
        execution_mode="live",
        meta={"execution_mode": "live", "max_position_usd": 250.0, "contracts": 1},
    )

    snapshot = {
        "local_order_id": order["local_order_id"],
        "client_id": order["client_id"],
        "execution_mode": order["execution_mode"],
        "signal_id": order["signal_id"],
    }
    monitor._maybe_hydrate_deferred_order(order)

    assert {
        "local_order_id": order["local_order_id"],
        "client_id": order["client_id"],
        "execution_mode": order["execution_mode"],
        "signal_id": order["signal_id"],
    } == snapshot


def test_broker_submit_is_never_called_and_broker_order_id_stays_null(monkeypatch):
    selector = MagicMock()
    selector.select.side_effect = lambda plan: SimpleNamespace(
        contract_symbol="AAPL260717C00200000",
        execution_price_per_share=1.23,
        affordable_contracts=2,
    )
    selector.get_last_failure.return_value = None

    monitor = _make_monitor(contract_selector=selector)
    _enable_window(monkeypatch)
    order = _make_order(broker_order_id=None)

    monitor._maybe_hydrate_deferred_order(order)

    assert order["broker_order_id"] is None
    assert monitor.broker.place_order.call_count == 0


def test_hydration_failure_keeps_row_pending_trigger_and_writes_meta_failure(monkeypatch):
    selector = MagicMock()
    selector.select.return_value = None
    selector.get_last_failure.return_value = {
        "reason_code": "NO_CHAIN_DATA",
        "stage": "chain_fetch",
        "explanation": "quotes not warm yet",
    }

    monitor = _make_monitor(contract_selector=selector)
    _enable_window(monkeypatch)
    order = _make_order()

    result = monitor._maybe_hydrate_deferred_order(order)

    assert result == {"attempted": True, "success": False, "reason": "NO_CHAIN_DATA"}
    assert order["status"] == "PENDING_TRIGGER"
    _, kwargs = monitor.osm.record_deferred_hydration_result.call_args
    assert kwargs["success"] is False
    assert kwargs["contract_selection_status"] == "HYDRATION_DATA_PENDING"
    assert kwargs["selector_audit"]["reason_code"] == "NO_CHAIN_DATA"


def test_hydration_ignores_rows_already_submitted_or_terminal(monkeypatch):
    monitor = _make_monitor(contract_selector=MagicMock())
    _enable_window(monkeypatch)

    assert monitor._maybe_hydrate_deferred_order(_make_order(submitted_ts="2026-07-02T13:37:00Z"))["attempted"] is False
    assert monitor._maybe_hydrate_deferred_order(_make_order(broker_order_id="broker-1"))["attempted"] is False
    assert monitor._maybe_hydrate_deferred_order(_make_order(status="CANCELED"))["attempted"] is False
    monitor.osm.record_deferred_hydration_result.assert_not_called()


def test_created_deferred_rows_are_ignored(monkeypatch):
    selector = MagicMock()
    monitor = _make_monitor(contract_selector=selector)
    _enable_window(monkeypatch)

    result = monitor._maybe_hydrate_deferred_order(_make_order(status="CREATED"))

    assert result == {"attempted": False, "reason": "status_not_eligible"}
    selector.select.assert_not_called()


def test_hydration_is_idempotent_second_run_is_duplicate_suppressed(monkeypatch):
    selector = MagicMock()
    selector.select.side_effect = lambda plan: SimpleNamespace(
        contract_symbol="AAPL260717C00200000",
        execution_price_per_share=1.23,
        affordable_contracts=2,
    )
    selector.get_last_failure.return_value = None

    monitor = _make_monitor(contract_selector=selector)
    _enable_window(monkeypatch)

    seen = set()
    first = monitor._maybe_hydrate_deferred_order(_make_order(), hydration_seen=seen)
    second = monitor._maybe_hydrate_deferred_order(_make_order(), hydration_seen=seen)

    assert first["success"] is True
    assert second == {"attempted": False, "reason": "duplicate_suppressed"}
    assert monitor.osm.record_deferred_hydration_result.call_count == 1


def test_live_paper_taxonomy_preserved_by_execution_mode_scope(monkeypatch):
    monitor = _make_monitor(contract_selector=MagicMock())
    _enable_window(monkeypatch)

    result = monitor._maybe_hydrate_deferred_order(
        _make_order(execution_mode="live", meta={"execution_mode": "live"})
    )

    assert result == {"attempted": False, "reason": "execution_mode_mismatch"}
    monitor.osm.record_deferred_hydration_result.assert_not_called()


def test_cap_stops_hydration_after_max_per_cycle(monkeypatch):
    selector = MagicMock()

    def _select(plan):
        plan.contract_symbol = "AAPL260717C00200000"
        plan.limit_price = 1.23
        plan.contracts = 1
        return SimpleNamespace(
            contract_symbol="AAPL260717C00200000",
            execution_price_per_share=1.23,
            affordable_contracts=1,
        )

    selector.select.side_effect = _select
    selector.get_last_failure.return_value = None

    monitor = _make_monitor(contract_selector=selector)
    _enable_window(monkeypatch)
    monkeypatch.setattr("ap.order_monitor.DEFERRED_HYDRATION_MAX_PER_CYCLE", 2)
    monkeypatch.setattr(
        monitor,
        "_get_active_entry_orders",
        lambda: [
            _make_order(local_order_id="local-1"),
            _make_order(local_order_id="local-2"),
            _make_order(local_order_id="local-3"),
        ],
    )
    monkeypatch.setattr(
        monitor,
        "_check_pending_trigger_order",
        lambda **kwargs: None,
    )

    monitor._check_entry_orders()

    assert selector.select.call_count == 2
    assert monitor.osm.record_deferred_hydration_result.call_count == 2


def test_successful_hydration_does_not_pass_stale_deferred_order_downstream(monkeypatch):
    monitor = _make_monitor(contract_selector=MagicMock())
    _enable_window(monkeypatch)
    monkeypatch.setattr(
        monitor,
        "_get_active_entry_orders",
        lambda: [_make_order(local_order_id="local-refresh-1", contract="DEFERRED:AAPL")],
    )
    monkeypatch.setattr(
        monitor,
        "_maybe_hydrate_deferred_order",
        lambda order, hydration_seen=None: {
            "attempted": True,
            "success": True,
            "contract": "AAPL260717C00200000",
        },
    )
    check_calls = []
    monkeypatch.setattr(
        monitor,
        "_check_pending_trigger_order",
        lambda **kwargs: check_calls.append(kwargs),
    )

    monitor._check_entry_orders()

    assert check_calls == []


def test_hydration_failure_keeps_existing_pending_trigger_downstream_behavior(monkeypatch):
    monitor = _make_monitor(contract_selector=MagicMock())
    _enable_window(monkeypatch)
    order = _make_order(local_order_id="local-refresh-2", contract="DEFERRED:AAPL")
    monkeypatch.setattr(
        monitor,
        "_get_active_entry_orders",
        lambda: [order],
    )
    monkeypatch.setattr(
        monitor,
        "_maybe_hydrate_deferred_order",
        lambda current_order, hydration_seen=None: {
            "attempted": True,
            "success": False,
            "reason": "NO_CHAIN_DATA",
        },
    )
    check_calls = []
    monkeypatch.setattr(
        monitor,
        "_check_pending_trigger_order",
        lambda **kwargs: check_calls.append(kwargs),
    )

    monitor._check_entry_orders()

    assert len(check_calls) == 1
    assert check_calls[0]["order"]["contract"] == "DEFERRED:AAPL"
