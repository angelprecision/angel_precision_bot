from __future__ import annotations

import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

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

