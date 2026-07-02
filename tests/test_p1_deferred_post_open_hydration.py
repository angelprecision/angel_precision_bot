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
        "signal_id": "sig-123",
        "status": "PENDING_TRIGGER",
        "kind": "ENTRY",
        "symbol": "AAPL",
        "contract": "DEFERRED:AAPL",
        "qty": 2,
        "limit_price": None,
        "reserved_cost": 0.0,
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
    return APOrderMonitor(
        client_id="client@example.com",
        broker=broker,
        order_state_machine=osm,
        position_manager=MagicMock(),
        contract_selector=contract_selector,
        client_mode="PAPER",
    )


def test_hydrates_real_contract_without_broker_submit(monkeypatch):
    selector = MagicMock()

    def _select(plan):
        plan.contract_symbol = "AAPL260717C00200000"
        plan.limit_price = 1.23
        return SimpleNamespace(
            contract_symbol="AAPL260717C00200000",
            execution_price_per_share=1.23,
        )

    selector.select.side_effect = _select
    selector.get_last_failure.return_value = None

    monitor = _make_monitor(contract_selector=selector)
    monkeypatch.setattr("ap.contract_quote_revalidator.is_market_open", lambda now=None: True)

    result = monitor._maybe_hydrate_deferred_pending_trigger(_make_order())

    assert result["attempted"] is True
    assert result["success"] is True
    monitor.osm.record_deferred_hydration_result.assert_called_once_with(
        "local-123",
        success=True,
        contract="AAPL260717C00200000",
        limit_price=1.23,
        reserved_cost=246.0,
    )
    assert monitor.broker.place_order.call_count == 0
    assert monitor.osm.submit_existing_entry.call_count == 0


def test_failed_hydration_preserves_pending_trigger_and_records_reason(monkeypatch):
    selector = MagicMock()
    selector.select.return_value = None
    selector.get_last_failure.return_value = {
        "reason_code": "NO_CHAIN_DATA",
        "explanation": "chain unavailable",
    }

    monitor = _make_monitor(contract_selector=selector)
    monkeypatch.setattr("ap.contract_quote_revalidator.is_market_open", lambda now=None: True)

    order = _make_order()
    result = monitor._maybe_hydrate_deferred_pending_trigger(order)

    assert result == {"attempted": True, "success": False, "reason": "NO_CHAIN_DATA"}
    assert order["status"] == "PENDING_TRIGGER"
    monitor.osm.record_deferred_hydration_result.assert_called_once_with(
        "local-123",
        success=False,
        reason="NO_CHAIN_DATA",
    )
    assert monitor.osm.transition.call_count == 0


def test_identity_fields_remain_unchanged_and_no_submit_side_effects(monkeypatch):
    selector = MagicMock()

    def _select(plan):
        assert plan.client_id == "client@example.com"
        assert plan.signal_id == "sig-keep"
        assert plan.execution_mode == "live"
        plan.contract_symbol = "MSFT260717P00400000"
        plan.limit_price = 2.05
        return SimpleNamespace(
            contract_symbol="MSFT260717P00400000",
            execution_price_per_share=2.05,
        )

    selector.select.side_effect = _select
    selector.get_last_failure.return_value = None

    monitor = _make_monitor(contract_selector=selector)
    monkeypatch.setattr("ap.contract_quote_revalidator.is_market_open", lambda now=None: True)

    order = _make_order(
        local_order_id="local-keep",
        signal_id="sig-keep",
        symbol="MSFT",
        contract="DEFERRED:MSFT",
        execution_mode="live",
        qty=1,
        meta={"execution_mode": "live", "max_position_usd": 250.0, "contracts": 1},
    )
    snapshot = {
        "local_order_id": order["local_order_id"],
        "signal_id": order["signal_id"],
        "execution_mode": order["execution_mode"],
    }

    result = monitor._maybe_hydrate_deferred_pending_trigger(order)

    assert result["success"] is True
    assert {
        "local_order_id": order["local_order_id"],
        "signal_id": order["signal_id"],
        "execution_mode": order["execution_mode"],
    } == snapshot
    assert monitor.osm.submit_existing_entry.call_count == 0
    assert monitor.osm.transition.call_count == 0


def test_selector_thresholds_unchanged(monkeypatch):
    selector = MagicMock()
    selector.max_spread_pct = 0.25
    selector.min_oi = 100
    selector.max_premium = 350.0

    def _select(plan):
        plan.contract_symbol = "QQQ260717C00500000"
        plan.limit_price = 1.11
        return SimpleNamespace(
            contract_symbol="QQQ260717C00500000",
            execution_price_per_share=1.11,
        )

    selector.select.side_effect = _select
    selector.get_last_failure.return_value = None

    monitor = _make_monitor(contract_selector=selector)
    monkeypatch.setattr("ap.contract_quote_revalidator.is_market_open", lambda now=None: True)

    before = (
        selector.max_spread_pct,
        selector.min_oi,
        selector.max_premium,
    )
    result = monitor._maybe_hydrate_deferred_pending_trigger(
        _make_order(symbol="QQQ", contract="DEFERRED:QQQ", qty=1)
    )

    assert result["success"] is True
    assert (
        selector.max_spread_pct,
        selector.min_oi,
        selector.max_premium,
    ) == before
