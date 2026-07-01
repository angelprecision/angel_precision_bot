from __future__ import annotations

import ast
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
sys.path.insert(0, str(REPO_ROOT))

from ap.order_monitor import APOrderMonitor  # noqa: E402


def _monitor(**kwargs):
    return APOrderMonitor(
        client_id="mon@test.local",
        broker=kwargs.get("broker", MagicMock()),
        order_state_machine=kwargs.get("osm", MagicMock()),
        position_manager=kwargs.get("pm", MagicMock()),
        exit_engine=kwargs.get("exit_engine", MagicMock()),
        entry_watcher=kwargs.get("entry_watcher", MagicMock()),
        client_mode="PAPER",
        data_broker=kwargs.get("data_broker"),
    )


def test_client_runner_passes_resolved_data_broker_to_order_monitor():
    src = (REPO_ROOT / "client_runner.py").read_text()
    assert "data_broker=data_broker" in src


def test_explicit_data_broker_is_stored_and_preferred():
    explicit = MagicMock(name="explicit_data_broker")
    fallback = MagicMock(name="fallback_data_broker")
    broker = MagicMock()
    broker.data_broker = fallback

    mon = _monitor(broker=broker, data_broker=explicit)

    assert mon.data_broker is explicit
    assert mon._quote_broker() is explicit


@pytest.mark.parametrize(
    ("broker_status", "kind", "blocked_status"),
    [
        ("filled", "ENTRY", "FILLED"),
        ("partially_filled", "ENTRY", "PARTIAL_FILL"),
        ("filled", "EXIT", "EXIT_FILLED"),
        ("partially_filled", "EXIT", "EXIT_PARTIAL_FILL"),
    ],
)
def test_status_only_broker_fillish_does_not_terminalize(broker_status, kind, blocked_status):
    osm = MagicMock()
    osm.get_order.return_value = {"kind": kind, "position_id": "pos-1"}
    mon = _monitor(osm=osm)
    mon._emit_order_event = MagicMock()

    mon._advance_from_broker_status("loc-1", broker_status, "AAPL260620C00100000")

    osm.transition.assert_not_called()
    mon._emit_order_event.assert_called_once()
    kwargs = mon._emit_order_event.call_args.kwargs
    assert kwargs["reason_code"] == "BROKER_FILL_SEEN_DEFER_TO_FILL_MONITOR"
    assert kwargs["inputs"]["blocked_status"] == blocked_status
    assert kwargs["inputs"]["requires_fill_monitor"] is True


def test_lost_handoff_rebuild_fails_closed_on_missing_side_and_emits():
    mon = object.__new__(APOrderMonitor)
    mon.client_id = "mon@test.local"
    mon._emit_order_event = MagicMock()
    mon._normalize_order_direction = APOrderMonitor._normalize_order_direction.__get__(mon, APOrderMonitor)
    mon._direction_from_occ_contract = APOrderMonitor._direction_from_occ_contract.__get__(mon, APOrderMonitor)

    plan = mon._build_lost_handoff_plan_from_order(
        {
            "local_order_id": "loc-1",
            "signal_id": "sig-1",
            "plan_id": "plan-1",
            "symbol": "SPY",
            "contract": "",
            "trigger_price": 500.0,
            "meta": {},
        }
    )

    assert plan is None
    mon._emit_order_event.assert_called_once()
    assert (
        mon._emit_order_event.call_args.kwargs["reason_code"]
        == "LOST_HANDOFF_REARM_FAILED_INVALID_OR_MISSING_SIDE"
    )


def test_lost_handoff_rebuild_uses_occ_side_when_order_direction_missing():
    mon = object.__new__(APOrderMonitor)
    mon.client_id = "mon@test.local"
    mon._emit_order_event = MagicMock()
    mon._normalize_order_direction = APOrderMonitor._normalize_order_direction.__get__(mon, APOrderMonitor)
    mon._direction_from_occ_contract = APOrderMonitor._direction_from_occ_contract.__get__(mon, APOrderMonitor)

    plan = mon._build_lost_handoff_plan_from_order(
        {
            "local_order_id": "loc-1",
            "signal_id": "sig-1",
            "plan_id": "plan-1",
            "symbol": "SPY",
            "contract": "SPY260620C00500000",
            "trigger_price": 500.0,
            "meta": {},
        }
    )

    assert plan is not None
    assert plan.direction == "CALL"
    assert plan.side == "CALL"
    mon._emit_order_event.assert_not_called()


def test_get_option_price_blocks_missing_or_sandbox_base_url():
    broker = MagicMock()
    broker.get_quote.return_value = {}
    broker.cfg.base_url = "https://sandbox.tradier.com"
    broker.cfg.access_token = "token"
    broker.session.get = MagicMock()
    mon = _monitor(broker=broker)

    assert mon._get_option_price("SPY260620C00500000") is None
    broker.session.get.assert_not_called()


def test_get_active_entry_orders_hydration_includes_required_fields():
    src = (REPO_ROOT / "ap" / "order_monitor.py").read_text()
    tree = ast.parse(src)
    fn_src = None
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "_get_active_entry_orders":
            fn_src = ast.get_source_segment(src, node)
            break
    assert fn_src
    select_blocks = [
        block for block in fn_src.split("FROM orders")[:-1]
        if "SELECT" in block
    ]
    assert select_blocks, "No SELECT block found in _get_active_entry_orders"
    select = select_blocks[0]
    for field in (
        "qty",
        "direction",
        "execution_mode",
        "reserved_cost",
        "stop_underlying",
        "target_underlying",
    ):
        assert field in select, f"{field} missing from _get_active_entry_orders hydration SELECT"
