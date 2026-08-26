from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
sys.path.insert(0, str(REPO_ROOT))

from ap.exit_safety import resolve_protective_exit_takeover  # noqa: E402
from ap import fill_monitor as fill_monitor_mod  # noqa: E402


CLIENT = "jasoncosby1@gmail.com"
CONTRACT = "NOW260828P00122000"


def _position(qty, contract=CONTRACT, account="ACC123"):
    return {"symbol": contract, "quantity": qty, "side": "PUT", "account_id": account}


def _stop(order_id="143387714", *, contract=CONTRACT, account="ACC123", status="open", qty=1, executed=0):
    return {
        "id": order_id,
        "status": status,
        "class": "option",
        "type": "stop",
        "side": "sell_to_close",
        "option_symbol": contract,
        "quantity": qty,
        "exec_quantity": executed,
        "duration": "gtc",
        "account_id": account,
    }


class _Broker:
    account_id = "ACC123"

    def __init__(self, *, positions, orders, terminal=None, cancel_error=None):
        self._positions = list(positions)
        self._orders = orders
        self._terminal = terminal or {"id": "143387714", "status": "canceled"}
        self._cancel_error = cancel_error
        self.cancel_calls = []
        self.get_calls = []
        self.list_orders_calls = 0

    def list_positions(self):
        current = self._positions.pop(0) if len(self._positions) > 1 else self._positions[0]
        if isinstance(current, Exception):
            raise current
        return current

    def list_orders(self):
        self.list_orders_calls += 1
        if isinstance(self._orders, Exception):
            raise self._orders
        return self._orders

    def cancel_order(self, order_id):
        self.cancel_calls.append(order_id)
        if self._cancel_error:
            raise self._cancel_error
        return {"ok": True, "status": "canceled", "broker_order_id": order_id}

    def get_order(self, order_id):
        self.get_calls.append(order_id)
        if isinstance(self._terminal, Exception):
            raise self._terminal
        return self._terminal


def _run(broker, *, qty=1, mode="live", contract=CONTRACT):
    return resolve_protective_exit_takeover(
        broker=broker,
        client_id=CLIENT,
        execution_mode=mode,
        position_id="position-now-live-1",
        local_order_id="exit-now-live-1",
        contract=contract,
        requested_qty=qty,
    )


def test_exact_now_positive_control_cancels_once_then_allows_one_contract():
    broker = _Broker(
        positions=[[_position(1)], [_position(1)]],
        orders=[_stop()],
        terminal={"id": "143387714", "status": "canceled", "exec_quantity": 0},
    )
    result = _run(broker)
    assert result["allowed"] is True
    assert result["replacement_qty"] == 1
    assert broker.cancel_calls == ["143387714"]
    assert broker.get_calls == ["143387714"]


def test_stop_filled_before_takeover_posts_zero():
    broker = _Broker(positions=[[]], orders=[_stop(status="filled")])
    result = _run(broker)
    assert result["allowed"] is False
    assert result["replacement_qty"] == 0
    assert broker.cancel_calls == []


def test_stop_fills_during_cancel_allows_only_proven_residual():
    broker = _Broker(
        positions=[[_position(2)], [_position(1)]],
        orders=[_stop(qty=2)],
        terminal={"id": "143387714", "status": "filled", "exec_quantity": 1},
    )
    result = _run(broker, qty=2)
    assert result["allowed"] is True
    assert result["replacement_qty"] == 1
    assert broker.cancel_calls == ["143387714"]


def test_cancel_ack_but_order_still_open_holds():
    broker = _Broker(positions=[[_position(1)]], orders=[_stop()], terminal=_stop())
    result = _run(broker)
    assert result["allowed"] is False
    assert result["reason"] == "EXIT_PROTECTIVE_CANCEL_OUTCOME_UNPROVEN"


def test_cancel_exception_can_only_proceed_when_requery_is_terminal():
    broker = _Broker(
        positions=[[_position(1)], [_position(1)]],
        orders=[_stop()],
        terminal={"id": "143387714", "status": "canceled"},
        cancel_error=RuntimeError("transport lost"),
    )
    assert _run(broker)["allowed"] is True


@pytest.mark.parametrize("terminal", [{"status": "UNKNOWN"}, {"garbage": True}, RuntimeError("down")])
def test_unknown_or_malformed_get_order_holds(terminal):
    broker = _Broker(positions=[[_position(1)]], orders=[_stop()], terminal=terminal)
    result = _run(broker)
    assert result["allowed"] is False
    assert result["replacement_qty"] == 0


def test_list_orders_unavailable_holds():
    broker = _Broker(positions=[[_position(1)]], orders=RuntimeError("orders unavailable"))
    result = _run(broker)
    assert result["allowed"] is False
    assert result["reason"] == "EXIT_PROTECTIVE_ORDERS_UNAVAILABLE"


def test_multiple_exact_active_sells_hold_without_cancel():
    broker = _Broker(positions=[[_position(1)]], orders=[_stop("1"), _stop("2")])
    result = _run(broker)
    assert result["allowed"] is False
    assert result["reason"] == "EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS"
    assert broker.cancel_calls == []


def test_existing_canonical_exit_is_held_not_canceled():
    order = _stop()
    order.update(type="limit", tag="exit-now-live-1")
    broker = _Broker(positions=[[_position(1)]], orders=[order])
    result = _run(broker)
    assert result["reason"] == "EXIT_CANONICAL_BROKER_SELL_ACTIVE"
    assert broker.cancel_calls == []


def test_same_ticker_different_occ_and_wrong_account_are_never_touched():
    other_occ = "NOW260828P00123000"
    broker = _Broker(
        positions=[[_position(1)]],
        orders=[_stop(contract=other_occ), _stop("wrong-account", account="OTHER")],
    )
    result = _run(broker)
    assert result["allowed"] is True
    assert broker.cancel_calls == []


@pytest.mark.parametrize("mode", ["paper", "", "unknown"])
def test_non_live_or_missing_mode_fails_closed_without_broker_mutation(mode):
    broker = _Broker(positions=[[_position(1)]], orders=[_stop()])
    result = _run(broker, mode=mode)
    assert result["allowed"] is False
    assert broker.list_orders_calls == 0
    assert broker.cancel_calls == []


def test_unknown_active_status_holds_without_cancel():
    broker = _Broker(positions=[[_position(1)]], orders=[_stop(status="mystery")])
    result = _run(broker)
    assert result["reason"] == "EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS"
    assert broker.cancel_calls == []


@pytest.mark.parametrize(
    ("stop_response", "expected_state", "expected_id"),
    [
        ({"id": "143387714", "status": "open"}, "ACTIVE", "143387714"),
        ({"status": "ok"}, "OUTCOME_UNPROVEN", None),
    ],
)
def test_standing_stop_identity_is_durable_only_with_concrete_broker_id(
    monkeypatch, stop_response, expected_state, expected_id,
):
    writes = []

    class _Cursor:
        rowcount = 1

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params):
            writes.append((sql, params))
            return _Cursor()

    class _StopBroker:
        def place_stop_order(self, **kwargs):
            return dict(stop_response)

    monkeypatch.setattr(fill_monitor_mod, "conn", lambda: _Conn())
    monkeypatch.setattr(fill_monitor_mod, "run_with_retry", lambda fn, *a, **k: fn())
    monkeypatch.setattr(fill_monitor_mod, "audit", lambda *a, **k: None)
    fill_monitor_mod._place_standing_stop_best_effort(
        broker=_StopBroker(),
        order={
            "local_order_id": "entry-now-1",
            "client_id": CLIENT,
            "execution_mode": "live",
            "contract": CONTRACT,
            "symbol": "NOW",
        },
        qty=1,
        entry_price=1.54,
    )

    assert len(writes) == 1
    payload = __import__("json").loads(writes[0][1][0])["protective_order"]
    assert payload["protective_order_state"] == expected_state
    assert payload["protective_broker_order_id"] == expected_id
    assert payload["client_id"] == CLIENT
    assert payload["execution_mode"] == "live"
    assert payload["protective_contract"] == CONTRACT
