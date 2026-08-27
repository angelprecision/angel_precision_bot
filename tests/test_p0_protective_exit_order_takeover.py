from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
sys.path.insert(0, str(REPO_ROOT))

from ap.exit_safety import resolve_protective_exit_takeover  # noqa: E402
from ap.exit_safety import resolve_exit_broker_truth  # noqa: E402
from ap.brokers.tradier import TradierBroker, TradierConfig  # noqa: E402
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
        if (
            isinstance(self._orders, list)
            and self._orders
            and isinstance(self._orders[0], list)
        ):
            return self._orders.pop(0)
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
        orders=[[_stop()], []],
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
        orders=[[_stop(qty=2)], []],
        terminal={"id": "143387714", "status": "filled", "exec_quantity": 1},
    )
    result = _run(broker, qty=2)
    assert result["allowed"] is True
    assert result["replacement_qty"] == 1
    assert broker.cancel_calls == ["143387714"]


def test_whole_stop_fill_with_stale_position_reread_posts_zero():
    broker = _Broker(
        positions=[[_position(1)], [_position(1)]],
        orders=[[_stop()], []],
        terminal={"id": "143387714", "status": "filled", "exec_quantity": 1},
    )
    result = _run(broker)
    assert result["allowed"] is False
    assert result["replacement_qty"] == 0
    assert result["reason"] == "EXIT_PROTECTIVE_BROKER_FLAT"
    assert broker.cancel_calls == ["143387714"]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("exec_quantity", "nan"),
        ("exec_quantity", True),
        ("exec_quantity", "0.5"),
        ("remaining_quantity", 0),
        ("remaining_quantity", "malformed"),
    ],
)
def test_explicit_malformed_or_contradictory_quantity_holds_before_cancel(field, value):
    order = _stop()
    order[field] = value
    broker = _Broker(positions=[[_position(1)]], orders=[order])
    result = _run(broker)
    assert result["allowed"] is False
    assert result["reason"] == "EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS"
    assert result["audit"]["reason"] == "quantity_unproven"
    assert broker.cancel_calls == []


def test_conflicting_status_aliases_hold_before_cancel():
    order = _stop(status="filled", executed=1)
    order["state"] = "open"
    broker = _Broker(positions=[[_position(1)]], orders=[order])
    result = _run(broker)
    assert result["allowed"] is False
    assert result["reason"] == "EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS"
    assert result["audit"]["reason"] == "conflicting_status"
    assert broker.cancel_calls == []


def test_malformed_terminal_quantity_holds_after_cancel():
    broker = _Broker(
        positions=[[_position(1)]],
        orders=[_stop()],
        terminal={"id": "143387714", "status": "filled", "exec_quantity": "nan"},
    )
    result = _run(broker)
    assert result["allowed"] is False
    assert result["reason"] == "EXIT_PROTECTIVE_CANCEL_OUTCOME_UNPROVEN"
    assert result["audit"]["reason"] == "quantity_unproven"
    assert broker.cancel_calls == ["143387714"]


def test_symbol_alias_exact_occ_is_taken_over():
    order = _stop()
    order.pop("option_symbol")
    order["symbol"] = CONTRACT
    broker = _Broker(
        positions=[[_position(1)], [_position(1)]],
        orders=[[order], []],
        terminal={"id": "143387714", "status": "canceled"},
    )
    result = _run(broker)
    assert result["allowed"] is True
    assert result["replacement_qty"] == 1
    assert broker.cancel_calls == ["143387714"]


def test_missing_broker_account_identity_holds_without_order_inventory():
    broker = _Broker(
        positions=[[_position(1)], [_position(1)]],
        orders=[[_stop()], []],
    )
    broker.account_id = ""
    result = _run(broker)
    assert result["allowed"] is False
    assert result["reason"] == "EXIT_PROTECTIVE_IDENTITY_UNPROVEN"
    assert broker.list_orders_calls == 0
    assert broker.cancel_calls == []


def test_cancel_ack_but_order_still_open_holds():
    broker = _Broker(positions=[[_position(1)]], orders=[_stop()], terminal=_stop())
    result = _run(broker)
    assert result["allowed"] is False
    assert result["reason"] == "EXIT_PROTECTIVE_CANCEL_OUTCOME_UNPROVEN"


def test_cancel_exception_can_only_proceed_when_requery_is_terminal():
    broker = _Broker(
        positions=[[_position(1)], [_position(1)]],
        orders=[[_stop()], []],
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


def test_historical_terminal_sells_without_fill_do_not_block_replacement():
    broker = _Broker(
        positions=[[_position(1)]],
        orders=[_stop(status="canceled"), _stop("expired", status="expired")],
    )
    result = _run(broker)
    assert result["allowed"] is True
    assert result["replacement_qty"] == 1
    assert broker.cancel_calls == []


def test_post_takeover_reinventory_blocks_external_exact_active_sell():
    broker = _Broker(
        positions=[[_position(1)], [_position(1)]],
        orders=[[_stop()], [_stop("external-sell", status="open", qty=1)]],
        terminal={"id": "143387714", "status": "canceled"},
    )
    result = _run(broker)
    assert result["allowed"] is False
    assert result["reason"] == "EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS"
    assert result["audit"]["reason"] == "active_sell_appeared_after_takeover"
    assert broker.cancel_calls == ["143387714"]
    assert broker.list_orders_calls == 2


def test_production_tradier_position_failure_never_becomes_authoritative_flat():
    broker = TradierBroker(
        TradierConfig(
            base_url="https://api.tradier.com",
            access_token="test-token",
            account_id="ACC123",
        )
    )
    broker._get = lambda *args, **kwargs: (_ for _ in ()).throw(TimeoutError("positions timeout"))
    result = resolve_exit_broker_truth(
        broker=broker, client_id=CLIENT, contract=CONTRACT,
    )
    assert result["broker_truth_open_qty"] is None
    assert result["is_fresh_exact"] is False
    assert result["audit"]["snapshot_status"] == "broker_positions_error"


def test_production_tradier_successful_empty_positions_remains_authoritative_flat():
    broker = TradierBroker(
        TradierConfig(
            base_url="https://api.tradier.com",
            access_token="test-token",
            account_id="ACC123",
        )
    )
    broker._get = lambda *args, **kwargs: {"positions": {"position": []}}
    result = resolve_exit_broker_truth(
        broker=broker, client_id=CLIENT, contract=CONTRACT,
    )
    assert result["broker_truth_open_qty"] == 0
    assert result["is_fresh_exact"] is True


def test_production_tradier_malformed_positions_are_unproven():
    broker = TradierBroker(
        TradierConfig(
            base_url="https://api.tradier.com",
            access_token="test-token",
            account_id="ACC123",
        )
    )
    broker._get = lambda *args, **kwargs: {"positions": {"position": "not-a-row"}}
    result = resolve_exit_broker_truth(
        broker=broker, client_id=CLIENT, contract=CONTRACT,
    )
    assert result["broker_truth_open_qty"] is None
    assert result["is_fresh_exact"] is False


@pytest.mark.parametrize("quantity", [-1, 0.5, 1.5, "0.5"])
def test_production_tradier_negative_or_fractional_position_quantity_is_unproven(quantity):
    broker = TradierBroker(
        TradierConfig(
            base_url="https://api.tradier.com",
            access_token="test-token",
            account_id="ACC123",
        )
    )
    broker._get = lambda *args, **kwargs: {
        "positions": {"position": {"symbol": CONTRACT, "quantity": quantity}}
    }
    result = resolve_exit_broker_truth(
        broker=broker, client_id=CLIENT, contract=CONTRACT,
    )
    assert result["broker_truth_open_qty"] is None
    assert result["is_fresh_exact"] is False
    assert result["audit"]["snapshot_status"] == "broker_positions_malformed"
    assert result["audit"]["error"] == "exact_contract_quantity_unproven"


def test_production_tradier_explicit_zero_position_quantity_remains_authoritative_flat():
    broker = TradierBroker(
        TradierConfig(
            base_url="https://api.tradier.com",
            access_token="test-token",
            account_id="ACC123",
        )
    )
    broker._get = lambda *args, **kwargs: {
        "positions": {"position": {"symbol": CONTRACT, "quantity": 0}}
    }
    result = resolve_exit_broker_truth(
        broker=broker, client_id=CLIENT, contract=CONTRACT,
    )
    assert result["broker_truth_open_qty"] == 0
    assert result["is_fresh_exact"] is True
    assert result["audit"]["snapshot_status"] == "exact_match"


@pytest.mark.parametrize(
    "payload",
    [
        {"positions": {"position": {"symbol": CONTRACT}}},
        {"positions": {"position": {"quantity": "1"}}},
        {"positions": {"position": {"symbol": "  ", "quantity": "1"}}},
    ],
)
def test_production_tradier_missing_position_identity_or_quantity_is_unproven(payload):
    broker = TradierBroker(
        TradierConfig(
            base_url="https://api.tradier.com",
            access_token="test-token",
            account_id="ACC123",
        )
    )
    broker._get = lambda *args, **kwargs: payload
    result = resolve_exit_broker_truth(
        broker=broker, client_id=CLIENT, contract=CONTRACT,
    )
    assert result["broker_truth_open_qty"] is None
    assert result["is_fresh_exact"] is False
    assert result["audit"]["snapshot_status"] == "broker_positions_error"


def test_production_tradier_malformed_order_member_holds_takeover_before_cancel_or_post():
    broker = TradierBroker(
        TradierConfig(
            base_url="https://api.tradier.com",
            access_token="test-token",
            account_id="ACC123",
        )
    )
    position_payload = {"positions": {"position": {"symbol": CONTRACT, "quantity": "1"}}}
    orders_payload = {
        "orders": {
            "order": [
                {"id": "unrelated", "status": "open", "option_symbol": "QQQ260828P00122000"},
                "MALFORMED_ORDER_ROW",
            ]
        }
    }

    def _get(path, *args, **kwargs):
        if path.endswith("/positions"):
            return position_payload
        if path.endswith("/orders"):
            return orders_payload
        raise AssertionError(f"unexpected endpoint: {path}")

    broker._get = _get
    cancel_calls = []
    broker.cancel_order = lambda order_id: cancel_calls.append(order_id)
    result = _run(broker)
    assert result["allowed"] is False
    assert result["replacement_qty"] == 0
    assert result["reason"] == "EXIT_PROTECTIVE_ORDERS_UNAVAILABLE"
    assert "TRADIER_ORDERS_PAYLOAD_MALFORMED" in result["audit"]["error"]
    assert cancel_calls == []


@pytest.mark.parametrize(
    ("order", "expected_reason"),
    [
        ({}, "EXIT_PROTECTIVE_ORDERS_MALFORMED"),
        (
            {"id": "malformed", "status": "open", "option_symbol": CONTRACT, "quantity": 1},
            "EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS",
        ),
        (
            {
                "id": "conflicting",
                "status": "open",
                "side": "sell_to_close",
                "type": "stop",
                "option_symbol": "NOW",
                "symbol": CONTRACT,
                "quantity": 1,
            },
            "EXIT_PROTECTIVE_ORDERS_MALFORMED",
        ),
    ],
)
def test_generic_malformed_order_rows_hold_before_cancel(order, expected_reason):
    broker = _Broker(
        positions=[[_position(1)]],
        orders=[order],
    )
    result = _run(broker)
    assert result["allowed"] is False
    assert result["replacement_qty"] == 0
    assert result["reason"] == expected_reason
    assert broker.cancel_calls == []


@pytest.mark.parametrize(
    ("order", "expected_reason"),
    [
        ({}, "EXIT_PROTECTIVE_ORDERS_MALFORMED"),
        (
            {"id": "malformed", "status": "open", "option_symbol": CONTRACT, "quantity": 1},
            "EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS",
        ),
        (
            {
                "id": "conflicting",
                "status": "open",
                "side": "sell_to_close",
                "type": "stop",
                "option_symbol": "NOW",
                "symbol": CONTRACT,
                "quantity": 1,
            },
            "EXIT_PROTECTIVE_ORDERS_MALFORMED",
        ),
    ],
)
def test_production_tradier_malformed_order_rows_hold_before_takeover(order, expected_reason):
    broker = TradierBroker(
        TradierConfig(
            base_url="https://api.tradier.com",
            access_token="test-token",
            account_id="ACC123",
        )
    )
    position_payload = {"positions": {"position": {"symbol": CONTRACT, "quantity": "1"}}}
    orders_payload = {"orders": {"order": [order]}}

    def _get(path, *args, **kwargs):
        if path.endswith("/positions"):
            return position_payload
        if path.endswith("/orders"):
            return orders_payload
        raise AssertionError(f"unexpected endpoint: {path}")

    broker._get = _get
    cancel_calls = []
    broker.cancel_order = lambda order_id: cancel_calls.append(order_id)
    result = _run(broker)
    assert result["allowed"] is False
    assert result["replacement_qty"] == 0
    assert result["reason"] == expected_reason
    assert cancel_calls == []


def test_production_tradier_empty_order_list_is_authoritative_empty():
    broker = TradierBroker(
        TradierConfig(
            base_url="https://api.tradier.com",
            access_token="test-token",
            account_id="ACC123",
        )
    )
    broker._get = lambda *args, **kwargs: {"orders": {"order": []}}
    assert broker.list_orders() == []


@pytest.mark.parametrize(
    "orders_payload",
    [{}, {"orders": {}}, {"orders": None}, {"orders": {"unexpected": []}}],
)
def test_production_tradier_malformed_top_level_orders_hold_before_cancel(orders_payload):
    broker = TradierBroker(
        TradierConfig(
            base_url="https://api.tradier.com",
            access_token="test-token",
            account_id="ACC123",
        )
    )
    position_payload = {"positions": {"position": {"symbol": CONTRACT, "quantity": "1"}}}

    def _get(path, *args, **kwargs):
        if path.endswith("/positions"):
            return position_payload
        if path.endswith("/orders"):
            return orders_payload
        raise AssertionError(f"unexpected endpoint: {path}")

    broker._get = _get
    cancel_calls = []
    broker.cancel_order = lambda order_id: cancel_calls.append(order_id)
    result = _run(broker)
    assert result["allowed"] is False
    assert result["replacement_qty"] == 0
    assert result["reason"] == "EXIT_PROTECTIVE_ORDERS_UNAVAILABLE"
    assert "TRADIER_ORDERS_PAYLOAD_MALFORMED" in result["audit"]["error"]
    assert cancel_calls == []


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
