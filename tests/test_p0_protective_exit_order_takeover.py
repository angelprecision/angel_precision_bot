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
from ap.order_state_machine import _durable_protective_order_id_for_position  # noqa: E402


CLIENT = "jasoncosby1@gmail.com"
CONTRACT = "NOW260828P00122000"
CURRENT_ENTRY_TS = "2026-08-28T15:00:00+00:00"
HISTORICAL_FILL_TS = "2026-08-28T14:59:00+00:00"
CURRENT_FILL_TS = "2026-08-28T15:01:00+00:00"


def _position(qty, contract=CONTRACT, account="ACC123"):
    return {"symbol": contract, "quantity": qty, "side": "PUT", "account_id": account}


def _stop(
    order_id="143387714", *, contract=CONTRACT, account="ACC123", status="open",
    qty=1, executed=0, fill_ts=None,
):
    order = {
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
    if fill_ts is not None:
        order["last_fill_date"] = fill_ts
    return order


def _terminal(
    order_id="143387714", *, status="canceled", qty=1, executed=0,
    fill_ts=None, **updates,
):
    order = _stop(
        order_id, status=status, qty=qty, executed=executed, fill_ts=fill_ts,
    )
    order["symbol"] = "NOW"
    order.update(updates)
    return order


class _Broker:
    account_id = "ACC123"

    def __init__(self, *, positions, orders, terminal=None, cancel_error=None):
        self._positions = list(positions)
        self._orders = orders
        self._terminal = terminal if terminal is not None else _terminal()
        self._cancel_error = cancel_error
        self.cancel_calls = []
        self.get_calls = []
        self.list_orders_calls = 0
        self.list_orders_strict_calls = 0

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

    def list_orders_strict(self):
        self.list_orders_strict_calls += 1
        return self.list_orders()

    def cancel_order(self, order_id):
        self.cancel_calls.append(order_id)
        if self._cancel_error:
            raise self._cancel_error
        return {"ok": True, "status": "canceled", "broker_order_id": order_id}

    def get_order(self, order_id):
        self.get_calls.append(order_id)
        if isinstance(self._terminal, Exception):
            raise self._terminal
        if isinstance(self._terminal, list):
            return self._terminal.pop(0) if len(self._terminal) > 1 else self._terminal[0]
        return self._terminal


def _run(
    broker, *, qty=1, mode="live", contract=CONTRACT,
    protective_broker_order_id=None, entry_ts=None,
):
    return resolve_protective_exit_takeover(
        broker=broker,
        client_id=CLIENT,
        execution_mode=mode,
        position_id="position-now-live-1",
        local_order_id="exit-now-live-1",
        contract=contract,
        requested_qty=qty,
        protective_broker_order_id=protective_broker_order_id,
        current_position_entry_ts=entry_ts,
    )


def test_exact_now_positive_control_cancels_once_then_allows_one_contract():
    broker = _Broker(
        positions=[[_position(1)], [_position(1)]],
        orders=[[_stop()], []],
        terminal=_terminal(executed=0),
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
        terminal=_terminal(status="filled", qty=2, executed=1),
    )
    result = _run(broker, qty=2)
    assert result["allowed"] is True
    assert result["replacement_qty"] == 1
    assert broker.cancel_calls == ["143387714"]


def test_partial_protective_fill_before_cancel_requires_total_position_coherence():
    broker = _Broker(
        positions=[[_position(2)], [_position(2)]],
        orders=[[_stop(qty=2, executed=1)], []],
        terminal=_terminal(status="canceled", qty=2, executed=1),
    )
    result = _run(broker, qty=2)
    assert result["allowed"] is False
    assert result["replacement_qty"] == 0
    assert result["reason"] == "EXIT_PROTECTIVE_POSITION_UNPROVEN"
    assert result["audit"]["reason"] == "position_snapshot_stale_after_order_fill"
    assert result["audit"]["observed_protective_execution_total"] == 1
    assert broker.cancel_calls == ["143387714"]


def test_whole_stop_fill_with_stale_position_reread_posts_zero():
    broker = _Broker(
        positions=[[_position(1)], [_position(1)]],
        orders=[[_stop()], []],
        terminal=_terminal(status="filled", executed=1),
    )
    result = _run(broker)
    assert result["allowed"] is False
    assert result["replacement_qty"] == 0
    assert result["reason"] == "EXIT_PROTECTIVE_POSITION_UNPROVEN"
    assert result["audit"]["reason"] == "position_snapshot_stale_after_order_fill"
    assert broker.cancel_calls == ["143387714"]


def test_protective_fill_in_post_inventory_requires_final_position_coherence():
    broker = _Broker(
        positions=[[_position(2)], [_position(2)]],
        orders=[
            [_stop(qty=2)],
            [_stop(status="filled", qty=2, executed=1)],
        ],
        terminal=_terminal(executed=0, qty=2),
    )
    result = _run(broker, qty=2)
    assert result["allowed"] is False
    assert result["replacement_qty"] == 0
    assert result["reason"] == "EXIT_PROTECTIVE_POSITION_UNPROVEN"
    assert result["audit"]["reason"] == "position_snapshot_stale_after_order_fill"
    assert result["audit"]["observed_protective_execution_delta"] == 1
    assert broker.cancel_calls == ["143387714"]


def test_protective_fill_in_post_inventory_sizes_from_final_position_once():
    broker = _Broker(
        positions=[[_position(2)], [_position(1)]],
        orders=[
            [_stop(qty=2)],
            [_stop(status="filled", qty=2, executed=1)],
        ],
        terminal=_terminal(executed=0, qty=2),
    )
    result = _run(broker, qty=2)
    assert result["allowed"] is True
    assert result["replacement_qty"] == 1
    assert result["audit"]["observed_protective_execution_delta"] == 1
    assert broker.cancel_calls == ["143387714"]


def test_cancel_terminal_proof_requires_exact_broker_order_identity():
    terminal = _terminal()
    terminal["id"] = "different-order"
    broker = _Broker(
        positions=[[_position(1)]],
        orders=[[_stop()], []],
        terminal=terminal,
    )
    result = _run(broker)
    assert result["allowed"] is False
    assert result["replacement_qty"] == 0
    assert result["reason"] == "EXIT_PROTECTIVE_CANCEL_OUTCOME_UNPROVEN"
    assert result["audit"]["reason"] == "terminal_order_id_mismatch"
    assert broker.cancel_calls == ["143387714"]


@pytest.mark.parametrize(
    ("field", "value", "expected_reason"),
    [
        ("id", None, "terminal_order_id_unproven"),
        ("option_symbol", "NOW260828P00123000", "terminal_order_contract_mismatch"),
        ("side", "buy_to_open", "terminal_order_side_mismatch"),
        ("account_id", "OTHER", "terminal_order_account_mismatch"),
        ("type", "limit", "terminal_order_type_mismatch"),
    ],
)
def test_cancel_terminal_proof_rejects_identity_mismatch(field, value, expected_reason):
    terminal = _terminal()
    if value is None:
        terminal.pop(field)
    else:
        terminal[field] = value
    broker = _Broker(
        positions=[[_position(1)]],
        orders=[[_stop()], []],
        terminal=terminal,
    )
    result = _run(broker)
    assert result["allowed"] is False
    assert result["replacement_qty"] == 0
    assert result["reason"] == "EXIT_PROTECTIVE_CANCEL_OUTCOME_UNPROVEN"
    assert result["audit"]["reason"] == expected_reason
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


@pytest.mark.parametrize(
    ("field", "value", "expected_reason"),
    [
        ("order_id", "different-order", "order_id_conflict"),
        ("qty", 2, "quantity_conflict"),
        ("order_type", "market", "order_type_conflict"),
        ("class", "equity", "order_class_mismatch"),
        ("order_class", "equity", "order_class_conflict"),
    ],
)
def test_exact_order_shape_conflicts_hold_before_cancel(field, value, expected_reason):
    order = _stop()
    order[field] = value
    broker = _Broker(positions=[[_position(1)]], orders=[order])
    result = _run(broker)
    assert result["allowed"] is False
    assert result["replacement_qty"] == 0
    assert result["reason"] == "EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS"
    assert result["audit"]["reason"] == expected_reason
    assert broker.cancel_calls == []


def test_malformed_terminal_quantity_holds_after_cancel():
    broker = _Broker(
        positions=[[_position(1)]],
        orders=[_stop()],
        terminal=_terminal(status="filled", executed="nan"),
    )
    result = _run(broker)
    assert result["allowed"] is False
    assert result["reason"] == "EXIT_PROTECTIVE_CANCEL_OUTCOME_UNPROVEN"
    assert result["audit"]["reason"] == "quantity_unproven"
    assert broker.cancel_calls == ["143387714"]


def test_terminal_cancel_without_fill_or_remaining_evidence_holds_after_cancel():
    terminal = _terminal()
    terminal.pop("exec_quantity")
    broker = _Broker(
        positions=[[_position(1)]],
        orders=[[_stop()], []],
        terminal=terminal,
    )
    result = _run(broker)
    assert result["allowed"] is False
    assert result["replacement_qty"] == 0
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
        terminal=_terminal(),
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
        terminal=_terminal(),
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


def test_takeover_requires_strict_order_inventory_instead_of_legacy_fallback():
    broker = _Broker(positions=[[_position(1)]], orders=[_stop()])
    broker.list_orders_strict = None
    result = _run(broker)
    assert result["allowed"] is False
    assert result["replacement_qty"] == 0
    assert result["reason"] == "EXIT_PROTECTIVE_ORDERS_UNAVAILABLE"
    assert result["audit"]["reason"] == "list_orders_strict_unavailable"
    assert broker.list_orders_calls == 0
    assert broker.cancel_calls == []


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


def test_historical_terminal_without_fill_or_remaining_evidence_holds():
    historical = _stop("history-x", status="canceled")
    historical.pop("exec_quantity")
    broker = _Broker(
        positions=[[_position(1)], [_position(1)]],
        orders=[[historical], []],
    )
    result = _run(broker)
    assert result["allowed"] is False
    assert result["replacement_qty"] == 0
    assert result["reason"] == "EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS"
    assert result["audit"]["reason"] == "quantity_unproven"
    assert broker.cancel_calls == []


def test_same_occ_reentry_ignores_pre_entry_terminal_fill_in_both_snapshots():
    historical = _stop(
        "history-x",
        status="filled",
        executed=1,
        fill_ts=HISTORICAL_FILL_TS,
    )
    broker = _Broker(
        positions=[[_position(1)], [_position(1)]],
        orders=[[historical], [dict(historical)]],
    )
    result = _run(broker, entry_ts=CURRENT_ENTRY_TS)
    assert result["allowed"] is True
    assert result["replacement_qty"] == 1
    assert result["reason"] == "EXIT_PROTECTIVE_NO_CONFLICT"
    assert result["audit"]["terminal_historical_fill_ids"] == ["history-x"]
    assert result["audit"]["observed_terminal_execution_total"] == 0
    assert broker.cancel_calls == []


def test_current_position_terminal_fill_after_entry_still_holds_on_stale_position():
    current_fill = _stop(
        "current-fill",
        status="filled",
        executed=1,
        fill_ts=CURRENT_FILL_TS,
    )
    broker = _Broker(
        positions=[[_position(1)], [_position(1)]],
        orders=[[current_fill], [dict(current_fill)]],
    )
    result = _run(broker, entry_ts=CURRENT_ENTRY_TS)
    assert result["allowed"] is False
    assert result["replacement_qty"] == 0
    assert result["reason"] == "EXIT_PROTECTIVE_POSITION_UNPROVEN"
    assert result["audit"]["reason"] == "position_snapshot_stale_after_order_fill"
    assert result["audit"]["terminal_current_fill_ids"] == ["current-fill"]
    assert result["audit"]["observed_terminal_execution_total"] == 1
    assert broker.cancel_calls == []


def test_durable_current_protective_fill_counts_from_first_terminal_inventory():
    protective_id = "current-protective"
    current_fill = _stop(protective_id, status="filled", executed=1)
    broker = _Broker(
        positions=[[_position(1)], [_position(1)]],
        orders=[[current_fill], [dict(current_fill)]],
        terminal=current_fill,
    )
    result = _run(
        broker,
        entry_ts=CURRENT_ENTRY_TS,
        protective_broker_order_id=protective_id,
    )
    assert result["allowed"] is False
    assert result["replacement_qty"] == 0
    assert result["reason"] == "EXIT_PROTECTIVE_POSITION_UNPROVEN"
    assert result["audit"]["terminal_current_fill_ids"] == [protective_id]
    assert result["audit"]["observed_terminal_execution_total"] == 1
    assert broker.cancel_calls == []


def test_terminal_fill_delta_does_not_get_subtracted_from_final_position():
    initial = _stop("history-x", status="canceled", executed=0)
    final = _stop("history-x", status="canceled", executed=1)
    broker = _Broker(
        positions=[[_position(2)], [_position(1)]],
        orders=[[initial], [final]],
    )
    result = _run(broker, qty=2, entry_ts=CURRENT_ENTRY_TS)
    assert result["allowed"] is True
    assert result["replacement_qty"] == 1
    assert result["audit"]["terminal_current_fill_ids"] == ["history-x"]
    assert result["audit"]["observed_terminal_execution_delta"] == 1
    assert broker.cancel_calls == []


def test_terminal_fill_delta_requires_final_position_coherence_before_sizing():
    initial = _stop("history-x", status="canceled", executed=0, qty=2)
    final = _stop("history-x", status="canceled", executed=1, qty=2)
    broker = _Broker(
        positions=[[_position(2)], [_position(2)]],
        orders=[[initial], [final]],
    )
    result = _run(broker, qty=2)
    assert result["allowed"] is False
    assert result["replacement_qty"] == 0
    assert result["reason"] == "EXIT_PROTECTIVE_POSITION_UNPROVEN"
    assert result["audit"]["reason"] == "position_snapshot_stale_after_order_fill"
    assert broker.cancel_calls == []


def test_duplicate_terminal_order_id_is_not_multiple_terminal_fills():
    historical = _stop("history-x", status="canceled", executed=0)
    broker = _Broker(
        positions=[[_position(1)], [_position(1)]],
        orders=[[historical], [dict(historical)]],
    )
    result = _run(broker)
    assert result["allowed"] is True
    assert result["replacement_qty"] == 1
    assert result["reason"] == "EXIT_PROTECTIVE_NO_CONFLICT"
    assert broker.cancel_calls == []


def test_post_takeover_reinventory_blocks_external_exact_active_sell():
    broker = _Broker(
        positions=[[_position(1)], [_position(1)]],
        orders=[[_stop()], [_stop("external-sell", status="open", qty=1)]],
        terminal=_terminal(),
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


def test_strict_position_snapshot_none_is_not_authoritative_flat():
    class _StrictNoneBroker:
        account_id = "ACC123"

        def list_positions_strict(self):
            return None

    result = resolve_exit_broker_truth(
        broker=_StrictNoneBroker(), client_id=CLIENT, contract=CONTRACT,
    )
    assert result["broker_truth_open_qty"] is None
    assert result["is_fresh_exact"] is False
    assert result["audit"]["snapshot_status"] == "broker_positions_malformed"


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
    assert broker.list_orders_strict() == []


def test_production_tradier_legacy_list_orders_filters_malformed_members_and_empty_nodes():
    broker = TradierBroker(
        TradierConfig(
            base_url="https://api.tradier.com",
            access_token="test-token",
            account_id="ACC123",
        )
    )
    valid = {"id": "valid"}
    payloads = [
        ({}, []),
        ({"orders": {}}, []),
        ({"orders": None}, []),
        ({"orders": ""}, []),
        ({"orders": "null"}, []),
        ({"orders": {"order": ""}}, []),
        ({"orders": {"order": "null"}}, []),
        ({"orders": {"unexpected": []}}, []),
        ({"orders": {"order": [valid, "MALFORMED_ORDER_ROW"]}}, [valid]),
    ]
    for payload, expected in payloads:
        broker._get = lambda *args, _payload=payload, **kwargs: _payload
        assert broker.list_orders() == expected


@pytest.mark.parametrize(
    "orders_payload",
    [
        {},
        {"orders": {"unexpected": []}},
        {"orders": "not-null"},
        {"orders": {"order": "not-null"}},
        {"orders": {"order": [{"id": "valid"}, "MALFORMED_ORDER_ROW"]}},
    ],
)
def test_production_tradier_strict_list_orders_rejects_ambiguous_payload(orders_payload):
    broker = TradierBroker(
        TradierConfig(
            base_url="https://api.tradier.com",
            access_token="test-token",
            account_id="ACC123",
        )
    )
    broker._get = lambda *args, **kwargs: orders_payload
    with pytest.raises(ValueError, match="TRADIER_ORDERS_PAYLOAD_MALFORMED"):
        broker.list_orders_strict()


@pytest.mark.parametrize("orders_payload", [{"orders": "null"}, {"orders": {"order": "null"}}])
def test_production_tradier_null_order_shapes_are_authoritative_empty(orders_payload):
    broker = TradierBroker(
        TradierConfig(
            base_url="https://api.tradier.com",
            access_token="test-token",
            account_id="ACC123",
        )
    )
    broker._get = lambda *args, **kwargs: orders_payload
    assert broker.list_orders() == []
    assert broker.list_orders_strict() == []


@pytest.mark.parametrize(
    "orders_payload",
    [
        {"orders": None},
        {"orders": ""},
        {"orders": {}},
        {"orders": {"order": None}},
        {"orders": {"order": ""}},
        {"orders": {"order": []}},
    ],
)
def test_production_tradier_other_known_empty_order_shapes_are_authoritative_empty(orders_payload):
    broker = TradierBroker(
        TradierConfig(
            base_url="https://api.tradier.com",
            access_token="test-token",
            account_id="ACC123",
        )
    )
    broker._get = lambda *args, **kwargs: orders_payload
    assert broker.list_orders_strict() == []


def test_takeover_uses_strict_tradier_inventory_not_legacy_method():
    broker = TradierBroker(
        TradierConfig(
            base_url="https://api.tradier.com",
            access_token="test-token",
            account_id="ACC123",
        )
    )
    position_payload = {"positions": {"position": {"symbol": CONTRACT, "quantity": "1"}}}
    orders_payload = {"orders": {"order": []}}

    def _get(path, *args, **kwargs):
        if path.endswith("/positions"):
            return position_payload
        if path.endswith("/orders"):
            return orders_payload
        raise AssertionError(f"unexpected endpoint: {path}")

    broker._get = _get
    broker.list_orders = lambda: (_ for _ in ()).throw(AssertionError("legacy list_orders used"))
    result = _run(broker)
    assert result["allowed"] is True
    assert result["replacement_qty"] == 1


def test_durable_gtc_protective_id_is_proved_when_current_order_list_is_empty():
    protective_id = "prior-session-gtc"
    active = _stop(protective_id)
    terminal = _stop(protective_id, status="canceled")
    broker = _Broker(
        positions=[[_position(1)], [_position(1)]],
        orders=[[], []],
        terminal=[active, terminal],
    )
    result = _run(broker, protective_broker_order_id=protective_id)
    assert result["allowed"] is True
    assert result["replacement_qty"] == 1
    assert broker.cancel_calls == [protective_id]
    assert broker.get_calls == [protective_id, protective_id]


def test_durable_gtc_protective_id_get_failure_holds_before_replacement():
    protective_id = "prior-session-gtc"
    broker = _Broker(
        positions=[[_position(1)]],
        orders=[[]],
        terminal=RuntimeError("order lookup unavailable"),
    )
    result = _run(broker, protective_broker_order_id=protective_id)
    assert result["allowed"] is False
    assert result["replacement_qty"] == 0
    assert result["reason"] == "EXIT_PROTECTIVE_IDENTITY_UNPROVEN"
    assert broker.cancel_calls == []


@pytest.mark.parametrize(
    "orders_payload",
    [
        {},
        {"orders": {"unexpected": []}},
        {"orders": "not-null"},
        {"orders": {"order": "not-null"}},
    ],
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
        ({"status": "rejected"}, "TERMINAL_NO_ORDER", None),
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

    assert len(writes) == 2
    pending = __import__("json").loads(writes[0][1][0])["protective_order"]
    assert pending["protective_order_state"] == "PLACEMENT_PENDING"
    assert pending["protective_broker_order_id"] is None
    payload = __import__("json").loads(writes[-1][1][0])["protective_order"]
    assert payload["protective_order_state"] == expected_state
    assert payload["protective_broker_order_id"] == expected_id
    assert payload["client_id"] == CLIENT
    assert payload["execution_mode"] == "live"
    assert payload["protective_contract"] == CONTRACT


@pytest.mark.parametrize(
    ("status_code", "expected_state"),
    [
        (422, "TERMINAL_NO_ORDER"),
        (409, "OUTCOME_UNPROVEN"),
        (500, "OUTCOME_UNPROVEN"),
    ],
)
def test_rest_stop_rejection_distinguishes_proven_no_order_from_ambiguity(
    monkeypatch, status_code, expected_state,
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

    class _Response:
        def __init__(self):
            self.status_code = status_code
            self.text = "stop rejected"

    class _Session:
        def __init__(self):
            self.calls = []

        def post(self, *args, **kwargs):
            self.calls.append((args, kwargs))
            return _Response()

    class _StopBroker:
        base_url = "https://api.tradier.com"
        account_id = "ACC123"

        def __init__(self):
            self.session = _Session()

    broker = _StopBroker()
    monkeypatch.setattr(fill_monitor_mod, "conn", lambda: _Conn())
    monkeypatch.setattr(fill_monitor_mod, "run_with_retry", lambda fn, *a, **k: fn())
    monkeypatch.setattr(fill_monitor_mod, "audit", lambda *a, **k: None)

    fill_monitor_mod._place_standing_stop_best_effort(
        broker=broker,
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

    payload = __import__("json").loads(writes[-1][1][0])["protective_order"]
    assert payload["protective_order_state"] == expected_state
    assert payload["protective_broker_order_id"] is None
    assert len(broker.session.calls) == 1


def test_standing_stop_is_not_posted_when_pending_identity_persistence_misses(monkeypatch):
    writes = []
    placement_calls = []

    class _Cursor:
        rowcount = 0

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
            placement_calls.append(kwargs)
            return {"id": "must-not-be-placed", "status": "open"}

    monkeypatch.setattr(fill_monitor_mod, "conn", lambda: _Conn())
    monkeypatch.setattr(fill_monitor_mod, "run_with_retry", lambda fn, *a, **k: fn())
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
    assert placement_calls == []


def test_unproven_standing_stop_marker_blocks_durable_identity_lookup():
    class _OSM:
        client_id = CLIENT

        def get_orders_for_position(self, position_id):
            return [
                {
                    "position_id": position_id,
                    "kind": "ENTRY",
                    "client_id": CLIENT,
                    "contract": CONTRACT,
                    "execution_mode": "live",
                    "meta": {
                        "protective_order": {
                            "protective_order_state": "OUTCOME_UNPROVEN",
                            "protective_broker_order_id": None,
                            "protective_contract": CONTRACT,
                            "protective_source": "standing_stop",
                            "execution_mode": "live",
                            "client_id": CLIENT,
                        }
                    },
                }
            ]

    assert _durable_protective_order_id_for_position(
        _OSM(),
        position_id="position-now-live-1",
        contract=CONTRACT,
        execution_mode="live",
    ) == (None, "durable_protective_identity_unproven")


def test_terminal_no_order_marker_does_not_claim_a_protective_owner():
    class _OSM:
        client_id = CLIENT

        def get_orders_for_position(self, position_id):
            return [
                {
                    "position_id": position_id,
                    "kind": "ENTRY",
                    "client_id": CLIENT,
                    "contract": CONTRACT,
                    "execution_mode": "live",
                    "meta": {
                        "protective_order": {
                            "protective_order_state": "TERMINAL_NO_ORDER",
                            "protective_broker_order_id": None,
                            "protective_contract": CONTRACT,
                            "protective_source": "standing_stop",
                            "execution_mode": "live",
                            "client_id": CLIENT,
                        }
                    },
                }
            ]

    assert _durable_protective_order_id_for_position(
        _OSM(),
        position_id="position-now-live-1",
        contract=CONTRACT,
        execution_mode="live",
    ) == (None, None)
