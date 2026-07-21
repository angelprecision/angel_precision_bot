import os
import types
from datetime import datetime, timezone

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@127.0.0.1:5432/test")

import client_runner as runner_mod


CLIENT = "jasoncosby1@gmail.com"
CONTRACT = "F260731C00014000"
POSITION_ID = "ca06eeca-f55b-4778-8756-66c91bae877b"
ENTRY_TS = "2026-07-21T15:26:58.911238+00:00"
DETECTED_EPOCH = datetime(
    2026, 7, 21, 15, 58, 0, tzinfo=timezone.utc
).timestamp()


class _Broker:
    def __init__(self, *, positions_payload=None, orders=None, positions_error=None, orders_error=None):
        self.cfg = types.SimpleNamespace(account_id="LIVE-ACCOUNT")
        self._positions_payload = (
            {"positions": "null"} if positions_payload is None else positions_payload
        )
        self._orders = list(orders or [])
        self._positions_error = positions_error
        self._orders_error = orders_error
        self.calls = []

    def _get(self, path):
        self.calls.append(("get", path))
        if self._positions_error:
            raise self._positions_error
        return self._positions_payload

    def list_orders(self):
        self.calls.append(("list_orders", None))
        if self._orders_error:
            raise self._orders_error
        return list(self._orders)

    def place_order(self, *args, **kwargs):  # pragma: no cover - must never run
        raise AssertionError("manual-close reconciliation must not submit orders")

    def cancel_order(self, *args, **kwargs):  # pragma: no cover - must never run
        raise AssertionError("manual-close reconciliation must not cancel orders")


class _PM:
    def __init__(self, result=True):
        self.result = result
        self.calls = []

    def close_position_from_exit_fill(self, **kwargs):
        self.calls.append(kwargs)
        return self.result


class _ExitEngine:
    def __init__(self):
        self.closed = []

    def mark_position_closed(self, position_id):
        self.closed.append(position_id)


def _position(**overrides):
    row = {
        "id": POSITION_ID,
        "client_id": CLIENT,
        "contract": CONTRACT,
        "underlying": "F",
        "avg_fill": 0.73,
        "qty": 2,
        "quantity_remaining": None,
        "side": "CALL",
        "local_order_id": "5a427bfb-9bd5-4e0d-81ac-8b6a40ba8795",
        "entry_ts": ENTRY_TS,
        "opened_at": ENTRY_TS,
        "execution_mode": "live",
        "exit_in_flight": False,
        "pending_exit_broker_order_id": None,
        "pending_exit_local_order_id": None,
    }
    row.update(overrides)
    return row


def _filled_exit(**overrides):
    row = {
        "id": "137780001",
        "status": "filled",
        "side": "sell_to_close",
        "symbol": "F",
        "option_symbol": CONTRACT,
        "quantity": 2,
        "exec_quantity": 2,
        "avg_fill_price": 0.75,
        "transaction_date": "2026-07-21T15:57:39.880419Z",
    }
    row.update(overrides)
    return row


def _runner(*, broker, pm):
    runner = runner_mod.ClientRunner.__new__(runner_mod.ClientRunner)
    runner.email = CLIENT
    runner.mode = "LIVE"
    runner.broker = broker
    runner.position_manager = pm
    runner.core = types.SimpleNamespace(exit_eng=_ExitEngine())
    runner._last_manual_close_check_ts = 0.0
    return runner


def test_shadow_package_patches_supervisor_class_binding():
    assert runner_mod._base.ClientRunner is runner_mod.ClientRunner
    assert runner_mod.ClientRunner._detect_manual_closes.__module__ == "client_runner"


def test_manual_close_uses_exact_broker_fill_and_canonical_finalizer(monkeypatch):
    broker = _Broker(orders=[_filled_exit()])
    pm = _PM()
    runner = _runner(broker=broker, pm=pm)

    monkeypatch.setattr(runner_mod._time, "time", lambda: DETECTED_EPOCH)
    monkeypatch.setattr(
        runner_mod,
        "_load_manual_close_state",
        lambda client_id: ([_position()], set()),
    )

    runner._detect_manual_closes()

    assert len(pm.calls) == 1
    call = pm.calls[0]
    assert call["position_id"] == POSITION_ID
    assert call["exit_price"] == 0.75
    assert call["filled_qty"] == 2
    assert call["filled_ts"] == "2026-07-21T15:57:39.880419+00:00"
    assert call["broker_order_id"] == "137780001"
    assert call["close_source"] == "manual_client_close_broker_fill"
    assert call["close_confidence"] == "HIGH"
    assert "MANUAL_CLIENT_CLOSE_BROKER_CONFIRMED" in call["exit_reason"]
    assert runner.core.exit_eng.closed == [POSITION_ID]
    assert broker.calls == [
        ("get", "/v1/accounts/LIVE-ACCOUNT/positions"),
        ("list_orders", None),
    ]


def test_positions_query_failure_never_closes_or_reads_orders(monkeypatch):
    broker = _Broker(
        positions_error=RuntimeError("tradier unavailable"),
        orders=[_filled_exit()],
    )
    pm = _PM()
    runner = _runner(broker=broker, pm=pm)

    monkeypatch.setattr(runner_mod._time, "time", lambda: DETECTED_EPOCH)
    monkeypatch.setattr(
        runner_mod,
        "_load_manual_close_state",
        lambda client_id: ([_position()], set()),
    )

    runner._detect_manual_closes()

    assert pm.calls == []
    assert runner.core.exit_eng.closed == []
    assert broker.calls == [("get", "/v1/accounts/LIVE-ACCOUNT/positions")]


def test_orders_query_failure_never_closes_position(monkeypatch):
    broker = _Broker(orders_error=RuntimeError("orders unavailable"))
    pm = _PM()
    runner = _runner(broker=broker, pm=pm)

    monkeypatch.setattr(runner_mod._time, "time", lambda: DETECTED_EPOCH)
    monkeypatch.setattr(
        runner_mod,
        "_load_manual_close_state",
        lambda client_id: ([_position()], set()),
    )

    runner._detect_manual_closes()

    assert pm.calls == []
    assert runner.core.exit_eng.closed == []


def test_bot_owned_exit_order_is_not_reclassified_as_manual(monkeypatch):
    broker = _Broker(orders=[_filled_exit(id="KNOWN-EXIT")])
    pm = _PM()
    runner = _runner(broker=broker, pm=pm)

    monkeypatch.setattr(runner_mod._time, "time", lambda: DETECTED_EPOCH)
    monkeypatch.setattr(
        runner_mod,
        "_load_manual_close_state",
        lambda client_id: ([_position()], {"KNOWN-EXIT"}),
    )

    runner._detect_manual_closes()

    assert pm.calls == []
    assert runner.core.exit_eng.closed == []


def test_mode_mismatch_is_fenced_before_position_or_proof_mutation(monkeypatch):
    broker = _Broker(orders=[_filled_exit()])
    pm = _PM()
    runner = _runner(broker=broker, pm=pm)

    monkeypatch.setattr(runner_mod._time, "time", lambda: DETECTED_EPOCH)
    monkeypatch.setattr(
        runner_mod,
        "_load_manual_close_state",
        lambda client_id: ([_position(execution_mode="paper")], set()),
    )

    runner._detect_manual_closes()

    assert pm.calls == []
    assert runner.core.exit_eng.closed == []


def test_partial_external_fill_cannot_terminally_close_full_position(monkeypatch):
    broker = _Broker(orders=[_filled_exit(exec_quantity=1, quantity=1)])
    pm = _PM()
    runner = _runner(broker=broker, pm=pm)

    monkeypatch.setattr(runner_mod._time, "time", lambda: DETECTED_EPOCH)
    monkeypatch.setattr(
        runner_mod,
        "_load_manual_close_state",
        lambda client_id: ([_position()], set()),
    )

    runner._detect_manual_closes()

    assert pm.calls == []
    assert runner.core.exit_eng.closed == []


def test_stale_same_contract_fill_before_entry_is_rejected(monkeypatch):
    broker = _Broker(
        orders=[
            _filled_exit(
                transaction_date="2026-07-21T15:20:00Z",
                avg_fill_price=9.99,
            )
        ]
    )
    pm = _PM()
    runner = _runner(broker=broker, pm=pm)

    monkeypatch.setattr(runner_mod._time, "time", lambda: DETECTED_EPOCH)
    monkeypatch.setattr(
        runner_mod,
        "_load_manual_close_state",
        lambda client_id: ([_position()], set()),
    )

    runner._detect_manual_closes()

    assert pm.calls == []
    assert runner.core.exit_eng.closed == []


def test_multiple_manual_fills_use_quantity_weighted_broker_price(monkeypatch):
    broker = _Broker(
        orders=[
            _filled_exit(
                id="EXIT-1",
                exec_quantity=1,
                quantity=1,
                avg_fill_price=0.74,
                transaction_date="2026-07-21T15:56:00Z",
            ),
            _filled_exit(
                id="EXIT-2",
                exec_quantity=1,
                quantity=1,
                avg_fill_price=0.76,
                transaction_date="2026-07-21T15:57:39Z",
            ),
        ]
    )
    pm = _PM()
    runner = _runner(broker=broker, pm=pm)

    monkeypatch.setattr(runner_mod._time, "time", lambda: DETECTED_EPOCH)
    monkeypatch.setattr(
        runner_mod,
        "_load_manual_close_state",
        lambda client_id: ([_position()], set()),
    )

    runner._detect_manual_closes()

    assert len(pm.calls) == 1
    assert pm.calls[0]["exit_price"] == 0.75
    assert pm.calls[0]["filled_qty"] == 2
    assert pm.calls[0]["broker_order_id"] == "EXIT-2"


def test_open_broker_contract_is_not_considered_manually_closed(monkeypatch):
    broker = _Broker(
        positions_payload={
            "positions": {
                "position": {
                    "symbol": CONTRACT,
                    "quantity": 2,
                    "cost_basis": 146,
                }
            }
        },
        orders=[_filled_exit()],
    )
    pm = _PM()
    runner = _runner(broker=broker, pm=pm)

    monkeypatch.setattr(runner_mod._time, "time", lambda: DETECTED_EPOCH)
    monkeypatch.setattr(
        runner_mod,
        "_load_manual_close_state",
        lambda client_id: ([_position()], set()),
    )

    runner._detect_manual_closes()

    assert pm.calls == []
    assert runner.core.exit_eng.closed == []
    assert broker.calls == [("get", "/v1/accounts/LIVE-ACCOUNT/positions")]
