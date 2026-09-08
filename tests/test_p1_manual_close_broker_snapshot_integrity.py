"""PR #592: incomplete broker-position truth cannot authorize manual close."""

from __future__ import annotations

import math
import os
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

# Module import installs the repository's DB guard even though these tests
# replace every durable boundary with in-memory doubles.
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@127.0.0.1:5432/test")

from ap import manual_close_reconciliation as manual_mod


CLIENT = "client@example.com"
CONTRACT = "AAPL250102C00100000"
OTHER_CONTRACT = "AAPL250102P00100000"


def _position(**overrides):
    row = {
        "id": "position-592",
        "client_id": CLIENT,
        "contract": CONTRACT,
        "underlying": "AAPL",
        "side": "CALL",
        "direction": "CALL",
        "qty": 1,
        "quantity_remaining": 1,
        "avg_fill": 0.52,
        "entry_price": 0.52,
        "entry_ts": "2025-01-02T14:00:00+00:00",
        "opened_at": "2025-01-02T14:00:00+00:00",
        "execution_mode": "live",
        "status": "OPEN",
        "exit_in_flight": False,
        "pending_exit_broker_order_id": None,
        "pending_exit_local_order_id": None,
    }
    row.update(overrides)
    return row


def _filled_exit():
    return {
        "id": "broker-exit-592",
        "status": "filled",
        "side": "sell_to_close",
        "symbol": "AAPL",
        "option_symbol": CONTRACT,
        "quantity": 1,
        "exec_quantity": 1,
        "avg_fill_price": 1.25,
        "last_fill_date": "2025-01-02T14:59:00Z",
    }


class _Broker:
    def __init__(self, *, positions_payload, orders=None):
        self.cfg = SimpleNamespace(account_id="ACCOUNT-592")
        self.positions_payload = positions_payload
        self.orders = list(orders or [])
        self.calls: list[str] = []

    def _get(self, path):
        self.calls.append(path)
        if "/positions" in path:
            return self.positions_payload
        if "/orders" in path:
            return {"orders": {"order": self.orders}}
        raise AssertionError(f"unexpected broker path: {path}")

    def place_order(self, *args, **kwargs):  # pragma: no cover
        raise AssertionError("manual-close reconciliation must not submit")

    def cancel_order(self, *args, **kwargs):  # pragma: no cover
        raise AssertionError("manual-close reconciliation must not cancel")


def _runner(broker, finalizer):
    return SimpleNamespace(
        email=CLIENT,
        mode="LIVE",
        broker=broker,
        position_manager=SimpleNamespace(close_position_from_exit_fill=finalizer),
        core=SimpleNamespace(exit_eng=SimpleNamespace(mark_position_closed=MagicMock())),
        _last_manual_close_check_ts=0.0,
    )


def _install_scan(monkeypatch, finalizer):
    monkeypatch.setattr(manual_mod, "MANUAL_CLOSE_INTERVAL_SEC", 0)
    monkeypatch.setattr(manual_mod.time, "time", lambda: 1735830000.0)
    monkeypatch.setattr(
        manual_mod,
        "load_manual_close_state",
        lambda *_: ([_position()], set(), {}),
    )
    monkeypatch.setattr(manual_mod, "load_terminal_recovery_candidates", lambda *_: [])
    adopted: list[dict] = []

    def _adopt(**kwargs):
        adopted.append(kwargs)
        return True, "test_adoption"

    monkeypatch.setattr(manual_mod, "adopt_external_exit_fills", _adopt)
    return adopted


def _payload(rows):
    return {"positions": {"position": rows}}


def test_valid_and_non_dict_rows_are_incomplete_not_a_reduced_complete_snapshot():
    snapshot = manual_mod.normalize_positions_payload(
        _payload([{"symbol": OTHER_CONTRACT, "quantity": 1}, "malformed"])
    )

    assert snapshot.state == manual_mod.POSITIONS_INCOMPLETE
    assert snapshot.rows == []
    assert snapshot.reason == "position_row_1_not_mapping"


@pytest.mark.parametrize(
    "payload",
    [
        None,
        [],
        {"positions": []},
        {"positions": {}},
        {"positions": {"position": "malformed"}},
        {"status": "unavailable", "positions": None},
    ],
)
def test_malformed_position_envelopes_are_not_empty_authority(payload):
    snapshot = manual_mod.normalize_positions_payload(payload)

    assert snapshot.state not in manual_mod.POSITION_COMPLETE_STATES
    assert snapshot.rows == []


@pytest.mark.parametrize("quantity", [0, 1.5, True, float("nan"), math.inf, "bad"])
def test_invalid_option_quantity_invalidates_the_whole_snapshot(quantity):
    snapshot = manual_mod.normalize_positions_payload(
        _payload([{"symbol": CONTRACT, "quantity": quantity}])
    )

    assert snapshot.state == manual_mod.POSITIONS_INCOMPLETE
    assert snapshot.rows == []


def test_signed_position_quantities_preserve_exact_presence_evidence():
    snapshot = manual_mod.normalize_positions_payload(
        _payload(
            [
                {"symbol": CONTRACT, "quantity": -1},
                {"symbol": "MSFT", "quantity": -100},
            ]
        )
    )

    assert snapshot.state == manual_mod.POSITIONS_AVAILABLE_COMPLETE_NONEMPTY
    assert [row["quantity"] for row in snapshot.rows] == [-1, -100]
    quantities, reason = manual_mod._broker_position_contract_quantities(snapshot)
    assert quantities == {CONTRACT: -1}
    assert reason == ""


def test_conflicting_quantity_aliases_invalidate_the_whole_snapshot():
    snapshot = manual_mod.normalize_positions_payload(
        _payload([{"symbol": CONTRACT, "quantity": 1, "qty": 2}])
    )

    assert snapshot.state == manual_mod.POSITIONS_INCOMPLETE
    assert snapshot.rows == []


@pytest.mark.parametrize(
    "row",
    [
        {"quantity": 1},
        {"symbol": "not-an-occ-symbol", "quantity": 1},
        {"symbol": "AAPL", "side": "CALL", "quantity": 1},
        {"option_symbol": "", "quantity": 1},
        {"underlying": "AAPL", "quantity": 1},
    ],
)
def test_weak_or_missing_option_identity_invalidates_the_whole_snapshot(row):
    snapshot = manual_mod.normalize_positions_payload(_payload([row]))

    assert snapshot.state == manual_mod.POSITIONS_INCOMPLETE
    assert snapshot.rows == []


def test_duplicate_exact_occ_rows_are_ambiguous():
    snapshot = manual_mod.normalize_positions_payload(
        _payload(
            [
                {"symbol": CONTRACT, "quantity": 1},
                {"symbol": CONTRACT, "quantity": 1},
            ]
        )
    )

    assert snapshot.state == manual_mod.POSITIONS_AMBIGUOUS
    assert snapshot.rows == []


def test_conflicting_exact_occ_aliases_are_ambiguous():
    snapshot = manual_mod.normalize_positions_payload(
        _payload(
            [
                {
                    "symbol": CONTRACT,
                    "option_symbol": OTHER_CONTRACT,
                    "quantity": 1,
                }
            ]
        )
    )

    assert snapshot.state == manual_mod.POSITIONS_AMBIGUOUS
    assert snapshot.rows == []


def test_complete_snapshot_requires_exact_option_identity_and_preserves_rows():
    snapshot = manual_mod.normalize_positions_payload(
        _payload(
            [
                {"symbol": CONTRACT, "quantity": 1},
                {"symbol": "MSFT", "quantity": 20},
            ]
        )
    )

    assert snapshot.state == manual_mod.POSITIONS_AVAILABLE_COMPLETE_NONEMPTY
    assert [row["symbol"] for row in snapshot.rows] == [CONTRACT, "MSFT"]
    quantities, reason = manual_mod._broker_position_contract_quantities(snapshot)
    assert quantities == {CONTRACT: 1}
    assert reason == ""


def test_documented_null_positions_payload_is_complete_empty_authority():
    snapshot = manual_mod.normalize_positions_payload({"positions": "null"})

    assert snapshot.state == manual_mod.POSITIONS_AVAILABLE_COMPLETE_EMPTY
    assert snapshot.rows == []
    assert manual_mod._broker_position_contract_quantities(snapshot) == ({}, "")


def test_json_null_positions_node_preserves_complete_empty_authority():
    snapshot = manual_mod.normalize_positions_payload({"positions": None})

    assert snapshot.state == manual_mod.POSITIONS_AVAILABLE_COMPLETE_EMPTY
    assert snapshot.rows == []
    assert manual_mod._broker_position_contract_quantities(snapshot) == ({}, "")


def test_json_null_position_member_preserves_complete_empty_authority():
    snapshot = manual_mod.normalize_positions_payload(
        {"positions": {"position": None}}
    )

    assert snapshot.state == manual_mod.POSITIONS_AVAILABLE_COMPLETE_EMPTY
    assert snapshot.rows == []
    assert manual_mod._broker_position_contract_quantities(snapshot) == ({}, "")


def test_nested_error_position_envelope_is_malformed_not_empty_authority():
    snapshot = manual_mod.normalize_positions_payload(
        {"positions": {"position": [], "error": "unavailable"}}
    )

    assert snapshot.state == manual_mod.POSITIONS_MALFORMED
    assert snapshot.rows == []
    assert snapshot.reason == "broker_positions_node_error"


def test_authoritative_adapter_rows_are_validated_without_filtering_bad_rows():
    class _AuthoritativeBroker:
        def list_positions_authoritative(self):
            return [{"symbol": CONTRACT, "quantity": 1}, object()]

    snapshot = manual_mod.fetch_authoritative_broker_positions(_AuthoritativeBroker())

    assert snapshot.state == manual_mod.POSITIONS_INCOMPLETE
    assert snapshot.rows == []


def test_authoritative_adapter_failure_is_unavailable():
    class _UnavailableBroker:
        def list_positions_authoritative(self):
            raise RuntimeError("broker down")

    snapshot = manual_mod.fetch_authoritative_broker_positions(_UnavailableBroker())

    assert snapshot.state == manual_mod.POSITIONS_UNAVAILABLE
    assert snapshot.rows == []


def test_incomplete_snapshot_blocks_order_discovery_and_adoption(monkeypatch):
    broker = _Broker(
        positions_payload=_payload(
            [{"symbol": OTHER_CONTRACT, "quantity": 1}, "malformed"]
        ),
        orders=[_filled_exit()],
    )
    finalizer = MagicMock(return_value=True)
    runner = _runner(broker, finalizer)
    adopted = _install_scan(monkeypatch, finalizer)

    manual_mod.detect_manual_closes(runner)

    assert adopted == []
    finalizer.assert_not_called()
    assert len(broker.calls) == 1
    assert "/positions" in broker.calls[0]
    assert not any("/orders" in call for call in broker.calls)


def test_nested_error_snapshot_blocks_order_discovery_and_adoption(monkeypatch):
    broker = _Broker(
        positions_payload={"positions": {"position": [], "error": "unavailable"}},
        orders=[_filled_exit()],
    )
    finalizer = MagicMock(return_value=True)
    runner = _runner(broker, finalizer)
    adopted = _install_scan(monkeypatch, finalizer)

    manual_mod.detect_manual_closes(runner)

    assert adopted == []
    finalizer.assert_not_called()
    assert len(broker.calls) == 1
    assert "/positions" in broker.calls[0]
    assert not any("/orders" in call for call in broker.calls)


def test_underlying_only_snapshot_blocks_order_discovery_and_adoption(monkeypatch):
    broker = _Broker(
        positions_payload=_payload([{"underlying": "AAPL", "quantity": 1}]),
        orders=[_filled_exit()],
    )
    finalizer = MagicMock(return_value=True)
    runner = _runner(broker, finalizer)
    adopted = _install_scan(monkeypatch, finalizer)

    manual_mod.detect_manual_closes(runner)

    assert adopted == []
    finalizer.assert_not_called()
    assert len(broker.calls) == 1
    assert "/positions" in broker.calls[0]
    assert not any("/orders" in call for call in broker.calls)


def test_invalid_quantity_snapshot_cannot_be_rescued_by_exact_external_fill(monkeypatch):
    broker = _Broker(
        positions_payload=_payload(
            [{"symbol": OTHER_CONTRACT, "quantity": 1}, {"symbol": CONTRACT, "quantity": 1.5}]
        ),
        orders=[_filled_exit()],
    )
    finalizer = MagicMock(return_value=True)
    runner = _runner(broker, finalizer)
    adopted = _install_scan(monkeypatch, finalizer)

    manual_mod.detect_manual_closes(runner)

    assert adopted == []
    finalizer.assert_not_called()
    assert not any("/orders" in call for call in broker.calls)


def test_complete_empty_snapshot_and_exact_external_fill_use_existing_path(monkeypatch):
    broker = _Broker(positions_payload={"positions": "null"}, orders=[_filled_exit()])
    finalizer_calls: list[dict] = []

    def _finalize(**kwargs):
        finalizer_calls.append(kwargs)
        return True

    runner = _runner(broker, _finalize)
    adopted = _install_scan(monkeypatch, _finalize)

    manual_mod.detect_manual_closes(runner)

    assert len(adopted) == 1
    assert adopted[0]["evidence"]["broker_order_ids"] == ["broker-exit-592"]
    assert len(finalizer_calls) == 1
    assert finalizer_calls[0]["position_id"] == "position-592"
    assert finalizer_calls[0]["filled_qty"] == 1
    assert finalizer_calls[0]["exit_price"] == pytest.approx(1.25)
    assert any("/orders" in call for call in broker.calls)


def test_well_formed_unrelated_stock_row_can_establish_option_absence(monkeypatch):
    broker = _Broker(
        positions_payload=_payload([{"symbol": "MSFT", "quantity": 20}]),
        orders=[_filled_exit()],
    )
    finalizer = MagicMock(return_value=True)
    runner = _runner(broker, finalizer)
    adopted = _install_scan(monkeypatch, finalizer)

    manual_mod.detect_manual_closes(runner)

    assert len(adopted) == 1
    finalizer.assert_called_once()


def test_unrelated_short_stock_row_can_establish_option_absence(monkeypatch):
    broker = _Broker(
        positions_payload=_payload([{"symbol": "MSFT", "quantity": -100}]),
        orders=[_filled_exit()],
    )
    finalizer = MagicMock(return_value=True)
    runner = _runner(broker, finalizer)
    adopted = _install_scan(monkeypatch, finalizer)

    manual_mod.detect_manual_closes(runner)

    assert len(adopted) == 1
    finalizer.assert_called_once()
