from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pytest

from ap.filled_entry_recovery_authority import (
    evaluate_filled_entry_recovery_authority,
)


TODAY = date(2026, 8, 13)


def _order(**overrides):
    row = {
        "client_id": "jasoncosby1@gmail.com",
        "local_order_id": "local-entry-1",
        "broker_order_id": "141000001",
        "position_id": None,
        "kind": "ENTRY",
        "status": "FILLED",
        "symbol": "F",
        "contract": "F260821P00014000",
        "direction": "PUT",
        "qty": 25,
        "filled_qty": 25,
        "fill_price": 0.24,
        "execution_mode": "live",
        "meta": {},
    }
    row.update(overrides)
    return row


class Broker:
    def __init__(self, positions=None, error: Exception | None = None):
        self.cfg = SimpleNamespace(account_id="acct-1")
        self.positions = list(positions or [])
        self.error = error
        self.calls = []

    def _get(self, path):
        self.calls.append(path)
        if self.error:
            raise self.error
        if not self.positions:
            return {"positions": "null"}
        return {"positions": {"position": [dict(row) for row in self.positions]}}


class PM:
    def __init__(self, position=None, error: Exception | None = None):
        self.position = position
        self.error = error
        self.calls = []

    def get_position_by_local_order(self, identity):
        self.calls.append(("local", identity))
        if self.error:
            raise self.error
        return self.position

    def get_position_by_broker_order(self, identity):
        self.calls.append(("broker", identity))
        if self.error:
            raise self.error
        return self.position


def _broker_position(contract="F260821P00014000", quantity=25):
    return {"symbol": contract, "quantity": quantity, "cost_basis": 600.0}


@pytest.mark.parametrize(
    "contract",
    [
        "SPY260716P00751000",
        "C260807P00134000",
        "CSCO260731C00121000",
    ],
)
def test_aug13_expired_historical_rows_are_nonactionable_without_broker_read(contract):
    broker = Broker(positions=[_broker_position(contract=contract, quantity=99)])
    result = evaluate_filled_entry_recovery_authority(
        pm=PM(),
        broker=broker,
        order=_order(contract=contract),
        today=TODAY,
    )

    assert result["disposition"] == "NONACTIONABLE"
    assert result["reason_code"] == "HISTORICAL_FILLED_RECOVERY_EXPIRED_CONTRACT"
    assert broker.calls == []


def test_unexpired_historical_fill_with_no_current_broker_position_is_nonactionable():
    broker = Broker(positions=[])
    result = evaluate_filled_entry_recovery_authority(
        pm=PM(), broker=broker, order=_order(), today=TODAY
    )

    assert result["disposition"] == "NONACTIONABLE"
    assert result["reason_code"] == "HISTORICAL_FILLED_RECOVERY_NO_CURRENT_BROKER_POSITION"
    assert broker.calls == ["/v1/accounts/acct-1/positions"]


def test_broker_position_transport_failure_holds_instead_of_guessing_absent():
    result = evaluate_filled_entry_recovery_authority(
        pm=PM(),
        broker=Broker(error=RuntimeError("tradier unavailable")),
        order=_order(),
        today=TODAY,
    )

    assert result["disposition"] == "HOLD"
    assert result["reason_code"] == "FILLED_ENTRY_RECOVERY_BROKER_POSITIONS_UNAVAILABLE"


def test_exact_current_broker_position_allows_bounded_local_recreate_only():
    result = evaluate_filled_entry_recovery_authority(
        pm=PM(),
        broker=Broker(positions=[_broker_position()]),
        order=_order(),
        today=TODAY,
    )

    assert result == {
        "disposition": "ACTIVE_RECREATE",
        "reason_code": "FILLED_ENTRY_RECOVERY_CURRENT_POSITION_PROVEN",
        "contract": "F260821P00014000",
        "broker_qty": 25,
        "durable_filled_qty": 25,
    }


def test_recreate_requires_exact_durable_filled_quantity():
    result = evaluate_filled_entry_recovery_authority(
        pm=PM(),
        broker=Broker(positions=[_broker_position(quantity=12)]),
        order=_order(filled_qty=25),
        today=TODAY,
    )

    assert result["disposition"] == "HOLD"
    assert result["reason_code"] == "FILLED_ENTRY_RECOVERY_RECREATE_QUANTITY_UNPROVEN"
    assert result["broker_qty"] == 12
    assert result["durable_filled_qty"] == 25


def test_existing_exact_active_position_and_broker_truth_allow_owner_repair():
    position = {
        "id": "position-1",
        "client_id": "jasoncosby1@gmail.com",
        "execution_mode": "live",
        "contract": "F260821P00014000",
        "status": "OPEN",
        "quantity_remaining": 25,
    }
    result = evaluate_filled_entry_recovery_authority(
        pm=PM(position=position),
        broker=Broker(positions=[_broker_position()]),
        order=_order(),
        today=TODAY,
    )

    assert result["disposition"] == "ACTIVE_EXISTING"
    assert result["position_id"] == "position-1"
    assert result["broker_qty"] == 25
    assert result["local_qty"] == 25


def test_stale_local_open_position_does_not_override_authoritative_broker_absence():
    position = {
        "id": "stale-position",
        "client_id": "jasoncosby1@gmail.com",
        "execution_mode": "live",
        "contract": "F260821P00014000",
        "status": "OPEN",
        "quantity_remaining": 25,
    }
    result = evaluate_filled_entry_recovery_authority(
        pm=PM(position=position),
        broker=Broker(positions=[]),
        order=_order(position_id="stale-position"),
        today=TODAY,
    )

    assert result["disposition"] == "NONACTIONABLE"
    assert result["reason_code"] == "HISTORICAL_FILLED_RECOVERY_NO_CURRENT_BROKER_POSITION"


def test_terminal_local_position_plus_live_broker_position_is_conflict_not_recreate():
    position = {
        "id": "closed-position",
        "client_id": "jasoncosby1@gmail.com",
        "execution_mode": "live",
        "contract": "F260821P00014000",
        "status": "CLOSED",
        "quantity_remaining": 0,
    }
    result = evaluate_filled_entry_recovery_authority(
        pm=PM(position=position),
        broker=Broker(positions=[_broker_position()]),
        order=_order(position_id="closed-position"),
        today=TODAY,
    )

    assert result["disposition"] == "HOLD"
    assert result["reason_code"] == "FILLED_ENTRY_RECOVERY_LOCAL_TERMINAL_BROKER_PRESENT_CONFLICT"


@pytest.mark.parametrize(
    "field,value",
    [
        ("execution_mode", "paper"),
        ("client_id", "other@example.com"),
        ("contract", "AAPL260821C00300000"),
    ],
)
def test_existing_position_must_match_exact_client_mode_contract(field, value):
    position = {
        "id": "position-1",
        "client_id": "jasoncosby1@gmail.com",
        "execution_mode": "live",
        "contract": "F260821P00014000",
        "status": "OPEN",
        "quantity_remaining": 25,
    }
    position[field] = value
    result = evaluate_filled_entry_recovery_authority(
        pm=PM(position=position),
        broker=Broker(positions=[_broker_position()]),
        order=_order(),
        today=TODAY,
    )

    assert result["disposition"] == "HOLD"
    assert result["reason_code"] == "FILLED_ENTRY_RECOVERY_LOCAL_POSITION_IDENTITY_CONFLICT"


def test_malformed_occ_expiration_holds_before_money_path():
    result = evaluate_filled_entry_recovery_authority(
        pm=PM(), broker=Broker(), order=_order(contract="F-NOT-OCC"), today=TODAY
    )

    assert result["disposition"] == "HOLD"
    assert result["reason_code"] == "FILLED_ENTRY_RECOVERY_CONTRACT_EXPIRATION_UNPROVEN"


def test_process_pending_order_nonactionable_recovery_has_zero_money_path_side_effects(monkeypatch):
    from ap import fill_monitor as fm

    order = _order(
        contract="SPY260716P00751000",
        symbol="SPY",
        qty=14,
        filled_qty=14,
        fill_price=0.43,
        direction="PUT",
    )

    monkeypatch.setattr(
        fm,
        "check_order_with_broker",
        lambda *_args, **_kwargs: {
            "status": "FILLED",
            "filled_qty": 14,
            "avg_fill": 0.43,
            "raw": {"status": "filled"},
        },
    )
    monkeypatch.setattr(
        fm,
        "evaluate_filled_entry_recovery_authority",
        lambda **_kwargs: {
            "disposition": "NONACTIONABLE",
            "reason_code": "HISTORICAL_FILLED_RECOVERY_EXPIRED_CONTRACT",
            "contract": "SPY260716P00751000",
        },
    )
    monkeypatch.setattr(fm, "audit", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *args, **kwargs: True)
    monkeypatch.setattr(fm, "_update_canonical_handoff_order", lambda *args, **kwargs: True)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("historical recovery reached a money-path side effect")

    monkeypatch.setattr(fm, "_cancel_pair_opposite", forbidden)
    monkeypatch.setattr(fm, "_open_position_safe", forbidden)
    monkeypatch.setattr(fm, "_establish_canonical_handoff_standing_stop", forbidden)
    monkeypatch.setattr(fm, "_seed_exit_engine", forbidden)
    monkeypatch.setattr(fm, "_release_entry_guards", forbidden)

    class OSM:
        def transition(self, *_args, **_kwargs):
            forbidden()

        def increment_retry(self, *_args, **_kwargs):
            forbidden()

    fm.process_pending_order(
        broker=Broker(),
        order=order,
        osm=OSM(),
        pm=object(),
        exit_engine=None,
        runtime_execution_mode="live",
    )


def test_broker_confirmed_recovery_never_replays_pair_cancel_or_standing_stop(monkeypatch):
    from ap import fill_monitor as fm

    order = _order(position_id="position-1")
    money_path_calls = []

    monkeypatch.setattr(
        fm,
        "check_order_with_broker",
        lambda *_args, **_kwargs: {
            "status": "FILLED",
            "filled_qty": 25,
            "avg_fill": 0.24,
            "raw": {"status": "filled"},
        },
    )
    monkeypatch.setattr(
        fm,
        "evaluate_filled_entry_recovery_authority",
        lambda **_kwargs: {
            "disposition": "ACTIVE_EXISTING",
            "reason_code": "FILLED_ENTRY_RECOVERY_CURRENT_POSITION_PROVEN",
            "position_id": "position-1",
            "contract": "F260821P00014000",
            "broker_qty": 25,
            "local_qty": 25,
        },
    )
    monkeypatch.setattr(fm, "audit", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *args, **kwargs: True)
    monkeypatch.setattr(fm, "trace_gate", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "_cancel_pair_opposite", lambda *a, **k: money_path_calls.append("cancel"))
    monkeypatch.setattr(
        fm,
        "_establish_canonical_handoff_standing_stop",
        lambda *a, **k: money_path_calls.append("stop") or {},
    )
    monkeypatch.setattr(fm, "_open_position_safe", lambda *a, **k: "position-1")
    monkeypatch.setattr(
        fm,
        "_bind_filled_entry_position_id",
        lambda *a, **k: {"ok": True, "disposition": "ALREADY_BOUND"},
    )
    monkeypatch.setattr(
        fm,
        "_seed_exit_engine",
        lambda *a, **k: {"ok": True, "disposition": "SEEDED"},
    )
    monkeypatch.setattr(
        fm,
        "_verify_canonical_entry_owner",
        lambda *a, **k: {"ok": True},
    )
    monkeypatch.setattr(fm, "_clear_canonical_owner_handoff_retry", lambda *a, **k: True)
    monkeypatch.setattr(fm, "_release_entry_guards", lambda *a, **k: None)

    class OSM:
        def transition(self, *_args, **_kwargs):
            raise AssertionError("FILLED recovery must not replay terminal OSM transition")

    class Engine:
        _lock = None

        def active_positions(self):
            return []

    fm.process_pending_order(
        broker=Broker(),
        order=order,
        osm=OSM(),
        pm=object(),
        exit_engine=Engine(),
        runtime_execution_mode="live",
    )

    assert money_path_calls == []
