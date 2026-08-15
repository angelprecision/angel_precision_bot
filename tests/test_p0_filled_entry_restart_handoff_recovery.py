"""Focused P0 regressions for terminal FILLED ENTRY handoff recovery."""

from __future__ import annotations

import ast
from datetime import date
import inspect
from pathlib import Path
from types import SimpleNamespace

import pytest

import os

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
os.environ.setdefault("AP_ENV", "test")

from ap import fill_monitor as fm
from ap.filled_entry_recovery_authority import (
    evaluate_filled_entry_recovery_authority,
)


CLIENT = "jasoncosby1@gmail.com"
LIVE = "live"
PAPER = "paper"
CONTRACT = "PEP260821C00141000"
PAPER_CONTRACT = "PEP260821C00141000"
LOCAL_ID = "1b8ec150-38cc-424c-93e5-b1a1820ea50b"
BROKER_ID = "141999576"
POSITION_ID = "position-jason-pep"


def _order(**overrides):
    row = {
        "client_id": CLIENT,
        "local_order_id": LOCAL_ID,
        "broker_order_id": BROKER_ID,
        "position_id": None,
        "kind": "ENTRY",
        "status": "FILLED",
        "symbol": "PEP",
        "contract": CONTRACT,
        "direction": "CALL",
        "qty": 1,
        "filled_qty": 1,
        "fill_price": 1.58,
        "execution_mode": LIVE,
        "meta": {"filled_entry_handoff_state": "IN_PROGRESS"},
    }
    row.update(overrides)
    return row


class _PM:
    def __init__(self, positions=None):
        self.positions = list(positions or [])
        self.open_calls = []

    def get_active_positions(self):
        return list(self.positions)

    def get_position_by_local_order(self, local_order_id):
        return next((p for p in self.positions if p.get("local_order_id") == local_order_id), None)

    def get_position_by_broker_order(self, broker_order_id):
        return next((p for p in self.positions if p.get("broker_order_id") == broker_order_id), None)

    def get_position(self, position_id):
        return next((p for p in self.positions if p.get("id") == position_id), None)

    def open_position(self, **kwargs):
        self.open_calls.append(kwargs)
        position_id = f"position-{len(self.positions) + 1}"
        self.positions.append(
            {
                "id": position_id,
                "client_id": kwargs["client_id"],
                "execution_mode": kwargs["execution_mode"],
                "contract": kwargs["contract"],
                "status": "OPEN",
                "quantity_remaining": kwargs["qty"],
                "local_order_id": kwargs["local_order_id"],
                "broker_order_id": kwargs["broker_order_id"],
            }
        )
        return position_id


class _Broker:
    def __init__(self, positions=None):
        self.positions = list(positions or [])
        self.mutations = []

    def list_positions_authoritative(self):
        return list(self.positions)

    def place_stop_order(self, **kwargs):
        self.mutations.append(("stop", kwargs))
        raise AssertionError("terminal FILLED recovery must not place a standing stop")

    def submit_order(self, **kwargs):
        self.mutations.append(("submit", kwargs))
        raise AssertionError("terminal FILLED recovery must not submit")

    def cancel_order(self, *args, **kwargs):
        self.mutations.append(("cancel", args, kwargs))
        raise AssertionError("terminal FILLED recovery must not cancel")


class _OSM:
    def __init__(self, marker_result=True):
        self.marker_result = marker_result
        self.events = []

    def update_order_meta(self, local_order_id, patch):
        self.events.append(("meta", local_order_id, patch))
        return self.marker_result

    def transition(self, local_order_id, status, **kwargs):
        self.events.append(("transition", local_order_id, status, kwargs))
        return True


def _broker_position(contract=CONTRACT, quantity=1):
    return {"symbol": contract, "quantity": quantity}


def test_jason_live_pep_current_risk_allows_bounded_recreate():
    result = evaluate_filled_entry_recovery_authority(
        pm=_PM(),
        broker=_Broker([_broker_position()]),
        broker_positions=[_broker_position()],
        order=_order(),
        runtime_execution_mode=LIVE,
        today=date(2026, 8, 14),
    )

    assert result["disposition"] == "ACTIVE_RECREATE"
    assert result["broker_qty"] == 1
    assert result["durable_filled_qty"] == 1


def test_current_broker_absence_is_nonactionable_not_recreate():
    result = evaluate_filled_entry_recovery_authority(
        pm=_PM(),
        broker=_Broker(),
        broker_positions=[],
        order=_order(),
        runtime_execution_mode=LIVE,
        today=date(2026, 8, 14),
    )

    assert result["disposition"] == "NONACTIONABLE"


@pytest.mark.parametrize(
    "order_overrides",
    [
        {"execution_mode": "LIVE"},
        {"execution_mode": PAPER},
        {"client_id": "jose@example.com"},
        {"contract": "PEP260821X00141000"},
        {"filled_qty": 1.5},
        {"fill_price": float("nan")},
    ],
)
def test_recovery_identity_or_money_truth_holds(order_overrides):
    result = evaluate_filled_entry_recovery_authority(
        pm=_PM(),
        broker=_Broker([_broker_position()]),
        broker_positions=[_broker_position()],
        order=_order(**order_overrides),
        runtime_execution_mode=LIVE,
        expected_client_id=CLIENT,
        today=date(2026, 8, 14),
    )

    assert result["disposition"] == "HOLD"


def test_recovery_applies_explicit_client_fence(monkeypatch):
    calls = []
    monkeypatch.setattr(fm, "_persist_filled_entry_handoff_state", _memory_persist(calls))

    result = fm.recover_interrupted_filled_entry_handoff(
        broker=_Broker([_broker_position()]),
        order=_order(),
        pm=_PM(),
        runtime_execution_mode=LIVE,
        expected_client_id="different-client@example.com",
        broker_positions=[_broker_position()],
    )

    assert result["disposition"] == "HOLD"
    assert result["reason_code"] == "FILLED_ENTRY_RECOVERY_CLIENT_MISMATCH"
    assert calls == [("HOLD", None)]


def test_recovery_query_is_separate_from_normal_pending_query():
    assert hasattr(fm, "get_interrupted_filled_entry_handoffs")
    assert hasattr(fm, "recover_interrupted_filled_entry_handoff")


def test_fresh_fill_persists_in_progress_before_terminal_osm(monkeypatch):
    order = _order(status="ACKNOWLEDGED", filled_qty=0)
    osm = _OSM()
    events = []
    monkeypatch.setattr(
        fm,
        "check_order_with_broker",
        lambda *_args, **_kwargs: {"status": "FILLED", "filled_qty": 1, "avg_fill": 1.58, "raw": {}},
    )
    monkeypatch.setattr(fm, "audit", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "trace_gate", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "_cancel_pair_opposite", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "_open_position_safe", lambda *args, **kwargs: POSITION_ID)
    monkeypatch.setattr(fm, "_place_standing_stop_best_effort", lambda **kwargs: None)
    monkeypatch.setattr(fm, "_bind_filled_entry_durable_identity", lambda **kwargs: (True, "BOUND"))
    monkeypatch.setattr(fm, "_seed_exit_engine", lambda *args, **kwargs: (True, "SEEDED"))
    monkeypatch.setattr(fm, "_release_entry_guards", lambda *args, **kwargs: None)

    def persist(*args, **kwargs):
        state = args[1] if len(args) > 1 else kwargs.get("state")
        events.append(("persist", state))
        return True

    def transition(*args, **kwargs):
        events.append(("transition", args[1] if len(args) > 1 else kwargs.get("status")))
        return True

    monkeypatch.setattr(fm, "_persist_filled_entry_handoff_state", persist)
    monkeypatch.setattr(osm, "transition", transition)

    fm.process_pending_order(
        _Broker(), order, osm=osm, pm=_PM(), exit_engine=SimpleNamespace(execution_mode=LIVE),
        runtime_execution_mode=LIVE,
    )

    marker_index = events.index(("persist", "IN_PROGRESS"))
    terminal_index = events.index(("transition", "FILLED"))
    assert marker_index < terminal_index


def test_marker_persistence_failure_keeps_fill_nonterminal(monkeypatch):
    order = _order(status="ACKNOWLEDGED", filled_qty=0)
    osm = _OSM(marker_result=False)
    side_effects = []
    monkeypatch.setattr(
        fm,
        "check_order_with_broker",
        lambda *_args, **_kwargs: {"status": "FILLED", "filled_qty": 1, "avg_fill": 1.58, "raw": {}},
    )
    monkeypatch.setattr(fm, "audit", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "_persist_filled_entry_handoff_state", lambda *args, **kwargs: False)
    monkeypatch.setattr(fm, "_cancel_pair_opposite", lambda *args, **kwargs: side_effects.append("cancel"))
    monkeypatch.setattr(fm, "_open_position_safe", lambda *args, **kwargs: side_effects.append("open"))

    fm.process_pending_order(
        _Broker(), order, osm=osm, pm=_PM(), exit_engine=SimpleNamespace(execution_mode=LIVE),
        runtime_execution_mode=LIVE,
    )

    assert not any(event[0] == "transition" for event in osm.events)
    assert side_effects == []


def test_terminal_recovery_has_zero_broker_mutations_and_no_osm_filled_replay(monkeypatch):
    order = _order(meta={"filled_entry_handoff_state": "IN_PROGRESS"})
    broker = _Broker([_broker_position()])
    osm = _OSM()
    monkeypatch.setattr(fm, "_persist_filled_entry_handoff_state", lambda *args, **kwargs: True)
    monkeypatch.setattr(fm, "_bind_filled_entry_durable_identity", lambda **kwargs: (True, "BOUND"))
    monkeypatch.setattr(fm, "_seed_exit_engine", lambda *args, **kwargs: (True, "SEEDED"))
    monkeypatch.setattr(fm, "_release_entry_guards", lambda *args, **kwargs: None)

    result = fm.recover_interrupted_filled_entry_handoff(
        broker=broker,
        order=order,
        osm=osm,
        pm=_PM([{
            "id": POSITION_ID,
            "client_id": CLIENT,
            "execution_mode": LIVE,
            "contract": CONTRACT,
            "status": "OPEN",
            "quantity_remaining": 1,
            "local_order_id": LOCAL_ID,
            "broker_order_id": BROKER_ID,
        }]),
        exit_engine=SimpleNamespace(execution_mode=LIVE, active_positions=lambda: []),
        runtime_execution_mode=LIVE,
        expected_client_id=CLIENT,
        broker_positions=[_broker_position()],
    )

    assert result["disposition"] in {"ACTIVE_EXISTING", "HOLD"}
    assert broker.mutations == []
    assert not any(event[0] == "transition" for event in osm.events)


def _existing_position(**overrides):
    position = {
        "id": POSITION_ID,
        "client_id": CLIENT,
        "execution_mode": LIVE,
        "contract": CONTRACT,
        "status": "OPEN",
        "quantity_remaining": 1,
        "local_order_id": LOCAL_ID,
        "broker_order_id": BROKER_ID,
    }
    position.update(overrides)
    return position


def _memory_persist(calls, *, complete_write=True, release_marker=True):
    def persist(order, state, **kwargs):
        position_id = kwargs.get("position_id")
        calls.append((state, position_id))
        if state == "COMPLETE" and not complete_write:
            return False
        meta = dict(order.get("meta") or {})
        meta["filled_entry_handoff_state"] = state
        if release_marker and kwargs.get("extra_meta"):
            meta.update(kwargs["extra_meta"])
        order["meta"] = meta
        if position_id:
            order["position_id"] = position_id
        return True

    return persist


def test_authority_allows_one_exact_active_local_position():
    result = evaluate_filled_entry_recovery_authority(
        pm=_PM([_existing_position()]),
        order=_order(position_id=POSITION_ID),
        runtime_execution_mode=LIVE,
        expected_client_id=CLIENT,
        broker_positions=[_broker_position()],
        today=date(2026, 8, 14),
    )

    assert result["disposition"] == "ACTIVE_EXISTING"
    assert result["position_id"] == POSITION_ID


def test_authority_holds_local_terminal_position_against_current_broker_risk():
    result = evaluate_filled_entry_recovery_authority(
        pm=_PM([_existing_position(status="CLOSED")]),
        order=_order(position_id=POSITION_ID),
        runtime_execution_mode=LIVE,
        expected_client_id=CLIENT,
        broker_positions=[_broker_position()],
        today=date(2026, 8, 14),
    )

    assert result["disposition"] == "HOLD"
    assert "TERMINAL" in result["reason_code"]


def test_authority_holds_multiple_local_candidates():
    result = evaluate_filled_entry_recovery_authority(
        pm=_PM([_existing_position(), _existing_position(id="position-duplicate")]),
        order=_order(),
        runtime_execution_mode=LIVE,
        expected_client_id=CLIENT,
        broker_positions=[_broker_position()],
        today=date(2026, 8, 14),
    )

    assert result["disposition"] == "HOLD"
    assert result["reason_code"] == "FILLED_ENTRY_RECOVERY_LOCAL_POSITION_AMBIGUOUS"


def test_authority_holds_same_contract_with_different_local_identity():
    result = evaluate_filled_entry_recovery_authority(
        pm=_PM(
            [
                _existing_position(
                    id="other-position",
                    local_order_id="other-local",
                    broker_order_id="other-broker",
                )
            ]
        ),
        order=_order(),
        runtime_execution_mode=LIVE,
        expected_client_id=CLIENT,
        broker_positions=[_broker_position()],
        today=date(2026, 8, 14),
    )

    assert result["disposition"] == "HOLD"
    assert result["reason_code"] == "FILLED_ENTRY_RECOVERY_LOCAL_POSITION_AMBIGUOUS"


def test_authority_holds_when_durable_position_identity_is_missing_locally():
    result = evaluate_filled_entry_recovery_authority(
        pm=_PM(),
        order=_order(position_id=POSITION_ID),
        runtime_execution_mode=LIVE,
        expected_client_id=CLIENT,
        broker_positions=[_broker_position()],
        today=date(2026, 8, 14),
    )

    assert result["disposition"] == "HOLD"
    assert result["reason_code"] == "FILLED_ENTRY_RECOVERY_LOCAL_POSITION_MISSING_DURABLE_IDENTITY"


def test_authority_holds_when_broker_quantity_does_not_equal_durable_fill():
    result = evaluate_filled_entry_recovery_authority(
        pm=_PM(),
        order=_order(filled_qty=1),
        runtime_execution_mode=LIVE,
        expected_client_id=CLIENT,
        broker_positions=[_broker_position(quantity=2)],
        today=date(2026, 8, 14),
    )

    assert result["disposition"] == "HOLD"
    assert result["reason_code"] == "FILLED_ENTRY_RECOVERY_RECREATE_QUANTITY_UNPROVEN"


def test_authority_holds_duplicate_current_broker_contract_rows():
    result = evaluate_filled_entry_recovery_authority(
        pm=_PM(),
        order=_order(),
        runtime_execution_mode=LIVE,
        expected_client_id=CLIENT,
        broker_positions=[_broker_position(), _broker_position()],
        today=date(2026, 8, 14),
    )

    assert result["disposition"] == "HOLD"
    assert result["reason_code"] == "FILLED_ENTRY_RECOVERY_BROKER_POSITION_AMBIGUOUS"


def test_authority_holds_when_current_broker_snapshot_is_unavailable():
    result = evaluate_filled_entry_recovery_authority(
        pm=_PM(),
        order=_order(),
        runtime_execution_mode=LIVE,
        expected_client_id=CLIENT,
        broker_positions_error="broker timeout",
        today=date(2026, 8, 14),
    )

    assert result["disposition"] == "HOLD"
    assert result["retryable"] is True
    assert result["reason_code"] == "FILLED_ENTRY_RECOVERY_BROKER_POSITIONS_UNAVAILABLE"


@pytest.mark.parametrize("payload", [None, "not-a-list", {}, 1])
def test_authority_holds_malformed_current_broker_snapshot(payload):
    result = evaluate_filled_entry_recovery_authority(
        pm=_PM(),
        order=_order(),
        runtime_execution_mode=LIVE,
        expected_client_id=CLIENT,
        broker_positions=payload,
        today=date(2026, 8, 14),
    )

    assert result["disposition"] == "HOLD"
    assert result["reason_code"] == "FILLED_ENTRY_RECOVERY_BROKER_POSITIONS_MALFORMED"


@pytest.mark.parametrize(
    "payload",
    [
        [{}],
        [{"symbol": CONTRACT, "quantity": 0}],
        [{"symbol": CONTRACT, "quantity": 0, "qty": 1}],
        [{"symbol": CONTRACT, "quantity": 1.5}],
        [{"symbol": CONTRACT, "quantity": True}],
    ],
)
def test_authority_holds_unproven_current_broker_quantity(payload):
    result = evaluate_filled_entry_recovery_authority(
        pm=_PM(),
        order=_order(),
        runtime_execution_mode=LIVE,
        expected_client_id=CLIENT,
        broker_positions=payload,
        today=date(2026, 8, 14),
    )

    assert result["disposition"] == "HOLD"


@pytest.mark.parametrize(
    "overrides, runtime_mode",
    [
        ({"broker_order_id": "N/A"}, LIVE),
        ({"kind": "EXIT"}, LIVE),
        ({"status": "ACKNOWLEDGED"}, LIVE),
        ({"execution_mode": "LIVE"}, LIVE),
        ({}, None),
        ({"execution_mode": PAPER}, LIVE),
        ({"contract": "PEP260821X00141000"}, LIVE),
        ({"filled_qty": 1.5}, LIVE),
        ({"fill_price": float("inf")}, LIVE),
    ],
)
def test_authority_holds_identity_mode_or_money_conflicts(overrides, runtime_mode):
    result = evaluate_filled_entry_recovery_authority(
        pm=_PM(),
        order=_order(**overrides),
        runtime_execution_mode=runtime_mode,
        expected_client_id=CLIENT,
        broker_positions=[_broker_position()],
        today=date(2026, 8, 14),
    )

    assert result["disposition"] == "HOLD"


def test_authority_marks_expired_historical_fill_nonactionable():
    contract = "PEP240101C00141000"
    result = evaluate_filled_entry_recovery_authority(
        pm=_PM(),
        order=_order(contract=contract),
        runtime_execution_mode=LIVE,
        expected_client_id=CLIENT,
        broker_positions=[_broker_position(contract=contract)],
        today=date(2026, 8, 14),
    )

    assert result["disposition"] == "NONACTIONABLE"
    assert result["reason_code"] == "HISTORICAL_FILLED_RECOVERY_EXPIRED_CONTRACT"


def test_jose_paper_mode_is_isolated_and_recoverable():
    result = evaluate_filled_entry_recovery_authority(
        pm=_PM(),
        order=_order(client_id="jose@example.com", execution_mode=PAPER),
        runtime_execution_mode=PAPER,
        expected_client_id="jose@example.com",
        broker_positions=[_broker_position()],
        today=date(2026, 8, 14),
    )

    assert result["disposition"] == "ACTIVE_RECREATE"


@pytest.mark.parametrize(
    "payload",
    [
        [{"symbol": "SPY", "quantity": 10}, {"symbol": CONTRACT, "quantity": 1}],
        [{"symbol": "SPY", "quantity": 10}],
    ],
)
def test_authority_ignores_equity_rows_but_not_option_identity(payload):
    result = evaluate_filled_entry_recovery_authority(
        pm=_PM(),
        order=_order(),
        runtime_execution_mode=LIVE,
        expected_client_id=CLIENT,
        broker_positions=payload,
        today=date(2026, 8, 14),
    )

    assert result["disposition"] in {"ACTIVE_RECREATE", "NONACTIONABLE"}


@pytest.mark.parametrize(
    "payload, expected_message",
    [
        ([{"option_symbol": "PEP-INVALID", "quantity": 1}], "OCC_INVALID"),
        ([{"symbol": CONTRACT, "quantity": 0}], "QUANTITY_INVALID"),
        ([{"symbol": CONTRACT, "quantity": 0, "qty": 1}], "QUANTITY_INVALID"),
        ([{"symbol": CONTRACT, "quantity": 1.5}], "QUANTITY_INVALID"),
    ],
)
def test_broker_snapshot_adapter_rejects_malformed_option_rows(
    monkeypatch, payload, expected_message
):
    import ap.manual_close_reconciliation as reconciliation

    monkeypatch.setattr(
        reconciliation,
        "fetch_authoritative_broker_positions",
        lambda _broker: payload,
    )
    from ap.filled_entry_recovery_authority import fetch_current_broker_positions

    with pytest.raises(ValueError, match=expected_message):
        fetch_current_broker_positions(object())


def test_broker_snapshot_adapter_returns_exact_option_rows_and_ignores_equities(monkeypatch):
    import ap.manual_close_reconciliation as reconciliation

    monkeypatch.setattr(
        reconciliation,
        "fetch_authoritative_broker_positions",
        lambda _broker: [{"symbol": "SPY", "quantity": 10}, _broker_position()],
    )
    from ap.filled_entry_recovery_authority import fetch_current_broker_positions

    assert fetch_current_broker_positions(object())[0]["symbol"] == CONTRACT


def test_active_existing_recovery_never_reopens_or_places_protection(monkeypatch):
    order = _order(position_id=POSITION_ID)
    pm = _PM([_existing_position()])
    broker = _Broker([_broker_position()])
    calls = []
    release_calls = []
    monkeypatch.setattr(fm, "_persist_filled_entry_handoff_state", _memory_persist(calls))
    monkeypatch.setattr(fm, "_bind_filled_entry_durable_identity", lambda **kwargs: (True, "BOUND"))
    monkeypatch.setattr(fm, "_seed_exit_engine", lambda *args, **kwargs: (True, "SEEDED"))
    monkeypatch.setattr(fm, "_release_entry_guards", lambda *args, **kwargs: release_calls.append(True))
    monkeypatch.setattr(
        fm,
        "_open_position_safe",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("must not recreate")),
    )
    monkeypatch.setattr(
        fm,
        "_place_standing_stop_best_effort",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("must not place stop")),
    )
    monkeypatch.setattr(
        fm,
        "_cancel_pair_opposite",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("must not cancel pair")),
    )

    result = fm.recover_interrupted_filled_entry_handoff(
        broker=broker,
        order=order,
        osm=_OSM(),
        pm=pm,
        exit_engine=SimpleNamespace(execution_mode=LIVE),
        runtime_execution_mode=LIVE,
        expected_client_id=CLIENT,
        broker_positions=[_broker_position()],
    )

    assert result["disposition"] == "ACTIVE_EXISTING"
    assert result["completed"] is True
    assert pm.open_calls == []
    assert release_calls == [True]
    assert broker.mutations == []
    assert [state for state, _ in calls] == ["IN_PROGRESS", "IN_PROGRESS", "COMPLETE"]


def test_active_recreate_recovery_opens_once_without_fresh_fill_side_effects(monkeypatch):
    order = _order()
    pm = _PM()
    broker = _Broker([_broker_position()])
    calls = []
    release_calls = []

    def recreate(*args, **kwargs):
        return pm.open_position(
            client_id=CLIENT,
            execution_mode=LIVE,
            contract=CONTRACT,
            qty=1,
            local_order_id=LOCAL_ID,
            broker_order_id=BROKER_ID,
        )

    monkeypatch.setattr(fm, "_open_position_safe", recreate)
    monkeypatch.setattr(fm, "_persist_filled_entry_handoff_state", _memory_persist(calls))
    monkeypatch.setattr(fm, "_bind_filled_entry_durable_identity", lambda **kwargs: (True, "BOUND"))
    monkeypatch.setattr(fm, "_seed_exit_engine", lambda *args, **kwargs: (True, "SEEDED"))
    monkeypatch.setattr(fm, "_release_entry_guards", lambda *args, **kwargs: release_calls.append(True))
    monkeypatch.setattr(fm, "_place_standing_stop_best_effort", lambda *args, **kwargs: calls.append(("STOP", None)))
    monkeypatch.setattr(fm, "_cancel_pair_opposite", lambda *args, **kwargs: calls.append(("CANCEL", None)))

    result = fm.recover_interrupted_filled_entry_handoff(
        broker=broker,
        order=order,
        osm=_OSM(),
        pm=pm,
        exit_engine=SimpleNamespace(execution_mode=LIVE),
        runtime_execution_mode=LIVE,
        expected_client_id=CLIENT,
        broker_positions=[_broker_position()],
    )

    assert result["disposition"] == "ACTIVE_RECREATE"
    assert result["completed"] is True
    assert len(pm.open_calls) == 1
    assert release_calls == [True]
    assert not any(state in {"STOP", "CANCEL"} for state, _ in calls)
    assert broker.mutations == []


def test_recovery_bind_failure_holds_before_guard_release_or_complete(monkeypatch):
    order = _order()
    calls = []
    release_calls = []
    monkeypatch.setattr(fm, "_open_position_safe", lambda *args, **kwargs: POSITION_ID)
    monkeypatch.setattr(fm, "_record_position_create_failure", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "_persist_filled_entry_handoff_state", _memory_persist(calls))
    monkeypatch.setattr(fm, "_bind_filled_entry_durable_identity", lambda **kwargs: (False, "IDENTITY_CONFLICT"))
    monkeypatch.setattr(fm, "_release_entry_guards", lambda *args, **kwargs: release_calls.append(True))

    result = fm.recover_interrupted_filled_entry_handoff(
        broker=_Broker([_broker_position()]),
        order=order,
        pm=_PM(),
        exit_engine=SimpleNamespace(execution_mode=LIVE),
        runtime_execution_mode=LIVE,
        expected_client_id=CLIENT,
        broker_positions=[_broker_position()],
    )

    assert result["disposition"] == "HOLD"
    assert result["reason_code"].startswith("FILLED_ENTRY_POSITION_IDENTITY_BIND_FAILED")
    assert release_calls == []
    assert "COMPLETE" not in [state for state, _ in calls]


def test_recovery_owner_seed_failure_holds_before_guard_release(monkeypatch):
    order = _order(position_id=POSITION_ID)
    calls = []
    release_calls = []
    monkeypatch.setattr(fm, "_persist_filled_entry_handoff_state", _memory_persist(calls))
    monkeypatch.setattr(fm, "_bind_filled_entry_durable_identity", lambda **kwargs: (True, "BOUND"))
    monkeypatch.setattr(fm, "_seed_exit_engine", lambda *args, **kwargs: (False, "OWNER_LOOKUP_FAILED"))
    monkeypatch.setattr(fm, "_release_entry_guards", lambda *args, **kwargs: release_calls.append(True))
    monkeypatch.setattr(fm, "_record_position_create_failure", lambda *args, **kwargs: None)

    result = fm.recover_interrupted_filled_entry_handoff(
        broker=_Broker([_broker_position()]),
        order=order,
        pm=_PM([_existing_position()]),
        exit_engine=SimpleNamespace(execution_mode=LIVE),
        runtime_execution_mode=LIVE,
        expected_client_id=CLIENT,
        broker_positions=[_broker_position()],
    )

    assert result["disposition"] == "HOLD"
    assert result["reason_code"].startswith("FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN")
    assert release_calls == []
    assert "COMPLETE" not in [state for state, _ in calls]


def test_recovery_guard_release_failure_holds_without_complete(monkeypatch):
    order = _order(position_id=POSITION_ID)
    calls = []
    monkeypatch.setattr(fm, "_persist_filled_entry_handoff_state", _memory_persist(calls))
    monkeypatch.setattr(fm, "_bind_filled_entry_durable_identity", lambda **kwargs: (True, "BOUND"))
    monkeypatch.setattr(fm, "_seed_exit_engine", lambda *args, **kwargs: (True, "SEEDED"))
    monkeypatch.setattr(fm, "_release_entry_guards", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("release unavailable")))
    monkeypatch.setattr(fm, "_record_position_create_failure", lambda *args, **kwargs: None)

    result = fm.recover_interrupted_filled_entry_handoff(
        broker=_Broker([_broker_position()]),
        order=order,
        pm=_PM([_existing_position()]),
        exit_engine=SimpleNamespace(execution_mode=LIVE),
        runtime_execution_mode=LIVE,
        expected_client_id=CLIENT,
        broker_positions=[_broker_position()],
    )

    assert result["disposition"] == "HOLD"
    assert result["reason_code"] == "FILLED_ENTRY_GUARDS_RELEASE_UNPROVEN"
    assert "COMPLETE" not in [state for state, _ in calls]


def test_legacy_recovery_holds_before_recreate_or_guard_release(monkeypatch):
    order = _order(meta={})
    calls = []
    release_calls = []
    monkeypatch.setattr(fm, "_persist_filled_entry_handoff_state", _memory_persist(calls))
    monkeypatch.setattr(
        fm,
        "_open_position_safe",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("must not recreate legacy handoff")),
    )
    monkeypatch.setattr(fm, "_release_entry_guards", lambda *args, **kwargs: release_calls.append(True))

    result = fm.recover_interrupted_filled_entry_handoff(
        broker=_Broker([_broker_position()]),
        order=order,
        pm=_PM(),
        exit_engine=SimpleNamespace(execution_mode=LIVE),
        runtime_execution_mode=LIVE,
        expected_client_id=CLIENT,
        broker_positions=[_broker_position()],
    )

    assert result["disposition"] == "HOLD"
    assert result["reason_code"] == "FILLED_ENTRY_LEGACY_GUARD_RELEASE_UNPROVEN"
    assert calls == [("HOLD", None)]
    assert release_calls == []


def test_recovery_complete_write_failure_remains_retryable_hold(monkeypatch):
    order = _order(position_id=POSITION_ID)
    calls = []
    release_calls = []
    monkeypatch.setattr(
        fm,
        "_persist_filled_entry_handoff_state",
        _memory_persist(calls, complete_write=False),
    )
    monkeypatch.setattr(fm, "_bind_filled_entry_durable_identity", lambda **kwargs: (True, "BOUND"))
    monkeypatch.setattr(fm, "_seed_exit_engine", lambda *args, **kwargs: (True, "SEEDED"))
    monkeypatch.setattr(fm, "_release_entry_guards", lambda *args, **kwargs: release_calls.append(True))
    monkeypatch.setattr(fm, "_record_position_create_failure", lambda *args, **kwargs: None)

    result = fm.recover_interrupted_filled_entry_handoff(
        broker=_Broker([_broker_position()]),
        order=order,
        pm=_PM([_existing_position()]),
        exit_engine=SimpleNamespace(execution_mode=LIVE),
        runtime_execution_mode=LIVE,
        expected_client_id=CLIENT,
        broker_positions=[_broker_position()],
    )

    assert result["disposition"] == "HOLD"
    assert result["reason_code"] == "FILLED_ENTRY_HANDOFF_COMPLETE_WRITE_FAILED"
    assert release_calls == [True]


def test_claimed_guard_release_outcome_is_fail_closed_and_not_repeated(monkeypatch):
    order = _order(
        position_id=POSITION_ID,
        meta={"filled_entry_guards_release_claimed": True},
    )
    monkeypatch.setattr(
        fm,
        "_release_entry_guards",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("must not repeat release")),
    )

    assert fm._release_entry_guards_once(order, position_id=POSITION_ID) is False


@pytest.mark.parametrize("marker", [None, "false", "true", 0, 1, []])
def test_malformed_guard_release_marker_fails_closed(monkeypatch, marker):
    order = _order(
        position_id=POSITION_ID,
        meta={"filled_entry_guards_released": marker},
    )
    release_calls = []
    persist_calls = []
    monkeypatch.setattr(
        fm,
        "_persist_filled_entry_handoff_state",
        _memory_persist(persist_calls),
    )
    monkeypatch.setattr(
        fm,
        "_release_entry_guards",
        lambda *args, **kwargs: release_calls.append(True),
    )

    assert fm._release_entry_guards_once(order, position_id=POSITION_ID) is False
    assert release_calls == []
    assert persist_calls == []


@pytest.mark.parametrize("marker", [None, "false", "true", 0, 1, []])
def test_complete_shortcut_requires_boolean_guard_release_marker(monkeypatch, marker):
    order = _order(
        position_id=POSITION_ID,
        meta={
            "filled_entry_handoff_state": "COMPLETE",
            "filled_entry_guards_released": marker,
        },
    )
    monkeypatch.setattr(
        fm,
        "_release_entry_guards",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("complete shortcut must not release guards")
        ),
    )

    result = fm.recover_interrupted_filled_entry_handoff(
        broker=_Broker([_broker_position()]),
        order=order,
        pm=_PM([_existing_position()]),
        runtime_execution_mode=LIVE,
        expected_client_id=CLIENT,
        broker_positions=[_broker_position()],
    )

    assert result["disposition"] == "HOLD"
    assert result["reason_code"] == "FILLED_ENTRY_GUARDS_RELEASE_UNPROVEN"


def test_guard_release_claim_is_atomic_across_stale_order_copies(monkeypatch):
    first = _order(position_id=POSITION_ID)
    second = _order(position_id=POSITION_ID)
    claim_taken = False
    release_calls = []
    persist_source = inspect.getsource(fm._persist_filled_entry_handoff_state)

    def persist(order, state, **kwargs):
        nonlocal claim_taken
        if kwargs.get("require_guard_release_claim_absent"):
            if claim_taken:
                return False
            claim_taken = True
        if kwargs.get("extra_meta"):
            order.setdefault("meta", {}).update(kwargs["extra_meta"])
        return True

    monkeypatch.setattr(fm, "_persist_filled_entry_handoff_state", persist)
    monkeypatch.setattr(fm, "_release_entry_guards", lambda *args, **kwargs: release_calls.append(True))

    assert fm._release_entry_guards_once(first, position_id=POSITION_ID) is True
    assert fm._release_entry_guards_once(second, position_id=POSITION_ID) is False
    assert release_calls == [True]
    assert "filled_entry_guards_release_claimed" in persist_source
    assert "filled_entry_guards_release_claimed' = 'false'::jsonb" in persist_source
    assert "filled_entry_guards_released' = 'true'::jsonb" in persist_source


def test_repeated_recovery_is_idempotent_for_position_open_and_guard_release(monkeypatch):
    order = _order()
    pm = _PM()
    broker = _Broker([_broker_position()])
    calls = []
    release_calls = []

    def recreate(*args, **kwargs):
        return pm.open_position(
            client_id=CLIENT,
            execution_mode=LIVE,
            contract=CONTRACT,
            qty=1,
            local_order_id=LOCAL_ID,
            broker_order_id=BROKER_ID,
        )

    monkeypatch.setattr(fm, "_open_position_safe", recreate)
    monkeypatch.setattr(fm, "_persist_filled_entry_handoff_state", _memory_persist(calls))
    monkeypatch.setattr(fm, "_bind_filled_entry_durable_identity", lambda **kwargs: (True, "BOUND"))
    monkeypatch.setattr(fm, "_seed_exit_engine", lambda *args, **kwargs: (True, "SEEDED"))
    monkeypatch.setattr(fm, "_release_entry_guards", lambda *args, **kwargs: release_calls.append(True))

    first = fm.recover_interrupted_filled_entry_handoff(
        broker=broker,
        order=order,
        pm=pm,
        exit_engine=SimpleNamespace(execution_mode=LIVE),
        runtime_execution_mode=LIVE,
        expected_client_id=CLIENT,
        broker_positions=[_broker_position()],
    )
    second = fm.recover_interrupted_filled_entry_handoff(
        broker=broker,
        order=order,
        pm=pm,
        exit_engine=SimpleNamespace(execution_mode=LIVE),
        runtime_execution_mode=LIVE,
        expected_client_id=CLIENT,
        broker_positions=[_broker_position()],
    )

    assert first["completed"] is True
    assert second["completed"] is True
    assert len(pm.open_calls) == 1
    assert release_calls == [True]
    assert broker.mutations == []


def test_recovery_function_has_no_broker_mutation_or_osm_transition_calls():
    source = inspect.getsource(fm.recover_interrupted_filled_entry_handoff)
    tree = ast.parse(source)
    call_names = {
        node.func.attr if isinstance(node.func, ast.Attribute) else node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, (ast.Attribute, ast.Name))
    }
    assert not call_names.intersection(
        {
            "check_order_with_broker",
            "_cancel_pair_opposite",
            "_place_standing_stop_best_effort",
            "submit_order",
            "cancel_order",
            "place_stop_order",
            "transition",
        }
    )


def test_startup_rehydrates_filled_entry_positions_before_monitors_start():
    client_runner = Path(fm.__file__).resolve().parents[1] / "client_runner.py"
    source = client_runner.read_text()
    startup = source[source.index("self._run_startup_recovery("):]
    assert startup.index("self._run_startup_recovery(") < startup.index("self._seed_exit_engine_from_db(")
    assert startup.index("self._seed_exit_engine_from_db(") < startup.index("self._start_position_quote_monitor(")
    assert startup.index("self._start_position_quote_monitor(") < startup.index("self._start_fill_monitor(")


def test_recovery_batch_takes_one_current_broker_snapshot():
    source = inspect.getsource(fm.fill_monitor_loop)
    assert source.count("fetch_current_broker_positions(broker)") == 1
    assert "expected_client_id=client_id" in source


def test_interrupted_query_does_not_silently_filter_malformed_money_or_mode_truth():
    source = inspect.getsource(fm.get_interrupted_filled_entry_handoffs)
    assert "filled_qty > 0" not in source
    assert "fill_price > 0" not in source
    assert "execution_mode IN ('live','paper')" not in source
    assert "OR (\n                      AND" not in source
