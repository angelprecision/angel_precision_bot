"""Focused P0 regressions for terminal FILLED ENTRY handoff recovery."""

from __future__ import annotations

import ast
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
import inspect
import json
import threading
from pathlib import Path
import sys
import types
from types import SimpleNamespace
import uuid

import pytest

import os

_REAL_PG_DSN = (
    os.environ.get("DATABASE_URL")
    or os.environ.get("INTELLIGENCE_POSTGRES_TEST_URL")
    or os.environ.get("MANUAL_CLOSE_POSTGRES_TEST_URL")
    or ""
)
os.environ.setdefault("DATABASE_URL", _REAL_PG_DSN or "postgresql://test:test@localhost/test")
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
        "signal_id": "signal-jason-pep",
        "plan_id": "plan-jason-pep",
        "meta": {
            "filled_entry_handoff_state": "IN_PROGRESS",
            # Most fills have no opposite 1-1 pair.  Existing tests below
            # exercise bind/seed/guard-release/idempotency logic downstream
            # of the pair-resolution gate, so the default here is resolved;
            # the pair-resolution gate itself is tested explicitly with its
            # own overrides further down this file.
            "filled_entry_pair_resolution_state": "NOT_APPLICABLE",
        },
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


@pytest.mark.parametrize(
    "unconfirmed_result",
    [
        None,
        {},
        object(),
        {"ok": False, "status": "canceled"},
        {"ok": True, "status": "unknown"},
        {"ok": True, "status": "open"},
    ],
)
def test_pair_cancel_requires_broker_confirmed_canceled_status(monkeypatch, unconfirmed_result):
    import ap.signal_pair_manager as pair_manager_module

    class _PairManager:
        def on_fill(self, **kwargs):
            return "opposite-local"

    class _PairOSM:
        def __init__(self):
            self.transitions = []

        def get_order(self, local_order_id):
            assert local_order_id == "opposite-local"
            return {"broker_order_id": "opposite-broker"}

        def transition(self, *args, **kwargs):
            self.transitions.append((args, kwargs))
            return True

    class _PairBroker:
        def __init__(self, result):
            self.result = result

        def cancel_order(self, broker_order_id):
            assert broker_order_id == "opposite-broker"
            return self.result

    monkeypatch.setattr(pair_manager_module, "get_pair_manager", lambda: _PairManager())
    monkeypatch.setattr(fm, "audit", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "_safe_alert", lambda *args, **kwargs: None)

    unconfirmed_osm = _PairOSM()
    fm._cancel_pair_opposite(
        _order(),
        _PairBroker(unconfirmed_result),
        unconfirmed_osm,
    )
    assert unconfirmed_osm.transitions == []

    confirmed_osm = _PairOSM()
    fm._cancel_pair_opposite(
        _order(),
        _PairBroker({"ok": True, "status": "canceled"}),
        confirmed_osm,
    )
    assert [args[1] for args, _kwargs in confirmed_osm.transitions] == ["CANCELED"]


def test_recovery_holds_when_active_recreate_provenance_is_missing(monkeypatch):
    order = _order(signal_id="", plan_id="")
    opened = []
    monkeypatch.setattr(fm, "_open_position_safe", lambda *args, **kwargs: opened.append(True))
    monkeypatch.setattr(fm, "_persist_filled_entry_handoff_state", lambda *args, **kwargs: True)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *args, **kwargs: None)

    result = fm.recover_interrupted_filled_entry_handoff(
        broker=_Broker([_broker_position()]),
        order=order,
        pm=_PM(),
        runtime_execution_mode=LIVE,
        expected_client_id=CLIENT,
        broker_positions=[_broker_position()],
    )

    assert result["disposition"] == "HOLD"
    assert result["reason_code"] == "FILLED_ENTRY_RECOVERY_PROVENANCE_UNPROVEN"
    assert opened == []


def test_recovery_is_processed_before_ordinary_pending_orders(monkeypatch):
    order_events = []
    recovery = _order(local_order_id="recovery-local")
    ordinary = _order(local_order_id="ordinary-local", status="ACKNOWLEDGED")

    monkeypatch.setattr(fm, "get_pending_orders", lambda _client_id: [ordinary])
    monkeypatch.setattr(fm, "get_broker_owned_exit_requests", lambda _client_id: [])
    monkeypatch.setattr(fm, "get_interrupted_filled_entry_handoffs", lambda _client_id: [recovery])
    monkeypatch.setattr(fm, "recover_interrupted_filled_entry_handoff", lambda **kwargs: order_events.append("recovery"))
    monkeypatch.setattr(fm, "process_pending_order", lambda _broker, order, **kwargs: order_events.append("ordinary"))

    import ap.filled_entry_recovery_authority as authority_module
    monkeypatch.setattr(authority_module, "fetch_current_broker_positions", lambda _broker: [])

    class _Stop:
        def __init__(self):
            self.waited = False

        def is_set(self):
            return self.waited

        def wait(self, _seconds):
            self.waited = True

    stop = _Stop()
    fm.fill_monitor_loop(
        broker=object(),
        poll_seconds=0,
        osm=SimpleNamespace(client_id=CLIENT),
        pm=SimpleNamespace(),
        exit_engine=SimpleNamespace(master_control=SimpleNamespace(mode=LIVE)),
        stop_event=stop,
        client_id=CLIENT,
    )

    assert order_events == ["recovery", "ordinary"]


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
    monkeypatch.setattr(
        fm, "_cancel_pair_opposite", lambda *args, **kwargs: ("NOT_APPLICABLE", "test_mock")
    )
    monkeypatch.setattr(
        fm,
        "_persist_filled_entry_pair_resolution_state",
        lambda *args, **kwargs: True,
    )
    monkeypatch.setattr(fm, "_open_position_safe", lambda *args, **kwargs: POSITION_ID)
    monkeypatch.setattr(fm, "_place_standing_stop_best_effort", lambda **kwargs: None)
    monkeypatch.setattr(fm, "_bind_filled_entry_durable_identity", lambda **kwargs: (True, "BOUND"))
    monkeypatch.setattr(fm, "_seed_exit_engine", lambda *args, **kwargs: (True, "SEEDED"))
    monkeypatch.setattr(fm, "_release_entry_guards_atomically", lambda *args, **kwargs: True)

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
    monkeypatch.setattr(
        fm,
        "_cancel_pair_opposite",
        lambda *args, **kwargs: (side_effects.append("cancel"), ("NOT_APPLICABLE", "test_mock"))[1],
    )
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
    monkeypatch.setattr(fm, "_release_entry_guards_atomically", lambda *args, **kwargs: True)

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
        # Alias disagreement is genuinely malformed data (which value is
        # true?), not a structurally-valid-but-non-AP-long quantity, and
        # must still be rejected at the account-wide snapshot boundary.
        ([{"symbol": CONTRACT, "quantity": 0, "qty": 1}], "QUANTITY_INVALID"),
        ([{"symbol": CONTRACT, "quantity": None}], "QUANTITY_INVALID"),
        ([{"symbol": CONTRACT, "quantity": "not-a-number"}], "QUANTITY_INVALID"),
        ([{"symbol": CONTRACT, "quantity": True}], "QUANTITY_INVALID"),
        ([{"symbol": CONTRACT, "quantity": float("nan")}], "QUANTITY_INVALID"),
        ([{"symbol": CONTRACT, "quantity": float("inf")}], "QUANTITY_INVALID"),
        ([{"symbol": CONTRACT}], "QUANTITY_INVALID"),
    ],
)
def test_broker_snapshot_adapter_rejects_malformed_option_rows(
    monkeypatch, payload, expected_message
):
    import ap.filled_entry_recovery_authority as authority

    monkeypatch.setattr(
        authority,
        "_fetch_authoritative_broker_positions",
        lambda _broker: payload,
    )
    from ap.filled_entry_recovery_authority import fetch_current_broker_positions

    with pytest.raises(ValueError, match=expected_message):
        fetch_current_broker_positions(object())


@pytest.mark.parametrize(
    "payload",
    [
        # #481 P0 integration fix: account-level broker snapshot validity is
        # not the same thing as AP exact-contract long-option authority.
        # A structurally valid negative, zero, or fractional quantity on
        # the target row must survive account-wide normalization — the
        # snapshot fetch layer must not raise for it.  AP long-only
        # validation happens later, only at the exact-OCC evaluation
        # boundary (see test_authority_holds_unproven_current_broker_quantity).
        [{"symbol": CONTRACT, "quantity": 0}],
        [{"symbol": CONTRACT, "quantity": 1.5}],
        [{"symbol": CONTRACT, "quantity": -1}],
    ],
)
def test_broker_snapshot_adapter_preserves_structurally_valid_non_long_quantity(payload):
    from ap.filled_entry_recovery_authority import _normalize_positions_payload

    node = {"positions": {"position": payload}}
    result = _normalize_positions_payload(node)
    assert result[0]["symbol"] == CONTRACT
    assert result[0]["quantity"] == payload[0]["quantity"]


def test_broker_snapshot_adapter_returns_exact_option_rows_and_ignores_equities(monkeypatch):
    import ap.filled_entry_recovery_authority as authority

    monkeypatch.setattr(
        authority,
        "_fetch_authoritative_broker_positions",
        lambda _broker: [{"symbol": "SPY", "quantity": 10}, _broker_position()],
    )
    from ap.filled_entry_recovery_authority import fetch_current_broker_positions

    assert fetch_current_broker_positions(object())[0]["symbol"] == CONTRACT


@pytest.mark.parametrize(
    "unrelated_row",
    [
        {"symbol": "SPY", "quantity": -10},
        {"symbol": "SPY", "quantity": 0.25},
        {"option_symbol": "PEP260821P00141000", "symbol": "PEP260821P00141000", "quantity": -1},
        {"symbol": "AAPL", "quantity": -20},
    ],
)
def test_unrelated_signed_or_fractional_row_does_not_poison_target_recovery(unrelated_row):
    """#481 P0 integration fix (production-shaped regression).

    This exercises the REAL production path: a broker fake without
    ``list_positions_authoritative`` (matching TradierBroker), so
    ``_fetch_authoritative_broker_positions`` falls through to
    ``broker._get(...)`` -> ``_normalize_positions_payload``, exactly as
    happens in production.  A prior defect made an unrelated
    negative/fractional account row raise during whole-snapshot
    normalization, HOLDing recovery on the exact AP target contract even
    though that contract's own broker quantity was clean.
    """
    from ap.filled_entry_recovery_authority import fetch_current_broker_positions

    class _ProductionShapedBroker:
        """No list_positions_authoritative — forces the real _get() path."""

        cfg = SimpleNamespace(account_id="acct-jason")

        def _get(self, path):
            assert path == "/v1/accounts/acct-jason/positions"
            return {
                "positions": {
                    "position": [
                        unrelated_row,
                        {"symbol": CONTRACT, "quantity": 1},
                    ]
                }
            }

    # The unrelated row must not raise and must not poison the snapshot.
    rows = fetch_current_broker_positions(_ProductionShapedBroker())
    target_rows = [row for row in rows if row["symbol"] == CONTRACT]
    assert len(target_rows) == 1
    assert target_rows[0]["quantity"] == 1

    # And the exact-OCC recovery authority must still prove the target
    # contract normally, despite the unrelated broker truth being present.
    result = evaluate_filled_entry_recovery_authority(
        pm=_PM(),
        order=_order(),
        runtime_execution_mode=LIVE,
        expected_client_id=CLIENT,
        broker_positions=rows,
        today=date(2026, 8, 14),
    )
    assert result["disposition"] == "ACTIVE_RECREATE"


# ─────────────────────────────────────────────────────────────────────────
# Merge-Gate A: present-null/missing quantity alias laundering must fail
# closed at every quantity-alias validation boundary.  A recognized alias
# key that is literally PRESENT with unusable content (None, "", whitespace,
# bool, NaN, inf, non-numeric, disagreement with another present alias) is
# NOT equivalent to that alias being absent, and must never be silently
# dropped in favor of a different, parseable alias.
# ─────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "raw_row, expected_message",
    [
        # A1: canonical quantity present-but-None, qty present-and-clean.
        # The None must not be silently dropped in favor of qty=1.
        ({"symbol": CONTRACT, "quantity": None, "qty": 1}, "QUANTITY_INVALID"),
        # A2: canonical `quantity` entirely absent; qty alone must not be
        # accepted as authoritative raw Tradier transport truth.
        ({"symbol": CONTRACT, "qty": 1}, "QUANTITY_INVALID"),
        # A3: canonical present-and-clean, qty present-but-None.
        ({"symbol": CONTRACT, "quantity": 1, "qty": None}, "QUANTITY_INVALID"),
        # A4: qty present as an empty string.
        ({"symbol": CONTRACT, "quantity": 1, "qty": ""}, "QUANTITY_INVALID"),
        # A5: qty present as a whitespace-only string.
        ({"symbol": CONTRACT, "quantity": 1, "qty": "   "}, "QUANTITY_INVALID"),
        # A6: qty present as a boolean.
        ({"symbol": CONTRACT, "quantity": 1, "qty": True}, "QUANTITY_INVALID"),
        # A8: both present and clean but numerically disagree.
        ({"symbol": CONTRACT, "quantity": 1, "qty": 2}, "QUANTITY_INVALID"),
    ],
)
def test_raw_tradier_quantity_alias_presence_is_never_laundered(
    raw_row, expected_message
):
    """Merge-Gate A: raw production-shaped Tradier input.

    Exercises _normalize_positions_payload directly with the exact raw
    node shape the real broker._get() fallback returns.  A present-but-
    unusable alias, or a missing canonical field, must HOLD — never
    silently resolve to a believable quantity via a different alias.
    """
    from ap.filled_entry_recovery_authority import _normalize_positions_payload

    node = {"positions": {"position": [raw_row]}}
    with pytest.raises(ValueError, match=expected_message):
        _normalize_positions_payload(node)


def test_raw_tradier_quantity_alias_agreement_is_structurally_valid():
    """A7: quantity=1, qty=1.0 — both present, both clean, numerically equal."""
    from ap.filled_entry_recovery_authority import _normalize_positions_payload

    node = {"positions": {"position": [{"symbol": CONTRACT, "quantity": 1, "qty": 1.0}]}}
    result = _normalize_positions_payload(node)
    assert result[0]["symbol"] == CONTRACT
    assert result[0]["quantity"] == 1.0


@pytest.mark.parametrize(
    "unrelated_row",
    [
        # A9: unrelated negative quantity row must still survive normalization.
        {"symbol": "SPY", "quantity": -10},
        # A10: unrelated fractional equity row must still survive normalization.
        {"symbol": "AAPL", "quantity": 0.25},
    ],
)
def test_unrelated_row_alias_fix_does_not_regress_prior_amendment(unrelated_row):
    """A9/A10: the alias-laundering fix must not reintroduce the account-wide
    poisoning defect closed in the prior amendment.  Unrelated valid
    negative/fractional rows must still survive alongside a clean target."""
    from ap.filled_entry_recovery_authority import _normalize_positions_payload

    node = {
        "positions": {
            "position": [unrelated_row, {"symbol": CONTRACT, "quantity": 1}]
        }
    }
    result = _normalize_positions_payload(node)
    symbols = {row["symbol"]: row["quantity"] for row in result}
    assert symbols[unrelated_row["symbol"]] == unrelated_row["quantity"]
    assert symbols[CONTRACT] == 1


@pytest.mark.parametrize(
    "target_quantity",
    [
        -1,    # A11
        0.5,   # A12
        0,     # A13
    ],
)
def test_exact_target_non_long_quantity_survives_snapshot_then_authority_holds(
    target_quantity,
):
    """A11/A12/A13: exact target row with a structurally valid but non-AP-long
    quantity (negative/fractional/zero) must survive account-wide snapshot
    normalization, then HOLD only at the exact-target-OCC authority gate."""
    from ap.filled_entry_recovery_authority import _normalize_positions_payload

    node = {"positions": {"position": [{"symbol": CONTRACT, "quantity": target_quantity}]}}
    normalized = _normalize_positions_payload(node)
    assert normalized[0]["quantity"] == target_quantity

    result = evaluate_filled_entry_recovery_authority(
        pm=_PM(),
        order=_order(),
        runtime_execution_mode=LIVE,
        expected_client_id=CLIENT,
        broker_positions=normalized,
        today=date(2026, 8, 14),
    )
    assert result["disposition"] == "HOLD"


@pytest.mark.parametrize(
    "target_quantity, expected_message",
    [
        (True, "QUANTITY_INVALID"),           # A14
        (float("nan"), "QUANTITY_INVALID"),   # A15
        (float("inf"), "QUANTITY_INVALID"),   # A16
    ],
)
def test_exact_target_malformed_quantity_fails_structural_normalization(
    target_quantity, expected_message
):
    """A14/A15/A16: boolean/NaN/inf on the exact target row are structurally
    malformed — HOLD occurs before exact-target authority is even reached,
    at account-wide snapshot normalization."""
    from ap.filled_entry_recovery_authority import _normalize_positions_payload

    node = {"positions": {"position": [{"symbol": CONTRACT, "quantity": target_quantity}]}}
    with pytest.raises(ValueError, match=expected_message):
        _normalize_positions_payload(node)


def test_duplicate_target_occ_rows_remain_ambiguous():
    """A17: duplicate target OCC rows must HOLD as ambiguous."""
    result = evaluate_filled_entry_recovery_authority(
        pm=_PM(),
        order=_order(),
        runtime_execution_mode=LIVE,
        expected_client_id=CLIENT,
        broker_positions=[
            {"symbol": CONTRACT, "quantity": 1},
            {"symbol": CONTRACT, "quantity": 1},
        ],
        today=date(2026, 8, 14),
    )
    assert result["disposition"] == "HOLD"
    assert result["reason_code"] == "FILLED_ENTRY_RECOVERY_BROKER_POSITION_AMBIGUOUS"


def test_broker_target_qty_exceeds_durable_filled_qty_holds_no_mutation(monkeypatch):
    """A18: broker target qty=2, durable filled_qty=1 — HOLD, zero mutation.

    This PR does not own partial-fill reconciliation (#480's scope); it
    must simply refuse to create a position, seed an owner, or release
    guards when broker-reported quantity contradicts the durably recorded
    filled quantity.
    """
    calls = []
    monkeypatch.setattr(fm, "_persist_filled_entry_handoff_state", _memory_persist(calls))
    monkeypatch.setattr(
        fm,
        "_open_position_safe",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("must not create position")),
    )
    monkeypatch.setattr(
        fm,
        "_seed_exit_engine",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("must not seed owner")),
    )
    monkeypatch.setattr(
        fm,
        "_release_entry_guards_atomically",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("must not release guards")),
    )

    result = fm.recover_interrupted_filled_entry_handoff(
        broker=_Broker([_broker_position(quantity=2)]),
        order=_order(filled_qty=1),
        pm=_PM(),
        exit_engine=SimpleNamespace(execution_mode=LIVE),
        runtime_execution_mode=LIVE,
        expected_client_id=CLIENT,
        broker_positions=[_broker_position(quantity=2)],
    )

    assert result["disposition"] == "HOLD"
    assert result.get("completed") is not True


def test_raw_tradier_alias_fix_normal_valid_target_row_unaffected():
    """Sanity: the ordinary single-canonical-quantity row used throughout
    this file's existing tests is completely unaffected by the alias fix."""
    from ap.filled_entry_recovery_authority import _normalize_positions_payload

    node = {"positions": {"position": [{"symbol": CONTRACT, "quantity": 1}]}}
    result = _normalize_positions_payload(node)
    assert result[0]["symbol"] == CONTRACT
    assert result[0]["quantity"] == 1


# ─────────────────────────────────────────────────────────────────────────
# Raw Tradier identity merge-gate: canonical `symbol` must be required at
# the raw transport boundary; option_symbol/contract aliases may confirm
# identity but must never manufacture it (when symbol is missing/blank) or
# override it (when symbol disagrees with an alias).
# ─────────────────────────────────────────────────────────────────────────

_DIFFERENT_OCC = "PEP260821P00141000"


@pytest.mark.parametrize(
    "raw_row",
    [
        # R1: option_symbol only, no canonical symbol at all.
        {"option_symbol": CONTRACT, "quantity": 1},
        # R2: contract only, no canonical symbol at all.
        {"contract": CONTRACT, "quantity": 1},
        # R3: canonical symbol present but empty string.
        {"symbol": "", "option_symbol": CONTRACT, "quantity": 1},
        # R4: canonical symbol present but None.
        {"symbol": None, "option_symbol": CONTRACT, "quantity": 1},
        # R15: no identity evidence of any kind.
        {"quantity": 1},
    ],
)
def test_raw_tradier_identity_missing_canonical_symbol_is_never_manufactured(raw_row):
    from ap.filled_entry_recovery_authority import _normalize_positions_payload

    node = {"positions": {"position": [raw_row]}}
    with pytest.raises(ValueError, match="IDENTITY_MISSING"):
        _normalize_positions_payload(node)


def test_raw_tradier_identity_non_occ_canonical_cannot_be_upgraded_by_alias():
    """R5: canonical symbol is non-OCC ('PEP'); alias claims target OCC.
    Must NEVER normalize to the alias's target OCC identity."""
    from ap.filled_entry_recovery_authority import _normalize_positions_payload

    node = {
        "positions": {
            "position": [{"symbol": "PEP", "option_symbol": CONTRACT, "quantity": 1}]
        }
    }
    with pytest.raises(ValueError, match="IDENTITY_AMBIGUOUS"):
        _normalize_positions_payload(node)


@pytest.mark.parametrize(
    "raw_row",
    [
        {"symbol": CONTRACT, "option_symbol": CONTRACT, "quantity": 1},  # R6
        {"symbol": CONTRACT, "contract": CONTRACT, "quantity": 1},        # R7
    ],
)
def test_raw_tradier_identity_agreeing_aliases_confirm_canonical_symbol(raw_row):
    from ap.filled_entry_recovery_authority import _normalize_positions_payload

    node = {"positions": {"position": [raw_row]}}
    result = _normalize_positions_payload(node)
    assert result[0]["symbol"] == CONTRACT
    assert result[0]["quantity"] == 1


@pytest.mark.parametrize(
    "raw_row",
    [
        {"symbol": CONTRACT, "option_symbol": _DIFFERENT_OCC, "quantity": 1},  # R8
        {"symbol": CONTRACT, "contract": _DIFFERENT_OCC, "quantity": 1},        # R9
    ],
)
def test_raw_tradier_identity_disagreeing_alias_is_ambiguous(raw_row):
    from ap.filled_entry_recovery_authority import _normalize_positions_payload

    node = {"positions": {"position": [raw_row]}}
    with pytest.raises(ValueError, match="IDENTITY_AMBIGUOUS"):
        _normalize_positions_payload(node)


def test_raw_tradier_identity_equity_fractional_remains_structurally_valid():
    """R10: unrelated equity row (AAPL, fractional) is valid raw identity;
    it is only later ignored as non-OCC by the exact-option matcher."""
    from ap.filled_entry_recovery_authority import _normalize_positions_payload

    node = {"positions": {"position": [{"symbol": "AAPL", "quantity": 0.25}]}}
    result = _normalize_positions_payload(node)
    assert result[0]["symbol"] == "AAPL"
    assert result[0]["quantity"] == 0.25


def test_raw_tradier_identity_equity_short_remains_structurally_valid():
    """R11: unrelated equity row (SPY, negative/short) is valid raw identity."""
    from ap.filled_entry_recovery_authority import _normalize_positions_payload

    node = {"positions": {"position": [{"symbol": "SPY", "quantity": -10}]}}
    result = _normalize_positions_payload(node)
    assert result[0]["symbol"] == "SPY"
    assert result[0]["quantity"] == -10


def test_raw_tradier_identity_unrelated_signed_option_does_not_poison_target():
    """R12: unrelated signed different-OCC row plus a clean target row —
    both survive normalization; target recovery remains possible."""
    from ap.filled_entry_recovery_authority import _normalize_positions_payload

    node = {
        "positions": {
            "position": [
                {"symbol": _DIFFERENT_OCC, "quantity": -1},
                {"symbol": CONTRACT, "quantity": 1},
            ]
        }
    }
    result = _normalize_positions_payload(node)
    by_symbol = {row["symbol"]: row["quantity"] for row in result}
    assert by_symbol[_DIFFERENT_OCC] == -1
    assert by_symbol[CONTRACT] == 1

    authority = evaluate_filled_entry_recovery_authority(
        pm=_PM(),
        order=_order(),
        runtime_execution_mode=LIVE,
        expected_client_id=CLIENT,
        broker_positions=result,
        today=date(2026, 8, 14),
    )
    assert authority["disposition"] == "ACTIVE_RECREATE"


def test_raw_tradier_identity_normal_target_path_unchanged():
    """R13: the ordinary single-canonical-symbol target row is unaffected."""
    from ap.filled_entry_recovery_authority import _normalize_positions_payload

    node = {"positions": {"position": [{"symbol": CONTRACT, "quantity": 1}]}}
    result = _normalize_positions_payload(node)
    assert result == [{"symbol": CONTRACT, "quantity": 1, "raw": {"symbol": CONTRACT, "quantity": 1}}]


def test_raw_tradier_identity_fix_does_not_bypass_quantity_gate():
    """R14: identity is valid, but quantity is laundered (None + qty=1).
    Proves the identity patch does not accidentally reorder past or
    bypass the already-fixed quantity alias gate."""
    from ap.filled_entry_recovery_authority import _normalize_positions_payload

    node = {
        "positions": {
            "position": [{"symbol": CONTRACT, "quantity": None, "qty": 1}]
        }
    }
    with pytest.raises(ValueError, match="QUANTITY_INVALID"):
        _normalize_positions_payload(node)


def test_raw_tradier_identity_malformed_end_to_end_never_reaches_active_recreate(
    monkeypatch,
):
    """End-to-end authority test: a raw production-shaped broker response
    with missing canonical symbol must HOLD through the full recovery
    authority evaluation — never ACTIVE_RECREATE, never any mutation."""
    from ap.filled_entry_recovery_authority import fetch_current_broker_positions

    monkeypatch.setattr(
        fm,
        "_open_position_safe",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("must not create position")
        ),
    )
    monkeypatch.setattr(
        fm,
        "_seed_exit_engine",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("must not seed owner")
        ),
    )
    monkeypatch.setattr(
        fm,
        "_release_entry_guards_atomically",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("must not release guards")
        ),
    )

    class _ProductionShapedBrokerMissingSymbol:
        cfg = SimpleNamespace(account_id="acct-jason")

        def _get(self, path):
            assert path == "/v1/accounts/acct-jason/positions"
            return {
                "positions": {
                    "position": [{"option_symbol": CONTRACT, "quantity": 1}]
                }
            }

    with pytest.raises(ValueError, match="IDENTITY_MISSING"):
        fetch_current_broker_positions(_ProductionShapedBrokerMissingSymbol())

    calls = []
    monkeypatch.setattr(fm, "_persist_filled_entry_handoff_state", _memory_persist(calls))
    result = fm.recover_interrupted_filled_entry_handoff(
        broker=_ProductionShapedBrokerMissingSymbol(),
        order=_order(),
        pm=_PM(),
        exit_engine=SimpleNamespace(execution_mode=LIVE),
        runtime_execution_mode=LIVE,
        expected_client_id=CLIENT,
        # broker_positions intentionally omitted so the real fetch path
        # (broker._get -> _normalize_positions_payload) is exercised.
    )
    assert result["disposition"] == "HOLD"
    assert result.get("completed") is not True


def test_raw_broker_position_payload_rejects_ambiguous_option_identity():
    from ap.filled_entry_recovery_authority import _normalize_positions_payload

    payload = {
        "positions": {
            "position": [
                {
                    "option_symbol": CONTRACT,
                    "symbol": "PEP260821P00141000",
                    "quantity": 1,
                }
            ]
        }
    }

    with pytest.raises(ValueError, match="IDENTITY_AMBIGUOUS"):
        _normalize_positions_payload(payload)


def test_authoritative_position_raw_alias_cannot_hide_invalid_option_truth():
    from ap.filled_entry_recovery_authority import fetch_current_broker_positions

    class _RawBroker:
        cfg = SimpleNamespace(account_id="acct-1")

        def _get(self, path):
            assert path == "/v1/accounts/acct-1/positions"
            return {
                "positions": {
                    "position": [
                        {
                            "symbol": "PEP",
                            "option_symbol": "PEP-INVALID",
                            "quantity": 1,
                        }
                    ]
                }
            }

    # Merge-gate amendment: canonical raw `symbol` ("PEP", non-OCC) now
    # correctly wins identity authority at the raw-transport layer. A
    # disagreeing option_symbol alias ("PEP-INVALID") can no longer
    # manufacture or hide behind option identity — it is rejected as an
    # identity conflict before OCC validity is even evaluated.
    with pytest.raises(ValueError, match="IDENTITY_AMBIGUOUS"):
        fetch_current_broker_positions(_RawBroker())


def test_active_existing_recovery_never_reopens_or_places_protection(monkeypatch):
    order = _order(position_id=POSITION_ID)
    pm = _PM([_existing_position()])
    broker = _Broker([_broker_position()])
    calls = []
    release_calls = []
    owner_ids = []
    monkeypatch.setattr(fm, "_persist_filled_entry_handoff_state", _memory_persist(calls))
    monkeypatch.setattr(fm, "_bind_filled_entry_durable_identity", lambda **kwargs: (True, "BOUND"))
    monkeypatch.setattr(
        fm,
        "_seed_exit_engine",
        lambda *args, **kwargs: owner_ids.append(args[1]) or (True, "SEEDED"),
    )
    monkeypatch.setattr(fm, "_release_entry_guards_atomically", lambda *args, **kwargs: release_calls.append(True) or True)
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
    assert [state for state, _ in calls] == ["COMPLETE"]


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
    monkeypatch.setattr(fm, "_release_entry_guards_atomically", lambda *args, **kwargs: release_calls.append(True) or True)
    monkeypatch.setattr(fm, "_place_standing_stop_best_effort", lambda *args, **kwargs: calls.append(("STOP", None)))
    monkeypatch.setattr(
        fm,
        "_cancel_pair_opposite",
        lambda *args, **kwargs: (calls.append(("CANCEL", None)), ("NOT_APPLICABLE", "test_mock"))[1],
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
    monkeypatch.setattr(fm, "_release_entry_guards_atomically", lambda *args, **kwargs: release_calls.append(True) or True)

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
    monkeypatch.setattr(fm, "_release_entry_guards_atomically", lambda *args, **kwargs: release_calls.append(True) or True)
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
    monkeypatch.setattr(fm, "_release_entry_guards_atomically", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("release unavailable")))
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
    monkeypatch.setattr(fm, "_release_entry_guards_atomically", lambda *args, **kwargs: release_calls.append(True) or True)

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
    monkeypatch.setattr(fm, "_release_entry_guards_atomically", lambda *args, **kwargs: release_calls.append(True) or True)
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
        "_release_entry_guards_atomically",
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
        "_release_entry_guards_atomically",
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
        "_release_entry_guards_atomically",
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
    release_calls = []

    def atomic_release(order, **kwargs):
        if release_calls:
            return False
        release_calls.append(order["local_order_id"])
        return True

    monkeypatch.setattr(fm, "_release_entry_guards_atomically", atomic_release)

    assert fm._release_entry_guards_once(first, position_id=POSITION_ID) is True
    assert fm._release_entry_guards_once(second, position_id=POSITION_ID) is False
    assert release_calls == [LOCAL_ID]


def test_atomic_guard_release_commits_marker_with_guard_mutations(monkeypatch):
    class _Conn:
        def __init__(self, lock_acquired=True):
            self.lock_acquired = lock_acquired
            self.current_row = None
            self.rowcount = 1
            self.statements = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=()):
            self.statements.append((sql, params))
            if "pg_try_advisory_xact_lock" in sql:
                self.current_row = {
                    "pg_try_advisory_xact_lock": self.lock_acquired,
                }
            elif "SELECT meta" in sql:
                # filled_ts is durable/fenced and always precedes guard
                # release; here it is 5s ago, older than the symbol-lock
                # timestamp provisioned below, so the old lock is safe to
                # delete.
                self.current_row = {
                    "meta": {"filled_entry_handoff_state": "IN_PROGRESS"},
                    "filled_ts": datetime.now(timezone.utc) - timedelta(seconds=5),
                }
            elif "SELECT v FROM kv" in sql:
                key = params[0] if params else ""
                if str(key).startswith("lock:"):
                    # Lock payload includes owner_id == LOCAL_ID: exact
                    # ownership proven → lock is safe to delete (case A).
                    old_ts = (
                        datetime.now(timezone.utc) - timedelta(seconds=10)
                    ).timestamp()
                    self.current_row = {
                        "v": json.dumps({"ts": old_ts, "owner_id": LOCAL_ID})
                    }
                else:
                    self.current_row = {"v": "120.0"}
            elif "UPDATE orders" in sql:
                self.rowcount = 1
                self.current_row = None
            else:
                self.current_row = None
            return self

        def fetchone(self):
            row = self.current_row
            self.current_row = None
            return row

    fake_conn = _Conn()
    monkeypatch.setattr(fm, "conn", lambda: fake_conn)
    monkeypatch.setattr(fm, "run_with_retry", lambda operation: operation())

    order = _order(reserved_cost=120.0)
    assert fm._release_entry_guards_atomically(order, position_id=POSITION_ID) is True
    assert order["meta"]["filled_entry_guards_released"] is True
    assert any("UPDATE orders" in sql for sql, _params in fake_conn.statements)
    assert any("INSERT INTO kv" in sql for sql, _params in fake_conn.statements)
    assert any("DELETE FROM kv" in sql for sql, _params in fake_conn.statements)

    busy_conn = _Conn(lock_acquired=False)
    monkeypatch.setattr(fm, "conn", lambda: busy_conn)
    order = _order(reserved_cost=120.0)
    assert fm._release_entry_guards_atomically(order, position_id=POSITION_ID) is False
    assert not any("UPDATE orders" in sql for sql, _params in busy_conn.statements)


def test_atomic_guard_release_preserves_newer_symbol_lock(monkeypatch):
    """BINDING INVARIANT (case C): a symbol lock whose owner_id belongs to a
    different order must never be deleted, regardless of timestamp ordering.
    Equity reservation and durable marker are still released for this order."""
    class _Conn:
        def __init__(self):
            self.current_row = None
            self.rowcount = 1
            self.statements = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=()):
            self.statements.append((sql, params))
            if "pg_try_advisory_xact_lock" in sql:
                self.current_row = {"pg_try_advisory_xact_lock": True}
            elif "SELECT meta" in sql:
                self.current_row = {
                    "meta": {"filled_entry_handoff_state": "IN_PROGRESS"},
                    "filled_ts": datetime.now(timezone.utc) - timedelta(seconds=10),
                }
            elif "SELECT v FROM kv" in sql:
                key = params[0] if params else ""
                if str(key).startswith("lock:"):
                    # Lock belongs to a DIFFERENT order (ENTRY B).  The
                    # owner_id does not match LOCAL_ID → must survive.
                    newer_ts = (
                        datetime.now(timezone.utc) - timedelta(seconds=1)
                    ).timestamp()
                    self.current_row = {
                        "v": json.dumps({
                            "ts": newer_ts,
                            "owner_id": "other-entry-b-local-order-id",
                        })
                    }
                else:
                    self.current_row = {"v": "120.0"}
            elif "UPDATE orders" in sql:
                self.rowcount = 1
                self.current_row = None
            else:
                self.current_row = None
            return self

        def fetchone(self):
            row = self.current_row
            self.current_row = None
            return row

    fake_conn = _Conn()
    monkeypatch.setattr(fm, "conn", lambda: fake_conn)
    monkeypatch.setattr(fm, "run_with_retry", lambda operation: operation())

    order = _order(reserved_cost=120.0)
    assert fm._release_entry_guards_atomically(order, position_id=POSITION_ID) is True
    assert order["meta"]["filled_entry_guards_released"] is True
    # Old order's own equity reservation is still released and the durable
    # marker still succeeds ...
    assert any("UPDATE orders" in sql for sql, _params in fake_conn.statements)
    assert any("INSERT INTO kv" in sql for sql, _params in fake_conn.statements)
    # ... but the mismatched-owner symbol lock survives: no DELETE.
    assert not any("DELETE FROM kv" in sql for sql, _params in fake_conn.statements)


def test_atomic_guard_release_no_symbol_lock_row_releases_normally(monkeypatch):
    """No symbol-lock row present: nothing to inspect or delete; normal
    successful release."""
    class _Conn:
        def __init__(self):
            self.current_row = None
            self.rowcount = 1
            self.statements = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=()):
            self.statements.append((sql, params))
            if "pg_try_advisory_xact_lock" in sql:
                self.current_row = {"pg_try_advisory_xact_lock": True}
            elif "SELECT meta" in sql:
                self.current_row = {
                    "meta": {"filled_entry_handoff_state": "IN_PROGRESS"},
                    "filled_ts": datetime.now(timezone.utc) - timedelta(seconds=5),
                }
            elif "SELECT v FROM kv" in sql:
                key = params[0] if params else ""
                if str(key).startswith("lock:"):
                    self.current_row = None  # no symbol-lock row exists
                else:
                    self.current_row = {"v": "120.0"}
            elif "UPDATE orders" in sql:
                self.rowcount = 1
                self.current_row = None
            else:
                self.current_row = None
            return self

        def fetchone(self):
            row = self.current_row
            self.current_row = None
            return row

    fake_conn = _Conn()
    monkeypatch.setattr(fm, "conn", lambda: fake_conn)
    monkeypatch.setattr(fm, "run_with_retry", lambda operation: operation())

    order = _order(reserved_cost=120.0)
    assert fm._release_entry_guards_atomically(order, position_id=POSITION_ID) is True
    assert order["meta"]["filled_entry_guards_released"] is True
    assert not any("DELETE FROM kv" in sql for sql, _params in fake_conn.statements)


def test_atomic_guard_release_legacy_lock_no_owner_token_preserves_lock(monkeypatch):
    """Case E: a legacy lock payload that has only 'ts' (no owner_id) must
    never be deleted — ownership is unproven.  The equity reservation and
    durable marker MUST still succeed so the order's handoff completes."""
    class _Conn:
        def __init__(self):
            self.current_row = None
            self.rowcount = 1
            self.statements = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=()):
            self.statements.append((sql, params))
            if "pg_try_advisory_xact_lock" in sql:
                self.current_row = {"pg_try_advisory_xact_lock": True}
            elif "SELECT meta" in sql:
                self.current_row = {
                    "meta": {"filled_entry_handoff_state": "IN_PROGRESS"},
                    "filled_ts": datetime.now(timezone.utc) - timedelta(seconds=5),
                }
            elif "SELECT v FROM kv" in sql:
                key = params[0] if params else ""
                if str(key).startswith("lock:"):
                    # Legacy payload: only "ts", no "owner_id".
                    # Ownership is unproven → lock must be preserved.
                    old_ts = (
                        datetime.now(timezone.utc) - timedelta(seconds=10)
                    ).timestamp()
                    self.current_row = {"v": json.dumps({"ts": old_ts})}
                else:
                    self.current_row = {"v": "120.0"}
            elif "UPDATE orders" in sql:
                self.rowcount = 1
                self.current_row = None
            else:
                self.current_row = None
            return self

        def fetchone(self):
            row = self.current_row
            self.current_row = None
            return row

    fake_conn = _Conn()
    monkeypatch.setattr(fm, "conn", lambda: fake_conn)
    monkeypatch.setattr(fm, "run_with_retry", lambda operation: operation())

    order = _order(reserved_cost=120.0)
    # Release MUST succeed: equity is released, marker is written.
    assert fm._release_entry_guards_atomically(order, position_id=POSITION_ID) is True
    assert order["meta"]["filled_entry_guards_released"] is True
    # But the unowned lock must NOT be deleted.
    assert not any("DELETE FROM kv" in sql for sql, _params in fake_conn.statements)
    assert any("UPDATE orders" in sql for sql, _params in fake_conn.statements)
    assert any("INSERT INTO kv" in sql for sql, _params in fake_conn.statements)


def test_atomic_guard_release_missing_filled_ts_fails_closed(monkeypatch):
    class _Conn:
        def __init__(self):
            self.current_row = None
            self.rowcount = 1
            self.statements = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=()):
            self.statements.append((sql, params))
            if "pg_try_advisory_xact_lock" in sql:
                self.current_row = {"pg_try_advisory_xact_lock": True}
            elif "SELECT meta" in sql:
                # filled_ts is missing/unproven despite an existing symbol lock.
                self.current_row = {
                    "meta": {"filled_entry_handoff_state": "IN_PROGRESS"},
                    "filled_ts": None,
                }
            elif "SELECT v FROM kv" in sql:
                key = params[0] if params else ""
                if str(key).startswith("lock:"):
                    self.current_row = {"v": json.dumps({"ts": 1000.0})}
                else:
                    self.current_row = {"v": "120.0"}
            elif "UPDATE orders" in sql:
                self.rowcount = 1
                self.current_row = None
            else:
                self.current_row = None
            return self

        def fetchone(self):
            row = self.current_row
            self.current_row = None
            return row

    fake_conn = _Conn()
    monkeypatch.setattr(fm, "conn", lambda: fake_conn)
    monkeypatch.setattr(fm, "run_with_retry", lambda operation: operation())

    order = _order(reserved_cost=120.0)
    assert fm._release_entry_guards_atomically(order, position_id=POSITION_ID) is False
    assert not any("DELETE FROM kv" in sql for sql, _params in fake_conn.statements)
    assert not any("UPDATE orders" in sql for sql, _params in fake_conn.statements)


def test_atomic_guard_release_ttl_reacquire_before_old_fill_preserved(monkeypatch):
    """Case D — CRITICAL TTL scenario:

    t=0   ENTRY A acquires lock (lock_ts = 0, owner_id = LOCAL_ID)
    t=90  A's lock TTL expires
    t=91  newer same-symbol ENTRY B reacquires lock (owner_id = ENTRY_B_ID)
    t=95  ENTRY A finally fills (filled_ts = epoch+95)
    crash before A completes handoff
    restart recovery processes A

    The current lock belongs to ENTRY B (lock_ts=91, filled_ts=95).
    Under the old code: lock_ts (91) <= filled_ts (95) → DELETE B's lock (BUG).
    Under the new code: lock owner_id is ENTRY_B_ID ≠ LOCAL_ID → PRESERVE.

    Equity release and durable marker for ENTRY A must still succeed.
    """
    # Simulate absolute epoch values matching the spec scenario.
    _epoch_base = 1_000_000_000.0          # arbitrary but reproducible
    _filled_ts_epoch = _epoch_base + 95     # ENTRY A filled at t=95
    _lock_ts_epoch   = _epoch_base + 91     # ENTRY B reacquired at t=91
    _filled_dt = datetime.fromtimestamp(_filled_ts_epoch, tz=timezone.utc)

    _ENTRY_B_LOCAL_ID = "entry-b-local-order-id-ttl-reacquire-scenario"

    class _Conn:
        def __init__(self):
            self.current_row = None
            self.rowcount = 1
            self.statements = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=()):
            self.statements.append((sql, params))
            if "pg_try_advisory_xact_lock" in sql:
                self.current_row = {"pg_try_advisory_xact_lock": True}
            elif "SELECT meta" in sql:
                self.current_row = {
                    "meta": {"filled_entry_handoff_state": "IN_PROGRESS"},
                    "filled_ts": _filled_dt,
                }
            elif "SELECT v FROM kv" in sql:
                key = params[0] if params else ""
                if str(key).startswith("lock:"):
                    # ENTRY B's lock: timestamp t=91 (numerically < filled_ts
                    # t=95), owner_id belongs to ENTRY B — NOT to LOCAL_ID.
                    # Old timestamp logic would DELETE this (bug).
                    # New owner_id logic must PRESERVE it.
                    self.current_row = {
                        "v": json.dumps({
                            "ts": _lock_ts_epoch,
                            "owner_id": _ENTRY_B_LOCAL_ID,
                        })
                    }
                else:
                    self.current_row = {"v": "120.0"}
            elif "UPDATE orders" in sql:
                self.rowcount = 1
                self.current_row = None
            else:
                self.current_row = None
            return self

        def fetchone(self):
            row = self.current_row
            self.current_row = None
            return row

    fake_conn = _Conn()
    monkeypatch.setattr(fm, "conn", lambda: fake_conn)
    monkeypatch.setattr(fm, "run_with_retry", lambda operation: operation())

    order = _order(reserved_cost=120.0)
    # ENTRY A's guard release must SUCCEED (equity released, marker written) …
    assert fm._release_entry_guards_atomically(order, position_id=POSITION_ID) is True
    assert order["meta"]["filled_entry_guards_released"] is True
    assert any("UPDATE orders" in sql for sql, _params in fake_conn.statements)
    assert any("INSERT INTO kv" in sql for sql, _params in fake_conn.statements)
    # … but ENTRY B's lock must NOT be deleted.
    assert not any("DELETE FROM kv" in sql for sql, _params in fake_conn.statements)


def test_atomic_guard_release_malformed_lock_payload_preserves_lock(monkeypatch):
    """Case F: unparseable JSON in the kv lock row must be treated as
    unproven ownership → lock preserved, equity + marker still released."""
    class _Conn:
        def __init__(self):
            self.current_row = None
            self.rowcount = 1
            self.statements = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=()):
            self.statements.append((sql, params))
            if "pg_try_advisory_xact_lock" in sql:
                self.current_row = {"pg_try_advisory_xact_lock": True}
            elif "SELECT meta" in sql:
                self.current_row = {
                    "meta": {"filled_entry_handoff_state": "IN_PROGRESS"},
                    "filled_ts": datetime.now(timezone.utc) - timedelta(seconds=5),
                }
            elif "SELECT v FROM kv" in sql:
                key = params[0] if params else ""
                if str(key).startswith("lock:"):
                    # Corrupt / unparseable payload.
                    self.current_row = {"v": "{{not valid json}}"}
                else:
                    self.current_row = {"v": "120.0"}
            elif "UPDATE orders" in sql:
                self.rowcount = 1
                self.current_row = None
            else:
                self.current_row = None
            return self

        def fetchone(self):
            row = self.current_row
            self.current_row = None
            return row

    fake_conn = _Conn()
    monkeypatch.setattr(fm, "conn", lambda: fake_conn)
    monkeypatch.setattr(fm, "run_with_retry", lambda operation: operation())

    order = _order(reserved_cost=120.0)
    # Release must succeed — equity freed, marker written.
    assert fm._release_entry_guards_atomically(order, position_id=POSITION_ID) is True
    assert order["meta"]["filled_entry_guards_released"] is True
    # Corrupt payload → lock preserved, not deleted.
    assert not any("DELETE FROM kv" in sql for sql, _params in fake_conn.statements)
    assert any("UPDATE orders" in sql for sql, _params in fake_conn.statements)
    assert any("INSERT INTO kv" in sql for sql, _params in fake_conn.statements)


def test_atomic_guard_release_symbol_lock_busy_fails_closed(monkeypatch):
    """A concurrent acquire_symbol_lock() holding the same advisory-lock
    namespace must block guard release rather than racing past it."""
    class _Conn:
        def __init__(self):
            self.current_row = None
            self.rowcount = 1
            self.statements = []
            self._advisory_calls = 0

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=()):
            self.statements.append((sql, params))
            if "pg_try_advisory_xact_lock" in sql:
                self._advisory_calls += 1
                # First call (release_key) succeeds; second (symbol_key)
                # is busy — simulating a concurrent acquire_symbol_lock().
                acquired = self._advisory_calls == 1
                self.current_row = {"pg_try_advisory_xact_lock": acquired}
            elif "SELECT meta" in sql:
                self.current_row = {
                    "meta": {"filled_entry_handoff_state": "IN_PROGRESS"},
                    "filled_ts": datetime.now(timezone.utc) - timedelta(seconds=5),
                }
            else:
                self.current_row = None
            return self

        def fetchone(self):
            row = self.current_row
            self.current_row = None
            return row

    fake_conn = _Conn()
    monkeypatch.setattr(fm, "conn", lambda: fake_conn)
    monkeypatch.setattr(fm, "run_with_retry", lambda operation: operation())

    order = _order(reserved_cost=120.0)
    assert fm._release_entry_guards_atomically(order, position_id=POSITION_ID) is False
    # Never even reached the row fence / mutation statements.
    assert not any("SELECT meta" in sql for sql, _params in fake_conn.statements)
    assert not any("UPDATE orders" in sql for sql, _params in fake_conn.statements)


def test_repeated_recovery_is_idempotent_for_position_open_and_guard_release(monkeypatch):
    order = _order()
    pm = _PM()
    broker = _Broker([_broker_position()])
    calls = []
    owner_ids = []
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
    monkeypatch.setattr(
        fm,
        "_seed_exit_engine",
        lambda *args, **kwargs: owner_ids.append(args[1]) or (True, "SEEDED"),
    )
    monkeypatch.setattr(fm, "_release_entry_guards_atomically", lambda *args, **kwargs: release_calls.append(True) or True)

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
    assert owner_ids == ["position-1"]
    assert release_calls == [True]
    assert broker.mutations == []


@contextmanager
def _real_postgres_filled_entry_guard_case():
    """Provision one isolated public row and a trigger for a real lock race."""
    if not _REAL_PG_DSN or _REAL_PG_DSN.endswith("/test"):
        pytest.skip("PostgreSQL test database is unavailable")
    psycopg2 = pytest.importorskip("psycopg2")

    token = uuid.uuid4().hex
    client_id = f"p0-recovery-race-{token}@example.com"
    local_order_id = f"p0-recovery-race-local-{token}"
    broker_order_id = f"p0-recovery-race-broker-{token}"
    position_id = f"p0-recovery-race-position-{token}"
    reserve_key = f"reserved_equity:{client_id}"
    symbol_key = f"lock:{client_id}:PEP"
    event_table = f"p0_recovery_guard_events_{token}"
    function_name = f"p0_recovery_guard_sleep_{token}"
    trigger_name = f"p0_recovery_guard_trigger_{token}"

    db = psycopg2.connect(_REAL_PG_DSN)
    db.autocommit = True
    try:
        with db.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.orders (
                    local_order_id TEXT PRIMARY KEY,
                    broker_order_id TEXT,
                    client_id TEXT,
                    position_id TEXT,
                    kind TEXT,
                    status TEXT,
                    meta JSONB DEFAULT '{}'::jsonb,
                    created_ts TIMESTAMPTZ DEFAULT NOW(),
                    updated_ts TIMESTAMPTZ DEFAULT NOW(),
                    contract TEXT,
                    execution_mode TEXT,
                    filled_qty INTEGER,
                    fill_price NUMERIC,
                    qty INTEGER,
                    signal_id TEXT,
                    plan_id TEXT,
                    filled_ts TIMESTAMPTZ
                )
                """
            )
            for column, column_type in (
                ("local_order_id", "TEXT"),
                ("broker_order_id", "TEXT"),
                ("client_id", "TEXT"),
                ("position_id", "TEXT"),
                ("kind", "TEXT"),
                ("status", "TEXT"),
                ("meta", "JSONB DEFAULT '{}'::jsonb"),
                ("created_ts", "TIMESTAMPTZ DEFAULT NOW()"),
                ("updated_ts", "TIMESTAMPTZ DEFAULT NOW()"),
                ("contract", "TEXT"),
                ("execution_mode", "TEXT"),
                ("filled_qty", "INTEGER"),
                ("fill_price", "NUMERIC"),
                ("qty", "INTEGER"),
                ("signal_id", "TEXT"),
                ("plan_id", "TEXT"),
                ("filled_ts", "TIMESTAMPTZ"),
            ):
                cur.execute(
                    f"ALTER TABLE public.orders ADD COLUMN IF NOT EXISTS {column} {column_type}"
                )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.kv (
                    k TEXT PRIMARY KEY,
                    v JSONB,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
                """
            )
            cur.execute(
                "ALTER TABLE public.kv ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()"
            )
            cur.execute(
                f"""
                CREATE TABLE public.{event_table} (
                    id BIGSERIAL PRIMARY KEY,
                    kind TEXT NOT NULL,
                    backend_pid INTEGER NOT NULL
                )
                """
            )
            cur.execute(
                f"""
                CREATE OR REPLACE FUNCTION public.{function_name}()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $function$
                BEGIN
                    IF NEW.local_order_id = '{local_order_id}' THEN
                        INSERT INTO public.{event_table} (kind, backend_pid)
                        VALUES (
                            CASE
                                WHEN COALESCE(NEW.meta->>'filled_entry_guards_released', 'false') = 'true'
                                 AND COALESCE(OLD.meta->>'filled_entry_guards_released', 'false') <> 'true'
                                THEN 'guard_release_marker'
                                ELSE 'meta_update'
                            END,
                            pg_backend_pid()
                        );
                        IF COALESCE(NEW.meta->>'filled_entry_guards_released', 'false') = 'true'
                           AND COALESCE(OLD.meta->>'filled_entry_guards_released', 'false') <> 'true' THEN
                            PERFORM pg_sleep(0.8);
                        END IF;
                    END IF;
                    RETURN NEW;
                END
                $function$
                """
            )
            cur.execute(
                f"""
                CREATE TRIGGER {trigger_name}
                BEFORE UPDATE OF meta ON public.orders
                FOR EACH ROW
                EXECUTE FUNCTION public.{function_name}()
                """
            )
            cur.execute(
                "DELETE FROM public.orders WHERE local_order_id = %s",
                (local_order_id,),
            )
            cur.execute(
                "DELETE FROM public.kv WHERE k IN (%s, %s)",
                (reserve_key, symbol_key),
            )
            meta = {
                "filled_entry_handoff_state": "IN_PROGRESS",
                "filled_entry_pair_resolution_state": "NOT_APPLICABLE",
            }
            cur.execute(
                """
                INSERT INTO public.orders
                    (local_order_id, broker_order_id, client_id, position_id,
                     kind, status, meta, contract, execution_mode, filled_qty,
                     fill_price, qty, signal_id, plan_id, filled_ts)
                VALUES (%s, %s, %s, %s, 'ENTRY', 'FILLED', %s::jsonb,
                        %s, 'live', 1, 1.58, 1, %s, %s, NOW() - INTERVAL '5 seconds')
                """,
                (
                    local_order_id,
                    broker_order_id,
                    client_id,
                    position_id,
                    json.dumps(meta),
                    CONTRACT,
                    f"signal-{token}",
                    f"plan-{token}",
                ),
            )
            cur.execute(
                """
                INSERT INTO public.kv (k, v, updated_at)
                VALUES (%s, %s::jsonb, NOW())
                ON CONFLICT (k) DO UPDATE
                    SET v = EXCLUDED.v, updated_at = EXCLUDED.updated_at
                """,
                (reserve_key, json.dumps(120.0)),
            )
            cur.execute(
                """
                INSERT INTO public.kv (k, v, updated_at)
                VALUES (%s, %s::jsonb, NOW())
                ON CONFLICT (k) DO UPDATE
                    SET v = EXCLUDED.v, updated_at = EXCLUDED.updated_at
                """,
                # P0 symbol-lock ownership fix (#473 final amendment): this
                # row must be shaped exactly like ap.state.acquire_symbol_lock()'s
                # owned payload ({"ts": ..., "owner_id": <local_order_id>}).
                # owner_id must exactly equal this order's local_order_id so
                # the guard-release fix can prove exact ownership and safely
                # delete it -- preserving this test's existing "release
                # succeeds, race resolves to one COMPLETE + one HOLD"
                # assertions. Timestamp ordering alone is no longer sufficient
                # proof; only owner_id is authoritative.
                (
                    symbol_key,
                    json.dumps(
                        {
                            "ts": (
                                datetime.now(timezone.utc) - timedelta(seconds=10)
                            ).timestamp(),
                            "owner_id": local_order_id,
                        }
                    ),
                ),
            )

        yield {
            "dsn": _REAL_PG_DSN,
            "client_id": client_id,
            "local_order_id": local_order_id,
            "broker_order_id": broker_order_id,
            "position_id": position_id,
            "reserve_key": reserve_key,
            "symbol_key": symbol_key,
            "event_table": event_table,
            "token": token,
        }
    finally:
        try:
            with db.cursor() as cur:
                cur.execute(f"DROP TRIGGER IF EXISTS {trigger_name} ON public.orders")
                cur.execute(f"DROP FUNCTION IF EXISTS public.{function_name}()")
                cur.execute(f"DROP TABLE IF EXISTS public.{event_table}")
                cur.execute(
                    "DELETE FROM public.orders WHERE local_order_id = %s",
                    (local_order_id,),
                )
                cur.execute(
                    "DELETE FROM public.kv WHERE k IN (%s, %s)",
                    (reserve_key, symbol_key),
                )
        finally:
            db.close()


def test_postgres_guard_release_race_uses_real_sessions_and_converges(monkeypatch):
    """Two real production DB sessions release one filled ENTRY exactly once."""
    with _real_postgres_filled_entry_guard_case() as case:
        monkeypatch.setattr(
            fm,
            "_bind_filled_entry_durable_identity",
            lambda **kwargs: (True, "BOUND"),
        )
        monkeypatch.setattr(
            fm,
            "_seed_exit_engine",
            lambda *args, **kwargs: (True, "SEEDED"),
        )
        monkeypatch.setattr(fm, "emit_fill_event", lambda *args, **kwargs: None)

        def make_order(meta=None):
            return _order(
                client_id=case["client_id"],
                local_order_id=case["local_order_id"],
                broker_order_id=case["broker_order_id"],
                position_id=case["position_id"],
                reserved_cost=120.0,
                signal_id=f"signal-{case['token']}",
                plan_id=f"plan-{case['token']}",
                meta=dict(
                    meta
                    or {
                        "filled_entry_handoff_state": "IN_PROGRESS",
                        "filled_entry_pair_resolution_state": "NOT_APPLICABLE",
                    }
                ),
            )

        position = _existing_position(
            id=case["position_id"],
            client_id=case["client_id"],
            local_order_id=case["local_order_id"],
            broker_order_id=case["broker_order_id"],
        )
        start = threading.Barrier(2)
        results = []
        errors = []
        session_pids = []
        result_lock = threading.Lock()

        def recover_once():
            try:
                # Hold one real production-pool session per worker until both
                # workers are present.  The recovery calls below then acquire
                # their own independent production sessions; neither conn()
                # nor run_with_retry is replaced by the test.
                with fm.conn() as session:
                    session.execute("SELECT pg_backend_pid()")
                    pid_row = session.fetchone() or {}
                    with result_lock:
                        session_pids.append(pid_row.get("pg_backend_pid"))
                    start.wait(timeout=10)
                result = fm.recover_interrupted_filled_entry_handoff(
                    broker=_Broker([_broker_position()]),
                    order=make_order(),
                    pm=_PM([position]),
                    exit_engine=SimpleNamespace(execution_mode=LIVE),
                    runtime_execution_mode=LIVE,
                    expected_client_id=case["client_id"],
                    broker_positions=[_broker_position()],
                )
                with result_lock:
                    results.append(result)
            except BaseException as exc:  # pragma: no cover - surfaced below
                with result_lock:
                    errors.append(exc)

        threads = [threading.Thread(target=recover_once) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=20)

        assert not any(thread.is_alive() for thread in threads)
        assert errors == []
        assert len(results) == 2
        assert len(set(session_pids)) == 2
        assert sum(result.get("completed") is True for result in results) == 1
        holds = [result for result in results if result.get("disposition") == "HOLD"]
        assert len(holds) == 1
        assert holds[0]["reason_code"] == "FILLED_ENTRY_GUARDS_RELEASE_UNPROVEN"

        psycopg2 = pytest.importorskip("psycopg2")
        check = psycopg2.connect(case["dsn"])
        check.autocommit = True
        try:
            with check.cursor() as cur:
                cur.execute(
                    "SELECT v, meta FROM public.kv JOIN public.orders ON TRUE "
                    "WHERE public.kv.k = %s AND public.orders.local_order_id = %s",
                    (case["reserve_key"], case["local_order_id"]),
                )
                reserved, meta = cur.fetchone()
                cur.execute(
                    f"SELECT count(*) FROM public.{case['event_table']} "
                    "WHERE kind = 'guard_release_marker'"
                )
                marker_count = cur.fetchone()[0]
                cur.execute(
                    f"SELECT count(DISTINCT backend_pid) FROM public.{case['event_table']} "
                    "WHERE kind = 'guard_release_marker'"
                )
                marker_session_count = cur.fetchone()[0]
            assert float(reserved) == 0.0
            assert meta["filled_entry_guards_release_claimed"] is True
            assert meta["filled_entry_guards_released"] is True
            assert meta["filled_entry_handoff_state"] == "COMPLETE"
            assert marker_count == 1
            assert marker_session_count == 1
        finally:
            check.close()

        retry = fm.recover_interrupted_filled_entry_handoff(
            broker=_Broker([_broker_position()]),
            order=make_order(
                meta={
                    "filled_entry_handoff_state": "COMPLETE",
                    "filled_entry_pair_resolution_state": "NOT_APPLICABLE",
                    "filled_entry_guards_release_claimed": True,
                    "filled_entry_guards_released": True,
                }
            ),
            pm=_PM([position]),
            exit_engine=SimpleNamespace(execution_mode=LIVE),
            runtime_execution_mode=LIVE,
            expected_client_id=case["client_id"],
            broker_positions=[_broker_position()],
        )
        assert retry["completed"] is True
        assert retry["reason_code"] == "FILLED_ENTRY_HANDOFF_ALREADY_COMPLETE"

        check = psycopg2.connect(case["dsn"])
        check.autocommit = True
        try:
            with check.cursor() as cur:
                cur.execute(
                    "SELECT v FROM public.kv WHERE k = %s",
                    (case["reserve_key"],),
                )
                assert float(cur.fetchone()[0]) == 0.0
                cur.execute(
                    f"SELECT count(*) FROM public.{case['event_table']} "
                    "WHERE kind = 'guard_release_marker'"
                )
                assert cur.fetchone()[0] == 1
                cur.execute(
                    "SELECT 1 FROM public.kv WHERE k = %s",
                    (case["symbol_key"],),
                )
                assert cur.fetchone() is None
        finally:
            check.close()


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


def test_fresh_exit_engine_hydration_restores_exact_position_owner(monkeypatch):
    from ap_exit_engine import APExitEngine

    engine = APExitEngine(broker=SimpleNamespace(), email=CLIENT)
    monkeypatch.setattr(engine, "hydrate_pending_exit_identity_from_db", lambda _position: False)

    class _HydrationPM:
        def get_active_positions(self):
            return [
                {
                    "id": POSITION_ID,
                    "client_id": CLIENT,
                    "signal_id": "signal-jason-pep",
                    "underlying": "PEP",
                    "contract": CONTRACT,
                    "direction": "CALL",
                    "qty": 1,
                    "quantity_remaining": 1,
                    "avg_fill": 1.58,
                    "underlying_entry": 149.0,
                    "target_underlying": 151.0,
                    "stop_underlying": 147.0,
                    "execution_mode": LIVE,
                    "meta": {},
                }
            ]

    engine.seed_from_db(_HydrationPM())

    active = engine.active_positions()
    assert len(active) == 1
    assert active[0].position_id == POSITION_ID
    assert active[0].client_id == CLIENT
    assert active[0].execution_mode == LIVE
    assert active[0].option_symbol == CONTRACT
    assert active[0].signal_id == "signal-jason-pep"


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


# =============================================================================
# P0 AMENDMENT — durable pair-resolution state for the opposite 1-1 pair
# =============================================================================


class _FakePairOSM:
    """Minimal OSM double for exercising _cancel_pair_opposite directly."""

    def __init__(self, *, opposite_broker_id="OPP-BROKER-1", transition_result=True):
        self.opposite_broker_id = opposite_broker_id
        self.transition_result = transition_result
        self.transitions = []

    def get_order(self, local_order_id):
        if not self.opposite_broker_id:
            return {"broker_order_id": None}
        return {"broker_order_id": self.opposite_broker_id}

    def transition(self, local_order_id, status, **kwargs):
        self.transitions.append((local_order_id, status, kwargs))
        return self.transition_result


def _install_fake_pair_manager(monkeypatch, *, cancel_local_id="opposite-local-id", raise_on_get=None):
    fake_module = types.ModuleType("ap.signal_pair_manager")

    class _FakeManager:
        def on_fill(self, **kwargs):
            return cancel_local_id

    def get_pair_manager():
        if raise_on_get is not None:
            raise raise_on_get
        return _FakeManager()

    fake_module.get_pair_manager = get_pair_manager
    monkeypatch.setitem(sys.modules, "ap.signal_pair_manager", fake_module)
    return fake_module


def test_pair_cancel_returns_not_applicable_when_no_opposite_exists(monkeypatch):
    _install_fake_pair_manager(monkeypatch, cancel_local_id=None)
    osm = _FakePairOSM()
    broker = _Broker()

    state, detail = fm._cancel_pair_opposite(_order(), broker, osm)

    assert state == "NOT_APPLICABLE"
    assert broker.mutations == []
    assert osm.transitions == []


def test_pair_cancel_returns_confirmed_when_broker_and_local_cancel_both_land(monkeypatch):
    _install_fake_pair_manager(monkeypatch, cancel_local_id="opposite-local-id")
    osm = _FakePairOSM(opposite_broker_id="OPP-1", transition_result=True)
    broker = _Broker()
    calls = []
    monkeypatch.setattr(fm, "audit", lambda *a, **k: None)
    broker.cancel_order = lambda *a, **k: calls.append(("cancel", a, k)) or {
        "ok": True,
        "status": "canceled",
        "broker_order_id": "OPP-1",
    }

    state, detail = fm._cancel_pair_opposite(_order(), broker, osm)

    assert state == "CONFIRMED"
    assert calls == [("cancel", ("OPP-1",), {})]
    assert osm.transitions[0][1] == "CANCELED"


def test_pair_cancel_returns_outcome_unproven_when_local_cas_misses(monkeypatch):
    _install_fake_pair_manager(monkeypatch, cancel_local_id="opposite-local-id")
    osm = _FakePairOSM(opposite_broker_id="OPP-1", transition_result=False)
    broker = _Broker()
    monkeypatch.setattr(fm, "audit", lambda *a, **k: None)
    broker.cancel_order = lambda *a, **k: {
        "ok": True,
        "status": "canceled",
        "broker_order_id": "OPP-1",
    }

    state, detail = fm._cancel_pair_opposite(_order(), broker, osm)

    assert state == "OUTCOME_UNPROVEN"
    assert detail == "PAIR_CANCEL_LOCAL_TRANSITION_UNCONFIRMED"


def test_pair_cancel_returns_outcome_unproven_when_broker_cancel_raises(monkeypatch):
    _install_fake_pair_manager(monkeypatch, cancel_local_id="opposite-local-id")
    osm = _FakePairOSM(opposite_broker_id="OPP-1")
    broker = _Broker()
    monkeypatch.setattr(fm, "audit", lambda *a, **k: None)

    def raise_cancel(*a, **k):
        raise RuntimeError("broker down")

    broker.cancel_order = raise_cancel

    state, detail = fm._cancel_pair_opposite(_order(), broker, osm)

    assert state == "OUTCOME_UNPROVEN"
    assert detail == "PAIR_CANCEL_BROKER_FAILED"
    assert osm.transitions == []


def test_pair_cancel_returns_outcome_unproven_when_opposite_broker_id_missing(monkeypatch):
    _install_fake_pair_manager(monkeypatch, cancel_local_id="opposite-local-id")
    osm = _FakePairOSM(opposite_broker_id=None)
    broker = _Broker()
    monkeypatch.setattr(fm, "audit", lambda *a, **k: None)

    state, detail = fm._cancel_pair_opposite(_order(), broker, osm)

    assert state == "OUTCOME_UNPROVEN"
    assert detail == "PAIR_CANCEL_SKIPPED_NO_BROKER_ID"
    assert broker.mutations == []


def test_pair_cancel_returns_outcome_unproven_when_side_unresolved(monkeypatch):
    _install_fake_pair_manager(monkeypatch, cancel_local_id="opposite-local-id")
    osm = _FakePairOSM()
    broker = _Broker()
    order = _order(direction=None, contract="NOTOCC")

    state, detail = fm._cancel_pair_opposite(order, broker, osm)

    assert state == "OUTCOME_UNPROVEN"
    assert detail == "PAIR_CANCEL_SIDE_UNRESOLVED"
    assert broker.mutations == []


def test_pair_cancel_returns_outcome_unproven_on_import_error(monkeypatch):
    monkeypatch.setitem(sys.modules, "ap.signal_pair_manager", None)
    osm = _FakePairOSM()
    broker = _Broker()

    state, detail = fm._cancel_pair_opposite(_order(), broker, osm)

    assert state == "OUTCOME_UNPROVEN"
    assert detail == "PAIR_MANAGER_IMPORT_UNAVAILABLE"


def test_pair_cancel_returns_outcome_unproven_on_manager_exception(monkeypatch):
    _install_fake_pair_manager(monkeypatch, raise_on_get=RuntimeError("manager broken"))
    osm = _FakePairOSM()
    broker = _Broker()
    monkeypatch.setattr(fm, "audit", lambda *a, **k: None)

    state, detail = fm._cancel_pair_opposite(_order(), broker, osm)

    assert state == "OUTCOME_UNPROVEN"
    assert detail.startswith("PAIR_CANCEL_MANAGER_FAILED")


def test_pair_cancel_returns_outcome_unproven_without_osm():
    state, detail = fm._cancel_pair_opposite(_order(), _Broker(), None)
    assert state == "OUTCOME_UNPROVEN"


# --- Crash-matrix requirement 1: crash after OSM FILLED but before pair
#     cancellation resolves -> restart must not cancel and must not COMPLETE.


def test_crash_before_pair_cancel_resolution_holds_on_restart(monkeypatch):
    # The fresh-fill path never got far enough to write ANY pair-resolution
    # marker before crashing -- meta shows IN_PROGRESS handoff with no
    # filled_entry_pair_resolution_state key at all, exactly like a process
    # that died right after OSM FILLED committed.
    order = _order(
        position_id=None,
        meta={"filled_entry_handoff_state": "IN_PROGRESS"},
    )
    calls = []
    bind_calls = []
    seed_calls = []
    monkeypatch.setattr(fm, "_persist_filled_entry_handoff_state", _memory_persist(calls))
    monkeypatch.setattr(
        fm,
        "_cancel_pair_opposite",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("recovery must not cancel pair")),
    )
    monkeypatch.setattr(
        fm,
        "_open_position_safe",
        lambda *a, **k: _PM().open_position(
            client_id=CLIENT,
            execution_mode=LIVE,
            contract=CONTRACT,
            qty=1,
            local_order_id=LOCAL_ID,
            broker_order_id=BROKER_ID,
        ),
    )
    monkeypatch.setattr(
        fm,
        "_bind_filled_entry_durable_identity",
        lambda **kwargs: bind_calls.append(kwargs["position_id"]) or (True, "BOUND"),
    )
    monkeypatch.setattr(
        fm,
        "_seed_exit_engine",
        lambda *args, **kwargs: seed_calls.append(args[1]) or (True, "SEEDED"),
    )
    monkeypatch.setattr(
        fm,
        "_release_entry_guards_once",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not release guards")),
    )

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
    assert result["reason_code"] == "FILLED_ENTRY_PAIR_RESOLUTION_UNPROVEN"
    assert result["canonical_owner_proven"] is True
    assert result["position_id"] == "position-1"
    assert bind_calls == ["position-1"]
    assert seed_calls == ["position-1"]
    assert order["position_id"] == "position-1"
    assert "COMPLETE" not in [state for state, _ in calls]


# --- Crash-matrix requirement 2: broker cancel may have happened but local
#     confirmation never landed -> restart must not repeat the cancel and
#     must hold.


def test_crash_after_ambiguous_pair_cancel_holds_without_repeating_cancel(monkeypatch):
    order = _order(
        position_id=POSITION_ID,
        meta={
            "filled_entry_handoff_state": "IN_PROGRESS",
            "filled_entry_pair_resolution_state": "OUTCOME_UNPROVEN",
            "filled_entry_pair_resolution_detail": "PAIR_CANCEL_LOCAL_TRANSITION_UNCONFIRMED",
        },
    )
    pm = _PM([_existing_position()])
    calls = []
    monkeypatch.setattr(fm, "_persist_filled_entry_handoff_state", _memory_persist(calls))
    monkeypatch.setattr(
        fm,
        "_cancel_pair_opposite",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("recovery must not cancel pair")),
    )
    monkeypatch.setattr(fm, "_bind_filled_entry_durable_identity", lambda **kwargs: (True, "BOUND"))
    monkeypatch.setattr(fm, "_seed_exit_engine", lambda *args, **kwargs: (True, "SEEDED"))
    monkeypatch.setattr(
        fm,
        "_release_entry_guards_once",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not release guards")),
    )

    result = fm.recover_interrupted_filled_entry_handoff(
        broker=_Broker([_broker_position()]),
        order=order,
        pm=pm,
        exit_engine=SimpleNamespace(execution_mode=LIVE),
        runtime_execution_mode=LIVE,
        expected_client_id=CLIENT,
        broker_positions=[_broker_position()],
    )

    assert result["disposition"] == "HOLD"
    assert result["reason_code"] == "FILLED_ENTRY_PAIR_RESOLUTION_UNPROVEN"
    assert result["canonical_owner_proven"] is True
    assert result["position_id"] == POSITION_ID
    assert "COMPLETE" not in [state for state, _ in calls]


# --- Crash-matrix requirement 3: fresh fill with no applicable pair
#     durably records NOT_APPLICABLE and may complete normally.


def test_fresh_fill_no_pair_persists_not_applicable_and_completes(monkeypatch):
    order = _order(status="ACKNOWLEDGED", filled_qty=0, meta={})
    osm = _OSM()
    persisted = []
    monkeypatch.setattr(
        fm, "check_order_with_broker",
        lambda *_a, **_k: {"status": "FILLED", "filled_qty": 1, "avg_fill": 1.58, "raw": {}},
    )
    monkeypatch.setattr(fm, "audit", lambda *a, **k: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *a, **k: None)
    monkeypatch.setattr(fm, "trace_gate", lambda *a, **k: None)
    monkeypatch.setattr(fm, "_cancel_pair_opposite", lambda *a, **k: ("NOT_APPLICABLE", "no_opposite_pair"))
    monkeypatch.setattr(fm, "_open_position_safe", lambda *a, **k: POSITION_ID)
    monkeypatch.setattr(fm, "_place_standing_stop_best_effort", lambda **k: None)
    monkeypatch.setattr(fm, "_bind_filled_entry_durable_identity", lambda **k: (True, "BOUND"))
    monkeypatch.setattr(fm, "_seed_exit_engine", lambda *a, **k: (True, "SEEDED"))
    monkeypatch.setattr(fm, "_release_entry_guards_atomically", lambda *a, **k: True)

    def persist(order_arg, state, **kwargs):
        persisted.append((state, kwargs.get("extra_meta")))
        meta = dict(order_arg.get("meta") or {})
        meta["filled_entry_handoff_state"] = state if state != "COMPLETE" else "COMPLETE"
        if kwargs.get("extra_meta"):
            meta.update(kwargs["extra_meta"])
        order_arg["meta"] = meta
        return True

    monkeypatch.setattr(fm, "_persist_filled_entry_handoff_state", persist)

    fm.process_pending_order(
        _Broker(), order, osm=osm, pm=_PM(), exit_engine=SimpleNamespace(execution_mode=LIVE),
        runtime_execution_mode=LIVE,
    )

    pair_writes = [p for _, p in persisted if p and "filled_entry_pair_resolution_state" in p]
    assert pair_writes and pair_writes[0]["filled_entry_pair_resolution_state"] == "NOT_APPLICABLE"
    assert order["meta"]["filled_entry_handoff_state"] == "COMPLETE"


# --- Crash-matrix requirement 4: fresh fill with confirmed opposite
#     cancellation durably records CONFIRMED and may complete normally.


def test_fresh_fill_confirmed_pair_cancel_persists_confirmed_and_completes(monkeypatch):
    order = _order(status="ACKNOWLEDGED", filled_qty=0, meta={})
    osm = _OSM()
    persisted = []
    monkeypatch.setattr(
        fm, "check_order_with_broker",
        lambda *_a, **_k: {"status": "FILLED", "filled_qty": 1, "avg_fill": 1.58, "raw": {}},
    )
    monkeypatch.setattr(fm, "audit", lambda *a, **k: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *a, **k: None)
    monkeypatch.setattr(fm, "trace_gate", lambda *a, **k: None)
    monkeypatch.setattr(fm, "_cancel_pair_opposite", lambda *a, **k: ("CONFIRMED", "PAIR_CANCEL_CONFIRMED"))
    monkeypatch.setattr(fm, "_open_position_safe", lambda *a, **k: POSITION_ID)
    monkeypatch.setattr(fm, "_place_standing_stop_best_effort", lambda **k: None)
    monkeypatch.setattr(fm, "_bind_filled_entry_durable_identity", lambda **k: (True, "BOUND"))
    monkeypatch.setattr(fm, "_seed_exit_engine", lambda *a, **k: (True, "SEEDED"))
    monkeypatch.setattr(fm, "_release_entry_guards_atomically", lambda *a, **k: True)

    def persist(order_arg, state, **kwargs):
        persisted.append((state, kwargs.get("extra_meta")))
        meta = dict(order_arg.get("meta") or {})
        meta["filled_entry_handoff_state"] = state
        if kwargs.get("extra_meta"):
            meta.update(kwargs["extra_meta"])
        order_arg["meta"] = meta
        return True

    monkeypatch.setattr(fm, "_persist_filled_entry_handoff_state", persist)

    fm.process_pending_order(
        _Broker(), order, osm=osm, pm=_PM(), exit_engine=SimpleNamespace(execution_mode=LIVE),
        runtime_execution_mode=LIVE,
    )

    pair_writes = [p for _, p in persisted if p and "filled_entry_pair_resolution_state" in p]
    assert pair_writes and pair_writes[0]["filled_entry_pair_resolution_state"] == "CONFIRMED"
    assert order["meta"]["filled_entry_handoff_state"] == "COMPLETE"


def test_fresh_fill_unproven_pair_keeps_guards_after_owner_seed(monkeypatch):
    order = _order(status="ACKNOWLEDGED", filled_qty=0, meta={})
    osm = _OSM()
    events = []
    persisted = []
    guard_calls = []
    monkeypatch.setattr(
        fm, "check_order_with_broker",
        lambda *_a, **_k: {"status": "FILLED", "filled_qty": 1, "avg_fill": 1.58, "raw": {}},
    )
    monkeypatch.setattr(fm, "audit", lambda *a, **k: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *a, **k: None)
    monkeypatch.setattr(fm, "trace_gate", lambda *a, **k: None)
    monkeypatch.setattr(
        fm, "_cancel_pair_opposite", lambda *a, **k: ("OUTCOME_UNPROVEN", "cancel_not_confirmed")
    )
    monkeypatch.setattr(fm, "_open_position_safe", lambda *a, **k: events.append("position") or POSITION_ID)
    monkeypatch.setattr(fm, "_place_standing_stop_best_effort", lambda **k: events.append("stop"))
    monkeypatch.setattr(
        fm,
        "_bind_filled_entry_durable_identity",
        lambda **k: events.append("bind") or (True, "BOUND"),
    )
    monkeypatch.setattr(
        fm, "_seed_exit_engine", lambda *a, **k: events.append("seed") or (True, "SEEDED")
    )
    monkeypatch.setattr(
        fm,
        "_release_entry_guards_once",
        lambda *a, **k: guard_calls.append(True) or True,
    )

    def persist(order_arg, state, **kwargs):
        persisted.append((state, kwargs.get("extra_meta"), kwargs.get("reason")))
        meta = dict(order_arg.get("meta") or {})
        meta["filled_entry_handoff_state"] = state
        if kwargs.get("extra_meta"):
            meta.update(kwargs["extra_meta"])
        order_arg["meta"] = meta
        if kwargs.get("position_id"):
            order_arg["position_id"] = kwargs["position_id"]
        return True

    monkeypatch.setattr(fm, "_persist_filled_entry_handoff_state", persist)

    fm.process_pending_order(
        _Broker(), order, osm=osm, pm=_PM(), exit_engine=SimpleNamespace(execution_mode=LIVE),
        runtime_execution_mode=LIVE,
    )

    assert events == ["position", "stop", "bind", "seed"]
    assert order["position_id"] == POSITION_ID
    assert order["meta"]["filled_entry_pair_resolution_state"] == "OUTCOME_UNPROVEN"
    assert order["meta"]["filled_entry_handoff_state"] == "HOLD"
    assert guard_calls == []
    assert "COMPLETE" not in [state for state, _, _ in persisted]
    assert any(
        state == "HOLD" and reason == "FILLED_ENTRY_PAIR_RESOLUTION_UNPROVEN"
        for state, _, reason in persisted
    )


def test_fresh_fill_pair_state_write_failure_keeps_guards_after_owner_seed(monkeypatch):
    order = _order(status="ACKNOWLEDGED", filled_qty=0, meta={})
    osm = _OSM()
    events = []
    persisted = []
    guard_calls = []
    monkeypatch.setattr(
        fm, "check_order_with_broker",
        lambda *_a, **_k: {"status": "FILLED", "filled_qty": 1, "avg_fill": 1.58, "raw": {}},
    )
    monkeypatch.setattr(fm, "audit", lambda *a, **k: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *a, **k: None)
    monkeypatch.setattr(fm, "trace_gate", lambda *a, **k: None)
    monkeypatch.setattr(
        fm, "_cancel_pair_opposite", lambda *a, **k: ("CONFIRMED", "cancel_confirmed")
    )
    monkeypatch.setattr(fm, "_persist_filled_entry_pair_resolution_state", lambda *a, **k: False)
    monkeypatch.setattr(fm, "_open_position_safe", lambda *a, **k: events.append("position") or POSITION_ID)
    monkeypatch.setattr(fm, "_place_standing_stop_best_effort", lambda **k: events.append("stop"))
    monkeypatch.setattr(
        fm,
        "_bind_filled_entry_durable_identity",
        lambda **k: events.append("bind") or (True, "BOUND"),
    )
    monkeypatch.setattr(
        fm, "_seed_exit_engine", lambda *a, **k: events.append("seed") or (True, "SEEDED")
    )
    monkeypatch.setattr(
        fm,
        "_release_entry_guards_once",
        lambda *a, **k: guard_calls.append(True) or True,
    )

    def persist(order_arg, state, **kwargs):
        persisted.append((state, kwargs.get("extra_meta"), kwargs.get("reason")))
        meta = dict(order_arg.get("meta") or {})
        meta["filled_entry_handoff_state"] = state
        if kwargs.get("extra_meta"):
            meta.update(kwargs["extra_meta"])
        order_arg["meta"] = meta
        if kwargs.get("position_id"):
            order_arg["position_id"] = kwargs["position_id"]
        return True

    monkeypatch.setattr(fm, "_persist_filled_entry_handoff_state", persist)

    fm.process_pending_order(
        _Broker(), order, osm=osm, pm=_PM(), exit_engine=SimpleNamespace(execution_mode=LIVE),
        runtime_execution_mode=LIVE,
    )

    assert events == ["position", "stop", "bind", "seed"]
    assert order["position_id"] == POSITION_ID
    assert "filled_entry_pair_resolution_state" not in order["meta"]
    assert order["meta"]["filled_entry_handoff_state"] == "HOLD"
    assert guard_calls == []
    assert "COMPLETE" not in [state for state, _, _ in persisted]
    assert any(
        state == "HOLD" and reason == "FILLED_ENTRY_PAIR_RESOLUTION_UNPROVEN"
        for state, _, reason in persisted
    )


# --- Crash-matrix requirements 5 & 6: terminal recovery with CONFIRMED or
#     NOT_APPLICABLE takes zero pair-cancel calls and may complete.


@pytest.mark.parametrize("pair_state", ["CONFIRMED", "NOT_APPLICABLE"])
def test_terminal_recovery_completes_when_pair_resolution_proven(monkeypatch, pair_state):
    order = _order(
        position_id=POSITION_ID,
        meta={
            "filled_entry_handoff_state": "IN_PROGRESS",
            "filled_entry_pair_resolution_state": pair_state,
        },
    )
    pm = _PM([_existing_position()])
    broker = _Broker([_broker_position()])
    calls = []
    release_calls = []
    monkeypatch.setattr(fm, "_persist_filled_entry_handoff_state", _memory_persist(calls))
    monkeypatch.setattr(fm, "_bind_filled_entry_durable_identity", lambda **kwargs: (True, "BOUND"))
    monkeypatch.setattr(fm, "_seed_exit_engine", lambda *args, **kwargs: (True, "SEEDED"))
    monkeypatch.setattr(
        fm,
        "_release_entry_guards_atomically",
        lambda *args, **kwargs: release_calls.append(True) or True,
    )
    monkeypatch.setattr(
        fm,
        "_cancel_pair_opposite",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("recovery must not cancel pair")),
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
    assert release_calls == [True]
    assert broker.mutations == []
    assert "COMPLETE" in [state for state, _ in calls]


# --- Crash-matrix requirement 7: terminal recovery with OUTCOME_UNPROVEN
#     takes zero pair-cancel calls, holds, and never completes.


def test_terminal_recovery_holds_when_pair_resolution_unproven(monkeypatch):
    order = _order(
        position_id=POSITION_ID,
        meta={
            "filled_entry_handoff_state": "IN_PROGRESS",
            "filled_entry_pair_resolution_state": "OUTCOME_UNPROVEN",
        },
    )
    pm = _PM([_existing_position()])
    broker = _Broker([_broker_position()])
    calls = []
    bind_calls = []
    seed_calls = []
    monkeypatch.setattr(fm, "_persist_filled_entry_handoff_state", _memory_persist(calls))
    monkeypatch.setattr(
        fm,
        "_bind_filled_entry_durable_identity",
        lambda **kwargs: bind_calls.append(kwargs["position_id"]) or (True, "BOUND"),
    )
    monkeypatch.setattr(
        fm,
        "_seed_exit_engine",
        lambda *args, **kwargs: seed_calls.append(args[1]) or (True, "SEEDED"),
    )
    monkeypatch.setattr(
        fm,
        "_release_entry_guards_once",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not release guards")),
    )
    monkeypatch.setattr(
        fm,
        "_cancel_pair_opposite",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("recovery must not cancel pair")),
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

    assert result["disposition"] == "HOLD"
    assert result["reason_code"] == "FILLED_ENTRY_PAIR_RESOLUTION_UNPROVEN"
    assert result["canonical_owner_proven"] is True
    assert result["position_id"] == POSITION_ID
    assert bind_calls == [POSITION_ID]
    assert seed_calls == [POSITION_ID]
    assert broker.mutations == []
    assert "COMPLETE" not in [state for state, _ in calls]


# --- Crash-matrix requirement 8: malformed/ambiguous pair-resolution truth
#     (unexpected string, wrong type) fails closed exactly like a missing
#     value -- never treated as proof.


@pytest.mark.parametrize(
    "malformed_value",
    ["", "confirmed", "NOT-APPLICABLE", "CONFIRMED ", None, 1, True, ["CONFIRMED"]],
)
def test_malformed_pair_resolution_truth_fails_closed(monkeypatch, malformed_value):
    order = _order(
        position_id=POSITION_ID,
        meta={
            "filled_entry_handoff_state": "IN_PROGRESS",
            "filled_entry_pair_resolution_state": malformed_value,
        },
    )
    pm = _PM([_existing_position()])
    broker = _Broker([_broker_position()])
    calls = []
    bind_calls = []
    seed_calls = []
    monkeypatch.setattr(fm, "_persist_filled_entry_handoff_state", _memory_persist(calls))
    monkeypatch.setattr(
        fm,
        "_bind_filled_entry_durable_identity",
        lambda **kwargs: bind_calls.append(kwargs["position_id"]) or (True, "BOUND"),
    )
    monkeypatch.setattr(
        fm,
        "_seed_exit_engine",
        lambda *args, **kwargs: seed_calls.append(args[1]) or (True, "SEEDED"),
    )
    monkeypatch.setattr(
        fm,
        "_release_entry_guards_once",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not release guards")),
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

    assert result["disposition"] == "HOLD"
    assert result["reason_code"] == "FILLED_ENTRY_PAIR_RESOLUTION_UNPROVEN"
    assert result["canonical_owner_proven"] is True
    assert result["position_id"] == POSITION_ID
    assert bind_calls == [POSITION_ID]
    assert seed_calls == [POSITION_ID]
    assert "COMPLETE" not in [state for state, _ in calls]


def test_complete_write_sql_requires_pair_resolution_proof():
    source = inspect.getsource(fm._persist_filled_entry_handoff_state)
    assert (
        "COALESCE(meta->>'filled_entry_pair_resolution_state','') "
        in source
    )
    assert "IN ('NOT_APPLICABLE','CONFIRMED')" in source


def test_recovery_never_calls_cancel_pair_opposite_statically():
    source = inspect.getsource(fm.recover_interrupted_filled_entry_handoff)
    tree = ast.parse(source)
    call_names = {
        node.func.attr if isinstance(node.func, ast.Attribute) else node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, (ast.Attribute, ast.Name))
    }
    assert "_cancel_pair_opposite" not in call_names


# =============================================================================
# SCOPE CORRECTION — EXIT terminalization semantics must be unchanged
# =============================================================================


def test_check_order_with_broker_no_longer_forces_exit_error_on_malformed_price():
    source = inspect.getsource(fm.check_order_with_broker)
    assert "BROKER_FILLED_TRUTH_INVALID" not in source


def test_exit_zero_qty_still_blocked_by_pre_existing_pr235_guard():
    source = inspect.getsource(fm.check_order_with_broker)
    assert "PR #235 (hardening #3)" in source
    assert "EXIT_FILLED" in source.split("PR #235 (hardening #3)")[1][:400]


def test_entry_admission_boundary_still_enforces_strict_qty_and_price():
    source = inspect.getsource(fm._validate_filled_entry_admission)
    assert "_strict_positive_integral(result.get(\"filled_qty\"))" in source
    assert "_strict_positive_finite(result.get(\"avg_fill\"))" in source


# =============================================================================
# WINDOW E — behavioral owner-rehydration proof, not only startup ordering
# =============================================================================


def test_seed_from_db_rehydrates_exactly_one_owner_for_complete_position(monkeypatch):
    """A COMPLETE canonical position must have exactly one behavior-active
    exit-engine owner after a full process restart -- not just the right
    startup call order.  This builds one exact active DB position row (as
    a COMPLETE filled-entry handoff would have left it), rehydrates a
    fresh exit engine from it, and asserts identity, cardinality, and zero
    broker mutation authority.
    """
    import ap_exit_engine

    position_row = {
        "id": POSITION_ID,
        "client_id": CLIENT,
        "execution_mode": LIVE,
        "contract": CONTRACT,
        "symbol": "PEP",
        "underlying": "PEP",
        "direction": "CALL",
        "qty": 1,
        "quantity_remaining": 1,
        "avg_fill": 1.58,
        "underlying_entry": 141.0,
        "target_underlying": 145.0,
        "stop_underlying": 138.0,
        "local_order_id": LOCAL_ID,
        "broker_order_id": BROKER_ID,
        "signal_id": LOCAL_ID,
        "scale_outs_done": 0,
        "meta": {},
    }
    pm = _PM([position_row])

    class _MutationTrapBroker:
        def submit_order(self, **kwargs):
            raise AssertionError("rehydration must not submit")

        def cancel_order(self, *a, **kwargs):
            raise AssertionError("rehydration must not cancel")

        def place_stop_order(self, **kwargs):
            raise AssertionError("rehydration must not place a stop")

    engine = ap_exit_engine.APExitEngine(broker=_MutationTrapBroker())

    engine.seed_from_db(pm)

    owners = [
        mp for mp in engine._positions if str(mp.position_id) == POSITION_ID
    ]
    assert len(owners) == 1
    owner = owners[0]
    assert owner.client_id == CLIENT
    assert owner.execution_mode == LIVE
    assert owner.option_symbol == CONTRACT
    assert owner.position_id == POSITION_ID
    assert engine._positions_by_id.get(POSITION_ID) is owner


# =============================================================================
# P0 REGRESSION — Malformed broker positions_node must HOLD, never NONACTIONABLE
# Audit finding: {"positions": {"unexpected": "shape"}} was silently returning []
# which caused the recovery authority to emit NONACTIONABLE / NO_CURRENT_BROKER_POSITION,
# destroying Jason's real LIVE exposure with no retry possible.
# =============================================================================


def test_malformed_positions_node_raises_not_returns_empty():
    """_normalize_positions_payload must raise on {"position" key absent} node.

    A broker HTTP 200 with a structurally valid outer dict but an unexpected
    positions_node shape (no "position" key, non-empty) is uninterpretable
    broker truth.  It must raise ValueError so the caller can route to HOLD,
    not silently return [] (flat account) which collapses to NONACTIONABLE.
    """
    from ap.filled_entry_recovery_authority import _normalize_positions_payload

    # Known-empty authoritative shapes must still return [] (not regressed).
    assert _normalize_positions_payload({"positions": None}) == []
    assert _normalize_positions_payload({"positions": "null"}) == []
    assert _normalize_positions_payload({"positions": {}}) == []

    # Malformed non-empty dict without "position" key must raise.
    with pytest.raises(ValueError, match="FILLED_ENTRY_RECOVERY_BROKER_POSITION_NODE_MISSING"):
        _normalize_positions_payload({"positions": {"unexpected": "shape"}})

    with pytest.raises(ValueError, match="FILLED_ENTRY_RECOVERY_BROKER_POSITION_NODE_MISSING"):
        _normalize_positions_payload({"positions": {"account": "123", "other": "data"}})


def test_malformed_positions_node_holds_not_nonactionable_end_to_end():
    """Broker HTTP success with malformed positions_node must produce HOLD/retryable.

    This is the full production chain:
      Tradier HTTP 200 → payload {"positions": {"unexpected": "shape"}}
      → _normalize_positions_payload raises
      → fetch_current_broker_positions raises
      → evaluate_filled_entry_recovery_authority catches → HOLD retryable=True
      → NOT NONACTIONABLE (which would be no retry, no reconstruction)
    """

    class _MalformedBroker:
        cfg = SimpleNamespace(account_id="123456789")

        def _get(self, path):
            # HTTP 200 but structurally ambiguous — not a known-empty shape.
            return {"positions": {"unexpected": "shape"}}

    result = evaluate_filled_entry_recovery_authority(
        pm=_PM(),
        broker=_MalformedBroker(),
        order=_order(today=None),
        runtime_execution_mode=LIVE,
        expected_client_id=CLIENT,
        today=date(2026, 8, 21),
    )

    # Must HOLD and be retryable — Jason's real LIVE fill must remain recoverable.
    assert result["disposition"] == "HOLD", (
        f"Expected HOLD got {result['disposition']!r}: {result}"
    )
    assert result.get("retryable") is True, (
        f"Must be retryable so recovery can retry: {result}"
    )
    # Explicit guard: must never reach NONACTIONABLE on uninterpretable broker truth.
    assert result["disposition"] != "NONACTIONABLE", (
        "Malformed broker payload must never collapse to NONACTIONABLE — "
        "that would destroy position/owner reconstruction authority with no retry."
    )


# =============================================================================
# P0 REGRESSION — Terminal FILLED must prove filled_qty == order.qty
# Audit finding: admission proved filled_qty > 0 but not filled_qty == order.qty,
# allowing a broker payload of status=FILLED / filled_qty=1 on a 2-contract order
# to pass admission, write position qty=1, and release reservation for 2 contracts.
# =============================================================================


def test_admission_rejects_terminal_underfill_quantity_conflict():
    """_validate_filled_entry_admission must block filled_qty != order.qty.

    A broker FILLED with filled_qty=1 on an order.qty=2 is a terminal
    quantity contradiction.  The admission gate must reject it before
    IN_PROGRESS, before OSM FILLED, before position creation, before guard
    release.  No partial truth must become durable terminal state.
    """
    ok, reason = fm._validate_filled_entry_admission(
        order=_order(qty=2, filled_qty=2),
        result={"filled_qty": 1, "avg_fill": 1.58},
        runtime_execution_mode=LIVE,
    )
    assert not ok, f"Underfill should be rejected but got ok=True, reason={reason!r}"
    assert reason == "FILLED_ENTRY_TERMINAL_QUANTITY_CONFLICT", (
        f"Expected FILLED_ENTRY_TERMINAL_QUANTITY_CONFLICT, got {reason!r}"
    )


def test_admission_rejects_overfill_after_clamp_still_disagrees():
    """filled_qty > order.qty after clamp bypass must also be rejected."""
    ok, reason = fm._validate_filled_entry_admission(
        order=_order(qty=1, filled_qty=1),
        result={"filled_qty": 3, "avg_fill": 1.58},
        runtime_execution_mode=LIVE,
    )
    assert not ok
    assert reason == "FILLED_ENTRY_TERMINAL_QUANTITY_CONFLICT"


def test_admission_rejects_missing_requested_qty():
    """An order with no parseable qty must be rejected before terminalization."""
    ok, reason = fm._validate_filled_entry_admission(
        order=_order(qty=None),
        result={"filled_qty": 1, "avg_fill": 1.58},
        runtime_execution_mode=LIVE,
    )
    assert not ok
    assert reason == "FILLED_ENTRY_REQUESTED_QUANTITY_UNPROVEN"


def test_admission_passes_when_filled_qty_matches_order_qty():
    """Happy path: exact quantity agreement must still admit cleanly."""
    ok, reason = fm._validate_filled_entry_admission(
        order=_order(qty=2, filled_qty=2),
        result={"filled_qty": 2, "avg_fill": 1.58},
        runtime_execution_mode=LIVE,
    )
    assert ok, f"Exact quantity match should admit but got reason={reason!r}"
    assert reason == "FILLED_ENTRY_ADMISSION_PROVEN"


def test_fresh_fill_terminal_underfill_blocked_before_any_mutation(monkeypatch):
    """End-to-end: broker FILLED with filled_qty=1 on qty=2 order must HOLD.

    This tests the full process_pending_order path to prove that the terminal
    underfill is blocked before IN_PROGRESS marker, before OSM transition,
    before position creation, and before guard release.
    """
    # Fresh 2-contract order that has not yet had a fill written locally.
    order = _order(qty=2, filled_qty=0, status="PENDING")
    order["meta"] = {
        "filled_entry_pair_resolution_state": "NOT_APPLICABLE",
    }

    # Broker says FILLED but only confirms 1 of 2 requested contracts.
    broker_result = {
        "status": "FILLED",
        "filled_qty": 1,   # ← underfill: only 1 of 2 contracts
        "avg_fill": 1.58,
        "broker_order_id": BROKER_ID,
    }

    # Intercept the broker check so we control the result.
    monkeypatch.setattr(fm, "check_order_with_broker", lambda broker, order: broker_result)

    in_progress_written = []
    osm_transitioned = []
    position_created = []
    guards_released = []

    monkeypatch.setattr(
        fm,
        "_persist_filled_entry_handoff_state",
        lambda order, state, **kw: in_progress_written.append(state) or True,
    )

    class _TrapOSM:
        def transition(self, *a, **kw):
            osm_transitioned.append((a, kw))
            return True

        def increment_retry(self, *a):
            pass

    class _TrapBroker:
        pass

    monkeypatch.setattr(
        fm,
        "_release_entry_guards_atomically",
        lambda *a, **kw: guards_released.append(True) or True,
    )

    monkeypatch.setattr(fm, "_resolve_runtime_execution_mode", lambda **kw: LIVE)

    fill_events = []

    def _capture_emit(order, *, decision, reason_code, **kw):
        fill_events.append({"decision": decision, "reason_code": reason_code})

    monkeypatch.setattr(fm, "emit_fill_event", _capture_emit)
    monkeypatch.setattr(fm, "_record_position_create_failure", lambda *a, **kw: None)
    monkeypatch.setattr(fm, "_reset_broker_anomaly_count", lambda *a, **kw: None)

    fm.process_pending_order(
        broker=_TrapBroker(),
        order=order,
        osm=_TrapOSM(),
        pm=_PM(),
        exit_engine=None,
        runtime_execution_mode=LIVE,
    )

    # No IN_PROGRESS marker may have been written.
    assert not in_progress_written, (
        f"IN_PROGRESS must not be written for terminal underfill: {in_progress_written}"
    )
    # OSM must not have been told FILLED.
    assert not osm_transitioned, (
        f"OSM must not transition on terminal underfill: {osm_transitioned}"
    )
    # Guards must not be released.
    assert not guards_released, (
        f"Guards must not be released on terminal underfill: {guards_released}"
    )
    # The fill event must reflect the block.
    assert fill_events, "A fill event must be emitted to explain the block"
    assert fill_events[-1]["decision"] == "HOLD", (
        f"Expected HOLD fill event, got: {fill_events[-1]}"
    )
    assert (
        "TERMINAL_QUANTITY_CONFLICT" in fill_events[-1]["reason_code"]
        or "REQUESTED_QUANTITY" in fill_events[-1]["reason_code"]
    ), f"Reason code must surface quantity conflict: {fill_events[-1]}"
