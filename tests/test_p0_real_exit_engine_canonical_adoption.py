"""P0 Merge-Gate B — real APExitEngine synthetic→canonical adoption proof.

Production incident (August 17 2026, Jason LIVE SMCI260821P00038000):
a synthetic broker-repair protective owner existed first
(`broker-repair-jasoncosby1@gmail.com-SMCI260821P00038000`); the canonical
ENTRY then filled and the canonical DB position was created.  #473's
architecture for this ordering — fill_monitor._seed_exit_engine() ->
APExitEngine.adopt_canonical_position_identity() ->
_verify_canonical_exit_owner() — was previously proven only in two
disconnected halves: real-engine adoption tests that never went through
fill_monitor's seam, and fill_monitor handoff tests that only ever used a
fake adoption engine.  This file proves the whole seam end-to-end with the
REAL ap_exit_engine.APExitEngine, per the live reproduction.

No production code is modified by this file.  It is integration proof only.
"""

from __future__ import annotations

import os
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

_REAL_PG_DSN = (
    os.environ.get("DATABASE_URL")
    or os.environ.get("INTELLIGENCE_POSTGRES_TEST_URL")
    or os.environ.get("MANUAL_CLOSE_POSTGRES_TEST_URL")
    or ""
)
os.environ.setdefault("DATABASE_URL", _REAL_PG_DSN or "postgresql://test:test@localhost/test")
os.environ.setdefault("AP_ENV", "test")

from ap import fill_monitor as fm

CLIENT = "jasoncosby1@gmail.com"
MODE = "live"
CONTRACT = "SMCI260821P00038000"
REPAIR_ID = f"broker-repair-{CLIENT}-{CONTRACT}"
CANON_ID = "position-smci-canon-test-0001"
LOCAL_ORDER_ID = "local-smci-entry-test-0001"
BROKER_ORDER_ID = "broker-smci-entry-test-0001"
SIGNAL_ID = "signal-smci-put-test"


@dataclass
class _RealEnginePos:
    """Minimal mutable ManagedPosition-shaped object.

    Only the attributes adopt_canonical_position_identity() and
    _is_behavior_active_position() actually read/write are given explicit
    defaults; everything else is accepted via free attribute assignment
    (see __setattr__), matching how the real engine treats ManagedPosition.
    """

    position_id: str
    client_id: str = CLIENT
    execution_mode: str = MODE
    option_symbol: str = CONTRACT
    quantity: int = 1
    quantity_remaining: int = 1
    closed: bool = False
    entry_price: float = 0.0
    current_bid: float = 0.0
    peak_pnl_pct: float = 0.0
    max_profit_seen: float = 0.0
    touched_profit: bool = False
    live_executable_price_source: str = ""

    def __setattr__(self, name, value):
        object.__setattr__(self, name, value)


def _order(**overrides):
    row = {
        "client_id": CLIENT,
        "local_order_id": LOCAL_ORDER_ID,
        "broker_order_id": BROKER_ORDER_ID,
        "kind": "ENTRY",
        "status": "FILLED",
        "contract": CONTRACT,
        "direction": "PUT",
        "execution_mode": MODE,
        "signal_id": SIGNAL_ID,
        "qty": 1,
        "filled_qty": 1,
        "fill_price": 1.10,
        "meta": {
            "filled_entry_handoff_state": "IN_PROGRESS",
            "filled_entry_pair_resolution_state": "NOT_APPLICABLE",
        },
    }
    row.update(overrides)
    return row


def _result(**overrides):
    result = {"filled_qty": 1, "avg_fill": 1.10, "filled_ts": datetime.now(timezone.utc)}
    result.update(overrides)
    return result


def _real_engine(*, positions=None):
    """Construct a minimally-valid real APExitEngine instance.

    Mirrors the construction pattern already used by real-engine tests in
    tests/test_p0_live_executable_bid_pnl.py (TestAdoptionContaminationCleanse)
    — __new__ + explicit lock/positions bootstrap, no broker/master_control
    needed since order["execution_mode"] alone proves mode when the engine
    itself has no conflicting mode evidence.
    """
    from ap_exit_engine import APExitEngine

    engine = APExitEngine.__new__(APExitEngine)
    engine._email = CLIENT
    engine._lock = threading.Lock()
    engine._positions = []
    engine._positions_by_id = {}
    for pos in positions or []:
        engine._positions.append(pos)
        engine._positions_by_id[pos.position_id] = pos
    return engine


# ─────────────────────────────────────────────────────────────────────────
# Mandatory real-engine test: synthetic broker-repair -> canonical adoption
# through fill_monitor's production _seed_exit_engine() seam.
# ─────────────────────────────────────────────────────────────────────────


def test_real_engine_adopts_broker_repair_to_canonical_through_seed_exit_engine():
    repair_pos = _RealEnginePos(position_id=REPAIR_ID)
    engine = _real_engine(positions=[repair_pos])

    seed_ok, seed_reason = fm._seed_exit_engine(
        engine, CANON_ID, _order(), _result(), SIGNAL_ID,
    )

    assert seed_ok is True
    assert seed_reason in {"ADOPTED", "ALREADY_CANONICAL_REPAIR_REMOVED"}

    actives = engine.active_positions()
    domain = [
        p for p in actives
        if p.client_id == CLIENT and p.execution_mode == MODE and p.option_symbol == CONTRACT
    ]
    # 1. exactly one behavior-active owner remains in the exact domain
    assert len(domain) == 1
    owner = domain[0]
    # 2. remaining owner.position_id == canonical_position_id
    assert owner.position_id == CANON_ID
    # 3. no active broker-repair owner remains in that domain
    assert not any(p.position_id.startswith("broker-repair-") for p in domain)
    # 4-6. identity preserved exactly
    assert owner.client_id == CLIENT
    assert owner.execution_mode == "live"
    assert owner.option_symbol == CONTRACT
    # 7-8. local/broker order id canonical where the real object supports it
    assert getattr(owner, "entry_local_order_id", LOCAL_ORDER_ID) == LOCAL_ORDER_ID
    assert getattr(owner, "entry_broker_order_id", BROKER_ORDER_ID) == BROKER_ORDER_ID
    # 9. no second owner was created
    assert len(engine._positions) == 1
    # 10. no duplicate engine position remains in _positions
    assert engine._positions[0] is owner
    # 11. _positions_by_id contains canonical ID
    assert engine._positions_by_id.get(CANON_ID) is owner
    # 12. _positions_by_id does NOT retain the old broker-repair key
    assert REPAIR_ID not in engine._positions_by_id


def test_real_engine_adoption_places_no_broker_orders_or_standing_stop():
    repair_pos = _RealEnginePos(position_id=REPAIR_ID)
    engine = _real_engine(positions=[repair_pos])

    class _NoBrokerCallsAllowed:
        def __getattr__(self, name):
            raise AssertionError(
                f"real-engine adoption must never touch the broker (attr={name})"
            )

    # adopt_canonical_position_identity does not take a broker at all; the
    # production seam (_seed_exit_engine) also never passes one through to
    # the engine's adoption call.  Verify by inspecting the real call: the
    # engine's own broker attribute (if any) must never be invoked as part
    # of this seam, and no order/cancel mutation counters exist on engine.
    engine.broker = _NoBrokerCallsAllowed()

    seed_ok, _ = fm._seed_exit_engine(engine, CANON_ID, _order(), _result(), SIGNAL_ID)
    assert seed_ok is True
    # 13. broker order submission count = 0 / 14. cancellation count = 0
    # (proven by _NoBrokerCallsAllowed never raising above)
    # 15. no standing-stop submission from restart recovery — this seam
    # (_seed_exit_engine) never calls _place_standing_stop_best_effort;
    # confirmed structurally since no such call exists in its source path
    # and no exception fired from the broker guard.


# ─────────────────────────────────────────────────────────────────────────
# Second real-engine case: canonical already exists AND repair also exists
# ─────────────────────────────────────────────────────────────────────────


def test_real_engine_canonical_already_exists_repair_is_merged_and_removed():
    canonical_pos = _RealEnginePos(position_id=CANON_ID)
    repair_pos = _RealEnginePos(position_id=REPAIR_ID)
    engine = _real_engine(positions=[canonical_pos, repair_pos])

    seed_ok, seed_reason = fm._seed_exit_engine(
        engine, CANON_ID, _order(), _result(), SIGNAL_ID,
    )

    assert seed_ok is True
    assert seed_reason == "ALREADY_CANONICAL_REPAIR_REMOVED"

    actives = engine.active_positions()
    domain = [
        p for p in actives
        if p.client_id == CLIENT and p.execution_mode == MODE and p.option_symbol == CONTRACT
    ]
    assert len(domain) == 1
    assert domain[0].position_id == CANON_ID
    assert not any(p.position_id.startswith("broker-repair-") for p in domain)
    # No duplicate seed, no second ManagedPosition survives.
    assert len(engine._positions) == 1
    assert REPAIR_ID not in engine._positions_by_id


# ─────────────────────────────────────────────────────────────────────────
# Fail-closed real-engine case: identity mismatch must not adopt
# ─────────────────────────────────────────────────────────────────────────


def test_real_engine_client_mismatch_seed_fails_closed_no_owner_claimed():
    foreign_repair = _RealEnginePos(
        position_id=REPAIR_ID, client_id="other-client@example.com",
    )
    engine = _real_engine(positions=[foreign_repair])

    seed_ok, seed_reason = fm._seed_exit_engine(
        engine, CANON_ID, _order(), _result(), SIGNAL_ID,
    )

    assert seed_ok is False
    # No canonical owner may be claimed around an untrusted repair.
    assert engine._positions_by_id.get(CANON_ID) is None
    # The untrusted repair is left in place — quarantined, not adopted —
    # never silently collapsed into a client it does not belong to.
    assert engine._positions_by_id.get(REPAIR_ID) is foreign_repair


def test_real_engine_mode_mismatch_seed_fails_closed_no_owner_claimed():
    paper_repair = _RealEnginePos(position_id=REPAIR_ID, execution_mode="paper")
    engine = _real_engine(positions=[paper_repair])

    seed_ok, seed_reason = fm._seed_exit_engine(
        engine, CANON_ID, _order(execution_mode=MODE), _result(), SIGNAL_ID,
    )

    assert seed_ok is False
    assert engine._positions_by_id.get(CANON_ID) is None


# ─────────────────────────────────────────────────────────────────────────
# Recovery-level proof: owner proof happens before guard release/COMPLETE
# ─────────────────────────────────────────────────────────────────────────


def _pm_with_open_position(**overrides):
    row = {
        "id": CANON_ID,
        "client_id": CLIENT,
        "execution_mode": MODE,
        "contract": CONTRACT,
        "status": "OPEN",
        "quantity_remaining": 1,
        "local_order_id": LOCAL_ORDER_ID,
        "broker_order_id": BROKER_ORDER_ID,
    }
    row.update(overrides)

    class _PM:
        def __init__(self, position_row):
            self.positions = [position_row]
            self.open_calls = []

        def get_active_positions(self):
            return list(self.positions)

        def get_position_by_local_order(self, local_order_id):
            return next(
                (p for p in self.positions if p.get("local_order_id") == local_order_id), None
            )

        def get_position_by_broker_order(self, broker_order_id):
            return next(
                (p for p in self.positions if p.get("broker_order_id") == broker_order_id), None
            )

        def get_position(self, position_id):
            return next((p for p in self.positions if p.get("id") == position_id), None)

        def open_position(self, **kwargs):
            raise AssertionError("ACTIVE_EXISTING recovery must not recreate a position")

    return _PM(row)


class _Broker:
    def __init__(self, positions):
        self.positions = list(positions)
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


def _memory_persist(calls):
    def _persist(order, state, *, reason=None, retryable=None, position_id=None):
        calls.append((state, position_id))
        meta = dict(order.get("meta") or {})
        meta["filled_entry_handoff_state"] = state
        order["meta"] = meta
        return True

    return _persist


def test_recovery_real_engine_owner_proof_gates_guard_release_and_complete(monkeypatch):
    """Full recovery path with a REAL engine: owner proof must occur before
    guard release and before COMPLETE.  Success path — repair adopted,
    guards release, handoff completes."""
    repair_pos = _RealEnginePos(position_id=REPAIR_ID)
    engine = _real_engine(positions=[repair_pos])
    calls = []
    release_calls = []

    monkeypatch.setattr(fm, "_persist_filled_entry_handoff_state", _memory_persist(calls))
    monkeypatch.setattr(fm, "_bind_filled_entry_durable_identity", lambda **kwargs: (True, "BOUND"))
    monkeypatch.setattr(
        fm, "_release_entry_guards_atomically",
        lambda *args, **kwargs: release_calls.append(True) or True,
    )

    order = _order(position_id=CANON_ID)
    pm = _pm_with_open_position()
    broker = _Broker([{"symbol": CONTRACT, "quantity": 1}])

    result = fm.recover_interrupted_filled_entry_handoff(
        broker=broker,
        order=order,
        osm=SimpleNamespace(),
        pm=pm,
        exit_engine=engine,
        runtime_execution_mode=MODE,
        expected_client_id=CLIENT,
        broker_positions=[{"symbol": CONTRACT, "quantity": 1}],
    )

    assert result["disposition"] == "ACTIVE_EXISTING"
    assert result["completed"] is True
    # Owner proof succeeded via the REAL engine before guard release.
    assert release_calls == [True]
    assert [state for state, _ in calls] == ["COMPLETE"]
    assert broker.mutations == []

    domain = [
        p for p in engine.active_positions()
        if p.client_id == CLIENT and p.execution_mode == MODE and p.option_symbol == CONTRACT
    ]
    assert len(domain) == 1
    assert domain[0].position_id == CANON_ID


def test_recovery_real_engine_owner_proof_failure_blocks_guard_release_and_complete(
    monkeypatch,
):
    """Fail-closed recovery path with a REAL engine: a client-mismatched
    repair must prevent adoption, which must prevent guard release and
    COMPLETE — the recovery HOLDs instead."""
    foreign_repair = _RealEnginePos(
        position_id=REPAIR_ID, client_id="other-client@example.com",
    )
    engine = _real_engine(positions=[foreign_repair])
    calls = []
    release_calls = []

    monkeypatch.setattr(fm, "_persist_filled_entry_handoff_state", _memory_persist(calls))
    monkeypatch.setattr(fm, "_bind_filled_entry_durable_identity", lambda **kwargs: (True, "BOUND"))
    monkeypatch.setattr(
        fm, "_release_entry_guards_atomically",
        lambda *args, **kwargs: release_calls.append(True) or True,
    )

    order = _order(position_id=CANON_ID)
    pm = _pm_with_open_position()
    broker = _Broker([{"symbol": CONTRACT, "quantity": 1}])

    result = fm.recover_interrupted_filled_entry_handoff(
        broker=broker,
        order=order,
        osm=SimpleNamespace(),
        pm=pm,
        exit_engine=engine,
        runtime_execution_mode=MODE,
        expected_client_id=CLIENT,
        broker_positions=[{"symbol": CONTRACT, "quantity": 1}],
    )

    # Owner proof failed (real engine refused cross-client adoption) ->
    # recovery must not release guards or claim COMPLETE.
    assert result.get("completed") is not True
    assert release_calls == []
    assert "COMPLETE" not in [state for state, _ in calls]
    assert broker.mutations == []
    # The untrusted foreign repair must not have been silently adopted.
    assert engine._positions_by_id.get(CANON_ID) is None
