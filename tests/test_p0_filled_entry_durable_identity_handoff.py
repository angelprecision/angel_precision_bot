"""P0 regression coverage for the filled ENTRY identity handoff.

The tests exercise the fill-monitor seam directly with a transactional in-memory
DB double.  They intentionally do not import or modify Position Manager or the
exit engine implementation: this PR's authority boundary is one fill-monitor
transaction before the existing seed/adoption call.
"""
from __future__ import annotations

import copy
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
os.environ.setdefault("AP_ENV", "test")

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from ap import fill_monitor as fm  # noqa: E402


CLIENT = "jasoncosby1@gmail.com"
MODE = "live"
CONTRACT = "PEP260821C00141000"
LOCAL_ORDER_ID = "1b8ec150-38cc-424c-93e5-b1a1820ea50b"
BROKER_ORDER_ID = "141999576"
POSITION_ID = "86ed1496-75ef-472c-8a36-94eee1698509"
SIGNAL_ID = "f42a3a99-974a-4793-b04f-9fa04ec5218f"
PLAN_ID = "720ec5fa-a14d-4eee-9816-ff33bead4de6"
RECONCILED_PLAN_ID = "reconciled:PEP260821C00141000:daeb6be995af7739a8cbcd3e"


class _MemoryCursor:
    def __init__(self, db):
        self.db = db
        self._row = None
        self.rowcount = 0

    def execute(self, sql, params=()):
        normalized = " ".join(str(sql).split())
        params = tuple(params)
        self.db.statements.append((normalized, params))
        self.rowcount = 0

        if normalized.startswith("SELECT * FROM orders"):
            client_id, local_order_id = params
            row = self.db.orders.get(local_order_id)
            self._row = (
                copy.deepcopy(row)
                if row and row.get("client_id") == client_id
                else None
            )
            return

        if normalized.startswith("SELECT * FROM positions"):
            position_id, client_id = params
            row = self.db.positions.get(position_id)
            self._row = (
                copy.deepcopy(row)
                if row and row.get("client_id") == client_id
                else None
            )
            return

        if normalized.startswith("UPDATE positions SET"):
            local_order_id = None
            broker_order_id = None
            index = 0
            if "local_order_id=%s" in normalized:
                local_order_id = params[index]
                index += 1
            if "broker_order_id=%s" in normalized:
                broker_order_id = params[index]
                index += 1
            position_id, client_id = params[index:index + 2]
            row = self.db.positions.get(position_id)
            if row and row.get("client_id") == client_id:
                if local_order_id is not None:
                    row["local_order_id"] = local_order_id
                if broker_order_id is not None:
                    row["broker_order_id"] = broker_order_id
                self.rowcount = 1
                self.db.mutations += 1
            return

        if normalized.startswith("UPDATE orders SET position_id=%s"):
            position_id, client_id, local_order_id = params
            row = self.db.orders.get(local_order_id)
            if (
                row
                and row.get("client_id") == client_id
                and (row.get("position_id") is None or str(row.get("position_id")).strip() == "")
            ):
                row["position_id"] = position_id
                self.rowcount = 1
                self.db.mutations += 1
            return

        raise AssertionError(f"unexpected SQL in focused test: {normalized}")

    def fetchone(self):
        return copy.deepcopy(self._row)


class _MemoryDB:
    def __init__(self, order, position):
        self.orders = {order["local_order_id"]: copy.deepcopy(order)}
        self.positions = {position["id"]: copy.deepcopy(position)}
        self.statements = []
        self.mutations = 0

    @contextmanager
    def conn(self):
        snapshot = (copy.deepcopy(self.orders), copy.deepcopy(self.positions), self.mutations)
        cursor = _MemoryCursor(self)
        try:
            yield cursor
        except Exception:
            self.orders, self.positions, self.mutations = snapshot
            raise


def _order(**overrides):
    row = {
        "client_id": CLIENT,
        "local_order_id": LOCAL_ORDER_ID,
        "broker_order_id": BROKER_ORDER_ID,
        "position_id": None,
        "kind": "ENTRY",
        "status": "FILLED",
        "execution_mode": MODE,
        "contract": CONTRACT,
        "signal_id": SIGNAL_ID,
        "plan_id": PLAN_ID,
        "filled_qty": 1,
    }
    row.update(overrides)
    return row


def _position(**overrides):
    row = {
        "id": POSITION_ID,
        "client_id": CLIENT,
        "execution_mode": MODE,
        "contract": CONTRACT,
        "status": "OPEN",
        "signal_id": SIGNAL_ID,
        "plan_id": RECONCILED_PLAN_ID,
        "local_order_id": None,
        "broker_order_id": None,
    }
    row.update(overrides)
    return row


def _result(**overrides):
    result = {"filled_qty": 1, "avg_fill": 1.58}
    result.update(overrides)
    return result


def _bind(monkeypatch, *, order=None, position=None, result=None, db_order=None):
    db = _MemoryDB(db_order or order or _order(), position or _position())
    monkeypatch.setattr(fm, "conn", db.conn)
    monkeypatch.setattr(fm, "run_with_retry", lambda fn: fn())
    outcome = fm._bind_filled_entry_durable_identity(
        position_id=POSITION_ID,
        order=order or _order(),
        result=result or _result(),
    )
    return outcome, db


def test_jason_pep_null_identity_binds_before_exit_seed(monkeypatch):
    outcome, db = _bind(monkeypatch)

    assert outcome == (True, "BOUND")
    assert db.orders[LOCAL_ORDER_ID]["position_id"] == POSITION_ID
    assert db.positions[POSITION_ID]["local_order_id"] == LOCAL_ORDER_ID
    assert db.positions[POSITION_ID]["broker_order_id"] == BROKER_ORDER_ID
    assert db.positions[POSITION_ID]["plan_id"] == RECONCILED_PLAN_ID


def test_already_correct_identity_is_idempotent(monkeypatch):
    outcome, db = _bind(
        monkeypatch,
        order=_order(position_id=POSITION_ID),
        position=_position(
            local_order_id=LOCAL_ORDER_ID,
            broker_order_id=BROKER_ORDER_ID,
        ),
    )

    assert outcome == (True, "BOUND")
    assert db.mutations == 0


@pytest.mark.parametrize(
    ("order_overrides", "db_order_overrides", "position_overrides", "reason"),
    [
        ({}, {"position_id": "foreign-position"}, {}, "entry_position_id_conflict"),
        ({}, {}, {"local_order_id": "foreign-local"}, "position_local_order_id_conflict"),
        ({}, {}, {"broker_order_id": "foreign-broker"}, "position_broker_order_id_conflict"),
        ({}, {}, {"signal_id": "foreign-signal"}, "position_signal_id_conflict"),
        ({}, {}, {"plan_id": "foreign-plan"}, "position_plan_conflict"),
        ({"execution_mode": "paper"}, {}, {}, "entry_execution_mode_conflict"),
        ({"contract": "PEP260821P00141000"}, {}, {}, "entry_contract_conflict"),
    ],
)
def test_exact_identity_conflicts_hold_without_overwrite(
    monkeypatch, order_overrides, db_order_overrides, position_overrides, reason
):
    outcome, db = _bind(
        monkeypatch,
        order=_order(**order_overrides),
        position=_position(**position_overrides),
        db_order=_order(**db_order_overrides),
    )

    assert outcome == (False, reason)
    assert db.mutations == 0
    assert db.orders[LOCAL_ORDER_ID]["position_id"] == db_order_overrides.get("position_id")
    assert db.positions[POSITION_ID]["local_order_id"] in (None, "foreign-local")
    assert db.positions[POSITION_ID]["broker_order_id"] in (None, "foreign-broker")


@pytest.mark.parametrize(
    ("position_overrides", "reason"),
    [
        ({"plan_id": "reconciled:WRONG_OCC:daeb6be995af7739a8cbcd3e"}, "position_plan_conflict"),
        ({"signal_id": "wrong-signal"}, "position_signal_id_conflict"),
        ({"signal_id": None}, "position_plan_conflict"),
        ({"plan_id": "ordinary-plan-1"}, "position_plan_conflict"),
    ],
)
def test_reconciled_plan_exception_remains_exactly_fenced(
    monkeypatch, position_overrides, reason
):
    outcome, db = _bind(
        monkeypatch,
        position=_position(**position_overrides),
    )

    assert outcome == (False, reason)
    assert db.mutations == 0


def test_reconciled_plan_exception_does_not_mask_order_plan_conflict(monkeypatch):
    outcome, db = _bind(
        monkeypatch,
        order=_order(plan_id="different-entry-plan"),
        db_order=_order(),
    )

    assert outcome == (False, "order_plan_conflict")
    assert db.mutations == 0


def test_wrong_client_and_wrong_mode_never_cross_bind(monkeypatch):
    outcome, db = _bind(
        monkeypatch,
        order=_order(client_id="other@example.com"),
        db_order=_order(),
    )
    assert outcome == (False, "entry_row_missing")
    assert db.mutations == 0

    outcome, db = _bind(
        monkeypatch,
        order=_order(execution_mode="paper"),
        db_order=_order(),
    )
    assert outcome == (False, "entry_execution_mode_conflict")
    assert db.mutations == 0


@pytest.mark.parametrize("broker_id", [None, "", "0", "N/A", "unknown", True, False])
def test_missing_or_placeholder_broker_identity_fails_before_db(monkeypatch, broker_id):
    db = _MemoryDB(_order(), _position())
    monkeypatch.setattr(fm, "conn", db.conn)
    monkeypatch.setattr(fm, "run_with_retry", lambda fn: fn())

    outcome = fm._bind_filled_entry_durable_identity(
        position_id=POSITION_ID,
        order=_order(broker_order_id=broker_id),
        result=_result(),
    )

    assert outcome == (False, "broker_order_id_missing_or_placeholder")
    assert db.statements == []
    assert db.mutations == 0


@pytest.mark.parametrize(
    ("field", "value"),
    [("client_id", ""), ("local_order_id", ""), ("execution_mode", "LIVE"), ("contract", "")],
)
def test_required_input_identity_is_fail_closed(monkeypatch, field, value):
    db = _MemoryDB(_order(), _position())
    monkeypatch.setattr(fm, "conn", db.conn)
    monkeypatch.setattr(fm, "run_with_retry", lambda fn: fn())

    outcome = fm._bind_filled_entry_durable_identity(
        position_id=POSITION_ID,
        order=_order(**{field: value}),
        result=_result(),
    )

    assert outcome[0] is False
    assert db.statements == []
    assert db.mutations == 0


def test_ticker_only_contract_is_not_an_exact_occ_identity(monkeypatch):
    db = _MemoryDB(_order(contract="PEP"), _position(contract="PEP"))
    monkeypatch.setattr(fm, "conn", db.conn)
    monkeypatch.setattr(fm, "run_with_retry", lambda fn: fn())

    outcome = fm._bind_filled_entry_durable_identity(
        position_id=POSITION_ID,
        order=_order(contract="PEP"),
        result=_result(),
    )

    assert outcome == (False, "contract_not_exact_OCC")
    assert db.statements == []
    assert db.mutations == 0


def test_occ_validation_rejects_non_occ_prefix_with_valid_looking_suffix(monkeypatch):
    malformed_contract = "BROKENPEP260821C00141000"
    db = _MemoryDB(
        _order(contract=malformed_contract),
        _position(contract=malformed_contract),
    )
    monkeypatch.setattr(fm, "conn", db.conn)
    monkeypatch.setattr(fm, "run_with_retry", lambda fn: fn())

    outcome = fm._bind_filled_entry_durable_identity(
        position_id=POSITION_ID,
        order=_order(contract=malformed_contract),
        result=_result(),
    )

    assert outcome == (False, "contract_not_exact_OCC")
    assert db.statements == []
    assert db.mutations == 0


def _owner(
    *,
    position_id=POSITION_ID,
    client_id=CLIENT,
    execution_mode=MODE,
    option_symbol=CONTRACT,
):
    return SimpleNamespace(
        position_id=position_id,
        client_id=client_id,
        execution_mode=execution_mode,
        option_symbol=option_symbol,
    )


class _SeedEngine:
    def __init__(self, *, raises=False, owners=None, materialize_owner=True):
        self.raises = raises
        self.seed_calls = 0
        self.owners = list(owners or [])
        self.materialize_owner = materialize_owner

    def seed_position(self, position_id, order, result):
        self.seed_calls += 1
        if self.raises:
            raise RuntimeError("seed failed")
        if self.materialize_owner:
            self.owners.append(_owner())

    def active_positions(self):
        return list(self.owners)


class _FallbackManagedPosition:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class _FallbackSeedEngine:
    def __init__(self):
        self.add_calls = 0
        self.owners = []

    def add_position(self, position):
        self.add_calls += 1
        self.owners.append(position)

    def active_positions(self):
        return list(self.owners)


def test_seed_result_reports_existing_success_and_failure(monkeypatch):
    monkeypatch.setattr(fm, "_load_managed_position_class", lambda: (_ for _ in ()).throw(RuntimeError("no fallback")))
    monkeypatch.setattr(fm, "audit", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *args, **kwargs: None)

    success = _SeedEngine()
    assert fm._seed_exit_engine(success, POSITION_ID, _order(), _result(), SIGNAL_ID) == (True, "SEEDED")
    assert success.seed_calls == 1

    failure = _SeedEngine(raises=True)
    assert fm._seed_exit_engine(failure, POSITION_ID, _order(), _result(), SIGNAL_ID) == (False, "SEED_FAILED")
    assert failure.seed_calls == 1


def test_seed_success_requires_exact_canonical_behavior_active_owner(monkeypatch):
    engine = _SeedEngine(materialize_owner=False)

    assert fm._seed_exit_engine(
        engine, POSITION_ID, _order(), _result(), SIGNAL_ID
    ) == (False, "OWNER_COUNT_0")
    assert engine.seed_calls == 1


def test_fallback_add_position_success_requires_proven_owner(monkeypatch):
    monkeypatch.setattr(fm, "_load_managed_position_class", lambda: _FallbackManagedPosition)

    engine = _FallbackSeedEngine()
    assert fm._seed_exit_engine(
        engine, POSITION_ID, _order(), _result(), SIGNAL_ID
    ) == (True, "SEEDED")
    assert engine.add_calls == 1
    assert engine.owners[0].position_id == POSITION_ID


class _NoOwnerLookupEngine:
    def __init__(self, *, raises=False):
        self.seed_calls = 0
        self.raises = raises

    def seed_position(self, position_id, order, result):
        self.seed_calls += 1

    def active_positions(self):
        if self.raises:
            raise RuntimeError("owner lookup failed")
        return []


@pytest.mark.parametrize(
    ("raises", "reason"),
    [(False, "OWNER_COUNT_0"), (True, "OWNER_LOOKUP_FAILED")],
)
def test_seed_never_reports_success_without_readable_owner_postcondition(raises, reason):
    engine = _NoOwnerLookupEngine(raises=raises)

    assert fm._seed_exit_engine(
        engine, POSITION_ID, _order(), _result(), SIGNAL_ID
    ) == (False, reason)
    assert engine.seed_calls == 1


@pytest.mark.parametrize(
    ("owners", "reason"),
    [
        ([_owner(position_id="broker-repair-1")], "BROKER_REPAIR_OWNER_PRESENT"),
        ([_owner(position_id="foreign-position")], "CANONICAL_OWNER_MISSING"),
        ([_owner(client_id="other@example.com")], "OWNER_COUNT_0"),
        ([_owner(execution_mode="paper")], "OWNER_COUNT_0"),
        ([_owner(option_symbol="PEP260821P00141000")], "OWNER_COUNT_0"),
        (
            [_owner(), _owner(position_id="duplicate-position")],
            "OWNER_COUNT_2",
        ),
    ],
)
def test_seed_postcondition_rejects_noncanonical_or_ambiguous_owner(owners, reason):
    engine = _SeedEngine(owners=owners, materialize_owner=False)

    assert fm._seed_exit_engine(
        engine, POSITION_ID, _order(), _result(), SIGNAL_ID
    ) == (False, reason)


def test_adoption_success_is_truthful(monkeypatch):
    class _AdoptEngine:
        def active_positions(self):
            return [_owner()]

        def adopt_canonical_position_identity(self, **kwargs):
            return SimpleNamespace(disposition="ADOPTED")

    monkeypatch.setattr(fm, "audit", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *args, **kwargs: None)

    assert fm._seed_exit_engine(
        _AdoptEngine(), POSITION_ID, _order(), _result(), SIGNAL_ID
    ) == (True, "ADOPTED")


class _Broker:
    def __init__(self):
        self.mutations = []

    def get_order(self, broker_order_id):
        return {
            "status": "filled",
            "exec_quantity": 1,
            "avg_fill_price": 1.58,
        }

    def get_quote(self, symbol):
        return {"last": 100.0}

    def place_stop_order(self, **kwargs):
        self.mutations.append(("entry_stop", kwargs))
        return {"id": "stop-1", "status": "ok"}

    def submit_order(self, **kwargs):
        self.mutations.append(("submit", kwargs))

    def cancel_order(self, **kwargs):
        self.mutations.append(("cancel", kwargs))


class _OSM:
    def __init__(self):
        self.transitions = []

    def transition(self, local_order_id, status, **kwargs):
        self.transitions.append((local_order_id, status, kwargs))
        return True


class _PM:
    def open_position(self, **kwargs):
        return POSITION_ID


def _process_entry(monkeypatch, *, bind_result, seed_result):
    order = _order(status="ACKNOWLEDGED", filled_qty=0)
    broker = _Broker()
    osm = _OSM()
    calls = []
    markers = []

    monkeypatch.setattr(fm, "audit", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "trace_gate", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "_cancel_pair_opposite", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "_place_standing_stop_best_effort", lambda **kwargs: calls.append("standing_stop"))
    monkeypatch.setattr(fm, "_release_entry_guards", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "_record_position_create_failure", lambda order, reason: markers.append((order["client_id"], order["local_order_id"], reason)))
    monkeypatch.setattr(
        fm,
        "_bind_filled_entry_durable_identity",
        lambda **kwargs: (calls.append("bind") or bind_result),
    )
    monkeypatch.setattr(
        fm,
        "_seed_exit_engine",
        lambda *args, **kwargs: (calls.append("seed") or seed_result),
    )

    fm.process_pending_order(
        broker,
        order,
        osm=osm,
        pm=_PM(),
        exit_engine=object(),
    )
    return calls, markers, broker, osm


def test_jason_reconciler_shape_binds_before_seed_and_preserves_plan(monkeypatch):
    db = _MemoryDB(_order(), _position())
    monkeypatch.setattr(fm, "conn", db.conn)
    monkeypatch.setattr(fm, "run_with_retry", lambda fn: fn())
    monkeypatch.setattr(
        fm,
        "check_order_with_broker",
        lambda broker, order: {"status": "FILLED", "filled_qty": 1, "avg_fill": 1.58},
    )
    monkeypatch.setattr(fm, "audit", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "trace_gate", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "_cancel_pair_opposite", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "_release_entry_guards", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "_open_position_safe", lambda *args, **kwargs: POSITION_ID)

    calls = []
    monkeypatch.setattr(
        fm,
        "_place_standing_stop_best_effort",
        lambda **kwargs: calls.append("standing_stop"),
    )
    real_bind = fm._bind_filled_entry_durable_identity

    def _bind_and_record(**kwargs):
        outcome = real_bind(**kwargs)
        calls.append("bind")
        return outcome

    monkeypatch.setattr(fm, "_bind_filled_entry_durable_identity", _bind_and_record)
    monkeypatch.setattr(
        fm,
        "_seed_exit_engine",
        lambda *args, **kwargs: (calls.append("seed") or (True, "SEEDED")),
    )

    fm.process_pending_order(
        _Broker(),
        _order(status="ACKNOWLEDGED", filled_qty=0),
        osm=_OSM(),
        pm=object(),
        exit_engine=object(),
    )

    # AMENDMENT (PR #544): the standing broker stop is no longer submitted
    # from fill reconciliation. The monkeypatch above stays as a defense —
    # if a future change re-adds the call, "standing_stop" will appear in
    # ``calls`` and this assertion will fail.
    assert calls == ["bind", "seed"]
    assert "standing_stop" not in calls
    assert db.orders[LOCAL_ORDER_ID]["position_id"] == POSITION_ID
    assert db.positions[POSITION_ID]["local_order_id"] == LOCAL_ORDER_ID
    assert db.positions[POSITION_ID]["broker_order_id"] == BROKER_ORDER_ID
    assert db.positions[POSITION_ID]["plan_id"] == RECONCILED_PLAN_ID


def test_handoff_binds_before_seed_and_does_not_add_broker_authority(monkeypatch):
    calls, markers, broker, osm = _process_entry(
        monkeypatch,
        bind_result=(True, "BOUND"),
        seed_result=(True, "SEEDED"),
    )

    # AMENDMENT (PR #544): standing broker stop removed; canonical exit owns.
    assert calls == ["bind", "seed"]
    assert "standing_stop" not in calls
    assert markers == []
    assert [kind for kind, _ in broker.mutations] == []
    assert osm.transitions[0][1] == "FILLED"


def test_bind_failure_marks_exact_entry_and_skips_seed(monkeypatch):
    calls, markers, broker, _ = _process_entry(
        monkeypatch,
        bind_result=(False, "position_contract_conflict"),
        seed_result=(True, "SEEDED"),
    )

    # AMENDMENT (PR #544): standing broker stop removed; canonical exit owns.
    assert calls == ["bind"]
    assert "standing_stop" not in calls
    assert markers == [(CLIENT, LOCAL_ORDER_ID, "FILLED_ENTRY_POSITION_IDENTITY_BIND_FAILED")]
    assert [kind for kind, _ in broker.mutations] == []


def test_owner_unproven_marks_exact_entry_without_second_seed(monkeypatch):
    calls, markers, broker, _ = _process_entry(
        monkeypatch,
        bind_result=(True, "BOUND"),
        seed_result=(False, "ADOPTION_RETRY"),
    )

    # AMENDMENT (PR #544): standing broker stop removed; canonical exit owns.
    assert calls == ["bind", "seed"]
    assert "standing_stop" not in calls
    assert markers == [(CLIENT, LOCAL_ORDER_ID, "FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN")]
    assert [kind for kind, _ in broker.mutations] == []
