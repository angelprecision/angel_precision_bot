"""P0 coverage for reconciler canonical exit-owner adoption."""

from __future__ import annotations

import os
import threading
from types import SimpleNamespace

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@127.0.0.1:5432/test")

import ap_reconciler as rec


CLIENT = "jasoncosby1@gmail.com"
CONTRACT = "NOW260828P00122000"
POSITION_ID = "2fe10f52-e459-4bc5-a57a-1a80e3618040"


def _owner(position_id, *, client_id=CLIENT, mode="live", contract=CONTRACT):
    return SimpleNamespace(
        position_id=position_id,
        client_id=client_id,
        execution_mode=mode,
        option_symbol=contract,
        closed=False,
    )


class _ExitEngine:
    def __init__(
        self,
        disposition="ADOPTED",
        *,
        owners=None,
        mutate=True,
        raise_adoption=False,
        raise_lookup=False,
        result_overrides=None,
    ):
        self.disposition = disposition
        self.owners = list(owners or [])
        self.mutate = mutate
        self.raise_adoption = raise_adoption
        self.raise_lookup = raise_lookup
        self.result_overrides = result_overrides or {}
        self.adopt_calls = []
        self.add_calls = []

    def adopt_canonical_position_identity(self, **kwargs):
        self.adopt_calls.append(kwargs)
        if self.raise_adoption:
            raise RuntimeError("adoption unavailable")
        if self.mutate and self.disposition == "ADOPTED":
            for owner in self.owners:
                if str(owner.position_id).startswith("broker-repair-"):
                    owner.position_id = kwargs["canonical_position_id"]
                    owner.client_id = kwargs["client_id"]
                    owner.execution_mode = kwargs["execution_mode"]
                    owner.option_symbol = kwargs["contract"]
                    break
        elif self.mutate and self.disposition == "ALREADY_CANONICAL_REPAIR_REMOVED":
            self.owners = [
                owner for owner in self.owners
                if not str(owner.position_id).startswith("broker-repair-")
            ]
        result = {
            "disposition": self.disposition,
            "adopted": self.disposition in {
                "ADOPTED", "ALREADY_CANONICAL_REPAIR_REMOVED"
            },
            "safe_to_seed": self.disposition == "NO_REPAIR_FOUND",
            "retryable": self.disposition.startswith("RETRY_"),
        }
        result.update(self.result_overrides)
        return SimpleNamespace(**result)

    def add_position(self, position):
        self.add_calls.append(position)
        self.owners.append(position)

    def active_positions(self):
        if self.raise_lookup:
            raise RuntimeError("owner lookup unavailable")
        return list(self.owners)


def _reconciler(engine, *, evidence_status="NO_EVIDENCE", evidence=None):
    reconciler = rec.APBrokerReconciler.__new__(rec.APBrokerReconciler)
    reconciler.client_id = CLIENT
    reconciler.execution_mode = "live"
    reconciler.exit_engine = engine
    reconciler._alert = lambda _message: None
    reconciler._record_position_reseeded = lambda **_kwargs: None
    reconciler._record_reconciler_rejection = lambda **_kwargs: None
    reconciler._report_health_error = lambda *_args, **_kwargs: None
    reconciler._heartbeat = lambda *_args, **_kwargs: None
    reconciler._filled_entry_evidence_for_canonical_position = (
        lambda **_kwargs: (evidence_status, evidence)
    )
    return reconciler


def _position(**overrides):
    position = {
        "id": POSITION_ID,
        "client_id": CLIENT,
        "execution_mode": "live",
        "underlying": "NOW",
        "contract": CONTRACT,
        "direction": "PUT",
        "qty": 1,
        "avg_fill": 1.30,
        "underlying_entry": 127.425,
        "local_order_id": "entry-local-1",
        "broker_order_id": "143201293",
        "signal_id": "signal-1",
        "canonical_signal_id": "canonical-signal-1",
    }
    position.update(overrides)
    return position


def test_now_shaped_repair_routes_through_adoption_before_generic_add():
    repair_id = f"broker-repair-{CLIENT}-{CONTRACT}"
    engine = _ExitEngine(owners=[_owner(repair_id)])
    assert _reconciler(engine)._seed_exit_engine_from_position(_position()) is True
    assert len(engine.adopt_calls) == 1
    assert engine.add_calls == []
    assert [owner.position_id for owner in engine.owners] == [POSITION_ID]


def test_real_exit_engine_adopts_through_reconciler_boundary():
    from ap_exit_engine import APExitEngine, ManagedPosition

    engine = APExitEngine.__new__(APExitEngine)
    engine._email = CLIENT
    engine._lock = threading.RLock()
    repair_id = f"broker-repair-{CLIENT}-{CONTRACT}"
    repair = ManagedPosition(
        ticker="NOW",
        option_symbol=CONTRACT,
        side="PUT",
        quantity=1,
        entry_price=1.30,
        underlying_entry=127.425,
        underlying_target=120.0,
        underlying_stop=130.0,
    )
    repair.position_id = repair_id
    repair.client_id = CLIENT
    repair.execution_mode = "live"
    engine._positions = [repair]
    engine._positions_by_id = {repair_id: repair}

    assert _reconciler(engine)._seed_exit_engine_from_position(_position()) is True
    assert engine.active_positions() == [repair]
    assert repair.position_id == POSITION_ID
    assert repair.client_id == CLIENT
    assert repair.execution_mode == "live"
    assert repair.option_symbol == CONTRACT


def test_already_canonical_repair_removed_never_adds():
    engine = _ExitEngine(
        "ALREADY_CANONICAL_REPAIR_REMOVED",
        owners=[_owner(POSITION_ID), _owner("broker-repair-stale")],
    )
    assert _reconciler(engine)._seed_exit_engine_from_position(_position()) is True
    assert engine.add_calls == []
    assert [owner.position_id for owner in engine.owners] == [POSITION_ID]


def test_no_repair_found_adds_canonical_once_and_proves_owner():
    engine = _ExitEngine("NO_REPAIR_FOUND")
    assert _reconciler(engine)._seed_exit_engine_from_position(_position()) is True
    assert len(engine.add_calls) == 1
    assert engine.owners[0].position_id == POSITION_ID


@pytest.mark.parametrize(
    "disposition",
    [
        "RETRY_CLIENT_MISMATCH",
        "RETRY_MODE_MISMATCH",
        "RETRY_REPAIR_IDENTITY_UNPROVEN",
        "RETRY_IDENTITY_CONFLICT",
        "RETRY_ADOPTION_ERROR",
    ],
)
def test_every_retry_disposition_holds_without_add(disposition):
    engine = _ExitEngine(disposition, owners=[_owner("broker-repair-held")])
    assert _reconciler(engine)._seed_exit_engine_from_position(_position()) is False
    assert engine.add_calls == []


@pytest.mark.parametrize("disposition", ["", "FUTURE_RESULT", "ADOPTED-ish"])
def test_unknown_or_unreadable_disposition_holds_without_add(disposition):
    engine = _ExitEngine(disposition, owners=[_owner("broker-repair-held")])
    assert _reconciler(engine)._seed_exit_engine_from_position(_position()) is False
    assert engine.add_calls == []


def test_adoption_exception_holds_without_add():
    engine = _ExitEngine(owners=[_owner("broker-repair-held")], raise_adoption=True)
    assert _reconciler(engine)._seed_exit_engine_from_position(_position()) is False
    assert engine.add_calls == []


@pytest.mark.parametrize(
    "owners",
    [
        [_owner("broker-repair-still-active")],
        [_owner(POSITION_ID), _owner("broker-repair-still-active")],
        [_owner("wrong-canonical-id")],
        [],
    ],
)
def test_reported_success_with_bad_postcondition_holds_without_add(owners):
    engine = _ExitEngine("ADOPTED", owners=owners, mutate=False)
    assert _reconciler(engine)._seed_exit_engine_from_position(_position()) is False
    assert engine.add_calls == []


def test_owner_lookup_failure_does_not_compensate_with_add():
    engine = _ExitEngine(
        "ADOPTED", owners=[_owner("broker-repair-held")], raise_lookup=True
    )
    assert _reconciler(engine)._seed_exit_engine_from_position(_position()) is False
    assert engine.add_calls == []


def test_repeated_reconciliation_is_idempotent():
    engine = _ExitEngine("NO_REPAIR_FOUND")
    reconciler = _reconciler(engine)
    assert reconciler._seed_exit_engine_from_position(_position()) is True
    engine.disposition = "ALREADY_CANONICAL_REPAIR_REMOVED"
    assert reconciler._seed_exit_engine_from_position(_position()) is True
    assert len(engine.add_calls) == 1
    assert [owner.position_id for owner in engine.owners] == [POSITION_ID]


def test_no_repair_race_never_exposes_two_behavior_active_owners():
    from ap_exit_engine import APExitEngine, ManagedPosition

    ready = threading.Event()
    installed = threading.Event()
    observed_counts = []

    class _RacingExitEngine(APExitEngine):
        def add_position(self, position):
            if position.position_id == POSITION_ID:
                ready.set()
                assert installed.wait(timeout=2)
            super().add_position(position)
            observed_counts.append(len(self.active_positions()))

    engine = _RacingExitEngine.__new__(_RacingExitEngine)
    engine._email = CLIENT
    engine._lock = threading.RLock()
    engine._positions = []
    engine._positions_by_id = {}
    competing = ManagedPosition(
        ticker="NOW", option_symbol=CONTRACT, side="PUT", quantity=1,
        entry_price=1.30, underlying_entry=127.425,
        underlying_target=120.0, underlying_stop=130.0,
    )
    competing.position_id = f"broker-repair-degraded:{CLIENT}:{CONTRACT}"
    competing.client_id = CLIENT
    competing.execution_mode = "live"

    def _install_competing_owner():
        assert ready.wait(timeout=2)
        APExitEngine.add_position(engine, competing)
        observed_counts.append(len(engine.active_positions()))
        installed.set()

    worker = threading.Thread(target=_install_competing_owner)
    worker.start()
    assert _reconciler(engine)._seed_exit_engine_from_position(_position()) is False
    worker.join(timeout=2)

    assert not worker.is_alive()
    assert observed_counts and max(observed_counts) == 1
    assert engine.active_positions() == [competing]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("client_id", "other@example.com"),
        ("execution_mode", "paper"),
        ("execution_mode", "unknown"),
        ("execution_mode", ""),
        ("contract", "NOW"),
        ("id", ""),
        ("id", "pending"),
        ("qty", 1.5),
        ("qty", True),
        ("avg_fill", float("nan")),
    ],
)
def test_unproven_canonical_identity_never_reaches_adoption_or_add(field, value):
    engine = _ExitEngine(owners=[_owner("broker-repair-held")])
    assert _reconciler(engine)._seed_exit_engine_from_position(
        _position(**{field: value})
    ) is False
    assert engine.adopt_calls == []
    assert engine.add_calls == []


def test_column_meta_mode_contradiction_holds_before_adoption():
    engine = _ExitEngine(owners=[_owner("broker-repair-held")])
    position = _position(meta={"execution_mode": "paper"})
    assert _reconciler(engine)._seed_exit_engine_from_position(position) is False
    assert engine.adopt_calls == []
    assert engine.add_calls == []


def test_conflicting_quantity_aliases_hold_before_adoption():
    engine = _ExitEngine(owners=[_owner("broker-repair-held")])
    position = _position(quantity=2)
    assert _reconciler(engine)._seed_exit_engine_from_position(position) is False
    assert engine.adopt_calls == []
    assert engine.add_calls == []


def test_total_and_remaining_quantity_must_agree_for_adoption():
    engine = _ExitEngine("NO_REPAIR_FOUND")
    position = _position(qty=2, quantity_remaining=2)
    assert _reconciler(engine)._seed_exit_engine_from_position(position) is True
    assert engine.add_calls[0].quantity == 2


@pytest.mark.parametrize(
    "overrides",
    [
        {"qty": 2, "quantity_remaining": 1},
        {"qty": 1, "quantity_remaining": 2},
        {"qty": 2, "quantity_remaining": 1.5},
        {"qty": 2, "quantity_remaining": -1},
        {"qty": 2, "quantity": 3, "quantity_remaining": 2},
    ],
)
def test_unproven_quantity_conflict_holds_before_adoption(overrides):
    engine = _ExitEngine(owners=[_owner("broker-repair-held")])
    assert _reconciler(engine)._seed_exit_engine_from_position(
        _position(**overrides)
    ) is False
    assert engine.adopt_calls == []
    assert engine.add_calls == []


def test_cross_client_owner_cannot_donate_or_be_removed():
    other = _owner("broker-repair-other", client_id="other@example.com")
    engine = _ExitEngine("NO_REPAIR_FOUND", owners=[other])
    assert _reconciler(engine)._seed_exit_engine_from_position(_position()) is True
    assert other in engine.owners
    assert len(engine.add_calls) == 1


@pytest.mark.parametrize("mode", ["paper", "unknown", ""])
def test_non_live_repair_owner_cannot_become_live(mode):
    repair = _owner("broker-repair-other-mode", mode=mode)
    engine = _ExitEngine("RETRY_MODE_MISMATCH", owners=[repair])
    assert _reconciler(engine)._seed_exit_engine_from_position(_position()) is False
    assert repair.position_id == "broker-repair-other-mode"
    assert engine.add_calls == []


def test_wrong_occ_repair_owner_cannot_become_canonical():
    repair = _owner("broker-repair-wrong-occ", contract="NOW260828C00122000")
    engine = _ExitEngine("NO_REPAIR_FOUND", owners=[repair])
    assert _reconciler(engine)._seed_exit_engine_from_position(_position()) is True
    assert repair in engine.owners
    assert len(engine.add_calls) == 1


def test_conflicting_position_order_aliases_hold_before_adoption():
    engine = _ExitEngine(owners=[_owner("broker-repair-held")])
    position = _position(entry_local_order_id="different-local-id")
    assert _reconciler(engine)._seed_exit_engine_from_position(position) is False
    assert engine.adopt_calls == []
    assert engine.add_calls == []


def test_exact_filled_entry_enriches_blank_canonical_metadata():
    evidence = {
        "local_order_id": "entry-local-1",
        "broker_order_id": "143201293",
        "signal_id": "signal-1",
        "canonical_signal_id": "canonical-signal-1",
        "fill_price": 1.30,
        "filled_ts": "2026-08-25T14:31:00+00:00",
    }
    engine = _ExitEngine(owners=[_owner("broker-repair-held")])
    position = _position(
        local_order_id="", broker_order_id="", signal_id="", canonical_signal_id=""
    )
    reconciler = _reconciler(engine, evidence_status="PROVEN", evidence=evidence)
    assert reconciler._seed_exit_engine_from_position(position) is True
    call = engine.adopt_calls[0]
    assert call["local_order_id"] == "entry-local-1"
    assert call["broker_order_id"] == "143201293"
    assert call["canonical_signal_id"] == "canonical-signal-1"


@pytest.mark.parametrize(
    ("status", "evidence"),
    [
        ("AMBIGUOUS", None),
        ("UNAVAILABLE", None),
        ("MALFORMED", None),
        ("PROVEN", {"local_order_id": "conflict", "fill_price": 1.30}),
        ("PROVEN", {"local_order_id": "entry-local-1", "fill_price": 9.99}),
    ],
)
def test_untrusted_or_conflicting_entry_evidence_holds(status, evidence):
    engine = _ExitEngine(owners=[_owner("broker-repair-held")])
    reconciler = _reconciler(engine, evidence_status=status, evidence=evidence)
    assert reconciler._seed_exit_engine_from_position(_position()) is False
    assert engine.adopt_calls == []
    assert engine.add_calls == []


def test_unknown_historical_entry_remains_zero_and_never_reads_current_quote():
    engine = _ExitEngine(owners=[_owner("broker-repair-held")])
    reconciler = _reconciler(engine)
    reconciler._filled_entry_underlying_for_position = lambda **_kwargs: 0.0
    reconciler._get_current_underlying_price = lambda *_args, **_kwargs: pytest.fail(
        "current quote must not become historical entry truth"
    )
    assert reconciler._seed_exit_engine_from_position(
        _position(underlying_entry=0.0)
    ) is True
    assert engine.adopt_calls[0]["underlying_entry"] == 0.0
    assert engine.adopt_calls[0]["underlying_entry_trusted"] is False


def test_real_exit_engine_clears_stale_repair_entry_when_canonical_truth_unknown():
    from ap_exit_engine import APExitEngine, ManagedPosition

    engine = APExitEngine.__new__(APExitEngine)
    engine._email = CLIENT
    engine._lock = threading.RLock()
    repair_id = f"broker-repair-{CLIENT}-{CONTRACT}"
    repair = ManagedPosition(
        ticker="NOW",
        option_symbol=CONTRACT,
        side="PUT",
        quantity=1,
        entry_price=1.30,
        underlying_entry=999.0,
        underlying_target=120.0,
        underlying_stop=130.0,
    )
    repair.position_id = repair_id
    repair.client_id = CLIENT
    repair.execution_mode = "live"
    repair.underlying_entry_untrusted = False
    engine._positions = [repair]
    engine._positions_by_id = {repair_id: repair}

    reconciler = _reconciler(engine)
    reconciler._filled_entry_underlying_for_position = lambda **_kwargs: 0.0
    assert reconciler._seed_exit_engine_from_position(
        _position(underlying_entry=0.0)
    ) is True
    assert repair.position_id == POSITION_ID
    assert repair.underlying_entry == 0.0
    assert repair.underlying_entry_untrusted is True


def test_existing_canonical_owner_clears_stale_entry_when_truth_unknown():
    from ap_exit_engine import APExitEngine, ManagedPosition

    engine = APExitEngine.__new__(APExitEngine)
    engine._email = CLIENT
    engine._lock = threading.RLock()
    canonical = ManagedPosition(
        ticker="NOW", option_symbol=CONTRACT, side="PUT", quantity=1,
        entry_price=1.30, underlying_entry=999.0,
        underlying_target=120.0, underlying_stop=130.0,
    )
    canonical.position_id = POSITION_ID
    canonical.client_id = CLIENT
    canonical.execution_mode = "live"
    canonical.underlying_entry_untrusted = False
    repair = ManagedPosition(
        ticker="NOW", option_symbol=CONTRACT, side="PUT", quantity=1,
        entry_price=1.30, underlying_entry=999.0,
        underlying_target=120.0, underlying_stop=130.0,
    )
    repair.position_id = f"broker-repair-{CLIENT}-{CONTRACT}"
    repair.client_id = CLIENT
    repair.execution_mode = "live"
    engine._positions = [canonical, repair]
    engine._positions_by_id = {
        canonical.position_id: canonical,
        repair.position_id: repair,
    }

    reconciler = _reconciler(engine)
    reconciler._filled_entry_underlying_for_position = lambda **_kwargs: 0.0
    assert reconciler._seed_exit_engine_from_position(
        _position(underlying_entry=0.0)
    ) is True
    assert engine.active_positions() == [canonical]
    assert canonical.underlying_entry == 0.0
    assert canonical.underlying_entry_untrusted is True


def test_conflicting_historical_underlying_aliases_hold_before_adoption():
    engine = _ExitEngine(owners=[_owner("broker-repair-held")])
    position = _position(entry_underlying=128.0)
    assert _reconciler(engine)._seed_exit_engine_from_position(position) is False
    assert engine.adopt_calls == []
    assert engine.add_calls == []


@pytest.mark.parametrize(
    "overrides",
    [
        {"direction": ""},
        {"direction": "PUT", "side": "CALL"},
        {"direction": "CALL"},
    ],
)
def test_unproven_or_occ_conflicting_direction_holds_before_adoption(overrides):
    engine = _ExitEngine(owners=[_owner("broker-repair-held")])
    assert _reconciler(engine)._seed_exit_engine_from_position(
        _position(**overrides)
    ) is False
    assert engine.adopt_calls == []
    assert engine.add_calls == []


@pytest.mark.parametrize(
    "overrides",
    [
        {"stop_underlying": 130.0, "underlying_stop": 131.0},
        {"target_underlying": 120.0, "underlying_target": 119.0},
    ],
)
def test_conflicting_exit_geometry_aliases_hold_before_adoption(overrides):
    engine = _ExitEngine(owners=[_owner("broker-repair-held")])
    assert _reconciler(engine)._seed_exit_engine_from_position(
        _position(**overrides)
    ) is False
    assert engine.adopt_calls == []
    assert engine.add_calls == []


def test_structurally_inconsistent_success_result_holds():
    engine = _ExitEngine(
        owners=[_owner("broker-repair-held")],
        result_overrides={"safe_to_seed": True},
    )
    assert _reconciler(engine)._seed_exit_engine_from_position(_position()) is False
    assert engine.add_calls == []


class _EvidenceCursor:
    def __init__(self, rows):
        self.rows = rows
        self.sql = ""
        self.params = ()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params):
        self.sql = " ".join(str(sql).split())
        self.params = tuple(params)

    def fetchall(self):
        return self.rows


def test_entry_evidence_lookup_is_exactly_domain_fenced(monkeypatch):
    import ap.db as db

    row = {
        "client_id": CLIENT,
        "kind": "ENTRY",
        "status": "FILLED",
        "local_order_id": "entry-local-1",
        "broker_order_id": "143201293",
        "signal_id": "signal-1",
        "canonical_signal_id": "canonical-signal-1",
        "fill_price": 1.30,
        "filled_qty": 1,
        "filled_ts": "2026-08-25T14:31:00+00:00",
        "execution_mode": "live",
        "position_id": POSITION_ID,
        "contract": CONTRACT,
        "meta": {},
    }
    cursor = _EvidenceCursor([row])
    monkeypatch.setattr(db, "conn", lambda: cursor)
    monkeypatch.setattr(db, "run_with_retry", lambda fn: fn())
    reconciler = _reconciler(_ExitEngine())

    status, evidence = reconciler.__class__._filled_entry_evidence_for_canonical_position(
        reconciler,
        contract=CONTRACT,
        position_id=POSITION_ID,
        execution_mode="live",
        local_order_id="entry-local-1",
        broker_order_id="143201293",
        signal_id="signal-1",
        canonical_signal_id="canonical-signal-1",
    )

    assert status == "PROVEN"
    assert evidence == row
    assert cursor.params == (
        POSITION_ID,
        "entry-local-1",
        "entry-local-1",
        "143201293",
        "143201293",
    )
    assert "client_id=%s" not in cursor.sql
    assert "position_id::text=%s" in cursor.sql
    assert "local_order_id=%s" in cursor.sql
    assert "broker_order_id=%s" in cursor.sql
    assert "filled_qty" not in cursor.sql.split("FROM orders", 1)[1]
    assert "execution_mode" not in cursor.sql.split("FROM orders", 1)[1]
    assert "LIMIT 2" in cursor.sql


@pytest.mark.parametrize(
    ("field", "value", "expected_status"),
    [
        ("filled_ts", "not-a-timestamp", "MALFORMED"),
        ("filled_qty", "not-a-number", "MALFORMED"),
        ("execution_mode", "paper", "IDENTITY_CONFLICT"),
        ("status", "REJECTED", "IDENTITY_CONFLICT"),
    ],
)
def test_entry_evidence_lookup_rejects_invalid_candidate_truth(
    monkeypatch, field, value, expected_status
):
    import ap.db as db

    row = {
        "client_id": CLIENT,
        "kind": "ENTRY",
        "status": "FILLED",
        "local_order_id": "entry-local-1",
        "broker_order_id": "143201293",
        "signal_id": "signal-1",
        "canonical_signal_id": "canonical-signal-1",
        "fill_price": 1.30,
        "filled_qty": 1,
        "filled_ts": "2026-08-25T14:31:00+00:00",
        "execution_mode": "live",
        "position_id": POSITION_ID,
        "contract": CONTRACT,
        "meta": {},
    }
    row[field] = value
    cursor = _EvidenceCursor([row])
    monkeypatch.setattr(db, "conn", lambda: cursor)
    monkeypatch.setattr(db, "run_with_retry", lambda fn: fn())
    reconciler = _reconciler(_ExitEngine())

    status, evidence = reconciler.__class__._filled_entry_evidence_for_canonical_position(
        reconciler,
        contract=CONTRACT,
        position_id=POSITION_ID,
        execution_mode="live",
        local_order_id="entry-local-1",
        broker_order_id="143201293",
        signal_id="signal-1",
        canonical_signal_id="canonical-signal-1",
    )

    assert status == expected_status
    assert evidence is None


def test_entry_evidence_lookup_rejects_conflicting_linked_position(monkeypatch):
    import ap.db as db

    row = {
        "client_id": CLIENT,
        "kind": "ENTRY",
        "status": "FILLED",
        "local_order_id": "entry-local-1",
        "broker_order_id": "143201293",
        "signal_id": "signal-1",
        "canonical_signal_id": "canonical-signal-1",
        "fill_price": 1.30,
        "filled_qty": 1,
        "filled_ts": "2026-08-25T14:31:00+00:00",
        "execution_mode": "live",
        "position_id": "different-position-id",
        "contract": CONTRACT,
        "meta": {},
    }
    cursor = _EvidenceCursor([row])
    monkeypatch.setattr(db, "conn", lambda: cursor)
    monkeypatch.setattr(db, "run_with_retry", lambda fn: fn())
    reconciler = _reconciler(_ExitEngine())

    status, evidence = reconciler.__class__._filled_entry_evidence_for_canonical_position(
        reconciler,
        contract=CONTRACT,
        position_id=POSITION_ID,
        execution_mode="live",
        local_order_id="entry-local-1",
        broker_order_id="143201293",
        signal_id="signal-1",
        canonical_signal_id="canonical-signal-1",
    )

    assert status == "IDENTITY_CONFLICT"
    assert evidence is None


def test_entry_evidence_lookup_rejects_ambiguous_exact_rows(monkeypatch):
    import ap.db as db

    cursor = _EvidenceCursor([{"id": 1}, {"id": 2}])
    monkeypatch.setattr(db, "conn", lambda: cursor)
    monkeypatch.setattr(db, "run_with_retry", lambda fn: fn())
    reconciler = _reconciler(_ExitEngine())

    status, evidence = reconciler.__class__._filled_entry_evidence_for_canonical_position(
        reconciler,
        contract=CONTRACT,
        position_id=POSITION_ID,
        execution_mode="live",
    )

    assert status == "AMBIGUOUS"
    assert evidence is None
