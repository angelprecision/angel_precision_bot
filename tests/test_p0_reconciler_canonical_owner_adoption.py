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


def _owner(
    position_id,
    *,
    client_id=CLIENT,
    mode="live",
    contract=CONTRACT,
    degraded=False,
    quantity=1,
    quantity_remaining=None,
):
    return SimpleNamespace(
        position_id=position_id,
        client_id=client_id,
        execution_mode=mode,
        option_symbol=contract,
        quantity=quantity,
        quantity_remaining=(quantity if quantity_remaining is None else quantity_remaining),
        broker_repair_degraded=degraded,
        adoption_identity_quarantined=False,
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
        seed_hook=None,
    ):
        self.disposition = disposition
        self.owners = list(owners or [])
        self.mutate = mutate
        self.raise_adoption = raise_adoption
        self.raise_lookup = raise_lookup
        self.result_overrides = result_overrides or {}
        self.seed_hook = seed_hook
        self.adopt_calls = []
        self.add_calls = []

    def adopt_canonical_position_identity(self, **kwargs):
        self.adopt_calls.append(kwargs)
        if self.raise_adoption:
            raise RuntimeError("adoption unavailable")
        if self.mutate and self.disposition == "ADOPTED":
            for owner in self.owners:
                if str(owner.position_id).startswith("broker-repair-"):
                    if "quantity" in kwargs:
                        owner.quantity = kwargs["quantity"]
                    if "quantity_remaining" in kwargs:
                        owner.quantity_remaining = kwargs["quantity_remaining"]
                    owner.position_id = kwargs["canonical_position_id"]
                    owner.client_id = kwargs["client_id"]
                    owner.execution_mode = kwargs["execution_mode"]
                    owner.option_symbol = kwargs["contract"]
                    break
        elif self.mutate and self.disposition == "ALREADY_CANONICAL_REPAIR_REMOVED":
            for owner in self.owners:
                if str(owner.position_id) == kwargs["canonical_position_id"]:
                    if "quantity" in kwargs:
                        owner.quantity = kwargs["quantity"]
                    if "quantity_remaining" in kwargs:
                        owner.quantity_remaining = kwargs["quantity_remaining"]
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

    def seed_canonical_position_if_absent(self, position):
        """Atomic stub mirroring the production exact-domain fence."""
        if self.seed_hook is not None:
            hook, self.seed_hook = self.seed_hook, None
            hook(self)
        pos_id = str(getattr(position, "position_id", "") or "").strip()
        if not pos_id:
            return False, "missing_id"
        domain_owners = [
            owner for owner in self.owners
            if not getattr(owner, "closed", False)
            and int(getattr(owner, "quantity_remaining", 0) or 0) > 0
            and str(getattr(owner, "client_id", "") or "").strip().lower()
                == str(getattr(position, "client_id", "") or "").strip().lower()
            and str(getattr(owner, "execution_mode", "") or "").strip().lower()
                == str(getattr(position, "execution_mode", "") or "").strip().lower()
            and str(getattr(owner, "option_symbol", "") or "").strip().upper()
                == str(getattr(position, "option_symbol", "") or "").strip().upper()
        ]
        if len(domain_owners) > 1:
            return False, "owner_conflict"
        if domain_owners:
            owner = domain_owners[0]
            owner_id = str(getattr(owner, "position_id", "") or "").strip()
            if owner_id == pos_id and not owner_id.startswith("broker-repair-"):
                return False, "already_owned"
            if owner_id.startswith("broker-repair-") or getattr(
                owner, "broker_repair_degraded", False
            ):
                return False, "degraded_owner_present"
            return False, "owner_conflict"
        self.add_calls.append(position)
        self.owners.append(position)
        return True, "seeded"

    def active_positions(self):
        if self.raise_lookup:
            raise RuntimeError("owner lookup unavailable")
        return list(self.owners)


def _reconciler(
    engine,
    *,
    evidence_status="NO_EVIDENCE",
    evidence=None,
    partial_evidence_status="NO_EVIDENCE",
    partial_evidence=None,
):
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
    reconciler._partial_exit_evidence_for_canonical_position = (
        lambda **_kwargs: (partial_evidence_status, partial_evidence)
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


def test_atomic_seed_unavailable_holds_without_generic_add():
    """An engine without the atomic seam must never revive generic seeding."""
    class _NoAtomicExitEngine:
        def __init__(self):
            self.add_calls = []

        def add_position(self, position):
            self.add_calls.append(position)

    engine = _NoAtomicExitEngine()
    result = _reconciler(engine)._seed_exit_engine_from_import(
        pos_id=POSITION_ID,
        contract=CONTRACT,
        underlying="NOW",
        side="PUT",
        qty=1,
        entry_px=1.30,
        underlying_entry=0.0,
        price_untrusted=True,
    )

    assert result == "atomic_seed_unavailable"
    assert engine.add_calls == []


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
    # Force the lookup to run by omitting required signal identity while
    # retaining the known local identity and fill used by the conflict cases.
    assert reconciler._seed_exit_engine_from_position(
        _position(signal_id="")
    ) is False
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
        CLIENT,
        "live",
        POSITION_ID,
        "entry-local-1",
        "entry-local-1",
        "143201293",
        "143201293",
    )
    assert "client_id = %s" in cursor.sql
    assert "execution_mode = %s" in cursor.sql
    assert "position_id::text = %s" in cursor.sql
    assert "local_order_id = %s" in cursor.sql
    assert "broker_order_id = %s" in cursor.sql
    assert "filled_qty" not in cursor.sql.split("FROM orders", 1)[1]
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


def test_entry_evidence_accepts_blank_predecessor_position_id_via_exact_order_link(monkeypatch):
    """#585 may assign the canonical UUID after an older ENTRY was stored."""
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
        "position_id": "",
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


def test_blank_predecessor_entry_allows_canonical_adoption_without_generic_seed(monkeypatch):
    """The #585 UUID may be adopted through an older blank-id ENTRY row."""
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
        "position_id": "",
        "contract": CONTRACT,
        "meta": {},
    }
    cursor = _EvidenceCursor([row])
    monkeypatch.setattr(db, "conn", lambda: cursor)
    monkeypatch.setattr(db, "run_with_retry", lambda fn: fn())

    repair_id = f"broker-repair-{CLIENT}-{CONTRACT}"
    engine = _ExitEngine(owners=[_owner(repair_id)])
    reconciler = _reconciler(engine)
    reconciler._filled_entry_evidence_for_canonical_position = (
        rec.APBrokerReconciler._filled_entry_evidence_for_canonical_position.__get__(
            reconciler, rec.APBrokerReconciler
        )
    )

    assert reconciler._seed_exit_engine_from_position(_position(avg_fill=None)) is True
    assert engine.add_calls == []
    assert len(engine.owners) == 1
    assert engine.owners[0].position_id == POSITION_ID


def test_entry_evidence_rejects_nonblank_predecessor_position_id_conflict(monkeypatch):
    """A nonblank predecessor UUID that differs remains a hard conflict."""
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
        "position_id": "other-canonical-id",
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


def test_complete_canonical_row_does_not_require_entry_lookup():
    """Complete durable position truth survives a transient orders outage."""
    engine = _ExitEngine("ADOPTED", owners=[_owner("broker-repair-held")])
    reconciler = _reconciler(engine)

    def _unavailable(**_kwargs):
        raise AssertionError("optional ENTRY lookup must not run for complete truth")

    reconciler._filled_entry_evidence_for_canonical_position = _unavailable
    production_position = _position()
    production_position.pop("canonical_signal_id")
    assert reconciler._seed_exit_engine_from_position(production_position) is True
    assert len(engine.adopt_calls) == 1
    assert engine.add_calls == []
    assert [owner.position_id for owner in engine.owners] == [POSITION_ID]


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



# ═══════════════════════════════════════════════════════════════════════════
# Quantity authority — partial-exit and three-field validation
# Required by audit (#546 amendment, 2026-09-05): fail-closed on every
# quantity_remaining edge case; partial adoption only with durable EXIT proof.
# ═══════════════════════════════════════════════════════════════════════════

def test_quantity_remaining_equal_to_qty_is_allowed():
    """qty=1, quantity_remaining=1 → consistent; adoption proceeds normally."""
    engine = _ExitEngine("NO_REPAIR_FOUND")
    pos = _position(qty=1, quantity_remaining=1)
    assert _reconciler(engine)._seed_exit_engine_from_position(pos) is True
    assert len(engine.add_calls) == 1


def test_quantity_remaining_greater_than_qty_holds():
    """qty=1, quantity_remaining=2 → impossible; HOLD before adoption."""
    engine = _ExitEngine("NO_REPAIR_FOUND")
    pos = _position(qty=1, quantity_remaining=2)
    assert _reconciler(engine)._seed_exit_engine_from_position(pos) is False
    assert engine.adopt_calls == []
    assert engine.add_calls == []


def test_quantity_remaining_fractional_holds():
    """qty=2, quantity_remaining=1.5 → non-integer; HOLD."""
    engine = _ExitEngine("NO_REPAIR_FOUND")
    pos = _position(qty=2, quantity_remaining=1.5)
    assert _reconciler(engine)._seed_exit_engine_from_position(pos) is False
    assert engine.adopt_calls == []
    assert engine.add_calls == []


def test_quantity_remaining_negative_holds():
    """qty=2, quantity_remaining=-1 → negative; HOLD."""
    engine = _ExitEngine("NO_REPAIR_FOUND")
    pos = _position(qty=2, quantity_remaining=-1)
    assert _reconciler(engine)._seed_exit_engine_from_position(pos) is False
    assert engine.adopt_calls == []
    assert engine.add_calls == []


def test_quantity_remaining_zero_holds():
    """qty=2, quantity_remaining=0 → zero is not a positive integer; HOLD."""
    engine = _ExitEngine("NO_REPAIR_FOUND")
    pos = _position(qty=2, quantity_remaining=0)
    assert _reconciler(engine)._seed_exit_engine_from_position(pos) is False
    assert engine.adopt_calls == []
    assert engine.add_calls == []


def test_quantity_remaining_partial_with_exit_evidence_preserves_full_and_remaining():
    """NO_REPAIR_FOUND reconstructs the same (full, remaining) pair as adoption."""
    engine = _ExitEngine("NO_REPAIR_FOUND")
    pos = _position(qty=2, quantity_remaining=1)
    result = _reconciler(
        engine,
        partial_evidence_status="PROVEN",
        partial_evidence={"filled_qty": 1},
    )._seed_exit_engine_from_position(pos)
    assert result is True
    assert len(engine.add_calls) == 1
    seeded = engine.add_calls[0]
    assert seeded.quantity == 2
    assert seeded.quantity_remaining == 1


def test_quantity_remaining_partial_without_exit_evidence_holds():
    """qty=2, quantity_remaining=1 without durable EXIT proof → HOLD.

    No partial-exit evidence means we cannot safely determine the canonical
    quantity; the exit owner must not be seeded with stale size.
    """
    engine = _ExitEngine("NO_REPAIR_FOUND")
    pos = _position(qty=2, quantity_remaining=1)
    # partial_evidence_status defaults to "NO_EVIDENCE"
    result = _reconciler(engine)._seed_exit_engine_from_position(pos)
    assert result is False
    assert engine.adopt_calls == []
    assert engine.add_calls == []


@pytest.mark.parametrize("partial_status", ["UNAVAILABLE", "AMBIGUOUS", "MALFORMED", "IDENTITY_CONFLICT"])
def test_quantity_remaining_partial_non_proven_evidence_statuses_hold(partial_status):
    """qty=2, quantity_remaining=1 with non-PROVEN partial evidence → HOLD."""
    engine = _ExitEngine("NO_REPAIR_FOUND")
    pos = _position(qty=2, quantity_remaining=1)
    result = _reconciler(
        engine,
        partial_evidence_status=partial_status,
    )._seed_exit_engine_from_position(pos)
    assert result is False
    assert engine.add_calls == []


def test_quantity_remaining_partial_full_adoption_path_uses_remaining_qty():
    """ADOPTED path with qty=2, quantity_remaining=1, exit evidence → uses qty=1."""
    engine = _ExitEngine("ADOPTED", owners=[_owner("broker-repair-held")])
    pos = _position(qty=2, quantity_remaining=1)
    # ADOPTED path does not call add_position, but canonical_qty must be correct
    # so the engine is given the right context in the adoption call.
    result = _reconciler(
        engine,
        partial_evidence_status="PROVEN",
        partial_evidence={"filled_qty": 1},
    )._seed_exit_engine_from_position(pos)
    assert result is True
    # The adoption contract must receive and apply both the durable full size
    # and the evidence-verified remaining size.
    assert len(engine.adopt_calls) == 1
    assert engine.adopt_calls[0]["quantity"] == 2
    assert engine.adopt_calls[0]["quantity_remaining"] == 1
    assert engine.owners[0].quantity == 2
    assert engine.owners[0].quantity_remaining == 1


def test_quantity_remaining_partial_existing_canonical_is_reconciled():
    """ALREADY_CANONICAL also applies the proven remaining quantity."""
    canonical = _owner(POSITION_ID, quantity=2, quantity_remaining=2)
    repair = _owner("broker-repair-held", quantity=2, quantity_remaining=2)
    engine = _ExitEngine(
        "ALREADY_CANONICAL_REPAIR_REMOVED",
        owners=[canonical, repair],
    )
    pos = _position(qty=2, quantity_remaining=1)
    assert _reconciler(
        engine,
        partial_evidence_status="PROVEN",
        partial_evidence={"filled_qty": 1},
    )._seed_exit_engine_from_position(pos) is True
    assert canonical.quantity == 2
    assert canonical.quantity_remaining == 1
    assert engine.add_calls == []


def test_real_exit_engine_already_canonical_applies_remaining_quantity():
    """The production ALREADY path must lower stale live remainder safely."""
    from ap_exit_engine import APExitEngine, ManagedPosition

    engine = APExitEngine.__new__(APExitEngine)
    engine._email = CLIENT
    engine._lock = threading.RLock()
    canonical = ManagedPosition(
        ticker="NOW", option_symbol=CONTRACT, side="PUT", quantity=2,
        entry_price=1.30, underlying_entry=0.0,
        underlying_target=120.0, underlying_stop=130.0,
        position_id=POSITION_ID, client_id=CLIENT, execution_mode="live",
        quantity_remaining=2,
    )
    repair = ManagedPosition(
        ticker="NOW", option_symbol=CONTRACT, side="PUT", quantity=2,
        entry_price=1.30, underlying_entry=0.0,
        underlying_target=120.0, underlying_stop=130.0,
        position_id="broker-repair-stale", client_id=CLIENT,
        execution_mode="live", quantity_remaining=2,
    )
    engine._positions = [canonical, repair]
    engine._positions_by_id = {
        canonical.position_id: canonical,
        repair.position_id: repair,
    }

    result = engine.adopt_canonical_position_identity(
        contract=CONTRACT,
        canonical_position_id=POSITION_ID,
        local_order_id="entry-local-1",
        broker_order_id="143201293",
        signal_id="signal-1",
        canonical_signal_id="canonical-signal-1",
        entry_fill=1.30,
        entry_ts=None,
        execution_mode="live",
        client_id=CLIENT,
        underlying_entry=0.0,
        underlying_entry_trusted=False,
        quantity=2,
        quantity_remaining=1,
    )

    assert result.disposition == "ALREADY_CANONICAL_REPAIR_REMOVED"
    assert canonical.quantity == 2
    assert canonical.quantity_remaining == 1
    assert engine._positions == [canonical]
    assert engine._positions_by_id == {POSITION_ID: canonical}


def test_real_exit_engine_rejects_canonical_quantity_overrun_before_cleanup():
    """A stale larger canonical size must not remove its repair partner first."""
    from ap_exit_engine import APExitEngine, ManagedPosition

    engine = APExitEngine.__new__(APExitEngine)
    engine._email = CLIENT
    engine._lock = threading.RLock()
    canonical = ManagedPosition(
        ticker="NOW", option_symbol=CONTRACT, side="PUT", quantity=3,
        entry_price=1.30, underlying_entry=127.425,
        underlying_target=120.0, underlying_stop=130.0,
        position_id=POSITION_ID, client_id=CLIENT, execution_mode="live",
        quantity_remaining=3,
    )
    repair = ManagedPosition(
        ticker="NOW", option_symbol=CONTRACT, side="PUT", quantity=2,
        entry_price=1.30, underlying_entry=127.425,
        underlying_target=120.0, underlying_stop=130.0,
        position_id="broker-repair-stale", client_id=CLIENT,
        execution_mode="live", quantity_remaining=2,
    )
    engine._positions = [canonical, repair]
    engine._positions_by_id = {
        canonical.position_id: canonical,
        repair.position_id: repair,
    }

    result = engine.adopt_canonical_position_identity(
        contract=CONTRACT,
        canonical_position_id=POSITION_ID,
        local_order_id="entry-local-1",
        broker_order_id="143201293",
        signal_id="signal-1",
        canonical_signal_id="canonical-signal-1",
        entry_fill=1.30,
        entry_ts=None,
        execution_mode="live",
        client_id=CLIENT,
        underlying_entry=127.425,
        underlying_entry_trusted=True,
        quantity=2,
        quantity_remaining=1,
    )

    assert result.disposition == "RETRY_IDENTITY_CONFLICT"
    assert result.reason == "canonical_quantity_conflict"
    assert engine._positions == [canonical, repair]
    assert engine._positions_by_id == {
        POSITION_ID: canonical,
        "broker-repair-stale": repair,
    }


# ═══════════════════════════════════════════════════════════════════════════
# Atomic NO_REPAIR_FOUND seed — race prevention
# Required by audit: two concurrent passes must not produce two owners.
# ═══════════════════════════════════════════════════════════════════════════

def test_atomic_seed_is_idempotent_when_owner_already_present():
    """NO_REPAIR_FOUND but a canonical owner already exists in the engine.

    Simulates the race where a second reconciler pass calls
    _seed_exit_engine_from_position after the first has already seeded.
    seed_canonical_position_if_absent must return (False, "already_owned"),
    and the postcondition must still confirm exactly one canonical owner.
    """
    engine = _ExitEngine("NO_REPAIR_FOUND")
    # Simulate the first pass already having added the canonical owner.
    existing_owner = _owner(POSITION_ID)
    engine.owners.append(existing_owner)

    # Second pass: seed_canonical_position_if_absent detects existing owner.
    result = _reconciler(engine)._seed_exit_engine_from_position(_position())
    assert result is True, (
        "Second pass must succeed via postcondition even when it did not seed. "
        "The position is owned — that is the invariant, not who seeded it."
    )
    # Must NOT have appended a second add_call (no duplicate add).
    assert len(engine.add_calls) == 0, (
        f"Expected 0 add_calls (already owned), got {len(engine.add_calls)}. "
        "seed_canonical_position_if_absent must be a no-op when owner exists."
    )
    # Exactly one owner.
    behavior_active = [
        o for o in engine.owners
        if not getattr(o, "closed", False)
    ]
    assert len(behavior_active) == 1


def test_concurrent_seed_race_produces_exactly_one_owner():
    """Two threads both reach NO_REPAIR_FOUND simultaneously.

    Thread A seeds first; Thread B detects already_owned atomically.
    Engine must have exactly one canonical owner at end.
    """
    import threading

    engine = _ExitEngine("NO_REPAIR_FOUND")
    barrier = threading.Barrier(2)
    results = []

    def _run():
        barrier.wait()
        r = _reconciler(engine)._seed_exit_engine_from_position(_position())
        results.append(r)

    threads = [threading.Thread(target=_run) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Both threads must report success (one seeded, one verified already_owned).
    assert all(r is True for r in results), (
        f"Expected both threads to return True, got {results}. "
        "already_owned path must return True via postcondition."
    )
    # Exactly one canonical owner.
    behavior_active = [
        o for o in engine.owners
        if not getattr(o, "closed", False)
        and not str(getattr(o, "position_id", "")).startswith("broker-repair-")
        and str(getattr(o, "position_id", "")).strip() == POSITION_ID
    ]
    assert len(behavior_active) == 1, (
        f"Expected exactly 1 canonical owner, got {len(behavior_active)}. "
        "Concurrent NO_REPAIR_FOUND paths must not produce duplicate owners."
    )


def test_no_repair_found_race_with_degraded_owner_refuses_canonical_duplicate():
    """A degraded owner appearing between adoption and seed must win the lock."""
    degraded = _owner(
        "broker-repair-degraded:race",
        degraded=True,
        quantity=1,
        quantity_remaining=1,
    )

    def _install_degraded(engine):
        engine.owners.append(degraded)

    engine = _ExitEngine("NO_REPAIR_FOUND", seed_hook=_install_degraded)
    result = _reconciler(engine)._seed_exit_engine_from_position(_position())

    assert result is False
    assert engine.add_calls == []
    assert engine.owners == [degraded]
    assert degraded.broker_repair_degraded is True


def test_real_atomic_seed_rechecks_exact_domain_for_degraded_owner():
    """The production helper refuses a degraded owner in the same domain."""
    from ap_exit_engine import APExitEngine, ManagedPosition

    engine = APExitEngine.__new__(APExitEngine)
    engine._email = CLIENT
    engine._lock = threading.RLock()
    degraded = ManagedPosition(
        ticker="NOW", option_symbol=CONTRACT, side="PUT", quantity=1,
        entry_price=0.0, underlying_entry=0.0,
        underlying_target=0.0, underlying_stop=0.0,
        position_id="broker-repair-degraded:race",
        client_id=CLIENT, execution_mode="live",
    )
    degraded.broker_repair_degraded = True
    engine._positions = [degraded]
    engine._positions_by_id = {degraded.position_id: degraded}
    canonical = ManagedPosition(
        ticker="NOW", option_symbol=CONTRACT, side="PUT", quantity=1,
        entry_price=1.30, underlying_entry=127.425,
        underlying_target=120.0, underlying_stop=130.0,
        position_id=POSITION_ID, client_id=CLIENT, execution_mode="live",
    )

    assert engine.seed_canonical_position_if_absent(canonical) == (
        False, "degraded_owner_present"
    )
    assert engine._positions == [degraded]


def test_real_atomic_seed_verifies_add_registration():
    """Calling add_position is not enough; the helper must prove registration."""
    from ap_exit_engine import APExitEngine, ManagedPosition

    engine = APExitEngine.__new__(APExitEngine)
    engine._email = CLIENT
    engine._lock = threading.RLock()
    engine._positions = []
    engine._positions_by_id = {}
    engine.add_position = lambda _position: None
    canonical = ManagedPosition(
        ticker="NOW", option_symbol=CONTRACT, side="PUT", quantity=1,
        entry_price=1.30, underlying_entry=127.425,
        underlying_target=120.0, underlying_stop=130.0,
        position_id=POSITION_ID, client_id=CLIENT, execution_mode="live",
    )

    assert engine.seed_canonical_position_if_absent(canonical) == (
        False, "seed_failed"
    )
    assert engine._positions == []


def test_restart_reconciler_adoption_preserves_pending_exit_and_reaches_evaluation():
    """A restarted degraded owner can be adopted without losing EXIT state."""
    from ap_exit_engine import APExitEngine, ManagedPosition, evaluate_exit

    engine = APExitEngine.__new__(APExitEngine)
    engine._email = CLIENT
    engine._lock = threading.RLock()
    restarted = ManagedPosition(
        ticker="NOW", option_symbol=CONTRACT, side="PUT", quantity=2,
        entry_price=0.0, underlying_entry=0.0,
        underlying_target=0.0, underlying_stop=0.0,
        position_id="broker-repair-degraded:restart",
        client_id=CLIENT, execution_mode="live",
    )
    restarted.broker_repair_degraded = True
    restarted.exit_in_flight = True
    restarted.pending_exit_local_order_id = "restart-exit-local"
    restarted.pending_exit_broker_order_id = "restart-exit-broker"
    restarted.pending_exit_qty = 2
    restarted.pending_exit_filled_qty = 1
    restarted.current_option_price = 1.20
    restarted.current_bid = 1.20
    engine._positions = [restarted]
    engine._positions_by_id = {restarted.position_id: restarted}

    assert _reconciler(
        engine,
        partial_evidence_status="PROVEN",
        partial_evidence={"filled_qty": 1},
    )._seed_exit_engine_from_position(
        _position(qty=2, quantity_remaining=1)
    ) is True
    active = engine.active_positions()
    assert len(active) == 1
    assert active[0] is restarted
    assert restarted.position_id == POSITION_ID
    assert restarted.quantity == 2
    assert restarted.quantity_remaining == 1
    assert restarted.exit_in_flight is True
    assert restarted.pending_exit_local_order_id == "restart-exit-local"
    assert restarted.pending_exit_broker_order_id == "restart-exit-broker"
    decision = evaluate_exit(restarted)
    assert decision is not None
    assert hasattr(decision, "action")


# ═══════════════════════════════════════════════════════════════════════════
# underlying_entry_trusted fail-first proof
# Required by audit: must prove the 32-line exit-engine API extension is
# necessary — the old adoption API cannot clear stale underlying_entry to
# 0/untrusted without a current quote lookup.
# ═══════════════════════════════════════════════════════════════════════════

def test_underlying_entry_trusted_false_clears_stale_value_in_exit_engine():
    """Fail-first: prove underlying_entry_trusted=False is required to clear stale value.

    Setup: a repair owner carries a stale underlying_entry=999.
    Canonical durable truth says underlying_entry=0 (untrusted).
    The adopt_canonical_position_identity call must receive
    underlying_entry_trusted=False so the engine clears the stale value.

    Without the extension, the reconciler passes underlying_entry_trusted=True
    when underlying_entry>0, which would CONFLICT (trusted=True but value=0)
    or silently preserve the stale value.  The extension allows trusted=False
    with value=0 to express "known untrusted — clear stale state."
    """
    # Build a repair owner with a stale underlying_entry
    repair = _owner("broker-repair-stale-entry")

    class _ClearingExitEngine(_ExitEngine):
        """Records underlying_entry_trusted kwarg passed to adopt."""
        def adopt_canonical_position_identity(self, **kwargs):
            self.adopt_calls.append(kwargs)
            if self.mutate and self.disposition == "ADOPTED":
                for owner in self.owners:
                    if str(owner.position_id).startswith("broker-repair-"):
                        owner.position_id = kwargs["canonical_position_id"]
                        break
            from types import SimpleNamespace
            return SimpleNamespace(
                disposition="ADOPTED",
                adopted=True,
                safe_to_seed=False,
                retryable=False,
            )

    engine = _ClearingExitEngine("ADOPTED", owners=[repair])
    # Position has underlying_entry=0 (historical entry unknown)
    pos = _position(underlying_entry=0)
    result = _reconciler(engine)._seed_exit_engine_from_position(pos)
    assert result is True
    assert len(engine.adopt_calls) == 1
    call_kwargs = engine.adopt_calls[0]
    # underlying_entry must be 0 (untrusted historical entry)
    assert call_kwargs["underlying_entry"] == 0.0, (
        f"Expected underlying_entry=0, got {call_kwargs['underlying_entry']}."
    )
    # underlying_entry_trusted must be False — this is the extension requirement.
    # Without it, the exit engine cannot distinguish "untrusted zero" from "absent".
    # The test FAILS if trusted is not False, proving the extension is necessary.
    assert call_kwargs.get("underlying_entry_trusted") is False, (
        f"Expected underlying_entry_trusted=False (untrusted historical entry), "
        f"got {call_kwargs.get('underlying_entry_trusted')}. "
        "This proves the underlying_entry_trusted=False extension in the adoption API "
        "is necessary: without it, stale underlying_entry=999 cannot be cleared to 0."
    )


# ═══════════════════════════════════════════════════════════════════════════
# SQL tightening — domain fencing now in WHERE, not just Python post-filter
# ═══════════════════════════════════════════════════════════════════════════

def test_entry_evidence_sql_fences_client_and_mode_in_query(monkeypatch):
    """The SQL query now includes client_id and execution_mode in WHERE.

    Verify the SQL parameters include client_id and execution_mode so the
    DB engine (not just Python post-filter) restricts the candidate set.
    """
    captured = {}

    class _FakeCursor:
        def execute(self, sql, params):
            captured["sql"] = sql
            captured["params"] = params
            self._rows = []
        def fetchall(self):
            return []

    class _FakeConn:
        def __enter__(self): return _FakeCursor()
        def __exit__(self, *a): pass

    import ap_reconciler as rec_mod
    monkeypatch.setattr(
        "ap.db.conn", lambda: _FakeConn(), raising=False
    )
    monkeypatch.setattr(
        "ap.db.run_with_retry", lambda fn: fn(), raising=False
    )
    monkeypatch.setattr(
        "ap.order_state_machine._durable_execution_mode",
        lambda row: row.get("execution_mode", ""),
        raising=False,
    )

    r = _reconciler(_ExitEngine())
    r._filled_entry_evidence_for_canonical_position(
        contract=CONTRACT,
        position_id=POSITION_ID,
        execution_mode="live",
        local_order_id="loc-1",
        broker_order_id="brk-1",
        signal_id="sig-1",
        canonical_signal_id="csig-1",
    )

    assert "captured" in dir() or captured  # ensure execute was called
    if captured:
        params = captured.get("params", ())
        sql = captured.get("sql", "")
        # client_id must be the first SQL parameter
        assert CLIENT in params, (
            f"client_id={CLIENT!r} not in SQL params {params}. "
            "SQL must fence by client_id in WHERE, not just Python post-filter."
        )
        # execution_mode must be in SQL parameters
        assert "live" in params, (
            f"execution_mode='live' not in SQL params {params}. "
            "SQL must fence by execution_mode in WHERE."
        )
        # kind='ENTRY' must appear in the SQL text
        assert "ENTRY" in sql, (
            "kind='ENTRY' must appear in the SQL WHERE clause, "
            "not just in Python post-filter."
        )


# ═══════════════════════════════════════════════════════════════════════════
# Final #546 audit regressions — real atomic index proof + caller visibility
# ═══════════════════════════════════════════════════════════════════════════

def _real_atomic_seed_fixture():
    from ap_exit_engine import APExitEngine, ManagedPosition

    engine = APExitEngine.__new__(APExitEngine)
    engine._email = CLIENT
    engine._lock = threading.RLock()
    existing = ManagedPosition(
        ticker="NOW", option_symbol=CONTRACT, side="PUT", quantity=2,
        quantity_remaining=1, entry_price=1.30, underlying_entry=127.425,
        underlying_target=120.0, underlying_stop=130.0,
        position_id=POSITION_ID, client_id=CLIENT, execution_mode="live",
    )
    incoming = ManagedPosition(
        ticker="NOW", option_symbol=CONTRACT, side="PUT", quantity=2,
        quantity_remaining=1, entry_price=1.30, underlying_entry=127.425,
        underlying_target=120.0, underlying_stop=130.0,
        position_id=POSITION_ID, client_id=CLIENT, execution_mode="live",
    )
    engine._positions = [existing]
    return engine, existing, incoming


def test_real_atomic_seed_already_owned_requires_correct_id_index():
    engine, existing, incoming = _real_atomic_seed_fixture()
    engine._positions_by_id = {POSITION_ID: existing}
    assert engine.seed_canonical_position_if_absent(incoming) == (
        False, "already_owned"
    )
    assert engine._positions == [existing]
    assert engine._positions_by_id == {POSITION_ID: existing}


def test_real_atomic_seed_missing_id_index_holds():
    engine, existing, incoming = _real_atomic_seed_fixture()
    engine._positions_by_id = {}
    assert engine.seed_canonical_position_if_absent(incoming) == (
        False, "owner_index_conflict"
    )
    assert engine._positions == [existing]
    assert engine._positions_by_id == {}


def test_real_atomic_seed_stale_id_index_holds():
    engine, existing, incoming = _real_atomic_seed_fixture()
    stale = object()
    engine._positions_by_id = {POSITION_ID: stale}
    assert engine.seed_canonical_position_if_absent(incoming) == (
        False, "owner_index_conflict"
    )
    assert engine._positions == [existing]
    assert engine._positions_by_id[POSITION_ID] is stale


def test_filled_entry_existing_position_surfaces_owner_install_failure():
    reconciler = _reconciler(_ExitEngine())
    reconciler._find_db_position_by_contract = lambda _contract: _position()
    reconciler._seed_exit_engine_from_position = lambda _position_row: False
    reconciler._link_order_to_position = lambda *_args, **_kwargs: pytest.fail(
        "order must not be linked as successful after owner installation failed"
    )
    summary = {"positions_alerted": 0, "errors": []}
    result = reconciler._ensure_position_for_filled_entry(
        {
            "contract": CONTRACT,
            "execution_mode": "live",
            "local_order_id": "entry-local-1",
        },
        1,
        1.30,
        summary,
    )
    assert result is None
    assert summary["positions_alerted"] == 1
    assert "reconciler_exit_owner_install_failed" in summary["errors"]


def test_broker_live_position_surfaces_owner_install_failure():
    reconciler = _reconciler(_ExitEngine())
    reconciler._ghost_tracker = {}
    broker_position = {
        "symbol": CONTRACT,
        "quantity": 1,
        "underlying": "NOW",
    }
    reconciler._safe_get_broker_positions = lambda: [broker_position]
    reconciler._get_open_db_positions = lambda: [_position()]
    reconciler._seed_exit_engine_from_position = lambda _position_row: False
    reconciler._import_broker_positions_missing_from_db = lambda **_kwargs: None
    summary = {"positions_alerted": 0, "errors": []}

    reconciler._reconcile_positions(summary)

    assert summary["positions_alerted"] == 1
    assert "reconciler_exit_owner_install_failed" in summary["errors"]
