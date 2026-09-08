from __future__ import annotations

import sys
from types import SimpleNamespace

import ap_reconciler as rec


class _FakeManagedPosition:
    """Minimal ManagedPosition stand-in for tests that cannot import ap_exit_engine
    (no DATABASE_URL in local environment).  Accepts the exact kwargs that
    _seed_exit_engine_from_import passes to ManagedPosition() and supports every
    attribute assignment the method makes afterwards.
    """

    def __init__(
        self,
        ticker="",
        option_symbol="",
        side="",
        quantity=0,
        entry_price=0.0,
        underlying_entry=0.0,
        underlying_target=0.0,
        underlying_stop=0.0,
    ):
        self.ticker = ticker
        self.option_symbol = option_symbol
        self.side = side
        self.quantity = quantity
        self.entry_price = entry_price
        self.underlying_entry = underlying_entry
        self.underlying_target = underlying_target
        self.underlying_stop = underlying_stop
        # Post-construction attributes set by the reconciler:
        self.position_id = ""
        self.client_id = ""
        self.signal_id = ""
        self.canonical_signal_id = ""
        self.execution_mode = ""
        self.current_option_price = 0.0
        self.price_untrusted = False
        self.underlying_entry_untrusted = False
        self.imported_by_reconciler = False
        self.entry_local_order_id = ""
        self.entry_broker_order_id = ""


CLIENT = "jasoncosby1@gmail.com"
CONTRACT = "NOW260828P00122000"
POSITION_ID = "2fe10f52-e459-4bc5-a57a-1a80e3618040"
LOCAL_ORDER_ID = "5087bd52-287a-43b4-b3f3-c7d1b3298110"
BROKER_ORDER_ID = "143201293"
SIGNAL_ID = "f7dbe723-d342-406b-8650-fce428e9a0c0"


class _Broker:
    execution_mode = "live"

    def get_quote(self, symbol):
        # Deliberately different from the real entry. Recovery must not use it.
        return {"last": 126.62, "bid": 126.60, "ask": 126.64}


class _ExitEngine:
    def __init__(self, disposition="ADOPTED", result=None):
        self.disposition = disposition
        self.result = result
        self.adopt_calls = []
        self.added = []

    def adopt_canonical_position_identity(self, **kwargs):
        self.adopt_calls.append(kwargs)
        if self.result is not None:
            return self.result
        if self.disposition == "ADOPTED":
            return SimpleNamespace(
                disposition="ADOPTED", adopted=True, retryable=False, safe_to_seed=False
            )
        if self.disposition == "NO_REPAIR_FOUND":
            return SimpleNamespace(
                disposition="NO_REPAIR_FOUND", adopted=False, retryable=False, safe_to_seed=True
            )
        return SimpleNamespace(
            disposition=self.disposition, adopted=False, retryable=True, safe_to_seed=False
        )

    def add_position(self, position):
        self.added.append(position)


class _OSM:
    pass


class _PM:
    pass


def _reconciler():
    return rec.APBrokerReconciler(
        broker=_Broker(),
        client_id=CLIENT,
        osm=_OSM(),
        pm=_PM(),
        execution_mode="live",
    )


def _evidence():
    return {
        "position_id": POSITION_ID,
        "local_order_id": LOCAL_ORDER_ID,
        "broker_order_id": BROKER_ORDER_ID,
        "signal_id": SIGNAL_ID,
        "canonical_signal_id": SIGNAL_ID,
        "fill_price": 1.30,
        "filled_qty": 1,
        "filled_ts": "2026-08-25T13:54:01.682835+00:00",
        "stop_underlying": 130.44,
        "target_underlying": 124.78,
        "underlying_entry": 127.425,
        "underlying_entry_source": "meta.underlying_entry",
    }


def _actual_exit_engine_with_repair():
    from ap_exit_engine import APExitEngine, ManagedPosition

    engine = APExitEngine(_Broker(), email=CLIENT)
    repair = ManagedPosition(
        ticker="NOW",
        option_symbol=CONTRACT,
        side="PUT",
        quantity=1,
        entry_price=1.30,
        underlying_entry=0.0,
        underlying_target=124.78,
        underlying_stop=130.44,
    )
    repair.position_id = f"broker-repair-{CLIENT}-NOW"
    repair.client_id = CLIENT
    repair.execution_mode = "live"
    repair.entry_broker_order_id = BROKER_ORDER_ID
    engine.add_position(repair)
    return engine, repair


def _canonical_row():
    return {
        "id": POSITION_ID,
        "client_id": CLIENT,
        "execution_mode": "live",
        "underlying": "NOW",
        "contract": CONTRACT,
        "direction": "PUT",
        "quantity_remaining": 1,
        "avg_fill": 1.30,
        "underlying_entry": None,
        "stop_underlying": 130.44,
        "target_underlying": 124.78,
    }


def test_real_exit_engine_collapses_now_repair_and_repeated_ticks_keep_one_owner(monkeypatch):
    reconciler = _reconciler()
    engine, repair = _actual_exit_engine_with_repair()
    reconciler.exit_engine = engine
    monkeypatch.setattr(reconciler, "_filled_entry_evidence", lambda *a, **k: _evidence())

    reconciler._seed_exit_engine_from_position(_canonical_row())
    reconciler._seed_exit_engine_from_position(_canonical_row())

    active = engine.active_positions()
    assert len(active) == 1
    assert active[0] is repair
    assert active[0].position_id == POSITION_ID
    assert active[0].client_id == CLIENT
    assert active[0].execution_mode == "live"
    assert active[0].option_symbol == CONTRACT
    assert active[0].entry_local_order_id == LOCAL_ORDER_ID
    assert active[0].entry_broker_order_id == BROKER_ORDER_ID
    assert active[0].signal_id == SIGNAL_ID
    assert active[0].canonical_signal_id == SIGNAL_ID
    assert active[0].entry_price == 1.30
    assert active[0].opened_at.isoformat() == "2026-08-25T13:54:01.682835+00:00"
    assert active[0].underlying_entry == 127.425


def test_real_exit_engine_removes_repair_when_canonical_owner_already_exists(monkeypatch):
    reconciler = _reconciler()
    engine, repair = _actual_exit_engine_with_repair()
    from ap_exit_engine import ManagedPosition

    canonical = ManagedPosition(
        ticker="NOW",
        option_symbol=CONTRACT,
        side="PUT",
        quantity=1,
        entry_price=1.30,
        underlying_entry=127.425,
        underlying_target=124.78,
        underlying_stop=130.44,
    )
    canonical.position_id = POSITION_ID
    canonical.client_id = CLIENT
    canonical.execution_mode = "live"
    engine._positions.insert(0, canonical)
    engine._positions_by_id[POSITION_ID] = canonical
    # Simulate two independently hydrated owners before the collapse call.
    engine._positions.append(repair)
    engine._positions_by_id[repair.position_id] = repair
    reconciler.exit_engine = engine
    monkeypatch.setattr(reconciler, "_filled_entry_evidence", lambda *a, **k: _evidence())

    reconciler._seed_exit_engine_from_position(_canonical_row())

    active = engine.active_positions()
    assert len(active) == 1
    assert active[0] is canonical
    assert repair not in active


def test_missing_db_underlying_entry_does_not_move_to_current_market(monkeypatch):
    reconciler = _reconciler()
    monkeypatch.setattr(reconciler, "_filled_entry_evidence", lambda *a, **k: None)
    monkeypatch.setattr(reconciler, "_get_current_underlying_price", lambda symbol: 126.62)

    value = reconciler._derive_underlying_entry_from_position(
        {"id": POSITION_ID, "underlying_entry": None},
        underlying="NOW",
        contract=CONTRACT,
    )

    # This is the incident regression: old code returned the current quote here.
    assert value == 0.0


def test_filled_entry_evidence_restores_immutable_now_anchor(monkeypatch):
    reconciler = _reconciler()
    monkeypatch.setattr(reconciler, "_filled_entry_evidence", lambda *a, **k: _evidence())
    monkeypatch.setattr(reconciler, "_get_current_underlying_price", lambda symbol: 126.62)

    value = reconciler._derive_underlying_entry_from_position(
        {"id": POSITION_ID, "underlying_entry": None},
        underlying="NOW",
        contract=CONTRACT,
    )

    assert value == 127.425
    assert value != 126.62


def test_reconciler_uses_canonical_adoption_before_generic_add(monkeypatch):
    reconciler = _reconciler()
    exit_engine = _ExitEngine("ADOPTED")
    reconciler.exit_engine = exit_engine
    monkeypatch.setattr(reconciler, "_filled_entry_evidence", lambda *a, **k: _evidence())

    reconciler._seed_exit_engine_from_position(
        {
            "id": POSITION_ID,
            "client_id": CLIENT,
            "execution_mode": "live",
            "underlying": "NOW",
            "contract": CONTRACT,
            "direction": "PUT",
            "quantity_remaining": 1,
            "qty": 1,
            "avg_fill": 1.30,
            "underlying_entry": None,
            "stop_underlying": 130.44,
            "target_underlying": 124.78,
        }
    )

    assert len(exit_engine.adopt_calls) == 1
    assert exit_engine.added == []
    call = exit_engine.adopt_calls[0]
    assert call["canonical_position_id"] == POSITION_ID
    assert call["client_id"] == CLIENT
    assert call["execution_mode"] == "live"
    assert call["contract"] == CONTRACT
    assert call["local_order_id"] == LOCAL_ORDER_ID
    assert call["broker_order_id"] == BROKER_ORDER_ID
    assert call["underlying_entry"] == 127.425
    assert call["underlying_stop"] == 130.44
    assert call["underlying_target"] == 124.78


def test_no_repair_found_seeds_one_canonical_owner(monkeypatch):
    reconciler = _reconciler()
    exit_engine = _ExitEngine("NO_REPAIR_FOUND")
    reconciler.exit_engine = exit_engine
    monkeypatch.setattr(reconciler, "_filled_entry_evidence", lambda *a, **k: _evidence())
    monkeypatch.setitem(
        sys.modules,
        "ap_exit_engine",
        SimpleNamespace(ManagedPosition=_FakeManagedPosition),
    )

    reconciler._seed_exit_engine_from_position(
        {
            "id": POSITION_ID,
            "client_id": CLIENT,
            "underlying": "NOW",
            "contract": CONTRACT,
            "direction": "PUT",
            "quantity_remaining": 1,
            "avg_fill": 1.30,
            "execution_mode": "live",
        }
    )

    assert len(exit_engine.adopt_calls) == 1
    assert len(exit_engine.added) == 1
    mp = exit_engine.added[0]
    assert mp.position_id == POSITION_ID
    assert mp.client_id == CLIENT
    assert mp.execution_mode == "live"
    assert mp.underlying_entry == 127.425
    assert mp.underlying_entry_untrusted is False
    assert mp.entry_local_order_id == LOCAL_ORDER_ID
    assert mp.entry_broker_order_id == BROKER_ORDER_ID


def test_retry_adoption_fails_closed_without_second_owner(monkeypatch):
    reconciler = _reconciler()
    exit_engine = _ExitEngine("RETRY_IDENTITY_CONFLICT")
    reconciler.exit_engine = exit_engine
    monkeypatch.setattr(reconciler, "_filled_entry_evidence", lambda *a, **k: _evidence())

    reconciler._seed_exit_engine_from_position(
        {
            "id": POSITION_ID,
            "client_id": CLIENT,
            "underlying": "NOW",
            "contract": CONTRACT,
            "direction": "PUT",
            "quantity_remaining": 1,
            "avg_fill": 1.30,
            "execution_mode": "live",
        }
    )

    assert len(exit_engine.adopt_calls) == 1
    assert exit_engine.added == []


def test_unknown_adoption_disposition_fails_closed_without_second_owner(monkeypatch):
    reconciler = _reconciler()
    exit_engine = _ExitEngine(
        result=SimpleNamespace(
            disposition="UNKNOWN_SELECTOR_RECOVERY_FAILURE",
            adopted=False,
            retryable=False,
            safe_to_seed=True,
        )
    )
    reconciler.exit_engine = exit_engine
    monkeypatch.setattr(reconciler, "_filled_entry_evidence", lambda *a, **k: _evidence())

    reconciler._seed_exit_engine_from_position(
        {
            "id": POSITION_ID,
            "client_id": CLIENT,
            "underlying": "NOW",
            "contract": CONTRACT,
            "direction": "PUT",
            "quantity_remaining": 1,
            "avg_fill": 1.30,
            "execution_mode": "live",
        }
    )

    assert len(exit_engine.adopt_calls) == 1
    assert exit_engine.added == []


def test_malformed_adoption_result_fails_closed_without_second_owner(monkeypatch):
    reconciler = _reconciler()
    exit_engine = _ExitEngine(
        result=SimpleNamespace(
            disposition="NO_REPAIR_FOUND",
            adopted=False,
            retryable=False,
        )
    )
    reconciler.exit_engine = exit_engine
    monkeypatch.setattr(reconciler, "_filled_entry_evidence", lambda *a, **k: _evidence())

    reconciler._seed_exit_engine_from_position(
        {
            "id": POSITION_ID,
            "client_id": CLIENT,
            "underlying": "NOW",
            "contract": CONTRACT,
            "direction": "PUT",
            "quantity_remaining": 1,
            "avg_fill": 1.30,
            "execution_mode": "live",
        }
    )

    assert len(exit_engine.adopt_calls) == 1
    assert exit_engine.added == []


def test_client_or_mode_mismatch_fails_closed_before_adoption(monkeypatch):
    reconciler = _reconciler()
    exit_engine = _ExitEngine("NO_REPAIR_FOUND")
    reconciler.exit_engine = exit_engine
    monkeypatch.setattr(reconciler, "_filled_entry_evidence", lambda *a, **k: _evidence())

    for field, value in (("client_id", "other-client"), ("execution_mode", "paper")):
        row = {
            "id": POSITION_ID,
            "client_id": CLIENT,
            "underlying": "NOW",
            "contract": CONTRACT,
            "direction": "PUT",
            "quantity_remaining": 1,
            "avg_fill": 1.30,
            "execution_mode": "live",
        }
        row[field] = value
        reconciler._seed_exit_engine_from_position(row)

    assert exit_engine.adopt_calls == []
    assert exit_engine.added == []


def test_missing_adoption_api_fails_closed_without_generic_add(monkeypatch):
    reconciler = _reconciler()

    class _NoAdoptionEngine:
        def __init__(self):
            self.added = []

        def add_position(self, position):
            self.added.append(position)

    exit_engine = _NoAdoptionEngine()
    reconciler.exit_engine = exit_engine
    monkeypatch.setattr(reconciler, "_filled_entry_evidence", lambda *a, **k: _evidence())

    reconciler._seed_exit_engine_from_position(
        {
            "id": POSITION_ID,
            "client_id": CLIENT,
            "underlying": "NOW",
            "contract": CONTRACT,
            "direction": "PUT",
            "quantity_remaining": 1,
            "avg_fill": 1.30,
            "execution_mode": "live",
        }
    )

    assert exit_engine.added == []


def test_missing_entry_truth_stays_untrusted_instead_of_fabricated(monkeypatch):
    reconciler = _reconciler()
    exit_engine = _ExitEngine("NO_REPAIR_FOUND")
    reconciler.exit_engine = exit_engine
    monkeypatch.setattr(reconciler, "_filled_entry_evidence", lambda *a, **k: None)
    monkeypatch.setattr(reconciler, "_get_current_underlying_price", lambda symbol: 126.62)
    monkeypatch.setitem(
        sys.modules,
        "ap_exit_engine",
        SimpleNamespace(ManagedPosition=_FakeManagedPosition),
    )

    reconciler._seed_exit_engine_from_import(
        pos_id=POSITION_ID,
        contract=CONTRACT,
        underlying="NOW",
        side="PUT",
        qty=1,
        entry_px=1.30,
        stop_underlying=130.44,
        target_underlying=124.78,
        underlying_entry=0.0,
        price_untrusted=False,
    )

    assert len(exit_engine.added) == 1
    mp = exit_engine.added[0]
    assert mp.underlying_entry == 0.0
    assert mp.underlying_entry_untrusted is True
    assert exit_engine.adopt_calls[0]["underlying_entry"] == 0.0
