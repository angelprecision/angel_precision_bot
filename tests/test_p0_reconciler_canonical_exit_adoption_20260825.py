from __future__ import annotations

from types import SimpleNamespace

import ap_reconciler as rec


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
    def __init__(self, disposition="ADOPTED"):
        self.disposition = disposition
        self.adopt_calls = []
        self.added = []

    def adopt_canonical_position_identity(self, **kwargs):
        self.adopt_calls.append(kwargs)
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

    reconciler._seed_exit_engine_from_position(
        {
            "id": POSITION_ID,
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


def test_missing_entry_truth_stays_untrusted_instead_of_fabricated(monkeypatch):
    reconciler = _reconciler()
    exit_engine = _ExitEngine("NO_REPAIR_FOUND")
    reconciler.exit_engine = exit_engine
    monkeypatch.setattr(reconciler, "_filled_entry_evidence", lambda *a, **k: None)
    monkeypatch.setattr(reconciler, "_get_current_underlying_price", lambda symbol: 126.62)

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
