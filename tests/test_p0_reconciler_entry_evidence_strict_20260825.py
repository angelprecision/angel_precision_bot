from __future__ import annotations

import ap.db as db
import ap_reconciler as rec


CLIENT = "jasoncosby1@gmail.com"
CONTRACT = "NOW260828P00122000"
POSITION_ID = "2fe10f52-e459-4bc5-a57a-1a80e3618040"
LOCAL_ORDER_ID = "5087bd52-287a-43b4-b3f3-c7d1b3298110"
BROKER_ORDER_ID = "143201293"
SIGNAL_ID = "f7dbe723-d342-406b-8650-fce428e9a0c0"
ENTRY_TS = "2026-08-25T13:54:06+00:00"
FILL_TS = "2026-08-25T13:54:01.682835+00:00"


class _Broker:
    execution_mode = "live"

    def get_quote(self, symbol):
        # Later market truth. It must never be re-labeled as historical entry truth.
        return {"last": 126.62, "bid": 126.60, "ask": 126.64}


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


def _order_row(*, local_order_id=LOCAL_ORDER_ID, filled_ts=FILL_TS):
    return {
        "position_id": None,
        "local_order_id": local_order_id,
        "broker_order_id": BROKER_ORDER_ID,
        "signal_id": SIGNAL_ID,
        "canonical_signal_id": SIGNAL_ID,
        "fill_price": 1.30,
        "filled_qty": 1,
        "filled_ts": filled_ts,
        "stop_underlying": 130.44,
        "target_underlying": 124.78,
        "meta": {"underlying_entry": 127.425},
    }


def test_generic_current_and_trigger_prices_are_not_immutable_entry_truth():
    value, source = rec._immutable_underlying_from_entry_meta(
        {
            "current_underlying_price": 127.425,
            "underlying_price": 127.425,
            "trigger": {"current_price": 127.425},
            "zero_underlying_repair": {
                "value": 127.425,
                "source": "selector_result.candidate_audit.underlying_price",
                "repaired": True,
            },
        }
    )

    assert value == 0.0
    assert source == ""


def test_explicit_entry_underlying_metadata_is_accepted():
    value, source = rec._immutable_underlying_from_entry_meta(
        {"underlying_entry": 127.425, "current_underlying_price": 126.62}
    )

    assert value == 127.425
    assert source == "meta.underlying_entry"


def test_trigger_price_column_is_not_redefined_as_underlying_entry(monkeypatch):
    reconciler = _reconciler()
    monkeypatch.setattr(reconciler, "_filled_entry_evidence", lambda *a, **k: None)

    value = reconciler._derive_underlying_entry_from_position(
        {
            "id": POSITION_ID,
            "entry_ts": ENTRY_TS,
            "underlying_entry": None,
            "trigger_price": 127.61,
        },
        underlying="NOW",
        contract=CONTRACT,
    )

    assert value == 0.0


class _DBState:
    def __init__(self, unlinked_rows):
        self.unlinked_rows = list(unlinked_rows)
        self.executions = []


class _Conn:
    def __init__(self, state: _DBState):
        self.state = state
        self.sql = ""
        self.params = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params):
        self.sql = str(sql)
        self.params = tuple(params)
        self.state.executions.append((self.sql, self.params))

    def fetchone(self):
        # Exact position-linked lookup misses during the short post-fill link race.
        return None

    def fetchall(self):
        return list(self.state.unlinked_rows)


def _install_fake_db(monkeypatch, rows):
    state = _DBState(rows)
    monkeypatch.setattr(db, "conn", lambda: _Conn(state))
    monkeypatch.setattr(db, "run_with_retry", lambda fn: fn())
    return state


def test_unique_unlinked_fill_within_position_time_window_recovers_incident_race(monkeypatch):
    state = _install_fake_db(monkeypatch, [_order_row()])
    reconciler = _reconciler()

    evidence = reconciler._filled_entry_evidence(
        CONTRACT,
        position_id=POSITION_ID,
        position_entry_ts=ENTRY_TS,
    )

    assert evidence is not None
    assert evidence["local_order_id"] == LOCAL_ORDER_ID
    assert evidence["broker_order_id"] == BROKER_ORDER_ID
    assert evidence["underlying_entry"] == 127.425
    assert len(state.executions) == 2
    linked_sql, linked_params = state.executions[0]
    unlinked_sql, unlinked_params = state.executions[1]
    assert "COALESCE(position_id, '') = %s" in linked_sql
    assert linked_params[-1] == POSITION_ID
    assert "COALESCE(position_id, '') = ''" in unlinked_sql
    assert unlinked_params == (CLIENT, "live", CONTRACT)


def test_multiple_unlinked_fills_fail_closed_instead_of_cross_linking(monkeypatch):
    _install_fake_db(
        monkeypatch,
        [
            _order_row(local_order_id="candidate-a", filled_ts="2026-08-25T13:54:01+00:00"),
            _order_row(local_order_id="candidate-b", filled_ts="2026-08-25T13:54:03+00:00"),
        ],
    )
    reconciler = _reconciler()

    evidence = reconciler._filled_entry_evidence(
        CONTRACT,
        position_id=POSITION_ID,
        position_entry_ts=ENTRY_TS,
    )

    assert evidence is None


def test_unlinked_fill_outside_five_minute_window_is_not_adopted(monkeypatch):
    _install_fake_db(
        monkeypatch,
        [_order_row(filled_ts="2026-08-25T13:40:00+00:00")],
    )
    reconciler = _reconciler()

    evidence = reconciler._filled_entry_evidence(
        CONTRACT,
        position_id=POSITION_ID,
        position_entry_ts=ENTRY_TS,
    )

    assert evidence is None


def test_unlinked_fallback_requires_canonical_position_time(monkeypatch):
    state = _install_fake_db(monkeypatch, [_order_row()])
    reconciler = _reconciler()

    evidence = reconciler._filled_entry_evidence(
        CONTRACT,
        position_id=POSITION_ID,
        position_entry_ts=None,
    )

    assert evidence is None
    # Only exact linked lookup ran; unsafe unlinked fallback was not queried.
    assert len(state.executions) == 1
