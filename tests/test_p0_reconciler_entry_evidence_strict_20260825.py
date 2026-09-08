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


def _order_row(
    *,
    local_order_id=LOCAL_ORDER_ID,
    filled_ts=FILL_TS,
    position_id=None,
    client_id=CLIENT,
    execution_mode="live",
    contract=CONTRACT,
):
    return {
        "position_id": position_id,
        "client_id": client_id,
        "execution_mode": execution_mode,
        "contract": contract,
        "kind": "ENTRY",
        "status": "FILLED",
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
    def __init__(self, unlinked_rows, linked_rows=None):
        self.unlinked_rows = list(unlinked_rows)
        self.linked_rows = list(linked_rows or [])
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
        rows = (
            self.state.linked_rows
            if "position_id = %s" in self.sql
            else self.state.unlinked_rows
        )
        expected_client, expected_mode, expected_contract = self.params[:3]
        filtered = [
            row for row in rows
            if row.get("client_id") == expected_client
            and row.get("execution_mode") == expected_mode
            and row.get("contract") == expected_contract
        ]
        if "position_id = %s" in self.sql:
            expected_position_id = self.params[-1]
            return [row for row in filtered if row.get("position_id") == expected_position_id]
        return [row for row in filtered if not str(row.get("position_id") or "").strip()]


def _install_fake_db(monkeypatch, rows, *, linked_rows=None):
    state = _DBState(rows, linked_rows=linked_rows)
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
    assert "position_id = %s" in linked_sql
    assert linked_params[-1] == POSITION_ID
    assert "position_id IS NULL OR position_id = ''" in unlinked_sql
    assert unlinked_params == (CLIENT, "live", CONTRACT)


def test_exact_position_linked_fill_is_accepted(monkeypatch):
    state = _install_fake_db(
        monkeypatch,
        [],
        linked_rows=[_order_row(position_id=POSITION_ID)],
    )
    reconciler = _reconciler()

    evidence = reconciler._filled_entry_evidence(
        CONTRACT,
        position_id=POSITION_ID,
        position_entry_ts=None,
    )

    assert evidence is not None
    assert evidence["position_id"] == POSITION_ID
    assert len(state.executions) == 1


def test_fill_linked_to_different_position_is_rejected(monkeypatch):
    state = _install_fake_db(
        monkeypatch,
        [_order_row(position_id="different-position")],
    )
    reconciler = _reconciler()

    evidence = reconciler._filled_entry_evidence(
        CONTRACT,
        position_id=POSITION_ID,
        position_entry_ts=ENTRY_TS,
    )

    assert evidence is None
    assert len(state.executions) == 2


def test_client_mode_and_contract_mismatch_do_not_donate_evidence(monkeypatch):
    row = _order_row()
    state = _install_fake_db(monkeypatch, [], linked_rows=[row])
    reconciler = _reconciler()

    for field, value, query_contract in (
        ("client_id", "other-client", CONTRACT),
        ("execution_mode", "paper", CONTRACT),
        ("contract", "OTHER260828P00122000", CONTRACT),
    ):
        row[field] = value
        evidence = reconciler._filled_entry_evidence(
            query_contract,
            position_id=POSITION_ID,
            position_entry_ts=ENTRY_TS,
        )
        assert evidence is None
        state.executions.clear()
        row[field] = _order_row()[field]


def test_malformed_execution_mode_fails_closed_before_query(monkeypatch):
    state = _install_fake_db(monkeypatch, [], linked_rows=[_order_row(position_id=POSITION_ID)])
    reconciler = _reconciler()
    reconciler.execution_mode = "sandbox"

    evidence = reconciler._filled_entry_evidence(
        CONTRACT,
        position_id=POSITION_ID,
        position_entry_ts=ENTRY_TS,
    )

    assert evidence is None
    assert state.executions == []


def test_multiple_position_linked_fills_fail_closed(monkeypatch):
    _install_fake_db(
        monkeypatch,
        [_order_row(local_order_id="unlinked-candidate")],
        linked_rows=[
            _order_row(local_order_id="linked-a", position_id=POSITION_ID),
            _order_row(local_order_id="linked-b", position_id=POSITION_ID),
        ],
    )
    reconciler = _reconciler()

    evidence = reconciler._filled_entry_evidence(
        CONTRACT,
        position_id=POSITION_ID,
        position_entry_ts=ENTRY_TS,
    )

    assert evidence is None


def test_unscoped_filled_entry_lookup_is_blocked(monkeypatch):
    state = _install_fake_db(monkeypatch, [_order_row()])
    reconciler = _reconciler()

    evidence = reconciler._filled_entry_evidence(CONTRACT)

    assert evidence is None
    assert state.executions == []


def test_unproven_fill_scalars_fail_closed(monkeypatch):
    for field, value in (
        ("broker_order_id", None),
        ("fill_price", float("inf")),
        ("filled_qty", 1.5),
        ("filled_ts", None),
    ):
        row = _order_row()
        row[field] = value
        _install_fake_db(monkeypatch, [], linked_rows=[row])
        reconciler = _reconciler()

        evidence = reconciler._filled_entry_evidence(
            CONTRACT,
            position_id=POSITION_ID,
            position_entry_ts=ENTRY_TS,
        )

        assert evidence is None, field


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
