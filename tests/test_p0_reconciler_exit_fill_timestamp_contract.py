import os
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@127.0.0.1:5432/test")
from datetime import datetime
import pytest
import ap.order_state_machine as osm_mod
from ap.order_state_machine import APOrderStateMachine, OrderStatus
from ap_reconciler import APBrokerReconciler, _extract_broker_fill_timestamp

FILL_TS = "2026-09-10T14:35:28+00:00"

class FakeOSM:
    def __init__(self, result=True):
        self.calls, self.result = [], result
    def transition(self, local_order_id, status, **kwargs):
        self.calls.append((local_order_id, status, kwargs))
        return self.result

def rec(osm=None):
    r = APBrokerReconciler.__new__(APBrokerReconciler)
    r.client_id = "jason-test@example.com"
    r.osm = osm or FakeOSM()
    r.pm = None
    r.exit_engine = None
    r._missing_id_exit_tracker = {}
    r.alerts = []
    r._alert = r.alerts.append
    return r

def order(**kw):
    x = {"local_order_id":"exit-local-1","broker_order_id":"145345180",
         "position_id":"position-1","kind":"EXIT","status":"EXIT_ACKNOWLEDGED",
         "contract":"HOOD260911P00113000","symbol":"HOOD","qty":1,"filled_qty":0}
    x.update(kw)
    return x

def fill(**kw):
    x = {"id":"145345180","status":"filled","filled_qty":1,
         "avg_fill_price":1.19,"last_fill_date":FILL_TS}
    x.update(kw)
    return x

def summary():
    return {"orders_corrected":0,"orders_alerted":0,"errors":[],"positions_imported":0}

class _RowsConn:
    def __init__(self, rows):
        self.rows = rows
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc, tb):
        return False
    def execute(self, *args, **kwargs):
        return self
    def fetchall(self):
        return self.rows


class _BrokerOrder:
    def __init__(self, raw):
        self.raw = raw
    def get_order(self, broker_order_id):
        return dict(self.raw)


def _install_stale_rows(monkeypatch, rows):
    import ap.db as db
    monkeypatch.setattr(db, "conn", lambda: _RowsConn(rows))
    monkeypatch.setattr(db, "run_with_retry", lambda fn: fn())


def test_parser_accepts_exact_tradier_fill_timestamp():
    ts = _extract_broker_fill_timestamp(fill())
    assert ts and datetime.fromisoformat(ts).utcoffset() is not None

def test_normal_exit_fill_propagates_timestamp_and_identity():
    o = FakeOSM(); r = rec(o); s = summary()
    r._advance_order_to_broker_fill(order(), fill(), "filled", s)
    assert len(o.calls) == 1
    _, status, k = o.calls[0]
    assert status == "EXIT_FILLED"
    assert k["filled_qty"] == 1 and k["fill_price"] == 1.19
    assert k["broker_order_id"] == "145345180"
    assert datetime.fromisoformat(k["filled_ts"]).utcoffset() is not None
    assert s["orders_corrected"] == 1

def test_normal_exit_fill_without_timestamp_holds_before_osm():
    o = FakeOSM(); r = rec(o); s = summary(); raw = fill(); raw.pop("last_fill_date")
    r._advance_order_to_broker_fill(order(), raw, "filled", s)
    assert o.calls == []
    assert s["orders_corrected"] == 0 and s["orders_alerted"] == 1
    report = r.alerts[-1]
    for token in ("EXIT_FILL_TIMESTAMP_MISSING_OR_INVALID", "client_id=jason-test@example.com",
                  "broker_order_id=145345180", "position_id=position-1", "contract=HOOD260911P00113000",
                  "action=HOLD", "position_mutated=false", "order_terminalized=false"):
        assert token in report

def test_missing_id_recent_fill_without_timestamp_never_terminalizes():
    o = FakeOSM(); r = rec(o); s = summary()
    r._recover_missing_broker_id_exit = lambda order, summary: False
    r._get_recent_exit_fill = lambda c, u: {"filled_qty":1,"fill_price":1.19,"broker_order_id":"145345180"}
    assert r._resolve_missing_id_exit_truth(order(broker_order_id=""), s, reason="test") is False
    assert o.calls == []

def test_missing_id_recent_fill_passes_timestamp_and_broker_id():
    o = FakeOSM(); r = rec(o); s = summary()
    r._recover_missing_broker_id_exit = lambda order, summary: False
    r._get_recent_exit_fill = lambda c, u: {"filled_qty":1,"fill_price":1.19,"filled_ts":FILL_TS,"broker_order_id":"145345180"}
    assert r._resolve_missing_id_exit_truth(order(broker_order_id=""), s, reason="test") is True
    _, status, k = o.calls[0]
    assert status == "EXIT_FILLED"
    assert k["broker_order_id"] == "145345180" and k["filled_ts"]

def test_osm_missing_timestamp_rejected_before_db_write(monkeypatch):
    o = APOrderStateMachine.__new__(APOrderStateMachine)
    o.client_id = "jason-test@example.com"
    o._get_order = lambda _: order()
    events = []
    o._emit_transition_event = lambda **k: events.append(k)
    o._record_error = lambda *a, **k: None
    monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn: pytest.fail("DB write reached before timestamp guard"))
    ok = o.transition("exit-local-1", OrderStatus.EXIT_FILLED,
                      broker_order_id="145345180", filled_qty=1, fill_price=1.19, filled_ts=None)
    assert ok is False
    assert events[-1]["decision"] == "HOLD"
    assert events[-1]["reason_code"] == "EXIT_FILL_TIMESTAMP_MISSING_OR_INVALID"
    assert events[-1]["extra_inputs"]["order_terminalized"] is False
def test_stale_ack_exit_fill_propagates_exact_timestamp(monkeypatch):
    stale = order(submitted_ts=FILL_TS, updated_ts=FILL_TS, age_sec=30.0)
    _install_stale_rows(monkeypatch, [stale])
    o = FakeOSM(); r = rec(o); s = summary()
    r.broker = _BrokerOrder(fill())
    r._handle_stale_acknowledged_exits(s)
    assert len(o.calls) == 1
    _, status, k = o.calls[0]
    assert status == "EXIT_FILLED"
    assert k["broker_order_id"] == "145345180"
    assert datetime.fromisoformat(k["filled_ts"]).utcoffset() is not None
    assert s["orders_corrected"] == 1
    assert s["orders_alerted"] == 0


def test_stale_ack_exit_fill_without_timestamp_holds_before_osm(monkeypatch):
    stale = order(submitted_ts=FILL_TS, updated_ts=FILL_TS, age_sec=30.0)
    _install_stale_rows(monkeypatch, [stale])
    raw = fill(); raw.pop("last_fill_date")
    o = FakeOSM(); r = rec(o); s = summary()
    r.broker = _BrokerOrder(raw)
    r._handle_stale_acknowledged_exits(s)
    assert o.calls == []
    assert s["orders_corrected"] == 0
    assert s["orders_alerted"] == 1
    assert "EXIT_FILL_TIMESTAMP_MISSING_OR_INVALID" in r.alerts[-1]


def test_stale_ack_terminal_only_counts_corrected_when_osm_accepts(monkeypatch):
    stale = order(submitted_ts=FILL_TS, updated_ts=FILL_TS, age_sec=30.0)
    _install_stale_rows(monkeypatch, [stale])
    o = FakeOSM(result=False); r = rec(o); s = summary()
    r.broker = _BrokerOrder({"id": "145345180", "status": "canceled"})
    r._handle_stale_acknowledged_exits(s)
    assert len(o.calls) == 1
    assert s["orders_corrected"] == 0
    assert s["orders_alerted"] == 1
    assert "stale_ack_exit_osm_terminal_held" in s["errors"]


def test_osm_exit_specific_state_enforces_timestamp_even_if_kind_missing(monkeypatch):
    o = APOrderStateMachine.__new__(APOrderStateMachine)
    o.client_id = "jason-test@example.com"
    o._get_order = lambda _: order(kind=None, status="EXIT_ACKNOWLEDGED")
    events = []
    o._emit_transition_event = lambda **k: events.append(k)
    o._record_error = lambda *a, **k: None
    monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn: pytest.fail("DB write reached before timestamp guard"))
    ok = o.transition(
        "exit-local-1",
        OrderStatus.EXIT_FILLED,
        broker_order_id="145345180",
        filled_qty=1,
        fill_price=1.19,
        filled_ts=None,
    )
    assert ok is False
    assert events[-1]["decision"] == "HOLD"
    assert events[-1]["reason_code"] == "EXIT_FILL_TIMESTAMP_MISSING_OR_INVALID"
