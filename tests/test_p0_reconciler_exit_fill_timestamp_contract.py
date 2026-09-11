import os
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@127.0.0.1:5432/test")
from datetime import datetime
from types import SimpleNamespace
import pytest
from ap.brokers.tradier import TradierBroker
import ap.fill_monitor as fill_monitor_mod
from ap.fill_monitor import check_order_with_broker
from ap.manual_close_reconciliation import (
    BROKER_ORDER_UPDATED_AT_KEY,
    BROKER_ORDER_UPDATED_AT_SOURCE,
    FILL_TIMESTAMP_QUALITY_ORDER_UPDATE_ONLY,
)
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

def raw_tradier_order(**kw):
    x = {
        "id": "145345180",
        "status": "filled",
        "exec_quantity": 2,
        "avg_fill_price": 1.19,
        "remaining_quantity": 0,
        "transaction_date": "2026-09-10T14:35:58.000Z",
    }
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


def test_parser_accepts_explicit_timezone_aware_fill_timestamp():
    ts = _extract_broker_fill_timestamp(fill())
    assert ts and datetime.fromisoformat(ts).utcoffset() is not None


def test_raw_tradier_transaction_date_is_not_execution_timestamp():
    assert _extract_broker_fill_timestamp(raw_tradier_order()) is None


def test_raw_tradier_documented_order_shape_flows_to_osm_as_nonexact_fill():
    o = FakeOSM(); r = rec(o); s = summary()
    r._advance_order_to_broker_fill(
        order(qty=5), raw_tradier_order(remaining_quantity=3), "filled", s
    )

    assert len(o.calls) == 1
    _, status, evidence = o.calls[0]
    assert status == "EXIT_FILLED"
    assert evidence["filled_qty"] == 2
    assert evidence["fill_price"] == 1.19
    assert evidence["filled_ts"] is None
    assert evidence["fill_timestamp_quality"] == FILL_TIMESTAMP_QUALITY_ORDER_UPDATE_ONLY
    assert evidence["fill_timestamp_source"] == BROKER_ORDER_UPDATED_AT_SOURCE
    assert evidence["fill_timestamp_key"] == BROKER_ORDER_UPDATED_AT_KEY
    assert evidence["broker_order_updated_at"]
    assert s["orders_corrected"] == 1
    assert s["orders_alerted"] == 0


def test_tradier_get_order_preserves_documented_raw_fill_fields():
    raw = raw_tradier_order()
    broker = TradierBroker.__new__(TradierBroker)
    broker.cfg = SimpleNamespace(account_id="ACCOUNT-605")
    broker._get = lambda path: {"order": raw}

    fetched = broker.get_order("145345180")
    assert fetched == raw
    assert "last_fill_date" not in fetched
    assert fetched["exec_quantity"] == 2
    assert fetched["avg_fill_price"] == 1.19
    assert fetched["transaction_date"] == "2026-09-10T14:35:58.000Z"

    result = check_order_with_broker(broker, order(qty=5))
    assert result["status"] == "EXIT_FILLED"
    assert result["filled_qty"] == 2
    assert result["avg_fill"] == 1.19
    assert result["filled_ts"] is None
    assert result["fill_timestamp_quality"] == FILL_TIMESTAMP_QUALITY_ORDER_UPDATE_ONLY
    assert result["fill_timestamp_source"] == BROKER_ORDER_UPDATED_AT_SOURCE
    assert result["fill_timestamp_key"] == BROKER_ORDER_UPDATED_AT_KEY
    assert result["broker_order_updated_at"]


def test_fill_monitor_never_uses_tradier_limit_price_as_execution_price(monkeypatch):
    monkeypatch.setattr(fill_monitor_mod, "audit", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        fill_monitor_mod,
        "emit_fill_event",
        lambda *args, **kwargs: None,
    )
    raw = raw_tradier_order(price=9.99)
    raw.pop("avg_fill_price")
    result = check_order_with_broker(
        SimpleNamespace(get_order=lambda _broker_id: raw),
        order(qty=5),
    )

    assert result["status"] == "ERROR"
    assert result["reason"] == "BROKER_FILLED_INVALID_PRICE"
    assert result["filled_qty"] == 2
    assert result["avg_fill"] == 0.0


@pytest.mark.parametrize("broker_status", ["partially_filled", "canceled", "rejected", "expired"])
def test_raw_tradier_partial_or_terminal_transaction_date_converges_before_mutation(
    broker_status,
):
    o = FakeOSM(); r = rec(o); s = summary()
    raw = raw_tradier_order(status=broker_status, remaining_quantity=3)

    if broker_status == "partially_filled":
        r._advance_order_to_broker_fill(order(qty=5), raw, broker_status, s)
    else:
        r._advance_order_to_terminal(
            order(qty=5, position_id=None), broker_status, s, broker_raw=raw
        )

    assert o.calls
    assert o.calls[0][2]["filled_ts"] is None
    assert o.calls[0][2]["fill_timestamp_quality"] == FILL_TIMESTAMP_QUALITY_ORDER_UPDATE_ONLY
    assert o.calls[0][2]["fill_timestamp_source"] == BROKER_ORDER_UPDATED_AT_SOURCE
    assert o.calls[0][2]["fill_timestamp_key"] == BROKER_ORDER_UPDATED_AT_KEY
    assert o.calls[0][2]["broker_order_updated_at"]
    if broker_status == "partially_filled":
        assert [status for _, status, _ in o.calls] == ["EXIT_PARTIAL_FILL"]
    else:
        assert [status for _, status, _ in o.calls] == [
            "EXIT_PARTIAL_FILL",
            {
                "canceled": "CANCELED",
                "rejected": "REJECTED",
                "expired": "EXPIRED",
            }[broker_status],
        ]
    assert s["orders_corrected"] == 1
    assert s["orders_alerted"] == 0

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


@pytest.mark.parametrize("broker_status", ["canceled", "cancelled", "rejected", "expired"])
def test_terminal_exit_fill_without_timestamp_holds_before_terminalization(
    broker_status,
):
    o = FakeOSM(); r = rec(o); s = summary()
    evidence = fill(status=broker_status); evidence.pop("last_fill_date")
    row = order(position_id=None)

    r._advance_order_to_terminal(row, broker_status, s, broker_raw=evidence)

    assert o.calls == []
    assert s["orders_corrected"] == 0
    assert s["orders_alerted"] == 1
    assert "EXIT_FILL_TIMESTAMP_MISSING_OR_INVALID" in r.alerts[-1]


@pytest.mark.parametrize("broker_status", ["canceled", "cancelled", "rejected", "expired"])
def test_terminal_exit_fill_converges_before_terminalization(broker_status):
    o = FakeOSM(); r = rec(o); s = summary()
    row = order(position_id=None, qty=5)
    evidence = fill(status=broker_status, filled_qty=2)

    r._advance_order_to_terminal(row, broker_status, s, broker_raw=evidence)

    assert [status for _, status, _ in o.calls] == [
        "EXIT_PARTIAL_FILL",
        {
            "canceled": "CANCELED",
            "cancelled": "CANCELED",
            "rejected": "REJECTED",
            "expired": "EXPIRED",
        }[broker_status],
    ]
    partial_kwargs = o.calls[0][2]
    assert partial_kwargs["filled_qty"] == 2
    assert partial_kwargs["fill_price"] == 1.19
    assert partial_kwargs["broker_order_id"] == "145345180"
    assert partial_kwargs["filled_ts"]
    assert s["orders_corrected"] == 1


def test_terminal_exit_fill_holds_when_preconvergence_osm_refuses():
    o = FakeOSM(result=False); r = rec(o); s = summary()
    evidence = fill(status="canceled", filled_qty=2)

    r._advance_order_to_terminal(
        order(position_id=None, qty=5), "canceled", s, broker_raw=evidence
    )

    assert [status for _, status, _ in o.calls] == ["EXIT_PARTIAL_FILL"]
    assert s["orders_corrected"] == 0
    assert s["orders_alerted"] == 1
    assert "exit_terminal_fill_osm_held" in s["errors"]


def test_stale_ack_terminal_exit_fill_without_timestamp_holds_before_osm(monkeypatch):
    stale = order(submitted_ts=FILL_TS, updated_ts=FILL_TS, age_sec=30.0)
    _install_stale_rows(monkeypatch, [stale])
    raw = fill(status="canceled"); raw.pop("last_fill_date")
    o = FakeOSM(); r = rec(o); s = summary()
    r.broker = _BrokerOrder(raw)

    r._handle_stale_acknowledged_exits(s)

    assert o.calls == []
    assert s["orders_corrected"] == 0
    assert s["orders_alerted"] == 1
    assert "EXIT_FILL_TIMESTAMP_MISSING_OR_INVALID" in r.alerts[-1]


def test_stale_ack_terminal_exit_fill_converges_before_terminalization(monkeypatch):
    stale = order(submitted_ts=FILL_TS, updated_ts=FILL_TS, age_sec=30.0)
    _install_stale_rows(monkeypatch, [stale])
    o = FakeOSM(); r = rec(o); s = summary()
    r.broker = _BrokerOrder(fill(status="canceled", filled_qty=1))

    r._handle_stale_acknowledged_exits(s)

    assert [status for _, status, _ in o.calls] == ["EXIT_PARTIAL_FILL", "CANCELED"]
    assert o.calls[0][2]["filled_ts"]
    assert s["orders_corrected"] == 1
    assert s["orders_alerted"] == 0


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
