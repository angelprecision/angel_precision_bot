import os
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@127.0.0.1:5432/test")
from datetime import datetime, timezone
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


class _PostgresConn:
    def __init__(self, connection):
        self.connection = connection
        self.cursor = None

    def __enter__(self):
        import psycopg2.extras
        self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        return self.cursor

    def __exit__(self, exc_type, exc, tb):
        if exc_type:
            self.connection.rollback()
        else:
            self.connection.commit()
        self.cursor.close()
        return False


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


@pytest.mark.parametrize("key", ["last_fill_date", "filled_at", "filled_ts", "fill_ts"])
def test_parser_accepts_each_exact_fill_timestamp_key(key):
    assert _extract_broker_fill_timestamp({key: FILL_TS}) == FILL_TS


@pytest.mark.parametrize(
    "raw",
    [
        {"transaction_date": FILL_TS},
        {"updated_at": FILL_TS},
        {"last_fill_date": "2026-09-10T14:35:28"},
        {"filled_at": "not-a-timestamp"},
    ],
    ids=["transaction-date-only", "updated-at-only", "naive", "malformed"],
)
def test_parser_rejects_non_exact_or_invalid_timestamp_authority(raw):
    assert _extract_broker_fill_timestamp(raw) is None


def test_parser_rejects_conflicting_exact_timestamps():
    assert _extract_broker_fill_timestamp({
        "last_fill_date": FILL_TS,
        "filled_at": "2026-09-10T14:35:29+00:00",
    }) is None


def test_parser_accepts_equivalent_exact_timestamp_offsets():
    assert _extract_broker_fill_timestamp({
        "last_fill_date": FILL_TS,
        "filled_at": "2026-09-10T07:35:28-07:00",
    }) == FILL_TS

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


def test_hood_incident_replay_with_exact_fill_timestamp_reaches_osm():
    o = FakeOSM(); r = rec(o); s = summary()
    r._advance_order_to_broker_fill(
        order(status="EXIT_ACKNOWLEDGED"),
        fill(id="145345180", status="filled", filled_qty=1, avg_fill_price=1.19),
        "filled",
        s,
    )
    assert len(o.calls) == 1
    assert o.calls[0][2]["broker_order_id"] == "145345180"
    assert o.calls[0][2]["filled_ts"] == FILL_TS
    assert s["orders_corrected"] == 1


def test_hood_incident_replay_without_exact_fill_timestamp_holds_before_osm():
    o = FakeOSM(); r = rec(o); s = summary()
    raw = fill(id="145345180", status="filled", filled_qty=1, avg_fill_price=1.19)
    raw.pop("last_fill_date")
    raw["transaction_date"] = FILL_TS
    r._advance_order_to_broker_fill(order(status="EXIT_ACKNOWLEDGED"), raw, "filled", s)
    assert o.calls == []
    assert s["orders_corrected"] == 0
    assert s["orders_alerted"] == 1
    assert "EXIT_FILL_TIMESTAMP_MISSING_OR_INVALID" in r.alerts[-1]

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


def test_missing_id_recent_fill_transaction_date_only_stays_quarantined():
    o = FakeOSM(); r = rec(o); s = summary()
    r._recover_missing_broker_id_exit = lambda order, summary: False
    r._get_recent_exit_fill = lambda c, u: {
        "filled_qty": 1,
        "fill_price": 1.19,
        "broker_order_id": "145345180",
        "transaction_date": FILL_TS,
    }
    assert r._resolve_missing_id_exit_truth(order(broker_order_id=""), s, reason="test") is False
    assert o.calls == []
    assert s["orders_alerted"] == 1

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


@pytest.mark.parametrize("status", [OrderStatus.EXIT_FILLED, OrderStatus.EXIT_PARTIAL_FILL])
@pytest.mark.parametrize(
    "bad_ts",
    [None, "2026-09-10T14:35:28", "not-a-timestamp"],
    ids=["missing", "naive", "malformed"],
)
def test_osm_positive_exit_statuses_hold_before_db_for_invalid_timestamp(
    monkeypatch, status, bad_ts
):
    o = APOrderStateMachine.__new__(APOrderStateMachine)
    o.client_id = "jason-test@example.com"
    o._get_order = lambda _: order(status="EXIT_ACKNOWLEDGED", filled_qty=0)
    events = []
    o._emit_transition_event = lambda **k: events.append(k)
    monkeypatch.setattr(
        osm_mod,
        "run_with_retry",
        lambda fn: pytest.fail("DB write reached before timestamp guard"),
    )
    ok = o.transition(
        "exit-local-1",
        status,
        broker_order_id="145345180",
        filled_qty=1,
        fill_price=1.19,
        filled_ts=bad_ts,
    )
    assert ok is False
    assert events[-1]["reason_code"] == "EXIT_FILL_TIMESTAMP_MISSING_OR_INVALID"
    assert events[-1]["extra_inputs"] == {
        "action": "HOLD",
        "position_mutated": False,
        "order_terminalized": False,
    }


def test_osm_exact_exit_timestamp_reaches_guarded_db_path(monkeypatch):
    o = APOrderStateMachine.__new__(APOrderStateMachine)
    o.client_id = "jason-test@example.com"
    o._get_order = lambda _: order(status="EXIT_ACKNOWLEDGED", filled_qty=0)
    o._emit_transition_event = lambda **k: None
    o._notify_opportunity_ledger = lambda **k: None
    o._handle_exit_engine_hooks = lambda **k: True
    db_calls = []
    monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn: db_calls.append(fn) or 1)
    assert o.transition(
        "exit-local-1",
        OrderStatus.EXIT_FILLED,
        broker_order_id="145345180",
        filled_qty=1,
        fill_price=1.19,
        filled_ts="2026-09-10T07:35:28-07:00",
    ) is True
    assert db_calls


def test_entry_filled_missing_timestamp_keeps_historical_db_path(monkeypatch):
    o = APOrderStateMachine.__new__(APOrderStateMachine)
    o.client_id = "jason-test@example.com"
    o._get_order = lambda _: order(
        kind="ENTRY", status="ACKNOWLEDGED", broker_order_id="", filled_qty=0
    )
    o._emit_transition_event = lambda **k: None
    o._notify_opportunity_ledger = lambda **k: None
    o._handle_exit_engine_hooks = lambda **k: True
    db_calls = []
    monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn: db_calls.append(fn) or 1)
    assert o.transition(
        "entry-local-1",
        OrderStatus.FILLED,
        broker_order_id="entry-broker-1",
        filled_qty=1,
        fill_price=1.19,
        filled_ts=None,
    ) is True
    assert db_calls


def test_apply_osm_fill_update_preserves_identity_and_timestamp():
    class ApplyOSM:
        def __init__(self):
            self.kwargs = None
        def apply_fill_update(self, local_order_id, **kwargs):
            self.kwargs = {"local_order_id": local_order_id, **kwargs}
            return True

    osm = ApplyOSM(); r = rec(osm)
    assert r._apply_osm_fill_update(
        "exit-local-1",
        "EXIT_PARTIAL_FILL",
        1,
        1.19,
        broker_order_id="145345180",
        filled_ts=FILL_TS,
    ) is True
    assert osm.kwargs["broker_order_id"] == "145345180"
    assert osm.kwargs["filled_ts"] == FILL_TS


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


def test_postgres_missing_exit_timestamp_leaves_durable_row_unchanged(monkeypatch):
    psycopg2 = pytest.importorskip("psycopg2")
    dsn = os.environ.get("P0_POSTGRES_DSN") or os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("disposable PostgreSQL DSN not configured")
    try:
        connection = psycopg2.connect(dsn, connect_timeout=2)
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL unavailable for durable OSM proof: {exc}")

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TEMP TABLE orders (
                    local_order_id TEXT PRIMARY KEY,
                    client_id TEXT NOT NULL,
                    position_id TEXT,
                    kind TEXT,
                    status TEXT,
                    filled_qty INTEGER,
                    fill_price DOUBLE PRECISION,
                    filled_ts TIMESTAMPTZ,
                    broker_order_id TEXT,
                    updated_ts TIMESTAMPTZ,
                    last_error TEXT
                ) ON COMMIT PRESERVE ROWS
                """
            )
            cursor.execute(
                """
                INSERT INTO orders (
                    local_order_id, client_id, position_id, kind, status,
                    filled_qty, fill_price, filled_ts, broker_order_id, updated_ts
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """,
                (
                    "exit-local-1",
                    "jason-test@example.com",
                    "position-1",
                    "EXIT",
                    "EXIT_ACKNOWLEDGED",
                    0,
                    None,
                    None,
                    "145345180",
                ),
            )
        connection.commit()

        monkeypatch.setattr(osm_mod, "conn", lambda: _PostgresConn(connection))
        monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn: fn())
        osm = APOrderStateMachine.__new__(APOrderStateMachine)
        osm.client_id = "jason-test@example.com"
        osm._emit_transition_event = lambda **kwargs: None
        osm._notify_opportunity_ledger = lambda **kwargs: None
        osm._handle_exit_engine_hooks = lambda **kwargs: True

        assert osm.transition(
            "exit-local-1",
            OrderStatus.EXIT_FILLED,
            broker_order_id="145345180",
            filled_qty=1,
            fill_price=1.19,
            filled_ts=None,
        ) is False

        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT status, filled_qty, fill_price, filled_ts "
                "FROM orders WHERE local_order_id=%s",
                ("exit-local-1",),
            )
            unchanged = cursor.fetchone()
        assert unchanged == ("EXIT_ACKNOWLEDGED", 0, None, None)

        assert osm.transition(
            "exit-local-1",
            OrderStatus.EXIT_FILLED,
            broker_order_id="145345180",
            filled_qty=1,
            fill_price=1.19,
            filled_ts=FILL_TS,
        ) is True

        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT status, filled_qty, fill_price, filled_ts "
                "FROM orders WHERE local_order_id=%s",
                ("exit-local-1",),
            )
            advanced = cursor.fetchone()
        assert advanced[0:3] == ("EXIT_FILLED", 1, 1.19)
        assert advanced[3].astimezone(timezone.utc).isoformat() == FILL_TS
    finally:
        connection.close()
