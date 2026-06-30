from __future__ import annotations

import os
import sys
from pathlib import Path

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
os.environ.setdefault("AP_ENV", "test")

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))


class _Resp:
    status_code = 200
    text = ""

    def json(self):
        return {"quotes": {"quote": {"last": 99.25}}}


class _Session:
    def __init__(self):
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append((url, params, headers, timeout))
        return _Resp()


class _Broker:
    def __init__(self, raw):
        self.raw = raw
        self.cfg = type("Cfg", (), {"base_url": "https://execution.example"})()
        self.session = _Session()

    def get_order(self, broker_order_id):
        return dict(self.raw)

    def get_quote(self, symbol):
        return {"last": 0}


class _DataBroker:
    def __init__(self):
        self.cfg = type("Cfg", (), {"base_url": "https://data.example"})()
        self.session = _Session()

    def get_quote(self, symbol):
        return {"last": 101.5}


class _PM:
    def __init__(self):
        self.kwargs = None

    def open_position(self, **kwargs):
        self.kwargs = kwargs
        return "pos-1"


class _ExitEngine:
    def __init__(self):
        self.added = []

    def add_position(self, mp):
        self.added.append(mp)


def _base_order(**overrides):
    order = {
        "client_id": "client@example.com",
        "local_order_id": "ord-1",
        "broker_order_id": "brk-1",
        "kind": "ENTRY",
        "symbol": "AAPL",
        "contract": "AAPL260626P00195000",
        "direction": None,
        "qty": 1,
        "limit_price": 1.20,
        "reserved_cost": 120.0,
        "status": "ACKNOWLEDGED",
        "plan_id": "plan-1",
        "signal_id": "sig-1",
        "tier": "B",
        "score": 75,
        "pattern": "daily",
        "trigger_price": None,
        "filled_qty": 0,
        "fill_price": None,
    }
    order.update(overrides)
    return order


def test_resolves_put_from_occ_contract_when_direction_missing(monkeypatch):
    from ap import fill_monitor as fm

    monkeypatch.setattr(fm._legacy, "audit", lambda *a, **k: None)
    monkeypatch.setattr(fm._legacy, "emit_fill_event", lambda *a, **k: None)

    order = _base_order(direction=None, contract="AAPL260626P00195000")
    broker = _Broker({"status": "filled", "exec_quantity": 1, "avg_fill_price": 1.05})

    result = fm.check_order_with_broker(broker, order)

    assert result["status"] == "FILLED"
    assert order["direction"] == "PUT"
    assert order["_fill_monitor_side_source"] == "occ_contract"


def test_broker_filled_zero_qty_is_error_not_filled(monkeypatch):
    from ap import fill_monitor as fm

    events = []
    monkeypatch.setattr(fm._legacy, "audit", lambda *a, **k: events.append(("audit", a, k)))
    monkeypatch.setattr(fm._legacy, "emit_fill_event", lambda *a, **k: events.append(("event", a, k)))

    order = _base_order(direction="CALL", contract="AAPL260626C00195000")
    broker = _Broker({"status": "filled", "quantity": 0, "avg_fill_price": 1.05})

    result = fm.check_order_with_broker(broker, order)

    assert result["status"] == "ERROR"
    assert result["reason"] == "BROKER_FILLED_ZERO_QTY"
    assert result["filled_qty"] == 0
    assert events


def test_release_symbol_lock_even_when_reserved_cost_missing(monkeypatch):
    from ap import fill_monitor as fm

    released_equity = []
    released_locks = []
    monkeypatch.setattr(fm, "release_equity", lambda *a: released_equity.append(a))
    monkeypatch.setattr(fm, "release_symbol_lock", lambda *a: released_locks.append(a))

    fm._release_entry_guards(
        _base_order(symbol="TSLA", reserved_cost=None, limit_price=None, qty=None)
    )

    assert released_equity == []
    assert released_locks == [("client@example.com", "TSLA")]


def test_open_position_uses_data_broker_for_underlying_entry(monkeypatch):
    from ap import fill_monitor as fm

    monkeypatch.setattr(fm._legacy, "audit", lambda *a, **k: None)
    monkeypatch.setattr(fm, "_record_filled_side_effect_failure", lambda *a, **k: None)

    pm = _PM()
    execution_broker = _Broker({"status": "filled", "exec_quantity": 1, "avg_fill_price": 1.10})
    data_broker = _DataBroker()
    order = _base_order(
        direction=None,
        contract="AAPL260626C00195000",
        trigger_price=None,
        underlying_entry=None,
        entry_underlying=None,
        last_underlying_price=None,
    )

    position_id = fm._open_position_safe(
        pm,
        order=order,
        result={"filled_qty": 1, "avg_fill": 1.10},
        plan_id="plan-1",
        signal_id="sig-1",
        local_id="ord-1",
        broker=execution_broker,
        quote_broker=data_broker,
    )

    assert position_id == "pos-1"
    assert pm.kwargs["side"] == "CALL"
    assert pm.kwargs["underlying_entry"] == 101.5


def test_exit_engine_seed_uses_occ_resolved_put(monkeypatch):
    from ap import fill_monitor as fm

    class _MP:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    monkeypatch.setattr(fm._legacy, "_load_managed_position_class", lambda: _MP)
    monkeypatch.setattr(fm._legacy, "audit", lambda *a, **k: None)

    ee = _ExitEngine()
    fm._seed_exit_engine(
        ee,
        "pos-1",
        _base_order(direction=None, contract="AAPL260626P00195000"),
        {"filled_qty": 1, "avg_fill": 1.10},
        "sig-1",
    )

    assert len(ee.added) == 1
    assert ee.added[0].side == "PUT"
    assert ee.added[0].signal["side"] == "PUT"
