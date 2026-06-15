from __future__ import annotations

import copy
import os
import sys
import types

import pytest

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_p0_startup_watcher_reseed",
)
os.environ.setdefault("ENCRYPTION_KEY", "angel-startup-watcher-reseed-test-key-2026")

if "supabase" not in sys.modules:
    _supa_stub = types.ModuleType("supabase")
    _supa_stub.create_client = lambda *a, **kw: None
    _supa_stub.Client = type("Client", (), {})
    sys.modules["supabase"] = _supa_stub


@pytest.fixture
def recovery_mod():
    import ap_recovery
    return ap_recovery


class _FakeDbState:
    def __init__(self, trade_queue_rows: list[dict], order_rows: list[dict]):
        self.trade_queue_rows = trade_queue_rows
        self.order_rows = order_rows


class _FakeCursor:
    def __init__(self, state: _FakeDbState):
        self.state = state
        self.rowcount = 0
        self._rows = []

    def execute(self, sql: str, params: tuple):
        if "UPDATE trade_queue" in sql:
            client_id, cutoff_utc = params
            cutoff_text = str(cutoff_utc)
            count = 0
            for row in self.state.trade_queue_rows:
                if row.get("client_id") != client_id:
                    continue
                if row.get("status") != "WATCHING":
                    continue
                if str(row.get("created_ts")) < cutoff_text:
                    continue
                has_live_pending = any(
                    str(o.get("client_id")) == client_id
                    and str(o.get("signal_id")) == str(row.get("signal_id"))
                    and str(o.get("kind")) == "ENTRY"
                    and str(o.get("status")) == "PENDING_TRIGGER"
                    and not str(o.get("broker_order_id") or "").strip()
                    and o.get("submitted_ts") is None
                    and o.get("filled_ts") is None
                    for o in self.state.order_rows
                )
                if has_live_pending:
                    continue
                row["status"] = "NEW"
                count += 1
            self.rowcount = count
            self._rows = []
            return self

        if "FROM orders" in sql and "status = 'PENDING_TRIGGER'" in sql:
            client_id, cutoff_utc = params
            cutoff_text = str(cutoff_utc)
            self._rows = [
                copy.deepcopy(o)
                for o in self.state.order_rows
                if str(o.get("client_id")) == client_id
                and str(o.get("kind")) == "ENTRY"
                and str(o.get("status")) == "PENDING_TRIGGER"
                and str(o.get("created_ts")) >= cutoff_text
                and not str(o.get("broker_order_id") or "").strip()
                and o.get("submitted_ts") is None
                and o.get("filled_ts") is None
            ]
            self.rowcount = len(self._rows)
            return self

        raise AssertionError(f"Unexpected SQL in watcher reseed test: {sql}")

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, state: _FakeDbState):
        self.state = state

    def __enter__(self):
        return _FakeCursor(self.state)

    def __exit__(self, exc_type, exc, tb):
        return False


def test_reseed_watchers_rearms_orphaned_pending_trigger_deferred_order(monkeypatch, recovery_mod):
    import ap.db as ap_db

    state = _FakeDbState(
        trade_queue_rows=[],
        order_rows=[
            {
                "local_order_id": "local-avgo-1",
                "signal_id": "sig-avgo-1",
                "plan_id": "plan-avgo-1",
                "client_id": "client-1",
                "kind": "ENTRY",
                "status": "PENDING_TRIGGER",
                "created_ts": "2099-06-15T12:00:00+00:00",
                "submitted_ts": None,
                "filled_ts": None,
                "broker_order_id": None,
                "symbol": "AVGO",
                "contract": "DEFERRED:AVGO",
                "direction": "CALL",
                "score": 78.0,
                "tier": "A",
                "trigger_price": 210.0,
                "stop_underlying": 205.0,
                "target_underlying": 220.0,
                "pattern": "2-1-2",
                "timeframe": "1d",
                "meta": {
                    "signal_id": "sig-avgo-1",
                    "plan_id": "plan-avgo-1",
                    "symbol": "AVGO",
                    "direction": "CALL",
                    "score": 78.0,
                    "tier": "A",
                    "signal_entry_price": 210.0,
                    "timeframe": "1d",
                    "pattern": "2-1-2",
                    "prior_day_high": 210.0,
                },
            }
        ],
    )

    watcher = types.SimpleNamespace()
    watcher.calls = []
    watcher._last_reject_reason = None
    watcher.has_order = lambda local_order_id: False

    def _watch(plan, local_order_id):
        watcher.calls.append((plan, local_order_id))
        return True

    watcher.watch = _watch

    monkeypatch.setattr(ap_db, "conn", lambda: _FakeConn(state))
    monkeypatch.setattr(ap_db, "run_with_retry", lambda fn, *a, **kw: fn())

    recovery = recovery_mod.APStartupRecovery(
        client_id="client-1",
        broker=object(),
        osm=None,
        pm=None,
        master_control=types.SimpleNamespace(),
        exit_engine=None,
        entry_watcher=watcher,
    )
    result = {"watchers_requeued": 0}

    recovery._reseed_watchers(result)

    assert result["watchers_requeued"] == 1
    assert len(watcher.calls) == 1
    plan, local_order_id = watcher.calls[0]
    assert local_order_id == "local-avgo-1"
    assert plan.ticker == "AVGO"
    assert plan.contract_symbol == "DEFERRED:AVGO"
    assert plan.trigger_price == 210.0
    assert plan.metadata["contract_deferred"] is True


def test_reseed_watchers_does_not_reset_queue_row_when_live_pending_order_exists(monkeypatch, recovery_mod):
    import ap.db as ap_db

    state = _FakeDbState(
        trade_queue_rows=[
            {
                "signal_id": "sig-avgo-1",
                "client_id": "client-1",
                "status": "WATCHING",
                "created_ts": "2099-06-15T12:00:00+00:00",
            }
        ],
        order_rows=[
            {
                "local_order_id": "local-avgo-1",
                "signal_id": "sig-avgo-1",
                "plan_id": "plan-avgo-1",
                "client_id": "client-1",
                "kind": "ENTRY",
                "status": "PENDING_TRIGGER",
                "created_ts": "2099-06-15T12:00:00+00:00",
                "submitted_ts": None,
                "filled_ts": None,
                "broker_order_id": None,
                "symbol": "AVGO",
                "contract": "DEFERRED:AVGO",
                "direction": "CALL",
                "score": 78.0,
                "tier": "A",
                "trigger_price": 210.0,
                "stop_underlying": 205.0,
                "target_underlying": 220.0,
                "pattern": "2-1-2",
                "timeframe": "1d",
                "meta": {"signal_id": "sig-avgo-1", "signal_entry_price": 210.0},
            }
        ],
    )

    watcher = types.SimpleNamespace()
    watcher.calls = []
    watcher._last_reject_reason = None
    watcher.has_order = lambda local_order_id: False
    watcher.watch = lambda plan, local_order_id: watcher.calls.append((plan, local_order_id)) or True

    monkeypatch.setattr(ap_db, "conn", lambda: _FakeConn(state))
    monkeypatch.setattr(ap_db, "run_with_retry", lambda fn, *a, **kw: fn())

    recovery = recovery_mod.APStartupRecovery(
        client_id="client-1",
        broker=object(),
        osm=None,
        pm=None,
        master_control=types.SimpleNamespace(),
        exit_engine=None,
        entry_watcher=watcher,
    )
    result = {"watchers_requeued": 0}

    recovery._reseed_watchers(result)

    assert state.trade_queue_rows[0]["status"] == "WATCHING"
    assert len(watcher.calls) == 1
