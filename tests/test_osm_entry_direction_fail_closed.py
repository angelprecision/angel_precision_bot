from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
sys.path.insert(0, str(REPO_ROOT))


class _Quiet:
    def info(self, *a, **kw): pass
    def warning(self, *a, **kw): pass
    def error(self, *a, **kw): pass
    def critical(self, *a, **kw): pass
    def debug(self, *a, **kw): pass
    def exception(self, *a, **kw): pass


class _FakeCursor:
    def __init__(self):
        self.executed = []
        self.next_fetchone = None

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        return self

    def fetchone(self):
        return self.next_fetchone

    def fetchall(self):
        return []


class _FakeConn:
    def __init__(self, cur):
        self.cur = cur

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        return self.cur.execute(sql, params)

    def fetchone(self):
        return self.cur.fetchone()

    def fetchall(self):
        return self.cur.fetchall()


def _patch_db(monkeypatch, cur):
    from ap import order_state_machine as osm_mod

    monkeypatch.setattr(osm_mod, "conn", lambda: _FakeConn(cur))
    monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn, *a, **kw: fn())
    monkeypatch.setattr(osm_mod, "log", _Quiet())


def _plan(side="CALL", direction="CALL"):
    return SimpleNamespace(
        plan_id="plan-1",
        signal_id="sig-1",
        ticker="SPY",
        contract_symbol="SPY260717C00500000",
        score=91.0,
        tier="A+",
        contracts=1,
        max_position_usd=150.0,
        limit_price=1.50,
        trigger_price=500.0,
        stop_underlying=495.0,
        target_underlying=510.0,
        pattern="daily_continuation",
        timeframe="1d",
        side=side,
        direction=direction,
        trigger_type="breach",
        metadata={},
    )


def _insert(cur):
    rows = [(sql, params) for sql, params in cur.executed if "INSERT INTO orders" in sql]
    assert rows
    return rows[0][1]


def _meta(params):
    return json.loads(next(p for p in params if isinstance(p, str) and p.startswith("{") and "direction" in p))


def test_missing_entry_direction_blocks_before_insert(monkeypatch):
    from ap.order_state_machine import APOrderStateMachine

    cur = _FakeCursor()
    _patch_db(monkeypatch, cur)
    osm = APOrderStateMachine(client_id="test@example.com")

    with pytest.raises(ValueError, match="invalid_or_missing_entry_direction"):
        osm.create_entry_order(_plan(side="", direction=""))

    assert not [sql for sql, _ in cur.executed if "INSERT INTO orders" in sql]


def test_invalid_entry_direction_blocks_before_insert(monkeypatch):
    from ap.order_state_machine import APOrderStateMachine

    cur = _FakeCursor()
    _patch_db(monkeypatch, cur)
    osm = APOrderStateMachine(client_id="test@example.com")

    with pytest.raises(ValueError, match="invalid_or_missing_entry_direction"):
        osm.create_entry_order(_plan(side="UNKNOWN", direction="UNKNOWN"))

    assert not [sql for sql, _ in cur.executed if "INSERT INTO orders" in sql]


@pytest.mark.parametrize(("raw", "expected"), [("CALL", "CALL"), ("PUT", "PUT"), ("BULLISH", "CALL"), ("BEARISH", "PUT")])
def test_entry_direction_normalized_into_order_and_meta(monkeypatch, raw, expected):
    from ap.order_state_machine import APOrderStateMachine

    cur = _FakeCursor()
    _patch_db(monkeypatch, cur)
    osm = APOrderStateMachine(client_id="test@example.com")
    osm.create_entry_order(_plan(side=raw, direction=raw))

    params = _insert(cur)
    meta = _meta(params)
    assert expected in params
    assert meta["direction"] == expected
    assert meta["side"] == expected


def test_direction_field_is_used_when_side_missing(monkeypatch):
    from ap.order_state_machine import APOrderStateMachine

    cur = _FakeCursor()
    _patch_db(monkeypatch, cur)
    osm = APOrderStateMachine(client_id="test@example.com")
    osm.create_entry_order(_plan(side="", direction="PUT"))

    params = _insert(cur)
    meta = _meta(params)
    assert "PUT" in params
    assert meta["direction"] == "PUT"
    assert meta["side"] == "PUT"
