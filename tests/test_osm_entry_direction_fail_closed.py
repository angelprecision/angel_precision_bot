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


@pytest.fixture(autouse=True)
def _bypass_entry_metadata_guard(monkeypatch):
    """Bypass ap.entry_metadata_guard's monkey-patch of create_entry_order so
    these tests exercise PR #234's direction guard in isolation.

    ap.entry_metadata_guard.install_entry_metadata_guard() runs at ap package
    import time and wraps APOrderStateMachine.create_entry_order with its own
    validator, which has its own direction check that would fire before ours.
    That's correct in production (defense in depth), but for the tests in this
    file we want to prove the PR #234 guard specifically.

    We restore the pre-guard create_entry_order for the duration of each test.
    """
    from ap.order_state_machine import APOrderStateMachine
    original_create = getattr(
        APOrderStateMachine, "_entry_metadata_guard_original_create", None
    )
    if original_create is not None:
        monkeypatch.setattr(
            APOrderStateMachine, "create_entry_order", original_create
        )
    yield


def _plan(side="CALL", direction="CALL"):
    # execution_mode is required by ap.entry_metadata_guard, which monkey-patches
    # create_entry_order at ap package import time.  Without a valid mode the
    # metadata guard raises ValueError("metadata_invalid:unknown_execution_mode")
    # before our direction guard ever runs.  Set to PAPER so tests exercise the
    # PR #234 direction fail-closed logic specifically.
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
        execution_mode="PAPER",
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


# ─────────────────────────────────────────────────────────────────────────────
# PR #234: additional coverage beyond the shim-era tests.
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize(
    "raw,expected",
    [
        ("CALL",    "CALL"),
        ("call",    "CALL"),
        ("  Call ", "CALL"),
        ("BUY",     "CALL"),
        ("long",    "CALL"),
        ("CALLS",   "CALL"),
        ("bullish", "CALL"),
        ("PUT",     "PUT"),
        ("sell",    "PUT"),
        ("SHORT",   "PUT"),
        ("puts",    "PUT"),
        ("BEARISH", "PUT"),
    ],
)
def test_normalize_entry_direction_accepts_canonical_and_aliases(raw, expected):
    from ap.order_state_machine import _normalize_entry_direction
    assert _normalize_entry_direction(SimpleNamespace(side=raw, direction=None)) == expected
    # And via direction fallback when side is empty
    assert _normalize_entry_direction(SimpleNamespace(side="", direction=raw)) == expected


@pytest.mark.parametrize(
    "raw",
    [None, "", "   ", "UNKNOWN", "neutral", "0", "sideways"],
)
def test_normalize_entry_direction_raises_on_invalid_or_missing(raw):
    from ap.order_state_machine import _normalize_entry_direction
    plan = SimpleNamespace(side=raw, direction=raw)
    with pytest.raises(ValueError) as excinfo:
        _normalize_entry_direction(plan)
    assert str(excinfo.value).startswith("invalid_or_missing_entry_direction:")


def test_create_entry_order_writes_canonical_direction_back_onto_plan(monkeypatch):
    """PR #234 spec: after normalization, plan.side and plan.direction must be
    canonical CALL/PUT so any downstream reader (retry engine, watcher, exit
    engine, logging) sees the normalized value."""
    from ap.order_state_machine import APOrderStateMachine

    cur = _FakeCursor()
    _patch_db(monkeypatch, cur)
    osm = APOrderStateMachine(client_id="test@example.com")

    plan = _plan(side="bullish", direction="bullish")
    osm.create_entry_order(plan)

    assert plan.side == "CALL"
    assert plan.direction == "CALL"


def test_create_entry_order_canonicalizes_caller_meta_direction(monkeypatch):
    """PR #234 spec: caller-supplied meta with a stale/mismatched direction
    must not leak into the persisted meta.  The defensive copy + override
    inside create_entry_order guarantees meta.direction / meta.side match the
    canonical value written to orders.direction."""
    from ap.order_state_machine import APOrderStateMachine

    cur = _FakeCursor()
    _patch_db(monkeypatch, cur)
    osm = APOrderStateMachine(client_id="test@example.com")

    # Caller passes a meta with WRONG direction — the plan says CALL but the
    # caller's meta says PUT.  OSM must force the plan's canonical CALL.
    stale_meta = {"direction": "PUT", "side": "PUT", "trace_id": "keep-me"}
    osm.create_entry_order(_plan(side="CALL", direction="CALL"), meta=stale_meta)

    params = _insert(cur)
    meta = _meta(params)
    assert meta["direction"] == "CALL"
    assert meta["side"] == "CALL"
    # Non-conflicting caller keys are preserved.
    assert meta.get("trace_id") == "keep-me"


def test_osm_shim_package_is_deleted():
    """PR #234: the ap/order_state_machine/__init__.py package-shadow must be
    gone.  Import must resolve to the .py file directly."""
    import importlib.util
    origin = importlib.util.find_spec("ap.order_state_machine").origin
    assert origin is not None and origin.endswith("order_state_machine.py"), (
        f"expected direct .py import, got {origin}"
    )
