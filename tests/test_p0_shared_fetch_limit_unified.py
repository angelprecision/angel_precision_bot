"""
tests/test_p0_shared_fetch_limit_unified.py
=============================================
PR #388 P0-8 — the shared ap_signals Supabase fetch must use the SAME
env-controlled limit as the trade_queue Postgres fetch, and must NOT be
capped at the hardcoded 300-row ceiling that starved older rows once
observed inventory exceeded 300.

Population invariant: a scenario with 313 shared rows must be able to
present ALL rows to the per-client disposition resolver, either in a
single fetch (default limit 500) or via a bumped OVERNIGHT_FETCH_LIMIT.
"""
from __future__ import annotations

import pathlib

import pytest

import ap_overnight_reeval as ov


REPO = pathlib.Path(__file__).parent.parent
OV_SRC = (REPO / "ap_overnight_reeval.py").read_text()


def _shared_signal_row(i: int) -> dict:
    return {
        "signal_id": f"sig-{i:03d}",
        "signal_payload": {
            "signal_id": f"sig-{i:03d}",
            "ticker": f"T{i:03d}",
            "side": "CALL" if i % 2 == 0 else "PUT",
            "timeframe": "1d",
            "entry_trigger": 100.0 + i,
        },
        "raw_payload": None,
        "created_at": f"2026-08-31T20:00:{(i % 60):02d}+00:00",
        "ticker": f"T{i:03d}",
        "side": "CALL" if i % 2 == 0 else "PUT",
        "score": 80.0,
        "timeframe": "1d",
        "pattern": "2-1-2",
        "tier": "A",
        "decision_status": "WATCHING",
        "entry_trigger": 100.0 + i,
        "stop_price": None,
        "target_price": None,
        "underlying_at_signal": None,
    }


def test_ap_signals_fetch_no_longer_hardcodes_limit_300():
    """The prior `.limit(300)` starved every row after the newest 300.
    Scan only non-comment lines so the historical mention in a comment
    doesn't false-positive."""
    for line in OV_SRC.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        assert ".limit(300)" not in line, (
            "The Supabase ap_signals query still hardcodes .limit(300). "
            "It must use the same env-controlled fetch limit as trade_queue "
            "so an inventory above 300 rows can still be seen."
        )


def test_ap_signals_fetch_uses_the_same_env_controlled_limit_variable():
    """Both queries reference the same env-controlled fetch limit."""
    # The Supabase call must probe one row beyond _fetch_limit.
    assert ".limit(_fetch_limit + 1)" in OV_SRC


def test_313_shared_rows_are_all_visible_when_supabase_stub_returns_them(monkeypatch):
    """End-to-end: stub the Supabase source to return 313 rows and verify
    _fetch_watching_signals_with_status_impl surfaces all of them (dedup
    aside), not just the newest 300."""
    # 313 unique setups.
    rows = [
        {
            "signal_id":              f"sig-{i:03d}",
            "signal_payload":         {
                "signal_id":  f"sig-{i:03d}",
                "ticker":     f"T{i:03d}",
                "side":       "CALL" if i % 2 == 0 else "PUT",
                "timeframe":  "1d",
                "entry_trigger": 100.0 + i,
                "created_at": f"2026-07-24T20:00:{(i % 60):02d}+00:00",
            },
            "raw_payload":            None,
            "created_at":             f"2026-07-24T20:00:{(i % 60):02d}+00:00",
            "ticker":                 f"T{i:03d}",
            "side":                   "CALL" if i % 2 == 0 else "PUT",
            "score":                  80.0,
            "timeframe":              "1d",
            "pattern":                "2-1-2",
            "tier":                   "A",
            "decision_status":        "WATCHING",
            "entry_trigger":          100.0 + i,
            "stop_price":             None,
            "target_price":           None,
            "underlying_at_signal":   None,
        }
        for i in range(313)
    ]

    # Fake Supabase client: capture the .limit() value and return `rows`.
    seen_limits: list[int] = []

    class _FakeExecute:
        def __init__(self, data):
            self.data = data

    class _FakeQuery:
        def __init__(self, data):
            self._data = data

        def select(self, *_a, **_kw):    return self
        def eq(self, *_a, **_kw):        return self
        def gte(self, *_a, **_kw):       return self
        def order(self, *_a, **_kw):     return self

        def limit(self, n):
            seen_limits.append(int(n))
            return self

        def execute(self):
            return _FakeExecute(list(self._data))

    class _FakeTable:
        def __init__(self, data):
            self._data = data

        def table(self, _name):
            return _FakeQuery(self._data)

    def _fake_create_client(_url, _key):
        return _FakeTable(rows)

    import sys, types
    _supabase_stub = types.ModuleType("supabase")
    _supabase_stub.create_client = _fake_create_client
    monkeypatch.setitem(sys.modules, "supabase", _supabase_stub)

    # Stub trade_queue to succeed with zero rows.
    monkeypatch.setattr(ov, "_fetch_watching_signals", ov._fetch_watching_signals)
    from ap import db as _apdb
    class _FakeCursor:
        def __enter__(self): return self
        def __exit__(self, *_a): return False
        def execute(self, *_a, **_kw): pass
        def fetchall(self): return []
    monkeypatch.setattr(_apdb, "conn", lambda: _FakeCursor())
    monkeypatch.setattr(_apdb, "run_with_retry", lambda fn: fn())

    monkeypatch.setenv("SUPABASE_URL", "https://example.com")
    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "test-key")

    # Bump the env limit so all 313 rows can come back in one query.
    monkeypatch.setenv("OVERNIGHT_FETCH_LIMIT", "500")

    result = ov._fetch_watching_signals_with_status_impl("jose@example.com")

    assert result.ap_signals_status == ov._SOURCE_STATUS_SUCCESS
    # Every row must reach the caller (dedup key is per-ticker+side+trigger;
    # our 313 rows have distinct triggers so none dedup).
    assert len(result.rows) == 313, (
        f"expected all 313 shared rows visible; got {len(result.rows)}. "
        f"seen limits passed to Supabase: {seen_limits}"
    )
    # And the limit passed to Supabase must be >= 313.
    assert seen_limits and seen_limits[0] >= 313, (
        f"Supabase query was capped below the row count: {seen_limits}"
    )


def test_default_fetch_limit_still_accommodates_up_to_500_rows(monkeypatch):
    """Without setting OVERNIGHT_FETCH_LIMIT, the default 500 must apply to
    the Supabase query too — not the old 300 ceiling."""
    monkeypatch.delenv("OVERNIGHT_FETCH_LIMIT", raising=False)
    # We only need to observe the .limit() value passed to Supabase.
    seen_limits: list[int] = []
    class _E:
        def __init__(self, data): self.data = data
    class _Q:
        def __init__(self): self._data = []
        def select(self, *_a, **_kw): return self
        def eq(self, *_a, **_kw): return self
        def gte(self, *_a, **_kw): return self
        def order(self, *_a, **_kw): return self
        def limit(self, n): seen_limits.append(int(n)); return self
        def execute(self): return _E([])
    class _T:
        def table(self, _n): return _Q()
    import sys, types
    _supabase_stub = types.ModuleType("supabase")
    _supabase_stub.create_client = lambda *_a, **_kw: _T()
    monkeypatch.setitem(sys.modules, "supabase", _supabase_stub)
    monkeypatch.setenv("SUPABASE_URL", "https://example.com")
    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "test-key")

    from ap import db as _apdb
    class _FC:
        def __enter__(self): return self
        def __exit__(self, *_a): return False
        def execute(self, *_a, **_kw): pass
        def fetchall(self): return []
    monkeypatch.setattr(_apdb, "conn", lambda: _FC())
    monkeypatch.setattr(_apdb, "run_with_retry", lambda fn: fn())

    ov._fetch_watching_signals_with_status_impl("x@example.com")
    assert seen_limits == [501], (
        f"expected default Supabase completeness probe of 501; got {seen_limits}"
    )


def test_ap_signals_over_limit_is_incomplete_not_complete(monkeypatch):
    """A successful bounded response must not certify the whole inventory."""
    rows = [_shared_signal_row(i) for i in range(501)]

    class _E:
        def __init__(self, data): self.data = data

    class _Q:
        def __init__(self, data):
            self._data = data
            self._limit = None
        def select(self, *_a, **_kw): return self
        def eq(self, *_a, **_kw): return self
        def gte(self, *_a, **_kw): return self
        def order(self, *_a, **_kw): return self
        def limit(self, n): self._limit = int(n); return self
        def execute(self): return _E(self._data[:self._limit])

    class _T:
        def table(self, _name): return _Q(rows)

    import sys, types
    _supabase_stub = types.ModuleType("supabase")
    _supabase_stub.create_client = lambda *_a, **_kw: _T()
    monkeypatch.setitem(sys.modules, "supabase", _supabase_stub)
    monkeypatch.setenv("SUPABASE_URL", "https://example.com")
    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "test-key")

    from ap import db as _apdb
    class _FC:
        def __enter__(self): return self
        def __exit__(self, *_a): return False
        def execute(self, *_a, **_kw): pass
        def fetchall(self): return []
    monkeypatch.setattr(_apdb, "conn", lambda: _FC())
    monkeypatch.setattr(_apdb, "run_with_retry", lambda fn: fn())
    monkeypatch.delenv("OVERNIGHT_FETCH_LIMIT", raising=False)

    result = ov._fetch_watching_signals_with_status_impl("jose@example.com")

    assert result.ap_signals_status == ov._SOURCE_STATUS_SUCCESS
    assert result.source_lookup_partial is True
    assert len(result.rows) == 500


def test_trade_queue_over_limit_is_incomplete_not_complete(monkeypatch):
    """The client-scoped source gets the same completeness fence."""
    rows = [
        {
            "id": f"job-{i:03d}",
            "signal_id": f"sig-{i:03d}",
            "payload": {
                "signal_id": f"sig-{i:03d}",
                "ticker": f"T{i:03d}",
                "side": "CALL" if i % 2 == 0 else "PUT",
                "timeframe": "1d",
                "entry_trigger": 100.0 + i,
            },
            "created_ts": "2026-08-31T20:00:00+00:00",
        }
        for i in range(501)
    ]

    class _FC:
        def __init__(self): self._limit = None
        def __enter__(self): return self
        def __exit__(self, *_a): return False
        def execute(self, _sql, params=None, **_kw): self._limit = int(params[-1])
        def fetchall(self): return rows[:self._limit]

    from ap import db as _apdb
    monkeypatch.setattr(_apdb, "conn", lambda: _FC())
    monkeypatch.setattr(_apdb, "run_with_retry", lambda fn: fn())
    monkeypatch.setenv("SUPABASE_URL", "")
    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "")
    monkeypatch.delenv("OVERNIGHT_FETCH_LIMIT", raising=False)

    result = ov._fetch_watching_signals_with_status_impl("jose@example.com")

    assert result.trade_queue_status == ov._SOURCE_STATUS_SUCCESS
    assert result.source_lookup_partial is True
    assert len(result.rows) == 500
