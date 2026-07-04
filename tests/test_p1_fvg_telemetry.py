# tests/test_p1_fvg_telemetry.py
# P1: FVG telemetry activation (observe-only wiring of PR #221).
#
# Evidence base: 0 of 4,331 signals in the 30 days ending 2026-07-03 carried
# any FVG output — #221 was merged dead code because production never supplied
# candles to the score-profile path.
#
# Invariants:
#   T1  Session-anchored aggregation: 15m→1h yields 09:30-anchored buckets
#       (last bar 30min); 15m→4h yields [09:30–13:30)+[13:30–16:00] per day;
#       OHLC composition correct (open=first, close=last, high=max, low=min).
#   T2  A genuine 3-candle gap in aggregated 4h bars is detected by #221's
#       detector through the full record path, and diagnostics persist compact.
#   T3  Observe-only: record_fvg_telemetry never raises — broker None, fetch
#       exploding, persist exploding, evaluate exploding all return status
#       dicts, never exceptions.
#   T4  TTL cache: two calls for the same ticker hit the network once.
#   T5  Kill switch: FVG_TELEMETRY_ENABLED=0 → status "disabled", no network.
#   T6  Persist merge: SQL merges under score_breakdown->'fvg' with composite
#       (signal_id, client_email) predicate — verified via captured SQL.

from __future__ import annotations

import json
from typing import Any

import pytest

import ap.fvg_telemetry as ft
from ap.fvg_telemetry import (
    aggregate_bars,
    record_fvg_telemetry,
)


@pytest.fixture(autouse=True)
def _fresh_cache(monkeypatch):
    monkeypatch.setattr(ft, "_candle_cache", {})
    yield


def _bar(t, o, h, l, c):
    return {"time": t, "open": o, "high": h, "low": l, "close": c}


def _session_15m(day: str, base: float, step: float = 0.0):
    """26 RTH 15-min bars for one session, mildly trending by `step`."""
    bars = []
    px = base
    minutes = 9 * 60 + 30
    for i in range(26):
        hh, mm = divmod(minutes + i * 15, 60)
        t = f"{day}T{hh:02d}:{mm:02d}:00-04:00"
        bars.append(_bar(t, px, px + 0.5, px - 0.5, px + step))
        px += step
    return bars


# ── T1: aggregation geometry ─────────────────────────────────────────────────

def test_t1_1h_aggregation_session_anchored():
    bars = _session_15m("2026-07-01", 100.0, 0.1)
    hourly = aggregate_bars(bars, bucket_minutes=60)
    assert len(hourly) == 7                      # 6x60min + final 30min
    assert hourly[0]["open"] == pytest.approx(100.0)
    assert hourly[0]["close"] == pytest.approx(100.4)   # 4th bar's close
    assert hourly[0]["high"] == pytest.approx(100.8)    # max of first 4 bars
    assert hourly[-1]["close"] == pytest.approx(bars[-1]["close"])


def test_t1b_4h_aggregation_two_buckets_per_session():
    two_days = _session_15m("2026-07-01", 100.0) + _session_15m("2026-07-02", 101.0)
    fourh = aggregate_bars(two_days, bucket_minutes=240)
    assert len(fourh) == 4                       # 2 buckets x 2 sessions
    assert fourh[0]["open"] == pytest.approx(100.0)
    assert fourh[2]["open"] == pytest.approx(101.0)     # new session restarts bucket


def test_t1c_garbage_timestamps_skipped():
    bars = [_bar("not-a-time", 1, 2, 0, 1)] + _session_15m("2026-07-01", 100.0)
    hourly = aggregate_bars(bars, bucket_minutes=60)
    assert len(hourly) == 7


# ── fake broker plumbing ─────────────────────────────────────────────────────

class _FakeResp:
    def __init__(self, items):
        self._items = items
    def raise_for_status(self):
        return None
    def json(self):
        return {"series": {"data": {"item": self._items}}}


class _FakeSession:
    def __init__(self, items):
        self.items = items
        self.calls = 0
    def get(self, *a, **k):
        self.calls += 1
        return _FakeResp(self.items)


class _FakeBroker:
    def __init__(self, items):
        self.session = _FakeSession(items)


def _gapped_session_items():
    """Three sessions where session 2 gaps up hard: the 4h bars form a
    bullish FVG (bar1.high < bar3.low)."""
    s1 = _session_15m("2026-06-29", 100.0)          # 4h highs ~100.5
    s2 = _session_15m("2026-06-30", 108.0)          # displacement day
    s3 = _session_15m("2026-07-01", 116.0)          # 4h lows ~115.5 > 100.5
    return s1 + s2 + s3


# ── T2: end-to-end detection + compact persistence ───────────────────────────

def test_t2_end_to_end_records_fvg(monkeypatch):
    captured: dict[str, Any] = {}

    def fake_persist(signal_id, client_email, compact):
        captured.update(signal_id=signal_id, client_email=client_email, compact=compact)
        return True

    monkeypatch.setattr(ft, "_persist", fake_persist)
    broker = _FakeBroker(_gapped_session_items())
    payload = {
        "ticker": "GOOGL", "side": "CALL", "direction": "CALL",
        "entry_trigger": 116.2, "current_underlying": 116.0,
        "target_underlying": 118.0,
    }
    out = record_fvg_telemetry(
        signal_id="sig-1", client_email="tradefluencehq@gmail.com",
        payload=payload, broker=broker,
    )
    assert out["status"] == "recorded"
    compact = captured["compact"]
    assert compact["v"] == 1
    assert compact["bars"]["4h"] == 6            # 2 buckets x 3 sessions
    tf4 = compact["timeframes"].get("4h") or {}
    assert tf4.get("available") is True
    assert (tf4.get("fvg_count") or 0) >= 1      # the engineered bullish gap
    assert "computed_at" in compact
    assert captured["client_email"] == "tradefluencehq@gmail.com"


# ── T3: never raises into dispatch ───────────────────────────────────────────

def test_t3a_broker_none_skips():
    out = record_fvg_telemetry(signal_id="s", client_email="c", payload={"ticker": "SPY"}, broker=None)
    assert out["status"] == "skipped"


def test_t3b_fetch_explosion_is_skipped(monkeypatch):
    class _Boom:
        @property
        def session(self):
            raise RuntimeError("session exploded")
    out = record_fvg_telemetry(signal_id="s", client_email="c", payload={"ticker": "SPY"}, broker=_Boom())
    assert out["status"] in ("skipped", "error")


def test_t3c_persist_explosion_reported_not_raised(monkeypatch):
    monkeypatch.setattr(ft, "_persist", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("db")))
    broker = _FakeBroker(_gapped_session_items())
    out = record_fvg_telemetry(
        signal_id="s", client_email="c",
        payload={"ticker": "SPY", "side": "CALL", "entry_trigger": 116.0},
        broker=broker,
    )
    assert out["status"] == "error"


def test_t3d_missing_ticker_skips():
    out = record_fvg_telemetry(signal_id="s", client_email="c", payload={}, broker=_FakeBroker([]))
    assert out["status"] == "skipped"
    assert out["reason"] == "missing_ticker_or_identity"


# ── T4: TTL cache bounds market-data spend ───────────────────────────────────

def test_t4_cache_one_network_call_per_ticker(monkeypatch):
    monkeypatch.setattr(ft, "_persist", lambda *a, **k: True)
    broker = _FakeBroker(_gapped_session_items())
    payload = {"ticker": "GOOGL", "side": "CALL", "entry_trigger": 116.0}
    record_fvg_telemetry(signal_id="a", client_email="c1", payload=payload, broker=broker)
    record_fvg_telemetry(signal_id="b", client_email="c2", payload=payload, broker=broker)
    assert broker.session.calls == 1


# ── T5: kill switch ──────────────────────────────────────────────────────────

def test_t5_kill_switch(monkeypatch):
    monkeypatch.setenv("FVG_TELEMETRY_ENABLED", "0")
    broker = _FakeBroker(_gapped_session_items())
    out = record_fvg_telemetry(
        signal_id="s", client_email="c",
        payload={"ticker": "SPY", "side": "CALL"}, broker=broker,
    )
    assert out["status"] == "disabled"
    assert broker.session.calls == 0


# ── T6: persistence SQL shape ────────────────────────────────────────────────

def test_t6_persist_sql_merges_under_fvg_key(monkeypatch):
    captured = {}

    class _Cur:
        rowcount = 1
        def execute(self, sql, params):
            captured["sql"] = sql
            captured["params"] = params
        def __enter__(self):
            return self
        def __exit__(self, *a):
            return False

    import ap.db as db
    monkeypatch.setattr(db, "conn", lambda: _Cur())
    monkeypatch.setattr(db, "run_with_retry", lambda fn: fn())

    ok = ft._persist("0f3542c0-244f-40c1-90af-bec8391e9bfa",
                     "tradefluencehq@gmail.com", {"v": 1, "status": "unfilled"})
    assert ok is True
    sql = captured["sql"]
    assert "score_breakdown" in sql
    assert "jsonb_build_object('fvg'" in sql
    assert "signal_id::text = %s" in sql
    assert "client_email = %s" in sql
    payload_json, sid, email = captured["params"]
    assert json.loads(payload_json)["status"] == "unfilled"
    assert sid == "0f3542c0-244f-40c1-90af-bec8391e9bfa"
    assert email == "tradefluencehq@gmail.com"
