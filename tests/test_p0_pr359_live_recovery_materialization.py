"""
tests/test_p0_pr359_live_recovery_materialization.py

PR #359 — LIVE watcher ownership and deferred materialization.
7 HARD HOLD blockers:
  B1  48h lookback, not midnight-calendar (prior-evening WMT/QCOM included)
  B2  LIVE_RECOVERY_MISSED_TRIGGER -> REJECTED (not WATCHING)
  B3  Ownerless CREATED LIVE orders block; PAPER orders do not
  B4  _restore UPDATE contains NOT EXISTS fence (TOCTOU prevention)
  B5  intel_rejected: risk_veto prefix is terminal policy skip
  B6  Stale/no-timestamp quotes fail closed
  B7  WMT/QCOM/C incident replays through real classifier
"""
from __future__ import annotations

import json
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

# Stub ap.db before any ap.* imports
_fake_ap_db = types.ModuleType("ap.db")
class _StubConn:
    class _C:
        rowcount = 0
        def execute(self, *a, **k): pass
        def fetchone(self): return None
        def fetchall(self): return []
        def __enter__(self): return self
        def __exit__(self, *_): pass
    def __enter__(self): return self._C()
    def __exit__(self, *_): pass

_fake_ap_db.conn = _StubConn
_fake_ap_db.run_with_retry = lambda fn, *a, **k: fn()
_fake_ap_db.get_open_orders_for_reconcile = lambda *a, **k: []
_fake_ap_db.list_positions = lambda *a, **k: []
sys.modules.setdefault("ap.db", _fake_ap_db)

from zoneinfo import ZoneInfo
ET = ZoneInfo("America/New_York")


def _fresh_quote(price):
    ts = datetime.now(timezone.utc).isoformat()
    m = MagicMock()
    m.get_quote = lambda sym: {"last": price, "mark": price, "timestamp": ts}
    return m

def _stale_quote(price, age_hours=3):
    ts = (datetime.now(timezone.utc) - timedelta(hours=age_hours)).isoformat()
    m = MagicMock()
    m.get_quote = lambda sym: {"last": price, "timestamp": ts}
    return m

def _no_ts_quote(price):
    m = MagicMock()
    m.get_quote = lambda sym: {"last": price}
    return m


def _q_row(*, symbol, direction, trigger, signal_id="sig-001",
           last_error=None, execution_mode="live", row_id=1, created_hours_ago=12):
    return {
        "id": row_id,
        "client_id": "jasoncosby1@gmail.com",
        "signal_id": signal_id,
        "status": "WATCHING",
        "created_ts": (datetime.now(timezone.utc) - timedelta(hours=created_hours_ago)).isoformat(),
        "started_ts": None, "finished_ts": None,
        "last_error": last_error,
        "payload": {
            "ticker": symbol, "symbol": symbol,
            "direction": direction, "side": direction,
            "trigger_price": trigger, "entry_price": trigger,
            "execution_mode": execution_mode,
            "canonical_signal_id": f"canonical-{signal_id}",
        },
    }


def _run_recovery(*, tq_rows, broker, client_id="jasoncosby1@gmail.com",
                  orders_rows=None, ap_signals_rows=None):
    import ap_recovery as _r
    sqls = []
    updates = []

    class _C:
        def __init__(self):
            self.rowcount = 1
            self._u = ""
        def execute(self, sql, params=()):
            norm = " ".join(str(sql).split())
            sqls.append((norm, tuple(params)))
            self._u = norm.upper()
            if "UPDATE" in self._u:
                updates.append((norm, tuple(params)))
        def fetchall(self):
            if "FROM TRADE_QUEUE" in self._u or ("SELECT" in self._u and "WATCHING" in self._u):
                return list(tq_rows)
            if "FROM ORDERS" in self._u:
                return list(orders_rows or [])
            if "FROM AP_SIGNALS" in self._u:
                return list(ap_signals_rows or [])
            return []
        def fetchone(self):
            rows = self.fetchall()
            return rows[0] if rows else None
        def __enter__(self): return self
        def __exit__(self, *_): pass

    class _Conn:
        def __enter__(self): return _C()
        def __exit__(self, *_): pass

    rec = object.__new__(_r.APStartupRecovery)
    rec.client_id = client_id
    rec.mc = SimpleNamespace(mode="LIVE")
    rec.entry_watcher = None
    rec.broker = broker

    fdb = types.ModuleType("ap.db")
    fdb.conn = _Conn
    fdb.run_with_retry = lambda fn, *a, **k: fn()

    with patch("ap_recovery.os.getenv", return_value="48"), \
         patch.dict(sys.modules, {"ap.db": fdb}):
        rec._reseed_watchers({})
    return sqls, updates


# B1 ─────────────────────────────────────────────────────────────────────────

def test_b1_no_upper_midnight_boundary():
    sqls, _ = _run_recovery(tq_rows=[], broker=MagicMock())
    load = [(s, p) for s, p in sqls if "FROM TRADE_QUEUE" in s.upper() and "WATCHING" in s.upper()]
    assert load, "No WATCHING trade_queue SELECT"
    assert "created_ts >= %s" in load[0][0], "Must use >= cutoff_utc"
    assert "created_ts < %s" not in load[0][0], "Must NOT have upper midnight boundary"


def test_b1_missed_trigger_rows_excluded():
    sqls, _ = _run_recovery(tq_rows=[], broker=MagicMock())
    load = [(s, p) for s, p in sqls if "FROM TRADE_QUEUE" in s.upper() and "WATCHING" in s.upper()]
    assert load
    sql_upper = load[0][0].upper()
    assert "LIVE_RECOVERY_MISSED_TRIGGER" in sql_upper or "LAST_ERROR NOT LIKE" in sql_upper


# B2 ─────────────────────────────────────────────────────────────────────────

def test_b2_missed_trigger_sets_rejected():
    row = _q_row(symbol="WMT", direction="CALL", trigger=55.0)
    _, updates = _run_recovery(tq_rows=[row], broker=_fresh_quote(58.0))
    missed = [(s, p) for s, p in updates if "LIVE_RECOVERY_MISSED_TRIGGER" in s]
    assert missed, f"No MISSED_TRIGGER UPDATE. updates={updates}"
    assert "status = 'REJECTED'" in missed[0][0], f"Must set REJECTED. SQL={missed[0][0]}"


def test_b2_eligible_not_rejected():
    row = _q_row(symbol="WMT", direction="PUT", trigger=60.5)
    _, updates = _run_recovery(tq_rows=[row], broker=_fresh_quote(62.0))
    assert not [(s, p) for s, p in updates if "LIVE_RECOVERY_MISSED_TRIGGER" in s]


# B3 ─────────────────────────────────────────────────────────────────────────

def test_b3_order_check_filters_execution_mode():
    import inspect, ap_recovery as _r
    src = inspect.getsource(_r)
    s = src.find("def _active_entry_order_exists(")
    e = src.find("\n            def ", s + 1)
    body = src[s:e]
    assert "execution_mode" in body.lower(), "Must filter by execution_mode"


def test_b3_no_ownership_predicate_required():
    import inspect, ap_recovery as _r
    src = inspect.getsource(_r)
    s = src.find("def _active_entry_order_exists(")
    e = src.find("\n            def ", s + 1)
    body = src[s:e]
    assert "status NOT IN" in body, "Must use status NOT IN (terminal states)"
    assert "COALESCE(broker_order_id" not in body, "Must not require broker_order_id"


# B4 ─────────────────────────────────────────────────────────────────────────

def test_b4_restore_has_not_exists_fence():
    import inspect, ap_recovery as _r
    src = inspect.getsource(_r)
    s = src.find("def _restore(row_id, diag:")
    e = src.find("\n            def ", s + 1)
    body = src[s:e]
    assert "NOT EXISTS" in body, "_restore UPDATE must contain NOT EXISTS fence"
    assert "execution_mode" in body.lower(), "NOT EXISTS must scope to execution_mode"


# B5 ─────────────────────────────────────────────────────────────────────────

def test_b5_intel_rejected_skips_before_quote():
    calls = []
    m = MagicMock()
    m.get_quote = lambda sym: (calls.append(sym), {"last": 15.0, "timestamp": datetime.now(timezone.utc).isoformat()})[1]
    row = _q_row(symbol="C", direction="PUT", trigger=16.0,
                 last_error="intel_rejected: risk_veto: PUT blocked — SPY in BULL trend")
    _run_recovery(tq_rows=[row], broker=m)
    assert calls == [], f"intel_rejected must skip before quote. Called: {calls}"


def test_b5_ap_signals_join_present():
    import inspect, ap_recovery as _r
    src = inspect.getsource(_r)
    s = src.find("def _classify(row)")
    e = src.find("\n            def ", s + 1)
    body = src[s:e]
    assert "ap_signals" in body.lower(), "_classify must join ap_signals"


def test_b5_policy_prefixes_still_skip():
    for prefix in ("POLICY_BLOCKED:x", "RISK_VETO:daily"):
        calls = []
        m = MagicMock()
        m.get_quote = lambda sym: (calls.append(sym), {"last": 10.0, "timestamp": datetime.now(timezone.utc).isoformat()})[1]
        row = _q_row(symbol="AAPL", direction="CALL", trigger=200.0, last_error=prefix)
        _run_recovery(tq_rows=[row], broker=m)
        assert calls == [], f"prefix {prefix!r} must skip before quote"


# B6 ─────────────────────────────────────────────────────────────────────────

def test_b6_stale_quote_no_termination():
    row = _q_row(symbol="WMT", direction="PUT", trigger=60.5)
    _, updates = _run_recovery(tq_rows=[row], broker=_stale_quote(58.0, age_hours=3))
    missed = [(s, p) for s, p in updates if "LIVE_RECOVERY_MISSED_TRIGGER" in s]
    assert not missed, f"Stale quote must not terminalize. Got {len(missed)}"


def test_b6_no_timestamp_no_termination():
    row = _q_row(symbol="QCOM", direction="PUT", trigger=175.0)
    _, updates = _run_recovery(tq_rows=[row], broker=_no_ts_quote(170.0))
    missed = [(s, p) for s, p in updates if "LIVE_RECOVERY_MISSED_TRIGGER" in s]
    assert not missed, f"No-timestamp quote must not terminalize. Got {len(missed)}"


def test_b6_freshness_in_source():
    import inspect, ap_recovery as _r
    src = inspect.getsource(_r)
    s = src.find("def _current_underlying(symbol:")
    e = src.find("\n            def ", s + 1)
    body = src[s:e]
    assert "_MAX_QUOTE_AGE_SECONDS" in body or "quote_age_sec" in body
    assert "return None" in body


# B7 ─────────────────────────────────────────────────────────────────────────

def test_b7_wmt_prior_evening_not_excluded():
    row = _q_row(symbol="WMT", direction="PUT", trigger=60.5, created_hours_ago=14)
    sqls, _ = _run_recovery(tq_rows=[row], broker=_fresh_quote(62.0))
    load = [(s, p) for s, p in sqls if "FROM TRADE_QUEUE" in s.upper() and "WATCHING" in s.upper()]
    assert load
    assert "created_ts < %s" not in load[0][0], "14h-old WMT excluded by midnight upper bound"


def test_b7_qcom_missed_trigger_rejected():
    row = _q_row(symbol="QCOM", direction="PUT", trigger=175.0, created_hours_ago=14)
    _, updates = _run_recovery(tq_rows=[row], broker=_fresh_quote(170.0))
    missed = [(s, p) for s, p in updates if "LIVE_RECOVERY_MISSED_TRIGGER" in s]
    assert missed, f"QCOM crossed trigger must be missed. updates={updates}"
    assert "status = 'REJECTED'" in missed[0][0]


def test_b7_wmt_stale_no_termination():
    row = _q_row(symbol="WMT", direction="PUT", trigger=60.5)
    _, updates = _run_recovery(tq_rows=[row], broker=_stale_quote(58.0, age_hours=3))
    missed = [(s, p) for s, p in updates if "LIVE_RECOVERY_MISSED_TRIGGER" in s]
    assert not missed, f"Stale WMT quote must not terminalize. Got {len(missed)}"


def test_b7_c_put_intel_rejected_no_quote():
    calls = []
    m = MagicMock()
    m.get_quote = lambda sym: (calls.append(sym), {"last": 15.0, "timestamp": datetime.now(timezone.utc).isoformat()})[1]
    row = _q_row(symbol="C", direction="PUT", trigger=16.0,
                 last_error="intel_rejected: risk_veto: PUT blocked — SPY in BULL trend")
    _run_recovery(tq_rows=[row], broker=m)
    assert calls == [], f"C PUT intel_rejected must skip before quote. Called: {calls}"


# OSM CAS (original PR #359) ─────────────────────────────────────────────────

class _OsmCursor:
    def __init__(self, sink, rowcount=1):
        self.sink = sink; self.rowcount = rowcount
    def execute(self, sql, params=()):
        self.sink.append((" ".join(str(sql).split()), tuple(params)))
    def fetchone(self): return None
    def fetchall(self): return []
    def __enter__(self): return self
    def __exit__(self, *_): pass

class _OsmConn:
    def __init__(self, sink, rowcount=1): self._c = _OsmCursor(sink, rowcount=rowcount)
    def __enter__(self): return self._c
    def __exit__(self, *_): return False

def _patch_osm_db(monkeypatch, *, rowcount=1):
    import ap.order_state_machine as osm_mod
    sink = []
    monkeypatch.setattr(osm_mod, "conn", lambda: _OsmConn(sink, rowcount=rowcount))
    monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn, *a, **k: fn())
    return sink

def _claim_kwargs(**ov):
    base = {
        "owner": "watcher:jason:nke", "generation": 1,
        "lease_until": (datetime.now(timezone.utc) + timedelta(seconds=120)).isoformat(),
        "trigger_crossed_at": datetime.now(timezone.utc).isoformat(),
        "trigger_price": 43.58, "observed_underlying_price": 43.59,
        "signal_id": "REEVAL:8d9338d0-5dde-4b7b-81ea-208039999b72:f4dc44",
        "execution_mode": "live",
        "canonical_signal_id": "REEVAL:8d9338d0-5dde-4b7b-81ea-208039999b72",
        "expected_order_status": "PENDING_TRIGGER",
        "expected_materialization_status": "WAITING_FOR_TRIGGER",
        "expected_lifecycle_state": "",
    }
    base.update(ov); return base

def test_reeval_suffixed_source_claim_uses_canonical_order_identity(monkeypatch):
    from ap.order_state_machine import APOrderStateMachine
    sink = _patch_osm_db(monkeypatch, rowcount=1)
    osm = APOrderStateMachine("jasoncosby1@gmail.com")
    assert osm.claim_deferred_materialization("nke-oid", **_claim_kwargs()) is True
    sql, params = sink[-1]
    patch_data = json.loads(params[0])
    assert "canonical_signal_id = %s" in sql
    assert "COALESCE(meta->>'submit_intent_at','') = ''" in sql
    assert patch_data["signal_id"].endswith(":f4dc44")
    assert "REEVAL:8d9338d0-5dde-4b7b-81ea-208039999b72" in params

def test_genuinely_different_opportunity_cannot_claim(monkeypatch):
    from ap.order_state_machine import APOrderStateMachine
    sink = _patch_osm_db(monkeypatch, rowcount=0)
    osm = APOrderStateMachine("jasoncosby1@gmail.com")
    assert osm.claim_deferred_materialization(
        "nke-oid", **_claim_kwargs(canonical_signal_id="DIFFERENT:canonical"),
    ) is False
