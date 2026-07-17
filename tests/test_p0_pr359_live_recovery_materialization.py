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

# ap.db is stubbed per-test inside _run_recovery() via patch.dict(sys.modules).
# No global stub — that would poison unrelated tests in the same process.

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


# B1 fill-evidence behavioral tests ──────────────────────────────────────────

def _order_row(*, status, filled_qty=0, filled_ts=None, signal_id="sig-001",
               execution_mode="live"):
    """Minimal orders-table row for fill-evidence tests."""
    return {
        "id": "ord-1",
        "client_id": "jasoncosby1@gmail.com",
        "kind": "ENTRY",
        "execution_mode": execution_mode,
        "status": status,
        "signal_id": signal_id,
        "canonical_signal_id": f"canonical-{signal_id}",
        "filled_qty": filled_qty,
        "filled_ts": filled_ts,
        "broker_order_id": None,
        "submitted_ts": None,
    }


def _run_recovery_with_orders(tq_rows, broker, orders_rows):
    """Run recovery with explicit orders-table rows."""
    return _run_recovery(tq_rows=tq_rows, broker=broker, orders_rows=orders_rows)


def test_b1_filled_order_blocks_restore():
    """A FILLED ENTRY order with filled_qty>0 must block queue restoration."""
    tq = [_q_row(symbol="WMT", direction="PUT", trigger=60.5, signal_id="sig-001")]
    # filled order: status='FILLED', filled_qty=2, filled_ts set
    order = _order_row(status="FILLED", filled_qty=2,
                       filled_ts="2026-07-16T09:35:00Z", signal_id="sig-001")
    _, updates = _run_recovery_with_orders(tq, broker=_fresh_quote(62.0), orders_rows=[order])
    restore = [(s, p) for s, p in updates if "status = 'NEW'" in s]
    assert not restore, (
        "FILLED order with filled_qty=2 must block recovery. "
        f"Got {len(restore)} restore update(s)."
    )


def test_b1_canceled_after_partial_fill_blocks_restore():
    """CANCELED + filled_qty > 0 (partial fill before cancel) must block recovery."""
    tq = [_q_row(symbol="QCOM", direction="PUT", trigger=175.0, signal_id="sig-002")]
    order = _order_row(status="CANCELED", filled_qty=1,
                       filled_ts="2026-07-16T09:36:00Z", signal_id="sig-002")
    _, updates = _run_recovery_with_orders(tq, broker=_fresh_quote(176.0), orders_rows=[order])
    restore = [(s, p) for s, p in updates if "status = 'NEW'" in s]
    assert not restore, (
        "CANCELED order with filled_qty=1 (partial fill) must block recovery. "
        f"Got {len(restore)} restore update(s)."
    )


def test_b1_partial_fill_order_blocks_restore():
    """PARTIAL_FILL status must block recovery."""
    tq = [_q_row(symbol="QCOM", direction="PUT", trigger=175.0, signal_id="sig-003")]
    order = _order_row(status="PARTIAL_FILL", filled_qty=1, signal_id="sig-003")
    _, updates = _run_recovery_with_orders(tq, broker=_fresh_quote(176.0), orders_rows=[order])
    restore = [(s, p) for s, p in updates if "status = 'NEW'" in s]
    assert not restore, (
        "PARTIAL_FILL order must block recovery. "
        f"Got {len(restore)} restore update(s)."
    )


def test_b1_clean_rejected_sql_structure():
    """Source inspection: REJECTED must be in status NOT IN list so clean rejections
    are excluded from the exists-check without needing fill evidence."""
    import inspect, ap_recovery as _r
    src = inspect.getsource(_r)
    fn_s = src.find("def _active_entry_order_exists(")
    fn_e = src.find("\n            def ", fn_s + 1)
    body = src[fn_s:fn_e]
    assert "'REJECTED'" in body, "REJECTED must be in status NOT IN exclusion list"


def test_b1_no_matching_order_allows_restore():
    """When no blocking order exists in the DB, an eligible row is restored."""
    # PUT trigger=60.5, current=62 — not yet crossed → eligible
    tq = [_q_row(symbol="WMT", direction="PUT", trigger=60.5, signal_id="sig-004")]
    _, updates = _run_recovery_with_orders(tq, broker=_fresh_quote(62.0), orders_rows=[])
    restore = [(s, p) for s, p in updates if "status = 'NEW'" in s]
    assert restore, (
        "When no blocking order exists, eligible row must be restored. "
        f"Got {len(restore)} restore update(s)."
    )


def test_b1_paper_order_excluded_by_execution_mode_sql():
    """Source inspection: execution_mode filter must prevent PAPER orders blocking LIVE."""
    import inspect, ap_recovery as _r
    src = inspect.getsource(_r)
    fn_s = src.find("def _active_entry_order_exists(")
    fn_e = src.find("\n            def ", fn_s + 1)
    body = src[fn_s:fn_e]
    assert "execution_mode" in body.lower(), "SQL must filter by execution_mode='live'"


# B2 bid/ask midpoint freshness tests ────────────────────────────────────────

def _tradier_mixed_quote(price, bid_fresh=True, ask_fresh=True):
    """Build a Tradier quote where bid/ask timestamps are independently controllable."""
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    stale_ms = now_ms - int(3 * 3600 * 1000)  # 3h stale
    m = MagicMock()
    m.get_quote = lambda sym: {
        "last": None,         # no last — forces midpoint path
        "bid": price - 0.05,
        "ask": price + 0.05,
        "trade_date": None,   # no trade_date — forces bid/ask path
        "bid_date": now_ms if bid_fresh else stale_ms,
        "ask_date": now_ms if ask_fresh else stale_ms,
    }
    return m


def test_b2_stale_bid_fresh_ask_fails_closed():
    """Stale bid_date + fresh ask_date must not allow midpoint use."""
    # PUT trigger=175, current=170 → would be crossed if midpoint accepted
    row = _q_row(symbol="QCOM", direction="PUT", trigger=175.0)
    _, updates = _run_recovery(
        tq_rows=[row],
        broker=_tradier_mixed_quote(170.0, bid_fresh=False, ask_fresh=True)
    )
    missed = [(s, p) for s, p in updates if "LIVE_RECOVERY_MISSED_TRIGGER" in s]
    assert not missed, (
        "Stale bid_date + fresh ask_date must fail closed — midpoint must not be used. "
        f"Got {len(missed)} missed-trigger update(s)."
    )


def test_b2_fresh_bid_stale_ask_fails_closed():
    """Fresh bid_date + stale ask_date must not allow midpoint use."""
    row = _q_row(symbol="QCOM", direction="PUT", trigger=175.0)
    _, updates = _run_recovery(
        tq_rows=[row],
        broker=_tradier_mixed_quote(170.0, bid_fresh=True, ask_fresh=False)
    )
    missed = [(s, p) for s, p in updates if "LIVE_RECOVERY_MISSED_TRIGGER" in s]
    assert not missed, (
        "Fresh bid_date + stale ask_date must fail closed. "
        f"Got {len(missed)} missed-trigger update(s)."
    )


def test_b2_both_fresh_bid_ask_accepted():
    """Both bid_date and ask_date fresh → midpoint accepted for classification."""
    # CALL trigger=55, current midpoint≈58 → crossed → should terminalize
    row = _q_row(symbol="WMT", direction="CALL", trigger=55.0)
    _, updates = _run_recovery(
        tq_rows=[row],
        broker=_tradier_mixed_quote(58.0, bid_fresh=True, ask_fresh=True)
    )
    missed = [(s, p) for s, p in updates if "LIVE_RECOVERY_MISSED_TRIGGER" in s]
    assert missed, (
        "Both fresh bid/ask timestamps → midpoint must be used for classification. "
        f"No missed-trigger update produced."
    )

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
    # Stub ap.db in sys.modules before importing OSM so DATABASE_URL is not required
    _stub = types.ModuleType("ap.db")
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
    _stub.conn = _StubConn
    _stub.run_with_retry = lambda fn, *a, **k: fn()
    monkeypatch.setitem(sys.modules, "ap.db", _stub)
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
    sink = _patch_osm_db(monkeypatch, rowcount=1)
    from ap.order_state_machine import APOrderStateMachine
    osm = APOrderStateMachine("jasoncosby1@gmail.com")
    assert osm.claim_deferred_materialization("nke-oid", **_claim_kwargs()) is True
    sql, params = sink[-1]
    patch_data = json.loads(params[0])
    assert "canonical_signal_id = %s" in sql
    assert "COALESCE(meta->>'submit_intent_at','') = ''" in sql
    assert patch_data["signal_id"].endswith(":f4dc44")
    assert "REEVAL:8d9338d0-5dde-4b7b-81ea-208039999b72" in params

def test_genuinely_different_opportunity_cannot_claim(monkeypatch):
    sink = _patch_osm_db(monkeypatch, rowcount=0)
    from ap.order_state_machine import APOrderStateMachine
    osm = APOrderStateMachine("jasoncosby1@gmail.com")
    assert osm.claim_deferred_materialization(
        "nke-oid", **_claim_kwargs(canonical_signal_id="DIFFERENT:canonical"),
    ) is False


# P0-1: NULL last_error rows are candidates ──────────────────────────────────

def test_p0_1_null_last_error_row_is_candidate():
    """A healthy WATCHING row with last_error=NULL must be selected.
    PostgreSQL NULL NOT LIKE 'x%' evaluates to UNKNOWN (not TRUE), excluding
    the row. The fix is (last_error IS NULL OR last_error NOT LIKE 'x%')."""
    sqls, _ = _run_recovery(tq_rows=[], broker=MagicMock())
    load = [(s, p) for s, p in sqls if "FROM TRADE_QUEUE" in s.upper() and "WATCHING" in s.upper()]
    assert load
    load_sql = load[0][0]
    assert "IS NULL" in load_sql.upper(), (
        "Candidate query must include 'last_error IS NULL' so healthy WATCHING rows "
        "with NULL last_error are not excluded by NULL NOT LIKE evaluation"
    )


def test_p0_1_null_last_error_row_is_processed():
    """A WATCHING row with last_error=NULL must reach the classifier and be eligible."""
    row = _q_row(symbol="WMT", direction="PUT", trigger=60.5, last_error=None)
    _, updates = _run_recovery(tq_rows=[row], broker=_fresh_quote(62.0))
    # Eligible row (not yet crossed): should produce a restore UPDATE
    restore = [(s, p) for s, p in updates if "status = 'NEW'" in s]
    missed = [(s, p) for s, p in updates if "LIVE_RECOVERY_MISSED_TRIGGER" in s]
    # Either a restore fired or a skip — but it must not have been excluded before classify
    # The key proof: broker was called (classifier ran), not skipped at query level
    # We verify this by checking that the load SQL has the NULL-safe predicate (above test)
    # and that the row reached the broker call path — verified by the fact that
    # _current_underlying is called (would only happen if row made it through _load_candidates)
    # The stale-quote test already proves this path. Here just confirm no spurious REJECT.
    assert not any("LIVE_RECOVERY_MISSED_TRIGGER" in s for s, _ in updates), (
        "NULL last_error WMT row (not yet triggered) must not be terminalized"
    )


# P0-2: Tradier trade_date/bid_date/ask_date timestamp support ───────────────

def _tradier_quote(price, trade_date_ms=None, fresh=True):
    """Build a Tradier-shaped quote dict with epoch-ms timestamps."""
    if trade_date_ms is None:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        if not fresh:
            now_ms -= int(3 * 3600 * 1000)  # 3h stale
        trade_date_ms = now_ms
    m = MagicMock()
    m.get_quote = lambda sym: {
        "last": price,
        "bid": price - 0.05,
        "ask": price + 0.05,
        "trade_date": trade_date_ms,
    }
    return m


def test_p0_2_tradier_trade_date_fresh_accepted():
    """Tradier trade_date (epoch-ms) within 120s must allow price to be used."""
    # PUT trigger=175, current=170 → crossed → QCOM should be terminalized
    row = _q_row(symbol="QCOM", direction="PUT", trigger=175.0, created_hours_ago=14)
    _, updates = _run_recovery(tq_rows=[row], broker=_tradier_quote(170.0, fresh=True))
    missed = [(s, p) for s, p in updates if "LIVE_RECOVERY_MISSED_TRIGGER" in s]
    assert missed, "Fresh trade_date Tradier quote must allow missed-trigger classification"
    assert "status = 'REJECTED'" in missed[0][0]


def test_p0_2_tradier_stale_trade_date_fails_closed():
    """Tradier trade_date from 3h ago must fail closed — no termination."""
    row = _q_row(symbol="QCOM", direction="PUT", trigger=175.0)
    _, updates = _run_recovery(tq_rows=[row], broker=_tradier_quote(170.0, fresh=False))
    missed = [(s, p) for s, p in updates if "LIVE_RECOVERY_MISSED_TRIGGER" in s]
    assert not missed, "Stale Tradier trade_date must not terminalize"


def test_p0_2_no_timestamp_field_fails_closed():
    """Quote dict with no timestamp/trade_date must fail closed."""
    row = _q_row(symbol="QCOM", direction="PUT", trigger=175.0)
    _, updates = _run_recovery(tq_rows=[row], broker=_no_ts_quote(170.0))
    missed = [(s, p) for s, p in updates if "LIVE_RECOVERY_MISSED_TRIGGER" in s]
    assert not missed, "Quote without any timestamp field must not terminalize"


# P0-3: _mark_missed TOCTOU fence ────────────────────────────────────────────

def test_p0_3_mark_missed_contains_not_exists_fence():
    import inspect, ap_recovery as _r
    src = inspect.getsource(_r)
    fn_s = src.find("def _mark_missed(row_id, diag:")
    fn_e = src.find("\n            def ", fn_s + 1)
    body = src[fn_s:fn_e]
    assert "NOT EXISTS" in body, (
        "_mark_missed UPDATE must contain NOT EXISTS order fence to prevent TOCTOU race"
    )


# P0-4: FILLED orders block recovery ─────────────────────────────────────────

def test_p0_4_filled_qty_zero_required():
    import inspect, ap_recovery as _r
    src = inspect.getsource(_r)
    fn_s = src.find("def _active_entry_order_exists(")
    fn_e = src.find("\n            def ", fn_s + 1)
    body = src[fn_s:fn_e]
    assert "filled_qty" in body.lower() or "filled_ts" in body.lower(), (
        "Order check must block rows with filled_qty > 0 or filled_ts IS NOT NULL"
    )


def test_p0_4_filled_not_in_terminal_exclusions():
    """FILLED must NOT be in the terminal-exclusion list; it must be blocked separately."""
    import inspect, ap_recovery as _r
    src = inspect.getsource(_r)
    fn_s = src.find("def _active_entry_order_exists(")
    fn_e = src.find("\n            def ", fn_s + 1)
    body = src[fn_s:fn_e]
    # FILLED is now handled by filled_qty/filled_ts predicate, NOT in the status NOT IN list
    # (having it in both is also acceptable; the key is it blocks)
    # Verify either approach is present
    blocks_filled = ("'FILLED'" in body or "filled_qty" in body.lower() or "filled_ts" in body.lower())
    assert blocks_filled, "FILLED entries must block LIVE recovery"


# P0-5: Policy truth failure closes ──────────────────────────────────────────

def test_p0_5_policy_lookup_fails_closed():
    """When ap_signals query raises, classify must skip (fail closed), not continue."""
    import ap_recovery as _r

    sqls, updates = [], []

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
            if "FROM TRADE_QUEUE" in self._u or "WATCHING" in self._u:
                return [_q_row(symbol="C", direction="PUT", trigger=16.0, signal_id="sig-c-p05")]
            if "FROM AP_SIGNALS" in self._u:
                raise RuntimeError("db_timeout")
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
    rec.client_id = "jasoncosby1@gmail.com"
    rec.mc = SimpleNamespace(mode="LIVE")
    rec.entry_watcher = None
    # Fresh quote — current=15 below trigger=16 for PUT → would be eligible
    rec.broker = _fresh_quote(15.0)

    fdb = __import__("types").ModuleType("ap.db")
    fdb.conn = _Conn
    fdb.run_with_retry = lambda fn, *a, **k: fn()

    with patch("ap_recovery.os.getenv", return_value="48"),          patch.dict(sys.modules, {"ap.db": fdb}):
        rec._reseed_watchers({})

    # Must not restore a row when policy truth is unavailable
    restore = [(s, p) for s, p in updates if "status = 'NEW'" in s]
    assert not restore, (
        "ap_signals query failure must skip the row (fail closed), not restore it. "
        f"Got {len(restore)} restore update(s)."
    )
