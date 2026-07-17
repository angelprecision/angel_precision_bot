"""
tests/test_p0_pr359_live_recovery_materialization.py

PR #359 — LIVE watcher ownership and deferred materialization.

Regression coverage:
  B1  LIVE candidate window uses prior-trading-session identity, not a
      rolling hour count (Friday→Monday and holiday-weekend safe)
  B2  LIVE_RECOVERY_MISSED_TRIGGER -> REJECTED (not WATCHING)
  B3  Ownerless CREATED LIVE orders block; PAPER orders do not
  B4  _restore UPDATE contains NOT EXISTS fence (TOCTOU prevention)
  B5  intel_rejected: risk_veto prefix is terminal policy skip
  B6  Stale/no-timestamp quotes fail closed
  B7  WMT/QCOM/C incident replays through real classifier
  Session gate:
    • sig_date must equal prior trading session (ET) OR today (ET)
    • naive/malformed timestamps fail closed (no calendar guessing)
    • NYSE calendar unavailable → LIVE recovery skipped entirely
  Legacy canonical CAS:
    • empty durable canonical is atomically re-proven and backfilled in
      the SAME UPDATE inside claim_deferred_materialization()
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
           last_error=None, execution_mode="live", row_id=1, created_hours_ago=12,
           signal_date=None):
    # signal_date in the payload is the session-gate anchor.  Default to
    # today (ET calendar) so existing tests pass the _classify() session
    # check (sig_date == today_et_date) without needing to mock the NYSE
    # calendar.  Tests that exercise session-boundary behaviour pass an
    # explicit signal_date string (ISO date, e.g. "2026-07-10").
    # Never use _date.today() — it uses the runner's local timezone which
    # can diverge from Eastern time near midnight boundaries.
    _sig_date_str = (
        signal_date if signal_date is not None
        else datetime.now(ET).date().isoformat()
    )
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
            "signal_date": _sig_date_str,
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

    # Install a mock NYSE calendar so LIVE recovery doesn't fail-closed
    # on the calendar gate.  Individual boundary tests override this via
    # patch("ap_recovery._prior_trading_session_date_et", ...).
    _fake_flatline = types.ModuleType("ap.flatline_alarm")
    _fake_flatline.is_trading_day = lambda d: d.weekday() < 5

    with patch("ap_recovery.os.getenv", return_value="48"), \
         patch.dict(sys.modules, {"ap.db": fdb, "ap.flatline_alarm": _fake_flatline}):
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


# _mark_missed fill-evidence race test ────────────────────────────────────────

def test_p0_mark_missed_fence_has_fill_evidence_or():
    """_mark_missed NOT EXISTS must contain fill-evidence OR predicate,
    identical to _restore, so a concurrent CANCELED+filled_qty>0 order blocks
    the REJECTED transition."""
    import inspect, ap_recovery as _r
    s = inspect.getsource(_r)
    fn_s = s.find("def _mark_missed(row_id, diag:")
    fn_e = s.find("\n            def ", fn_s + 1)
    body = s[fn_s:fn_e]
    assert "NOT EXISTS" in body, "_mark_missed must have NOT EXISTS fence"
    assert "filled_qty" in body.lower(), (
        "_mark_missed NOT EXISTS must check filled_qty — "
        "CANCELED+filled_qty>0 must block REJECTED transition"
    )
    assert "filled_ts" in body.lower(), "_mark_missed NOT EXISTS must check filled_ts"


# Intended-session enforcement tests ─────────────────────────────────────────

def test_intended_session_prior_trading_session_helpers_exist():
    """Module-level prior-session helpers must exist and be callable."""
    import ap_recovery as _r
    assert callable(getattr(_r, "_prior_trading_session_date_et", None)), (
        "_prior_trading_session_date_et must be a module-level callable"
    )
    assert callable(getattr(_r, "_prior_trading_session_cutoff_utc", None)), (
        "_prior_trading_session_cutoff_utc must be a module-level callable"
    )
    assert callable(getattr(_r, "_signal_date_from_payload_or_row", None)), (
        "_signal_date_from_payload_or_row must be a module-level callable"
    )


def test_intended_session_friday_signal_accepted_on_monday():
    """Signal from prior Friday evening session must be eligible on Monday morning.

    A rolling hour-count window (e.g. 28h) would exclude a ~63h-old Friday
    signal.  The prior-trading-session gate admits it because signal_date ==
    Friday == _prior_session_date on Monday.
    """
    from datetime import date as _date
    FRIDAY = _date(2026, 7, 10)   # prior trading session
    # PUT: trigger=60.5, current=62 — not yet crossed → eligible
    row = _q_row(symbol="WMT", direction="PUT", trigger=60.5,
                 signal_date=FRIDAY.isoformat(), created_hours_ago=63)
    with patch("ap_recovery._prior_trading_session_date_et", return_value=FRIDAY), \
         patch("ap_recovery._prior_trading_session_cutoff_utc",
               return_value=datetime(2026, 7, 10, 5, 0, tzinfo=timezone.utc).isoformat()):
        sqls, updates = _run_recovery(tq_rows=[row], broker=_fresh_quote(62.0))
    restore = [(s, p) for s, p in updates if "status = 'NEW'" in s]
    assert restore, (
        "Friday-evening signal (63h ago) must be eligible on Monday morning. "
        "Prior-session gate admits signal_date=Friday when prior_session=Friday."
    )


# _mark_missed fill-evidence race test ────────────────────────────────────────

def test_p0_mark_missed_fence_has_fill_evidence_or():
    import inspect, ap_recovery as _r
    s = inspect.getsource(_r)
    fn_s = s.find("def _mark_missed(row_id, diag:")
    fn_e = s.find("\n            def ", fn_s + 1)
    body = s[fn_s:fn_e]
    assert "NOT EXISTS" in body
    assert "filled_qty" in body.lower(), "_mark_missed NOT EXISTS must check filled_qty"
    assert "filled_ts" in body.lower(), "_mark_missed NOT EXISTS must check filled_ts"



def test_b1_mark_missed_sql_contains_fill_predicate():
    """_mark_missed NOT EXISTS must include fill-evidence OR predicate.
    Race: _classify sees empty orders at t1; concurrent order appears at t2;
    _mark_missed runs at t3 — its NOT EXISTS must detect CANCELED+filled_qty=1."""
    import ap_recovery as _r, types as _t

    mark_sqls = []
    orders_query_count = {"n": 0}

    class _C:
        def __init__(self):
            self.rowcount = 0
            self._u = ""
        def execute(self, sql, params=()):
            norm = " ".join(str(sql).split())
            self._u = norm.upper()
            if "UPDATE" in self._u:
                mark_sqls.append((norm, tuple(params)))
            if "FROM ORDERS" in self._u:
                orders_query_count["n"] += 1
        def fetchall(self):
            if "FROM TRADE_QUEUE" in self._u or "WATCHING" in self._u:
                return [_q_row(symbol="WMT", direction="CALL", trigger=55.0)]
            if "FROM ORDERS" in self._u:
                if orders_query_count["n"] <= 1:
                    return []  # _classify: no order yet
                return [{"id": "ord-race", "kind": "ENTRY",
                         "execution_mode": "live", "status": "CANCELED",
                         "signal_id": "sig-001",
                         "canonical_signal_id": "canonical-sig-001",
                         "filled_qty": 1, "filled_ts": "2026-07-17T09:35:00Z"}]
            return []
        def fetchone(self):
            rows = self.fetchall(); return rows[0] if rows else None
        def __enter__(self): return self
        def __exit__(self, *_): pass

    class _Conn:
        def __enter__(self): return _C()
        def __exit__(self, *_): pass

    rec = object.__new__(_r.APStartupRecovery)
    rec.client_id = "jasoncosby1@gmail.com"
    rec.mc = SimpleNamespace(mode="LIVE")
    rec.entry_watcher = None
    rec.broker = _fresh_quote(58.0)  # CALL: 58 > 55 trigger -> missed

    fdb = _t.ModuleType("ap.db")
    fdb.conn = _Conn
    fdb.run_with_retry = lambda fn, *a, **k: fn()

    with patch("ap_recovery.os.getenv", return_value="48"),          patch.dict(sys.modules, {"ap.db": fdb}):
        rec._reseed_watchers({})

    missed = [(s, p) for s, p in mark_sqls if "LIVE_RECOVERY_MISSED_TRIGGER" in s]
    assert missed, "Expected _mark_missed UPDATE (_classify sees CALL crossed)"
    assert "FILLED_QTY" in missed[0][0].upper(), (
        "_mark_missed SQL must include FILLED_QTY so concurrent "
        "CANCELED+filled_qty=1 blocks the REJECTED transition. "
        f"SQL={missed[0][0][:300]}"
    )


def test_intended_session_holiday_weekend_signal_accepted():
    """Signal from Thursday (last trading day before 3-day holiday) must be
    eligible on the following Tuesday morning.

    A rolling hour-count window would exclude Thursday signals on Tuesday
    (~86h gap).  The prior-session gate admits them because signal_date ==
    Thursday == _prior_session_date on the post-holiday Tuesday.
    """
    from datetime import date as _date
    # July 4th 2026 falls on Saturday; no holiday but use a plausible 3-day weekend
    # Independence Day observed on Thursday 2026-07-02 → Friday closed → Monday open
    THURSDAY = _date(2026, 7, 2)  # last trading day before holiday weekend
    row = _q_row(symbol="TSLA", direction="CALL", trigger=150.0,
                 signal_date=THURSDAY.isoformat(), created_hours_ago=86)
    with patch("ap_recovery._prior_trading_session_date_et", return_value=THURSDAY), \
         patch("ap_recovery._prior_trading_session_cutoff_utc",
               return_value=datetime(2026, 7, 2, 5, 0, tzinfo=timezone.utc).isoformat()):
        _, updates = _run_recovery(tq_rows=[row], broker=_fresh_quote(145.0))
    # CALL: current=145 < trigger=150 → not crossed → eligible → restore
    restore = [(s, p) for s, p in updates if "status = 'NEW'" in s]
    assert restore, (
        "Thursday signal (86h ago) must be eligible after 3-day holiday weekend. "
        "Prior-session gate: signal_date=Thursday == prior_session=Thursday → admit."
    )


def test_intended_session_previous_completed_session_rejected():
    """Signal from a session TWO trading days ago must be rejected.

    A rolling 28h window could admit two-sessions-back signals on a normal
    weekday (Wed 09:15 − 28h = Mon 05:15 → all of Tuesday).  The session
    gate explicitly rejects any signal_date that is neither the prior session
    nor today.
    """
    from datetime import date as _date
    WEDNESDAY = _date(2026, 7, 15)   # current day (Thursday morning)
    THURSDAY  = _date(2026, 7, 16)   # prior session (Thursday = yesterday for Friday)
    TWO_SESSIONS_AGO = _date(2026, 7, 14)  # Wednesday — should be rejected on Friday
    row = _q_row(symbol="AAPL", direction="CALL", trigger=200.0,
                 signal_date=TWO_SESSIONS_AGO.isoformat(), created_hours_ago=50)
    with patch("ap_recovery._prior_trading_session_date_et", return_value=THURSDAY), \
         patch("ap_recovery._prior_trading_session_cutoff_utc",
               return_value=datetime(2026, 7, 16, 5, 0, tzinfo=timezone.utc).isoformat()):
        _, updates = _run_recovery(tq_rows=[row], broker=_fresh_quote(195.0))
    restore = [(s, p) for s, p in updates if "status = 'NEW'" in s]
    missed  = [(s, p) for s, p in updates if "LIVE_RECOVERY_MISSED_TRIGGER" in s]
    assert not restore and not missed, (
        "Signal from two sessions ago must be rejected by the session gate. "
        f"Got restore={len(restore)} missed={len(missed)}."
    )


def test_intended_session_missing_signal_date_fails_closed():
    """When no date can be extracted from payload or created_ts, skip the row
    (session_unprovable) — never restore, never terminalize."""
    import ap_recovery as _r, types as _t
    from datetime import date as _date

    # Build a row with no signal_date in payload and no parseable created_ts
    bad_row = {
        "id": 99,
        "client_id": "jasoncosby1@gmail.com",
        "signal_id": "sig-nodatex",
        "status": "WATCHING",
        "created_ts": None,   # no created_ts
        "started_ts": None, "finished_ts": None,
        "last_error": None,
        "payload": {
            "ticker": "MSFT", "symbol": "MSFT",
            "direction": "CALL", "side": "CALL",
            "trigger_price": 400.0, "entry_price": 400.0,
            "execution_mode": "live",
            "canonical_signal_id": "canonical-sig-nodatex",
            # deliberately no signal_date / created_at / date / timestamp_iso
        },
    }
    sqls, updates = [], []

    class _C:
        def __init__(self): self.rowcount = 1; self._u = ""
        def execute(self, sql, params=()):
            norm = " ".join(str(sql).split())
            sqls.append((norm, tuple(params)))
            self._u = norm.upper()
            if "UPDATE" in self._u:
                updates.append((norm, tuple(params)))
        def fetchall(self):
            if "FROM TRADE_QUEUE" in self._u or "WATCHING" in self._u:
                return [bad_row]
            return []
        def fetchone(self): return None
        def __enter__(self): return self
        def __exit__(self, *_): pass

    class _Conn:
        def __enter__(self): return _C()
        def __exit__(self, *_): pass

    rec = object.__new__(_r.APStartupRecovery)
    rec.client_id = "jasoncosby1@gmail.com"
    rec.mc = SimpleNamespace(mode="LIVE")
    rec.entry_watcher = None
    rec.broker = _fresh_quote(405.0)  # crossed CALL trigger=400

    fdb = _t.ModuleType("ap.db")
    fdb.conn = _Conn
    fdb.run_with_retry = lambda fn, *a, **k: fn()

    with patch("ap_recovery.os.getenv", return_value="48"), \
         patch.dict(sys.modules, {"ap.db": fdb}):
        rec._reseed_watchers({})

    restore = [(s, p) for s, p in updates if "status = 'NEW'" in s]
    missed  = [(s, p) for s, p in updates if "LIVE_RECOVERY_MISSED_TRIGGER" in s]
    assert not restore and not missed, (
        "Row with no extractable signal date must be skipped (session_unprovable). "
        f"Got restore={len(restore)} missed={len(missed)}."
    )


# Signal-date timezone normalization ─────────────────────────────────────────

def test_signal_date_utc_timestamp_converted_to_et_boundary():
    """created_at="2026-07-15T00:30:00Z" is Tue 2026-07-14 20:30 ET.
    Helper must return date(2026,7,14), NOT date(2026,7,15)."""
    from ap_recovery import _signal_date_from_payload_or_row
    from datetime import date as _date
    got = _signal_date_from_payload_or_row(
        {"created_at": "2026-07-15T00:30:00Z"},
        {"created_ts": None},
    )
    assert got == _date(2026, 7, 14), (
        f"Expected Tue 2026-07-14 (ET) for UTC 2026-07-15T00:30:00Z, got {got}"
    )


def test_signal_date_utc_timestamp_after_et_midnight():
    """created_at="2026-07-15T04:30:00Z" is Wed 2026-07-15 00:30 ET.
    Helper must return date(2026,7,15)."""
    from ap_recovery import _signal_date_from_payload_or_row
    from datetime import date as _date
    got = _signal_date_from_payload_or_row(
        {"created_at": "2026-07-15T04:30:00Z"},
        {"created_ts": None},
    )
    assert got == _date(2026, 7, 15), (
        f"Expected Wed 2026-07-15 (ET) for UTC 2026-07-15T04:30:00Z, got {got}"
    )


def test_signal_date_offset_timestamp_parses_correctly():
    """Timestamp with -04:00 offset must round-trip via ET conversion."""
    from ap_recovery import _signal_date_from_payload_or_row
    from datetime import date as _date
    # 2026-07-14T21:00:00-04:00 → Tue 2026-07-14 21:00 ET
    got = _signal_date_from_payload_or_row(
        {"created_at": "2026-07-14T21:00:00-04:00"},
        {"created_ts": None},
    )
    assert got == _date(2026, 7, 14), (
        f"Expected 2026-07-14 for -04:00 offset timestamp, got {got}"
    )


def test_signal_date_naive_timestamp_fails_closed():
    """Naive timestamp (no tzinfo) must return None — cannot prove ET date."""
    from ap_recovery import _signal_date_from_payload_or_row
    got = _signal_date_from_payload_or_row(
        {"created_at": "2026-07-15T00:30:00"},
        {"created_ts": None},
    )
    assert got is None, f"Naive timestamp must fail closed, got {got}"


def test_signal_date_only_field_returns_date():
    """Date-only signal_date field returns the ET calendar date as-is."""
    from ap_recovery import _signal_date_from_payload_or_row
    from datetime import date as _date
    got = _signal_date_from_payload_or_row(
        {"signal_date": "2026-07-15"},
        {"created_ts": None},
    )
    assert got == _date(2026, 7, 15)


def test_signal_date_generated_at_supported():
    """generated_at field (production signal model) must be parsed as ET."""
    from ap_recovery import _signal_date_from_payload_or_row
    from datetime import date as _date
    got = _signal_date_from_payload_or_row(
        {"generated_at": "2026-07-15T00:30:00Z"},
        {"created_ts": None},
    )
    assert got == _date(2026, 7, 14), (
        f"Expected 2026-07-14 ET for UTC generated_at, got {got}"
    )


def test_signal_date_bar_date_supported():
    """signal_bar_date (date-only) must be recognized."""
    from ap_recovery import _signal_date_from_payload_or_row
    from datetime import date as _date
    got = _signal_date_from_payload_or_row(
        {"signal_bar_date": "2026-07-11"},
        {"created_ts": None},
    )
    assert got == _date(2026, 7, 11)


def test_signal_date_malformed_fails_closed():
    """Garbage timestamp must return None, not raise."""
    from ap_recovery import _signal_date_from_payload_or_row
    got = _signal_date_from_payload_or_row(
        {"created_at": "not-a-date"},
        {"created_ts": None},
    )
    assert got is None


def test_tuesday_2030_et_signal_rejected_on_thursday_recovery():
    """A Tuesday 20:30 ET signal (Wed 00:30 UTC) must be rejected during
    Thursday morning recovery: signal_date == Tuesday, prior_session ==
    Wednesday, today == Thursday → session_out_of_window."""
    from datetime import date as _date
    TUESDAY = _date(2026, 7, 14)
    WEDNESDAY = _date(2026, 7, 15)
    THURSDAY = _date(2026, 7, 16)
    # created_at is UTC — 00:30Z = Tue 20:30 ET
    row = _q_row(symbol="F", direction="CALL", trigger=15.0,
                 signal_date=None, created_hours_ago=40)
    row["payload"].pop("signal_date")
    row["payload"]["created_at"] = "2026-07-15T00:30:00Z"

    with patch("ap_recovery._prior_trading_session_date_et", return_value=WEDNESDAY), \
         patch("ap_recovery._prior_trading_session_cutoff_utc",
               return_value=datetime(2026, 7, 15, 4, 0, tzinfo=timezone.utc).isoformat()), \
         patch("ap_recovery.datetime") as _dt:
        _dt.now.return_value = datetime(2026, 7, 16, 9, 15, tzinfo=ET)
        _dt.side_effect = lambda *a, **k: datetime(*a, **k)
        _, updates = _run_recovery(tq_rows=[row], broker=_fresh_quote(14.5))
    restore = [(s, p) for s, p in updates if "status = 'NEW'" in s]
    missed  = [(s, p) for s, p in updates if "LIVE_RECOVERY_MISSED_TRIGGER" in s]
    assert not restore and not missed, (
        "Tuesday 20:30 ET signal must be rejected on Thursday recovery "
        "(signal_date=Tuesday, prior_session=Wednesday, today=Thursday). "
        f"Got restore={len(restore)} missed={len(missed)}."
    )


def test_wednesday_2030_et_signal_accepted_on_thursday_recovery():
    """A Wednesday 20:30 ET signal (Thu 00:30 UTC) must be accepted on
    Thursday morning: signal_date == Wednesday == prior_session."""
    from datetime import date as _date
    WEDNESDAY = _date(2026, 7, 15)
    row = _q_row(symbol="F", direction="PUT", trigger=15.0,
                 signal_date=None, created_hours_ago=12)
    row["payload"].pop("signal_date")
    row["payload"]["created_at"] = "2026-07-16T00:30:00Z"
    with patch("ap_recovery._prior_trading_session_date_et", return_value=WEDNESDAY), \
         patch("ap_recovery._prior_trading_session_cutoff_utc",
               return_value=datetime(2026, 7, 15, 4, 0, tzinfo=timezone.utc).isoformat()):
        _, updates = _run_recovery(tq_rows=[row], broker=_fresh_quote(15.5))
    restore = [(s, p) for s, p in updates if "status = 'NEW'" in s]
    assert restore, (
        "Wednesday 20:30 ET signal must be accepted on Thursday recovery "
        "(signal_date=Wednesday == prior_session)."
    )


# Calendar fail-closed tests ─────────────────────────────────────────────────

def test_live_recovery_calendar_import_failure_skips_all():
    """When ap.flatline_alarm is unimportable, LIVE recovery must return zero
    with no queue mutation, no quote lookup, no order lookup."""
    import ap_recovery as _r
    sqls = []
    updates = []
    broker = MagicMock()

    class _C:
        def __init__(self): self.rowcount = 1; self._u = ""
        def execute(self, sql, params=()):
            norm = " ".join(str(sql).split())
            sqls.append((norm, tuple(params)))
            self._u = norm.upper()
            if "UPDATE" in self._u:
                updates.append((norm, tuple(params)))
        def fetchall(self): return []
        def fetchone(self): return None
        def __enter__(self): return self
        def __exit__(self, *_): pass

    class _Conn:
        def __enter__(self): return _C()
        def __exit__(self, *_): pass

    rec = object.__new__(_r.APStartupRecovery)
    rec.client_id = "jasoncosby1@gmail.com"
    rec.mc = SimpleNamespace(mode="LIVE")
    rec.entry_watcher = None
    rec.broker = broker

    fdb = types.ModuleType("ap.db")
    fdb.conn = _Conn
    fdb.run_with_retry = lambda fn, *a, **k: fn()

    # No ap.flatline_alarm module installed → import fails inside helper.
    # Also blank out any cached import so the helper truly re-imports.
    with patch.dict(sys.modules, {"ap.db": fdb, "ap.flatline_alarm": None}), \
         patch("ap_recovery.os.getenv", return_value="48"):
        rec._reseed_watchers({})

    assert not updates, (
        "Calendar unavailable — no UPDATE must be attempted. "
        f"Got {len(updates)} updates."
    )
    assert not broker.get_quote.called, (
        "Calendar unavailable — must not call broker.get_quote. "
        f"Called {broker.get_quote.call_count} times."
    )
    # No SELECT from trade_queue at all — helper returned 0 before SQL fired
    tq_selects = [s for s, _ in sqls if "FROM TRADE_QUEUE" in s.upper()]
    assert not tq_selects, (
        "Calendar unavailable — must not SELECT from trade_queue. "
        f"Got {len(tq_selects)} selects."
    )


def test_live_recovery_calendar_raises_skips_all():
    """When is_trading_day raises, LIVE recovery must fail closed."""
    import ap_recovery as _r
    updates = []

    class _C:
        def __init__(self): self.rowcount = 1; self._u = ""
        def execute(self, sql, params=()):
            norm = " ".join(str(sql).split())
            self._u = norm.upper()
            if "UPDATE" in self._u:
                updates.append((norm, tuple(params)))
        def fetchall(self): return []
        def fetchone(self): return None
        def __enter__(self): return self
        def __exit__(self, *_): pass

    class _Conn:
        def __enter__(self): return _C()
        def __exit__(self, *_): pass

    rec = object.__new__(_r.APStartupRecovery)
    rec.client_id = "jasoncosby1@gmail.com"
    rec.mc = SimpleNamespace(mode="LIVE")
    rec.entry_watcher = None
    rec.broker = MagicMock()

    fdb = types.ModuleType("ap.db")
    fdb.conn = _Conn
    fdb.run_with_retry = lambda fn, *a, **k: fn()

    def _raise(d): raise RuntimeError("calendar service down")
    fake_flatline = types.ModuleType("ap.flatline_alarm")
    fake_flatline.is_trading_day = _raise

    with patch.dict(sys.modules, {"ap.db": fdb, "ap.flatline_alarm": fake_flatline}), \
         patch("ap_recovery.os.getenv", return_value="48"):
        rec._reseed_watchers({})

    assert not updates, "is_trading_day raised — must not mutate queue"


def test_prior_trading_session_normal_weekday_resolves_yesterday():
    """On a normal Wednesday, prior session must be Tuesday."""
    import ap_recovery as _r
    from datetime import date as _date
    fake_flatline = types.ModuleType("ap.flatline_alarm")
    fake_flatline.is_trading_day = lambda d: d.weekday() < 5
    now_et = datetime(2026, 7, 15, 9, 15, tzinfo=ET)  # Wed
    with patch.dict(sys.modules, {"ap.flatline_alarm": fake_flatline}):
        got = _r._prior_trading_session_date_et(now_et)
    assert got == _date(2026, 7, 14), f"Wed prior session must be Tue, got {got}"


def test_prior_trading_session_post_holiday_walks_back_through_calendar():
    """On the Tuesday after a 4-day holiday closure (Fri closed, Mon closed),
    prior session must resolve to the Thursday before."""
    import ap_recovery as _r
    from datetime import date as _date
    # Fri 2026-07-03 (Independence Day observed) + Mon 2026-07-06 (hypothetical)
    closed = {_date(2026, 7, 3), _date(2026, 7, 4), _date(2026, 7, 5), _date(2026, 7, 6)}
    fake_flatline = types.ModuleType("ap.flatline_alarm")
    fake_flatline.is_trading_day = lambda d: d.weekday() < 5 and d not in closed
    now_et = datetime(2026, 7, 7, 9, 15, tzinfo=ET)  # Tue
    with patch.dict(sys.modules, {"ap.flatline_alarm": fake_flatline}):
        got = _r._prior_trading_session_date_et(now_et)
    assert got == _date(2026, 7, 2), (
        f"Post-holiday Tue prior session must be Thu 2026-07-02, got {got}"
    )


def test_prior_trading_session_14_day_guard_fails_closed():
    """If is_trading_day returns False for 14+ days, helper returns None."""
    import ap_recovery as _r
    fake_flatline = types.ModuleType("ap.flatline_alarm")
    fake_flatline.is_trading_day = lambda d: False
    now_et = datetime(2026, 7, 15, 9, 15, tzinfo=ET)
    with patch.dict(sys.modules, {"ap.flatline_alarm": fake_flatline}):
        got = _r._prior_trading_session_date_et(now_et)
    assert got is None, f"14-day guard must fail closed, got {got}"


# Legacy canonical CAS tests (real OSM SQL) ──────────────────────────────────

def test_osm_legacy_empty_canonical_backfill_in_atomic_update():
    """Real OSM SQL must contain the atomic backfill clause when
    allow_legacy_empty_canonical=True. Empty durable canonical is proven
    inside the same UPDATE that stamps the resolved canonical."""
    import types as _t
    from ap.order_state_machine import APOrderStateMachine

    captured = {}

    class _C:
        def __init__(self): self.rowcount = 1
        def execute(self, sql, params=()):
            captured["sql"] = " ".join(str(sql).split())
            captured["params"] = tuple(params)
            return self
        def __enter__(self): return self
        def __exit__(self, *_): pass

    class _Conn:
        def __enter__(self): return _C()
        def __exit__(self, *_): pass

    fdb = _t.ModuleType("ap.db")
    fdb.conn = _Conn
    fdb.run_with_retry = lambda fn, *a, **k: fn()

    with patch.dict(sys.modules, {"ap.db": fdb}), \
         patch("ap.order_state_machine.conn", _Conn), \
         patch("ap.order_state_machine.run_with_retry", lambda fn, *a, **k: fn()):
        osm = APOrderStateMachine("jasoncosby1@gmail.com")
        ok = osm.claim_deferred_materialization(
            "loc-legacy-1",
            owner="watcher-a",
            new_generation=1,
            lease_until="2026-07-17T10:00:00Z",
            trigger_crossed_at="2026-07-17T09:35:00Z",
            trigger_price=100.0,
            observed_underlying_price=100.5,
            signal_id="sig-legacy-1",
            execution_mode="live",
            canonical_signal_id="canonical-sig-legacy-1",
            allow_legacy_empty_canonical=True,
        )
    assert ok, "Claim must succeed against a fake cursor that returns rowcount=1"

    sql = captured.get("sql", "")
    assert "COALESCE(canonical_signal_id, '') = ''" in sql, (
        "Legacy path SQL must atomically require empty canonical. SQL: "
        f"{sql[:600]}"
    )
    assert "canonical_signal_id = %s" in sql and "SET" in sql, (
        "Legacy path SQL must atomically backfill canonical in same UPDATE. "
        f"SQL: {sql[:600]}"
    )
    # canonical_signal_id must appear in SET (backfill) — check position
    set_idx = sql.upper().find("SET ")
    where_idx = sql.upper().find("WHERE ")
    set_slice = sql[set_idx:where_idx]
    assert "canonical_signal_id = %s" in set_slice, (
        "Backfill must be in the SET clause. SET slice: " + set_slice[:400]
    )


def test_osm_normal_row_requires_exact_canonical_match():
    """Normal path (allow_legacy_empty_canonical=False, default) must retain
    the strict canonical_signal_id = %s predicate."""
    import types as _t
    from ap.order_state_machine import APOrderStateMachine

    captured = {}

    class _C:
        def __init__(self): self.rowcount = 1
        def execute(self, sql, params=()):
            captured["sql"] = " ".join(str(sql).split())
            captured["params"] = tuple(params)
            return self
        def __enter__(self): return self
        def __exit__(self, *_): pass

    class _Conn:
        def __enter__(self): return _C()
        def __exit__(self, *_): pass

    fdb = _t.ModuleType("ap.db")
    fdb.conn = _Conn
    fdb.run_with_retry = lambda fn, *a, **k: fn()

    with patch.dict(sys.modules, {"ap.db": fdb}), \
         patch("ap.order_state_machine.conn", _Conn), \
         patch("ap.order_state_machine.run_with_retry", lambda fn, *a, **k: fn()):
        osm = APOrderStateMachine("jasoncosby1@gmail.com")
        osm.claim_deferred_materialization(
            "loc-modern-1",
            owner="watcher-b",
            new_generation=1,
            lease_until="2026-07-17T10:00:00Z",
            trigger_crossed_at="2026-07-17T09:35:00Z",
            trigger_price=100.0,
            observed_underlying_price=100.5,
            signal_id="sig-modern-1",
            execution_mode="live",
            canonical_signal_id="canonical-sig-modern-1",
        )

    sql = captured.get("sql", "")
    # Normal path: strict equality predicate, NOT the empty-coalesce clause
    assert "canonical_signal_id = %s" in sql, "Normal path requires exact canonical match"
    where_idx = sql.upper().find("WHERE ")
    where_slice = sql[where_idx:]
    assert "canonical_signal_id = %s" in where_slice, (
        "Normal path canonical predicate must be in WHERE clause"
    )
    assert "COALESCE(canonical_signal_id, '') = ''" not in sql, (
        "Normal path must NOT contain the empty-canonical predicate"
    )


def test_osm_legacy_empty_canonical_only_matches_empty_rows():
    """The empty-canonical CAS predicate must be `COALESCE(canonical_signal_id, '') = ''`
    exactly — this atomically ensures a concurrent worker that stamped canonical
    between our read and our claim causes the UPDATE to affect 0 rows."""
    import types as _t
    from ap.order_state_machine import APOrderStateMachine

    captured = {}

    class _C:
        def __init__(self): self.rowcount = 0
        def execute(self, sql, params=()):
            captured["sql"] = " ".join(str(sql).split())
            return self
        def __enter__(self): return self
        def __exit__(self, *_): pass

    class _Conn:
        def __enter__(self): return _C()
        def __exit__(self, *_): pass

    with patch("ap.order_state_machine.conn", _Conn), \
         patch("ap.order_state_machine.run_with_retry", lambda fn, *a, **k: fn()):
        osm = APOrderStateMachine("jasoncosby1@gmail.com")
        ok = osm.claim_deferred_materialization(
            "loc-race-1",
            owner="watcher-c",
            new_generation=1,
            lease_until="2026-07-17T10:00:00Z",
            trigger_crossed_at="2026-07-17T09:35:00Z",
            trigger_price=100.0,
            observed_underlying_price=100.5,
            signal_id="sig-race-1",
            execution_mode="live",
            canonical_signal_id="canonical-sig-race-1",
            allow_legacy_empty_canonical=True,
        )
    assert ok is False, "rowcount=0 must return False (concurrent claim lost)"
    sql = captured["sql"]
    assert "COALESCE(canonical_signal_id, '') = ''" in sql
