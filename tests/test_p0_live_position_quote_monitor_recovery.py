"""P0 — Live Position Quote Monitor Recovery (PR #387)

Surgical tests for the July 21 LIVE stale-position monitoring incident.
All tests exercise real production code paths; no exit or order logic is touched.
"""
from __future__ import annotations

import os
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock, patch, call

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")

import ap.position_quote_monitor as _qpm_mod
from ap.position_quote_monitor import (
    APPositionQuoteMonitor,
    HEARTBEAT_DEGRADED_SEC,
    DEGRADED_MAX_SEC,
    QPM_DIRECT_RECOVERY_MIN_INTERVAL_SEC,
    _broker_transport_identity,
)

# ── Shared constants ────────────────────────────────────────────────────────

_CONTRACT  = "SPY260717C00550000"
_TICKER    = "SPY"
_POS_ID    = "pos-test-001"
_CLIENT    = "test@example.com"
_CLIENT_B  = "other@example.com"


# ── Minimal position dataclass ──────────────────────────────────────────────

@dataclass
class _Pos:
    position_id: str = _POS_ID
    positionid: str = _POS_ID
    client_id: str = _CLIENT
    execution_mode: str = "live"
    executionmode: str = "live"
    ticker: str = _TICKER
    option_symbol: str = _CONTRACT
    optionsymbol: str = _CONTRACT
    entry_price: float = 1.00
    entryprice: float = 1.00
    current_option_price: float = 0.0
    currentoptionprice: float = 0.0
    current_bid: float = 0.0
    currentbid: float = 0.0
    current_ask: float = 0.0
    currentask: float = 0.0
    current_underlying: float = 0.0
    currentunderlying: float = 0.0
    last_option_quote_update_ts: Optional[datetime] = None
    lastoptionquoteupdatets: Optional[datetime] = None
    last_underlying_quote_update_ts: Optional[datetime] = None
    lastunderlyingquoteupdatets: Optional[datetime] = None
    quotestate: str = ""
    quote_state: str = ""
    peak_pnl_pct: float = 0.0
    peakpnlpct: float = 0.0
    max_profit_seen: float = 0.0
    maxprofitseen: float = 0.0
    option_pnl_pct: float = 0.0
    optionpnlpct: float = 0.0
    touched_profit: bool = False
    touchedprofit: bool = False

    def __setattr__(self, name, value):
        object.__setattr__(self, name, value)


# ── QPM factory ────────────────────────────────────────────────────────────

def _make_broker(*, bid=1.10, ask=1.20, und=550.0, base_url="https://api.tradier.com"):
    broker = MagicMock()
    broker.base_url = base_url
    cfg = MagicMock()
    cfg.base_url = base_url
    cfg.baseurl = base_url
    cfg.account_id = "ACC123"
    broker.cfg = cfg
    broker.account_id = "ACC123"
    opt_quote = {"symbol": _CONTRACT, "bid": bid, "ask": ask, "last": bid, "mark": (bid + ask) / 2}
    und_quote = {"symbol": _TICKER, "last": und}
    broker.get_quotes = MagicMock(return_value={_CONTRACT: opt_quote, _TICKER: und_quote})
    return broker


def _make_exit_engine(positions):
    ee = MagicMock()
    ee.active_positions = MagicMock(return_value=positions)
    ee._lock = threading.Lock()
    ee.applyquotesnapshots = MagicMock()
    ee.apply_quote_snapshots = MagicMock()
    event = threading.Event()
    ee.quotearrivedevent = event
    ee.quote_arrived_event = event
    return ee


_BASE_METRICS = {
    "cycles": 0, "cache_hits": 0, "cache_misses": 0, "rate_limited": 0,
    "batch_calls": 0, "fallback_calls": 0, "spread_rejects": 0,
    "blind_alerts": 0, "wakes_sent": 0, "wakes_suppressed": 0,
    "immediate_retry_requests": 0, "immediate_retry_coalesced": 0,
    "immediate_retry_evictions": 0, "immediate_retry_backoff_suppressed": 0,
    "exits_gated_blind": 0, "exits_gated_stale": 0,
    "direct_recovery_attempts": 0, "direct_recovery_successes": 0,
    "direct_recovery_failures": 0, "direct_recovery_suppressed": 0,
    "coverage_incomplete_cycles": 0,
}


def _make_qpm(
    positions,
    *,
    broker=None,
    exit_engine=None,
    execution_mode="live",
    account_id="ACC123",
    client_id=_CLIENT,
) -> APPositionQuoteMonitor:
    broker = broker or _make_broker()
    ee = exit_engine or _make_exit_engine(positions)

    qpm = APPositionQuoteMonitor.__new__(APPositionQuoteMonitor)
    qpm.broker = broker
    qpm.client_id = client_id
    qpm.exit_engine = ee
    qpm._alert_fn = lambda m: None
    qpm._interval = 2.0
    qpm.execution_mode = execution_mode.lower()
    qpm.account_id = account_id
    qpm._health_key = f"ap_quote_monitor:{client_id}:{execution_mode.lower() or 'unknown'}"
    qpm._cache_namespace = f"{execution_mode.lower() or 'unknown'}|{_broker_transport_identity(broker)}"
    qpm._last_successful_refresh_ts = 0.0
    qpm._last_active_position_count = 0
    qpm._last_direct_recovery_ts = {}
    qpm._coverage_degraded = False
    qpm._stop = threading.Event()
    qpm._kick = threading.Event()
    qpm._thread = None
    qpm._health = {}
    qpm._health_lock = threading.Lock()
    qpm._last_push_price = {}
    qpm._last_wake_price = {}
    qpm._last_wake_ts = {}
    qpm._last_immediate_refresh_ts = {}
    qpm._last_db_persist_ts = {}
    qpm._last_db_persist_price = {}
    qpm._orders_meta_available = False
    qpm._cycles = 0
    qpm._consecutive_failures = 0
    qpm._rate_limit_backoff_sec = 1.0
    qpm._last_cycle_ts = time.time()
    qpm._metrics = dict(_BASE_METRICS)
    return qpm


# ── Helpers ─────────────────────────────────────────────────────────────────

def _utc_now():
    return datetime.now(timezone.utc)


# ════════════════════════════════════════════════════════════════════════════
# Test 1 — Failed refresh does not advance _last_successful_refresh_ts
# ════════════════════════════════════════════════════════════════════════════

def test_failed_refresh_does_not_advance_successful_ts():
    pos = _Pos()
    # No timestamps on the position — it's never been quoted
    broker = _make_broker(bid=0.0, ask=0.0, und=550.0)
    # Broker returns only the underlying, not the option
    broker.get_quotes = MagicMock(return_value={_TICKER: {"symbol": _TICKER, "last": 550.0}})
    ee = _make_exit_engine([pos])
    qpm = _make_qpm([pos], broker=broker, exit_engine=ee)

    # Suppress direct recovery so it can't promote the timestamp
    qpm._last_direct_recovery_ts[_POS_ID] = time.time()

    _qpm_mod._SHARED_CACHE.clear()
    before = qpm._last_successful_refresh_ts
    result = qpm._refresh_once()

    assert result["coverage_complete"] is False, (
        "No option quote → coverage must be incomplete"
    )
    assert qpm._last_successful_refresh_ts == before, (
        "_last_successful_refresh_ts must not advance when coverage is incomplete"
    )


# ════════════════════════════════════════════════════════════════════════════
# Test 2 — Active position + expired refresh ts → is_healthy() False
# ════════════════════════════════════════════════════════════════════════════

def test_is_healthy_false_when_active_position_and_stale_refresh():
    pos = _Pos()
    qpm = _make_qpm([pos])

    # Simulate alive thread
    mock_thread = MagicMock()
    mock_thread.is_alive.return_value = True
    qpm._thread = mock_thread

    # Recent cycle (not stale)
    qpm._last_cycle_ts = time.time()

    # Position count set
    qpm._last_active_position_count = 1

    # Refresh timestamp is way in the past (expired)
    qpm._last_successful_refresh_ts = time.time() - (HEARTBEAT_DEGRADED_SEC + 60)

    assert qpm.is_healthy() is False


# ════════════════════════════════════════════════════════════════════════════
# Test 3 — No active positions → healthy with thread/cycle liveness only
# ════════════════════════════════════════════════════════════════════════════

def test_is_healthy_true_with_no_active_positions():
    qpm = _make_qpm([])

    mock_thread = MagicMock()
    mock_thread.is_alive.return_value = True
    qpm._thread = mock_thread

    qpm._last_cycle_ts = time.time()
    qpm._last_active_position_count = 0
    qpm._last_successful_refresh_ts = 0.0  # never refreshed

    assert qpm.is_healthy() is True


# ════════════════════════════════════════════════════════════════════════════
# Test 4 — Health keys differ across client and execution mode
# ════════════════════════════════════════════════════════════════════════════

def test_health_keys_differ_across_client_and_mode():
    broker = _make_broker()
    ee = _make_exit_engine([])

    qpm_live = _make_qpm([], broker=broker, exit_engine=ee,
                          execution_mode="live", client_id=_CLIENT)
    qpm_paper = _make_qpm([], broker=broker, exit_engine=ee,
                           execution_mode="paper", client_id=_CLIENT)
    qpm_other = _make_qpm([], broker=broker, exit_engine=ee,
                           execution_mode="live", client_id=_CLIENT_B)

    assert qpm_live._health_key != qpm_paper._health_key
    assert qpm_live._health_key != qpm_other._health_key
    assert qpm_paper._health_key != qpm_other._health_key


# ════════════════════════════════════════════════════════════════════════════
# Test 5 — Shared-cache keys differ across PAPER and LIVE instances
# ════════════════════════════════════════════════════════════════════════════

def test_cache_keys_differ_across_paper_and_live():
    broker_live = _make_broker(base_url="https://api.tradier.com")
    broker_paper = _make_broker(base_url="https://sandbox.tradier.com")

    qpm_live = _make_qpm([], broker=broker_live, execution_mode="live")
    qpm_paper = _make_qpm([], broker=broker_paper, execution_mode="paper")

    live_key = qpm_live._cache_key(_CONTRACT)
    paper_key = qpm_paper._cache_key(_CONTRACT)

    assert live_key != paper_key, (
        "PAPER and LIVE must not share cache records for the same symbol"
    )


# ════════════════════════════════════════════════════════════════════════════
# Test 6 — Fresh cached quote does not trigger direct recovery
# ════════════════════════════════════════════════════════════════════════════

def test_fresh_cached_quote_no_direct_recovery():
    pos = _Pos()
    # Give the position fresh timestamps so it won't look stale
    now_utc = _utc_now()
    pos.last_option_quote_update_ts = now_utc
    pos.lastoptionquoteupdatets = now_utc
    pos.last_underlying_quote_update_ts = now_utc
    pos.lastunderlyingquoteupdatets = now_utc

    broker = _make_broker(bid=1.10, ask=1.20, und=550.0)
    ee = _make_exit_engine([pos])
    qpm = _make_qpm([pos], broker=broker, exit_engine=ee)

    with patch.object(qpm, "_fetch_batch", wraps=qpm._fetch_batch) as mock_fb:
        qpm._refresh_once()
        # _fetch_batch may be called by _fetch_batch_cached for cache misses,
        # but _maybe_direct_recover_position must NOT add an extra call
        # when quotes are fresh.
        direct_calls = qpm._metrics["direct_recovery_attempts"]
        assert direct_calls == 0, (
            f"Direct recovery must not fire for a fresh position; got {direct_calls} attempts"
        )


# ════════════════════════════════════════════════════════════════════════════
# Test 7 — Stale position quote triggers exactly one _fetch_batch() recovery call
# ════════════════════════════════════════════════════════════════════════════

def test_stale_position_triggers_one_direct_recovery_call():
    pos = _Pos()
    # Make the position look stale (old timestamps beyond DEGRADED_MAX_SEC)
    old_ts = _utc_now() - timedelta(seconds=DEGRADED_MAX_SEC + 30)
    pos.last_option_quote_update_ts = old_ts
    pos.lastoptionquoteupdatets = old_ts
    pos.last_underlying_quote_update_ts = old_ts
    pos.lastunderlyingquoteupdatets = old_ts

    ee = _make_exit_engine([pos])
    qpm = _make_qpm([pos], exit_engine=ee)

    recovery_quote = {_CONTRACT: {"symbol": _CONTRACT, "bid": 1.05, "ask": 1.15, "last": 1.05}}
    recovery_und = {_TICKER: {"symbol": _TICKER, "last": 550.0}}

    fetch_batch_calls = []

    def fake_fetch_batch(symbols):
        fetch_batch_calls.append(list(symbols))
        result = {}
        for s in symbols:
            if s in recovery_quote:
                result[s] = recovery_quote[s]
            if s in recovery_und:
                result[s] = recovery_und[s]
        return result

    # _fetch_batch_cached returns empty (no option quote from the normal path)
    # _fetch_batch (used by recovery) returns real data
    qpm._fetch_batch_cached = MagicMock(
        side_effect=lambda syms: {_TICKER: {"symbol": _TICKER, "last": 550.0}}
        if _TICKER in syms else {}
    )
    qpm._fetch_batch = fake_fetch_batch

    qpm._refresh_once()

    assert qpm._metrics["direct_recovery_attempts"] == 1, (
        "Expected exactly one direct recovery attempt for a stale position"
    )


# ════════════════════════════════════════════════════════════════════════════
# Test 8 — Direct recovery suppressed inside per-position minimum interval
# ════════════════════════════════════════════════════════════════════════════

def test_direct_recovery_suppressed_within_min_interval():
    pos = _Pos()
    old_ts = _utc_now() - timedelta(seconds=DEGRADED_MAX_SEC + 30)
    pos.last_option_quote_update_ts = old_ts
    pos.lastoptionquoteupdatets = old_ts
    pos.last_underlying_quote_update_ts = old_ts
    pos.lastunderlyingquoteupdatets = old_ts

    broker = _make_broker(bid=0.0, ask=0.0, und=550.0)
    broker.get_quotes = MagicMock(return_value={})

    ee = _make_exit_engine([pos])
    qpm = _make_qpm([pos], broker=broker, exit_engine=ee)

    # Pre-stamp the recovery timestamp so the per-position rate limit fires
    qpm._last_direct_recovery_ts[_POS_ID] = time.time()

    _qpm_mod._SHARED_CACHE.clear()
    qpm._refresh_once()

    assert qpm._metrics["direct_recovery_suppressed"] >= 1, (
        "Direct recovery must be suppressed when inside the min interval"
    )
    assert qpm._metrics["direct_recovery_attempts"] == 0


# ════════════════════════════════════════════════════════════════════════════
# Test 9 — Shared 429 backoff suppresses direct recovery
# ════════════════════════════════════════════════════════════════════════════

def test_shared_429_backoff_suppresses_direct_recovery():
    pos = _Pos()
    old_ts = _utc_now() - timedelta(seconds=DEGRADED_MAX_SEC + 30)
    pos.last_option_quote_update_ts = old_ts
    pos.lastoptionquoteupdatets = old_ts
    pos.last_underlying_quote_update_ts = old_ts
    pos.lastunderlyingquoteupdatets = old_ts

    broker = _make_broker(bid=0.0, ask=0.0, und=550.0)
    broker.get_quotes = MagicMock(return_value={})

    ee = _make_exit_engine([pos])
    qpm = _make_qpm([pos], broker=broker, exit_engine=ee)

    _qpm_mod._SHARED_CACHE.clear()

    # Set global backoff to future
    original_backoff = _qpm_mod._SHARED_BACKOFF_UNTIL
    try:
        _qpm_mod._SHARED_BACKOFF_UNTIL = time.time() + 30.0
        qpm._refresh_once()

        assert qpm._metrics["direct_recovery_suppressed"] >= 1
        assert qpm._metrics["direct_recovery_attempts"] == 0
    finally:
        _qpm_mod._SHARED_BACKOFF_UNTIL = original_backoff


# ════════════════════════════════════════════════════════════════════════════
# Test 10 — Successful direct recovery reaches snapshot applier and wakes exit engine
# ════════════════════════════════════════════════════════════════════════════

def test_successful_direct_recovery_reaches_snapshot_and_wake():
    pos = _Pos()
    old_ts = _utc_now() - timedelta(seconds=DEGRADED_MAX_SEC + 30)
    pos.last_option_quote_update_ts = old_ts
    pos.lastoptionquoteupdatets = old_ts
    pos.last_underlying_quote_update_ts = old_ts
    pos.lastunderlyingquoteupdatets = old_ts
    pos.entryprice = 1.00
    pos.entry_price = 1.00

    wake_event = threading.Event()
    ee = _make_exit_engine([pos])
    ee.quotearrivedevent = wake_event
    ee.quote_arrived_event = wake_event

    qpm = _make_qpm([pos], exit_engine=ee)

    def fake_fetch_batch(symbols):
        result = {}
        for s in symbols:
            if s == _CONTRACT:
                result[s] = {"symbol": s, "bid": 1.10, "ask": 1.20, "last": 1.10, "mark": 1.15}
            elif s == _TICKER:
                result[s] = {"symbol": s, "last": 550.0}
        return result

    # Normal cache path returns only underlying — option is missing
    qpm._fetch_batch_cached = MagicMock(
        side_effect=lambda syms: {_TICKER: {"symbol": _TICKER, "last": 550.0}}
        if _TICKER in syms else {}
    )
    # Direct recovery path (bypasses cache) returns full quotes
    qpm._fetch_batch = fake_fetch_batch

    result = qpm._refresh_once()

    assert qpm._metrics["direct_recovery_successes"] == 1, (
        "Recovery must succeed when _fetch_batch returns a valid bid"
    )
    assert result["refreshed_positions"] >= 1, (
        "Recovered position must count as refreshed"
    )

    # Snapshot applier must have been called through the existing path
    ee.applyquotesnapshots.assert_called_once()

    # Exit engine must have been woken
    assert wake_event.is_set(), "Exit engine wake event must be set after successful recovery"


# ════════════════════════════════════════════════════════════════════════════
# Test 11 — Failed direct recovery does not overwrite prior timestamps as fresh
# ════════════════════════════════════════════════════════════════════════════

def test_failed_direct_recovery_does_not_overwrite_timestamps():
    old_ts = _utc_now() - timedelta(seconds=DEGRADED_MAX_SEC + 30)
    pos = _Pos()
    pos.last_option_quote_update_ts = old_ts
    pos.lastoptionquoteupdatets = old_ts
    pos.last_underlying_quote_update_ts = old_ts
    pos.lastunderlyingquoteupdatets = old_ts

    broker = _make_broker(bid=0.0, ask=0.0, und=550.0)
    broker.get_quotes = MagicMock(return_value={})

    ee = _make_exit_engine([pos])
    qpm = _make_qpm([pos], broker=broker, exit_engine=ee)

    # Recovery returns empty — failure
    qpm._fetch_batch = MagicMock(return_value={})
    _qpm_mod._SHARED_CACHE.clear()

    qpm._refresh_once()

    assert qpm._metrics["direct_recovery_failures"] == 1

    # The timestamps on the position must remain at the old stale value
    # (not promoted to a fresh "now") — pos.last_option_quote_update_ts
    # should either be unchanged or None (missing quote path), never a new "now".
    current_opt_ts = pos.last_option_quote_update_ts or pos.lastoptionquoteupdatets
    if current_opt_ts is not None and current_opt_ts != old_ts:
        age = (_utc_now() - current_opt_ts).total_seconds()
        assert age > DEGRADED_MAX_SEC, (
            "Failed recovery must not write a fresh timestamp — "
            f"timestamp is only {age:.1f}s old"
        )


# ════════════════════════════════════════════════════════════════════════════
# Test 12 — Runner reuses one alive monitor when all bindings match
# ════════════════════════════════════════════════════════════════════════════

def test_runner_reuses_monitor_when_all_bindings_match():
    sys.path.insert(0, str(REPO_ROOT))

    broker = _make_broker()
    ee = _make_exit_engine([])

    existing_qm = MagicMock(spec=APPositionQuoteMonitor)
    existing_qm.is_alive.return_value = True
    existing_qm.client_id = _CLIENT
    existing_qm.execution_mode = "live"
    existing_qm.account_id = "ACC123"
    existing_qm.broker = broker
    existing_qm.exit_engine = ee

    runner = MagicMock()
    runner.email = _CLIENT
    runner.mode = "live"
    runner.account_id = "ACC123"
    runner.quotemonitor = existing_qm
    runner.quote_monitor = existing_qm
    runner.core = None

    # We call the actual method, bound to our mock runner
    import client_runner as cr_mod
    method = cr_mod.ClientRunner._start_position_quote_monitor.__get__(runner, type(runner))

    with patch.object(cr_mod, "APPositionQuoteMonitor", APPositionQuoteMonitor):
        method(broker=broker, exit_eng=ee)

    # The existing monitor must NOT have been stopped
    existing_qm.stop.assert_not_called()
    # No new monitor was created (runner.quote_monitor still points at existing)
    assert runner.quote_monitor is existing_qm or runner.quotemonitor is existing_qm


# ════════════════════════════════════════════════════════════════════════════
# Test 13 — Runner stops and replaces monitor when any binding differs
# ════════════════════════════════════════════════════════════════════════════

def test_runner_replaces_monitor_when_binding_differs():
    sys.path.insert(0, str(REPO_ROOT))

    broker_old = _make_broker(base_url="https://old.tradier.com")
    broker_new = _make_broker(base_url="https://new.tradier.com")
    ee = _make_exit_engine([])

    existing_qm = MagicMock(spec=APPositionQuoteMonitor)
    existing_qm.is_alive.return_value = True
    existing_qm.client_id = _CLIENT
    existing_qm.execution_mode = "paper"  # DIFFERENT from resolved "live"
    existing_qm.account_id = "ACC123"
    existing_qm.broker = broker_old
    existing_qm.exit_engine = ee

    runner = MagicMock()
    runner.email = _CLIENT
    runner.mode = "live"
    runner.account_id = "ACC123"
    runner.quotemonitor = existing_qm
    runner.quote_monitor = existing_qm
    runner.core = None

    import client_runner as cr_mod

    started_monitors = []

    def fake_init(self_qm, broker, client_id, exit_engine, alert_fn=None,
                  execution_mode="", account_id="", poll_interval_sec=2.0):
        self_qm.broker = broker
        self_qm.client_id = client_id
        self_qm.exit_engine = exit_engine
        self_qm.execution_mode = execution_mode
        self_qm.account_id = account_id
        self_qm.start = MagicMock()
        started_monitors.append(self_qm)

    with patch.object(cr_mod.APPositionQuoteMonitor, "__init__", fake_init):
        with patch.object(cr_mod.APPositionQuoteMonitor, "start", MagicMock()):
            method = cr_mod.ClientRunner._start_position_quote_monitor.__get__(runner, type(runner))
            method(broker=broker_new, exit_eng=ee)

    # Existing mismatched monitor must have been stopped
    existing_qm.stop.assert_called_once()


# ════════════════════════════════════════════════════════════════════════════
# Test 14 — Repeated _start_position_quote_monitor never creates two live threads
# ════════════════════════════════════════════════════════════════════════════

def test_repeated_start_never_creates_two_live_threads():
    sys.path.insert(0, str(REPO_ROOT))

    broker = _make_broker()
    ee = _make_exit_engine([])

    runner = MagicMock()
    runner.email = _CLIENT
    runner.mode = "live"
    runner.account_id = "ACC123"
    runner.quotemonitor = None
    runner.quote_monitor = None
    runner.core = None

    import client_runner as cr_mod

    created = []

    class FakeQPM:
        def __init__(self, broker, client_id, exit_engine, alert_fn=None,
                     execution_mode="", account_id="", poll_interval_sec=2.0):
            self.broker = broker
            self.client_id = client_id
            self.exit_engine = exit_engine
            self.execution_mode = execution_mode
            self.account_id = account_id
            self._alive = False
            created.append(self)

        def is_alive(self):
            return self._alive

        def start(self):
            self._alive = True

        def stop(self):
            self._alive = False

    with patch.object(cr_mod, "APPositionQuoteMonitor", FakeQPM):
        method = cr_mod.ClientRunner._start_position_quote_monitor.__get__(runner, type(runner))

        # First call — creates a monitor
        method(broker=broker, exit_eng=ee)
        runner.quotemonitor = runner.quote_monitor = created[-1]

        # Second call — same bindings → must reuse, not create another
        method(broker=broker, exit_eng=ee)

    live_count = sum(1 for q in created if q.is_alive())
    assert live_count <= 1, (
        f"At most one live monitor thread must exist; found {live_count} alive, "
        f"{len(created)} total created"
    )
    assert len(created) == 1, (
        f"Only one monitor should have been instantiated; got {len(created)}"
    )
