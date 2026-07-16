"""
tests/test_p0_paper_recovery_restart_guard_bypass.py

PR #358 — P0: let current-session PAPER recovery pass the restart guard.

Production incident 2026-07-16: APStartupRecovery reset Jose's 124
overnight-evaluated PAPER WATCHING rows to NEW, stamped recovery_rescue +
recovery_rescue_ts. The queue restart guard ignored that marker; 122 signals
were rejected as restart_guard:overnight_skip after market open.

Root cause: producer-consumer contract mismatch. Fixed in-place in ap/queue.py.
"""

from __future__ import annotations

import sys
import types
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

import ap.queue as queue

ET_NAME = "America/New_York"
from zoneinfo import ZoneInfo
ET = ZoneInfo(ET_NAME)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now_ts(delta=timedelta(0)):
    return (datetime.now(timezone.utc) + delta).isoformat()


def _marker(delta=timedelta(0), *, lookback=48, ts_override=None, rescue_bool=True):
    return {
        "recovery_rescue": rescue_bool,
        "recovery_rescue_ts": ts_override if ts_override is not None else _now_ts(delta),
        "recovery_rescue_lookback_hours": lookback,
        "signal_id": "sig-001", "ticker": "TSLA", "side": "CALL",
        "score": 82.0, "execution_mode": "paper",
    }


def _make_mc(mode):
    mc = MagicMock()
    mc.mode = mode
    return mc


def _run_dispatch(*, job_id, client_id, mode, payload, signal_id,
                  should_skip_return=True):
    """Run _dispatch with restart_guard stubbed and required mocks provided.

    _execution_mode inside _dispatch derives from master_control.mode; there
    is no execution_mode parameter on _dispatch itself.
    """
    rg_stub = types.ModuleType("ap.restart_guard")
    rg_stub.should_skip_on_restart = lambda p: should_skip_return

    rejected = []

    def _fake_mark(jid, status, *, result=None, error=None):
        if error == "restart_guard:overnight_skip":
            rejected.append(error)

    sys.modules["ap.restart_guard"] = rg_stub
    try:
        with patch("ap.queue._mark_job", side_effect=_fake_mark), \
             patch("ap.queue._get_sb_client", return_value=None), \
             patch("ap.queue._log_rejection_to_db", return_value=None):
            try:
                queue._dispatch(
                    job_id=job_id, client_id=client_id,
                    signal_id=signal_id, payload=payload,
                    job_last_error=None, job_result=None,
                    master_control=_make_mc(mode),
                    contract_selector=MagicMock(),
                    order_state_machine=MagicMock(),
                    entry_watcher=MagicMock(),
                )
            except Exception:
                pass
    finally:
        sys.modules.pop("ap.restart_guard", None)
    return rejected


# ── §1 Core classifier ────────────────────────────────────────────────────────

def test_valid_paper_recovery_accepted():
    assert queue._is_current_session_paper_recovery(
        payload=_marker(), execution_mode="PAPER"
    )


def test_case_insensitive_paper():
    for mode in ("paper", "Paper", "PAPER"):
        assert queue._is_current_session_paper_recovery(
            payload=_marker(), execution_mode=mode
        ), f"mode={mode!r} should be accepted"


def test_bypass_enabled_for_valid_marker():
    assert queue._manual_restart_guard_bypass_enabled(
        payload=_marker(), execution_mode="PAPER"
    )


# ── §2 LIVE isolation ─────────────────────────────────────────────────────────

@pytest.mark.parametrize("mode", [
    "LIVE", "live", "Live", " LIVE ", None, "", "unknown",
])
def test_non_paper_mode_rejected(mode):
    assert not queue._is_current_session_paper_recovery(
        payload=_marker(), execution_mode=mode
    ), f"mode={mode!r} must be rejected"


def test_live_bypass_disabled():
    assert not queue._manual_restart_guard_bypass_enabled(
        payload=_marker(), execution_mode="LIVE"
    )


def test_payload_claiming_paper_while_runtime_live_rejected():
    """Payload-embedded execution_mode must never downgrade LIVE to PAPER."""
    payload = {**_marker(), "execution_mode": "paper"}
    assert not queue._is_current_session_paper_recovery(
        payload=payload, execution_mode="LIVE"
    )
    assert not queue._manual_restart_guard_bypass_enabled(
        payload=payload, execution_mode="LIVE"
    )


def test_missing_execution_mode_rejected():
    assert not queue._is_current_session_paper_recovery(
        payload=_marker(), execution_mode=None
    )


# ── §3 Timestamp semantics ────────────────────────────────────────────────────

def test_utc_plus_suffix_accepted():
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")
    assert queue._is_current_session_paper_recovery(
        payload=_marker(ts_override=ts), execution_mode="PAPER"
    )


def test_z_suffix_accepted():
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    assert queue._is_current_session_paper_recovery(
        payload=_marker(ts_override=ts), execution_mode="PAPER"
    )


def test_other_aware_offset_accepted():
    ts = datetime.now(timezone(timedelta(hours=-5))).isoformat()
    assert queue._is_current_session_paper_recovery(
        payload=_marker(ts_override=ts), execution_mode="PAPER"
    )


def test_naive_timestamp_rejected():
    ts = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    assert not queue._is_current_session_paper_recovery(
        payload=_marker(ts_override=ts), execution_mode="PAPER"
    )


@pytest.mark.parametrize("bad", ["not-a-date", "2026-99-99", "", "null", "true"])
def test_malformed_string_rejected(bad):
    assert not queue._is_current_session_paper_recovery(
        payload=_marker(ts_override=bad), execution_mode="PAPER"
    )


def test_missing_timestamp_rejected():
    p = _marker()
    del p["recovery_rescue_ts"]
    assert not queue._is_current_session_paper_recovery(payload=p, execution_mode="PAPER")


def test_previous_day_marker_rejected():
    ts = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    assert not queue._is_current_session_paper_recovery(
        payload=_marker(ts_override=ts), execution_mode="PAPER"
    )


def test_within_5min_future_accepted():
    ts = (datetime.now(timezone.utc) + timedelta(minutes=4)).isoformat()
    assert queue._is_current_session_paper_recovery(
        payload=_marker(ts_override=ts), execution_mode="PAPER"
    )


def test_beyond_5min_future_rejected():
    ts = (datetime.now(timezone.utc) + timedelta(minutes=6)).isoformat()
    assert not queue._is_current_session_paper_recovery(
        payload=_marker(ts_override=ts), execution_mode="PAPER"
    )


def test_datetime_object_accepted_by_parse():
    dt = datetime.now(timezone.utc)
    assert queue._parse_aware_timestamp(dt) is not None


def test_naive_datetime_object_rejected():
    assert queue._parse_aware_timestamp(datetime.now().replace(tzinfo=None)) is None


def test_premarket_recovery_accepted():
    """No 9:30 a.m. gate — legitimate recovery can happen premarket."""
    now_et = datetime.now(ET).replace(hour=7, minute=0, second=0, microsecond=0)
    ts = now_et.astimezone(timezone.utc).isoformat()
    now_utc = now_et.astimezone(timezone.utc)
    assert queue._is_current_session_paper_recovery(
        payload=_marker(ts_override=ts), execution_mode="PAPER", now=now_utc
    )


def test_midnight_et_boundary():
    """Marker from previous day must be rejected even if UTC date matches."""
    now_et = datetime.now(ET).replace(hour=0, minute=1, second=0, microsecond=0)
    prev_et = now_et - timedelta(minutes=2)  # 23:59 previous ET day
    ts = prev_et.astimezone(timezone.utc).isoformat()
    now_utc = now_et.astimezone(timezone.utc)
    assert not queue._is_current_session_paper_recovery(
        payload=_marker(ts_override=ts), execution_mode="PAPER", now=now_utc
    )


# ── §4 Lookback bounds ────────────────────────────────────────────────────────

@pytest.mark.parametrize("h", [1, 24, 48])
def test_valid_lookback_accepted(h):
    assert queue._is_current_session_paper_recovery(
        payload=_marker(lookback=h), execution_mode="PAPER"
    )


@pytest.mark.parametrize("h", [0, -1, 49, 100])
def test_out_of_bounds_lookback_rejected(h):
    assert not queue._is_current_session_paper_recovery(
        payload=_marker(lookback=h), execution_mode="PAPER"
    )


@pytest.mark.parametrize("bad", ["48", "1", None, 1.5, True, False, ""])
def test_non_integer_lookback_rejected(bad):
    p = {**_marker(), "recovery_rescue_lookback_hours": bad}
    assert not queue._is_current_session_paper_recovery(payload=p, execution_mode="PAPER")


def test_missing_lookback_rejected():
    p = _marker()
    del p["recovery_rescue_lookback_hours"]
    assert not queue._is_current_session_paper_recovery(payload=p, execution_mode="PAPER")


# ── §5 Existing bypass paths preserved ───────────────────────────────────────

def test_manual_rescue_true_preserved():
    assert queue._manual_restart_guard_bypass_enabled(
        job_result={"manual_rescue": True}, payload={}, execution_mode="PAPER"
    )


def test_manual_rescue_current_session_error_preserved():
    assert queue._manual_restart_guard_bypass_enabled(
        job_last_error="manual_rescue_current_session", payload={}, execution_mode="PAPER"
    )


def test_manual_requeue_timeout_error_preserved():
    assert queue._manual_restart_guard_bypass_enabled(
        job_last_error="manual_requeue_after_overnight_reeval_timeout",
        payload={}, execution_mode="PAPER"
    )


def test_force_overnight_reeval_only_preserved():
    assert queue._manual_restart_guard_bypass_enabled(
        payload={"force_overnight_reeval_only": True, "do_not_queue_directly": True},
        execution_mode="PAPER"
    )


@pytest.mark.parametrize("mode", ["LIVE", "live"])
def test_legacy_bypasses_rejected_for_live(mode):
    assert not queue._manual_restart_guard_bypass_enabled(
        job_result={"manual_rescue": True}, payload={}, execution_mode=mode
    )
    assert not queue._manual_restart_guard_bypass_enabled(
        job_last_error="manual_rescue_current_session", payload={}, execution_mode=mode
    )


def test_string_boolean_rescue_rejected():
    p = {**_marker(), "recovery_rescue": "true"}
    assert not queue._is_current_session_paper_recovery(payload=p, execution_mode="PAPER")


def test_integer_1_rescue_rejected():
    p = {**_marker(), "recovery_rescue": 1}
    assert not queue._is_current_session_paper_recovery(payload=p, execution_mode="PAPER")


# ── §6 _dispatch() seam ──────────────────────────────────────────────────────

def test_dispatch_paper_valid_marker_passes_guard():
    """Overnight PAPER row with valid recovery marker must not be rejected."""
    payload = {**_marker(), "ticker": "TSLA", "side": "CALL", "score": 80.0}
    rejected = _run_dispatch(
        job_id=1, client_id="jose@x.com", mode="PAPER",
        payload=payload, signal_id="sig-001"
    )
    assert rejected == [], f"Valid PAPER recovery must pass guard. got={rejected}"


def test_dispatch_live_same_marker_rejected():
    """LIVE + overnight signal must be rejected by restart guard regardless of recovery marker.

    Payload uses execution_mode=live to match runtime mode (avoids the separate
    execution_mode_mismatch pre-check) so we can prove the restart guard itself
    fires for LIVE.
    """
    payload = {**_marker(), "ticker": "TSLA", "side": "CALL",
               "score": 80.0, "execution_mode": "live"}
    rejected = _run_dispatch(
        job_id=2, client_id="jason@x.com", mode="LIVE",
        payload=payload, signal_id="sig-002"
    )
    assert len(rejected) == 1, f"LIVE must be rejected by restart guard. got={rejected}"


def test_dispatch_stale_paper_marker_rejected():
    """Yesterday's recovery marker must not bypass restart guard."""
    ts = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    payload = {**_marker(ts_override=ts), "ticker": "TSLA", "side": "CALL", "score": 80.0}
    rejected = _run_dispatch(
        job_id=3, client_id="jose@x.com", mode="PAPER",
        payload=payload, signal_id="sig-003"
    )
    assert len(rejected) == 1, f"Stale marker must be rejected. got={rejected}"


# ── §7 Producer contract ──────────────────────────────────────────────────────
#
# Production reads self.mc.mode (not self.mode) to detect LIVE vs PAPER.
# Tests must set rec.mc = SimpleNamespace(mode=...) or production defaults to
# PAPER on AttributeError — making a test that sets rec.mode="live" silently
# exercise the PAPER path while claiming to test LIVE.

from types import SimpleNamespace as _NS


def _make_paper_rec(client_id="jose.vasquez4011@gmail.com"):
    import ap_recovery as _r
    rec = object.__new__(_r.APStartupRecovery)
    rec.client_id = client_id
    rec.mc = _NS(mode="PAPER")
    rec.entry_watcher = None
    return rec


def _make_live_rec(client_id="jasoncosby1@gmail.com"):
    import ap_recovery as _r
    rec = object.__new__(_r.APStartupRecovery)
    rec.client_id = client_id
    rec.mc = _NS(mode="LIVE")
    rec.entry_watcher = None
    return rec


def _fake_db_module(sqls, params_list=None):
    """Return a fake ap.db module whose cursor records SQL/params."""
    class FakeCursor:
        rowcount = 0
        def execute(self, sql, params=None):
            sqls.append(sql)
            if params_list is not None:
                params_list.append(params or [])
        def __enter__(self): return self
        def __exit__(self, *_): pass
        def fetchall(self): return []
        def fetchone(self): return None

    class FakeConn:
        def __enter__(self): return FakeCursor()
        def __exit__(self, *_): pass

    mod = types.ModuleType("ap.db")
    mod.conn = FakeConn
    mod.run_with_retry = lambda fn, *a, **k: fn()
    return mod


def test_reseed_watchers_writes_all_marker_fields():
    """_reseed_watchers must stamp all three recovery_rescue fields atomically
    via JSONB merge (||) so existing signal_id/ticker/score are preserved."""
    sqls, params_list = [], []
    rec = _make_paper_rec()
    with patch("ap_recovery.os.getenv", return_value="48"), \
         patch.dict(sys.modules, {"ap.db": _fake_db_module(sqls, params_list)}):
        rec._reseed_watchers({})   # must not raise

    combined_sql    = " ".join(sqls)
    combined_params = str(params_list)

    assert "WATCHING" in combined_sql, "SQL must filter by WATCHING status"
    assert "||" in combined_sql, "SQL must use JSONB merge (||) to preserve existing payload"
    assert "recovery_rescue" in combined_params, \
        f"recovery_rescue must be in SQL params. got={combined_params[:200]}"
    assert "recovery_rescue_ts" in combined_params, "recovery_rescue_ts must be in params"
    assert "recovery_rescue_lookback_hours" in combined_params, "lookback must be in params"


def test_reseed_watchers_only_targets_watching():
    """UPDATE predicate must be restricted to status='WATCHING'.
    Terminal status strings must not appear in the UPDATE clause."""
    sqls = []
    rec = _make_paper_rec()
    with patch("ap_recovery.os.getenv", return_value="48"), \
         patch.dict(sys.modules, {"ap.db": _fake_db_module(sqls)}):
        rec._reseed_watchers({})   # must not raise

    assert sqls, "Expected at least one SQL statement"
    combined = " ".join(sqls)
    assert "WATCHING" in combined, "SQL must filter by status='WATCHING'"

    update_clause = combined.split("WHERE")[0] if "WHERE" in combined else combined
    for terminal in ("REJECTED", "ERROR", "CANCELED", "EXPIRED"):
        assert f"'{terminal}'" not in update_clause, (
            f"Terminal status '{terminal}' must not appear in UPDATE clause"
        )


def test_reseed_watchers_skips_reset_for_live():
    """_reseed_watchers must NOT execute WATCHING->NEW for LIVE runners.
    Production reads self.mc.mode (not self.mode). Setting rec.mc.mode=LIVE
    causes _is_live=True and _reset() is skipped; no UPDATE trade_queue fires.
    """
    sqls = []
    rec = _make_live_rec()
    with patch("ap_recovery.os.getenv", return_value="48"), \
         patch.dict(sys.modules, {"ap.db": _fake_db_module(sqls)}):
        rec._reseed_watchers({})   # must not raise

    combined = " ".join(sqls)
    assert "UPDATE trade_queue" not in combined, (
        "LIVE _reseed_watchers must not emit UPDATE trade_queue SET status='NEW'. "
        f"Got SQL: {combined[:300]}"
    )


def test_reseed_watchers_paper_scoped_by_client_id():
    """PAPER reset SQL must include the runner's client_id in params."""
    sqls, params_list = [], []
    rec = _make_paper_rec(client_id="jose.vasquez4011@gmail.com")
    with patch("ap_recovery.os.getenv", return_value="48"), \
         patch.dict(sys.modules, {"ap.db": _fake_db_module(sqls, params_list)}):
        rec._reseed_watchers({})   # must not raise

    combined_params = str(params_list)
    assert "jose.vasquez4011@gmail.com" in combined_params, (
        "SQL params must contain the runner's client_id — no global mutations"
    )


# ── §8 Diagnostics ────────────────────────────────────────────────────────────

def test_structured_log_emitted_on_recovery_bypass(caplog):
    """PAPER_RECOVERY_RESTART_GUARD_BYPASS must be logged when bypass fires."""
    payload = {**_marker(), "ticker": "TSLA", "side": "CALL", "score": 80.0}
    rejected = []

    rg_stub = types.ModuleType("ap.restart_guard")
    rg_stub.should_skip_on_restart = lambda p: True
    sys.modules["ap.restart_guard"] = rg_stub

    def _fake_mark(jid, status, *, result=None, error=None):
        if error == "restart_guard:overnight_skip":
            rejected.append(error)

    mc = _make_mc("PAPER")
    try:
        with caplog.at_level(logging.INFO), \
             patch("ap.queue._mark_job", side_effect=_fake_mark), \
             patch("ap.queue._get_sb_client", return_value=None), \
             patch("ap.queue._log_rejection_to_db", return_value=None):
            try:
                queue._dispatch(
                    job_id=99, client_id="jose@x.com",
                    signal_id="sig-log", payload=payload,
                    job_last_error=None, job_result=None,
                    master_control=mc,
                    contract_selector=MagicMock(),
                    order_state_machine=MagicMock(),
                    entry_watcher=MagicMock(),
                )
            except Exception:
                pass
    finally:
        sys.modules.pop("ap.restart_guard", None)

    msgs = [r.getMessage() for r in caplog.records]
    assert any("PAPER_RECOVERY_RESTART_GUARD_BYPASS" in m for m in msgs), \
        f"Structured bypass log must be emitted. got={msgs}"
    # client_id must appear in the log (required field per spec)
    assert any("jose@x.com" in m for m in msgs if "PAPER_RECOVERY_RESTART_GUARD_BYPASS" in m), \
        f"client_id must appear in PAPER_RECOVERY_RESTART_GUARD_BYPASS log. got={msgs}"
    assert rejected == [], "Row must not have been rejected after bypass"


# ── §9 Read-only classifier ───────────────────────────────────────────────────

def test_classifier_makes_no_db_call():
    """_is_current_session_paper_recovery must not import or call any DB."""
    import builtins
    _orig = builtins.__import__
    db_calls = []

    def _spy(name, *a, **kw):
        if name in ("ap.db", "psycopg2"):
            db_calls.append(name)
        return _orig(name, *a, **kw)

    with patch("builtins.__import__", side_effect=_spy):
        result = queue._is_current_session_paper_recovery(
            payload=_marker(), execution_mode="PAPER"
        )

    assert result is True
    assert db_calls == [], f"Classifier must not touch DB. got={db_calls}"


# ── §10 Import architecture ───────────────────────────────────────────────────

def test_ap_queue_is_flat_module_not_package():
    """ap.queue must be the flat module queue.py, not a package __init__.py."""
    assert queue.__file__.endswith("queue.py"), \
        f"Expected queue.py, got {queue.__file__}"
    assert "__init__" not in queue.__file__


def test_no_package_shim_exists():
    """The package shim directory must be gone — in-place edit only."""
    shim = _REPO / "ap" / "queue" / "__init__.py"
    assert not shim.exists(), f"Package shim must not exist: {shim}"


def test_no_split_brain_state():
    """No secondary base module means no split-brain mutable state."""
    assert not hasattr(queue, "_base")
    assert not hasattr(queue, "_ap_queue_base")


def test_production_surface_intact():
    """All production symbols must be importable from ap.queue."""
    assert callable(queue.enqueue_signal)
    assert callable(queue.worker_loop)
    assert callable(queue._dispatch)
    assert callable(queue._claim_one_job)
    assert callable(queue._mark_job)
    assert hasattr(queue, "_PAPER_RECOVERY_IMMEDIATE_COUNTS")
    assert hasattr(queue, "_selector_failure_by_job")
    assert callable(queue._is_current_session_paper_recovery)
    assert callable(queue._parse_aware_timestamp)
    assert callable(queue._manual_restart_guard_bypass_enabled)
