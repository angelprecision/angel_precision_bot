"""
P0 regression: centralized preopen_readiness deadline enforcement.

Audit contract (2026-07-25):
  The prior amendment installed deadline-enforced readiness only at the tail
  of a performed, incomplete run_overnight_reeval_attempt. Every early-return
  state bypassed it:
    - already-marked RETRY_EXHAUSTED
    - WAITING_FOR_RETRY
    - max-attempts exhaustion (computed RETRY_EXHAUSTED)
    - ATTEMPT_ALREADY_RUNNING
    - trading-day ticks outside 9:00-9:45 window (SKIPPED_NOT_DUE)

  Deterministic bypass sequence:
    1. Overnight reeval exhausts attempts before the readiness deadline.
    2. Deadline arrives; every health tick sees exhausted_date == today.
    3. Function returns RETRY_EXHAUSTED immediately without enforcement.
    4. LIVE runner never enters degraded; entries remain authorized despite
       no verified pre-open watcher ownership.

  Additionally, the tail-path exception handler only logged: a
  preopen_readiness raise did not enter degraded mode, silently violating
  the "verified watcher ownership before LIVE entries" contract.

Contract enforced here:
  - Every early-return branch calls _enforce_preopen_readiness_at_deadline.
  - The helper no-ops on: NYSE holidays, weekends, pre-deadline,
    _overnight_reeval_success_date == today.
  - LIVE + BLOCKED → _enter_degraded_mode with preopen_readiness_blocked.
  - LIVE + readiness exception → _enter_degraded_mode with
    preopen_readiness_enforcement_failed:* (FAIL CLOSED).
  - PAPER never enters degraded from this path.
"""
from __future__ import annotations

import datetime as dt
import sys
import types
from unittest.mock import MagicMock, patch

import pytest

# CI installs supabase, psycopg2-binary, and cryptography via the P0 workflow;
# local dev without those packages skips this module. Every skip is reported
# so a partial local run does not silently hide the enforcement contract.
pytest.importorskip("supabase", reason="Deadline-enforcement tests require the "
                    "same runtime deps as client_runner (installed in CI).")
pytest.importorskip("psycopg2", reason="Deadline-enforcement tests require psycopg2.")
pytest.importorskip("cryptography", reason="Deadline-enforcement tests require cryptography.")


# ─── Test infrastructure ─────────────────────────────────────────────────────

_ET = None  # set by fixture from client_runner._ET

CLIENT_ID = "live-poison-client@example.com"
PAPER_CLIENT_ID = "paper-diag-client@example.com"


class _FakeReadiness:
    """Container so tests can wire preopen_readiness behavior per case."""
    def __init__(self):
        self.calls = []
        self.enforcement_active = True
        self.result_status = "BLOCKED"
        self.result_errors = ["overnight_reeval_missing"]
        self.raise_on_call = None  # Exception instance or None
        # NEW: fail-closed audit surfaces.
        self.raise_on_predicate = None      # Exception → _readiness_enforcement_active raises
        self.return_shape_override = None   # (marker, value) — replace readiness return

    def _readiness_enforcement_active(self, now=None):
        if self.raise_on_predicate is not None:
            raise self.raise_on_predicate
        return self.enforcement_active

    def run_preopen_autonomous_readiness(self, client_id, mode, *,
                                          dry_run, stage, runner):
        self.calls.append({
            "client_id": client_id, "mode": mode, "stage": stage,
        })
        if self.raise_on_call is not None:
            raise self.raise_on_call
        if self.return_shape_override is not None:
            marker, value = self.return_shape_override
            if marker == "value":
                return value
        return {
            "ok":     self.result_status == "OK",
            "status": self.result_status,
            "errors": list(self.result_errors),
        }


@pytest.fixture
def rr_env(monkeypatch):
    """Import client_runner and stub only what tests need to control:
    ap.flatline_alarm.is_trading_day and ap.preopen_readiness. Real
    supabase / psycopg2 / cryptography imports proceed normally (matches
    CI, where those packages are installed)."""

    def _stub(mod_name, **attrs):
        mod = types.ModuleType(mod_name)
        for k, v in attrs.items():
            setattr(mod, k, v)
        monkeypatch.setitem(sys.modules, mod_name, mod)
        return mod

    # Force NYSE calendar to say "trading day" unless a test says otherwise.
    _fake_flatline = _stub("ap.flatline_alarm", is_trading_day=lambda d: True)

    # Provide a fake preopen_readiness that captures calls.
    fake_ready = _FakeReadiness()
    _stub(
        "ap.preopen_readiness",
        _readiness_enforcement_active=fake_ready._readiness_enforcement_active,
        run_preopen_autonomous_readiness=fake_ready.run_preopen_autonomous_readiness,
    )

    import importlib
    if "client_runner" in sys.modules:
        importlib.reload(sys.modules["client_runner"])
    import client_runner as cr

    yield cr, fake_ready, _fake_flatline


def _mk_runner(cr, *, mode="live", email=None):
    """Minimal-viable runner instance that exercises only the enforcement
    path. Bypasses __init__ (which needs Supabase, Fernet, brokers, etc.)."""
    runner = cr.ClientRunner.__new__(cr.ClientRunner)
    runner.email = email or (CLIENT_ID if mode == "live" else PAPER_CLIENT_ID)
    runner.mode = mode

    # Overnight-reeval state fields _enforce_preopen_readiness_at_deadline
    # and run_overnight_reeval_attempt read.
    runner._overnight_reeval_success_date = None
    runner._overnight_reeval_exhausted_date = None
    runner._overnight_reeval_next_retry_at = None
    runner._overnight_reeval_attempt_count = 0
    runner._overnight_reeval_last_result_class = None
    runner._overnight_reeval_last_retry_reason = None
    runner._overnight_reeval_last_attempt_at = None
    import threading
    runner._overnight_reeval_attempt_lock = threading.Lock()

    # Capture degraded-mode transitions.
    runner._entered_degraded = []
    runner._cleared_keys = []
    def _enter(reason):
        runner._entered_degraded.append(str(reason))
    def _clear(key):
        runner._cleared_keys.append(str(key))
    runner._enter_degraded_mode = _enter
    runner._clear_degraded_reason_key = _clear

    # Neutral stubs for helpers called by run_overnight_reeval_attempt.
    runner._reset_overnight_reeval_state_for_date = lambda d: None
    runner._overnight_reeval_in_window = lambda et: (
        et.hour == 9 and 0 <= et.minute < 45
    )
    runner._overnight_reeval_base_result = lambda **kw: {**kw}
    runner._persist_overnight_reeval_lock = lambda *a, **kw: None
    return runner


def _et_time(hour, minute, cr):
    return dt.datetime(2026, 7, 24, hour, minute, tzinfo=cr._ET)


# ─── Every early-return state enforces at deadline ────────────────────────────

def test_retry_exhausted_early_return_enforces_after_deadline(rr_env):
    """Exhaust retries before deadline; tick after deadline. RETRY_EXHAUSTED
    early return must call readiness and LIVE must enter degraded on BLOCKED.
    """
    cr, fake_ready, _ = rr_env
    runner = _mk_runner(cr, mode="live")
    # Pre-mark exhausted for today so the early-return path fires.
    now_et = _et_time(9, 35, cr)  # after 9:18 default deadline start? actually deadline is 9:00-10:00
    runner._overnight_reeval_exhausted_date = now_et.date()

    # In-window is False at 9:35 outside 9:00-9:45? 35 < 45 → in window.
    # Bring the tick out to 9:50 so we're SKIPPED_NOT_DUE, no wait — we want
    # to exercise the exhausted early return specifically, which fires BEFORE
    # the in-window check gate? Re-check: the code checks in_window first,
    # then exhausted. So we need in_window True and exhausted True.
    result = runner.run_overnight_reeval_attempt(force=False, source="tick", now_et=now_et)

    assert result["result_class"] == "RETRY_EXHAUSTED"
    assert len(fake_ready.calls) == 1, (
        f"RETRY_EXHAUSTED early return MUST enforce readiness after deadline; "
        f"got {len(fake_ready.calls)} calls."
    )
    assert result.get("readiness_result", {}).get("status") == "BLOCKED"
    assert any("preopen_readiness_blocked" in r for r in runner._entered_degraded), (
        f"LIVE runner must enter degraded mode on BLOCKED; got "
        f"{runner._entered_degraded!r}"
    )


def test_waiting_for_retry_early_return_enforces_after_deadline(rr_env):
    """WAITING_FOR_RETRY early return past deadline → readiness runs, LIVE
    enters degraded."""
    cr, fake_ready, _ = rr_env
    runner = _mk_runner(cr, mode="live")
    now_et = _et_time(9, 35, cr)
    # Schedule next retry in the future.
    runner._overnight_reeval_next_retry_at = now_et + dt.timedelta(minutes=5)

    result = runner.run_overnight_reeval_attempt(force=False, source="tick", now_et=now_et)

    assert result["result_class"] == "WAITING_FOR_RETRY"
    assert len(fake_ready.calls) == 1
    assert result.get("readiness_result", {}).get("status") == "BLOCKED"
    assert any("preopen_readiness_blocked" in r for r in runner._entered_degraded)


def test_max_attempts_exhaustion_computed_path_enforces(rr_env):
    """The compute path that sets exhausted_date AT max attempts must
    enforce before returning."""
    cr, fake_ready, _ = rr_env
    runner = _mk_runner(cr, mode="live")
    now_et = _et_time(9, 35, cr)
    runner._overnight_reeval_attempt_count = cr.OVERNIGHT_REEVAL_MAX_ATTEMPTS

    result = runner.run_overnight_reeval_attempt(force=False, source="tick", now_et=now_et)

    assert result["result_class"] == "RETRY_EXHAUSTED"
    assert runner._overnight_reeval_exhausted_date == now_et.date()
    assert len(fake_ready.calls) == 1
    assert any("preopen_readiness_blocked" in r for r in runner._entered_degraded)


def test_attempt_already_running_enforces_after_deadline(rr_env):
    """When the attempt lock is held (another thread mid-run) and the
    deadline has passed, the current tick still enforces LIVE readiness."""
    cr, fake_ready, _ = rr_env
    runner = _mk_runner(cr, mode="live")
    now_et = _et_time(9, 35, cr)
    # Hold the lock so acquire(blocking=False) returns False.
    assert runner._overnight_reeval_attempt_lock.acquire(blocking=False)

    result = runner.run_overnight_reeval_attempt(force=False, source="tick", now_et=now_et)

    assert result["result_class"] == "ATTEMPT_ALREADY_RUNNING"
    assert len(fake_ready.calls) == 1
    assert any("preopen_readiness_blocked" in r for r in runner._entered_degraded)


def test_skipped_not_due_after_window_still_enforces_on_trading_day(rr_env):
    """A scheduler tick at 9:50 (outside the 9:00-9:45 attempt window) on
    a trading day yields SKIPPED_NOT_DUE. Because the readiness deadline is
    already active, enforcement must still run."""
    cr, fake_ready, _ = rr_env
    runner = _mk_runner(cr, mode="live")
    now_et = _et_time(9, 50, cr)  # after window

    result = runner.run_overnight_reeval_attempt(force=False, source="tick", now_et=now_et)

    assert result["result_class"] == "SKIPPED_NOT_DUE"
    assert len(fake_ready.calls) == 1
    assert any("preopen_readiness_blocked" in r for r in runner._entered_degraded)


# ─── Pre-deadline early returns must NOT enforce ─────────────────────────────

def test_waiting_for_retry_before_deadline_does_not_enforce(rr_env):
    """Pre-deadline retries are legitimate — no readiness enforcement."""
    cr, fake_ready, _ = rr_env
    fake_ready.enforcement_active = False  # deadline not yet reached
    runner = _mk_runner(cr, mode="live")
    now_et = _et_time(9, 5, cr)
    runner._overnight_reeval_next_retry_at = now_et + dt.timedelta(minutes=5)

    result = runner.run_overnight_reeval_attempt(force=False, source="tick", now_et=now_et)

    assert result["result_class"] == "WAITING_FOR_RETRY"
    assert fake_ready.calls == [], (
        "Pre-deadline WAITING_FOR_RETRY must not run readiness — legitimate "
        "retry window would otherwise be blocked."
    )
    assert runner._entered_degraded == []


# ─── Fail-closed exception path ──────────────────────────────────────────────

def test_readiness_raises_after_deadline_live_fails_closed(rr_env):
    """When run_preopen_autonomous_readiness raises during enforcement,
    LIVE must _enter_degraded_mode with preopen_readiness_enforcement_failed.
    Merely logging leaves entries_allowed=True and violates the contract."""
    cr, fake_ready, _ = rr_env
    fake_ready.raise_on_call = RuntimeError("supabase table missing")
    runner = _mk_runner(cr, mode="live")
    now_et = _et_time(9, 35, cr)
    runner._overnight_reeval_exhausted_date = now_et.date()

    result = runner.run_overnight_reeval_attempt(force=False, source="tick", now_et=now_et)

    assert result["result_class"] == "RETRY_EXHAUSTED"
    assert result.get("readiness_result", {}).get("status") == "ERROR"
    assert result["readiness_result"].get("fail_closed") is True
    # Fail-closed degraded entry with the specific reason prefix.
    assert any(
        r.startswith("preopen_readiness_enforcement_failed:")
        for r in runner._entered_degraded
    ), (
        f"LIVE must fail closed via _enter_degraded_mode on readiness "
        f"exception; got {runner._entered_degraded!r}"
    )


def test_readiness_raises_paper_does_not_degrade(rr_env):
    """PAPER stays diagnostic-only — an enforcement exception does NOT
    degrade paper runners."""
    cr, fake_ready, _ = rr_env
    fake_ready.raise_on_call = RuntimeError("supabase down")
    runner = _mk_runner(cr, mode="paper")
    now_et = _et_time(9, 35, cr)
    runner._overnight_reeval_exhausted_date = now_et.date()

    result = runner.run_overnight_reeval_attempt(force=False, source="tick", now_et=now_et)
    assert result["result_class"] == "RETRY_EXHAUSTED"
    # Paper: no degraded transition, even on exception.
    assert runner._entered_degraded == []


# ─── Holiday and success skips ───────────────────────────────────────────────

def test_holiday_skips_enforcement_entirely(rr_env, monkeypatch):
    """Even when RETRY_EXHAUSTED early return fires, an NYSE holiday must
    not trigger readiness (or LIVE would falsely degrade on July 4)."""
    cr, fake_ready, fake_flatline = rr_env
    # Make the NYSE calendar say False.
    fake_flatline.is_trading_day = lambda d: False
    runner = _mk_runner(cr, mode="live")
    now_et = _et_time(9, 35, cr)
    runner._overnight_reeval_exhausted_date = now_et.date()

    result = runner.run_overnight_reeval_attempt(force=False, source="tick", now_et=now_et)
    # The trading-day precheck up front sends this down SKIPPED_NOT_DUE.
    assert result["result_class"] == "SKIPPED_NOT_DUE"
    assert fake_ready.calls == [], (
        "Holidays must never trigger readiness enforcement; got "
        f"{fake_ready.calls!r}"
    )
    assert runner._entered_degraded == []


def test_completed_overnight_success_skips_enforcement(rr_env):
    """If overnight reeval already succeeded today, the ALREADY_COMPLETED
    early return must not re-enforce readiness — success means watchers
    are armed and the normal completion path already ran readiness."""
    cr, fake_ready, _ = rr_env
    runner = _mk_runner(cr, mode="live")
    now_et = _et_time(9, 35, cr)
    runner._overnight_reeval_success_date = now_et.date()

    result = runner.run_overnight_reeval_attempt(force=False, source="tick", now_et=now_et)
    assert result["result_class"] == "ALREADY_COMPLETED"
    assert fake_ready.calls == []
    assert runner._entered_degraded == []


# ─── OK status clears prior degraded key ─────────────────────────────────────

def test_readiness_ok_clears_prior_preopen_readiness_blocked_key(rr_env):
    """When readiness runs and returns OK, the prior blocked degraded key
    must be cleared so a transient bad state does not linger."""
    cr, fake_ready, _ = rr_env
    fake_ready.result_status = "OK"
    fake_ready.result_errors = []
    runner = _mk_runner(cr, mode="live")
    now_et = _et_time(9, 35, cr)
    runner._overnight_reeval_exhausted_date = now_et.date()

    result = runner.run_overnight_reeval_attempt(force=False, source="tick", now_et=now_et)
    assert result["result_class"] == "RETRY_EXHAUSTED"
    assert "preopen_readiness_blocked" in runner._cleared_keys
    assert runner._entered_degraded == []


# ─── Fail-closed guards: the entire operation is protected, not just run() ───

def test_readiness_module_import_failure_after_deadline_live_fails_closed(rr_env, monkeypatch):
    """When `import ap.preopen_readiness` fails inside the helper (supabase
    outage, packaging bug, etc.), LIVE must still enter degraded mode via
    _enter_degraded_mode('preopen_readiness_enforcement_failed:*'). Merely
    returning None (prior behavior) left entries_allowed=True."""
    cr, fake_ready, _ = rr_env
    # Force the import statement inside the helper to raise.
    import sys as _sys
    monkeypatch.setitem(_sys.modules, "ap.preopen_readiness", None)

    runner = _mk_runner(cr, mode="live")
    now_et = _et_time(9, 35, cr)
    runner._overnight_reeval_exhausted_date = now_et.date()

    result = runner.run_overnight_reeval_attempt(force=False, source="tick", now_et=now_et)

    assert result["result_class"] == "RETRY_EXHAUSTED"
    assert result.get("readiness_result", {}).get("status") == "ERROR"
    assert result["readiness_result"].get("failure_kind") == "preopen_readiness_import_failed"
    assert result["readiness_result"].get("fail_closed") is True
    assert any(
        r.startswith("preopen_readiness_enforcement_failed:preopen_readiness_import_failed:")
        for r in runner._entered_degraded
    ), (
        f"LIVE must fail closed on readiness import failure; got "
        f"{runner._entered_degraded!r}"
    )


def test_readiness_module_import_failure_paper_does_not_degrade(rr_env, monkeypatch):
    """PAPER never degrades on import failure — diagnostic ERROR only."""
    cr, fake_ready, _ = rr_env
    import sys as _sys
    monkeypatch.setitem(_sys.modules, "ap.preopen_readiness", None)

    runner = _mk_runner(cr, mode="paper")
    now_et = _et_time(9, 35, cr)
    runner._overnight_reeval_exhausted_date = now_et.date()

    result = runner.run_overnight_reeval_attempt(force=False, source="tick", now_et=now_et)
    assert result["result_class"] == "RETRY_EXHAUSTED"
    assert result["readiness_result"].get("status") == "ERROR"
    assert result["readiness_result"].get("fail_closed") is False
    assert runner._entered_degraded == []


def test_readiness_deadline_predicate_raises_live_fails_closed(rr_env):
    """If _readiness_enforcement_active() itself raises, prior behavior let
    the exception propagate up and be logged nonfatally by the scheduler
    wrapper without _enter_degraded_mode. Now LIVE must degrade."""
    cr, fake_ready, _ = rr_env
    fake_ready.raise_on_predicate = ValueError("bad tz value")

    runner = _mk_runner(cr, mode="live")
    now_et = _et_time(9, 35, cr)
    runner._overnight_reeval_exhausted_date = now_et.date()

    result = runner.run_overnight_reeval_attempt(force=False, source="tick", now_et=now_et)

    assert result["result_class"] == "RETRY_EXHAUSTED"
    assert result.get("readiness_result", {}).get("status") == "ERROR"
    assert result["readiness_result"].get("failure_kind") == "readiness_deadline_predicate_raised"
    assert result["readiness_result"].get("fail_closed") is True
    assert any(
        r.startswith("preopen_readiness_enforcement_failed:readiness_deadline_predicate_raised:")
        for r in runner._entered_degraded
    )
    # Predicate raise short-circuits: readiness function itself never runs.
    assert fake_ready.calls == []


def test_readiness_deadline_predicate_raises_paper_does_not_degrade(rr_env):
    """PAPER stays diagnostic-only on predicate raise."""
    cr, fake_ready, _ = rr_env
    fake_ready.raise_on_predicate = RuntimeError("boom")

    runner = _mk_runner(cr, mode="paper")
    now_et = _et_time(9, 35, cr)
    runner._overnight_reeval_exhausted_date = now_et.date()

    result = runner.run_overnight_reeval_attempt(force=False, source="tick", now_et=now_et)
    assert result["readiness_result"].get("status") == "ERROR"
    assert runner._entered_degraded == []


def test_readiness_returns_none_live_fails_closed(rr_env):
    """A malformed readiness return of None was previously blown up by
    readiness.get(...) outside any try/except, propagating out and getting
    logged nonfatally. LIVE must now fail closed with
    readiness_return_not_a_dict."""
    cr, fake_ready, _ = rr_env
    fake_ready.return_shape_override = ("value", None)

    runner = _mk_runner(cr, mode="live")
    now_et = _et_time(9, 35, cr)
    runner._overnight_reeval_exhausted_date = now_et.date()

    result = runner.run_overnight_reeval_attempt(force=False, source="tick", now_et=now_et)

    assert result["result_class"] == "RETRY_EXHAUSTED"
    assert result["readiness_result"].get("status") == "ERROR"
    assert result["readiness_result"].get("failure_kind") == "readiness_return_not_a_dict"
    assert result["readiness_result"].get("fail_closed") is True
    assert any(
        r.startswith("preopen_readiness_enforcement_failed:readiness_return_not_a_dict:")
        for r in runner._entered_degraded
    )


def test_readiness_returns_dict_missing_status_live_fails_closed(rr_env):
    """A partial-shape dict (missing 'status') is not safe to interpret.
    LIVE must fail closed with readiness_return_missing_status."""
    cr, fake_ready, _ = rr_env
    fake_ready.return_shape_override = ("value", {"ok": True, "errors": []})  # no 'status'

    runner = _mk_runner(cr, mode="live")
    now_et = _et_time(9, 35, cr)
    runner._overnight_reeval_exhausted_date = now_et.date()

    result = runner.run_overnight_reeval_attempt(force=False, source="tick", now_et=now_et)

    assert result["readiness_result"].get("failure_kind") == "readiness_return_missing_status"
    assert result["readiness_result"].get("fail_closed") is True
    assert any(
        r.startswith("preopen_readiness_enforcement_failed:readiness_return_missing_status:")
        for r in runner._entered_degraded
    )


def test_readiness_returns_wrong_type_str_live_fails_closed(rr_env):
    """A non-dict return of any type must fail closed. This covers stubs
    that inadvertently return a string, list, or dataclass without the
    expected shape."""
    cr, fake_ready, _ = rr_env
    fake_ready.return_shape_override = ("value", "OK")  # bare string

    runner = _mk_runner(cr, mode="live")
    now_et = _et_time(9, 35, cr)
    runner._overnight_reeval_exhausted_date = now_et.date()

    result = runner.run_overnight_reeval_attempt(force=False, source="tick", now_et=now_et)
    assert result["readiness_result"].get("failure_kind") == "readiness_return_not_a_dict"
    assert result["readiness_result"].get("fail_closed") is True
