"""
PR #395 regression tests.

Two consumers of preopen readiness now route through the shared PR #394
validator: startup handoff and deadline enforcement. These tests cover the
minimum proofs for both:

1. Startup exception degrades LIVE.
2. Startup malformed / ERROR result degrades LIVE.
3. Deadline OK after a startup enforcement failure clears BOTH reason
   families (`preopen_readiness_blocked` + `preopen_readiness_enforcement_failed`)
   and restores `entries_allowed`.
4. PAPER startup failure does not enter degraded mode.

Validator shape combinations are already exhaustively covered by
tests/test_p0_readiness_deadline_enforcement.py (PR #394). We do not
duplicate them here.
"""
from __future__ import annotations

import datetime as _datetime
import sys
import threading
import types

import client_runner as cr


class _Evt:
    def __init__(self, value=False):
        self._value = value

    def is_set(self):
        return self._value

    def set(self):
        self._value = True

    def clear(self):
        self._value = False


def _runner(email="jason@example.com", mode="LIVE"):
    runner = object.__new__(cr.ClientRunner)
    runner.email = email
    runner.mode = mode
    runner._degraded_lock = threading.Lock()
    runner.degraded_reasons = set()
    runner.degraded = _Evt(False)
    runner.entries_allowed = _Evt(True)
    runner.failed = _Evt(False)
    runner.stopping = _Evt(False)
    runner.stopped = _Evt(False)
    runner._set_entry_permission = lambda: runner.entries_allowed.set()
    runner._overnight_reeval_success_date = None
    return runner


def _install_handoff_ok(monkeypatch):
    mod = types.ModuleType("ap.morning_handoff")
    mod.run_morning_handoff_audit = lambda **kwargs: {"ok": True}
    monkeypatch.setitem(sys.modules, "ap.morning_handoff", mod)


def _install_readiness(monkeypatch, response):
    mod = types.ModuleType("ap.preopen_readiness")
    if isinstance(response, Exception):
        def _raise(*a, **kw):
            raise response
        mod.run_preopen_autonomous_readiness = _raise
    else:
        mod.run_preopen_autonomous_readiness = lambda *a, **kw: response
    mod._readiness_enforcement_active = lambda now_et: True
    monkeypatch.setitem(sys.modules, "ap.preopen_readiness", mod)


# ── 1. startup exception → LIVE degraded ──────────────────────────────────────
def test_startup_readiness_exception_degrades_live(monkeypatch):
    _install_handoff_ok(monkeypatch)
    _install_readiness(monkeypatch, RuntimeError("db down"))

    runner = _runner(mode="LIVE")
    cr.ClientRunner._run_startup_morning_handoff(runner)

    assert any(
        r.startswith("preopen_readiness_enforcement_failed:startup:exception:RuntimeError")
        for r in runner.degraded_reasons
    )
    assert runner.degraded.is_set() is True


# ── 2. startup ERROR / malformed → LIVE degraded ──────────────────────────────
def test_startup_readiness_error_or_malformed_degrades_live(monkeypatch):
    _install_handoff_ok(monkeypatch)
    _install_readiness(monkeypatch, {"ok": False, "status": "ERROR", "errors": ["db_unavailable"]})
    runner = _runner(mode="LIVE")
    cr.ClientRunner._run_startup_morning_handoff(runner)
    assert "preopen_readiness_enforcement_failed:startup:status_error" in runner.degraded_reasons

    _install_readiness(monkeypatch, "not-a-dict")
    runner2 = _runner(mode="LIVE")
    cr.ClientRunner._run_startup_morning_handoff(runner2)
    assert any(
        r.startswith("preopen_readiness_enforcement_failed:startup:readiness_return_not_a_dict")
        for r in runner2.degraded_reasons
    )


# ── 3. deadline OK unfreezes a startup-fail-closed LIVE runner ────────────────
def test_deadline_ok_clears_startup_enforcement_failure_and_restores_entries(monkeypatch):
    _install_readiness(monkeypatch, {"ok": True, "status": "OK", "errors": []})
    flat_mod = types.ModuleType("ap.flatline_alarm")
    flat_mod.is_trading_day = lambda d: True
    monkeypatch.setitem(sys.modules, "ap.flatline_alarm", flat_mod)

    runner = _runner(mode="LIVE")
    runner.degraded_reasons = {
        "preopen_readiness_enforcement_failed:startup:exception:RuntimeError",
    }
    runner.degraded.set()
    runner.entries_allowed.clear()

    now_et = _datetime.datetime(2026, 7, 27, 9, 31)
    today = now_et.date()

    readiness = cr.ClientRunner._enforce_preopen_readiness_at_deadline(
        runner,
        now_et=now_et,
        today=today,
        source="scheduler",
        result_class="SKIPPED_NOT_DUE",
    )

    assert readiness is not None
    assert readiness.get("status") == "OK"
    assert not any(r.startswith("preopen_readiness_enforcement_failed") for r in runner.degraded_reasons)
    assert not any(r.startswith("preopen_readiness_blocked") for r in runner.degraded_reasons)
    assert runner.entries_allowed.is_set() is True


# ── 4. PAPER startup failure stays diagnostic ─────────────────────────────────
def test_paper_startup_readiness_exception_is_diagnostic_only(monkeypatch):
    _install_handoff_ok(monkeypatch)
    _install_readiness(monkeypatch, RuntimeError("db down"))

    runner = _runner(mode="PAPER")
    runner.entries_allowed.set()
    cr.ClientRunner._run_startup_morning_handoff(runner)

    assert runner.degraded_reasons == set()
    assert runner.entries_allowed.is_set() is True
