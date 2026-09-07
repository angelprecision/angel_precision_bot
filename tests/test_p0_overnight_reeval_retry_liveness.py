import inspect
import os
import pathlib
import sys
import threading
import time
import types
import datetime as _dt_module
from datetime import datetime
from types import SimpleNamespace

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/test")

import ap_overnight_reeval as ov
import client_runner as cr


REPO = pathlib.Path(__file__).resolve().parents[1]
CR_SRC = (REPO / "client_runner.py").read_text()
APP_SRC = (REPO / "app.py").read_text()
OV_SRC = (REPO / "ap_overnight_reeval.py").read_text()


@pytest.fixture(autouse=True)
def _bind_reloaded_overnight_module():
    # Earlier P0 files reload this module during collection. Always patch the
    # instance ClientRunner will import at execution time.
    global ov
    ov = sys.modules["ap_overnight_reeval"]


def _dt(hour, minute=0, second=0):
    # PR #388 weekend-CI fix: pin to a KNOWN WEEKDAY so the retry-liveness
    # tests don't collapse to SKIPPED_NOT_DUE when CI runs on a Saturday or
    # Sunday. The prior helper used today's date, which meant every
    # weekend run of the retry-liveness suite failed on scheduler.weekday()
    # >= 5. Thursday 2026-07-23 is a plain non-holiday weekday inside the
    # scheduler's 9:00–9:45 ET window.
    _pinned = _dt_module.date(2026, 7, 23)
    return datetime(_pinned.year, _pinned.month, _pinned.day, hour, minute, second, tzinfo=cr._ET)


def _result(**overrides):
    out = {
        "processed": 125,
        "armed": 0,
        "rejected": 0,
        "skipped": 125,
        "errors": 0,
        "stale_skipped": 0,
        "fresh_processed": 125,
        "fresh_armed": 0,
        "fetched": 125,
        "stalled": True,
        "retry_owned": 0,
        "unresolved": 0,
        "result_class": "RETRYABLE_ALL_DEFERRED",
        "completed": False,
        "retryable": True,
        "retry_reason": "all_fetched_rows_deferred",
    }
    out.update(overrides)
    return out


class _Evt:
    def __init__(self, value=False):
        self._value = value

    def is_set(self):
        return self._value

    def set(self):
        self._value = True

    def clear(self):
        self._value = False


def _runner(email="jose@example.com"):
    runner = object.__new__(cr.ClientRunner)
    runner.email = email
    runner.mode = "PAPER"
    runner.core = SimpleNamespace(broker=object(), entry_watcher=object())
    runner.data_broker = object()
    runner.master_control = object()
    runner.contract_selector = object()
    runner.order_state_machine = object()
    runner.position_manager = object()
    runner._overnight_reeval_attempt_lock = threading.Lock()
    runner._overnight_reeval_state_date = None
    runner._overnight_reeval_success_date = None
    runner._overnight_reeval_exhausted_date = None
    runner._overnight_reeval_last_attempt_at = None
    runner._overnight_reeval_next_retry_at = None
    runner._overnight_reeval_attempt_count = 0
    runner._overnight_reeval_last_result_class = None
    runner._overnight_reeval_last_retry_reason = None
    runner._degraded_lock = threading.Lock()
    runner.degraded_reasons = set()
    runner.degraded = _Evt(False)
    runner.entries_allowed = _Evt(True)
    runner.failed = _Evt(False)
    runner.stopping = _Evt(False)
    runner.stopped = _Evt(False)
    runner.persisted_locks = []
    runner.post_calls = []
    runner._set_entry_permission = lambda: runner.entries_allowed.set()

    def _persist(result, *, today, source, now_et, last_error=None):
        runner.persisted_locks.append(
            {
                "result": dict(result),
                "today": today,
                "source": source,
                "now_et": now_et,
                "last_error": last_error,
            }
        )

    def _post(result):
        runner.post_calls.append(dict(result))
        return {"handoff_result": {"ok": True}, "readiness_result": {"ok": True}}

    runner._persist_overnight_reeval_lock = _persist
    runner._run_post_overnight_morning_handoff = _post
    return runner


def test_first_all_deferred_stall_schedules_retry_and_skips_post_handoff(monkeypatch):
    calls = []
    monkeypatch.setattr(ov, "run_overnight_reeval", lambda **kwargs: calls.append(kwargs) or _result())
    runner = _runner()

    result = runner.run_overnight_reeval_attempt(now_et=_dt(9), source="scheduler")

    assert result["result_class"] == "RETRYABLE_ALL_DEFERRED"
    assert result["retryable"] is True
    assert runner._overnight_reeval_success_date is None
    assert runner._overnight_reeval_next_retry_at is not None
    assert runner.post_calls == []
    assert len(calls) == 1


def test_client_runner_retry_owned_is_not_success_or_post_handoff(monkeypatch):
    """ClientRunner must keep recovery ownership incomplete until truth resolves."""
    monkeypatch.setattr(
        ov,
        "run_overnight_reeval",
        lambda **kwargs: _result(
            retry_owned=1,
            skipped=1,
            stalled=False,
            completed=True,
            retryable=False,
            retry_reason=None,
            result_class="COMPLETED_WITH_OWNED_RETRIES",
        ),
    )
    runner = _runner()

    result = runner.run_overnight_reeval_attempt(
        now_et=_dt(9, 31),
        source="scheduler",
    )

    assert result["result_class"] == "RETRYABLE_MARKET_TRUTH_PENDING"
    assert result["completed"] is False
    assert result["retryable"] is True
    assert result["retry_reason"] == "retry_owned_rows_remain"
    assert runner._overnight_reeval_success_date is None
    assert runner.post_calls == []
    assert runner._overnight_reeval_next_retry_at is not None
    assert runner.persisted_locks[-1]["result"]["completed"] is False


def test_premarket_9am_attempt_retries_on_interval_not_post_open(monkeypatch):
    """Blocker 2 fix: a 9:00 retry must be scheduled at now+RETRY_SEC, NOT at 9:30:05.
    PR #388 deferred contract selection to breach time; premarket work (prior-levels,
    snapshot, MC, OSM, watcher arm) can and must complete before open."""
    monkeypatch.setattr(ov, "run_overnight_reeval", lambda **kwargs: _result())
    runner = _runner()

    runner.run_overnight_reeval_attempt(now_et=_dt(9), source="scheduler")

    expected = _dt(9) + cr.timedelta(seconds=cr.OVERNIGHT_REEVAL_RETRY_SEC)
    assert runner._overnight_reeval_next_retry_at == expected, (
        f"9:00 retry must schedule at {expected} (now + interval), "
        f"not at post-open 9:30:05; got {runner._overnight_reeval_next_retry_at}"
    )
    # Explicitly confirm it does NOT land at or after 9:30:05
    post_open = _dt(9, 30, 5)
    assert runner._overnight_reeval_next_retry_at < post_open, (
        "premarket retry must not be forced to post-open — that recreates arm_already_through_trigger"
    )


def test_stalled_931_attempt_uses_retry_interval(monkeypatch):
    monkeypatch.setattr(ov, "run_overnight_reeval", lambda **kwargs: _result())
    runner = _runner()

    runner.run_overnight_reeval_attempt(now_et=_dt(9, 31), source="scheduler")

    assert runner._overnight_reeval_next_retry_at == _dt(9, 31) + cr.timedelta(
        seconds=cr.OVERNIGHT_REEVAL_RETRY_SEC
    )


def test_second_attempt_success_sets_success_clears_retry_and_runs_post_once(monkeypatch):
    results = [
        _result(),
        _result(
            armed=8,
            rejected=117,
            skipped=0,
            stalled=False,
            completed=True,
            retryable=False,
            retry_reason=None,
            result_class="COMPLETED_WITH_DECISIONS",
        ),
    ]
    monkeypatch.setattr(ov, "run_overnight_reeval", lambda **kwargs: results.pop(0))
    runner = _runner()

    runner.run_overnight_reeval_attempt(now_et=_dt(9), source="scheduler")
    result = runner.run_overnight_reeval_attempt(now_et=_dt(9, 30, 5), source="scheduler")

    assert result["completed"] is True
    assert runner._overnight_reeval_success_date == _dt(9).date()
    assert runner._overnight_reeval_next_retry_at is None
    assert len(runner.post_calls) == 1


def test_scheduler_tick_after_success_performs_no_engine_run(monkeypatch):
    calls = []
    monkeypatch.setattr(
        ov,
        "run_overnight_reeval",
        lambda **kwargs: calls.append(kwargs)
        or _result(stalled=False, completed=True, retryable=False, retry_reason=None, result_class="COMPLETED_WITH_DECISIONS"),
    )
    runner = _runner()

    runner.run_overnight_reeval_attempt(now_et=_dt(9, 30), source="scheduler")
    result = runner.run_overnight_reeval_attempt(now_et=_dt(9, 31), source="scheduler")

    assert result["result_class"] == "ALREADY_COMPLETED"
    assert result["completed"] is False
    assert result["attempt_performed"] is False
    assert len(calls) == 1
    assert len(runner.post_calls) == 1


def test_completed_post_readiness_exception_degrades_live(monkeypatch):
    monkeypatch.setattr(
        ov,
        "run_overnight_reeval",
        lambda **kwargs: _result(
            stalled=False,
            completed=True,
            retryable=False,
            retry_reason=None,
            result_class="COMPLETED_WITH_DECISIONS",
        ),
    )
    handoff_mod = types.ModuleType("ap.morning_handoff")
    handoff_mod.run_morning_handoff_audit = lambda **kwargs: {"ok": True}
    readiness_mod = types.ModuleType("ap.preopen_readiness")

    def _raise_readiness(*args, **kwargs):
        raise RuntimeError("db down")

    readiness_mod.run_preopen_autonomous_readiness = _raise_readiness
    monkeypatch.setitem(sys.modules, "ap.morning_handoff", handoff_mod)
    monkeypatch.setitem(sys.modules, "ap.preopen_readiness", readiness_mod)

    runner = _runner()
    runner.mode = "LIVE"
    runner._run_post_overnight_morning_handoff = cr.ClientRunner._run_post_overnight_morning_handoff.__get__(runner, cr.ClientRunner)

    result = runner.run_overnight_reeval_attempt(now_et=_dt(9, 31), source="scheduler")

    assert result["completed"] is True
    assert result["readiness_result"]["status"] == "ERROR"
    assert result["readiness_result"]["failure_reason"] == (
        "preopen_readiness_enforcement_failed:post_overnight_completion:exception:RuntimeError"
    )
    assert runner._overnight_reeval_success_date == _dt(9).date()
    assert any(
        reason.startswith("preopen_readiness_enforcement_failed:post_overnight_completion:")
        for reason in runner.degraded_reasons
    )


def test_completed_post_readiness_error_status_degrades_live(monkeypatch):
    monkeypatch.setattr(
        ov,
        "run_overnight_reeval",
        lambda **kwargs: _result(
            stalled=False,
            completed=True,
            retryable=False,
            retry_reason=None,
            result_class="COMPLETED_WITH_DECISIONS",
        ),
    )
    handoff_mod = types.ModuleType("ap.morning_handoff")
    handoff_mod.run_morning_handoff_audit = lambda **kwargs: {"ok": True}
    readiness_mod = types.ModuleType("ap.preopen_readiness")
    readiness_mod.run_preopen_autonomous_readiness = lambda *args, **kwargs: {
        "ok": False,
        "status": "ERROR",
        "errors": ["db_unavailable"],
    }
    monkeypatch.setitem(sys.modules, "ap.morning_handoff", handoff_mod)
    monkeypatch.setitem(sys.modules, "ap.preopen_readiness", readiness_mod)

    runner = _runner()
    runner.mode = "LIVE"
    runner._run_post_overnight_morning_handoff = cr.ClientRunner._run_post_overnight_morning_handoff.__get__(runner, cr.ClientRunner)

    result = runner.run_overnight_reeval_attempt(now_et=_dt(9, 31), source="scheduler")

    assert result["readiness_result"]["failure_reason"] == (
        "preopen_readiness_enforcement_failed:post_overnight_completion:status_error"
    )
    assert "preopen_readiness_enforcement_failed:post_overnight_completion:status_error" in runner.degraded_reasons


def test_already_completed_retries_readiness_after_deadline_when_enforcement_failed(monkeypatch):
    engine_calls = []
    monkeypatch.setattr(ov, "run_overnight_reeval", lambda **kwargs: engine_calls.append(kwargs) or _result())
    readiness_calls = []
    readiness_mod = types.ModuleType("ap.preopen_readiness")
    readiness_mod.run_preopen_autonomous_readiness = lambda *args, **kwargs: (
        readiness_calls.append((args, kwargs)) or {"ok": True, "status": "OK", "errors": []}
    )
    monkeypatch.setitem(sys.modules, "ap.preopen_readiness", readiness_mod)

    runner = _runner()
    runner.mode = "LIVE"
    today = _dt(9).date()
    runner._overnight_reeval_state_date = today
    runner._overnight_reeval_success_date = today
    runner.degraded_reasons = {
        "preopen_readiness_enforcement_failed:post_overnight_completion:status_error"
    }
    runner.degraded.set()
    runner.entries_allowed.clear()

    result = runner.run_overnight_reeval_attempt(now_et=_dt(9, 31), source="scheduler")

    assert result["result_class"] == "ALREADY_COMPLETED"
    assert result["attempt_performed"] is False
    assert result["readiness_result"]["status"] == "OK"
    assert engine_calls == []
    assert len(readiness_calls) == 1
    assert not runner._has_degraded_reason_key("preopen_readiness_enforcement_failed")


def test_already_completed_retries_readiness_after_deadline_when_only_blocked_reason_exists(monkeypatch):
    engine_calls = []
    monkeypatch.setattr(
        ov,
        "run_overnight_reeval",
        lambda **kwargs: engine_calls.append(kwargs)
        or _result(
            stalled=False,
            completed=True,
            retryable=False,
            retry_reason=None,
            result_class="COMPLETED_WITH_DECISIONS",
        ),
    )
    handoff_mod = types.ModuleType("ap.morning_handoff")
    handoff_mod.run_morning_handoff_audit = lambda **kwargs: {"ok": True}
    readiness_responses = iter(
        [
            {"ok": False, "status": "BLOCKED", "errors": ["watcher_missing"]},
            {"ok": True, "status": "OK", "errors": []},
        ]
    )
    readiness_mod = types.ModuleType("ap.preopen_readiness")
    readiness_mod.run_preopen_autonomous_readiness = lambda *args, **kwargs: next(readiness_responses)
    monkeypatch.setitem(sys.modules, "ap.morning_handoff", handoff_mod)
    monkeypatch.setitem(sys.modules, "ap.preopen_readiness", readiness_mod)

    runner = _runner()
    runner.mode = "LIVE"
    runner._run_post_overnight_morning_handoff = cr.ClientRunner._run_post_overnight_morning_handoff.__get__(runner, cr.ClientRunner)

    completed = runner.run_overnight_reeval_attempt(now_et=_dt(9, 30), source="scheduler")

    assert completed["completed"] is True
    assert completed["readiness_result"]["status"] == "BLOCKED"
    assert runner.degraded_reasons == {"preopen_readiness_blocked:watcher_missing"}
    assert len(engine_calls) == 1

    recovered = runner.run_overnight_reeval_attempt(now_et=_dt(9, 31), source="scheduler")

    assert recovered["result_class"] == "ALREADY_COMPLETED"
    assert recovered["attempt_performed"] is False
    assert recovered["readiness_result"]["status"] == "OK"
    assert not runner._has_degraded_reason_key("preopen_readiness_enforcement_failed")
    assert not runner._has_degraded_reason_key("preopen_readiness_blocked")
    assert runner.entries_allowed.is_set() is True
    assert len(engine_calls) == 1


def test_already_completed_retries_blocked_readiness_after_window_close_without_engine_rerun(monkeypatch):
    engine_calls = []
    monkeypatch.setattr(
        ov,
        "run_overnight_reeval",
        lambda **kwargs: engine_calls.append(kwargs)
        or _result(
            stalled=False,
            completed=True,
            retryable=False,
            retry_reason=None,
            result_class="COMPLETED_WITH_DECISIONS",
        ),
    )
    handoff_mod = types.ModuleType("ap.morning_handoff")
    handoff_mod.run_morning_handoff_audit = lambda **kwargs: {"ok": True}
    readiness_calls = []
    readiness_responses = iter(
        [
            {"ok": False, "status": "BLOCKED", "errors": ["watcher_missing"]},
            {"ok": True, "status": "OK", "errors": []},
        ]
    )
    readiness_mod = types.ModuleType("ap.preopen_readiness")
    readiness_mod.run_preopen_autonomous_readiness = lambda *args, **kwargs: (
        readiness_calls.append((args, kwargs)) or next(readiness_responses)
    )
    monkeypatch.setitem(sys.modules, "ap.morning_handoff", handoff_mod)
    monkeypatch.setitem(sys.modules, "ap.preopen_readiness", readiness_mod)

    runner = _runner()
    runner.mode = "LIVE"
    runner._run_post_overnight_morning_handoff = cr.ClientRunner._run_post_overnight_morning_handoff.__get__(runner, cr.ClientRunner)

    completed = runner.run_overnight_reeval_attempt(now_et=_dt(9, 44), source="scheduler")

    assert completed["completed"] is True
    assert completed["readiness_result"]["status"] == "BLOCKED"
    assert runner.degraded_reasons == {"preopen_readiness_blocked:watcher_missing"}
    assert len(engine_calls) == 1

    recovered = runner.run_overnight_reeval_attempt(now_et=_dt(9, 50), source="scheduler")

    assert recovered["result_class"] == "ALREADY_COMPLETED"
    assert recovered["attempt_performed"] is False
    assert recovered["readiness_result"]["status"] == "OK"
    assert len(readiness_calls) == 2
    assert not runner._has_degraded_reason_key("preopen_readiness_enforcement_failed")
    assert not runner._has_degraded_reason_key("preopen_readiness_blocked")
    assert runner.entries_allowed.is_set() is True
    assert len(engine_calls) == 1


def test_recovered_ok_readiness_clears_enforcement_failure_and_blocked(monkeypatch):
    readiness_mod = types.ModuleType("ap.preopen_readiness")
    readiness_mod.run_preopen_autonomous_readiness = lambda *args, **kwargs: {
        "ok": True,
        "status": "OK",
        "errors": [],
    }
    monkeypatch.setitem(sys.modules, "ap.preopen_readiness", readiness_mod)

    runner = _runner()
    runner.mode = "LIVE"
    today = _dt(9).date()
    runner._overnight_reeval_state_date = today
    runner._overnight_reeval_success_date = today
    runner.degraded_reasons = {
        "preopen_readiness_enforcement_failed:post_overnight_completion:status_error",
        "preopen_readiness_blocked:watcher_missing",
    }
    runner.degraded.set()
    runner.entries_allowed.clear()

    runner.run_overnight_reeval_attempt(now_et=_dt(9, 31), source="scheduler")

    assert not runner._has_degraded_reason_key("preopen_readiness_enforcement_failed")
    assert not runner._has_degraded_reason_key("preopen_readiness_blocked")
    assert runner.entries_allowed.is_set() is True


def test_no_work_classifies_completed_no_work():
    result = ov._classify_overnight_reeval_result({"fetched": 0, "errors": 0, "skipped": 0})

    assert result["result_class"] == "COMPLETED_NO_WORK"
    assert result["completed"] is True
    assert result["retryable"] is False


def test_errors_are_retryable_even_when_one_row_armed():
    result = ov._classify_overnight_reeval_result(
        {"fetched": 10, "armed": 1, "rejected": 0, "skipped": 8, "errors": 1, "stalled": False}
    )

    assert result["result_class"] == "RETRYABLE_ROW_ERRORS"
    assert result["retryable"] is True
    assert result["retry_reason"] == "row_errors"


def test_exception_is_retryable_and_does_not_complete(monkeypatch):
    def _raise(**_kwargs):
        raise RuntimeError("market data down")

    monkeypatch.setattr(ov, "run_overnight_reeval", _raise)
    runner = _runner()

    result = runner.run_overnight_reeval_attempt(now_et=_dt(9, 31), source="scheduler")

    assert result["result_class"] == "RETRYABLE_EXCEPTION"
    assert result["retryable"] is True
    assert result["completed"] is False
    assert result["errors"] == 1
    assert runner._overnight_reeval_success_date is None


def test_max_attempts_stop_additional_runs_without_success(monkeypatch):
    calls = []
    monkeypatch.setattr(ov, "run_overnight_reeval", lambda **kwargs: calls.append(kwargs) or _result())
    runner = _runner()
    runner._overnight_reeval_state_date = _dt(9).date()
    runner._overnight_reeval_attempt_count = cr.OVERNIGHT_REEVAL_MAX_ATTEMPTS

    result = runner.run_overnight_reeval_attempt(now_et=_dt(9, 31), source="scheduler")

    assert result["result_class"] == "RETRY_EXHAUSTED"
    assert result["completed"] is False
    assert runner._overnight_reeval_success_date is None
    assert calls == []


def test_945_window_stops_later_retries_and_persists_exhaustion(monkeypatch):
    monkeypatch.setattr(ov, "run_overnight_reeval", lambda **kwargs: _result())
    runner = _runner()

    result = runner.run_overnight_reeval_attempt(now_et=_dt(9, 44), source="scheduler")

    assert result["retry_exhausted"] is True
    assert result["result_class"] == "RETRY_EXHAUSTED"
    assert result["retryable"] is False
    assert result["last_error"] == "OVERNIGHT_REEVAL_RETRY_EXHAUSTED"
    assert runner._overnight_reeval_next_retry_at is None
    assert runner._overnight_reeval_exhausted_date == _dt(9).date()
    assert runner.persisted_locks[-1]["last_error"] == "OVERNIGHT_REEVAL_RETRY_EXHAUSTED"


def test_repeated_health_ticks_after_window_exhaustion_do_not_rerun(monkeypatch):
    calls = []
    monkeypatch.setattr(ov, "run_overnight_reeval", lambda **kwargs: calls.append(kwargs) or _result())
    runner = _runner()

    first = runner.run_overnight_reeval_attempt(now_et=_dt(9, 44), source="scheduler")
    second = runner.run_overnight_reeval_attempt(now_et=_dt(9, 44, 30), source="scheduler")
    third = runner.run_overnight_reeval_attempt(now_et=_dt(9, 44, 59), source="scheduler")

    assert first["result_class"] == "RETRY_EXHAUSTED"
    assert second["result_class"] == "RETRY_EXHAUSTED"
    assert third["result_class"] == "RETRY_EXHAUSTED"
    assert second["attempt_performed"] is False
    assert third["attempt_performed"] is False
    assert len(calls) == 1


def test_window_boundary_is_945_exclusive(monkeypatch):
    calls = []
    monkeypatch.setattr(ov, "run_overnight_reeval", lambda **kwargs: calls.append(kwargs) or _result())
    runner = _runner()

    before = runner.run_overnight_reeval_attempt(now_et=_dt(9, 44, 59), source="scheduler")
    at_end = runner.run_overnight_reeval_attempt(now_et=_dt(9, 45, 0), source="scheduler")
    after_end = runner.run_overnight_reeval_attempt(now_et=_dt(9, 45, 30), source="scheduler")

    assert before["attempt_performed"] is True
    assert at_end["result_class"] == "SKIPPED_NOT_DUE"
    assert after_end["result_class"] == "SKIPPED_NOT_DUE"
    assert at_end["attempt_performed"] is False
    assert after_end["attempt_performed"] is False
    assert len(calls) == 1


def test_retry_scheduled_at_exact_window_end_is_exhausted(monkeypatch):
    monkeypatch.setattr(ov, "run_overnight_reeval", lambda **kwargs: _result())
    runner = _runner()

    result = runner.run_overnight_reeval_attempt(now_et=_dt(9, 43, 30), source="scheduler")

    assert result["result_class"] == "RETRY_EXHAUSTED"
    assert result["next_retry_at"] is None
    assert runner._overnight_reeval_exhausted_date == _dt(9).date()


def test_client_runner_objects_maintain_independent_state(monkeypatch):
    monkeypatch.setattr(ov, "run_overnight_reeval", lambda **kwargs: _result())
    jose = _runner("jose@example.com")
    tradefluence = _runner("tradefluence@example.com")

    jose.run_overnight_reeval_attempt(now_et=_dt(9), source="scheduler")

    assert jose._overnight_reeval_attempt_count == 1
    assert tradefluence._overnight_reeval_attempt_count == 0
    assert jose._overnight_reeval_next_retry_at is not tradefluence._overnight_reeval_next_retry_at


def test_concurrent_scheduler_and_admin_share_inflight_lock(monkeypatch):
    calls = []

    def _slow(**kwargs):
        calls.append(kwargs)
        time.sleep(0.05)
        return _result()

    monkeypatch.setattr(ov, "run_overnight_reeval", _slow)
    runner = _runner()
    out = []
    t = threading.Thread(
        target=lambda: out.append(runner.run_overnight_reeval_attempt(now_et=_dt(9, 31), source="scheduler"))
    )
    t.start()
    time.sleep(0.01)
    admin_result = runner.run_overnight_reeval_attempt(now_et=_dt(9, 31), source="admin_sync")
    t.join()

    assert len(calls) == 1
    assert admin_result["result_class"] == "ATTEMPT_ALREADY_RUNNING"


def test_sync_admin_path_uses_runner_owned_transition():
    assert 'source="admin_sync"' in APP_SRC
    admin_block = APP_SRC[APP_SRC.index('@app.post("/admin/overnight_reeval")'):]
    assert "runner.run_overnight_reeval_attempt" in admin_block


def test_background_admin_path_uses_runner_owned_transition():
    assert 'source="admin_background"' in APP_SRC
    assert "runner.run_overnight_reeval_attempt" in APP_SRC


def test_no_last_overnight_reeval_date_assignment_remains_in_runner_or_app():
    assert "_last_overnight_reeval_date =" not in CR_SRC
    assert "_last_overnight_reeval_date =" not in APP_SRC


def test_stalled_result_never_calls_post_overnight_handoff(monkeypatch):
    monkeypatch.setattr(ov, "run_overnight_reeval", lambda **kwargs: _result())
    runner = _runner()

    runner.run_overnight_reeval_attempt(now_et=_dt(9, 31), source="scheduler")

    assert runner.post_calls == []


def test_successful_result_calls_post_overnight_handoff_once(monkeypatch):
    monkeypatch.setattr(
        ov,
        "run_overnight_reeval",
        lambda **kwargs: _result(stalled=False, completed=True, retryable=False, retry_reason=None, result_class="COMPLETED_WITH_DECISIONS"),
    )
    runner = _runner()

    runner.run_overnight_reeval_attempt(now_et=_dt(9, 31), source="scheduler")

    assert len(runner.post_calls) == 1


def test_residual_counts_override_false_completed_result(monkeypatch):
    monkeypatch.setattr(
        ov,
        "run_overnight_reeval",
        lambda **kwargs: _result(
            stalled=False,
            completed=True,
            retryable=False,
            retry_reason=None,
            result_class="COMPLETED_WITH_DECISIONS",
            retryable_deferred=1,
            unresolved=0,
        ),
    )
    runner = _runner()

    result = runner.run_overnight_reeval_attempt(now_et=_dt(9, 31), source="scheduler")

    assert result["result_class"] == "RETRYABLE_PARTIAL_DEFERRED"
    assert result["completed"] is False
    assert result["retryable"] is True
    assert runner.post_calls == []
    assert runner._overnight_reeval_success_date is None


def test_retry_scheduler_does_not_create_duplicate_entry_orders():
    body = inspect.getsource(cr.ClientRunner.run_overnight_reeval_attempt)
    assert "create_entry_order" not in body
    assert "enqueue_signal" not in body


def test_retry_scheduler_does_not_arm_watcher_directly():
    body = inspect.getsource(cr.ClientRunner.run_overnight_reeval_attempt)
    assert ".add_order" not in body
    assert ".arm" not in body


def test_retry_scheduler_does_not_revive_terminal_or_stale_rows():
    body = inspect.getsource(cr.ClientRunner.run_overnight_reeval_attempt)
    forbidden = ["REJECTED", "terminal", "_mark_job", "UPDATE trade_queue", "decision_status"]
    assert all(token not in body for token in forbidden)


def test_client_identity_and_execution_mode_are_runner_owned(monkeypatch):
    captured = []
    monkeypatch.setattr(ov, "run_overnight_reeval", lambda **kwargs: captured.append(kwargs) or _result())
    runner = _runner("jose@example.com")

    runner.run_overnight_reeval_attempt(now_et=_dt(9, 31), source="scheduler")

    assert captured[0]["client_id"] == "jose@example.com"
    assert runner.persisted_locks[-1]["source"] == "scheduler"


def test_retry_scheduler_does_not_call_broker_submit_or_cancel():
    body = inspect.getsource(cr.ClientRunner.run_overnight_reeval_attempt)
    assert ".submit" not in body
    assert ".cancel" not in body
    assert "cancel_order" not in body


def test_force_true_bypasses_success_suppression_but_respects_lock(monkeypatch):
    calls = []
    monkeypatch.setattr(
        ov,
        "run_overnight_reeval",
        lambda **kwargs: calls.append(kwargs)
        or _result(stalled=False, completed=True, retryable=False, retry_reason=None, result_class="COMPLETED_WITH_DECISIONS"),
    )
    runner = _runner()
    runner._overnight_reeval_state_date = _dt(9).date()
    runner._overnight_reeval_success_date = _dt(9).date()
    runner._overnight_reeval_attempt_lock.acquire()
    try:
        locked = runner.run_overnight_reeval_attempt(force=True, now_et=_dt(9, 32), source="admin_sync")
    finally:
        runner._overnight_reeval_attempt_lock.release()
    forced = runner.run_overnight_reeval_attempt(force=True, now_et=_dt(9, 33), source="admin_sync")

    assert locked["result_class"] == "ATTEMPT_ALREADY_RUNNING"
    assert forced["completed"] is True
    assert len(calls) == 1


def test_force_true_bypasses_exhausted_suppression(monkeypatch):
    calls = []
    monkeypatch.setattr(ov, "run_overnight_reeval", lambda **kwargs: calls.append(kwargs) or _result())
    runner = _runner()
    runner._overnight_reeval_state_date = _dt(9).date()
    runner._overnight_reeval_exhausted_date = _dt(9).date()

    automatic = runner.run_overnight_reeval_attempt(now_et=_dt(9, 32), source="scheduler")
    forced = runner.run_overnight_reeval_attempt(force=True, now_et=_dt(9, 33), source="admin_sync")

    assert automatic["result_class"] == "RETRY_EXHAUSTED"
    assert automatic["attempt_performed"] is False
    assert forced["attempt_performed"] is True
    assert len(calls) == 1


def test_lock_details_contain_attempt_result_retry_and_next_retry(monkeypatch):
    calls = []
    mod = types.ModuleType("ap.morning_handoff")
    mod._upsert_handoff_run_lock = lambda **kwargs: calls.append(kwargs)
    monkeypatch.setitem(sys.modules, "ap.morning_handoff", mod)
    runner = _runner()
    del runner._persist_overnight_reeval_lock

    runner._persist_overnight_reeval_lock(
        _result(
            terminal_rejected=3,
            terminal_errors=0,
            retryable_deferred=122,
            unresolved=0,
        ),
        today=_dt(9).date(),
        source="scheduler",
        now_et=_dt(9),
        last_error=None,
    )

    details = calls[0]["details"]
    assert details["attempt_count"] == 0
    assert details["result_class"] == "RETRYABLE_ALL_DEFERRED"
    assert details["retryable"] is True
    assert details["terminal_rejected"] == 3
    assert details["terminal_errors"] == 0
    assert details["retryable_deferred"] == 122
    assert details["unresolved"] == 0
    assert "next_retry_at" in details


def test_status_is_never_success_when_completed_false(monkeypatch):
    calls = []
    mod = types.ModuleType("ap.morning_handoff")
    mod._upsert_handoff_run_lock = lambda **kwargs: calls.append(kwargs)
    monkeypatch.setitem(sys.modules, "ap.morning_handoff", mod)
    runner = _runner()
    del runner._persist_overnight_reeval_lock

    runner._persist_overnight_reeval_lock(
        _result(completed=False),
        today=_dt(9).date(),
        source="scheduler",
        now_et=_dt(9),
    )

    assert calls[0]["status"] != "success"
    assert calls[0]["mark_success"] is False


def test_production_engine_keeps_idempotency_and_watcher_guards():
    assert "create_entry_order(" in OV_SRC
    assert "initial_status=\"PENDING_TRIGGER\"" in OV_SRC
    assert "entry_watcher" in OV_SRC
    assert "duplicate_setup:same_session_watch_arm_or_terminal_failure_proof" in OV_SRC
