from __future__ import annotations

import os
import sys
import types

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_p0_manual_rescue_restart_guard_bypass",
)
os.environ.setdefault("ENCRYPTION_KEY", "angel-manual-rescue-restart-guard-test-key-2026")

if "supabase" not in sys.modules:
    _supa_stub = types.ModuleType("supabase")
    _supa_stub.create_client = lambda *a, **kw: None
    _supa_stub.Client = type("Client", (), {})
    sys.modules["supabase"] = _supa_stub


def test_manual_restart_guard_bypass_enabled_from_result_json():
    from ap.queue import _manual_restart_guard_bypass_enabled

    assert _manual_restart_guard_bypass_enabled(
        job_last_error=None,
        job_result={"manual_rescue": True},
        execution_mode="PAPER",
    ) is True
    assert _manual_restart_guard_bypass_enabled(
        job_last_error=None,
        job_result={"manual_rescue": True},
        execution_mode="LIVE",
    ) is False


def test_manual_restart_guard_bypass_enabled_from_manual_error_marker():
    from ap.queue import _manual_restart_guard_bypass_enabled

    assert _manual_restart_guard_bypass_enabled(
        job_last_error="manual_requeue_after_overnight_reeval_timeout",
        job_result=None,
        execution_mode="PAPER",
    ) is True
    assert _manual_restart_guard_bypass_enabled(
        job_last_error="manual_rescue_current_session",
        job_result=None,
        execution_mode="PAPER",
    ) is True
    assert _manual_restart_guard_bypass_enabled(
        job_last_error="manual_rescue_current_session",
        job_result=None,
        execution_mode="LIVE",
    ) is False



def test_manual_restart_guard_bypass_enabled_from_paper_overnight_only_payload():
    from ap.queue import _manual_restart_guard_bypass_enabled

    assert _manual_restart_guard_bypass_enabled(
        job_last_error=None,
        job_result=None,
        payload={"force_overnight_reeval_only": True, "do_not_queue_directly": True},
        execution_mode="PAPER",
    ) is True
    assert _manual_restart_guard_bypass_enabled(
        job_last_error=None,
        job_result=None,
        payload={"force_overnight_reeval_only": True, "do_not_queue_directly": True},
        execution_mode="LIVE",
    ) is False


def test_dispatch_paper_overnight_only_payload_restores_watching_instead_of_direct_execution(monkeypatch):
    import ap.queue as queue_mod

    calls: list[tuple[str, str | None]] = []

    class _DummyMC:
        mode = "PAPER"
        _equity_cache_ts = 10**12

        def evaluate(self, payload, client_id=None):
            calls.append(("evaluate", payload.get("_queue_id")))
            raise RuntimeError("should_not_reach_master_control")

    monkeypatch.setattr(queue_mod, "_mark_job", lambda job_id, status, *, result=None, error=None: calls.append((status, error)))

    queue_mod._dispatch(
        99,
        "paper-client",
        "sig-paper",
        {
            "ticker": "HD",
            "side": "CALL",
            "score": 78,
            "force_overnight_reeval_only": True,
            "do_not_queue_directly": True,
        },
        job_last_error="manual_rescue_current_session",
        job_result={"manual_rescue": True},
        master_control=_DummyMC(),
        contract_selector=None,
        order_state_machine=None,
        entry_watcher=None,
        position_manager=None,
        exit_eng=None,
        broker=None,
        on_split_brain=None,
    )

    assert ("evaluate", 99) not in calls
    assert ("WATCHING", "after_hours_deferred:awaiting_overnight_reeval") in calls
    assert ("REJECTED", "restart_guard:overnight_skip") not in calls


def test_manual_restart_guard_bypass_disabled_without_marker():
    from ap.queue import _manual_restart_guard_bypass_enabled

    assert _manual_restart_guard_bypass_enabled(
        job_last_error="restart_guard:overnight_skip",
        job_result=None,
        execution_mode="PAPER",
    ) is False
    assert _manual_restart_guard_bypass_enabled(
        job_last_error="manual_rescue_current_session",
        job_result={"manual_rescue": True},
        execution_mode=None,
    ) is False


def test_dispatch_manual_rescue_bypasses_restart_guard_and_reaches_master_control(monkeypatch):
    import ap.queue as queue_mod

    calls: list[tuple[str, str | None]] = []

    class _DummyMC:
        mode = "PAPER"
        _equity_cache_ts = 10**12

        def evaluate(self, payload, client_id=None):
            calls.append(("evaluate", payload.get("_queue_id")))
            raise RuntimeError("stop_after_restart_guard")

    fake_restart_guard = types.ModuleType("ap.restart_guard")
    fake_restart_guard.should_skip_on_restart = lambda payload: True
    monkeypatch.setitem(sys.modules, "ap.restart_guard", fake_restart_guard)

    monkeypatch.setattr(queue_mod, "_mark_job", lambda job_id, status, *, result=None, error=None: calls.append((status, error)))

    queue_mod._dispatch(
        42,
        "client-1",
        "sig-1",
        {"ticker": "AVGO", "side": "CALL", "score": 78},
        job_last_error="manual_rescue_current_session",
        job_result=None,
        master_control=_DummyMC(),
        contract_selector=None,
        order_state_machine=None,
        entry_watcher=None,
        position_manager=None,
        exit_eng=None,
        broker=None,
        on_split_brain=None,
    )

    assert ("evaluate", 42) in calls
    assert ("REJECTED", "restart_guard:overnight_skip") not in calls


def test_dispatch_live_manual_rescue_does_not_bypass_restart_guard(monkeypatch):
    import ap.queue as queue_mod

    calls: list[tuple[str, str | None]] = []

    class _DummyMC:
        mode = "LIVE"
        _equity_cache_ts = 10**12

        def evaluate(self, payload, client_id=None):
            calls.append(("evaluate", payload.get("_queue_id")))
            raise RuntimeError("live_should_not_reach_master_control")

    fake_restart_guard = types.ModuleType("ap.restart_guard")
    fake_restart_guard.should_skip_on_restart = lambda payload: True
    monkeypatch.setitem(sys.modules, "ap.restart_guard", fake_restart_guard)

    monkeypatch.setattr(queue_mod, "_mark_job", lambda job_id, status, *, result=None, error=None: calls.append((status, error)))

    queue_mod._dispatch(
        77,
        "jasoncosby1@gmail.com",
        "sig-live",
        {"ticker": "AVGO", "side": "CALL", "score": 78},
        job_last_error="manual_rescue_current_session",
        job_result={"manual_rescue": True},
        master_control=_DummyMC(),
        contract_selector=None,
        order_state_machine=None,
        entry_watcher=None,
        position_manager=None,
        exit_eng=None,
        broker=None,
        on_split_brain=None,
    )

    assert ("evaluate", 77) not in calls
    assert ("REJECTED", "restart_guard:overnight_skip") in calls
