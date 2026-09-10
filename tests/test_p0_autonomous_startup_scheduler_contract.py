from __future__ import annotations

import logging
import threading
import time
from pathlib import Path
from types import SimpleNamespace


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_scheduler_counts_actual_clients_not_endpoint_calls():
    from ap.morning_jobs import MorningJobCall
    from ap.scripts.live_morning_jobs import _client_telemetry_from_calls

    calls = [
        MorningJobCall(
            job_name="morning_handoff_primary_paper",
            endpoint="/admin/morning_handoff_audit",
            payload={
                "clients": [
                    "jose.vasquez4011@gmail.com",
                    "tradefluencehq@gmail.com",
                ],
                "mode": "paper",
            },
            client_scope="jose.vasquez4011@gmail.com,tradefluencehq@gmail.com",
            execution_mode="paper",
        ),
        MorningJobCall(
            job_name="morning_handoff_primary_live",
            endpoint="/admin/morning_handoff_audit",
            payload={"clients": ["jasoncosby1@gmail.com"], "mode": "live"},
            client_scope="jasoncosby1@gmail.com",
            execution_mode="live",
        ),
    ]

    telemetry = _client_telemetry_from_calls(calls)

    assert telemetry["client_count"] == 3
    assert telemetry["paper_client_count"] == 2
    assert telemetry["live_client_count"] == 1
    assert telemetry["unknown_client_count"] == 0
    assert telemetry["client_ids"] == [
        "jose.vasquez4011@gmail.com",
        "tradefluencehq@gmail.com",
        "jasoncosby1@gmail.com",
    ]
    assert telemetry["paper_client_ids"] == [
        "jose.vasquez4011@gmail.com",
        "tradefluencehq@gmail.com",
    ]
    assert telemetry["live_client_ids"] == ["jasoncosby1@gmail.com"]
    assert telemetry["unknown_client_ids"] == []


def test_scheduler_counts_mixed_recovery_call_by_client_mode():
    from ap.morning_jobs import MorningJobCall
    from ap.scripts.live_morning_jobs import _client_telemetry_from_calls

    calls = [
        MorningJobCall(
            job_name="release_after_hours_deferred_live+paper",
            endpoint="/admin/release_after_hours_deferred",
            payload={
                "clients": [
                    {"client_id": "jose.vasquez4011@gmail.com", "execution_mode": "paper"},
                    {"client_id": "tradefluencehq@gmail.com", "execution_mode": "paper"},
                    {"client_id": "jasoncosby1@gmail.com", "execution_mode": "live"},
                ],
            },
            client_scope="jasoncosby1@gmail.com,jose.vasquez4011@gmail.com,tradefluencehq@gmail.com",
            execution_mode="mixed",
        ),
    ]

    telemetry = _client_telemetry_from_calls(calls)

    assert telemetry["client_count"] == 3
    assert telemetry["paper_client_count"] == 2
    assert telemetry["live_client_count"] == 1
    assert telemetry["unknown_client_count"] == 0
    assert telemetry["paper_client_ids"] == [
        "jose.vasquez4011@gmail.com",
        "tradefluencehq@gmail.com",
    ]
    assert telemetry["live_client_ids"] == ["jasoncosby1@gmail.com"]
    assert telemetry["unknown_client_ids"] == []


def test_scheduler_deduplicates_client_across_live_and_mixed_calls():
    from ap.morning_jobs import MorningJobCall
    from ap.scripts.live_morning_jobs import _client_telemetry_from_calls

    calls = [
        MorningJobCall(
            job_name="morning_handoff_primary_live",
            endpoint="/admin/morning_handoff_audit",
            payload={"clients": ["jasoncosby1@gmail.com"], "mode": "live"},
            client_scope="jasoncosby1@gmail.com",
            execution_mode="live",
        ),
        MorningJobCall(
            job_name="release_after_hours_deferred_live+paper",
            endpoint="/admin/release_after_hours_deferred",
            payload={
                "clients": [
                    {"client_id": "jasoncosby1@gmail.com", "execution_mode": "live"},
                ],
            },
            client_scope="jasoncosby1@gmail.com",
            execution_mode="mixed",
        ),
    ]

    telemetry = _client_telemetry_from_calls(calls)

    assert telemetry["client_count"] == 1
    assert telemetry["live_client_count"] == 1
    assert telemetry["client_ids"] == ["jasoncosby1@gmail.com"]


def test_scheduler_keeps_unproven_mixed_client_visible_as_unknown():
    from ap.morning_jobs import MorningJobCall
    from ap.scripts.live_morning_jobs import _client_telemetry_from_calls

    calls = [
        MorningJobCall(
            job_name="release_after_hours_deferred_live+paper",
            endpoint="/admin/release_after_hours_deferred",
            payload={"clients": ["future-client@example.com"]},
            client_scope="future-client@example.com",
            execution_mode="mixed",
        ),
    ]

    telemetry = _client_telemetry_from_calls(calls)

    assert telemetry["client_count"] == 1
    assert telemetry["paper_client_count"] == 0
    assert telemetry["live_client_count"] == 0
    assert telemetry["unknown_client_count"] == 1
    assert telemetry["client_ids"] == ["future-client@example.com"]
    assert telemetry["unknown_client_ids"] == ["future-client@example.com"]


def test_scheduler_entrypoint_emits_autonomy_lifecycle_markers():
    script_src = (REPO_ROOT / "ap" / "scripts" / "live_morning_jobs.py").read_text()
    assert "AUTONOMY_JOB_REGISTERED" in script_src
    assert "OVERNIGHT_JOB_STARTED" in script_src
    assert "MORNING_REEVAL_STARTED" in script_src
    assert "unknown_client_count=%s" in script_src
    assert "paper_client_ids=%s live_client_ids=%s unknown_client_ids=%s" in script_src


def test_runtime_startup_logs_recovery_and_handoff_markers():
    runner_src = (REPO_ROOT / "client_runner.py").read_text()
    handoff_src = (REPO_ROOT / "ap" / "morning_handoff.py").read_text()
    assert "STARTUP_RECOVERY_COMPLETE" in runner_src
    assert "status=%s" in runner_src
    assert "duration_ms=%s" in runner_src
    assert "recovery_attempt_id=%s" in runner_src
    assert "CLIENT_HANDOFF_STARTED" in handoff_src
    assert "CLIENT_HANDOFF_COMPLETE" in handoff_src
    assert "deferred_retries_restored" in handoff_src


def test_startup_lock_cannot_skip_runtime_ownership_recovery():
    handoff_src = (REPO_ROOT / "ap" / "morning_handoff.py").read_text()
    assert "Startup is process ownership, not only a daily job event" in handoff_src
    assert "can_skip_existing = False" in handoff_src
    assert "_has_unowned_pending_trigger_orders(client_id, entry_watcher, now=now)" not in handoff_src


def _runner_for_startup_recovery(client_runner_mod, *, mode="PAPER"):
    runner = client_runner_mod.ClientRunner.__new__(client_runner_mod.ClientRunner)
    runner.email = "telemetry@example.com"
    runner.mode = mode
    runner.degraded = threading.Event()
    runner.entries_allowed = threading.Event()
    runner.degraded_reasons = set()
    runner._degraded_lock = threading.Lock()
    runner.order_state_machine = object()
    runner.position_manager = object()
    runner.master_control = object()
    runner.core = SimpleNamespace(entry_watcher=object())
    return runner


def _startup_recovery_messages(caplog):
    return [
        record.getMessage()
        for record in caplog.records
        if "STARTUP_RECOVERY_COMPLETE" in record.getMessage()
    ]


def test_startup_recovery_timeout_still_emits_completion_marker(monkeypatch, caplog):
    import client_runner

    class _Recovery:
        def __init__(self, **kwargs):
            pass

        def run(self, include_watcher_reseed=False):
            time.sleep(0.02)
            return {}

    monkeypatch.setattr(client_runner, "APStartupRecovery", _Recovery)
    monkeypatch.setenv("STARTUP_RECOVERY_TIMEOUT_SEC", "0.001")
    caplog.set_level(logging.INFO, logger="client_runner")

    runner = _runner_for_startup_recovery(client_runner)
    runner._run_startup_recovery(broker=object(), exit_eng=object())

    messages = _startup_recovery_messages(caplog)
    assert len(messages) == 1
    assert "status=timeout" in messages[0]
    assert "recovery_attempt_id=" in messages[0]


def test_startup_recovery_exception_emits_failed_marker(monkeypatch, caplog):
    import client_runner

    class _Recovery:
        def __init__(self, **kwargs):
            pass

        def run(self, include_watcher_reseed=False):
            raise RuntimeError("forced recovery failure")

    monkeypatch.setattr(client_runner, "APStartupRecovery", _Recovery)
    caplog.set_level(logging.INFO, logger="client_runner")

    runner = _runner_for_startup_recovery(client_runner, mode="LIVE")
    degraded = []
    runner._enter_degraded_mode = lambda reason, stop_runner=False: degraded.append((reason, stop_runner))
    runner._run_startup_recovery(broker=object(), exit_eng=object())

    messages = _startup_recovery_messages(caplog)
    assert len(messages) == 1
    assert "status=failed" in messages[0]
    assert "forced recovery failure" in messages[0]
    assert degraded == [("startup_recovery_failed:forced recovery failure", False)]


def test_startup_recovery_success_emits_complete_metrics(monkeypatch, caplog):
    import client_runner

    class _Recovery:
        def __init__(self, **kwargs):
            pass

        def run(self, include_watcher_reseed=False):
            return {
                "positions_recovered": 4,
                "entries_corrected": 3,
                "watchers_requeued": 2,
                "deferred_lifecycles_recovered": 1,
                "errors": [],
            }

    monkeypatch.setattr(client_runner, "APStartupRecovery", _Recovery)
    caplog.set_level(logging.INFO, logger="client_runner")

    runner = _runner_for_startup_recovery(client_runner)
    runner._run_startup_recovery(broker=object(), exit_eng=object())

    messages = _startup_recovery_messages(caplog)
    assert len(messages) == 1
    assert "status=success" in messages[0]
    assert "watchers_restored=2" in messages[0]
    assert "deferred_retries_restored=1" in messages[0]
    assert "duration_ms=" in messages[0]


def test_pr_does_not_touch_trade_decision_or_submit_surfaces():
    handoff_src = (REPO_ROOT / "ap" / "morning_handoff.py").read_text()
    scheduler_src = (REPO_ROOT / "ap" / "scripts" / "live_morning_jobs.py").read_text()
    combined = handoff_src + "\n" + scheduler_src
    assert "place_order(" not in combined
    assert "cancel_order(" not in combined
    assert "create_entry_order(" not in combined
    assert "submit_existing_entry" not in combined
