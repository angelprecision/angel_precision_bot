from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_scheduler_entrypoint_emits_autonomy_lifecycle_markers():
    script_src = (REPO_ROOT / "ap" / "scripts" / "live_morning_jobs.py").read_text()
    assert "AUTONOMY_JOB_REGISTERED" in script_src
    assert "OVERNIGHT_JOB_STARTED" in script_src
    assert "MORNING_REEVAL_STARTED" in script_src
    assert "commit_sha=%s pod_id=%s client_count=%s execution_mode=%s" in script_src


def test_runtime_startup_logs_recovery_and_handoff_markers():
    runner_src = (REPO_ROOT / "client_runner.py").read_text()
    handoff_src = (REPO_ROOT / "ap" / "morning_handoff.py").read_text()
    assert "STARTUP_RECOVERY_COMPLETE" in runner_src
    assert "CLIENT_HANDOFF_STARTED" in handoff_src
    assert "CLIENT_HANDOFF_COMPLETE" in handoff_src
    assert "deferred_retries_restored" in handoff_src


def test_startup_lock_cannot_skip_runtime_ownership_recovery():
    handoff_src = (REPO_ROOT / "ap" / "morning_handoff.py").read_text()
    assert "Startup is process ownership, not only a daily job event" in handoff_src
    assert "can_skip_existing = False" in handoff_src
    assert "_has_unowned_pending_trigger_orders(client_id, entry_watcher, now=now)" not in handoff_src


def test_pr_does_not_touch_trade_decision_or_submit_surfaces():
    handoff_src = (REPO_ROOT / "ap" / "morning_handoff.py").read_text()
    scheduler_src = (REPO_ROOT / "ap" / "scripts" / "live_morning_jobs.py").read_text()
    combined = handoff_src + "\n" + scheduler_src
    assert "place_order(" not in combined
    assert "cancel_order(" not in combined
    assert "create_entry_order(" not in combined
    assert "submit_existing_entry" not in combined
