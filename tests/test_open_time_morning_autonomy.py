from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from ap import morning_jobs, preopen_readiness
from ap.scripts import live_morning_jobs


ET = ZoneInfo("America/New_York")
REPO_ROOT = Path(__file__).resolve().parents[1]


def _et(hour: int, minute: int, second: int = 0) -> datetime:
    # 2026-08-28 was a Friday, so these tests do not depend on weekend behavior.
    return datetime(2026, 8, 28, hour, minute, second, tzinfo=ET)


def _set_live_job_env(monkeypatch, job: str) -> None:
    monkeypatch.setenv("BOT_URL", "https://bot.example")
    monkeypatch.setenv("SIGNING_SECRET", "test-secret")
    monkeypatch.setenv("MORNING_JOB", job)
    monkeypatch.setenv("MORNING_JOB_EXECUTION_MODE", "live")
    monkeypatch.setenv("MORNING_JOB_CLIENT_ID", "live@example.com")
    monkeypatch.setenv("MORNING_JOB_LIVE_CLIENT", "live@example.com")
    monkeypatch.setenv("MORNING_JOB_PAPER_CLIENTS", "paper@example.com")
    monkeypatch.setenv("MORNING_JOB_FORCE_WINDOW", "1")


def test_readiness_and_overnight_due_begin_at_market_open(monkeypatch):
    monkeypatch.setattr(preopen_readiness, "_nyse_is_trading_day", lambda _dt: True)

    assert preopen_readiness._readiness_enforcement_active(_et(9, 29, 59)) is False
    assert preopen_readiness._overnight_reeval_due(_et(9, 29, 59)) is False

    assert preopen_readiness._readiness_enforcement_active(_et(9, 30, 0)) is True
    assert preopen_readiness._overnight_reeval_due(_et(9, 30, 0)) is True


def test_authoritative_overnight_scheduler_target_is_market_open():
    assert morning_jobs.JOB_TARGET_MINUTE[morning_jobs.OVERNIGHT_REEVAL_BATCH_JOB] == (9, 30)
    assert not morning_jobs.should_run_now(
        morning_jobs.OVERNIGHT_REEVAL_BATCH_JOB,
        now=_et(9, 29),
        tolerance_minutes=0,
    )
    assert morning_jobs.should_run_now(
        morning_jobs.OVERNIGHT_REEVAL_BATCH_JOB,
        now=_et(9, 30),
        tolerance_minutes=0,
    )


def test_live_recovery_reuses_canonical_overnight_evaluator():
    calls = morning_jobs.build_job_calls(
        morning_jobs.MORNING_RECOVERY_JOB,
        live_client="live@example.com",
        paper_clients=["paper@example.com"],
    )

    live_calls = [call for call in calls if call.execution_mode == "live"]
    assert len(live_calls) == 1

    call = live_calls[0]
    assert call.job_name == "morning_recovery_live_overnight_reeval"
    assert call.endpoint == morning_jobs.OVERNIGHT_REEVAL_ENDPOINT
    assert call.payload["clients"] == ["live@example.com"]
    assert call.payload["force"] is True
    assert call.client_scope == "live@example.com"
    assert all(item.execution_mode != "mixed" for item in calls)
    assert not any(
        item.execution_mode == "live"
        and item.endpoint == morning_jobs.RELEASE_AFTER_HOURS_DEFERRED_ENDPOINT
        for item in calls
    )


def test_paper_recovery_remains_paper_only():
    calls = morning_jobs.build_job_calls(
        morning_jobs.MORNING_RECOVERY_JOB,
        live_client="live@example.com",
        paper_clients=["paper@example.com"],
    )

    paper_calls = [call for call in calls if call.execution_mode == "paper"]
    assert [call.endpoint for call in paper_calls] == [
        morning_jobs.RELEASE_AFTER_HOURS_DEFERRED_ENDPOINT,
        morning_jobs.PAPER_RESTART_GUARD_ENDPOINT,
    ]
    assert all(call.payload["clients"] == ["paper@example.com"] for call in paper_calls)


def test_live_recovery_main_executes_exactly_one_canonical_call(monkeypatch):
    _set_live_job_env(monkeypatch, morning_jobs.MORNING_RECOVERY_JOB)
    recorded = []

    def _fake_call_admin_endpoint(**kwargs):
        recorded.append(kwargs)
        return {
            "ok": True,
            "status_code": 200,
            "elapsed_seconds": 0.01,
            "body": {"ok": True},
            "job_window_key": "test",
        }

    monkeypatch.setattr(live_morning_jobs, "call_admin_endpoint", _fake_call_admin_endpoint)

    assert live_morning_jobs.main() == 0
    assert len(recorded) == 1
    assert recorded[0]["endpoint"] == morning_jobs.OVERNIGHT_REEVAL_ENDPOINT
    assert recorded[0]["execution_mode"] == "live"
    assert recorded[0]["client_scope"] == "live@example.com"
    assert recorded[0]["payload"]["clients"] == ["live@example.com"]
    assert recorded[0]["payload"]["force"] is True


def test_empty_live_scope_fails_closed_without_endpoint_call(monkeypatch, capsys):
    _set_live_job_env(monkeypatch, morning_jobs.MORNING_RECOVERY_JOB)

    paper_only = morning_jobs.MorningJobCall(
        job_name="paper-only",
        endpoint=morning_jobs.RELEASE_AFTER_HOURS_DEFERRED_ENDPOINT,
        payload={"clients": ["paper@example.com"]},
        client_scope="paper@example.com",
        execution_mode="paper",
    )
    monkeypatch.setattr(
        live_morning_jobs,
        "build_job_calls",
        lambda *args, **kwargs: [paper_only],
    )

    endpoint_calls = []
    monkeypatch.setattr(
        live_morning_jobs,
        "call_admin_endpoint",
        lambda **kwargs: endpoint_calls.append(kwargs) or {"ok": True},
    )

    assert live_morning_jobs.main() == 2
    assert endpoint_calls == []

    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert summary["ok"] is False
    assert summary["executed"] == 0
    assert summary["failures"] == 1
    assert summary["execution_mode"] == "live"
    assert summary["error"] == (
        "no_calls_resolved_for_expected_scope:job=morning_recovery:mode=live"
    )


def test_scheduler_files_use_open_time_and_no_blind_primary_handoff():
    render = (REPO_ROOT / "render.yaml").read_text()
    live_backup = (REPO_ROOT / ".github" / "workflows" / "overnight-reeval.yml").read_text()
    paper = (REPO_ROOT / ".github" / "workflows" / "paper-morning-jobs.yml").read_text()

    assert "30 13 * * 1-5" in render
    assert "30 14 * * 1-5" in render
    assert "ap-morning-handoff-primary" not in render
    assert "MORNING_JOB, value: morning_handoff_primary" not in render
    assert "MORNING_JOB, value: morning_handoff_backup" in render
    assert "MORNING_JOB, value: morning_recovery" in render

    assert "40 13 * * 1-5" in live_backup
    assert "40 14 * * 1-5" in live_backup
    assert "JOB=\"overnight_reeval_batch\"" in live_backup
    assert "MORNING_JOB_EXECUTION_MODE: live" in live_backup

    assert "30 13 * * 1-5" in paper
    assert "32 13 * * 1-5" in paper
    assert "36 13 * * 1-5" in paper
    assert "18 13 * * 1-5" not in paper
    assert "25 13 * * 1-5" not in paper
    assert "31 13 * * 1-5" not in paper


def test_scheduler_layer_has_no_direct_broker_submit_or_cancel():
    scheduler_sources = "\n".join(
        [
            (REPO_ROOT / "ap" / "morning_jobs.py").read_text(),
            (REPO_ROOT / "ap" / "scripts" / "live_morning_jobs.py").read_text(),
        ]
    )
    assert "submit_order(" not in scheduler_sources
    assert "cancel_order(" not in scheduler_sources
    assert "place_order(" not in scheduler_sources
