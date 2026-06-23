from __future__ import annotations

from pathlib import Path

from ap import morning_handoff


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_startup_source_calls_morning_handoff_once():
    src = (REPO_ROOT / "client_runner.py").read_text()
    assert "self._run_startup_morning_handoff()" in src
    assert 'stage="startup"' in src


def test_post_overnight_source_calls_handoff_after_result():
    src = (REPO_ROOT / "client_runner.py").read_text()
    assert "self._run_post_overnight_morning_handoff(result)" in src
    assert 'stage="post_overnight_reeval"' in src
    assert "if armed <= 0:" in src


def test_startup_recovery_disables_internal_watcher_reseed():
    src = (REPO_ROOT / "client_runner.py").read_text()
    assert "recovery.run(include_watcher_reseed=False)" in src


def test_health_surfaces_morning_handoff_status():
    app_src = (REPO_ROOT / "app.py").read_text()
    health_src = (REPO_ROOT / "ap_health_endpoints.py").read_text()
    assert '"morning_handoff": morning_handoff' in app_src
    assert '"morning_handoff":    get_morning_handoff_health()' in health_src


def test_manual_endpoint_still_exists_and_uses_shared_helper():
    src = (REPO_ROOT / "app.py").read_text()
    assert '@app.post("/admin/morning_handoff_audit")' in src
    assert "run_morning_handoff_audit(" in src
    assert 'stage="manual"' in src


def test_helper_source_has_stage_aware_locking():
    src = (REPO_ROOT / "ap" / "morning_handoff.py").read_text()
    assert "handoff_run_locks" in src
    assert 'stage not in {"startup", "post_overnight_reeval", "manual"}' in src
    assert '"reason": "handoff_already_succeeded_for_stage_today"' in src


def test_helper_source_has_missing_component_guardrails():
    src = (REPO_ROOT / "ap" / "morning_handoff.py").read_text()
    assert "order_state_machine_missing" in src
    assert "entry_watcher_missing" in src
    assert "master_control_missing" in src


def test_helper_source_has_no_direct_submit_cancel_or_order_creation():
    src = (REPO_ROOT / "ap" / "morning_handoff.py").read_text()
    assert "place_order(" not in src
    assert "cancel_order(" not in src
    assert "create_entry_order(" not in src
    assert "limit_price =" not in src


def test_post_overnight_endpoint_returns_handoff_results():
    src = (REPO_ROOT / "app.py").read_text()
    assert '"handoff_results": handoff_results' in src
    assert 'stage="post_overnight_reeval"' in src


def test_duplicate_same_stage_success_is_skipped(monkeypatch):
    monkeypatch.setattr(
        morning_handoff,
        "_load_handoff_run_lock",
        lambda **kwargs: {"status": "success", "last_success_at": "2026-06-22T09:25:00-04:00"},
    )
    result = morning_handoff.run_morning_handoff_audit(
        client_id="jason@example.com",
        execution_mode="live",
        stage="startup",
        dry_run=False,
        runner=object(),
    )
    assert result["ok"] is True
    assert result["skipped"] is True
    assert result["reason"] == "handoff_already_succeeded_for_stage_today"


def test_missing_osm_is_safe_and_non_crashing(monkeypatch):
    writes = []

    monkeypatch.setattr(morning_handoff, "_load_handoff_run_lock", lambda **kwargs: None)
    monkeypatch.setattr(morning_handoff, "_upsert_handoff_run_lock", lambda **kwargs: writes.append(kwargs))
    monkeypatch.setattr(
        morning_handoff,
        "_count_state",
        lambda client_id: {"watching_rows": 0, "new_rows": 0, "pending_trigger_rows": 0},
    )

    class _Core:
        broker = object()
        exit_eng = object()
        entry_watcher = object()

    class _Runner:
        order_state_machine = None
        position_manager = object()
        master_control = object()
        core = _Core()

    result = morning_handoff.run_morning_handoff_audit(
        client_id="paper@example.com",
        execution_mode="paper",
        stage="startup",
        dry_run=False,
        runner=_Runner(),
    )
    assert result["ok"] is False
    assert "order_state_machine_missing" in result["warnings"]
    assert writes[-1]["status"] == "failed"
