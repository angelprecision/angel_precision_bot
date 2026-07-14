from __future__ import annotations

from pathlib import Path

from ap import morning_handoff


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_startup_source_calls_only_unified_morning_handoff_once():
    src = (REPO_ROOT / "client_runner.py").read_text()
    run_idx = src.find("self._run_startup_morning_handoff()")
    assert run_idx != -1
    assert "self._run_morning_handoff_audit_startup()" not in src
    assert 'stage="startup"' in src


def test_post_overnight_source_calls_handoff_after_result():
    src = (REPO_ROOT / "client_runner.py").read_text()
    assert "self._run_post_overnight_morning_handoff(result)" in src
    assert 'stage="post_overnight_reeval"' in src
    assert "if not isinstance(overnight_result, dict):" in src
    assert "if armed <= 0:" not in src



def test_legacy_startup_helper_is_wrapper_only():
    src = (REPO_ROOT / "client_runner.py").read_text()
    start = src.find("def _run_morning_handoff_audit_startup")
    end = src.find("def _run_startup_recovery", start)
    block = src[start:end]
    assert "self._run_startup_morning_handoff()" in block
    assert "ap_morning_handoff_audit" not in block


def test_render_primary_jobs_are_explicitly_live_scoped():
    src = (REPO_ROOT / "render.yaml").read_text()
    assert src.count("MORNING_JOB_EXECUTION_MODE, value: live") == 8
    assert src.count("MORNING_JOB_CLIENT_ID, value: jasoncosby1@gmail.com") == 8

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


def test_morning_handoff_audit_loads_retry_eligible_rows():
    src = (REPO_ROOT / "ap_morning_handoff_audit.py").read_text()
    assert '"RETRY_ELIGIBLE"' in src or "'RETRY_ELIGIBLE'" in src
    assert "PENDING_TRIGGER', 'WATCHING', 'CREATED', 'RETRY_ELIGIBLE" in src


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
    monkeypatch.setattr(morning_handoff, "_has_unowned_pending_trigger_orders", lambda *args, **kwargs: False)
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


def test_startup_same_day_restart_does_not_skip_when_watcher_ownership_missing(monkeypatch):
    monkeypatch.setattr(
        morning_handoff,
        "_load_handoff_run_lock",
        lambda **kwargs: {"status": "success", "last_success_at": "2026-06-22T09:25:00-04:00"},
    )
    monkeypatch.setattr(morning_handoff, "_upsert_handoff_run_lock", lambda **kwargs: None)
    monkeypatch.setattr(
        morning_handoff,
        "_count_state",
        lambda client_id: {"watching_rows": 0, "new_rows": 0, "pending_trigger_rows": 1},
    )
    monkeypatch.setattr(morning_handoff, "_has_unowned_pending_trigger_orders", lambda *args, **kwargs: True)

    enqueue_calls = []
    monkeypatch.setattr(
        morning_handoff,
        "enqueue_watching_signals_to_trade_queue",
        lambda **kwargs: enqueue_calls.append(kwargs) or {
            "errors": [],
            "inserted": [],
            "skipped_duplicate": [{"signal_id": "sig-1"}],
            "rejected": [],
            "signals_found": 1,
        },
    )

    reseed_calls = []

    class _Recovery:
        def __init__(self, **kwargs):
            pass

        def _reseed_watchers(self, result):
            reseed_calls.append(True)
            result["watchers_requeued"] = 1

    import sys

    monkeypatch.setitem(sys.modules, "ap_recovery", type("_M", (), {"APStartupRecovery": _Recovery}))

    class _Core:
        broker = object()
        exit_eng = object()
        entry_watcher = type("_Watcher", (), {"has_order": lambda self, local_order_id: False})()

    class _Runner:
        order_state_machine = object()
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
    assert result["ok"] is True
    assert not result.get("skipped")
    assert enqueue_calls == [{
        "target_client_id": "paper@example.com",
        "execution_mode": "paper",
        "trading_date": result["trading_date"],
        "dry_run": False,
        "now": None,
    }]
    assert reseed_calls == [True]


def test_paper_startup_success_lock_without_orders_still_enqueues_and_reseeds(monkeypatch):
    monkeypatch.setattr(
        morning_handoff,
        "_load_handoff_run_lock",
        lambda **kwargs: {
            "status": "success",
            "last_success_at": "2026-06-22T09:25:00-04:00",
            "details": {"enqueue_result": {}},
        },
    )
    monkeypatch.setattr(morning_handoff, "_upsert_handoff_run_lock", lambda **kwargs: None)
    monkeypatch.setattr(
        morning_handoff,
        "_count_state",
        lambda client_id: {"watching_rows": 0, "new_rows": 0, "pending_trigger_rows": 0},
    )
    monkeypatch.setattr(
        morning_handoff,
        "_has_unowned_pending_trigger_orders",
        lambda *args, **kwargs: False,
    )

    enqueue_calls = []
    monkeypatch.setattr(
        morning_handoff,
        "enqueue_watching_signals_to_trade_queue",
        lambda **kwargs: enqueue_calls.append(kwargs) or {
            "errors": [],
            "inserted": [{"signal_id": "new-sig"}],
            "skipped_duplicate": [],
            "rejected": [],
            "signals_found": 1,
        },
    )

    reseed_calls = []

    class _Recovery:
        def __init__(self, **kwargs):
            pass

        def _reseed_watchers(self, result):
            reseed_calls.append(True)
            result["watchers_requeued"] = 1

    import sys

    monkeypatch.setitem(sys.modules, "ap_recovery", type("_M", (), {"APStartupRecovery": _Recovery}))

    class _Core:
        broker = object()
        exit_eng = object()
        entry_watcher = object()

    class _Runner:
        order_state_machine = object()
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

    assert result["ok"] is True
    assert not result.get("skipped")
    assert len(enqueue_calls) == 1
    assert reseed_calls == [True]


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
