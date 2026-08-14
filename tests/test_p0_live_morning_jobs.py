from __future__ import annotations

import hashlib
import hmac
import io
import json
import urllib.error
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
import os
import sys

import pytest

from ap.morning_jobs import (
    DEFAULT_LIVE_CLIENT,
    DEFAULT_PAPER_CLIENTS,
    MORNING_HANDOFF_BACKUP_JOB,
    MORNING_HANDOFF_PRIMARY_JOB,
    MORNING_RECOVERY_JOB,
    OVERNIGHT_REEVAL_BATCH_JOB,
    build_hmac_headers,
    build_job_calls,
    build_job_window_key,
    build_morning_handoff_payload,
    build_overnight_reeval_payload,
    call_admin_endpoint,
    select_runner_items,
    should_run_now,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


_LOADED_APP_MODULE = None


def _load_flask_app():
    old_env = os.environ.get("APP_ENV")
    old_db = os.environ.get("DATABASE_URL")
    old_fill = os.environ.get("ALLOW_LEGACY_FILL_MONITOR")
    os.environ["APP_ENV"] = "dev"
    os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")
    os.environ["ALLOW_LEGACY_FILL_MONITOR"] = "1"

    psycopg2_mod = MagicMock()
    psycopg2_mod.errors = SimpleNamespace()
    psycopg2_extras_mod = MagicMock()
    psycopg2_pool_mod = MagicMock()
    supabase_mod = MagicMock()
    cryptography_mod = MagicMock()
    fernet_mod = MagicMock()
    fernet_mod.Fernet = MagicMock()
    supabase_mod.create_client = MagicMock()
    supabase_mod.Client = MagicMock()

    with patch.dict(sys.modules, {
        "psycopg2": psycopg2_mod,
        "psycopg2.extras": psycopg2_extras_mod,
        "psycopg2.pool": psycopg2_pool_mod,
        "supabase": supabase_mod,
        "cryptography": cryptography_mod,
        "cryptography.fernet": fernet_mod,
    }):
        try:
            import app as app_mod
            global _LOADED_APP_MODULE
            _LOADED_APP_MODULE = app_mod
            return app_mod.app
        finally:
            if old_env is None:
                os.environ.pop("APP_ENV", None)
            else:
                os.environ["APP_ENV"] = old_env
            if old_db is None:
                os.environ.pop("DATABASE_URL", None)
            else:
                os.environ["DATABASE_URL"] = old_db
            if old_fill is None:
                os.environ.pop("ALLOW_LEGACY_FILL_MONITOR", None)
            else:
                os.environ["ALLOW_LEGACY_FILL_MONITOR"] = old_fill


@pytest.fixture(scope="module")
def morning_handoff_client():
    flask_app = _load_flask_app()
    flask_app.testing = True
    client = flask_app.test_client()
    client._app_mod = _LOADED_APP_MODULE  # type: ignore[attr-defined]
    return client


def _fake_client_runner_module(active_runners=None):
    lock = type("_Lock", (), {"__enter__": lambda self: self, "__exit__": lambda self, *a: False})()
    return SimpleNamespace(
        _active_runners=active_runners or {},
        _registry_lock=lock,
    )


def _fake_runner(mode="live"):
    runner = MagicMock()
    runner.mode = mode
    runner.master_control = SimpleNamespace(mode=mode)
    return runner


def test_hmac_signature_matches_shell_format():
    secret = "031b4935137f1999d176700274b73bd0"
    ts = "1718591880"
    payload = build_overnight_reeval_payload(clients=[DEFAULT_LIVE_CLIENT])
    body = json.dumps(payload).encode("utf-8")
    expected = hmac.new(
        secret.encode("utf-8"),
        f"{ts}.{body.decode('utf-8')}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    headers, raw = build_hmac_headers(secret=secret, payload=payload, timestamp=ts)

    assert raw == body
    assert headers["X-AP-Timestamp"] == ts
    assert headers["X-AP-Signature"] == expected


def test_scheduler_payload_for_overnight_reeval_handles_live_and_paper():
    live_payload = build_overnight_reeval_payload(clients=[DEFAULT_LIVE_CLIENT])
    paper_payload = build_overnight_reeval_payload(clients=DEFAULT_PAPER_CLIENTS)
    assert live_payload["clients"] == [DEFAULT_LIVE_CLIENT]
    assert live_payload["max_clients"] == 1
    assert paper_payload["clients"] == list(DEFAULT_PAPER_CLIENTS)
    assert paper_payload["max_clients"] == len(DEFAULT_PAPER_CLIENTS)


def test_existing_clients_mode_payload_still_works():
    payload = build_morning_handoff_payload(
        clients=DEFAULT_PAPER_CLIENTS,
        mode="paper",
        dry_run=False,
    )
    assert payload == {
        "dry_run": False,
        "clients": list(DEFAULT_PAPER_CLIENTS),
        "mode": "paper",
    }


def test_new_client_id_execution_mode_payload_still_works():
    payload = build_morning_handoff_payload(
        client_id=DEFAULT_LIVE_CLIENT,
        execution_mode="live",
        dry_run=False,
    )
    assert payload == {
        "dry_run": False,
        "client_id": DEFAULT_LIVE_CLIENT,
        "mode": "live",
        "execution_mode": "live",
    }


def test_paper_mode_never_defaults_to_live():
    calls = build_job_calls(MORNING_HANDOFF_PRIMARY_JOB)
    paper_call = next(call for call in calls if call.execution_mode == "paper")
    assert paper_call.payload["mode"] == "paper"
    assert paper_call.payload["clients"] == list(DEFAULT_PAPER_CLIENTS)


def test_same_window_uses_stable_job_window_key():
    now = datetime(2026, 6, 19, 9, 18)
    key1 = build_job_window_key(
        OVERNIGHT_REEVAL_BATCH_JOB,
        client_scope=DEFAULT_LIVE_CLIENT,
        execution_mode="live",
        now=now,
    )
    key2 = build_job_window_key(
        OVERNIGHT_REEVAL_BATCH_JOB,
        client_scope=DEFAULT_LIVE_CLIENT,
        execution_mode="live",
        now=now,
    )
    assert key1 == key2


def test_should_run_now_uses_wide_delay_tolerance_but_skips_wrong_season():
    assert should_run_now(
        OVERNIGHT_REEVAL_BATCH_JOB,
        now=datetime(2026, 6, 19, 9, 52),
        tolerance_minutes=45,
    )
    assert not should_run_now(
        OVERNIGHT_REEVAL_BATCH_JOB,
        now=datetime(2026, 6, 19, 10, 18),
        tolerance_minutes=45,
    )


def test_select_runner_items_honors_client_filter_and_max_clients():
    runners = {
        "tradefluencehq@gmail.com": object(),
        "jasoncosby1@gmail.com": object(),
        "jose.vasquez4011@gmail.com": object(),
    }
    selected = select_runner_items(
        runners,
        requested_clients=["jasoncosby1@gmail.com", "jose.vasquez4011@gmail.com"],
        max_clients=1,
    )
    assert [email for email, _runner in selected] == ["jasoncosby1@gmail.com"]


def test_non_200_endpoint_response_returns_failure_with_body():
    payload = build_morning_handoff_payload(client_id=DEFAULT_LIVE_CLIENT, execution_mode="live")

    def _raise_http_error(_req, timeout=0):
        raise urllib.error.HTTPError(
            url="https://example.com/admin/morning_handoff_audit",
            code=500,
            msg="boom",
            hdrs=None,
            fp=io.BytesIO(b'{"ok": false, "error": "boom"}'),
        )

    result = call_admin_endpoint(
        bot_url="https://example.com",
        endpoint="/admin/morning_handoff_audit",
        secret="secret",
        payload=payload,
        job_name=MORNING_HANDOFF_PRIMARY_JOB,
        client_scope=DEFAULT_LIVE_CLIENT,
        execution_mode="live",
        urlopen=_raise_http_error,
    )
    assert result["ok"] is False
    assert result["status_code"] == 500
    assert "boom" in str(result["body"])


def test_network_exception_returns_failure_without_crashing():
    payload = build_overnight_reeval_payload(clients=[DEFAULT_LIVE_CLIENT])

    def _raise_network_error(_req, timeout=0):
        raise OSError("network down")

    result = call_admin_endpoint(
        bot_url="https://example.com",
        endpoint="/admin/overnight_reeval",
        secret="secret",
        payload=payload,
        job_name=OVERNIGHT_REEVAL_BATCH_JOB,
        client_scope=DEFAULT_LIVE_CLIENT,
        execution_mode="live",
        urlopen=_raise_network_error,
    )
    assert result["ok"] is False
    assert result["status_code"] is None
    assert "network down" in str(result["body"])


def test_workflow_is_backup_only_and_uses_standalone_runner():
    workflow = (REPO_ROOT / ".github" / "workflows" / "overnight-reeval.yml").read_text()
    assert "50 13 * * 1-5" in workflow
    assert "50 14 * * 1-5" in workflow
    assert "18 13 * * 1-5" not in workflow
    assert "26 13 * * 1-5" not in workflow
    assert "python3 ap/scripts/live_morning_jobs.py" in workflow
    assert "python3 -m ap.scripts.live_morning_jobs" not in workflow


def test_render_blueprint_defines_primary_render_cron_jobs():
    blueprint = (REPO_ROOT / "render.yaml").read_text()
    assert blueprint.count("type: cron") == 8
    for expr in (
        "30 13 * * 1-5",
        "30 14 * * 1-5",
        "32 13 * * 1-5",
        "32 14 * * 1-5",
        "36 13 * * 1-5",
        "36 14 * * 1-5",
        "40 13 * * 1-5",
        "40 14 * * 1-5",
    ):
        assert expr in blueprint
    assert "startCommand: \"python ap/scripts/live_morning_jobs.py\"" in blueprint
    assert "startCommand: \"python -m ap.scripts.live_morning_jobs\"" not in blueprint



def test_render_primary_crons_are_live_scoped_to_jason_only():
    blueprint = (REPO_ROOT / "render.yaml").read_text()
    assert blueprint.count("MORNING_JOB_EXECUTION_MODE, value: live") == 8
    assert blueprint.count("MORNING_JOB_CLIENT_ID, value: jasoncosby1@gmail.com") == 8


def test_script_accepts_render_and_backup_env_aliases():
    script_src = (REPO_ROOT / "ap" / "scripts" / "live_morning_jobs.py").read_text()
    assert 'os.getenv("BOT_URL", os.getenv("AP_BOT_URL", ""))' in script_src
    assert 'os.getenv("SIGNING_SECRET", os.getenv("AP_SIGNING_SECRET", ""))' in script_src
    assert "if __package__ in {None, \"\"}:" in script_src
    assert 'os.getenv("MORNING_JOB_EXECUTION_MODE", "")' in script_src
    assert 'os.getenv("MORNING_JOB_CLIENT_ID", "")' in script_src


def test_recovery_release_is_paper_only_by_default():
    import os

    old = os.environ.pop("ENABLE_LIVE_AUTO_RELEASE_AFTER_OPEN", None)
    try:
        calls = build_job_calls(MORNING_RECOVERY_JOB)
        release_call = calls[0]
        assert DEFAULT_LIVE_CLIENT not in release_call.payload["clients"]
        assert release_call.payload["clients"] == list(DEFAULT_PAPER_CLIENTS)
        assert release_call.execution_mode == "paper"
    finally:
        if old is not None:
            os.environ["ENABLE_LIVE_AUTO_RELEASE_AFTER_OPEN"] = old


def test_recovery_release_includes_live_only_when_flag_set():
    import os

    old = os.environ.get("ENABLE_LIVE_AUTO_RELEASE_AFTER_OPEN")
    os.environ["ENABLE_LIVE_AUTO_RELEASE_AFTER_OPEN"] = "true"
    try:
        calls = build_job_calls(MORNING_RECOVERY_JOB)
        release_call = calls[0]
        assert DEFAULT_LIVE_CLIENT in release_call.payload["clients"]
        assert release_call.execution_mode == "mixed"
    finally:
        if old is None:
            os.environ.pop("ENABLE_LIVE_AUTO_RELEASE_AFTER_OPEN", None)
        else:
            os.environ["ENABLE_LIVE_AUTO_RELEASE_AFTER_OPEN"] = old


def test_handoff_backup_plan_builds_both_modes():
    calls = build_job_calls(MORNING_HANDOFF_BACKUP_JOB)
    assert [call.execution_mode for call in calls] == ["live", "paper"]


def test_paper_workflow_uses_batch_job_name_and_paper_only_env():
    workflow = (REPO_ROOT / ".github" / "workflows" / "paper-morning-jobs.yml").read_text()
    assert 'default: "overnight_reeval_batch"' in workflow
    assert '          - overnight_reeval_batch' in workflow
    assert '"30 13 * * 1-5"|"30 14 * * 1-5") JOB="overnight_reeval_batch"' in workflow
    assert "BOT_URL: ${{ secrets.PAPER_BOT_URL }}" in workflow
    assert "MORNING_JOB_EXECUTION_MODE: paper" in workflow
    assert "MORNING_JOB_PAPER_CLIENTS: ${{ matrix.client_id }}" in workflow
    assert "MORNING_JOB_WINDOW_TOLERANCE_MINUTES: \"20\"" in workflow
    assert "jasoncosby1@gmail.com" not in workflow


def test_script_can_filter_to_paper_only_matrix_client():
    old_mode = os.environ.get("MORNING_JOB_EXECUTION_MODE")
    old_client = os.environ.get("MORNING_JOB_CLIENT_ID")
    old_paper = os.environ.get("MORNING_JOB_PAPER_CLIENTS")
    try:
        os.environ["MORNING_JOB_EXECUTION_MODE"] = "paper"
        os.environ["MORNING_JOB_CLIENT_ID"] = "jose.vasquez4011@gmail.com"
        os.environ["MORNING_JOB_PAPER_CLIENTS"] = "jose.vasquez4011@gmail.com"
        from ap.scripts.live_morning_jobs import _resolve_scoped_clients

        live_client, paper_clients, mode_filter = _resolve_scoped_clients()
        calls = [
            call for call in build_job_calls(
                OVERNIGHT_REEVAL_BATCH_JOB,
                live_client=live_client,
                paper_clients=paper_clients,
            )
            if call.execution_mode == mode_filter
        ]
        assert mode_filter == "paper"
        assert len(calls) == 1
        assert calls[0].execution_mode == "paper"
        assert calls[0].payload["clients"] == ["jose.vasquez4011@gmail.com"]
        assert DEFAULT_LIVE_CLIENT not in calls[0].payload["clients"]
    finally:
        if old_mode is None:
            os.environ.pop("MORNING_JOB_EXECUTION_MODE", None)
        else:
            os.environ["MORNING_JOB_EXECUTION_MODE"] = old_mode
        if old_client is None:
            os.environ.pop("MORNING_JOB_CLIENT_ID", None)
        else:
            os.environ["MORNING_JOB_CLIENT_ID"] = old_client
        if old_paper is None:
            os.environ.pop("MORNING_JOB_PAPER_CLIENTS", None)
        else:
            os.environ["MORNING_JOB_PAPER_CLIENTS"] = old_paper


def test_app_source_restores_existing_admin_endpoints_and_handoff_contract():
    src = (REPO_ROOT / "app.py").read_text()
    assert '@app.post("/admin/operator/manual-rescue-restart-guard")' in src
    assert '@app.get("/admin/overnight_reeval/status")' in src
    assert '@app.post("/admin/release_after_hours_deferred")' in src
    assert '@app.get("/admin/morning_handoff_audit")' in src
    assert '@app.post("/admin/morning_handoff_audit")' in src
    assert '@app.post("/admin/paper_rescue_restart_guard")' in src
    assert '@app.route("/admin/preopen_readiness", methods=["GET", "POST"])' in src
    assert 'body.get("clients")' in src
    assert 'body.get("client_id")' in src
    assert 'body.get("execution_mode")' in src
    handoff_idx = src.find('def admin_morning_handoff_audit_post')
    handoff_region = src[handoff_idx: handoff_idx + 5000]
    assert "APStartupRecovery" not in handoff_region


def test_existing_async_overnight_status_contract_still_exists():
    src = (REPO_ROOT / "app.py").read_text()
    assert "_OVERNIGHT_REEVAL_JOBS" in src
    assert 'body.get("async_background"' in src
    assert '"status_endpoint"' in src
    assert '@app.get("/admin/overnight_reeval/status")' in src


def test_admin_morning_handoff_source_uses_shared_modules_not_legacy_module():
    src = (REPO_ROOT / "app.py").read_text()
    assert "from ap.morning_handoff import run_morning_handoff_audit" in src
    assert "from ap.preopen_readiness import run_preopen_autonomous_readiness" in src
    assert "from ap_morning_handoff_audit import run_morning_handoff_audit" not in src


def test_admin_morning_handoff_post_conflicting_mode_and_execution_mode_returns_400(morning_handoff_client):
    with patch.dict(sys.modules, {"client_runner": _fake_client_runner_module()}):
        resp = morning_handoff_client.post(
            "/admin/morning_handoff_audit",
            json={"client_id": "jason@example.com", "mode": "live", "execution_mode": "paper"},
        )
    assert resp.status_code == 400
    assert resp.get_json() == {
        "ok": False,
        "error": "mode_execution_mode_mismatch",
        "mode": "live",
        "execution_mode": "paper",
    }


def test_admin_morning_handoff_post_conflicting_mode_and_execution_mode_returns_400_reverse(morning_handoff_client):
    with patch.dict(sys.modules, {"client_runner": _fake_client_runner_module()}):
        resp = morning_handoff_client.post(
            "/admin/morning_handoff_audit",
            json={"client_id": "paper@example.com", "mode": "paper", "execution_mode": "live"},
        )
    assert resp.status_code == 400
    assert resp.get_json() == {
        "ok": False,
        "error": "mode_execution_mode_mismatch",
        "mode": "paper",
        "execution_mode": "live",
    }


def test_admin_morning_handoff_get_conflicting_mode_and_execution_mode_returns_400(morning_handoff_client):
    with patch.dict(sys.modules, {"client_runner": _fake_client_runner_module()}):
        resp = morning_handoff_client.get(
            "/admin/morning_handoff_audit?client_id=jason@example.com&mode=live&execution_mode=paper"
        )
    assert resp.status_code == 400
    assert resp.get_json() == {
        "ok": False,
        "error": "mode_execution_mode_mismatch",
        "mode": "live",
        "execution_mode": "paper",
    }


def test_admin_morning_handoff_get_rejects_runner_mode_mismatch(morning_handoff_client):
    active = {"jason@example.com": _fake_runner(mode="paper")}
    with patch.dict(sys.modules, {"client_runner": _fake_client_runner_module(active)}):
        resp = morning_handoff_client.get(
            "/admin/morning_handoff_audit?client_id=jason@example.com&execution_mode=live"
        )
    assert resp.status_code == 409
    assert resp.get_json() == {
        "ok": False,
        "error": "runner_mode_mismatch",
        "client_id": "jason@example.com",
        "requested_mode": "live",
        "runner_mode": "paper",
    }


def test_admin_morning_handoff_post_rejects_zero_matched_explicit_client(morning_handoff_client):
    active = {"jose@example.com": _fake_runner(mode="paper")}
    fake_lock_mod = SimpleNamespace(
        build_run_key=lambda **_: "rk-zero",
        try_acquire_run_lock=lambda *a, **k: {
            "acquired": True,
            "run_key": "rk-zero",
            "owner_token": "tok-zero",
            "reclaimed": False,
        },
        mark_run_lock_completed=MagicMock(),
        mark_run_lock_failed=MagicMock(),
    )
    with patch.dict(sys.modules, {
        "client_runner": _fake_client_runner_module(active),
        "ap_handoff_run_lock": fake_lock_mod,
    }):
        resp = morning_handoff_client.post(
            "/admin/morning_handoff_audit",
            json={"client_id": "jason@example.com", "execution_mode": "live", "dry_run": False},
        )
    body = resp.get_json()
    assert resp.status_code == 404
    assert body["ok"] is False
    assert body["error"] == "requested_clients_not_in_active_runners_for_mode"
    assert body["requested_clients"] == ["jason@example.com"]
    assert body["requested_mode"] == "live"
    fake_lock_mod.mark_run_lock_completed.assert_not_called()
    fake_lock_mod.mark_run_lock_failed.assert_called_once()


def test_admin_morning_handoff_post_rejects_conflicting_mode_and_execution_mode():
    src = (REPO_ROOT / "app.py").read_text()
    assert 'mode_raw = body.get("mode")' in src
    assert 'execution_mode_alias = str(body.get("execution_mode") or "").strip().lower()' in src
    assert '"error": "mode_execution_mode_mismatch"' in src
    assert '"mode": str(mode_raw).strip().lower()' in src
    assert '"execution_mode": execution_mode_alias' in src


def test_admin_morning_handoff_post_accepts_execution_mode_only_and_mode_only():
    src = (REPO_ROOT / "app.py").read_text()
    assert 'mode = str(mode_raw or execution_mode_alias or "live").lower().strip()' in src


def test_admin_morning_handoff_get_rejects_conflicting_mode_and_execution_mode():
    src = (REPO_ROOT / "app.py").read_text()
    assert 'mode_raw = request.args.get("mode")' in src
    assert 'execution_mode_alias = str(request.args.get("execution_mode") or "").strip().lower()' in src
    assert 'if mode_raw and execution_mode_alias and str(mode_raw).strip().lower() != execution_mode_alias:' in src
    assert '"mode": str(mode_raw).strip().lower()' in src
    assert 'mode = str(mode_raw or execution_mode_alias or "live").lower().strip()' in src


def test_admin_morning_handoff_get_accepts_execution_mode_only():
    src = (REPO_ROOT / "app.py").read_text()
    assert 'request.args.get("execution_mode")' in src
    assert 'mode = str(mode_raw or execution_mode_alias or "live").lower().strip()' in src


def test_admin_morning_handoff_returns_readiness_payload():
    src = (REPO_ROOT / "app.py").read_text()
    assert '"handoff": handoff' in src
    assert '"readiness": readiness' in src
    assert '"handoff_results": handoff_results' in src
    assert '"readiness_results": readiness_results' in src


def test_overnight_reeval_returns_readiness_results_in_sync_and_async_contracts():
    src = (REPO_ROOT / "app.py").read_text()
    assert '"readiness_results": {' in src
    assert '"skipped": "async_background_readiness_not_run"' in src
    assert '"readiness_results": readiness_results' in src


def test_no_code_path_calls_broker_submit_or_cancel_directly():
    script_src = (REPO_ROOT / "ap" / "scripts" / "live_morning_jobs.py").read_text()
    helper_src = (REPO_ROOT / "ap" / "morning_jobs.py").read_text()
    combined = script_src + "\n" + helper_src
    assert "place_order(" not in combined
    assert "cancel_order(" not in combined
    assert "submit_existing_entry" not in combined
