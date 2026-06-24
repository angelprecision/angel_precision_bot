from __future__ import annotations

import hashlib
import hmac
import io
import json
import urllib.error
from datetime import datetime
from pathlib import Path
import os

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


def test_workflow_is_backup_only_and_uses_module_invocation():
    workflow = (REPO_ROOT / ".github" / "workflows" / "overnight-reeval.yml").read_text()
    assert "50 13 * * 1-5" in workflow
    assert "50 14 * * 1-5" in workflow
    assert "18 13 * * 1-5" not in workflow
    assert "26 13 * * 1-5" not in workflow
    assert "python3 -m ap.scripts.live_morning_jobs" in workflow
    assert "python3 ap/scripts/live_morning_jobs.py" not in workflow


def test_render_blueprint_defines_primary_render_cron_jobs():
    blueprint = (REPO_ROOT / "render.yaml").read_text()
    assert blueprint.count("type: cron") == 8
    for expr in (
        "18 13 * * 1-5",
        "18 14 * * 1-5",
        "26 13 * * 1-5",
        "26 14 * * 1-5",
        "32 13 * * 1-5",
        "32 14 * * 1-5",
        "36 13 * * 1-5",
        "36 14 * * 1-5",
    ):
        assert expr in blueprint
    assert "startCommand: \"python -m ap.scripts.live_morning_jobs\"" in blueprint


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
    assert '"18 13 * * 1-5"|"18 14 * * 1-5") JOB="overnight_reeval_batch"' in workflow
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
    assert "APStartupRecovery" not in src


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
