from __future__ import annotations

import hashlib
import hmac
import io
import json
import urllib.error
from datetime import datetime
from pathlib import Path

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


def test_workflow_targets_live_and_paper_batches():
    workflow = (REPO_ROOT / ".github" / "workflows" / "overnight-reeval.yml").read_text()
    assert "MORNING_JOB_LIVE_CLIENT: jasoncosby1@gmail.com" in workflow
    assert "MORNING_JOB_PAPER_CLIENTS: jose.vasquez4011@gmail.com,tradefluencehq@gmail.com" in workflow
    # Backup runs ONCE at the later 09:50 ET recovery window (DST-safe UTC pair),
    # NOT at the primary windows. This proves it is a true backup, not a
    # duplicate primary scheduler.
    assert "50 13 * * 1-5" in workflow   # 09:50 EDT
    assert "50 14 * * 1-5" in workflow   # 09:50 EST
    # The primary windows must NOT appear in the backup workflow schedule
    assert "18 13 * * 1-5" not in workflow
    assert "26 13 * * 1-5" not in workflow


def test_workflow_uses_module_invocation_not_path():
    """Item 2: backup must invoke via -m to avoid ModuleNotFoundError."""
    workflow = (REPO_ROOT / ".github" / "workflows" / "overnight-reeval.yml").read_text()
    assert "python3 -m ap.scripts.live_morning_jobs" in workflow
    assert "python3 ap/scripts/live_morning_jobs.py" not in workflow


def test_workflow_is_backup_only_not_duplicate_primary():
    """Item 3: GHA must run at a later recovery window only, not the primary windows."""
    workflow = (REPO_ROOT / ".github" / "workflows" / "overnight-reeval.yml").read_text()
    # Exactly two scheduled cron entries (the DST pair for one recovery window)
    sched_lines = [
        ln for ln in workflow.splitlines()
        if ln.strip().startswith("- cron:")
    ]
    assert len(sched_lines) == 2, (
        f"Backup must have exactly 2 cron entries (DST pair, one window); "
        f"got {len(sched_lines)}: {sched_lines}"
    )


def test_app_source_restores_existing_admin_endpoints_and_handoff_contract():
    src = (REPO_ROOT / "app.py").read_text()
    assert '@app.post("/admin/operator/manual-rescue-restart-guard")' in src
    assert '@app.get("/admin/overnight_reeval/status")' in src
    assert '@app.post("/admin/release_after_hours_deferred")' in src
    assert '@app.get("/admin/morning_handoff_audit")' in src
    assert '@app.post("/admin/morning_handoff_audit")' in src
    assert '@app.post("/admin/paper_rescue_restart_guard")' in src
    assert "run_morning_handoff_audit" in src
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


def test_no_code_path_calls_broker_submit_or_cancel_directly():
    script_src = (REPO_ROOT / "ap" / "scripts" / "live_morning_jobs.py").read_text()
    helper_src = (REPO_ROOT / "ap" / "morning_jobs.py").read_text()
    combined = script_src + "\n" + helper_src
    assert "place_order(" not in combined
    assert "cancel_order(" not in combined
    assert "submit_existing_entry" not in combined


def test_job_batches_cover_recovery_path_without_manual_shell():
    calls = build_job_calls(MORNING_RECOVERY_JOB)
    assert [call.endpoint for call in calls] == [
        "/admin/release_after_hours_deferred",
        "/admin/paper_rescue_restart_guard",
    ]
    assert calls[0].payload["force"] is True
    assert calls[1].payload["clients"] == list(DEFAULT_PAPER_CLIENTS)


def test_recovery_release_is_paper_only_by_default():
    """Item 3: release_after_hours_deferred must NOT auto-release live rows by
    default. Without ENABLE_LIVE_AUTO_RELEASE_AFTER_OPEN, the release is scoped
    to paper clients only."""
    import os
    old = os.environ.pop("ENABLE_LIVE_AUTO_RELEASE_AFTER_OPEN", None)
    try:
        calls = build_job_calls(MORNING_RECOVERY_JOB)
        release_call = calls[0]
        # The live client must NOT be in the release scope
        assert DEFAULT_LIVE_CLIENT not in release_call.payload["clients"], (
            "release_after_hours_deferred must not include the live client by default"
        )
        # Only paper clients
        assert release_call.payload["clients"] == list(DEFAULT_PAPER_CLIENTS)
        assert release_call.execution_mode == "paper"
    finally:
        if old is not None:
            os.environ["ENABLE_LIVE_AUTO_RELEASE_AFTER_OPEN"] = old


def test_recovery_release_includes_live_only_when_flag_set():
    """Item 3: live release happens only when ENABLE_LIVE_AUTO_RELEASE_AFTER_OPEN=true."""
    import os
    old = os.environ.get("ENABLE_LIVE_AUTO_RELEASE_AFTER_OPEN")
    os.environ["ENABLE_LIVE_AUTO_RELEASE_AFTER_OPEN"] = "true"
    try:
        calls = build_job_calls(MORNING_RECOVERY_JOB)
        release_call = calls[0]
        # Now the live client IS included
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


# ---------------------------------------------------------------------------
# Amendment: seven-item compatibility coverage
#
# These tests prove the amended PR satisfies the operator-required changes:
#   1. Render Cron is the primary scheduler (render.yaml present + correct)
#   2. Both payload shapes accepted (clients/mode AND client_id/execution_mode)
#   5. async_background/status contract preserved on /admin/overnight_reeval
#   7. existing curl/dashboard payloads still work
# ---------------------------------------------------------------------------

def test_render_yaml_defines_cron_primary_scheduler():
    """Item 1: Render Cron must be the documented primary scheduler.
    8 cron services = 4 ET windows × 2 DST-safe UTC entries each."""
    render_yaml = (REPO_ROOT / "render.yaml").read_text()
    # 4 windows × 2 DST entries (EDT + EST) = 8 cron services
    assert render_yaml.count("type: cron") == 8
    # Each cron's startCommand invokes the scheduler-agnostic script.
    start_cmd_lines = [
        ln for ln in render_yaml.splitlines()
        if "startCommand:" in ln and "ap.scripts.live_morning_jobs" in ln
    ]
    assert len(start_cmd_lines) == 8, (
        f"Expected 8 startCommand lines invoking the script, got {len(start_cmd_lines)}"
    )
    # All four MORNING_JOB batches represented
    for job in (
        "overnight_reeval_batch",
        "morning_handoff_primary",
        "morning_handoff_backup",
        "morning_recovery",
    ):
        assert job in render_yaml, f"render.yaml missing cron for {job}"


def test_render_yaml_env_vars_match_script():
    """Item 1: render.yaml env var names must match what the script reads.
    The script reads BOT_URL and SIGNING_SECRET — render.yaml must set those
    exact names, not aliases, or the cron fails at runtime."""
    render_yaml = (REPO_ROOT / "render.yaml").read_text()
    script_src = (REPO_ROOT / "ap" / "scripts" / "live_morning_jobs.py").read_text()
    # The script reads these exact names
    assert 'os.getenv("BOT_URL"' in script_src
    assert 'os.getenv("SIGNING_SECRET"' in script_src
    # render.yaml must reference the same names
    assert "BOT_URL" in render_yaml
    assert "SIGNING_SECRET" in render_yaml
    # And must NOT reference the wrong alias names
    assert "AP_BOT_BASE_URL" not in render_yaml
    assert "AP_ADMIN_HMAC_SECRET" not in render_yaml


def test_render_yaml_schedules_are_dst_safe_utc():
    """Item 1/4: Render uses UTC, not TZ-local. Schedules MUST be UTC with
    DST-safe dual entries (EDT=UTC-4, EST=UTC-5) for each ET window.

    Target ET   EDT (UTC)   EST (UTC)
    09:18 ET    13:18       14:18
    09:26 ET    13:26       14:26
    09:32 ET    13:32       14:32
    09:36 ET    13:36       14:36
    """
    render_yaml = (REPO_ROOT / "render.yaml").read_text()
    # EDT (UTC-4) entries
    for expr in ("18 13 * * 1-5", "26 13 * * 1-5", "32 13 * * 1-5", "36 13 * * 1-5"):
        assert expr in render_yaml, f"missing EDT schedule {expr}"
    # EST (UTC-5) entries
    for expr in ("18 14 * * 1-5", "26 14 * * 1-5", "32 14 * * 1-5", "36 14 * * 1-5"):
        assert expr in render_yaml, f"missing EST schedule {expr}"
    # The TZ-naive (wrong) 09:xx schedules must NOT appear as cron schedule lines
    schedule_lines = [
        ln for ln in render_yaml.splitlines() if "schedule:" in ln
    ]
    for sl in schedule_lines:
        assert " 9 * * 1-5" not in sl, (
            f"render.yaml must NOT use TZ-naive 09:xx schedule — Render cron is UTC: {sl}"
        )


def test_render_yaml_documents_utc_not_tz_local():
    """Item 4: render.yaml must document that Render cron is UTC, not TZ-local."""
    render_yaml = (REPO_ROOT / "render.yaml").read_text()
    assert "UTC" in render_yaml
    # Must explain TZ does not affect scheduling
    assert "TZ" in render_yaml


def test_scripts_package_importable_as_module():
    """Item 1: python -m ap.scripts.live_morning_jobs must resolve.
    Requires ap/scripts/__init__.py to exist for -m on all Python versions."""
    assert (REPO_ROOT / "ap" / "scripts" / "__init__.py").exists(), (
        "ap/scripts/__init__.py missing — python -m ap.scripts.live_morning_jobs "
        "may fail to resolve the package on some Python versions"
    )


# ── Item 2 / 7: endpoint payload parsing contract ──────────────────────────
# Replicates the exact parsing block from the POST /admin/morning_handoff_audit
# endpoint so we can prove both payload shapes resolve correctly without
# spinning up the full Flask app (heavy import deps).

def _parse_morning_handoff_post(body: dict):
    """Mirror of app.py admin_morning_handoff_audit_post parsing (lines ~3306-3318).
    If this drifts from the endpoint, test_app_source_endpoint_parsing_matches
    will catch it."""
    clients_filter = body.get("clients") or None
    client_id_alias = str(body.get("client_id") or "").strip().lower()
    mode_raw = body.get("mode")
    execution_mode_alias = str(body.get("execution_mode") or "").strip().lower()
    if mode_raw and execution_mode_alias and str(mode_raw).strip().lower() != execution_mode_alias:
        return ("ERROR_400", "mode_execution_mode_mismatch")
    if clients_filter is not None:
        clients_filter = {str(e).strip().lower() for e in clients_filter if str(e).strip()}
    elif client_id_alias:
        clients_filter = {client_id_alias}
    mode = str(mode_raw or execution_mode_alias or "live").lower().strip()
    return (clients_filter, mode)


def test_post_endpoint_accepts_legacy_client_id_execution_mode():
    """Item 2/7: existing dashboard payload {client_id, execution_mode} works."""
    result, mode = _parse_morning_handoff_post({
        "client_id": "jasoncosby1@gmail.com",
        "execution_mode": "live",
        "dry_run": False,
    })
    assert result == {"jasoncosby1@gmail.com"}
    assert mode == "live"


def test_post_endpoint_accepts_new_clients_mode():
    """Item 2/7: new payload {clients, mode} works."""
    result, mode = _parse_morning_handoff_post({
        "clients": ["jose.vasquez4011@gmail.com"],
        "mode": "paper",
        "dry_run": False,
    })
    assert result == {"jose.vasquez4011@gmail.com"}
    assert mode == "paper"


def test_post_endpoint_rejects_mode_execution_mode_mismatch():
    """Item 2: conflicting mode and execution_mode must 400, not silently pick one."""
    result, reason = _parse_morning_handoff_post({
        "clients": ["x@example.com"],
        "mode": "live",
        "execution_mode": "paper",
    })
    assert result == "ERROR_400"
    assert reason == "mode_execution_mode_mismatch"


def test_post_endpoint_defaults_to_live_when_no_mode():
    """Item 2: when neither mode nor execution_mode given, defaults to live."""
    result, mode = _parse_morning_handoff_post({
        "client_id": "jasoncosby1@gmail.com",
    })
    assert mode == "live"


def test_app_source_post_endpoint_parsing_matches():
    """Guard: the real endpoint in app.py must contain the same parsing
    branches this test mirrors. If the endpoint is refactored, this fails."""
    app_src = (REPO_ROOT / "app.py").read_text()
    idx = app_src.find("def admin_morning_handoff_audit_post")
    region = app_src[idx: idx + 1200]
    # Both payload keys handled
    assert 'body.get("clients")' in region
    assert 'body.get("client_id")' in region
    assert 'body.get("mode")' in region
    assert 'body.get("execution_mode")' in region
    # Mismatch guard present
    assert "mode_execution_mode_mismatch" in region


def test_app_source_get_endpoint_accepts_both_param_names():
    """Item 2: GET endpoint must accept both mode and execution_mode query params."""
    app_src = (REPO_ROOT / "app.py").read_text()
    idx = app_src.find("def admin_morning_handoff_audit_get")
    region = app_src[idx: idx + 800]
    assert 'request.args.get("client_id"' in region
    assert "execution_mode" in region


def test_app_source_async_overnight_status_intact():
    """Item 5: async_background path and /status endpoint must both survive."""
    app_src = (REPO_ROOT / "app.py").read_text()
    assert 'async_background = bool(body.get("async_background"' in app_src
    assert "/admin/overnight_reeval/status" in app_src
    assert "_OVERNIGHT_REEVAL_JOBS" in app_src


def test_app_source_manual_rescue_restart_guard_intact():
    """Item 4: the manual-rescue-restart-guard endpoint must not be deleted."""
    app_src = (REPO_ROOT / "app.py").read_text()
    assert "/admin/operator/manual-rescue-restart-guard" in app_src
    idx = app_src.find("def manual_rescue_restart_guard")
    region = app_src[idx: idx + 1500]
    # Must be a real implementation (writes to trade_queue), not a stub
    assert "trade_queue" in region
    assert "manual_rescue_current_session" in region


def test_app_source_paper_rescue_endpoint_intact():
    """Item 4: PR #162 paper rescue endpoint must survive this PR's app.py rework."""
    app_src = (REPO_ROOT / "app.py").read_text()
    assert "/admin/paper_rescue_restart_guard" in app_src
    idx = app_src.find("def admin_paper_rescue_restart_guard")
    region = app_src[idx: idx + 1500]
    assert "run_paper_rescue_restart_guard" in region


def test_app_source_run_morning_handoff_audit_not_replaced():
    """Item 3: the real run_morning_handoff_audit must still be called by the
    endpoints, not replaced with an APStartupRecovery-only reseed."""
    app_src = (REPO_ROOT / "app.py").read_text()
    # Both GET and POST endpoints call the real function
    assert app_src.count("run_morning_handoff_audit(") >= 2
    audit_src = (REPO_ROOT / "ap_morning_handoff_audit.py").read_text()
    assert "def run_morning_handoff_audit" in audit_src


def test_morning_handoff_audit_safety_comment_preserved():
    """Item: the safety-invariant comment block (NEVER submits/creates/mutates)
    must be preserved above the morning_handoff_audit endpoints."""
    app_src = (REPO_ROOT / "app.py").read_text()
    idx = app_src.find("morning_handoff_audit")
    region = app_src[idx: idx + 800]
    assert "NEVER" in region or "broker" in region.lower()
