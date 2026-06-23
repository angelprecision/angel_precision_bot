from __future__ import annotations

import hashlib
import hmac
import io
import json
import urllib.error
from datetime import datetime
from pathlib import Path

from ap.morning_jobs import (
    DEFAULT_EXECUTION_MODE,
    DEFAULT_LIVE_CLIENT,
    MORNING_HANDOFF_BACKUP_JOB,
    MORNING_HANDOFF_PRIMARY_JOB,
    OVERNIGHT_REEVAL_JOB,
    build_hmac_headers,
    build_job_window_key,
    build_morning_handoff_payload,
    build_overnight_reeval_payload,
    call_admin_endpoint,
    select_runner_items,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_hmac_signature_matches_shell_format():
    secret = "031b4935137f1999d176700274b73bd0"
    ts = "1718591880"
    payload = build_overnight_reeval_payload()
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


def test_scheduler_payload_for_overnight_reeval_is_jason_live_only():
    payload = build_overnight_reeval_payload()
    assert payload == {
        "force": True,
        "clients": [DEFAULT_LIVE_CLIENT],
        "max_clients": 1,
        "time_budget_seconds": 120,
        "async_background": False,
    }


def test_scheduler_payload_for_morning_handoff_is_jason_live_only():
    payload = build_morning_handoff_payload()
    assert payload == {
        "client_id": DEFAULT_LIVE_CLIENT,
        "execution_mode": DEFAULT_EXECUTION_MODE,
        "dry_run": False,
    }


def test_duplicate_run_same_window_uses_same_idempotency_key():
    now = datetime(2026, 6, 19, 9, 18)
    key1 = build_job_window_key(
        OVERNIGHT_REEVAL_JOB,
        client_id=DEFAULT_LIVE_CLIENT,
        execution_mode=DEFAULT_EXECUTION_MODE,
        now=now,
    )
    key2 = build_job_window_key(
        OVERNIGHT_REEVAL_JOB,
        client_id=DEFAULT_LIVE_CLIENT,
        execution_mode=DEFAULT_EXECUTION_MODE,
        now=now,
    )
    assert key1 == key2


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
    payload = build_morning_handoff_payload()

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
        client_id=DEFAULT_LIVE_CLIENT,
        execution_mode=DEFAULT_EXECUTION_MODE,
        urlopen=_raise_http_error,
    )
    assert result["ok"] is False
    assert result["status_code"] == 500
    assert "boom" in str(result["body"])


def test_network_exception_returns_failure_without_crashing():
    payload = build_overnight_reeval_payload()

    def _raise_network_error(_req, timeout=0):
        raise OSError("network down")

    result = call_admin_endpoint(
        bot_url="https://example.com",
        endpoint="/admin/overnight_reeval",
        secret="secret",
        payload=payload,
        job_name=OVERNIGHT_REEVAL_JOB,
        client_id=DEFAULT_LIVE_CLIENT,
        execution_mode=DEFAULT_EXECUTION_MODE,
        urlopen=_raise_network_error,
    )
    assert result["ok"] is False
    assert result["status_code"] is None
    assert "network down" in str(result["body"])


def test_workflow_targets_jason_live_only():
    workflow = (REPO_ROOT / ".github" / "workflows" / "overnight-reeval.yml").read_text()
    assert "MORNING_JOB_CLIENT_ID: jasoncosby1@gmail.com" in workflow
    assert "MORNING_JOB_EXECUTION_MODE: live" in workflow
    assert "18 13 * * 1-5" in workflow
    assert "25 13 * * 1-5" in workflow
    assert "31 13 * * 1-5" in workflow


def test_app_source_supports_filtered_overnight_and_morning_handoff():
    src = (REPO_ROOT / "app.py").read_text()
    assert 'body.get("clients")' in src
    assert '@app.post("/admin/morning_handoff_audit")' in src
    assert "APStartupRecovery" in src


def test_no_code_path_calls_broker_submit_or_cancel_directly():
    script_src = (REPO_ROOT / "ap" / "scripts" / "live_morning_jobs.py").read_text()
    helper_src = (REPO_ROOT / "ap" / "morning_jobs.py").read_text()
    combined = script_src + "\n" + helper_src
    assert "place_order(" not in combined
    assert "cancel_order(" not in combined
    assert "submit_existing_entry" not in combined
