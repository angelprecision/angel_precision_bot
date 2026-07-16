#!/usr/bin/env python3
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ap.morning_jobs import (
    DEFAULT_LIVE_CLIENT,
    DEFAULT_PAPER_CLIENTS,
    MORNING_HANDOFF_BACKUP_JOB,
    MORNING_HANDOFF_PRIMARY_JOB,
    MORNING_RECOVERY_JOB,
    OVERNIGHT_REEVAL_BATCH_JOB,
    build_job_calls,
    call_admin_endpoint,
    should_run_now,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
log = logging.getLogger("ap.scripts.live_morning_jobs")

ET = ZoneInfo("America/New_York")


def _commit_sha() -> str:
    return str(
        os.getenv("RENDER_GIT_COMMIT")
        or os.getenv("COMMIT_SHA")
        or os.getenv("GITHUB_SHA")
        or "unknown"
    )[:12]


def _pod_id() -> str:
    return str(os.getenv("POD_ID", "") or "unknown").strip() or "unknown"


def _resolve_job_from_env() -> str:
    job = str(os.getenv("MORNING_JOB", "") or "").strip().lower()
    if job:
        return job
    raise SystemExit("MORNING_JOB env required")


def _resolve_paper_clients() -> list[str]:
    raw = str(
        os.getenv(
            "MORNING_JOB_PAPER_CLIENTS",
            ",".join(DEFAULT_PAPER_CLIENTS),
        )
        or ""
    ).strip()
    return [item.strip() for item in raw.split(",") if item.strip()]


def _resolve_execution_mode_filter() -> str | None:
    raw = str(os.getenv("MORNING_JOB_EXECUTION_MODE", "") or "").strip().lower()
    if not raw:
        return None
    if raw not in {"live", "paper"}:
        raise SystemExit(f"unsupported MORNING_JOB_EXECUTION_MODE={raw!r}")
    return raw


def _resolve_scoped_clients() -> tuple[str, list[str], str | None]:
    mode_filter = _resolve_execution_mode_filter()
    explicit_client_id = str(os.getenv("MORNING_JOB_CLIENT_ID", "") or "").strip()
    live_client = str(os.getenv("MORNING_JOB_LIVE_CLIENT", DEFAULT_LIVE_CLIENT) or "").strip()
    paper_clients = _resolve_paper_clients()

    if explicit_client_id and mode_filter == "paper":
        paper_clients = [explicit_client_id]
    elif explicit_client_id and mode_filter == "live":
        live_client = explicit_client_id

    return live_client, paper_clients, mode_filter


def main() -> int:
    bot_url = str(os.getenv("BOT_URL", os.getenv("AP_BOT_URL", "")) or "").strip()
    signing_secret = str(
        os.getenv("SIGNING_SECRET", os.getenv("AP_SIGNING_SECRET", "")) or ""
    ).strip()
    live_client, paper_clients, mode_filter = _resolve_scoped_clients()
    timeout_seconds = int(os.getenv("MORNING_JOB_TIMEOUT_SECONDS", "180"))
    tolerance_minutes = int(os.getenv("MORNING_JOB_WINDOW_TOLERANCE_MINUTES", "45"))
    force_window = str(os.getenv("MORNING_JOB_FORCE_WINDOW", "0")).strip().lower() in {"1", "true", "yes"}

    if not bot_url:
        raise SystemExit("BOT_URL env required")
    if not signing_secret:
        raise SystemExit("SIGNING_SECRET env required")

    job_name = _resolve_job_from_env()
    if job_name not in {
        OVERNIGHT_REEVAL_BATCH_JOB,
        MORNING_HANDOFF_PRIMARY_JOB,
        MORNING_HANDOFF_BACKUP_JOB,
        MORNING_RECOVERY_JOB,
    }:
        raise SystemExit(f"unsupported MORNING_JOB={job_name!r}")

    now_et = datetime.now(ET)
    if not force_window and not should_run_now(job_name, now=now_et, tolerance_minutes=tolerance_minutes):
        log.info(
            "job=%s success=true skipped=true reason=outside_expected_window tolerance_minutes=%s now_et=%s",
            job_name,
            tolerance_minutes,
            now_et.isoformat(),
        )
        return 0

    results = []
    failures = 0
    calls = build_job_calls(
        job_name,
        live_client=live_client,
        paper_clients=paper_clients,
    )
    if mode_filter is not None:
        calls = [call for call in calls if call.execution_mode == mode_filter]

    execution_modes = ",".join(sorted({call.execution_mode for call in calls})) or "none"
    log.info(
        "AUTONOMY_JOB_REGISTERED job=%s commit_sha=%s pod_id=%s client_count=%s execution_mode=%s",
        job_name,
        _commit_sha(),
        _pod_id(),
        len(calls),
        execution_modes,
    )

    for call in calls:
        if call.job_name.startswith("overnight_reeval"):
            log.info(
                "OVERNIGHT_JOB_STARTED job=%s client_scope=%s execution_mode=%s commit_sha=%s pod_id=%s client_count=%s",
                call.job_name,
                call.client_scope,
                call.execution_mode,
                _commit_sha(),
                _pod_id(),
                len(calls),
            )
        elif call.endpoint == "/admin/morning_handoff_audit":
            log.info(
                "MORNING_REEVAL_STARTED job=%s client_scope=%s execution_mode=%s commit_sha=%s pod_id=%s client_count=%s",
                call.job_name,
                call.client_scope,
                call.execution_mode,
                _commit_sha(),
                _pod_id(),
                len(calls),
            )
        result = call_admin_endpoint(
            bot_url=bot_url,
            endpoint=call.endpoint,
            secret=signing_secret,
            payload=call.payload,
            job_name=call.job_name,
            client_scope=call.client_scope,
            execution_mode=call.execution_mode,
            timeout_seconds=timeout_seconds,
            now=now_et,
        )
        results.append(
            {
                "job_name": call.job_name,
                "endpoint": call.endpoint,
                "client_scope": call.client_scope,
                "execution_mode": call.execution_mode,
                **result,
            }
        )
        if not result.get("ok"):
            failures += 1

    summary = {
        "ok": failures == 0,
        "job": job_name,
        "executed": len(results),
        "failures": failures,
        "results": results,
    }
    print(json.dumps(summary, default=str))
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
