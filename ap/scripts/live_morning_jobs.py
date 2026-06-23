#!/usr/bin/env python3
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from ap.morning_jobs import (
    DEFAULT_EXECUTION_MODE,
    DEFAULT_LIVE_CLIENT,
    MORNING_HANDOFF_AUDIT_ENDPOINT,
    MORNING_HANDOFF_BACKUP_JOB,
    MORNING_HANDOFF_PRIMARY_JOB,
    OVERNIGHT_REEVAL_ENDPOINT,
    OVERNIGHT_REEVAL_JOB,
    build_morning_handoff_payload,
    build_overnight_reeval_payload,
    call_admin_endpoint,
    should_run_now,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
log = logging.getLogger("ap.scripts.live_morning_jobs")

ET = ZoneInfo("America/New_York")


def _resolve_job_from_env() -> str:
    job = str(os.getenv("MORNING_JOB", "") or "").strip().lower()
    if job:
        return job
    raise SystemExit("MORNING_JOB env required")


def _build_job_config(job_name: str) -> tuple[str, dict]:
    if job_name == OVERNIGHT_REEVAL_JOB:
        return OVERNIGHT_REEVAL_ENDPOINT, build_overnight_reeval_payload()
    if job_name in (MORNING_HANDOFF_PRIMARY_JOB, MORNING_HANDOFF_BACKUP_JOB):
        return MORNING_HANDOFF_AUDIT_ENDPOINT, build_morning_handoff_payload()
    raise SystemExit(f"unsupported MORNING_JOB={job_name!r}")


def main() -> int:
    bot_url = str(os.getenv("BOT_URL", "") or "").strip()
    signing_secret = str(os.getenv("SIGNING_SECRET", "") or "").strip()
    client_id = str(os.getenv("MORNING_JOB_CLIENT_ID", DEFAULT_LIVE_CLIENT) or "").strip()
    execution_mode = str(os.getenv("MORNING_JOB_EXECUTION_MODE", DEFAULT_EXECUTION_MODE) or "").strip().lower()
    timeout_seconds = int(os.getenv("MORNING_JOB_TIMEOUT_SECONDS", "180"))
    force_window = str(os.getenv("MORNING_JOB_FORCE_WINDOW", "0")).strip().lower() in {"1", "true", "yes"}

    if not bot_url:
        raise SystemExit("BOT_URL env required")
    if not signing_secret:
        raise SystemExit("SIGNING_SECRET env required")

    job_name = _resolve_job_from_env()
    now_et = datetime.now(ET)
    if not force_window and not should_run_now(job_name, now=now_et):
        log.info(
            "job=%s client=%s execution_mode=%s success=true skipped=true reason=outside_expected_window now_et=%s",
            job_name,
            client_id,
            execution_mode,
            now_et.isoformat(),
        )
        return 0

    endpoint, payload = _build_job_config(job_name)
    if job_name == OVERNIGHT_REEVAL_JOB:
        payload = build_overnight_reeval_payload(
            client_id=client_id,
            execution_mode=execution_mode if execution_mode != "live" else None,
        )
    else:
        payload = build_morning_handoff_payload(
            client_id=client_id,
            execution_mode=execution_mode,
            dry_run=False,
        )

    result = call_admin_endpoint(
        bot_url=bot_url,
        endpoint=endpoint,
        secret=signing_secret,
        payload=payload,
        job_name=job_name,
        client_id=client_id,
        execution_mode=execution_mode,
        timeout_seconds=timeout_seconds,
        now=now_et,
    )

    print(json.dumps(result, default=str))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main())
