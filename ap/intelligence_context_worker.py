from __future__ import annotations

import logging
import os
import threading
import time
import uuid
from typing import Any, Optional

from ap.intelligence_context_materializer import build_snapshot_kwargs
from ap.intelligence_snapshot_store import (
    claim_due_intelligence_jobs,
    complete_job_with_snapshot,
    mark_job_retry,
    mark_job_terminal,
)

log = logging.getLogger("ap.intelligence_context_worker")

_THREADS: dict[str, threading.Thread] = {}


def process_due_intelligence_jobs_once(
    *,
    claim_owner: Optional[str] = None,
    limit: int = 5,
    lease_seconds: int = 120,
) -> dict[str, Any]:
    owner = claim_owner or f"intelligence-context-worker:{uuid.uuid4().hex[:8]}"
    claimed = claim_due_intelligence_jobs(
        claim_owner=owner,
        limit=limit,
        lease_seconds=lease_seconds,
    )
    if not claimed.get("ok"):
        return {"ok": False, "claimed": 0, "completed": 0, "errors": 1, "error": claimed.get("error")}
    completed = 0
    retried = 0
    terminal = 0
    errors = 0
    for job in claimed.get("jobs") or []:
        try:
            snapshot_kwargs = build_snapshot_kwargs(job)
            result = complete_job_with_snapshot(job, claim_owner=owner, snapshot_kwargs=snapshot_kwargs)
            if result.get("ok") and result.get("completed"):
                completed += 1
                continue
            errors += 1
            attempts = int(job.get("attempt_count") or 0)
            max_attempts = int(job.get("max_attempts") or 3)
            if attempts >= max_attempts:
                mark_job_terminal(
                    str(job.get("id")),
                    claim_owner=owner,
                    error_code="snapshot_persist_failed",
                    error_detail=str(result.get("error") or result)[:500],
                )
                terminal += 1
            else:
                mark_job_retry(
                    str(job.get("id")),
                    claim_owner=owner,
                    error_code="snapshot_persist_failed",
                    error_detail=str(result.get("error") or result)[:500],
                    retry_delay_seconds=30,
                )
                retried += 1
        except Exception as exc:
            errors += 1
            attempts = int(job.get("attempt_count") or 0)
            max_attempts = int(job.get("max_attempts") or 3)
            if attempts >= max_attempts:
                mark_job_terminal(
                    str(job.get("id")),
                    claim_owner=owner,
                    error_code=type(exc).__name__,
                    error_detail=str(exc)[:500],
                )
                terminal += 1
            else:
                mark_job_retry(
                    str(job.get("id")),
                    claim_owner=owner,
                    error_code=type(exc).__name__,
                    error_detail=str(exc)[:500],
                    retry_delay_seconds=30,
                )
                retried += 1
    return {
        "ok": True,
        "claimed": len(claimed.get("jobs") or []),
        "completed": completed,
        "retried": retried,
        "terminal": terminal,
        "errors": errors,
    }


def start_intelligence_context_worker(
    *,
    client_id: str,
    stop_event: threading.Event,
    interval_seconds: Optional[float] = None,
) -> Optional[threading.Thread]:
    if os.getenv("INTELLIGENCE_CONTEXT_WORKER_ENABLED", "0").strip().lower() not in {"1", "true", "yes", "on"}:
        return None
    name = f"intelligence-context-{client_id}"
    existing = _THREADS.get(name)
    if existing and existing.is_alive():
        return existing
    interval = float(interval_seconds or os.getenv("INTELLIGENCE_CONTEXT_WORKER_INTERVAL_SEC", "5"))

    def _loop() -> None:
        owner = f"{name}:{uuid.uuid4().hex[:8]}"
        while not stop_event.is_set():
            result = process_due_intelligence_jobs_once(claim_owner=owner)
            if result.get("errors"):
                log.warning("[%s] intelligence context worker errors: %s", client_id, result)
            stop_event.wait(interval)

    thread = threading.Thread(target=_loop, daemon=True, name=name)
    thread.start()
    _THREADS[name] = thread
    return thread
