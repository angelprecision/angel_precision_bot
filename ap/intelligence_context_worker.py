from __future__ import annotations

import logging
import os
import threading
import time
import uuid
from typing import Any, Optional

from ap.intelligence_context_materializer import (
    build_snapshot_kwargs,
    recover_missing_intelligence_jobs,
)
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
    client_id: str,
    execution_mode: str,
    claim_owner: Optional[str] = None,
    limit: int = 5,
    lease_seconds: int = 120,
    broker: Any = None,
) -> dict[str, Any]:
    owner = claim_owner or f"intelligence-context-worker:{uuid.uuid4().hex[:8]}"
    claimed = claim_due_intelligence_jobs(
        claim_owner=owner,
        client_id=client_id,
        execution_mode=execution_mode,
        limit=limit,
        lease_seconds=lease_seconds,
    )
    if not claimed.get("ok"):
        return {"ok": False, "claimed": 0, "completed": 0, "errors": 1, "error": claimed.get("error")}
    completed = 0
    retried = 0
    terminal = 0
    errors = 0
    transition_failures = 0
    for job in claimed.get("jobs") or []:
        try:
            snapshot_kwargs = build_snapshot_kwargs(job, broker=broker)
            result = complete_job_with_snapshot(job, claim_owner=owner, snapshot_kwargs=snapshot_kwargs)
            if result.get("ok") and result.get("completed"):
                completed += 1
                continue
            errors += 1
            attempts = int(job.get("attempt_count") or 0)
            max_attempts = int(job.get("max_attempts") or 3)
            error_code = str(result.get("error_code") or "SNAPSHOT_PERSIST_FAILED")
            if error_code == "JOB_CLAIM_OWNERSHIP_LOST":
                transition_failures += 1
                log.critical("intelligence completion ownership lost job_id=%s owner=%s",
                             job.get("id"), owner)
                continue
            if attempts >= max_attempts:
                transition = mark_job_terminal(
                    str(job.get("id")),
                    claim_owner=owner,
                    error_code=error_code,
                    error_detail=str(result.get("error") or result)[:500],
                )
                if transition.get("ok") and transition.get("updated"):
                    terminal += 1
                else:
                    transition_failures += 1
                    log.critical("intelligence terminal transition failed job_id=%s owner=%s result=%s",
                                 job.get("id"), owner, transition)
            else:
                transition = mark_job_retry(
                    str(job.get("id")),
                    claim_owner=owner,
                    error_code=error_code,
                    error_detail=str(result.get("error") or result)[:500],
                    retry_delay_seconds=30,
                )
                if transition.get("ok") and transition.get("updated"):
                    retried += 1
                else:
                    transition_failures += 1
                    log.critical("intelligence retry transition failed job_id=%s owner=%s result=%s",
                                 job.get("id"), owner, transition)
        except Exception as exc:
            errors += 1
            attempts = int(job.get("attempt_count") or 0)
            max_attempts = int(job.get("max_attempts") or 3)
            error_code = str(getattr(exc, "error_code", "") or type(exc).__name__)
            if error_code.startswith("BREACH_"):
                attempts = max_attempts
            if attempts >= max_attempts:
                transition = mark_job_terminal(
                    str(job.get("id")),
                    claim_owner=owner,
                    error_code=error_code,
                    error_detail=str(exc)[:500],
                )
                if transition.get("ok") and transition.get("updated"):
                    terminal += 1
                else:
                    transition_failures += 1
                    log.critical("intelligence terminal transition failed job_id=%s owner=%s result=%s",
                                 job.get("id"), owner, transition)
            else:
                transition = mark_job_retry(
                    str(job.get("id")),
                    claim_owner=owner,
                    error_code=error_code,
                    error_detail=str(exc)[:500],
                    retry_delay_seconds=30,
                )
                if transition.get("ok") and transition.get("updated"):
                    retried += 1
                else:
                    transition_failures += 1
                    log.critical("intelligence retry transition failed job_id=%s owner=%s result=%s",
                                 job.get("id"), owner, transition)
    return {
        "ok": True,
        "claimed": len(claimed.get("jobs") or []),
        "completed": completed,
        "retried": retried,
        "terminal": terminal,
        "errors": errors,
        "transition_failures": transition_failures,
    }


def start_intelligence_context_worker(
    *,
    client_id: str,
    execution_mode: str,
    stop_event: threading.Event,
    interval_seconds: Optional[float] = None,
    broker: Any = None,
) -> Optional[threading.Thread]:
    if os.getenv("INTELLIGENCE_CONTEXT_WORKER_ENABLED", "0").strip().lower() not in {"1", "true", "yes", "on"}:
        return None
    mode = str(execution_mode or "").strip().upper()
    name = f"intelligence-context-{client_id}-{mode.lower()}"
    existing = _THREADS.get(name)
    if existing and existing.is_alive():
        return existing
    interval = float(interval_seconds or os.getenv("INTELLIGENCE_CONTEXT_WORKER_INTERVAL_SEC", "5"))

    def _loop() -> None:
        owner = f"{name}:{uuid.uuid4().hex[:8]}"
        recovery_interval = float(os.getenv("INTELLIGENCE_CONTEXT_RECOVERY_INTERVAL_SEC", "60"))
        last_recovery = 0.0
        while not stop_event.is_set():
            now = time.monotonic()
            if now - last_recovery >= recovery_interval:
                recovery = recover_missing_intelligence_jobs(
                    client_id=client_id, execution_mode=execution_mode,
                )
                last_recovery = now
                if not recovery.get("ok") or recovery.get("breach_errors"):
                    log.warning("[%s] intelligence recovery scan incomplete: %s", client_id, recovery)
            result = process_due_intelligence_jobs_once(
                claim_owner=owner,
                client_id=client_id,
                execution_mode=execution_mode,
                broker=broker,
            )
            if result.get("errors"):
                log.warning("[%s] intelligence context worker errors: %s", client_id, result)
            stop_event.wait(interval)

    thread = threading.Thread(target=_loop, daemon=True, name=name)
    thread.start()
    _THREADS[name] = thread
    return thread
