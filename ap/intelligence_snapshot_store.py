from __future__ import annotations

import copy
import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

PHASES = {"PRETRIGGER", "PREOPEN", "BREACH", "CONTRACT_SELECTED"}
SNAPSHOT_STATUSES = {"COMPLETE", "PARTIAL", "STALE", "UNAVAILABLE", "ERROR"}
JOB_STATUSES = {"PENDING", "RUNNING", "RETRY_PENDING", "COMPLETED", "FAILED_TERMINAL"}
DEFAULT_PROFILE_VERSION = "intelligence_context_v1_observe_only"

_MEMORY_JOBS: dict[str, dict[str, Any]] = {}
_MEMORY_SNAPSHOTS: dict[str, dict[str, Any]] = {}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _memory_enabled() -> bool:
    return os.getenv("INTELLIGENCE_CONTEXT_STORE_BACKEND", "").lower() == "memory"


def _db_conn():
    from ap.db import conn

    return conn


def _run_with_retry(fn):
    from ap.db import run_with_retry

    return run_with_retry(fn)


def normalize_execution_mode(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if raw in {"LIVE", "PAPER"}:
        return raw
    return raw or "UNKNOWN"


def normalize_phase(value: Any) -> str:
    phase = str(value or "").strip().upper()
    if phase not in PHASES:
        raise ValueError(f"invalid intelligence phase: {phase}")
    return phase


def normalized_local_order_id(value: Any) -> str:
    return str(value or "").strip()


def identity_key(
    *,
    client_id: str,
    execution_mode: str,
    canonical_signal_id: str,
    local_order_id: str = "",
    phase: str,
    context_revision: int = 1,
    profile_version: str = DEFAULT_PROFILE_VERSION,
) -> str:
    return "|".join(
        (
            str(client_id or ""),
            normalize_execution_mode(execution_mode).lower(),
            str(canonical_signal_id or ""),
            normalized_local_order_id(local_order_id) or "__none__",
            normalize_phase(phase),
            str(int(context_revision or 1)),
            str(profile_version or DEFAULT_PROFILE_VERSION),
        )
    )


def _result(ok: bool, **fields: Any) -> dict[str, Any]:
    return {"ok": bool(ok), **fields}


def enqueue_intelligence_job(
    *,
    client_id: str,
    execution_mode: str,
    canonical_signal_id: str,
    signal_id: str = "",
    local_order_id: str = "",
    phase: str,
    context_revision: int = 1,
    max_attempts: int = 3,
    payload: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    try:
        phase = normalize_phase(phase)
        execution_mode = normalize_execution_mode(execution_mode)
        key = identity_key(
            client_id=client_id,
            execution_mode=execution_mode,
            canonical_signal_id=canonical_signal_id,
            local_order_id=local_order_id,
            phase=phase,
            context_revision=context_revision,
        )
        if _memory_enabled():
            existing = _MEMORY_JOBS.get(key)
            if existing:
                return _result(True, inserted=False, duplicate=True, job_id=existing["id"])
            job_id = str(uuid.uuid4())
            _MEMORY_JOBS[key] = {
                "id": job_id,
                "client_id": client_id,
                "execution_mode": execution_mode,
                "canonical_signal_id": canonical_signal_id,
                "signal_id": signal_id,
                "local_order_id": normalized_local_order_id(local_order_id),
                "phase": phase,
                "context_revision": int(context_revision or 1),
                "status": "PENDING",
                "attempt_count": 0,
                "max_attempts": int(max_attempts or 3),
                "payload": copy.deepcopy(payload or {}),
                "created_at": _now_iso(),
                "updated_at": _now_iso(),
            }
            return _result(True, inserted=True, duplicate=False, job_id=job_id)

        payload_json = json.dumps(payload or {}, default=str)

        def _fn():
            with _db_conn()() as c:
                c.execute(
                    """
                    INSERT INTO ap_intelligence_jobs (
                      client_id, execution_mode, canonical_signal_id, signal_id,
                      local_order_id, phase, context_revision, max_attempts, payload
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                    ON CONFLICT DO NOTHING
                    RETURNING id
                    """,
                    (
                        client_id,
                        execution_mode,
                        canonical_signal_id,
                        signal_id,
                        normalized_local_order_id(local_order_id),
                        phase,
                        int(context_revision or 1),
                        int(max_attempts or 3),
                        payload_json,
                    ),
                )
                inserted = c.fetchone()
                if inserted:
                    return _result(True, inserted=True, duplicate=False, job_id=str(inserted["id"]))
                c.execute(
                    """
                    SELECT id FROM ap_intelligence_jobs
                    WHERE client_id=%s
                      AND lower(execution_mode)=lower(%s)
                      AND canonical_signal_id=%s
                      AND COALESCE(NULLIF(BTRIM(local_order_id), ''), '__none__')=%s
                      AND phase=%s
                      AND context_revision=%s
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (
                        client_id,
                        execution_mode,
                        canonical_signal_id,
                        normalized_local_order_id(local_order_id) or "__none__",
                        phase,
                        int(context_revision or 1),
                    ),
                )
                existing = c.fetchone() or {}
                return _result(True, inserted=False, duplicate=True, job_id=str(existing.get("id") or ""))

        return _run_with_retry(_fn)
    except Exception as exc:
        return _result(False, inserted=False, duplicate=False, error=str(exc)[:500])


def claim_due_intelligence_jobs(
    *,
    claim_owner: str,
    limit: int = 5,
    lease_seconds: int = 120,
) -> dict[str, Any]:
    try:
        if _memory_enabled():
            now = time.time()
            claimed = []
            for job in _MEMORY_JOBS.values():
                if len(claimed) >= limit:
                    break
                expired = float(job.get("_claim_expires_epoch") or 0) <= now
                if job.get("status") in {"PENDING", "RETRY_PENDING"} or (
                    job.get("status") == "RUNNING" and expired
                ):
                    job["status"] = "RUNNING"
                    job["claim_owner"] = claim_owner
                    job["_claim_expires_epoch"] = now + lease_seconds
                    job["attempt_count"] = int(job.get("attempt_count") or 0) + 1
                    claimed.append(copy.deepcopy(job))
            return _result(True, jobs=claimed)

        def _fn():
            with _db_conn()() as c:
                c.execute(
                    """
                    WITH due AS (
                      SELECT id
                      FROM ap_intelligence_jobs
                      WHERE (
                        status IN ('PENDING', 'RETRY_PENDING')
                        AND next_attempt_at <= now()
                      )
                      OR (
                        status='RUNNING'
                        AND claim_expires_at IS NOT NULL
                        AND claim_expires_at <= now()
                      )
                      ORDER BY next_attempt_at ASC, created_at ASC
                      LIMIT %s
                      FOR UPDATE SKIP LOCKED
                    )
                    UPDATE ap_intelligence_jobs j
                    SET status='RUNNING',
                        attempt_count=attempt_count + 1,
                        claimed_at=now(),
                        claim_owner=%s,
                        claim_expires_at=now() + (%s || ' seconds')::interval,
                        updated_at=now()
                    FROM due
                    WHERE j.id=due.id
                    RETURNING j.*
                    """,
                    (int(limit or 5), claim_owner, int(lease_seconds or 120)),
                )
                return _result(True, jobs=c.fetchall())

        return _run_with_retry(_fn)
    except Exception as exc:
        return _result(False, jobs=[], error=str(exc)[:500])


def mark_job_retry(
    job_id: str,
    *,
    claim_owner: str,
    error_code: str,
    error_detail: str = "",
    retry_delay_seconds: int = 30,
) -> dict[str, Any]:
    return _update_job_terminality(
        job_id,
        claim_owner=claim_owner,
        status="RETRY_PENDING",
        error_code=error_code,
        error_detail=error_detail,
        retry_delay_seconds=retry_delay_seconds,
    )


def mark_job_terminal(
    job_id: str,
    *,
    claim_owner: str,
    error_code: str,
    error_detail: str = "",
) -> dict[str, Any]:
    return _update_job_terminality(
        job_id,
        claim_owner=claim_owner,
        status="FAILED_TERMINAL",
        error_code=error_code,
        error_detail=error_detail,
        retry_delay_seconds=0,
    )


def _update_job_terminality(
    job_id: str,
    *,
    claim_owner: str,
    status: str,
    error_code: str,
    error_detail: str,
    retry_delay_seconds: int,
) -> dict[str, Any]:
    try:
        if _memory_enabled():
            for job in _MEMORY_JOBS.values():
                if str(job["id"]) == str(job_id) and job.get("claim_owner") == claim_owner:
                    job["status"] = status
                    job["last_error_code"] = error_code
                    job["last_error_detail"] = error_detail[:500]
                    return _result(True, updated=True)
            return _result(False, updated=False, error="job_not_owned")

        def _fn():
            with _db_conn()() as c:
                c.execute(
                    """
                    UPDATE ap_intelligence_jobs
                    SET status=%s,
                        last_error_code=%s,
                        last_error_detail=%s,
                        next_attempt_at=now() + (%s || ' seconds')::interval,
                        claim_expires_at=NULL,
                        updated_at=now()
                    WHERE id=%s AND claim_owner=%s AND status='RUNNING'
                    """,
                    (
                        status,
                        error_code,
                        error_detail[:500],
                        int(retry_delay_seconds or 0),
                        job_id,
                        claim_owner,
                    ),
                )
                return _result(c.rowcount == 1, updated=c.rowcount == 1)

        return _run_with_retry(_fn)
    except Exception as exc:
        return _result(False, updated=False, error=str(exc)[:500])


def write_snapshot(
    *,
    client_id: str,
    execution_mode: str,
    canonical_signal_id: str,
    signal_id: str = "",
    local_order_id: str = "",
    phase: str,
    context_revision: int = 1,
    profile_version: str = DEFAULT_PROFILE_VERSION,
    parent_snapshot_id: Optional[str] = None,
    input_hash: str,
    config_hash: str = "",
    git_commit: str = "",
    data_as_of: Optional[str] = None,
    status: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    try:
        phase = normalize_phase(phase)
        status = str(status or "").strip().upper()
        if status not in SNAPSHOT_STATUSES:
            raise ValueError(f"invalid snapshot status: {status}")
        key = identity_key(
            client_id=client_id,
            execution_mode=execution_mode,
            canonical_signal_id=canonical_signal_id,
            local_order_id=local_order_id,
            phase=phase,
            context_revision=context_revision,
            profile_version=profile_version,
        )
        if _memory_enabled():
            existing = _MEMORY_SNAPSHOTS.get(key)
            if existing:
                return _result(True, inserted=False, duplicate=True, snapshot_id=existing["id"])
            snapshot_id = str(uuid.uuid4())
            _MEMORY_SNAPSHOTS[key] = {
                "id": snapshot_id,
                "client_id": client_id,
                "execution_mode": normalize_execution_mode(execution_mode),
                "canonical_signal_id": canonical_signal_id,
                "signal_id": signal_id,
                "local_order_id": normalized_local_order_id(local_order_id),
                "phase": phase,
                "context_revision": int(context_revision or 1),
                "profile_version": profile_version,
                "parent_snapshot_id": parent_snapshot_id,
                "input_hash": input_hash,
                "config_hash": config_hash,
                "git_commit": git_commit,
                "data_as_of": data_as_of,
                "computed_at": _now_iso(),
                "status": status,
                "payload": copy.deepcopy(payload),
            }
            return _result(True, inserted=True, duplicate=False, snapshot_id=snapshot_id)

        payload_json = json.dumps(payload or {}, default=str)

        def _fn():
            with _db_conn()() as c:
                c.execute(
                    """
                    INSERT INTO ap_intelligence_snapshots (
                      client_id, execution_mode, canonical_signal_id, signal_id,
                      local_order_id, phase, context_revision, profile_version,
                      parent_snapshot_id, input_hash, config_hash, git_commit,
                      data_as_of, status, payload
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                    ON CONFLICT DO NOTHING
                    RETURNING id
                    """,
                    (
                        client_id,
                        normalize_execution_mode(execution_mode),
                        canonical_signal_id,
                        signal_id,
                        normalized_local_order_id(local_order_id),
                        phase,
                        int(context_revision or 1),
                        profile_version,
                        parent_snapshot_id,
                        input_hash,
                        config_hash,
                        git_commit,
                        data_as_of,
                        status,
                        payload_json,
                    ),
                )
                inserted = c.fetchone()
                if inserted:
                    return _result(True, inserted=True, duplicate=False, snapshot_id=str(inserted["id"]))
                c.execute(
                    """
                    SELECT id FROM ap_intelligence_snapshots
                    WHERE client_id=%s
                      AND lower(execution_mode)=lower(%s)
                      AND canonical_signal_id=%s
                      AND COALESCE(NULLIF(BTRIM(local_order_id), ''), '__none__')=%s
                      AND phase=%s
                      AND context_revision=%s
                      AND profile_version=%s
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (
                        client_id,
                        execution_mode,
                        canonical_signal_id,
                        normalized_local_order_id(local_order_id) or "__none__",
                        phase,
                        int(context_revision or 1),
                        profile_version,
                    ),
                )
                existing = c.fetchone() or {}
                return _result(True, inserted=False, duplicate=True, snapshot_id=str(existing.get("id") or ""))

        return _run_with_retry(_fn)
    except Exception as exc:
        return _result(False, inserted=False, duplicate=False, snapshot_id="", error=str(exc)[:500])


def complete_job_with_snapshot(
    job: dict[str, Any],
    *,
    claim_owner: str,
    snapshot_kwargs: dict[str, Any],
) -> dict[str, Any]:
    snapshot = write_snapshot(**snapshot_kwargs)
    if not snapshot.get("ok"):
        return _result(False, completed=False, snapshot=snapshot, error=snapshot.get("error"))
    job_id = str(job.get("id") or "")
    try:
        if _memory_enabled():
            for stored in _MEMORY_JOBS.values():
                if str(stored["id"]) == job_id and stored.get("claim_owner") == claim_owner:
                    stored["status"] = "COMPLETED"
                    stored["snapshot_id"] = snapshot.get("snapshot_id")
                    return _result(True, completed=True, snapshot=snapshot)
            return _result(False, completed=False, snapshot=snapshot, error="job_not_owned")

        def _fn():
            with _db_conn()() as c:
                c.execute(
                    """
                    UPDATE ap_intelligence_jobs
                    SET status='COMPLETED',
                        claim_expires_at=NULL,
                        updated_at=now()
                    WHERE id=%s AND claim_owner=%s AND status='RUNNING'
                    """,
                    (job_id, claim_owner),
                )
                return _result(c.rowcount == 1, completed=c.rowcount == 1, snapshot=snapshot)

        return _run_with_retry(_fn)
    except Exception as exc:
        return _result(False, completed=False, snapshot=snapshot, error=str(exc)[:500])


def get_latest_snapshot(
    *,
    client_id: str,
    execution_mode: str,
    canonical_signal_id: str,
    phase: str,
) -> dict[str, Any]:
    try:
        phase = normalize_phase(phase)
        if _memory_enabled():
            rows = [
                row
                for row in _MEMORY_SNAPSHOTS.values()
                if row["client_id"] == client_id
                and row["execution_mode"].upper() == normalize_execution_mode(execution_mode)
                and row["canonical_signal_id"] == canonical_signal_id
                and row["phase"] == phase
            ]
            rows.sort(key=lambda row: (row["context_revision"], row["computed_at"]), reverse=True)
            return _result(True, snapshot=copy.deepcopy(rows[0]) if rows else None)

        def _fn():
            with _db_conn()() as c:
                c.execute(
                    """
                    SELECT * FROM ap_intelligence_snapshots
                    WHERE client_id=%s
                      AND lower(execution_mode)=lower(%s)
                      AND canonical_signal_id=%s
                      AND phase=%s
                    ORDER BY context_revision DESC, computed_at DESC
                    LIMIT 1
                    """,
                    (client_id, execution_mode, canonical_signal_id, phase),
                )
                return _result(True, snapshot=c.fetchone())

        return _run_with_retry(_fn)
    except Exception as exc:
        return _result(False, snapshot=None, error=str(exc)[:500])


def get_latest_phase_snapshots(
    *,
    client_id: str,
    execution_mode: str,
    canonical_signal_id: str,
) -> dict[str, Any]:
    snapshots = {}
    for phase in ("PRETRIGGER", "PREOPEN", "BREACH", "CONTRACT_SELECTED"):
        latest = get_latest_snapshot(
            client_id=client_id,
            execution_mode=execution_mode,
            canonical_signal_id=canonical_signal_id,
            phase=phase,
        )
        if latest.get("snapshot"):
            snapshots[phase] = latest["snapshot"]
    return _result(True, snapshots=snapshots)


def build_latest_composite(
    *,
    client_id: str,
    execution_mode: str,
    canonical_signal_id: str,
) -> dict[str, Any]:
    phases = get_latest_phase_snapshots(
        client_id=client_id,
        execution_mode=execution_mode,
        canonical_signal_id=canonical_signal_id,
    )
    composite = {
        "client_id": client_id,
        "execution_mode": normalize_execution_mode(execution_mode),
        "canonical_signal_id": canonical_signal_id,
        "observe_only": True,
        "affected_eligibility": False,
        "phases": {
            phase: (snapshot or {}).get("payload", {})
            for phase, snapshot in (phases.get("snapshots") or {}).items()
        },
    }
    return _result(True, composite=composite)


def _reset_memory_store_for_tests() -> None:
    _MEMORY_JOBS.clear()
    _MEMORY_SNAPSHOTS.clear()
