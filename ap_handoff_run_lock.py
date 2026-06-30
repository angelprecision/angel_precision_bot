"""
ap_handoff_run_lock.py
──────────────────────
Server-side idempotency for market-open morning jobs.

Render Cron (primary), the GitHub Actions backup, and operator manual triggers
can all target the same job window. This module enforces SINGLE execution at the
DB layer via an INSERT ... ON CONFLICT DO NOTHING on a unique run_key.

This is REAL enforcement, not log-only job_window_key:
  - try_acquire_run_lock() returns an acquire result dict with `acquired=True`
    only for the winning caller of a run_key.
  - All subsequent callers within the same trade_date get
    `{"acquired": False, "reason": "lock_held"}`.
  - A stale 'running' lock older than STALE_LOCK_SECONDS is reclaimable so a
    crashed run does not block the window forever.

Usage in an admin endpoint:

    from ap_handoff_run_lock import (
        build_run_key, try_acquire_run_lock, mark_run_lock_completed,
        mark_run_lock_failed,
    )

    run_key = build_run_key(job_name="morning_handoff_primary",
                            execution_mode="live", client_scope="jason@...")
    acquire_result = try_acquire_run_lock(
        run_key,
        job_name=...,
        execution_mode=...,
        client_scope=...,
        triggered_by="render_cron",
    )
    if not acquire_result.get("acquired"):
        return jsonify({"ok": True, "skipped": "run_lock_held", "run_key": run_key})
    try:
        ... do the work ...
        mark_run_lock_completed(run_key, acquire_result["owner_token"], summary)
    except Exception as e:
        mark_run_lock_failed(run_key, acquire_result["owner_token"], str(e))
        raise
"""
from __future__ import annotations

import hashlib
import logging
import uuid
from datetime import datetime, timezone, date
# PR #182: table renamed handoff_run_locks → handoff_job_locks.
# See migrations/20260625_handoff_run_locks_schema_fix.sql.
# handoff_run_locks is now reserved for ap/morning_handoff.py (per-client stage tracking).
from typing import Any, Optional

log = logging.getLogger("ap.handoff_run_lock")

# A 'running' lock older than this is considered stale and may be reclaimed —
# protects against a crashed run blocking the window forever. 10 minutes is
# well beyond the longest morning job (handoff audit over ~20 rows).
STALE_LOCK_SECONDS = 600


def _today_et() -> date:
    """Trade date in ET. Imported lazily so this module stays import-light."""
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("America/New_York")).date()


def _client_scope_hash(client_scope: str) -> str:
    """Short stable hash of the client scope so run_key length stays bounded."""
    return hashlib.sha256(client_scope.encode("utf-8")).hexdigest()[:12]


def build_run_key(
    *,
    job_name: str,
    execution_mode: str,
    client_scope: str,
    trade_date: Optional[date] = None,
) -> str:
    """Deterministic run_key. Same inputs on the same trade_date → same key."""
    td = trade_date or _today_et()
    scope_hash = _client_scope_hash(client_scope)
    return f"morning_job:{td.isoformat()}:{job_name}:{execution_mode}:{scope_hash}"


def try_acquire_run_lock(
    run_key: str,
    *,
    job_name: str,
    execution_mode: str,
    client_scope: str,
    triggered_by: str = "unknown",
    trade_date: Optional[date] = None,
) -> dict[str, Any]:
    """Attempt to acquire the run lock.

    Single atomic statement: INSERT ... ON CONFLICT DO NOTHING. The unique
    PRIMARY KEY on run_key guarantees exactly one winner across all processes
    (Render pod, GitHub backup runner, manual trigger).

    A stale 'running' lock (older than STALE_LOCK_SECONDS) is reclaimed via a
    second UPDATE that only succeeds if the existing row is still stale —
    itself race-safe because the WHERE clause re-checks staleness atomically.
    """
    from ap.db import conn, run_with_retry

    td = trade_date or _today_et()
    owner_token = uuid.uuid4().hex

    def _acquire() -> dict[str, Any]:
        with conn() as c:
            # Fast path: first writer wins.
            c.execute(
                """
                INSERT INTO public.handoff_job_locks
                    (run_key, trade_date, job_name, execution_mode,
                     client_scope, triggered_by, status, acquired_at, owner_token)
                VALUES (%s, %s, %s, %s, %s, %s, 'running', NOW(), %s)
                ON CONFLICT (run_key) DO NOTHING
                """,
                (run_key, td, job_name, execution_mode, client_scope, triggered_by, owner_token),
            )
            if c.rowcount == 1:
                return {
                    "acquired": True,
                    "run_key": run_key,
                    "owner_token": owner_token,
                    "reclaimed": False,
                    "lock_persisted": True,
                }

            # Lock exists. Reclaim if it is:
            #   (a) a stale 'running' row (crashed run), OR
            #   (b) a 'failed' row (the primary attempt failed — a later backup
            #       window SHOULD retry it).
            # A 'completed' row is never reclaimed — work already succeeded, so
            # the backup correctly skips. Atomic: WHERE re-checks status+age so
            # two reclaimers cannot both win.
            c.execute(
                """
                UPDATE public.handoff_job_locks
                SET    status='running', acquired_at=NOW(),
                       triggered_by=%s, completed_at=NULL, result_summary=NULL,
                       failed_reason=NULL, owner_token=%s
                WHERE  run_key=%s
                  AND  (
                          status='failed'
                       OR (status='running'
                           AND acquired_at < NOW() - (%s || ' seconds')::interval)
                       )
                """,
                (triggered_by, owner_token, run_key, str(STALE_LOCK_SECONDS)),
            )
            if c.rowcount == 1:
                return {
                    "acquired": True,
                    "run_key": run_key,
                    "owner_token": owner_token,
                    "reclaimed": True,
                    "lock_persisted": True,
                }

            return {
                "acquired": False,
                "run_key": run_key,
                "owner_token": None,
                "reason": "lock_held",
            }

    try:
        result = run_with_retry(_acquire)
        if result.get("acquired"):
            log.info(
                "HANDOFF_RUN_LOCK_ACQUIRED run_key=%s triggered_by=%s reclaimed=%s",
                run_key, triggered_by, bool(result.get("reclaimed")),
            )
        else:
            log.info(
                "HANDOFF_RUN_LOCK_SKIPPED run_key=%s triggered_by=%s reason=lock_held",
                run_key, triggered_by,
            )
        return result
    except Exception as exc:
        # If the lock table is unavailable, FAIL OPEN with a warning rather than
        # blocking the morning job entirely. Double-execution is undesirable but
        # the underlying endpoints are themselves idempotent, so a fail-open here
        # degrades to the prior (endpoint-level) idempotency, not to corruption.
        log.warning(
            "HANDOFF_RUN_LOCK_ERROR run_key=%s triggered_by=%s error=%s — failing open",
            run_key, triggered_by, exc,
        )
        return {
            "acquired": True,
            "run_key": run_key,
            "owner_token": owner_token,
            "reclaimed": False,
            "reason": "lock_error_fail_open",
            "lock_persisted": False,
        }


def mark_run_lock_completed(
    run_key: str,
    owner_token: str,
    result_summary: Optional[dict] = None,
) -> None:
    """Mark the lock completed. Never raises."""
    from ap.db import conn, run_with_retry
    import json as _json

    def _complete():
        with conn() as c:
            c.execute(
                """
                UPDATE public.handoff_job_locks
                SET    status='completed', completed_at=NOW(), result_summary=%s
                WHERE  run_key=%s
                  AND  owner_token=%s
                  AND  status='running'
                """,
                (_json.dumps(result_summary or {}), run_key, owner_token),
            )
            if c.rowcount == 0:
                log.warning("HANDOFF_RUN_LOCK_OWNER_MISMATCH run_key=%s action=complete", run_key)
    try:
        run_with_retry(_complete)
    except Exception as exc:
        log.warning("HANDOFF_RUN_LOCK_COMPLETE_FAILED run_key=%s error=%s", run_key, exc)


def mark_run_lock_failed(
    run_key: str,
    owner_token: str,
    error: str,
    result_summary: Optional[dict] = None,
) -> None:
    """Mark the lock failed so the window can be retried. Never raises."""
    from ap.db import conn, run_with_retry
    import json as _json

    def _fail():
        with conn() as c:
            c.execute(
                """
                UPDATE public.handoff_job_locks
                SET    status='failed',
                       completed_at=NOW(),
                       failed_reason=%s,
                       result_summary=%s
                WHERE  run_key=%s
                  AND  owner_token=%s
                  AND  status='running'
                """,
                (error, _json.dumps(result_summary or {}), run_key, owner_token),
            )
            if c.rowcount == 0:
                log.warning("HANDOFF_RUN_LOCK_OWNER_MISMATCH run_key=%s action=fail", run_key)
    try:
        run_with_retry(_fail)
    except Exception as exc:
        log.warning("HANDOFF_RUN_LOCK_FAIL_MARK_FAILED run_key=%s error=%s", run_key, exc)
