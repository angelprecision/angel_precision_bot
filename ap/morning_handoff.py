from __future__ import annotations

import logging
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo


log = logging.getLogger("ap.morning_handoff")

ET = ZoneInfo("America/New_York")
_TABLE_READY = False


def _now_et(now: datetime | None = None) -> datetime:
    return (now or datetime.now(ET)).astimezone(ET)


def _trading_date(now: datetime | None = None) -> str:
    return _now_et(now).date().isoformat()


def _is_market_day(now: datetime | None = None) -> bool:
    return _now_et(now).weekday() < 5


def _after_929_et(now: datetime | None = None) -> bool:
    dt = _now_et(now)
    return _is_market_day(dt) and (dt.hour > 9 or (dt.hour == 9 and dt.minute >= 29))


def _normalize_mode(value: str | None) -> str:
    return str(value or "").strip().lower()


def _ensure_handoff_table() -> None:
    global _TABLE_READY
    if _TABLE_READY:
        return
    from ap.db import conn, run_with_retry

    def _create():
        with conn() as c:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS handoff_run_locks (
                    client_id TEXT NOT NULL,
                    execution_mode TEXT NOT NULL,
                    trading_date DATE NOT NULL,
                    stage TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'running',
                    last_run_at TIMESTAMPTZ,
                    last_success_at TIMESTAMPTZ,
                    last_error TEXT,
                    details JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (client_id, execution_mode, trading_date, stage)
                )
                """
            )
            return True

    run_with_retry(_create)
    _TABLE_READY = True


def _load_handoff_run_lock(
    *,
    client_id: str,
    execution_mode: str,
    trading_date: str,
    stage: str,
) -> dict | None:
    from ap.db import conn, run_with_retry

    _ensure_handoff_table()

    def _load():
        with conn() as c:
            c.execute(
                """
                SELECT client_id, execution_mode, trading_date::text AS trading_date,
                       stage, status, last_run_at::text AS last_run_at,
                       last_success_at::text AS last_success_at,
                       last_error, details
                FROM handoff_run_locks
                WHERE client_id = %s
                  AND execution_mode = %s
                  AND trading_date = %s::date
                  AND stage = %s
                """,
                (client_id, execution_mode, trading_date, stage),
            )
            row = c.fetchone()
            if not row:
                return None
            cols = [d[0] for d in getattr(c, "description", [])]
            return dict(row) if isinstance(row, dict) else dict(zip(cols, row))

    return run_with_retry(_load)


def _upsert_handoff_run_lock(
    *,
    client_id: str,
    execution_mode: str,
    trading_date: str,
    stage: str,
    status: str,
    last_error: str | None,
    details: dict | None,
    mark_success: bool,
) -> None:
    from ap.db import conn, run_with_retry
    import json

    _ensure_handoff_table()

    def _write():
        with conn() as c:
            c.execute(
                """
                INSERT INTO handoff_run_locks (
                    client_id, execution_mode, trading_date, stage, status,
                    last_run_at, last_success_at, last_error, details, updated_at
                )
                VALUES (
                    %s, %s, %s::date, %s, %s,
                    NOW(),
                    CASE WHEN %s THEN NOW() ELSE NULL END,
                    %s,
                    %s::jsonb,
                    NOW()
                )
                ON CONFLICT (client_id, execution_mode, trading_date, stage)
                DO UPDATE SET
                    status = EXCLUDED.status,
                    last_run_at = EXCLUDED.last_run_at,
                    last_success_at = CASE
                        WHEN %s THEN EXCLUDED.last_run_at
                        ELSE handoff_run_locks.last_success_at
                    END,
                    last_error = EXCLUDED.last_error,
                    details = EXCLUDED.details,
                    updated_at = NOW()
                """,
                (
                    client_id,
                    execution_mode,
                    trading_date,
                    stage,
                    status,
                    mark_success,
                    last_error,
                    json.dumps(details or {}, default=str),
                    mark_success,
                ),
            )
            return True

    run_with_retry(_write)


def _count_state(client_id: str) -> dict:
    from ap.db import conn, run_with_retry

    def _snapshot():
        with conn() as c:
            c.execute(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN status = 'WATCHING' THEN 1 ELSE 0 END), 0)::int AS watching_rows,
                    COALESCE(SUM(CASE WHEN status = 'NEW' THEN 1 ELSE 0 END), 0)::int AS new_rows
                FROM trade_queue
                WHERE client_id = %s
                """,
                (client_id,),
            )
            tq_row = c.fetchone()
            tq_cols = [d[0] for d in getattr(c, "description", [])]
            c.execute(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN status = 'PENDING_TRIGGER' THEN 1 ELSE 0 END), 0)::int AS pending_trigger_rows
                FROM orders
                WHERE client_id = %s
                  AND kind = 'ENTRY'
                """,
                (client_id,),
            )
            orders_row = c.fetchone()
            orders_cols = [d[0] for d in getattr(c, "description", [])]
            tq = dict(tq_row or {}) if isinstance(tq_row, dict) else dict(zip(tq_cols, tq_row or ()))
            orders = dict(orders_row or {}) if isinstance(orders_row, dict) else dict(zip(orders_cols, orders_row or ()))
            tq.update(orders)
            return tq

    return run_with_retry(_snapshot) or {}


def _resolve_runner(client_id: str):
    from client_runner import _active_runners, _registry_lock

    with _registry_lock:
        return _active_runners.get(client_id)


def _expected_clients_by_mode() -> dict[str, list[str]]:
    from client_runner import _active_runners, _registry_lock

    paper: list[str] = []
    live: list[str] = []
    with _registry_lock:
        items = list(_active_runners.items())
    for email, runner in items:
        mode = _normalize_mode(getattr(runner, "mode", None) or getattr(getattr(runner, "master_control", None), "mode", None))
        if mode == "live":
            live.append(email)
        elif mode == "paper":
            paper.append(email)
    return {"paper": sorted(paper), "live": sorted(live)}


def _latest_handoff_rows(trading_date: str) -> list[dict]:
    from ap.db import conn, run_with_retry

    _ensure_handoff_table()

    def _load():
        with conn() as c:
            c.execute(
                """
                SELECT client_id, execution_mode, trading_date::text AS trading_date,
                       stage, status, last_run_at::text AS last_run_at,
                       last_success_at::text AS last_success_at,
                       last_error, details
                FROM handoff_run_locks
                WHERE trading_date = %s::date
                ORDER BY execution_mode, client_id, updated_at DESC
                """,
                (trading_date,),
            )
            rows = c.fetchall() or []
            cols = [d[0] for d in getattr(c, "description", [])]
            out = []
            for row in rows:
                out.append(dict(row) if isinstance(row, dict) else dict(zip(cols, row)))
            return out

    try:
        return run_with_retry(_load) or []
    except Exception:
        return []


def get_morning_handoff_health(now: datetime | None = None) -> dict:
    trading_date = _trading_date(now)
    rows = _latest_handoff_rows(trading_date)
    expected = _expected_clients_by_mode()

    by_mode: dict[str, dict[str, Any]] = {
        "paper": {"clients": {}, "last_run_at": None, "last_success_at": None},
        "live": {"clients": {}, "last_run_at": None, "last_success_at": None},
    }
    seen: set[tuple[str, str]] = set()

    for row in rows:
        mode = _normalize_mode(row.get("execution_mode"))
        client_id = str(row.get("client_id") or "").strip()
        if mode not in by_mode or not client_id:
            continue
        key = (mode, client_id)
        if key in seen:
            continue
        seen.add(key)
        by_mode[mode]["clients"][client_id] = {
            "last_run_at": row.get("last_run_at"),
            "last_success_at": row.get("last_success_at"),
            "last_stage": row.get("stage"),
            "last_error": row.get("last_error"),
            "status": row.get("status"),
        }
        if row.get("last_run_at") and (
            by_mode[mode]["last_run_at"] is None or str(row.get("last_run_at")) > str(by_mode[mode]["last_run_at"])
        ):
            by_mode[mode]["last_run_at"] = row.get("last_run_at")
        if row.get("last_success_at") and (
            by_mode[mode]["last_success_at"] is None or str(row.get("last_success_at")) > str(by_mode[mode]["last_success_at"])
        ):
            by_mode[mode]["last_success_at"] = row.get("last_success_at")

    after_guard = _after_929_et(now)
    live_missing = any(not by_mode["live"]["clients"].get(client_id, {}).get("last_success_at") for client_id in expected["live"])
    paper_missing_clients = [client_id for client_id in expected["paper"] if not by_mode["paper"]["clients"].get(client_id, {}).get("last_success_at")]

    by_mode["live"]["missing_after_929_et"] = bool(after_guard and live_missing)
    by_mode["paper"]["missing_after_929_et"] = bool(after_guard and bool(paper_missing_clients))
    by_mode["paper"]["missing_clients"] = paper_missing_clients
    return {
        "trading_date": trading_date,
        "paper": by_mode["paper"],
        "live": by_mode["live"],
    }


def run_morning_handoff_audit(
    *,
    client_id: str,
    execution_mode: str,
    stage: str,
    dry_run: bool = False,
    runner=None,
    now: datetime | None = None,
) -> dict:
    mode = _normalize_mode(execution_mode)
    client_id = str(client_id or "").strip()
    stage = str(stage or "").strip().lower()
    if not client_id:
        return {"ok": False, "error": "client_id_required", "stage": stage}
    if mode not in {"paper", "live"}:
        return {"ok": False, "error": "invalid_execution_mode", "stage": stage, "client_id": client_id}
    if stage not in {"startup", "post_overnight_reeval", "manual"}:
        return {"ok": False, "error": "invalid_stage", "stage": stage, "client_id": client_id}

    trading_date = _trading_date(now)
    existing = _load_handoff_run_lock(
        client_id=client_id,
        execution_mode=mode,
        trading_date=trading_date,
        stage=stage,
    )
    if existing and str(existing.get("status") or "").lower() == "success" and existing.get("last_success_at"):
        return {
            "ok": True,
            "skipped": True,
            "reason": "handoff_already_succeeded_for_stage_today",
            "client_id": client_id,
            "execution_mode": mode,
            "stage": stage,
            "trading_date": trading_date,
            "last_success_at": existing.get("last_success_at"),
        }

    _upsert_handoff_run_lock(
        client_id=client_id,
        execution_mode=mode,
        trading_date=trading_date,
        stage=stage,
        status="running",
        last_error=None,
        details={"dry_run": dry_run},
        mark_success=False,
    )

    runner = runner or _resolve_runner(client_id)
    if runner is None:
        err = "runner_not_found"
        _upsert_handoff_run_lock(
            client_id=client_id,
            execution_mode=mode,
            trading_date=trading_date,
            stage=stage,
            status="failed",
            last_error=err,
            details={"dry_run": dry_run},
            mark_success=False,
        )
        return {"ok": False, "error": err, "client_id": client_id, "execution_mode": mode, "stage": stage}

    osm = getattr(runner, "order_state_machine", None)
    pm = getattr(runner, "position_manager", None)
    mc = getattr(runner, "master_control", None)
    core = getattr(runner, "core", None)
    broker = getattr(core, "broker", None) if core else getattr(runner, "broker", None)
    exit_engine = getattr(core, "exit_eng", None) if core else None
    entry_watcher = getattr(core, "entry_watcher", None) if core else None

    warnings: list[str] = []
    if osm is None:
        warnings.append("order_state_machine_missing")
        log.warning("morning_handoff missing OSM client=%s execution_mode=%s stage=%s", client_id, mode, stage)
    if entry_watcher is None:
        warnings.append("entry_watcher_missing")
        log.warning("morning_handoff missing entry_watcher client=%s execution_mode=%s stage=%s", client_id, mode, stage)
    if mc is None:
        warnings.append("master_control_missing")
        log.warning("morning_handoff missing master_control client=%s execution_mode=%s stage=%s", client_id, mode, stage)

    before = _count_state(client_id)
    recovery_result = {"client_id": client_id, "watchers_requeued": 0, "errors": []}
    ok = True
    error = None

    if not dry_run and warnings:
        ok = False
        error = ",".join(warnings)
    elif not dry_run:
        try:
            from ap_recovery import APStartupRecovery

            recovery = APStartupRecovery(
                client_id=client_id,
                broker=broker,
                osm=osm,
                pm=pm,
                master_control=mc,
                exit_engine=exit_engine,
                entry_watcher=entry_watcher,
            )
            recovery._reseed_watchers(recovery_result)
        except Exception as exc:  # noqa: BLE001
            ok = False
            error = str(exc)
            recovery_result.setdefault("errors", []).append(str(exc))
            log.error(
                "morning_handoff failed client=%s execution_mode=%s stage=%s err=%s",
                client_id, mode, stage, exc, exc_info=True,
            )

    after = _count_state(client_id)
    details = {
        "dry_run": dry_run,
        "before": before,
        "after": after,
        "watchers_requeued": int(recovery_result.get("watchers_requeued", 0) or 0),
        "warnings": warnings,
        "errors": list(recovery_result.get("errors") or []),
    }
    _upsert_handoff_run_lock(
        client_id=client_id,
        execution_mode=mode,
        trading_date=trading_date,
        stage=stage,
        status="success" if ok else "failed",
        last_error=error,
        details=details,
        mark_success=ok,
    )
    return {
        "ok": ok,
        "client_id": client_id,
        "execution_mode": mode,
        "stage": stage,
        "trading_date": trading_date,
        "dry_run": dry_run,
        "watchers_requeued": int(recovery_result.get("watchers_requeued", 0) or 0),
        "before": before,
        "after": after,
        "warnings": warnings,
        "errors": list(recovery_result.get("errors") or []),
        "error": error,
    }
