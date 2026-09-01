from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo


log = logging.getLogger("ap.preopen_readiness")

ET = ZoneInfo("America/New_York")
_TABLE_READY = False

PROCESSING_STALE_MINUTES = int(os.getenv("PREOPEN_PROCESSING_STALE_MINUTES", "10"))
WATCHING_ORPHAN_GRACE_MINUTES = int(os.getenv("PREOPEN_WATCHING_ORPHAN_GRACE_MINUTES", "5"))
PENDING_TRIGGER_LOOKBACK_HOURS = int(os.getenv("STARTUP_WATCHER_RESEED_LOOKBACK_HOURS", "48"))
READINESS_ENFORCEMENT_START_HOUR_ET = int(os.getenv("PREOPEN_READINESS_START_HOUR_ET", "9"))
READINESS_ENFORCEMENT_START_MINUTE_ET = int(os.getenv("PREOPEN_READINESS_START_MINUTE_ET", "0"))
READINESS_ENFORCEMENT_END_HOUR_ET = int(os.getenv("PREOPEN_READINESS_END_HOUR_ET", "10"))
READINESS_ENFORCEMENT_END_MINUTE_ET = int(os.getenv("PREOPEN_READINESS_END_MINUTE_ET", "0"))
OVERNIGHT_REEVAL_DUE_HOUR_ET = int(os.getenv("OVERNIGHT_REEVAL_DUE_HOUR_ET", "9"))
OVERNIGHT_REEVAL_DUE_MINUTE_ET = int(os.getenv("OVERNIGHT_REEVAL_DUE_MINUTE_ET", "18"))


def _now_et(now: datetime | None = None) -> datetime:
    return (now or datetime.now(ET)).astimezone(ET)


def _trading_date(now: datetime | None = None) -> str:
    return _now_et(now).date().isoformat()


def _normalize_mode(value: str | None) -> str:
    return str(value or "").strip().lower()


def _nyse_is_trading_day(dt: datetime) -> bool:
    """Route through the canonical NYSE calendar in ap.flatline_alarm.

    Fail-safe: on calendar import failure, fall back to weekday-only. This
    preserves prior behavior for environments where the calendar module is
    unavailable, while every deployment that carries flatline_alarm (all
    production pods) uses the authoritative holiday-aware truth.
    """
    try:
        from ap.flatline_alarm import is_trading_day as _nyse
        return bool(_nyse(dt.date()))
    except Exception:
        return dt.weekday() < 5  # legacy fallback


def _after_929_et(now: datetime | None = None) -> bool:
    dt = _now_et(now)
    if not _nyse_is_trading_day(dt):
        return False
    return dt.hour > 9 or (dt.hour == 9 and dt.minute >= 29)


def _overnight_reeval_due(now: datetime | None = None) -> bool:
    dt = _now_et(now)
    if not _nyse_is_trading_day(dt):
        return False
    current = dt.hour * 60 + dt.minute
    due = OVERNIGHT_REEVAL_DUE_HOUR_ET * 60 + OVERNIGHT_REEVAL_DUE_MINUTE_ET
    return current >= due


def _is_market_day(now: datetime | None = None) -> bool:
    return _nyse_is_trading_day(_now_et(now))


def _readiness_enforcement_active(now: datetime | None = None) -> bool:
    dt = _now_et(now)
    if not _is_market_day(dt):
        return False
    current = dt.hour * 60 + dt.minute
    start = READINESS_ENFORCEMENT_START_HOUR_ET * 60 + READINESS_ENFORCEMENT_START_MINUTE_ET
    end = READINESS_ENFORCEMENT_END_HOUR_ET * 60 + READINESS_ENFORCEMENT_END_MINUTE_ET
    return start <= current <= end


def _pod_mode() -> str:
    return _normalize_mode(os.getenv("BOT_MODE", os.getenv("MODE", "paper")))


def _ensure_preopen_table() -> None:
    global _TABLE_READY
    if _TABLE_READY:
        return
    from ap.db import conn, run_with_retry

    def _create():
        with conn() as c:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS preopen_readiness_runs (
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


def _load_preopen_row(*, client_id: str, execution_mode: str, trading_date: str, stage: str) -> dict | None:
    from ap.db import conn, run_with_retry

    _ensure_preopen_table()

    def _load():
        with conn() as c:
            c.execute(
                """
                SELECT client_id, execution_mode, trading_date::text AS trading_date,
                       stage, status, last_run_at::text AS last_run_at,
                       last_success_at::text AS last_success_at,
                       last_error, details
                FROM preopen_readiness_runs
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


def _upsert_preopen_row(
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

    _ensure_preopen_table()

    def _write():
        with conn() as c:
            c.execute(
                """
                INSERT INTO preopen_readiness_runs (
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
                        ELSE preopen_readiness_runs.last_success_at
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


def _latest_preopen_rows(trading_date: str) -> list[dict]:
    from ap.db import conn, run_with_retry

    _ensure_preopen_table()

    def _load():
        with conn() as c:
            c.execute(
                """
                SELECT client_id, execution_mode, trading_date::text AS trading_date,
                       stage, status, last_run_at::text AS last_run_at,
                       last_success_at::text AS last_success_at,
                       last_error, details
                FROM preopen_readiness_runs
                WHERE trading_date = %s::date
                ORDER BY execution_mode, client_id, updated_at DESC
                """,
                (trading_date,),
            )
            rows = c.fetchall() or []
            cols = [d[0] for d in getattr(c, "description", [])]
            return [dict(r) if isinstance(r, dict) else dict(zip(cols, r)) for r in rows]

    try:
        return run_with_retry(_load) or []
    except Exception:
        return []


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


def _broker_credentials_present(runner, execution_mode: str) -> tuple[bool, dict]:
    member = getattr(runner, "member", None) or {}
    broker = getattr(getattr(runner, "core", None), "broker", None)
    broker_cfg = getattr(broker, "cfg", None)
    base_url = str(
        getattr(runner, "base_url", None)
        or getattr(broker, "base_url", None)
        or getattr(broker_cfg, "base_url", None)
        or ""
    )
    account_id = str(
        getattr(runner, "account_id", None)
        or getattr(broker, "account_id", None)
        or getattr(broker_cfg, "account_id", None)
        or ""
    )

    token_candidates: list[tuple[str, Any]] = [
        ("runner._resolved_tradier_token", getattr(runner, "_resolved_tradier_token", None)),
        ("broker.access_token", getattr(broker, "access_token", None)),
        ("broker._access_token", getattr(broker, "_access_token", None)),
        ("broker.cfg.access_token", getattr(broker_cfg, "access_token", None)),
    ]
    if execution_mode == "live":
        token_candidates.extend([
            ("member.tradier_live_access_token", member.get("tradier_live_access_token")),
            ("env.TRADIER_ACCESS_TOKEN", os.getenv("TRADIER_ACCESS_TOKEN")),
        ])
    else:
        token_candidates.extend([
            ("member.tradier_paper_access_token", member.get("tradier_paper_access_token")),
            ("member.tradier_access_token", member.get("tradier_access_token")),
            ("env.TRADIER_ACCESS_TOKEN", os.getenv("TRADIER_ACCESS_TOKEN")),
        ])

    token_sources = [
        name for name, value in token_candidates
        if str(value or "").strip()
    ]
    token_state = "configured" if token_sources else "missing"
    status = "configured" if (base_url and account_id and token_sources) else "missing"

    return status == "configured", {
        "base_url": base_url,
        "account_id_present": bool(account_id),
        "token_state": token_state,
        "token_present": bool(token_sources),
        "token_sources": token_sources,
        "credential_status": status,
        "execution_mode": execution_mode,
    }


def _selector_identity(runner) -> dict:
    selector = getattr(runner, "contract_selector", None)
    brk = getattr(selector, "data_broker", None) or getattr(selector, "broker", None) or getattr(getattr(runner, "core", None), "broker", None)
    base_url = (
        getattr(getattr(brk, "cfg", None), "base_url", None)
        or getattr(brk, "base_url", None)
        or ""
    )
    base_url = str(base_url)
    sandbox = "sandbox" in base_url.lower()
    if not base_url:
        source = "unknown"
    elif sandbox:
        source = "tradier_sandbox"
    else:
        source = "tradier_live"
    return {
        "quote_source": source,
        "chain_source": "tradier_options_chain" if base_url else "unknown",
        "tradier_base_url": base_url,
        "sandbox_mode": bool(sandbox),
    }


def _morning_handoff_success_exists(client_id: str, execution_mode: str, trading_date: str) -> bool:
    from ap.morning_handoff import _latest_handoff_rows

    rows = _latest_handoff_rows(trading_date)
    for row in rows:
        if (
            str(row.get("client_id") or "").strip() == client_id
            and _normalize_mode(row.get("execution_mode")) == execution_mode
            and str(row.get("status") or "").lower() == "success"
            and row.get("last_success_at")
        ):
            return True
    return False


def _post_overnight_reeval_success_exists(client_id: str, execution_mode: str, trading_date: str) -> bool:
    from ap.morning_handoff import _latest_handoff_rows

    rows = _latest_handoff_rows(trading_date)
    for row in rows:
        if (
            str(row.get("client_id") or "").strip() == client_id
            and _normalize_mode(row.get("execution_mode")) == execution_mode
            and str(row.get("stage") or "").strip().lower() == "post_overnight_reeval"
            and str(row.get("status") or "").strip().lower() == "success"
            and row.get("last_success_at")
        ):
            return True
    return False


def _query_client_state(client_id: str) -> dict:
    from ap.db import conn, run_with_retry

    now_utc = datetime.now(timezone.utc)
    processing_cutoff = now_utc - timedelta(minutes=PROCESSING_STALE_MINUTES)
    watching_cutoff = now_utc - timedelta(minutes=WATCHING_ORPHAN_GRACE_MINUTES)
    pending_cutoff = now_utc - timedelta(hours=PENDING_TRIGGER_LOOKBACK_HOURS)

    def _load():
        with conn() as c:
            c.execute(
                """
                SELECT id
                FROM trade_queue
                WHERE client_id = %s
                  AND status = 'PROCESSING'
                  AND COALESCE(started_ts, created_ts) < %s
                ORDER BY id
                """,
                (client_id, processing_cutoff),
            )
            stale_processing = [r[0] if not isinstance(r, dict) else r.get("id") for r in (c.fetchall() or [])]

            c.execute(
                """
                SELECT q.id, q.signal_id
                FROM trade_queue q
                WHERE q.client_id = %s
                  AND q.status = 'WATCHING'
                  AND q.created_ts < %s
                  AND NOT EXISTS (
                      SELECT 1
                      FROM orders o
                      WHERE o.client_id = q.client_id
                        AND COALESCE(o.signal_id, '') = COALESCE(q.signal_id, '')
                        AND o.kind = 'ENTRY'
                  )
                ORDER BY q.id
                """,
                (client_id, watching_cutoff),
            )
            watching_orphans = []
            for row in (c.fetchall() or []):
                if isinstance(row, dict):
                    watching_orphans.append({"id": row.get("id"), "signal_id": row.get("signal_id")})
                else:
                    watching_orphans.append({"id": row[0], "signal_id": row[1]})

            c.execute(
                """
                SELECT local_order_id,
                       signal_id,
                       meta->>'overnight_source_table' AS overnight_source_table,
                       meta->>'overnight_source_job_id' AS overnight_source_job_id,
                       meta->>'overnight_source_signal_id' AS overnight_source_signal_id
                FROM orders
                WHERE client_id = %s
                  AND kind = 'ENTRY'
                  AND status = 'PENDING_TRIGGER'
                  AND created_ts >= %s
                  AND broker_order_id IS NULL
                  AND submitted_ts IS NULL
                  AND filled_ts IS NULL
                ORDER BY created_ts
                """,
                (client_id, pending_cutoff),
            )
            pending_trigger = []
            for row in (c.fetchall() or []):
                if isinstance(row, dict):
                    pending_trigger.append({
                        "local_order_id": row.get("local_order_id"),
                        "signal_id": row.get("signal_id"),
                        "overnight_source_table": row.get("overnight_source_table"),
                        "overnight_source_job_id": row.get("overnight_source_job_id"),
                        "overnight_source_signal_id": row.get("overnight_source_signal_id"),
                    })
                else:
                    pending_trigger.append({
                        "local_order_id": row[0],
                        "signal_id": row[1],
                        "overnight_source_table": row[2],
                        "overnight_source_job_id": row[3],
                        "overnight_source_signal_id": row[4],
                    })

            c.execute(
                """
                SELECT COUNT(*)::int AS n
                FROM trade_queue
                WHERE client_id = %s
                  AND status = 'WATCHING'
                """,
                (client_id,),
            )
            watch_count_row = c.fetchone()
            watching_count = watch_count_row["n"] if isinstance(watch_count_row, dict) else (watch_count_row[0] if watch_count_row else 0)

            return {
                "stale_processing_ids": stale_processing,
                "watching_orphans": watching_orphans,
                "pending_trigger_rows": pending_trigger,
                "watching_count": int(watching_count or 0),
            }

    return run_with_retry(_load) or {
        "stale_processing_ids": [],
        "watching_orphans": [],
        "pending_trigger_rows": [],
        "watching_count": 0,
    }


def _pending_trigger_without_watcher(runner, pending_rows: list[dict]) -> list[dict]:
    entry_watcher = getattr(getattr(runner, "core", None), "entry_watcher", None)
    if entry_watcher is None or not hasattr(entry_watcher, "has_order"):
        return list(pending_rows or [])
    out = []
    for row in pending_rows or []:
        local_order_id = str(row.get("local_order_id") or "").strip()
        if not local_order_id:
            out.append(row)
            continue
        try:
            if not entry_watcher.has_order(local_order_id):
                out.append(row)
        except Exception:
            out.append(row)
    return out


def _overnight_status(
    runner,
    client_state: dict,
    trading_date: str,
    *,
    client_id: str,
    execution_mode: str,
    stage: str = "",
    now: datetime | None = None,
) -> tuple[str, dict]:
    success_date = getattr(runner, "_overnight_reeval_success_date", None)
    if str(success_date or "") == trading_date:
        return "success", {"source": "runner_overnight_reeval_success_date"}
    current_state_date = getattr(runner, "_overnight_reeval_state_date", None)
    if str(current_state_date or "") == trading_date:
        if str(stage or "").strip().lower() == "startup" and not _overnight_reeval_due(now):
            return "pending", {"source": "startup_before_overnight_reeval_due"}
        return "missing", {"source": "runner_overnight_reeval_not_successful_today"}
    if _post_overnight_reeval_success_exists(client_id, execution_mode, trading_date):
        return "success", {"source": "handoff_run_locks.post_overnight_reeval"}
    if int(client_state.get("watching_count", 0) or 0) == 0 and not client_state.get("pending_trigger_rows"):
        return "explicit_noop", {"source": "no_watching_or_pending_trigger_rows"}
    if str(stage or "").strip().lower() == "startup" and not _overnight_reeval_due(now):
        return "pending", {"source": "startup_before_overnight_reeval_due"}
    return "missing", {"source": "watching_or_pending_trigger_present_without_overnight_success"}


def _stage_success_recent(existing: dict | None) -> bool:
    return bool(existing and str(existing.get("status") or "").lower() == "success" and existing.get("last_success_at"))


def run_preopen_autonomous_readiness(
    client_id: str,
    execution_mode: str,
    *,
    dry_run: bool = True,
    repair: bool = False,
    stage: str = "manual",
    runner=None,
    now: datetime | None = None,
) -> dict:
    mode = _normalize_mode(execution_mode)
    client_id = str(client_id or "").strip()
    stage = str(stage or "manual").strip().lower()
    trading_date = _trading_date(now)

    if not client_id:
        return {"ok": False, "status": "BLOCKED", "error": "client_id_required"}
    if mode not in {"paper", "live"}:
        return {"ok": False, "status": "BLOCKED", "error": "invalid_execution_mode", "client_id": client_id}

    runner = runner or _resolve_runner(client_id)
    _upsert_preopen_row(
        client_id=client_id,
        execution_mode=mode,
        trading_date=trading_date,
        stage=stage,
        status="running",
        last_error=None,
        details={"dry_run": dry_run, "repair": repair},
        mark_success=False,
    )

    errors: list[str] = []
    warnings: list[str] = []
    details: dict[str, Any] = {"dry_run": dry_run, "repair": repair}

    if runner is None:
        errors.append("runner_not_found")
        result = {
            "ok": False,
            "status": "BLOCKED",
            "client_id": client_id,
            "execution_mode": mode,
            "stage": stage,
            "trading_date": trading_date,
            "errors": errors,
            "warnings": warnings,
            "details": details,
        }
        _upsert_preopen_row(
            client_id=client_id,
            execution_mode=mode,
            trading_date=trading_date,
            stage=stage,
            status="blocked",
            last_error="runner_not_found",
            details=result,
            mark_success=False,
        )
        return result

    initialized = bool(getattr(runner, "initialized", None) and runner.initialized.is_set())
    worker_alive = bool(getattr(runner, "worker_thread", None) and runner.worker_thread.is_alive())
    runner_alive = bool(hasattr(runner, "is_alive") and runner.is_alive())
    osm_present = getattr(runner, "order_state_machine", None) is not None
    entry_watcher_present = getattr(getattr(runner, "core", None), "entry_watcher", None) is not None
    mc_present = getattr(runner, "master_control", None) is not None

    details.update({
        "runner_alive": runner_alive,
        "runner_initialized": initialized,
        "queue_worker_alive": worker_alive,
        "order_state_machine_present": osm_present,
        "entry_watcher_present": entry_watcher_present,
        "master_control_present": mc_present,
    })

    if not runner_alive:
        errors.append("runner_not_alive")
    if not initialized:
        errors.append("runner_not_initialized")
    if not worker_alive:
        errors.append("queue_worker_not_alive")
    if not osm_present:
        errors.append("order_state_machine_missing")
    if not entry_watcher_present:
        errors.append("entry_watcher_missing")
    if not mc_present:
        errors.append("master_control_missing")

    pod_mode = _pod_mode()
    actual_mode = _normalize_mode(getattr(runner, "mode", None) or getattr(getattr(runner, "master_control", None), "mode", None))
    details["pod_mode"] = pod_mode
    details["runner_mode"] = actual_mode
    if actual_mode != mode:
        errors.append("requested_execution_mode_mismatch")
    if pod_mode and actual_mode and pod_mode != actual_mode:
        errors.append("pod_mode_client_mode_mismatch")

    broker_ok, broker_details = _broker_credentials_present(runner, mode)
    details["broker_credentials"] = broker_details
    broker_state = str(broker_details.get("credential_status") or "missing").lower()
    if stage != "startup":
        if not broker_ok and broker_state == "missing":
            errors.append("broker_credentials_missing")
        elif not broker_ok:
            errors.append("broker_credentials_unverified")
    elif not broker_ok:
        warnings.append("broker_credentials_unverified")

    selector_identity = _selector_identity(runner)
    details["selector_identity"] = selector_identity
    if selector_identity["quote_source"] == "unknown" or not selector_identity["tradier_base_url"]:
        errors.append("selector_quote_identity_unresolved")

    client_state = _query_client_state(client_id)
    details["client_state"] = client_state

    if client_state.get("stale_processing_ids"):
        errors.append("stale_processing_rows")

    if client_state.get("watching_orphans"):
        errors.append("watching_rows_missing_orders_recommend_new_rescue")

    unowned_pending = _pending_trigger_without_watcher(runner, client_state.get("pending_trigger_rows") or [])
    details["pending_trigger_without_watcher"] = unowned_pending
    if unowned_pending:
        errors.append("pending_trigger_without_watcher_ownership")

    handoff_ok = _morning_handoff_success_exists(client_id, mode, trading_date)
    details["morning_handoff_success"] = handoff_ok
    if stage != "startup":
        if not handoff_ok:
            if mode == "live" or _after_929_et(now):
                errors.append("morning_handoff_missing")
            else:
                warnings.append("morning_handoff_missing")
    elif not handoff_ok:
        warnings.append("morning_handoff_pending_startup")

    overnight_state, overnight_details = _overnight_status(
        runner,
        client_state,
        trading_date,
        client_id=client_id,
        execution_mode=mode,
        stage=stage,
        now=now,
    )
    if overnight_state == "pending":
        warnings.append("overnight_reeval_pending_startup")
    elif overnight_state == "missing":
        if mode == "live" or _after_929_et(now):
            errors.append("overnight_reeval_missing")
        else:
            warnings.append("overnight_reeval_missing")
    details["overnight_reeval"] = {"status": overnight_state, **overnight_details}

    # PR #388 LIVE blocked_keys hardening
    # ────────────────────────────────────
    # The prior set omitted the two conditions that most directly violate
    # this PR's core promise of verified pre-open watcher ownership:
    #   • overnight_reeval_missing — no overnight watchers armed for this
    #     trading day. Live entries must not be authorized without them;
    #     the whole PR is about restoring that guarantee.
    #   • pending_trigger_without_watcher_ownership — DB rows in
    #     PENDING_TRIGGER with no live in-memory watcher owner. A breach
    #     would never fire; a broker-side fill (if any) would be untracked.
    # Both are BLOCKED for live mode. Paper continues to DEGRADE so paper
    # sessions can still surface diagnostics without freezing.
    blocked_keys = {
        "pod_mode_client_mode_mismatch",
        "requested_execution_mode_mismatch",
        "entry_watcher_missing",
        "morning_handoff_missing",
        "selector_quote_identity_unresolved",
        "runner_not_alive",
        "overnight_reeval_missing",
        "pending_trigger_without_watcher_ownership",
    }
    if mode == "live" and any(err in blocked_keys for err in errors):
        status = "BLOCKED"
    elif errors:
        status = "DEGRADED"
    else:
        status = "OK"

    ok = status == "OK"
    result = {
        "ok": ok,
        "status": status,
        "client_id": client_id,
        "execution_mode": mode,
        "stage": stage,
        "trading_date": trading_date,
        "errors": errors,
        "warnings": warnings,
        "details": details,
    }
    _upsert_preopen_row(
        client_id=client_id,
        execution_mode=mode,
        trading_date=trading_date,
        stage=stage,
        status=status.lower(),
        last_error=";".join(errors) if errors else None,
        details=result,
        mark_success=ok,
    )
    return result


def get_preopen_readiness_health(now: datetime | None = None) -> dict:
    trading_date = _trading_date(now)
    enforcement_active = _readiness_enforcement_active(now)
    rows = _latest_preopen_rows(trading_date)
    expected = _expected_clients_by_mode()
    by_mode: dict[str, dict[str, Any]] = {
        "paper": {"clients": {}, "last_run_at": None, "last_success_at": None, "errors": []},
        "live": {"clients": {}, "last_run_at": None, "last_success_at": None, "errors": []},
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
        details = row.get("details") or {}
        if not isinstance(details, dict):
            details = {}
        by_mode[mode]["clients"][client_id] = {
            "status": row.get("status"),
            "last_run_at": row.get("last_run_at"),
            "last_success_at": row.get("last_success_at"),
            "last_stage": row.get("stage"),
            "last_error": row.get("last_error"),
            "errors": details.get("errors") or [],
        }
        if row.get("last_run_at") and (
            by_mode[mode]["last_run_at"] is None or str(row.get("last_run_at")) > str(by_mode[mode]["last_run_at"])
        ):
            by_mode[mode]["last_run_at"] = row.get("last_run_at")
        if row.get("last_success_at") and (
            by_mode[mode]["last_success_at"] is None or str(row.get("last_success_at")) > str(by_mode[mode]["last_success_at"])
        ):
            by_mode[mode]["last_success_at"] = row.get("last_success_at")
        if row.get("last_error"):
            by_mode[mode]["errors"].append(row.get("last_error"))

    observed_statuses = []
    missing_expected_clients: dict[str, list[str]] = {"paper": [], "live": []}
    for mode, client_ids in expected.items():
        for client_id in client_ids:
            status = str(by_mode[mode]["clients"].get(client_id, {}).get("status") or "missing").upper()
            if status == "MISSING":
                missing_expected_clients[mode].append(client_id)
            else:
                observed_statuses.append(status)

    if any(s == "BLOCKED" for s in observed_statuses):
        overall = "BLOCKED"
    elif any(s == "DEGRADED" for s in observed_statuses):
        overall = "DEGRADED"
    elif enforcement_active and any(missing_expected_clients.values()):
        overall = "DEGRADED"
    else:
        overall = "OK"

    return {
        "status": overall,
        "enforcement_active": enforcement_active,
        "trading_date": trading_date,
        "paper": by_mode["paper"],
        "live": by_mode["live"],
        "missing_expected_clients": missing_expected_clients,
        "last_run_at": max(filter(None, [by_mode["paper"]["last_run_at"], by_mode["live"]["last_run_at"]]), default=None),
        "errors": by_mode["paper"]["errors"] + by_mode["live"]["errors"],
    }
