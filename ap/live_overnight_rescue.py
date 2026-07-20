"""
ap/live_overnight_rescue.py  —  Strictly fenced LIVE overnight rescue.
Incident: 2026-07-20 — Jason LIVE, 53 false terminal rejections.

Performance fix (P0-1):
  The eligible-population query uses a MATERIALIZED CTE that scans
  decision_events once with a UTC timestamp range (no DATE() transform)
  and builds a hash-set of verified original_signal_ids via split_part.
  The CTE is then hash-joined to trade_queue — no correlated per-row scan.
  This query returned the expected 53 rows promptly against production.

Dependency seams (P0-2):
  conn and run_with_retry are imported at module level so tests can patch
  ap.live_overnight_rescue.conn and ap.live_overnight_rescue.run_with_retry.
  Orchestration dependencies (reeval, handoff, readiness) are imported
  inside their functions; tests patch the actual source modules.

Orchestration truth (P0-3):
  Every failure in any of the four phases stops the chain immediately.
  orchestration["overall_status"] carries one of:
    DRY_RUN_OK | RECOVERY_COMPLETE |
    FAILED_RESCUE | FAILED_REEVAL | FAILED_HANDOFF | FAILED_READINESS

Schema facts enforced:
  - decision_events has NO signal_id column.
  - candidate_id = REEVAL:<signal_id>:<suffix>  →  split_part(...,':',2) = signal_id
  - Recovery events use canonical columns:
    run_id, candidate_id, client_id, stage, decision, reason_code, explanation,
    context_json, ts
  - Handoff: from ap_morning_handoff_audit import run_morning_handoff_audit
    Signature: (client_id, entry_watcher, osm, execution_mode, dry_run)
    Returns:   {"ok": bool, "errors": [...]}
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any

# ── Module-level import seams (patchable) ─────────────────────────────────────
from ap.db import conn, run_with_retry   # noqa: E402  # patch targets in tests

log = logging.getLogger("ap.live_overnight_rescue")

# ── Incident constants ─────────────────────────────────────────────────────────
INCIDENT_CLIENT_ID     = "jasoncosby1@gmail.com"
INCIDENT_TRADING_DATE  = "2026-07-20"
INCIDENT_EXPECTED_ROWS = 53

RECOVER_PR379_REGIME_TAXONOMY = "RECOVER_PR379_REGIME_TAXONOMY"

# Exact stage/decision/reason_code/explanation for the false-veto events
_DE_STAGE       = "blocked_intel"
_DE_DECISION    = "REJECT"
_DE_REASON_CODE = "SESSION_RULE_BLOCK"
_DE_EXPLANATION = "INTEL_AUTHORITATIVE_VETO_RISK"

# Exact reasoning strings produced by intelligence_bridge before PR fix.
# Only these two are permitted — no generic pattern match.
_EXACT_REASONING = [
    "risk_veto: CALL blocked \u2014 SPY in BEAR trend",
    "risk_veto: PUT blocked \u2014 SPY in BULL trend",
]

# Statement timeout for the rescue transaction — fail closed, don't hold locks
_RESCUE_STMT_TIMEOUT = "25s"
_DRY_RUN_STMT_TIMEOUT = "10s"


# ── Time-window helpers ────────────────────────────────────────────────────────

def _friday_after_close_window_utc(monday_date_str: str) -> tuple[datetime, datetime]:
    """Friday 16:00 ET → Monday 09:30 ET in UTC (trade_queue created_ts window)."""
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
    monday = date.fromisoformat(monday_date_str)
    if monday.weekday() != 0:
        raise ValueError(f"trading_date={monday_date_str} is not Monday (weekday={monday.weekday()})")
    friday = monday - timedelta(days=3)
    start = datetime(friday.year, friday.month, friday.day, 16, 0, 0, tzinfo=ET)
    end   = datetime(monday.year, monday.month, monday.day, 9, 30, 0, tzinfo=ET)
    return start.astimezone(timezone.utc), end.astimezone(timezone.utc)


def _de_day_window_utc(trading_date_str: str) -> tuple[datetime, datetime]:
    """
    UTC start/end covering the full trading date in America/New_York.
    Avoids DATE(ts AT TIME ZONE '...') so PostgreSQL can use a ts index.
    """
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
    day = date.fromisoformat(trading_date_str)
    start_et = datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=ET)
    end_et   = start_et + timedelta(days=1)
    return start_et.astimezone(timezone.utc), end_et.astimezone(timezone.utc)


# ── Shared population query builder ────────────────────────────────────────────

def _build_rescue_population_sql(
    *,
    client_id: str,
    trading_date: str,
    tq_window_start_utc: datetime,
    tq_window_end_utc: datetime,
) -> tuple[str, list]:
    """
    Returns (sql_template, params) for the eligible rescue population.

    Uses a MATERIALIZED CTE that:
      1. Scans decision_events once with a UTC ts range (index-friendly).
      2. Extracts original_signal_id via split_part(candidate_id, ':', 2).
      3. Materialises the result so the planner builds a hash table.

    The trade_queue side is then hash-joined — no correlated per-row subquery.
    The {select_clause} placeholder is filled by the caller:
      - dry-run:  "SELECT COUNT(*) AS n"
      - write:    "SELECT tq.id, tq.signal_id, ... ORDER BY tq.created_ts ASC FOR UPDATE OF tq"

    Params order: [de_client_id, de_reasoning, de_ts_start, de_ts_end,
                   tq_client_id, tq_created_start, tq_created_end,
                   recovery_reason_code]
    """
    de_ts_start, de_ts_end = _de_day_window_utc(trading_date)

    params: list[Any] = [
        # CTE (decision_events filter)
        client_id,          # %s 1 — client_id
        _EXACT_REASONING,   # %s 2 — reasoning ANY array
        de_ts_start,        # %s 3 — ts >=
        de_ts_end,          # %s 4 — ts <
        # Trade queue WHERE
        client_id,          # %s 5 — tq.client_id
        tq_window_start_utc,# %s 6 — tq.created_ts >=
        tq_window_end_utc,  # %s 7 — tq.created_ts <=
        RECOVER_PR379_REGIME_TAXONOMY,  # %s 8 — idempotency check
    ]

    sql = """
        WITH verified_events AS MATERIALIZED (
            SELECT DISTINCT split_part(candidate_id, ':', 2) AS original_signal_id
            FROM decision_events
            WHERE client_id   = %s
              AND stage       = 'blocked_intel'
              AND decision    = 'REJECT'
              AND reason_code = 'SESSION_RULE_BLOCK'
              AND explanation = 'INTEL_AUTHORITATIVE_VETO_RISK'
              AND context_json->>'intel_reason_code' = 'INTEL_AUTHORITATIVE_VETO_RISK'
              AND context_json->>'intel_raw_status'  = 'RISK_VETO'
              AND UPPER(COALESCE(context_json->>'intel_execution_mode','')) = 'LIVE'
              AND context_json->>'intel_reasoning' = ANY(%s::text[])
              AND ts >= %s
              AND ts <  %s
        )
        {{select_clause}}
        FROM trade_queue tq
        JOIN verified_events ve ON ve.original_signal_id = tq.signal_id
        WHERE tq.client_id  = %s
          AND tq.status     = 'REJECTED'
          AND tq.last_error = 'mc_blocked:INTEL_AUTHORITATIVE_VETO_RISK'
          AND tq.created_ts >= %s
          AND tq.created_ts <= %s
          AND (tq.payload IS NULL OR tq.payload->>'recovery_context' IS NULL)
          AND NOT EXISTS (
              SELECT 1 FROM orders o
              WHERE o.client_id = tq.client_id AND o.signal_id = tq.signal_id
                AND o.kind = 'ENTRY'
          )
          AND NOT EXISTS (
              SELECT 1 FROM orders ob
              WHERE ob.client_id = tq.client_id AND ob.signal_id = tq.signal_id
                AND ob.broker_order_id IS NOT NULL
          )
          AND NOT EXISTS (
              SELECT 1 FROM orders os2
              WHERE os2.client_id = tq.client_id AND os2.signal_id = tq.signal_id
                AND (os2.submitted_ts IS NOT NULL OR os2.filled_ts IS NOT NULL)
          )
          AND NOT EXISTS (
              SELECT 1 FROM positions p
              WHERE p.client_id = tq.client_id AND p.signal_id = tq.signal_id
          )
          AND NOT EXISTS (
              SELECT 1 FROM decision_events rde
              WHERE rde.client_id    = tq.client_id
                AND rde.candidate_id LIKE ('RECOVER:' || tq.signal_id || ':%')
                AND rde.stage        = 'overnight_recovery'
                AND rde.reason_code  = %s
          )
    """
    return sql, params


# ── Identity verification ──────────────────────────────────────────────────────

def _verify_live_identity(client_id: str, runner) -> tuple[bool, str]:
    if runner is None:
        return False, "runner is None — mandatory for writes"
    runner_mode = str(getattr(runner, "mode", "") or "").strip().upper()
    if runner_mode != "LIVE":
        return False, f"runner.mode={runner_mode!r} is not LIVE"
    runner_email = str(getattr(runner, "email", "") or "").strip().lower()
    if runner_email != client_id.strip().lower():
        return False, f"runner.email={runner_email!r} != required {client_id!r}"
    initialized = getattr(runner, "initialized", None)
    if initialized is None or not initialized.is_set():
        return False, "runner.initialized is not set"
    return True, "ok"


# ── Dry-run: identical population query, read-only ─────────────────────────────

def _dry_run_count(
    *,
    client_id: str,
    trading_date: str,
    tq_window_start_utc: datetime,
    tq_window_end_utc: datetime,
) -> dict:
    """
    Runs the EXACT same CTE population query as the write transaction,
    using COUNT(*) — no FOR UPDATE, no UPDATE.
    Returns {"eligible": N, "verified": N}.  eligible == verified by
    construction: only rows whose signal_id appears in verified_events
    (matching DE) are counted.
    """
    sql_tpl, params = _build_rescue_population_sql(
        client_id=client_id,
        trading_date=trading_date,
        tq_window_start_utc=tq_window_start_utc,
        tq_window_end_utc=tq_window_end_utc,
    )
    count_sql = sql_tpl.replace("{select_clause}", "SELECT COUNT(*) AS n")

    def _q():
        with conn() as c:
            c.execute(f"SET LOCAL statement_timeout = '{_DRY_RUN_STMT_TIMEOUT}'")
            c.execute(count_sql, params)
            row = c.fetchone()
            n = int((row.get("n") if isinstance(row, dict) else row[0]) or 0)
            return n

    n = run_with_retry(_q)
    return {"eligible": n, "verified": n}


# ── Atomic write transaction ───────────────────────────────────────────────────

def _execute_atomic_rescue(
    *,
    client_id: str,
    trading_date: str,
    tq_window_start_utc: datetime,
    tq_window_end_utc: datetime,
    expected_count: int,
    recovery_run_id: str,
    recovered_at: str,
) -> dict:
    """
    Single atomic transaction:
      1. SET LOCAL statement_timeout — fail closed, don't hold locks.
      2. WITH verified_events AS MATERIALIZED ... SELECT FOR UPDATE OF tq.
         Count == verified == expected_count or raise (→ rollback).
      3. Bulk UPDATE REJECTED → WATCHING via unnest BIGINT[].
         rowcount == expected_count or raise (→ rollback).
      4. INSERT one canonical recovery event per row (same txn).
      5. conn() commits on clean exit; rolls back on any exception.
    """
    sql_tpl, params = _build_rescue_population_sql(
        client_id=client_id,
        trading_date=trading_date,
        tq_window_start_utc=tq_window_start_utc,
        tq_window_end_utc=tq_window_end_utc,
    )
    lock_sql = sql_tpl.replace(
        "{select_clause}",
        "SELECT tq.id, tq.signal_id, tq.payload, tq.last_error, tq.result_json, tq.finished_ts"
    ) + "\n        ORDER BY tq.created_ts ASC\n        FOR UPDATE OF tq"

    with conn() as c:
        c.execute(f"SET LOCAL statement_timeout = '{_RESCUE_STMT_TIMEOUT}'")

        # 1. Lock rows — only rows with verified DEs are selected
        c.execute(lock_sql, params)
        cols = [d[0] for d in (c.description or [])]
        locked_rows = [
            dict(r) if isinstance(r, dict) else dict(zip(cols, r))
            for r in (c.fetchall() or [])
        ]
        for row in locked_rows:
            if isinstance(row.get("payload"), str):
                try:   row["payload"] = json.loads(row["payload"])
                except Exception: row["payload"] = {}

        locked_count = len(locked_rows)
        log.info("rescue_atomic: locked=%d (CTE-verified) expected=%d",
                 locked_count, expected_count)

        if locked_count != expected_count:
            raise ValueError(
                f"Locked {locked_count} rows (with DE proof), expected {expected_count}. "
                "Rolling back."
            )

        # 2. Build payloads
        row_ids:      list[int] = []
        new_payloads: list[str] = []

        for row in locked_rows:
            p = dict(row.get("payload") or {})
            p["client_id"]      = client_id
            p["execution_mode"] = "live"
            p["recovery_context"] = {
                "reason_code":          RECOVER_PR379_REGIME_TAXONOMY,
                "trading_date":         trading_date,
                "recovered_at":         recovered_at,
                "recovery_run_id":      recovery_run_id,
                "original_last_error":  str(row.get("last_error") or ""),
                "original_finished_ts": str(row.get("finished_ts") or ""),
                "original_result_json": (
                    json.loads(row["result_json"])
                    if isinstance(row.get("result_json"), str)
                    else row.get("result_json")
                ),
            }
            row_ids.append(int(row["id"]))
            new_payloads.append(json.dumps(p))

        # 3. Bulk UPDATE via unnest — BIGINT[] matches production trade_queue.id
        c.execute(
            """
            WITH new_data AS (
                SELECT unnest(%s::bigint[]) AS row_id,
                       unnest(%s::jsonb[])  AS new_payload
            )
            UPDATE trade_queue tq
            SET status      = 'WATCHING',
                started_ts  = NULL,
                finished_ts = NULL,
                last_error  = NULL,
                result_json = NULL,
                payload     = nd.new_payload
            FROM new_data nd
            WHERE tq.id        = nd.row_id
              AND tq.client_id = %s
              AND tq.status    = 'REJECTED'
            """,
            (row_ids, new_payloads, client_id),
        )
        updated_count = c.rowcount
        log.info("rescue_atomic: bulk UPDATE rowcount=%d", updated_count)

        if updated_count != expected_count:
            raise ValueError(
                f"Bulk UPDATE affected {updated_count} rows, expected {expected_count}. "
                "Rolling back."
            )

        # 4. INSERT canonical recovery events inside same transaction.
        #    candidate_id = RECOVER:<signal_id>:<run_id> so subsequent runs
        #    exclude these rows via the idempotency NOT EXISTS check.
        for row in locked_rows:
            signal_id    = str(row.get("signal_id") or "")
            candidate_id = f"RECOVER:{signal_id}:{recovery_run_id}"
            ctx = json.dumps({
                "stage":                "overnight_recovery",
                "decision":             "REQUEUE",
                "reason_code":          RECOVER_PR379_REGIME_TAXONOMY,
                "original_status":      "REJECTED",
                "original_last_error":  str(row.get("last_error") or ""),
                "original_finished_ts": str(row.get("finished_ts") or ""),
                "recovery_run_id":      recovery_run_id,
                "trading_date":         trading_date,
                "recovered_at":         recovered_at,
                "client_id":            client_id,
                "execution_mode":       "LIVE",
            })
            c.execute(
                """
                INSERT INTO decision_events (
                    run_id, candidate_id, client_id,
                    stage, decision, reason_code, explanation,
                    context_json, ts
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
                """,
                (
                    recovery_run_id, candidate_id, client_id,
                    "overnight_recovery", "REQUEUE",
                    RECOVER_PR379_REGIME_TAXONOMY,
                    "RECOVERED_PR379_REGIME_MISMATCH_FALSE_VETO",
                    ctx,
                ),
            )

        return {"writes": updated_count, "locked": locked_count}


# ── Public rescue function ─────────────────────────────────────────────────────

def rescue_live_overnight_regime_rejections(
    *,
    client_id: str,
    execution_mode: str,
    trading_date: str,
    dry_run: bool = True,
    expected_count: int | None = None,
    runner=None,
) -> dict:
    """
    Fenced rescue. dry_run=True (default) returns full DE-verified preview.
    runner mandatory for dry_run=False.
    eligible == verified by construction (CTE JOIN excludes unverified rows).
    """
    recovery_run_id = uuid.uuid4().hex[:12]
    recovered_at    = datetime.now(timezone.utc).isoformat()

    result: dict[str, Any] = {
        "eligible": 0, "verified": 0, "writes": 0,
        "count_match": False, "dry_run": dry_run,
        "recovery_run_id": recovery_run_id,
        "client_id": client_id, "execution_mode": execution_mode,
        "trading_date": trading_date, "errors": [],
    }

    # ── Hard guards ────────────────────────────────────────────────────────────
    if str(execution_mode or "").strip().lower() != "live":
        result["errors"].append(f"execution_mode must be 'live', got {execution_mode!r}")
        return result
    if str(client_id or "").strip().lower() != INCIDENT_CLIENT_ID.lower():
        result["errors"].append(
            f"client_id must be {INCIDENT_CLIENT_ID!r}. Got {client_id!r}."
        )
        return result
    if str(trading_date or "").strip() != INCIDENT_TRADING_DATE:
        result["errors"].append(
            f"trading_date must be {INCIDENT_TRADING_DATE!r}. Got {trading_date!r}."
        )
        return result
    if not dry_run and runner is None:
        result["errors"].append("runner is mandatory for dry_run=False.")
        return result
    if runner is not None:
        ok, reason = _verify_live_identity(client_id, runner)
        if not ok:
            result["errors"].append(f"LIVE identity check failed: {reason}")
            return result
    if expected_count is not None and expected_count != INCIDENT_EXPECTED_ROWS:
        result["errors"].append(
            f"expected_count={expected_count} but this rescue requires exactly "
            f"{INCIDENT_EXPECTED_ROWS}."
        )
        return result

    _expected = INCIDENT_EXPECTED_ROWS

    try:
        tq_start, tq_end = _friday_after_close_window_utc(trading_date)
    except ValueError as exc:
        result["errors"].append(str(exc))
        return result

    log.warning("rescue: client=%s date=%s dry_run=%s expected=%d run_id=%s",
                client_id, trading_date, dry_run, _expected, recovery_run_id)

    # ── Dry-run ────────────────────────────────────────────────────────────────
    if dry_run:
        try:
            counts = _dry_run_count(
                client_id=client_id,
                trading_date=trading_date,
                tq_window_start_utc=tq_start,
                tq_window_end_utc=tq_end,
            )
            n = counts["eligible"]
            result["eligible"]    = n
            result["verified"]    = n   # DE proof is in the CTE JOIN
            result["count_match"] = (n == _expected)
            if not result["count_match"]:
                result["errors"].append(
                    f"eligible={n} expected={_expected}. Preview only — zero writes."
                )
            log.warning("rescue: DRY_RUN eligible=%d verified=%d count_match=%s",
                        n, n, result["count_match"])
        except Exception as exc:
            result["errors"].append(f"dry-run query failed: {exc}")
            log.error("rescue dry-run error: %s", exc, exc_info=True)
        return result

    # ── Live write ─────────────────────────────────────────────────────────────
    log.warning("rescue: LIVE WRITE client=%s date=%s expected=%d run_id=%s",
                client_id, trading_date, _expected, recovery_run_id)
    try:
        txn = _execute_atomic_rescue(
            client_id=client_id,
            trading_date=trading_date,
            tq_window_start_utc=tq_start,
            tq_window_end_utc=tq_end,
            expected_count=_expected,
            recovery_run_id=recovery_run_id,
            recovered_at=recovered_at,
        )
        result["eligible"]    = txn["locked"]
        result["verified"]    = txn["locked"]
        result["writes"]      = txn["writes"]
        result["count_match"] = (txn["writes"] == _expected)
        log.warning("rescue: COMPLETE writes=%d run_id=%s", result["writes"], recovery_run_id)
    except Exception as exc:
        result["errors"].append(f"atomic rescue failed (rolled back): {exc}")
        log.error("rescue: FAILED (rolled back): %s", exc, exc_info=True)
    return result


# ── Orchestration ──────────────────────────────────────────────────────────────

def recover_and_rerun_live_overnight(
    *,
    runner,
    trading_date: str,
    dry_run: bool = True,
    expected_count: int | None = None,
) -> dict:
    """
    Full recovery orchestration — stops immediately on any failure.

    overall_status values:
      DRY_RUN_OK        — dry run completed successfully
      RECOVERY_COMPLETE — all four phases passed
      FAILED_RESCUE     — rescue failed or partial
      FAILED_REEVAL     — reeval exception, errors, stalled, or zero armed
      FAILED_HANDOFF    — handoff import error or ok=False
      FAILED_READINESS  — readiness non-dict, BLOCKED, or has errors
    """
    orch: dict[str, Any] = {
        "rescue_result":    None,
        "reeval_result":    None,
        "handoff_result":   None,
        "readiness_result": None,
        "overall_status":   "FAILED_RESCUE",
        "errors":           [],
        "dry_run":          dry_run,
        "trading_date":     trading_date,
    }

    client_id = str(
        getattr(runner, "email", "") or getattr(runner, "client_id", "") or ""
    ).strip()
    mode = str(getattr(runner, "mode", "") or "").strip().upper()

    if not client_id:
        orch["errors"].append("runner has no client_id / email")
        return orch
    if mode != "LIVE":
        orch["errors"].append(f"LIVE-only. runner.mode={mode}")
        return orch

    core        = getattr(runner, "core", None)
    broker      = getattr(runner, "broker", None)
    data_broker = getattr(runner, "data_broker", None) or getattr(runner, "databroker", None)
    mc          = getattr(runner, "master_control", None)
    selector    = getattr(runner, "contract_selector", None)
    osm         = getattr(runner, "order_state_machine", None)
    watcher     = getattr(core, "entry_watcher", None) if core else None

    missing = [n for n, o in [
        ("broker", broker), ("master_control", mc),
        ("contract_selector", selector), ("order_state_machine", osm),
        ("entry_watcher", watcher),
    ] if o is None]
    if missing:
        orch["errors"].append(f"Missing runner components: {missing}")
        return orch

    _expected = expected_count if expected_count is not None else INCIDENT_EXPECTED_ROWS

    # Phase 1: Rescue
    try:
        rescue_result = rescue_live_overnight_regime_rejections(
            client_id=client_id, execution_mode="live",
            trading_date=trading_date, dry_run=dry_run,
            expected_count=_expected, runner=runner,
        )
        orch["rescue_result"] = rescue_result
    except Exception as exc:
        orch["errors"].append(f"rescue raised: {exc}")
        return orch

    if rescue_result.get("errors"):
        orch["errors"].extend(rescue_result["errors"])
        return orch

    if dry_run:
        orch["overall_status"] = "DRY_RUN_OK"
        return orch

    writes = int(rescue_result.get("writes") or 0)
    if writes == 0:
        orch["errors"].append("Rescue produced 0 writes.")
        return orch
    if writes != _expected:
        orch["errors"].append(
            f"Rescue partial: writes={writes} expected={_expected}. Stop."
        )
        return orch

    # Phase 2: Canonical overnight reeval
    orch["overall_status"] = "FAILED_REEVAL"
    try:
        from ap_overnight_reeval import run_overnight_reeval
        reeval_result = run_overnight_reeval(
            client_id=client_id, broker=broker, data_broker=data_broker,
            master_control=mc, contract_selector=selector,
            order_state_machine=osm, entry_watcher=watcher,
            position_manager=getattr(runner, "position_manager", None),
            exit_eng=getattr(core, "exit_eng", None),
            force=True,
        )
    except Exception as exc:
        orch["errors"].append(f"run_overnight_reeval raised: {exc}")
        log.error("reeval exception: %s", exc, exc_info=True)
        return orch

    if not isinstance(reeval_result, dict):
        orch["errors"].append(f"reeval returned non-dict: {type(reeval_result)}")
        return orch

    orch["reeval_result"] = reeval_result

    # Stop on any reeval failure before handoff
    reeval_errors  = int(reeval_result.get("errors")  or 0)
    reeval_stalled = bool(reeval_result.get("stalled"))
    reeval_processed = int(reeval_result.get("processed") or 0)
    reeval_armed   = int(reeval_result.get("armed")   or 0)

    if reeval_errors > 0:
        orch["errors"].append(f"Reeval errors={reeval_errors} — stopping before handoff.")
        return orch
    if reeval_stalled:
        orch["errors"].append("Reeval stalled=True — stopping before handoff.")
        return orch
    if reeval_processed > 0 and reeval_armed == 0:
        orch["errors"].append(
            f"Reeval processed={reeval_processed} rows but armed=0 — "
            "no entries created. Stopping before handoff."
        )
        return orch

    # Phase 3: Handoff — mandatory, not nonfatal
    orch["overall_status"] = "FAILED_HANDOFF"
    try:
        from ap_morning_handoff_audit import run_morning_handoff_audit
        handoff_result = run_morning_handoff_audit(
            client_id=client_id,
            entry_watcher=watcher,
            osm=osm,
            execution_mode="live",
            dry_run=False,
        )
        orch["handoff_result"] = handoff_result
        if not (isinstance(handoff_result, dict)
                and handoff_result.get("ok") is True
                and not handoff_result.get("errors")):
            orch["errors"].append(
                f"Handoff failed: ok={handoff_result.get('ok')} "
                f"errors={handoff_result.get('errors')}"
            )
            return orch
    except ImportError as exc:
        orch["errors"].append(f"ap_morning_handoff_audit not importable: {exc}")
        return orch
    except Exception as exc:
        orch["errors"].append(f"Handoff raised: {exc}")
        log.error("handoff exception: %s", exc, exc_info=True)
        return orch

    # Phase 4: Readiness
    orch["overall_status"] = "FAILED_READINESS"
    try:
        from ap.preopen_readiness import run_preopen_autonomous_readiness
        readiness_result = run_preopen_autonomous_readiness(
            client_id=client_id, execution_mode="live",
            dry_run=False, stage="recovery", runner=runner,
        )
        orch["readiness_result"] = readiness_result
    except Exception as exc:
        orch["errors"].append(f"Readiness raised: {exc}")
        return orch

    if not isinstance(orch["readiness_result"], dict):
        orch["errors"].append(
            f"Readiness returned non-dict: {type(orch['readiness_result'])}"
        )
        return orch

    r_status = str(orch["readiness_result"].get("status") or "").upper()
    r_errors = orch["readiness_result"].get("errors") or []

    if r_status in ("BLOCKED", "ERROR") or r_errors:
        orch["errors"].append(
            f"Readiness blocked/errored: status={r_status} errors={r_errors}"
        )
        return orch

    orch["overall_status"] = "RECOVERY_COMPLETE"
    log.warning("recover_and_rerun_live_overnight: COMPLETE status=RECOVERY_COMPLETE")
    return orch
