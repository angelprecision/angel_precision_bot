"""
ap/live_overnight_rescue.py — Strictly fenced LIVE overnight rescue.
Incident: 2026-07-20 — Jason LIVE, 53 false terminal rejections.

Production schema facts enforced:
  - decision_events has NO signal_id column.
    Signals are identified via candidate_id = REEVAL:<signal_id>:<suffix>.
  - Recovery events use canonical columns:
    run_id, candidate_id, client_id, stage, decision, reason_code,
    explanation, context_json, ts.
  - Handoff: from ap_morning_handoff_audit import run_morning_handoff_audit
    Signature: (client_id, entry_watcher, osm, execution_mode, dry_run)
    Returns: {"ok": bool, "errors": [...]}
  - Dry-run runs the IDENTICAL eligibility + DE-proof query as the write
    transaction, under a read-only connection, and returns eligible=N
    and verified=N from a single COUNT.

Invariants:
  - Decision-event proof mandatory for every row — no fallback.
  - Single atomic transaction (SELECT FOR UPDATE → bulk UPDATE →
    INSERT recovery events → COMMIT). Partial writes impossible.
  - Fenced: client=jasoncosby1@gmail.com, date=2026-07-20.
  - runner mandatory for dry_run=False; identity verified via runner state.
  - Stops immediately on any failure in the orchestration chain.
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any

log = logging.getLogger("ap.live_overnight_rescue")

# ── Incident constants ─────────────────────────────────────────────────────────

INCIDENT_CLIENT_ID     = "jasoncosby1@gmail.com"
INCIDENT_TRADING_DATE  = "2026-07-20"
INCIDENT_EXPECTED_ROWS = 53

RECOVER_PR379_REGIME_TAXONOMY = "RECOVER_PR379_REGIME_TAXONOMY"

# Exact decision_events field values that identify the false veto.
# stage/decision/reason_code/explanation match the production event writer.
_DE_STAGE       = "blocked_intel"
_DE_DECISION    = "REJECT"
_DE_REASON_CODE = "SESSION_RULE_BLOCK"
_DE_EXPLANATION = "INTEL_AUTHORITATIVE_VETO_RISK"

# Context fields — must all match exactly.
_DE_CTX_REASON_CODE = "INTEL_AUTHORITATIVE_VETO_RISK"
_DE_CTX_RAW_STATUS  = "RISK_VETO"
_DE_CTX_EXEC_MODE   = "LIVE"

# Exact reasoning strings produced by intelligence_bridge._block_gate()
# when APRiskManager returned "CALL blocked — SPY in BEAR trend" (pre-PR format).
# No generic "regime" match — exact two family strings only.
_DE_REASONING_EXACT = (
    "risk_veto: CALL blocked \u2014 SPY in BEAR trend",
    "risk_veto: PUT blocked \u2014 SPY in BULL trend",
)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _friday_after_close_window_utc(monday_date_str: str) -> tuple[datetime, datetime]:
    """Friday 16:00 ET → Monday 09:30 ET in UTC."""
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
    monday = date.fromisoformat(monday_date_str)
    if monday.weekday() != 0:
        raise ValueError(
            f"trading_date={monday_date_str} weekday={monday.weekday()} — must be Monday."
        )
    friday = monday - timedelta(days=3)
    start = datetime(friday.year, friday.month, friday.day, 16, 0, 0, tzinfo=ET)
    end   = datetime(monday.year, monday.month, monday.day, 9, 30, 0, tzinfo=ET)
    return start.astimezone(timezone.utc), end.astimezone(timezone.utc)


def _verify_live_identity(client_id: str, runner) -> tuple[bool, str]:
    """Verify runner is genuinely LIVE via durable state, not caller string."""
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


def _build_eligibility_exists_clause() -> str:
    """
    Returns the EXISTS subquery that verifies a trade_queue row has a matching
    decision_events record using the production candidate_id shape:
        REEVAL:<trade_queue.signal_id>:<suffix>

    This is the single source of truth used by BOTH dry-run and write
    transaction to guarantee they operate on the identical population.
    """
    return """
        EXISTS (
            SELECT 1
            FROM decision_events de
            WHERE de.client_id       = tq.client_id
              AND de.candidate_id LIKE ('REEVAL:' || tq.signal_id || ':%')
              AND de.stage           = 'blocked_intel'
              AND de.decision        = 'REJECT'
              AND de.reason_code     = 'SESSION_RULE_BLOCK'
              AND de.explanation     = 'INTEL_AUTHORITATIVE_VETO_RISK'
              AND de.context_json->>'intel_reason_code' = 'INTEL_AUTHORITATIVE_VETO_RISK'
              AND de.context_json->>'intel_raw_status'  = 'RISK_VETO'
              AND UPPER(COALESCE(de.context_json->>'intel_execution_mode','')) = 'LIVE'
              AND de.context_json->>'intel_reasoning' IN (
                  'risk_veto: CALL blocked \u2014 SPY in BEAR trend',
                  'risk_veto: PUT blocked \u2014 SPY in BULL trend'
              )
              AND DATE(de.ts AT TIME ZONE 'America/New_York') = '{trading_date}'
        )
    """


def _build_structural_where(
    *,
    client_id: str,
    window_start_utc: datetime,
    window_end_utc: datetime,
) -> tuple[str, list]:
    """
    Returns (WHERE clause fragment, params) for all structural predicates.
    Used by both dry-run count and write transaction.
    """
    sql = """
        tq.client_id  = %s
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
            WHERE rde.client_id   = tq.client_id
              AND rde.candidate_id LIKE ('RECOVER:' || tq.signal_id || ':%')
              AND rde.stage        = 'overnight_recovery'
              AND rde.reason_code  = %s
        )
    """
    params = [client_id, window_start_utc, window_end_utc,
              RECOVER_PR379_REGIME_TAXONOMY]
    return sql, params


# ── Dry-run: identical query to write, no UPDATE ───────────────────────────────

def _dry_run_count(
    *,
    client_id: str,
    trading_date: str,
    window_start_utc: datetime,
    window_end_utc: datetime,
) -> dict:
    """
    Runs the EXACT same query as the write transaction (with DE proof EXISTS),
    but as a COUNT — no FOR UPDATE, no UPDATE. Returns eligible=N, verified=N.
    Since the EXISTS clause requires a matching DE per row, eligible == verified
    by construction.
    """
    from ap.db import conn, run_with_retry

    structural_where, params = _build_structural_where(
        client_id=client_id,
        window_start_utc=window_start_utc,
        window_end_utc=window_end_utc,
    )
    de_exists = _build_eligibility_exists_clause().format(trading_date=trading_date)

    def _query():
        with conn() as c:
            c.execute(
                f"""
                SELECT COUNT(*) AS n
                FROM trade_queue tq
                WHERE {structural_where}
                  AND {de_exists}
                """,
                params,
            )
            row = c.fetchone()
            return int((row.get("n") if isinstance(row, dict) else row[0]) or 0)

    return {"eligible": run_with_retry(_query)}


# ── Atomic write transaction ───────────────────────────────────────────────────

def _execute_atomic_rescue(
    *,
    client_id: str,
    trading_date: str,
    window_start_utc: datetime,
    window_end_utc: datetime,
    expected_count: int,
    recovery_run_id: str,
    recovered_at: str,
) -> dict:
    """
    Single atomic transaction:
    1. SELECT tq rows WHERE [structural predicates] AND [DE EXISTS proof] FOR UPDATE.
       The DE EXISTS is embedded in the WHERE — only rows with verified events
       are locked. verified == locked by construction.
    2. Verify locked_count == expected_count; raise otherwise (rollback).
    3. Bulk UPDATE via unnest CTE: REJECTED → WATCHING.
    4. Verify updated_count == expected_count; raise otherwise (rollback).
    5. INSERT one recovery event per row (canonical columns).
    6. COMMIT.  Any exception triggers full ROLLBACK.

    Returns: {writes, locked, errors:[]}
    Raises:  ValueError on any count mismatch.
    """
    from ap.db import conn

    structural_where, params = _build_structural_where(
        client_id=client_id,
        window_start_utc=window_start_utc,
        window_end_utc=window_end_utc,
    )
    de_exists = _build_eligibility_exists_clause().format(trading_date=trading_date)

    with conn() as c:

        # 1. Lock rows — DE EXISTS is part of the WHERE, so locked == verified
        c.execute(
            f"""
            SELECT tq.id, tq.signal_id, tq.payload,
                   tq.last_error, tq.result_json, tq.finished_ts
            FROM trade_queue tq
            WHERE {structural_where}
              AND {de_exists}
            ORDER BY tq.created_ts ASC
            FOR UPDATE OF tq
            """,
            params,
        )
        cols = [d[0] for d in (c.description or [])]
        locked_rows = [
            dict(r) if isinstance(r, dict) else dict(zip(cols, r))
            for r in (c.fetchall() or [])
        ]
        for row in locked_rows:
            if isinstance(row.get("payload"), str):
                try:
                    row["payload"] = json.loads(row["payload"])
                except Exception:
                    row["payload"] = {}

        locked_count = len(locked_rows)
        log.info(
            "rescue_atomic: locked=%d (includes DE proof) expected=%d",
            locked_count, expected_count,
        )

        if locked_count != expected_count:
            raise ValueError(
                f"Locked {locked_count} rows with DE proof, expected {expected_count}. "
                "Count mismatch — rolling back."
            )

        # 2. Build new payloads (stamp identity + recovery_context)
        row_ids: list[int] = []
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

        # 3. Bulk UPDATE via unnest CTE
        c.execute(
            """
            WITH new_data AS (
                SELECT unnest(%s::int[])   AS row_id,
                       unnest(%s::jsonb[]) AS new_payload
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

        # 4. INSERT recovery events using canonical decision_events columns.
        #    candidate_id = RECOVER:<signal_id>:<recovery_run_id> so it
        #    matches the NOT EXISTS exclusion in subsequent runs (idempotency).
        for row in locked_rows:
            signal_id     = str(row.get("signal_id") or "")
            candidate_id  = f"RECOVER:{signal_id}:{recovery_run_id}"
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
                    recovery_run_id,
                    candidate_id,
                    client_id,
                    "overnight_recovery",
                    "REQUEUE",
                    RECOVER_PR379_REGIME_TAXONOMY,
                    "RECOVERED_PR379_REGIME_MISMATCH_FALSE_VETO",
                    ctx,
                ),
            )

        # conn() commits on clean exit; rolls back on any exception.
        return {"writes": updated_count, "locked": locked_count, "errors": []}


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
    Fenced rescue. dry_run=True (default) returns preview with full DE proof.
    runner is mandatory for dry_run=False.

    Returns:
      eligible  — rows matching all structural + DE predicates
      verified  — same as eligible (DE proof is in the eligibility query)
      writes    — 0 on dry_run or failure
      count_match — True when eligible == expected_count
      errors    — list of hard-failure messages
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
            f"client_id must be {INCIDENT_CLIENT_ID!r} (incident-fenced). "
            f"Got {client_id!r}."
        )
        return result

    if str(trading_date or "").strip() != INCIDENT_TRADING_DATE:
        result["errors"].append(
            f"trading_date must be {INCIDENT_TRADING_DATE!r} (incident-fenced). "
            f"Got {trading_date!r}."
        )
        return result

    # Runner mandatory for writes; verified for both dry and live.
    if not dry_run and runner is None:
        result["errors"].append(
            "runner is mandatory for dry_run=False. "
            "LIVE identity cannot be verified without a runner."
        )
        return result

    if runner is not None:
        ok, reason = _verify_live_identity(client_id, runner)
        if not ok:
            result["errors"].append(f"LIVE identity verification failed: {reason}")
            return result

    if expected_count is not None and expected_count != INCIDENT_EXPECTED_ROWS:
        result["errors"].append(
            f"expected_count={expected_count} but this rescue requires exactly "
            f"{INCIDENT_EXPECTED_ROWS}."
        )
        return result

    _expected = INCIDENT_EXPECTED_ROWS

    try:
        window_start_utc, window_end_utc = _friday_after_close_window_utc(trading_date)
    except ValueError as exc:
        result["errors"].append(str(exc))
        return result

    log.warning(
        "rescue: client=%s date=%s dry_run=%s expected=%d run_id=%s",
        client_id, trading_date, dry_run, _expected, recovery_run_id,
    )

    # ── Dry-run: full eligibility + DE proof, no writes ────────────────────────
    if dry_run:
        try:
            counts = _dry_run_count(
                client_id=client_id,
                trading_date=trading_date,
                window_start_utc=window_start_utc,
                window_end_utc=window_end_utc,
            )
            n = counts["eligible"]
            result["eligible"]    = n
            result["verified"]    = n   # DE proof is in the EXISTS — eligible == verified
            result["count_match"] = (n == _expected)
            if not result["count_match"]:
                result["errors"].append(
                    f"eligible={n} expected={_expected}. "
                    "Preview — zero writes. Inspect candidates."
                )
            log.warning(
                "rescue: DRY_RUN eligible=%d verified=%d expected=%d count_match=%s",
                n, n, _expected, result["count_match"],
            )
        except Exception as exc:
            result["errors"].append(f"dry-run query failed: {exc}")
            log.error("rescue dry-run failed: %s", exc, exc_info=True)
        return result

    # ── Live write ─────────────────────────────────────────────────────────────
    log.warning(
        "rescue: LIVE WRITE STARTING client=%s date=%s expected=%d run_id=%s",
        client_id, trading_date, _expected, recovery_run_id,
    )
    try:
        txn = _execute_atomic_rescue(
            client_id=client_id, trading_date=trading_date,
            window_start_utc=window_start_utc, window_end_utc=window_end_utc,
            expected_count=_expected,
            recovery_run_id=recovery_run_id, recovered_at=recovered_at,
        )
        result["eligible"]    = txn["locked"]
        result["verified"]    = txn["locked"]   # locked == DE-verified by construction
        result["writes"]      = txn["writes"]
        result["count_match"] = (txn["writes"] == _expected)
        log.warning(
            "rescue: COMPLETE writes=%d client=%s date=%s run_id=%s",
            result["writes"], client_id, trading_date, recovery_run_id,
        )
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
    Full recovery orchestration. Stops on any failure.
    Handoff: ap_morning_handoff_audit.run_morning_handoff_audit(
        client_id, entry_watcher, osm, execution_mode="live", dry_run=False
    )
    """
    orch: dict[str, Any] = {
        "rescue_result": None, "reeval_result": None,
        "handoff_result": None, "readiness_result": None,
        "errors": [], "dry_run": dry_run, "trading_date": trading_date,
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

    core      = getattr(runner, "core", None)
    broker    = getattr(runner, "broker", None)
    data_broker = getattr(runner, "data_broker", None) or getattr(runner, "databroker", None)
    mc        = getattr(runner, "master_control", None)
    selector  = getattr(runner, "contract_selector", None)
    osm       = getattr(runner, "order_state_machine", None)
    watcher   = getattr(core, "entry_watcher", None) if core else None
    pos_mgr   = getattr(runner, "position_manager", None)

    missing = [
        name for name, obj in [
            ("broker", broker), ("master_control", mc),
            ("contract_selector", selector), ("order_state_machine", osm),
            ("entry_watcher", watcher),
        ] if obj is None
    ]
    if missing:
        orch["errors"].append(f"Missing runner components: {missing}")
        return orch

    _expected = expected_count if expected_count is not None else INCIDENT_EXPECTED_ROWS

    # Step 1: Fenced rescue
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
        return orch

    writes = int(rescue_result.get("writes") or 0)
    if writes == 0:
        orch["errors"].append("Rescue produced 0 writes.")
        return orch
    if writes != _expected:
        orch["errors"].append(
            f"Rescue partial: writes={writes} expected={_expected}. "
            "Stopping — do not run reeval on incomplete recovery."
        )
        return orch

    # Step 2: Canonical overnight reeval
    try:
        from ap_overnight_reeval import run_overnight_reeval
        reeval_result = run_overnight_reeval(
            client_id=client_id, broker=broker, data_broker=data_broker,
            master_control=mc, contract_selector=selector,
            order_state_machine=osm, entry_watcher=watcher,
            position_manager=pos_mgr,
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
    if int(reeval_result.get("errors") or 0) > 0:
        orch["errors"].append(
            f"Reeval errors={reeval_result['errors']} — some rows may not have armed."
        )

    # Step 3: Handoff via the real production function signature
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
                f"Handoff did not succeed: ok={handoff_result.get('ok')} "
                f"errors={handoff_result.get('errors')}"
            )
            return orch
    except ImportError as exc:
        orch["errors"].append(
            f"ap_morning_handoff_audit not importable: {exc}. "
            "Cannot complete post-overnight handoff."
        )
        return orch
    except Exception as exc:
        orch["errors"].append(f"Handoff failed: {exc}")
        log.error("handoff exception: %s", exc, exc_info=True)
        return orch

    # Step 4: Readiness
    try:
        from ap.preopen_readiness import run_preopen_autonomous_readiness
        readiness_result = run_preopen_autonomous_readiness(
            client_id=client_id, execution_mode="live",
            dry_run=False, stage="recovery", runner=runner,
        )
        orch["readiness_result"] = readiness_result
        log.info(
            "readiness status=%s errors=%s",
            readiness_result.get("status"), readiness_result.get("errors"),
        )
    except Exception as exc:
        orch["errors"].append(f"Readiness check failed: {exc}")

    return orch
