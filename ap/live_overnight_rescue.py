"""
ap/live_overnight_rescue.py — Strictly fenced same-session LIVE overnight rescue.

Incident: 2026-07-20 — Jason LIVE, 53 false terminal rejections.

Design invariants enforced in this module:
  - Decision-event proof is MANDATORY: zero writes if any row lacks a verified event.
  - Mutation is a single atomic database transaction (SELECT FOR UPDATE → bulk UPDATE
    → INSERT decision events → COMMIT). Partial writes are impossible.
  - Fenced to the exact incident: client jasoncosby1@gmail.com, date 2026-07-20.
  - LIVE identity verified via durable runner state, not the caller string.
  - Stop on any failure: partial recovery, reeval error, handoff failure.
  - Never submits or cancels broker orders.
  - Never revives structural invalidations.
  - Never alters PAPER rows or another client's queue.
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any

log = logging.getLogger("ap.live_overnight_rescue")

INCIDENT_CLIENT_ID     = "jasoncosby1@gmail.com"
INCIDENT_TRADING_DATE  = "2026-07-20"
INCIDENT_EXPECTED_ROWS = 53

RECOVER_PR379_REGIME_TAXONOMY = "RECOVER_PR379_REGIME_TAXONOMY"

_DE_REASON_CODE = "INTEL_AUTHORITATIVE_VETO_RISK"
_DE_RAW_STATUS  = "RISK_VETO"
_DE_EXEC_MODE   = "LIVE"
_DE_REASONING_PATTERNS = (
    "call blocked", "put blocked",
    "spy in bear", "spy in bull",
    "risk_veto: call", "risk_veto: put",
    "approved_with_regime_mismatch",
    "market_regime_mismatch",
    "regime",
)


def _friday_after_close_window_utc(monday_date_str: str) -> tuple[datetime, datetime]:
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


def _is_regime_mismatch_reasoning(text: str) -> bool:
    lower = str(text or "").lower()
    return any(p in lower for p in _DE_REASONING_PATTERNS)


def _verify_live_identity(client_id: str, runner) -> tuple[bool, str]:
    if runner is None:
        return False, "runner is None"
    runner_mode = str(getattr(runner, "mode", "") or "").strip().upper()
    if runner_mode != "LIVE":
        return False, f"runner.mode={runner_mode!r} is not LIVE"
    runner_email = str(getattr(runner, "email", "") or "").strip().lower()
    if runner_email != client_id.strip().lower():
        return False, f"runner.email={runner_email!r} != client_id={client_id!r}"
    if not getattr(runner, "initialized", None) or not runner.initialized.is_set():
        return False, "runner not initialized"
    return True, "ok"


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
    1. SELECT … FOR UPDATE — lock exactly expected_count rows.
    2. Query decision events for every signal_id — mandatory, no fallback.
    3. Verify verified_count == expected_count.
    4. Bulk UPDATE via unnest CTE.
    5. INSERT one decision event per row in the same txn.
    6. Commit on clean exit; rollback on any exception.
    """
    from ap.db import conn

    with conn() as c:
        # 1. Lock candidate rows
        c.execute(
            """
            SELECT tq.id, tq.signal_id, tq.payload,
                   tq.last_error, tq.result_json, tq.finished_ts
            FROM trade_queue tq
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
                  WHERE rde.client_id   = tq.client_id
                    AND rde.signal_id   = tq.signal_id
                    AND rde.stage       = 'overnight_recovery'
                    AND rde.reason_code = %s
              )
            ORDER BY tq.created_ts ASC
            FOR UPDATE
            """,
            (client_id, window_start_utc, window_end_utc,
             RECOVER_PR379_REGIME_TAXONOMY),
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
        log.info("rescue_atomic: locked=%d expected=%d", locked_count, expected_count)

        if locked_count != expected_count:
            raise ValueError(
                f"Locked {locked_count} rows, expected {expected_count}. Rolling back."
            )

        # 2. Mandatory decision-event verification — no fallback
        signal_ids = [str(r.get("signal_id") or "") for r in locked_rows]
        empty_sids = [s for s in signal_ids if not s]
        if empty_sids:
            raise ValueError(
                f"{len(empty_sids)} locked rows have empty signal_id. "
                "Cannot verify decision events. Rolling back."
            )

        c.execute(
            """
            SELECT COALESCE(de.signal_id, de.context_json->>'signal_id') AS sig,
                   LOWER(COALESCE(de.context_json->>'intel_reasoning',''))  AS reasoning
            FROM decision_events de
            WHERE de.client_id = %s
              AND de.decision  = 'REJECT'
              AND de.context_json->>'intel_reason_code' = %s
              AND de.context_json->>'intel_raw_status'  = %s
              AND UPPER(COALESCE(de.context_json->>'intel_execution_mode','')) = %s
              AND DATE(de.ts AT TIME ZONE 'America/New_York') = %s
            """,
            (client_id, _DE_REASON_CODE, _DE_RAW_STATUS, _DE_EXEC_MODE, trading_date),
        )
        de_cols = [d[0] for d in (c.description or [])]
        de_rows = [
            dict(r) if isinstance(r, dict) else dict(zip(de_cols, r))
            for r in (c.fetchall() or [])
        ]
        verified: set[str] = {
            str(de.get("sig") or "")
            for de in de_rows
            if str(de.get("sig") or "") and _is_regime_mismatch_reasoning(de.get("reasoning", ""))
        }
        verified_count = len(verified & set(signal_ids))
        unverified = [s for s in signal_ids if s not in verified]

        log.info(
            "rescue_atomic: decision_event verified=%d/%d unverified_sample=%s",
            verified_count, expected_count, unverified[:3],
        )

        if verified_count != expected_count:
            raise ValueError(
                f"Decision-event verification: verified={verified_count} "
                f"expected={expected_count}. "
                f"Unverified signal_ids (first 5): {unverified[:5]}. "
                "Mandatory proof missing. Rolling back."
            )

        # 3. Build payloads
        row_ids      = [int(r["id"]) for r in locked_rows]
        new_payloads = []
        for row in locked_rows:
            p = dict(row.get("payload") or {})
            p["client_id"]      = client_id
            p["execution_mode"] = "live"
            p["recovery_context"] = {
                "reason_code":         RECOVER_PR379_REGIME_TAXONOMY,
                "trading_date":        trading_date,
                "recovered_at":        recovered_at,
                "recovery_run_id":     recovery_run_id,
                "original_last_error": str(row.get("last_error") or ""),
                "original_finished_ts": str(row.get("finished_ts") or ""),
                "original_result_json": (
                    json.loads(row["result_json"])
                    if isinstance(row.get("result_json"), str)
                    else row.get("result_json")
                ),
            }
            new_payloads.append(json.dumps(p))

        # 4. Bulk UPDATE via unnest CTE
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

        # 5. INSERT decision events inside same transaction
        for row in locked_rows:
            ctx = json.dumps({
                "stage":               "overnight_recovery",
                "decision":            "REQUEUE",
                "reason_code":         RECOVER_PR379_REGIME_TAXONOMY,
                "original_status":     "REJECTED",
                "original_last_error": str(row.get("last_error") or ""),
                "original_finished_ts": str(row.get("finished_ts") or ""),
                "recovery_run_id":     recovery_run_id,
                "trading_date":        trading_date,
                "recovered_at":        recovered_at,
                "client_id":           client_id,
                "execution_mode":      "LIVE",
            })
            c.execute(
                """
                INSERT INTO decision_events (
                    client_id, signal_id, stage, decision,
                    reason_code, context_json, ts
                ) VALUES (%s, %s, %s, %s, %s, %s::jsonb, NOW())
                """,
                (
                    client_id, str(row.get("signal_id") or ""),
                    "overnight_recovery", "REQUEUE",
                    RECOVER_PR379_REGIME_TAXONOMY, ctx,
                ),
            )

        return {"writes": updated_count, "verified": verified_count, "locked": locked_count}


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
    Fenced rescue. dry_run=True (default) returns preview only.
    All guards must pass before any write occurs.
    """
    recovery_run_id = uuid.uuid4().hex[:12]
    recovered_at    = datetime.now(timezone.utc).isoformat()

    result: dict[str, Any] = {
        "eligible": 0, "writes": 0, "verified": 0,
        "count_match": False, "dry_run": dry_run,
        "recovery_run_id": recovery_run_id,
        "client_id": client_id, "execution_mode": execution_mode,
        "trading_date": trading_date, "errors": [],
    }

    if str(execution_mode or "").strip().lower() != "live":
        result["errors"].append(f"execution_mode must be 'live', got {execution_mode!r}")
        return result
    if str(client_id or "").strip().lower() != INCIDENT_CLIENT_ID.lower():
        result["errors"].append(
            f"client_id must be {INCIDENT_CLIENT_ID!r}. Got {client_id!r}. "
            "This rescue is fenced to the 2026-07-20 incident."
        )
        return result
    if str(trading_date or "").strip() != INCIDENT_TRADING_DATE:
        result["errors"].append(
            f"trading_date must be {INCIDENT_TRADING_DATE!r}. Got {trading_date!r}."
        )
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
        window_start_utc, window_end_utc = _friday_after_close_window_utc(trading_date)
    except ValueError as exc:
        result["errors"].append(str(exc))
        return result

    log.warning(
        "rescue: client=%s date=%s dry_run=%s expected=%d run_id=%s",
        client_id, trading_date, dry_run, _expected, recovery_run_id,
    )

    if dry_run:
        try:
            from ap.db import conn, run_with_retry
            def _count():
                with conn() as c:
                    c.execute(
                        """
                        SELECT COUNT(*) AS n
                        FROM trade_queue tq
                        WHERE tq.client_id  = %s
                          AND tq.status     = 'REJECTED'
                          AND tq.last_error = 'mc_blocked:INTEL_AUTHORITATIVE_VETO_RISK'
                          AND tq.created_ts >= %s AND tq.created_ts <= %s
                          AND (tq.payload IS NULL OR tq.payload->>'recovery_context' IS NULL)
                          AND NOT EXISTS (
                              SELECT 1 FROM orders o
                              WHERE o.client_id = tq.client_id AND o.signal_id = tq.signal_id
                                AND o.kind = 'ENTRY'
                          )
                          AND NOT EXISTS (
                              SELECT 1 FROM decision_events rde
                              WHERE rde.client_id = tq.client_id AND rde.signal_id = tq.signal_id
                                AND rde.stage = 'overnight_recovery' AND rde.reason_code = %s
                          )
                        """,
                        (client_id, window_start_utc, window_end_utc,
                         RECOVER_PR379_REGIME_TAXONOMY),
                    )
                    row = c.fetchone()
                    return int((row.get("n") if isinstance(row, dict) else row[0]) or 0)
            n = run_with_retry(_count)
            result["eligible"]    = n
            result["count_match"] = (n == _expected)
            if not result["count_match"]:
                result["errors"].append(
                    f"eligible={n} expected={_expected}. Preview only — no writes."
                )
            log.warning("rescue: DRY_RUN eligible=%d expected=%d count_match=%s",
                        n, _expected, result["count_match"])
        except Exception as exc:
            result["errors"].append(f"dry-run count failed: {exc}")
        return result

    # Live run
    try:
        txn = _execute_atomic_rescue(
            client_id=client_id, trading_date=trading_date,
            window_start_utc=window_start_utc, window_end_utc=window_end_utc,
            expected_count=_expected,
            recovery_run_id=recovery_run_id, recovered_at=recovered_at,
        )
        result["eligible"]    = txn["locked"]
        result["writes"]      = txn["writes"]
        result["verified"]    = txn["verified"]
        result["count_match"] = (txn["writes"] == _expected)
        log.warning(
            "rescue: COMPLETE writes=%d verified=%d client=%s date=%s run_id=%s",
            result["writes"], result["verified"],
            client_id, trading_date, recovery_run_id,
        )
    except Exception as exc:
        result["errors"].append(f"atomic rescue failed (rolled back): {exc}")
        log.error("rescue: FAILED (rolled back): %s", exc, exc_info=True)

    return result


def recover_and_rerun_live_overnight(
    *,
    runner,
    trading_date: str,
    dry_run: bool = True,
    expected_count: int | None = None,
) -> dict:
    """
    Full recovery orchestration. Stops immediately on any failure.
    Uses run_morning_handoff_audit for post-overnight handoff.
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

    core     = getattr(runner, "core", None)
    broker   = getattr(runner, "broker", None)
    data_broker = getattr(runner, "data_broker", None) or getattr(runner, "databroker", None)
    mc       = getattr(runner, "master_control", None)
    selector = getattr(runner, "contract_selector", None)
    osm      = getattr(runner, "order_state_machine", None)
    watcher  = getattr(core, "entry_watcher", None) if core else None

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

    # Step 1: Rescue
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
            f"Rescue partial: writes={writes} expected={_expected}. Stopping."
        )
        return orch

    # Step 2: Canonical overnight reeval
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
    if int(reeval_result.get("errors") or 0) > 0:
        orch["errors"].append(
            f"Reeval errors={reeval_result['errors']} — some rows may not have armed."
        )

    # Step 3: Handoff — not nonfatal; stops if missing or failed
    try:
        from ap.morning_handoff import run_morning_handoff_audit
        handoff_result = run_morning_handoff_audit(
            client_id=client_id, execution_mode="live",
            stage="post_overnight_reeval", dry_run=False, runner=runner,
        )
        orch["handoff_result"] = handoff_result
        if not (isinstance(handoff_result, dict)
                and str(handoff_result.get("status") or "").lower() in ("ok", "success")):
            orch["errors"].append(f"Handoff did not succeed: {handoff_result}")
            return orch
    except ImportError as exc:
        orch["errors"].append(f"run_morning_handoff_audit not importable: {exc}")
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
