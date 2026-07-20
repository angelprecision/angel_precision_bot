"""
ap/live_overnight_rescue.py — Strictly fenced same-session LIVE overnight rescue.

Incident context (2026-07-20):
  53 Jason LIVE trade_queue rows were false-terminally rejected as
  mc_blocked:INTEL_AUTHORITATIVE_VETO_RISK because APRiskManager's
  SPY regime mismatch returns (advisory, hard_veto=False) were not yet
  carrying structured authority fields — intelligence_bridge converted
  the generic approved=False into an execution-authoritative RISK_VETO.
  PR #379 taxonomy fix closes the root cause; this module recovers the
  specific incident rows under strict multi-layer eligibility criteria.

Scope boundary (enforced):
  - Only mutates trade_queue rows satisfying ALL eligibility criteria.
  - Never submits or cancels broker orders directly.
  - Never revives structural invalidations (INVALIDATED_PRIOR_HIGH_BREACHED).
  - Never mutates existing positions, proof trades, or exit orders.
  - Never alters PAPER rows or another client's queue.
  - Recovered rows re-enter the canonical overnight path via run_overnight_reeval().
"""
from __future__ import annotations

import logging
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any

log = logging.getLogger("ap.live_overnight_rescue")

# ── Stable identifiers ────────────────────────────────────────────────────────

RECOVER_PR379_REGIME_TAXONOMY = "RECOVER_PR379_REGIME_TAXONOMY"

# SPY regime mismatch reasoning patterns emitted by intelligence_bridge before
# the taxonomy fix.  Bridge emits: "risk_veto: {APRiskManager.reason}" where
# reason is the human-readable string from _reject().
_REGIME_MISMATCH_PATTERNS = (
    "call blocked",       # "CALL blocked — SPY in BEAR trend"
    "put blocked",        # "PUT blocked — SPY in BULL trend"
    "spy in bear",
    "spy in bull",
    "risk_veto: call",
    "risk_veto: put",
    "market_regime_mismatch",
    "regime mismatch",
    "directional_context",
)

# last_error values that are genuine structural failures and MUST NOT be rescued.
_INELIGIBLE_LAST_ERROR_PREFIXES = (
    "overnight_invalidated:",          # structural validation failure
    "stale_signal:",
    "trigger_invalid",
    "invalid_or_missing_side",
    "live_authorization:",
    "overnight_watch_arm_failed:",
    "order_materialization_failed",
    "risk_blocked",
    "duplicate_setup",
    "intraday_timeframe_rejected",
)


# ── Time-window helpers ────────────────────────────────────────────────────────

def _friday_after_close_window_utc(monday_date_str: str) -> tuple[datetime, datetime]:
    """
    Return (window_start_utc, window_end_utc) covering Friday-after-close
    through Monday pre-open for overnight inventory eligible for trading_date.

    Friday 16:00 ET → Monday 09:30 ET (exclusive upper bound).
    """
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
    monday = date.fromisoformat(monday_date_str)
    if monday.weekday() != 0:
        raise ValueError(
            f"trading_date={monday_date_str} is weekday={monday.weekday()} "
            f"(expected 0=Monday). This rescue is only valid for Monday trading dates."
        )
    friday = monday - timedelta(days=3)
    window_start = datetime(friday.year, friday.month, friday.day, 16, 0, 0, tzinfo=ET)
    window_end   = datetime(monday.year, monday.month, monday.day, 9, 30, 0, tzinfo=ET)
    return window_start.astimezone(timezone.utc), window_end.astimezone(timezone.utc)


def _is_regime_mismatch_reasoning(intel_reasoning: str) -> bool:
    """True if intel_reasoning matches the SPY directional mismatch family."""
    lower = str(intel_reasoning or "").lower()
    return any(pat in lower for pat in _REGIME_MISMATCH_PATTERNS)


# ── Database helpers ───────────────────────────────────────────────────────────

def _fetch_structurally_eligible_rows(
    *,
    client_id: str,
    window_start_utc: datetime,
    window_end_utc: datetime,
) -> list[dict]:
    """
    Fetch trade_queue rows satisfying structural eligibility:
      - status='REJECTED', last_error='mc_blocked:INTEL_AUTHORITATIVE_VETO_RISK'
      - created_ts within Friday-after-close → Monday-preopen window
      - No ENTRY order exists for the same client + signal
      - payload has no recovery_context (not already recovered)

    Decision-event intel field verification is done separately in Python
    so we can emit per-row diagnostics.
    """
    from ap.db import conn, run_with_retry
    import json as _json

    def _query():
        with conn() as c:
            c.execute(
                """
                SELECT
                    tq.id,
                    tq.signal_id,
                    tq.payload,
                    tq.created_ts,
                    tq.last_error,
                    tq.finished_ts
                FROM trade_queue tq
                WHERE tq.client_id = %s
                  AND tq.status = 'REJECTED'
                  AND tq.last_error = 'mc_blocked:INTEL_AUTHORITATIVE_VETO_RISK'
                  AND tq.created_ts >= %s
                  AND tq.created_ts <= %s
                  AND (
                      tq.payload IS NULL
                      OR tq.payload->>'recovery_context' IS NULL
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM orders o
                      WHERE o.client_id = tq.client_id
                        AND COALESCE(o.signal_id, '') != ''
                        AND o.signal_id = tq.signal_id
                        AND o.kind = 'ENTRY'
                  )
                ORDER BY tq.created_ts ASC
                """,
                (client_id, window_start_utc, window_end_utc),
            )
            rows = c.fetchall() or []
            cols = [desc[0] for desc in (c.description or [])]
            result = []
            for row in rows:
                d = dict(row) if isinstance(row, dict) else dict(zip(cols, row))
                if isinstance(d.get("payload"), str):
                    try:
                        d["payload"] = _json.loads(d["payload"])
                    except Exception:
                        d["payload"] = {}
                result.append(d)
            return result

    return run_with_retry(_query) or []


def _verify_decision_event_intel(
    *,
    client_id: str,
    signal_ids: list[str],
    trading_date: str,
) -> set[str]:
    """
    Return the set of signal_ids that have a matching decision_event with:
      - intel_reason_code = 'INTEL_AUTHORITATIVE_VETO_RISK'
      - intel_raw_status  = 'RISK_VETO'
      - intel_execution_mode = 'LIVE'
      - intel_reasoning matches SPY regime mismatch family
      - ts on trading_date (ET)

    Falls back to an empty set if decision_events table doesn't exist or
    signal_id is not queryable — rescue will proceed on structural criteria
    only and log a warning.
    """
    if not signal_ids:
        return set()

    from ap.db import conn, run_with_retry

    def _query():
        with conn() as c:
            c.execute(
                """
                SELECT DISTINCT
                    COALESCE(
                        signal_id,
                        context_json->>'signal_id'
                    ) AS signal_id
                FROM decision_events
                WHERE client_id = %s
                  AND decision = 'REJECT'
                  AND context_json->>'intel_reason_code' = 'INTEL_AUTHORITATIVE_VETO_RISK'
                  AND context_json->>'intel_raw_status' = 'RISK_VETO'
                  AND UPPER(COALESCE(context_json->>'intel_execution_mode','')) = 'LIVE'
                  AND (
                      LOWER(COALESCE(context_json->>'intel_reasoning','')) LIKE '%%call blocked%%'
                      OR LOWER(COALESCE(context_json->>'intel_reasoning','')) LIKE '%%put blocked%%'
                      OR LOWER(COALESCE(context_json->>'intel_reasoning','')) LIKE '%%spy in bear%%'
                      OR LOWER(COALESCE(context_json->>'intel_reasoning','')) LIKE '%%spy in bull%%'
                      OR LOWER(COALESCE(context_json->>'intel_reasoning','')) LIKE '%%regime%%mismatch%%'
                  )
                  AND DATE(ts AT TIME ZONE 'America/New_York') = %s
                """,
                (client_id, trading_date),
            )
            rows = c.fetchall() or []
            result = set()
            for row in rows:
                sid = (row.get("signal_id") if isinstance(row, dict) else row[0])
                if sid:
                    result.add(str(sid))
            return result

    try:
        return run_with_retry(_query) or set()
    except Exception as exc:
        log.warning(
            "live_overnight_rescue: decision_event intel verify failed "
            "(non-fatal, will proceed on structural criteria only): %s", exc,
        )
        return set()


def _emit_recovery_decision_event(
    *,
    client_id: str,
    row: dict,
    trading_date: str,
    recovery_run_id: str,
    recovered_at: str,
) -> None:
    """Emit a durable decision event for each recovered row."""
    try:
        from ap.db import conn, run_with_retry
        import json as _json

        signal_id   = str(row.get("signal_id") or "")
        payload     = row.get("payload") or {}
        ticker      = str(payload.get("ticker") or payload.get("symbol") or "")
        side        = str(payload.get("side") or payload.get("direction") or "")
        original_last_error = str(row.get("last_error") or "")
        original_finished   = row.get("finished_ts")

        context = {
            "stage":                  "overnight_recovery",
            "decision":               "REQUEUE",
            "reason_code":            RECOVER_PR379_REGIME_TAXONOMY,
            "original_status":        "REJECTED",
            "original_last_error":    original_last_error,
            "original_finished_ts":   str(original_finished or ""),
            "recovery_run_id":        recovery_run_id,
            "recovery_trading_date":  trading_date,
            "recovered_at":           recovered_at,
            "client_id":              client_id,
            "execution_mode":         "LIVE",
        }

        def _write():
            with conn() as c:
                c.execute(
                    """
                    INSERT INTO decision_events (
                        client_id, signal_id, stage, decision,
                        reason_code, context_json, ts
                    ) VALUES (%s, %s, %s, %s, %s, %s::jsonb, NOW())
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        client_id,
                        signal_id,
                        "overnight_recovery",
                        "REQUEUE",
                        RECOVER_PR379_REGIME_TAXONOMY,
                        _json.dumps(context),
                    ),
                )
        run_with_retry(_write)
    except Exception as exc:
        log.warning(
            "[%s] live_overnight_rescue: decision event emit failed (non-fatal): %s",
            row.get("id"), exc,
        )


def _atomic_requeue_rows(
    *,
    client_id: str,
    row_ids: list,
    payloads_by_id: dict,
    trading_date: str,
    recovery_run_id: str,
    recovered_at: str,
) -> int:
    """
    Atomically transition selected trade_queue rows REJECTED → WATCHING.

    For each row:
      - status          = 'WATCHING'
      - started_ts      = NULL
      - finished_ts     = NULL
      - last_error      = NULL
      - result_json     = NULL
      - payload updated to include client_id, execution_mode, recovery_context

    Returns the number of rows successfully updated.
    """
    from ap.db import conn, run_with_retry
    import json as _json

    updated = 0

    for row_id in row_ids:
        payload = dict(payloads_by_id.get(row_id) or {})
        # Stamp identity into payload (may have been absent in original rows)
        payload["client_id"]      = client_id
        payload["execution_mode"] = "live"
        payload["recovery_context"] = {
            "reason_code":       RECOVER_PR379_REGIME_TAXONOMY,
            "trading_date":      trading_date,
            "recovered_at":      recovered_at,
            "recovery_run_id":   recovery_run_id,
        }

        def _update(rid=row_id, p=payload):
            with conn() as c:
                c.execute(
                    """
                    UPDATE trade_queue
                    SET status      = 'WATCHING',
                        started_ts  = NULL,
                        finished_ts = NULL,
                        last_error  = NULL,
                        result_json = NULL,
                        payload     = %s::jsonb
                    WHERE id        = %s
                      AND client_id = %s
                      AND status    = 'REJECTED'
                      AND last_error = 'mc_blocked:INTEL_AUTHORITATIVE_VETO_RISK'
                    """,
                    (_json.dumps(p), rid, client_id),
                )
                return c.rowcount

        try:
            rows_affected = run_with_retry(_update)
            if rows_affected:
                updated += 1
        except Exception as exc:
            log.error(
                "live_overnight_rescue: failed to requeue row id=%s: %s", row_id, exc
            )

    return updated


# ── Public rescue function ─────────────────────────────────────────────────────

def rescue_live_overnight_regime_rejections(
    *,
    client_id: str,
    execution_mode: str,
    trading_date: str,
    dry_run: bool = True,
    expected_count: int | None = None,
) -> dict:
    """
    Strictly fenced same-session LIVE rescue for the 2026-07-20 incident.

    Identifies trade_queue rows that were false-terminally rejected as
    mc_blocked:INTEL_AUTHORITATIVE_VETO_RISK due to the missing hard_veto
    taxonomy in APRiskManager's regime mismatch returns, and transitions
    them back to WATCHING so run_overnight_reeval() can reprocess them.

    Hard guards (any failure → zero writes and explicit error return):
      - execution_mode must be exactly "live"
      - client_id must be the exact LIVE client
      - trading_date must be a Monday ISO date
      - eligible count must match expected_count if supplied

    The rescue NEVER selects:
      - overnight structural invalidations (INVALIDATED_PRIOR_HIGH_BREACHED)
      - account / capital / authorization / contract-quality vetoes
      - PAPER rows or another client's rows
      - rows already recovered (have recovery_context in payload)
      - rows that have ENTRY orders, broker orders, or positions

    Args:
        client_id:      Exact LIVE client email.
        execution_mode: Must be "live" (case-insensitive); refuses otherwise.
        trading_date:   Monday ISO date (e.g. "2026-07-20").
        dry_run:        Default True — returns preview counts, zero writes.
        expected_count: If supplied, the eligible count must match exactly;
                        mismatch aborts with zero writes.

    Returns:
        {
          "eligible":    int,        # rows satisfying all criteria
          "writes":      int,        # rows actually updated (0 on dry_run)
          "count_match": bool,       # True if eligible == expected_count (or no expectation)
          "dry_run":     bool,
          "recovery_run_id": str,
          "diagnostics": dict,       # per-row detail
          "errors":      list[str],  # hard errors
        }
    """
    recovery_run_id = uuid.uuid4().hex[:12]
    recovered_at    = datetime.now(timezone.utc).isoformat()

    result: dict[str, Any] = {
        "eligible":        0,
        "writes":          0,
        "count_match":     False,
        "dry_run":         dry_run,
        "recovery_run_id": recovery_run_id,
        "client_id":       client_id,
        "execution_mode":  execution_mode,
        "trading_date":    trading_date,
        "diagnostics":     {},
        "errors":          [],
    }

    # ── Hard guards ───────────────────────────────────────────────────────────
    _mode = str(execution_mode or "").strip().lower()
    if _mode != "live":
        result["errors"].append(
            f"execution_mode must be 'live', got '{execution_mode}'. "
            "This rescue is LIVE-only."
        )
        log.error("live_overnight_rescue: rejected — execution_mode=%s is not live", execution_mode)
        return result

    if not str(client_id or "").strip():
        result["errors"].append("client_id is required")
        return result

    if not str(trading_date or "").strip():
        result["errors"].append("trading_date is required")
        return result

    try:
        window_start_utc, window_end_utc = _friday_after_close_window_utc(trading_date)
    except ValueError as exc:
        result["errors"].append(str(exc))
        log.error("live_overnight_rescue: invalid trading_date: %s", exc)
        return result

    log.info(
        "live_overnight_rescue: starting | client=%s mode=%s date=%s "
        "dry_run=%s expected_count=%s recovery_run_id=%s "
        "window=[%s, %s]",
        client_id, _mode, trading_date, dry_run, expected_count,
        recovery_run_id,
        window_start_utc.isoformat(), window_end_utc.isoformat(),
    )

    # ── Fetch structurally eligible rows ──────────────────────────────────────
    try:
        candidates = _fetch_structurally_eligible_rows(
            client_id=client_id,
            window_start_utc=window_start_utc,
            window_end_utc=window_end_utc,
        )
    except Exception as exc:
        result["errors"].append(f"structural query failed: {exc}")
        log.error("live_overnight_rescue: structural query error: %s", exc)
        return result

    log.info(
        "live_overnight_rescue: structural candidates=%d client=%s date=%s",
        len(candidates), client_id, trading_date,
    )

    # ── Decision-event intel field verification (best-effort) ─────────────────
    candidate_signal_ids = [
        str(r.get("signal_id") or "") for r in candidates if r.get("signal_id")
    ]
    verified_signal_ids = _verify_decision_event_intel(
        client_id=client_id,
        signal_ids=candidate_signal_ids,
        trading_date=trading_date,
    )

    if verified_signal_ids:
        log.info(
            "live_overnight_rescue: decision_event verification matched %d/%d signal_ids",
            len(verified_signal_ids), len(candidate_signal_ids),
        )
    else:
        log.warning(
            "live_overnight_rescue: decision_event verification returned 0 matches "
            "(table may not have signal_id column or events not yet written). "
            "Proceeding on structural criteria only for %d candidates.",
            len(candidates),
        )

    # ── Build eligible set ────────────────────────────────────────────────────
    eligible_rows: list[dict] = []
    diag: dict[str, Any] = {"skipped_structural": [], "skipped_decision_event": []}

    for row in candidates:
        row_id    = row.get("id")
        signal_id = str(row.get("signal_id") or "")
        payload   = row.get("payload") or {}
        last_err  = str(row.get("last_error") or "")

        # Paranoia: double-check ineligible last_error prefixes
        if any(last_err.startswith(p) for p in _INELIGIBLE_LAST_ERROR_PREFIXES):
            diag["skipped_structural"].append({
                "id": row_id, "signal_id": signal_id,
                "reason": f"ineligible_last_error:{last_err[:80]}",
            })
            continue

        # If decision events verified any, require signal_id to be in the set
        if verified_signal_ids and signal_id and signal_id not in verified_signal_ids:
            diag["skipped_decision_event"].append({
                "id": row_id, "signal_id": signal_id,
                "reason": "not_in_decision_event_verified_set",
            })
            continue

        eligible_rows.append(row)

    result["eligible"]    = len(eligible_rows)
    result["diagnostics"] = diag

    # ── expected_count guard ──────────────────────────────────────────────────
    if expected_count is not None:
        count_match = (len(eligible_rows) == expected_count)
        result["count_match"] = count_match
        if not count_match:
            result["errors"].append(
                f"expected_count={expected_count} but eligible={len(eligible_rows)}. "
                "Zero writes performed. Re-run dry_run=True to inspect candidates."
            )
            log.error(
                "live_overnight_rescue: COUNT MISMATCH eligible=%d expected=%d "
                "— zero writes, aborting",
                len(eligible_rows), expected_count,
            )
            return result
    else:
        result["count_match"] = True

    if not eligible_rows:
        log.info(
            "live_overnight_rescue: no eligible rows found for client=%s date=%s",
            client_id, trading_date,
        )
        return result

    log.info(
        "live_overnight_rescue: %d rows eligible for recovery "
        "(count_match=%s dry_run=%s)",
        len(eligible_rows), result["count_match"], dry_run,
    )

    # ── Dry-run: return preview, no writes ────────────────────────────────────
    if dry_run:
        result["writes"] = 0
        log.info(
            "live_overnight_rescue: DRY_RUN preview complete eligible=%d writes=0",
            len(eligible_rows),
        )
        return result

    # ── Live run: emit decision events then atomically requeue ────────────────
    log.warning(
        "live_overnight_rescue: LIVE WRITE — transitioning %d rows REJECTED→WATCHING "
        "client=%s date=%s recovery_run_id=%s",
        len(eligible_rows), client_id, trading_date, recovery_run_id,
    )

    # 1. Emit durable recovery decision events
    for row in eligible_rows:
        _emit_recovery_decision_event(
            client_id=client_id,
            row=row,
            trading_date=trading_date,
            recovery_run_id=recovery_run_id,
            recovered_at=recovered_at,
        )

    # 2. Atomically requeue
    payloads_by_id = {r["id"]: r.get("payload") or {} for r in eligible_rows}
    row_ids = [r["id"] for r in eligible_rows]

    writes = _atomic_requeue_rows(
        client_id=client_id,
        row_ids=row_ids,
        payloads_by_id=payloads_by_id,
        trading_date=trading_date,
        recovery_run_id=recovery_run_id,
        recovered_at=recovered_at,
    )

    result["writes"] = writes
    log.warning(
        "live_overnight_rescue: COMPLETE writes=%d/%d eligible "
        "client=%s date=%s recovery_run_id=%s",
        writes, len(eligible_rows), client_id, trading_date, recovery_run_id,
    )
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
    Full recovery orchestration:
      1. Run fenced rescue (rescue_live_overnight_regime_rejections).
      2. On non-dry success: run_overnight_reeval(..., force=True) via runner.
      3. Run post-overnight morning handoff (if available).
      4. Run preopen readiness.
      5. Return all counts and diagnostics.

    The canonical overnight path enforces:
      - Monday structural validation and trigger geometry checks
      - Contract selection (deferred to breach if pre-market)
      - Account risk through master_control
      - Entry watcher arming
      - Already-through-trigger protection (entry_watcher refuses stale triggers)

    No broker orders are submitted until a breach fires at market open.

    Args:
        runner:         Initialized ClientRunner in LIVE mode.
        trading_date:   Monday ISO date (e.g. "2026-07-20").
        dry_run:        Default True.
        expected_count: If supplied, rescue aborts on count mismatch.

    Returns:
        dict with keys: rescue_result, reeval_result, readiness_result, errors.
    """
    orchestration: dict[str, Any] = {
        "rescue_result":   None,
        "reeval_result":   None,
        "readiness_result": None,
        "errors":          [],
        "dry_run":         dry_run,
        "trading_date":    trading_date,
    }

    client_id = getattr(runner, "email", None) or getattr(runner, "client_id", None)
    mode      = str(getattr(runner, "mode", "") or "").strip().upper()

    if not client_id:
        orchestration["errors"].append("runner has no client_id / email")
        return orchestration

    if mode != "LIVE":
        orchestration["errors"].append(
            f"recover_and_rerun_live_overnight is LIVE-only (runner mode={mode})"
        )
        return orchestration

    # ── Step 1: Run the fenced rescue ─────────────────────────────────────────
    try:
        rescue_result = rescue_live_overnight_regime_rejections(
            client_id=client_id,
            execution_mode="live",
            trading_date=trading_date,
            dry_run=dry_run,
            expected_count=expected_count,
        )
        orchestration["rescue_result"] = rescue_result
    except Exception as exc:
        orchestration["errors"].append(f"rescue failed: {exc}")
        log.error("recover_and_rerun_live_overnight: rescue exception: %s", exc)
        return orchestration

    if rescue_result.get("errors"):
        orchestration["errors"].extend(rescue_result["errors"])
        return orchestration

    if dry_run:
        log.info(
            "recover_and_rerun_live_overnight: DRY_RUN complete "
            "eligible=%d writes=0 — not proceeding to reeval",
            rescue_result.get("eligible", 0),
        )
        return orchestration

    if not rescue_result.get("writes", 0):
        log.warning(
            "recover_and_rerun_live_overnight: rescue produced 0 writes "
            "— skipping reeval (nothing to re-evaluate)"
        )
        return orchestration

    # ── Step 2: Re-run overnight reeval via canonical path ────────────────────
    try:
        from ap_overnight_reeval import run_overnight_reeval

        core        = getattr(runner, "core", None)
        broker      = getattr(runner, "broker", None)
        data_broker = getattr(runner, "data_broker", None) or getattr(runner, "databroker", None)
        mc          = getattr(runner, "master_control", None)
        selector    = getattr(runner, "contract_selector", None)
        osm         = getattr(runner, "order_state_machine", None)
        watcher     = getattr(core, "entry_watcher", None) if core else None
        pos_mgr     = getattr(runner, "position_manager", None)
        exit_eng    = getattr(core, "exit_eng", None) if core else None

        reeval_result = run_overnight_reeval(
            client_id=client_id,
            broker=broker,
            data_broker=data_broker,
            master_control=mc,
            contract_selector=selector,
            order_state_machine=osm,
            entry_watcher=watcher,
            position_manager=pos_mgr,
            exit_eng=exit_eng,
            force=True,
        )
        orchestration["reeval_result"] = reeval_result
        log.info(
            "recover_and_rerun_live_overnight: reeval complete %s",
            {k: reeval_result.get(k) for k in ("processed", "armed", "rejected", "errors")},
        )
    except Exception as exc:
        orchestration["errors"].append(f"reeval failed: {exc}")
        log.error("recover_and_rerun_live_overnight: reeval exception: %s", exc, exc_info=True)

    # ── Step 3: Run post-overnight morning handoff (best-effort) ──────────────
    try:
        from ap.morning_handoff import run_post_overnight_reeval_handoff
        handoff_result = run_post_overnight_reeval_handoff(
            client_id=client_id,
            execution_mode="live",
            reeval_result=orchestration.get("reeval_result") or {},
        )
        orchestration["handoff_result"] = handoff_result
    except Exception as exc:
        orchestration["handoff_result"] = {"error": str(exc)}
        log.warning(
            "recover_and_rerun_live_overnight: handoff failed (non-fatal): %s", exc
        )

    # ── Step 4: Run preopen readiness ─────────────────────────────────────────
    try:
        from ap.preopen_readiness import run_preopen_autonomous_readiness
        readiness_result = run_preopen_autonomous_readiness(
            client_id=client_id,
            execution_mode="live",
            dry_run=False,
            stage="recovery",
            runner=runner,
        )
        orchestration["readiness_result"] = readiness_result
        log.info(
            "recover_and_rerun_live_overnight: readiness status=%s errors=%s",
            readiness_result.get("status"),
            readiness_result.get("errors"),
        )
    except Exception as exc:
        orchestration["readiness_result"] = {"error": str(exc)}
        log.warning(
            "recover_and_rerun_live_overnight: readiness check failed (non-fatal): %s", exc
        )

    return orchestration
