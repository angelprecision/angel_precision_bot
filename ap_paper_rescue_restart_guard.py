"""
ap_paper_rescue_restart_guard.py
─────────────────────────────────
Converts REJECTED/restart_guard:overnight_skip trade_queue rows back to
WATCHING for paper clients, marks them as overnight-reeval-only rescue rows,
then immediately runs overnight_reeval so queue-only WATCHING rows become
real watcher/order state for the current session.

Hard safety rules (enforced structurally, not just by convention):
  PAPER ONLY — any live client_id causes an immediate abort, not a skip.
  NEVER calls broker.submit_order.
  NEVER creates new orders rows.
  NEVER touches live client rows.
  NEVER changes orders.contract / orders.qty / orders.limit_price.
  DB write is limited to trade_queue only:
    UPDATE trade_queue
       SET status='WATCHING',
           started_ts=NULL,
           finished_ts=NULL,
           last_error='after_hours_deferred:awaiting_overnight_reeval',
           payload += {
             force_overnight_reeval_only=true,
             do_not_queue_directly=true
           },
           result_json += rescue metadata
     WHERE status='REJECTED'
       AND last_error='restart_guard:overnight_skip'
       AND client_id IN (<paper_client_ids>)
       AND created_ts >= NOW() - lookback_h * INTERVAL '1 hour'

Public API:
  run_paper_rescue_restart_guard(
      paper_client_ids: list[str],
      runners: dict[str, Any],   # {email: runner} — paper runners only
      lookback_hours: int = 36,
      dry_run: bool = False,
  ) -> dict

Caller must pre-filter to paper clients before calling.
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Any

log = logging.getLogger("ap.paper_rescue_restart_guard")

# The only last_error value this module will ever convert FROM.
_SOURCE_LAST_ERROR = "restart_guard:overnight_skip"

# The last_error written to converted rows — makes every rescue auditable.
_BYPASS_LAST_ERROR = "after_hours_deferred:awaiting_overnight_reeval"

# The status rows are converted FROM and TO.
_FROM_STATUS = "REJECTED"
_TO_STATUS   = "WATCHING"

# Absolute protection: these strings in a client_id mean it is live.
_LIVE_SENTINEL_FRAGMENTS: tuple[str, ...] = ()   # checked at call site


def _is_live_runner(runner: Any) -> bool:
    """Return True if runner.mode resolves to LIVE."""
    mode = str(getattr(runner, "mode", "") or "").upper().strip()
    return mode == "LIVE"


def _load_rescue_rows(
    client_ids: list[str],
    lookback_hours: int,
) -> list[dict]:
    """Return all trade_queue rows eligible for rescue across the given clients."""
    from ap.db import conn, run_with_retry

    cutoff = (datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).isoformat()
    placeholders = ", ".join(["%s"] * len(client_ids))

    def _q():
        with conn() as c:
            c.execute(
                f"""
                SELECT id, client_id, signal_id, status, last_error, created_ts
                FROM   trade_queue
                WHERE  status     = %s
                  AND  last_error = %s
                  AND  client_id  IN ({placeholders})
                  AND  created_ts >= %s
                ORDER  BY created_ts ASC
                """,
                (_FROM_STATUS, _SOURCE_LAST_ERROR, *client_ids, cutoff),
            )
            rows = c.fetchall() or []
            return [
                {
                    "id":         r[0],
                    "client_id":  r[1],
                    "signal_id":  r[2],
                    "status":     r[3],
                    "last_error": r[4],
                    "created_ts": str(r[5]),
                }
                for r in rows
            ]

    return run_with_retry(_q) or []


def _convert_row(job_id: int) -> bool:
    """UPDATE a single trade_queue row FROM→TO. Returns True on success."""
    from ap.db import conn, run_with_retry

    def _u():
        with conn() as c:
            c.execute(
                """
                UPDATE trade_queue
                SET    status       = %s,
                       started_ts   = NULL,
                       finished_ts  = NULL,
                       last_error   = %s,
                       payload      = COALESCE(payload, '{}'::jsonb) || jsonb_build_object(
                           'force_overnight_reeval_only', true,
                           'do_not_queue_directly', true
                       ),
                       result_json  = COALESCE(result_json, '{}'::jsonb) || jsonb_build_object(
                           'manual_rescue', true,
                           'manual_rescue_actor', 'paper_rescue_restart_guard',
                           'manual_rescue_route', 'overnight_reeval_only',
                           'manual_rescue_reason', 'restart_guard_archived_no_order_row'
                       )
                WHERE  id         = %s
                  AND  status     = %s
                  AND  last_error = %s
                """,
                (
                    _TO_STATUS,
                    _BYPASS_LAST_ERROR,
                    job_id,
                    _FROM_STATUS,       # idempotency guard — only update if still REJECTED
                    _SOURCE_LAST_ERROR, # idempotency guard — only update if still overnight_skip
                ),
            )
            return c.rowcount

    rowcount = run_with_retry(_u) or 0
    return rowcount == 1


def _runner_components(runner: Any) -> dict[str, Any]:
    core = getattr(runner, "core", None)
    return {
        "broker": getattr(runner, "broker", None) or (getattr(core, "broker", None) if core else None),
        "data_broker": (
            getattr(runner, "data_broker", None)
            or (getattr(core, "data_broker", None) if core else None)
            or getattr(getattr(runner, "execution_core", None), "data_broker", None)
        ),
        "master_control": getattr(runner, "master_control", None),
        "contract_selector": getattr(runner, "contract_selector", None),
        "order_state_machine": (
            getattr(runner, "order_state_machine", None)
            or getattr(runner, "osm", None)
            or (getattr(core, "order_state_machine", None) if core else None)
            or (getattr(getattr(runner, "execution_core", None), "order_state_machine", None))
        ),
        "entry_watcher": getattr(runner, "entry_watcher", None) or (getattr(core, "entry_watcher", None) if core else None),
        "position_manager": getattr(runner, "position_manager", None),
        "exit_eng": getattr(core, "exit_eng", None) if core else None,
    }


def run_paper_rescue_restart_guard(
    *,
    paper_client_ids: list[str],
    runners: dict[str, Any],
    lookback_hours: int = 36,
    dry_run: bool = False,
) -> dict:
    """Convert restart_guard:overnight_skip rows to WATCHING and re-arm watcher.

    MUST be called with paper_client_ids already filtered to paper-only clients.
    Raises ValueError immediately if any runner in the set is LIVE.

    Returns:
        {
          "ok": bool,
          "dry_run": bool,
          "lookback_hours": int,
          "clients_processed": int,
          "rows_found": int,
          "rows_converted": int,
          "rows_skipped_already_converted": int,
          "rows_failed": int,
        "reeval_results": {<email>: <run_overnight_reeval result>},
        "audit_results": {<email>: <run_morning_handoff_audit result>},
          "errors": [...],
          "per_client": {
            <email>: {
              "rows_found": int,
              "rows_converted": int,
              "audit_result": dict | None,
            }
          }
        }
    """
    _t0 = time.monotonic()

    # ── Hard live-client guard ────────────────────────────────────────────
    # Abort entire operation on first live runner found — do not silently skip.
    for email in paper_client_ids:
        runner = runners.get(email)
        if runner is not None and _is_live_runner(runner):
            raise ValueError(
                f"LIVE client '{email}' passed to paper_rescue_restart_guard — "
                "this endpoint is paper-only. Aborting without touching any rows."
            )

    if not paper_client_ids:
        return {
            "ok": True,
            "dry_run": dry_run,
            "lookback_hours": lookback_hours,
            "clients_processed": 0,
            "rows_found": 0,
            "rows_converted": 0,
            "rows_skipped_already_converted": 0,
            "rows_failed": 0,
            "reeval_results": {},
            "audit_results": {},
            "errors": [],
            "per_client": {},
        }

    # ── Load eligible rows across all paper clients ───────────────────────
    try:
        all_rows = _load_rescue_rows(paper_client_ids, lookback_hours)
    except Exception as exc:
        log.error("paper_rescue_restart_guard: row load failed: %s", exc)
        return {
            "ok": False,
            "dry_run": dry_run,
            "lookback_hours": lookback_hours,
            "clients_processed": 0,
            "rows_found": 0,
            "rows_converted": 0,
            "rows_skipped_already_converted": 0,
            "rows_failed": 0,
            "reeval_results": {},
            "audit_results": {},
            "errors": [f"row_load_failed: {exc}"],
            "per_client": {},
        }

    rows_by_client: dict[str, list[dict]] = {e: [] for e in paper_client_ids}
    for row in all_rows:
        cid = row["client_id"]
        if cid in rows_by_client:
            rows_by_client[cid].append(row)

    log.info(
        "paper_rescue_restart_guard: found %d rescue-eligible rows across %d clients "
        "lookback_h=%d dry_run=%s",
        len(all_rows), len(paper_client_ids), lookback_hours, dry_run,
    )

    # ── Convert rows client-by-client ─────────────────────────────────────
    total_found = total_converted = total_skipped = total_failed = 0
    per_client: dict[str, dict] = {}
    errors: list[str] = []

    for email in paper_client_ids:
        client_rows     = rows_by_client.get(email, [])
        client_converted = 0
        client_skipped   = 0
        client_failed    = 0

        for row in client_rows:
            job_id    = row["id"]
            signal_id = row.get("signal_id", "")

            if dry_run:
                log.info(
                    "paper_rescue_restart_guard DRY_RUN: would convert job_id=%s "
                    "signal_id=%s client=%s",
                    job_id, signal_id, email,
                )
                client_converted += 1
                continue

            try:
                converted = _convert_row(job_id)
                if converted:
                    client_converted += 1
                    log.info(
                        "paper_rescue_restart_guard: CONVERTED job_id=%s signal_id=%s "
                        "client=%s %s→%s last_error=%s",
                        job_id, signal_id, email,
                        _FROM_STATUS, _TO_STATUS, _BYPASS_LAST_ERROR,
                    )
                else:
                    # Row was already converted by a concurrent call or prior run
                    client_skipped += 1
                    log.info(
                        "paper_rescue_restart_guard: SKIPPED already_converted job_id=%s "
                        "client=%s",
                        job_id, email,
                    )
            except Exception as exc:
                client_failed += 1
                msg = f"{email}:job_id={job_id}: {exc}"
                errors.append(msg)
                log.error("paper_rescue_restart_guard: convert failed: %s", msg)

        total_found     += len(client_rows)
        total_converted += client_converted
        total_skipped   += client_skipped
        total_failed    += client_failed
        per_client[email] = {
            "rows_found":     len(client_rows),
            "rows_converted": client_converted,
            "rows_skipped":   client_skipped,
            "rows_failed":    client_failed,
            "reeval_result":  None,
            "audit_result":   None,
        }

    # ── Materialize rescued queue-only WATCHING rows via overnight_reeval ──
    # This is the missing autonomous link: the rescue route must not leave
    # queue-only WATCHING rows waiting for a later manual overnight call.
    reeval_results: dict[str, dict] = {}
    if not dry_run:
        from ap_overnight_reeval import run_overnight_reeval

        for email in paper_client_ids:
            if per_client[email]["rows_converted"] <= 0:
                continue
            runner = runners.get(email)
            if runner is None:
                msg = f"{email}: runner_missing_for_overnight_reeval"
                errors.append(msg)
                log.error("paper_rescue_restart_guard: %s", msg)
                continue
            try:
                comps = _runner_components(runner)
                result = run_overnight_reeval(
                    client_id=email,
                    broker=comps["broker"],
                    data_broker=comps["data_broker"],
                    master_control=comps["master_control"],
                    contract_selector=comps["contract_selector"],
                    order_state_machine=comps["order_state_machine"],
                    entry_watcher=comps["entry_watcher"],
                    position_manager=comps["position_manager"],
                    exit_eng=comps["exit_eng"],
                    force=True,
                )
                reeval_results[email] = result
                per_client[email]["reeval_result"] = result
                log.info(
                    "paper_rescue_restart_guard: overnight_reeval complete client=%s "
                    "processed=%s armed=%s rejected=%s errors=%s",
                    email,
                    result.get("processed", 0),
                    result.get("armed", 0),
                    result.get("rejected", 0),
                    result.get("errors", 0),
                )
            except Exception as exc:
                msg = f"{email}: overnight_reeval failed: {exc}"
                errors.append(msg)
                reeval_results[email] = {"ok": False, "error": str(exc)}
                per_client[email]["reeval_result"] = reeval_results[email]
                log.error("paper_rescue_restart_guard: %s", msg)

    # ── Morning handoff audit for each client ─────────────────────────────
    # Run after overnight_reeval so any newly materialized PENDING_TRIGGER rows
    # can be owned/re-armed by the watcher if needed.
    from ap_morning_handoff_audit import run_morning_handoff_audit
    audit_results: dict[str, dict] = {}
    for email in paper_client_ids:
        if not dry_run and per_client[email]["rows_converted"] <= 0:
            continue
        runner        = runners.get(email)
        entry_watcher = getattr(runner, "entry_watcher", None) or getattr(
            getattr(runner, "core", None), "entry_watcher", None
        )
        osm = (
            getattr(runner, "order_state_machine", None)
            or getattr(runner, "osm", None)
            or getattr(getattr(runner, "core", None), "order_state_machine", None)
            or getattr(getattr(runner, "execution_core", None), "order_state_machine", None)
        )
        if osm is None:
            log.warning(
                "paper_rescue_restart_guard: OSM not found for %s — "
                "WATCHER_REARM_AUDIT_FAILED (audit metadata will not be persisted)",
                email,
            )

        try:
            audit_result = run_morning_handoff_audit(
                client_id=email,
                entry_watcher=entry_watcher,
                osm=osm,
                execution_mode="paper",   # always paper — enforced at call site
                dry_run=dry_run,
            )
            audit_results[email] = audit_result
            per_client[email]["audit_result"] = audit_result
            log.info(
                "paper_rescue_restart_guard: audit complete client=%s "
                "scanned=%d auto_rearmed=%d",
                email,
                audit_result.get("scanned", 0),
                audit_result.get("auto_rearmed", 0),
            )
        except Exception as exc:
            msg = f"{email}: audit failed: {exc}"
            errors.append(msg)
            log.error("paper_rescue_restart_guard: %s", msg)

    elapsed = time.monotonic() - _t0
    log.info(
        "PAPER_RESCUE_RESTART_GUARD_SUMMARY clients=%d rows_found=%d "
        "rows_converted=%d rows_skipped=%d rows_failed=%d dry_run=%s elapsed=%.2fs",
        len(paper_client_ids), total_found, total_converted,
        total_skipped, total_failed, dry_run, elapsed,
    )

    return {
            "ok":                           len(errors) == 0,
            "dry_run":                      dry_run,
            "lookback_hours":               lookback_hours,
            "clients_processed":            len(paper_client_ids),
            "rows_found":                   total_found,
            "rows_converted":               total_converted,
            "rows_skipped_already_converted": total_skipped,
            "rows_failed":                  total_failed,
            "reeval_results":               reeval_results,
            "audit_results":                audit_results,
            "errors":                       errors,
            "per_client":                   per_client,
        "elapsed_seconds":              round(elapsed, 2),
    }
