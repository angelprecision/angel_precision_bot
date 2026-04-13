# ap/queue.py -- UNIFIED CONTROL QUEUE (Postgres)
# =============================================================================
# Architecture:
#   enqueue_signal() → trade_queue table
#   worker_loop()    → claim job → master_control.evaluate() → 
#                      contract_selector.select() → order_state_machine.create_entry_order()
#                      → APEntryWatcher (breach) OR immediate execution
#
# What was removed vs old queue:
#   ❌ _check_rate_limit()     -- owned by master_control (trades_today gate)
#   ❌ _is_triggered()         -- owned by APEntryWatcher
#   ❌ _get_stock_price()      -- owned by APEntryWatcher
#   ❌ trigger requeue loop    -- owned by APEntryWatcher
#   ❌ process_signal() direct -- replaced by control path
#   ❌ ev_score legacy filter  -- all signals go through master control now
#   ❌ ? placeholders          -- all %s (Postgres)
#
# What was kept:
#   ✅ enqueue_signal()        -- idempotent insert
#   ✅ _claim_one_job()        -- atomic claim + stale reclaim
#   ✅ _mark_job()             -- terminal status
#   ✅ worker_loop()           -- poll + dispatch
# =============================================================================

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Optional

from ap.db import conn, run_with_retry, init_db
from ap.state import update_state
from ap.utils import now_utc_iso

log = logging.getLogger("ap.queue")

POLL_INTERVAL         = float(os.getenv("QUEUE_POLL_INTERVAL", "1.0"))
PROCESSING_STALE_SECS = int(os.getenv("PROCESSING_STALE_SECS", "120"))  # 2 min -- was 15min


# =============================================================================
# HELPERS
# =============================================================================

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_dumps(data: Any) -> str:
    return json.dumps(data, separators=(",", ":"), ensure_ascii=False)


def _parse_payload(raw) -> dict:
    """Postgres JSONB returns dict; handle string fallback."""
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        return json.loads(raw)
    return {}


# =============================================================================
# ENQUEUE -- idempotent insert
# =============================================================================

def enqueue_signal(
    sig,
    client_id: str = "default",
    idempotency_key: str | None = None,
) -> bool:
    """
    Push a signal into trade_queue.
    Returns True if inserted, False if duplicate (idempotency_key conflict).
    """
    if isinstance(sig, dict):
        payload = sig
    elif hasattr(sig, "model_dump"):
        payload = sig.model_dump()
    elif hasattr(sig, "dict"):
        payload = sig.dict()
    else:
        raise TypeError(f"Unsupported signal type: {type(sig)}")

    signal_id = payload.get("signal_id") or f"signal_{_now_iso()}"
    if not idempotency_key:
        idempotency_key = f"{client_id}:{signal_id}"

    def _ins():
        with conn() as c:
            c.execute(
                """
                INSERT INTO trade_queue (
                    client_id, signal_id, created_ts, status, payload, idempotency_key
                )
                VALUES (%s, %s, NOW(), 'NEW', %s, %s)
                ON CONFLICT (idempotency_key) DO NOTHING
                """,
                (client_id, signal_id, _json_dumps(payload), idempotency_key),
            )

    try:
        run_with_retry(_ins)
        log.info(f"✅ Enqueued: {signal_id} client={client_id}")
        return True
    except Exception as e:
        msg = str(e).lower()
        if "unique" in msg or "conflict" in msg:
            log.debug(f"Duplicate ignored: {signal_id}")
            return False
        raise


# =============================================================================
# JOB MANAGEMENT
# =============================================================================

def _mark_job(
    job_id: int,
    status: str,
    *,
    result: dict | None = None,
    error: str | None = None,
):
    def _fn():
        with conn() as c:
            c.execute(
                """
                UPDATE trade_queue
                SET status=%s,
                    finished_ts=NOW(),
                    result_json=%s,
                    last_error=%s
                WHERE id=%s
                """,
                (
                    status,
                    _json_dumps(result) if result is not None else None,
                    error,
                    job_id,
                ),
            )
    run_with_retry(_fn)


def _claim_one_job(client_id: str = "default") -> Optional[dict]:
    """
    Atomically claim one NEW job for this specific client_id.

    Uses a single CTE with FOR UPDATE SKIP LOCKED -- eliminates the 3-query
    SELECT / UPDATE / re-read race condition. Only one worker can ever claim
    a given row because SKIP LOCKED skips rows held by another transaction.

    Also reclaims stale PROCESSING jobs in the same transaction.
    """
    def _atomic_claim():
        with conn() as c:
            # Step 1: reclaim stale PROCESSING rows for this client
            c.execute(
                f"""
                UPDATE trade_queue
                SET status      = 'NEW',
                    started_ts  = NULL,
                    last_error  = COALESCE(last_error,'') || ' | reclaimed_stale'
                WHERE status    = 'PROCESSING'
                  AND client_id = %s
                  AND started_ts IS NOT NULL
                  AND started_ts < NOW() - INTERVAL '{PROCESSING_STALE_SECS} seconds'
                """,
                (client_id,),
            )

            # Step 2: atomic claim via CTE -- FOR UPDATE SKIP LOCKED guarantees
            # no two workers ever claim the same row, even across gunicorn workers.
            c.execute(
                """
                WITH next_job AS (
                    SELECT id
                    FROM   trade_queue
                    WHERE  status    = 'NEW'
                      AND  client_id = %s
                    ORDER BY created_ts ASC
                    LIMIT  1
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE trade_queue tq
                SET    status     = 'PROCESSING',
                       started_ts = NOW()
                FROM   next_job
                WHERE  tq.id = next_job.id
                RETURNING tq.id, tq.client_id, tq.signal_id, tq.payload
                """,
                (client_id,),
            )
            return c.fetchone()

    return run_with_retry(_atomic_claim)


# =============================================================================
# DISPATCH -- master control path
# =============================================================================

def _dispatch(
    job_id: int,
    client_id: str,
    signal_id: str,
    payload: dict,
    *,
    master_control,
    contract_selector,
    order_state_machine,
    entry_watcher,
):
    """
    Unified control path:
      1. master_control.evaluate()      → gate with placeholder estimate → ApprovedExecutionPlan
      2. contract_selector.select()     → real premium, updates plan in-place
      3. master_control.revalidate()    → re-check capital/sector/ticker with REAL premium
      4. order_state_machine.create_entry_order()
      5. breach → entry_watcher  |  immediate → SUBMITTED transition
    """
    ticker = payload.get("ticker") or payload.get("symbol", "?")

    # ── 1. MASTER CONTROL (initial gate, placeholder estimate) ────────────────
    try:
        decision = master_control.evaluate(payload, client_id=client_id)
    except Exception as e:
        log.error(f"[{ticker}] master_control.evaluate() failed: {e}")
        _mark_job(job_id, "ERROR", error=f"master_control_error: {e}")
        return

    if not decision.ok:
        log.info(f"[{ticker}] BLOCKED | stage={decision.stage} reason={decision.reason}")
        _mark_job(job_id, "REJECTED",
                  result={"stage": decision.stage, "reason": decision.reason})
        return

    plan = decision.plan

    # ── 2. CONTRACT SELECTION -- replaces placeholder with real premium ─────────
    if contract_selector:
        try:
            selected = contract_selector.select(plan)
            if selected is None:
                # Fail loudly -- no silent fallback, no improvised contract
                log.warning(f"[{ticker}] Contract selection failed -- no suitable contract")
                _mark_job(job_id, "REJECTED",
                          result={"stage": "contract_selection",
                                  "reason": "no_contract_found",
                                  "ticker": ticker})
                return
            # plan.contract_symbol, plan.limit_price, plan.contracts,
            # plan.max_position_usd now reflect REAL premium (set by selector)
            log.info(
                f"[{ticker}] Contract selected: {selected.contract_symbol} "
                f"@ ${selected.mid:.2f} x{plan.contracts} "
                f"cost=${plan.max_position_usd:.0f}"
            )
        except Exception as e:
            log.error(f"[{ticker}] contract_selector.select() raised: {e}")
            _mark_job(job_id, "ERROR", error=f"contract_selector_error: {e}")
            return
    else:
        log.debug(f"[{ticker}] No contract selector -- plan uses placeholder sizing")

    # ── 3. RE-VALIDATE with REAL premium ──────────────────────────────────────
    # Now that plan.max_position_usd is the actual cost, re-check capital/sector/ticker caps.
    # This replaces the placeholder-based gate from step 1.
    revalidation = master_control.revalidate_exposure(plan, client_id=client_id)
    if not revalidation.ok:
        log.warning(
            f"[{ticker}] BLOCKED at re-validation (real premium) | "
            f"reason={revalidation.reason}"
        )
        _mark_job(job_id, "REJECTED",
                  result={"stage": "revalidation", "reason": revalidation.reason,
                          "real_cost": plan.max_position_usd})
        return

    # ── 4. ORDER STATE MACHINE ─────────────────────────────────────────────────
    try:
        local_order_id = order_state_machine.create_entry_order(plan)
        log.info(
            f"[{ticker}] Entry order created: {local_order_id} "
            f"contract={getattr(plan, 'contract_symbol', '?')}"
        )
    except Exception as e:
        log.error(f"[{ticker}] create_entry_order() failed: {e}")
        _mark_job(job_id, "ERROR", error=f"order_create_error: {e}")
        return

    # ── 5. ROUTE -- BREACH vs IMMEDIATE ────────────────────────────────────────
    trigger_type = getattr(plan, "trigger_type", "immediate")

    if trigger_type == "breach" and entry_watcher:
        try:
            entry_watcher.watch(plan=plan, local_order_id=local_order_id)
            log.info(
                f"[{ticker}] Handed to entry watcher | "
                f"trigger=${getattr(plan, 'trigger_price', '?')}"
            )
            _mark_job(job_id, "WATCHING",
                      result={"plan_id": plan.plan_id,
                              "local_order_id": local_order_id,
                              "contract": getattr(plan, "contract_symbol", ""),
                              "real_cost": plan.max_position_usd,
                              "trigger_type": "breach"})
        except Exception as e:
            log.error(f"[{ticker}] entry_watcher.watch() failed: {e}")
            _mark_job(job_id, "ERROR", error=f"watcher_error: {e}")
    else:
        try:
            order_state_machine.transition(local_order_id, "SUBMITTED",
                                           submitted_ts=now_utc_iso())
            log.info(f"[{ticker}] Order submitted immediately: {local_order_id}")
            _mark_job(job_id, "SUBMITTED",
                      result={"plan_id": plan.plan_id,
                              "local_order_id": local_order_id,
                              "contract": getattr(plan, "contract_symbol", ""),
                              "real_cost": plan.max_position_usd,
                              "trigger_type": "immediate"})
        except Exception as e:
            log.error(f"[{ticker}] immediate submit failed: {e}")
            _mark_job(job_id, "ERROR", error=f"submit_error: {e}")


# =============================================================================
# WORKER LOOP
# =============================================================================

def worker_loop(
    broker,
    poll_seconds: float = POLL_INTERVAL,
    *,
    master_control=None,
    contract_selector=None,
    order_state_machine=None,
    entry_watcher=None,
    client_id: str = "default",
    stop_event=None,           # threading.Event -- worker exits when set
    live_mode: bool = False,   # if True, legacy fallback is DISABLED
):
    """
    Main queue worker.

    Pass master_control, contract_selector, order_state_machine, entry_watcher
    from client_runner so the worker has the full control stack.

    stop_event: threading.Event -- set by ClientRunner.stop() to cleanly exit.
    live_mode:  if True, legacy process_signal() fallback is disabled entirely.
                Paper mode can fall back; live mode requires full control stack.
    """
    init_db()
    mode_label = "control" if master_control else ("LIVE-NO-FALLBACK" if live_mode else "legacy")
    log.info(
        f"🤖 Worker started | client={client_id} poll={poll_seconds}s path={mode_label}"
    )

    while True:
        # Clean shutdown check
        if stop_event and stop_event.is_set():
            log.info(f"[{client_id}] Worker stop_event set -- exiting cleanly")
            return

        # Heartbeat -- advance bot_status AND self-healing stall detection
        try:
            update_state({"last_heartbeat_ts": now_utc_iso()}, client_id=client_id)
        except Exception:
            pass
        try:
            from ap.self_healing import get_healer
            h = get_healer()
            if h:
                h.heartbeat(client_id, "worker")
        except Exception:
            pass

        job    = None
        job_id = None

        try:
            job = _claim_one_job(client_id=client_id)
            if not job:
                time.sleep(poll_seconds)
                continue

            job_id    = int(job["id"])
            job_cid   = job["client_id"]
            signal_id = job["signal_id"]

            try:
                payload = _parse_payload(job["payload"])
            except Exception as e:
                log.error(f"Payload parse error: {e}")
                _mark_job(job_id, "REJECTED", error=f"parse_error: {e}")
                continue

            # ── NEW CONTROL PATH ──────────────────────────────────────────────
            if master_control is not None:
                _dispatch(
                    job_id, job_cid, signal_id, payload,
                    master_control=master_control,
                    contract_selector=contract_selector,
                    order_state_machine=order_state_machine,
                    entry_watcher=entry_watcher,
                )

            # ── LEGACY FALLBACK (paper mode only) ────────────────────────────
            else:
                # LIVE MODE: fallback is NEVER allowed -- block and alert
                if live_mode:
                    log.error(
                        f"[{client_id}] LIVE MODE -- master_control required. "
                        f"Rejecting {signal_id} without fallback."
                    )
                    _mark_job(job_id, "REJECTED",
                              error="live_mode_no_fallback_no_master_control")
                else:
                    log.debug(f"[legacy/paper] processing {signal_id}")
                    try:
                        from ap.execution import process_signal
                        result = process_signal(
                            broker=broker,
                            client_id=job_cid,
                            signal_payload=payload,
                        )
                        ok     = bool(result.get("ok"))
                        status = "DONE" if ok else "REJECTED"
                        error  = None if ok else (
                            result.get("error") or result.get("reason") or "unknown"
                        )
                        _mark_job(job_id, status, result=result, error=error)
                        if ok:
                            log.info(f"✅ [legacy/paper] {signal_id}: {result.get('contract')}")
                        else:
                            log.warning(f"❌ [legacy/paper] {signal_id}: {error}")
                    except Exception as e:
                        log.error(f"[legacy/paper] execution error: {e}", exc_info=True)
                        _mark_job(job_id, "ERROR", error=str(e))

        except Exception as e:
            log.error(f"Worker loop error: {e}", exc_info=True)
            if job_id is not None:
                try:
                    _mark_job(job_id, "ERROR", error=str(e))
                except Exception:
                    pass
            time.sleep(poll_seconds)
