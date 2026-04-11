# ap/queue.py — UNIFIED CONTROL QUEUE (Postgres)
# =============================================================================
# Architecture:
#   enqueue_signal() → trade_queue table
#   worker_loop()    → claim job → master_control.evaluate() → 
#                      contract_selector.select() → order_state_machine.create_entry_order()
#                      → APEntryWatcher (breach) OR immediate execution
#
# What was removed vs old queue:
#   ❌ _check_rate_limit()     — owned by master_control (trades_today gate)
#   ❌ _is_triggered()         — owned by APEntryWatcher
#   ❌ _get_stock_price()      — owned by APEntryWatcher
#   ❌ trigger requeue loop    — owned by APEntryWatcher
#   ❌ process_signal() direct — replaced by control path
#   ❌ ev_score legacy filter  — all signals go through master control now
#   ❌ ? placeholders          — all %s (Postgres)
#
# What was kept:
#   ✅ enqueue_signal()        — idempotent insert
#   ✅ _claim_one_job()        — atomic claim + stale reclaim
#   ✅ _mark_job()             — terminal status
#   ✅ worker_loop()           — poll + dispatch
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
PROCESSING_STALE_SECS = int(os.getenv("PROCESSING_STALE_SECS", "900"))


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
# ENQUEUE — idempotent insert
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


def _claim_one_job() -> Optional[dict]:
    """
    Atomically claim one NEW job from trade_queue.
    First reclaims any stale PROCESSING jobs (all signals, no ev_score filter).
    Returns row dict or None.
    """
    # Reclaim stale PROCESSING jobs
    def _reclaim():
        with conn() as c:
            c.execute(
                f"""
                UPDATE trade_queue
                SET status='NEW',
                    started_ts=NULL,
                    last_error=COALESCE(last_error,'') || ' | reclaimed_stale',
                    created_ts=NOW()
                WHERE status='PROCESSING'
                  AND started_ts IS NOT NULL
                  AND started_ts < NOW() - INTERVAL '{PROCESSING_STALE_SECS} seconds'
                """,
            )
    run_with_retry(_reclaim)

    # Find next NEW job — ALL signals, master control decides what to do with them
    def _find():
        with conn() as c:
            c.execute(
                """
                SELECT id, client_id, signal_id, payload
                FROM trade_queue
                WHERE status='NEW'
                ORDER BY created_ts ASC
                LIMIT 1
                """
            )
            return c.fetchone()

    job = run_with_retry(_find)
    if not job:
        return None

    job_id = int(job["id"])

    # Atomic claim
    def _claim():
        with conn() as c:
            c.execute(
                """
                UPDATE trade_queue
                SET status='PROCESSING', started_ts=NOW()
                WHERE id=%s AND status='NEW'
                """,
                (job_id,),
            )
            return c.rowcount

    if run_with_retry(_claim) != 1:
        return None

    # Re-read claimed row
    def _reread():
        with conn() as c:
            c.execute(
                "SELECT id, client_id, signal_id, payload FROM trade_queue WHERE id=%s",
                (job_id,),
            )
            return c.fetchone()

    return run_with_retry(_reread)


# =============================================================================
# DISPATCH — master control path
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
    New unified control path:
      1. master_control.evaluate()   → gate + build ApprovedExecutionPlan
      2. contract_selector.select()  → pick best contract, update plan
      3. order_state_machine.create_entry_order()
      4. breach trigger → hand to entry_watcher
         immediate     → submit execution directly
    """
    ticker = payload.get("ticker") or payload.get("symbol", "?")

    # ── 1. MASTER CONTROL ────────────────────────────────────────────────────
    try:
        decision = master_control.evaluate(payload, client_id=client_id)
    except Exception as e:
        log.error(f"[{ticker}] master_control.evaluate() failed: {e}")
        _mark_job(job_id, "ERROR", error=f"master_control_error: {e}")
        return

    if not decision.ok:
        log.info(
            f"[{ticker}] BLOCKED by master control | "
            f"stage={decision.stage} reason={decision.reason}"
        )
        _mark_job(
            job_id, "REJECTED",
            result={"stage": decision.stage, "reason": decision.reason},
        )
        return

    plan = decision.plan

    # ── 2. CONTRACT SELECTION ─────────────────────────────────────────────────
    if contract_selector:
        try:
            selected = contract_selector.select(plan)
            if selected is None:
                log.warning(
                    f"[{ticker}] No suitable contract found — blocking trade"
                )
                _mark_job(
                    job_id, "REJECTED",
                    result={"stage": "contract_selection", "reason": "no_contract_found"},
                )
                return
            log.info(
                f"[{ticker}] Contract selected: {selected.contract_symbol} "
                f"@ ${selected.mid:.2f} x{plan.contracts}"
            )
        except Exception as e:
            log.error(f"[{ticker}] contract_selector.select() failed: {e}")
            _mark_job(job_id, "ERROR", error=f"contract_selector_error: {e}")
            return
    else:
        log.debug(f"[{ticker}] No contract selector — using plan as-is")

    # ── 3. ORDER STATE MACHINE ────────────────────────────────────────────────
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

    # ── 4. ROUTE — BREACH vs IMMEDIATE ────────────────────────────────────────
    trigger_type = getattr(plan, "trigger_type", "immediate")

    if trigger_type == "breach" and entry_watcher:
        # Hand to entry watcher — it will poll price, submit when triggered
        try:
            entry_watcher.watch(plan=plan, local_order_id=local_order_id)
            log.info(
                f"[{ticker}] Handed to entry watcher | "
                f"trigger=${getattr(plan, 'trigger_price', '?')}"
            )
            _mark_job(
                job_id, "WATCHING",
                result={
                    "plan_id":        plan.plan_id,
                    "local_order_id": local_order_id,
                    "contract":       getattr(plan, "contract_symbol", ""),
                    "trigger_type":   "breach",
                },
            )
        except Exception as e:
            log.error(f"[{ticker}] entry_watcher.watch() failed: {e}")
            _mark_job(job_id, "ERROR", error=f"watcher_error: {e}")
    else:
        # Immediate execution — submit order now via order state machine
        try:
            order_state_machine.transition(
                local_order_id,
                "SUBMITTED",
                submitted_ts=now_utc_iso(),
            )
            log.info(f"[{ticker}] Order submitted immediately: {local_order_id}")
            _mark_job(
                job_id, "SUBMITTED",
                result={
                    "plan_id":        plan.plan_id,
                    "local_order_id": local_order_id,
                    "contract":       getattr(plan, "contract_symbol", ""),
                    "trigger_type":   "immediate",
                },
            )
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
):
    """
    Main queue worker.

    Pass master_control, contract_selector, order_state_machine, entry_watcher
    from client_runner so the worker has the full control stack.

    If master_control is None, falls back to legacy process_signal() path
    so the bot stays live while you migrate.
    """
    init_db()
    log.info(
        f"🤖 Worker started (poll={poll_seconds}s) | "
        f"path={'control' if master_control else 'legacy'}"
    )

    while True:
        # Heartbeat
        try:
            update_state({"last_heartbeat_ts": now_utc_iso()}, client_id=client_id)
        except Exception:
            pass

        job    = None
        job_id = None

        try:
            job = _claim_one_job()
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

            # ── LEGACY FALLBACK (while migrating) ─────────────────────────────
            else:
                log.debug(f"[legacy] processing {signal_id}")
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
                        log.info(f"✅ [legacy] {signal_id}: {result.get('contract')}")
                    else:
                        log.warning(f"❌ [legacy] {signal_id}: {error}")
                except Exception as e:
                    log.error(f"[legacy] execution error: {e}", exc_info=True)
                    _mark_job(job_id, "ERROR", error=str(e))

        except Exception as e:
            log.error(f"Worker loop error: {e}", exc_info=True)
            if job_id is not None:
                try:
                    _mark_job(job_id, "ERROR", error=str(e))
                except Exception:
                    pass
            time.sleep(poll_seconds)
