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

# Deferred imports to avoid circular: app.py loads ap.db then ap.queue,
# but ap.queue importing ap.db at module level crashes while ap.db is mid-load.
from ap.state import update_state

def _conn():
    from ap.db import conn
    return conn

def _run_with_retry(fn, *args, **kwargs):
    from ap.db import run_with_retry
    return run_with_retry(fn, *args, **kwargs)

def _init_db():
    from ap.db import init_db
    return init_db

from ap.utils import now_utc_iso

log = logging.getLogger("ap.queue")

POLL_INTERVAL         = float(os.getenv("QUEUE_POLL_INTERVAL", "1.0"))
PROCESSING_STALE_SECS = int(os.getenv("PROCESSING_STALE_SECS", "120"))  # 2 min -- was 15min


# =============================================================================
# HELPERS
# =============================================================================

# MED-011: removed duplicate _now_iso(), using now_utc_iso from ap.utils
def _now_iso() -> str:
    return now_utc_iso()


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

    # Normalize ticker/symbol -- both fields must be present
    if not payload.get("ticker") and payload.get("symbol"):
        payload["ticker"] = payload["symbol"]
    if not payload.get("symbol") and payload.get("ticker"):
        payload["symbol"] = payload["ticker"]
    # Normalize score -- scanner may not send it; default to 65.0 (Tier B floor)
    if payload.get("score") is None:
        payload["score"] = 65.0
    if payload.get("ev_score") is None:
        payload["ev_score"] = payload["score"]

    # Normalize side/direction -- scanner embeds direction in signal_id
    # e.g. "2026-04-15:1-1:DLTR:Daily:PUT" -> side=PUT
    if not payload.get("side") and not payload.get("direction"):
        sig_id = str(payload.get("signal_id", "")).upper()
        if sig_id.endswith(":PUT") or ":PUT:" in sig_id:
            payload["side"] = "PUT"
            payload["direction"] = "PUT"
        else:
            payload["side"] = "CALL"
            payload["direction"] = "CALL"
    elif not payload.get("direction"):
        payload["direction"] = payload["side"]
    elif not payload.get("side"):
        payload["side"] = payload["direction"]
    signal_id = payload.get("signal_id") or f"signal_{_now_iso()}"
    if not idempotency_key:
        idempotency_key = f"{client_id}:{signal_id}"

    def _ins():
        with _conn()() as c:
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
        _run_with_retry(_ins)
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
        with _conn()() as c:
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
    _run_with_retry(_fn)


def _claim_one_job(client_id: str = "default") -> Optional[dict]:
    """
    Atomically claim one NEW job for this specific client_id.

    Uses a single CTE with FOR UPDATE SKIP LOCKED -- eliminates the 3-query
    SELECT / UPDATE / re-read race condition. Only one worker can ever claim
    a given row because SKIP LOCKED skips rows held by another transaction.

    Also reclaims stale PROCESSING jobs in the same transaction.
    """
    def _atomic_claim():
        with _conn()() as c:
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

    return _run_with_retry(_atomic_claim)


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
    position_manager=None,
    exit_eng=None,
    broker=None,
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

    # Block new intraday entries after 3:15 PM ET — too close to close for 0DTE
    from datetime import datetime
    from zoneinfo import ZoneInfo
    _now_et = datetime.now(ZoneInfo("America/New_York"))
    _too_late = _now_et.hour > 15 or (_now_et.hour == 15 and _now_et.minute >= 15)
    if _too_late and trigger_type == "breach":
        log.warning(
            "[%s] ENTRY BLOCKED — too late in session (%02d:%02d ET, cutoff 15:15) | "
            "signal held overnight for next session",
            ticker, _now_et.hour, _now_et.minute,
        )
        _mark_job(job_id, "REJECTED", error="entry_cutoff: too_late_in_session")
        return

    if trigger_type == "breach" and entry_watcher:
        try:
            entry_watcher.watch(plan=plan, local_order_id=local_order_id)
            log.info(
                f"[{ticker}] Handed to entry watcher | "
                f"trigger=${getattr(plan, 'trigger_price', '?')}"
            )
            # Transition order to SUBMITTED so order_monitor doesn't stale-cancel it.
            # The order is legitimately alive — it's waiting for breach confirmation.
            order_state_machine.transition(
                local_order_id, "SUBMITTED",
                submitted_ts=now_utc_iso(),
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

            # PAPER MODE: simulate immediate fill at limit price
            # In paper mode there is no real broker to confirm the order --
            # transition straight to FILLED so the position goes live.
            import os as _os
            _bot_mode = (_os.getenv("AP_MODE") or _os.getenv("BOT_MODE") or "PAPER").upper()
            if _bot_mode != "LIVE":
                fill_price = getattr(plan, "limit_price", None)
                if fill_price:
                    try:
                        # ── Submit real order to Tradier sandbox ──────────────
                        # This makes trades visible in the Tradier sandbox account
                        # while keeping all local tracking/exit engine intact.
                        _submitted_to_broker = False
                        if broker is not None:
                            try:
                                _resp = broker.place_order(
                                    symbol=ticker,
                                    contract=getattr(plan, "contract_symbol", ""),
                                    qty=getattr(plan, "contracts", 1),
                                    limit_price=fill_price,
                                    side="buy_to_open",
                                )
                                _submitted_to_broker = True
                                _raw_broker_id = (
                                    getattr(_resp, "broker_order_id", None)
                                    or getattr(_resp, "order_id", None)
                                    or ""
                                )
                                # Only use the ID if it's a real broker ID (not N/A or empty)
                                _broker_order_id = str(_raw_broker_id) if _raw_broker_id and str(_raw_broker_id) not in ("", "N/A", "None") else ""
                                log.info(
                                    f"[{ticker}] SANDBOX ORDER SUBMITTED | "
                                    f"broker_id={_broker_order_id or 'REJECTED'} "
                                    f"status={getattr(_resp, 'status', '?')}"
                                )
                                # Save broker_order_id directly — avoids state machine
                                # transition rules blocking a SUBMITTED→SUBMITTED no-op
                                if _broker_order_id:
                                    from ap.db import update_order as _upd_order
                                    _upd_order(local_order_id,
                                               broker_order_id=_broker_order_id)
                            except Exception as _be:
                                log.warning(f"[{ticker}] Sandbox order failed ({_be}) -- continuing with local paper fill")
                                _broker_order_id = ""

                        order_state_machine.transition(
                            local_order_id, "FILLED",
                            fill_price=fill_price,
                            filled_qty=getattr(plan, "contracts", 1),
                            filled_ts=now_utc_iso(),
                        )
                        log.info(
                            f"[{ticker}] PAPER FILL | {getattr(plan, 'contract_symbol', '?')} "
                            f"@ ${fill_price:.2f} x{getattr(plan, 'contracts', 1)}"
                        )
                        # ── Open position so exit engine can monitor it ──────
                        _pos_id = None
                        if position_manager is not None:
                            try:
                                _pos_id = position_manager.open_position(
                                    plan_id=plan.plan_id,
                                    signal_id=signal_id,
                                    ticker=ticker,
                                    contract=getattr(plan, "contract_symbol", ""),
                                    side=getattr(plan, "side", "CALL"),
                                    qty=getattr(plan, "contracts", 1),
                                    entry_price=fill_price,
                                    tier=str(getattr(plan, "tier", "B")),
                                    score=float(getattr(plan, "score", 0.0) or 0),
                                    pattern=str(getattr(plan, "pattern", "") or ""),
                                    tp_pct=float(getattr(plan, "tp_pct", 0.20) or 0.20),
                                    sl_pct=float(getattr(plan, "sl_pct", 0.25) or 0.25),
                                    stop_underlying=getattr(plan, "stop_price", None),
                                    target_underlying=getattr(plan, "target_underlying", None),
                                )
                                # Link position_id back to the order
                                order_state_machine.transition(
                                    local_order_id, "FILLED",
                                    position_id=_pos_id,
                                )
                                log.info(
                                    f"[{ticker}] POSITION CREATED | id={_pos_id}"
                                )
                                # ── Register with exit engine for stop/target/EOD monitoring ──
                                if exit_eng is not None:
                                    try:
                                        from ap_exit_engine import ManagedPosition
                                        mp = ManagedPosition(
                                            ticker=ticker,
                                            option_symbol=getattr(plan, "contract_symbol", ""),
                                            side=getattr(plan, "side", "CALL"),
                                            quantity=getattr(plan, "contracts", 1),
                                            entry_price=fill_price,
                                            underlying_entry=getattr(plan, "trigger_price", 0.0) or 0.0,
                                            # Use real levels when available.
                                            # When absent: target=inf (CALL) or 0 (PUT) so TARGET HIT never
                                            # fires on first poll. is_at_target guards 0 too, but inf is explicit.
                                            underlying_target=float(
                                                getattr(plan, "target_underlying", None)
                                                or (float("inf") if getattr(plan, "side", "CALL") == "CALL"
                                                    else 0.0)
                                            ),
                                            underlying_stop=float(
                                                getattr(plan, "stop_underlying", None) or 0.0
                                            ),
                                        )
                                        mp.current_option_price = fill_price
                                        exit_eng.add_position(mp)
                                        log.info("[%s] Registered with exit engine | %s", ticker, getattr(plan, 'contract_symbol', ''))
                                    except Exception as e:
                                        log.error("[%s] EXIT ENGINE REGISTRATION FAILED — position has NO stop loss: %s", ticker, e)
                                else:
                                    log.error("[%s] exit_eng not injected — position has NO stop loss protection", ticker)
                            except Exception as ope:
                                log.warning(f"[{ticker}] open_position failed: {ope}")
                        else:
                            log.warning(f"[{ticker}] position_manager not injected -- position not tracked")
                        _mark_job(job_id, "COMPLETED",
                                  result={"plan_id": plan.plan_id,
                                          "local_order_id": local_order_id,
                                          "contract": getattr(plan, "contract_symbol", ""),
                                          "fill_price": fill_price,
                                          "real_cost": plan.max_position_usd,
                                          "position_id": _pos_id,
                                          "trigger_type": "immediate_paper_fill"})
                        return
                    except Exception as pe:
                        log.warning(f"[{ticker}] Paper fill transition failed: {pe}")

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
    position_manager=None,
    exit_eng=None,
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
    _init_db()()
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
                    position_manager=position_manager,
                    exit_eng=exit_eng,
                    broker=broker,
                )

            # ── LEGACY FALLBACK (paper mode only) ────────────────────────────
            else:
                # ONE PATH ONLY — master_control is required in all modes.
                # Legacy process_signal() fallback is permanently disabled.
                # If master_control is missing, the runner failed to initialize.
                log.error(
                    f"[{client_id}] master_control not available — "
                    f"rejecting {signal_id}. Check runner startup logs."
                )
                _mark_job(job_id, "REJECTED",
                          error="master_control_required_not_initialized")

        except Exception as e:
            log.error(f"Worker loop error: {e}", exc_info=True)
            if job_id is not None:
                try:
                    _mark_job(job_id, "ERROR", error=str(e))
                except Exception:
                    pass
            time.sleep(poll_seconds)
