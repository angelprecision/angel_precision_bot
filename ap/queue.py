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
#
# Upgrades in this version:
#   ✅ restart_guard           -- blocks overnight signals on mid-session restart
#   ✅ 1-1 pair manager        -- cancels opposite side on fill
#   ✅ rejection feed          -- every rejection posted to Discord
# =============================================================================

from __future__ import annotations

import json
import logging
from ap.trace import trace_gate
import os
import time
from typing import Any, Optional

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
from zoneinfo import ZoneInfo
from datetime import datetime, timezone, time as dtime

log = logging.getLogger("ap.queue")

ET = ZoneInfo("America/New_York")

def _now_et() -> datetime:
    """Current datetime in US/Eastern."""
    return datetime.now(ET)

def _is_regular_session_et(dt=None) -> bool:
    """True if dt (or now) is within regular market hours Mon-Fri 9:30-16:00 ET."""
    dt = dt or _now_et()
    return dt.weekday() < 5 and dtime(9, 30) <= dt.time() < dtime(16, 0)

POLL_INTERVAL         = float(os.getenv("QUEUE_POLL_INTERVAL", "1.0"))
PROCESSING_STALE_SECS = int(os.getenv("PROCESSING_STALE_SECS", "120"))
ALLOW_IMMEDIATE_EXECUTION = os.getenv("ALLOW_IMMEDIATE_EXECUTION", "0").lower() in {"1", "true", "yes", "on"}


# =============================================================================
# HELPERS
# =============================================================================

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

    if not payload.get("ticker") and payload.get("symbol"):
        payload["ticker"] = payload["symbol"]
    if not payload.get("symbol") and payload.get("ticker"):
        payload["symbol"] = payload["ticker"]

    if payload.get("score") is None:
        payload["score"] = 65.0
    if payload.get("ev_score") is None:
        payload["ev_score"] = payload["score"]

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
            # rowcount is 0 when ON CONFLICT DO NOTHING fires (duplicate);
            # 1 when the row was actually inserted.
            return getattr(c, "rowcount", None)

    try:
        rowcount = _run_with_retry(_ins)
        if rowcount == 0:
            log.debug(f"Duplicate ignored: {signal_id}")
            return False
        log.info(f"✅ Enqueued: {signal_id} client={client_id}")
        return True
    except Exception as e:
        msg = str(e).lower()
        if "unique" in msg or "conflict" in msg:
            log.debug(f"Duplicate ignored (exception path): {signal_id}")
            return False
        raise


# =============================================================================
# JOB MANAGEMENT
# =============================================================================

_TERMINAL_QUEUE_STATUSES = {
    "REJECTED",
    "ERROR",
    "SUBMITTED",
    "FILLED",
    "CANCELED",
    "CANCELLED",
    "EXPIRED",
    "DONE",
}


def _mark_job(
    job_id: int,
    status: str,
    *,
    result: dict | None = None,
    error: str | None = None,
):
    """
    Update a queue job without falsely finishing non-terminal states.

    WATCHING is intentionally non-terminal: the entry watcher owns the trigger
    and later broker submission path. Marking finished_ts on WATCHING can make
    a live watched job look complete before any broker order fires.
    """
    terminal = str(status).upper() in _TERMINAL_QUEUE_STATUSES

    def _fn():
        with _conn()() as c:
            c.execute(
                """
                UPDATE trade_queue
                SET status=%s,
                    finished_ts=CASE WHEN %s THEN NOW() ELSE finished_ts END,
                    result_json=%s,
                    last_error=%s
                WHERE id=%s
                """,
                (
                    status,
                    terminal,
                    _json_dumps(result) if result is not None else None,
                    error,
                    job_id,
                ),
            )
    _run_with_retry(_fn)


def _claim_one_job(client_id: str = "default") -> Optional[dict]:
    """
    Atomically claim one NEW job for this specific client_id.
    Uses a single CTE with FOR UPDATE SKIP LOCKED — no race conditions.
    Also reclaims stale PROCESSING jobs in the same transaction.
    """
    def _atomic_claim():
        with _conn()() as c:
            c.execute(
                """
                UPDATE trade_queue
                SET status      = 'NEW',
                    started_ts  = NULL,
                    last_error  = COALESCE(last_error,'') || ' | reclaimed_stale'
                WHERE status    = 'PROCESSING'
                  AND client_id = %s
                  AND started_ts IS NOT NULL
                  AND started_ts < NOW() - (%s || ' seconds')::interval
                """,
                (client_id, str(PROCESSING_STALE_SECS)),
            )
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
    on_split_brain=None,
):
    """
    Unified control path:
      0. restart_guard          → block overnight signals if bot restarted mid-session
      1. master_control.evaluate()      → gate with placeholder estimate
      2. contract_selector.select()     → real premium, updates plan in-place
      3. master_control.revalidate()    → re-check capital/sector/ticker with REAL premium
      4. order_state_machine.create_entry_order()
      5. breach → entry_watcher  |  immediate → SUBMITTED transition
    """
    ticker = payload.get("ticker") or payload.get("symbol", "?")

    # ── 0. RESTART GUARD ─────────────────────────────────────────────────────
    # Blocks overnight (previous-day) signals during market hours.
    # Prevents 47-signal mass re-fire when bot restarts mid-session.
    # Pre-market restarts are allowed through for morning revalidation.
    try:
        from ap.restart_guard import should_skip_on_restart
        if should_skip_on_restart(payload):
            log.warning("[%s] RESTART GUARD — overnight signal blocked", ticker)
            _mark_job(job_id, "REJECTED", error="restart_guard:overnight_skip")
            try:
                from ap.rejection_feed import post_master_control_block
                post_master_control_block(
                    ticker=ticker,
                    side=payload.get("side", ""),
                    stage="restart_guard",
                    reason="restart_guard:overnight_skip",
                    score=float(payload.get("score") or 0),
                    pattern=payload.get("pattern_id") or payload.get("pattern", ""),
                )
            except Exception:
                pass
            return
    except ImportError:
        pass  # restart_guard not yet deployed — skip silently

    # ── 1. MASTER CONTROL (initial gate, placeholder estimate) ────────────────
    try:
        decision = master_control.evaluate(payload, client_id=client_id)
    except Exception as e:
        log.error(f"[{ticker}] master_control.evaluate() failed: {e}")
        _mark_job(job_id, "ERROR", error=f"master_control_error: {e}")
        return

    if not decision.ok:
        log.info(f"[{ticker}] BLOCKED | stage={decision.stage} reason={decision.reason}")
        trace_gate(str(payload.get("signal_id","")), ticker, "MC_REJECTED", "REJECT",
                   reason=decision.reason, score=float(payload.get("score") or 0))
        _mark_job(job_id, "REJECTED",
                  result={"stage": decision.stage, "reason": decision.reason})

        # Post rejection to Discord
        try:
            from ap.rejection_feed import post_master_control_block
            post_master_control_block(
                ticker=ticker,
                side=payload.get("side", ""),
                stage=decision.stage,
                reason=decision.reason,
                score=float(payload.get("score") or 0),
                pattern=payload.get("pattern_id") or payload.get("pattern", ""),
            )
        except Exception:
            pass

        # Write to ap_signals as watching for deferrable post-market blocks only
        _block_reason = str(decision.reason or "")
        _is_deferrable = any(k in _block_reason for k in ("daily_stop", "sizer_blocked", "after_hours"))
        try:
            _now_et_def = _now_et()
            _in_session = _is_regular_session_et(_now_et_def)
            if not _in_session and _is_deferrable:
                import uuid as _uuid
                import os as _os
                from supabase import create_client as _create_client
                _sb_url = _os.getenv("SUPABASE_URL", "")
                _sb_key = _os.getenv("SUPABASE_SERVICE_KEY", "")
                _sbc = _create_client(_sb_url, _sb_key) if _sb_url and _sb_key else None
                if _sbc:
                    _sig_id = str(payload.get("signal_id") or _uuid.uuid4())
                    _sbc.table("ap_signals").upsert({
                        "signal_id":       _sig_id,
                        "client_email":    client_id,
                        "system_version":  "v2",
                        "ticker":          ticker,
                        "side":            str(payload.get("side") or payload.get("direction") or "CALL").upper(),
                        "score":           float(payload.get("score") or payload.get("ev_score") or 0),
                        "tier":            str(payload.get("tier") or "B"),
                        "pattern":         str(payload.get("pattern") or ""),
                        "timeframe":       str(payload.get("timeframe") or "1d"),
                        "entry_trigger":   float(payload.get("entry_price") or payload.get("trigger_price") or 0) or None,
                        "stop_price":      float(payload.get("stop_price") or 0) or None,
                        "target_price":    float(payload.get("target_price") or 0) or None,
                        "decision_status": "watching",
                        "context_notes":   f"post_market_blocked: {decision.reason}",
                        "raw_payload":     payload,
                    }, on_conflict="signal_id").execute()
                    log.info(f"[{ticker}] Written to ap_signals as watching (post-market queue)")
        except Exception as _e:
            log.debug(f"[{ticker}] ap_signals write skipped: {_e}")
        return

    plan = decision.plan

    # ── 1-1 PAIR MANAGER — register signal so opposite side cancels on fill ──
    try:
        from ap.signal_pair_manager import get_pair_manager
        _pm = get_pair_manager()
        _pair_key = _pm.register(
            ticker=ticker,
            side=payload.get("side", ""),
            local_order_id=signal_id,
            pattern_id=payload.get("pattern_id") or payload.get("pattern", ""),
        )
        if _pair_key:
            log.info("[%s] Registered as 1-1 pair: %s", ticker, _pair_key)
    except Exception:
        pass

    # ── 2. CONTRACT SELECTION -- skip outside market hours ──────────────────────
    _skip_contract_selection = False
    try:
        _now_et2 = _now_et()
        _in_mkt  = _is_regular_session_et(_now_et2)
        _bypass_mkt = bool(payload.get("bypass_market_hours") or payload.get("test_mode"))
        if not _in_mkt and contract_selector and not _bypass_mkt:
            _skip_contract_selection = True
            log.info(
                f"[{ticker}] Post-market signal — skipping contract selection "
                f"(stale quotes). Will select at breach time with live quotes."
            )
            # Explicitly mark this plan as watcher/breach-time contract selection.
            # Prevents downstream confusion: no broker order should be submitted
            # until live quotes are available at breach.
            try:
                plan.contract_symbol = None
                setattr(plan, "_needs_contract_selection", True)
                if hasattr(plan, "metadata") and isinstance(plan.metadata, dict):
                    plan.metadata["needs_contract_selection"] = True
                    plan.metadata["contract_selection_deferred"] = "outside_regular_session"
            except Exception:
                pass
            # Reject immediately — market is closed, live quotes unavailable for
            # contract selection. Accept only when breach-time re-selection is
            # wired end-to-end; until then dropping is safer than a stale contract.
            log.warning(
                "[%s] Signal %s rejected — market closed, no live quotes for contract selection",
                ticker, signal_id,
            )
            _mark_job(job_id, "REJECTED", error="market_closed_no_contract_selection")
            return
    except Exception:
        pass

    if contract_selector and not _skip_contract_selection:
        try:
            selected = contract_selector.select(plan)
            if selected is None:
                log.warning(f"[{ticker}] Contract selection failed -- no suitable contract")
                trace_gate(str(payload.get("signal_id","")), ticker, "QUALITY_FILTER", "REJECT",
                           reason="no_eligible_contracts", score=float(payload.get("score") or 0))
                _mark_job(job_id, "REJECTED",
                          result={"stage": "contract_selection",
                                  "reason": "no_contract_found",
                                  "ticker": ticker})
                try:
                    from ap.rejection_feed import post_no_contracts
                    post_no_contracts(
                        ticker=ticker,
                        side=payload.get("side", ""),
                        chain_size=0,
                        top_rejections={},
                        score=float(payload.get("score") or 0),
                        pattern=payload.get("pattern_id") or payload.get("pattern", ""),
                    )
                except Exception:
                    pass
                return
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
    # Overnight signals always go breach-only — never immediate execution.
    # Use ET date to avoid UTC/ET day-boundary misclassification.
    _signal_date  = (payload.get("created_at") or payload.get("timestamp_iso") or "")[:10]
    _today_str    = _now_et().date().isoformat()
    forced_breach = False

    if _signal_date and _signal_date < _today_str:
        forced_breach = True
        log.warning("[%s] Overnight signal (created %s) — forcing breach-only path",
                    ticker, _signal_date)
        try:
            plan.trigger_type = "breach"
        except Exception:
            pass
        if not entry_watcher:
            log.error("[%s] Dropping overnight signal — no entry watcher", ticker)
            _mark_job(job_id, "REJECTED", error="overnight_signal_no_watcher")
            return

    # Breach-first policy:
    #   - forced_breach is sticky
    #   - immediate execution is disabled by default
    #   - set ALLOW_IMMEDIATE_EXECUTION=1 only for controlled internal testing
    raw_trigger_type = str(getattr(plan, "trigger_type", "breach") or "breach").lower()
    if forced_breach:
        trigger_type = "breach"
    elif raw_trigger_type == "immediate" and ALLOW_IMMEDIATE_EXECUTION:
        trigger_type = "immediate"
    else:
        if raw_trigger_type == "immediate" and not ALLOW_IMMEDIATE_EXECUTION:
            log.warning(
                "[%s] Immediate execution requested but disabled — forcing breach/watch path",
                ticker,
            )
        trigger_type = "breach"

    if trigger_type == "breach" and not entry_watcher:
        log.critical("[%s] ENTRY WATCHER MISSING — cannot arm breach entry", ticker)
        _mark_job(job_id, "REJECTED", error="entry_watcher_missing")
        try:
            from ap.rejection_feed import post_master_control_block
            post_master_control_block(
                ticker=ticker,
                side=payload.get("side", ""),
                stage="entry_watcher",
                reason="entry_watcher_missing",
                score=float(payload.get("score") or 0),
                pattern=payload.get("pattern_id") or payload.get("pattern", ""),
            )
        except Exception:
            pass
        return

    # Block entries after 3:15 PM ET during market hours (weekdays only)
    _now_et_cut  = _now_et()
    _in_session  = _is_regular_session_et(_now_et_cut)
    _too_late    = _in_session and (
        _now_et_cut.hour > 15 or (_now_et_cut.hour == 15 and _now_et_cut.minute >= 15)
    )
    if _too_late and trigger_type == "breach":
        log.warning(
            "[%s] ENTRY BLOCKED — too late in session (%02d:%02d ET, cutoff 15:15)",
            ticker, _now_et_cut.hour, _now_et_cut.minute,
        )
        _mark_job(job_id, "REJECTED", error="entry_cutoff: too_late_in_session")
        return

    if trigger_type == "breach":
        try:
            # Move the local ENTRY order into explicit watcher-owned state before
            # arming. This proves the queue/OSM contract early and prevents a
            # fake WATCHING job when the OSM cannot represent PENDING_TRIGGER.
            try:
                if hasattr(order_state_machine, "mark_entry_pending_trigger"):
                    pending_ok = bool(order_state_machine.mark_entry_pending_trigger(local_order_id))
                else:
                    pending_ok = bool(order_state_machine.transition(
                        local_order_id, "PENDING_TRIGGER", submitted_ts=None,
                    ))
            except Exception as _st:
                log.error(
                    "[%s] PENDING_TRIGGER transition failed before watcher arm: %s",
                    ticker, _st, exc_info=True,
                )
                pending_ok = False

            if not pending_ok:
                log.critical(
                    "[%s] WATCH_ARM_ABORTED — could not mark order PENDING_TRIGGER | local=%s",
                    ticker, local_order_id,
                )
                _mark_job(job_id, "ERROR", error="pending_trigger_transition_failed")
                return

            # Register with watcher — no broker submit yet. The watcher returns
            # False for stale signals, dedup blocks, opposite-side conflicts,
            # and failed OSM local-order validation. Do not mark WATCHING unless
            # this returns True.
            armed = bool(entry_watcher.watch(plan=plan, local_order_id=local_order_id))
            if not armed:
                log.warning(
                    "[%s] WATCH_ARM_FAILED — watcher refused signal | local=%s",
                    ticker, local_order_id,
                )
                try:
                    if hasattr(order_state_machine, "expire_pending_entry"):
                        order_state_machine.expire_pending_entry(local_order_id, reason="watch_arm_failed")
                    elif hasattr(order_state_machine, "cancel_pending_entry"):
                        order_state_machine.cancel_pending_entry(local_order_id, reason="watch_arm_failed")
                    else:
                        order_state_machine.transition(local_order_id, "EXPIRED", last_error="watch_arm_failed")
                except Exception as _cleanup_exc:
                    log.error(
                        "[%s] Failed to cleanup unarmed pending entry %s: %s",
                        ticker, local_order_id, _cleanup_exc, exc_info=True,
                    )
                _mark_job(job_id, "REJECTED", error="watch_arm_failed")
                return

            log.info(
                f"[{ticker}] Handed to entry watcher | "
                f"trigger=${getattr(plan, 'trigger_price', '?')}"
            )

            # Queue job = WATCHING only after watcher arm succeeded.
            _mark_job(job_id, "WATCHING",
                      result={"plan_id": plan.plan_id,
                              "local_order_id": local_order_id,
                              "contract": getattr(plan, "contract_symbol", ""),
                              "real_cost": plan.max_position_usd,
                              "trigger_type": "breach",
                              "needs_contract_selection": bool(getattr(plan, "_needs_contract_selection", False))})
        except Exception as e:
            log.error(f"[{ticker}] entry_watcher.watch() failed: {e}", exc_info=True)
            try:
                if hasattr(order_state_machine, "expire_pending_entry"):
                    order_state_machine.expire_pending_entry(local_order_id, reason=f"watcher_error:{e}")
                elif hasattr(order_state_machine, "cancel_pending_entry"):
                    order_state_machine.cancel_pending_entry(local_order_id, reason=f"watcher_error:{e}")
                else:
                    order_state_machine.transition(local_order_id, "ERROR", last_error=f"watcher_error:{e}")
            except Exception:
                pass
            _mark_job(job_id, "ERROR", error=f"watcher_error: {e}")
    else:
        log.warning(
            "[%s] IMMEDIATE EXECUTION PATH ENABLED — this should only run when ALLOW_IMMEDIATE_EXECUTION=1",
            ticker,
        )

        # Money-safe immediate path:
        # Never mark SUBMITTED before broker acceptance. We submit the existing
        # CREATED order through OSM, and OSM transitions to SUBMITTED only after
        # broker accepts and returns broker_order_id.
        try:
            if not hasattr(order_state_machine, "submit_existing_entry"):
                raise RuntimeError("order_state_machine_missing_submit_existing_entry")

            submit_res = order_state_machine.submit_existing_entry(
                local_order_id=local_order_id,
                broker=broker,
                plan=plan,
                limit_price=getattr(plan, "limit_price", None),
            )

            # WIRE-3: split-brain — broker accepted but OSM DB transition failed
            if submit_res.get("split_brain") and callable(on_split_brain):
                try:
                    on_split_brain(
                        local_order_id=submit_res.get("local_order_id", ""),
                        broker_order_id=submit_res.get("broker_order_id", ""),
                    )
                except Exception as _sb_err:
                    log.error("[%s] split_brain callback failed: %s", ticker, _sb_err)

            if not submit_res.get("ok"):
                log.error(
                    "[%s] Immediate submit failed safely | local=%s error=%s",
                    ticker, local_order_id, submit_res.get("error"),
                )
                _mark_job(job_id, "ERROR", error=f"submit_error:{submit_res.get('error')}")
                return

            log.info(
                "[%s] Order submitted immediately after broker acceptance | local=%s broker=%s",
                ticker, local_order_id, submit_res.get("broker_order_id"),
            )

            _mark_job(job_id, "SUBMITTED",
                      result={"plan_id": plan.plan_id,
                              "local_order_id": local_order_id,
                              "broker_order_id": submit_res.get("broker_order_id"),
                              "contract": getattr(plan, "contract_symbol", ""),
                              "real_cost": plan.max_position_usd,
                              "trigger_type": "immediate"})
            return
        except Exception as e:
            log.error(f"[{ticker}] immediate submit failed: {e}", exc_info=True)
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
    stop_event=None,
    live_mode: bool = False,
    on_split_brain=None,
):
    """
    Main queue worker.
    stop_event: threading.Event — set by ClientRunner.stop() to cleanly exit.
    live_mode:  if True, legacy process_signal() fallback is disabled entirely.
    on_split_brain: optional callback(local_order_id, broker_order_id) fired when
                    a submit returns split_brain=True (broker accepted, OSM DB failed).
    """
    _init_db()()
    mode_label = "control" if master_control else ("LIVE-NO-FALLBACK" if live_mode else "legacy")
    log.info(
        f"🤖 Worker started | client={client_id} poll={poll_seconds}s path={mode_label}"
    )

    # Log restart guard status on startup
    try:
        from ap.restart_guard import log_startup
        log_startup()
    except Exception:
        pass

    while True:
        if stop_event and stop_event.is_set():
            log.info(f"[{client_id}] Worker stop_event set -- exiting cleanly")
            return

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
                    on_split_brain=on_split_brain,
                )
            else:
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
