"""
ap_overnight_reeval.py — Overnight Daily Signal Re-Evaluation Engine
=====================================================================
This is the missing piece of the full trading loop.

FLOW:
  1. Market close → scanner runs → signals arrive with timeframe=1d
  2. Bot marks them WATCHING (audit record, not yet armed)
  3. *** THIS MODULE *** runs at 9:15 AM ET (15 min before open)
  4. For each WATCHING signal:
     a. Fetch prior-day high/low from Tradier history
     b. Run overnight_daily_validator (directional invalidation check)
     c. If VALID: select contract, create OSM entry order, arm entry_watcher
     d. If INVALID: mark REJECTED with reason code, log to ap_signals
  5. At 9:30 AM ET open: entry_watcher polls quotes, waits for breach
  6. On breach: on_trigger fires → OSM submits entry → fill monitor takes over

WHEN IT RUNS:
  - Called by client_runner's health loop at ~9:15 AM ET on trading days
  - Also callable via /admin/overnight_reeval for manual trigger
  - ONLY processes signals with date == yesterday (no stale signals)

ENTRY TRIGGER LOGIC:
  - If scanner provides entry_trigger: use it directly
  - If not: use prior_day_high (CALL) or prior_day_low (PUT) as the breach level
    This matches The Strat: we enter on prior-day boundary breach

FAIL-CLOSED:
  - Missing prior levels → REJECTED
  - Broker unavailable → skip (will retry on next poll)
  - No contract found → REJECTED with reason
"""
from __future__ import annotations

import logging
import os
import time
from datetime import date, datetime, timezone, timedelta
from typing import TYPE_CHECKING, Optional

log = logging.getLogger("ap.overnight_reeval")

if TYPE_CHECKING:
    pass

# How many calendar days back a signal is still considered "fresh"
# e.g. a Friday signal is valid Monday morning = 3 days
OVERNIGHT_SIGNAL_MAX_AGE_DAYS = int(os.getenv("OVERNIGHT_SIGNAL_MAX_AGE_DAYS", "4"))


def _et_now() -> datetime:
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("America/New_York"))


def _is_trading_day(dt: datetime) -> bool:
    return dt.weekday() < 5  # Mon-Fri


def _signal_date(signal: dict) -> Optional[date]:
    """Extract the date the signal was generated (not when we process it)."""
    for key in ("created_at", "signal_date", "date", "timestamp_iso"):
        val = signal.get(key, "")
        if val and len(str(val)) >= 10:
            try:
                return date.fromisoformat(str(val)[:10])
            except ValueError:
                continue
    return None


def run_overnight_reeval(
    *,
    client_id: str,
    broker,
    master_control,
    contract_selector,
    order_state_machine,
    entry_watcher,
    position_manager=None,
    exit_eng=None,
    on_split_brain=None,
    force: bool = False,
) -> dict:
    """
    Re-evaluate all WATCHING signals for client_id.
    Called at ~9:15 AM ET before market open.

    Returns summary dict: {processed, armed, rejected, skipped, errors}
    """
    from ap.overnight_daily_validator import (
        validate_overnight_daily_signal,
        fetch_market_snapshot,
        InvalidationReason,
    )

    result = {"processed": 0, "armed": 0, "rejected": 0, "skipped": 0, "errors": 0}

    # Guard: only run on trading days, 9:00-9:45 AM ET (unless force=True)
    now_et = _et_now()
    if not force:
        if not _is_trading_day(now_et):
            log.info("[%s] overnight_reeval: skipping — not a trading day", client_id)
            result["skipped"] = -1
            return result
        if not (9 <= now_et.hour < 10 or (now_et.hour == 9 and now_et.minute <= 45)):
            # Also allow 9:00-9:45
            if not (now_et.hour == 9 and 0 <= now_et.minute <= 45):
                log.info("[%s] overnight_reeval: skipping — outside 9:00-9:45 AM ET window (now=%02d:%02d)",
                         client_id, now_et.hour, now_et.minute)
                result["skipped"] = -1
                return result

    # Fetch WATCHING signals from trade_queue
    watching_signals = _fetch_watching_signals(client_id)
    if not watching_signals:
        log.info("[%s] overnight_reeval: no WATCHING signals found", client_id)
        return result

    log.info("[%s] overnight_reeval: found %d WATCHING signals to reeval", client_id, len(watching_signals))

    today = now_et.date()
    for job in watching_signals:
        result["processed"] += 1
        job_id = job["id"]
        signal = job["payload"]
        if isinstance(signal, str):
            import json; signal = json.loads(signal)
        signal_id = job.get("signal_id") or signal.get("signal_id", "")
        ticker = signal.get("ticker") or signal.get("symbol", "?")
        side = (signal.get("side") or "").upper()

        try:
            # Age check: skip stale signals
            sig_date = _signal_date(signal)
            if sig_date:
                age_days = (today - sig_date).days
                if age_days > OVERNIGHT_SIGNAL_MAX_AGE_DAYS:
                    log.info("[%s] overnight_reeval: skipping stale signal %s (age=%dd)", ticker, signal_id, age_days)
                    _mark_job_rejected(job_id, client_id, f"stale_signal:age={age_days}d")
                    result["rejected"] += 1
                    continue

            # Step 1: Fetch prior-day levels from broker
            prior_levels = {}
            if hasattr(broker, "get_prior_day_levels"):
                prior_levels = broker.get_prior_day_levels(ticker) or {}
            else:
                log.warning("[%s] broker has no get_prior_day_levels — cannot validate overnight daily signal", ticker)

            prior_day_high = (
                float(signal.get("prior_day_high") or 0) or
                float(prior_levels.get("prior_day_high") or 0) or
                None
            )
            prior_day_low = (
                float(signal.get("prior_day_low") or 0) or
                float(prior_levels.get("prior_day_low") or 0) or
                None
            )

            # Enrich signal with fetched levels for watcher and validator
            signal["prior_day_high"] = prior_day_high
            signal["prior_day_low"] = prior_day_low
            if prior_levels.get("prior_day_close"):
                signal["prior_day_close"] = prior_levels["prior_day_close"]

            # Step 2: Derive entry_trigger if not provided by scanner
            # The Strat: CALL entries breach prior-day high; PUT entries breach prior-day low
            entry_trigger = (
                float(signal.get("entry_trigger") or 0) or
                (prior_day_high if side == "CALL" else prior_day_low)
            )
            if entry_trigger:
                signal["entry_trigger"] = entry_trigger

            # Step 3: Overnight daily structure validation
            # Fetch a fresh market snapshot (pre-market quote)
            snapshot = fetch_market_snapshot(ticker, broker)
            validation = validate_overnight_daily_signal(
                ticker=ticker,
                side=side,
                prior_day_high=prior_day_high,
                prior_day_low=prior_day_low,
                snapshot=snapshot,
            )

            if not validation.valid:
                log.info("[%s] overnight_reeval: INVALIDATED %s — %s", ticker, signal_id, validation.reason_code)
                _mark_job_rejected(job_id, client_id, f"overnight_invalidated:{validation.reason_code}")
                _log_rejection_supabase(
                    signal_id=signal_id, client_id=client_id, ticker=ticker,
                    side=side, score=float(signal.get("score") or 0),
                    stage="overnight_reeval",
                    reason_code=validation.reason_code,
                    human_reason=validation.reason_text,
                    payload=signal,
                )
                result["rejected"] += 1
                continue

            log.info("[%s] overnight_reeval: VALID %s — arming entry watcher | entry_trigger=%.4f",
                     ticker, signal_id, entry_trigger or 0)

            # Step 4: Master control pre-check (score gates, capital, etc.)
            try:
                decision = master_control.evaluate(signal, client_id=client_id)
                if not decision.ok:
                    log.info("[%s] overnight_reeval: MC blocked %s — %s", ticker, signal_id, decision.reason)
                    _mark_job_rejected(job_id, client_id, f"mc_blocked:{decision.reason}")
                    result["rejected"] += 1
                    continue
            except Exception as mc_exc:
                log.error("[%s] overnight_reeval: master_control.evaluate failed: %s", ticker, mc_exc)
                result["errors"] += 1
                continue

            # Step 5: Contract selection
            try:
                plan = contract_selector.select(signal, decision)
            except Exception as cs_exc:
                log.error("[%s] overnight_reeval: contract_selector.select failed: %s", ticker, cs_exc)
                _mark_job_rejected(job_id, client_id, f"contract_selection_failed:{cs_exc}")
                result["rejected"] += 1
                continue

            if not plan or not getattr(plan, "contract", None):
                log.warning("[%s] overnight_reeval: no contract found for %s", ticker, signal_id)
                _mark_job_rejected(job_id, client_id, "no_contract_selected")
                result["rejected"] += 1
                continue

            # Ensure entry_trigger propagates to the plan
            if entry_trigger and not getattr(plan, "entry_trigger", None):
                try:
                    plan.entry_trigger = entry_trigger
                    plan.trigger_type = "breach"
                    plan.overnight = True
                except Exception:
                    pass

            # Step 6: Create OSM entry order
            try:
                local_order_id = f"{signal_id}:{client_id}"
                osm_result = order_state_machine.create_entry_order(
                    signal=signal,
                    plan=plan,
                    client_id=client_id,
                    local_order_id=local_order_id,
                )
            except Exception as osm_exc:
                log.error("[%s] overnight_reeval: OSM create_entry_order failed: %s", ticker, osm_exc)
                result["errors"] += 1
                continue

            # Step 7: Arm entry watcher
            try:
                armed = entry_watcher.watch(plan, local_order_id)
                if armed:
                    _mark_job_watching_armed(job_id, client_id, plan.contract)
                    log.info("[%s] ✅ ARMED for tomorrow — contract=%s entry_trigger=%.4f",
                             ticker, plan.contract, entry_trigger or 0)
                    result["armed"] += 1
                else:
                    log.error("[%s] overnight_reeval: entry_watcher.watch() returned False", ticker)
                    result["errors"] += 1
            except Exception as ew_exc:
                log.error("[%s] overnight_reeval: entry_watcher.watch failed: %s", ticker, ew_exc)
                result["errors"] += 1

        except Exception as outer_exc:
            log.error("[%s] overnight_reeval: unexpected error for %s: %s", ticker, signal_id, outer_exc, exc_info=True)
            result["errors"] += 1

    log.info(
        "[%s] overnight_reeval complete: processed=%d armed=%d rejected=%d skipped=%d errors=%d",
        client_id, result["processed"], result["armed"], result["rejected"],
        result["skipped"], result["errors"],
    )
    return result


def _fetch_watching_signals(client_id: str) -> list:
    """Fetch WATCHING jobs from local trade_queue for this client."""
    try:
        from ap.db import run_with_retry
        def _q():
            from ap.db import conn as _conn
            with _conn() as c:
                c.execute("""
                    SELECT id, signal_id, payload, created_ts
                    FROM trade_queue
                    WHERE client_id = %s
                      AND status = 'WATCHING'
                    ORDER BY created_ts DESC
                    LIMIT 100
                """, (client_id,))
                cols = [d[0] for d in c.description]
                return [dict(zip(cols, row)) for row in c.fetchall()]
        return run_with_retry(_q)
    except Exception as e:
        log.error("_fetch_watching_signals failed: %s", e)
        return []


def _mark_job_rejected(job_id: int, client_id: str, reason: str) -> None:
    try:
        from ap.db import run_with_retry
        from datetime import datetime, timezone
        def _u():
            from ap.db import conn as _conn
            with _conn() as c:
                c.execute("""
                    UPDATE trade_queue
                    SET status = 'REJECTED',
                        last_error = %s,
                        finished_ts = NOW()
                    WHERE id = %s AND client_id = %s
                """, (reason, job_id, client_id))
        run_with_retry(_u)
    except Exception as e:
        log.debug("_mark_job_rejected failed (non-fatal): %s", e)


def _mark_job_watching_armed(job_id: int, client_id: str, contract: str) -> None:
    """Update the WATCHING job to record that it has been armed in the watcher."""
    try:
        from ap.db import run_with_retry
        def _u():
            from ap.db import conn as _conn
            with _conn() as c:
                c.execute("""
                    UPDATE trade_queue
                    SET last_error = %s,
                        started_ts = COALESCE(started_ts, NOW())
                    WHERE id = %s AND client_id = %s
                """, (f"armed:contract={contract}", job_id, client_id))
        run_with_retry(_u)
    except Exception as e:
        log.debug("_mark_job_watching_armed failed (non-fatal): %s", e)


def _log_rejection_supabase(
    signal_id: str, client_id: str, ticker: str, side: str,
    score: float, stage: str, reason_code: str, human_reason: str, payload: dict,
) -> None:
    try:
        import os as _os
        from supabase import create_client as _cc
        sb_url = _os.getenv("SUPABASE_URL", "")
        sb_key = _os.getenv("SUPABASE_SERVICE_KEY", "")
        if not sb_url or not sb_key:
            return
        sbc = _cc(sb_url, sb_key)
        sbc.table("ap_signals").upsert({
            "signal_id":       str(signal_id),
            "client_email":    str(client_id),
            "system_version":  "v2",
            "ticker":          str(ticker),
            "side":            str(side).upper(),
            "score":           float(score or 0),
            "tier":            str(payload.get("tier") or "B"),
            "pattern":         str(payload.get("pattern") or ""),
            "timeframe":       str(payload.get("timeframe") or "1d"),
            "decision_status": "rejected",
            "context_notes":   f"stage={stage} | {reason_code} | {human_reason}",
            "raw_payload": {
                "stage": stage,
                "reason_code": reason_code,
                "human_reason": human_reason,
                **{k: v for k, v in payload.items()
                   if k not in ("raw_payload",) and not callable(v)},
            },
        }, on_conflict="signal_id").execute()
    except Exception as e:
        log.debug("_log_rejection_supabase failed: %s", e)
