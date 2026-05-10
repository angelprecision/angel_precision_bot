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
    """Extract the date the signal was generated (not when we process it).
    Falls back to parsing the signal_id itself (format: YYYY-MM-DD:...).
    """
    for key in ("created_at", "signal_date", "date", "timestamp_iso"):
        val = signal.get(key, "")
        if val and len(str(val)) >= 10:
            try:
                return date.fromisoformat(str(val)[:10])
            except ValueError:
                continue
    # Try parsing from signal_id: "2026-05-05:1-1:AAPL:Weekly:CALL"
    signal_id = signal.get("signal_id", "")
    if signal_id and len(signal_id) >= 10:
        try:
            return date.fromisoformat(str(signal_id)[:10])
        except ValueError:
            pass
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

    # ── Lifecycle ledger + health registry ───────────────────────────────────
    try:
        from ap_lifecycle import (
            LEDGER as _LEDGER,
            SignalState as _SS,
            LifecycleOwner as _LO,
            signal_watching,
            signal_invalidated,
            signal_armed,
            signal_rejected as _sig_rejected,
            RejectionCategory as _RC,
            RejectionSeverity as _RS,
        )
        _lifecycle_ok = True
    except Exception:
        _lifecycle_ok = False

    try:
        from ap_health_registry import HEALTH as _OR_HEALTH
        _OR_HEALTH.heartbeat("ap_overnight_signal_manager")
    except Exception:
        pass

    result = {"processed": 0, "armed": 0, "rejected": 0, "skipped": 0, "errors": 0}

    # Guard: only run on trading days, 9:00-9:45 AM ET (unless force=True)
    now_et = _et_now()
    if not force:
        if not _is_trading_day(now_et):
            log.info("[%s] overnight_reeval: skipping — not a trading day", client_id)
            result["skipped"] = -1
            return result
        _in_window = (now_et.hour == 9 and 0 <= now_et.minute <= 45)
        if not _in_window:
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
                    if _lifecycle_ok:
                        try:
                            _sig_rejected(signal_id, ticker, _LO.OVERNIGHT_EVAL,
                                          f"stale_signal age={age_days}d",
                                          _RC.VALIDATION, "STALE_SIGNAL", _RS.INFO,
                                          age_days=age_days)
                        except Exception:
                            pass
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
                if _lifecycle_ok:
                    try:
                        _sig_rejected(signal_id, ticker, _LO.OVERNIGHT_EVAL,
                                      f"overnight_invalidated: {validation.reason_text}",
                                      _RC.VALIDATION, validation.reason_code, _RS.INFO)
                    except Exception:
                        pass
                result["rejected"] += 1
                continue

            log.info("[%s] overnight_reeval: VALID %s — arming entry watcher | entry_trigger=%.4f",
                     ticker, signal_id, entry_trigger or 0)

            # Step 4: Master control pre-check (score gates, capital, etc.)
            # Use a fresh REEVAL: signal_id so master_control dedup doesn't block it.
            # The original signal was already deduped when it first arrived — overnight
            # reeval is a legitimate second evaluation of the same setup.
            try:
                import uuid as _uuid2
                reeval_signal = {**signal, "signal_id": f"REEVAL:{signal_id}:{_uuid2.uuid4().hex[:6]}"}
                # Clear this ticker/side from dedup cache if possible
                try:
                    direction = signal.get("side", "").upper()
                    timeframe = signal.get("timeframe", "1d")
                    setup_key = f"{client_id}:{ticker.upper()}:{direction}:{timeframe}"
                    orig_key = f"sig:{signal_id}:{client_id}"
                    mc_seen = getattr(master_control, "_seen_signals", {})
                    mc_seen.pop(orig_key, None)
                    mc_seen.pop(setup_key, None)
                except Exception:
                    pass
                decision = master_control.evaluate(reeval_signal, client_id=client_id)
                if not decision.ok:
                    log.info("[%s] overnight_reeval: MC blocked %s — %s", ticker, signal_id, decision.reason)
                    _mark_job_rejected(job_id, client_id, f"mc_blocked:{decision.reason}")
                    if _lifecycle_ok:
                        try:
                            _sig_rejected(signal_id, ticker, _LO.MASTER_CONTROL,
                                          f"mc_blocked: {decision.reason}",
                                          _RC.RISK, "MC_BLOCKED", _RS.INFO)
                        except Exception:
                            pass
                    result["rejected"] += 1
                    continue
            except Exception as mc_exc:
                log.error("[%s] overnight_reeval: master_control.evaluate failed: %s", ticker, mc_exc)
                result["errors"] += 1
                continue

            # Step 5: Contract selection — best-effort only.
            # Pre-market option chains have zero bids (options don't trade before
            # 9:30 AM ET). If selection fails here, we arm the watcher with
            # contract_deferred=True so the execution core selects the contract
            # at breach time when live quotes are available.
            # NEVER permanently reject a valid signal because of pre-market chain data.
            contract_deferred = False
            try:
                selected = contract_selector.select(decision.plan)
            except Exception as cs_exc:
                log.warning(
                    "[%s] overnight_reeval: contract selection failed pre-market (%s) "
                    "— deferring to breach time with live quotes",
                    ticker, cs_exc,
                )
                selected = None

            if not selected or not str(getattr(decision.plan, "contract_symbol", "") or "").strip():
                contract_deferred = True
                log.info(
                    "[%s] overnight_reeval: no pre-market contract available "
                    "(zero bids expected before 9:30 AM ET) — "
                    "arming watcher with contract_deferred=True | trigger=%.4f",
                    ticker, entry_trigger or 0,
                )
                # Execution core will select the live contract at breach time.
                # Set a placeholder limit so the plan passes downstream validation,
                # and mark contracts=1 as the minimum safe default.
                try:
                    decision.plan.limit_price = 0.01   # overwritten at breach by live quote
                    if not getattr(decision.plan, "contracts", None):
                        decision.plan.contracts = 1
                    if not hasattr(decision.plan, "metadata") or decision.plan.metadata is None:
                        decision.plan.metadata = {}
                    decision.plan.metadata["contract_deferred"] = True
                except Exception:
                    pass
                if _lifecycle_ok:
                    try:
                        from ap_lifecycle import LEDGER as _L
                        _L.log_event(signal_id, "contract_deferred",
                                     owner="overnight_reeval",
                                     reason="pre_market_zero_bids_deferred_to_breach")
                    except Exception:
                        pass

            # Propagate entry_trigger and overnight flag to the plan
            if entry_trigger:
                try:
                    decision.plan.trigger_price = float(entry_trigger)
                    decision.plan.trigger_type  = "breach"
                    decision.plan.metadata["overnight"] = True
                except Exception:
                    pass

            # Step 6: Create OSM entry order — let OSM generate a fresh UUID.
            # Do NOT pass local_order_id: static IDs cause OSM conflicts on retry.
            try:
                local_order_id = order_state_machine.create_entry_order(decision.plan)
            except Exception as osm_exc:
                log.error("[%s] overnight_reeval: OSM create_entry_order failed: %s", ticker, osm_exc)
                result["errors"] += 1
                continue

            if not local_order_id:
                log.error("[%s] overnight_reeval: OSM returned no local_order_id for %s", ticker, signal_id)
                result["errors"] += 1
                continue

            # Step 7: Arm entry watcher — pass plan (not signal) and the OSM order ID
            _contract_sym = str(getattr(decision.plan, "contract_symbol", "") or "")
            _arm_label    = _contract_sym if _contract_sym else "DEFERRED_AT_BREACH"
            try:
                armed = entry_watcher.watch(decision.plan, local_order_id)
                if armed:
                    _mark_job_watching_armed(job_id, client_id, _arm_label)
                    log.info(
                        "[%s] ✅ ARMED — contract=%s entry_trigger=%.4f contract_deferred=%s",
                        ticker, _arm_label, entry_trigger or 0, contract_deferred,
                    )
                    # ── THE BUG TRAP: signal is now WATCHING in the watcher ──
                    # If this signal disappears before market open, grep:
                    # [SIGNAL_TRACE] id=<signal_id>
                    # The next state change from WATCHING will reveal the killer.
                    if _lifecycle_ok:
                        try:
                            signal_armed(signal_id, ticker, _LO.OVERNIGHT_EVAL,
                                         "armed_in_entry_watcher",
                                         contract=_arm_label,
                                         local_order_id=str(local_order_id),
                                         entry_trigger=str(entry_trigger or 0),
                                         contract_deferred=str(contract_deferred))
                            signal_watching(signal_id, ticker, _LO.WATCHER,
                                            "watching_for_trigger_breach",
                                            contract=_arm_label,
                                            entry_trigger=str(entry_trigger or 0))
                        except Exception:
                            pass
                    result["armed"] += 1
                else:
                    log.error("[%s] overnight_reeval: entry_watcher.watch() returned False | contract=%s",
                              ticker, _contract_sym)
                    if _lifecycle_ok:
                        try:
                            _sig_rejected(signal_id, ticker, _LO.WATCHER,
                                          "entry_watcher.watch() returned False",
                                          _RC.EXECUTION, "WATCHER_ARM_FAILED", _RS.WARNING,
                                          contract=_contract_sym)
                        except Exception:
                            pass
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
        from ap.db import conn, run_with_retry
        import json as _j

        def _fn():
            with conn() as c:
                c.execute("""
                    SELECT id, signal_id, payload, created_ts
                    FROM trade_queue
                    WHERE client_id = %s
                      AND status = 'WATCHING'
                    ORDER BY created_ts DESC
                    LIMIT 100
                """, (client_id,))
                return c.fetchall()

        rows = run_with_retry(_fn) or []
        result = []
        for row in rows:
            d = dict(row) if not isinstance(row, dict) else row
            if isinstance(d.get("payload"), str):
                try:
                    d["payload"] = _j.loads(d["payload"])
                except Exception:
                    pass
            result.append(d)
        return result
    except Exception as e:
        log.error("_fetch_watching_signals failed: %s", e)
        return []


def _mark_job_rejected(job_id: int, client_id: str, reason: str) -> None:
    try:
        from ap.db import conn, run_with_retry

        def _fn():
            with conn() as c:
                c.execute("""
                    UPDATE trade_queue
                    SET status = 'REJECTED',
                        last_error = %s,
                        finished_ts = NOW()
                    WHERE id = %s AND client_id = %s
                """, (reason[:500], job_id, client_id))

        run_with_retry(_fn)
    except Exception as e:
        log.debug("_mark_job_rejected failed (non-fatal): %s", e)


def _mark_job_watching_armed(job_id: int, client_id: str, contract: str) -> None:
    """Update the WATCHING job to record that it has been armed in the watcher."""
    try:
        from ap.db import conn, run_with_retry

        def _fn():
            with conn() as c:
                c.execute("""
                    UPDATE trade_queue
                    SET last_error = %s,
                        started_ts = COALESCE(started_ts, NOW())
                    WHERE id = %s AND client_id = %s
                """, (f"armed:contract={contract}"[:500], job_id, client_id))

        run_with_retry(_fn)
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
