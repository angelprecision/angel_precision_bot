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
    except Exception as _e:
        log.warning("overnight_health_heartbeat_failed: %s", _e)

    result = {"processed": 0, "armed": 0, "rejected": 0, "skipped": 0, "errors": 0}

    # Guard: only run on trading days, 9:00-9:29 AM ET window (unless force=True)
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

            # Timeframe guard: overnight reeval is DAILY signals only.
            # 60m, 5m, 15m, 1h signals are intraday — by 9 AM the thesis
            # is hours stale and the prior-day levels are meaningless.
            # Only 1d / daily / overnight timeframes are valid here.
            _OVERNIGHT_TIMEFRAMES = set(
                os.getenv("OVERNIGHT_VALID_TIMEFRAMES", "1d,daily,overnight,weekly,1w").lower().split(",")
            )
            _sig_tf = str(signal.get("timeframe") or "1d").lower().strip()
            if _sig_tf not in _OVERNIGHT_TIMEFRAMES:
                log.info(
                    "[%s] overnight_reeval: skipping intraday signal %s (timeframe=%s) — "
                    "overnight reeval is daily signals only",
                    ticker, signal_id, _sig_tf,
                )
                _mark_job_rejected(job_id, client_id, f"intraday_timeframe_rejected:{_sig_tf}")
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
                # and mark contracts with MIN_CONTRACTS_PER_POSITION as the minimum configured default.
                try:
                    decision.plan.limit_price = 0.01   # overwritten at breach by live quote
                    if not getattr(decision.plan, "contracts", None):
                        decision.plan.contracts = int(os.getenv("MIN_CONTRACTS_PER_POSITION", "2"))
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

            # Propagate entry_trigger and overnight flag to the plan.
            # CRITICAL: also propagate prior_day_high, prior_day_low, and
            # timeframe so APEntryWatcher.watch() correctly identifies this
            # as an overnight signal and SKIPS the intraday staleness check.
            # Without these fields, _is_overnight_signal = False, and the
            # 1.5% drift guard fires at 0.2% movement — killing valid setups.
            # (GOOGL, JPM, AAPL all expired today with watch_arm_failed:
            # stale_price because the plan didn't carry these fields forward.)
            if entry_trigger:
                try:
                    decision.plan.trigger_price  = float(entry_trigger)
                    decision.plan.trigger_type   = "breach"
                    decision.plan.metadata["overnight"] = True
                    # Overnight signal identification fields for entry watcher
                    decision.plan.prior_day_high = prior_day_high or None
                    decision.plan.prior_day_low  = prior_day_low  or None
                    decision.plan.timeframe      = str(signal.get("timeframe") or "1d")
                except Exception:
                    pass

            # Step 5.5: LIVE AUTHORIZATION GATE — overnight reeval arms NEW entries
            # that fire at next open, so a LIVE client must be authorized here too.
            # Paper/sandbox clients are exempt (is_live_broker False). Fail closed
            # for LIVE clients on gate error.
            try:
                from ap.authorization import is_live_broker, check_live_authorization
                if is_live_broker(broker):
                    _authz_reason = check_live_authorization(client_id)
                    if _authz_reason:
                        log.warning("[%s] overnight_reeval: LIVE ENTRY BLOCKED — %s | client=%s",
                                    ticker, _authz_reason, client_id)
                        _mark_job_rejected(job_id, client_id, f"live_authorization:{_authz_reason}")
                        _log_rejection_supabase(
                            signal_id=signal_id, client_id=client_id, ticker=ticker,
                            side=side, score=float(signal.get("score") or 0),
                            stage="live_authorization",
                            reason_code=_authz_reason, human_reason=_authz_reason,
                            payload=signal,
                        )
                        try:
                            from ap.execution import audit
                            audit(client_id, "WARNING", "LIVE_ENTRY_BLOCKED", {
                                "reason_code": _authz_reason, "ticker": ticker,
                                "signal_id": signal_id, "path": "overnight_reeval",
                            })
                        except Exception:
                            pass
                        result["rejected"] += 1
                        continue
            except Exception as _authz_exc:
                try:
                    from ap.authorization import is_live_broker as _ilb
                    _live = _ilb(broker)
                except Exception:
                    _live = False
                if _live:
                    log.error("[%s] overnight_reeval: LIVE authz gate error — failing closed: %s",
                              ticker, _authz_exc)
                    _mark_job_rejected(job_id, client_id, f"live_authorization_gate_error:{_authz_exc}")
                    result["errors"] += 1
                    continue

            # Step 6: Create OSM entry order — let OSM generate a fresh UUID.
            # Do NOT pass local_order_id: static IDs cause OSM conflicts on retry.
            # For deferred contracts: ensure contract_symbol is NOT set to the
            # ticker symbol — store as DEFERRED so orders table is clean and
            # the execution core knows to select live at breach time.
            if contract_deferred:
                try:
                    if not getattr(decision.plan, "contract_symbol", None) or \
                       decision.plan.contract_symbol == decision.plan.ticker:
                        decision.plan.contract_symbol = f"DEFERRED:{ticker}"
                except Exception:
                    pass
            try:
                # PR fix/osm-watcher-handoff-and-entry-meta:
                # Insert OSM row already in PENDING_TRIGGER. Previously this path
                # created the row in CREATED and then called
                # mark_entry_pending_trigger() in Step 6b. The two-step approach
                # was racing the 30s LOST_HANDOFF_30S cancellation in
                # APOrderMonitor: 1,103 orders on 2026-05-26 went CREATED→CANCELED
                # with no intermediate PENDING_TRIGGER transition observable in
                # the OSM event log. Atomic initial_status='PENDING_TRIGGER'
                # eliminates the race entirely.
                from ap.authorization import execution_mode_for_broker
                local_order_id = order_state_machine.create_entry_order(
                    decision.plan,
                    initial_status="PENDING_TRIGGER",
                    execution_mode=execution_mode_for_broker(broker),
                )
            except Exception as osm_exc:
                log.error("[%s] overnight_reeval: OSM create_entry_order failed: %s", ticker, osm_exc)
                result["errors"] += 1
                continue

            if not local_order_id:
                log.error("[%s] overnight_reeval: OSM returned no local_order_id for %s", ticker, signal_id)
                result["errors"] += 1
                continue

            # Step 6b: Mark order PENDING_TRIGGER so order_monitor does not
            # cancel it as a stale CREATED order before the trigger breach fires.
            # This mirrors the queue.py intraday path which does the same before arming.
            try:
                if hasattr(order_state_machine, "mark_entry_pending_trigger"):
                    _pt_ok = order_state_machine.mark_entry_pending_trigger(local_order_id)
                else:
                    _pt_ok = order_state_machine.transition(
                        local_order_id, "PENDING_TRIGGER", submitted_ts=None)
                if not _pt_ok:
                    log.error("[%s] overnight_reeval: could not mark PENDING_TRIGGER for %s — skipping arm",
                              ticker, local_order_id)
                    result["errors"] += 1
                    continue
            except Exception as _pt_exc:
                log.error("[%s] overnight_reeval: PENDING_TRIGGER transition failed: %s — skipping arm",
                          ticker, _pt_exc)
                result["errors"] += 1
                continue

            # Step 7: Arm entry watcher — pass plan (not signal) and the OSM order ID
            _contract_sym = str(getattr(decision.plan, "contract_symbol", "") or "")
            _arm_label    = _contract_sym if _contract_sym else "DEFERRED_AT_BREACH"
            try:
                armed = entry_watcher.watch(decision.plan, local_order_id)
                if armed:
                    # Overnight reeval creates a LOCAL entry order before broker
                    # submission. That order waits for APEntryWatcher to see the
                    # breach. It MUST NOT remain in CREATED status, because
                    # APOrderMonitor treats CREATED/no broker_order_id as
                    # "never submitted" and cancels it after ORDER_TIMEOUT_CREATED.
                    #
                    # Correct overnight lifecycle:
                    #   CREATED          → immediately after local order creation
                    #   PENDING_TRIGGER  → after watcher arms (this block)
                    #   SUBMITTED        → after breach via
                    #                      APExecutionCore._on_entry_trigger()
                    #                      → APOrderStateMachine.submit_existing_entry()
                    #
                    # DO NOT submit to Tradier here. Submitting before breach
                    # would bypass the trigger-breach rule and enter prematurely.
                    # PENDING_TRIGGER was already set in Step 6b (pre-arm).
                    # Second call removed — duplicate transition on same local_order_id.

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
    """
    Fetch WATCHING signals from both sources of truth.

    1. trade_queue.status='WATCHING' for locally queued in-session jobs.
    2. ap_signals.decision_status='WATCHING' for scanner/audit signals written
       by APSignalStore.

    This fixes the production bug where the overnight reeval ran correctly but
    saw zero jobs because it only queried trade_queue while the scanner stored
    WATCHING state in Supabase ap_signals.decision_status.
    """
    results: list[dict] = []
    seen_signal_ids: set[str] = set()

    # Source 1: local Postgres trade_queue.
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
        for row in rows:
            d = dict(row) if not isinstance(row, dict) else row
            if isinstance(d.get("payload"), str):
                try:
                    d["payload"] = _j.loads(d["payload"])
                except Exception:
                    pass
            d["_source"] = "trade_queue"
            results.append(d)
            if d.get("signal_id"):
                seen_signal_ids.add(str(d["signal_id"]))
    except Exception as e:
        log.error("_fetch_watching_signals[trade_queue] failed: %s", e)

    # Source 2: Supabase ap_signals.
    try:
        import json as _j2
        from supabase import create_client as _cc

        sb_url = os.getenv("SUPABASE_URL", "")
        sb_key = (
            os.getenv("SUPABASE_SERVICE_KEY", "")
            or os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
            or os.getenv("SUPABASE_ANON_KEY", "")
        )
        if not sb_url or not sb_key:
            log.warning("_fetch_watching_signals[ap_signals]: missing Supabase credentials")
        else:
            cutoff = (
                datetime.now(timezone.utc)
                - timedelta(days=OVERNIGHT_SIGNAL_MAX_AGE_DAYS + 1)
            ).isoformat()
            sb = _cc(sb_url, sb_key)
            # ── MULTI-CLIENT FAN-OUT FIX ──────────────────────────────────
            # Do NOT filter ap_signals by client_email here. WATCHING rows in
            # ap_signals are SCANNER SETUPS (SMCI, BA, HOOD, SPY, ...), not
            # per-client jobs. The scanner writes a setup once, tagged to
            # whichever client_email it first ran for. Filtering by
            # client_email meant each client's overnight reeval only saw the
            # setups tagged to itself — so SMCI reached tradefluence but Jose's
            # reeval queried client_email=jose, found nothing, and SILENTLY
            # skipped SMCI entirely (no decision, no rejection, no trail).
            #
            # Every active client's reeval must see the SAME universe of
            # setups. Each client then runs its OWN risk/decision path via
            # master_control.evaluate(signal, client_id=client_id) downstream,
            # which logs an APPROVE or REJECT decision event per client. Same
            # setups for everyone; separate per-client execution decisions.
            #
            # (Source 1 trade_queue stays client-scoped — those are genuine
            # per-client in-session jobs, not shared scanner setups.)
            res = (
                sb.table("ap_signals")
                .select(
                    "signal_id, signal_payload, raw_payload, created_at, ticker, "
                    "side, score, timeframe, pattern, tier, decision_status, "
                    "entry_trigger, stop_price, target_price, underlying_at_signal"
                )
                .eq("decision_status", "WATCHING")
                .gte("created_at", cutoff)
                .order("created_at", desc=True)
                .limit(300)
                .execute()
            )

            for row in (res.data or []):
                sid = str(row.get("signal_id") or "")
                if not sid or sid in seen_signal_ids:
                    continue

                payload = row.get("signal_payload") or row.get("raw_payload") or {}
                if isinstance(payload, str):
                    try:
                        payload = _j2.loads(payload)
                    except Exception:
                        payload = {}
                if not isinstance(payload, dict):
                    payload = {}

                # Hydrate required downstream fields from columns if the JSON
                # payload is sparse.
                for key in ("ticker", "side", "score", "timeframe", "pattern", "tier"):
                    if not payload.get(key) and row.get(key) is not None:
                        payload[key] = row.get(key)
                if not payload.get("symbol") and row.get("ticker"):
                    payload["symbol"] = row.get("ticker")
                if not payload.get("signal_id"):
                    payload["signal_id"] = sid
                if not payload.get("created_at") and row.get("created_at"):
                    payload["created_at"] = row.get("created_at")
                if not payload.get("entry_trigger") and row.get("entry_trigger") is not None:
                    payload["entry_trigger"] = row.get("entry_trigger")
                if not payload.get("stop_price") and row.get("stop_price") is not None:
                    payload["stop_price"] = row.get("stop_price")
                if not payload.get("target_price") and row.get("target_price") is not None:
                    payload["target_price"] = row.get("target_price")
                if not payload.get("underlying_price") and row.get("underlying_at_signal") is not None:
                    payload["underlying_price"] = row.get("underlying_at_signal")

                results.append({
                    "id": f"sup:{sid}",
                    "signal_id": sid,
                    "payload": payload,
                    "created_ts": row.get("created_at"),
                    "_source": "ap_signals",
                })
                seen_signal_ids.add(sid)
    except Exception as e:
        log.error("_fetch_watching_signals[ap_signals] failed: %s", e)

    # ── SETUP-IDENTITY DEDUP ──────────────────────────────────────────────
    # Now that ap_signals is fetched WITHOUT a client_email filter, the same
    # logical setup (e.g. SMCI PUT 1d) may appear multiple times if it was
    # stored under more than one client_email. signal_id dedup (above) only
    # catches identical signal_ids. Collapse to one row per unique setup
    # identity (ticker|side|timeframe|entry_trigger) so each client's reeval
    # evaluates each distinct setup exactly once. trade_queue rows are kept
    # as-is (genuine per-client jobs, never deduped against scanner setups).
    _deduped: list[dict] = []
    _seen_setup_keys: set = set()
    for r in results:
        if r.get("_source") == "trade_queue":
            _deduped.append(r)
            continue
        p = r.get("payload") or {}
        _tkr = str(p.get("ticker") or p.get("symbol") or "").upper().strip()
        _side = str(p.get("side") or "").upper().strip()
        _tf = str(p.get("timeframe") or "").lower().strip()
        _trig = p.get("entry_trigger")
        try:
            _trig_k = round(float(_trig), 4) if _trig is not None else None
        except (TypeError, ValueError):
            _trig_k = None
        _key = (_tkr, _side, _tf, _trig_k)
        if _tkr and _key in _seen_setup_keys:
            log.info(
                "[%s] dedup: skipping duplicate setup %s %s %s (already have one)",
                client_id, _tkr, _side, _tf,
            )
            continue
        if _tkr:
            _seen_setup_keys.add(_key)
        _deduped.append(r)
    results = _deduped

    tq_count = sum(1 for r in results if r.get("_source") == "trade_queue")
    sup_count = sum(1 for r in results if r.get("_source") == "ap_signals")
    log.info(
        "[%s] _fetch_watching_signals: total=%d trade_queue=%d ap_signals=%d "
        "(fan-out: all clients see same scanner setups)",
        client_id, len(results), tq_count, sup_count,
    )
    return results


def _mark_job_rejected(job_id, client_id: str, reason: str) -> None:
    """Mark a WATCHING job rejected in its original source table.

    MULTI-CLIENT NOTE: for ap_signals scanner setups (sup: prefix) we do NOT
    flip the shared row's decision_status. That row is shared across all
    clients now (fan-out fix) — if client A's reeval flipped it to 'rejected'
    it would vanish from the WATCHING pool for clients B, C, ... before their
    reevals ran, recreating the exact silent-skip bug we just fixed. The
    per-client rejection is ALREADY recorded by master_control.evaluate's
    DECISION_EVENT (client_id scoped) and there is no per-client column on
    ap_signals. The shared scanner row ages out naturally via the created_at
    cutoff. trade_queue rows ARE genuinely per-client and still update.
    """
    job_id_str = str(job_id)
    if job_id_str.startswith("sup:"):
        # Per-client decision already logged via master_control DECISION_EVENT.
        # Do not mutate the shared scanner signal row.
        log.info(
            "[%s] reeval rejected shared setup %s — per-client decision logged "
            "(shared ap_signals row left WATCHING for other clients): %s",
            client_id, job_id_str[4:], reason[:200],
        )
        return

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
        log.debug("_mark_job_rejected[trade_queue] failed non-fatal: %s", e)


def _mark_job_watching_armed(job_id, client_id: str, contract: str) -> None:
    """Record that a WATCHING job has been armed in the entry watcher.

    MULTI-CLIENT NOTE: same rule as _mark_job_rejected. For shared ap_signals
    scanner setups (sup: prefix) we do NOT flip the shared row to 'armed'.
    The arm is tracked per-client by the entry watcher (keyed by client_id)
    and the per-client order/watcher rows. Flipping the shared row would hide
    the setup from other clients' reevals. trade_queue rows are per-client
    and still update normally.
    """
    job_id_str = str(job_id)
    label = f"armed:contract={contract}"[:500]
    if job_id_str.startswith("sup:"):
        log.info(
            "[%s] reeval armed shared setup %s for this client "
            "(per-client watcher created; shared ap_signals row left WATCHING "
            "for other clients) %s",
            client_id, job_id_str[4:], label,
        )
        return

    try:
        from ap.db import conn, run_with_retry

        def _fn():
            with conn() as c:
                c.execute("""
                    UPDATE trade_queue
                    SET last_error = %s,
                        started_ts = COALESCE(started_ts, NOW())
                    WHERE id = %s AND client_id = %s
                """, (label, job_id, client_id))
        run_with_retry(_fn)
    except Exception as e:
        log.debug("_mark_job_watching_armed[trade_queue] failed non-fatal: %s", e)


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
