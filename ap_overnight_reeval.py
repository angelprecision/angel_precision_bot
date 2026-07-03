"""
ap_overnight_reeval.py — Overnight Daily Signal Re-Evaluation Engine
=====================================================================
This is the missing piece of the full trading loop.

FLOW:
  1. Market close → scanner runs → signals arrive with timeframe=1d
  2. Bot marks them WATCHING (audit record, not yet armed)
  3. *** THIS MODULE *** runs during the 9:00–9:45 AM ET pre-open/open handoff window
  4. For each WATCHING signal:
     a. Fetch prior-day high/low from Tradier history
     b. Run overnight_daily_validator (directional invalidation check)
     c. If VALID: select contract, create OSM entry order, arm entry_watcher
     d. If INVALID: mark REJECTED with reason code, log to ap_signals
  5. After 9:30 AM ET open: entry_watcher polls quotes, waits for breach
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
import inspect
import time
from datetime import date, datetime, timezone, timedelta
from typing import TYPE_CHECKING, Optional

log = logging.getLogger("ap.overnight_reeval")

if TYPE_CHECKING:
    pass

# How many calendar days back a signal is still considered "fresh"
# e.g. a Friday signal is valid Monday morning = 3 days
OVERNIGHT_SIGNAL_MAX_AGE_DAYS = int(os.getenv("OVERNIGHT_SIGNAL_MAX_AGE_DAYS", "4"))

# OVERNIGHT_SNAPSHOT_FAIL_CLOSED: false (default) = snapshot unavailable
# before market open is DATA_NOT_READY, not a true invalidation.
# Signal stays WATCHING; retry at next reeval window.
_OVERNIGHT_SNAPSHOT_FAIL_CLOSED = (
    os.getenv("OVERNIGHT_SNAPSHOT_FAIL_CLOSED", "false").strip().lower()
    in ("true", "1")
)

# OVERNIGHT_REEVAL_SCORE_RECHECK_ENABLED=false (default):
# Already-WATCHING signals must not be hard-killed by a fresh morning
# second/intel score. Hard safety checks (capital, kill switch, client
# enabled, auth, hard risk veto, contract quality) STILL apply — only
# the intel/second-score block is treated as skip→RETRY_LATER.
_OVERNIGHT_REEVAL_SCORE_RECHECK_ENABLED = (
    os.getenv("OVERNIGHT_REEVAL_SCORE_RECHECK_ENABLED", "false").strip().lower()
    in ("true", "1")
)


def _hydrate_plan_from_signal(signal):
    """Build a minimal TradePlan-compatible namespace from a WATCHING signal.
    Used when MC rejects on intel/second-score but recheck is disabled —
    the signal was already scanner-approved and has all the fields needed
    for contract deferment + watcher arming.
    """
    import types as _types
    _trig = float(signal.get("entry_trigger") or 0) or None
    return _types.SimpleNamespace(
        ticker          = signal.get("ticker") or signal.get("symbol"),
        side            = (signal.get("side") or "").upper(),
        direction       = (signal.get("side") or "").upper(),
        score           = float(signal.get("score") or 0),
        timeframe       = str(signal.get("timeframe") or "1d"),
        entry_trigger   = _trig,
        trigger_price   = _trig,
        trigger_type    = "breach",
        prior_day_high  = signal.get("prior_day_high"),
        prior_day_low   = signal.get("prior_day_low"),
        pattern         = signal.get("pattern"),
        tier            = signal.get("tier"),
        contract_symbol = None,
        contracts       = None,
        limit_price     = None,
        metadata        = {
            "overnight":             True,
            "hydrated_from_signal":  True,
            "second_score_mode":     "observe_only",
        },
    )


def _normalize_overnight_side(value) -> str:
    raw = str(value or "").strip().upper()
    if raw in {"CALL", "BUY", "LONG", "BULL", "BULLISH", "CALLS"}:
        return "CALL"
    if raw in {"PUT", "SELL", "SHORT", "BEAR", "BEARISH", "PUTS"}:
        return "PUT"
    return "UNKNOWN"


# SIGNALS_LOOKBACK: hours-based alternative. When set, takes precedence over
# OVERNIGHT_SIGNAL_MAX_AGE_DAYS for the initial created_at cutoff query.
# Default 18h — covers signals from previous session's close to pre-market.
_SIGNALS_LOOKBACK_HOURS = int(os.getenv("SIGNALS_LOOKBACK", "18"))


def _et_now() -> datetime:
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("America/New_York"))


def _is_trading_day(dt: datetime) -> bool:
    return dt.weekday() < 5  # Mon-Fri


def _prior_trading_session_date(ref: Optional[datetime] = None) -> date:
    """Return the date of the prior *trading* session relative to ref (ET).

    Walks back at least one day and skips weekends, so on a Monday morning the
    prior trading session is the preceding Friday. This is the session a cached
    prior-day high/low MUST match to be considered fresh.

    NOTE: this does not model market holidays. A cached level whose stamped
    session is a holiday-shifted day will simply fail the equality guard and be
    treated as stale (fail-safe), which is the conservative behavior we want.
    """
    d = (ref or _et_now()).date()
    d = d - timedelta(days=1)
    while d.weekday() >= 5:  # Sat/Sun
        d = d - timedelta(days=1)
    return d


# ── PR4 (prior-day-level cache fallback) ─────────────────────────────────────
# DEFAULT OFF. When PRIOR_LEVEL_CACHE_FALLBACK != "1", the re-arm path behaves
# exactly as before. When enabled, prior-day H/L successfully fetched at re-arm
# are cached stamped with the trading session they represent, and a later re-arm
# whose fresh fetch returns null may fall back to that cache ONLY if the stamped
# session matches the actual prior trading session (no stale weekend/holiday/halt
# levels). Cache is process-local and best-effort — purely a resilience layer.
_PRIOR_LEVEL_CACHE_ENABLED = os.getenv("PRIOR_LEVEL_CACHE_FALLBACK", "0").strip() in ("1", "true", "yes")
_PRIOR_LEVEL_CACHE: dict[str, dict] = {}


def _cache_prior_levels(ticker: str, broker_session_date: date, high, low, close=None) -> None:
    """Stamp and store prior-day levels for a ticker keyed by trading session.

    `broker_session_date` MUST be the broker-provided prior_day_date that the
    caller has already verified equals the expected prior trading session. We
    never stamp with a locally-computed date — see the call site in the re-arm
    loop. Best-effort; never raises."""
    try:
        if high is None and low is None:
            return
        _PRIOR_LEVEL_CACHE[(ticker or "").upper()] = {
            "session_date": broker_session_date.isoformat(),
            "prior_day_high": high,
            "prior_day_low": low,
            "prior_day_close": close,
        }
    except Exception:
        pass


def _get_cached_prior_levels(ticker: str, expected_session: date) -> Optional[dict]:
    """Return cached prior-day levels for ticker ONLY if the stamped session
    matches expected_session (the actual prior trading session). Otherwise None
    (stale → fail-safe). Never raises."""
    try:
        rec = _PRIOR_LEVEL_CACHE.get((ticker or "").upper())
        if not rec:
            return None
        if rec.get("session_date") != expected_session.isoformat():
            return None  # stale: wrong session, do not use
        return rec
    except Exception:
        return None


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


def _log_overnight_watch_cleanup(
    event: str,
    *,
    level: str,
    client_id: str,
    signal_id: str,
    ticker: str,
    side: str,
    local_order_id: str,
    contract: str,
    contract_deferred: bool,
    entry_trigger,
    cleanup_method: str,
    cleanup_success: bool,
    reason: str,
) -> None:
    try:
        _entry_trigger = "None" if entry_trigger is None else f"{float(entry_trigger):.4f}"
    except Exception:
        _entry_trigger = str(entry_trigger)
    getattr(log, level)(
        "%s client_id=%s signal_id=%s ticker=%s side=%s local_order_id=%s "
        "contract=%s contract_deferred=%s entry_trigger=%s cleanup_method=%s "
        "cleanup_success=%s reason=%s",
        event,
        client_id,
        signal_id,
        ticker,
        side,
        local_order_id,
        contract,
        contract_deferred,
        _entry_trigger,
        cleanup_method,
        cleanup_success,
        reason,
    )


def _cleanup_overnight_watch_arm_failure(
    *,
    order_state_machine,
    client_id: str,
    signal_id: str,
    ticker: str,
    side: str,
    local_order_id: str,
    contract: str,
    contract_deferred: bool,
    entry_trigger,
    reason: str,
    done_event: str,
) -> tuple[bool, str]:
    _log_overnight_watch_cleanup(
        "OVERNIGHT_WATCH_ARM_FAILED_CLEANUP_START",
        level="warning",
        client_id=client_id,
        signal_id=signal_id,
        ticker=ticker,
        side=side,
        local_order_id=local_order_id,
        contract=contract,
        contract_deferred=contract_deferred,
        entry_trigger=entry_trigger,
        cleanup_method="pending",
        cleanup_success=False,
        reason=reason,
    )

    cleanup_method = "none"
    cleanup_success = False

    if local_order_id:
        if hasattr(order_state_machine, "expire_pending_entry"):
            cleanup_method = "expire_pending_entry"
            try:
                cleanup_success = bool(
                    order_state_machine.expire_pending_entry(
                        local_order_id,
                        reason=reason,
                    )
                )
            except Exception as exp_exc:
                log.error(
                    "[%s] overnight_reeval: expire_pending_entry cleanup failed "
                    "| local_order_id=%s reason=%s error=%s",
                    ticker, local_order_id, reason, exp_exc,
                )
                cleanup_success = False

        if not cleanup_success and hasattr(order_state_machine, "cancel_pending_entry"):
            cleanup_method = "cancel_pending_entry"
            try:
                cleanup_success = bool(
                    order_state_machine.cancel_pending_entry(
                        local_order_id,
                        reason=reason,
                    )
                )
            except Exception as cancel_exc:
                log.error(
                    "[%s] overnight_reeval: cancel_pending_entry cleanup failed "
                    "| local_order_id=%s reason=%s error=%s",
                    ticker, local_order_id, reason, cancel_exc,
                )
                cleanup_success = False

        if not cleanup_success and hasattr(order_state_machine, "transition"):
            cleanup_method = "transition:EXPIRED"
            try:
                cleanup_success = bool(
                    order_state_machine.transition(
                        local_order_id,
                        "EXPIRED",
                        last_error=reason,
                    )
                )
            except Exception as trans_exc:
                log.error(
                    "[%s] overnight_reeval: transition(EXPIRED) cleanup failed "
                    "| local_order_id=%s reason=%s error=%s",
                    ticker, local_order_id, reason, trans_exc,
                )
                cleanup_success = False
    else:
        cleanup_method = "missing_local_order_id"

    _log_overnight_watch_cleanup(
        done_event,
        level="info" if cleanup_success else "error",
        client_id=client_id,
        signal_id=signal_id,
        ticker=ticker,
        side=side,
        local_order_id=local_order_id,
        contract=contract,
        contract_deferred=contract_deferred,
        entry_trigger=entry_trigger,
        cleanup_method=cleanup_method,
        cleanup_success=cleanup_success,
        reason=reason,
    )
    return cleanup_success, cleanup_method


def _resolve_canonical_signal_id(signal_id: str, signal: dict) -> str:
    try:
        from ap_canonical_signal import build_canonical_signal_id
        return build_canonical_signal_id(signal_id, signal) or str(signal_id or "")
    except Exception:
        return str((signal or {}).get("canonical_signal_id") or signal_id or "")


def _overnight_reeval_session_key(now: Optional[datetime] = None) -> str:
    now = now or _et_now()
    return now.date().isoformat()


def _watch_arm_cleanup_failed_reason(original_reason: str) -> str:
    original_reason = str(original_reason or "unknown")
    return f"overnight_watch_arm_failed_cleanup_failed:{original_reason}"


def _paper_overnight_rescue_only_signal(
    signal: dict | None,
    *,
    job_source: str | None,
    execution_mode: str | None,
) -> bool:
    if str(job_source or "").strip().lower() != "trade_queue":
        return False
    if str(execution_mode or "").strip().upper() != "PAPER":
        return False
    payload = signal if isinstance(signal, dict) else {}
    return bool(payload.get("force_overnight_reeval_only")) and bool(payload.get("do_not_queue_directly"))


def _paper_rescue_queue_reason(kind: str, detail: str | None = None) -> str:
    kind = str(kind or "").strip()
    detail = str(detail or "").strip()
    return f"{kind}:{detail}" if detail else kind


def _is_terminal_watch_arm_failure_reason(reason: str) -> bool:
    reason = str(reason or "")
    return (
        reason.startswith("overnight_watch_arm_failed:")
        or reason.startswith("overnight_watch_arm_failed_cleanup_failed:")
    )


def _get_client_opportunity_row(signal_id: str, client_id: str, signal: dict) -> tuple[str, Optional[dict]]:
    canonical_signal_id = _resolve_canonical_signal_id(signal_id, signal)
    if not canonical_signal_id or not client_id:
        return canonical_signal_id, None

    try:
        from ap.opportunity_ledger import _get_sb
        sb = _get_sb()
        if not sb:
            return canonical_signal_id, None
        res = (
            sb.table("client_signal_opportunities")
            .select("opportunity_status, miss_stage, miss_reason, metadata")
            .eq("canonical_signal_id", canonical_signal_id)
            .eq("client_id", client_id)
            .limit(1)
            .execute()
        )
        rows = getattr(res, "data", None) or []
        if rows:
            row = rows[0] if isinstance(rows[0], dict) else dict(rows[0])
            return canonical_signal_id, row
    except Exception as exc:
        log.debug(
            "[%s] overnight_reeval: client opportunity lookup failed for %s: %s",
            client_id,
            canonical_signal_id,
            exc,
        )
    return canonical_signal_id, None


def _shared_watch_arm_failure_already_recorded(
    signal_id: str,
    client_id: str,
    signal: dict,
    *,
    session_key: Optional[str] = None,
) -> bool:
    canonical_signal_id, row = _get_client_opportunity_row(signal_id, client_id, signal)
    if not row:
        return False

    _status = str(row.get("opportunity_status") or "").upper()
    _stage = str(row.get("miss_stage") or "").upper()
    _reason = str(row.get("miss_reason") or "")
    _metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    _session_key = session_key or _overnight_reeval_session_key()
    _recorded_session = str(_metadata.get("overnight_reeval_session_key") or "")
    if _recorded_session != _session_key:
        return False

    if _status == "MISSED" and _stage == "WATCHER_ARM" and _is_terminal_watch_arm_failure_reason(_reason):
        log.info(
            "[%s] overnight_reeval: shared setup already has same-session WATCHER_ARM proof "
            "for canonical_signal_id=%s session_key=%s — skipping repeat local order creation",
            client_id,
            canonical_signal_id,
            _session_key,
        )
        return True

    if _status == "INTERNAL_ERROR" and _is_terminal_watch_arm_failure_reason(_reason):
        log.info(
            "[%s] overnight_reeval: shared setup already has same-session INTERNAL_ERROR arm-failure proof "
            "for canonical_signal_id=%s session_key=%s — skipping repeat local order creation",
            client_id,
            canonical_signal_id,
            _session_key,
        )
        return True

    return False


def _record_watch_arm_failure_proof(
    *,
    signal_id: str,
    client_id: str,
    signal: dict,
    reason: str,
    local_order_id: str,
    job_id,
    is_exception: bool,
    session_key: Optional[str] = None,
    cleanup_method: Optional[str] = None,
    cleanup_success: Optional[bool] = None,
    cleanup_failed: bool = False,
    original_reason: Optional[str] = None,
) -> None:
    try:
        from ap.opportunity_ledger import (
            create_opportunities,
            mark_internal_error,
            mark_watcher_invalidated,
        )
        canonical_signal_id = _resolve_canonical_signal_id(signal_id, signal)
        _payload = dict(signal or {})
        _payload.setdefault("signal_id", signal_id)
        create_opportunities(
            signal_id,
            [client_id],
            _payload,
            canonical_signal_id=canonical_signal_id,
        )
        _extra_meta = {
            "overnight_watch_arm_failure": True,
            "overnight_source_job_id": str(job_id),
            "overnight_source_table": (
                "ap_signals" if str(job_id).startswith("sup:") else "trade_queue"
            ),
            "overnight_reeval_session_key": session_key or _overnight_reeval_session_key(),
        }
        if cleanup_method is not None:
            _extra_meta["cleanup_method"] = str(cleanup_method)
        if cleanup_success is not None:
            _extra_meta["cleanup_success"] = bool(cleanup_success)
        if cleanup_failed:
            _extra_meta["overnight_watch_arm_cleanup_failed"] = True
        if original_reason is not None:
            _extra_meta["original_reason"] = str(original_reason)
        if is_exception:
            mark_internal_error(
                signal_id,
                client_id,
                reason,
                canonical_signal_id=canonical_signal_id,
                order_local_id=str(local_order_id or ""),
                extra_meta=_extra_meta,
            )
        else:
            mark_watcher_invalidated(
                signal_id,
                client_id,
                reason,
                canonical_signal_id=canonical_signal_id,
                order_local_id=str(local_order_id or ""),
                extra_meta=_extra_meta,
            )
    except Exception as exc:
        log.warning(
            "[%s] overnight_reeval: failed to persist watch-arm failure proof "
            "signal=%s local_order_id=%s error=%s",
            client_id,
            signal_id,
            local_order_id,
            exc,
        )


def run_overnight_reeval(
    *,
    client_id: str,
    broker,
    data_broker=None,
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
    Called during the 9:00–9:45 AM ET handoff window. Contract selection is
    deferred when pre-market option chains are not yet ready, so this can
    safely run before 9:30 without requiring live regular-session quotes.

    Returns summary dict with core counts plus stale/fresh visibility.
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

    attached_data_broker = None
    if broker is not None:
        try:
            _attached_data_broker = inspect.getattr_static(broker, "data_broker")
        except AttributeError:
            _attached_data_broker = None
        if _attached_data_broker is not None:
            attached_data_broker = getattr(broker, "data_broker", None)
    market_data_broker = data_broker or attached_data_broker or broker
    try:
        from ap.authorization import execution_mode_for_broker as _exec_mode_for_broker
        _run_execution_mode = str(_exec_mode_for_broker(broker) or "").upper()
    except Exception:
        _run_execution_mode = ""
    log.info(
        "OVERNIGHT_REEVAL_MARKET_DATA_BROKER_SELECTED "
        "client_id=%s execution_mode=%s data_broker_present=%s "
        "broker_class=%s data_broker_class=%s market_data_broker_class=%s",
        client_id,
        _run_execution_mode or "UNKNOWN",
        bool(data_broker is not None or attached_data_broker is not None),
        type(broker).__name__ if broker is not None else "None",
        type(data_broker).__name__ if data_broker is not None else "None",
        type(market_data_broker).__name__ if market_data_broker is not None else "None",
    )
    if (
        _run_execution_mode == "PAPER"
        and data_broker is None
        and attached_data_broker is None
        and market_data_broker is broker
    ):
        log.warning(
            "PAPER_OVERNIGHT_DATA_BROKER_MISSING_USING_EXECUTION_BROKER "
            "client_id=%s execution_mode=%s broker_class=%s market_data_broker_class=%s",
            client_id,
            _run_execution_mode,
            type(broker).__name__ if broker is not None else "None",
            type(market_data_broker).__name__ if market_data_broker is not None else "None",
        )
    result = {
        "processed": 0,
        "armed": 0,
        "rejected": 0,
        "skipped": 0,
        "errors": 0,
        "stale_skipped": 0,
        "fresh_processed": 0,
        "fresh_armed": 0,
        "stale_inventory_only": False,
        # P0 (2026-07-02): silent-stall detection. Production paper reeval
        # reported plain "success" for 4 consecutive sessions while every
        # fetched row hit the RETRY_LATER branches (sandbox broker cannot
        # serve prior-day levels) and stayed WATCHING forever — 486 rows
        # frozen at awaiting_overnight_reeval with a green lock row. These
        # two fields make that condition visible in the return value so the
        # caller can persist an honest status instead of "success".
        "fetched": 0,
        "stalled": False,
    }

    # Guard: only run on trading days, 9:00-9:45 AM ET window (unless force=True)
    now_et = _et_now()
    session_key = _overnight_reeval_session_key(now_et)
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
    result["fetched"] = len(watching_signals or [])
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
        job_source = job.get("_source") or ("ap_signals" if str(job_id).startswith("sup:") else "trade_queue")
        ticker = signal.get("ticker") or signal.get("symbol", "?")
        side = _normalize_overnight_side(signal.get("side") or signal.get("direction"))
        if side not in {"CALL", "PUT"}:
            log.warning(
                "[%s] overnight_reeval: INVALID_OR_MISSING_SIDE signal=%s raw_side=%r raw_direction=%r — rejecting before trigger math",
                ticker,
                signal_id,
                signal.get("side"),
                signal.get("direction"),
            )
            _mark_job_rejected(job_id, client_id, "invalid_or_missing_side")
            result["rejected"] += 1
            continue
        signal["side"] = side
        signal["direction"] = side
        _execution_mode = _run_execution_mode
        _paper_rescue_only = _paper_overnight_rescue_only_signal(
            signal,
            job_source=job_source,
            execution_mode=_execution_mode,
        )

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
                    result["stale_skipped"] += 1
                    continue
            result["fresh_processed"] += 1

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

            _shared_watch_arm_recorded = _shared_watch_arm_failure_already_recorded(
                signal_id, client_id, signal, session_key=session_key
            )
            if _shared_watch_arm_recorded and _paper_rescue_only:
                _mark_job_rejected(
                    job_id,
                    client_id,
                    "duplicate_setup:same_session_watch_arm_or_terminal_failure_proof",
                )
                result["rejected"] += 1
                continue
            if job_source == "ap_signals" and _shared_watch_arm_recorded:
                result["skipped"] = result.get("skipped", 0) + 1
                continue

            # Step 1: Fetch prior-day levels from broker
            prior_levels = {}
            if hasattr(market_data_broker, "get_prior_day_levels"):
                prior_levels = market_data_broker.get_prior_day_levels(ticker) or {}
            else:
                log.warning(
                    "[%s] market-data broker has no get_prior_day_levels — cannot validate overnight daily signal",
                    ticker,
                )

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

            # ── PR4: session-validated fresh + cache fallback ──────────────────
            # DEFAULT OFF (PRIOR_LEVEL_CACHE_FALLBACK). When on:
            #   (a) FRESH levels are only valid for use AND cache when the broker's
            #       own returned prior_day_date equals the expected prior trading
            #       session. (amendment) On mismatch/missing broker date we FAIL
            #       CLOSED on the fresh values themselves — clear them — so a
            #       stale/lagged/holiday bar can never arm a trade, not just never
            #       be cached.
            #   (b) if fresh is null or was invalidated, fall back to a cached
            #       level ONLY if its stamped (broker) session matches the
            #       expected prior trading session.
            _prior_levels_source = "fresh"
            if _PRIOR_LEVEL_CACHE_ENABLED:
                _expected_session = _prior_trading_session_date(_et_now())
                _broker_date_raw = prior_levels.get("prior_day_date")
                _broker_session = None
                if _broker_date_raw:
                    try:
                        _broker_session = date.fromisoformat(str(_broker_date_raw)[:10])
                    except Exception:
                        _broker_session = None

                _have_fresh = (prior_day_high is not None or prior_day_low is not None)
                _session_ok = (_broker_session is not None and _broker_session == _expected_session)

                if _have_fresh and _session_ok:
                    # Correct-session fresh data: use it AND cache it.
                    _cache_prior_levels(
                        ticker, _broker_session,
                        prior_day_high, prior_day_low,
                        prior_levels.get("prior_day_close"),
                    )
                elif _have_fresh and not _session_ok:
                    # FAIL CLOSED: broker date missing or wrong session. Do NOT
                    # use these levels directly and do NOT cache them — discard.
                    log.warning(
                        "[%s] PRIOR_LEVEL_SESSION_MISMATCH signal=%s broker_date=%s "
                        "expected_session=%s — discarding fresh levels (stale/lagged)",
                        ticker, signal_id,
                        (_broker_session.isoformat() if _broker_session else _broker_date_raw),
                        _expected_session.isoformat(),
                    )
                    prior_day_high = None
                    prior_day_low = None
                    # Only clear prior_day_close if it came from this same stale
                    # broker fetch (don't discard a trusted pre-existing value).
                    if prior_levels.get("prior_day_close") is not None:
                        prior_levels = dict(prior_levels)
                        prior_levels["prior_day_close"] = None

                # Single cache fallback: whenever we have no usable fresh value
                # (null fetch, or fresh just discarded on session mismatch), try
                # the session-matched cache. If the cache is wrong-session or
                # absent, levels stay None and the existing fail-safe
                # (INVALIDATED_MISSING_PRIOR_LEVELS) runs below.
                if prior_day_high is None and prior_day_low is None:
                    _cached = _get_cached_prior_levels(ticker, _expected_session)
                    if _cached:
                        prior_day_high = float(_cached.get("prior_day_high") or 0) or None
                        prior_day_low = float(_cached.get("prior_day_low") or 0) or None
                        _prior_levels_source = "cache"
                        log.warning(
                            "[%s] PRIOR_LEVEL_CACHE_FALLBACK_USED signal=%s session=%s "
                            "high=%s low=%s — using broker-session-matched cache",
                            ticker, signal_id, _expected_session.isoformat(),
                            prior_day_high, prior_day_low,
                        )

            # Enrich signal with fetched levels for watcher and validator
            signal["prior_day_high"] = prior_day_high
            signal["prior_day_low"] = prior_day_low
            signal["prior_levels_source"] = _prior_levels_source
            if prior_levels.get("prior_day_close"):
                signal["prior_day_close"] = prior_levels["prior_day_close"]

            # Side-specific prior-level check:
            #   CALL needs prior_day_high (or explicit entry_trigger)
            #   PUT  needs prior_day_low  (or explicit entry_trigger)
            # If the required level is unavailable due to broker/history fetch
            # failure, keep WATCHING and retry — do not permanently reject.
            _has_trigger  = bool(float(signal.get("entry_trigger") or 0))
            _side_upper   = (side or "").upper()
            _missing_level = None
            if not _has_trigger:
                if _side_upper == "CALL" and prior_day_high is None:
                    _missing_level = "prior_day_high"
                elif _side_upper == "PUT" and prior_day_low is None:
                    _missing_level = "prior_day_low"
            if _missing_level:
                log.warning(
                    "[%s] overnight_reeval: OVERNIGHT_PRIOR_LEVELS_UNAVAILABLE "
                    "signal=%s side=%s missing=%s "
                    "category=DATA_UNAVAILABLE severity=WARNING "
                    "final_decision=RETRY_LATER "
                    "reason_code=OVERNIGHT_PRIOR_LEVELS_UNAVAILABLE",
                    ticker, signal_id, _side_upper, _missing_level,
                )
                if _paper_rescue_only:
                    _mark_job_watching_reason(
                        job_id,
                        client_id,
                        _paper_rescue_queue_reason(
                            "after_hours_deferred",
                            f"overnight_reeval_data_pending:{_missing_level}",
                        ),
                    )
                result["skipped"] = result.get("skipped", 0) + 1
                continue  # leave job WATCHING for next reeval run

            # Step 2: Derive entry_trigger if not provided by scanner
            # The Strat: CALL entries breach prior-day high; PUT entries breach prior-day low
            entry_trigger = float(signal.get("entry_trigger") or 0) or None
            if entry_trigger is None:
                if side == "CALL":
                    entry_trigger = prior_day_high
                elif side == "PUT":
                    entry_trigger = prior_day_low
            if not entry_trigger and _paper_rescue_only:
                _mark_job_rejected(job_id, client_id, "trigger_invalid")
                result["rejected"] += 1
                continue
            if entry_trigger:
                signal["entry_trigger"] = entry_trigger

            # Step 3: Overnight daily structure validation
            # Fetch a fresh market snapshot (pre-market quote)
            snapshot = fetch_market_snapshot(ticker, market_data_broker)
            validation = validate_overnight_daily_signal(
                ticker=ticker,
                side=side,
                prior_day_high=prior_day_high,
                prior_day_low=prior_day_low,
                snapshot=snapshot,
            )

            if not validation.valid:
                # Distinguish snapshot-unavailable (DATA_NOT_READY) from true invalidation.
                _snap_miss = "SNAPSHOT_UNAVAILABLE" in (validation.reason_code or "").upper()
                if _snap_miss and not _OVERNIGHT_SNAPSHOT_FAIL_CLOSED:
                    # Pre-market snapshot not ready — keep signal WATCHING for next retry.
                    # OVERNIGHT_SNAPSHOT_FAIL_CLOSED=false (default).
                    log.warning(
                        "[%s] overnight_reeval: OVERNIGHT_SNAPSHOT_UNAVAILABLE %s "
                        "category=DATA_UNAVAILABLE severity=WARNING "
                        "final_decision=RETRY_LATER "
                        "human_reason='Market snapshot unavailable — retry later'",
                        ticker, signal_id,
                    )
                    if _paper_rescue_only:
                        _mark_job_watching_reason(
                            job_id,
                            client_id,
                            "after_hours_deferred:overnight_snapshot_unavailable",
                        )
                    result["skipped"] = result.get("skipped", 0) + 1
                    continue  # leave job WATCHING for next reeval run
                # True invalidation — reject
                log.info("[%s] overnight_reeval: OVERNIGHT_TRUE_INVALIDATION %s — %s",
                         ticker, signal_id, validation.reason_code)
                _mark_job_rejected(job_id, client_id,
                                   f"overnight_invalidated:{validation.reason_code}")
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
                    # Classify the rejection: intel/second-score vs hard safety.
                    # When OVERNIGHT_REEVAL_SCORE_RECHECK_ENABLED=false, morning
                    # score/intel rechecks on already-WATCHING signals are
                    # observe-only and must proceed to watcher arm.
                    # Hard safety blocks ALWAYS reject regardless of the flag.
                    _r = (decision.reason or "").lower()
                    _reason_code = str(getattr(decision, "reason_code", "") or "").upper()
                    _intel_phrases = (
                        "intel_skip", "blocked_intel", "intel_score",
                        "insufficient edge", "gate_g", "gate g",
                        "scanner_approved_intel_observe_only",
                        "data_unavailable_scanner_fallback",
                        "blocked_score",
                        "rejected_low_score",
                        "score_below_floor",
                        "score_below_priority_floor",
                        "context_below_floor",
                        "tier_reject",
                    )
                    _observe_only_reason_codes = {
                        "SCORE_BELOW_THRESHOLD",
                    }
                    _hard_safety_phrases = (
                        "capital", "buying_power", "buying power",
                        "kill_switch", "kill switch",
                        "entries_paused", "entries paused",
                        "client_disabled", "client disabled",
                        "auth", "invalid_side", "invalid side",
                        "invalid direction", "risk_limit", "risk limit",
                        "daily_loss", "daily loss",
                        "risk manager", "risk_manager",
                        "contract quality failed", "contract_quality",
                        "scanner signal is neutral", "neutral direction",
                    )
                    _is_intel_block        = (
                        any(p in _r for p in _intel_phrases)
                        or _reason_code in _observe_only_reason_codes
                    )
                    _is_hard_safety_block  = any(p in _r for p in _hard_safety_phrases)

                    if (_is_intel_block
                            and not _is_hard_safety_block
                            and not _OVERNIGHT_REEVAL_SCORE_RECHECK_ENABLED):
                        # Intel/second-score block on already-WATCHING signal.
                        # Do NOT skip and do NOT reject. Hydrate a minimal plan
                        # from the scanner-approved signal and proceed to arm path.
                        log.info(
                            "[%s] overnight_reeval: OVERNIGHT_SCORE_RECHECK_DISABLED "
                            "signal=%s mc_reason=%s "
                            "decision=PROCEED_TO_ARM second_score_mode=observe_only "
                            "— intel/second-score ignored, using scanner-approved signal",
                            ticker, signal_id, decision.reason,
                        )
                        _hydrated = _hydrate_plan_from_signal(signal)
                        try:
                            if getattr(decision, "plan", None) is None:
                                decision.plan = _hydrated
                            else:
                                # MC built a plan but rejected on intel — keep MC's
                                # plan and overlay signal fields the watcher needs.
                                for _attr in ("ticker", "side", "score", "timeframe",
                                              "entry_trigger", "trigger_price",
                                              "prior_day_high", "prior_day_low"):
                                    if getattr(decision.plan, _attr, None) in (None, 0, "", 0.0):
                                        setattr(decision.plan, _attr,
                                                getattr(_hydrated, _attr, None))
                            decision.ok = True
                        except Exception:
                            # decision is immutable — build a fresh namespace
                            import types as _types_p
                            decision = _types_p.SimpleNamespace(
                                ok=True, plan=_hydrated,
                                reason=f"observe_only:{decision.reason}",
                                score=float(signal.get("score") or 0),
                            )
                        # Fall through to Step 5 (contract selection / arm)
                    else:
                        # Hard safety block OR recheck enabled — reject as before
                        log.info(
                            "[%s] overnight_reeval: MC blocked %s — %s "
                            "(hard_safety=%s intel=%s recheck_enabled=%s)",
                            ticker, signal_id, decision.reason,
                            _is_hard_safety_block, _is_intel_block,
                            _OVERNIGHT_REEVAL_SCORE_RECHECK_ENABLED,
                        )
                        if _paper_rescue_only:
                            _reason_text = str(decision.reason or "")
                            _reason_prefix = "duplicate_setup" if "duplicate" in _reason_text.lower() else "risk_blocked"
                            _mark_job_rejected(
                                job_id,
                                client_id,
                                _paper_rescue_queue_reason(_reason_prefix, _reason_text),
                            )
                        else:
                            _mark_job_rejected(job_id, client_id,
                                               f"mc_blocked:{decision.reason}")
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
                if _paper_rescue_only:
                    _mark_job_error(
                        job_id,
                        client_id,
                        _paper_rescue_queue_reason(
                            "risk_blocked",
                            f"master_control_exception:{type(mc_exc).__name__}",
                        ),
                    )
                result["errors"] += 1
                continue

            # Step 5: Contract selection — best-effort only.
            # Pre-market option chains have zero bids (options don't trade before
            # 9:30 AM ET). If selection fails here, we arm the watcher with
            # contract_deferred=True so the execution core selects the contract
            # at breach time when live quotes are available.
            # NEVER permanently reject a valid signal because of pre-market chain data.
            contract_deferred = False
            _selector_failure = None
            try:
                selected = contract_selector.select(decision.plan)
            except Exception as cs_exc:
                _selector_failure = {
                    "reason_code": "PRE_MARKET_SELECTOR_EXCEPTION",
                    "error_type": type(cs_exc).__name__,
                    "error": str(cs_exc),
                }
                log.warning(
                    "[%s] overnight_reeval: contract selection failed pre-market (%s) "
                    "— deferring to breach time with live quotes",
                    ticker, cs_exc,
                )
                selected = None
            if selected is None:
                try:
                    if hasattr(contract_selector, "get_last_failure"):
                        _selector_failure = contract_selector.get_last_failure() or _selector_failure
                except Exception:
                    pass

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
                    decision.plan.metadata.update({
                        "contract_deferred": True,
                        "pre_market_contract_selection_failed": True,
                        "pre_market_selector_failure": _selector_failure,
                        "pre_market_selector_reason_code": (
                            _selector_failure.get("reason_code")
                            if isinstance(_selector_failure, dict)
                            else None
                        ),
                        "contract_selection_deferred_to": "breach_time",
                    })
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
            # that fire at next open, so a LIVE client must be evaluated here too.
            # Same semantics as ap/queue.py: an UNKNOWN/unverifiable broker mode
            # is treated as a block candidate (never assumed paper); known paper/
            # sandbox clients are exempt. OBSERVE mode logs WOULD_BLOCK and arms
            # the entry anyway; ENFORCE mode rejects it. Gate errors fail closed
            # only in ENFORCE mode for live/unknown brokers.
            try:
                from ap.authorization import (
                    is_live_broker, broker_live_mode_known, check_live_authorization,
                    authorization_gate_enforced, LIVE_AUTHORIZATION_GATE_UNAVAILABLE,
                )
                _gate_reason = None
                if not broker_live_mode_known(broker):
                    _gate_reason = LIVE_AUTHORIZATION_GATE_UNAVAILABLE
                elif is_live_broker(broker):
                    _gate_reason = check_live_authorization(client_id)

                if _gate_reason:
                    if not authorization_gate_enforced():
                        # OBSERVE MODE — log and ARM anyway.
                        log.warning("[%s] overnight_reeval: LIVE_AUTHORIZATION_WOULD_BLOCK — %s | client=%s (observe mode; armed)",
                                    ticker, _gate_reason, client_id)
                        try:
                            from ap.execution import audit
                            audit(client_id, "WARNING", "LIVE_AUTHORIZATION_WOULD_BLOCK", {
                                "reason_code": _gate_reason, "ticker": ticker,
                                "signal_id": signal_id, "path": "overnight_reeval",
                                "enforced": False,
                            })
                        except Exception:
                            pass
                        # fall through — entry is armed in observe mode.
                    else:
                        # ENFORCE MODE — reject the armed entry.
                        log.warning("[%s] overnight_reeval: LIVE ENTRY BLOCKED — %s | client=%s",
                                    ticker, _gate_reason, client_id)
                        _mark_job_rejected(job_id, client_id, f"live_authorization:{_gate_reason}")
                        _log_rejection_supabase(
                            signal_id=signal_id, client_id=client_id, ticker=ticker,
                            side=side, score=float(signal.get("score") or 0),
                            stage="live_authorization",
                            reason_code=_gate_reason, human_reason=_gate_reason,
                            payload=signal,
                        )
                        try:
                            from ap.execution import audit
                            audit(client_id, "WARNING", "LIVE_ENTRY_BLOCKED", {
                                "reason_code": _gate_reason, "ticker": ticker,
                                "signal_id": signal_id, "path": "overnight_reeval",
                                "enforced": True,
                            })
                        except Exception:
                            pass
                        result["rejected"] += 1
                        continue
            except Exception as _authz_exc:
                try:
                    from ap.authorization import (
                        is_live_broker as _ilb, broker_live_mode_known as _bmk,
                        authorization_gate_enforced as _enf,
                    )
                    _enforced = _enf()
                    _risky = (not _bmk(broker)) or _ilb(broker)
                except Exception:
                    _enforced, _risky = False, False
                if _enforced and _risky:
                    log.error("[%s] overnight_reeval: LIVE authz gate error — failing closed (enforce): %s",
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
                #
                # P0 (hotfix/p0-preserve-overnight-sizing-meta):
                # Pass decision.plan.metadata into OSM so sizing_context,
                # contract_deferred, risk_profile_source, and snapshot_at_eval
                # are persisted on the order row. Without this, overnight/deferred
                # orders had null sizing_context — making budget debugging blind.
                # OSM merges caller meta on top of its auto-meta (caller wins on
                # conflict), so sizing_context is preserved without erasing OSM
                # auto-fields like score, tier, signal_id, etc.
                from ap.authorization import execution_mode_for_broker
                _plan_meta = getattr(decision.plan, "metadata", None) or {}
                local_order_id = order_state_machine.create_entry_order(
                    decision.plan,
                    initial_status="PENDING_TRIGGER",
                    execution_mode=execution_mode_for_broker(broker),
                    meta=_plan_meta,
                )
            except Exception as osm_exc:
                log.error("[%s] overnight_reeval: OSM create_entry_order failed: %s", ticker, osm_exc)
                if _paper_rescue_only:
                    _mark_job_error(
                        job_id,
                        client_id,
                        _paper_rescue_queue_reason(
                            "order_materialization_failed",
                            f"create_entry_order:{type(osm_exc).__name__}",
                        ),
                    )
                result["errors"] += 1
                continue

            if not local_order_id:
                log.error("[%s] overnight_reeval: OSM returned no local_order_id for %s", ticker, signal_id)
                if _paper_rescue_only:
                    _mark_job_error(
                        job_id,
                        client_id,
                        "order_materialization_failed:missing_local_order_id",
                    )
                result["errors"] += 1
                continue

            # Step 6b: Mark order PENDING_TRIGGER so order_monitor does not
            # cancel it as a stale CREATED order before the trigger breach fires.
            # This mirrors the queue.py intraday path which does the same before arming.
            try:
                _current_status = ""
                if hasattr(order_state_machine, "get_order"):
                    _current = order_state_machine.get_order(local_order_id) or {}
                    _current_status = str((_current or {}).get("status") or "").upper()

                if _current_status == "PENDING_TRIGGER":
                    _pt_ok = True
                elif hasattr(order_state_machine, "mark_entry_pending_trigger"):
                    _pt_ok = order_state_machine.mark_entry_pending_trigger(local_order_id)
                else:
                    _pt_ok = order_state_machine.transition(
                        local_order_id,
                        "PENDING_TRIGGER",
                        submitted_ts=None,
                    )
                if not _pt_ok:
                    log.error(
                        "[%s] overnight_reeval: could not mark PENDING_TRIGGER for %s — skipping arm",
                        ticker,
                        local_order_id,
                    )
                    if _paper_rescue_only:
                        _mark_job_error(
                            job_id,
                            client_id,
                            "order_materialization_failed:pending_trigger_transition_false",
                        )
                    result["errors"] += 1
                    continue
            except Exception as _pt_exc:
                log.error(
                    "[%s] overnight_reeval: pending-trigger transition failed for %s: %s",
                    ticker,
                    local_order_id,
                    _pt_exc,
                )
                if _paper_rescue_only:
                    _mark_job_error(
                        job_id,
                        client_id,
                        f"order_materialization_failed:{type(_pt_exc).__name__}",
                    )
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
                    result["fresh_armed"] += 1
                else:
                    _reject_reason = str(
                        getattr(entry_watcher, "_last_reject_reason", None)
                        or "watch_returned_false"
                    )
                    _full_error = f"overnight_watch_arm_failed:{_reject_reason}"
                    log.error(
                        "[%s] overnight_reeval: entry_watcher.watch() returned False "
                        "| contract=%s reason=%s",
                        ticker, _arm_label, _reject_reason,
                    )
                    _cleanup_success, _cleanup_method = _cleanup_overnight_watch_arm_failure(
                        order_state_machine=order_state_machine,
                        client_id=client_id,
                        signal_id=signal_id,
                        ticker=ticker,
                        side=side,
                        local_order_id=str(local_order_id),
                        contract=_arm_label,
                        contract_deferred=contract_deferred,
                        entry_trigger=entry_trigger,
                        reason=_full_error,
                        done_event="OVERNIGHT_WATCH_ARM_FAILED_CLEANUP_DONE",
                    )
                    if not _cleanup_success:
                        _cleanup_failed_reason = _watch_arm_cleanup_failed_reason(_full_error)
                        log.critical(
                            "[%s] OVERNIGHT_WATCH_ARM_FAILED_CLEANUP_FAILED | "
                            "watch arm failed but local ENTRY cleanup failed "
                            "| local_order_id=%s cleanup_method=%s reason=%s "
                            "| cleanup_success=%s cleanup_failed_reason=%s",
                            ticker,
                            local_order_id,
                            _cleanup_method,
                            _full_error,
                            _cleanup_success,
                            _cleanup_failed_reason,
                        )
                        _record_watch_arm_failure_proof(
                            signal_id=signal_id,
                            client_id=client_id,
                            signal=signal,
                            reason=_cleanup_failed_reason,
                            local_order_id=str(local_order_id),
                            job_id=job_id,
                            is_exception=True,
                            session_key=session_key,
                            cleanup_method=_cleanup_method,
                            cleanup_success=False,
                            cleanup_failed=True,
                            original_reason=_full_error,
                        )
                        _mark_job_error(job_id, client_id, _cleanup_failed_reason)
                        result["errors"] += 1
                        continue
                    _record_watch_arm_failure_proof(
                        signal_id=signal_id,
                        client_id=client_id,
                        signal=signal,
                        reason=_full_error,
                        local_order_id=str(local_order_id),
                        job_id=job_id,
                        is_exception=False,
                        session_key=session_key,
                    )
                    _mark_job_rejected(job_id, client_id, _full_error)
                    if _lifecycle_ok:
                        try:
                            _sig_rejected(signal_id, ticker, _LO.WATCHER,
                                          "entry_watcher.watch() returned False",
                                          _RC.EXECUTION, "WATCHER_ARM_FAILED", _RS.WARNING,
                                          contract=_arm_label)
                        except Exception:
                            pass
                    result["rejected"] += 1
            except Exception as ew_exc:
                _full_error = f"overnight_watch_arm_failed:exception:{ew_exc}"
                _cleanup_success, _cleanup_method = _cleanup_overnight_watch_arm_failure(
                    order_state_machine=order_state_machine,
                    client_id=client_id,
                    signal_id=signal_id,
                    ticker=ticker,
                    side=side,
                    local_order_id=str(local_order_id),
                    contract=_arm_label,
                    contract_deferred=contract_deferred,
                    entry_trigger=entry_trigger,
                    reason=_full_error,
                    done_event="OVERNIGHT_WATCH_ARM_EXCEPTION_CLEANUP_DONE",
                )
                if not _cleanup_success:
                    _cleanup_failed_reason = _watch_arm_cleanup_failed_reason(_full_error)
                    log.critical(
                        "[%s] OVERNIGHT_WATCH_ARM_FAILED_CLEANUP_FAILED | "
                        "entry_watcher.watch exception and local ENTRY cleanup failed "
                        "| local_order_id=%s cleanup_method=%s reason=%s "
                        "| cleanup_success=%s cleanup_failed_reason=%s",
                        ticker,
                        local_order_id,
                        _cleanup_method,
                        _full_error,
                        _cleanup_success,
                        _cleanup_failed_reason,
                    )
                    _record_watch_arm_failure_proof(
                        signal_id=signal_id,
                        client_id=client_id,
                        signal=signal,
                        reason=_cleanup_failed_reason,
                        local_order_id=str(local_order_id),
                        job_id=job_id,
                        is_exception=True,
                        session_key=session_key,
                        cleanup_method=_cleanup_method,
                        cleanup_success=False,
                        cleanup_failed=True,
                        original_reason=_full_error,
                    )
                    _mark_job_error(job_id, client_id, _cleanup_failed_reason)
                    result["errors"] += 1
                    continue
                _record_watch_arm_failure_proof(
                    signal_id=signal_id,
                    client_id=client_id,
                    signal=signal,
                    reason=_full_error,
                    local_order_id=str(local_order_id),
                    job_id=job_id,
                    is_exception=True,
                    session_key=session_key,
                )
                _mark_job_error(job_id, client_id, _full_error)
                log.error("[%s] overnight_reeval: entry_watcher.watch failed: %s", ticker, ew_exc)
                result["errors"] += 1

        except Exception as outer_exc:
            log.error("[%s] overnight_reeval: unexpected error for %s: %s", ticker, signal_id, outer_exc, exc_info=True)
            if _paper_rescue_only:
                _mark_job_error(
                    job_id,
                    client_id,
                    _paper_rescue_queue_reason(
                        "overnight_reeval_internal_error",
                        type(outer_exc).__name__,
                    ),
                )
            result["errors"] += 1

    result["stale_inventory_only"] = bool(
        result["processed"] > 0
        and result["fresh_processed"] == 0
        and result["stale_skipped"] > 0
    )
    log.info(
        "[%s] overnight_reeval complete: processed=%d armed=%d rejected=%d skipped=%d errors=%d stale_skipped=%d fresh_processed=%d fresh_armed=%d stale_inventory_only=%s",
        client_id,
        result["processed"],
        result["armed"],
        result["rejected"],
        result["skipped"],
        result["errors"],
        result["stale_skipped"],
        result["fresh_processed"],
        result["fresh_armed"],
        result["stale_inventory_only"],
    )
    if result["stale_inventory_only"] and result["armed"] == 0:
        log.warning(
            "[%s] overnight_reeval stale inventory only: handoff/readiness must not imply fresh setup success | "
            "handoff_already_succeeded_for_stage_today may be expected downstream | fresh_armed=%d stale_inventory_only=%s",
            client_id,
            result["fresh_armed"],
            result["stale_inventory_only"],
        )
    # P0 (2026-07-02): a run that fetched work but produced NO decisions —
    # nothing armed, nothing rejected, no errors, everything deferred to
    # "next reeval" — is a stall, not a success. This is exactly the paper
    # signature when market data is unavailable: every row takes a
    # RETRY_LATER continue, the queue never drains, and the run reports
    # green. Flag it so callers persist status='partial' and operators see
    # the pipeline is wedged the same morning, not four sessions later.
    if (
        result["fetched"] > 0
        and result["armed"] == 0
        and result["rejected"] == 0
        and result["errors"] == 0
        and result.get("skipped", 0) and result["skipped"] > 0
    ):
        result["stalled"] = True
        log.error(
            "[%s] OVERNIGHT_REEVAL_STALLED fetched=%d skipped=%d armed=0 rejected=0 — "
            "every fetched signal deferred to next run; queue is NOT draining. "
            "category=SILENT_STALL severity=ERROR "
            "likely_cause=market_data_unavailable_or_all_rows_retry_later",
            client_id,
            result["fetched"],
            result["skipped"],
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

        # P0 (2026-07-02): backlog starvation fix. The hardcoded LIMIT 100
        # starved the queue when the paper WATCHING backlog exceeded 100 rows
        # per client (observed: 248–258/client). DESC ordering meant only the
        # newest 100 were ever fetched; anything older was never re-evaluated
        # and never expired — rows from 2026-06-22 were still WATCHING on
        # 2026-07-02. Env-tunable with a hard floor (never below the old 100)
        # and ceiling (bounded per-run work).
        try:
            _fetch_limit = int(os.getenv("OVERNIGHT_FETCH_LIMIT", "500"))
        except (TypeError, ValueError):
            _fetch_limit = 500
        _fetch_limit = max(100, min(_fetch_limit, 2000))

        def _fn():
            with conn() as c:
                c.execute("""
                    SELECT id, signal_id, payload, created_ts
                    FROM trade_queue
                    WHERE client_id = %s
                      AND status = 'WATCHING'
                    ORDER BY created_ts DESC
                    LIMIT %s
                """, (client_id, _fetch_limit))
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
            # Use SIGNALS_LOOKBACK (hours) when set; fall back to day-based
            _lookback_hours = _SIGNALS_LOOKBACK_HOURS
            if _lookback_hours and _lookback_hours > 0:
                cutoff = (
                    datetime.now(timezone.utc) - timedelta(hours=_lookback_hours)
                ).isoformat()
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


def _mark_job_error(job_id, client_id: str, reason: str) -> None:
    """Mark a WATCHING job errored in its original source table.

    Shared ap_signals rows are not mutated; the per-client proof lives in the
    opportunity ledger. trade_queue rows remain client-scoped and can be
    terminally updated.
    """
    job_id_str = str(job_id)
    if job_id_str.startswith("sup:"):
        log.info(
            "[%s] reeval errored shared setup %s — per-client error proof logged "
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
                    SET status = 'ERROR',
                        last_error = %s,
                        finished_ts = NOW()
                    WHERE id = %s AND client_id = %s
                """, (reason[:500], job_id, client_id))
        run_with_retry(_fn)
    except Exception as e:
        log.debug("_mark_job_error[trade_queue] failed non-fatal: %s", e)


def _mark_job_watching_reason(job_id, client_id: str, reason: str) -> None:
    """Keep WATCHING rows non-terminal but explicit when data is still pending."""
    job_id_str = str(job_id)
    if job_id_str.startswith("sup:"):
        log.info(
            "[%s] reeval waiting shared setup %s — shared row untouched: %s",
            client_id, job_id_str[4:], reason[:200],
        )
        return

    try:
        from ap.db import conn, run_with_retry

        def _fn():
            with conn() as c:
                c.execute("""
                    UPDATE trade_queue
                    SET status = 'WATCHING',
                        last_error = %s,
                        started_ts = COALESCE(started_ts, NOW()),
                        finished_ts = NULL
                    WHERE id = %s AND client_id = %s
                """, (reason[:500], job_id, client_id))
        run_with_retry(_fn)
    except Exception as e:
        log.debug("_mark_job_watching_reason[trade_queue] failed non-fatal: %s", e)


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
