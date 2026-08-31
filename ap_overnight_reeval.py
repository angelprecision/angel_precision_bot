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
     c. If VALID: create deferred OSM entry order and arm entry_watcher
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
from typing import TYPE_CHECKING, NamedTuple, Optional

from ap_signal_store import canonical_client_email, canonical_signal_id, upsert_ap_signal_row_with_fallback
from ap_entry_watcher import (
    RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN,
    recovery_trigger_evidence_identity_is_proven,
)

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


def _hydrate_plan_from_signal(signal, *, client_id=None, execution_mode=None):
    """Build a minimal TradePlan-compatible namespace from a WATCHING signal.
    Used when MC rejects on intel/second-score but recheck is disabled —
    the signal was already scanner-approved and has all the fields needed
    for contract deferment + watcher arming.

    P0-5: the plan MUST carry identity + risk fields the watcher reads
    directly (signal_id, canonical_signal_id, client_id, execution_mode,
    stop_underlying, target_underlying). Prior implementation dropped these,
    letting the watcher generate a random signal identity, a blank
    client_id, and no stop/target — silently manufacturing an identity-free,
    stop-free watcher when master_control returned an observe-only reject.
    """
    import types as _types
    _trig = float(signal.get("entry_trigger") or 0) or None
    def _coerce_float(v):
        try:
            f = float(v) if v is not None else None
            return f if f and f > 0 else None
        except (TypeError, ValueError):
            return None
    _stop   = _coerce_float(signal.get("stop_price"))
    _target = _coerce_float(signal.get("target_price"))
    _sid    = str(signal.get("signal_id") or "").strip()
    _canon  = str(
        signal.get("canonical_signal_id")
        or _resolve_canonical_signal_id(_sid, signal)
        or ""
    ).strip()
    _mode = str(execution_mode or "").strip().lower() or None
    return _types.SimpleNamespace(
        ticker              = signal.get("ticker") or signal.get("symbol"),
        side                = (signal.get("side") or "").upper(),
        direction           = (signal.get("side") or "").upper(),
        score               = float(signal.get("score") or 0),
        timeframe           = str(signal.get("timeframe") or "1d"),
        entry_trigger       = _trig,
        trigger_price       = _trig,
        trigger_type        = "breach",
        prior_day_high      = signal.get("prior_day_high"),
        prior_day_low       = signal.get("prior_day_low"),
        pattern             = signal.get("pattern"),
        tier                = signal.get("tier"),
        contract_symbol     = None,
        contracts           = None,
        limit_price         = None,
        # Identity + risk fields the watcher reads from the plan.
        signal_id           = _sid,
        canonical_signal_id = _canon,
        client_id           = client_id,
        execution_mode      = _mode,
        stop_underlying     = _stop,
        target_underlying   = _target,
        metadata            = {
            "overnight":             True,
            "hydrated_from_signal":  True,
            "second_score_mode":     "observe_only",
            "signal_id":             _sid,
            "canonical_signal_id":   _canon,
            "client_id":             client_id,
            "execution_mode":        _mode,
            "stop_underlying":       _stop,
            "target_underlying":     _target,
        },
    )


def _normalize_overnight_side(value) -> str:
    raw = str(value or "").strip().upper()
    if raw in {"CALL", "BUY", "LONG", "BULL", "BULLISH", "CALLS"}:
        return "CALL"
    if raw in {"PUT", "SELL", "SHORT", "BEAR", "BEARISH", "PUTS"}:
        return "PUT"
    return "UNKNOWN"


# SIGNALS_LOOKBACK: hours-based override. Only applied when explicitly set.
# Without the env var, the cutoff is computed session-aware (see below) so a
# Friday scanner setup remains visible on Monday morning without every
# deployment having to override the environment to survive the weekend.
def _signals_lookback_hours_env() -> Optional[int]:
    raw = os.getenv("SIGNALS_LOOKBACK")
    if raw is None or str(raw).strip() == "":
        return None
    try:
        v = int(str(raw).strip())
    except (TypeError, ValueError):
        return None
    return v if v > 0 else None


def _et_now() -> datetime:
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("America/New_York"))


def _is_trading_day(dt: datetime) -> bool:
    # P0 (monday-trade-flow-readiness): delegate to the canonical NYSE
    # calendar in ap.flatline_alarm (single source of truth per #282) instead
    # of weekday-only logic. On 2026-07-03 (Independence Day observed) the
    # weekday-only check let the overnight reeval run against dead holiday
    # quotes. Fail-safe: if the calendar import ever breaks, fall back to the
    # previous weekday-only behavior rather than blocking reeval entirely.
    try:
        from ap.flatline_alarm import is_trading_day as _nyse_is_trading_day
        return _nyse_is_trading_day(dt.date())
    except Exception:
        return dt.weekday() < 5  # legacy fallback: Mon-Fri


def _prior_trading_session_date(ref: Optional[datetime] = None) -> date:
    """Return the date of the prior *trading* session relative to ref (ET).

    Walks back at least one day and skips weekends AND NYSE full-closure
    holidays (via ap.flatline_alarm.is_trading_day, the canonical calendar),
    so on a Monday morning after a Friday holiday the prior trading session is
    the preceding Thursday. This is the session a cached prior-day high/low
    MUST match to be considered fresh. Fail-safe: on calendar import failure,
    weekend-only walk-back (previous behavior) — a holiday-stamped cache row
    then simply fails the equality guard and is treated as stale.
    """
    d = (ref or _et_now()).date()
    d = d - timedelta(days=1)
    try:
        from ap.flatline_alarm import is_trading_day as _nyse_is_trading_day
        _guard = 0
        while not _nyse_is_trading_day(d) and _guard < 14:
            d = d - timedelta(days=1)
            _guard += 1
        return d
    except Exception:
        while d.weekday() >= 5:  # Sat/Sun
            d = d - timedelta(days=1)
        return d


def _signals_lookback_cutoff_iso(now: Optional[datetime] = None) -> str:
    """Return the ISO-8601 UTC cutoff to use for the ap_signals/trade_queue
    created_at filter.

    Rules (in order):
      1. If SIGNALS_LOOKBACK is explicitly set to a positive integer,
         cutoff = now - SIGNALS_LOOKBACK hours (backwards-compat override).
      2. Otherwise cutoff = start (00:00 ET) of the PRIOR TRADING SESSION.
         On Monday morning this reaches back to Friday 00:00 ET so a Friday
         scanner setup remains visible through the entire Monday morning
         reeval window. Skips weekends and NYSE full-closure holidays via
         the canonical calendar in _prior_trading_session_date.

    The prior default of 18 hours defeats the exact Friday-to-Monday recovery
    this PR was written to guarantee; that is why the default is now session-
    aware. The env var remains honored so operators can pin an explicit
    window for A/B experiments — but correctness does not depend on it.
    """
    _now = now or datetime.now(timezone.utc)
    hours = _signals_lookback_hours_env()
    if hours is not None:
        return (_now - timedelta(hours=hours)).isoformat()
    # Session-aware default. Compute prior-trading-session date in ET, then
    # anchor cutoff at 00:00 ET of that date and convert to UTC.
    try:
        from zoneinfo import ZoneInfo as _ZI
        _et = _ZI("America/New_York")
        prior = _prior_trading_session_date(_now.astimezone(_et))
        prior_start_et = datetime(prior.year, prior.month, prior.day, 0, 0, 0, tzinfo=_et)
        return prior_start_et.astimezone(timezone.utc).isoformat()
    except Exception:
        # Fail safe: 96h (Friday-to-Tuesday round-trip covers every weekend
        # + a Monday holiday). Never fall back to the old 18h default.
        return (_now - timedelta(hours=96)).isoformat()


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


def _overnight_signal_sort_key(job: dict) -> tuple:
    """Tier A, score, recency, then stable row identity."""
    signal = job.get("payload") or {}
    if isinstance(signal, str):
        try:
            import json as _json
            signal = _json.loads(signal)
        except Exception:
            signal = {}
    tier = str(signal.get("tier") or "").strip().upper()
    tier_rank = {"A": 0, "B": 1}.get(tier, 2)
    try:
        score_rank = -float(signal.get("score") or 0)
    except (TypeError, ValueError):
        score_rank = 0.0
    created_raw = (
        signal.get("created_at")
        or signal.get("timestamp_iso")
        or signal.get("signal_date")
        or job.get("created_ts")
        or ""
    )
    try:
        created = datetime.fromisoformat(str(created_raw).replace("Z", "+00:00"))
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        created_rank = -created.timestamp()
    except Exception:
        created_rank = 0.0
    return tier_rank, score_rank, created_rank, str(job.get("id") or job.get("signal_id") or "")


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
        # PR #388: durable REATTACH ambiguity marker (see
        # _persist_reattach_post_watch_ambiguity). Written when watch()
        # returns False AND the post-watch verify cannot prove the order
        # is preserved — treat as terminal on the next reeval so no
        # replacement ENTRY is ever created.
        or reason.startswith("reattach_post_watch_")
    )


# ── Lookup result ─────────────────────────────────────────────────────────────

class _LookupResult(NamedTuple):
    """Explicit result from _get_client_opportunity_row.

    lookup_status values:
      FOUND        — a valid matching row was returned in .row
      NOT_FOUND    — query succeeded; definitively zero matching rows
      LOOKUP_FAILED — truth could not be established (Supabase unavailable,
                      exception, malformed response, missing identity, etc.)
    """
    canonical_signal_id: str
    lookup_status: str   # "FOUND" | "NOT_FOUND" | "LOOKUP_FAILED"
    row: Optional[dict]
    error: Optional[str]


_LS_FOUND         = "FOUND"
_LS_NOT_FOUND     = "NOT_FOUND"
_LS_LOOKUP_FAILED = "LOOKUP_FAILED"


def _get_client_opportunity_row(signal_id: str, client_id: str, signal: dict) -> _LookupResult:
    """Query the opportunity ledger for this client/canonical-signal pair.

    Returns an explicit _LookupResult — FOUND, NOT_FOUND, or LOOKUP_FAILED.
    LOOKUP_FAILED is NEVER collapsed into NOT_FOUND: a backing-store outage
    must not allow a second ENTRY order to be created.
    """
    canonical = _resolve_canonical_signal_id(signal_id, signal)
    if not canonical or not client_id:
        return _LookupResult(
            canonical or "",
            _LS_LOOKUP_FAILED,
            None,
            "missing_canonical_or_client_id",
        )

    try:
        from ap.opportunity_ledger import _get_sb
        sb = _get_sb()
        if not sb:
            return _LookupResult(canonical, _LS_LOOKUP_FAILED, None, "supabase_client_unavailable")

        res = (
            sb.table("client_signal_opportunities")
            .select(
                "canonical_signal_id, client_id, opportunity_status, miss_stage, "
                "miss_reason, metadata, order_local_id"
            )
            .eq("canonical_signal_id", canonical)
            .eq("client_id", client_id)
            .limit(1)
            .execute()
        )
        rows = getattr(res, "data", None)
        if rows is None:
            # Malformed response — cannot distinguish zero rows from error.
            return _LookupResult(canonical, _LS_LOOKUP_FAILED, None, "malformed_response_data_none")

        if not rows:
            return _LookupResult(canonical, _LS_NOT_FOUND, None, None)

        row = rows[0] if isinstance(rows[0], dict) else dict(rows[0])
        return _LookupResult(canonical, _LS_FOUND, row, None)

    except Exception as exc:
        log.debug(
            "[%s] overnight_reeval: opportunity lookup failed canonical=%s: %s",
            client_id, canonical, exc,
        )
        return _LookupResult(canonical, _LS_LOOKUP_FAILED, None, str(exc))


# ── Active-order statuses that prove entry ownership ──────────────────────────
# PENDING_TRIGGER / SUBMITTED / ACKNOWLEDGED / PARTIAL_FILL → order exists and
# may need a watcher reattached (Cases A/B) or is in-flight (Case C).
# FILLED → already entered; must not enter again.
# PR #388 amendment: full nonterminal ENTRY family. Prior amendment only
# recognized PENDING_TRIGGER/SUBMITTED/ACKNOWLEDGED/PARTIAL_FILL/FILLED,
# which meant CREATED/ACCEPTED/OPEN/PARTIALLY_FILLED slipped through the
# active-order fence and produced duplicate create_entry_order calls on
# retry. The set matches the repository's existing durable duplicate logic.
_ACTIVE_ENTRY_OWN_STATUSES = frozenset({
    "CREATED",
    "PENDING_TRIGGER",
    "SUBMITTED",
    "ACCEPTED",
    "ACKNOWLEDGED",
    "OPEN",
    "PARTIAL_FILL",
    "PARTIALLY_FILLED",
    "FILLED",
})

# Statuses that mean the order is in flight / done (never re-create).
_ALREADY_OWNED_STATUSES = frozenset({
    "SUBMITTED", "ACCEPTED", "ACKNOWLEDGED", "OPEN",
    "PARTIAL_FILL", "PARTIALLY_FILLED", "FILLED",
})


def _query_active_entry_order(
    client_id: str,
    execution_mode: str,
    canonical_sig_id: str,
) -> tuple[str, Optional[dict]]:
    """Query for an exact active ENTRY order for this client/mode/canonical-signal.

    Uses direct DB query with exact 5-tuple identity:
      client_id + execution_mode (exact, lower-normalized) + canonical_signal_id
      + kind='ENTRY' + status in _ACTIVE_ENTRY_OWN_STATUSES.

    Returns:
      (_LS_FOUND,         row)  — active order exists; row is a dict
      (_LS_NOT_FOUND,    None)  — definitively no matching order
      (_LS_LOOKUP_FAILED, None) — query failed; truth unknown
    """
    if not client_id or not canonical_sig_id:
        return _LS_LOOKUP_FAILED, None

    mode = str(execution_mode or "").strip().lower()
    if mode not in {"live", "paper"}:
        log.warning(
            "[%s] _query_active_entry_order: unknown execution_mode=%r canonical=%s — LOOKUP_FAILED",
            client_id, execution_mode, canonical_sig_id,
        )
        return _LS_LOOKUP_FAILED, None

    statuses = tuple(_ACTIVE_ENTRY_OWN_STATUSES)
    placeholders = ", ".join(["%s"] * len(statuses))

    try:
        from ap.db import conn, run_with_retry

        def _fn():
            with conn() as c:
                c.execute(
                    f"SELECT * FROM orders "
                    f"WHERE client_id = %s "
                    f"AND kind = 'ENTRY' "
                    f"AND LOWER(TRIM(COALESCE(execution_mode, ''))) = %s "
                    f"AND canonical_signal_id = %s "
                    f"AND status IN ({placeholders}) "
                    f"ORDER BY created_ts DESC LIMIT 1",
                    (client_id, mode, canonical_sig_id, *statuses),
                )
                return c.fetchone()

        row = run_with_retry(_fn)
        if row is None:
            return _LS_NOT_FOUND, None
        return _LS_FOUND, (dict(row) if not isinstance(row, dict) else row)

    except Exception as exc:
        log.warning(
            "[%s] _query_active_entry_order: DB query failed canonical=%s mode=%s: %s",
            client_id, canonical_sig_id, mode, exc,
        )
        return _LS_LOOKUP_FAILED, None


def _query_exact_entry_order_by_local_id(
    local_order_id: str,
    client_id: str,
    execution_mode: str,
    canonical_signal_id: str,
) -> tuple[str, Optional[dict]]:
    """Post-watch exact-order lookup with NO status filter.

    _query_active_entry_order() is the active-ownership fence and filters
    to _ACTIVE_ENTRY_OWN_STATUSES — it CANNOT see CANCELED / EXPIRED /
    REJECTED / ERROR rows. That is correct for the fence, but a REATTACH
    watch() that terminalizes the order needs a lookup that CAN see the
    terminal state so the caller can classify already_resolved rather
    than incorrectly labeling retryable and enabling a replacement.

    This helper queries by exact 4-tuple identity with NO status filter:
      local_order_id + client_id + execution_mode + canonical_signal_id + kind='ENTRY'.

    Returns (_LS_FOUND, row) / (_LS_NOT_FOUND, None) / (_LS_LOOKUP_FAILED, None).
    """
    if not local_order_id or not client_id or not canonical_signal_id:
        return _LS_LOOKUP_FAILED, None
    mode = str(execution_mode or "").strip().lower()
    if mode not in {"live", "paper"}:
        log.warning(
            "[%s] _query_exact_entry_order_by_local_id: unknown mode=%r local_order_id=%s"
            " — LOOKUP_FAILED",
            client_id, execution_mode, local_order_id,
        )
        return _LS_LOOKUP_FAILED, None
    try:
        from ap.db import conn, run_with_retry

        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT * FROM orders "
                    "WHERE local_order_id = %s "
                    "AND client_id = %s "
                    "AND LOWER(TRIM(COALESCE(execution_mode, ''))) = %s "
                    "AND canonical_signal_id = %s "
                    "AND kind = 'ENTRY' "
                    "LIMIT 1",
                    (local_order_id, client_id, mode, canonical_signal_id),
                )
                return c.fetchone()

        row = run_with_retry(_fn)
        if row is None:
            return _LS_NOT_FOUND, None
        return _LS_FOUND, (dict(row) if not isinstance(row, dict) else row)

    except Exception as exc:
        log.warning(
            "[%s] _query_exact_entry_order_by_local_id: DB query failed "
            "local_order_id=%s canonical=%s mode=%s: %s",
            client_id, local_order_id, canonical_signal_id, mode, exc,
        )
        return _LS_LOOKUP_FAILED, None


def _query_latest_entry_order_no_status(
    client_id: str,
    execution_mode: str,
    canonical_signal_id: str,
) -> tuple[str, Optional[dict]]:
    """Resolver-level latest ENTRY fence with no status filter.

    This is deliberately separate from _query_active_entry_order(), which
    remains the active ownership fence. This helper is the final resurrection
    guard before NEW: if an exact terminal ENTRY exists for this
    client/mode/canonical, a retry must not create a replacement just because
    the active-only fence cannot see terminal statuses.
    """
    if not client_id or not canonical_signal_id:
        return _LS_LOOKUP_FAILED, None
    mode = str(execution_mode or "").strip().lower()
    if mode not in {"live", "paper"}:
        log.warning(
            "[%s] _query_latest_entry_order_no_status: unknown mode=%r canonical=%s"
            " — LOOKUP_FAILED",
            client_id, execution_mode, canonical_signal_id,
        )
        return _LS_LOOKUP_FAILED, None
    try:
        from ap.db import conn, run_with_retry

        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT * FROM orders "
                    "WHERE client_id = %s "
                    "AND LOWER(TRIM(COALESCE(execution_mode, ''))) = %s "
                    "AND canonical_signal_id = %s "
                    "AND kind = 'ENTRY' "
                    "ORDER BY created_ts DESC LIMIT 1",
                    (client_id, mode, canonical_signal_id),
                )
                return c.fetchone()

        row = run_with_retry(_fn)
        if row is None:
            return _LS_NOT_FOUND, None
        return _LS_FOUND, (dict(row) if not isinstance(row, dict) else row)

    except Exception as exc:
        log.warning(
            "[%s] _query_latest_entry_order_no_status: DB query failed "
            "canonical=%s mode=%s: %s",
            client_id, canonical_signal_id, mode, exc,
        )
        return _LS_LOOKUP_FAILED, None


# Terminal ENTRY statuses the post-watch verifier recognises. Anything not in
# _ACTIVE_ENTRY_OWN_STATUSES and not in this set is treated as ambiguous.
_TERMINAL_ENTRY_STATUSES = frozenset({
    "CANCELED", "CANCELLED", "EXPIRED", "REJECTED", "ERROR", "FAILED",
    "TERMINAL", "CLOSED",
})


def _entry_order_disposition_from_status(
    *,
    client_id: str,
    canonical_signal_id: str,
    session_key: str,
    status: str,
    local_order_id: str,
    order_row: Optional[dict] = None,
    source: str,
) -> Optional[_DispositionResult]:
    _status = str(status or "").upper()
    _local_id = str(local_order_id or "").strip()
    if _status == "PENDING_TRIGGER":
        log.info(
            "[%s] reeval disposition=REATTACH_WATCHER canonical=%s session=%s "
            "local_order_id=%s source=%s",
            client_id, canonical_signal_id, session_key, _local_id, source,
        )
        return _DispositionResult(_DISPOSITION_REATTACH_WATCHER, _local_id, order_row)
    if _status == "CREATED":
        log.info(
            "[%s] reeval disposition=ACTIVE_CREATED_RETRY canonical=%s session=%s "
            "local_order_id=%s source=%s",
            client_id, canonical_signal_id, session_key, _local_id, source,
        )
        return _DispositionResult(_DISPOSITION_ACTIVE_CREATED_RETRY, _local_id, order_row)
    if _status in _ALREADY_OWNED_STATUSES:
        log.info(
            "[%s] reeval disposition=ALREADY_OWNED canonical=%s session=%s "
            "order_status=%s source=%s",
            client_id, canonical_signal_id, session_key, _status, source,
        )
        return _DispositionResult(_DISPOSITION_ALREADY_OWNED)
    if _status in _TERMINAL_ENTRY_STATUSES:
        log.info(
            "[%s] reeval disposition=ALREADY_TERMINAL (latest-entry-order) "
            "canonical=%s session=%s order_status=%s local_order_id=%s source=%s",
            client_id, canonical_signal_id, session_key, _status, _local_id, source,
        )
        return _DispositionResult(_DISPOSITION_ALREADY_TERMINAL)
    return None


def _persist_reattach_in_progress_fence(
    *,
    client_id: str,
    execution_mode: str,
    canonical_signal_id: str,
    signal_id: str,
    signal_payload: dict,
    local_order_id: str,
    session_key: str,
) -> bool:
    """Persist and verify current-session REATTACH ownership before watch().

    If watch() later terminalizes the order but the terminal suppression marker
    fails, this verified metadata keeps the next resolver attempt out of NEW.
    """
    _mode = str(execution_mode or "").strip().lower()
    _session = str(session_key or _overnight_reeval_session_key()).strip()
    if not (
        client_id and _mode in {"live", "paper"} and canonical_signal_id
        and signal_id and local_order_id and _session
    ):
        log.critical(
            "REATTACH_IN_PROGRESS_FENCE_INVALID_INPUT client=%s mode=%s "
            "canonical=%s signal=%s local_order_id=%s session=%s",
            client_id, execution_mode, canonical_signal_id,
            signal_id, local_order_id, session_key,
        )
        return False
    try:
        from ap.opportunity_ledger import (
            CREATED as _OL_CREATED,
            create_opportunities as _create_opps,
            update_opportunity as _update_opportunity,
        )

        _sig_payload = dict(signal_payload or {})
        _sig_payload.setdefault("signal_id", signal_id)
        _created = _create_opps(
            signal_id, [client_id], _sig_payload,
            canonical_signal_id=canonical_signal_id,
        )
        if int(_created or 0) < 1:
            log.critical(
                "REATTACH_IN_PROGRESS_FENCE_CREATE_FAILED client=%s mode=%s "
                "canonical=%s local_order_id=%s",
                client_id, _mode, canonical_signal_id, local_order_id,
            )
            return False

        _fence_meta = {
            "reattach_in_progress": True,
            "execution_mode": _mode,
            "overnight_reeval_session_key": _session,
            "source_table": "ap_signals",
            "original_signal_id": signal_id,
            "canonical_signal_id": canonical_signal_id,
            "local_order_id": str(local_order_id),
            "reattach_fenced_at": datetime.now(timezone.utc).isoformat(),
        }
        _updated = _update_opportunity(
            signal_id,
            client_id,
            _OL_CREATED,
            canonical_signal_id=canonical_signal_id,
            order_local_id=str(local_order_id),
            extra_meta=_fence_meta,
        )
        if not _updated:
            log.critical(
                "REATTACH_IN_PROGRESS_FENCE_UPDATE_FAILED client=%s mode=%s "
                "canonical=%s local_order_id=%s",
                client_id, _mode, canonical_signal_id, local_order_id,
            )
            return False

        _readback = _get_client_opportunity_row(signal_id, client_id, _sig_payload)
        if _readback.lookup_status != _LS_FOUND or not isinstance(_readback.row, dict):
            log.critical(
                "REATTACH_IN_PROGRESS_FENCE_READBACK_MISSING client=%s mode=%s "
                "canonical=%s local_order_id=%s status=%s err=%s",
                client_id, _mode, canonical_signal_id, local_order_id,
                _readback.lookup_status, _readback.error,
            )
            return False

        _row = _readback.row
        _meta = _row.get("metadata") if isinstance(_row.get("metadata"), dict) else {}
        _verified = (
            str(_row.get("canonical_signal_id") or _readback.canonical_signal_id or "") == canonical_signal_id
            and str(_row.get("client_id") or client_id) == client_id
            and str(_row.get("order_local_id") or _meta.get("local_order_id") or "") == str(local_order_id)
            and str(_meta.get("execution_mode") or _meta.get("mode") or "").strip().lower() == _mode
            and str(_meta.get("overnight_reeval_session_key") or "").strip() == _session
            and bool(_meta.get("reattach_in_progress")) is True
        )
        if not _verified:
            log.critical(
                "REATTACH_IN_PROGRESS_FENCE_READBACK_MISMATCH client=%s mode=%s "
                "canonical=%s local_order_id=%s row=%s metadata=%s",
                client_id, _mode, canonical_signal_id, local_order_id, _row, _meta,
            )
            return False
        return True
    except Exception as _fence_exc:
        log.critical(
            "REATTACH_IN_PROGRESS_FENCE_EXCEPTION client=%s mode=%s canonical=%s "
            "local_order_id=%s err=%s",
            client_id, execution_mode, canonical_signal_id,
            local_order_id, _fence_exc,
        )
        return False


def _persist_reattach_terminal_suppression_marker(
    *,
    client_id: str,
    execution_mode: str,
    canonical_signal_id: str,
    signal_id: str,
    signal_payload: dict,
    local_order_id: str,
    session_key: str,
    reason: str,
    post_status: str,
    post_row_status: str,
) -> bool:
    """Persist and verify a durable terminal marker for a REATTACH outcome.

    The caller may have proved the exact order is terminal, or may be unable
    to prove whether watch() terminalized it. In both cases, a retry must not
    classify the setup as NEW just because the active-order fence no longer
    sees a nonterminal ENTRY row.
    """
    _mode = str(execution_mode or "").strip().lower()
    _session = str(session_key or _overnight_reeval_session_key()).strip()
    _reason = str(reason or "reattach_post_watch_unknown").strip()
    if not (
        client_id and _mode in {"live", "paper"} and canonical_signal_id
        and signal_id and local_order_id and _session
    ):
        log.critical(
            "REATTACH_TERMINAL_SUPPRESSION_MARKER_INVALID_INPUT "
            "client=%s mode=%s canonical=%s signal=%s local_order_id=%s session=%s",
            client_id, execution_mode, canonical_signal_id,
            signal_id, local_order_id, session_key,
        )
        return False

    try:
        from ap.opportunity_ledger import (
            create_opportunities as _create_opps,
            mark_watcher_invalidated as _mark_watcher_invalidated,
        )

        _sig_payload = dict(signal_payload or {})
        _sig_payload.setdefault("signal_id", signal_id)
        _created = _create_opps(
            signal_id, [client_id], _sig_payload,
            canonical_signal_id=canonical_signal_id,
        )
        if int(_created or 0) < 1:
            log.critical(
                "REATTACH_TERMINAL_SUPPRESSION_CREATE_FAILED "
                "client=%s mode=%s canonical=%s local_order_id=%s reason=%s",
                client_id, _mode, canonical_signal_id, local_order_id, _reason,
            )
            return False

        _marker_meta = {
            "reattach_terminal_suppression": True,
            "execution_mode": _mode,
            "overnight_reeval_session_key": _session,
            "source_table": "ap_signals",
            "original_signal_id": signal_id,
            "canonical_signal_id": canonical_signal_id,
            "local_order_id": str(local_order_id),
            "reattach_post_watch_status": str(post_status or ""),
            "reattach_post_watch_row_status": str(post_row_status or ""),
            "reattach_terminal_suppressed_at": datetime.now(timezone.utc).isoformat(),
        }
        _marked = _mark_watcher_invalidated(
            signal_id,
            client_id,
            _reason,
            canonical_signal_id=canonical_signal_id,
            order_local_id=str(local_order_id),
            extra_meta=_marker_meta,
        )
        if not _marked:
            log.critical(
                "REATTACH_TERMINAL_SUPPRESSION_MARK_FAILED "
                "client=%s mode=%s canonical=%s local_order_id=%s reason=%s",
                client_id, _mode, canonical_signal_id, local_order_id, _reason,
            )
            return False

        _readback = _get_client_opportunity_row(signal_id, client_id, _sig_payload)
        if _readback.lookup_status != _LS_FOUND or not isinstance(_readback.row, dict):
            log.critical(
                "REATTACH_TERMINAL_SUPPRESSION_READBACK_MISSING "
                "client=%s mode=%s canonical=%s local_order_id=%s status=%s err=%s",
                client_id, _mode, canonical_signal_id, local_order_id,
                _readback.lookup_status, _readback.error,
            )
            return False

        _row = _readback.row
        _meta = _row.get("metadata") if isinstance(_row.get("metadata"), dict) else {}
        _row_canonical = str(_row.get("canonical_signal_id") or _readback.canonical_signal_id or "")
        _row_client = str(_row.get("client_id") or client_id)
        _row_status = str(_row.get("opportunity_status") or "").upper()
        _row_stage = str(_row.get("miss_stage") or "").upper()
        _row_reason = str(_row.get("miss_reason") or "")
        _row_order = str(_row.get("order_local_id") or _meta.get("local_order_id") or "")
        _row_mode = str(_meta.get("execution_mode") or _meta.get("mode") or "").strip().lower()
        _row_session = str(_meta.get("overnight_reeval_session_key") or "").strip()

        _verified = (
            _row_canonical == canonical_signal_id
            and _row_client == client_id
            and _row_status == "MISSED"
            and _row_stage == "WATCHER_ARM"
            and _row_reason == _reason
            and _row_mode == _mode
            and _row_session == _session
            and _row_order == str(local_order_id)
        )
        if not _verified:
            log.critical(
                "REATTACH_TERMINAL_SUPPRESSION_READBACK_MISMATCH "
                "client=%s mode=%s canonical=%s local_order_id=%s "
                "row_canonical=%s row_client=%s row_status=%s row_stage=%s "
                "row_reason=%s row_mode=%s row_session=%s row_order=%s",
                client_id, _mode, canonical_signal_id, local_order_id,
                _row_canonical, _row_client, _row_status, _row_stage,
                _row_reason, _row_mode, _row_session, _row_order,
            )
            return False

        return True
    except Exception as _marker_exc:
        log.critical(
            "REATTACH_TERMINAL_SUPPRESSION_MARKER_EXCEPTION "
            "client=%s mode=%s canonical=%s local_order_id=%s reason=%s err=%s",
            client_id, execution_mode, canonical_signal_id,
            local_order_id, _reason, _marker_exc,
        )
        return False


def _persist_watcher_armed_proof(
    *,
    client_id: str,
    execution_mode: str,
    canonical_signal_id: str,
    signal_id: str,
    signal_payload: dict,
    local_order_id: str,
    session_key: str,
    extra_meta: Optional[dict] = None,
    clear_reattach_in_progress: bool = False,
) -> bool:
    """Persist WATCHER_ARMED and verify the row actually transitioned.

    opportunity_ledger.update_opportunity() returns True when the update
    query succeeds, even if its monotonic guard preserved a prior terminal
    status and only merged metadata. This helper treats the write as durable
    proof only after readback shows WATCHER_ARMED for the exact
    client/mode/session/order.
    """
    _mode = str(execution_mode or "").strip().lower()
    _session = str(session_key or _overnight_reeval_session_key()).strip()
    if not (
        client_id and _mode in {"live", "paper"} and canonical_signal_id
        and signal_id and local_order_id and _session
    ):
        log.critical(
            "WATCHER_ARMED_PROOF_INVALID_INPUT client=%s mode=%s canonical=%s "
            "signal=%s local_order_id=%s session=%s",
            client_id, execution_mode, canonical_signal_id,
            signal_id, local_order_id, session_key,
        )
        return False

    try:
        from ap.opportunity_ledger import (
            WATCHER_ARMED as _OL_WATCHER_ARMED,
            create_opportunities as _create_opps,
            mark_watcher_armed as _mark_watcher_armed,
        )

        _sig_payload = dict(signal_payload or {})
        _sig_payload.setdefault("signal_id", signal_id)
        _created = _create_opps(
            signal_id, [client_id], _sig_payload,
            canonical_signal_id=canonical_signal_id,
        )
        if int(_created or 0) < 1:
            log.critical(
                "WATCHER_ARMED_PROOF_CREATE_FAILED client=%s mode=%s "
                "canonical=%s local_order_id=%s",
                client_id, _mode, canonical_signal_id, local_order_id,
            )
            return False

        _proof_meta = dict(extra_meta or {})
        _proof_meta.update({
            "overnight_reeval_session_key": _session,
            "execution_mode": _mode,
            "original_signal_id": signal_id,
            "canonical_signal_id": canonical_signal_id,
            "local_order_id": str(local_order_id),
        })
        _marked = _mark_watcher_armed(
            canonical_signal_id,
            client_id,
            canonical_signal_id=canonical_signal_id,
            order_local_id=str(local_order_id),
            extra_meta=_proof_meta,
        )
        if not _marked:
            log.critical(
                "WATCHER_ARMED_PROOF_MARK_FAILED client=%s mode=%s "
                "canonical=%s local_order_id=%s",
                client_id, _mode, canonical_signal_id, local_order_id,
            )
            return False

        def _readback_verified(*, expect_reattach_cleared: bool) -> bool:
            _readback = _get_client_opportunity_row(signal_id, client_id, _sig_payload)
            if _readback.lookup_status != _LS_FOUND or not isinstance(_readback.row, dict):
                log.critical(
                    "WATCHER_ARMED_PROOF_READBACK_MISSING client=%s mode=%s "
                    "canonical=%s local_order_id=%s status=%s err=%s",
                    client_id, _mode, canonical_signal_id, local_order_id,
                    _readback.lookup_status, _readback.error,
                )
                return False

            _row = _readback.row
            _meta = _row.get("metadata") if isinstance(_row.get("metadata"), dict) else {}
            _row_canonical = str(_row.get("canonical_signal_id") or _readback.canonical_signal_id or "")
            _row_client = str(_row.get("client_id") or client_id)
            _row_status = str(_row.get("opportunity_status") or "").upper()
            _row_order = str(_row.get("order_local_id") or _meta.get("local_order_id") or "")
            _row_mode = str(_meta.get("execution_mode") or _meta.get("mode") or "").strip().lower()
            _row_session = str(_meta.get("overnight_reeval_session_key") or "").strip()
            _reattach_flag = bool(_meta.get("reattach_in_progress"))
            _verified = (
                _row_canonical == canonical_signal_id
                and _row_client == client_id
                and _row_status == _OL_WATCHER_ARMED
                and _row_order == str(local_order_id)
                and _row_mode == _mode
                and _row_session == _session
                and (not expect_reattach_cleared or not _reattach_flag)
            )
            if not _verified:
                log.critical(
                    "WATCHER_ARMED_PROOF_READBACK_MISMATCH client=%s mode=%s "
                    "canonical=%s local_order_id=%s row_status=%s row_order=%s "
                    "row_mode=%s row_session=%s reattach_in_progress=%s row=%s metadata=%s",
                    client_id, _mode, canonical_signal_id, local_order_id,
                    _row_status, _row_order, _row_mode, _row_session,
                    _reattach_flag, _row, _meta,
                )
                return False
            return True

        if not _readback_verified(expect_reattach_cleared=False):
            return False

        if clear_reattach_in_progress:
            _clear_meta = dict(_proof_meta)
            _clear_meta.update({
                "reattach_in_progress": False,
                "reattach_in_progress_cleared_at": datetime.now(timezone.utc).isoformat(),
            })
            _cleared = _mark_watcher_armed(
                canonical_signal_id,
                client_id,
                canonical_signal_id=canonical_signal_id,
                order_local_id=str(local_order_id),
                extra_meta=_clear_meta,
            )
            if not _cleared:
                log.critical(
                    "WATCHER_ARMED_PROOF_CLEAR_FAILED client=%s mode=%s "
                    "canonical=%s local_order_id=%s",
                    client_id, _mode, canonical_signal_id, local_order_id,
                )
                return False
            return _readback_verified(expect_reattach_cleared=True)

        return True
    except Exception as _proof_exc:
        log.critical(
            "WATCHER_ARMED_PROOF_EXCEPTION client=%s mode=%s canonical=%s "
            "local_order_id=%s err=%s",
            client_id, execution_mode, canonical_signal_id,
            local_order_id, _proof_exc,
        )
        return False


def _persist_reattach_post_watch_ambiguity(
    *,
    client_id: str,
    execution_mode: str,
    canonical_signal_id: str,
    signal_id: str,
    signal_payload: dict,
    local_order_id: str,
    session_key: str,
    post_status: str,
    post_row_status: str,
) -> bool:
    """Persist a durable AMBIGUOUS opportunity so the NEXT reeval attempt
    cannot classify the setup as NEW and create a replacement ENTRY.

    Returns True only after create, terminal mark, and read-back verification.
    A False return is a hard failure that MUST be treated as fail-closed
    retryable by the caller.
    """
    return _persist_reattach_terminal_suppression_marker(
        client_id=client_id,
        execution_mode=execution_mode,
        canonical_signal_id=canonical_signal_id,
        signal_id=signal_id,
        signal_payload=signal_payload,
        local_order_id=local_order_id,
        session_key=session_key,
        reason="reattach_post_watch_ambiguous_terminal_or_missing",
        post_status=post_status,
        post_row_status=post_row_status,
    )


# ── Disposition constants ─────────────────────────────────────────────────────

_DISPOSITION_ALREADY_ARMED       = "ALREADY_ARMED"
_DISPOSITION_ALREADY_OWNED       = "ALREADY_OWNED"       # broker-submitted / filled
_DISPOSITION_ALREADY_TERMINAL    = "ALREADY_TERMINAL"
_DISPOSITION_REATTACH_WATCHER    = "REATTACH_WATCHER"    # PENDING_TRIGGER order exists, no watcher proof
_DISPOSITION_ACTIVE_CREATED_RETRY = "ACTIVE_CREATED_RETRY"  # exact CREATED order — retry, never re-enter
_DISPOSITION_RETRYABLE           = "RETRYABLE"
_DISPOSITION_NEW                 = "NEW"
_DISPOSITION_LOOKUP_FAILED       = "LOOKUP_FAILED"
_DISPOSITION_AMBIGUOUS_OWNERSHIP = "AMBIGUOUS_OWNERSHIP"  # fail closed as retryable


class _DispositionResult(NamedTuple):
    """Full disposition result for a shared ap_signals row.

    .disposition            — one of the _DISPOSITION_* constants
    .existing_local_order_id — set for REATTACH_WATCHER; the order to reuse
    .existing_order_row     — set for REATTACH_WATCHER; full order dict
    """
    disposition: str
    existing_local_order_id: Optional[str] = None
    existing_order_row: Optional[dict] = None


def _resolve_shared_setup_disposition(
    signal_id: str,
    client_id: str,
    signal: dict,
    execution_mode: str,
    *,
    session_key: Optional[str] = None,
) -> _DispositionResult:
    """Return the per-client disposition for a shared ap_signals row.

    Called ONLY for rows whose source is ap_signals (job_id starts 'sup:').
    Must run before master_control, OSM, selector, or broker calls.

    Processing order (10 steps per spec):
      1. Normalize canonical signal identity.
      2. Normalize exact execution mode.
      3. Determine overnight session key.
      4. Query opportunity ledger → FOUND / NOT_FOUND / LOOKUP_FAILED.
      5. If FOUND: inspect durable evidence (mode, session, status).
      6. Query exact active local ENTRY ownership.
      7. Resolve disposition from active order if found.
      8. Resolve NEW vs RETRYABLE from opportunity evidence.
      9. Ambiguous → fail closed.
     10. Return _DispositionResult.

    Mode isolation: blank stored mode never establishes ownership.
    Session isolation: blank stored session never counts as current session.
    Terminal coverage: uses canonical sets imported from opportunity_ledger.
    """
    from ap.opportunity_ledger import (
        WATCHER_ARMED            as _OL_WATCHER_ARMED,
        BROKER_SUBMITTED         as _OL_BROKER_SUBMITTED,
        BROKER_ACKED             as _OL_BROKER_ACKED,
        FILLED                   as _OL_FILLED,
        TERMINAL_STATUSES        as _OL_TERMINAL_STATUSES,
    )

    # Step 1: Normalize canonical signal identity.
    lookup = _get_client_opportunity_row(signal_id, client_id, signal)
    canonical = lookup.canonical_signal_id

    if not canonical:
        log.warning(
            "[%s] reeval disposition=LOOKUP_FAILED cannot resolve canonical_signal_id signal=%s",
            client_id, signal_id,
        )
        return _DispositionResult(_DISPOSITION_LOOKUP_FAILED)

    # Step 2: Normalize exact execution mode (blank = unknown = fail closed).
    _req_mode = str(execution_mode or "").strip().lower()
    if not _req_mode or _req_mode not in {"live", "paper"}:
        log.warning(
            "[%s] reeval disposition=LOOKUP_FAILED unknown execution_mode=%r canonical=%s",
            client_id, execution_mode, canonical,
        )
        return _DispositionResult(_DISPOSITION_LOOKUP_FAILED)

    # Step 3: Determine overnight session key.
    _session_key = session_key or _overnight_reeval_session_key()

    # Step 4: Query opportunity ledger.
    if lookup.lookup_status == _LS_LOOKUP_FAILED:
        log.warning(
            "[%s] reeval disposition=LOOKUP_FAILED opportunity-store error canonical=%s err=%s",
            client_id, canonical, lookup.error,
        )
        return _DispositionResult(_DISPOSITION_LOOKUP_FAILED)

    # Step 5: If FOUND, inspect durable evidence with exact mode + session guards.
    _has_current_session_proof = False
    if lookup.lookup_status == _LS_FOUND:
        row = lookup.row
        _status = str(row.get("opportunity_status") or "").upper()
        _stage  = str(row.get("miss_stage") or "").upper()
        _reason = str(row.get("miss_reason") or "")
        _meta   = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}

        # Mode isolation: blank stored mode NEVER establishes ownership.
        _recorded_mode = str(_meta.get("execution_mode") or _meta.get("mode") or "").strip().lower()
        _mode_match = bool(_recorded_mode and _recorded_mode == _req_mode)

        # Session isolation: blank stored session NEVER counts as current.
        _recorded_session = str(_meta.get("overnight_reeval_session_key") or "").strip()
        _session_match = bool(_recorded_session and _recorded_session == _session_key)
        _reattach_in_progress = bool(_meta.get("reattach_in_progress"))

        if _mode_match and _session_match:
            _has_current_session_proof = True

            # PR #388 nonterminal-ownership-poisoning amendment
            # ────────────────────────────────────────────────
            # opportunity_ledger.update_opportunity() blocks lifecycle
            # regression but continues to write identifier + metadata fields
            # and returns True. That means a prior WATCHER_ARMED /
            # BROKER_SUBMITTED / BROKER_ACKED / FILLED row can retain its
            # stale lifecycle status while accepting current-session
            # metadata (mode, session key, local_order_id, and
            # reattach_in_progress=true) from _persist_reattach_in_progress_
            # fence(). Trusting that stale status immediately here would
            # return ALREADY_ARMED or ALREADY_OWNED before the exact
            # active-order fence runs, stranding a live PENDING_TRIGGER
            # order when watch() later fails and the marker stays set.
            #
            # Contract: when reattach_in_progress=true for the exact
            # current mode/session/order, defer ALL opportunity-based
            # ownership conclusions — WATCHER_ARMED, broker-owned,
            # terminal — and let the exact active-order fence decide.
            # The marker itself must remain until watch() succeeds and
            # _persist_watcher_armed_proof() explicitly clears it.

            # ALREADY_ARMED: durable WATCHER_ARMED proof for this client/mode/session.
            if _status == _OL_WATCHER_ARMED:
                if _reattach_in_progress:
                    log.warning(
                        "[%s] reeval: current-session WATCHER_ARMED opportunity "
                        "carries reattach_in_progress=true; deferring ALREADY_ARMED "
                        "disposition until exact active-order fence runs canonical=%s "
                        "session=%s",
                        client_id, canonical, _session_key,
                    )
                else:
                    log.info(
                        "[%s] reeval disposition=ALREADY_ARMED canonical=%s session=%s",
                        client_id, canonical, _session_key,
                    )
                    return _DispositionResult(_DISPOSITION_ALREADY_ARMED)

            # ALREADY_OWNED: broker-submitted, acked, or filled — entry is in flight.
            if _status in {_OL_BROKER_SUBMITTED, _OL_BROKER_ACKED, _OL_FILLED}:
                if _reattach_in_progress:
                    log.warning(
                        "[%s] reeval: current-session broker-owned opportunity "
                        "(status=%s) carries reattach_in_progress=true; deferring "
                        "ALREADY_OWNED disposition until exact active-order fence "
                        "runs canonical=%s session=%s",
                        client_id, _status, canonical, _session_key,
                    )
                else:
                    log.info(
                        "[%s] reeval disposition=ALREADY_OWNED canonical=%s session=%s status=%s",
                        client_id, canonical, _session_key, _status,
                    )
                    return _DispositionResult(_DISPOSITION_ALREADY_OWNED)

            # ALREADY_TERMINAL: canonical terminal set from opportunity_ledger.
            if _status in _OL_TERMINAL_STATUSES:
                if _reattach_in_progress:
                    log.warning(
                        "[%s] reeval: current-session terminal opportunity carries "
                        "reattach_in_progress=true; deferring terminal disposition "
                        "until exact active-order fence runs canonical=%s session=%s "
                        "status=%s stage=%s reason=%s",
                        client_id, canonical, _session_key, _status, _stage, _reason,
                    )
                else:
                    log.info(
                        "[%s] reeval disposition=ALREADY_TERMINAL canonical=%s session=%s "
                        "status=%s stage=%s reason=%s",
                        client_id, canonical, _session_key, _status, _stage, _reason,
                    )
                    return _DispositionResult(_DISPOSITION_ALREADY_TERMINAL)

            # Legacy terminal watcher-arm failure recorded in same-session.
            if (_status == "MISSED" and _stage == "WATCHER_ARM"
                    and _is_terminal_watch_arm_failure_reason(_reason)):
                if _reattach_in_progress:
                    log.warning(
                        "[%s] reeval: current-session watcher-arm terminal marker "
                        "carries reattach_in_progress=true; deferring terminal "
                        "disposition until exact active-order fence runs canonical=%s "
                        "session=%s reason=%s",
                        client_id, canonical, _session_key, _reason,
                    )
                else:
                    log.info(
                        "[%s] reeval disposition=ALREADY_TERMINAL (watcher-arm-failure) "
                        "canonical=%s session=%s",
                        client_id, canonical, _session_key,
                    )
                    return _DispositionResult(_DISPOSITION_ALREADY_TERMINAL)

            if (_status == "INTERNAL_ERROR"
                    and _is_terminal_watch_arm_failure_reason(_reason)):
                if _reattach_in_progress:
                    log.warning(
                        "[%s] reeval: current-session internal-error terminal marker "
                        "carries reattach_in_progress=true; deferring terminal "
                        "disposition until exact active-order fence runs canonical=%s "
                        "session=%s reason=%s",
                        client_id, canonical, _session_key, _reason,
                    )
                else:
                    log.info(
                        "[%s] reeval disposition=ALREADY_TERMINAL (internal-error) "
                        "canonical=%s session=%s",
                        client_id, canonical, _session_key,
                    )
                    return _DispositionResult(_DISPOSITION_ALREADY_TERMINAL)

        # Different mode or session: fall through to active-order check below.

    # Step 6: Query exact active local ENTRY ownership (durable DB truth).
    _order_status, _order_row = _query_active_entry_order(client_id, _req_mode, canonical)

    # Active-order lookup failure → fail closed (do not assume no order exists).
    if _order_status == _LS_LOOKUP_FAILED:
        log.warning(
            "[%s] reeval disposition=LOOKUP_FAILED active-order query failed canonical=%s mode=%s",
            client_id, canonical, _req_mode,
        )
        return _DispositionResult(_DISPOSITION_LOOKUP_FAILED)

    # Step 7: Resolve from active order.
    if _order_status == _LS_FOUND and _order_row:
        _active_status = str(_order_row.get("status") or "").upper()
        _existing_local_id = str(_order_row.get("local_order_id") or "").strip()

        _active_disp = _entry_order_disposition_from_status(
            client_id=client_id,
            canonical_signal_id=canonical,
            session_key=_session_key,
            status=_active_status,
            local_order_id=_existing_local_id,
            order_row=_order_row,
            source="active-entry-fence",
        )
        if _active_disp is not None:
            return _active_disp

        log.warning(
            "[%s] reeval disposition=AMBIGUOUS_OWNERSHIP active order has "
            "unrecognized status canonical=%s status=%s",
            client_id, canonical, _active_status,
        )
        return _DispositionResult(_DISPOSITION_AMBIGUOUS_OWNERSHIP)

    # Step 7b: No active order. Check latest exact ENTRY without a status
    # filter before any path can return NEW. Terminal order history is durable
    # no-replacement truth even when opportunity marker storage failed.
    _latest_status, _latest_row = _query_latest_entry_order_no_status(
        client_id, _req_mode, canonical,
    )
    if _latest_status == _LS_LOOKUP_FAILED:
        log.warning(
            "[%s] reeval disposition=LOOKUP_FAILED latest-entry query failed "
            "canonical=%s mode=%s",
            client_id, canonical, _req_mode,
        )
        return _DispositionResult(_DISPOSITION_LOOKUP_FAILED)
    if _latest_status == _LS_FOUND and _latest_row:
        _latest_order_status = str(_latest_row.get("status") or "").upper()
        _latest_local_id = str(_latest_row.get("local_order_id") or "").strip()
        _latest_disp = _entry_order_disposition_from_status(
            client_id=client_id,
            canonical_signal_id=canonical,
            session_key=_session_key,
            status=_latest_order_status,
            local_order_id=_latest_local_id,
            order_row=_latest_row,
            source="latest-entry-no-status-fence",
        )
        if _latest_disp is not None:
            return _latest_disp
        log.warning(
            "[%s] reeval disposition=AMBIGUOUS_OWNERSHIP latest entry has "
            "unrecognized status canonical=%s status=%s",
            client_id, canonical, _latest_order_status,
        )
        return _DispositionResult(_DISPOSITION_AMBIGUOUS_OWNERSHIP)

    # Step 8: No active order — resolve from opportunity evidence.
    if lookup.lookup_status == _LS_NOT_FOUND:
        # Both opportunity AND order queries definitively returned zero rows.
        log.debug(
            "[%s] reeval disposition=NEW canonical=%s session=%s",
            client_id, canonical, _session_key,
        )
        return _DispositionResult(_DISPOSITION_NEW)

    if _has_current_session_proof:
        # Had current-session opportunity evidence but it wasn't classified above
        # (e.g. unrecognised status) — do not guess; fail closed.
        log.warning(
            "[%s] reeval disposition=AMBIGUOUS_OWNERSHIP canonical=%s session=%s",
            client_id, canonical, _session_key,
        )
        return _DispositionResult(_DISPOSITION_AMBIGUOUS_OWNERSHIP)

    # Step 9: Old-session or different-mode row with no active order.
    # Safe to treat as NEW for this session + mode.
    log.debug(
        "[%s] reeval disposition=NEW (prior-session/mode record, no active order) "
        "canonical=%s session=%s mode=%s",
        client_id, canonical, _session_key, _req_mode,
    )
    return _DispositionResult(_DISPOSITION_NEW)


def _shared_watch_arm_failure_already_recorded(
    signal_id: str,
    client_id: str,
    signal: dict,
    *,
    session_key: Optional[str] = None,
) -> bool:
    """Legacy compatibility shim — delegates to the full disposition resolver.

    Returns True when disposition is ALREADY_ARMED or ALREADY_TERMINAL,
    preserving callers that used the old boolean interface.
    Do not add new callers; prefer _resolve_shared_setup_disposition directly.
    """
    disp_result = _resolve_shared_setup_disposition(
        signal_id, client_id, signal,
        execution_mode="paper",   # legacy callers don't pass mode; default paper avoids LOOKUP_FAILED
        session_key=session_key,
    )
    return disp_result.disposition in (_DISPOSITION_ALREADY_ARMED, _DISPOSITION_ALREADY_TERMINAL)


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


def _record_retryable_row(
    result: dict,
    *,
    job_id,
    signal_id,
    source: str,
) -> None:
    """Keep the durable retry count bound to the exact source rows."""
    result["retryable_deferred"] = int(result.get("retryable_deferred", 0) or 0) + 1
    retryable_rows = result.setdefault("retryable_rows", [])
    if not isinstance(retryable_rows, list):
        retryable_rows = []
        result["retryable_rows"] = retryable_rows
    retryable_rows.append(
        {
            "job_id": "" if job_id is None else str(job_id).strip(),
            "signal_id": "" if signal_id is None else str(signal_id).strip(),
            "source": str(source or "").strip().lower(),
        }
    )


def _overnight_source_provenance(*, source: str, job_id, signal_id) -> dict | None:
    """Return the exact source-row identity that must follow an overnight order."""
    _source = str(source or "").strip().lower()
    _job_id = "" if job_id is None else str(job_id).strip()
    _signal_id = "" if signal_id is None else str(signal_id).strip()
    if _source not in {"trade_queue", "ap_signals"} or not _job_id or not _signal_id:
        return None
    return {
        "overnight_source_table": _source,
        "overnight_source_job_id": _job_id,
        "overnight_source_signal_id": _signal_id,
    }


def _merge_overnight_source_provenance(metadata: dict, provenance: dict) -> bool:
    """Add source provenance without relabeling an already-identified order."""
    _keys = (
        "overnight_source_table",
        "overnight_source_job_id",
        "overnight_source_signal_id",
    )
    _existing = {
        key: str(metadata.get(key) or "").strip()
        for key in _keys
    }
    _has_existing = any(_existing.values())
    if _has_existing and any(_existing[key] != str(provenance.get(key) or "").strip() for key in _keys):
        return False
    metadata.update(provenance)
    return True


def _persist_overnight_order_provenance(
    order_state_machine,
    *,
    local_order_id: str,
    client_id: str,
    execution_mode: str,
    signal_id: str,
    provenance: dict,
) -> bool:
    """Durably bind a reattached order to its exact overnight source row."""
    _update_order_meta = getattr(order_state_machine, "update_order_meta", None)
    if not callable(_update_order_meta):
        log.critical(
            "OVERNIGHT_ORDER_PROVENANCE_UPDATE_UNAVAILABLE client=%s mode=%s "
            "signal=%s local_order_id=%s",
            client_id, execution_mode, signal_id, local_order_id,
        )
        return False
    try:
        return bool(
            _update_order_meta(
                local_order_id,
                dict(provenance),
                expected_status="PENDING_TRIGGER",
                expected_execution_mode=str(execution_mode or "").strip().lower(),
                expected_signal_id=str(signal_id or "").strip(),
            )
        )
    except Exception as exc:
        log.critical(
            "OVERNIGHT_ORDER_PROVENANCE_UPDATE_FAILED client=%s mode=%s "
            "signal=%s local_order_id=%s error=%s",
            client_id, execution_mode, signal_id, local_order_id, exc,
        )
        return False


def _classify_overnight_reeval_result(result: dict) -> dict:
    """Attach the operational completion/retry contract to a result dict."""
    if not isinstance(result, dict):
        result = {}
    fetched = int(result.get("fetched", 0) or 0)
    errors = int(result.get("errors", 0) or 0)
    stalled = bool(result.get("stalled"))
    skipped = int(result.get("skipped", 0) or 0)
    armed = int(result.get("armed", 0) or 0)
    terminal_rejected = int(result.get("terminal_rejected", result.get("rejected", 0)) or 0)
    terminal_errors = int(result.get("terminal_errors", errors) or 0)
    retryable_deferred = int(
        result.get("retryable_deferred", fetched if stalled and fetched > 0 else 0) or 0
    )
    already_resolved = int(result.get("already_resolved", 0) or 0)
    retryable_rows = result.get("retryable_rows")
    if not isinstance(retryable_rows, list):
        retryable_rows = []
    unresolved = int(
        result.get(
            "unresolved",
            max(
                0,
                fetched
                - armed
                - terminal_rejected
                - terminal_errors
                - retryable_deferred
                - already_resolved,
            ),
        )
        or 0
    )

    if skipped == -1:
        result_class = "SKIPPED_NOT_DUE"
        completed = False
        retryable = False
        retry_reason = None
    elif errors > 0 or terminal_errors > 0:
        result_class = "RETRYABLE_ROW_ERRORS"
        completed = False
        retryable = True
        retry_reason = "row_errors"
    elif retryable_deferred > 0:
        if armed > 0 or terminal_rejected > 0 or already_resolved > 0:
            result_class = "RETRYABLE_PARTIAL_DEFERRED"
            retry_reason = "retryable_rows_remain"
        else:
            result_class = "RETRYABLE_ALL_DEFERRED"
            retry_reason = "all_fetched_rows_deferred"
        completed = False
        retryable = True
    elif unresolved > 0:
        result_class = "RETRYABLE_ROW_ERRORS"
        completed = False
        retryable = True
        retry_reason = "unresolved_rows"
    elif fetched == 0:
        result_class = "COMPLETED_NO_WORK"
        completed = True
        retryable = False
        retry_reason = None
    else:
        result_class = "COMPLETED_WITH_DECISIONS"
        completed = True
        retryable = False
        retry_reason = None

    result["armed"] = armed
    result["terminal_rejected"] = terminal_rejected
    result["terminal_errors"] = terminal_errors
    result["retryable_deferred"] = retryable_deferred
    result["already_resolved"] = already_resolved
    result["retryable_rows"] = retryable_rows
    result["unresolved"] = unresolved
    result["stalled"] = bool(
        retryable_deferred > 0
        and armed == 0
        and terminal_rejected == 0
        and terminal_errors == 0
        and already_resolved == 0
    )
    # PR #388 P0-2: partial-source inventory override. If either source
    # lookup failed, we cannot certify the run complete or treat the visible
    # retryable subset as the complete inventory. This must run regardless of
    # the earlier row-deferred branch; otherwise RETRYABLE_PARTIAL_DEFERRED
    # can exhaust into the #559 safe-partial readiness exception.
    _source_lookup_failed = (
        result.get("trade_queue_status") == _SOURCE_STATUS_FAILED
        and result.get("ap_signals_status") == _SOURCE_STATUS_FAILED
    )
    if _source_lookup_failed:
        result_class = "RETRYABLE_SOURCE_LOOKUP_FAILED"
        completed = False
        retryable = True
        retry_reason = "source_lookup_failed"
    elif bool(result.get("source_lookup_partial")):
        result_class = "RETRYABLE_PARTIAL_SOURCE_INVENTORY"
        completed = False
        retryable = True
        retry_reason = "partial_source_inventory"

    result["result_class"] = result_class
    result["completed"] = completed
    result["retryable"] = retryable
    result["retry_reason"] = retry_reason
    return result


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
    always deferred to the breach-time execution seam, so watcher ownership
    is established without waiting for pre-market option-chain work.

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
        "terminal_rejected": 0,
        "skipped": 0,
        "errors": 0,
        "terminal_errors": 0,
        "retryable_deferred": 0,
        "retryable_rows": [],
        "already_resolved": 0,
        "unresolved": 0,
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
        "source_lookup_partial": False,
        "trade_queue_status": None,
        "ap_signals_status": None,
        "trade_queue_error": None,
        "ap_signals_error": None,
    }

    # Guard: only run on trading days, 9:00-9:45 AM ET window (unless force=True)
    now_et = _et_now()
    session_key = _overnight_reeval_session_key(now_et)
    if not force:
        if not _is_trading_day(now_et):
            log.info("[%s] overnight_reeval: skipping — not a trading day", client_id)
            result["skipped"] = -1
            return _classify_overnight_reeval_result(result)
        _in_window = (now_et.hour == 9 and 0 <= now_et.minute < 45)
        if not _in_window:
            log.info("[%s] overnight_reeval: skipping — outside 9:00-9:45 AM ET window (now=%02d:%02d)",
                     client_id, now_et.hour, now_et.minute)
            result["skipped"] = -1
            return _classify_overnight_reeval_result(result)

    # Fetch WATCHING signals from both sources of truth AND their status.
    # Source failure must NEVER be interpreted as "completed no work" — that
    # was the exact silent-success bug in the prior amendment where a Postgres
    # or Supabase outage produced zero rows and the runner marked the day
    # successful.
    _fetch_result = _fetch_watching_signals_with_status(client_id)
    watching_signals = sorted(_fetch_result.rows or [], key=_overnight_signal_sort_key)
    result["fetched"] = len(watching_signals or [])
    result["trade_queue_status"] = _fetch_result.trade_queue_status
    result["ap_signals_status"]  = _fetch_result.ap_signals_status
    result["trade_queue_error"]  = _fetch_result.trade_queue_error
    result["ap_signals_error"]   = _fetch_result.ap_signals_error

    _tq_failed  = _fetch_result.trade_queue_status != _SOURCE_STATUS_SUCCESS
    _sup_failed = _fetch_result.ap_signals_status  != _SOURCE_STATUS_SUCCESS
    result["source_lookup_partial"] = bool(_tq_failed or _sup_failed)

    if _tq_failed and _sup_failed and not watching_signals:
        # Both sources failed and no rows visible → cannot certify empty
        # inventory. Explicit retryable classification; do NOT let this
        # collapse into COMPLETED_NO_WORK.
        log.error(
            "[%s] overnight_reeval: SOURCE_LOOKUP_FAILED trade_queue=%s ap_signals=%s "
            "tq_err=%r sup_err=%r — refusing to certify empty inventory",
            client_id,
            _fetch_result.trade_queue_status, _fetch_result.ap_signals_status,
            _fetch_result.trade_queue_error, _fetch_result.ap_signals_error,
        )
        result["result_class"] = "RETRYABLE_SOURCE_LOOKUP_FAILED"
        result["completed"]    = False
        result["retryable"]    = True
        result["retry_reason"] = "source_lookup_failed"
        result["source_lookup_failed"] = True
        # Preserve counters and skip the classifier's COMPLETED_NO_WORK
        # branch; return this exact shape so the runner never sets
        # success_date and never runs post-overnight handoff.
        return result

    if _tq_failed or _sup_failed:
        # Partial inventory. Process the rows we did see, but mark the run
        # retryable so the runner does not set success_date on incomplete
        # truth. This is applied after the loop as an override below.
        log.warning(
            "[%s] overnight_reeval: PARTIAL_SOURCE_INVENTORY trade_queue=%s ap_signals=%s "
            "processing %d rows but classifying run retryable",
            client_id,
            _fetch_result.trade_queue_status, _fetch_result.ap_signals_status,
            len(watching_signals),
        )
        result["source_lookup_partial"] = True

    if not watching_signals:
        log.info("[%s] overnight_reeval: no WATCHING signals found", client_id)
        return _classify_overnight_reeval_result(result)

    log.info("[%s] overnight_reeval: found %d WATCHING signals to reeval", client_id, len(watching_signals))

    today = now_et.date()
    prior_levels_by_ticker: dict[str, dict] = {}
    snapshot_by_ticker: dict[str, dict] = {}
    for job in watching_signals:
        result["processed"] += 1
        job_id = job["id"]
        signal = job["payload"]
        if isinstance(signal, str):
            import json; signal = json.loads(signal)
        signal_id = job.get("signal_id") or signal.get("signal_id", "")
        job_source = job.get("_source") or ("ap_signals" if str(job_id).startswith("sup:") else "trade_queue")
        source_provenance = _overnight_source_provenance(
            source=job_source,
            job_id=job_id,
            signal_id=signal_id,
        )
        if source_provenance is None:
            log.critical(
                "[%s] overnight_reeval: source-row identity incomplete source=%r job_id=%r signal_id=%r "
                "— classifying retryable_deferred",
                client_id, job_source, job_id, signal_id,
            )
            result["skipped"] = result.get("skipped", 0) + 1
            _record_retryable_row(
                result, job_id=job_id, signal_id=signal_id, source=job_source
            )
            continue
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
            result["terminal_rejected"] += 1
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
                    result["terminal_rejected"] += 1
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
                result["terminal_rejected"] += 1
                continue

            # ── Per-client/mode/session disposition lookup ────────────────────
            # Shared ap_signals rows stay WATCHING for all clients; this lookup
            # checks whether this exact client + mode + session already has durable
            # proof before touching master_control, OSM, selector, or broker.
            # _disp_result is set here for ap_signals rows; cleared for trade_queue.
            _disp_result: Optional[_DispositionResult] = None
            if job_source == "ap_signals":
                _disp_result = _resolve_shared_setup_disposition(
                    signal_id, client_id, signal,
                    execution_mode=_execution_mode,
                    session_key=session_key,
                )
                _disp = _disp_result.disposition

                if _disp == _DISPOSITION_ALREADY_ARMED:
                    log.info(
                        "[%s] overnight_reeval: ALREADY_ARMED %s — skip repeat arm",
                        ticker, signal_id,
                    )
                    result["skipped"] = result.get("skipped", 0) + 1
                    result["already_resolved"] += 1
                    continue

                if _disp == _DISPOSITION_ALREADY_OWNED:
                    log.info(
                        "[%s] overnight_reeval: ALREADY_OWNED %s — entry in-flight or filled",
                        ticker, signal_id,
                    )
                    result["skipped"] = result.get("skipped", 0) + 1
                    result["already_resolved"] += 1
                    continue

                if _disp == _DISPOSITION_ALREADY_TERMINAL:
                    log.info(
                        "[%s] overnight_reeval: ALREADY_TERMINAL %s — skip re-evaluation",
                        ticker, signal_id,
                    )
                    result["skipped"] = result.get("skipped", 0) + 1
                    result["already_resolved"] += 1
                    continue

                if _disp in (_DISPOSITION_LOOKUP_FAILED, _DISPOSITION_AMBIGUOUS_OWNERSHIP):
                    # Fail closed: truth is unavailable or ambiguous.
                    # Do NOT create an order while ownership is unresolved.
                    log.warning(
                        "[%s] overnight_reeval: disposition=%s for %s — "
                        "fail-closed as retryable_deferred",
                        ticker, _disp, signal_id,
                    )
                    result["skipped"] = result.get("skipped", 0) + 1
                    _record_retryable_row(
                        result, job_id=job_id, signal_id=signal_id, source=job_source
                    )
                    continue

                if _disp == _DISPOSITION_REATTACH_WATCHER:
                    # Case B: PENDING_TRIGGER order exists but WATCHER_ARMED
                    # proof is missing. Reuse the existing local_order_id;
                    # never create a new order.  No master_control, no
                    # selector, no broker.
                    #
                    # P0-3/P0-4: reattachment must be idempotent (never
                    # cancel a valid order) and must preserve exact identity
                    # + risk levels for THIS row only (no cross-row leakage
                    # from outer-loop variables).
                    _existing_oid = _disp_result.existing_local_order_id or ""
                    _existing_ord = _disp_result.existing_order_row or {}
                    log.info(
                        "[%s] overnight_reeval: REATTACH_WATCHER %s local_order_id=%s — "
                        "reusing existing PENDING_TRIGGER order",
                        ticker, signal_id, _existing_oid,
                    )
                    if not _existing_oid:
                        log.error(
                            "[%s] overnight_reeval: REATTACH_WATCHER missing local_order_id "
                            "signal=%s — failing closed as retryable",
                            ticker, signal_id,
                        )
                        result["skipped"] = result.get("skipped", 0) + 1
                        _record_retryable_row(
                            result, job_id=job_id, signal_id=signal_id, source=job_source
                        )
                        continue

                    # Explicit per-row locals — never fall back to outer-loop
                    # variables. `entry_trigger` is set later in the normal
                    # processing path, so referencing it here could either
                    # raise UnboundLocalError on the first affected row or
                    # (worse) reuse the previous ticker's trigger on a later
                    # iteration.
                    _reattach_trigger = None
                    _trig_raw = _existing_ord.get("trigger_price")
                    if _trig_raw is None:
                        _trig_raw = signal.get("entry_trigger")
                    try:
                        _reattach_trigger = float(_trig_raw) if _trig_raw is not None else None
                    except (TypeError, ValueError):
                        _reattach_trigger = None
                    if not _reattach_trigger or _reattach_trigger <= 0:
                        log.error(
                            "[%s] overnight_reeval: REATTACH_WATCHER cannot prove trigger "
                            "for signal=%s local_order_id=%s — failing closed as retryable "
                            "(never inherit trigger from a prior loop iteration)",
                            ticker, signal_id, _existing_oid,
                        )
                        result["skipped"] = result.get("skipped", 0) + 1
                        _record_retryable_row(
                            result, job_id=job_id, signal_id=signal_id, source=job_source
                        )
                        continue

                    def _reattach_price(order_key, signal_key):
                        v = _existing_ord.get(order_key)
                        if v is None:
                            v = signal.get(signal_key)
                        try:
                            return float(v) if v is not None else None
                        except (TypeError, ValueError):
                            return None

                    _reattach_stop   = _reattach_price("stop_underlying",   "stop_price")
                    _reattach_target = _reattach_price("target_underlying", "target_price")
                    _reattach_client = client_id
                    _reattach_mode   = _execution_mode
                    _reattach_side   = str(_existing_ord.get("direction") or side).upper()
                    _reattach_signal_id = str(_existing_ord.get("signal_id") or signal_id)
                    _reattach_canonical = str(
                        _existing_ord.get("canonical_signal_id")
                        or _resolve_canonical_signal_id(signal_id, signal)
                    )

                    # Parse existing order metadata (best-effort).
                    _ord_meta = _existing_ord.get("meta") or {}
                    if isinstance(_ord_meta, str):
                        try:
                            import json as _json
                            _ord_meta = _json.loads(_ord_meta)
                        except Exception:
                            _ord_meta = {}
                    if not isinstance(_ord_meta, dict):
                        _ord_meta = {}

                    _source_meta_present = all(
                        str(_ord_meta.get(key) or "").strip()
                        == str(value).strip()
                        for key, value in source_provenance.items()
                    )
                    if not _merge_overnight_source_provenance(
                        _ord_meta, source_provenance
                    ):
                        log.critical(
                            "[%s] overnight_reeval: REATTACH_WATCHER source provenance "
                            "mismatch signal=%s local_order_id=%s expected=%s existing=%s "
                            "— preserving order and classifying retryable",
                            ticker, signal_id, _existing_oid,
                            source_provenance, _ord_meta,
                        )
                        result["skipped"] = result.get("skipped", 0) + 1
                        _record_retryable_row(
                            result, job_id=job_id, signal_id=signal_id, source=job_source
                        )
                        continue

                    if not _source_meta_present and not _persist_overnight_order_provenance(
                        order_state_machine,
                        local_order_id=_existing_oid,
                        client_id=client_id,
                        execution_mode=_reattach_mode,
                        signal_id=signal_id,
                        provenance=source_provenance,
                    ):
                        log.critical(
                            "[%s] overnight_reeval: REATTACH_WATCHER source provenance "
                            "was not durably persisted signal=%s local_order_id=%s "
                            "— classifying retryable",
                            ticker, signal_id, _existing_oid,
                        )
                        result["skipped"] = result.get("skipped", 0) + 1
                        _record_retryable_row(
                            result, job_id=job_id, signal_id=signal_id, source=job_source
                        )
                        continue

                    # Metadata merge: existing FIRST, canonical values LAST
                    # so proven session/mode/client/canonical always win over
                    # any stale or blank stored metadata.
                    _reattach_metadata = {
                        **_ord_meta,
                        "overnight":                        True,
                        "reattach_watcher":                 True,
                        "contract_deferred":                True,
                        "contract_selection_deferred_to":   "breach_time",
                        "client_id":                        _reattach_client,
                        "execution_mode":                   _reattach_mode,
                        "overnight_reeval_session_key":     session_key,
                        "signal_id":                        _reattach_signal_id,
                        "canonical_signal_id":              _reattach_canonical,
                        **source_provenance,
                        # PR #388 Blocker 1: REATTACH is a proven PR#388
                        # seam and opts in to the late-attachment classifier.
                        "late_attachment_policy_eligible":  True,
                    }

                    import types as _types_mod
                    _reattach_plan = _types_mod.SimpleNamespace(
                        ticker            = str(_existing_ord.get("symbol") or ticker),
                        side              = _reattach_side,
                        direction         = _reattach_side,
                        score             = float(_existing_ord.get("score") or signal.get("score") or 0),
                        timeframe         = str(_existing_ord.get("timeframe") or signal.get("timeframe") or "1d"),
                        entry_trigger     = _reattach_trigger,
                        trigger_price     = _reattach_trigger,
                        stop_underlying   = _reattach_stop,
                        target_underlying = _reattach_target,
                        trigger_type      = "breach",
                        prior_day_high    = signal.get("prior_day_high"),
                        prior_day_low     = signal.get("prior_day_low"),
                        pattern           = _existing_ord.get("pattern") or signal.get("pattern"),
                        tier              = _existing_ord.get("tier") or signal.get("tier"),
                        contract_symbol   = str(_existing_ord.get("contract") or f"DEFERRED:{ticker}"),
                        contracts         = int(_existing_ord.get("qty") or 1),
                        limit_price       = float(_existing_ord.get("limit_price") or 0.01),
                        plan_id           = str(_existing_ord.get("plan_id") or ""),
                        signal_id         = _reattach_signal_id,
                        canonical_signal_id = _reattach_canonical,
                        client_id         = _reattach_client,
                        execution_mode    = _reattach_mode,
                        local_order_id    = _existing_oid,
                        materialization_generation = _reattach_metadata.get(
                            "materialization_generation"
                        ),
                        trigger_crossed_at = _reattach_metadata.get(
                            "trigger_crossed_at"
                        ),
                        late_attachment_policy_eligible = True,
                        metadata          = _reattach_metadata,
                    )

                    # P0-3 precheck: if the existing watcher already owns
                    # this exact local_order_id, do NOT call watch() again.
                    # A second watch() would hit add_signal() dedup, return
                    # False, and (without the recovery flags) attempt to
                    # cancel the valid PENDING_TRIGGER order. Just retry the
                    # durable proof write below.
                    _already_owned = False
                    try:
                        _has_order_fn = getattr(entry_watcher, "has_order", None)
                        if callable(_has_order_fn):
                            _already_owned = bool(_has_order_fn(_existing_oid))
                    except Exception as _has_exc:
                        log.warning(
                            "[%s] REATTACH_WATCHER has_order() check failed for %s: %s "
                            "— proceeding with recovery_rearm watch() (safe)",
                            ticker, _existing_oid, _has_exc,
                        )
                        _already_owned = False

                    if _already_owned:
                        log.info(
                            "[%s] REATTACH_WATCHER watcher already owns local_order_id=%s "
                            "— skipping second watch() call; retrying durable proof only",
                            ticker, _existing_oid,
                        )
                        _reattach_armed = True
                    else:
                        # Refuse stale/incomplete confirmed-trigger evidence
                        # before the mutating REATTACH ownership fence.  An
                        # already-owned watcher took the read-only durable
                        # proof path above; this gate remains mandatory for
                        # every path that would fence or call watch().
                        if not recovery_trigger_evidence_identity_is_proven(
                            _reattach_plan, _existing_oid
                        ):
                            log.critical(
                                "[%s] %s signal=%s local_order_id=%s — "
                                "refusing reattach; existing order unchanged",
                                ticker,
                                RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN,
                                signal_id,
                                _existing_oid,
                            )
                            result["unresolved"] += 1
                            continue

                        _pre_watch_fenced = _persist_reattach_in_progress_fence(
                            client_id=client_id,
                            execution_mode=_reattach_mode,
                            canonical_signal_id=_reattach_canonical,
                            signal_id=signal_id,
                            signal_payload=signal,
                            local_order_id=_existing_oid,
                            session_key=session_key,
                        )
                        if not _pre_watch_fenced:
                            log.critical(
                                "[%s] overnight_reeval: REATTACH_WATCHER pre-watch "
                                "ownership fence failed signal=%s local_order_id=%s "
                                "— watcher not called; classifying retryable_deferred",
                                ticker, signal_id, _existing_oid,
                            )
                            result["skipped"] = result.get("skipped", 0) + 1
                            _record_retryable_row(
                                result, job_id=job_id, signal_id=signal_id, source=job_source
                            )
                            continue
                        try:
                            _reattach_armed = entry_watcher.watch(
                                _reattach_plan, _existing_oid,
                                recovery_rearm=True,
                                no_cancel_on_reject=True,
                            )
                        except Exception as _reat_exc:
                            log.error(
                                "[%s] overnight_reeval: REATTACH_WATCHER watch() exception "
                                "signal=%s local_order_id=%s: %s",
                                ticker, signal_id, _existing_oid, _reat_exc,
                            )
                            # Retryable, not terminal: the existing
                            # PENDING_TRIGGER order is untouched and the
                            # next retry can attempt reattach again.
                            result["skipped"] = result.get("skipped", 0) + 1
                            _record_retryable_row(
                                result, job_id=job_id, signal_id=signal_id, source=job_source
                            )
                            continue

                    if not _reattach_armed:
                        # PR #388 truthful accounting on watch() False.
                        # Use the NO-STATUS-FILTER exact-order lookup so
                        # terminal statuses (CANCELED / EXPIRED / REJECTED
                        # / ERROR) are visible. The active-ownership fence
                        # helper _query_active_entry_order filters those
                        # out and would incorrectly return NOT_FOUND, which
                        # would cascade into a replacement ENTRY on the
                        # next reeval.
                        _post_status, _post_row = _query_exact_entry_order_by_local_id(
                            _existing_oid, client_id, _reattach_mode, _reattach_canonical,
                        )
                        _post_active_status = ""
                        if _post_status == _LS_FOUND and isinstance(_post_row, dict):
                            _post_active_status = str(_post_row.get("status") or "").upper()

                        # Case 1: row found, still preserved.
                        if _post_status == _LS_FOUND and _post_active_status in ("PENDING_TRIGGER", "CREATED"):
                            log.warning(
                                "[%s] overnight_reeval: REATTACH_WATCHER watch()"
                                " returned False signal=%s local_order_id=%s"
                                " — VERIFIED PRESERVED (order still %s);"
                                " classifying retryable_deferred",
                                ticker, signal_id, _existing_oid, _post_active_status,
                            )
                            result["skipped"] = result.get("skipped", 0) + 1
                            _record_retryable_row(
                                result, job_id=job_id, signal_id=signal_id, source=job_source
                            )
                            continue

                        # Case 2: row found in active-ownership family
                        # (submitted / accepted / open / partial / filled)
                        # — already owned, no further reeval work.
                        if _post_status == _LS_FOUND and _post_active_status in _ALREADY_OWNED_STATUSES:
                            log.warning(
                                "[%s] overnight_reeval: REATTACH_WATCHER watch()"
                                " returned False signal=%s local_order_id=%s"
                                " — order in-flight/filled (%s);"
                                " classifying already_resolved",
                                ticker, signal_id, _existing_oid, _post_active_status,
                            )
                            result["skipped"] = result.get("skipped", 0) + 1
                            result["already_resolved"] += 1
                            continue

                        # Case 3: row found in an explicit terminal status.
                        if _post_status == _LS_FOUND and _post_active_status in _TERMINAL_ENTRY_STATUSES:
                            _terminal_marker_persisted = _persist_reattach_terminal_suppression_marker(
                                client_id=client_id,
                                execution_mode=_reattach_mode,
                                canonical_signal_id=_reattach_canonical,
                                signal_id=signal_id,
                                signal_payload=signal,
                                local_order_id=_existing_oid,
                                session_key=session_key,
                                reason=f"reattach_post_watch_terminal:{_post_active_status}",
                                post_status=_post_status,
                                post_row_status=_post_active_status,
                            )
                            if not _terminal_marker_persisted:
                                log.critical(
                                    "[%s] overnight_reeval: REATTACH_WATCHER watch()"
                                    " returned False signal=%s local_order_id=%s"
                                    " — order terminalized (%s) but durable"
                                    " terminal marker persist FAILED;"
                                    " classifying retryable_deferred (fail closed)",
                                    ticker, signal_id, _existing_oid, _post_active_status,
                                )
                                result["skipped"] = result.get("skipped", 0) + 1
                                _record_retryable_row(
                                    result, job_id=job_id, signal_id=signal_id, source=job_source
                                )
                                continue

                            log.warning(
                                "[%s] overnight_reeval: REATTACH_WATCHER watch()"
                                " returned False signal=%s local_order_id=%s"
                                " — order terminalized (%s); durable marker"
                                " persisted; classifying already_resolved"
                                " (no replacement now or on next attempt)",
                                ticker, signal_id, _existing_oid, _post_active_status,
                            )
                            result["skipped"] = result.get("skipped", 0) + 1
                            result["already_resolved"] += 1
                            continue

                        # Case 4: row found in an UNRECOGNISED status —
                        # persist ambiguity so the next reeval sees terminal.
                        # Case 5: row missing (NOT_FOUND) — persist ambiguity.
                        # Case 6: lookup failed — persist ambiguity.
                        # In every one of these, `retryable_deferred` alone
                        # is not enough: the next reeval could see no
                        # active order and classify NEW, creating a
                        # replacement ENTRY. Write a durable AMBIGUITY
                        # record via the opportunity ledger so the
                        # disposition resolver returns ALREADY_TERMINAL.
                        _ambiguity_persisted = _persist_reattach_post_watch_ambiguity(
                            client_id=client_id,
                            execution_mode=_reattach_mode,
                            canonical_signal_id=_reattach_canonical,
                            signal_id=signal_id,
                            signal_payload=signal,
                            local_order_id=_existing_oid,
                            session_key=session_key,
                            post_status=_post_status,
                            post_row_status=_post_active_status,
                        )
                        if _ambiguity_persisted:
                            log.critical(
                                "[%s] overnight_reeval: REATTACH_WATCHER watch()"
                                " returned False AND post-verify=%s row_status=%r"
                                " signal=%s local_order_id=%s"
                                " — durable ambiguity marker persisted;"
                                " next reeval will classify ALREADY_TERMINAL"
                                " (no replacement ENTRY)",
                                ticker, _post_status, _post_active_status,
                                signal_id, _existing_oid,
                            )
                            result["skipped"] = result.get("skipped", 0) + 1
                            result["already_resolved"] += 1
                            continue

                        # Ambiguity marker persist failed — fail closed
                        # by classifying the run RETRYABLE with an explicit
                        # unresolved counter so this attempt cannot mark
                        # success and the next attempt retries the whole
                        # path (which will hit the same ambiguity and
                        # attempt to persist again).
                        log.critical(
                            "[%s] overnight_reeval: REATTACH_WATCHER watch()"
                            " returned False AND post-verify=%s row_status=%r"
                            " AND ambiguity marker persist FAILED;"
                            " signal=%s local_order_id=%s"
                            " — classifying retryable_deferred (fail closed;"
                            " next reeval may still see this row)",
                            ticker, _post_status, _post_active_status,
                            signal_id, _existing_oid,
                        )
                        result["skipped"] = result.get("skipped", 0) + 1
                        _record_retryable_row(
                            result, job_id=job_id, signal_id=signal_id, source=job_source
                        )
                        continue

                    # Persist durable WATCHER_ARMED proof after successful reattachment.
                    _canonical_for_proof = _resolve_canonical_signal_id(signal_id, signal)
                    _reat_proof_ok = _persist_watcher_armed_proof(
                        client_id=client_id,
                        execution_mode=_execution_mode,
                        canonical_signal_id=_canonical_for_proof,
                        signal_id=signal_id,
                        signal_payload=signal,
                        local_order_id=str(_existing_oid),
                        session_key=session_key,
                        clear_reattach_in_progress=True,
                        extra_meta={
                            "source_table": source_provenance["overnight_source_table"],
                            "source_job_id": source_provenance["overnight_source_job_id"],
                            **source_provenance,
                            "ticker": ticker,
                            "side": side,
                            "contract_deferred": True,
                            "contract_selection_deferred_to": "breach_time",
                            "reattach_watcher": True,
                            "armed_at": datetime.now(timezone.utc).isoformat(),
                        },
                    )

                    if not _reat_proof_ok:
                        log.critical(
                            "OVERNIGHT_REEVAL_REATTACH_PROOF_WRITE_FAILED | "
                            "client=%s mode=%s canonical=%s local_order_id=%s session=%s | "
                            "watcher reattached but WATCHER_ARMED proof NOT persisted — "
                            "next retry will REATTACH again (idempotent)",
                            client_id, _execution_mode, _canonical_for_proof,
                            _existing_oid, session_key,
                        )
                        result["skipped"] = result.get("skipped", 0) + 1
                        _record_retryable_row(
                            result, job_id=job_id, signal_id=signal_id, source=job_source
                        )
                        continue

                    log.info(
                        "[%s] ✅ REATTACH_WATCHER ARMED — local_order_id=%s canonical=%s",
                        ticker, _existing_oid, _canonical_for_proof,
                    )
                    _mark_job_watching_armed(job_id, client_id, f"reattached:{_existing_oid}")
                    result["armed"] += 1
                    result["fresh_armed"] += 1
                    continue

                if _disp == _DISPOSITION_ACTIVE_CREATED_RETRY:
                    # PR #388 Blocker #6: an exact active ENTRY order in
                    # CREATED status already exists for this client + mode
                    # + canonical_signal_id. Preserve it. Do NOT run
                    # master_control, contract selector, OSM
                    # create_entry_order, or the broker. The next reeval
                    # (or the row's established owner) can transition it
                    # to PENDING_TRIGGER and take the REATTACH_WATCHER
                    # path; a duplicate create_entry_order here would
                    # defeat the whole active-order fence.
                    _created_oid = (_disp_result.existing_local_order_id or "")
                    log.info(
                        "[%s] overnight_reeval: ACTIVE_CREATED_RETRY %s "
                        "local_order_id=%s — preserving CREATED order; "
                        "classifying run retryable_deferred",
                        ticker, signal_id, _created_oid,
                    )
                    result["skipped"] = result.get("skipped", 0) + 1
                    _record_retryable_row(
                        result, job_id=job_id, signal_id=signal_id, source=job_source
                    )
                    continue

                # RETRYABLE or NEW — fall through to normal processing.
            else:
                # trade_queue rows are genuinely per-client; use legacy boolean fence.
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
                    result["terminal_rejected"] += 1
                    continue

            # Step 1: Fetch prior-day levels from broker
            _ticker_key = str(ticker or "").upper()
            prior_levels = prior_levels_by_ticker.get(_ticker_key)
            if prior_levels is None:
                prior_levels = {}
                if hasattr(market_data_broker, "get_prior_day_levels"):
                    try:
                        prior_levels = market_data_broker.get_prior_day_levels(ticker) or {}
                    except Exception as _prior_exc:
                        log.warning(
                            "[%s] overnight_reeval: prior-level fetch failed; retrying row later: %s",
                            ticker,
                            _prior_exc,
                        )
                        if _paper_rescue_only:
                            _mark_job_watching_reason(
                                job_id,
                                client_id,
                                "after_hours_deferred:overnight_prior_levels_fetch_failed",
                            )
                        result["skipped"] += 1
                        _record_retryable_row(
                            result, job_id=job_id, signal_id=signal_id, source=job_source
                        )
                        continue
                else:
                    log.warning(
                        "[%s] market-data broker has no get_prior_day_levels — cannot validate overnight daily signal",
                        ticker,
                    )
                if prior_levels:
                    prior_levels_by_ticker[_ticker_key] = prior_levels

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
                _record_retryable_row(
                    result, job_id=job_id, signal_id=signal_id, source=job_source
                )
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
                result["terminal_rejected"] += 1
                continue
            if entry_trigger:
                signal["entry_trigger"] = entry_trigger

            # Step 3: Overnight daily structure validation
            # Fetch a fresh market snapshot (pre-market quote)
            snapshot = snapshot_by_ticker.get(_ticker_key)
            if snapshot is None:
                try:
                    snapshot = fetch_market_snapshot(ticker, market_data_broker)
                except Exception as _snapshot_exc:
                    log.warning(
                        "[%s] overnight_reeval: snapshot fetch failed; retrying row later: %s",
                        ticker,
                        _snapshot_exc,
                    )
                    if _paper_rescue_only:
                        _mark_job_watching_reason(
                            job_id,
                            client_id,
                            "after_hours_deferred:overnight_snapshot_fetch_failed",
                        )
                    result["skipped"] += 1
                    _record_retryable_row(
                        result, job_id=job_id, signal_id=signal_id, source=job_source
                    )
                    continue
                if snapshot:
                    snapshot_by_ticker[_ticker_key] = snapshot
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
                    _record_retryable_row(
                        result, job_id=job_id, signal_id=signal_id, source=job_source
                    )
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
                result["terminal_rejected"] += 1
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
                        _hydrated = _hydrate_plan_from_signal(
                            signal,
                            client_id=client_id,
                            execution_mode=_execution_mode,
                        )
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
                        result["terminal_rejected"] += 1
                        continue
                # ── PR #388 P0-5 plan normalization ──────────────────────
                # Regardless of whether the plan came from MC or _hydrate_
                # plan_from_signal, force identity + risk fields to the
                # canonical scanner values so the watcher never inherits a
                # random REEVAL: id, a blank client_id, or a missing stop.
                if getattr(decision, "plan", None) is not None:
                    _plan = decision.plan
                    _canon_for_norm = _resolve_canonical_signal_id(signal_id, signal)
                    _sid_for_norm = str(signal.get("signal_id") or signal_id or "").strip()
                    setattr(_plan, "signal_id", _sid_for_norm)
                    setattr(_plan, "canonical_signal_id", _canon_for_norm)
                    setattr(_plan, "client_id", client_id)
                    setattr(_plan, "execution_mode", str(_execution_mode).strip().lower())
                    for _plan_attr, _sig_key in (
                        ("stop_underlying",   "stop_price"),
                        ("target_underlying", "target_price"),
                    ):
                        if getattr(_plan, _plan_attr, None) in (None, 0, 0.0):
                            _sv = signal.get(_sig_key)
                            try:
                                _sv_f = float(_sv) if _sv is not None else None
                                if _sv_f and _sv_f > 0:
                                    setattr(_plan, _plan_attr, _sv_f)
                            except (TypeError, ValueError):
                                pass
                    _meta = getattr(_plan, "metadata", None) or {}
                    if not isinstance(_meta, dict):
                        _meta = {}
                    _meta.update({
                        "signal_id":           _sid_for_norm,
                        "canonical_signal_id": _canon_for_norm,
                        "client_id":           client_id,
                        "execution_mode":      str(_execution_mode).strip().lower(),
                    })
                    setattr(_plan, "metadata", _meta)
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
                result["terminal_errors"] += 1
                continue

            # Step 5: Establish watcher ownership before expensive option work.
            # Contract selection remains authoritative at the existing breach-time
            # execution seam, where live option quotes and every quality gate apply.
            contract_deferred = True
            log.info(
                "[%s] overnight_reeval: deferring contract selection to breach time "
                "and arming watcher immediately | trigger=%.4f",
                ticker, entry_trigger or 0,
            )
            decision.plan.contract_symbol = f"DEFERRED:{ticker}"
            decision.plan.limit_price = 0.01
            if not getattr(decision.plan, "contracts", None):
                decision.plan.contracts = int(os.getenv("MIN_CONTRACTS_PER_POSITION", "2"))
            if not hasattr(decision.plan, "metadata") or decision.plan.metadata is None:
                decision.plan.metadata = {}
            decision.plan.metadata.update({
                **source_provenance,
                "contract_deferred": True,
                "deferred_breach_selection": True,
                "selection_context": "deferred_breach",
                "pre_market_contract_selection_skipped": True,
                "pre_market_contract_selection_failed": False,
                "pre_market_selector_failure": None,
                "pre_market_selector_reason_code": None,
                "contract_selection_deferred_to": "breach_time",
                # PR #388 Blocker 1: only PR#388 seams opt in to the
                # late-attachment continuation/reset classifier. Ordinary
                # intraday/direct arms keep committed-main's strict
                # arm_already_through_trigger invariant.
                "late_attachment_policy_eligible": True,
            })
            # Also expose the flag as a top-level plan attribute so
            # APEntryWatcher.watch() can read it without descending into
            # metadata (matches how client_id / execution_mode are wired).
            setattr(decision.plan, "late_attachment_policy_eligible", True)
            if _lifecycle_ok:
                try:
                    from ap_lifecycle import LEDGER as _L
                    _L.log_event(
                        signal_id,
                        "contract_deferred",
                        owner="overnight_reeval",
                        reason="watcher_first_deferred_to_breach",
                    )
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
                        result["terminal_rejected"] += 1
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
                    result["terminal_errors"] += 1
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
                result["terminal_errors"] += 1
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
                result["terminal_errors"] += 1
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
                    result["terminal_errors"] += 1
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
                result["terminal_errors"] += 1
                continue

            # Step 7: Arm entry watcher — pass plan (not signal) and the OSM order ID
            _contract_sym = str(getattr(decision.plan, "contract_symbol", "") or "")
            _arm_label    = _contract_sym if _contract_sym else "DEFERRED_AT_BREACH"
            try:
                armed = entry_watcher.watch(decision.plan, local_order_id)
                if armed:
                    try:
                        from ap.intelligence_context_handoff import enqueue_preopen_context_best_effort

                        enqueue_preopen_context_best_effort(
                            signal,
                            client_id=client_id,
                            execution_mode=_execution_mode,
                            canonical_signal_id=_resolve_canonical_signal_id(signal_id, signal),
                            local_order_id=str(local_order_id),
                        )
                    except Exception as _preopen_intel_exc:
                        log.warning(
                            "[%s] PREOPEN intelligence handoff error signal_id=%s local_order_id=%s: %s",
                            ticker, signal_id, local_order_id, _preopen_intel_exc,
                        )
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

                    # ── Durable WATCHER_ARMED proof write (shared ap_signals rows) ──
                    # For ap_signals source rows _mark_job_watching_armed() only logs
                    # (correctly leaves shared row WATCHING for other clients).
                    # We MUST persist an explicit WATCHER_ARMED record in
                    # client_signal_opportunities so that the next retry can find
                    # ALREADY_ARMED instead of creating a second order.
                    # If the write fails: fail as retryable — the watcher is armed
                    # in memory but the next retry must discover the existing order
                    # via the active-order fence (REATTACH_WATCHER path) rather than
                    # creating a duplicate.
                    _arm_proof_ok = True
                    if job_source == "ap_signals":
                        _canonical_for_arm = _resolve_canonical_signal_id(signal_id, signal)
                        _arm_proof_ok = _persist_watcher_armed_proof(
                            client_id=client_id,
                            execution_mode=_execution_mode,
                            canonical_signal_id=_canonical_for_arm,
                            signal_id=signal_id,
                            signal_payload=signal,
                            local_order_id=str(local_order_id),
                            session_key=session_key,
                            extra_meta={
                                "source_table": source_provenance["overnight_source_table"],
                                "source_job_id": source_provenance["overnight_source_job_id"],
                                **source_provenance,
                                "ticker": ticker,
                                "side": side,
                                "contract_deferred": contract_deferred,
                                "contract_selection_deferred_to": "breach_time",
                                "armed_at": datetime.now(timezone.utc).isoformat(),
                            },
                        )

                        if not _arm_proof_ok:
                            log.critical(
                                "OVERNIGHT_REEVAL_ARM_PROOF_WRITE_FAILED | "
                                "client=%s mode=%s canonical=%s local_order_id=%s session=%s | "
                                "watcher armed in-memory but WATCHER_ARMED NOT persisted — "
                                "classifying as retryable_deferred; next retry will find "
                                "PENDING_TRIGGER order via active-order fence (REATTACH path)",
                                client_id, _execution_mode, _canonical_for_arm,
                                local_order_id, session_key,
                            )
                            result["skipped"] = result.get("skipped", 0) + 1
                            _record_retryable_row(
                                result, job_id=job_id, signal_id=signal_id, source=job_source
                            )
                            continue

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
                        result["terminal_errors"] += 1
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
                    result["terminal_rejected"] += 1
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
                    result["terminal_errors"] += 1
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
                result["terminal_errors"] += 1

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
            result["terminal_errors"] += 1

    result["stale_inventory_only"] = bool(
        result["processed"] > 0
        and result["fresh_processed"] == 0
        and result["stale_skipped"] > 0
    )
    result["unresolved"] = max(
        0,
        result["processed"]
        - result["armed"]
        - result["terminal_rejected"]
        - result["terminal_errors"]
        - result["retryable_deferred"]
        - result["already_resolved"],
    )
    log.info(
        "[%s] overnight_reeval complete: processed=%d armed=%d terminal_rejected=%d "
        "terminal_errors=%d retryable_deferred=%d unresolved=%d skipped=%d "
        "stale_skipped=%d fresh_processed=%d fresh_armed=%d stale_inventory_only=%s",
        client_id,
        result["processed"],
        result["armed"],
        result["terminal_rejected"],
        result["terminal_errors"],
        result["retryable_deferred"],
        result["unresolved"],
        result["skipped"],
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
    if result["retryable_deferred"] > 0 and result["terminal_rejected"] == 0 and result["armed"] == 0:
        log.error(
            "[%s] OVERNIGHT_REEVAL_STALLED fetched=%d retryable_deferred=%d — "
            "every owned signal deferred to next run; queue is NOT draining. "
            "category=SILENT_STALL severity=ERROR "
            "likely_cause=market_data_unavailable_or_all_rows_retry_later",
            client_id,
            result["fetched"],
            result["retryable_deferred"],
        )
    return _classify_overnight_reeval_result(result)


_SOURCE_STATUS_SUCCESS = "SUCCESS"
_SOURCE_STATUS_FAILED  = "FAILED"


class _FetchWatchingSignalsResult(NamedTuple):
    """Source-truth result of _fetch_watching_signals.

    .rows                — list of job dicts (may be non-empty even if one
                           source failed; caller must inspect the statuses).
    .trade_queue_status  — SUCCESS / FAILED
    .ap_signals_status   — SUCCESS / FAILED
    .trade_queue_error   — short error string if FAILED
    .ap_signals_error    — short error string if FAILED

    A FAILED status must never be interpreted as "zero rows / completed no
    work". The caller must classify the run retryable when any source is
    FAILED (partial inventory) and RETRYABLE_SOURCE_LOOKUP_FAILED when both
    sources are FAILED and rows==0.
    """
    rows: list
    trade_queue_status: str
    ap_signals_status:  str
    trade_queue_error:  Optional[str]
    ap_signals_error:   Optional[str]


def _fetch_watching_signals(client_id: str) -> list:
    """Backward-compatible wrapper that returns rows only.

    Retained for callers that already expect a list AND for existing test
    harnesses that monkey-patch this function to inject fixture rows. The
    real reeval path calls _fetch_watching_signals_with_status which, when
    this function is patched, wraps the patched result with SUCCESS status
    so P0-2 source-truth behavior stays consistent for legacy fixtures."""
    return _fetch_watching_signals_with_status_impl(client_id).rows


# Sentinel attribute stamped on the built-in wrapper so
# _fetch_watching_signals_with_status can detect a monkey-patched
# replacement (which will not carry the sentinel) even across
# importlib.reload cycles, where an is-comparison against a captured
# original can fail because reload creates a new function object.
_fetch_watching_signals._is_original_wrapper = True


def _fetch_watching_signals_with_status(client_id: str) -> _FetchWatchingSignalsResult:
    """Public status-aware entry point used by run_overnight_reeval.

    Two-tier dispatch so legacy tests keep working:
      * If a test has monkey-patched _fetch_watching_signals (list-returning),
        call it and wrap the list in a SUCCESS status result.
      * Otherwise run the real implementation which captures per-source
        status alongside rows.

    A test that wants to inject a source FAILURE must monkey-patch this
    function directly (or _fetch_watching_signals_with_status_impl).

    Detection strategy: the built-in wrapper carries a sentinel attribute
    _is_original_wrapper=True. Any test-supplied replacement will not
    have that attribute. This is robust to importlib.reload cycles used
    by other tests (e.g. test_p0_paper_overnight_rescue_materialization
    reloads this module twice, which invalidates a captured-object
    identity check).

    We use globals() rather than sys.modules[__name__] because
    importlib.reload creates a NEW module object registered at the same
    name; a test file that imported this module BEFORE the reload holds
    an OLD reference and applies monkeypatch.setattr(ov, ...) to the OLD
    module, while sys.modules[__name__] returns the NEW post-reload
    module. globals() always returns the namespace of the module this
    function was DEFINED in — the same one whose _fetch_watching_signals
    the test just patched.
    """
    _current_legacy = globals().get("_fetch_watching_signals")
    _is_patched = (
        _current_legacy is not None
        and not getattr(_current_legacy, "_is_original_wrapper", False)
    )
    if _is_patched:
        # A test has replaced the legacy wrapper with its own function.
        # Call it and wrap the list result as SUCCESS from both sources.
        try:
            rows = list(_current_legacy(client_id) or [])
        except Exception as _pexc:
            log.error("patched _fetch_watching_signals raised: %s", _pexc)
            return _FetchWatchingSignalsResult(
                rows=[],
                trade_queue_status=_SOURCE_STATUS_FAILED,
                ap_signals_status=_SOURCE_STATUS_FAILED,
                trade_queue_error=str(_pexc),
                ap_signals_error=str(_pexc),
            )
        return _FetchWatchingSignalsResult(
            rows=rows,
            trade_queue_status=_SOURCE_STATUS_SUCCESS,
            ap_signals_status=_SOURCE_STATUS_SUCCESS,
            trade_queue_error=None,
            ap_signals_error=None,
        )
    return _fetch_watching_signals_with_status_impl(client_id)


# Captured at module import so _fetch_watching_signals_with_status can detect
# a test-time replacement of the legacy wrapper.
_ORIGINAL_FETCH_WATCHING_SIGNALS = _fetch_watching_signals


def _fetch_watching_signals_with_status_impl(client_id: str) -> _FetchWatchingSignalsResult:
    """
    Fetch WATCHING signals from both sources of truth AND report per-source
    lookup status. See _FetchWatchingSignalsResult docstring.

    1. trade_queue.status='WATCHING' for locally queued in-session jobs.
    2. ap_signals.decision_status='WATCHING' for scanner/audit signals written
       by APSignalStore.
    """
    results: list[dict] = []
    seen_signal_ids: set[str] = set()
    trade_queue_status: str = _SOURCE_STATUS_SUCCESS
    trade_queue_error:  Optional[str] = None
    ap_signals_status:  str = _SOURCE_STATUS_SUCCESS
    ap_signals_error:   Optional[str] = None

    # PR #388 P0-8: SINGLE fetch limit for BOTH sources. Previously the
    # shared ap_signals query was hardcoded at .limit(300) while trade_queue
    # honored OVERNIGHT_FETCH_LIMIT (default 500). Once the shared inventory
    # exceeded 300 rows, the newest 300 could permanently occupy every fetch
    # and older rows never entered the per-client disposition resolver.
    try:
        _fetch_limit = int(os.getenv("OVERNIGHT_FETCH_LIMIT", "500"))
    except (TypeError, ValueError):
        _fetch_limit = 500
    _fetch_limit = max(100, min(_fetch_limit, 2000))

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
        trade_queue_status = _SOURCE_STATUS_FAILED
        trade_queue_error = str(e)

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
            log.error(
                "_fetch_watching_signals[ap_signals]: missing Supabase credentials "
                "— source lookup cannot proceed, treating as FAILED (not zero-rows)"
            )
            ap_signals_status = _SOURCE_STATUS_FAILED
            ap_signals_error = "missing_supabase_credentials"
        else:
            # Session-aware cutoff (see _signals_lookback_cutoff_iso). A
            # Friday scanner setup remains visible on Monday morning without
            # requiring an explicit SIGNALS_LOOKBACK override on every
            # deployment. Env var still honored when explicitly set.
            cutoff = _signals_lookback_cutoff_iso()
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
                .limit(_fetch_limit)  # P0-8: unified with trade_queue limit
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
        ap_signals_status = _SOURCE_STATUS_FAILED
        ap_signals_error = str(e)

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
        "trade_queue_status=%s ap_signals_status=%s "
        "(fan-out: all clients see same scanner setups)",
        client_id, len(results), tq_count, sup_count,
        trade_queue_status, ap_signals_status,
    )
    return _FetchWatchingSignalsResult(
        rows=results,
        trade_queue_status=trade_queue_status,
        ap_signals_status=ap_signals_status,
        trade_queue_error=trade_queue_error,
        ap_signals_error=ap_signals_error,
    )


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
                    SET status = 'ARMED',
                        last_error = %s,
                        started_ts = COALESCE(started_ts, NOW())
                    WHERE id = %s AND client_id = %s AND status = 'WATCHING'
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
        upsert_ap_signal_row_with_fallback(sbc, {
            "signal_id":       canonical_signal_id(str(signal_id)),
            "client_email":    canonical_client_email(client_id),
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
        })
    except Exception as e:
        log.debug("_log_rejection_supabase failed: %s", e)
